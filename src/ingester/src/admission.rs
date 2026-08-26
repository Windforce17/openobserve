// Copyright 2026 OpenObserve Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

//! Pre-body ingest admission accounting.
//!
//! The memory circuit breaker samples process RSS once per second (see
//! `update_node_memory_usage` in the jobs crate) and is only consulted AFTER
//! an ingest request body has been fully buffered (and decompressed). A burst
//! of concurrent large batches can therefore all pass the breaker while the
//! sampled RSS is stale, then expand simultaneously
//! (decompress -> decode -> flatten -> arrow) and drive the cgroup past its
//! limit before the next RSS sample — the kernel OOM-kills while the breaker
//! believes memory is still under the threshold.
//!
//! This module maintains a global count of PROJECTED in-flight ingest bytes
//! (content-length x expansion factor), reserved before the body is read and
//! released when the request finishes. The reservation is added to the
//! breaker's memory reading, so admission of the allocation ABOUT to happen is
//! decided against the same envelope the breaker enforces.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use config::{get_config, metrics};

use crate::errors::{Error, Result};

/// Projected in-flight ingest bytes. Source of truth; the prometheus gauge is
/// a mirror for observability.
static RESERVED_BYTES: AtomicI64 = AtomicI64::new(0);

/// A reservation of projected ingest bytes against the memory envelope.
/// Dropping it releases the reservation.
#[derive(Debug)]
pub struct IngestReservation {
    bytes: i64,
}

impl IngestReservation {
    /// The projected bytes held by this reservation.
    pub fn bytes(&self) -> usize {
        self.bytes as usize
    }
}

impl Drop for IngestReservation {
    fn drop(&mut self) {
        if self.bytes > 0 {
            let prev = RESERVED_BYTES.fetch_sub(self.bytes, Ordering::AcqRel);
            metrics::INGEST_ADMISSION_RESERVED_BYTES
                .with_label_values::<&str>(&[])
                .set((prev - self.bytes).max(0));
        }
    }
}

/// Currently reserved projected in-flight ingest bytes.
pub fn reserved_bytes() -> usize {
    RESERVED_BYTES.load(Ordering::Acquire).max(0) as usize
}

/// The memory envelope the breaker enforces: ratio% of the cgroup/node memory
/// limit. Returns `None` when the memory circuit breaker is disabled.
///
/// Uses the exact same arithmetic as `check_memory_circuit_breaker` so the
/// trip point is identical.
pub fn memory_envelope() -> Option<usize> {
    let cfg = get_config();
    if !cfg.common.memory_circuit_breaker_enabled || cfg.common.memory_circuit_breaker_ratio == 0 {
        return None;
    }
    Some(envelope_from(
        cfg.limit.mem_total,
        cfg.common.memory_circuit_breaker_ratio,
    ))
}

/// ratio% of mem_total, with the same integer arithmetic the breaker has
/// always used (`mem_total / 100 * ratio`).
pub fn envelope_from(mem_total: usize, ratio: usize) -> usize {
    mem_total / 100 * ratio
}

/// Pure admission decision: would admitting `projected` more bytes push
/// `current + reserved` past the envelope?
pub(crate) fn over_envelope(
    current: usize,
    reserved: usize,
    projected: usize,
    envelope: usize,
) -> bool {
    current.saturating_add(reserved).saturating_add(projected) > envelope
}

/// Try to reserve `projected` bytes against the memory envelope.
///
/// Returns the reservation guard on success. Fails with
/// `MemoryCircuitBreakerError` when the sampled process memory plus all
/// reservations (including this one) would exceed the envelope. When the
/// breaker is disabled the reservation always succeeds (still metered, for
/// observability).
pub fn try_reserve(projected: usize) -> Result<IngestReservation> {
    let cur_mem = metrics::NODE_MEMORY_USAGE
        .with_label_values::<&str>(&[])
        .get()
        .max(0) as usize;
    try_reserve_with(projected, memory_envelope(), cur_mem)
}

fn try_reserve_with(
    projected: usize,
    envelope: Option<usize>,
    cur_mem: usize,
) -> Result<IngestReservation> {
    if projected == 0 {
        return Ok(IngestReservation { bytes: 0 });
    }
    let projected_i64 = projected.min(i64::MAX as usize) as i64;

    let prev = RESERVED_BYTES.fetch_add(projected_i64, Ordering::AcqRel);
    if let Some(envelope) = envelope
        && over_envelope(cur_mem, prev.max(0) as usize, projected, envelope)
    {
        let after = RESERVED_BYTES.fetch_sub(projected_i64, Ordering::AcqRel) - projected_i64;
        metrics::INGEST_ADMISSION_RESERVED_BYTES
            .with_label_values::<&str>(&[])
            .set(after.max(0));
        return Err(Error::MemoryCircuitBreakerError {});
    }
    metrics::INGEST_ADMISSION_RESERVED_BYTES
        .with_label_values::<&str>(&[])
        .set(prev + projected_i64);
    Ok(IngestReservation {
        bytes: projected_i64,
    })
}

// ---- windowed rejection accounting -----------------------------------------
//
// Ingest rejections happen in storms (the breaker rejecting 100% of traffic
// for tens of seconds); per-request ERROR logging turns that into thousands
// of log lines. Per the hot-path log discipline: counts at info in windows,
// details at debug.

/// Rejection reason: Content-Length over the payload limit (or a projection
/// that can never fit the envelope). Maps to HTTP 413.
pub const REJECT_OVERSIZE: &str = "oversize";
/// Rejection reason: projected bytes do not currently fit the memory
/// envelope (pre-body admission). Maps to HTTP 503.
pub const REJECT_MEMORY: &str = "memory";
/// Rejection reason: resource backpressure discovered at write time (memory
/// breaker, disk breaker, memtable overflow, WAL queue full). Maps to 503.
pub const REJECT_RESOURCE: &str = "resource";

static REJECTED_OVERSIZE: AtomicU64 = AtomicU64::new(0);
static REJECTED_MEMORY: AtomicU64 = AtomicU64::new(0);
static REJECTED_RESOURCE: AtomicU64 = AtomicU64::new(0);
static LAST_SUMMARY_SECS: AtomicI64 = AtomicI64::new(0);
const SUMMARY_INTERVAL_SECS: i64 = 30;

/// Count one rejected ingest request. Increments the prometheus counter and
/// emits at most one info-level summary line per 30s window — never
/// per-request ERROR spam.
pub fn note_rejection(reason: &'static str) {
    metrics::INGEST_ADMISSION_REJECTED_TOTAL
        .with_label_values(&[reason])
        .inc();
    match reason {
        REJECT_OVERSIZE => REJECTED_OVERSIZE.fetch_add(1, Ordering::Relaxed),
        REJECT_MEMORY => REJECTED_MEMORY.fetch_add(1, Ordering::Relaxed),
        _ => REJECTED_RESOURCE.fetch_add(1, Ordering::Relaxed),
    };
    let now = chrono::Utc::now().timestamp();
    let last = LAST_SUMMARY_SECS.load(Ordering::Relaxed);
    if now - last >= SUMMARY_INTERVAL_SECS
        && LAST_SUMMARY_SECS
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    {
        let oversize = REJECTED_OVERSIZE.swap(0, Ordering::Relaxed);
        let memory = REJECTED_MEMORY.swap(0, Ordering::Relaxed);
        let resource = REJECTED_RESOURCE.swap(0, Ordering::Relaxed);
        log::info!(
            "[INGEST:ADMISSION] rejected {memory} requests at admission (memory envelope), {resource} at write (resource backpressure), {oversize} oversized, in last {SUMMARY_INTERVAL_SECS}s window"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    // The reservation counter and NODE_MEMORY_USAGE gauge are process-global;
    // serialize the tests that touch them.
    static TEST_LOCK: Mutex<()> = Mutex::new(());

    fn set_node_memory(v: i64) {
        metrics::NODE_MEMORY_USAGE
            .with_label_values::<&str>(&[])
            .set(v);
    }

    #[test]
    fn test_envelope_matches_breaker_arithmetic() {
        // must stay in lock-step with check_memory_circuit_breaker
        // (34359738368 / 100) * 90 with integer division
        assert_eq!(envelope_from(32 * 1024 * 1024 * 1024, 90), 30923764470);
        assert_eq!(envelope_from(100, 90), 90);
        assert_eq!(envelope_from(0, 90), 0);
    }

    #[test]
    fn test_over_envelope_identical_trip_point_with_zero_reserved() {
        // with nothing reserved the trip condition is exactly the breaker's
        // historical `cur_mem > envelope`
        let envelope = 1000;
        assert!(!over_envelope(1000, 0, 0, envelope));
        assert!(over_envelope(1001, 0, 0, envelope));
    }

    #[test]
    fn test_over_envelope_accounts_reserved_and_projected() {
        let envelope = 1000;
        assert!(!over_envelope(500, 200, 300, envelope));
        assert!(over_envelope(500, 200, 301, envelope));
        // saturation safety
        assert!(over_envelope(usize::MAX, usize::MAX, usize::MAX, envelope));
    }

    #[test]
    fn test_reserve_and_release() {
        let _guard = TEST_LOCK.lock().unwrap();
        set_node_memory(0);
        let base = reserved_bytes();
        {
            let r = try_reserve(1024).expect("reserve should succeed");
            assert_eq!(r.bytes(), 1024);
            assert_eq!(reserved_bytes(), base + 1024);
            {
                let r2 = try_reserve(2048).expect("second reserve should succeed");
                assert_eq!(reserved_bytes(), base + 1024 + 2048);
                drop(r2);
            }
            assert_eq!(reserved_bytes(), base + 1024);
        }
        assert_eq!(reserved_bytes(), base);
    }

    #[test]
    fn test_zero_reserve_is_free() {
        let _guard = TEST_LOCK.lock().unwrap();
        let base = reserved_bytes();
        let r = try_reserve(0).expect("zero reserve always succeeds");
        assert_eq!(r.bytes(), 0);
        assert_eq!(reserved_bytes(), base);
        drop(r);
        assert_eq!(reserved_bytes(), base);
    }

    #[test]
    fn test_reserve_rejects_over_envelope_and_rolls_back() {
        let _guard = TEST_LOCK.lock().unwrap();
        let base = reserved_bytes();
        let envelope = 10 * 1024 * 1024;

        // sampled memory sits right at the envelope: any projected bytes must
        // be rejected, and the counter must roll back
        let res = try_reserve_with(1024 * 1024, Some(envelope), envelope);
        assert!(matches!(res, Err(Error::MemoryCircuitBreakerError {})));
        assert_eq!(reserved_bytes(), base);

        // with headroom the same reservation succeeds and releases
        let r = try_reserve_with(1024 * 1024, Some(envelope), 0).expect("reserve with headroom");
        assert_eq!(reserved_bytes(), base + 1024 * 1024);
        drop(r);
        assert_eq!(reserved_bytes(), base);

        // outstanding reservations count against later admissions
        let r1 = try_reserve_with(6 * 1024 * 1024, Some(envelope), 0).expect("first reserve");
        let res = try_reserve_with(6 * 1024 * 1024, Some(envelope), 0);
        assert!(
            matches!(res, Err(Error::MemoryCircuitBreakerError {})),
            "second reserve must see the first reservation"
        );
        drop(r1);
        assert_eq!(reserved_bytes(), base);

        // breaker disabled (no envelope): reservations always succeed but are
        // still metered
        let r = try_reserve_with(1024, None, usize::MAX).expect("reserve with breaker disabled");
        assert_eq!(reserved_bytes(), base + 1024);
        drop(r);
        assert_eq!(reserved_bytes(), base);
    }

    #[test]
    fn test_breaker_trips_on_reservations() {
        // the public breaker check must count outstanding reservations: pin
        // via the same arithmetic path (reserved_bytes is what
        // check_memory_circuit_breaker adds to the sampled memory)
        let _guard = TEST_LOCK.lock().unwrap();
        let base = reserved_bytes();
        let envelope = 10 * 1024 * 1024;
        let r = try_reserve_with(8 * 1024 * 1024, Some(envelope), 0).expect("reserve");
        // sampled memory 4MB + reserved 8MB > 10MB envelope
        assert!(over_envelope(
            4 * 1024 * 1024,
            reserved_bytes(),
            0,
            envelope
        ));
        drop(r);
        // sampled memory 4MB alone stays under
        assert!(!over_envelope(
            4 * 1024 * 1024,
            reserved_bytes() - base,
            0,
            envelope
        ));
    }

    #[test]
    fn test_note_rejection_counts_metric() {
        note_rejection(REJECT_OVERSIZE);
        note_rejection(REJECT_MEMORY);
        note_rejection(REJECT_RESOURCE);
        assert!(
            metrics::INGEST_ADMISSION_REJECTED_TOTAL
                .with_label_values(&[REJECT_OVERSIZE])
                .get()
                >= 1
        );
        assert!(
            metrics::INGEST_ADMISSION_REJECTED_TOTAL
                .with_label_values(&[REJECT_MEMORY])
                .get()
                >= 1
        );
        assert!(
            metrics::INGEST_ADMISSION_REJECTED_TOTAL
                .with_label_values(&[REJECT_RESOURCE])
                .get()
                >= 1
        );
    }
}
