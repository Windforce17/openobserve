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

//! The per-node in-memory segment buffer (DESIGN-SEGMENT-WAL.md).
//!
//! One `std::sync::Mutex` guards the frame list and its byte accounting.
//! Everything expensive (arrow size computation, string allocation, and — on
//! the flusher side — encoding) happens OUTSIDE the lock, so hold times stay
//! tiny under ingest concurrency.

use std::{
    sync::{LazyLock, Mutex, MutexGuard},
    time::{Duration, Instant},
};

use arrow::record_batch::RecordBatch;
use config::{meta::stream::StreamType, utils::record_batch_ext::RecordBatchExt};

use crate::format::SegmentFrame;

/// Append rejection: the buffer is at `ZO_SEGMENT_BUFFER_MAX_MB` — callers
/// map this to a 503 (honest backpressure while object storage is slow).
#[derive(Debug)]
pub struct BufferFull {
    pub buffered_bytes: usize,
    pub max_bytes: usize,
}

impl std::fmt::Display for BufferFull {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "segment buffer full: {} of {} bytes — object storage flushes are behind",
            self.buffered_bytes, self.max_bytes
        )
    }
}

impl std::error::Error for BufferFull {}

/// Append rejection: the frame violates the segment format's encode bounds
/// (`format::encode_segment` writes u16 length prefixes for the identity
/// strings and a u32 row count). DETERMINISTIC — callers must fail the
/// request, never retry it: retrying re-sends the same over-limit field.
#[derive(Debug)]
pub struct UnencodableFrame {
    /// Which bound was violated: `"org"`, `"stream_type"`, `"stream"`, or
    /// `"rows"`.
    pub field: &'static str,
    pub len: usize,
    pub max: usize,
}

impl std::fmt::Display for UnencodableFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "frame is not encodable as a segment frame: {} length {} exceeds the format limit {}",
            self.field, self.len, self.max
        )
    }
}

impl std::error::Error for UnencodableFrame {}

/// Why [`SegmentBuffer::append`] rejected a frame. The two cases demand
/// opposite client treatment (retry vs fail), so they are distinct variants,
/// never one type with a sentinel field.
#[derive(Debug)]
pub enum AppendError {
    /// Buffer at cap — retryable, maps to 503.
    Full(BufferFull),
    /// Frame can never encode — deterministic per-request failure.
    Unencodable(UnencodableFrame),
}

impl std::fmt::Display for AppendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AppendError::Full(e) => e.fmt(f),
            AppendError::Unencodable(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for AppendError {}

impl From<BufferFull> for AppendError {
    fn from(e: BufferFull) -> Self {
        AppendError::Full(e)
    }
}

impl From<UnencodableFrame> for AppendError {
    fn from(e: UnencodableFrame) -> Self {
        AppendError::Unencodable(e)
    }
}

#[derive(Default)]
struct Inner {
    frames: Vec<SegmentFrame>,
    /// Per-frame arrow sizes, index-parallel with `frames` — computed at
    /// append (outside the lock) so `take_if`'s size-capped scan never does
    /// arrow walks while holding the mutex.
    frame_sizes: Vec<usize>,
    buffered_bytes: usize,
    /// When the oldest buffered frame arrived; None while empty (explicit,
    /// never a zero-instant sentinel).
    oldest: Option<Instant>,
    /// M31a LATE sub-buffer: frames whose hour partition is ≥
    /// `ZO_SEGMENT_LATE_LANE_HOURS` behind now accumulate here across flush
    /// ticks and ship as their OWN (all-late) segments — the thing that
    /// makes the builder's late-lane hold possible at whole-segment
    /// granularity. Same accounting shape as the main buffer; `late_bytes`
    /// counts toward the shared `ZO_SEGMENT_BUFFER_MAX_MB` cap so the 503
    /// backpressure contract is unchanged. Empty forever while the lane is
    /// off (append routes nothing here).
    late_frames: Vec<SegmentFrame>,
    late_frame_sizes: Vec<usize>,
    late_bytes: usize,
    late_oldest: Option<Instant>,
}

pub struct SegmentBuffer {
    inner: Mutex<Inner>,
}

static GLOBAL_BUFFER: LazyLock<SegmentBuffer> = LazyLock::new(SegmentBuffer::new);

/// Process-wide buffer instance shared by every ingest handler and the
/// flusher.
pub fn global_buffer() -> &'static SegmentBuffer {
    &GLOBAL_BUFFER
}

impl Default for SegmentBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl SegmentBuffer {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
        }
    }

    /// Every critical section below is panic-free (accounting arithmetic and
    /// Vec swaps only), so a poisoned mutex can only mean a bug elsewhere —
    /// recover the guard instead of taking the whole ingest path down.
    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Ack-on-append: this is the point after which the owner-accepted
    /// "≤ one flush interval on node crash" durability contract applies.
    /// Errors MUST reach the client as a failed request — never ack a failed
    /// append: a full buffer is retryable (503), an unencodable frame is a
    /// deterministic rejection.
    pub fn append(
        &self,
        org: &str,
        stream_type: StreamType,
        stream: &str,
        min_ts: i64,
        max_ts: i64,
        batch: RecordBatch,
    ) -> Result<(), AppendError> {
        // caps read per call so a config reload takes effect without restart
        let cfg = config::get_config();
        let max_bytes = cfg.common.segment_buffer_max_mb * 1024 * 1024;
        // M31a: a frame is one (stream, hour, schema) entry by construction
        // (ingestion buckets by hour partition key), so `max_ts` sits inside
        // the frame's hour — a frame at least `late_lane_hours` behind now
        // is a LATE frame, routed to the late sub-buffer. `>` on the hour
        // floor keeps hour-boundary rows (previous hour right after
        // rollover) on the fresh path at lane=2.
        let late = match cfg.common.segment_late_lane_hours {
            0 => false,
            lane_hours => {
                let cutoff = config::utils::time::now_micros()
                    - (lane_hours as i64).saturating_mul(3_600_000_000);
                max_ts < cutoff
            }
        };
        self.append_with_cap(
            org,
            stream_type,
            stream,
            min_ts,
            max_ts,
            batch,
            max_bytes,
            late,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn append_with_cap(
        &self,
        org: &str,
        stream_type: StreamType,
        stream: &str,
        min_ts: i64,
        max_ts: i64,
        batch: RecordBatch,
        max_bytes: usize,
        late: bool,
    ) -> Result<(), AppendError> {
        // a frame the encoder cannot represent must be rejected ALONE, here,
        // before it is buffered — once buffered it fails encode_segment for
        // every frame drained with it (up to the whole buffer cap)
        validate_encodable(org, stream_type, stream, &batch)?;
        // real arrow memory size (schema + arrays), computed OUTSIDE the lock
        let incoming = batch.size();
        let frame = SegmentFrame {
            org: org.to_string(),
            stream_type,
            stream: stream.to_string(),
            min_ts,
            max_ts,
            batch,
        };
        let mut inner = self.lock();
        // shared cap across BOTH sub-buffers: the 503 backpressure contract
        // is about total node memory, not which lane holds it
        if inner.buffered_bytes + inner.late_bytes + incoming > max_bytes {
            return Err(BufferFull {
                buffered_bytes: inner.buffered_bytes + inner.late_bytes,
                max_bytes,
            }
            .into());
        }
        if late {
            inner.late_bytes += incoming;
            if inner.late_oldest.is_none() {
                inner.late_oldest = Some(Instant::now());
            }
            inner.late_frames.push(frame);
            inner.late_frame_sizes.push(incoming);
        } else {
            inner.buffered_bytes += incoming;
            if inner.oldest.is_none() {
                inner.oldest = Some(Instant::now());
            }
            inner.frames.push(frame);
            inner.frame_sizes.push(incoming);
        }
        Ok(())
    }

    /// Swap out the accumulated frames when either trigger is due: buffered
    /// bytes ≥ `min_bytes`, or the oldest frame is older than `max_age`.
    /// Returns None when neither trigger has fired or the buffer is empty.
    pub fn take_if(&self, min_bytes: usize, max_age: Duration) -> Option<Vec<SegmentFrame>> {
        let mut inner = self.lock();
        if inner.frames.is_empty() {
            return None;
        }
        let size_due = inner.buffered_bytes >= min_bytes;
        let age_due = inner
            .oldest
            .map(|oldest| oldest.elapsed() >= max_age)
            .unwrap_or(false);
        if !size_due && !age_due {
            return None;
        }
        // Take AT MOST ~min_bytes per call, not the whole buffer: after a
        // brief ship stall the backlog could otherwise leave in ONE segment
        // of up to the buffer cap, and oversized segments blow the L0
        // builder's DataFusion pool downstream (prod OOM loop, 2026-07-31).
        // The flusher's drain loop calls straight back for the remainder, so
        // capping costs no throughput — it only bounds segment size. The
        // taken slice always contains at least one frame (a single frame may
        // exceed min_bytes; it still ships alone).
        let mut take_bytes = 0usize;
        let mut take_count = 0usize;
        for &fsize in inner.frame_sizes.iter() {
            if take_count > 0 && take_bytes + fsize > min_bytes {
                break;
            }
            take_bytes += fsize;
            take_count += 1;
        }
        if take_count == inner.frames.len() {
            inner.buffered_bytes = 0;
            inner.oldest = None;
            inner.frame_sizes.clear();
            Some(std::mem::take(&mut inner.frames))
        } else {
            let rest = inner.frames.split_off(take_count);
            let taken = std::mem::replace(&mut inner.frames, rest);
            let rest_sizes = inner.frame_sizes.split_off(take_count);
            inner.frame_sizes = rest_sizes;
            inner.buffered_bytes = inner.buffered_bytes.saturating_sub(take_bytes);
            // remaining frames keep their age; the oldest is now the first
            // of the remainder, whose true arrival we no longer know —
            // NOW is a conservative bound that can only delay, not lose
            inner.oldest = Some(Instant::now());
            Some(taken)
        }
    }

    /// M31a: swap out the LATE sub-buffer when either trigger is due —
    /// same contract as [`Self::take_if`] but over the late frames, and it
    /// always takes the WHOLE late set (late segments exist to be few and
    /// coalesced; size-capping them back into slivers would defeat the
    /// lane). Returns None when the lane is empty or neither trigger fired.
    pub fn take_late_if(&self, min_bytes: usize, max_age: Duration) -> Option<Vec<SegmentFrame>> {
        let mut inner = self.lock();
        if inner.late_frames.is_empty() {
            return None;
        }
        let size_due = inner.late_bytes >= min_bytes;
        let age_due = inner
            .late_oldest
            .map(|oldest| oldest.elapsed() >= max_age)
            .unwrap_or(false);
        if !size_due && !age_due {
            return None;
        }
        inner.late_bytes = 0;
        inner.late_oldest = None;
        inner.late_frame_sizes.clear();
        Some(std::mem::take(&mut inner.late_frames))
    }

    /// Unconditionally drain everything buffered (shutdown's final flush) —
    /// BOTH sub-buffers, main frames first: on graceful shutdown late rows
    /// keep the same durability as fresh ones.
    pub fn drain(&self) -> Vec<SegmentFrame> {
        let mut inner = self.lock();
        inner.buffered_bytes = 0;
        inner.oldest = None;
        inner.frame_sizes.clear();
        inner.late_bytes = 0;
        inner.late_oldest = None;
        inner.late_frame_sizes.clear();
        let mut frames = std::mem::take(&mut inner.frames);
        frames.append(&mut inner.late_frames);
        frames
    }

    pub fn buffered_bytes(&self) -> usize {
        let inner = self.lock();
        inner.buffered_bytes + inner.late_bytes
    }
}

/// The segment format's per-frame bounds (`format::write_data_frame`):
/// identity strings carry u16 length prefixes, the row count a u32. Mirrors
/// the encoder's `try_from` checks exactly so an accepted frame can never
/// fail encode on these fields.
fn validate_encodable(
    org: &str,
    stream_type: StreamType,
    stream: &str,
    batch: &RecordBatch,
) -> Result<(), UnencodableFrame> {
    const MAX_STR: usize = u16::MAX as usize;
    for (field, len) in [
        ("org", org.len()),
        ("stream_type", stream_type.as_str().len()),
        ("stream", stream.len()),
    ] {
        if len > MAX_STR {
            return Err(UnencodableFrame {
                field,
                len,
                max: MAX_STR,
            });
        }
    }
    let rows = batch.num_rows();
    if u32::try_from(rows).is_err() {
        return Err(UnencodableFrame {
            field: "rows",
            len: rows,
            max: u32::MAX as usize,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn batch(values: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values.to_vec()))]).unwrap()
    }

    fn append(buf: &SegmentBuffer, b: RecordBatch, cap: usize) -> Result<(), AppendError> {
        buf.append_with_cap("org1", StreamType::Logs, "app1", 1, 2, b, cap, false)
    }

    fn expect_full(err: AppendError) -> BufferFull {
        match err {
            AppendError::Full(e) => e,
            other => panic!("expected AppendError::Full, got {other:?}"),
        }
    }

    #[test]
    fn append_then_take_if_size_trigger() {
        let buf = SegmentBuffer::new();
        let b = batch(&[1, 2, 3]);
        let expected = b.size();
        append(&buf, b, usize::MAX).unwrap();
        assert_eq!(buf.buffered_bytes(), expected);

        // size not reached, age huge -> not due
        assert!(
            buf.take_if(expected + 1, Duration::from_secs(3600))
                .is_none()
        );
        assert_eq!(buf.buffered_bytes(), expected);

        // size reached -> due
        let frames = buf.take_if(expected, Duration::from_secs(3600)).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].org, "org1");
        assert_eq!(frames[0].stream_type, StreamType::Logs);
        assert_eq!(frames[0].stream, "app1");
        assert_eq!(buf.buffered_bytes(), 0);

        // now empty -> None even with zero thresholds
        assert!(buf.take_if(0, Duration::ZERO).is_none());
    }

    #[test]
    fn take_if_age_trigger() {
        let buf = SegmentBuffer::new();
        append(&buf, batch(&[1]), usize::MAX).unwrap();
        // young and small -> not due
        assert!(buf.take_if(usize::MAX, Duration::from_millis(80)).is_none());
        std::thread::sleep(Duration::from_millis(100));
        let frames = buf.take_if(usize::MAX, Duration::from_millis(80)).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(buf.buffered_bytes(), 0);
    }

    #[test]
    fn age_clock_resets_after_take() {
        let buf = SegmentBuffer::new();
        append(&buf, batch(&[1]), usize::MAX).unwrap();
        std::thread::sleep(Duration::from_millis(60));
        assert!(buf.take_if(usize::MAX, Duration::from_millis(50)).is_some());
        // a fresh append starts a fresh age window
        append(&buf, batch(&[2]), usize::MAX).unwrap();
        assert!(buf.take_if(usize::MAX, Duration::from_millis(50)).is_none());
    }

    /// M31a: late-routed frames live in the late sub-buffer — invisible to
    /// take_if, drained by take_late_if on its own size/age triggers, and
    /// still part of the shared cap + the unconditional drain.
    #[test]
    fn late_lane_routing_and_triggers() {
        let buf = SegmentBuffer::new();
        let b_fresh = batch(&[1]);
        let b_late = batch(&[2, 3]);
        let late_size = b_late.size();
        buf.append_with_cap("org1", StreamType::Logs, "app1", 1, 2, b_fresh, usize::MAX, false)
            .unwrap();
        buf.append_with_cap("org1", StreamType::Logs, "app1", 1, 2, b_late, usize::MAX, true)
            .unwrap();
        // main take never surfaces late frames
        let taken = buf.take_if(0, Duration::ZERO).expect("fresh frame due");
        assert_eq!(taken.len(), 1);
        // late lane: size trigger not met, age not met -> None
        assert!(buf.take_late_if(late_size + 1, Duration::from_secs(3600)).is_none());
        // size trigger met -> whole late set
        let late = buf.take_late_if(late_size, Duration::from_secs(3600)).unwrap();
        assert_eq!(late.len(), 1);
        assert_eq!(buf.buffered_bytes(), 0);
        // age trigger fires with tiny bytes
        buf.append_with_cap("org1", StreamType::Logs, "app1", 1, 2, batch(&[4]), usize::MAX, true)
            .unwrap();
        assert!(buf.take_late_if(usize::MAX, Duration::from_secs(3600)).is_none());
        std::thread::sleep(Duration::from_millis(30));
        assert!(buf.take_late_if(usize::MAX, Duration::from_millis(20)).is_some());
    }

    #[test]
    fn late_lane_shares_the_cap_and_the_drain() {
        let buf = SegmentBuffer::new();
        let b1 = batch(&[1, 2, 3, 4]);
        let size1 = b1.size();
        let cap = size1 + 8;
        buf.append_with_cap("org1", StreamType::Logs, "app1", 1, 2, b1, cap, true)
            .unwrap();
        // a fresh append is rejected against the SHARED cap
        let err = expect_full(
            buf.append_with_cap("org1", StreamType::Logs, "app1", 1, 2, batch(&[1, 2, 3, 4]), cap, false)
                .unwrap_err(),
        );
        assert_eq!(err.buffered_bytes, size1);
        // drain returns late frames too and zeroes all accounting
        assert_eq!(buf.drain().len(), 1);
        assert_eq!(buf.buffered_bytes(), 0);
        assert!(buf.take_late_if(0, Duration::ZERO).is_none());
    }

    #[test]
    fn drain_returns_everything_unconditionally() {
        let buf = SegmentBuffer::new();
        append(&buf, batch(&[1]), usize::MAX).unwrap();
        append(&buf, batch(&[2, 3]), usize::MAX).unwrap();
        let frames = buf.drain();
        assert_eq!(frames.len(), 2);
        assert_eq!(buf.buffered_bytes(), 0);
        assert!(buf.drain().is_empty());
    }

    #[test]
    fn cap_rejection_reports_exact_numbers() {
        let buf = SegmentBuffer::new();
        let b1 = batch(&[1, 2, 3, 4]);
        let size1 = b1.size();
        let cap = size1 + 8; // second identical batch cannot fit
        append(&buf, b1, cap).unwrap();

        let err = expect_full(append(&buf, batch(&[1, 2, 3, 4]), cap).unwrap_err());
        assert_eq!(err.buffered_bytes, size1);
        assert_eq!(err.max_bytes, cap);
        // rejected append must not change accounting
        assert_eq!(buf.buffered_bytes(), size1);

        // a batch bigger than the whole cap is rejected even when empty
        let empty = SegmentBuffer::new();
        let err = expect_full(append(&empty, batch(&[1, 2, 3, 4]), 1).unwrap_err());
        assert_eq!(err.buffered_bytes, 0);
        assert_eq!(err.max_bytes, 1);
        assert_eq!(empty.buffered_bytes(), 0);
    }

    #[test]
    fn public_append_enforces_configured_cap() {
        // default ZO_SEGMENT_BUFFER_MAX_MB is 128; build one batch bigger
        // than whatever the current config says and expect a BufferFull
        // carrying exactly that cap
        let max_bytes = config::get_config().common.segment_buffer_max_mb * 1024 * 1024;
        let values = vec![0i64; max_bytes / 8 + 1024];
        let big = batch(&values);
        assert!(big.size() > max_bytes);

        let buf = SegmentBuffer::new();
        let err = expect_full(
            buf.append("org1", StreamType::Logs, "app1", 1, 2, big)
                .unwrap_err(),
        );
        assert_eq!(err.buffered_bytes, 0);
        assert_eq!(err.max_bytes, max_bytes);
        assert_eq!(buf.buffered_bytes(), 0);

        // and a small batch is accepted through the same public path
        buf.append("org1", StreamType::Logs, "app1", 1, 2, batch(&[1]))
            .unwrap();
        assert!(buf.buffered_bytes() > 0);
    }

    #[test]
    fn append_rejects_oversized_identity_fields() {
        let max = u16::MAX as usize;
        let long = "x".repeat(max + 1);
        let buf = SegmentBuffer::new();

        // each over-limit identity field is a named Unencodable rejection and
        // buffers NOTHING — it must never reach a future encode_segment
        for (field, org, stream) in [("org", long.as_str(), "app1"), ("stream", "org1", &long)] {
            let err = buf
                .append_with_cap(
                    org,
                    StreamType::Logs,
                    stream,
                    1,
                    2,
                    batch(&[1]),
                    usize::MAX,
                    false,
                )
                .unwrap_err();
            match err {
                AppendError::Unencodable(e) => {
                    assert_eq!(e.field, field);
                    assert_eq!(e.len, max + 1);
                    assert_eq!(e.max, max);
                    let msg = e.to_string();
                    assert!(msg.contains(field), "msg: {msg}");
                    assert!(msg.contains("65535"), "msg: {msg}");
                }
                other => panic!("expected AppendError::Unencodable, got {other:?}"),
            }
            assert_eq!(buf.buffered_bytes(), 0);
        }
        assert!(buf.drain().is_empty());

        // exactly at the limit is accepted (the bound mirrors the encoder's
        // u16 try_from, which allows 65535)
        let at_limit = "x".repeat(max);
        buf.append_with_cap(
            &at_limit,
            StreamType::Logs,
            &at_limit,
            1,
            2,
            batch(&[1]),
            usize::MAX,
            false,
        )
        .unwrap();
        assert_eq!(buf.drain().len(), 1);
    }

    #[test]
    fn concurrent_appends_keep_accounting_exact() {
        const THREADS: usize = 8;
        const PER_THREAD: usize = 50;
        let buf = SegmentBuffer::new();
        let per_size = batch(&[1, 2, 3]).size();
        std::thread::scope(|scope| {
            for _ in 0..THREADS {
                scope.spawn(|| {
                    for _ in 0..PER_THREAD {
                        append(&buf, batch(&[1, 2, 3]), usize::MAX).unwrap();
                    }
                });
            }
        });
        assert_eq!(buf.buffered_bytes(), THREADS * PER_THREAD * per_size);
        let frames = buf.drain();
        assert_eq!(frames.len(), THREADS * PER_THREAD);
        assert_eq!(buf.buffered_bytes(), 0);
    }
}
