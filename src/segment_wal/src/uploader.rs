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

//! The segment flusher: buffer → encoded object → PUT → register
//! (DESIGN-SEGMENT-WAL.md).
//!
//! One flusher per process, one segment in flight at a time (seq order).
//! Taken frames are NEVER dropped while the node is online: PUT/register
//! retry forever while the next buffer keeps filling — the buffer cap
//! turning appends into 503s is the designed backpressure.

use std::{
    collections::BTreeSet,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use anyhow::{Context, anyhow};
use config::{cluster::LOCAL_NODE, utils::time::now_micros};
use infra::wal_segments::{SegmentMeta, SegmentStatus};

use crate::{
    buffer::global_buffer,
    format::{SegmentFrame, SegmentHeader, encode_segment},
};

/// Process-lifetime segment sequence, starting at 0 each boot. Combined with
/// the per-boot node uuid it makes object keys unique, ordered, and
/// retry-idempotent.
static SEQ: AtomicU64 = AtomicU64::new(0);

const RETRY_BACKOFF_SECS: [u64; 3] = [1, 2, 4];
const RETRY_STEADY_SECS: u64 = 30;
/// Extra attempts once the node is observed offline mid-retry, spaced 1s.
const OFFLINE_EXTRA_ATTEMPTS: u32 = 3;
/// Failed attempts after which a deterministic error (`InvalidFileMeta`)
/// stops retrying: the full 1s/2s/4s backoff has run by then, enough to rule
/// out a transiently misclassified failure.
const DETERMINISTIC_GIVE_UP_ATTEMPTS: usize = 4;

/// In-flight ship bound: encode/PUT/register of consecutive segments
/// overlap instead of serializing. Bounds in-flight encoded bytes to
/// roughly `SHIP_CONCURRENCY x` the flush size; segments are independent
/// (deterministic per-seq keys), so completion order is irrelevant.
const SHIP_CONCURRENCY: usize = 4;

static SHIP_PERMITS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(SHIP_CONCURRENCY);

/// Run the per-process flush loop.
///
/// THROUGHPUT CONTRACT (prod brownout, 2026-07-31): the loop must never
/// sleep while a flush trigger is already due — the original
/// sleep-then-ship-one-per-tick shape capped throughput at
/// `flush_size / (tick + ship_time)` (~16MB/s/node), below prod inbound,
/// and the buffer cap turned the shortfall into fleet-wide 503 storms.
/// Now: while a take is due, take and ship CONCURRENTLY (bounded by
/// [`SHIP_CONCURRENCY`]); sleep only when the buffer says nothing is due.
///
/// Shutdown: on offline, wait for in-flight ships, then one final
/// drain-and-ship. That drain is best effort — HTTP/gRPC can keep acking
/// after it; the AUTHORITATIVE last ship is main.rs calling [`flush_now`]
/// after both servers have fully stopped.
///
/// PUT and register retry with backoff inside each ship; keys are
/// deterministic per (boot, seq) so retries are idempotent. A ship that
/// ultimately fails has already screamed CRITICAL with the object key —
/// the loop keeps going (restarting the flusher would not recover frames).
/// Callers wrap this in a supervised spawn.
pub async fn run_flusher() -> Result<(), anyhow::Error> {
    log::info!(
        "[SEGMENT:FLUSH] flusher started, node_uuid={}, ship_concurrency={SHIP_CONCURRENCY}",
        LOCAL_NODE.uuid
    );
    loop {
        let cfg = config::get_config();
        let interval = Duration::from_millis(cfg.common.segment_flush_interval_ms);
        if config::cluster::is_offline() {
            // let in-flight ships finish, then drain what remains
            let _all = SHIP_PERMITS
                .acquire_many(SHIP_CONCURRENCY as u32)
                .await
                .expect("static semaphore is never closed");
            let frames = global_buffer().drain();
            if !frames.is_empty() {
                ship(frames).await?;
            }
            log::info!("[SEGMENT:FLUSH] node offline, flusher exiting after final drain");
            return Ok(());
        }
        let min_bytes = cfg.common.segment_flush_size_mb * 1024 * 1024;
        // acquire the permit BEFORE taking frames: when all ships are stuck
        // (object store outage) nothing is taken, the buffer fills, and the
        // cap 503s appends — backpressure lands on clients, not on taken
        // frames parked in memory.
        let permit = SHIP_PERMITS
            .acquire()
            .await
            .expect("static semaphore is never closed");
        match global_buffer().take_if(min_bytes, interval) {
            Some(frames) => {
                tokio::spawn(async move {
                    let _permit = permit;
                    if let Err(e) = ship(frames).await {
                        // already CRITICAL-logged inside ship with the key
                        log::error!("[SEGMENT:FLUSH] ship task failed: {e:#}");
                    }
                });
                // immediately check for more due work — no sleep with a
                // backlog standing
            }
            None => {
                drop(permit);
                tokio::time::sleep(interval).await;
            }
        }
    }
}

/// Drain everything buffered and ship it before returning. The ONLY caller
/// is main.rs's shutdown sequence (the authoritative last ship after
/// HTTP/gRPC have fully stopped — see [`run_flusher`]'s shutdown notes).
/// Waits for in-flight flusher ships first; once the node is offline,
/// [`retry_until_done`] gives up after a bounded number of extra attempts so
/// shutdown can finish.
pub async fn flush_now() -> Result<(), anyhow::Error> {
    let _all = SHIP_PERMITS
        .acquire_many(SHIP_CONCURRENCY as u32)
        .await
        .expect("static semaphore is never closed");
    let frames = global_buffer().drain();
    if frames.is_empty() {
        return Ok(());
    }
    ship(frames).await
}

/// Encode, upload, and register one segment built from `frames`.
///
/// Never called with an empty frame list (both `take_if` and the drain path
/// guarantee that), so the min/max fold identities below can never reach the
/// table.
async fn ship(frames: Vec<SegmentFrame>) -> Result<(), anyhow::Error> {
    let started = Instant::now();
    let seq = SEQ.fetch_add(1, Ordering::SeqCst);
    let header = SegmentHeader {
        node_uuid: LOCAL_NODE.uuid.clone(),
        seq,
        created_at: now_micros(),
    };
    let object_key = segment_object_key(&header.node_uuid, header.seq);

    let frame_count = frames.len();
    let (min_ts, max_ts, streams) = fold_frame_meta(&frames);
    let stream_count = streams.len();

    // zstd over up to the whole buffer cap — keep it off the async workers.
    // An encode failure is deterministic (retrying cannot fix it): the frames
    // are lost, so scream and propagate to the supervisor.
    let encode_header = header.clone();
    let bytes = match tokio::task::spawn_blocking(move || encode_segment(&encode_header, &frames))
        .await
    {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(e)) => {
            log::error!(
                "[SEGMENT:FLUSH] CRITICAL: encoding segment {object_key} failed, {frame_count} frames are LOST: {e:#}"
            );
            return Err(e.context(format!("encode segment {object_key}")));
        }
        Err(e) => {
            log::error!(
                "[SEGMENT:FLUSH] CRITICAL: encode task for segment {object_key} did not complete, {frame_count} frames are LOST: {e}"
            );
            return Err(anyhow!("encode task for segment {object_key} failed: {e}"));
        }
    };
    let size = bytes.len();
    let data = bytes::Bytes::from(bytes);

    // build the row before any side effect so conversion errors fail early
    let now = now_micros();
    let meta = SegmentMeta {
        id: 0, // assigned by the table on insert
        node_uuid: header.node_uuid.clone(),
        seq: i64::try_from(header.seq)
            .map_err(|_| anyhow!("segment {object_key}: seq {} overflows i64", header.seq))?,
        object_key: object_key.clone(),
        min_ts,
        max_ts,
        size: i64::try_from(size)
            .map_err(|_| anyhow!("segment {object_key}: size {size} overflows i64"))?,
        streams,
        status: SegmentStatus::Pending,
        builder_node: String::new(),
        created_at: now,
        updated_at: now,
    };

    // default/empty storage account — same as the WAL mover's uploads
    retry_until_done("PUT", &object_key, || {
        let object_key = object_key.clone();
        let data = data.clone();
        async move {
            infra::storage::put("", &object_key, data)
                .await
                .with_context(|| format!("put segment object {object_key}"))
        }
    })
    .await?;

    // wal_segments::add is idempotent on (node_uuid, seq), so retries after
    // a partial failure cannot create a second row
    retry_until_done("register", &object_key, || {
        let meta = meta.clone();
        async move {
            infra::wal_segments::add(&meta)
                .await
                .map(|_id| ())
                .with_context(|| format!("register segment object {}", meta.object_key))
        }
    })
    .await?;

    log::info!(
        "[SEGMENT:FLUSH] shipped segment seq={seq} frames={frame_count} streams={stream_count} bytes={size} took_ms={}",
        started.elapsed().as_millis()
    );
    Ok(())
}

/// Retry `op` with 1s,2s,4s backoff, then every 30s FOREVER while the node
/// is online (never drop a taken segment — appends 503ing at the buffer cap
/// is the designed backpressure), with two exits:
///
/// - a DETERMINISTIC failure — the error chain carries [`infra::errors::Error::InvalidFileMeta`],
///   our own pre-SQL registration validation — can never succeed on retry, so after
///   `DETERMINISTIC_GIVE_UP_ATTEMPTS` it logs CRITICAL with the object key and returns the error:
///   the supervisor restarts the flusher and ingest keeps flowing instead of wedging behind the
///   ship lock forever;
/// - once the node is observed offline, make `OFFLINE_EXTRA_ATTEMPTS` more attempts, then log
///   CRITICAL with the object key and give up so shutdown can finish.
async fn retry_until_done<F, Fut>(
    op: &str,
    object_key: &str,
    make_attempt: F,
) -> Result<(), anyhow::Error>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<(), anyhow::Error>>,
{
    let mut attempt: usize = 0;
    let mut offline_attempts_left: Option<u32> = None;
    loop {
        match make_attempt().await {
            Ok(()) => return Ok(()),
            Err(e) => {
                attempt += 1;
                if attempt >= DETERMINISTIC_GIVE_UP_ATTEMPTS && chain_has_invalid_file_meta(&e) {
                    log::error!(
                        "[SEGMENT:FLUSH] CRITICAL: {op} for segment object {object_key} fails deterministically (InvalidFileMeta) after {attempt} attempts — giving up, this segment's frames are LOST: {e:#}"
                    );
                    return Err(e.context(format!(
                        "{op} for segment object {object_key} failed deterministically (InvalidFileMeta)"
                    )));
                }
                log::error!(
                    "[SEGMENT:FLUSH] {op} failed for segment object {object_key} (attempt {attempt}), will retry: {e:#}"
                );
                if config::cluster::is_offline() {
                    let left = offline_attempts_left.get_or_insert(OFFLINE_EXTRA_ATTEMPTS);
                    if *left == 0 {
                        log::error!(
                            "[SEGMENT:FLUSH] CRITICAL: node is offline and {op} still fails for segment object {object_key} — giving up, this segment is NOT durable: {e:#}"
                        );
                        return Err(e.context(format!(
                            "{op} for segment object {object_key} failed through shutdown retries"
                        )));
                    }
                    *left -= 1;
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
                tokio::time::sleep(Duration::from_secs(retry_delay_secs(attempt))).await;
            }
        }
    }
}

/// Delay after the `attempt`-th failure (1-based): 1s, 2s, 4s, then 30s.
fn retry_delay_secs(attempt: usize) -> u64 {
    RETRY_BACKOFF_SECS
        .get(attempt - 1)
        .copied()
        .unwrap_or(RETRY_STEADY_SECS)
}

/// True when `e`'s chain carries [`infra::errors::Error::InvalidFileMeta`]
/// (the registration path's pre-SQL meta validation): the same meta fails
/// the same way forever, so retrying cannot succeed.
fn chain_has_invalid_file_meta(e: &anyhow::Error) -> bool {
    e.chain().any(|cause| {
        matches!(
            cause.downcast_ref::<infra::errors::Error>(),
            Some(infra::errors::Error::InvalidFileMeta(_))
        )
    })
}

fn segment_object_key(node_uuid: &str, seq: u64) -> String {
    format!("wal_segments/{node_uuid}/{seq:020}")
}

/// Fold registration metadata across the segment's frames: min/max ts (with
/// i64::MAX/MIN identities — never in-band "unset" markers) and the sorted,
/// deduped "org/stream_type/stream" identities.
///
/// `min_ts` is clamped to at least 1: registration validation rejects a
/// non-positive `min_ts` as degenerate, and one frame carrying a zero (epoch
/// default) or negative timestamp must not make the whole segment
/// unregistrable. Over-inclusive time-range pruning is harmless; a wedged
/// segment is not.
fn fold_frame_meta(frames: &[SegmentFrame]) -> (i64, i64, Vec<String>) {
    let mut min_ts = i64::MAX;
    let mut max_ts = i64::MIN;
    let mut streams = BTreeSet::new();
    for frame in frames {
        min_ts = min_ts.min(frame.min_ts);
        max_ts = max_ts.max(frame.max_ts);
        streams.insert(format!(
            "{}/{}/{}",
            frame.org, frame.stream_type, frame.stream
        ));
    }
    if min_ts <= 0 {
        log::warn!(
            "[SEGMENT:FLUSH] frame fold produced min_ts {min_ts} <= 0 across {} frames, clamping to 1 for registration",
            frames.len()
        );
        min_ts = 1;
    }
    (min_ts, max_ts, streams.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{array::Int64Array, record_batch::RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use config::meta::stream::StreamType;

    use super::*;

    fn frame(
        org: &str,
        stream_type: StreamType,
        stream: &str,
        min_ts: i64,
        max_ts: i64,
    ) -> SegmentFrame {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1i64]))]).unwrap();
        SegmentFrame {
            org: org.to_string(),
            stream_type,
            stream: stream.to_string(),
            min_ts,
            max_ts,
            batch,
        }
    }

    #[test]
    fn object_key_is_zero_padded_to_20() {
        assert_eq!(
            segment_object_key("node-a", 42),
            "wal_segments/node-a/00000000000000000042"
        );
        assert_eq!(
            segment_object_key("node-a", u64::MAX),
            "wal_segments/node-a/18446744073709551615"
        );
    }

    #[test]
    fn fold_meta_dedups_streams_and_keeps_positive_range() {
        let frames = vec![
            frame("org1", StreamType::Logs, "app1", 3, 20),
            frame("org1", StreamType::Logs, "app1", 10, 20),
            frame("org2", StreamType::Traces, "spans", 5, 7),
            frame("org1", StreamType::Metrics, "cpu", 3, 4),
        ];
        let (min_ts, max_ts, streams) = fold_frame_meta(&frames);
        // a positive fold is passed through unclamped
        assert_eq!(min_ts, 3);
        assert_eq!(max_ts, 20);
        // sorted and deduped
        assert_eq!(
            streams,
            vec![
                "org1/logs/app1".to_string(),
                "org1/metrics/cpu".to_string(),
                "org2/traces/spans".to_string(),
            ]
        );
    }

    #[test]
    fn fold_meta_clamps_non_positive_min_ts_to_one() {
        // one epoch-default frame among normal ones: the registration
        // validator rejects min_ts <= 0, so the fold must never emit it
        let frames = vec![
            frame("org1", StreamType::Logs, "app1", 0, 7),
            frame("org1", StreamType::Logs, "app1", 10, 20),
        ];
        assert_eq!(
            fold_frame_meta(&frames),
            (1, 20, vec!["org1/logs/app1".to_string()])
        );

        // negative folds clamp the same way
        let frames = vec![frame("org1", StreamType::Logs, "app1", -5, 20)];
        let (min_ts, max_ts, _) = fold_frame_meta(&frames);
        assert_eq!((min_ts, max_ts), (1, 20));

        // min_ts == 1 is already valid: no clamp
        let frames = vec![frame("org1", StreamType::Logs, "app1", 1, 2)];
        let (min_ts, ..) = fold_frame_meta(&frames);
        assert_eq!(min_ts, 1);
    }

    #[test]
    fn retry_delays_follow_contract() {
        assert_eq!(retry_delay_secs(1), 1);
        assert_eq!(retry_delay_secs(2), 2);
        assert_eq!(retry_delay_secs(3), 4);
        assert_eq!(retry_delay_secs(4), 30);
        assert_eq!(retry_delay_secs(100), 30);
    }

    fn invalid_file_meta_err(key: &str) -> anyhow::Error {
        // the exact shape ship() produces: the infra error wrapped in
        // with_context, so it sits mid-chain, not at the top
        anyhow::Error::new(infra::errors::Error::InvalidFileMeta(
            "degenerate time range [0, 0]".to_string(),
        ))
        .context(format!("register segment object {key}"))
    }

    #[test]
    fn invalid_file_meta_is_found_anywhere_in_the_chain() {
        assert!(chain_has_invalid_file_meta(&invalid_file_meta_err("k")));
        assert!(!chain_has_invalid_file_meta(&anyhow!("connection reset")));
        // other infra errors are NOT deterministic for this loop
        assert!(!chain_has_invalid_file_meta(
            &anyhow::Error::new(infra::errors::Error::Message("boom".to_string()))
                .context("register segment object k")
        ));
    }

    // start_paused: the 1s/2s/4s/30s backoff sleeps auto-advance, so these
    // finish in milliseconds of real time
    #[tokio::test(start_paused = true)]
    async fn retry_gives_up_on_invalid_file_meta_after_backoff() {
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = attempts.clone();
        let err = retry_until_done("register", "wal_segments/test-node/0", move || {
            let counter = counter.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Err(invalid_file_meta_err("wal_segments/test-node/0"))
            }
        })
        .await
        .unwrap_err();
        // the full initial backoff ran before classification kicked in
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            DETERMINISTIC_GIVE_UP_ATTEMPTS
        );
        let msg = format!("{err:#}");
        assert!(msg.contains("InvalidFileMeta"), "unexpected error: {msg}");
        assert!(
            msg.contains("wal_segments/test-node/0"),
            "unexpected error: {msg}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn retry_keeps_retrying_transient_errors_past_backoff() {
        // fails past the deterministic give-up threshold with a TRANSIENT
        // error, then succeeds — the loop must ride it out, never bail
        const TRANSIENT_FAILURES: usize = DETERMINISTIC_GIVE_UP_ATTEMPTS + 2;
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = attempts.clone();
        retry_until_done("PUT", "wal_segments/test-node/1", move || {
            let counter = counter.clone();
            async move {
                if counter.fetch_add(1, Ordering::SeqCst) < TRANSIENT_FAILURES {
                    Err(anyhow!("connection reset by peer"))
                } else {
                    Ok(())
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(attempts.load(Ordering::SeqCst), TRANSIENT_FAILURES + 1);
    }
}
