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

//! Querier-side scan of segment-WAL objects (DESIGN-SEGMENT-WAL.md).
//!
//! Recent, not-yet-built segments must be queryable with exactly-once
//! semantics. The leader seam ([`append_surviving`]) runs after the
//! file_list snapshot is fetched and appends the surviving segments as
//! pseudo-files; the follower seam ([`search`]) resolves and scans them.
//!
//! # Transport (documented loudly)
//!
//! The flight ticket (`proto::cluster_rpc::SearchInfo`) carries per-node
//! file assignments as `repeated int64 file_id_list` — a fixed proto we must
//! not regenerate. Segments therefore ride the SAME field as NEGATIVE ids:
//! `-wal_segments.id`. Real file_list ids are strictly positive (bigserial),
//! segment row ids are strictly positive, so the two id spaces cannot
//! collide and `0` belongs to neither. [`split_pseudo_ids`] separates them on
//! the follower, which resolves negatives against `wal_segments` instead of
//! `file_list`. The follower splits UNCONDITIONALLY (not gated on its own
//! `ingest_segment_mode`): a mixed-flag rollout must never silently drop a
//! leader-assigned segment — that would be silent partial data.
//!
//! Because segments enter the file id list BEFORE
//! `partition_file_lists`, they are partitioned across the same follower set
//! by the same assignment function as real files — each segment is scanned
//! exactly once.
//!
//! # Dup/gap rules (clock-skew independent — ORDERING is the invariant)
//!
//! - Candidates ([`list_candidates`]) are read BEFORE the file_list snapshot and include ONLY
//!   not-yet-Built segments. Built segments are always excluded: the builder commits the L0
//!   `batch_add` and `mark_built` in ONE fenced transaction (`mark_built_with_files`), so a segment
//!   observed Built has its L0 rows visible to the LATER file_list snapshot — no gap. (A grace
//!   window was tried instead and produced real double-counts the moment compaction merged an L0
//!   away while its segment was still inside the grace: e2e heal test, 2026-07-31.)
//! - A candidate observed Pending/Building whose build commits between the two reads would be
//!   served twice (segment + L0 rows in the snapshot). L0 filenames embed provenance for exactly
//!   this race: legacy/h1 keys encode inclusive segment-id ranges, while h2 keys encode the exact
//!   contributing ids. [`dedup_candidates`] drops only candidates covered by that provenance. Only
//!   snapshot provenance members may suppress a candidate — an L0 registered after the snapshot
//!   names data this query will not scan from files, so honoring it would open a gap.
//! - The ordering invariant additionally requires the `wal_segments` read and the file_list
//!   snapshot read to be CAUSALLY CONSISTENT. Both run on the RO pool, and `CLIENT_RO == CLIENT` by
//!   default: an empty `ZO_META_POSTGRES_RO_DSN` falls back to the RW DSN
//!   (`infra::db::postgres::connect`), so the invariant holds by default. Do NOT point
//!   `ZO_META_POSTGRES_RO_DSN` at an async replica with segment mode on — a lagging replica can
//!   serve a Built status whose L0 file_list rows have not replicated yet, reopening the gap this
//!   ordering closes.

use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    future::Future,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicBool, AtomicI64, Ordering},
    },
    time::{Duration, Instant},
};

use arrow::array::{BooleanArray, Int64Array};
use arrow_schema::{DataType, Schema};
use config::{
    TIMESTAMP_COL_NAME, get_config, is_local_disk_storage,
    meta::{search::ScanStats, stream::StreamType},
    metrics,
    utils::record_batch_ext::{RecordBatchExt, concat_batches},
};
use dashmap::{DashMap, mapref::entry::Entry};
use datafusion::{arrow::record_batch::RecordBatch, datasource::TableProvider};
use futures::{Stream, StreamExt, stream::BoxStream};
use hashbrown::{HashMap, HashSet};
use infra::{
    cache::file_data,
    errors::{Error, Result},
    file_list::{FileId, FileIdWithFile},
    l0_provenance::{L0Provenance, parse_l0_provenance},
    wal_segments::{self, SegmentMeta},
};

use crate::service::search::{
    datafusion::table_provider::memtable::NewMemTable,
    index::{Condition, IndexCondition},
    utils::AbortOnDrop,
};

/// Cap on segments one query ships per stream. Past this the builder is far
/// behind; scanning an unbounded segment list would make followers
/// fetch/decode without bound.
///
/// NON-FATAL BY DESIGN (prod, 2026-07-31): this cap used to ERROR, which
/// turned a builder backlog into a total query outage — every query
/// overlapping the backlog failed. Now the query serves the NEWEST
/// `MAX_QUERY_SEGMENTS` (recent data is what recent-window queries want)
/// plus every registered file, and reports the shortfall through the
/// standard partial-results channel: the caller receives a
/// [`SegmentShortfall`] and surfaces `is_partial` + a message naming how
/// many segments were skipped. Degraded and honest beats blacked out.
const MAX_QUERY_SEGMENTS: usize = 10_000;

/// How many segments a query had to skip because the backlog exceeded
/// [`MAX_QUERY_SEGMENTS`]. Empty (`total == 0`) on the healthy path.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct SegmentShortfall {
    pub skipped: usize,
    pub stream: String,
}

impl SegmentShortfall {
    pub fn message(&self) -> String {
        format!(
            "segment builder backlog: {} unbuilt segments of {} were skipped — results exclude the OLDEST unbuilt data for this range; it appears as builds land",
            self.skipped, self.stream
        )
    }
}

/// Byte budgets on the kept batches one follower accumulates for a single
/// query's segment scan, measured with [`RecordBatchExt::size`] — the same
/// arrow accounting the segment buffer caps on. [`MAX_QUERY_SEGMENTS`]
/// bounds only the segment COUNT; each segment can carry up to a whole
/// flush buffer of one stream's rows, so count alone does not bound
/// follower memory.
///
/// Returns `(soft, hard)`. Crossing the SOFT budget
/// (`ZO_SEGMENT_SCAN_MAX_BYTES`, default 512 MiB, 0 = no warning) logs one
/// warning and the query CONTINUES — recent-data queries must not fail
/// just because the stream is busy (owner call, 2026-08-11: a filtered
/// last-15min query died 0.03% over the old hard cap). The HARD ceiling —
/// half the pod's cgroup memory limit, never below the soft budget — still
/// fails the query loudly: past it the materialized backlog itself
/// endangers the pod (correctness over availability, same stance as
/// fetch/decode errors; a silently truncated scan would be silent partial
/// data).
fn segment_scan_budgets() -> (usize, usize) {
    let soft = get_config().limit.segment_scan_max_bytes;
    let hard = (config::utils::sysinfo::get_memory_limit() / 2).max(soft.max(1));
    (soft, hard)
}

/// Storage account segment objects live under — the flusher PUTs with the
/// default/empty account (see `segment_wal::uploader`).
const SEGMENT_STORAGE_ACCOUNT: &str = "";

fn choose_segment_cache_type(
    total_size: usize,
    memory_skip_size: Option<usize>,
    disk_skip_size: Option<usize>,
) -> file_data::CacheType {
    if memory_skip_size.is_some_and(|skip_size| total_size < skip_size) {
        file_data::CacheType::Memory
    } else if disk_skip_size.is_some_and(|skip_size| total_size < skip_size) {
        file_data::CacheType::Disk
    } else {
        file_data::CacheType::None
    }
}

fn segment_cache_type(metas: &[SegmentMeta]) -> file_data::CacheType {
    let total_size = metas.iter().fold(0usize, |total, meta| {
        total.saturating_add(usize::try_from(meta.size).unwrap_or(usize::MAX))
    });
    let cfg = get_config();
    choose_segment_cache_type(
        total_size,
        cfg.memory_cache
            .enabled
            .then_some(cfg.memory_cache.skip_size),
        (!is_local_disk_storage() && cfg.disk_cache.enabled).then_some(cfg.disk_cache.skip_size),
    )
}

async fn cache_segment(
    cache_type: file_data::CacheType,
    object_key: &str,
    bytes: bytes::Bytes,
) -> std::result::Result<(), anyhow::Error> {
    match cache_type {
        file_data::CacheType::Memory => file_data::memory::set(object_key, bytes).await,
        file_data::CacheType::Disk => file_data::disk::set(object_key, bytes).await,
        file_data::CacheType::None => Ok(()),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SegmentFetchSource {
    Memory,
    Disk,
    Remote,
    Coalesced,
}

struct SharedSegmentFetch {
    bytes: bytes::Bytes,
    source: SegmentFetchSource,
    cache_lookup: Duration,
    remote_fetch: Duration,
    cache_lookup_metric_claimed: AtomicBool,
    remote_metric_claimed: AtomicBool,
}

type SharedSegmentFetchResult = std::result::Result<Arc<SharedSegmentFetch>, Arc<str>>;
type SharedSegmentFetchCell = tokio::sync::OnceCell<SharedSegmentFetchResult>;

static INFLIGHT_SEGMENT_FETCHES: LazyLock<DashMap<String, Arc<SharedSegmentFetchCell>>> =
    LazyLock::new(DashMap::new);
/// Process-wide admission for CPU-heavy zstd/CRC/IPC decode work. Per-query
/// limits alone let repeated cancelled searches leave an unbounded number of
/// already-running `spawn_blocking` closures behind. Queued closures are
/// aborted on drop below; this gate bounds the closures that can be running.
static SEGMENT_DECODE_ADMISSION: LazyLock<Arc<tokio::sync::Semaphore>> = LazyLock::new(|| {
    Arc::new(tokio::sync::Semaphore::new(
        get_config().limit.cpu_num.max(1),
    ))
});

struct InflightSegmentFetchCleanup {
    key: String,
    cell: Arc<SharedSegmentFetchCell>,
    armed: bool,
}

impl Drop for InflightSegmentFetchCleanup {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // Hold the DashMap shard lock while proving that only the map and
        // this guard own an uninitialized cell. Otherwise a new waiter can
        // clone the cell between strong_count and remove, then race a second
        // initializer installed under the same key.
        if let Entry::Occupied(entry) = INFLIGHT_SEGMENT_FETCHES.entry(self.key.clone())
            && Arc::ptr_eq(entry.get(), &self.cell)
            && !entry.get().initialized()
            && Arc::strong_count(entry.get()) == 2
        {
            entry.remove();
        }
    }
}

fn remove_inflight_segment_fetch(key: &str, cell: &Arc<SharedSegmentFetchCell>) {
    if let Entry::Occupied(entry) = INFLIGHT_SEGMENT_FETCHES.entry(key.to_string())
        && Arc::ptr_eq(entry.get(), cell)
    {
        entry.remove();
    }
}

struct FetchedSegment {
    bytes: bytes::Bytes,
    source: SegmentFetchSource,
    cache_lookup: Duration,
    fetch_wait: Duration,
    remote_fetch: Duration,
}

struct SegmentFetchStats {
    source: SegmentFetchSource,
    compressed_size: i64,
    cache_lookup: Duration,
    fetch_wait: Duration,
    remote_fetch: Duration,
}

impl FetchedSegment {
    fn into_parts(self) -> (bytes::Bytes, SegmentFetchStats) {
        let compressed_size = self.bytes.len() as i64;
        (
            self.bytes,
            SegmentFetchStats {
                source: self.source,
                compressed_size,
                cache_lookup: self.cache_lookup,
                fetch_wait: self.fetch_wait,
                remote_fetch: self.remote_fetch,
            },
        )
    }
}
enum SegmentFetchWork {
    SkippedBeforeFetch,
    Fetched {
        meta: SegmentMeta,
        fetched: FetchedSegment,
        permit: tokio::sync::OwnedSemaphorePermit,
    },
}

enum SegmentDecodeWork {
    SkippedBeforeFetch,
    SkippedBeforeDecode {
        fetch_stats: SegmentFetchStats,
        blocking_queue: Duration,
    },
    Scanned {
        fetch_stats: SegmentFetchStats,
        permit: tokio::sync::OwnedSemaphorePermit,
        blocking_queue: Duration,
        decode: Duration,
        scanned: ScannedSegment,
    },
}

#[inline]
fn should_skip_by_top_n(can_skip_segments: bool, max_ts: i64, threshold: &AtomicI64) -> bool {
    can_skip_segments && max_ts < threshold.load(Ordering::Acquire)
}

#[derive(Default)]
struct SegmentScanTimings {
    memory_hits: u64,
    disk_hits: u64,
    remote_fetches: u64,
    coalesced_fetches: u64,
    cache_lookup_sum: Duration,
    cache_lookup_max: Duration,
    fetch_wait_sum: Duration,
    fetch_wait_max: Duration,
    remote_fetch_sum: Duration,
    remote_fetch_max: Duration,
    blocking_queue_sum: Duration,
    blocking_queue_max: Duration,
    decode_sum: Duration,
    decode_max: Duration,
    format_sum: Duration,
    format_max: Duration,
    condition_sum: Duration,
    condition_max: Duration,
    projection_sum: Duration,
    projection_max: Duration,
    frames_seen: u64,
    stream_frames: u64,
    time_frames: u64,
    ipc_frames: u64,
    top_n_skipped_frames: u64,
    exact_batches: u64,
    whole_batches: u64,
    dropped_batches: u64,
    exact_rows_after_condition: u64,
    whole_rows_retained: u64,
}

impl SegmentScanTimings {
    fn record_fetch(&mut self, fetched: &SegmentFetchStats) {
        match fetched.source {
            SegmentFetchSource::Memory => self.memory_hits += 1,
            SegmentFetchSource::Disk => self.disk_hits += 1,
            SegmentFetchSource::Remote => self.remote_fetches += 1,
            SegmentFetchSource::Coalesced => self.coalesced_fetches += 1,
        }
        self.cache_lookup_sum += fetched.cache_lookup;
        self.cache_lookup_max = self.cache_lookup_max.max(fetched.cache_lookup);
        self.fetch_wait_sum += fetched.fetch_wait;
        self.fetch_wait_max = self.fetch_wait_max.max(fetched.fetch_wait);
        self.remote_fetch_sum += fetched.remote_fetch;
        self.remote_fetch_max = self.remote_fetch_max.max(fetched.remote_fetch);
    }

    fn record_decode(&mut self, blocking_queue: Duration, decode: Duration) {
        self.blocking_queue_sum += blocking_queue;
        self.blocking_queue_max = self.blocking_queue_max.max(blocking_queue);
        self.decode_sum += decode;
        self.decode_max = self.decode_max.max(decode);
    }

    fn record_profiled_decode(
        &mut self,
        blocking_queue: Duration,
        decode: Duration,
        scanned: &ScannedSegment,
    ) {
        self.record_decode(blocking_queue, decode);
        self.condition_sum += scanned.condition_time;
        self.condition_max = self.condition_max.max(scanned.condition_time);
        self.projection_sum += scanned.projection_time;
        self.projection_max = self.projection_max.max(scanned.projection_time);
        let format = decode.saturating_sub(scanned.condition_time + scanned.projection_time);
        self.format_sum += format;
        self.format_max = self.format_max.max(format);
        self.frames_seen += scanned.frames_seen;
        self.stream_frames += scanned.stream_frames as u64;
        self.time_frames += scanned.time_frames;
        self.ipc_frames += scanned.ipc_frames;
        self.top_n_skipped_frames += scanned.top_n_skipped_frames;
        self.exact_batches += scanned.exact_batches;
        self.whole_batches += scanned.whole_batches;
        self.dropped_batches += scanned.dropped_batches;
        self.exact_rows_after_condition += scanned.exact_rows_after_condition;
        self.whole_rows_retained += scanned.whole_rows_retained;
    }

    fn apply_cache_stats(&self, scan_stats: &mut ScanStats) {
        scan_stats.querier_memory_cached_files += self.memory_hits as i64;
        scan_stats.querier_disk_cached_files += self.disk_hits as i64;
    }
}

fn record_segment_cache_outcome(
    org_id: &str,
    stream_type: StreamType,
    source: Option<SegmentFetchSource>,
) {
    let metric = if matches!(
        source,
        Some(SegmentFetchSource::Memory | SegmentFetchSource::Disk)
    ) {
        &metrics::QUERY_DISK_CACHE_HIT_COUNT
    } else {
        &metrics::QUERY_DISK_CACHE_MISS_COUNT
    };
    metric
        .with_label_values(&[org_id, stream_type.as_str(), "segment"])
        .inc();
}

async fn read_segment_cache(
    object_key: &str,
    expected_size: usize,
) -> Option<(bytes::Bytes, SegmentFetchSource)> {
    let cached = if let Some(bytes) = file_data::memory::get(object_key, None).await {
        Some((bytes, SegmentFetchSource::Memory))
    } else {
        file_data::disk::get(object_key, None)
            .await
            .map(|bytes| (bytes, SegmentFetchSource::Disk))
    };
    let (bytes, source) = cached?;
    if bytes.len() == expected_size {
        return Some((bytes, source));
    }
    log::warn!(
        "[SEGMENT:SCAN] cached segment object {object_key} has size {}, expected {expected_size}; evicting it before a verified remote read",
        bytes.len()
    );
    if let Err(e) = file_data::memory::remove(object_key).await {
        log::warn!("[SEGMENT:SCAN] could not evict {object_key} from memory cache: {e:#}");
    }
    if let Err(e) = file_data::disk::remove(object_key).await {
        log::warn!("[SEGMENT:SCAN] could not evict {object_key} from disk cache: {e:#}");
    }
    None
}
async fn fetch_segment(
    meta: &SegmentMeta,
    cache_type: file_data::CacheType,
) -> Result<FetchedSegment> {
    let expected_size = usize::try_from(meta.size)
        .ok()
        .filter(|size| *size > 0)
        .ok_or_else(|| {
            Error::Message(format!(
                "[SEGMENT:SCAN] segment object {} (id {}) has invalid expected size {}",
                meta.object_key, meta.id, meta.size
            ))
        })?;
    let fetch_started = Instant::now();
    let cache_started = Instant::now();
    if let Some((bytes, source)) = read_segment_cache(&meta.object_key, expected_size).await {
        return Ok(FetchedSegment {
            bytes,
            source,
            cache_lookup: cache_started.elapsed(),
            fetch_wait: fetch_started.elapsed(),
            remote_fetch: Duration::ZERO,
        });
    }
    let cache_lookup = cache_started.elapsed();

    let cell = match INFLIGHT_SEGMENT_FETCHES.entry(meta.object_key.clone()) {
        Entry::Occupied(entry) => Arc::clone(entry.get()),
        Entry::Vacant(entry) => {
            let cell = Arc::new(tokio::sync::OnceCell::new());
            entry.insert(Arc::clone(&cell));
            cell
        }
    };
    let mut cleanup = InflightSegmentFetchCleanup {
        key: meta.object_key.clone(),
        cell,
        armed: true,
    };
    let object_key = meta.object_key.clone();
    let shared = cleanup
        .cell
        .get_or_init(|| async {
            // A cache fill can win between the caller's first lookup and its
            // singleflight slot. Recheck before issuing the only remote GET.
            let recheck_started = Instant::now();
            let rechecked = read_segment_cache(&object_key, expected_size).await;
            let recheck_lookup = recheck_started.elapsed();
            if let Some((bytes, source)) = rechecked {
                return Ok(Arc::new(SharedSegmentFetch {
                    bytes,
                    source,
                    cache_lookup: recheck_lookup,
                    remote_fetch: Duration::ZERO,
                    cache_lookup_metric_claimed: AtomicBool::new(false),
                    remote_metric_claimed: AtomicBool::new(false),
                }));
            }

            let remote_started = Instant::now();
            let (_, bytes) = file_data::download_from_storage_exact(
                SEGMENT_STORAGE_ACCOUNT,
                &object_key,
                expected_size,
            )
            .await
            .map_err(|e| {
                Arc::<str>::from(format!(
                    "fetch segment object {object_key} from storage failed: {e:#}"
                ))
            })?;
            let remote_fetch = remote_started.elapsed();
            if let Err(e) = cache_segment(cache_type, &object_key, bytes.clone()).await {
                log::warn!(
                    "[SEGMENT:SCAN] fetched segment object {object_key} but could not populate the {cache_type:?} query cache: {e:#}"
                );
            }
            Ok(Arc::new(SharedSegmentFetch {
                bytes,
                source: SegmentFetchSource::Remote,
                cache_lookup: recheck_lookup,
                remote_fetch,
                cache_lookup_metric_claimed: AtomicBool::new(false),
                remote_metric_claimed: AtomicBool::new(false),
            }))
        })
        .await
        .clone();
    // Successful and failed initializations are both one-shot. Remove the
    // published cell before interpreting the result so a transient storage
    // failure cannot poison this object key forever.
    remove_inflight_segment_fetch(&cleanup.key, &cleanup.cell);
    cleanup.armed = false;
    let shared = shared.map_err(|e| {
        Error::Message(format!(
            "[SEGMENT:SCAN] fetch segment object {} (id {}) failed: {e}",
            meta.object_key, meta.id
        ))
    })?;

    let shared_cache_lookup = if !shared
        .cache_lookup_metric_claimed
        .swap(true, Ordering::AcqRel)
    {
        shared.cache_lookup
    } else {
        Duration::ZERO
    };
    let (source, remote_fetch) = if shared.source == SegmentFetchSource::Remote {
        if !shared.remote_metric_claimed.swap(true, Ordering::AcqRel) {
            (SegmentFetchSource::Remote, shared.remote_fetch)
        } else {
            (SegmentFetchSource::Coalesced, Duration::ZERO)
        }
    } else {
        (shared.source, Duration::ZERO)
    };
    Ok(FetchedSegment {
        bytes: shared.bytes.clone(),
        source,
        cache_lookup: cache_lookup + shared_cache_lookup,
        fetch_wait: fetch_started.elapsed(),
        remote_fetch,
    })
}

const SEGMENT_FETCH_PERMIT_BYTES: usize = 1024 * 1024;

fn segment_fetch_budget_permits() -> usize {
    let soft = get_config().limit.segment_scan_max_bytes;
    let bytes = if soft == 0 { 512 * 1024 * 1024 } else { soft };
    bytes.div_ceil(SEGMENT_FETCH_PERMIT_BYTES).max(1)
}

fn segment_permits(size: i64, budget_permits: usize) -> u32 {
    let bytes = usize::try_from(size).unwrap_or(SEGMENT_FETCH_PERMIT_BYTES);
    bytes
        .div_ceil(SEGMENT_FETCH_PERMIT_BYTES)
        .max(1)
        .min(budget_permits)
        .min(u32::MAX as usize) as u32
}

/// Keep a bounded stage continuously full and yield completed work without a
/// slow head blocking later ready items. Input admission remains newest-first
/// for segment Top-N scans; the threshold proof is order-independent.
fn rolling_stage<S, F, T>(stream: S, concurrency: usize) -> BoxStream<'static, T>
where
    S: Stream<Item = F> + Send + 'static,
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    stream.buffer_unordered(concurrency.max(1)).boxed()
}

// ---------------------------------------------------------------------------
// leader side
// ---------------------------------------------------------------------------

/// Leader seam, phase 1 — MUST run BEFORE the file_list snapshot query:
/// list this stream's not-yet-Built segments (module docs explain why the
/// ordering, not a grace window, is the dup/gap invariant). No-op unless
/// `ZO_INGEST_SEGMENT_MODE` is enabled.
pub async fn list_candidates(
    org_id: &str,
    stream_type: StreamType,
    stream_name: &str,
    time_range: (i64, i64),
) -> Result<(Vec<SegmentMeta>, Option<SegmentShortfall>)> {
    if !get_config().common.ingest_segment_mode {
        return Ok((Vec::new(), None));
    }
    // include_built_after = i64::MAX: Built segments are never candidates.
    // Read one page beyond the cap so an over-cap backlog is DETECTABLE
    // (rather than silently truncated at the SQL boundary), then keep the
    // newest page and report the rest as a shortfall.
    let candidates = wal_segments::query_unbuilt(
        org_id,
        stream_type.as_str(),
        stream_name,
        time_range,
        i64::MAX,
        MAX_QUERY_SEGMENTS + 1,
    )
    .await?;
    let stream = format!("{org_id}/{stream_type}/{stream_name}");
    let (candidates, shortfall) = apply_query_cap(candidates, &stream);
    if let Some(sf) = &shortfall {
        log::warn!("[SEGMENT:SCAN] {}", sf.message());
    }
    Ok((candidates, shortfall))
}

/// Keep the NEWEST [`MAX_QUERY_SEGMENTS`] candidates and report the rest as
/// a shortfall. Newest-first because a backlog means the builder is behind:
/// the recent end of the range is what recent-window queries ask for, and
/// the older tail becomes visible as builds land.
fn apply_query_cap(
    mut candidates: Vec<SegmentMeta>,
    stream: &str,
) -> (Vec<SegmentMeta>, Option<SegmentShortfall>) {
    if candidates.len() <= MAX_QUERY_SEGMENTS {
        return (candidates, None);
    }
    candidates.sort_unstable_by(|a, b| b.max_ts.cmp(&a.max_ts).then(b.id.cmp(&a.id)));
    let skipped = candidates.len() - MAX_QUERY_SEGMENTS;
    candidates.truncate(MAX_QUERY_SEGMENTS);
    (
        candidates,
        Some(SegmentShortfall {
            skipped,
            stream: stream.to_string(),
        }),
    )
}

/// Compact provenance coverage projected from the exact file-list snapshot a
/// query scans. Legacy/h1 ranges stay ranges; sparse h2 ids stay exact.
#[derive(Debug, Clone, Default)]
pub struct L0Coverage {
    ranges: Vec<(i64, i64)>,
    exact_ids: HashSet<i64>,
}

impl L0Coverage {
    pub fn range_count(&self) -> usize {
        self.ranges.len()
    }

    pub fn exact_id_count(&self) -> usize {
        self.exact_ids.len()
    }

    fn is_empty(&self) -> bool {
        self.ranges.is_empty() && self.exact_ids.is_empty()
    }

    fn insert(&mut self, provenance: L0Provenance, candidate_ids: &[i64]) {
        match provenance {
            L0Provenance::Range(min, max) => self.ranges.push((min, max)),
            L0Provenance::Exact(ids) => self.exact_ids.extend(
                ids.into_iter()
                    .filter(|id| candidate_ids.binary_search(id).is_ok()),
            ),
        }
    }

    fn compact(&mut self) {
        self.ranges.sort_unstable();
        let mut write = 0;
        for read in 0..self.ranges.len() {
            let (min, max) = self.ranges[read];
            if write > 0 && min <= self.ranges[write - 1].1.saturating_add(1) {
                self.ranges[write - 1].1 = self.ranges[write - 1].1.max(max);
            } else {
                self.ranges[write] = (min, max);
                write += 1;
            }
        }
        self.ranges.truncate(write);
        let ranges = &self.ranges;
        self.exact_ids.retain(|id| !range_contains(ranges, *id));
    }

    fn contains(&self, id: i64) -> bool {
        self.exact_ids.contains(&id) || range_contains(&self.ranges, id)
    }
}

fn range_contains(ranges: &[(i64, i64)], id: i64) -> bool {
    let index = ranges.partition_point(|&(min, _)| min <= id);
    index > 0 && id <= ranges[index - 1].1
}

/// Split one causally consistent file-list snapshot into the compact ids sent
/// to followers and the L0 provenance coverage used to suppress duplicate
/// segment candidates. Exact h2 coverage is intersected with the bounded
/// candidate set while decoding, so accumulated file provenance cannot expand
/// query-leader memory beyond [`MAX_QUERY_SEGMENTS`].
pub fn split_snapshot_file_ids(
    snapshot: Vec<FileIdWithFile>,
    candidates: &[SegmentMeta],
) -> (Vec<FileId>, L0Coverage) {
    let mut candidate_ids = candidates.iter().map(|meta| meta.id).collect::<Vec<_>>();
    candidate_ids.sort_unstable();
    candidate_ids.dedup();

    let mut files = Vec::with_capacity(snapshot.len());
    let mut coverage = L0Coverage::default();
    for row in snapshot {
        if let Some(provenance) = parse_l0_provenance(&row.file) {
            coverage.insert(provenance, &candidate_ids);
        }
        files.push(FileId {
            id: row.id,
            records: row.records,
            original_size: row.original_size,
            deleted: row.deleted,
        });
    }
    coverage.compact();
    (files, coverage)
}

/// Leader seam, phase 2 — runs AFTER the file_list snapshot is fetched:
/// dedup phase-1 candidates against that same snapshot's `l0_` provenance
/// and append the survivors as negative-id pseudo-files.
pub fn append_surviving(
    trace_id: &str,
    org_id: &str,
    stream_type: StreamType,
    stream_name: &str,
    candidates: Vec<SegmentMeta>,
    l0_coverage: &L0Coverage,
    files: &mut Vec<FileId>,
) -> Result<()> {
    if candidates.is_empty() {
        return Ok(());
    }

    let survivors = dedup_candidates(candidates, l0_coverage);
    if survivors.is_empty() {
        return Ok(());
    }
    if survivors.len() > MAX_QUERY_SEGMENTS {
        return Err(Error::Message(format!(
            "[SEGMENT:SCAN] {org_id}/{stream_type}/{stream_name}: {} queryable segments exceed the per-query cap {MAX_QUERY_SEGMENTS} — the segment builder backlog is too large to search safely",
            survivors.len()
        )));
    }
    log::info!(
        "[trace_id {trace_id}] segments_scan: {org_id}/{stream_type}/{stream_name} appending {} segments ({} L0 ranges and {} exact L0 ids in snapshot coverage) to the file id list",
        survivors.len(),
        l0_coverage.range_count(),
        l0_coverage.exact_id_count(),
    );
    files.reserve(survivors.len());
    for meta in &survivors {
        files.push(pseudo_file_id(meta)?);
    }
    Ok(())
}

/// Segment row -> negative-id pseudo `FileId`. `records` is 0 ("unknown", the
/// pre-existing file_list convention for unpopulated counts): a segment spans
/// streams, so the per-stream row count is unknowable leader-side.
fn pseudo_file_id(meta: &SegmentMeta) -> Result<FileId> {
    if meta.id <= 0 {
        return Err(Error::Message(format!(
            "[SEGMENT:SCAN] segment object {} has non-positive row id {} — cannot encode as a pseudo file id",
            meta.object_key, meta.id
        )));
    }
    Ok(FileId {
        id: -meta.id,
        records: 0,
        original_size: meta.size,
        deleted: false,
    })
}

/// Drop every candidate covered by registered L0 provenance — its rows are
/// already served by files the query scans. Exact h2 ids are looked up in a
/// hash set, so candidate checks allocate nothing and numeric gaps survive.
fn dedup_candidates(candidates: Vec<SegmentMeta>, coverage: &L0Coverage) -> Vec<SegmentMeta> {
    if coverage.is_empty() {
        return candidates;
    }
    candidates
        .into_iter()
        .filter(|candidate| !coverage.contains(candidate.id))
        .collect()
}

// ---------------------------------------------------------------------------
// follower side
// ---------------------------------------------------------------------------

/// Split a flight ticket's id list into (file_list ids, segment row ids).
/// Negative entries are segment pseudo-ids (see the module docs); the
/// returned segment ids are already negated back to their positive row ids.
pub fn split_pseudo_ids(ids: &[i64]) -> (Vec<i64>, Vec<i64>) {
    let mut file_ids = Vec::with_capacity(ids.len());
    let mut segment_ids = Vec::new();
    for &id in ids {
        if id < 0 {
            segment_ids.push(-id);
        } else {
            file_ids.push(id);
        }
    }
    (file_ids, segment_ids)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct SegmentTopNPlan {
    limit: usize,
    can_skip_segments: bool,
}

impl SegmentTopNPlan {
    pub(super) fn exact_desc(limit: usize) -> Option<Self> {
        (limit > 0).then_some(Self {
            limit,
            can_skip_segments: true,
        })
    }

    pub(super) fn trim_only(limit: usize) -> Option<Self> {
        (limit > 0).then_some(Self {
            limit,
            can_skip_segments: false,
        })
    }
}

/// Follower seam: scan this node's assigned segments and return tables the
/// existing union/exec path consumes alongside the storage tables.
///
/// Any failure — an id that no longer resolves, a fetch error, a decode
/// error — fails the WHOLE query: a silently missing segment is silent
/// partial data, the exact prod bug class this design exists to kill.
pub(super) async fn search(
    query: Arc<super::QueryParams>,
    schema: Arc<Schema>,
    plan_schema: Arc<Schema>,
    segment_ids: &[i64],
    sorted_by_time: bool,
    top_n_plan: Option<SegmentTopNPlan>,
    index_condition: Option<IndexCondition>,
    fst_fields: Vec<String>,
) -> Result<(Vec<Arc<dyn TableProvider>>, ScanStats)> {
    let trace_id = &query.trace_id;
    if segment_ids.is_empty() {
        return Ok((vec![], ScanStats::new()));
    }
    if segment_ids.len() > MAX_QUERY_SEGMENTS {
        return Err(Error::Message(format!(
            "[SEGMENT:SCAN] {}/{}/{}: assigned {} segments exceed the per-query cap {MAX_QUERY_SEGMENTS}",
            query.org_id,
            query.stream_type,
            query.stream_name,
            segment_ids.len()
        )));
    }
    let load_start = std::time::Instant::now();

    // Resolve the assigned ids against wal_segments. The leader already
    // range- and stream-filtered; resolution must find exactly the assigned
    // ids — a missing id is a hard error (silent partial data is the bug
    // class this path exists to prevent).
    let rows = wal_segments::get_by_ids(segment_ids).await?;
    let mut metas = resolve_assigned(segment_ids, rows, &query.org_id, &query.stream_name)?;
    let cache_type = segment_cache_type(&metas);

    // `ORDER BY _timestamp DESC LIMIT n` scans keep a running threshold from
    // in-window rows proven to satisfy the complete predicate. File metadata
    // stays newest-first for admission, while completion is unordered to
    // avoid a slow object blocking already-ready work.
    let mut top_n = top_n_plan.map(|plan| TopNTimestamps::new(plan.limit, query.time_range));
    if top_n.is_some() {
        metas.sort_by_key(|m| Reverse(m.max_ts));
    }
    let metas_len = metas.len();

    let mut scan_stats = ScanStats::new();
    scan_stats.files = metas_len as i64;
    scan_stats.querier_files = scan_stats.files;

    // check memory circuit breaker before decoding anything
    ingester::check_memory_circuit_breaker().map_err(|e| Error::ResourceError(e.to_string()))?;

    // The only columns any operator can read from these batches: the
    // projected plan schema (the downstream union table is built on it), the
    // condition columns (the memtable provider re-applies the filter), and
    // `_timestamp` (time pruning + sort order). Everything else is dead
    // weight the budget must not hold — an unconditioned count over a busy
    // live tail died at 512MB of kept bytes whose columns no operator would
    // ever touch, while the rows it needed amounted to one u32 per batch.
    let mut needed_columns: HashSet<String> = plan_schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    if let Some(condition) = index_condition.as_ref() {
        needed_columns.extend(condition.get_schema_fields(&fst_fields));
    }
    needed_columns.insert(TIMESTAMP_COL_NAME.to_string());

    // Fetch and decode are independent, bounded, unordered rolling stages.
    // Metas are admitted newest-first, but a slow head must not block later
    // cache hits or completed decodes. The exact Top-N threshold is
    // monotonic and order-independent; current and later stages recheck it
    // before paying remote/cache work, blocking decode, or frame IPC decode.
    //
    // Byte permits cover active reads, queued objects, and blocking decodes.
    // The permit enters spawn_blocking so cancellation cannot release its
    // accounting while an orphaned decode still retains the compressed bytes.
    let fetch_concurrency = get_config().common.segment_scan_fetch_concurrency.max(1);
    let decode_concurrency = get_config().common.segment_scan_decode_concurrency.max(1);
    let fetch_budget_permits = segment_fetch_budget_permits();
    let fetch_budget = Arc::new(tokio::sync::Semaphore::new(fetch_budget_permits));
    let can_skip_by_top_n = top_n_plan.is_some_and(|plan| plan.can_skip_segments);
    let top_n_threshold = Arc::new(AtomicI64::new(i64::MIN));
    let needed_columns = Arc::new(needed_columns);
    let scan_fst_fields = Arc::new(fst_fields.clone());
    let scan_condition = Arc::new(index_condition.clone());
    let metric_org_id = query.org_id.clone();
    let metric_stream_type = query.stream_type;

    let fetches = futures::stream::iter(metas.into_iter()).map({
        let fetch_budget = Arc::clone(&fetch_budget);
        let top_n_threshold = Arc::clone(&top_n_threshold);
        move |meta| {
            let fetch_budget = Arc::clone(&fetch_budget);
            let top_n_threshold = Arc::clone(&top_n_threshold);
            let org_id = metric_org_id.clone();
            async move {
                if should_skip_by_top_n(can_skip_by_top_n, meta.max_ts, &top_n_threshold) {
                    return Ok::<_, Error>(SegmentFetchWork::SkippedBeforeFetch);
                }
                let permit_count = segment_permits(meta.size, fetch_budget_permits);
                let permit = fetch_budget
                    .acquire_many_owned(permit_count)
                    .await
                    .map_err(|_| {
                        Error::Message(
                            "[SEGMENT:SCAN] compressed-byte fetch budget closed".to_string(),
                        )
                    })?;
                if should_skip_by_top_n(can_skip_by_top_n, meta.max_ts, &top_n_threshold) {
                    drop(permit);
                    return Ok(SegmentFetchWork::SkippedBeforeFetch);
                }
                let fetched = match fetch_segment(&meta, cache_type).await {
                    Ok(fetched) => {
                        record_segment_cache_outcome(
                            &org_id,
                            metric_stream_type,
                            Some(fetched.source),
                        );
                        fetched
                    }
                    Err(err) => {
                        record_segment_cache_outcome(&org_id, metric_stream_type, None);
                        return Err(err);
                    }
                };
                Ok(SegmentFetchWork::Fetched {
                    meta,
                    fetched,
                    permit,
                })
            }
        }
    });
    let channel_capacity = fetch_concurrency.max(decode_concurrency);
    let (fetch_tx, fetch_rx) =
        tokio::sync::mpsc::channel::<Result<SegmentFetchWork>>(channel_capacity);
    let producer_trace_id = query.trace_id.clone();
    let mut fetch_producer = AbortOnDrop::new(
        tokio::spawn(async move {
            let mut fetches = rolling_stage(fetches, fetch_concurrency);
            while let Some(result) = fetches.next().await {
                let failed = result.is_err();
                if fetch_tx.send(result).await.is_err() {
                    break;
                }
                if failed {
                    break;
                }
            }
            Ok::<_, Error>(())
        }),
        format!("{producer_trace_id}-segment-fetch"),
    );
    let fetched = futures::stream::unfold(fetch_rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    });

    let consumer_query = Arc::clone(&query);
    let segment_plan_schema = Arc::clone(&schema);
    let decoded_tasks = fetched.map({
        let needed_columns = Arc::clone(&needed_columns);
        let scan_fst_fields = Arc::clone(&scan_fst_fields);
        let scan_condition = Arc::clone(&scan_condition);
        let top_n_threshold = Arc::clone(&top_n_threshold);
        let segment_plan_schema = Arc::clone(&segment_plan_schema);
        move |result| {
            let query = Arc::clone(&consumer_query);
            let needed_columns = Arc::clone(&needed_columns);
            let scan_fst_fields = Arc::clone(&scan_fst_fields);
            let scan_condition = Arc::clone(&scan_condition);
            let top_n_threshold = Arc::clone(&top_n_threshold);
            let segment_plan_schema = Arc::clone(&segment_plan_schema);
            async move {
                let SegmentFetchWork::Fetched {
                    meta,
                    fetched,
                    permit,
                } = result?
                else {
                    return Ok::<_, Error>(SegmentDecodeWork::SkippedBeforeFetch);
                };
                let (bytes, fetch_stats) = fetched.into_parts();
                if should_skip_by_top_n(
                    can_skip_by_top_n,
                    meta.max_ts,
                    &top_n_threshold,
                ) {
                    drop(permit);
                    return Ok(SegmentDecodeWork::SkippedBeforeDecode {
                        fetch_stats,
                        blocking_queue: Duration::ZERO,
                    });
                }
                let decode_admission = Arc::clone(&SEGMENT_DECODE_ADMISSION)
                    .acquire_owned()
                    .await
                    .map_err(|_| {
                        Error::Message(
                            "[SEGMENT:SCAN] process-wide decode admission closed".to_string(),
                        )
                    })?;
                let object_key = meta.object_key.clone();
                let decode_task_name =
                    format!("{}-segment-decode-{}", query.trace_id, meta.id);
                let submitted = Instant::now();
                let blocking_handle = tokio::task::spawn_blocking(move || {
                    let _decode_admission = decode_admission;
                    let blocking_queue = submitted.elapsed();
                    if should_skip_by_top_n(
                        can_skip_by_top_n,
                        meta.max_ts,
                        &top_n_threshold,
                    ) {
                        return (
                            permit,
                            blocking_queue,
                            Duration::ZERO,
                            Ok::<_, Error>(None),
                        );
                    }
                    let decode_started = Instant::now();
                    let scanned = scan_segment_object(
                        &bytes,
                        &query.org_id,
                        query.stream_type,
                        &query.stream_name,
                        query.time_range,
                        scan_condition.as_ref().as_ref(),
                        &segment_plan_schema,
                        &scan_fst_fields,
                        &needed_columns,
                        can_skip_by_top_n.then_some(top_n_threshold.as_ref()),
                    )
                    .map(Some)
                    .map_err(|e| {
                        Error::Message(format!(
                            "[SEGMENT:SCAN] decode segment object {object_key} failed: {e:#}"
                        ))
                    });
                    (
                        permit,
                        blocking_queue,
                        decode_started.elapsed(),
                        scanned,
                    )
                });
                let mut blocking_decode = AbortOnDrop::new(blocking_handle, decode_task_name);
                let (permit, blocking_queue, decode, scanned) =
                    blocking_decode.join().await.map_err(|e| {
                        Error::Message(format!(
                            "[SEGMENT:SCAN] decode task for segment object {} (id {}) did not complete: {e}",
                            meta.object_key, meta.id
                        ))
                    })?;
                let Some(scanned) = scanned? else {
                    drop(permit);
                    return Ok(SegmentDecodeWork::SkippedBeforeDecode {
                        fetch_stats,
                        blocking_queue,
                    });
                };
                Ok(SegmentDecodeWork::Scanned {
                    fetch_stats,
                    permit,
                    blocking_queue,
                    decode,
                    scanned,
                })
            }
        }
    });
    let mut decoded = rolling_stage(decoded_tasks, decode_concurrency);

    let mut kept_exact_batches: Vec<RecordBatch> = Vec::new();
    let mut kept_deferred_batches: Vec<RecordBatch> = Vec::new();
    let mut kept_exact_bytes: usize = 0;
    let mut kept_deferred_bytes: usize = 0;
    let mut kept_bytes: usize = 0;
    let mut soft_budget_warned = false;
    let (scan_soft_budget, scan_hard_ceiling) = segment_scan_budgets();
    let mut zero_yield_stream_absent = 0usize;
    let mut zero_yield_time_pruned = 0usize;
    let mut zero_yield_top_n_pruned = 0usize;
    let mut timings = SegmentScanTimings::default();
    let mut skipped_before_fetch = 0usize;
    let mut skipped_before_decode = 0usize;
    while let Some(result) = decoded.next().await {
        let (fetch_stats, permit, blocking_queue, decode, scanned) = match result? {
            SegmentDecodeWork::SkippedBeforeFetch => {
                skipped_before_fetch += 1;
                continue;
            }
            SegmentDecodeWork::SkippedBeforeDecode {
                fetch_stats,
                blocking_queue,
            } => {
                timings.record_fetch(&fetch_stats);
                timings.record_decode(blocking_queue, Duration::ZERO);
                scan_stats.compressed_size += fetch_stats.compressed_size;
                skipped_before_decode += 1;
                continue;
            }
            SegmentDecodeWork::Scanned {
                fetch_stats,
                permit,
                blocking_queue,
                decode,
                scanned,
            } => (fetch_stats, permit, blocking_queue, decode, scanned),
        };
        timings.record_fetch(&fetch_stats);
        timings.record_profiled_decode(blocking_queue, decode, &scanned);
        scan_stats.compressed_size += fetch_stats.compressed_size;
        drop(permit);
        scan_stats.records += scanned.rows_examined;
        if scanned.stream_frames == 0 {
            zero_yield_stream_absent += 1;
        } else if scanned.time_frames == 0 {
            zero_yield_time_pruned += 1;
        } else if scanned.rows_examined == 0 && scanned.top_n_skipped_frames == scanned.time_frames
        {
            zero_yield_top_n_pruned += 1;
        }
        let threshold_before = top_n.as_ref().and_then(TopNTimestamps::threshold);
        if let Some(top) = top_n.as_mut() {
            for (is_exact, batch) in &scanned.kept {
                if *is_exact {
                    top.observe_batch(batch);
                }
            }
        }
        let top_state = top_n.as_ref().and_then(|top| {
            top.threshold()
                .map(|threshold| (threshold, top.window, top.n))
        });
        let threshold_advanced = top_state
            .is_some_and(|(threshold, ..)| threshold_before.is_none_or(|old| threshold > old));
        let mut appended_exact = false;
        for (is_exact, batch) in scanned.kept {
            // All Exact rows were observed above before any materialized batch
            // can trip the hard budget. Both classes may now discard rows
            // below the established threshold; Exact boundary ties are
            // globally capped below.
            let batch = if let Some((threshold, window, limit)) = top_state {
                trim_batch_to_threshold(batch, threshold, window, limit)?
            } else {
                Some(batch)
            };
            let Some(batch) = batch else {
                continue;
            };
            if is_exact {
                kept_exact_bytes = kept_exact_bytes.saturating_add(batch.size());
                kept_exact_batches.push(batch);
                appended_exact = true;
            } else {
                kept_deferred_bytes = kept_deferred_bytes.saturating_add(batch.size());
                kept_deferred_batches.push(batch);
            }
        }
        if let Some((threshold, window, limit)) = top_state {
            if threshold_advanced || appended_exact {
                compact_exact_top_n(
                    &mut kept_exact_batches,
                    &mut kept_exact_bytes,
                    threshold,
                    window,
                    limit,
                )?;
            }
            if threshold_advanced {
                trim_deferred_top_n(
                    &mut kept_deferred_batches,
                    &mut kept_deferred_bytes,
                    threshold,
                    window,
                    limit,
                )?;
            }
            top_n_threshold.store(threshold, Ordering::Release);
        }
        kept_bytes = kept_exact_bytes.saturating_add(kept_deferred_bytes);
        check_retained_budget(
            kept_bytes,
            &mut soft_budget_warned,
            scan_soft_budget,
            scan_hard_ceiling,
            &query.org_id,
            query.stream_type,
            &query.stream_name,
        )?;
        tokio::task::coop::consume_budget().await;
    }
    fetch_producer
        .join()
        .await
        .map_err(|e| Error::Message(format!("[SEGMENT:SCAN] fetch producer failed: {e}")))??;
    let skipped_by_top_n = skipped_before_fetch + skipped_before_decode;
    scan_stats.querier_files = (metas_len - skipped_before_fetch) as i64;
    timings.apply_cache_stats(&mut scan_stats);
    let mut kept_batches = kept_exact_batches;
    kept_batches.extend(kept_deferred_batches);
    // scan_size for the segment branch = the bytes the query actually HELD
    // after prune/project/trim (what the budget guarded). Summing decoded
    // batch capacities double-counted the shared IPC body buffer per batch
    // and reported hundreds of GB for a 15-second tail.
    scan_stats.original_size = kept_bytes as i64;

    log::info!(
        "[trace_id {trace_id}] segments_scan: {}/{}/{} loaded {} segments ({} skipped by top-n), kept {} batches, records {}, scan_size {}, zero-yield {} stream-absent + {} time-pruned, concurrency fetch/decode {}/{}, budget_mib {}, cache memory/disk/remote/coalesced {}/{}/{}/{}, phase_ms lookup sum/max {}/{}, fetch-wait sum/max {}/{}, remote sum/max {}/{}, blocking-queue sum/max {}/{}, decode sum/max {}/{}, took {} ms",
        query.org_id,
        query.stream_type,
        query.stream_name,
        metas_len - skipped_by_top_n,
        skipped_by_top_n,
        kept_batches.len(),
        scan_stats.records,
        scan_stats.original_size,
        zero_yield_stream_absent,
        zero_yield_time_pruned,
        fetch_concurrency,
        decode_concurrency,
        fetch_budget_permits * SEGMENT_FETCH_PERMIT_BYTES / (1024 * 1024),
        timings.memory_hits,
        timings.disk_hits,
        timings.remote_fetches,
        timings.coalesced_fetches,
        timings.cache_lookup_sum.as_millis(),
        timings.cache_lookup_max.as_millis(),
        timings.fetch_wait_sum.as_millis(),
        timings.fetch_wait_max.as_millis(),
        timings.remote_fetch_sum.as_millis(),
        timings.remote_fetch_max.as_millis(),
        timings.blocking_queue_sum.as_millis(),
        timings.blocking_queue_max.as_millis(),
        timings.decode_sum.as_millis(),
        timings.decode_max.as_millis(),
        load_start.elapsed().as_millis(),
    );
    log::info!(
        "[trace_id {trace_id}] segments_scan profile: skips before-fetch/decode {}/{}, zero-yield top-n objects {}, frames seen/stream/time/ipc/top-n-skipped {}/{}/{}/{}/{}, batches exact/whole/dropped {}/{}/{}, rows exact-after-condition/whole-retained {}/{}, phase_ms format sum/max {}/{}, condition sum/max {}/{}, projection sum/max {}/{}",
        skipped_before_fetch,
        skipped_before_decode,
        zero_yield_top_n_pruned,
        timings.frames_seen,
        timings.stream_frames,
        timings.time_frames,
        timings.ipc_frames,
        timings.top_n_skipped_frames,
        timings.exact_batches,
        timings.whole_batches,
        timings.dropped_batches,
        timings.exact_rows_after_condition,
        timings.whole_rows_retained,
        timings.format_sum.as_millis(),
        timings.format_max.as_millis(),
        timings.condition_sum.as_millis(),
        timings.condition_max.as_millis(),
        timings.projection_sum.as_millis(),
        timings.projection_max.as_millis(),
    );

    if kept_batches.is_empty() {
        return Ok((vec![], scan_stats));
    }

    let kept_batch_count = kept_batches.len();
    let table_build_start = Instant::now();
    let tables = build_tables_from_batches(
        trace_id,
        kept_batches,
        schema,
        sorted_by_time,
        index_condition,
        fst_fields,
        query.time_range,
    )?;
    log::info!(
        "[trace_id {trace_id}] segments_scan profile: table build {} batches -> {} tables took {} ms",
        kept_batch_count,
        tables.len(),
        table_build_start.elapsed().as_millis(),
    );
    Ok((tables, scan_stats))
}
/// Direct segment-WAL path for a no-filter histogram. Matching frames whose
/// exact bounds are wholly inside the half-open query window and one bucket
/// contribute their declared row count without parsing Arrow IPC. Only
/// window- or bucket-straddling frames decode, and decoded batches are
/// consumed immediately rather than retained or wrapped in a MemTable.
///
/// As with [`search`], every assigned id must resolve and every fetched
/// object must decompress and pass its frame CRCs. Any failure aborts the
/// whole query rather than returning a partial histogram.
pub async fn search_histogram(
    query: Arc<super::QueryParams>,
    segment_ids: &[i64],
    min_value: i64,
    bucket_width: u64,
    num_buckets: usize,
    ts_offset: i64,
) -> Result<(Vec<u64>, ScanStats)> {
    let trace_id = &query.trace_id;
    if segment_ids.len() > MAX_QUERY_SEGMENTS {
        return Err(Error::Message(format!(
            "[SEGMENT:SCAN] {}/{}/{}: assigned {} segments exceed the per-query cap {MAX_QUERY_SEGMENTS}",
            query.org_id,
            query.stream_type,
            query.stream_name,
            segment_ids.len()
        )));
    }
    ingester::check_memory_circuit_breaker().map_err(|e| Error::ResourceError(e.to_string()))?;
    let histogram_bytes = num_buckets
        .checked_mul(std::mem::size_of::<u64>())
        .ok_or_else(|| {
            Error::ResourceError(format!(
                "[SEGMENT:SCAN] histogram grid with {num_buckets} buckets overflows addressable memory"
            ))
        })?;
    let (_, hard_budget) = segment_scan_budgets();
    if histogram_bytes > hard_budget {
        return Err(Error::ResourceError(format!(
            "[SEGMENT:SCAN] histogram grid requires {histogram_bytes} bytes for {num_buckets} buckets, exceeding the per-query hard budget {hard_budget} bytes"
        )));
    }
    let mut histogram = Vec::new();
    histogram.try_reserve_exact(num_buckets).map_err(|e| {
        Error::ResourceError(format!(
            "[SEGMENT:SCAN] failed to reserve {histogram_bytes} bytes for {num_buckets} histogram buckets: {e}"
        ))
    })?;
    histogram.resize(num_buckets, 0u64);
    if segment_ids.is_empty() {
        return Ok((histogram, ScanStats::new()));
    }
    let load_start = std::time::Instant::now();

    let rows = wal_segments::get_by_ids(segment_ids).await?;
    let metas = resolve_assigned(segment_ids, rows, &query.org_id, &query.stream_name)?;
    let cache_type = segment_cache_type(&metas);

    let mut scan_stats = ScanStats::new();
    scan_stats.files = metas.len() as i64;
    scan_stats.querier_files = scan_stats.files;

    // Fetch and decode are separate bounded stages. The old fixed waves
    // waited for the slowest GET+decode before submitting any work from the
    // next wave, amplifying small-object latency. The channel keeps fetches
    // moving while at most `decode_concurrency` blocking tasks consume prior
    // results. Byte permits cover active reads, queued objects, and objects
    // being decoded, so higher fetch concurrency cannot grow memory without
    // bound. Queued blocking decodes abort on cancellation; already-running
    // decodes are bounded process-wide by [`SEGMENT_DECODE_ADMISSION`].
    let fetch_concurrency = get_config().common.segment_scan_fetch_concurrency.max(1);
    let decode_concurrency = get_config().common.segment_scan_decode_concurrency.max(1);
    let fetch_budget_permits = segment_fetch_budget_permits();
    let fetch_budget = Arc::new(tokio::sync::Semaphore::new(fetch_budget_permits));
    let channel_capacity = fetch_concurrency.max(decode_concurrency);
    let (tx, rx) = tokio::sync::mpsc::channel::<
        Result<(
            SegmentMeta,
            FetchedSegment,
            tokio::sync::OwnedSemaphorePermit,
        )>,
    >(channel_capacity);
    let metas_len = metas.len();
    let metric_org_id = query.org_id.clone();
    let metric_stream_type = query.stream_type;

    let producer = async move {
        let mut fetches = futures::stream::iter(metas.into_iter())
            .map(|meta| {
                let fetch_budget = Arc::clone(&fetch_budget);
                async move {
                    let permit_count = segment_permits(meta.size, fetch_budget_permits);
                    let permit = fetch_budget
                        .acquire_many_owned(permit_count)
                        .await
                        .map_err(|_| {
                            Error::Message(
                                "[SEGMENT:SCAN] compressed-byte fetch budget closed".to_string(),
                            )
                        })?;
                    let fetched = fetch_segment(&meta, cache_type).await?;
                    Ok::<_, Error>((meta, fetched, permit))
                }
            })
            .buffer_unordered(fetch_concurrency);
        while let Some(result) = fetches.next().await {
            match result.as_ref() {
                Ok((_, fetched, _)) => record_segment_cache_outcome(
                    &metric_org_id,
                    metric_stream_type,
                    Some(fetched.source),
                ),
                Err(_) => record_segment_cache_outcome(&metric_org_id, metric_stream_type, None),
            }
            let failed = result.is_err();
            if tx.send(result).await.is_err() {
                break;
            }
            if failed {
                break;
            }
        }
        Ok::<_, Error>(())
    };

    let consumer_query = Arc::clone(&query);
    let consumer = async {
        let received = futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        });
        let decoded = received
            .map(|result| {
                let query = Arc::clone(&consumer_query);
                async move {
                    let (meta, fetched, permit) = result?;
                    let (bytes, fetch_stats) = fetched.into_parts();
                    let decode_admission = Arc::clone(&SEGMENT_DECODE_ADMISSION)
                        .acquire_owned()
                        .await
                        .map_err(|_| {
                            Error::Message(
                                "[SEGMENT:SCAN] process-wide decode admission closed".to_string(),
                            )
                        })?;
                    let object_key = meta.object_key.clone();
                    let decode_task_name =
                        format!("{}-segment-histogram-decode-{}", query.trace_id, meta.id);
                    let submitted = Instant::now();
                    let blocking_handle = tokio::task::spawn_blocking(move || {
                        let _decode_admission = decode_admission;
                        // The permit enters the blocking closure so query
                        // cancellation cannot release byte accounting while a
                        // running decode still retains `bytes`.
                        let blocking_queue = submitted.elapsed();
                        let decode_started = Instant::now();
                        let scanned = scan_segment_histogram(
                            &bytes,
                            &query.org_id,
                            query.stream_type,
                            &query.stream_name,
                            query.time_range,
                            min_value,
                            bucket_width,
                            num_buckets,
                            ts_offset,
                        )
                        .map_err(|e| {
                            Error::Message(format!(
                                "[SEGMENT:SCAN] decode segment object {object_key} failed: {e:#}"
                            ))
                        });
                        (
                            permit,
                            blocking_queue,
                            decode_started.elapsed(),
                            scanned,
                        )
                    });
                    let mut blocking_decode =
                        AbortOnDrop::new(blocking_handle, decode_task_name);
                    let (permit, blocking_queue, decode, scanned) =
                        blocking_decode.join().await.map_err(|e| {
                            Error::Message(format!(
                                "[SEGMENT:SCAN] decode task for segment object {} (id {}) did not complete: {e}",
                                meta.object_key, meta.id
                            ))
                        })?;
                    Ok::<_, Error>((fetch_stats, permit, blocking_queue, decode, scanned?))
                }
            })
            .buffer_unordered(decode_concurrency);
        futures::pin_mut!(decoded);

        let mut timings = SegmentScanTimings::default();
        while let Some(result) = decoded.next().await {
            let (fetch_stats, permit, blocking_queue, decode, scanned) = result?;
            timings.record_fetch(&fetch_stats);
            timings.record_decode(blocking_queue, decode);
            scan_stats.compressed_size += fetch_stats.compressed_size;
            drop(permit);
            scan_stats.records += scanned.rows_examined;
            for (bucket, count) in scanned.histogram {
                let Some(total) = histogram.get_mut(bucket) else {
                    return Err(Error::Message(format!(
                        "[SEGMENT:SCAN] decoded histogram bucket {bucket} exceeds grid length {num_buckets}"
                    )));
                };
                *total = total.checked_add(count).ok_or_else(|| {
                    Error::Message("[SEGMENT:SCAN] histogram count overflow".to_string())
                })?;
            }
            tokio::task::coop::consume_budget().await;
        }
        Ok::<_, Error>(timings)
    };

    let (_, timings) = tokio::try_join!(producer, consumer)?;
    timings.apply_cache_stats(&mut scan_stats);

    log::info!(
        "[trace_id {trace_id}] segments_scan histogram: {}/{}/{} loaded {} segments, records {}, compressed_size {}, concurrency fetch/decode {}/{}, budget_mib {}, cache memory/disk/remote/coalesced {}/{}/{}/{}, phase_ms lookup sum/max {}/{}, fetch-wait sum/max {}/{}, remote sum/max {}/{}, blocking-queue sum/max {}/{}, decode sum/max {}/{}, took {} ms",
        query.org_id,
        query.stream_type,
        query.stream_name,
        metas_len,
        scan_stats.records,
        scan_stats.compressed_size,
        fetch_concurrency,
        decode_concurrency,
        fetch_budget_permits * SEGMENT_FETCH_PERMIT_BYTES / (1024 * 1024),
        timings.memory_hits,
        timings.disk_hits,
        timings.remote_fetches,
        timings.coalesced_fetches,
        timings.cache_lookup_sum.as_millis(),
        timings.cache_lookup_max.as_millis(),
        timings.fetch_wait_sum.as_millis(),
        timings.fetch_wait_max.as_millis(),
        timings.remote_fetch_sum.as_millis(),
        timings.remote_fetch_max.as_millis(),
        timings.blocking_queue_sum.as_millis(),
        timings.blocking_queue_max.as_millis(),
        timings.decode_sum.as_millis(),
        timings.decode_max.as_millis(),
        load_start.elapsed().as_millis(),
    );
    Ok((histogram, scan_stats))
}

#[derive(Debug)]
struct ScannedHistogram {
    histogram: HashMap<usize, u64>,
    rows_examined: i64,
    #[cfg(test)]
    decoded_frames: usize,
}

/// Consume one segment into sparse histogram counters. `decode_segment_filtered`
/// still walks and CRC-checks every frame; returning false merely avoids IPC
/// parsing for irrelevant or whole-frame-folded data.
#[allow(clippy::too_many_arguments)]
fn scan_segment_histogram(
    bytes: &[u8],
    org_id: &str,
    stream_type: StreamType,
    stream_name: &str,
    time_range: (i64, i64),
    min_value: i64,
    bucket_width: u64,
    num_buckets: usize,
    ts_offset: i64,
) -> anyhow::Result<ScannedHistogram> {
    // Validate the shared grid arithmetic once. The selector cannot return
    // an error, so all subsequent range-helper calls are then infallible.
    if num_buckets != 0 {
        let _ = ::search::vix::histogram_bucket(
            min_value,
            min_value,
            bucket_width,
            num_buckets,
            ts_offset,
        )?;
    }

    let histogram = std::cell::RefCell::new(HashMap::<usize, u64>::new());
    let rows_examined = std::cell::Cell::new(0i64);
    let selector_error = std::cell::RefCell::new(None::<anyhow::Error>);
    #[cfg(test)]
    let decoded_frames = std::cell::Cell::new(0usize);

    segment_wal::format::decode_segment_filtered(
        bytes,
        |info| {
            if selector_error.borrow().is_some()
                || info.org != org_id
                || info.stream_type != stream_type
                || info.stream != stream_name
                || !frame_time_overlaps_half_open(info.min_ts, info.max_ts, time_range)
            {
                return false;
            }

            let Some(rows) = rows_examined.get().checked_add(i64::from(info.rows)) else {
                *selector_error.borrow_mut() =
                    Some(anyhow::anyhow!("segment histogram row count overflow"));
                return false;
            };
            rows_examined.set(rows);

            if frame_fully_inside_half_open(info.min_ts, info.max_ts, time_range) {
                match ::search::vix::histogram_range_bucket(
                    info.min_ts,
                    info.max_ts,
                    min_value,
                    bucket_width,
                    num_buckets,
                    ts_offset,
                ) {
                    Ok(Some(bucket)) => {
                        let mut counts = histogram.borrow_mut();
                        let count = counts.entry(bucket).or_default();
                        let Some(next) = count.checked_add(u64::from(info.rows)) else {
                            *selector_error.borrow_mut() =
                                Some(anyhow::anyhow!("segment histogram bucket count overflow"));
                            return false;
                        };
                        *count = next;
                        return false;
                    }
                    Ok(None) => {}
                    Err(err) => {
                        *selector_error.borrow_mut() = Some(err);
                        return false;
                    }
                }
            }
            true
        },
        |frame| {
            #[cfg(test)]
            decoded_frames.set(decoded_frames.get() + 1);
            let timestamps = frame
                .batch
                .column_by_name(TIMESTAMP_COL_NAME)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "stream {}/{}/{}: decoded histogram frame lacks {TIMESTAMP_COL_NAME}",
                        frame.org,
                        frame.stream_type,
                        frame.stream
                    )
                })?
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "stream {}/{}/{}: decoded histogram frame {TIMESTAMP_COL_NAME} is not Int64",
                        frame.org,
                        frame.stream_type,
                        frame.stream
                    )
                })?;
            let mut counts = histogram.borrow_mut();
            for timestamp in timestamps.iter().flatten() {
                if !timestamp_inside_half_open(timestamp, time_range) {
                    continue;
                }
                if let Some(bucket) = ::search::vix::histogram_bucket(
                    timestamp,
                    min_value,
                    bucket_width,
                    num_buckets,
                    ts_offset,
                )? {
                    let count = counts.entry(bucket).or_default();
                    *count = count.checked_add(1).ok_or_else(|| {
                        anyhow::anyhow!("segment histogram bucket count overflow")
                    })?;
                }
            }
            Ok(())
        },
    )?;

    if let Some(err) = selector_error.into_inner() {
        return Err(err);
    }
    Ok(ScannedHistogram {
        histogram: histogram.into_inner(),
        rows_examined: rows_examined.get(),
        #[cfg(test)]
        decoded_frames: decoded_frames.get(),
    })
}

/// Exact overlap with the query's half-open `[start, end)` row semantics.
/// `(0, 0)` retains the existing segment-scan convention of no time bound.
fn frame_time_overlaps_half_open(frame_min: i64, frame_max: i64, time_range: (i64, i64)) -> bool {
    time_range == (0, 0)
        || (time_range.0 < time_range.1 && frame_max >= time_range.0 && frame_min < time_range.1)
}

fn frame_fully_inside_half_open(frame_min: i64, frame_max: i64, time_range: (i64, i64)) -> bool {
    time_range == (0, 0)
        || (time_range.0 < time_range.1 && frame_min >= time_range.0 && frame_max < time_range.1)
}

fn timestamp_inside_half_open(timestamp: i64, time_range: (i64, i64)) -> bool {
    time_range == (0, 0)
        || (time_range.0 < time_range.1 && timestamp >= time_range.0 && timestamp < time_range.1)
}

/// One scanned segment object's contribution: rows examined (pre-prune, for
/// stats) and the kept batches, each flagged `is_exact` (its surviving rows
/// are KNOWN condition matches — see [`PrunedBatch`]) — trim-eligible
/// downstream.
#[derive(Debug)]
struct ScannedSegment {
    rows_examined: i64,
    kept: Vec<(bool, RecordBatch)>,
    /// Frames whose STREAM IDENTITY matched, before time pruning — the
    /// classifier for zero-yield objects: 0 here means the object never
    /// carried the stream (registry over-match / builder carry), >0 with
    /// zero examined rows means its frames missed the query window
    /// (object-level time bounds are coarser than per-stream ones).
    stream_frames: i64,
    frames_seen: u64,
    time_frames: u64,
    ipc_frames: u64,
    top_n_skipped_frames: u64,
    exact_batches: u64,
    whole_batches: u64,
    dropped_batches: u64,
    exact_rows_after_condition: u64,
    whole_rows_retained: u64,
    condition_time: Duration,
    projection_time: Duration,
}

fn predicate_data_types_equivalent(batch_type: &DataType, plan_type: &DataType) -> bool {
    batch_type == plan_type
        || (matches!(
            batch_type,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        ) && matches!(
            plan_type,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        ))
}

/// Streaming scan of one segment object: inspect each frame's stream identity
/// and time range before IPC parsing, then run kept frames
/// through condition prune + plan projection immediately, so peak memory is
/// one frame plus its post-projection remnant — never the whole payload.
#[allow(clippy::too_many_arguments)]
fn scan_segment_object(
    bytes: &[u8],
    org_id: &str,
    stream_type: StreamType,
    stream_name: &str,
    time_range: (i64, i64),
    condition: Option<&IndexCondition>,
    plan_schema: &Schema,
    fst_fields: &[String],
    needed_columns: &HashSet<String>,
    top_n_threshold: Option<&AtomicI64>,
) -> anyhow::Result<ScannedSegment> {
    let mut out = ScannedSegment {
        rows_examined: 0,
        kept: Vec::new(),
        stream_frames: 0,
        frames_seen: 0,
        time_frames: 0,
        ipc_frames: 0,
        top_n_skipped_frames: 0,
        exact_batches: 0,
        whole_batches: 0,
        dropped_batches: 0,
        exact_rows_after_condition: 0,
        whole_rows_retained: 0,
        condition_time: Duration::ZERO,
        projection_time: Duration::ZERO,
    };
    let mut frames_seen = 0u64;
    let mut stream_frames = 0i64;
    let mut time_frames = 0u64;
    let mut top_n_skipped_frames = 0u64;
    segment_wal::format::decode_segment_filtered(
        bytes,
        |info| {
            frames_seen += 1;
            let identity =
                info.org == org_id && info.stream_type == stream_type && info.stream == stream_name;
            if !identity {
                return false;
            }
            stream_frames += 1;
            if !frame_time_overlaps_half_open(info.min_ts, info.max_ts, time_range) {
                return false;
            }
            time_frames += 1;
            if top_n_threshold
                .is_some_and(|threshold| info.max_ts < threshold.load(Ordering::Acquire))
            {
                top_n_skipped_frames += 1;
                return false;
            }
            true
        },
        |frame| {
            out.ipc_frames += 1;
            let batch = frame.batch;
            if batch.num_rows() == 0 {
                return Ok(());
            }
            out.rows_examined += batch.num_rows() as i64;
            // Evaluate only conjuncts whose write-time types match the latest
            // plan semantics. Safe conjuncts still narrow mixed-schema
            // batches; any deferred conjunct classifies survivors as Whole.
            let condition_started = Instant::now();
            let pruned = prune_batch_by_condition(batch, condition, fst_fields, Some(plan_schema));
            out.condition_time += condition_started.elapsed();
            match pruned {
                PrunedBatch::Dropped => out.dropped_batches += 1,
                PrunedBatch::Exact(batch) => {
                    out.exact_batches += 1;
                    out.exact_rows_after_condition += batch.num_rows() as u64;
                    let projection_started = Instant::now();
                    let batch = project_batch_to_needed(batch, needed_columns)
                        .map_err(|e| anyhow::anyhow!("{e}"))?;
                    out.projection_time += projection_started.elapsed();
                    out.kept.push((true, batch));
                }
                PrunedBatch::Whole(batch) => {
                    out.whole_batches += 1;
                    out.whole_rows_retained += batch.num_rows() as u64;
                    let projection_started = Instant::now();
                    let batch = project_batch_to_needed(batch, needed_columns)
                        .map_err(|e| anyhow::anyhow!("{e}"))?;
                    out.projection_time += projection_started.elapsed();
                    out.kept.push((false, batch));
                }
            }
            Ok(())
        },
    )?;
    out.frames_seen = frames_seen;
    out.stream_frames = stream_frames;
    out.time_frames = time_frames;
    out.top_n_skipped_frames = top_n_skipped_frames;
    Ok(out)
}

/// Resolve assigned segment ids against the listed rows; every id must
/// resolve or the whole query fails, naming the id. Duplicate assignments
/// (impossible from the partitioner, defensive here) collapse to one scan.
fn resolve_assigned(
    assigned: &[i64],
    rows: Vec<SegmentMeta>,
    org_id: &str,
    stream_name: &str,
) -> Result<Vec<SegmentMeta>> {
    let mut by_id: HashMap<i64, SegmentMeta> = rows.into_iter().map(|m| (m.id, m)).collect();
    let mut seen = HashSet::with_capacity(assigned.len());
    let mut metas = Vec::with_capacity(assigned.len());
    for &id in assigned {
        if !seen.insert(id) {
            continue;
        }
        match by_id.remove(&id) {
            Some(meta) => metas.push(meta),
            None => {
                return Err(Error::Message(format!(
                    "[SEGMENT:SCAN] {org_id}/{stream_name}: assigned segment id {id} not found in wal_segments — deleted or aged out mid-query, refusing to return partial data"
                )));
            }
        }
    }
    Ok(metas)
}

/// Outcome of pruning one decoded batch against the index condition.
/// The distinction matters downstream: only `Exact` batches — whose
/// surviving rows are KNOWN condition matches — may feed top-n trimming;
/// `Whole` batches are re-filtered by the provider and must pass through
/// untrimmed.
enum PrunedBatch {
    /// The condition was fully evaluated here; every surviving row matches.
    /// (Also the no-condition case: trivially, every row "matches".)
    Exact(RecordBatch),
    /// The condition could not be evaluated on this batch's schema — kept
    /// whole, the provider re-applies the filter downstream.
    Whole(RecordBatch),
    /// No row of this batch can match.
    Dropped,
}

/// Prune a decoded segment batch down to the rows the query's index
/// condition can match, BEFORE it counts against the scan budget. The
/// budget is an OOM guard for the live backlog — without this prune a
/// 3-row needle (`trace_id = X` over a wide range) died at 512MB of KEPT
/// bytes purely because the stream was busy, while the rows it needed
/// were a few KB. Conservative by construction:
/// - conjuncts are handled per top-level AND member (see the body): absent fields DROP the batch
///   only for positive null-rejecting predicates, everything else evaluates what it can and
///   over-keeps;
/// - evaluation errors keep the batch whole;
/// - the provider downstream re-applies the condition uniformly, so pruning here only ever narrows
///   what the budget must hold.
fn prune_batch_by_condition(
    batch: RecordBatch,
    condition: Option<&IndexCondition>,
    fst_fields: &[String],
    plan_schema: Option<&Schema>,
) -> PrunedBatch {
    use datafusion::physical_plan::ColumnarValue;
    let Some(condition) = condition else {
        return PrunedBatch::Exact(batch);
    };
    // Schema-mixed segment batches may lack condition columns entirely (raw
    // write-time schemas carry present fields only) — and `to_physical_expr`
    // resolves columns with an infallible lookup (panic, not Err), so
    // presence is checked HERE, per top-level AND conjunct:
    // - every field present         => the conjunct joins the prune filter;
    // - a null-rejecting predicate  => no row of this batch has the field, on an absent field the
    //   conjunct is false everywhere, the whole AND is: DROP the batch (this is what keeps a shared
    //   busy stream from blowing the budget on batches that provably cannot match — most batches of
    //   other services lack the filtered field entirely);
    // - anything else on an absent  => the conjunct is skipped here and left field (IsNull matches
    //   to the downstream re-filter; pruning absent, Or may match via        with the REMAINING
    //   conjuncts still another arm, negations          narrows what the budget must hold depend on
    //   product null          (partially-pruned batches classify semantics, match_all may        as
    //   Whole, never Exact: surviving hit a present fts field)        rows are NOT known full
    //   matches and must stay out of top-n trimming).
    let schema = batch.schema();
    let mut evaluable: Vec<&Condition> = Vec::with_capacity(condition.conditions.len());
    let mut skipped_conjunct = false;
    for cond in &condition.conditions {
        let fields = cond.get_schema_fields(fst_fields);
        let all_present = fields.iter().all(|field| schema.index_of(field).is_ok());
        let types_match = all_present
            && plan_schema.is_none_or(|plan_schema| {
                fields.iter().all(|field| {
                    let Ok(batch_field) = schema.field_with_name(field) else {
                        return false;
                    };
                    let Ok(plan_field) = plan_schema.field_with_name(field) else {
                        return false;
                    };
                    predicate_data_types_equivalent(batch_field.data_type(), plan_field.data_type())
                })
            });
        if types_match {
            evaluable.push(cond);
        } else if !all_present && fields.len() == 1 && conjunct_is_false_without_its_field(cond) {
            return PrunedBatch::Dropped;
        } else {
            skipped_conjunct = true;
        }
    }
    if evaluable.is_empty() {
        return PrunedBatch::Whole(batch);
    }
    let classify = |batch: RecordBatch| {
        if skipped_conjunct {
            PrunedBatch::Whole(batch)
        } else {
            PrunedBatch::Exact(batch)
        }
    };
    let partial = IndexCondition {
        conditions: evaluable.into_iter().cloned().collect(),
    };
    let expr = match partial.to_physical_expr(schema.as_ref(), fst_fields) {
        Ok(expr) => expr,
        Err(_) => return PrunedBatch::Whole(batch),
    };
    let mask = match expr.evaluate(&batch) {
        Ok(ColumnarValue::Array(array)) => array,
        Ok(ColumnarValue::Scalar(scalar)) => {
            // a constant verdict: keep or drop the whole batch (a FALSE
            // verdict on the evaluated conjuncts falsifies the full AND
            // even when other conjuncts were skipped)
            return match scalar {
                datafusion::scalar::ScalarValue::Boolean(Some(true)) => classify(batch),
                _ => PrunedBatch::Dropped,
            };
        }
        Err(_) => return PrunedBatch::Whole(batch),
    };
    let Some(mask) = mask.as_any().downcast_ref::<BooleanArray>() else {
        return PrunedBatch::Whole(batch);
    };
    match arrow::compute::filter_record_batch(&batch, mask) {
        // zero survivors of the evaluated conjuncts => zero survivors of
        // the full AND, no matter what was skipped
        Ok(filtered) if filtered.num_rows() == 0 => PrunedBatch::Dropped,
        Ok(filtered) => classify(filtered),
        Err(_) => PrunedBatch::Whole(batch),
    }
}

/// Whether this conjunct is FALSE for every row of a batch whose schema
/// lacks the (single) field it references — i.e. a positive, null-rejecting
/// predicate: a row without the field has it null, and none of these match
/// null. Only shapes where the index semantics and the downstream SQL
/// filter AGREE that absent-cannot-match are listed; complement shapes
/// (NotEqual, negated In/NumericCmp, IsNull, Not) are excluded because the
/// product treats them as set complements that INCLUDE rows lacking the
/// field, and structural/fts shapes (Or, And, MatchAll, Fuzzy, All) are
/// excluded because another arm or another fts field may still match.
fn conjunct_is_false_without_its_field(cond: &Condition) -> bool {
    matches!(
        cond,
        Condition::Equal(..)
            | Condition::StrMatch(..)
            | Condition::In(_, _, false)
            | Condition::NumericCmp(_, _, false, _)
            | Condition::Regex(..)
            | Condition::IsNotNull(_)
    )
}

/// Running top-n `_timestamp` tracker for `ORDER BY _timestamp DESC LIMIT n`
/// scans (`sorted_by_time` is set exactly for that shape, and the pushed
/// `limit` already includes any OFFSET). Once n timestamps are collected,
/// `threshold()` is the n-th newest seen so far — monotonically
/// non-decreasing, so any row (or whole segment) strictly older can never
/// re-enter the final top-n.
///
/// WINDOW CLAMP (correctness, not optimization): frames are kept on
/// FRAME-level time overlap, so batches carry rows outside the query
/// window — and rows NEWER than `end` always exist on a live stream
/// (ingest continues past the window snapshot). The heap observes ONLY
/// rows that definitely survive the downstream time filter
/// (`start <= ts < end`, the strict subset); an unclamped heap inflates
/// the threshold with rows the plan will drop and trims away rows that
/// belong in the result. The trim mask keeps the looser closed-range
/// superset — over-keeping is safe, the provider re-filters.
struct TopNTimestamps {
    n: usize,
    window: (i64, i64),
    heap: BinaryHeap<Reverse<i64>>,
}

impl TopNTimestamps {
    fn new(n: usize, window: (i64, i64)) -> Self {
        Self {
            n,
            window,
            // `LIMIT` is user-controlled and can be huge. Grow only with rows
            // actually observed instead of reserving O(limit) up front.
            heap: BinaryHeap::new(),
        }
    }

    /// The n-th newest in-window timestamp observed, once n have been seen.
    fn threshold(&self) -> Option<i64> {
        (self.heap.len() >= self.n)
            .then(|| self.heap.peek().map(|r| r.0))
            .flatten()
    }

    fn observe_batch(&mut self, batch: &RecordBatch) {
        let Ok(ts_idx) = batch.schema().index_of(TIMESTAMP_COL_NAME) else {
            return;
        };
        let Some(ts) = batch.column(ts_idx).as_any().downcast_ref::<Int64Array>() else {
            return;
        };
        self.observe(ts);
    }
    fn observe(&mut self, ts: &Int64Array) {
        for v in ts.iter().flatten() {
            if !timestamp_inside_half_open(v, self.window) {
                continue;
            }
            if self.heap.len() < self.n {
                self.heap.push(Reverse(v));
            } else if self.heap.peek().is_some_and(|&Reverse(min)| v > min) {
                self.heap.pop();
                self.heap.push(Reverse(v));
            }
        }
    }
}

/// Trim a batch of KNOWN condition matches down to rows that can still be
/// in the top-n newest of the query's half-open window: `_timestamp >=
/// threshold`. Ties stay; the plan's own sort+limit resolves them. Returns
/// `None` when nothing survives. Batches without a readable `_timestamp`
/// column pass through whole (defensive; the writer refuses
/// timestamp-less rows).
#[cfg(test)]
fn trim_batch_to_top_n(
    batch: RecordBatch,
    top: &mut TopNTimestamps,
) -> Result<Option<RecordBatch>> {
    top.observe_batch(&batch);
    let Some(threshold) = top.threshold() else {
        return Ok(Some(batch));
    };
    trim_batch_to_threshold(batch, threshold, top.window, top.n)
}

fn trim_batch_to_threshold(
    batch: RecordBatch,
    threshold: i64,
    window: (i64, i64),
    limit: usize,
) -> Result<Option<RecordBatch>> {
    let Ok(ts_idx) = batch.schema().index_of(TIMESTAMP_COL_NAME) else {
        return Ok(Some(batch));
    };
    let Some(ts) = batch.column(ts_idx).as_any().downcast_ref::<Int64Array>() else {
        return Ok(Some(batch));
    };
    let (start, end) = window;
    let mask = BooleanArray::from_iter(ts.iter().map(|v| {
        Some(v.is_some_and(|v| (window == (0, 0) || (v >= start && v < end)) && v >= threshold))
    }));
    if mask.true_count() == batch.num_rows() {
        return Ok(Some(batch));
    }
    match arrow::compute::filter_record_batch(&batch, &mask) {
        Ok(trimmed) if trimmed.num_rows() == 0 => Ok(None),
        Ok(trimmed) => Ok(Some(trimmed)),
        Err(e) => Err(Error::Message(format!(
            "[SEGMENT:SCAN] trimming a segment batch to the top-{limit} newest rows failed: {e}"
        ))),
    }
}

fn compact_exact_top_n(
    batches: &mut Vec<RecordBatch>,
    bytes: &mut usize,
    threshold: i64,
    window: (i64, i64),
    limit: usize,
) -> Result<()> {
    let (start, end) = window;
    let in_window = |value: i64| window == (0, 0) || (value >= start && value < end);
    let mut above_threshold = 0usize;
    for batch in batches.iter() {
        let Ok(ts_idx) = batch.schema().index_of(TIMESTAMP_COL_NAME) else {
            continue;
        };
        let Some(ts) = batch.column(ts_idx).as_any().downcast_ref::<Int64Array>() else {
            continue;
        };
        above_threshold = above_threshold.saturating_add(
            ts.iter()
                .flatten()
                .filter(|value| in_window(*value) && *value > threshold)
                .count(),
        );
    }
    debug_assert!(above_threshold <= limit);
    let mut ties_remaining = limit.saturating_sub(above_threshold);
    let previous_batches = std::mem::take(batches);
    *bytes = 0;
    for batch in previous_batches {
        let Ok(ts_idx) = batch.schema().index_of(TIMESTAMP_COL_NAME) else {
            *bytes = bytes.saturating_add(batch.size());
            batches.push(batch);
            continue;
        };
        let Some(ts) = batch.column(ts_idx).as_any().downcast_ref::<Int64Array>() else {
            *bytes = bytes.saturating_add(batch.size());
            batches.push(batch);
            continue;
        };
        let mask = BooleanArray::from_iter(ts.iter().map(|value| {
            Some(value.is_some_and(|value| {
                if !in_window(value) || value < threshold {
                    return false;
                }
                if value > threshold {
                    return true;
                }
                if ties_remaining == 0 {
                    return false;
                }
                ties_remaining -= 1;
                true
            }))
        }));
        let batch = if mask.true_count() == batch.num_rows() {
            batch
        } else {
            match arrow::compute::filter_record_batch(&batch, &mask) {
                Ok(batch) if batch.num_rows() == 0 => continue,
                Ok(batch) => batch,
                Err(e) => {
                    return Err(Error::Message(format!(
                        "[SEGMENT:SCAN] compacting exact top-{limit} candidates failed: {e}"
                    )));
                }
            }
        };
        *bytes = bytes.saturating_add(batch.size());
        batches.push(batch);
    }
    Ok(())
}

fn trim_deferred_top_n(
    batches: &mut Vec<RecordBatch>,
    bytes: &mut usize,
    threshold: i64,
    window: (i64, i64),
    limit: usize,
) -> Result<()> {
    let previous_batches = std::mem::take(batches);
    *bytes = 0;
    for batch in previous_batches {
        let Some(batch) = trim_batch_to_threshold(batch, threshold, window, limit)? else {
            continue;
        };
        *bytes = bytes.saturating_add(batch.size());
        batches.push(batch);
    }
    Ok(())
}

/// Drop every column the query can never read, BEFORE the batch counts
/// against the scan budget. Row counts are always preserved.
///
/// Two non-obvious points:
/// - IPC stream decode slices ALL columns of a batch out of one message-body buffer, so
///   `RecordBatch::project` alone would keep the whole decoded frame resident (and the budget
///   accounting would lie). Batches that actually shed columns are therefore detached with a `take`
///   gather copy — cheap, it only materializes the columns being kept.
/// - a batch can project to ZERO columns (a pure `count(*)` plan against a frame with no surviving
///   needed column); arrow preserves `num_rows` through empty projections, which is exactly what
///   such plans consume.
fn project_batch_to_needed(batch: RecordBatch, needed: &HashSet<String>) -> Result<RecordBatch> {
    // A plan that reads `_source` consumes WHOLE rows: that column is
    // synthesized from every stored column whenever the batch does not
    // materialize it (segment frames never do). Projecting such batches
    // silently hollows out star hits to bare timestamps (e2e-caught), so
    // they are kept whole and the budget guards them at full width.
    if needed.contains(vortex_index::SOURCE_COL_NAME) {
        return Ok(batch);
    }
    let schema = batch.schema();
    let keep: Vec<usize> = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| needed.contains(f.name().as_str()))
        .map(|(i, _)| i)
        .collect();
    if keep.len() == schema.fields().len() {
        return Ok(batch);
    }
    let projected = batch.project(&keep).map_err(|e| {
        Error::Message(format!(
            "[SEGMENT:SCAN] projecting a decoded segment batch to its needed columns failed: {e}"
        ))
    })?;
    if keep.is_empty() {
        return Ok(projected);
    }
    let indices = arrow::array::UInt32Array::from_iter_values(0..projected.num_rows() as u32);
    let columns = projected
        .columns()
        .iter()
        .map(|c| arrow::compute::take(c.as_ref(), &indices, None))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| {
            Error::Message(format!(
                "[SEGMENT:SCAN] detaching a projected segment batch from its decode buffer failed: {e}"
            ))
        })?;
    RecordBatch::try_new(projected.schema(), columns).map_err(|e| {
        Error::Message(format!(
            "[SEGMENT:SCAN] rebuilding a projected segment batch failed: {e}"
        ))
    })
}

/// Fold a kept batch into the accumulator, enforcing the scan byte budgets
/// with [`RecordBatchExt::size`] accounting. `soft_budget`/`hard_ceiling`
/// are parameters so tests can shrink them; the public path always passes
/// [`segment_scan_budgets`]. Crossing the soft budget WARNS exactly once
/// per accumulator and keeps going (recent-data queries must not fail on a
/// busy stream); crossing the hard ceiling is an error, never a truncation
/// (a capped-off subset would be silent partial data).
fn check_retained_budget(
    kept_bytes: usize,
    soft_budget_warned: &mut bool,
    soft_budget: usize,
    hard_ceiling: usize,
    org_id: &str,
    stream_type: StreamType,
    stream_name: &str,
) -> Result<()> {
    if kept_bytes > hard_ceiling {
        return Err(Error::Message(format!(
            "[SEGMENT:SCAN] {org_id}/{stream_type}/{stream_name}: this query materialized {kept_bytes} bytes of not-yet-sealed live data, past the hard per-query ceiling {hard_ceiling} (half the pod's memory limit) — narrow the time range, add filters on always-present fields, or select fewer columns"
        )));
    }
    if soft_budget > 0 && !*soft_budget_warned && kept_bytes > soft_budget {
        *soft_budget_warned = true;
        log::warn!(
            "[SEGMENT:SCAN] {org_id}/{stream_type}/{stream_name}: live-data scan passed the soft budget {soft_budget} bytes (ZO_SEGMENT_SCAN_MAX_BYTES) and continues — hard stop at {hard_ceiling}; consider a narrower time range, filters on always-present fields, or fewer columns"
        );
    }
    Ok(())
}

#[cfg(test)]
fn push_within_budget(
    kept: &mut Vec<RecordBatch>,
    kept_bytes: &mut usize,
    soft_budget_warned: &mut bool,
    batch: RecordBatch,
    soft_budget: usize,
    hard_ceiling: usize,
    org_id: &str,
    stream_type: StreamType,
    stream_name: &str,
) -> Result<()> {
    let next_bytes = kept_bytes.saturating_add(batch.size());
    check_retained_budget(
        next_bytes,
        soft_budget_warned,
        soft_budget,
        hard_ceiling,
        org_id,
        stream_type,
        stream_name,
    )?;
    *kept_bytes = next_bytes;
    kept.push(batch);
    Ok(())
}

/// Group batches by their OWN write-time schema, merge small batches within
/// each group, and build one `NewMemTable` per group — exactly the memtable
/// path in `wal.rs`: batches stay RAW (present fields only, no `_source`);
/// `NewMemTable` adapts raw -> plan per streamed batch (null-padding, type
/// casts, lazy `_source`). Never concat mixed schemas: a field that
/// type-flipped between writes puts mixed-type batches in one stream (the
/// 2026-07-30 lesson).
fn build_tables_from_batches(
    trace_id: &str,
    batches: Vec<RecordBatch>,
    schema: Arc<Schema>,
    sorted_by_time: bool,
    index_condition: Option<IndexCondition>,
    fst_fields: Vec<String>,
    time_range: (i64, i64),
) -> Result<Vec<Arc<dyn TableProvider>>> {
    let latest_schema = Arc::new(schema.as_ref().clone().with_metadata(Default::default()));
    let batch_groups = group_by_batch_schema(batches);

    let mut tables: Vec<Arc<dyn TableProvider>> = Vec::with_capacity(batch_groups.len());
    for (i, (group_schema, record_batches)) in batch_groups.into_iter().enumerate() {
        if record_batches.is_empty() {
            continue;
        }
        // merge small batches into big batches
        let group_limit = config::get_batch_size();
        let mut merge_groups: Vec<Vec<RecordBatch>> = Vec::new();
        let mut current_group: Vec<RecordBatch> = Vec::new();
        let mut group_size = 0;
        for batch in record_batches {
            if group_size > 0 && group_size + batch.num_rows() > group_limit {
                merge_groups.push(std::mem::take(&mut current_group));
                group_size = 0;
            }
            group_size += batch.num_rows();
            current_group.push(batch);
        }
        if !current_group.is_empty() {
            merge_groups.push(current_group);
        }
        // groups are schema-homogeneous by construction, but never unwrap on
        // data-shaped input
        let record_batches = merge_groups
            .into_iter()
            .map(|mut group| {
                if group.len() == 1 {
                    Ok(group.remove(0))
                } else {
                    concat_batches(group_schema.clone(), group)
                }
            })
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| {
                Error::Message(format!(
                    "[SEGMENT:SCAN] concat segment batches for group {i} failed: {e}"
                ))
            })?;

        let table = NewMemTable::try_new(
            record_batches[0].schema().clone(),
            vec![record_batches],
            latest_schema.clone(),
            sorted_by_time,
            index_condition.clone(),
            fst_fields.clone(),
            time_range,
        )
        .map_err(|e| {
            log::error!("[trace_id {trace_id}] segments_scan: create memtable error: {e}");
            Error::from(e)
        })?;
        tables.push(Arc::new(table) as _);
    }
    Ok(tables)
}

/// Same grouping rule as `wal.rs`: by each batch's OWN schema, never the
/// segment- or stream-reported one.
fn group_by_batch_schema(batches: Vec<RecordBatch>) -> HashMap<Arc<Schema>, Vec<RecordBatch>> {
    let mut groups: HashMap<Arc<Schema>, Vec<RecordBatch>> = HashMap::with_capacity(2);
    for batch in batches {
        groups.entry(batch.schema()).or_default().push(batch);
    }
    groups
}

#[cfg(test)]
mod tests {
    use arrow::array::{Int64Array, StringArray};

    #[test]
    fn segment_fetch_permits_are_bounded_and_never_zero() {
        assert_eq!(segment_permits(-1, 512), 1);
        assert_eq!(segment_permits(0, 512), 1);
        assert_eq!(segment_permits(1, 512), 1);
        assert_eq!(
            segment_permits(SEGMENT_FETCH_PERMIT_BYTES as i64 + 1, 512),
            2
        );
        assert_eq!(segment_permits(i64::MAX, 7), 7);
        assert_eq!(segment_permits(i64::MAX, usize::MAX), u32::MAX);
    }

    #[test]
    fn segment_top_n_plan_separates_exact_skip_from_trim_only() {
        let exact = SegmentTopNPlan::exact_desc(1000).unwrap();
        assert_eq!(exact.limit, 1000);
        assert!(exact.can_skip_segments);

        let trim_only = SegmentTopNPlan::trim_only(1000).unwrap();
        assert_eq!(trim_only.limit, 1000);
        assert!(!trim_only.can_skip_segments);

        assert!(SegmentTopNPlan::exact_desc(0).is_none());
        assert!(SegmentTopNPlan::trim_only(0).is_none());
    }

    #[test]
    fn segment_top_n_skip_is_strict_and_proof_gated() {
        let threshold = AtomicI64::new(100);
        assert!(should_skip_by_top_n(true, 99, &threshold));
        assert!(!should_skip_by_top_n(true, 100, &threshold));
        assert!(!should_skip_by_top_n(true, 101, &threshold));
        assert!(!should_skip_by_top_n(false, 99, &threshold));
    }

    #[test]
    fn schema_mismatch_defers_only_the_unsafe_conjunct() {
        assert!(!predicate_data_types_equivalent(
            &DataType::Float64,
            &DataType::Utf8,
        ));
        assert!(predicate_data_types_equivalent(
            &DataType::Utf8View,
            &DataType::Utf8,
        ));

        let raw_schema = Arc::new(Schema::new(vec![
            arrow_schema::Field::new("_timestamp", DataType::Int64, false),
            arrow_schema::Field::new("service", DataType::Utf8, true),
            arrow_schema::Field::new("code", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            raw_schema,
            vec![
                Arc::new(Int64Array::from(vec![3, 2, 1])),
                Arc::new(StringArray::from(vec!["temporal", "other", "temporal"])),
                Arc::new(arrow::array::Float64Array::from(vec![38.0, 38.0, 39.0])),
            ],
        )
        .unwrap();
        let plan = Schema::new(vec![
            arrow_schema::Field::new("_timestamp", DataType::Int64, false),
            arrow_schema::Field::new("service", DataType::Utf8, true),
            arrow_schema::Field::new("code", DataType::Utf8, true),
        ]);
        let mut condition = IndexCondition::new();
        condition.add_condition(Condition::Equal(
            "service".to_string(),
            "temporal".to_string(),
        ));
        condition.add_condition(Condition::Equal("code".to_string(), "38".to_string()));

        let PrunedBatch::Whole(pruned) =
            prune_batch_by_condition(batch, Some(&condition), &[], Some(&plan))
        else {
            panic!("type-mismatched code must defer while service still prunes");
        };
        assert_eq!(pruned.num_rows(), 2);
    }

    #[tokio::test]
    async fn rolling_stage_refills_before_the_slowest_item_finishes() {
        let gates = Arc::new(
            (0..3)
                .map(|_| Arc::new(tokio::sync::Notify::new()))
                .collect::<Vec<_>>(),
        );
        let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
        let work = futures::stream::iter(0..3).map({
            let gates = Arc::clone(&gates);
            move |index| {
                let gate = Arc::clone(&gates[index]);
                let started_tx = started_tx.clone();
                async move {
                    started_tx.send(index).unwrap();
                    gate.notified().await;
                    index
                }
            }
        });
        let collector = tokio::spawn(rolling_stage(work, 2).collect::<Vec<usize>>());

        let mut first_two = Vec::with_capacity(2);
        for _ in 0..2 {
            first_two.push(
                tokio::time::timeout(std::time::Duration::from_secs(1), started_rx.recv())
                    .await
                    .expect("initial rolling-stage window did not fill")
                    .expect("started channel closed"),
            );
        }
        first_two.sort_unstable();
        assert_eq!(first_two, vec![0, 1]);

        // Item 1 deliberately remains blocked. Completing item 0 must start
        // item 2 immediately; fixed waves would wait for item 1.
        gates[0].notify_one();
        let third = tokio::time::timeout(std::time::Duration::from_secs(1), started_rx.recv())
            .await
            .expect("rolling stage waited for its slowest peer before refilling")
            .expect("started channel closed");
        assert_eq!(third, 2);

        gates[1].notify_one();
        gates[2].notify_one();
        let mut completed = collector.await.expect("collector task failed");
        completed.sort_unstable();
        assert_eq!(completed, vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn unordered_rolling_stage_yields_ready_work_behind_a_slow_head() {
        let slow = Arc::new(tokio::sync::Notify::new());
        let work = futures::stream::iter(0..2).map({
            let slow = Arc::clone(&slow);
            move |index| {
                let slow = Arc::clone(&slow);
                async move {
                    if index == 0 {
                        slow.notified().await;
                    }
                    index
                }
            }
        });
        let mut stage = rolling_stage(work, 2);
        let first = tokio::time::timeout(Duration::from_secs(1), stage.next())
            .await
            .expect("ready work was blocked behind the slow head");
        assert_eq!(first, Some(1));
        slow.notify_one();
        assert_eq!(stage.next().await, Some(0));
    }

    #[test]
    fn segment_cache_type_respects_aggregate_placement_limits() {
        assert_eq!(
            choose_segment_cache_type(10, Some(11), Some(101)),
            file_data::CacheType::Memory
        );
        assert_eq!(
            choose_segment_cache_type(11, Some(11), Some(101)),
            file_data::CacheType::Disk
        );
        assert_eq!(
            choose_segment_cache_type(101, Some(11), Some(101)),
            file_data::CacheType::None
        );
        assert_eq!(
            choose_segment_cache_type(10, None, Some(101)),
            file_data::CacheType::Disk
        );
        assert_eq!(
            choose_segment_cache_type(10, None, None),
            file_data::CacheType::None
        );
    }

    #[test]
    fn cancelled_last_waiter_removes_uninitialized_singleflight_cell() {
        let key = "segments_scan_test_cancelled_last_waiter";
        INFLIGHT_SEGMENT_FETCHES.remove(key);
        let cell = Arc::new(SharedSegmentFetchCell::new());
        INFLIGHT_SEGMENT_FETCHES.insert(key.to_string(), Arc::clone(&cell));
        let first = InflightSegmentFetchCleanup {
            key: key.to_string(),
            cell: Arc::clone(&cell),
            armed: true,
        };
        let last = InflightSegmentFetchCleanup {
            key: key.to_string(),
            cell: Arc::clone(&cell),
            armed: true,
        };
        drop(cell);

        drop(first);
        assert!(INFLIGHT_SEGMENT_FETCHES.contains_key(key));
        drop(last);
        assert!(!INFLIGHT_SEGMENT_FETCHES.contains_key(key));
    }

    #[test]
    fn initialized_error_cell_can_be_removed_before_retry() {
        let key = "segments_scan_test_initialized_error";
        INFLIGHT_SEGMENT_FETCHES.remove(key);
        let failed = Arc::new(SharedSegmentFetchCell::new());
        assert!(
            failed
                .set(Err(Arc::<str>::from("transient storage failure")))
                .is_ok()
        );
        INFLIGHT_SEGMENT_FETCHES.insert(key.to_string(), Arc::clone(&failed));

        remove_inflight_segment_fetch(key, &failed);
        assert!(!INFLIGHT_SEGMENT_FETCHES.contains_key(key));

        let retry = Arc::new(SharedSegmentFetchCell::new());
        INFLIGHT_SEGMENT_FETCHES.insert(key.to_string(), Arc::clone(&retry));
        assert!(!Arc::ptr_eq(&failed, &retry));
        INFLIGHT_SEGMENT_FETCHES.remove(key);
    }

    /// THE prod failure this guards against (2026-08-01): `trace_id = X`
    /// over a wide range hit 3 rows but died at the 512MB scan budget
    /// because the WHOLE live backlog counted against it. The prune keeps
    /// only rows the condition can match, so the budget sees KBs; a batch
    /// whose schema lacks a positively-required field DROPS (#35), one
    /// that merely cannot refute the condition stays whole; with no
    /// condition the budget still guards full scans.
    #[test]
    fn test_prune_batch_by_condition_saves_needle_queries() {
        use crate::service::search::index::Condition;
        let schema = Arc::new(Schema::new(vec![
            arrow_schema::Field::new("_timestamp", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("trace_id", arrow_schema::DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2, 3, 4])),
                Arc::new(StringArray::from(vec!["a", "needle", "b", "needle"])),
            ],
        )
        .unwrap();
        let mut condition = IndexCondition::new();
        condition.add_condition(Condition::Equal(
            "trace_id".to_string(),
            "needle".to_string(),
        ));

        // matching rows survive as KNOWN matches, everything else pruned
        // before budgeting
        let PrunedBatch::Exact(pruned) =
            prune_batch_by_condition(batch.clone(), Some(&condition), &[], None)
        else {
            panic!("evaluated condition must yield Exact");
        };
        assert_eq!(pruned.num_rows(), 2);
        let mut top = TopNTimestamps::new(2, (0, 10));
        let pruned = trim_batch_to_top_n(pruned, &mut top)
            .unwrap()
            .expect("exact equality rows should seed top-n");
        assert_eq!(pruned.num_rows(), 2);
        assert_eq!(top.threshold(), Some(2));

        // a condition nothing matches drops the batch entirely
        let mut miss = IndexCondition::new();
        miss.add_condition(Condition::Equal(
            "trace_id".to_string(),
            "absent-value".to_string(),
        ));
        assert!(matches!(
            prune_batch_by_condition(batch.clone(), Some(&miss), &[], None),
            PrunedBatch::Dropped
        ));

        // no condition: trivially Exact (all rows "match"; full scans are
        // still budget-guarded)
        let PrunedBatch::Exact(whole) = prune_batch_by_condition(batch.clone(), None, &[], None)
        else {
            panic!("no condition must yield Exact");
        };
        assert_eq!(whole.num_rows(), 4);

        // a schema without the column DROPS the batch for a positive
        // null-rejecting predicate: no row of this batch has the field, so
        // `trace_id = needle` is false everywhere (#35 — a shared busy
        // stream must not blow the budget on batches that provably cannot
        // match)
        let no_col_schema = Arc::new(Schema::new(vec![arrow_schema::Field::new(
            "_timestamp",
            arrow_schema::DataType::Int64,
            false,
        )]));
        let no_col = RecordBatch::try_new(
            no_col_schema,
            vec![Arc::new(Int64Array::from(vec![1i64, 2]))],
        )
        .unwrap();
        assert!(matches!(
            prune_batch_by_condition(no_col.clone(), Some(&condition), &[], None),
            PrunedBatch::Dropped
        ));

        // ...but only for shapes where absent-cannot-match is certain: an
        // IS NULL (matches rows lacking the field) keeps the batch WHOLE
        // for the downstream re-filter
        let mut is_null = IndexCondition::new();
        is_null.add_condition(Condition::IsNull("trace_id".to_string()));
        let PrunedBatch::Whole(kept) =
            prune_batch_by_condition(no_col.clone(), Some(&is_null), &[], None)
        else {
            panic!("IS NULL on an absent field must stay Whole");
        };
        assert_eq!(kept.num_rows(), 2);

        // negated set-membership is a complement shape (the product treats
        // it as including rows lacking the field): never dropped on absence
        let mut not_in = IndexCondition::new();
        not_in.add_condition(Condition::In(
            "trace_id".to_string(),
            vec!["x".to_string()],
            true,
        ));
        assert!(matches!(
            prune_batch_by_condition(no_col, Some(&not_in), &[], None),
            PrunedBatch::Whole(_)
        ));
    }

    /// #35 conjunct granularity: a batch that can evaluate only SOME of the
    /// AND conjuncts prunes with those and over-keeps the rest — classified
    /// Whole (surviving rows are NOT known full matches, so top-n trimming
    /// must not touch them) — and zero survivors of the evaluated subset
    /// still drop the batch (an empty superset empties the full AND).
    #[test]
    fn test_prune_batch_partial_conjuncts_narrow_but_never_claim_exact() {
        use crate::service::search::index::Condition;
        let schema = Arc::new(Schema::new(vec![
            arrow_schema::Field::new("_timestamp", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("service_name", arrow_schema::DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2, 3, 4])),
                Arc::new(StringArray::from(vec![
                    "temporal", "nginx", "temporal", "api",
                ])),
            ],
        )
        .unwrap();

        // AND(service_name='temporal', queue IS NULL): the queue field is
        // absent — IS NULL can't be refuted, so it is skipped; the present
        // conjunct still cuts the batch to 2 rows, kept as Whole
        let mut cond = IndexCondition::new();
        cond.add_condition(Condition::Equal(
            "service_name".to_string(),
            "temporal".to_string(),
        ));
        cond.add_condition(Condition::IsNull("wf_task_queue_name".to_string()));
        let PrunedBatch::Whole(kept) =
            prune_batch_by_condition(batch.clone(), Some(&cond), &[], None)
        else {
            panic!("partially evaluated condition must yield Whole");
        };
        assert_eq!(kept.num_rows(), 2, "the present conjunct prunes rows");

        // same shape, but the present conjunct matches nothing: Dropped
        let mut cond_miss = IndexCondition::new();
        cond_miss.add_condition(Condition::Equal(
            "service_name".to_string(),
            "no-such-service".to_string(),
        ));
        cond_miss.add_condition(Condition::IsNull("wf_task_queue_name".to_string()));
        assert!(matches!(
            prune_batch_by_condition(batch.clone(), Some(&cond_miss), &[], None),
            PrunedBatch::Dropped
        ));

        // AND(service_name='temporal', queue='q1'): the absent queue field
        // is a positive equality — the whole batch drops without evaluating
        // anything (the user-reported prod shape: shared stream, per-service
        // fields)
        let mut cond_drop = IndexCondition::new();
        cond_drop.add_condition(Condition::Equal(
            "service_name".to_string(),
            "temporal".to_string(),
        ));
        cond_drop.add_condition(Condition::Equal(
            "wf_task_queue_name".to_string(),
            "q1".to_string(),
        ));
        assert!(matches!(
            prune_batch_by_condition(batch, Some(&cond_drop), &[], None),
            PrunedBatch::Dropped
        ));
    }

    /// End-to-end shape: pruned batches fit a budget the unpruned stream
    /// would blow through.
    #[test]
    fn test_pruned_needle_fits_a_budget_the_backlog_would_trip() {
        use crate::service::search::index::Condition;
        let schema = Arc::new(Schema::new(vec![
            arrow_schema::Field::new("_timestamp", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("trace_id", arrow_schema::DataType::Utf8, true),
        ]));
        let make_batch = |needle_rows: usize| {
            let n = 10_000usize;
            let ts: Vec<i64> = (0..n as i64).collect();
            let ids: Vec<String> = (0..n)
                .map(|i| {
                    if i < needle_rows {
                        "needle".to_string()
                    } else {
                        format!("filler-{i}")
                    }
                })
                .collect();
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ts)),
                    Arc::new(StringArray::from(ids)),
                ],
            )
            .unwrap()
        };
        let mut condition = IndexCondition::new();
        condition.add_condition(Condition::Equal(
            "trace_id".to_string(),
            "needle".to_string(),
        ));
        let budget = 64 * 1024; // far below one raw batch
        let mut kept = Vec::new();
        let mut kept_bytes = 0usize;
        let mut soft_budget_warned = false;
        for needle_rows in [1usize, 2, 0] {
            let batch = make_batch(needle_rows);
            assert!(batch.get_array_memory_size() > budget, "test premise");
            let batch = match prune_batch_by_condition(batch, Some(&condition), &[], None) {
                PrunedBatch::Exact(b) | PrunedBatch::Whole(b) => b,
                PrunedBatch::Dropped => continue,
            };
            push_within_budget(
                &mut kept,
                &mut kept_bytes,
                &mut soft_budget_warned,
                batch,
                0,
                budget,
                "org",
                StreamType::Traces,
                "default",
            )
            .expect("pruned needle rows must fit the budget");
        }
        let total_rows: usize = kept.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3, "exactly the needle rows are kept");
    }

    /// THE prod failure this guards against (2026-08-03, .62 cutover): an
    /// unconditioned `count(*)` touching the live tail of a 120k-spans/s
    /// traces stream died at 512MB of kept bytes, all of them columns no
    /// operator would ever read. Projection to the plan's needed columns
    /// must shrink kept accounting to ~one column and preserve every row.
    #[test]
    fn test_project_batch_to_needed_saves_unconditioned_counts() {
        let n = 10_000usize;
        let schema = Arc::new(Schema::new(vec![
            arrow_schema::Field::new("_timestamp", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("span_payload", arrow_schema::DataType::Utf8, true),
            arrow_schema::Field::new("service_name", arrow_schema::DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from((0..n as i64).collect::<Vec<_>>())),
                Arc::new(StringArray::from(
                    (0..n)
                        .map(|i| format!("wide-filler-{i:0>64}"))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    (0..n).map(|i| format!("svc-{}", i % 7)).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        let whole_size = batch.size();

        // a count(*) plan needs only _timestamp
        let needed: HashSet<String> = ["_timestamp".to_string()].into_iter().collect();
        let projected = project_batch_to_needed(batch.clone(), &needed).unwrap();
        assert_eq!(projected.num_rows(), n, "projection must never drop rows");
        assert_eq!(projected.num_columns(), 1);
        assert!(
            projected.size() * 4 < whole_size,
            "projected accounting must shrink far below whole-width ({} vs {whole_size})",
            projected.size()
        );

        // the budget the whole batch trips, the projected batch fits
        let budget = whole_size - 1;
        let mut kept = Vec::new();
        let mut kept_bytes = 0usize;
        let mut soft_budget_warned = false;
        push_within_budget(
            &mut kept,
            &mut kept_bytes,
            &mut soft_budget_warned,
            projected,
            0,
            budget,
            "org",
            StreamType::Traces,
            "default",
        )
        .expect("projected count batch must fit a budget the raw tail trips");

        // a plan needing every column keeps the batch untouched
        let all: HashSet<String> = ["_timestamp", "span_payload", "service_name"]
            .into_iter()
            .map(String::from)
            .collect();
        let untouched = project_batch_to_needed(batch.clone(), &all).unwrap();
        assert_eq!(untouched.num_columns(), 3);

        // a batch with NO needed column projects to zero columns, rows intact
        // (pure row-count consumers still count correctly)
        let unrelated: HashSet<String> = ["absent_col".to_string()].into_iter().collect();
        let empty = project_batch_to_needed(batch.clone(), &unrelated).unwrap();
        assert_eq!(empty.num_columns(), 0);
        assert_eq!(
            empty.num_rows(),
            n,
            "row count survives an empty projection"
        );

        // a plan that reads `_source` (star selects) keeps batches WHOLE:
        // the column is synthesized from every stored column, so dropping
        // "unneeded" ones would hollow out the hits (e2e-caught regression)
        let star: HashSet<String> = [
            "_timestamp".to_string(),
            vortex_index::SOURCE_COL_NAME.to_string(),
        ]
        .into_iter()
        .collect();
        let whole = project_batch_to_needed(batch, &star).unwrap();
        assert_eq!(
            whole.num_columns(),
            3,
            "_source plans must keep every column"
        );
    }

    /// Batches decoded from an IPC stream slice every column out of one
    /// message-body allocation; projection must DETACH (gather-copy) the
    /// kept columns so dropping the batch actually frees the wide columns.
    /// Observable here: the projected batch's accounted size is that of a
    /// fresh, exactly-sized timestamp column — not the shared decode body.
    #[test]
    fn test_projection_detaches_from_ipc_decode_buffers() {
        let n = 4_096usize;
        let schema = Arc::new(Schema::new(vec![
            arrow_schema::Field::new("_timestamp", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("payload", arrow_schema::DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from((0..n as i64).collect::<Vec<_>>())),
                Arc::new(StringArray::from(
                    (0..n)
                        .map(|i| format!("padding-{i:0>128}"))
                        .collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        let mut ipc = Vec::new();
        {
            let mut w = arrow::ipc::writer::StreamWriter::try_new(&mut ipc, &schema).unwrap();
            w.write(&batch).unwrap();
            w.finish().unwrap();
        }
        let decoded = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(ipc), None)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();

        let needed: HashSet<String> = ["_timestamp".to_string()].into_iter().collect();
        let projected = project_batch_to_needed(decoded.clone(), &needed).unwrap();
        assert_eq!(projected.num_rows(), n);
        // fresh Int64 buffer: n * 8 bytes plus small constants — nowhere near
        // the >512KB decoded payload column
        assert!(
            projected.size() < n * 8 * 2,
            "projected size {} must reflect a detached, exactly-sized column",
            projected.size()
        );
    }

    /// THE prod failure this guards against (2026-08-03, post-.63): a star
    /// `ORDER BY _timestamp DESC LIMIT n` over the live traces tail kept
    /// the whole ~537MB tail (plans reading `_source` keep full rows) and
    /// died on the budget — while the query only ever returns the n newest
    /// rows. The running top-n threshold must (a) keep a superset of the
    /// true top-n incl. ties, (b) never grow kept rows past the candidate
    /// set once locked, (c) rise monotonically so old segments can be
    /// skipped wholesale.
    #[test]
    fn test_top_n_trim_keeps_exactly_the_newest_candidates() {
        let make = |ts: Vec<i64>| {
            let schema = Arc::new(Schema::new(vec![
                arrow_schema::Field::new("_timestamp", arrow_schema::DataType::Int64, false),
                arrow_schema::Field::new("payload", arrow_schema::DataType::Utf8, true),
            ]));
            let vals: Vec<String> = ts.iter().map(|t| format!("row-{t}-{:0>128}", "")).collect();
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(ts)),
                    Arc::new(StringArray::from(vals)),
                ],
            )
            .unwrap()
        };
        let huge_limit = TopNTimestamps::new(usize::MAX, (0, i64::MAX));
        assert_eq!(
            huge_limit.heap.capacity(),
            0,
            "a user-controlled LIMIT must not reserve its capacity eagerly"
        );
        assert!(
            !frame_time_overlaps_half_open(10, 10, (0, 10)),
            "a frame beginning at the exclusive query end must not be decoded"
        );
        assert!(frame_time_overlaps_half_open(9, 10, (0, 10)));
        let mut top = TopNTimestamps::new(3, (0, i64::MAX));

        // newest segment first (the loop sorts metas by max_ts desc):
        // threshold locks at the 3rd newest of the first batch
        let b1 = trim_batch_to_top_n(make((100..110).collect()), &mut top)
            .unwrap()
            .expect("first batch keeps its top-3 candidates");
        assert_eq!(top.threshold(), Some(107));
        assert_eq!(b1.num_rows(), 3, "only ts>=107 survive: 107,108,109");

        // an older batch trims to nothing and cannot lower the threshold
        assert!(
            trim_batch_to_top_n(make((0..50).collect()), &mut top)
                .unwrap()
                .is_none(),
            "strictly older rows are all trimmed"
        );
        assert_eq!(top.threshold(), Some(107), "threshold never regresses");

        // a straddling batch is trimmed against the POST-observe threshold:
        // 200 enters the top-3 (now 200,109,108), so 107 falls out and only
        // 108 (tie at the new threshold) and 200 survive
        let b3 = trim_batch_to_top_n(make(vec![105, 107, 108, 200]), &mut top)
            .unwrap()
            .expect("straddling batch keeps candidates");
        assert_eq!(b3.num_rows(), 2, "108 and 200; 107 fell out of the top-3");
        assert_eq!(top.threshold(), Some(108), "top-3 is now 200,109,108");

        // a batch without a readable _timestamp passes through whole
        let no_ts = RecordBatch::try_new(
            Arc::new(Schema::new(vec![arrow_schema::Field::new(
                "other",
                arrow_schema::DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1i64, 2]))],
        )
        .unwrap();
        assert_eq!(
            trim_batch_to_top_n(no_ts, &mut top)
                .unwrap()
                .unwrap()
                .num_rows(),
            2
        );

        // THE window clamp (live-stream correctness): frames are kept on
        // FRAME-level overlap, so rows NEWER than the query window ride
        // along in the same batches. They must not feed the threshold —
        // an inflated threshold trims away rows that belong in the result.
        let mut clamped = TopNTimestamps::new(2, (100, 200));
        let b = trim_batch_to_top_n(make(vec![150, 160, 500, 501, 502]), &mut clamped)
            .unwrap()
            .expect("in-window rows survive despite newer out-of-window rows");
        assert_eq!(
            clamped.threshold(),
            Some(150),
            "threshold from in-window rows only — 500s are outside [100,200)"
        );
        assert_eq!(
            b.num_rows(),
            2,
            "150 and 160 kept; 500s dropped by the mask"
        );
        // rows older than the window never occupy heap slots either
        let mut older = TopNTimestamps::new(2, (100, 200));
        let ts_old = Int64Array::from(vec![10i64, 20, 150]);
        older.observe(&ts_old);
        assert_eq!(older.threshold(), None, "only one in-window row observed");

        let mut unbounded = TopNTimestamps::new(2, (0, 0));
        unbounded.observe(&Int64Array::from(vec![-10, 20, 30]));
        assert_eq!(
            unbounded.threshold(),
            Some(20),
            "(0,0) follows the segment convention of an unbounded window",
        );

        let retrimmed = trim_batch_to_threshold(make((100..110).collect()), 108, (0, 0), 3)
            .unwrap()
            .expect("final threshold keeps its newest superset");
        assert_eq!(retrimmed.num_rows(), 2);

        let mut batches = vec![make((100..110).collect()), make((200..210).collect())];
        let mut bytes = batches.iter().map(|batch| batch.size()).sum();
        compact_exact_top_n(&mut batches, &mut bytes, 200, (0, 0), 10).unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 10);
        let kept_ts = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(kept_ts.iter().flatten().all(|value| value >= 200));
        let mut warned = false;
        check_retained_budget(
            bytes,
            &mut warned,
            0,
            bytes,
            "org",
            StreamType::Traces,
            "default",
        )
        .unwrap();

        let mut deferred_batches = vec![make(vec![1, 2, 199, 200, 300])];
        let mut deferred_bytes = deferred_batches[0].size();
        trim_deferred_top_n(&mut deferred_batches, &mut deferred_bytes, 200, (0, 0), 10).unwrap();
        let deferred_ts = deferred_batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values();
        assert_eq!(
            deferred_ts,
            &[200, 300],
            "a deferred condition batch keeps the threshold superset but drops obsolete rows"
        );

        let mut boundary_batches = vec![make(vec![5, 4, 10])];
        let mut boundary_bytes = boundary_batches[0].size();
        compact_exact_top_n(&mut boundary_batches, &mut boundary_bytes, 4, (0, 10), 2).unwrap();
        let boundary_ts = boundary_batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values();

        let mut stable_threshold_batches = vec![make(vec![101, 100, 100]), make(vec![200])];
        let mut stable_threshold_bytes =
            stable_threshold_batches.iter().map(RecordBatch::size).sum();
        compact_exact_top_n(
            &mut stable_threshold_batches,
            &mut stable_threshold_bytes,
            100,
            (0, 0),
            3,
        )
        .unwrap();
        let mut stable_ts = stable_threshold_batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .flatten()
            })
            .collect::<Vec<_>>();
        stable_ts.sort_unstable();
        assert_eq!(stable_ts, vec![100, 101, 200]);
        assert_eq!(boundary_ts, &[5, 4]);
    }

    /// End-to-end shape of the fix: wide star batches stream oldest-last
    /// (metas sorted newest-first), limit 10 — the kept set stays far under
    /// a budget the untrimmed tail would blow through, and the final kept
    /// rows are a superset of the true top-10.
    #[test]
    fn test_top_n_trimmed_star_fits_a_budget_the_tail_would_trip() {
        let schema = Arc::new(Schema::new(vec![
            arrow_schema::Field::new("_timestamp", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("payload", arrow_schema::DataType::Utf8, true),
        ]));
        let make_batch = |start: i64, n: i64| {
            let ts: Vec<i64> = (start..start + n).collect();
            let vals: Vec<String> = ts.iter().map(|t| format!("p-{t}-{:0>256}", "")).collect();
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ts)),
                    Arc::new(StringArray::from(vals)),
                ],
            )
            .unwrap()
        };
        let budget = 128 * 1024;
        let mut top = TopNTimestamps::new(10, (0, i64::MAX));
        let mut kept = Vec::new();
        let mut kept_bytes = 0usize;
        let mut soft_budget_warned = false;
        // 20 segments x 5000 rows, newest first; untrimmed this is ~28MB
        for seg in (0..20).rev() {
            let batch = make_batch(seg * 5_000, 5_000);
            assert!(batch.get_array_memory_size() > budget, "test premise");
            let Some(batch) = trim_batch_to_top_n(batch, &mut top).unwrap() else {
                continue;
            };
            push_within_budget(
                &mut kept,
                &mut kept_bytes,
                &mut soft_budget_warned,
                batch,
                0,
                budget,
                "org",
                StreamType::Traces,
                "default",
            )
            .expect("trimmed star candidates must fit the budget");
        }
        let mut all_ts: Vec<i64> = kept
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        all_ts.sort_unstable_by(|a, b| b.cmp(a));
        assert!(all_ts.len() >= 10, "at least the top-10 survive");
        assert_eq!(
            &all_ts[..10],
            &(99_990..100_000).rev().collect::<Vec<i64>>()[..],
            "the true 10 newest timestamps all survive trimming"
        );
    }

    use arrow_schema::{DataType, Field};
    use datafusion::{physical_plan::collect, prelude::SessionContext};
    use segment_wal::format::{SegmentFrame, SegmentHeader, encode_segment};

    use super::*;

    #[test]
    fn cap_is_non_fatal_and_keeps_the_newest() {
        // under the cap: untouched, no shortfall
        let small: Vec<SegmentMeta> = (1..=5).map(|i| seg_meta(i, i * 10, i * 10 + 5)).collect();
        let (kept, sf) = apply_query_cap(small.clone(), "org1/logs/app1");
        assert_eq!(kept.len(), 5);
        assert!(sf.is_none(), "no shortfall below the cap");

        // over the cap: keeps exactly the cap, newest by max_ts, and reports
        // the remainder instead of failing the query (2026-07-31 outage)
        let big: Vec<SegmentMeta> = (1..=(MAX_QUERY_SEGMENTS as i64 + 25))
            .map(|i| seg_meta(i, i * 10, i * 10 + 5))
            .collect();
        let (kept, sf) = apply_query_cap(big, "org1/logs/app1");
        assert_eq!(kept.len(), MAX_QUERY_SEGMENTS);
        let sf = sf.expect("over-cap must report a shortfall, not error");
        assert_eq!(sf.skipped, 25);
        // newest kept: the highest max_ts survives, the oldest are dropped
        let min_kept = kept.iter().map(|m| m.max_ts).min().unwrap();
        let max_kept = kept.iter().map(|m| m.max_ts).max().unwrap();
        assert_eq!(max_kept, (MAX_QUERY_SEGMENTS as i64 + 25) * 10 + 5);
        assert_eq!(min_kept, 26 * 10 + 5, "the 25 oldest are the skipped ones");
        assert!(
            sf.message().contains("25 unbuilt segments"),
            "message must name the shortfall: {}",
            sf.message()
        );
    }

    fn seg_meta(id: i64, min_ts: i64, max_ts: i64) -> SegmentMeta {
        SegmentMeta {
            id,
            node_uuid: "node-a".to_string(),
            seq: id,
            object_key: format!("wal_segments/node-a/{id:020}"),
            min_ts,
            max_ts,
            size: 1024,
            streams: vec!["org1/logs/app1".to_string()],
            status: wal_segments::SegmentStatus::Pending,
            builder_node: String::new(),
            created_at: min_ts,
            updated_at: min_ts,
        }
    }

    // ---- provenance dedup rule ----

    #[test]
    fn dedup_drops_legacy_ranges_but_only_exact_h2_ids() {
        let candidates = [1, 2, 5, 9, 10, 20, 21, 22, 30, 31, 32]
            .into_iter()
            .map(|id| seg_meta(id, 0, 10))
            .collect::<Vec<_>>();
        let token = infra::l0_provenance::encode_exact_ids(&[20, 22, 99]).unwrap();
        let snapshot = vec![
            FileIdWithFile {
                id: 100,
                file: "files/o/logs/s/l0_h1_node_a_2_9_3.vix".to_string(),
                records: 11,
                original_size: 101,
                deleted: false,
            },
            FileIdWithFile {
                id: 101,
                file: format!("files/o/logs/s/l0_h2_{token}_3.vix"),
                records: 12,
                original_size: 102,
                deleted: true,
            },
            FileIdWithFile {
                id: 102,
                // A malformed h2 key must never suppress its apparent range.
                file: "files/o/logs/s/l0_h2_30_32_3.vix".to_string(),
                records: 13,
                original_size: 103,
                deleted: false,
            },
        ];
        let (files, coverage) = split_snapshot_file_ids(snapshot, &candidates);
        assert_eq!(coverage.range_count(), 1);
        assert_eq!(coverage.exact_id_count(), 2);
        assert_eq!(files.len(), 3);
        assert_eq!(files[0].id, 100);
        assert_eq!(files[0].records, 11);
        assert_eq!(files[0].original_size, 101);
        assert!(!files[0].deleted);
        assert!(files[1].deleted);

        let survivors = dedup_candidates(candidates, &coverage);
        assert_eq!(
            survivors.iter().map(|meta| meta.id).collect::<Vec<_>>(),
            vec![1, 10, 21, 30, 31, 32]
        );
    }

    #[test]
    fn coverage_compacts_ranges_and_exact_ids_already_inside_them() {
        let mut coverage = L0Coverage::default();
        let candidate_ids = [3, 9];
        coverage.insert(L0Provenance::Range(5, 7), &candidate_ids);
        coverage.insert(L0Provenance::Range(1, 4), &candidate_ids);
        coverage.insert(L0Provenance::Exact(vec![3, 9]), &candidate_ids);
        coverage.compact();
        assert_eq!(coverage.range_count(), 1);
        assert_eq!(coverage.exact_id_count(), 1);
        assert!(coverage.contains(1));
        assert!(coverage.contains(7));
        assert!(!coverage.contains(8));
        assert!(coverage.contains(9));
    }

    #[test]
    fn dedup_with_empty_coverage_keeps_everything() {
        let candidates = vec![seg_meta(1, 0, 10), seg_meta(2, 0, 10)];
        let survivors = dedup_candidates(candidates, &L0Coverage::default());
        assert_eq!(survivors.len(), 2);
    }

    // ---- pseudo id transport ----

    #[test]
    fn pseudo_file_id_negates_and_rejects_bad_rows() {
        let meta = seg_meta(42, 0, 10);
        let fid = pseudo_file_id(&meta).unwrap();
        assert_eq!(fid.id, -42);
        assert_eq!(fid.original_size, meta.size);
        assert_eq!(fid.records, 0);
        assert!(!fid.deleted);

        for bad in [0, -7] {
            let err = pseudo_file_id(&seg_meta(bad, 0, 10)).unwrap_err();
            assert!(
                err.to_string().contains("wal_segments/node-a/"),
                "error must name the object: {err}"
            );
        }
    }

    #[test]
    fn split_pseudo_ids_separates_and_negates() {
        let (files, segments) = split_pseudo_ids(&[5, -42, 7, -1, 0]);
        assert_eq!(files, vec![5, 7, 0]);
        assert_eq!(segments, vec![42, 1]);

        let (files, segments) = split_pseudo_ids(&[]);
        assert!(files.is_empty());
        assert!(segments.is_empty());
    }

    /// Pseudo ids ride the SAME partitioner as real files: partitioning is a
    /// pure function of the input list, so the same input yields the same
    /// assignment, and every id (negative included) lands in exactly one
    /// bucket.
    #[test]
    fn assignment_is_deterministic_and_exactly_once_for_pseudo_ids() {
        use crate::service::search::cluster::flight::{
            partition_file_by_bytes, partition_file_by_nums,
        };
        let ids = vec![
            FileId {
                id: 10,
                records: 5,
                original_size: 100,
                deleted: false,
            },
            pseudo_file_id(&seg_meta(3, 0, 10)).unwrap(),
            FileId {
                id: 11,
                records: 5,
                original_size: 100,
                deleted: false,
            },
            pseudo_file_id(&seg_meta(4, 0, 10)).unwrap(),
        ];
        for parts in [
            partition_file_by_nums(ids.clone(), 3),
            partition_file_by_bytes(ids.clone(), 3),
        ] {
            let mut all: Vec<i64> = parts.iter().flatten().copied().collect();
            all.sort_unstable();
            assert_eq!(
                all,
                vec![-4, -3, 10, 11],
                "every id exactly once: {parts:?}"
            );
        }
        // determinism: identical input -> identical buckets
        assert_eq!(
            partition_file_by_nums(ids.clone(), 3),
            partition_file_by_nums(ids.clone(), 3)
        );
        assert_eq!(
            partition_file_by_bytes(ids.clone(), 3),
            partition_file_by_bytes(ids, 3)
        );
    }

    // ---- follower resolution ----

    #[test]
    fn resolve_assigned_errors_on_missing_id_naming_it() {
        let rows = vec![seg_meta(1, 0, 10), seg_meta(2, 0, 10)];
        let metas = resolve_assigned(&[2, 1], rows.clone(), "org1", "app1").unwrap();
        assert_eq!(metas.iter().map(|m| m.id).collect::<Vec<_>>(), vec![2, 1]);

        // duplicates collapse to one scan
        let metas = resolve_assigned(&[1, 1], rows.clone(), "org1", "app1").unwrap();
        assert_eq!(metas.len(), 1);

        let err = resolve_assigned(&[1, 3], rows, "org1", "app1").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("segment id 3") && msg.contains("org1/app1"),
            "error must name the id and stream: {msg}"
        );
    }

    // ---- frame filtering ----

    fn ts_batch(field: &str, ts: &[i64], vals_int: Option<&[i64]>) -> RecordBatch {
        // every ingested row carries `_timestamp`; second column flips type
        match vals_int {
            Some(vals) => {
                let schema = Arc::new(Schema::new(vec![
                    Field::new("_timestamp", DataType::Int64, false),
                    Field::new(field, DataType::Int64, true),
                ]));
                RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(Int64Array::from(ts.to_vec())),
                        Arc::new(Int64Array::from(vals.to_vec())),
                    ],
                )
                .expect("build int batch")
            }
            None => {
                let vals: Vec<String> = ts.iter().map(|v| format!("s{v}")).collect();
                let schema = Arc::new(Schema::new(vec![
                    Field::new("_timestamp", DataType::Int64, false),
                    Field::new(field, DataType::Utf8, true),
                ]));
                RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(Int64Array::from(ts.to_vec())),
                        Arc::new(StringArray::from(
                            vals.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                        )),
                    ],
                )
                .expect("build str batch")
            }
        }
    }

    fn frame(
        org: &str,
        stream_type: StreamType,
        stream: &str,
        min_ts: i64,
        max_ts: i64,
        batch: RecordBatch,
    ) -> SegmentFrame {
        SegmentFrame {
            org: org.to_string(),
            stream_type,
            stream: stream.to_string(),
            min_ts,
            max_ts,
            batch,
        }
    }

    /// One helper for the tests below: stream-scan an encoded segment with
    /// no condition and every column needed (identity + time + empty-drop
    /// behavior only).
    fn scan_all(
        encoded: &[u8],
        org: &str,
        stype: StreamType,
        stream: &str,
        range: (i64, i64),
        needed: &[&str],
    ) -> ScannedSegment {
        let needed: HashSet<String> = needed.iter().map(|s| s.to_string()).collect();
        scan_segment_object(
            encoded,
            org,
            stype,
            stream,
            range,
            None,
            &Schema::empty(),
            &[],
            &needed,
            None,
        )
        .expect("scan segment")
    }

    #[test]
    fn exact_top_n_threshold_skips_old_frame_before_ipc_decode() {
        let header = SegmentHeader {
            node_uuid: "node-top-n-frame-skip".to_string(),
            seq: 1,
            created_at: 1_700_000_000_000_000,
        };
        let frames = vec![
            frame(
                "org1",
                StreamType::Traces,
                "default",
                100,
                109,
                ts_batch("value", &[100, 109], Some(&[1, 2])),
            ),
            frame(
                "org1",
                StreamType::Traces,
                "default",
                200,
                209,
                ts_batch("value", &[200, 209], Some(&[3, 4])),
            ),
        ];
        let encoded = encode_segment(&header, &frames).unwrap();
        let needed = HashSet::from_iter(["_timestamp".to_string(), "value".to_string()]);
        let threshold = AtomicI64::new(150);
        let scanned = scan_segment_object(
            &encoded,
            "org1",
            StreamType::Traces,
            "default",
            (0, i64::MAX),
            None,
            &Schema::empty(),
            &[],
            &needed,
            Some(&threshold),
        )
        .unwrap();

        assert_eq!(scanned.frames_seen, 2);
        assert_eq!(scanned.stream_frames, 2);
        assert_eq!(scanned.time_frames, 2);
        assert_eq!(scanned.top_n_skipped_frames, 1);
        assert_eq!(scanned.ipc_frames, 1);
        assert_eq!(scanned.rows_examined, 2);
        assert_eq!(scanned.exact_rows_after_condition, 2);
        assert_eq!(scanned.whole_rows_retained, 0);
    }
    #[test]
    fn histogram_filters_stream_and_window_and_folds_single_bucket_frames() {
        let header = SegmentHeader {
            node_uuid: "node-histogram-fold".to_string(),
            seq: 1,
            created_at: 1_700_000_000_000_000,
        };
        let frames = vec![
            frame(
                "other-org",
                StreamType::Logs,
                "app1",
                102,
                108,
                ts_batch("v", &[102], Some(&[1])),
            ),
            frame(
                "org1",
                StreamType::Traces,
                "app1",
                102,
                108,
                ts_batch("v", &[102], Some(&[1])),
            ),
            frame(
                "org1",
                StreamType::Logs,
                "other-stream",
                102,
                108,
                ts_batch("v", &[102], Some(&[1])),
            ),
            frame(
                "org1",
                StreamType::Logs,
                "app1",
                80,
                90,
                ts_batch("v", &[80, 90], Some(&[1, 2])),
            ),
            frame(
                "org1",
                StreamType::Logs,
                "app1",
                102,
                108,
                ts_batch("v", &[102, 105, 108], Some(&[1, 2, 3])),
            ),
        ];
        let encoded = encode_segment(&header, &frames).expect("encode segment");
        let scanned = scan_segment_histogram(
            &encoded,
            "org1",
            StreamType::Logs,
            "app1",
            (100, 130),
            100,
            10,
            3,
            0,
        )
        .expect("scan histogram");

        assert_eq!(scanned.histogram, HashMap::from([(0, 3)]));
        assert_eq!(
            scanned.rows_examined, 3,
            "foreign and out-of-window frame rows do not enter scan stats"
        );
        assert_eq!(
            scanned.decoded_frames, 0,
            "the matching single-bucket frame must fold from FrameInfo.rows"
        );
    }

    #[test]
    fn histogram_decodes_window_and_bucket_boundaries_with_half_open_bounds() {
        let header = SegmentHeader {
            node_uuid: "node-histogram-boundaries".to_string(),
            seq: 2,
            created_at: 1_700_000_000_000_000,
        };
        let frames = vec![
            // Query-start straddle: 90 drops, 100 is included.
            frame(
                "org1",
                StreamType::Logs,
                "app1",
                90,
                100,
                ts_batch("v", &[90, 100], Some(&[1, 2])),
            ),
            // Bucket edge: each side lands exactly once.
            frame(
                "org1",
                StreamType::Logs,
                "app1",
                109,
                110,
                ts_batch("v", &[109, 110], Some(&[3, 4])),
            ),
            // Query-end straddle: 129 is included, 130 is excluded.
            frame(
                "org1",
                StreamType::Logs,
                "app1",
                129,
                130,
                ts_batch("v", &[129, 130], Some(&[5, 6])),
            ),
        ];
        let encoded = encode_segment(&header, &frames).expect("encode segment");
        let scanned = scan_segment_histogram(
            &encoded,
            "org1",
            StreamType::Logs,
            "app1",
            (100, 130),
            100,
            10,
            3,
            0,
        )
        .expect("scan histogram");

        assert_eq!(
            scanned.histogram,
            HashMap::from([(0, 2), (1, 1), (2, 1)]),
            "100+109, 110, and 129 count once; 90 and the exclusive end 130 drop"
        );
        assert_eq!(
            scanned.rows_examined, 6,
            "scan stats retain pre-window frame-row accounting"
        );
        assert_eq!(scanned.decoded_frames, 3);
    }

    #[test]
    fn histogram_whole_frame_fold_uses_timestamp_offset_grid() {
        let header = SegmentHeader {
            node_uuid: "node-histogram-offset".to_string(),
            seq: 3,
            created_at: 1_700_000_000_000_000,
        };
        let frames = vec![
            frame(
                "org1",
                StreamType::Logs,
                "app1",
                100,
                109,
                ts_batch("v", &[100, 109], Some(&[1, 2])),
            ),
            frame(
                "org1",
                StreamType::Logs,
                "app1",
                110,
                110,
                ts_batch("v", &[110], Some(&[3])),
            ),
        ];
        let encoded = encode_segment(&header, &frames).expect("encode segment");
        let scanned = scan_segment_histogram(
            &encoded,
            "org1",
            StreamType::Logs,
            "app1",
            (100, 120),
            102,
            10,
            2,
            2,
        )
        .expect("scan histogram");

        assert_eq!(scanned.histogram, HashMap::from([(0, 2), (1, 1)]));
        assert_eq!(scanned.rows_examined, 3);
        assert_eq!(
            scanned.decoded_frames, 0,
            "offset-adjusted single-bucket frames still avoid IPC callbacks"
        );
    }

    #[test]
    fn histogram_folded_frames_still_surface_segment_corruption() {
        let header = SegmentHeader {
            node_uuid: "node-histogram-corrupt".to_string(),
            seq: 4,
            created_at: 1_700_000_000_000_000,
        };
        let frames = vec![frame(
            "org1",
            StreamType::Logs,
            "app1",
            101,
            102,
            ts_batch("v", &[101, 102], Some(&[1, 2])),
        )];
        let mut encoded = encode_segment(&header, &frames).expect("encode segment");
        let middle = encoded.len() / 2;
        encoded[middle] ^= 0x55;

        assert!(
            scan_segment_histogram(
                &encoded,
                "org1",
                StreamType::Logs,
                "app1",
                (100, 110),
                100,
                10,
                1,
                0,
            )
            .is_err(),
            "CRC or decompression corruption must remain a hard error even when IPC is skipped"
        );
    }

    #[test]
    fn keep_stream_frames_filters_stream_identity_time_and_empties() {
        let frames = vec![
            // wrong org
            frame(
                "org2",
                StreamType::Logs,
                "app1",
                100,
                200,
                ts_batch("v", &[100], Some(&[1])),
            ),
            // wrong stream type
            frame(
                "org1",
                StreamType::Traces,
                "app1",
                100,
                200,
                ts_batch("v", &[100], Some(&[1])),
            ),
            // wrong stream
            frame(
                "org1",
                StreamType::Logs,
                "app2",
                100,
                200,
                ts_batch("v", &[100], Some(&[1])),
            ),
            // out of range (ends before the query window)
            frame(
                "org1",
                StreamType::Logs,
                "app1",
                10,
                50,
                ts_batch("v", &[10], Some(&[1])),
            ),
            // out of range (starts after the query window)
            frame(
                "org1",
                StreamType::Logs,
                "app1",
                500,
                600,
                ts_batch("v", &[500], Some(&[1])),
            ),
            // in range
            frame(
                "org1",
                StreamType::Logs,
                "app1",
                150,
                180,
                ts_batch("v", &[150, 180], Some(&[1, 2])),
            ),
            // boundary overlap: max_ts == range start (closed-range keep)
            frame(
                "org1",
                StreamType::Logs,
                "app1",
                50,
                100,
                ts_batch("v", &[100], Some(&[3])),
            ),
            // empty batch dropped even when in range
            frame(
                "org1",
                StreamType::Logs,
                "app1",
                150,
                180,
                ts_batch("v", &[], Some(&[])),
            ),
        ];
        let header = SegmentHeader {
            node_uuid: "node-filter".to_string(),
            seq: 9,
            created_at: 1_700_000_000_000_000,
        };
        let encoded = encode_segment(&header, &frames).expect("encode segment");
        let scanned = scan_all(
            &encoded,
            "org1",
            StreamType::Logs,
            "app1",
            (100, 200),
            &["_timestamp", "v"],
        );
        let rows: usize = scanned.kept.iter().map(|(_, b)| b.num_rows()).sum();
        assert_eq!(scanned.kept.len(), 2);
        assert_eq!(rows, 3);
        assert_eq!(
            scanned.rows_examined, 3,
            "only matching frames' rows are examined"
        );
        assert!(
            scanned.kept.iter().all(|(exact, _)| *exact),
            "no condition => every kept batch is trim-eligible"
        );
    }

    #[test]
    fn push_within_budget_rejects_over_budget_and_keeps_nothing_extra() {
        let mut kept: Vec<RecordBatch> = Vec::new();
        let mut kept_bytes = 0usize;
        let mut soft_budget_warned = false;
        let first = ts_batch("v", &[1, 2], Some(&[1, 2]));
        // budget admits exactly the first batch (exceed is strictly greater)
        let budget = first.size();
        push_within_budget(
            &mut kept,
            &mut kept_bytes,
            &mut soft_budget_warned,
            first,
            0,
            budget,
            "org1",
            StreamType::Logs,
            "app1",
        )
        .expect("a batch that exactly fills the budget is kept");
        assert_eq!(kept.len(), 1);
        assert_eq!(kept_bytes, budget);

        let err = push_within_budget(
            &mut kept,
            &mut kept_bytes,
            &mut soft_budget_warned,
            ts_batch("v", &[3], Some(&[3])),
            0,
            budget,
            "org1",
            StreamType::Logs,
            "app1",
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("org1/logs/app1") && msg.contains(&format!("ceiling {budget}")),
            "error must name the stream and the ceiling: {msg}"
        );
        assert_eq!(kept.len(), 1, "the over-ceiling batch must not be kept");
    }

    /// Crossing the SOFT budget warns and keeps going — only the hard
    /// ceiling fails the query (the owner call that recent-data queries on
    /// a busy stream must not die at an arbitrary byte line).
    #[test]
    fn push_within_budget_soft_crossing_keeps_the_batch_and_continues() {
        let mut kept: Vec<RecordBatch> = Vec::new();
        let mut kept_bytes = 0usize;
        let mut soft_budget_warned = false;
        let first = ts_batch("v", &[1, 2], Some(&[1, 2]));
        let soft = first.size(); // second push crosses the soft line
        for i in 0..3i64 {
            push_within_budget(
                &mut kept,
                &mut kept_bytes,
                &mut soft_budget_warned,
                ts_batch("v", &[i], Some(&[i])),
                soft,
                usize::MAX,
                "org1",
                StreamType::Logs,
                "app1",
            )
            .expect("soft budget must never fail the push");
        }
        push_within_budget(
            &mut kept,
            &mut kept_bytes,
            &mut soft_budget_warned,
            first,
            soft,
            usize::MAX,
            "org1",
            StreamType::Logs,
            "app1",
        )
        .expect("far past the soft budget still keeps going");
        assert_eq!(kept.len(), 4, "every batch is kept in warn mode");
        assert!(kept_bytes > soft);
    }

    #[test]
    fn group_by_batch_schema_separates_type_flips_and_concats_cleanly() {
        let groups = group_by_batch_schema(vec![
            ts_batch("code", &[1, 2], Some(&[200, 404])),
            ts_batch("code", &[3], None),
            ts_batch("code", &[4], Some(&[500])),
        ]);
        assert_eq!(groups.len(), 2);
        for (schema, batches) in groups {
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            let merged = concat_batches(schema, batches).expect("homogeneous group must concat");
            assert!(merged.num_rows() == rows);
        }
    }

    // ---- end-to-end: encode -> decode -> filter -> NewMemTable -> scan ----

    /// Two frames of the SAME stream whose `code` field type-flipped between
    /// writes (Int64 then Utf8 — the 2026-07-30 incident shape), plus a
    /// foreign-stream frame and an out-of-range frame. Rows must survive
    /// into the plan schema (code: Utf8) with correct casting — mirroring
    /// the wal.rs memtable semantics.
    #[tokio::test]
    async fn decoded_frames_build_scannable_tables_across_type_flips() {
        let header = SegmentHeader {
            node_uuid: "node-e2e".to_string(),
            seq: 1,
            created_at: 1_700_000_000_000_000,
        };
        let frames = vec![
            frame(
                "org1",
                StreamType::Logs,
                "app1",
                100,
                120,
                ts_batch("code", &[100, 120], Some(&[200, 404])),
            ),
            frame(
                "org1",
                StreamType::Logs,
                "app1",
                130,
                140,
                ts_batch("code", &[130], None),
            ),
            frame(
                "org1",
                StreamType::Logs,
                "other",
                100,
                120,
                ts_batch("code", &[100], Some(&[1])),
            ),
            frame(
                "org1",
                StreamType::Logs,
                "app1",
                900,
                950,
                ts_batch("code", &[900], Some(&[7])),
            ),
        ];
        let encoded = encode_segment(&header, &frames).expect("encode segment");
        let scanned = scan_all(
            &encoded,
            "org1",
            StreamType::Logs,
            "app1",
            (0, 500),
            &["_timestamp", "code"],
        );
        let kept: Vec<RecordBatch> = scanned.kept.into_iter().map(|(_, b)| b).collect();
        assert_eq!(kept.len(), 2, "foreign stream and out-of-range dropped");

        // plan schema: `code` evolved to Utf8 (latest schema wins)
        let plan_schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("code", DataType::Utf8, true),
        ]));
        let tables = build_tables_from_batches(
            "test-trace",
            kept,
            plan_schema.clone(),
            false,
            None,
            vec![],
            (0, i64::MAX),
        )
        .expect("build tables");
        assert_eq!(tables.len(), 2, "one table per write-time schema group");

        let ctx = SessionContext::new();
        // the plan always passes an explicit projection (star is rewritten);
        // request both plan fields
        let projection = vec![
            plan_schema.index_of("_timestamp").unwrap(),
            plan_schema.index_of("code").unwrap(),
        ];
        let mut codes = Vec::new();
        for table in tables {
            let exec = table
                .scan(&ctx.state(), Some(&projection), &[], None)
                .await
                .expect("scan table");
            let batches = collect(exec, ctx.task_ctx()).await.expect("collect");
            for b in &batches {
                assert_eq!(
                    b.schema().field_with_name("code").unwrap().data_type(),
                    &DataType::Utf8
                );
                let col = b
                    .column_by_name("code")
                    .expect("code column")
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("utf8 code after cast")
                    .iter()
                    .map(|v| v.map(|s| s.to_string()))
                    .collect::<Vec<_>>();
                codes.extend(col);
            }
        }
        let mut codes: Vec<String> = codes.into_iter().flatten().collect();
        codes.sort();
        assert_eq!(
            codes,
            vec!["200".to_string(), "404".to_string(), "s130".to_string()],
            "int rows cast to the latest Utf8 schema, string rows pass through"
        );
    }

    #[test]
    fn scan_surfaces_decode_failures() {
        // the async wrapper adds the object key; the scan itself must
        // surface the format-level failure as a hard error
        let needed: HashSet<String> = HashSet::new();
        let err = scan_segment_object(
            b"definitely not a segment",
            "org1",
            StreamType::Logs,
            "app1",
            (0, 0),
            None,
            &Schema::empty(),
            &[],
            &needed,
            None,
        )
        .unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("bad magic"), "unexpected error: {msg}");
    }
}
