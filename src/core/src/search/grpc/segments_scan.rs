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
//! semantics. The leader seam ([`leader_append_segments`]) runs after the
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
//!   this race: `l0_{uuid-or-multi}_{minSegId}_{maxSegId}_{n}` with any extension; any candidate
//!   whose id falls inside a snapshot `l0_` range is dropped ([`dedup_candidates`]). Only snapshot
//!   members may suppress a candidate — an L0 registered after the snapshot names data this query
//!   will not scan from files, so honoring it would open a gap.
//! - The ordering invariant additionally requires the `wal_segments` read and the file_list
//!   snapshot read to be CAUSALLY CONSISTENT. Both run on the RO pool, and `CLIENT_RO == CLIENT` by
//!   default: an empty `ZO_META_POSTGRES_RO_DSN` falls back to the RW DSN
//!   (`infra::db::postgres::connect`), so the invariant holds by default. Do NOT point
//!   `ZO_META_POSTGRES_RO_DSN` at an async replica with segment mode on — a lagging replica can
//!   serve a Built status whose L0 file_list rows have not replicated yet, reopening the gap this
//!   ordering closes.

use std::sync::Arc;

use arrow_schema::Schema;
use config::{
    get_config,
    meta::{search::ScanStats, stream::StreamType},
    metrics,
    utils::record_batch_ext::{RecordBatchExt, concat_batches},
};
use datafusion::{arrow::record_batch::RecordBatch, datasource::TableProvider};
use hashbrown::{HashMap, HashSet};
use infra::{
    cache::file_data,
    errors::{Error, Result},
    file_list::FileId,
    wal_segments::{self, SegmentMeta},
};
use segment_wal::format::{SegmentFrame, decode_segment};

use crate::service::search::{
    datafusion::table_provider::memtable::NewMemTable, index::IndexCondition,
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

/// Hard cap on the bytes of kept batches one follower accumulates for a
/// single query's segment scan, measured with [`RecordBatchExt::size`] — the
/// same arrow accounting the segment buffer caps on. [`MAX_QUERY_SEGMENTS`]
/// bounds only the segment COUNT; each segment can carry up to a whole flush
/// buffer of one stream's rows, so count alone does not bound follower
/// memory. Exceeding the budget fails the query loudly (correctness over
/// availability, same stance as fetch/decode errors: a silently truncated
/// scan would be silent partial data).
const SEGMENT_SCAN_MAX_BYTES: usize = 512 * 1024 * 1024;

/// Storage account segment objects live under — the flusher PUTs with the
/// default/empty account (see `segment_wal::uploader`).
const SEGMENT_STORAGE_ACCOUNT: &str = "";

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

/// Leader seam, phase 2 — runs AFTER the file_list snapshot is fetched:
/// dedup phase-1 candidates against the snapshot's `l0_` provenance and
/// append the survivors as negative-id pseudo-files.
pub async fn append_surviving(
    trace_id: &str,
    org_id: &str,
    stream_type: StreamType,
    stream_name: &str,
    candidates: Vec<SegmentMeta>,
    files: &mut Vec<FileId>,
) -> Result<()> {
    if candidates.is_empty() {
        return Ok(());
    }

    // Provenance dedup needs the SNAPSHOT files' names, but the snapshot was
    // fetched id-only. Re-query keys over the candidates' (narrow) time
    // window and keep only rows whose id is in the snapshot — a key fetched
    // here that the snapshot does not contain must not suppress a candidate
    // (its data will not be scanned by this query).
    let snapshot_ids: HashSet<i64> = files.iter().map(|f| f.id).collect();
    let (window_min, window_max) = candidates
        .iter()
        .fold((i64::MAX, i64::MIN), |(min, max), c| {
            (min.min(c.min_ts), max.max(c.max_ts))
        });
    let recent_files = crate::service::file_list::query(
        trace_id,
        org_id,
        stream_type,
        stream_name,
        infra::schema::get_partition_time_level(stream_type),
        window_min,
        window_max,
    )
    .await?;
    let l0_ranges = l0_ranges_in_snapshot(
        recent_files.iter().map(|f| (f.id, f.key.as_str())),
        &snapshot_ids,
    );
    let survivors = dedup_candidates(candidates, &l0_ranges);
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
        "[trace_id {trace_id}] segments_scan: {org_id}/{stream_type}/{stream_name} appending {} segments ({} L0 ranges deduped) to the file id list",
        survivors.len(),
        l0_ranges.len(),
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

/// Parse the `(minSegId, maxSegId)` provenance range out of an L0 file key.
///
/// Filenames match `l0_{uuid-or-multi}_{minSegId}_{maxSegId}_{n}` with any
/// extension. The middle part may itself contain `_`, so the numeric fields
/// are taken from the END. Returns `None` for anything that does not parse
/// cleanly — an unparsable name must never suppress a segment (dup is
/// recoverable, a gap is not).
fn parse_l0_range(key: &str) -> Option<(i64, i64)> {
    let name = key.rsplit('/').next()?;
    let stem = name.rsplit_once('.').map(|(s, _)| s).unwrap_or(name);
    let rest = stem.strip_prefix("l0_")?;
    let mut parts = rest.rsplit('_');
    let _n: u64 = parts.next()?.parse().ok()?;
    let max: i64 = parts.next()?.parse().ok()?;
    let min: i64 = parts.next()?.parse().ok()?;
    // at least the {uuid-or-multi} field must remain
    parts.next()?;
    if min < 1 || max < min {
        return None;
    }
    Some((min, max))
}

/// Collect the provenance ranges of every L0 file that is IN the snapshot.
fn l0_ranges_in_snapshot<'a>(
    files: impl Iterator<Item = (i64, &'a str)>,
    snapshot_ids: &HashSet<i64>,
) -> Vec<(i64, i64)> {
    files
        .filter(|(id, _)| snapshot_ids.contains(id))
        .filter_map(|(_, key)| parse_l0_range(key))
        .collect()
}

/// Drop every candidate whose id falls inside any registered L0 range — its
/// rows are already served by files the query scans.
fn dedup_candidates(candidates: Vec<SegmentMeta>, l0_ranges: &[(i64, i64)]) -> Vec<SegmentMeta> {
    if l0_ranges.is_empty() {
        return candidates;
    }
    candidates
        .into_iter()
        .filter(|c| {
            !l0_ranges
                .iter()
                .any(|&(min, max)| c.id >= min && c.id <= max)
        })
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

/// Follower seam: scan this node's assigned segments and return tables the
/// existing union/exec path consumes alongside the storage tables.
///
/// Any failure — an id that no longer resolves, a fetch error, a decode
/// error — fails the WHOLE query: a silently missing segment is silent
/// partial data, the exact prod bug class this design exists to kill.
pub async fn search(
    query: Arc<super::QueryParams>,
    schema: Arc<Schema>,
    segment_ids: &[i64],
    sorted_by_time: bool,
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
    let metas = resolve_assigned(segment_ids, rows, &query.org_id, &query.stream_name)?;

    let mut scan_stats = ScanStats::new();
    scan_stats.files = metas.len() as i64;
    scan_stats.querier_files = scan_stats.files;

    // Warm the disk-backed file cache exactly the way parquet files are
    // warmed: cache hits are counted into scan_stats, misses are queued for
    // background download so repeated dashboards hit the disk cache. The
    // reads below go through the same cache and fall back to object storage
    // for anything not yet downloaded.
    let account = SEGMENT_STORAGE_ACCOUNT.to_string();
    let cache_tuples = metas
        .iter()
        .map(|m| (-m.id, &account, &m.object_key, m.size, m.max_ts, 0i64))
        .collect::<Vec<_>>();
    let (_cache_type, cache_hits, cache_misses) =
        ::search::file_cache::cache_files(trace_id, &cache_tuples, &mut scan_stats, "segment")
            .await;
    metrics::QUERY_DISK_CACHE_HIT_COUNT
        .with_label_values(&[query.org_id.as_str(), query.stream_type.as_str(), "segment"])
        .inc_by(cache_hits);
    metrics::QUERY_DISK_CACHE_MISS_COUNT
        .with_label_values(&[query.org_id.as_str(), query.stream_type.as_str(), "segment"])
        .inc_by(cache_misses);

    // check memory circuit breaker before decoding anything
    ingester::check_memory_circuit_breaker().map_err(|e| Error::ResourceError(e.to_string()))?;

    // Fetch + decode SEQUENTIALLY: peak memory is one decoded segment plus
    // the kept batches (this stream's in-range rows — the data itself),
    // never `segments x decoded-size`. The kept batches themselves are
    // bounded by [`SEGMENT_SCAN_MAX_BYTES`].
    let mut kept_batches: Vec<RecordBatch> = Vec::new();
    let mut kept_bytes: usize = 0;
    for meta in &metas {
        let bytes = file_data::get(SEGMENT_STORAGE_ACCOUNT, &meta.object_key, None)
            .await
            .map_err(|e| {
                Error::Message(format!(
                    "[SEGMENT:SCAN] fetch segment object {} (id {}) failed: {e}",
                    meta.object_key, meta.id
                ))
            })?;
        scan_stats.compressed_size += bytes.len() as i64;
        let object_key = meta.object_key.clone();
        // zstd + arrow ipc over up to a whole flush buffer — keep it off the
        // async workers
        let frames = tokio::task::spawn_blocking(move || decode_named(&bytes, &object_key))
            .await
            .map_err(|e| {
                Error::Message(format!(
                    "[SEGMENT:SCAN] decode task for segment object {} (id {}) did not complete: {e}",
                    meta.object_key, meta.id
                ))
            })??;
        for batch in keep_stream_frames(
            frames,
            &query.org_id,
            query.stream_type,
            &query.stream_name,
            query.time_range,
        ) {
            scan_stats.records += batch.num_rows() as i64;
            scan_stats.original_size += batch.get_array_memory_size() as i64;
            push_within_budget(
                &mut kept_batches,
                &mut kept_bytes,
                batch,
                SEGMENT_SCAN_MAX_BYTES,
                &query.org_id,
                query.stream_type,
                &query.stream_name,
            )?;
        }
        tokio::task::coop::consume_budget().await;
    }

    log::info!(
        "[trace_id {trace_id}] segments_scan: {}/{}/{} loaded {} segments, kept {} batches, records {}, scan_size {}, took {} ms",
        query.org_id,
        query.stream_type,
        query.stream_name,
        metas.len(),
        kept_batches.len(),
        scan_stats.records,
        scan_stats.original_size,
        load_start.elapsed().as_millis(),
    );

    if kept_batches.is_empty() {
        return Ok((vec![], scan_stats));
    }

    let tables = build_tables_from_batches(
        trace_id,
        kept_batches,
        schema,
        sorted_by_time,
        index_condition,
        fst_fields,
        query.time_range,
    )?;
    Ok((tables, scan_stats))
}

/// Wrap `decode_segment` so every failure names the object involved, and
/// drop the header (registration metadata already carries it).
fn decode_named(bytes: &[u8], object_key: &str) -> Result<Vec<SegmentFrame>> {
    let (_header, frames) = decode_segment(bytes).map_err(|e| {
        Error::Message(format!(
            "[SEGMENT:SCAN] decode segment object {object_key} failed: {e:#}"
        ))
    })?;
    Ok(frames)
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

/// Fold a kept batch into the accumulator, enforcing the scan bytes budget
/// with [`RecordBatchExt::size`] accounting. `budget` is a parameter so
/// tests can shrink it; the public path always passes
/// [`SEGMENT_SCAN_MAX_BYTES`]. Over budget is a hard error, never a
/// truncation (a capped-off subset would be silent partial data).
fn push_within_budget(
    kept: &mut Vec<RecordBatch>,
    kept_bytes: &mut usize,
    batch: RecordBatch,
    budget: usize,
    org_id: &str,
    stream_type: StreamType,
    stream_name: &str,
) -> Result<()> {
    *kept_bytes = kept_bytes.saturating_add(batch.size());
    if *kept_bytes > budget {
        return Err(Error::Message(format!(
            "[SEGMENT:SCAN] {org_id}/{stream_type}/{stream_name}: kept segment batches reach {} bytes, over the per-query scan budget {budget} — the segment builder backlog is too large to search safely",
            *kept_bytes
        )));
    }
    kept.push(batch);
    Ok(())
}

/// Keep only frames of the queried stream that overlap the query time range
/// (file-overlap semantics: closed range test, mirroring the WAL parquet
/// skip check), dropping empty batches.
fn keep_stream_frames(
    frames: Vec<SegmentFrame>,
    org_id: &str,
    stream_type: StreamType,
    stream_name: &str,
    time_range: (i64, i64),
) -> Vec<RecordBatch> {
    let (min_ts, max_ts) = time_range;
    frames
        .into_iter()
        .filter(|f| {
            f.org == org_id
                && f.stream_type == stream_type
                && f.stream == stream_name
                && !((min_ts, max_ts) != (0, 0) && (f.min_ts > max_ts || f.max_ts < min_ts))
                && f.batch.num_rows() > 0
        })
        .map(|f| f.batch)
        .collect()
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
    use arrow_schema::{DataType, Field};
    use datafusion::{physical_plan::collect, prelude::SessionContext};
    use segment_wal::format::{SegmentHeader, encode_segment};

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

    // ---- provenance parsing ----

    #[test]
    fn parse_l0_range_valid_forms() {
        // plain uuid middle, .vix extension, nested path
        assert_eq!(
            parse_l0_range(
                "files/org1/logs/app1/2026/07/31/10/l0_7f9c24e5-1a2b-4c3d-8e9f-000000000001_2_9_3.vix"
            ),
            Some((2, 9))
        );
        // "multi" middle, other extension — extension-agnostic
        assert_eq!(
            parse_l0_range("files/o/logs/s/l0_multi_10_10_1.parquet"),
            Some((10, 10))
        );
        // no extension at all
        assert_eq!(parse_l0_range("l0_multi_5_7_2"), Some((5, 7)));
        // middle containing underscores parses from the END
        assert_eq!(
            parse_l0_range("l0_node_a_b_100_200_4.vix"),
            Some((100, 200))
        );
        // large ids
        assert_eq!(
            parse_l0_range("l0_x_9223372036854775806_9223372036854775807_1.vix"),
            Some((9223372036854775806, 9223372036854775807))
        );
    }

    #[test]
    fn parse_l0_range_rejects_junk_without_panicking() {
        for key in [
            "",
            "/",
            "files/o/logs/s/1234.parquet",    // not l0_
            "files/o/logs/s/al0_x_1_2_3.vix", // prefix not at start
            "l0_2_9_3.vix",                   // missing uuid-or-multi field
            "l0_x_2_9.vix",                   // missing {n}
            "l0_x_a_9_3.vix",                 // non-numeric min
            "l0_x_2_b_3.vix",                 // non-numeric max
            "l0_x_2_9_c.vix",                 // non-numeric n
            "l0_x_9_2_3.vix",                 // min > max
            "l0_x_0_9_3.vix",                 // min < 1 (ids are >= 1)
            "l0_x_-5_9_3.vix",                // negative min
            "l0_x_2_9_3.",                    // trailing dot only
            "l0__2_9_3",                      // empty middle is still a field
        ] {
            let got = parse_l0_range(key);
            match key {
                // empty middle part is tolerated shape-wise: 5 fields present
                "l0__2_9_3" => assert_eq!(got, Some((2, 9)), "key {key:?}"),
                "l0_x_2_9_3." => assert_eq!(got, Some((2, 9)), "key {key:?}"),
                _ => assert_eq!(got, None, "key {key:?} must not parse"),
            }
        }
    }

    // ---- dedup rule ----

    #[test]
    fn dedup_drops_inside_keeps_outside_multiple_ranges() {
        let candidates = vec![
            seg_meta(1, 0, 10),
            seg_meta(2, 0, 10), // == min of [2,9] -> dropped
            seg_meta(5, 0, 10), // inside [2,9] -> dropped
            seg_meta(9, 0, 10), // == max of [2,9] -> dropped
            seg_meta(10, 0, 10),
            seg_meta(15, 0, 10), // inside [15,15] -> dropped
            seg_meta(16, 0, 10),
        ];
        let ranges = vec![(2, 9), (15, 15)];
        let survivors = dedup_candidates(candidates, &ranges);
        assert_eq!(
            survivors.iter().map(|m| m.id).collect::<Vec<_>>(),
            vec![1, 10, 16]
        );
    }

    #[test]
    fn dedup_with_no_ranges_keeps_everything() {
        let candidates = vec![seg_meta(1, 0, 10), seg_meta(2, 0, 10)];
        let survivors = dedup_candidates(candidates, &[]);
        assert_eq!(survivors.len(), 2);
    }

    #[test]
    fn l0_ranges_only_counted_from_snapshot_members() {
        // the L0 registered after the snapshot (id NOT in snapshot) must be
        // ignored — honoring it would drop a segment whose rows the query
        // will not scan from files (a gap)
        let snapshot: HashSet<i64> = [100, 101].into_iter().collect();
        let files = vec![
            (100i64, "files/o/logs/s/l0_u_2_9_3.vix"),   // in snapshot
            (999i64, "files/o/logs/s/l0_u_10_20_2.vix"), // NOT in snapshot
            (101i64, "files/o/logs/s/plain_file.parquet"), // in snapshot, not l0
        ];
        let ranges = l0_ranges_in_snapshot(files.into_iter(), &snapshot);
        assert_eq!(ranges, vec![(2, 9)]);
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
        let kept = keep_stream_frames(frames, "org1", StreamType::Logs, "app1", (100, 200));
        let rows: usize = kept.iter().map(|b| b.num_rows()).sum();
        assert_eq!(kept.len(), 2);
        assert_eq!(rows, 3);
    }

    #[test]
    fn push_within_budget_rejects_over_budget_and_keeps_nothing_extra() {
        let mut kept: Vec<RecordBatch> = Vec::new();
        let mut kept_bytes = 0usize;
        let first = ts_batch("v", &[1, 2], Some(&[1, 2]));
        // budget admits exactly the first batch (exceed is strictly greater)
        let budget = first.size();
        push_within_budget(
            &mut kept,
            &mut kept_bytes,
            first,
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
            ts_batch("v", &[3], Some(&[3])),
            budget,
            "org1",
            StreamType::Logs,
            "app1",
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("org1/logs/app1") && msg.contains(&format!("budget {budget}")),
            "error must name the stream and the budget: {msg}"
        );
        assert_eq!(kept.len(), 1, "the over-budget batch must not be kept");
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
        let decoded = decode_named(&encoded, "wal_segments/node-e2e/1").expect("decode segment");
        assert_eq!(decoded.len(), 4);

        let kept = keep_stream_frames(decoded, "org1", StreamType::Logs, "app1", (0, 500));
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
    fn decode_named_error_names_the_object() {
        let err = decode_named(b"definitely not a segment", "wal_segments/node-x/7").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("wal_segments/node-x/7"),
            "error must name the object key: {msg}"
        );
    }
}
