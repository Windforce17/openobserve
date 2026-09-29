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

use std::sync::Arc;

pub use ::search::file_cache::{cache_files, calc_target_partitions};
use arrow_schema::Schema;
use config::{
    cluster::LOCAL_NODE,
    get_config,
    meta::{
        inverted_index::IndexOptimizeMode,
        search::{ScanStats, StorageType},
        stream::FileKey,
    },
    metrics::{self, QUERY_PARQUET_CACHE_RATIO_NODE},
    utils::size::bytes_to_human_readable,
};
use datafusion::{datasource::TableProvider, execution::cache::cache_manager::FileStatisticsCache};
use infra::{
    cache::file_data,
    errors::{Error, ErrorCodes},
};
use itertools::Itertools;
use tracing::Instrument;

pub use crate::service::search::vix::vix_search;
use crate::service::{
    file_list,
    search::{
        bloom_pruner,
        index::IndexCondition,
        inspector::{SearchInspectorFieldsBuilder, search_inspector_fields},
    },
};

/// Storage-branch search result: the registered tables, the scan stats of
/// the files actually opened, and the shortfall when
/// `ZO_STORAGE_SCAN_MAX_BYTES` truncated the file set.
pub type StorageSearchTable = infra::errors::Result<(
    Vec<Arc<dyn TableProvider>>,
    ScanStats,
    Option<StorageScanShortfall>,
)>;

/// Files the storage scan branch skipped because the query's compressed
/// bytes exceeded `ZO_STORAGE_SCAN_MAX_BYTES`. Reported through the standard
/// partial-results channel (`is_partial` + message), like
/// [`super::segments_scan::SegmentShortfall`]: degraded and honest beats a
/// follower that reserves the whole shared DataFusion pool for one lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageScanShortfall {
    pub stream: String,
    pub skipped_files: usize,
    pub skipped_bytes: usize,
    pub kept_files: usize,
    pub kept_bytes: usize,
    pub budget: usize,
    /// `max_ts` of the oldest kept file (µs): results are complete from here
    /// to the end of the range and exclude older data.
    pub oldest_kept_ts: i64,
}

impl StorageScanShortfall {
    pub fn message(&self) -> String {
        format!(
            "storage scan budget: {} files ({}) of {} were skipped — the scan branch of this \
             query on {} exceeded ZO_STORAGE_SCAN_MAX_BYTES ({}); results cover the NEWEST {} \
             files ({}) from {} onward and exclude older unindexed data — narrow the time range \
             or filter on an indexed field",
            self.skipped_files,
            bytes_to_human_readable(self.skipped_bytes as f64),
            self.skipped_files + self.kept_files,
            self.stream,
            bytes_to_human_readable(self.budget as f64),
            self.kept_files,
            bytes_to_human_readable(self.kept_bytes as f64),
            chrono::DateTime::<chrono::Utc>::from_timestamp_micros(self.oldest_kept_ts)
                .map(|t| t.format("%Y-%m-%dT%H:%M:%SZ").to_string())
                .unwrap_or_else(|| self.oldest_kept_ts.to_string()),
        )
    }
}

/// The cap applies ONLY to row-returning `LIMIT` shapes — the optimizer's
/// `SimpleSelect(n > 0, _)`. Truncating the scan branch to the newest files
/// leaves "the newest n matching rows" what it is for a log search; for a
/// count, histogram, percentile or GROUP BY it silently changes the number
/// (2026-09-29: a 6 h `p99 by service` covered 10 % of its rows, a 24 h
/// filtered count came back 1.5 % short, both flagged `partial` and both
/// wrong). Aggregates run the whole scan branch, bounded by memory admission,
/// or fail loudly — never a truncated value. Owner decision 2026-09-30.
fn scan_cap_budget(idx_optimize_rule: &Option<IndexOptimizeMode>, configured: usize) -> usize {
    match idx_optimize_rule {
        Some(IndexOptimizeMode::SimpleSelect(limit, _)) if *limit > 0 => configured,
        _ => 0,
    }
}

/// Keep the NEWEST files whose compressed bytes fit `budget` (always at least
/// one), in `max_ts` DESC order, and report the rest. Newest-first because a
/// scan-branch flood means the index could not prune a wide window: the
/// recent end is what the query most likely wants, and the caller sees
/// exactly where the coverage stops.
fn apply_storage_scan_cap(
    files: &mut Vec<FileKey>,
    stream: &str,
    budget: usize,
) -> Option<StorageScanShortfall> {
    if budget == 0 {
        return None;
    }
    let total: usize = files
        .iter()
        .map(|f| f.meta.compressed_size.max(0) as usize)
        .sum();
    if total <= budget {
        return None;
    }
    files.sort_unstable_by(|a, b| b.meta.max_ts.cmp(&a.meta.max_ts).then(b.id.cmp(&a.id)));
    let mut kept_bytes = 0usize;
    let mut kept = 0usize;
    for file in files.iter() {
        let bytes = file.meta.compressed_size.max(0) as usize;
        if kept > 0 && kept_bytes + bytes > budget {
            break;
        }
        kept_bytes += bytes;
        kept += 1;
    }
    let oldest_kept_ts = files[kept - 1].meta.max_ts;
    let skipped_files = files.len() - kept;
    files.truncate(kept);
    Some(StorageScanShortfall {
        stream: stream.to_string(),
        skipped_files,
        skipped_bytes: total - kept_bytes,
        kept_files: kept,
        kept_bytes,
        budget,
        oldest_kept_ts,
    })
}

/// search in remote object storage
#[tracing::instrument(name = "service:search:grpc:storage", skip_all, fields(org_id = query.org_id, stream_name = query.stream_name))]
#[allow(clippy::too_many_arguments)]
pub async fn search(
    query: Arc<super::QueryParams>,
    schema: Arc<Schema>,
    file_list: &[FileKey],
    sorted_by_time: bool,
    collect_stat: bool,
    file_stat_cache: Option<Arc<dyn FileStatisticsCache>>,
    mut index_condition: Option<IndexCondition>,
    mut fst_fields: Vec<String>,
    idx_optimize_rule: Option<IndexOptimizeMode>,
) -> StorageSearchTable {
    let super::QueryParams {
        trace_id,
        org_id,
        stream_type,
        stream_name,
        use_inverted_index,
        work_group,
        ..
    } = query.as_ref();
    let enter_span = tracing::span::Span::current();
    log::info!("[trace_id {trace_id}] search->storage: enter");
    let mut files = file_list.to_vec();
    if files.is_empty() {
        return Ok((vec![], ScanStats::default(), None));
    }
    let original_files_len = files.len();
    log::info!(
        "[trace_id {trace_id}] search->storage: stream {org_id}/{stream_type}/{stream_name}, load file_list num {}",
        files.len(),
    );

    let mut idx_took = 0;
    let mut is_add_filter_back = false;
    // index_condition is None when the filter has nothing index-extractable
    // (use_inverted_index is false then) — never unwrap it unconditionally:
    // a None here panicked the whole storage partition and the query
    // silently degraded to WAL-only results.
    let condition_all = index_condition
        .as_ref()
        .is_some_and(IndexCondition::is_condition_all);

    // The vix index also answers the no-filter SimpleSelect (bare
    // `SELECT * ORDER BY _timestamp LIMIT n`, condition ALL): its exact
    // `_timestamp` candidates prune the file list to the global top-N and
    // narrow the winners to row selections (file-level early termination +
    // row-level late materialization). Every other condition-ALL shape has
    // nothing for the index to answer.
    let vix_applicable =
        vix_search_applicable(*use_inverted_index, condition_all, &idx_optimize_rule);
    let scan_cap = scan_cap_budget(
        &idx_optimize_rule,
        get_config().limit.storage_scan_max_bytes,
    );
    if vix_applicable {
        // check vix inverted index
        (idx_took, is_add_filter_back, ..) = vix_search(
            query.clone(),
            &mut files,
            index_condition.clone(),
            idx_optimize_rule,
        )
        .await?;

        log::info!(
            "{}",
            search_inspector_fields(
                format!(
                    "[trace_id {trace_id}] search->vix: stream {org_id}/{stream_type}/{stream_name}, inverted index reduced file_list num to {} in {idx_took} ms",
                    files.len(),
                ),
                SearchInspectorFieldsBuilder::new()
                    .trace_id(trace_id.to_string())
                    .node_name(LOCAL_NODE.name.clone())
                    .component("storage inverted index reduced file_list num".to_string())
                    .search_role("follower".to_string())
                    .duration(idx_took)
                    .desc(format!(
                        "inverted index reduced file_list from {original_files_len} to {} in {idx_took} ms",
                        files.len(),
                    ))
                    .build()
            )
        );
    }

    // set index_condition to None, means we do not need to add filter back.
    // EXCEPTION (#40): when the vix step never ran over a REAL (non-ALL)
    // extracted condition — index-off stream types keep
    // `use_inverted_index` false — the condition MUST stay: the IndexRule
    // already removed those conjuncts from the physical plan, so the scan
    // tables are their only remaining evaluation point (nulling it here
    // would silently return unfiltered rows).
    let index_step_skipped_with_condition =
        !vix_applicable && !condition_all && index_condition.is_some();
    if !is_add_filter_back && !index_step_skipped_with_condition {
        index_condition = None;
        fst_fields = vec![];
    }

    let cfg = get_config();
    let mut scan_stats = match file_list::calculate_files_size(&files).await {
        Ok(size) => size,
        Err(err) => {
            log::error!("[trace_id {trace_id}] calculate files size error: {err}",);
            return Err(Error::ErrorCode(ErrorCodes::ServerInternalError(
                "calculate files size error".to_string(),
            )));
        }
    };

    log::info!(
        "[trace_id {trace_id}] search->storage: stream {org_id}/{stream_type}/{stream_name}, load files {}, scan_size {}, compressed_size {}",
        scan_stats.files,
        scan_stats.original_size,
        scan_stats.compressed_size
    );

    // Per-query byte budget on the scan branch (ZO_STORAGE_SCAN_MAX_BYTES),
    // row-returning LIMIT shapes only (`scan_cap_budget`): decided here,
    // before any IO or plan, from the file_list sizes already in hand. The
    // kept set is re-measured so scan_stats describe what runs.
    let stream_key = format!("{org_id}/{stream_type}/{stream_name}");
    let scan_shortfall = apply_storage_scan_cap(&mut files, &stream_key, scan_cap);
    if let Some(shortfall) = &scan_shortfall {
        scan_stats = match file_list::calculate_files_size(&files).await {
            Ok(size) => size,
            Err(err) => {
                log::error!("[trace_id {trace_id}] calculate files size error: {err}",);
                return Err(Error::ErrorCode(ErrorCodes::ServerInternalError(
                    "calculate files size error".to_string(),
                )));
            }
        };
        metrics::QUERY_STORAGE_SCAN_CAPPED_TOTAL
            .with_label_values(&[org_id.as_str(), stream_type.as_str()])
            .inc();
        log::warn!(
            "[trace_id {trace_id}] search->storage: {} (kept compressed_size {})",
            shortfall.message(),
            scan_stats.compressed_size
        );
    }

    // check memory circuit breaker
    ingester::check_memory_circuit_breaker().map_err(|e| Error::ResourceError(e.to_string()))?;

    // load files to local cache
    let cache_start = std::time::Instant::now();
    let (cache_type, cache_hits, cache_misses) = cache_files(
        &query.trace_id,
        &files
            .iter()
            .map(|f| {
                (
                    f.id,
                    &f.account,
                    &f.key,
                    f.meta.compressed_size,
                    f.meta.max_ts,
                    f.meta.records,
                )
            })
            .collect_vec(),
        &mut scan_stats,
        "parquet",
    )
    .instrument(enter_span.clone())
    .await;

    // report cache hit and miss metrics
    metrics::QUERY_DISK_CACHE_HIT_COUNT
        .with_label_values(&[org_id.as_str(), stream_type.as_str(), "parquet"])
        .inc_by(cache_hits);
    metrics::QUERY_DISK_CACHE_MISS_COUNT
        .with_label_values(&[org_id.as_str(), stream_type.as_str(), "parquet"])
        .inc_by(cache_misses);

    scan_stats.idx_took = idx_took as i64;
    scan_stats.querier_files = scan_stats.files;
    let cached_ratio = (scan_stats.querier_memory_cached_files
        + scan_stats.querier_disk_cached_files) as f64
        / scan_stats.querier_files as f64;

    let download_msg = if cache_type == file_data::CacheType::None {
        "".to_string()
    } else {
        format!(" downloading others into {cache_type:?} in background,")
    };
    log::info!(
        "{}",
        search_inspector_fields(
            format!(
                "[trace_id {trace_id}] search->storage: stream {org_id}/{stream_type}/{stream_name}, load files {}, memory cached {}, disk cached {}, cached ratio {}%,{download_msg} took: {} ms",
                scan_stats.querier_files,
                scan_stats.querier_memory_cached_files,
                scan_stats.querier_disk_cached_files,
                (cached_ratio * 100.0) as usize,
                cache_start.elapsed().as_millis()
            ),
            SearchInspectorFieldsBuilder::new()
                .trace_id(trace_id.to_string())
                .node_name(LOCAL_NODE.name.clone())
                .component("storage load files".to_string())
                .search_role("follower".to_string())
                .duration(cache_start.elapsed().as_millis() as usize)
                .desc(format!(
                    "load files {}, memory cached {}, disk cached {}, scan_size {}, compressed_size {}",
                    scan_stats.querier_files,
                    scan_stats.querier_memory_cached_files,
                    scan_stats.querier_disk_cached_files,
                    bytes_to_human_readable(scan_stats.original_size as f64),
                    bytes_to_human_readable(scan_stats.compressed_size as f64)
                ))
                .build()
        )
    );

    if scan_stats.querier_files > 0 {
        QUERY_PARQUET_CACHE_RATIO_NODE
            .with_label_values(&[org_id.as_str(), stream_type.as_str()])
            .observe(cached_ratio);
    }

    let target_partitions =
        calc_target_partitions(cfg.limit.cpu_num, cfg.limit.query_thread_num, cached_ratio);

    log::info!(
        "[trace_id {trace_id}] search->storage: session target_partitions: {target_partitions}"
    );

    let session = config::meta::search::Session {
        id: format!("{trace_id}-storage"),
        storage_type: StorageType::Memory,
        work_group: work_group.clone(),
        target_partitions,
    };

    let start = std::time::Instant::now();
    let tables = super::create_tables_from_files(
        files,
        session,
        query.clone(),
        schema,
        sorted_by_time,
        collect_stat,
        file_stat_cache,
        index_condition,
        fst_fields,
        || {},
    )
    .await?;

    log::info!(
        "{}",
        search_inspector_fields(
            format!(
                "[trace_id {trace_id}] search->storage: create tables took: {} ms",
                start.elapsed().as_millis()
            ),
            SearchInspectorFieldsBuilder::new()
                .trace_id(trace_id.to_string())
                .node_name(LOCAL_NODE.name.clone())
                .component("storage create tables".to_string())
                .search_role("follower".to_string())
                .duration(start.elapsed().as_millis() as usize)
                .build()
        )
    );
    Ok((tables, scan_stats, scan_shortfall))
}

/// Whether the vix index step runs for this query shape: any real (non-ALL)
/// condition, or a condition-ALL SimpleSelect — the one optimize mode that
/// works without terms, ranking rows purely by `_timestamp`.
fn vix_search_applicable(
    use_inverted_index: bool,
    condition_all: bool,
    idx_optimize_rule: &Option<IndexOptimizeMode>,
) -> bool {
    use_inverted_index
        && (!condition_all
            || matches!(idx_optimize_rule, Some(IndexOptimizeMode::SimpleSelect(..))))
}

/// Prune the file list with per-field and policy-scoped composite blooms.
#[tracing::instrument(name = "service:search:grpc:storage:check_bloom_filter", skip_all)]
pub async fn check_bloom_filter(
    query: Arc<super::QueryParams>,
    file_list: &mut Vec<FileKey>,
    index_condition: Option<&IndexCondition>,
    bloom_indexed_fields: Vec<String>,
) -> Result<(usize, bool), Error> {
    let cfg = get_config();
    if !cfg.common.bloom_filter_enabled || file_list.is_empty() {
        return Ok((0, false));
    }
    let Some(index_condition) = index_condition else {
        return Ok((0, false));
    };
    let composite_scope = config::vix_bloom_composite_scope(&cfg);
    let auto_id_scope =
        cfg.common.vix_bloom_only_auto_id_only && cfg.common.vix_bloom_only_auto_ratio > 0.0;
    let auto_id_never = cfg
        .common
        .vix_bloom_only_never
        .split(',')
        .map(str::trim)
        .filter(|field| !field.is_empty())
        .map(str::to_owned)
        .collect::<std::collections::HashSet<_>>();

    // Decide whether there is real Bloom work before transferring ownership
    // of the potentially large vector. In particular, ordinary predicates
    // outside a selective composite scope return without a second O(files)
    // clone and without recording a Bloom run.
    if !bloom_pruner::is_applicable(
        index_condition,
        &bloom_indexed_fields,
        &composite_scope,
        auto_id_scope,
        &auto_id_never,
    ) {
        return Ok((0, false));
    }

    let start = std::time::Instant::now();
    let before_num = file_list.len();
    let files = std::mem::take(file_list);
    *file_list = bloom_pruner::prune(
        &query.trace_id,
        &query.org_id,
        query.stream_type,
        &query.stream_name,
        files,
        index_condition,
        bloom_indexed_fields,
        &composite_scope,
        auto_id_scope,
        &auto_id_never,
    )
    .await;

    // metrics
    let elapsed = start.elapsed();
    let after_num = file_list.len();
    config::metrics::BLOOM_PRUNE_KEEP_RATIO
        .with_label_values(&[query.org_id.as_str(), query.stream_type.as_str()])
        .observe(after_num as f64 / before_num as f64);

    config::metrics::BLOOM_PRUNE_DURATION
        .with_label_values(&[query.org_id.as_str(), query.stream_type.as_str()])
        .observe(elapsed.as_secs_f64());

    Ok((elapsed.as_millis() as usize, true))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vix_search_applicable() {
        let select = Some(IndexOptimizeMode::SimpleSelect(51, false));
        let count = Some(IndexOptimizeMode::SimpleCount);

        // real condition: always applicable, whatever the rule
        assert!(vix_search_applicable(true, false, &None));
        assert!(vix_search_applicable(true, false, &count));
        assert!(vix_search_applicable(true, false, &select));

        // condition ALL: only SimpleSelect has index work to do
        assert!(vix_search_applicable(true, true, &select));
        assert!(!vix_search_applicable(true, true, &None));
        assert!(!vix_search_applicable(true, true, &count));

        // inverted index off: never applicable
        assert!(!vix_search_applicable(false, false, &select));
        assert!(!vix_search_applicable(false, true, &select));
    }

    #[test]
    fn scan_cap_applies_only_to_row_returning_limit_shapes() {
        let budget = 4 << 30;
        assert_eq!(
            scan_cap_budget(&Some(IndexOptimizeMode::SimpleSelect(50, false)), budget),
            budget
        );
        assert_eq!(
            scan_cap_budget(&Some(IndexOptimizeMode::SimpleSelect(50, true)), budget),
            budget
        );
        // a LIMIT 0 select drops every row anyway: nothing to protect
        assert_eq!(
            scan_cap_budget(&Some(IndexOptimizeMode::SimpleSelect(0, false)), budget),
            0
        );
        // aggregates and unclassified plans: never truncated
        assert_eq!(
            scan_cap_budget(&Some(IndexOptimizeMode::SimpleCount), budget),
            0
        );
        assert_eq!(
            scan_cap_budget(
                &Some(IndexOptimizeMode::SimpleHistogram(0, 1, 1, 0)),
                budget
            ),
            0
        );
        assert_eq!(scan_cap_budget(&None, budget), 0);
        // disabled stays disabled
        assert_eq!(
            scan_cap_budget(&Some(IndexOptimizeMode::SimpleSelect(50, false)), 0),
            0
        );
    }

    fn file(id: i64, max_ts: i64, compressed: i64) -> FileKey {
        let mut f = FileKey::from_file_name(&format!("files/o/traces/s/2026/09/28/17/{id}.vix"));
        f.id = id;
        f.meta.max_ts = max_ts;
        f.meta.min_ts = max_ts - 1_000;
        f.meta.compressed_size = compressed;
        f
    }

    /// The cap keeps the NEWEST files that fit and reports the rest, so the
    /// answer is complete from the oldest kept file onward.
    #[test]
    fn storage_scan_cap_keeps_newest_prefix_and_reports_the_rest() {
        // insertion order is deliberately not time order
        let mut files = vec![
            file(1, 100, 40),
            file(2, 400, 40),
            file(3, 200, 40),
            file(4, 300, 40),
        ];
        let shortfall = apply_storage_scan_cap(&mut files, "o/traces/s", 100).expect("over budget");
        assert_eq!(
            files.iter().map(|f| f.id).collect::<Vec<_>>(),
            vec![2, 4],
            "newest two fit the budget"
        );
        assert_eq!(
            shortfall,
            StorageScanShortfall {
                stream: "o/traces/s".to_string(),
                skipped_files: 2,
                skipped_bytes: 80,
                kept_files: 2,
                kept_bytes: 80,
                budget: 100,
                oldest_kept_ts: 300,
            }
        );
        let message = shortfall.message();
        assert!(message.contains("2 files"), "{message}");
        assert!(message.contains("NEWEST 2 files"), "{message}");
        assert!(
            message.contains("1970-01-01T00:00:00Z"),
            "µs 300 formats: {message}"
        );
    }

    /// Within budget or disabled: untouched, no shortfall, order preserved.
    #[test]
    fn storage_scan_cap_is_a_no_op_within_budget_or_when_off() {
        let original = vec![file(1, 100, 40), file(2, 400, 40)];
        let mut files = original.clone();
        assert_eq!(apply_storage_scan_cap(&mut files, "s", 80), None);
        assert_eq!(files, original);
        let mut files = original.clone();
        assert_eq!(apply_storage_scan_cap(&mut files, "s", 0), None);
        assert_eq!(files, original);
    }

    /// A single file larger than the budget still runs: the cap bounds the
    /// set, it never empties it (an empty scan would be a silent blackout).
    #[test]
    fn storage_scan_cap_always_keeps_at_least_the_newest_file() {
        let mut files = vec![file(1, 100, 500), file(2, 200, 500)];
        let shortfall = apply_storage_scan_cap(&mut files, "s", 10).unwrap();
        assert_eq!(files.iter().map(|f| f.id).collect::<Vec<_>>(), vec![2]);
        assert_eq!((shortfall.kept_files, shortfall.skipped_files), (1, 1));
        assert_eq!(shortfall.kept_bytes, 500);
    }
}
