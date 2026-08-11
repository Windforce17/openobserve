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

//! `.vix` inverted-index read path.
//!
//! [`vix_search`] filters a file list through the per-file `.vix` indexes:
//! it opens each core file with [`vortex_index::VixReader`], evaluates the
//! [`IndexCondition`] into a per-row bitmap and attaches it to the [`FileKey`]
//! as a [`FileSelection`]. Files without a usable index (missing fields
//! skipped, partial fields, errors) keep the DataFusion filter
//! (`is_add_filter_back = true`).
//!
//! How the object is read is governed by `ZO_VIX_READ_MODE`
//! ([`source::VixReadMode`]):
//! - `ranged` (default): the reader fetches the puffin footer, the small dictionary DIRECTORY and
//!   only the FST-cell/postings/docs chunks the query touches, through the range-capable cache
//!   ladder (memory → disk → remote range GETs). Parsed readers are memoized in
//!   [`reader_cache::GLOBAL_CACHE`] and keep their lazily loaded FST cells, so hot files skip even
//!   the tail fetch. `cache_files` still enqueues whole-file background downloads — the DataFusion
//!   scan of matched files and repeat queries are then served from the local cache.
//! - `cached`: the whole object is downloaded through the file cache ladder and opened in memory
//!   (pre-F2 behavior).
//!
//! When an [`IndexOptimizeMode`] is set, the per-file evaluation instead runs
//! the matching aggregation fast path over the docs-blob columns (see
//! [`collect`]) and the per-file results merge exactly like the aggregate
//! semantics ([`MultiResultBuilder`]); SimpleSelect narrows the file list to
//! the exact global top-N rows via the [`pruner`]. Unfiltered full-range
//! single-field TopN/Distinct are answered from the term dictionary alone
//! (`field_value_counts`), with docs-column and `_source` fallbacks per
//! file.
//!
//! Only core files (the data file itself ends in `.vix`) carry an index —
//! the file IS the index. Other data files (`.parquet`/`.vortex`) are
//! index-less here: they keep the DataFusion filter and are answered by the
//! scan path.
//!
//! Per-file evaluation fans out behind a semaphore of `ZO_VIX_SEARCH_CONCURRENCY`
//! permits (default 4x CPU cores, capped at 64): each file's work is a few
//! small — usually locally cached — range reads plus microseconds of CPU, so
//! the sweet spot sits well above the core count. Only `SimpleSelect`
//! partitions files into multiple groups (its global top-N prunes remaining
//! groups between rounds); every other mode evaluates all files as one group.

pub mod cache;
mod collect;

mod partition;
mod pruner;
pub mod reader_cache;
mod result;
pub mod source;

use std::{collections::HashSet, sync::Arc};

use arrow::buffer::BooleanBuffer;
use config::{
    cluster::LOCAL_NODE,
    get_config,
    meta::{
        inverted_index::IndexOptimizeMode,
        search::ScanStats,
        stream::{FileKey, FileSelection, RowIdBitmap, StreamType},
    },
    metrics::{self, QUERY_PARQUET_CACHE_RATIO_NODE},
    utils::size::bytes_to_human_readable,
};
use futures::{StreamExt, stream};
use hashbrown::HashMap;
use infra::{cache::file_data, errors::Error};
use itertools::Itertools;
pub use result::{MultiResult, MultiResultBuilder, VixSearchResult};
use tokio::sync::Semaphore;
use tokio_stream::StreamExt as _;
use vortex_index::VixReader;

use self::{
    cache::{self as vix_result_cache, CacheEntry},
    partition::partition_vix_files,
    source::{LadderRangeSource, VixReadMode, vix_read_mode},
};
use crate::{
    file_cache::{cache_files, calc_target_partitions},
    index::{FieldCap, IndexCondition},
    inspector::{SearchInspectorFieldsBuilder, search_inspector_fields},
    types::QueryParams,
};

/// Filter file list using the vix inverted index.
///
/// Contract on return: `file_list` holds exactly the files the index could
/// NOT answer — they must be scanned (with the filter re-applied when
/// `is_add_filter_back` is true). Under an aggregate [`IndexOptimizeMode`]
/// a fully-answered file is REMOVED from the list and its contribution
/// lives in the returned [`MultiResult`]; per-file failures (partial
/// fields, missing docs columns, IO errors after one retry, skipped
/// conditions) leave the file in the list — the caller degrades those to
/// the scan branch so a per-file condition never fails the whole query and
/// partial aggregates are impossible. A PANICKED evaluation task is not a
/// per-file condition: the remaining files are restored and an `Err` is
/// returned (both callers fail loudly).
#[tracing::instrument(name = "service:search:grpc:storage:vix_search", skip_all)]
pub async fn vix_search(
    query: Arc<QueryParams>,
    file_list: &mut Vec<FileKey>,
    index_condition: Option<IndexCondition>,
    idx_optimize_mode: Option<IndexOptimizeMode>,
) -> Result<(usize, bool, MultiResult), Error> {
    let start = std::time::Instant::now();
    let cfg = get_config();
    let trace_id = &query.trace_id;
    let read_mode = vix_read_mode();

    // Cache the corresponding index files
    let mut scan_stats = ScanStats::new();
    let mut file_list_map = file_list
        .drain(..)
        .map(|f| (f.key.clone(), f))
        .collect::<HashMap<_, _>>();
    // #27: a SimpleSelect over condition-ALL bounds its winners from
    // file_list metadata alone — files that provably cannot reach the
    // top-`limit` are dropped before they are cached, opened, or scanned
    // (zero index fetches for them).
    if let Some(IndexOptimizeMode::SimpleSelect(limit, ascend)) = &idx_optimize_mode
        && *limit > 0
        && index_condition
            .as_ref()
            .is_some_and(|condition| condition.is_condition_all())
    {
        pruner::metadata_preprune(
            trace_id,
            &mut file_list_map,
            *limit,
            *ascend,
            query.time_range,
        );
    }
    // Only core files carry an index (the data file itself). Legacy
    // parquet/vortex files — including v1-era ones with a sibling index
    // object still in storage — are index-less: they keep the DataFusion
    // filter (add-filter-back) and are answered by the scan path.
    let index_files = file_list_map
        .iter()
        .filter_map(|(_, f)| {
            if is_core_file(&f.key) {
                scan_stats.compressed_size += f.meta.compressed_size;
                Some(f.clone())
            } else {
                None
            }
        })
        .collect_vec();
    scan_stats.querier_files = index_files.len() as i64;
    let (cache_type, cache_hits, cache_misses) = cache_files(
        &query.trace_id,
        &index_files
            .iter()
            .map(|f| {
                // the core file IS the index object: whole-object size
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
        "index",
    )
    .await;

    // report cache hit and miss metrics
    metrics::QUERY_DISK_CACHE_HIT_COUNT
        .with_label_values(&[query.org_id.as_str(), query.stream_type.as_str(), "index"])
        .inc_by(cache_hits);
    metrics::QUERY_DISK_CACHE_MISS_COUNT
        .with_label_values(&[query.org_id.as_str(), query.stream_type.as_str(), "index"])
        .inc_by(cache_misses);

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
                "[trace_id {trace_id}] search->vix: stream {}/{}/{}, load vix index files {}, index size: {}, memory cached {}, disk cached {}, cached ratio {}%,{download_msg} took: {} ms",
                query.org_id,
                query.stream_type,
                query.stream_name,
                scan_stats.querier_files,
                bytes_to_human_readable(scan_stats.compressed_size as f64),
                scan_stats.querier_memory_cached_files,
                scan_stats.querier_disk_cached_files,
                (cached_ratio * 100.0) as usize,
                start.elapsed().as_millis()
            ),
            SearchInspectorFieldsBuilder::new()
                .trace_id(query.trace_id.to_string())
                .node_name(LOCAL_NODE.name.clone())
                .component("vix load files".to_string())
                .search_role("follower".to_string())
                .duration(start.elapsed().as_millis() as usize)
                .desc(format!(
                    "load vix index files {}, memory cached {}, disk cached {}",
                    scan_stats.querier_files,
                    scan_stats.querier_memory_cached_files,
                    scan_stats.querier_disk_cached_files,
                ))
                .build()
        )
    );

    if scan_stats.querier_files > 0 {
        QUERY_PARQUET_CACHE_RATIO_NODE
            .with_label_values(&[&query.org_id, &StreamType::Index.to_string()])
            .observe(cached_ratio);
    }

    let target_partitions =
        calc_target_partitions(cfg.limit.cpu_num, cfg.limit.query_thread_num, cached_ratio);
    // Per-file index evaluation is a few small (usually cached) IO waits
    // plus microseconds of CPU — far more IO- than CPU-bound. Fan it out
    // beyond the DataFusion partition count (`ZO_VIX_SEARCH_CONCURRENCY`).
    let eval_concurrency = cfg.limit.vix_search_concurrency.max(1);

    log::info!(
        "[trace_id {trace_id}] search->vix: session target_partitions: {target_partitions}, eval_concurrency: {eval_concurrency}",
    );

    let search_start = std::time::Instant::now();
    let mut is_add_filter_back = file_list_map.len() != index_files.len();
    if is_add_filter_back {
        log::info!(
            "[trace_id {trace_id}] search->vix: {} of {} files have no vix index, the filter will be added back to datafusion for them",
            file_list_map.len() - index_files.len(),
            file_list_map.len(),
        );
    }
    let time_range = query.time_range;
    let index_parquet_files =
        partition_vix_files(index_files, &idx_optimize_mode, target_partitions);

    // Per-query index-fetch accounting (ranged mode): every range GET issued
    // through sources THIS query opens ticks these counters. Fetches through
    // readers memoized by an EARLIER query tick that query's counters (and
    // always the global metrics) — hot readers cost ~0 fetches anyway.
    let fetch_stats = Arc::new(source::FetchStats::default());

    let mut no_more_files = false;
    let mut result_builder = MultiResultBuilder::new(&idx_optimize_mode, &index_parquet_files);
    log::info!(
        "[trace_id {trace_id}] search->vix: target_partitions: {target_partitions}, file_groups: {}",
        index_parquet_files.len(),
    );

    // Projected-cost bail-out for optimize-mode evaluations: a
    // low-selectivity condition (a service filter matching a large share of
    // rows) fetches dict cells + fat postings + timestamp chunks per file —
    // costlier than the columnar scan it is meant to beat. After a sample of
    // files, project the total fetch volume; past the cap, stop evaluating
    // and hand every not-yet-answered file to the scan branch (their exact
    // per-file answers already collected stay used — the optimize exec merges
    // index answers with scanned files). Files already answered are exact,
    // so bailing mid-flight never double-counts.
    let bail_bytes_cap = cfg.common.vix_eval_bail_bytes as u64;
    const BAIL_SAMPLE_FILES: usize = 32;
    let eval_bail = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let files_total: usize = index_parquet_files.iter().map(|group| group.len()).sum();
    let mut files_evaluated: usize = 0;
    // The scan alternative the bail hands files to is not free: it reads the
    // needed data columns of every remaining file AND (post per-file
    // fallback) re-applies the condition on them. Bailing is only a win when
    // the projected index cost exceeds what the scan would read — estimated
    // as a fraction of the window's compressed bytes. The flat cap stays as
    // the floor so tiny windows keep the old behavior.
    // (Measured miss without this: a broad-term 12h histogram projected
    // 1.07 GB of index fetches, tripped the flat 512 MB cap, and pushed ~900
    // files to a ~2.4 GB-per-part scan — 2.6s instead of ~1.2s index-only.)
    let window_compressed_bytes: u64 = file_list_map
        .values()
        .map(|file| file.meta.compressed_size.max(0) as u64)
        .sum();
    let bail_threshold = eval_bail_threshold(bail_bytes_cap, window_compressed_bytes);

    for (group_id, file_group) in index_parquet_files.into_iter().enumerate() {
        if no_more_files {
            // the simple-select limit is already satisfied: drop the rest
            for file in file_group {
                file_list_map.remove(&file.key);
            }
            continue;
        }

        // Spawn a task for each file in the group to filter rows via the index
        let mut tasks = Vec::new();
        let semaphore = Arc::new(Semaphore::new(eval_concurrency));
        for file in file_group {
            let trace_id = query.trace_id.to_string();
            let index_condition_clone = index_condition.clone();
            let idx_optimize_rule_clone = idx_optimize_mode.clone();
            let semaphore_clone = semaphore.clone();
            let fetch_stats = Arc::clone(&fetch_stats);
            let eval_bail = Arc::clone(&eval_bail);
            let task = tokio::task::spawn(async move {
                let permit = semaphore_clone.acquire_owned().await.unwrap();
                if eval_bail.load(std::sync::atomic::Ordering::Relaxed) {
                    // the query tripped the projected-cost bail: this file
                    // goes back nameless -> it stays in the map for the scan
                    // branch, with the filter added back
                    drop(permit);
                    return Ok((
                        String::new(),
                        VixSearchResult::Skipped { percent: 100 },
                        true,
                    ));
                }
                let mut ret = search_vix_index(
                    &trace_id,
                    time_range,
                    index_condition_clone.clone(),
                    idx_optimize_rule_clone.clone(),
                    &file,
                    read_mode,
                    &fetch_stats,
                )
                .await;
                // Retry transient failures once: the parsed reader is
                // memoized, so a post-open failure re-issues only the failed
                // range fetches. Deterministic per-file skips (every AND
                // condition unbuildable for this file) never retry.
                if let Err(e) = &ret
                    && e.downcast_ref::<crate::index::AllConditionsSkipped>()
                        .is_none()
                {
                    log::warn!(
                        "[trace_id {trace_id}] search->vix: retrying file {} after error: {e:?}",
                        file.key,
                    );
                    ret = search_vix_index(
                        &trace_id,
                        time_range,
                        index_condition_clone,
                        idx_optimize_rule_clone,
                        &file,
                        read_mode,
                        &fetch_stats,
                    )
                    .await;
                }
                drop(permit);
                match ret {
                    Ok(ret) => Ok(ret),
                    Err(e) => {
                        log::error!(
                            "[trace_id {trace_id}] search->vix: error filtering via index: {}, index_size: {}, error: {e:?}",
                            file.key,
                            file.meta.index_size,
                        );
                        Err(e)
                    }
                }
            });
            tasks.push(task)
        }

        // if more than cpu_num's file returned many row_ids, we skip the vix search
        let mut threshold_num = cfg.limit.cpu_num;
        let mut total_row_ids_percent = 0;
        let mut tasks = stream::iter(tasks).buffer_unordered(eval_concurrency);
        while let Some(result) = match tasks.try_next().await {
            Err(e) => {
                // A panicked eval task is not a per-file condition: restore
                // the remaining files and fail LOUDLY — both callers either
                // propagate the error (storage search) or fail the aggregate
                // query, never silently continue with an empty file list.
                let took = start.elapsed().as_millis() as usize;
                log::error!(
                    "[trace_id {trace_id}] search->vix: index evaluation task panicked, error: {e:?}, took: {took} ms",
                );
                file_list.extend(file_list_map.into_values());
                return Err(Error::Message(format!(
                    "vix index evaluation task panicked: {e}"
                )));
            }
            Ok(result) => result,
        } {
            // Each result corresponds to a file in the file list
            match result {
                Ok((file_name, result, has_skipped_conditions)) => {
                    // when has_skipped_conditions is true, we should add filter back to datafusion,
                    // because the index result is not accurate
                    if has_skipped_conditions {
                        is_add_filter_back = true;
                    }
                    if file_name.is_empty() {
                        // no need inverted index for this file, need add filter back
                        is_add_filter_back = true;
                        // Skip-threshold give-up: only for plain row-id
                        // searches. Under an optimize mode earlier files may
                        // already be answered AND removed from the map —
                        // giving up would lose their contribution (they would
                        // be neither counted nor scanned).
                        if idx_optimize_mode.is_none() {
                            let took = start.elapsed().as_millis() as usize;
                            threshold_num -= 1;
                            total_row_ids_percent += result.percent();
                            if threshold_num == 0 {
                                log::warn!(
                                    "[trace_id {trace_id}] search->vix: skip vix search, too many row_ids returned from the index, avg percent: {}, took: {took} ms",
                                    total_row_ids_percent as f64 / cfg.limit.cpu_num as f64,
                                );
                                file_list.extend(file_list_map.into_values());
                                return Ok((took, true, MultiResult::RowNums(0)));
                            }
                        }
                        continue;
                    }
                    // A named result came from a real evaluation: fold it
                    // into the projected-cost sample (bailed files return
                    // nameless above and never dilute the average).
                    files_evaluated += 1;
                    if bail_bytes_cap > 0
                        && idx_optimize_mode.is_some()
                        && files_evaluated >= BAIL_SAMPLE_FILES
                        && files_evaluated < files_total
                        && !eval_bail.load(std::sync::atomic::Ordering::Relaxed)
                    {
                        let bytes_so_far =
                            fetch_stats.bytes.load(std::sync::atomic::Ordering::Relaxed);
                        let projected = bytes_so_far / files_evaluated as u64 * files_total as u64;
                        if projected > bail_threshold {
                            eval_bail.store(true, std::sync::atomic::Ordering::Relaxed);
                            is_add_filter_back = true;
                            log::warn!(
                                "[trace_id {trace_id}] search->vix: projected-cost bail-out: {files_evaluated}/{files_total} files fetched {} so far, projected {} > threshold {} (cap {}, window/2 {}) — remaining files go to the scan branch with the filter added back",
                                bytes_to_human_readable(bytes_so_far as f64),
                                bytes_to_human_readable(projected as f64),
                                bytes_to_human_readable(bail_threshold as f64),
                                bytes_to_human_readable(bail_bytes_cap as f64),
                                bytes_to_human_readable((window_compressed_bytes / 2) as f64),
                            );
                        }
                    }
                    // An aggregate fast-path result computed with skipped
                    // conditions answers a WEAKER predicate (it would
                    // overcount); keep the file for the scan branch instead.
                    // Bitmap/candidate selections stay usable: they are a
                    // superset the re-applied filter narrows.
                    if has_skipped_conditions
                        && matches!(
                            result,
                            VixSearchResult::Count(..)
                                | VixSearchResult::Histogram(..)
                                | VixSearchResult::MultiHistogram(..)
                                | VixSearchResult::TopN(..)
                                | VixSearchResult::Distinct(..)
                        )
                    {
                        log::info!(
                            "[trace_id {trace_id}] search->vix: file: {file_name}, aggregate fast path skipped a condition, keep file for the scan branch",
                        );
                        continue;
                    }
                    match result {
                        VixSearchResult::RowIdsSelection {
                            row_ids,
                            row_group_size,
                        } => {
                            let matched = row_ids.matched();
                            result_builder.add_row_nums(matched);
                            let file = file_list_map.get_mut(&file_name).unwrap();
                            file.with_selection(FileSelection::Rows(row_ids), row_group_size);
                            // no condition skipped => the selection IS the
                            // predicate for this file; the scan branch can
                            // skip the re-applied filter for it
                            file.selection_exact = !has_skipped_conditions;
                        }
                        VixSearchResult::SelectCandidates {
                            candidates,
                            row_group_size,
                        } => {
                            result_builder.add_select_candidates(
                                file_name,
                                candidates,
                                row_group_size,
                            );
                        }
                        VixSearchResult::NoMatch => {
                            file_list_map.remove(&file_name);
                        }
                        VixSearchResult::Count(count) => {
                            result_builder.add_count(count as u64);
                            file_list_map.remove(&file_name);
                        }
                        VixSearchResult::Histogram(histogram) => {
                            result_builder.add_histogram(histogram);
                            file_list_map.remove(&file_name);
                        }
                        VixSearchResult::MultiHistogram(multi_histogram) => {
                            result_builder.add_multi_histogram(multi_histogram);
                            file_list_map.remove(&file_name);
                        }
                        VixSearchResult::TopN(top_n) => {
                            result_builder.add_top_n(top_n);
                            file_list_map.remove(&file_name);
                        }
                        VixSearchResult::Distinct(distinct) => {
                            result_builder.add_distinct(distinct);
                            file_list_map.remove(&file_name);
                        }
                        VixSearchResult::Skipped { .. } => {
                            // skipped results always come with an empty file
                            // name and are handled before this match
                            unreachable!("Skipped should not be returned with a file name");
                        }
                    }
                }
                Err(e) => {
                    log::error!(
                        "[trace_id {trace_id}] search->vix: error filtering via index. Keep file to search, error: {e}"
                    );
                    is_add_filter_back = true;
                    continue;
                }
            }
        }
        // only for simple select: stop when the limit is already satisfied
        if result_builder.should_prune_remaining_groups(trace_id, group_id) {
            no_more_files = true;
        }
    }

    // get the merged result; for simple select this finalizes the global top-N
    let multi_result = result_builder.build(trace_id, &mut file_list_map);

    // actually-fetched index bytes for core files (ranged mode; cached mode
    // downloads whole objects through the file cache instead). Repurposes
    // idx_scan_size in THIS function's local ScanStats/log line: for core
    // files the meaningful "index scan size" is the bytes the evaluation
    // really fetched, not a per-file metadata sum.
    let fetch_count = fetch_stats
        .fetches
        .load(std::sync::atomic::Ordering::Relaxed);
    scan_stats.idx_scan_size = fetch_stats.bytes.load(std::sync::atomic::Ordering::Relaxed) as i64;

    log::info!(
        "{}",
        search_inspector_fields(
            format!(
                "[trace_id {trace_id}] search->vix: total hits for index_condition: {index_condition:?} found {multi_result}, is_add_filter_back: {is_add_filter_back}, file_num: {}, index fetches: {fetch_count} ({}), took: {} ms",
                file_list_map.len(),
                bytes_to_human_readable(scan_stats.idx_scan_size as f64),
                search_start.elapsed().as_millis()
            ),
            SearchInspectorFieldsBuilder::new()
                .trace_id(query.trace_id.to_string())
                .node_name(LOCAL_NODE.name.clone())
                .component("vix search".to_string())
                .search_role("follower".to_string())
                .duration(search_start.elapsed().as_millis() as usize)
                .desc(format!(
                    "found {multi_result}, is_add_filter_back: {is_add_filter_back}, file_num: {}, index fetches: {fetch_count} ({} bytes)",
                    file_list_map.len(),
                    scan_stats.idx_scan_size,
                ))
                .build()
        )
    );

    file_list.extend(file_list_map.into_values());
    Ok((
        start.elapsed().as_millis() as usize,
        is_add_filter_back,
        multi_result,
    ))
}

/// Raw output of the blocking index evaluation for one file.
enum RawVixResult {
    /// a queried field has skipped (oversize) terms: the index may miss
    /// documents, the whole file must be scanned
    PartialFields,
    /// a docs column the optimize mode reads is missing in this file (it
    /// predates the `column_store_fields` setting): scan fallback
    MissingColumn { field: String },
    /// simple count fast path
    Count { count: u64, has_skipped: bool },
    /// per-row match bitmap (length == index row_count)
    Bitmap {
        bitmap: BooleanBuffer,
        row_count: u64,
        row_group_size: Option<u32>,
        has_skipped: bool,
    },
    /// simple select fast path: top-`limit` matches as `(_timestamp, doc_id)`
    SelectCandidates {
        candidates: Vec<(i64, u32)>,
        row_count: u64,
        row_group_size: Option<u32>,
    },
    /// simple histogram fast path
    Histogram {
        histogram: Vec<u64>,
        has_skipped: bool,
    },
    /// simple multi-histogram fast path
    MultiHistogram {
        rows: Vec<(i64, String, u64)>,
        has_skipped: bool,
    },
    /// simple top-n fast path
    TopN {
        groups: Vec<(Vec<String>, u64)>,
        has_skipped: bool,
    },
    /// simple distinct fast path
    Distinct {
        values: HashSet<String>,
        has_skipped: bool,
    },
}

/// The three ways one `.vix` object reaches the blocking evaluation.
enum VixReaderInput {
    /// Complete object bytes (cached mode, plus ranged-mode fallbacks).
    Bytes(bytes::Bytes),
    /// Open over a range source, memoizing the parsed reader under
    /// `cache_key`.
    Ranged {
        source: Arc<dyn vortex_index::VixRangeSource>,
        cache_key: String,
    },
    /// A previously parsed (memoized) reader — zero IO to open.
    Shared(Arc<VixReader>),
}

impl VixReaderInput {
    /// Open (or reuse) the reader. Blocking work: for ranged inputs, the
    /// footer tail and dictionary-directory fetches (row-group FSTs load
    /// lazily during evaluation).
    fn open(self) -> anyhow::Result<Arc<VixReader>> {
        Ok(match self {
            VixReaderInput::Bytes(bytes) => Arc::new(VixReader::open(bytes)?),
            VixReaderInput::Ranged { source, cache_key } => {
                let reader = Arc::new(VixReader::open_ranged(source)?);
                reader_cache::GLOBAL_CACHE.put(cache_key, Arc::clone(&reader));
                reader
            }
            VixReaderInput::Shared(reader) => reader,
        })
    }
}

/// Returns (file_key, result, has_skipped_conditions).
/// when has_skipped_conditions is true, we should add filter back to datafusion.
async fn search_vix_index(
    trace_id: &str,
    time_range: (i64, i64),
    index_condition: Option<IndexCondition>,
    idx_optimize_rule: Option<IndexOptimizeMode>,
    parquet_file: &FileKey,
    read_mode: VixReadMode,
    fetch_stats: &Arc<source::FetchStats>,
) -> anyhow::Result<(String, VixSearchResult, bool)> {
    // test-only rendezvous proving the per-file fan-out really runs
    // concurrently (see `test_eval_fan_out_runs_files_concurrently`)
    #[cfg(test)]
    if let Some(barrier) = tests::eval_concurrency_barrier(trace_id) {
        barrier.wait().await;
    }
    let file_account = parquet_file.account.clone();
    // A core .vix data file IS its own index. The caller only passes core
    // files; guard against anything else (legacy files are index-less).
    if !is_core_file(&parquet_file.key) {
        return Err(anyhow::anyhow!(
            "[trace_id {trace_id}] search->vix: {} is not a core .vix file (legacy data files \
             carry no readable index)",
            parquet_file.key
        ));
    }
    let vix_file_name = parquet_file.key.clone();

    let condition: IndexCondition =
        index_condition.ok_or(anyhow::anyhow!("IndexCondition not found"))?;

    // when the file is not fully within the time range, add a timestamp filter
    let (start_time, end_time) = time_range;
    let file_in_range =
        parquet_file.meta.min_ts >= start_time && parquet_file.meta.max_ts < end_time;
    // Stats-served count: an unfiltered COUNT over a file FULLY inside the
    // range needs no bytes from the file — file_list's `records` (the
    // writer-stamped row count already in hand) IS the answer. This is how
    // o2 answers the class in sub-second cold; without it a 3h/775M-span
    // window read ~5.6K files' zone tables for 33s. Straddling files keep
    // the open+zone path below, and any real condition still evaluates.
    if file_in_range
        && parquet_file.meta.records > 0
        && matches!(idx_optimize_rule, Some(IndexOptimizeMode::SimpleCount))
        && condition.is_condition_all()
    {
        return Ok((
            parquet_file.key.to_string(),
            VixSearchResult::Count(parquet_file.meta.records as usize),
            false,
        ));
    }
    let cfg = get_config();
    let mut cache_key = String::new();
    if cfg.common.inverted_index_result_cache_enabled {
        metrics::VIX_RESULT_CACHE_REQUESTS_TOTAL
            .with_label_values::<&str>(&[])
            .inc();
        // A fully-covered file's result is time-independent: its key carries
        // no range and hits across shifted windows. A straddling file's
        // result depends on the applied timestamp filter, so its key pins
        // the effective clamp — deep-merged multi-hour files at the window
        // boundary are the most expensive evaluations we memoize.
        let time_clamp = if file_in_range {
            None
        } else {
            Some((
                start_time.max(parquet_file.meta.min_ts),
                end_time.min(parquet_file.meta.max_ts + 1),
            ))
        };
        cache_key = generate_cache_key(&condition, &idx_optimize_rule, parquet_file, time_clamp);
        if let Some(result) =
            vix_result_cache::GLOBAL_CACHE.get(&cache_key, idx_optimize_rule.as_ref())
        {
            metrics::VIX_RESULT_CACHE_HITS_TOTAL
                .with_label_values::<&str>(&[])
                .inc();
            return Ok((parquet_file.key.to_string(), result, false));
        }
    }

    // Resolve how the object reaches the reader. Ranged mode opens it over
    // the range-capable cache ladder (memoizing the parsed reader for hot
    // queries); it degrades to the whole-file download when the object size
    // is unknown. Cached mode always downloads the whole object through the
    // cache ladder (memory -> disk -> object storage).
    let reader_input = match read_mode {
        VixReadMode::Ranged => {
            if let Some(reader) = reader_cache::GLOBAL_CACHE.get(&vix_file_name) {
                Some(VixReaderInput::Shared(reader))
            } else {
                // the core file IS the index object: whole-object size
                u64::try_from(parquet_file.meta.compressed_size)
                    .ok()
                    .filter(|size| *size > 0)
                    .zip(tokio::runtime::Handle::try_current().ok())
                    .map(|(size, handle)| VixReaderInput::Ranged {
                        source: Arc::new(LadderRangeSource::new(
                            file_account.clone(),
                            &vix_file_name,
                            size,
                            handle,
                            Some(Arc::clone(fetch_stats)),
                        )),
                        cache_key: vix_file_name.clone(),
                    })
            }
        }
        VixReadMode::Cached => None,
    };
    let reader_input = match reader_input {
        Some(input) => input,
        None => {
            log::debug!("[trace_id {trace_id}] search->vix: load index file: {vix_file_name}");
            let bytes = file_data::get(&file_account, &vix_file_name, None)
                .await
                .map_err(|e| {
                    anyhow::anyhow!("failed to load vix index file {vix_file_name}: {e}")
                })?;
            VixReaderInput::Bytes(bytes)
        }
    };

    // open + evaluate is CPU-bound (in ranged mode it also blocks on range
    // fetches): run it off the runtime
    let task_trace_id = trace_id.to_string();
    // the rule moves into the eval task; keep a copy for the result-cache put
    let idx_optimize_rule_for_cache = idx_optimize_rule.clone();
    // A straddling file's final result depends on the window clamp, so its
    // result-cache key shifts with every dashboard refresh and the file's
    // dense postings re-decode each time. The CONDITION bitmap is
    // time-independent: cache it under the clamp-free no-rule key (the same
    // key a covered-file no-rule query stores its bitmap under — identical
    // semantics) and let the eval AND the cheap timestamp clamp per query.
    let bitmap_cache_key = (cfg.common.inverted_index_result_cache_enabled && !file_in_range)
        .then(|| generate_cache_key(&condition, &None, parquet_file, None));
    let raw = tokio::task::spawn_blocking(move || -> anyhow::Result<RawVixResult> {
        let reader = reader_input.open()?;
        evaluate_vix_index(
            &task_trace_id,
            &reader,
            &condition,
            idx_optimize_rule,
            (start_time, end_time),
            file_in_range,
            bitmap_cache_key,
        )
    })
    .await??;

    let key = parquet_file.key.to_string();
    let (result, has_skipped) = match raw {
        RawVixResult::PartialFields => {
            log::info!(
                "[trace_id {trace_id}] search->vix: file: {}, query touches partial-indexed fields, back to datafusion",
                parquet_file.key
            );
            // the whole file must be scanned: 100% of its rows stay candidates
            return Ok((
                String::new(),
                VixSearchResult::Skipped { percent: 100 },
                true,
            ));
        }
        RawVixResult::MissingColumn { field } => {
            log::info!(
                "[trace_id {trace_id}] search->vix: file: {}, docs blob lacks column {field:?} needed by the optimize mode, back to datafusion",
                parquet_file.key
            );
            return Ok((
                String::new(),
                VixSearchResult::Skipped { percent: 100 },
                true,
            ));
        }
        RawVixResult::Count { count, has_skipped } => {
            (VixSearchResult::Count(count as usize), has_skipped)
        }
        RawVixResult::Bitmap {
            bitmap,
            row_count,
            row_group_size,
            has_skipped,
        } => {
            let matched = bitmap.count_set_bits();
            match guard_matched_rows(trace_id, parquet_file, matched, row_count)? {
                Some(result) => {
                    // NoMatch keeps its file key; Skipped goes back nameless
                    return match result {
                        VixSearchResult::NoMatch => {
                            // zero matches is deterministic per (condition,
                            // file) — memoize it like any other exact result
                            if !cache_key.is_empty() {
                                vix_result_cache::GLOBAL_CACHE.put(cache_key, CacheEntry::NoMatch);
                            }
                            Ok((key, result, false))
                        }
                        _ => Ok((String::new(), result, true)),
                    };
                }
                None => (
                    // the guard passed => the match set is under the skip
                    // threshold; compress it once here so everything resident
                    // downstream (result cache, FileKey selection, per-query
                    // scan registry) holds the sparse form
                    VixSearchResult::RowIdsSelection {
                        row_ids: Arc::new(RowIdBitmap::from_dense(&bitmap)),
                        row_group_size,
                    },
                    has_skipped,
                ),
            }
        }
        RawVixResult::SelectCandidates {
            candidates,
            row_count,
            row_group_size,
        } => {
            if candidates.is_empty() || parquet_file.meta.records == 0 {
                // zero matches is deterministic per (condition, file)
                if !cache_key.is_empty() {
                    vix_result_cache::GLOBAL_CACHE.put(cache_key, CacheEntry::NoMatch);
                }
                return Ok((key, VixSearchResult::NoMatch, false));
            }
            // NO percent guard here (#27): candidates are bounded by the
            // query limit and merged into the exact global top-N — the
            // guard was pushing a small file's WHOLE row set to the scan
            // branch in place of its <= limit winning rows.
            if row_count != parquet_file.meta.records as u64 {
                return Err(anyhow::anyhow!(
                    "vix index row_count {row_count} does not match file records {}",
                    parquet_file.meta.records,
                ));
            }
            // candidates are exact by construction (no skipped conditions)
            (
                VixSearchResult::SelectCandidates {
                    candidates: Arc::new(candidates),
                    row_group_size,
                },
                false,
            )
        }
        RawVixResult::Histogram {
            histogram,
            has_skipped,
        } => (VixSearchResult::Histogram(histogram), has_skipped),
        RawVixResult::MultiHistogram { rows, has_skipped } => {
            (VixSearchResult::MultiHistogram(rows), has_skipped)
        }
        RawVixResult::TopN {
            groups,
            has_skipped,
        } => (VixSearchResult::TopN(groups), has_skipped),
        RawVixResult::Distinct {
            values,
            has_skipped,
        } => (VixSearchResult::Distinct(values), has_skipped),
    };

    if !cache_key.is_empty()
        && !has_skipped
        && result.get_memory_size() < cfg.limit.inverted_index_result_cache_max_entry_size
        && let Some(entry) = get_cache_entry(result.clone(), idx_optimize_rule_for_cache.as_ref())
    {
        vix_result_cache::GLOBAL_CACHE.put(cache_key, entry);
    }
    Ok((key, result, has_skipped))
}

/// Evaluate one opened `.vix` index against the condition (and optional
/// optimize mode). Pure and synchronous over the reader — the reader decides
/// whether reads are in-memory or ranged, which is exactly what the
/// cached/ranged parity tests pivot on.
fn evaluate_vix_index(
    trace_id: &str,
    reader: &VixReader,
    condition: &IndexCondition,
    idx_optimize_rule: Option<IndexOptimizeMode>,
    time_range: (i64, i64),
    file_in_range: bool,
    bitmap_cache_key: Option<String>,
) -> anyhow::Result<RawVixResult> {
    let (start_time, end_time) = time_range;

    // a condition over a field with skipped oversize values may miss
    // documents: using its bitmap would wrongly drop matching rows
    if condition.uses_partial_fields(reader.partial_fields(), reader.fts_fields()) {
        return Ok(RawVixResult::PartialFields);
    }

    // build the per-file query. Fields the file carries without raw-value
    // term capability skip their condition (has_skipped => add filter
    // back); fields NO document carries evaluate to the exact empty result
    // (the file is eliminated, not scanned). match_all values tokenize with
    // the canonical index tokenizer (the writer's).
    let (query, has_skipped) = condition.to_vix_query(
        trace_id,
        &|field| field_capability(trace_id, reader, field),
        &index_match_all_tokens,
    )?;

    // pilot fix B: an unfiltered (condition-all) full-range single-field
    // TopN/Distinct is served index-only where possible — term-dictionary
    // value counts first, docs column second, `_source` extraction last —
    // so such a file never needs docs columns (and is never skipped).
    // Multi-field TopN stays on the docs-column path.
    let single_group_field = match idx_optimize_rule.as_ref() {
        Some(IndexOptimizeMode::SimpleTopN(fields, ..)) if fields.len() == 1 => {
            Some(fields[0].clone())
        }
        Some(IndexOptimizeMode::SimpleDistinct(field, ..)) => Some(field.clone()),
        _ => None,
    };
    let index_only_field = single_group_field
        .clone()
        .filter(|_| condition.is_condition_all() && file_in_range);

    // every non-`_timestamp` docs column a fast path reads must exist in
    // this file; files predating the setting fall back to a scan — EXCEPT a
    // single-field TopN/Distinct group field, which the arms below can serve
    // from the term dictionary (filtered via postings when a condition
    // applies), so its absence as a docs column is not disqualifying
    if let Some(rule) = idx_optimize_rule.as_ref()
        && index_only_field.is_none()
        && let Some(field) = collect::missing_docs_column(reader, rule)?
        && single_group_field.as_deref() != Some(field.as_str())
    {
        return Ok(RawVixResult::MissingColumn { field });
    }

    let row_group_size = u32::try_from(reader.row_group_size())
        .ok()
        .filter(|v| *v > 0);

    // the per-row match bitmap; every mode but the pure count fast path
    // needs it. AND with `_timestamp in [start_time, end_time)` when the
    // file is only partially inside the query range (inclusive start,
    // exclusive end — the query layer's time-range convention).
    //
    // The pre-clamp condition bitmap is time-independent; for straddling
    // files it is memoized under `bitmap_cache_key` so a sliding window
    // re-clamps a cached bitmap instead of re-decoding dense postings. The
    // skipped-condition flag stays out of the entry: `has_skipped` derives
    // from the condition alone, so every hit under the same key shares it.
    let eval_bitmap = |reader: &VixReader| -> anyhow::Result<BooleanBuffer> {
        let cached: Option<BooleanBuffer> = bitmap_cache_key.as_deref().and_then(|key| {
            match vix_result_cache::GLOBAL_CACHE.get(key, None) {
                Some(VixSearchResult::RowIdsSelection { row_ids, .. })
                    if row_ids.num_rows() == reader.row_count() as usize =>
                {
                    // materialize the dense form for the eval pipeline; for
                    // the sparse sets the cache holds this is cheaper than
                    // the full-buffer deep clone it replaced
                    Some(row_ids.to_dense())
                }
                Some(VixSearchResult::NoMatch) => {
                    Some(BooleanBuffer::new_unset(reader.row_count() as usize))
                }
                _ => None,
            }
        });
        let mut bitmap = match cached {
            Some(bitmap) => bitmap,
            None => {
                let bitmap = reader.eval(&query)?;
                if let Some(key) = bitmap_cache_key.as_deref() {
                    let entry = CacheEntry::RowIds(
                        Arc::new(RowIdBitmap::from_dense(&bitmap)),
                        row_group_size,
                    );
                    if entry.get_memory_size()
                        < get_config()
                            .limit
                            .inverted_index_result_cache_max_entry_size
                    {
                        vix_result_cache::GLOBAL_CACHE.put(key.to_string(), entry);
                    }
                }
                bitmap
            }
        };
        if !file_in_range {
            bitmap = &bitmap & &reader.timestamp_range(start_time, end_time)?;
        }
        Ok(bitmap)
    };

    match idx_optimize_rule {
        Some(IndexOptimizeMode::SimpleCount) => {
            let count = if file_in_range {
                reader.count(&query)?
            } else if !has_skipped
                && reader.zone_chunks().is_some()
                && let Some(cursor) = reader.single_term_plist_cursor(&query)?
            {
                // dense out-of-row term on a window-straddling file: rank
                // diffs per zone chunk instead of decoding the whole list
                // into a bitmap (stage 4 of the plist design)
                collect::ranked_count_in_window(reader, &cursor, start_time, end_time)?
            } else {
                eval_bitmap(reader)?.count_set_bits() as u64
            };
            Ok(RawVixResult::Count { count, has_skipped })
        }
        Some(IndexOptimizeMode::SimpleSelect(limit, ascend)) if !has_skipped => {
            // exact candidates only when no condition was skipped: every
            // candidate row must survive a re-applied filter
            let bitmap = eval_bitmap(reader)?;
            let candidates = collect::simple_select(reader, &bitmap, limit, ascend)?;
            Ok(RawVixResult::SelectCandidates {
                candidates,
                row_count: reader.row_count(),
                row_group_size,
            })
        }
        Some(IndexOptimizeMode::SimpleHistogram(
            min_value,
            bucket_width,
            num_buckets,
            ts_offset,
        )) => {
            // dense out-of-row term with a zone table: per-chunk rank diffs
            // fold straight into buckets, only bucket-straddling chunks
            // decode (stage 4 of the plist design). The grid IS the query
            // window, so out-of-window rows drop identically to the
            // time-clamped bitmap path.
            if !has_skipped
                && reader.zone_chunks().is_some()
                && let Some(cursor) = reader.single_term_plist_cursor(&query)?
            {
                let histogram = collect::ranked_simple_histogram(
                    reader,
                    &cursor,
                    min_value,
                    bucket_width,
                    num_buckets,
                    ts_offset,
                    (start_time, end_time),
                )?;
                return Ok(RawVixResult::Histogram {
                    histogram,
                    has_skipped,
                });
            }
            let bitmap = eval_bitmap(reader)?;
            let histogram = collect::simple_histogram(
                reader,
                &bitmap,
                min_value,
                bucket_width,
                num_buckets,
                ts_offset,
            )?;
            Ok(RawVixResult::Histogram {
                histogram,
                has_skipped,
            })
        }
        Some(IndexOptimizeMode::SimpleMultiHistogram(
            min_value,
            max_value,
            bucket_width,
            ts_offset,
            breakdown_field,
        )) => {
            let bitmap = eval_bitmap(reader)?;
            let rows = collect::simple_multi_histogram(
                reader,
                &bitmap,
                min_value,
                max_value,
                bucket_width,
                ts_offset,
                &breakdown_field,
            )?;
            Ok(RawVixResult::MultiHistogram { rows, has_skipped })
        }
        Some(IndexOptimizeMode::SimpleTopN(fields, limit, ascend)) => {
            if let Some(field) = &index_only_field {
                // Prefer the docs column when materialized: its cost tracks the
                // file's row count, while the dictionary walk scans the whole
                // FST (every field's tokens) to filter one field-id suffix.
                if !collect::docs_column_available(reader, field)? {
                    if let Some(groups) = collect::unfiltered_top_n(reader, field, limit, ascend)? {
                        return Ok(RawVixResult::TopN {
                            groups,
                            has_skipped,
                        });
                    }
                    // core files always carry `_source`: total, never skipped.
                    // This is the expensive last resort (full `_source` JSON
                    // re-parse of every row) — #29 lever 3 makes it visible
                    log::info!(
                        "[trace_id {trace_id}] search->vix: unfiltered topn on {field:?} \
                         fell through to the _source re-parse (dict and docs column \
                         both unavailable)",
                    );
                    let groups = collect::source_top_n(reader, field, limit, ascend)?;
                    return Ok(RawVixResult::TopN {
                        groups,
                        has_skipped,
                    });
                }
                // the docs column serves it below
            }
            let bitmap = eval_bitmap(reader)?;
            // conditioned single-field TopN on a file without the group
            // field's docs column: serve from the term dictionary — per
            // value, count its postings inside the condition bitmap (the
            // pre-column_store_fields history's fast path)
            if fields.len() == 1 && !collect::docs_column_available(reader, &fields[0])? {
                if let Some(groups) =
                    collect::filtered_top_n(reader, &bitmap, &fields[0], limit, ascend)?
                {
                    return Ok(RawVixResult::TopN {
                        groups,
                        has_skipped,
                    });
                }
                // dictionary cannot prove exact per-value counts here and
                // the docs column is absent: the scan path owns this file
                return Ok(RawVixResult::MissingColumn {
                    field: fields[0].clone(),
                });
            }
            let groups = collect::simple_top_n(reader, &bitmap, &fields, limit, ascend)?;
            Ok(RawVixResult::TopN {
                groups,
                has_skipped,
            })
        }
        Some(IndexOptimizeMode::SimpleDistinct(field, limit, ascend)) => {
            if let Some(field) = &index_only_field {
                // Same preference order as SimpleTopN: docs column first.
                if !collect::docs_column_available(reader, field)? {
                    if let Some(values) =
                        collect::unfiltered_distinct(reader, field, limit, ascend)?
                    {
                        return Ok(RawVixResult::Distinct {
                            values,
                            has_skipped,
                        });
                    }
                    log::info!(
                        "[trace_id {trace_id}] search->vix: unfiltered distinct on {field:?} \
                         fell through to the _source re-parse (dict and docs column \
                         both unavailable)",
                    );
                    let values = collect::source_distinct(reader, field, limit, ascend)?;
                    return Ok(RawVixResult::Distinct {
                        values,
                        has_skipped,
                    });
                }
            }
            let bitmap = eval_bitmap(reader)?;
            // same dictionary serve as the TopN arm for column-less files
            if !collect::docs_column_available(reader, &field)? {
                if let Some(values) =
                    collect::filtered_distinct(reader, &bitmap, &field, limit, ascend)?
                {
                    return Ok(RawVixResult::Distinct {
                        values,
                        has_skipped,
                    });
                }
                return Ok(RawVixResult::MissingColumn {
                    field: field.clone(),
                });
            }
            let values = collect::simple_distinct(reader, &bitmap, &field, limit, ascend)?;
            Ok(RawVixResult::Distinct {
                values,
                has_skipped,
            })
        }
        // plain row-id search (also the SimpleSelect fallback when a
        // condition was skipped)
        _ => {
            let bitmap = eval_bitmap(reader)?;
            Ok(RawVixResult::Bitmap {
                bitmap,
                row_count: reader.row_count(),
                row_group_size,
                has_skipped,
            })
        }
    }
}

/// Per-file capability of a named field for term-index lookups (the closure
/// [`IndexCondition::to_vix_query`] classifies conditions with):
///
/// - [`FieldCap::Term`] — the field's raw whole values are term-indexed in this file: conditions on
///   it map to index queries directly.
/// - [`FieldCap::Absent`] — the key-term dictionary probe proves NO document carries the field
///   (`VixReader::key_term_exists` is false ⇒ NULL in every row): never-TRUE-on-NULL conditions
///   become [`vortex_index::VixQuery::Nothing`], eliminating the file exactly instead of scanning
///   it.
/// - [`FieldCap::FtsOnly`] — everything else: the file carries the field but the term index cannot
///   serve exact-value predicates on it (fts tokens only, column-store/numeric storage, internal
///   columns), or the probe itself failed. The condition is skipped and the DataFusion filter
///   re-applied — never a silent miss.
fn field_capability(trace_id: &str, reader: &VixReader, field: &str) -> FieldCap {
    if reader.has_term_capability(field) {
        return FieldCap::Term;
    }
    match reader.key_term_exists(field) {
        Ok(false) => FieldCap::Absent,
        Ok(true) => FieldCap::FtsOnly,
        Err(e) => {
            log::warn!(
                "[trace_id {trace_id}] search->vix: key-term probe failed for field {field:?}: {e}; keeping the scan fallback"
            );
            FieldCap::FtsOnly
        }
    }
}

/// The match_all tokenizer for index queries: the SINGLE canonical tokenizer
/// (`vortex_index::o2_tokenize`, the same function the writer indexes
/// with), applied unconditionally — files stamped with the legacy tokenizer
/// property re-tokenize at their next compaction (merge-mismatch rebuild);
/// ASCII behavior is identical across versions and non-ASCII on legacy files
/// was already broken before this fix. The config-side
/// `o2_collect_search_tokens` stays only for the DataFusion LIKE fallback
/// and SQL validation.
fn index_match_all_tokens(value: &str) -> Vec<String> {
    let cfg = get_config();
    vortex_index::o2_tokenize(
        value,
        cfg.limit.inverted_index_min_token_length,
        cfg.limit.inverted_index_max_token_length,
    )
    .collect()
}

/// Common guards for a matched-row bitmap: returns `Some(NoMatch)` when there
/// is nothing to select, `Some(Skipped)` when the match count exceeds the
/// skip threshold (the caller falls back to datafusion), and an error when
/// the bitmap does not line up with the parquet row count.
fn guard_matched_rows(
    trace_id: &str,
    parquet_file: &FileKey,
    matched: usize,
    row_count: u64,
) -> anyhow::Result<Option<VixSearchResult>> {
    if matched == 0 || parquet_file.meta.records == 0 {
        return Ok(Some(VixSearchResult::NoMatch));
    }
    let skip_threshold = get_config().limit.inverted_index_skip_threshold;
    let row_ids_percent = matched as f64 / parquet_file.meta.records as f64 * 100.0;
    if skip_threshold > 0 && row_ids_percent > skip_threshold as f64 {
        log::info!(
            "[trace_id {trace_id}] search->vix: file: {}, result percent {row_ids_percent}% is too large, back to datafusion",
            parquet_file.key
        );
        return Ok(Some(VixSearchResult::Skipped {
            percent: row_ids_percent as usize,
        }));
    }
    // out-of-range guard: the bitmap length (index row_count) must match the
    // data file row count, otherwise row selection would be misaligned
    if row_count != parquet_file.meta.records as u64 {
        return Err(anyhow::anyhow!(
            "vix index row_count {row_count} does not match file records {}",
            parquet_file.meta.records,
        ));
    }
    Ok(None)
}

/// Build the cache entry for an evaluated result. `None` means the result
/// cannot be represented as a reusable entry (a histogram evaluated without
/// its `SimpleHistogram` rule — never produced in practice) and is simply
/// not cached.
fn get_cache_entry(
    result: VixSearchResult,
    rule: Option<&IndexOptimizeMode>,
) -> Option<CacheEntry> {
    Some(match result {
        VixSearchResult::RowIdsSelection {
            row_ids,
            row_group_size,
        } => CacheEntry::RowIds(row_ids, row_group_size),
        VixSearchResult::SelectCandidates {
            candidates,
            row_group_size,
        } => CacheEntry::SelectCandidates(candidates, row_group_size),
        VixSearchResult::Count(count) => CacheEntry::Count(count),
        VixSearchResult::Histogram(histogram) => {
            // Store on the entry's own absolute grid (trimmed to the file's
            // occupied buckets) so any later query sharing width+phase can
            // reposition it — see [`CacheEntry::Histogram`].
            let Some(IndexOptimizeMode::SimpleHistogram(min_value, bucket_width, _, ts_offset)) =
                rule
            else {
                return None;
            };
            let width = (*bucket_width).max(1) as i64;
            let grid_origin = min_value - ts_offset;
            let first = histogram.iter().position(|c| *c > 0).unwrap_or(0);
            let last = histogram
                .iter()
                .rposition(|c| *c > 0)
                .map_or(first, |i| i + 1);
            CacheEntry::Histogram {
                origin: grid_origin + first as i64 * width,
                width,
                counts: histogram[first..last].to_vec(),
            }
        }
        VixSearchResult::MultiHistogram(multi_histogram) => {
            CacheEntry::MultiHistogram(multi_histogram)
        }
        VixSearchResult::TopN(top_n) => CacheEntry::TopN(top_n),
        VixSearchResult::Distinct(distinct) => CacheEntry::Distinct(distinct),
        VixSearchResult::NoMatch => CacheEntry::NoMatch,
        VixSearchResult::Skipped { .. } => {
            unreachable!("unsupported vix search result in search_vix_index")
        }
    })
}

/// The per-file result-cache key. Keyed on the STRUCTURAL hash of the
/// condition (`IndexCondition` derives `Hash`), never on its display string:
/// display strings join conditions with unescaped separators, so a crafted
/// VALUE containing " AND " could collide with a different query and serve
/// its cached result. Plain row-selection (no optimize rule) caches under
/// the reserved rule tag "n" — its bitmap is a pure function of
/// (condition, file), same as the optimize-mode results.
///
/// `time_clamp`: `None` for a file fully covered by the query range (the
/// result is time-independent, so the key reuses across shifted windows);
/// `Some((start, end))` — the range INTERSECTED with the file's own span —
/// for straddling files, whose result depends on the applied timestamp
/// filter. Clamping to the intersection maximizes reuse: any window with
/// the same effective overlap shares the key.
fn generate_cache_key(
    index_condition: &IndexCondition,
    idx_optimize_rule: &Option<IndexOptimizeMode>,
    parquet_file: &FileKey,
    time_clamp: Option<(i64, i64)>,
) -> String {
    use std::hash::{Hash, Hasher};

    let rule = match idx_optimize_rule {
        // SimpleHistogram keys carry bucket width + PHASE, not the absolute
        // window: entries store their own grid and reposition on read, so a
        // sliding dashboard window (same width, same alignment) keeps
        // hitting per-file entries instead of going cold every refresh.
        // num_buckets stays out of the key for the same reason.
        Some(IndexOptimizeMode::SimpleHistogram(min_value, bucket_width, _, ts_offset)) => {
            let width = (*bucket_width).max(1) as i64;
            format!(
                "h(p:{},b:{width})",
                (min_value - ts_offset).rem_euclid(width)
            )
        }
        Some(rule) => rule.to_rule_string(),
        None => "n".to_string(),
    };
    let mut hasher = std::hash::DefaultHasher::new();
    index_condition.hash(&mut hasher);
    let clamp = match time_clamp {
        Some((start, end)) => format!("{start}-{end}"),
        None => "full".to_string(),
    };
    format!(
        "{:016x}_{rule}_{clamp}_{}",
        hasher.finish(),
        parquet_file.key
    )
}

/// Whether a stream data file is a core file (the object itself is the
/// `.vix` container holding records + index).
fn is_core_file(key: &str) -> bool {
    config::FileFormat::from_extension(key) == Some(config::FileFormat::Vix)
}

/// The projected-cost bail threshold. The flat cap (`ZO_VIX_EVAL_BAIL_BYTES`)
/// floors it; the scan alternative — approximated as HALF the window's
/// compressed bytes (the scan reads the needed data columns of every
/// remaining file, then re-applies the condition) — raises it: bailing to a
/// scan that reads more than the index projection would is never a win.
/// The `bail_bytes_cap > 0` gate upstream keeps bail fully disabled at 0.
fn eval_bail_threshold(cap: u64, window_compressed_bytes: u64) -> u64 {
    cap.max(window_compressed_bytes / 2)
}

#[cfg(test)]
mod tests {
    /// Bailing must compare the index projection against the SCAN
    /// alternative, not only a flat cap: a 12h broad-term histogram
    /// projecting ~1 GB of index fetches must NOT bail when the scan branch
    /// would read ~6 GB of data columns instead (measured regression: 2.6s
    /// scan vs ~1.2s index-only). Small windows keep the flat-cap behavior.
    #[test]
    fn test_eval_bail_threshold_scales_with_the_scan_alternative() {
        const MB: u64 = 1024 * 1024;
        // small window: the flat cap floors the threshold
        assert_eq!(eval_bail_threshold(512 * MB, 100 * MB), 512 * MB);
        // the prod shape: 12 GB window / 2 = 6 GB threshold — a 1 GB
        // projection stays on the index path
        let threshold = eval_bail_threshold(512 * MB, 12 * 1024 * MB);
        assert_eq!(threshold, 6 * 1024 * MB);
        assert!(1024 * MB < threshold, "1GB projection must not bail");
        // a genuinely pathological projection still bails
        assert!(8 * 1024 * MB > threshold);
    }

    use config::meta::stream::FileMeta;

    use super::*;
    use crate::index::{Condition, IndexCondition};

    fn create_file_key(min_ts: i64, max_ts: i64) -> FileKey {
        FileKey {
            key: format!("file_{min_ts}_{max_ts}"),
            meta: FileMeta {
                min_ts,
                max_ts,
                records: 1000,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn equal_condition() -> IndexCondition {
        let mut index_condition = IndexCondition::new();
        index_condition.add_condition(Condition::Equal("field1".to_string(), "value1".to_string()));
        index_condition
    }

    #[test]
    fn test_generate_cache_key_none_rule() {
        // bitmap searches (no optimize rule) cache under the reserved "n" tag
        let result = generate_cache_key(&equal_condition(), &None, &create_file_key(1, 10), None);
        assert!(!result.is_empty());
        assert!(result.contains("_n_"));
        assert!(result.contains("file_1_10"));
    }

    #[test]
    fn test_generate_cache_key_valid() {
        let idx_optimize_rule = Some(config::meta::inverted_index::IndexOptimizeMode::SimpleCount);
        let result = generate_cache_key(
            &equal_condition(),
            &idx_optimize_rule,
            &create_file_key(1, 10),
            None,
        );
        assert!(!result.is_empty());
        assert!(result.contains("file_1_10"));
    }

    /// Rule-independent results build their entry without a rule.
    fn get_cache_entry_none(result: VixSearchResult) -> CacheEntry {
        get_cache_entry(result, None).unwrap()
    }

    /// SimpleHistogram cache keys carry width + phase, not the absolute
    /// window: a slid dashboard window (same width, same alignment) shares
    /// keys; a phase or width change does not.
    #[test]
    fn test_histogram_cache_key_is_phase_aligned() {
        let file = create_file_key(1, 10);
        let condition = equal_condition();
        let key = |rule| generate_cache_key(&condition, &Some(rule), &file, None);

        // 1000 % 60 == 1600 % 60: the window slid by 10 buckets, key holds
        assert_eq!(
            key(IndexOptimizeMode::SimpleHistogram(1000, 60, 10, 0)),
            key(IndexOptimizeMode::SimpleHistogram(1600, 60, 99, 0)),
        );
        // ts_offset folds into the phase: m=1000/o=0 ≡ m=1060/o=60
        assert_eq!(
            key(IndexOptimizeMode::SimpleHistogram(1000, 60, 10, 0)),
            key(IndexOptimizeMode::SimpleHistogram(1060, 60, 10, 60)),
        );
        // different phase or width: different key
        assert_ne!(
            key(IndexOptimizeMode::SimpleHistogram(1000, 60, 10, 0)),
            key(IndexOptimizeMode::SimpleHistogram(1001, 60, 10, 0)),
        );
        assert_ne!(
            key(IndexOptimizeMode::SimpleHistogram(1000, 60, 10, 0)),
            key(IndexOptimizeMode::SimpleHistogram(1000, 120, 10, 0)),
        );
    }

    /// Entries trim to the occupied buckets and reposition into any
    /// same-width/same-phase query grid via the cache get path.
    #[test]
    fn test_histogram_cache_entry_repositions_across_windows() {
        let cache = vix_result_cache::VixResultCache::new(10);
        // evaluated under a grid starting at 1000: counts land in buckets
        // 2 and 3 -> stored trimmed with origin 1020
        let rule_a = IndexOptimizeMode::SimpleHistogram(1000, 10, 5, 0);
        let entry = get_cache_entry(
            VixSearchResult::Histogram(vec![0, 0, 5, 7, 0]),
            Some(&rule_a),
        )
        .unwrap();
        assert!(matches!(
            &entry,
            CacheEntry::Histogram { origin: 1020, width: 10, counts } if *counts == vec![5, 7]
        ));
        cache.put("k".to_string(), entry);

        // same grid: identical answer
        assert!(matches!(
            cache.get("k", Some(&rule_a)),
            Some(VixSearchResult::Histogram(h)) if h == vec![0, 0, 5, 7, 0]
        ));
        // window slid back 3 buckets (phase preserved): counts shift to 5, 6
        let rule_b = IndexOptimizeMode::SimpleHistogram(970, 10, 12, 0);
        assert!(matches!(
            cache.get("k", Some(&rule_b)),
            Some(VixSearchResult::Histogram(h))
                if h.len() == 12 && h[5] == 5 && h[6] == 7 && h.iter().sum::<u64>() == 12
        ));
        // phase mismatch: a defensive miss, never a shifted answer
        assert!(
            cache
                .get(
                    "k",
                    Some(&IndexOptimizeMode::SimpleHistogram(971, 10, 12, 0))
                )
                .is_none()
        );
        // grid that excludes the entry's occupied buckets: miss, not zeros
        assert!(
            cache
                .get(
                    "k",
                    Some(&IndexOptimizeMode::SimpleHistogram(1040, 10, 5, 0))
                )
                .is_none()
        );
        // width mismatch: miss
        assert!(
            cache
                .get(
                    "k",
                    Some(&IndexOptimizeMode::SimpleHistogram(1000, 20, 5, 0))
                )
                .is_none()
        );
        // no rule at all (defensive): miss
        assert!(cache.get("k", None).is_none());
    }

    #[test]
    fn test_get_cache_entry_row_ids_selection() {
        let result = VixSearchResult::RowIdsSelection {
            row_ids: Arc::new(RowIdBitmap::from_row_ids(4, [0u32, 2])),
            row_group_size: Some(1024),
        };

        let entry = get_cache_entry(result, None).unwrap();
        match entry {
            CacheEntry::RowIds(packed, row_group_size) => {
                assert_eq!(packed.matched(), 2);
                assert_eq!(packed.iter().collect::<Vec<_>>(), vec![0, 2]);
                assert_eq!(row_group_size, Some(1024));
            }
            _ => panic!("Expected RowIds cache entry"),
        }
    }

    #[test]
    fn test_get_cache_entry_count() {
        let entry = get_cache_entry(VixSearchResult::Count(42), None).unwrap();
        match entry {
            CacheEntry::Count(count) => {
                assert_eq!(count, 42);
            }
            _ => panic!("Expected Count cache entry"),
        }
    }

    #[test]
    fn test_guard_matched_rows_no_match() {
        let file = create_file_key(1, 10);
        let guarded = guard_matched_rows("test", &file, 0, 1000).unwrap();
        assert!(matches!(guarded, Some(VixSearchResult::NoMatch)));

        let mut empty_file = create_file_key(1, 10);
        empty_file.meta.records = 0;
        let guarded = guard_matched_rows("test", &empty_file, 5, 1000).unwrap();
        assert!(matches!(guarded, Some(VixSearchResult::NoMatch)));
    }

    #[test]
    fn test_guard_matched_rows_row_count_mismatch_is_error() {
        let file = create_file_key(1, 10); // records = 1000
        let result = guard_matched_rows("test", &file, 5, 999);
        assert!(result.is_err());
    }

    #[test]
    fn test_guard_matched_rows_passes_aligned_bitmap() {
        let file = create_file_key(1, 10); // records = 1000
        let guarded = guard_matched_rows("test", &file, 5, 1000).unwrap();
        assert!(guarded.is_none());
    }

    #[test]
    fn test_get_cache_entry_fast_path_results() {
        let entry = get_cache_entry_none(VixSearchResult::SelectCandidates {
            candidates: Arc::new(vec![(100i64, 7u32), (99, 3)]),
            row_group_size: Some(1024),
        });
        match entry {
            CacheEntry::SelectCandidates(cached, row_group_size) => {
                assert_eq!(*cached, vec![(100, 7), (99, 3)]);
                assert_eq!(row_group_size, Some(1024));
            }
            _ => panic!("Expected SelectCandidates cache entry"),
        }

        let hist_rule = IndexOptimizeMode::SimpleHistogram(0, 60, 3, 0);
        let entry =
            get_cache_entry(VixSearchResult::Histogram(vec![1, 2, 3]), Some(&hist_rule)).unwrap();
        assert!(matches!(
            &entry,
            CacheEntry::Histogram { origin: 0, width: 60, counts } if *counts == vec![1, 2, 3]
        ));
        // a histogram without its rule is simply not cacheable
        assert!(get_cache_entry(VixSearchResult::Histogram(vec![1]), None).is_none());

        let entry = get_cache_entry_none(VixSearchResult::MultiHistogram(vec![(
            1,
            "a".to_string(),
            2,
        )]));
        assert!(
            matches!(entry, CacheEntry::MultiHistogram(rows) if rows == vec![(1, "a".to_string(), 2)])
        );

        let entry = get_cache_entry_none(VixSearchResult::TopN(vec![(vec!["a".to_string()], 2)]));
        assert!(
            matches!(entry, CacheEntry::TopN(rows) if rows == vec![(vec!["a".to_string()], 2)])
        );

        let entry =
            get_cache_entry_none(VixSearchResult::Distinct(HashSet::from(["v".to_string()])));
        assert!(matches!(entry, CacheEntry::Distinct(values) if values.contains("v")));
    }

    /// Build one core file with 10 rows ordered `_timestamp` DESC starting
    /// at `max_ts`, and its FileKey.
    fn build_select_file(name: &str, max_ts: i64) -> (VixReader, FileKey) {
        use arrow::{
            array::{Int64Array, RecordBatch, StringArray},
            datatypes::{DataType, Field, Schema},
        };
        use vortex_index::{VixWriter, VixWriterOptions};

        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("level", DataType::Utf8, true),
        ]));
        let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
        let ts: Vec<i64> = (0..10).map(|i| max_ts - i).collect();
        let levels: Vec<&str> = (0..10).map(|_| "info").collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ts.clone())),
                Arc::new(StringArray::from(levels)),
            ],
        )
        .unwrap();
        let sources: Vec<String> = ts
            .iter()
            .map(|t| format!(r#"{{"_timestamp":{t},"level":"info"}}"#))
            .collect();
        writer
            .push_batch_with_source(&batch, &StringArray::from(sources), None)
            .unwrap();
        let reader = VixReader::open(bytes::Bytes::from(writer.finish().unwrap())).unwrap();
        let file = FileKey {
            key: name.to_string(),
            meta: FileMeta {
                min_ts: max_ts - 9,
                max_ts,
                records: 10,
                ..Default::default()
            },
            ..Default::default()
        };
        (reader, file)
    }

    /// Pilot fix A read-side: equality on an fts field (tokens only, no raw
    /// values) is skipped per file with the filter added back — never a
    /// silent empty result.
    #[test]
    fn test_equality_on_fts_field_falls_back_per_file() {
        use arrow::{
            array::{Int64Array, RecordBatch, StringArray},
            datatypes::{DataType, Field, Schema},
        };
        use vortex_index::{VixWriter, VixWriterOptions};

        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("level", DataType::Utf8, true),
            Field::new("message", DataType::Utf8, true),
        ]));
        let opts = VixWriterOptions {
            fts_field_names: vec!["message".to_string()],
            ..Default::default()
        };
        let mut writer = VixWriter::new(&schema, opts, false);
        let ts = vec![100i64, 99, 98, 97];
        let levels = vec![Some("info"), Some("error"), Some("info"), Some("error")];
        let messages = vec![
            Some("hello world"),
            Some("goodbye world"),
            Some("hello again"),
            None,
        ];
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ts.clone())),
                Arc::new(StringArray::from(levels)),
                Arc::new(StringArray::from(messages)),
            ],
        )
        .unwrap();
        let sources: Vec<String> = ts
            .iter()
            .map(|t| format!(r#"{{"_timestamp":{t}}}"#))
            .collect();
        writer
            .push_batch_with_source(&batch, &StringArray::from(sources), None)
            .unwrap();
        let reader = VixReader::open(bytes::Bytes::from(writer.finish().unwrap())).unwrap();
        assert!(!reader.has_term_capability("message"));
        assert!(reader.has_term_capability("level"));

        // mixed condition: the fts equality is skipped, the term field
        // still narrows the file
        let condition = IndexCondition {
            conditions: vec![
                Condition::Equal("message".to_string(), "hello world".to_string()),
                Condition::Equal("level".to_string(), "info".to_string()),
            ],
        };
        match evaluate_vix_index("t", &reader, &condition, None, (0, 1000), true, None).unwrap() {
            RawVixResult::Bitmap {
                bitmap,
                has_skipped,
                ..
            } => {
                assert!(has_skipped, "fts equality must be skipped, not answered");
                assert_eq!(bitmap.set_indices().collect::<Vec<_>>(), vec![0, 2]);
            }
            _ => panic!("expected a bitmap"),
        }

        // a lone fts-field equality builds no per-file query at all: the
        // evaluation errors and the caller keeps the file for the scan with
        // the filter re-applied (add-filter-back)
        let lone = IndexCondition {
            conditions: vec![Condition::Equal(
                "message".to_string(),
                "hello world".to_string(),
            )],
        };
        assert!(evaluate_vix_index("t", &reader, &lone, None, (0, 1000), true, None).is_err());

        // match_all over the fts tokens is unaffected by fix A
        let match_all = IndexCondition {
            conditions: vec![Condition::MatchAll("hello".to_string())],
        };
        match evaluate_vix_index("t", &reader, &match_all, None, (0, 1000), true, None).unwrap() {
            RawVixResult::Bitmap {
                bitmap,
                has_skipped,
                ..
            } => {
                assert!(!has_skipped);
                assert_eq!(bitmap.set_indices().collect::<Vec<_>>(), vec![0, 2]);
            }
            _ => panic!("expected a bitmap"),
        }
    }

    /// Straddle bitmap cache: a window-straddling file's CONDITION bitmap is
    /// time-independent and memoized under the clamp-free key, so a sliding
    /// dashboard window re-clamps a cached bitmap instead of re-decoding
    /// postings. Rows: ts 100/99/98/97, match_all("hello") matches rows 0
    /// (ts 100) and 2 (ts 98).
    #[test]
    fn test_straddle_bitmap_cache_serves_sliding_windows() {
        use arrow::{
            array::{Int64Array, RecordBatch, StringArray},
            datatypes::{DataType, Field, Schema},
        };
        use vortex_index::{VixWriter, VixWriterOptions};

        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("message", DataType::Utf8, true),
        ]));
        let opts = VixWriterOptions {
            fts_field_names: vec!["message".to_string()],
            ..Default::default()
        };
        let mut writer = VixWriter::new(&schema, opts, false);
        let ts = vec![100i64, 99, 98, 97];
        let messages = vec![
            Some("hello world"),
            Some("goodbye world"),
            Some("hello again"),
            None,
        ];
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ts.clone())),
                Arc::new(StringArray::from(messages)),
            ],
        )
        .unwrap();
        let sources: Vec<String> = ts
            .iter()
            .map(|t| format!(r#"{{"_timestamp":{t}}}"#))
            .collect();
        writer
            .push_batch_with_source(&batch, &StringArray::from(sources), None)
            .unwrap();
        let reader = VixReader::open(bytes::Bytes::from(writer.finish().unwrap())).unwrap();
        let condition = IndexCondition {
            conditions: vec![Condition::MatchAll("hello".to_string())],
        };
        let clamped_rows = |raw: RawVixResult| match raw {
            RawVixResult::Bitmap { bitmap, .. } => bitmap.set_indices().collect::<Vec<_>>(),
            _ => panic!("expected a bitmap"),
        };

        // miss + populate: window [99, 1000) clamps row 2 (ts 98) away, but
        // the CACHED bitmap must be the PRE-clamp condition bitmap [0, 2]
        let key = "bitmap-cache-test-k1".to_string();
        let raw = evaluate_vix_index(
            "t",
            &reader,
            &condition,
            None,
            (99, 1000),
            false,
            Some(key.clone()),
        )
        .unwrap();
        assert_eq!(clamped_rows(raw), vec![0], "ts 98 is outside [99,1000)");
        match vix_result_cache::GLOBAL_CACHE.get(&key, None) {
            Some(VixSearchResult::RowIdsSelection { row_ids, .. }) => {
                assert_eq!(
                    row_ids.iter().collect::<Vec<_>>(),
                    vec![0, 2],
                    "the cache must hold the PRE-clamp condition bitmap"
                );
            }
            other => panic!("expected a cached RowIds bitmap, got {other:?}"),
        }

        // hit path proof by poisoning: overwrite the entry with all-rows-set;
        // a re-eval under the same key must reflect the poisoned bitmap
        // (clamp of all-set over [99,1000) = rows 0 and 1), proving the
        // condition bitmap was served from the cache, not recomputed
        vix_result_cache::GLOBAL_CACHE.put(
            key.clone(),
            CacheEntry::RowIds(Arc::new(RowIdBitmap::from_row_ids(4, 0..4u32)), None),
        );
        let raw = evaluate_vix_index(
            "t",
            &reader,
            &condition,
            None,
            (99, 1000),
            false,
            Some(key.clone()),
        )
        .unwrap();
        assert_eq!(clamped_rows(raw), vec![0, 1], "poisoned bitmap must serve");

        // SimpleCount over a straddling file flows through the same cache
        let raw = evaluate_vix_index(
            "t",
            &reader,
            &condition,
            Some(IndexOptimizeMode::SimpleCount),
            (99, 1000),
            false,
            Some(key.clone()),
        )
        .unwrap();
        match raw {
            RawVixResult::Count { count, .. } => assert_eq!(count, 2, "poisoned count"),
            _ => panic!("expected a count"),
        }

        // a NoMatch entry under the key maps to the all-zeros bitmap
        let key_nm = "bitmap-cache-test-k2".to_string();
        vix_result_cache::GLOBAL_CACHE.put(key_nm.clone(), CacheEntry::NoMatch);
        let raw = evaluate_vix_index(
            "t",
            &reader,
            &condition,
            None,
            (99, 1000),
            false,
            Some(key_nm),
        )
        .unwrap();
        assert_eq!(clamped_rows(raw), Vec::<usize>::new());

        // defensive: a cached bitmap whose length mismatches the file's row
        // count is a MISS (recompute), never a wrong-length AND
        let key_bad = "bitmap-cache-test-k3".to_string();
        vix_result_cache::GLOBAL_CACHE.put(
            key_bad.clone(),
            CacheEntry::RowIds(Arc::new(RowIdBitmap::from_row_ids(3, 0..3u32)), None),
        );
        let raw = evaluate_vix_index(
            "t",
            &reader,
            &condition,
            None,
            (99, 1000),
            false,
            Some(key_bad.clone()),
        )
        .unwrap();
        assert_eq!(
            clamped_rows(raw),
            vec![0],
            "mismatched entry must be recomputed"
        );
        match vix_result_cache::GLOBAL_CACHE.get(&key_bad, None) {
            Some(VixSearchResult::RowIdsSelection { row_ids, .. }) => {
                assert_eq!(
                    row_ids.num_rows(),
                    4,
                    "recompute must overwrite the bad entry"
                );
            }
            other => panic!("expected the overwritten RowIds bitmap, got {other:?}"),
        }
    }

    /// Stage 4 (cuts+ranks): SimpleCount and SimpleHistogram over a dense
    /// out-of-row term answer from skip-table rank diffs per zone chunk —
    /// and must match the bitmap path EXACTLY on the same rows, across
    /// full-range and straddling windows, coarse (chunk-folding) and fine
    /// (bucket-straddling) grids. Multi-chunk file via tiny docs chunks;
    /// deterministic scattered timestamps.
    #[test]
    fn test_ranked_consumers_match_bitmap_path() {
        use arrow::{
            array::{Int64Array, RecordBatch, StringArray},
            datatypes::{DataType, Field, Schema},
        };
        use vortex_index::{VixWriter, VixWriterOptions};

        const N: usize = 4000;
        let mut lcg = 0xBADC0FFEE0DDF00Du64;
        let mut next = move |bound: u64| {
            lcg = lcg
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (lcg >> 16) % bound
        };
        // timestamps scattered over [10_000, 90_000), ~70% of rows carry the
        // dense token, the rest a filler token
        let ts: Vec<i64> = (0..N).map(|_| 10_000 + next(80_000) as i64).collect();
        let msgs: Vec<Option<String>> = (0..N)
            .map(|i| {
                Some(if next(10) < 7 {
                    format!("hello event {i}")
                } else {
                    format!("filler event {i}")
                })
            })
            .collect();
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("message", DataType::Utf8, true),
        ]));
        let build = |plist_min_docs: u32| -> VixReader {
            let opts = VixWriterOptions {
                fts_field_names: vec!["message".to_string()],
                postings_plist_min_docs: plist_min_docs,
                docs_chunk_bytes: 4096, // force many zone chunks
                ..Default::default()
            };
            let mut writer = VixWriter::new(&schema, opts, false);
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(ts.clone())),
                    Arc::new(StringArray::from(msgs.clone())),
                ],
            )
            .unwrap();
            let sources: Vec<String> = ts
                .iter()
                .map(|t| format!(r#"{{"_timestamp":{t}}}"#))
                .collect();
            writer
                .push_batch_with_source(&batch, &StringArray::from(sources), None)
                .unwrap();
            VixReader::open(bytes::Bytes::from(writer.finish().unwrap())).unwrap()
        };
        let bitmap_file = build(0);
        let plist_file = build(100);
        assert!(
            plist_file
                .zone_chunks()
                .map(|c| c.len() > 4)
                .unwrap_or(false),
            "test premise: the file must have several zone chunks, got {:?}",
            plist_file.zone_chunks().map(|c| c.len())
        );

        let condition = IndexCondition {
            conditions: vec![Condition::MatchAll("hello".to_string())],
        };
        // engagement proof: the plist file opens a cursor for this condition
        {
            let (query, has_skipped) = condition
                .to_vix_query(
                    "t",
                    &|field| field_capability("t", &plist_file, field),
                    &index_match_all_tokens,
                )
                .unwrap();
            assert!(!has_skipped);
            assert!(
                plist_file
                    .single_term_plist_cursor(&query)
                    .unwrap()
                    .is_some(),
                "the dense token must resolve to an out-of-row cursor"
            );
            assert!(
                bitmap_file
                    .single_term_plist_cursor(&query)
                    .unwrap()
                    .is_none(),
                "the plist-less file must not"
            );
        }

        // (window, in_range) shapes: full-range and two straddles
        let windows = [
            ((0i64, 100_000i64), true),
            ((25_000, 100_000), false),
            ((33_333, 61_111), false),
        ];
        for (range, in_range) in windows {
            // SimpleCount
            let counts: Vec<u64> = [&plist_file, &bitmap_file]
                .iter()
                .map(|reader| {
                    match evaluate_vix_index(
                        "t",
                        reader,
                        &condition,
                        Some(IndexOptimizeMode::SimpleCount),
                        range,
                        in_range,
                        None,
                    )
                    .unwrap()
                    {
                        RawVixResult::Count { count, .. } => count,
                        _ => panic!("expected a count"),
                    }
                })
                .collect();
            assert_eq!(counts[0], counts[1], "count mismatch at {range:?}");

            // SimpleHistogram: coarse buckets (chunks fold) and fine buckets
            // (chunks straddle edges -> boundary decode)
            for bucket_width in [50_000u64, 1_000] {
                let num_buckets = ((range.1 - range.0) as u64).div_ceil(bucket_width) as usize;
                let hists: Vec<Vec<u64>> = [&plist_file, &bitmap_file]
                    .iter()
                    .map(|reader| {
                        match evaluate_vix_index(
                            "t",
                            reader,
                            &condition,
                            Some(IndexOptimizeMode::SimpleHistogram(
                                range.0,
                                bucket_width,
                                num_buckets,
                                0,
                            )),
                            range,
                            in_range,
                            None,
                        )
                        .unwrap()
                        {
                            RawVixResult::Histogram { histogram, .. } => histogram,
                            _ => panic!("expected a histogram"),
                        }
                    })
                    .collect();
                assert_eq!(
                    hists[0], hists[1],
                    "histogram mismatch at {range:?} width={bucket_width}"
                );
                // grid totals must also equal the window count
                let total: u64 = hists[0].iter().sum();
                let expected = ts
                    .iter()
                    .zip(&msgs)
                    .filter(|(t, m)| {
                        **t >= range.0
                            && **t < range.1
                            && m.as_deref().is_some_and(|m| m.contains("hello"))
                    })
                    .count() as u64;
                assert_eq!(
                    total, expected,
                    "ground truth at {range:?} w={bucket_width}"
                );
            }
        }
    }

    /// One-off dict-shape probe (set BENCH_VIX_FILE, run --ignored
    /// --nocapture): prints the file's dictionary row-group count, indexed
    /// field count, and times ONE cold TokenAnyField lookup + count.
    #[test]
    #[ignore]
    fn probe_dict_shape_of_bench_file() {
        use std::time::Instant;
        let path = std::env::var("BENCH_VIX_FILE").expect("set BENCH_VIX_FILE");
        let bytes = std::fs::read(&path).unwrap();
        let t = Instant::now();
        let reader = VixReader::open(bytes::Bytes::from(bytes)).unwrap();
        eprintln!("open: {:?}", t.elapsed());
        eprintln!(
            "rows={} terms={} dict_row_groups={} ",
            reader.row_count(),
            reader.term_count(),
            reader.term_row_group_count(),
        );
        let query = vortex_index::VixQuery::TokenAnyField {
            token: b"failed".to_vec(),
        };
        let t = Instant::now();
        let count = reader.count(&query).unwrap();
        eprintln!(
            "cold TokenAnyField count: {:?} (count={count})",
            t.elapsed()
        );
        let t = Instant::now();
        let count2 = reader.count(&query).unwrap();
        eprintln!(
            "warm TokenAnyField count: {:?} (count={count2})",
            t.elapsed()
        );
    }

    /// Deep-merge-scale isolation bench (run with --ignored --nocapture):
    /// one 20M-row file, dense token in ~80% of rows — times the bitmap
    /// eval vs the ranked histogram/count on identical data. This is the
    /// profile where plist pays: per-file postings in the tens of millions.
    #[test]
    #[ignore]
    fn bench_ranked_vs_bitmap_at_deep_merge_scale() {
        use std::time::Instant;

        use arrow::{
            array::{Int64Array, RecordBatch, StringArray},
            datatypes::{DataType, Field, Schema},
        };
        use vortex_index::{VixWriter, VixWriterOptions};

        const N: usize = 20_000_000;
        const BASE: i64 = 1_000_000_000;
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("message", DataType::Utf8, true),
        ]));
        let opts = VixWriterOptions {
            fts_field_names: vec!["message".to_string()],
            postings_plist_min_docs: 8192,
            ..Default::default()
        };
        let mut writer = VixWriter::new(&schema, opts, false);
        let t_build = Instant::now();
        // push in 1M-row batches to bound memory
        let mut lcg = 0x5EEDCAFEF00Du64;
        let mut next = move |bound: u64| {
            lcg = lcg
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (lcg >> 16) % bound
        };
        for chunk_start in (0..N).step_by(1_000_000) {
            let n = 1_000_000.min(N - chunk_start);
            let ts: Vec<i64> = (0..n).map(|i| BASE + (chunk_start + i) as i64).collect();
            let msgs: Vec<Option<String>> = (0..n)
                .map(|_| {
                    Some(if next(10) < 8 {
                        "failed request".to_string()
                    } else {
                        "ok request".to_string()
                    })
                })
                .collect();
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(ts.clone())),
                    Arc::new(StringArray::from(msgs)),
                ],
            )
            .unwrap();
            let sources: Vec<String> = ts
                .iter()
                .map(|t| format!(r#"{{"_timestamp":{t}}}"#))
                .collect();
            writer
                .push_batch_with_source(&batch, &StringArray::from(sources), None)
                .unwrap();
        }
        let reader = VixReader::open(bytes::Bytes::from(writer.finish().unwrap())).unwrap();
        eprintln!("build: {:?} ({N} rows)", t_build.elapsed());
        let condition = IndexCondition {
            conditions: vec![Condition::MatchAll("failed".to_string())],
        };
        let (query, _) = condition
            .to_vix_query(
                "b",
                &|field| field_capability("b", &reader, field),
                &index_match_all_tokens,
            )
            .unwrap();
        let cursor = reader.single_term_plist_cursor(&query).unwrap().unwrap();
        eprintln!("dense term doc_count = {}", cursor.doc_count());

        // bitmap eval (what the pre-stage-4 histogram/straddle-count paid)
        let t = Instant::now();
        let bitmap = reader.eval(&query).unwrap();
        let t_eval = t.elapsed();
        eprintln!(
            "bitmap eval (decode {}M ids): {:?}",
            cursor.doc_count() / 1_000_000,
            t_eval
        );

        // bitmap histogram on top of the eval
        let t = Instant::now();
        let h_bitmap =
            collect::simple_histogram(&reader, &bitmap, BASE, 1_000_000, N / 1_000_000, 0).unwrap();
        eprintln!("bitmap histogram fold: {:?}", t.elapsed());

        // ranked histogram (stage 4): no bitmap at all
        let t = Instant::now();
        let h_ranked = collect::ranked_simple_histogram(
            &reader,
            &cursor,
            BASE,
            1_000_000,
            N / 1_000_000,
            0,
            (BASE, BASE + N as i64),
        )
        .unwrap();
        let t_ranked = t.elapsed();
        eprintln!("ranked histogram (rank diffs): {:?}", t_ranked);
        assert_eq!(h_bitmap, h_ranked, "the two paths must agree");

        // ranked windowed count (stage 4 straddle-count path)
        let t = Instant::now();
        let c =
            collect::ranked_count_in_window(&reader, &cursor, BASE + N as i64 / 3, BASE + N as i64)
                .unwrap();
        eprintln!("ranked windowed count: {:?} (count={c})", t.elapsed());
        let t = Instant::now();
        let clamped = &bitmap
            & &reader
                .timestamp_range(BASE + N as i64 / 3, BASE + N as i64)
                .unwrap();
        let c2 = clamped.count_set_bits() as u64;
        eprintln!(
            "bitmap windowed count (excl eval): {:?} (count={c2})",
            t.elapsed()
        );
        assert_eq!(c, c2);
    }

    /// Absent-field fix: a condition on a field NO document of the file
    /// carries evaluates to the exact empty result (`VixQuery::Nothing`) —
    /// `has_skipped` stays false, the empty bitmap becomes `NoMatch` via
    /// `guard_matched_rows`, and the collector removes the file instead of
    /// scanning it (the pre-fix behavior errored with AllConditionsSkipped
    /// and fully scanned every such file).
    #[test]
    fn test_absent_field_condition_eliminates_file() {
        let (reader, file) = build_select_file("file_absent.vix", 1000);
        assert!(reader.has_term_capability("level"));
        assert!(!reader.key_term_exists("client_id").unwrap());

        let expect_empty_bitmap = |condition: IndexCondition| {
            match evaluate_vix_index("t", &reader, &condition, None, (0, 2000), true, None).unwrap()
            {
                RawVixResult::Bitmap {
                    bitmap,
                    has_skipped,
                    row_count,
                    ..
                } => {
                    assert!(!has_skipped, "absent fields are exact, never a skip");
                    assert_eq!(bitmap.count_set_bits(), 0);
                    // the empty bitmap turns into NoMatch (file removed, no
                    // filter-back) through the existing guard
                    let guarded =
                        guard_matched_rows("t", &file, bitmap.count_set_bits(), row_count).unwrap();
                    assert!(matches!(guarded, Some(VixSearchResult::NoMatch)));
                }
                _ => panic!("expected a bitmap"),
            }
        };

        // the live-bug shape: a lone equality on the absent field
        expect_empty_bitmap(IndexCondition {
            conditions: vec![Condition::Equal("client_id".into(), "x".into())],
        });
        // NotEqual: SQL three-valued logic (NULL != 'x' is not TRUE)
        expect_empty_bitmap(IndexCondition {
            conditions: vec![Condition::NotEqual("client_id".into(), "x".into())],
        });
        // absent + servable under the AND list: still exact and empty
        expect_empty_bitmap(IndexCondition {
            conditions: vec![
                Condition::Equal("client_id".into(), "x".into()),
                Condition::Equal("level".into(), "info".into()),
            ],
        });
        // IS NOT NULL goes through KeyExists (ungated) and is exact too
        expect_empty_bitmap(IndexCondition {
            conditions: vec![Condition::IsNotNull("client_id".into())],
        });

        // simple-select fast path: zero exact candidates, not a scan
        let condition = IndexCondition {
            conditions: vec![Condition::Equal("client_id".into(), "x".into())],
        };
        match evaluate_vix_index(
            "t",
            &reader,
            &condition,
            Some(IndexOptimizeMode::SimpleSelect(51, false)),
            (0, 2000),
            true,
            None,
        )
        .unwrap()
        {
            RawVixResult::SelectCandidates { candidates, .. } => {
                assert!(candidates.is_empty());
            }
            _ => panic!("expected select candidates"),
        }

        // simple-count fast path: exact zero
        match evaluate_vix_index(
            "t",
            &reader,
            &condition,
            Some(IndexOptimizeMode::SimpleCount),
            (0, 2000),
            true,
            None,
        )
        .unwrap()
        {
            RawVixResult::Count { count, has_skipped } => {
                assert_eq!(count, 0);
                assert!(!has_skipped);
            }
            _ => panic!("expected a count"),
        }

        // an OR mixing the absent field with a servable one cannot be
        // decided by the index: it keeps the skip + filter-back fallback
        let mixed = IndexCondition {
            conditions: vec![Condition::Or(
                Box::new(Condition::Equal("client_id".into(), "x".into())),
                Box::new(Condition::Equal("level".into(), "info".into())),
            )],
        };
        assert!(evaluate_vix_index("t", &reader, &mixed, None, (0, 2000), true, None).is_err());
    }

    /// Build one core file whose `svc` values are term-indexed but NOT
    /// column-stored. `svc` values may be `None` or `""` (empty strings are
    /// raw-indexed like any other value since the wave-A writer fix, so the
    /// dictionary serves them too).
    fn build_term_only_file(
        name: &str,
        max_ts: i64,
        svcs: &[Option<&str>],
    ) -> (VixReader, FileKey) {
        use arrow::{
            array::{Int64Array, RecordBatch, StringArray},
            datatypes::{DataType, Field, Schema},
        };
        use vortex_index::{VixWriter, VixWriterOptions};

        let rows = svcs.len();
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("svc", DataType::Utf8, true),
        ]));
        let ts: Vec<i64> = (0..rows as i64).map(|i| max_ts - i).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ts.clone())),
                Arc::new(StringArray::from(svcs.to_vec())),
            ],
        )
        .unwrap();
        let sources: Vec<String> = (0..rows)
            .map(|i| match svcs[i] {
                Some(svc) => format!(
                    r#"{{"_timestamp":{},"svc":{}}}"#,
                    ts[i],
                    serde_json::to_string(svc).unwrap()
                ),
                None => format!(r#"{{"_timestamp":{}}}"#, ts[i]),
            })
            .collect();
        let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
        writer
            .push_batch_with_source(&batch, &StringArray::from(sources), None)
            .unwrap();
        let reader = VixReader::open(bytes::Bytes::from(writer.finish().unwrap())).unwrap();
        let file = FileKey {
            key: name.to_string(),
            meta: FileMeta {
                min_ts: max_ts - rows as i64 + 1,
                max_ts,
                records: rows as i64,
                ..Default::default()
            },
            ..Default::default()
        };
        (reader, file)
    }

    /// Pilot fix B end-to-end: unfiltered full-range TopN/Distinct over a
    /// field that is NOT column-stored. Both files serve from the term
    /// dictionary (empty-string values are ordinary dictionary citizens
    /// since the wave-A writer fix), and a partial-time-range evaluation
    /// falls back per file (MissingColumn; the flight-side routing keeps
    /// such files in the DataFusion branch).
    #[test]
    fn test_unfiltered_topn_distinct_over_term_indexed_field() {
        let condition = IndexCondition {
            conditions: vec![Condition::All()],
        };
        let (reader_a, _file_a) = build_term_only_file(
            "file_a.vix",
            1000,
            &[
                Some("api"),
                Some("api"),
                Some("db"),
                Some("api"),
                None,
                Some("web"),
            ],
        );
        let (reader_b, _file_b) =
            build_term_only_file("file_b.vix", 500, &[Some("api"), Some(""), Some("auth")]);

        // both are dictionary-servable — the "" value is raw-indexed too;
        // neither stores svc as a docs column
        assert!(reader_a.field_value_counts("svc").unwrap().is_some());
        assert!(reader_b.field_value_counts("svc").unwrap().is_some());
        assert!(!reader_a.has_column_store_field("svc"));
        assert!(!reader_b.has_column_store_field("svc"));

        let full_range = (0i64, 2000i64);
        let topn_rule = IndexOptimizeMode::SimpleTopN(vec!["svc".to_string()], 10, false);
        let mut merged: Vec<(Vec<String>, u64)> = Vec::new();
        for reader in [&reader_a, &reader_b] {
            match evaluate_vix_index(
                "t",
                reader,
                &condition,
                Some(topn_rule.clone()),
                full_range,
                true,
                None,
            )
            .unwrap()
            {
                RawVixResult::TopN {
                    groups,
                    has_skipped,
                } => {
                    assert!(!has_skipped);
                    merged.extend(groups);
                }
                _ => panic!("expected TopN"),
            }
        }
        // re-aggregate like the final DataFusion aggregate does
        let mut totals: std::collections::BTreeMap<String, u64> = Default::default();
        for (key, count) in merged {
            *totals.entry(key[0].clone()).or_default() += count;
        }
        assert_eq!(
            totals,
            std::collections::BTreeMap::from([
                ("".to_string(), 1),
                ("api".to_string(), 4),
                ("auth".to_string(), 1),
                ("db".to_string(), 1),
                ("web".to_string(), 1),
            ])
        );

        // Distinct, asc and desc, over both serving paths
        for (reader, ascend, expected) in [
            (&reader_a, true, HashSet::from(["api".into(), "db".into()])),
            (&reader_a, false, HashSet::from(["web".into(), "db".into()])),
            (&reader_b, true, HashSet::from(["".into(), "api".into()])),
            (
                &reader_b,
                false,
                HashSet::from(["auth".into(), "api".into()]),
            ),
        ] {
            let rule = IndexOptimizeMode::SimpleDistinct("svc".to_string(), 2, ascend);
            match evaluate_vix_index("t", reader, &condition, Some(rule), full_range, true, None)
                .unwrap()
            {
                RawVixResult::Distinct {
                    values,
                    has_skipped,
                } => {
                    assert!(!has_skipped);
                    assert_eq!(values, expected, "ascend={ascend}");
                }
                _ => panic!("expected Distinct"),
            }
        }

        // a file only partially inside the range cannot use raw doc_counts
        // (they include out-of-range docs) — the filtered dictionary path
        // now serves it exactly: per value, postings ∩ the time bitmap.
        // file_a stamps ts descending from 1000: api@1000, api@999, db@998,
        // api@997, None@996, web@995 — [996, 1200) keeps api=3, db=1.
        let narrow = (996i64, 1200i64);
        match evaluate_vix_index(
            "t",
            &reader_a,
            &condition,
            Some(topn_rule),
            narrow,
            false,
            None,
        )
        .unwrap()
        {
            RawVixResult::TopN {
                groups,
                has_skipped,
            } => {
                assert!(!has_skipped);
                let totals: std::collections::BTreeMap<String, u64> = groups
                    .into_iter()
                    .map(|(key, count)| (key[0].clone(), count))
                    .collect();
                assert_eq!(
                    totals,
                    std::collections::BTreeMap::from([
                        ("api".to_string(), 3),
                        ("db".to_string(), 1),
                    ])
                );
            }
            _ => panic!("expected TopN via the filtered dictionary path"),
        }
    }

    /// THE prod shape (2026-08-03, backlog #21): `WHERE service = X GROUP BY
    /// attribute ORDER BY count DESC LIMIT n` over files where the group
    /// attribute is term-indexed but NOT column-stored (all pre-setting
    /// history). Served from the dictionary: per value, its postings
    /// intersected with the condition bitmap — never the docs columns,
    /// never `_source`.
    #[test]
    fn test_filtered_topn_distinct_over_term_indexed_field() {
        use arrow::{
            array::{Int64Array, RecordBatch, StringArray},
            datatypes::{DataType, Field, Schema},
        };
        use vortex_index::{VixWriter, VixWriterOptions};

        let svcs = [
            Some("api"),
            Some("api"),
            Some("db"),
            Some("api"),
            Some("api"),
            Some("web"),
            None,
        ];
        let fns = [
            Some("handle"),
            Some("route"),
            Some("handle"),
            Some("handle"),
            None,
            Some("render"),
            Some("orphan"),
        ];
        let rows = svcs.len();
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("svc", DataType::Utf8, true),
            Field::new("code_fn", DataType::Utf8, true),
        ]));
        let ts: Vec<i64> = (0..rows as i64).map(|i| 1000 - i).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ts.clone())),
                Arc::new(StringArray::from(svcs.to_vec())),
                Arc::new(StringArray::from(fns.to_vec())),
            ],
        )
        .unwrap();
        let sources: Vec<String> = (0..rows)
            .map(|i| {
                let mut obj = serde_json::Map::new();
                obj.insert("_timestamp".into(), ts[i].into());
                if let Some(svc) = svcs[i] {
                    obj.insert("svc".into(), svc.into());
                }
                if let Some(f) = fns[i] {
                    obj.insert("code_fn".into(), f.into());
                }
                serde_json::Value::Object(obj).to_string()
            })
            .collect();
        let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
        writer
            .push_batch_with_source(&batch, &StringArray::from(sources), None)
            .unwrap();
        let reader = VixReader::open(bytes::Bytes::from(writer.finish().unwrap())).unwrap();
        assert!(!reader.has_column_store_field("code_fn"), "test premise");

        let condition = IndexCondition {
            conditions: vec![Condition::Equal("svc".into(), "api".into())],
        };
        let full_range = (0i64, 2000i64);

        // svc=api rows (indices 0,1,3,4) carry code_fn: handle, route,
        // handle, None -> handle=2, route=1 (the api row without code_fn
        // forms no group; web/render, None/orphan and db's handle are
        // filtered out)
        match evaluate_vix_index(
            "t",
            &reader,
            &condition,
            Some(IndexOptimizeMode::SimpleTopN(
                vec!["code_fn".to_string()],
                10,
                false,
            )),
            full_range,
            true,
            None,
        )
        .unwrap()
        {
            RawVixResult::TopN {
                groups,
                has_skipped,
            } => {
                assert!(!has_skipped);
                let totals: std::collections::BTreeMap<String, u64> = groups
                    .into_iter()
                    .map(|(key, count)| (key[0].clone(), count))
                    .collect();
                assert_eq!(
                    totals,
                    std::collections::BTreeMap::from([
                        ("handle".to_string(), 2),
                        ("route".to_string(), 1),
                    ])
                );
            }
            _ => panic!("expected TopN via the filtered dictionary path"),
        }

        // Distinct under the same condition: only values with an api hit,
        // ascending/descending ends of the value order
        for (ascend, expected) in [
            (true, HashSet::from(["handle".to_string()])),
            (false, HashSet::from(["route".to_string()])),
        ] {
            let rule = IndexOptimizeMode::SimpleDistinct("code_fn".to_string(), 1, ascend);
            match evaluate_vix_index("t", &reader, &condition, Some(rule), full_range, true, None)
                .unwrap()
            {
                RawVixResult::Distinct {
                    values,
                    has_skipped,
                } => {
                    assert!(!has_skipped);
                    assert_eq!(values, expected, "ascend={ascend}");
                }
                _ => panic!("expected Distinct via the filtered dictionary path"),
            }
        }
    }

    /// SimpleSelect end to end over three synthetic core files: per-file
    /// candidates from the docs columns, then the global cross-file merge
    /// narrows the file list to the exact top-N rows.
    #[test]
    fn test_simple_select_global_merge_across_three_files() {
        let (reader_a, file_a) = build_select_file("file_a", 300); // ts 300..291
        let (reader_b, file_b) = build_select_file("file_b", 200); // ts 200..191
        let (reader_c, file_c) = build_select_file("file_c", 100); // ts 100..91

        let limit = 5usize;
        let readers = [
            (&reader_a, &file_a),
            (&reader_b, &file_b),
            (&reader_c, &file_c),
        ];

        // ---- descending (ORDER BY _timestamp DESC LIMIT 5) ----
        let groups = vec![vec![file_a.clone(), file_b.clone(), file_c.clone()]];
        let mut builder = MultiResultBuilder::new(
            &Some(IndexOptimizeMode::SimpleSelect(limit, false)),
            &groups,
        );
        let mut file_map: HashMap<String, FileKey> = groups[0]
            .iter()
            .map(|f| (f.key.clone(), f.clone()))
            .collect();
        for (reader, file) in readers {
            let bitmap = BooleanBuffer::new_set(reader.row_count() as usize);
            let candidates = collect::simple_select(reader, &bitmap, limit, false).unwrap();
            builder.add_select_candidates(file.key.clone(), Arc::new(candidates), None);
        }
        let result = builder.build("test", &mut file_map);
        match result {
            MultiResult::SimpleSelect(num) => assert_eq!(num, 15), // 5 per file
            _ => panic!("expected SimpleSelect"),
        }
        // global top-5 desc = the 5 newest rows, all in file_a (rows 0..4)
        assert_eq!(file_map.len(), 1);
        let selection = file_map["file_a"].selection.as_ref().unwrap();
        match selection {
            FileSelection::Rows(bits) => {
                assert_eq!(bits.iter().collect::<Vec<_>>(), vec![0, 1, 2, 3, 4]);
            }
            other => panic!("expected Rows selection, got {other:?}"),
        }

        // ---- ascending (ORDER BY _timestamp ASC LIMIT 5) ----
        let groups = vec![vec![file_a.clone(), file_b.clone(), file_c.clone()]];
        let mut builder =
            MultiResultBuilder::new(&Some(IndexOptimizeMode::SimpleSelect(limit, true)), &groups);
        let mut file_map: HashMap<String, FileKey> = groups[0]
            .iter()
            .map(|f| (f.key.clone(), f.clone()))
            .collect();
        for (reader, file) in [
            (&reader_a, &file_a),
            (&reader_b, &file_b),
            (&reader_c, &file_c),
        ] {
            let bitmap = BooleanBuffer::new_set(reader.row_count() as usize);
            let candidates = collect::simple_select(reader, &bitmap, limit, true).unwrap();
            builder.add_select_candidates(file.key.clone(), Arc::new(candidates), None);
        }
        builder.build("test", &mut file_map);
        // global top-5 asc = the 5 oldest rows: ts 91..95 = file_c rows 9..5
        assert_eq!(file_map.len(), 1);
        let selection = file_map["file_c"].selection.as_ref().unwrap();
        match selection {
            FileSelection::Rows(bits) => {
                assert_eq!(bits.iter().collect::<Vec<_>>(), vec![5, 6, 7, 8, 9]);
            }
            other => panic!("expected Rows selection, got {other:?}"),
        }

        // ---- desc with a limit spanning two files ----
        let groups = vec![vec![file_a.clone(), file_b.clone(), file_c.clone()]];
        let mut builder =
            MultiResultBuilder::new(&Some(IndexOptimizeMode::SimpleSelect(12, false)), &groups);
        let mut file_map: HashMap<String, FileKey> = groups[0]
            .iter()
            .map(|f| (f.key.clone(), f.clone()))
            .collect();
        for (reader, file) in [
            (&reader_a, &file_a),
            (&reader_b, &file_b),
            (&reader_c, &file_c),
        ] {
            let bitmap = BooleanBuffer::new_set(reader.row_count() as usize);
            let candidates = collect::simple_select(reader, &bitmap, 12, false).unwrap();
            builder.add_select_candidates(file.key.clone(), Arc::new(candidates), None);
        }
        builder.build("test", &mut file_map);
        // top-12 desc: all 10 rows of file_a + the 2 newest of file_b
        assert_eq!(file_map.len(), 2);
        match file_map["file_a"].selection.as_ref().unwrap() {
            FileSelection::Rows(bits) => assert_eq!(bits.matched(), 10),
            other => panic!("expected Rows selection, got {other:?}"),
        }
        match file_map["file_b"].selection.as_ref().unwrap() {
            FileSelection::Rows(bits) => {
                assert_eq!(bits.iter().collect::<Vec<_>>(), vec![0, 1]);
            }
            other => panic!("expected Rows selection, got {other:?}"),
        }
    }

    /// The no-filter select-star shape (condition ALL + SimpleSelect) that
    /// the storage gate now routes into the index: the evaluation answers
    /// with exact candidates, and a window-straddling file clamps them to
    /// the query range before ranking.
    #[test]
    fn test_condition_all_simple_select_eval_clamps_to_window() {
        let (reader, _file) = build_select_file("files/org/logs/t/all.vix", 1000); // ts 1000..991
        let condition = IndexCondition {
            conditions: vec![Condition::All()],
        };

        // fully covered window: top-3 DESC = the 3 newest rows
        match evaluate_vix_index(
            "t",
            &reader,
            &condition,
            Some(IndexOptimizeMode::SimpleSelect(3, false)),
            (0, 2000),
            true,
            None,
        )
        .unwrap()
        {
            RawVixResult::SelectCandidates { candidates, .. } => {
                assert_eq!(candidates, vec![(1000, 0), (999, 1), (998, 2)]);
            }
            _ => panic!("expected select candidates"),
        }

        // straddling window [0, 999): ts >= 999 clamped away (exclusive
        // end), top-3 DESC inside the window = 998, 997, 996
        match evaluate_vix_index(
            "t",
            &reader,
            &condition,
            Some(IndexOptimizeMode::SimpleSelect(3, false)),
            (0, 999),
            false,
            None,
        )
        .unwrap()
        {
            RawVixResult::SelectCandidates { candidates, .. } => {
                assert_eq!(candidates, vec![(998, 2), (997, 3), (996, 4)]);
            }
            _ => panic!("expected select candidates"),
        }

        // ascending with a straddling lower bound [995, 2000): the oldest
        // in-window rows win, smallest timestamp first (inclusive start)
        match evaluate_vix_index(
            "t",
            &reader,
            &condition,
            Some(IndexOptimizeMode::SimpleSelect(2, true)),
            (995, 2000),
            false,
            None,
        )
        .unwrap()
        {
            RawVixResult::SelectCandidates { candidates, .. } => {
                assert_eq!(candidates, vec![(995, 5), (996, 4)]);
            }
            _ => panic!("expected select candidates"),
        }
    }

    /// Test-only rendezvous gate: `search_vix_index` calls
    /// [`eval_concurrency_barrier`] at entry; when a test installed a
    /// barrier under its trace-id prefix, every per-file evaluation of that
    /// query waits on it. Keyed by trace id so concurrently running tests
    /// (one process, many test threads) never trip each other's gates.
    static EVAL_GATE: parking_lot::Mutex<Option<(String, Arc<tokio::sync::Barrier>)>> =
        parking_lot::Mutex::new(None);

    pub(super) fn eval_concurrency_barrier(trace_id: &str) -> Option<Arc<tokio::sync::Barrier>> {
        let gate = EVAL_GATE.lock();
        gate.as_ref()
            .filter(|(prefix, _)| trace_id.starts_with(prefix))
            .map(|(_, barrier)| Arc::clone(barrier))
    }

    /// The 200M-benchmark regression guard: per-file index evaluation must
    /// FAN OUT (`ZO_VIX_SEARCH_CONCURRENCY` permits, default 4x cpu). All
    /// files of one query rendezvous on a barrier INSIDE `search_vix_index`
    /// — a sequential per-file loop would deadlock there, which the timeout
    /// turns into a loud failure.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_eval_fan_out_runs_files_concurrently() {
        const FILES: usize = 4;
        let trace_prefix = "vix-fanout-gate-test";
        let barrier = Arc::new(tokio::sync::Barrier::new(FILES));
        *EVAL_GATE.lock() = Some((trace_prefix.to_string(), Arc::clone(&barrier)));
        struct ClearGate;
        impl Drop for ClearGate {
            fn drop(&mut self) {
                *EVAL_GATE.lock() = None;
            }
        }
        let _clear = ClearGate;

        let mut file_list = Vec::with_capacity(FILES);
        for i in 0..FILES {
            let (reader, mut file) =
                build_select_file(&format!("files/org/logs/t/fanout-{i}.vix"), 1000);
            file.meta.compressed_size = 4096;
            reader_cache::GLOBAL_CACHE.put(file.key.clone(), Arc::new(reader));
            file_list.push(file);
        }

        let params = Arc::new(crate::types::QueryParams {
            trace_id: trace_prefix.to_string(),
            org_id: "org".to_string(),
            stream: datafusion::sql::TableReference::from("t"),
            stream_type: StreamType::Logs,
            stream_name: "t".to_string(),
            time_range: (0, 2000),
            work_group: None,
            use_inverted_index: true,
        });
        let mut condition = IndexCondition::new();
        condition.add_condition(Condition::Equal("level".to_string(), "info".to_string()));

        let (_took, add_filter_back, result) = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            vix_search(
                params,
                &mut file_list,
                Some(condition),
                Some(IndexOptimizeMode::SimpleCount),
            ),
        )
        .await
        .expect(
            "per-file evaluation did NOT fan out: the files never met at the barrier \
             (sequential evaluation regression)",
        )
        .unwrap();

        assert!(!add_filter_back);
        assert!(
            file_list.is_empty(),
            "every file must be answered by the index"
        );
        // 4 files x 10 rows, all level=info
        assert!(
            matches!(result, MultiResult::Count(40)),
            "expected Count(40), got {result:?}"
        );
    }
}

/// Cached-vs-ranged parity: the same query battery, evaluated through the
/// production evaluation function over the same synthetic core files, once
/// with the in-memory reader (cached mode) and once with the ranged reader.
#[cfg(test)]
mod ranged_parity_tests {
    use std::ops::Range;

    use arrow::{
        array::{Int64Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use bytes::Bytes;
    use futures::{FutureExt, future::BoxFuture};
    use vortex_index::{VixRangeSource, VixWriter, VixWriterOptions};

    use super::*;
    use crate::index::{Condition, NumericKind};

    /// Plain in-memory range source (ready futures), standing in for the
    /// infra cache ladder.
    struct MemRangeSource(Bytes);

    impl VixRangeSource for MemRangeSource {
        fn len(&self) -> u64 {
            self.0.len() as u64
        }

        fn fetch(&self, range: Range<u64>) -> BoxFuture<'static, anyhow::Result<Bytes>> {
            let out = if range.start <= range.end && range.end <= self.0.len() as u64 {
                Ok(self.0.slice(range.start as usize..range.end as usize))
            } else {
                Err(anyhow::anyhow!("range {range:?} out of bounds"))
            };
            async move { out }.boxed()
        }
    }

    /// One core file, 3000 rows: `svc`/`level` column-store + term fields,
    /// `code` term-only, `http.status` only inside `_source` (with per-row
    /// incompressible padding so the file exceeds the 64 KiB tail window and
    /// the ranged reader really fetches blob windows).
    fn build_parity_file() -> Bytes {
        let rows = 3000usize;
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("svc", DataType::Utf8, true),
            Field::new("level", DataType::Utf8, true),
            Field::new("code", DataType::Int64, true),
        ]));
        let opts = VixWriterOptions {
            fts_field_names: vec!["level".to_string()],
            column_store_field_names: vec!["svc".to_string(), "level".to_string()],
            row_group_size: 512,
            ..Default::default()
        };
        let services = ["api", "auth", "db", "web"];
        let levels = ["info", "warn", "error"];
        let mut lcg = 0x2F2F_1234_5678_9ABCu64;
        let mut pad = || {
            let mut out = String::with_capacity(64);
            for _ in 0..8 {
                lcg = lcg
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                out.push_str(&format!("{:08x}", (lcg >> 24) as u32));
            }
            out
        };
        let ts: Vec<i64> = (0..rows as i64).map(|i| 1_000_000 - i).collect();
        let svc: Vec<Option<&str>> = (0..rows)
            .map(|i| (i % 7 != 3).then(|| services[i % services.len()]))
            .collect();
        let level: Vec<Option<&str>> = (0..rows)
            .map(|i| (i % 5 != 2).then(|| levels[i % levels.len()]))
            .collect();
        let code: Vec<Option<i64>> = (0..rows)
            .map(|i| (i % 3 != 1).then_some(200 + (i % 5) as i64 * 100))
            .collect();
        let sources: Vec<String> = (0..rows)
            .map(|i| {
                let mut parts = vec![format!("\"_timestamp\":{}", ts[i])];
                if let Some(s) = svc[i] {
                    parts.push(format!("\"svc\":\"{s}\""));
                }
                if let Some(l) = level[i] {
                    parts.push(format!("\"level\":\"{l}\""));
                }
                if let Some(c) = code[i] {
                    parts.push(format!("\"code\":{c}"));
                }
                if i % 2 == 0 {
                    parts.push(format!("\"http.status\":\"{}\"", 200 + (i % 4) * 100));
                }
                parts.push(format!("\"payload\":\"{}\"", pad()));
                format!("{{{}}}", parts.join(","))
            })
            .collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ts)),
                Arc::new(StringArray::from(svc)),
                Arc::new(StringArray::from(level)),
                Arc::new(Int64Array::from(code)),
            ],
        )
        .unwrap();
        let mut writer = VixWriter::new(&schema, opts, false);
        writer
            .push_batch_with_source(
                &batch,
                &StringArray::from_iter_values(sources.iter().map(String::as_str)),
                None,
            )
            .unwrap();
        Bytes::from(writer.finish().unwrap())
    }

    /// A deterministic, order-insensitive rendering of one evaluation result.
    fn fingerprint(raw: &RawVixResult) -> String {
        match raw {
            RawVixResult::PartialFields => "partial-fields".to_string(),
            RawVixResult::MissingColumn { field } => format!("missing-column:{field}"),
            RawVixResult::Count { count, has_skipped } => format!("count:{count}:{has_skipped}"),
            RawVixResult::Bitmap {
                bitmap,
                row_count,
                row_group_size,
                has_skipped,
            } => format!(
                "bitmap:{:?}:{row_count}:{row_group_size:?}:{has_skipped}",
                bitmap.set_indices().collect::<Vec<_>>()
            ),
            RawVixResult::SelectCandidates {
                candidates,
                row_count,
                row_group_size,
            } => format!("select:{candidates:?}:{row_count}:{row_group_size:?}"),
            RawVixResult::Histogram {
                histogram,
                has_skipped,
            } => format!("histogram:{histogram:?}:{has_skipped}"),
            RawVixResult::MultiHistogram { rows, has_skipped } => {
                // per-bucket breakdown order is hash-map driven (the merge
                // re-aggregates): compare as a sorted multiset
                let mut sorted = rows.clone();
                sorted.sort();
                format!("multi-histogram:{sorted:?}:{has_skipped}")
            }
            RawVixResult::TopN {
                groups,
                has_skipped,
            } => {
                // tied counts come back in hash-map order (the final
                // aggregate re-sums): compare as a sorted multiset
                let mut sorted = groups.clone();
                sorted.sort();
                format!("topn:{sorted:?}:{has_skipped}")
            }
            RawVixResult::Distinct {
                values,
                has_skipped,
            } => {
                let mut sorted: Vec<&String> = values.iter().collect();
                sorted.sort();
                format!("distinct:{sorted:?}:{has_skipped}")
            }
        }
    }

    fn condition(conditions: Vec<Condition>) -> IndexCondition {
        let mut index_condition = IndexCondition::new();
        for c in conditions {
            index_condition.add_condition(c);
        }
        index_condition
    }

    #[test]
    fn cached_and_ranged_modes_return_identical_results() {
        let data = build_parity_file();
        assert!(
            data.len() > 64 * 1024,
            "parity file must exceed the tail window to exercise ranged blobs"
        );
        // cached mode's reader: complete in-memory bytes
        let mem_reader = VixReader::open(data.clone()).unwrap();
        // decode-path reader: the same bytes with the zone table stripped, so
        // the fast paths take the full-decode path — a third parity leg
        // proving the zone-map path (mem/ranged) agrees with the decode path
        // (old files) end-to-end through `evaluate_vix_index`
        let decode_reader = VixReader::open(Bytes::from(
            vortex_index::test_support::strip_zone_map_property(&data).unwrap(),
        ))
        .unwrap();
        assert!(mem_reader.zone_chunks().is_some());
        assert!(decode_reader.zone_chunks().is_none());
        // ranged mode's reader: range fetches against the same bytes
        let ranged_reader = VixReader::open_ranged(Arc::new(MemRangeSource(data)) as _).unwrap();

        let full_range = (0i64, 2_000_000i64);
        let narrow_range = (995_000i64, 999_000i64);

        let conditions = vec![
            condition(vec![Condition::Equal("svc".into(), "api".into())]),
            condition(vec![Condition::Equal("svc".into(), "nope".into())]),
            condition(vec![Condition::NotEqual("level".into(), "info".into())]),
            condition(vec![Condition::In(
                "level".into(),
                vec!["warn".into(), "error".into()],
                false,
            )]),
            condition(vec![Condition::Regex("svc".into(), "a.*".into())]),
            condition(vec![Condition::MatchAll("error".into())]),
            condition(vec![Condition::MatchAll("err*".into())]),
            condition(vec![Condition::FuzzyMatchAll("erros".into(), 1)]),
            condition(vec![Condition::IsNotNull("http.status".into())]),
            condition(vec![Condition::All()]),
            condition(vec![
                Condition::Equal("svc".into(), "db".into()),
                Condition::Equal("level".into(), "error".into()),
            ]),
            condition(vec![Condition::Or(
                Box::new(Condition::Equal("svc".into(), "web".into())),
                Box::new(Condition::Not(Box::new(Condition::Equal(
                    "level".into(),
                    "info".into(),
                )))),
            )]),
            // string-semantics Equal on the numeric field: numeric values
            // live under TAGGED canonical terms, so the raw-string probe
            // matches only string-stored rows (none here) — served exactly,
            // not skipped (json_get_str maps number-stored rows to NULL)
            condition(vec![
                Condition::Equal("code".into(), "200".into()),
                Condition::Equal("svc".into(), "api".into()),
            ]),
            // numeric-semantics comparisons probe the tagged canonical
            // forms (plus the raw spellings for string-stored drift)
            condition(vec![Condition::NumericCmp(
                "code".into(),
                vec!["200".into()],
                false,
                NumericKind::Int,
            )]),
            condition(vec![Condition::NumericCmp(
                "code".into(),
                vec!["200".into(), "400".into()],
                true,
                NumericKind::Int,
            )]),
            condition(vec![Condition::NumericCmp(
                "code".into(),
                vec!["200".into()],
                false,
                NumericKind::Float,
            )]),
            // references a field NO document carries -> that condition is
            // the exact empty result (VixQuery::Nothing), the AND matches
            // nothing and has_skipped stays false
            condition(vec![
                Condition::Equal("unknown_field".into(), "x".into()),
                Condition::Equal("svc".into(), "api".into()),
            ]),
        ];
        let rules: Vec<Option<IndexOptimizeMode>> = vec![
            None,
            Some(IndexOptimizeMode::SimpleCount),
            Some(IndexOptimizeMode::SimpleSelect(7, false)),
            Some(IndexOptimizeMode::SimpleSelect(7, true)),
            Some(IndexOptimizeMode::SimpleHistogram(990_000, 1_000, 12, 0)),
            // buckets narrower than a docs chunk, plus a ts_offset — stresses
            // the zone-map straddle/decode split and the origin shift
            Some(IndexOptimizeMode::SimpleHistogram(990_017, 100, 120, 17)),
            Some(IndexOptimizeMode::SimpleMultiHistogram(
                990_000,
                1_000_001,
                5_000,
                0,
                "level".into(),
            )),
            // narrow multi-histogram buckets with an offset
            Some(IndexOptimizeMode::SimpleMultiHistogram(
                990_017,
                1_000_018,
                500,
                17,
                "level".into(),
            )),
            Some(IndexOptimizeMode::SimpleTopN(vec!["svc".into()], 3, false)),
            Some(IndexOptimizeMode::SimpleDistinct("level".into(), 10, true)),
            // reads a docs column the file lacks -> MissingColumn parity
            Some(IndexOptimizeMode::SimpleTopN(
                vec!["http.status".into()],
                3,
                false,
            )),
        ];

        let mut cases = 0usize;
        for cond in &conditions {
            for rule in &rules {
                for (range, in_range) in [(full_range, true), (narrow_range, false)] {
                    let cached = evaluate_vix_index(
                        "parity-cached",
                        &mem_reader,
                        cond,
                        rule.clone(),
                        range,
                        in_range,
                        None,
                    );
                    let ranged = evaluate_vix_index(
                        "parity-ranged",
                        &ranged_reader,
                        cond,
                        rule.clone(),
                        range,
                        in_range,
                        None,
                    );
                    let decode = evaluate_vix_index(
                        "parity-decode",
                        &decode_reader,
                        cond,
                        rule.clone(),
                        range,
                        in_range,
                        None,
                    );
                    let render = |result: &anyhow::Result<RawVixResult>| match result {
                        Ok(raw) => fingerprint(raw),
                        Err(e) => format!("error:{e}"),
                    };
                    assert_eq!(
                        render(&cached),
                        render(&ranged),
                        "parity failure for condition {:?} rule {rule:?} range {range:?}",
                        cond.to_query(),
                    );
                    assert_eq!(
                        render(&cached),
                        render(&decode),
                        "zone/decode parity failure for condition {:?} rule {rule:?} range \
                         {range:?}",
                        cond.to_query(),
                    );
                    cases += 1;
                }
            }
        }
        assert_eq!(cases, conditions.len() * rules.len() * 2);

        // a condition referencing ONLY a field no document carries is the
        // exact empty result in both modes (the file gets eliminated, no
        // filter-back) — the ranged reader answers the key-term probe from
        // the same dictionary ranges
        let unknown = condition(vec![Condition::Equal("unknown_field".into(), "x".into())]);
        for reader in [&mem_reader, &ranged_reader] {
            match evaluate_vix_index("p", reader, &unknown, None, full_range, true, None).unwrap() {
                RawVixResult::Bitmap {
                    bitmap,
                    has_skipped,
                    ..
                } => {
                    assert!(!has_skipped, "absent fields are exact, never a skip");
                    assert_eq!(bitmap.count_set_bits(), 0);
                }
                other => panic!("expected an empty bitmap, got {}", fingerprint(&other)),
            }
        }

        // a condition referencing ONLY a carried-but-unservable field
        // (fts-only storage: tokens exist, raw whole values do not) errors
        // identically in both modes (the caller adds the filter back)
        let unservable = condition(vec![Condition::Equal("level".into(), "info".into())]);
        let cached_err =
            evaluate_vix_index("p", &mem_reader, &unservable, None, full_range, true, None)
                .err()
                .expect("all-skipped condition must error")
                .to_string();
        let ranged_err = evaluate_vix_index(
            "p",
            &ranged_reader,
            &unservable,
            None,
            full_range,
            true,
            None,
        )
        .err()
        .expect("all-skipped condition must error")
        .to_string();
        assert_eq!(cached_err, ranged_err);

        // sanity: the battery had real matches, not a wall of empty bitmaps
        let probe = evaluate_vix_index(
            "parity-probe",
            &ranged_reader,
            &condition(vec![Condition::Equal("svc".into(), "api".into())]),
            None,
            full_range,
            true,
            None,
        )
        .unwrap();
        match probe {
            RawVixResult::Bitmap { bitmap, .. } => assert!(bitmap.count_set_bits() > 100),
            other => panic!("expected a bitmap, got {}", fingerprint(&other)),
        }
    }
}

// =====================================================================
// Adversarial review tests — query-evaluation correctness pass.
//
// `review_finding_*` tests PIN currently-wrong behavior (each carries a
// FINDING comment stating the correct answer); they are tripwires that
// must be updated when the underlying bug is fixed. The other `review_*`
// tests VERIFY an attacked area as correct.
// =====================================================================
#[cfg(test)]
mod review_tests {
    use std::sync::Arc;

    use arrow::{
        array::{Int64Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use config::meta::{
        inverted_index::IndexOptimizeMode,
        stream::{FileKey, FileMeta, FileSelection},
    };
    use hashbrown::HashMap;
    use vortex_index::{VixQuery, VixReader, VixWriter, VixWriterOptions};

    use super::{
        MultiResult, RawVixResult, evaluate_vix_index, generate_cache_key,
        pruner::SimpleSelectPruner,
    };
    use crate::index::{Condition, IndexCondition};

    /// One-batch core file with a single structured string field `svc`
    /// (term-indexed, not column-stored).
    fn svc_file(svcs: &[Option<&str>]) -> VixReader {
        let rows = svcs.len();
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("svc", DataType::Utf8, true),
        ]));
        let ts: Vec<i64> = (0..rows as i64).map(|i| 1000 - i).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ts.clone())),
                Arc::new(StringArray::from(svcs.to_vec())),
            ],
        )
        .unwrap();
        let sources: Vec<String> = (0..rows)
            .map(|i| match svcs[i] {
                Some(svc) => format!(
                    r#"{{"_timestamp":{},"svc":{}}}"#,
                    ts[i],
                    serde_json::to_string(svc).unwrap()
                ),
                None => format!(r#"{{"_timestamp":{}}}"#, ts[i]),
            })
            .collect();
        let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
        writer
            .push_batch_with_source(&batch, &StringArray::from(sources), None)
            .unwrap();
        VixReader::open(bytes::Bytes::from(writer.finish().unwrap())).unwrap()
    }

    /// One-batch core file with a full-text field `message` (tokens only).
    fn fts_file(messages: &[&str]) -> VixReader {
        let rows = messages.len();
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("message", DataType::Utf8, true),
        ]));
        let opts = VixWriterOptions {
            fts_field_names: vec!["message".to_string()],
            ..Default::default()
        };
        let ts: Vec<i64> = (0..rows as i64).map(|i| 1000 - i).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ts.clone())),
                Arc::new(StringArray::from(messages.to_vec())),
            ],
        )
        .unwrap();
        let sources: Vec<String> = (0..rows)
            .map(|i| {
                format!(
                    r#"{{"_timestamp":{},"message":{}}}"#,
                    ts[i],
                    serde_json::to_string(messages[i]).unwrap()
                )
            })
            .collect();
        let mut writer = VixWriter::new(&schema, opts, false);
        writer
            .push_batch_with_source(&batch, &StringArray::from(sources), None)
            .unwrap();
        VixReader::open(bytes::Bytes::from(writer.finish().unwrap())).unwrap()
    }

    fn eval_bits(reader: &VixReader, conditions: Vec<Condition>) -> (Vec<usize>, bool) {
        let condition = IndexCondition { conditions };
        match evaluate_vix_index("review", reader, &condition, None, (0, 2000), true, None).unwrap()
        {
            RawVixResult::Bitmap {
                bitmap,
                has_skipped,
                ..
            } => (bitmap.set_indices().collect(), has_skipped),
            _ => panic!("expected a bitmap"),
        }
    }

    /// FIXED (was a finding): `field = ''` now matches from the index —
    /// the writer raw-indexes empty-string values (wave-A writer fix), and
    /// the negation carries the KeyExists non-null guard (fix 5), so
    /// `svc != ''` returns exactly the SQL rows with the filter still
    /// removable.
    #[test]
    fn review_finding_equal_empty_string_drops_rows() {
        let reader = svc_file(&[Some("a"), Some(""), Some("b"), None]);
        // the index's own key terms prove row 1 carries a svc value
        assert!(reader.key_exists("svc").unwrap().value(1));
        assert!(reader.partial_fields().is_empty());

        // svc = '' -> row 1, straight from the index
        let (rows, has_skipped) =
            eval_bits(&reader, vec![Condition::Equal("svc".into(), "".into())]);
        assert_eq!(rows, vec![1]);
        assert!(!has_skipped);

        // svc IN ('', 'a') -> rows 0 and 1
        let (rows, has_skipped) = eval_bits(
            &reader,
            vec![Condition::In(
                "svc".into(),
                vec!["".into(), "a".into()],
                false,
            )],
        );
        assert_eq!(rows, vec![0, 1]);
        assert!(!has_skipped);

        // svc != '' -> exactly the SQL rows {0, 2}: the '' row is excluded
        // by the negation and the null row by the KeyExists guard
        let (rows, has_skipped) =
            eval_bits(&reader, vec![Condition::NotEqual("svc".into(), "".into())]);
        assert_eq!(rows, vec![0, 2]);
        assert!(!has_skipped);
        // ... and the (now correct) index answer may drop the filter
        assert!(Condition::NotEqual("svc".into(), "".into()).can_remove_filter());
    }

    /// FIXED (was a finding inherited from the baseline implementation):
    /// `field != value` / `field NOT IN (..)` no longer match NULL rows.
    /// SQL three-valued logic excludes them (`NULL != 'a'` is NULL);
    /// to_vix_query now conjoins `KeyExists(field)` (key term ⇔ non-null,
    /// exact on core files), so `can_remove_filter() == true` stays sound.
    /// General `NOT(..)` shapes get no guard and keep the scan-side filter
    /// instead (`can_remove_filter() == false`).
    #[test]
    fn review_finding_not_equal_matches_null_rows() {
        let reader = svc_file(&[Some("a"), None, Some("b")]);
        let (rows, has_skipped) =
            eval_bits(&reader, vec![Condition::NotEqual("svc".into(), "a".into())]);
        // SQL answer: {2} — the null row 1 is excluded.
        assert_eq!(rows, vec![2]);
        assert!(!has_skipped);
        assert!(Condition::NotEqual("svc".into(), "a".into()).can_remove_filter());

        // negated IN carries the same guard
        let (rows, has_skipped) = eval_bits(
            &reader,
            vec![Condition::In(
                "svc".into(),
                vec!["a".into(), "c".into()],
                true,
            )],
        );
        assert_eq!(rows, vec![2]);
        assert!(!has_skipped);
        assert!(Condition::In("svc".into(), vec!["a".into()], true).can_remove_filter());

        // a general NOT(..) still inverts the bitmap (null row included),
        // but its filter is NOT removable, so the scan re-check repairs it
        let (rows, _) = eval_bits(
            &reader,
            vec![Condition::Not(Box::new(Condition::Equal(
                "svc".into(),
                "a".into(),
            )))],
        );
        assert_eq!(rows, vec![1, 2]);
        assert!(
            !Condition::Not(Box::new(Condition::Equal("svc".into(), "a".into())))
                .can_remove_filter()
        );
    }

    /// FIXED (was a finding): write side and match_all now share ONE
    /// canonical tokenizer (`vortex_index::o2_tokenize` — ASCII runs
    /// split at non-ASCII boundaries, per-char non-ASCII alphanumerics,
    /// byte-length filter), so match_all agrees with the indexed tokens on
    /// non-ASCII text. Files stamped with the legacy `o2-v1` tokenizer
    /// property re-tokenize at their next compaction; ASCII behavior is
    /// identical either way.
    #[test]
    fn review_finding_match_all_tokenizer_mismatch_on_non_ascii() {
        let reader = fts_file(&[
            "用户admin登录",
            "plain admin login",
            "café latte",
            "size 中 large",
        ]);

        // the index contains the canonical tokens
        let any = |token: &str| VixQuery::TokenAnyField {
            token: token.as_bytes().to_vec(),
        };
        assert_eq!(
            reader
                .eval(&any("admin"))
                .unwrap()
                .set_indices()
                .collect::<Vec<_>>(),
            vec![0, 1],
            "the CJK-glued 'admin' run is indexed as its own token"
        );
        assert_eq!(
            reader
                .eval(&any("caf"))
                .unwrap()
                .set_indices()
                .collect::<Vec<_>>(),
            vec![2]
        );

        // ... and match_all tokenizes with the same function
        assert_eq!(
            super::index_match_all_tokens("café"),
            vec!["caf".to_string(), "é".to_string()]
        );
        assert_eq!(
            super::index_match_all_tokens("用户admin登录"),
            vec![
                "用".to_string(),
                "户".to_string(),
                "admin".to_string(),
                "登".to_string(),
                "录".to_string()
            ]
        );

        // match_all('café'): doc 2 matches (caf AND é)
        let (rows, has_skipped) = eval_bits(&reader, vec![Condition::MatchAll("café".into())]);
        assert_eq!(rows, vec![2]);
        assert!(!has_skipped);

        // match_all('admin'): both the plain and the CJK-glued doc match
        let (rows, has_skipped) = eval_bits(&reader, vec![Condition::MatchAll("admin".into())]);
        assert_eq!(rows, vec![0, 1]);
        assert!(!has_skipped);

        // match_all('用户admin登录'): exactly the doc containing that text
        let (rows, has_skipped) =
            eval_bits(&reader, vec![Condition::MatchAll("用户admin登录".into())]);
        assert_eq!(rows, vec![0]);
        assert!(!has_skipped);

        // a standalone CJK char (3 bytes >= min 2) is indexed AND queryable
        let (rows, has_skipped) = eval_bits(&reader, vec![Condition::MatchAll("中".into())]);
        assert_eq!(rows, vec![3]);
        assert!(!has_skipped);
    }

    /// FIXED (was a finding inherited in shape from the baseline
    /// implementation):
    /// the per-file result-cache key now hashes the condition STRUCT
    /// (derived `Hash`) instead of concatenating display strings, so a
    /// crafted VALUE containing " AND " no longer collides with a
    /// two-condition query on the same file and rule.
    #[test]
    fn review_finding_result_cache_key_not_injective() {
        let file = FileKey {
            key: "files/org/logs/s/1.vix".to_string(),
            meta: FileMeta {
                min_ts: 1,
                max_ts: 10,
                records: 100,
                ..Default::default()
            },
            ..Default::default()
        };
        let rule = Some(IndexOptimizeMode::SimpleCount);

        let crafted = IndexCondition {
            conditions: vec![Condition::Equal("a".into(), "b AND c=d".into())],
        };
        let two = IndexCondition {
            conditions: vec![
                Condition::Equal("a".into(), "b".into()),
                Condition::Equal("c".into(), "d".into()),
            ],
        };
        let key_crafted = generate_cache_key(&crafted, &rule, &file, None);
        let key_two = generate_cache_key(&two, &rule, &file, None);
        assert!(!key_crafted.is_empty());
        assert!(!key_two.is_empty());
        // structurally different queries never share a key
        assert_ne!(key_crafted, key_two);

        // same condition still hits the same key (cache stays useful)
        assert_eq!(
            key_two,
            generate_cache_key(&two.clone(), &rule, &file, None)
        );
        // ...and the key still separates rules and files
        let other_rule = Some(IndexOptimizeMode::SimpleDistinct("a".into(), 10, true));
        assert_ne!(key_two, generate_cache_key(&two, &other_rule, &file, None));
        let other_file = FileKey {
            key: "files/org/logs/s/2.vix".to_string(),
            ..file.clone()
        };
        assert_ne!(key_two, generate_cache_key(&two, &rule, &other_file, None));
    }

    /// Plain row-selection (no optimize rule) caches under the reserved "n"
    /// rule tag: the key is non-empty, stable, and distinct from every
    /// optimize-mode key for the same condition + file.
    #[test]
    fn result_cache_key_covers_plain_row_selection() {
        let file = FileKey {
            key: "files/org/logs/s/1.vix".to_string(),
            meta: FileMeta {
                min_ts: 1,
                max_ts: 10,
                records: 100,
                ..Default::default()
            },
            ..Default::default()
        };
        let condition = IndexCondition {
            conditions: vec![Condition::Equal("a".into(), "b".into())],
        };

        let plain = generate_cache_key(&condition, &None, &file, None);
        assert!(!plain.is_empty());
        assert_eq!(plain, generate_cache_key(&condition, &None, &file, None));
        assert_ne!(
            plain,
            generate_cache_key(
                &condition,
                &Some(IndexOptimizeMode::SimpleCount),
                &file,
                None
            )
        );
        // "n" is reserved: no optimize-mode rule string may collide with it
        for rule in [
            IndexOptimizeMode::SimpleCount,
            IndexOptimizeMode::SimpleSelect(10, false),
            IndexOptimizeMode::SimpleHistogram(0, 60, 10, 0),
            IndexOptimizeMode::SimpleTopN(vec!["f".into()], 10, false),
            IndexOptimizeMode::SimpleDistinct("f".into(), 10, false),
        ] {
            assert_ne!(rule.to_rule_string(), "n");
        }
    }

    /// Straddling files key on the EFFECTIVE time clamp (query range
    /// intersected with the file span): different clamps never collide,
    /// and windows sharing the same overlap share the key.
    #[test]
    fn result_cache_key_pins_time_clamp_for_straddlers() {
        let file = FileKey {
            key: "files/org/logs/s/1.vix".to_string(),
            meta: FileMeta {
                min_ts: 100,
                max_ts: 200,
                records: 100,
                ..Default::default()
            },
            ..Default::default()
        };
        let condition = IndexCondition {
            conditions: vec![Condition::Equal("a".into(), "b".into())],
        };
        let rule = Some(IndexOptimizeMode::SimpleCount);

        let full = generate_cache_key(&condition, &rule, &file, None);
        let clamp_a = generate_cache_key(&condition, &rule, &file, Some((150, 201)));
        let clamp_b = generate_cache_key(&condition, &rule, &file, Some((160, 201)));
        assert_ne!(full, clamp_a);
        assert_ne!(clamp_a, clamp_b);
        // identical effective overlap -> identical key (cross-window reuse)
        assert_eq!(
            clamp_a,
            generate_cache_key(&condition, &rule, &file, Some((150, 201)))
        );
    }

    /// VERIFIED (per-file layer): filtered aggregates over a file that
    /// lacks the docs column. Single-field TopN/Distinct now serve EXACTLY
    /// from the term dictionary (per value, postings ∩ condition bitmap —
    /// the pre-column_store_fields history path, backlog #21);
    /// MultiHistogram still reports MissingColumn (scan fallback). The
    /// MissingColumn probe remains the ONLY old/new-file routing: flight
    /// keeps every MissingColumn/PartialFields/error file in vix_search's
    /// returned list and moves it to the scan branch — degradation per
    /// file, never a hard error and never a partial aggregate. (There is
    /// no global settings stamp; see split_file_list_by_time_range.)
    #[test]
    fn review_filtered_aggregates_on_missing_docs_column_fall_back() {
        let reader = svc_file(&[Some("api"), Some("db"), Some("api")]);
        let condition = IndexCondition {
            conditions: vec![Condition::Equal("svc".into(), "api".into())],
        };
        match evaluate_vix_index(
            "review",
            &reader,
            &condition,
            Some(IndexOptimizeMode::SimpleTopN(
                vec!["svc".to_string()],
                10,
                false,
            )),
            (0, 2000),
            true,
            None,
        )
        .unwrap()
        {
            RawVixResult::TopN {
                groups,
                has_skipped,
            } => {
                assert!(!has_skipped);
                assert_eq!(groups, vec![(vec!["api".to_string()], 2)]);
            }
            _ => panic!("filtered single-field TopN must serve from the dictionary"),
        }
        match evaluate_vix_index(
            "review",
            &reader,
            &condition,
            Some(IndexOptimizeMode::SimpleDistinct(
                "svc".to_string(),
                10,
                true,
            )),
            (0, 2000),
            true,
            None,
        )
        .unwrap()
        {
            RawVixResult::Distinct {
                values,
                has_skipped,
            } => {
                assert!(!has_skipped);
                assert_eq!(values.len(), 1);
                assert!(values.contains("api"));
            }
            _ => panic!("filtered Distinct must serve from the dictionary"),
        }
        // MultiHistogram reads the docs column: still a per-file fallback
        match evaluate_vix_index(
            "review",
            &reader,
            &condition,
            Some(IndexOptimizeMode::SimpleMultiHistogram(
                0,
                2000,
                100,
                0,
                "svc".to_string(),
            )),
            (0, 2000),
            true,
            None,
        )
        .unwrap()
        {
            RawVixResult::MissingColumn { field } => assert_eq!(field, "svc"),
            _ => panic!("expected MissingColumn"),
        }
        // unfiltered single-field TopN over the same file is served
        // index-only (dictionary), never skipped
        let all = IndexCondition {
            conditions: vec![Condition::All()],
        };
        match evaluate_vix_index(
            "review",
            &reader,
            &all,
            Some(IndexOptimizeMode::SimpleTopN(
                vec!["svc".to_string()],
                10,
                false,
            )),
            (0, 2000),
            true,
            None,
        )
        .unwrap()
        {
            RawVixResult::TopN {
                mut groups,
                has_skipped,
            } => {
                groups.sort();
                assert!(!has_skipped);
                assert_eq!(
                    groups,
                    vec![(vec!["api".to_string()], 2), (vec!["db".to_string()], 1),]
                );
            }
            _ => panic!("expected TopN"),
        }
    }

    /// VERIFIED: the SimpleSelect global merge selects exactly `limit`
    /// rows when candidates tie at the cut, and the weakest-winner file
    /// pruning keeps candidate-less files whose range TOUCHES the weakest
    /// winning timestamp (`>=`), so a tied row in an unindexed file is
    /// never wrongly dropped.
    #[test]
    fn review_simple_select_ties_at_the_cut() {
        let mk_file = |key: &str, min_ts: i64, max_ts: i64| FileKey {
            key: key.to_string(),
            meta: FileMeta {
                min_ts,
                max_ts,
                records: 100,
                ..Default::default()
            },
            ..Default::default()
        };
        // desc, limit 2; candidates: a: (100, 99), b: (99) — the two 99s
        // tie at the cut; c has NO candidates but max_ts == 99 (tied)
        // and d sorts strictly below the weakest winner.
        let files = vec![
            mk_file("a", 90, 100),
            mk_file("b", 90, 99),
            mk_file("c", 10, 99),
            mk_file("d", 10, 98),
        ];
        let groups = vec![files.clone()];
        let mut pruner = SimpleSelectPruner::new(2, false, &groups);
        pruner.record_candidates("a".to_string(), Arc::new(vec![(100, 0), (99, 1)]), None);
        pruner.record_candidates("b".to_string(), Arc::new(vec![(99, 7)]), None);
        let mut map: HashMap<String, FileKey> =
            files.into_iter().map(|f| (f.key.clone(), f)).collect();
        pruner.finalize("review", &mut map);

        // exactly 2 winning rows across the candidate files
        let selected: u64 = ["a", "b"]
            .iter()
            .filter_map(|k| map.get(*k))
            .map(|f| match f.selection.as_ref() {
                Some(FileSelection::Rows(bits)) => bits.matched(),
                _ => 0,
            })
            .sum();
        assert_eq!(selected, 2);
        // ts=100 always wins, so file a keeps at least one row
        assert!(map.contains_key("a"));
        // the tied candidate-less file must survive (max_ts == weakest 99)
        assert!(
            map.contains_key("c"),
            "file tying the weakest winner must not be pruned"
        );
        // strictly-older file is safely pruned
        assert!(!map.contains_key("d"));
    }

    fn query_params(trace_id: &str) -> Arc<super::QueryParams> {
        Arc::new(crate::types::QueryParams {
            trace_id: trace_id.to_string(),
            org_id: "org".to_string(),
            stream: datafusion::sql::TableReference::from("t"),
            stream_type: config::meta::stream::StreamType::Logs,
            stream_name: "t".to_string(),
            time_range: (0, 2000),
            work_group: None,
            use_inverted_index: true,
        })
    }

    fn agg_file_key(key: &str, records: i64, compressed_size: i64) -> FileKey {
        FileKey {
            key: key.to_string(),
            meta: FileMeta {
                min_ts: 1000 - records + 1,
                max_ts: 1000,
                records,
                compressed_size,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// Fix 3 degradation, whole vix_search layer: under an aggregate mode a
    /// file that ERRORS twice (its bytes exist nowhere — open fails, the
    /// retry fails again) comes back in the file list for the caller's scan
    /// branch, while an answered file's contribution stays in the result —
    /// correct totals, no hard error, no lost files.
    #[tokio::test(flavor = "multi_thread")]
    async fn review_wave_b_erroring_file_lands_in_scan_branch_with_correct_totals() {
        // GOOD file: its parsed reader is pre-seeded into the reader cache
        // (ranged mode serves it with zero IO)
        let good_key = "files/org/logs/t/2024/01/01/00/wave_b_good_count.vix";
        let good_reader = Arc::new(svc_file(&[Some("api"), Some("db"), Some("api")]));
        crate::vix::reader_cache::GLOBAL_CACHE.put(good_key.to_string(), Arc::clone(&good_reader));
        let good_file = agg_file_key(good_key, 3, 4096);

        // BAD file: not cached anywhere, no such object in storage — every
        // read attempt (including the retry) fails
        let bad_key = "files/org/logs/t/2024/01/01/00/wave_b_missing_object.vix";
        let bad_file = agg_file_key(bad_key, 5, 4096);

        let condition = IndexCondition {
            conditions: vec![Condition::Equal("svc".into(), "api".into())],
        };
        let mut files = vec![good_file, bad_file];
        let (_took, add_filter_back, result) = super::vix_search(
            query_params("wave-b-error-degrade"),
            &mut files,
            Some(condition),
            Some(IndexOptimizeMode::SimpleCount),
        )
        .await
        .expect("per-file errors must degrade, not fail the query");

        // the failed file is BACK for the scan branch, with the filter
        assert!(add_filter_back);
        assert_eq!(
            files.iter().map(|f| f.key.as_str()).collect::<Vec<_>>(),
            vec![bad_key],
            "the twice-erroring file must land in the scan list"
        );
        // ... while the good file's exact count is already in the result
        match result {
            MultiResult::Count(count) => assert_eq!(count, 2, "svc=api matches 2 of 3 rows"),
            other => panic!("expected Count, got {other:?}"),
        }
    }

    /// Fix 3 degradation: an aggregate fast-path result computed with a
    /// SKIPPED condition (here: equality on an fts-only field) would answer
    /// a weaker predicate — the file must be kept for the scan branch
    /// instead of contributing an overcount.
    #[tokio::test(flavor = "multi_thread")]
    async fn review_wave_b_skipped_condition_keeps_file_for_scan_branch() {
        let key = "files/org/logs/t/2024/01/01/00/wave_b_fts_skip.vix";
        let reader = Arc::new(fts_file(&["hello world", "goodbye world"]));
        crate::vix::reader_cache::GLOBAL_CACHE.put(key.to_string(), Arc::clone(&reader));
        let file = agg_file_key(key, 2, 4096);

        // equality on the fts field is skipped per file (tokens only, no
        // raw terms); the remaining conditions still evaluate — but the
        // Count would be an overcount, so the file must go to the scan
        let condition = IndexCondition {
            conditions: vec![
                Condition::Equal("message".into(), "hello world".into()),
                Condition::MatchAll("world".into()),
            ],
        };
        let mut files = vec![file];
        let (_took, add_filter_back, result) = super::vix_search(
            query_params("wave-b-skip-degrade"),
            &mut files,
            Some(condition),
            Some(IndexOptimizeMode::SimpleCount),
        )
        .await
        .unwrap();

        assert!(add_filter_back);
        assert_eq!(
            files.len(),
            1,
            "the file with the skipped condition must be kept for the scan"
        );
        match result {
            MultiResult::Count(count) => {
                assert_eq!(count, 0, "no partial contribution may be recorded")
            }
            other => panic!("expected Count, got {other:?}"),
        }
    }

    /// VERIFIED: partial-range files AND their match bitmap with the
    /// `_timestamp` range, including when the matched term is dense-elided
    /// (all-ones bitmap) — the range must still narrow it.
    #[test]
    fn review_dense_term_intersects_timestamp_range_on_partial_files() {
        // svc constant across all rows -> the value term is dense-elided
        let reader = svc_file(&[Some("k"), Some("k"), Some("k"), Some("k")]);
        // ts values are 1000, 999, 998, 997; query range covers the middle
        let condition = IndexCondition {
            conditions: vec![Condition::Equal("svc".into(), "k".into())],
        };
        match evaluate_vix_index(
            "review",
            &reader,
            &condition,
            None,
            (998, 1000),
            false,
            None,
        )
        .unwrap()
        {
            RawVixResult::Bitmap {
                bitmap,
                has_skipped,
                ..
            } => {
                assert!(!has_skipped);
                // [998, 1000) -> rows with ts 999 (row 1) and 998 (row 2)
                assert_eq!(bitmap.set_indices().collect::<Vec<_>>(), vec![1, 2]);
            }
            _ => panic!("expected a bitmap"),
        }
    }
}

// =====================================================================
// End-to-end differential for the UI-generated histogram shapes (extends
// the zone-map differential battery above): the IndexOptimizeMode is
// extracted from the REAL planned SQL through the full leader pipeline +
// RemoteScan split + flight proto roundtrip + follower rule (the
// follower-fidelity harness), the fast path runs the vix collectors over a
// real `.vix` file with those exact parameters, and the referee is REAL
// DataFusion executing the same SQL over the same rows — wrong bucket
// width/edges (auto-interval drift, origin misalignment) fail here.
// =====================================================================
#[cfg(test)]
mod ui_histogram_differential_tests {
    use std::sync::Arc;

    use arrow::{
        array::{Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray},
        compute::cast,
        datatypes::{DataType, Field, Schema},
    };
    use config::meta::inverted_index::IndexOptimizeMode;
    use datafusion::{datasource::MemTable, prelude::SessionContext};
    use hashbrown::HashSet as HbHashSet;
    use vortex_index::{VixReader, VixWriter, VixWriterOptions};

    use super::{RawVixResult, evaluate_vix_index};
    use crate::{
        datafusion::{
            optimizer::{
                logical_optimizer::rewrite_histogram::RewriteHistogram,
                physical_optimizer::index_optimizer::test_harness::follower_extracted_mode,
            },
            udf::histogram_udf,
        },
        index::{Condition, IndexCondition},
        sql::visitor::histogram_interval::{
            convert_histogram_interval_to_seconds, generate_histogram_interval,
            validate_and_adjust_histogram_interval,
        },
    };

    const BREAKDOWN_FIELD: &str = "kubernetes.namespace.name";

    /// Deterministic rows: timestamps scattered over `[start, end)` (LCG),
    /// breakdown values ns-a/ns-b/ns-c with a null every 11th row; when
    /// `with_out_of_range` is set, extra rows land before `start` and at/after
    /// `end` (the partial-range file case).
    struct Rows {
        ts: Vec<i64>,
        ns: Vec<Option<String>>,
    }

    fn make_rows(time_range: (i64, i64), in_window: usize, with_out_of_range: bool) -> Rows {
        let (start, end) = time_range;
        let span = (end - start) as u64;
        let mut lcg = 0xD1FF_C0DE_5EED_0001u64;
        let mut next = |bound: u64| {
            lcg = lcg
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (lcg >> 16) % bound
        };
        let mut ts: Vec<i64> = (0..in_window).map(|_| start + next(span) as i64).collect();
        if with_out_of_range {
            for _ in 0..40 {
                ts.push(start - 1 - next(span / 4) as i64); // before the window
                ts.push(end + next(span / 4) as i64); // at/after the window
            }
        }
        let values = ["ns-a", "ns-b", "ns-c"];
        let ns: Vec<Option<String>> = (0..ts.len())
            .map(|i| (i % 11 != 7).then(|| values[i % values.len()].to_string()))
            .collect();
        Rows { ts, ns }
    }

    fn rows_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new(BREAKDOWN_FIELD, DataType::Utf8, true),
        ]))
    }

    fn build_vix_file(rows: &Rows) -> VixReader {
        let schema = rows_schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(rows.ts.clone())),
                Arc::new(StringArray::from(rows.ns.clone())),
            ],
        )
        .unwrap();
        let sources: Vec<String> = (0..rows.ts.len())
            .map(|i| match &rows.ns[i] {
                Some(v) => format!(
                    r#"{{"_timestamp":{},"kubernetes.namespace.name":{}}}"#,
                    rows.ts[i],
                    serde_json::to_string(v).unwrap()
                ),
                None => format!(r#"{{"_timestamp":{}}}"#, rows.ts[i]),
            })
            .collect();
        let opts = VixWriterOptions {
            column_store_field_names: vec![BREAKDOWN_FIELD.to_string()],
            row_group_size: 128,
            ..Default::default()
        };
        let mut writer = VixWriter::new(&schema, opts, false);
        writer
            .push_batch_with_source(
                &batch,
                &StringArray::from_iter_values(sources.iter().map(String::as_str)),
                None,
            )
            .unwrap();
        VixReader::open(bytes::Bytes::from(writer.finish().unwrap())).unwrap()
    }

    /// Executes `sql` (the exact generated shape) over the same rows with
    /// REAL DataFusion — RewriteHistogram configured like the leader — and
    /// returns `(bucket µs, Option<breakdown>, count)` rows.
    async fn datafusion_scan(
        rows: &Rows,
        sql: &str,
        time_range: (i64, i64),
        preset_interval_secs: i64,
        with_breakdown: bool,
    ) -> Vec<(i64, Option<String>, u64)> {
        let schema = rows_schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(rows.ts.clone())),
                Arc::new(StringArray::from(rows.ns.clone())),
            ],
        )
        .unwrap();
        let ctx = SessionContext::new();
        let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("t", Arc::new(provider)).unwrap();
        ctx.register_udf(histogram_udf::HISTOGRAM_UDF.clone());
        ctx.add_optimizer_rule(Arc::new(RewriteHistogram::new(
            time_range.0,
            time_range.1,
            preset_interval_secs,
            None,
        )));
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let mut out = Vec::new();
        for batch in &batches {
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("zo_sql_key must be Timestamp(Microsecond)");
            let breakdown = with_breakdown.then(|| {
                cast(batch.column(1), &DataType::Utf8)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .clone()
            });
            let counts = cast(
                batch.column(if with_breakdown { 2 } else { 1 }),
                &DataType::Int64,
            )
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone();
            for i in 0..batch.num_rows() {
                let value = breakdown
                    .as_ref()
                    .and_then(|b| (!b.is_null(i)).then(|| b.value(i).to_string()));
                out.push((keys.value(i), value, counts.value(i) as u64));
            }
        }
        out
    }

    /// The streaming preset for a request: the full-range auto interval in
    /// seconds (the exact chain `process_search_stream_request` pre-computes).
    fn preset_secs(full_range: (i64, i64)) -> i64 {
        let secs =
            convert_histogram_interval_to_seconds(generate_histogram_interval(full_range)).unwrap();
        validate_and_adjust_histogram_interval(secs, full_range)
    }

    fn condition_all() -> IndexCondition {
        IndexCondition {
            conditions: vec![Condition::All()],
        }
    }

    /// One differential case: plan the SQL, extract the mode, run the vix
    /// fast path with the extracted parameters, run the DataFusion scan, and
    /// assert equal groups. `eval_range` is the (sub-)range the follower
    /// evaluates (the streaming partition), `file_in_range` mirrors the
    /// production per-file routing input.
    #[allow(clippy::too_many_arguments)]
    async fn assert_fast_path_matches_scan(
        rows: &Rows,
        reader: &VixReader,
        sql: &str,
        full_range: (i64, i64),
        eval_range: (i64, i64),
        file_in_range: bool,
        with_breakdown: bool,
        context: &str,
    ) {
        let preset = preset_secs(full_range);
        let cs_fields: HbHashSet<String> = HbHashSet::from([BREAKDOWN_FIELD.to_string()]);
        let mode = follower_extracted_mode(sql, rows_schema(), eval_range, preset, cs_fields)
            .await
            .unwrap_or_else(|| panic!("{context}: no mode extracted for {sql}"));

        // fast path with the EXTRACTED parameters
        let raw = evaluate_vix_index(
            "ui-diff",
            reader,
            &condition_all(),
            Some(mode.clone()),
            eval_range,
            file_in_range,
            None,
        )
        .unwrap();

        // scan referee over the same rows
        let scan = datafusion_scan(rows, sql, eval_range, preset, with_breakdown).await;

        match (&mode, raw) {
            (
                IndexOptimizeMode::SimpleHistogram(min_value, bucket_width, num_buckets, _),
                RawVixResult::Histogram {
                    histogram,
                    has_skipped,
                },
            ) => {
                assert!(!has_skipped, "{context}: condition-all never skips");
                assert_eq!(histogram.len(), *num_buckets, "{context}: bucket count");
                let mut fast: Vec<(i64, Option<String>, u64)> = histogram
                    .iter()
                    .enumerate()
                    .filter(|(_, count)| **count > 0)
                    .map(|(i, count)| (min_value + i as i64 * *bucket_width as i64, None, *count))
                    .collect();
                let mut scan = scan;
                fast.sort();
                scan.sort();
                assert_eq!(fast, scan, "{context}: fast path != DataFusion scan");
            }
            (
                IndexOptimizeMode::SimpleMultiHistogram(..),
                RawVixResult::MultiHistogram {
                    rows: groups,
                    has_skipped,
                },
            ) => {
                assert!(!has_skipped, "{context}: condition-all never skips");
                let mut fast: Vec<(i64, Option<String>, u64)> = groups
                    .into_iter()
                    .map(|(bucket, value, count)| (bucket, Some(value), count))
                    .collect();
                // fast-path contract: rows with a null breakdown form no
                // group; the scan keeps a NULL group — assert it covers
                // exactly the in-window null rows, then compare the rest
                let (null_groups, mut scan): (Vec<_>, Vec<_>) =
                    scan.into_iter().partition(|(_, value, _)| value.is_none());
                let expected_null_rows = (0..rows.ts.len())
                    .filter(|&i| {
                        rows.ns[i].is_none()
                            && rows.ts[i] >= eval_range.0
                            && rows.ts[i] < eval_range.1
                    })
                    .count() as u64;
                let null_total: u64 = null_groups.iter().map(|(_, _, count)| count).sum();
                assert_eq!(
                    null_total, expected_null_rows,
                    "{context}: scan's NULL-breakdown group must cover exactly the in-window \
                     null rows (dropped by the fast path per contract)"
                );
                fast.sort();
                scan.sort();
                assert_eq!(fast, scan, "{context}: fast path != DataFusion scan");
            }
            (mode, raw) => panic!(
                "{context}: unexpected mode/result combination: {mode:?} / {}",
                match raw {
                    RawVixResult::MissingColumn { field } => format!("MissingColumn({field})"),
                    _ => "other".to_string(),
                }
            ),
        }
    }

    #[tokio::test]
    async fn ui_histogram_shapes_fast_path_matches_datafusion_scan() {
        let start = 1_757_401_694_060_000i64;
        let minute = 60 * 1_000_000i64;
        // two windows exercising different auto-interval steps
        for (window, expected_tier) in [(10 * minute, "10 second"), (3 * 60 * minute, "1 minute")] {
            let full_range = (start, start + window);
            assert_eq!(generate_histogram_interval(full_range), expected_tier);

            // ---- case A: file fully inside the window, exact generated SQL
            let rows = make_rows(full_range, 600, false);
            let reader = build_vix_file(&rows);
            let plain_sql = crate::sql::histogram::convert_to_histogram_query(
                "SELECT * FROM \"t\"",
                &["t".to_string()],
                false,
                None,
                full_range,
                0,
            )
            .unwrap();
            assert_fast_path_matches_scan(
                &rows,
                &reader,
                &plain_sql,
                full_range,
                full_range,
                true,
                false,
                &format!("plain/in-range/{expected_tier}"),
            )
            .await;
            let breakdown_sql = crate::sql::histogram::convert_to_histogram_query(
                "SELECT * FROM \"t\"",
                &["t".to_string()],
                false,
                Some(BREAKDOWN_FIELD),
                full_range,
                0,
            )
            .unwrap();
            assert_fast_path_matches_scan(
                &rows,
                &reader,
                &breakdown_sql,
                full_range,
                full_range,
                true,
                true,
                &format!("breakdown/in-range/{expected_tier}"),
            )
            .await;

            // ---- case B: file extends beyond the window (partial-range
            // routing), original query carries the window as WHERE — the
            // shape convert_to_histogram_query preserves
            let rows = make_rows(full_range, 600, true);
            let reader = build_vix_file(&rows);
            let original = format!(
                "SELECT * FROM \"t\" WHERE _timestamp >= {} AND _timestamp < {}",
                full_range.0, full_range.1
            );
            for (breakdown, with_breakdown) in [(None, false), (Some(BREAKDOWN_FIELD), true)] {
                let sql = crate::sql::histogram::convert_to_histogram_query(
                    &original,
                    &["t".to_string()],
                    false,
                    breakdown,
                    full_range,
                    0,
                )
                .unwrap();
                assert_fast_path_matches_scan(
                    &rows,
                    &reader,
                    &sql,
                    full_range,
                    full_range,
                    false,
                    with_breakdown,
                    &format!("partial/{with_breakdown}/{expected_tier}"),
                )
                .await;
            }

            // ---- case C: streaming partition sub-ranges — the SAME
            // generated SQL (full-range explicit interval) evaluated over
            // narrower partition windows; bucket widths must stay the
            // full-range width and results must match the scan of the same
            // partition
            if window == 3 * 60 * minute {
                let rows = make_rows(full_range, 600, false);
                let reader = build_vix_file(&rows);
                let partitions = [
                    (start, start + 37 * minute),
                    (start + 37 * minute, start + 120 * minute),
                ];
                for partition in partitions {
                    let original = format!(
                        "SELECT * FROM \"t\" WHERE _timestamp >= {} AND _timestamp < {}",
                        partition.0, partition.1
                    );
                    for (breakdown, with_breakdown) in
                        [(None, false), (Some(BREAKDOWN_FIELD), true)]
                    {
                        let sql = crate::sql::histogram::convert_to_histogram_query(
                            &original,
                            &["t".to_string()],
                            false,
                            breakdown,
                            full_range, // interval resolved from the FULL range
                            0,
                        )
                        .unwrap();
                        // partitions plan with their own (narrower) range
                        let mode = follower_extracted_mode(
                            &sql,
                            rows_schema(),
                            partition,
                            preset_secs(full_range),
                            HbHashSet::from([BREAKDOWN_FIELD.to_string()]),
                        )
                        .await
                        .expect("partition mode");
                        let width = match &mode {
                            IndexOptimizeMode::SimpleHistogram(_, width, ..) => *width,
                            IndexOptimizeMode::SimpleMultiHistogram(_, _, width, ..) => *width,
                            other => panic!("unexpected mode {other:?}"),
                        };
                        assert_eq!(
                            width, 60_000_000,
                            "partition {partition:?} must keep the full-range bucket width"
                        );
                        assert_fast_path_matches_scan(
                            &rows,
                            &reader,
                            &sql,
                            full_range,
                            partition,
                            false,
                            with_breakdown,
                            &format!("partition/{partition:?}/{with_breakdown}"),
                        )
                        .await;
                    }
                }
            }
        }
    }
}
