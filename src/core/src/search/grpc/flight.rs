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

use ::datafusion::{
    common::tree_node::TreeNode,
    datasource::TableProvider,
    logical_expr::Operator,
    physical_expr::{
        PhysicalExpr,
        expressions::{BinaryExpr, Column, Literal},
    },
    physical_plan::{
        ExecutionPlan,
        filter::FilterExec,
        limit::{GlobalLimitExec, LocalLimitExec},
        projection::ProjectionExec,
    },
    prelude::SessionContext,
    scalar::ScalarValue,
};
use arrow_schema::{DataType, Schema};
use config::{
    ID_COL_NAME, ORIGINAL_DATA_COL_NAME, TIMESTAMP_COL_NAME,
    cluster::LOCAL_NODE,
    datafusion::request::FlightSearchRequest,
    get_config,
    meta::{
        inverted_index::IndexOptimizeMode,
        search::ScanStats,
        sql::TableReferenceExt,
        stream::{FileKey, StreamType},
    },
};
use datafusion::{
    common::TableReference,
    physical_optimizer::{
        PhysicalOptimizerRule, filter_pushdown::FilterPushdown, limit_pushdown::LimitPushdown,
        projection_pushdown::ProjectionPushdown,
    },
};
use datafusion_proto::bytes::physical_plan_from_bytes_with_extension_codec;
use hashbrown::{HashMap, HashSet};
use infra::{
    errors::{Error, ErrorCodes},
    schema::{
        get_stream_setting_bloom_filter_fields, get_stream_setting_fts_fields,
        unwrap_stream_settings,
    },
};
use itertools::Itertools;
#[cfg(feature = "enterprise")]
use o2_enterprise::enterprise::search::sampling::execution::apply_sampling_to_files;
use parking_lot::Mutex;
use rayon::slice::ParallelSliceMut;

use crate::service::{
    db,
    search::{
        datafusion::{
            distributed_plan::{
                NewEmptyExecVisitor, ReplaceTableScanExec, codec::get_physical_extension_codec,
                rewrite::aggregate_optimize_rewrite,
            },
            exec::{DataFusionContextBuilder, register_udf},
            optimizer::physical_optimizer::{
                index::IndexRule, index_optimizer::FollowerIndexOptimizerRule,
                rewrite_match::RewriteMatchPhysical,
            },
            table_provider::{enrich_table::EnrichTable, uniontable::NewUnionTable},
        },
        grpc::QueryParams,
        index::{Condition, IndexCondition, numeric_kind_of},
        inspector::{SearchInspectorFieldsBuilder, search_inspector_fields},
        match_file,
        vix::MultiResult,
    },
};

/// Follower search: the session, the physical plan, the scan stats, and —
/// when `ZO_STORAGE_SCAN_MAX_BYTES` truncated the storage scan branch — the
/// partial-results message the caller must ship to the leader before the
/// first batch.
#[tracing::instrument(name = "service:search:grpc:flight:do_get::search", skip_all, fields(org_id = req.query_identifier.org_id))]
pub async fn search(
    trace_id: &str,
    req: &FlightSearchRequest,
) -> Result<
    (
        SessionContext,
        Arc<dyn ExecutionPlan>,
        ScanStats,
        Option<String>,
    ),
    Error,
> {
    let cfg = get_config();

    let org_id = req.query_identifier.org_id.to_string();
    let stream_type = StreamType::from(req.query_identifier.stream_type.as_str());
    let work_group = req.super_cluster_info.work_group.clone();

    let trace_id = Arc::new(trace_id.to_string());
    log::info!("[trace_id {trace_id}] flight->search: start");

    // Setup registrations also outlive the branches while the union is planned.
    // Transfer cleanup to the executor only after the complete plan succeeds.
    let mut cleanup = QueryFilesCleanup(Some(&trace_id));

    // create datafusion context, just used for decode plan, the params can use default
    let mut ctx = DataFusionContextBuilder::new()
        .trace_id(&trace_id)
        .work_group(work_group.clone())
        .build(cfg.limit.cpu_num)
        .await?;

    // register udf
    register_udf(&ctx, &org_id)?;
    datafusion_functions_json::register_all(&mut ctx)?;

    // Decode physical plan from bytes
    let proto = get_physical_extension_codec();
    let physical_plan = physical_plan_from_bytes_with_extension_codec(
        &req.search_info.plan,
        &ctx.task_ctx(),
        &proto,
    )?;

    // replace empty table to real table
    let mut visitor = NewEmptyExecVisitor::default();
    if physical_plan.visit(&mut visitor).is_err() || !visitor.has_empty_exec() {
        return Err(Error::Message(
            "flight->search: physical plan visit error: there is no EmptyTable".to_string(),
        ));
    }
    let empty_exec = visitor.plan();

    // here need reset the option because when init ctx we don't know this information
    if empty_exec.sorted_by_time() {
        ctx.state_ref().write().config_mut().options_mut().set(
            "datafusion.execution.split_file_groups_by_statistics",
            "true",
        )?;
    }

    // get stream name
    let stream = TableReference::from(empty_exec.name());
    let stream_name = stream.stream_name().to_string();
    let stream_type = stream.get_stream_type(stream_type);

    // check if we are allowed to search
    if db::compact::retention::is_deleting_stream(&org_id, stream_type, &stream_name, None) {
        return Err(Error::ErrorCode(ErrorCodes::SearchStreamNotFound(format!(
            "stream [{stream_name}] is being deleted"
        ))));
    }

    log::info!(
        "[trace_id {trace_id}] flight->search: part_id: {}, stream: {org_id}/{stream_type}/{stream_name}",
        req.query_identifier.partition
    );

    // construct latest schema map
    let latest_schema = empty_exec.full_schema();
    let mut latest_schema_map = HashMap::with_capacity(latest_schema.fields().len());
    for field in latest_schema.fields() {
        latest_schema_map.insert(field.name(), field);
    }

    let db_schema = infra::schema::get(&org_id, &stream_name, stream_type)
        .await
        .unwrap_or(arrow_schema::Schema::empty());
    let stream_settings = unwrap_stream_settings(&db_schema);
    let mut fst_fields = get_stream_setting_fts_fields(&stream_settings)
        .into_iter()
        .filter(|v| latest_schema_map.contains_key(v))
        .collect_vec();
    fst_fields.sort_unstable();
    fst_fields.dedup();
    // the vix index term-indexes every string field's raw values and every
    // numeric/bool field's canonical value forms: filter/condition
    // extraction (IndexRule) is eligible for all of them (minus the internal
    // columns), carrying the registry type so the rule can gate predicate
    // shapes per type (string fields keep every shape; numeric/bool fields
    // serve =/!=/IN/IS NOT NULL with value-normalized literals). Even for
    // files written before numeric value terms existed, eligibility alone
    // pays: the per-file capability probe classifies files without the key
    // as Absent (eliminated — no IO) and files with it as Unservable (skip
    // + filter-back) instead of bypassing the index entirely. Float16 is
    // excluded: ingest never produces it and the filter-back literal
    // reconstruction does not support it.
    let index_fields: HashMap<String, DataType> = latest_schema
        .fields()
        .iter()
        .filter(|f| {
            matches!(
                f.data_type(),
                DataType::Utf8
                    | DataType::LargeUtf8
                    | DataType::Utf8View
                    | DataType::Boolean
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
                    | DataType::Float32
                    | DataType::Float64
            ) && f.name() != TIMESTAMP_COL_NAME
                && f.name() != ID_COL_NAME
                && f.name() != ORIGINAL_DATA_COL_NAME
                // a row-store star plan carries the `_source` column itself;
                // it is a raw record image, never a term-indexed field
                && f.name() != vortex_index::SOURCE_COL_NAME
        })
        .map(|f| (f.name().clone(), f.data_type().clone()))
        .collect();
    // fast-path eligibility: v2 all-present-columns (DESIGN §2) makes every
    // schema field a docs column of the files that carry it — the per-file
    // capability probes remain the correctness backstop, so the plan-level
    // eligibility set is simply the schema's fields
    let column_store_fields: HashSet<String> = latest_schema_map
        .keys()
        .map(|name| name.to_string())
        .collect();
    let bloom_indexed_fields = get_stream_setting_bloom_filter_fields(&stream_settings)
        .into_iter()
        .filter(|v| latest_schema_map.contains_key(v))
        .collect_vec();
    // construct partition filters
    let search_partition_keys: Vec<(String, String)> = req
        .index_info
        .equal_keys
        .iter()
        .filter_map(|v| {
            latest_schema_map
                .contains_key(&v.key)
                .then_some((v.key.to_string(), v.value.to_string()))
        })
        .collect::<Vec<_>>();

    // get all tables
    let file_stats_cache = ctx.runtime_env().cache_manager.get_file_statistic_cache();

    // optimize physical plan, currently for the vix index optimize
    let index_optimize_mode = req.index_info.index_optimize_mode.clone();
    let index_condition_ref = Arc::new(Mutex::new(None));
    // a malformed request (oneof mode not set) is the PEER's error: reject
    // it instead of panicking the process
    let index_optimize_mode = index_optimize_mode
        .map(IndexOptimizeMode::try_from)
        .transpose()
        .map_err(|e| {
            Error::ErrorCode(ErrorCodes::SearchSQLNotValid(format!(
                "invalid index optimize mode in flight request: {e}"
            )))
        })?;
    let index_optimizer_rule_ref = Arc::new(Mutex::new(index_optimize_mode));
    let mut physical_plan = optimizer_physical_plan(
        physical_plan,
        &ctx,
        &latest_schema,
        (req.search_info.start_time, req.search_info.end_time),
        fst_fields.clone(),
        index_fields,
        column_store_fields.clone(),
        index_condition_ref.clone(),
        index_optimizer_rule_ref.clone(),
    )?;
    let has_noncanonical_filter = plan_has_noncanonical_filter(
        &physical_plan,
        (req.search_info.start_time, req.search_info.end_time),
    );
    let has_row_dropping_limit = plan_has_row_dropping_limit(&physical_plan);
    let has_transformed_timestamp = plan_transforms_timestamp(&physical_plan);
    let index_condition = { index_condition_ref.lock().clone() };
    let idx_optimize_rule = { index_optimizer_rule_ref.lock().clone() };
    // A WHERE-less aggregation (histogram/count over everything) derives no
    // index condition, so the index fast path never engaged and every file
    // was scanned (23s current-hour histograms over thousands of small
    // files). The eval handles Condition::All natively — all-match bitmap,
    // zone-fold, per-file result cache — so synthesize it whenever an
    // optimize rule exists without a condition.
    let index_condition = match (index_condition, &idx_optimize_rule) {
        (None, Some(_)) => Some(IndexCondition {
            conditions: vec![Condition::All()],
        }),
        (condition, _) => condition,
    };
    let use_metadata_count = can_use_metadata_count(&idx_optimize_rule, index_condition.as_ref());

    let query_params = Arc::new(QueryParams {
        trace_id: trace_id.to_string(),
        org_id: org_id.clone(),
        stream: stream.clone(),
        stream_type,
        stream_name: stream_name.to_string(),
        time_range: (req.search_info.start_time, req.search_info.end_time),
        work_group: work_group.clone(),
        full_text_fields: Some(fst_fields.clone()),
        // Stream types in ZO_VIX_INDEX_DISABLED_STREAM_TYPES (#40, metrics
        // by default) never probe the index: their core files are
        // column-store only, so every query routes straight to the columnar
        // scan (the extracted condition is re-applied there — see
        // storage::search's add-filter-back handling for the skipped step).
        use_inverted_index: index_condition.is_some()
            && cfg.common.inverted_index_enabled
            && !config::is_vix_index_disabled(stream_type)
            && (!index_condition.as_ref().unwrap().is_condition_all()
                || idx_optimize_rule.is_some()),
    });

    log::info!(
        "[trace_id {trace_id}] flight->search: use_inverted_index: {}, index_condition: {index_condition:?}, index_optimizer_rule: {idx_optimize_rule:?}",
        query_params.use_inverted_index
    );

    // Negative ids in the ticket are segment-WAL pseudo-files (a leader
    // running ZO_INGEST_SEGMENT_MODE); they resolve against wal_segments,
    // not file_list. Split UNCONDITIONALLY — gating this on the local flag
    // would silently drop assigned segments during a mixed-flag rollout.
    let (parquet_file_ids, segment_ids) =
        super::segments_scan::split_pseudo_ids(&req.search_info.file_id_list);

    // Both branches use only the ticket's frozen id sets above. Neither may
    // re-query a time window while the builder moves segments into L0 files.
    // Own results inside the futures, including completed tables. A sibling
    // failure must release those tables before clearing their registrations.
    let persisted_setup = async {
        let mut tables = Vec::new();
        let mut scan_stats = ScanStats::new();
        let mut metadata_count_file_list = Vec::new();
        let mut index_file_list = Vec::new();
        let mut index_result = None;
        let mut storage_shortfall = None;
        if !parquet_file_ids.is_empty() {
            let (mut file_list, file_list_took) = get_file_list_by_ids(
                &trace_id,
                &org_id,
                stream_type,
                &stream_name,
                Some(query_params.time_range),
                &search_partition_keys,
                &parquet_file_ids,
            )
            .await?;
            let refreshed = refresh_cached_bloom_vers(
                &mut file_list,
                &req.search_info.file_id_list,
                &req.search_info.file_bloom_ver_list,
            );
            if refreshed > 0 {
                log::info!(
                    "[trace_id {trace_id}] flight->search in: part_id: {}, refreshed stale cached \
                     bloom_ver on {refreshed} of {} files from the leader's file list",
                    req.query_identifier.partition,
                    file_list.len(),
                );
            }
            log::info!(
                "{}",
                search_inspector_fields(
                    format!(
                        "[trace_id {trace_id}] flight->search in: part_id: {}, get file_list by ids, files: {}, took: {file_list_took} ms",
                        req.query_identifier.partition,
                        file_list.len(),
                    ),
                    SearchInspectorFieldsBuilder::new()
                        .trace_id(trace_id.to_string())
                        .node_name(LOCAL_NODE.name.clone())
                        .component("flight:do_get::search get file_list by ids".to_string())
                        .search_role("follower".to_string())
                        .duration(file_list_took)
                        .build()
                )
            );

            // Prune the hydrated persisted snapshot once, before any metadata,
            // eager aggregate, or residual storage branch takes ownership.
            if query_params.use_inverted_index
                && !index_condition
                    .as_ref()
                    .is_some_and(IndexCondition::is_condition_all)
            {
                let before = file_list.len();
                let (bloom_took, applied) = super::storage::check_bloom_filter(
                    query_params.clone(),
                    &mut file_list,
                    index_condition.as_ref(),
                    bloom_indexed_fields,
                )
                .await?;
                if applied {
                    log::info!(
                        "{}",
                        search_inspector_fields(
                            format!(
                                "[trace_id {trace_id}] search->bloom: stream {org_id}/{stream_type}/{stream_name}, bloom filter reduced file_list from {before} to {} in {bloom_took} ms",
                                file_list.len()
                            ),
                            SearchInspectorFieldsBuilder::new()
                                .trace_id(trace_id.to_string())
                                .node_name(LOCAL_NODE.name.clone())
                                .component("flight persisted bloom filter".to_string())
                                .search_role("follower".to_string())
                                .duration(bloom_took)
                                .build()
                        )
                    );
                }
            }

            if use_metadata_count {
                let (metadata_files, scan_files) =
                    split_metadata_count_files(file_list, query_params.time_range);
                if !metadata_files.is_empty() {
                    log::info!(
                        "[trace_id {trace_id}] flight->search: metadata count files: {}, remaining storage files: {}",
                        metadata_files.len(),
                        scan_files.len()
                    );
                }
                metadata_count_file_list.extend(metadata_files);
                file_list = scan_files;
            }

            let index_optimize_start = std::time::Instant::now();
            let mut storage_idx_optimize_rule = idx_optimize_rule.clone();
            // Index-off stream types (#40) never route files to the index-only
            // aggregate fast paths: their files carry no term index (and old
            // indexed files must not be probed against policy either) — the
            // whole list stays on the scan branch.
            (index_file_list, file_list) = if config::is_vix_index_disabled(stream_type) {
                (Vec::new(), file_list)
            } else {
                handle_index_optimize(
                    &mut storage_idx_optimize_rule,
                    file_list,
                    query_params.time_range,
                    &column_store_fields,
                    index_condition.as_ref(),
                )
                .await?
            };

            // Evaluate the aggregate fast path EAGERLY over the index files.
            // vix_search leaves every file it could NOT answer (missing docs
            // column, partial fields, IO errors after one retry, per-file
            // skipped conditions) in the list: those move to the DataFusion
            // scan branch below, where storage::search re-applies the filter —
            // per-file degradation instead of failing the whole query. Every
            // file is answered exactly once (index result XOR scan), so partial
            // aggregates remain impossible; IndexOptimizeExec later just adapts
            // this precomputed result.
            if !index_file_list.is_empty() {
                match idx_optimize_rule.clone() {
                    Some(agg_mode) => {
                        let all_index_files = index_file_list.clone();
                        let (idx_took, _add_filter_back, result) = super::storage::vix_search(
                            query_params.clone(),
                            &mut index_file_list,
                            index_condition.clone(),
                            Some(agg_mode),
                        )
                        .await?;
                        scan_stats.idx_took = std::cmp::max(scan_stats.idx_took, idx_took as i64);
                        if !index_file_list.is_empty() {
                            log::warn!(
                                "[trace_id {trace_id}] flight->search: {} of {} index files could not be answered by the aggregate fast path, moving them to the scan branch",
                                index_file_list.len(),
                                all_index_files.len(),
                            );
                            let unanswered: HashSet<String> =
                                index_file_list.iter().map(|f| f.key.clone()).collect();
                            file_list.append(&mut index_file_list);
                            // keep only the ANSWERED files on the index side
                            // (plan display + scan stats)
                            index_file_list = all_index_files
                                .into_iter()
                                .filter(|f| !unanswered.contains(&f.key))
                                .collect();
                        } else {
                            index_file_list = all_index_files;
                        }
                        index_result = Some(result);
                    }
                    None => {
                        // defensive: routing produced index files without a mode
                        // — scan them all instead
                        log::warn!(
                            "[trace_id {trace_id}] flight->search: index files routed without an optimize mode, moving {} files to the scan branch",
                            index_file_list.len(),
                        );
                        file_list.append(&mut index_file_list);
                    }
                }
            }
            log::info!(
                "{}",
                search_inspector_fields(
                    format!(
                        "[trace_id {trace_id}] flight->search: handle index optimize, index files answered: {}, datafusion files: {}",
                        index_file_list.len(),
                        file_list.len()
                    ),
                    SearchInspectorFieldsBuilder::new()
                        .trace_id(trace_id.to_string())
                        .node_name(LOCAL_NODE.name.clone())
                        .component("flight:do_get::search handle index optimize".to_string())
                        .search_role("follower".to_string())
                        .duration(index_optimize_start.elapsed().as_millis() as usize)
                        .build()
                )
            );

            // Apply sampling if configured (enterprise feature)
            #[cfg(feature = "enterprise")]
            if let Some(sampling_config) = &req.search_info.sampling_config {
                apply_sampling_to_files(
                    &mut file_list,
                    sampling_config,
                    Some(query_params.time_range),
                    req.search_info.histogram_interval,
                    &trace_id,
                )
                .await;
            }

            // An unsorted ALL histogram neither orders nor prunes residual files
            // from footer statistics. Avoid reopening every fallback object
            // during planning solely to rediscover bounds already in FileKey.
            let collect_file_stats = !(index_condition
                .as_ref()
                .is_some_and(IndexCondition::is_condition_all)
                && matches!(
                    &idx_optimize_rule,
                    Some(IndexOptimizeMode::SimpleHistogram(..))
                ));
            let storage_search_start = std::time::Instant::now();
            let (tbls, stats, shortfall) = match super::storage::search(
                query_params.clone(),
                latest_schema.clone(),
                &file_list,
                empty_exec.sorted_by_time(),
                collect_file_stats,
                file_stats_cache.clone(),
                index_condition.clone(),
                fst_fields.clone(),
                storage_idx_optimize_rule,
            )
            .await
            {
                Ok(v) => v,
                Err(e) => {
                    log::error!(
                        "[trace_id {trace_id}] flight->search: search storage parquet error: {e}"
                    );
                    return Err(e);
                }
            };
            log::info!(
                "{}",
                search_inspector_fields(
                    format!(
                        "[trace_id {trace_id}] flight->search: storage search completed, {} files",
                        file_list.len()
                    ),
                    SearchInspectorFieldsBuilder::new()
                        .trace_id(trace_id.to_string())
                        .node_name(LOCAL_NODE.name.clone())
                        .component("flight:do_get::search storage search".to_string())
                        .search_role("follower".to_string())
                        .duration(storage_search_start.elapsed().as_millis() as usize)
                        .build()
                )
            );
            tables.extend(tbls);
            scan_stats.add(&stats);
            storage_shortfall = shortfall.map(|s| s.message());
        }
        Ok::<_, Error>(PersistedSetup {
            tables,
            scan_stats,
            metadata_count_file_list,
            index_file_list,
            index_result,
            storage_shortfall,
        })
    };
    let segment_setup = async {
        let mut segment_tables = Vec::new();
        let mut segment_stats = ScanStats::new();
        let mut segment_histogram = None;
        // Scan assigned segment-WAL objects (negative ticket ids). Errors MUST
        // fail the query: a silently missing segment is silent partial data.
        if !segment_ids.is_empty() {
            let segments_scan_start = std::time::Instant::now();
            let all_histogram = match (&idx_optimize_rule, index_condition.as_ref()) {
                (
                    Some(IndexOptimizeMode::SimpleHistogram(
                        min_value,
                        bucket_width,
                        num_buckets,
                        ts_offset,
                    )),
                    Some(condition),
                ) if condition.is_condition_all() && !has_noncanonical_filter => {
                    Some((*min_value, *bucket_width, *num_buckets, *ts_offset))
                }
                _ => None,
            };

            if let Some((min_value, bucket_width, num_buckets, ts_offset)) = all_histogram {
                let (histogram, stats) = match super::segments_scan::search_histogram(
                    query_params.clone(),
                    &segment_ids,
                    min_value,
                    bucket_width,
                    num_buckets,
                    ts_offset,
                )
                .await
                {
                    Ok(v) => v,
                    Err(e) => {
                        log::error!(
                            "[trace_id {trace_id}] flight->search: segment histogram failed: {e}"
                        );
                        return Err(e);
                    }
                };
                segment_histogram = Some(histogram);
                segment_stats.add(&stats);
                log::info!(
                    "{}",
                    search_inspector_fields(
                        format!(
                            "[trace_id {trace_id}] flight->search: segment histogram completed, {} segments",
                            segment_ids.len()
                        ),
                        SearchInspectorFieldsBuilder::new()
                            .trace_id(trace_id.to_string())
                            .node_name(LOCAL_NODE.name.clone())
                            .component("flight:do_get::search segment histogram".to_string())
                            .search_role("follower".to_string())
                            .duration(segments_scan_start.elapsed().as_millis() as usize)
                            .build()
                    )
                );
            } else {
                let segment_top_n = segment_top_n_plan(
                    &idx_optimize_rule,
                    empty_exec.sorted_by_time(),
                    has_noncanonical_filter,
                    has_row_dropping_limit,
                    has_transformed_timestamp,
                    empty_exec.limit(),
                );
                let (tbls, stats) = match super::segments_scan::search(
                    query_params.clone(),
                    latest_schema.clone(),
                    empty_exec.schema().clone(),
                    &segment_ids,
                    empty_exec.sorted_by_time(),
                    segment_top_n,
                    index_condition.clone(),
                    fst_fields.clone(),
                )
                .await
                {
                    Ok(v) => v,
                    Err(e) => {
                        log::error!(
                            "[trace_id {trace_id}] flight->search: search segments error: {e}"
                        );
                        return Err(e);
                    }
                };
                segment_tables.extend(tbls);
                segment_stats.add(&stats);
                log::info!(
                    "{}",
                    search_inspector_fields(
                        format!(
                            "[trace_id {trace_id}] flight->search: segments scan completed, {} segments",
                            segment_ids.len()
                        ),
                        SearchInspectorFieldsBuilder::new()
                            .trace_id(trace_id.to_string())
                            .node_name(LOCAL_NODE.name.clone())
                            .component("flight:do_get::search segments scan".to_string())
                            .search_role("follower".to_string())
                            .duration(segments_scan_start.elapsed().as_millis() as usize)
                            .build()
                    )
                );
            }
        }
        Ok::<_, Error>(SegmentSetup {
            tables: segment_tables,
            scan_stats: segment_stats,
            histogram: segment_histogram,
        })
    };
    let (persisted, segments) = run_setup_branches(
        &trace_id,
        cfg.common.search_parallel_segment_setup,
        persisted_setup,
        segment_setup,
    )
    .await?;
    let PersistedSetup {
        mut tables,
        mut scan_stats,
        metadata_count_file_list,
        index_file_list,
        index_result,
        storage_shortfall,
    } = persisted.with_segments(segments)?;

    // search in WAL memory first to capture the snapshot_time
    // IMPORTANT: WAL data is NEVER sampled - it's always returned in full
    // Sampling only applies to parquet files (applied above in file_list processing)
    let mut memtable_ids = HashSet::new();
    if LOCAL_NODE.is_ingester() {
        let (tbls, stats, ids) = match super::wal::search_memtable(
            query_params.clone(),
            latest_schema.clone(),
            &search_partition_keys,
            empty_exec.sorted_by_time(),
            index_condition.clone(),
            fst_fields.clone(),
        )
        .await
        {
            Ok(v) => v,
            Err(e) => {
                log::error!(
                    "[trace_id {trace_id}] flight->search: search wal memtable error: {e:?}"
                );
                return Err(e);
            }
        };
        memtable_ids.extend(ids);
        tables.extend(tbls);
        scan_stats.add(&stats);
    }

    // Now search in WAL parquet with snapshot_time filter
    if LOCAL_NODE.is_ingester() {
        let (tbls, stats, _) = match super::wal::search_parquet(
            query_params.clone(),
            latest_schema.clone(),
            &search_partition_keys,
            empty_exec.sorted_by_time(),
            file_stats_cache.clone(),
            index_condition.clone(),
            fst_fields.clone(),
            memtable_ids,
        )
        .await
        {
            Ok(v) => v,
            Err(e) => {
                log::error!("[trace_id {trace_id}] flight->search: search wal parquet error: {e}");
                return Err(e);
            }
        };
        tables.extend(tbls);
        scan_stats.add(&stats);
    }

    // due to we rewrite empty exec in rewrite match_all
    let mut visitor = NewEmptyExecVisitor::default();
    if physical_plan.visit(&mut visitor).is_err() || !visitor.has_empty_exec() {
        return Err(Error::Message(
            "flight->search: physical plan visit error: there is no EmptyTable".to_string(),
        ));
    }
    let empty_exec = visitor.plan();

    // if the stream type is enrichment tables and the enrich mode is true, we need to load
    // enrichment data from db to datafusion tables
    if stream_type == StreamType::EnrichmentTables && req.query_identifier.enrich_mode {
        // get the enrichment table from db
        let enrichment_table = EnrichTable::new(
            &org_id,
            &stream,
            empty_exec.full_schema().clone(),
            query_params.time_range,
        );
        // add the enrichment table to the tables
        tables.push(Arc::new(enrichment_table) as _);
    }

    // create a Union Plan to merge all tables
    let start = std::time::Instant::now();
    let union_table = Arc::new(NewUnionTable::new(empty_exec.schema().clone(), tables));
    log::info!(
        "{}",
        search_inspector_fields(
            format!("[trace_id {trace_id}] flight->search: created union table"),
            SearchInspectorFieldsBuilder::new()
                .trace_id(trace_id.to_string())
                .node_name(LOCAL_NODE.name.clone())
                .component("flight:do_get::search union table creation".to_string())
                .search_role("follower".to_string())
                .duration(start.elapsed().as_millis() as usize)
                .build()
        )
    );

    let scan_start = std::time::Instant::now();
    let union_exec = union_table
        .scan(
            &ctx.state(),
            empty_exec.projection(),
            empty_exec.filters(),
            empty_exec.limit(),
        )
        .await?;
    log::info!(
        "{}",
        search_inspector_fields(
            format!("[trace_id {trace_id}] flight->search: union table scan"),
            SearchInspectorFieldsBuilder::new()
                .trace_id(trace_id.to_string())
                .node_name(LOCAL_NODE.name.clone())
                .component("flight:do_get::search union table scan".to_string())
                .search_role("follower".to_string())
                .duration(scan_start.elapsed().as_millis() as usize)
                .build()
        )
    );

    let rewrite_start = std::time::Instant::now();
    let mut rewriter = ReplaceTableScanExec::new(union_exec);
    physical_plan = physical_plan.rewrite(&mut rewriter)?.data;
    log::info!(
        "{}",
        search_inspector_fields(
            format!("[trace_id {trace_id}] flight->search: physical plan rewrite"),
            SearchInspectorFieldsBuilder::new()
                .trace_id(trace_id.to_string())
                .node_name(LOCAL_NODE.name.clone())
                .component("flight:do_get::search physical plan rewrite".to_string())
                .search_role("follower".to_string())
                .duration(rewrite_start.elapsed().as_millis() as usize)
                .build()
        )
    );

    physical_plan = apply_pushdowns_and_optimizations(
        &trace_id,
        &ctx,
        physical_plan,
        &mut scan_stats,
        query_params.clone(),
        metadata_count_file_list,
        index_file_list,
        index_result,
        idx_optimize_rule,
    )?;

    log::info!(
        "{}",
        search_inspector_fields(
            format!(
                "[trace_id {trace_id}] flight->search: generated physical plan, took: {} ms",
                start.elapsed().as_millis()
            ),
            SearchInspectorFieldsBuilder::new()
                .trace_id(trace_id.to_string())
                .node_name(LOCAL_NODE.name.clone())
                .component("flight:do_get::search generated physical plan".to_string())
                .search_role("follower".to_string())
                .duration(start.elapsed().as_millis() as usize)
                .build()
        )
    );

    cleanup.0 = None;
    Ok((ctx, physical_plan, scan_stats, storage_shortfall))
}

#[derive(Default)]
struct PersistedSetup {
    tables: Vec<Arc<dyn TableProvider>>,
    scan_stats: ScanStats,
    metadata_count_file_list: Vec<FileKey>,
    index_file_list: Vec<FileKey>,
    index_result: Option<MultiResult>,
    storage_shortfall: Option<String>,
}

#[derive(Default)]
struct SegmentSetup {
    tables: Vec<Arc<dyn TableProvider>>,
    scan_stats: ScanStats,
    histogram: Option<Vec<u64>>,
}

impl PersistedSetup {
    fn with_segments(mut self, segments: SegmentSetup) -> Result<Self, Error> {
        // Completion order must not change union partition order or stats folding.
        self.tables.extend(segments.tables);
        self.scan_stats.add(&segments.scan_stats);
        if let Some(histogram) = segments.histogram {
            merge_histogram_result(&mut self.index_result, histogram)?;
        }
        Ok(self)
    }
}

struct QueryFilesCleanup<'a>(Option<&'a str>);

impl Drop for QueryFilesCleanup<'_> {
    fn drop(&mut self) {
        if let Some(trace_id) = self.0 {
            super::super::datafusion::storage::file_list::clear(trace_id);
        }
    }
}

/// Run the two bounded setup pipelines without spawning a detached task.
/// An error drops the sibling future before clearing its file registrations.
/// Cancellation does the same through the guard, including after one side
/// finished. Successful registrations remain owned by the query executor.
async fn run_setup_branches<P, S, A, B>(
    trace_id: &str,
    parallel: bool,
    persisted: P,
    segments: S,
) -> Result<(A, B), Error>
where
    P: std::future::Future<Output = Result<A, Error>>,
    S: std::future::Future<Output = Result<B, Error>>,
{
    let mut cleanup = QueryFilesCleanup(Some(trace_id));
    // Own both futures in a scope created AFTER the guard: cancellation must
    // drop their resources before clearing request-owned listing entries.
    let work = async move {
        if parallel {
            futures::try_join!(persisted, segments)
        } else {
            Ok((persisted.await?, segments.await?))
        }
    };
    let result = work.await?;
    cleanup.0 = None;
    Ok(result)
}

#[allow(clippy::too_many_arguments)]
fn apply_pushdowns_and_optimizations(
    trace_id: &str,
    ctx: &SessionContext,
    mut physical_plan: Arc<dyn ExecutionPlan>,
    scan_stats: &mut ScanStats,
    query_params: Arc<QueryParams>,
    metadata_count_file_list: Vec<FileKey>,
    index_file_list: Vec<FileKey>,
    index_result: Option<MultiResult>,
    idx_optimize_rule: Option<IndexOptimizeMode>,
) -> Result<Arc<dyn ExecutionPlan>, Error> {
    let cfg = get_config();

    let pushdown_filter = FilterPushdown::new();
    physical_plan = pushdown_filter
        .optimize(physical_plan, ctx.state().config_options())
        .map_err(|e| {
            log::error!("[trace_id {trace_id}] flight->search: pushdown filter error: {e}");
            e
        })?;
    // Numeric conjuncts under a FilterExec push into the vix scans below
    // it. This MUST run while the filter is still ADJACENT to the scan:
    // ProjectionPushdown may insert a ProjectionExec in between, and the
    // injection deliberately refuses to cross projections (they can
    // rename columns). vortex prunes chunks by per-chunk stats, ranged
    // sources skip the fetches, provably-disjoint files skip entirely via
    // footer stats. Conservative-only — the filter re-applies everything.
    physical_plan = search::datafusion::vix_format::inject_vix_scan_pruning(physical_plan)
        .map_err(|e| {
            log::error!("[trace_id {trace_id}] flight->search: vix numeric pushdown error: {e}");
            e
        })?;

    let limit_pushdown = LimitPushdown::new();
    physical_plan = limit_pushdown
        .optimize(physical_plan, ctx.state().config_options())
        .map_err(|e| {
            log::error!("[trace_id {trace_id}] flight->search: limit pushdown error: {e}");
            e
        })?;
    let projection_pushdown = ProjectionPushdown::new();
    physical_plan = projection_pushdown
        .optimize(physical_plan, ctx.state().config_options())
        .map_err(|e| {
            log::error!("[trace_id {trace_id}] flight->search: projection pushdown error: {e}");
            e
        })?;

    if cfg.common.feature_dynamic_pushdown_filter_enabled {
        let pushdown_filter = FilterPushdown::new_post_optimization();
        physical_plan = pushdown_filter.optimize(physical_plan, ctx.state().config_options()).map_err(|e| {
            log::error!("[trace_id {trace_id}] flight->search: pushdown filter post optimization error: {e}");
            e
        })?;
    }

    if !metadata_count_file_list.is_empty() || !index_file_list.is_empty() || index_result.is_some()
    {
        let index_optimize_start = std::time::Instant::now();
        scan_stats.add(&collect_stats(&metadata_count_file_list));
        scan_stats.add(&collect_stats(&index_file_list));
        physical_plan = aggregate_optimize_rewrite(
            query_params.clone(),
            metadata_count_file_list,
            index_file_list,
            index_result,
            idx_optimize_rule,
            physical_plan,
        )?;
        log::info!(
            "{}",
            search_inspector_fields(
                format!("[trace_id {trace_id}] flight->search: index optimize rewrite"),
                SearchInspectorFieldsBuilder::new()
                    .trace_id(trace_id.to_string())
                    .node_name(LOCAL_NODE.name.clone())
                    .component("flight:do_get::search index optimize rewrite".to_string())
                    .search_role("follower".to_string())
                    .duration(index_optimize_start.elapsed().as_millis() as usize)
                    .build()
            )
        );
    }

    Ok(physical_plan)
}

fn plan_has_noncanonical_filter(plan: &Arc<dyn ExecutionPlan>, time_range: (i64, i64)) -> bool {
    plan.downcast_ref::<FilterExec>()
        .is_some_and(|filter| !is_canonical_timestamp_filter(filter.predicate(), time_range))
        || plan
            .children()
            .iter()
            .any(|child| plan_has_noncanonical_filter(child, time_range))
}

fn is_canonical_timestamp_filter(
    predicate: &Arc<dyn PhysicalExpr>,
    time_range: (i64, i64),
) -> bool {
    fn visit(
        expr: &Arc<dyn PhysicalExpr>,
        time_range: (i64, i64),
        lower_seen: &mut bool,
        upper_seen: &mut bool,
    ) -> bool {
        let Some(binary) = expr.downcast_ref::<BinaryExpr>() else {
            return false;
        };
        if binary.op() == &Operator::And {
            return visit(binary.left(), time_range, lower_seen, upper_seen)
                && visit(binary.right(), time_range, lower_seen, upper_seen);
        }
        let (Some(column), Some(literal)) = (
            binary.left().downcast_ref::<Column>(),
            binary.right().downcast_ref::<Literal>(),
        ) else {
            return false;
        };
        if column.name() != TIMESTAMP_COL_NAME {
            return false;
        }
        let ScalarValue::Int64(Some(value)) = literal.value() else {
            return false;
        };
        match (binary.op(), *value) {
            (Operator::GtEq, value) if value == time_range.0 && !*lower_seen => {
                *lower_seen = true;
                true
            }
            (Operator::Lt, value) if value == time_range.1 && !*upper_seen => {
                *upper_seen = true;
                true
            }
            _ => false,
        }
    }

    let (mut lower_seen, mut upper_seen) = (false, false);
    visit(predicate, time_range, &mut lower_seen, &mut upper_seen) && lower_seen && upper_seen
}

fn plan_has_row_dropping_limit(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.downcast_ref::<GlobalLimitExec>().is_some()
        || plan.downcast_ref::<LocalLimitExec>().is_some()
        || plan
            .children()
            .iter()
            .any(|child| plan_has_row_dropping_limit(child))
}

fn plan_transforms_timestamp(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let transforms_here = plan
        .downcast_ref::<ProjectionExec>()
        .is_some_and(|projection| {
            projection.expr().iter().any(|projection_expr| {
                projection_expr.alias == TIMESTAMP_COL_NAME
                    && !projection_expr
                        .expr
                        .downcast_ref::<Column>()
                        .is_some_and(|column| column.name() == TIMESTAMP_COL_NAME)
            })
        });
    transforms_here
        || plan
            .children()
            .iter()
            .any(|child| plan_transforms_timestamp(child))
}

/// Only post-optimizer descending SimpleSelect certifies that every non-time
/// predicate is represented by `index_condition`. A raw scan-node limit may
/// trim retained exact batches but cannot authorize whole-segment skipping.
fn segment_top_n_plan(
    idx_optimize_rule: &Option<IndexOptimizeMode>,
    sorted_by_time: bool,
    has_residual_filter: bool,
    has_row_dropping_limit: bool,
    has_transformed_timestamp: bool,
    scan_limit: Option<usize>,
) -> Option<super::segments_scan::SegmentTopNPlan> {
    match idx_optimize_rule {
        Some(IndexOptimizeMode::SimpleSelect(n, false))
            if *n > 0
                && sorted_by_time
                && !has_residual_filter
                && !has_row_dropping_limit
                && !has_transformed_timestamp =>
        {
            super::segments_scan::SegmentTopNPlan::exact_desc(*n)
        }
        _ if sorted_by_time => {
            scan_limit.and_then(super::segments_scan::SegmentTopNPlan::trim_only)
        }
        _ => None,
    }
}

fn can_use_metadata_count(
    idx_optimize_rule: &Option<IndexOptimizeMode>,
    index_condition: Option<&IndexCondition>,
) -> bool {
    cfg!(feature = "enterprise")
        && matches!(idx_optimize_rule, Some(IndexOptimizeMode::SimpleCount))
        && index_condition.is_some_and(IndexCondition::is_condition_all)
}

/// `index_fields` feeds filter/condition extraction (`IndexRule`) and covers
/// every term-indexed field with its registry type (strings plus
/// numeric/bool), while `column_store_fields` is the fast-path eligibility
/// set (DESIGN §15.6): the index-only aggregation fast paths
/// (`FollowerIndexOptimizerRule`) may only touch non-`_timestamp` fields
/// that are in the stream's `column_store_fields` setting.
#[allow(clippy::too_many_arguments)]
fn optimizer_physical_plan(
    plan: Arc<dyn ExecutionPlan>,
    ctx: &SessionContext,
    schema: &Schema,
    time_range: (i64, i64),
    fst_fields: Vec<String>,
    index_fields: HashMap<String, DataType>,
    column_store_fields: HashSet<String>,
    index_condition_ref: Arc<Mutex<Option<IndexCondition>>>,
    index_optimizer_rule_ref: Arc<Mutex<Option<IndexOptimizeMode>>>,
) -> Result<Arc<dyn ExecutionPlan>, Error> {
    // pilot fix B: term-indexed STRING fields (everything except the fts
    // fields, whose values are token-indexed only) are additionally eligible
    // for single-field TopN/Distinct when the query has no condition —
    // served from the term dictionary alone, per-file capability check as
    // backstop. Numeric/bool fields are deliberately EXCLUDED: their
    // dictionary holds canonical int/float text forms (`38` vs `38.0` are
    // distinct terms for one numeric value, and tagged numeric terms are not
    // string values), so dictionary counts cannot reproduce the scan's typed
    // grouping — those queries keep the docs-column / scan paths.
    let unfiltered_index_fields: HashSet<String> = index_fields
        .iter()
        .filter(|(field, data_type)| {
            numeric_kind_of(data_type).is_none() && !fst_fields.contains(*field)
        })
        .map(|(field, _)| field.clone())
        .collect();
    let index_rule = IndexRule::new(index_fields, index_condition_ref.clone());
    let original_plan = Arc::clone(&plan);
    let plan = index_rule.optimize(plan, ctx.state().config_options())?;

    // if the index rule can't optimize, we should take the index optimizer rule
    if !index_rule.can_optimize() {
        index_optimizer_rule_ref.lock().take();
    }

    // if the index condition is some, and the index optimizer rule is none,
    // and filter only have _timestamp filter, we can try to optimize the plan
    if index_condition_ref.lock().is_some()
        && index_optimizer_rule_ref.lock().is_none()
        && index_rule.can_optimize()
    {
        let index_optimizer_rule = FollowerIndexOptimizerRule::new(
            time_range,
            column_store_fields,
            unfiltered_index_fields,
            index_optimizer_rule_ref.clone(),
        );
        let _ = index_optimizer_rule.optimize(original_plan, ctx.state().config_options())?;
    }

    let rewrite_match_rule = RewriteMatchPhysical::new(
        fst_fields
            .clone()
            .into_iter()
            .map(|f| {
                (
                    f.clone(),
                    schema.field_with_name(&f).unwrap().data_type().clone(),
                )
            })
            .collect(),
    );
    let plan = rewrite_match_rule.optimize(plan, ctx.state().config_options())?;

    // reset the index_condition if index_optimizer_rule is none and index_condition is all
    let index_condition = index_condition_ref.lock().clone();
    if index_condition.is_some()
        && index_condition.as_ref().unwrap().is_condition_all()
        && index_optimizer_rule_ref.lock().is_none()
    {
        index_condition_ref.lock().take();
    }

    Ok(plan)
}

/// Adopt the leader's `bloom_ver` for every hydrated file. `query_by_ids`
/// serves rows from the id-keyed local file-list cache, which never observes
/// later `bloom_ver` stamps (0 -> v) or restamps (consolidation, heal): a
/// stale value makes bloom pruning skip the group `.bf` or probe a retired
/// one. The leader read `bloom_ver` from the DB for this query, so its value
/// is at least as fresh as the cache. `ids` and `bloom_vers` are the request's
/// parallel lists; anything but an exact length match (e.g. an older leader
/// sending none) leaves the cached values untouched. Returns how many changed.
fn refresh_cached_bloom_vers(files: &mut [FileKey], ids: &[i64], bloom_vers: &[i64]) -> usize {
    if bloom_vers.is_empty() || bloom_vers.len() != ids.len() {
        return 0;
    }
    let current: HashMap<i64, i64> = ids
        .iter()
        .zip(bloom_vers)
        .filter(|(_, ver)| **ver != infra::file_list::BLOOM_VER_UNKNOWN)
        .map(|(id, ver)| (*id, *ver))
        .collect();
    let mut changed = 0;
    for file in files.iter_mut() {
        if let Some(&ver) = current.get(&file.id)
            && file.meta.bloom_ver != ver
        {
            file.meta.bloom_ver = ver;
            changed += 1;
        }
    }
    changed
}

#[allow(clippy::too_many_arguments)]
#[tracing::instrument(skip_all, fields(org_id = org_id, stream_name = stream_name))]
async fn get_file_list_by_ids(
    trace_id: &str,
    org_id: &str,
    stream_type: StreamType,
    stream_name: &str,
    time_range: Option<(i64, i64)>,
    equal_items: &[(String, String)],
    ids: &[i64],
) -> Result<(Vec<FileKey>, usize), Error> {
    let start = std::time::Instant::now();
    let stream_settings = infra::schema::get_settings(org_id, stream_name, stream_type)
        .await
        .unwrap_or_default();
    let partition_keys = stream_settings.partition_keys;
    let file_list = crate::service::file_list::query_by_ids(
        trace_id,
        org_id,
        stream_type,
        stream_name,
        time_range,
        ids,
    )
    .await?;

    let mut files = Vec::with_capacity(file_list.len());
    for file in file_list {
        if match_file(
            org_id,
            stream_type,
            stream_name,
            time_range,
            &file,
            &partition_keys,
            equal_items,
        )
        .await
        {
            files.push(file);
        }
    }
    files.par_sort_unstable_by(|a, b| a.key.cmp(&b.key));
    files.dedup_by(|a, b| a.key == b.key);
    Ok((files, start.elapsed().as_millis() as usize))
}

fn split_metadata_count_files(
    file_list: Vec<FileKey>,
    time_range: (i64, i64),
) -> (Vec<FileKey>, Vec<FileKey>) {
    file_list
        .into_iter()
        .partition(|file| file.meta.min_ts >= time_range.0 && file.meta.max_ts < time_range.1)
}

async fn handle_index_optimize(
    idx_optimize_rule: &mut Option<IndexOptimizeMode>,
    file_list: Vec<FileKey>,
    time_range: (i64, i64),
    column_store_fields: &HashSet<String>,
    index_condition: Option<&IndexCondition>,
) -> Result<(Vec<FileKey>, Vec<FileKey>), Error> {
    // early return if not simple count, histogram, topn or the M16
    // count(field)/min-max stats-answered modes
    if !matches!(
        idx_optimize_rule,
        Some(IndexOptimizeMode::SimpleCount)
            | Some(IndexOptimizeMode::SimpleCountField(..))
            | Some(IndexOptimizeMode::SimpleMinMax(..))
            | Some(IndexOptimizeMode::SimpleHistogram(..))
            | Some(IndexOptimizeMode::SimpleMultiHistogram(..))
            | Some(IndexOptimizeMode::SimpleTopN(..))
            | Some(IndexOptimizeMode::SimpleDistinct(..))
    ) {
        return Ok((vec![], file_list));
    }

    // pilot fix B: a TopN/Distinct over a field that is NOT column-stored
    // can only be served index-only — from the term dictionary of core
    // files lying fully inside the query range. Partial-range files go to
    // the DataFusion branch, which is always correct.
    let needs_dict_only = match idx_optimize_rule {
        Some(IndexOptimizeMode::SimpleTopN(fields, ..)) => fields
            .iter()
            .any(|field| !column_store_fields.contains(field)),
        Some(IndexOptimizeMode::SimpleDistinct(field, ..)) => !column_store_fields.contains(field),
        _ => false,
    };

    let allow_data_only_vix = data_only_vix_capable(idx_optimize_rule, index_condition);
    // TODO: support IndexOptimizeMode::SimpleDistinct for add timestamp
    // filter to vix search
    let time_range = if needs_dict_only
        || matches!(
            idx_optimize_rule,
            Some(IndexOptimizeMode::SimpleDistinct(..))
        ) {
        Some(time_range)
    } else {
        None
    };
    let (index_files, datafusion_files) =
        split_file_list_by_time_range(file_list, time_range, allow_data_only_vix);
    // set optimize rule to None, because datafusion should not use it
    *idx_optimize_rule = None;

    Ok((index_files, datafusion_files))
}

/// Whether an indexless core data file can answer this query exactly without
/// a `.vxi` sidecar. Native docs-column equality supports SimpleHistogram,
/// SimpleCount and SimpleSelect; unfiltered SimpleHistogram/SimpleCount need
/// only the data object's row count, zone table and `_timestamp` chunks. ALL
/// multi-histograms remain on DataFusion until their extracted timestamp
/// coordinates match collection semantics.
fn data_only_vix_capable(
    idx_optimize_rule: &Option<IndexOptimizeMode>,
    index_condition: Option<&IndexCondition>,
) -> bool {
    let Some(condition) = index_condition else {
        return false;
    };
    match idx_optimize_rule {
        Some(IndexOptimizeMode::SimpleHistogram(..)) | Some(IndexOptimizeMode::SimpleCount) => {
            condition.is_condition_all() || condition.single_equal_term().is_some()
        }
        Some(IndexOptimizeMode::SimpleSelect(limit, _)) if *limit > 0 => {
            condition.single_equal_term().is_some()
        }
        _ => false,
    }
}

/// Index-branch eligibility: every core `.vix` file with an index sidecar,
/// plus data-only core files whose query shape is exact without one. When
/// `time_range` is set, the file must lie fully inside `[start, end)`
/// (`max_ts < end`, matching the per-file `file_in_range` check). Everything
/// else takes the DataFusion branch.
///
/// There is deliberately NO settings-freshness gate here (the legacy
/// `index_updated_at` settings stamp was removed entirely): whether an
/// individual file can serve a query is decided per file by the capability
/// probes (fields table / key-term probe for terms, `missing_docs_column` /
/// `docs_column_available` for the aggregate fast paths). A global stamp
/// would disable the index for the ENTIRE existing dataset on any settings
/// change (observed live when adding `column_store_fields`), while the
/// per-file probes already route incapable files to the DataFusion branch.
fn split_file_list_by_time_range(
    file_list: Vec<FileKey>,
    time_range: Option<(i64, i64)>,
    allow_data_only_vix: bool,
) -> (Vec<FileKey>, Vec<FileKey>) {
    file_list.into_iter().partition(|file| {
        config::FileFormat::from_extension(&file.key) == Some(config::FileFormat::Vix)
            && (file.meta.index_size > 0 || allow_data_only_vix)
            && time_range
                .is_none_or(|(start, end)| file.meta.min_ts >= start && file.meta.max_ts < end)
    })
}

fn collect_stats(files: &[FileKey]) -> ScanStats {
    let mut scan_stats = ScanStats::new();
    scan_stats.files = files.len() as i64;
    for file in files.iter() {
        scan_stats.records += file.meta.records;
        scan_stats.original_size += file.meta.original_size;
        scan_stats.compressed_size += file.meta.compressed_size;
        scan_stats.idx_scan_size += file.meta.index_size;
    }
    scan_stats
}
fn merge_histogram_result(
    target: &mut Option<MultiResult>,
    contribution: Vec<u64>,
) -> Result<(), Error> {
    let Some(current) = target else {
        *target = Some(MultiResult::Histogram(contribution));
        return Ok(());
    };
    let MultiResult::Histogram(histogram) = current else {
        return Err(Error::Message(
            "segment histogram cannot merge with a non-histogram index result".to_string(),
        ));
    };
    if histogram.is_empty() {
        histogram.resize(contribution.len(), 0);
    }
    if histogram.len() != contribution.len() {
        return Err(Error::Message(format!(
            "segment histogram has {} buckets, precomputed result has {}",
            contribution.len(),
            histogram.len()
        )));
    }
    for (total, value) in histogram.iter_mut().zip(contribution) {
        *total = total.checked_add(value).ok_or_else(|| {
            Error::Message("segment histogram bucket count overflowed u64".to_string())
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use config::meta::stream::{FileKey, FileMeta};
    use datafusion::{
        execution::{SessionStateBuilder, runtime_env::RuntimeEnvBuilder},
        prelude::SessionConfig,
    };

    use super::*;
    use crate::service::search::{
        datafusion::{
            optimizer::logical_optimizer::rewrite_histogram::RewriteHistogram,
            table_provider::empty_table::NewEmptyTable, udf::histogram_udf,
        },
        index::Condition,
    };

    #[test]
    fn canonical_timestamp_filter_rejects_additional_residuals() {
        let timestamp = || Arc::new(Column::new(TIMESTAMP_COL_NAME, 0)) as Arc<dyn PhysicalExpr>;
        let literal = |value| {
            Arc::new(Literal::new(ScalarValue::Int64(Some(value)))) as Arc<dyn PhysicalExpr>
        };
        let lower = Arc::new(BinaryExpr::new(timestamp(), Operator::GtEq, literal(100)))
            as Arc<dyn PhysicalExpr>;
        let upper = Arc::new(BinaryExpr::new(timestamp(), Operator::Lt, literal(200)))
            as Arc<dyn PhysicalExpr>;
        let canonical = Arc::new(BinaryExpr::new(
            Arc::clone(&lower),
            Operator::And,
            Arc::clone(&upper),
        )) as Arc<dyn PhysicalExpr>;
        assert!(is_canonical_timestamp_filter(&canonical, (100, 200)));

        let stricter = Arc::new(BinaryExpr::new(timestamp(), Operator::Gt, literal(100)))
            as Arc<dyn PhysicalExpr>;
        let residual =
            Arc::new(BinaryExpr::new(canonical, Operator::And, stricter)) as Arc<dyn PhysicalExpr>;
        assert!(!is_canonical_timestamp_filter(&residual, (100, 200)));
        assert!(!is_canonical_timestamp_filter(&lower, (100, 200)));
    }

    #[test]
    fn segment_top_n_plan_requires_optimizer_proof_for_skipping() {
        assert_eq!(
            segment_top_n_plan(
                &Some(IndexOptimizeMode::SimpleSelect(1000, false)),
                true,
                false,
                false,
                false,
                Some(10),
            ),
            super::super::segments_scan::SegmentTopNPlan::exact_desc(1000),
        );
        assert_eq!(
            segment_top_n_plan(
                &Some(IndexOptimizeMode::SimpleSelect(1000, false)),
                true,
                true,
                false,
                false,
                Some(1000),
            ),
            super::super::segments_scan::SegmentTopNPlan::trim_only(1000),
        );
        assert_eq!(
            segment_top_n_plan(&None, true, false, false, false, Some(1000)),
            super::super::segments_scan::SegmentTopNPlan::trim_only(1000),
        );
        assert_eq!(
            segment_top_n_plan(
                &Some(IndexOptimizeMode::SimpleSelect(1000, true)),
                false,
                false,
                false,
                false,
                Some(1000),
            ),
            None,
        );
        assert_eq!(
            segment_top_n_plan(&None, true, false, false, false, Some(0)),
            None
        );
        assert_eq!(
            segment_top_n_plan(
                &Some(IndexOptimizeMode::SimpleSelect(1000, false)),
                true,
                false,
                true,
                false,
                Some(1000),
            ),
            super::super::segments_scan::SegmentTopNPlan::trim_only(1000),
        );
        assert_eq!(
            segment_top_n_plan(
                &Some(IndexOptimizeMode::SimpleSelect(1000, false)),
                true,
                false,
                false,
                true,
                Some(1000),
            ),
            super::super::segments_scan::SegmentTopNPlan::trim_only(1000),
        );
    }

    fn make_file(key: &str, min_ts: i64, max_ts: i64, index_size: i64) -> FileKey {
        FileKey {
            key: key.to_string(),
            meta: FileMeta {
                min_ts,
                max_ts,
                index_size,
                records: 10,
                original_size: 100,
                compressed_size: 50,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn bloom_survivors_remain_once_when_eager_aggregate_falls_back() {
        use infra::bloom::{BloomBuilder, BloomWriter, path::bloom_path};
        let stream = format!("bloom-aggregate-{}", config::ider::generate_trace_id());
        let date = "2026/01/01/00";
        let version = 1_234_567;
        let mut builder = BloomBuilder::new();
        // An empty Bloom proves no matches without probabilistic collisions.
        builder.begin_with_blocks(1, "value", 1);
        let present = builder.begin_with_blocks(2, "value", 1);
        builder.insert(present, b"hit");
        let path = bloom_path("test", StreamType::Logs, &stream, date, version);
        let account = infra::storage::get_account("test", &path).unwrap_or_default();
        infra::storage::put(
            &account,
            &path,
            bytes::Bytes::from(BloomWriter::serialize(builder.finish()).unwrap()),
        )
        .await
        .unwrap();
        let condition = IndexCondition {
            conditions: vec![Condition::Equal("value".into(), "hit".into())],
        };
        let files = (1..=3)
            .map(|id| {
                let mut file = make_file(
                    &format!("files/test/logs/{stream}/{date}/{id}.vix"),
                    10,
                    20,
                    1,
                );
                file.id = id;
                file.account = account.clone();
                // The third file has unavailable Bloom information and must survive.
                file.meta.bloom_ver = if id == 3 { version + 1 } else { version };
                file
            })
            .collect();
        let files = crate::service::search::bloom_pruner::prune(
            "bloom-aggregate",
            "test",
            StreamType::Logs,
            &stream,
            files,
            &condition,
            vec!["value".into()],
            &config::VixBloomCompositeScope::All,
            false,
            &std::collections::HashSet::new(),
        )
        .await;
        let mut rule = Some(IndexOptimizeMode::SimpleCount);
        let (mut eager, mut residual) =
            handle_index_optimize(&mut rule, files, (0, 30), &HashSet::new(), Some(&condition))
                .await
                .unwrap();
        let params = Arc::new(QueryParams {
            trace_id: "bloom-aggregate".into(),
            org_id: "test".into(),
            stream: TableReference::from(stream.as_str()),
            stream_type: StreamType::Logs,
            stream_name: stream,
            time_range: (0, 30),
            work_group: None,
            use_inverted_index: true,
            full_text_fields: None,
        });
        // Deliberately missing data/index objects force the real fail-open
        // aggregate path, not a synthetic result or a mocked collector.
        let (_, add_filter, _) = super::super::storage::vix_search(
            params,
            &mut eager,
            Some(condition),
            Some(IndexOptimizeMode::SimpleCount),
        )
        .await
        .unwrap();
        assert!(add_filter);
        residual.append(&mut eager);
        residual.sort_unstable_by_key(|file| file.id);
        assert_eq!(
            residual.iter().map(|file| file.id).collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert_eq!(collect_stats(&residual).records, 20);
        infra::storage::del(vec![(&account, &path)]).await.unwrap();
    }

    fn core_file(min_ts: i64, max_ts: i64, index_size: i64) -> FileKey {
        make_file(
            &format!("files/default/logs/s/2024/02/16/16/{min_ts}_{max_ts}.vix"),
            min_ts,
            max_ts,
            index_size,
        )
    }

    #[test]
    fn test_merge_segment_histogram_with_core_result() {
        let mut result = None;
        merge_histogram_result(&mut result, vec![1, 2, 0]).unwrap();
        merge_histogram_result(&mut result, vec![3, 4, 5]).unwrap();
        assert!(matches!(
            &result,
            Some(MultiResult::Histogram(histogram))
                if histogram == &vec![4, 6, 5]
        ));

        let err = merge_histogram_result(&mut result, vec![1, 2]).unwrap_err();
        assert!(err.to_string().contains("2 buckets"));

        let mut wrong = Some(MultiResult::Count(1));
        let err = merge_histogram_result(&mut wrong, vec![1]).unwrap_err();
        assert!(err.to_string().contains("non-histogram"));
    }

    #[test]
    fn test_split_file_list_empty() {
        let (index_files, datafusion) = split_file_list_by_time_range(vec![], None, false);
        assert!(index_files.is_empty());
        assert!(datafusion.is_empty());
    }

    #[test]
    fn test_split_file_list_all_index() {
        let files = vec![core_file(100, 200, 512), core_file(300, 400, 1024)];
        let (index_files, datafusion) = split_file_list_by_time_range(files, None, false);
        assert_eq!(index_files.len(), 2);
        assert!(datafusion.is_empty());
    }

    #[test]
    fn test_split_file_list_no_index_goes_to_datafusion() {
        let files = vec![core_file(100, 200, 0)]; // index_size == 0
        let (index_files, datafusion) = split_file_list_by_time_range(files, None, false);
        assert!(index_files.is_empty());
        assert_eq!(datafusion.len(), 1);
    }

    #[test]
    fn test_split_file_list_indexless_multi_histogram_goes_to_datafusion() {
        let mode = Some(IndexOptimizeMode::SimpleMultiHistogram(
            0,
            20,
            10,
            0,
            "service".to_string(),
        ));
        let mut all = IndexCondition::new();
        all.add_condition(Condition::All());
        let allow = data_only_vix_capable(&mode, Some(&all));
        assert!(!allow);
        let (index_files, datafusion) =
            split_file_list_by_time_range(vec![core_file(100, 200, 0)], None, allow);
        assert!(index_files.is_empty());
        assert_eq!(datafusion.len(), 1);

        let mut equality = IndexCondition::new();
        equality.add_condition(Condition::Equal("service".to_string(), "api".to_string()));
        assert!(!data_only_vix_capable(&mode, Some(&equality)));
    }

    #[test]
    fn test_data_only_vix_capability_is_query_shaped() {
        let mut all = IndexCondition::new();
        all.add_condition(Condition::All());
        let mut equality = IndexCondition::new();
        equality.add_condition(Condition::Equal("service".to_string(), "api".to_string()));
        let mut all_plus_equality = IndexCondition::new();
        all_plus_equality.add_condition(Condition::All());
        all_plus_equality.add_condition(Condition::Equal("service".to_string(), "api".to_string()));

        assert!(data_only_vix_capable(
            &Some(IndexOptimizeMode::SimpleHistogram(0, 10, 2, 0)),
            Some(&all),
        ));
        assert!(data_only_vix_capable(
            &Some(IndexOptimizeMode::SimpleHistogram(0, 10, 2, 0)),
            Some(&equality),
        ));
        assert!(!data_only_vix_capable(
            &Some(IndexOptimizeMode::SimpleMultiHistogram(
                0,
                20,
                10,
                0,
                "service".to_string(),
            )),
            Some(&all),
        ));
        assert!(!data_only_vix_capable(
            &Some(IndexOptimizeMode::SimpleMultiHistogram(
                0,
                20,
                10,
                0,
                "service".to_string(),
            )),
            Some(&equality),
        ));
        assert!(!data_only_vix_capable(
            &Some(IndexOptimizeMode::SimpleMultiHistogram(
                0,
                20,
                10,
                0,
                "service".to_string(),
            )),
            Some(&all_plus_equality),
        ));
        assert!(data_only_vix_capable(
            &Some(IndexOptimizeMode::SimpleSelect(10, false)),
            Some(&equality),
        ));
        assert!(!data_only_vix_capable(
            &Some(IndexOptimizeMode::SimpleSelect(10, false)),
            Some(&all),
        ));
        // an unfiltered count needs only data-side metadata; a single
        // equality counts the native predicate column; anything else needs
        // the sidecar's terms
        assert!(data_only_vix_capable(
            &Some(IndexOptimizeMode::SimpleCount),
            Some(&all),
        ));
        assert!(data_only_vix_capable(
            &Some(IndexOptimizeMode::SimpleCount),
            Some(&equality),
        ));
        assert!(!data_only_vix_capable(
            &Some(IndexOptimizeMode::SimpleCount),
            Some(&all_plus_equality),
        ));
        assert!(!data_only_vix_capable(
            &Some(IndexOptimizeMode::SimpleHistogram(0, 10, 2, 0)),
            None,
        ));
    }

    /// Only core `.vix` files are index-eligible: parquet/vortex files never
    /// take the index branch, no matter their index_size or creation time.
    #[test]
    fn test_split_file_list_non_core_files_go_to_datafusion() {
        // a snowflake-named parquet file with a nonzero index_size
        let id = 1_700_000_000_001i64 << 22;
        let files = vec![
            make_file(
                &format!("files/default/logs/quickstart1/2024/02/16/16/{id}.parquet"),
                100,
                200,
                512,
            ),
            make_file("files/default/logs/s/1.vortex", 100, 200, 512),
            core_file(100, 200, 512),
        ];
        let (index_files, datafusion) = split_file_list_by_time_range(files, None, false);
        assert_eq!(index_files.len(), 1);
        assert!(index_files[0].key.ends_with(".vix"));
        assert_eq!(datafusion.len(), 2);
    }

    /// Pilot fix B routing: with a `time_range`, only files fully inside
    /// `[start, end)` (strict end, matching the per-file `file_in_range`
    /// check) take the index branch.
    #[test]
    fn test_split_file_list_strict_range() {
        let file = |key: &str, min_ts: i64, max_ts: i64| FileKey {
            key: key.to_string(),
            meta: FileMeta {
                min_ts,
                max_ts,
                index_size: 512,
                records: 10,
                ..Default::default()
            },
            ..Default::default()
        };
        let files = vec![
            file("files/default/logs/s/a.vix", 100, 150), // core, fully in range
            file("files/default/logs/s/b.parquet", 100, 150), // legacy (index-less)
            file("files/default/logs/s/c.vix", 100, 200), // core, max_ts == end (out)
            file("files/default/logs/s/d.vix", 50, 150),  // core, starts before range
        ];
        let (index_files, datafusion) =
            split_file_list_by_time_range(files, Some((100, 200)), false);
        assert_eq!(index_files.len(), 1);
        assert!(index_files[0].key.ends_with("a.vix"));
        assert_eq!(datafusion.len(), 3);
    }

    /// There is no settings-stamp gate: files created before a settings
    /// change (e.g. a `column_store_fields` addition) stay index-eligible —
    /// per-file capability probes route incapable files to the scan branch.
    /// Regression for the live incident where a settings PUT disabled the
    /// index for the entire pre-existing dataset.
    #[test]
    fn test_split_file_list_ignores_settings_age() {
        let old_ms = 1_600_000_000_000i64;
        let files = vec![
            make_file(
                &format!("files/default/logs/s/2024/02/16/16/{}.vix", old_ms << 22),
                100,
                200,
                512,
            ),
            // generate_file_name form (snowflake + 4 hex chars)
            make_file(
                &format!(
                    "files/default/logs/s/2024/02/16/16/{}a3f2.vix",
                    old_ms << 22
                ),
                100,
                200,
                512,
            ),
        ];
        let (index_files, datafusion) = split_file_list_by_time_range(files, None, false);
        assert_eq!(index_files.len(), 2);
        assert!(datafusion.is_empty());
    }

    #[test]
    fn test_split_file_list_for_metadata_count_only_full_range_files() {
        let files = vec![
            core_file(100, 199, 0), // fully in [100, 200)
            core_file(99, 150, 0),  // overlaps the start boundary
            core_file(150, 200, 0), // touches the exclusive end boundary
        ];

        let (metadata, scan) = split_metadata_count_files(files, (100, 200));

        assert_eq!(metadata.len(), 1);
        assert_eq!(metadata[0].meta.min_ts, 100);
        assert_eq!(metadata[0].meta.max_ts, 199);
        assert_eq!(scan.len(), 2);
    }

    #[test]
    fn test_collect_stats_empty() {
        let stats = collect_stats(&[]);
        assert_eq!(stats.files, 0);
        assert_eq!(stats.records, 0);
        assert_eq!(stats.original_size, 0);
        assert_eq!(stats.compressed_size, 0);
        assert_eq!(stats.idx_scan_size, 0);
    }

    #[test]
    fn test_collect_stats_aggregates() {
        let files = vec![core_file(0, 100, 10), core_file(100, 200, 20)];
        let stats = collect_stats(&files);
        assert_eq!(stats.files, 2);
        assert_eq!(stats.records, 20); // 10 + 10
        assert_eq!(stats.original_size, 200); // 100 + 100
        assert_eq!(stats.compressed_size, 100); // 50 + 50
        assert_eq!(stats.idx_scan_size, 30); // 10 + 20
    }

    #[tokio::test]
    async fn test_optimizer_physical_plan_histogram_with_index_filter() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("kubernetes_namespace_name", DataType::Utf8, false),
        ]));
        let start_time = 1757401694060000;
        let end_time = 1757402594060000;
        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_target_partitions(12))
            .with_runtime_env(Arc::new(RuntimeEnvBuilder::new().build().unwrap()))
            .with_default_features()
            .with_optimizer_rule(Arc::new(RewriteHistogram::new(
                start_time, end_time, 60, None,
            )))
            .build();
        let ctx = SessionContext::new_with_state(state);
        let provider = NewEmptyTable::new("default", schema.clone());
        ctx.register_table("default", Arc::new(provider)).unwrap();
        ctx.register_udf(histogram_udf::HISTOGRAM_UDF.clone());

        let logical_plan = ctx
            .state()
            .create_logical_plan(
                "SELECT histogram(_timestamp) as ts, count(*) as cnt \
                 FROM default \
                 WHERE kubernetes_namespace_name = 'ziox' \
                 GROUP BY ts ORDER BY ts",
            )
            .await
            .unwrap();
        let physical_plan = ctx
            .state()
            .create_physical_plan(&logical_plan)
            .await
            .unwrap();
        let index_condition_ref = Arc::new(Mutex::new(None));
        let index_optimizer_rule_ref = Arc::new(Mutex::new(None));

        let _plan = optimizer_physical_plan(
            physical_plan,
            &ctx,
            &schema,
            (start_time, end_time),
            vec![],
            HashMap::from([("kubernetes_namespace_name".to_string(), DataType::Utf8)]),
            HashSet::new(),
            index_condition_ref.clone(),
            index_optimizer_rule_ref.clone(),
        )
        .unwrap();

        assert_eq!(
            index_condition_ref.lock().clone(),
            Some(IndexCondition {
                conditions: vec![Condition::Equal(
                    "kubernetes_namespace_name".to_string(),
                    "ziox".to_string(),
                )],
            })
        );
        // SimpleHistogram reads only `_timestamp`, so it stays eligible even
        // with an empty column_store_fields set: the histogram fast path
        // engages on top of the extracted index condition.
        assert_eq!(
            index_optimizer_rule_ref.lock().clone(),
            Some(IndexOptimizeMode::SimpleHistogram(
                1757401680000000,
                60000000,
                16,
                0
            ))
        );
    }

    #[tokio::test]
    async fn test_optimizer_physical_plan_multi_histogram_column_store_eligibility() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("level", DataType::Utf8, false),
            Field::new("kubernetes_namespace_name", DataType::Utf8, false),
        ]));
        let start_time = 1757401694060000;
        let end_time = 1757402594060000;
        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_target_partitions(12))
            .with_runtime_env(Arc::new(RuntimeEnvBuilder::new().build().unwrap()))
            .with_default_features()
            .with_optimizer_rule(Arc::new(RewriteHistogram::new(
                start_time, end_time, 60, None,
            )))
            .build();
        let ctx = SessionContext::new_with_state(state);
        let provider = NewEmptyTable::new("default", schema.clone());
        ctx.register_table("default", Arc::new(provider)).unwrap();
        ctx.register_udf(histogram_udf::HISTOGRAM_UDF.clone());

        let logical_plan = ctx
            .state()
            .create_logical_plan(
                "SELECT histogram(_timestamp) as ts, level, count(*) as cnt \
                 FROM default \
                 WHERE kubernetes_namespace_name = 'ziox' \
                 GROUP BY ts, level ORDER BY ts",
            )
            .await
            .unwrap();

        // the breakdown field drives fast-path eligibility (DESIGN §15.6):
        // SimpleMultiHistogram only engages when `level` is in the stream's
        // column_store_fields
        let cases = vec![
            (HashSet::new(), None),
            (
                HashSet::from(["level".to_string()]),
                Some(IndexOptimizeMode::SimpleMultiHistogram(
                    1757401680000000,
                    1757402594060000,
                    60000000,
                    0,
                    "level".to_string(),
                )),
            ),
        ];
        for (column_store_fields, expected) in cases {
            let physical_plan = ctx
                .state()
                .create_physical_plan(&logical_plan)
                .await
                .unwrap();
            let index_condition_ref = Arc::new(Mutex::new(None));
            let index_optimizer_rule_ref = Arc::new(Mutex::new(None));

            let _plan = optimizer_physical_plan(
                physical_plan,
                &ctx,
                &schema,
                (start_time, end_time),
                vec![],
                HashMap::from([
                    ("kubernetes_namespace_name".to_string(), DataType::Utf8),
                    ("level".to_string(), DataType::Utf8),
                ]),
                column_store_fields.clone(),
                index_condition_ref.clone(),
                index_optimizer_rule_ref.clone(),
            )
            .unwrap();

            // the filter is extracted into the index condition either way
            assert_eq!(
                index_condition_ref.lock().clone(),
                Some(IndexCondition {
                    conditions: vec![Condition::Equal(
                        "kubernetes_namespace_name".to_string(),
                        "ziox".to_string(),
                    )],
                }),
                "index condition mismatch for column_store_fields: {column_store_fields:?}"
            );
            assert_eq!(
                index_optimizer_rule_ref.lock().clone(),
                expected,
                "optimize mode mismatch for column_store_fields: {column_store_fields:?}"
            );
        }
    }

    fn cached_file(id: i64, bloom_ver: i64) -> FileKey {
        let mut file = FileKey::new(
            id,
            String::new(),
            format!("files/o/logs/s/2026/10/25/09/{id}.vix"),
            FileMeta::default(),
            false,
        );
        file.meta.bloom_ver = bloom_ver;
        file
    }

    #[test]
    fn refresh_cached_bloom_vers_adopts_the_leaders_current_versions() {
        // 1: cached while pending, since stamped; 2: restamped by heal;
        // 3: already current; 4: version unknown to the leader (dump row);
        // 5: not listed (segment pseudo-file ids are negative and absent).
        let mut files = vec![
            cached_file(1, 0),
            cached_file(2, 700),
            cached_file(3, 800),
            cached_file(4, 900),
            cached_file(5, 0),
        ];
        let ids = [1, 2, 3, 4, -9];
        let vers = [600, 800, 800, infra::file_list::BLOOM_VER_UNKNOWN, 42];
        assert_eq!(refresh_cached_bloom_vers(&mut files, &ids, &vers), 2);
        let got: Vec<i64> = files.iter().map(|f| f.meta.bloom_ver).collect();
        assert_eq!(got, vec![600, 800, 800, 900, 0]);
    }

    #[test]
    fn refresh_cached_bloom_vers_ignores_missing_or_misaligned_lists() {
        let mut files = vec![cached_file(1, 0), cached_file(2, 700)];
        // older leader: no list at all
        assert_eq!(refresh_cached_bloom_vers(&mut files, &[1, 2], &[]), 0);
        // misaligned: never pair an id with another file's version
        assert_eq!(refresh_cached_bloom_vers(&mut files, &[1, 2], &[600]), 0);
        let got: Vec<i64> = files.iter().map(|f| f.meta.bloom_ver).collect();
        assert_eq!(got, vec![0, 700]);
    }
}

#[cfg(test)]
mod setup_tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use arrow_schema::Schema;
    use config::meta::search::StorageType;
    use tokio::sync::{Semaphore, oneshot};

    use super::{Error, run_setup_branches};
    use crate::service::search::datafusion::storage::file_list;

    async fn register(trace: &str) {
        for suffix in ["", "-storage", "-wal"] {
            file_list::set(
                &format!("{trace}{suffix}"),
                trace,
                "setup",
                "vix",
                vec![],
                StorageType::Memory,
                Arc::new(Schema::empty()),
            )
            .await;
        }
    }

    fn registered(trace: &str) -> bool {
        ["", "-storage", "-wal"].iter().any(|suffix| {
            file_list::get(&format!("{trace}{suffix}/schema=setup/format=vix")).is_ok()
        })
    }

    async fn pending_branch(
        trace: &str,
        permits: Arc<Semaphore>,
        ready: oneshot::Sender<()>,
    ) -> Result<(), Error> {
        let _owned = lease(trace, permits).await;
        ready.send(()).unwrap();
        std::future::pending().await
    }
    #[tokio::test]
    async fn setup_parallel_overlaps_both_branches() {
        let barrier = tokio::sync::Barrier::new(2);
        let run = run_setup_branches(
            "setup-overlap",
            true,
            async {
                barrier.wait().await;
                Ok::<_, Error>(41)
            },
            async {
                barrier.wait().await;
                Ok::<_, Error>(42)
            },
        );
        // A serial implementation deadlocks at the barrier, rather than
        // passing because a noisy wall-clock measurement looked faster.
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(5), run)
                .await
                .unwrap()
                .unwrap(),
            (41, 42),
        );
    }

    #[tokio::test]
    async fn setup_serial_rollback_preserves_order() {
        let complete = AtomicBool::new(false);
        let (first, second) = run_setup_branches(
            "setup-serial",
            false,
            async {
                tokio::task::yield_now().await;
                complete.store(true, Ordering::Release);
                Ok::<_, Error>(1)
            },
            async {
                assert!(complete.load(Ordering::Acquire));
                Ok::<_, Error>(2)
            },
        )
        .await
        .unwrap();
        assert_eq!((first, second), (1, 2));
    }

    #[tokio::test]
    async fn setup_parallel_matches_serial_local_object_results() {
        use object_store::{ObjectStoreExt, memory::InMemory, path::Path};

        let store = InMemory::new();
        let persisted = Path::from("persisted");
        let segment = Path::from("segment");
        store
            .put(&persisted, bytes::Bytes::from_static(b"[3,0,7]").into())
            .await
            .unwrap();
        store
            .put(&segment, bytes::Bytes::from_static(b"[1,4,0]").into())
            .await
            .unwrap();
        let mut results = Vec::new();
        for parallel in [false, true] {
            let (persisted, segment) = run_setup_branches(
                "setup-local-results",
                parallel,
                async {
                    Ok::<_, Error>(store.get(&persisted).await.unwrap().bytes().await.unwrap())
                },
                async { Ok::<_, Error>(store.get(&segment).await.unwrap().bytes().await.unwrap()) },
            )
            .await
            .unwrap();
            let persisted: Vec<u64> = serde_json::from_slice(&persisted).unwrap();
            let segment: Vec<u64> = serde_json::from_slice(&segment).unwrap();
            let setup = super::PersistedSetup {
                index_result: Some(super::MultiResult::Histogram(persisted)),
                scan_stats: super::ScanStats {
                    files: 2,
                    records: 10,
                    idx_took: 9,
                    ..Default::default()
                },
                ..Default::default()
            }
            .with_segments(super::SegmentSetup {
                histogram: Some(segment),
                scan_stats: super::ScanStats {
                    files: 3,
                    records: 20,
                    idx_took: 4,
                    ..Default::default()
                },
                ..Default::default()
            })
            .unwrap();
            assert_eq!(setup.scan_stats.files, 5);
            assert_eq!(setup.scan_stats.records, 30);
            assert_eq!(setup.scan_stats.idx_took, 9);
            let Some(super::MultiResult::Histogram(counts)) = setup.index_result else {
                panic!("missing histogram")
            };
            results.push(counts);
        }
        assert_eq!(results, vec![vec![4, 4, 7], vec![4, 4, 7]]);
    }

    #[tokio::test]
    async fn setup_failure_drops_sibling_and_clears_registrations() {
        for fail_persisted in [false, true] {
            let trace = format!("setup-failure-{fail_persisted}");
            let permits = Arc::new(Semaphore::new(1));
            let (ready_tx, ready_rx) = oneshot::channel();
            let pending = pending_branch(&trace, permits.clone(), ready_tx);
            let failing = async {
                ready_rx.await.unwrap();
                Err::<(), _>(Error::Message("injected setup failure".into()))
            };
            let result = if fail_persisted {
                run_setup_branches(&trace, true, failing, pending).await
            } else {
                run_setup_branches(&trace, true, pending, failing).await
            };
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("injected setup failure")
            );
            assert_eq!(permits.available_permits(), 1);
            assert!(!registered(&trace));
        }
    }

    #[tokio::test]
    async fn setup_serial_failure_never_starts_segments() {
        let started = AtomicBool::new(false);
        let result = run_setup_branches(
            "setup-serial-failure",
            false,
            async { Err::<(), _>(Error::Message("persisted failed".into())) },
            async {
                started.store(true, Ordering::Relaxed);
                Ok::<_, Error>(())
            },
        )
        .await;
        assert!(result.is_err());
        assert!(!started.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn setup_cancellation_drops_both_active_branches() {
        let trace = "setup-cancel-active";
        let permits = Arc::new(Semaphore::new(2));
        let (first_tx, first_rx) = oneshot::channel();
        let (second_tx, second_rx) = oneshot::channel();
        let mut run = Box::pin(run_setup_branches(
            trace,
            true,
            pending_branch(trace, permits.clone(), first_tx),
            pending_branch(trace, permits.clone(), second_tx),
        ));
        tokio::select! {
            result = &mut run => panic!("setup unexpectedly completed: {result:?}"),
            _ = async { first_rx.await.unwrap(); second_rx.await.unwrap(); } => {}
        }
        assert_eq!(permits.available_permits(), 0);
        assert!(registered(trace));
        drop(run);
        assert_eq!(permits.available_permits(), 2);
        assert!(!registered(trace));
    }

    #[tokio::test]
    async fn setup_cancellation_after_one_branch_completed_cleans_both() {
        for parallel in [false, true] {
            let trace = format!("setup-cancel-completed-{parallel}");
            let permits = Arc::new(Semaphore::new(2));
            let (ready_tx, ready_rx) = oneshot::channel();
            let completed = async { Ok::<_, Error>(lease(&trace, permits.clone()).await) };
            let pending = pending_branch(&trace, permits.clone(), ready_tx);
            let mut run = Box::pin(run_setup_branches(&trace, parallel, completed, pending));
            tokio::select! {
                result = &mut run => panic!("setup unexpectedly completed: {result:?}"),
                _ = ready_rx => {}
            }
            assert_eq!(permits.available_permits(), 0);
            drop(run);
            assert_eq!(permits.available_permits(), 2);
            assert!(!registered(&trace));
        }
    }

    #[tokio::test]
    async fn setup_success_keeps_query_owned_registrations() {
        for parallel in [false, true] {
            let trace = format!("setup-success-{parallel}");
            run_setup_branches(
                &trace,
                parallel,
                async {
                    register(&trace).await;
                    Ok::<_, Error>(())
                },
                async { Ok::<_, Error>(()) },
            )
            .await
            .unwrap();
            assert!(registered(&trace));
            file_list::clear(&trace);
        }
    }

    #[derive(Debug)]
    struct RegisteredLease {
        trace: String,
        _permit: tokio::sync::OwnedSemaphorePermit,
    }

    impl Drop for RegisteredLease {
        fn drop(&mut self) {
            assert!(
                registered(&self.trace),
                "cleanup preceded branch resource drop"
            );
        }
    }

    async fn lease(trace: &str, permits: Arc<Semaphore>) -> RegisteredLease {
        register(trace).await;
        RegisteredLease {
            trace: trace.to_string(),
            _permit: permits.acquire_owned().await.unwrap(),
        }
    }

    #[tokio::test]
    async fn setup_completed_results_drop_before_failure_cleanup() {
        for parallel in [false, true] {
            let trace = format!("setup-completed-failure-{parallel}");
            let permits = Arc::new(Semaphore::new(2));
            let (ready_tx, ready_rx) = oneshot::channel();
            let completed = async {
                let owned = lease(&trace, permits.clone()).await;
                ready_tx.send(()).unwrap();
                Ok::<_, Error>(owned)
            };
            let failing = async {
                ready_rx.await.unwrap();
                let _owned = lease(&trace, permits.clone()).await;
                Err::<(), _>(Error::ResourceError(
                    "injected segment budget failure".into(),
                ))
            };
            let result = run_setup_branches(&trace, parallel, completed, failing).await;
            assert!(matches!(result, Err(Error::ResourceError(_))));
            assert_eq!(permits.available_permits(), 2);
            assert!(!registered(&trace));
        }
    }

    #[tokio::test]
    async fn setup_post_join_failure_and_cancellation_clear_owned_sessions_only() {
        for cancel in [false, true] {
            let trace = format!("setup-post-join-{cancel}");
            let other = format!("{trace}-neighbor");
            register(&other).await;
            let permits = Arc::new(Semaphore::new(2));
            let (ready_tx, ready_rx) = oneshot::channel();
            let work = async {
                // This is the same ownership boundary as search: the guard
                // remains armed across all fallible union/planning operations.
                let _cleanup = super::QueryFilesCleanup(Some(&trace));
                let (_persisted, _segments) = run_setup_branches(
                    &trace,
                    true,
                    async { Ok::<_, Error>(lease(&trace, permits.clone()).await) },
                    async { Ok::<_, Error>(lease(&trace, permits.clone()).await) },
                )
                .await?;
                ready_tx.send(()).unwrap();
                if cancel {
                    std::future::pending::<()>().await;
                }
                Err::<(), _>(Error::Message("injected union planning failure".into()))
            };
            let mut work = Box::pin(work);
            if cancel {
                tokio::select! {
                    result = &mut work => panic!("unexpected completion: {result:?}"),
                    result = ready_rx => result.unwrap(),
                }
                assert_eq!(permits.available_permits(), 0);
                drop(work);
            } else {
                assert!(work.await.is_err());
            }
            assert_eq!(permits.available_permits(), 2);
            assert!(!registered(&trace));
            assert!(registered(&other));
            file_list::clear(&other);
        }
    }

    #[test]
    fn setup_histogram_merge_rejects_overflow_and_shape_changes() {
        for (persisted, segment) in [(vec![u64::MAX], vec![1]), (vec![1], vec![1, 2])] {
            let setup = super::PersistedSetup {
                index_result: Some(super::MultiResult::Histogram(persisted)),
                ..Default::default()
            };
            assert!(
                setup
                    .with_segments(super::SegmentSetup {
                        histogram: Some(segment),
                        ..Default::default()
                    })
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn setup_parallel_union_matches_serial_and_exact_rows() {
        use arrow::{
            array::{Int64Array, StringArray},
            record_batch::RecordBatch,
        };
        use arrow_schema::{DataType, Field};
        use datafusion::{
            datasource::{MemTable, TableProvider},
            physical_plan::collect_partitioned,
            prelude::SessionContext,
        };
        use object_store::{ObjectStoreExt, memory::InMemory, path::Path};

        async fn read_batch(store: &InMemory, path: &Path, schema: Arc<Schema>) -> RecordBatch {
            let bytes = store.get(path).await.unwrap().bytes().await.unwrap();
            let rows: Vec<(i64, Option<String>, i64)> = serde_json::from_slice(&bytes).unwrap();
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
                    Arc::new(StringArray::from_iter(rows.iter().map(|r| r.1.as_deref()))),
                    Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.2))),
                ],
            )
            .unwrap()
        }

        fn table(batch: RecordBatch) -> Arc<dyn TableProvider> {
            Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap())
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("service", DataType::Utf8, true),
            Field::new("value", DataType::Int64, false),
        ]));
        let store = InMemory::new();
        let persisted_path = Path::from("persisted");
        let segment_path = Path::from("segment");
        store
            .put(
                &persisted_path,
                bytes::Bytes::from_static(br#"[[100,"api",3],[101,null,0],[102,"",7]]"#).into(),
            )
            .await
            .unwrap();
        store
            .put(
                &segment_path,
                bytes::Bytes::from_static(br#"[[103,"api",1],[104,null,4],[105,"",0]]"#).into(),
            )
            .await
            .unwrap();
        let persisted_batch = read_batch(&store, &persisted_path, schema.clone()).await;
        let segment_batch = read_batch(&store, &segment_path, schema.clone()).await;
        let queries = [
            "SELECT * FROM rt ORDER BY ts",
            "SELECT service, count(*) AS n, sum(value) AS total FROM rt GROUP BY service ORDER BY service NULLS FIRST",
            "SELECT ts, service FROM rt WHERE value > 0 ORDER BY ts DESC LIMIT 2 OFFSET 1",
            "SELECT count(*) FROM rt WHERE ts >= 101 AND ts < 105",
            "SELECT * FROM rt WHERE ts < 0",
            "SELECT * FROM rt LIMIT 0",
        ];
        // Cover persisted-only, segment-only, empty, and mixed tickets.
        for (has_persisted, has_segment) in
            [(true, true), (true, false), (false, true), (false, false)]
        {
            let expected: Vec<_> = [
                has_persisted.then_some(persisted_batch.clone()),
                has_segment.then_some(segment_batch.clone()),
            ]
            .into_iter()
            .flatten()
            .collect();
            let oracle = SessionContext::new();
            oracle
                .register_table(
                    "rt",
                    Arc::new(MemTable::try_new(schema.clone(), vec![expected.clone()]).unwrap()),
                )
                .unwrap();
            let mut expected_results = Vec::new();
            for sql in queries {
                expected_results.push(oracle.sql(sql).await.unwrap().collect().await.unwrap());
            }
            for parallel in [false, true] {
                let (ready_tx, ready_rx) = oneshot::channel();
                let persisted = async {
                    if parallel {
                        ready_rx.await.unwrap();
                    }
                    let batch = read_batch(&store, &persisted_path, schema.clone()).await;
                    Ok::<_, Error>(super::PersistedSetup {
                        tables: if has_persisted {
                            vec![table(batch)]
                        } else {
                            vec![]
                        },
                        storage_shortfall: Some("preserved truncation warning".into()),
                        ..Default::default()
                    })
                };
                let segments = async {
                    let batch = read_batch(&store, &segment_path, schema.clone()).await;
                    let result = super::SegmentSetup {
                        tables: if has_segment {
                            vec![table(batch)]
                        } else {
                            vec![]
                        },
                        ..Default::default()
                    };
                    // Force segment-first completion in parallel mode. Serial
                    // execution has already dropped the unused receiver.
                    let _ = ready_tx.send(());
                    Ok::<_, Error>(result)
                };
                let (persisted, segments) =
                    run_setup_branches("setup-union", parallel, persisted, segments)
                        .await
                        .unwrap();
                let merged = persisted.with_segments(segments).unwrap();
                assert_eq!(
                    merged.storage_shortfall.as_deref(),
                    Some("preserved truncation warning")
                );
                let union = Arc::new(super::NewUnionTable::new(schema.clone(), merged.tables));
                let ctx = SessionContext::new();
                let plan = union.scan(&ctx.state(), None, &[], None).await.unwrap();
                let partitions = collect_partitioned(plan, ctx.task_ctx()).await.unwrap();
                let batches: Vec<_> = partitions.into_iter().flatten().collect();
                assert_eq!(
                    batches, expected,
                    "persisted must be the first union partition"
                );
                // Flight supplies an already-projected schema for an empty union.
                // The physical empty result above covers that boundary; asking SQL
                // to project this generic empty provider again is a different path.
                if expected.is_empty() {
                    continue;
                }
                ctx.register_table("rt", union).unwrap();
                for (sql, expected) in queries.iter().zip(&expected_results) {
                    let actual = ctx.sql(sql).await.unwrap().collect().await.unwrap();
                    assert_eq!(&actual, expected, "parallel={parallel}, sql={sql}");
                }
            }
        }
    }
}
