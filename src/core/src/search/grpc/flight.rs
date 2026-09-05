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

#[tracing::instrument(name = "service:search:grpc:flight:do_get::search", skip_all, fields(org_id = req.query_identifier.org_id))]
pub async fn search(
    trace_id: &str,
    req: &FlightSearchRequest,
) -> Result<(SessionContext, Arc<dyn ExecutionPlan>, ScanStats), Error> {
    let cfg = get_config();

    let org_id = req.query_identifier.org_id.to_string();
    let stream_type = StreamType::from(req.query_identifier.stream_type.as_str());
    let work_group = req.super_cluster_info.work_group.clone();

    let trace_id = Arc::new(trace_id.to_string());
    log::info!("[trace_id {trace_id}] flight->search: start");

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
    let fst_fields = get_stream_setting_fts_fields(&stream_settings)
        .into_iter()
        .filter(|v| latest_schema_map.contains_key(v))
        .collect_vec();
    // the vix index term-indexes every string field's raw values and every
    // numeric/bool field's canonical value forms: filter/condition
    // extraction (IndexRule) is eligible for all of them (minus the internal
    // columns), carrying the registry type so the rule can gate predicate
    // shapes per type (string fields keep every shape; numeric/bool fields
    // serve =/!=/IN/IS NOT NULL with value-normalized literals). Even for
    // files written before numeric value terms existed, eligibility alone
    // pays: the per-file capability probe classifies files without the key
    // as Absent (eliminated — no IO) and files with it as FtsOnly (skip +
    // filter-back) instead of bypassing the index entirely. Float16 is
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
    let mut tables = Vec::new();
    let mut scan_stats = ScanStats::new();
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

    // search in object storage
    let mut metadata_count_file_list = Vec::new();
    let mut index_file_list = Vec::new();
    // the precomputed aggregate fast-path result over index_file_list
    let mut index_result: Option<MultiResult> = None;
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
        let (tbls, stats, _) = match super::storage::search(
            query_params.clone(),
            latest_schema.clone(),
            &file_list,
            empty_exec.sorted_by_time(),
            collect_file_stats,
            file_stats_cache.clone(),
            index_condition.clone(),
            fst_fields.clone(),
            bloom_indexed_fields.clone(),
            storage_idx_optimize_rule,
        )
        .await
        {
            Ok(v) => v,
            Err(e) => {
                // clear session data
                super::super::datafusion::storage::file_list::clear(&trace_id);
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
    }

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
                    super::super::datafusion::storage::file_list::clear(&trace_id);
                    log::error!(
                        "[trace_id {trace_id}] flight->search: segment histogram failed: {e}"
                    );
                    return Err(e);
                }
            };
            if let Err(e) = merge_histogram_result(&mut index_result, histogram) {
                super::super::datafusion::storage::file_list::clear(&trace_id);
                return Err(e);
            }
            scan_stats.add(&stats);
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
                    super::super::datafusion::storage::file_list::clear(&trace_id);
                    log::error!("[trace_id {trace_id}] flight->search: search segments error: {e}");
                    return Err(e);
                }
            };
            tables.extend(tbls);
            scan_stats.add(&stats);
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
                // clear session data
                super::super::datafusion::storage::file_list::clear(&trace_id);
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

    Ok((ctx, physical_plan, scan_stats))
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
/// a `.vxi` sidecar. Native docs-column equality supports SimpleHistogram
/// and SimpleSelect; ALL multi-histograms remain on DataFusion until their
/// extracted timestamp coordinates match collection semantics.
fn data_only_vix_capable(
    idx_optimize_rule: &Option<IndexOptimizeMode>,
    index_condition: Option<&IndexCondition>,
) -> bool {
    let Some(condition) = index_condition else {
        return false;
    };
    match idx_optimize_rule {
        Some(IndexOptimizeMode::SimpleHistogram(..)) => {
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
}
