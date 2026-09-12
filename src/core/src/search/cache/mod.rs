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

use std::{str::FromStr, sync::Arc};

use chrono::Utc;
#[cfg(feature = "vectorscan")]
use config::meta::projections::ProjectionColumnMapping;
use config::{
    TIMESTAMP_COL_NAME,
    cluster::LOCAL_NODE,
    get_config,
    meta::{
        dashboards::usage_report::DashboardInfo,
        function::RESULT_ARRAY_SKIP_VRL,
        search::{self, PARTIAL_ERROR_RESPONSE_MESSAGE, ResponseTook},
        self_reporting::usage::{RequestStats, UsageType},
        sql::{OrderBy, resolve_stream_names},
        stream::StreamType,
    },
    utils::{
        base64,
        hash::Sum64,
        json,
        sql::{is_complex_query, is_eligible_for_histogram},
        time::{format_duration, now_micros, second_micros},
    },
};
use infra::{
    cache::{file_data::disk::QUERY_RESULT_CACHE, meta::ResultCacheMeta},
    errors::Error,
};
#[cfg(feature = "enterprise")]
use o2_enterprise::enterprise::common::config::get_config as get_o2_config;
#[cfg(feature = "vectorscan")]
use o2_enterprise::enterprise::re_patterns::get_pattern_manager;
use proto::cluster_rpc::SearchQuery;
use result_utils::get_ts_value;
use tracing::Instrument;

use crate::{
    common::{
        meta::search::{CachedQueryResponse, MultiCachedQueryResponse, QueryDelta},
        utils::{functions, http::get_work_group},
    },
    service::{
        search::{
            self as SearchService,
            cache::{cacher::check_cache, result_utils::extract_timestamp_range},
            init_vrl_runtime,
            inspector::{SearchInspectorFieldsBuilder, search_inspector_fields},
            sql::{RE_HISTOGRAM, RE_SELECT_FROM, Sql, SqlPreparation},
        },
        self_reporting::{http_report_metrics, report_request_usage_stats},
    },
};

pub mod cacher;
pub mod multi;
pub mod result_utils;

// v5: every stored range is complete half-open coverage, including LIMIT ties
// and full histogram buckets. v4 mixed exclusive request ends with inclusive
// observed-row ends and could claim coverage for truncated results.
// Retains v4's full per-record `_source` semantics for SELECT *.
const CACHE_VERSION: &str = "v5";

#[tracing::instrument(name = "service:search:cacher:search", skip_all)]
#[allow(clippy::too_many_arguments)]
pub async fn search(
    trace_id: &str,
    org_id: &str,
    stream_type: StreamType,
    user_id: Option<String>,
    in_req: &search::Request,
    range_error: String,
    is_http2_streaming: bool,
    dashboard_info: Option<DashboardInfo>,
    is_multi_stream_search: bool,
) -> Result<search::Response, Error> {
    let start = std::time::Instant::now();
    let started_at = Utc::now().timestamp_micros();
    let cfg = get_config();
    // result cache can be enable only when its from the start
    let use_cache = if in_req.query.from == 0 {
        in_req.use_cache
    } else {
        false
    };

    let mut req = in_req.clone();

    // check the original query function first
    let mut query_fn = req
        .query
        .query_fn
        .as_ref()
        .map(|v| match base64::decode_url(v) {
            Ok(v) => v,
            Err(_) => v.to_string(),
        });
    let backup_query_fn = query_fn.clone();
    let is_result_array_skip_vrl = query_fn
        .as_ref()
        .map(|v| is_result_array_skip_vrl(v))
        .unwrap_or(false);
    if is_result_array_skip_vrl {
        query_fn = None;
    }

    // Result caching check start
    let (mut c_resp, should_exec_query, prepared) =
        prepare_cache_response(trace_id, org_id, stream_type, &mut req, use_cache).await?;
    let file_path = c_resp.file_path.clone();

    // get the modified original sql from req
    let origin_sql = in_req.query.sql.clone();
    let is_complex_query = is_complex_query(&origin_sql).unwrap_or(true);
    let (stream_name, all_streams) = match resolve_stream_names(&origin_sql) {
        // result cache doesn't support multiple stream names
        Ok(v) => (v[0].clone(), v.join(",")),
        Err(e) => {
            return Err(Error::Message(e.to_string()));
        }
    };

    // No cache data present, add delta for full query
    if !c_resp.has_cached_data && c_resp.deltas.is_empty() {
        c_resp.deltas.push(QueryDelta {
            delta_start_time: req.query.start_time,
            delta_end_time: req.query.end_time,
        });
    } else if use_cache && c_resp.deltas.is_empty() {
        log::info!("[trace_id {trace_id}] Query hit full cache");
    }

    #[allow(unused_mut)]
    let mut search_role = "leader".to_string();
    #[cfg(feature = "enterprise")]
    if get_o2_config().super_cluster.enabled {
        search_role = "super".to_string();
    }

    // Result caching check ends, start search
    let cache_took = start.elapsed().as_millis() as usize;
    let mut results = Vec::new();
    let mut work_group_set = Vec::new();
    let mut uncached_results_have_hits = false;
    let mut res = if !should_exec_query {
        // no need to search, just merge the cached response
        // TODO: which case we don't need to search?
        merge_response(
            trace_id,
            &mut c_resp
                .cached_response
                .iter()
                .map(|r| r.cached_response.clone())
                .collect(),
            &mut vec![],
            &c_resp.ts_column,
            c_resp.limit,
            c_resp.is_descending,
            c_resp.took,
            c_resp.order_by,
        )
    } else {
        // run the searches
        if let Some(vrl_function) = &query_fn
            && !vrl_function.trim().ends_with('.')
        {
            query_fn = Some(format!("{vrl_function} \n ."));
        }
        req.query.query_fn = query_fn;

        c_resp.deltas.sort();
        c_resp.deltas.dedup();
        let total = (req.query.end_time - req.query.start_time) as usize;
        let deltas_total: usize = c_resp
            .deltas
            .iter()
            .map(|d| (d.delta_end_time - d.delta_start_time) as usize)
            .sum();

        log::info!(
            "{}",
            search_inspector_fields(
                format!(
                    "[trace_id {trace_id}] Qeury deltas are: {:?}",
                    c_resp.deltas
                ),
                SearchInspectorFieldsBuilder::new()
                    .trace_id(trace_id.to_string())
                    .node_name(LOCAL_NODE.name.clone())
                    .component("cacher:search deltas".to_string())
                    .search_role(search_role.clone())
                    .duration(start.elapsed().as_millis() as usize)
                    .desc(format!(
                        "search cacher search from {} reduce to {}",
                        format_duration(total as u64 / 1000),
                        format_duration(deltas_total as u64 / 1000)
                    ))
                    .build()
            )
        );

        log::info!(
            "[trace_id {trace_id}] Query original start time: {}, end time: {}",
            req.query.start_time,
            req.query.end_time
        );

        let mut tasks = Vec::new();
        let partition_num = c_resp.deltas.len();
        // fire all the deltas search requests in parallel
        for (i, delta) in c_resp.deltas.into_iter().enumerate() {
            let mut req = req.clone();
            let org_id = org_id.to_string();
            let trace_id = if partition_num == 1 {
                trace_id.to_string()
            } else {
                format!("{trace_id}-{i}")
            };
            let user_id = user_id.clone();
            let prepared = prepared.clone();

            let enter_span = tracing::span::Span::current();
            let guard_trace_id = trace_id.clone();
            // Abort-on-drop (#37): this future being dropped is how a client
            // disconnect reaches us (the oneshot heartbeat stream owns it) —
            // a bare JoinHandle would DETACH every delta search and burn it
            // to completion. Guarded, the drop aborts each delta, whose own
            // guards cascade to the flight leaders and followers.
            let task = crate::service::search::utils::AbortOnDrop::new(
                tokio::task::spawn(
                    (async move {
                        let trace_id = trace_id.clone();
                        req.query.start_time = delta.delta_start_time;
                        req.query.end_time = delta.delta_end_time;

                        let cfg = get_config();
                        if cfg.common.result_cache_enabled
                            && cfg.common.print_key_sql
                            && c_resp.has_cached_data
                        {
                            log::info!(
                                "[trace_id {trace_id}] Query new start time: {}, end time: {}",
                                req.query.start_time,
                                req.query.end_time
                            );
                        }

                        SearchService::search_impl(
                            &trace_id,
                            &org_id,
                            stream_type,
                            user_id,
                            &req,
                            Some(prepared),
                        )
                        .await
                    })
                    .instrument(enter_span),
                ),
                guard_trace_id,
            );
            tasks.push(task);
        }

        for mut task in tasks {
            results.push(
                task.join()
                    .await
                    .map_err(|e| Error::Message(e.to_string()))??,
            );
        }
        for res in &results {
            work_group_set.push(res.work_group.clone());
        }
        // merge the cached response and the search response
        if c_resp.has_cached_data {
            merge_response(
                trace_id,
                &mut c_resp
                    .cached_response
                    .iter()
                    .map(|r| r.cached_response.clone())
                    .collect(),
                &mut results,
                &c_resp.ts_column,
                c_resp.limit,
                c_resp.is_descending,
                c_resp.took,
                c_resp.order_by,
            )
        } else {
            uncached_results_have_hits = results.first().is_some_and(|res| !res.hits.is_empty())
                || results.last().is_some_and(|res| !res.hits.is_empty());
            let mut reps = std::mem::take(&mut results[0]);
            sort_response(
                c_resp.is_descending,
                &mut reps,
                &c_resp.ts_column,
                &c_resp.order_by,
            );
            reps
        }
    };

    // search is done
    let took_time = start.elapsed().as_secs_f64();
    log::info!(
        "{}",
        search_inspector_fields(
            format!(
                "[trace_id {trace_id}] search for cache is done, took: {} ms",
                start.elapsed().as_millis()
            ),
            SearchInspectorFieldsBuilder::new()
                .trace_id(trace_id.to_string())
                .node_name(LOCAL_NODE.name.clone())
                .component("summary".to_string())
                .search_role(search_role)
                .sql(req.query.sql.clone())
                .time_range((
                    req.query.start_time.to_string(),
                    req.query.end_time.to_string()
                ))
                .scan_size(res.scan_size as usize)
                .scan_records(res.scan_records as usize)
                .data_records(res.hits.len())
                .duration(start.elapsed().as_millis() as usize)
                .build()
        )
    );

    let work_group = get_work_group(work_group_set);

    let search_type = req
        .search_type
        .map(|t| t.to_string())
        .unwrap_or("".to_string());
    let search_group = work_group.clone().unwrap_or("".to_string());
    http_report_metrics(
        start,
        org_id,
        stream_type,
        "200",
        "_search",
        &search_type,
        &search_group,
    );

    res.set_trace_id(trace_id.to_string());
    res.set_took(took_time as usize);
    res.set_cache_took(cache_took);

    if is_complex_query
        && res.histogram_interval.is_none()
        && !c_resp.ts_column.is_empty()
        && c_resp.histogram_interval > 0
    {
        res.histogram_interval = Some(c_resp.histogram_interval);
    }

    let num_fn = req.query.query_fn.is_some() as u16;
    let req_stats = RequestStats {
        records: res.hits.len() as i64,
        response_time: took_time,
        size: res.scan_size as f64,
        scan_files: if res.scan_files > 0 {
            Some(res.scan_files as i64)
        } else {
            None
        },
        request_body: Some(req.query.sql.clone()),
        function: req.query.query_fn.clone(),
        user_email: user_id,
        min_ts: Some(req.query.start_time),
        max_ts: Some(req.query.end_time),
        cached_ratio: Some(res.cached_ratio),
        search_type: req.search_type,
        search_event_context: req.search_event_context.clone(),
        trace_id: Some(trace_id.to_string()),
        took_wait_in_queue: Some(res.took_detail.wait_in_queue),
        work_group,
        result_cache_ratio: Some(res.result_cache_ratio),
        dashboard_info,
        peak_memory_usage: res.peak_memory_usage,
        ..Default::default()
    };
    report_request_usage_stats(
        req_stats,
        org_id,
        &all_streams,
        stream_type,
        UsageType::Search,
        num_fn,
        started_at,
    )
    .await;

    if res.is_partial {
        let partial_err = PARTIAL_ERROR_RESPONSE_MESSAGE;
        res.function_error = if res.function_error.is_empty() {
            vec![partial_err.to_string()]
        } else {
            // check if the error is about the stream not found
            let mut skip_warning = false;
            for err in &res.function_error {
                if err.starts_with("Stream not found") {
                    skip_warning = true;
                    break;
                }
            }
            if !skip_warning {
                res.function_error.push(partial_err.to_string());
            }
            res.function_error
        }
    }
    if !range_error.is_empty() {
        res.is_partial = true;
        let range_error_str = range_error.clone();
        res.function_error = if res.function_error.is_empty() {
            vec![range_error_str]
        } else {
            res.function_error.push(range_error_str);
            res.function_error
        };
        res.new_start_time = Some(req.query.start_time);
        res.new_end_time = Some(req.query.end_time);
    }

    res.is_histogram_eligible = is_eligible_for_histogram(&req.query.sql, is_multi_stream_search)
        .ok()
        .map(|(is_eligible, _)| is_eligible);

    // There are 3 types of partial responses:
    // 1. VRL error
    // 2. Super cluster error
    // 3. Range error (max_query_limit)

    // result cache save changes start
    let should_cache_results = cfg.common.result_cache_enabled
        && !is_http2_streaming
        && should_exec_query
        && c_resp.cache_query_response
        && res.new_start_time.is_none()
        && res.new_end_time.is_none()
        && res.function_error.is_empty()
        && !res.hits.is_empty();
    log::info!(
        "[trace_id {trace_id}] should_cache_results: {should_cache_results}, is_http2_streaming: {is_http2_streaming}, hits: {}",
        res.hits.len()
    );
    if should_cache_results
        && (uncached_results_have_hits
            || results.first().is_some_and(|res| !res.hits.is_empty())
            || results.last().is_some_and(|res| !res.hits.is_empty()))
    {
        // A histogram without timestamp-first ordering may be an arbitrary
        // top-N subset even though the final response was sorted by timestamp.
        let is_histogram_non_ts_order = c_resp.histogram_interval > 0
            && prepared.order_by.first().is_none_or(|(field, _)| {
                !result_utils::is_timestamp_field(field, &c_resp.ts_column)
            });

        write_results(
            trace_id,
            &c_resp.ts_column,
            req.query.start_time,
            req.query.end_time,
            c_resp.limit,
            res.clone(),
            file_path,
            is_complex_query,
            c_resp.is_descending,
            req.clear_cache,
            is_histogram_non_ts_order,
        )
        .await;
    }
    // result cache save changes Ends

    #[cfg(feature = "vectorscan")]
    crate::service::search::cache::apply_regex_to_response(
        &req,
        org_id,
        &stream_name,
        stream_type,
        &mut res,
        trace_id,
        "search_fn",
    )
    .await?;

    if is_result_array_skip_vrl {
        res.hits = apply_vrl_to_response(backup_query_fn, &mut res, org_id, &stream_name, trace_id);
        return Ok(res);
    }

    Ok(res)
}

#[tracing::instrument(name = "service:search:cacher:prepare_cache_response", skip_all)]
pub async fn prepare_cache_response(
    trace_id: &str,
    org_id: &str,
    stream_type: StreamType,
    req: &mut search::Request,
    use_cache: bool,
) -> Result<(MultiCachedQueryResponse, bool, Arc<Sql>), Error> {
    let mut origin_sql = req.query.sql.clone();
    let is_complex_query = is_complex_query(&origin_sql).unwrap_or(true);
    let stream_name = match resolve_stream_names(&origin_sql) {
        // result cache doesn't support multiple stream names
        Ok(v) => {
            if v.is_empty() {
                return Err(Error::Message("Stream name is empty".to_string()));
            } else {
                v[0].clone()
            }
        }
        Err(e) => {
            return Err(Error::Message(e.to_string()));
        }
    };

    let mut query_fn = req
        .query
        .query_fn
        .as_ref()
        .map(|v| match base64::decode_url(v) {
            Ok(v) => v,
            Err(_) => v.to_string(),
        });
    let is_result_array_skip_vrl = query_fn
        .as_ref()
        .map(|v| is_result_array_skip_vrl(v))
        .unwrap_or(false);
    if is_result_array_skip_vrl {
        query_fn = None;
    }

    let action = req
        .query
        .action_id
        .as_ref()
        .and_then(|v| svix_ksuid::Ksuid::from_str(v).ok());

    // Source-time coverage cannot be recovered from payloads transformed before
    // caching: they may change timestamps, cardinality, or both. The SkipVRL
    // result-array path is applied after cache merging and remains eligible.
    for fn_name in functions::get_all_transform_keys(org_id).await {
        if req.query.sql.contains(&format!("{fn_name}(")) {
            req.query.uses_zo_fn = true;
            break;
        }
    }
    let has_pre_cache_transform = query_fn.as_ref().is_some_and(|value| !value.is_empty())
        || req
            .query
            .action_id
            .as_ref()
            .is_some_and(|value| !value.is_empty())
        || req.query.uses_zo_fn;

    // Parse SQL first to get metadata needed for normalization
    let query: SearchQuery = req.query.clone().into();
    let preparation = SqlPreparation::new(&query, org_id, stream_type).await?;
    let sql = preparation.finish(&query, org_id, stream_type, req.search_type, false)?;
    // Normalize histogram interval in SQL before computing hash
    // This ensures the hash is consistent regardless of when handle_histogram is called
    if is_complex_query && sql.histogram_interval.is_some() {
        let mut req_time_range = (req.query.start_time, req.query.end_time);
        // If end_time is 0, it means "now" (current time)
        if req_time_range.1 == 0 {
            req_time_range.1 = now_micros();
        }

        let meta_time_range_is_empty = sql.time_range == (0, 0);
        let q_time_range =
            if meta_time_range_is_empty && (req_time_range.0 > 0 || req_time_range.1 > 0) {
                req_time_range
            } else {
                sql.time_range
            };
        crate::service::search::sql::histogram::handle_histogram(
            &mut origin_sql,
            q_time_range,
            req.query.histogram_interval,
        );
    }

    // calculate hash for the query with version (after normalizing histogram interval)
    let mut hash_body = vec![
        CACHE_VERSION.to_string(),
        origin_sql.to_string(),
        req.query.size.to_string(),
    ];
    if let Some(vrl_function) = &query_fn {
        hash_body.push(vrl_function.to_string());
    }
    if let Some(action_id) = action {
        hash_body.push(action_id.to_string());
    }
    if !req.regions.is_empty() {
        hash_body.extend(req.regions.clone());
    }
    if !req.clusters.is_empty() {
        hash_body.extend(req.clusters.clone());
    }
    let mut h = config::utils::hash::gxhash::new();
    let hashed_query = h.sum64(&hash_body.join(","));

    let (mut ts_column, mut is_descending) =
        cacher::get_ts_col_order_by(&sql, TIMESTAMP_COL_NAME, is_complex_query).unwrap_or_default();

    // Refine ts_column for non-complex queries with SELECT * or missing _timestamp
    // Also modify the SQL to add _timestamp to SELECT clause if missing
    let mut added_timestamp = false;
    if !is_complex_query && origin_sql.contains('*') {
        ts_column = TIMESTAMP_COL_NAME.to_string();
    } else if !is_complex_query
        && sql.group_by.is_empty()
        && sql.order_by.is_empty()
        && !origin_sql.contains('*')
        && let Some(caps) = RE_SELECT_FROM.captures(&origin_sql)
        && let Some(cap) = caps.get(1)
    {
        let cap_str = cap.as_str();
        if !cap_str.contains(TIMESTAMP_COL_NAME) {
            // Add _timestamp to SELECT clause
            origin_sql =
                origin_sql.replacen(cap_str, &format!("{TIMESTAMP_COL_NAME},{cap_str}"), 1);
            req.query.sql = origin_sql.clone();
            ts_column = TIMESTAMP_COL_NAME.to_string();
            added_timestamp = true;
        }
    }

    // Refine is_descending for histogram queries with non-timestamp ORDER BY
    is_descending = cacher::refine_is_descending_for_histogram(&sql, &ts_column, is_descending);
    // Consult the original immutable AST, not the normalized execution SQL:
    // normalization can replace function arguments but cannot certify their source.
    let cache_limit = sql.cache_coverage_limit(&ts_column);
    let histogram_coordinates_supported = sql.histogram_interval.is_none_or(|seconds| {
        // RewriteHistogram bins against 2001-01-01 UTC. Our half-open bucket
        // clipping uses Unix-aligned boundaries and unshifted source timestamps.
        seconds.checked_mul(1_000_000).is_some_and(|interval| {
            interval > 0 && 978_307_200_000_000_i64.rem_euclid(interval) == 0
        }) && sql
            .timezone
            .as_deref()
            .is_none_or(|zone| matches!(zone, "" | "UTC" | "Etc/UTC" | "Z" | "+00:00"))
    });
    // HTTP preserves an explicit SQL LIMIT, while streaming can truncate at
    // positive request.size before writing this shared cache. Reject conflicting
    // caps rather than treating the smaller delivered payload as exhausted.
    let cache_eligible = !has_pre_cache_transform
        && cache_limit
            .is_some_and(|limit| req.query.size <= 0 || (limit >= 0 && limit <= req.query.size))
        && histogram_coordinates_supported;

    // Compute the complete file path once for both branches
    let base_file_path = format!("{org_id}/{stream_type}/{stream_name}/{hashed_query}");
    let file_path = cacher::compute_cache_file_path(
        &base_file_path,
        is_complex_query,
        sql.histogram_interval,
        &ts_column,
    );

    let mut should_exec_query = true;

    let mut resp = if use_cache && cache_eligible {
        // if cache is used, we need to check the cache
        check_cache(
            trace_id,
            org_id,
            req,
            &mut origin_sql,
            &file_path,
            is_complex_query,
            &sql,
            &ts_column,
            is_descending,
            &mut should_exec_query,
        )
        .await
    } else {
        // if cache is not used, return the parsed metadata
        MultiCachedQueryResponse {
            ts_column,
            is_aggregate: is_complex_query,
            is_descending,
            order_by: sql.order_by.clone(),
            limit: sql.limit,
            file_path,
            ..Default::default()
        }
    };
    if cache_eligible && let Some(limit) = cache_limit {
        // Merge and writer saturation must use the same actual executed cap;
        // a SQL LIMIT can override a positive request size.
        resp.limit = limit;
    }
    // Cache identity above deliberately uses pre-projection SQL. Finalize only
    // the edits that actually reached execution (check_cache can return early).
    let sql = if req.query.sql != query.sql {
        let before = RE_HISTOGRAM.find(&query.sql).map(|m| m.as_str());
        let after = RE_HISTOGRAM.find(&req.query.sql).map(|m| m.as_str());
        let replacement = before.zip(after).filter(|(before, after)| before != after);
        let mut final_query: SearchQuery = req.query.clone().into();
        // Cache retention may advance req.start; interval selection belongs to
        // the original full request, not the remaining cache gaps.
        final_query.start_time = query.start_time;
        final_query.end_time = query.end_time;
        preparation.finish_cache(
            &final_query,
            org_id,
            stream_type,
            req.search_type,
            replacement,
            added_timestamp,
        )?
    } else {
        sql
    };
    Ok((resp, should_exec_query, Arc::new(sql)))
}

// based on _timestamp of first record in config::meta::search::Response either add it in start
// or end to cache response
#[tracing::instrument(name = "service:search:cache:merge_response", skip_all)]
#[allow(clippy::too_many_arguments)]
pub fn merge_response(
    trace_id: &str,
    cached_responses: &mut Vec<config::meta::search::Response>,
    search_responses: &mut Vec<config::meta::search::Response>,
    ts_column: &str,
    limit: i64,
    is_descending: bool,
    cache_took: usize,
    order_by: Vec<(String, OrderBy)>,
) -> config::meta::search::Response {
    cached_responses.retain(|res| !res.hits.is_empty());
    search_responses.retain(|res| !res.hits.is_empty());

    if cached_responses.is_empty() && search_responses.is_empty() {
        return config::meta::search::Response::default();
    }
    let mut fn_error = vec![];

    let mut cache_response = if cached_responses.is_empty() {
        config::meta::search::Response::default()
    } else {
        let mut resp = config::meta::search::Response::default();
        for res in cached_responses {
            resp.total += res.total;
            resp.scan_size += res.scan_size;
            resp.scan_records += res.scan_records;
            if res.hits.is_empty() {
                continue;
            }
            resp.hits.extend(res.hits.clone());
            resp.histogram_interval = res.histogram_interval;
            if !res.function_error.is_empty() {
                fn_error.extend(res.function_error.clone());
            }
        }
        resp.took = cache_took;
        resp
    };

    if cache_response.hits.is_empty()
        && !search_responses.is_empty()
        && search_responses
            .first()
            .is_none_or(|res| res.hits.is_empty())
        && search_responses
            .last()
            .is_none_or(|res| res.hits.is_empty())
    {
        for res in search_responses {
            cache_response.total += res.total;
            cache_response.scan_size += res.scan_size;
            cache_response.took += res.took;
            cache_response.histogram_interval = res.histogram_interval;
            if !res.function_error.is_empty() {
                fn_error.extend(res.function_error.clone());
            }
        }
        cache_response.function_error = fn_error;
        return cache_response;
    }
    let cached_hits_len = cache_response.hits.len();

    cache_response.scan_size = 0;

    let mut files_cache_ratio = 0;
    let mut search_hits_len = 0;

    let mut res_took = ResponseTook::default();

    for res in search_responses.clone() {
        cache_response.total += res.total;
        cache_response.scan_size += res.scan_size;
        cache_response.took += res.took;
        files_cache_ratio += res.cached_ratio;
        cache_response.histogram_interval = res.histogram_interval;

        search_hits_len += res.total;

        if res.hits.is_empty() {
            continue;
        }
        // here the searches in paralles, so we use the max value of the took_detail
        res_took.idx_took = std::cmp::max(res_took.idx_took, res.took_detail.idx_took);
        res_took.wait_in_queue =
            std::cmp::max(res_took.wait_in_queue, res.took_detail.wait_in_queue);
        res_took.search_took = std::cmp::max(res_took.search_took, res.took_detail.search_took);
        res_took.file_list_took =
            std::cmp::max(res_took.file_list_took, res.took_detail.file_list_took);
        if !res.function_error.is_empty() {
            fn_error.extend(res.function_error.clone());
        }

        cache_response.peak_memory_usage = Some(
            cache_response
                .peak_memory_usage
                .unwrap_or(0.0)
                .max(res.peak_memory_usage.unwrap_or(0.0)),
        );

        cache_response.hits.extend(res.hits.clone());
    }
    sort_response(is_descending, &mut cache_response, ts_column, &order_by);

    if cache_response.hits.len() > (limit as usize) {
        cache_response.hits.truncate(limit as usize);
    }
    if limit > 0 {
        cache_response.total = cache_response.hits.len();
    }

    if !search_responses.is_empty() {
        cache_response.cached_ratio = files_cache_ratio / search_responses.len();
    }
    cache_response.size = cache_response.hits.len() as i64;
    let total = cache_response.size;

    #[allow(unused_mut)]
    let mut search_role = "leader".to_string();
    #[cfg(feature = "enterprise")]
    if get_o2_config().super_cluster.enabled {
        search_role = "super".to_string();
    }
    log::info!(
        "{}",
        search_inspector_fields(
            format!(
                "[trace_id {trace_id}] total: {total}, cached hits: {cached_hits_len}, search hits: {search_hits_len}",
            ),
            SearchInspectorFieldsBuilder::new()
                .trace_id(trace_id.to_string())
                .node_name(LOCAL_NODE.name.clone())
                .component("merge response".to_string())
                .desc(format!(
                    "total: {total}, cached hits: {cached_hits_len}, search hits: {search_hits_len}"
                ))
                .search_role(search_role)
                .build()
        )
    );

    cache_response.took_detail = res_took;
    cache_response.order_by = search_responses
        .first()
        .map(|res| res.order_by)
        .unwrap_or_default();
    cache_response.order_by_metadata = search_responses
        .first()
        .map(|res| res.order_by_metadata.clone())
        .unwrap_or_default();
    cache_response.result_cache_ratio = (((cached_hits_len as f64) * 100_f64)
        / ((search_hits_len + cached_hits_len) as f64))
        as usize;
    if !fn_error.is_empty() {
        cache_response.function_error.extend(fn_error);
        cache_response.is_partial = true;
    }
    cache_response.is_histogram_eligible = search_responses
        .first()
        .map(|res| res.is_histogram_eligible)
        .unwrap_or_default();
    cache_response
}

fn sort_response(
    is_descending: bool,
    cache_response: &mut search::Response,
    ts_column: &str,
    in_order_by: &Vec<(String, OrderBy)>,
) {
    let order_by = if in_order_by.is_empty() {
        &vec![(
            ts_column.to_string(),
            if is_descending {
                OrderBy::Desc
            } else {
                OrderBy::Asc
            },
        )]
    } else {
        in_order_by
    };

    cache_response.hits.sort_by(|a, b| {
        for (field, order) in order_by {
            let cmp = if ts_column == field {
                let a_ts = get_ts_value(ts_column, a);
                let b_ts = get_ts_value(ts_column, b);
                a_ts.partial_cmp(&b_ts).unwrap_or(std::cmp::Ordering::Equal)
            } else {
                let a_val = a.get(field).unwrap_or(&serde_json::Value::Null);
                let b_val = b.get(field).unwrap_or(&serde_json::Value::Null);

                match (a_val, b_val) {
                    (serde_json::Value::String(a_str), serde_json::Value::String(b_str)) => {
                        a_str.cmp(b_str)
                    }
                    (serde_json::Value::Number(a_num), serde_json::Value::Number(b_num)) => {
                        if let (Some(a_f64), Some(b_f64)) = (a_num.as_f64(), b_num.as_f64()) {
                            a_f64
                                .partial_cmp(&b_f64)
                                .unwrap_or(std::cmp::Ordering::Equal)
                        } else {
                            std::cmp::Ordering::Equal
                        }
                    }
                    (serde_json::Value::String(_), serde_json::Value::Number(_)) => {
                        std::cmp::Ordering::Less
                    }
                    (serde_json::Value::Number(_), serde_json::Value::String(_)) => {
                        std::cmp::Ordering::Greater
                    }
                    _ => std::cmp::Ordering::Equal,
                }
            };

            // Apply order direction
            let final_cmp = if order == &OrderBy::Desc {
                cmp.reverse()
            } else {
                cmp
            };

            // If this field comparison is not equal, return the result
            // Otherwise, continue to the next field
            if final_cmp != std::cmp::Ordering::Equal {
                return final_cmp;
            }
        }

        // If all fields are equal, maintain stable sort
        std::cmp::Ordering::Equal
    });
}

/// Cache only complete half-open coverage. LIMIT-saturated results exclude the
/// entire terminal timestamp group: other rows at that timestamp may be missing.
/// Histogram coverage contains only complete buckets, including at the delay edge.
#[tracing::instrument(name = "service:search:cache:write_results", skip_all)]
#[allow(clippy::too_many_arguments)]
pub async fn write_results(
    trace_id: &str,
    ts_column: &str,
    req_query_start_time: i64,
    req_query_end_time: i64,
    limit: i64,
    mut res: config::meta::search::Response,
    file_path: String,
    is_aggregate: bool,
    is_descending: bool,
    clear_cache: bool,
    is_histogram_non_ts_order: bool,
) {
    let delay_ts = second_micros(get_config().limit.cache_delay_secs);
    let Some((accept_start_time, accept_end_time)) = prepare_results_for_cache(
        &mut res,
        ts_column,
        req_query_start_time,
        req_query_end_time,
        limit,
        is_aggregate,
        is_descending,
        is_histogram_non_ts_order,
        Utc::now().timestamp_micros() - delay_ts,
    ) else {
        return;
    };
    if accept_end_time - accept_start_time < delay_ts {
        log::info!("[trace_id {trace_id}] Time range is too short for caching, skipping caching");
        return;
    }

    // 6. cache to disk
    let file_name = format!(
        "{}_{}_{}_{}.json",
        accept_start_time,
        accept_end_time,
        if is_aggregate { 1 } else { 0 },
        if is_descending { 1 } else { 0 }
    );
    let res_cache = json::to_string(&res).unwrap();
    let query_key = file_path.replace('/', "_");
    let trace_id = trace_id.to_string();
    tokio::spawn(async move {
        match SearchService::cache::cacher::cache_results_to_disk(
            &trace_id,
            &file_path,
            &file_name,
            res_cache,
            clear_cache,
            Some(accept_start_time),
            Some(accept_end_time),
        )
        .await
        {
            Ok(success) => {
                if success {
                    // success: true, cache to disk success
                    // success: false, cache to disk already exists, skipping caching
                    QUERY_RESULT_CACHE
                        .write()
                        .await
                        .entry(query_key)
                        .or_insert_with(Vec::new)
                        .push(ResultCacheMeta {
                            start_time: accept_start_time,
                            end_time: accept_end_time,
                            is_aggregate,
                            is_descending,
                        });
                }
            }
            Err(e) => {
                log::error!("Cache results to disk failed: {e}");
            }
        }
    });
}

#[allow(clippy::too_many_arguments)]
fn prepare_results_for_cache(
    res: &mut search::Response,
    ts_column: &str,
    start: i64,
    end: i64,
    limit: i64,
    is_aggregate: bool,
    is_descending: bool,
    is_histogram_non_ts_order: bool,
    stable_end: i64,
) -> Option<(i64, i64)> {
    if res.hits.is_empty() || res.is_partial || !res.function_error.is_empty() {
        return None;
    }
    let interval = if is_aggregate {
        res.histogram_interval
            .unwrap_or_default()
            .max(0)
            .checked_mul(1_000_000)?
    } else {
        0
    };
    let mut start = start;
    let mut end = end.min(stable_end);
    if limit > 0 && res.hits.len() >= limit as usize {
        // A non-time-ordered top-N says nothing about completeness anywhere
        // in time. An exhausted histogram is still fully cacheable.
        if is_histogram_non_ts_order {
            return None;
        }
        let terminal_ts = get_ts_value(ts_column, res.hits.last()?);
        if is_descending {
            start = start.max(terminal_ts.checked_add(interval.max(1))?);
        } else {
            end = end.min(terminal_ts);
        }
    }
    if interval > 0 {
        let remainder = start.rem_euclid(interval);
        if remainder != 0 {
            start = start.checked_add(interval - remainder)?;
        }
        end = end.checked_sub(end.rem_euclid(interval))?;
    }
    if start >= end {
        return None;
    }
    let (data_start, data_end) =
        extract_timestamp_range(&res.hits, ts_column, !is_histogram_non_ts_order);
    if data_start < start || data_end >= end {
        res.hits.retain(|hit| {
            let ts = get_ts_value(ts_column, hit);
            start <= ts && ts < end
        });
    }
    if res.hits.is_empty() {
        return None;
    }
    res.total = res.hits.len();
    res.size = res.hits.len() as i64;
    Some((start, end))
}

pub fn apply_vrl_to_response(
    query_fn: Option<String>,
    res: &mut config::meta::search::Response,
    org_id: &str,
    stream_name: &str,
    trace_id: &str,
) -> Vec<serde_json::Value> {
    let mut local_res = res.clone();

    local_res.hits = if let Some(query_fn) = query_fn
        && !local_res.hits.is_empty()
        && !local_res.is_partial
    {
        // compile vrl function & apply the same before returning the response
        let mut input_fn = query_fn.trim().to_string();

        let apply_over_hits = RESULT_ARRAY_SKIP_VRL.is_match(&input_fn);
        if apply_over_hits {
            input_fn = RESULT_ARRAY_SKIP_VRL.replace(&input_fn, "").to_string();
        }
        let mut runtime = init_vrl_runtime();
        let program = match crate::service::ingestion::compile_vrl_function(&input_fn, org_id) {
            Ok(program) => {
                let registry = program
                    .config
                    .get_custom::<vector_enrichment::TableRegistry>()
                    .unwrap();
                registry.finish_load();
                Some(program)
            }
            Err(err) => {
                log::error!("[trace_id {trace_id}] search->vrl: compile err: {err:?}");
                local_res.function_error.push(err.to_string());
                local_res.is_partial = true;
                None
            }
        };
        match program {
            Some(program) => {
                if apply_over_hits {
                    let (ret_val, err) = crate::service::ingestion::apply_vrl_fn(
                        &mut runtime,
                        &config::meta::function::VRLResultResolver {
                            program: program.program.clone(),
                            fields: program.fields.clone(),
                        },
                        json::Value::Array(local_res.hits.clone()),
                        org_id,
                        &[stream_name.to_string()],
                    );
                    if let Some(e) = err {
                        log::error!("Error applying vrl function: {e}");
                    }
                    ret_val
                        .as_array()
                        .unwrap()
                        .iter()
                        .filter_map(|v| {
                            (!v.is_null())
                                .then_some(config::utils::flatten::flatten(v.clone()).unwrap())
                        })
                        .collect()
                } else {
                    let mut error = "".to_string();
                    let res = local_res
                        .hits
                        .into_iter()
                        .filter_map(|hit| {
                            let (ret_val, err) = crate::service::ingestion::apply_vrl_fn(
                                &mut runtime,
                                &config::meta::function::VRLResultResolver {
                                    program: program.program.clone(),
                                    fields: program.fields.clone(),
                                },
                                hit,
                                org_id,
                                &[stream_name.to_string()],
                            );
                            if let Some(e) = err {
                                error = e;
                            }
                            (!ret_val.is_null())
                                .then_some(config::utils::flatten::flatten(ret_val).unwrap())
                        })
                        .collect();
                    if !error.is_empty() {
                        log::error!("Error applying vrl function: {error}");
                    }
                    res
                }
            }
            None => local_res.hits,
        }
    } else {
        local_res.hits
    };
    local_res.hits
}

pub fn is_result_array_skip_vrl(vrl_fn: &str) -> bool {
    RESULT_ARRAY_SKIP_VRL.is_match(vrl_fn)
}

#[cfg(feature = "vectorscan")]
pub async fn apply_regex_to_response(
    req: &config::meta::search::Request,
    org_id: &str,
    all_streams: &str,
    stream_type: StreamType,
    res: &mut config::meta::search::Response,
    trace_id: &str,
    ctx: &str,
) -> Result<(), infra::errors::Error> {
    if res.hits.is_empty() {
        return Ok(());
    }

    let start = std::time::Instant::now();
    let pattern_manager = get_pattern_manager().await?;

    let query: proto::cluster_rpc::SearchQuery = req.query.clone().into();
    let sql =
        match crate::service::search::sql::Sql::new(&query, org_id, stream_type, req.search_type)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                log::error!("Error parsing sql: {e}");
                return Ok(());
            }
        };

    let projections: std::collections::HashMap<String, Vec<ProjectionColumnMapping>> =
        crate::service::search::datafusion::plan::regex_projections::get_columns_from_projections(
            sql,
        )
        .await?;
    if projections.is_empty() {
        return Ok(());
    }

    let ret = match pattern_manager.process_at_search(
        org_id,
        StreamType::Logs,
        &mut res.hits,
        projections,
    ) {
        Ok(_) => Ok(()),
        Err(e) => {
            log::error!(
                "[trace_id {trace_id}] SDR patterns application: error in processing records for stream: {all_streams}: {e}"
            );
            Err(infra::errors::Error::Message(e.to_string()))
        }
    };
    let took = start.elapsed().as_millis();
    log::info!(
        "[trace_id {trace_id}] SDR patterns application: context: {ctx}, took: {took} ms, processed: {} hits",
        res.hits.len()
    );
    ret
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_result_array_skip_vrl() {
        let query_fn = r#"#ResultArray#SkipVRL#
        arr1_final = []
        for_each(array!(.)) -> |index, value| {
            value.arr = {"a": 4}
            arr1_final = push(arr1_final,value)
        }
        . = arr1_final"#;
        assert!(is_result_array_skip_vrl(query_fn));
    }

    #[test]
    fn test_is_result_array_skip_vrl_no_marker() {
        let query_fn = "just a normal query";
        assert!(!is_result_array_skip_vrl(query_fn));
    }
}
