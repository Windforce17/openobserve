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

use bytes::Bytes;
use config::{
    TIMESTAMP_COL_NAME,
    meta::{search::Response, sql::OrderBy, stream::StreamType},
    utils::{file::scan_files, json},
};
use infra::cache::{
    file_data::disk::{self, QUERY_RESULT_CACHE},
    meta::ResultCacheMeta,
};
#[cfg(feature = "enterprise")]
use o2_enterprise::enterprise::search::cache::streaming_agg::STREAMING_AGGS_CACHE_DIR;

use crate::{
    common::meta::search::{
        CacheQueryRequest, CachedQueryResponse, QueryDelta, ResultCacheSelectionStrategy,
    },
    service::search::{
        cache::{
            MultiCachedQueryResponse,
            result_utils::{get_ts_value, has_non_timestamp_ordering, is_timestamp_field},
        },
        sql::Sql,
    },
};

/// Invalidate cached response by stream min ts
/// This is done to ensure that any stale data which is no longer retained in the stream is not
/// returned as part of the cached response
/// The cache will eventually remove the stale data as part of the cache eviction policy
pub async fn invalidate_cached_response_by_stream_min_ts(
    file_path: &str,
    responses: &[CachedQueryResponse],
    histogram_interval: i64, // microseconds; 0 for non-histogram queries
) -> Result<Vec<CachedQueryResponse>, String> {
    let components: Vec<&str> = file_path.split('/').collect();
    if components.len() < 3 {
        return Err(format!(
            "File path does not contain sufficient components: {file_path}"
        ));
    }

    let (org_id, stream_type_str, stream_name) = (components[0], components[1], components[2]);
    let stream_type = StreamType::from(stream_type_str);

    let stream_min_ts =
        infra::cache::stats::get_stream_stats(org_id, stream_name, stream_type).doc_time_min;

    let filtered_responses = responses
        .iter()
        .cloned()
        .filter_map(|mut meta| {
            // A histogram bucket straddling retention may include expired rows.
            // Re-query it; do not reuse an aggregate that cannot be subtracted.
            let mut retained_start = stream_min_ts;
            if histogram_interval > 0 {
                let remainder = retained_start.rem_euclid(histogram_interval);
                if remainder != 0 {
                    retained_start = retained_start.checked_add(histogram_interval - remainder)?;
                }
            }
            if retained_start >= meta.response_end_time {
                return None;
            }
            if retained_start > meta.response_start_time {
                meta.response_start_time = retained_start;
                meta.cached_response
                    .hits
                    .retain(|hit| get_ts_value(&meta.ts_column, hit) >= retained_start);
                meta.cached_response.total = meta.cached_response.hits.len();
                meta.cached_response.size = meta.cached_response.hits.len() as i64;
            }
            Some(meta)
        })
        .collect();

    Ok(filtered_responses)
}

#[tracing::instrument(
    name = "service:search:cache:cacher:check_cache",
    skip_all,
    fields(org_id = org_id)
)]
#[allow(clippy::too_many_arguments)]
pub async fn check_cache(
    trace_id: &str,
    org_id: &str,
    req: &mut config::meta::search::Request,
    origin_sql: &mut str,
    file_path: &str,
    is_aggregate: bool,
    sql: &Sql,
    result_ts_col: &str,
    is_descending: bool,
    should_exec_query: &mut bool,
) -> MultiCachedQueryResponse {
    let start = std::time::Instant::now();

    let order_by = &sql.order_by;

    // skip the queries with no timestamp column
    if result_ts_col.is_empty() && (is_aggregate || !sql.group_by.is_empty()) {
        return MultiCachedQueryResponse {
            order_by: order_by.clone(),
            ..Default::default()
        };
    }

    // Check if query contains histogram function
    let is_histogram_query = sql.histogram_interval.is_some();

    // skip the count queries & queries first order by is not _timestamp field
    // Exception: Allow histogram queries even if ORDER BY is not on timestamp,
    // because histogram is plotted based on timestamp
    if req.query.track_total_hits
        || (!order_by.is_empty()
            && order_by.first().as_ref().unwrap().0 != TIMESTAMP_COL_NAME
            && !is_histogram_query
            && !result_ts_col.is_empty()
            && result_ts_col != order_by.first().as_ref().unwrap().0)
    {
        return MultiCachedQueryResponse {
            order_by: order_by.clone(),
            ..Default::default()
        };
    }

    // Note: Both result_ts_col refinement and SQL modification (adding _timestamp to SELECT)
    // are now done in prepare_cache_response() before calling this function.
    // just use the refined result_ts_col that was passed in.

    // Check ts_col again, if it is still empty, return default
    if result_ts_col.is_empty() {
        return MultiCachedQueryResponse {
            order_by: order_by.clone(),
            ..Default::default()
        };
    }

    let mut histogram_interval = -1;
    if is_aggregate && let Some(interval) = sql.histogram_interval {
        // Note: handle_histogram is now called in prepare_cache_response() before hash computation
        // to ensure consistent hashing. We just need to update req.query.sql with the normalized
        // SQL.
        req.query.sql = origin_sql.to_owned();
        histogram_interval = interval * 1000 * 1000; // in microseconds
    }

    // Note: is_descending refinement for histogram queries is now done in prepare_cache_response()
    // before calling this function, so we use the pre-refined value directly

    if is_aggregate && order_by.is_empty() && result_ts_col.is_empty() {
        return MultiCachedQueryResponse::default();
    }

    // Determine if this is a histogram query with non-timestamp ORDER BY.
    // These queries need special handling because results may not be time-ordered,
    // requiring us to scan all hits to find the actual time range.
    let is_histogram_non_ts_order = is_histogram_query
        && !order_by.is_empty()
        && has_non_timestamp_ordering(order_by, result_ts_col);

    let mut multi_resp = MultiCachedQueryResponse {
        trace_id: trace_id.to_string(),
        is_aggregate,
        is_descending,
        ..Default::default()
    };
    if histogram_interval > 0 {
        multi_resp.histogram_interval = histogram_interval / 1000 / 1000;
    }
    log::info!(
        "[trace_id {trace_id}] check_cache: result_ts_col: {}, histogram_interval: {}, time range: {} - {}",
        result_ts_col,
        histogram_interval,
        req.query.start_time,
        req.query.end_time
    );

    if config::get_config().common.use_multi_result_cache {
        let mut cached_responses = super::multi::get_cached_results(
            trace_id,
            file_path,
            CacheQueryRequest {
                q_start_time: req.query.start_time,
                q_end_time: req.query.end_time,
                is_aggregate,
                ts_column: result_ts_col.to_string(),
                histogram_interval,
                is_descending,
                is_histogram_non_ts_order,
            },
        )
        .await;
        if is_descending {
            cached_responses.sort_by_key(|meta| meta.response_end_time);
        } else {
            cached_responses.sort_by_key(|meta| meta.response_start_time);
        }

        // remove the cached response older than stream min ts
        match invalidate_cached_response_by_stream_min_ts(
            file_path,
            &cached_responses,
            histogram_interval,
        )
        .await
        {
            Ok(responses) => {
                cached_responses = responses;
            }
            Err(e) => log::error!("Error invalidating cached response by stream min ts: {e}"),
        }

        let (deltas, updated_start_time, cache_duration) = calculate_deltas_multi(
            &cached_responses,
            req.query.start_time,
            req.query.end_time,
            is_aggregate,
            is_descending,
            histogram_interval,
        );
        multi_resp.total_cache_duration = cache_duration as usize;
        if let Some(start_time) = updated_start_time {
            req.query.start_time = start_time;
        }

        // v5 coverage is complete even when the interval contains fewer than
        // LIMIT rows (or no rows after clipping).
        if deltas.is_empty() {
            *should_exec_query = false;
        }

        for res in cached_responses {
            if res.has_cached_data {
                multi_resp.has_cached_data = true;
                multi_resp.cached_response.push(res);
            }
        }

        multi_resp.deltas = deltas;
        multi_resp.cache_query_response = true;
        multi_resp.limit = sql.limit;
        multi_resp.ts_column = result_ts_col.to_string();
        multi_resp.took = start.elapsed().as_millis() as usize;
        multi_resp.file_path = file_path.to_string();
        multi_resp.order_by = order_by.clone();
        multi_resp.is_aggregate = is_aggregate;
        multi_resp
    } else {
        let c_resp = match get_cached_results(
            trace_id,
            file_path,
            CacheQueryRequest {
                q_start_time: req.query.start_time,
                q_end_time: req.query.end_time,
                is_aggregate,
                ts_column: result_ts_col.to_string(),
                histogram_interval,
                is_descending,
                is_histogram_non_ts_order,
            },
            None,
        )
        .await
        {
            Some(mut cached_resp) => {
                // remove the cached response older than stream min ts
                match invalidate_cached_response_by_stream_min_ts(
                    file_path,
                    &[cached_resp.clone()],
                    histogram_interval,
                )
                .await
                {
                    Ok(responses) => {
                        // single cached query response is expected
                        cached_resp = match responses.first() {
                            Some(v) => v.clone(),
                            None => {
                                log::error!("No cached response found after validation");
                                CachedQueryResponse {
                                    is_descending,
                                    ..Default::default()
                                }
                            }
                        };
                    }
                    Err(e) => {
                        log::error!("Error invalidating cached response by stream min ts: {e:?}")
                    }
                }

                let mut deltas = vec![];
                calculate_deltas(
                    &(ResultCacheMeta {
                        start_time: cached_resp.response_start_time,
                        end_time: cached_resp.response_end_time,
                        is_aggregate,
                        is_descending,
                    }),
                    req.query.start_time,
                    req.query.end_time,
                    histogram_interval,
                    &mut deltas,
                );

                let search_delta: Vec<QueryDelta> = deltas.to_vec();
                if search_delta.is_empty() {
                    log::debug!("cached response found");
                    *should_exec_query = false;
                }

                cached_resp.deltas = search_delta;

                cached_resp.cached_response.took = start.elapsed().as_millis() as usize;
                cached_resp
            }
            None => {
                // since there is no cache & will be cached in the end we should return the response
                log::debug!("cached response not found");
                CachedQueryResponse {
                    is_descending,
                    ..Default::default()
                }
            }
        };
        multi_resp.has_cached_data = c_resp.has_cached_data;
        if !c_resp.deltas.is_empty() {
            multi_resp.deltas = c_resp.deltas.clone();
        };
        if c_resp.has_cached_data {
            multi_resp.cached_response.push(c_resp);
        }
        multi_resp.took = start.elapsed().as_millis() as usize;
        multi_resp.cache_query_response = true;
        multi_resp.limit = sql.limit;
        multi_resp.ts_column = result_ts_col.to_string();
        multi_resp.file_path = file_path.to_string();
        multi_resp.order_by = order_by.clone();
        multi_resp.is_aggregate = is_aggregate;
        multi_resp
    }
}

pub async fn get_cached_results(
    trace_id: &str,
    file_path: &str,
    cache_req: CacheQueryRequest,
    cache_metas: Option<Vec<ResultCacheMeta>>,
) -> Option<CachedQueryResponse> {
    let query_key = file_path.replace('/', "_");
    let cache_metas = match cache_metas {
        Some(v) => v,
        None => QUERY_RESULT_CACHE.read().await.get(&query_key).cloned()?,
    };

    let selection_strategy = ResultCacheSelectionStrategy::from(
        config::get_config()
            .common
            .result_cache_selection_strategy
            .as_str(),
    );

    // get the best matching cache meta
    let mut matching_meta = match cache_metas
        .iter()
        .filter(|m| {
            // to make sure there is overlap between cache time range and query time range
            log::info!(
                "[CACHE CANDIDATES {trace_id}] get_cached_results: cache_meta time_range: {} - {}",
                m.start_time,
                m.end_time
            );
            // check if the data is matching for histogram
            if cache_req.is_aggregate
                && cache_req.histogram_interval > 0
                && (m.start_time % cache_req.histogram_interval != 0
                    || m.end_time % cache_req.histogram_interval != 0)
            {
                return false;
            }
            m.start_time < cache_req.q_end_time && m.end_time > cache_req.q_start_time
        })
        .max_by_key(|result| select_cache_meta(result, &cache_req, &selection_strategy))
    {
        Some(v) => v.clone(),
        None => {
            log::debug!("No matching cache found for query key: {query_key}");
            return None;
        }
    };

    // get the cache data from disk
    let file_name = format!(
        "{}_{}_{}_{}.json",
        matching_meta.start_time,
        matching_meta.end_time,
        if cache_req.is_aggregate { 1 } else { 0 },
        if cache_req.is_descending { 1 } else { 0 }
    );
    let data = match get_results(file_path, &file_name).await {
        Ok(v) => v,
        Err(e) => {
            log::error!(
                "[trace_id {trace_id}] Get results from disk failed: file: {file_path}/{file_name}, error: {e}"
            );
            return None;
        }
    };
    let mut cached_response: Response = match json::from_slice::<Response>(&data) {
        Ok(v) => v,
        Err(e) => {
            log::error!("[trace_id {trace_id}] Error parsing cached response: {e:?}");
            return None;
        }
    };

    // Coverage is an interval, not the extrema of the rows that happen to exist.
    // Intersect it with the request, preserving empty covered portions as well.
    if !clip_cached_response(&mut cached_response, &mut matching_meta, &cache_req) {
        return None;
    }

    log::info!(
        "[CACHE RESULT {trace_id}] Get results from disk success for query key: {query_key} with time range {} - {} ",
        matching_meta.start_time,
        matching_meta.end_time
    );

    Some(CachedQueryResponse {
        cached_response,
        deltas: vec![],
        has_cached_data: true,
        cache_query_response: true,
        response_start_time: matching_meta.start_time,
        response_end_time: matching_meta.end_time,
        ts_column: cache_req.ts_column.to_string(),
        is_descending: cache_req.is_descending,
        limit: -1,
    })
}

fn clip_cached_response(
    response: &mut Response,
    meta: &mut ResultCacheMeta,
    req: &CacheQueryRequest,
) -> bool {
    let original_range = (meta.start_time, meta.end_time);
    meta.start_time = meta.start_time.max(req.q_start_time);
    meta.end_time = meta.end_time.min(req.q_end_time);
    if req.histogram_interval > 0 {
        let remainder = meta.start_time.rem_euclid(req.histogram_interval);
        if remainder != 0 {
            let Some(start) = meta
                .start_time
                .checked_add(req.histogram_interval - remainder)
            else {
                return false;
            };
            meta.start_time = start;
        }
        meta.end_time -= meta.end_time.rem_euclid(req.histogram_interval);
    }
    if meta.start_time >= meta.end_time {
        return false;
    }
    if original_range != (meta.start_time, meta.end_time) {
        response.hits.retain(|hit| {
            let ts = get_ts_value(&req.ts_column, hit);
            meta.start_time <= ts && ts < meta.end_time
        });
    }
    response.total = response.hits.len();
    response.size = response.hits.len() as i64;
    true
}

pub fn calculate_deltas(
    result_meta: &ResultCacheMeta,
    query_start_time: i64,
    query_end_time: i64,
    _histogram_interval: i64,
    deltas: &mut Vec<QueryDelta>,
) {
    let start = result_meta.start_time.max(query_start_time);
    let end = result_meta.end_time.min(query_end_time);
    if start >= end {
        if query_start_time < query_end_time {
            deltas.push(QueryDelta {
                delta_start_time: query_start_time,
                delta_end_time: query_end_time,
            });
        }
        return;
    }
    if query_start_time < start {
        deltas.push(QueryDelta {
            delta_start_time: query_start_time,
            delta_end_time: start,
        });
    }
    if end < query_end_time {
        deltas.push(QueryDelta {
            delta_start_time: end,
            delta_end_time: query_end_time,
        });
    }
}

pub async fn cache_results_to_disk(
    trace_id: &str,
    file_path: &str,
    file_name: &str,
    data: String,
    clear_cache: bool,
    clean_start_ts: Option<i64>,
    clean_end_ts: Option<i64>,
) -> std::io::Result<bool> {
    let start = std::time::Instant::now();
    log::info!("[trace_id {trace_id}] Caching results to disk");

    if clear_cache {
        log::info!(
            "[trace_id {trace_id}] Clearing cache for file path as use_cache: false, clear_cache: {clear_cache}, start: {},  {file_path}",
            start.elapsed().as_millis(),
        );
        let _ = delete_cache(file_path, 0, clean_start_ts, clean_end_ts)
            .await
            .map_err(|e| {
                log::error!(
                    "[trace_id {trace_id}] Clearing cache for file path error: {}",
                    e
                );
                e
            });
        log::info!(
            "[trace_id {trace_id}] Clearing cache for file path completed. use_cache: false, clear_cache: {clear_cache}, took: {} ms, {file_path}",
            start.elapsed().as_millis(),
        );
    }

    let file = format!("results/{file_path}/{file_name}");
    if disk::exist(&file).await {
        return Ok(false);
    }
    match disk::set(&file, Bytes::from(data)).await {
        Ok(_) => {
            log::info!(
                "[trace_id {trace_id}] After clearing cache, Cached results to disk completed, took: {} ms",
                start.elapsed().as_millis()
            );
        }
        Err(e) => {
            log::error!("[trace_id {trace_id}] Error caching results to disk: {e}");
            return Err(std::io::Error::other("Error caching results to disk"));
        }
    }

    log::info!(
        "[trace_id {trace_id}] Cached results to disk completed, took: {} ms",
        start.elapsed().as_millis()
    );
    Ok(true)
}

pub async fn get_results(file_path: &str, file_name: &str) -> std::io::Result<Bytes> {
    let file = format!("results/{file_path}/{file_name}");
    match disk::get(&file, None).await {
        Some(v) => Ok(v),
        None => Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "File not found",
        )),
    }
}

pub fn get_ts_col_order_by(
    parsed_sql: &Sql,
    _ts_col: &str,
    _is_aggregate: bool,
) -> Option<(String, bool)> {
    let mut is_descending = true;
    let order_by = &parsed_sql.order_by;
    let result_ts_col = {
        #[cfg(not(feature = "enterprise"))]
        {
            let mut ts_col = String::new();
            for (original, alias) in &parsed_sql.aliases {
                if original == _ts_col || original.contains("histogram") {
                    ts_col = alias.clone();
                }
            }
            if !_is_aggregate
                && (parsed_sql
                    .columns
                    .iter()
                    .any(|(_, v)| v.contains(&_ts_col.to_owned()))
                    || parsed_sql.order_by.iter().any(|v| v.0.eq(&_ts_col)))
            {
                ts_col = _ts_col.to_string();
            }
            ts_col
        }

        #[cfg(feature = "enterprise")]
        {
            match o2_enterprise::enterprise::search::cache_ts_util::get_timestamp_column_name(
                &parsed_sql.sql,
            ) {
                Some(result) => result,
                None => "".to_string(),
            }
        }
    };

    if !order_by.is_empty() && !result_ts_col.is_empty() {
        for (field, order) in order_by {
            if is_timestamp_field(field, &result_ts_col) {
                is_descending = order == &OrderBy::Desc;
                break;
            }
        }
    };
    if result_ts_col.is_empty() {
        None
    } else {
        Some((result_ts_col, is_descending))
    }
}

/// Computes the cache file path based on query metadata and histogram information.
/// This function ensures consistent file path generation across different code paths.
///
/// # Arguments
/// * `base_path` - The base path format: "{org_id}/{stream_type}/{stream_name}/{hashed_query}"
/// * `is_aggregate` - Whether the query is an aggregate query
/// * `histogram_interval` - Optional histogram interval from the SQL query
/// * `ts_column` - The timestamp column name
///
/// # Returns
/// The complete file path with histogram information appended if applicable
pub fn compute_cache_file_path(
    base_path: &str,
    is_aggregate: bool,
    histogram_interval: Option<i64>,
    ts_column: &str,
) -> String {
    let mut file_path = base_path.to_string();

    // For histogram queries, append interval and ts_column to file_path
    if is_aggregate && let Some(interval) = histogram_interval {
        file_path = format!("{file_path}_{interval}_{ts_column}");
    }

    file_path
}

/// Refines is_descending for histogram queries with non-timestamp ORDER BY.
///
/// For histogram queries, if ORDER BY is not on the timestamp column,
/// we use ascending as the default for cache operations.
///
/// # Arguments
/// * `sql` - Parsed SQL query
/// * `ts_column` - The timestamp column name
/// * `initial_is_descending` - The initial is_descending value from ORDER BY
///
/// # Returns
/// Refined is_descending value
pub fn refine_is_descending_for_histogram(
    sql: &Sql,
    ts_column: &str,
    initial_is_descending: bool,
) -> bool {
    let is_histogram_query = sql.histogram_interval.is_some();

    if !is_histogram_query || sql.order_by.is_empty() {
        return initial_is_descending;
    }

    // Check if ORDER BY includes the timestamp column
    let mut found_ts_order = false;
    let mut refined_is_descending = initial_is_descending;

    for (field, order) in &sql.order_by {
        if is_timestamp_field(field, ts_column) {
            refined_is_descending = order == &OrderBy::Desc;
            found_ts_order = true;
            break;
        }
    }

    // For histogram queries ordered by non-timestamp columns (e.g., ORDER BY count),
    // use ascending as default
    if !found_ts_order {
        refined_is_descending = false;
    }

    refined_is_descending
}

enum DeletionCriteria {
    TimeRange(i64, i64),
    ThresholdTimestamp(i64),
    DeleteAll,
}

/// Cache selection strategies determine how to choose the best cached result when multiple caches
/// exist:
///
/// 1. Overlap: Selects cache with maximum overlap with query time range Example: Query:
///    10:00-10:30, Cache1: 10:00-10:15, Cache2: 10:10-10:25 Chooses Cache2 (15min overlap) over
///    Cache1 (10min overlap)
///
/// 2. Duration: Selects cache with longest duration regardless of overlap Example: Query:
///    10:00-10:30, Cache1: 09:00-10:00, Cache2: 09:30-10:30   Chooses Cache1 (1hr) over Cache2
///    (30min)
///
/// 3. Both: Calculates what percentage of the cache duration overlaps with query Example: Query:
///    10:00-11:00 Cache1: 10:00-10:30 (duration: 30min, overlap: 30min) = (30/30)*100 = 100%
///    Cache2: 10:15-11:15 (duration: 60min, overlap: 45min) = (45/60)*100 = 75% Chooses Cache1
///    because 100% of its duration is useful for the query
pub fn select_cache_meta(
    meta: &ResultCacheMeta,
    req: &CacheQueryRequest,
    strategy: &ResultCacheSelectionStrategy,
) -> i64 {
    match strategy {
        ResultCacheSelectionStrategy::Overlap => {
            let overlap_start = meta.start_time.max(req.q_start_time);
            let overlap_end = meta.end_time.min(req.q_end_time);
            overlap_end - overlap_start
        }
        ResultCacheSelectionStrategy::Duration => meta.end_time - meta.start_time,
        ResultCacheSelectionStrategy::Both => {
            let overlap_start = req.q_start_time.max(meta.start_time);
            let overlap_end = req.q_end_time.min(meta.end_time);
            let overlap_duration = overlap_end - overlap_start;
            let cache_duration = meta.end_time - meta.start_time;
            if cache_duration > 0 {
                (overlap_duration * 100) / cache_duration
            } else {
                0
            }
        }
    }
}

fn parse_cache_file_timestamps(file_path: &str) -> Option<(i64, i64)> {
    let file_name = file_path.split('/').next_back()?;
    // Remove file extension (e.g., .json, .arrow) before parsing
    let file_name_without_ext = file_name.split('.').next()?;

    let parts: Vec<&str> = file_name_without_ext.split('_').collect();
    if parts.len() >= 2
        && let (Ok(start_ts), Ok(end_ts)) = (parts[0].parse::<i64>(), parts[1].parse::<i64>())
    {
        return Some((start_ts, end_ts));
    }
    None
}

fn time_ranges_overlap(start1: i64, end1: i64, start2: i64, end2: i64) -> bool {
    // Check if file data range overlaps with clean range
    // File overlaps if: file_start < clean_end AND file_end > clean_start
    // NOTE: Partial overlap is considered an overlap
    start1 < end2 && end1 > start2
}

fn should_delete_cache_file(file_path: &str, criteria: &DeletionCriteria) -> bool {
    let Some((file_start_ts, file_end_ts)) = parse_cache_file_timestamps(file_path) else {
        return false;
    };

    match criteria {
        // First check for time range overlapping files
        DeletionCriteria::TimeRange(clean_start, clean_end) => {
            time_ranges_overlap(file_start_ts, file_end_ts, *clean_start, *clean_end)
        }
        // Second check for threshold timestamp
        // Only delete if start_time <= delete_ts (keep cache from delete_ts onwards)
        DeletionCriteria::ThresholdTimestamp(delete_ts) => file_start_ts <= *delete_ts,
        // Last check for delete all
        DeletionCriteria::DeleteAll => true,
    }
}

#[tracing::instrument]
pub async fn delete_cache(
    path: &str,
    delete_ts: i64,
    clean_start_ts: Option<i64>,
    clean_end_ts: Option<i64>,
) -> std::io::Result<bool> {
    let root_dir = disk::get_dir().await;
    // Part 1: delete the results cache
    let pattern = format!("{root_dir}/results/{path}");
    let prefix = format!("{root_dir}/");
    let files = scan_files(&pattern, "json", None).unwrap_or_default();
    let mut remove_files: Vec<String> = vec![];

    let criteria = match (clean_start_ts, clean_end_ts, delete_ts) {
        // First check for time range
        (Some(start), Some(end), _) => DeletionCriteria::TimeRange(start, end),
        // Second check for threshold timestamp
        (_, _, ts) if ts > 0 => DeletionCriteria::ThresholdTimestamp(ts),
        // Last check for delete all
        _ => DeletionCriteria::DeleteAll,
    };

    for file in files {
        if !should_delete_cache_file(&file, &criteria) {
            continue;
        }
        match disk::remove(file.strip_prefix(&prefix).unwrap()).await {
            Ok(_) => remove_files.push(file),
            Err(e) => {
                log::error!("Error deleting cache: {:?}", e);
                return Err(std::io::Error::other("Error deleting cache"));
            }
        }
    }

    // Part 2: delete the aggregation cache
    #[cfg(feature = "enterprise")]
    {
        let aggs_pattern = format!("{root_dir}/{STREAMING_AGGS_CACHE_DIR}/{path}");
        let aggs_files = scan_files(&aggs_pattern, "arrow", None).unwrap_or_default();

        for file in aggs_files {
            if !should_delete_cache_file(&file, &criteria) {
                continue;
            }
            match disk::remove(file.strip_prefix(&prefix).unwrap()).await {
                Ok(_) => remove_files.push(file),
                Err(e) => {
                    log::error!("Error deleting cache: {:?}", e);
                    return Err(std::io::Error::other("Error deleting cache"));
                }
            }
        }
    }

    for file in remove_files {
        let columns = file
            .strip_prefix(&prefix)
            .unwrap()
            .split('/')
            .collect::<Vec<&str>>();

        let query_key = format!(
            "{}_{}_{}_{}",
            columns[1], columns[2], columns[3], columns[4]
        );
        let mut r = QUERY_RESULT_CACHE.write().await;
        r.remove(&query_key);
    }
    Ok(true)
}

fn calculate_deltas_multi(
    results: &[CachedQueryResponse],
    start_time: i64,
    end_time: i64,
    _is_aggregate: bool,
    _is_descending: bool,
    _histogram_interval: i64,
) -> (Vec<QueryDelta>, Option<i64>, i64) {
    let mut deltas = Vec::new();
    let mut cache_duration = 0_i64;
    let mut cursor = start_time;
    // Sort only borrowed metadata, not complete cached JSON responses.
    let mut results: Vec<_> = results.iter().collect();
    results.sort_unstable_by_key(|meta| meta.response_start_time);
    for meta in results {
        let start = meta.response_start_time.max(start_time);
        let end = meta.response_end_time.min(end_time);
        if start >= end || end <= cursor {
            continue;
        }
        if cursor < start {
            deltas.push(QueryDelta {
                delta_start_time: cursor,
                delta_end_time: start,
            });
        }
        cache_duration += end - start.max(cursor);
        cursor = end;
    }
    if cursor < end_time {
        deltas.push(QueryDelta {
            delta_start_time: cursor,
            delta_end_time: end_time,
        });
    }
    (deltas, None, cache_duration)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{Field, Schema};
    use config::meta::{
        search::{Query, Request, RequestEncoding, Response, SearchEventType},
        sql::OrderBy,
    };
    use datafusion::common::TableReference;
    use infra::schema::{STREAM_SCHEMAS_LATEST, SchemaCache};
    use proto::cluster_rpc::SearchQuery;

    use super::*;
    use crate::{common::meta::search::CachedQueryResponse, service::search::Sql};

    #[test]
    fn test_parse_cache_file_timestamps_valid() {
        let (start, end) = parse_cache_file_timestamps("cache/1000_2000.json").unwrap();
        assert_eq!(start, 1000);
        assert_eq!(end, 2000);
    }

    #[test]
    fn test_parse_cache_file_timestamps_no_extension() {
        let result = parse_cache_file_timestamps("cache/1000_2000");
        assert!(result.is_some());
        let (start, end) = result.unwrap();
        assert_eq!(start, 1000);
        assert_eq!(end, 2000);
    }

    #[test]
    fn test_parse_cache_file_timestamps_invalid_returns_none() {
        assert!(parse_cache_file_timestamps("cache/abc_def.json").is_none());
    }

    #[test]
    fn test_time_ranges_overlap_overlapping() {
        assert!(time_ranges_overlap(100, 300, 200, 400));
    }

    #[test]
    fn test_time_ranges_overlap_non_overlapping() {
        assert!(!time_ranges_overlap(100, 200, 300, 400));
    }

    #[test]
    fn test_time_ranges_overlap_adjacent_no_overlap() {
        assert!(!time_ranges_overlap(100, 200, 200, 300));
    }

    #[test]
    fn test_should_delete_cache_file_delete_all() {
        let criteria = DeletionCriteria::DeleteAll;
        assert!(should_delete_cache_file("path/1000_2000.json", &criteria));
    }

    #[test]
    fn test_should_delete_cache_file_invalid_path_returns_false() {
        let criteria = DeletionCriteria::DeleteAll;
        assert!(!should_delete_cache_file(
            "path/no_timestamps_here.json",
            &criteria
        ));
    }

    #[test]
    fn test_should_delete_cache_file_threshold() {
        let criteria = DeletionCriteria::ThresholdTimestamp(1500);
        // start_ts=1000 <= 1500 → delete
        assert!(should_delete_cache_file("path/1000_2000.json", &criteria));
        // start_ts=2000 > 1500 → keep
        assert!(!should_delete_cache_file("path/2000_3000.json", &criteria));
    }

    fn boundary_context(timestamps: &[i64]) -> datafusion::prelude::SessionContext {
        use arrow::{array::Int64Array, record_batch::RecordBatch};
        use datafusion::datasource::MemTable;

        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", arrow_schema::DataType::Int64, false),
            Field::new("id", arrow_schema::DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(timestamps.to_vec())),
                Arc::new(Int64Array::from_iter_values(0..timestamps.len() as i64)),
            ],
        )
        .unwrap();
        let ctx = datafusion::prelude::SessionContext::new();
        ctx.register_table(
            "seam_rows",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
        ctx
    }

    async fn boundary_query(
        ctx: &datafusion::prelude::SessionContext,
        start: i64,
        end: i64,
        limit: i64,
        descending: bool,
        histogram: bool,
    ) -> Response {
        use arrow::array::Int64Array;

        let projection = if histogram {
            "CAST(floor(_timestamp / 1000000.0) AS BIGINT) * 1000000 AS _timestamp, count(*) AS id"
        } else {
            "_timestamp, id"
        };
        let group_by = if histogram { "GROUP BY 1" } else { "" };
        let direction = if descending { "DESC" } else { "ASC" };
        let batches = ctx
            .sql(&format!(
                "SELECT {projection} FROM seam_rows WHERE _timestamp >= {start} \
                 AND _timestamp < {end} {group_by} ORDER BY _timestamp {direction}, id ASC LIMIT {limit}"
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut hits = Vec::new();
        for batch in batches {
            let ts = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let ids = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                hits.push(serde_json::json!({"_timestamp": ts.value(row), "id": ids.value(row)}));
            }
        }
        Response {
            total: hits.len(),
            size: hits.len() as i64,
            hits,
            histogram_interval: histogram.then_some(1),
            ..Default::default()
        }
    }

    // Exercise the writer's persisted payload, the reader's clipping, real SQL
    // execution of every gap, and the final response consumed by the caller.
    // Never normalize away missing/duplicate seam rows.
    #[allow(clippy::too_many_arguments)]
    async fn assert_boundary_output(
        ctx: &datafusion::prelude::SessionContext,
        windows: &[(i64, i64)],
        query: (i64, i64),
        limit: i64,
        stable_end: i64,
        descending: bool,
        histogram: bool,
        multi: bool,
    ) {
        let interval = if histogram { 1_000_000 } else { 0 };
        let mut cached = Vec::new();
        for &(start, end) in windows {
            let mut response = boundary_query(ctx, start, end, limit, descending, histogram).await;
            let Some((start_time, end_time)) = super::super::prepare_results_for_cache(
                &mut response,
                "_timestamp",
                start,
                end,
                limit,
                histogram,
                descending,
                false,
                stable_end,
            ) else {
                continue;
            };
            let mut meta = ResultCacheMeta {
                start_time,
                end_time,
                is_aggregate: histogram,
                is_descending: descending,
            };
            let mut response = json::from_slice(&json::to_vec(&response).unwrap()).unwrap();
            let request = CacheQueryRequest {
                q_start_time: query.0,
                q_end_time: query.1,
                is_aggregate: histogram,
                ts_column: "_timestamp".to_string(),
                histogram_interval: interval,
                is_descending: descending,
                is_histogram_non_ts_order: false,
            };
            if clip_cached_response(&mut response, &mut meta, &request) {
                cached.push(CachedQueryResponse {
                    cached_response: response,
                    response_start_time: meta.start_time,
                    response_end_time: meta.end_time,
                    has_cached_data: true,
                    ..Default::default()
                });
            }
        }
        assert_eq!(
            cached.len(),
            windows.len(),
            "regression must exercise cache coverage"
        );
        let deltas = if multi || cached.is_empty() {
            calculate_deltas_multi(&cached, query.0, query.1, histogram, descending, interval).0
        } else {
            assert_eq!(cached.len(), 1);
            let mut deltas = Vec::new();
            calculate_deltas(
                &ResultCacheMeta {
                    start_time: cached[0].response_start_time,
                    end_time: cached[0].response_end_time,
                    is_aggregate: histogram,
                    is_descending: descending,
                },
                query.0,
                query.1,
                interval,
                &mut deltas,
            );
            deltas
        };
        let mut fresh = Vec::new();
        for delta in deltas {
            fresh.push(
                boundary_query(
                    ctx,
                    delta.delta_start_time,
                    delta.delta_end_time,
                    limit,
                    descending,
                    histogram,
                )
                .await,
            );
        }
        let actual = super::super::merge_response(
            "cache-boundary-regression",
            &mut cached.into_iter().map(|r| r.cached_response).collect(),
            &mut fresh,
            "_timestamp",
            limit,
            descending,
            0,
            vec![
                (
                    "_timestamp".to_string(),
                    if descending {
                        OrderBy::Desc
                    } else {
                        OrderBy::Asc
                    },
                ),
                ("id".to_string(), OrderBy::Asc),
            ],
        );
        let expected = boundary_query(ctx, query.0, query.1, limit, descending, histogram).await;
        assert_eq!(
            actual.hits, expected.hits,
            "windows={windows:?}, query={query:?}, limit={limit}, descending={descending}, multi={multi}"
        );
        assert_eq!(actual.total, expected.total);
    }

    #[tokio::test]
    async fn test_cache_boundary_rows_match_uncached_sql() {
        let ctx = boundary_context(&[
            100, 200, 430, 430, 430, 500, 600, 700, 700, 700, 700, 800, 900,
        ]);
        for descending in [false, true] {
            for multi in [false, true] {
                // No cache, full cache, and an exhausted prefix with three rows
                // exactly at its exclusive end.
                for windows in [vec![], vec![(100, 900)], vec![(100, 430)]] {
                    assert_boundary_output(
                        &ctx,
                        &windows,
                        (100, 900),
                        100,
                        1000,
                        descending,
                        false,
                        multi,
                    )
                    .await;
                }
                // Clipping must exclude every row at the new exclusive end.
                assert_boundary_output(
                    &ctx,
                    &[(100, 900)],
                    (200, 700),
                    100,
                    1000,
                    descending,
                    false,
                    multi,
                )
                .await;
                // An empty subwindow is still covered; row extrema cannot define coverage.
                assert_boundary_output(
                    &ctx,
                    &[(100, 900)],
                    (250, 400),
                    100,
                    1000,
                    descending,
                    false,
                    multi,
                )
                .await;
                // LIMIT lands inside a timestamp tie in either sort direction.
                assert_boundary_output(
                    &ctx,
                    &[(100, 900)],
                    (100, 1000),
                    4,
                    1000,
                    descending,
                    false,
                    multi,
                )
                .await;
                // The cache-delay cutoff has the same exclusive-end semantics.
                assert_boundary_output(
                    &ctx,
                    &[(100, 900)],
                    (100, 900),
                    100,
                    430,
                    descending,
                    false,
                    multi,
                )
                .await;
            }
            assert_boundary_output(
                &ctx,
                &[(100, 430), (600, 800)],
                (100, 1000),
                100,
                1000,
                descending,
                false,
                true,
            )
            .await;
        }
    }

    #[tokio::test]
    async fn test_cache_boundary_histograms_match_uncached_sql() {
        let ctx = boundary_context(&[
            0, 500_000, 1_000_000, 1_500_000, 2_000_000, 2_500_000, 3_000_000, 3_500_000,
            4_000_000, 4_500_000,
        ]);
        for descending in [false, true] {
            for multi in [false, true] {
                // The last complete bucket ends exactly at the clipped query end.
                assert_boundary_output(
                    &ctx,
                    &[(0, 5_000_000)],
                    (1_000_000, 4_000_000),
                    100,
                    5_000_000,
                    descending,
                    true,
                    multi,
                )
                .await;
                // Partial request and delay buckets are re-queried, never split.
                assert_boundary_output(
                    &ctx,
                    &[(500_000, 4_500_000)],
                    (0, 5_000_000),
                    100,
                    3_500_000,
                    descending,
                    true,
                    multi,
                )
                .await;
                // A saturated terminal bucket must be fetched in its entirety.
                assert_boundary_output(
                    &ctx,
                    &[(0, 4_000_000)],
                    (0, 5_000_000),
                    3,
                    5_000_000,
                    descending,
                    true,
                    multi,
                )
                .await;
            }
        }
    }

    #[test]
    fn test_get_ts_col_order_by() {
        let sql = Sql {
            metadata: Arc::new(crate::service::search::sql::SqlMetadata {
                sql: "SELECT _timestamp, field1 FROM logs ORDER BY _timestamp DESC".to_string(),
                is_complex: false,
                org_id: "test_org".to_string(),
                stream_type: StreamType::Logs,
                stream_names: vec![TableReference::from("logs")],
                has_match_all: false,
                equal_items: hashbrown::HashMap::new(),
                columns: {
                    let mut cols = hashbrown::HashMap::new();
                    let mut set = hashbrown::HashSet::new();
                    set.insert("_timestamp".to_string());
                    set.insert("field1".to_string());
                    cols.insert(TableReference::from("logs"), set);
                    cols
                },
                aliases: vec![("_timestamp".to_string(), "_timestamp".to_string())],
                schemas: {
                    let mut schemas = hashbrown::HashMap::new();
                    schemas.insert(
                        TableReference::from("logs"),
                        Arc::new(SchemaCache::new(Schema::empty())),
                    );
                    schemas
                },
                limit: 100,
                offset: 0,
                group_by: vec![],
                order_by: vec![("_timestamp".to_string(), OrderBy::Desc)],
                histogram_interval: None,
                timezone: None,
                sorted_by_time: true,
                pagination: Default::default(),
            }),
            time_range: (0, 0),
            sampling_config: None,
        };

        let result = get_ts_col_order_by(&sql, "_timestamp", false);
        assert!(result.is_some());
        let (ts_col, is_descending) = result.unwrap();
        assert_eq!(ts_col, "_timestamp");
        assert!(is_descending);
    }

    #[tokio::test]
    async fn test_cache_boundary_retention_matches_live_histogram() {
        let old = boundary_context(&[0, 500_000, 1_000_000, 1_500_000, 2_000_000, 2_500_000]);
        let live = boundary_context(&[1_500_000, 2_000_000, 2_500_000]);
        let response = boundary_query(&old, 0, 3_000_000, 100, false, true).await;
        let cached = CachedQueryResponse {
            cached_response: response,
            response_start_time: 0,
            response_end_time: 3_000_000,
            ts_column: "_timestamp".to_string(),
            has_cached_data: true,
            ..Default::default()
        };
        let org = "cache_boundary_retention_regression";
        infra::cache::stats::set_stream_stats(
            org,
            "histogram",
            StreamType::Logs,
            config::meta::stream::StreamStats {
                doc_time_min: 1_500_000,
                ..Default::default()
            },
        );
        let retained = invalidate_cached_response_by_stream_min_ts(
            &format!("{org}/logs/histogram"),
            &[cached],
            1_000_000,
        )
        .await;
        infra::cache::stats::remove_stream_stats(org, "histogram", StreamType::Logs);
        let retained = retained.unwrap();
        assert_eq!(retained.len(), 1);
        let deltas = calculate_deltas_multi(&retained, 0, 3_000_000, true, false, 1_000_000).0;
        let mut fresh = Vec::new();
        for delta in deltas {
            fresh.push(
                boundary_query(
                    &live,
                    delta.delta_start_time,
                    delta.delta_end_time,
                    100,
                    false,
                    true,
                )
                .await,
            );
        }
        let actual = super::super::merge_response(
            "retention-boundary",
            &mut retained.into_iter().map(|r| r.cached_response).collect(),
            &mut fresh,
            "_timestamp",
            100,
            false,
            0,
            vec![],
        );
        let expected = boundary_query(&live, 0, 3_000_000, 100, false, true).await;
        assert_eq!(actual.hits, expected.hits);
        assert_eq!(actual.total, expected.total);
    }

    #[tokio::test]
    async fn test_check_cache() {
        // Test case 1: Basic cache check with valid SQL
        let trace_id = "test_trace_123";
        let org_id = "test_org";
        let stream_type = StreamType::Logs;

        // Add the stream to the STREAM_SCHEMA_LATEST cache
        let schema = Schema::new(vec![
            Field::new("_timestamp", arrow_schema::DataType::Int64, false),
            Field::new("message", arrow_schema::DataType::Utf8, true),
        ]);
        {
            let mut w = STREAM_SCHEMAS_LATEST.write().await;
            w.insert(
                format!("{org_id}/{stream_type}/logs"),
                SchemaCache::new(schema),
            );
        } // Lock is dropped here before calling check_cache

        let mut req = Request {
            query: Query {
                sql: "SELECT _timestamp, message FROM logs WHERE _timestamp >= 1640995200000000 AND _timestamp <= 1641081600000000 ORDER BY _timestamp DESC LIMIT 100".to_string(),
                start_time: 1640995200000000,
                end_time: 1641081600000000,
                from: 0,
                size: 100,
                track_total_hits: false,
                query_fn: None,
                quick_mode: false,
                query_type: "sql".to_string(),
                uses_zo_fn: false,
                action_id: None,
                skip_wal: false,
                streaming_output: false,
                streaming_id: None,
                histogram_interval: 0,
                timezone: None,
                sampling_ratio: None,
                sampling_config: None,
            },
            encoding: RequestEncoding::Empty,
            regions: vec![],
            clusters: vec![],
            timeout: 30,
            search_type: Some(SearchEventType::UI),
            search_event_context: None,
            use_cache: true,
            clear_cache: false,
            local_mode: None,
        };
        let mut origin_sql = req.query.sql.clone();
        let file_path = "test_org/logs/test_stream".to_string();
        let is_aggregate = false;
        let mut should_exec_query = true;

        // Parse SQL to get metadata (new signature requires this)
        let query: SearchQuery = req.query.clone().into();
        let sql = Sql::new(&query, org_id, stream_type, req.search_type)
            .await
            .unwrap();
        let (result_ts_col, is_descending) =
            get_ts_col_order_by(&sql, TIMESTAMP_COL_NAME, is_aggregate).unwrap_or_default();

        let result = check_cache(
            trace_id,
            org_id,
            &mut req,
            &mut origin_sql,
            &file_path,
            is_aggregate,
            &sql,
            &result_ts_col,
            is_descending,
            &mut should_exec_query,
        )
        .await;

        assert!(result.cache_query_response);
        assert_eq!(result.ts_column, "_timestamp");
        assert!(result.is_descending);
        assert_eq!(result.limit, 100);
        assert_eq!(result.file_path, "test_org/logs/test_stream");
    }
}
