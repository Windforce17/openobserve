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

use std::{path::Path, sync::Arc};

use chrono::DateTime;
use config::{
    cluster::LOCAL_NODE,
    get_config,
    meta::{
        search::{ScanStats, StorageType},
        stream::{FileKey, PartitionTimeLevel, StreamParams, StreamPartition},
    },
    utils::{
        async_file::{create_wal_dir_datetime_filter, scan_files_filtered},
        file::is_exists,
        parquet::{
            get_memtable_id_from_file_name, parse_time_range_from_filename, read_metadata_from_file,
        },
        record_batch_ext::concat_batches,
        size::bytes_to_human_readable,
        time::{DAY_MICRO_SECS, HOUR_MICRO_SECS},
    },
};
use datafusion::{
    arrow::{datatypes::Schema, record_batch::RecordBatch},
    execution::cache::cache_manager::FileStatisticsCache,
};
use futures::StreamExt;
use hashbrown::{HashMap, HashSet};
use infra::{
    errors::{Error, ErrorCodes},
    schema::get_partition_time_level,
};
use ingester::WAL_PARQUET_METADATA;
use rayon::iter::{IntoParallelIterator, ParallelIterator};

use crate::{
    common::infra::wal,
    service::{
        file_list,
        search::{
            datafusion::table_provider::memtable::NewMemTable,
            generate_filter_from_equal_items,
            index::IndexCondition,
            inspector::{SearchInspectorFieldsBuilder, search_inspector_fields},
            match_source,
        },
    },
};

/// search in local WAL, which haven't been sync to object storage
#[tracing::instrument(name = "service:search:wal:parquet", skip_all, fields(org_id = query.org_id, stream_name = query.stream_name))]
#[allow(clippy::too_many_arguments)]
pub async fn search_parquet(
    query: Arc<super::QueryParams>,
    schema: Arc<Schema>,
    search_partition_keys: &[(String, String)],
    sorted_by_time: bool,
    file_stat_cache: Option<Arc<dyn FileStatisticsCache>>,
    index_condition: Option<IndexCondition>,
    fst_fields: Vec<String>,
    memtable_ids: HashSet<u64>,
) -> super::SearchTable {
    let load_start = std::time::Instant::now();
    let trace_id = &query.trace_id;
    // get file list
    let stream_settings =
        infra::schema::get_settings(&query.org_id, &query.stream_name, query.stream_type)
            .await
            .unwrap_or_default();
    let partition_time_level = get_partition_time_level(query.stream_type);
    let files = get_file_list(
        query.clone(),
        &stream_settings.partition_keys,
        Some(query.time_range),
        search_partition_keys,
        partition_time_level,
        memtable_ids,
    )
    .await?;
    log::info!(
        "[trace_id {trace_id}] wal->parquet->search: get file list files: {}, took {} ms",
        files.len(),
        load_start.elapsed().as_millis()
    );
    if files.is_empty() {
        return Ok((vec![], ScanStats::new(), HashSet::new()));
    }

    let mut lock_files = files.iter().map(|f| f.key.clone()).collect::<Vec<_>>();
    let cfg = get_config();
    // get file metadata to build file_list
    let files_num = files.len();
    let mut new_files = Vec::with_capacity(files_num);
    let files_metadata = futures::stream::iter(files)
        .map(|mut file| async move {
            let cfg = get_config();
            let r = WAL_PARQUET_METADATA.read().await;
            let source_file = cfg.common.data_wal_dir.to_string() + file.key.as_str();
            if let Some(meta) = r.get(file.key.as_str()) {
                file.meta = meta.clone();
                // reset file meta if it already removed
                if !is_exists(&source_file) {
                    file.meta = Default::default();
                }
                return file;
            }
            drop(r);
            let path = source_file.into();
            let meta = read_metadata_from_file(&path).await.unwrap_or_default();
            file.meta = meta;
            file
        })
        .buffer_unordered(cfg.limit.cpu_num)
        .collect::<Vec<FileKey>>()
        .await;
    for file in files_metadata {
        if file.meta.is_empty() {
            wal::release_files(std::slice::from_ref(&file.key));
            lock_files.retain(|f| f != &file.key);
            continue;
        }
        let (min_ts, max_ts) = query.time_range;
        if file.meta.min_ts > max_ts || file.meta.max_ts < min_ts {
            log::debug!(
                "[trace_id {trace_id}] skip wal parquet file: {} time_range: [{},{})",
                file.key,
                file.meta.min_ts,
                file.meta.max_ts
            );
            wal::release_files(std::slice::from_ref(&file.key));
            lock_files.retain(|f| f != &file.key);
            continue;
        }
        new_files.push(file);
    }
    let files = new_files;

    let mut scan_stats = ScanStats::new();
    scan_stats.files = files.len() as i64;
    if scan_stats.files == 0 {
        // release all files
        wal::release_files(&lock_files);
        return Ok((vec![], scan_stats, HashSet::new()));
    }

    let scan_stats = match file_list::calculate_files_size(&files).await {
        Ok(size) => size,
        Err(err) => {
            // release all files
            wal::release_files(&lock_files);
            log::error!("[trace_id {trace_id}] calculate files size error: {err}",);
            return Err(Error::ErrorCode(ErrorCodes::ServerInternalError(
                "calculate files size error".to_string(),
            )));
        }
    };

    log::info!(
        "{}",
        search_inspector_fields(
            format!(
                "[trace_id {trace_id}] wal->parquet->search: load files {}, scan_size {}, compressed_size {}",
                files.len(),
                scan_stats.original_size,
                scan_stats.compressed_size
            ),
            SearchInspectorFieldsBuilder::new()
                .trace_id(trace_id.to_string())
                .node_name(LOCAL_NODE.name.clone())
                .component("wal:parquet load".to_string())
                .search_role("follower".to_string())
                .duration(load_start.elapsed().as_millis() as usize)
                .desc(format!(
                    "wal parquet search load files {}, scan_size {}, compressed_size {}",
                    files.len(),
                    bytes_to_human_readable(scan_stats.original_size as f64),
                    bytes_to_human_readable(scan_stats.compressed_size as f64)
                ))
                .build()
        )
    );

    // check memory circuit breaker
    if let Err(e) = ingester::check_memory_circuit_breaker() {
        // release all files
        wal::release_files(&lock_files);
        return Err(Error::ResourceError(e.to_string()));
    }

    let session = config::meta::search::Session {
        id: format!("{trace_id}-wal"),
        storage_type: StorageType::Wal,
        work_group: query.work_group.clone(),
        target_partitions: cfg.limit.cpu_num,
    };

    let start = std::time::Instant::now();
    let tables = super::create_tables_from_files(
        files,
        session,
        query.clone(),
        schema,
        sorted_by_time,
        file_stat_cache,
        index_condition,
        fst_fields,
        || {
            wal::release_files(&lock_files);
        },
    )
    .await?;

    // lock these files for this request
    wal::lock_request(&query.trace_id, &lock_files);

    log::info!(
        "{}",
        search_inspector_fields(
            format!(
                "[trace_id {trace_id}] wal->parquet->search: create tables took {} ms",
                start.elapsed().as_millis()
            ),
            SearchInspectorFieldsBuilder::new()
                .trace_id(trace_id.to_string())
                .node_name(LOCAL_NODE.name.clone())
                .component("wal:parquet create tables".to_string())
                .search_role("follower".to_string())
                .duration(start.elapsed().as_millis() as usize)
                .build()
        )
    );

    Ok((tables, scan_stats, HashSet::new()))
}

/// search in local WAL, which haven't been sync to object storage
#[tracing::instrument(name = "service:search:wal:memtable", skip_all, fields(org_id = query.org_id, stream_name = query.stream_name))]
pub async fn search_memtable(
    query: Arc<super::QueryParams>,
    schema: Arc<Schema>,
    search_partition_keys: &[(String, String)],
    sorted_by_time: bool,
    index_condition: Option<IndexCondition>,
    fst_fields: Vec<String>,
) -> super::SearchTable {
    let load_start = std::time::Instant::now();
    let mut scan_stats = ScanStats::new();

    // format partition keys
    let stream_settings =
        infra::schema::get_settings(&query.org_id, &query.stream_name, query.stream_type)
            .await
            .unwrap_or_default();
    let partition_keys = &stream_settings.partition_keys;
    let mut filters = generate_filter_from_equal_items(search_partition_keys);
    let partition_keys: HashMap<&String, &StreamPartition> =
        partition_keys.iter().map(|v| (&v.field, v)).collect();
    for (key, value) in filters.iter_mut() {
        if let Some(partition_key) = partition_keys.get(key) {
            for val in value.iter_mut() {
                *val = partition_key.get_partition_value(val);
            }
        }
    }

    let start = std::time::Instant::now();
    let (mut memtable_ids, mut batches) = ingester::read_from_memtable(
        &query.org_id,
        query.stream_type.as_str(),
        &query.stream_name,
        Some(query.time_range),
        &filters,
    )
    .await
    .unwrap_or_default();
    let memtable_time = start.elapsed().as_millis();
    let start = std::time::Instant::now();
    let (im_ids, im_batches) = ingester::read_from_immutable(
        &query.trace_id,
        &query.org_id,
        query.stream_type.as_str(),
        &query.stream_name,
        Some(query.time_range),
        &filters,
        &memtable_ids,
    )
    .await
    .unwrap_or_default();
    let immutable_time = start.elapsed().as_millis();
    memtable_ids.extend(im_ids);
    batches.extend(im_batches);

    log::info!(
        "[trace_id {}] wal->mem->search: read from metable took {memtable_time} ms, immutable took {immutable_time} ms",
        query.trace_id,
    );

    scan_stats.files = batches.iter().map(|(_, k)| k.len()).sum::<usize>() as i64;
    if scan_stats.files == 0 {
        return Ok((vec![], ScanStats::new(), HashSet::new()));
    }

    let mut record_batches = Vec::with_capacity(scan_stats.files as usize);
    for (_schema, batch) in batches {
        for r in batch.iter() {
            scan_stats.records += r.data.num_rows() as i64;
            scan_stats.original_size += r.data_json_size as i64;
            scan_stats.compressed_size += r.data_arrow_size as i64;
        }
        record_batches.extend(batch.into_iter().map(|r| r.data.clone()));
    }
    let batch_groups = group_by_batch_schema(record_batches);

    log::info!(
        "{}",
        search_inspector_fields(
            format!(
                "[trace_id {}] wal->mem->search: load groups {}, files {}, scan_size {}, compressed_size {}",
                query.trace_id,
                batch_groups.len(),
                scan_stats.files,
                scan_stats.original_size,
                scan_stats.compressed_size,
            ),
            SearchInspectorFieldsBuilder::new()
                .trace_id(query.trace_id.to_string())
                .node_name(LOCAL_NODE.name.clone())
                .component("wal:memtable load".to_string())
                .search_role("follower".to_string())
                .duration(load_start.elapsed().as_millis() as usize)
                .desc(format!(
                    "wal mem search load groups {}, files {}, scan_size {}, compressed_size {}",
                    batch_groups.len(),
                    scan_stats.files,
                    bytes_to_human_readable(scan_stats.original_size as f64),
                    bytes_to_human_readable(scan_stats.compressed_size as f64)
                ))
                .build()
        )
    );

    // check memory circuit breaker
    ingester::check_memory_circuit_breaker().map_err(|e| Error::ResourceError(e.to_string()))?;

    // construct latest schema map
    let latest_schema = Arc::new(schema.as_ref().clone().with_metadata(Default::default()));
    let mut latest_schema_map = HashMap::with_capacity(latest_schema.fields().len());
    for field in latest_schema.fields() {
        latest_schema_map.insert(field.name(), field);
    }

    let mut tables = Vec::new();
    let start = std::time::Instant::now();
    let latest_schema_fields = latest_schema.fields().len();
    for (i, (schema, record_batches)) in batch_groups.into_iter().enumerate() {
        if record_batches.is_empty() {
            continue;
        }

        let batch_fields = schema.fields().len();
        // The group's batches stay RAW (present fields only, no `_source`):
        // `NewMemTable` adapts raw -> plan per streamed batch at scan time —
        // null-padding, type casts, and LAZY `_source` synthesis. Adapting
        // here eagerly synthesized `_source` for every row of every batch in
        // range and retained it, so each concurrent star query held the
        // memtable's whole JSON image (12-24GB for a 6GB memtable) and
        // OOMKilled prod ingesters (2026-07-30).
        log::debug!(
            "[trace_id {}] wal->mem->search: group {i}, plan fields {latest_schema_fields}, raw fields {batch_fields}",
            query.trace_id,
        );

        // merge small batches into big batches
        let merge_start = std::time::Instant::now();
        let batch_num = record_batches.len();
        let mut merge_groupes = Vec::new();
        let mut current_group = Vec::new();
        let group_limit = config::get_batch_size();
        let mut group_size = 0;
        for batch in record_batches {
            if group_size > 0 && group_size + batch.num_rows() > group_limit {
                merge_groupes.push(current_group);
                current_group = Vec::new();
                group_size = 0;
            }
            group_size += batch.num_rows();
            current_group.push(batch);
        }
        if !current_group.is_empty() {
            merge_groupes.push(current_group);
        }
        // Groups are schema-homogeneous by construction above, so concat
        // cannot type-mismatch — but never unwrap on data-shaped input: a
        // panic here unwinds through rayon into the gRPC handler and resets
        // the stream, which the leader renders as a silently-partial result.
        let record_batches = merge_groupes
            .into_par_iter()
            .map(|mut group| {
                if group.len() == 1 {
                    Ok(group.remove(0))
                } else {
                    concat_batches(group[0].schema().clone(), group)
                }
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                Error::Message(format!(
                    "wal->mem->search: concat memtable batches for group {i} failed: {e}"
                ))
            })?;

        log::info!(
            "[trace_id {}] wal->mem->search: merge batches for group {i}, batches {batch_num}, took {} ms",
            query.trace_id,
            merge_start.elapsed().as_millis()
        );

        tokio::task::coop::consume_budget().await;

        let table = match NewMemTable::try_new(
            record_batches[0].schema().clone(),
            vec![record_batches],
            latest_schema.clone(),
            sorted_by_time,
            index_condition.clone(),
            fst_fields.clone(),
            query.time_range,
        ) {
            Ok(table) => Arc::new(table),
            Err(e) => {
                log::error!(
                    "[trace_id {}] wal->mem->search: create memtable error: {e}",
                    query.trace_id,
                );
                return Err(e.into());
            }
        };
        tables.push(table as _);
    }

    log::info!(
        "{}",
        search_inspector_fields(
            format!(
                "[trace_id {}] wal->mem->search: create tables took {} ms",
                query.trace_id,
                start.elapsed().as_millis()
            ),
            SearchInspectorFieldsBuilder::new()
                .trace_id(query.trace_id.to_string())
                .node_name(LOCAL_NODE.name.clone())
                .component("wal:memtable create tables".to_string())
                .search_role("follower".to_string())
                .duration(start.elapsed().as_millis() as usize)
                .build()
        )
    );
    Ok((tables, scan_stats, memtable_ids))
}

#[tracing::instrument(name = "service:search:grpc:wal:get_file_list", skip_all, fields(org_id = query.org_id, stream_name = query.stream_name))]
async fn get_file_list(
    query: Arc<super::QueryParams>,
    partition_keys: &[StreamPartition],
    time_range: Option<(i64, i64)>,
    search_partition_keys: &[(String, String)],
    partition_time_level: PartitionTimeLevel,
    memtable_ids: HashSet<u64>,
) -> Result<Vec<FileKey>, Error> {
    let wal_dir = match Path::new(&get_config().common.data_wal_dir).canonicalize() {
        Ok(path) => {
            let mut path = path.to_str().unwrap().to_string();
            // Hack for windows
            if let Some(stripped) = path.strip_prefix("\\\\?\\") {
                path = stripped.to_string().replace('\\', "/");
            }
            path
        }
        Err(_) => {
            return Ok(vec![]);
        }
    };

    // get all files
    let pattern = format!(
        "{}/files/{}/{}/{}/",
        wal_dir, query.org_id, query.stream_type, query.stream_name
    );
    let files = {
        let (start_ts, end_ts) = query.time_range;

        // normalize timestamps to partition boundaries
        let normalized = if partition_time_level == PartitionTimeLevel::Daily {
            (
                start_ts - start_ts % DAY_MICRO_SECS,
                end_ts - end_ts % DAY_MICRO_SECS,
            )
        } else {
            (
                start_ts - start_ts % HOUR_MICRO_SECS,
                end_ts - end_ts % HOUR_MICRO_SECS,
            )
        };

        if let Some((start_time, end_time)) = DateTime::from_timestamp_micros(normalized.0)
            .zip(DateTime::from_timestamp_micros(normalized.1))
        {
            let skip_count = AsRef::<Path>::as_ref(&pattern).components().count();
            // Skip count is the number of segments in the cannonicalised path before
            // <YY>/<MM>/<DD>/<HH>/<file> appear
            let filter = create_wal_dir_datetime_filter(start_time, end_time, skip_count + 1);

            scan_files_filtered(&pattern, filter, None).await?
        } else {
            scan_files_filtered(&pattern, |_| true, None).await?
        }
    };

    let files = files
        .iter()
        .filter_map(|f| {
            let memtable_id = get_memtable_id_from_file_name(f);

            // filter out parquet file that already in memtable, because it will be duplicated
            if memtable_id > 0 && memtable_ids.contains(&memtable_id) {
                log::debug!(
                    "[trace_id {}] skip wal parquet file: {f} because memtable id: {memtable_id} already in memtable",
                    query.trace_id,
                );
                return None;
            }

            Some(
                f.strip_prefix(&wal_dir)
                    .unwrap()
                    .to_string()
                    .replace('\\', "/")
                    .trim_start_matches('/')
                    .to_string(),
            )
        })
        .collect::<Vec<_>>();

    if files.is_empty() {
        return Ok(vec![]);
    }

    // filter by pending delete
    let mut files = crate::service::db::file_list::local::filter_by_pending_delete(files).await;
    if files.is_empty() {
        return Ok(vec![]);
    }

    let files_num = files.len();
    files.sort_unstable();
    files.dedup();
    if files_num != files.len() {
        log::warn!(
            "[trace_id {}] wal->parquet->search: found duplicate files from {files_num} to {}",
            query.trace_id,
            files.len()
        );
    }

    // lock theses files
    wal::lock_files(&files);

    let stream_params = Arc::new(StreamParams::new(
        &query.org_id,
        &query.stream_name,
        query.stream_type,
    ));
    let mut filters = generate_filter_from_equal_items(search_partition_keys);
    let partition_keys: HashMap<&String, &StreamPartition> =
        partition_keys.iter().map(|v| (&v.field, v)).collect();
    for (key, value) in filters.iter_mut() {
        if let Some(partition_key) = partition_keys.get(key) {
            for val in value.iter_mut() {
                *val = partition_key.get_partition_value(val);
            }
        }
    }

    let mut result = Vec::with_capacity(files.len());
    let (min_ts, max_ts) = query.time_range;
    for file in files.iter() {
        let file_key = FileKey::from_file_name(file);
        if (min_ts, max_ts) != (0, 0) {
            let (file_min_ts, file_max_ts) = parse_time_range_from_filename(file);
            if (file_min_ts > 0 && file_max_ts > 0)
                && ((max_ts > 0 && file_min_ts > max_ts) || (min_ts > 0 && file_max_ts < min_ts))
            {
                log::debug!(
                    "[trace_id {}] skip wal parquet file: {file} time_range: [{file_min_ts},{file_max_ts}]",
                    query.trace_id,
                );
                wal::release_files(std::slice::from_ref(file));
                continue;
            }
        }

        if match_source(stream_params.clone(), time_range, &filters, &file_key).await {
            result.push(file_key);
        } else {
            wal::release_files(std::slice::from_ref(file));
        }
    }
    Ok(result)
}

/// Group memtable batches by each batch's OWN schema, not the
/// partition-reported one: entries under one memtable partition carry their
/// write-time schemas, so a field that type-flipped between writes
/// (Int64 <-> Utf8 is routine for dynamic JSON) puts mixed-type batches
/// under a single reported schema. Concatenation requires true homogeneity —
/// grouping by the reported schema panicked every ingester fleet-wide
/// (2026-07-30).
fn group_by_batch_schema<I>(batches: I) -> HashMap<Arc<Schema>, Vec<RecordBatch>>
where
    I: IntoIterator<Item = RecordBatch>,
{
    let mut groups: HashMap<Arc<Schema>, Vec<RecordBatch>> = HashMap::with_capacity(2);
    for batch in batches {
        groups.entry(batch.schema()).or_default().push(batch);
    }
    groups
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use config::utils::record_batch_ext::concat_batches;
    use datafusion::arrow::{
        array::{Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };

    use super::group_by_batch_schema;

    fn int_batch(field: &str, vals: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(field, DataType::Int64, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vals.to_vec()))]).unwrap()
    }

    fn str_batch(field: &str, vals: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(field, DataType::Utf8, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vals.to_vec()))]).unwrap()
    }

    #[test]
    fn type_flipped_batches_land_in_separate_groups_and_concat_cleanly() {
        // same field NAME, different write-time types — the 2026-07-30
        // incident shape; one group per actual schema, concat within each
        // group must succeed
        let groups = group_by_batch_schema(vec![
            int_batch("code", &[200, 404]),
            str_batch("code", &["200", "404"]),
            int_batch("code", &[500]),
        ]);
        assert_eq!(groups.len(), 2);
        for (schema, batches) in groups {
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            let merged = concat_batches(schema, batches).expect("homogeneous group must concat");
            assert_eq!(merged.num_rows(), rows);
        }
    }

    #[test]
    fn identical_schemas_share_one_group() {
        let groups =
            group_by_batch_schema(vec![int_batch("code", &[1]), int_batch("code", &[2, 3])]);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups.into_values().next().unwrap().len(), 2);
    }
}
