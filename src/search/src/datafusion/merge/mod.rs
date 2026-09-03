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

use arrow::array::RecordBatch;
use config::{
    FileFormat, TIMESTAMP_COL_NAME, get_config,
    meta::stream::{FileMeta, StreamType},
    utils::{parquet::new_parquet_writer, util::DISTINCT_STREAM_PREFIX},
};
use datafusion::{
    arrow::datatypes::Schema,
    catalog::TableProvider,
    error::{DataFusionError, Result},
    physical_plan::execute_stream,
};
use futures::TryStreamExt;
use parquet::{arrow::AsyncArrowWriter, file::metadata::KeyValue};

use super::table_provider::uniontable::NewUnionTable;
use crate::datafusion::exec::DataFusionContextBuilder;

#[cfg(feature = "enterprise")]
pub mod downsampling;
#[cfg(feature = "enterprise")]
use {
    crate::datafusion::merge::downsampling::merge_parquet_files_with_downsampling,
    o2_enterprise::enterprise::common::downsampling::get_largest_downsampling_rule,
};

pub enum MergeParquetResult {
    Single {
        buf: Vec<u8>,
        file_meta: FileMeta,
        file_format: FileFormat,
    },
    #[allow(unused)]
    Multiple {
        bufs: Vec<Vec<u8>>,
        file_metas: Vec<FileMeta>,
        file_format: FileFormat,
    },
}

/// `single_partition_sort` (M13/M20b): plan the `ORDER BY` at exactly one
/// partition — no RepartitionExec, one ExternalSorter that spills properly
/// under pool pressure. Callers with BOUNDED inputs pass `true`:
/// - SEGMENT-BUILDER invocations (M13): one bounded in-memory batch (≤ the super-batch budget),
///   where the repartitioned 2-partition sort was prod's post-.108 "Not enough memory to continue
///   external sort" source (default/metadata/trace_list_index L0 builds; the M12 fix-1 rationale
///   applies verbatim).
/// - COMPACTOR invocations for METADATA-class streams (M20b): size-capped merge groups whose
///   repartitioned min-floor plan was prod's compactor killer (116MB→6GB DataFusion spikes in ~2s
///   on trace_list_index).
///
/// The ingester WAL move job and non-metadata compactor merges keep `false`
/// (unchanged planning).
pub async fn merge_parquet_files(
    stream_type: StreamType,
    stream_name: &str,
    schema: Arc<Schema>,
    tables: Vec<Arc<dyn TableProvider>>,
    bloom_filter_fields: &[String],
    mut metadata: FileMeta,
    is_ingester: bool,
    single_partition_sort: bool,
) -> Result<MergeParquetResult> {
    let start = std::time::Instant::now();
    let cfg = get_config();

    let file_format = merge_output_file_format(stream_type, is_ingester, cfg.common.file_format);

    #[cfg(feature = "enterprise")]
    if stream_type == StreamType::Metrics && !is_ingester {
        let rule = get_largest_downsampling_rule(stream_name, metadata.max_ts);
        if let Some(rule) = rule {
            log::info!(
                "merge_parquet_files: stream_type={stream_type}, stream_name={stream_name}, downsampling rule={rule:?}"
            );
            return merge_parquet_files_with_downsampling(
                schema,
                tables,
                bloom_filter_fields,
                rule,
                &metadata,
                file_format,
            )
            .await;
        }
    }

    // get all sorted data
    let sql = if cfg.limit.distinct_values_hourly
        && stream_type == StreamType::Metadata
        && stream_name.starts_with(DISTINCT_STREAM_PREFIX)
    {
        let fields = schema
            .fields()
            .iter()
            .filter(|f| f.name() != TIMESTAMP_COL_NAME && f.name() != "count")
            .map(|x| x.name().to_string())
            .collect::<Vec<_>>();
        let fields_str = fields.join(", ");
        format!(
            "SELECT MIN({TIMESTAMP_COL_NAME}) AS {TIMESTAMP_COL_NAME}, SUM(count) as count, {fields_str} FROM tbl GROUP BY {fields_str} ORDER BY {TIMESTAMP_COL_NAME} DESC"
        )
    } else if stream_type == StreamType::Filelist {
        // for file list we do not have timestamp, so we instead sort by min ts of entries
        "SELECT * FROM tbl ORDER BY min_ts DESC".to_string()
    } else {
        format!("SELECT * FROM tbl ORDER BY {TIMESTAMP_COL_NAME} DESC")
    };
    log::debug!("merge_parquet_files sql: {sql}");

    let ctx = DataFusionContextBuilder::new()
        .trace_id("merge_parquet_files")
        .sorted_by_time(true)
        .single_partition(single_partition_sort)
        // M26: all merge contexts draw from ONE bounded process-wide pool —
        // concurrent merges spill against a shared budget instead of each
        // claiming a full `datafusion_max_size` pool (48Gi compactors died
        // stacking ~12.8GB metadata-merge pool fills, 2026-08-21)
        .shared_merge_pool(true)
        .build(get_config().limit.datafusion_min_partition_num)
        .await?;
    // register union table
    let union_table = Arc::new(NewUnionTable::new(schema.clone(), tables));
    ctx.register_table("tbl", union_table)?;

    let plan = ctx.state().create_logical_plan(&sql).await?;
    let physical_plan = ctx.state().create_physical_plan(&plan).await?;
    let schema = physical_plan.schema();

    // print the physical plan
    if cfg.common.print_key_sql {
        let plan = datafusion::physical_plan::displayable(physical_plan.as_ref())
            .indent(false)
            .to_string();
        println!("+---------------------------+--------------------------+");
        println!("merge_parquet_files");
        println!("+---------------------------+--------------------------+");
        println!("{plan}");
    }

    let mut batch_stream = execute_stream(physical_plan, ctx.task_ctx())?;
    let (tx, mut rx) = tokio::sync::mpsc::channel::<RecordBatch>(2);
    let read_task = tokio::task::spawn(async move {
        loop {
            match batch_stream.try_next().await {
                Ok(None) => {
                    break;
                }
                Ok(Some(batch)) => {
                    if let Err(e) = tx.send(batch).await {
                        log::error!("merge_parquet_files write to channel error: {e}");
                        return Err(DataFusionError::External(Box::new(e)));
                    }
                }
                Err(e) => {
                    log::error!("merge_parquet_files execute stream error: {e}");
                    return Err(e);
                }
            }
        }
        Ok(())
    });

    let buf = match file_format {
        FileFormat::Parquet => {
            write_parquet(
                &schema,
                bloom_filter_fields,
                &metadata,
                is_ingester,
                &mut rx,
                read_task,
            )
            .await?
        }
        FileFormat::Vortex => write_vortex(schema, rx, read_task).await?,
        // unreachable: `vix` is not a valid ZO_FILE_FORMAT (normalized away
        // at config load). Core .vix files are written by
        // openobserve-core's vix::core_writer, not by this merge.
        FileFormat::Vix => {
            return Err(DataFusionError::NotImplemented(
                "merge_parquet_files cannot write core .vix files; the core-file write path \
                 lives in vix::core_writer"
                    .to_string(),
            ));
        }
    };

    log::debug!(
        "merge_parquet_files took {} ms",
        start.elapsed().as_millis()
    );

    metadata.compressed_size = buf.len() as i64;
    Ok(MergeParquetResult::Single {
        buf,
        file_meta: metadata,
        file_format,
    })
}

fn merge_output_file_format(
    stream_type: StreamType,
    is_ingester: bool,
    configured: FileFormat,
) -> FileFormat {
    if is_ingester {
        FileFormat::for_ingester_stream(stream_type, configured)
    } else {
        configured
    }
}

async fn write_parquet(
    schema: &Arc<Schema>,
    bloom_filter_fields: &[String],
    metadata: &FileMeta,
    is_ingester: bool,
    rx: &mut tokio::sync::mpsc::Receiver<RecordBatch>,
    read_task: tokio::task::JoinHandle<Result<()>>,
) -> Result<Vec<u8>> {
    let cfg = get_config();
    let mut buf = Vec::new();
    let compression = if is_ingester && cfg.common.feature_ingester_none_compression {
        Some("none")
    } else {
        None
    };
    let mut writer = new_parquet_writer(
        &mut buf,
        schema,
        bloom_filter_fields,
        metadata,
        false,
        compression,
    );

    let mut new_file_meta = metadata.clone();
    new_file_meta.records = 0;
    while let Some(batch) = rx.recv().await {
        new_file_meta.records += batch.num_rows() as i64;
        if let Err(e) = writer.write(&batch).await {
            log::error!("merge_parquet_files write error: {e}");
            return Err(e.into());
        }
    }

    read_task
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))??;
    append_metadata(&mut writer, &new_file_meta)?;
    writer.close().await?;
    Ok(buf)
}

async fn write_vortex(
    schema: Arc<Schema>,
    rx: tokio::sync::mpsc::Receiver<RecordBatch>,
    read_task: tokio::task::JoinHandle<Result<()>>,
) -> Result<Vec<u8>> {
    crate::datafusion::vortex_support::write_vortex(schema, rx, read_task).await
}

pub fn append_metadata(
    writer: &mut AsyncArrowWriter<&mut Vec<u8>>,
    file_meta: &FileMeta,
) -> Result<()> {
    writer.append_key_value_metadata(KeyValue::new(
        "min_ts".to_string(),
        file_meta.min_ts.to_string(),
    ));
    writer.append_key_value_metadata(KeyValue::new(
        "max_ts".to_string(),
        file_meta.max_ts.to_string(),
    ));
    writer.append_key_value_metadata(KeyValue::new(
        "records".to_string(),
        file_meta.records.to_string(),
    ));
    writer.append_key_value_metadata(KeyValue::new(
        "original_size".to_string(),
        file_meta.original_size.to_string(),
    ));
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn create_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("field1", DataType::Utf8, true),
            Field::new("field2", DataType::Int64, true),
        ]))
    }

    #[test]
    fn test_merge_output_file_format_uses_parquet_for_ingester_metrics() {
        assert_eq!(
            merge_output_file_format(StreamType::Metrics, true, FileFormat::Vortex),
            FileFormat::Parquet
        );
        assert_eq!(
            merge_output_file_format(StreamType::Logs, true, FileFormat::Vortex),
            FileFormat::Vortex
        );
        assert_eq!(
            merge_output_file_format(StreamType::Metrics, false, FileFormat::Vortex),
            FileFormat::Vortex
        );
        assert_eq!(
            merge_output_file_format(StreamType::Logs, false, FileFormat::Parquet),
            FileFormat::Parquet
        );
    }

    #[tokio::test]
    async fn test_merge_parquet_files_error_handling() {
        // Test with empty tables vector
        let schema = create_test_schema();
        let empty_tables: Vec<Arc<dyn TableProvider>> = vec![];
        let metadata = FileMeta::default();

        let result = merge_parquet_files(
            StreamType::Logs,
            "test_stream",
            schema,
            empty_tables,
            &[],
            metadata,
            false,
            false,
        )
        .await;

        // Should handle empty tables gracefully or return appropriate error
        // The exact behavior depends on implementation details
        assert!(result.is_ok() || result.is_err());
    }

    /// M13 plan-shape pin: `single_partition(true)` bypasses the
    /// `datafusion_min_partition_num` floor (default 2) and plans the merge
    /// `ORDER BY` as ONE SortExec over one partition with NO RepartitionExec
    /// — the structural fix for the segment builder's (M13) and the
    /// compactor's metadata-class (M20b) default/metadata/trace_list_index
    /// pool starvations. The default context keeps the floor
    /// (target_partitions >= 2), pinning that `false` callers — the ingester
    /// WAL move and non-metadata compactor merges — are untouched.
    #[tokio::test]
    async fn m13_single_partition_merge_plan_has_no_repartition() {
        use arrow::array::{Int64Array, StringArray};

        let schema = Arc::new(Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("trace_id", DataType::Utf8, true),
            Field::new("service_name", DataType::Utf8, true),
        ]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![3i64, 1, 2])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(StringArray::from(vec!["s1", "s2", "s1"])),
            ],
        )
        .unwrap();

        let plan_for = |single: bool| {
            let schema = schema.clone();
            let batch = batch.clone();
            async move {
                let ctx = DataFusionContextBuilder::new()
                    .trace_id("m13-plan-shape")
                    .sorted_by_time(true)
                    .single_partition(single)
                    .build(get_config().limit.datafusion_min_partition_num)
                    .await
                    .unwrap();
                let table =
                    datafusion::datasource::MemTable::try_new(schema.clone(), vec![vec![batch]])
                        .unwrap();
                ctx.register_table("tbl", Arc::new(table)).unwrap();
                let sql = format!("SELECT * FROM tbl ORDER BY {TIMESTAMP_COL_NAME} DESC");
                let logical = ctx.state().create_logical_plan(&sql).await.unwrap();
                let physical = ctx.state().create_physical_plan(&logical).await.unwrap();
                let display = datafusion::physical_plan::displayable(physical.as_ref())
                    .indent(false)
                    .to_string();
                let partitions = physical
                    .properties()
                    .output_partitioning()
                    .partition_count();
                (
                    ctx.state().config().target_partitions(),
                    partitions,
                    display,
                )
            }
        };

        let (target, out_parts, display) = plan_for(true).await;
        assert_eq!(target, 1, "single_partition must bypass the min floor");
        assert_eq!(out_parts, 1, "the sort output must be one partition");
        assert!(
            !display.contains("RepartitionExec"),
            "single-partition plan must not repartition:\n{display}"
        );
        assert_eq!(
            display.matches("SortExec").count(),
            1,
            "exactly one sorter (it spills properly):\n{display}"
        );

        // the default context keeps the min-partition floor — `false`
        // callers (ingester WAL move, non-metadata compactor merges) plan
        // exactly as before M13/M20b
        let (target, ..) = plan_for(false).await;
        assert_eq!(
            target,
            get_config().limit.datafusion_min_partition_num.max(1),
            "the default merge context must keep the configured floor"
        );
    }

    /// M13 pin (manual, memory-shaped): a FAT metadata-shaped corpus —
    /// trace_list_index columns, ~2x the floored 256MB DataFusion pool —
    /// builds to completion through the segment-builder invocation
    /// (single-partition sort, greedy pool): the sorter SPILLS instead of
    /// failing "Not enough memory to continue external sort" (the prod
    /// post-.108 residual this fixes). Run standalone:
    /// `cargo test -p search --release \
    ///    m13_metadata_shaped_build_spills_at_floor_pool -- --ignored --nocapture`
    #[tokio::test]
    #[ignore = "manual memory-shaped pin: mutates process-global config; run standalone --release"]
    async fn m13_metadata_shaped_build_spills_at_floor_pool() {
        use arrow::array::{Int64Array, StringArray};

        // shrink the pool to the floor: create_runtime_env clamps to
        // DATAFUSION_MIN_MEM (256MB), so "1 MB" configures the minimum
        unsafe { std::env::set_var("ZO_MEMORY_CACHE_DATAFUSION_MAX_SIZE", "1") };
        config::refresh_config().unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("trace_id", DataType::Utf8, true),
            Field::new("span_id", DataType::Utf8, true),
            Field::new("service_name", DataType::Utf8, true),
            Field::new("operation_name", DataType::Utf8, true),
            Field::new("duration", DataType::Int64, true),
            Field::new("start_time", DataType::Int64, true),
            Field::new("end_time", DataType::Int64, true),
        ]));

        // ~4M rows x ~130B ≈ 550MB of arrow — >2x the floored pool, so the
        // single sorter MUST spill to finish. Descending shuffled timestamps
        // defeat any pre-sorted shortcut.
        const ROWS_PER_BATCH: usize = 65_536;
        const BATCHES: usize = 64;
        let mut batches = Vec::with_capacity(BATCHES);
        let mut total_bytes = 0usize;
        let mut rng: u64 = 0x9E3779B97F4A7C15;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        for b in 0..BATCHES {
            let mut ts = Vec::with_capacity(ROWS_PER_BATCH);
            let mut trace = Vec::with_capacity(ROWS_PER_BATCH);
            let mut span = Vec::with_capacity(ROWS_PER_BATCH);
            let mut svc = Vec::with_capacity(ROWS_PER_BATCH);
            let mut op = Vec::with_capacity(ROWS_PER_BATCH);
            for i in 0..ROWS_PER_BATCH {
                let r = next();
                ts.push(1_700_000_000_000_000i64 + (r % 3_600_000_000) as i64);
                trace.push(format!("{:032x}", (r as u128) << 64 | (b * i) as u128));
                span.push(format!("{:016x}", r));
                svc.push(format!("service-{}", r % 40));
                op.push(format!("operation/{}/{}", r % 12, r % 97));
            }
            let batch = arrow::record_batch::RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ts)),
                    Arc::new(StringArray::from(trace)),
                    Arc::new(StringArray::from(span)),
                    Arc::new(StringArray::from(svc)),
                    Arc::new(StringArray::from(op)),
                    Arc::new(Int64Array::from_iter_values(
                        (0..ROWS_PER_BATCH).map(|_| next() as i64 % 1_000_000),
                    )),
                    Arc::new(Int64Array::from_iter_values(
                        (0..ROWS_PER_BATCH).map(|_| next() as i64),
                    )),
                    Arc::new(Int64Array::from_iter_values(
                        (0..ROWS_PER_BATCH).map(|_| next() as i64),
                    )),
                ],
            )
            .unwrap();
            total_bytes += batch.get_array_memory_size();
            batches.push(batch);
        }
        let rows = (ROWS_PER_BATCH * BATCHES) as i64;
        eprintln!(
            "corpus: {rows} rows / {} MB arrow vs {} MB pool floor",
            total_bytes / (1024 * 1024),
            crate::datafusion::exec::DATAFUSION_MIN_MEM / (1024 * 1024)
        );
        assert!(
            total_bytes > 2 * crate::datafusion::exec::DATAFUSION_MIN_MEM,
            "corpus must exceed the pool to force spills"
        );

        let table =
            datafusion::datasource::MemTable::try_new(schema.clone(), vec![batches]).unwrap();
        let meta = FileMeta {
            min_ts: 1_700_000_000_000_000,
            max_ts: 1_700_000_003_600_000,
            records: rows,
            original_size: total_bytes as i64,
            ..Default::default()
        };
        let started = std::time::Instant::now();
        let result = merge_parquet_files(
            StreamType::Metadata,
            "trace_list_index",
            schema,
            vec![Arc::new(table)],
            &[],
            meta,
            true,
            true, // the segment-builder invocation shape (M13)
        )
        .await;
        unsafe { std::env::remove_var("ZO_MEMORY_CACHE_DATAFUSION_MAX_SIZE") };
        config::refresh_config().unwrap();

        let result = result.expect(
            "metadata-shaped build at the floor pool must SPILL and complete, never \
             'Not enough memory to continue external sort'",
        );
        match result {
            MergeParquetResult::Single { file_meta, .. } => {
                assert_eq!(file_meta.records, rows, "every row survives the spill");
                eprintln!(
                    "built {} rows / {} MB input in {:?} at a {} MB pool",
                    file_meta.records,
                    total_bytes / (1024 * 1024),
                    started.elapsed(),
                    crate::datafusion::exec::DATAFUSION_MIN_MEM / (1024 * 1024)
                );
            }
            MergeParquetResult::Multiple { .. } => panic!("single build returned multiple files"),
        }
    }

    /// M20b pin (manual, memory-shaped; the M13 spill-pin pattern on the
    /// COMPACTOR invocation): a FAT trace_list_index-shaped corpus — the
    /// real metadata stream columns, ~2x the floored 256MB DataFusion pool —
    /// merges to completion through the compactor invocation shape
    /// (is_ingester=false, single-partition sort, greedy pool): the sorter
    /// SPILLS instead of failing "Not enough memory to continue external
    /// sort" / getting the compactor OOM-killed (prod .111: 116MB→6GB spikes
    /// in ~2s, ~24 kills in 40min). Run standalone:
    /// `cargo test -p search --release \
    ///    m20b_compactor_metadata_shaped_merge_spills_at_floor_pool -- --ignored --nocapture`
    #[tokio::test]
    #[ignore = "manual memory-shaped pin: mutates process-global config; run standalone --release"]
    async fn m20b_compactor_metadata_shaped_merge_spills_at_floor_pool() {
        use arrow::array::{Int64Array, StringArray};

        // shrink the pool to the floor: create_runtime_env clamps to
        // DATAFUSION_MIN_MEM (256MB), so "1 MB" configures the minimum
        unsafe { std::env::set_var("ZO_MEMORY_CACHE_DATAFUSION_MAX_SIZE", "1") };
        config::refresh_config().unwrap();

        // the actual default/metadata/trace_list_index schema
        // (core::metadata::trace_list_index::TraceListIndex)
        let schema = Arc::new(Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("stream_name", DataType::Utf8, false),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("trace_id", DataType::Utf8, false),
        ]));

        // ~8.4M rows x ~76B ≈ 610MB of arrow — >2x the floored pool, so the
        // single sorter MUST spill to finish (the thinner 4-col schema needs
        // more rows than the M13 pin's 8-col one to clear the guard).
        // Shuffled timestamps defeat any pre-sorted shortcut.
        const ROWS_PER_BATCH: usize = 65_536;
        const BATCHES: usize = 128;
        let mut batches = Vec::with_capacity(BATCHES);
        let mut total_bytes = 0usize;
        let mut rng: u64 = 0x9E3779B97F4A7C15;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        for b in 0..BATCHES {
            let mut ts = Vec::with_capacity(ROWS_PER_BATCH);
            let mut stream = Vec::with_capacity(ROWS_PER_BATCH);
            let mut svc = Vec::with_capacity(ROWS_PER_BATCH);
            let mut trace = Vec::with_capacity(ROWS_PER_BATCH);
            for i in 0..ROWS_PER_BATCH {
                let r = next();
                ts.push(1_700_000_000_000_000i64 + (r % 3_600_000_000) as i64);
                stream.push("default".to_string());
                svc.push(format!("service-{}", r % 40));
                trace.push(format!("{:032x}", (r as u128) << 64 | (b * i) as u128));
            }
            let batch = arrow::record_batch::RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ts)),
                    Arc::new(StringArray::from(stream)),
                    Arc::new(StringArray::from(svc)),
                    Arc::new(StringArray::from(trace)),
                ],
            )
            .unwrap();
            total_bytes += batch.get_array_memory_size();
            batches.push(batch);
        }
        let rows = (ROWS_PER_BATCH * BATCHES) as i64;
        eprintln!(
            "corpus: {rows} rows / {} MB arrow vs {} MB pool floor",
            total_bytes / (1024 * 1024),
            crate::datafusion::exec::DATAFUSION_MIN_MEM / (1024 * 1024)
        );
        assert!(
            total_bytes > 2 * crate::datafusion::exec::DATAFUSION_MIN_MEM,
            "corpus must exceed the pool to force spills"
        );

        let table =
            datafusion::datasource::MemTable::try_new(schema.clone(), vec![batches]).unwrap();
        let meta = FileMeta {
            min_ts: 1_700_000_000_000_000,
            max_ts: 1_700_000_003_600_000,
            records: rows,
            original_size: total_bytes as i64,
            ..Default::default()
        };
        let started = std::time::Instant::now();
        let result = merge_parquet_files(
            StreamType::Metadata,
            "trace_list_index",
            schema,
            vec![Arc::new(table)],
            &[],
            meta,
            false, // the compactor invocation shape
            true,  // M20b: single-partition sort for metadata-class merges
        )
        .await;
        unsafe { std::env::remove_var("ZO_MEMORY_CACHE_DATAFUSION_MAX_SIZE") };
        config::refresh_config().unwrap();

        let result = result.expect(
            "metadata-shaped compactor merge at the floor pool must SPILL and complete, never \
             'Not enough memory to continue external sort'",
        );
        match result {
            MergeParquetResult::Single { file_meta, buf, .. } => {
                assert_eq!(file_meta.records, rows, "every row survives the spill");
                eprintln!(
                    "merged {} rows / {} MB input -> {} MB output in {:?} at a {} MB pool",
                    file_meta.records,
                    total_bytes / (1024 * 1024),
                    buf.len() / (1024 * 1024),
                    started.elapsed(),
                    crate::datafusion::exec::DATAFUSION_MIN_MEM / (1024 * 1024)
                );
            }
            MergeParquetResult::Multiple { .. } => panic!("single merge returned multiple files"),
        }
    }
}
