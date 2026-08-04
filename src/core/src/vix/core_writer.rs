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

//! Writers of **core files**: a single `.vix` object per data unit
//! holding records + inverted index — the unconditional storage format of
//! logs/traces (no parquet data file and no sibling index).
//!
//! Two producers live here:
//! - [`write_core_file_from_tables`] — the WAL→storage move job: runs the same `SELECT * FROM tbl
//!   ORDER BY _timestamp DESC` merge plan the parquet path uses, synthesizes `_source` per batch
//!   and streams everything into a [`VixWriter`] (column-driven term extraction).
//! - [`merge_core_files`] — the compactor: k-way merges already-written core files by `_timestamp`
//!   **without DataFusion**. The merged index is assembled straight from the inputs' term
//!   dictionaries (`VixWriter::merge_input_indexes`; no re-tokenizing) and disjoint-time inputs
//!   stream-copy their docs rows; [`merge_core_files_rebuild`] — terms re-derived from `_source`
//!   ([`VixWriter::push_docs_rows`], source-driven) — remains as the fallback for inputs the index
//!   merge cannot express (tokenizer/capability mismatches, unreadable index blobs) and as the
//!   differential-test oracle.
//!
//! Both keep the storage-file convention of descending `_timestamp` order.
//!
//! Every producer stages docs rows in batches bounded by [`BatchCaps`] (row
//! count AND variable-length bytes): only `_timestamp` (fixed 8 bytes/row)
//! is ever materialized across a whole file. Unbounded columns
//! (`_source`/`_original`/cs) as one arrow `Utf8` array overflow the `i32`
//! value offsets on multi-GB inputs — arrow panics with "byte array offset
//! overflow" — so they stream through bounded windows instead.
//!
//! Every producer also CLEANSES degenerate rows: a row whose `_timestamp`
//! is `<= 0` (or null) is dropped before term emission and cs derivation,
//! counted in [`CoreFileResult::dropped_rows`]. Stored files with such rows
//! exist (the pre-guard mover hid literal zeros behind healthy-looking
//! metadata) and would otherwise wedge every merge on the writer's finish
//! guard forever — meta-based sweeps cannot find them, only reading the data
//! can, which is exactly what a merge does. The finish guard STAYS: after
//! cleansing a degenerate range means a new bug, not old data.

use std::{
    cmp::Ordering,
    collections::{BinaryHeap, VecDeque},
    sync::{
        Arc,
        mpsc::{Receiver, SyncSender, sync_channel},
    },
};

use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Int64Array, LargeBinaryArray,
        LargeStringArray, StringArray, StringViewArray, new_empty_array,
    },
    compute::{cast, filter_record_batch, interleave},
    record_batch::RecordBatch,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use config::{
    ID_COL_NAME, ORIGINAL_DATA_COL_NAME, PARQUET_MAX_ROW_GROUP_SIZE, TIMESTAMP_COL_NAME, cluster,
    get_config, meta::stream::FileMeta,
};
use datafusion::{catalog::TableProvider, physical_plan::execute_stream};
pub use vortex_index::VixOutput;
use vortex_index::{
    DocIdMap, SOURCE_COL_NAME, SOURCE_RENAMED_COL_NAME, VixDocs, VixRangeSource, VixReader,
    VixWriter, VixWriterOptions, VixWriterStats,
};

use crate::search::datafusion::{
    exec::DataFusionContextBuilder, source_synthesis::synthesize_source,
    table_provider::uniontable::NewUnionTable, vix_format::derive_cs_column_from_source,
};

/// Maximum rows of one docs batch pushed into the writer by the core-file
/// producers (both the move job and the compaction merges).
const DOCS_BATCH_ROWS: usize = 8192;

/// Byte budget of one pushed docs batch, measured over the variable-length
/// values it carries (`_source`, `_original`, string/binary column-store
/// cells). Every producer stages docs rows in batches within this budget and
/// NEVER materializes a whole file's `_source`/`_original` as one arrow
/// array: a `Utf8` array's offsets are `i32`, so cumulative value bytes
/// beyond `i32::MAX` panic in arrow's offset builder ("byte array offset
/// overflow") — a ~3 GB-`_source` hour reached exactly that when the merge
/// loaded whole input columns. 256 MiB keeps an ~8x margin; a single row
/// larger than the budget still forms its own batch (progress is by row).
const DOCS_BATCH_BYTES: usize = 256 * 1024 * 1024;

/// The row/byte bounds of one staged docs batch. Production uses
/// [`Default`]; tests shrink the caps to prove the chunked flow with small
/// data.
#[derive(Clone, Copy, Debug)]
struct BatchCaps {
    rows: usize,
    bytes: usize,
}

impl Default for BatchCaps {
    fn default() -> Self {
        Self {
            rows: DOCS_BATCH_ROWS,
            bytes: DOCS_BATCH_BYTES,
        }
    }
}

/// The finished bytes of one core file plus the writer's stats
/// (`stats.index_size` feeds `FileMeta::index_size`).
pub struct CoreFileResult {
    /// In-memory container bytes — EMPTY when the build spooled to disk
    /// (see `output`); tests and non-spooling paths keep using this.
    pub data: Vec<u8>,
    /// The build output: bytes or a disk spool (upload from its path; the
    /// spool file deletes when this drops). The move job uploads from here.
    pub output: Option<vortex_index::VixOutput>,
    pub stats: VixWriterStats,
    /// Compaction marker: `true` when the file's index was produced by the
    /// input-dictionary merge fast path, `false` for a full term rebuild
    /// (and for move-job outputs, which never merge).
    pub used_index_merge: bool,
    /// Bounded docs batches pushed into the writer (observability; the
    /// chunked-flow tests assert the producers never collapse a large file
    /// into one giant batch).
    pub docs_batches: usize,
    /// Rows dropped by degenerate-`_timestamp` cleansing (`_timestamp <= 0`
    /// or null): pre-guard-era stored data on the merge paths, a
    /// should-never-happen backstop on the move path. Callers emit ONE loud
    /// WARN and a `compact_dropped_zero_ts_rows` counter per output when
    /// this is non-zero; `stats.row_count == 0` with `dropped_rows > 0`
    /// means an all-poison input set (the merge commits "inputs deleted, no
    /// output file"; the move job fails distinctly).
    pub dropped_rows: u64,
}

impl std::fmt::Debug for CoreFileResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoreFileResult")
            .field("data_len", &self.data.len())
            .field(
                "spooled",
                &self.output.as_ref().map(|o| o.spool_path().is_some()),
            )
            .field("stats", &self.stats)
            .field("used_index_merge", &self.used_index_merge)
            .field("docs_batches", &self.docs_batches)
            .field("dropped_rows", &self.dropped_rows)
            .finish()
    }
}

/// One finished compaction merge: like [`CoreFileResult`] but the container
/// is a [`VixOutput`] — SPOOLED to the compactor's data volume in
/// production (`build_merge_plan` sets the spool dir), so a multi-GB merged
/// file never resides in RAM; the upload streams from the spool path and
/// the temp file deletes on drop. The move-job path keeps [`CoreFileResult`]
/// (small files, in-memory bytes).
#[derive(Debug)]
pub struct MergedCoreFile {
    pub output: VixOutput,
    pub stats: VixWriterStats,
    /// `true` when the index came from the input-dictionary merge fast
    /// path, `false` for a full term rebuild.
    pub used_index_merge: bool,
    /// Bounded docs batches pushed into the writer (observability).
    pub docs_batches: usize,
    /// Rows dropped by degenerate-`_timestamp` cleansing.
    pub dropped_rows: u64,
}

/// The shared [`VixWriterOptions`] of every core-file producer.
///
/// `_original` is never term-indexed, so it is dropped from the full-text
/// list.
fn core_writer_options(
    fts_fields: &[String],
    column_store_fields: Vec<String>,
    bloom_fields: Vec<String>,
) -> VixWriterOptions {
    let cfg = get_config();
    VixWriterOptions {
        bloom_field_names: bloom_fields,
        bloom_fpp: cfg.common.vix_bloom_fpp,
        fts_field_names: fts_fields
            .iter()
            .filter(|f| f.as_str() != ORIGINAL_DATA_COL_NAME)
            .cloned()
            .collect(),
        column_store_field_names: column_store_fields,
        postings_chunk_bytes: cfg.common.vix_postings_chunk_bytes,
        max_raw_term_len: cfg.common.vix_max_raw_term_len,
        row_group_size: PARQUET_MAX_ROW_GROUP_SIZE,
        docs_chunk_bytes: cfg.common.vix_docs_chunk_bytes,
        min_token_len: cfg.limit.inverted_index_min_token_length,
        max_token_len: cfg.limit.inverted_index_max_token_length,
        // #15 rollout discipline: default 0 keeps the out-of-row postings
        // writer dark; flip ZO_VIX_PLIST_MIN_DOCS only after the release
        // carrying pointer-cell read support is on EVERY pod.
        postings_plist_min_docs: cfg.common.vix_plist_min_docs as u32,
        // Single-file build (move job): parallelize the `docs`/index blob
        // encode across cores when spare parallelism exists. The compaction
        // merge overrides this with merge_threads() in build_merge_plan.
        encode_threads: build_encode_threads(),
        // 0 = the writer's default sample budget (tests shrink it)
        docs_encode_sample_bytes: 0,
        // move-job builds never spill terms (small dictionaries); the
        // compaction merge sets the spill dir in build_merge_plan, and the
        // move path sets output_spool_dir per build (big batched moves
        // spool, small ones stay in memory).
        term_spill_dir: None,
        term_spill_bytes: 0,
        output_spool_dir: None,
    }
}

/// Threads of one compaction merge (`ZO_VIX_MERGE_THREAD_NUM`; `0` = auto).
/// Drives the term-dictionary merge partitioning, the per-input decode fan-out
/// and the blob encode pools. Auto = the machine's available parallelism
/// divided by the co-located CPU-heavy role count, so a combined node's merge
/// pool does not stack on top of its ingest/query pools (dedicated compactor /
/// LOCAL_MODE keep the full count — see [`cluster::cpu_role_divisor`]).
fn merge_threads() -> usize {
    let configured = get_config().common.vix_merge_thread_num;
    if configured != 0 {
        return configured;
    }
    let base = std::thread::available_parallelism().map_or(1, |n| n.get());
    std::cmp::max(1, base / cluster::cpu_role_divisor())
}

/// Encode threads for ONE single-file core build on the WAL→storage move job
/// (`ZO_VIX_BUILD_THREAD_NUM`; `0` = auto). Auto = the machine's available
/// parallelism on a DEDICATED ingester (spare cores, no query/compaction
/// competition, and the move build's `docs` blob zstd is the parallelizable
/// phase), else `1`: a combined ingester+querier/compactor node keeps each
/// build single-threaded so it never competes with the query fan-out or the
/// compaction merge — the cores are better spent on query tail latency (C2).
/// The across-file mover pool (`ZO_FILE_MOVE_THREAD_NUM`) is the primary
/// throughput lever; this only fills spare cores when files are few and large.
fn build_encode_threads() -> usize {
    let cfg = get_config();
    let configured = cfg.common.vix_build_thread_num;
    if configured != 0 {
        return configured;
    }
    if !cfg.common.local_mode
        && cluster::LOCAL_NODE.is_ingester()
        && !cluster::LOCAL_NODE.is_querier()
        && !cluster::LOCAL_NODE.is_compactor()
    {
        std::thread::available_parallelism().map_or(1, |n| n.get())
    } else {
        1
    }
}

/// Fold a finished core file's writer stats into its `FileMeta`: the object
/// size, the embedded index bytes and — authoritatively — the `_timestamp`
/// range AND row count of the rows the writer actually stored. Upstream
/// metadata (WAL parquet footers, the inputs' file_list rows) has been
/// observed degenerate (min_ts/max_ts = 0 while the rows carry normal
/// timestamps; one zeroed input pins a folded min to 0 forever), so the
/// data-derived values always win, with a WARN when they disagree. Callers
/// whose pipeline cleansed degenerate rows pre-adjust `meta.records` by
/// [`CoreFileResult::dropped_rows`] (and warn once themselves), so agreement
/// here is the healthy case and this WARN stays an anomaly signal. Shared by
/// the ingester move job and the compactor.
///
/// HARD guard, not a warning: a meta that still ends up degenerate (a
/// claimed-non-empty file with min_ts/max_ts ≤ 0 or an inverted range) is an
/// error — such a row must never reach the file_list DB, where it poisons
/// time-range pruning and (observed live) wedged the compactor's commit
/// retry loop. The writer's own `finish` guard makes a degenerate DATA range
/// unreachable; this catches the remaining fold-side shape (`records > 0`
/// from the input metas while the writer stored zero rows — callers handle
/// the cleansed-to-empty case explicitly BEFORE calling here).
pub fn apply_core_stats_to_meta(
    meta: &mut FileMeta,
    data_len: usize,
    stats: &VixWriterStats,
    context: &str,
) -> Result<(), anyhow::Error> {
    meta.compressed_size = data_len as i64;
    meta.index_size = stats.index_size as i64;
    if stats.row_count > 0 {
        if meta.min_ts != stats.min_ts
            || meta.max_ts != stats.max_ts
            || meta.records != stats.row_count as i64
        {
            log::warn!(
                "{context}: input meta ({} records, time range [{}, {}]) disagrees with the \
                 written data ({} rows, [{}, {}]); using the data range",
                meta.records,
                meta.min_ts,
                meta.max_ts,
                stats.row_count,
                stats.min_ts,
                stats.max_ts,
            );
        }
        meta.min_ts = stats.min_ts;
        meta.max_ts = stats.max_ts;
        meta.records = stats.row_count as i64;
    }
    if meta.records > 0 && (meta.min_ts <= 0 || meta.max_ts <= 0 || meta.min_ts > meta.max_ts) {
        return Err(anyhow::anyhow!(
            "{context}: degenerate time range [{}, {}] for a {}-record file (writer stored {} \
             rows); refusing to publish the corrupt meta",
            meta.min_ts,
            meta.max_ts,
            meta.records,
            stats.row_count,
        ));
    }
    Ok(())
}

/// The column-store field list actually written: the stream's configured
/// fields plus `_o2_id` when the data carries it — `_o2_id` is excluded from
/// `_source` (internal dedup handle), so the docs column is its only home in
/// a core file.
fn effective_column_store_fields(column_store_fields: &[String], has_o2_id: bool) -> Vec<String> {
    let mut fields = column_store_fields.to_vec();
    if has_o2_id && !fields.iter().any(|f| f == ID_COL_NAME) {
        fields.push(ID_COL_NAME.to_string());
    }
    fields
}

/// Move-job producer: merge the WAL batches behind `tables` (same table
/// providers the parquet path builds) into ONE core `.vix` file.
///
/// `schema` is the merged (shared-fields) schema of the input files; the
/// plan output may include `_original`, which is split into the writer's
/// dedicated docs column instead of being treated as a field.
/// `store_original` is the stream's `store_original_data` setting — it is
/// also force-enabled when the inputs already carry an `_original` column,
/// so a mid-hour settings flip never drops captured data.
pub async fn write_core_file_from_tables(
    trace_id: &str,
    schema: Arc<Schema>,
    tables: Vec<Arc<dyn TableProvider>>,
    fts_fields: &[String],
    column_store_fields: &[String],
    bloom_fields: &[String],
    store_original: bool,
    input_original_bytes: usize,
) -> Result<CoreFileResult, anyhow::Error> {
    write_core_file_from_tables_with_caps(
        trace_id,
        schema,
        tables,
        fts_fields,
        column_store_fields,
        bloom_fields,
        store_original,
        input_original_bytes,
        BatchCaps::default(),
    )
    .await
}

/// [`write_core_file_from_tables`] with explicit batch caps (tests shrink
/// them to prove the byte-bounded chunked flow with small data).
async fn write_core_file_from_tables_with_caps(
    trace_id: &str,
    schema: Arc<Schema>,
    tables: Vec<Arc<dyn TableProvider>>,
    fts_fields: &[String],
    column_store_fields: &[String],
    bloom_fields: &[String],
    store_original: bool,
    input_original_bytes: usize,
    caps: BatchCaps,
) -> Result<CoreFileResult, anyhow::Error> {
    let cfg = get_config();
    let sql = format!("SELECT * FROM tbl ORDER BY {TIMESTAMP_COL_NAME} DESC");
    let ctx = DataFusionContextBuilder::new()
        .trace_id(trace_id)
        .sorted_by_time(true)
        .build(cfg.limit.datafusion_min_partition_num)
        .await?;
    let union_table = Arc::new(NewUnionTable::new(schema.clone(), tables));
    ctx.register_table("tbl", union_table)?;
    let plan = ctx.state().create_logical_plan(&sql).await?;
    let physical_plan = ctx.state().create_physical_plan(&plan).await?;
    let plan_schema = physical_plan.schema();

    let mut batch_stream = execute_stream(physical_plan, ctx.task_ctx())?;
    let (tx, mut rx) = tokio::sync::mpsc::channel::<RecordBatch>(2);
    let read_task = tokio::task::spawn(async move {
        while let Some(batch) = futures::TryStreamExt::try_next(&mut batch_stream).await? {
            if tx.send(batch).await.is_err() {
                break; // builder exited (error on its side); stop reading
            }
        }
        Ok::<(), anyhow::Error>(())
    });

    let mut opts = core_writer_options(
        fts_fields,
        effective_column_store_fields(
            column_store_fields,
            plan_schema.field_with_name(ID_COL_NAME).is_ok(),
        ),
        bloom_fields.to_vec(),
    );
    // Big batched moves spool the finished container to the WAL volume and
    // upload from the spool path — the buffered container (plus its upload
    // clone) was the ingester's next OOM vector once the fields-limit
    // batching guard retired. Small moves keep the in-memory path (no extra
    // disk round-trip); tests and benches pass 0 and never spool.
    let spool_min = get_config().common.vix_move_spool_min_bytes;
    if spool_min > 0 && input_original_bytes >= spool_min {
        opts.output_spool_dir =
            Some(std::path::Path::new(&get_config().common.data_wal_dir).join("vix_spool"));
    }
    let store_original =
        store_original || plan_schema.field_with_name(ORIGINAL_DATA_COL_NAME).is_ok();

    // All CPU-heavy work — _source synthesis, tokenizing, FST/postings/
    // vortex encoding — stays off the async runtime.
    let builder = tokio::task::spawn_blocking(move || {
        // A user field literally named `_source` (pre-guard WAL data — the
        // logs ingest path renames it now) collides with the reserved
        // serialized-record column: rename the column so its values survive
        // in the stored record instead of being silently dropped (excluded
        // from `_source` synthesis AND filtered from the writer schema).
        let needs_source_rename = plan_schema.field_with_name(SOURCE_COL_NAME).is_ok();
        let plan_schema = if needs_source_rename {
            Arc::new(rename_reserved_source_field(&plan_schema))
        } else {
            plan_schema
        };
        let writer_schema = writer_input_schema(&plan_schema);
        let writer_indices: Vec<usize> = plan_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| writer_schema.field_with_name(field.name()).is_ok())
            .map(|(index, _)| index)
            .collect();
        let mut writer = VixWriter::new(&writer_schema, opts, store_original);
        let mut docs_batches = 0usize;
        let mut dropped_rows = 0u64;
        while let Some(batch) = rx.blocking_recv() {
            if batch.num_rows() == 0 {
                continue;
            }
            let batch = if needs_source_rename {
                // same columns under the renamed schema (only a name differs)
                RecordBatch::try_new(Arc::clone(&plan_schema), batch.columns().to_vec())?
            } else {
                batch
            };
            // Cleansing backstop: drop degenerate-`_timestamp` rows (zero
            // minted by a pre-canonicalization WAL, or null) BEFORE term
            // emission/`_source` synthesis, so a poisoned WAL file moves
            // cleanly instead of wedging on the writer's finish guard. The
            // logs-ingest canonicalization mints none — the caller warns
            // and counts whenever this fires.
            let timestamps =
                as_int64_array(batch.column_by_name(TIMESTAMP_COL_NAME).ok_or_else(|| {
                    anyhow::anyhow!("move-job batch is missing {TIMESTAMP_COL_NAME:?}")
                })?)?;
            let batch = match cleanse_degenerate_ts_rows(&batch, &timestamps)? {
                Some((cleansed, dropped)) => {
                    dropped_rows += dropped;
                    cleansed
                }
                None => batch,
            };
            if batch.num_rows() == 0 {
                continue;
            }
            // Cap by BYTES, not just the plan's row count: `_source`
            // synthesis materializes one JSON string per row, so a wide-row
            // WAL batch must be split before the strings of a whole batch
            // land in one arrow array.
            for part in split_batch_by_bytes(&batch, caps) {
                let source = synthesize_source(&part)?;
                let original = if store_original {
                    batch_string_column(&part, ORIGINAL_DATA_COL_NAME)?
                } else {
                    None
                };
                let projected = part.project(&writer_indices)?;
                writer.push_batch_with_source(&projected, &source, original.as_ref())?;
                docs_batches += 1;
            }
        }
        let (output, stats) = writer.finish_output()?;
        // spooled outputs stay on disk (upload from path); in-memory
        // outputs land in `data` as before
        let (data, output) = match output {
            vortex_index::VixOutput::Bytes(bytes) => (bytes, None),
            spooled => (Vec::new(), Some(spooled)),
        };
        Ok::<CoreFileResult, anyhow::Error>(CoreFileResult {
            data,
            output,
            stats,
            used_index_merge: false,
            docs_batches,
            dropped_rows,
        })
    });

    read_task.await??;
    builder.await?
}

/// The record-batch schema handed to [`VixWriter::new`]: the plan
/// schema minus the writer-managed `_source`/`_original` columns (a user
/// field named `_source` was renamed to [`SOURCE_RENAMED_COL_NAME`] before
/// this runs, so the filter only ever drops the reserved column itself).
fn writer_input_schema(plan_schema: &SchemaRef) -> Schema {
    let fields: Vec<Field> = plan_schema
        .fields()
        .iter()
        .filter(|field| field.name() != ORIGINAL_DATA_COL_NAME && field.name() != SOURCE_COL_NAME)
        .map(|field| field.as_ref().clone())
        .collect();
    Schema::new(fields)
}

/// The plan schema with a user field literally named `_source` renamed to
/// [`SOURCE_RENAMED_COL_NAME`] (field type/nullability/metadata preserved).
/// Only call when such a field exists. Degenerate double naming (a
/// `_source_field` column already present) keeps both columns; lookups by
/// name resolve the first.
fn rename_reserved_source_field(plan_schema: &SchemaRef) -> Schema {
    let fields: Vec<Field> = plan_schema
        .fields()
        .iter()
        .map(|field| {
            if field.name() == SOURCE_COL_NAME {
                field.as_ref().clone().with_name(SOURCE_RENAMED_COL_NAME)
            } else {
                field.as_ref().clone()
            }
        })
        .collect();
    Schema::new(fields)
}

/// Fetch a batch column as a `StringArray` (casting when needed); `None`
/// when the batch has no such column.
fn batch_string_column(
    batch: &RecordBatch,
    name: &str,
) -> Result<Option<StringArray>, anyhow::Error> {
    let Some(column) = batch.column_by_name(name) else {
        return Ok(None);
    };
    Ok(Some(as_string_array(column)?))
}

fn as_string_array(column: &ArrayRef) -> Result<StringArray, anyhow::Error> {
    let column = cast(column, &DataType::Utf8)?;
    column
        .as_any()
        .downcast_ref::<StringArray>()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("column is not a string array"))
}

fn as_int64_array(column: &ArrayRef) -> Result<Int64Array, anyhow::Error> {
    let column = cast(column, &DataType::Int64)?;
    column
        .as_any()
        .downcast_ref::<Int64Array>()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("column is not an int64 array"))
}

/// Per-row value-byte accessor of one column, used for the batch byte caps.
/// Variable-length families report each slot's value bytes; everything else
/// counts as its fixed width (`8` when unknown). Null slots cost `0`.
enum VarBytes<'a> {
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Utf8View(&'a StringViewArray),
    Binary(&'a BinaryArray),
    LargeBinary(&'a LargeBinaryArray),
    BinaryView(&'a BinaryViewArray),
    Fixed(usize),
}

impl<'a> VarBytes<'a> {
    fn new(array: &'a dyn Array) -> Self {
        let any = array.as_any();
        match array.data_type() {
            DataType::Utf8 => any.downcast_ref().map_or(Self::Fixed(8), Self::Utf8),
            DataType::LargeUtf8 => any.downcast_ref().map_or(Self::Fixed(8), Self::LargeUtf8),
            DataType::Utf8View => any.downcast_ref().map_or(Self::Fixed(8), Self::Utf8View),
            DataType::Binary => any.downcast_ref().map_or(Self::Fixed(8), Self::Binary),
            DataType::LargeBinary => any.downcast_ref().map_or(Self::Fixed(8), Self::LargeBinary),
            DataType::BinaryView => any.downcast_ref().map_or(Self::Fixed(8), Self::BinaryView),
            other => Self::Fixed(other.primitive_width().unwrap_or(8)),
        }
    }

    fn get(&self, row: usize) -> usize {
        fn valid(array: &dyn Array, row: usize, len: usize) -> usize {
            if array.is_valid(row) { len } else { 0 }
        }
        match self {
            Self::Utf8(array) => valid(*array, row, array.value_length(row) as usize),
            Self::LargeUtf8(array) => valid(*array, row, array.value_length(row) as usize),
            Self::Utf8View(array) => valid(*array, row, array.value(row).len()),
            Self::Binary(array) => valid(*array, row, array.value_length(row) as usize),
            Self::LargeBinary(array) => valid(*array, row, array.value_length(row) as usize),
            Self::BinaryView(array) => valid(*array, row, array.value(row).len()),
            Self::Fixed(width) => *width,
        }
    }
}

/// Split `batch` into consecutive row slices (zero-copy), each within the
/// caps. Bytes are measured with [`VarBytes`] plus a small per-row constant
/// per column (covers the key/punctuation overhead of the `_source` JSON
/// image a slice may be serialized into). A single row over the byte budget
/// still forms its own slice.
fn split_batch_by_bytes(batch: &RecordBatch, caps: BatchCaps) -> Vec<RecordBatch> {
    let rows = batch.num_rows();
    if rows <= 1 {
        return vec![batch.clone()];
    }
    let accessors: Vec<VarBytes> = batch
        .columns()
        .iter()
        .map(|column| VarBytes::new(column.as_ref()))
        .collect();
    let fixed_per_row = 24usize.saturating_mul(batch.num_columns());
    let mut parts = Vec::new();
    let mut start = 0usize;
    let mut part_rows = 0usize;
    let mut part_bytes = 0usize;
    for row in 0..rows {
        part_rows += 1;
        part_bytes = part_bytes
            .saturating_add(fixed_per_row)
            .saturating_add(accessors.iter().map(|a| a.get(row)).sum());
        if part_rows >= caps.rows.max(1) || part_bytes >= caps.bytes.max(1) {
            parts.push(batch.slice(start, part_rows));
            start = row + 1;
            part_rows = 0;
            part_bytes = 0;
        }
    }
    if part_rows > 0 {
        parts.push(batch.slice(start, part_rows));
    }
    parts
}

/// Degenerate-`_timestamp` predicate of one row: timestamps are
/// microseconds since epoch, so `<= 0` (and null, reachable only on the
/// move path — stored docs columns pin the column non-null) is always
/// corrupt data, never a real event time. Rows failing this are DROPPED by
/// the producers' cleansing and counted in [`CoreFileResult::dropped_rows`].
fn is_valid_ts(ts: Option<i64>) -> bool {
    ts.is_some_and(|t| t > 0)
}

/// Drop the degenerate-`_timestamp` rows of `batch` (see [`is_valid_ts`]),
/// returning the surviving rows and the dropped count. `timestamps` must be
/// the batch's `_timestamp` column. Row order is preserved.
fn cleanse_degenerate_ts_rows(
    batch: &RecordBatch,
    timestamps: &Int64Array,
) -> Result<Option<(RecordBatch, u64)>, anyhow::Error> {
    if timestamps.iter().all(is_valid_ts) {
        return Ok(None);
    }
    let keep = BooleanArray::from_iter(timestamps.iter().map(|ts| Some(is_valid_ts(ts))));
    let cleansed = filter_record_batch(batch, &keep)?;
    let dropped = (batch.num_rows() - cleansed.num_rows()) as u64;
    Ok(Some((cleansed, dropped)))
}

/// Degenerate-`_timestamp` rows across the merge inputs' timestamp columns
/// (the row-merge drivers; null-free by [`read_timestamp_columns`]).
fn count_degenerate_ts_rows(timestamps: &[Int64Array]) -> u64 {
    timestamps
        .iter()
        .map(|ts| ts.values().iter().filter(|t| **t <= 0).count() as u64)
        .sum()
}

/// One opened compaction input. The index merge needs the full
/// [`VixReader`]; when a file's index blobs are unreadable the docs blob is
/// opened on its own ([`VixDocs`]) so the rebuild path can still merge the
/// rows (its terms are then re-derived from `_source`; term fields only that
/// input knew degrade to `partial_fields`).
enum MergeSource {
    Indexed(Box<VixReader>),
    DocsOnly(VixDocs),
}

impl MergeSource {
    fn row_count(&self) -> u64 {
        match self {
            MergeSource::Indexed(reader) => reader.row_count(),
            MergeSource::DocsOnly(docs) => docs.row_count(),
        }
    }

    fn docs_schema(&self) -> Result<SchemaRef, anyhow::Error> {
        match self {
            MergeSource::Indexed(reader) => reader.docs_schema(),
            MergeSource::DocsOnly(docs) => Ok(docs.schema().clone()),
        }
    }

    fn term_field_names(&self) -> Vec<String> {
        match self {
            MergeSource::Indexed(reader) => reader
                .term_field_names()
                .iter()
                .map(|s| s.to_string())
                .collect(),
            MergeSource::DocsOnly(_) => Vec::new(),
        }
    }

    /// Whether any document of this input carries a (non-null) value at
    /// `path` — the key-term probe. Docs-only inputs (unreadable index)
    /// cannot answer and report `false`; they force the rebuild path anyway.
    fn key_term_exists(&self, path: &str) -> bool {
        match self {
            MergeSource::Indexed(reader) => reader.key_term_exists(path).unwrap_or(false),
            MergeSource::DocsOnly(_) => false,
        }
    }

    /// Whether this input marks `name` as partially indexed.
    fn is_partial_field(&self, name: &str) -> bool {
        match self {
            MergeSource::Indexed(reader) => reader.partial_fields().contains(name),
            MergeSource::DocsOnly(_) => false,
        }
    }

    /// Read the whole `_timestamp` column — the row merge's driver, and the
    /// ONLY column the merge ever materializes across a whole file (fixed
    /// 8 bytes/row). The unbounded columns (`_source`/`_original`/cs) stream
    /// through [`stream_merge_windows`] in byte-capped batches instead: a
    /// whole-file `Utf8` materialization overflows arrow's `i32` offsets on
    /// multi-GB inputs.
    fn read_timestamp_column(&self) -> Result<ArrayRef, anyhow::Error> {
        let name = TIMESTAMP_COL_NAME;
        match self {
            MergeSource::Indexed(reader) => reader.read_docs_column(name),
            MergeSource::DocsOnly(docs) => {
                let batches = docs.read_docs(Some(&[name.to_string()]), None, None)?;
                let arrays: Vec<ArrayRef> = batches
                    .iter()
                    .map(|batch| {
                        batch
                            .column_by_name(name)
                            .cloned()
                            .ok_or_else(|| anyhow::anyhow!("docs scan is missing column {name:?}"))
                    })
                    .collect::<Result<_, _>>()?;
                if arrays.is_empty() {
                    let field = self.docs_schema()?.field_with_name(name)?.clone();
                    return Ok(new_empty_array(field.data_type()));
                }
                let refs: Vec<&dyn Array> = arrays.iter().map(AsRef::as_ref).collect();
                Ok(arrow::compute::concat(&refs)?)
            }
        }
    }
}

/// One merge input: its file key (error messages) and a ranged byte source.
/// The compactor passes cache-ladder sources (ranged reads from the local
/// disk cache with remote fallback) so input files are never materialized
/// whole in memory; tests wrap fabricated bytes in
/// [`vortex_index::BytesRangeSource`].
pub type MergeInput = (String, Arc<dyn vortex_index::VixRangeSource>);

/// The shared shape of one core-file merge, derived from the inputs and the
/// current stream settings before either merge strategy runs.
struct MergePlan {
    store_original: bool,
    /// Preserved docs columns with their target types.
    preserved: Vec<(String, DataType)>,
    writer_schema: Schema,
    opts: VixWriterOptions,
    /// Row/byte bounds of every staged docs batch.
    caps: BatchCaps,
}

/// Why the index-merge fast path was abandoned.
enum IndexedMergeFailure {
    /// The inputs' dictionaries cannot express the merge plan (tokenizer /
    /// capability mismatch, unreadable or malformed index data): rebuild the
    /// terms from `_source` instead.
    Fallback(anyhow::Error),
    /// The merge itself is impossible (docs unreadable, bad arguments):
    /// a rebuild would fail the same way.
    Fatal(anyhow::Error),
}

/// Compactor producer: k-way merge core files into one, ordered by
/// `_timestamp` descending (the storage-file convention; inputs are already
/// internally descending), ties broken stably by input order.
///
/// The **index** of the merged file is normally assembled straight from the
/// inputs' term dictionaries (`VixWriter::merge_input_indexes`: k-way merged
/// term streams, postings remapped through the row merge's doc-id maps, doc
/// counts summed, dense elision re-checked) — no `_source` parsing and no
/// re-tokenizing. When the inputs cannot support that (tokenizer version or
/// term/fts capability mismatch with the current settings, a legacy
/// dual-marked field, unreadable index blobs), the merge falls back to the
/// full rebuild ([`merge_core_files_rebuild`]) with a warning.
///
/// The **docs** blob is stream-copied batch-by-batch when the inputs occupy
/// disjoint time ranges (the row merge degenerates to a concatenation —
/// the common case, since move-job outputs do not interleave); genuinely
/// overlapping inputs go through the windowed row-interleave path. Either
/// way the staged batches are bounded by [`BatchCaps`].
///
/// Preserved docs columns: the current `column_store_fields` ∩ the union of
/// the inputs' available columns, plus `_o2_id` whenever any input stores it
/// (it is unrecoverable from `_source`). Column types follow the current
/// stream schema, falling back to the first input that has the column; an
/// input lacking a column (it predates the field's `column_store_fields`
/// entry) has it **derived from that input's `_source`**, exactly as a
/// query-time scan would extract it — never null-filled, because the merged
/// file's docs column is authoritative for reads. A configured column that
/// NO input stores is still materialized (derived for every row) when the
/// current schema types the field and some input carries values — all-old
/// groups and the single-file healing rebuild converge to current
/// capabilities. `_original` is preserved whenever any input carries it.
///
/// **Cleansing**: inputs carrying degenerate-`_timestamp` rows
/// (`_timestamp <= 0`; the pre-guard mover stored literal zeros behind
/// healthy-looking metadata) are routed to the rebuild, which DROPS those
/// rows — the index-merge fast path cannot drop rows, its doc-id maps and
/// postings remap cover every input row. [`CoreFileResult::dropped_rows`]
/// reports the count; an all-poison input set yields a zero-row result the
/// caller commits as "inputs deleted, no output file".
///
/// Synchronous and CPU-bound — call it on a blocking thread.
pub fn merge_core_files(
    inputs: &[MergeInput],
    latest_schema: &Schema,
    fts_fields: &[String],
    column_store_fields: &[String],
    bloom_fields: &[String],
) -> Result<MergedCoreFile, anyhow::Error> {
    merge_core_files_with_caps(
        inputs,
        latest_schema,
        fts_fields,
        column_store_fields,
        bloom_fields,
        BatchCaps::default(),
    )
}

/// [`merge_core_files`] with explicit batch caps (tests shrink them to prove
/// the chunked flow with small data).
fn merge_core_files_with_caps(
    inputs: &[MergeInput],
    latest_schema: &Schema,
    fts_fields: &[String],
    column_store_fields: &[String],
    bloom_fields: &[String],
    caps: BatchCaps,
) -> Result<MergedCoreFile, anyhow::Error> {
    let started = std::time::Instant::now();
    let sources = open_merge_sources(inputs)?;
    log::debug!(
        "vix merge: opened {} inputs in {:?}",
        sources.len(),
        started.elapsed()
    );
    let plan = build_merge_plan(
        &sources,
        latest_schema,
        fts_fields,
        column_store_fields,
        bloom_fields,
        caps,
    );

    let readers: Option<Vec<&VixReader>> = sources
        .iter()
        .map(|source| match source {
            MergeSource::Indexed(reader) => Some(reader.as_ref()),
            MergeSource::DocsOnly(_) => None,
        })
        .collect();
    if let Some(readers) = readers {
        match merge_core_files_indexed(inputs, &sources, &readers, &plan) {
            Ok(result) => return Ok(result),
            Err(IndexedMergeFailure::Fatal(e)) => return Err(e),
            Err(IndexedMergeFailure::Fallback(reason)) => {
                log::warn!(
                    "merge_core_files: index merge not applicable, rebuilding terms from \
                     _source: {reason:#}"
                );
            }
        }
    }
    rebuild_over_sources(inputs, &sources, &plan)
}

/// The full-rebuild merge: k-way row merge + terms re-derived from `_source`
/// with the *current* stream settings, exactly like a fresh build of the
/// merged rows. [`merge_core_files`] falls back to this when the index-merge
/// fast path does not apply; it is public as the reference implementation
/// (differential tests oracle).
pub fn merge_core_files_rebuild(
    inputs: &[MergeInput],
    latest_schema: &Schema,
    fts_fields: &[String],
    column_store_fields: &[String],
    bloom_fields: &[String],
) -> Result<MergedCoreFile, anyhow::Error> {
    merge_core_files_rebuild_with_caps(
        inputs,
        latest_schema,
        fts_fields,
        column_store_fields,
        bloom_fields,
        BatchCaps::default(),
    )
}

/// [`merge_core_files_rebuild`] with explicit batch caps.
fn merge_core_files_rebuild_with_caps(
    inputs: &[MergeInput],
    latest_schema: &Schema,
    fts_fields: &[String],
    column_store_fields: &[String],
    bloom_fields: &[String],
    caps: BatchCaps,
) -> Result<MergedCoreFile, anyhow::Error> {
    let sources = open_merge_sources(inputs)?;
    let plan = build_merge_plan(
        &sources,
        latest_schema,
        fts_fields,
        column_store_fields,
        bloom_fields,
        caps,
    );
    rebuild_over_sources(inputs, &sources, &plan)
}

/// Outcome of [`classify_core_file`]: would a single-file healing rebuild
/// change the file?
#[derive(Debug)]
pub enum CoreFileStatus {
    /// The file already carries every capability the current plan would
    /// give it — rebuilding would only churn bytes.
    Current,
    /// The file lacks capabilities the current plan carries (or its
    /// dictionary cannot express the plan at all); a rebuild heals it. The
    /// string names the first gap found.
    NeedsRebuild(String),
}

/// Classify ONE stored core file against the CURRENT stream schema and
/// settings: would compaction's healing rebuild give it capabilities it
/// lacks? This powers single-file merge groups — a partition holding one
/// file can never form a >= 2 merge group, so without this probe an
/// outdated file (fts-tainted partial field, missing numeric value terms,
/// missing configured docs columns) keeps its gaps forever.
///
/// `NeedsRebuild` fires on exactly the conditions the merge paths already
/// enforce (no new probes):
/// - [`VixWriter::check_merge_inputs`] rejects the file: tokenizer mismatch, fts/term marking
///   mismatch vs the plan, a plan-fts field marked partial (the pre-fix oversize taint), or a
///   partial field only a rebuild can re-index;
/// - [`VixWriter::merge_inputs_lacking_term_capability`] finds a term-planned field the file
///   carries without value terms (pre-numeric-value-terms files, fast-path-DEMOTED fields — the
///   index-merge fast path can only demote them again, never heal);
/// - a column the merge plan preserves ([`build_merge_plan`]: configured `column_store_fields`,
///   stored in the docs schema or derivable from `_source`) has no docs column in the file
///   (fields-table `cs`-marker probe).
///
/// COST: the container footer, fields table and dictionary directory, at
/// most one FST cell per probed field, and the docs blob's FOOTER (its
/// schema) — never postings and never docs data — so the probe is cheap
/// over a ranged [`VixRangeSource`] and a `Current` verdict downloads no
/// docs. A file whose INDEX is unreadable (malformed dictionary, missing
/// layout property) but whose docs open classifies `NeedsRebuild`: the
/// healing rebuild re-derives every term from `_source` under the CURRENT
/// plan — the same [`MergeSource::DocsOnly`] route multi-file merges take
/// — so nothing is lost (prod 2026-07-29: full-size merge outputs with
/// overlapping dict row groups were unreachable by any ≥2-file merge and
/// unhealable without this). Only a container whose DOCS are also
/// unreadable errors. Blocks on fetches — call from a blocking thread.
///
/// The verdict is stable by construction: a healing rebuild's output
/// classifies `Current` (both sides run the same [`build_merge_plan`]), so
/// healing converges instead of looping.
pub fn classify_core_file(
    key: &str,
    source: Arc<dyn VixRangeSource>,
    latest_schema: &Schema,
    fts_fields: &[String],
    column_store_fields: &[String],
    bloom_fields: &[String],
) -> Result<CoreFileStatus, anyhow::Error> {
    let reader = match VixReader::open_ranged(Arc::clone(&source)) {
        Ok(reader) => reader,
        Err(index_error) => {
            return match VixDocs::open_ranged(source) {
                // docs readable: the single-file healing rebuild re-derives
                // terms from `_source` (the DocsOnly merge route)
                Ok(_) => Ok(CoreFileStatus::NeedsRebuild(format!(
                    "index unreadable ({index_error:#}); docs are readable — rebuild from _source"
                ))),
                Err(_) => Err(anyhow::anyhow!("open core file {key}: {index_error:#}")),
            };
        }
    };
    let sources = [MergeSource::Indexed(Box::new(reader))];
    let plan = build_merge_plan(
        &sources,
        latest_schema,
        fts_fields,
        column_store_fields,
        bloom_fields,
        BatchCaps::default(),
    );
    let MergeSource::Indexed(reader) = &sources[0] else {
        unreachable!("constructed as Indexed above");
    };
    let reader = reader.as_ref();

    let writer = VixWriter::new(&plan.writer_schema, plan.opts.clone(), plan.store_original);
    if let Err(reason) = writer.check_merge_inputs(&[reader]) {
        return Ok(CoreFileStatus::NeedsRebuild(reason));
    }
    let lacking = writer
        .merge_inputs_lacking_term_capability(&[reader])
        .map_err(|reason| anyhow::anyhow!("core file {key}: {reason}"))?;
    if let Some(name) = lacking.first() {
        return Ok(CoreFileStatus::NeedsRebuild(format!(
            "field {name:?} carries values without the value terms the current plan derives \
             (a fast-path merge could only demote it)"
        )));
    }
    for (name, _) in &plan.preserved {
        if !reader.has_column_store_field(name) {
            return Ok(CoreFileStatus::NeedsRebuild(format!(
                "configured column-store field {name:?} has no docs column (derivable from \
                 _source)"
            )));
        }
    }
    Ok(CoreFileStatus::Current)
}

/// Open every input: full [`VixReader`]s normally, docs-only handles for
/// files whose index blobs are unreadable (logged; those merges rebuild).
fn open_merge_sources(inputs: &[MergeInput]) -> Result<Vec<MergeSource>, anyhow::Error> {
    if inputs.is_empty() {
        return Err(anyhow::anyhow!("merge_core_files: no input files"));
    }
    inputs
        .iter()
        .map(
            |(key, data)| match VixReader::open_ranged(Arc::clone(data)) {
                Ok(reader) => Ok(MergeSource::Indexed(Box::new(reader))),
                Err(index_error) => match VixDocs::open_ranged(Arc::clone(data)) {
                    Ok(docs) => {
                        log::warn!(
                            "merge_core_files: core file {key} has an unreadable index \
                         ({index_error:#}); merging its docs and rebuilding terms from _source"
                        );
                        Ok(MergeSource::DocsOnly(docs))
                    }
                    Err(_) => Err(anyhow::anyhow!("open core file {key}: {index_error}")),
                },
            },
        )
        .collect()
}

/// Derive the merge shape shared by both strategies (see
/// [`merge_core_files`] for the rules). Inputs whose docs schema is
/// unreadable poison nothing here — their columns are simply not offered —
/// but such files fail later when their rows are read.
fn build_merge_plan(
    sources: &[MergeSource],
    latest_schema: &Schema,
    fts_fields: &[String],
    column_store_fields: &[String],
    bloom_fields: &[String],
    caps: BatchCaps,
) -> MergePlan {
    // docs columns available across inputs (name -> first stored type),
    // writer-managed columns excluded
    let mut available: Vec<(String, DataType)> = Vec::new();
    let mut store_original = false;
    for source in sources {
        let Ok(docs_schema) = source.docs_schema() else {
            continue;
        };
        for field in docs_schema.fields() {
            match field.name().as_str() {
                TIMESTAMP_COL_NAME | SOURCE_COL_NAME => {}
                ORIGINAL_DATA_COL_NAME => store_original = true,
                name => {
                    if !available.iter().any(|(n, _)| n == name) {
                        available.push((name.to_string(), field.data_type().clone()));
                    }
                }
            }
        }
    }

    // internal columns never join the term plan or the derive-from-source
    // path: `key_term_exists` reports internals as always-present, and
    // `_o2_id` is not recoverable from `_source` (it is excluded from it)
    const NON_PLAN_COLS: [&str; 4] = [
        TIMESTAMP_COL_NAME,
        SOURCE_COL_NAME,
        ORIGINAL_DATA_COL_NAME,
        ID_COL_NAME,
    ];

    // preserved cs columns: current settings ∩ available, plus _o2_id
    let mut preserved: Vec<(String, DataType)> = Vec::new();
    for name in effective_column_store_fields(
        column_store_fields,
        available.iter().any(|(n, _)| n == ID_COL_NAME),
    ) {
        if preserved.iter().any(|(n, _)| *n == name) {
            continue;
        }
        let Some((_, stored_type)) = available.iter().find(|(n, _)| *n == name) else {
            // Configured but a docs column in NO input: the field predates
            // its `column_store_fields` entry everywhere, so its values
            // live only in `_source`. When the CURRENT schema types the
            // field and some input carries values (key-term probe),
            // MATERIALIZE the column — `normalize_merge_chunk` derives it
            // from `_source` per chunk, exactly like the mixed-inputs case
            // (DESIGN §8) — so all-old merge groups and the single-file
            // healing rebuild converge to current capabilities instead of
            // leaving the column missing forever. Untyped (not in the
            // schema) or value-less fields stay unmaterialized: there is
            // nothing to derive.
            if !NON_PLAN_COLS.contains(&name.as_str())
                && let Ok(field) = latest_schema.field_with_name(&name)
                && sources.iter().any(|source| source.key_term_exists(&name))
            {
                let data_type = field.data_type().clone();
                preserved.push((name, data_type));
            }
            continue;
        };
        let target_type = latest_schema
            .field_with_name(&name)
            .map(|field| field.data_type().clone())
            .unwrap_or_else(|_| stored_type.clone());
        preserved.push((name, target_type));
    }

    // writer construction schema: _timestamp + preserved cs columns (their
    // target types) + every term field any input knows (string-typed unless
    // already preserved with another type)
    let mut writer_fields: Vec<Field> =
        vec![Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false)];
    for (name, data_type) in &preserved {
        writer_fields.push(Field::new(name, data_type.clone(), true));
    }
    for source in sources {
        for name in source.term_field_names() {
            if name != TIMESTAMP_COL_NAME
                && !writer_fields.iter().any(|field| field.name() == &name)
            {
                writer_fields.push(Field::new(&name, DataType::Utf8, true));
            }
        }
    }
    // ... plus every value-indexable field of the CURRENT stream schema that
    // some input CARRIES (key-term probe) without value-indexing it — e.g. a
    // numeric field in files written before numeric value terms existed
    // (only its key terms and `_source` values exist). With a plan field id,
    // a REBUILD re-derives its value terms from `_source` (old files
    // converge to fully indexed at compaction), and the index-merge fast
    // path can apply its per-field capability INTERSECTION (demotion)
    // instead of silently claiming term coverage the merged dictionary lacks.
    // Fields some input marks partial-without-value-indexing stay OUT of the
    // plan: planning them would force the rebuild path on every merge
    // (check_merge_inputs), while leaving them out keeps today's
    // partial-union behavior.
    {
        let known: std::collections::HashSet<String> = writer_fields
            .iter()
            .map(|field| field.name().clone())
            .collect();
        for field in latest_schema.fields() {
            let name = field.name().as_str();
            if known.contains(name)
                || NON_PLAN_COLS.contains(&name)
                || !vortex_index::is_value_indexed_type(field.data_type())
            {
                continue;
            }
            if sources.iter().any(|source| source.is_partial_field(name)) {
                continue;
            }
            if sources.iter().any(|source| source.key_term_exists(name)) {
                writer_fields.push(Field::new(name, field.data_type().clone(), true));
            }
        }
    }
    let writer_schema = Schema::new(writer_fields);
    let mut opts = core_writer_options(
        fts_fields,
        preserved.iter().map(|(name, _)| name.clone()).collect(),
        bloom_fields.to_vec(),
    );
    opts.encode_threads = merge_threads();
    // Rebuilds re-derive EVERY term from _source and, for a 10GB-original
    // group, used to hold ~15-19GB of term map until finish — the
    // compactor's worst-case memory bound. Spill runs to the data volume
    // (the compactor's PVC) bound it to the budget; the fast path never
    // accumulates terms, so this only pays on the rebuild/healing tail.
    let scratch = std::path::Path::new(&get_config().common.data_dir).join("vix_spill");
    opts.term_spill_dir = Some(scratch.clone());
    // ... and the finished container spools there too: the upload streams
    // from the spool, so the merged multi-GB object never resides in RAM.
    opts.output_spool_dir = Some(scratch);
    MergePlan {
        store_original,
        preserved,
        writer_schema,
        opts,
        caps,
    }
}

/// K-way merge head: max-heap on `_timestamp` (descending output), equal
/// timestamps resolve to the smaller input index (stable ties).
struct Head {
    ts: i64,
    input: usize,
}
impl PartialEq for Head {
    fn eq(&self, other: &Self) -> bool {
        self.ts == other.ts && self.input == other.input
    }
}
impl Eq for Head {}
impl PartialOrd for Head {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Head {
    fn cmp(&self, other: &Self) -> Ordering {
        // max-heap: larger ts first; equal ts -> smaller input index first
        self.ts
            .cmp(&other.ts)
            .then_with(|| other.input.cmp(&self.input))
    }
}

/// Read every input's `_timestamp` column (rejecting nulls) — the row
/// merge's driver.
fn read_timestamp_columns(
    inputs: &[MergeInput],
    sources: &[MergeSource],
) -> Result<Vec<Int64Array>, anyhow::Error> {
    sources
        .iter()
        .zip(inputs)
        .map(|(source, (key, _))| {
            let column = source
                .read_timestamp_column()
                .map_err(|e| anyhow::anyhow!("read core file {key}: {e}"))?;
            let timestamps = as_int64_array(&column)?;
            if timestamps.null_count() > 0 {
                return Err(anyhow::anyhow!("core file {key} has null _timestamp rows"));
            }
            Ok(timestamps)
        })
        .collect()
}

/// Run the k-way `_timestamp` merge over the inputs' timestamp columns and
/// return each input's `old row -> merged doc id` map.
fn merge_order(timestamps: &[Int64Array]) -> Vec<Vec<u32>> {
    let mut maps: Vec<Vec<u32>> = timestamps.iter().map(|ts| vec![0u32; ts.len()]).collect();
    let mut cursors = vec![0usize; timestamps.len()];
    let mut heap = BinaryHeap::with_capacity(timestamps.len());
    for (index, ts) in timestamps.iter().enumerate() {
        if !ts.is_empty() {
            heap.push(Head {
                ts: ts.value(0),
                input: index,
            });
        }
    }
    let mut next = 0u32;
    while let Some(head) = heap.pop() {
        let input = head.input;
        let row = cursors[input];
        cursors[input] += 1;
        if cursors[input] < timestamps[input].len() {
            heap.push(Head {
                ts: timestamps[input].value(cursors[input]),
                input,
            });
        }
        maps[input][row] = next;
        next += 1;
    }
    maps
}

/// When every input's rows land in one contiguous run of the merged file
/// (disjoint time ranges), return the per-input run offsets — the doc-id
/// maps degenerate to constants and the docs blob is a concatenation.
fn contiguous_offsets(maps: &[Vec<u32>]) -> Option<Vec<u32>> {
    let mut offsets = Vec::with_capacity(maps.len());
    for map in maps {
        let base = map.first().copied().unwrap_or(0);
        if map
            .iter()
            .enumerate()
            .any(|(row, &id)| id != base + row as u32)
        {
            return None;
        }
        offsets.push(base);
    }
    Some(offsets)
}

/// `merged doc id -> (input, input row)` — the inverse of the per-input maps.
fn merge_order_inverse(maps: &[Vec<u32>]) -> Vec<(usize, usize)> {
    let total: usize = maps.iter().map(Vec::len).sum();
    let mut order = vec![(0usize, 0usize); total];
    for (input, map) in maps.iter().enumerate() {
        for (row, &new_id) in map.iter().enumerate() {
            order[new_id as usize] = (input, row);
        }
    }
    order
}

/// The index-merge fast path (see [`merge_core_files`]).
fn merge_core_files_indexed(
    inputs: &[MergeInput],
    sources: &[MergeSource],
    readers: &[&VixReader],
    plan: &MergePlan,
) -> Result<MergedCoreFile, IndexedMergeFailure> {
    use IndexedMergeFailure::{Fallback, Fatal};

    let mut writer = VixWriter::new(&plan.writer_schema, plan.opts.clone(), plan.store_original);
    writer
        .check_merge_inputs(readers)
        .map_err(|reason| Fallback(anyhow::anyhow!(reason)))?;

    // row merge (timestamps only): failures here would hit a rebuild too
    let started = std::time::Instant::now();
    let timestamps = read_timestamp_columns(inputs, sources).map_err(Fatal)?;
    // Degenerate-`_timestamp` rows must be DROPPED (compaction-time
    // cleansing), which the index merge cannot express: the doc-id maps
    // remap postings over EVERY input row, so removing rows would corrupt
    // the doc ids. Route such inputs to the rebuild, whose per-chunk
    // normalization drops the rows before term re-derivation. One-time
    // cost: once cleansed, later merges take the fast path again.
    let degenerate = count_degenerate_ts_rows(&timestamps);
    if degenerate > 0 {
        return Err(Fallback(anyhow::anyhow!(
            "inputs carry {degenerate} rows with a degenerate _timestamp <= 0 (pre-guard stored \
             data); dropping them requires the rebuild path"
        )));
    }
    let maps = merge_order(&timestamps);
    let offsets = contiguous_offsets(&maps);
    let doc_maps: Vec<DocIdMap> = match &offsets {
        Some(offsets) => offsets
            .iter()
            .map(|&offset| DocIdMap::Offset(offset))
            .collect(),
        None => maps
            .iter()
            .map(|map| DocIdMap::Table(map.clone()))
            .collect(),
    };
    log::debug!(
        "vix merge: row merge order over {} inputs in {:?} (disjoint: {})",
        inputs.len(),
        started.elapsed(),
        offsets.is_some(),
    );

    // merge the term dictionaries BEFORE any docs work: every index-side
    // problem (malformed postings, remap disorder, ...) falls back to the
    // rebuild with nothing wasted
    let started = std::time::Instant::now();
    writer
        .merge_input_indexes(readers, &doc_maps, merge_threads())
        .map_err(Fallback)?;
    log::debug!("vix merge: index merge total {:?}", started.elapsed());

    let started = std::time::Instant::now();
    let docs_batches = if let Some(offsets) = offsets {
        // disjoint inputs: the merged docs blob is the inputs' rows
        // concatenated in offset order — streamed batch copy, no per-row
        // work. Each input decodes (and normalizes) on its own thread a
        // bounded channel ahead of the pushes, which stay ordered.
        let mut input_order: Vec<usize> = (0..inputs.len()).collect();
        input_order.sort_unstable_by_key(|&index| offsets[index]);
        stream_inputs_sequential(inputs, plan, &input_order, |ts, cs, source, original| {
            writer.push_docs_rows_unindexed(ts, cs, source, original)
        })
        .map_err(Fatal)?
    } else {
        // overlapping inputs: interleave rows in merged order (same
        // streaming and windowing as the rebuild, minus all term extraction)
        let order = merge_order_inverse(&maps);
        stream_merge_windows(inputs, plan, &order, |ts, cs, source, original| {
            writer.push_docs_rows_unindexed(ts, cs, source, original)
        })
        .map_err(Fatal)?
    };
    log::debug!(
        "vix merge: docs rows staged in {:?} ({docs_batches} bounded batches)",
        started.elapsed()
    );

    let started = std::time::Instant::now();
    let (output, stats) = writer.finish_output().map_err(Fatal)?;
    log::debug!(
        "vix merge: finish (docs blob encode + container) in {:?}",
        started.elapsed()
    );
    Ok(MergedCoreFile {
        output,
        stats,
        used_index_merge: true,
        docs_batches,
        // poison inputs fell back to the rebuild before any index work
        dropped_rows: 0,
    })
}

/// One normalized run of consecutive docs rows of one input, in the merge
/// plan's shapes: `_timestamp` as `i64`, the preserved cs columns cast to
/// their target types — **derived from this run's `_source`** when the input
/// predates the column (never null-filled, DESIGN §8) — and
/// `_source`/`_original` as `Utf8`. Runs are bounded by the plan's
/// [`BatchCaps`] (oversized decoded chunks are split before normalization)
/// and carry per-row byte sizes for the window accounting.
struct MergeChunk {
    timestamps: Int64Array,
    /// Aligned with `plan.preserved`.
    cs: Vec<ArrayRef>,
    source: StringArray,
    /// All-null when the input has no `_original` column; ignored when the
    /// plan does not store `_original`.
    original: StringArray,
    /// Per-row variable-length bytes (`_source` + `_original` + cs values).
    row_bytes: Vec<u32>,
}

impl MergeChunk {
    fn rows(&self) -> usize {
        self.timestamps.len()
    }
}

/// Normalize one (already byte-capped) decoded docs batch into a
/// [`MergeChunk`]. Runs on the decode threads, so per-chunk cs derivation
/// (`json_get_*` over `_source`) parallelizes across inputs.
///
/// Cleansing happens FIRST: degenerate-`_timestamp` rows are dropped before
/// cs derivation here and before term emission downstream (the rebuild's
/// windowed [`VixWriter::push_docs_rows`]), so stats, terms, zone tables and
/// cs columns are all derived from the surviving row set only. The row-merge
/// driver ([`rebuild_over_sources`]) filters the SAME rows out of its
/// `_timestamp` columns, keeping the merge order aligned with the streams;
/// the index-merge fast path never sees poison (it falls back on it).
fn normalize_merge_chunk(
    key: &str,
    plan: &MergePlan,
    batch: &RecordBatch,
) -> Result<MergeChunk, anyhow::Error> {
    let raw_timestamps =
        as_int64_array(batch.column_by_name(TIMESTAMP_COL_NAME).ok_or_else(|| {
            anyhow::anyhow!("core file {key}: docs batch is missing {TIMESTAMP_COL_NAME:?}")
        })?)?;
    let cleansed_batch;
    let (batch, timestamps) = match cleanse_degenerate_ts_rows(batch, &raw_timestamps)? {
        Some((cleansed, _)) => {
            let timestamps =
                as_int64_array(cleansed.column_by_name(TIMESTAMP_COL_NAME).ok_or_else(|| {
                    anyhow::anyhow!("core file {key}: cleansed batch lost {TIMESTAMP_COL_NAME:?}")
                })?)?;
            cleansed_batch = cleansed;
            (&cleansed_batch, timestamps)
        }
        None => (batch, raw_timestamps),
    };
    let rows = batch.num_rows();
    let source = as_string_array(batch.column_by_name(SOURCE_COL_NAME).ok_or_else(|| {
        anyhow::anyhow!("core file {key}: docs batch is missing {SOURCE_COL_NAME:?}")
    })?)?;
    let original = match batch.column_by_name(ORIGINAL_DATA_COL_NAME) {
        Some(column) => as_string_array(column)?,
        None => StringArray::new_null(rows),
    };
    let mut cs = Vec::with_capacity(plan.preserved.len());
    for (name, target_type) in &plan.preserved {
        let column = match batch.column_by_name(name) {
            Some(column) => cast(column, target_type).map_err(|e| {
                anyhow::anyhow!("core file {key}: column {name:?} cast to {target_type}: {e}")
            })?,
            // Pre-column input: the field predates its
            // `column_store_fields` entry, so it has no docs column and
            // lives only in `_source`. DERIVE it exactly as a query-time
            // scan would extract it (identical `json_get_*` + cast), instead
            // of null-filling — the merged file materializes ONE
            // authoritative docs column that reads serve from directly, so a
            // null fill would silently drop these rows from
            // aggregations/TopN (DESIGN §8).
            None => derive_cs_column_from_source(&source, name, target_type).map_err(|e| {
                anyhow::anyhow!("core file {key}: derive column {name:?} from _source: {e}")
            })?,
        };
        cs.push(column);
    }
    let mut accessors: Vec<VarBytes> = Vec::with_capacity(cs.len() + 2);
    accessors.push(VarBytes::new(&source));
    accessors.push(VarBytes::new(&original));
    for column in &cs {
        accessors.push(VarBytes::new(column.as_ref()));
    }
    let row_bytes: Vec<u32> = (0..rows)
        .map(|row| {
            accessors
                .iter()
                .map(|accessor| accessor.get(row))
                .sum::<usize>()
                .min(u32::MAX as usize) as u32
        })
        .collect();
    Ok(MergeChunk {
        timestamps,
        cs,
        source,
        original,
        row_bytes,
    })
}

/// Spawn one input's decode thread: stream the projected docs columns
/// ([`VixDocs::scan_docs`], one decoded chunk at a time), split every chunk
/// to the byte caps, normalize, and send the bounded [`MergeChunk`]s through
/// a small channel — the input-side memory stays a few chunks regardless of
/// file size. The thread stops as soon as the receiver is dropped.
fn spawn_input_stream<'scope, 'env>(
    scope: &'scope std::thread::Scope<'scope, 'env>,
    key: &'env str,
    data: Arc<dyn vortex_index::VixRangeSource>,
    plan: &'env MergePlan,
) -> Receiver<Result<MergeChunk, anyhow::Error>> {
    let (tx, rx) = sync_channel(2);
    scope.spawn(move || {
        if let Err(error) = stream_input_chunks(key, data, plan, &tx) {
            // a failed send here means the consumer is already gone
            let _ = tx.send(Err(error));
        }
    });
    rx
}

fn stream_input_chunks(
    key: &str,
    data: Arc<dyn vortex_index::VixRangeSource>,
    plan: &MergePlan,
    tx: &SyncSender<Result<MergeChunk, anyhow::Error>>,
) -> Result<(), anyhow::Error> {
    let docs =
        VixDocs::open_ranged(data).map_err(|e| anyhow::anyhow!("open core file {key}: {e}"))?;
    if docs.row_count() == 0 {
        return Ok(());
    }
    let schema = docs.schema().clone();
    let mut projection: Vec<String> =
        vec![TIMESTAMP_COL_NAME.to_string(), SOURCE_COL_NAME.to_string()];
    for (name, _) in &plan.preserved {
        if schema.field_with_name(name).is_ok() {
            projection.push(name.clone());
        }
    }
    if plan.store_original && schema.field_with_name(ORIGINAL_DATA_COL_NAME).is_ok() {
        projection.push(ORIGINAL_DATA_COL_NAME.to_string());
    }
    docs.scan_docs(Some(&projection), None, None, &mut |batch| {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        for part in split_batch_by_bytes(&batch, plan.caps) {
            let chunk = normalize_merge_chunk(key, plan, &part)?;
            if chunk.rows() == 0 {
                // every row of the part was cleansed away: nothing to stage
                // (the merge order skipped the same rows)
                continue;
            }
            if tx.send(Ok(chunk)).is_err() {
                // consumer gone (finished or failed): abort the scan quietly
                return Err(anyhow::anyhow!("merge staging stopped"));
            }
        }
        Ok(())
    })
    .map_err(|e| anyhow::anyhow!("stream core file {key}: {e}"))
}

/// The staging side of one input's decode stream: buffers only the chunks
/// covering the not-yet-pushed rows the open window needs.
struct InputCursor {
    key: String,
    rx: Receiver<Result<MergeChunk, anyhow::Error>>,
    pending: VecDeque<MergeChunk>,
    /// Rows of `pending[0]` already taken by closed windows.
    consumed: usize,
    /// Rows staged into the open window (starting at `consumed`, running
    /// across `pending` in order).
    staged: usize,
}

/// One input's contribution to one merge window (the interleave sources).
struct StagedInput {
    timestamps: ArrayRef,
    cs: Vec<ArrayRef>,
    source: ArrayRef,
    original: ArrayRef,
}

/// A typed-empty [`StagedInput`] for inputs a window takes no rows from —
/// keeps the interleave arrays aligned with the input indices.
fn empty_staged_input(plan: &MergePlan) -> StagedInput {
    StagedInput {
        timestamps: new_empty_array(&DataType::Int64),
        cs: plan
            .preserved
            .iter()
            .map(|(_, data_type)| new_empty_array(data_type))
            .collect(),
        source: new_empty_array(&DataType::Utf8),
        original: new_empty_array(&DataType::Utf8),
    }
}

/// One array from consecutive slices (single-slice fast path).
fn concat_parts(parts: Vec<ArrayRef>) -> Result<ArrayRef, anyhow::Error> {
    if parts.len() == 1 {
        return Ok(parts.into_iter().next().expect("one part"));
    }
    let refs: Vec<&dyn Array> = parts.iter().map(AsRef::as_ref).collect();
    Ok(arrow::compute::concat(&refs)?)
}

impl InputCursor {
    fn new(key: String, rx: Receiver<Result<MergeChunk, anyhow::Error>>) -> Self {
        Self {
            key,
            rx,
            pending: VecDeque::new(),
            consumed: 0,
            staged: 0,
        }
    }

    /// Receive the next chunk from the decode thread (`None` on clean end).
    fn recv_chunk(&mut self) -> Result<Option<MergeChunk>, anyhow::Error> {
        match self.rx.recv() {
            Ok(Ok(chunk)) => Ok(Some(chunk)),
            Ok(Err(error)) => Err(error),
            Err(_) => Ok(None),
        }
    }

    /// The next whole chunk in row order (the sequential path).
    fn next_chunk(&mut self) -> Result<Option<MergeChunk>, anyhow::Error> {
        debug_assert_eq!(self.consumed + self.staged, 0);
        if let Some(chunk) = self.pending.pop_front() {
            return Ok(Some(chunk));
        }
        self.recv_chunk()
    }

    /// Stage the next unstaged row for the open window and return its
    /// variable-length byte size. Pulls chunks from the decode thread on
    /// demand.
    fn stage_next_row(&mut self) -> Result<usize, anyhow::Error> {
        // absolute position across `pending` (offset of pending[0] included)
        let mut position = self.consumed + self.staged;
        let mut chunk_index = 0usize;
        loop {
            match self.pending.get(chunk_index) {
                Some(chunk) if position < chunk.rows() => {
                    self.staged += 1;
                    return Ok(chunk.row_bytes[position] as usize);
                }
                Some(chunk) => {
                    position -= chunk.rows();
                    chunk_index += 1;
                }
                None => match self.recv_chunk()? {
                    Some(chunk) => self.pending.push_back(chunk),
                    None => {
                        return Err(anyhow::anyhow!(
                            "core file {}: docs stream ended before all rows of the timestamp \
                             column were staged",
                            self.key
                        ));
                    }
                },
            }
        }
    }

    /// The staged rows as one array per column (concatenating across chunk
    /// boundaries — bounded by the window caps), advancing the cursor past
    /// them.
    fn take_staged(&mut self, plan: &MergePlan) -> Result<StagedInput, anyhow::Error> {
        let mut remaining = self.staged;
        let mut ts_parts: Vec<ArrayRef> = Vec::new();
        let mut source_parts: Vec<ArrayRef> = Vec::new();
        let mut original_parts: Vec<ArrayRef> = Vec::new();
        let mut cs_parts: Vec<Vec<ArrayRef>> = vec![Vec::new(); plan.preserved.len()];
        while remaining > 0 {
            let chunk = self.pending.front().ok_or_else(|| {
                anyhow::anyhow!(
                    "core file {}: internal: staged rows beyond the buffered chunks",
                    self.key
                )
            })?;
            let start = self.consumed;
            let len = remaining.min(chunk.rows() - start);
            ts_parts.push(Arc::new(chunk.timestamps.slice(start, len)));
            source_parts.push(Arc::new(chunk.source.slice(start, len)));
            original_parts.push(Arc::new(chunk.original.slice(start, len)));
            for (column, parts) in chunk.cs.iter().zip(&mut cs_parts) {
                parts.push(column.slice(start, len));
            }
            remaining -= len;
            if start + len == chunk.rows() {
                self.pending.pop_front();
                self.consumed = 0;
            } else {
                self.consumed = start + len;
            }
        }
        self.staged = 0;
        Ok(StagedInput {
            timestamps: concat_parts(ts_parts)?,
            source: concat_parts(source_parts)?,
            original: concat_parts(original_parts)?,
            cs: cs_parts
                .into_iter()
                .map(concat_parts)
                .collect::<Result<_, _>>()?,
        })
    }
}

/// Stream the merged rows to `push` in bounded windows: walk `order` (the
/// merged doc order), staging each row on its input's cursor, and close a
/// window when it reaches the plan's row cap or byte budget. Each window
/// interleaves the inputs' staged runs into one bounded batch — the ONLY
/// arrays the row-interleave merge materializes over
/// `_source`/`_original`/cs values, so no whole-file column ever exists in
/// memory (a whole-hour `_source` column overflows arrow's `i32` Utf8
/// offsets). Returns the number of windows pushed.
fn stream_merge_windows(
    inputs: &[MergeInput],
    plan: &MergePlan,
    order: &[(usize, usize)],
    mut push: impl FnMut(
        &Int64Array,
        &[(String, ArrayRef)],
        &StringArray,
        Option<&StringArray>,
    ) -> Result<(), anyhow::Error>,
) -> Result<usize, anyhow::Error> {
    std::thread::scope(|scope| {
        let mut cursors: Vec<InputCursor> = inputs
            .iter()
            .map(|(key, data)| {
                InputCursor::new(
                    key.clone(),
                    spawn_input_stream(scope, key, Arc::clone(data), plan),
                )
            })
            .collect();

        let mut windows = 0usize;
        let mut start = 0usize;
        while start < order.len() {
            // stage rows until a cap closes the window (always ≥ 1 row)
            let mut end = start;
            let mut bytes = 0usize;
            while end < order.len() && end - start < plan.caps.rows.max(1) {
                let (input, _) = order[end];
                bytes = bytes.saturating_add(cursors[input].stage_next_row()?);
                end += 1;
                if bytes >= plan.caps.bytes.max(1) {
                    break;
                }
            }

            let staged: Vec<StagedInput> = cursors
                .iter_mut()
                .map(|cursor| {
                    if cursor.staged > 0 {
                        cursor.take_staged(plan)
                    } else {
                        Ok(empty_staged_input(plan))
                    }
                })
                .collect::<Result<_, _>>()?;

            // interleave indices: each merged row is its input's next
            // staged row
            let mut positions = vec![0usize; cursors.len()];
            let indices: Vec<(usize, usize)> = order[start..end]
                .iter()
                .map(|&(input, _)| {
                    let position = positions[input];
                    positions[input] += 1;
                    (input, position)
                })
                .collect();

            let timestamps = {
                let arrays: Vec<&dyn Array> =
                    staged.iter().map(|s| s.timestamps.as_ref()).collect();
                as_int64_array(&interleave(&arrays, &indices)?)?
            };
            let source = {
                let arrays: Vec<&dyn Array> = staged.iter().map(|s| s.source.as_ref()).collect();
                as_string_array(&interleave(&arrays, &indices)?)?
            };
            let original = if plan.store_original {
                let arrays: Vec<&dyn Array> = staged.iter().map(|s| s.original.as_ref()).collect();
                Some(as_string_array(&interleave(&arrays, &indices)?)?)
            } else {
                None
            };
            let cs_columns: Vec<(String, ArrayRef)> = plan
                .preserved
                .iter()
                .enumerate()
                .map(|(column, (name, _))| {
                    let arrays: Vec<&dyn Array> =
                        staged.iter().map(|s| s.cs[column].as_ref()).collect();
                    Ok((name.clone(), interleave(&arrays, &indices)?))
                })
                .collect::<Result<_, arrow::error::ArrowError>>()?;
            push(&timestamps, &cs_columns, &source, original.as_ref())?;
            windows += 1;
            start = end;
        }
        Ok(windows)
    })
}

/// Stream the inputs' rows to `push` input-by-input in `input_order` (the
/// disjoint fast path: the merged docs blob is the inputs' rows
/// concatenated). All inputs decode in parallel, each a bounded channel
/// ahead of the staging; every pushed batch is byte-capped. Returns the
/// number of batches pushed.
fn stream_inputs_sequential(
    inputs: &[MergeInput],
    plan: &MergePlan,
    input_order: &[usize],
    mut push: impl FnMut(
        &Int64Array,
        &[(String, ArrayRef)],
        &StringArray,
        Option<&StringArray>,
    ) -> Result<(), anyhow::Error>,
) -> Result<usize, anyhow::Error> {
    std::thread::scope(|scope| {
        let mut cursors: Vec<InputCursor> = input_order
            .iter()
            .map(|&index| {
                let (key, data) = &inputs[index];
                InputCursor::new(
                    key.clone(),
                    spawn_input_stream(scope, key, Arc::clone(data), plan),
                )
            })
            .collect();
        let mut batches = 0usize;
        for cursor in &mut cursors {
            while let Some(chunk) = cursor.next_chunk()? {
                let cs_columns: Vec<(String, ArrayRef)> = plan
                    .preserved
                    .iter()
                    .zip(&chunk.cs)
                    .map(|((name, _), column)| (name.clone(), Arc::clone(column)))
                    .collect();
                push(
                    &chunk.timestamps,
                    &cs_columns,
                    &chunk.source,
                    plan.store_original.then_some(&chunk.original),
                )?;
                batches += 1;
            }
        }
        Ok(batches)
    })
}

/// The rebuild strategy body: k-way merge order from the timestamp columns,
/// then stream the rows through the indexed push path in bounded windows —
/// terms (numeric value terms included) are re-derived from `_source` per
/// window, cs derivation runs per chunk on the decode threads, and no
/// whole-file `_source`/`_original` array is ever materialized.
///
/// Degenerate-`_timestamp` rows are CLEANSED here: the merge order is built
/// over the FILTERED timestamp columns while [`normalize_merge_chunk`]
/// drops the same rows from the decode streams (arrow's filter preserves
/// order, so the per-input row sequences stay aligned). Everything derived
/// downstream — terms, stats, the zone table, cs columns — sees only the
/// surviving rows, and the writer's finish guard passes by construction.
/// An all-poison input set yields a legitimate EMPTY file (`row_count` 0)
/// with `dropped_rows` telling the caller what happened.
fn rebuild_over_sources(
    inputs: &[MergeInput],
    sources: &[MergeSource],
    plan: &MergePlan,
) -> Result<MergedCoreFile, anyhow::Error> {
    let mut writer = VixWriter::new(&plan.writer_schema, plan.opts.clone(), plan.store_original);

    let timestamps = read_timestamp_columns(inputs, sources)?;
    let dropped_rows = count_degenerate_ts_rows(&timestamps);
    let timestamps: Vec<Int64Array> = if dropped_rows == 0 {
        timestamps
    } else {
        timestamps
            .iter()
            .map(|ts| Int64Array::from_iter_values(ts.values().iter().copied().filter(|t| *t > 0)))
            .collect()
    };
    let maps = merge_order(&timestamps);
    let order = merge_order_inverse(&maps);
    let expected_rows: usize = sources
        .iter()
        .map(|source| source.row_count() as usize)
        .sum::<usize>()
        - dropped_rows as usize;
    if order.len() != expected_rows {
        return Err(anyhow::anyhow!(
            "merge_core_files ordered {} rows, expected {expected_rows}",
            order.len()
        ));
    }
    let docs_batches = stream_merge_windows(inputs, plan, &order, |ts, cs, source, original| {
        writer.push_docs_rows(ts, cs, source, original)
    })?;
    log::debug!("vix merge: rebuild staged docs in {docs_batches} bounded batches");

    let (output, stats) = writer.finish_output()?;
    Ok(MergedCoreFile {
        output,
        stats,
        used_index_merge: false,
        docs_batches,
        dropped_rows,
    })
}

#[cfg(test)]
mod tests {
    use arrow::array::{BooleanArray, Int64Array, StringArray};
    use datafusion::datasource::MemTable;
    use vortex_index::{VixDocs, VixQuery, VixReader};

    use super::*;

    /// Wrap fabricated in-memory files as ranged merge inputs (the merge
    /// paths take [`MergeInput`] — production feeds cache-ladder sources).
    fn as_inputs(v: &[(String, bytes::Bytes)]) -> Vec<MergeInput> {
        v.iter()
            .map(|(key, data)| {
                (
                    key.clone(),
                    vortex_index::BytesRangeSource::new(key.clone(), data.clone()),
                )
            })
            .collect()
    }

    fn exact(field: &str, token: &str) -> VixQuery {
        VixQuery::Exact {
            field: field.to_string(),
            token: token.as_bytes().to_vec(),
        }
    }

    fn key_exists(path: &str) -> VixQuery {
        VixQuery::KeyExists {
            path: path.to_string(),
        }
    }

    fn matching_docs(reader: &VixReader, query: &VixQuery) -> Vec<u32> {
        reader
            .eval(query)
            .unwrap()
            .iter()
            .enumerate()
            .filter_map(|(doc, set)| set.then_some(doc as u32))
            .collect()
    }

    fn read_i64(reader: &VixReader, name: &str) -> Vec<i64> {
        as_int64_array(&reader.read_docs_column(name).unwrap())
            .unwrap()
            .values()
            .to_vec()
    }

    fn read_strings(reader: &VixReader, name: &str) -> Vec<Option<String>> {
        as_string_array(&reader.read_docs_column(name).unwrap())
            .unwrap()
            .iter()
            .map(|value| value.map(str::to_string))
            .collect()
    }

    /// WAL-style batches: flattened columns plus `_o2_id`/`_original`, rows
    /// deliberately NOT in timestamp order.
    fn wal_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("log", DataType::Utf8, true),
            Field::new("svc", DataType::Utf8, true),
            Field::new("code", DataType::Int64, true),
            Field::new("ok", DataType::Boolean, true),
            Field::new(ID_COL_NAME, DataType::Int64, true),
            Field::new(ORIGINAL_DATA_COL_NAME, DataType::Utf8, true),
        ]))
    }

    fn wal_batches(schema: &Arc<Schema>) -> Vec<RecordBatch> {
        let batch1 = RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int64Array::from(vec![300, 100])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("error timeout db"),
                    Some("all good"),
                ])),
                Arc::new(StringArray::from(vec![Some("api"), Some("web")])),
                Arc::new(Int64Array::from(vec![Some(500), None])),
                Arc::new(BooleanArray::from(vec![Some(false), Some(true)])),
                Arc::new(Int64Array::from(vec![Some(1), Some(2)])),
                Arc::new(StringArray::from(vec![Some("raw-300"), None])),
            ],
        )
        .unwrap();
        let batch2 = RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int64Array::from(vec![200, 400])) as ArrayRef,
                Arc::new(StringArray::from(vec![None, Some("error again")])),
                Arc::new(StringArray::from(vec![Some("api"), Some("db")])),
                Arc::new(Int64Array::from(vec![Some(200), Some(503)])),
                Arc::new(BooleanArray::from(vec![None, Some(false)])),
                Arc::new(Int64Array::from(vec![Some(3), Some(4)])),
                Arc::new(StringArray::from(vec![Some("raw-200"), Some("raw-400")])),
            ],
        )
        .unwrap();
        vec![batch1, batch2]
    }

    /// The spooled arm of the move build: an input-bytes figure past the
    /// spool threshold streams the container to `<wal>/vix_spool`, leaves
    /// `data` EMPTY, and the spool bytes are a complete, readable .vix whose
    /// length matches `output.len()` (the move job's compressed_size).
    #[tokio::test]
    async fn move_job_spools_big_builds() {
        let schema = wal_schema();
        let table =
            Arc::new(MemTable::try_new(schema.clone(), vec![wal_batches(&schema)]).unwrap());
        let result = write_core_file_from_tables(
            "test-move-spool",
            schema,
            vec![table],
            &["log".to_string()],
            &["svc".to_string()],
            &[],
            false,
            usize::MAX, // any real batch is "big enough" -> forces the spool
        )
        .await
        .unwrap();

        assert!(result.data.is_empty(), "spooled build must not buffer");
        let output = result.output.expect("spooled output");
        let spool = output.spool_path().expect("spool path");
        assert!(
            spool.parent().is_some_and(|d| d.ends_with("vix_spool")),
            "spool lands in <wal>/vix_spool, got {spool:?}"
        );
        let bytes = std::fs::read(spool).unwrap();
        assert_eq!(bytes.len() as u64, output.len());
        assert_eq!(result.stats.row_count, 4);
        let reader = VixReader::open(bytes::Bytes::from(bytes)).unwrap();
        assert_eq!(reader.row_count(), 4);
        assert_eq!(
            read_i64(&reader, TIMESTAMP_COL_NAME),
            vec![400, 300, 200, 100]
        );
        // dropping the output deletes the spool file
        let path = spool.to_path_buf();
        drop(output);
        assert!(!path.exists(), "spool file must delete on drop");
    }

    /// (a) The move-job producer over synthetic WAL batches: one core file,
    /// rows in _timestamp DESC order, terms + key terms queryable, _source
    /// faithful (with `_timestamp`, without `_o2_id`/`_original`), cs
    /// columns (incl. the auto-preserved `_o2_id`) and `_original` stored,
    /// stats usable for FileMeta.
    #[tokio::test]
    async fn move_job_writes_one_core_file() {
        let schema = wal_schema();
        let table =
            Arc::new(MemTable::try_new(schema.clone(), vec![wal_batches(&schema)]).unwrap());
        let result = write_core_file_from_tables(
            "test-move-job",
            schema,
            vec![table],
            &["log".to_string()],
            &["svc".to_string()],
            &[],
            false, // setting off, but the batches carry _original -> kept,
            0,
        )
        .await
        .unwrap();

        assert!(!result.data.is_empty());
        assert!(result.stats.index_size > 0);
        assert!(result.stats.docs_size > 0);
        assert_eq!(result.stats.row_count, 4);
        // FileMeta mapping (as done by the move job): compressed_size is the
        // object size, index_size the embedded index bytes
        assert!(result.stats.index_size < result.data.len() as u64);
        // ... and min_ts/max_ts come from the DATA the writer stored — the
        // authoritative FileMeta range (WAL footer metadata is never trusted)
        assert_eq!((result.stats.min_ts, result.stats.max_ts), (100, 400));

        let reader = VixReader::open(bytes::Bytes::from(result.data)).unwrap();
        assert_eq!(reader.row_count(), 4);

        // rows ordered by _timestamp DESC (docs 0..4 = ts 400,300,200,100)
        assert_eq!(
            read_i64(&reader, TIMESTAMP_COL_NAME),
            vec![400, 300, 200, 100]
        );

        // value terms + fts tokens
        assert_eq!(matching_docs(&reader, &exact("svc", "api")), vec![1, 2]);
        assert_eq!(
            matching_docs(
                &reader,
                &VixQuery::TokenAnyField {
                    token: b"error".to_vec()
                }
            ),
            vec![0, 1]
        );
        assert_eq!(
            matching_docs(
                &reader,
                &VixQuery::TokenAnyField {
                    token: b"timeout".to_vec()
                }
            ),
            vec![1]
        );
        // the fts field's whole values are not raw terms: per-field value
        // lookups on it do not resolve
        assert!(reader.eval(&exact("log", "error timeout db")).is_err());

        // key terms: every non-internal column with a value; none for the
        // internal `_timestamp`/`_o2_id`/`_original`
        assert_eq!(matching_docs(&reader, &key_exists("log")), vec![0, 1, 3]);
        assert_eq!(matching_docs(&reader, &key_exists("code")), vec![0, 1, 2]);
        assert_eq!(matching_docs(&reader, &key_exists("ok")), vec![0, 1, 3]);
        assert_eq!(
            matching_docs(&reader, &key_exists(TIMESTAMP_COL_NAME)),
            Vec::<u32>::new()
        );
        assert_eq!(
            matching_docs(&reader, &key_exists(ID_COL_NAME)),
            Vec::<u32>::new()
        );
        assert_eq!(
            matching_docs(&reader, &key_exists(ORIGINAL_DATA_COL_NAME)),
            Vec::<u32>::new()
        );

        // _source: reconstructs the record — includes _timestamp, excludes
        // _o2_id/_original, omits nulls, keeps native types
        let sources = reader.read_source(&[0, 1, 2, 3]).unwrap();
        let doc0: serde_json::Value = serde_json::from_str(sources.value(0)).unwrap();
        assert_eq!(
            doc0,
            serde_json::json!({
                "_timestamp": 400,
                "log": "error again",
                "svc": "db",
                "code": 503,
                "ok": false,
            })
        );
        let doc2: serde_json::Value = serde_json::from_str(sources.value(2)).unwrap();
        assert_eq!(
            doc2,
            serde_json::json!({"_timestamp": 200, "svc": "api", "code": 200})
        );

        // docs columns: configured cs field + the auto-preserved _o2_id
        assert_eq!(
            read_strings(&reader, "svc"),
            vec![
                Some("db".to_string()),
                Some("api".to_string()),
                Some("api".to_string()),
                Some("web".to_string())
            ]
        );
        assert_eq!(read_i64(&reader, ID_COL_NAME), vec![4, 1, 3, 2]);
        assert_eq!(
            read_strings(&reader, ORIGINAL_DATA_COL_NAME),
            vec![
                Some("raw-400".to_string()),
                Some("raw-300".to_string()),
                Some("raw-200".to_string()),
                None
            ]
        );
    }

    /// The move job globally sorts by `_timestamp` before the writer emits
    /// terms, so a shuffled multi-batch WAL yields a monotonic file whose
    /// postings still map to the right (sorted) rows, and the file carries a
    /// `_timestamp` zone table covering every row. This is the highest-risk
    /// property of the time-sorted build: term extraction must see the sorted
    /// batch, not the ingestion order.
    #[tokio::test]
    async fn move_job_sorted_build_zone_map_and_term_lookups() {
        let schema = wal_schema();
        // three batches, timestamps deliberately shuffled across batches;
        // `svc` cycles so each value lands on scattered ingestion rows
        let svc_values = ["api", "web", "db"];
        let mut batches = Vec::new();
        // interleave so no single batch is sorted and the runs overlap
        for chunk in [[37i64, 11, 29], [5, 41, 17], [23, 2, 33]] {
            let n = chunk.len();
            batches.push(
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int64Array::from(chunk.to_vec())) as ArrayRef,
                        Arc::new(StringArray::from(vec![Some("log line"); n])),
                        Arc::new(StringArray::from(
                            chunk
                                .iter()
                                .map(|&t| Some(svc_values[(t % 3) as usize]))
                                .collect::<Vec<_>>(),
                        )),
                        Arc::new(Int64Array::from(vec![Some(200i64); n])),
                        Arc::new(BooleanArray::from(vec![Some(true); n])),
                        Arc::new(Int64Array::from(chunk.to_vec())),
                        Arc::new(StringArray::from(vec![None::<&str>; n])),
                    ],
                )
                .unwrap(),
            );
        }
        let table = Arc::new(MemTable::try_new(schema.clone(), vec![batches]).unwrap());
        let result = write_core_file_from_tables(
            "test-sorted-build",
            schema,
            vec![table],
            &["log".to_string()],
            &["svc".to_string()],
            &[],
            false,
            0,
        )
        .await
        .unwrap();
        let reader = VixReader::open(bytes::Bytes::from(result.data)).unwrap();
        assert_eq!(reader.row_count(), 9);

        // (1) rows are sorted `_timestamp` DESC (non-increasing)
        let ts = read_i64(&reader, TIMESTAMP_COL_NAME);
        assert_eq!(ts, vec![41, 37, 33, 29, 23, 17, 11, 5, 2], "sorted DESC");

        // (2) the file carries a zone table covering every row, with bounds
        // equal to the stored min/max
        let chunks = reader
            .zone_chunks()
            .expect("move-job file carries a zone table");
        let total: u64 = chunks.iter().map(|c| c.row_count).sum();
        assert_eq!(total, reader.row_count());
        assert_eq!(chunks.first().unwrap().ts_max, 41);
        assert_eq!(chunks.last().unwrap().ts_min, 2);
        assert_eq!(
            (result.stats.min_ts, result.stats.max_ts),
            (2, 41),
            "stats range matches the sorted data"
        );

        // (3) term postings map to the SORTED rows: the docs the `svc="api"`
        // term matches are exactly the rows whose stored (sorted) `svc`
        // column is "api" — proving term emission saw the sorted batch
        let svc_sorted = read_strings(&reader, "svc");
        for value in svc_values {
            let expected: Vec<u32> = svc_sorted
                .iter()
                .enumerate()
                .filter(|(_, v)| v.as_deref() == Some(value))
                .map(|(i, _)| i as u32)
                .collect();
            assert_eq!(
                matching_docs(&reader, &exact("svc", value)),
                expected,
                "term postings for svc={value} must align with the sorted column"
            );
        }
    }

    /// Build one synthetic core file the way the move job does (column-driven
    /// with production `_source` synthesis).
    fn build_core_file(
        fields: Vec<Field>,
        columns: Vec<ArrayRef>,
        fts: &[String],
        cs: Vec<String>,
        original: Option<StringArray>,
    ) -> bytes::Bytes {
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let source = synthesize_source(&batch).unwrap();
        let mut writer = VixWriter::new(
            &schema,
            core_writer_options(fts, cs, Vec::new()),
            original.is_some(),
        );
        writer
            .push_batch_with_source(&batch, &source, original.as_ref())
            .unwrap();
        bytes::Bytes::from(writer.finish().unwrap())
    }

    /// [`build_core_file`] through the test-support UNGUARDED finish:
    /// fabricates a pre-guard-era stored file whose rows may carry
    /// `_timestamp <= 0` — the poison population compaction-time cleansing
    /// drops. Production writers refuse to build such files.
    fn build_poisoned_core_file(
        fields: Vec<Field>,
        columns: Vec<ArrayRef>,
        fts: &[String],
        cs: Vec<String>,
    ) -> bytes::Bytes {
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let source = synthesize_source(&batch).unwrap();
        let mut writer = VixWriter::new(&schema, core_writer_options(fts, cs, Vec::new()), false);
        writer
            .push_batch_with_source(&batch, &source, None)
            .unwrap();
        bytes::Bytes::from(
            vortex_index::test_support::finish_ignoring_timestamp_guard(writer).unwrap(),
        )
    }

    /// (b) The compactor k-way merge over 3 synthetic core files: global
    /// _timestamp DESC order with stable ties, schema union across inputs
    /// (missing cs columns -> nulls), _original preserved, terms re-derived
    /// from _source answering identically to a column-driven build of the
    /// same logical data (cross-layer extraction parity).
    #[tokio::test]
    async fn compactor_merges_core_files_by_timestamp() {
        let fts = vec!["log".to_string()];
        // file 1: log+svc, svc column-stored, carries _original; ts 100,50
        let file1 = build_core_file(
            vec![
                Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
                Field::new("log", DataType::Utf8, true),
                Field::new("svc", DataType::Utf8, true),
            ],
            vec![
                Arc::new(Int64Array::from(vec![100, 50])),
                Arc::new(StringArray::from(vec!["error one", "fine two"])),
                Arc::new(StringArray::from(vec!["api", "web"])),
            ],
            &fts,
            vec!["svc".to_string()],
            Some(StringArray::from(vec![Some("raw-100"), Some("raw-50")])),
        );
        // file 2: no svc at all, has `extra` field; ties with file1/file3 on
        // ts 50; ts 90,50
        let file2 = build_core_file(
            vec![
                Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
                Field::new("log", DataType::Utf8, true),
                Field::new("extra", DataType::Utf8, true),
            ],
            vec![
                Arc::new(Int64Array::from(vec![90, 50])),
                Arc::new(StringArray::from(vec![Some("error three"), None])),
                Arc::new(StringArray::from(vec![Some("x1"), Some("x2")])),
            ],
            &fts,
            vec![],
            None,
        );
        // file 3: svc + numeric code; ts 80,50,10
        let file3 = build_core_file(
            vec![
                Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
                Field::new("svc", DataType::Utf8, true),
                Field::new("code", DataType::Int64, true),
            ],
            vec![
                Arc::new(Int64Array::from(vec![80, 50, 10])),
                Arc::new(StringArray::from(vec!["db", "api", "db"])),
                Arc::new(Int64Array::from(vec![Some(500), Some(200), None])),
            ],
            &fts,
            vec!["svc".to_string()],
            None,
        );

        let latest_schema = Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("log", DataType::Utf8, true),
            Field::new("svc", DataType::Utf8, true),
            Field::new("extra", DataType::Utf8, true),
            Field::new("code", DataType::Int64, true),
        ]);
        let inputs = vec![
            ("f1.vix".to_string(), file1),
            ("f2.vix".to_string(), file2),
            ("f3.vix".to_string(), file3),
        ];
        let result = merge_core_files(
            &as_inputs(&inputs),
            &latest_schema,
            &fts,
            &["svc".to_string()],
            &[],
        )
        .unwrap();
        assert_eq!(result.stats.row_count, 7);
        assert!(result.stats.index_size > 0);
        // overlapping inputs still take the index-merge fast path (table
        // doc-id maps + row interleave)
        assert!(result.used_index_merge);

        let merged =
            VixReader::open(bytes::Bytes::from(result.output.to_bytes().unwrap())).unwrap();
        assert_eq!(merged.row_count(), 7);

        // global DESC order; the ts=50 tie resolves in input order
        // (file1 row, then file2 row, then file3 row)
        assert_eq!(
            read_i64(&merged, TIMESTAMP_COL_NAME),
            vec![100, 90, 80, 50, 50, 50, 10]
        );

        // the compactor (interleave path) re-derives a `_timestamp` zone
        // table over the merged rows, covering every row with the merged
        // min/max bounds
        let zone_chunks = merged
            .zone_chunks()
            .expect("merged file re-derives the zone table");
        assert_eq!(
            zone_chunks.iter().map(|c| c.row_count).sum::<u64>(),
            merged.row_count()
        );
        assert_eq!(zone_chunks.first().unwrap().ts_max, 100);
        assert_eq!(zone_chunks.last().unwrap().ts_min, 10);
        assert_eq!(
            read_strings(&merged, "svc"),
            vec![
                Some("api".to_string()), // file1 ts=100
                None,                    // file2 ts=90 (no svc column)
                Some("db".to_string()),  // file3 ts=80
                Some("web".to_string()), // file1 ts=50 (tie, first)
                None,                    // file2 ts=50 (tie, second)
                Some("api".to_string()), // file3 ts=50 (tie, third)
                Some("db".to_string()),  // file3 ts=10
            ]
        );
        assert_eq!(
            read_strings(&merged, ORIGINAL_DATA_COL_NAME),
            vec![
                Some("raw-100".to_string()),
                None,
                None,
                Some("raw-50".to_string()),
                None,
                None,
                None
            ]
        );

        // term answers on the merged file
        assert_eq!(matching_docs(&merged, &exact("svc", "api")), vec![0, 5]);
        assert_eq!(
            matching_docs(
                &merged,
                &VixQuery::TokenAnyField {
                    token: b"error".to_vec()
                }
            ),
            vec![0, 1]
        );
        assert_eq!(matching_docs(&merged, &key_exists("extra")), vec![1, 4]);
        assert_eq!(matching_docs(&merged, &key_exists("code")), vec![2, 5]);
        assert_eq!(matching_docs(&merged, &key_exists("log")), vec![0, 1, 3]);

        // cross-layer extraction parity: a column-driven build of the same
        // logical rows (already in merged order) answers identically
        let reference_schema = Arc::new(Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("log", DataType::Utf8, true),
            Field::new("svc", DataType::Utf8, true),
            Field::new("extra", DataType::Utf8, true),
            Field::new("code", DataType::Int64, true),
        ]));
        let reference_batch = RecordBatch::try_new(
            reference_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![100, 90, 80, 50, 50, 50, 10])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("error one"),
                    Some("error three"),
                    None,
                    Some("fine two"),
                    None,
                    None,
                    None,
                ])),
                Arc::new(StringArray::from(vec![
                    Some("api"),
                    None,
                    Some("db"),
                    Some("web"),
                    None,
                    Some("api"),
                    Some("db"),
                ])),
                Arc::new(StringArray::from(vec![
                    None,
                    Some("x1"),
                    None,
                    None,
                    Some("x2"),
                    None,
                    None,
                ])),
                Arc::new(Int64Array::from(vec![
                    None,
                    None,
                    Some(500),
                    None,
                    None,
                    Some(200),
                    None,
                ])),
            ],
        )
        .unwrap();
        let reference_source = synthesize_source(&reference_batch).unwrap();
        let mut reference_writer = VixWriter::new(
            &reference_schema,
            core_writer_options(&fts, vec!["svc".to_string()], Vec::new()),
            false,
        );
        reference_writer
            .push_batch_with_source(&reference_batch, &reference_source, None)
            .unwrap();
        let reference =
            VixReader::open(bytes::Bytes::from(reference_writer.finish().unwrap())).unwrap();

        let queries = [
            exact("svc", "api"),
            exact("svc", "db"),
            VixQuery::TokenAnyField {
                token: b"one".to_vec(),
            },
            exact("extra", "x2"),
            VixQuery::TokenAnyField {
                token: b"error".to_vec(),
            },
            VixQuery::Prefix {
                field: None,
                prefix: b"fin".to_vec(),
            },
            key_exists("log"),
            key_exists("svc"),
            key_exists("extra"),
            key_exists("code"),
        ];
        for query in &queries {
            assert_eq!(
                matching_docs(&merged, query),
                matching_docs(&reference, query),
                "merged vs column-driven answers diverge for {query:?}"
            );
        }
        assert_eq!(
            merged.keys_with_prefix("").unwrap(),
            reference.keys_with_prefix("").unwrap()
        );
        // the merged _source matches the reference synthesis row for row
        let rows: Vec<u64> = (0..7).collect();
        let merged_sources = merged.read_source(&rows).unwrap();
        for row in 0..7 {
            let merged_row: serde_json::Value =
                serde_json::from_str(merged_sources.value(row)).unwrap();
            let reference_row: serde_json::Value =
                serde_json::from_str(reference_source.value(row)).unwrap();
            assert_eq!(merged_row, reference_row, "row {row} _source diverges");
        }
    }

    #[tokio::test]
    async fn compactor_rejects_non_core_inputs() {
        let latest_schema =
            Schema::new(vec![Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false)]);
        let err = merge_core_files(
            &as_inputs(&[("bogus.vix".to_string(), bytes::Bytes::from_static(b"nope"))]),
            &latest_schema,
            &[],
            &[],
            &[],
        )
        .unwrap_err();
        assert!(err.to_string().contains("bogus.vix"), "{err}");

        let err = merge_core_files(&[], &latest_schema, &[], &[], &[]).unwrap_err();
        assert!(err.to_string().contains("no input files"), "{err}");
    }

    /// Full reader-visible equivalence of two core files: same rows in the
    /// same order (every docs column and `_source`), same term set with the
    /// same doc_counts and postings, same field capabilities and partials,
    /// and the same answers to a query battery.
    fn assert_core_files_equivalent(left: &VixReader, right: &VixReader, context: &str) {
        assert_eq!(left.row_count(), right.row_count(), "{context}: row_count");
        assert_eq!(
            left.term_count(),
            right.term_count(),
            "{context}: term_count"
        );

        // docs store: identical schema and identical column data
        let left_schema = left.docs_schema().unwrap();
        let right_schema = right.docs_schema().unwrap();
        assert_eq!(left_schema, right_schema, "{context}: docs schema");
        for field in left_schema.fields() {
            let name = field.name();
            let left_column = left.read_docs_column(name).unwrap();
            let right_column = right.read_docs_column(name).unwrap();
            assert_eq!(
                left_column.to_data(),
                right_column.to_data(),
                "{context}: docs column {name:?}"
            );
        }

        // the whole term table: keys, doc_counts, decoded postings
        let dump = |reader: &VixReader| {
            let mut terms: Vec<(Vec<u8>, u64, Vec<u32>)> = Vec::new();
            reader
                .for_each_term(&mut |key, doc_count, ids| {
                    terms.push((key.to_vec(), doc_count, ids.to_vec()));
                    Ok(())
                })
                .unwrap();
            terms
        };
        assert_eq!(dump(left), dump(right), "{context}: term table");

        // properties-level metadata
        assert_eq!(
            left.partial_fields(),
            right.partial_fields(),
            "{context}: partial_fields"
        );
        assert_eq!(
            left.term_field_names(),
            right.term_field_names(),
            "{context}: term fields"
        );
        for name in left.term_field_names() {
            assert_eq!(
                left.has_term_capability(name),
                right.has_term_capability(name),
                "{context}: term capability of {name:?}"
            );
            assert_eq!(
                left.field_value_counts(name).unwrap(),
                right.field_value_counts(name).unwrap(),
                "{context}: value counts of {name:?}"
            );
        }
        assert_eq!(
            left.keys_with_prefix("").unwrap(),
            right.keys_with_prefix("").unwrap(),
            "{context}: key coverage"
        );

        // a query battery over every term field + composites
        let mut queries: Vec<VixQuery> = vec![
            VixQuery::All,
            VixQuery::TokenAnyField {
                token: b"error".to_vec(),
            },
            VixQuery::TokenAnyField {
                token: b"timeout".to_vec(),
            },
            VixQuery::Prefix {
                field: None,
                prefix: b"pro".to_vec(),
            },
            VixQuery::Contains {
                field: None,
                needle: b"rr".to_vec(),
                case_insensitive: true,
            },
            VixQuery::Fuzzy {
                token: "eror".to_string(),
                distance: 1,
            },
        ];
        for name in left.term_field_names() {
            queries.push(key_exists(name));
            if left.has_term_capability(name) {
                queries.push(VixQuery::Prefix {
                    field: Some(name.to_string()),
                    prefix: b"a".to_vec(),
                });
                queries.push(VixQuery::Regex {
                    field: Some(name.to_string()),
                    pattern: ".*o.*".to_string(),
                });
            }
        }
        queries.push(VixQuery::And(vec![
            VixQuery::TokenAnyField {
                token: b"error".to_vec(),
            },
            VixQuery::Not(Box::new(key_exists("code"))),
        ]));
        if left.has_term_capability("env") {
            queries.push(VixQuery::Or(vec![
                exact("env", "prod"),
                VixQuery::TokenAnyField {
                    token: b"disk".to_vec(),
                },
            ]));
        }
        for query in &queries {
            assert_eq!(
                matching_docs(left, query),
                matching_docs(right, query),
                "{context}: query {query:?}"
            );
            assert_eq!(
                left.count(query).unwrap(),
                right.count(query).unwrap(),
                "{context}: count {query:?}"
            );
        }
        for range in [(0i64, i64::MAX), (250, 850), (700, 701)] {
            assert_eq!(
                left.timestamp_range(range.0, range.1).unwrap(),
                right.timestamp_range(range.0, range.1).unwrap(),
                "{context}: ts range {range:?}"
            );
        }
    }

    /// The differential-test corpus: three feature-dense core files —
    /// fts tokens (shared across files), key terms, a value dense across
    /// every row (`env=prod`), string + numeric cs columns, `_o2_id`,
    /// `_original` (one file), empty strings (fts AND structured — the
    /// empty raw term), non-finite floats (NaN/±Inf: key-term-less but
    /// cs-stored), a NUL-byte value, an oversize value (partial field), and
    /// a field only one file knows.
    ///
    /// `ts` supplies each file's descending timestamp column, so callers
    /// choose disjoint or overlapping ranges.
    fn differential_inputs(ts: [Vec<i64>; 3]) -> Vec<(String, bytes::Bytes)> {
        use arrow::array::Float64Array;
        let fts = vec!["log".to_string()];
        let cs = vec!["svc".to_string(), "code".to_string()];
        let oversize = "z".repeat(70_000);
        let file1 = build_core_file(
            vec![
                Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
                Field::new("log", DataType::Utf8, true),
                Field::new("level", DataType::Utf8, true),
                Field::new("svc", DataType::Utf8, true),
                Field::new("env", DataType::Utf8, false),
                Field::new("huge", DataType::Utf8, true),
                Field::new("code", DataType::Int64, true),
                Field::new("ratio", DataType::Float64, true),
                Field::new(ID_COL_NAME, DataType::Int64, true),
            ],
            vec![
                Arc::new(Int64Array::from(ts[0].clone())),
                Arc::new(StringArray::from(vec![
                    Some("Error connecting to db"),
                    Some(""),
                    Some("disk almost full"),
                ])),
                Arc::new(StringArray::from(vec![Some("info"), Some("a\x00b"), None])),
                Arc::new(StringArray::from(vec![Some("api"), Some("api"), None])),
                Arc::new(StringArray::from(vec!["prod", "prod", "prod"])),
                Arc::new(StringArray::from(vec![
                    Some(oversize.as_str()),
                    None,
                    Some("small"),
                ])),
                Arc::new(Int64Array::from(vec![Some(500), None, Some(200)])),
                // NaN: key-term-less (treated as null; `_source` says null)
                Arc::new(Float64Array::from(vec![Some(f64::NAN), Some(1.5), None])),
                Arc::new(Int64Array::from(vec![Some(11), Some(12), Some(13)])),
            ],
            &fts,
            cs.clone(),
            Some(StringArray::from(vec![Some("raw-a"), None, Some("raw-c")])),
        );
        let file2 = build_core_file(
            vec![
                Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
                Field::new("log", DataType::Utf8, true),
                Field::new("level", DataType::Utf8, true),
                Field::new("extra", DataType::Utf8, true),
                Field::new("env", DataType::Utf8, false),
                Field::new("ratio", DataType::Float64, true),
                // svc + code carried as PLAIN fields (values in `_source`,
                // no docs column) even though file1/file3 column-store them:
                // file2 predates the `column_store_fields` setting. The merge
                // must derive these columns from `_source`, not null-fill.
                Field::new("svc", DataType::Utf8, true),
                Field::new("code", DataType::Int64, true),
            ],
            vec![
                Arc::new(Int64Array::from(ts[1].clone())),
                Arc::new(StringArray::from(vec![
                    Some("timeout waiting for error"),
                    None,
                ])),
                Arc::new(StringArray::from(vec![Some("error"), Some("info")])),
                // one empty structured value: the empty raw term must
                // survive both merge strategies identically
                Arc::new(StringArray::from(vec![Some("x1"), Some("")])),
                Arc::new(StringArray::from(vec!["prod", "prod"])),
                // ±Inf: key-term-less like NaN
                Arc::new(Float64Array::from(vec![
                    Some(f64::INFINITY),
                    Some(f64::NEG_INFINITY),
                ])),
                // pre-column svc/code values (recovered only from `_source`);
                // a negative int exercises the json_get_int path
                Arc::new(StringArray::from(vec![Some("cache"), Some("queue")])),
                Arc::new(Int64Array::from(vec![Some(-7), None])),
            ],
            &fts,
            vec![],
            None,
        );
        let file3 = build_core_file(
            vec![
                Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
                Field::new("log", DataType::Utf8, true),
                Field::new("svc", DataType::Utf8, true),
                Field::new("env", DataType::Utf8, false),
                Field::new("code", DataType::Int64, true),
                Field::new(ID_COL_NAME, DataType::Int64, true),
            ],
            vec![
                Arc::new(Int64Array::from(ts[2].clone())),
                Arc::new(StringArray::from(vec![
                    Some("error error error"),
                    Some("all good"),
                    None,
                ])),
                Arc::new(StringArray::from(vec![Some("db"), Some("api"), Some("db")])),
                Arc::new(StringArray::from(vec!["prod", "prod", "prod"])),
                Arc::new(Int64Array::from(vec![None, Some(503), Some(200)])),
                Arc::new(Int64Array::from(vec![Some(31), Some(32), Some(33)])),
            ],
            &fts,
            vec!["svc".to_string()],
            None,
        );
        vec![
            ("f2.vix".to_string(), file2),
            ("f1.vix".to_string(), file1),
            ("f3.vix".to_string(), file3),
        ]
    }

    fn differential_latest_schema() -> Schema {
        Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("log", DataType::Utf8, true),
            Field::new("level", DataType::Utf8, true),
            Field::new("svc", DataType::Utf8, true),
            Field::new("env", DataType::Utf8, true),
            Field::new("extra", DataType::Utf8, true),
            Field::new("huge", DataType::Utf8, true),
            Field::new("code", DataType::Int64, true),
            Field::new("ratio", DataType::Float64, true),
        ])
    }

    /// THE differential gate: the index-merge fast path must be
    /// reader-equivalent to the full rebuild — for disjoint inputs (offset
    /// maps + docs bulk copy, including a contiguity-preserving boundary
    /// tie) and for overlapping inputs (table maps + interleave, including
    /// cross-file ties).
    #[tokio::test]
    async fn differential_index_merge_matches_rebuild() {
        let fts = vec!["log".to_string()];
        let cs = vec!["svc".to_string(), "code".to_string()];
        let latest_schema = differential_latest_schema();

        // input order is f2, f1, f3 (see differential_inputs): f1 is the
        // newest file, and f2's oldest row ties f3's newest (contiguity
        // holds because f2 has the smaller input index)
        let disjoint = differential_inputs([
            vec![1000, 950, 900], // f1 (input index 1)
            vec![800, 700],       // f2 (input index 0)
            vec![700, 600, 500],  // f3 (input index 2): boundary tie at 700
        ]);
        // genuinely interleaved, with a three-way tie at 600
        let overlapping =
            differential_inputs([vec![1000, 600, 500], vec![900, 600], vec![950, 600, 400]]);

        for (context, inputs) in [("disjoint", disjoint), ("overlapping", overlapping)] {
            let fast =
                merge_core_files(&as_inputs(&inputs), &latest_schema, &fts, &cs, &[]).unwrap();
            assert!(fast.used_index_merge, "{context}: expected the fast path");
            let rebuild =
                merge_core_files_rebuild(&as_inputs(&inputs), &latest_schema, &fts, &cs, &[])
                    .unwrap();
            assert!(!rebuild.used_index_merge);

            assert_eq!(fast.stats.row_count, rebuild.stats.row_count, "{context}");
            assert_eq!(fast.stats.term_count, rebuild.stats.term_count, "{context}");

            let fast_reader =
                VixReader::open(bytes::Bytes::from(fast.output.to_bytes().unwrap())).unwrap();
            let rebuild_reader =
                VixReader::open(bytes::Bytes::from(rebuild.output.to_bytes().unwrap())).unwrap();
            assert_core_files_equivalent(&fast_reader, &rebuild_reader, context);

            // spot-check the merged features on the fast-path file itself
            assert!(fast_reader.partial_fields().contains("huge"), "{context}");
            assert_eq!(
                fast_reader.count(&exact("env", "prod")).unwrap(),
                fast_reader.row_count(),
                "{context}: dense value term"
            );
            assert!(
                !matching_docs(
                    &fast_reader,
                    &VixQuery::TokenAnyField {
                        token: b"error".to_vec()
                    }
                )
                .is_empty(),
                "{context}: fts tokens present"
            );
            // the empty structured value survived the merge as an exact term
            assert_eq!(
                fast_reader.count(&exact("extra", "")).unwrap(),
                1,
                "{context}: empty-string raw term"
            );
            // non-finite floats (NaN in f1, ±Inf in f2) never key the doc:
            // only the single finite ratio row answers IS NOT NULL
            assert_eq!(
                fast_reader.count(&key_exists("ratio")).unwrap(),
                1,
                "{context}: non-finite ratio rows are key-term-less"
            );
            // Phase C1 regression: f2 predates svc/code as column-store fields
            // (values live only in its `_source`). The merge must DERIVE those
            // columns, so the docs column a scan reads is consistent with the
            // key terms — NOT null-filled. assert_core_files_equivalent above
            // already proves fast == rebuild; this proves they agree on the
            // CORRECT value: docs-column non-null == IS NOT NULL (key terms).
            for field in ["svc", "code"] {
                let column = fast_reader.read_docs_column(field).unwrap();
                let docs_non_null = (column.len() - column.null_count()) as u64;
                assert_eq!(
                    docs_non_null,
                    fast_reader.count(&key_exists(field)).unwrap(),
                    "{context}: {field:?} docs-column non-null must equal IS NOT NULL \
                     (derived from _source, not null-filled)"
                );
            }
        }
    }

    /// THE chunked-flow gate (live "byte array offset overflow" regression):
    /// every merge strategy must stage docs in MULTIPLE bounded batches when
    /// the caps demand it, and the bounded flow must stay reader-equivalent
    /// to the default (single-window) flow. Tiny caps force windows/splits on
    /// small data; the differential corpus makes cs derivation (a pre-column
    /// input whose svc/code live only in `_source`) and value/numeric term
    /// re-derivation cross the batch boundaries.
    #[tokio::test]
    async fn merge_bounded_batches_match_default_caps() {
        let fts = vec!["log".to_string()];
        let cs = vec!["svc".to_string(), "code".to_string()];
        let latest_schema = differential_latest_schema();
        let tiny = BatchCaps { rows: 2, bytes: 96 };

        // overlapping ranges: the rebuild and the fast-path row interleave
        // both go through stream_merge_windows
        let overlapping =
            differential_inputs([vec![1000, 600, 500], vec![900, 600], vec![950, 600, 400]]);
        let rebuild_default =
            merge_core_files_rebuild(&as_inputs(&overlapping), &latest_schema, &fts, &cs, &[])
                .unwrap();
        assert_eq!(
            rebuild_default.docs_batches, 1,
            "8 rows fit one default-caps window"
        );
        let rebuild_tiny = merge_core_files_rebuild_with_caps(
            &as_inputs(&overlapping),
            &latest_schema,
            &fts,
            &cs,
            &[],
            tiny,
        )
        .unwrap();
        assert!(
            rebuild_tiny.docs_batches >= 4,
            "tiny caps must stage the rebuild in multiple bounded windows, got {}",
            rebuild_tiny.docs_batches
        );
        let default_reader = VixReader::open(bytes::Bytes::from(
            rebuild_default.output.to_bytes().unwrap(),
        ))
        .unwrap();
        let tiny_reader =
            VixReader::open(bytes::Bytes::from(rebuild_tiny.output.to_bytes().unwrap())).unwrap();
        assert_core_files_equivalent(&tiny_reader, &default_reader, "rebuild tiny-vs-default");

        let fast_tiny = merge_core_files_with_caps(
            &as_inputs(&overlapping),
            &latest_schema,
            &fts,
            &cs,
            &[],
            tiny,
        )
        .unwrap();
        assert!(fast_tiny.used_index_merge, "overlapping fast path expected");
        assert!(
            fast_tiny.docs_batches >= 4,
            "tiny caps must window the fast-path interleave too, got {}",
            fast_tiny.docs_batches
        );
        let fast_tiny_reader =
            VixReader::open(bytes::Bytes::from(fast_tiny.output.to_bytes().unwrap())).unwrap();
        assert_core_files_equivalent(&fast_tiny_reader, &default_reader, "fast tiny-vs-rebuild");

        // the bounded output keeps the merge invariants: global DESC order
        // and a zone table covering every row
        let ts = read_i64(&tiny_reader, TIMESTAMP_COL_NAME);
        assert!(ts.windows(2).all(|pair| pair[0] >= pair[1]), "global DESC");
        let zone = tiny_reader.zone_chunks().expect("merged zone table");
        assert_eq!(
            zone.iter().map(|chunk| chunk.row_count).sum::<u64>(),
            tiny_reader.row_count()
        );

        // disjoint ranges: the sequential stream path splits every input's
        // chunks by the caps as well
        let disjoint =
            differential_inputs([vec![1000, 950, 900], vec![800, 700], vec![700, 600, 500]]);
        let disjoint_default =
            merge_core_files(&as_inputs(&disjoint), &latest_schema, &fts, &cs, &[]).unwrap();
        assert!(disjoint_default.used_index_merge);
        let disjoint_tiny =
            merge_core_files_with_caps(&as_inputs(&disjoint), &latest_schema, &fts, &cs, &[], tiny)
                .unwrap();
        assert!(disjoint_tiny.used_index_merge);
        assert!(
            disjoint_tiny.docs_batches >= 4
                && disjoint_tiny.docs_batches > disjoint_default.docs_batches,
            "tiny caps must split the disjoint stream copy: {} vs {}",
            disjoint_tiny.docs_batches,
            disjoint_default.docs_batches
        );
        let disjoint_default_reader = VixReader::open(bytes::Bytes::from(
            disjoint_default.output.to_bytes().unwrap(),
        ))
        .unwrap();
        let disjoint_tiny_reader =
            VixReader::open(bytes::Bytes::from(disjoint_tiny.output.to_bytes().unwrap())).unwrap();
        assert_core_files_equivalent(
            &disjoint_tiny_reader,
            &disjoint_default_reader,
            "disjoint tiny-vs-default",
        );
    }

    /// The move job splits wide batches by BYTES before `_source` synthesis
    /// (one JSON string per row lands in an arrow array): a tiny byte cap
    /// must stage one bounded batch per row and still produce a file
    /// equivalent to the default-caps build.
    #[tokio::test]
    async fn move_job_bounded_batches_match_default_caps() {
        let schema = wal_schema();
        let table =
            Arc::new(MemTable::try_new(schema.clone(), vec![wal_batches(&schema)]).unwrap());
        let fts = vec!["log".to_string()];
        let cs = vec!["svc".to_string()];
        let default = write_core_file_from_tables(
            "test-move-caps",
            schema.clone(),
            vec![table.clone()],
            &fts,
            &cs,
            &[],
            false,
            0,
        )
        .await
        .unwrap();
        assert!(
            default.docs_batches <= 2,
            "4 sorted rows arrive in at most the stream's batches, got {}",
            default.docs_batches
        );
        let tiny = write_core_file_from_tables_with_caps(
            "test-move-caps",
            schema,
            vec![table],
            &fts,
            &cs,
            &[],
            false,
            0,
            BatchCaps { rows: 1, bytes: 1 },
        )
        .await
        .unwrap();
        assert_eq!(
            tiny.docs_batches, 4,
            "the tiny caps stage one batch per row"
        );
        let default_reader = VixReader::open(bytes::Bytes::from(default.data)).unwrap();
        let tiny_reader = VixReader::open(bytes::Bytes::from(tiny.data)).unwrap();
        assert_core_files_equivalent(&tiny_reader, &default_reader, "move-job tiny-vs-default");
    }

    /// LIVE-SHAPE regression (image .8 zero-ts merge outputs): an event-time
    /// BACKFILL partition holds DOZENS of tiny files (1-5 rows each, stale
    /// timestamps spread inside one hour). The merged output's stats — the
    /// authoritative `FileMeta::min_ts`/`max_ts` source — must equal the
    /// actual `_timestamp` range of the merged rows on EVERY strategy
    /// (fast/rebuild x overlapping/disjoint), and a poisoned folded meta
    /// (min_ts = 0 from a degenerate input row) must heal through
    /// `apply_core_stats_to_meta`.
    #[tokio::test]
    async fn merge_many_tiny_inputs_meta_range_matches_data() {
        let fts = vec!["log".to_string()];
        let cs = vec!["svc".to_string()];
        // 2026-07-24T10:00:00Z in micros — the live partition's hour
        const HOUR: i64 = 1_784_887_200_000_000;
        let latest_schema = Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("log", DataType::Utf8, true),
            Field::new("svc", DataType::Utf8, true),
            Field::new("code", DataType::Int64, true),
        ]);

        // one tiny move-job-shaped file: rows sorted DESC like real outputs
        let tiny_file = |ts: Vec<i64>, with_svc_column: bool| {
            let rows = ts.len();
            let mut fields = vec![
                Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
                Field::new("log", DataType::Utf8, true),
                Field::new("code", DataType::Int64, true),
            ];
            let mut columns: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from(ts)),
                Arc::new(StringArray::from(vec!["backfill error line"; rows])),
                Arc::new(Int64Array::from(vec![207; rows])),
            ];
            let mut file_cs = vec![];
            if with_svc_column {
                fields.push(Field::new("svc", DataType::Utf8, true));
                columns.push(Arc::new(StringArray::from(vec!["batch"; rows])));
                file_cs.push("svc".to_string());
            }
            build_core_file(fields, columns, &fts, file_cs, None)
        };

        // overlapping: input i's rows interleave across the whole hour
        // (stale-timestamp apps flush independently); disjoint: input i owns
        // one strictly older slice than input i-1
        let mut overlapping = Vec::new();
        let mut disjoint = Vec::new();
        let mut expected: Vec<i64> = Vec::new();
        let inputs_total = 40usize;
        for i in 0..inputs_total {
            let rows = 1 + (i % 5);
            let over_ts: Vec<i64> = (0..rows)
                .map(|j| HOUR + (((rows - 1 - j) * 601 + i * 17) % 3600) as i64 * 1_000_000)
                .collect();
            expected.extend(&over_ts);
            let dis_ts: Vec<i64> = (0..rows)
                .map(|j| HOUR + ((inputs_total - i) * 60 - j) as i64 * 1_000_000)
                .collect();
            overlapping.push((format!("over{i}.vix"), tiny_file(over_ts, i % 3 != 2)));
            disjoint.push((format!("dis{i}.vix"), tiny_file(dis_ts, i % 3 != 2)));
        }
        let expected_rows = expected.len() as u64;
        let (expected_min, expected_max) = (
            *expected.iter().min().unwrap(),
            *expected.iter().max().unwrap(),
        );

        for (context, inputs) in [("overlapping", &overlapping), ("disjoint", &disjoint)] {
            for (strategy, result) in [
                (
                    "fast",
                    merge_core_files(&as_inputs(inputs), &latest_schema, &fts, &cs, &[]).unwrap(),
                ),
                (
                    "rebuild",
                    merge_core_files_rebuild(&as_inputs(inputs), &latest_schema, &fts, &cs, &[])
                        .unwrap(),
                ),
            ] {
                assert_eq!(
                    result.stats.row_count, expected_rows,
                    "{context}/{strategy}: row count"
                );
                let reader =
                    VixReader::open(bytes::Bytes::from(result.output.to_bytes().unwrap())).unwrap();
                let stored = read_i64(&reader, TIMESTAMP_COL_NAME);
                let data_min = *stored.iter().min().unwrap();
                let data_max = *stored.iter().max().unwrap();
                assert_eq!(
                    (data_min, data_max),
                    if context == "overlapping" {
                        (expected_min, expected_max)
                    } else {
                        (data_min, data_max)
                    },
                    "{context}/{strategy}: stored data range sanity"
                );
                assert_eq!(
                    (result.stats.min_ts, result.stats.max_ts),
                    (data_min, data_max),
                    "{context}/{strategy}: stats must equal the stored data range"
                );
                // zone table consistent: covers every row within the range
                let zone = reader.zone_chunks().expect("merged zone table");
                assert_eq!(
                    zone.iter().map(|chunk| chunk.row_count).sum::<u64>(),
                    expected_rows,
                    "{context}/{strategy}: zone rows"
                );
                assert!(
                    zone.iter()
                        .all(|chunk| chunk.ts_min >= data_min && chunk.ts_max <= data_max),
                    "{context}/{strategy}: zone bounds inside the data range"
                );

                // the compactor fold with a poisoned input row (min_ts = 0,
                // the 829-row live shape) must heal from the stats
                let mut meta = FileMeta {
                    min_ts: 0,
                    max_ts: expected_max,
                    records: expected_rows as i64,
                    original_size: 1,
                    compressed_size: 0,
                    flattened: false,
                    index_size: 0,
                    bloom_ver: 0,
                };
                apply_core_stats_to_meta(&mut meta, 1, &result.stats, context).unwrap();
                assert_eq!(
                    (meta.min_ts, meta.max_ts),
                    (data_min, data_max),
                    "{context}/{strategy}: healed meta"
                );
            }
        }
    }

    /// The compaction-time cleansing corpus: three pre-guard-era files with
    /// `_timestamp <= 0` rows spread ACROSS inputs and positions (interior,
    /// trailing), plus their CLEAN TWINS holding exactly the healthy rows.
    /// Poison rows carry the same field set as healthy rows plus a marker
    /// token (`zeroline`) that must vanish from the merged terms, and f2
    /// carries svc/code as PLAIN fields (pre-`column_store_fields` shape) so
    /// cs derivation from `_source` crosses the cleansing too.
    ///
    /// Returns `(poisoned_inputs, clean_inputs, poison_count)`.
    #[allow(clippy::type_complexity)]
    fn cleansing_inputs(
        disjoint: bool,
    ) -> (
        Vec<(String, bytes::Bytes)>,
        Vec<(String, bytes::Bytes)>,
        u64,
    ) {
        let fts = vec!["log".to_string()];
        let file = |name: &str, ts: Vec<i64>, svc_column_stored: bool, poisoned: bool| {
            let logs: Vec<String> = ts
                .iter()
                .enumerate()
                .map(|(row, &t)| {
                    if t <= 0 {
                        format!("zeroline poison row {row}")
                    } else {
                        format!("error healthy line {t}")
                    }
                })
                .collect();
            let svc: Vec<&str> = ts
                .iter()
                .map(|&t| if t % 2 == 0 { "api" } else { "db" })
                .collect();
            // every per-row value is keyed to the row's TIMESTAMP so the
            // clean twins' surviving rows carry identical values
            let codes: Vec<Option<i64>> = ts.iter().map(|&t| Some(200 + t.rem_euclid(7))).collect();
            let fields = vec![
                Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
                Field::new("log", DataType::Utf8, true),
                Field::new("svc", DataType::Utf8, true),
                Field::new("code", DataType::Int64, true),
            ];
            let columns: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from(ts)),
                Arc::new(StringArray::from(
                    logs.iter().map(String::as_str).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(svc)),
                Arc::new(Int64Array::from(codes)),
            ];
            let file_cs = if svc_column_stored {
                vec!["svc".to_string(), "code".to_string()]
            } else {
                vec![]
            };
            let data = if poisoned {
                build_poisoned_core_file(fields, columns, &fts, file_cs)
            } else {
                build_core_file(fields, columns, &fts, file_cs, None)
            };
            (name.to_string(), data)
        };

        // f1: interior poison x2; f2: trailing poison, svc/code PLAIN
        // (pre-column, derivation from `_source`); f3: all healthy.
        // `disjoint` shifts the healthy ranges apart so the (clean) inputs
        // would take the contiguous-offsets fast path.
        let (f1_ts, f2_ts, f3_ts) = if disjoint {
            (
                vec![1000, 0, 900, -5, 800],
                vec![750, 720, 0],
                vec![700, 600],
            )
        } else {
            (
                vec![1000, 0, 900, -5, 800],
                vec![950, 850, 0],
                vec![700, 600],
            )
        };
        let poisoned = vec![
            file("f1.vix", f1_ts.clone(), true, true),
            file("f2.vix", f2_ts.clone(), false, true),
            file("f3.vix", f3_ts.clone(), true, false),
        ];
        let healthy = |ts: Vec<i64>| ts.into_iter().filter(|t| *t > 0).collect::<Vec<i64>>();
        let clean = vec![
            file("c1.vix", healthy(f1_ts.clone()), true, false),
            file("c2.vix", healthy(f2_ts.clone()), false, false),
            file("c3.vix", healthy(f3_ts), true, false),
        ];
        let poison_count = f1_ts.iter().chain(&f2_ts).filter(|t| **t <= 0).count() as u64;
        (poisoned, clean, poison_count)
    }

    fn cleansing_latest_schema() -> Schema {
        Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("log", DataType::Utf8, true),
            Field::new("svc", DataType::Utf8, true),
            Field::new("code", DataType::Int64, true),
        ])
    }

    /// COMPACTION-TIME CLEANSING (the pre-guard poison population): merging
    /// stored files whose data carries `_timestamp <= 0` rows behind
    /// healthy-looking metadata must DROP those rows — the output contains
    /// exactly the healthy rows and is reader-EQUIVALENT to a merge of clean
    /// twins (terms, cs columns, zone table, stats all consistent with the
    /// cleansed row set); the dropped counter equals the poison count; the
    /// index-merge fast path refuses poison (falls back to the rebuild,
    /// whose finish guard then passes by construction). Tiny caps prove the
    /// drops across chunk boundaries.
    #[tokio::test]
    async fn merge_cleanses_zero_timestamp_rows() {
        let fts = vec!["log".to_string()];
        let cs = vec!["svc".to_string(), "code".to_string()];
        let latest_schema = cleansing_latest_schema();

        for (context, disjoint) in [("overlapping", false), ("disjoint", true)] {
            let (poisoned, clean, poison_count) = cleansing_inputs(disjoint);
            assert_eq!(poison_count, 3, "{context}: corpus sanity");

            // the clean twins take the index-merge fast path (sanity that
            // the poison check does not over-block healthy merges)...
            let clean_fast =
                merge_core_files(&as_inputs(&clean), &latest_schema, &fts, &cs, &[]).unwrap();
            assert!(clean_fast.used_index_merge, "{context}: clean fast path");
            assert_eq!(clean_fast.dropped_rows, 0, "{context}");
            // ... and the referee is the clean rebuild (the oracle)
            let referee =
                merge_core_files_rebuild(&as_inputs(&clean), &latest_schema, &fts, &cs, &[])
                    .unwrap();
            assert_eq!(referee.dropped_rows, 0, "{context}");
            let referee_reader =
                VixReader::open(bytes::Bytes::from(referee.output.to_bytes().unwrap())).unwrap();

            // the poisoned merge: fast path refuses -> rebuild cleanses
            let merged =
                merge_core_files(&as_inputs(&poisoned), &latest_schema, &fts, &cs, &[]).unwrap();
            assert!(
                !merged.used_index_merge,
                "{context}: poison must force the rebuild path"
            );
            assert_eq!(
                merged.dropped_rows, poison_count,
                "{context}: dropped counter"
            );
            assert_eq!(
                merged.stats.row_count, referee.stats.row_count,
                "{context}: healthy rows only"
            );
            assert_eq!(
                (merged.stats.min_ts, merged.stats.max_ts),
                (referee.stats.min_ts, referee.stats.max_ts),
                "{context}: stats range == healthy range"
            );
            let merged_reader =
                VixReader::open(bytes::Bytes::from(merged.output.to_bytes().unwrap())).unwrap();
            assert_core_files_equivalent(
                &merged_reader,
                &referee_reader,
                &format!("{context}: cleansed-vs-clean"),
            );

            // direct assertions on the cleansed output
            let ts = read_i64(&merged_reader, TIMESTAMP_COL_NAME);
            assert!(ts.iter().all(|t| *t > 0), "{context}: no degenerate ts");
            assert!(
                ts.windows(2).all(|pair| pair[0] >= pair[1]),
                "{context}: global DESC"
            );
            assert_eq!(
                merged_reader
                    .count(&VixQuery::TokenAnyField {
                        token: b"zeroline".to_vec(),
                    })
                    .unwrap(),
                0,
                "{context}: poison-row tokens gone from the terms"
            );
            let zone = merged_reader.zone_chunks().expect("merged zone table");
            assert_eq!(
                zone.iter().map(|chunk| chunk.row_count).sum::<u64>(),
                merged_reader.row_count(),
                "{context}: zone covers the cleansed rows"
            );
            assert!(
                zone.iter().all(|chunk| chunk.ts_min > 0),
                "{context}: zone bounds inside the cleansed range"
            );

            // tiny caps: poison rows cross chunk boundaries and still drop
            let tiny = BatchCaps { rows: 2, bytes: 96 };
            let bounded = merge_core_files_rebuild_with_caps(
                &as_inputs(&poisoned),
                &latest_schema,
                &fts,
                &cs,
                &[],
                tiny,
            )
            .unwrap();
            assert_eq!(
                bounded.dropped_rows, poison_count,
                "{context}: bounded dropped counter"
            );
            assert!(
                bounded.docs_batches >= 3,
                "{context}: tiny caps must window the cleansed rebuild, got {}",
                bounded.docs_batches
            );
            let bounded_reader =
                VixReader::open(bytes::Bytes::from(bounded.output.to_bytes().unwrap())).unwrap();
            assert_core_files_equivalent(
                &bounded_reader,
                &referee_reader,
                &format!("{context}: bounded cleansed-vs-clean"),
            );
        }
    }

    /// ALL-POISON input set (the empty-output edge): every row of every
    /// input carries `_timestamp <= 0` — the merge succeeds with a ZERO-ROW
    /// result (`dropped_rows` = the full input row count, stats/range empty,
    /// the bytes a valid empty `.vix`). The compactor caller
    /// (`merge_core_group`) turns this into "inputs deleted, no output
    /// file": the commit flow supports delete-only batches (empty
    /// `new_files` keys are filtered before the events build; the file_list
    /// VALUES builder skips an empty add set), so the poison files leave
    /// file_list for GC without publishing a useless empty object.
    #[tokio::test]
    async fn merge_all_poison_inputs_yield_empty_output() {
        let fts = vec!["log".to_string()];
        let cs = vec!["svc".to_string()];
        let latest_schema = cleansing_latest_schema();
        let fields = || {
            vec![
                Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
                Field::new("log", DataType::Utf8, true),
                Field::new("svc", DataType::Utf8, true),
                Field::new("code", DataType::Int64, true),
            ]
        };
        let p1 = build_poisoned_core_file(
            fields(),
            vec![
                Arc::new(Int64Array::from(vec![0, 0])),
                Arc::new(StringArray::from(vec!["zeroline a", "zeroline b"])),
                Arc::new(StringArray::from(vec!["api", "db"])),
                Arc::new(Int64Array::from(vec![Some(1), Some(2)])),
            ],
            &fts,
            vec!["svc".to_string(), "code".to_string()],
        );
        let p2 = build_poisoned_core_file(
            fields(),
            vec![
                Arc::new(Int64Array::from(vec![-3])),
                Arc::new(StringArray::from(vec!["zeroline c"])),
                Arc::new(StringArray::from(vec!["api"])),
                Arc::new(Int64Array::from(vec![Some(3)])),
            ],
            &fts,
            vec![],
        );
        // one legitimately EMPTY file rides along: zero rows, zero dropped
        let empty = build_core_file(
            fields(),
            vec![
                Arc::new(Int64Array::from(Vec::<i64>::new())),
                Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
                Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
                Arc::new(Int64Array::from(Vec::<Option<i64>>::new())),
            ],
            &fts,
            vec!["svc".to_string()],
            None,
        );
        let inputs = vec![
            ("p1.vix".to_string(), p1),
            ("empty.vix".to_string(), empty),
            ("p2.vix".to_string(), p2),
        ];

        let result = merge_core_files(&as_inputs(&inputs), &latest_schema, &fts, &cs, &[]).unwrap();
        assert!(!result.used_index_merge, "poison must force the rebuild");
        assert_eq!(result.dropped_rows, 3, "every data row was poison");
        assert_eq!(result.stats.row_count, 0, "nothing survives");
        assert_eq!(
            (result.stats.min_ts, result.stats.max_ts),
            (0, 0),
            "an empty file keeps the legit (0, 0) range"
        );
        // the bytes are still a valid (empty) core file — callers that DO
        // want to keep it could; merge_core_group deliberately commits
        // "inputs deleted, no output" instead
        let reader =
            VixReader::open(bytes::Bytes::from(result.output.to_bytes().unwrap())).unwrap();
        assert_eq!(reader.row_count(), 0);
    }

    /// MOVE-PATH backstop: a WAL batch that still carries `_timestamp <= 0`
    /// rows (pre-canonicalization WAL) moves with those rows DROPPED and
    /// counted instead of wedging on the writer's finish guard; the output
    /// equals a build over only the healthy rows. An all-poison WAL set
    /// yields the zero-row result the jobs caller converts into a distinct
    /// error (the move path has no clean drop-without-upload commit).
    #[tokio::test]
    async fn move_job_drops_zero_timestamp_rows() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("log", DataType::Utf8, true),
            Field::new("svc", DataType::Utf8, true),
        ]));
        let batch = |ts: Vec<i64>| {
            let logs: Vec<String> = ts
                .iter()
                .map(|&t| {
                    if t <= 0 {
                        "zeroline poison".to_string()
                    } else {
                        format!("error healthy {t}")
                    }
                })
                .collect();
            let svc: Vec<&str> = ts.iter().map(|_| "api").collect();
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(ts)) as ArrayRef,
                    Arc::new(StringArray::from(
                        logs.iter().map(String::as_str).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(svc)),
                ],
            )
            .unwrap()
        };
        let fts = vec!["log".to_string()];
        let cs = vec!["svc".to_string()];

        // poison spread across two WAL batches
        let poisoned_table = Arc::new(
            MemTable::try_new(
                schema.clone(),
                vec![vec![batch(vec![500, 0, 400]), batch(vec![-2, 300])]],
            )
            .unwrap(),
        );
        let result = write_core_file_from_tables(
            "test-move-cleansing",
            schema.clone(),
            vec![poisoned_table],
            &fts,
            &cs,
            &[],
            false,
            0,
        )
        .await
        .unwrap();
        assert_eq!(result.dropped_rows, 2, "both poison rows dropped");
        assert_eq!(result.stats.row_count, 3);
        assert_eq!((result.stats.min_ts, result.stats.max_ts), (300, 500));

        let clean_table = Arc::new(
            MemTable::try_new(
                schema.clone(),
                vec![vec![batch(vec![500, 400]), batch(vec![300])]],
            )
            .unwrap(),
        );
        let clean = write_core_file_from_tables(
            "test-move-cleansing-clean",
            schema.clone(),
            vec![clean_table],
            &fts,
            &cs,
            &[],
            false,
            0,
        )
        .await
        .unwrap();
        assert_eq!(clean.dropped_rows, 0);
        let cleansed_reader = VixReader::open(bytes::Bytes::from(result.data)).unwrap();
        let clean_reader = VixReader::open(bytes::Bytes::from(clean.data)).unwrap();
        assert_core_files_equivalent(&cleansed_reader, &clean_reader, "move cleansed-vs-clean");
        assert_eq!(
            cleansed_reader
                .count(&VixQuery::TokenAnyField {
                    token: b"zeroline".to_vec(),
                })
                .unwrap(),
            0,
            "poison-row tokens never reach the terms"
        );

        // all-poison WAL: zero-row result; the jobs caller fails distinctly
        let all_poison_table =
            Arc::new(MemTable::try_new(schema.clone(), vec![vec![batch(vec![0, -1])]]).unwrap());
        let empty = write_core_file_from_tables(
            "test-move-cleansing-empty",
            schema.clone(),
            vec![all_poison_table],
            &fts,
            &cs,
            &[],
            false,
            0,
        )
        .await
        .unwrap();
        assert_eq!(empty.dropped_rows, 2);
        assert_eq!(empty.stats.row_count, 0);
        let empty_reader = VixReader::open(bytes::Bytes::from(empty.data)).unwrap();
        assert_eq!(empty_reader.row_count(), 0);
    }

    /// LIVE REGRESSION (image .10, `default/logs/default`): after the
    /// cleansing sweep REBUILT stored files, `match_all` filter-backed whole
    /// multi-hundred-thousand-row files ("query touches partial-indexed
    /// fields") — the writer's term derivations applied the
    /// `max_raw_term_len` RAW-term bound to fts fields too, so one
    /// oversize `body` value dropped its tokens and tainted the field into
    /// `partial_fields`. THE differential: a rebuild through the
    /// cleansing/poison fallback of inputs whose fts field carries values
    /// LONGER than `max_raw_term_len` must keep the field fully
    /// match_all-servable — fts typing preserved in the fields table, the
    /// oversize values' tokens present and queryable, `partial_fields`
    /// empty, no raw whole-value terms — and the output must be
    /// reader-EQUIVALENT to a move-built file over the same healthy rows.
    /// A pre-fix TAINTED input (fts field marked partial) must force the
    /// same healing rebuild instead of unioning the taint forward through
    /// the fast path, while untainted files keep the fast path.
    #[tokio::test]
    async fn rebuild_preserves_fts_capability_for_oversize_values() {
        let fts = vec!["body".to_string()];
        let cs = vec!["svc".to_string()];
        let fields = || {
            vec![
                Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
                Field::new("body", DataType::Utf8, true),
                Field::new("note", DataType::Utf8, true),
                Field::new("svc", DataType::Utf8, true),
            ]
        };
        // every healthy row's body is far beyond ZO_VIX_MAX_RAW_TERM_LENGTH
        // (65532): the raw-term path would skip it and taint the field; the
        // fts path must tokenize it regardless. Values are keyed to the
        // row's timestamp so the poisoned inputs' healthy rows equal the
        // move referee's rows exactly.
        let columns = |ts: Vec<i64>| -> Vec<ArrayRef> {
            let bodies: Vec<String> = ts
                .iter()
                .map(|&t| {
                    if t <= 0 {
                        format!("zeroline poison {}", "y".repeat(70_000))
                    } else {
                        format!("heartbeat evt{t} {}", "z".repeat(70_000))
                    }
                })
                .collect();
            let notes: Vec<String> = ts.iter().map(|&t| format!("note-{t}")).collect();
            let svc: Vec<&str> = ts
                .iter()
                .map(|&t| if t % 2 == 0 { "api" } else { "db" })
                .collect();
            vec![
                Arc::new(Int64Array::from(ts)),
                Arc::new(StringArray::from(
                    bodies.iter().map(String::as_str).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    notes.iter().map(String::as_str).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(svc)),
            ]
        };
        let latest_schema = Schema::new(fields());

        // f1 poisoned (interior zero-ts row): merge_core_files refuses the
        // fast path and lands in rebuild_over_sources — the .10 cleansing
        // fallback, the exact path the live sweep rebuilt files through
        let f1 = build_poisoned_core_file(
            fields(),
            columns(vec![900, 0, 700]),
            &fts,
            vec!["svc".to_string()],
        );
        let f2 = build_core_file(
            fields(),
            columns(vec![800, 600]),
            &fts,
            vec!["svc".to_string()],
            None,
        );
        let inputs = vec![
            ("f1.vix".to_string(), f1),
            ("f2.vix".to_string(), f2.clone()),
        ];
        let merged = merge_core_files(&as_inputs(&inputs), &latest_schema, &fts, &cs, &[]).unwrap();
        assert!(!merged.used_index_merge, "poison must force the rebuild");
        assert_eq!(merged.dropped_rows, 1);
        assert_eq!(merged.stats.row_count, 4);
        let merged_reader =
            VixReader::open(bytes::Bytes::from(merged.output.to_bytes().unwrap())).unwrap();

        // the move-built referee over the same healthy rows
        let wal_schema = Arc::new(Schema::new(fields()));
        let batch = |ts: Vec<i64>| RecordBatch::try_new(wal_schema.clone(), columns(ts)).unwrap();
        let table = Arc::new(
            MemTable::try_new(
                wal_schema.clone(),
                vec![vec![batch(vec![900, 700]), batch(vec![800, 600])]],
            )
            .unwrap(),
        );
        let move_built = write_core_file_from_tables(
            "test-fts-oversize-move",
            wal_schema.clone(),
            vec![table],
            &fts,
            &cs,
            &[],
            false,
            0,
        )
        .await
        .unwrap();
        assert_eq!(move_built.dropped_rows, 0);
        let move_reader = VixReader::open(bytes::Bytes::from(move_built.data)).unwrap();

        for (context, reader) in [("rebuilt", &merged_reader), ("move-built", &move_reader)] {
            // fts typing preserved in the fields table: `body` stays in the
            // term plan WITHOUT raw-value capability — the fts marking
            assert!(reader.term_field_names().contains(&"body"), "{context}");
            assert!(
                !reader.has_term_capability("body"),
                "{context}: fts, never term"
            );
            // no partial marking for the oversize fts values (a non-empty
            // set is what sent match_all back to a whole-file scan live)
            assert!(
                reader.partial_fields().is_empty(),
                "{context}: partial_fields must be empty, got {:?}",
                reader.partial_fields()
            );
            // the oversize values' match_all tokens are present + queryable
            assert_eq!(
                matching_docs(
                    reader,
                    &VixQuery::TokenAnyField {
                        token: b"heartbeat".to_vec()
                    }
                ),
                vec![0, 1, 2, 3],
                "{context}: every healthy row"
            );
            for (doc, t) in [(0u32, 900i64), (1, 800), (2, 700), (3, 600)] {
                assert_eq!(
                    matching_docs(
                        reader,
                        &VixQuery::TokenAnyField {
                            token: format!("evt{t}").into_bytes()
                        }
                    ),
                    vec![doc],
                    "{context}: per-row token from inside the oversize value"
                );
            }
            // ... and no raw whole-value term: per-field value lookups on an
            // fts field do not resolve
            assert!(
                reader.eval(&exact("body", "heartbeat")).is_err(),
                "{context}"
            );
        }
        // wholesale reader equivalence: same rows, term table, postings,
        // capabilities, partials and query battery ("byte-identical term
        // behavior to a move-built file over the same rows")
        assert_core_files_equivalent(&merged_reader, &move_reader, "rebuilt-vs-move");
        // the poison row's tokens were cleansed away, oversize and all
        assert_eq!(
            merged_reader
                .count(&VixQuery::TokenAnyField {
                    token: b"zeroline".to_vec()
                })
                .unwrap(),
            0
        );

        // NO OVER-BLOCKING: fixed-writer files with oversize fts values are
        // clean, so their ordinary merge keeps the index-merge fast path —
        // and the merged dictionary carries the oversize values' tokens
        let f1_clean = build_core_file(
            fields(),
            columns(vec![900, 700]),
            &fts,
            vec!["svc".to_string()],
            None,
        );
        let clean_inputs = vec![
            ("c1.vix".to_string(), f1_clean.clone()),
            ("c2.vix".to_string(), f2.clone()),
        ];
        let clean_fast =
            merge_core_files(&as_inputs(&clean_inputs), &latest_schema, &fts, &cs, &[]).unwrap();
        assert!(
            clean_fast.used_index_merge,
            "clean oversize-fts inputs must keep the fast path"
        );
        let clean_fast_reader =
            VixReader::open(bytes::Bytes::from(clean_fast.output.to_bytes().unwrap())).unwrap();
        assert_core_files_equivalent(&clean_fast_reader, &move_reader, "clean-fast-vs-move");

        // HEALING: a pre-fix tainted input — fts field marked partial,
        // fabricated by property patching since the fixed writer cannot
        // produce the shape — must force the rebuild (the input's
        // dictionary is missing the oversize values' tokens only a
        // `_source` rebuild re-derives), and the rebuilt output must drop
        // the taint instead of unioning it forward forever.
        let tainted = bytes::Bytes::from(
            vortex_index::test_support::repack_with_partial_fields(&f2, &["body"]).unwrap(),
        );
        let tainted_inputs = vec![
            ("c1.vix".to_string(), f1_clean),
            ("tainted.vix".to_string(), tainted),
        ];
        let healed =
            merge_core_files(&as_inputs(&tainted_inputs), &latest_schema, &fts, &cs, &[]).unwrap();
        assert!(
            !healed.used_index_merge,
            "a tainted fts field must force the healing rebuild"
        );
        assert_eq!(healed.dropped_rows, 0);
        let healed_reader =
            VixReader::open(bytes::Bytes::from(healed.output.to_bytes().unwrap())).unwrap();
        assert!(
            healed_reader.partial_fields().is_empty(),
            "the taint must not survive the rebuild"
        );
        assert_core_files_equivalent(&healed_reader, &move_reader, "healed-vs-move");
    }

    /// REGRESSION: a stream whose settings carry an EMPTY
    /// `full_text_search_keys` (the live `default/logs/default` shape) still
    /// resolves the config-default fts set — `get_stream_setting_fts_fields`
    /// (settings ∪ `SQL_FULL_TEXT_SEARCH_FIELDS`) is THE single shared
    /// resolution, used by the move job (`jobs/parquet.rs`) and the
    /// compactor (`compact/merge.rs`) for BOTH merge strategies — and a
    /// rebuild driven by that resolved set classifies the default fields
    /// (`body`, ...) as fts: tokens queryable, no raw whole-value term, no
    /// partial marking for oversize values.
    #[test]
    fn rebuild_resolves_default_fts_for_empty_settings_keys() {
        use config::meta::stream::StreamSettings;
        use infra::schema::get_stream_setting_fts_fields;

        // the live stream shape: settings PRESENT, fts keys EMPTY
        let settings = StreamSettings::from(r#"{"full_text_search_keys": []}"#);
        assert!(settings.full_text_search_keys.is_empty(), "corpus sanity");
        let resolved = get_stream_setting_fts_fields(&Some(settings));
        for default_field in ["body", "log", "message"] {
            assert!(
                resolved.iter().any(|f| f == default_field),
                "config default {default_field:?} must survive empty settings keys, got \
                 {resolved:?}"
            );
        }

        // drive a REBUILD with exactly that resolved set: `body` (a config
        // default the stream settings never name) must come out fts-typed
        // with its oversize value tokenized
        let fields = || {
            vec![
                Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
                Field::new("body", DataType::Utf8, true),
            ]
        };
        let oversize = format!("heartbeat {}", "z".repeat(70_000));
        let input = build_core_file(
            fields(),
            vec![
                Arc::new(Int64Array::from(vec![200, 100])),
                Arc::new(StringArray::from(vec![
                    oversize.as_str(),
                    "heartbeat compact",
                ])),
            ],
            &resolved,
            vec![],
            None,
        );
        let latest_schema = Schema::new(fields());
        let rebuilt = merge_core_files_rebuild(
            &as_inputs(&[("in.vix".to_string(), input)]),
            &latest_schema,
            &resolved,
            &[],
            &[],
        )
        .unwrap();
        let reader =
            VixReader::open(bytes::Bytes::from(rebuilt.output.to_bytes().unwrap())).unwrap();
        assert!(reader.term_field_names().contains(&"body"));
        assert!(
            !reader.has_term_capability("body"),
            "body must be fts-typed (tokens, no raw-value capability)"
        );
        assert!(
            reader.partial_fields().is_empty(),
            "no partial marking, got {:?}",
            reader.partial_fields()
        );
        assert_eq!(
            matching_docs(
                &reader,
                &VixQuery::TokenAnyField {
                    token: b"heartbeat".to_vec()
                }
            ),
            vec![0, 1],
            "tokens of the oversize AND compact values are queryable"
        );
        assert!(
            reader.eval(&exact("body", "heartbeat compact")).is_err(),
            "no raw whole-value term on an fts field"
        );
    }

    /// In-memory [`VixRangeSource`] counting fetched bytes — proves what a
    /// probe reads (a `Current` classification must never touch docs data).
    struct CountingRangeSource {
        data: bytes::Bytes,
        fetched: Arc<std::sync::atomic::AtomicU64>,
    }

    impl VixRangeSource for CountingRangeSource {
        fn len(&self) -> u64 {
            self.data.len() as u64
        }

        fn fetch(
            &self,
            range: std::ops::Range<u64>,
        ) -> futures::future::BoxFuture<'static, anyhow::Result<bytes::Bytes>> {
            use futures::FutureExt;
            self.fetched.fetch_add(
                range.end - range.start,
                std::sync::atomic::Ordering::Relaxed,
            );
            let bytes = self.data.slice(range.start as usize..range.end as usize);
            async move { Ok(bytes) }.boxed()
        }
    }

    fn classify_bytes(
        data: &bytes::Bytes,
        latest_schema: &Schema,
        fts: &[String],
        cs: &[String],
    ) -> Result<CoreFileStatus, anyhow::Error> {
        classify_core_file(
            "probe.vix",
            Arc::new(CountingRangeSource {
                data: data.clone(),
                fetched: Arc::default(),
            }),
            latest_schema,
            fts,
            cs,
            &[],
        )
    }

    #[track_caller]
    fn assert_needs_rebuild(
        status: &Result<CoreFileStatus, anyhow::Error>,
        needles: &[&str],
        context: &str,
    ) {
        match status {
            Ok(CoreFileStatus::NeedsRebuild(reason)) => {
                for needle in needles {
                    assert!(
                        reason.contains(needle),
                        "{context}: reason {reason:?} must name {needle:?}"
                    );
                }
            }
            other => panic!("{context}: expected NeedsRebuild, got {other:?}"),
        }
    }

    /// The single-file healing corpus: one hour partition's lone core file,
    /// fabricated in every capability state the probe must distinguish.
    fn healing_fields() -> Vec<Field> {
        vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("body", DataType::Utf8, true),
            Field::new("note", DataType::Utf8, true),
            Field::new("svc", DataType::Utf8, true),
            Field::new("code", DataType::Int64, true),
        ]
    }

    fn healing_columns(ts: Vec<i64>) -> Vec<ArrayRef> {
        let bodies: Vec<String> = ts.iter().map(|&t| format!("heartbeat evt{t}")).collect();
        let notes: Vec<String> = ts.iter().map(|&t| format!("note-{t}")).collect();
        let svc: Vec<&str> = ts
            .iter()
            .map(|&t| if t % 2 == 0 { "api" } else { "db" })
            .collect();
        let codes: Vec<i64> = ts.iter().map(|&t| 200 + (t % 2) * 300).collect();
        vec![
            Arc::new(Int64Array::from(ts)),
            Arc::new(StringArray::from(
                bodies.iter().map(String::as_str).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                notes.iter().map(String::as_str).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(svc)),
            Arc::new(Int64Array::from(codes)),
        ]
    }

    /// THE single-file healing eligibility table: `classify_core_file` must
    /// flag exactly the capability gaps the merge paths already enforce —
    /// pre-.11 fts taint, tokenizer / fts-vs-term marking drift, value
    /// terms the current plan derives but the file lacks, configured
    /// column-store fields with no docs column — and stay `Current` (the
    /// no-op) for a fully capable file and for cs entries with nothing to
    /// derive. Every `NeedsRebuild` file must classify `Current` after one
    /// single-input rebuild: healing converges, never loops.
    #[test]
    fn classify_core_file_decides_single_file_healing() {
        let fts = vec!["body".to_string()];
        let cs = vec!["svc".to_string()];
        let latest_schema = Schema::new(healing_fields());
        let current = build_core_file(
            healing_fields(),
            healing_columns(vec![900, 800, 700]),
            &fts,
            vec!["svc".to_string()],
            None,
        );

        // fully capable file: the no-op verdict
        assert!(
            matches!(
                classify_bytes(&current, &latest_schema, &fts, &cs),
                Ok(CoreFileStatus::Current)
            ),
            "a current file must classify Current"
        );

        // pre-.11 oversize taint: a plan-fts field marked partial
        let tainted = bytes::Bytes::from(
            vortex_index::test_support::repack_with_partial_fields(&current, &["body"]).unwrap(),
        );
        assert_needs_rebuild(
            &classify_bytes(&tainted, &latest_schema, &fts, &cs),
            &["body", "partial"],
            "fts taint",
        );

        // missing value terms: the file carries `code` values without term
        // capability (pre-numeric-value-terms files, fast-path-demoted
        // fields) — the registry-enriched plan value-indexes it
        let demoted = bytes::Bytes::from(
            vortex_index::test_support::repack_dropping_field_term_capability(&current, "code")
                .unwrap(),
        );
        assert_needs_rebuild(
            &classify_bytes(&demoted, &latest_schema, &fts, &cs),
            &["code", "value terms"],
            "dropped value-term capability",
        );

        // configured cs field stored only in _source (no docs column)
        let plain = build_core_file(
            healing_fields(),
            healing_columns(vec![900, 800, 700]),
            &fts,
            vec![],
            None,
        );
        assert_needs_rebuild(
            &classify_bytes(&plain, &latest_schema, &fts, &cs),
            &["svc", "docs column"],
            "missing cs column",
        );

        // fts settings drift: `note` raw-term-indexed in the file, fts in
        // the current plan
        assert_needs_rebuild(
            &classify_bytes(
                &current,
                &latest_schema,
                &["body".to_string(), "note".to_string()],
                &cs,
            ),
            &["note"],
            "term-vs-fts marking drift",
        );

        // tokenizer drift
        let old_tokenizer = bytes::Bytes::from(
            vortex_index::test_support::repack_with_tokenizer_property(&current, "o2-v1").unwrap(),
        );
        assert_needs_rebuild(
            &classify_bytes(&old_tokenizer, &latest_schema, &fts, &cs),
            &["tokenizer"],
            "tokenizer drift",
        );

        // NO false heal: a configured cs field the file does not carry (no
        // key term) has nothing to derive — Current either when the schema
        // types it or when it is unknown entirely
        let mut ghost_schema_fields = healing_fields();
        ghost_schema_fields.push(Field::new("ghost", DataType::Utf8, true));
        let ghost_schema = Schema::new(ghost_schema_fields);
        for (schema, cs_config, context) in [
            (
                &ghost_schema,
                vec!["svc".to_string(), "ghost".to_string()],
                "carried by no doc",
            ),
            (
                &latest_schema,
                vec!["svc".to_string(), "not_in_schema".to_string()],
                "not in the schema",
            ),
        ] {
            assert!(
                matches!(
                    classify_bytes(&current, schema, &fts, &cs_config),
                    Ok(CoreFileStatus::Current)
                ),
                "cs field {context}: nothing to derive, must stay Current"
            );
        }

        // an unreadable container errors (healing it would gut the index);
        // the probe caller logs and leaves the file alone
        assert!(
            classify_bytes(
                &bytes::Bytes::from_static(b"not a vix container"),
                &latest_schema,
                &fts,
                &cs
            )
            .is_err()
        );

        // CONVERGENCE: one single-input healing rebuild makes every
        // outdated file classify Current — the probe can never loop
        for (context, data) in [
            ("fts taint", &tainted),
            ("dropped value-term capability", &demoted),
            ("missing cs column", &plain),
            ("tokenizer drift", &old_tokenizer),
        ] {
            let healed = merge_core_files_rebuild(
                &as_inputs(&[("single.vix".to_string(), data.clone())]),
                &latest_schema,
                &fts,
                &cs,
                &[],
            )
            .unwrap();
            assert!(
                matches!(
                    classify_bytes(
                        &bytes::Bytes::from(healed.output.to_bytes().unwrap()),
                        &latest_schema,
                        &fts,
                        &cs
                    ),
                    Ok(CoreFileStatus::Current)
                ),
                "{context}: the healed output must classify Current"
            );
        }
    }

    /// The `Current` probe is METADATA-CHEAP: classifying a docs-heavy
    /// current file over a counting range source reads the container
    /// footer, fields table, dictionary directory and docs FOOTER only —
    /// a small fraction of the object — never the docs data itself (the
    /// no-op path of a healing job downloads no docs).
    #[test]
    fn classify_current_file_reads_no_docs_data() {
        // ~2000 rows of high-entropy `_original` payload: the docs blob
        // dominates the object even after compression, while the term
        // dictionary stays small (few distinct tokens/values)
        let rows = 2000usize;
        let ts: Vec<i64> = (0..rows as i64).map(|i| 2_000_000 - i).collect();
        let mut seed = 0x9E37_79B9_7F4A_7C15u64;
        let mut random_hex = |chunks: usize| -> String {
            let mut out = String::with_capacity(chunks * 16);
            for _ in 0..chunks {
                seed = seed
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                out.push_str(&format!("{seed:016x}"));
            }
            out
        };
        let originals: Vec<String> = (0..rows).map(|_| random_hex(100)).collect();
        let fields = vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("body", DataType::Utf8, true),
            Field::new("svc", DataType::Utf8, true),
        ];
        let bodies: Vec<&str> = (0..rows).map(|_| "heartbeat lorem ipsum").collect();
        let svc: Vec<&str> = (0..rows)
            .map(|i| if i % 2 == 0 { "api" } else { "db" })
            .collect();
        let data = build_core_file(
            fields.clone(),
            vec![
                Arc::new(Int64Array::from(ts)),
                Arc::new(StringArray::from(bodies)),
                Arc::new(StringArray::from(svc)),
            ],
            &["body".to_string()],
            vec!["svc".to_string()],
            Some(StringArray::from(
                originals.iter().map(String::as_str).collect::<Vec<_>>(),
            )),
        );

        let fetched = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let status = classify_core_file(
            "probe.vix",
            Arc::new(CountingRangeSource {
                data: data.clone(),
                fetched: Arc::clone(&fetched),
            }),
            &Schema::new(fields),
            &["body".to_string()],
            &["svc".to_string()],
            &[],
        )
        .unwrap();
        assert!(matches!(status, CoreFileStatus::Current));

        let read = fetched.load(std::sync::atomic::Ordering::Relaxed);
        let total = data.len() as u64;
        assert!(read > 0, "the probe must have read the footer");
        assert!(
            read * 4 < total,
            "a Current probe must stay metadata-only: fetched {read} of {total} bytes"
        );
    }

    /// THE single-file healing differential: each outdated shape — the
    /// pre-.11 fts taint, dropped value-term capability, a configured cs
    /// field with no docs column — is healed by ONE single-input rebuild
    /// whose output is reader-EQUIVALENT to a move-built file of the same
    /// rows under the current settings (docs columns, term table +
    /// postings, capabilities, partials, query battery), restores the
    /// specific capability, and classifies Current.
    #[tokio::test]
    async fn single_file_healing_rebuild_restores_capabilities() {
        let fts = vec!["body".to_string()];
        let cs = vec!["svc".to_string()];
        let latest_schema = Schema::new(healing_fields());
        let ts = vec![900i64, 800, 700, 600];

        // the move-built referee over the same rows with CURRENT settings
        let wal_schema = Arc::new(Schema::new(healing_fields()));
        let batch = RecordBatch::try_new(wal_schema.clone(), healing_columns(ts.clone())).unwrap();
        let table = Arc::new(MemTable::try_new(wal_schema.clone(), vec![vec![batch]]).unwrap());
        let move_built = write_core_file_from_tables(
            "test-single-heal-move",
            wal_schema.clone(),
            vec![table],
            &fts,
            &cs,
            &[],
            false,
            0,
        )
        .await
        .unwrap();
        let move_reader = VixReader::open(bytes::Bytes::from(move_built.data)).unwrap();

        let current = build_core_file(
            healing_fields(),
            healing_columns(ts.clone()),
            &fts,
            vec!["svc".to_string()],
            None,
        );
        let cases: Vec<(&str, bytes::Bytes)> = vec![
            (
                "fts-tainted",
                bytes::Bytes::from(
                    vortex_index::test_support::repack_with_partial_fields(&current, &["body"])
                        .unwrap(),
                ),
            ),
            (
                "value-terms-dropped",
                bytes::Bytes::from(
                    vortex_index::test_support::repack_dropping_field_term_capability(
                        &current, "code",
                    )
                    .unwrap(),
                ),
            ),
            (
                "cs-missing",
                build_core_file(
                    healing_fields(),
                    healing_columns(ts.clone()),
                    &fts,
                    vec![],
                    None,
                ),
            ),
        ];

        for (context, data) in cases {
            assert!(
                matches!(
                    classify_bytes(&data, &latest_schema, &fts, &cs),
                    Ok(CoreFileStatus::NeedsRebuild(_))
                ),
                "{context}: the probe must flag the file"
            );
            let healed = merge_core_files_rebuild(
                &as_inputs(&[("single.vix".to_string(), data)]),
                &latest_schema,
                &fts,
                &cs,
                &[],
            )
            .unwrap();
            assert!(!healed.used_index_merge, "{context}: heal is a rebuild");
            assert_eq!(healed.dropped_rows, 0, "{context}");
            let healed_bytes = bytes::Bytes::from(healed.output.to_bytes().unwrap());
            let healed_reader = VixReader::open(healed_bytes.clone()).unwrap();

            // the healed output carries EVERY current capability and is
            // indistinguishable from a fresh move build of the same rows
            assert_core_files_equivalent(
                &healed_reader,
                &move_reader,
                &format!("{context}: healed vs move-built"),
            );
            assert!(
                healed_reader.partial_fields().is_empty(),
                "{context}: no taint survives"
            );
            assert_eq!(
                matching_docs(
                    &healed_reader,
                    &VixQuery::TokenAnyField {
                        token: b"heartbeat".to_vec()
                    }
                ),
                vec![0, 1, 2, 3],
                "{context}: match_all capability restored for every row"
            );
            assert!(
                healed_reader.has_term_capability("code"),
                "{context}: numeric value terms restored"
            );
            assert!(
                healed_reader.has_column_store_field("svc"),
                "{context}: configured cs column materialized"
            );
            assert_eq!(
                read_strings(&healed_reader, "svc"),
                vec![
                    Some("api".to_string()),
                    Some("api".to_string()),
                    Some("api".to_string()),
                    Some("api".to_string()),
                ],
                "{context}: cs values come from _source truth"
            );
            assert!(
                matches!(
                    classify_bytes(&healed_bytes, &latest_schema, &fts, &cs),
                    Ok(CoreFileStatus::Current)
                ),
                "{context}: healing converges (no rebuild loop)"
            );
        }
    }

    /// A configured `column_store_fields` entry that NO merge input stores
    /// as a docs column (every file predates the setting) is MATERIALIZED
    /// from `_source` — on the index-merge fast path AND the rebuild, which
    /// stay reader-equivalent — instead of being skipped forever.
    #[tokio::test]
    async fn merge_materializes_configured_cs_column_missing_from_all_inputs() {
        let fts = vec!["body".to_string()];
        let cs = vec!["svc".to_string()];
        let latest_schema = Schema::new(healing_fields());
        // disjoint time ranges: the docs stream-copy (sequential) path
        let old1 = build_core_file(
            healing_fields(),
            healing_columns(vec![900, 800]),
            &fts,
            vec![],
            None,
        );
        let old2 = build_core_file(
            healing_fields(),
            healing_columns(vec![700, 600]),
            &fts,
            vec![],
            None,
        );
        let inputs = vec![
            ("old1.vix".to_string(), old1),
            ("old2.vix".to_string(), old2),
        ];

        let fast = merge_core_files(&as_inputs(&inputs), &latest_schema, &fts, &cs, &[]).unwrap();
        assert!(
            fast.used_index_merge,
            "cs materialization must not force the rebuild path"
        );
        let fast_reader =
            VixReader::open(bytes::Bytes::from(fast.output.to_bytes().unwrap())).unwrap();
        assert!(fast_reader.has_column_store_field("svc"));
        assert_eq!(
            read_strings(&fast_reader, "svc"),
            vec![
                Some("api".to_string()),
                Some("api".to_string()),
                Some("api".to_string()),
                Some("api".to_string()),
            ],
            "derived docs column holds the _source truth for every row"
        );

        let rebuilt =
            merge_core_files_rebuild(&as_inputs(&inputs), &latest_schema, &fts, &cs, &[]).unwrap();
        let rebuilt_reader =
            VixReader::open(bytes::Bytes::from(rebuilt.output.to_bytes().unwrap())).unwrap();
        assert_core_files_equivalent(&fast_reader, &rebuilt_reader, "all-old cs: fast vs rebuild");
    }

    /// REAL-SIZE regression for the live compactor panic: a rebuild merge
    /// over an input whose `_source` column alone exceeds `i32::MAX` bytes
    /// (the old code materialized that column as ONE arrow array and died in
    /// arrow's offset builder with "byte array offset overflow"). Ignored by
    /// default — it allocates several GiB; the bounded-flow property is
    /// asserted on small data by `merge_bounded_batches_match_default_caps`.
    ///
    /// ```text
    /// cargo test --release -p openobserve-core --lib \
    ///   vix::core_writer::tests::merge_rebuild_survives_2gib_source -- --ignored --nocapture
    /// ```
    #[test]
    #[ignore = "allocates several GiB (real-size byte-overflow regression)"]
    fn merge_rebuild_survives_2gib_source() {
        // 34 rows x 64 MiB ≈ 2.13 GiB of `_source` text in ONE input file —
        // just past the i32 offset limit the old whole-column load hit.
        const ROWS: usize = 34;
        const ROW_BYTES: usize = 64 * 1024 * 1024;
        let schema = Arc::new(Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("payload", DataType::Utf8, true),
        ]));
        let mut writer =
            VixWriter::new(&schema, core_writer_options(&[], vec![], Vec::new()), false);
        let filler = "x".repeat(ROW_BYTES);
        for row in 0..ROWS {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![(ROWS - row) as i64 * 1000])) as ArrayRef,
                    Arc::new(StringArray::from(vec![format!("{row:04}-{filler}")])),
                ],
            )
            .unwrap();
            let source = synthesize_source(&batch).unwrap();
            writer
                .push_batch_with_source(&batch, &source, None)
                .unwrap();
        }
        let input = bytes::Bytes::from(writer.finish().unwrap());
        eprintln!("input file: {} MiB compressed", input.len() / (1024 * 1024));

        let latest_schema = Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("payload", DataType::Utf8, true),
        ]);
        let inputs = vec![("big.vix".to_string(), input)];
        let started = std::time::Instant::now();
        let rebuild =
            merge_core_files_rebuild(&as_inputs(&inputs), &latest_schema, &[], &[], &[]).unwrap();
        eprintln!(
            "rebuild: {} rows in {} bounded batches, {:?}",
            rebuild.stats.row_count,
            rebuild.docs_batches,
            started.elapsed()
        );
        assert_eq!(rebuild.stats.row_count, ROWS as u64);
        assert!(
            rebuild.docs_batches >= (ROWS * ROW_BYTES) / DOCS_BATCH_BYTES,
            "a >2 GiB `_source` must stream in multiple byte-capped batches, got {}",
            rebuild.docs_batches
        );
        let reader =
            VixReader::open(bytes::Bytes::from(rebuild.output.to_bytes().unwrap())).unwrap();
        assert_eq!(reader.row_count(), ROWS as u64);
        let ts = read_i64(&reader, TIMESTAMP_COL_NAME);
        assert!(ts.windows(2).all(|pair| pair[0] >= pair[1]), "DESC order");
        // spot-check data integrity through a bounded point read
        let sources = reader.read_source(&[0, (ROWS - 1) as u64]).unwrap();
        let first: serde_json::Value = serde_json::from_str(sources.value(0)).unwrap();
        assert_eq!(
            first["payload"].as_str().unwrap().len(),
            ROW_BYTES + 5,
            "row 0 payload survived intact"
        );
    }

    /// A term/fts capability conflict between an input and the current
    /// settings must fall back to the rebuild (and still produce a correct
    /// file).
    #[tokio::test]
    async fn merge_falls_back_on_capability_conflict() {
        let schema_fields = || {
            vec![
                Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
                Field::new("svc", DataType::Utf8, true),
            ]
        };
        // svc tokenized in file1, raw-indexed in file2
        let file1 = build_core_file(
            schema_fields(),
            vec![
                Arc::new(Int64Array::from(vec![100, 90])),
                Arc::new(StringArray::from(vec!["api gateway", "db"])),
            ],
            &["svc".to_string()],
            vec![],
            None,
        );
        let file2 = build_core_file(
            schema_fields(),
            vec![
                Arc::new(Int64Array::from(vec![80, 70])),
                Arc::new(StringArray::from(vec!["api gateway", "web"])),
            ],
            &[],
            vec![],
            None,
        );
        let latest_schema = Schema::new(schema_fields());
        let inputs = vec![("f1.vix".to_string(), file1), ("f2.vix".to_string(), file2)];

        // current settings: svc NOT fts -> file1 conflicts -> rebuild
        let result = merge_core_files(&as_inputs(&inputs), &latest_schema, &[], &[], &[]).unwrap();
        assert!(!result.used_index_merge);
        let reference =
            merge_core_files_rebuild(&as_inputs(&inputs), &latest_schema, &[], &[], &[]).unwrap();
        let result_reader =
            VixReader::open(bytes::Bytes::from(result.output.to_bytes().unwrap())).unwrap();
        let reference_reader =
            VixReader::open(bytes::Bytes::from(reference.output.to_bytes().unwrap())).unwrap();
        assert_core_files_equivalent(&result_reader, &reference_reader, "conflict");
        // the rebuild re-derived raw svc terms for every row
        assert_eq!(
            matching_docs(&result_reader, &exact("svc", "api gateway")),
            vec![0, 2]
        );

        // current settings: svc fts -> file2 conflicts -> rebuild too
        let result = merge_core_files(
            &as_inputs(&inputs),
            &latest_schema,
            &["svc".to_string()],
            &[],
            &[],
        )
        .unwrap();
        assert!(!result.used_index_merge);
    }

    /// Manual timing harness over REAL core files (compaction-shaped data).
    /// Copy a handful of `.vix` move-job outputs into a scratch dir and run:
    ///
    /// ```text
    /// O2_VIX_MERGE_BENCH_DIR=/path/to/scratch cargo test --release \
    ///   -p openobserve-core --lib vix::core_writer::tests::bench_merge_core_files_real \
    ///   -- --ignored --nocapture
    /// ```
    ///
    /// Times the index-merge fast path against the full rebuild over the
    /// same inputs. `O2_VIX_MERGE_BENCH_VERIFY=1` additionally proves the
    /// two outputs reader-equivalent (decodes every posting of both files —
    /// slow, not part of the timing).
    /// Minimal stderr logger so the merge's `log::debug!` phase timings are
    /// visible under `--nocapture`.
    struct StderrLogger;
    impl log::Log for StderrLogger {
        fn enabled(&self, metadata: &log::Metadata) -> bool {
            metadata.target().contains("vix")
                || metadata.target().starts_with("vortex_index")
                || metadata.level() <= log::Level::Warn
        }
        fn log(&self, record: &log::Record) {
            if self.enabled(record.metadata()) {
                eprintln!("[{}] {}", record.level(), record.args());
            }
        }
        fn flush(&self) {}
    }

    #[test]
    #[ignore = "manual timing over real core files (set O2_VIX_MERGE_BENCH_DIR)"]
    fn bench_merge_core_files_real() {
        let Some(dir) = std::env::var_os("O2_VIX_MERGE_BENCH_DIR") else {
            eprintln!("O2_VIX_MERGE_BENCH_DIR not set; skipping");
            return;
        };
        static LOGGER: StderrLogger = StderrLogger;
        if log::set_logger(&LOGGER).is_ok() {
            log::set_max_level(log::LevelFilter::Debug);
        }
        let mut paths: Vec<std::path::PathBuf> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|entry| {
                let path = entry.unwrap().path();
                path.extension()
                    .is_some_and(|ext| ext == "vix")
                    .then_some(path)
            })
            .collect();
        paths.sort();
        assert!(!paths.is_empty(), "no .vix files in {dir:?}");
        let inputs: Vec<(String, bytes::Bytes)> = paths
            .iter()
            .map(|path| {
                (
                    path.file_name().unwrap().to_string_lossy().into_owned(),
                    bytes::Bytes::from(std::fs::read(path).unwrap()),
                )
            })
            .collect();
        let mib = |bytes: usize| bytes as f64 / (1024.0 * 1024.0);
        let total_bytes: usize = inputs.iter().map(|(_, data)| data.len()).sum();

        // derive the stream settings from the files themselves — the
        // compactor merges under unchanged settings in the common case
        let mut fts: Vec<String> = Vec::new();
        let mut cs: Vec<String> = Vec::new();
        let mut latest_fields: Vec<Field> = Vec::new();
        let mut rows = 0u64;
        let mut ranges: Vec<(i64, i64)> = Vec::new();
        for (key, data) in &inputs {
            let reader = VixReader::open(data.clone()).unwrap();
            rows += reader.row_count();
            let ts = as_int64_array(&reader.read_docs_column(TIMESTAMP_COL_NAME).unwrap()).unwrap();
            let (min_ts, max_ts) = (
                arrow::compute::min(&ts).unwrap_or(0),
                arrow::compute::max(&ts).unwrap_or(0),
            );
            eprintln!(
                "  {key}: {} rows, ts [{min_ts}, {max_ts}], {:.1} MiB",
                reader.row_count(),
                mib(data.len()),
            );
            ranges.push((min_ts, max_ts));
            for field in reader.docs_schema().unwrap().fields() {
                let name = field.name().as_str();
                if name == SOURCE_COL_NAME || name == ORIGINAL_DATA_COL_NAME {
                    continue;
                }
                if !latest_fields.iter().any(|f| f.name() == name) {
                    latest_fields.push(Field::new(
                        name,
                        field.data_type().clone(),
                        name != TIMESTAMP_COL_NAME,
                    ));
                }
                if name != TIMESTAMP_COL_NAME
                    && name != ID_COL_NAME
                    && !cs.iter().any(|f| f == name)
                {
                    cs.push(name.to_string());
                }
            }
            for name in reader.term_field_names() {
                if !latest_fields.iter().any(|f| f.name() == name) {
                    latest_fields.push(Field::new(name, DataType::Utf8, true));
                }
                if !reader.has_term_capability(name) && !fts.iter().any(|f| f == name) {
                    fts.push(name.to_string());
                }
            }
        }
        let latest_schema = Schema::new(latest_fields);
        ranges.sort_unstable();
        let disjoint = ranges.windows(2).all(|pair| pair[0].1 < pair[1].0);
        eprintln!(
            "merging {} files / {rows} rows / {:.1} MiB input; disjoint time ranges: {disjoint}; \
             fts={fts:?} cs={cs:?}",
            inputs.len(),
            mib(total_bytes),
        );

        let started = std::time::Instant::now();
        let fast = merge_core_files(&as_inputs(&inputs), &latest_schema, &fts, &cs, &[]).unwrap();
        let fast_elapsed = started.elapsed();
        eprintln!(
            "index-merge path: {fast_elapsed:>8.2?}  (used_index_merge={}, out {:.1} MiB, {} \
             terms, index {:.1} MiB, docs {:.1} MiB)",
            fast.used_index_merge,
            mib(fast.output.len() as usize),
            fast.stats.term_count,
            mib(fast.stats.index_size as usize),
            mib(fast.stats.docs_size as usize),
        );
        assert!(
            fast.used_index_merge,
            "expected the fast path on real move-job outputs"
        );

        if std::env::var("O2_VIX_MERGE_BENCH_SKIP_REBUILD").is_ok_and(|v| v == "1") {
            eprintln!("rebuild path skipped (O2_VIX_MERGE_BENCH_SKIP_REBUILD=1)");
            return;
        }
        let started = std::time::Instant::now();
        let rebuild =
            merge_core_files_rebuild(&as_inputs(&inputs), &latest_schema, &fts, &cs, &[]).unwrap();
        let rebuild_elapsed = started.elapsed();
        eprintln!(
            "rebuild path:     {rebuild_elapsed:>8.2?}  (out {:.1} MiB, {} terms)",
            mib(rebuild.output.len() as usize),
            rebuild.stats.term_count,
        );
        eprintln!(
            "speedup: {:.1}x",
            rebuild_elapsed.as_secs_f64() / fast_elapsed.as_secs_f64()
        );

        assert_eq!(fast.stats.row_count, rebuild.stats.row_count);
        assert_eq!(fast.stats.term_count, rebuild.stats.term_count);

        if std::env::var("O2_VIX_MERGE_BENCH_VERIFY").is_ok_and(|v| v == "1") {
            use std::hash::{DefaultHasher, Hash, Hasher};
            let fast_reader =
                VixReader::open(bytes::Bytes::from(fast.output.to_bytes().unwrap())).unwrap();
            let rebuild_reader =
                VixReader::open(bytes::Bytes::from(rebuild.output.to_bytes().unwrap())).unwrap();
            let digest = |reader: &VixReader| {
                let mut hasher = DefaultHasher::new();
                let mut count = 0u64;
                reader
                    .for_each_term(&mut |key, doc_count, ids| {
                        key.hash(&mut hasher);
                        doc_count.hash(&mut hasher);
                        ids.hash(&mut hasher);
                        count += 1;
                        Ok(())
                    })
                    .unwrap();
                (count, hasher.finish())
            };
            assert_eq!(
                digest(&fast_reader),
                digest(&rebuild_reader),
                "term tables diverge"
            );
            assert_eq!(
                fast_reader.partial_fields(),
                rebuild_reader.partial_fields()
            );
            for field in fast_reader.docs_schema().unwrap().fields() {
                let fast_column = fast_reader.read_docs_column(field.name()).unwrap();
                let rebuild_column = rebuild_reader.read_docs_column(field.name()).unwrap();
                assert_eq!(
                    fast_column.to_data(),
                    rebuild_column.to_data(),
                    "docs column {:?}",
                    field.name()
                );
            }
            eprintln!("verify: term tables, partial fields and docs columns identical");
        }
    }

    /// Manual timing harness for the SINGLE-FILE build path (the WAL->storage
    /// move job: `push_batch_with_source` term extraction + `finish` encode).
    /// Synthesizes a k8s-logs-shaped batch and times the two phases separately
    /// across a set of `encode_threads` values, so the encode-parallelism win
    /// is visible against the (thread-independent) term-accumulation floor:
    ///
    /// ```text
    /// O2_VIX_BUILD_BENCH_ROWS=200000 O2_VIX_BUILD_BENCH_THREADS=1,2,4,8 \
    ///   cargo test --release -p openobserve-core --lib \
    ///   vix::core_writer::tests::bench_build_core_file -- --ignored --nocapture
    /// ```
    #[test]
    #[ignore = "manual timing of the single-file build (set O2_VIX_BUILD_BENCH_ROWS)"]
    fn bench_build_core_file() {
        let rows: usize = std::env::var("O2_VIX_BUILD_BENCH_ROWS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(200_000);
        let thread_list: Vec<usize> = std::env::var("O2_VIX_BUILD_BENCH_THREADS")
            .unwrap_or_else(|_| "1,2,4,8".to_string())
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        let mib = |bytes: usize| bytes as f64 / (1024.0 * 1024.0);

        // A flattened k8s-logs batch: structured string fields (bounded
        // cardinality), one full-text `message` field (the tokenization cost
        // driver — a request id per row makes the dictionary realistically
        // large), plus numeric/id columns. Matches the move job's column set.
        let schema = Arc::new(Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("level", DataType::Utf8, true),
            Field::new("service", DataType::Utf8, true),
            Field::new("kubernetes.namespace.name", DataType::Utf8, true),
            Field::new("kubernetes.pod.name", DataType::Utf8, true),
            Field::new("http.method", DataType::Utf8, true),
            Field::new("http.status", DataType::Int64, true),
            Field::new("message", DataType::Utf8, true),
        ]));
        let levels = ["info", "warn", "error", "debug", "trace"];
        let services = [
            "svc-0", "svc-1", "svc-2", "svc-3", "svc-4", "svc-5", "svc-6", "svc-7", "svc-8",
            "svc-9", "svc-10", "svc-11",
        ];
        let namespaces = ["ns-0", "ns-1", "ns-2", "ns-3", "ns-4", "ns-5"];
        let methods = ["GET", "POST", "PUT", "DELETE", "PATCH"];

        let chunk_rows = 8192usize;
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut sources: Vec<StringArray> = Vec::new();
        let mut built = 0usize;
        let base_ts = 1_700_000_000_000_000i64;
        while built < rows {
            let n = chunk_rows.min(rows - built);
            let mut ts = Vec::with_capacity(n);
            let mut level = Vec::with_capacity(n);
            let mut service = Vec::with_capacity(n);
            let mut ns = Vec::with_capacity(n);
            let mut pod = Vec::with_capacity(n);
            let mut method = Vec::with_capacity(n);
            let mut status = Vec::with_capacity(n);
            let mut message = Vec::with_capacity(n);
            for row in 0..n {
                let g = built + row;
                ts.push(base_ts - g as i64 * 1000);
                level.push(levels[g % levels.len()]);
                let svc = services[g % services.len()];
                service.push(svc);
                ns.push(namespaces[g % namespaces.len()]);
                pod.push(format!("{svc}-pod-{}", g % 200));
                method.push(methods[g % methods.len()]);
                status.push(if g.is_multiple_of(7) { 500i64 } else { 200 });
                // ~16 tokens; `req` id has high cardinality (dict stress).
                message.push(format!(
                    "GET /api/v1/namespaces/{}/pods/{}-pod-{} returned status {} in {}ms request \
                     req-{} user user-{} action reconcile failed retry",
                    namespaces[g % namespaces.len()],
                    svc,
                    g % 200,
                    if g.is_multiple_of(7) { 500 } else { 200 },
                    g % 900,
                    g % 100_000,
                    g % 5000,
                ));
            }
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ts)),
                    Arc::new(StringArray::from(level)),
                    Arc::new(StringArray::from(service)),
                    Arc::new(StringArray::from(ns)),
                    Arc::new(StringArray::from(pod)),
                    Arc::new(StringArray::from(method)),
                    Arc::new(Int64Array::from(status)),
                    Arc::new(StringArray::from(message)),
                ],
            )
            .unwrap();
            let source = synthesize_source(&batch).unwrap();
            batches.push(batch);
            sources.push(source);
            built += n;
        }
        eprintln!(
            "built {rows} rows in {} chunks; threads {thread_list:?}",
            batches.len()
        );

        let fts = vec!["message".to_string()];
        let cs = vec!["kubernetes.namespace.name".to_string()];
        for &threads in &thread_list {
            let mut opts = core_writer_options(&fts, cs.clone(), Vec::new());
            opts.encode_threads = threads;
            let writer_schema = writer_input_schema(&Arc::new((*schema).clone()));
            let mut writer = VixWriter::new(&writer_schema, opts, false);
            let t_push = std::time::Instant::now();
            for (batch, source) in batches.iter().zip(&sources) {
                writer.push_batch_with_source(batch, source, None).unwrap();
            }
            let push_elapsed = t_push.elapsed();
            let t_finish = std::time::Instant::now();
            let (data, stats) = writer.finish_with_stats().unwrap();
            let finish_elapsed = t_finish.elapsed();
            eprintln!(
                "threads={threads:>2}  push(term-accum) {push_elapsed:>8.2?}  \
                 finish(encode) {finish_elapsed:>8.2?}  total {:>8.2?}  \
                 (out {:.1} MiB, {} terms, index {:.1} MiB, docs {:.1} MiB)",
                push_elapsed + finish_elapsed,
                mib(data.len()),
                stats.term_count,
                mib(stats.index_size as usize),
                mib(stats.docs_size as usize),
            );
        }
    }

    /// An input whose index blobs are unreadable falls back to the rebuild
    /// through a docs-only open — and, since its fields are covered by the
    /// healthy input, produces the file a rebuild over pristine inputs
    /// would.
    #[tokio::test]
    async fn merge_falls_back_on_corrupt_index() {
        let schema_fields = || {
            vec![
                Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
                Field::new("log", DataType::Utf8, true),
                Field::new("svc", DataType::Utf8, true),
            ]
        };
        let build = |ts: Vec<i64>, log: Vec<&str>, svc: Vec<&str>| {
            build_core_file(
                schema_fields(),
                vec![
                    Arc::new(Int64Array::from(ts)),
                    Arc::new(StringArray::from(log)),
                    Arc::new(StringArray::from(svc)),
                ],
                &["log".to_string()],
                vec!["svc".to_string()],
                None,
            )
        };
        let file1 = build(vec![100, 90], vec!["error one", "fine"], vec!["api", "db"]);
        let file2 = build(vec![80, 70], vec!["error two", "ok"], vec!["db", "web"]);

        // corrupt the dictionary blob of file1 (located by tag — blob order
        // is not part of the format); the puffin footer and the docs blob
        // stay intact, so a docs-only open still works
        let mut corrupt = file1.to_vec();
        let dict_range = vortex_index::test_support::blob_byte_range(&corrupt, "dict").unwrap();
        for byte in &mut corrupt[dict_range.start..(dict_range.start + 32).min(dict_range.end)] {
            *byte = 0xAB;
        }
        let corrupt = bytes::Bytes::from(corrupt);
        // open is footer-only under the block dictionary: corruption in the
        // dict blob surfaces at the first DICTIONARY touch, not at open —
        // and the merge maps that error to the rebuild fallback below
        let opened = VixReader::open(corrupt.clone()).unwrap();
        assert!(
            opened.for_each_term(&mut |_k, _d, _i| Ok(())).is_err(),
            "corruption did not break the dictionary read"
        );
        assert!(VixDocs::open(corrupt.clone()).is_ok());

        let latest_schema = Schema::new(schema_fields());
        let fts = vec!["log".to_string()];
        let cs = vec!["svc".to_string()];
        let inputs = vec![
            ("bad.vix".to_string(), corrupt),
            ("good.vix".to_string(), file2.clone()),
        ];
        let result = merge_core_files(&as_inputs(&inputs), &latest_schema, &fts, &cs, &[]).unwrap();
        assert!(!result.used_index_merge);

        // equivalent to rebuilding over the pristine bytes (file2 supplies
        // the same term-field set the corrupt input would have)
        let pristine = vec![
            ("f1.vix".to_string(), file1),
            ("good.vix".to_string(), file2),
        ];
        let reference =
            merge_core_files_rebuild(&as_inputs(&pristine), &latest_schema, &fts, &cs, &[])
                .unwrap();
        let result_reader =
            VixReader::open(bytes::Bytes::from(result.output.to_bytes().unwrap())).unwrap();
        let reference_reader =
            VixReader::open(bytes::Bytes::from(reference.output.to_bytes().unwrap())).unwrap();
        assert_core_files_equivalent(&result_reader, &reference_reader, "corrupt");
    }

    /// A pre-fix input stamped `tokenizer = "o2-v1"` merged with a current
    /// `"o2-v2"` file: the property mismatch must reject the index-merge
    /// fast path, and the rebuild re-tokenizes everything from `_source`
    /// with the CURRENT tokenizer — the output is a coherent `"o2-v2"` file
    /// (non-ASCII text becomes findable; old files converge at compaction).
    #[tokio::test]
    async fn merge_with_legacy_tokenizer_input_rebuilds_to_current() {
        let fts = vec!["log".to_string()];
        let fields = || {
            vec![
                Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
                Field::new("log", DataType::Utf8, true),
            ]
        };
        // the "old" file carries non-ASCII text a v1 tokenizer kept whole
        let old_file = build_core_file(
            fields(),
            vec![
                Arc::new(Int64Array::from(vec![100, 90])),
                Arc::new(StringArray::from(vec!["café latte", "用户admin登录"])),
            ],
            &fts,
            vec![],
            None,
        );
        let old_file = bytes::Bytes::from(
            vortex_index::test_support::repack_with_tokenizer_property(&old_file, "o2-v1").unwrap(),
        );
        let new_file = build_core_file(
            fields(),
            vec![
                Arc::new(Int64Array::from(vec![80])),
                Arc::new(StringArray::from(vec!["plain admin login"])),
            ],
            &fts,
            vec![],
            None,
        );

        let latest_schema = Schema::new(fields());
        let inputs = vec![
            ("old.vix".to_string(), old_file),
            ("new.vix".to_string(), new_file),
        ];
        let result = merge_core_files(&as_inputs(&inputs), &latest_schema, &fts, &[], &[]).unwrap();
        assert!(
            !result.used_index_merge,
            "the tokenizer property mismatch must force the rebuild"
        );
        // ... and the rebuild is what merge_core_files_rebuild produces
        let reference =
            merge_core_files_rebuild(&as_inputs(&inputs), &latest_schema, &fts, &[], &[]).unwrap();
        let result_reader =
            VixReader::open(bytes::Bytes::from(result.output.to_bytes().unwrap())).unwrap();
        let reference_reader =
            VixReader::open(bytes::Bytes::from(reference.output.to_bytes().unwrap())).unwrap();
        assert_core_files_equivalent(&result_reader, &reference_reader, "legacy tokenizer");

        // the output file is stamped with the current tokenizer ...
        assert_eq!(
            vortex_index::test_support::tokenizer_property(&result.output.to_bytes().unwrap())
                .unwrap(),
            Some("o2-v2".to_string())
        );
        // ... and its tokens are current-semantics: per-char non-ASCII plus
        // split ASCII runs (merged ts DESC: doc0="café latte",
        // doc1="用户admin登录", doc2="plain admin login")
        let any = |token: &str| VixQuery::TokenAnyField {
            token: token.as_bytes().to_vec(),
        };
        assert_eq!(matching_docs(&result_reader, &any("caf")), vec![0]);
        assert_eq!(matching_docs(&result_reader, &any("é")), vec![0]);
        assert_eq!(matching_docs(&result_reader, &any("latte")), vec![0]);
        assert_eq!(matching_docs(&result_reader, &any("admin")), vec![1, 2]);
        assert_eq!(matching_docs(&result_reader, &any("用")), vec![1]);
        // the v1 whole-run tokens do not exist in the merged dictionary
        assert_eq!(
            matching_docs(&result_reader, &any("café")),
            Vec::<u32>::new()
        );
        assert_eq!(
            matching_docs(&result_reader, &any("用户admin登录")),
            Vec::<u32>::new()
        );
    }

    // ─── Adversarial-review probes (write path / merge / lifecycle audit,
    //     2026-07-23). Tests marked REVIEW FINDING reproduce a shipped
    //     behavior the review flagged; the rest pin invariants the review
    //     verified. ───────────────────────────────────────────────────────

    /// FIXED: non-finite floats (reachable via OTLP double attributes and
    /// VRL math) no longer break the merge differential guarantee.
    ///
    /// `synthesize_source` (arrow-json) stores NaN/Inf as the JSON literal
    /// `null` — `_source` is authoritative — so `index_key_terms` now treats
    /// non-finite float slots as null too: the move-job file, the
    /// index-merge fast path (which copies the inputs' key terms) and the
    /// rebuild (which re-derives from `_source`) all agree the doc has no
    /// value at the path. The docs cs column still stores the real NaN/Inf
    /// (only the key-term derivation changed).
    #[tokio::test]
    async fn review_merge_paths_disagree_on_nan_inf_key_terms() {
        use arrow::array::Float64Array;
        let fields = vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("log", DataType::Utf8, true),
            Field::new("ratio", DataType::Float64, true),
        ];
        let file1 = build_core_file(
            fields.clone(),
            vec![
                Arc::new(Int64Array::from(vec![100, 90])),
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(Float64Array::from(vec![Some(f64::NAN), None])),
            ],
            &[],
            vec![],
            None,
        );
        let file2 = build_core_file(
            fields.clone(),
            vec![
                Arc::new(Int64Array::from(vec![80, 70])),
                Arc::new(StringArray::from(vec!["c", "d"])),
                Arc::new(Float64Array::from(vec![Some(1.5), Some(f64::INFINITY)])),
            ],
            &[],
            vec![],
            None,
        );

        // the write side: NaN becomes a JSON null inside _source ...
        let r1 = VixReader::open(file1.clone()).unwrap();
        let src: serde_json::Value =
            serde_json::from_str(r1.read_source(&[0]).unwrap().value(0)).unwrap();
        assert_eq!(
            src.get("ratio"),
            Some(&serde_json::Value::Null),
            "arrow-json serializes NaN as the literal null"
        );
        // ... and the key-term derivation agrees: the non-finite slot is null
        assert_eq!(matching_docs(&r1, &key_exists("ratio")), Vec::<u32>::new());

        let latest_schema = Schema::new(fields);
        let inputs = vec![("f1.vix".to_string(), file1), ("f2.vix".to_string(), file2)];
        let fast = merge_core_files(&as_inputs(&inputs), &latest_schema, &[], &[], &[]).unwrap();
        assert!(fast.used_index_merge);
        let rebuild =
            merge_core_files_rebuild(&as_inputs(&inputs), &latest_schema, &[], &[], &[]).unwrap();

        // merged order (ts DESC): doc0=NaN, doc1=absent, doc2=1.5, doc3=Inf
        // — both strategies agree: only the finite value keys the doc
        let fast_reader =
            VixReader::open(bytes::Bytes::from(fast.output.to_bytes().unwrap())).unwrap();
        let rebuild_reader =
            VixReader::open(bytes::Bytes::from(rebuild.output.to_bytes().unwrap())).unwrap();
        for (context, reader) in [("fast", &fast_reader), ("rebuild", &rebuild_reader)] {
            assert_eq!(
                matching_docs(reader, &key_exists("ratio")),
                vec![2],
                "{context}: only the finite ratio row has a value at the path"
            );
        }
        assert_core_files_equivalent(&fast_reader, &rebuild_reader, "nan/inf key terms");
    }

    /// Fully tied timestamps: every row of every input shares one
    /// `_timestamp`. The stable tie rule (input order) makes each input one
    /// contiguous run, so the merge must take the offset fast path, keep the
    /// rows in input order, and stay equivalent to the rebuild.
    #[tokio::test]
    async fn review_merge_full_timestamp_ties_stay_stable_and_equivalent() {
        let build = |svc: &str| {
            build_core_file(
                vec![
                    Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
                    Field::new("svc", DataType::Utf8, true),
                ],
                vec![
                    Arc::new(Int64Array::from(vec![500, 500])),
                    Arc::new(StringArray::from(vec![svc, svc])),
                ],
                &[],
                vec!["svc".to_string()],
                None,
            )
        };
        let latest_schema = Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("svc", DataType::Utf8, true),
        ]);
        let inputs = vec![
            ("f1.vix".to_string(), build("a")),
            ("f2.vix".to_string(), build("b")),
            ("f3.vix".to_string(), build("c")),
        ];
        let fast = merge_core_files(
            &as_inputs(&inputs),
            &latest_schema,
            &[],
            &["svc".to_string()],
            &[],
        )
        .unwrap();
        assert!(fast.used_index_merge);
        let rebuild = merge_core_files_rebuild(
            &as_inputs(&inputs),
            &latest_schema,
            &[],
            &["svc".to_string()],
            &[],
        )
        .unwrap();

        let fast_reader =
            VixReader::open(bytes::Bytes::from(fast.output.to_bytes().unwrap())).unwrap();
        let rebuild_reader =
            VixReader::open(bytes::Bytes::from(rebuild.output.to_bytes().unwrap())).unwrap();
        assert_core_files_equivalent(&fast_reader, &rebuild_reader, "full ties");
        assert_eq!(
            read_i64(&fast_reader, TIMESTAMP_COL_NAME),
            vec![500; 6],
            "all rows tie"
        );
        assert_eq!(
            read_strings(&fast_reader, "svc"),
            ["a", "a", "b", "b", "c", "c"]
                .iter()
                .map(|s| Some(s.to_string()))
                .collect::<Vec<_>>(),
            "ties resolve in input order, one contiguous run per input"
        );
    }

    /// Degenerate input shapes the eligibility checks allow: a single-input
    /// merge (offset-0 verbatim postings reuse) and a zero-row input mixed
    /// with a real one. Both must produce files equivalent to the rebuild —
    /// and the single-input merge equivalent to the original file itself.
    #[tokio::test]
    async fn review_merge_single_and_empty_inputs() {
        let fts = vec!["log".to_string()];
        let cs = vec!["svc".to_string()];
        let fields = || {
            vec![
                Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
                Field::new("log", DataType::Utf8, true),
                Field::new("svc", DataType::Utf8, true),
            ]
        };
        let real = build_core_file(
            fields(),
            vec![
                Arc::new(Int64Array::from(vec![100, 90])),
                Arc::new(StringArray::from(vec![Some("error one"), None])),
                Arc::new(StringArray::from(vec!["api", "db"])),
            ],
            &fts,
            cs.clone(),
            None,
        );
        let empty = build_core_file(
            fields(),
            vec![
                Arc::new(Int64Array::from(Vec::<i64>::new())),
                Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
                Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            ],
            &fts,
            cs.clone(),
            None,
        );
        let latest_schema = Schema::new(fields());

        // single input: the merged file must be equivalent to the original
        let single = vec![("real.vix".to_string(), real.clone())];
        let fast = merge_core_files(&as_inputs(&single), &latest_schema, &fts, &cs, &[]).unwrap();
        assert!(fast.used_index_merge);
        let fast_reader =
            VixReader::open(bytes::Bytes::from(fast.output.to_bytes().unwrap())).unwrap();
        let original_reader = VixReader::open(real.clone()).unwrap();
        assert_core_files_equivalent(&fast_reader, &original_reader, "single input");

        // a zero-row input alongside a real one
        let with_empty = vec![
            ("empty.vix".to_string(), empty),
            ("real.vix".to_string(), real),
        ];
        let fast =
            merge_core_files(&as_inputs(&with_empty), &latest_schema, &fts, &cs, &[]).unwrap();
        assert!(fast.used_index_merge);
        assert_eq!(fast.stats.row_count, 2);
        let rebuild =
            merge_core_files_rebuild(&as_inputs(&with_empty), &latest_schema, &fts, &cs, &[])
                .unwrap();
        let fast_reader =
            VixReader::open(bytes::Bytes::from(fast.output.to_bytes().unwrap())).unwrap();
        let rebuild_reader =
            VixReader::open(bytes::Bytes::from(rebuild.output.to_bytes().unwrap())).unwrap();
        assert_core_files_equivalent(&fast_reader, &rebuild_reader, "empty input");
        assert_eq!(read_i64(&fast_reader, TIMESTAMP_COL_NAME), vec![100, 90]);
    }

    /// Column-store type drift across inputs: `code` stored as Utf8 in one
    /// file and Int64 in the other, with the stream schema saying Int64.
    /// Both merge strategies cast the stored column to the target type with
    /// arrow's safe cast — unparsable values become SILENT NULLS in the
    /// merged docs column (the original value survives only inside
    /// `_source`) and the field's dropped value terms land in
    /// `partial_fields`. This pins that the two paths at least agree
    /// (differential holds) and documents the null-out semantics.
    #[tokio::test]
    async fn review_merge_cs_type_drift_nulls_are_consistent() {
        let cs = vec!["code".to_string()];
        let file1 = build_core_file(
            vec![
                Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
                Field::new("code", DataType::Utf8, true),
            ],
            vec![
                Arc::new(Int64Array::from(vec![100, 90])),
                Arc::new(StringArray::from(vec![Some("abc"), Some("123")])),
            ],
            &[],
            cs.clone(),
            None,
        );
        let file2 = build_core_file(
            vec![
                Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
                Field::new("code", DataType::Int64, true),
            ],
            vec![
                Arc::new(Int64Array::from(vec![80, 70])),
                Arc::new(Int64Array::from(vec![Some(7), None])),
            ],
            &[],
            cs.clone(),
            None,
        );
        let latest_schema = Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("code", DataType::Int64, true),
        ]);
        let inputs = vec![("f1.vix".to_string(), file1), ("f2.vix".to_string(), file2)];

        let fast = merge_core_files(&as_inputs(&inputs), &latest_schema, &[], &cs, &[]).unwrap();
        assert!(
            fast.used_index_merge,
            "a typed-conflict field is dropped+partial, not a fast-path rejection"
        );
        let rebuild =
            merge_core_files_rebuild(&as_inputs(&inputs), &latest_schema, &[], &cs, &[]).unwrap();
        let fast_reader =
            VixReader::open(bytes::Bytes::from(fast.output.to_bytes().unwrap())).unwrap();
        let rebuild_reader =
            VixReader::open(bytes::Bytes::from(rebuild.output.to_bytes().unwrap())).unwrap();
        assert_core_files_equivalent(&fast_reader, &rebuild_reader, "cs type drift");

        // "abc" nulled by the safe cast, "123" parsed — silently
        let code = as_int64_array(&fast_reader.read_docs_column("code").unwrap()).unwrap();
        assert_eq!(
            (0..4).map(|i| code.is_valid(i)).collect::<Vec<_>>(),
            vec![false, true, true, false]
        );
        assert_eq!(code.value(1), 123);
        assert_eq!(code.value(2), 7);
        // the original string survives only in _source
        let src: serde_json::Value =
            serde_json::from_str(fast_reader.read_source(&[0]).unwrap().value(0)).unwrap();
        assert_eq!(src.get("code"), Some(&serde_json::json!("abc")));
        // numeric plan fields are term fields now: the string-era raw terms
        // REMAP into the merged dictionary (no drop, no partial) alongside
        // the int-era tagged canonical terms, and both inputs were term-
        // capable, so the merged file keeps full capability
        assert!(!fast_reader.partial_fields().contains("code"));
        assert!(fast_reader.has_term_capability("code"));
        assert_eq!(matching_docs(&fast_reader, &exact("code", "123")), vec![1]);
        assert_eq!(
            matching_docs(&fast_reader, &tagged_numeric("code", "7")),
            vec![2]
        );
    }

    /// REGRESSION (Phase C1, the live 210k->240k wrong-count bug): merging a
    /// file written BEFORE a field joined `column_store_fields` (its value
    /// lives only in `_source`, it has no docs column) with column-bearing
    /// files must NOT null-fill the pre-column rows in the merged docs column.
    /// Both merge strategies derive the missing column from `_source`, so a
    /// read served from the docs column (GROUP BY / TopN / aggregations)
    /// equals the equality count (postings) equals the IS NOT NULL total (key
    /// terms) — the exact consistency triangle that failed on the cluster
    /// (docs-column GROUP BY undercounted while equality and IS NOT NULL were
    /// correct).
    ///
    /// Covers a string field (like `kubernetes.namespace.name`) and an Int64
    /// field, with empty-string, absent-key, and negative-number cases, and
    /// exercises BOTH derivation sites: the `merge_core_files` disjoint fast
    /// path and `merge_core_files_rebuild` (each derives per streamed chunk
    /// in `normalize_merge_chunk`). It FAILS before the fix (docs column
    /// null-filled) and passes after.
    #[tokio::test]
    async fn merge_derives_missing_cs_column_from_source_not_nulls() {
        let cs = vec!["ns".to_string(), "code".to_string()];
        // Input A (newer): ns/code ARE column-stored.
        let with_columns = build_core_file(
            vec![
                Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
                Field::new("ns", DataType::Utf8, true),
                Field::new("code", DataType::Int64, true),
            ],
            vec![
                Arc::new(Int64Array::from(vec![100, 90, 80])),
                Arc::new(StringArray::from(vec![
                    Some("ns-1"),
                    Some("ns-2"),
                    Some("ns-2"),
                ])),
                Arc::new(Int64Array::from(vec![Some(500), Some(-5), Some(200)])),
            ],
            &[],
            cs.clone(),
            None,
        );
        // Input B (older, PRE-COLUMN): ns/code are plain fields -> `_source`
        // only, no docs column. Row order gives an empty string, an absent
        // key, and a negative number.
        let pre_column = build_core_file(
            vec![
                Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
                Field::new("ns", DataType::Utf8, true),
                Field::new("code", DataType::Int64, true),
            ],
            vec![
                Arc::new(Int64Array::from(vec![70, 60, 50, 40])),
                Arc::new(StringArray::from(vec![
                    Some("ns-1"),
                    Some(""),
                    Some("ns-2"),
                    None,
                ])),
                Arc::new(Int64Array::from(vec![Some(-5), None, Some(300), None])),
            ],
            &[],
            vec![], // NOT column-stored in this input
            None,
        );
        let latest_schema = Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("ns", DataType::Utf8, true),
            Field::new("code", DataType::Int64, true),
        ]);
        // disjoint timestamps (A: 100..80 > B: 70..40) -> the fast path
        // stream-copies (stream_inputs_sequential).
        let inputs = vec![
            ("a.vix".to_string(), with_columns),
            ("b.vix".to_string(), pre_column),
        ];

        // Ground truth over all 7 rows (A then B, merged _timestamp DESC):
        //   ns:   ns-1 x2, ns-2 x3, "" x1, absent x1   -> IS NOT NULL = 6
        //   code: 500,-5,200,-5,absent,300,absent       -> IS NOT NULL = 5
        let ns_truth: &[(&str, u64)] = &[("ns-1", 2), ("ns-2", 3), ("", 1)];
        let ns_not_null = 6u64;
        // merged DESC-by-ts order: A(100,90,80) then B(70,60,50,40)
        let code_expected: Vec<Option<i64>> = vec![
            Some(500),
            Some(-5),
            Some(200),
            Some(-5),
            None,
            Some(300),
            None,
        ];
        let code_not_null = 5u64;

        let fast = merge_core_files(&as_inputs(&inputs), &latest_schema, &[], &cs, &[]).unwrap();
        assert!(fast.used_index_merge, "disjoint inputs take the fast path");
        let rebuild =
            merge_core_files_rebuild(&as_inputs(&inputs), &latest_schema, &[], &cs, &[]).unwrap();
        assert!(!rebuild.used_index_merge);

        for (label, result) in [("fast", fast), ("rebuild", rebuild)] {
            let reader =
                VixReader::open(bytes::Bytes::from(result.output.to_bytes().unwrap())).unwrap();
            assert_eq!(reader.row_count(), 7, "{label}: row count");

            // --- string field: the full consistency triangle ---
            // (1) GROUP BY served from the docs column (what a scan reads)
            let ns_column = read_strings(&reader, "ns");
            let mut group_by: std::collections::HashMap<Option<String>, u64> =
                std::collections::HashMap::new();
            for value in &ns_column {
                *group_by.entry(value.clone()).or_default() += 1;
            }
            let docs_non_null: u64 = group_by
                .iter()
                .filter(|(key, _)| key.is_some())
                .map(|(_, count)| *count)
                .sum();
            // (2) IS NOT NULL via key terms
            assert_eq!(
                docs_non_null,
                reader.count(&key_exists("ns")).unwrap(),
                "{label}: ns docs-column GROUP BY total must equal IS NOT NULL"
            );
            assert_eq!(docs_non_null, ns_not_null, "{label}: ns GROUP BY vs truth");
            // (3) per-value: docs GROUP BY == equality (postings) == truth
            for (value, truth) in ns_truth {
                let by_docs = *group_by.get(&Some((*value).to_string())).unwrap_or(&0);
                let by_equality = reader.count(&exact("ns", value)).unwrap();
                assert_eq!(
                    by_docs, by_equality,
                    "{label}: ns={value:?} docs GROUP BY vs equality"
                );
                assert_eq!(by_docs, *truth, "{label}: ns={value:?} vs truth");
            }

            // --- Int64 field: derived values (incl. negative), never nulls ---
            let code = as_int64_array(&reader.read_docs_column("code").unwrap()).unwrap();
            let got: Vec<Option<i64>> = (0..code.len())
                .map(|row| code.is_valid(row).then(|| code.value(row)))
                .collect();
            assert_eq!(
                got, code_expected,
                "{label}: code docs column derived from _source"
            );
            let code_non_null = got.iter().filter(|value| value.is_some()).count() as u64;
            assert_eq!(
                code_non_null,
                reader.count(&key_exists("code")).unwrap(),
                "{label}: code docs non-null count must equal IS NOT NULL"
            );
            assert_eq!(code_non_null, code_not_null, "{label}: code vs truth");
        }
    }

    /// FIXED (was `review_move_job_drops_user_field_named_source`): a user
    /// field literally named `_source` is no longer silently dropped by the
    /// move job — the column is renamed to `_source_field`
    /// (`SOURCE_RENAMED_COL_NAME`) before `_source` synthesis and term
    /// extraction, so the values survive in the stored record, get key/value
    /// terms, and stay queryable. (The logs ingest funnel applies the same
    /// rename, so new WAL data never carries the reserved name; this
    /// writer-side rename covers pre-guard WAL data.) The reserved `_source`
    /// name itself never appears as a field of the stored file.
    #[tokio::test]
    async fn review_move_job_drops_user_field_named_source() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("log", DataType::Utf8, true),
            Field::new(vortex_index::SOURCE_COL_NAME, DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![100, 90])) as ArrayRef,
                Arc::new(StringArray::from(vec!["keep me", "and me"])),
                Arc::new(StringArray::from(vec!["USER-SOURCE-0", "USER-SOURCE-1"])),
            ],
        )
        .unwrap();
        let table = Arc::new(MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap());
        let result = write_core_file_from_tables(
            "review-source-col",
            schema,
            vec![table],
            &[],
            &[],
            &[],
            false,
            0,
        )
        .await
        .unwrap();

        let reader = VixReader::open(bytes::Bytes::from(result.data)).unwrap();
        assert_eq!(reader.row_count(), 2);
        // the synthesized _source carries the value under the renamed key
        let sources = reader.read_source(&[0, 1]).unwrap();
        for row in 0..2 {
            let value: serde_json::Value = serde_json::from_str(sources.value(row)).unwrap();
            let object = value.as_object().unwrap();
            assert!(
                object.keys().all(|k| k != vortex_index::SOURCE_COL_NAME),
                "the reserved _source key must not appear inside row {row}: {object:?}"
            );
            assert_eq!(
                object.get(SOURCE_RENAMED_COL_NAME),
                Some(&serde_json::json!(format!("USER-SOURCE-{row}"))),
                "row {row} lost the user value"
            );
            assert!(object.contains_key("log"));
        }
        // the renamed field is fully indexed: key term + exact value lookup
        assert_eq!(
            matching_docs(&reader, &key_exists(SOURCE_RENAMED_COL_NAME)),
            vec![0, 1]
        );
        assert_eq!(
            matching_docs(&reader, &exact(SOURCE_RENAMED_COL_NAME, "USER-SOURCE-0")),
            vec![0]
        );
        // and the reserved name has no key term
        assert_eq!(
            matching_docs(&reader, &key_exists(vortex_index::SOURCE_COL_NAME)),
            Vec::<u32>::new()
        );
        let coverage = reader.keys_with_prefix("").unwrap();
        assert!(coverage.iter().all(|(path, _)| path != "_source"));
        assert!(coverage.iter().any(|(path, _)| path == "_source_field"));
    }

    /// Stats-fix regression (live problem: file_list rows with
    /// min_ts/max_ts = 0): the FileMeta mapping trusts the writer's
    /// data-derived range, never the upstream (WAL footer / input file_list)
    /// metadata — and a meta that would STILL be degenerate is a hard error,
    /// never a published row.
    #[test]
    fn apply_core_stats_to_meta_overrides_degenerate_ranges() {
        let stats = VixWriterStats {
            row_count: 4,
            term_count: 10,
            index_size: 128,
            docs_size: 512,
            min_ts: 1_700_000_000_000_000,
            max_ts: 1_700_000_400_000_000,
        };
        // a WAL-meta-degenerate input (the live bug shape: min 0, max real)
        let mut meta = FileMeta {
            min_ts: 0,
            max_ts: 1_700_000_400_000_000,
            records: 4,
            original_size: 1024,
            ..Default::default()
        };
        apply_core_stats_to_meta(&mut meta, 4096, &stats, "test").unwrap();
        assert_eq!(meta.min_ts, 1_700_000_000_000_000);
        assert_eq!(meta.max_ts, 1_700_000_400_000_000);
        assert_eq!(meta.records, 4);
        assert_eq!(meta.compressed_size, 4096);
        assert_eq!(meta.index_size, 128);

        // fully zeroed input meta heals too
        let mut meta = FileMeta {
            records: 4,
            ..Default::default()
        };
        apply_core_stats_to_meta(&mut meta, 4096, &stats, "test").unwrap();
        assert_eq!(
            (meta.min_ts, meta.max_ts),
            (1_700_000_000_000_000, 1_700_000_400_000_000)
        );

        // a records count disagreeing with the stored rows heals from the
        // data as well (records is authoritative alongside the range —
        // cleansing callers pre-adjust, so a residual mismatch is corrected
        // and warned as an anomaly)
        let mut meta = FileMeta {
            min_ts: stats.min_ts,
            max_ts: stats.max_ts,
            records: 9,
            ..Default::default()
        };
        apply_core_stats_to_meta(&mut meta, 4096, &stats, "test").unwrap();
        assert_eq!(meta.records, 4);

        // an empty file (row_count 0) keeps whatever the caller had — there
        // is no data range to assert
        let empty = VixWriterStats::default();
        let mut meta = FileMeta {
            min_ts: 7,
            max_ts: 9,
            ..Default::default()
        };
        apply_core_stats_to_meta(&mut meta, 64, &empty, "test").unwrap();
        assert_eq!((meta.min_ts, meta.max_ts), (7, 9));
    }

    /// The guard of the live regression: a meta claiming records while the
    /// writer stored nothing (stats cannot heal it) — and a data-degenerate
    /// stats range — must FAIL, with no way to reach a DB write. Also pins
    /// the writer-level guard: pushing a `_timestamp = 0` row refuses at
    /// `finish` (error, not warn). The writer-API leg deliberately BYPASSES
    /// the producers' cleansing (which would drop the row before the writer
    /// ever saw it — `merge_cleanses_zero_timestamp_rows` /
    /// `move_job_drops_zero_timestamp_rows`): the guard is the
    /// defense-in-depth layer for degeneracy reaching the writer directly,
    /// which after cleansing indicates a NEW bug, not old data.
    #[test]
    fn degenerate_meta_or_zero_ts_data_fails_loudly() {
        // fold-side degeneracy: records > 0 from the input metas, writer
        // stored zero rows -> stats cannot heal -> hard error
        let empty_stats = VixWriterStats::default();
        let mut meta = FileMeta {
            records: 12,
            ..Default::default()
        };
        let err = apply_core_stats_to_meta(&mut meta, 64, &empty_stats, "test-guard")
            .expect_err("a 12-record meta with a zeroed range must be refused");
        assert!(
            err.to_string().contains("degenerate time range"),
            "unexpected error: {err}"
        );

        // stats-side degeneracy (defense in depth: the writer's own finish
        // guard makes this shape unreachable, but the meta fold re-checks)
        let zero_stats = VixWriterStats {
            row_count: 3,
            min_ts: 0,
            max_ts: 1_700_000_400_000_000,
            ..Default::default()
        };
        let mut meta = FileMeta {
            records: 3,
            min_ts: 1_700_000_000_000_000,
            max_ts: 1_700_000_400_000_000,
            ..Default::default()
        };
        let err = apply_core_stats_to_meta(&mut meta, 64, &zero_stats, "test-guard")
            .expect_err("a zero-min stats range must be refused");
        assert!(
            err.to_string().contains("degenerate time range"),
            "unexpected error: {err}"
        );

        // writer-level guard: an actual stored `_timestamp = 0` row makes
        // `finish` itself fail loudly — the corrupt file is never built
        let schema = Arc::new(Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("log", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1_700_000_000_000_000, 0])),
                Arc::new(StringArray::from(vec!["fine", "epoch-zero"])),
            ],
        )
        .unwrap();
        let source = synthesize_source(&batch).unwrap();
        let mut writer =
            VixWriter::new(&schema, core_writer_options(&[], vec![], Vec::new()), false);
        writer
            .push_batch_with_source(&batch, &source, None)
            .unwrap();
        let err = writer
            .finish()
            .expect_err("a stored zero timestamp must refuse to finish");
        assert!(
            err.to_string().contains("degenerate _timestamp range"),
            "unexpected error: {err}"
        );
    }

    /// Build a core file whose WRITER PLAN misses some batch columns — the
    /// shape of files written before those fields were value-indexed: their
    /// rows carry key terms and `_source` values, but no value terms.
    fn build_core_file_with_plan(
        plan_fields: Vec<Field>,
        batch_fields: Vec<Field>,
        columns: Vec<ArrayRef>,
    ) -> bytes::Bytes {
        let plan_schema = Arc::new(Schema::new(plan_fields));
        let batch_schema = Arc::new(Schema::new(batch_fields));
        let batch = RecordBatch::try_new(batch_schema, columns).unwrap();
        let source = synthesize_source(&batch).unwrap();
        let mut writer = VixWriter::new(
            &plan_schema,
            core_writer_options(&[], vec![], Vec::new()),
            false,
        );
        writer
            .push_batch_with_source(&batch, &source, None)
            .unwrap();
        bytes::Bytes::from(writer.finish().unwrap())
    }

    fn tagged_numeric(field: &str, canonical: &str) -> VixQuery {
        VixQuery::Exact {
            field: field.to_string(),
            token: vortex_index::numeric_value_token(canonical),
        }
    }

    /// Merge-capability correctness (task-critical): merging an OLD-style
    /// file (numeric field carried without value terms) with a NEW-style one
    /// must not let the merged fields table claim term capability the
    /// dictionary cannot honor — the field is DEMOTED (per-field capability
    /// intersection), queries fall back to the scan filter, and the
    /// `_source` ground truth survives intact. A rebuild converges the same
    /// rows to full capability.
    #[tokio::test]
    async fn merge_demotes_numeric_capability_and_rebuild_converges() {
        let ts_field = || Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false);
        let svc_field = || Field::new("svc", DataType::Utf8, true);
        let code_field = || Field::new("code", DataType::Int64, true);

        // OLD-style: plan without `code`, rows with it
        let old_file = build_core_file_with_plan(
            vec![ts_field(), svc_field()],
            vec![ts_field(), svc_field(), code_field()],
            vec![
                Arc::new(Int64Array::from(vec![100, 90])),
                Arc::new(StringArray::from(vec![Some("api"), Some("db")])),
                Arc::new(Int64Array::from(vec![Some(38), Some(7)])),
            ],
        );
        // NEW-style: `code` fully term-indexed
        let new_file = build_core_file_with_plan(
            vec![ts_field(), svc_field(), code_field()],
            vec![ts_field(), svc_field(), code_field()],
            vec![
                Arc::new(Int64Array::from(vec![80, 70])),
                Arc::new(StringArray::from(vec![Some("api"), Some("web")])),
                Arc::new(Int64Array::from(vec![Some(38), Some(9)])),
            ],
        );
        {
            let old_reader = VixReader::open(old_file.clone()).unwrap();
            assert!(!old_reader.has_term_capability("code"));
            let new_reader = VixReader::open(new_file.clone()).unwrap();
            assert!(new_reader.has_term_capability("code"));
        }

        let latest_schema = Schema::new(vec![ts_field(), svc_field(), code_field()]);
        let inputs = vec![
            ("old.vix".to_string(), old_file.clone()),
            ("new.vix".to_string(), new_file),
        ];

        // FAST-path merge: capability intersection demotes `code`
        let merged = merge_core_files(&as_inputs(&inputs), &latest_schema, &[], &[], &[]).unwrap();
        assert!(merged.used_index_merge, "old+new must keep the fast path");
        assert_eq!((merged.stats.min_ts, merged.stats.max_ts), (70, 100));
        let reader =
            VixReader::open(bytes::Bytes::from(merged.output.to_bytes().unwrap())).unwrap();
        assert!(
            !reader.has_term_capability("code"),
            "a term claim would silently miss the old input's rows"
        );
        assert!(!reader.partial_fields().contains("code"));
        // per-field lookups error -> the search layer skips + filters back
        assert!(reader.eval(&tagged_numeric("code", "38")).is_err());
        // the string field keeps full capability
        assert!(reader.has_term_capability("svc"));
        assert_eq!(matching_docs(&reader, &exact("svc", "api")), vec![0, 2]);
        // FILTER-BACK ground truth: the scan-identical extraction over the
        // merged `_source` sees every code value (old rows included) — the
        // results a query gets through the skip + filter-back path
        let source = reader.read_source(&[0, 1, 2, 3]).unwrap();
        let derived = derive_cs_column_from_source(&source, "code", &DataType::Int64).unwrap();
        let derived = derived
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone();
        let matches_38: Vec<u32> = (0..derived.len())
            .filter(|&row| derived.is_valid(row) && derived.value(row) == 38)
            .map(|row| row as u32)
            .collect();
        assert_eq!(matches_38, vec![0, 2], "rows ts=100 (old) and ts=80 (new)");

        // REBUILD of the same inputs: terms re-derived from `_source`
        // (registry-driven plan enrichment gives `code` a field id even when
        // NO input value-indexed it), converging old files to full capability
        for inputs in [
            inputs.clone(),
            vec![
                ("old.vix".to_string(), old_file.clone()),
                ("old2.vix".to_string(), old_file.clone()),
            ],
        ] {
            let rebuilt =
                merge_core_files_rebuild(&as_inputs(&inputs), &latest_schema, &[], &[], &[])
                    .unwrap();
            assert!(!rebuilt.used_index_merge);
            let reader =
                VixReader::open(bytes::Bytes::from(rebuilt.output.to_bytes().unwrap())).unwrap();
            assert!(reader.has_term_capability("code"));
            let hits = matching_docs(&reader, &tagged_numeric("code", "38"));
            assert!(!hits.is_empty(), "rebuild must index old numeric rows");
        }

        // ... while the old+old FAST path demotes (no capable input at all)
        let both_old = vec![
            ("old.vix".to_string(), old_file.clone()),
            ("old2.vix".to_string(), old_file),
        ];
        let merged =
            merge_core_files(&as_inputs(&both_old), &latest_schema, &[], &[], &[]).unwrap();
        assert!(merged.used_index_merge);
        let reader =
            VixReader::open(bytes::Bytes::from(merged.output.to_bytes().unwrap())).unwrap();
        assert!(!reader.has_term_capability("code"));
        assert!(reader.has_term_capability("svc"));
    }

    /// Mixed-type parity (task-critical): for one field holding numbers AND
    /// strings across rows, the index-served bitmap of a numeric comparison
    /// equals the scan-side filter ground truth — the SAME `json_get_*`
    /// extraction + comparison the filter-back path evaluates
    /// (`derive_cs_column_from_source` builds the scan's expression by
    /// construction).
    #[tokio::test]
    async fn numeric_index_probes_match_json_get_ground_truth() {
        use crate::search::index::{Condition, NumericKind};

        let schema = Arc::new(Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("credit", DataType::Float64, true),
            Field::new("code", DataType::Int64, true),
            Field::new("ok", DataType::Boolean, true),
        ]));
        // one field, many stored shapes: float, int, canonical strings,
        // junk string, absent
        let sources = StringArray::from_iter_values([
            r#"{"_timestamp":100,"credit":38.0,"code":38,"ok":true}"#,
            r#"{"_timestamp":99,"credit":38,"code":38.0,"ok":"true"}"#,
            r#"{"_timestamp":98,"credit":"38.0","code":"38","ok":1}"#,
            r#"{"_timestamp":97,"credit":"38","code":"38.0","ok":false}"#,
            r#"{"_timestamp":96,"credit":"x","code":"x","ok":"yes"}"#,
            r#"{"_timestamp":95,"credit":39.5,"code":39}"#,
            r#"{"_timestamp":94}"#,
        ]);
        let timestamps = Int64Array::from(vec![100, 99, 98, 97, 96, 95, 94]);
        let mut writer =
            VixWriter::new(&schema, core_writer_options(&[], vec![], Vec::new()), false);
        writer
            .push_docs_rows(&timestamps, &[], &sources, None)
            .unwrap();
        let reader = VixReader::open(bytes::Bytes::from(writer.finish().unwrap())).unwrap();

        let tokenize = |_: &str| Vec::<String>::new();
        let index_rows = |condition: &Condition| -> Vec<u32> {
            let query = condition.to_vix_query(&tokenize).unwrap();
            matching_docs(&reader, &query)
        };

        // Float64 registry field: json_get_float coerces ints, floats and
        // f64-parseable strings
        let derived = derive_cs_column_from_source(&sources, "credit", &DataType::Float64).unwrap();
        let derived = derived
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap()
            .clone();
        let truth: Vec<u32> = (0..derived.len())
            .filter(|&row| derived.is_valid(row) && derived.value(row) == 38.0)
            .map(|row| row as u32)
            .collect();
        assert_eq!(truth, vec![0, 1, 2, 3], "ground-truth sanity");
        assert_eq!(
            index_rows(&Condition::NumericCmp(
                "credit".into(),
                vec!["38.0".into()],
                false,
                NumericKind::Float,
            )),
            truth
        );

        // Int64 registry field: json_get_int REJECTS floats and float-text
        // strings — the index must not match them either
        let derived = derive_cs_column_from_source(&sources, "code", &DataType::Int64).unwrap();
        let derived = derived
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone();
        let truth: Vec<u32> = (0..derived.len())
            .filter(|&row| derived.is_valid(row) && derived.value(row) == 38)
            .map(|row| row as u32)
            .collect();
        assert_eq!(truth, vec![0, 2], "ground-truth sanity");
        assert_eq!(
            index_rows(&Condition::NumericCmp(
                "code".into(),
                vec!["38".into()],
                false,
                NumericKind::Int,
            )),
            truth
        );

        // Boolean registry field: json_get_bool accepts booleans and
        // "true"/"false" strings; numbers and other strings are NULL
        let derived = derive_cs_column_from_source(&sources, "ok", &DataType::Boolean).unwrap();
        let derived = derived
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .clone();
        let truth: Vec<u32> = (0..derived.len())
            .filter(|&row| derived.is_valid(row) && derived.value(row))
            .map(|row| row as u32)
            .collect();
        assert_eq!(truth, vec![0, 1], "ground-truth sanity");
        assert_eq!(
            index_rows(&Condition::NumericCmp(
                "ok".into(),
                vec!["true".into()],
                false,
                NumericKind::Bool,
            )),
            truth
        );
    }
}
