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

//! DataFusion scan support for core files (`.vix`).
//!
//! A core file is a puffin container whose `docs` blob stores one row per
//! record: `_timestamp`, the column-store fields, the full flattened record
//! as `_source` JSON, and optionally `_original`/`_o2_id`. The table exposed
//! to DataFusion uses the LOGICAL stream schema (all queried fields, registry
//! types); this module maps it onto the physical docs columns per file:
//!
//! - a logical column that exists in the file's docs blob is read natively (cast to the registry
//!   type when the stored type differs);
//! - every other logical column is extracted as `json_get_{str|int|float|bool}(_source, '<literal
//!   dotted name>')` (datafusion-functions-json) cast to the registry type — `_source` is fetched
//!   only when at least one such column is referenced;
//! - the per-file row selection produced by the inverted index ([`VixScanSelection`], attached by
//!   `generate_access_plan`) and the query `_timestamp` range push down into the vortex scan of the
//!   docs blob.
//!
//! Wiring mirrors the other formats: [`VixCoreFormat`] (a DataFusion
//! [`FileFormat`]) is constructed in `exec.rs::build_table_for_format` for
//! `config::FileFormat::Vix` file groups; [`VixCoreSource`] follows the
//! `JsonSource` pattern (simple column selection in the opener, remainder
//! expressions applied by [`ProjectionOpener`]).

use std::{fmt, sync::Arc};

use arrow::{
    array::{Array, ArrayRef, Int64Array, RecordBatchOptions, StringArray, new_null_array},
    buffer::BooleanBuffer,
    datatypes::{DataType, Field, FieldRef, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    common::{
        ColumnStatistics, DataFusionError, GetExt, Result, Statistics, config::ConfigOptions,
        stats::Precision,
    },
    datasource::{
        file_format::{
            FileFormat as DataFusionFileFormat, file_compression_type::FileCompressionType,
        },
        listing::PartitionedFile,
        physical_plan::{FileOpenFuture, FileOpener, FileScanConfig, FileSource},
        projection::{ProjectionOpener, SplitProjection},
        source::DataSourceExec,
        table_schema::TableSchema,
    },
    execution::memory_pool::{MemoryConsumer, MemoryPool},
    logical_expr::ScalarUDF,
    physical_expr::{PhysicalExpr, ScalarFunctionExpr, projection::ProjectionExprs},
    physical_plan::{
        ExecutionPlan,
        expressions::{CastExpr, Column, Literal},
        metrics::ExecutionPlanMetricsSet,
    },
    scalar::ScalarValue,
};
use futures::{FutureExt, StreamExt};
use object_store::{GetOptions, ObjectMeta, ObjectStore};
use tokio_stream::wrappers::ReceiverStream;
use vortex_index::{
    ColumnBound, NumScalar, SOURCE_COL_NAME, TIMESTAMP_COL_NAME, VixDocs, VixRangeSource,
};

use super::vortex_support::VORTEX_RUNTIME;
use crate::vix::source::{StoreRangeSource, VixReadMode, vix_read_mode};

/// File extension (without the dot) of core files.
const VIX_EXT: &str = "vix";

/// Per-file row selection for a core-file scan, attached as a typed
/// [`PartitionedFile`] extension by `generate_access_plan` (the counterpart
/// of `VortexAccessPlan` / `ParquetAccessPlan` for `.vix` files). One bit per
/// docs-blob row; only the set rows are decoded.
#[derive(Debug, Clone)]
pub struct VixScanSelection {
    pub row_ids: Arc<BooleanBuffer>,
}

/// DataFusion [`FileFormat`] for core `.vix` files.
#[derive(Debug, Default)]
pub struct VixCoreFormat {
    /// The query time range (`[start, end)`), pushed into the vortex scan of
    /// every file (zone-map pruned). The same bounds are re-applied by the
    /// combined filter above the scan, so this is a pure early-out.
    timestamp_filter: Option<(i64, i64)>,
    /// Numeric column conjuncts extracted from the plan's FilterExec
    /// (see [`inject_vix_numeric_bounds`]) — pushed into every file's
    /// vortex scan (per-chunk stats pruning; ranged sources skip the
    /// FETCH too) and checked against file-level stats for whole-file
    /// skips. Conservative-only: the FilterExec above re-applies the
    /// predicate on every returned row.
    column_bounds: Vec<ColumnBound>,
}

impl VixCoreFormat {
    pub fn new(timestamp_filter: Option<(i64, i64)>) -> Self {
        Self {
            timestamp_filter,
            column_bounds: Vec::new(),
        }
    }
}

impl GetExt for VixCoreFormat {
    fn get_ext(&self) -> String {
        VIX_EXT.to_string()
    }
}

/// The ranged source over `meta`, when ranged reads are wanted, configured
/// (`ZO_VIX_READ_MODE=ranged`) and possible (known size, tokio runtime at
/// hand). `None` falls back to whole-object bytes.
fn docs_range_source(
    store: &Arc<dyn ObjectStore>,
    meta: &ObjectMeta,
    wants_ranged: bool,
) -> Option<Arc<dyn VixRangeSource>> {
    if !wants_ranged || vix_read_mode() != VixReadMode::Ranged || meta.size == 0 {
        return None;
    }
    let handle = tokio::runtime::Handle::try_current().ok()?;
    Some(Arc::new(StoreRangeSource::new(
        Arc::clone(store),
        meta.location.clone(),
        meta.size,
        handle,
    )))
}

/// Open the docs blob of one object. In ranged mode this opens it over
/// range fetches (puffin footer + docs-blob footer only — plan-time
/// schema/stats inference never downloads objects); otherwise the complete
/// bytes are fetched (served from the local file-data cache by the
/// `memory:///` / `wal:///` adapters).
async fn open_docs(store: &Arc<dyn ObjectStore>, meta: &ObjectMeta) -> Result<VixDocs> {
    let location = meta.location.clone();
    if let Some(source) = docs_range_source(store, meta, true) {
        // ranged opens block on fetches: keep them off the async runtime
        return VORTEX_RUNTIME
            .spawn_blocking(move || VixDocs::open_ranged(source))
            .await
            .map_err(|e| DataFusionError::Execution(format!("vix open task failed: {e}")))?
            .map_err(|e| {
                DataFusionError::Execution(format!("failed to open core .vix file {location}: {e}"))
            });
    }
    let data = store
        .get_opts(&meta.location, GetOptions::default())
        .await?
        .bytes()
        .await?;
    VixDocs::open(data).map_err(|e| {
        DataFusionError::Execution(format!("failed to open core .vix file {location}: {e}"))
    })
}

/// `_timestamp` bounds of a non-empty docs blob. The rows are stored ordered
/// `_timestamp` DESC, so only the first and the last row are read (the row
/// indices are deduped internally when `num_rows == 1`); min/max of the two
/// values is taken for robustness. `None` when the values cannot be read as
/// non-null `Int64` — the column then keeps unknown statistics.
fn timestamp_bounds(docs: &VixDocs, num_rows: usize) -> Option<(i64, i64)> {
    let rows = vec![0, num_rows as u64 - 1];
    let batches = docs
        .read_docs(Some(&[TIMESTAMP_COL_NAME.to_string()]), Some(rows), None)
        .ok()?;
    let mut bounds: Option<(i64, i64)> = None;
    for batch in &batches {
        let column = batch.column_by_name(TIMESTAMP_COL_NAME)?;
        let column = column.as_any().downcast_ref::<Int64Array>()?;
        for value in column.iter() {
            let value = value?;
            bounds = match bounds {
                None => Some((value, value)),
                Some((min, max)) => Some((min.min(value), max.max(value))),
            };
        }
    }
    bounds
}

/// The min/max bounds as `ScalarValue`s of the table schema's `_timestamp`
/// type. That type is `Int64` on our log streams (`exec.rs` forces the
/// `_timestamp` field to non-null `Int64`); other types are cast to
/// defensively, and `None` is returned when the cast fails so the column
/// keeps unknown statistics instead of erroring.
fn timestamp_scalars(
    min: i64,
    max: i64,
    data_type: &DataType,
) -> Option<(ScalarValue, ScalarValue)> {
    let min = ScalarValue::Int64(Some(min));
    let max = ScalarValue::Int64(Some(max));
    if data_type == &DataType::Int64 {
        return Some((min, max));
    }
    match (min.cast_to(data_type), max.cast_to(data_type)) {
        (Ok(min), Ok(max)) => Some((min, max)),
        _ => None,
    }
}

#[async_trait]
impl DataFusionFileFormat for VixCoreFormat {
    fn compression_type(&self) -> Option<FileCompressionType> {
        None
    }

    fn get_ext(&self) -> String {
        GetExt::get_ext(self)
    }

    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> Result<String> {
        match file_compression_type.get_variant() {
            datafusion::common::parsers::CompressionTypeVariant::UNCOMPRESSED => {
                Ok(GetExt::get_ext(self))
            }
            _ => Err(DataFusionError::Internal(
                "core .vix files do not support file-level compression".to_string(),
            )),
        }
    }

    /// The physical schema of the docs blobs. Our table building always
    /// supplies the logical stream schema explicitly, so this only serves
    /// ad-hoc listings.
    async fn infer_schema(
        &self,
        _state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        let mut schemas = Vec::with_capacity(objects.len());
        for object in objects {
            schemas.push(open_docs(store, object).await?.schema().as_ref().clone());
        }
        Ok(Arc::new(Schema::try_merge(schemas)?))
    }

    /// Exact row count from the container properties (one footer parse over
    /// locally cached bytes), plus exact `_timestamp` min/max column
    /// statistics: core files store their rows ordered `_timestamp` DESC, so
    /// the bounds come from a point read of just the first and last row.
    /// They let `split_file_groups_by_statistics` arrange non-overlapping
    /// files into file groups that uphold the `_timestamp` DESC file sort
    /// order declared in `exec.rs::build_table_for_format`, eliding the sort
    /// for `ORDER BY _timestamp DESC` queries. Statistics of every other
    /// column are unknown (and stay unknown for zero-row files).
    async fn infer_stats(
        &self,
        _state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Statistics> {
        let docs = open_docs(store, object).await?;
        let num_rows = usize::try_from(docs.row_count())
            .map_err(|_| DataFusionError::Execution("row count overflow".to_string()))?;
        let mut column_statistics = vec![ColumnStatistics::default(); table_schema.fields().len()];
        // Ranged docs block on chunk fetches while reading the two boundary
        // rows: keep that off the async runtime (pure CPU for cached bytes).
        let bounds = if num_rows > 0 {
            VORTEX_RUNTIME
                .spawn_blocking(move || timestamp_bounds(&docs, num_rows))
                .await
                .map_err(|e| DataFusionError::Execution(format!("vix stats task failed: {e}")))?
        } else {
            None
        };
        if num_rows > 0
            && let Some((ts_index, ts_field)) = table_schema
                .fields()
                .iter()
                .enumerate()
                .find(|(_, field)| field.name() == TIMESTAMP_COL_NAME)
            && let Some((min, max)) =
                bounds.and_then(|(min, max)| timestamp_scalars(min, max, ts_field.data_type()))
        {
            column_statistics[ts_index] = ColumnStatistics {
                null_count: Precision::Exact(0),
                min_value: Precision::Exact(min),
                max_value: Precision::Exact(max),
                ..ColumnStatistics::default()
            };
        }
        Ok(Statistics {
            num_rows: Precision::Exact(num_rows),
            total_byte_size: Precision::Exact(object.size as usize),
            column_statistics,
        })
    }

    async fn create_physical_plan(
        &self,
        state: &dyn Session,
        mut conf: FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Thread the session memory pool into the source so the opener can
        // reserve the object/decode bytes it is about to hold — without
        // this the scan is invisible to the pool and can OOM the process
        // under concurrency with zero pushback.
        let file_source: &dyn std::any::Any = conf.file_source.as_ref();
        if let Some(source) = file_source.downcast_ref::<VixCoreSource>() {
            let mut source = source.clone();
            source.memory_pool = Some(Arc::clone(&state.runtime_env().memory_pool));
            conf.file_source = Arc::new(source);
        }
        Ok(DataSourceExec::from_data_source(conf))
    }

    fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
        Arc::new(
            VixCoreSource::new(table_schema, self.timestamp_filter)
                .with_column_bounds(self.column_bounds.clone()),
        )
    }
}

/// [`FileSource`] for core files: plain column selection is handled by
/// [`VixCoreOpener`]; any remainder projection expressions are applied by the
/// wrapping [`ProjectionOpener`] (the `JsonSource` pattern).
#[derive(Clone)]
pub struct VixCoreSource {
    table_schema: TableSchema,
    batch_size: Option<usize>,
    metrics: ExecutionPlanMetricsSet,
    projection: SplitProjection,
    timestamp_filter: Option<(i64, i64)>,
    /// See [`VixCoreFormat::column_bounds`] (injected post-plan by
    /// [`inject_vix_numeric_bounds`]).
    column_bounds: Vec<ColumnBound>,
    /// Session memory pool (injected by `create_physical_plan`); the opener
    /// reserves its object/decode bytes against it.
    memory_pool: Option<Arc<dyn MemoryPool>>,
}

impl VixCoreSource {
    pub fn new(table_schema: TableSchema, timestamp_filter: Option<(i64, i64)>) -> Self {
        Self {
            projection: SplitProjection::unprojected(&table_schema),
            table_schema,
            batch_size: None,
            metrics: ExecutionPlanMetricsSet::new(),
            timestamp_filter,
            column_bounds: Vec::new(),
            memory_pool: None,
        }
    }

    pub fn with_column_bounds(mut self, column_bounds: Vec<ColumnBound>) -> Self {
        self.column_bounds = column_bounds;
        self
    }
}

impl fmt::Debug for VixCoreSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VixCoreSource")
            .field("timestamp_filter", &self.timestamp_filter)
            .finish()
    }
}

impl FileSource for VixCoreSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        _base_config: &FileScanConfig,
        _partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        // The logical columns this scan must produce (simple column part of
        // the projection); remainder expressions are applied on top by the
        // ProjectionOpener.
        let file_schema = self.table_schema.file_schema();
        let projected_schema = Arc::new(file_schema.project(&self.projection.file_indices)?);

        let opener = Arc::new(VixCoreOpener {
            column_bounds: self.column_bounds.clone(),
            object_store,
            projected_schema,
            timestamp_filter: self.timestamp_filter,
            memory_pool: self.memory_pool.clone(),
        }) as Arc<dyn FileOpener>;

        ProjectionOpener::try_new(self.projection.clone(), opener, file_schema)
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.batch_size = Some(batch_size);
        Arc::new(conf)
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        let mut source = self.clone();
        let new_projection = self.projection.source.try_merge(projection)?;
        source.projection = SplitProjection::new(self.table_schema.file_schema(), &new_projection);
        Ok(Some(Arc::new(source)))
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        Some(&self.projection.source)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn file_type(&self) -> &str {
        VIX_EXT
    }

    /// A puffin container cannot be split by byte ranges — every partition
    /// scans whole files (parallelism comes from distributing files across
    /// groups at listing time).
    fn supports_repartitioning(&self) -> bool {
        false
    }
}

/// Opens one core file and streams record batches of the requested LOGICAL
/// columns, resolving each against the file's physical docs columns.
struct VixCoreOpener {
    /// Numeric conjuncts pushed into the scan + file-level skip check.
    column_bounds: Vec<ColumnBound>,
    object_store: Arc<dyn ObjectStore>,
    /// The logical columns to produce, in output order.
    projected_schema: SchemaRef,
    timestamp_filter: Option<(i64, i64)>,
    /// Session memory pool for the scan's reservation, when threaded through
    /// `VixCoreFormat::create_physical_plan`.
    memory_pool: Option<Arc<dyn MemoryPool>>,
}

/// How the opener reaches one object's docs blob.
enum DocsInput {
    /// The complete object bytes (full scans, cached mode, fallbacks).
    Bytes(bytes::Bytes),
    /// Ranged open — the scan fetches only the chunks it touches.
    Ranged(Arc<dyn VixRangeSource>),
}

impl DocsInput {
    fn open(self) -> anyhow::Result<VixDocs> {
        match self {
            DocsInput::Bytes(data) => VixDocs::open(data),
            DocsInput::Ranged(source) => VixDocs::open_ranged(source),
        }
    }
}

impl FileOpener for VixCoreOpener {
    fn open(&self, file: PartitionedFile) -> Result<FileOpenFuture> {
        let store = Arc::clone(&self.object_store);
        let projected_schema = Arc::clone(&self.projected_schema);
        let timestamp_filter = self.timestamp_filter;
        let column_bounds = self.column_bounds.clone();
        let memory_pool = self.memory_pool.clone();
        // Row selection from the inverted index, if any.
        let selection = file
            .extensions
            .get::<VixScanSelection>()
            .map(|s| Arc::clone(&s.row_ids));

        Ok(async move {
            let location = file.object_meta.location.clone();
            // Full scans of LARGE objects also go ranged: the whole-object
            // GET buffers the entire compressed blob in RAM and reserves it
            // from the pool — a handful of multi-GB consolidated files
            // otherwise exhausts any pool (observed: greedy 12.0/12.0 GB).
            // Chunk-granular ranged decode keeps the reservation at
            // 4 x docs_chunk_bytes regardless of file size.
            let force_ranged_size = config::get_config().common.vix_full_scan_ranged_min_bytes;
            let ranged_wanted = selection.is_some()
                || (force_ranged_size > 0
                    && usize::try_from(file.object_meta.size).unwrap_or(usize::MAX)
                        >= force_ranged_size);
            let wants_ranged =
                docs_range_source(&store, &file.object_meta, ranged_wanted).is_some();

            // Register the scan with the session memory pool BEFORE holding
            // the bytes: whole-object scans reserve the object size, ranged
            // point reads only their decode window; both add 2 chunks for
            // the in-flight batch channel. Failure here is the pool's
            // pushback (ResourcesExhausted), not an OOM later.
            let chunk_bytes = {
                let configured = config::get_config().common.vix_docs_chunk_bytes;
                if configured > 0 {
                    configured
                } else {
                    vortex_index::DEFAULT_DOCS_CHUNK_BYTES
                }
            };
            let estimate = if wants_ranged {
                // fetched windows are chunk-granular; two more chunks cover
                // the streamed batches in flight
                chunk_bytes.saturating_mul(4)
            } else {
                usize::try_from(file.object_meta.size)
                    .unwrap_or(usize::MAX)
                    .saturating_add(chunk_bytes.saturating_mul(2))
            };
            let reservation = match &memory_pool {
                Some(pool) => {
                    let reservation =
                        MemoryConsumer::new(format!("VixCoreOpener[{location}]")).register(pool);
                    reservation.try_grow(estimate)?;
                    Some(reservation)
                }
                None => None,
            };

            // An index row selection means a point read: in ranged mode open
            // the docs blob over range fetches and decode only the selected
            // chunks. Full scans (no selection) keep the single whole-object
            // get — they decode most of the docs blob anyway, and the object
            // is usually already in the local file cache (cache_files
            // enqueues background downloads at index-evaluation time).
            let input = match docs_range_source(&store, &file.object_meta, ranged_wanted) {
                Some(source) => DocsInput::Ranged(source),
                None => DocsInput::Bytes(
                    store
                        .get_opts(&location, GetOptions::default())
                        .await?
                        .bytes()
                        .await?,
                ),
            };

            // The whole scan (footer parse + chunk decode, plus the range
            // fetches it blocks on in ranged mode) runs on the dedicated
            // vortex runtime; batches stream over a small channel so memory
            // stays bounded. A PANIC inside the decode must surface as a
            // stream error — a silently dropped sender would end the stream
            // early and truncate results.
            let (tx, rx) = tokio::sync::mpsc::channel::<Result<RecordBatch>>(2);
            let panic_tx = tx.clone();
            let panic_location = location.clone();
            let handle = VORTEX_RUNTIME.spawn_blocking(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    input.open().and_then(|docs| {
                        // file-level skip: the predicate provably matches
                        // nothing in this file (footer stats only — zero
                        // data reads). Empty stream, reservation released.
                        if file_provably_disjoint(&docs, &column_bounds) {
                            return Ok(());
                        }
                        scan_core_docs(
                            docs,
                            &projected_schema,
                            selection.as_deref(),
                            timestamp_filter,
                            &column_bounds,
                            &mut |batch| {
                                tx.blocking_send(Ok(batch))
                                    .map_err(|_| anyhow::anyhow!("scan consumer dropped"))
                            },
                        )
                    })
                }));
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        // Receiver may already be gone (e.g. limit reached);
                        // nothing to do then.
                        let _ = tx.blocking_send(Err(DataFusionError::Execution(format!(
                            "core .vix scan of {location} failed: {e}"
                        ))));
                    }
                    Err(panic) => {
                        let msg = panic_message(&panic);
                        let _ = tx.blocking_send(Err(DataFusionError::Execution(format!(
                            "core .vix scan of {location} panicked: {msg}"
                        ))));
                        // resume the panic so the runtime's panic accounting
                        // (and abort-on-panic setups) still see it
                        std::panic::resume_unwind(panic);
                    }
                }
            });
            // Watchdog on the JoinHandle: even if the panic escaped the
            // catch (or the task was aborted), the consumer sees an error
            // instead of a clean short stream.
            tokio::spawn(async move {
                if let Err(e) = handle.await
                    && e.is_panic()
                {
                    let _ = panic_tx
                        .send(Err(DataFusionError::Execution(format!(
                            "core .vix scan of {panic_location} panicked: {e}"
                        ))))
                        .await;
                }
            });

            // The reservation lives exactly as long as the stream.
            let stream = ReceiverStream::new(rx).map(move |item| {
                let _hold = &reservation;
                item
            });
            Ok(stream.boxed())
        }
        .boxed())
    }
}

/// Best-effort text of a panic payload.
fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = panic.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

/// Blocking scan of one core file held fully in memory (see
/// [`scan_core_docs`]).
#[cfg(test)]
fn scan_core_file(
    data: bytes::Bytes,
    projected_schema: &SchemaRef,
    selection: Option<&BooleanBuffer>,
    timestamp_filter: Option<(i64, i64)>,
    on_batch: &mut dyn FnMut(RecordBatch) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    scan_core_docs(
        VixDocs::open(data)?,
        projected_schema,
        selection,
        timestamp_filter,
        &[],
        on_batch,
    )
}

/// Blocking scan of one opened docs blob: read the needed physical columns
/// (row selection + `_timestamp` range pushed down) and materialize the
/// requested logical columns batch by batch.
fn scan_core_docs(
    docs: VixDocs,
    projected_schema: &SchemaRef,
    selection: Option<&BooleanBuffer>,
    timestamp_filter: Option<(i64, i64)>,
    column_bounds: &[ColumnBound],
    on_batch: &mut dyn FnMut(RecordBatch) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let plan = LogicalProjectionPlan::new(&docs, projected_schema)?;
    let rows = selection.map(|bits| bits.set_indices().map(|i| i as u64).collect::<Vec<u64>>());
    if let Some(rows) = rows.as_ref()
        && rows.is_empty()
    {
        // The index selected nothing (defensive: such files are usually
        // dropped from the file list before the scan).
        return Ok(());
    }
    // full scans may parallelize one file's chunk decode; point reads
    // (index row selections) stay single-threaded — they touch few chunks
    // and run many files concurrently already
    let decode_threads = if rows.is_none() {
        config::get_config().common.vix_scan_decode_threads
    } else {
        0
    };
    docs.scan_docs_opts(
        Some(&plan.physical_projection),
        rows,
        timestamp_filter,
        column_bounds,
        None,
        decode_threads,
        &mut |batch| on_batch(plan.project(&batch)?),
    )
}

/// `true` when the file's footer statistics PROVE no row can satisfy every
/// bound (exact stats only; any uncertainty keeps the file).
fn file_provably_disjoint(docs: &VixDocs, bounds: &[ColumnBound]) -> bool {
    use std::cmp::Ordering;
    let cmp = |a: NumScalar, b: NumScalar| -> Option<Ordering> {
        match (a, b) {
            (NumScalar::I64(a), NumScalar::I64(b)) => Some(a.cmp(&b)),
            (NumScalar::F64(a), NumScalar::F64(b)) => a.partial_cmp(&b),
            _ => None,
        }
    };
    for bound in bounds {
        let Ok(Some((file_min, file_max))) = docs.column_stats(&bound.column) else {
            continue;
        };
        if let Some((value, inclusive)) = bound.min {
            // need rows with column >(=) value; impossible if file_max <(=) value
            match cmp(file_max, value) {
                Some(Ordering::Less) => return true,
                Some(Ordering::Equal) if !inclusive => return true,
                _ => {}
            }
        }
        if let Some((value, inclusive)) = bound.max {
            match cmp(file_min, value) {
                Some(Ordering::Greater) => return true,
                Some(Ordering::Equal) if !inclusive => return true,
                _ => {}
            }
        }
    }
    false
}

/// The per-file mapping from the requested logical columns to the physical
/// docs columns: which physical columns to read, and one expression per
/// logical output column (native column reference, or typed `json_get_*`
/// extraction from `_source`, cast to the logical type).
struct LogicalProjectionPlan {
    /// Physical docs columns the scan must read (scan output order).
    physical_projection: Vec<String>,
    /// Schema of the scanned physical batch (`physical_projection` order).
    scan_schema: SchemaRef,
    /// One expression per logical output column, evaluated over `scan_schema`.
    exprs: Vec<Arc<dyn PhysicalExpr>>,
    /// The logical output schema.
    output_schema: SchemaRef,
}

impl LogicalProjectionPlan {
    fn new(docs: &VixDocs, projected_schema: &SchemaRef) -> anyhow::Result<Self> {
        let docs_schema = docs.schema();

        // Split the logical columns into physical passthroughs and _source
        // extractions.
        let mut physical_projection: Vec<String> = Vec::new();
        let mut needs_source = false;
        for field in projected_schema.fields() {
            if docs_schema.field_with_name(field.name()).is_ok() {
                if !physical_projection.iter().any(|n| n == field.name()) {
                    physical_projection.push(field.name().clone());
                }
            } else {
                needs_source = true;
            }
        }
        if needs_source && !physical_projection.iter().any(|n| n == SOURCE_COL_NAME) {
            physical_projection.push(SOURCE_COL_NAME.to_string());
        }
        if physical_projection.is_empty() {
            // Zero-column scans (e.g. bare COUNT(*)) still need row counts;
            // `_timestamp` is always present and cheap.
            physical_projection.push(TIMESTAMP_COL_NAME.to_string());
        }

        let scan_fields: Vec<FieldRef> = physical_projection
            .iter()
            .map(|name| {
                docs_schema
                    .field_with_name(name)
                    .map(|f| Arc::new(f.clone()))
                    .map_err(|_| anyhow::anyhow!("docs blob is missing the {name:?} column"))
            })
            .collect::<anyhow::Result<_>>()?;
        let scan_schema = Arc::new(Schema::new(scan_fields));

        let config_options = Arc::new(ConfigOptions::default());
        let exprs = projected_schema
            .fields()
            .iter()
            .map(|field| logical_column_expr(field, &scan_schema, &config_options))
            .collect::<anyhow::Result<Vec<_>>>()?;

        Ok(Self {
            physical_projection,
            scan_schema,
            exprs,
            output_schema: Arc::clone(projected_schema),
        })
    }

    /// Evaluate the logical columns over one scanned physical batch.
    fn project(&self, batch: &RecordBatch) -> anyhow::Result<RecordBatch> {
        let num_rows = batch.num_rows();
        // The scanned batch carries the physical columns in projection
        // order; re-key it against the cached scan schema (types identical).
        debug_assert_eq!(batch.num_columns(), self.scan_schema.fields().len());
        let batch = RecordBatch::try_new_with_options(
            Arc::clone(&self.scan_schema),
            batch.columns().to_vec(),
            &RecordBatchOptions::new().with_row_count(Some(num_rows)),
        )?;
        let mut arrays = Vec::with_capacity(self.exprs.len());
        for expr in &self.exprs {
            arrays.push(expr.evaluate(&batch)?.into_array(num_rows)?);
        }
        Ok(RecordBatch::try_new_with_options(
            Arc::clone(&self.output_schema),
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(num_rows)),
        )?)
    }
}

/// Expression producing one logical column from the scanned physical batch.
fn logical_column_expr(
    field: &Field,
    scan_schema: &SchemaRef,
    config_options: &Arc<ConfigOptions>,
) -> anyhow::Result<Arc<dyn PhysicalExpr>> {
    // Native docs column: reference it, cast when the stored type differs.
    if let Ok(index) = scan_schema.index_of(field.name()) {
        let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new(field.name(), index));
        return Ok(cast_to(
            column,
            scan_schema.field(index).data_type(),
            field.data_type(),
        ));
    }

    // Extracted column: json_get_{str|int|float|bool}(_source, 'name'),
    // cast to the registry type.
    let source_index = scan_schema
        .index_of(SOURCE_COL_NAME)
        .map_err(|_| anyhow::anyhow!("internal: _source not in the physical scan projection"))?;
    let (udf, native_type, safe_cast) = json_extraction_udf(field.data_type());
    let args: Vec<Arc<dyn PhysicalExpr>> = vec![
        Arc::new(Column::new(SOURCE_COL_NAME, source_index)),
        Arc::new(Literal::new(ScalarValue::Utf8(Some(field.name().clone())))),
    ];
    let extraction: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::try_new(
        udf,
        args,
        scan_schema,
        Arc::clone(config_options),
    )?);
    Ok(if safe_cast {
        cast_to_safe(extraction, &native_type, field.data_type())
    } else {
        cast_to(extraction, &native_type, field.data_type())
    })
}

/// The `json_get_*` UDF whose native return type is closest to the logical
/// type, plus that native type (for deciding whether a cast is needed) and
/// whether the cast must be SAFE (invalid values become NULL, matching the
/// json_get_* mismatch semantics, instead of erroring the scan).
fn json_extraction_udf(logical_type: &DataType) -> (Arc<ScalarUDF>, DataType, bool) {
    use datafusion_functions_json::udfs;
    match logical_type {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32 => (udfs::json_get_int_udf(), DataType::Int64, false),
        // UInt64 exceeds json_get_int's i64: values above i64::MAX would
        // extract as NULL while column-store files serve them natively.
        // Extract the raw text and safe-cast for the exact full u64 range
        // (non-numeric text becomes NULL, like a json_get_int mismatch).
        DataType::UInt64 => (udfs::json_as_text_udf(), DataType::Utf8, true),
        DataType::Float16 | DataType::Float32 | DataType::Float64 => {
            (udfs::json_get_float_udf(), DataType::Float64, false)
        }
        DataType::Boolean => (udfs::json_get_bool_udf(), DataType::Boolean, false),
        // Strings and everything else: extract as text and cast.
        _ => (udfs::json_get_str_udf(), DataType::Utf8, false),
    }
}

fn cast_to(expr: Arc<dyn PhysicalExpr>, from: &DataType, to: &DataType) -> Arc<dyn PhysicalExpr> {
    if from == to {
        expr
    } else {
        Arc::new(CastExpr::new(expr, to.clone(), None))
    }
}

/// Like [`cast_to`] but with a SAFE cast: values that do not fit the target
/// type become NULL instead of erroring the whole scan.
fn cast_to_safe(
    expr: Arc<dyn PhysicalExpr>,
    from: &DataType,
    to: &DataType,
) -> Arc<dyn PhysicalExpr> {
    if from == to {
        expr
    } else {
        Arc::new(CastExpr::new(
            expr,
            to.clone(),
            Some(arrow::compute::CastOptions {
                safe: true,
                format_options: Default::default(),
            }),
        ))
    }
}

/// Derive one column-store field's values from a batch of `_source` JSON
/// strings, producing EXACTLY what a query-time scan would extract for a file
/// that lacks the native docs column.
///
/// Compaction ([`crate`] is not the merge crate — this is called from
/// `openobserve-core`'s `core_writer`) uses this when it merges a file written
/// before the field joined `column_store_fields` (so it has no docs column,
/// only the value inside `_source`) with column-bearing files. The merged
/// file materializes the field as ONE native docs column, and a scan of a
/// merged file reads that column directly (the extraction branch below only
/// fires when the column is absent). The column is therefore authoritative at
/// query time, so it MUST hold, for every pre-column row, what `_source`
/// extraction would have produced pre-merge — otherwise `GROUP BY`/`TopN`/
/// aggregations over the merged file silently undercount those rows while
/// equality (postings) and `IS NOT NULL` (key terms) stay correct.
///
/// Parity with the scan is **by construction**: it builds the same
/// [`ScalarFunctionExpr`] over the same [`json_extraction_udf`] selection and
/// applies the same [`cast_to`]/[`cast_to_safe`] as [`logical_column_expr`]'s
/// extraction branch. `target_type` is the type the merged docs column is
/// stored under, which is the stream-schema type — the same type the scan
/// resolves the field to — so the UDF selection matches the scan's. `_source`
/// is a single-level JSON object keyed by the exact dotted field name
/// (nulls/absent omitted), so extraction is a top-level key lookup with no
/// nested traversal.
pub fn derive_cs_column_from_source(
    source: &StringArray,
    field: &str,
    target_type: &DataType,
) -> Result<ArrayRef> {
    let rows = source.len();
    if rows == 0 {
        return Ok(new_null_array(target_type, 0));
    }
    // A one-column batch keyed by `_source`, exactly the shape the scan's
    // extraction branch evaluates against.
    let schema = Arc::new(Schema::new(vec![Field::new(
        SOURCE_COL_NAME,
        DataType::Utf8,
        true,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(source.clone()) as ArrayRef],
    )?;

    let (udf, native_type, safe_cast) = json_extraction_udf(target_type);
    let args: Vec<Arc<dyn PhysicalExpr>> = vec![
        Arc::new(Column::new(SOURCE_COL_NAME, 0)),
        Arc::new(Literal::new(ScalarValue::Utf8(Some(field.to_string())))),
    ];
    let extraction: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::try_new(
        udf,
        args,
        &schema,
        Arc::new(ConfigOptions::default()),
    )?);
    let expr = if safe_cast {
        cast_to_safe(extraction, &native_type, target_type)
    } else {
        cast_to(extraction, &native_type, target_type)
    };
    expr.evaluate(&batch)?.into_array(rows)
}

#[cfg(test)]
mod tests {
    use arrow::array::StringArray;
    use config::utils::vix::docs_blob_from_vix_bytes;
    use datafusion::{
        datasource::{
            listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
            object_store::{DefaultObjectStoreRegistry, ObjectStoreRegistry},
        },
        execution::{runtime_env::RuntimeEnvBuilder, session_state::SessionStateBuilder},
        physical_plan::{collect, displayable},
        prelude::{SessionConfig, SessionContext},
    };
    use object_store::{ObjectStoreExt, memory::InMemory, path::Path};
    use vortex_index::{VixWriter, VixWriterOptions};

    use super::*;

    /// 4 docs, schema `_timestamp` + `level` (term) + `code` (cs, i64);
    /// `http.status` exists only inside `_source`.
    pub(super) fn build_core_file() -> bytes::Bytes {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("level", DataType::Utf8, true),
            Field::new("code", DataType::Int64, true),
        ]));
        let opts = VixWriterOptions {
            fts_field_names: vec![],
            column_store_field_names: vec!["code".to_string()],
            row_group_size: 2,
            ..Default::default()
        };
        let mut writer = VixWriter::new(&schema, opts, false);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1000, 1001, 1002, 1003])),
                Arc::new(StringArray::from(vec![
                    Some("info"),
                    Some("error"),
                    Some("error"),
                    None,
                ])),
                Arc::new(Int64Array::from(vec![
                    Some(200),
                    Some(500),
                    None,
                    Some(301),
                ])),
            ],
        )
        .unwrap();
        let sources = StringArray::from(vec![
            r#"{"_timestamp":1000,"level":"info","code":200,"http.status":"200"}"#,
            r#"{"_timestamp":1001,"level":"error","code":500,"http.status":"500"}"#,
            r#"{"_timestamp":1002,"level":"error","http.status":"500"}"#,
            r#"{"_timestamp":1003,"code":301}"#,
        ]);
        writer
            .push_batch_with_source(&batch, &sources, None)
            .unwrap();
        bytes::Bytes::from(writer.finish().unwrap())
    }

    /// One core file with 10 rows ordered `_timestamp` DESC
    /// (`newest..=newest-9`), all tagged with the same `level` value.
    fn build_desc_core_file(newest: i64, level: &str) -> bytes::Bytes {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("level", DataType::Utf8, true),
        ]));
        let opts = VixWriterOptions {
            column_store_field_names: vec!["level".to_string()],
            row_group_size: 4,
            ..Default::default()
        };
        let timestamps: Vec<i64> = (0..10).map(|i| newest - i).collect();
        let sources: Vec<String> = timestamps
            .iter()
            .map(|ts| format!(r#"{{"_timestamp":{ts},"level":"{level}"}}"#))
            .collect();
        let mut writer = VixWriter::new(&schema, opts, false);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(timestamps)),
                Arc::new(StringArray::from(vec![Some(level); 10])),
            ],
        )
        .unwrap();
        let sources = StringArray::from_iter_values(sources.iter().map(String::as_str));
        writer
            .push_batch_with_source(&batch, &sources, None)
            .unwrap();
        bytes::Bytes::from(writer.finish().unwrap())
    }

    fn build_single_row_core_file(ts: i64) -> bytes::Bytes {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "_timestamp",
            DataType::Int64,
            false,
        )]));
        let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![ts]))],
        )
        .unwrap();
        let sources = StringArray::from(vec![format!(r#"{{"_timestamp":{ts}}}"#)]);
        writer
            .push_batch_with_source(&batch, &sources, None)
            .unwrap();
        bytes::Bytes::from(writer.finish().unwrap())
    }

    pub(super) fn logical_schema() -> SchemaRef {
        // registry view: includes a field that is only in _source, with a
        // non-string registry type for one of them
        Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("level", DataType::Utf8, true),
            Field::new("code", DataType::Int64, true),
            Field::new("http.status", DataType::Utf8, true),
        ]))
    }

    fn scan_all(
        data: bytes::Bytes,
        projected: SchemaRef,
        selection: Option<&BooleanBuffer>,
        ts: Option<(i64, i64)>,
    ) -> Vec<RecordBatch> {
        let mut out = Vec::new();
        scan_core_file(data, &projected, selection, ts, &mut |batch| {
            out.push(batch);
            Ok(())
        })
        .unwrap();
        out
    }

    fn column_strings(batches: &[RecordBatch], name: &str) -> Vec<Option<String>> {
        batches
            .iter()
            .flat_map(|batch| {
                let col = batch.column_by_name(name).unwrap();
                let col = arrow::compute::cast(col, &DataType::Utf8).unwrap();
                let col = col.as_any().downcast_ref::<StringArray>().unwrap().clone();
                (0..col.len())
                    .map(|i| (!col.is_null(i)).then(|| col.value(i).to_string()))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    #[test]
    fn scan_mixes_physical_and_extracted_columns() {
        let batches = scan_all(build_core_file(), logical_schema(), None, None);
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 4);
        // physical passthrough
        assert_eq!(
            column_strings(&batches, "level"),
            vec![
                Some("info".into()),
                Some("error".into()),
                Some("error".into()),
                None
            ]
        );
        // extracted from _source with correct dotted-name literal
        assert_eq!(
            column_strings(&batches, "http.status"),
            vec![
                Some("200".into()),
                Some("500".into()),
                Some("500".into()),
                None
            ]
        );
        // native i64 column with a null
        assert_eq!(
            column_strings(&batches, "code"),
            vec![
                Some("200".into()),
                Some("500".into()),
                None,
                Some("301".into())
            ]
        );
    }

    #[test]
    fn scan_applies_row_selection_and_ts_filter() {
        // rows 1 and 2 match level=error; select them via the bitmap
        let bits = BooleanBuffer::from_iter([false, true, true, false]);
        let batches = scan_all(build_core_file(), logical_schema(), Some(&bits), None);
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
        assert_eq!(
            column_strings(&batches, "http.status"),
            vec![Some("500".into()), Some("500".into())]
        );

        // ts range [1001, 1003) cuts the selection down further
        let batches = scan_all(
            build_core_file(),
            logical_schema(),
            Some(&bits),
            Some((1002, 1003)),
        );
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
        assert_eq!(
            column_strings(&batches, "level"),
            vec![Some("error".into())]
        );
    }

    #[test]
    fn scan_without_columns_reports_row_counts() {
        let projected = Arc::new(Schema::empty());
        let batches = scan_all(build_core_file(), projected, None, None);
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 4);
        for batch in &batches {
            assert_eq!(batch.num_columns(), 0);
        }
    }

    #[test]
    fn scan_extracts_typed_values() {
        // registry says code is Int64 but the file predating the cs setting
        // only has it in _source -> json_get_int path
        let data = {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "_timestamp",
                DataType::Int64,
                false,
            )]));
            let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(vec![1, 2]))],
            )
            .unwrap();
            let sources = StringArray::from(vec![
                r#"{"_timestamp":1,"code":7,"ok":true,"ratio":0.5}"#,
                r#"{"_timestamp":2}"#,
            ]);
            writer
                .push_batch_with_source(&batch, &sources, None)
                .unwrap();
            bytes::Bytes::from(writer.finish().unwrap())
        };
        let projected = Arc::new(Schema::new(vec![
            Field::new("code", DataType::Int64, true),
            Field::new("ok", DataType::Boolean, true),
            Field::new("ratio", DataType::Float64, true),
        ]));
        let batches = scan_all(data, projected, None, None);
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
        let code = batches[0].column_by_name("code").unwrap();
        let code = code.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(code.value(0), 7);
        assert!(code.is_null(1));
    }

    /// Row-store-driven `SELECT *` regression (the live truncation bug): the
    /// registry knows only a SUBSET of the fields the file's records carry —
    /// the star projection reads `_timestamp` + cs columns + `_source` and
    /// the exploded hits still return EVERY field of each record, because
    /// hits are materialized from the record itself, never from the
    /// registry. Also pins the parity contract: hit fields ≡ record
    /// `_source` keys ∪ physical columns.
    #[test]
    fn star_scan_returns_full_records_from_subset_registry() {
        use crate::{
            datafusion::source_synthesis::expand_star_source_hits,
            sql::schema::generate_row_store_star_fields,
        };

        // registry SUBSET: knows `_timestamp` + `level` only — the records
        // in the file also carry `code` and `http.status`
        let mut metadata = std::collections::HashMap::new();
        metadata.insert(
            "settings".to_string(),
            r#"{"column_store_fields":["code"]}"#.to_string(),
        );
        let registry = infra::schema::SchemaCache::new(
            Schema::new(vec![
                Field::new("_timestamp", DataType::Int64, false),
                Field::new("level", DataType::Utf8, true),
            ])
            .with_metadata(metadata),
        );
        // the query references `level` (WHERE) — nothing else
        let columns = hashbrown::HashSet::from(["level".to_string()]);
        let star_fields = generate_row_store_star_fields(&registry, Some(&columns), false, false);
        let names: Vec<&str> = star_fields.iter().map(|f| f.name().as_str()).collect();
        // registry-independent shape: ts + referenced + _source ("code" is
        // configured as cs but absent from this registry version — skipped)
        assert_eq!(names, vec!["_timestamp", "level", SOURCE_COL_NAME]);

        let projected = Arc::new(Schema::new(star_fields));
        let batches = scan_all(build_core_file(), projected, None, None);
        let refs: Vec<&RecordBatch> = batches.iter().collect();
        let mut rows = config::utils::arrow::record_batches_to_json_rows(&refs).unwrap();
        expand_star_source_hits(&mut rows);

        // every record comes back with ITS OWN fields — including the ones
        // the registry never heard of
        let expected = [
            serde_json::json!({"_timestamp":1000,"level":"info","code":200,"http.status":"200"}),
            serde_json::json!({"_timestamp":1001,"level":"error","code":500,"http.status":"500"}),
            serde_json::json!({"_timestamp":1002,"level":"error","http.status":"500"}),
            serde_json::json!({"_timestamp":1003,"code":301}),
        ];
        assert_eq!(rows.len(), expected.len());
        for (row, expected) in rows.iter().zip(expected.iter()) {
            assert_eq!(&serde_json::Value::Object(row.clone()), expected);
        }
    }

    /// Column-store precedence: when a docs cs column and the row's
    /// `_source` text disagree (the documented cs type-drift case), the star
    /// hit carries the PHYSICAL column value — the cs column is
    /// authoritative for reads (DESIGN §8).
    #[test]
    fn star_hits_prefer_cs_column_over_source() {
        use crate::datafusion::source_synthesis::expand_star_source_hits;

        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("code", DataType::Int64, true),
        ]));
        let opts = VixWriterOptions {
            column_store_field_names: vec!["code".to_string()],
            ..Default::default()
        };
        let mut writer = VixWriter::new(&schema, opts, false);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![10, 11])),
                Arc::new(Int64Array::from(vec![Some(1), None])),
            ],
        )
        .unwrap();
        // row 0: the source text drifted from the cs cell; row 1: the cs
        // cell is null while _source still has a value (absent physical
        // values never mask the record)
        let sources = StringArray::from(vec![
            r#"{"_timestamp":10,"code":999,"msg":"a"}"#,
            r#"{"_timestamp":11,"code":7,"msg":"b"}"#,
        ]);
        writer
            .push_batch_with_source(&batch, &sources, None)
            .unwrap();
        let data = bytes::Bytes::from(writer.finish().unwrap());

        let projected = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("code", DataType::Int64, true),
            Field::new(SOURCE_COL_NAME, DataType::Utf8, true),
        ]));
        let batches = scan_all(data, projected, None, None);
        let refs: Vec<&RecordBatch> = batches.iter().collect();
        let mut rows = config::utils::arrow::record_batches_to_json_rows(&refs).unwrap();
        expand_star_source_hits(&mut rows);

        assert_eq!(
            serde_json::Value::Object(rows[0].clone()),
            serde_json::json!({"_timestamp":10,"code":1,"msg":"a"})
        );
        // physical null is omitted from the row, so the record's own value
        // shows through
        assert_eq!(
            serde_json::Value::Object(rows[1].clone()),
            serde_json::json!({"_timestamp":11,"code":7,"msg":"b"})
        );
    }

    /// The config-crate byte readers must expose the same docs blob this
    /// scan reads (they cannot depend on vortex_index, so their footer walk
    /// is independent — pin the two together here).
    #[tokio::test]
    async fn config_byte_readers_see_the_docs_blob() {
        let data = build_core_file();
        let sliced = docs_blob_from_vix_bytes(&data).unwrap();
        assert!(!sliced.is_empty());
        let schema = config::utils::parquet::read_schema_from_bytes(config::FileFormat::Vix, &data)
            .await
            .unwrap();
        assert!(schema.field_with_name("_timestamp").is_ok());
        assert!(schema.field_with_name("_source").is_ok());
        assert!(schema.field_with_name("code").is_ok());
        let (_, batches) =
            config::utils::parquet::read_recordbatch_from_bytes(config::FileFormat::Vix, data)
                .await
                .unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 4);
    }

    /// `infer_stats` must report exact `_timestamp` min/max (plus null count
    /// and row count) so the listing machinery can order file groups by time.
    #[tokio::test]
    async fn infer_stats_reports_exact_timestamp_bounds() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("f1.vix");
        store.put(&path, build_core_file().into()).await.unwrap();
        let meta = store.head(&path).await.unwrap();
        let state = SessionContext::new().state();
        let format = VixCoreFormat::new(None);

        let table_schema = logical_schema();
        let stats = format
            .infer_stats(&state, &store, Arc::clone(&table_schema), &meta)
            .await
            .unwrap();
        assert_eq!(stats.num_rows, Precision::Exact(4));
        assert_eq!(stats.total_byte_size, Precision::Exact(meta.size as usize));
        let ts_index = table_schema.index_of(TIMESTAMP_COL_NAME).unwrap();
        for (index, column) in stats.column_statistics.iter().enumerate() {
            if index == ts_index {
                assert_eq!(
                    column.min_value,
                    Precision::Exact(ScalarValue::Int64(Some(1000)))
                );
                assert_eq!(
                    column.max_value,
                    Precision::Exact(ScalarValue::Int64(Some(1003)))
                );
                assert_eq!(column.null_count, Precision::Exact(0));
            } else {
                // every other column stays unknown
                assert_eq!(column, &ColumnStatistics::default());
            }
        }

        // a single-row file (row indices [0, 0] dedupe) gets min == max
        let path = Path::from("one.vix");
        store
            .put(&path, build_single_row_core_file(42).into())
            .await
            .unwrap();
        let meta = store.head(&path).await.unwrap();
        let stats = format
            .infer_stats(&state, &store, Arc::clone(&table_schema), &meta)
            .await
            .unwrap();
        assert_eq!(stats.num_rows, Precision::Exact(1));
        let ts_stats = &stats.column_statistics[ts_index];
        assert_eq!(
            ts_stats.min_value,
            Precision::Exact(ScalarValue::Int64(Some(42)))
        );
        assert_eq!(
            ts_stats.max_value,
            Precision::Exact(ScalarValue::Int64(Some(42)))
        );

        // a table schema without _timestamp keeps all-unknown column stats
        let no_ts_schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("level", DataType::Utf8, true)]));
        let stats = format
            .infer_stats(&state, &store, no_ts_schema, &meta)
            .await
            .unwrap();
        assert_eq!(stats.column_statistics, vec![ColumnStatistics::default()]);
    }

    /// End-to-end: with per-file `_timestamp` statistics and the declared
    /// `_timestamp` DESC file sort order (as set up by
    /// `exec.rs::build_table_for_format`), `ORDER BY _timestamp DESC LIMIT n`
    /// plans without a SortExec — the ordered file groups are merged by a
    /// SortPreservingMergeExec — and still returns the right rows.
    #[tokio::test]
    async fn order_by_timestamp_desc_elides_the_sort() -> Result<()> {
        // three files with non-overlapping descending time ranges
        let store = Arc::new(InMemory::new());
        for (name, newest, level) in [
            ("data/f1.vix", 300, "a"),
            ("data/f2.vix", 200, "b"),
            ("data/f3.vix", 100, "c"),
        ] {
            let data = build_desc_core_file(newest, level);
            store.put(&Path::from(name), data.into()).await?;
        }

        // a session with statistics-based file-group splitting, resolving
        // test:/// urls to the in-memory store (mirroring how exec.rs
        // registers memory:///)
        let registry = DefaultObjectStoreRegistry::new();
        registry.register_store(&url::Url::parse("test:///").unwrap(), store);
        let runtime_env = RuntimeEnvBuilder::new()
            .with_object_store_registry(Arc::new(registry))
            .build()?;
        let mut session_config = SessionConfig::new().with_target_partitions(3);
        session_config
            .options_mut()
            .execution
            .split_file_groups_by_statistics = true;
        let state = SessionStateBuilder::new()
            .with_config(session_config)
            .with_runtime_env(Arc::new(runtime_env))
            .with_default_features()
            .build();
        let ctx = SessionContext::new_with_state(state);

        // the vix listing exactly as exec.rs::build_table_for_format sets it
        // up: collected statistics + declared _timestamp DESC file sort order
        let table_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("level", DataType::Utf8, true),
        ]));
        let listing_options = ListingOptions::new(Arc::new(VixCoreFormat::new(None)))
            .with_collect_stat(true)
            .with_target_partitions(3)
            .with_file_sort_order(vec![vec![
                datafusion::prelude::col(TIMESTAMP_COL_NAME).sort(false, false),
            ]]);
        let config = ListingTableConfig::new(ListingTableUrl::parse("test:///data/")?)
            .with_listing_options(listing_options)
            .with_schema(Arc::clone(&table_schema));
        ctx.register_table("t", Arc::new(ListingTable::try_new(config)?))?;

        let df = ctx
            .sql("SELECT _timestamp, level FROM t ORDER BY _timestamp DESC LIMIT 5")
            .await?;
        let plan = df.create_physical_plan().await?;
        let rendered = displayable(plan.as_ref()).indent(true).to_string();
        assert!(
            !rendered.contains("SortExec"),
            "the sort must be elided, got plan:\n{rendered}"
        );
        assert!(
            rendered.contains("SortPreservingMergeExec"),
            "the ordered file groups should be merged, got plan:\n{rendered}"
        );

        // and the merged output is really the 5 newest rows, newest first
        let batches = collect(plan, ctx.task_ctx()).await?;
        let mut rows = Vec::new();
        for batch in &batches {
            let ts = batch.column_by_name(TIMESTAMP_COL_NAME).unwrap();
            let ts = ts.as_any().downcast_ref::<Int64Array>().unwrap();
            let level = batch.column_by_name("level").unwrap();
            let level = level.as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..batch.num_rows() {
                rows.push((ts.value(i), level.value(i).to_string()));
            }
        }
        let expected: Vec<(i64, String)> = (0..5).map(|i| (300 - i, "a".to_string())).collect();
        assert_eq!(rows, expected);
        Ok(())
    }
}

/// Adversarial-review proving tests (DataFusion scan integration / SQL
/// surface of the `.vix` core architecture). Each test pins down a behavior
/// discussed in the review; tests asserting a WRONG behavior carry a
/// `FINDING:` comment and assert the current (buggy) output so the suite
/// stays green while the bug is open.
#[cfg(test)]
mod review_tests {
    use arrow::array::{BooleanArray, Float64Array, StringArray};
    use datafusion::{
        datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        execution::{
            memory_pool::GreedyMemoryPool, runtime_env::RuntimeEnvBuilder,
            session_state::SessionStateBuilder,
        },
        physical_plan::{collect as collect_plan, displayable},
        prelude::{SessionConfig, SessionContext},
    };
    use futures::TryStreamExt;
    use object_store::{ObjectStoreExt, memory::InMemory, path::Path};
    use vortex_index::{VixWriter, VixWriterOptions};

    use super::{
        tests::{build_core_file, logical_schema},
        *,
    };
    use crate::datafusion::peak_memory_pool::PeakMemoryPool;

    fn scan_all(
        data: bytes::Bytes,
        projected: SchemaRef,
        selection: Option<&BooleanBuffer>,
        ts: Option<(i64, i64)>,
    ) -> Vec<RecordBatch> {
        let mut out = Vec::new();
        scan_core_file(data, &projected, selection, ts, &mut |batch| {
            out.push(batch);
            Ok(())
        })
        .unwrap();
        out
    }

    /// Build a core file whose `_source` carries typed edge-case values; no
    /// column-store fields, so every logical column is a `json_get_*`
    /// extraction.
    fn build_typed_source_file() -> bytes::Bytes {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "_timestamp",
            DataType::Int64,
            false,
        )]));
        let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1, 2]))],
        )
        .unwrap();
        let sources = StringArray::from(vec![
            // row 0: negatives + type-mismatched values + a u64 > i64::MAX
            r#"{"_timestamp":1,"neg_int":-5,"neg_float":-0.5,"neg_exp":-1.5e3,"big_u64":18446744073709551615,"int_as_str":"123","float_for_int":200.5,"num_for_str":123,"bool_as_str":"true","num_for_bool":1,"pos_int":7,"pos_float":1.5}"#,
            // row 1: everything missing
            r#"{"_timestamp":2}"#,
        ]);
        writer
            .push_batch_with_source(&batch, &sources, None)
            .unwrap();
        bytes::Bytes::from(writer.finish().unwrap())
    }

    /// PARITY GATE: the compaction-time [`derive_cs_column_from_source`] and
    /// the query-time scan extraction (`logical_column_expr` -> the same
    /// `json_get_*` UDF + cast) must produce byte-identical columns for the
    /// SAME `_source`. This is the invariant the merge fix relies on — a
    /// merged file's derived docs column must equal what a scan would have
    /// extracted from `_source` pre-merge. Covers Utf8, Int64, Float64,
    /// Boolean, UInt64, negatives, string<->number coercion, type mismatches
    /// (-> NULL), and missing keys (-> NULL).
    #[test]
    fn derive_cs_column_matches_scan_extraction() {
        let data = build_typed_source_file();
        // the exact `_source` bytes the scan reads for this file
        let docs = VixDocs::open(data.clone()).unwrap();
        let source_batches = docs
            .read_docs(Some(&[SOURCE_COL_NAME.to_string()]), None, None)
            .unwrap();
        // The docs blob may decode `_source` as any string variant; the merge
        // normalizes it to Utf8 (`as_string_array`) before deriving, so match.
        let source_raw = source_batches[0].column_by_name(SOURCE_COL_NAME).unwrap();
        let source = arrow::compute::cast(source_raw, &DataType::Utf8).unwrap();
        let source = source
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone();

        let cases: &[(&str, DataType)] = &[
            ("int_as_str", DataType::Int64),     // string "123" -> 123 (coercion)
            ("neg_int", DataType::Int64),        // -5 (negative-number patch)
            ("float_for_int", DataType::Int64),  // 200.5 -> NULL
            ("pos_int", DataType::Int64),        // 7
            ("neg_float", DataType::Float64),    // -0.5
            ("neg_exp", DataType::Float64),      // -1.5e3
            ("pos_float", DataType::Float64),    // 1.5
            ("big_u64", DataType::UInt64),       // > i64::MAX via as_text + safe cast
            ("bool_as_str", DataType::Boolean),  // "true" -> true
            ("num_for_bool", DataType::Boolean), // 1 -> NULL
            ("num_for_str", DataType::Utf8),     // 123 -> NULL
            ("int_as_str", DataType::Utf8),      // "123" -> "123"
            ("missing_key", DataType::Utf8),     // absent -> NULL
            ("missing_key", DataType::Int64),    // absent -> NULL (numeric)
        ];

        for (field, target) in cases {
            let projected: SchemaRef =
                Arc::new(Schema::new(vec![Field::new(*field, target.clone(), true)]));
            let scan_batches = scan_all(data.clone(), projected, None, None);
            let scanned = scan_batches[0].column_by_name(field).unwrap();

            let derived = derive_cs_column_from_source(&source, field, target).unwrap();

            assert_eq!(
                scanned.to_data(),
                derived.to_data(),
                "field {field:?} as {target:?}: derive must equal the scan extraction"
            );
        }

        // empty input -> empty typed array (no panic on the 0-row fast path)
        let empty = StringArray::from(Vec::<Option<&str>>::new());
        let derived_empty = derive_cs_column_from_source(&empty, "x", &DataType::Int64).unwrap();
        assert_eq!(derived_empty.len(), 0);
        assert_eq!(derived_empty.data_type(), &DataType::Int64);
    }

    /// Projection planning must not read `_source` when every requested
    /// logical column exists natively in the docs blob, must add `_source`
    /// exactly once for any number of extracted columns, and must fall back
    /// to `_timestamp` for zero-column scans.
    #[test]
    fn review_source_read_only_when_needed() {
        let docs = VixDocs::open(build_core_file()).unwrap();

        // all physical (docs blob = _timestamp, code, _source): no _source
        let physical_only: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("code", DataType::Int64, true),
        ]));
        let plan = LogicalProjectionPlan::new(&docs, &physical_only).unwrap();
        assert_eq!(plan.physical_projection, vec!["_timestamp", "code"]);
        assert!(
            !plan
                .physical_projection
                .iter()
                .any(|n| n == SOURCE_COL_NAME),
            "SELECT of only physical columns must not fetch _source"
        );

        // two extracted columns: _source appended exactly once
        let extracted: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("level", DataType::Utf8, true),
            Field::new("http.status", DataType::Utf8, true),
        ]));
        let plan = LogicalProjectionPlan::new(&docs, &extracted).unwrap();
        assert_eq!(
            plan.physical_projection
                .iter()
                .filter(|n| n.as_str() == SOURCE_COL_NAME)
                .count(),
            1
        );

        // zero columns (COUNT(*)): row counts via _timestamp only
        let empty: SchemaRef = Arc::new(Schema::empty());
        let plan = LogicalProjectionPlan::new(&docs, &empty).unwrap();
        assert_eq!(plan.physical_projection, vec![TIMESTAMP_COL_NAME]);
    }

    /// The output columns must follow the REQUESTED projection order even
    /// when it differs from the docs-blob column order (guards the
    /// re-keying of the scanned batch against `scan_schema` by position).
    #[test]
    fn review_projection_order_is_request_order_not_file_order() {
        // file column order is [_timestamp, code, _source]; request reversed
        let projected: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("code", DataType::Int64, true),
            Field::new("_timestamp", DataType::Int64, false),
        ]));
        let batches = scan_all(build_core_file(), projected, None, None);
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 4);
        let code = batches[0].column_by_name("code").unwrap();
        let code = code.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(code.value(0), 200);
        assert!(code.is_null(2));
        let ts = batches[0].column_by_name("_timestamp").unwrap();
        let ts = ts.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(
            (0..ts.len()).map(|i| ts.value(i)).collect::<Vec<_>>(),
            vec![1000, 1001, 1002, 1003]
        );
    }

    /// FIXED (was a finding in the datafusion-functions-json fork,
    /// rev 0df53d7): json_get_int/json_get_float treated `Peek::Minus` as
    /// an error, so every NEGATIVE number stored only in `_source`
    /// extracted as NULL. The local fork patch lets Minus fall through to
    /// jiter's `known_int`/`known_number`, which parse the sign. Also
    /// covers the UInt64 route: values above `i64::MAX` extract exactly
    /// via `json_as_text` + safe cast (json_get_int's i64 cannot hold
    /// them), matching what column-store files serve natively.
    #[test]
    fn review_negative_numbers_from_source_extract_as_null() {
        let projected: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("neg_int", DataType::Int64, true),
            Field::new("neg_float", DataType::Float64, true),
            Field::new("neg_exp", DataType::Float64, true),
            Field::new("big_u64", DataType::UInt64, true),
            Field::new("pos_int", DataType::Int64, true),
            Field::new("pos_float", DataType::Float64, true),
        ]));
        let batches = scan_all(build_typed_source_file(), projected, None, None);
        let batch = &batches[0];

        let neg_int = batch.column_by_name("neg_int").unwrap();
        let neg_int = neg_int.as_any().downcast_ref::<Int64Array>().unwrap();
        let neg_float = batch.column_by_name("neg_float").unwrap();
        let neg_float = neg_float.as_any().downcast_ref::<Float64Array>().unwrap();
        let neg_exp = batch.column_by_name("neg_exp").unwrap();
        let neg_exp = neg_exp.as_any().downcast_ref::<Float64Array>().unwrap();
        let big_u64 = batch.column_by_name("big_u64").unwrap();
        let big_u64 = big_u64
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        let pos_int = batch.column_by_name("pos_int").unwrap();
        let pos_int = pos_int.as_any().downcast_ref::<Int64Array>().unwrap();
        let pos_float = batch.column_by_name("pos_float").unwrap();
        let pos_float = pos_float.as_any().downcast_ref::<Float64Array>().unwrap();

        // positives work
        assert_eq!(pos_int.value(0), 7);
        assert_eq!(pos_float.value(0), 1.5);

        // negatives extract as values, not NULL
        assert_eq!(neg_int.value(0), -5);
        assert_eq!(neg_float.value(0), -0.5);
        assert_eq!(neg_exp.value(0), -1500.0);

        // u64 above i64::MAX round-trips exactly through as_text + safe cast
        assert_eq!(big_u64.value(0), 18446744073709551615);

        // missing keys stay NULL
        assert!(neg_int.is_null(1));
        assert!(big_u64.is_null(1));
    }

    /// Typed extraction semantics for registry-vs-`_source` type mismatches
    /// (schema type evolution). Documents where the json_get path diverges
    /// from the parquet-era arrow-cast behavior:
    /// - string "123" for an Int64 field -> 123 (fork coercion, matches cast)
    /// - float 200.5 for an Int64 field -> NULL (arrow cast gives 200)
    /// - number 123 for a Utf8 field -> NULL (arrow cast gives "123")
    /// - "true" for a Boolean field -> true
    /// - number 1 for a Boolean field -> NULL
    /// - missing key -> NULL (matches missing-column semantics)
    #[test]
    fn review_type_mismatch_extraction_semantics() {
        let projected: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("int_as_str", DataType::Int64, true),
            Field::new("float_for_int", DataType::Int64, true),
            Field::new("num_for_str", DataType::Utf8, true),
            Field::new("bool_as_str", DataType::Boolean, true),
            Field::new("num_for_bool", DataType::Boolean, true),
            Field::new("missing_key", DataType::Utf8, true),
        ]));
        let batches = scan_all(build_typed_source_file(), projected, None, None);
        let batch = &batches[0];

        let int_as_str = batch.column_by_name("int_as_str").unwrap();
        let int_as_str = int_as_str.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(int_as_str.value(0), 123, "string '123' coerces to 123");

        let float_for_int = batch.column_by_name("float_for_int").unwrap();
        let float_for_int = float_for_int.as_any().downcast_ref::<Int64Array>().unwrap();
        // DIVERGENCE: parquet-era cast(200.5 f64 -> i64) = 200; here NULL
        assert!(float_for_int.is_null(0));

        let num_for_str = batch.column_by_name("num_for_str").unwrap();
        let num_for_str = num_for_str.as_any().downcast_ref::<StringArray>().unwrap();
        // DIVERGENCE: parquet-era cast(123 -> utf8) = "123"; here NULL
        assert!(num_for_str.is_null(0));

        let bool_as_str = batch.column_by_name("bool_as_str").unwrap();
        let bool_as_str = bool_as_str.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(bool_as_str.value(0));

        let num_for_bool = batch.column_by_name("num_for_bool").unwrap();
        let num_for_bool = num_for_bool
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(num_for_bool.is_null(0));

        let missing = batch.column_by_name("missing_key").unwrap();
        assert!(missing.is_null(0));
        // row 1 has nothing: all NULL, no errors mid-batch
        assert!(int_as_str.is_null(1));
    }

    /// An all-false selection yields zero rows and zero batches (defensive
    /// path: such files are normally dropped before the scan).
    #[test]
    fn review_empty_selection_returns_no_rows() {
        let bits = BooleanBuffer::new_unset(4);
        let batches = scan_all(build_core_file(), logical_schema(), Some(&bits), None);
        assert!(batches.is_empty());
    }

    /// The pushed-down `_timestamp` range is start-inclusive / end-exclusive
    /// (`[start, end)`), matching `apply_combined_filter` and the index's
    /// `timestamp_range` bitmap. Rows exactly at `start` stay, rows exactly
    /// at `end` drop.
    #[test]
    fn review_ts_filter_boundaries_start_inclusive_end_exclusive() {
        // file rows at ts 1000..=1003
        let batches = scan_all(
            build_core_file(),
            logical_schema(),
            None,
            Some((1001, 1003)),
        );
        let mut ts = Vec::new();
        for batch in &batches {
            let col = batch.column_by_name("_timestamp").unwrap();
            let col = col.as_any().downcast_ref::<Int64Array>().unwrap();
            ts.extend((0..col.len()).map(|i| col.value(i)));
        }
        assert_eq!(
            ts,
            vec![1001, 1002],
            "1001 (== start) in, 1003 (== end) out"
        );
    }

    /// A truncated container must fail the open loudly (footer parse), and
    /// interior corruption of the docs blob must fail the scan loudly —
    /// never silently return partial rows.
    #[test]
    fn review_corrupt_vix_fails_loudly() {
        let data = build_core_file();

        // truncation: cut the tail (footer gone)
        let truncated = data.slice(0..data.len() / 2);
        assert!(
            VixDocs::open(truncated).is_err(),
            "truncated .vix must fail to open"
        );

        // interior corruption: flip bytes inside the docs blob region (the
        // docs blob is the first blob, well before the puffin footer)
        let mut corrupt = data.to_vec();
        let start = 64.min(corrupt.len() / 4);
        for byte in corrupt.iter_mut().skip(start).take(256) {
            *byte = !*byte;
        }
        let corrupt = bytes::Bytes::from(corrupt);
        let result = (|| -> anyhow::Result<usize> {
            let mut rows = 0;
            scan_core_file(corrupt, &logical_schema(), None, None, &mut |batch| {
                rows += batch.num_rows();
                Ok(())
            })?;
            Ok(rows)
        })();
        match result {
            Err(_) => {} // loud failure — required behavior
            Ok(rows) => {
                // If decode "succeeds", it must still never drop rows
                // silently: all 4 rows or bust. (Bit flips inside padding or
                // unused space can legitimately decode.)
                assert_eq!(
                    rows, 4,
                    "corrupt docs blob returned PARTIAL rows silently - this must be an error"
                );
            }
        }
    }

    /// The same corruption surfaced through the real `VixCoreOpener` stream:
    /// the receiver must observe an `Err`, not a clean short stream.
    #[tokio::test(flavor = "multi_thread")]
    async fn review_opener_stream_surfaces_corrupt_file_error() {
        let data = build_core_file();
        let mut corrupt = data.to_vec();
        // wipe out the puffin footer magic: open() must error
        let len = corrupt.len();
        for byte in corrupt.iter_mut().skip(len - 8) {
            *byte = 0;
        }
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("stream/corrupt.vix");
        store
            .put(&path, bytes::Bytes::from(corrupt).into())
            .await
            .unwrap();
        let meta = store.head(&path).await.unwrap();

        let opener = VixCoreOpener {
            column_bounds: Vec::new(),
            object_store: Arc::clone(&store),
            projected_schema: logical_schema(),
            timestamp_filter: None,
            memory_pool: None,
        };
        let mut file = PartitionedFile::new(path.to_string(), meta.size);
        file.object_meta = meta;
        let stream = opener.open(file).unwrap().await.unwrap();
        let collected: Result<Vec<RecordBatch>> = stream.try_collect().await;
        assert!(
            collected.is_err(),
            "corrupt .vix must fail the scan stream loudly, got {:?} batches",
            collected.map(|b| b.len())
        );
        let msg = collected.err().unwrap().to_string();
        assert!(
            msg.contains("corrupt.vix"),
            "error should name the file: {msg}"
        );
    }

    /// A consumer abort (callback error, e.g. the query was cancelled and
    /// the channel receiver dropped) aborts the blocking scan instead of
    /// decoding the rest of the file.
    #[test]
    fn review_callback_error_aborts_scan() {
        let mut calls = 0;
        let result = scan_core_file(
            build_core_file(),
            &logical_schema(),
            None,
            None,
            &mut |_batch| {
                calls += 1;
                Err(anyhow::anyhow!("scan consumer dropped"))
            },
        );
        assert!(result.is_err());
        assert_eq!(calls, 1, "scan must stop after the first callback error");
    }

    /// One core file with 10 rows `_timestamp` DESC starting at `newest`,
    /// stepping by `stride` (interleaves values across files while keeping
    /// every file's time RANGE overlapping the others').
    fn build_strided_core_file(newest: i64, stride: i64) -> bytes::Bytes {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "_timestamp",
            DataType::Int64,
            false,
        )]));
        let timestamps: Vec<i64> = (0..10).map(|i| newest - i * stride).collect();
        let sources: Vec<String> = timestamps
            .iter()
            .map(|ts| format!(r#"{{"_timestamp":{ts}}}"#))
            .collect();
        let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(timestamps))],
        )
        .unwrap();
        let sources = StringArray::from_iter_values(sources.iter().map(String::as_str));
        writer
            .push_batch_with_source(&batch, &sources, None)
            .unwrap();
        bytes::Bytes::from(writer.finish().unwrap())
    }

    /// file_sort_order hazard probe: four MUTUALLY overlapping files while
    /// target_partitions = 2, so `split_groups_by_statistics_with_target_
    /// partitions` needs 4 chains > 2 and ListingTable falls back to the
    /// original (size-balanced) grouping — while `.with_output_ordering`
    /// still declares `_timestamp DESC`. The query must STILL return the
    /// globally correct ORDER BY / LIMIT rows (via a real sort or correct
    /// merge); if this test fails, the declared ordering is a lie and silent
    /// wrong results are possible.
    #[tokio::test]
    async fn review_order_by_desc_overlapping_files_exceeding_target_partitions() -> Result<()> {
        let store = Arc::new(InMemory::new());
        // stride 4, newest 300/299/298/297: values interleave, all four
        // ranges [261..300]-ish mutually overlap -> 4 chains
        for (name, newest) in [
            ("data/f1.vix", 300),
            ("data/f2.vix", 299),
            ("data/f3.vix", 298),
            ("data/f4.vix", 297),
        ] {
            let data = build_strided_core_file(newest, 4);
            store.put(&Path::from(name), data.into()).await?;
        }

        let registry = datafusion::datasource::object_store::DefaultObjectStoreRegistry::new();
        use datafusion::datasource::object_store::ObjectStoreRegistry;
        registry.register_store(&url::Url::parse("test:///").unwrap(), store);
        let runtime_env = RuntimeEnvBuilder::new()
            .with_object_store_registry(Arc::new(registry))
            .build()?;
        let mut session_config = SessionConfig::new().with_target_partitions(2);
        session_config
            .options_mut()
            .execution
            .split_file_groups_by_statistics = true;
        let state = SessionStateBuilder::new()
            .with_config(session_config)
            .with_runtime_env(Arc::new(runtime_env))
            .with_default_features()
            .build();
        let ctx = SessionContext::new_with_state(state);

        let table_schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            TIMESTAMP_COL_NAME,
            DataType::Int64,
            false,
        )]));
        let listing_options = ListingOptions::new(Arc::new(VixCoreFormat::new(None)))
            .with_collect_stat(true)
            .with_target_partitions(2)
            .with_file_sort_order(vec![vec![
                datafusion::prelude::col(TIMESTAMP_COL_NAME).sort(false, false),
            ]]);
        let config = ListingTableConfig::new(ListingTableUrl::parse("test:///data/")?)
            .with_listing_options(listing_options)
            .with_schema(Arc::clone(&table_schema));
        ctx.register_table("t", Arc::new(ListingTable::try_new(config)?))?;

        let df = ctx
            .sql("SELECT _timestamp FROM t ORDER BY _timestamp DESC LIMIT 8")
            .await?;
        let plan = df.create_physical_plan().await?;
        let rendered = displayable(plan.as_ref()).indent(true).to_string();
        let batches = collect_plan(plan, ctx.task_ctx()).await?;
        let mut got = Vec::new();
        for batch in &batches {
            let ts = batch.column_by_name(TIMESTAMP_COL_NAME).unwrap();
            let ts = ts.as_any().downcast_ref::<Int64Array>().unwrap();
            got.extend((0..batch.num_rows()).map(|i| ts.value(i)));
        }
        // global truth: 300, 299, 298, 297, 296, 295, 294, 293
        let expected: Vec<i64> = (0..8).map(|i| 300 - i).collect();
        assert_eq!(
            got, expected,
            "ORDER BY _timestamp DESC over overlapping vix files returned wrong rows;\nplan:\n{rendered}"
        );
        Ok(())
    }

    /// FIXED (was a memory-accounting finding): the vix scan now registers
    /// a reservation with the DataFusion memory pool, sized to the object /
    /// docs-read bytes plus two in-flight chunks. An ample pool observes a
    /// non-zero peak covering at least the object bytes, and a tiny pool
    /// pushes back with a resources error instead of letting the scan
    /// allocate invisibly.
    #[tokio::test]
    async fn review_scan_is_invisible_to_the_memory_pool() -> Result<()> {
        use datafusion::datasource::object_store::ObjectStoreRegistry;

        let data = build_core_file();
        let object_size = data.len();

        let build_ctx = |pool_bytes: usize| -> Result<(SessionContext, Arc<PeakMemoryPool>)> {
            let store = Arc::new(InMemory::new());
            futures::executor::block_on(store.put(&Path::from("data/f1.vix"), data.clone().into()))
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let registry = datafusion::datasource::object_store::DefaultObjectStoreRegistry::new();
            registry.register_store(&url::Url::parse("test:///").unwrap(), store);
            let peak_pool = Arc::new(PeakMemoryPool::new(
                Arc::new(GreedyMemoryPool::new(pool_bytes)),
                "review".to_string(),
            ));
            let runtime_env = RuntimeEnvBuilder::new()
                .with_object_store_registry(Arc::new(registry))
                .with_memory_pool(Arc::clone(&peak_pool) as _)
                .build()?;
            let state = SessionStateBuilder::new()
                .with_config(SessionConfig::new().with_target_partitions(1))
                .with_runtime_env(Arc::new(runtime_env))
                .with_default_features()
                .build();
            let ctx = SessionContext::new_with_state(state);
            let listing_options =
                ListingOptions::new(Arc::new(VixCoreFormat::new(None))).with_collect_stat(true);
            let config = ListingTableConfig::new(ListingTableUrl::parse("test:///data/")?)
                .with_listing_options(listing_options)
                .with_schema(logical_schema());
            ctx.register_table("t", Arc::new(ListingTable::try_new(config)?))?;
            Ok((ctx, peak_pool))
        };

        // ample pool: the scan succeeds and the pool SAW the reservation
        let (ctx, peak_pool) = build_ctx(64 * 1024 * 1024)?;
        let batches = ctx.sql("SELECT * FROM t").await?.collect().await?;
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 4);
        assert!(
            peak_pool.peak_memory() >= object_size,
            "scan reservation must cover the object bytes: peak {} < {object_size}",
            peak_pool.peak_memory(),
        );

        // absurdly small pool: the accounted scan is pushed back with a
        // resources error instead of allocating invisibly
        let (ctx, _peak_pool) = build_ctx(16)?;
        let result = ctx.sql("SELECT * FROM t").await?.collect().await;
        let err = result.expect_err("16-byte pool must reject the scan reservation");
        let msg = err.to_string();
        assert!(
            msg.contains("Failed to allocate") || msg.contains("Resources exhausted"),
            "expected a memory-pool error, got: {msg}"
        );
        Ok(())
    }
}

/// Ranged-mode scan tests: prove the point-read path never fetches whole
/// objects and matches the in-memory scan bit for bit.
#[cfg(test)]
mod ranged_tests {
    use futures::stream::BoxStream;
    use object_store::{
        CopyOptions, Error as ObjectStoreError, GetResult, ListResult, MultipartUpload,
        ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
        Result as ObjectStoreResult, memory::InMemory, path::Path,
    };

    use super::{
        tests::{build_core_file, logical_schema},
        *,
    };
    use crate::vix::source::StoreRangeSource;

    /// Wraps a store and rejects every read that is not a range read (or a
    /// head probe): a whole-object `get` in the ranged path fails the test.
    #[derive(Debug)]
    struct RangeOnlyStore {
        inner: InMemory,
    }

    impl std::fmt::Display for RangeOnlyStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "RangeOnlyStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for RangeOnlyStore {
        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> ObjectStoreResult<GetResult> {
            if options.range.is_none() && !options.head {
                return Err(ObjectStoreError::Generic {
                    store: "RangeOnlyStore",
                    source: format!("whole-object get of {location} in ranged mode").into(),
                });
            }
            self.inner.get_opts(location, options).await
        }

        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> ObjectStoreResult<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, ObjectStoreResult<Path>>,
        ) -> BoxStream<'static, ObjectStoreResult<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> ObjectStoreResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> ObjectStoreResult<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// A selection-driven scan over a ranged docs blob returns exactly the
    /// batches of the in-memory scan while issuing only range reads, and
    /// plan-time stats inference stays ranged too.
    #[tokio::test(flavor = "multi_thread")]
    async fn ranged_scan_and_stats_use_range_reads_only() {
        let data = build_core_file();
        let store: Arc<dyn ObjectStore> = Arc::new(RangeOnlyStore {
            inner: InMemory::new(),
        });
        let path = Path::from("stream/f1.vix");
        store.put(&path, data.clone().into()).await.unwrap();
        let meta = store.head(&path).await.unwrap();

        // in-memory truth
        let bits = BooleanBuffer::from_iter([false, true, true, false]);
        let expect = {
            let mut out = Vec::new();
            scan_core_file(
                data.clone(),
                &logical_schema(),
                Some(&bits),
                None,
                &mut |batch| {
                    out.push(batch);
                    Ok(())
                },
            )
            .unwrap();
            out
        };

        // ranged scan through the guarded store (fails on any whole get)
        let source: Arc<dyn VixRangeSource> = Arc::new(StoreRangeSource::new(
            Arc::clone(&store),
            path.clone(),
            meta.size,
            tokio::runtime::Handle::current(),
        ));
        let projected = logical_schema();
        let got = tokio::task::spawn_blocking(move || {
            let docs = VixDocs::open_ranged(source)?;
            let mut out = Vec::new();
            scan_core_docs(docs, &projected, Some(&bits), None, &[], &mut |batch| {
                out.push(batch);
                Ok(())
            })?;
            anyhow::Ok(out)
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(format!("{got:?}"), format!("{expect:?}"));

        // infer_stats goes ranged as well (default read mode is `ranged`)
        let state = datafusion::prelude::SessionContext::new().state();
        let format = VixCoreFormat::new(None);
        let stats = format
            .infer_stats(&state, &store, logical_schema(), &meta)
            .await
            .unwrap();
        assert_eq!(stats.num_rows, Precision::Exact(4));
        let ts_index = logical_schema().index_of(TIMESTAMP_COL_NAME).unwrap();
        assert_eq!(
            stats.column_statistics[ts_index].min_value,
            Precision::Exact(ScalarValue::Int64(Some(1000)))
        );
    }
}

#[cfg(test)]
mod prod_probe_tests {
    use arrow::array::{Int64Array, StringArray};
    use datafusion::{
        datasource::{
            listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
            object_store::{DefaultObjectStoreRegistry, ObjectStoreRegistry},
        },
        execution::{runtime_env::RuntimeEnvBuilder, session_state::SessionStateBuilder},
        prelude::{SessionConfig, SessionContext},
    };
    use object_store::{ObjectStoreExt, memory::InMemory, path::Path};

    use super::*;

    /// Ad-hoc probe: scan a downloaded PRODUCTION file through the real
    /// format and audit `_source` per row, with and without the narrow
    /// timestamp filter the live query used.
    /// VIX_PROBE_FILE=... VIX_PROBE_TS=... cargo test -p search \
    ///   probe_prod_file_star_source -- --ignored --nocapture
    #[tokio::test]
    #[ignore = "ad-hoc production-file probe; set VIX_PROBE_FILE"]
    async fn probe_prod_file_star_source() -> datafusion::error::Result<()> {
        let path = std::env::var("VIX_PROBE_FILE").expect("set VIX_PROBE_FILE");
        let target_ts: i64 = std::env::var("VIX_PROBE_TS").unwrap().parse().unwrap();
        let data = std::fs::read(&path).unwrap();

        let variants: [(&str, Option<(i64, i64)>, String); 3] = [
            (
                "eq/no-ts-filter",
                None,
                format!("SELECT _timestamp, \"_source\" FROM t WHERE _timestamp = {target_ts}"),
            ),
            (
                "eq/narrow-ts-filter",
                Some((target_ts - 1_000_000, target_ts + 1_000_000)),
                format!("SELECT _timestamp, \"_source\" FROM t WHERE _timestamp = {target_ts}"),
            ),
            (
                "full-scan",
                None,
                "SELECT _timestamp, \"_source\" FROM t".to_string(),
            ),
        ];
        for (label, ts_filter, sql) in variants {
            let store = Arc::new(InMemory::new());
            store
                .put(&Path::from("data/probe.vix"), data.clone().into())
                .await
                .unwrap();
            let registry = DefaultObjectStoreRegistry::new();
            registry.register_store(&url::Url::parse("test:///").unwrap(), store);
            let runtime_env = RuntimeEnvBuilder::new()
                .with_object_store_registry(Arc::new(registry))
                .build()?;
            let state = SessionStateBuilder::new()
                .with_config(SessionConfig::new().with_target_partitions(1))
                .with_runtime_env(Arc::new(runtime_env))
                .with_default_features()
                .build();
            let ctx = SessionContext::new_with_state(state);
            let table_schema: SchemaRef = Arc::new(Schema::new(vec![
                Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
                Field::new(SOURCE_COL_NAME, DataType::Utf8, true),
            ]));
            let listing_options = ListingOptions::new(Arc::new(VixCoreFormat::new(ts_filter)))
                .with_collect_stat(true);
            let config = ListingTableConfig::new(ListingTableUrl::parse("test:///data/")?)
                .with_listing_options(listing_options)
                .with_schema(Arc::clone(&table_schema));
            ctx.register_table("t", Arc::new(ListingTable::try_new(config)?))?;
            let batches = ctx.sql(&sql).await?.collect().await?;
            let (mut rows, mut short) = (0usize, 0usize);
            for b in &batches {
                let ts = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                let src = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
                for i in 0..b.num_rows() {
                    rows += 1;
                    let len = if src.is_null(i) {
                        0
                    } else {
                        src.value(i).len()
                    };
                    if len < 40 {
                        short += 1;
                        println!("[{label}] SHORT ts {} _source len {len}", ts.value(i));
                    } else if ts.value(i) == target_ts {
                        println!("[{label}] target ts _source len {len} OK");
                    }
                }
            }
            println!("[{label}] rows {rows} short {short}");
        }
        Ok(())
    }
}

/// Walk a follower's physical plan; for every `FilterExec` sitting (through
/// pass-through nodes) above a vix scan, extract top-level-AND numeric
/// conjuncts (`col > | >= | < | <= | = literal`, i64/f64, type-matching the
/// scan schema) and inject them into the scan's [`VixCoreSource`]. The
/// FilterExec stays — pruning is conservative-only and every row is
/// re-checked. Non-matching shapes (OR, functions, casts, other types) are
/// simply not pushed.
pub fn inject_vix_numeric_bounds(
    plan: Arc<dyn ExecutionPlan>,
) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
    use datafusion::{
        common::tree_node::{Transformed, TreeNode},
        logical_expr::Operator,
        physical_plan::{
            coalesce_batches::CoalesceBatchesExec, expressions as phys, filter::FilterExec,
            repartition::RepartitionExec,
        },
    };

    fn literal_scalar(value: &ScalarValue) -> Option<NumScalar> {
        match value {
            ScalarValue::Int64(Some(v)) => Some(NumScalar::I64(*v)),
            ScalarValue::UInt64(Some(v)) => i64::try_from(*v).ok().map(NumScalar::I64),
            ScalarValue::Float64(Some(v)) => Some(NumScalar::F64(*v)),
            _ => None,
        }
    }

    fn scalar_matches_type(scalar: NumScalar, data_type: &DataType) -> bool {
        matches!(
            (scalar, data_type),
            (NumScalar::I64(_), DataType::Int64) | (NumScalar::F64(_), DataType::Float64)
        )
    }

    fn extract_bounds(
        predicate: &Arc<dyn PhysicalExpr>,
        schema: &Schema,
        out: &mut Vec<ColumnBound>,
    ) {
        if let Some(binary) = predicate.downcast_ref::<phys::BinaryExpr>() {
            if matches!(binary.op(), Operator::And) {
                extract_bounds(binary.left(), schema, out);
                extract_bounds(binary.right(), schema, out);
                return;
            }
            let (column, literal, op) = match (
                binary.left().downcast_ref::<phys::Column>(),
                binary.right().downcast_ref::<phys::Literal>(),
                binary.right().downcast_ref::<phys::Column>(),
                binary.left().downcast_ref::<phys::Literal>(),
            ) {
                (Some(column), Some(literal), ..) => (column, literal, *binary.op()),
                // literal OP column: flip the comparison
                (_, _, Some(column), Some(literal)) => {
                    let flipped = match binary.op() {
                        Operator::Gt => Operator::Lt,
                        Operator::GtEq => Operator::LtEq,
                        Operator::Lt => Operator::Gt,
                        Operator::LtEq => Operator::GtEq,
                        Operator::Eq => Operator::Eq,
                        _ => return,
                    };
                    (column, literal, flipped)
                }
                _ => return,
            };
            let Some(value) = literal_scalar(literal.value()) else {
                return;
            };
            let Ok(field) = schema.field_with_name(column.name()) else {
                return;
            };
            if !scalar_matches_type(value, field.data_type()) {
                return;
            }
            let name = column.name().to_string();
            let bound = match op {
                Operator::Gt => ColumnBound {
                    column: name,
                    min: Some((value, false)),
                    max: None,
                },
                Operator::GtEq => ColumnBound {
                    column: name,
                    min: Some((value, true)),
                    max: None,
                },
                Operator::Lt => ColumnBound {
                    column: name,
                    min: None,
                    max: Some((value, false)),
                },
                Operator::LtEq => ColumnBound {
                    column: name,
                    min: None,
                    max: Some((value, true)),
                },
                Operator::Eq => ColumnBound {
                    column: name,
                    min: Some((value, true)),
                    max: Some((value, true)),
                },
                _ => return,
            };
            out.push(bound);
        }
    }

    /// Inject `bounds` into every vix DataSourceExec under `node`, passing
    /// through repartition/coalesce/projection-free nodes only (a
    /// projection can rename columns — stop there).
    fn inject(
        node: Arc<dyn ExecutionPlan>,
        bounds: &[ColumnBound],
    ) -> datafusion::common::Result<Transformed<Arc<dyn ExecutionPlan>>> {
        if let Some(exec) = node.downcast_ref::<DataSourceExec>()
            && let Some(conf) = exec.data_source().downcast_ref::<FileScanConfig>()
        {
            let source: &dyn std::any::Any = conf.file_source.as_ref();
            if let Some(source) = source.downcast_ref::<VixCoreSource>() {
                let mut source = source.clone();
                let mut merged = source.column_bounds.clone();
                merged.extend(bounds.iter().cloned());
                source.column_bounds = merged;
                let mut conf = conf.clone();
                conf.file_source = Arc::new(source);
                return Ok(Transformed::yes(DataSourceExec::from_data_source(conf)));
            }
            return Ok(Transformed::no(node));
        }
        let passthrough = node.downcast_ref::<RepartitionExec>().is_some()
            || node.downcast_ref::<CoalesceBatchesExec>().is_some();
        if !passthrough {
            return Ok(Transformed::no(node));
        }
        let mut changed = false;
        let children: Vec<Arc<dyn ExecutionPlan>> = node
            .children()
            .into_iter()
            .map(|child| {
                inject(Arc::clone(child), bounds).map(|t| {
                    changed |= t.transformed;
                    t.data
                })
            })
            .collect::<datafusion::common::Result<_>>()?;
        if changed {
            Ok(Transformed::yes(node.with_new_children(children)?))
        } else {
            Ok(Transformed::no(node))
        }
    }

    plan.transform_down(|node| {
        let Some(filter) = node.downcast_ref::<FilterExec>() else {
            return Ok(Transformed::no(node));
        };
        let mut bounds = Vec::new();
        extract_bounds(filter.predicate(), &filter.input().schema(), &mut bounds);
        if bounds.is_empty() {
            return Ok(Transformed::no(node));
        }
        let injected = inject(Arc::clone(filter.input()), &bounds)?;
        if !injected.transformed {
            log::debug!(
                "vix numeric pushdown: {} bound(s) extracted but no vix scan adjacent",
                bounds.len()
            );
            return Ok(Transformed::no(node));
        }
        log::info!(
            "vix numeric pushdown: injected {} bound(s): {bounds:?}",
            bounds.len()
        );
        Ok(Transformed::yes(
            node.with_new_children(vec![injected.data])?,
        ))
    })
    .map(|t| t.data)
}
