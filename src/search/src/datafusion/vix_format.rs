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
    datatypes::{DataType, Field, FieldRef, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use config::meta::stream::RowIdBitmap;
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
    execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation},
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
    BoundValue, ColumnBound, SOURCE_COL_NAME, TIMESTAMP_COL_NAME, VixDocs, VixRangeSource,
};

use super::vortex_support::VORTEX_RUNTIME;
use crate::vix::source::{StoreRangeSource, VixReadMode, vix_read_mode};

/// File extension (without the dot) of core files.
const VIX_EXT: &str = "vix";

/// Per-file row selection for a core-file scan, attached as a typed
/// [`PartitionedFile`] extension by `generate_access_plan` (the counterpart
/// of `VortexAccessPlan` / `ParquetAccessPlan` for `.vix` files). A
/// compressed bitmap over the docs-blob rows; only the set rows are decoded.
#[derive(Debug, Clone)]
pub struct VixScanSelection {
    pub row_ids: Arc<RowIdBitmap>,
}

/// DataFusion [`FileFormat`] for core `.vix` files.
#[derive(Debug, Default)]
pub struct VixCoreFormat {
    /// The query time range (`[start, end)`), pushed into the vortex scan of
    /// every file (zone-map pruned). The same bounds are re-applied by the
    /// combined filter above the scan, so this is a pure early-out.
    timestamp_filter: Option<(i64, i64)>,
    /// Range conjuncts extracted from the plan's FilterExec (see
    /// [`inject_vix_scan_pruning`]) — numeric bounds push into every file's
    /// vortex scan as row filters, and every bound (strings included)
    /// prunes chunks/files through the O2 stats blob. Conservative-only:
    /// the FilterExec above re-applies the predicate on every returned row.
    column_bounds: Vec<ColumnBound>,
    /// Columns on which the plan's filter is NULL-REJECTING (a NULL cell
    /// can never satisfy the conjunct: `=`, `!=`, `<`, `>`, LIKE, IN,
    /// IS NOT NULL, str_match…). §4 field-presence pruning: a file whose
    /// footer proves such a column all-NULL (absent from a
    /// columns-complete file, or presence count 0) is skipped whole,
    /// BEFORE any docs read or json_get fallback.
    null_rejected_columns: Vec<String>,
    /// §6.2: this table DECLARES per-file `_timestamp DESC` output, so a
    /// concat file's scan must stream through the k-way region merge (its
    /// stored rows are only piecewise sorted). exec.rs sets it on the
    /// declared-sort table only — and routes there only concat files whose
    /// proven region decomposition fits the merge (probed at plan time), so
    /// an opener finding no regions is a hard error, never a silent
    /// unordered stream. `false` (the default; the undeclared/concat table
    /// and every ad-hoc listing): scans stream in stored order.
    emit_ts_desc: bool,
}

impl VixCoreFormat {
    pub fn new(timestamp_filter: Option<(i64, i64)>) -> Self {
        Self {
            timestamp_filter,
            column_bounds: Vec::new(),
            null_rejected_columns: Vec::new(),
            emit_ts_desc: false,
        }
    }

    /// Declare that every scan of this table must emit `_timestamp` DESC
    /// (the declared-sort table): concat files k-way merge their regions.
    pub fn with_ordered_output(mut self, emit_ts_desc: bool) -> Self {
        self.emit_ts_desc = emit_ts_desc;
        self
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
        // ranged opens block on fetches: keep them off the async runtime.
        // `{e:#}` keeps the full cause chain in the text (the not-found
        // detection below string-matches it).
        return VORTEX_RUNTIME
            .spawn_blocking(move || VixDocs::open_ranged(source))
            .await
            .map_err(|e| DataFusionError::Execution(format!("vix open task failed: {e}")))?
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "failed to open core .vix file {location}: {e:#}"
                ))
            });
    }
    let data = store
        .get_opts(&meta.location, GetOptions::default())
        .await?
        .bytes()
        .await?;
    VixDocs::open(data).map_err(|e| {
        DataFusionError::Execution(format!("failed to open core .vix file {location}: {e:#}"))
    })
}

// ─── M19: lifecycle-expired objects on the query path ────────────────────────
//
// A `.vix` object can be deleted externally (S3 lifecycle expiry) while its
// file_list row is still live. Pre-M19 that 404 failed the WHOLE query, and
// nothing removed the row — every retry/query re-listed the file forever.
// The scan now (a) degrades: the missing file contributes zero rows instead
// of erroring the query (only for the NOT-FOUND error class — outages,
// timeouts and permission errors still fail loudly), and (b) reconciles:
// the file's stale file_list row is removed in the background, so the next
// query does not list it at all. The `.vxi` sidecar needs no handling here:
// index absence already fails open to the scan, and a heal-rewritten sidecar
// can only be YOUNGER than its data object under the lifecycle.

/// The file_list key to reconcile when `location`'s object is gone, if the
/// location names a file_list-tracked storage object: the search stores
/// encode locations as `trace_id/$$/[account/::/]files/org/...`; after
/// stripping those prefixes the key must be the canonical 9-segment
/// `files/{org}/{stype}/{stream}/{Y}/{M}/{D}/{H}/{file}.vix` shape. WAL
/// paths (11+ segments) and anything else return `None` — their missing
/// files keep erroring loudly (a vanished WAL file is a bug, not a
/// lifecycle).
fn stale_row_cleanup_key(location: &object_store::path::Path) -> Option<String> {
    let (_account, path) = super::storage::format_location(location);
    let key = path.to_string();
    let start = if key.starts_with("files/") {
        0
    } else {
        // ad-hoc listings (tests, tools) may carry a URL-prefix before the
        // canonical key — anchor on the first segment-aligned "files/"
        key.find("/files/").map(|i| i + 1)?
    };
    let key = &key[start..];
    (key.split('/').count() == 9 && key.ends_with(".vix")).then(|| key.to_string())
}

/// Whether an already-stringified DataFusion/anyhow error means "the object
/// does not exist" — see `infra::storage::is_not_found_error` for the typed
/// variant; here errors have been formatted through the scan pipeline, so
/// the ALTERNATE-formatted text is the reliable carrier (reformatting layers
/// embed the object_store `... not found ...` display verbatim).
fn error_text_is_not_found(text: &str) -> bool {
    text.to_lowercase().contains("not found")
}

/// Background reconciliation of one stale row: remove it from the meta
/// file_list (+ the local cache mirror) and drop the file's memoized reader.
/// Idempotent; failures only log (the next query's 404 retries it).
async fn remove_stale_file_list_row(key: String) {
    match infra::file_list::remove(&key).await {
        Ok(()) => log::warn!(
            "vix scan: removed stale file_list row of {key} (object deleted externally, e.g. lifecycle expiry)"
        ),
        Err(e) => log::warn!("vix scan: removing stale file_list row of {key} failed: {e}"),
    }
    if let Err(e) = infra::file_list::LOCAL_CACHE.remove(&key).await {
        log::debug!("vix scan: local file_list cache remove of {key} failed: {e}");
    }
    crate::vix::reader_cache::GLOBAL_CACHE.remove(&key);
}

/// Fire [`remove_stale_file_list_row`] without blocking the scan (callable
/// from the async opener; the blocking-thread arm passes a captured handle).
fn spawn_stale_row_cleanup(handle: Option<&tokio::runtime::Handle>, key: String) {
    match handle {
        Some(handle) => {
            handle.spawn(remove_stale_file_list_row(key));
        }
        None => {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(remove_stale_file_list_row(key));
            } else {
                log::warn!(
                    "vix scan: no runtime at hand to reconcile stale file_list row of {key}"
                );
            }
        }
    }
}

/// `_timestamp` bounds of a non-empty docs blob.
///
/// Sorted files (`row_order` ts_desc — every historical file and every
/// non-concat writer output): the rows are stored ordered `_timestamp`
/// DESC, so only the first and the last row are read (the row indices are
/// deduped internally when `num_rows == 1`); min/max of the two values is
/// taken for robustness.
///
/// #51c-c CONCAT-order files: the first/last rows are NOT the newest/oldest
/// — the exact bounds come from the file's zone table instead (parsed at
/// open, zero data reads; the spliced table covers every row). A concat
/// file without a trustworthy zone table reports `None`.
///
/// `None` either way when the bounds cannot be derived — the column then
/// keeps unknown statistics (fail-open: no pruning, no ordering assumption).
fn timestamp_bounds(docs: &VixDocs, num_rows: usize) -> Option<(i64, i64)> {
    if !docs.row_order().is_ts_desc() {
        return docs.zone_ts_bounds();
    }
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
    /// statistics: sorted core files store their rows ordered `_timestamp`
    /// DESC, so the bounds come from a point read of just the first and
    /// last row; #51c-c concat-order files (not globally sorted) derive
    /// them from their zone table instead — see [`timestamp_bounds`]. They
    /// let `split_file_groups_by_statistics` arrange non-overlapping files
    /// into file groups that uphold the `_timestamp` DESC file sort order
    /// declared in `exec.rs::build_table_for_format`, eliding the sort for
    /// `ORDER BY _timestamp DESC` queries. Statistics of every other column
    /// are unknown (and stay unknown for zero-row files).
    async fn infer_stats(
        &self,
        _state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Statistics> {
        let docs = match open_docs(store, object).await {
            Ok(docs) => docs,
            Err(e) => {
                // M19: a stats probe hitting an externally deleted object
                // must not fail the query at PLAN time — return unknown
                // statistics (the exec-side opener degrades the same file to
                // zero rows) and reconcile the stale row in the background.
                if error_text_is_not_found(&e.to_string())
                    && let Some(key) = stale_row_cleanup_key(&object.location)
                {
                    log::warn!(
                        "vix stats: object {} is gone (deleted externally): degrading to unknown statistics; removing its stale file_list row",
                        object.location
                    );
                    spawn_stale_row_cleanup(None, key);
                    return Ok(Statistics::new_unknown(&table_schema));
                }
                return Err(e);
            }
        };
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
                .with_column_bounds(self.column_bounds.clone())
                .with_null_rejected_columns(self.null_rejected_columns.clone())
                .with_ordered_output(self.emit_ts_desc),
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
    /// [`inject_vix_scan_pruning`]).
    column_bounds: Vec<ColumnBound>,
    /// See [`VixCoreFormat::null_rejected_columns`].
    null_rejected_columns: Vec<String>,
    /// See [`VixCoreFormat::emit_ts_desc`].
    emit_ts_desc: bool,
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
            null_rejected_columns: Vec::new(),
            emit_ts_desc: false,
            memory_pool: None,
        }
    }

    pub fn with_column_bounds(mut self, column_bounds: Vec<ColumnBound>) -> Self {
        self.column_bounds = column_bounds;
        self
    }

    pub fn with_null_rejected_columns(mut self, columns: Vec<String>) -> Self {
        self.null_rejected_columns = columns;
        self
    }

    pub fn with_ordered_output(mut self, emit_ts_desc: bool) -> Self {
        self.emit_ts_desc = emit_ts_desc;
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
            null_rejected_columns: self.null_rejected_columns.clone(),
            emit_ts_desc: self.emit_ts_desc,
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
    /// Range conjuncts pushed into the scan + file/chunk skip checks.
    column_bounds: Vec<ColumnBound>,
    /// Columns whose conjuncts are null-rejecting (field-presence skips).
    null_rejected_columns: Vec<String>,
    /// §6.2: the table declares `_timestamp DESC` — concat files must
    /// stream through the k-way region merge.
    emit_ts_desc: bool,
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
        let null_rejected_columns = self.null_rejected_columns.clone();
        let emit_ts_desc = self.emit_ts_desc;
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
            // Shared so the ordered merge path can GROW it per opened
            // region from the blocking scan thread (pool pushback instead
            // of invisible allocation); the stream holds it alive.
            let reservation: Arc<parking_lot::Mutex<Option<MemoryReservation>>> =
                Arc::new(parking_lot::Mutex::new(match &memory_pool {
                    Some(pool) => {
                        let reservation = MemoryConsumer::new(format!(
                            "VixCoreOpener[{location}]"
                        ))
                        .register(pool);
                        reservation.try_grow(estimate)?;
                        Some(reservation)
                    }
                    None => None,
                }));
            let scan_reservation = Arc::clone(&reservation);

            // M19: when this file's object is gone from the store (deleted
            // externally, e.g. S3 lifecycle expiry) the scan DEGRADES — the
            // file contributes zero rows instead of failing the whole query
            // — and its stale file_list row is reconciled in the background.
            // Only for the not-found error class and file_list-tracked keys.
            let cleanup_key = stale_row_cleanup_key(&location);
            let cleanup_handle = tokio::runtime::Handle::try_current().ok();

            // An index row selection means a point read: in ranged mode open
            // the docs blob over range fetches and decode only the selected
            // chunks. Full scans (no selection) keep the single whole-object
            // get — they decode most of the docs blob anyway, and the object
            // is usually already in the local file cache (cache_files
            // enqueues background downloads at index-evaluation time).
            let input = match docs_range_source(&store, &file.object_meta, ranged_wanted) {
                Some(source) => DocsInput::Ranged(source),
                None => {
                    let bytes = async {
                        store
                            .get_opts(&location, GetOptions::default())
                            .await?
                            .bytes()
                            .await
                    }
                    .await;
                    match bytes {
                        Ok(bytes) => DocsInput::Bytes(bytes),
                        Err(e @ object_store::Error::NotFound { .. })
                            if cleanup_key.is_some() =>
                        {
                            log::warn!(
                                "vix scan: object {location} is gone (deleted externally): degrading to an empty scan; removing its stale file_list row ({e})"
                            );
                            if let Some(key) = cleanup_key {
                                spawn_stale_row_cleanup(cleanup_handle.as_ref(), key);
                            }
                            return Ok(futures::stream::empty().boxed());
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
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
                        // nothing in this file (footer metadata only — zero
                        // data reads). Empty stream, reservation released.
                        if let Some(tier) = file_provably_skippable(
                            &docs,
                            timestamp_filter,
                            &column_bounds,
                            &null_rejected_columns,
                        ) {
                            log::debug!("vix scan: skipped {location} ({tier})");
                            return Ok(());
                        }
                        let mut send = |batch: RecordBatch| {
                            tx.blocking_send(Ok(batch))
                                .map_err(|_| anyhow::anyhow!("scan consumer dropped"))
                        };
                        // §6.2: under a declared-sort table a CONCAT file
                        // streams through the k-way region merge (its rows
                        // are only piecewise sorted); each opened region
                        // grows the reservation by its decode window.
                        if emit_ts_desc && !docs.row_order().is_ts_desc() {
                            return scan_core_docs_merged(
                                docs,
                                &projected_schema,
                                selection.as_deref(),
                                timestamp_filter,
                                &column_bounds,
                                &mut || {
                                    if let Some(reservation) =
                                        scan_reservation.lock().as_mut()
                                    {
                                        reservation
                                            .try_grow(chunk_bytes.saturating_mul(3))
                                            .map_err(|e| anyhow::anyhow!("{e}"))?;
                                    }
                                    Ok(())
                                },
                                &mut send,
                            );
                        }
                        scan_core_docs(
                            docs,
                            &projected_schema,
                            selection.as_deref(),
                            timestamp_filter,
                            &column_bounds,
                            &mut send,
                        )
                    })
                }));
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        // M19: a ranged open/fetch hitting an externally
                        // deleted object degrades this file to the rows
                        // already emitted (usually none — the open fails on
                        // the first tail fetch) instead of failing the
                        // query, and reconciles the stale row. `{e:#}`
                        // carries the chain, so the not-found class is
                        // detectable even through reformatting layers.
                        let text = format!("{e:#}");
                        if error_text_is_not_found(&text) && cleanup_key.is_some() {
                            log::warn!(
                                "vix scan: object {location} is gone (deleted externally): degrading to an empty scan; removing its stale file_list row ({text})"
                            );
                            if let Some(key) = cleanup_key {
                                spawn_stale_row_cleanup(cleanup_handle.as_ref(), key);
                            }
                            // drop tx without sending: the stream ends clean
                        } else {
                            // Receiver may already be gone (e.g. limit
                            // reached); nothing to do then.
                            let _ = tx.blocking_send(Err(DataFusionError::Execution(format!(
                                "core .vix scan of {location} failed: {text}"
                            ))));
                        }
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
    selection: Option<&RowIdBitmap>,
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
    selection: Option<&RowIdBitmap>,
    timestamp_filter: Option<(i64, i64)>,
    column_bounds: &[ColumnBound],
    on_batch: &mut dyn FnMut(RecordBatch) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let plan = LogicalProjectionPlan::new(&docs, projected_schema)?;
    let rows = selection.map(|bits| bits.iter().map(u64::from).collect::<Vec<u64>>());
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

/// [`scan_core_docs`] through the §6.2 k-way region merge: the same
/// logical-projection mapping, but rows stream in GLOBAL `_timestamp` DESC
/// order (a concat file's regions merged by timestamp instead of a full
/// sort above the scan). `on_region_open` is the per-opened-region memory
/// hook. Used only under a declared-sort table; a file without a proven
/// region decomposition errs (never a silent unordered stream).
fn scan_core_docs_merged(
    docs: VixDocs,
    projected_schema: &SchemaRef,
    selection: Option<&RowIdBitmap>,
    timestamp_filter: Option<(i64, i64)>,
    column_bounds: &[ColumnBound],
    on_region_open: &mut dyn FnMut() -> anyhow::Result<()>,
    on_batch: &mut dyn FnMut(RecordBatch) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let plan = LogicalProjectionPlan::new(&docs, projected_schema)?;
    let rows = selection.map(|bits| bits.iter().map(u64::from).collect::<Vec<u64>>());
    if let Some(rows) = rows.as_ref()
        && rows.is_empty()
    {
        return Ok(());
    }
    docs.scan_docs_ts_desc_merged(
        Some(&plan.physical_projection),
        rows,
        timestamp_filter,
        column_bounds,
        None,
        on_region_open,
        &mut |batch| on_batch(plan.project(&batch)?),
    )
}

/// The v2 per-file skip hook: `Some(tier)` when the file's FOOTER metadata
/// PROVES no row can satisfy the plan's filter — zero data reads either
/// way (ranged opens have fetched only the footer at this point). Tiers,
/// cheapest first; any uncertainty keeps the file (fail-open):
///
/// 1. `"field-presence"` (§4): a NULL-REJECTING conjunct references a column that is provably
///    all-NULL here — its presence count is 0, or it is absent from the docs schema of a
///    `columns_complete` file (the all-present invariant makes absence a proof; the `json_get`
///    fallback would only synthesize NULLs). Files without the completeness marker never prune on
///    absence — their `_source` may hide values.
/// 2. `"file-stats"`: a range conjunct is provably outside the vortex footer's file-level min/max
///    (first-encode files only; numeric, exact stats).
/// 3. `"chunk-stats-empty"`: the zone table + the O2 per-column chunk stats exclude EVERY chunk
///    (the passthrough-surviving path — such files carry no vortex stats at all). This is the
///    file-level fold of the same per-chunk logic the scan applies.
fn file_provably_skippable(
    docs: &VixDocs,
    ts_range: Option<(i64, i64)>,
    bounds: &[ColumnBound],
    null_rejected: &[String],
) -> Option<&'static str> {
    use std::cmp::Ordering;

    // Tier 1: field presence. The `columns` list enumerates the docs
    // columns (reserved `_source`/`_original` excluded — the schema check
    // below keeps predicates on those out of this tier).
    let presence = docs.column_presence();
    if !null_rejected.is_empty() && !presence.is_empty() {
        for column in null_rejected {
            match presence.iter().find(|(name, _)| name == column) {
                // an explicit zero presence count: every native cell is
                // NULL, and native columns are authoritative for reads
                Some((_, Some(0))) => return Some("field-presence"),
                Some(_) => {}
                // absent from the columns list: only a columns-complete
                // file proves the field never occurs in `_source` either
                None => {
                    if docs.columns_complete()
                        && docs.schema().field_with_name(column).is_err()
                    {
                        return Some("field-presence");
                    }
                }
            }
        }
    }

    // Tier 2: vortex footer file-level stats (numeric, first-encode files).
    for bound in bounds {
        let Ok(Some((file_min, file_max))) = docs.column_stats(&bound.column) else {
            continue;
        };
        if let Some((value, inclusive)) = &bound.min {
            // need rows with column >(=) value; impossible if file_max <(=) value
            match vortex_index::cmp_num_vs_bound(file_max, value) {
                Some(Ordering::Less) => return Some("file-stats"),
                Some(Ordering::Equal) if !inclusive => return Some("file-stats"),
                _ => {}
            }
        }
        if let Some((value, inclusive)) = &bound.max {
            match vortex_index::cmp_num_vs_bound(file_min, value) {
                Some(Ordering::Greater) => return Some("file-stats"),
                Some(Ordering::Equal) if !inclusive => return Some("file-stats"),
                _ => {}
            }
        }
    }

    // Tier 3: every chunk excluded by the zone table + O2 chunk stats.
    if docs
        .pruned_scan_ranges(ts_range, bounds)
        .is_some_and(|ranges| ranges.is_empty())
    {
        return Some("chunk-stats-empty");
    }
    None
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
        bytes::Bytes::from(writer.finish().unwrap().0)
    }

    /// One core file with 10 rows ordered `_timestamp` DESC
    /// (`newest..=newest-9`), all tagged with the same `level` value.
    fn build_desc_core_file(newest: i64, level: &str) -> bytes::Bytes {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("level", DataType::Utf8, true),
        ]));
        let opts = VixWriterOptions {
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
        bytes::Bytes::from(writer.finish().unwrap().0)
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
        bytes::Bytes::from(writer.finish().unwrap().0)
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
        selection: Option<&RowIdBitmap>,
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
        let bits = RowIdBitmap::from_row_ids(4, [1u32, 2]);
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
            bytes::Bytes::from(writer.finish().unwrap().0)
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
        let data = bytes::Bytes::from(writer.finish().unwrap().0);

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

    // ── M19: lifecycle-expired objects on the query path ─────────────────

    /// An ObjectStore whose LISTING still advertises objects the store no
    /// longer serves — the exact desync a live file_list row + an S3
    /// lifecycle expiry produces (the `memory:///` adapter lists from
    /// file_list rows and gets from storage).
    #[derive(Debug)]
    struct LifecycleExpiredStore {
        inner: InMemory,
        missing: Vec<ObjectMeta>,
    }

    impl fmt::Display for LifecycleExpiredStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "LifecycleExpiredStore")
        }
    }

    #[async_trait]
    impl ObjectStore for LifecycleExpiredStore {
        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            if self.missing.iter().any(|m| &m.location == location) {
                return Err(object_store::Error::NotFound {
                    path: location.to_string(),
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "lifecycle expired",
                    )),
                });
            }
            self.inner.get_opts(location, options).await
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
            let prefix_str = prefix.map(|p| p.to_string()).unwrap_or_default();
            let ghosts: Vec<object_store::Result<ObjectMeta>> = self
                .missing
                .iter()
                .filter(|m| m.location.as_ref().starts_with(&prefix_str))
                .cloned()
                .map(Ok)
                .collect();
            self.inner
                .list(prefix)
                .chain(futures::stream::iter(ghosts))
                .boxed()
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn put_opts(
            &self,
            location: &Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<Path>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    fn ghost_meta(key: &str, size: u64) -> ObjectMeta {
        ObjectMeta {
            location: Path::from(key),
            last_modified: chrono::Utc::now(),
            size,
            e_tag: None,
            version: None,
        }
    }

    /// M19 end-to-end: a query over one LIVE file and two EXPIRED ones —
    /// one small (whole-object `Bytes` open) and one past the 256MB ranged
    /// threshold (`open_ranged`, the blocking-thread error arm) — must
    /// SUCCEED with exactly the live file's rows (each missing file
    /// degrades to zero rows), and the expired files' stale file_list rows
    /// must be reconciled away in the background. `collect_stat(true)`
    /// routes the missing files through the `infer_stats` hook first, so
    /// the plan-time arm is exercised by the same query.
    #[tokio::test(flavor = "multi_thread")]
    async fn m19_scan_missing_objects_degrades_and_reconciles_rows() -> Result<()> {
        // sqlite file_list tables so the background reconciliation is
        // observable end-to-end
        std::fs::create_dir_all(&config::get_config().common.data_db_dir)
            .expect("create data_db_dir for tests");
        infra::file_list::create_table()
            .await
            .expect("create file_list tables");

        let run = config::utils::time::now_micros();
        let live_key = format!("files/m19org{run}/logs/s1/2021/01/02/00/live.vix");
        let gone_small_key = format!("files/m19org{run}/logs/s1/2021/01/02/00/gone_small.vix");
        let gone_ranged_key = format!("files/m19org{run}/logs/s1/2021/01/02/00/gone_ranged.vix");

        // seed live file_list rows for the EXPIRED keys (the stale rows)
        for key in [&gone_small_key, &gone_ranged_key] {
            infra::file_list::add(
                "",
                key,
                &config::meta::stream::FileMeta {
                    min_ts: 1000,
                    max_ts: 1003,
                    records: 4,
                    original_size: 100,
                    compressed_size: 4096,
                    ..Default::default()
                },
            )
            .await
            .expect("seed stale file_list row");
            assert!(infra::file_list::contains(key).await.unwrap());
        }

        let inner = InMemory::new();
        inner
            .put(&Path::from(live_key.as_str()), build_core_file().into())
            .await?;
        let store = Arc::new(LifecycleExpiredStore {
            inner,
            missing: vec![
                ghost_meta(&gone_small_key, 4096),
                // >= ZO_VIX_FULL_SCAN_RANGED_MIN_BYTES (256MB default): the
                // opener takes the ranged arm for this one
                ghost_meta(&gone_ranged_key, 512 * 1024 * 1024),
            ],
        });

        let registry = DefaultObjectStoreRegistry::new();
        registry.register_store(&url::Url::parse("test:///").unwrap(), store);
        let runtime_env = RuntimeEnvBuilder::new()
            .with_object_store_registry(Arc::new(registry))
            .build()?;
        // canonical storage keys live DEEP under the listing prefix —
        // mirror exec.rs: nested files must not be ignored
        let mut session_config = SessionConfig::new().with_target_partitions(2);
        session_config
            .options_mut()
            .execution
            .listing_table_ignore_subdirectory = false;
        let state = SessionStateBuilder::new()
            .with_config(session_config)
            .with_runtime_env(Arc::new(runtime_env))
            .with_default_features()
            .build();
        let ctx = SessionContext::new_with_state(state);

        let table_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("level", DataType::Utf8, true),
        ]));
        let listing_options = ListingOptions::new(Arc::new(VixCoreFormat::new(None)))
            .with_collect_stat(true)
            .with_target_partitions(2);
        let config = ListingTableConfig::new(ListingTableUrl::parse(format!(
            "test:///files/m19org{run}/"
        ))?)
        .with_listing_options(listing_options)
        .with_schema(Arc::clone(&table_schema));
        ctx.register_table("t", Arc::new(ListingTable::try_new(config)?))?;

        // the query must SUCCEED with exactly the live file's 4 rows
        let df = ctx
            .sql("SELECT _timestamp FROM t ORDER BY _timestamp")
            .await?;
        let batches = df.collect().await?;
        let mut rows = Vec::new();
        for batch in &batches {
            let ts = batch.column_by_name(TIMESTAMP_COL_NAME).unwrap();
            let ts = ts.as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..batch.num_rows() {
                rows.push(ts.value(i));
            }
        }
        assert_eq!(
            rows,
            vec![1000, 1001, 1002, 1003],
            "the live file's rows and ONLY those — each expired file degrades to zero rows"
        );

        // the background reconciliation removes the stale rows (spawned
        // tasks; poll briefly)
        for key in [&gone_small_key, &gone_ranged_key] {
            let mut gone = false;
            for _ in 0..100 {
                if !infra::file_list::contains(key).await.unwrap() {
                    gone = true;
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            assert!(gone, "the stale file_list row of {key} must be reconciled away");
        }
        Ok(())
    }

    /// M19 plan-time arm in isolation: `infer_stats` on a vanished object
    /// returns UNKNOWN statistics instead of failing the query at plan time.
    #[tokio::test(flavor = "multi_thread")]
    async fn m19_infer_stats_missing_object_degrades_to_unknown() {
        let key = "files/m19statorg/logs/s1/2021/01/02/00/gone.vix";
        let meta = ghost_meta(key, 4096);
        let store: Arc<dyn ObjectStore> = Arc::new(LifecycleExpiredStore {
            inner: InMemory::new(),
            missing: vec![meta.clone()],
        });
        let state = SessionContext::new().state();
        let format = VixCoreFormat::new(None);
        let table_schema = logical_schema();
        let stats = format
            .infer_stats(&state, &store, Arc::clone(&table_schema), &meta)
            .await
            .expect("a vanished object must not fail the stats probe");
        assert_eq!(stats.num_rows, Precision::Absent);
        assert!(
            stats
                .column_statistics
                .iter()
                .all(|c| c.min_value == Precision::Absent && c.max_value == Precision::Absent),
            "all column statistics stay unknown"
        );
    }

    /// M19: only canonical 9-segment `files/...*.vix` storage keys are
    /// eligible for stale-row cleanup (and the error swallow that comes
    /// with it) — WAL-shaped paths and other extensions keep erroring
    /// loudly.
    #[test]
    fn m19_stale_row_cleanup_key_gates_on_storage_shape() {
        // the memory:/// adapter's real location shape (trace + account)
        assert_eq!(
            stale_row_cleanup_key(&Path::from(
                "trace1/schema=abc/format=vix/$$/acct/::/files/org/logs/s1/2021/01/02/00/a.vix"
            ))
            .as_deref(),
            Some("files/org/logs/s1/2021/01/02/00/a.vix")
        );
        // no account
        assert_eq!(
            stale_row_cleanup_key(&Path::from(
                "trace1/schema=abc/format=vix/$$/files/org/logs/s1/2021/01/02/00/a.vix"
            ))
            .as_deref(),
            Some("files/org/logs/s1/2021/01/02/00/a.vix")
        );
        // bare canonical key (ad-hoc listing)
        assert_eq!(
            stale_row_cleanup_key(&Path::from("files/org/logs/s1/2021/01/02/00/a.vix"))
                .as_deref(),
            Some("files/org/logs/s1/2021/01/02/00/a.vix")
        );
        // WAL-shaped path (thread_id + schema_key = 11 segments): ineligible
        assert_eq!(
            stale_row_cleanup_key(&Path::from(
                "files/org/logs/s1/0/2021/01/02/00/1234567890abcdef/a.vix"
            )),
            None
        );
        // non-.vix extensions: ineligible
        assert_eq!(
            stale_row_cleanup_key(&Path::from(
                "files/org/logs/s1/2021/01/02/00/a.parquet"
            )),
            None
        );
        // unrelated paths: ineligible
        assert_eq!(stale_row_cleanup_key(&Path::from("results/org/x.json")), None);
    }

    /// #51c-c: one CONCAT-order core file — DESC `runs` (each `(newest,
    /// len)`) stored back-to-back, stamped `row_order=concat` exactly like
    /// a concatenation-merge output. NOT globally sorted whenever a later
    /// run is newer than the previous run's tail.
    fn build_concat_core_file(runs: &[(i64, usize)], level: &str) -> bytes::Bytes {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("level", DataType::Utf8, true),
        ]));
        let opts = VixWriterOptions {
            row_group_size: 4,
            concat_row_order: true,
            ..Default::default()
        };
        let timestamps: Vec<i64> = runs
            .iter()
            .flat_map(|&(newest, len)| (0..len as i64).map(move |i| newest - i))
            .collect();
        let sources: Vec<String> = timestamps
            .iter()
            .map(|ts| format!(r#"{{"_timestamp":{ts},"level":"{level}"}}"#))
            .collect();
        let mut writer = VixWriter::new(&schema, opts, false);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(timestamps.clone())),
                Arc::new(StringArray::from(vec![Some(level); timestamps.len()])),
            ],
        )
        .unwrap();
        let sources = StringArray::from_iter_values(sources.iter().map(String::as_str));
        writer
            .push_batch_with_source(&batch, &sources, None)
            .unwrap();
        bytes::Bytes::from(writer.finish().unwrap().0)
    }

    /// #51c-c: `infer_stats` over a CONCAT-order file must report the TRUE
    /// `_timestamp` bounds. The sorted-file shortcut (first + last row)
    /// would report (316, 300) for this file — both wrong — because neither
    /// boundary row is a global extreme; the zone table carries the exact
    /// span (296, 320).
    #[tokio::test]
    async fn infer_stats_concat_file_reports_zone_bounds() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("c1.vix");
        // rows: [300..296] then [320..316] — first row 300, last row 316,
        // true span [296, 320]
        store
            .put(
                &path,
                build_concat_core_file(&[(300, 5), (320, 5)], "c").into(),
            )
            .await
            .unwrap();
        let meta = store.head(&path).await.unwrap();
        let state = SessionContext::new().state();
        let format = VixCoreFormat::new(None);

        let table_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("level", DataType::Utf8, true),
        ]));
        let stats = format
            .infer_stats(&state, &store, Arc::clone(&table_schema), &meta)
            .await
            .unwrap();
        assert_eq!(stats.num_rows, Precision::Exact(10));
        let ts_stats = &stats.column_statistics[table_schema.index_of(TIMESTAMP_COL_NAME).unwrap()];
        assert_eq!(
            ts_stats.min_value,
            Precision::Exact(ScalarValue::Int64(Some(296))),
            "concat min must come from the zone table, not the last row"
        );
        assert_eq!(
            ts_stats.max_value,
            Precision::Exact(ScalarValue::Int64(Some(320))),
            "concat max must come from the zone table, not the first row"
        );
        assert_eq!(ts_stats.null_count, Precision::Exact(0));
    }

    /// #51c-c: the `[min, max)` `_timestamp` filter pushed into a
    /// CONCAT-order file's scan returns exactly the per-row truth — the
    /// pushdown re-evaluates per row (chunk pruning is stats-based and
    /// fail-open), so an unsorted file loses nothing.
    #[test]
    fn concat_file_ts_filter_scan_equivalence() {
        let data = build_concat_core_file(&[(300, 5), (320, 5)], "c");
        let projected: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            TIMESTAMP_COL_NAME,
            DataType::Int64,
            false,
        )]));
        let stored: Vec<i64> = vec![300, 299, 298, 297, 296, 320, 319, 318, 317, 316];
        for (min, max) in [
            (0i64, i64::MAX),
            (297, 319), // cuts both runs
            (296, 297), // one row of run 1
            (320, 321), // one row of run 2
            (301, 316), // gap between the runs: nothing
        ] {
            let mut got: Vec<i64> = Vec::new();
            for batch in scan_all(data.clone(), Arc::clone(&projected), None, Some((min, max))) {
                let ts = batch.column_by_name(TIMESTAMP_COL_NAME).unwrap();
                let ts = ts.as_any().downcast_ref::<Int64Array>().unwrap();
                got.extend(ts.iter().flatten());
            }
            let expected: Vec<i64> = stored
                .iter()
                .copied()
                .filter(|&t| t >= min && t < max)
                .collect();
            assert_eq!(
                got, expected,
                "ts filter [{min}, {max}) over the concat file"
            );
        }
    }

    /// Register the two-file table (one CONCAT-order file + one sorted
    /// file) the way `exec.rs::build_table_for_format` does. `declare_sort`
    /// adds the per-file `_timestamp DESC` declaration; `ordered_source`
    /// makes the source ordered-aware (M4: concat files k-way merge their
    /// regions) — `declare_sort && ordered_source` is the M4 declared
    /// table, `declare_sort && !ordered_source` is the pinned hazard.
    async fn concat_order_by_ctx(declare_sort: bool, ordered_source: bool) -> Result<SessionContext> {
        let store = Arc::new(InMemory::new());
        // concat file: rows [300..296, 320..316] — the newest rows of the
        // TABLE live mid-file; a trusted per-file DESC order returns 300
        // before 320
        store
            .put(
                &Path::from("data/c1.vix"),
                build_concat_core_file(&[(300, 5), (320, 5)], "c").into(),
            )
            .await?;
        // plus one honestly sorted file, strictly older
        store
            .put(
                &Path::from("data/f2.vix"),
                build_desc_core_file(200, "b").into(),
            )
            .await?;

        let registry = DefaultObjectStoreRegistry::new();
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

        let table_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("level", DataType::Utf8, true),
        ]));
        let mut listing_options = ListingOptions::new(Arc::new(
            VixCoreFormat::new(None).with_ordered_output(ordered_source),
        ))
        .with_collect_stat(true)
        .with_target_partitions(2);
        if declare_sort {
            listing_options = listing_options.with_file_sort_order(vec![vec![
                datafusion::prelude::col(TIMESTAMP_COL_NAME).sort(false, false),
            ]]);
        }
        let config = ListingTableConfig::new(ListingTableUrl::parse("test:///data/")?)
            .with_listing_options(listing_options)
            .with_schema(table_schema);
        ctx.register_table("t", Arc::new(ListingTable::try_new(config)?))?;
        Ok(ctx)
    }

    async fn concat_order_by_top7(ctx: &SessionContext) -> Result<(String, Vec<i64>)> {
        let df = ctx
            .sql("SELECT _timestamp FROM t ORDER BY _timestamp DESC LIMIT 7")
            .await?;
        let plan = df.create_physical_plan().await?;
        let rendered = displayable(plan.as_ref()).indent(true).to_string();
        let batches = collect(plan, ctx.task_ctx()).await?;
        let mut got = Vec::new();
        for batch in &batches {
            let ts = batch.column_by_name(TIMESTAMP_COL_NAME).unwrap();
            let ts = ts.as_any().downcast_ref::<Int64Array>().unwrap();
            got.extend((0..batch.num_rows()).map(|i| ts.value(i)));
        }
        Ok((rendered, got))
    }

    /// #51c-c wrong-results regression (non-negotiable): `ORDER BY
    /// _timestamp DESC LIMIT k` over a table containing a CONCAT-order file
    /// must equal the full-sort truth WHEN the per-file sort declaration is
    /// dropped — the exec.rs routing for OPAQUE concat files. The plan pays
    /// a real SortExec; that is the documented trade.
    #[tokio::test]
    async fn order_by_desc_over_concat_file_without_declaration_is_correct() -> Result<()> {
        let ctx = concat_order_by_ctx(false, false).await?;
        let (rendered, got) = concat_order_by_top7(&ctx).await?;
        assert!(
            rendered.contains("SortExec"),
            "without the declaration a real sort must run, got plan:\n{rendered}"
        );
        // global truth: the concat file's SECOND run holds the newest rows
        let expected: Vec<i64> = vec![320, 319, 318, 317, 316, 300, 299];
        assert_eq!(
            got, expected,
            "full-sort baseline must hold over the concat file;\nplan:\n{rendered}"
        );
        Ok(())
    }

    /// #51c-c hazard probe (the WHY of the two-table split): the SAME table
    /// WITH the `_timestamp DESC` per-file declaration but WITHOUT an
    /// ordered-aware source — a declared table wrongly fed a concat file
    /// whose scan streams stored order — elides the sort (no SortExec:
    /// DataFusion trusts the declared within-file order) and returns WRONG
    /// top-k rows. This is why exec.rs routes ONLY provably-mergeable files
    /// into the declared table and sets `with_ordered_output` there. If a
    /// DataFusion upgrade ever makes `got` equal the truth here, revisit.
    #[tokio::test]
    async fn order_by_desc_over_concat_file_with_declaration_is_the_hazard() -> Result<()> {
        let ctx = concat_order_by_ctx(true, false).await?;
        let (rendered, got) = concat_order_by_top7(&ctx).await?;
        assert!(
            !rendered.contains("SortExec"),
            "the declaration elides the sort — that elision is the hazard, got plan:\n{rendered}"
        );
        let truth: Vec<i64> = vec![320, 319, 318, 317, 316, 300, 299];
        assert_ne!(
            got, truth,
            "declared order over a concat file returned the right rows by luck — if this is a \
             DataFusion behavior change, revisit the #51c-c declaration drop;\nplan:\n{rendered}"
        );
        Ok(())
    }

    /// §6.2 M4: the declared table WITH the ordered-aware source — exactly
    /// what exec.rs builds for ts_desc + region-mergeable files — elides
    /// the sort AND returns the exact top-k: the concat file's scan k-way
    /// merges its regions, so its declared per-file order is genuinely
    /// upheld. The piecewise-order read this milestone exists for.
    #[tokio::test]
    async fn order_by_desc_over_concat_file_with_ordered_source_is_correct() -> Result<()> {
        let ctx = concat_order_by_ctx(true, true).await?;
        let (rendered, got) = concat_order_by_top7(&ctx).await?;
        assert!(
            !rendered.contains("SortExec"),
            "the declared+merged path must elide the sort, got plan:\n{rendered}"
        );
        let truth: Vec<i64> = vec![320, 319, 318, 317, 316, 300, 299];
        assert_eq!(
            got, truth,
            "region-merged ordered scan must equal the full-sort truth;\nplan:\n{rendered}"
        );
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
        selection: Option<&RowIdBitmap>,
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
        bytes::Bytes::from(writer.finish().unwrap().0)
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
        let bits = RowIdBitmap::from_row_ids(4, std::iter::empty());
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
            null_rejected_columns: Vec::new(),
            emit_ts_desc: false,
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
        bytes::Bytes::from(writer.finish().unwrap().0)
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
        let bits = RowIdBitmap::from_row_ids(4, [1u32, 2]);
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

/// M4 pruning-tier tests: field presence (§4), chunk-stats file exclusion,
/// null-semantics fail-open pins, and the native-vs-json_get routing pin.
#[cfg(test)]
mod m4_pruning_tests {
    use arrow::array::StringArray;
    use datafusion::{
        datasource::{
            listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
            object_store::{DefaultObjectStoreRegistry, ObjectStoreRegistry},
        },
        execution::{runtime_env::RuntimeEnvBuilder, session_state::SessionStateBuilder},
        physical_plan::collect,
        prelude::{SessionConfig, SessionContext},
    };
    use object_store::{ObjectStoreExt, memory::InMemory, path::Path};
    use vortex_index::{VixWriter, VixWriterOptions};

    use super::*;

    /// One file with `_timestamp` + `level` columns (optionally stamped
    /// `columns_complete`); `hidden` optionally smuggles a `ghost` field
    /// into `_source` WITHOUT a column — the deliberate invariant violation
    /// that makes a fired presence skip OBSERVABLE (a scan that reaches
    /// json_get would find the values; a skipped file returns nothing).
    fn build_presence_file(complete: bool, hidden: bool) -> bytes::Bytes {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("level", DataType::Utf8, true),
        ]));
        let opts = VixWriterOptions {
            columns_complete: complete,
            ..Default::default()
        };
        let mut writer = VixWriter::new(&schema, opts, false);
        let ts: Vec<i64> = (0..4).map(|i| 1000 - i).collect();
        let sources: Vec<String> = ts
            .iter()
            .map(|t| {
                if hidden {
                    format!(r#"{{"_timestamp":{t},"level":"info","ghost":"boo"}}"#)
                } else {
                    format!(r#"{{"_timestamp":{t},"level":"info"}}"#)
                }
            })
            .collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ts)),
                Arc::new(StringArray::from(vec![Some("info"); 4])),
            ],
        )
        .unwrap();
        let sources = StringArray::from_iter_values(sources.iter().map(String::as_str));
        writer
            .push_batch_with_source(&batch, &sources, None)
            .unwrap();
        bytes::Bytes::from(writer.finish().unwrap().0)
    }

    /// Tier-by-tier verdicts of [`file_provably_skippable`].
    #[test]
    fn skip_hook_tiers_fire_correctly() {
        // FIELD PRESENCE: a columns-complete file skips on an absent
        // null-rejected column; an incomplete file never does (fail-open)
        let complete = VixDocs::open(build_presence_file(true, false)).unwrap();
        assert_eq!(
            file_provably_skippable(&complete, None, &[], &["ghost".to_string()]),
            Some("field-presence")
        );
        // a PRESENT column with values never presence-skips
        assert_eq!(
            file_provably_skippable(&complete, None, &[], &["level".to_string()]),
            None
        );
        // reserved columns are in the docs schema, never presence-skipped
        assert_eq!(
            file_provably_skippable(&complete, None, &[], &[SOURCE_COL_NAME.to_string()]),
            None
        );
        let incomplete = VixDocs::open(build_presence_file(false, true)).unwrap();
        assert_eq!(
            file_provably_skippable(&incomplete, None, &[], &["ghost".to_string()]),
            None,
            "no completeness marker: ghost may hide in _source — fail open"
        );

        // ZERO-PRESENCE: an all-NULL column skips unconditionally (native
        // columns are authoritative for reads), completeness irrelevant
        let all_null = {
            let schema = Arc::new(Schema::new(vec![
                Field::new("_timestamp", DataType::Int64, false),
                Field::new("empty_col", DataType::Int64, true),
            ]));
            let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(vec![10i64, 9])),
                    Arc::new(Int64Array::from(vec![None::<i64>, None])),
                ],
            )
            .unwrap();
            let sources = StringArray::from(vec!["{}", "{}"]);
            writer
                .push_batch_with_source(&batch, &sources, None)
                .unwrap();
            VixDocs::open(bytes::Bytes::from(writer.finish().unwrap().0)).unwrap()
        };
        assert_eq!(
            file_provably_skippable(&all_null, None, &[], &["empty_col".to_string()]),
            Some("field-presence")
        );

        // FILE-STATS (vortex footer, first-encode numerics): code 0..=3
        let coded = {
            let schema = Arc::new(Schema::new(vec![
                Field::new("_timestamp", DataType::Int64, false),
                Field::new("code", DataType::Int64, true),
            ]));
            let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(vec![10i64, 9, 8, 7])),
                    Arc::new(Int64Array::from(vec![0i64, 1, 2, 3])),
                ],
            )
            .unwrap();
            let sources = StringArray::from(vec!["{}"; 4]);
            writer
                .push_batch_with_source(&batch, &sources, None)
                .unwrap();
            VixDocs::open(bytes::Bytes::from(writer.finish().unwrap().0)).unwrap()
        };
        let ge_code = |v: i64| {
            vec![ColumnBound {
                column: "code".to_string(),
                min: Some((BoundValue::I64(v), true)),
                max: None,
            }]
        };
        assert_eq!(
            file_provably_skippable(&coded, None, &ge_code(100), &["code".to_string()]),
            Some("file-stats")
        );
        assert_eq!(
            file_provably_skippable(&coded, None, &ge_code(2), &["code".to_string()]),
            None
        );

        // CHUNK-STATS-EMPTY: a STRING bound has no vortex file stats
        // (tier 2 silent), so the O2 chunk fold is what proves emptiness
        let eq_level = |v: &str| {
            vec![ColumnBound {
                column: "level".to_string(),
                min: Some((BoundValue::Str(v.to_string()), true)),
                max: Some((BoundValue::Str(v.to_string()), true)),
            }]
        };
        assert_eq!(
            file_provably_skippable(&complete, None, &eq_level("zzz"), &["level".to_string()]),
            Some("chunk-stats-empty")
        );
        assert_eq!(
            file_provably_skippable(&complete, None, &eq_level("info"), &["level".to_string()]),
            None
        );
    }

    async fn presence_ctx(files: Vec<(&str, bytes::Bytes)>) -> Result<SessionContext> {
        let store = Arc::new(InMemory::new());
        for (name, data) in files {
            store.put(&Path::from(name), data.into()).await?;
        }
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
        // logical schema knows `ghost` (registry type Utf8); the files may not
        let table_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("level", DataType::Utf8, true),
            Field::new("ghost", DataType::Utf8, true),
        ]));
        let listing_options =
            ListingOptions::new(Arc::new(VixCoreFormat::new(None))).with_collect_stat(true);
        let config = ListingTableConfig::new(ListingTableUrl::parse("test:///data/")?)
            .with_listing_options(listing_options)
            .with_schema(table_schema);
        ctx.register_table("t", Arc::new(ListingTable::try_new(config)?))?;
        Ok(ctx)
    }

    async fn run_counting(ctx: &SessionContext, sql: &str) -> Result<usize> {
        let df = ctx.sql(sql).await?;
        let plan = df.create_physical_plan().await?;
        // the follower-side injection pass (flight.rs applies it post-plan)
        let plan = inject_vix_scan_pruning(plan)?;
        let batches = collect(plan, ctx.task_ctx()).await?;
        Ok(batches.iter().map(|b| b.num_rows()).sum())
    }

    /// E2E §4 presence pruning through the injector + opener: the file is
    /// stamped columns-complete BUT its `_source` secretly carries `ghost`
    /// values (the deliberate lie that makes the skip observable). Every
    /// null-rejecting shape returns 0 rows — json_get was never consulted —
    /// while the null-ACCEPTING shapes return all rows, proving both the
    /// skip and its exact null-semantics boundary.
    #[tokio::test]
    async fn presence_pruning_e2e_null_semantics() -> Result<()> {
        let ctx = presence_ctx(vec![("data/lying.vix", build_presence_file(true, true))]).await?;

        // null-rejecting shapes on the absent column: the presence skip
        // fires BEFORE the scan could fall back to json_get(_source)
        for sql in [
            "SELECT _timestamp FROM t WHERE ghost = 'boo'",
            "SELECT _timestamp FROM t WHERE ghost != 'x'",
            "SELECT _timestamp FROM t WHERE ghost < 'zzz'",
            "SELECT _timestamp FROM t WHERE ghost LIKE 'b%'",
            "SELECT _timestamp FROM t WHERE ghost IN ('boo', 'bar')",
            "SELECT _timestamp FROM t WHERE ghost IS NOT NULL",
            "SELECT _timestamp FROM t WHERE str_match(ghost, 'boo')",
        ] {
            // register the udf-bearing context per query (str_match)
            crate::datafusion::exec::register_udf(&ctx, "org").unwrap();
            assert_eq!(
                run_counting(&ctx, sql).await?,
                0,
                "null-rejecting predicate must skip the file whole: {sql}"
            );
        }

        // OR across DIFFERENT columns never presence-prunes (intersection
        // is empty): the rows survive through the json_get fallback branch
        assert_eq!(
            run_counting(
                &ctx,
                "SELECT _timestamp FROM t WHERE ghost = 'boo' OR level = 'info'"
            )
            .await?,
            4
        );

        // null-ACCEPTING shapes must NOT prune. Pinned on an HONEST
        // columns-complete file whose `ghost` really is all-NULL: a wrong
        // presence skip would return 0 where the truth is every row.
        let ctx =
            presence_ctx(vec![("data/honest.vix", build_presence_file(true, false))]).await?;
        for sql in [
            "SELECT _timestamp FROM t WHERE ghost IS NULL",
            "SELECT _timestamp FROM t WHERE COALESCE(ghost, 'x') = 'x'",
            "SELECT _timestamp FROM t WHERE ghost IS DISTINCT FROM 'boo'",
        ] {
            assert_eq!(
                run_counting(&ctx, sql).await?,
                4,
                "null-accepting predicate must keep every row: {sql}"
            );
        }
        Ok(())
    }

    /// An honest INCOMPLETE file (no marker) with `ghost` only in `_source`
    /// keeps the json_get fallback: the same equality returns the rows.
    #[tokio::test]
    async fn incomplete_file_keeps_json_get_fallback() -> Result<()> {
        let ctx =
            presence_ctx(vec![("data/honest.vix", build_presence_file(false, true))]).await?;
        assert_eq!(
            run_counting(&ctx, "SELECT _timestamp FROM t WHERE ghost = 'boo'").await?,
            4,
            "without the completeness marker the scan must fall back to json_get"
        );
        Ok(())
    }

    /// Task-5 pin: a column PRESENT in the file is read natively (no
    /// `_source` fetch, no json_get), an ABSENT one is a `json_get`
    /// extraction over `_source` with NULL-correct semantics.
    #[test]
    fn present_column_native_absent_column_json_get() {
        let docs = VixDocs::open(build_presence_file(false, true)).unwrap();

        // present only: native reference, _source NOT fetched
        let present_only: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "level",
            DataType::Utf8,
            true,
        )]));
        let plan = LogicalProjectionPlan::new(&docs, &present_only).unwrap();
        assert_eq!(plan.physical_projection, vec!["level"]);

        // absent column: _source fetched exactly once, extraction is NULL
        // where _source lacks the key and the value where it has it
        let mixed: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("level", DataType::Utf8, true),
            Field::new("ghost", DataType::Utf8, true),
            Field::new("never_anywhere", DataType::Utf8, true),
        ]));
        let plan = LogicalProjectionPlan::new(&docs, &mixed).unwrap();
        assert_eq!(plan.physical_projection, vec!["level", SOURCE_COL_NAME]);

        let mut batches = Vec::new();
        scan_core_docs(docs, &mixed, None, None, &[], &mut |batch| {
            batches.push(batch);
            Ok(())
        })
        .unwrap();
        let batch = &batches[0];
        let level = batch.column_by_name("level").unwrap();
        let level = level.as_any().downcast_ref::<StringArray>().unwrap();
        let ghost = batch.column_by_name("ghost").unwrap();
        let ghost = ghost.as_any().downcast_ref::<StringArray>().unwrap();
        let never = batch.column_by_name("never_anywhere").unwrap();
        for row in 0..batch.num_rows() {
            assert_eq!(level.value(row), "info", "native column read");
            assert_eq!(ghost.value(row), "boo", "json_get extraction");
            assert!(never.is_null(row), "absent everywhere: NULL");
        }
    }
}

/// Walk a follower's physical plan; for every `FilterExec` sitting (through
/// pass-through nodes) above a vix scan, extract from its top-level-AND
/// conjuncts:
///
/// - RANGE BOUNDS (`col > | >= | < | <= | = literal`, numeric or string, type-family-matching the
///   scan schema; plus the min/max fold of a non-negated all-literal `IN` list) — pushed into the
///   scan for vortex row filtering (numerics) and O2 chunk/file stats pruning (strings included,
///   conservative prefix logic);
/// - NULL-REJECTED COLUMNS: plain columns on which the conjunct can NEVER hold for a NULL cell
///   (`=`, `!=`, `<`…, `LIKE`/`NOT LIKE`, `IN`/`NOT IN` over non-null literals, `IS NOT NULL`,
///   `str_match`/`match_field`/`fuzzy_match`) — the §4 field-presence file skip. Null-ACCEPTING
///   shapes (`IS NULL`, `IS [NOT] DISTINCT FROM`, `coalesce(...)`, OR branches, NOT wrappers,
///   casts) are deliberately NOT extracted: when in doubt, don't prune.
///
/// The FilterExec stays — pruning is conservative-only and every returned
/// row is re-checked.
pub fn inject_vix_scan_pruning(
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

    fn literal_value(value: &ScalarValue) -> Option<BoundValue> {
        match value {
            ScalarValue::Int8(Some(v)) => Some(BoundValue::I64(*v as i64)),
            ScalarValue::Int16(Some(v)) => Some(BoundValue::I64(*v as i64)),
            ScalarValue::Int32(Some(v)) => Some(BoundValue::I64(*v as i64)),
            ScalarValue::Int64(Some(v)) => Some(BoundValue::I64(*v)),
            ScalarValue::UInt8(Some(v)) => Some(BoundValue::U64(*v as u64)),
            ScalarValue::UInt16(Some(v)) => Some(BoundValue::U64(*v as u64)),
            ScalarValue::UInt32(Some(v)) => Some(BoundValue::U64(*v as u64)),
            ScalarValue::UInt64(Some(v)) => Some(BoundValue::U64(*v)),
            ScalarValue::Float32(Some(v)) => Some(BoundValue::F64(*v as f64)),
            ScalarValue::Float64(Some(v)) => Some(BoundValue::F64(*v)),
            ScalarValue::Utf8(Some(v))
            | ScalarValue::LargeUtf8(Some(v))
            | ScalarValue::Utf8View(Some(v)) => Some(BoundValue::Str(v.clone())),
            _ => None,
        }
    }

    fn value_matches_type(value: &BoundValue, data_type: &DataType) -> bool {
        matches!(
            (value, data_type),
            (
                BoundValue::I64(_),
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            ) | (
                BoundValue::U64(_),
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
            ) | (BoundValue::F64(_), DataType::Float32 | DataType::Float64)
                | (
                    BoundValue::Str(_),
                    DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                )
        )
    }

    /// Whether the conjunct's literal is non-null (a `col = NULL` shape is
    /// never true, but keep it out of the analysis entirely).
    fn non_null_literal(literal: &phys::Literal) -> bool {
        !literal.value().is_null()
    }

    #[derive(Default)]
    struct Extracted {
        bounds: Vec<ColumnBound>,
        null_rejected: Vec<String>,
    }

    impl Extracted {
        fn reject_null(&mut self, column: &str) {
            if !self.null_rejected.iter().any(|c| c == column) {
                self.null_rejected.push(column.to_string());
            }
        }
    }

    /// The columns a predicate is null-rejecting on — NULL in the column
    /// makes the predicate non-true, whatever the other columns hold. AND
    /// unions its branches (any branch failing kills the row); OR
    /// INTERSECTS them (every branch must fail on NULL — this catches the
    /// `IN`-list shapes DataFusion rewrites into `=` OR-chains). Leaves are
    /// the allowlisted shapes; anything else contributes nothing.
    fn null_rejected_of(predicate: &Arc<dyn PhysicalExpr>, out: &mut Extracted) {
        if let Some(binary) = predicate.downcast_ref::<phys::BinaryExpr>() {
            match binary.op() {
                Operator::And => {
                    null_rejected_of(binary.left(), out);
                    null_rejected_of(binary.right(), out);
                    return;
                }
                Operator::Or => {
                    let mut left = Extracted::default();
                    let mut right = Extracted::default();
                    null_rejected_of(binary.left(), &mut left);
                    null_rejected_of(binary.right(), &mut right);
                    for column in left.null_rejected {
                        if right.null_rejected.iter().any(|c| *c == column) {
                            out.reject_null(&column);
                        }
                    }
                    return;
                }
                _ => {}
            }
        }
        // leaves share the AND-conjunct analysis, bounds discarded
        let mut leaf = Extracted::default();
        extract(predicate, &Schema::empty(), &mut leaf);
        for column in leaf.null_rejected {
            out.reject_null(&column);
        }
    }

    fn extract(predicate: &Arc<dyn PhysicalExpr>, schema: &Schema, out: &mut Extracted) {
        if let Some(binary) = predicate.downcast_ref::<phys::BinaryExpr>() {
            if matches!(binary.op(), Operator::And) {
                extract(binary.left(), schema, out);
                extract(binary.right(), schema, out);
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
                        Operator::NotEq => Operator::NotEq,
                        _ => return,
                    };
                    (column, literal, flipped)
                }
                _ => return,
            };
            // Every plain comparison against a NON-NULL literal is
            // null-rejecting (NULL op x = NULL, filtered out). NOTE:
            // IsDistinctFrom/IsNotDistinctFrom are DIFFERENT operators and
            // never reach here as Eq/NotEq — they accept NULLs.
            if !matches!(
                op,
                Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq
            ) || !non_null_literal(literal)
            {
                return;
            }
            out.reject_null(column.name());
            let Some(value) = literal_value(literal.value()) else {
                return;
            };
            let Ok(field) = schema.field_with_name(column.name()) else {
                return;
            };
            if !value_matches_type(&value, field.data_type()) {
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
                    min: Some((value.clone(), true)),
                    max: Some((value, true)),
                },
                // `!=` rejects NULL but bounds nothing
                _ => return,
            };
            out.bounds.push(bound);
            return;
        }
        // col [NOT] LIKE 'pattern': NULL LIKE p is NULL either way
        if let Some(like) = predicate.downcast_ref::<phys::LikeExpr>() {
            if let Some(column) = like.expr().downcast_ref::<phys::Column>()
                && let Some(literal) = like.pattern().downcast_ref::<phys::Literal>()
                && non_null_literal(literal)
            {
                out.reject_null(column.name());
            }
            return;
        }
        // col [NOT] IN (l1, l2, ...): NULL IN (...) is NULL either way when
        // the list is all non-null literals; the non-negated list also
        // folds to an inclusive [min, max] bound
        if let Some(in_list) = predicate.downcast_ref::<phys::InListExpr>() {
            let Some(column) = in_list.expr().downcast_ref::<phys::Column>() else {
                return;
            };
            let literals: Option<Vec<BoundValue>> = in_list
                .list()
                .iter()
                .map(|item| {
                    item.downcast_ref::<phys::Literal>()
                        .filter(|l| non_null_literal(l))
                        .and_then(|l| literal_value(l.value()))
                })
                .collect();
            let Some(values) = literals else {
                return; // non-literal or NULL member: fail-open
            };
            if values.is_empty() {
                return;
            }
            out.reject_null(column.name());
            if in_list.negated() {
                return; // NOT IN rejects NULL but bounds nothing
            }
            if let Ok(field) = schema.field_with_name(column.name())
                && values.iter().all(|v| value_matches_type(v, field.data_type()))
                && let (Some(min), Some(max)) = (
                    values.iter().cloned().reduce(|a, b| bound_min(a, b)),
                    values.iter().cloned().reduce(|a, b| bound_max(a, b)),
                )
            {
                out.bounds.push(ColumnBound {
                    column: column.name().to_string(),
                    min: Some((min, true)),
                    max: Some((max, true)),
                });
            }
            return;
        }
        // col IS NOT NULL: rejects NULL by definition
        if let Some(is_not_null) = predicate.downcast_ref::<phys::IsNotNullExpr>() {
            if let Some(column) = is_not_null.arg().downcast_ref::<phys::Column>() {
                out.reject_null(column.name());
            }
            return;
        }
        // str_match/match_field/fuzzy_match(col, 'value'): the arrow string
        // kernels propagate NULL input to a non-true output
        if let Some(udf) = predicate.downcast_ref::<ScalarFunctionExpr>() {
            use crate::datafusion::udf::{
                FUZZY_MATCH_UDF_NAME, MATCH_FIELD_IGNORE_CASE_UDF_NAME, MATCH_FIELD_UDF_NAME,
                STR_MATCH_UDF_IGNORE_CASE_NAME, STR_MATCH_UDF_NAME,
            };
            let null_rejecting = matches!(
                udf.name(),
                STR_MATCH_UDF_NAME
                    | STR_MATCH_UDF_IGNORE_CASE_NAME
                    | MATCH_FIELD_UDF_NAME
                    | MATCH_FIELD_IGNORE_CASE_UDF_NAME
                    | FUZZY_MATCH_UDF_NAME
            );
            if null_rejecting
                && let Some(column) = udf.args().first().and_then(|a| a.downcast_ref::<phys::Column>())
            {
                out.reject_null(column.name());
            }
        }
        // everything else (IS NULL, IS [NOT] DISTINCT FROM, coalesce, OR,
        // NOT, casts, ...): fail-open — extracted nothing
    }

    /// Total order over same-family bound values (IN-list folding; the
    /// caller has type-gated the family). NaN floats sort last (max) /
    /// first (min) harmlessly — the fold stays conservative.
    fn bound_min(a: BoundValue, b: BoundValue) -> BoundValue {
        if bound_le(&a, &b) { a } else { b }
    }
    fn bound_max(a: BoundValue, b: BoundValue) -> BoundValue {
        if bound_le(&a, &b) { b } else { a }
    }
    fn bound_le(a: &BoundValue, b: &BoundValue) -> bool {
        match (a, b) {
            (BoundValue::I64(a), BoundValue::I64(b)) => a <= b,
            (BoundValue::U64(a), BoundValue::U64(b)) => a <= b,
            (BoundValue::F64(a), BoundValue::F64(b)) => a <= b || b.is_nan(),
            (BoundValue::Str(a), BoundValue::Str(b)) => a <= b,
            _ => true, // mixed families are type-gated out before folding
        }
    }

    /// Inject the extraction into every vix DataSourceExec under `node`,
    /// passing through repartition/coalesce/projection-free nodes only (a
    /// projection can rename columns — stop there).
    fn inject(
        node: Arc<dyn ExecutionPlan>,
        extracted: &Extracted,
    ) -> datafusion::common::Result<Transformed<Arc<dyn ExecutionPlan>>> {
        if let Some(exec) = node.downcast_ref::<DataSourceExec>()
            && let Some(conf) = exec.data_source().downcast_ref::<FileScanConfig>()
        {
            let source: &dyn std::any::Any = conf.file_source.as_ref();
            if let Some(source) = source.downcast_ref::<VixCoreSource>() {
                let mut source = source.clone();
                let mut merged = source.column_bounds.clone();
                merged.extend(extracted.bounds.iter().cloned());
                source.column_bounds = merged;
                for column in &extracted.null_rejected {
                    if !source.null_rejected_columns.iter().any(|c| c == column) {
                        source.null_rejected_columns.push(column.clone());
                    }
                }
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
                inject(Arc::clone(child), extracted).map(|t| {
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
        let mut extracted = Extracted::default();
        extract(filter.predicate(), &filter.input().schema(), &mut extracted);
        // the null-rejection walk also crosses OR (intersection semantics),
        // catching the IN-list shapes the planner rewrites to `=` OR-chains
        let mut or_aware = Extracted::default();
        null_rejected_of(filter.predicate(), &mut or_aware);
        for column in or_aware.null_rejected {
            extracted.reject_null(&column);
        }
        if extracted.bounds.is_empty() && extracted.null_rejected.is_empty() {
            return Ok(Transformed::no(node));
        }
        let injected = inject(Arc::clone(filter.input()), &extracted)?;
        if !injected.transformed {
            log::debug!(
                "vix scan pruning: {} bound(s) / {} null-rejected column(s) extracted but no \
                 vix scan adjacent",
                extracted.bounds.len(),
                extracted.null_rejected.len(),
            );
            return Ok(Transformed::no(node));
        }
        log::info!(
            "vix scan pruning: injected {} bound(s), {} null-rejected column(s)",
            extracted.bounds.len(),
            extracted.null_rejected.len(),
        );
        log::debug!(
            "vix scan pruning details: bounds {:?}, null-rejected {:?}",
            extracted.bounds,
            extracted.null_rejected,
        );
        Ok(Transformed::yes(
            node.with_new_children(vec![injected.data])?,
        ))
    })
    .map(|t| t.data)
}
