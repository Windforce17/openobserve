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

//! Lightweight scan access to the `docs` blob of a `.vix` core file.
//!
//! [`VixDocs`] is the scan-path counterpart of [`crate::VixReader`]: it
//! parses only the puffin envelope and the `docs` blob footer — the term
//! dictionary (FSTs) is never touched — so opening a file for a table scan
//! stays cheap. The embedded `docs` blob is a complete Vortex file; scans
//! support column projection, row-index selection (the output of an
//! inverted-index match) and a `_timestamp` range filter pushed down to the
//! Vortex layer (zone-map pruned).
//!
//! [`VixDocs::open_ranged`] opens the same structure over a
//! [`VixRangeSource`] instead of complete bytes: one tail fetch parses the
//! puffin footer, one more the docs-blob Vortex footer (cached for the
//! scans), and scans then fetch only the chunks their projection/selection
//! touches — a point read of a few rows never downloads the object.

use std::sync::Arc;

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use bytes::Bytes;
use vortex::expr::{and, get_item, gt_eq, lit, lt, root};

/// A numeric literal for pushed-down column bounds ([`ColumnBound`]).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum NumScalar {
    I64(i64),
    F64(f64),
}

/// One bound value of a pushed-down range conjunct. Numeric values push
/// into the vortex row filter AND prune chunks through the O2 stats blob;
/// string values are STATS-ONLY (chunk/file pruning against the blob's
/// conservative prefix bounds — never a row filter, the engine's own
/// FilterExec re-applies the predicate).
#[derive(Debug, Clone, PartialEq)]
pub enum BoundValue {
    I64(i64),
    U64(u64),
    F64(f64),
    Str(String),
}

impl BoundValue {
    /// The vortex literal for the ROW-FILTER push, gated on the stored
    /// column type family (a family mismatch pushes nothing — the bound
    /// still prunes through the stats blob, and the engine re-filters).
    fn to_row_filter_lit(
        &self,
        stored: &arrow::datatypes::DataType,
    ) -> Option<vortex::expr::Expression> {
        use arrow::datatypes::DataType;
        match (self, stored) {
            (
                BoundValue::I64(v),
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64,
            ) => Some(lit(*v)),
            (
                BoundValue::U64(v),
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64,
            ) => Some(lit(*v)),
            (BoundValue::F64(v), DataType::Float32 | DataType::Float64) => Some(lit(*v)),
            _ => None,
        }
    }
}

/// One range conjunct pushed into a docs scan: optional min/max, each with
/// an inclusive flag. Composes `>`, `>=`, `<`, `<=`, `=` (min==max
/// inclusive) and BETWEEN. The extraction site guarantees the represented
/// predicate is NULL-REJECTING (a NULL cell can never satisfy it), which is
/// what lets zero-presence chunks prune.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnBound {
    pub column: String,
    pub min: Option<(BoundValue, bool)>,
    pub max: Option<(BoundValue, bool)>,
}

use crate::{
    container::{
        BlobHandle, PROP_COLUMNS, PROP_ROW_COUNT, PROP_ROW_GROUP_SIZE, PROP_ROW_ORDER,
        PROP_ZONE_MAP, RowOrder, RowSelection, VixContainer, blob_arrow_schema, parse_container,
        parse_container_ranged, require_supported_data_format, scan_blob_encoded_chunks,
        scan_blob_streaming,
    },
    error::{Result, VixError},
    source::VixRangeSource,
    stats::SpliceableStats,
    writer::TIMESTAMP_COL_NAME,
};

/// One already-encoded docs chunk streamed by
/// [`VixDocs::scan_docs_encoded_chunks`] (#51c): a vortex struct array in
/// the docs blob's stored (compressed) form — opaque to callers, consumed by
/// `VixWriter::push_docs_encoded_chunk`, which copies it into a merged file
/// without decoding or recompressing its columns.
pub struct EncodedDocsChunk {
    pub(crate) array: vortex::array::ArrayRef,
    rows: usize,
}

impl EncodedDocsChunk {
    /// Number of docs rows in the chunk.
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Test constructor: wrap a hand-built encoded struct chunk (encoding
    /// fixtures the samplers refuse to produce on synthetic data).
    #[cfg(test)]
    pub(crate) fn for_tests(array: vortex::array::ArrayRef, rows: usize) -> Self {
        Self { array, rows }
    }
}

impl std::fmt::Debug for EncodedDocsChunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncodedDocsChunk")
            .field("rows", &self.rows)
            .finish()
    }
}

/// M17 (gen-1 encode-once): how one passthrough input's encoded docs chunks
/// widen to the merge OUTPUT's docs schema without decoding anything.
///
/// v2 all-present-columns files carry per-file schema UNIONS, so a
/// multi-input merge's output schema is almost always a strict superset of
/// each input's — the historical passthrough required exact schema identity
/// and every gen-1 rebuild re-encoded every byte over it. A widen plan maps
/// each OUTPUT column to the input column holding it, or to a synthesized
/// all-null column (a [`vortex constant`] — it encodes to ~nothing); shared
/// columns must match at the STORED (vortex) dtype exactly, same as the
/// identity check. [`Self::widen`] then rebuilds each scanned struct chunk
/// in output shape: moved field arrays stay in their stored encoded form
/// (the whole point), null columns are constants, and the writer's
/// encoded-run dtype check still guards the result.
///
/// NEVER a re-encode: a schema pair this plan cannot express (a shared
/// column with a different stored dtype — a genuine type flip) refuses at
/// construction and the caller falls open to the decode path for that
/// input, counted in the merge summary.
pub struct DocsWidenPlan {
    /// Output field position -> input field position; `None` = synthesize
    /// an all-null constant of the output field's dtype.
    mapping: Vec<Option<usize>>,
    /// Output struct shape for chunk reassembly.
    names: vortex::dtype::FieldNames,
    dtypes: Vec<vortex::dtype::DType>,
    /// Expected input struct field count (chunk sanity check).
    input_fields: usize,
    /// Input == output at the stored-dtype level: [`Self::widen`] is a
    /// zero-cost passthrough.
    identity: bool,
}

impl std::fmt::Debug for DocsWidenPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DocsWidenPlan")
            .field("identity", &self.identity)
            .field("input_fields", &self.input_fields)
            .field("output_fields", &self.mapping.len())
            .field(
                "null_synthesized",
                &self.mapping.iter().filter(|m| m.is_none()).count(),
            )
            .finish()
    }
}

/// Build the widen plan from one input's docs schema to the output writer's
/// docs schema (see [`DocsWidenPlan`]). `Err` carries the human reason the
/// input must take the decode path instead:
///
/// - a shared column whose stored (vortex) dtype differs — type widening is a real re-encode;
/// - an input column ABSENT from the output — impossible when the output schema is the union of the
///   inputs' (refused defensively);
/// - an output column missing from the input that is NOT nullable — nothing can null-fill it
///   (`_timestamp`/`_source` are always present in practice).
pub fn docs_widen_plan(
    input: &arrow::datatypes::Schema,
    output: &arrow::datatypes::Schema,
) -> std::result::Result<DocsWidenPlan, String> {
    use vortex::{arrow::FromArrowType, dtype::DType};
    let out_dtype = DType::from_arrow(output);
    let Some(struct_fields) = out_dtype.as_struct_fields_opt() else {
        return Err("output docs schema is not a struct dtype".to_string());
    };
    let names = struct_fields.names().clone();
    let dtypes: Vec<DType> = struct_fields.fields().collect();
    let identity = DType::from_arrow(input) == out_dtype;
    if identity {
        return Ok(DocsWidenPlan {
            mapping: (0..output.fields().len()).map(Some).collect(),
            names,
            dtypes,
            input_fields: input.fields().len(),
            identity: true,
        });
    }
    // by-name mapping: input schemas keep writer order (_timestamp, sorted
    // cs fields, _source, _original) but the reassembly is positional in
    // OUTPUT order, so only name+dtype identity matters per column
    let mut mapping: Vec<Option<usize>> = Vec::with_capacity(output.fields().len());
    let mut used = 0usize;
    for (position, field) in output.fields().iter().enumerate() {
        match input.index_of(field.name()) {
            Ok(index) => {
                let theirs = DType::from_arrow(input.field(index));
                if theirs != dtypes[position] {
                    return Err(format!(
                        "docs column {:?} stores dtype {} but the output stores {} — type \
                         widening is a re-encode, not a chunk copy",
                        field.name(),
                        theirs,
                        dtypes[position]
                    ));
                }
                used += 1;
                mapping.push(Some(index));
            }
            Err(_) => {
                if !dtypes[position].is_nullable() {
                    return Err(format!(
                        "output docs column {:?} is non-nullable but absent from the input — \
                         nothing can null-fill it",
                        field.name()
                    ));
                }
                mapping.push(None);
            }
        }
    }
    if used != input.fields().len() {
        // an input column the output does not store would silently drop
        let extra: Vec<&str> = input
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .filter(|name| output.index_of(name).is_err())
            .collect();
        return Err(format!(
            "input docs column(s) {extra:?} are absent from the output schema — copying would \
             drop their values"
        ));
    }
    Ok(DocsWidenPlan {
        mapping,
        names,
        dtypes,
        input_fields: input.fields().len(),
        identity: false,
    })
}

impl DocsWidenPlan {
    /// Whether [`Self::widen`] is a zero-cost passthrough.
    pub fn is_identity(&self) -> bool {
        self.identity
    }

    /// Output columns synthesized as all-null constants (for the merge
    /// summary accounting).
    pub fn null_columns(&self) -> usize {
        if self.identity {
            0
        } else {
            self.mapping.iter().filter(|m| m.is_none()).count()
        }
    }

    /// Rebuild one scanned encoded chunk in the OUTPUT struct shape: moved
    /// columns keep their stored encoded arrays verbatim, missing columns
    /// become all-null constants of the output dtype (encode to ~nothing).
    /// Chunk-level surgery only — no column data is ever decoded here.
    pub fn widen(&self, chunk: EncodedDocsChunk) -> anyhow::Result<EncodedDocsChunk> {
        use vortex::{
            array::{
                IntoArray,
                arrays::{ConstantArray, Struct, StructArray, struct_::StructArrayExt},
                validity::Validity,
            },
            scalar::Scalar,
        };
        if self.identity {
            return Ok(chunk);
        }
        let rows = chunk.rows;
        let sa = chunk
            .array
            .as_typed::<Struct>()
            .ok_or_else(|| {
                VixError::Malformed("encoded docs chunk is not a struct array".to_string())
            })?
            .clone();
        let input_fields = sa.unmasked_fields();
        if input_fields.len() != self.input_fields {
            return Err(VixError::Malformed(format!(
                "encoded docs chunk carries {} fields but the widen plan mapped {}",
                input_fields.len(),
                self.input_fields
            ))
            .into());
        }
        let fields: Vec<vortex::array::ArrayRef> = self
            .mapping
            .iter()
            .zip(&self.dtypes)
            .map(|(source, dtype)| match source {
                Some(index) => {
                    let field = input_fields[*index].clone();
                    if field.dtype() != dtype {
                        return Err(VixError::Malformed(format!(
                            "widen plan mapped a column of dtype {} into an output slot of \
                             dtype {dtype}",
                            field.dtype()
                        )));
                    }
                    Ok(field)
                }
                None => Ok(ConstantArray::new(Scalar::null(dtype.clone()), rows).into_array()),
            })
            .collect::<Result<_>>()?;
        let array = StructArray::try_new(self.names.clone(), fields, rows, Validity::NonNullable)
            .map_err(|e| VixError::Malformed(format!("widen docs chunk: {e}")))?
            .into_array();
        Ok(EncodedDocsChunk { array, rows })
    }
}

/// Bounded exact top-N candidates. The heap root is always the weakest
/// retained row, so memory is O(limit) even for an unordered broad match.
enum CandidateHeap {
    Oldest {
        limit: usize,
        rows: std::collections::BinaryHeap<(i64, u32)>,
    },
    Newest {
        limit: usize,
        rows: std::collections::BinaryHeap<std::cmp::Reverse<(i64, u32)>>,
    },
}

impl CandidateHeap {
    fn new(limit: usize, ascend: bool) -> Self {
        if ascend {
            Self::Oldest {
                limit,
                rows: std::collections::BinaryHeap::with_capacity(limit),
            }
        } else {
            Self::Newest {
                limit,
                rows: std::collections::BinaryHeap::with_capacity(limit),
            }
        }
    }

    fn push(&mut self, candidate: (i64, u32)) {
        match self {
            Self::Oldest { limit, rows } => {
                if rows.len() < *limit {
                    rows.push(candidate);
                } else if rows.peek().is_some_and(|weakest| candidate < *weakest) {
                    rows.pop();
                    rows.push(candidate);
                }
            }
            Self::Newest { limit, rows } => {
                if rows.len() < *limit {
                    rows.push(std::cmp::Reverse(candidate));
                } else if rows.peek().is_some_and(|weakest| candidate > weakest.0) {
                    rows.pop();
                    rows.push(std::cmp::Reverse(candidate));
                }
            }
        }
    }

    fn into_best_first(self) -> Vec<(i64, u32)> {
        match self {
            Self::Oldest { rows, .. } => {
                let mut rows = rows.into_vec();
                rows.sort_unstable();
                rows
            }
            Self::Newest { rows, .. } => {
                let mut rows: Vec<_> = rows.into_iter().map(|row| row.0).collect();
                rows.sort_unstable_by(|a, b| b.cmp(a));
                rows
            }
        }
    }
}

/// Scan handle over the `docs` blob of one `.vix` core file — held fully
/// in memory or fetched by ranges on demand. Opening parses the puffin
/// footer and the blob's arrow schema only.
#[derive(Debug)]
pub struct VixDocs {
    row_count: u64,
    row_group_size: usize,
    docs_blob: BlobHandle,
    schema: SchemaRef,
    /// Physical row order (`row_order` property, #51c-c). Missing ==
    /// [`RowOrder::TsDesc`] (every historical file is sorted).
    row_order: RowOrder,
    /// Exact file-level `_timestamp` bounds from the zone table, parsed at
    /// open ONLY for non-sorted (concat) files — the stats source replacing
    /// the first/last-row read, whose DESC assumption such files break.
    /// Always `None` on sorted files (their bounds come from the boundary
    /// rows as before) and on a concat file whose zone table is absent or
    /// untrustworthy (the caller then reports unknown bounds, fail-open).
    zone_ts_bounds: Option<(i64, i64)>,
    /// Per-column present-row counts from the `columns` property (`None`
    /// count = unknown, an M1 plain-name entry). Empty when the property is
    /// absent/unreadable (defensive; every v3 writer stamps it).
    column_presence: Vec<(String, Option<u64>)>,
    /// The H2 per-column chunk-stats blob (absent on pre-stats and empty
    /// files — readers fail open).
    stats_blob: Option<BlobHandle>,
    /// §4 REGION table of a concat file (`row_regions` property, validated):
    /// per-region row counts in stored order, each region internally
    /// `_timestamp` DESC. `None` on ts_desc files (whole file = one region)
    /// and on concat files without a proven decomposition.
    row_regions: Option<Vec<u64>>,
    /// Full `_timestamp` zone table (`zone_map` property) with derived row
    /// offsets, validated to cover `row_count` — the chunk-pruning and
    /// region-merge granularity. `None` when absent/untrustworthy
    /// (fail-open: no chunk pruning, no region ts bounds).
    zone_chunks: Option<Vec<crate::reader::ZoneChunk>>,
    /// Lazily decoded per-column chunk stats (`stats` blob) for scan-side
    /// pruning. `None` inside = no blob or undecodable (fail-open).
    decoded_stats: std::sync::OnceLock<Option<crate::stats::FileColumnStats>>,
    /// §4: the file asserts the all-present-columns invariant
    /// (`columns_complete` property) — the license for absent-column file
    /// pruning. `false` when absent (fail-open).
    columns_complete: bool,
}

impl VixDocs {
    /// Open the `docs` blob of a core file from its complete bytes.
    pub fn open(data: Bytes) -> anyhow::Result<Self> {
        Ok(Self::open_inner(data)?)
    }

    /// Open the `docs` blob of a core file over a ranged source: the
    /// puffin footer comes from a tail fetch and the docs blob is opened as
    /// a Vortex file over its byte window — scans fetch only the chunks
    /// they touch. Blocks on fetches — call from a blocking thread, never
    /// on an async executor.
    pub fn open_ranged(source: Arc<dyn VixRangeSource>) -> anyhow::Result<Self> {
        let container = parse_container_ranged(&source)?;
        Ok(Self::from_container(container)?)
    }

    fn open_inner(data: Bytes) -> Result<Self> {
        let container = parse_container(&data)?;
        Self::from_container(container)
    }

    fn from_container(container: VixContainer) -> Result<Self> {
        let properties = &container.properties;
        require_supported_data_format(properties)?;
        let row_count: u64 = properties
            .get(PROP_ROW_COUNT)
            .ok_or_else(|| {
                VixError::Malformed(format!("missing file property {PROP_ROW_COUNT:?}"))
            })?
            .parse()
            .map_err(|_| {
                VixError::Malformed(format!("property {PROP_ROW_COUNT:?} is not an integer"))
            })?;
        let row_group_size = match properties.get(PROP_ROW_GROUP_SIZE) {
            None => 0,
            Some(raw) => raw.parse().map_err(|_| {
                VixError::Malformed(format!(
                    "property {PROP_ROW_GROUP_SIZE:?} is not an integer: {raw:?}"
                ))
            })?,
        };
        let row_order = RowOrder::from_property(properties.get(PROP_ROW_ORDER).map(String::as_str));
        // Full zone table for every file (M4): the per-chunk row ranges are
        // the granularity of scan-side chunk pruning (per-column stats and
        // the pushed `_timestamp` range) and of the concat region merge.
        // The parse is footer-local and tiny (3 ints per chunk).
        let zone_chunks = crate::reader::parse_zone_map(
            properties.get(PROP_ZONE_MAP).map(String::as_str),
            row_count,
        );
        // Zone-derived ts bounds only for NON-sorted files: sorted files
        // keep their boundary-row read as the stats source.
        let zone_ts_bounds = if row_order.is_ts_desc() {
            None
        } else {
            zone_chunks.as_deref().and_then(|chunks| {
                let min = chunks.iter().map(|c| c.ts_min).min()?;
                let max = chunks.iter().map(|c| c.ts_max).max()?;
                Some((min, max))
            })
        };
        let row_regions = if row_order.is_ts_desc() {
            None
        } else {
            crate::container::parse_row_regions(
                properties
                    .get(crate::container::PROP_ROW_REGIONS)
                    .map(String::as_str),
                row_count,
            )
        };
        let column_presence = properties
            .get(PROP_COLUMNS)
            .map(|raw| crate::stats::parse_columns_prop(raw))
            .transpose()?
            .unwrap_or_default();
        let columns_complete = properties
            .get(crate::container::PROP_COLUMNS_COMPLETE)
            .is_some_and(|v| v == "true");
        let docs_blob = container
            .docs
            .ok_or_else(|| VixError::Malformed("missing docs blob".to_string()))?;
        // Footer/dtype only — on a ranged blob this fetches (and caches)
        // the docs-blob Vortex footer; the scans reuse it.
        let schema = Arc::new(blob_arrow_schema(&docs_blob)?);
        Ok(Self {
            row_count,
            row_group_size,
            docs_blob,
            schema,
            row_order,
            zone_ts_bounds,
            column_presence,
            stats_blob: container.stats,
            row_regions,
            zone_chunks,
            decoded_stats: std::sync::OnceLock::new(),
            columns_complete,
        })
    }

    /// Per-column present-row counts (`columns` property; `None` count =
    /// unknown, an M1 plain-name entry).
    pub fn column_presence(&self) -> &[(String, Option<u64>)] {
        &self.column_presence
    }

    /// §4: whether the file asserts the all-present-columns invariant —
    /// every field present in any row's `_source` is a docs column, so a
    /// field ABSENT from [`Self::column_presence`] is provably all-NULL.
    /// `false` (absent property) forbids absent-column pruning (fail-open).
    pub fn columns_complete(&self) -> bool {
        self.columns_complete
    }

    /// The file's spliceable stats (H2, DESIGN §4): the per-column chunk
    /// table from the `stats` blob plus the file-level presence counts.
    /// `Ok(None)` when the file carries NO stats blob (pre-stats file, or
    /// empty file) — such an input cannot feed a stats-preserving
    /// passthrough and must decode.
    pub fn spliceable_stats(&self) -> anyhow::Result<Option<SpliceableStats>> {
        let Some(blob) = &self.stats_blob else {
            return Ok(None);
        };
        let bytes = blob.bytes()?;
        let chunks = crate::stats::decode_stats_blob(&bytes)?;
        Ok(Some(SpliceableStats {
            presence: self.column_presence.clone(),
            chunks,
        }))
    }

    /// Number of documents stored in the file (`row_count` property).
    /// Byte length of the `docs` blob alone (no fetch) — the verbatim-copy
    /// storage-parity comparisons want the data-bearing blob, not the
    /// container (per-file footer/stats overheads dominate small files).
    pub fn docs_blob_len(&self) -> u64 {
        self.docs_blob.len()
    }

    #[cfg(test)]
    pub(crate) fn docs_blob_handle(&self) -> &crate::container::BlobHandle {
        &self.docs_blob
    }

    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    /// Physical row order of the stored rows (`row_order` property, #51c-c):
    /// [`RowOrder::TsDesc`] for every historical file (missing property) and
    /// every sorted writer output; [`RowOrder::Concat`] for
    /// concatenation-order merge outputs (and unknown future values — the
    /// fail-safe reading). Callers deriving anything from stored order
    /// (newest == first row, per-file sort declarations) must check this
    /// first.
    pub fn row_order(&self) -> RowOrder {
        self.row_order
    }

    /// The validated per-chunk `_timestamp` zone table (`zone_map`
    /// property) with derived row offsets — the chunk-pruning granularity.
    /// `None` = untrustworthy/absent (fail-open: no pruning).
    pub fn zone_chunks(&self) -> Option<&[crate::reader::ZoneChunk]> {
        self.zone_chunks.as_deref()
    }

    /// §4: the file's internally-`_timestamp`-DESC row ranges, in stored
    /// order (the piecewise-order decomposition). One full-file range for a
    /// ts_desc file; the validated `row_regions` table for a concat file;
    /// `None` for a concat file without a proven decomposition (callers
    /// must not assume ANY order — full sort only).
    pub fn ts_desc_row_ranges(&self) -> Option<Vec<std::ops::Range<u64>>> {
        if self.row_order.is_ts_desc() {
            return Some(if self.row_count == 0 {
                Vec::new()
            } else {
                vec![0..self.row_count]
            });
        }
        self.row_regions
            .as_deref()
            .map(crate::container::region_row_ranges)
    }

    /// Exact per-file top-N candidates for `column = needle`, ordered by
    /// `_timestamp`. Only the predicate column, timestamp, and absolute row
    /// index are evaluated; `_source` is never projected.
    ///
    /// For proven timestamp-descending regions the scan is adaptive:
    /// descending queries stop after `limit` matches, while ascending
    /// queries read backward in exponentially growing row windows and stop
    /// once the oldest `limit` matches are known. A concat file with proven
    /// regions keeps `limit` candidates per region and reduces them through
    /// a bounded heap. Unknown physical order scans the two narrow columns
    /// once and retains only `limit` rows. Thus broad predicates are cheap
    /// without changing their SQL semantics, and memory stays O(limit).
    ///
    /// `Ok(None)` means the native column is absent or not string-family;
    /// callers must keep the ordinary filtered scan for that file.
    pub fn eq_string_top_n(
        &self,
        column: &str,
        needle: &str,
        ts_range: Option<(i64, i64)>,
        limit: usize,
        ascend: bool,
    ) -> anyhow::Result<Option<Vec<(i64, u32)>>> {
        use arrow::datatypes::DataType;

        let Ok(field) = self.schema.field_with_name(column) else {
            return Ok(None);
        };
        if !matches!(
            field.data_type(),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        ) {
            return Ok(None);
        }
        if limit == 0 || self.row_count == 0 {
            return Ok(Some(Vec::new()));
        }

        let mut winners = CandidateHeap::new(limit, ascend);
        match self.ts_desc_row_ranges() {
            Some(regions) => {
                for region in regions {
                    if region.start >= region.end {
                        continue;
                    }
                    let mut local = Vec::with_capacity(limit);
                    if ascend {
                        // Stored rows are newest-first. Start at the tail of
                        // each region; grow the window only when selectivity
                        // is too low to fill the bounded candidate set.
                        const INITIAL_ROWS: u64 = 4 * 1024;
                        const MAX_ROWS: u64 = 256 * 1024;
                        let mut end = region.end;
                        let mut rows = INITIAL_ROWS;
                        while end > region.start && local.len() < limit {
                            let start = end.saturating_sub(rows).max(region.start);
                            let remaining = limit - local.len();
                            let mut tail = std::collections::VecDeque::with_capacity(remaining);
                            crate::container::scan_eq_string_candidates_range(
                                &self.docs_blob,
                                column,
                                needle,
                                start..end,
                                ts_range,
                                &mut |ts, row_id| {
                                    if tail.len() == remaining {
                                        tail.pop_front();
                                    }
                                    tail.push_back((ts, row_id));
                                    true
                                },
                            )?;
                            // Scan order is newest-first; reverse the last
                            // matches so this region remains oldest-first.
                            local.extend(tail.into_iter().rev());
                            end = start;
                            rows = rows.saturating_mul(2).min(MAX_ROWS);
                        }
                    } else {
                        crate::container::scan_eq_string_candidates_range(
                            &self.docs_blob,
                            column,
                            needle,
                            region,
                            ts_range,
                            &mut |ts, row_id| {
                                local.push((ts, row_id));
                                local.len() < limit
                            },
                        )?;
                    }
                    for candidate in local {
                        winners.push(candidate);
                    }
                }
            }
            None => {
                // No order proof: evaluate every match, but never retain an
                // unbounded row-id vector.
                crate::container::scan_eq_string_candidates_range(
                    &self.docs_blob,
                    column,
                    needle,
                    0..self.row_count,
                    ts_range,
                    &mut |ts, row_id| {
                        winners.push((ts, row_id));
                        true
                    },
                )?;
            }
        }
        Ok(Some(winners.into_best_first()))
    }
    /// Exact fixed-width `_timestamp` histogram for `column = needle`.
    /// Only the predicate column and timestamp are decoded; results stream
    /// into `num_buckets` counters, so memory is independent of match count.
    ///
    /// `Ok(None)` means the native column is absent or not string-family;
    /// callers must keep the ordinary filtered scan for that file.
    pub fn eq_string_histogram(
        &self,
        column: &str,
        needle: &str,
        ts_range: Option<(i64, i64)>,
        min_value: i64,
        bucket_width: u64,
        num_buckets: usize,
        ts_offset: i64,
    ) -> anyhow::Result<Option<Vec<u64>>> {
        use arrow::datatypes::DataType;

        let Ok(field) = self.schema.field_with_name(column) else {
            return Ok(None);
        };
        if !matches!(
            field.data_type(),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        ) {
            return Ok(None);
        }
        let mut counts = vec![0u64; num_buckets];
        if num_buckets == 0 || self.row_count == 0 {
            return Ok(Some(counts));
        }
        let width = i64::try_from(bucket_width.max(1))
            .map_err(|_| anyhow::anyhow!("histogram bucket width overflows i64: {bucket_width}"))?;
        let origin = min_value
            .checked_sub(ts_offset)
            .ok_or_else(|| anyhow::anyhow!("histogram bucket origin overflows i64"))?;
        crate::container::scan_eq_string_candidates_range(
            &self.docs_blob,
            column,
            needle,
            0..self.row_count,
            ts_range,
            &mut |timestamp, _row_id| {
                if let Some(offset) = timestamp.checked_sub(origin)
                    && offset >= 0
                {
                    let bucket = (offset / width) as usize;
                    if bucket < counts.len() {
                        counts[bucket] += 1;
                    }
                }
                true
            },
        )?;
        Ok(Some(counts))
    }

    /// The decoded per-column chunk-stats table (`stats` blob), fetched and
    /// parsed once per open handle. `None` = no blob / undecodable
    /// (fail-open: no per-column chunk pruning).
    fn chunk_stats(&self) -> Option<&crate::stats::FileColumnStats> {
        self.decoded_stats
            .get_or_init(|| {
                let blob = self.stats_blob.as_ref()?;
                let bytes = match blob.bytes() {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        log::debug!("vix: stats blob unreadable, no chunk pruning: {e}");
                        return None;
                    }
                };
                match crate::stats::decode_stats_blob(&bytes) {
                    Ok(stats) => Some(stats),
                    Err(e) => {
                        log::debug!("vix: stats blob undecodable, no chunk pruning: {e}");
                        None
                    }
                }
            })
            .as_ref()
    }

    /// #51c-c: exact file-level `_timestamp` bounds `(min, max)` derived
    /// from the zone table at open — populated ONLY for non-sorted (concat)
    /// files, whose first/last rows are NOT their newest/oldest; `None` on
    /// sorted files (read the boundary rows instead, as always) and on a
    /// concat file without a trustworthy zone table (report unknown bounds,
    /// fail-open). Zero data reads either way.
    pub fn zone_ts_bounds(&self) -> Option<(i64, i64)> {
        self.zone_ts_bounds
    }

    /// Row-group size recorded at build time (`0` = unknown) — a logical
    /// grouping constant (e.g. for compact row-id encodings), *not* the
    /// docs-blob chunking: chunks are sized by the writer's
    /// `docs_chunk_bytes` budget.
    pub fn row_group_size(&self) -> usize {
        self.row_group_size
    }

    /// The arrow schema of the `docs` blob: `_timestamp`, the column-store
    /// fields with their stored types, `_source` and (when present)
    /// `_original` / `_o2_id`.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// m25 worktree instrumentation: per-column `(name, leaf_count,
    /// total_leaf_segment_bytes)` from the docs blob's vortex footer — the
    /// storage-side width diagnostic.
    pub fn leaf_report(&self) -> anyhow::Result<Vec<(String, u64, u64)>> {
        use vortex::{
            VortexSessionDefault,
            io::{
                runtime::{BlockingRuntime, single::SingleThreadRuntime},
                session::RuntimeSessionExt,
            },
            layout::LayoutRef,
            session::VortexSession,
        };
        let runtime = SingleThreadRuntime::default();
        let session = VortexSession::default().with_handle(runtime.handle());
        let vxf = crate::container::open_blob(&runtime, &session, &self.docs_blob)
            .map_err(|e| anyhow::anyhow!("open docs blob: {e}"))?;
        let footer = vxf.footer();
        let segmap = footer.segment_map().clone();
        let root = footer.layout().clone();
        let names: Vec<std::sync::Arc<str>> = root.child_names().collect();
        let children = root
            .children()
            .map_err(|e| anyhow::anyhow!("root children: {e}"))?;
        let mut report = Vec::with_capacity(names.len());
        for (name, child) in names.iter().zip(children) {
            let mut leaves = 0u64;
            let mut bytes = 0u64;
            let mut stack: Vec<LayoutRef> = vec![child];
            while let Some(node) = stack.pop() {
                for segment in node.segment_ids() {
                    leaves += 1;
                    bytes += segmap[*segment as usize].length as u64;
                }
                if let Ok(kids) = node.children() {
                    stack.extend(kids);
                }
            }
            report.push((name.to_string(), leaves, bytes));
        }
        Ok(report)
    }

    /// m25 worktree instrumentation: the first `take_n` stored chunks' ARRAY
    /// encoding trees + nbytes of one docs column (what the flat leaves
    /// actually hold).
    pub fn column_chunk_encodings(
        &self,
        column: &str,
        take_n: usize,
    ) -> anyhow::Result<Vec<String>> {
        let mut out = Vec::new();
        let mut seen = 0usize;
        let column = column.to_string();
        let result = self.scan_docs_encoded_chunks(&mut |chunk| {
            use vortex::array::arrays::{Struct, struct_::StructArrayExt};
            let sa = chunk
                .array
                .as_typed::<Struct>()
                .ok_or_else(|| anyhow::anyhow!("not a struct chunk"))?
                .clone();
            let index = sa
                .names()
                .iter()
                .position(|n| n.as_ref() == column)
                .ok_or_else(|| anyhow::anyhow!("column {column:?} missing"))?;
            let field = &sa.unmasked_fields()[index];
            let ids: Vec<String> = field
                .depth_first_traversal()
                .map(|node| format!("{}({}B)", node.encoding_id(), node.nbytes()))
                .collect();
            let invalid = {
                use vortex::{VortexSessionDefault, array::VortexSessionExecute};
                let session = vortex::session::VortexSession::default();
                let mut ctx = session.create_execution_ctx();
                field.invalid_count(&mut ctx).unwrap_or(usize::MAX)
            };
            out.push(format!(
                "chunk {seen}: rows={} nulls={invalid} nbytes={} tree=[{}]",
                field.len(),
                field.nbytes(),
                ids.join(" ")
            ));
            seen += 1;
            if seen >= take_n {
                return Err(anyhow::anyhow!("m25: done"));
            }
            Ok(())
        });
        if let Err(e) = result
            && !format!("{e:#}").contains("m25: done")
        {
            return Err(e);
        }
        Ok(out)
    }

    /// m25 worktree instrumentation: `(offset, length)` of one column's flat
    /// leaf segments within the DOCS BLOB (add the blob's container offset
    /// for file-absolute positions — Mem handles are blob-relative anyway).
    pub fn column_leaf_extents(&self, column: &str) -> anyhow::Result<Vec<(u64, u64)>> {
        use vortex::{
            VortexSessionDefault,
            io::{
                runtime::{BlockingRuntime, single::SingleThreadRuntime},
                session::RuntimeSessionExt,
            },
            layout::LayoutRef,
            session::VortexSession,
        };
        let runtime = SingleThreadRuntime::default();
        let session = VortexSession::default().with_handle(runtime.handle());
        let vxf = crate::container::open_blob(&runtime, &session, &self.docs_blob)
            .map_err(|e| anyhow::anyhow!("open docs blob: {e}"))?;
        let footer = vxf.footer();
        let segmap = footer.segment_map().clone();
        let root = footer.layout().clone();
        let names: Vec<std::sync::Arc<str>> = root.child_names().collect();
        let children = root
            .children()
            .map_err(|e| anyhow::anyhow!("root children: {e}"))?;
        for (name, child) in names.iter().zip(children) {
            if name.as_ref() != column {
                continue;
            }
            let mut extents = Vec::new();
            let mut stack: Vec<LayoutRef> = vec![child];
            while let Some(node) = stack.pop() {
                for segment in node.segment_ids() {
                    let spec = &segmap[*segment as usize];
                    extents.push((spec.offset, spec.length as u64));
                }
                if let Ok(kids) = node.children() {
                    stack.extend(kids);
                }
            }
            extents.sort_unstable();
            return Ok(extents);
        }
        Err(anyhow::anyhow!("column {column:?} not found"))
    }

    /// m25 worktree instrumentation: the vortex layout tree of one docs
    /// column (encodings + segment sizes), for the storage-bloat diagnosis.
    pub fn column_layout_tree(&self, column: &str) -> anyhow::Result<String> {
        use vortex::{
            VortexSessionDefault,
            io::{
                runtime::{BlockingRuntime, single::SingleThreadRuntime},
                session::RuntimeSessionExt,
            },
            session::VortexSession,
        };
        let runtime = SingleThreadRuntime::default();
        let session = VortexSession::default().with_handle(runtime.handle());
        let vxf = crate::container::open_blob(&runtime, &session, &self.docs_blob)
            .map_err(|e| anyhow::anyhow!("open docs blob: {e}"))?;
        let root = vxf.footer().layout().clone();
        let names: Vec<std::sync::Arc<str>> = root.child_names().collect();
        let children = root
            .children()
            .map_err(|e| anyhow::anyhow!("root children: {e}"))?;
        for (name, child) in names.iter().zip(children) {
            if name.as_ref() == column {
                return Ok(format!("{}", child.display_tree()));
            }
        }
        Err(anyhow::anyhow!("column {column:?} not found"))
    }

    /// Stream the selected rows of the `docs` blob.
    ///
    /// - `projection`: physical column names to read (must exist in [`Self::schema`]); `None` reads
    ///   every column.
    /// - `rows`: row indices to point-read (sorted + deduped internally); `None` scans all rows.
    /// - `ts_range`: `[min, max)` filter on `_timestamp`, pushed down into the Vortex scan
    ///   (zone-map pruned) — the same bounds the query engine applies, so applying it here is a
    ///   pure early-out.
    /// - `on_batch` receives each decoded chunk in row order; returning an error aborts the scan.
    pub fn scan_docs(
        &self,
        projection: Option<&[String]>,
        rows: Option<Vec<u64>>,
        ts_range: Option<(i64, i64)>,
        on_batch: &mut dyn FnMut(RecordBatch) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        self.scan_docs_opts(projection, rows, ts_range, &[], None, 0, on_batch)
    }

    /// [`Self::scan_docs`] with the full pushdown surface:
    /// - `bounds`: numeric column ranges pushed into the vortex scan as filter conjuncts — vortex
    ///   prunes chunks by its per-chunk stats BEFORE decode (and, on ranged sources, before FETCH).
    ///   Bounds on columns absent from the docs schema are ignored (conservative: the query layer
    ///   re-applies its predicates).
    /// - `limit`: stop the scan after this many produced rows (LIMIT shapes; rows are stored
    ///   `_timestamp` DESC, so a DESC top-N stops at the first `limit` matching rows).
    /// - `decode_threads`: >1 decodes one file's chunks in parallel.
    #[allow(clippy::too_many_arguments)]
    pub fn scan_docs_opts(
        &self,
        projection: Option<&[String]>,
        rows: Option<Vec<u64>>,
        ts_range: Option<(i64, i64)>,
        bounds: &[ColumnBound],
        limit: Option<u64>,
        decode_threads: usize,
        on_batch: &mut dyn FnMut(RecordBatch) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let names: Option<Vec<&str>> =
            projection.map(|cols| cols.iter().map(String::as_str).collect());
        if let Some(names) = names.as_deref() {
            for name in names {
                if self.schema.field_with_name(name).is_err() {
                    return Err(VixError::ColumnNotFound((*name).to_string()).into());
                }
            }
        }
        let filter = self.build_scan_filter(ts_range, bounds);

        // M15: dictionary-aware string-equality pre-pass. A pushed
        // `col = 'needle'` on a string column (min == max == Str, both
        // inclusive — the #52 demoted-needle filter-back shape) resolves the
        // needle per chunk against the column's DICTIONARY and scans code
        // ids instead of materializing and comparing one string per row;
        // the surviving row ids then point-read the projection (only
        // matching rows decode their other columns). Broad matches stop the
        // shared pre-pass at an aggregate ~2% row-id budget and fall through
        // to the plain streaming scan, rather than finishing and retaining a
        // redundant full-column pass. Exact by construction (byte equality on the
        // stored values), and the engine re-applies the predicate on
        // returned rows regardless.
        if rows.is_none()
            && let Some(eq) = bounds.iter().find_map(|bound| self.string_eq_bound(bound))
        {
            let (column, needle) = eq;
            match self.eq_string_prepass(column, needle, ts_range, bounds, decode_threads)? {
                Some(matched) if matched.is_empty() => return Ok(()),
                Some(matched) => {
                    scan_blob_streaming(
                        &self.docs_blob,
                        names.as_deref(),
                        RowSelection::Indices(matched),
                        filter,
                        limit,
                        0,
                        &mut |batch| on_batch(batch).map_err(VixError::Callback),
                    )?;
                    return Ok(());
                }
                None => {} // no basis / broad match: the normal scan below
            }
        }

        // O2-owned chunk pruning (§4, M4): the zone table + the spliceable
        // per-column stats restrict the scan to the surviving row ranges —
        // this is what prunes PASSTHROUGH outputs, whose docs blobs carry no
        // vortex statistics. Point reads (index row selections) skip it:
        // vortex only touches the selected rows' chunks anyway.
        let pruned = if rows.is_none() {
            self.pruned_scan_ranges(ts_range, bounds)
        } else {
            None
        };
        match rows {
            Some(indices) => {
                scan_blob_streaming(
                    &self.docs_blob,
                    names.as_deref(),
                    RowSelection::Indices(indices),
                    filter,
                    limit,
                    decode_threads,
                    &mut |batch| on_batch(batch).map_err(VixError::Callback),
                )?;
            }
            None => match pruned {
                // provably nothing survives: zero data reads
                Some(ranges) if ranges.is_empty() => {}
                // a strict subset survives: one ranged scan per contiguous
                // surviving run, in row order (ranges are ascending), with
                // the remaining limit threaded through
                Some(ranges) => {
                    let mut remaining = limit;
                    for range in ranges {
                        if remaining == Some(0) {
                            break;
                        }
                        let mut produced = 0u64;
                        scan_blob_streaming(
                            &self.docs_blob,
                            names.as_deref(),
                            RowSelection::Range(range),
                            filter.clone(),
                            remaining,
                            decode_threads,
                            &mut |batch| {
                                produced += batch.num_rows() as u64;
                                on_batch(batch).map_err(VixError::Callback)
                            },
                        )?;
                        if let Some(left) = remaining.as_mut() {
                            *left = left.saturating_sub(produced);
                        }
                    }
                }
                // no pruning basis (or nothing pruned): the plain full scan
                None => {
                    scan_blob_streaming(
                        &self.docs_blob,
                        names.as_deref(),
                        RowSelection::All,
                        filter,
                        limit,
                        decode_threads,
                        &mut |batch| on_batch(batch).map_err(VixError::Callback),
                    )?;
                }
            },
        }
        Ok(())
    }

    /// M23b: stream ONE contiguous half-open row range of the `docs` blob —
    /// the k-way merge scan's bounded decode unit. Unlike [`Self::scan_docs`]
    /// with a full selection (whose per-callback batches are the STORED
    /// chunks — for a small L0 file effectively the whole file decoded at
    /// once), a range selection materializes only the selected rows, so a
    /// caller pulling `caps`-sized ranges holds O(range) decoded per input
    /// instead of O(file). A range crossing stored-chunk boundaries may
    /// invoke `on_batch` more than once (row order, ranges compose exactly).
    /// No filter/limit/pruning: the merge scans every stored row by
    /// position.
    pub fn scan_docs_row_range(
        &self,
        projection: Option<&[String]>,
        range: std::ops::Range<u64>,
        on_batch: &mut dyn FnMut(RecordBatch) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let names: Option<Vec<&str>> =
            projection.map(|cols| cols.iter().map(String::as_str).collect());
        if let Some(names) = names.as_deref() {
            for name in names {
                if self.schema.field_with_name(name).is_err() {
                    return Err(VixError::ColumnNotFound((*name).to_string()).into());
                }
            }
        }
        scan_blob_streaming(
            &self.docs_blob,
            names.as_deref(),
            RowSelection::Range(range),
            None,
            None,
            0,
            &mut |batch| on_batch(batch).map_err(VixError::Callback),
        )?;
        Ok(())
    }

    /// M15: `Some((column, needle))` when `bound` is a string EQUALITY on a
    /// string-family stored column — the shape the dictionary-aware
    /// filter-back pre-pass serves. Both bounds inclusive with the same
    /// literal (`=` and single-value `IN`).
    fn string_eq_bound<'a>(&self, bound: &'a ColumnBound) -> Option<(&'a str, &'a str)> {
        let (Some((BoundValue::Str(min), true)), Some((BoundValue::Str(max), true))) =
            (&bound.min, &bound.max)
        else {
            return None;
        };
        if min != max {
            return None;
        }
        let field = self.schema.field_with_name(&bound.column).ok()?;
        matches!(
            field.data_type(),
            arrow::datatypes::DataType::Utf8
                | arrow::datatypes::DataType::LargeUtf8
                | arrow::datatypes::DataType::Utf8View
        )
        .then_some((bound.column.as_str(), min.as_str()))
    }

    /// M15: the dictionary-aware equality pre-pass — matching row ids of
    /// `column == needle` over the scan's zone-pruned surviving ranges,
    /// chunk-parallel across `threads` (the `ZO_VIX_SCAN_DECODE_THREADS`
    /// machinery; each worker owns a contiguous chunk-aligned range group,
    /// so the concatenated ids stay ascending).
    ///
    /// - `Ok(Some(vec![]))` — provably nothing matches (also when pruning excluded every chunk).
    /// - `Ok(Some(ids))` — the exact matching rows; the caller point-reads them.
    /// - `Ok(None)` — broad match (beyond ~2% of rows): the plain streaming scan wins from here,
    ///   run it unchanged.
    pub(crate) fn eq_string_prepass(
        &self,
        column: &str,
        needle: &str,
        ts_range: Option<(i64, i64)>,
        bounds: &[ColumnBound],
        threads: usize,
    ) -> anyhow::Result<Option<Vec<u64>>> {
        if self.row_count == 0 {
            return Ok(Some(Vec::new()));
        }
        let ranges = match self.pruned_scan_ranges(ts_range, bounds) {
            Some(ranges) if ranges.is_empty() => return Ok(Some(Vec::new())),
            Some(ranges) => ranges,
            None => vec![0..self.row_count],
        };
        let groups = split_ranges_for_threads(threads, &ranges, self.zone_chunks());
        let budget = crate::container::EqMatchBudget::new(self.row_count / 50);
        let matched: Vec<u64> = if groups.len() <= 1 {
            match crate::container::eq_string_rows_ranges(
                &self.docs_blob,
                column,
                needle,
                &ranges,
                &budget,
            )? {
                Some(rows) => rows,
                None => return Ok(None),
            }
        } else {
            // Contiguous chunk-aligned groups per worker: concatenation in
            // group order preserves ascending row ids. The shared budget
            // bounds retained row ids across every worker.
            let results = std::thread::scope(|scope| {
                let workers: Vec<_> = groups
                    .iter()
                    .map(|group| {
                        let blob = &self.docs_blob;
                        let budget = &budget;
                        scope.spawn(move || {
                            crate::container::eq_string_rows_ranges(
                                blob, column, needle, group, budget,
                            )
                        })
                    })
                    .collect();
                workers
                    .into_iter()
                    .map(|worker| {
                        worker.join().unwrap_or_else(|_| {
                            Err(VixError::Malformed("eq-scan worker panicked".to_string()))
                        })
                    })
                    .collect::<Vec<_>>()
            });
            let mut out = Vec::new();
            for result in results {
                match result? {
                    Some(rows) => out.extend(rows),
                    None => return Ok(None),
                }
            }
            out
        };
        debug_assert!(!budget.is_exceeded());
        Ok(Some(matched))
    }

    /// The vortex row-filter expression of a scan: the half-open
    /// `_timestamp` range AND every type-family-matching numeric bound
    /// (string bounds are stats-only, never row filters).
    fn build_scan_filter(
        &self,
        ts_range: Option<(i64, i64)>,
        bounds: &[ColumnBound],
    ) -> Option<vortex::expr::Expression> {
        let mut filter = ts_range.map(|(min_micros, max_micros)| {
            let ts = || get_item(TIMESTAMP_COL_NAME, root());
            and(gt_eq(ts(), lit(min_micros)), lt(ts(), lit(max_micros)))
        });
        for bound in bounds {
            // absent column: ignore the bound as a row filter (the caller's
            // engine re-applies every predicate on returned rows)
            let Ok(field) = self.schema.field_with_name(&bound.column) else {
                continue;
            };
            let column = || get_item(bound.column.clone(), root());
            let mut add = |expr: vortex::expr::Expression| {
                filter = Some(match filter.take() {
                    Some(prev) => and(prev, expr),
                    None => expr,
                });
            };
            if let Some((value, inclusive)) = &bound.min
                && let Some(value) = value.to_row_filter_lit(field.data_type())
            {
                add(if *inclusive {
                    gt_eq(column(), value)
                } else {
                    vortex::expr::gt(column(), value)
                });
            }
            if let Some((value, inclusive)) = &bound.max
                && let Some(value) = value.to_row_filter_lit(field.data_type())
            {
                add(if *inclusive {
                    vortex::expr::lt_eq(column(), value)
                } else {
                    lt(column(), value)
                });
            }
        }
        filter
    }

    /// §4 chunk pruning: the surviving contiguous row ranges of a scan
    /// under `ts_range` (half-open) plus the null-rejecting `bounds`,
    /// decided per zone chunk from the zone table and the O2 stats blob.
    ///
    /// - `None`: no pruning basis (no zone table) or nothing pruned — scan everything as before.
    /// - `Some(vec![])`: every chunk is provably excluded — the whole FILE is skippable with zero
    ///   data reads.
    /// - `Some(ranges)`: scan only these (ascending, coalesced) row ranges.
    ///
    /// Per chunk, exclusion is proven by EITHER the zone `_timestamp` span
    /// missing `ts_range`, OR some bound's column having a stats row that
    /// excludes it: zero present values (a null-rejecting predicate cannot
    /// hold), or a min/max window provably outside the bound. String stats
    /// are conservative prefix bounds (stored min <= true min, stored max
    /// >= true max), so the same interval logic stays sound. Unknown rows,
    /// missing tables, tag/alignment mismatches and cross-family compares
    /// all KEEP the chunk (fail-open).
    pub fn pruned_scan_ranges(
        &self,
        ts_range: Option<(i64, i64)>,
        bounds: &[ColumnBound],
    ) -> Option<Vec<std::ops::Range<u64>>> {
        if ts_range.is_none() && bounds.is_empty() {
            return None;
        }
        let zone = self.zone_chunks()?;
        // per-bound stats tables, validated to align 1:1 with the zone table
        let stats = self.chunk_stats();
        let tables: Vec<Option<&crate::stats::ColumnChunkStats>> = bounds
            .iter()
            .map(|bound| {
                stats
                    .and_then(|s| s.columns.get(&bound.column))
                    .filter(|table| table.chunks.len() == zone.len())
            })
            .collect();
        if ts_range.is_none() && tables.iter().all(Option::is_none) {
            return None; // no basis beyond the zone table itself
        }

        let mut ranges: Vec<std::ops::Range<u64>> = Vec::new();
        let mut pruned_any = false;
        for (index, chunk) in zone.iter().enumerate() {
            let mut survives = match ts_range {
                Some((min, max)) => chunk.ts_max >= min && chunk.ts_min < max,
                None => true,
            };
            if survives {
                for (bound, table) in bounds.iter().zip(&tables) {
                    let row = table.and_then(|t| t.chunks[index].as_ref());
                    if !chunk_survives_bound(row, bound) {
                        survives = false;
                        break;
                    }
                }
            }
            if !survives {
                pruned_any = true;
                continue;
            }
            let start = chunk.row_offset;
            let end = chunk.row_offset + chunk.row_count;
            match ranges.last_mut() {
                Some(last) if last.end == start => last.end = end,
                _ => ranges.push(start..end),
            }
        }
        if !pruned_any {
            return None; // everything survives: keep the single full scan
        }
        Some(ranges)
    }

    /// §6.2 piecewise-ordered read (M4): stream the selected rows in GLOBAL
    /// `_timestamp` DESC order by k-way merging the file's proven
    /// internally-DESC regions (`row_regions` / whole-file for ts_desc) —
    /// a concat file serves `ORDER BY _timestamp DESC [LIMIT n]` without a
    /// full sort, with early exit at `limit`.
    ///
    /// Mechanics: one lazily-opened cursor per surviving region (chunk
    /// pruning from [`Self::pruned_scan_ranges`] clips each region's scan
    /// ranges first; an index row `selection` splits by region instead), a
    /// max-heap keyed by each region's current row `_timestamp` — regions
    /// not yet opened sit in the heap at their ZONE-derived upper bound and
    /// only open (decode) when that bound reaches the top, so time-disjoint
    /// regions never decode past the limit. Runs are emitted as zero-copy
    /// batch slices. `_timestamp` is added to the scan projection when the
    /// caller did not request it and stripped from emitted batches.
    ///
    /// `on_region_open` fires before each region's decode stream spawns
    /// (memory-accounting hook); its error aborts the scan. Errors when the
    /// file has no proven piecewise decomposition — callers route such
    /// files to a real sort instead.
    #[allow(clippy::too_many_arguments)]
    pub fn scan_docs_ts_desc_merged(
        &self,
        projection: Option<&[String]>,
        rows: Option<Vec<u64>>,
        ts_range: Option<(i64, i64)>,
        bounds: &[ColumnBound],
        limit: Option<u64>,
        on_region_open: &mut dyn FnMut() -> anyhow::Result<()>,
        on_batch: &mut dyn FnMut(RecordBatch) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        use std::cmp::Reverse;

        let regions = self.ts_desc_row_ranges().ok_or_else(|| {
            anyhow::anyhow!(
                "ordered read of a concat file without a proven region decomposition \
                 (row_regions) — the caller must sort instead"
            )
        })?;
        if self.row_count == 0 || limit == Some(0) {
            return Ok(());
        }
        // the scan must produce `_timestamp` for the merge; strip it from
        // emitted batches when the caller did not ask for it
        let (scan_projection, strip_ts): (Option<Vec<String>>, bool) = match projection {
            None => (None, false),
            Some(cols) => {
                if cols.iter().any(|c| c == TIMESTAMP_COL_NAME) {
                    (Some(cols.to_vec()), false)
                } else {
                    let mut with_ts = cols.to_vec();
                    with_ts.push(TIMESTAMP_COL_NAME.to_string());
                    (Some(with_ts), true)
                }
            }
        };
        if let Some(names) = scan_projection.as_deref() {
            for name in names {
                if self.schema.field_with_name(name).is_err() {
                    return Err(VixError::ColumnNotFound(name.clone()).into());
                }
            }
        }
        // the emitted-batch `_timestamp` position: projection order, or the
        // docs schema position for a full-column scan
        let ts_index = match scan_projection.as_deref() {
            Some(names) => names
                .iter()
                .position(|n| n == TIMESTAMP_COL_NAME)
                .expect("appended above"),
            None => self
                .schema
                .index_of(TIMESTAMP_COL_NAME)
                .map_err(|_| anyhow::anyhow!("docs blob lacks {TIMESTAMP_COL_NAME}"))?,
        };

        // per-region work: chunk-pruned scan ranges, or the selection split
        enum RegionWork {
            Ranges(Vec<std::ops::Range<u64>>),
            Indices(Vec<u64>),
        }
        let pruned = if rows.is_none() {
            self.pruned_scan_ranges(ts_range, bounds)
        } else {
            None
        };
        let selection = rows.map(|mut indices| {
            indices.sort_unstable();
            indices.dedup();
            indices
        });
        let zone = self.zone_chunks();
        let mut work: Vec<(RegionWork, i64)> = Vec::new(); // (work, ts upper bound)
        for region in &regions {
            let item = match &selection {
                Some(indices) => {
                    let start = indices.partition_point(|&row| row < region.start);
                    let end = indices.partition_point(|&row| row < region.end);
                    if start == end {
                        continue;
                    }
                    RegionWork::Indices(indices[start..end].to_vec())
                }
                None => {
                    let ranges: Vec<std::ops::Range<u64>> = match &pruned {
                        None => vec![region.clone()],
                        Some(survivors) => survivors
                            .iter()
                            .filter_map(|r| {
                                let start = r.start.max(region.start);
                                let end = r.end.min(region.end);
                                (start < end).then_some(start..end)
                            })
                            .collect(),
                    };
                    if ranges.is_empty() {
                        continue;
                    }
                    RegionWork::Ranges(ranges)
                }
            };
            // lazy-open upper bound: the max ts over the zone chunks this
            // region's rows touch; unknown zone = i64::MAX (opens eagerly)
            let bound = match zone {
                Some(chunks) => {
                    let mut bound = i64::MIN;
                    for chunk in chunks {
                        let c_start = chunk.row_offset;
                        let c_end = chunk.row_offset + chunk.row_count;
                        if c_end > region.start && c_start < region.end {
                            bound = bound.max(chunk.ts_max);
                        }
                    }
                    if bound == i64::MIN { i64::MAX } else { bound }
                }
                None => i64::MAX,
            };
            work.push((item, bound));
        }
        if work.is_empty() {
            return Ok(());
        }

        let filter = self.build_scan_filter(ts_range, bounds);
        let docs_blob = &self.docs_blob;

        // strip the internal `_timestamp` from an emitted slice
        let output_schema: std::cell::OnceCell<SchemaRef> = std::cell::OnceCell::new();
        let mut emit = |batch: &RecordBatch,
                        offset: usize,
                        len: usize,
                        on_batch: &mut dyn FnMut(RecordBatch) -> anyhow::Result<()>|
         -> anyhow::Result<()> {
            let slice = batch.slice(offset, len);
            if !strip_ts {
                return on_batch(slice);
            }
            let schema = output_schema.get_or_init(|| {
                let fields: Vec<arrow::datatypes::FieldRef> = slice
                    .schema()
                    .fields()
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| *i != ts_index)
                    .map(|(_, f)| f.clone())
                    .collect();
                Arc::new(arrow::datatypes::Schema::new(fields))
            });
            let columns: Vec<arrow::array::ArrayRef> = slice
                .columns()
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != ts_index)
                .map(|(_, c)| c.clone())
                .collect();
            let out = RecordBatch::try_new_with_options(
                Arc::clone(schema),
                columns,
                &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(len)),
            )?;
            on_batch(out)
        };

        // one cursor per region: a lazily spawned scan thread streaming
        // batches over a 1-slot channel (≤2 decoded batches resident per
        // OPEN region), plus the in-batch position
        struct Cursor {
            work: Option<RegionWork>, // taken by open()
            rx: Option<std::sync::mpsc::Receiver<anyhow::Result<RecordBatch>>>,
            batch: Option<RecordBatch>,
            pos: usize,
            ts: i64,
            opened: bool,
        }
        impl Cursor {
            /// The current batch's `_timestamp` values (non-null by contract).
            fn ts_values<'b>(batch: &'b RecordBatch, ts_index: usize) -> anyhow::Result<&'b [i64]> {
                let column = batch.column(ts_index);
                if column.null_count() > 0 {
                    return Err(anyhow::anyhow!("{TIMESTAMP_COL_NAME} carries nulls"));
                }
                let column = column
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .ok_or_else(|| anyhow::anyhow!("{TIMESTAMP_COL_NAME} is not i64"))?;
                Ok(column.values())
            }

            /// Pull the next non-empty batch; `Ok(false)` = region exhausted.
            fn fetch(&mut self, ts_index: usize) -> anyhow::Result<bool> {
                let Some(rx) = &self.rx else { return Ok(false) };
                loop {
                    match rx.recv() {
                        Ok(Ok(batch)) => {
                            if batch.num_rows() == 0 {
                                continue;
                            }
                            self.ts = Self::ts_values(&batch, ts_index)?[0];
                            self.batch = Some(batch);
                            self.pos = 0;
                            return Ok(true);
                        }
                        Ok(Err(e)) => return Err(e),
                        Err(_) => {
                            self.rx = None;
                            self.batch = None;
                            return Ok(false);
                        }
                    }
                }
            }
        }

        let mut cursors: Vec<Cursor> = work
            .iter()
            .map(|_| Cursor {
                work: None,
                rx: None,
                batch: None,
                pos: 0,
                ts: i64::MIN,
                opened: false,
            })
            .collect();
        for (cursor, (item, _)) in cursors.iter_mut().zip(work.iter_mut()) {
            cursor.work = Some(std::mem::replace(item, RegionWork::Indices(Vec::new())));
        }

        let mut remaining = limit;
        std::thread::scope(|scope| -> anyhow::Result<()> {
            // max-heap on (ts key, smaller region index wins ties)
            let mut heap: std::collections::BinaryHeap<(i64, Reverse<usize>)> =
                std::collections::BinaryHeap::new();
            for (index, (_, bound)) in work.iter().enumerate() {
                heap.push((*bound, Reverse(index)));
            }
            while let Some((_, Reverse(index))) = heap.pop() {
                let cursor = &mut cursors[index];
                if !cursor.opened {
                    // open at its bound: spawn the region's scan stream
                    on_region_open()?;
                    cursor.opened = true;
                    let (tx, rx) = std::sync::mpsc::sync_channel::<anyhow::Result<RecordBatch>>(1);
                    let region_work = cursor.work.take().expect("unopened cursor has work");
                    let scan_projection = scan_projection.clone();
                    let filter = filter.clone();
                    scope.spawn(move || {
                        let names: Option<Vec<&str>> = scan_projection
                            .as_ref()
                            .map(|cols| cols.iter().map(String::as_str).collect());
                        let run =
                            |selection: RowSelection,
                             tx: &std::sync::mpsc::SyncSender<anyhow::Result<RecordBatch>>|
                             -> anyhow::Result<()> {
                                scan_blob_streaming(
                                    docs_blob,
                                    names.as_deref(),
                                    selection,
                                    filter.clone(),
                                    None,
                                    0,
                                    &mut |batch| {
                                        tx.send(Ok(batch)).map_err(|_| {
                                            VixError::Callback(anyhow::anyhow!(
                                                "merge consumer dropped"
                                            ))
                                        })
                                    },
                                )?;
                                Ok(())
                            };
                        let result = match region_work {
                            RegionWork::Ranges(ranges) => ranges
                                .into_iter()
                                .try_for_each(|range| run(RowSelection::Range(range), &tx)),
                            RegionWork::Indices(indices) => {
                                run(RowSelection::Indices(indices), &tx)
                            }
                        };
                        if let Err(e) = result {
                            // consumer may be gone (limit/cancel): ignore
                            let _ = tx.send(Err(e));
                        }
                    });
                    cursor.rx = Some(rx);
                    if cursor.fetch(ts_index)? {
                        heap.push((cursor.ts, Reverse(index)));
                    }
                    continue;
                }
                // opened cursor at its ACTUAL current ts: emit its run down
                // to the next-best key (rows are non-increasing per region)
                let next_key = heap.peek().map(|(key, _)| *key).unwrap_or(i64::MIN);
                loop {
                    let batch = cursor.batch.as_ref().expect("open cursor holds a batch");
                    let values = Cursor::ts_values(batch, ts_index)?;
                    // rows [pos..end) all have ts >= next_key
                    let end =
                        cursor.pos + values[cursor.pos..].partition_point(|&ts| ts >= next_key);
                    let mut take = end - cursor.pos;
                    if let Some(left) = remaining {
                        take = take.min(left as usize);
                    }
                    if take > 0 {
                        emit(batch, cursor.pos, take, on_batch)?;
                        cursor.pos += take;
                        if let Some(left) = remaining.as_mut() {
                            *left -= take as u64;
                            if *left == 0 {
                                return Ok(());
                            }
                        }
                    }
                    if cursor.pos < batch.num_rows() {
                        // stopped at a row below next_key: yield the turn
                        cursor.ts = values[cursor.pos];
                        heap.push((cursor.ts, Reverse(index)));
                        break;
                    }
                    // batch drained: pull the next one and keep emitting
                    // while it still tops next_key
                    if !cursor.fetch(ts_index)? {
                        break; // region exhausted
                    }
                    if cursor.ts < next_key {
                        heap.push((cursor.ts, Reverse(index)));
                        break;
                    }
                }
            }
            Ok(())
        })
    }

    /// #51c: stream the docs blob's chunks in their STORED (encoded) form —
    /// no projection, no filter, no row selection, so nothing decompresses.
    /// Chunks arrive in row order and cover every row exactly once; sliced
    /// columns (a scan window cutting inside one of THAT column's stored
    /// leaves) are canonicalized internally so every yielded chunk survives
    /// a serialize round-trip byte-exactly (M18 deterministic slice guard —
    /// see `container::scan_blob_encoded_chunks`). Returns how many
    /// column-windows the guard canonicalized (the merge summary's
    /// sliced-canonicalized count). Returning an error from `on_chunk`
    /// aborts the scan.
    pub fn scan_docs_encoded_chunks(
        &self,
        on_chunk: &mut dyn FnMut(EncodedDocsChunk) -> anyhow::Result<()>,
    ) -> anyhow::Result<u64> {
        let canonicalized = scan_blob_encoded_chunks(&self.docs_blob, &mut |array, rows| {
            on_chunk(EncodedDocsChunk { array, rows }).map_err(VixError::Callback)
        })?;
        Ok(canonicalized)
    }

    /// M17 item 2: hash one PRESENT string-family column's values into the
    /// hasher's per-field set off the column's ENCODED chunks — dict chunks
    /// decode only their dictionary (each referenced distinct value hashed
    /// once), FSST chunks hash raw slices off one bulk decompress, anything
    /// else takes the canonical per-row path chunk-locally. Bit-identical
    /// coverage to a decoded scan by construction (one shared value policy —
    /// [`crate::BloomOnlyHasher::raw_sink`]); returns the per-encoding-class
    /// chunk census. A field the hasher does not track (or a non-string
    /// column) scans nothing.
    pub fn hash_bloom_only_encoded(
        &self,
        hasher: &mut crate::BloomOnlyHasher,
        field: &str,
    ) -> anyhow::Result<crate::BloomEncodingCensus> {
        let Some(mut sink) = hasher.raw_sink(field) else {
            return Ok(crate::BloomEncodingCensus::default());
        };
        Ok(crate::container::hash_blob_column_bloom_encoded(
            &self.docs_blob,
            field,
            &mut |value| sink.observe(value),
        )?)
    }

    /// File-level `(min, max)` of a numeric docs column from the VORTEX
    /// footer statistics — zero data reads, first-encode files only
    /// (passthrough outputs carry none; their pruning source is the O2
    /// stats blob through [`Self::pruned_scan_ranges`]). `None` = cannot
    /// prune (no stats, non-numeric, or unknown column). A predicate that
    /// cannot match within these bounds may skip the WHOLE file.
    pub fn column_stats(&self, column: &str) -> anyhow::Result<Option<(NumScalar, NumScalar)>> {
        Ok(crate::container::blob_column_stats(
            &self.docs_blob,
            column,
        )?)
    }

    /// Collect a scan into memory (convenience for small reads and tests).
    pub fn read_docs(
        &self,
        projection: Option<&[String]>,
        rows: Option<Vec<u64>>,
        ts_range: Option<(i64, i64)>,
    ) -> anyhow::Result<Vec<RecordBatch>> {
        let mut batches = Vec::new();
        self.scan_docs(projection, rows, ts_range, &mut |batch| {
            batches.push(batch);
            Ok(())
        })?;
        Ok(batches)
    }
}

/// Whether one chunk can hold a row satisfying `bound`, judged from its
/// stats row. `row = None` (unknown) keeps the chunk. A known row with ZERO
/// present values excludes it (the bound is null-rejecting by contract).
/// Min/max windows exclude only on a PROVEN miss; any cross-family or
/// missing comparison keeps the chunk (fail-open).
/// M15: split ascending disjoint row `ranges` into at most `threads`
/// CONTIGUOUS groups of roughly equal row counts, cutting on zone-chunk
/// boundaries where a zone table exists (a mid-chunk cut makes both workers
/// decode the shared chunk) and at fixed 128Ki-row steps otherwise.
/// `threads <= 1` returns one group (the caller's single-threaded path).
fn split_ranges_for_threads(
    threads: usize,
    ranges: &[std::ops::Range<u64>],
    zone: Option<&[crate::reader::ZoneChunk]>,
) -> Vec<Vec<std::ops::Range<u64>>> {
    if threads <= 1 || ranges.is_empty() {
        return vec![ranges.to_vec()];
    }
    let mut atoms: Vec<std::ops::Range<u64>> = Vec::new();
    match zone {
        Some(chunks) if !chunks.is_empty() => {
            for range in ranges {
                let mut start = range.start;
                for boundary in chunks
                    .iter()
                    .map(|c| c.row_offset)
                    .filter(|&b| b > range.start && b < range.end)
                {
                    atoms.push(start..boundary);
                    start = boundary;
                }
                atoms.push(start..range.end);
            }
        }
        _ => {
            const STEP: u64 = 131_072;
            for range in ranges {
                let mut start = range.start;
                while range.end - start > STEP {
                    atoms.push(start..start + STEP);
                    start += STEP;
                }
                atoms.push(start..range.end);
            }
        }
    }
    let total: u64 = atoms.iter().map(|r| r.end - r.start).sum();
    let per_group = (total.div_ceil(threads as u64)).max(1);
    let mut groups: Vec<Vec<std::ops::Range<u64>>> = Vec::new();
    let mut current: Vec<std::ops::Range<u64>> = Vec::new();
    let mut current_rows = 0u64;
    for atom in atoms {
        let rows = atom.end - atom.start;
        match current.last_mut() {
            Some(last) if last.end == atom.start => last.end = atom.end,
            _ => current.push(atom),
        }
        current_rows += rows;
        if current_rows >= per_group && groups.len() + 1 < threads {
            groups.push(std::mem::take(&mut current));
            current_rows = 0;
        }
    }
    if !current.is_empty() {
        groups.push(current);
    }
    groups
}

fn chunk_survives_bound(row: Option<&crate::stats::ColumnChunkStat>, bound: &ColumnBound) -> bool {
    use std::cmp::Ordering;
    let Some(stat) = row else {
        return true; // unknown: cannot prune
    };
    if stat.present == 0 {
        return false; // every cell NULL: a null-rejecting predicate cannot hold
    }
    if let Some((value, inclusive)) = &bound.min {
        // need a row with column >(=) value; impossible if chunk_max <(=) value
        if let Some(max) = &stat.max {
            match cmp_stat_vs_bound(max, value) {
                Some(Ordering::Less) => return false,
                Some(Ordering::Equal) if !*inclusive => return false,
                _ => {}
            }
        }
    }
    if let Some((value, inclusive)) = &bound.max {
        if let Some(min) = &stat.min {
            match cmp_stat_vs_bound(min, value) {
                Some(Ordering::Greater) => return false,
                Some(Ordering::Equal) if !*inclusive => return false,
                _ => {}
            }
        }
    }
    true
}

/// EXACT ordering of a vortex-footer file-level stat vs a bound value —
/// the file-skip twin of the chunk comparator, shared with the query layer.
/// `None` = not comparable (keep the file).
pub fn cmp_num_vs_bound(stat: NumScalar, bound: &BoundValue) -> Option<std::cmp::Ordering> {
    let stat = match stat {
        NumScalar::I64(v) => crate::stats::StatValue::I64(v),
        NumScalar::F64(v) => crate::stats::StatValue::F64(v),
    };
    cmp_stat_vs_bound(&stat, bound)
}

/// EXACT ordering of a stored stat value vs a bound value; `None` = not
/// comparable (cross str/numeric families, bool stats, NaN) — the caller
/// keeps the chunk. Integer/float comparisons are exact (no lossy i64→f64
/// rounding: a rounded compare could prune a chunk that actually matches).
fn cmp_stat_vs_bound(
    stat: &crate::stats::StatValue,
    bound: &BoundValue,
) -> Option<std::cmp::Ordering> {
    use crate::stats::StatValue;
    match (stat, bound) {
        (StatValue::I64(a), BoundValue::I64(b)) => Some(a.cmp(b)),
        (StatValue::U64(a), BoundValue::U64(b)) => Some(a.cmp(b)),
        (StatValue::I64(a), BoundValue::U64(b)) => Some((*a as i128).cmp(&(*b as i128))),
        (StatValue::U64(a), BoundValue::I64(b)) => Some((*a as i128).cmp(&(*b as i128))),
        (StatValue::F64(a), BoundValue::F64(b)) => a.partial_cmp(b),
        (StatValue::I64(a), BoundValue::F64(b)) => cmp_i128_vs_f64(*a as i128, *b),
        (StatValue::U64(a), BoundValue::F64(b)) => cmp_i128_vs_f64(*a as i128, *b),
        (StatValue::F64(a), BoundValue::I64(b)) => {
            cmp_i128_vs_f64(*b as i128, *a).map(std::cmp::Ordering::reverse)
        }
        (StatValue::F64(a), BoundValue::U64(b)) => {
            cmp_i128_vs_f64(*b as i128, *a).map(std::cmp::Ordering::reverse)
        }
        (StatValue::Str(a), BoundValue::Str(b)) => Some(a.as_str().cmp(b.as_str())),
        _ => None,
    }
}

/// Exact ordering of an i128 vs an f64 (no rounding through a lossy cast).
/// `None` for NaN. Pub since M16: the search-side min/max fold compares
/// cross-family per-file extremes with the same exactness rules.
pub fn cmp_i128_vs_f64(a: i128, b: f64) -> Option<std::cmp::Ordering> {
    use std::cmp::Ordering;
    if b.is_nan() {
        return None;
    }
    if b == f64::INFINITY {
        return Some(Ordering::Less);
    }
    if b == f64::NEG_INFINITY {
        return Some(Ordering::Greater);
    }
    // 2^127 as f64; any finite |b| >= this exceeds every i128
    const I128_LIMIT: f64 = 170141183460469231731687303715884105728.0;
    if b >= I128_LIMIT {
        return Some(Ordering::Less);
    }
    if b < -I128_LIMIT {
        return Some(Ordering::Greater);
    }
    // |b| < 2^127: trunc(b) is an integral f64 exactly representable in i128
    let truncated = b.trunc();
    let whole = truncated as i128;
    match a.cmp(&whole) {
        Ordering::Equal => {
            let frac = b - truncated;
            if frac > 0.0 {
                Some(Ordering::Less)
            } else if frac < 0.0 {
                Some(Ordering::Greater)
            } else {
                Some(Ordering::Equal)
            }
        }
        other => Some(other),
    }
}
