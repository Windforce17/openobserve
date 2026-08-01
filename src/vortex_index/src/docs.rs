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

impl NumScalar {
    fn to_lit(self) -> vortex::expr::Expression {
        match self {
            NumScalar::I64(v) => lit(v),
            NumScalar::F64(v) => lit(v),
        }
    }
}

/// One numeric range conjunct pushed into a docs scan: optional min/max,
/// each with an inclusive flag. Composes `>`, `>=`, `<`, `<=`, `=`
/// (min==max inclusive) and BETWEEN.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnBound {
    pub column: String,
    pub min: Option<(NumScalar, bool)>,
    pub max: Option<(NumScalar, bool)>,
}

use crate::{
    container::{
        BlobHandle, PROP_ROW_COUNT, PROP_ROW_GROUP_SIZE, RowSelection, VixContainer,
        blob_arrow_schema, parse_container, parse_container_ranged, require_supported_format,
        scan_blob_streaming,
    },
    error::{Result, VixError},
    source::VixRangeSource,
    writer::TIMESTAMP_COL_NAME,
};

/// Scan handle over the `docs` blob of one `.vix` core file — held fully
/// in memory or fetched by ranges on demand. Opening parses the puffin
/// footer and the blob's arrow schema only.
#[derive(Debug)]
pub struct VixDocs {
    row_count: u64,
    row_group_size: usize,
    docs_blob: BlobHandle,
    schema: SchemaRef,
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
        require_supported_format(properties)?;
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
        })
    }

    /// Number of documents stored in the file (`row_count` property).
    pub fn row_count(&self) -> u64 {
        self.row_count
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
        let selection = match rows {
            None => RowSelection::All,
            Some(indices) => RowSelection::Indices(indices),
        };
        let mut filter = ts_range.map(|(min_micros, max_micros)| {
            let ts = || get_item(TIMESTAMP_COL_NAME, root());
            and(gt_eq(ts(), lit(min_micros)), lt(ts(), lit(max_micros)))
        });
        for bound in bounds {
            // absent column: ignore the bound (the caller's engine
            // re-applies every predicate on returned rows)
            if self.schema.field_with_name(&bound.column).is_err() {
                continue;
            }
            let column = || get_item(bound.column.clone(), root());
            let mut add = |expr: vortex::expr::Expression| {
                filter = Some(match filter.take() {
                    Some(prev) => and(prev, expr),
                    None => expr,
                });
            };
            if let Some((value, inclusive)) = bound.min {
                let value = value.to_lit();
                add(if inclusive {
                    gt_eq(column(), value)
                } else {
                    vortex::expr::gt(column(), value)
                });
            }
            if let Some((value, inclusive)) = bound.max {
                let value = value.to_lit();
                add(if inclusive {
                    vortex::expr::lt_eq(column(), value)
                } else {
                    lt(column(), value)
                });
            }
        }
        scan_blob_streaming(
            &self.docs_blob,
            names.as_deref(),
            selection,
            filter,
            limit,
            decode_threads,
            &mut |batch| on_batch(batch).map_err(VixError::Callback),
        )?;
        Ok(())
    }

    /// File-level `(min, max)` of a numeric docs column from the vortex
    /// footer statistics — zero data reads. `None` = cannot prune (no
    /// stats, non-numeric, or unknown column). A predicate that cannot
    /// match within these bounds may skip the WHOLE file.
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
