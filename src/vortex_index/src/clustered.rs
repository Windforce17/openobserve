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

//! M6: stripe-clustered docs layout for passthrough writes.
//!
//! The M5 A/B caught the passthrough docs blob defeating projected ranged
//! reads: `TableStrategy` emits one flat leaf per (pushed chunk, column) in
//! PUSH order, so a merged file interleaves every column's tiny leaves at
//! the pushed-chunk stride (~0.7 MiB on the bench corpus). vortex's
//! object-storage coalescer bridges gaps ≤1 MiB, so a 2-column projection
//! whose true bytes were ~45 MB fetched the ENTIRE 2.4 GB docs blob in
//! 16 MiB spans (146 fetches), and a needle selection paid one round trip
//! per surviving chunk (1600 GETs). First-encode files do not have the
//! problem: vortex's default write pipeline buffers per column and lands
//! each column's segments in contiguous multi-MB runs.
//!
//! [`ClusteredDocsStrategy`] restores that property on the passthrough path
//! without decoding what it copies:
//!
//! - **Stripes**: physical segment order is [`SequenceId`] order (the file sink collapses ids
//!   before appending), so the strategy mints ids as `[stripe, column, chunk]` — within each
//!   ~[`STRIPE_BYTES`]-of-output stripe, every column's leaves land CONTIGUOUS (column 0's run,
//!   then column 1's, ...). A projected read then touches one byte run per column per stripe;
//!   unprojected columns sit between the runs as skippable >1 MiB gaps, and selection-driven
//!   fetches inside a run coalesce into single round trips. Fetch count scales as O(projected
//!   columns × blob_bytes / STRIPE_BYTES). Stripe size is measured in OUTPUT bytes, estimated
//!   online: passthrough chunks count at face value, decoded chunks at the compression ratio
//!   observed so far (starting conservative at 1/4), so parked memory stays ~one stripe of
//!   compressed segments regardless of schema.
//! - **Decoded-run coalescing**: per column, consecutive DECODED-family chunks (re-encoded merge
//!   runs, plus the slice-guard canonicalizations of narrow columns — see
//!   `container::scan_blob_encoded_chunks`) are concatenated up to [`COALESCE_MAX_ROWS`] rows /
//!   [`COALESCE_MAX_BYTES`] decoded bytes and compressed ONCE. This recovers the coarse per-column
//!   chunks first-encode files get from vortex's repartitioner (the M5 in-memory scan decoded 3640
//!   batches where the baseline decoded 233) at no extra decode cost: those chunks were already
//!   being recompressed one-by-one. Chunks that arrive ENCODED (`_source` passthrough,
//!   self-contained slices) are never touched — copying them without decode is the point of the
//!   path.
//!
//! Work is spawned EAGERLY: each finalized chunk concatenates/compresses on
//! the session's CPU pool as soon as it exists, and only the ordered sink
//! emission waits on the stripe's sequence ids. In-flight memory is the
//! open coalescing runs (≤ [`COALESCE_MAX_BYTES`] × columns) plus roughly
//! one stripe of compressed bytes parked for ordering.
//!
//! The layout TREE is unchanged (struct → chunked-per-column → flat
//! leaves), so readers need no changes; only physical segment order and
//! per-column chunk granularity differ. The o2 zone table / stats blob keep
//! the pushed-chunk axis and are unaffected.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use async_trait::async_trait;
use futures::StreamExt;
use vortex::{
    array::{
        ArrayContext, ArrayRef, Canonical, IntoArray, VortexSessionExecute,
        arrays::{ChunkedArray, StructArray, struct_::StructArrayExt},
    },
    dtype::DType,
    error::{VortexResult, vortex_bail},
    io::{runtime::Handle, session::RuntimeSessionExt},
    layout::{
        IntoLayout, LayoutChildren, LayoutRef, LayoutStrategy,
        layouts::{
            chunked::ChunkedLayout, compressed::CompressorPlugin, flat::writer::FlatLayoutStrategy,
            struct_::StructLayout,
        },
        segments::SegmentSinkRef,
        sequence::{
            SendableSequentialStream, SequenceId, SequencePointer, SequentialStreamAdapter,
            SequentialStreamExt,
        },
    },
    session::VortexSession,
};

use crate::container::is_decoded_root;

/// Stripe budget in estimated OUTPUT bytes: how much compressed data is
/// grouped into one column-major byte cluster. Larger stripes mean fewer,
/// larger per-column runs (fewer ranged GETs per projected column) at the
/// cost of up to ~one stripe of compressed segments parked in RAM awaiting
/// ordered emission. 160 MiB keeps a 2.4 GB bench blob at ~15 stripes
/// (≈2 GETs per 2-column projection per stripe → tens of fetches, order
/// parity with the v1 baseline's ~20).
pub(crate) const STRIPE_BYTES: u64 = 160 << 20;

/// Compression-ratio prior for decoded chunks before any measurement (and
/// its floor): stripe estimation starts conservative so the first stripe
/// errs small, then converges on the observed ratio.
const RATIO_PRIOR: f64 = 0.25;
const RATIO_FLOOR: f64 = 1.0 / 32.0;

/// Coalescing caps for consecutive coalescible chunks of one column
/// (whichever binds first). 128Ki rows matches the coarse per-column chunks
/// vortex's default pipeline produces for narrow columns; the 4 MiB decoded
/// cap keeps fat text columns (a re-encoded `_source` run) near their
/// pushed granularity so point reads keep a bounded decompression unit.
pub(crate) const COALESCE_MAX_ROWS: usize = 128 * 1024;
pub(crate) const COALESCE_MAX_BYTES: u64 = 4 << 20;

/// ENCODED chunks at or below this size also join coalescing runs (the
/// concat canonicalization decodes them — trivial at this size — and the
/// run recompresses as one). Without this, a column whose slices are
/// SELF-CONTAINED (e.g. `_timestamp` sequence encodings, which the slice
/// guard rightly passes through) keeps one tiny leaf per pushed chunk;
/// those fine boundaries then pin the scan's output-batch grid, and every
/// COARSE column pays a partial re-decode per tiny slice (the M6 probe
/// measured ~3x wall on a 2-column scan from exactly this). Real
/// passthrough payloads (`_source` at hundreds of KB per chunk) sit far
/// above the threshold and are never decoded.
pub(crate) const COALESCE_MAX_ENCODED_BYTES: u64 = 16 * 1024;

/// In-memory [`LayoutChildren`] over an owned child vec (vortex's own
/// `OwnedLayoutChildren` is crate-private).
#[derive(Clone)]
struct OwnedChildren(Vec<LayoutRef>);

impl LayoutChildren for OwnedChildren {
    fn to_arc(&self) -> Arc<dyn LayoutChildren> {
        Arc::new(self.clone())
    }

    fn child(&self, idx: usize, dtype: &DType) -> VortexResult<LayoutRef> {
        let Some(child) = self.0.get(idx) else {
            vortex_bail!("child index out of bounds: {idx} of {}", self.0.len());
        };
        if child.dtype() != dtype {
            vortex_bail!("child dtype mismatch: {} != {dtype}", child.dtype());
        }
        Ok(Arc::clone(child))
    }

    fn child_row_count(&self, idx: usize) -> u64 {
        self.0[idx].row_count()
    }

    fn nchildren(&self) -> usize {
        self.0.len()
    }
}

/// Column-major-per-stripe struct layout strategy for the docs blob
/// passthrough write (see the module docs). `compressor` is the
/// compress-or-pass plugin from
/// [`crate::container::docs_passthrough_strategy`]; leaves are written by a
/// plain [`FlatLayoutStrategy`] (no chunk statistics — computing them on
/// encoded chunks would decode them; readers fail open, the o2 footer
/// carries the pruning metadata).
pub(crate) struct ClusteredDocsStrategy {
    compressor: Arc<dyn CompressorPlugin>,
    flat: FlatLayoutStrategy,
    stripe_bytes: u64,
    coalesce_max_rows: usize,
    coalesce_max_bytes: u64,
}

impl ClusteredDocsStrategy {
    pub(crate) fn new<C: CompressorPlugin>(compressor: C) -> Self {
        Self {
            compressor: Arc::new(compressor),
            flat: FlatLayoutStrategy::default(),
            stripe_bytes: STRIPE_BYTES,
            coalesce_max_rows: COALESCE_MAX_ROWS,
            coalesce_max_bytes: COALESCE_MAX_BYTES,
        }
    }

    /// Test hook: shrink the stripe/coalesce budgets so small fixtures
    /// exercise multi-stripe layouts and run coalescing.
    #[cfg(test)]
    pub(crate) fn with_budgets(
        mut self,
        stripe_bytes: u64,
        coalesce_max_rows: usize,
        coalesce_max_bytes: u64,
    ) -> Self {
        self.stripe_bytes = stripe_bytes;
        self.coalesce_max_rows = coalesce_max_rows;
        self.coalesce_max_bytes = coalesce_max_bytes;
        self
    }
}

/// One finalized column chunk headed for its own flat leaf.
///
/// `decoded_raw` is the AS-PUSHED nbytes sum of the decoded-family chunks
/// behind this work (`None`/`0` = encoded verbatim) — the
/// [`OutputRatio`] observation input. Captured at push time because the
/// resident arrays may since have been COMPACTED (M25): the ratio (and with
/// it the stripe cadence, hence the emitted leaf boundaries) must keep
/// seeing the exact byte stream the un-compacted writer saw.
enum ChunkWork {
    /// Written as-is (an encoded passthrough chunk, or a lone decoded chunk
    /// the compressor encodes).
    Ready(ArrayRef, Option<u64>),
    /// ≥2 decoded-family chunks of one column, concatenated (and thereby
    /// canonicalized) on the CPU pool before compression.
    Concat(Vec<ArrayRef>, u64),
}

/// Per-column open coalescing run. Each pending entry carries its AS-PUSHED
/// nbytes and decoded-ness snapshot — the resident array may since have been
/// compacted, and the [`OutputRatio`] observation must keep seeing the exact
/// byte stream the un-compacted writer saw (single decoded chunks observe
/// their pushed nbytes; multi-part runs observe the parts' sum; encoded
/// singles observe nothing — the pre-M25 semantics verbatim).
#[derive(Default)]
struct ColumnState {
    pending: Vec<(ArrayRef, u64, bool)>,
    pending_rows: usize,
    pending_bytes: u64,
}

impl ColumnState {
    fn take_pending(&mut self) -> Option<ChunkWork> {
        self.pending_rows = 0;
        self.pending_bytes = 0;
        match self.pending.len() {
            0 => None,
            1 => {
                let (chunk, raw, decoded) = self.pending.pop().expect("len 1");
                Some(ChunkWork::Ready(chunk, decoded.then_some(raw)))
            }
            _ => {
                let parts = std::mem::take(&mut self.pending);
                let raw: u64 = parts.iter().map(|(_, raw, _)| raw).sum();
                Some(ChunkWork::Concat(
                    parts.into_iter().map(|(chunk, ..)| chunk).collect(),
                    raw,
                ))
            }
        }
    }

    /// Queue one column chunk; returns up to two finalized works (a closed
    /// run and/or the chunk itself), in row order.
    ///
    /// M25: routing keys on the chunk's ROOT ([`is_decoded_root`]) — a
    /// decoded root over encoded descendants is a freshly decoded window
    /// borrowing input buffers (the sliced-dict canonical shape), NOT a
    /// verbatim copy; it must coalesce + recompress like any decoded chunk.
    /// Decoded chunks above [`COMPACT_MIN_BYTES`] are COMPACTED into owned
    /// minimal buffers before they sit in the run: the accounting keeps the
    /// as-pushed nbytes (identical run boundaries, identical bytes out), but
    /// the RESIDENT set drops from the materialized decode form (16 B/row
    /// views + borrowed buffers x every mid-run window x every column) to
    /// the true value bytes — the width-scaled heal-arm mass M25 measured.
    fn push(
        &mut self,
        chunk: ArrayRef,
        max_rows: usize,
        max_bytes: u64,
        exec_ctx: &mut vortex::array::ExecutionCtx,
    ) -> [Option<ChunkWork>; 2] {
        let raw = chunk.nbytes();
        let decoded = is_decoded_root(&chunk);
        if !decoded && raw > COALESCE_MAX_ENCODED_BYTES {
            // encoded passthrough chunk: never merged, never reordered
            return [self.take_pending(), Some(ChunkWork::Ready(chunk, None))];
        }
        let rows = chunk.len();
        // compact-for-residence AFTER the accounting snapshot (value-
        // preserving; the run canonicalizes again at close and the
        // compressor's own entry canonicalizes+compacts, so the emitted leaf
        // bytes cannot depend on the resident representation)
        let chunk = if decoded && raw >= COMPACT_MIN_BYTES {
            compact_for_residence(chunk, exec_ctx)
        } else {
            chunk
        };
        // decoded floor for the byte cap: a tiny ENCODED chunk expands when
        // the run canonicalizes, so never count it below 8 bytes/row
        let bytes = raw.max(rows as u64 * 8);
        // close the run BEFORE it would exceed a cap (a single oversized
        // chunk still goes through alone)
        let flushed = if !self.pending.is_empty()
            && (self.pending_rows + rows > max_rows || self.pending_bytes + bytes > max_bytes)
        {
            self.take_pending()
        } else {
            None
        };
        self.pending_rows += rows;
        self.pending_bytes += bytes;
        self.pending.push((chunk, raw, decoded));
        [flushed, None]
    }
}

/// M25: decoded chunks below this size skip compact-for-residence (the copy
/// costs more than it frees).
const COMPACT_MIN_BYTES: u64 = 32 * 1024;

/// Rewrite a decoded-family chunk into owned minimal buffers (canonicalize +
/// compact — the same value-preserving entry the compressor itself applies
/// before scheme selection). Fail-open: on error keep the original chunk
/// (residence optimization only, never correctness).
fn compact_for_residence(chunk: ArrayRef, exec_ctx: &mut vortex::array::ExecutionCtx) -> ArrayRef {
    use vortex::array::CanonicalValidity;
    match chunk
        .clone()
        .execute::<CanonicalValidity>(exec_ctx)
        .and_then(|canonical| canonical.0.compact(exec_ctx))
    {
        Ok(compact) => compact.into_array(),
        Err(error) => {
            log::debug!("vix clustered: compact-for-residence failed (keeping original): {error}");
            chunk
        }
    }
}

/// Online output-size estimator: decoded chunks count at the observed
/// compressed/raw ratio (prior [`RATIO_PRIOR`] until data arrives).
#[derive(Default)]
struct OutputRatio {
    decoded_raw: AtomicU64,
    decoded_out: AtomicU64,
}

impl OutputRatio {
    fn observe(&self, raw: u64, out: u64) {
        self.decoded_raw.fetch_add(raw, Ordering::Relaxed);
        self.decoded_out.fetch_add(out, Ordering::Relaxed);
    }

    fn ratio(&self) -> f64 {
        let raw = self.decoded_raw.load(Ordering::Relaxed);
        if raw == 0 {
            return RATIO_PRIOR;
        }
        let out = self.decoded_out.load(Ordering::Relaxed);
        (out as f64 / raw as f64).max(RATIO_FLOOR)
    }
}

/// Concatenate/compress one chunk on the CPU pool, then write it as one
/// flat leaf under the pre-minted stripe-ordered sequence id.
#[allow(clippy::too_many_arguments)]
fn spawn_chunk_write(
    handle: &Handle,
    session: &VortexSession,
    ctx: &ArrayContext,
    segment_sink: &SegmentSinkRef,
    compressor: &Arc<dyn CompressorPlugin>,
    flat: &FlatLayoutStrategy,
    ratio: &Arc<OutputRatio>,
    field_ptr: &mut SequencePointer,
    field_dtype: &DType,
    work: ChunkWork,
) -> vortex::io::runtime::Task<VortexResult<LayoutRef>> {
    let mut chunk_ptr = field_ptr.advance().descend();
    let sequence_id = chunk_ptr.advance();
    let chunk_eof = chunk_ptr;
    let session = session.clone();
    let ctx = ctx.clone();
    let segment_sink = Arc::clone(segment_sink);
    let compressor = Arc::clone(compressor);
    let flat = flat.clone();
    let ratio = Arc::clone(ratio);
    let field_dtype = field_dtype.clone();
    handle.spawn_nested(move |h| async move {
        let session = session.with_handle(h.clone());
        let cpu_session = session.clone();
        let cpu_dtype = field_dtype.clone();
        let (array, decoded_raw) = h
            .spawn_cpu(move || -> VortexResult<(ArrayRef, u64)> {
                let mut exec = cpu_session.create_execution_ctx();
                // M25: the ratio-observation raw bytes ride in the work (the
                // AS-PUSHED nbytes) — the resident arrays may be compacted,
                // and the ratio stream must match the un-compacted writer's.
                let (chunk, decoded_raw) = match work {
                    ChunkWork::Ready(chunk, raw) => (chunk, raw),
                    ChunkWork::Concat(parts, raw) => {
                        let joined = ChunkedArray::try_new(parts, cpu_dtype)?
                            .into_array()
                            .execute::<Canonical>(&mut exec)?
                            .into_array();
                        (joined, Some(raw))
                    }
                };
                let out = compressor.compress_chunk(&chunk, &mut exec)?;
                Ok((out, decoded_raw.unwrap_or(0)))
            })
            .await?;
        if decoded_raw > 0 {
            ratio.observe(decoded_raw, array.nbytes());
        }
        flat.write_stream(
            ctx,
            segment_sink,
            SequentialStreamAdapter::new(
                field_dtype,
                futures::stream::iter([Ok((sequence_id, array))]),
            )
            .sendable(),
            chunk_eof,
            &session,
        )
        .await
    })
}

#[async_trait]
impl LayoutStrategy for ClusteredDocsStrategy {
    async fn write_stream(
        &self,
        ctx: ArrayContext,
        segment_sink: SegmentSinkRef,
        mut stream: SendableSequentialStream,
        _eof: SequencePointer,
        session: &VortexSession,
    ) -> VortexResult<LayoutRef> {
        let dtype = stream.dtype().clone();
        let Some(struct_dtype) = dtype.as_struct_fields_opt() else {
            vortex_bail!("ClusteredDocsStrategy can only write struct-typed streams, got {dtype}");
        };
        if dtype.is_nullable() {
            // the docs writer always produces a non-nullable top struct; a
            // nullable one would need a validity child this layout does not
            // model
            vortex_bail!("ClusteredDocsStrategy requires a non-nullable struct stream");
        }
        let nfields = struct_dtype.nfields();
        let field_dtypes: Vec<DType> = struct_dtype.fields().collect();

        let handle = session.handle();
        let ratio = Arc::new(OutputRatio::default());
        type ChunkTask = vortex::io::runtime::Task<VortexResult<LayoutRef>>;
        // per column: spawned chunk-layout tasks, in row order
        let mut column_tasks: Vec<Vec<ChunkTask>> = (0..nfields).map(|_| Vec::new()).collect();
        let mut column_rows: Vec<u64> = vec![0; nfields];
        let mut columns: Vec<ColumnState> = (0..nfields).map(|_| ColumnState::default()).collect();
        let mut total_rows: u64 = 0;

        // Open stripe: the first pushed chunk's sequence id is descended
        // into per-column branch pointers (physical order [stripe, column,
        // chunk]); the stripe's later incoming ids are parked so the NEXT
        // stripe stays ordered after this one, and everything drops at
        // stripe close.
        let mut stripe_ptr: Option<SequencePointer> = None;
        let mut field_ptrs: Vec<SequencePointer> = Vec::new();
        let mut parked_ids: Vec<SequenceId> = Vec::new();
        // estimated OUTPUT bytes accumulated in the open stripe
        let mut stripe_encoded: u64 = 0;
        let mut stripe_decoded_raw: u64 = 0;

        while let Some(item) = stream.next().await {
            let (sequence_id, chunk) = item?;
            if chunk.is_empty() {
                continue;
            }
            let mut exec = session.create_execution_ctx();
            let struct_chunk = chunk.clone().execute::<StructArray>(&mut exec)?;
            let fields = struct_chunk.unmasked_fields();
            if fields.len() != nfields {
                vortex_bail!(
                    "docs chunk carries {} fields, schema has {nfields}",
                    fields.len()
                );
            }
            if stripe_ptr.is_none() {
                let mut root = sequence_id.descend();
                field_ptrs = (0..nfields).map(|_| root.advance().descend()).collect();
                stripe_ptr = Some(root);
            } else {
                parked_ids.push(sequence_id);
            }
            total_rows += chunk.len() as u64;
            for (index, field) in fields.iter().enumerate() {
                column_rows[index] += field.len() as u64;
                // M25: root-keyed like the routing below — a decoded root
                // over borrowed encoded buffers is decoded transit, and its
                // output estimate must ride the observed decoded ratio, not
                // count its materialized-views nbytes as if it were output.
                if is_decoded_root(field) {
                    stripe_decoded_raw += field.nbytes();
                } else {
                    stripe_encoded += field.nbytes();
                }
                let works = columns[index].push(
                    field.clone(),
                    self.coalesce_max_rows,
                    self.coalesce_max_bytes,
                    &mut exec,
                );
                for work in works.into_iter().flatten() {
                    column_tasks[index].push(spawn_chunk_write(
                        &handle,
                        session,
                        &ctx,
                        &segment_sink,
                        &self.compressor,
                        &self.flat,
                        &ratio,
                        &mut field_ptrs[index],
                        &field_dtypes[index],
                        work,
                    ));
                }
            }
            let estimated = stripe_encoded as f64 + stripe_decoded_raw as f64 * ratio.ratio();
            if estimated >= self.stripe_bytes as f64 {
                // close the stripe: flush open runs into it, then release
                // its sequence branches so the next stripe's (greater) ids
                // can reach the sink
                for index in 0..nfields {
                    if let Some(work) = columns[index].take_pending() {
                        column_tasks[index].push(spawn_chunk_write(
                            &handle,
                            session,
                            &ctx,
                            &segment_sink,
                            &self.compressor,
                            &self.flat,
                            &ratio,
                            &mut field_ptrs[index],
                            &field_dtypes[index],
                            work,
                        ));
                    }
                }
                field_ptrs.clear();
                stripe_ptr = None;
                parked_ids.clear();
                stripe_encoded = 0;
                stripe_decoded_raw = 0;
            }
        }
        // final (partial) stripe: `_stripe_root` drops at scope end,
        // releasing the sequence branch
        if let Some(_stripe_root) = stripe_ptr.take() {
            for index in 0..nfields {
                if let Some(work) = columns[index].take_pending() {
                    column_tasks[index].push(spawn_chunk_write(
                        &handle,
                        session,
                        &ctx,
                        &segment_sink,
                        &self.compressor,
                        &self.flat,
                        &ratio,
                        &mut field_ptrs[index],
                        &field_dtypes[index],
                        work,
                    ));
                }
            }
            field_ptrs.clear();
            parked_ids.clear();
        }

        // assemble: per column a ChunkedLayout over its chunk layouts (or
        // the single chunk itself), then the StructLayout — the exact tree
        // TableStrategy produced, so readers are unaffected
        let mut children: Vec<LayoutRef> = Vec::with_capacity(nfields);
        for (index, tasks) in column_tasks.into_iter().enumerate() {
            let mut layouts = futures::future::try_join_all(tasks).await?;
            let child = if layouts.len() == 1 {
                layouts.pop().expect("one layout")
            } else {
                ChunkedLayout::new(
                    column_rows[index],
                    field_dtypes[index].clone(),
                    Arc::new(OwnedChildren(layouts)),
                )
                .into_layout()
            };
            children.push(child);
        }
        Ok(StructLayout::new(total_rows, dtype, children).into_layout())
    }
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{ArrayRef as ArrowArrayRef, Int64Array, StringArray},
        datatypes::{DataType as ArrowDataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use vortex::{
        VortexSessionDefault,
        file::OpenOptionsSessionExt,
        io::runtime::{BlockingRuntime, single::SingleThreadRuntime},
    };

    use super::*;
    use crate::container::{BlobHandle, RowSelection, scan_blob};

    fn fixture_schema() -> Schema {
        Schema::new(vec![
            Field::new("a", ArrowDataType::Int64, false),
            Field::new("b", ArrowDataType::Utf8, false),
        ])
    }

    /// `nbatches` two-column batches of `rows_per_batch` rows with globally
    /// increasing values.
    fn fixture_batches(nbatches: usize, rows_per_batch: usize) -> Vec<RecordBatch> {
        let schema = Arc::new(fixture_schema());
        (0..nbatches)
            .map(|batch| {
                let base = (batch * rows_per_batch) as i64;
                let a: Vec<i64> = (0..rows_per_batch as i64).map(|i| base + i).collect();
                let b: Vec<String> = a.iter().map(|v| format!("value-{v:08}")).collect();
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int64Array::from(a)) as ArrowArrayRef,
                        Arc::new(StringArray::from(b)) as ArrowArrayRef,
                    ],
                )
                .unwrap()
            })
            .collect()
    }

    /// Per-column `(offset, length)` of every flat leaf, in chunk order,
    /// straight from the blob footer.
    fn column_leaf_extents(blob: &[u8]) -> Vec<(String, Vec<(u64, u64)>)> {
        use vortex::io::session::RuntimeSessionExt;
        let runtime = SingleThreadRuntime::default();
        let session = VortexSession::default().with_handle(runtime.handle());
        let vxf = session
            .open_options()
            .open_buffer(bytes::Bytes::from(blob.to_vec()))
            .unwrap();
        let footer = vxf.footer();
        let segmap = footer.segment_map().clone();
        let root = footer.layout().clone();
        let names: Vec<Arc<str>> = root.child_names().collect();
        let children = root.children().unwrap();
        names
            .iter()
            .zip(children)
            .map(|(name, child)| {
                let mut leaf_segments: Vec<u32> = Vec::new();
                let mut stack = vec![child];
                while let Some(node) = stack.pop() {
                    for segment in node.segment_ids() {
                        leaf_segments.push(*segment);
                    }
                    if let Ok(kids) = node.children() {
                        stack.extend(kids);
                    }
                }
                leaf_segments.sort_unstable();
                let extents = leaf_segments
                    .into_iter()
                    .map(|segment| {
                        let spec = &segmap[segment as usize];
                        (spec.offset, spec.length as u64)
                    })
                    .collect();
                (name.to_string(), extents)
            })
            .collect()
    }

    /// The identity "compressor": every chunk is written as-is, so the test
    /// asserts pure LAYOUT behavior (order/segmentation), not encodings.
    fn identity_strategy(
        stripe_bytes: u64,
        max_rows: usize,
        max_bytes: u64,
    ) -> Arc<dyn LayoutStrategy> {
        Arc::new(
            ClusteredDocsStrategy::new(
                |chunk: &ArrayRef,
                 _ctx: &mut vortex::array::ExecutionCtx|
                 -> VortexResult<ArrayRef> { Ok(chunk.clone()) },
            )
            .with_budgets(stripe_bytes, max_rows, max_bytes),
        )
    }

    /// Stripe clustering: with a stripe budget that splits the input, each
    /// column's leaves are physically CONTIGUOUS within a stripe and the
    /// columns' runs are ordered (col0 run, then col1 run) — the property
    /// that keeps a projected ranged read from fetching unprojected bytes.
    #[test]
    fn stripes_cluster_columns_physically() {
        // 8 batches x 1k rows; tiny coalesce caps keep one leaf per batch,
        // and a small stripe budget forces >=2 stripes
        let batches = fixture_batches(8, 1000);
        let blob = crate::container::write_vortex_blob(
            &fixture_schema(),
            &batches,
            identity_strategy(64 * 1024, 1000, 1),
            0,
        )
        .unwrap();

        let columns = column_leaf_extents(&blob);
        assert_eq!(columns.len(), 2);
        let (a_name, a_extents) = &columns[0];
        let (b_name, b_extents) = &columns[1];
        assert_eq!(a_name, "a");
        assert_eq!(b_name, "b");
        assert_eq!(a_extents.len(), 8, "one leaf per pushed batch");
        assert_eq!(b_extents.len(), 8);

        // reconstruct stripes: a stripe starts wherever column a's leaf is
        // NOT adjacent to its previous leaf (alignment padding tolerated)
        let adjacent = |prev: (u64, u64), next: (u64, u64)| {
            next.0 >= prev.0 + prev.1 && next.0 - (prev.0 + prev.1) <= 64
        };
        let mut stripe_starts = vec![0usize];
        for i in 1..a_extents.len() {
            if !adjacent(a_extents[i - 1], a_extents[i]) {
                stripe_starts.push(i);
            }
        }
        assert!(
            stripe_starts.len() >= 2,
            "the 64KiB stripe budget must split 8x1k rows into >=2 stripes, got {stripe_starts:?}"
        );
        // per stripe: a's run is contiguous, b's run is contiguous, and the
        // whole a-run precedes the whole b-run
        let mut bounds = stripe_starts.clone();
        bounds.push(a_extents.len());
        for window in bounds.windows(2) {
            let (start, end) = (window[0], window[1]);
            for i in start + 1..end {
                assert!(
                    adjacent(a_extents[i - 1], a_extents[i]),
                    "column a leaves must be contiguous within a stripe: {:?} -> {:?}",
                    a_extents[i - 1],
                    a_extents[i]
                );
                assert!(
                    adjacent(b_extents[i - 1], b_extents[i]),
                    "column b leaves must be contiguous within a stripe: {:?} -> {:?}",
                    b_extents[i - 1],
                    b_extents[i]
                );
            }
            let a_end = a_extents[end - 1].0 + a_extents[end - 1].1;
            assert!(
                b_extents[start].0 >= a_end,
                "column b's stripe run must start after column a's ends: b at {} vs a end {a_end}",
                b_extents[start].0
            );
        }

        // and the bytes round-trip
        assert_roundtrip(&blob, &batches);
    }

    /// Decoded-run coalescing: consecutive decoded chunks concatenate up to
    /// the row cap, shrinking per-column chunk counts while the rows
    /// round-trip exactly.
    #[test]
    fn decoded_runs_coalesce_to_row_cap() {
        // 16 batches x 500 rows, cap 2000 rows -> 4 leaves per column
        let batches = fixture_batches(16, 500);
        let blob = crate::container::write_vortex_blob(
            &fixture_schema(),
            &batches,
            identity_strategy(u64::MAX, 2000, u64::MAX),
            0,
        )
        .unwrap();
        let columns = column_leaf_extents(&blob);
        for (name, extents) in &columns {
            assert_eq!(
                extents.len(),
                4,
                "column {name}: 16x500 rows under a 2000-row cap must make 4 leaves"
            );
        }
        assert_roundtrip(&blob, &batches);
    }

    /// Read the blob back and compare against the pushed batches, row by
    /// row (concatenated; scanned columns are cast to the fixture types —
    /// vortex canonical strings decode as Utf8View).
    fn assert_roundtrip(blob: &[u8], batches: &[RecordBatch]) {
        let got = scan_blob(
            &BlobHandle::Mem(bytes::Bytes::from(blob.to_vec())),
            None,
            RowSelection::All,
        )
        .unwrap();
        let schema = Arc::new(fixture_schema());
        let got: Vec<RecordBatch> = got
            .iter()
            .map(|batch| {
                let columns: Vec<ArrowArrayRef> = batch
                    .columns()
                    .iter()
                    .zip(schema.fields())
                    .map(|(column, field)| arrow::compute::cast(column, field.data_type()).unwrap())
                    .collect();
                RecordBatch::try_new(Arc::clone(&schema), columns).unwrap()
            })
            .collect();
        let want = arrow::compute::concat_batches(&schema, batches).unwrap();
        let got = arrow::compute::concat_batches(&schema, &got).unwrap();
        assert_eq!(got.num_rows(), want.num_rows());
        assert_eq!(got, want, "clustered write must round-trip the rows");
    }
}
