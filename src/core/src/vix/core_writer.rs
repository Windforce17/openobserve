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
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering as AtomicOrdering},
        mpsc::{Receiver, SyncSender, sync_channel},
    },
};

use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Float16Array, Float32Array,
        Float64Array, Int64Array, LargeBinaryArray, LargeStringArray, StringArray, StringViewArray,
        new_empty_array,
    },
    compute::{cast, filter_record_batch, interleave, nullif},
    record_batch::RecordBatch,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use config::{
    ID_COL_NAME, ORIGINAL_DATA_COL_NAME, PARQUET_MAX_ROW_GROUP_SIZE, TIMESTAMP_COL_NAME, cluster,
    get_config,
    meta::stream::{FileMeta, StreamType},
};
use datafusion::{catalog::TableProvider, physical_plan::execute_stream};
pub use vortex_index::VixOutput;
use vortex_index::{
    DocIdMap, SOURCE_COL_NAME, SOURCE_RENAMED_COL_NAME, VixDocs, VixRangeSource, VixReader,
    VixWriter, VixWriterOptions, VixWriterStats,
};

type FastHashMap<K, V> = std::collections::HashMap<K, V, rapidhash::fast::GlobalState>;
type FastHashSet<K> = std::collections::HashSet<K, rapidhash::fast::GlobalState>;

use crate::search::datafusion::{
    exec::DataFusionContextBuilder, source_synthesis::synthesize_source,
    table_provider::uniontable::NewUnionTable,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MergeTypePolicy {
    /// Existing behavior: latest stream schema selects the target type, and
    /// column derivation requires equivalent physical types.
    Legacy,
    /// Latest stream schema selects the fixed target type. In addition to
    /// derivation-equivalent representations, scalar JSON values may cast to
    /// a string-family target; lossy and value-dependent reverse casts stay
    /// on the `_source` fallback.
    LatestSchema,
}

impl MergeTypePolicy {
    fn configured() -> Self {
        match get_config().common.vix_merge_type_policy.as_str() {
            "latest_schema" => Self::LatestSchema,
            _ => Self::Legacy,
        }
    }
}

/// The row/byte bounds of one staged docs batch. Production uses
/// [`Default`]; tests shrink the caps to prove the chunked flow with small
/// data.
#[derive(Clone, Copy, Debug)]
struct BatchCaps {
    rows: usize,
    bytes: usize,
    /// Force the index policy instead of resolving it from the live config
    /// (build path: [`vix_build_index_enabled`]; merge path:
    /// [`vix_index_enabled`], consulted by [`build_merge_plan`]). Two
    /// callers: tests (env-backed sets are process-global and cannot be
    /// toggled safely), and — M31 — the compactor's index-DEFER policy,
    /// which passes `Some(false)` for a non-final all-index-less merge
    /// group (see `ZO_VIX_MERGE_INDEX_DEFER_BELOW_MB`).
    index_enabled_override: Option<bool>,
    /// Test seam (#52): comma list of bloom-only fields injected into the
    /// merge writer options (env-backed config is process-global, so tests
    /// cannot toggle `ZO_VIX_BLOOM_ONLY_FIELDS` safely). `None` in every
    /// production call.
    bloom_only_override: Option<&'static str>,
    /// Test seam (#52/M7): `(auto_ratio, min_distinct)` injected into the
    /// writer options on BOTH the build and merge paths — drives the
    /// first-encode AUTO demotion and the merge-time input-dictionary AUTO
    /// without touching the process-global env config (whose v2 defaults,
    /// ratio 0.5 / floor 65536, keep AUTO out of small-data tests). `None`
    /// in every production call.
    bloom_auto_override: Option<(f64, u64)>,
    /// Test seam: disable the #46 column-derived rebuild so a test can
    /// produce the SOURCE-derived output over the same inputs (the parity
    /// referee). `false` in every production call.
    force_source_derivation: bool,
    /// Test seam: disable the docs-chunk passthrough (and with it the
    /// concatenation fast path) so a test can produce the pure
    /// decode + re-encode output over the same inputs — the differential
    /// oracle passthrough outputs are compared against. `false` in every
    /// production call: passthrough is the DEFAULT merge shape.
    force_decode: bool,
    /// Test seam for the env-backed compaction type policy.
    merge_type_policy_override: Option<MergeTypePolicy>,
}

impl Default for BatchCaps {
    fn default() -> Self {
        Self {
            rows: DOCS_BATCH_ROWS,
            bytes: DOCS_BATCH_BYTES,
            index_enabled_override: None,
            bloom_only_override: None,
            bloom_auto_override: None,
            force_source_derivation: false,
            force_decode: false,
            merge_type_policy_override: None,
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
    /// The `.vxi` index-sidecar bytes; `None` for index-off builds
    /// (#40/#42 — `stats.index_size == 0`). Uploaded AFTER the data object,
    /// BEFORE the file_list row.
    pub index: Option<Vec<u8>>,
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
    /// The `.vxi` index-sidecar bytes; `None` for index-off merge plans
    /// (`stats.index_size == 0`).
    pub index: Option<Vec<u8>>,
    pub stats: VixWriterStats,
    /// `true` when the index came from the input-dictionary merge fast
    /// path, `false` for a full term rebuild.
    pub used_index_merge: bool,
    /// Bounded docs batches pushed into the writer (observability).
    pub docs_batches: usize,
    /// Rows dropped by degenerate-`_timestamp` cleansing.
    pub dropped_rows: u64,
    /// #51c observability: inputs whose docs rows were copied through the
    /// encoded-chunk passthrough (no decode, no recompression — the default
    /// merge shape). `0` when no input qualified (every input decoded).
    pub docs_passthrough_inputs: usize,
    /// #51c-c: `true` when the output was written in CONCATENATION order
    /// (stamped `row_order=concat` — rows not globally time-sorted). `false`
    /// for every sorted output (disjoint concat of sorted inputs included:
    /// that order IS globally sorted).
    pub concat_order: bool,
    /// M18 observability: column-windows the deterministic slice guard
    /// canonicalized + recompressed during the encoded-chunk copies (a scan
    /// window cutting inside one column's stored leaf — the shape that used
    /// to reach the writer as a non-serializable slice or, worse, as an
    /// offset-lossy reduced slice). `0` when every copied window was
    /// leaf-aligned.
    pub docs_sliced_windows: u64,
    /// M18 observability: encoded column chunks the passthrough WRITE
    /// strategy re-encoded because their tree carried an encoding the file
    /// writer cannot serialize (per-chunk fail-open backstop; the loud prod
    /// ".110 vortex.slice not permitted by ctx" class). With the scan-side
    /// guard upstream this should stay `0` — nonzero means a wrapper shape
    /// the scan did not predict, worth a look at debug logs.
    pub docs_failopen_chunks: u64,
    /// M31 observability: `true` when a REBUILD derived its terms from the
    /// streamed columns (#46) instead of parsing `_source` per row (the
    /// 5.4x arm). Always `false` on the fast path (no derivation ran).
    /// Watched in the compactor's merged-file summary line — the gate's
    /// fleet-wide silent miss (Utf8View vs Utf8) hid behind having no
    /// signal for exactly this.
    pub terms_from_columns: bool,
    /// Merge-orchestration work that should disappear on pure concat/disjoint
    /// runs. These counters are intentionally independent of writer/index
    /// statistics: they measure row-order materialization and Arrow staging,
    /// not logical output.
    pub perf: MergePerfStats,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MergePerfStats {
    /// `(input, row)` entries materialized for a genuine sorted interleave.
    /// Pure concat/disjoint paths keep this at zero.
    pub order_entries_materialized: u64,
    /// Typed empty arrays constructed solely to align inactive inputs with an
    /// Arrow interleave. Active-input compaction keeps this at zero.
    pub staged_empty_arrays: u64,
    /// Columns passed through Arrow's generic interleave kernel.
    pub interleaved_columns: u64,
}

/// Dependency-neutral cooperative cancellation shared with compactor
/// orchestration. Cancellation is monotonic and checks are one relaxed
/// atomic load at bounded merge boundaries (never per value).
#[derive(Clone, Debug, Default)]
pub struct VixMergeCancellation(Arc<AtomicBool>);

impl VixMergeCancellation {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_flag(flag: Arc<AtomicBool>) -> Self {
        Self(flag)
    }

    pub fn flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.0)
    }

    pub fn cancel(&self) {
        self.0.store(true, AtomicOrdering::Relaxed);
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.load(AtomicOrdering::Relaxed)
    }

    fn check(&self, boundary: &str) -> Result<(), anyhow::Error> {
        if self.is_cancelled() {
            Err(anyhow::anyhow!("vix merge cancelled at {boundary}"))
        } else {
            Ok(())
        }
    }

    fn check_context(
        &self,
        boundary: &str,
        context: impl std::fmt::Display,
    ) -> Result<(), anyhow::Error> {
        if self.is_cancelled() {
            Err(anyhow::anyhow!(
                "vix merge cancelled at {boundary} ({context})"
            ))
        } else {
            Ok(())
        }
    }
}

/// Whether core files of `stream_type` carry a term index at all (#40):
/// stream types named in `ZO_VIX_INDEX_DISABLED_STREAM_TYPES` (default:
/// metrics) write COLUMN-STORE-ONLY files — no dict/terms/bloom blobs,
/// `index=none` stamped, every schema field materialized as a docs column.
fn vix_index_enabled(stream_type: StreamType) -> bool {
    !config::is_vix_index_disabled(stream_type)
}

/// Whether THIS build-path file (the WAL move job or the segment L0
/// builder) carries a term index. Two knobs compose: #40's
/// `ZO_VIX_INDEX_DISABLED_STREAM_TYPES` disables the index at EVERY level
/// (builds and merges), while #42's `ZO_VIX_L0_INDEX_OFF_STREAM_TYPES`
/// disables it for ingest-side builds only — merge plans keep resolving
/// via [`vix_index_enabled`], so #42 files HEAL to indexed as compaction
/// merges them (index-off inputs force the `_source` rebuild) or as the
/// single-file sweep classifies them `NeedsRebuild` under an indexed plan.
fn vix_build_index_enabled(stream_type: StreamType) -> bool {
    vix_index_enabled(stream_type) && !config::is_vix_l0_index_off(stream_type)
}

/// The shared [`VixWriterOptions`] of every core-file producer.
///
/// `_original` is never term-indexed, so it is dropped from the full-text
/// list. `index_enabled` is the stream-type policy resolved by the caller
/// ([`vix_index_enabled`]): `false` builds a column-store-only file (#40).
/// Which fields become docs columns is not an option anymore (v2 DESIGN §2:
/// every schema field is a column).
fn core_writer_options(
    fts_fields: &[String],
    bloom_fields: Vec<String>,
    index_enabled: bool,
) -> VixWriterOptions {
    let cfg = get_config();
    vortex_index::configure_shared_cpu_executor(vix_cpu_executor_threads());
    VixWriterOptions {
        index_enabled,
        bloom_field_names: bloom_fields,
        bloom_composite: cfg.common.vix_bloom_composite,
        bloom_only_field_names: cfg
            .common
            .vix_bloom_only_fields
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect(),
        bloom_only_never: cfg
            .common
            .vix_bloom_only_never
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect(),
        // #52/M7 first-encode AUTO demotion (the writer applies the shared
        // rule to its own term map at finish; merge plans ALSO apply it to
        // input dictionaries in build_merge_plan)
        bloom_only_auto_ratio: cfg.common.vix_bloom_only_auto_ratio,
        bloom_only_min_distinct: cfg.common.vix_bloom_only_min_distinct,
        bloom_fpp: cfg.common.vix_bloom_fpp,
        fts_field_names: fts_fields
            .iter()
            .filter(|f| f.as_str() != ORIGINAL_DATA_COL_NAME)
            .cloned()
            .collect(),
        postings_chunk_bytes: cfg.common.vix_postings_chunk_bytes,
        max_raw_term_len: cfg.common.vix_max_raw_term_len,
        row_group_size: PARQUET_MAX_ROW_GROUP_SIZE,
        docs_chunk_bytes: cfg.common.vix_docs_chunk_bytes,
        docs_chunk_max_rows: cfg.common.vix_docs_chunk_max_rows,
        min_token_len: cfg.limit.inverted_index_min_token_length,
        max_token_len: cfg.limit.inverted_index_max_token_length,
        // #15 rollout discipline: default 0 keeps the out-of-row postings
        // writer dark; flip ZO_VIX_PLIST_MIN_DOCS only after the release
        // carrying pointer-cell read support is on EVERY pod.
        postings_plist_min_docs: cfg.common.vix_plist_min_docs as u32,
        // H2 per-column chunk stats (pay-as-you-go density + byte cap)
        stats_min_density: cfg.common.vix_stats_min_density,
        stats_max_bytes: cfg.common.vix_stats_max_bytes,
        // Single-file build (move job): parallelize the `docs`/index blob
        // encode across cores when spare parallelism exists. The compaction
        // merge overrides this with merge_threads() in build_merge_plan.
        encode_threads: build_encode_threads(),
        // #51b: only the compaction merge consumes this; build_merge_plan
        // overrides it from ZO_VIX_MERGE_KWAY_THREADS
        merge_kway_threads: 0,
        // 0 = the writer's default sample budget (tests shrink it)
        docs_encode_sample_bytes: 0,
        // move-job builds never spill terms (small dictionaries); the
        // compaction merge sets the spill dir in build_merge_plan, and the
        // move path sets output_spool_dir per build (big batched moves
        // spool, small ones stay in memory).
        term_spill_dir: None,
        term_spill_bytes: 0,
        output_spool_dir: None,
        // #51c: never a producer default — the compaction merge's disjoint
        // arm flips it per merge when the gate says so
        // (merge_core_files_indexed).
        docs_passthrough: false,
        // #51c-c: never a producer default — only the compaction merge's
        // concatenation path stamps its writer concat (qualify_concat_fast_
        // path / qualify_heal_passthrough / the forced-concat rebuild).
        concat_row_order: false,
        // §4: every core-file producer here upholds the all-present-columns
        // invariant (DESIGN §2: docs schema = union of the batches' present
        // fields), which licenses the query path's absent-column file skip.
        // build_merge_plan demotes this to the AND over its inputs — an
        // incomplete input's `_source` rows can hide fields that never
        // became columns, and a decode-merge carries them along.
        columns_complete: true,
    }
}

/// M12/M30 rebuild admission: a process-wide gate on CONCURRENT rebuild-path
/// merges (`ZO_VIX_REBUILD_CONCURRENCY`; 0 = auto: max(1,
/// ZO_FILE_MERGE_THREAD_NUM / 2); always ≥ 1). The dev-launch OOM wave was
/// 8 concurrent first-gen rebuilds over multi-GB groups: each rebuild's
/// working set (windowed decode staging + `_source` term derivation + term
/// map) is individually bounded by the batch caps and the term-spill
/// budget, but the incident dimension was the NUMBER of such footprints
/// stacking — the H3 byte budget bounds downloads, not per-rebuild working
/// memory. A concurrency cap bounds the stack count exactly, with none of
/// the 5-10x per-stream error a `original_size × factor` byte estimate
/// carries (vpc-flow vs traces arrow expansion measured that far apart).
/// Fast-path (passthrough + k-way) merges never touch this gate.
///
/// M30 closes the gate contract's owed re-measure the other way around: the
/// count stays the hard CAP, but every slot beyond the guaranteed first one
/// additionally requires LIVE memory headroom — sampled process RSS (the
/// same 1s-cadence `NODE_MEMORY_USAGE` gauge the ingest breaker consults)
/// plus `ZO_VIX_REBUILD_HEADROOM_MB` charged for each extra rebuild in
/// flight (candidate included — their transit may not be RSS-visible yet,
/// the ingest-admission burst lesson) must stay under 90% of the memory
/// limit. Per-rebuild transit varies 5-10x per stream, so it is bounded at
/// runtime instead of estimated; a pod whose RSS floor leaves no room simply
/// stays at one slot, which is exactly the pinned prod regime that held
/// kills=0. Waiters re-check on a 500ms tick since RSS moves without gate
/// events. Headroom 0 = the exact M12 count-only behavior.
struct RebuildGate {
    in_flight: parking_lot::Mutex<usize>,
    cv: parking_lot::Condvar,
    max: usize,
    /// Bytes charged per extra rebuild against the envelope; 0 = count-only.
    headroom_bytes: usize,
    /// 90% of the cgroup/node memory limit; admission ceiling for extras.
    envelope_bytes: usize,
}

struct RebuildPermit<'a>(&'a RebuildGate);

impl RebuildGate {
    fn new(max: usize, headroom_bytes: usize, envelope_bytes: usize) -> Self {
        Self {
            in_flight: parking_lot::Mutex::new(0),
            cv: parking_lot::Condvar::new(),
            max,
            headroom_bytes,
            envelope_bytes,
        }
    }

    /// Would admitting one more rebuild (with `running` already in flight)
    /// keep projected memory inside the envelope? `running ≥ 1` here — the
    /// first slot never consults this. Charges the full headroom for every
    /// extra INCLUDING the candidate: an admitted rebuild's transit lags in
    /// the sampled RSS, and double-charging while it materializes only errs
    /// conservative. An unsampled gauge (rss 0 — benches, tests, boot)
    /// admits up to the count cap: there is nothing to bound against.
    fn headroom_admits(&self, running: usize, rss: usize) -> bool {
        if self.headroom_bytes == 0 {
            return true;
        }
        let projected = self.headroom_bytes.saturating_mul(running);
        rss.saturating_add(projected) <= self.envelope_bytes
    }

    /// The 1s-cadence sampled process RSS (same gauge the ingest breaker
    /// consults; updated by `update_node_memory_usage` in the jobs crate).
    fn sampled_rss() -> usize {
        config::metrics::NODE_MEMORY_USAGE
            .with_label_values::<&str>(&[])
            .get()
            .max(0) as usize
    }

    /// Block the calling (merge worker) thread until a rebuild slot frees.
    /// Blocking is the mechanism, not an accident: the worker holds nothing
    /// else, and the first slot always admits, so progress is guaranteed
    /// while the queue drains one bounded rebuild at a time.
    #[cfg(test)]
    fn acquire(&self) -> RebuildPermit<'_> {
        self.acquire_with_cancellation(None)
            .expect("uncancellable rebuild gate acquisition")
    }

    fn acquire_with_cancellation(
        &self,
        cancellation: Option<&VixMergeCancellation>,
    ) -> Result<RebuildPermit<'_>, anyhow::Error> {
        let started = std::time::Instant::now();
        let mut in_flight = self.in_flight.lock();
        loop {
            if let Some(cancellation) = cancellation {
                cancellation.check("rebuild admission")?;
            }
            if *in_flight == 0 {
                break; // guaranteed slot: progress regardless of memory
            }
            if *in_flight < self.max && self.headroom_admits(*in_flight, Self::sampled_rss()) {
                break;
            }
            // Timed wait: a permit drop notifies immediately, but memory
            // headroom can also open with NO gate event (RSS falls as a
            // build wave drains) — the tick re-evaluates admission then.
            self.cv
                .wait_for(&mut in_flight, std::time::Duration::from_millis(500));
        }
        *in_flight += 1;
        let busy = *in_flight;
        drop(in_flight);
        let waited = started.elapsed();
        if waited > std::time::Duration::from_millis(50) {
            log::info!(
                "vix merge: rebuild admitted after {waited:?} wait ({busy}/{} slots busy)",
                self.max
            );
        } else {
            log::debug!(
                "vix merge: rebuild admitted ({busy}/{} slots busy)",
                self.max
            );
        }
        Ok(RebuildPermit(self))
    }
}

impl Drop for RebuildPermit<'_> {
    fn drop(&mut self) {
        let mut in_flight = self.0.in_flight.lock();
        *in_flight = in_flight.saturating_sub(1);
        drop(in_flight);
        // notify_all, not one: with the memory check, the waiter woken by a
        // permit drop is not necessarily admissible while another is — every
        // waiter re-evaluates its own admission (worker counts are ≤ ~10).
        self.0.cv.notify_all();
    }
}

static REBUILD_GATE: std::sync::LazyLock<RebuildGate> = std::sync::LazyLock::new(|| {
    let cfg = get_config();
    let configured = cfg.common.vix_rebuild_concurrency;
    let max = if configured > 0 {
        configured
    } else {
        // file_merge_thread_num is already auto-resolved (>0) at config load
        std::cmp::max(1, cfg.limit.file_merge_thread_num / 2)
    };
    RebuildGate::new(
        max.max(1),
        cfg.common
            .vix_rebuild_headroom_mb
            .saturating_mul(1024 * 1024),
        cfg.limit.mem_total / 100 * 90,
    )
});
/// An owned process-wide rebuild-memory admission permit. The compactor
/// acquires this before starting the blocking rebuild controller, so only
/// memory-admitted rebuilds enter execution. Construction stays private to
/// this module; dropping the value releases the slot.
pub struct VixRebuildPermit {
    _permit: RebuildPermit<'static>,
}

/// Block an orchestration thread until rebuild memory is available.
///
/// The caller acquires this before starting the rebuild controller.
/// Cancellation is checked on every gate wake/tick.
pub fn acquire_vix_rebuild_permit_with_cancellation(
    cancellation: &VixMergeCancellation,
) -> Result<VixRebuildPermit, anyhow::Error> {
    REBUILD_GATE
        .acquire_with_cancellation(Some(cancellation))
        .map(|permit| VixRebuildPermit { _permit: permit })
}

#[cfg(test)]
mod rebuild_gate_tests {
    use super::RebuildGate;

    const GB: usize = 1024 * 1024 * 1024;

    #[test]
    fn headroom_disabled_is_count_only() {
        let gate = RebuildGate::new(4, 0, 0);
        // envelope 0 + any rss would reject every extra were the check live
        assert!(gate.headroom_admits(1, 40 * GB));
        assert!(gate.headroom_admits(3, usize::MAX));
    }

    #[test]
    fn headroom_charges_every_extra_including_candidate() {
        // 48G limit -> 43.2G envelope, 5G headroom, floor 30G:
        // extras 1..2 admit (35G, 40G), the 3rd extra projects 45G > 43.2G.
        let envelope = 48 * GB / 100 * 90;
        let gate = RebuildGate::new(8, 5 * GB, envelope);
        assert!(gate.headroom_admits(1, 30 * GB));
        assert!(gate.headroom_admits(2, 30 * GB));
        assert!(!gate.headroom_admits(3, 30 * GB));
        // a fatter floor stays at one slot (the pinned-prod regime)
        assert!(!gate.headroom_admits(1, 40 * GB));
        // an unsampled gauge (0) bounds nothing — count cap governs
        assert!(gate.headroom_admits(7, 0));
    }

    #[test]
    fn headroom_arithmetic_saturates() {
        let gate = RebuildGate::new(2, usize::MAX, usize::MAX);
        // usize::MAX projected + rss must not overflow-panic
        assert!(gate.headroom_admits(1, usize::MAX));
    }

    #[test]
    fn first_slot_admits_even_with_zero_envelope() {
        // memory check would reject everything; the first acquire must not block
        let gate = RebuildGate::new(2, GB, 0);
        let permit = gate.acquire();
        drop(permit);
    }

    #[test]
    fn count_cap_blocks_and_release_unblocks() {
        use std::{
            sync::{
                Arc,
                atomic::{AtomicBool, Ordering},
            },
            time::Duration,
        };
        // headroom disabled -> pure count gate at 1
        let gate = Arc::new(RebuildGate::new(1, 0, 0));
        let first = gate.acquire();
        let entered = Arc::new(AtomicBool::new(false));
        let handle = {
            let gate = Arc::clone(&gate);
            let entered = Arc::clone(&entered);
            std::thread::spawn(move || {
                let _p = gate.acquire();
                entered.store(true, Ordering::SeqCst);
            })
        };
        std::thread::sleep(Duration::from_millis(100));
        assert!(
            !entered.load(Ordering::SeqCst),
            "second acquire ran past a full gate"
        );
        drop(first);
        handle.join().unwrap();
        assert!(entered.load(Ordering::SeqCst));
    }
}

/// CPU quota available to VIX after sharing the process with configured
/// query/ingest roles.
fn vix_role_cpu_capacity() -> usize {
    std::cmp::max(1, get_config().limit.cpu_num / cluster::cpu_role_divisor())
}

/// Until flat compaction joins the same executor, retain two CPUs for Tokio,
/// lease/range completion, and the independent DataFusion path.
fn vix_cpu_executor_threads() -> usize {
    vix_role_cpu_capacity().saturating_sub(2).max(1)
}

/// Threads of one compaction merge (`ZO_VIX_MERGE_THREAD_NUM`; `0` = auto).
/// Drives the term-dictionary merge partitioning, the per-input decode fan-out
/// and the blob encode pools. Auto = the machine's available parallelism
/// divided by the co-located CPU-heavy role count, so a combined node's merge
/// pool does not stack on top of its ingest/query pools. The per-merge auto
/// pool is capped at eight so the compactor's across-merge CPU gate can fill
/// a larger machine with several bounded merges instead of one machine-wide
/// nested pool (see [`cluster::cpu_role_divisor`]).
fn merge_threads() -> usize {
    let configured = get_config().common.vix_merge_thread_num;
    let role_capacity = vix_role_cpu_capacity();
    if configured != 0 {
        return configured.clamp(1, role_capacity);
    }
    role_capacity.min(8)
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

/// Move-job producer: merge the WAL batches behind `tables` (same table
/// providers the parquet path builds) into ONE core `.vix` file.
///
/// `schema` is the merged (shared-fields) schema of the input files; the
/// plan output may include `_original`, which is split into the writer's
/// dedicated docs column instead of being treated as a field.
/// `store_original` is the stream's `store_original_data` setting — it is
/// also force-enabled when the inputs already carry an `_original` column,
/// so a mid-hour settings flip never drops captured data.
///
/// EVERY plan field materializes as a docs column (v2 all-present-columns,
/// DESIGN §2) — under narrow-schema WAL batches the plan schema is the
/// union of PRESENT fields, so files stay hundreds of columns wide, never
/// registry-wide. `stream_type` resolves the BUILD-path index policy
/// ([`vix_build_index_enabled`]): #40 stream types (metrics by default)
/// and #42 L0-mode stream types build a column-store-only file. #42 files
/// re-index when compaction merges them (merge plans resolve
/// [`vix_index_enabled`]).
#[allow(clippy::too_many_arguments)]
pub async fn write_core_file_from_tables(
    trace_id: &str,
    stream_type: StreamType,
    schema: Arc<Schema>,
    tables: Vec<Arc<dyn TableProvider>>,
    fts_fields: &[String],
    bloom_fields: &[String],
    store_original: bool,
    input_original_bytes: usize,
) -> Result<CoreFileResult, anyhow::Error> {
    write_core_file_from_tables_with_caps(
        trace_id,
        stream_type,
        schema,
        tables,
        fts_fields,
        bloom_fields,
        store_original,
        input_original_bytes,
        BatchCaps::default(),
    )
    .await
}

/// [`write_core_file_from_tables`] with explicit batch caps (tests shrink
/// them to prove the byte-bounded chunked flow with small data).
#[allow(clippy::too_many_arguments)]
async fn write_core_file_from_tables_with_caps(
    trace_id: &str,
    stream_type: StreamType,
    schema: Arc<Schema>,
    tables: Vec<Arc<dyn TableProvider>>,
    fts_fields: &[String],
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
    let (tx, rx) = tokio::sync::mpsc::channel::<RecordBatch>(2);
    let read_task = tokio::task::spawn(async move {
        while let Some(batch) = futures::TryStreamExt::try_next(&mut batch_stream).await? {
            if tx.send(batch).await.is_err() {
                break; // builder exited (error on its side); stop reading
            }
        }
        Ok::<(), anyhow::Error>(())
    });

    let opts = single_file_build_opts(
        stream_type,
        fts_fields,
        bloom_fields,
        &caps,
        input_original_bytes,
    );
    let store_original =
        store_original || plan_schema.field_with_name(ORIGINAL_DATA_COL_NAME).is_ok();

    let builder = spawn_core_file_builder(rx, plan_schema, opts, store_original, caps, None);

    read_task.await??;
    builder.await?
}

/// The single-file build options every one-file core build shares (the WAL
/// mover's DataFusion path and the L0 builder's direct sorted-batch path):
/// v2 all-present-columns (DESIGN §2 — EVERY plan field becomes a docs
/// column; under narrow-schema WAL batches the plan schema is the union of
/// PRESENT fields, so files stay hundreds of columns wide, not
/// registry-wide), the BUILD-path index policy (#40/#42 stream types build
/// column-store-only), test-cap overrides, and the output spool decision
/// (big builds spool the finished container to the WAL volume — the
/// buffered container plus its upload clone was the ingester's OOM vector).
fn single_file_build_opts(
    stream_type: StreamType,
    fts_fields: &[String],
    bloom_fields: &[String],
    caps: &BatchCaps,
    input_original_bytes: usize,
) -> VixWriterOptions {
    let index_enabled = caps
        .index_enabled_override
        .unwrap_or_else(|| vix_build_index_enabled(stream_type));
    let mut opts = core_writer_options(fts_fields, bloom_fields.to_vec(), index_enabled);
    if let Some((ratio, floor)) = caps.bloom_auto_override {
        opts.bloom_only_auto_ratio = ratio;
        opts.bloom_only_min_distinct = floor;
    }
    let spool_min = get_config().common.vix_move_spool_min_bytes;
    if spool_min > 0 && input_original_bytes >= spool_min {
        opts.output_spool_dir =
            Some(std::path::Path::new(&get_config().common.data_wal_dir).join("vix_spool"));
    }
    opts
}

/// The shared single-file builder loop, off the async runtime: drain `rx`
/// record batches — from the mover's DataFusion plan or the L0 builder's
/// direct slicer, both delivering rows in the stored (`_timestamp` DESC)
/// order — through degenerate-`_timestamp` cleansing, byte-capped
/// splitting, `_source` synthesis and the indexed push path into ONE
/// finished core file. All CPU-heavy work — `_source` synthesis,
/// tokenizing, FST/postings/vortex encoding — stays on the blocking pool.
fn spawn_core_file_builder(
    mut rx: tokio::sync::mpsc::Receiver<RecordBatch>,
    plan_schema: SchemaRef,
    opts: VixWriterOptions,
    store_original: bool,
    caps: BatchCaps,
    auto_demote_expected_max_rows: Option<u64>,
) -> tokio::task::JoinHandle<Result<CoreFileResult, anyhow::Error>> {
    tokio::task::spawn_blocking(move || {
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
        if let Some(rows) = auto_demote_expected_max_rows {
            writer.set_expected_max_rows_for_auto_demotion(rows)?;
        }
        let mut docs_batches = 0usize;
        let mut dropped_rows = 0u64;
        let push_started = std::time::Instant::now();
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
            for part in split_batch_by_bytes(&batch, caps, true) {
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
        let push_wall_ms = u64::try_from(push_started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let (output, index, mut stats) = writer.finish_output()?;
        stats.timings.push_wall_ms = push_wall_ms;
        // spooled outputs stay on disk (upload from path); in-memory
        // outputs land in `data` as before
        let (data, output) = match output {
            vortex_index::VixOutput::Bytes(bytes) => (bytes, None),
            spooled => (Vec::new(), Some(spooled)),
        };
        Ok::<CoreFileResult, anyhow::Error>(CoreFileResult {
            data,
            output,
            index,
            stats,
            used_index_merge: false,
            docs_batches,
            dropped_rows,
        })
    })
}

/// M12 L0-build fast path: build ONE core file from a record batch whose
/// rows are ALREADY sorted `_timestamp` DESC — the L0 segment builder sorts
/// its (stream, hour) bucket itself, so the DataFusion `ORDER BY ... DESC`
/// plan [`write_core_file_from_tables`] runs contributed NOTHING here but
/// its sort: with `target_partitions` ≥ 2 that plan shape
/// (`RepartitionExec` feeding two `ExternalSorter`s under
/// `SortPreservingMergeExec`) starved the shared greedy memory pool on prod
/// fat-stream super-batches — RepartitionExec buffered ~3 GB it cannot
/// spill while one sorter held the rest, and the second sorter's FIRST
/// allocation failed with 0 bytes reserved (nothing of its own to spill).
/// This entry feeds the SAME builder loop directly in bounded slices: no
/// plan, no repartition, no sort, no memory-pool interaction at all.
///
/// The DESC contract is VERIFIED (one O(rows) pass), not trusted — an
/// unsorted push would silently store a file violating the v2 row-order
/// contract.
pub async fn write_core_file_from_sorted_batch(
    trace_id: &str,
    stream_type: StreamType,
    batch: RecordBatch,
    fts_fields: &[String],
    bloom_fields: &[String],
    store_original: bool,
    input_original_bytes: usize,
) -> Result<CoreFileResult, anyhow::Error> {
    write_core_file_from_sorted_batch_with_caps(
        trace_id,
        stream_type,
        batch,
        fts_fields,
        bloom_fields,
        store_original,
        input_original_bytes,
        BatchCaps::default(),
    )
    .await
}

/// [`write_core_file_from_sorted_batch`] with explicit batch caps (tests
/// shrink them to prove the byte-bounded chunked flow with small data).
#[allow(clippy::too_many_arguments)]
async fn write_core_file_from_sorted_batch_with_caps(
    trace_id: &str,
    stream_type: StreamType,
    batch: RecordBatch,
    fts_fields: &[String],
    bloom_fields: &[String],
    store_original: bool,
    input_original_bytes: usize,
    caps: BatchCaps,
) -> Result<CoreFileResult, anyhow::Error> {
    // verify the caller's DESC contract before anything is built
    let timestamps =
        as_int64_array(batch.column_by_name(TIMESTAMP_COL_NAME).ok_or_else(|| {
            anyhow::anyhow!(
                "[trace_id {trace_id}] sorted-batch build is missing {TIMESTAMP_COL_NAME:?}"
            )
        })?)?;
    let values = timestamps.values();
    if values.windows(2).any(|pair| pair[0] < pair[1]) {
        return Err(anyhow::anyhow!(
            "[trace_id {trace_id}] sorted-batch build: rows are not sorted {TIMESTAMP_COL_NAME} \
             DESC (caller contract) — refusing to store an out-of-order file"
        ));
    }

    let plan_schema = batch.schema();
    let opts = single_file_build_opts(
        stream_type,
        fts_fields,
        bloom_fields,
        &caps,
        input_original_bytes,
    );
    let store_original =
        store_original || plan_schema.field_with_name(ORIGINAL_DATA_COL_NAME).is_ok();

    // Producer: zero-copy row slices in stored order, mirroring the
    // DataFusion stream's batch granularity so the builder's byte-capped
    // splitting and `_source` synthesis see the same shapes. The bounded
    // channel keeps at most a couple of slices' derived state in flight.
    const SLICE_ROWS: usize = 8192;
    let (tx, rx) = tokio::sync::mpsc::channel::<RecordBatch>(2);
    let rows = batch.num_rows();
    let producer_batch = batch.clone();
    let read_task = tokio::task::spawn(async move {
        let mut offset = 0usize;
        while offset < rows {
            let len = SLICE_ROWS.min(rows - offset);
            if tx.send(producer_batch.slice(offset, len)).await.is_err() {
                break; // builder exited (error on its side); stop feeding
            }
            offset += len;
        }
        Ok::<(), anyhow::Error>(())
    });

    let builder = spawn_core_file_builder(
        rx,
        plan_schema,
        opts,
        store_original,
        caps,
        Some(rows as u64),
    );
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
    Fixed(&'a dyn Array, usize),
    Opaque(&'a dyn Array),
}

impl<'a> VarBytes<'a> {
    fn new(array: &'a dyn Array) -> Self {
        let any = array.as_any();
        match array.data_type() {
            DataType::Utf8 => any.downcast_ref().map_or(Self::Fixed(array, 8), Self::Utf8),
            DataType::LargeUtf8 => any
                .downcast_ref()
                .map_or(Self::Fixed(array, 8), Self::LargeUtf8),
            DataType::Utf8View => any
                .downcast_ref()
                .map_or(Self::Fixed(array, 8), Self::Utf8View),
            DataType::Binary => any
                .downcast_ref()
                .map_or(Self::Fixed(array, 8), Self::Binary),
            DataType::LargeBinary => any
                .downcast_ref()
                .map_or(Self::Fixed(array, 8), Self::LargeBinary),
            DataType::BinaryView => any
                .downcast_ref()
                .map_or(Self::Fixed(array, 8), Self::BinaryView),
            DataType::Null => Self::Fixed(array, 0),
            DataType::Boolean => Self::Fixed(array, 1),
            other if other.primitive_width().is_some() => {
                Self::Fixed(array, other.primitive_width().unwrap_or(8))
            }
            _ => Self::Opaque(array),
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
            Self::Fixed(array, width) => valid(*array, row, *width),
            Self::Opaque(array) => valid(*array, row, usize::MAX),
        }
    }

    /// Bytes needed for this scalar in Arrow JSON. String families use the
    /// exact JSON escaping expansion; binary gets a conservative 6x bound;
    /// fixed-width term-eligible scalars fit in 32 textual bytes.
    fn json_len_bound(&self, row: usize) -> usize {
        match self {
            Self::Utf8(array) => array
                .is_valid(row)
                .then(|| json_string_len(array.value(row).as_bytes()))
                .unwrap_or(0),
            Self::LargeUtf8(array) => array
                .is_valid(row)
                .then(|| json_string_len(array.value(row).as_bytes()))
                .unwrap_or(0),
            Self::Utf8View(array) => array
                .is_valid(row)
                .then(|| json_string_len(array.value(row).as_bytes()))
                .unwrap_or(0),
            Self::Binary(array) => array
                .is_valid(row)
                .then(|| array.value(row).len().saturating_mul(6).saturating_add(2))
                .unwrap_or(0),
            Self::LargeBinary(array) => array
                .is_valid(row)
                .then(|| array.value(row).len().saturating_mul(6).saturating_add(2))
                .unwrap_or(0),
            Self::BinaryView(array) => array
                .is_valid(row)
                .then(|| array.value(row).len().saturating_mul(6).saturating_add(2))
                .unwrap_or(0),
            Self::Fixed(array, _) => {
                if array.is_valid(row) {
                    128
                } else {
                    0
                }
            }
            // Nested/dictionary values do not have a cheap exact per-row
            // memory/JSON bound. Force them into the documented one-row
            // oversize exception rather than weakening the aggregate cap.
            Self::Opaque(array) => {
                if array.is_valid(row) {
                    usize::MAX
                } else {
                    0
                }
            }
        }
    }
}

/// Exact byte length of a valid UTF-8 string after JSON quoting/escaping.
fn json_string_len(value: &[u8]) -> usize {
    value.iter().fold(2usize, |bytes, byte| {
        bytes.saturating_add(match byte {
            b'"' | b'\\' | b'\x08' | b'\t' | b'\n' | b'\x0c' | b'\r' => 2,
            0x00..=0x1f => 6,
            _ => 1,
        })
    })
}

/// Conservative serialized length of one `_source` row. Value escaping is
/// exact for string families; key/punctuation bytes are included explicitly.
fn source_json_row_len_bound(
    fields: &[Arc<Field>],
    accessors: &[VarBytes<'_>],
    row: usize,
) -> usize {
    fields
        .iter()
        .zip(accessors)
        .filter(|(field, _)| {
            !matches!(
                field.name().as_str(),
                ID_COL_NAME | ORIGINAL_DATA_COL_NAME | SOURCE_COL_NAME
            )
        })
        .fold(2usize, |bytes, (field, accessor)| {
            let value_bytes = accessor.json_len_bound(row);
            if value_bytes == 0 {
                bytes
            } else {
                bytes
                    .saturating_add(json_string_len(field.name().as_bytes()))
                    .saturating_add(1) // ':'
                    .saturating_add(value_bytes)
                    .saturating_add(1) // conservative trailing ','
            }
        })
}

/// Split `batch` into consecutive row slices (zero-copy), each within the
/// logical work caps. When `synthesize_source` is true, the charge also
/// includes twice the bounded `_source` JSON length: the serializer briefly
/// owns both its line buffer and the copied [`StringArray`] values. Ordinary
/// source-preserving scans skip that expensive sizing pass. A single row over
/// the byte budget still forms its own slice.
fn split_batch_by_bytes(
    batch: &RecordBatch,
    caps: BatchCaps,
    synthesize_source: bool,
) -> Vec<RecordBatch> {
    let rows = batch.num_rows();
    if rows <= 1 {
        return vec![batch.clone()];
    }
    let accessors: Vec<VarBytes> = batch
        .columns()
        .iter()
        .map(|column| VarBytes::new(column.as_ref()))
        .collect();
    let fields = batch.schema().fields().to_vec();
    let fixed_per_row = 24usize.saturating_mul(batch.num_columns());
    let mut parts = Vec::new();
    let mut start = 0usize;
    let mut part_rows = 0usize;
    let mut part_bytes = 0usize;
    for row in 0..rows {
        let resident = accessors.iter().fold(0usize, |bytes, accessor| {
            bytes.saturating_add(accessor.get(row))
        });
        let source_peak = synthesize_source
            .then(|| source_json_row_len_bound(&fields, &accessors, row).saturating_mul(2))
            .unwrap_or(0);
        let row_bytes = fixed_per_row
            .saturating_add(resident)
            .saturating_add(source_peak);
        let next_rows = part_rows.saturating_add(1);
        let next_bytes = part_bytes.saturating_add(row_bytes);
        if part_rows > 0 && (next_rows > caps.rows.max(1) || next_bytes > caps.bytes.max(1)) {
            parts.push(batch.slice(start, part_rows));
            start = row;
            part_rows = 0;
            part_bytes = 0;
        }
        part_rows += 1;
        part_bytes = part_bytes.saturating_add(row_bytes);
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

    /// Physical row order of the stored rows (#51c-c, `row_order` property;
    /// missing == sorted). A [`vortex_index::RowOrder::Concat`] input is NOT
    /// globally `_timestamp` DESC: the k-way merge order is meaningless
    /// over it, so such inputs are exempt from the DESC input guard and
    /// FORCE the concatenation-order merge.
    fn row_order(&self) -> vortex_index::RowOrder {
        match self {
            MergeSource::Indexed(reader) => reader.row_order(),
            MergeSource::DocsOnly(docs) => docs.row_order(),
        }
    }

    fn docs_schema(&self) -> Result<SchemaRef, anyhow::Error> {
        match self {
            MergeSource::Indexed(reader) => reader.docs_schema(),
            MergeSource::DocsOnly(docs) => Ok(docs.schema().clone()),
        }
    }

    /// §4: whether the input asserts the all-present-columns invariant
    /// (`columns_complete` property) — ANDed into the merge output's own
    /// assertion by [`build_merge_plan`].
    fn columns_complete(&self) -> bool {
        match self {
            MergeSource::Indexed(reader) => reader.columns_complete(),
            MergeSource::DocsOnly(docs) => docs.columns_complete(),
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
/// One merge input: `(object key, data-object source, index-sidecar
/// source)`. The sidecar source is `Some` iff the file_list row carries
/// `index_size > 0` (v3 split: the index lives in a separate `.vxi`
/// object; a data file with none merges through the docs-only rebuild).
pub type MergeInput = (
    String,
    Arc<dyn vortex_index::VixRangeSource>,
    Option<Arc<dyn vortex_index::VixRangeSource>>,
);

/// The shared shape of one core-file merge, derived from the inputs and the
/// current stream settings before either merge strategy runs.
#[derive(Clone)]
struct MergePlan {
    store_original: bool,
    /// Preserved docs columns with their target types.
    preserved: Vec<(String, DataType)>,
    writer_schema: SchemaRef,
    opts: VixWriterOptions,
    /// Row/byte bounds of every staged docs batch.
    caps: BatchCaps,
    /// M31: project `_source` into the decode scan. `true` everywhere except
    /// the one shape that provably never reads it — the #46 column-derived
    /// heal-passthrough scan with EVERY input spliced (docs copied encoded,
    /// terms derived from columns): there the projected `_source` array
    /// reached the writer only for a length/null check, i.e. the single
    /// fattest column of every input was decoded for nothing (M17 measured
    /// the derivation scan at 113.6s of a 133.8s gen-1 merge — `_source`
    /// decode is a large share of it). The scan substitutes a synthesized
    /// empty-string array to keep the push contract intact.
    scan_source: bool,
    /// #46: every input is readable and ALL-COLUMNAR with compatible
    /// term-derivable types. Legacy enables this for index-off inputs; the
    /// opt-in fixed latest-schema migration also enables it for indexed
    /// inputs that need their representation normalized. The rebuild derives
    /// terms from streamed COLUMNS instead of parsing `_source` JSON per row
    /// (measured 5.4× dict-merge cost).
    /// Any gate miss keeps the source-driven derivation. The preserved
    /// union IS the derivation column set (v2 all-columns) — no extra
    /// streamed columns exist.
    derive_from_columns: bool,
    /// The fixed latest-schema policy admitted at least one physical-type to
    /// string-family cast. The rebuild must therefore synthesize `_source`
    /// from the normalized complete columns as well as deriving terms from
    /// them. This makes the representation durable: a later source-driven
    /// heal or an older/legacy compactor sees the same string values and
    /// cannot silently revert their term/token semantics.
    rewrite_source_from_columns: bool,
    cancellation: Option<VixMergeCancellation>,
}

impl MergePlan {
    #[inline]
    fn check_cancel(&self, boundary: &str) -> Result<(), anyhow::Error> {
        match &self.cancellation {
            Some(cancellation) => cancellation.check(boundary),
            None => Ok(()),
        }
    }

    #[inline]
    fn check_cancel_context(
        &self,
        boundary: &str,
        context: impl std::fmt::Display,
    ) -> Result<(), anyhow::Error> {
        match &self.cancellation {
            Some(cancellation) => cancellation.check_context(boundary, context),
            None => Ok(()),
        }
    }
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
/// Strategy requested by the compactor's first CPU phase.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoreMergeMode {
    /// Use the indexed merge when possible and return a prepared rebuild on
    /// an incompatible or docs-only input.
    Automatic,
    /// Refuse every rebuild fallback. Used by batches above the safe rebuild
    /// size ceiling.
    IndexedOnly,
    /// Produce a column-store-only intermediate; its final index is built by
    /// a later merge or terminal heal.
    IndexDeferred,
}

/// The first CPU phase either finishes the merge or returns owned state for a
/// separately admitted rebuild. No indexed child work or partial writer
/// survives in `NeedsRebuild`.
pub enum CoreMergeAttempt {
    Complete(MergedCoreFile),
    NeedsRebuild(PreparedCoreRebuild),
}

/// Open inputs and a completed merge plan retained across rebuild-memory
/// admission. All fields are private so execution can only resume through
/// [`execute_prepared_core_rebuild`].
pub struct PreparedCoreRebuild {
    inputs: Vec<MergeInput>,
    sources: Vec<MergeSource>,
    plan: MergePlan,
    requires_memory_admission: bool,
}

impl PreparedCoreRebuild {
    /// Whether this continuation builds a term index and therefore owns the
    /// memory-heavy rebuild footprint.
    pub fn requires_memory_admission(&self) -> bool {
        self.requires_memory_admission
    }
}
enum InternalCoreMergeAttempt {
    Complete(MergedCoreFile),
    NeedsRebuild {
        sources: Vec<MergeSource>,
        plan: MergePlan,
    },
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
/// Preserved docs columns (v2 all-present-columns, DESIGN §2/§6): the UNION
/// of the inputs' docs columns — never the registry. Column types follow
/// the current stream schema when it types the field, else the first input
/// that has the column; an input lacking a column contributes NULLS for its
/// rows (the column was absent from those records — `_source` still carries
/// whatever each record really held, and the scan-side `json_get(_source)`
/// fallback keeps serving fields absent from a file's columns).
/// `_original` is preserved whenever any input carries it.
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
///
/// `stream_type` resolves the index policy (#40): under an index-off plan
/// the term dictionaries are never touched — docs rows stream through
/// unchanged (disjoint concat or windowed interleave) and the output is
/// column-store only, whatever mix of indexed/index-off inputs arrives.
pub fn merge_core_files(
    stream_type: StreamType,
    inputs: &[MergeInput],
    latest_schema: &Schema,
    fts_fields: &[String],
    bloom_fields: &[String],
) -> Result<MergedCoreFile, anyhow::Error> {
    merge_core_files_with_caps(
        stream_type,
        inputs,
        latest_schema,
        fts_fields,
        bloom_fields,
        BatchCaps::default(),
    )
}

/// [`merge_core_files`] with cooperative cancellation. The token may be
/// cancelled from another thread; the merge returns a contextual
/// cancellation error at its next bounded orchestration boundary.
pub fn merge_core_files_with_cancellation(
    stream_type: StreamType,
    inputs: &[MergeInput],
    latest_schema: &Schema,
    fts_fields: &[String],
    bloom_fields: &[String],
    cancellation: &VixMergeCancellation,
) -> Result<MergedCoreFile, anyhow::Error> {
    merge_core_files_with_caps_and_cancellation(
        stream_type,
        inputs,
        latest_schema,
        fts_fields,
        bloom_fields,
        BatchCaps::default(),
        Some(cancellation.clone()),
        false,
    )
}
/// [`merge_core_files_with_cancellation`] without the full-rebuild fallback.
///
/// Large indexed-core compactions use this entry point: falling back after
/// planning a multi-gigabyte index merge could multiply peak memory. An
/// incompatible or damaged input fails the job with the precise fast-path
/// rejection instead; ordinary rebuild-sized batches keep using
/// [`merge_core_files_with_cancellation`] and can heal through a rebuild.
pub fn merge_core_files_indexed_only_with_cancellation(
    stream_type: StreamType,
    inputs: &[MergeInput],
    latest_schema: &Schema,
    fts_fields: &[String],
    bloom_fields: &[String],
    cancellation: &VixMergeCancellation,
) -> Result<MergedCoreFile, anyhow::Error> {
    merge_core_files_with_caps_and_cancellation(
        stream_type,
        inputs,
        latest_schema,
        fts_fields,
        bloom_fields,
        BatchCaps::default(),
        Some(cancellation.clone()),
        true,
    )
}

/// M31: [`merge_core_files`] with the index build DEFERRED — the output is
/// COLUMN-STORE-ONLY (`index=None`, `index_size` 0), the copy-shape merge:
/// no dictionary/postings/bloom work, no rebuild-gate admission. For
/// non-final merge groups whose output will provably be merged again (the
/// compactor's `ZO_VIX_MERGE_INDEX_DEFER_BELOW_MB` policy); the index is
/// built once, at the group that crosses the line (or by the single-file
/// heal on a terminal leftover).
pub fn merge_core_files_index_deferred(
    stream_type: StreamType,
    inputs: &[MergeInput],
    latest_schema: &Schema,
    fts_fields: &[String],
    bloom_fields: &[String],
) -> Result<MergedCoreFile, anyhow::Error> {
    merge_core_files_with_caps(
        stream_type,
        inputs,
        latest_schema,
        fts_fields,
        bloom_fields,
        BatchCaps {
            index_enabled_override: Some(false),
            ..BatchCaps::default()
        },
    )
}

/// [`merge_core_files_index_deferred`] with cooperative cancellation.
pub fn merge_core_files_index_deferred_with_cancellation(
    stream_type: StreamType,
    inputs: &[MergeInput],
    latest_schema: &Schema,
    fts_fields: &[String],
    bloom_fields: &[String],
    cancellation: &VixMergeCancellation,
) -> Result<MergedCoreFile, anyhow::Error> {
    merge_core_files_with_caps_and_cancellation(
        stream_type,
        inputs,
        latest_schema,
        fts_fields,
        bloom_fields,
        BatchCaps {
            index_enabled_override: Some(false),
            ..BatchCaps::default()
        },
        Some(cancellation.clone()),
        false,
    )
}
/// Run the compactor's planning/indexed phase while owning all inputs. A
/// fallback returns a continuation instead of entering rebuild admission
/// while the caller still owns CPU capacity.
#[allow(clippy::too_many_arguments)]
pub fn try_merge_core_files_with_cancellation(
    stream_type: StreamType,
    inputs: Vec<MergeInput>,
    latest_schema: Arc<Schema>,
    fts_fields: Vec<String>,
    bloom_fields: Vec<String>,
    cancellation: VixMergeCancellation,
    mode: CoreMergeMode,
) -> Result<CoreMergeAttempt, anyhow::Error> {
    let (caps, require_indexed_merge) = match mode {
        CoreMergeMode::Automatic => (BatchCaps::default(), false),
        CoreMergeMode::IndexedOnly => (BatchCaps::default(), true),
        CoreMergeMode::IndexDeferred => (
            BatchCaps {
                index_enabled_override: Some(false),
                ..BatchCaps::default()
            },
            false,
        ),
    };
    match attempt_core_merge(
        stream_type,
        &inputs,
        latest_schema.as_ref(),
        &fts_fields,
        &bloom_fields,
        caps,
        Some(cancellation),
        require_indexed_merge,
    )? {
        InternalCoreMergeAttempt::Complete(result) => Ok(CoreMergeAttempt::Complete(result)),
        InternalCoreMergeAttempt::NeedsRebuild { sources, plan } => {
            let requires_memory_admission = plan.opts.index_enabled;
            Ok(CoreMergeAttempt::NeedsRebuild(PreparedCoreRebuild {
                inputs,
                sources,
                plan,
                requires_memory_admission,
            }))
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn attempt_core_merge(
    stream_type: StreamType,
    inputs: &[MergeInput],
    latest_schema: &Schema,
    fts_fields: &[String],
    bloom_fields: &[String],
    caps: BatchCaps,
    cancellation: Option<VixMergeCancellation>,
    require_indexed_merge: bool,
) -> Result<InternalCoreMergeAttempt, anyhow::Error> {
    let started = std::time::Instant::now();
    let sources = open_merge_sources(inputs, cancellation.as_ref())?;
    log::debug!(
        "vix merge: opened {} inputs in {:?}",
        sources.len(),
        started.elapsed()
    );
    let mut plan = build_merge_plan(
        stream_type,
        &sources,
        latest_schema,
        fts_fields,
        bloom_fields,
        caps,
    );
    plan.cancellation = cancellation;
    plan.check_cancel("post-plan")?;

    let readers: Option<Vec<&VixReader>> = sources
        .iter()
        .map(|source| match source {
            MergeSource::Indexed(reader) => Some(reader.as_ref()),
            MergeSource::DocsOnly(_) => None,
        })
        .collect();
    if plan.rewrite_source_from_columns {
        if require_indexed_merge {
            return Err(anyhow::anyhow!(
                "required indexed merge is not applicable: latest-schema casts must rewrite \
                 _source together with docs"
            ));
        }
        log::info!(
            "vix merge: rebuilding to persist latest-schema string casts in docs and _source"
        );
    } else if let Some(readers) = readers {
        match merge_core_files_indexed(inputs, &sources, &readers, &plan) {
            Ok(result) => return Ok(InternalCoreMergeAttempt::Complete(result)),
            Err(IndexedMergeFailure::Fatal(error)) => return Err(error),
            Err(IndexedMergeFailure::Fallback(reason)) => {
                if require_indexed_merge {
                    return Err(reason.context(
                        "required indexed merge is not applicable; refusing a large full rebuild",
                    ));
                }
                log::warn!(
                    "merge_core_files: index merge not applicable, rebuilding terms from \
                     _source: {reason:#}"
                );
            }
        }
    } else if require_indexed_merge {
        return Err(anyhow::anyhow!(
            "required indexed merge is not applicable: one or more inputs has no readable index \
             sidecar; refusing a large full rebuild"
        ));
    }
    Ok(InternalCoreMergeAttempt::NeedsRebuild { sources, plan })
}

/// [`merge_core_files`] with explicit batch caps (tests shrink them to prove
/// the chunked flow with small data).
fn merge_core_files_with_caps(
    stream_type: StreamType,
    inputs: &[MergeInput],
    latest_schema: &Schema,
    fts_fields: &[String],
    bloom_fields: &[String],
    caps: BatchCaps,
) -> Result<MergedCoreFile, anyhow::Error> {
    merge_core_files_with_caps_and_cancellation(
        stream_type,
        inputs,
        latest_schema,
        fts_fields,
        bloom_fields,
        caps,
        None,
        false,
    )
}

fn merge_core_files_with_caps_and_cancellation(
    stream_type: StreamType,
    inputs: &[MergeInput],
    latest_schema: &Schema,
    fts_fields: &[String],
    bloom_fields: &[String],
    caps: BatchCaps,
    cancellation: Option<VixMergeCancellation>,
    require_indexed_merge: bool,
) -> Result<MergedCoreFile, anyhow::Error> {
    match attempt_core_merge(
        stream_type,
        inputs,
        latest_schema,
        fts_fields,
        bloom_fields,
        caps,
        cancellation,
        require_indexed_merge,
    )? {
        InternalCoreMergeAttempt::Complete(result) => Ok(result),
        InternalCoreMergeAttempt::NeedsRebuild { sources, plan } => {
            rebuild_over_sources(inputs, &sources, &plan)
        }
    }
}

/// The full-rebuild merge: k-way row merge + terms re-derived from `_source`
/// with the *current* stream settings, exactly like a fresh build of the
/// merged rows. [`merge_core_files`] falls back to this when the index-merge
/// fast path does not apply; it is public as the reference implementation
/// (differential tests oracle).
///
/// Note the #51c HEAL docs-chunk passthrough lives INSIDE the rebuild
/// ([`rebuild_over_sources`]): with every input qualified, the terms still
/// rebuild from the decoded scan but the docs chunks copy verbatim (the
/// default). Only a qualification miss decodes + re-encodes the docs.
pub fn merge_core_files_rebuild(
    stream_type: StreamType,
    inputs: &[MergeInput],
    latest_schema: &Schema,
    fts_fields: &[String],
    bloom_fields: &[String],
) -> Result<MergedCoreFile, anyhow::Error> {
    merge_core_files_rebuild_with_caps(
        stream_type,
        inputs,
        latest_schema,
        fts_fields,
        bloom_fields,
        BatchCaps::default(),
    )
}

/// [`merge_core_files_rebuild`] with cooperative cancellation.
pub fn merge_core_files_rebuild_with_cancellation(
    stream_type: StreamType,
    inputs: &[MergeInput],
    latest_schema: &Schema,
    fts_fields: &[String],
    bloom_fields: &[String],
    cancellation: &VixMergeCancellation,
) -> Result<MergedCoreFile, anyhow::Error> {
    merge_core_files_rebuild_with_caps_and_cancellation(
        stream_type,
        inputs,
        latest_schema,
        fts_fields,
        bloom_fields,
        BatchCaps::default(),
        Some(cancellation.clone()),
    )
}
/// Run a known full rebuild after the caller has acquired memory admission.
/// Admission stays outside the blocking rebuild controller so a
/// memory-ineligible rebuild never enters execution.
pub fn merge_core_files_rebuild_admitted_with_cancellation(
    stream_type: StreamType,
    inputs: &[MergeInput],
    latest_schema: &Schema,
    fts_fields: &[String],
    bloom_fields: &[String],
    cancellation: &VixMergeCancellation,
    permit: VixRebuildPermit,
) -> Result<MergedCoreFile, anyhow::Error> {
    let _permit = permit;
    let sources = open_merge_sources(inputs, Some(cancellation))?;
    let mut plan = build_merge_plan(
        stream_type,
        &sources,
        latest_schema,
        fts_fields,
        bloom_fields,
        BatchCaps::default(),
    );
    plan.cancellation = Some(cancellation.clone());
    plan.check_cancel("post-plan")?;
    rebuild_over_sources_admitted(inputs, &sources, &plan)
}

/// Resume an automatic/deferred merge after its indexed phase has yielded.
/// Indexed rebuilds require an owned memory permit; index-deferred execution
/// has no term-map footprint and therefore requires none.
pub fn execute_prepared_core_rebuild(
    prepared: PreparedCoreRebuild,
    permit: Option<VixRebuildPermit>,
) -> Result<MergedCoreFile, anyhow::Error> {
    if prepared.requires_memory_admission && permit.is_none() {
        return Err(anyhow::anyhow!(
            "prepared indexed rebuild resumed without rebuild-memory admission"
        ));
    }
    let _permit = permit;
    rebuild_over_sources_admitted(&prepared.inputs, &prepared.sources, &prepared.plan)
}

/// [`merge_core_files_rebuild`] with explicit batch caps.
fn merge_core_files_rebuild_with_caps(
    stream_type: StreamType,
    inputs: &[MergeInput],
    latest_schema: &Schema,
    fts_fields: &[String],
    bloom_fields: &[String],
    caps: BatchCaps,
) -> Result<MergedCoreFile, anyhow::Error> {
    merge_core_files_rebuild_with_caps_and_cancellation(
        stream_type,
        inputs,
        latest_schema,
        fts_fields,
        bloom_fields,
        caps,
        None,
    )
}

fn merge_core_files_rebuild_with_caps_and_cancellation(
    stream_type: StreamType,
    inputs: &[MergeInput],
    latest_schema: &Schema,
    fts_fields: &[String],
    bloom_fields: &[String],
    caps: BatchCaps,
    cancellation: Option<VixMergeCancellation>,
) -> Result<MergedCoreFile, anyhow::Error> {
    let sources = open_merge_sources(inputs, cancellation.as_ref())?;
    let mut plan = build_merge_plan(
        stream_type,
        &sources,
        latest_schema,
        fts_fields,
        bloom_fields,
        caps,
    );
    plan.cancellation = cancellation;
    plan.check_cancel("post-plan")?;
    rebuild_over_sources(inputs, &sources, &plan)
}

/// Outcome of a sidecar-only heal attempt over ONE stored core file
/// ([`rebuild_core_file_sidecar`]).
pub enum SidecarHealOutcome {
    /// A fresh `.vxi` was built over the UNTOUCHED data object: upload it
    /// to the SAME sidecar key and update the existing row's `index_size`.
    Rebuilt {
        index: Vec<u8>,
        stats: VixWriterStats,
    },
    /// The current plan is index-off but the file carries a sidecar: the
    /// heal is metadata-only — delete the `.vxi`, set `index_size = 0`.
    /// (v2 all-columns files already materialize every present field as a
    /// docs column, so the index-off direction needs no docs rewrite
    /// either.)
    DropSidecar,
    /// This heal genuinely rewrites docs; route it to the whole-file
    /// rebuild (new data object + new row). The two arms today:
    /// degenerate-`_timestamp` cleansing, and an index scan whose oversize
    /// skips the untouched data object's allowance cannot cover.
    NeedsDocsRewrite(String),
}

impl std::fmt::Debug for SidecarHealOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rebuilt { index, stats } => f
                .debug_struct("Rebuilt")
                .field("index_len", &index.len())
                .field("stats", stats)
                .finish(),
            Self::DropSidecar => write!(f, "DropSidecar"),
            Self::NeedsDocsRewrite(reason) => {
                f.debug_tuple("NeedsDocsRewrite").field(reason).finish()
            }
        }
    }
}

/// M3 sidecar-only heal (DESIGN-V2 §5): rebuild ONLY the `.vxi` index of
/// one stored core file — terms re-derived from the docs with the CURRENT
/// stream settings (column-derived when the #46 gate holds, `_source`-
/// derived otherwise, exactly like the whole-file rebuild) — while the
/// data object stays byte-identical and keeps its key, so querier caches
/// of the docs bytes survive the heal by construction.
///
/// Doc-id contract: the index-only scan consumes the single input in its
/// STORED row order (a one-input concatenation — concat-order files
/// included), so doc ids equal stored positions; the writer's
/// [`VixWriter::finish_index_sidecar`] verifies the scan covered the data
/// object's row count exactly.
///
/// Falls out to [`SidecarHealOutcome::NeedsDocsRewrite`] when the file
/// needs what only a docs rewrite can do:
/// - degenerate-`_timestamp` rows must be CLEANSED (a sidecar cannot drop stored rows);
/// - the fresh index skipped oversize values in a field the data object's `oversize_skips`
///   allowance does not record (the allowance rides the data object and cannot be restamped) —
///   without it readers would read a bloom/dictionary miss on that field as authoritative absence.
///
/// M1-era stats-less data objects stay stats-less (the data object is
/// untouched by design); they converge at their first real merge.
///
/// Blocks on ranged fetches — call from a blocking thread.
pub fn rebuild_core_file_sidecar(
    stream_type: StreamType,
    input: &MergeInput,
    latest_schema: &Schema,
    fts_fields: &[String],
    bloom_fields: &[String],
) -> Result<SidecarHealOutcome, anyhow::Error> {
    rebuild_core_file_sidecar_with_caps_and_cancellation(
        stream_type,
        input,
        latest_schema,
        fts_fields,
        bloom_fields,
        BatchCaps::default(),
        None,
    )
}

/// Production sidecar build after rebuild-memory admission.
pub fn rebuild_core_file_sidecar_admitted_with_cancellation(
    stream_type: StreamType,
    input: &MergeInput,
    latest_schema: &Schema,
    fts_fields: &[String],
    bloom_fields: &[String],
    cancellation: &VixMergeCancellation,
    permit: VixRebuildPermit,
) -> Result<SidecarHealOutcome, anyhow::Error> {
    let _permit = permit;
    rebuild_core_file_sidecar_with_caps_and_cancellation(
        stream_type,
        input,
        latest_schema,
        fts_fields,
        bloom_fields,
        BatchCaps::default(),
        Some(cancellation.clone()),
    )
}

fn rebuild_core_file_sidecar_with_caps_and_cancellation(
    stream_type: StreamType,
    input: &MergeInput,
    latest_schema: &Schema,
    fts_fields: &[String],
    bloom_fields: &[String],
    caps: BatchCaps,
    cancellation: Option<VixMergeCancellation>,
) -> Result<SidecarHealOutcome, anyhow::Error> {
    let started = std::time::Instant::now();
    let inputs = std::slice::from_ref(input);
    let sources = open_merge_sources(inputs, cancellation.as_ref())?;
    let mut plan = build_merge_plan(
        stream_type,
        &sources,
        latest_schema,
        fts_fields,
        bloom_fields,
        caps,
    );
    plan.cancellation = cancellation;
    plan.check_cancel("sidecar post-plan")?;

    // Index-off plan: the heal direction is "drop the sidecar" — pure
    // metadata, nothing to scan.
    if !plan.opts.index_enabled {
        return Ok(SidecarHealOutcome::DropSidecar);
    }
    if plan.rewrite_source_from_columns {
        return Ok(SidecarHealOutcome::NeedsDocsRewrite(
            "latest-schema casts must rewrite _source together with docs; a sidecar-only heal \
             would leave non-repeatable term semantics"
                .to_string(),
        ));
    }

    // The data object's existing oversize allowance (a DATA-side property):
    // the new index may only skip within it. DocsOnly sources (unreadable
    // index) expose none here — conservative: any new skip falls back.
    let existing_allowance: FastHashSet<String> = match &sources[0] {
        MergeSource::Indexed(reader) => reader.oversize_skips().keys().cloned().collect(),
        MergeSource::DocsOnly(_) => Default::default(),
    };

    // Stored-order scan driver + the cleansing probe (fixed 8 bytes/row).
    let timestamps = read_timestamp_columns(inputs, &sources, plan.cancellation.as_ref())?;
    let dropped_rows = count_degenerate_ts_rows(&timestamps);
    if dropped_rows > 0 {
        return Ok(SidecarHealOutcome::NeedsDocsRewrite(format!(
            "{dropped_rows} degenerate-_timestamp row(s) must be cleansed, which a sidecar-only \
             heal cannot express"
        )));
    }
    let row_count = timestamps[0].len() as u64;
    if row_count != sources[0].row_count() {
        return Err(anyhow::anyhow!(
            "core file {} stores {} rows but its container property says {} — refusing the \
             sidecar heal over inconsistent metadata",
            input.0,
            row_count,
            sources[0].row_count()
        ));
    }

    // Index-only writer over the file's OWN stored order: a single input's
    // concatenation IS its stored order, `row_order=concat` files included
    // (the k-way merge order is never consulted).
    let mut writer_opts = plan.opts.clone();
    // enables the detached index-only pushes (#51c machinery); no docs
    // store is ever assembled and no data-object property is written
    writer_opts.docs_passthrough = true;
    let mut writer = VixWriter::new(&plan.writer_schema, writer_opts, plan.store_original);
    writer.set_expected_max_rows_for_auto_demotion(row_count)?;
    let runs = concat_runs(&[0], &timestamps);
    let mut scan_plan = plan.clone();
    if plan.derive_from_columns {
        // The detached column-derived index scan never consumes `_source`.
        scan_plan.scan_source = false;
    }
    let scan_windows = if plan.derive_from_columns {
        log::info!(
            "vix sidecar heal: deriving terms from {} columns (index-off input)",
            plan.preserved.len()
        );
        stream_concat_windows(inputs, &scan_plan, &runs, |ts, cs, source, original| {
            let batch = derivation_window_batch(&scan_plan, ts, cs)?;
            writer.push_batch_with_source_index_only(&batch, source, original)?;
            Ok(())
        })?
        .batches
    } else {
        stream_concat_windows(inputs, &scan_plan, &runs, |ts, cs, source, original| {
            writer.push_docs_rows_index_only(ts, cs, source, original)
        })?
        .batches
    };

    // Oversize coverage: a field the NEW index skipped values from must
    // already carry the data-side allowance — the untouched data object
    // cannot be restamped, and an unrecorded skip would turn index misses
    // on that field into wrong "definitely absent" answers.
    let new_skips: Vec<&String> = writer
        .oversize_skips()
        .keys()
        .filter(|field| !existing_allowance.contains(*field))
        .collect();
    if !new_skips.is_empty() {
        return Ok(SidecarHealOutcome::NeedsDocsRewrite(format!(
            "index scan skipped oversize value(s) in field(s) {new_skips:?} that the data \
             object's oversize allowance does not record — only a docs rewrite restamps it"
        )));
    }

    let (index, stats) = writer.finish_index_sidecar(row_count)?;
    log::debug!(
        "vix sidecar heal: rebuilt the index of {} over {row_count} rows in {scan_windows} \
         windows, sidecar {} bytes, took {:?}",
        input.0,
        index.len(),
        started.elapsed()
    );
    Ok(SidecarHealOutcome::Rebuilt { index, stats })
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
/// - (v2 all-columns removed the per-column probe: the plan's preserved set is the file's own
///   column union, so a column-capability gap vs settings can no longer exist.)
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
    stream_type: StreamType,
    key: &str,
    source: Arc<dyn VixRangeSource>,
    index_source: Option<Arc<dyn VixRangeSource>>,
    latest_schema: &Schema,
    fts_fields: &[String],
    bloom_fields: &[String],
) -> Result<CoreFileStatus, anyhow::Error> {
    let reader = match VixReader::open_ranged_with_index(Arc::clone(&source), index_source) {
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
        stream_type,
        &sources,
        latest_schema,
        fts_fields,
        bloom_fields,
        BatchCaps::default(),
    );
    let MergeSource::Indexed(reader) = &sources[0] else {
        unreachable!("constructed as Indexed above");
    };
    let reader = reader.as_ref();

    // Index-mode alignment first (#40): both drift directions rebuild —
    // the dictionary probes below are meaningless across modes (an
    // index-off writer rejects every dictionary merge, and an index-off
    // FILE cannot join an indexed plan's fast path).
    if plan.opts.index_enabled && !reader.has_index() {
        return Ok(CoreFileStatus::NeedsRebuild(
            "file carries no index sidecar but the stream policy indexes; a rebuild re-derives \
             every term from _source"
                .to_string(),
        ));
    }
    if !plan.opts.index_enabled && reader.has_index() {
        return Ok(CoreFileStatus::NeedsRebuild(
            "file carries a term index but the stream policy is index-off (column-store only); \
             a rebuild drops the index and materializes every field's docs column"
                .to_string(),
        ));
    }

    if plan.opts.index_enabled {
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
    }
    // v2 all-columns: the plan's preserved set IS this file's own column
    // union, so a per-column probe is vacuous — column capabilities can no
    // longer lag settings.
    Ok(CoreFileStatus::Current)
}

/// Open every input: full [`VixReader`]s normally, docs-only handles for
/// files whose index blobs are unreadable (logged; those merges rebuild).
fn open_merge_sources(
    inputs: &[MergeInput],
    cancellation: Option<&VixMergeCancellation>,
) -> Result<Vec<MergeSource>, anyhow::Error> {
    if inputs.is_empty() {
        return Err(anyhow::anyhow!("merge_core_files: no input files"));
    }
    inputs
        .iter()
        .map(|(key, data, index)| {
            if let Some(cancellation) = cancellation {
                cancellation.check_context("input-open before", key)?;
            }
            match VixReader::open_ranged_with_index(Arc::clone(data), index.clone()) {
                Ok(reader) => {
                    if let Some(cancellation) = cancellation {
                        cancellation.check_context("input-open after", key)?;
                    }
                    Ok(MergeSource::Indexed(Box::new(reader)))
                }
                Err(index_error) => match VixDocs::open_ranged(Arc::clone(data)) {
                    Ok(docs) => {
                        if let Some(cancellation) = cancellation {
                            cancellation.check_context("input-open after", key)?;
                        }
                        log::warn!(
                            "merge_core_files: core file {key} has an unreadable index \
                         ({index_error:#}); merging its docs and rebuilding terms from _source"
                        );
                        Ok(MergeSource::DocsOnly(docs))
                    }
                    Err(_) => Err(anyhow::anyhow!("open core file {key}: {index_error}")),
                },
            }
        })
        .collect()
}

/// Derive the merge shape shared by both strategies (see
/// [`merge_core_files`] for the rules). Inputs whose docs schema is
/// unreadable poison nothing here — their columns are simply not offered —
/// but such files fail later when their rows are read.
/// M31: type equivalence for the #46 derivation gate. Strict equality is the
/// rule — the gate's fear is a cast canonicalizing differently than the
/// `_source` derivation — but the arrow STRING representations (`Utf8` /
/// `LargeUtf8` / `Utf8View`) hold byte-identical logical values, and the
/// normalize cast between them is lossless, so terms derive identically.
/// Measured 2026-08-26 on prod L0s: 909/918 traces fields "mismatched" as
/// stored `Utf8View` vs registry `Utf8` — strict equality kept the WHOLE
/// FLEET on the 5.4x `_source` arm for a representation difference.
/// Numerics stay strict: Int64 vs Float64 genuinely canonicalize apart.
fn derivation_type_equivalent(a: &DataType, b: &DataType) -> bool {
    a == b || (string_family(a) && string_family(b))
}

fn string_family(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

/// Casts admitted by the opt-in fixed latest-schema policy. The source must
/// already be a scalar whose `_source` image is a JSON Boolean/number, and the
/// authoritative target must be a string representation. Reverse parsing,
/// numeric narrowing, and Boolean coercion are deliberately excluded: Arrow
/// has kernels for them, but they can silently null or change valid values.
fn latest_schema_derivation_cast_allowed(source: &DataType, target: &DataType) -> bool {
    derivation_type_equivalent(source, target)
        || (string_family(target)
            && matches!(
                source,
                DataType::Boolean
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
                    | DataType::Float16
                    | DataType::Float32
                    | DataType::Float64
            ))
}

fn build_merge_plan(
    stream_type: StreamType,
    sources: &[MergeSource],
    latest_schema: &Schema,
    fts_fields: &[String],
    bloom_fields: &[String],
    caps: BatchCaps,
) -> MergePlan {
    let merge_type_policy = caps
        .merge_type_policy_override
        .unwrap_or_else(MergeTypePolicy::configured);
    // M31: the caps override (production use: the compactor's index-defer
    // policy over non-final all-index-less groups) beats the stream-type
    // resolution, exactly like the build path's consult.
    let index_enabled = caps
        .index_enabled_override
        .unwrap_or_else(|| vix_index_enabled(stream_type));
    // docs columns available across inputs (name -> first stored type),
    // writer-managed columns excluded
    let mut available: Vec<(String, DataType)> = Vec::new();
    let mut available_index = FastHashMap::<String, usize>::default();
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
                    if !available_index.contains_key(name) {
                        available_index.insert(name.to_string(), available.len());
                        available.push((name.to_string(), field.data_type().clone()));
                    }
                }
            }
        }
    }

    // #52: the full bloom-only list (config + test seam + STICKY input
    // markers + merge-time AUTO from the inputs' dictionary block metas).
    // Purely an INDEX-side concept since v2 all-columns: demoted fields
    // lose dictionary/postings and keep bloom coverage — their docs
    // columns exist like every other field's, no column-store side effect
    // to manage.
    let bloom_only_names: Vec<String> = {
        let cfg = get_config();
        let mut names: Vec<String> = cfg
            .common
            .vix_bloom_only_fields
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();
        if let Some(list) = caps.bloom_only_override {
            names.extend(list.split(',').map(str::trim).map(str::to_string));
        }
        let (ratio, floor) = caps.bloom_auto_override.unwrap_or((
            cfg.common.vix_bloom_only_auto_ratio,
            cfg.common.vix_bloom_only_min_distinct,
        ));
        let merged_rows: u64 = sources
            .iter()
            .map(|source| match source {
                MergeSource::Indexed(reader) => reader.row_count(),
                _ => 0,
            })
            .sum();
        if index_enabled {
            // M7 STICKY demotion: a field ANY input already marks
            // bloom-only stays bloom-only. Demoted inputs hold no
            // dictionary terms for it, so the count-driven AUTO below can
            // never re-derive the decision — without stickiness a second-
            // generation merge would degrade the field to capability-less
            // (bloom coverage lost) and the single-file sweep would
            // rebuild → re-demote → rebuild forever. Un-demotion is the
            // never-list (it wins at writer resolution) + the heal that
            // then re-derives the terms.
            for source in sources {
                if let MergeSource::Indexed(reader) = source {
                    names.extend(reader.bloom_only_fields().map(str::to_string));
                }
            }
        }
        if index_enabled && ratio > 0.0 && merged_rows > 0 {
            let mut counts = FastHashMap::<String, u64>::default();
            for source in sources {
                if let MergeSource::Indexed(reader) = source
                    && let Ok(per_field) = reader.term_counts_by_field()
                {
                    for (name, count) in per_field {
                        // M12: only STRING-family fields are bloom-only
                        // candidates — the writer's construction re-check
                        // enforces this anyway, but resolving (and INFO-
                        // logging) a numeric field here was a lie: a
                        // high-distinct numeric like `duration` logged
                        // "AUTO bloom-only demotion" on every merge while
                        // never actually leaving the dictionary (this
                        // artifact misdirected the M10 phase analysis).
                        let string_family = available.iter().any(|(n, stored)| {
                            n == &name
                                && matches!(
                                    stored,
                                    DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                                )
                        });
                        if !string_family {
                            continue;
                        }
                        *counts.entry(name).or_default() += count;
                    }
                }
            }
            // the shared #52 AUTO rule (one function, two call sites: here
            // over input-dictionary counts, and the writer's finish over
            // its own term map — see resolve_auto_bloom_only)
            let never: Vec<String> = cfg
                .common
                .vix_bloom_only_never
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect();
            names.extend(vortex_index::resolve_auto_bloom_only(
                counts
                    .iter()
                    .filter(|(name, _)| !names.contains(name))
                    .map(|(name, count)| (name.as_str(), *count)),
                merged_rows,
                ratio,
                floor,
                &never,
                "merge",
            ));
        }
        names.sort_unstable();
        names.dedup();
        names
    };

    // Preserved docs columns (v2 DESIGN §2/§6): the UNION of the inputs'
    // docs columns, never the registry. Types follow the current stream
    // schema when it types the field (normalize casts per chunk), else the
    // first-seen stored type. `_o2_id` rides along like any other stored
    // column (it is excluded from `_source`, so its docs column is its only
    // home). There is no derive-from-`_source` arm anymore: a column absent
    // from one input contributes NULLS for that input's rows.
    let preserved: Vec<(String, DataType)> = available
        .iter()
        .map(|(name, stored_type)| {
            let target_type = latest_schema
                .field_with_name(name)
                .map(|field| field.data_type().clone())
                .unwrap_or_else(|_| stored_type.clone());
            (name.clone(), target_type)
        })
        .collect();
    let preserved_index: FastHashMap<&str, &DataType> = preserved
        .iter()
        .map(|(name, data_type)| (name.as_str(), data_type))
        .collect();

    // writer construction schema: `_timestamp` + the preserved columns at
    // their target types. Since v3 files store EVERY present field as a
    // column, this union already covers every term-derivable field any
    // input carries — no dictionary/registry appendix arms.
    let mut writer_fields: Vec<Field> =
        vec![Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false)];
    for (name, data_type) in &preserved {
        writer_fields.push(Field::new(name, data_type.clone(), true));
    }
    let writer_schema = Arc::new(Schema::new(writer_fields));
    let mut opts = core_writer_options(fts_fields, bloom_fields.to_vec(), index_enabled);
    opts.encode_threads = merge_threads();
    // #51b: k-way range parallelism (0 = min(available parallelism, 8),
    // capped by the merge thread budget inside merge_indexes)
    opts.merge_kway_threads = get_config().common.vix_merge_kway_threads;
    if let Some((ratio, floor)) = caps.bloom_auto_override {
        opts.bloom_only_auto_ratio = ratio;
        opts.bloom_only_min_distinct = floor;
    }
    // §4 completeness propagation: the output asserts all-present-columns
    // only when EVERY input did — an incomplete input's `_source` rows may
    // carry fields that never became columns, and both the passthrough and
    // the decode merge preserve those rows verbatim.
    opts.columns_complete = sources.iter().all(MergeSource::columns_complete);
    // #52: hand the writer the resolved bloom-only list (writer-side
    // resolution re-filters to string-family ∩ term plan − never − fts).
    opts.bloom_only_field_names = bloom_only_names.clone();

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

    // #46 gate: an INDEXED output over complete all-columnar inputs derives
    // its terms from streamed COLUMNS (the cheap column-driven path) instead
    // of parsing `_source` per row. Legacy limits this to index-off inputs;
    // latest_schema also admits indexed inputs when normalizing their durable
    // representation.
    // Strict preconditions, any miss = the source-driven fallback:
    // readable index (index-off, not unreadable), every non-internal input
    // column term-derivable (string/numeric/bool — write-time JSON types by
    // construction), and no type flips — neither across inputs (`available`
    // holds the first-seen type) nor between an input's stored type and the
    // plan's target type (a cast could canonicalize differently than the
    // `_source` derivation).
    let all_index_off = sources.iter().all(|source| match source {
        MergeSource::Indexed(reader) => !reader.has_index(),
        MergeSource::DocsOnly(_) => false,
    });
    let mut derive_from_columns = index_enabled
        && !caps.force_source_derivation
        && !sources.is_empty()
        && (all_index_off || merge_type_policy == MergeTypePolicy::LatestSchema)
        && sources.iter().all(|source| match source {
            MergeSource::Indexed(reader) => reader.columns_complete(),
            MergeSource::DocsOnly(_) => false,
        });
    let mut rewrite_source_from_columns = false;
    if derive_from_columns {
        'gate: for source in sources {
            let Ok(schema) = source.docs_schema() else {
                derive_from_columns = false;
                log::info!(
                    "vix merge: column derivation off — an input's docs schema is unreadable"
                );
                break;
            };
            for field in schema.fields() {
                let name = field.name().as_str();
                if name == TIMESTAMP_COL_NAME
                    || name == SOURCE_COL_NAME
                    || name == ORIGINAL_DATA_COL_NAME
                {
                    continue;
                }
                let stored = available_index
                    .get(name)
                    .and_then(|index| available.get(*index));
                let target = preserved_index.get(name).copied();
                let type_ok =
                    vortex_index::is_value_indexed_type(field.data_type()) || name == ID_COL_NAME;
                let target_ok = target.is_some_and(|target_type| {
                    if merge_type_policy == MergeTypePolicy::LatestSchema
                        && latest_schema.field_with_name(name).is_ok()
                    {
                        latest_schema_derivation_cast_allowed(field.data_type(), target_type)
                    } else {
                        derivation_type_equivalent(field.data_type(), target_type)
                    }
                });
                let rewrites_source = merge_type_policy == MergeTypePolicy::LatestSchema
                    && latest_schema.field_with_name(name).is_ok()
                    && target.is_some_and(|target_type| {
                        !derivation_type_equivalent(field.data_type(), target_type)
                            && latest_schema_derivation_cast_allowed(field.data_type(), target_type)
                    });
                let first_seen_ok = merge_type_policy == MergeTypePolicy::LatestSchema
                    || stored.is_some_and(|(_, data_type)| {
                        derivation_type_equivalent(data_type, field.data_type())
                    });
                if !type_ok || !first_seen_ok || !target_ok {
                    derive_from_columns = false;
                    // M31: the reason line this gate always lacked — its
                    // absence hid a FLEET-WIDE silent miss (every prod L0
                    // stores strings as Utf8View while the registry says
                    // Utf8; the strict != above kept every rebuild on the
                    // 5.4x _source arm and nothing said why).
                    log::info!(
                        "vix merge: column derivation off — field {name:?} stored {:?} vs \
                         first-seen {:?} / target {:?} (value-indexed type: {type_ok})",
                        field.data_type(),
                        stored.map(|(_, t)| t),
                        target,
                    );
                    break 'gate;
                }
                rewrite_source_from_columns |= rewrites_source;
            }
        }
    }
    if !derive_from_columns {
        rewrite_source_from_columns = false;
    }
    if rewrite_source_from_columns {
        // The default writer samples 256 MiB before choosing docs chunking.
        // A fixed-type migration cannot passthrough old chunks (it rewrites
        // `_source`), so that sample remains resident alongside normalized
        // columns + synthesized source and dominated the benchmark's RSS.
        // 8 MiB still samples tens of thousands of rows at the measured
        // production-like density while starting the streaming encoder early;
        // it is scoped to this opt-in one-generation migration only.
        opts.docs_encode_sample_bytes = 8 * 1024 * 1024;
    }
    drop(preserved_index);

    MergePlan {
        store_original,
        preserved,
        writer_schema,
        opts,
        caps,
        scan_source: !rewrite_source_from_columns,
        derive_from_columns,
        rewrite_source_from_columns,
        cancellation: None,
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
    cancellation: Option<&VixMergeCancellation>,
) -> Result<Vec<Int64Array>, anyhow::Error> {
    sources
        .iter()
        .zip(inputs)
        .map(|(source, (key, ..))| {
            if let Some(cancellation) = cancellation {
                cancellation.check_context("timestamp read before", key)?;
            }
            let column = source
                .read_timestamp_column()
                .map_err(|e| anyhow::anyhow!("read core file {key}: {e}"))?;
            if let Some(cancellation) = cancellation {
                cancellation.check_context("timestamp read after", key)?;
            }
            let timestamps = as_int64_array(&column)?;
            if timestamps.null_count() > 0 {
                return Err(anyhow::anyhow!("core file {key} has null _timestamp rows"));
            }
            // #27 armor: the k-way merge — and the scan layer's declared
            // per-file ordering plus its first/last-row min/max stats — is
            // conditional on every input being internally `_timestamp`
            // DESC. Every writer upholds it but nothing records or checks
            // it; a violated input would silently corrupt the merged
            // order, the derived stats, and top-N candidate selection.
            // Reject loudly instead (same discipline as write_index_blobs
            // hard-rejecting out-of-order parts). Degenerate rows
            // (ts <= 0) are exempt: the merge cleanses them before order
            // matters, mirroring the mover's backstop.
            //
            // #51c-c exemption: a file stamped `row_order=concat` is
            // DECLARED not globally sorted — that is its contract, not
            // corruption. Such inputs never feed the k-way merge order
            // (the caller routes any merge containing one to the
            // concatenation-order path, which is order-free); the guard
            // stays armed for every sorted-declared file.
            if source.row_order().is_ts_desc() {
                let mut prev: Option<(usize, i64)> = None;
                for (row, &ts) in timestamps.values().iter().enumerate() {
                    if ts <= 0 {
                        continue;
                    }
                    if let Some((prev_row, prev_ts)) = prev
                        && ts > prev_ts
                    {
                        return Err(anyhow::anyhow!(
                            "core file {key} violates the _timestamp DESC row order at rows \
                             {prev_row}..={row} ({prev_ts} then {ts}): refusing to merge",
                        ));
                    }
                    prev = Some((row, ts));
                }
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

/// One consecutive input run in output doc-id order. A concat/disjoint merge
/// needs only one entry per input, rather than one `(input, row)` entry per
/// record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ConcatRun {
    input: usize,
    first_doc: u64,
    rows: usize,
}

enum RebuildRowOrder {
    Runs(Vec<ConcatRun>),
    Interleave(Vec<(usize, usize)>),
}

impl RebuildRowOrder {
    fn rows(&self) -> usize {
        match self {
            Self::Runs(runs) => runs.iter().map(|run| run.rows).sum(),
            Self::Interleave(order) => order.len(),
        }
    }
}

fn concat_runs(input_order: &[usize], timestamps: &[Int64Array]) -> Vec<ConcatRun> {
    let mut first_doc = 0u64;
    input_order
        .iter()
        .map(|&input| {
            let rows = timestamps[input].len();
            let run = ConcatRun {
                input,
                first_doc,
                rows,
            };
            first_doc = first_doc.saturating_add(rows as u64);
            run
        })
        .collect()
}

/// Detect the common strictly-disjoint timestamp shape without first paying
/// for the full k-way heap merge and its per-row doc-id maps. Inputs have
/// already passed the internal-DESC validation. Boundary-equal ranges stay on
/// the generic proof path because stable tie ordering can depend on input
/// position.
fn strict_disjoint_input_order(timestamps: &[Int64Array]) -> Option<Vec<usize>> {
    let mut non_empty: Vec<usize> = timestamps
        .iter()
        .enumerate()
        .filter_map(|(input, ts)| (!ts.is_empty()).then_some(input))
        .collect();
    non_empty.sort_unstable_by(|&a, &b| {
        timestamps[b]
            .value(0)
            .cmp(&timestamps[a].value(0))
            .then_with(|| a.cmp(&b))
    });
    if non_empty.windows(2).any(|pair| {
        let newer = &timestamps[pair[0]];
        let older = &timestamps[pair[1]];
        newer.value(newer.len() - 1) <= older.value(0)
    }) {
        return None;
    }
    let mut order = non_empty;
    order.extend(
        timestamps
            .iter()
            .enumerate()
            .filter_map(|(input, ts)| ts.is_empty().then_some(input)),
    );
    Some(order)
}

/// The key of the first #51c-c concat-order input, if any. Such an input is
/// NOT globally `_timestamp` DESC, so the k-way merge order ([`merge_order`])
/// is meaningless over the set: the merge always takes the
/// concatenation-order path (there is no machinery to re-sort a multi-GB
/// unsorted input, and proceeding sorted would corrupt the sorted-file
/// contract).
fn first_concat_order_input<'a>(
    inputs: &'a [MergeInput],
    sources: &[MergeSource],
) -> Option<&'a str> {
    sources
        .iter()
        .zip(inputs)
        .find(|(source, _)| !source.row_order().is_ts_desc())
        .map(|(_, (key, ..))| key.as_str())
}

/// #51c-c deterministic concatenation order over the merge inputs: sorted by
/// each input's minimum `_timestamp` DESCENDING (the newest-starting input
/// first — the nearest analogue of the storage convention and of the
/// disjoint path's offset order), ties broken by file key ascending. Inputs
/// with no rows (possible after cleansing on the rebuild path) sort last, by
/// key. Computed over the CLEANSED per-input timestamp columns, so the order
/// is a pure function of the rows the merge actually stores.
fn concat_input_order(inputs: &[MergeInput], timestamps: &[Int64Array]) -> Vec<usize> {
    let min_ts: Vec<Option<i64>> = timestamps.iter().map(arrow::compute::min).collect();
    let mut order: Vec<usize> = (0..inputs.len()).collect();
    order.sort_by(|&a, &b| {
        match (min_ts[a], min_ts[b]) {
            // larger minimum first; empty inputs (None) last
            (Some(ta), Some(tb)) => tb.cmp(&ta),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        }
        .then_with(|| inputs[a].0.cmp(&inputs[b].0))
    });
    order
}

/// Per-input contiguous run offsets of a concatenation merge (parallel to
/// `inputs`): the input at concat position `p` starts at the sum of the
/// earlier inputs' row counts — the exact [`DocIdMap::Offset`] shape the
/// disjoint fast path feeds the index merge. Errors when the total leaves
/// the u32 doc-id space (the writer would refuse such a file anyway; fail
/// before any index work).
fn concat_doc_id_offsets(
    input_order: &[usize],
    timestamps: &[Int64Array],
) -> Result<Vec<u32>, anyhow::Error> {
    let mut offsets = vec![0u32; timestamps.len()];
    let mut next = 0u64;
    for &input in input_order {
        offsets[input] = u32::try_from(next).map_err(|_| {
            anyhow::anyhow!(
                "concatenation merge exceeds the u32 doc-id space at input offset {next}"
            )
        })?;
        next += timestamps[input].len() as u64;
    }
    if next > u64::from(u32::MAX) {
        return Err(anyhow::anyhow!(
            "concatenation merge stores {next} rows, exceeding the u32 doc-id space"
        ));
    }
    Ok(offsets)
}

/// #51c-c: qualify the ENTIRE input set for a concatenation-order fast-path
/// merge and build its writer (docs passthrough + concat row order — the
/// encoder strategy is fixed at spawn, so the writer must be born concat).
/// Requires EVERY input to pass the per-input #51c qualification
/// (all-or-nothing): concatenation trades the sorted-file contract for the
/// chunk copy, so if any input would decode anyway the trade buys nothing —
/// the caller keeps today's sorted interleave (or, when a concat INPUT
/// forces concatenation regardless, falls back to the rebuild, whose forced
/// concatenation decodes unqualified inputs).
fn qualify_concat_fast_path(
    inputs: &[MergeInput],
    readers: &[&VixReader],
    plan: &MergePlan,
    timestamps: &[Int64Array],
) -> Result<(VixWriter, Vec<usize>), String> {
    let mut writer_opts = plan.opts.clone();
    writer_opts.docs_passthrough = true;
    writer_opts.concat_row_order = true;
    let writer = VixWriter::new(&plan.writer_schema, writer_opts, plan.store_original);
    for (index, reader) in readers.iter().enumerate() {
        if let Err(reason) =
            qualify_passthrough_input(reader, timestamps[index].len() as u64, &writer)
        {
            return Err(format!(
                "input {} does not qualify for the docs passthrough: {reason}",
                inputs[index].0
            ));
        }
    }
    Ok((writer, concat_input_order(inputs, timestamps)))
}

/// The index-merge fast path (see [`merge_core_files`]).
///
/// Under an index-off plan (#40, `plan.opts.index_enabled == false`) the
/// dictionary steps vanish — no input capability check, no index merge —
/// and this degenerates to the pure docs-stream copy (disjoint concat or
/// windowed interleave), reported with `used_index_merge: false`. The
/// degenerate-`_timestamp` fallback to the rebuild stays: cleansing needs
/// the rebuild's per-chunk row filtering either way.
fn merge_core_files_indexed(
    inputs: &[MergeInput],
    sources: &[MergeSource],
    readers: &[&VixReader],
    plan: &MergePlan,
) -> Result<MergedCoreFile, IndexedMergeFailure> {
    use IndexedMergeFailure::{Fallback, Fatal};

    // row merge (timestamps only) FIRST: the #51c passthrough gate needs to
    // know whether the disjoint stream copy runs before the writer (whose
    // docs encoder strategy is fixed at spawn) is constructed. Failures
    // here would hit a rebuild too.
    let started = std::time::Instant::now();
    let timestamps =
        read_timestamp_columns(inputs, sources, plan.cancellation.as_ref()).map_err(Fatal)?;
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
    // #51c-c: a concat-order input invalidates the k-way merge order (its
    // rows are declared unsorted), so the whole merge takes the
    // concatenation path (concat inputs are always legal — passthrough is
    // the native merge shape).
    let concat_input = first_concat_order_input(inputs, sources);
    let early_disjoint = concat_input
        .is_none()
        .then(|| strict_disjoint_input_order(&timestamps))
        .flatten();
    let (maps, offsets) = match (concat_input, early_disjoint.as_deref()) {
        // never run merge_order over an unsorted input: its maps are garbage
        (Some(_), _) => (None, None),
        // The strict range proof makes per-row merge maps unnecessary.
        (None, Some(input_order)) => (
            None,
            Some(concat_doc_id_offsets(input_order, &timestamps).map_err(Fatal)?),
        ),
        (None, None) => {
            let maps = merge_order(&timestamps);
            let offsets = contiguous_offsets(&maps);
            (Some(maps), offsets)
        }
    };
    log::debug!(
        "vix merge: row merge order over {} inputs in {:?} (disjoint: {}, concat input: {})",
        inputs.len(),
        started.elapsed(),
        offsets.is_some(),
        concat_input.is_some(),
    );

    // #51c-c concatenation order — the DEFAULT for OVERLAPPING inputs
    // (where the sorted interleave decodes everything) and for merges
    // containing a concat input (which forces it): store the inputs
    // back-to-back, unlocking the chunk copy. All-or-nothing per-input
    // qualification; a miss keeps the sorted interleave (or, with a concat
    // input, hands the merge to the rebuild's forced concatenation, which
    // decodes unqualified inputs).
    let mut concat: Option<(VixWriter, Vec<usize>)> = None;
    if offsets.is_none() {
        let disqualified = if plan.caps.force_decode {
            // test seam: the fast path's concat requires the chunk copy —
            // a merge containing a concat INPUT still must concatenate,
            // through the rebuild's forced (decoding) concatenation
            Some("force_decode test seam".to_string())
        } else {
            match qualify_concat_fast_path(inputs, readers, plan, &timestamps) {
                Ok(qualified) => {
                    log::debug!(
                        "vix merge: {} overlapping inputs take the #51c-c concatenation order \
                         (all passthrough-qualified)",
                        inputs.len(),
                    );
                    concat = Some(qualified);
                    None
                }
                Err(reason) => Some(reason),
            }
        };
        if let Some(reason) = disqualified {
            if let Some(key) = concat_input {
                return Err(Fallback(anyhow::anyhow!(
                    "input {key} is concatenation-order but the concat fast path is \
                     disqualified ({reason}); the rebuild's forced concatenation handles it"
                )));
            }
            log::debug!(
                "vix merge: concatenation order disqualified ({reason}); interleaving as \
                 today"
            );
        }
    }

    // The passthrough encoder engages wherever the docs blob is a pure
    // chunk copy — the disjoint stream copy and the #51c-c concatenation
    // (passthrough-native, no dark knob). An interleaving merge always
    // decodes, so it keeps the standard docs pipeline (zoned stats, dict
    // layout, coalescing) untouched.
    let disjoint_passthrough = offsets.is_some() && !plan.caps.force_decode;
    let (mut writer, concat_order) = match concat {
        Some((writer, input_order)) => (writer, Some(input_order)),
        None => {
            let mut writer_opts = plan.opts.clone();
            writer_opts.docs_passthrough = disjoint_passthrough;
            (
                VixWriter::new(&plan.writer_schema, writer_opts, plan.store_original),
                None,
            )
        }
    };
    if plan.opts.index_enabled {
        writer
            .check_merge_inputs(readers)
            .map_err(|reason| Fallback(anyhow::anyhow!(reason)))?;
    }

    // merge the term dictionaries BEFORE any docs work: every index-side
    // problem (malformed postings, remap disorder, ...) falls back to the
    // rebuild with nothing wasted. Index-off plans (#40) have no
    // dictionaries to merge (and no doc-id maps to build) — the merge order
    // above still shaped the row order, and the docs push below is the
    // whole merge.
    if plan.opts.index_enabled {
        let doc_maps: Vec<DocIdMap> = if let Some(input_order) = &concat_order {
            // #51c-c: per-input sequential offsets in concatenation order —
            // the index merge consumes them exactly as in the disjoint case
            concat_doc_id_offsets(input_order, &timestamps)
                .map_err(Fatal)?
                .into_iter()
                .map(DocIdMap::Offset)
                .collect()
        } else {
            match &offsets {
                Some(offsets) => offsets
                    .iter()
                    .map(|&offset| DocIdMap::Offset(offset))
                    .collect(),
                None => maps
                    .as_ref()
                    .expect("sorted inputs always have merge maps")
                    .iter()
                    .map(|map| DocIdMap::Table(map.clone()))
                    .collect(),
            }
        };
        let started = std::time::Instant::now();
        plan.check_cancel("index merge before").map_err(Fatal)?;
        writer
            .merge_input_indexes(readers, &doc_maps, merge_threads())
            .map_err(Fallback)?;
        plan.check_cancel("index merge after").map_err(Fatal)?;
        log::debug!("vix merge: index merge total {:?}", started.elapsed());
    }

    let started = std::time::Instant::now();
    let mut perf = MergePerfStats::default();
    let (docs_batches, docs_passthrough_inputs, docs_sliced_windows) = if let Some(input_order) =
        &concat_order
    {
        // #51c-c: the merged docs blob is the inputs' rows concatenated in
        // min_ts order — the same streamed copy as the disjoint arm below
        // (every input qualified up front; a pre-push runtime failure still
        // decodes that input in place, its rows land at the same positions)
        stream_inputs_disjoint(
            inputs,
            readers,
            plan,
            input_order,
            &timestamps,
            true,
            &mut writer,
        )
        .map_err(Fatal)?
    } else if let Some(offsets) = offsets {
        // disjoint inputs: the merged docs blob is the inputs' rows
        // concatenated in offset order — streamed batch copy, no per-row
        // work. Each decoded input runs on its own thread a bounded channel
        // ahead of the pushes, which stay ordered; #51c-qualified inputs
        // copy their encoded chunks instead of decoding at all.
        let mut input_order: Vec<usize> = (0..inputs.len()).collect();
        input_order.sort_unstable_by_key(|&index| offsets[index]);
        stream_inputs_disjoint(
            inputs,
            readers,
            plan,
            &input_order,
            &timestamps,
            disjoint_passthrough,
            &mut writer,
        )
        .map_err(Fatal)?
    } else {
        // overlapping inputs: interleave rows in merged order (same
        // streaming and windowing as the rebuild, minus all term extraction)
        let order = merge_order_inverse(maps.as_ref().expect("sorted inputs have merge maps"));
        perf.order_entries_materialized = order.len() as u64;
        let streamed = stream_merge_windows(inputs, plan, &order, |ts, cs, source, original| {
            writer.push_docs_rows_unindexed(ts, cs, source, original)
        })
        .map_err(Fatal)?;
        perf.interleaved_columns = streamed.perf.interleaved_columns;
        perf.staged_empty_arrays = streamed.perf.staged_empty_arrays;
        (streamed.batches, 0, 0)
    };
    log::debug!(
        "vix merge: docs rows staged in {:?} ({docs_batches} bounded batches, \
         {docs_passthrough_inputs} passthrough inputs, {docs_sliced_windows} sliced \
         column-windows canonicalized)",
        started.elapsed()
    );

    let started = std::time::Instant::now();
    // M18: the fail-open counter lives past finish (the encoder worker
    // finishes inside finish_output)
    let failopen = writer.docs_failopen_counter();
    plan.check_cancel("writer finish before").map_err(Fatal)?;
    let (output, index, stats) = writer.finish_output().map_err(Fatal)?;
    plan.check_cancel("writer finish after").map_err(Fatal)?;
    let docs_failopen_chunks = failopen.load(std::sync::atomic::Ordering::Relaxed);
    if docs_failopen_chunks > 0 {
        log::info!(
            "vix merge: docs passthrough re-encoded {docs_failopen_chunks} chunk(s) (fail-open: \
             non-writable encodings reached the encoder — see debug logs)"
        );
    }
    log::debug!(
        "vix merge: finish (docs blob encode + container) in {:?}",
        started.elapsed()
    );
    Ok(MergedCoreFile {
        output,
        index,
        stats,
        // index-off plans (#40) never merged a dictionary: there is no index
        used_index_merge: plan.opts.index_enabled,
        docs_batches,
        // poison inputs fell back to the rebuild before any index work
        dropped_rows: 0,
        docs_passthrough_inputs,
        concat_order: concat_order.is_some(),
        docs_sliced_windows,
        docs_failopen_chunks,
        // fast path / index-off plan: no term derivation ran
        terms_from_columns: false,
        perf,
    })
}

/// One normalized run of consecutive docs rows of one input, in the merge
/// plan's shapes: `_timestamp` as `i64`, the preserved cs columns cast to
/// their target types — NULL-FILLED when the input lacks the column (v2
/// all-present-columns; `_source` still carries each record's real fields) —
/// and `_source`/`_original` as `Utf8`. Runs are bounded by the plan's
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
    /// M23b: `true` on the LAST chunk the producer sends for one DECODE UNIT
    /// (one granted row range of a gated stream; free-running streams leave
    /// it `false`) — the consumer counts delivered units off this tag to
    /// keep its grant accounting in producer units.
    last_in_unit: bool,
    /// M25: aligned with `plan.preserved` — `true` where the column is
    /// ABSENT from this input's docs schema, so `cs[i]` is a SYNTHESIZED
    /// all-null array (one shared allocation per (type, len) per input, see
    /// [`NullArrayCache`]) rather than decoded data. Synthesized columns are
    /// skipped by the gated deep copy (they share nothing with decode
    /// buffers) and by the transit byte accounting (their cost amortizes to
    /// ~zero) — at prod schema widths they are MOST of the union, and
    /// counting them at face value made a 4096-row unit ~35-50 MB of
    /// transit. Empty slice = nothing synthesized.
    synthesized: Arc<[bool]>,
}

impl MergeChunk {
    fn rows(&self) -> usize {
        self.timestamps.len()
    }
}

/// M23b: consumer-driven decode admission for one GATED (row-range) decode
/// stream — see [`spawn_ranged_input_stream`].
///
/// The producer may decode unit `u` (1-based) only once `granted >= u`; it
/// parks BEFORE decoding otherwise (a parked producer holds no decoded
/// data). The CONSUMER issues grants — on demand (it is about to block on
/// this input's channel) or on low-water prefetch (this input's remaining
/// lookahead ran low, so the next unit's decode overlaps the remaining
/// consumption). `close()` (cursor drop) releases a parked producer
/// unconditionally so the scan can abort; the producer's next send then
/// fails and the thread exits.
///
/// Deadlock-freedom: the consumer blocks only inside `rx.recv()`, and every
/// blocking recv is preceded by `ensure_grant` — so a not-yet-exhausted
/// producer always holds a grant covering the unit the consumer is waiting
/// for (an exhausted producer has dropped `tx`, and recv returns
/// immediately). A granted unit whose rows all cleansed away delivers
/// nothing and consumes no grant (the producer proceeds to the next range
/// under the same grant), so the consumer's delivered-units view never
/// drifts from the producer's. The producer blocks only (a) in `tx.send`,
/// released by the consumer's recv or by cursor drop (rx dropped -> send
/// errs), or (b) in `await_grant`, released by a grant or by `close()` on
/// cursor drop. Cursors drop before the enclosing `std::thread::scope`
/// joins (they are locals of its closure, on success and unwind alike), so
/// the join always completes.
struct DecodeGate {
    state: Mutex<(u64, bool)>, // (granted units, closed)
    cv: Condvar,
}

impl DecodeGate {
    /// A gate with `granted` units pre-granted (the spawn itself is the
    /// consumer's demand for unit 1).
    fn new(granted: u64) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new((granted, false)),
            cv: Condvar::new(),
        })
    }

    /// Producer: park until unit `unit` is granted. `false` = closed (the
    /// consumer is gone): abort the scan.
    fn await_grant(&self, unit: u64) -> bool {
        let mut state = self.state.lock().unwrap();
        while state.0 < unit && !state.1 {
            state = self.cv.wait(state).unwrap();
        }
        !state.1
    }

    /// Consumer: raise the grant watermark (monotonic).
    fn grant_to(&self, granted: u64) {
        let mut state = self.state.lock().unwrap();
        if granted > state.0 {
            state.0 = granted;
            self.cv.notify_all();
        }
    }

    /// Consumer gone: release the producer unconditionally.
    fn close(&self) {
        self.state.lock().unwrap().1 = true;
        self.cv.notify_all();
    }
}

/// M23b: a k-way merge order is served by GATED row-range decode streams
/// (instead of today's free-running whole-blob streams) only when at least
/// this many of its inputs are SCATTERED (their rows do not form one
/// contiguous run of the merged order). Scattered inputs are all mid-flight
/// at once, so each one's resident decode must be a SMALL WINDOW; with
/// fewer scattered inputs than this the whole-group buffering is bounded by
/// (inputs x channel allowance) anyway — a few files' worth — and the
/// free-running stream avoids the ranged scan's decode redundancy (a stored
/// chunk intersecting k ranges decodes k times), which a low-N interleave
/// consuming each input at up to 1/N of the scan rate could actually feel.
/// At >= 8 scattered inputs each input is consumed at <= 1/8 of the scan
/// rate and the redundant decode disappears into idle cores. Constant by
/// design — resident-set correctness must not depend on deployment knobs
/// (M23's no-knobs rule); compaction groups big enough to OOM are 40+
/// files.
const MERGE_SCATTERED_INPUTS_MIN: usize = 8;

/// M23b: rows per GATED decode unit (one grant = one ranged scan of this
/// many rows). Half a window (4096 at production caps) balances the two
/// costs that scale with it: resident decode (every mid-flight input holds
/// ~one unit; the interleaved wave crest holds ~two) and decode redundancy
/// (a stored chunk intersecting k units decodes k times — at prod chunk
/// geometry, 16 MiB decoded over 1.5-3 KB rows ≈ 5-10k rows/chunk, that is
/// ~x2). `max(1024)` keeps tiny test caps from degenerating to per-row
/// scans.
///
/// M25: this is now the UPPER bound — the producer adapts each unit's row
/// count down toward [`MERGE_RANGED_UNIT_TARGET_BYTES`] of ARROW bytes when
/// rows are wide (see [`stream_input_row_ranges`]).
fn ranged_unit_rows(caps: BatchCaps) -> usize {
    (caps.rows / 2).max(1024)
}

/// M25: target ARROW bytes per gated decode unit. M23b bounded the
/// interleaved decode transit in ROWS (`caps.rows/2` = 4096), which priced a
/// row at its VALUE bytes (~1.5 KB) — but the transit holds the rows' arrow
/// form, whose per-row cost is value bytes + ~4-8 B x SCHEMA WIDTH of
/// offsets/validity slots (every preserved column materializes per chunk,
/// null-filled when absent). At prod logs width (~2,164 fields) a 4096-row
/// unit is ~35-50 MB of arrow, x (1 delivered + 1 in-flight) x hundreds of
/// interleaved inputs = the 15 GB decode transit M25 measured (128 x 12 MB
/// inputs at width 2,000: peak 15.0 GB, ~7 GB of it unit transit). Sizing
/// units by BYTES makes the bound width-invariant: narrow rows keep the full
/// 4096-row unit (8 MiB / ~1.6 KB ≈ 5k rows, clamped), wide rows shrink to
/// ~600-1000 rows. Unit boundaries are decode granularity only — outputs
/// are proven byte-identical across unit sizes (the M23b gated-vs-free
/// oracle) — so the adaptation cannot change stored bytes. The extra ranged
/// decode redundancy stays in the M23b-accepted class (a stored chunk
/// intersecting k units decodes k times; gated inputs are consumed at
/// <= 1/8 scan rate, so it rides idle cores).
const MERGE_RANGED_UNIT_TARGET_BYTES: usize = 8 << 20;

/// M25: hard floor on adaptive unit rows — below this the per-unit fixed
/// costs (grant round trip, ranged-scan setup, per-column array overhead of
/// a part) dominate.
const MERGE_RANGED_UNIT_MIN_ROWS: usize = 256;

/// M25: total ARROW bytes of one normalized+copied [`MergeChunk`] — the
/// resident cost of holding it (offsets/validity of every DECODED preserved
/// column included; SYNTHESIZED all-null columns amortize to ~zero via the
/// per-input shared allocation and are skipped), driving the producer's
/// unit-size adaptation.
fn merge_chunk_arrow_bytes(chunk: &MergeChunk) -> usize {
    chunk.timestamps.get_array_memory_size()
        + chunk.source.get_array_memory_size()
        + chunk.original.get_array_memory_size()
        + chunk
            .cs
            .iter()
            .enumerate()
            .filter(|(index, _)| !chunk.synthesized.get(*index).copied().unwrap_or(false))
            .map(|(_, column)| column.get_array_memory_size())
            .sum::<usize>()
}

/// M25: shared all-null arrays for preserved columns ABSENT from one input's
/// docs schema, keyed by (type, rows). v2 null-fill used to allocate a fresh
/// null array PER absent column PER chunk — at prod widths ~1,500-2,100 of
/// the union's columns are absent from any given narrow-WAL file, and those
/// allocations (4-8 B/row of offsets/validity EACH) dominated the merge's
/// decode transit (~10 KB/row of arrow for ~0.6 KB of values). All absent
/// columns of one (type, len) are the SAME logical array, so one allocation
/// serves them all via Arc clones; the byte-identity of the output is
/// untouched (an all-null array is an all-null array — the writer sees
/// identical windows).
#[derive(Default)]
struct NullArrayCache(FastHashMap<(DataType, usize), ArrayRef>);

impl NullArrayCache {
    fn get(&mut self, data_type: &DataType, rows: usize) -> ArrayRef {
        Arc::clone(
            self.0
                .entry((data_type.clone(), rows))
                .or_insert_with(|| arrow::array::new_null_array(data_type, rows)),
        )
    }
}

/// M23b: rows per deep-copied CHUNK within a gated unit (a quarter unit).
/// Consumed rows of a unit free at chunk-copy granularity — without the
/// split, a partially consumed unit stays fully resident until its LAST row
/// is staged, doubling the interleaved wave crest. M25: computed from the
/// CURRENT (byte-adapted) unit rows; `max(64)` bounds the per-part fixed
/// cost (one array per preserved column materializes per part).
fn ranged_part_rows_for(unit_rows: usize) -> usize {
    (unit_rows / 4).max(64)
}

/// M23b: per-input low-water prefetch threshold, in rows of remaining
/// delivered-not-yet-staged lookahead: when a gated input's lookahead drops
/// below a quarter unit (1024 at production caps), the consumer grants its
/// next decode unit so the decode overlaps the remaining consumption.
/// Gated inputs exist only in >= [`MERGE_SCATTERED_INPUTS_MIN`]-way
/// interleaves, where each input is consumed at <= 1/8 of the scan rate —
/// a quarter unit of rows is then >= ~0.2 s of runway against a ~0.1 s
/// unit decode (and SECONDS at real fan-ins), while keeping the crest
/// overlap of consecutive units to ~a quarter unit. `max(64)` keeps tiny
/// test caps prefetching.
fn merge_low_water_rows(caps: BatchCaps) -> usize {
    (ranged_unit_rows(caps) / 4).max(64)
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
    scan: &MergeScanPlan,
    batch: &RecordBatch,
    null_cache: &mut NullArrayCache,
) -> Result<MergeChunk, anyhow::Error> {
    let raw_timestamps = as_int64_array(batch.column(scan.timestamp_index))?;
    let cleansed_batch;
    let (batch, timestamps) = match cleanse_degenerate_ts_rows(batch, &raw_timestamps)? {
        Some((cleansed, _)) => {
            let timestamps = as_int64_array(cleansed.column(scan.timestamp_index))?;
            cleansed_batch = cleansed;
            (&cleansed_batch, timestamps)
        }
        None => (batch, raw_timestamps),
    };
    let rows = batch.num_rows();
    let source = match scan.source_index {
        Some(index) => as_string_array(batch.column(index))?,
        // M31 (!plan.scan_source): `_source` was deliberately not projected
        // — the #46 index-only scan never reads it. A synthesized all-empty
        // (non-null) array keeps the push contract (len match, no nulls)
        // at offsets-buffer cost only.
        None if !plan.scan_source => StringArray::from(vec![""; rows]),
        None => {
            return Err(anyhow::anyhow!(
                "core file {key}: docs batch is missing {SOURCE_COL_NAME:?}"
            ));
        }
    };
    let original = match scan.original_index {
        Some(index) => as_string_array(batch.column(index))?,
        None => StringArray::new_null(rows),
    };
    let mut cs = Vec::with_capacity(plan.preserved.len());
    for ((name, target_type), input_index) in plan.preserved.iter().zip(&scan.preserved_indices) {
        let column = match input_index {
            Some(index) => normalize_merge_column(
                batch.column(*index),
                target_type,
                plan.rewrite_source_from_columns,
            )
            .map_err(|e| {
                anyhow::anyhow!("core file {key}: column {name:?} cast to {target_type}: {e}")
            })?,
            // v2 all-present-columns: an input lacking a column means the
            // column was ABSENT from its records — it contributes nulls for
            // these rows (`_source` still carries each record's real
            // fields; the scan-side json_get fallback serves fields absent
            // from a file's columns). The pre-v2 derive-from-`_source`
            // materialization is gone with `column_store_fields`. M25: the
            // null array is SHARED per (type, len) across all absent
            // columns and chunks of this input (see [`NullArrayCache`]).
            None => null_cache.get(target_type, rows),
        };
        cs.push(column);
    }
    debug_assert!(
        scan.synthesized.len() == plan.preserved.len(),
        "core file {key}: synthesized mask out of sync with the batch's columns"
    );
    let mut accessors: Vec<VarBytes> = Vec::with_capacity(cs.len() + 2);
    accessors.push(VarBytes::new(&source));
    accessors.push(VarBytes::new(&original));
    for column in &cs {
        accessors.push(VarBytes::new(column.as_ref()));
    }
    // The fixed latest-schema path deliberately does not decode the old
    // `_source`, but it materializes a replacement from these columns just
    // before the writer push. Charge exact string escaping, conservative
    // scalar text, every key, and BOTH simultaneously-live JSON copies (the
    // arrow-json line buffer and the returned StringArray value buffer).
    let synthesized_source_fixed = plan.rewrite_source_from_columns.then(|| {
        2usize // object braces
            .saturating_add(json_string_len(TIMESTAMP_COL_NAME.as_bytes()))
            .saturating_add(1 + 128 + 1) // ':', timestamp value, ','
            .saturating_add(
                plan.preserved
                    .iter()
                    .map(|(name, _)| json_string_len(name.as_bytes()) + 2)
                    .sum::<usize>(),
            )
    });
    let row_bytes: Vec<u32> = (0..rows)
        .map(|row| {
            let resident_values = accessors.iter().fold(8usize, |bytes, accessor| {
                bytes.saturating_add(accessor.get(row))
            });
            // At the window peak both the staged arrays and the interleaved
            // output arrays are live. Charge two copies of value buffers plus
            // conservative offset/validity/capacity overhead for timestamp,
            // source, original, and every preserved column.
            let resident_peak = resident_values
                .saturating_mul(2)
                .saturating_add(48usize.saturating_mul(accessors.len() + 1));
            let synthesized_source = synthesized_source_fixed.map_or(0, |fixed| {
                fixed
                    .saturating_add(accessors[2..].iter().fold(0usize, |bytes, accessor| {
                        bytes.saturating_add(accessor.json_len_bound(row))
                    }))
                    .saturating_mul(2)
            });
            resident_peak
                .saturating_add(synthesized_source)
                .min(u32::MAX as usize) as u32
        })
        .collect();
    Ok(MergeChunk {
        timestamps,
        cs,
        source,
        original,
        row_bytes,
        last_in_unit: false,
        synthesized: Arc::clone(&scan.synthesized),
    })
}

/// Normalize one preserved docs column while keeping `_source`'s JSON-null
/// contract for non-finite floats. Arrow's float-to-string kernel emits
/// `"NaN"`/`"inf"`; arrow-json writes those source slots as `null`, so mask
/// them before the cast or compaction would manufacture docs values/postings.
fn normalize_merge_column(
    column: &ArrayRef,
    target_type: &DataType,
    mask_non_finite: bool,
) -> Result<ArrayRef, arrow_schema::ArrowError> {
    let finite_column = if mask_non_finite && string_family(target_type) {
        let non_finite = match column.data_type() {
            DataType::Float16 => Some(BooleanArray::from_iter(
                column
                    .as_any()
                    .downcast_ref::<Float16Array>()
                    .expect("Float16 type")
                    .iter()
                    .map(|value| value.map(|value| !value.is_finite())),
            )),
            DataType::Float32 => Some(BooleanArray::from_iter(
                column
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .expect("Float32 type")
                    .iter()
                    .map(|value| value.map(|value| !value.is_finite())),
            )),
            DataType::Float64 => Some(BooleanArray::from_iter(
                column
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .expect("Float64 type")
                    .iter()
                    .map(|value| value.map(|value| !value.is_finite())),
            )),
            _ => None,
        };
        match non_finite {
            Some(mask) => nullif(column.as_ref(), &mask)?,
            None => Arc::clone(column),
        }
    } else {
        Arc::clone(column)
    };
    cast(&finite_column, target_type)
}

/// One input's positional projection/mapping, built once from its docs schema
/// and reused for every decoded chunk. This avoids repeated schema scans and
/// `RecordBatch::column_by_name` calls on the width-thousands hot path.
struct MergeScanPlan {
    projection: Vec<String>,
    timestamp_index: usize,
    source_index: Option<usize>,
    original_index: Option<usize>,
    preserved_indices: Vec<Option<usize>>,
    synthesized: Arc<[bool]>,
}

fn build_merge_scan_plan(schema: &SchemaRef, plan: &MergePlan) -> MergeScanPlan {
    let present: FastHashSet<&str> = schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    let mut projection: Vec<String> = vec![TIMESTAMP_COL_NAME.to_string()];
    let timestamp_index = 0;
    let source_index = if plan.scan_source {
        let index = projection.len();
        projection.push(SOURCE_COL_NAME.to_string());
        Some(index)
    } else {
        None
    };
    let mut preserved_indices = Vec::with_capacity(plan.preserved.len());
    for (name, _) in &plan.preserved {
        if present.contains(name.as_str()) {
            preserved_indices.push(Some(projection.len()));
            projection.push(name.clone());
        } else {
            preserved_indices.push(None);
        }
    }
    let original_index = if plan.store_original && present.contains(ORIGINAL_DATA_COL_NAME) {
        let index = projection.len();
        projection.push(ORIGINAL_DATA_COL_NAME.to_string());
        Some(index)
    } else {
        None
    };
    let synthesized = preserved_indices
        .iter()
        .map(Option::is_none)
        .collect::<Vec<_>>()
        .into();
    MergeScanPlan {
        projection,
        timestamp_index,
        source_index,
        original_index,
        preserved_indices,
        synthesized,
    }
}

/// Spawn one input's decode thread: stream the projected docs columns
/// ([`VixDocs::scan_docs`], one decoded chunk at a time), split every chunk
/// to the byte caps, normalize, and send the bounded [`MergeChunk`]s through
/// a small channel — the input-side memory stays a few chunks regardless of
/// file size (but NOTE: a small L0 file is ~ONE stored chunk, so "a few
/// chunks" is the whole file — use [`spawn_ranged_input_stream`] wherever
/// many inputs are mid-flight at once). The thread stops as soon as the
/// receiver is dropped.
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
    plan.check_cancel_context("decode open", key)?;
    if docs.row_count() == 0 {
        return Ok(());
    }
    let scan = build_merge_scan_plan(docs.schema(), plan);
    let mut null_cache = NullArrayCache::default();
    docs.scan_docs(Some(&scan.projection), None, None, &mut |batch| {
        plan.check_cancel_context("decode unit", key)?;
        if batch.num_rows() == 0 {
            return Ok(());
        }
        for part in split_batch_by_bytes(&batch, plan.caps, false) {
            let chunk = normalize_merge_chunk(key, plan, &scan, &part, &mut null_cache)?;
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

/// M23b: one array deep-copied into fresh minimal buffers (a 0..len `take`
/// gather — NOT `concat`, whose single-input fast path returns a
/// buffer-sharing slice). Batch slices share the decode's backing buffers,
/// so a ranged part sent as a slice would keep its whole decode unit (or
/// worse, whatever the scan yielded) alive until the LAST of its rows is
/// consumed. The copy makes every part independently freeable — the memory
/// bound must hold by construction, not by the reader's current
/// materialization habits — and is one gather of bytes the scan was going
/// to consume anyway, off the consumer's critical path.
fn deep_copy_array(array: &dyn Array) -> Result<ArrayRef, anyhow::Error> {
    let indices = arrow::array::UInt32Array::from_iter_values(0..array.len() as u32);
    Ok(arrow::compute::take(array, &indices, None)?)
}

/// M23b: spawn one input's GATED row-range decode thread — the bounded
/// twin of [`spawn_input_stream`] for k-way orders where many inputs are
/// mid-flight at once (see [`MERGE_SCATTERED_INPUTS_MIN`]).
///
/// Instead of one free-running whole-blob scan (whose decode unit is the
/// STORED chunk — for the small-L0 population effectively the whole file),
/// the producer decodes `caps.rows`-sized ROW RANGES
/// ([`VixDocs::scan_docs_row_range`]), one per consumer grant
/// ([`DecodeGate`]), deep-copying each normalized unit out of the decode's
/// backing buffers. Resident per input ≈ one delivered unit (+ one granted
/// unit in flight around a handoff) regardless of file or stored-chunk
/// size; a stored chunk intersecting k ranges decodes k times, which is
/// idle-core work at the input counts this stream is used for.
fn spawn_ranged_input_stream<'scope, 'env>(
    scope: &'scope std::thread::Scope<'scope, 'env>,
    key: &'env str,
    data: Arc<dyn vortex_index::VixRangeSource>,
    plan: &'env MergePlan,
    gate: Arc<DecodeGate>,
) -> Receiver<Result<MergeChunk, anyhow::Error>> {
    let (tx, rx) = sync_channel(2);
    scope.spawn(move || {
        if let Err(error) = stream_input_row_ranges(key, data, plan, &tx, &gate) {
            // a failed send here means the consumer is already gone
            let _ = tx.send(Err(error));
        }
    });
    rx
}

fn stream_input_row_ranges(
    key: &str,
    data: Arc<dyn vortex_index::VixRangeSource>,
    plan: &MergePlan,
    tx: &SyncSender<Result<MergeChunk, anyhow::Error>>,
    gate: &DecodeGate,
) -> Result<(), anyhow::Error> {
    let docs =
        VixDocs::open_ranged(data).map_err(|e| anyhow::anyhow!("open core file {key}: {e}"))?;
    let rows = docs.row_count();
    if rows == 0 {
        return Ok(());
    }
    let scan = build_merge_scan_plan(docs.schema(), plan);
    let mut null_cache = NullArrayCache::default();
    let max_unit_rows = ranged_unit_rows(plan.caps);
    // M25: width-aware unit sizing — the resident cost of a unit is its
    // ARROW bytes (per-column offset/validity slots of the DECODED columns;
    // synthesized nulls are shared and near-free), so size units by bytes:
    // start from a width-based estimate over this input's PRESENT columns,
    // then track the measured bytes/row of the previous unit. Unit
    // boundaries are decode granularity only (byte-identical outputs across
    // unit sizes — the M23b oracle).
    let present_columns = scan.synthesized.iter().filter(|synth| !**synth).count();
    let mut est_row_arrow_bytes = present_columns * 4 + 512;
    // units DELIVERED (>= 1 chunk sent): a unit whose rows all cleansed away
    // consumes no grant — the consumer never sees it, so it must not advance
    // the grant bookkeeping either (see DecodeGate's deadlock notes)
    let mut sent_units = 0u64;
    let mut start = 0u64;
    while start < rows {
        plan.check_cancel_context("decode unit", format_args!("{key} rows {start}"))?;
        // park BEFORE decoding — a parked producer holds no decoded data
        if !gate.await_grant(sent_units + 1) {
            return Err(anyhow::anyhow!("merge staging stopped"));
        }
        let unit_rows = (MERGE_RANGED_UNIT_TARGET_BYTES / est_row_arrow_bytes.max(1))
            .clamp(MERGE_RANGED_UNIT_MIN_ROWS, max_unit_rows) as u64;
        // parts are deep copies, so consumed rows free at part granularity —
        // a caps whose byte bound is tighter than the part rows still applies
        let part_caps = BatchCaps {
            rows: ranged_part_rows_for(unit_rows as usize),
            ..plan.caps
        };
        let end = rows.min(start + unit_rows);
        let mut chunks: Vec<MergeChunk> = Vec::new();
        docs.scan_docs_row_range(Some(&scan.projection), start..end, &mut |batch| {
            plan.check_cancel_context("decode batch", format_args!("{key} rows {start}..{end}"))?;
            if batch.num_rows() == 0 {
                return Ok(());
            }
            for part in split_batch_by_bytes(&batch, part_caps, false) {
                let mut chunk = normalize_merge_chunk(key, plan, &scan, &part, &mut null_cache)?;
                if chunk.rows() == 0 {
                    continue; // fully cleansed part (order skipped the rows)
                }
                // decouple the unit from the decode's backing buffers.
                // M25: SYNTHESIZED all-null columns skip the copy — they
                // are our own shared allocations (see [`NullArrayCache`]),
                // not views into the reader's decode buffers.
                chunk.timestamps = as_int64_array(&deep_copy_array(&chunk.timestamps)?)?;
                chunk.source = as_string_array(&deep_copy_array(&chunk.source)?)?;
                chunk.original = as_string_array(&deep_copy_array(&chunk.original)?)?;
                for (index, column) in chunk.cs.iter_mut().enumerate() {
                    if chunk.synthesized.get(index).copied().unwrap_or(false) {
                        continue;
                    }
                    *column = deep_copy_array(column.as_ref())?;
                }
                chunks.push(chunk);
            }
            Ok(())
        })
        .map_err(|e| anyhow::anyhow!("stream core file {key} rows {start}..{end}: {e}"))?;
        start = end;
        // adapt the next unit to the measured arrow weight of this one
        let unit_arrow: usize = chunks.iter().map(merge_chunk_arrow_bytes).sum();
        let unit_sent_rows: usize = chunks.iter().map(MergeChunk::rows).sum();
        if unit_sent_rows > 0 {
            est_row_arrow_bytes = (unit_arrow / unit_sent_rows).max(64);
        }
        if let Some(last) = chunks.last_mut() {
            last.last_in_unit = true;
            sent_units += 1;
        }
        for chunk in chunks {
            if tx.send(Ok(chunk)).is_err() {
                // consumer gone (finished or failed): stop quietly
                return Err(anyhow::anyhow!("merge staging stopped"));
            }
        }
    }
    Ok(())
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
    /// M23b decode admission (gated cursors only; `None` = free-running,
    /// the sequential drain paths).
    gate: Option<Arc<DecodeGate>>,
    /// Decode units granted to the producer (it may decode unit `u` while
    /// `granted_units >= u`).
    granted_units: u64,
    /// Decode units fully received (`last_in_unit` chunks seen). The grant
    /// invariant `granted_units <= delivered_units + 1` bounds each input to
    /// ONE unit in flight beyond its delivered ones.
    delivered_units: u64,
    /// Rows delivered but not yet staged/consumed — the input's remaining
    /// lookahead, maintained incrementally (the low-water prefetch trigger).
    lookahead_rows: usize,
    /// [`merge_low_water_rows`] of the plan's caps, resolved at spawn.
    low_water_rows: usize,
}

/// One input's contribution to one merge window (the interleave sources).
struct StagedInput {
    timestamps: ArrayRef,
    cs: Vec<ArrayRef>,
    source: ArrayRef,
    original: ArrayRef,
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
    /// A free-running cursor (no decode admission): the sequential drain
    /// paths, where one input is consumed at a time and the channel bound is
    /// the right in-flight cap.
    fn new(key: String, rx: Receiver<Result<MergeChunk, anyhow::Error>>) -> Self {
        Self {
            key,
            rx,
            pending: VecDeque::new(),
            consumed: 0,
            staged: 0,
            gate: None,
            granted_units: 0,
            delivered_units: 0,
            lookahead_rows: 0,
            low_water_rows: 0,
        }
    }

    /// M23b: a gated cursor for the k-way window staging. `gate` must be the
    /// one the producer was spawned with, pre-granted ONE unit (the spawn is
    /// the demand for unit 1).
    fn gated(
        key: String,
        rx: Receiver<Result<MergeChunk, anyhow::Error>>,
        gate: Arc<DecodeGate>,
        low_water_rows: usize,
    ) -> Self {
        let mut cursor = Self::new(key, rx);
        cursor.gate = Some(gate);
        cursor.granted_units = 1;
        cursor.low_water_rows = low_water_rows;
        cursor
    }

    /// Grant the producer its next decode unit if none is in flight
    /// (`granted_units == delivered_units`) — the ONLY grant path, so
    /// `granted_units <= delivered_units + 1` holds by construction.
    fn ensure_grant(&mut self) {
        if let Some(gate) = &self.gate
            && self.granted_units == self.delivered_units
        {
            self.granted_units += 1;
            gate.grant_to(self.granted_units);
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

    /// The next whole chunk in row order (the sequential path — free-running
    /// cursors only).
    fn next_chunk(&mut self) -> Result<Option<MergeChunk>, anyhow::Error> {
        debug_assert_eq!(self.consumed + self.staged, 0);
        debug_assert!(self.gate.is_none(), "next_chunk on a gated cursor");
        if let Some(chunk) = self.pending.pop_front() {
            return Ok(Some(chunk));
        }
        self.recv_chunk()
    }

    /// M23b: pull one more chunk into `pending`, keeping the admission
    /// bookkeeping current. `false` = the stream ended cleanly.
    fn recv_into_pending(&mut self) -> Result<bool, anyhow::Error> {
        // demand: never block on a channel without a unit in flight — an
        // un-exhausted producer then always progresses toward this recv
        // (the deadlock-freedom half the gate comment relies on)
        self.ensure_grant();
        let Some(chunk) = self.recv_chunk()? else {
            return Ok(false);
        };
        self.lookahead_rows += chunk.rows();
        if chunk.last_in_unit {
            self.delivered_units += 1;
        }
        self.pending.push_back(chunk);
        Ok(true)
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
                    let bytes = chunk.row_bytes[position] as usize;
                    self.staged += 1;
                    self.lookahead_rows = self.lookahead_rows.saturating_sub(1);
                    // low-water prefetch: the remaining lookahead of THIS
                    // input is about to run out — grant the next unit now so
                    // its decode overlaps the remaining consumption instead
                    // of stalling the (much slower) scan at exhaustion.
                    if self.lookahead_rows < self.low_water_rows {
                        self.ensure_grant();
                    }
                    return Ok(bytes);
                }
                Some(chunk) => {
                    position -= chunk.rows();
                    chunk_index += 1;
                }
                None => {
                    if !self.recv_into_pending()? {
                        return Err(anyhow::anyhow!(
                            "core file {}: docs stream ended before all rows of the timestamp \
                             column were staged",
                            self.key
                        ));
                    }
                }
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

impl Drop for InputCursor {
    fn drop(&mut self) {
        // release a producer parked at the gate (its next send then fails on
        // the dropped rx and the thread exits) — cursors drop before their
        // thread scope joins, on success and unwind alike
        if let Some(gate) = &self.gate {
            gate.close();
        }
    }
}

/// Stream the merged rows to `push` in bounded windows: walk `order` (the
/// merged doc order), staging each row on its input's cursor, and close a
/// window when it reaches the plan's row cap or byte budget. Each window
/// interleaves the inputs' staged runs into one bounded batch — the ONLY
/// arrays the row-interleave merge materializes over
/// `_source`/`_original`/cs values, so no whole-file column ever exists in
/// memory (a whole-hour `_source` column overflows arrow's `i32` Utf8
/// offsets).
#[derive(Debug, Default)]
struct MergeStreamResult {
    batches: usize,
    perf: MergePerfStats,
}

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
) -> Result<MergeStreamResult, anyhow::Error> {
    std::thread::scope(|scope| {
        // M23: decode streams spawn LAZILY, on the first row the merge order
        // actually needs from an input — NOT all upfront. Spawning every
        // input's decode thread at once made a CONCATENATION-shaped order
        // (disjoint inputs — the dominant compaction group shape) buffer the
        // entire not-yet-consumed group DECODED in RAM: each idle input's
        // thread eagerly decoded its whole (small) file into its bounded
        // channel and sat on it until the scan reached that input. For a
        // group of hundreds of small L0 files that is ~2-3x the group's
        // original bytes of arrow, climbing at aggregate-decode speed — the
        // production compactor OOM (M23). Lazy spawn keeps in-flight decode
        // O(active input) on concatenation orders.
        //
        // M23b: an INTERLEAVED k-way order (overlapping inputs — the prod
        // L0 population of one (stream,hour)) touches every input within
        // the first window, so lazy spawn degenerates to eager — and the
        // free-running stream's decode unit is the STORED chunk, for a
        // small L0 file effectively the whole file: whole group resident
        // again, the OOM signature unchanged (measured: 256x12MB
        // interleaved peaks ~11.4 GB either way). Inputs whose rows are
        // SCATTERED across the order therefore stream as GATED row-range
        // decodes ([`spawn_ranged_input_stream`]): ≤ one `caps.rows` unit
        // in flight beyond the delivered-and-unconsumed one, granted by
        // demand or low-water prefetch — resident decode
        // O(mid-flight inputs x caps unit) instead of O(group bytes).
        // CONTIGUOUS-run inputs (concatenation orders — the dominant
        // compaction shape, M23) and low-scatter orders keep the
        // free-running stream: their consumption drains each input as it
        // spawns, and they skip the ranged scan's decode redundancy.
        let scattered: Vec<bool> = {
            // per input: rows in `order` + [first, last] order positions
            let mut count = vec![0usize; inputs.len()];
            let mut first = vec![usize::MAX; inputs.len()];
            let mut last = vec![0usize; inputs.len()];
            for (position, &(input, _)) in order.iter().enumerate() {
                count[input] += 1;
                if first[input] == usize::MAX {
                    first[input] = position;
                }
                last[input] = position;
            }
            let scattered: Vec<bool> = (0..inputs.len())
                .map(|input| count[input] > 0 && last[input] - first[input] + 1 != count[input])
                .collect();
            let scattered_inputs = scattered.iter().filter(|s| **s).count();
            if scattered_inputs >= MERGE_SCATTERED_INPUTS_MIN {
                log::debug!(
                    "vix merge: {scattered_inputs} of {} inputs are order-scattered: gated \
                     row-range decode streams (unit {} rows)",
                    inputs.len(),
                    plan.caps.rows.max(1)
                );
                scattered
            } else {
                vec![false; inputs.len()]
            }
        };
        let low_water_rows = merge_low_water_rows(plan.caps);
        let mut cursors: Vec<Option<InputCursor>> = (0..inputs.len()).map(|_| None).collect();

        let mut result = MergeStreamResult::default();
        let mut start = 0usize;
        while start < order.len() {
            plan.check_cancel("merge window start")?;
            // stage rows until a cap closes the window (always ≥ 1 row)
            let mut end = start;
            let mut bytes = 0usize;
            while end < order.len() && end - start < plan.caps.rows.max(1) {
                let (input, _) = order[end];
                let cursor = cursors[input].get_or_insert_with(|| {
                    let (key, data, _) = &inputs[input];
                    if scattered[input] {
                        let gate = DecodeGate::new(1);
                        let stream = spawn_ranged_input_stream(
                            scope,
                            key,
                            Arc::clone(data),
                            plan,
                            Arc::clone(&gate),
                        );
                        InputCursor::gated(key.clone(), stream, gate, low_water_rows)
                    } else {
                        InputCursor::new(
                            key.clone(),
                            spawn_input_stream(scope, key, Arc::clone(data), plan),
                        )
                    }
                });
                bytes = bytes.saturating_add(cursor.stage_next_row()?);
                end += 1;
                if bytes >= plan.caps.bytes.max(1) {
                    break;
                }
            }

            // Compact this window to inputs that actually contribute rows.
            // A wide fan-in commonly touches only a small subset per
            // bounded window; typed empty arrays for every other input add
            // allocations and make Arrow's interleave dispatch wider for no
            // semantic benefit.
            let mut staged = Vec::new();
            let mut local_input = vec![usize::MAX; cursors.len()];
            for (input, cursor) in cursors.iter_mut().enumerate() {
                if let Some(cursor) = cursor
                    && cursor.staged > 0
                {
                    local_input[input] = staged.len();
                    staged.push(cursor.take_staged(plan)?);
                }
            }

            // interleave indices: each merged row is its input's next
            // staged row
            let mut positions = vec![0usize; cursors.len()];
            let indices: Vec<(usize, usize)> = order[start..end]
                .iter()
                .map(|&(input, _)| {
                    let position = positions[input];
                    positions[input] += 1;
                    (local_input[input], position)
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
            // the preserved columns, in MergeChunk::cs positional order
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
            plan.check_cancel("writer push before interleave window")?;
            push(&timestamps, &cs_columns, &source, original.as_ref())?;
            plan.check_cancel("writer push after interleave window")?;
            result.batches += 1;
            result.perf.interleaved_columns +=
                (2 + usize::from(plan.store_original) + plan.preserved.len()) as u64;
            // M23b transit gauge (debug builds of the OOM analysis): counts
            // only — proves the staged-transit bound vs writer-side growth.
            // m25: plus the pending chunks' ARROW bytes (width-scaled: every
            // pending chunk holds one array per preserved column, null
            // arrays included).
            if result.batches % 100 == 0 && log::log_enabled!(log::Level::Debug) {
                let mut live = 0usize;
                let mut pending_rows = 0usize;
                let mut pending_arrow_bytes = 0usize;
                let mut inflight_units = 0u64;
                for cursor in cursors.iter().flatten() {
                    live += 1;
                    for chunk in &cursor.pending {
                        pending_rows += chunk.rows();
                        pending_arrow_bytes += merge_chunk_arrow_bytes(chunk);
                    }
                    inflight_units += cursor.granted_units - cursor.delivered_units;
                }
                log::debug!(
                    "vix merge: window {}: {live} cursors, {pending_rows} pending rows, \
                     {pending_arrow_bytes} pending arrow bytes, {inflight_units} decode units \
                     in flight",
                    result.batches,
                );
            }
            start = end;
        }
        Ok(result)
    })
}

/// Stream pure concatenation/disjoint runs without a row-order vector or
/// Arrow's generic interleave kernel. Each input is decoded only when its
/// run becomes active, keeping resident memory bounded by one input stream
/// plus the writer's own bounded pipeline.
fn stream_concat_windows(
    inputs: &[MergeInput],
    plan: &MergePlan,
    runs: &[ConcatRun],
    mut push: impl FnMut(
        &Int64Array,
        &[(String, ArrayRef)],
        &StringArray,
        Option<&StringArray>,
    ) -> Result<(), anyhow::Error>,
) -> Result<MergeStreamResult, anyhow::Error> {
    std::thread::scope(|scope| {
        let mut result = MergeStreamResult::default();
        for run in runs.iter().filter(|run| run.rows > 0) {
            let (key, data, _) = &inputs[run.input];
            plan.check_cancel_context("concat run before", key)?;
            let mut cursor = InputCursor::new(
                key.clone(),
                spawn_input_stream(scope, key, Arc::clone(data), plan),
            );
            let mut consumed = 0usize;
            while let Some(chunk) = cursor.next_chunk()? {
                consumed += chunk.rows();
                // Raw decode splitting does not charge a synthesized
                // replacement `_source`; use the normalized row weights to
                // retain the byte bound without invoking interleave.
                let mut start = 0usize;
                while start < chunk.rows() {
                    let mut end = start;
                    let mut bytes = 0usize;
                    while end < chunk.rows() && end - start < plan.caps.rows.max(1) {
                        bytes = bytes.saturating_add(chunk.row_bytes[end] as usize);
                        end += 1;
                        if bytes >= plan.caps.bytes.max(1) {
                            break;
                        }
                    }
                    plan.check_cancel_context("writer push before concat chunk", key)?;
                    let len = end - start;
                    let timestamps = chunk.timestamps.slice(start, len);
                    let source = chunk.source.slice(start, len);
                    let original = chunk.original.slice(start, len);
                    let cs_columns: Vec<(String, ArrayRef)> = plan
                        .preserved
                        .iter()
                        .zip(&chunk.cs)
                        .map(|((name, _), column)| (name.clone(), column.slice(start, len)))
                        .collect();
                    push(
                        &timestamps,
                        &cs_columns,
                        &source,
                        plan.store_original.then_some(&original),
                    )?;
                    plan.check_cancel_context("writer push after concat chunk", key)?;
                    result.batches += 1;
                    start = end;
                }
            }
            if consumed != run.rows {
                return Err(anyhow::anyhow!(
                    "core file {key} (input {}, output doc {}): concat stream produced \
                     {consumed} rows, expected {}",
                    run.input,
                    run.first_doc,
                    run.rows,
                ));
            }
        }
        Ok(result)
    })
}

/// Stream the inputs' rows into `writer` input-by-input in `input_order`
/// (the disjoint fast path: the merged docs blob is the inputs' rows
/// concatenated). Decoding inputs run in parallel, each a bounded channel
/// ahead of the staging; every pushed batch is byte-capped. Inputs that
/// qualify (#51c — the DEFAULT merge shape) skip decoding entirely: their
/// docs chunks are copied in stored (compressed) form through the writer's
/// encoded-run API, their zone tables spliced verbatim, and — when the plan
/// carries #52 bloom-only fields — ONLY those columns are decoded (a
/// projected scan) for composite-bloom coverage. A qualification or
/// pre-push failure silently falls back to the decode path for that input
/// only. Returns `(batches_pushed, passthrough_inputs, sliced_windows)`.
fn stream_inputs_disjoint(
    inputs: &[MergeInput],
    readers: &[&VixReader],
    plan: &MergePlan,
    input_order: &[usize],
    timestamps: &[Int64Array],
    docs_passthrough: bool,
    writer: &mut VixWriter,
) -> Result<(usize, usize, u64), anyhow::Error> {
    // Qualify up front, so decode threads spawn only for decoding inputs.
    // `docs_passthrough` mirrors the writer's own construction (false only
    // under the force_decode test seam): a non-passthrough writer cannot
    // accept encoded runs, so qualifying would be a pointless probe.
    let mut qualified: Vec<Option<PassthroughSplice>> = (0..inputs.len()).map(|_| None).collect();
    if docs_passthrough {
        for &index in input_order {
            let key = &inputs[index].0;
            plan.check_cancel_context("passthrough qualification", key)?;
            match qualify_passthrough_input(readers[index], timestamps[index].len() as u64, writer)
            {
                Ok(splice) => {
                    log::debug!(
                        "vix merge: input {key} qualifies for docs passthrough ({} zone entries)",
                        splice.zone_entries.len()
                    );
                    qualified[index] = Some(splice);
                }
                Err(reason) => {
                    log::debug!(
                        "vix merge: input {key} does not qualify for docs passthrough \
                         (decode path): {reason}"
                    );
                }
            }
        }
    }

    // M12: the #52 composite-bloom coverage scans of the qualified inputs —
    // restricted per input to the fields the k-way dictionary walk cannot
    // absorb from it (double-hash elimination) and run in PARALLEL across
    // inputs on the merge thread budget (hash absorption commutes; each
    // worker's set folds into the writer by union, identical to any
    // sequential order). A failed scan demotes that input to the decode
    // path — the same fallback a mid-copy scan failure took before — and
    // its partial hashes are discarded (the decode push re-hashes every
    // value; the sets dedupe).
    let mut scan_queue: Vec<(usize, vortex_index::BloomOnlyHasher)> = Vec::new();
    for &index in input_order {
        plan.check_cancel_context("bloom coverage planning", &inputs[index].0)?;
        if qualified[index].is_none() {
            continue; // decode inputs hash at push time
        }
        let fields = bloom_scan_fields_for_input(writer, readers[index]);
        if fields.is_empty() {
            continue;
        }
        let hasher = writer.bloom_only_hasher(&fields);
        if !hasher.is_empty() {
            scan_queue.push((index, hasher));
        }
    }
    if !scan_queue.is_empty() {
        let started = std::time::Instant::now();
        let tasks = scan_queue.len();
        let workers = merge_threads().min(tasks).max(1);
        let queue = std::sync::Mutex::new(scan_queue);
        type ScanOutcome = Result<vortex_index::BloomEncodingCensus, anyhow::Error>;
        let done: std::sync::Mutex<Vec<(usize, vortex_index::BloomOnlyHasher, ScanOutcome)>> =
            std::sync::Mutex::new(Vec::with_capacity(tasks));
        std::thread::scope(|scope| {
            for _ in 0..workers {
                scope.spawn(|| {
                    loop {
                        let Some((index, mut hasher)) = queue.lock().unwrap().pop() else {
                            break;
                        };
                        let (key, data, _) = &inputs[index];
                        let outcome = scan_bloom_coverage(key, Arc::clone(data), &mut hasher);
                        done.lock().unwrap().push((index, hasher, outcome));
                    }
                });
            }
        });
        plan.check_cancel("bloom coverage scan")?;
        let mut results = done.into_inner().unwrap();
        // absorb in input order — cosmetic determinism (union commutes)
        results.sort_unstable_by_key(|(index, ..)| *index);
        let mut census = vortex_index::BloomEncodingCensus::default();
        for (index, hasher, outcome) in results {
            match outcome {
                Ok(input_census) => {
                    census.absorb(input_census);
                    writer.absorb_bloom_only_hashes(hasher);
                }
                Err(reason) => {
                    log::debug!(
                        "vix merge: input {} fell back to the decode path (coverage scan): \
                         {reason:#}",
                        inputs[index].0
                    );
                    qualified[index] = None;
                }
            }
        }
        // M17 item 2's prod probe: one line per merge, counts only —
        // which encoding classes the demoted-field byte volume rides.
        log::info!(
            "vix merge: bloom coverage encoding census: dict={} fsst={} other={} chunks over \
             {tasks} inputs ({workers} workers, {:?})",
            census.dict_chunks,
            census.fsst_chunks,
            census.other_chunks,
            started.elapsed()
        );
    }

    std::thread::scope(|scope| {
        // M23b: decode streams spawn when their input's DRAIN starts — not
        // all upfront. The drain is strictly input-by-input, so an upfront
        // spawn made every not-yet-reached decode input buffer
        // min(whole file, channel allowance) decoded for the whole time the
        // earlier inputs drained — the M23 whole-group pathology whenever
        // many inputs take the decode path (e.g. a type-flipped column
        // disqualifying the passthrough). One active input needs no
        // admission gate: the channel bound is the right in-flight cap.
        let mut batches = 0usize;
        let mut passthrough_inputs = 0usize;
        let mut sliced_windows = 0u64;
        for &index in input_order {
            let (key, data, _) = &inputs[index];
            plan.check_cancel_context("disjoint input", key)?;
            if let Some(splice) = &qualified[index] {
                match copy_passthrough_input(
                    key,
                    Arc::clone(data),
                    &timestamps[index],
                    splice,
                    plan,
                    writer,
                ) {
                    Ok((chunks, sliced)) => {
                        log::debug!(
                            "vix merge: input {key} copied through docs passthrough \
                             ({chunks} encoded chunks, {sliced} sliced column-windows \
                             canonicalized)"
                        );
                        batches += chunks;
                        passthrough_inputs += 1;
                        sliced_windows += sliced;
                        continue;
                    }
                    // nothing reached the writer: this input can still take
                    // the decode path (spawn its stream now)
                    Err(PassthroughFailure::BeforePush(reason)) => {
                        log::debug!(
                            "vix merge: input {key} fell back to the decode path: {reason:#}"
                        );
                        let mut cursor = InputCursor::new(
                            key.clone(),
                            spawn_input_stream(scope, key, Arc::clone(data), plan),
                        );
                        batches += drain_input_cursor(&mut cursor, plan, writer)?;
                    }
                    // encoded chunks already reached the writer: the docs
                    // blob is part-written, no fallback can express that
                    Err(PassthroughFailure::Poisoned(error)) => return Err(error),
                }
            } else {
                let mut cursor = InputCursor::new(
                    key.clone(),
                    spawn_input_stream(scope, key, Arc::clone(data), plan),
                );
                batches += drain_input_cursor(&mut cursor, plan, writer)?;
            }
        }
        Ok((batches, passthrough_inputs, sliced_windows))
    })
}

/// Drain one decoding input's chunks into the writer (the disjoint decode
/// path, unchanged from before #51c).
fn drain_input_cursor(
    cursor: &mut InputCursor,
    plan: &MergePlan,
    writer: &mut VixWriter,
) -> Result<usize, anyhow::Error> {
    let mut batches = 0usize;
    while let Some(chunk) = cursor.next_chunk()? {
        plan.check_cancel_context("writer push before disjoint chunk", &cursor.key)?;
        let cs_columns: Vec<(String, ArrayRef)> = plan
            .preserved
            .iter()
            .zip(&chunk.cs)
            .map(|((name, _), column)| (name.clone(), Arc::clone(column)))
            .collect();
        writer.push_docs_rows_unindexed(
            &chunk.timestamps,
            &cs_columns,
            &chunk.source,
            plan.store_original.then_some(&chunk.original),
        )?;
        plan.check_cancel_context("writer push after disjoint chunk", &cursor.key)?;
        batches += 1;
    }
    Ok(batches)
}

/// Why one input's #51c passthrough copy did not complete.
enum PassthroughFailure {
    /// Nothing reached the writer yet — the input silently takes the
    /// decode path.
    BeforePush(anyhow::Error),
    /// Encoded chunks were already stored — the merge must abort (the
    /// docs blob cannot be un-written).
    Poisoned(anyhow::Error),
}

/// One qualified passthrough input's spliceable metadata: its zone table
/// (verbatim entries), its per-column chunk stats + presence counts, its §4
/// region decomposition (per-run row counts, `None` = unproven — poisons
/// the output's region table while the copy proceeds), and the M17 widen
/// plan mapping its chunks into the output docs schema (identity when the
/// schemas already match; null-synthesizing when the output union is a
/// strict superset — the gen-1 encode-once case).
struct PassthroughSplice {
    zone_entries: Vec<vortex_index::ZoneEntry>,
    stats: vortex_index::SpliceableStats,
    regions: Option<Vec<u64>>,
    widen: vortex_index::DocsWidenPlan,
}

/// #51c qualification of one disjoint-merge input for the encoded-chunk
/// copy. Every check is metadata-cheap (footer + one small stats-blob
/// fetch); any miss returns the reason and the input takes the decode path.
/// STRUCTURAL stats guarantee (H2/§4): an input without a spliceable stats
/// table — a pre-stats file, or misaligned tables — CANNOT copy through;
/// it decodes and the output computes fresh stats, so a passthrough output
/// always carries full pruning metadata (the v1 stats-loss regression is
/// impossible by construction).
fn qualify_passthrough_input(
    reader: &VixReader,
    materialized_rows: u64,
    writer: &VixWriter,
) -> Result<PassthroughSplice, String> {
    if materialized_rows == 0 {
        return Err("the input is empty".to_string());
    }
    if reader.row_count() != materialized_rows {
        return Err(format!(
            "row_count property ({}) disagrees with the timestamp column ({materialized_rows} \
             rows)",
            reader.row_count()
        ));
    }
    let input_schema = match reader.docs_schema() {
        Ok(schema) => schema,
        Err(error) => return Err(format!("unreadable docs schema: {error:#}")),
    };
    // M17: schema identity is no longer required — a widen plan maps the
    // input's chunks into the output union (missing columns synthesize as
    // all-null constants). Only a genuine type flip (or a column the output
    // would drop) still forces this input onto the decode path.
    let widen = vortex_index::docs_widen_plan(&input_schema, writer.docs_schema())?;
    let Some(chunks) = reader.zone_chunks() else {
        return Err("no zone table (pre-zone-map file)".to_string());
    };
    let entries: Vec<vortex_index::ZoneEntry> = chunks
        .iter()
        .map(|zone| (zone.row_count, zone.ts_min, zone.ts_max))
        .collect();
    let covered: u64 = entries.iter().map(|(rows, ..)| rows).sum();
    if covered != materialized_rows {
        return Err(format!(
            "zone table covers {covered} of {materialized_rows} rows"
        ));
    }
    let stats = match reader.spliceable_stats() {
        Ok(Some(stats)) => stats,
        Ok(None) => {
            return Err(
                "no spliceable column stats (pre-stats file) — the decode path computes them \
                 fresh"
                    .to_string(),
            );
        }
        Err(error) => return Err(format!("unreadable stats blob: {error:#}")),
    };
    if let Err(reason) = vortex_index::validate_spliceable(&stats, entries.len()) {
        return Err(format!("stats not spliceable: {reason}"));
    }
    // §4 region decomposition: a ts_desc input is ONE desc run; a concat
    // input contributes its own proven region table; a concat input without
    // one stays copyable but poisons the output's region table (fail-open).
    let regions = reader
        .ts_desc_row_ranges()
        .map(|ranges| ranges.iter().map(|r| r.end - r.start).collect());
    Ok(PassthroughSplice {
        zone_entries: entries,
        stats,
        regions,
        widen,
    })
}

/// Copy one qualified input through the #51c passthrough: splice its zone
/// table, then stream its docs chunks in stored form into the writer.
/// Returns `(encoded chunks copied, sliced column-windows canonicalized)` —
/// the second is the M18 deterministic slice guard's per-input count (a
/// window cutting inside one column's stored leaf re-encodes that column
/// window instead of copying a form that cannot survive serialize). Pure
/// copy since M12 — the #52 bloom-only coverage scan runs BEFORE the copy
/// loop, in parallel across inputs ([`scan_bloom_coverage`] via
/// [`bloom_scan_fields_for_input`]); the HEAL passthrough never scanned
/// here anyway (its index-building decoded scan hashes every bloom-only
/// value).
fn copy_passthrough_input(
    key: &str,
    data: Arc<dyn vortex_index::VixRangeSource>,
    timestamps: &Int64Array,
    splice: &PassthroughSplice,
    plan: &MergePlan,
    writer: &mut VixWriter,
) -> Result<(usize, u64), PassthroughFailure> {
    use PassthroughFailure::{BeforePush, Poisoned};
    let docs = VixDocs::open_ranged(data)
        .map_err(|e| BeforePush(anyhow::anyhow!("open core file {key}: {e:#}")))?;
    plan.check_cancel_context("encoded run before", key)
        .map_err(BeforePush)?;

    // run bounds from the already-materialized timestamp column — never
    // from the input's footer stats
    let rows = timestamps.len() as u64;
    let (Some(ts_min), Some(ts_max)) = (
        arrow::compute::min(timestamps),
        arrow::compute::max(timestamps),
    ) else {
        return Err(BeforePush(anyhow::anyhow!(
            "core file {key}: _timestamp range of a non-empty input is undefined"
        )));
    };
    writer
        .begin_docs_encoded_run(
            rows,
            ts_min,
            ts_max,
            &splice.zone_entries,
            &splice.stats,
            splice.regions.as_deref(),
        )
        .map_err(|e| BeforePush(anyhow::anyhow!("core file {key}: {e:#}")))?;
    // from here on the writer owns spliced zone entries and (after the
    // first push) stored chunks: failures are fatal to the whole merge
    let mut chunks = 0usize;
    let widen = &splice.widen;
    let sliced_windows = docs
        .scan_docs_encoded_chunks(&mut |chunk| {
            plan.check_cancel_context("writer push encoded chunk", key)?;
            chunks += 1;
            // M17: chunk-level surgery into the output union (identity = free)
            writer.push_docs_encoded_chunk(widen.widen(chunk)?)
        })
        .map_err(|e| {
            Poisoned(anyhow::anyhow!(
                "copy encoded docs chunks of core file {key}: {e:#}"
            ))
        })?;
    if !widen.is_identity() {
        log::debug!(
            "vix merge: input {key} chunks widened into the output union ({} null-synthesized \
             columns per chunk)",
            widen.null_columns()
        );
    }
    writer
        .finish_docs_encoded_run()
        .map_err(|e| Poisoned(anyhow::anyhow!("core file {key}: {e:#}")))?;
    Ok((chunks, sliced_windows))
}

/// M12 double-hash elimination: the #52 bloom-only fields whose values the
/// k-way dictionary walk CANNOT absorb from `reader`'s input — the input
/// demoted them itself (`bloom` marker: no value terms in its dictionary),
/// never had term capability for them, or stamped them PARTIAL (its
/// dictionary is knowingly incomplete; the docs column has every value).
/// Only these need the projected coverage scan. Dictionary-covered fields
/// are absorbed exactly once by `observe_bloom_only_key` during the index
/// merge — before M12 the scan re-hashed them off the docs columns row by
/// row (a merge-time AUTO demotion like `duration` was hashed 16M times
/// into a set the dict walk had already completed; the dedupe hid it from
/// the output bytes while the CPU dominated the merge wall).
fn bloom_scan_fields_for_input(writer: &VixWriter, reader: &VixReader) -> Vec<String> {
    writer
        .bloom_only_fields()
        .into_iter()
        .filter(|name| !reader.has_term_capability(name) || reader.partial_fields().contains(name))
        .collect()
}

/// M12/M17: one input's composite-bloom coverage scan into a detached
/// hasher — exactly the hasher's fields. PRESENT columns hash off their
/// ENCODED chunks (M17 item 2 — dict chunks decode only their dictionary
/// and hash each referenced distinct value once, FSST chunks hash raw
/// slices off one bulk decompress, other encodings keep the canonical
/// per-row path; the M12 scan was decode-bandwidth-bound at 8 workers and
/// this removes the per-row string materialization for the dominant
/// encodings); fields with NO docs column keep the projected `_source`
/// scan (#51c-d). One shared per-value policy everywhere, so any path mix
/// derives bit-identical coverage. Runs on a scan worker thread; the
/// caller folds the hasher into the writer (set union —
/// order-independent) and aggregates the returned per-encoding-class
/// census into the merge summary (the item-2 prod probe).
fn scan_bloom_coverage(
    key: &str,
    data: Arc<dyn vortex_index::VixRangeSource>,
    hasher: &mut vortex_index::BloomOnlyHasher,
) -> Result<vortex_index::BloomEncodingCensus, anyhow::Error> {
    let docs =
        VixDocs::open_ranged(data).map_err(|e| anyhow::anyhow!("open core file {key}: {e:#}"))?;
    let input_schema = docs.schema();
    let (present, missing): (Vec<String>, Vec<String>) = hasher
        .field_names()
        .into_iter()
        .partition(|name| input_schema.field_with_name(name).is_ok());
    let mut census = vortex_index::BloomEncodingCensus::default();
    for name in &present {
        census.absorb(docs.hash_bloom_only_encoded(hasher, name).map_err(|e| {
            anyhow::anyhow!("bloom-only encoded scan of {name:?} in core file {key}: {e:#}")
        })?);
    }
    if !missing.is_empty() {
        let projection = vec![SOURCE_COL_NAME.to_string()];
        docs.scan_docs(Some(&projection), None, None, &mut |batch| {
            let source = batch.column_by_name(SOURCE_COL_NAME).ok_or_else(|| {
                anyhow::anyhow!("bloom-only scan lost the {SOURCE_COL_NAME:?} column")
            })?;
            hasher.hash_source(source, &missing)?;
            Ok(())
        })
        .map_err(|e| anyhow::anyhow!("bloom-only coverage scan of core file {key}: {e:#}"))?;
    }
    Ok(census)
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
///
/// #51c HEAL passthrough: when the gate + every-input qualification passes
/// ([`qualify_heal_passthrough`] — the dominant compactor shape, a
/// single-file heal of an index-off L0 file, is exactly this), the decoded
/// scan still runs and builds the ENTIRE index exactly as above, but the
/// docs rows are NOT re-encoded — each input's already-compressed docs
/// chunks are copied verbatim instead (docs re-compression is ~2/3 of a
/// heal's CPU). Any failure after qualification restarts the standard
/// rebuild from scratch with a fresh writer (never a partial output).
fn rebuild_over_sources(
    inputs: &[MergeInput],
    sources: &[MergeSource],
    plan: &MergePlan,
) -> Result<MergedCoreFile, anyhow::Error> {
    // Compatibility entry points acquire internally. Production compaction
    // uses the admitted APIs so this wait occurs before rebuild execution.
    let _rebuild_permit = REBUILD_GATE.acquire_with_cancellation(plan.cancellation.as_ref())?;
    rebuild_over_sources_admitted(inputs, sources, plan)
}

fn rebuild_over_sources_admitted(
    inputs: &[MergeInput],
    sources: &[MergeSource],
    plan: &MergePlan,
) -> Result<MergedCoreFile, anyhow::Error> {
    let timestamps = read_timestamp_columns(inputs, sources, plan.cancellation.as_ref())?;
    let dropped_rows = count_degenerate_ts_rows(&timestamps);
    let timestamps: Vec<Int64Array> = if dropped_rows == 0 {
        timestamps
    } else {
        timestamps
            .iter()
            .map(|ts| Int64Array::from_iter_values(ts.values().iter().copied().filter(|t| *t > 0)))
            .collect()
    };
    // #51c-c: a concat-order input's rows are declared unsorted — the k-way
    // merge order is meaningless over it, so the WHOLE rebuild runs in
    // concatenation order (concat inputs are always legal). Unlike the fast
    // path, the rebuild's forced concatenation needs no passthrough
    // qualification: unqualified inputs simply decode, and the coupled
    // pushes below store them at the same concatenated positions.
    let forced_concat = first_concat_order_input(inputs, sources);
    let (order, natural_input_order) = if forced_concat.is_some() {
        // Never run merge_order over an unsorted input: its maps are garbage.
        let input_order = concat_input_order(inputs, timestamps.as_slice());
        (
            RebuildRowOrder::Runs(concat_runs(&input_order, &timestamps)),
            None,
        )
    } else if let Some(input_order) = strict_disjoint_input_order(&timestamps) {
        (
            RebuildRowOrder::Runs(concat_runs(&input_order, &timestamps)),
            Some(input_order),
        )
    } else {
        let maps = merge_order(&timestamps);
        if let Some(offsets) = contiguous_offsets(&maps) {
            let mut input_order: Vec<usize> = (0..inputs.len()).collect();
            input_order.sort_unstable_by_key(|&input| offsets[input]);
            (
                RebuildRowOrder::Runs(concat_runs(&input_order, &timestamps)),
                Some(input_order),
            )
        } else {
            (
                RebuildRowOrder::Interleave(merge_order_inverse(&maps)),
                None,
            )
        }
    };
    let expected_rows: usize = sources
        .iter()
        .map(|source| source.row_count() as usize)
        .sum::<usize>()
        - dropped_rows as usize;
    if order.rows() != expected_rows {
        return Err(anyhow::anyhow!(
            "merge_core_files ordered {} rows, expected {expected_rows}",
            order.rows()
        ));
    }

    if let Some(heal) = qualify_heal_passthrough(
        inputs,
        sources,
        plan,
        &timestamps,
        natural_input_order.as_deref(),
        dropped_rows,
    ) {
        match rebuild_with_docs_passthrough(inputs, plan, &timestamps, heal) {
            Ok(result) => return Ok(result),
            // Fallback-silent-safe: the qualified attempt built nothing
            // durable (its writer, spool and spill die with it) — restart
            // the standard rebuild below with a fresh writer. Loud, because
            // qualification said this could not happen.
            Err(error) => log::warn!(
                "vix merge: heal docs passthrough failed after qualification; restarting the \
                 standard rebuild (decode + re-encode): {error:#}"
            ),
        }
    }

    // The standard rebuild: decode + re-encode every row in `order`. Under a
    // forced concatenation (#51c-c) the writer is stamped concat — its rows
    // are stored in concatenation order, NOT globally sorted.
    let mut writer_opts = plan.opts.clone();
    writer_opts.concat_row_order = forced_concat.is_some();
    let mut writer = VixWriter::new(&plan.writer_schema, writer_opts, plan.store_original);
    writer.set_expected_max_rows_for_auto_demotion(expected_rows as u64)?;
    let mut perf = MergePerfStats::default();
    if let RebuildRowOrder::Interleave(interleave) = &order {
        perf.order_entries_materialized = interleave.len() as u64;
    }
    let streamed = if plan.derive_from_columns {
        // #46: every input is index-off all-columnar — assemble the window
        // into a RecordBatch and run the COLUMN-driven derivation (the
        // move-job path, whose byte-parity with the `_source` derivation is
        // the pinned contract). `project_docs` stores only the docs-schema
        // columns; the extra derivation columns feed terms and vanish.
        log::info!(
            "vix merge: rebuild derives terms from {} columns (rewrite _source: {})",
            plan.preserved.len(),
            plan.rewrite_source_from_columns,
        );
        let mut push = |ts: &Int64Array,
                        cs: &[(String, ArrayRef)],
                        source: &StringArray,
                        original: Option<&StringArray>| {
            let batch = derivation_window_batch(plan, ts, cs)?;
            let rewritten_source = plan
                .rewrite_source_from_columns
                .then(|| synthesize_source(&batch))
                .transpose()?;
            writer.push_batch_with_source(
                &batch,
                rewritten_source.as_ref().unwrap_or(source),
                original,
            )?;
            Ok(())
        };
        match &order {
            RebuildRowOrder::Runs(runs) => stream_concat_windows(inputs, plan, runs, &mut push)?,
            RebuildRowOrder::Interleave(order) => {
                stream_merge_windows(inputs, plan, order, &mut push)?
            }
        }
    } else {
        let mut push = |ts: &Int64Array,
                        cs: &[(String, ArrayRef)],
                        source: &StringArray,
                        original: Option<&StringArray>| {
            writer.push_docs_rows(ts, cs, source, original)
        };
        match &order {
            RebuildRowOrder::Runs(runs) => stream_concat_windows(inputs, plan, runs, &mut push)?,
            RebuildRowOrder::Interleave(order) => {
                stream_merge_windows(inputs, plan, order, &mut push)?
            }
        }
    };
    let docs_batches = streamed.batches;
    perf.interleaved_columns = streamed.perf.interleaved_columns;
    perf.staged_empty_arrays = streamed.perf.staged_empty_arrays;
    log::debug!("vix merge: rebuild staged docs in {docs_batches} bounded batches");

    plan.check_cancel("writer finish before")?;
    let (output, index, stats) = writer.finish_output()?;
    plan.check_cancel("writer finish after")?;
    Ok(MergedCoreFile {
        output,
        index,
        stats,
        used_index_merge: false,
        docs_batches,
        dropped_rows,
        docs_passthrough_inputs: 0,
        concat_order: forced_concat.is_some(),
        // the standard rebuild decodes everything: nothing copies, nothing
        // can carry a foreign encoding into the encoder
        docs_sliced_windows: 0,
        docs_failopen_chunks: 0,
        terms_from_columns: plan.derive_from_columns,
        perf,
    })
}

/// Assemble one #46 column-derivation window batch (`_timestamp` + the
/// preserved ⊕ derivation columns) for [`VixWriter::push_batch_with_source`]
/// — shared by the standard rebuild and the heal-passthrough scan so the two
/// derive terms from byte-identical batches.
fn derivation_window_batch(
    plan: &MergePlan,
    ts: &Int64Array,
    cs: &[(String, ArrayRef)],
) -> Result<RecordBatch, anyhow::Error> {
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(cs.len() + 1);
    arrays.push(Arc::new(ts.clone()) as ArrayRef);
    for (_, column) in cs {
        arrays.push(Arc::clone(column));
    }
    Ok(RecordBatch::try_new(
        Arc::clone(&plan.writer_schema),
        arrays,
    )?)
}

/// One qualified #51c heal-passthrough plan over a rebuild's inputs: the
/// passthrough writer (constructed with
/// [`VixWriterOptions::docs_passthrough`] so its docs encoder can accept
/// copied chunks), the inputs in output-row order, and each input's verbatim
/// zone table.
struct HealPassthrough {
    writer: VixWriter,
    /// Input indices in OUTPUT-ROW order: sorted by contiguous run offset
    /// when the merge order is a natural concatenation (disjoint inputs),
    /// or the deterministic #51c-c concatenation order (min `_timestamp`
    /// DESC, ties by key) when concatenation is forced/allowed over
    /// overlapping inputs. Either way the index-building scan consumes the
    /// inputs in exactly this order, so its doc ids equal the chunk-copy
    /// positions.
    input_order: Vec<usize>,
    /// Per input (parallel to `inputs`): `Some` = the zone table + column
    /// stats to splice and the widen plan (chunk copy); `None` = this input
    /// FAILED qualification (type flip, stats-less, unreadable index) and
    /// takes the decode + re-encode path at its concatenated position (M17
    /// per-input fail-open — counted in the merge summary).
    splices: Vec<Option<PassthroughSplice>>,
    /// #51c-c: the output is concatenation-ordered over OVERLAPPING inputs
    /// (stamped `row_order=concat`); `false` = the disjoint case, whose
    /// concatenation IS globally sorted.
    concat_order: bool,
}

/// #51c heal-passthrough gate + per-input qualification for a REBUILD.
/// `None` (with the reason logged at debug) keeps today's full rebuild.
///
/// The doc-id/row-order invariant this enforces: the rebuild's index build
/// assigns doc ids sequentially over the scanned rows, and the copied docs
/// chunks must hold THE SAME rows at THE SAME positions. That holds exactly
/// when
/// - no row is cleansed (`dropped_rows == 0` — degenerate-`_timestamp` cleansing filters scan rows
///   the copied chunks would still carry),
/// - the scan order is a pure concatenation of the inputs: naturally so for disjoint inputs
///   ([`contiguous_offsets`]; a single-input heal is trivially contiguous), and — #51c-c — BY
///   CHOICE for overlapping inputs, whose scan then runs in the deterministic concatenation order
///   and the output is stamped `row_order=concat`, and
/// - each input's stored chunks cover its timestamp column's rows exactly, in row order
///   ([`qualify_passthrough_input`]'s row-count and zone-table checks; the encoded-run API
///   re-verifies per copied chunk).
///
/// The SCHEMA gate rides in [`qualify_passthrough_input`]: the input's docs
/// schema must WIDEN into the output's (M17 — shared columns identical at
/// the stored dtype; output-only columns synthesize as all-null constants,
/// which is exactly what the decode path would store for them). Only a
/// genuine type flip (or a stats-less/unreadable input) forces that input
/// onto the per-input decode fail-open.
///
/// `natural_input_order` is present when timestamp ranges prove that the
/// sorted output is already a pure concatenation. Its absence selects the
/// deterministic concat-order heal (overlap or a concat-order input).
fn qualify_heal_passthrough(
    inputs: &[MergeInput],
    sources: &[MergeSource],
    plan: &MergePlan,
    timestamps: &[Int64Array],
    natural_input_order: Option<&[usize]>,
    dropped_rows: u64,
) -> Option<HealPassthrough> {
    if plan.caps.force_decode {
        return None;
    }
    if plan.rewrite_source_from_columns {
        // The encoded docs chunks still contain the old `_source`; copying
        // them would defeat the durable fixed-type migration.
        return None;
    }
    if !plan.opts.index_enabled {
        // An index-off plan's rebuild is a pure docs copy already (no terms
        // to split from the store); it only lands here for degenerate or
        // unreadable-index inputs — keep those on the conservative path.
        log::debug!("vix merge: heal passthrough disqualified: index-off plan");
        return None;
    }
    if dropped_rows > 0 {
        log::debug!(
            "vix merge: heal passthrough disqualified: {dropped_rows} degenerate-_timestamp \
             row(s) must be cleansed, which a chunk copy cannot express"
        );
        return None;
    }
    // Output-row order: the natural concatenation when the inputs are
    // disjoint; otherwise (overlap, or a concat input that skipped the
    // k-way order entirely) the #51c-c concatenation order — the output's
    // rows are then NOT globally sorted and the file is stamped concat.
    let concat_order = natural_input_order.is_none();

    let mut writer_opts = plan.opts.clone();
    writer_opts.docs_passthrough = true;
    writer_opts.concat_row_order = concat_order;
    let mut writer = VixWriter::new(&plan.writer_schema, writer_opts, plan.store_original);
    let expected_rows = timestamps.iter().map(|values| values.len() as u64).sum();
    if let Err(error) = writer.set_expected_max_rows_for_auto_demotion(expected_rows) {
        log::warn!(
            "vix merge: heal passthrough disqualified: could not configure the exact output-row \
             bound for early AUTO bloom-only demotion: {error:#}"
        );
        return None;
    }
    // M17 per-input verdicts: a qualification miss no longer kills the
    // whole copy — that input decodes + re-encodes at its concatenated
    // position while every qualified input still copies (the index scan
    // covers every row either way, so doc ids stay position-exact).
    let mut splices: Vec<Option<PassthroughSplice>> = Vec::with_capacity(inputs.len());
    let mut qualified = 0usize;
    for (index, source) in sources.iter().enumerate() {
        let verdict = match source {
            MergeSource::Indexed(reader) => {
                qualify_passthrough_input(reader, timestamps[index].len() as u64, &writer)
            }
            MergeSource::DocsOnly(_) => Err(
                "unreadable index (DocsOnly source) — its metadata cannot be trusted for a \
                 verbatim chunk copy"
                    .to_string(),
            ),
        };
        match verdict {
            Ok(splice) => {
                qualified += 1;
                splices.push(Some(splice));
            }
            Err(reason) => {
                log::debug!(
                    "vix merge: rebuild input {} takes the decode path (of a docs-copy \
                     rebuild): {reason}",
                    inputs[index].0,
                );
                splices.push(None);
            }
        }
    }
    if qualified == 0 {
        log::debug!(
            "vix merge: docs-copy rebuild disqualified: none of the {} inputs qualified",
            inputs.len()
        );
        return None;
    }
    let input_order = natural_input_order
        .map(<[usize]>::to_vec)
        .unwrap_or_else(|| concat_input_order(inputs, timestamps));
    if concat_order {
        log::debug!(
            "vix merge: heal passthrough takes the #51c-c concatenation order over {} \
             overlapping inputs",
            inputs.len()
        );
    }
    Some(HealPassthrough {
        writer,
        input_order,
        splices,
        concat_order,
    })
}

/// The #51c/M17 docs-copy rebuild body (see [`rebuild_over_sources`]): the
/// SAME decoded windowed scan as the standard rebuild feeds ONLY the index
/// (terms, key terms, #52 bloom-only hashes, oversize/partial accounting —
/// via the writer's index-only pushes), then every input's docs land in
/// output-row order: QUALIFIED inputs copy their encoded chunks verbatim
/// (widened by null-column synthesis when the output union is wider — the
/// M17 gen-1 encode-once path; zone tables spliced, `row_count`/ts range
/// advancing on the copy side), UNQUALIFIED inputs decode + re-encode
/// through store-only pushes at the same positions (per-input fail-open,
/// counted in the merge summary). The writer's finish refuses any
/// index/docs row-count divergence.
///
/// The scan runs in `heal.input_order`'s concatenation — for disjoint
/// inputs that IS the merge order; for a #51c-c concatenation heal it is
/// the output's (unsorted) row order, so the index doc ids match the
/// stored positions by construction either way.
///
/// Errors leave nothing durable — the caller restarts the standard rebuild.
fn rebuild_with_docs_passthrough(
    inputs: &[MergeInput],
    plan: &MergePlan,
    timestamps: &[Int64Array],
    heal: HealPassthrough,
) -> Result<MergedCoreFile, anyhow::Error> {
    let HealPassthrough {
        mut writer,
        input_order,
        splices,
        concat_order,
    } = heal;
    let runs = concat_runs(&input_order, timestamps);
    let ordered_rows: usize = runs.iter().map(|run| run.rows).sum();

    // 1) Index build: today's decoded scan, docs staging detached.
    let started = std::time::Instant::now();
    let scan_windows = if plan.derive_from_columns {
        // This detached pass exists ONLY to derive terms from columns, so it
        // never projects `_source`. Inputs that fail encoded docs splicing
        // read `_source` exactly once later in their store-only re-encode.
        let mut scan_plan = plan.clone();
        scan_plan.scan_source = false;
        log::info!(
            "vix merge: heal-passthrough rebuild derives terms from {} columns \
             (index-off inputs, _source projected: {})",
            plan.preserved.len(),
            scan_plan.scan_source
        );
        stream_concat_windows(inputs, &scan_plan, &runs, |ts, cs, source, original| {
            let batch = derivation_window_batch(&scan_plan, ts, cs)?;
            writer.push_batch_with_source_index_only(&batch, source, original)?;
            Ok(())
        })?
        .batches
    } else {
        stream_concat_windows(inputs, plan, &runs, |ts, cs, source, original| {
            writer.push_docs_rows_index_only(ts, cs, source, original)
        })?
        .batches
    };
    log::debug!(
        "vix merge: heal passthrough indexed {} rows in {:?} ({scan_windows} windows, \
         concat_order: {concat_order})",
        ordered_rows,
        started.elapsed()
    );

    // 2) Docs store: each input lands at its concatenated position in
    // output-row order (the same order the scan consumed rows — the
    // contiguity proof). Qualified inputs copy their encoded chunks
    // verbatim (widened into the output union when schemas differ, M17);
    // an unqualified input decodes and RE-ENCODES its rows through the
    // store-only push — the per-input fail-open the merge summary counts.
    // A mid-copy failure after chunks reached the writer aborts the
    // attempt and the caller restarts the standard rebuild with a fresh
    // writer.
    let started = std::time::Instant::now();
    let mut docs_batches = 0usize;
    let mut copied_inputs = 0usize;
    let mut reencoded_inputs = 0usize;
    let mut widened_inputs = 0usize;
    let mut docs_sliced_windows = 0u64;
    std::thread::scope(|scope| -> Result<(), anyhow::Error> {
        // Decode streams spawn only for the fail-open inputs, and — M23b —
        // only when that input's drain STARTS (upfront spawns buffered every
        // not-yet-reached fail-open input ~fully decoded while earlier
        // inputs copied/drained; same M23 shape, rare here because fail-open
        // inputs are the exception). Input-by-input drain: the channel bound
        // caps the one active stream, no admission gate needed.
        for &index in &input_order {
            let (key, data, _) = &inputs[index];
            match &splices[index] {
                Some(splice) => {
                    // the index scan above already hashed every #52
                    // bloom-only value (cs columns + _source) — the copy
                    // never re-hashes
                    let (chunks, sliced) = match copy_passthrough_input(
                        key,
                        Arc::clone(data),
                        &timestamps[index],
                        splice,
                        plan,
                        &mut writer,
                    ) {
                        Ok(counts) => counts,
                        Err(PassthroughFailure::BeforePush(error))
                        | Err(PassthroughFailure::Poisoned(error)) => {
                            return Err(error.context(format!(
                                "heal passthrough copy of core file {key} (input {index})"
                            )));
                        }
                    };
                    log::debug!(
                        "vix merge: docs-copy rebuild copied input {key} ({chunks} encoded \
                         chunks, {sliced} sliced column-windows canonicalized)"
                    );
                    docs_batches += chunks;
                    copied_inputs += 1;
                    docs_sliced_windows += sliced;
                    if !splice.widen.is_identity() {
                        widened_inputs += 1;
                    }
                }
                None => {
                    let mut cursor = InputCursor::new(
                        key.clone(),
                        spawn_input_stream(scope, key, Arc::clone(data), plan),
                    );
                    docs_batches += drain_input_cursor(&mut cursor, plan, &mut writer)?;
                    reencoded_inputs += 1;
                }
            }
        }
        Ok(())
    })?;

    let store_elapsed = started.elapsed();

    // M18: the fail-open counter lives past finish (the encoder worker
    // finishes inside finish_output, so the count is final only after it)
    let failopen = writer.docs_failopen_counter();
    plan.check_cancel("writer finish before")?;
    let (output, index, stats) = writer.finish_output()?;
    plan.check_cancel("writer finish after")?;
    let docs_failopen_chunks = failopen.load(std::sync::atomic::Ordering::Relaxed);
    // The merge summary (M17 + M18): the fail-open counts are the signals
    // the encode-once design watches in prod — counts only, no key lists.
    log::info!(
        "vix merge: docs-copy rebuild stored {} inputs in {store_elapsed:?}: copied \
         {copied_inputs} ({widened_inputs} schema-widened), re-encoded {reencoded_inputs} \
         (fail-open), re-encoded {docs_failopen_chunks} chunk(s) (fail-open), \
         sliced-canonicalized {docs_sliced_windows} column-window(s), concat_order: \
         {concat_order}",
        input_order.len(),
    );
    Ok(MergedCoreFile {
        output,
        index,
        stats,
        used_index_merge: false,
        docs_batches,
        dropped_rows: 0,
        docs_passthrough_inputs: copied_inputs,
        concat_order,
        docs_sliced_windows,
        docs_failopen_chunks,
        terms_from_columns: plan.derive_from_columns,
        perf: MergePerfStats::default(),
    })
}

#[cfg(test)]
mod tests {
    use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};
    use datafusion::datasource::MemTable;
    use vortex_index::{RowOrder, VixDocs, VixQuery, VixReader};

    use super::*;
    // The scan-extraction twin (`json_get_* + cast`, by construction): the
    // parity ORACLE for tests that pin index answers against the scan-side
    // `_source` fallback semantics. Production merges no longer derive
    // columns from `_source` (v2 null-fill), so this import is test-only.
    use crate::search::datafusion::vix_format::derive_cs_column_from_source;

    /// One fabricated file's (data, sidecar) bytes — what every test
    /// builder returns since the v3 split.
    type BuiltPair = (bytes::Bytes, Option<bytes::Bytes>);

    #[test]
    fn split_batch_preflights_the_next_row_against_the_byte_cap() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("body", DataType::Utf8, false),
        ]));
        let escaped = "\u{0001}".repeat(40);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![3, 2, 1])),
                Arc::new(StringArray::from(vec![
                    escaped.clone(),
                    escaped,
                    "c".to_string(),
                ])),
            ],
        )
        .unwrap();
        let parts = split_batch_by_bytes(
            &batch,
            BatchCaps {
                rows: 10,
                bytes: 1_000,
                ..BatchCaps::default()
            },
            true,
        );

        assert_eq!(
            parts.iter().map(RecordBatch::num_rows).collect::<Vec<_>>(),
            vec![1, 1, 1]
        );
    }

    /// Open one fabricated pair as a full reader.
    fn open_pair(pair: &BuiltPair) -> VixReader {
        VixReader::open_with_index(pair.0.clone(), pair.1.clone()).unwrap()
    }

    /// FILE-LEVEL fold of one output's per-column chunk stats (H2): per
    /// column, (total present, global min, global max) — the aggregate the
    /// §11 pruning gate compares between a passthrough (SPLICED stats) and
    /// a forced-decode rebuild (FRESH stats). Chunk windows legitimately
    /// differ between the two (spliced tables keep the inputs' windows), so
    /// the fold is the comparable surface; presence counts compare exactly.
    #[allow(clippy::type_complexity)]
    fn file_level_stats(
        result: &MergedCoreFile,
    ) -> (
        std::collections::BTreeMap<
            String,
            (
                u64,
                Option<vortex_index::StatValue>,
                Option<vortex_index::StatValue>,
            ),
        >,
        std::collections::BTreeMap<String, Option<u64>>,
    ) {
        let docs = VixDocs::open(bytes::Bytes::from(result.output.to_bytes().unwrap())).unwrap();
        let presence = docs
            .column_presence()
            .iter()
            .cloned()
            .collect::<std::collections::BTreeMap<_, _>>();
        let stats = docs
            .spliceable_stats()
            .unwrap()
            .expect("every non-empty output carries a stats blob");
        let mut folded = std::collections::BTreeMap::new();
        for (name, table) in &stats.chunks.columns {
            let mut present = 0u64;
            let mut min: Option<vortex_index::StatValue> = None;
            let mut max: Option<vortex_index::StatValue> = None;
            let less = |a: &vortex_index::StatValue, b: &vortex_index::StatValue| -> bool {
                use vortex_index::StatValue::*;
                match (a, b) {
                    (I64(x), I64(y)) => x < y,
                    (U64(x), U64(y)) => x < y,
                    (F64(x), F64(y)) => x < y,
                    (Bool(x), Bool(y)) => x < y,
                    (Str(x), Str(y)) => x < y,
                    _ => false,
                }
            };
            for entry in table.chunks.iter().flatten() {
                present += entry.present;
                if let Some(value) = &entry.min {
                    if min.as_ref().is_none_or(|m| less(value, m)) {
                        min = Some(value.clone());
                    }
                }
                if let Some(value) = &entry.max {
                    if max.as_ref().is_none_or(|m| less(m, value)) {
                        max = Some(value.clone());
                    }
                }
            }
            folded.insert(name.clone(), (present, min, max));
        }
        (folded, presence)
    }

    /// The §11 splice gate: a passthrough output's (spliced) stats must
    /// equal the forced-decode rebuild's (fresh) stats at the file-level
    /// fold, presence counts exactly — the v1 stats-loss regression check.
    fn assert_stats_splice_parity(
        passthrough: &MergedCoreFile,
        rebuild: &MergedCoreFile,
        context: &str,
    ) {
        let (fast_fold, fast_presence) = file_level_stats(passthrough);
        let (rebuild_fold, rebuild_presence) = file_level_stats(rebuild);
        assert_eq!(
            fast_presence, rebuild_presence,
            "{context}: presence counts must survive the splice exactly"
        );
        assert_eq!(
            fast_fold.keys().collect::<Vec<_>>(),
            rebuild_fold.keys().collect::<Vec<_>>(),
            "{context}: the same columns must carry chunk stats"
        );
        for (name, folded) in &fast_fold {
            assert_eq!(
                folded, &rebuild_fold[name],
                "{context}: file-level fold of column {name:?} must match the fresh stats"
            );
        }
    }

    /// Open one merge result's (data, sidecar) outputs as a full reader.
    fn open_merged(result: &MergedCoreFile) -> VixReader {
        VixReader::open_with_index(
            bytes::Bytes::from(result.output.to_bytes().unwrap()),
            result.index.clone().map(bytes::Bytes::from),
        )
        .unwrap()
    }

    /// Wrap fabricated in-memory (data, sidecar) pairs as ranged merge
    /// inputs (the merge paths take [`MergeInput`] — production feeds
    /// cache-ladder sources).
    fn as_inputs(v: &[(String, BuiltPair)]) -> Vec<MergeInput> {
        v.iter()
            .map(|(key, (data, index))| {
                (
                    key.clone(),
                    vortex_index::BytesRangeSource::new(key.clone(), data.clone()),
                    index.as_ref().map(|bytes| {
                        vortex_index::BytesRangeSource::new(format!("{key}.vxi"), bytes.clone())
                    }),
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
            StreamType::Logs,
            schema,
            vec![table],
            &["log".to_string()],
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
            StreamType::Logs,
            schema,
            vec![table],
            &["log".to_string()],
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

        let reader = VixReader::open_with_index(
            bytes::Bytes::from(result.data),
            result.index.map(bytes::Bytes::from),
        )
        .unwrap();
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
            StreamType::Logs,
            schema,
            vec![table],
            &["log".to_string()],
            &[],
            false,
            0,
        )
        .await
        .unwrap();
        let reader = VixReader::open_with_index(
            bytes::Bytes::from(result.data),
            result.index.map(bytes::Bytes::from),
        )
        .unwrap();
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
        original: Option<StringArray>,
    ) -> BuiltPair {
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let source = synthesize_source(&batch).unwrap();
        let mut writer = VixWriter::new(
            &schema,
            core_writer_options(fts, Vec::new(), true),
            original.is_some(),
        );
        writer
            .push_batch_with_source(&batch, &source, original.as_ref())
            .unwrap();
        let (data, index) = writer.finish().unwrap();
        (bytes::Bytes::from(data), index.map(bytes::Bytes::from))
    }

    /// [`build_core_file`] through the test-support UNGUARDED finish:
    /// fabricates a pre-guard-era stored file whose rows may carry
    /// `_timestamp <= 0` — the poison population compaction-time cleansing
    /// drops. Production writers refuse to build such files.
    fn build_poisoned_core_file(
        fields: Vec<Field>,
        columns: Vec<ArrayRef>,
        fts: &[String],
    ) -> BuiltPair {
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let source = synthesize_source(&batch).unwrap();
        let mut writer = VixWriter::new(&schema, core_writer_options(fts, Vec::new(), true), false);
        writer
            .push_batch_with_source(&batch, &source, None)
            .unwrap();
        let (data, index) =
            vortex_index::test_support::finish_ignoring_timestamp_guard(writer).unwrap();
        (bytes::Bytes::from(data), index.map(bytes::Bytes::from))
    }

    /// #27 armor: a merge input whose rows violate the `_timestamp` DESC
    /// storage convention is rejected loudly by BOTH merge flavors — a
    /// silent pass-through would corrupt the merged order, the
    /// first/last-row stats the scan layer derives, and top-N selection.
    #[tokio::test]
    async fn merge_rejects_ascending_input() {
        let fts = vec!["log".to_string()];
        let make = |ts: Vec<i64>, logs: Vec<&str>| {
            build_core_file(
                vec![
                    Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
                    Field::new("log", DataType::Utf8, true),
                ],
                vec![
                    Arc::new(Int64Array::from(ts)),
                    Arc::new(StringArray::from(logs)),
                ],
                &fts,
                None,
            )
        };
        let good = make(vec![100, 50], vec!["a", "b"]);
        let bad = make(vec![10, 60, 40], vec!["x", "y", "z"]);
        let latest_schema = Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("log", DataType::Utf8, true),
        ]);
        let inputs = vec![("good.vix".to_string(), good), ("bad.vix".to_string(), bad)];
        let err = merge_core_files(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &fts,
            &[],
        )
        .expect_err("ascending input must be rejected");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("violates the _timestamp DESC row order") && msg.contains("bad.vix"),
            "unexpected error: {msg}"
        );
        let err = merge_core_files_rebuild(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &fts,
            &[],
        )
        .expect_err("ascending input must be rejected by the rebuild flavor");
        assert!(
            format!("{err:#}").contains("violates the _timestamp DESC row order"),
            "rebuild flavor shares the ordering armor"
        );
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
        // force_decode: this test pins the SORTED-INTERLEAVE arm (table
        // doc-id maps, tie-breaking, per-window interleave). Since M17 the
        // production default for these schema-differing overlapping inputs
        // is the widened concat chunk copy — covered by the gen1_docs_copy
        // and concat differential tests.
        let result = merge_core_files_with_caps(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &fts,
            &[],
            oracle_caps(),
        )
        .unwrap();
        assert_eq!(result.stats.row_count, 7);
        assert!(result.stats.index_size > 0);
        // overlapping inputs take the index-merge fast path (table
        // doc-id maps + row interleave) under the forced decode
        assert!(result.used_index_merge);

        let merged = open_merged(&result);
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
            core_writer_options(&fts, Vec::new(), true),
            false,
        );
        reference_writer
            .push_batch_with_source(&reference_batch, &reference_source, None)
            .unwrap();
        let reference = {
            let (data, index) = reference_writer.finish().unwrap();
            VixReader::open_with_index(bytes::Bytes::from(data), index.map(bytes::Bytes::from))
                .unwrap()
        };

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
            StreamType::Logs,
            &as_inputs(&[(
                "bogus.vix".to_string(),
                (bytes::Bytes::from_static(b"nope"), None),
            )]),
            &latest_schema,
            &[],
            &[],
        )
        .unwrap_err();
        assert!(err.to_string().contains("bogus.vix"), "{err}");

        let err = merge_core_files(StreamType::Logs, &[], &latest_schema, &[], &[]).unwrap_err();
        assert!(err.to_string().contains("no input files"), "{err}");
    }

    /// Full reader-visible equivalence of two core files: same rows in the
    /// same order (every docs column and `_source`), same term set with the
    /// same doc_counts and postings, same field capabilities and partials,
    /// and the same answers to a query battery.
    fn assert_core_files_equivalent(left: &VixReader, right: &VixReader, context: &str) {
        assert_core_files_equivalent_inner(left, right, context, false)
    }

    /// #51c: like [`assert_core_files_equivalent`] but docs columns compare
    /// by LOGICAL row value (null-aware) instead of raw arrow buffers — a
    /// passthrough output's chunks come from a different encode lineage
    /// than the rebuild's, so bytes under NULL slots may legitimately
    /// differ while every observable value is identical.
    fn assert_core_files_equivalent_logical_docs(
        left: &VixReader,
        right: &VixReader,
        context: &str,
    ) {
        assert_core_files_equivalent_inner(left, right, context, true)
    }

    /// One logical (null-masked) row value, printable for assert messages.
    fn logical_value(column: &ArrayRef, row: usize) -> Option<String> {
        use arrow::array::Float64Array;
        if column.is_null(row) {
            return None;
        }
        Some(match column.data_type() {
            DataType::Int64 => column
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row)
                .to_string(),
            DataType::Utf8 => column
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row)
                .to_string(),
            DataType::Utf8View => column
                .as_any()
                .downcast_ref::<StringViewArray>()
                .unwrap()
                .value(row)
                .to_string(),
            DataType::Float64 => {
                let value = column
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(row);
                format!("{value:?}")
            }
            DataType::Boolean => column
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(row)
                .to_string(),
            other => panic!("logical_value: unhandled docs column type {other:?}"),
        })
    }

    fn assert_core_files_equivalent_inner(
        left: &VixReader,
        right: &VixReader,
        context: &str,
        logical_docs: bool,
    ) {
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
            if logical_docs {
                assert_eq!(
                    left_column.len(),
                    right_column.len(),
                    "{context}: docs column {name:?} length"
                );
                for row in 0..left_column.len() {
                    assert_eq!(
                        logical_value(&left_column, row),
                        logical_value(&right_column, row),
                        "{context}: docs column {name:?} row {row}"
                    );
                }
            } else {
                assert_eq!(
                    left_column.to_data(),
                    right_column.to_data(),
                    "{context}: docs column {name:?}"
                );
            }
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
        let queries = query_battery(left);
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

    /// The shared equivalence query battery (every term field + composites),
    /// built from `reference`'s term surface — used positionally by
    /// [`assert_core_files_equivalent_inner`] and by row CONTENT by
    /// [`assert_core_files_content_equivalent`].
    fn query_battery(reference: &VixReader) -> Vec<VixQuery> {
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
        for name in reference.term_field_names() {
            queries.push(key_exists(name));
            if reference.has_term_capability(name) {
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
        if reference.has_term_capability("env") {
            queries.push(VixQuery::Or(vec![
                exact("env", "prod"),
                VixQuery::TokenAnyField {
                    token: b"disk".to_vec(),
                },
            ]));
        }
        queries
    }

    /// #51c-c: every docs row of `reader` rendered as one stable string
    /// (columns in sorted-name order, logical null-aware values) — the unit
    /// of ORDER-INSENSITIVE comparisons between merge outputs whose row
    /// order legitimately differs (concatenation vs sorted interleave).
    fn docs_row_contents(reader: &VixReader) -> Vec<String> {
        let schema = reader.docs_schema().unwrap();
        let mut names: Vec<String> = schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        names.sort();
        let columns: Vec<(String, ArrayRef)> = names
            .into_iter()
            .map(|name| {
                let column = reader.read_docs_column(&name).unwrap();
                (name, column)
            })
            .collect();
        (0..reader.row_count() as usize)
            .map(|row| {
                columns
                    .iter()
                    .map(|(name, column)| format!("{name}={:?}", logical_value(column, row)))
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect()
    }

    /// Render one doc-id list as its rows' CONTENT, sorted — how postings
    /// and query answers compare across differently-ordered outputs.
    fn contents_of(rows: &[String], ids: &[u32]) -> Vec<String> {
        let mut contents: Vec<String> = ids.iter().map(|&id| rows[id as usize].clone()).collect();
        contents.sort();
        contents
    }

    /// #51c-c: reader equivalence between two merge outputs whose ROW ORDER
    /// legitimately differs (a concatenation-order output vs the sorted
    /// rebuild oracle): identical row MULTISETS (logical content), the same
    /// term surface (names, capabilities, value counts, key coverage,
    /// term_count), per-term `doc_count` + matched-row CONTENT sets, the
    /// same [`query_battery`] answers by content and count, and
    /// content-equal `timestamp_range` answers over probe windows spanning
    /// the corpus. This replaces exactly the row-order-dependent pieces of
    /// [`assert_core_files_equivalent`] (positional docs columns, postings
    /// doc ids); everything else is held to equality.
    fn assert_core_files_content_equivalent(left: &VixReader, right: &VixReader, context: &str) {
        assert_eq!(left.row_count(), right.row_count(), "{context}: row_count");
        assert_eq!(
            left.term_count(),
            right.term_count(),
            "{context}: term_count"
        );
        let left_rows = docs_row_contents(left);
        let right_rows = docs_row_contents(right);
        {
            let mut left_sorted = left_rows.clone();
            let mut right_sorted = right_rows.clone();
            left_sorted.sort();
            right_sorted.sort();
            assert_eq!(left_sorted, right_sorted, "{context}: row multisets");
        }

        // term surface parity (order-free metadata)
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

        // the whole term table, postings compared by ROW CONTENT
        let dump = |reader: &VixReader, rows: &[String]| {
            let mut terms: Vec<(Vec<u8>, u64, Vec<String>)> = Vec::new();
            reader
                .for_each_term(&mut |key, doc_count, ids| {
                    terms.push((key.to_vec(), doc_count, contents_of(rows, ids)));
                    Ok(())
                })
                .unwrap();
            terms
        };
        assert_eq!(
            dump(left, &left_rows),
            dump(right, &right_rows),
            "{context}: term table by content"
        );

        // the query battery, answers compared by ROW CONTENT
        for query in &query_battery(left) {
            assert_eq!(
                contents_of(&left_rows, &matching_docs(left, query)),
                contents_of(&right_rows, &matching_docs(right, query)),
                "{context}: query {query:?} by content"
            );
            assert_eq!(
                left.count(query).unwrap(),
                right.count(query).unwrap(),
                "{context}: count {query:?}"
            );
        }

        // timestamp ranges by content — non-monotonic zone tables (the
        // concat shape) must prune to exactly the sorted oracle's rows
        for range in [
            (0i64, i64::MAX),
            (700, 951),
            (725, 726),
            (975, 976),
            (850, 1001),
        ] {
            let left_bits = left.timestamp_range(range.0, range.1).unwrap();
            let right_bits = right.timestamp_range(range.0, range.1).unwrap();
            let ids = |bits: &arrow::buffer::BooleanBuffer| -> Vec<u32> {
                bits.iter()
                    .enumerate()
                    .filter_map(|(doc, set)| set.then_some(doc as u32))
                    .collect()
            };
            assert_eq!(
                contents_of(&left_rows, &ids(&left_bits)),
                contents_of(&right_rows, &ids(&right_bits)),
                "{context}: ts range {range:?} by content"
            );
        }
    }

    /// The differential-test corpus: three feature-dense core files —
    /// fts tokens (shared across files), key terms, a value dense across
    /// every row (`env=prod`), string + numeric cs columns, `_o2_id`,
    /// `_original` (one file), empty strings (fts AND structured — the
    /// empty raw term), non-finite floats (NaN/±Inf: key-term-less but
    /// cs-stored), a NUL-byte value, an oversize value (term skipped, field
    /// untainted since 2026-08-12), and a field only one file knows.
    ///
    /// `ts` supplies each file's descending timestamp column, so callers
    /// choose disjoint or overlapping ranges.
    fn differential_inputs(ts: [Vec<i64>; 3]) -> Vec<(String, BuiltPair)> {
        use arrow::array::Float64Array;
        let fts = vec!["log".to_string()];
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
            let fast = merge_core_files(
                StreamType::Logs,
                &as_inputs(&inputs),
                &latest_schema,
                &fts,
                &[],
            )
            .unwrap();
            assert!(fast.used_index_merge, "{context}: expected the fast path");
            let rebuild = merge_core_files_rebuild(
                StreamType::Logs,
                &as_inputs(&inputs),
                &latest_schema,
                &fts,
                &[],
            )
            .unwrap();
            assert!(!rebuild.used_index_merge);

            assert_eq!(fast.stats.row_count, rebuild.stats.row_count, "{context}");
            assert_eq!(fast.stats.term_count, rebuild.stats.term_count, "{context}");

            let fast_reader = open_merged(&fast);
            let rebuild_reader = open_merged(&rebuild);
            assert_core_files_equivalent(&fast_reader, &rebuild_reader, context);

            // spot-check the merged features on the fast-path file itself:
            // the oversize `huge` value skipped its term WITHOUT tainting
            // (2026-08-12), so the merged file is clean through both
            // strategies — the skipped literal misses, the same field's
            // short value stays exact
            assert!(
                fast_reader.partial_fields().is_empty(),
                "{context}: {:?}",
                fast_reader.partial_fields()
            );
            assert_eq!(
                fast_reader
                    .count(&exact("huge", &"z".repeat(70_000)))
                    .unwrap(),
                0,
                "{context}: the skipped oversize literal misses"
            );
            assert_eq!(
                fast_reader.count(&exact("huge", "small")).unwrap(),
                1,
                "{context}: short values on the same field stay exact"
            );
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
        let latest_schema = differential_latest_schema();
        // This test pins the WINDOWED DECODE flow under byte/row caps —
        // since M17 widens schema-differing inputs into the chunk copy,
        // the decode arms are forced explicitly (copies don't window).
        let tiny = BatchCaps {
            rows: 2,
            bytes: 96,
            force_decode: true,
            ..BatchCaps::default()
        };

        // overlapping ranges: the rebuild and the fast-path row interleave
        // both go through stream_merge_windows
        let overlapping =
            differential_inputs([vec![1000, 600, 500], vec![900, 600], vec![950, 600, 400]]);
        let rebuild_default = merge_core_files_rebuild_with_caps(
            StreamType::Logs,
            &as_inputs(&overlapping),
            &latest_schema,
            &fts,
            &[],
            oracle_caps(),
        )
        .unwrap();
        assert_eq!(rebuild_default.perf.order_entries_materialized, 8);
        assert_eq!(rebuild_default.perf.staged_empty_arrays, 0);
        assert!(rebuild_default.perf.interleaved_columns > 0);
        assert_eq!(
            rebuild_default.docs_batches, 1,
            "8 rows fit one default-caps window"
        );
        let rebuild_tiny = merge_core_files_rebuild_with_caps(
            StreamType::Logs,
            &as_inputs(&overlapping),
            &latest_schema,
            &fts,
            &[],
            tiny,
        )
        .unwrap();
        assert_eq!(rebuild_tiny.perf.order_entries_materialized, 8);
        assert_eq!(rebuild_tiny.perf.staged_empty_arrays, 0);
        assert!(rebuild_tiny.perf.interleaved_columns > 0);
        assert!(
            rebuild_tiny.docs_batches >= 4,
            "tiny caps must stage the rebuild in multiple bounded windows, got {}",
            rebuild_tiny.docs_batches
        );
        let default_reader = open_merged(&rebuild_default);
        let tiny_reader = open_merged(&rebuild_tiny);
        assert_core_files_equivalent(&tiny_reader, &default_reader, "rebuild tiny-vs-default");

        let fast_tiny = merge_core_files_with_caps(
            StreamType::Logs,
            &as_inputs(&overlapping),
            &latest_schema,
            &fts,
            &[],
            tiny,
        )
        .unwrap();
        assert_eq!(fast_tiny.perf.order_entries_materialized, 8);
        assert_eq!(fast_tiny.perf.staged_empty_arrays, 0);
        assert!(fast_tiny.perf.interleaved_columns > 0);
        assert!(fast_tiny.used_index_merge, "overlapping fast path expected");
        assert!(
            fast_tiny.docs_batches >= 4,
            "tiny caps must window the fast-path interleave too, got {}",
            fast_tiny.docs_batches
        );
        let fast_tiny_reader = open_merged(&fast_tiny);
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
        let disjoint_default = merge_core_files(
            StreamType::Logs,
            &as_inputs(&disjoint),
            &latest_schema,
            &fts,
            &[],
        )
        .unwrap();
        assert!(disjoint_default.used_index_merge);
        let disjoint_tiny = merge_core_files_with_caps(
            StreamType::Logs,
            &as_inputs(&disjoint),
            &latest_schema,
            &fts,
            &[],
            tiny,
        )
        .unwrap();
        assert!(disjoint_tiny.used_index_merge);
        assert!(
            disjoint_tiny.docs_batches >= 4
                && disjoint_tiny.docs_batches > disjoint_default.docs_batches,
            "tiny caps must split the disjoint stream copy: {} vs {}",
            disjoint_tiny.docs_batches,
            disjoint_default.docs_batches
        );
        let disjoint_default_reader = open_merged(&disjoint_default);
        let disjoint_tiny_reader = open_merged(&disjoint_tiny);
        // logical docs compare: the default arm now COPIES (M17 widen) while
        // the tiny arm force-decodes — bytes under null slots legitimately
        // differ across encode lineages
        assert_core_files_equivalent_logical_docs(
            &disjoint_tiny_reader,
            &disjoint_default_reader,
            "disjoint tiny-vs-default",
        );
    }

    #[test]
    fn strict_disjoint_detection_avoids_boundary_tie_ambiguity() {
        let strict = vec![
            Int64Array::from(vec![80, 70]),
            Int64Array::from(vec![100, 90]),
            Int64Array::from(Vec::<i64>::new()),
        ];
        assert_eq!(strict_disjoint_input_order(&strict), Some(vec![1, 0, 2]));

        let boundary_tie = vec![
            Int64Array::from(vec![100, 90]),
            Int64Array::from(vec![90, 80]),
        ];
        assert_eq!(strict_disjoint_input_order(&boundary_tie), None);
    }

    #[test]
    fn disjoint_rebuild_streams_runs_without_row_order_or_interleave() {
        let fts = vec!["log".to_string()];
        let inputs =
            differential_inputs([vec![1_000, 950, 900], vec![800, 700], vec![600, 500, 400]]);
        let merged = merge_core_files_rebuild_with_caps(
            StreamType::Logs,
            &as_inputs(&inputs),
            &differential_latest_schema(),
            &fts,
            &[],
            BatchCaps {
                rows: 2,
                bytes: 96,
                force_decode: true,
                ..BatchCaps::default()
            },
        )
        .unwrap();

        assert_eq!(merged.stats.row_count, 8);
        assert!(merged.docs_batches >= 4);
        assert_eq!(merged.perf.order_entries_materialized, 0);
        assert_eq!(merged.perf.staged_empty_arrays, 0);
        assert_eq!(merged.perf.interleaved_columns, 0);
        let reader = open_merged(&merged);
        let timestamps = read_i64(&reader, TIMESTAMP_COL_NAME);
        assert!(timestamps.windows(2).all(|pair| pair[0] >= pair[1]));
    }

    #[test]
    fn cancelled_merge_stops_before_opening_inputs() {
        let file = build_core_file(
            vec![Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false)],
            vec![Arc::new(Int64Array::from(vec![100]))],
            &[],
            None,
        );
        let inputs = vec![("cancel.vix".to_string(), file)];
        let cancellation = VixMergeCancellation::new();
        cancellation.cancel();
        let error = merge_core_files_with_cancellation(
            StreamType::Logs,
            &as_inputs(&inputs),
            &Schema::new(vec![Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false)]),
            &[],
            &[],
            &cancellation,
        )
        .expect_err("pre-cancelled merge must stop before input I/O");
        let message = format!("{error:#}");
        assert!(message.contains("cancelled at input-open before"));
        assert!(message.contains("cancel.vix"));
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
        let default = write_core_file_from_tables(
            "test-move-caps",
            StreamType::Logs,
            schema.clone(),
            vec![table.clone()],
            &fts,
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
            StreamType::Logs,
            schema,
            vec![table],
            &fts,
            &[],
            false,
            0,
            BatchCaps {
                rows: 1,
                bytes: 1,
                ..BatchCaps::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(
            tiny.docs_batches, 4,
            "the tiny caps stage one batch per row"
        );
        let default_reader = VixReader::open_with_index(
            bytes::Bytes::from(default.data),
            default.index.map(bytes::Bytes::from),
        )
        .unwrap();
        let tiny_reader = VixReader::open_with_index(
            bytes::Bytes::from(tiny.data),
            tiny.index.map(bytes::Bytes::from),
        )
        .unwrap();
        assert_core_files_equivalent(&tiny_reader, &default_reader, "move-job tiny-vs-default");
    }

    /// #42 L0 index-off mode, exercised through the BatchCaps policy seam
    /// (the env-backed stream-type set is process-global, so tests cannot
    /// toggle it): a LOGS-stream build forced index-off produces a
    /// column-store-only file — index_size 0, EVERY plan field a docs
    /// column, term-shaped evals erroring instead of row-dropping — and a
    /// LOGS merge over two such files HEALS to an indexed output that is
    /// reader-equivalent to the same rows merged from indexed builds (the
    /// index-off inputs force the `_source` rebuild; the indexed control
    /// takes the dictionary fast path — both strategies, one differential).
    /// #52: merging LEGACY term-indexed inputs into a bloom-only plan — the
    /// demoted field contributes zero output dictionary terms while its
    /// values (recovered from the inputs' dictionaries through the k-way
    /// stream) stay probeable in the composite bloom with coverage guards,
    /// and the field gains a docs column for the scan path. The sibling
    /// field keeps exact index answers. A second-generation merge (bloom-
    /// only inputs, values recovered from COLUMNS) preserves all of it.
    #[test]
    fn bloom_only_merge_converges_legacy_indexed_inputs() {
        use vortex_index::{
            bloom::{
                COMPOSITE_BLOOM_FIELD, COMPOSITE_GUARD_PROBES, composite_guard_key,
                composite_value_key,
            },
            sbbf::{BLOCK_BYTES, block_index, check_block, hash_value},
        };

        let fields = || {
            vec![
                Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
                Field::new("svc", DataType::Utf8, true),
                Field::new("trace_id", DataType::Utf8, true),
            ]
        };
        let file1 = build_core_file(
            fields(),
            vec![
                Arc::new(Int64Array::from(vec![100i64, 90])),
                Arc::new(StringArray::from(vec!["api", "api"])),
                Arc::new(StringArray::from(vec!["t-a1", "t-a2"])),
            ],
            &[],
            None,
        );
        let file2 = build_core_file(
            fields(),
            vec![
                Arc::new(Int64Array::from(vec![80i64, 70])),
                Arc::new(StringArray::from(vec!["web", "web"])),
                Arc::new(StringArray::from(vec!["t-b1", "t-b2"])),
            ],
            &[],
            None,
        );
        let latest_schema = Schema::new(fields());
        let inputs = vec![("f1.vix".to_string(), file1), ("f2.vix".to_string(), file2)];
        let caps = BatchCaps {
            bloom_only_override: Some("trace_id"),
            ..Default::default()
        };
        let result = merge_core_files_with_caps(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &[],
            &[],
            caps,
        )
        .unwrap();
        let merged = open_merged(&result);
        assert_eq!(merged.row_count(), 4);

        // demoted: no value-index capability, no dictionary value terms
        assert!(merged.term_field_id("trace_id").is_none());
        let mut trace_terms = 0;
        merged
            .for_each_term(&mut |key, _dc, _rgs| {
                // v2 field-major keys: `{fid u16 BE}{token}`
                if key.len() > 2 && key[2..].starts_with(b"t-") {
                    trace_terms += 1;
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(
            trace_terms, 0,
            "no trace_id values in the merged dictionary"
        );
        // sibling keeps exact index behavior
        assert_eq!(matching_docs(&merged, &exact("svc", "api")).len(), 2);
        // the scan column exists and carries the values
        assert_eq!(
            read_strings(&merged, "trace_id")
                .iter()
                .filter(|v| v.is_some())
                .count(),
            4
        );

        // composite: all four values probeable, absent misses, guards claim
        let blooms = merged.file_blooms().unwrap().expect("blob");
        let comp = blooms
            .iter()
            .find(|b| b.field == COMPOSITE_BLOOM_FIELD)
            .expect("composite section");
        let probe = |key: &[u8]| {
            let h = hash_value(key);
            let i = block_index(h, comp.num_blocks) as usize;
            let block: &[u8; BLOCK_BYTES] = comp.bytes[i * BLOCK_BYTES..(i + 1) * BLOCK_BYTES]
                .try_into()
                .unwrap();
            check_block(block, h)
        };
        let mut buf = Vec::new();
        for v in ["t-a1", "t-a2", "t-b1", "t-b2"] {
            assert!(
                probe(composite_value_key("trace_id", v.as_bytes(), &mut buf).unwrap()),
                "merged composite must carry {v}"
            );
        }
        assert!(!probe(
            composite_value_key("trace_id", b"t-absent", &mut buf).unwrap()
        ));
        for pr in 0..COMPOSITE_GUARD_PROBES {
            assert!(probe(
                composite_guard_key("trace_id", pr, &mut buf).unwrap()
            ));
        }

        // SECOND GENERATION: merge the bloom-only output with itself-shaped
        // sibling — values must survive via the docs COLUMNS this time
        let gen2_inputs = vec![(
            "g1.vix".to_string(),
            (
                bytes::Bytes::from(result.output.to_bytes().unwrap()),
                result.index.clone().map(bytes::Bytes::from),
            ),
        )];
        let gen2 = merge_core_files_with_caps(
            StreamType::Logs,
            &as_inputs(&gen2_inputs),
            &latest_schema,
            &[],
            &[],
            caps,
        )
        .unwrap();
        let merged2 = open_merged(&gen2);
        let blooms2 = merged2.file_blooms().unwrap().expect("gen2 blob");
        let comp2 = blooms2
            .iter()
            .find(|b| b.field == COMPOSITE_BLOOM_FIELD)
            .expect("gen2 composite");
        let probe2 = |key: &[u8]| {
            let h = hash_value(key);
            let i = block_index(h, comp2.num_blocks) as usize;
            let block: &[u8; BLOCK_BYTES] = comp2.bytes[i * BLOCK_BYTES..(i + 1) * BLOCK_BYTES]
                .try_into()
                .unwrap();
            check_block(block, h)
        };
        for v in ["t-a1", "t-a2", "t-b1", "t-b2"] {
            assert!(
                probe2(composite_value_key("trace_id", v.as_bytes(), &mut buf).unwrap()),
                "gen2 composite must carry {v} (column-derived)"
            );
        }
        for pr in 0..COMPOSITE_GUARD_PROBES {
            assert!(probe2(
                composite_guard_key("trace_id", pr, &mut buf).unwrap()
            ));
        }
    }

    /// M7 helper: one file built through the REAL move-job path
    /// (`write_core_file_from_tables`) with the AUTO thresholds shrunk so
    /// `trace_id` (distinct == rows) demotes AT FIRST ENCODE while `svc`
    /// (2 distinct) stays term-indexed. `ts` descending, values salted.
    async fn demoted_at_birth_file(ts: &[i64], salt: &str) -> BuiltPair {
        let schema = Arc::new(Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("svc", DataType::Utf8, true),
            Field::new("trace_id", DataType::Utf8, true),
        ]));
        let n = ts.len();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ts.to_vec())) as ArrayRef,
                Arc::new(StringArray::from(
                    (0..n)
                        .map(|r| if r % 2 == 0 { "api" } else { "web" })
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(StringArray::from(
                    (0..n)
                        .map(|r| format!("t-{salt}-{r:04}"))
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
            ],
        )
        .unwrap();
        let table = Arc::new(MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap());
        let result = write_core_file_from_tables_with_caps(
            "test-m7-demoted-birth",
            StreamType::Logs,
            schema,
            vec![table],
            &[],
            &[],
            false,
            0,
            BatchCaps {
                bloom_auto_override: Some((0.5, 4)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        (
            bytes::Bytes::from(result.data),
            result.index.map(bytes::Bytes::from),
        )
    }

    /// Probe one composite section for a key (SBBF mechanics, shared by the
    /// M7 tests below).
    fn composite_probe(blooms: &[vortex_index::bloom::FileBloom], key: &[u8]) -> bool {
        use vortex_index::{
            bloom::COMPOSITE_BLOOM_FIELD,
            sbbf::{BLOCK_BYTES, block_index, check_block, hash_value},
        };
        let comp = blooms
            .iter()
            .find(|b| b.field == COMPOSITE_BLOOM_FIELD)
            .expect("composite section");
        let h = hash_value(key);
        let i = block_index(h, comp.num_blocks) as usize;
        let block: &[u8; BLOCK_BYTES] = comp.bytes[i * BLOCK_BYTES..(i + 1) * BLOCK_BYTES]
            .try_into()
            .unwrap();
        check_block(block, h)
    }

    /// Count a reader's dictionary VALUE terms with a given token prefix
    /// (key terms under `KEY_FIELD_ID` excluded) — the "no demoted values
    /// in the dictionary" assertion of the M7 tests.
    fn value_terms_with_prefix(reader: &VixReader, prefix: &[u8]) -> usize {
        let mut hits = 0;
        reader
            .for_each_term(&mut |key, _dc, _rgs| {
                if key.len() > 2 && key[0..2] != [0xFF, 0xFF] && key[2..].starts_with(prefix) {
                    hits += 1;
                }
                Ok(())
            })
            .unwrap();
        hits
    }

    /// #52/M7 (1): a file demoted at FIRST ENCODE through the real move-job
    /// path carries the construction-demotion semantics — `bloom` marker,
    /// no dictionary values, composite coverage + guards, key terms intact,
    /// the scan column readable for filter-back — and the single-file sweep
    /// classifies it CURRENT under the default plan (sticky marker), never
    /// looping rebuild → re-demote → rebuild.
    #[tokio::test]
    async fn auto_demotes_at_first_encode_and_classifies_current() {
        use vortex_index::bloom::{
            COMPOSITE_GUARD_PROBES, composite_guard_key, composite_value_key,
        };

        let ts: Vec<i64> = (0..8).map(|i| 1_000 - i).collect();
        let built = demoted_at_birth_file(&ts, "a").await;
        let reader = open_pair(&built);
        assert_eq!(reader.row_count(), 8);

        // marker + capabilities
        assert_eq!(reader.bloom_only_fields().collect::<Vec<_>>(), ["trace_id"]);
        assert!(reader.term_field_id("trace_id").is_none());
        assert!(reader.has_term_capability("svc"));
        assert_eq!(value_terms_with_prefix(&reader, b"t-"), 0);
        // key terms stay: IS [NOT] NULL proofs remain exact
        assert_eq!(matching_docs(&reader, &key_exists("trace_id")).len(), 8);
        // sibling keeps exact index behavior
        assert_eq!(matching_docs(&reader, &exact("svc", "api")).len(), 4);
        // filter-back scan column: every demoted value present natively
        let stored = read_strings(&reader, "trace_id");
        assert_eq!(stored.iter().filter(|v| v.is_some()).count(), 8);
        // composite: every value probeable, absent misses, guards claim
        let blooms = reader.file_blooms().unwrap().expect("blob");
        let mut buf = Vec::new();
        for value in stored.iter().flatten() {
            assert!(composite_probe(
                &blooms,
                composite_value_key("trace_id", value.as_bytes(), &mut buf).unwrap()
            ));
        }
        assert!(!composite_probe(
            &blooms,
            composite_value_key("trace_id", b"t-absent", &mut buf).unwrap()
        ));
        for pr in 0..COMPOSITE_GUARD_PROBES {
            assert!(composite_probe(
                &blooms,
                composite_guard_key("trace_id", pr, &mut buf).unwrap()
            ));
        }

        // classify under the DEFAULT plan (no caps seam): the sticky marker
        // must make the demoted file Current — a NeedsRebuild here would be
        // the rebuild → re-demote → rebuild loop
        let latest_schema = Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("svc", DataType::Utf8, true),
            Field::new("trace_id", DataType::Utf8, true),
        ]);
        let status = classify_core_file(
            StreamType::Logs,
            "m7-demoted.vix",
            vortex_index::BytesRangeSource::new("m7-demoted.vix", built.0.clone()),
            built
                .1
                .as_ref()
                .map(|b| vortex_index::BytesRangeSource::new("m7-demoted.vxi", b.clone())),
            &latest_schema,
            &[],
            &[],
        )
        .unwrap();
        assert!(
            matches!(status, CoreFileStatus::Current),
            "demoted-at-birth file must classify Current, got {status:?}"
        );
    }

    /// #52/M7 (2): merging two demoted-at-birth inputs with AUTO OFF at
    /// merge time keeps the demotion (STICKY marker) — fast path + docs
    /// passthrough, `bloom` marker on the output, composite coverage
    /// re-derived from the docs columns for BOTH inputs' values — and the
    /// merged output classifies Current in turn.
    #[tokio::test]
    async fn sticky_merge_of_demoted_inputs_keeps_bloom_only() {
        use vortex_index::bloom::{
            COMPOSITE_GUARD_PROBES, composite_guard_key, composite_value_key,
        };

        let file_a =
            demoted_at_birth_file(&(0..8).map(|i| 2_000 - i).collect::<Vec<_>>(), "a").await;
        let file_b =
            demoted_at_birth_file(&(0..8).map(|i| 1_000 - i).collect::<Vec<_>>(), "b").await;
        let latest_schema = Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("svc", DataType::Utf8, true),
            Field::new("trace_id", DataType::Utf8, true),
        ]);
        let inputs = vec![
            ("m7-a.vix".to_string(), file_a),
            ("m7-b.vix".to_string(), file_b),
        ];
        let result = merge_core_files_with_caps(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &[],
            // trace_id IS a configured per-file bloom field: the demotion
            // must suppress its per-field section (an empty one would
            // reject every probe and wrongly drop the file)
            &["trace_id".to_string()],
            BatchCaps {
                // AUTO fully OFF at merge: stickiness alone must carry
                bloom_auto_override: Some((0.0, u64::MAX)),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(result.used_index_merge, "demoted inputs keep the fast path");
        assert_eq!(
            result.docs_passthrough_inputs, 2,
            "schema-identical disjoint inputs copy encoded; bloom coverage \
             comes from the projected column scan"
        );
        let merged = open_merged(&result);
        assert_eq!(merged.row_count(), 16);
        assert_eq!(merged.bloom_only_fields().collect::<Vec<_>>(), ["trace_id"]);
        assert!(merged.term_field_id("trace_id").is_none());
        assert_eq!(value_terms_with_prefix(&merged, b"t-"), 0);
        assert_eq!(matching_docs(&merged, &exact("svc", "api")).len(), 8);
        assert_eq!(matching_docs(&merged, &key_exists("trace_id")).len(), 16);

        let blooms = merged.file_blooms().unwrap().expect("blob");
        // the demoted field's per-field section is SUPPRESSED, never
        // published empty: an all-zero filter would read "definitely not"
        // for every value and wrongly drop the file (M7 P0)
        assert!(
            blooms.iter().all(|b| b.field != "trace_id"),
            "no per-field section for the demoted configured bloom field"
        );
        let mut buf = Vec::new();
        for value in read_strings(&merged, "trace_id").iter().flatten() {
            assert!(
                composite_probe(
                    &blooms,
                    composite_value_key("trace_id", value.as_bytes(), &mut buf).unwrap()
                ),
                "merged composite must carry {value}"
            );
        }
        for pr in 0..COMPOSITE_GUARD_PROBES {
            assert!(composite_probe(
                &blooms,
                composite_guard_key("trace_id", pr, &mut buf).unwrap()
            ));
        }

        // convergence: the merged output classifies Current too
        let merged_pair: BuiltPair = (
            bytes::Bytes::from(result.output.to_bytes().unwrap()),
            result.index.clone().map(bytes::Bytes::from),
        );
        let status = classify_core_file(
            StreamType::Logs,
            "m7-merged.vix",
            vortex_index::BytesRangeSource::new("m7-merged.vix", merged_pair.0.clone()),
            merged_pair
                .1
                .as_ref()
                .map(|b| vortex_index::BytesRangeSource::new("m7-merged.vxi", b.clone())),
            &latest_schema,
            &[],
            &[],
        )
        .unwrap();
        assert!(matches!(status, CoreFileStatus::Current));
    }

    /// #52/M7 (3): a MIXED merge — one demoted-at-birth input + one legacy
    /// term-indexed input — converges on bloom-only: sticky drives the plan
    /// (AUTO off), the legacy input's dictionary values are diverted into
    /// the composite (never the output dictionary), and coverage spans BOTH
    /// inputs' values.
    #[tokio::test]
    async fn mixed_merge_demoted_and_legacy_term_inputs_converges() {
        use vortex_index::bloom::{
            COMPOSITE_GUARD_PROBES, composite_guard_key, composite_value_key,
        };

        let fields = || {
            vec![
                Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
                Field::new("svc", DataType::Utf8, true),
                Field::new("trace_id", DataType::Utf8, true),
            ]
        };
        let demoted =
            demoted_at_birth_file(&(0..8).map(|i| 2_000 - i).collect::<Vec<_>>(), "a").await;
        assert_eq!(
            open_pair(&demoted).bloom_only_fields().count(),
            1,
            "precondition: input A demoted at birth"
        );
        // legacy: same schema, trace_id fully TERM-indexed
        let legacy = build_core_file(
            fields(),
            vec![
                Arc::new(Int64Array::from(vec![900i64, 800])),
                Arc::new(StringArray::from(vec!["api", "web"])),
                Arc::new(StringArray::from(vec!["t-legacy-1", "t-legacy-2"])),
            ],
            &[],
            None,
        );
        assert!(
            open_pair(&legacy).has_term_capability("trace_id"),
            "precondition: input B term-indexed"
        );

        let latest_schema = Schema::new(fields());
        let inputs = vec![
            ("m7-demoted.vix".to_string(), demoted),
            ("m7-legacy.vix".to_string(), legacy),
        ];
        let result = merge_core_files_with_caps(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &[],
            // configured per-file bloom field: the mixed merge would give
            // its per-field acc ONLY the legacy input's dictionary values —
            // a PARTIAL filter wrongly dropping the demoted input's values
            // — so demotion must suppress the section entirely
            &["trace_id".to_string()],
            BatchCaps {
                bloom_auto_override: Some((0.0, u64::MAX)),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(result.used_index_merge, "mixed inputs keep the fast path");
        let merged = open_merged(&result);
        assert_eq!(merged.row_count(), 10);
        // converged: bloom marker (NOT the capability-less demoted_fields
        // degrade — bloom typing loses to it, so this assert covers both)
        assert_eq!(merged.bloom_only_fields().collect::<Vec<_>>(), ["trace_id"]);
        assert!(merged.term_field_id("trace_id").is_none());
        assert_eq!(
            value_terms_with_prefix(&merged, b"t-"),
            0,
            "legacy dictionary values must divert to the bloom, not the output dictionary"
        );
        assert_eq!(matching_docs(&merged, &key_exists("trace_id")).len(), 10);
        assert_eq!(matching_docs(&merged, &exact("svc", "api")).len(), 5);
        // the scan column carries every row's value for filter-back
        let stored = read_strings(&merged, "trace_id");
        assert_eq!(stored.iter().filter(|v| v.is_some()).count(), 10);
        // composite coverage spans BOTH inputs
        let blooms = merged.file_blooms().unwrap().expect("blob");
        assert!(
            blooms.iter().all(|b| b.field != "trace_id"),
            "no partial per-field section for the demoted configured bloom field"
        );
        let mut buf = Vec::new();
        for value in stored.iter().flatten() {
            assert!(
                composite_probe(
                    &blooms,
                    composite_value_key("trace_id", value.as_bytes(), &mut buf).unwrap()
                ),
                "merged composite must carry {value}"
            );
        }
        assert!(!composite_probe(
            &blooms,
            composite_value_key("trace_id", b"t-absent", &mut buf).unwrap()
        ));
        for pr in 0..COMPOSITE_GUARD_PROBES {
            assert!(composite_probe(
                &blooms,
                composite_guard_key("trace_id", pr, &mut buf).unwrap()
            ));
        }
    }

    /// M12 double-hash elimination predicate: an input whose DICTIONARY
    /// fully covers a bloom-only output field (term capability, not
    /// partial) contributes its values through the k-way walk and must NOT
    /// be re-scanned; inputs that demoted the field at birth (bloom
    /// marker), lack capability, or stamped it partial MUST scan.
    #[tokio::test]
    async fn m12_bloom_scan_fields_skip_dictionary_covered_inputs() {
        let fields = || {
            vec![
                Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
                Field::new("svc", DataType::Utf8, true),
                Field::new("trace_id", DataType::Utf8, true),
            ]
        };
        // a merge writer whose OUTPUT plan demotes trace_id to bloom-only
        let schema = Schema::new(fields());
        let mut opts = core_writer_options(&[], Vec::new(), true);
        opts.bloom_only_field_names = vec!["trace_id".to_string()];
        let writer = VixWriter::new(&schema, opts, false);
        assert_eq!(writer.bloom_only_fields(), vec!["trace_id".to_string()]);

        // (a) term-capable input: its dictionary holds every trace_id value
        // — the k-way walk absorbs them, the scan must SKIP the field
        let ts = vec![900i64, 800, 700, 600];
        let capable = build_core_file(
            fields(),
            vec![
                Arc::new(Int64Array::from(ts.clone())),
                Arc::new(StringArray::from(vec!["api", "db", "api", "db"])),
                Arc::new(StringArray::from(vec!["t-1", "t-2", "t-3", "t-4"])),
            ],
            &[],
            None,
        );
        let capable_reader = open_pair(&capable);
        assert!(capable_reader.has_term_capability("trace_id"));
        assert!(
            bloom_scan_fields_for_input(&writer, &capable_reader).is_empty(),
            "dictionary-covered fields must not re-scan (the M12 double-hash)"
        );

        // (b) birth-demoted input: bloom marker, no value terms — scan
        let demoted = demoted_at_birth_file(&ts, "m12").await;
        let demoted_reader = open_pair(&demoted);
        assert!(!demoted_reader.has_term_capability("trace_id"));
        assert_eq!(
            bloom_scan_fields_for_input(&writer, &demoted_reader),
            vec!["trace_id".to_string()],
            "a bloom-marked input's values exist only in its docs column"
        );

        // (c) partial input: capability claimed but the dictionary is
        // knowingly incomplete — the docs column is the only complete source
        let partial: BuiltPair = (
            capable.0.clone(),
            Some(bytes::Bytes::from(
                vortex_index::test_support::repack_with_partial_fields(
                    capable.1.as_deref().expect("sidecar"),
                    &["trace_id"],
                )
                .unwrap(),
            )),
        );
        let partial_reader = open_pair(&partial);
        assert!(partial_reader.partial_fields().contains("trace_id"));
        assert_eq!(
            bloom_scan_fields_for_input(&writer, &partial_reader),
            vec!["trace_id".to_string()],
            "a partial dictionary must never be trusted for coverage"
        );
    }

    /// #51c corpus fields: feature-dense (fts tokens, nullable string +
    /// numeric cs columns with nulls, a dense value field) and buildable
    /// SCHEMA-IDENTICAL across inputs — the shape the docs-chunk
    /// passthrough requires (exact docs-schema identity with the output).
    fn passthrough_fields() -> Vec<Field> {
        vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("log", DataType::Utf8, true),
            Field::new("svc", DataType::Utf8, true),
            Field::new("env", DataType::Utf8, true),
            Field::new("code", DataType::Int64, true),
        ]
    }

    /// One #51c input file over `ts` (descending), salted so the two
    /// inputs' values are distinguishable.
    fn passthrough_file(ts: &[i64], salt: &str) -> BuiltPair {
        let n = ts.len();
        let fts = vec!["log".to_string()];
        build_core_file(
            passthrough_fields(),
            vec![
                Arc::new(Int64Array::from(ts.to_vec())),
                Arc::new(StringArray::from(
                    (0..n)
                        .map(|r| (r % 3 != 0).then(|| format!("error {salt} row {r}")))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    (0..n)
                        .map(|r| (r % 4 != 1).then(|| format!("svc-{salt}-{}", r % 2)))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(vec![Some("prod"); n])),
                Arc::new(Int64Array::from(
                    (0..n as i64)
                        .map(|r| (r % 5 != 2).then_some(200 + r))
                        .collect::<Vec<_>>(),
                )),
            ],
            &fts,
            None,
        )
    }

    /// #51c differential (a): a disjoint merge (passthrough is the DEFAULT)
    /// must be reader-equivalent to the forced-decode rebuild —
    /// identical row content (logical compare: bytes under null slots may
    /// differ across encode lineages), identical row_count and `_timestamp`
    /// range, and a zone table covering every row contiguously — with BOTH
    /// inputs copied encoded (counter == input count) and none under the
    /// force_decode oracle seam.
    #[tokio::test]
    async fn docs_passthrough_matches_rebuild() {
        let fts = vec!["log".to_string()];
        let latest_schema = Schema::new(passthrough_fields());
        let inputs = vec![
            (
                "pa.vix".to_string(),
                passthrough_file(&[1000, 950, 900, 850], "a"),
            ),
            (
                "pb.vix".to_string(),
                passthrough_file(&[800, 750, 700], "b"),
            ),
        ];

        let fast = merge_core_files(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &fts,
            &[],
        )
        .unwrap();
        assert!(fast.used_index_merge, "expected the index-merge fast path");
        assert_eq!(
            fast.docs_passthrough_inputs, 2,
            "both schema-identical disjoint inputs must copy encoded (the default)"
        );

        // seam sanity: the same merge under force_decode decodes everything
        let control = merge_core_files_with_caps(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &fts,
            &[],
            BatchCaps {
                force_decode: true,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(control.docs_passthrough_inputs, 0);

        let rebuild = merge_core_files_rebuild_with_caps(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &fts,
            &[],
            BatchCaps {
                force_decode: true,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(fast.stats.row_count, rebuild.stats.row_count);
        assert_eq!(fast.stats.min_ts, rebuild.stats.min_ts, "min_ts");
        assert_eq!(fast.stats.max_ts, rebuild.stats.max_ts, "max_ts");

        let fast_reader = open_merged(&fast);
        let rebuild_reader = open_merged(&rebuild);
        assert_core_files_equivalent_logical_docs(
            &fast_reader,
            &rebuild_reader,
            "passthrough-vs-rebuild",
        );
        // H2 §11 splice gate: the copied output carries FULL stats equal to
        // the fresh ones — the v1 stats-loss regression is structurally gone
        assert_stats_splice_parity(&fast, &rebuild, "passthrough-vs-rebuild stats");

        // spliced zone table: contiguous entries covering every row
        let zone = fast_reader.zone_chunks().expect("merged zone table");
        let mut expected_offset = 0u64;
        for chunk in zone {
            assert_eq!(chunk.row_offset, expected_offset, "contiguous zone rows");
            assert!(chunk.row_count > 0, "no empty zone entries");
            assert!(chunk.ts_min <= chunk.ts_max, "zone bounds sane");
            expected_offset += chunk.row_count;
        }
        assert_eq!(expected_offset, fast_reader.row_count(), "zone coverage");
        // the concat preserved the global DESC storage order
        let ts = read_i64(&fast_reader, TIMESTAMP_COL_NAME);
        assert!(ts.windows(2).all(|pair| pair[0] >= pair[1]), "global DESC");
    }

    /// #51c × #52 (b): a passthrough merge over bloom-only-plan inputs
    /// (whose trace_id values live ONLY in docs columns) must keep
    /// composite-bloom coverage for BOTH inputs' values — the projected
    /// bloom-only column scan is the single coverage source when docs rows
    /// never decode.
    #[test]
    fn docs_passthrough_bloom_only_coverage() {
        use vortex_index::{
            bloom::{
                COMPOSITE_BLOOM_FIELD, COMPOSITE_GUARD_PROBES, composite_guard_key,
                composite_value_key,
            },
            sbbf::{BLOCK_BYTES, block_index, check_block, hash_value},
        };

        let fields = || {
            vec![
                Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
                Field::new("svc", DataType::Utf8, true),
                Field::new("trace_id", DataType::Utf8, true),
            ]
        };
        let latest_schema = Schema::new(fields());
        let build = |ts: Vec<i64>, svc: Vec<&str>, traces: Vec<&str>| {
            build_core_file(
                fields(),
                vec![
                    Arc::new(Int64Array::from(ts)),
                    Arc::new(StringArray::from(svc)),
                    Arc::new(StringArray::from(traces)),
                ],
                &[],
                None,
            )
        };
        // gen1: term-indexed inputs converge onto the bloom-only plan
        // through the DECODE path (force_decode) — this stage exists to
        // produce bloom-only files (columns exist from the build; v2
        // all-columns) whose values live in NO dictionary
        let gen1_caps = BatchCaps {
            bloom_only_override: Some("trace_id"),
            force_decode: true,
            ..Default::default()
        };
        let gen1 = |name: &str, file: BuiltPair| {
            let inputs = vec![(name.to_string(), file)];
            let result = merge_core_files_with_caps(
                StreamType::Logs,
                &as_inputs(&inputs),
                &latest_schema,
                &[],
                &[],
                gen1_caps,
            )
            .unwrap();
            assert_eq!(
                result.docs_passthrough_inputs, 0,
                "{name}: the force_decode merge must decode"
            );
            (
                bytes::Bytes::from(result.output.to_bytes().unwrap()),
                result.index.map(bytes::Bytes::from),
            )
        };
        let gen1a = gen1(
            "a1.vix",
            build(vec![100, 90], vec!["api", "api"], vec!["t-a1", "t-a2"]),
        );
        let gen1b = gen1(
            "b1.vix",
            build(vec![80, 70], vec!["web", "web"], vec!["t-b1", "t-b2"]),
        );

        // gen2: both inputs store trace_id as a docs column and share the
        // plan's exact docs schema — the passthrough (the default) engages,
        // and the composite bloom must still carry every value of BOTH
        // inputs
        let gen2_inputs = vec![("ga.vix".to_string(), gen1a), ("gb.vix".to_string(), gen1b)];
        let gen2 = merge_core_files_with_caps(
            StreamType::Logs,
            &as_inputs(&gen2_inputs),
            &latest_schema,
            &[],
            &[],
            BatchCaps {
                bloom_only_override: Some("trace_id"),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(gen2.used_index_merge);
        assert_eq!(
            gen2.docs_passthrough_inputs, 2,
            "gen2 inputs are schema-identical bloom-only files"
        );
        let merged = open_merged(&gen2);
        assert_eq!(merged.row_count(), 4);

        let blooms = merged.file_blooms().unwrap().expect("bloom blob");
        let comp = blooms
            .iter()
            .find(|b| b.field == COMPOSITE_BLOOM_FIELD)
            .expect("composite section");
        let probe = |key: &[u8]| {
            let h = hash_value(key);
            let i = block_index(h, comp.num_blocks) as usize;
            let block: &[u8; BLOCK_BYTES] = comp.bytes[i * BLOCK_BYTES..(i + 1) * BLOCK_BYTES]
                .try_into()
                .unwrap();
            check_block(block, h)
        };
        let mut buf = Vec::new();
        for value in ["t-a1", "t-a2", "t-b1", "t-b2"] {
            assert!(
                probe(composite_value_key("trace_id", value.as_bytes(), &mut buf).unwrap()),
                "passthrough merge must keep composite coverage for {value} (projected \
                 bloom-only scan)"
            );
        }
        assert!(
            !probe(composite_value_key("trace_id", b"t-absent", &mut buf).unwrap()),
            "absent value must miss"
        );
        for pr in 0..COMPOSITE_GUARD_PROBES {
            assert!(
                probe(composite_guard_key("trace_id", pr, &mut buf).unwrap()),
                "coverage guard {pr}"
            );
        }

        // and the passthrough rows equal the rebuild's, logically — under
        // force_decode for the oracle: the rebuild path passes through too
        // by default, which would compare copied chunks against copied
        // chunks
        let rebuild = merge_core_files_rebuild_with_caps(
            StreamType::Logs,
            &as_inputs(&gen2_inputs),
            &latest_schema,
            &[],
            &[],
            gen1_caps,
        )
        .unwrap();
        assert_eq!(rebuild.docs_passthrough_inputs, 0, "oracle stays plain");
        let rebuild_reader = open_merged(&rebuild);
        assert_core_files_equivalent_logical_docs(
            &merged,
            &rebuild_reader,
            "gen2 passthrough-vs-rebuild",
        );
    }

    /// #51c (c): mixed qualification under UNION schemas (v2) — the output
    /// docs schema is the UNION of the inputs' columns, so the input whose
    /// schema equals the union copies encoded while the NARROWER input
    /// silently decodes (its missing column null-fills); the output stays
    /// equivalent to the rebuild and carries the union column.
    #[tokio::test]
    async fn docs_passthrough_mixed_qualification() {
        let fts = vec!["log".to_string()];
        let latest_schema = Schema::new(passthrough_fields());

        // input B stores an EXTRA column ("extra"): the union plan output
        // includes it — pre-M17 that DISQUALIFIED the narrower input A;
        // the widen plan now null-synthesizes "extra" for A's chunks, so A
        // copies too. Input C stores `code` at a FLIPPED width (Int32 vs
        // the plan's Int64) — a genuine re-encode the widen plan refuses —
        // and is the one input that must keep the decode path.
        let mut extra_fields = passthrough_fields();
        extra_fields.push(Field::new("extra", DataType::Utf8, true));
        let ts_b = [800i64, 750, 700];
        let n = ts_b.len();
        let file_b = build_core_file(
            extra_fields,
            vec![
                Arc::new(Int64Array::from(ts_b.to_vec())),
                Arc::new(StringArray::from(
                    (0..n)
                        .map(|r| Some(format!("error b row {r}")))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(vec![Some("svc-b-0"); n])),
                Arc::new(StringArray::from(vec![Some("prod"); n])),
                Arc::new(Int64Array::from(vec![Some(500i64); n])),
                Arc::new(StringArray::from(vec![Some("x1"); n])),
            ],
            &fts,
            None,
        );
        let mut flip_fields = passthrough_fields();
        flip_fields[4] = Field::new("code", DataType::Int32, true);
        let file_c = build_core_file(
            flip_fields,
            vec![
                Arc::new(Int64Array::from(vec![650i64, 600])),
                Arc::new(StringArray::from(vec![Some("error c row 0"), None])),
                Arc::new(StringArray::from(vec![Some("svc-c-0"); 2])),
                Arc::new(StringArray::from(vec![Some("prod"); 2])),
                Arc::new(arrow::array::Int32Array::from(vec![Some(301), Some(302)])),
            ],
            &fts,
            None,
        );
        let inputs = vec![
            (
                "pa.vix".to_string(),
                passthrough_file(&[1000, 950, 900, 850], "a"),
            ),
            ("pb-extra.vix".to_string(), file_b),
            ("pc-flip.vix".to_string(), file_c),
        ];

        let fast = merge_core_files(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &fts,
            &[],
        )
        .unwrap();
        assert!(fast.used_index_merge);
        assert_eq!(
            fast.docs_passthrough_inputs, 2,
            "the union-widened input copies alongside the identical one; only the width-flipped \
             input decodes (M17 per-input fail-open)"
        );

        let rebuild = merge_core_files_rebuild_with_caps(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &fts,
            &[],
            BatchCaps {
                force_decode: true,
                ..Default::default()
            },
        )
        .unwrap();
        let fast_reader = open_merged(&fast);
        let rebuild_reader = open_merged(&rebuild);
        assert_core_files_equivalent_logical_docs(
            &fast_reader,
            &rebuild_reader,
            "mixed passthrough-vs-rebuild",
        );
        // the union column exists; rows that never carried it read NULL —
        // v2 null-fill semantics, consistent with `_source` — whether the
        // rows arrived by widened chunk copy (A), identity copy (B) or the
        // decode fail-open (C)
        let extra = read_strings(&fast_reader, "extra");
        // merged DESC order: A's 4 rows (1000..850), B's 3 (800..700),
        // C's 2 (650..600)
        assert_eq!(
            extra,
            vec![
                None,
                None,
                None,
                None,
                Some("x1".to_string()),
                Some("x1".to_string()),
                Some("x1".to_string()),
                None,
                None,
            ],
            "the narrower inputs' rows null-fill the union column"
        );
        // the flipped input's code values were CAST to the plan width by
        // the decode path
        let code = read_i64(&fast_reader, "code");
        assert_eq!(&code[7..], &[301, 302], "Int32 -> Int64 cast image");
        // zone coverage still exact over the mixed (spliced + folded) table
        let zone = fast_reader.zone_chunks().expect("zone table");
        assert_eq!(
            zone.iter().map(|chunk| chunk.row_count).sum::<u64>(),
            fast_reader.row_count()
        );
    }

    /// #51c (d): overlapping inputs CONCATENATE by default (v2 §6.1 —
    /// passthrough-native): both inputs copy encoded and the output is
    /// stamped concat; under the force_decode oracle the merge interleaves
    /// sorted, and the two outputs are content-equivalent as multisets.
    #[tokio::test]
    async fn overlap_merge_concatenates_by_default() {
        let fts = vec!["log".to_string()];
        let latest_schema = Schema::new(passthrough_fields());
        let inputs = vec![
            (
                "pa.vix".to_string(),
                passthrough_file(&[1000, 800, 600], "a"),
            ),
            (
                "pb.vix".to_string(),
                passthrough_file(&[900, 800, 500], "b"),
            ),
        ];

        let fast = merge_core_files(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &fts,
            &[],
        )
        .unwrap();
        assert!(fast.used_index_merge);
        assert!(fast.concat_order, "overlap concatenates by default");
        assert_eq!(
            fast.docs_passthrough_inputs, 2,
            "every overlapping input copies encoded under the default concat"
        );

        let sorted = merge_core_files_with_caps(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &fts,
            &[],
            BatchCaps {
                force_decode: true,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(
            !sorted.concat_order,
            "force_decode keeps the sorted interleave"
        );
        assert_eq!(sorted.docs_passthrough_inputs, 0);
        let fast_reader = open_merged(&fast);
        let sorted_reader = open_merged(&sorted);
        assert_eq!(fast_reader.row_order(), RowOrder::Concat);
        assert_eq!(sorted_reader.row_order(), RowOrder::TsDesc);
        let ts = read_i64(&sorted_reader, TIMESTAMP_COL_NAME);
        assert!(ts.windows(2).all(|pair| pair[0] >= pair[1]), "global DESC");
        assert_core_files_content_equivalent(
            &fast_reader,
            &sorted_reader,
            "default concat vs forced-decode interleave",
        );
    }

    /// #51c HEAL differential (a): a SINGLE index-off L0 input healing to
    /// indexed through the REBUILD — with the passthrough on must produce a
    /// file reader-equivalent to today's full rebuild (identical logical
    /// docs content, identical term table / capabilities / query battery
    /// via the shared helper, identical row_count and `_timestamp` range)
    /// while copying the docs chunks instead of re-encoding them
    /// (`docs_passthrough_inputs == 1`), and the knob off keeps today's
    /// byte path (`== 0`).
    ///
    /// M3 NOTE: production single-file heals are SIDECAR-ONLY now (the docs
    /// never rewrite — `rebuild_core_file_sidecar`); this pins the
    /// surviving docs-rewriting REBUILD ARM, which single-file heals still
    /// reach on the NeedsDocsRewrite fallback and multi-input rebuild
    /// merges reach routinely.
    ///
    /// The heal's cs plan covers EVERY input column, so the output docs
    /// schema equals the index-off input's all-columnar docs schema — the
    /// exact-identity gate the passthrough requires.
    #[tokio::test]
    async fn heal_docs_passthrough_matches_rebuild() {
        let schema = wal_schema();
        let fts = vec!["log".to_string()];
        let table =
            Arc::new(MemTable::try_new(schema.clone(), vec![wal_batches(&schema)]).unwrap());
        let l0 = write_core_file_from_tables_with_caps(
            "test-heal-passthrough",
            StreamType::Logs,
            schema.clone(),
            vec![table],
            &fts,
            &[],
            false,
            0,
            BatchCaps {
                index_enabled_override: Some(false),
                ..BatchCaps::default()
            },
        )
        .await
        .unwrap();
        let l0_reader = VixReader::open_with_index(
            bytes::Bytes::from(l0.data.clone()),
            l0.index.clone().map(bytes::Bytes::from),
        )
        .unwrap();
        assert!(!l0_reader.has_index(), "the heal input must be index-off");

        let latest_schema = schema.as_ref().clone();
        let inputs = vec![(
            "l0-heal.vix".to_string(),
            (
                bytes::Bytes::from(l0.data.clone()),
                l0.index.clone().map(bytes::Bytes::from),
            ),
        )];
        let heal_with = |force_decode: bool| {
            merge_core_files_with_caps(
                StreamType::Logs,
                &as_inputs(&inputs),
                &latest_schema,
                &fts,
                &[],
                BatchCaps {
                    force_decode,
                    ..Default::default()
                },
            )
            .unwrap()
        };

        let healed = heal_with(false);
        assert!(
            !healed.used_index_merge,
            "an index-off input must force the rebuild (the heal)"
        );
        assert_eq!(
            healed.docs_passthrough_inputs, 1,
            "the schema-identical single-input heal must copy its docs chunks (the default)"
        );
        assert_eq!(healed.dropped_rows, 0);

        let control = heal_with(true);
        assert!(!control.used_index_merge);
        assert_eq!(
            control.docs_passthrough_inputs, 0,
            "force_decode keeps the decode + re-encode oracle"
        );

        // identical logical outcome: rows, ts range, index, queries
        assert_eq!(healed.stats.row_count, control.stats.row_count);
        assert_eq!(healed.stats.min_ts, control.stats.min_ts, "min_ts");
        assert_eq!(healed.stats.max_ts, control.stats.max_ts, "max_ts");
        assert_eq!(healed.stats.term_count, control.stats.term_count);
        let healed_reader = open_merged(&healed);
        let control_reader = open_merged(&control);
        assert!(healed_reader.has_index(), "the heal output is indexed");
        assert!(healed_reader.term_count() > 0);
        assert_core_files_equivalent_logical_docs(
            &healed_reader,
            &control_reader,
            "heal passthrough vs rebuild",
        );
        // H2 §11 splice gate on the heal shape too
        assert_stats_splice_parity(&healed, &control, "heal passthrough stats");

        // spliced zone table: contiguous, covering every row, and bounding
        // the same global _timestamp range the stats report
        let zone = healed_reader.zone_chunks().expect("healed zone table");
        let mut expected_offset = 0u64;
        for chunk in zone {
            assert_eq!(chunk.row_offset, expected_offset, "contiguous zone rows");
            assert!(chunk.row_count > 0, "no empty zone entries");
            assert!(chunk.ts_min <= chunk.ts_max, "zone bounds sane");
            expected_offset += chunk.row_count;
        }
        assert_eq!(expected_offset, healed_reader.row_count(), "zone coverage");
        assert_eq!(
            zone.iter().map(|chunk| chunk.ts_min).min().unwrap(),
            healed.stats.min_ts,
            "zone global min == stats"
        );
        assert_eq!(
            zone.iter().map(|chunk| chunk.ts_max).max().unwrap(),
            healed.stats.max_ts,
            "zone global max == stats"
        );
        // the copy preserved the storage DESC order
        let ts = read_i64(&healed_reader, TIMESTAMP_COL_NAME);
        assert!(ts.windows(2).all(|pair| pair[0] >= pair[1]), "global DESC");
    }

    /// M18 (prod .110 "vortex.slice not permitted by ctx"): a heal
    /// passthrough over an input whose NARROW columns store coarser chunks
    /// than `_source`'s byte-budget grid — the scan then yields those
    /// columns as slices of one stored leaf, the exact shape that pre-M18
    /// either errored the docs encoder (wrapper slices → whole-merge
    /// restart) or, worse, copied an offset-lossy reduced slice (silent
    /// wrong rows; the vortex_index-level pin measured 96% corruption on
    /// this corpus class). The deterministic slice guard must canonicalize
    /// exactly those column-windows (counted in `docs_sliced_windows`),
    /// the copy must complete as a passthrough (no restart, no write-side
    /// fail-open), and rows + doc ids must be position-exact against the
    /// forced-decode oracle.
    #[test]
    fn m18_heal_passthrough_sliced_columns_stay_row_exact() {
        let rows = 65_536usize;
        let fts = vec!["log".to_string()];
        let schema = Arc::new(Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("log", DataType::Utf8, true),
            Field::new("status", DataType::Int64, true),
            Field::new("level", DataType::Utf8, true),
        ]));
        let ts: Vec<i64> = (0..rows).map(|i| 5_000_000 - i as i64).collect();
        // variable 5..=12-row runs of pseudo-random values (inline LCG —
        // deterministic): the column stores as one coarse compressed chunk
        // per ~64Ki rows, far coarser than _source's grid, so every scan
        // window slices it
        let mut state = 0x1818_u64;
        let mut next = move || {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            state >> 16
        };
        let status: Vec<i64> = {
            let mut out = Vec::with_capacity(rows);
            while out.len() < rows {
                let run = 5 + (next() % 8) as usize;
                let value = (next() as i64) >> 8;
                for _ in 0..run.min(rows - out.len()) {
                    out.push(value);
                }
            }
            out
        };
        let level: Vec<&str> = (0..rows)
            .map(|i| ["info", "warn", "error", "debug"][(i / 512) % 4])
            .collect();
        let log_col: Vec<String> = (0..rows).map(|i| format!("evt{}", i % 97)).collect();
        let sources: Vec<String> = (0..rows)
            .map(|i| {
                format!(
                    r#"{{"_timestamp":{},"log":"{}","status":{},"level":"{}","pad":"{}"}}"#,
                    ts[i],
                    log_col[i],
                    status[i],
                    level[i],
                    "x".repeat(120)
                )
            })
            .collect();

        // index-off all-columnar input (the #46 L0 shape) with a SMALL docs
        // chunk budget so _source stores many fine chunks
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ts.clone())) as ArrayRef,
                Arc::new(StringArray::from(log_col.clone())) as ArrayRef,
                Arc::new(Int64Array::from(status.clone())) as ArrayRef,
                Arc::new(StringArray::from(level.clone())) as ArrayRef,
            ],
        )
        .unwrap();
        let mut input_writer = VixWriter::new(
            &schema,
            vortex_index::VixWriterOptions {
                index_enabled: false,
                docs_chunk_bytes: 128 * 1024,
                ..Default::default()
            },
            false,
        );
        input_writer
            .push_batch_with_source(&batch, &StringArray::from(sources.clone()), None)
            .unwrap();
        let (data, index) = input_writer.finish().unwrap();
        let inputs = vec![(
            "m18-sliced.vix".to_string(),
            (bytes::Bytes::from(data), index.map(bytes::Bytes::from)),
        )];
        let heal_with = |force_decode: bool| {
            merge_core_files_with_caps(
                StreamType::Logs,
                &as_inputs(&inputs),
                &schema,
                &fts,
                &[],
                BatchCaps {
                    force_decode,
                    ..Default::default()
                },
            )
            .unwrap()
        };

        let healed = heal_with(false);
        assert!(!healed.used_index_merge, "index-off input forces the heal");
        assert_eq!(
            healed.docs_passthrough_inputs, 1,
            "the copy must COMPLETE as a passthrough — no whole-merge restart"
        );
        assert!(
            healed.docs_sliced_windows > 0,
            "the coarse narrow columns must trip the deterministic slice guard"
        );
        assert_eq!(
            healed.docs_failopen_chunks, 0,
            "the scan-side guard catches everything before the writer backstop"
        );

        let control = heal_with(true);
        assert_eq!(control.docs_passthrough_inputs, 0, "forced-decode oracle");
        assert_eq!(healed.stats.row_count, control.stats.row_count);
        assert_eq!(healed.stats.min_ts, control.stats.min_ts);
        assert_eq!(healed.stats.max_ts, control.stats.max_ts);
        assert_eq!(healed.stats.term_count, control.stats.term_count);

        let healed_reader = open_merged(&healed);
        let control_reader = open_merged(&control);
        assert_core_files_equivalent_logical_docs(
            &healed_reader,
            &control_reader,
            "M18 sliced heal vs rebuild",
        );
        assert_stats_splice_parity(&healed, &control, "M18 sliced heal stats");

        // row-position exactness through the canonicalized column-windows:
        // the copied file holds the SAME values at the SAME positions
        assert_eq!(read_i64(&healed_reader, "status"), status);
        assert_eq!(read_i64(&healed_reader, TIMESTAMP_COL_NAME), ts);

        // doc-id invariant: index hits land on the same positions as the
        // oracle's, and the docs rows AT those positions hold the queried
        // value
        let healed_levels = read_strings(&healed_reader, "level");
        for token in ["error", "warn"] {
            let healed_docs = matching_docs(&healed_reader, &exact("level", token));
            let control_docs = matching_docs(&control_reader, &exact("level", token));
            assert!(!healed_docs.is_empty(), "{token} must have hits");
            assert_eq!(healed_docs, control_docs, "{token}: doc ids position-exact");
            for &doc in &healed_docs {
                assert_eq!(
                    healed_levels[doc as usize].as_deref(),
                    Some(token),
                    "doc {doc} must hold the queried value"
                );
            }
        }
    }

    /// #51c HEAL (b), v2 rescope: a docs-schema DIFFERENCE still falls back
    /// to the full decode+rebuild. Union schemas make a shrink impossible
    /// (the plan preserves every input column), so the surviving
    /// non-additive case is a TYPE CHANGE: the current stream schema types
    /// a column differently than the file stores it — the plan's target
    /// type differs, the output dtype differs, and the input must decode
    /// (cast) instead of copying chunks. The same input under an agreeing
    /// schema passes through — proving the gate, not the harness, makes
    /// the call.
    #[test]
    fn heal_docs_passthrough_type_change_falls_back() {
        let fts = vec!["log".to_string()];
        let fields = vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("log", DataType::Utf8, true),
            Field::new("svc", DataType::Utf8, true),
            Field::new("code", DataType::Int64, true),
        ];
        let file = build_core_file(
            fields.clone(),
            vec![
                Arc::new(Int64Array::from(vec![900i64, 800, 700])),
                Arc::new(StringArray::from(vec![
                    Some("error timeout"),
                    Some("all good"),
                    Some("error again"),
                ])),
                Arc::new(StringArray::from(vec![Some("api"), Some("web"), None])),
                Arc::new(Int64Array::from(vec![Some(500i64), None, Some(503)])),
            ],
            &fts,
            None,
        );
        let inputs = vec![("typeflip.vix".to_string(), file)];
        let heal = |latest: &Schema| {
            merge_core_files_rebuild(StreamType::Logs, &as_inputs(&inputs), latest, &fts, &[])
                .unwrap()
        };

        // type-flipped plan: the registry types `code` Utf8 while the file
        // stores Int64 — the output stores Utf8, so the chunks cannot copy
        let mut flipped_fields = fields.clone();
        flipped_fields[3] = Field::new("code", DataType::Utf8, true);
        let flipped = heal(&Schema::new(flipped_fields));
        assert_eq!(
            flipped.docs_passthrough_inputs, 0,
            "a type-changing heal must fall back to the full rebuild"
        );
        let flipped_reader = open_merged(&flipped);
        assert_eq!(
            read_strings(&flipped_reader, "code"),
            vec![Some("500".to_string()), None, Some("503".to_string())],
            "the rebuild casts the column to the plan type"
        );
        assert_eq!(read_i64(&flipped_reader, TIMESTAMP_COL_NAME).len(), 3);

        // sanity: the SAME input under the agreeing schema does copy
        let identical = heal(&Schema::new(fields));
        assert_eq!(
            identical.docs_passthrough_inputs, 1,
            "a schema-identical heal must pass through (otherwise this test \
             proves nothing about the type gate)"
        );
    }

    /// #51c HEAL (c): degenerate `_timestamp` rows force compaction-time
    /// cleansing — a row FILTER the chunk copy cannot express — so the heal
    /// passthrough must fall back and the rebuild must drop the rows as
    /// today.
    #[test]
    fn heal_docs_passthrough_degenerate_ts_falls_back() {
        let fts = vec!["log".to_string()];
        let fields = vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("log", DataType::Utf8, true),
        ];
        let poisoned = build_poisoned_core_file(
            fields.clone(),
            vec![
                Arc::new(Int64Array::from(vec![100i64, 0, 50])),
                Arc::new(StringArray::from(vec![
                    Some("keep new"),
                    Some("poison row"),
                    Some("keep old"),
                ])),
            ],
            &fts,
        );
        let latest_schema = Schema::new(fields);
        let inputs = vec![("poison.vix".to_string(), poisoned)];
        let healed = merge_core_files(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &fts,
            &[],
        )
        .unwrap();
        assert!(
            !healed.used_index_merge,
            "degenerate rows force the rebuild"
        );
        assert_eq!(
            healed.docs_passthrough_inputs, 0,
            "cleansing disqualifies the chunk copy"
        );
        assert_eq!(healed.dropped_rows, 1, "the poison row is dropped");
        assert_eq!(healed.stats.row_count, 2);
        let reader = open_merged(&healed);
        assert_eq!(
            read_i64(&reader, TIMESTAMP_COL_NAME),
            vec![100, 50],
            "only the cleansed rows survive, in DESC order"
        );
    }

    /// #51c HEAL (d) × #52: a heal over a bloom-only-plan input must keep
    /// composite-bloom coverage for the input's values with the passthrough
    /// ON — the coverage now comes from the index-building decoded scan
    /// (the projected bloom scan is skipped to avoid double hashing).
    #[test]
    fn heal_docs_passthrough_bloom_only_coverage() {
        use vortex_index::{
            bloom::{
                COMPOSITE_BLOOM_FIELD, COMPOSITE_GUARD_PROBES, composite_guard_key,
                composite_value_key,
            },
            sbbf::{BLOCK_BYTES, block_index, check_block, hash_value},
        };

        let fields = || {
            vec![
                Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
                Field::new("svc", DataType::Utf8, true),
                Field::new("trace_id", DataType::Utf8, true),
            ]
        };
        let latest_schema = Schema::new(fields());
        let caps = BatchCaps {
            bloom_only_override: Some("trace_id"),
            ..Default::default()
        };
        // gen1: a term-indexed file converges onto the bloom-only plan
        // through a FORCE-DECODE merge — this stage exists to produce the
        // bloom-only heal input whose values live in no dictionary
        let legacy = build_core_file(
            fields(),
            vec![
                Arc::new(Int64Array::from(vec![100i64, 90])),
                Arc::new(StringArray::from(vec!["api", "api"])),
                Arc::new(StringArray::from(vec!["t-h1", "t-h2"])),
            ],
            &[],
            None,
        );
        let gen1_inputs = vec![("legacy.vix".to_string(), legacy)];
        let gen1 = merge_core_files_with_caps(
            StreamType::Logs,
            &as_inputs(&gen1_inputs),
            &latest_schema,
            &[],
            &[],
            BatchCaps {
                force_decode: true,
                ..caps
            },
        )
        .unwrap();
        assert_eq!(
            gen1.docs_passthrough_inputs, 0,
            "the force_decode merge must decode"
        );
        let bloom_only_file: BuiltPair = (
            bytes::Bytes::from(gen1.output.to_bytes().unwrap()),
            gen1.index.clone().map(bytes::Bytes::from),
        );

        // the heal: a single bloom-only input rebuilt under the SAME plan —
        // schema-identical, so the passthrough engages (the default)
        let heal_inputs = vec![("bloom-heal.vix".to_string(), bloom_only_file)];
        let healed = merge_core_files_rebuild_with_caps(
            StreamType::Logs,
            &as_inputs(&heal_inputs),
            &latest_schema,
            &[],
            &[],
            caps,
        )
        .unwrap();
        assert_eq!(
            healed.docs_passthrough_inputs, 1,
            "the schema-identical bloom-only heal must copy its docs chunks"
        );
        let healed_reader = open_merged(&healed);
        assert_eq!(healed_reader.row_count(), 2);

        // composite coverage for the input's values, from the decoded scan
        let blooms = healed_reader.file_blooms().unwrap().expect("bloom blob");
        let comp = blooms
            .iter()
            .find(|b| b.field == COMPOSITE_BLOOM_FIELD)
            .expect("composite section");
        let probe = |key: &[u8]| {
            let h = hash_value(key);
            let i = block_index(h, comp.num_blocks) as usize;
            let block: &[u8; BLOCK_BYTES] = comp.bytes[i * BLOCK_BYTES..(i + 1) * BLOCK_BYTES]
                .try_into()
                .unwrap();
            check_block(block, h)
        };
        let mut buf = Vec::new();
        for value in ["t-h1", "t-h2"] {
            assert!(
                probe(composite_value_key("trace_id", value.as_bytes(), &mut buf).unwrap()),
                "heal passthrough must keep composite coverage for {value}"
            );
        }
        assert!(
            !probe(composite_value_key("trace_id", b"t-absent", &mut buf).unwrap()),
            "absent value must miss"
        );
        for pr in 0..COMPOSITE_GUARD_PROBES {
            assert!(
                probe(composite_guard_key("trace_id", pr, &mut buf).unwrap()),
                "coverage guard {pr}"
            );
        }

        // and the healed rows equal the plain rebuild's, logically
        let control = merge_core_files_rebuild_with_caps(
            StreamType::Logs,
            &as_inputs(&heal_inputs),
            &latest_schema,
            &[],
            &[],
            BatchCaps {
                force_decode: true,
                ..caps
            },
        )
        .unwrap();
        assert_eq!(control.docs_passthrough_inputs, 0);
        let control_reader = open_merged(&control);
        assert_core_files_equivalent_logical_docs(
            &healed_reader,
            &control_reader,
            "bloom-only heal vs rebuild",
        );
    }

    /// v2 union semantics: a merge over inputs with DIFFERENT column sets
    /// produces the UNION docs schema — since M17 BOTH inputs copy encoded
    /// (the narrower one's chunks widen by null-column synthesis), and the
    /// narrower input's rows read NULL in the union column (consistent with
    /// their `_source`, which never carried the field). The old
    /// derive-from-`_source` materialization is gone with
    /// `column_store_fields`.
    #[tokio::test]
    async fn merge_union_null_fills_missing_columns() {
        let fts = vec!["log".to_string()];
        let narrow_fields = vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("log", DataType::Utf8, true),
        ];
        let wide_fields = vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("log", DataType::Utf8, true),
            Field::new("duration", DataType::Int64, true),
        ];
        // input A: records genuinely WITHOUT `duration`
        let narrow = build_core_file(
            narrow_fields,
            vec![
                Arc::new(Int64Array::from(vec![1000i64, 950])),
                Arc::new(StringArray::from(vec![
                    Some("error a row 0"),
                    Some("error a row 1"),
                ])),
            ],
            &fts,
            None,
        );
        // input B: records WITH `duration`
        let wide = build_core_file(
            wide_fields.clone(),
            vec![
                Arc::new(Int64Array::from(vec![600i64, 500])),
                Arc::new(StringArray::from(vec![
                    Some("error w row 0"),
                    Some("error w row 1"),
                ])),
                Arc::new(Int64Array::from(vec![Some(11i64), Some(13)])),
            ],
            &fts,
            None,
        );
        let latest_schema = Schema::new(wide_fields);
        let inputs = vec![
            ("na.vix".to_string(), narrow),
            ("wide.vix".to_string(), wide),
        ];
        let merged = merge_core_files(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &fts,
            &[],
        )
        .unwrap();
        assert!(merged.used_index_merge, "disjoint fast path");
        assert_eq!(
            merged.docs_passthrough_inputs, 2,
            "M17: the narrow input widens and copies alongside the union-schema one"
        );
        let reader = open_merged(&merged);
        let durations = reader
            .read_docs_column("duration")
            .expect("the merged output must store the UNION schema");
        // rows are [1000, 950, 600, 500] DESC: the narrow input's rows
        // null-fill, the wide input's values copy through
        assert_eq!(
            as_int64_array(&durations).unwrap(),
            Int64Array::from(vec![None, None, Some(11i64), Some(13)]),
        );
        // ... and the null rows' `_source` never carried the field, so a
        // scan-side json_get(_source) fallback agrees with the nulls
        let sources = read_strings(&reader, SOURCE_COL_NAME);
        assert!(
            sources[0].as_ref().unwrap().find("duration").is_none(),
            "the narrow rows' _source has no duration key"
        );
        assert!(
            sources[2].as_ref().unwrap().contains("\"duration\":11"),
            "the wide rows' _source carries the value"
        );
        // the force_decode oracle produces the same logical file
        let oracle = merge_core_files_with_caps(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &fts,
            &[],
            BatchCaps {
                force_decode: true,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(oracle.docs_passthrough_inputs, 0);
        let oracle_reader = open_merged(&oracle);
        assert_core_files_equivalent_logical_docs(&reader, &oracle_reader, "union null-fill");
    }

    /// The pure decode + re-encode oracle caps (test seam; the passthrough
    /// and concat order are production defaults).
    fn oracle_caps() -> BatchCaps {
        BatchCaps {
            force_decode: true,
            ..Default::default()
        }
    }

    /// #51c-c differential (a): a merge over OVERLAPPING inputs (prod's
    /// width-N heal-merge shape — concurrently written files, interleaved
    /// timestamps, `contiguous_offsets` None) concatenates BY DEFAULT:
    /// every input copies its docs chunks encoded
    /// (`docs_passthrough_inputs == input count`, previously 0 on every
    /// overlap), the output is stamped `row_order=concat` with the inputs
    /// back-to-back in min-`_timestamp`-DESC order, and the file is
    /// CONTENT-equivalent to the sorted rebuild oracle — equal row
    /// multisets, equal term behavior (equality/needle answers by row
    /// content), zone table covering every row (non-monotonic zones prune
    /// exactly), equal file `_timestamp` span. The row-order-dependent
    /// digest legitimately differs; content is the oracle.
    #[test]
    fn concat_merge_matches_rebuild_as_multiset() {
        let fts = vec!["log".to_string()];
        let latest_schema = Schema::new(passthrough_fields());
        // mutually overlapping ranges: min_ts a=700 < c=725 < b=750
        let inputs = vec![
            (
                "pa.vix".to_string(),
                passthrough_file(&[1000, 900, 800, 700], "a"),
            ),
            (
                "pb.vix".to_string(),
                passthrough_file(&[950, 850, 750], "b"),
            ),
            ("pc.vix".to_string(), passthrough_file(&[975, 725], "c")),
        ];

        let fast = merge_core_files_with_caps(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &fts,
            &[],
            BatchCaps::default(),
        )
        .unwrap();
        assert!(fast.used_index_merge, "the index still merges (fast path)");
        assert!(fast.concat_order, "overlap = concat order (the default)");
        assert_eq!(
            fast.docs_passthrough_inputs, 3,
            "every overlapping input must copy encoded under the concat order"
        );

        // the sorted decode + re-encode oracle
        let rebuild = merge_core_files_rebuild_with_caps(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &fts,
            &[],
            oracle_caps(),
        )
        .unwrap();
        assert!(!rebuild.concat_order);
        assert_eq!(rebuild.docs_passthrough_inputs, 0, "oracle stays plain");

        // identical file-level accounting: rows, span, term count
        assert_eq!(fast.stats.row_count, rebuild.stats.row_count);
        assert_eq!(fast.stats.min_ts, rebuild.stats.min_ts, "min_ts span");
        assert_eq!(fast.stats.max_ts, rebuild.stats.max_ts, "max_ts span");
        assert_eq!((fast.stats.min_ts, fast.stats.max_ts), (700, 1000));
        assert_eq!(fast.stats.term_count, rebuild.stats.term_count);

        let fast_reader = open_merged(&fast);
        let rebuild_reader = open_merged(&rebuild);
        assert_eq!(fast_reader.row_order(), RowOrder::Concat, "concat stamp");
        assert_eq!(
            rebuild_reader.row_order(),
            RowOrder::TsDesc,
            "the sorted oracle stamps ts_desc"
        );

        // deterministic concatenation: inputs by min_ts DESC (b, c, a),
        // each input's run verbatim
        assert_eq!(
            read_i64(&fast_reader, TIMESTAMP_COL_NAME),
            vec![950, 850, 750, 975, 725, 1000, 900, 800, 700],
            "concatenation order: min_ts DESC, runs verbatim"
        );

        // the non-negotiable oracle: content equivalence (rows, terms,
        // queries, ts ranges)
        assert_core_files_content_equivalent(
            &fast_reader,
            &rebuild_reader,
            "concat-vs-rebuild multiset",
        );
        // H2 §11 splice gate: the concat copy carries FULL stats — the
        // file-level fold equals the sorted rebuild's fresh stats (windows
        // differ, aggregates must not)
        assert_stats_splice_parity(&fast, &rebuild, "concat-vs-rebuild stats");

        // spliced zone table: contiguous coverage of every row, sane
        // per-entry bounds, and REALLY non-monotonic (the shape the pruning
        // equivalence above exercises)
        let zone = fast_reader.zone_chunks().expect("merged zone table");
        let mut expected_offset = 0u64;
        for chunk in zone {
            assert_eq!(chunk.row_offset, expected_offset, "contiguous zone rows");
            assert!(chunk.row_count > 0, "no empty zone entries");
            assert!(chunk.ts_min <= chunk.ts_max, "zone bounds sane");
            expected_offset += chunk.row_count;
        }
        assert_eq!(expected_offset, fast_reader.row_count(), "zone coverage");
        assert!(
            zone.windows(2).any(|pair| pair[1].ts_max > pair[0].ts_min),
            "the concat zone table must be non-monotonic — otherwise this test proves nothing \
             about non-monotonic pruning"
        );
    }

    /// M23b differential: the SORTED rebuild over a wide interleave must be
    /// BYTE-identical whether its inputs stream through the GATED row-range
    /// decode (>= [`MERGE_SCATTERED_INPUTS_MIN`] scattered inputs) or the
    /// free-running whole-blob decode (below the threshold). One row
    /// population with globally unique timestamps is partitioned round-robin
    /// into 10 files (gated) and re-partitioned into 7 (free): the k-way
    /// order, the window boundaries and every staged value are identical by
    /// construction, so the outputs (data AND index sidecar) must match to
    /// the byte — pinning that gating/deep-copying/re-chunking changes
    /// DELIVERY only, never content.
    ///
    /// Tiny caps make the shape adversarial for the admission protocol
    /// (the "tiny budget" stress): 64-row windows over a 10-way interleave
    /// stage ~6 rows per input per window, units are 1024 rows (~3 grant
    /// cycles per input, 256-row deep-copied parts), and every input is
    /// mid-flight from the first window to the last — a deadlock or a
    /// lost/duplicated grant would hang or corrupt this test immediately.
    #[test]
    fn gated_ranged_rebuild_matches_free_rebuild_bytes() {
        let fts = vec!["log".to_string()];
        let fields = || {
            vec![
                Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
                Field::new("log", DataType::Utf8, true),
                Field::new("code", DataType::Int64, true),
            ]
        };
        let latest_schema = Schema::new(fields());
        // one global population, unique DESC timestamps
        const ROWS: i64 = 26_000;
        let build_partition = |ways: usize| -> Vec<(String, BuiltPair)> {
            (0..ways)
                .map(|part| {
                    // rows of this partition, globally DESC: ts = 1e9 - r
                    // for r ≡ part (mod ways)
                    let ts: Vec<i64> = (0..ROWS)
                        .filter(|r| (*r as usize) % ways == part)
                        .map(|r| 1_000_000_000 - r)
                        .collect();
                    let logs: Vec<String> = ts
                        .iter()
                        .map(|t| format!("event word{} tail{}", t % 97, t % 13))
                        .collect();
                    let codes: Vec<i64> = ts.iter().map(|t| 200 + (t % 5)).collect();
                    let pair = build_core_file(
                        fields(),
                        vec![
                            Arc::new(Int64Array::from(ts)),
                            Arc::new(StringArray::from(logs)),
                            Arc::new(Int64Array::from(codes)),
                        ],
                        &fts,
                        None,
                    );
                    (format!("part-{ways}way-{part}.vix"), pair)
                })
                .collect()
        };
        // force_decode disqualifies the heal passthrough on BOTH sides, so
        // both run the standard sorted rebuild through stream_merge_windows
        let caps = BatchCaps {
            rows: 64,
            force_decode: true,
            ..Default::default()
        };
        assert!(MERGE_SCATTERED_INPUTS_MIN <= 10 && MERGE_SCATTERED_INPUTS_MIN > 7);

        let gated_inputs = build_partition(10); // scattered >= threshold
        let gated = merge_core_files_rebuild_with_caps(
            StreamType::Logs,
            &as_inputs(&gated_inputs),
            &latest_schema,
            &fts,
            &[],
            caps,
        )
        .unwrap();

        let free_inputs = build_partition(7); // scattered < threshold
        let free = merge_core_files_rebuild_with_caps(
            StreamType::Logs,
            &as_inputs(&free_inputs),
            &latest_schema,
            &fts,
            &[],
            caps,
        )
        .unwrap();

        assert_eq!(gated.stats.row_count, ROWS as u64);
        assert_eq!(free.stats.row_count, ROWS as u64);
        // the merged rows are one strictly-DESC run either way
        let gated_reader = open_merged(&gated);
        let merged_ts = read_i64(&gated_reader, TIMESTAMP_COL_NAME);
        assert!(
            merged_ts.windows(2).all(|pair| pair[0] > pair[1]),
            "sorted rebuild must produce strictly DESC timestamps"
        );
        // byte identity: same rows, same order, same windows => same file
        assert_eq!(
            gated.output.to_bytes().unwrap(),
            free.output.to_bytes().unwrap(),
            "gated (10-way) and free (7-way) sorted rebuilds must emit identical data bytes"
        );
        assert_eq!(
            gated.index, free.index,
            "gated and free sorted rebuilds must emit identical index sidecars"
        );
    }

    /// #51c-c (b): the FAST path's concat order engages only when EVERY
    /// input passes the passthrough qualification (all-or-nothing) — one
    /// unqualifiable input keeps the whole merge on the sorted interleave
    /// (equivalent to the rebuild). Since M17 widens schema-subset inputs,
    /// the disqualifier here is a genuine TYPE-WIDTH FLIP (`code` stored
    /// Int32 vs the plan's Int64 — a real re-encode the widen plan
    /// refuses). Union-only differences no longer disqualify anything.
    #[test]
    fn concat_merge_requires_all_inputs_qualified() {
        let fts = vec!["log".to_string()];
        let latest_schema = Schema::new(passthrough_fields());
        // input B stores `code` at a flipped width: the plan targets Int64
        // (latest schema), so B's chunks cannot copy
        let mut flip_fields = passthrough_fields();
        flip_fields[4] = Field::new("code", DataType::Int32, true);
        let ts_b = [990i64, 780, 710];
        let n = ts_b.len();
        let file_b = build_core_file(
            flip_fields,
            vec![
                Arc::new(Int64Array::from(ts_b.to_vec())),
                Arc::new(StringArray::from(
                    (0..n)
                        .map(|r| Some(format!("error b row {r}")))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(vec![Some("svc-b-0"); n])),
                Arc::new(StringArray::from(vec![Some("prod"); n])),
                Arc::new(arrow::array::Int32Array::from(vec![Some(500); n])),
            ],
            &fts,
            None,
        );
        let inputs = vec![
            (
                "pa.vix".to_string(),
                passthrough_file(&[1000, 900, 800, 700], "a"),
            ),
            ("pb-extra.vix".to_string(), file_b),
        ];

        let fast = merge_core_files(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &fts,
            &[],
        )
        .unwrap();
        assert!(fast.used_index_merge);
        assert!(
            !fast.concat_order,
            "one unqualified input must keep the sorted interleave"
        );
        assert_eq!(fast.docs_passthrough_inputs, 0, "interleave never copies");

        let rebuild = merge_core_files_rebuild_with_caps(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &fts,
            &[],
            oracle_caps(),
        )
        .unwrap();
        let fast_reader = open_merged(&fast);
        let rebuild_reader = open_merged(&rebuild);
        assert_eq!(fast_reader.row_order(), RowOrder::TsDesc);
        assert_core_files_equivalent(&fast_reader, &rebuild_reader, "disqualified concat");
        // ... and the interleaved output really is globally sorted
        let ts = read_i64(&fast_reader, TIMESTAMP_COL_NAME);
        assert!(ts.windows(2).all(|pair| pair[0] >= pair[1]), "global DESC");
    }

    /// #51c-c (c): a concat-order OUTPUT re-entering a later merge is
    /// always legal (concat inputs force the concatenation path): the
    /// default re-merge concatenates again through the chunk copy, and the
    /// force_decode flavor lands on the rebuild's forced concatenation
    /// (decode; still stamped concat). The union multiset survives both.
    #[test]
    fn concat_output_remerges_by_default() {
        let fts = vec!["log".to_string()];
        let latest_schema = Schema::new(passthrough_fields());
        let gen1_inputs = vec![
            (
                "pa.vix".to_string(),
                passthrough_file(&[1000, 900, 800, 700], "a"),
            ),
            (
                "pb.vix".to_string(),
                passthrough_file(&[950, 850, 750], "b"),
            ),
        ];
        let gen1 = merge_core_files(
            StreamType::Logs,
            &as_inputs(&gen1_inputs),
            &latest_schema,
            &fts,
            &[],
        )
        .unwrap();
        assert!(gen1.concat_order && gen1.docs_passthrough_inputs == 2);
        let concat_bytes: BuiltPair = (
            bytes::Bytes::from(gen1.output.to_bytes().unwrap()),
            gen1.index.clone().map(bytes::Bytes::from),
        );
        assert_eq!(open_pair(&concat_bytes).row_order(), RowOrder::Concat);
        let fresh = passthrough_file(&[985, 735], "d");
        let gen2_inputs = vec![
            ("gen1-concat.vix".to_string(), concat_bytes.clone()),
            ("fresh.vix".to_string(), fresh.clone()),
        ];

        // expected union multiset (content-level)
        let mut expected = docs_row_contents(&open_pair(&concat_bytes));
        expected.extend(docs_row_contents(&open_pair(&fresh)));
        expected.sort();

        // default: concatenates again through the copy
        let gen2 = merge_core_files(
            StreamType::Logs,
            &as_inputs(&gen2_inputs),
            &latest_schema,
            &fts,
            &[],
        )
        .unwrap();
        assert!(gen2.concat_order);
        assert_eq!(gen2.docs_passthrough_inputs, 2);
        let gen2_reader = open_merged(&gen2);
        assert_eq!(gen2_reader.row_order(), RowOrder::Concat);
        let mut gen2_rows = docs_row_contents(&gen2_reader);
        gen2_rows.sort();
        assert_eq!(gen2_rows, expected, "gen2 copy keeps the union multiset");

        // force_decode: the fast path cannot copy, so the rebuild's forced
        // concatenation DECODES — same union, still concat
        let gen2_decode = merge_core_files_with_caps(
            StreamType::Logs,
            &as_inputs(&gen2_inputs),
            &latest_schema,
            &fts,
            &[],
            BatchCaps {
                force_decode: true,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(gen2_decode.concat_order, "forced concat, decode flavor");
        assert!(
            !gen2_decode.used_index_merge,
            "the decode flavor is the rebuild"
        );
        assert_eq!(gen2_decode.docs_passthrough_inputs, 0);
        let decode_reader = open_merged(&gen2_decode);
        assert_eq!(decode_reader.row_order(), RowOrder::Concat);
        let mut decode_rows = docs_row_contents(&decode_reader);
        decode_rows.sort();
        assert_eq!(
            decode_rows, expected,
            "gen2 decode keeps the union multiset"
        );
        // and the two gen2 flavors agree on the whole reader surface
        assert_core_files_content_equivalent(
            &gen2_reader,
            &decode_reader,
            "gen2 copy vs gen2 decode",
        );
    }

    /// #51c-c HEAL (d): OVERLAPPING index-off L0 inputs (written
    /// concurrently by multiple ingesters) healing to indexed through the
    /// rebuild. With both knobs on the heal builds the ENTIRE index from
    /// the decoded scan in concatenation order and copies every input's
    /// docs chunks (`docs_passthrough_inputs == 2`, previously 0 on every
    /// overlap), content-equivalent to the sorted rebuild oracle; #52
    /// bloom-only coverage (the values live only in docs columns) holds for
    /// BOTH inputs' values from the index-building scan.
    ///
    /// M3 NOTE: this ≥2-input shape is a MERGE (both inputs consumed into
    /// one output) — the rebuild path here is untouched by the sidecar-only
    /// heal, which covers exactly the single-file case.
    #[tokio::test]
    async fn heal_concat_passthrough_matches_rebuild() {
        let fields = || {
            vec![
                Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
                Field::new("log", DataType::Utf8, true),
                Field::new("svc", DataType::Utf8, true),
                Field::new("trace_id", DataType::Utf8, true),
            ]
        };
        let schema = Arc::new(Schema::new(fields()));
        let fts = vec!["log".to_string()];
        let build_l0 =
            |name: &'static str, ts: Vec<i64>, svc: &'static str, traces: Vec<&'static str>| {
                let schema = Arc::clone(&schema);
                let fts = fts.clone();
                async move {
                    let n = ts.len();
                    let batch = RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![
                            Arc::new(Int64Array::from(ts)) as ArrayRef,
                            Arc::new(StringArray::from(
                                (0..n)
                                    .map(|r| Some(format!("error {svc} row {r}")))
                                    .collect::<Vec<_>>(),
                            )),
                            Arc::new(StringArray::from(vec![Some(svc); n])),
                            Arc::new(StringArray::from(
                                traces.into_iter().map(Some).collect::<Vec<_>>(),
                            )),
                        ],
                    )
                    .unwrap();
                    let table = Arc::new(
                        MemTable::try_new(Arc::clone(&schema), vec![vec![batch]]).unwrap(),
                    );
                    let l0 = write_core_file_from_tables_with_caps(
                        name,
                        StreamType::Logs,
                        Arc::clone(&schema),
                        vec![table],
                        &fts,
                        &[],
                        false,
                        0,
                        BatchCaps {
                            index_enabled_override: Some(false),
                            ..BatchCaps::default()
                        },
                    )
                    .await
                    .unwrap();
                    (
                        bytes::Bytes::from(l0.data),
                        l0.index.map(bytes::Bytes::from),
                    )
                }
            };
        // fully overlapping ranges — the concurrent-ingester shape
        let l0_a = build_l0(
            "concat-heal-a",
            vec![1000, 800, 600, 400],
            "api",
            vec!["t-a1", "t-a2", "t-a3", "t-a4"],
        )
        .await;
        let l0_b = build_l0(
            "concat-heal-b",
            vec![900, 700, 500, 300],
            "web",
            vec!["t-b1", "t-b2", "t-b3", "t-b4"],
        )
        .await;
        assert!(
            !open_pair(&l0_a).has_index(),
            "the heal inputs must be index-off"
        );

        let latest_schema = schema.as_ref().clone();
        let inputs = vec![
            ("l0-a.vix".to_string(), l0_a),
            ("l0-b.vix".to_string(), l0_b),
        ];
        let heal_with = |caps: BatchCaps| {
            merge_core_files_with_caps(
                StreamType::Logs,
                &as_inputs(&inputs),
                &latest_schema,
                &fts,
                &[],
                caps,
            )
            .unwrap()
        };

        let bloom = |caps: BatchCaps| BatchCaps {
            bloom_only_override: Some("trace_id"),
            ..caps
        };
        let healed = heal_with(bloom(BatchCaps::default()));
        assert!(
            !healed.used_index_merge,
            "index-off inputs force the rebuild (the heal)"
        );
        assert!(healed.concat_order, "overlapping heal concatenates");
        assert_eq!(
            healed.docs_passthrough_inputs, 2,
            "both overlapping index-off inputs must copy their docs chunks"
        );
        assert_eq!(healed.dropped_rows, 0);

        let control = heal_with(bloom(oracle_caps()));
        assert!(!control.concat_order);
        assert_eq!(
            control.docs_passthrough_inputs, 0,
            "the oracle keeps today's sorted decode + re-encode"
        );

        assert_eq!(healed.stats.row_count, control.stats.row_count);
        assert_eq!(healed.stats.min_ts, control.stats.min_ts, "min_ts");
        assert_eq!(healed.stats.max_ts, control.stats.max_ts, "max_ts");
        assert_eq!((healed.stats.min_ts, healed.stats.max_ts), (300, 1000));
        let healed_reader = open_merged(&healed);
        let control_reader = open_merged(&control);
        assert!(healed_reader.has_index(), "the heal output is indexed");
        assert!(healed_reader.term_count() > 0);
        assert_eq!(healed_reader.row_order(), RowOrder::Concat);
        assert_eq!(control_reader.row_order(), RowOrder::TsDesc);
        assert_core_files_content_equivalent(
            &healed_reader,
            &control_reader,
            "concat heal vs sorted rebuild",
        );

        // zone table: contiguous coverage; bounds equal the stats span
        let zone = healed_reader.zone_chunks().expect("healed zone table");
        assert_eq!(
            zone.iter().map(|chunk| chunk.row_count).sum::<u64>(),
            healed_reader.row_count(),
            "zone coverage"
        );
        assert_eq!(
            (
                zone.iter().map(|chunk| chunk.ts_min).min().unwrap(),
                zone.iter().map(|chunk| chunk.ts_max).max().unwrap(),
            ),
            (healed.stats.min_ts, healed.stats.max_ts),
            "zone span == stats span"
        );

        // #52 bloom-only coverage from the index-building scan: BOTH
        // inputs' trace values probe positive, absents miss, guards hold
        use vortex_index::{
            bloom::{
                COMPOSITE_BLOOM_FIELD, COMPOSITE_GUARD_PROBES, composite_guard_key,
                composite_value_key,
            },
            sbbf::{BLOCK_BYTES, block_index, check_block, hash_value},
        };
        let blooms = healed_reader.file_blooms().unwrap().expect("bloom blob");
        let comp = blooms
            .iter()
            .find(|b| b.field == COMPOSITE_BLOOM_FIELD)
            .expect("composite section");
        let probe = |key: &[u8]| {
            let h = hash_value(key);
            let i = block_index(h, comp.num_blocks) as usize;
            let block: &[u8; BLOCK_BYTES] = comp.bytes[i * BLOCK_BYTES..(i + 1) * BLOCK_BYTES]
                .try_into()
                .unwrap();
            check_block(block, h)
        };
        let mut buf = Vec::new();
        for value in [
            "t-a1", "t-a2", "t-a3", "t-a4", "t-b1", "t-b2", "t-b3", "t-b4",
        ] {
            assert!(
                probe(composite_value_key("trace_id", value.as_bytes(), &mut buf).unwrap()),
                "concat heal must keep composite coverage for {value}"
            );
        }
        assert!(
            !probe(composite_value_key("trace_id", b"t-absent", &mut buf).unwrap()),
            "absent value must miss"
        );
        for pr in 0..COMPOSITE_GUARD_PROBES {
            assert!(
                probe(composite_guard_key("trace_id", pr, &mut buf).unwrap()),
                "coverage guard {pr}"
            );
        }
    }

    /// M17 gen-1 encode-once (a): a multi-input REBUILD merge over
    /// index-off L0s whose per-file schema UNIONS DIFFER (the dominant prod
    /// gen-1 shape — every fat stream's segments see different field sets)
    /// copies EVERY input's docs chunks, widening them into the output
    /// union by null-column synthesis — no docs byte is re-encoded. Pinned
    /// against the forced decode + re-encode oracle: row-content
    /// equivalence, §11 stats-splice parity (presence + file-level fold),
    /// M4 region decomposition (piecewise-DESC regions k-way merge to the
    /// exact global order), and storage-size parity (verbatim chunk copy at
    /// the same zstd level).
    #[tokio::test]
    async fn gen1_docs_copy_widens_schema_differing_inputs() {
        let fts = vec!["log".to_string()];
        // per-input schemas: a={log,svc}, b={log,code}, c={log,svc,code,region}
        let build_l0 = |name: &'static str, fields: Vec<Field>, columns: Vec<ArrayRef>| {
            let fts = fts.clone();
            async move {
                let schema = Arc::new(Schema::new(fields));
                let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
                let table =
                    Arc::new(MemTable::try_new(Arc::clone(&schema), vec![vec![batch]]).unwrap());
                let l0 = write_core_file_from_tables_with_caps(
                    name,
                    StreamType::Logs,
                    Arc::clone(&schema),
                    vec![table],
                    &fts,
                    &[],
                    false,
                    0,
                    BatchCaps {
                        index_enabled_override: Some(false),
                        ..BatchCaps::default()
                    },
                )
                .await
                .unwrap();
                (
                    bytes::Bytes::from(l0.data),
                    l0.index.map(bytes::Bytes::from),
                )
            }
        };
        let ts_field = || Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false);
        // sized so DATA dominates the file (the ±5% storage pin is
        // meaningless when fixed footers dominate a dozen rows); overlapping
        // interleaved ts ranges force the concat order
        let (n_a, n_b, n_c) = (4096usize, 3000usize, 2000usize);
        let logs = |svc: &str, n: usize, base: usize| -> Vec<String> {
            (0..n)
                .map(|r| {
                    format!(
                        "error {svc} request {} failed with backend latency {}ms attempt {}",
                        (base + r) * 7919 % 100_000,
                        (base + r) * 37 % 1500,
                        r % 5
                    )
                })
                .collect()
        };
        let ts_desc = |n: usize, hi: i64, step: i64| -> Vec<i64> {
            (0..n as i64).map(|i| hi - i * step).collect()
        };
        let l0_a = build_l0(
            "gen1-widen-a",
            vec![
                ts_field(),
                Field::new("log", DataType::Utf8, true),
                Field::new("svc", DataType::Utf8, true),
            ],
            vec![
                Arc::new(Int64Array::from(ts_desc(n_a, 1_000_000, 3))) as ArrayRef,
                Arc::new(StringArray::from(logs("api", n_a, 0))),
                Arc::new(StringArray::from(vec![Some("api"); n_a])),
            ],
        )
        .await;
        let l0_b = build_l0(
            "gen1-widen-b",
            vec![
                ts_field(),
                Field::new("code", DataType::Int64, true),
                Field::new("log", DataType::Utf8, true),
            ],
            vec![
                Arc::new(Int64Array::from(ts_desc(n_b, 999_500, 4))) as ArrayRef,
                Arc::new(Int64Array::from(
                    (0..n_b)
                        .map(|r| [200i64, 404, 500][r % 3])
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(logs("web", n_b, 50_000))),
            ],
        )
        .await;
        let l0_c = build_l0(
            "gen1-widen-c",
            vec![
                ts_field(),
                Field::new("code", DataType::Int64, true),
                Field::new("log", DataType::Utf8, true),
                Field::new("region", DataType::Utf8, true),
                Field::new("svc", DataType::Utf8, true),
            ],
            vec![
                Arc::new(Int64Array::from(ts_desc(n_c, 999_800, 5))) as ArrayRef,
                Arc::new(Int64Array::from(
                    (0..n_c).map(|r| 300 + (r as i64 % 4)).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(logs("db", n_c, 90_000))),
                Arc::new(StringArray::from(
                    (0..n_c)
                        .map(|r| (r % 10 != 1).then(|| format!("eu-{}", r % 3)))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(vec![Some("db"); n_c])),
            ],
        )
        .await;
        for (name, pair) in [("a", &l0_a), ("b", &l0_b), ("c", &l0_c)] {
            assert!(
                !open_pair(pair).has_index(),
                "input {name} must be index-off"
            );
        }

        // union schema (the merge's latest_schema): types agree, no flip
        let latest_schema = Schema::new(vec![
            ts_field(),
            Field::new("code", DataType::Int64, true),
            Field::new("log", DataType::Utf8, true),
            Field::new("region", DataType::Utf8, true),
            Field::new("svc", DataType::Utf8, true),
        ]);
        let inputs = vec![
            ("gen1-a.vix".to_string(), l0_a),
            ("gen1-b.vix".to_string(), l0_b),
            ("gen1-c.vix".to_string(), l0_c),
        ];
        let merge_with = |caps: BatchCaps| {
            merge_core_files_with_caps(
                StreamType::Logs,
                &as_inputs(&inputs),
                &latest_schema,
                &fts,
                &[],
                caps,
            )
            .unwrap()
        };

        let copied = merge_with(BatchCaps::default());
        assert!(
            !copied.used_index_merge,
            "index-off inputs force the rebuild (gen-1)"
        );
        assert!(copied.concat_order, "overlapping gen-1 inputs concatenate");
        assert_eq!(
            copied.docs_passthrough_inputs, 3,
            "ALL schema-differing inputs must copy through the widen plan"
        );
        assert_eq!(copied.dropped_rows, 0);

        let oracle = merge_with(oracle_caps());
        assert_eq!(
            oracle.docs_passthrough_inputs, 0,
            "the oracle is the pure decode + re-encode"
        );

        let copied_reader = open_merged(&copied);
        let oracle_reader = open_merged(&oracle);
        assert!(copied_reader.has_index(), "gen-1 output is indexed");
        assert!(copied_reader.term_count() > 0);
        assert_eq!(copied_reader.row_order(), RowOrder::Concat);
        assert_core_files_content_equivalent(
            &copied_reader,
            &oracle_reader,
            "gen-1 widen copy vs decode rebuild",
        );

        // §11 stats splice parity: spliced tables (widened inputs
        // synthesize zero-presence chunk rows for their missing columns)
        // fold to exactly the fresh-stats oracle
        assert_stats_splice_parity(&copied, &oracle, "gen-1 widen splice");

        // widened columns hold NULLS where the input lacked them: probe by
        // stored position (input a's run stores svc values but null code)
        let rows = copied_reader.row_count() as usize;
        assert_eq!(rows, n_a + n_b + n_c);
        let code = copied_reader.read_docs_column("code").unwrap();
        let svc = copied_reader.read_docs_column("svc").unwrap();
        let region = copied_reader.read_docs_column("region").unwrap();
        assert_eq!(code.len(), rows);
        let null_counts = (code.null_count(), svc.null_count(), region.null_count());
        assert_eq!(
            null_counts,
            (n_a, n_b, n_a + n_b + n_c / 10),
            "null runs must land exactly where inputs lacked the column \
             (a: no code; b: no svc; a+b: no region, c every 10th null region)"
        );

        // M4 region decomposition on the concat output: every region is
        // internally `_timestamp` DESC, regions cover every row exactly,
        // and their k-way merge reproduces the global sorted order (the
        // ordered-read contract the query path exploits)
        let docs = VixDocs::open(bytes::Bytes::from(copied.output.to_bytes().unwrap())).unwrap();
        let regions = docs.ts_desc_row_ranges().expect("proven region table");
        assert_eq!(
            regions.iter().map(|r| r.end - r.start).sum::<u64>(),
            copied_reader.row_count()
        );
        let ts = read_i64(&copied_reader, TIMESTAMP_COL_NAME);
        let mut merged_ts: Vec<i64> = Vec::with_capacity(ts.len());
        {
            let mut cursors: Vec<(usize, usize)> = Vec::new(); // (next, end)
            for range in &regions {
                let (start, end) = (range.start as usize, range.end as usize);
                assert!(
                    ts[start..end].windows(2).all(|w| w[0] >= w[1]),
                    "region rows must be internally DESC"
                );
                cursors.push((start, end));
            }
            while let Some(best) = cursors
                .iter()
                .enumerate()
                .filter(|(_, (next, end))| next < end)
                .max_by_key(|(_, (next, _))| ts[*next])
                .map(|(index, _)| index)
            {
                merged_ts.push(ts[cursors[best].0]);
                cursors[best].0 += 1;
            }
        }
        let mut sorted_ts = ts.clone();
        sorted_ts.sort_unstable_by(|a, b| b.cmp(a));
        assert_eq!(merged_ts, sorted_ts, "region k-way merge == global sort");

        // Storage-size pins ("same zstd level — assert, don't assume").
        // The copy can only SHRINK relative to both baselines, never grow:
        // `_source`-scale chunks copy byte-identical (same zstd frames),
        // the M6 coalescer recompresses only tiny (≤16KiB) column slices —
        // fewer, larger frames — and widened null constants encode to
        // ~nothing. Measured here: 0.92x vs Σ input docs blobs (coalesced
        // small columns + one vortex footer instead of three), 0.82x vs a
        // same-order re-encode (fresh per-chunk scheme sampling), 0.59x vs
        // the sorted-interleave re-encode (interleaving destroys per-input
        // value locality). A symmetric ±5% against a re-encode is
        // therefore not a sound assertion; the protective direction is.
        let copied_bytes = copied.output.to_bytes().unwrap().len() as f64;
        let oracle_bytes = oracle.output.to_bytes().unwrap().len() as f64;
        let merged_docs_bytes = docs.docs_blob_len() as f64;
        let input_docs_bytes: f64 = inputs
            .iter()
            .map(|(_, (data, _))| VixDocs::open(data.clone()).unwrap().docs_blob_len() as f64)
            .sum();
        let structural = merged_docs_bytes / input_docs_bytes;
        assert!(
            structural <= 1.05,
            "verbatim-copy no-bloat: merged docs blob {merged_docs_bytes} vs Σ input docs \
             blobs {input_docs_bytes} (ratio {structural:.3})"
        );
        assert!(
            copied_bytes <= oracle_bytes * 1.05,
            "the copy must never bloat storage vs the decode path: copy {copied_bytes} vs \
             sorted re-encode {oracle_bytes}"
        );
    }

    /// M17 gen-1 encode-once (b): per-input fail-open. An input whose
    /// stored column type FLIPPED against the merge target (a genuine
    /// re-encode) decodes at its concatenated position while every other
    /// input still copies — a qualification miss no longer forfeits the
    /// whole copy (pre-M17: any miss = every byte re-encoded). Content
    /// stays equivalent to the full-decode oracle.
    #[tokio::test]
    async fn gen1_docs_copy_type_flip_fails_open_per_input() {
        let fts: Vec<String> = Vec::new();
        let build_l0 = |name: &'static str, fields: Vec<Field>, columns: Vec<ArrayRef>| async move {
            let schema = Arc::new(Schema::new(fields));
            let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
            let table =
                Arc::new(MemTable::try_new(Arc::clone(&schema), vec![vec![batch]]).unwrap());
            let l0 = write_core_file_from_tables_with_caps(
                name,
                StreamType::Logs,
                Arc::clone(&schema),
                vec![table],
                &[],
                &[],
                false,
                0,
                BatchCaps {
                    index_enabled_override: Some(false),
                    ..BatchCaps::default()
                },
            )
            .await
            .unwrap();
            (
                bytes::Bytes::from(l0.data),
                l0.index.map(bytes::Bytes::from),
            )
        };
        let ts_field = || Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false);
        // input a stores `code` as Int64 — the flip victim
        let l0_a = build_l0(
            "gen1-flip-a",
            vec![ts_field(), Field::new("code", DataType::Int64, true)],
            vec![
                Arc::new(Int64Array::from(vec![1000, 800])) as ArrayRef,
                Arc::new(Int64Array::from(vec![200, 404])),
            ],
        )
        .await;
        // input b stores `code` as Utf8 — matches the merge target
        let l0_b = build_l0(
            "gen1-flip-b",
            vec![ts_field(), Field::new("code", DataType::Utf8, true)],
            vec![
                Arc::new(Int64Array::from(vec![900, 700])) as ArrayRef,
                Arc::new(StringArray::from(vec!["500", "302"])),
            ],
        )
        .await;
        let latest_schema = Schema::new(vec![ts_field(), Field::new("code", DataType::Utf8, true)]);
        let inputs = vec![
            ("gen1-flip-a.vix".to_string(), l0_a),
            ("gen1-flip-b.vix".to_string(), l0_b),
        ];
        let merge_with = |caps: BatchCaps| {
            merge_core_files_with_caps(
                StreamType::Logs,
                &as_inputs(&inputs),
                &latest_schema,
                &fts,
                &[],
                caps,
            )
            .unwrap()
        };
        let mixed = merge_with(BatchCaps::default());
        assert!(!mixed.used_index_merge);
        assert_eq!(
            mixed.docs_passthrough_inputs, 1,
            "the type-flipped input must fail open to the decode path; the clean input copies"
        );
        let oracle = merge_with(oracle_caps());
        assert_eq!(oracle.docs_passthrough_inputs, 0);
        assert_core_files_content_equivalent(
            &open_merged(&mixed),
            &open_merged(&oracle),
            "type-flip fail-open vs decode rebuild",
        );
        // the flipped input's values were CAST to the target type by the
        // decode path — verify the stored column is the cast image
        let reader = open_merged(&mixed);
        let mut codes = read_strings(&reader, "code");
        codes.sort();
        assert_eq!(
            codes,
            vec![
                Some("200".to_string()),
                Some("302".to_string()),
                Some("404".to_string()),
                Some("500".to_string()),
            ]
        );
    }

    #[tokio::test]
    async fn l0_index_off_build_heals_to_indexed_on_merge() {
        let schema = wal_schema();
        let fts = vec!["log".to_string()];
        let build = |index: Option<bool>| {
            let schema = schema.clone();
            let fts = fts.clone();
            async move {
                let table = Arc::new(
                    MemTable::try_new(schema.clone(), vec![wal_batches(&schema)]).unwrap(),
                );
                write_core_file_from_tables_with_caps(
                    "test-l0-index-off",
                    StreamType::Logs,
                    schema,
                    vec![table],
                    &fts,
                    &[],
                    false,
                    0,
                    BatchCaps {
                        index_enabled_override: index,
                        ..BatchCaps::default()
                    },
                )
                .await
                .unwrap()
            }
        };

        let l0_a = build(Some(false)).await;
        let l0_b = build(Some(false)).await;
        assert_eq!(l0_a.stats.index_size, 0, "no dict/terms/bloom bytes");
        assert_eq!(l0_a.stats.term_count, 0);
        let l0_reader = VixReader::open_with_index(
            bytes::Bytes::from(l0_a.data.clone()),
            l0_a.index.clone().map(bytes::Bytes::from),
        )
        .unwrap();
        assert!(!l0_reader.has_index(), "index=none must round-trip");
        // every plan field materialized as a docs column (the writer drops
        // _timestamp/_source/_original itself)
        for field in ["log", "svc", "code", "ok", ID_COL_NAME] {
            assert!(
                l0_reader.read_docs_column(field).is_ok(),
                "{field:?} must be a docs column"
            );
        }
        // term-shaped evals error — never an empty (row-dropping) result
        assert!(l0_reader.eval(&exact("svc", "api")).is_err());

        let indexed_a = build(None).await; // production resolve: logs = indexed
        let indexed_b = build(None).await;
        assert!(indexed_a.stats.index_size > 0);

        let latest_schema = schema.as_ref().clone();
        let merge = |name: &str, a: &CoreFileResult, b: &CoreFileResult| {
            let inputs = vec![
                (
                    format!("{name}-a.vix"),
                    (
                        bytes::Bytes::from(a.data.clone()),
                        a.index.clone().map(bytes::Bytes::from),
                    ),
                ),
                (
                    format!("{name}-b.vix"),
                    (
                        bytes::Bytes::from(b.data.clone()),
                        b.index.clone().map(bytes::Bytes::from),
                    ),
                ),
            ];
            merge_core_files(
                StreamType::Logs,
                &as_inputs(&inputs),
                &latest_schema,
                &fts,
                &[],
            )
            .unwrap()
        };
        let healed = merge("l0", &l0_a, &l0_b);
        assert!(
            !healed.used_index_merge,
            "index-off inputs must force the source rebuild"
        );
        let control = merge("idx", &indexed_a, &indexed_b);
        assert!(
            control.used_index_merge,
            "indexed inputs keep the dictionary fast path"
        );
        let healed_reader = open_merged(&healed);
        assert!(healed_reader.has_index(), "the merge output is indexed");
        assert!(healed_reader.term_count() > 0);
        let control_reader = open_merged(&control);
        assert_core_files_equivalent(&healed_reader, &control_reader, "l0-healed-vs-indexed");

        // #46 referee: the heal above ran the COLUMN-derived rebuild (all
        // inputs index-off all-columnar); force the SOURCE-derived rebuild
        // over the same inputs and demand reader equivalence — the two term
        // derivations must agree byte-identically.
        let l0_inputs = vec![
            (
                "l0-a.vix".to_string(),
                (
                    bytes::Bytes::from(l0_a.data.clone()),
                    l0_a.index.clone().map(bytes::Bytes::from),
                ),
            ),
            (
                "l0-b.vix".to_string(),
                (
                    bytes::Bytes::from(l0_b.data.clone()),
                    l0_b.index.clone().map(bytes::Bytes::from),
                ),
            ),
        ];
        let source_forced = merge_core_files_rebuild_with_caps(
            StreamType::Logs,
            &as_inputs(&l0_inputs),
            &latest_schema,
            &fts,
            &[],
            BatchCaps {
                force_source_derivation: true,
                ..BatchCaps::default()
            },
        )
        .unwrap();
        assert!(!source_forced.used_index_merge);
        let source_reader = open_merged(&source_forced);
        assert_core_files_equivalent(&healed_reader, &source_reader, "column-vs-source heal");
        // the healed run took the column arm — the M31 prod signal
        assert!(healed.terms_from_columns);
        assert!(!source_forced.terms_from_columns);

        // M31 regression: registry/stored STRING-REPRESENTATION drift must
        // not kill column derivation. Prod measured 909/918 traces fields
        // "mismatching" as stored Utf8View vs registry Utf8 — a lossless
        // representation difference — which silently kept the WHOLE FLEET
        // on the 5.4x `_source` arm. Exercised here in the mirror
        // direction (stored Utf8, registry Utf8View): same equivalence
        // class, and the parity referee must still hold byte-identically.
        let drifted_schema = Schema::new(
            latest_schema
                .fields()
                .iter()
                .map(|f| match f.data_type() {
                    DataType::Utf8 => Field::new(f.name(), DataType::Utf8View, f.is_nullable()),
                    _ => f.as_ref().clone(),
                })
                .collect::<Vec<_>>(),
        );
        let drifted = merge_core_files(
            StreamType::Logs,
            &as_inputs(&l0_inputs),
            &drifted_schema,
            &fts,
            &[],
        )
        .unwrap();
        assert!(
            drifted.terms_from_columns,
            "string-family registry drift (Utf8View vs Utf8) must keep the #46 column arm"
        );
        let drifted_source_forced = merge_core_files_rebuild_with_caps(
            StreamType::Logs,
            &as_inputs(&l0_inputs),
            &drifted_schema,
            &fts,
            &[],
            BatchCaps {
                force_source_derivation: true,
                ..BatchCaps::default()
            },
        )
        .unwrap();
        assert!(!drifted_source_forced.terms_from_columns);
        assert_core_files_equivalent(
            &open_merged(&drifted),
            &open_merged(&drifted_source_forced),
            "column-vs-source under string-representation drift",
        );

        // M31: the DEFERRED merge over the same index-off inputs writes a
        // COLUMN-STORE-ONLY output (the copy-shape non-final hop): no
        // index, no derivation, docs columns intact and L0-read semantics.
        let deferred = merge_core_files_index_deferred(
            StreamType::Logs,
            &as_inputs(&l0_inputs),
            &latest_schema,
            &fts,
            &[],
        )
        .unwrap();
        assert!(deferred.index.is_none(), "deferred output has no sidecar");
        assert_eq!(deferred.stats.index_size, 0);
        assert!(!deferred.terms_from_columns, "no term derivation ran");
        let deferred_reader = open_merged(&deferred);
        assert!(!deferred_reader.has_index());
        assert_eq!(deferred_reader.row_count(), healed_reader.row_count());
        for field in ["log", "svc", "code", "ok", ID_COL_NAME] {
            assert!(
                deferred_reader.read_docs_column(field).is_ok(),
                "{field:?} must be a docs column on the deferred output"
            );
        }
        assert!(deferred_reader.eval(&exact("svc", "api")).is_err());
        // the FINAL hop over deferred outputs then heals to indexed exactly
        // like L0s do (same index-less class): parity against the indexed
        // control ensures the deferred generation lost nothing.
        let deferred_pair = vec![(
            "deferred-a.vix".to_string(),
            (
                bytes::Bytes::from(deferred.output.to_bytes().unwrap()),
                None,
            ),
        )];
        let finalized = merge_core_files_rebuild_with_caps(
            StreamType::Logs,
            &as_inputs(&deferred_pair),
            &latest_schema,
            &fts,
            &[],
            BatchCaps::default(),
        )
        .unwrap();
        assert!(finalized.stats.index_size > 0, "final hop builds the index");
        assert!(finalized.terms_from_columns, "final hop takes the #46 arm");
        assert_core_files_equivalent(
            &open_merged(&finalized),
            &healed_reader,
            "deferred-then-finalized vs direct heal",
        );
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
            build_core_file(fields, columns, &fts, None)
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
                    merge_core_files(
                        StreamType::Logs,
                        &as_inputs(inputs),
                        &latest_schema,
                        &fts,
                        &[],
                    )
                    .unwrap(),
                ),
                (
                    "rebuild",
                    merge_core_files_rebuild(
                        StreamType::Logs,
                        &as_inputs(inputs),
                        &latest_schema,
                        &fts,
                        &[],
                    )
                    .unwrap(),
                ),
            ] {
                assert_eq!(
                    result.stats.row_count, expected_rows,
                    "{context}/{strategy}: row count"
                );
                let reader = open_merged(&result);
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
    /// token (`zeroline`) that must vanish from the merged terms.
    ///
    /// Returns `(poisoned_inputs, clean_inputs, poison_count)`.
    #[allow(clippy::type_complexity)]
    fn cleansing_inputs(
        disjoint: bool,
    ) -> (Vec<(String, BuiltPair)>, Vec<(String, BuiltPair)>, u64) {
        let fts = vec!["log".to_string()];
        let file = |name: &str, ts: Vec<i64>, poisoned: bool| {
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
            let data = if poisoned {
                build_poisoned_core_file(fields, columns, &fts)
            } else {
                build_core_file(fields, columns, &fts, None)
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
            file("f1.vix", f1_ts.clone(), true),
            file("f2.vix", f2_ts.clone(), true),
            file("f3.vix", f3_ts.clone(), false),
        ];
        let healthy = |ts: Vec<i64>| ts.into_iter().filter(|t| *t > 0).collect::<Vec<i64>>();
        let clean = vec![
            file("c1.vix", healthy(f1_ts.clone()), false),
            file("c2.vix", healthy(f2_ts.clone()), false),
            file("c3.vix", healthy(f3_ts), false),
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
        let latest_schema = cleansing_latest_schema();

        for (context, disjoint) in [("overlapping", false), ("disjoint", true)] {
            let (poisoned, clean, poison_count) = cleansing_inputs(disjoint);
            assert_eq!(poison_count, 3, "{context}: corpus sanity");

            // the clean twins take the index-merge fast path (sanity that
            // the poison check does not over-block healthy merges)...
            let clean_fast = merge_core_files(
                StreamType::Logs,
                &as_inputs(&clean),
                &latest_schema,
                &fts,
                &[],
            )
            .unwrap();
            assert!(clean_fast.used_index_merge, "{context}: clean fast path");
            assert_eq!(clean_fast.dropped_rows, 0, "{context}");
            // ... and the referee is the clean force_decode rebuild (the
            // sorted oracle — the poisoned merge lands on the sorted
            // standard rebuild, so the referee must be sorted too)
            let referee = merge_core_files_rebuild_with_caps(
                StreamType::Logs,
                &as_inputs(&clean),
                &latest_schema,
                &fts,
                &[],
                BatchCaps {
                    force_decode: true,
                    ..Default::default()
                },
            )
            .unwrap();
            assert_eq!(referee.dropped_rows, 0, "{context}");
            let referee_reader = open_merged(&referee);

            // the poisoned merge: fast path refuses -> rebuild cleanses
            let merged = merge_core_files(
                StreamType::Logs,
                &as_inputs(&poisoned),
                &latest_schema,
                &fts,
                &[],
            )
            .unwrap();
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
            let merged_reader = open_merged(&merged);
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
            let tiny = BatchCaps {
                rows: 2,
                bytes: 96,
                ..BatchCaps::default()
            };
            let bounded = merge_core_files_rebuild_with_caps(
                StreamType::Logs,
                &as_inputs(&poisoned),
                &latest_schema,
                &fts,
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
            let bounded_reader = open_merged(&bounded);
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
            None,
        );
        let inputs = vec![
            ("p1.vix".to_string(), p1),
            ("empty.vix".to_string(), empty),
            ("p2.vix".to_string(), p2),
        ];

        let result = merge_core_files(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &fts,
            &[],
        )
        .unwrap();
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
        let reader = open_merged(&result);
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
            StreamType::Logs,
            schema.clone(),
            vec![poisoned_table],
            &fts,
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
            StreamType::Logs,
            schema.clone(),
            vec![clean_table],
            &fts,
            &[],
            false,
            0,
        )
        .await
        .unwrap();
        assert_eq!(clean.dropped_rows, 0);
        let cleansed_reader = VixReader::open_with_index(
            bytes::Bytes::from(result.data),
            result.index.map(bytes::Bytes::from),
        )
        .unwrap();
        let clean_reader = VixReader::open_with_index(
            bytes::Bytes::from(clean.data),
            clean.index.map(bytes::Bytes::from),
        )
        .unwrap();
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
            StreamType::Logs,
            schema.clone(),
            vec![all_poison_table],
            &fts,
            &[],
            false,
            0,
        )
        .await
        .unwrap();
        assert_eq!(empty.dropped_rows, 2);
        assert_eq!(empty.stats.row_count, 0);
        let empty_reader = VixReader::open_with_index(
            bytes::Bytes::from(empty.data),
            empty.index.map(bytes::Bytes::from),
        )
        .unwrap();
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
        let f1 = build_poisoned_core_file(fields(), columns(vec![900, 0, 700]), &fts);
        let f2 = build_core_file(fields(), columns(vec![800, 600]), &fts, None);
        let inputs = vec![
            ("f1.vix".to_string(), f1),
            ("f2.vix".to_string(), f2.clone()),
        ];
        let merged = merge_core_files(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &fts,
            &[],
        )
        .unwrap();
        assert!(!merged.used_index_merge, "poison must force the rebuild");
        assert_eq!(merged.dropped_rows, 1);
        assert_eq!(merged.stats.row_count, 4);
        let merged_reader = open_merged(&merged);

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
            StreamType::Logs,
            wal_schema.clone(),
            vec![table],
            &fts,
            &[],
            false,
            0,
        )
        .await
        .unwrap();
        assert_eq!(move_built.dropped_rows, 0);
        let move_reader = VixReader::open_with_index(
            bytes::Bytes::from(move_built.data),
            move_built.index.map(bytes::Bytes::from),
        )
        .unwrap();

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
        let f1_clean = build_core_file(fields(), columns(vec![900, 700]), &fts, None);
        let clean_inputs = vec![
            ("c1.vix".to_string(), f1_clean.clone()),
            ("c2.vix".to_string(), f2.clone()),
        ];
        // force_decode: the move referee is SORTED, and these inputs
        // overlap — the default concat order would legitimately differ
        let clean_fast = merge_core_files_with_caps(
            StreamType::Logs,
            &as_inputs(&clean_inputs),
            &latest_schema,
            &fts,
            &[],
            BatchCaps {
                force_decode: true,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(
            clean_fast.used_index_merge,
            "clean oversize-fts inputs must keep the fast path"
        );
        let clean_fast_reader = open_merged(&clean_fast);
        assert_core_files_equivalent(&clean_fast_reader, &move_reader, "clean-fast-vs-move");

        // HEALING: a pre-fix tainted input — fts field marked partial,
        // fabricated by property patching since the fixed writer cannot
        // produce the shape — must force the rebuild (the input's
        // dictionary is missing the oversize values' tokens only a
        // `_source` rebuild re-derives), and the rebuilt output must drop
        // the taint instead of unioning it forward forever.
        let tainted: BuiltPair = (
            f2.0.clone(),
            Some(bytes::Bytes::from(
                vortex_index::test_support::repack_with_partial_fields(
                    f2.1.as_deref().expect("sidecar"),
                    &["body"],
                )
                .unwrap(),
            )),
        );
        let tainted_inputs = vec![
            ("c1.vix".to_string(), f1_clean),
            ("tainted.vix".to_string(), tainted),
        ];
        // force_decode: the move referee is sorted; the default heal over
        // these overlapping inputs would concatenate
        let healed = merge_core_files_with_caps(
            StreamType::Logs,
            &as_inputs(&tainted_inputs),
            &latest_schema,
            &fts,
            &[],
            BatchCaps {
                force_decode: true,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(
            !healed.used_index_merge,
            "a tainted fts field must force the healing rebuild"
        );
        assert_eq!(healed.dropped_rows, 0);
        let healed_reader = open_merged(&healed);
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
            None,
        );
        let latest_schema = Schema::new(fields());
        let rebuilt = merge_core_files_rebuild(
            StreamType::Logs,
            &as_inputs(&[("in.vix".to_string(), input)]),
            &latest_schema,
            &resolved,
            &[],
        )
        .unwrap();
        let reader = open_merged(&rebuilt);
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
        pair: &BuiltPair,
        latest_schema: &Schema,
        fts: &[String],
    ) -> Result<CoreFileStatus, anyhow::Error> {
        classify_core_file(
            StreamType::Logs,
            "probe.vix",
            Arc::new(CountingRangeSource {
                data: pair.0.clone(),
                fetched: Arc::default(),
            }),
            pair.1.as_ref().map(|index| {
                Arc::new(CountingRangeSource {
                    data: index.clone(),
                    fetched: Arc::default(),
                }) as Arc<dyn VixRangeSource>
            }),
            latest_schema,
            fts,
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
        let latest_schema = Schema::new(healing_fields());
        let current = build_core_file(
            healing_fields(),
            healing_columns(vec![900, 800, 700]),
            &fts,
            None,
        );

        // fully capable file: the no-op verdict
        assert!(
            matches!(
                classify_bytes(&current, &latest_schema, &fts),
                Ok(CoreFileStatus::Current)
            ),
            "a current file must classify Current"
        );

        // pre-.11 oversize taint: a plan-fts field marked partial
        let tainted: BuiltPair = (
            current.0.clone(),
            Some(bytes::Bytes::from(
                vortex_index::test_support::repack_with_partial_fields(
                    current.1.as_deref().expect("sidecar"),
                    &["body"],
                )
                .unwrap(),
            )),
        );
        assert_needs_rebuild(
            &classify_bytes(&tainted, &latest_schema, &fts),
            &["body", "partial"],
            "fts taint",
        );

        // missing value terms: the file carries `code` values without term
        // capability (pre-numeric-value-terms files, fast-path-demoted
        // fields) — the registry-enriched plan value-indexes it
        let demoted: BuiltPair = (
            current.0.clone(),
            Some(bytes::Bytes::from(
                vortex_index::test_support::repack_dropping_field_term_capability(
                    current.1.as_deref().expect("sidecar"),
                    "code",
                )
                .unwrap(),
            )),
        );
        assert_needs_rebuild(
            &classify_bytes(&demoted, &latest_schema, &fts),
            &["code", "value terms"],
            "dropped value-term capability",
        );

        // (v2 all-columns: the "configured cs field stored only in _source"
        // heal class no longer exists — a file's columns are its own
        // present-field union, so no column-capability gap can open.)

        // fts settings drift: `note` raw-term-indexed in the file, fts in
        // the current plan
        assert_needs_rebuild(
            &classify_bytes(
                &current,
                &latest_schema,
                &["body".to_string(), "note".to_string()],
            ),
            &["note"],
            "term-vs-fts marking drift",
        );

        // tokenizer drift
        let old_tokenizer: BuiltPair = (
            current.0.clone(),
            Some(bytes::Bytes::from(
                vortex_index::test_support::repack_with_tokenizer_property(
                    current.1.as_deref().expect("sidecar"),
                    "o2-v1",
                )
                .unwrap(),
            )),
        );
        assert_needs_rebuild(
            &classify_bytes(&old_tokenizer, &latest_schema, &fts),
            &["tokenizer"],
            "tokenizer drift",
        );

        // NO false heal: a registry field the file does not carry (v2: the
        // file's columns are its own present-field union; the registry can
        // be arbitrarily wider) must never trigger a heal
        let mut ghost_schema_fields = healing_fields();
        ghost_schema_fields.push(Field::new("ghost", DataType::Utf8, true));
        let ghost_schema = Schema::new(ghost_schema_fields);
        assert!(
            matches!(
                classify_bytes(&current, &ghost_schema, &fts),
                Ok(CoreFileStatus::Current)
            ),
            "a registry-only field (carried by no doc) must stay Current"
        );

        // an unreadable container errors (healing it would gut the index);
        // the probe caller logs and leaves the file alone
        assert!(
            classify_bytes(
                &(bytes::Bytes::from_static(b"not a vix container"), None),
                &latest_schema,
                &fts,
            )
            .is_err()
        );

        // CONVERGENCE: one single-input healing rebuild makes every
        // outdated file classify Current — the probe can never loop
        for (context, data) in [
            ("fts taint", &tainted),
            ("dropped value-term capability", &demoted),
            ("tokenizer drift", &old_tokenizer),
        ] {
            let healed = merge_core_files_rebuild(
                StreamType::Logs,
                &as_inputs(&[("single.vix".to_string(), data.clone())]),
                &latest_schema,
                &fts,
                &[],
            )
            .unwrap();
            assert!(
                matches!(
                    classify_bytes(
                        &(
                            bytes::Bytes::from(healed.output.to_bytes().unwrap()),
                            healed.index.clone().map(bytes::Bytes::from),
                        ),
                        &latest_schema,
                        &fts,
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
            Some(StringArray::from(
                originals.iter().map(String::as_str).collect::<Vec<_>>(),
            )),
        );

        let fetched = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let status = classify_core_file(
            StreamType::Logs,
            "probe.vix",
            Arc::new(CountingRangeSource {
                data: data.0.clone(),
                fetched: Arc::clone(&fetched),
            }),
            data.1.as_ref().map(|index| {
                Arc::new(CountingRangeSource {
                    data: index.clone(),
                    fetched: Arc::clone(&fetched),
                }) as Arc<dyn VixRangeSource>
            }),
            &Schema::new(fields),
            &["body".to_string()],
            &[],
        )
        .unwrap();
        assert!(matches!(status, CoreFileStatus::Current));

        let read = fetched.load(std::sync::atomic::Ordering::Relaxed);
        let total = (data.0.len() + data.1.as_ref().map_or(0, |b| b.len())) as u64;
        assert!(read > 0, "the probe must have read the footer");
        assert!(
            read * 4 < total,
            "a Current probe must stay metadata-only: fetched {read} of {total} bytes"
        );
    }

    /// The single-input REBUILD differential: each outdated shape — the
    /// pre-.11 fts taint, dropped value-term capability — is healed by ONE
    /// single-input rebuild
    /// whose output is reader-EQUIVALENT to a move-built file of the same
    /// rows under the current settings (docs columns, term table +
    /// postings, capabilities, partials, query battery), restores the
    /// specific capability, and classifies Current.
    ///
    /// M3 NOTE: production routes these index-only reasons to the
    /// SIDECAR-ONLY heal (`sidecar_only_heal_restores_capabilities_without_
    /// touching_docs` is the production-shape twin); this keeps pinning
    /// `merge_core_files_rebuild` as the docs-rewriting fallback arm and
    /// the reference oracle.
    #[tokio::test]
    async fn single_file_healing_rebuild_restores_capabilities() {
        let fts = vec!["body".to_string()];
        let latest_schema = Schema::new(healing_fields());
        let ts = vec![900i64, 800, 700, 600];

        // the move-built referee over the same rows with CURRENT settings
        let wal_schema = Arc::new(Schema::new(healing_fields()));
        let batch = RecordBatch::try_new(wal_schema.clone(), healing_columns(ts.clone())).unwrap();
        let table = Arc::new(MemTable::try_new(wal_schema.clone(), vec![vec![batch]]).unwrap());
        let move_built = write_core_file_from_tables(
            "test-single-heal-move",
            StreamType::Logs,
            wal_schema.clone(),
            vec![table],
            &fts,
            &[],
            false,
            0,
        )
        .await
        .unwrap();
        let move_reader = VixReader::open_with_index(
            bytes::Bytes::from(move_built.data),
            move_built.index.map(bytes::Bytes::from),
        )
        .unwrap();

        let current = build_core_file(healing_fields(), healing_columns(ts.clone()), &fts, None);
        let cases: Vec<(&str, BuiltPair)> = vec![
            (
                "fts-tainted",
                (
                    current.0.clone(),
                    Some(bytes::Bytes::from(
                        vortex_index::test_support::repack_with_partial_fields(
                            current.1.as_deref().expect("sidecar"),
                            &["body"],
                        )
                        .unwrap(),
                    )),
                ),
            ),
            (
                "value-terms-dropped",
                (
                    current.0.clone(),
                    Some(bytes::Bytes::from(
                        vortex_index::test_support::repack_dropping_field_term_capability(
                            current.1.as_deref().expect("sidecar"),
                            "code",
                        )
                        .unwrap(),
                    )),
                ),
            ),
        ];

        for (context, data) in cases {
            assert!(
                matches!(
                    classify_bytes(&data, &latest_schema, &fts),
                    Ok(CoreFileStatus::NeedsRebuild(_))
                ),
                "{context}: the probe must flag the file"
            );
            let healed = merge_core_files_rebuild(
                StreamType::Logs,
                &as_inputs(&[("single.vix".to_string(), data)]),
                &latest_schema,
                &fts,
                &[],
            )
            .unwrap();
            assert!(!healed.used_index_merge, "{context}: heal is a rebuild");
            assert_eq!(healed.dropped_rows, 0, "{context}");
            let healed_bytes: BuiltPair = (
                bytes::Bytes::from(healed.output.to_bytes().unwrap()),
                healed.index.clone().map(bytes::Bytes::from),
            );
            let healed_reader = open_pair(&healed_bytes);

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
                    classify_bytes(&healed_bytes, &latest_schema, &fts),
                    Ok(CoreFileStatus::Current)
                ),
                "{context}: healing converges (no rebuild loop)"
            );
        }
    }

    /// M3 SIDECAR-ONLY HEAL differential (DESIGN-V2 §5): the same outdated
    /// shapes as `single_file_healing_rebuild_restores_capabilities`, healed
    /// by [`rebuild_core_file_sidecar`] — a fresh `.vxi` built over the
    /// UNTOUCHED data object (no docs bytes are produced at all). The
    /// healed pair (ORIGINAL data bytes + new sidecar) must be
    /// reader-equivalent to a fresh move build of the same rows, restore
    /// each capability, and classify Current (convergence — no heal loop).
    #[tokio::test]
    async fn sidecar_only_heal_restores_capabilities_without_touching_docs() {
        let fts = vec!["body".to_string()];
        let latest_schema = Schema::new(healing_fields());
        let ts = vec![900i64, 800, 700, 600];

        // the move-built referee over the same rows with CURRENT settings
        let wal_schema = Arc::new(Schema::new(healing_fields()));
        let batch = RecordBatch::try_new(wal_schema.clone(), healing_columns(ts.clone())).unwrap();
        let table = Arc::new(MemTable::try_new(wal_schema.clone(), vec![vec![batch]]).unwrap());
        let move_built = write_core_file_from_tables(
            "test-sidecar-heal-move",
            StreamType::Logs,
            wal_schema.clone(),
            vec![table],
            &fts,
            &[],
            false,
            0,
        )
        .await
        .unwrap();
        let move_reader = VixReader::open_with_index(
            bytes::Bytes::from(move_built.data),
            move_built.index.map(bytes::Bytes::from),
        )
        .unwrap();

        let current = build_core_file(healing_fields(), healing_columns(ts.clone()), &fts, None);
        let cases: Vec<(&str, BuiltPair)> = vec![
            (
                "fts-tainted",
                (
                    current.0.clone(),
                    Some(bytes::Bytes::from(
                        vortex_index::test_support::repack_with_partial_fields(
                            current.1.as_deref().expect("sidecar"),
                            &["body"],
                        )
                        .unwrap(),
                    )),
                ),
            ),
            (
                "value-terms-dropped",
                (
                    current.0.clone(),
                    Some(bytes::Bytes::from(
                        vortex_index::test_support::repack_dropping_field_term_capability(
                            current.1.as_deref().expect("sidecar"),
                            "code",
                        )
                        .unwrap(),
                    )),
                ),
            ),
        ];

        for (context, data) in cases {
            assert!(
                matches!(
                    classify_bytes(&data, &latest_schema, &fts),
                    Ok(CoreFileStatus::NeedsRebuild(_))
                ),
                "{context}: the probe must flag the file"
            );
            let input = &as_inputs(&[("single.vix".to_string(), data.clone())])[0];
            let outcome =
                rebuild_core_file_sidecar(StreamType::Logs, input, &latest_schema, &fts, &[])
                    .unwrap();
            let SidecarHealOutcome::Rebuilt { index, stats } = outcome else {
                panic!("{context}: expected Rebuilt, got {outcome:?}");
            };
            assert_eq!(
                stats.docs_size, 0,
                "{context}: a sidecar-only heal produces NO data-object bytes"
            );
            assert_eq!(stats.index_size as usize, index.len(), "{context}");
            assert_eq!(stats.row_count, ts.len() as u64, "{context}");

            // the healed pair = the ORIGINAL data bytes + the fresh sidecar
            // (data key/bytes unchanged by construction)
            let healed_pair: BuiltPair = (data.0.clone(), Some(bytes::Bytes::from(index)));
            let healed_reader = open_pair(&healed_pair);
            assert_core_files_equivalent(
                &healed_reader,
                &move_reader,
                &format!("{context}: sidecar-healed vs move-built"),
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
                matches!(
                    classify_bytes(&healed_pair, &latest_schema, &fts),
                    Ok(CoreFileStatus::Current)
                ),
                "{context}: sidecar healing converges (no rebuild loop)"
            );

            // M12 result-cache heal invalidation e2e: a query result memoized
            // BEFORE the heal must be unreachable by the same (condition,
            // file) query AFTER it. The key carries meta.index_size — the
            // heal rewrites the sidecar under a stable data key, and the new
            // size (the same freshness witness M3's byte-cache eviction
            // uses) versions the key; the broadcast purge then frees the
            // dead entry.
            {
                use crate::search::{
                    index::{Condition, IndexCondition},
                    vix::{
                        cache::{CacheEntry, GLOBAL_CACHE},
                        generate_cache_key,
                    },
                };
                let file_key = format!("files/e2e/logs/healcache/2026/08/18/00/{context}.vix");
                let pre_index_size = data.1.as_ref().expect("pre-heal sidecar").len() as i64;
                let post_index_size = stats.index_size as i64;
                assert_ne!(
                    pre_index_size, post_index_size,
                    "{context}: the heal must change the sidecar size — index_size is the \
                     freshness witness BOTH the M3 byte-cache eviction and the M12 result-cache \
                     key rely on"
                );
                let mut condition = IndexCondition::new();
                condition.add_condition(Condition::Equal("level".to_string(), "error".to_string()));
                let file = |index_size: i64| config::meta::stream::FileKey {
                    key: file_key.clone(),
                    meta: config::meta::stream::FileMeta {
                        min_ts: 600,
                        max_ts: 900,
                        records: ts.len() as i64,
                        compressed_size: healed_pair.0.len() as i64,
                        index_size,
                        ..Default::default()
                    },
                    ..Default::default()
                };
                // a pre-heal query memoized its (wrong-after-heal) answer
                let key_pre = generate_cache_key(&condition, &None, &file(pre_index_size), None);
                GLOBAL_CACHE.put(key_pre.clone(), CacheEntry::NoMatch);
                // the SAME query against the healed row's meta: new key, miss
                let key_post = generate_cache_key(&condition, &None, &file(post_index_size), None);
                assert_ne!(
                    key_pre, key_post,
                    "{context}: post-heal queries must not share the pre-heal cache key"
                );
                assert!(
                    GLOBAL_CACHE.get(&key_post, None).is_none(),
                    "{context}: the same (condition, file) query must MISS after the heal, \
                     never serve the pre-heal entry"
                );
                // and the broadcast sweep purges the dead pre-heal entry
                assert_eq!(
                    GLOBAL_CACHE.remove_file_entries([file_key.as_str()]),
                    1,
                    "{context}: the heal broadcast purge must evict the pre-heal entry"
                );
                assert!(GLOBAL_CACHE.get(&key_pre, None).is_none(), "{context}");
            }
        }
    }

    /// M12 item 4 (L0 external-sort fix): the direct sorted-batch build is a
    /// DROP-IN for the DataFusion `ORDER BY _timestamp DESC` build the L0
    /// builder used before — same rows, fts, terms, capabilities and stats
    /// over a fat multi-slice corpus with SHRUNK byte caps (many bounded
    /// splits), with zero plan/pool/sort involvement. Also pins the DESC
    /// contract: an unsorted batch is refused loudly, never stored.
    #[tokio::test]
    async fn m12_sorted_batch_build_matches_tables_build() {
        let fts = vec!["body".to_string()];
        let rows = 20_000i64;
        // unsorted input rows; the DF arm sorts, the direct arm gets them
        // pre-sorted DESC (what the M12 L0 builder now produces)
        let ts_unsorted: Vec<i64> = (0..rows).map(|i| 1_000_000 + ((i * 7919) % rows)).collect();
        let mut ts_desc = ts_unsorted.clone();
        ts_desc.sort_unstable_by(|a, b| b.cmp(a));
        let schema = Arc::new(Schema::new(healing_fields()));
        let caps = BatchCaps {
            rows: 512,
            bytes: 16 * 1024,
            index_enabled_override: Some(true),
            ..BatchCaps::default()
        };

        let df_built = {
            let batch =
                RecordBatch::try_new(Arc::clone(&schema), healing_columns(ts_unsorted)).unwrap();
            let table =
                Arc::new(MemTable::try_new(Arc::clone(&schema), vec![vec![batch]]).unwrap());
            write_core_file_from_tables_with_caps(
                "m12-sorted-df",
                StreamType::Logs,
                Arc::clone(&schema),
                vec![table],
                &fts,
                &[],
                false,
                0,
                caps.clone(),
            )
            .await
            .unwrap()
        };
        let direct_built = {
            let batch = RecordBatch::try_new(Arc::clone(&schema), healing_columns(ts_desc.clone()))
                .unwrap();
            write_core_file_from_sorted_batch_with_caps(
                "m12-sorted-direct",
                StreamType::Logs,
                batch,
                &fts,
                &[],
                false,
                0,
                caps.clone(),
            )
            .await
            .unwrap()
        };
        assert_eq!(direct_built.stats.row_count, rows as u64);
        assert!(
            direct_built.docs_batches > 1,
            "the shrunk caps must exercise the multi-slice bounded flow \
             (got {} batches)",
            direct_built.docs_batches
        );
        let df_reader = VixReader::open_with_index(
            bytes::Bytes::from(df_built.data),
            df_built.index.map(bytes::Bytes::from),
        )
        .unwrap();
        let direct_reader = VixReader::open_with_index(
            bytes::Bytes::from(direct_built.data),
            direct_built.index.map(bytes::Bytes::from),
        )
        .unwrap();
        assert_core_files_equivalent(
            &direct_reader,
            &df_reader,
            "m12: direct sorted-batch build vs DataFusion tables build",
        );

        // the DESC contract is verified, not trusted
        let unsorted =
            RecordBatch::try_new(Arc::clone(&schema), healing_columns(vec![100, 300, 200]))
                .unwrap();
        let err = write_core_file_from_sorted_batch_with_caps(
            "m12-sorted-bad",
            StreamType::Logs,
            unsorted,
            &fts,
            &[],
            false,
            0,
            caps,
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string().contains("not sorted"),
            "unsorted input must be refused: {err:#}"
        );
    }

    /// A file holding degenerate-`_timestamp` rows cannot heal sidecar-only
    /// (cleansing drops stored rows, which only a docs rewrite expresses):
    /// [`rebuild_core_file_sidecar`] must route it to the whole-file arm.
    #[tokio::test]
    async fn sidecar_only_heal_falls_back_on_degenerate_ts() {
        let fts = vec!["body".to_string()];
        let latest_schema = Schema::new(healing_fields());
        let poisoned = build_poisoned_core_file(
            healing_fields(),
            healing_columns(vec![900, 0, 700, 600]),
            &fts,
        );
        let input = &as_inputs(&[("poison.vix".to_string(), poisoned)])[0];
        let outcome =
            rebuild_core_file_sidecar(StreamType::Logs, input, &latest_schema, &fts, &[]).unwrap();
        let SidecarHealOutcome::NeedsDocsRewrite(reason) = outcome else {
            panic!("expected NeedsDocsRewrite, got {outcome:?}");
        };
        assert!(
            reason.contains("degenerate"),
            "the reason must name the cleansing need: {reason}"
        );
    }

    /// An index-off plan (default: metrics) over an INDEXED file heals by
    /// DROPPING the sidecar — pure metadata, no scan, no docs rewrite (v2
    /// all-columns files already materialize every present field).
    #[tokio::test]
    async fn sidecar_only_heal_drops_sidecar_on_index_off_plan() {
        assert_default_index_policy();
        let fts = vec!["body".to_string()];
        let latest_schema = Schema::new(healing_fields());
        let indexed = build_core_file(
            healing_fields(),
            healing_columns(vec![900, 800]),
            &fts,
            None,
        );
        assert!(indexed.1.is_some(), "the input must carry a sidecar");
        let input = &as_inputs(&[("single.vix".to_string(), indexed)])[0];
        // metrics resolve index-off under the default policy
        let outcome =
            rebuild_core_file_sidecar(StreamType::Metrics, input, &latest_schema, &fts, &[])
                .unwrap();
        assert!(
            matches!(outcome, SidecarHealOutcome::DropSidecar),
            "expected DropSidecar, got {outcome:?}"
        );
    }

    /// The v2 L0 heal shape: an index-off all-columnar L0 file under an
    /// indexed plan gains its FIRST sidecar via the sidecar-only heal
    /// (column-derived terms, #46). The healed pair answers queries like
    /// the whole-file rebuild of the same input and classifies Current.
    #[tokio::test]
    async fn sidecar_only_heal_indexes_index_off_l0_file() {
        assert_default_index_policy();
        let latest_schema = Schema::new(index_off_fields());
        let l0 = build_index_off_core_file(
            index_off_fields(),
            index_off_columns(vec![400, 300, 200], vec!["api", "db", "api"], vec![1, 2, 3]),
        );
        assert!(l0.1.is_none(), "an index-off build carries no sidecar");

        let input = &as_inputs(&[("l0.vix".to_string(), l0.clone())])[0];
        let outcome =
            rebuild_core_file_sidecar(StreamType::Logs, input, &latest_schema, &[], &[]).unwrap();
        let SidecarHealOutcome::Rebuilt { index, stats } = outcome else {
            panic!("expected Rebuilt, got {outcome:?}");
        };
        assert_eq!(stats.docs_size, 0);
        assert_eq!(stats.row_count, 3);

        let healed_pair: BuiltPair = (l0.0.clone(), Some(bytes::Bytes::from(index)));
        let healed_reader = open_pair(&healed_pair);
        assert!(healed_reader.has_index(), "the healed pair is indexed");
        assert_eq!(
            matching_docs(&healed_reader, &exact("svc", "api")),
            vec![0, 2],
            "value terms derived from the columns"
        );

        // parity referee: the whole-file rebuild of the same input
        let rebuilt = merge_core_files_rebuild(
            StreamType::Logs,
            &as_inputs(&[("l0.vix".to_string(), l0)]),
            &latest_schema,
            &[],
            &[],
        )
        .unwrap();
        let rebuilt_reader = open_merged(&rebuilt);
        assert_core_files_equivalent(
            &healed_reader,
            &rebuilt_reader,
            "sidecar-healed L0 vs whole-file rebuild",
        );

        assert!(
            matches!(
                classify_bytes(&healed_pair, &latest_schema, &[]),
                Ok(CoreFileStatus::Current)
            ),
            "the sidecar-healed L0 file converges"
        );
    }

    /// A configured `column_store_fields` entry that NO merge input stores
    /// as a docs column (every file predates the setting) is MATERIALIZED
    /// from `_source` — on the index-merge fast path AND the rebuild, which
    /// stay reader-equivalent — instead of being skipped forever.
    #[tokio::test]
    async fn merge_materializes_configured_cs_column_missing_from_all_inputs() {
        let fts = vec!["body".to_string()];
        let latest_schema = Schema::new(healing_fields());
        // disjoint time ranges: the docs stream-copy (sequential) path
        let old1 = build_core_file(
            healing_fields(),
            healing_columns(vec![900, 800]),
            &fts,
            None,
        );
        let old2 = build_core_file(
            healing_fields(),
            healing_columns(vec![700, 600]),
            &fts,
            None,
        );
        let inputs = vec![
            ("old1.vix".to_string(), old1),
            ("old2.vix".to_string(), old2),
        ];

        let fast = merge_core_files(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &fts,
            &[],
        )
        .unwrap();
        assert!(
            fast.used_index_merge,
            "cs materialization must not force the rebuild path"
        );
        let fast_reader = open_merged(&fast);
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

        let rebuilt = merge_core_files_rebuild(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &fts,
            &[],
        )
        .unwrap();
        let rebuilt_reader = open_merged(&rebuilt);
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
        let mut writer = VixWriter::new(&schema, core_writer_options(&[], Vec::new(), true), false);
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
        let (input_data, input_index) = writer.finish().unwrap();
        let input: BuiltPair = (
            bytes::Bytes::from(input_data),
            input_index.map(bytes::Bytes::from),
        );
        eprintln!(
            "input file: {} MiB compressed",
            input.0.len() / (1024 * 1024)
        );

        let latest_schema = Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("payload", DataType::Utf8, true),
        ]);
        let inputs = vec![("big.vix".to_string(), input)];
        let started = std::time::Instant::now();
        let rebuild = merge_core_files_rebuild(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &[],
            &[],
        )
        .unwrap();
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
        let reader = open_merged(&rebuild);
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
            None,
        );
        let file2 = build_core_file(
            schema_fields(),
            vec![
                Arc::new(Int64Array::from(vec![80, 70])),
                Arc::new(StringArray::from(vec!["api gateway", "web"])),
            ],
            &[],
            None,
        );
        let latest_schema = Schema::new(schema_fields());
        let inputs = vec![("f1.vix".to_string(), file1), ("f2.vix".to_string(), file2)];

        // current settings: svc NOT fts -> file1 conflicts -> rebuild
        let result = merge_core_files(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &[],
            &[],
        )
        .unwrap();
        assert!(!result.used_index_merge);
        let reference = merge_core_files_rebuild(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &[],
            &[],
        )
        .unwrap();
        let result_reader = open_merged(&result);
        let reference_reader = open_merged(&reference);
        assert_core_files_equivalent(&result_reader, &reference_reader, "conflict");
        // the rebuild re-derived raw svc terms for every row
        assert_eq!(
            matching_docs(&result_reader, &exact("svc", "api gateway")),
            vec![0, 2]
        );

        // current settings: svc fts -> file2 conflicts -> rebuild too
        let result = merge_core_files(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &["svc".to_string()],
            &[],
        )
        .unwrap();
        assert!(!result.used_index_merge);
    }

    #[test]
    fn prepared_rebuild_enforces_admission_strictness_and_cancellation() {
        let schema_fields = || {
            vec![
                Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
                Field::new("svc", DataType::Utf8, true),
            ]
        };
        let file1 = build_core_file(
            schema_fields(),
            vec![
                Arc::new(Int64Array::from(vec![100, 90])),
                Arc::new(StringArray::from(vec!["api gateway", "db"])),
            ],
            &["svc".to_string()],
            None,
        );
        let file2 = build_core_file(
            schema_fields(),
            vec![
                Arc::new(Int64Array::from(vec![80, 70])),
                Arc::new(StringArray::from(vec!["api gateway", "web"])),
            ],
            &[],
            None,
        );
        let pairs = vec![("f1.vix".to_string(), file1), ("f2.vix".to_string(), file2)];
        let latest_schema = Arc::new(Schema::new(schema_fields()));

        let prepare =
            |cancellation: VixMergeCancellation| match try_merge_core_files_with_cancellation(
                StreamType::Logs,
                as_inputs(&pairs),
                Arc::clone(&latest_schema),
                Vec::new(),
                Vec::new(),
                cancellation,
                CoreMergeMode::Automatic,
            )
            .unwrap()
            {
                CoreMergeAttempt::NeedsRebuild(prepared) => prepared,
                CoreMergeAttempt::Complete(_) => {
                    panic!("the capability conflict must return a prepared rebuild")
                }
            };

        let cancellation = VixMergeCancellation::new();
        let prepared = prepare(cancellation);
        assert!(prepared.requires_memory_admission());
        let error = execute_prepared_core_rebuild(prepared, None)
            .expect_err("an indexed rebuild requires memory admission");
        assert!(format!("{error:#}").contains("without rebuild-memory admission"));

        let strict = try_merge_core_files_with_cancellation(
            StreamType::Logs,
            as_inputs(&pairs),
            Arc::clone(&latest_schema),
            Vec::new(),
            Vec::new(),
            VixMergeCancellation::new(),
            CoreMergeMode::IndexedOnly,
        );
        let error = match strict {
            Ok(_) => panic!("the strict path must reject the rebuild fallback"),
            Err(error) => error,
        };
        assert!(
            format!("{error:#}").contains(
                "required indexed merge is not applicable; refusing a large full rebuild"
            )
        );

        let cancellation = VixMergeCancellation::new();
        let prepared = prepare(cancellation.clone());
        let permit = acquire_vix_rebuild_permit_with_cancellation(&cancellation).unwrap();
        cancellation.cancel();
        let error = execute_prepared_core_rebuild(prepared, Some(permit))
            .expect_err("a prepared rebuild must retain cooperative cancellation");
        assert!(format!("{error:#}").contains("cancelled"));

        let cancellation = VixMergeCancellation::new();
        let prepared = prepare(cancellation.clone());
        let permit = acquire_vix_rebuild_permit_with_cancellation(&cancellation).unwrap();
        let resumed = execute_prepared_core_rebuild(prepared, Some(permit)).unwrap();
        let reference = merge_core_files_rebuild(
            StreamType::Logs,
            &as_inputs(&pairs),
            latest_schema.as_ref(),
            &[],
            &[],
        )
        .unwrap();
        assert_core_files_equivalent(
            &open_merged(&resumed),
            &open_merged(&reference),
            "prepared rebuild",
        );
    }

    #[test]
    fn prepared_index_deferred_merge_needs_no_rebuild_memory() {
        let fields = vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("svc", DataType::Utf8, true),
        ];
        let pair = build_poisoned_core_file(
            fields.clone(),
            vec![
                Arc::new(Int64Array::from(vec![100, 0])),
                Arc::new(StringArray::from(vec!["api", "db"])),
            ],
            &[],
        );
        let inputs = vec![("deferred-poison.vix".to_string(), pair)];
        let attempt = try_merge_core_files_with_cancellation(
            StreamType::Logs,
            as_inputs(&inputs),
            Arc::new(Schema::new(fields)),
            Vec::new(),
            Vec::new(),
            VixMergeCancellation::new(),
            CoreMergeMode::IndexDeferred,
        )
        .unwrap();
        let CoreMergeAttempt::NeedsRebuild(prepared) = attempt else {
            panic!("a deferred merge that must cleanse rows must resume through the copy rebuild");
        };
        assert!(!prepared.requires_memory_admission());
        let output = execute_prepared_core_rebuild(prepared, None).unwrap();
        assert!(output.index.is_none());
        assert_eq!(output.stats.index_size, 0);
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
        let inputs: Vec<(String, BuiltPair)> = paths
            .iter()
            .map(|path| {
                let index = std::fs::read(path.with_extension("vxi"))
                    .ok()
                    .map(bytes::Bytes::from);
                (
                    path.file_name().unwrap().to_string_lossy().into_owned(),
                    (bytes::Bytes::from(std::fs::read(path).unwrap()), index),
                )
            })
            .collect();
        let mib = |bytes: usize| bytes as f64 / (1024.0 * 1024.0);
        let total_bytes: usize = inputs
            .iter()
            .map(|(_, (data, index))| data.len() + index.as_ref().map_or(0, |b| b.len()))
            .sum();

        // derive the stream settings from the files themselves — the
        // compactor merges under unchanged settings in the common case
        let mut fts: Vec<String> = Vec::new();
        let mut cs: Vec<String> = Vec::new();
        let mut latest_fields: Vec<Field> = Vec::new();
        let mut rows = 0u64;
        let mut ranges: Vec<(i64, i64)> = Vec::new();
        for (key, data) in &inputs {
            let reader = open_pair(data);
            rows += reader.row_count();
            let ts = as_int64_array(&reader.read_docs_column(TIMESTAMP_COL_NAME).unwrap()).unwrap();
            let (min_ts, max_ts) = (
                arrow::compute::min(&ts).unwrap_or(0),
                arrow::compute::max(&ts).unwrap_or(0),
            );
            eprintln!(
                "  {key}: {} rows, ts [{min_ts}, {max_ts}], {:.1} MiB",
                reader.row_count(),
                mib(data.0.len() + data.1.as_ref().map_or(0, |b| b.len())),
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
        let fast = merge_core_files(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &fts,
            &[],
        )
        .unwrap();
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
        let rebuild = merge_core_files_rebuild(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &fts,
            &[],
        )
        .unwrap();
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
            let fast_reader = open_merged(&fast);
            let rebuild_reader = open_merged(&rebuild);
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
        for &threads in &thread_list {
            let mut opts = core_writer_options(&fts, Vec::new(), true);
            opts.encode_threads = threads;
            let writer_schema = writer_input_schema(&Arc::new((*schema).clone()));
            let mut writer = VixWriter::new(&writer_schema, opts, false);
            let t_push = std::time::Instant::now();
            for (batch, source) in batches.iter().zip(&sources) {
                writer.push_batch_with_source(batch, source, None).unwrap();
            }
            let push_elapsed = t_push.elapsed();
            let t_finish = std::time::Instant::now();
            let (data, _index, stats) = writer.finish_with_stats().unwrap();
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
                None,
            )
        };
        let file1 = build(vec![100, 90], vec!["error one", "fine"], vec!["api", "db"]);
        let file2 = build(vec![80, 70], vec!["error two", "ok"], vec!["db", "web"]);

        // corrupt the dictionary blob of file1's SIDECAR (located by tag —
        // blob order is not part of the format); the data object stays
        // intact, so a docs-only open still works
        let mut corrupt_index = file1.1.as_deref().expect("sidecar").to_vec();
        let dict_range =
            vortex_index::test_support::blob_byte_range(&corrupt_index, "dict").unwrap();
        for byte in
            &mut corrupt_index[dict_range.start..(dict_range.start + 32).min(dict_range.end)]
        {
            *byte = 0xAB;
        }
        let corrupt: BuiltPair = (file1.0.clone(), Some(bytes::Bytes::from(corrupt_index)));
        // open is footer-only under the block dictionary: corruption in the
        // dict blob surfaces at the first DICTIONARY touch, not at open —
        // and the merge maps that error to the rebuild fallback below
        let opened = open_pair(&corrupt);
        assert!(
            opened.for_each_term(&mut |_k, _d, _i| Ok(())).is_err(),
            "corruption did not break the dictionary read"
        );
        assert!(VixDocs::open(corrupt.0.clone()).is_ok());

        let latest_schema = Schema::new(schema_fields());
        let fts = vec!["log".to_string()];
        let inputs = vec![
            ("bad.vix".to_string(), corrupt),
            ("good.vix".to_string(), file2.clone()),
        ];
        let cancellation = VixMergeCancellation::new();
        let strict_error = merge_core_files_indexed_only_with_cancellation(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &fts,
            &[],
            &cancellation,
        )
        .unwrap_err();
        assert!(
            format!("{strict_error:#}").contains("required indexed merge is not applicable"),
            "large indexed batches must refuse the rebuild fallback: {strict_error:#}"
        );

        let result = merge_core_files(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &fts,
            &[],
        )
        .unwrap();
        assert!(!result.used_index_merge);

        // equivalent to rebuilding over the pristine bytes (file2 supplies
        // the same term-field set the corrupt input would have)
        let pristine = vec![
            ("f1.vix".to_string(), file1),
            ("good.vix".to_string(), file2),
        ];
        let reference = merge_core_files_rebuild(
            StreamType::Logs,
            &as_inputs(&pristine),
            &latest_schema,
            &fts,
            &[],
        )
        .unwrap();
        let result_reader = open_merged(&result);
        let reference_reader = open_merged(&reference);
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
            None,
        );
        let old_file: BuiltPair = (
            old_file.0.clone(),
            Some(bytes::Bytes::from(
                vortex_index::test_support::repack_with_tokenizer_property(
                    old_file.1.as_deref().expect("sidecar"),
                    "o2-v1",
                )
                .unwrap(),
            )),
        );
        let new_file = build_core_file(
            fields(),
            vec![
                Arc::new(Int64Array::from(vec![80])),
                Arc::new(StringArray::from(vec!["plain admin login"])),
            ],
            &fts,
            None,
        );

        let latest_schema = Schema::new(fields());
        let inputs = vec![
            ("old.vix".to_string(), old_file),
            ("new.vix".to_string(), new_file),
        ];
        let result = merge_core_files(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &fts,
            &[],
        )
        .unwrap();
        assert!(
            !result.used_index_merge,
            "the tokenizer property mismatch must force the rebuild"
        );
        // ... and the rebuild is what merge_core_files_rebuild produces
        let reference = merge_core_files_rebuild(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &fts,
            &[],
        )
        .unwrap();
        let result_reader = open_merged(&result);
        let reference_reader = open_merged(&reference);
        assert_core_files_equivalent(&result_reader, &reference_reader, "legacy tokenizer");

        // the output SIDECAR is stamped with the current tokenizer ...
        assert_eq!(
            vortex_index::test_support::tokenizer_property(
                result.index.as_deref().expect("indexed merge output"),
            )
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
            None,
        );

        // the write side: NaN becomes a JSON null inside _source ...
        let r1 = open_pair(&file1);
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
        let fast = merge_core_files(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &[],
            &[],
        )
        .unwrap();
        assert!(fast.used_index_merge);
        let rebuild = merge_core_files_rebuild(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &[],
            &[],
        )
        .unwrap();

        // merged order (ts DESC): doc0=NaN, doc1=absent, doc2=1.5, doc3=Inf
        // — both strategies agree: only the finite value keys the doc
        let fast_reader = open_merged(&fast);
        let rebuild_reader = open_merged(&rebuild);
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
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &[],
            &[],
        )
        .unwrap();
        assert!(fast.used_index_merge);
        let rebuild = merge_core_files_rebuild(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &[],
            &[],
        )
        .unwrap();

        let fast_reader = open_merged(&fast);
        let rebuild_reader = open_merged(&rebuild);
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
            None,
        );
        let latest_schema = Schema::new(fields());

        // single input: the merged file must be equivalent to the original
        let single = vec![("real.vix".to_string(), real.clone())];
        let fast = merge_core_files(
            StreamType::Logs,
            &as_inputs(&single),
            &latest_schema,
            &fts,
            &[],
        )
        .unwrap();
        assert!(fast.used_index_merge);
        let fast_reader = open_merged(&fast);
        let original_reader = open_pair(&real);
        assert_core_files_equivalent(&fast_reader, &original_reader, "single input");

        // a zero-row input alongside a real one
        let with_empty = vec![
            ("empty.vix".to_string(), empty),
            ("real.vix".to_string(), real),
        ];
        let fast = merge_core_files(
            StreamType::Logs,
            &as_inputs(&with_empty),
            &latest_schema,
            &fts,
            &[],
        )
        .unwrap();
        assert!(fast.used_index_merge);
        assert_eq!(fast.stats.row_count, 2);
        let rebuild = merge_core_files_rebuild(
            StreamType::Logs,
            &as_inputs(&with_empty),
            &latest_schema,
            &fts,
            &[],
        )
        .unwrap();
        let fast_reader = open_merged(&fast);
        let rebuild_reader = open_merged(&rebuild);
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
            None,
        );
        let latest_schema = Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("code", DataType::Int64, true),
        ]);
        let inputs = vec![("f1.vix".to_string(), file1), ("f2.vix".to_string(), file2)];

        let fast = merge_core_files(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &[],
            &[],
        )
        .unwrap();
        assert!(
            fast.used_index_merge,
            "a typed-conflict field is dropped+partial, not a fast-path rejection"
        );
        let rebuild = merge_core_files_rebuild(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &[],
            &[],
        )
        .unwrap();
        let fast_reader = open_merged(&fast);
        let rebuild_reader = open_merged(&rebuild);
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

    /// Fixed-type policy: the latest stream schema remains authoritative, but
    /// physical Boolean/Float/numeric/string variants safe-cast to that target
    /// and continue down the column-derived rebuild path.
    #[tokio::test]
    async fn merge_latest_schema_policy_safe_casts_type_drift_from_columns() {
        let file1 = build_index_off_core_file(
            vec![
                Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
                Field::new("code", DataType::Int64, true),
            ],
            vec![
                Arc::new(Int64Array::from(vec![100])),
                Arc::new(Int64Array::from(vec![Some(7)])),
            ],
        );
        let file2 = build_index_off_core_file(
            vec![
                Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
                Field::new("code", DataType::Utf8, true),
            ],
            vec![
                Arc::new(Int64Array::from(vec![90, 80])),
                Arc::new(StringArray::from(vec![Some("123"), Some("abc")])),
            ],
        );
        let latest_schema = Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("code", DataType::Utf8, true),
        ]);
        let inputs = vec![("f1.vix".to_string(), file1), ("f2.vix".to_string(), file2)];

        let rebuilt = merge_core_files_rebuild_with_caps(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &[],
            &[],
            BatchCaps {
                merge_type_policy_override: Some(MergeTypePolicy::LatestSchema),
                ..BatchCaps::default()
            },
        )
        .unwrap();
        assert!(rebuilt.terms_from_columns);
        let reader = open_merged(&rebuilt);
        assert!(
            matches!(
                reader
                    .docs_schema()
                    .unwrap()
                    .field_with_name("code")
                    .unwrap()
                    .data_type(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ),
            "the writer may normalize the fixed logical string type to Utf8View",
        );
        let code = as_string_array(&reader.read_docs_column("code").unwrap()).unwrap();
        assert_eq!(
            (0..code.len())
                .map(|row| code.is_valid(row).then(|| code.value(row)))
                .collect::<Vec<_>>(),
            vec![Some("7"), Some("123"), Some("abc")],
        );
        assert_eq!(matching_docs(&reader, &exact("code", "7")), vec![0]);
        assert_eq!(matching_docs(&reader, &exact("code", "123")), vec![1]);
        assert_eq!(matching_docs(&reader, &exact("code", "abc")), vec![2]);
        assert_eq!(matching_docs(&reader, &key_exists("code")), vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn merge_latest_schema_policy_rejects_string_to_numeric_derivation() {
        let file = build_index_off_core_file(
            vec![
                Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
                Field::new("code", DataType::Utf8, true),
            ],
            vec![
                Arc::new(Int64Array::from(vec![100, 90])),
                Arc::new(StringArray::from(vec![Some("123"), Some("abc")])),
            ],
        );
        let latest_schema = Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("code", DataType::Int64, true),
        ]);
        let inputs = vec![("string.vix".to_string(), file)];

        let rebuilt = merge_core_files_rebuild_with_caps(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &[],
            &[],
            BatchCaps {
                merge_type_policy_override: Some(MergeTypePolicy::LatestSchema),
                ..BatchCaps::default()
            },
        )
        .unwrap();
        assert!(
            !rebuilt.terms_from_columns,
            "value-dependent Utf8 parsing must stay on the _source derivation path"
        );
    }

    #[tokio::test]
    async fn merge_latest_schema_policy_casts_boolean_and_float_to_utf8_terms() {
        let file = build_index_off_core_file(
            vec![
                Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
                Field::new("flag", DataType::Boolean, true),
                Field::new("ratio", DataType::Float64, true),
            ],
            vec![
                Arc::new(Int64Array::from(vec![100, 90, 80, 70, 60])),
                Arc::new(BooleanArray::from(vec![
                    Some(true),
                    Some(false),
                    Some(true),
                    Some(false),
                    None,
                ])),
                Arc::new(Float64Array::from(vec![
                    Some(1.5),
                    Some(-2.25),
                    Some(f64::NAN),
                    Some(f64::INFINITY),
                    Some(f64::NEG_INFINITY),
                ])),
            ],
        );
        let latest_schema = Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("flag", DataType::Utf8, true),
            Field::new("ratio", DataType::Utf8, true),
        ]);
        let inputs = vec![("typed.vix".to_string(), file)];

        let rebuilt = merge_core_files_rebuild_with_caps(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &[],
            &[],
            BatchCaps {
                merge_type_policy_override: Some(MergeTypePolicy::LatestSchema),
                ..BatchCaps::default()
            },
        )
        .unwrap();
        assert!(rebuilt.terms_from_columns);
        let reader = open_merged(&rebuilt);
        assert_eq!(matching_docs(&reader, &exact("flag", "true")), vec![0, 2]);
        assert_eq!(matching_docs(&reader, &exact("flag", "false")), vec![1, 3]);
        assert_eq!(matching_docs(&reader, &exact("ratio", "1.5")), vec![0]);
        assert_eq!(matching_docs(&reader, &exact("ratio", "-2.25")), vec![1]);
        assert_eq!(
            matching_docs(&reader, &exact("ratio", "NaN")),
            Vec::<u32>::new()
        );
        assert_eq!(
            matching_docs(&reader, &exact("ratio", "inf")),
            Vec::<u32>::new()
        );
        assert_eq!(
            matching_docs(&reader, &key_exists("flag")),
            vec![0, 1, 2, 3]
        );
        assert_eq!(matching_docs(&reader, &key_exists("ratio")), vec![0, 1]);

        let ratio = as_string_array(&reader.read_docs_column("ratio").unwrap()).unwrap();
        assert_eq!(
            (0..ratio.len())
                .map(|row| ratio.is_valid(row).then(|| ratio.value(row)))
                .collect::<Vec<_>>(),
            vec![Some("1.5"), Some("-2.25"), None, None, None],
        );
        for row in 2..5 {
            let source = reader.read_source(&[row]).unwrap();
            let source: serde_json::Value = serde_json::from_str(source.value(0)).unwrap();
            assert_eq!(source.get("ratio"), None);
        }

        // The fixed-type output is durable, not merely an index-side view:
        // `_source` carries the same authoritative strings as the docs
        // columns, so a future source-driven rebuild or a legacy rollback
        // cannot silently restore numeric/bool tagged terms.
        for (row, expected) in [(0, "1.5"), (1, "-2.25")] {
            let source = reader.read_source(&[row]).unwrap();
            let source: serde_json::Value = serde_json::from_str(source.value(0)).unwrap();
            assert_eq!(source.get("ratio"), Some(&serde_json::json!(expected)));
        }
        let generation_one = (
            bytes::Bytes::from(rebuilt.output.to_bytes().unwrap()),
            rebuilt.index.clone().map(bytes::Bytes::from),
        );
        let generation_two_inputs = vec![("generation-one.vix".to_string(), generation_one)];
        let generation_two = merge_core_files_rebuild_with_caps(
            StreamType::Logs,
            &as_inputs(&generation_two_inputs),
            &latest_schema,
            &[],
            &[],
            BatchCaps {
                force_source_derivation: true,
                merge_type_policy_override: Some(MergeTypePolicy::Legacy),
                ..BatchCaps::default()
            },
        )
        .unwrap();
        assert!(!generation_two.terms_from_columns);
        let generation_two_reader = open_merged(&generation_two);
        assert_core_files_equivalent(
            &reader,
            &generation_two_reader,
            "latest-schema output after legacy source rebuild",
        );
    }

    #[tokio::test]
    async fn merge_legacy_rollback_stabilizes_mixed_typed_and_rewritten_inputs() {
        let old_typed = build_index_off_core_file(
            vec![
                Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
                Field::new("flag", DataType::Boolean, true),
                Field::new("ratio", DataType::Float64, true),
            ],
            vec![
                Arc::new(Int64Array::from(vec![100])),
                Arc::new(BooleanArray::from(vec![Some(false)])),
                Arc::new(Float64Array::from(vec![Some(2.5)])),
            ],
        );
        let canary_input = build_index_off_core_file(
            vec![
                Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
                Field::new("flag", DataType::Boolean, true),
                Field::new("ratio", DataType::Float64, true),
            ],
            vec![
                Arc::new(Int64Array::from(vec![90])),
                Arc::new(BooleanArray::from(vec![Some(true)])),
                Arc::new(Float64Array::from(vec![Some(1.5)])),
            ],
        );
        let latest_schema = Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("flag", DataType::Utf8, true),
            Field::new("ratio", DataType::Utf8, true),
        ]);
        let canary_inputs = vec![("canary-input.vix".to_string(), canary_input)];
        let canary = merge_core_files_rebuild_with_caps(
            StreamType::Logs,
            &as_inputs(&canary_inputs),
            &latest_schema,
            &[],
            &[],
            BatchCaps {
                merge_type_policy_override: Some(MergeTypePolicy::LatestSchema),
                ..BatchCaps::default()
            },
        )
        .unwrap();
        assert!(canary.terms_from_columns);
        let canary_pair = (
            bytes::Bytes::from(canary.output.to_bytes().unwrap()),
            canary.index.clone().map(bytes::Bytes::from),
        );

        // A rollback can see untouched typed files and already rewritten
        // canary output in one ordinary merge group. Force the legacy/source
        // arm and pin that each row keeps its own durable representation.
        let mixed_inputs = vec![
            ("old-typed.vix".to_string(), old_typed),
            ("canary-output.vix".to_string(), canary_pair),
        ];
        let rolled_back = merge_core_files_rebuild_with_caps(
            StreamType::Logs,
            &as_inputs(&mixed_inputs),
            &latest_schema,
            &[],
            &[],
            BatchCaps {
                force_source_derivation: true,
                merge_type_policy_override: Some(MergeTypePolicy::Legacy),
                ..BatchCaps::default()
            },
        )
        .unwrap();
        assert!(!rolled_back.terms_from_columns);
        let rolled_back_reader = open_merged(&rolled_back);
        let ratio =
            as_string_array(&rolled_back_reader.read_docs_column("ratio").unwrap()).unwrap();
        assert_eq!(
            (0..ratio.len())
                .map(|row| ratio.value(row))
                .collect::<Vec<_>>(),
            vec!["2.5", "1.5"]
        );
        let old_source: serde_json::Value =
            serde_json::from_str(rolled_back_reader.read_source(&[0]).unwrap().value(0)).unwrap();
        let canary_source: serde_json::Value =
            serde_json::from_str(rolled_back_reader.read_source(&[1]).unwrap().value(0)).unwrap();
        assert_eq!(old_source.get("ratio"), Some(&serde_json::json!(2.5)));
        assert_eq!(canary_source.get("ratio"), Some(&serde_json::json!("1.5")));
        assert_eq!(
            matching_docs(&rolled_back_reader, &tagged_numeric("ratio", "2.5")),
            vec![0]
        );
        assert_eq!(
            matching_docs(&rolled_back_reader, &exact("ratio", "1.5")),
            vec![1]
        );
        assert_eq!(
            matching_docs(&rolled_back_reader, &key_exists("ratio")),
            vec![0, 1]
        );

        // A later all-legacy generation is byte-semantically stable: terms,
        // postings, docs columns and each row's `_source` all reproduce.
        let rolled_back_pair = (
            bytes::Bytes::from(rolled_back.output.to_bytes().unwrap()),
            rolled_back.index.clone().map(bytes::Bytes::from),
        );
        let next_inputs = vec![("rolled-back.vix".to_string(), rolled_back_pair)];
        let next = merge_core_files_rebuild_with_caps(
            StreamType::Logs,
            &as_inputs(&next_inputs),
            &latest_schema,
            &[],
            &[],
            BatchCaps {
                force_source_derivation: true,
                merge_type_policy_override: Some(MergeTypePolicy::Legacy),
                ..BatchCaps::default()
            },
        )
        .unwrap();
        assert_core_files_equivalent(
            &rolled_back_reader,
            &open_merged(&next),
            "mixed canary/legacy rollback after another legacy generation",
        );
    }

    #[tokio::test]
    async fn merge_legacy_policy_keeps_non_finite_float_to_string_behavior() {
        let file = build_index_off_core_file(
            vec![
                Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
                Field::new("ratio", DataType::Float64, true),
            ],
            vec![
                Arc::new(Int64Array::from(vec![100, 90, 80])),
                Arc::new(Float64Array::from(vec![
                    Some(f64::NAN),
                    Some(f64::INFINITY),
                    Some(f64::NEG_INFINITY),
                ])),
            ],
        );
        let latest_schema = Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("ratio", DataType::Utf8, true),
        ]);
        let inputs = vec![("typed.vix".to_string(), file)];

        let rebuilt = merge_core_files_rebuild_with_caps(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &[],
            &[],
            BatchCaps {
                merge_type_policy_override: Some(MergeTypePolicy::Legacy),
                ..BatchCaps::default()
            },
        )
        .unwrap();
        assert!(!rebuilt.terms_from_columns);
        let reader = open_merged(&rebuilt);
        let ratio = as_string_array(&reader.read_docs_column("ratio").unwrap()).unwrap();
        assert_eq!(
            (0..ratio.len())
                .map(|row| ratio.value(row))
                .collect::<Vec<_>>(),
            vec!["NaN", "inf", "-inf"],
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
            &[], // NOT column-stored in this input
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

        let fast = merge_core_files(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &[],
            &[],
        )
        .unwrap();
        assert!(fast.used_index_merge, "disjoint inputs take the fast path");
        let rebuild = merge_core_files_rebuild(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &[],
            &[],
        )
        .unwrap();
        assert!(!rebuild.used_index_merge);

        for (label, result) in [("fast", fast), ("rebuild", rebuild)] {
            let reader = open_merged(&result);
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
            StreamType::Logs,
            schema,
            vec![table],
            &[],
            &[],
            false,
            0,
        )
        .await
        .unwrap();

        let reader = VixReader::open_with_index(
            bytes::Bytes::from(result.data),
            result.index.map(bytes::Bytes::from),
        )
        .unwrap();
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
            oversize_skipped: 0,
            min_ts: 1_700_000_000_000_000,
            max_ts: 1_700_000_400_000_000,
            timings: Default::default(),
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
        let mut writer = VixWriter::new(&schema, core_writer_options(&[], Vec::new(), true), false);
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
    ) -> BuiltPair {
        let plan_schema = Arc::new(Schema::new(plan_fields));
        let batch_schema = Arc::new(Schema::new(batch_fields));
        let batch = RecordBatch::try_new(batch_schema, columns).unwrap();
        let source = synthesize_source(&batch).unwrap();
        let mut writer = VixWriter::new(
            &plan_schema,
            core_writer_options(&[], Vec::new(), true),
            false,
        );
        writer
            .push_batch_with_source(&batch, &source, None)
            .unwrap();
        let (data, index) = writer.finish().unwrap();
        (bytes::Bytes::from(data), index.map(bytes::Bytes::from))
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
            let old_reader = open_pair(&old_file);
            assert!(!old_reader.has_term_capability("code"));
            let new_reader = open_pair(&new_file);
            assert!(new_reader.has_term_capability("code"));
        }

        let latest_schema = Schema::new(vec![ts_field(), svc_field(), code_field()]);
        let inputs = vec![
            ("old.vix".to_string(), old_file.clone()),
            ("new.vix".to_string(), new_file),
        ];

        // FAST-path merge: capability intersection demotes `code`
        let merged = merge_core_files(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &[],
            &[],
        )
        .unwrap();
        assert!(merged.used_index_merge, "old+new must keep the fast path");
        assert_eq!((merged.stats.min_ts, merged.stats.max_ts), (70, 100));
        let reader = open_merged(&merged);
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
        // v2 union semantics on the merged DOCS COLUMN: the old input never
        // stored `code` as a column, so its rows read NULL (never derived) —
        // while `_source` retains every value (the scan-extraction twin
        // proves it), so star reads and json_get fallbacks stay whole.
        // (Pre-v2-shaped inputs like old.vix are fabrications: production
        // v2 writers materialize every present field as a column.)
        assert_eq!(
            as_int64_array(&reader.read_docs_column("code").unwrap()).unwrap(),
            Int64Array::from(vec![None, None, Some(38i64), Some(9)]),
            "old rows null-fill; new rows carry values"
        );
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
        assert_eq!(
            matches_38,
            vec![0, 2],
            "_source keeps rows ts=100 (old) and ts=80 (new)"
        );

        // REBUILD of old+new: `code` is in the plan (the union carries the
        // NEW input's column), so terms re-derive from `_source` — old
        // rows' values included — converging to full capability
        let rebuilt = merge_core_files_rebuild(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &[],
            &[],
        )
        .unwrap();
        assert!(!rebuilt.used_index_merge);
        let reader = open_merged(&rebuilt);
        assert!(reader.has_term_capability("code"));
        let hits = matching_docs(&reader, &tagged_numeric("code", "38"));
        assert_eq!(hits, vec![0, 2], "rebuild must index old numeric rows");

        // ... while old+old (NO input carries a `code` column) stays
        // uncapable on BOTH strategies: v2 plans are the union of the
        // inputs' PRESENT columns, never the registry — a value living only
        // in `_source` of a pre-v2-shaped file is unreachable by plan
        // (production v2 writers cannot produce such files)
        let both_old = vec![
            ("old.vix".to_string(), old_file.clone()),
            ("old2.vix".to_string(), old_file),
        ];
        for (context, result) in [
            (
                "fast",
                merge_core_files(
                    StreamType::Logs,
                    &as_inputs(&both_old),
                    &latest_schema,
                    &[],
                    &[],
                )
                .unwrap(),
            ),
            (
                "rebuild",
                merge_core_files_rebuild(
                    StreamType::Logs,
                    &as_inputs(&both_old),
                    &latest_schema,
                    &[],
                    &[],
                )
                .unwrap(),
            ),
        ] {
            let reader = open_merged(&result);
            assert!(
                !reader.has_term_capability("code"),
                "{context}: no input column, no plan entry, no capability"
            );
            assert!(reader.has_term_capability("svc"), "{context}");
        }
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
        let mut writer = VixWriter::new(&schema, core_writer_options(&[], Vec::new(), true), false);
        writer
            .push_docs_rows(&timestamps, &[], &sources, None)
            .unwrap();
        let reader = {
            let (data, index) = writer.finish().unwrap();
            VixReader::open_with_index(bytes::Bytes::from(data), index.map(bytes::Bytes::from))
                .unwrap()
        };

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

    // ---------- #40: index-off (column-store-only) merges + classify ----------

    /// The default config puts EXACTLY metrics on the index-off list — the
    /// tests below pivot on StreamType::Metrics (index-off plan) vs
    /// StreamType::Logs (indexed plan) resolving through the real policy.
    fn assert_default_index_policy() {
        assert!(
            config::is_vix_index_disabled(StreamType::Metrics),
            "test premise: metrics defaults to index-off"
        );
        assert!(
            !config::is_vix_index_disabled(StreamType::Logs),
            "test premise: logs defaults to indexed"
        );
    }

    /// Build one synthetic COLUMN-STORE-ONLY core file (#40) the way the
    /// index-off move job does: no term index, EVERY schema field a docs
    /// column.
    fn build_index_off_core_file(fields: Vec<Field>, columns: Vec<ArrayRef>) -> BuiltPair {
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let source = synthesize_source(&batch).unwrap();
        let mut writer =
            VixWriter::new(&schema, core_writer_options(&[], Vec::new(), false), false);
        writer
            .push_batch_with_source(&batch, &source, None)
            .unwrap();
        let (data, index) = writer.finish().unwrap();
        (bytes::Bytes::from(data), index.map(bytes::Bytes::from))
    }

    fn index_off_fields() -> Vec<Field> {
        vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("svc", DataType::Utf8, true),
            Field::new("code", DataType::Int64, true),
        ]
    }

    fn index_off_columns(ts: Vec<i64>, svc: Vec<&str>, code: Vec<i64>) -> Vec<ArrayRef> {
        vec![
            Arc::new(Int64Array::from(ts)),
            Arc::new(StringArray::from(svc)),
            Arc::new(Int64Array::from(code)),
        ]
    }

    /// (#40 a) index-off x index-off merge: the output stays column-store
    /// only — no dictionary was ever touched (`used_index_merge` false), the
    /// row count is the inputs' sum, order stays global DESC, every docs
    /// column survives, and the merged file classifies Current under the
    /// index-off plan (healing converges, never loops). Covers BOTH docs
    /// strategies: the disjoint concat fast path and the windowed
    /// interleave.
    #[tokio::test]
    async fn index_off_merge_stays_column_store_only() {
        assert_default_index_policy();
        let latest_schema = Schema::new(index_off_fields());
        let disjoint = vec![
            (
                "a.vix".to_string(),
                build_index_off_core_file(
                    index_off_fields(),
                    index_off_columns(vec![400, 300], vec!["api", "db"], vec![1, 2]),
                ),
            ),
            (
                "b.vix".to_string(),
                build_index_off_core_file(
                    index_off_fields(),
                    index_off_columns(vec![200, 100], vec!["api", "web"], vec![3, 4]),
                ),
            ),
        ];

        let merged = merge_core_files(
            StreamType::Metrics,
            &as_inputs(&disjoint),
            &latest_schema,
            &[],
            &[],
        )
        .unwrap();
        assert!(
            !merged.used_index_merge,
            "an index-off plan never merges a dictionary"
        );
        assert_eq!(merged.stats.row_count, 4, "row counts sum");
        assert_eq!(merged.stats.index_size, 0, "no index bytes");
        assert_eq!(merged.dropped_rows, 0);
        let merged_bytes = bytes::Bytes::from(merged.output.to_bytes().unwrap());
        let reader = VixReader::open(merged_bytes.clone()).unwrap();
        assert!(!reader.has_index(), "output must stay index=none");
        assert_eq!(reader.term_count(), 0);
        assert_eq!(reader.row_count(), 4);
        assert_eq!(
            read_i64(&reader, TIMESTAMP_COL_NAME),
            vec![400, 300, 200, 100]
        );
        assert_eq!(
            read_strings(&reader, "svc"),
            vec![
                Some("api".to_string()),
                Some("db".to_string()),
                Some("api".to_string()),
                Some("web".to_string())
            ]
        );
        // the non-configured `code` column survives too: with no term index
        // the docs column is the only per-field read path (#40 widening)
        assert_eq!(read_i64(&reader, "code"), vec![1, 2, 3, 4]);
        // condition-free eval works; term evals error (scan-branch route)
        assert_eq!(reader.count(&VixQuery::All).unwrap(), 4);
        assert!(reader.eval(&exact("svc", "api")).is_err());

        // convergence: the merged output is Current under the same plan
        assert!(
            matches!(
                classify_core_file(
                    StreamType::Metrics,
                    "merged.vix",
                    vortex_index::BytesRangeSource::new("merged.vix", merged_bytes),
                    None,
                    &latest_schema,
                    &[],
                    &[],
                )
                .unwrap(),
                CoreFileStatus::Current
            ),
            "an index-off merge output must classify Current under the index-off plan"
        );

        // overlapping inputs: the default CONCATENATES (passthrough-native,
        // index-off included) — runs in min-ts-DESC input order, stamped
        // concat
        let overlapping = vec![
            disjoint[0].clone(),
            disjoint[1].clone(),
            (
                "c.vix".to_string(),
                build_index_off_core_file(
                    index_off_fields(),
                    index_off_columns(vec![350, 150], vec!["db", "api"], vec![5, 6]),
                ),
            ),
        ];
        let merged = merge_core_files(
            StreamType::Metrics,
            &as_inputs(&overlapping),
            &latest_schema,
            &[],
            &[],
        )
        .unwrap();
        assert!(!merged.used_index_merge);
        assert!(merged.concat_order, "overlap concatenates by default");
        assert_eq!(merged.stats.row_count, 6);
        let reader = open_merged(&merged);
        assert!(!reader.has_index());
        assert_eq!(reader.row_order(), RowOrder::Concat);
        assert_eq!(
            read_i64(&reader, TIMESTAMP_COL_NAME),
            vec![400, 300, 350, 150, 200, 100],
            "concat order: inputs by min_ts DESC (a: min 300, c: min 150, b: min 100)"
        );
        assert_eq!(read_i64(&reader, "code"), vec![1, 2, 5, 6, 3, 4]);

        // ... and the force_decode interleave keeps the sorted shape
        let sorted = merge_core_files_with_caps(
            StreamType::Metrics,
            &as_inputs(&overlapping),
            &latest_schema,
            &[],
            &[],
            BatchCaps {
                force_decode: true,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(!sorted.concat_order);
        let sorted_reader = open_merged(&sorted);
        assert_eq!(
            read_i64(&sorted_reader, TIMESTAMP_COL_NAME),
            vec![400, 350, 300, 200, 150, 100]
        );
        assert_eq!(read_i64(&sorted_reader, "code"), vec![1, 5, 2, 3, 6, 4]);
    }

    /// (#40 b) MIXED inputs under the index-off plan: an INDEXED input
    /// carrying a `label` column (v2 all-columns: every present field is
    /// one) merged with an index-off input that never saw the field — the
    /// union output keeps the label column, the labelless input's rows
    /// null-fill, and the output itself is column-store only.
    #[tokio::test]
    async fn index_off_merge_materializes_label_from_indexed_input() {
        assert_default_index_policy();
        let indexed_fields = vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("svc", DataType::Utf8, true),
            Field::new("label", DataType::Utf8, true),
        ];
        let indexed = build_core_file(
            indexed_fields,
            vec![
                Arc::new(Int64Array::from(vec![400, 300])),
                Arc::new(StringArray::from(vec![Some("api"), Some("db")])),
                Arc::new(StringArray::from(vec![Some("us"), None])),
            ],
            &[],
            None,
        );
        let index_off = build_index_off_core_file(
            vec![
                Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
                Field::new("svc", DataType::Utf8, true),
            ],
            vec![
                Arc::new(Int64Array::from(vec![200, 100])),
                Arc::new(StringArray::from(vec![Some("api"), Some("web")])),
            ],
        );
        let inputs = vec![
            ("indexed.vix".to_string(), indexed),
            ("off.vix".to_string(), index_off),
        ];
        let latest_schema = Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("svc", DataType::Utf8, true),
            Field::new("label", DataType::Utf8, true),
        ]);

        let merged = merge_core_files(
            StreamType::Metrics,
            &as_inputs(&inputs),
            &latest_schema,
            &[],
            &[],
        )
        .unwrap();
        assert!(!merged.used_index_merge);
        assert_eq!(merged.stats.row_count, 4);
        let reader = open_merged(&merged);
        assert!(!reader.has_index(), "index-off plan output is index=none");
        assert_eq!(
            read_i64(&reader, TIMESTAMP_COL_NAME),
            vec![400, 300, 200, 100]
        );
        assert!(
            reader.has_column_store_field("label"),
            "the label the indexed input carried only in _source must \
             materialize as a docs column"
        );
        assert_eq!(
            read_strings(&reader, "label"),
            vec![Some("us".to_string()), None, None, None],
            "derived from the indexed input's _source; null where no input \
             carried it"
        );
        assert_eq!(
            read_strings(&reader, "svc"),
            vec![
                Some("api".to_string()),
                Some("db".to_string()),
                Some("api".to_string()),
                Some("web".to_string())
            ]
        );
    }

    /// (#40 c, the rollback direction) MIXED inputs under an INDEXED plan:
    /// the fast path must fall back (an index-off input cannot join a
    /// dictionary merge), and the rebuild re-derives EVERY term from
    /// `_source` — the output is fully indexed and term queries find rows
    /// from BOTH inputs.
    #[tokio::test]
    async fn rollback_mixed_inputs_rebuild_to_indexed() {
        assert_default_index_policy();
        let latest_schema = Schema::new(index_off_fields());
        let inputs = vec![
            (
                "indexed.vix".to_string(),
                build_core_file(
                    index_off_fields(),
                    index_off_columns(vec![400, 300], vec!["api", "db"], vec![1, 2]),
                    &[],
                    None,
                ),
            ),
            (
                "off.vix".to_string(),
                build_index_off_core_file(
                    index_off_fields(),
                    index_off_columns(vec![200, 100], vec!["api", "web"], vec![3, 4]),
                ),
            ),
        ];

        let merged = merge_core_files(
            StreamType::Logs,
            &as_inputs(&inputs),
            &latest_schema,
            &[],
            &[],
        )
        .unwrap();
        assert!(
            !merged.used_index_merge,
            "the index-off input must force the rebuild fallback"
        );
        assert_eq!(merged.stats.row_count, 4);
        let reader = open_merged(&merged);
        assert!(reader.has_index(), "the indexed plan re-indexes everything");
        assert!(reader.term_count() > 0);
        assert_eq!(
            read_i64(&reader, TIMESTAMP_COL_NAME),
            vec![400, 300, 200, 100]
        );
        // term queries span BOTH inputs (row 0 from the indexed input, row 2
        // from the index-off input)
        assert_eq!(matching_docs(&reader, &exact("svc", "api")), vec![0, 2]);
        assert_eq!(matching_docs(&reader, &exact("svc", "web")), vec![3]);
        assert_eq!(
            matching_docs(&reader, &key_exists("code")),
            vec![0, 1, 2, 3]
        );
    }

    /// (#40 d) classify_core_file index-mode matrix: both drift directions
    /// classify NeedsRebuild, aligned modes classify Current, and ONE
    /// healing rebuild converges each drift direction to Current under its
    /// plan.
    #[test]
    fn classify_core_file_index_mode_matrix() {
        assert_default_index_policy();
        let latest_schema = Schema::new(index_off_fields());
        let indexed = build_core_file(
            index_off_fields(),
            index_off_columns(vec![400, 300], vec!["api", "db"], vec![1, 2]),
            &[],
            None,
        );
        let index_off = build_index_off_core_file(
            index_off_fields(),
            index_off_columns(vec![200, 100], vec!["api", "web"], vec![3, 4]),
        );

        let classify = |stream_type: StreamType, pair: &BuiltPair| {
            classify_core_file(
                stream_type,
                "probe.vix",
                vortex_index::BytesRangeSource::new("probe.vix", pair.0.clone()),
                pair.1
                    .as_ref()
                    .map(|index| vortex_index::BytesRangeSource::new("probe.vxi", index.clone())),
                &latest_schema,
                &[],
                &[],
            )
        };

        // aligned modes: the no-op verdicts
        assert!(
            matches!(
                classify(StreamType::Logs, &indexed),
                Ok(CoreFileStatus::Current)
            ),
            "indexed file under the indexed plan"
        );
        assert!(
            matches!(
                classify(StreamType::Metrics, &index_off),
                Ok(CoreFileStatus::Current)
            ),
            "index-off file under the index-off plan"
        );

        // drift: indexed file, index-off policy
        assert_needs_rebuild(
            &classify(StreamType::Metrics, &indexed),
            &["index-off"],
            "indexed file under the index-off plan",
        );
        // drift (rollback): index-off file, indexed policy
        assert_needs_rebuild(
            &classify(StreamType::Logs, &index_off),
            &["no index sidecar"],
            "index-off file under the indexed plan",
        );

        // convergence: one single-input healing rebuild per direction
        for (stream_type, data, context) in [
            (StreamType::Metrics, &indexed, "indexed -> index-off"),
            (StreamType::Logs, &index_off, "index-off -> indexed"),
        ] {
            let healed = merge_core_files_rebuild(
                stream_type,
                &as_inputs(&[("single.vix".to_string(), data.clone())]),
                &latest_schema,
                &[],
                &[],
            )
            .unwrap();
            let healed_bytes: BuiltPair = (
                bytes::Bytes::from(healed.output.to_bytes().unwrap()),
                healed.index.clone().map(bytes::Bytes::from),
            );
            let healed_reader = open_pair(&healed_bytes);
            assert_eq!(
                healed_reader.has_index(),
                stream_type == StreamType::Logs,
                "{context}: the healed output's mode follows the plan"
            );
            assert!(
                matches!(
                    classify(stream_type, &healed_bytes),
                    Ok(CoreFileStatus::Current)
                ),
                "{context}: the healed output must classify Current"
            );
        }
    }
}
