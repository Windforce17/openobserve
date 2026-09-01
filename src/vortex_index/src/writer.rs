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

//! `.vix` core-file writer.
//!
//! [`VixWriter`] consumes the record batches of one data file **in document
//! order** (`doc_id` = global row index across batches, `u32`) and produces
//! the complete `.vix` puffin container ([`VixWriter::new`] +
//! [`VixWriter::push_batch_with_source`] / [`VixWriter::push_docs_rows`]):
//! the file *is* the data file. A `docs` blob stores one row per record
//! (`_timestamp`, EVERY schema field with its arrow type, the
//! caller-supplied `_source` string and optionally `_original`); the
//! inverted index additionally emits one *key term* (`{path}\x00\xFF\xFF`)
//! per doc per non-internal column with a non-null value, and postings of
//! terms present in **every** doc are elided (written empty, doc_count still
//! exact — the reader synthesizes them).
//!
//! Every string-family column except the reserved ones is value-indexed:
//! fields in [`VixWriterOptions::fts_field_names`] emit
//! [`o2_tokenize`](crate::o2_tokenize) tokens **only** (no raw
//! whole-value term — a free-text value would otherwise become a unique
//! dictionary entry per record, the benchmark-pilot storage blowup),
//! every other string field emits the raw whole-value term —
//! including the **empty string** (`""` is a value, distinct from null; the
//! 3-byte composite key `\x00{field_id}` is valid, so `field = ''` answers
//! from the index). Numeric and boolean columns are value-indexed too:
//! each finite value emits ONE canonical, [`crate::numeric`]-tagged term
//! (`\x01` + itoa/ryu text — value-based, so JSON `38.00` and `38.0` are one
//! term while `38` and `38.0` stay distinct int/float forms the query layer
//! probes as a union). EVERY schema field (any type) is stored natively in
//! the `docs` blob (v2 all-present-columns, DESIGN §2).

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use arrow::{
    array::{
        Array, ArrayRef as ArrowArrayRef, BinaryArray, BinaryBuilder, BooleanArray, Float32Array,
        Float64Array, Int64Array, LargeStringArray, StringArray, StringViewArray, UInt32Array,
        UInt64Array,
    },
    compute::cast,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use rapidhash::fast::GlobalState;

use crate::{
    container::{
        BLOB_TAG_BLOOM, BLOB_TAG_DICT, BLOB_TAG_DICT_BLOCKS, BLOB_TAG_PLIST, BLOB_TAG_STATS,
        BLOB_TAG_TERMS, BLOB_TYPE_BLOOM, BLOB_TYPE_DICT, BLOB_TYPE_DICT_BLOCKS, BLOB_TYPE_PLIST,
        BLOB_TYPE_STATS, BLOB_TYPE_TERMS, BlobPart, DICT_LAYOUT_BLOCKS, DocsBlobEncoder,
        FIELD_TYPE_BLOOM, FIELD_TYPE_CS, FIELD_TYPE_FTS, FIELD_TYPE_TERM, FieldEntry,
        KEY_LAYOUT_FID_V2, PROP_COLUMNS, PROP_COLUMNS_COMPLETE, PROP_DICT_LAYOUT, PROP_FIELDS,
        PROP_KEY_LAYOUT, PROP_OVERSIZE_SKIPS, PROP_PARTIAL_FIELDS, PROP_PLIST_MIN_DOCS,
        PROP_ROW_COUNT, PROP_ROW_GROUP_SIZE, PROP_ROW_ORDER, PROP_ROW_REGIONS, PROP_TERM_COUNT,
        PROP_TOKENIZER, PROP_VERSION, PROP_ZONE_MAP, ROW_ORDER_CONCAT, ROW_ORDER_TS_DESC,
        TOKENIZER_ID, TermsBlobSpooler, VIX_FORMAT_VERSION, VixOutput, ZoneEntry,
        addressable_strategy, build_container_parts, finish_streamed_container, write_vortex_blob,
    },
    error::{Result, VixError},
    merge::{self, DocIdMap},
    numeric::{
        NUMERIC_TERM_TAG, canonical_bool_text, canonical_f32_text, canonical_f64_text,
        canonical_i64_text, canonical_number_text, canonical_u64_text,
    },
    postings,
    query::{KEY_FIELD_ID, MAX_REAL_FIELD_ID, write_composite},
    reader::VixReader,
    spill,
    stats::{ColumnStatsFolder, SpliceableStats},
    term_accumulator::{SortedTermShard, TermAccumulator},
    tokenizer::o2_tokenize,
};

/// The timestamp column: never term-indexed, always stored (as `i64`)
/// when present. Mirrors `config::TIMESTAMP_COL_NAME`; hardcoded locally to
/// keep this crate dependency-light.
pub const TIMESTAMP_COL_NAME: &str = "_timestamp";
/// The unique-id column, never term-indexed (mirrors `config::ID_COL_NAME`).
pub const ID_COL_NAME: &str = "_o2_id";
/// The original-record column, never term-indexed (mirrors
/// `config::ORIGINAL_DATA_COL_NAME`). In core files it is an optional
/// `docs` column supplied through [`VixWriter::push_batch_with_source`].
pub const ORIGINAL_DATA_COL_NAME: &str = "_original";
/// The serialized-record column of the `docs` blob, supplied by the
/// caller through [`VixWriter::push_batch_with_source`]. It must never
/// appear as an input batch column.
pub const SOURCE_COL_NAME: &str = "_source";
/// Replacement name for a *user* field literally named `_source`
/// ([`SOURCE_COL_NAME`] is reserved for the serialized record): the ingest
/// guard and the move job rename such fields to this so their values survive
/// in the stored record instead of being silently dropped.
pub const SOURCE_RENAMED_COL_NAME: &str = "_source_field";

/// Internal columns: never term-indexed and never given key terms.
pub(crate) const NON_INDEXED_COLS: [&str; 3] =
    [TIMESTAMP_COL_NAME, ID_COL_NAME, ORIGINAL_DATA_COL_NAME];

type FastHashMap<K, V> = std::collections::HashMap<K, V, GlobalState>;
type FastHashSet<K> = std::collections::HashSet<K, GlobalState>;

fn is_string_family(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

/// #52 AUTO bloom-only demotion — the ONE rule shared by its two call
/// sites (M7): the merge planner (`build_merge_plan` in the core crate,
/// counting distinct terms from the INPUT dictionaries) and the writer's
/// own finish (first encode / rebuild, counting from the accumulated term
/// map). A candidate whose distinct-value count clears the absolute
/// `min_distinct` floor AND whose distinct/rows ratio clears `ratio` is
/// demoted, unless named in `never`. `ratio <= 0` disables. Candidates are
/// pre-filtered by the caller (string-family ∩ term plan − fts; the writer
/// construction resolution re-checks for the merge site), so the numeric
/// thresholds here are the whole decision. `site` labels the log line
/// (`"merge"` / `"build"`). Returned names are sorted and deduplicated.
pub fn resolve_auto_bloom_only<'a>(
    candidates: impl IntoIterator<Item = (&'a str, u64)>,
    rows: u64,
    ratio: f64,
    min_distinct: u64,
    never: &[String],
    site: &str,
) -> Vec<String> {
    if ratio <= 0.0 || rows == 0 {
        return Vec::new();
    }
    let mut selected: Vec<String> = candidates
        .into_iter()
        .filter(|&(name, distinct)| {
            distinct >= min_distinct
                && distinct as f64 / rows as f64 >= ratio
                && !never.iter().any(|n| n == name)
        })
        .map(|(name, distinct)| {
            log::info!(
                "vix {site}: AUTO bloom-only demotion of {name:?} \
                 (distinct≈{distinct} / rows={rows})"
            );
            name.to_string()
        })
        .collect();
    selected.sort_unstable();
    selected.dedup();
    selected
}

/// THE value policy for hashing one bloom-only field's docs-column values
/// into a composite-key hash set (#52; #41: values over `max_raw_term_len`
/// are skipped silently — merge mode carries the inputs' stamped oversize
/// allowances, counting here would double them; nulls hash nothing;
/// non-string columns hash nothing). ONE implementation shared by the
/// writer's inline absorption ([`VixWriter::absorb_bloom_only_columns`] /
/// the streamed-merge push) and the detached M12 [`BloomOnlyHasher`], so
/// every path derives bit-identical coverage by construction.
fn hash_bloom_only_column_values(
    bloom_name: &str,
    column: &ArrowArrayRef,
    max_raw_term_len: usize,
    scratch: &mut Vec<u8>,
    sink: &mut FastHashSet<u64>,
) {
    if let Some(strings) = StringColumn::try_new(column.as_ref()) {
        for row in 0..column.len() {
            let Some(value) = strings.value(row) else {
                continue;
            };
            if value.len() > max_raw_term_len {
                continue;
            }
            if let Some(k) =
                crate::bloom::composite_value_key(bloom_name, value.as_bytes(), scratch)
            {
                sink.insert(crate::sbbf::hash_value(k));
            }
        }
    }
}

/// The `_source`-projected sibling of [`hash_bloom_only_column_values`]
/// (#51c-d: fields with NO docs column in an input — their values live only
/// in `_source`). Same value policy; `insert` routes each hash to its
/// field's sink. Errors name the offending row.
fn hash_bloom_only_source_values(
    source: &ArrowArrayRef,
    wanted: &[(String, u16)],
    max_raw_term_len: usize,
    scratch: &mut Vec<u8>,
    mut insert: impl FnMut(u16, u64),
) -> anyhow::Result<()> {
    use sonic_rs::JsonValueTrait;
    let Some(strings) = StringColumn::try_new(source.as_ref()) else {
        return Err(VixError::Writer(format!(
            "absorb_bloom_only_source: the {SOURCE_COL_NAME:?} column is {} — expected a \
             string array",
            source.data_type()
        ))
        .into());
    };
    for row in 0..source.len() {
        let Some(text) = strings.value(row) else {
            // `_source` is non-null by the docs-schema contract; treat a
            // stray null defensively as an absent record
            continue;
        };
        for entry in sonic_rs::to_object_iter(text) {
            let (key, value) = entry.map_err(|e| {
                VixError::Writer(format!(
                    "absorb_bloom_only_source: _source of row {row} is not a JSON object: {e}"
                ))
            })?;
            let key: &str = key.as_ref();
            let Some((name, fid)) = wanted.iter().find(|(name, _)| name == key) else {
                continue;
            };
            let Some(value) = value.as_str() else {
                // non-string values never hash (bloom-only fields are
                // string-family; a drifted row's value has no raw term)
                continue;
            };
            if value.len() > max_raw_term_len {
                continue;
            }
            if let Some(k) = crate::bloom::composite_value_key(name, value.as_bytes(), scratch) {
                insert(*fid, crate::sbbf::hash_value(k));
            }
        }
    }
    Ok(())
}

/// M12: one parallel coverage-scan worker's detached bloom-only hashing
/// state — created by [`VixWriter::bloom_only_hasher`] over exactly the
/// fields that worker's input must scan, filled off the writer on a scan
/// thread, folded back with [`VixWriter::absorb_bloom_only_hashes`]. The
/// two hashing entry points delegate to the SAME value-policy functions the
/// writer's inline absorption uses.
pub struct BloomOnlyHasher {
    /// fid -> (bloom field name, this worker's hash set)
    sets: FastHashMap<u16, (String, FastHashSet<u64>)>,
    max_raw_term_len: usize,
    scratch: Vec<u8>,
}

impl BloomOnlyHasher {
    /// No tracked fields — the caller can skip the scan entirely.
    pub fn is_empty(&self) -> bool {
        self.sets.is_empty()
    }

    /// The tracked field names (sorted — the scan projection).
    pub fn field_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.sets.values().map(|(name, _)| name.clone()).collect();
        names.sort_unstable();
        names
    }

    /// Hash tracked fields' values from materialized docs columns
    /// (untracked columns are ignored).
    pub fn hash_columns(&mut self, cs_columns: &[(String, ArrowArrayRef)]) {
        for (name, column) in cs_columns {
            let Some(fid) = self
                .sets
                .iter()
                .find_map(|(fid, (n, _))| (n == name).then_some(*fid))
            else {
                continue;
            };
            let Some((bloom_name, sink)) = self.sets.get_mut(&fid) else {
                continue;
            };
            hash_bloom_only_column_values(
                bloom_name,
                column,
                self.max_raw_term_len,
                &mut self.scratch,
                sink,
            );
        }
    }

    /// M17: a borrowed per-field raw-value sink applying THE value policy
    /// ([`hash_bloom_only_column_values`]'s per-value body) to caller-fed
    /// byte slices — the encoded-chunk coverage scan's tight loop, which
    /// hashes straight off dict values / FSST-decompressed slices without
    /// per-value field lookups or arrow materialization. `None` = the field
    /// is not tracked by this hasher (the caller skips its scan).
    pub fn raw_sink(&mut self, field: &str) -> Option<RawValueSink<'_>> {
        let Self {
            sets,
            max_raw_term_len,
            scratch,
        } = self;
        let entry = sets
            .iter_mut()
            .find_map(|(_, entry)| (entry.0 == field).then_some(entry))?;
        let (name, sink) = (&entry.0, &mut entry.1);
        Some(RawValueSink {
            name,
            max_raw_term_len: *max_raw_term_len,
            scratch,
            sink,
        })
    }

    /// Test/diagnostic view: per-field hash sets (name -> sorted hashes) —
    /// the byte-equality pins compare these across scan implementations.
    pub fn hash_sets(&self) -> std::collections::BTreeMap<String, Vec<u64>> {
        self.sets
            .values()
            .map(|(name, set)| {
                let mut hashes: Vec<u64> = set.iter().copied().collect();
                hashes.sort_unstable();
                (name.clone(), hashes)
            })
            .collect()
    }

    /// Hash the named tracked fields' values out of `_source` (#51c-d — the
    /// fields with no docs column in this input).
    pub fn hash_source(&mut self, source: &ArrowArrayRef, fields: &[String]) -> anyhow::Result<()> {
        let wanted: Vec<(String, u16)> = self
            .sets
            .iter()
            .filter(|(_, (name, _))| fields.iter().any(|f| f == name))
            .map(|(fid, (name, _))| (name.clone(), *fid))
            .collect();
        if wanted.is_empty() {
            return Ok(());
        }
        let mut scratch = std::mem::take(&mut self.scratch);
        let sets = &mut self.sets;
        let result = hash_bloom_only_source_values(
            source,
            &wanted,
            self.max_raw_term_len,
            &mut scratch,
            |fid, hash| {
                if let Some((_, sink)) = sets.get_mut(&fid) {
                    sink.insert(hash);
                }
            },
        );
        self.scratch = scratch;
        result
    }
}

/// M17: one tracked field's raw-value hash sink (see
/// [`BloomOnlyHasher::raw_sink`]). [`Self::observe`] is the per-value body
/// of [`hash_bloom_only_column_values`] verbatim — len gate, composite key,
/// [`crate::sbbf::hash_value`] — so any scan feeding it raw value bytes
/// derives bit-identical coverage to the decoded-column path.
pub struct RawValueSink<'a> {
    name: &'a str,
    max_raw_term_len: usize,
    scratch: &'a mut Vec<u8>,
    sink: &'a mut FastHashSet<u64>,
}

impl RawValueSink<'_> {
    /// Apply the value policy to one NON-NULL raw value's bytes.
    #[inline]
    pub fn observe(&mut self, value: &[u8]) {
        if value.len() > self.max_raw_term_len {
            return;
        }
        if let Some(k) = crate::bloom::composite_value_key(self.name, value, self.scratch) {
            self.sink.insert(crate::sbbf::hash_value(k));
        }
    }
}

/// Types whose values are term-indexed: the string family (raw whole-value /
/// fts token terms) plus every type whose arrow-json `_source` image is a
/// JSON number or boolean — those emit tagged canonical value terms (see
/// [`crate::numeric`]). Types with a non-scalar or type-morphing `_source`
/// image (Timestamp becomes an ISO string, Decimal/Binary/... ) are excluded:
/// their term derivation could not agree between the column-driven writer and
/// a `_source`-driven rebuild. Public so the compaction planner selects the
/// same field set the writer would.
pub fn is_value_indexed_type(data_type: &DataType) -> bool {
    is_string_family(data_type)
        || matches!(
            data_type,
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
        )
}

/// Build-time options for [`VixWriter`].
///
/// Note: growing this struct requires touching every field-by-field
/// constructor (`core_writer_options` in the core crate; everything else
/// spreads `..Default::default()`); per-file switches (e.g.
/// `store_original`) are parameters of [`VixWriter::new`] instead.
///
/// Which fields become docs columns is NOT an option (v2, DESIGN §2): EVERY
/// schema field is stored as a native Vortex column — the docs schema is
/// `_timestamp` + all present fields + `_source` (+ `_original` opt-in).
#[derive(Debug, Clone)]
pub struct VixWriterOptions {
    /// Fields whose values are additionally tokenized for full-text search.
    pub fts_field_names: Vec<String>,
    /// Raw-string term-indexed fields to record per-file value blooms for
    /// (the `bloom` puffin blob, built as a byproduct of term emission —
    /// see [`crate::bloom`]). Typically `trace_id`/`span_id`.
    pub bloom_field_names: Vec<String>,
    /// #48: additionally build the reserved COMPOSITE bloom section —
    /// `{field name}\0{value}` keys for EVERY term field — so equality on
    /// any field is bloom-decidable ([`crate::bloom::COMPOSITE_BLOOM_FIELD`]).
    pub bloom_composite: bool,
    /// #52: fields demoted from the term index to BLOOM-ONLY — no
    /// dictionary entries or postings; their raw string values are hashed
    /// into the composite bloom (and the per-field bloom when also in
    /// `bloom_field_names`). Equality on them = file-level bloom prune +
    /// in-file filter-back scan. Resolution keeps string-family term-plan
    /// fields only; `bloom_only_never` wins over both this list and any
    /// caller-side auto demotion.
    pub bloom_only_field_names: Vec<String>,
    pub bloom_only_never: Vec<String>,
    /// #52/M7 AUTO demotion at FIRST ENCODE: when `> 0`, a string-family
    /// non-fts term field whose distinct-value count (from the writer's own
    /// accumulated term map at `finish`) clears `bloom_only_min_distinct`
    /// AND whose distinct/rows ratio clears this value is demoted to
    /// bloom-only — the exact sidecar semantics of a construction-list
    /// demotion (marker, composite coverage, no dict/postings). The rule is
    /// [`resolve_auto_bloom_only`], shared with the merge planner's
    /// input-dictionary AUTO. Skipped when the term map SPILLED (partial
    /// resident counts would undercount) — move-job builds never spill.
    /// `0.0` (the crate default) disables; production wires
    /// `ZO_VIX_BLOOM_ONLY_AUTO_RATIO` (default-on in v2).
    pub bloom_only_auto_ratio: f64,
    /// Absolute distinct-count floor for [`Self::bloom_only_auto_ratio`] —
    /// small files' noisy ratios must not demote real fields.
    pub bloom_only_min_distinct: u64,
    /// False-positive probability of the per-file value blooms.
    pub bloom_fpp: f64,
    /// Target byte size of one postings row block (point-read granularity).
    pub postings_chunk_bytes: usize,
    /// **Raw** (non-fts) values longer than this many bytes are skipped from
    /// the term index WITHOUT degrading the field (owner call 2026-08-12,
    /// performance-first): the index stays authoritative for the field, so
    /// an equality probe for one of the skipped oversize literals silently
    /// misses its rows — the accepted trade. Skips are counted in
    /// [`VixWriterStats::oversize_skipped`]. Key terms are still emitted for
    /// the skipped rows, keeping `IS [NOT] NULL` exact. Fts fields are never
    /// gated by it: their values tokenize regardless of length (tokens are
    /// byte-bounded by [`Self::max_token_len`]). The bound itself is a
    /// format limit — a composite term key `{token}{fid}` must fit the
    /// dictionary's 64 KiB key space.
    pub max_raw_term_len: usize,
    /// Logical row-group size recorded as a file property (a grouping
    /// constant for downstream row-id encodings). `0` = unknown. The `docs`
    /// chunks are sized by [`Self::docs_chunk_bytes`] instead.
    pub row_group_size: usize,
    /// Uncompressed-byte budget of one `docs`-blob chunk — the
    /// decompression unit of a matched-row point read. Rows per chunk =
    /// `clamp(budget / avg_row_present_bytes, 64, 65536)`, where a row's
    /// weight is the sum of its PRESENT (non-null) values' byte lengths
    /// plus a small per-present-value overhead — NEVER whole-row arrow
    /// width (H1, DESIGN §3: 2,557 nullable Utf8 columns ≈ 10.5 KiB/row of
    /// arrow padding even all-null used to collapse rows-per-chunk on wide
    /// sparse schemas). The low floor lets the byte budget govern even
    /// multi-KiB rows — a 1024-row floor used to inflate ~4 KiB-row chunks
    /// to hundreds of times a small budget — while the ceiling bounds
    /// decoded batch sizes. Vortex's write pipeline still coalesces
    /// sub-1 MiB chunks up to ~1 MiB (its S3-tuned segment minimum, in
    /// multiples of the row count above), so the effective decode unit is
    /// ≈ `max(budget, 1 MiB)` — plus the 64-row floor for pathological
    /// >16 KiB average rows. `0` = the 16 MiB default
    /// ([`DEFAULT_DOCS_CHUNK_BYTES`]).
    pub docs_chunk_bytes: usize,
    /// Rows-per-chunk CEILING of the [`Self::docs_chunk_bytes`] clamp
    /// (`0` = [`DEFAULT_DOCS_CHUNK_MAX_ROWS`], 65,536 — the historical hard
    /// cap, unchanged by the M9 budget flip). The cap is what bounds a
    /// huge byte budget: at ~1 KiB average rows a 64 MiB budget already
    /// saturates it, so budgets beyond that change nothing unless this is
    /// raised too. M8 chunk-size sweep knob — raising it toward the file's
    /// row count makes the whole file one chunk, which is one zone-table
    /// entry (no intra-file `_timestamp`/stats pruning), one `RowSelection`
    /// granule and one DECOMPRESSION unit per matched-row point read.
    /// Values below the 64-row floor are raised to the floor.
    pub docs_chunk_max_rows: usize,
    /// Minimum full-text token length in **bytes** (clamped to `>= 2`;
    /// see [`crate::o2_tokenize`]).
    pub min_token_len: usize,
    /// Maximum full-text token length in **bytes** (clamped to `>= 64`,
    /// exclusive bound; see [`crate::o2_tokenize`]).
    pub max_token_len: usize,
    /// Per-writer blob encode policy at `finish` (and for compaction index
    /// blob writes). `0`/`1` keeps child compression on the calling thread;
    /// values above one submit CPU leaves to the bounded process-wide
    /// executor instead of creating a private pool. The compactor supplies
    /// `ZO_VIX_MERGE_THREAD_NUM`.
    pub encode_threads: usize,
    /// #51b: range parallelism of the compaction index merge's k-way phase
    /// ([`Self::merge_input_indexes`]) — the OUTPUT key space is split into
    /// real-key ranges merged concurrently. `0` (the default) =
    /// `min(available_parallelism, 8)`; `1` = exactly one range, the
    /// sequential path through the same code. Always additionally capped by
    /// the per-merge thread budget passed to `merge_input_indexes`, so it
    /// stacks with (never widens) `ZO_VIX_MERGE_THREAD_NUM`. Production
    /// wires `ZO_VIX_MERGE_KWAY_THREADS`.
    ///
    /// M17 item 4: the same knob drives the REBUILD's parallel index-blob
    /// build (the writer's own unspilled term map, range-partitioned at
    /// field boundaries — byte-identical output for any value, capped by
    /// `encode_threads`).
    pub merge_kway_threads: usize,
    /// Arrow-bytes budget of the pre-encode sample that locks the docs
    /// blob's rows-per-chunk before the streaming encode starts (see
    /// [`DOCS_ENCODE_SAMPLE_BYTES`], the `0` default). Tests shrink it to
    /// force the sample→stream transition on small data; production keeps
    /// the default.
    pub docs_encode_sample_bytes: usize,
    /// Directory for term-accumulation SPILL runs (external sort of the
    /// build/rebuild term map — see [`crate::spill`]). `None` (the default)
    /// never spills: the map grows unbounded, the historical behavior —
    /// right for move-job builds, whose files are small. The compaction
    /// merge sets it (a scratch dir on the compactor's data volume) so a
    /// 10 GB-group REBUILD is no longer bound by ~100 bytes × every
    /// distinct term (~15-19 GB observed); runs k-way merge back into the
    /// same sink at finish, producing byte-identical blobs.
    pub term_spill_dir: Option<std::path::PathBuf>,
    /// Estimated resident bytes of the term map that trigger a spill.
    /// `0` = [`crate::spill::DEFAULT_TERM_SPILL_BYTES`] (1.5 GiB). Tests
    /// shrink it to force multi-run merges on small data.
    pub term_spill_bytes: usize,
    /// Spool the finished container to a temp file in this directory
    /// instead of RAM (retrieve it via [`VixWriter::finish_output`]) —
    /// the compaction paths set it so a multi-GB merged container never
    /// resides in memory; uploads stream from the spool. `None` (default)
    /// keeps the container in memory, the move-job shape.
    pub output_spool_dir: Option<std::path::PathBuf>,
    /// Doc-count threshold at/above which a term's postings are written
    /// OUT-OF-ROW into the `plist` blob: the terms cell becomes a 12-byte
    /// `[u64 LE offset][u32 LE len]` pointer into that blob, whose bytes
    /// there are the [`crate::postings::encode_record`] skip-table record
    /// (a ranged reader can rank/probe the term by fetching a few KB
    /// instead of a multi-MB inline cell). Dense elision takes precedence:
    /// a term in every row keeps its EMPTY cell regardless of the
    /// threshold. The threshold is persisted as the `plist_min_docs` file
    /// property, and readers distinguish pointer from inline cells ONLY by
    /// `doc_count >= threshold` — never by sniffing cell bytes. `0` (the
    /// default) disables the feature entirely: no `plist` blob, no
    /// property, byte-identical output to pre-plist writers.
    pub postings_plist_min_docs: u32,
    /// `false` (#40, index-off stream types e.g. metrics; #42 L0 builds):
    /// NO term index is built — no value/key/source term emission, and NO
    /// `.vxi` sidecar is produced at all (`stats.index_size = 0`), so
    /// readers void every dictionary-absence proof by construction. The
    /// docs blob, `_source`, zone table and stats are unchanged: the file
    /// is column-store only.
    pub index_enabled: bool,
    /// H2 per-column chunk stats: presence-density threshold below which a
    /// docs column gets NO per-chunk stats rows (file-level presence only).
    /// `0.0` = [`crate::stats::DEFAULT_STATS_MIN_DENSITY`].
    pub stats_min_density: f64,
    /// H2: byte cap of the serialized `stats` blob (densest columns kept
    /// first). `0` = [`crate::stats::DEFAULT_STATS_MAX_BYTES`].
    pub stats_max_bytes: usize,
    /// #51c docs-chunk passthrough (default `false`): the docs blob is
    /// written with the passthrough strategy, which copies already-encoded
    /// chunks pushed through the encoded-run API
    /// ([`VixWriter::begin_docs_encoded_run`]) WITHOUT decompressing or
    /// recompressing them, while arrow batches pushed the normal way still
    /// compress through the BtrBlocks compact pipeline. The blob format is
    /// unchanged (same container, same blob type, same reader paths); the
    /// write pipeline differs: no vortex zoned stats / dict layout /
    /// coalescing (per-chunk pruning stats absent — readers fail open) and
    /// no vortex footer FileStatistics. Only the compaction merge's
    /// disjoint fast path sets this.
    pub docs_passthrough: bool,
    /// #51c-c concatenation-order output (default `false`): the rows this
    /// writer stores are NOT globally `_timestamp` DESC — they are the
    /// merge inputs' runs concatenated (each run internally DESC). The
    /// finished file is stamped `row_order=concat` so every order-dependent
    /// read fast path (declared file sort order, first/last-row stats,
    /// first-set-bits top-N candidates) refuses to trust the file's row
    /// order; `false` stamps `row_order=ts_desc` (the storage convention,
    /// now explicit). Storage-only: the writer's own accounting (ts range,
    /// zone folding, doc ids) is order-free either way. Only the compaction
    /// merge's concatenation-order path sets this.
    pub concat_row_order: bool,
    /// §4: the CALLER asserts the all-present-columns invariant — every
    /// field present in any pushed row's `_source` is also a docs column.
    /// Stamps the `columns_complete` property, which is what licenses the
    /// query path's "predicate on an absent column skips the whole file"
    /// pruning. Producers whose batch shape upholds DESIGN §2 set it; a
    /// merge sets it only when EVERY input carried it (incomplete inputs'
    /// `_source` rows may hide fields that never became columns). Default
    /// `false` (no pruning license) — raw-writer tests that fake
    /// `_source`-only fields stay honest automatically.
    pub columns_complete: bool,
}

/// Default [`VixWriterOptions::docs_chunk_bytes`]: 16 MiB (owner call
/// 2026-08-18 on the M8 chunk-size sweep, S2: merge wall −25% / merge VmHWM
/// −17%, storage-neutral; cost ~2x `_source` point-read decode. 4 MiB — the
/// point-read-optimal setting — remains a knob, `ZO_VIX_DOCS_CHUNK_BYTES`).
pub const DEFAULT_DOCS_CHUNK_BYTES: usize = 16 * 1024 * 1024;
/// Default [`VixWriterOptions::docs_chunk_max_rows`]: the 65,536-row
/// ceiling that has always bounded rows-per-chunk (decoded batch sizes).
pub const DEFAULT_DOCS_CHUNK_MAX_ROWS: usize = 65536;
/// Rows-per-chunk clamp FLOOR of the `docs` blob (see
/// [`VixWriterOptions::docs_chunk_bytes`]). The floor is low so the byte
/// budget governs wide rows too; with the 16 MiB default budget it only
/// engages beyond ~256 KiB average rows. The ceiling is
/// [`VixWriterOptions::docs_chunk_max_rows`].
const DOCS_CHUNK_MIN_ROWS: usize = 64;

/// #51c: rows-per-chunk locked when a PASSTHROUGH writer's first push is an
/// encoded run (empty sample — [`docs_rows_per_chunk`] would return the
/// empty-file `0`, which disables zone folding for later re-encoded rows).
/// A zone window + re-encode slicing width only; matches the core
/// producers' docs batch row cap.
const PASSTHROUGH_FALLBACK_ROWS_PER_CHUNK: usize = 8192;

/// Arrow-bytes budget of the pre-encode sample that locks the docs blob's
/// rows-per-chunk: pushed docs batches buffer until they reach it, then the
/// streaming [`DocsBlobEncoder`] starts with [`docs_rows_per_chunk`] computed
/// over the sample and every batch — sample included — encodes incrementally.
/// Files smaller than the budget buffer entirely, so their average is exact
/// and their output matches the historical everything-buffered writer;
/// larger files trade the tail's influence on the average row size for a
/// bounded memory profile (the chunk size is a read-side decompression
/// budget, not a format invariant — each file self-describes through its own
/// layout and zone table). Before this, a compaction merge kept EVERY stored
/// batch alive until `finish` — ~10 GB of arrow for a 10 GB-original group,
/// the dominant term of the compactor's ~24 GB merge peak.
const DOCS_ENCODE_SAMPLE_BYTES: usize = 256 * 1024 * 1024;

impl Default for VixWriterOptions {
    fn default() -> Self {
        Self {
            fts_field_names: Vec::new(),
            bloom_field_names: Vec::new(),
            bloom_composite: false,
            bloom_only_field_names: Vec::new(),
            bloom_only_never: Vec::new(),
            bloom_only_auto_ratio: 0.0,
            bloom_only_min_distinct: 65536,
            bloom_fpp: crate::bloom::DEFAULT_FILE_BLOOM_FPP,
            postings_chunk_bytes: 128 * 1024,
            max_raw_term_len: 65532,
            row_group_size: 0,
            docs_chunk_bytes: DEFAULT_DOCS_CHUNK_BYTES,
            docs_chunk_max_rows: DEFAULT_DOCS_CHUNK_MAX_ROWS,
            min_token_len: 2,
            max_token_len: 64,
            encode_threads: 0,
            merge_kway_threads: 0,
            docs_encode_sample_bytes: 0,
            term_spill_dir: None,
            term_spill_bytes: 0,
            output_spool_dir: None,
            postings_plist_min_docs: 0,
            stats_min_density: 0.0,
            stats_max_bytes: 0,
            index_enabled: true,
            docs_passthrough: false,
            concat_row_order: false,
            columns_complete: false,
        }
    }
}

/// Size/count statistics of one finished `.vix` file, returned by
/// [`VixWriter::finish_with_stats`].
#[derive(Debug, Clone, Copy, Default)]
pub struct VixWriterStats {
    /// Documents in the file.
    pub row_count: u64,
    /// Composite terms (values, tokens and key terms).
    pub term_count: u64,
    /// TOTAL byte size of the `.vxi` index sidecar object (container
    /// overhead included) — the `FileMeta::index_size` value. `0` ⟺ no
    /// sidecar was produced (index-off builds), exactly the marker warmup
    /// and the bloom queue key on.
    pub index_size: u64,
    /// Bytes of the stored-records blob (`docs`).
    pub docs_size: u64,
    /// Raw (non-fts) values skipped from the term index for exceeding
    /// [`VixWriterOptions::max_raw_term_len`]. The field is NOT degraded to
    /// `partial_fields` for these (owner call 2026-08-12): equality probes
    /// for the skipped literals themselves may silently miss — observability
    /// for that trade lives in this counter.
    pub oversize_skipped: u64,
    /// Smallest `_timestamp` among the stored rows (`0` for an empty file).
    /// Computed from the actual data the writer stored — the authoritative
    /// source for `FileMeta::min_ts` (never trust upstream footer stats).
    pub min_ts: i64,
    /// Largest `_timestamp` among the stored rows (`0` for an empty file).
    pub max_ts: i64,
}

/// Builder of one `.vix` core file. See the [module docs](self).
pub struct VixWriter {
    opts: VixWriterOptions,
    /// Whether the `docs` blob carries an `_original` column.
    store_original: bool,
    /// Term-indexed field names sorted by name; the index is the field id.
    term_fields: Vec<String>,
    term_field_ids: FastHashMap<String, u16>,
    /// Term-indexed fields that also emit full-text tokens.
    fts_fields: FastHashSet<String>,
    /// Term fields with non-string (canonical tagged) value terms — kept out
    /// of the composite bloom coverage (see `new_inner`).
    non_string_term_fields: FastHashSet<String>,
    /// #52 bloom-only fields: `fid -> (name, distinct composite-value-key
    /// hashes)`. Values observed at push time (deduped here so bloom sizing
    /// stays exact); folded into the bloom accumulation at finish. These
    /// fids never reach `self.terms`.
    bloom_only: FastHashMap<u16, (String, FastHashSet<u64>)>,
    /// Column-store fields present in the schema (`_timestamp` excluded).
    cs_fields: BTreeSet<String>,
    /// Arrow schema of the `docs` blob.
    docs_schema: SchemaRef,
    /// Field-sharded term -> ascending doc ids (deduped on push).
    terms: TermAccumulator,
    /// Reusable numeric-tag buffer (`\x01{canonical}`) fed to the layout
    /// composite builder.
    tag_scratch: Vec<u8>,
    /// External-sort state: sorted runs already drained from `terms`.
    /// `None` until the first spill (and always `None` when
    /// [`VixWriterOptions::term_spill_dir`] is unset).
    term_spill: Option<spill::TermSpill>,
    partial_fields: BTreeSet<String>,
    /// Per-field count of raw values skipped for exceeding
    /// [`VixWriterOptions::max_raw_term_len`] — stamped as the
    /// `oversize_skips` property (never `partial_fields`) so the
    /// dictionary-serve reconciliation can treat the shortfall as an exact
    /// allowance; total reported via [`VixWriterStats::oversize_skipped`].
    oversize_skips: BTreeMap<String, u64>,
    /// Docs batches buffered while the chunk-size sample is still open
    /// ([`DOCS_ENCODE_SAMPLE_BYTES`]); once the streaming encoder starts
    /// this stays empty.
    sample_batches: Vec<RecordBatch>,
    /// Arrow in-memory bytes of `sample_batches`.
    sample_bytes: usize,
    /// The streaming docs-blob encoder: spawned when the sample closes (or
    /// at finish, for files that never crossed the budget), it encodes
    /// pushed batches as they arrive so the writer never holds the whole
    /// file's decoded rows.
    docs_encoder: Option<DocsBlobEncoder>,
    /// M18: encoded column chunks the passthrough write strategy had to
    /// canonicalize + re-encode because their tree carried an encoding the
    /// file writer cannot serialize (per-chunk fail-open — see
    /// [`crate::container::docs_passthrough_strategy`]). Incremented on the
    /// encoder worker; final after [`Self::finish`]/`finish_output` joins
    /// it. Callers keep a clone of the handle
    /// ([`Self::docs_failopen_counter`]) to read the count after finish.
    docs_failopen_chunks: Arc<std::sync::atomic::AtomicU64>,
    /// `_timestamp` zone folding over the pushed rows, windowed by the
    /// locked rows-per-chunk. Lives and dies with `docs_encoder`.
    zone_folder: Option<ZoneMapFolder>,
    /// #51c: the open encoded-chunk run
    /// ([`Self::begin_docs_encoded_run`]..[`Self::finish_docs_encoded_run`]).
    /// While open, every other push path is rejected — the run's zone
    /// entries were spliced at begin, so foreign rows interleaving would
    /// corrupt the zone table's row order.
    encoded_run: Option<EncodedRunState>,
    /// #51c heal mode: `Some(rows indexed so far)` once the first
    /// index-only push ([`Self::push_docs_rows_index_only`] /
    /// [`Self::push_batch_with_source_index_only`]) arrives — the doc-id
    /// cursor of an index whose docs rows are stored separately through the
    /// encoded-run API. While set, the coupled push paths are rejected
    /// (they would advance `row_count` AND assign doc ids, colliding with
    /// the split accounting), and finish demands this counter equal
    /// `row_count` exactly — otherwise the postings' doc ids would
    /// misaddress the stored rows.
    index_only_rows: Option<u64>,
    row_count: u64,
    /// Optional caller-known maximum for this writer's final row count. This
    /// is an internal performance hint, not a merge-size limit. It lets
    /// AUTO bloom-only fields demote as soon as their final ratio is already
    /// mathematically guaranteed, instead of retaining doomed postings until
    /// `finish`. A bound is only a performance hint; final AUTO semantics
    /// still use the actual row count.
    auto_demote_expected_max_rows: Option<u64>,
    /// Whether the expected-maximum hint has already caused a demotion. If a
    /// caller later exceeds its bound, finishing must fail rather than keep a
    /// field demoted under a ratio that was not actually satisfied.
    auto_demoted_early: bool,
    /// `_timestamp` range of the stored rows (`None` until the first row) —
    /// reported through [`VixWriterStats`], the authoritative FileMeta range.
    ts_range: Option<(i64, i64)>,
    /// Deferred construction error (`new` is infallible by contract).
    init_error: Option<String>,
    /// Reusable composite-key buffer.
    scratch: Vec<u8>,
    /// Merge mode ([`Self::merge_input_indexes`]): the pre-merged
    /// `dict`/`terms` blobs the finished file will carry instead of terms
    /// accumulated from pushes.
    merged_index: Option<PrebuiltIndex>,
    /// Merge mode: term-planned fields DEMOTED from `term` capability in the
    /// output fields table because some input carries rows with the field
    /// (key term present) without value-indexing it — its value terms are
    /// missing for those rows, so claiming the capability would make lookups
    /// silently miss them. Per-field capability INTERSECTION across inputs;
    /// conditions on demoted fields take the skip + filter-back path.
    demoted_fields: BTreeSet<String>,
    /// Test-support escape ONLY ([`Self::finish_unguarded`]): skip the
    /// degenerate-`_timestamp` finish guard so tests can fabricate the
    /// pre-guard-era files (stored rows with `_timestamp <= 0`) that the
    /// compaction-time cleansing has to digest. Never set in production.
    skip_ts_guard: bool,
}

/// How one push call splits between the term index and the docs store.
/// The build/rebuild paths couple both ([`IndexAndStore`]); the merge fast
/// path stores without indexing ([`StoreOnly`], the index is pre-merged);
/// the #51c heal passthrough indexes without storing ([`IndexOnly`] — the
/// docs rows are copied separately through the encoded-run API).
///
/// [`IndexAndStore`]: DocsPushMode::IndexAndStore
/// [`StoreOnly`]: DocsPushMode::StoreOnly
/// [`IndexOnly`]: DocsPushMode::IndexOnly
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum DocsPushMode {
    IndexAndStore,
    StoreOnly,
    IndexOnly,
}

impl DocsPushMode {
    /// Whether this push derives index terms from the rows.
    fn indexes(self) -> bool {
        matches!(self, Self::IndexAndStore | Self::IndexOnly)
    }

    /// Whether this push stages the rows into the docs store.
    fn stores(self) -> bool {
        matches!(self, Self::IndexAndStore | Self::StoreOnly)
    }
}

/// State of one open #51c encoded-chunk run (see
/// [`VixWriter::begin_docs_encoded_run`]).
struct EncodedRunState {
    /// The docs blob's vortex dtype — every pushed chunk must match EXACTLY
    /// (names, order, types, nullability); a foreign chunk would corrupt
    /// the blob for every reader.
    dtype: vortex::dtype::DType,
    /// Rows still owed by [`VixWriter::push_docs_encoded_chunk`] calls
    /// before [`VixWriter::finish_docs_encoded_run`] may close the run.
    remaining: u64,
}

/// The output of [`VixWriter::merge_input_indexes`], consumed by `finish`.
struct PrebuiltIndex {
    /// The index blob bytes; `None` when the merged inputs have no terms
    /// at all.
    blobs: Option<IndexBlobs>,
    term_count: u64,
    /// Per-file value-bloom hashes collected by the merge workers.
    bloom: crate::bloom::BloomHashAcc,
    /// Sum of the inputs' row counts — the docs pushes must cover exactly
    /// this many rows.
    expected_rows: u64,
}

impl VixWriter {
    /// Create a writer: the produced file carries the records themselves in
    /// its `docs` blob.
    ///
    /// `schema` describes the flattened record batches that will be pushed
    /// (it must contain `_timestamp` and must contain neither `_source` nor
    /// `_original` — those arrive per batch through
    /// [`Self::push_batch_with_source`]). When `store_original` is set, the
    /// `docs` blob gets a nullable `_original` column filled from the
    /// per-batch `original` argument.
    pub fn new(schema: &Schema, opts: VixWriterOptions, store_original: bool) -> Self {
        Self::new_inner(schema, opts, store_original, MAX_REAL_FIELD_ID)
    }

    /// Test-only: a writer with a synthetic real-field-id cap, so the
    /// `partial_fields` overflow path is exercisable without 65k+ columns.
    #[cfg(test)]
    pub(crate) fn new_with_field_cap(
        schema: &Schema,
        opts: VixWriterOptions,
        store_original: bool,
        max_real_field_id: u16,
    ) -> Self {
        Self::new_inner(schema, opts, store_original, max_real_field_id)
    }

    fn new_inner(
        schema: &Schema,
        opts: VixWriterOptions,
        store_original: bool,
        max_real_field_id: u16,
    ) -> Self {
        let mut term_fields: Vec<String> = if opts.index_enabled {
            schema
                .fields()
                .iter()
                .filter(|field| {
                    is_value_indexed_type(field.data_type())
                        && !NON_INDEXED_COLS.contains(&field.name().as_str())
                })
                .map(|field| field.name().clone())
                .collect()
        } else {
            // index-off (#40): empty term plan — no field is term-indexed,
            // no fts, and the field-id cap can never overflow into
            // partial_fields
            Vec::new()
        };
        term_fields.sort_unstable();
        term_fields.dedup();

        let mut partial_fields = BTreeSet::new();
        let mut init_error = None;
        // Field ids beyond the cap (0xFFFF is the reserved key marker) are
        // not an error: the overflowing fields are left out of the term
        // index and recorded in `partial_fields` (queries on them fall back
        // to scan-time filtering). Their key terms are still emitted — key
        // terms need no field id.
        let cap = usize::from(max_real_field_id) + 1;
        if term_fields.len() > cap {
            partial_fields.extend(term_fields.drain(cap..));
        }
        if schema.field_with_name(TIMESTAMP_COL_NAME).is_err() {
            init_error = Some(format!(
                "core files require a {TIMESTAMP_COL_NAME:?} column"
            ));
        }
        for reserved in [SOURCE_COL_NAME, ORIGINAL_DATA_COL_NAME] {
            if schema.field_with_name(reserved).is_ok() {
                init_error = Some(format!(
                    "the schema must not contain {reserved:?}; it is supplied through \
                     push_batch_with_source"
                ));
            }
        }
        let term_field_ids: FastHashMap<String, u16> = if init_error.is_none() {
            term_fields
                .iter()
                .enumerate()
                .map(|(id, name)| (name.clone(), id as u16))
                .collect()
        } else {
            FastHashMap::default()
        };

        // fts marking applies to string-family fields only: tokenization is
        // a text concept. A numeric/bool field named in `fts_field_names`
        // stays a plain term field (canonical value terms), matching the
        // source-driven path where non-string values never tokenize.
        let fts_fields: FastHashSet<String> = opts
            .fts_field_names
            .iter()
            .filter(|name| {
                term_field_ids.contains_key(*name)
                    && schema
                        .field_with_name(name)
                        .is_ok_and(|field| is_string_family(field.data_type()))
            })
            .cloned()
            .collect();

        // Term fields whose value terms are NOT raw string bytes (numeric/
        // bool: tagged canonical forms). They must stay OUT of the composite
        // bloom's coverage set: the pruner probes with the query literal's
        // raw bytes, so a covered-but-canonical field would read every miss
        // as "definitely not" and wrongly drop files (`status = 200`).
        // Uncovered ⇒ guards miss ⇒ "no info" ⇒ keep + scan, the safe side.
        let non_string_term_fields: FastHashSet<String> = term_field_ids
            .keys()
            .filter(|name| {
                schema
                    .field_with_name(name)
                    .is_ok_and(|field| !is_string_family(field.data_type()))
            })
            .cloned()
            .collect();

        // #52 bloom-only resolution: explicit list ∩ term plan ∩ string
        // family − never-list − fts. Kept in the fields table (they hold
        // their field-id slot and emit KEY terms) but typed "bloom", and
        // their raw values bypass the dictionary entirely.
        let bloom_only: FastHashMap<u16, (String, FastHashSet<u64>)> = opts
            .bloom_only_field_names
            .iter()
            .filter(|name| {
                term_field_ids.contains_key(*name)
                    && !opts.bloom_only_never.iter().any(|n| n == *name)
                    && !opts.fts_field_names.iter().any(|n| n == *name)
                    && schema
                        .field_with_name(name)
                        .is_ok_and(|field| is_string_family(field.data_type()))
            })
            .map(|name| (term_field_ids[name], (name.clone(), FastHashSet::default())))
            .collect();

        // The `docs` blob schema (v2, DESIGN §2 — ALL present fields as
        // columns): `_timestamp` first (always as i64), then EVERY other
        // schema field sorted by name with its original arrow type, then
        // `_source` and optionally `_original`. There is no column-store
        // curation: every field the batches carry is a native column (the
        // #52 bloom-only demotion stays an index-side concept — its fields
        // are columns like everything else).
        let cs_fields: BTreeSet<String> = schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .filter(|name| name.as_str() != TIMESTAMP_COL_NAME)
            .collect();
        let mut docs_fields: Vec<Field> = Vec::with_capacity(cs_fields.len() + 3);
        docs_fields.push(Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false));
        for name in &cs_fields {
            if let Ok(field) = schema.field_with_name(name) {
                // Docs columns are stored NULLABLE regardless of the batch
                // schema: a merged file legitimately holds null runs for
                // rows whose input lacked the column, and uniform
                // nullability keeps the stored dtype identical across files
                // (the passthrough identity requirement).
                docs_fields.push(field.clone().with_nullable(true));
            }
        }
        docs_fields.push(Field::new(SOURCE_COL_NAME, DataType::Utf8, false));
        if store_original {
            docs_fields.push(Field::new(ORIGINAL_DATA_COL_NAME, DataType::Utf8, true));
        }
        let docs_schema = Arc::new(Schema::new(docs_fields));
        let term_field_count = term_fields.len();

        Self {
            opts,
            store_original,
            term_fields,
            term_field_ids,
            fts_fields,
            non_string_term_fields,
            bloom_only,
            cs_fields,
            docs_schema,
            terms: TermAccumulator::new(term_field_count),
            tag_scratch: Vec::new(),
            term_spill: None,
            partial_fields,
            oversize_skips: BTreeMap::new(),
            sample_batches: Vec::new(),
            sample_bytes: 0,
            docs_encoder: None,
            docs_failopen_chunks: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            zone_folder: None,
            encoded_run: None,
            index_only_rows: None,
            row_count: 0,
            auto_demote_expected_max_rows: None,
            auto_demoted_early: false,
            ts_range: None,
            init_error,
            scratch: Vec::new(),
            merged_index: None,
            demoted_fields: BTreeSet::new(),
            skip_ts_guard: false,
        }
    }

    /// Supply the maximum rows this writer is expected to finish with.
    ///
    /// When AUTO bloom-only is enabled, a string field can be demoted early
    /// once its current distinct count already satisfies the configured
    /// threshold against this (possibly larger) final denominator. This is
    /// exact: distinct counts can only increase and the actual row count is
    /// required not to exceed the bound after an early demotion.
    pub fn set_expected_max_rows_for_auto_demotion(&mut self, rows: u64) -> anyhow::Result<()> {
        if self.row_count != 0
            || self.index_only_rows.is_some()
            || !self.terms.is_empty()
            || self.merged_index.is_some()
        {
            anyhow::bail!(
                "expected maximum rows for AUTO bloom-only demotion must be supplied before the \
                 first writer operation"
            );
        }
        self.auto_demote_expected_max_rows = Some(rows);
        Ok(())
    }

    /// Index one record batch of a core file, together with the
    /// per-row `_source` strings (required, non-null, one per row) and the
    /// optional per-row `_original` strings.
    ///
    /// `batch` holds the flattened fields (including `_timestamp`) and must
    /// not contain `_source`/`_original` columns — those come only through
    /// the dedicated arguments; the crate never serializes records itself.
    /// `original` may only be passed when the writer was built with
    /// `store_original = true`; batches pushed without it store nulls.
    /// Batches must arrive in document order.
    pub fn push_batch_with_source(
        &mut self,
        batch: &RecordBatch,
        source: &StringArray,
        original: Option<&StringArray>,
    ) -> anyhow::Result<()> {
        self.push_batch_inner(batch, source, original, DocsPushMode::IndexAndStore)?;
        Ok(())
    }

    /// #51c heal passthrough: [`Self::push_batch_with_source`] with the docs
    /// STORE side detached — the batch feeds ONLY the term index (value
    /// terms, key terms, #52 bloom-only hashing, oversize/partial
    /// accounting, exactly like the coupled push), while the rows' stored
    /// form arrives separately through the encoded-run API in the SAME
    /// order (`row_count`, the `_timestamp` range and the zone table
    /// advance there). Doc ids are assigned from the index-only cursor;
    /// finish verifies it equals the stored row count. Requires
    /// [`VixWriterOptions::docs_passthrough`] and an indexed, non-merge-mode
    /// writer.
    pub fn push_batch_with_source_index_only(
        &mut self,
        batch: &RecordBatch,
        source: &StringArray,
        original: Option<&StringArray>,
    ) -> anyhow::Result<()> {
        self.push_batch_inner(batch, source, original, DocsPushMode::IndexOnly)?;
        Ok(())
    }

    /// Index one chunk of rows whose terms are derived from the
    /// `_source` JSON itself instead of from flattened columns — the
    /// compaction push path, where inputs are core files that carry no
    /// column form of most fields.
    ///
    /// Each `_source` string must be a single-level JSON object with dotted
    /// keys (exactly what [`Self::push_batch_with_source`]-built files
    /// store). Per `(key, value)` entry:
    /// - `null` values are treated as absent (no terms; `_source` synthesis omits nulls, so this is
    ///   defensive),
    /// - every non-null value emits the key term `{key}\x00\xFF\xFF` (internal keys —
    ///   `_timestamp`/`_o2_id`/`_original`/`_source` — are skipped, mirroring the column-driven
    ///   path),
    /// - a JSON **string** value additionally emits its value terms — the full-text tokens for
    ///   fields in [`VixWriterOptions::fts_field_names`] (regardless of value length), the raw
    ///   whole-value term (empty strings included) for every other field — with the same
    ///   raw-oversize/partial rules as the column-driven path. A string value whose key is not a
    ///   value-indexed field of the writer's schema cannot be indexed and marks the field `partial`
    ///   (scan fallback),
    /// - numbers/bools emit the key term only (numeric columns are never term-indexed).
    ///
    /// The stored `docs` row is assembled from the passed arrays:
    /// `timestamps` (non-null, one per row), the docs columns looked up by
    /// name in `cs_columns` (cast to the schema type; a docs column the
    /// caller supplies no data for stores null — v2 all-columns semantics,
    /// where a merge input lacking a column contributes nulls — while a
    /// supplied name that is NOT a docs column errors loudly), `source` and
    /// `original` (same rules as [`Self::push_batch_with_source`]). Rows
    /// must arrive in document order.
    pub fn push_docs_rows(
        &mut self,
        timestamps: &Int64Array,
        cs_columns: &[(String, ArrowArrayRef)],
        source: &StringArray,
        original: Option<&StringArray>,
    ) -> anyhow::Result<()> {
        self.push_docs_rows_inner(
            timestamps,
            cs_columns,
            source,
            original,
            DocsPushMode::IndexAndStore,
        )?;
        Ok(())
    }

    /// #51c heal passthrough: [`Self::push_docs_rows`] with the docs STORE
    /// side detached — the rows feed ONLY the term index (source-driven
    /// term derivation, #52 bloom-only hashing from both the cs columns and
    /// `_source`, oversize/partial accounting, exactly like the coupled
    /// push), while their stored form arrives separately through the
    /// encoded-run API in the SAME order (`row_count`, the `_timestamp`
    /// range and the zone table advance there). Doc ids are assigned from
    /// the index-only cursor; finish verifies it equals the stored row
    /// count. Requires [`VixWriterOptions::docs_passthrough`] and an
    /// indexed, non-merge-mode writer.
    pub fn push_docs_rows_index_only(
        &mut self,
        timestamps: &Int64Array,
        cs_columns: &[(String, ArrowArrayRef)],
        source: &StringArray,
        original: Option<&StringArray>,
    ) -> anyhow::Result<()> {
        self.push_docs_rows_inner(
            timestamps,
            cs_columns,
            source,
            original,
            DocsPushMode::IndexOnly,
        )?;
        Ok(())
    }

    /// Merge-compatibility pre-flight for [`Self::merge_input_indexes`]:
    /// `Err(reason)` means the inputs' dictionaries cannot be merged into
    /// this writer's field/token plan and the caller must fall back to a
    /// full rebuild (re-deriving terms from `_source`). Rejected inputs:
    ///
    /// - a `tokenizer` property other than this writer's (tokens may differ from what a rebuild
    ///   would emit),
    /// - a field marked `fts` in an input but planned as `term` here (its dictionary holds tokens,
    ///   not the raw values a rebuild would index) — or the reverse,
    /// - a field the plan marks `fts` that is `partial` in an input: an fts field never
    ///   legitimately goes partial (tokens are length-bounded, so no value is ever skipped) — the
    ///   marking means the input was written before fts values tokenized unconditionally, its
    ///   dictionary is missing the skipped oversize values' tokens, and only a rebuild from
    ///   `_source` re-derives them (the rebuilt output drops the marking, un-tainting match_all for
    ///   the file),
    /// - a field that is `partial` in an input **without** being value-indexed there while the
    ///   merge plan value-indexes it (the input's dictionary is missing values that only a rebuild
    ///   from `_source` can recover).
    ///
    /// Fields dropped by the plan (no output field id — e.g. stored under a
    /// non-string type here) need no check: their input terms are discarded
    /// and the field is marked `partial`, exactly like a rebuild.
    pub fn check_merge_inputs(&self, inputs: &[&VixReader]) -> std::result::Result<(), String> {
        if let Some(error) = &self.init_error {
            return Err(error.clone());
        }
        for (position, reader) in inputs.iter().enumerate() {
            // #40: an index-off input carries rows the dictionary merge
            // would silently miss (it has no term/fts entries to demote
            // on) — an INDEXED merge plan must rebuild from `_source`
            // instead of fast-pathing over it.
            if !reader.has_index() {
                return Err(format!(
                    "input {position}: column-store-only file (no index sidecar) cannot join a \
                     dictionary merge"
                ));
            }
            // The writer emits the canonical [`TOKENIZER_ID`] tokens: an
            // input stamped with any other tokenizer id cannot be
            // dictionary-merged — the caller rebuilds from `_source`, which
            // re-tokenizes everything with the current tokenizer.
            if reader.tokenizer_prop() != Some(TOKENIZER_ID) {
                return Err(format!(
                    "input {position}: tokenizer {:?} does not match {TOKENIZER_ID:?}",
                    reader.tokenizer_prop()
                ));
            }
            for entry in reader.field_entries() {
                let input_term = entry.has_type(FIELD_TYPE_TERM);
                let input_fts = entry.has_type(FIELD_TYPE_FTS);
                if !input_term && !input_fts {
                    continue;
                }
                if !self.term_field_ids.contains_key(&entry.name) {
                    continue; // dropped by the plan: terms discarded + partial
                }
                let output_fts = self.fts_fields.contains(&entry.name);
                if output_fts != input_fts {
                    return Err(format!(
                        "field {:?} is {} in input {position} but {} in the merge plan",
                        entry.name,
                        if input_fts { "fts" } else { "term" },
                        if output_fts { "fts" } else { "term" },
                    ));
                }
            }
            for name in reader.partial_fields() {
                if self.fts_fields.contains(name) {
                    return Err(format!(
                        "field {name:?} is partial in input {position} but fts in the merge \
                         plan — its dictionary is missing the skipped values' tokens, which \
                         only a rebuild from _source can re-derive"
                    ));
                }
                let value_indexed = reader.field_entries().iter().any(|entry| {
                    entry.name == *name
                        && (entry.has_type(FIELD_TYPE_TERM) || entry.has_type(FIELD_TYPE_FTS))
                });
                if !value_indexed && self.term_field_ids.contains_key(name) {
                    return Err(format!(
                        "field {name:?} is partial and not value-indexed in input {position}, \
                         but the merge plan value-indexes it — its values are only recoverable \
                         by a rebuild"
                    ));
                }
            }
        }
        Ok(())
    }

    /// Term-planned raw-value fields (non-fts) that some input CARRIES
    /// (key term present ⇒ documents hold values) **without** `term`
    /// capability there — e.g. a numeric field in a file written before
    /// numeric value terms existed, or a field a previous fast-path merge
    /// demoted. [`Self::merge_input_indexes`] DEMOTES exactly these fields
    /// in the merged fields table (per-field capability intersection), so a
    /// fast-path merge output still lacks their value terms; only a rebuild
    /// from `_source` re-derives them. Compaction's single-file healing
    /// probe uses this to detect "missing value terms the current plan
    /// carries" cheaply: fields-table reads plus at most one key-term
    /// dictionary probe per candidate field — never postings or docs data.
    pub fn merge_inputs_lacking_term_capability(
        &self,
        inputs: &[&VixReader],
    ) -> std::result::Result<Vec<String>, String> {
        if let Some(error) = &self.init_error {
            return Err(error.clone());
        }
        let mut lacking = Vec::new();
        for name in &self.term_fields {
            if self.fts_fields.contains(name) {
                continue; // fts entries never claim raw-value capability
            }
            // #52/M7: a plan-BLOOM-ONLY field claims no term capability in
            // the output, so inputs owe it none — its bloom coverage
            // re-derives completely from the docs columns (and legacy term
            // inputs additionally converge through the dictionary path).
            // Without this skip, every merge or classify of an
            // already-demoted file would flag the field lacking: merges
            // would degrade it to capability-less (coverage lost) and the
            // single-file sweep would rebuild → re-demote → rebuild forever.
            if self
                .term_field_ids
                .get(name)
                .is_some_and(|id| self.bloom_only.contains_key(id))
            {
                continue;
            }
            for reader in inputs {
                if reader.has_term_capability(name) {
                    continue;
                }
                let carried = reader
                    .key_term_exists(name)
                    .map_err(|e| format!("key-term probe of field {name:?} failed: {e}"))?;
                if carried {
                    lacking.push(name.clone());
                    break;
                }
            }
        }
        Ok(lacking)
    }

    /// Switch the writer into **merge mode**: build the merged `dict`/`terms`
    /// blobs directly from the inputs' term dictionaries (k-way key merge +
    /// postings remap through `doc_maps`, doc counts summed, dense elision
    /// re-checked against the merged row count) instead of re-deriving terms
    /// from `_source`. See [`crate::merge`] for the mechanics.
    ///
    /// Contract:
    /// - must be the writer's **first** operation (before any push); afterwards rows are stored
    ///   with [`Self::push_docs_rows_unindexed`] — the indexed push paths are rejected,
    /// - `doc_maps[i]` maps input `i`'s doc ids into the merged doc-id space `0..Σ row_count`
    ///   (injectively across all inputs); the docs pushes must then supply exactly the merged rows
    ///   in that order,
    /// - callers are expected to have run [`Self::check_merge_inputs`] first; any error here (or
    ///   there) leaves the inputs untouched, so falling back to a rebuild is always possible.
    ///
    /// `threads` bounds the parallelism of the key-range-partitioned merge
    /// (`0` = the machine's available parallelism, `1` = sequential).
    ///
    /// The inputs' `partial_fields` are unioned into the writer's, plus any
    /// field whose value terms were dropped for lack of an output field id.
    pub fn merge_input_indexes(
        &mut self,
        inputs: &[&VixReader],
        doc_maps: &[DocIdMap],
        threads: usize,
    ) -> anyhow::Result<()> {
        self.merge_input_indexes_inner(inputs, doc_maps, threads)?;
        Ok(())
    }

    fn merge_input_indexes_inner(
        &mut self,
        inputs: &[&VixReader],
        doc_maps: &[DocIdMap],
        threads: usize,
    ) -> Result<()> {
        if let Some(error) = &self.init_error {
            return Err(VixError::Writer(error.clone()));
        }
        if !self.opts.index_enabled {
            return Err(VixError::Writer(
                "merge_input_indexes on an index-off writer (#40): the plan must route this merge \
                 through the docs-only path"
                    .to_string(),
            ));
        }
        if self.merged_index.is_some() || self.row_count > 0 || !self.terms.is_empty() {
            return Err(VixError::Writer(
                "merge_input_indexes must be the writer's first operation".to_string(),
            ));
        }
        if inputs.len() != doc_maps.len() {
            return Err(VixError::Writer(format!(
                "{} inputs but {} doc-id maps",
                inputs.len(),
                doc_maps.len()
            )));
        }
        let total_rows: u64 = inputs.iter().map(|reader| reader.row_count()).sum();
        if total_rows > u64::from(u32::MAX) {
            return Err(VixError::Writer(format!(
                "doc id overflow: {total_rows} total rows exceed the u32 doc-id space"
            )));
        }
        // Validate the maps: offset runs in bounds and disjoint, tables
        // sized to their input and in bounds. Cross-input injectivity of
        // table maps is proven lazily — a collision surfaces as a duplicate
        // doc id when the affected postings merge.
        let mut spans: Vec<(u64, u64)> = Vec::new();
        for (reader, map) in inputs.iter().zip(doc_maps) {
            let rows = reader.row_count();
            match map {
                DocIdMap::Offset(offset) => {
                    let end = u64::from(*offset) + rows;
                    if end > total_rows {
                        return Err(VixError::Writer(format!(
                            "doc-id offset {offset} + {rows} rows exceeds the merged row count \
                             {total_rows}"
                        )));
                    }
                    if rows > 0 {
                        spans.push((u64::from(*offset), end));
                    }
                }
                DocIdMap::Table(table) => {
                    if table.len() as u64 != rows {
                        return Err(VixError::Writer(format!(
                            "doc-id table has {} entries for a {rows}-row input",
                            table.len()
                        )));
                    }
                    if table.iter().any(|&id| u64::from(id) >= total_rows) {
                        return Err(VixError::Writer(format!(
                            "doc-id table maps beyond the merged row count {total_rows}"
                        )));
                    }
                }
            }
        }
        spans.sort_unstable();
        if spans.windows(2).any(|pair| pair[0].1 > pair[1].0) {
            return Err(VixError::Writer("doc-id offset runs overlap".to_string()));
        }

        // Per-field term-capability INTERSECTION across the inputs: a
        // term-planned field that some input CARRIES (key term ⇒ rows with
        // values) without term capability there (e.g. a numeric field in a
        // file written before numeric value terms existed) contributed no
        // value terms for those rows — an output entry claiming `term`
        // would make lookups silently miss them. Demote such fields: the
        // entry keeps its field-id slot but drops the `term` type, so
        // queries take the skip + filter-back path. Value terms the capable
        // inputs contributed stay in the dictionary under the field's id —
        // orphaned but harmless, since capability gates lookups (and a later
        // REBUILD re-derives everything from `_source`, restoring full
        // capability). The detection is shared with compaction's
        // single-file healing probe
        // ([`Self::merge_inputs_lacking_term_capability`]).
        for name in self
            .merge_inputs_lacking_term_capability(inputs)
            .map_err(VixError::Writer)?
        {
            log::info!(
                "vix merge: field {name:?} carried without term capability in an input; \
                 demoting it in the merged fields table (filter-back until a rebuild)"
            );
            self.demoted_fields.insert(name);
        }

        // #52: per-field bloom sections track only NON-demoted bloom fields
        // — a demoted field's per-field acc observes NOTHING when the
        // inputs are already demoted (no dictionary keys carry its values)
        // and would publish an EMPTY reject-all filter: every equality
        // probe against it reads "definitely not" and wrongly drops the
        // file (caught by the M7 measurement's bloom accounting: a 32-byte
        // n_items=0 trace_id section on a demoted-inputs merge). The build
        // path has filtered exactly this since #52 (assemble_index_blobs);
        // the composite section carries demoted values on both paths, and
        // the pruner treats the ABSENT per-field section as "no info".
        let bloom_field_names: Vec<String> = self
            .opts
            .bloom_field_names
            .iter()
            .filter(|name| {
                self.term_field_ids
                    .get(*name)
                    .is_none_or(|id| !self.bloom_only.contains_key(id))
            })
            .cloned()
            .collect();
        let bloom_only_fids: FastHashSet<u16> = self.bloom_only.keys().copied().collect();
        let merged = merge::merge_indexes(
            inputs,
            doc_maps,
            &self.term_field_ids,
            &bloom_field_names,
            &self.composite_pairs(),
            &bloom_only_fids,
            total_rows,
            self.opts.postings_chunk_bytes,
            self.opts.postings_plist_min_docs,
            threads,
            self.opts.merge_kway_threads,
        )?;
        for reader in inputs {
            self.partial_fields
                .extend(reader.partial_fields().iter().cloned());
            // Sum the inputs' oversize-skip allowances: the merged
            // dictionary carries exactly the inputs' terms, so the merged
            // file's serve reconciliation needs the combined allowance to
            // keep serving (a lost allowance would demote every merged
            // file back to the scan fallback).
            for (field, count) in reader.oversize_skips() {
                *self.oversize_skips.entry(field.clone()).or_default() += count;
            }
        }
        self.partial_fields.extend(merged.dropped);
        self.merged_index = Some(PrebuiltIndex {
            blobs: merged.blobs,
            term_count: merged.term_count,
            bloom: merged.bloom,
            expected_rows: total_rows,
        });
        Ok(())
    }

    /// Store one chunk of docs rows **without any term extraction** — the
    /// merge-mode storage path (the index came from
    /// [`Self::merge_input_indexes`]). Same array contracts as
    /// [`Self::push_docs_rows`]; rows must arrive in merged doc-id order.
    pub fn push_docs_rows_unindexed(
        &mut self,
        timestamps: &Int64Array,
        cs_columns: &[(String, ArrowArrayRef)],
        source: &StringArray,
        original: Option<&StringArray>,
    ) -> anyhow::Result<()> {
        self.push_docs_rows_inner(
            timestamps,
            cs_columns,
            source,
            original,
            DocsPushMode::StoreOnly,
        )?;
        Ok(())
    }

    /// #51c: open one encoded-chunk run — `expected_rows` docs rows about to
    /// arrive as already-encoded chunks ([`Self::push_docs_encoded_chunk`]),
    /// copied from ONE disjoint merge input without decode/re-encode. The
    /// run's `_timestamp` bounds come from the caller's own read of the
    /// input's timestamp column (never its footer), `zone_entries` are the
    /// input's zone table spliced VERBATIM (the writer's own zone folding
    /// flushes its open window first, so the spliced entries keep the
    /// table's row-order invariant: each entry bounds its contiguous row
    /// range; entries cover every row exactly once), and `spliced_stats`
    /// carries the input's per-column chunk stats + file presence counts,
    /// spliced alongside — a passthrough output ALWAYS carries full stats
    /// (H2/§4: the v1 stats-loss regression is structurally impossible).
    ///
    /// Errors (before any chunk is accepted, so a caller can still fall
    /// back to the decode path): the writer was not built with
    /// [`VixWriterOptions::docs_passthrough`]; a run is already open; the
    /// writer is not in a mode that accepts unindexed docs pushes; the run
    /// is empty; the `_timestamp` bounds are degenerate (`<= 0`) or
    /// inverted; the zone entries do not sum to `expected_rows`, carry an
    /// empty/inverted window, or exceed the run's bounds.
    /// `run_regions` (§4 region table): the run's PROVEN internally-DESC
    /// decomposition as row counts summing to `expected_rows` — a ts_desc
    /// input is `Some(&[expected_rows])`, a concat input passes its own
    /// stamped region table. `None` = no proven decomposition (a concat
    /// input without a region table): the OUTPUT's region table is poisoned
    /// (property omitted, readers fail open to the full sort) while the
    /// copy itself proceeds unchanged.
    pub fn begin_docs_encoded_run(
        &mut self,
        expected_rows: u64,
        ts_min: i64,
        ts_max: i64,
        zone_entries: &[ZoneEntry],
        spliced_stats: &SpliceableStats,
        run_regions: Option<&[u64]>,
    ) -> anyhow::Result<()> {
        if !self.opts.docs_passthrough {
            return Err(VixError::Writer(
                "begin_docs_encoded_run requires VixWriterOptions::docs_passthrough — this \
                 writer's docs encoder compresses through the standard pipeline and would \
                 canonicalize (or corrupt the accounting of) pre-encoded chunks"
                    .to_string(),
            )
            .into());
        }
        if self.encoded_run.is_some() {
            return Err(VixError::Writer(
                "begin_docs_encoded_run: a previous encoded run is still open (missing \
                 finish_docs_encoded_run)"
                    .to_string(),
            )
            .into());
        }
        // Mode: the encoded run is a docs-STORE path. In the heal's
        // index-only build mode (#51c heal passthrough) the run is exactly
        // where the scanned-and-indexed rows get stored, so it is accepted
        // as-is; otherwise the same mode rules as push_docs_rows_unindexed
        // apply: merge mode (or index-off, where both push variants store
        // identically).
        if self.index_only_rows.is_none() {
            self.check_push_mode(DocsPushMode::StoreOnly)?;
        }
        if expected_rows == 0 {
            return Err(VixError::Writer(
                "begin_docs_encoded_run: an encoded run must cover at least one row".to_string(),
            )
            .into());
        }
        if ts_min <= 0 || ts_max <= 0 || ts_min > ts_max {
            return Err(VixError::Writer(format!(
                "begin_docs_encoded_run: degenerate _timestamp bounds [{ts_min}, {ts_max}] — \
                 passthrough inputs must be cleansed (rows with _timestamp <= 0 take the \
                 rebuild path)"
            ))
            .into());
        }
        let mut covered = 0u64;
        for &(rows, min, max) in zone_entries {
            if rows == 0 || min > max || min < ts_min || max > ts_max {
                return Err(VixError::Writer(format!(
                    "begin_docs_encoded_run: zone entry ({rows}, {min}, {max}) is inconsistent \
                     with the run ({expected_rows} rows, _timestamp [{ts_min}, {ts_max}]) — \
                     refusing to splice a zone table that misbounds its rows"
                ))
                .into());
            }
            covered = covered.checked_add(rows).ok_or_else(|| {
                VixError::Writer("begin_docs_encoded_run: zone row counts overflow u64".to_string())
            })?;
        }
        if covered != expected_rows {
            return Err(VixError::Writer(format!(
                "begin_docs_encoded_run: zone entries cover {covered} rows but the run brings \
                 {expected_rows} — a spliced zone table must cover the run exactly"
            ))
            .into());
        }
        if let Some(regions) = run_regions {
            let mut region_rows = 0u64;
            for &rows in regions {
                if rows == 0 {
                    return Err(VixError::Writer(
                        "begin_docs_encoded_run: run_regions carries a zero-row region".to_string(),
                    )
                    .into());
                }
                region_rows = region_rows.checked_add(rows).ok_or_else(|| {
                    VixError::Writer(
                        "begin_docs_encoded_run: run_regions row counts overflow u64".to_string(),
                    )
                })?;
            }
            if region_rows != expected_rows {
                return Err(VixError::Writer(format!(
                    "begin_docs_encoded_run: run_regions cover {region_rows} rows but the run \
                     brings {expected_rows} — a region decomposition must cover the run exactly"
                ))
                .into());
            }
        }
        self.check_doc_capacity(expected_rows as usize)?;

        // rows-per-chunk locking still happens through the standard path
        // (from the sample when one exists, the passthrough fallback window
        // otherwise) before the first encoded push
        if self.docs_encoder.is_none() {
            self.start_docs_encoder()?;
        }
        let folder = self
            .zone_folder
            .as_mut()
            .expect("zone folder exists whenever the encoder does");
        folder.flush_open_window();
        folder.append_spliced(zone_entries, spliced_stats, run_regions);
        self.track_ts_bounds(ts_min, ts_max);
        let dtype = {
            use vortex::arrow::FromArrowType;
            vortex::dtype::DType::from_arrow(self.docs_schema.as_ref())
        };
        self.encoded_run = Some(EncodedRunState {
            dtype,
            remaining: expected_rows,
        });
        Ok(())
    }

    /// #51c: store one already-encoded docs chunk of the open run (see
    /// [`Self::begin_docs_encoded_run`]). The chunk's dtype must equal the
    /// writer's docs dtype exactly, and the run's chunks must sum to the
    /// declared row count by [`Self::finish_docs_encoded_run`].
    pub fn push_docs_encoded_chunk(
        &mut self,
        chunk: crate::docs::EncodedDocsChunk,
    ) -> anyhow::Result<()> {
        let run = self.encoded_run.as_mut().ok_or_else(|| {
            VixError::Writer(
                "push_docs_encoded_chunk without an open run (begin_docs_encoded_run first)"
                    .to_string(),
            )
        })?;
        let rows = chunk.rows() as u64;
        if rows == 0 {
            return Ok(());
        }
        if chunk.array.len() as u64 != rows {
            return Err(VixError::Writer(format!(
                "push_docs_encoded_chunk: chunk claims {rows} rows but the array holds {}",
                chunk.array.len()
            ))
            .into());
        }
        if chunk.array.dtype() != &run.dtype {
            return Err(VixError::Writer(format!(
                "push_docs_encoded_chunk: chunk dtype {} does not equal the writer docs dtype \
                 {} — passthrough requires exact schema identity (names, order, types, \
                 nullability); the input must take the decode path",
                chunk.array.dtype(),
                run.dtype
            ))
            .into());
        }
        if rows > run.remaining {
            return Err(VixError::Writer(format!(
                "push_docs_encoded_chunk: chunk brings {rows} rows but the run has only {} \
                 left of its declared count",
                run.remaining
            ))
            .into());
        }
        run.remaining -= rows;
        self.docs_encoder
            .as_mut()
            .expect("encoder started at begin_docs_encoded_run")
            .push_encoded(chunk.array)?;
        self.row_count += rows;
        Ok(())
    }

    /// #51c: close the open encoded run, verifying every declared row
    /// arrived.
    pub fn finish_docs_encoded_run(&mut self) -> anyhow::Result<()> {
        let run = self.encoded_run.take().ok_or_else(|| {
            VixError::Writer("finish_docs_encoded_run without an open run".to_string())
        })?;
        if run.remaining != 0 {
            return Err(VixError::Writer(format!(
                "finish_docs_encoded_run: the run is short {} rows of its declared count — \
                 the spliced zone table would misdescribe the file",
                run.remaining
            ))
            .into());
        }
        Ok(())
    }

    /// The arrow schema of the `docs` blob this writer stores — the schema
    /// a #51c passthrough input must match (compare with
    /// [`docs_schema_mismatch_reason`], which checks identity at the STORED
    /// dtype level).
    pub fn docs_schema(&self) -> &SchemaRef {
        &self.docs_schema
    }

    /// M18: handle onto the passthrough encoder's per-chunk fail-open
    /// counter (encoded column chunks canonicalized + re-encoded because
    /// their tree carried a non-writable encoding). The count is final only
    /// after `finish`/`finish_output` joins the encoder worker — callers
    /// clone the handle before finishing and read it after, for the merge
    /// summary.
    pub fn docs_failopen_counter(&self) -> Arc<std::sync::atomic::AtomicU64> {
        Arc::clone(&self.docs_failopen_chunks)
    }

    /// #52: the resolved bloom-only field names (string-family term-plan
    /// fields minus never/fts). The #51c passthrough qualifier projects
    /// exactly these columns for its bloom-coverage scan.
    pub fn bloom_only_fields(&self) -> Vec<String> {
        self.bloom_only
            .values()
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// #52 + #51c: hash bloom-only field values from already-materialized
    /// docs columns into the composite-bloom accumulation — the SAME
    /// derivation [`Self::push_docs_rows_unindexed`] applies to streamed
    /// columns, exposed for the passthrough path (whose docs rows never
    /// decode; the caller scans ONLY the bloom-only columns). Row counts
    /// are per call; the hash set dedupes across calls.
    pub fn absorb_bloom_only_columns(
        &mut self,
        cs_columns: &[(String, ArrowArrayRef)],
    ) -> anyhow::Result<()> {
        if let Some((first_name, first)) = cs_columns.first() {
            for (name, column) in cs_columns {
                if column.len() != first.len() {
                    return Err(VixError::Writer(format!(
                        "absorb_bloom_only_columns: column {name:?} has {} rows but \
                         {first_name:?} has {}",
                        column.len(),
                        first.len()
                    ))
                    .into());
                }
            }
        }
        self.hash_bloom_only_columns(cs_columns);
        Ok(())
    }

    /// M12: a DETACHED bloom-only hasher over `fields` (∩ this writer's
    /// resolved bloom-only set) — the parallel coverage scan's per-worker
    /// state. Workers hash into their own sets off the writer; the writer
    /// folds them back with [`Self::absorb_bloom_only_hashes`]. Hash
    /// absorption is order-independent and set-union dedupes exactly like
    /// the writer's own sets, so any worker partition produces the
    /// identical final set.
    pub fn bloom_only_hasher(&self, fields: &[String]) -> BloomOnlyHasher {
        let wanted: Vec<(String, u16)> = fields
            .iter()
            .filter_map(|name| {
                let fid = *self.term_field_ids.get(name)?;
                self.bloom_only
                    .contains_key(&fid)
                    .then(|| (name.clone(), fid))
            })
            .collect();
        BloomOnlyHasher {
            sets: wanted
                .iter()
                .map(|(name, fid)| (*fid, (name.clone(), FastHashSet::default())))
                .collect(),
            max_raw_term_len: self.opts.max_raw_term_len,
            scratch: Vec::new(),
        }
    }

    /// M12: fold a detached hasher's sets back into the writer (set union —
    /// the same dedupe the writer's own absorption applies).
    pub fn absorb_bloom_only_hashes(&mut self, hasher: BloomOnlyHasher) {
        for (fid, (_, hashes)) in hasher.sets {
            if let Some((_, sink)) = self.bloom_only.get_mut(&fid) {
                sink.extend(hashes);
            }
        }
    }

    /// #52 + #51c-d: hash bloom-only field values out of `_source` JSON into
    /// the composite-bloom accumulation — the coverage source for a
    /// schema-PINNED passthrough merge whose input stores NO docs column for
    /// the field (nothing to project; the values live only in `_source`).
    /// `fields` restricts hashing to the named bloom-only fields (the caller
    /// passes exactly the ones missing from the input's docs schema; fields
    /// present as columns keep the cheaper projected-column hashing).
    ///
    /// Value policy matches [`Self::absorb_bloom_only_columns`] exactly:
    /// string values only, oversize values silently skipped WITHOUT touching
    /// the `oversize_skips` allowance (merge mode carries the inputs'
    /// stamped allowances; counting here would double them), nulls/absent
    /// keys hash nothing, and the per-field hash sets dedupe across calls.
    /// `source` accepts any arrow string flavor (a scanned `_source` column
    /// reads back as the VIEW form). Errors name the offending row; the
    /// caller treats any error as a pre-push qualification failure.
    pub fn absorb_bloom_only_source(
        &mut self,
        source: &ArrowArrayRef,
        fields: &[String],
    ) -> anyhow::Result<()> {
        // fid lookup for exactly the requested-AND-resolved bloom-only names
        let wanted: Vec<(String, u16)> = fields
            .iter()
            .filter_map(|name| {
                let fid = *self.term_field_ids.get(name)?;
                self.bloom_only
                    .contains_key(&fid)
                    .then(|| (name.clone(), fid))
            })
            .collect();
        if wanted.is_empty() {
            return Ok(());
        }
        let mut scratch = std::mem::take(&mut self.scratch);
        let max_raw_term_len = self.opts.max_raw_term_len;
        let bloom_only = &mut self.bloom_only;
        let result = hash_bloom_only_source_values(
            source,
            &wanted,
            max_raw_term_len,
            &mut scratch,
            |fid, hash| {
                if let Some((_, hashes)) = bloom_only.get_mut(&fid) {
                    hashes.insert(hash);
                }
            },
        );
        self.scratch = scratch;
        result
    }

    /// Build the file's TWO byte streams: the `.vix` DATA object (puffin
    /// with the `docs` blob + data-descriptive properties) and the `.vxi`
    /// INDEX sidecar (`None` for index-off builds — #40/#42 files have no
    /// sidecar at all). With [`VixWriterOptions::output_spool_dir`] set this
    /// reads the data spool back into memory — callers that spool should use
    /// [`Self::finish_output`].
    pub fn finish(self) -> anyhow::Result<(Vec<u8>, Option<Vec<u8>>)> {
        let (output, index, _) = self.finish_inner()?;
        Ok((output.into_bytes()?, index))
    }

    /// Like [`Self::finish`], additionally returning size/count stats of the
    /// produced file (`index_size` = the sidecar's byte size — the
    /// `FileMeta::index_size` value; `0` ⟺ no sidecar).
    pub fn finish_with_stats(self) -> anyhow::Result<(Vec<u8>, Option<Vec<u8>>, VixWriterStats)> {
        let (output, index, stats) = self.finish_inner()?;
        Ok((output.into_bytes()?, index, stats))
    }

    /// Finish into a [`VixOutput`] for the DATA object — in-memory bytes,
    /// or, with [`VixWriterOptions::output_spool_dir`] set, a temp-file
    /// spool the container streamed into (upload from its path; it deletes
    /// on drop) — plus the in-memory INDEX sidecar bytes (`None` for
    /// index-off builds). The sidecar stays in memory: its blobs were
    /// RAM-resident before assembly either way, so this matches the
    /// pre-split memory profile.
    pub fn finish_output(self) -> anyhow::Result<(VixOutput, Option<Vec<u8>>, VixWriterStats)> {
        Ok(self.finish_inner()?)
    }

    /// Test-support escape ([`crate::test_support::finish_ignoring_timestamp_guard`]):
    /// finish WITHOUT the degenerate-`_timestamp` guard, fabricating the
    /// pre-guard-era files (stored rows with `_timestamp <= 0`) that
    /// compaction-time cleansing tests need as merge inputs. Production
    /// writers must never call this — every real producer goes through
    /// [`Self::finish`]/[`Self::finish_with_stats`] and keeps the guard.
    pub(crate) fn finish_unguarded(mut self) -> Result<(Vec<u8>, Option<Vec<u8>>, VixWriterStats)> {
        self.skip_ts_guard = true;
        let (output, index, stats) = self.finish_inner()?;
        let bytes = output
            .into_bytes()
            .map_err(|e| VixError::Writer(format!("read back spooled output: {e}")))?;
        Ok((bytes, index, stats))
    }

    fn push_batch_inner(
        &mut self,
        batch: &RecordBatch,
        source: &StringArray,
        original: Option<&StringArray>,
        mode: DocsPushMode,
    ) -> Result<()> {
        debug_assert!(
            mode.indexes(),
            "push_batch_inner is a term-deriving path; StoreOnly rows go through \
             push_docs_rows_unindexed"
        );
        let num_rows = batch.num_rows();
        self.check_push_mode(mode)?;
        for reserved in [SOURCE_COL_NAME, ORIGINAL_DATA_COL_NAME] {
            if batch.column_by_name(reserved).is_some() {
                return Err(VixError::Writer(format!(
                    "batch must not contain a {reserved:?} column; it is supplied through the \
                     push_batch_with_source arguments"
                )));
            }
        }
        self.check_push_inputs(num_rows, source, original)?;
        let first_doc = self.next_first_doc(mode, num_rows)?;
        if num_rows == 0 {
            return Ok(());
        }

        if self.opts.index_enabled {
            self.index_value_terms(batch, first_doc);
            self.index_key_terms(batch, first_doc);
        }

        if mode.stores() {
            let docs_batch = self.project_docs(batch, source, original)?;
            self.track_ts_range(&docs_batch)?;
            self.stage_docs_batch(docs_batch)?;
            self.row_count += num_rows as u64;
        } else {
            // IndexOnly (#51c heal): the rows' stored form arrives
            // separately through the encoded-run API (row_count, ts range
            // and zone table advance there); only the doc-id cursor moves.
            self.advance_index_only_cursor(num_rows);
        }
        self.maybe_auto_demote_bloom_only_early()?;
        self.maybe_spill_terms()?;
        Ok(())
    }

    /// Route one projected docs batch to the docs-blob pipeline: buffered
    /// while the chunk-size sample is still open, streamed to the encoder
    /// worker after (see [`DOCS_ENCODE_SAMPLE_BYTES`]).
    fn stage_docs_batch(&mut self, batch: RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        if let Some(encoder) = self.docs_encoder.as_mut() {
            self.zone_folder
                .as_mut()
                .expect("zone folder exists whenever the encoder does")
                .fold(&batch)?;
            return encoder.push(batch);
        }
        let budget = if self.opts.docs_encode_sample_bytes == 0 {
            DOCS_ENCODE_SAMPLE_BYTES
        } else {
            self.opts.docs_encode_sample_bytes
        };
        let batch_bytes = batch.get_array_memory_size();
        // Keep the sample budget a resident-memory bound, not merely a
        // post-push threshold. Once a non-empty representative sample exists,
        // start the encoder BEFORE a whole next batch would cross the budget;
        // that batch then streams directly. A first batch larger than the
        // budget remains the explicit single-batch oversize exception.
        if !self.sample_batches.is_empty() && self.sample_bytes.saturating_add(batch_bytes) > budget
        {
            self.start_docs_encoder()?;
            return self.stage_docs_batch(batch);
        }
        self.sample_bytes = self.sample_bytes.saturating_add(batch_bytes);
        self.sample_batches.push(batch);
        if self.sample_bytes >= budget {
            self.start_docs_encoder()?;
        }
        Ok(())
    }

    /// Lock the docs chunking on the buffered sample, spawn the streaming
    /// encoder and hand it the sample (in push order).
    fn start_docs_encoder(&mut self) -> Result<()> {
        let mut rows_per_chunk = docs_rows_per_chunk(
            self.opts.docs_chunk_bytes,
            self.opts.docs_chunk_max_rows,
            &self.sample_batches,
        );
        // #51c: a passthrough merge whose FIRST rows arrive as encoded
        // chunks starts the encoder over an EMPTY sample (rows_per_chunk 0
        // = the empty-file shape). That would make the zone folder a no-op
        // for any LATER re-encoded batches — their rows would be missing
        // from the zone table, breaking its every-row-covered invariant —
        // so lock a fixed window instead. The window is only the zone
        // table's granularity and the re-encode slicing width; it need not
        // match any physical chunking (see [`ZoneMapFolder`]).
        if self.opts.docs_passthrough && rows_per_chunk == 0 {
            rows_per_chunk = PASSTHROUGH_FALLBACK_ROWS_PER_CHUNK;
        }
        let mut folder = ZoneMapFolder::new(
            rows_per_chunk,
            self.docs_schema.as_ref(),
            self.opts.stats_min_density,
            self.opts.stats_max_bytes,
            // §4 region table: only concat outputs need the desc-run
            // decomposition (a ts_desc file is one region by definition)
            self.opts.concat_row_order,
        );
        let mut encoder = DocsBlobEncoder::spawn(
            Arc::clone(&self.docs_schema),
            rows_per_chunk,
            self.opts.encode_threads,
            self.opts.output_spool_dir.clone(),
            self.opts.docs_passthrough,
            Arc::clone(&self.docs_failopen_chunks),
        )?;
        for batch in std::mem::take(&mut self.sample_batches) {
            folder.fold(&batch)?;
            encoder.push(batch)?;
        }
        self.sample_bytes = 0;
        self.zone_folder = Some(folder);
        self.docs_encoder = Some(encoder);
        Ok(())
    }

    /// Fold a stored docs batch's `_timestamp` column into the writer's
    /// running range (the [`VixWriterStats::min_ts`]/`max_ts` source). The
    /// docs schema pins the column to non-null `Int64`, so min/max exist for
    /// any non-empty batch.
    fn track_ts_range(&mut self, docs_batch: &RecordBatch) -> Result<()> {
        if docs_batch.num_rows() == 0 {
            return Ok(());
        }
        let column = docs_batch
            .column_by_name(TIMESTAMP_COL_NAME)
            .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| {
                VixError::Writer(format!(
                    "internal: docs batch lacks the {TIMESTAMP_COL_NAME:?} i64 column"
                ))
            })?;
        let (Some(min), Some(max)) = (arrow::compute::min(column), arrow::compute::max(column))
        else {
            return Err(VixError::Writer(format!(
                "internal: {TIMESTAMP_COL_NAME:?} range of a non-empty batch is undefined"
            )));
        };
        self.track_ts_bounds(min, max);
        Ok(())
    }

    /// Fold an externally computed `_timestamp` range into the writer's
    /// running range (the #51c encoded run's bounds, computed by the caller
    /// from the input's materialized timestamp column).
    fn track_ts_bounds(&mut self, min: i64, max: i64) {
        self.ts_range = Some(match self.ts_range {
            Some((cur_min, cur_max)) => (cur_min.min(min), cur_max.max(max)),
            None => (min, max),
        });
    }

    /// Reject push calls that do not match the writer's mode: after
    /// [`Self::merge_input_indexes`] only the unindexed docs push is valid
    /// (the index is already built), and vice versa. While a #51c encoded
    /// run is open, every push is rejected — its zone entries were spliced
    /// at begin, so interleaved rows would corrupt the zone table's order.
    /// Index-only pushes (#51c heal) additionally require a passthrough,
    /// indexed, non-merge-mode writer; once one arrived, the coupled push
    /// paths are rejected (docs rows then arrive ONLY as encoded chunks).
    fn check_push_mode(&self, mode: DocsPushMode) -> Result<()> {
        if self.encoded_run.is_some() {
            return Err(VixError::Writer(
                "rows pushed while an encoded docs run is open — finish_docs_encoded_run first \
                 (the run's spliced zone entries own this row range)"
                    .to_string(),
            ));
        }
        if mode == DocsPushMode::IndexOnly {
            if !self.opts.docs_passthrough {
                return Err(VixError::Writer(
                    "index-only push requires VixWriterOptions::docs_passthrough — without the \
                     passthrough docs encoder there is no path for this row's stored form, so \
                     the file would index rows it does not hold"
                        .to_string(),
                ));
            }
            if !self.opts.index_enabled {
                return Err(VixError::Writer(
                    "index-only push on an index-off writer (#40) derives no terms and stores \
                     nothing — the rows would vanish; push them as encoded chunks or through \
                     the storing paths instead"
                        .to_string(),
                ));
            }
            if self.merged_index.is_some() {
                return Err(VixError::Writer(
                    "the writer is in merge mode (merge_input_indexes); it cannot also build \
                     terms from scanned rows — index-only pushes are for the heal rebuild"
                        .to_string(),
                ));
            }
            return Ok(());
        }
        if self.index_only_rows.is_some() {
            if mode == DocsPushMode::StoreOnly {
                // M17 mixed passthrough rebuild: an input the chunk copy
                // cannot express (type flip, stats-less) re-encodes through
                // decoded STORE-ONLY pushes while its terms came from the
                // detached index-only scan — same split accounting as the
                // encoded-run API (index_only_rows vs row_count; finish
                // still refuses any divergence).
                return Ok(());
            }
            return Err(VixError::Writer(
                "this writer is in index-only build mode (#51c heal): docs rows arrive only \
                 through the encoded-run API or store-only pushes, coupled index+store pushes \
                 would fork the doc-id and row accounting"
                    .to_string(),
            ));
        }
        if !self.opts.index_enabled {
            // index-off (#40): there is no index either way — both push
            // variants store docs rows identically, so merge closures can
            // reuse either API
            return Ok(());
        }
        match (
            mode == DocsPushMode::IndexAndStore,
            self.merged_index.is_some(),
        ) {
            (true, true) => Err(VixError::Writer(
                "the writer is in merge mode (merge_input_indexes); rows must be stored with \
                 push_docs_rows_unindexed"
                    .to_string(),
            )),
            (false, false) => Err(VixError::Writer(
                "push_docs_rows_unindexed requires merge_input_indexes first (the file would \
                 have no index for its rows)"
                    .to_string(),
            )),
            _ => Ok(()),
        }
    }

    /// The doc id of the next pushed row plus the u32 capacity check, on
    /// the counter `mode` advances: the stored-row count for coupled/store
    /// pushes, the index-only cursor for detached index pushes (#51c heal —
    /// latched to `Some` here so the mode is declared even by an empty
    /// first push).
    fn next_first_doc(&mut self, mode: DocsPushMode, num_rows: usize) -> Result<u64> {
        if mode == DocsPushMode::IndexOnly {
            let first_doc = *self.index_only_rows.get_or_insert(0);
            if first_doc + num_rows as u64 > u64::from(u32::MAX) {
                return Err(VixError::Writer(format!(
                    "doc id overflow: {} total rows exceed the u32 doc-id space",
                    first_doc + num_rows as u64
                )));
            }
            Ok(first_doc)
        } else {
            self.check_doc_capacity(num_rows)
        }
    }

    /// Advance the index-only doc-id cursor past one pushed chunk.
    fn advance_index_only_cursor(&mut self, num_rows: usize) {
        *self
            .index_only_rows
            .as_mut()
            .expect("latched by next_first_doc") += num_rows as u64;
    }

    /// Shared validation of the push paths: writer state plus the
    /// `_source`/`_original` array contracts.
    fn check_push_inputs(
        &self,
        num_rows: usize,
        source: &StringArray,
        original: Option<&StringArray>,
    ) -> Result<()> {
        if let Some(error) = &self.init_error {
            return Err(VixError::Writer(error.clone()));
        }
        if source.len() != num_rows {
            return Err(VixError::Writer(format!(
                "source array has {} rows but the batch has {num_rows}",
                source.len()
            )));
        }
        if source.null_count() > 0 {
            return Err(VixError::Writer(
                "_source is required per record; the source array contains nulls".to_string(),
            ));
        }
        match original {
            Some(_) if !self.store_original => {
                return Err(VixError::Writer(
                    "original strings passed to a writer built with store_original = false"
                        .to_string(),
                ));
            }
            Some(values) if values.len() != num_rows => {
                return Err(VixError::Writer(format!(
                    "original array has {} rows but the batch has {num_rows}",
                    values.len()
                )));
            }
            _ => {}
        }
        Ok(())
    }

    fn push_docs_rows_inner(
        &mut self,
        timestamps: &Int64Array,
        cs_columns: &[(String, ArrowArrayRef)],
        source: &StringArray,
        original: Option<&StringArray>,
        mode: DocsPushMode,
    ) -> Result<()> {
        let num_rows = timestamps.len();
        self.check_push_mode(mode)?;
        self.check_push_inputs(num_rows, source, original)?;
        if timestamps.null_count() > 0 {
            return Err(VixError::Writer(
                "_timestamp is required per record; the timestamps array contains nulls"
                    .to_string(),
            ));
        }
        for (name, column) in cs_columns {
            if column.len() != num_rows {
                return Err(VixError::Writer(format!(
                    "column {name:?} has {} rows but the chunk has {num_rows}",
                    column.len()
                )));
            }
        }
        // #52: bloom-only values ride the docs columns on this path (the
        // merge fast path never runs term extraction) — hash them here so
        // merged files keep composite coverage even when the inputs carried
        // no dictionary terms for the field. Deduped by the hash set, so
        // the indexed rebuild (whose source-driven derivation hashes the
        // same values again) and the heal's index-only scan converge on
        // identical coverage.
        self.hash_bloom_only_columns(cs_columns);
        let first_doc = self.next_first_doc(mode, num_rows)?;
        if num_rows == 0 {
            return Ok(());
        }

        if mode.indexes() && self.opts.index_enabled {
            self.index_source_terms(source, first_doc)?;
        }

        if mode.stores() {
            let docs_batch = self.assemble_docs_rows(timestamps, cs_columns, source, original)?;
            self.track_ts_range(&docs_batch)?;
            self.stage_docs_batch(docs_batch)?;
            self.row_count += num_rows as u64;
        } else {
            // IndexOnly (#51c heal): the rows' stored form arrives
            // separately through the encoded-run API (row_count, ts range
            // and zone table advance there); only the doc-id cursor moves.
            self.advance_index_only_cursor(num_rows);
        }
        self.maybe_auto_demote_bloom_only_early()?;
        self.maybe_spill_terms()?;
        Ok(())
    }

    /// #52: hash the bloom-only fields' values out of materialized docs
    /// columns into `self.bloom_only` — shared by the streamed merge push
    /// ([`Self::push_docs_rows_unindexed`]) and the #51c passthrough's
    /// projected bloom scan ([`Self::absorb_bloom_only_columns`]), so both
    /// derive the identical composite coverage. No-op when no field is
    /// bloom-only; values over `max_raw_term_len` are skipped (#41 policy;
    /// skip counters live on the build path).
    fn hash_bloom_only_columns(&mut self, cs_columns: &[(String, ArrowArrayRef)]) {
        if self.bloom_only.is_empty() {
            return;
        }
        let mut scratch = std::mem::take(&mut self.scratch);
        let max_raw_term_len = self.opts.max_raw_term_len;
        for (name, column) in cs_columns {
            let Some(&fid) = self.term_field_ids.get(name) else {
                continue;
            };
            let Some((bname, hashes)) = self.bloom_only.get_mut(&fid) else {
                continue;
            };
            hash_bloom_only_column_values(bname, column, max_raw_term_len, &mut scratch, hashes);
        }
        self.scratch = scratch;
    }

    /// #52/M7 AUTO bloom-only demotion at FIRST ENCODE (and unspilled
    /// rebuilds): count each field's distinct value terms from the
    /// accumulated term map, run the shared AUTO rule
    /// ([`resolve_auto_bloom_only`] — the same function the merge planner
    /// applies to input dictionaries), and DEMOTE the selected fields by
    /// carving their value-term range out of the map (field-major keys make
    /// that a contiguous `[fid, fid+1)` slice; KEY terms live under
    /// [`KEY_FIELD_ID`] and stay, keeping `IS [NOT] NULL` exact) while
    /// hashing every removed raw value into the field's composite-bloom set
    /// — exactly where a construction-time demotion would have put it at
    /// push time. Everything downstream (fields-table `bloom` marker,
    /// per-field-bloom exclusion, composite coverage + guards via
    /// `absorb_composite_hashes`) then follows from `self.bloom_only`
    /// membership, so a demoted-at-birth field is byte-identical to a
    /// construction-list demotion.
    ///
    /// Candidate filters mirror the construction resolution: string-family
    /// ∩ term plan − fts − never-list (inside the shared rule) − already
    /// demoted; `partial_fields` members are excluded too — their term maps
    /// are knowingly incomplete, and claiming composite coverage from an
    /// incomplete value set would turn bloom misses into wrong drops.
    ///
    /// SKIPPED when the term map spilled: the resident map then holds only
    /// a suffix of the terms, distinct counts would undercount, and a
    /// demotion decided on partial counts could strand spilled-run values
    /// outside the bloom. Move-job builds never spill; a budget-crossing
    /// rebuild keeps its terms and converges at its next merge instead
    /// (input-dictionary AUTO).
    fn auto_demote_bloom_only_at_finish(&mut self, row_count: u64) {
        self.auto_demote_bloom_only(row_count, "build");
    }

    /// Use a caller-known final-row maximum to discard value postings
    /// as soon as AUTO demotion is guaranteed. This is especially valuable
    /// for unique identifiers: without it, the writer built and sorted a
    /// near-row-count dictionary only to remove it at finish.
    fn maybe_auto_demote_bloom_only_early(&mut self) -> Result<()> {
        let Some(row_upper_bound) = self.auto_demote_expected_max_rows else {
            return Ok(());
        };
        let observed_rows = self.index_only_rows.unwrap_or(self.row_count);
        if observed_rows > row_upper_bound {
            if self.auto_demoted_early {
                return Err(VixError::Writer(format!(
                    "writer exceeded the expected maximum rows after early AUTO bloom-only \
                     demotion: observed {observed_rows} rows, expected maximum {row_upper_bound}"
                )));
            }
            // No decision used the hint yet, so discard a bad hint and let
            // finish evaluate AUTO against the actual row count.
            self.auto_demote_expected_max_rows = None;
            return Ok(());
        }
        if self.term_spill.is_some() || row_upper_bound == 0 {
            return Ok(());
        }
        let required_distinct = self
            .opts
            .bloom_only_min_distinct
            .max((self.opts.bloom_only_auto_ratio * row_upper_bound as f64).ceil() as u64);
        if observed_rows < required_distinct {
            return Ok(());
        }
        if self.auto_demote_bloom_only(row_upper_bound, "build-early") > 0 {
            self.auto_demoted_early = true;
        }
        Ok(())
    }

    /// Demote every currently qualifying field using `ratio_rows` as the
    /// denominator. Returns the number newly demoted.
    fn auto_demote_bloom_only(&mut self, ratio_rows: u64, phase: &str) -> usize {
        let ratio = self.opts.bloom_only_auto_ratio;
        if !self.opts.index_enabled || ratio <= 0.0 || ratio_rows == 0 || self.terms.is_empty() {
            return 0;
        }
        if self.term_spill.is_some() {
            log::debug!(
                "vix build: term map spilled; skipping first-encode AUTO bloom-only demotion \
                 (counts would be partial)"
            );
            return 0;
        }
        let candidates: Vec<(&str, u64)> = self
            .term_fields
            .iter()
            .enumerate()
            .filter_map(|(fid, _name)| {
                let fid = fid as u16;
                let distinct = self.terms.field_len(fid) as u64;
                if distinct == 0 {
                    return None;
                }
                let name = self.term_fields.get(usize::from(fid))?;
                (!self.fts_fields.contains(name)
                    && !self.non_string_term_fields.contains(name)
                    && !self.demoted_fields.contains(name)
                    && !self.partial_fields.contains(name)
                    && !self.bloom_only.contains_key(&fid))
                .then_some((name.as_str(), distinct))
            })
            .collect();
        let selected = resolve_auto_bloom_only(
            candidates,
            ratio_rows,
            ratio,
            self.opts.bloom_only_min_distinct,
            &self.opts.bloom_only_never,
            phase,
        );
        let selected_count = selected.len();
        for name in selected {
            let Some(&fid) = self.term_field_ids.get(&name) else {
                continue;
            };
            // Remove the complete field shard and fold its raw values into
            // the bloom set. No global-map range walk or split is needed.
            let demoted = self.terms.take_field(fid);
            let mut scratch = std::mem::take(&mut self.scratch);
            let mut hashes: FastHashSet<u64> =
                FastHashSet::with_capacity_and_hasher(demoted.len(), GlobalState::default());
            for token in demoted.keys() {
                // the token IS the raw string value: candidates exclude
                // fts (tokens) and non-string (tagged canonical) fields
                if let Some(k) = crate::bloom::composite_value_key(&name, token, &mut scratch) {
                    hashes.insert(crate::sbbf::hash_value(k));
                }
            }
            self.scratch = scratch;
            self.bloom_only.insert(fid, (name, hashes));
        }
        selected_count
    }

    /// Spill the term map to a sorted run when it crosses the budget —
    /// only at push (batch) boundaries, which is what guarantees the
    /// cursor-order postings-concatenation invariant of the finish merge
    /// (doc ids grow monotonically across pushes, so a term's doc ranges
    /// never interleave between runs). No-op unless
    /// [`VixWriterOptions::term_spill_dir`] is set.
    fn maybe_spill_terms(&mut self) -> Result<()> {
        let Some(dir) = self.opts.term_spill_dir.as_deref() else {
            return Ok(());
        };
        let budget = if self.opts.term_spill_bytes == 0 {
            spill::DEFAULT_TERM_SPILL_BYTES
        } else {
            self.opts.term_spill_bytes
        };
        if self.terms.estimated_bytes() < budget || self.terms.is_empty() {
            return Ok(());
        }
        if self.term_spill.is_none() {
            self.term_spill = Some(spill::TermSpill::new(dir)?);
        }
        self.term_spill
            .as_mut()
            .expect("created above")
            .write_run(&mut self.terms)?;
        Ok(())
    }

    /// Source-driven term extraction: parse each `_source` object and emit
    /// the same key/value/fts terms the column-driven path derives from
    /// flattened columns (see [`Self::push_docs_rows`] for the exact rules).
    fn index_source_terms(&mut self, source: &StringArray, first_doc: u64) -> Result<()> {
        use sonic_rs::JsonValueTrait;
        for row in 0..source.len() {
            let doc = (first_doc + row as u64) as u32;
            let text = source.value(row);
            // SIMD lazy parse (sonic-rs): iterate (key, raw-span) pairs
            // without materializing a DOM. Strings unescape on demand;
            // numbers stay RAW TEXT until a `serde_json::Number`
            // (arbitrary_precision) re-parses just the token, so
            // `canonical_number_text` sees exactly what the old whole-doc
            // parse carried — byte parity with the column-driven derivation
            // is pinned by the differential tests. `_source` is
            // engine-synthesized from a Map, so objects carry no duplicate
            // keys (a hand-crafted duplicate would now index every
            // occurrence instead of serde_json's last-wins).
            for entry in sonic_rs::to_object_iter(text) {
                let (key, value) = entry.map_err(|e| {
                    VixError::Writer(format!("_source of doc {doc} is not a JSON object: {e}"))
                })?;
                if value.is_null() {
                    // synthesis omits nulls; treat a stray one as absent
                    continue;
                }
                let key: &str = key.as_ref();
                if NON_INDEXED_COLS.contains(&key) || key == SOURCE_COL_NAME {
                    continue;
                }
                // key term: this doc has a value at `key`
                self.terms.push(KEY_FIELD_ID, key.as_bytes(), doc);

                match value.get_type() {
                    // a JSON string emits its value terms — fts tokens or
                    // the raw whole value
                    sonic_rs::JsonType::String => {
                        let Some(&field_id) = self.term_field_ids.get(key) else {
                            // a string value we cannot value-index (the key
                            // is not a term field of the writer schema, or
                            // overflowed the field-id space): lookups on it
                            // may miss docs
                            self.partial_fields.insert(key.to_string());
                            continue;
                        };
                        let Some(value) = value.as_str() else {
                            return Err(VixError::Writer(format!(
                                "_source of doc {doc}: invalid JSON string at key {key:?}"
                            )));
                        };
                        if self.bloom_only.contains_key(&field_id) {
                            // #52 bloom-only: the raw value is hashed in the
                            // composite key form and never touches the
                            // dictionary. Oversize values inherit the #41
                            // skip policy (the bloom then can't answer for
                            // them — same accepted hole as the index had).
                            if value.len() > self.opts.max_raw_term_len {
                                *self.oversize_skips.entry(key.to_string()).or_default() += 1;
                            } else {
                                let mut scratch = std::mem::take(&mut self.scratch);
                                let (name, hashes) =
                                    self.bloom_only.get_mut(&field_id).expect("checked");
                                if let Some(k) = crate::bloom::composite_value_key(
                                    name,
                                    value.as_bytes(),
                                    &mut scratch,
                                ) {
                                    hashes.insert(crate::sbbf::hash_value(k));
                                }
                                self.scratch = scratch;
                            }
                        } else if self.fts_fields.contains(key) {
                            // fts fields: tokens only, never the raw whole
                            // value (an empty value simply yields no
                            // tokens). Identical to the column-driven path:
                            // the value's length is irrelevant — tokens are
                            // byte-bounded by the tokenizer's own max, so
                            // `max_raw_term_len` (a RAW-term bound) never
                            // applies, oversize values still tokenize, and
                            // the field never degrades to `partial_fields`.
                            for token in
                                o2_tokenize(value, self.opts.min_token_len, self.opts.max_token_len)
                            {
                                self.terms.push(field_id, token.as_bytes(), doc);
                            }
                        } else if value.len() > self.opts.max_raw_term_len {
                            // oversize raw value: skipped WITHOUT degrading
                            // the field (owner call 2026-08-12) — identical
                            // to the column-driven path, so rebuilds do not
                            // re-taint what the move build left clean
                            *self.oversize_skips.entry(key.to_string()).or_default() += 1;
                        } else {
                            // the empty string included: `""` is a value
                            // (distinct from null) and its fid-only composite
                            // key is valid, so `field = ''` answers from the
                            // index
                            self.terms.push(field_id, value.as_bytes(), doc);
                        }
                    }
                    // a JSON number emits its tagged CANONICAL value term
                    // (crate::numeric). Keys outside the writer's term plan
                    // get no term and no partial mark: without a fields-table
                    // entry, per-field lookups already skip + filter back,
                    // and numbers carry no match_all token contract (unlike
                    // unindexable strings, whose missing tokens force the
                    // partial taint). Field-id overflow was already recorded
                    // as partial at construction.
                    sonic_rs::JsonType::Number => {
                        if self.fts_fields.contains(key) {
                            continue; // numbers have no tokens
                        }
                        let Some(&field_id) = self.term_field_ids.get(key) else {
                            continue;
                        };
                        // re-parse just the number token through serde_json's
                        // arbitrary_precision Number: exact-text semantics,
                        // identical canonicalization to the old DOM parse
                        let number: serde_json::Number =
                            serde_json::from_str(value.as_raw_str().trim()).map_err(|e| {
                                VixError::Writer(format!(
                                    "_source of doc {doc}: invalid JSON number at key {key:?}: {e}"
                                ))
                            })?;
                        let Some(text) = canonical_number_text(&number) else {
                            continue; // ±Inf overflow text: value-less, like null
                        };
                        if text.len() + 1 > self.opts.max_raw_term_len {
                            *self.oversize_skips.entry(key.to_string()).or_default() += 1;
                            continue;
                        }
                        push_numeric_term(
                            &mut self.terms,
                            &mut self.tag_scratch,
                            &text,
                            field_id,
                            doc,
                        );
                    }
                    sonic_rs::JsonType::Boolean => {
                        if self.fts_fields.contains(key) {
                            continue;
                        }
                        let Some(&field_id) = self.term_field_ids.get(key) else {
                            continue;
                        };
                        // get_type read the token's first byte; the raw span
                        // is exactly `true`/`false`
                        let flag = value.as_bool().unwrap_or(false);
                        push_numeric_term(
                            &mut self.terms,
                            &mut self.tag_scratch,
                            canonical_bool_text(flag),
                            field_id,
                            doc,
                        );
                    }
                    // flattened `_source` objects hold scalars only;
                    // defensive no-op for anything else
                    _ => {}
                }
            }
        }
        Ok(())
    }

    /// Assemble one `docs` blob batch from loose arrays (the
    /// [`Self::push_docs_rows`] storage side).
    fn assemble_docs_rows(
        &self,
        timestamps: &Int64Array,
        cs_columns: &[(String, ArrowArrayRef)],
        source: &StringArray,
        original: Option<&StringArray>,
    ) -> Result<RecordBatch> {
        let docs_schema = &self.docs_schema;
        // A supplied name that is NOT a docs column is a caller bug (typo /
        // plan-schema drift) — its values would silently vanish. Error
        // loudly instead. The reverse direction is legitimate since v2
        // all-columns: a docs column the caller has no data for stores null
        // (a merge input lacking the column contributes nulls by design).
        for (name, _) in cs_columns {
            if name == TIMESTAMP_COL_NAME
                || name == SOURCE_COL_NAME
                || name == ORIGINAL_DATA_COL_NAME
                || docs_schema.field_with_name(name).is_err()
            {
                return Err(VixError::Writer(format!(
                    "push_docs_rows supplied column {name:?} which is not a docs column of \
                     this writer — its values would be dropped"
                )));
            }
        }
        let mut arrays: Vec<ArrowArrayRef> = Vec::with_capacity(docs_schema.fields().len());
        for field in docs_schema.fields() {
            let array: ArrowArrayRef = match field.name().as_str() {
                TIMESTAMP_COL_NAME => Arc::new(timestamps.clone()),
                SOURCE_COL_NAME => Arc::new(source.clone()),
                ORIGINAL_DATA_COL_NAME => match original {
                    Some(values) => Arc::new(values.clone()),
                    None => Arc::new(StringArray::new_null(timestamps.len())),
                },
                name => match cs_columns
                    .iter()
                    .find(|(cs_name, _)| cs_name == name)
                    .map(|(_, column)| column)
                {
                    Some(column) => array_cast_as(column, field)?,
                    // no data for this docs column in these rows: null
                    None => arrow::array::new_null_array(field.data_type(), timestamps.len()),
                },
            };
            arrays.push(array);
        }
        RecordBatch::try_new(Arc::clone(docs_schema), arrays)
            .map_err(|e| VixError::Writer(format!("docs batch: {e}")))
    }

    /// Keep doc ids (and per-term doc counts) strictly within `u32`; returns
    /// the doc id of the batch's first row.
    fn check_doc_capacity(&self, num_rows: usize) -> Result<u64> {
        let first_doc = self.row_count;
        if first_doc + num_rows as u64 > u64::from(u32::MAX) {
            return Err(VixError::Writer(format!(
                "doc id overflow: {} total rows exceed the u32 doc-id space",
                first_doc + num_rows as u64
            )));
        }
        Ok(first_doc)
    }

    /// Emit the value terms of `batch`: tokens for fts fields, the raw whole
    /// value (empty strings included) for every other string field, and
    /// tagged canonical value terms for numeric/bool columns (see
    /// [`crate::numeric`]).
    fn index_value_terms(&mut self, batch: &RecordBatch, first_doc: u64) {
        let num_rows = batch.num_rows();
        for (field_id, field_name) in self.term_fields.iter().enumerate() {
            let Some(column) = batch.column_by_name(field_name) else {
                // Tolerate a column missing from this batch: all-null.
                continue;
            };
            let field_id = field_id as u16;
            let is_fts = self.fts_fields.contains(field_name);
            if let Some(strings) = StringColumn::try_new(column.as_ref()) {
                if is_fts {
                    // fts fields: tokens only, never the raw whole value (an
                    // empty value simply yields no tokens). The whole value's
                    // LENGTH is irrelevant — tokens are byte-bounded by the
                    // tokenizer's own max, so `max_raw_term_len` (a RAW-term
                    // bound) never applies and no value is ever skipped: an
                    // oversize log line still contributes every token, and
                    // the field never degrades to `partial_fields` (which
                    // would cost whole-file match_all filter-backs — the
                    // live regression this fixed).
                    for row in 0..num_rows {
                        let Some(value) = strings.value(row) else {
                            continue;
                        };
                        let doc = (first_doc + row as u64) as u32;
                        for token in
                            o2_tokenize(value, self.opts.min_token_len, self.opts.max_token_len)
                        {
                            self.terms.push(field_id, token.as_bytes(), doc);
                        }
                    }
                } else if self.bloom_only.contains_key(&field_id) {
                    // #52 bloom-only: values hash into the composite key
                    // form, no dictionary entries. Oversize inherits #41.
                    let mut scratch = std::mem::take(&mut self.scratch);
                    let mut oversize = 0u64;
                    let (name, hashes) = self.bloom_only.get_mut(&field_id).expect("checked");
                    for row in 0..num_rows {
                        let Some(value) = strings.value(row) else {
                            continue;
                        };
                        if value.len() > self.opts.max_raw_term_len {
                            oversize += 1;
                            continue;
                        }
                        if let Some(k) =
                            crate::bloom::composite_value_key(name, value.as_bytes(), &mut scratch)
                        {
                            hashes.insert(crate::sbbf::hash_value(k));
                        }
                    }
                    self.scratch = scratch;
                    if oversize > 0 {
                        *self.oversize_skips.entry(field_name.clone()).or_default() += oversize;
                    }
                } else {
                    for row in 0..num_rows {
                        let Some(value) = strings.value(row) else {
                            continue;
                        };
                        if value.len() > self.opts.max_raw_term_len {
                            // Oversize raw value: skipped from the term index
                            // WITHOUT degrading the field (owner call
                            // 2026-08-12, performance-first). The index stays
                            // authoritative — an equality probe for THIS
                            // literal silently misses this row; every other
                            // value keeps exact index answers. The row's key
                            // term still lands (IS [NOT] NULL stays exact).
                            *self.oversize_skips.entry(field_name.clone()).or_default() += 1;
                            continue;
                        }
                        let doc = (first_doc + row as u64) as u32;
                        // the empty string included: `""` is a value
                        // (distinct from null) and its fid-only composite key
                        // is valid, so `field = ''` answers from the index
                        self.terms.push(field_id, value.as_bytes(), doc);
                    }
                }
            } else if let Some(numbers) = NumericColumn::try_new(column.as_ref()) {
                if is_fts {
                    // numbers/bools have no tokens — exactly what the
                    // source-driven path does for non-string values under an
                    // fts field (no terms, no partial mark)
                    continue;
                }
                let mut text = String::new();
                for row in 0..num_rows {
                    if !numbers.canonical_into(row, &mut text) {
                        continue; // null, or non-finite float (== null in _source)
                    }
                    if text.len() + 1 > self.opts.max_raw_term_len {
                        // canonical texts are ≤ ~25 bytes; guard kept for
                        // uniformity with the raw-term path (skip + count,
                        // no field degrade — same policy)
                        *self.oversize_skips.entry(field_name.clone()).or_default() += 1;
                        continue;
                    }
                    let doc = (first_doc + row as u64) as u32;
                    push_numeric_term(&mut self.terms, &mut self.tag_scratch, &text, field_id, doc);
                }
            } else {
                // The batch stores this term field under a type with no term
                // derivation (per-batch schema drift to e.g. Timestamp): its
                // values cannot be term-indexed, so per-field lookups could
                // silently miss these rows — mark the field partial (scan
                // fallback), like any other unindexable value.
                self.partial_fields.insert(field_name.clone());
            }
        }
    }

    /// Emit one key term (`{path}\x00\xFF\xFF`) per doc per
    /// non-internal batch column with a non-null value in that row. Columns
    /// of any arrow type participate; an empty string is a value, a null is
    /// not — and a **non-finite float** (NaN/±Inf) is treated as null:
    /// `_source` is authoritative, and arrow-json serializes those as the
    /// JSON literal `null`, so the source-driven writer (compaction rebuild)
    /// sees them as absent. Keying them here would make the two derivations
    /// disagree on `IS NOT NULL`. Key terms bypass field-id assignment
    /// entirely, so fields beyond the real-field-id cap still get them.
    fn index_key_terms(&mut self, batch: &RecordBatch, first_doc: u64) {
        let num_rows = batch.num_rows();
        for (index, field) in batch.schema_ref().fields().iter().enumerate() {
            let name = field.name().as_str();
            if NON_INDEXED_COLS.contains(&name) || name == SOURCE_COL_NAME {
                continue;
            }
            let column = batch.column(index);
            // `Some(mask)` for float columns: `mask[row]` = valid AND finite.
            let finite = finite_float_mask(column.as_ref());
            let emits_any = match &finite {
                Some(mask) => mask.iter().any(|&keep| keep),
                None => column.null_count() < column.len(),
            };
            if !emits_any {
                continue; // the path exists in none of this batch's docs
            }
            let all_valid = finite.is_none() && column.null_count() == 0;
            self.terms.extend(
                KEY_FIELD_ID,
                name.as_bytes(),
                (0..num_rows).filter_map(|row| {
                    let keep = match &finite {
                        Some(mask) => mask[row],
                        None => all_valid || column.is_valid(row),
                    };
                    keep.then_some((first_doc + row as u64) as u32)
                }),
            );
        }
    }

    /// Assemble one `docs` blob batch: the projected/cast stored columns of
    /// `batch` plus the caller-supplied `_source`/`_original` strings.
    fn project_docs(
        &self,
        batch: &RecordBatch,
        source: &StringArray,
        original: Option<&StringArray>,
    ) -> Result<RecordBatch> {
        let docs_schema = &self.docs_schema;
        let mut arrays: Vec<ArrowArrayRef> = Vec::with_capacity(docs_schema.fields().len());
        for field in docs_schema.fields() {
            let array: ArrowArrayRef = match field.name().as_str() {
                SOURCE_COL_NAME => Arc::new(source.clone()),
                ORIGINAL_DATA_COL_NAME => match original {
                    Some(values) => Arc::new(values.clone()),
                    None => Arc::new(StringArray::new_null(batch.num_rows())),
                },
                _ => batch_column_as(batch, field)?,
            };
            arrays.push(array);
        }
        RecordBatch::try_new(Arc::clone(docs_schema), arrays)
            .map_err(|e| VixError::Writer(format!("docs batch: {e}")))
    }

    fn finish_inner(mut self) -> Result<(VixOutput, Option<Vec<u8>>, VixWriterStats)> {
        if let Some(error) = &self.init_error {
            return Err(VixError::Writer(error.clone()));
        }
        if let Some(run) = &self.encoded_run {
            return Err(VixError::Writer(format!(
                "finish with an open encoded docs run ({} rows still owed) — \
                 finish_docs_encoded_run first",
                run.remaining
            )));
        }
        // #51c heal: an index built from a detached scan must cover the
        // stored rows EXACTLY — a shortfall or surplus means the postings'
        // doc ids misaddress the docs blob (the corruption class the prior
        // incident taught us to refuse loudly).
        if let Some(indexed) = self.index_only_rows
            && indexed != self.row_count
        {
            return Err(VixError::Writer(format!(
                "index-only build indexed {indexed} rows but the docs store holds {} — the \
                 index's doc ids would misaddress the stored rows; refusing to finish",
                self.row_count
            )));
        }
        let row_count = self.row_count;
        let (min_ts, max_ts) = self.ts_range.unwrap_or((0, 0));
        // HARD guard, not a warning: a non-empty file whose `_timestamp`
        // range is degenerate must never be built — its FileMeta would carry
        // min_ts/max_ts ≤ 0 into the file_list DB, poisoning time-range
        // pruning and wedging the compactor's commit loop (observed live:
        // rows minted with `_timestamp = 0` by a lossy upstream coercion).
        // Timestamps are microseconds since epoch; 0/negative values are
        // always corrupt inputs, so refuse loudly (before any blob encode)
        // and name the range. The producers' pipelines CLEANSE such rows
        // before they reach the writer (core_writer merge/move), so tripping
        // this guard means a NEW bug, not old data — defense in depth.
        // `skip_ts_guard` is the test-support fabrication escape only.
        if !self.skip_ts_guard && row_count > 0 && (min_ts <= 0 || max_ts <= 0) {
            return Err(VixError::Writer(format!(
                "refusing to finish a {row_count}-row file with a degenerate _timestamp range \
                 [{min_ts}, {max_ts}]: some stored row carries a zero/negative timestamp"
            )));
        }

        // Docs: make sure the streaming encoder ran — files below the
        // sample budget (and the empty file, whose schema-only blob encodes
        // the same way) spawn it here on the buffered sample — then signal
        // the end of the batches. The worker drains and finalizes the docs
        // blob WHILE the index blobs encode below; `join` collects the
        // `MAGIC`-prefixed container buffer the docs bytes were streamed
        // into (never copied again — see `finish_streamed_container`).
        if self.docs_encoder.is_none() {
            self.start_docs_encoder()?;
        }
        let mut encoder = self.docs_encoder.take().expect("started above");
        let zone_folder = self.zone_folder.take().expect("created with the encoder");
        encoder.signal_finish()?;

        let (blobs, term_count) = self.assemble_index_blobs(row_count)?;
        let (sink, docs_size) = encoder.join()?;
        // Zone table + per-column chunk stats: one `(row_count, ts_min,
        // ts_max)` entry per docs row-block, folded over the stored
        // `_timestamp` values windowed by the same `rows_per_chunk` the docs
        // strategy blocks on, plus the H2 per-column table over the SAME
        // windows. Cheap (one pass, no blob read-back), derived for EVERY
        // finish and SPLICED through the passthrough — the move-job build
        // and the compactor merge both land here.
        let (zone_map, column_presence, stats_blob, row_regions) = zone_folder.finish();

        // DATA-object properties: everything that describes the stored docs
        // themselves. Survives index heals (a later milestone rewrites only
        // the sidecar) by construction.
        let mut data_properties = vec![
            (PROP_VERSION.to_string(), VIX_FORMAT_VERSION.to_string()),
            (PROP_ROW_COUNT.to_string(), row_count.to_string()),
            (
                PROP_ROW_GROUP_SIZE.to_string(),
                self.opts.row_group_size.to_string(),
            ),
            // The docs-column field list WITH per-column present-row counts
            // (field presence is data-descriptive and feeds pruning +
            // sidecar-less column routing): the docs schema minus the
            // reserved `_source`/`_original` columns — exactly the set the
            // fields table types "cs". `_timestamp` is non-null by contract
            // (count = row_count); a splice from a presence-less input
            // degrades a column's count to unknown (plain-name entry).
            (
                PROP_COLUMNS.to_string(),
                crate::stats::encode_columns_prop(
                    &self
                        .docs_schema
                        .fields()
                        .iter()
                        .map(|field| field.name().as_str())
                        .filter(|name| *name != SOURCE_COL_NAME && *name != ORIGINAL_DATA_COL_NAME)
                        .map(|name| {
                            if name == TIMESTAMP_COL_NAME {
                                (name.to_string(), Some(row_count))
                            } else {
                                (
                                    name.to_string(),
                                    column_presence
                                        .iter()
                                        .find(|(column, _)| column == name)
                                        .and_then(|(_, count)| *count),
                                )
                            }
                        })
                        .collect::<Vec<_>>(),
                )?,
            ),
            // #51c-c row order, stamped EXPLICITLY on every file: "ts_desc"
            // (the storage convention — also what a MISSING property means)
            // or "concat" (a concatenation-order merge output whose rows are
            // NOT globally time-sorted; readers disable order-dependent
            // fast paths for it).
            (
                PROP_ROW_ORDER.to_string(),
                if self.opts.concat_row_order {
                    ROW_ORDER_CONCAT
                } else {
                    ROW_ORDER_TS_DESC
                }
                .to_string(),
            ),
        ];
        // Oversize-skip allowance: stamped only when something was skipped
        // (absent == {} for readers). Data-side: it records what the DATA
        // holds that the term index does not, so it must survive
        // sidecar-only rewrites.
        if !self.oversize_skips.is_empty() {
            data_properties.push((
                PROP_OVERSIZE_SKIPS.to_string(),
                serde_json::to_string(&self.oversize_skips)?,
            ));
        }
        // Only non-empty files get a zone table (an empty file has no chunks
        // and its decode path already returns the empty result).
        if !zone_map.is_empty() {
            data_properties.push((PROP_ZONE_MAP.to_string(), serde_json::to_string(&zone_map)?));
        }
        // §4 REGION table: stamped only on concat outputs with a PROVEN
        // internally-DESC run decomposition (derived from the stored
        // `_timestamp` values on the decode path, spliced from the inputs'
        // own tables on the passthrough path). Absent = piecewise order
        // unknown — readers keep the full-sort path (fail-open).
        if self.opts.concat_row_order
            && let Some(regions) = row_regions
            && !regions.is_empty()
        {
            data_properties.push((
                PROP_ROW_REGIONS.to_string(),
                serde_json::to_string(&regions)?,
            ));
        }
        // §4 all-present-columns completeness: the caller's assertion that
        // no `_source` field is missing from the columns list — the license
        // for absent-column file pruning. Absent = incomplete (fail-open).
        if self.opts.columns_complete {
            data_properties.push((PROP_COLUMNS_COMPLETE.to_string(), "true".to_string()));
        }
        // H2 per-column chunk stats: a small tail blob (near the footer, so
        // the eager tail fetch usually covers it). Every NON-EMPTY file
        // carries one — even with zero qualifying columns (all-sparse
        // schemas), so stats-era files always splice through the
        // passthrough; only empty files (no chunks) omit it.
        let data_blobs: Vec<(&'static str, &'static str, Vec<u8>)> = match stats_blob {
            Some(blob) if !zone_map.is_empty() => vec![(BLOB_TYPE_STATS, BLOB_TAG_STATS, blob)],
            _ => Vec::new(),
        };
        let output = finish_streamed_container(sink, docs_size, data_properties, data_blobs)?;

        let index_output = self.build_index_sidecar_container(row_count, term_count, blobs)?;
        let index_size = index_output.as_ref().map_or(0, |bytes| bytes.len() as u64);
        let oversize_skipped: u64 = self.oversize_skips.values().sum();
        if oversize_skipped > 0 {
            // Observability for the accepted-miss trade: these values have
            // no term, so equality probes for them return nothing from this
            // file — while the per-field allowance keeps the dictionary
            // top-k serves eligible (counts omit the skipped values).
            log::info!(
                "vix writer: skipped {oversize_skipped} oversize raw value(s) (> {} bytes) from \
                 the term index without field degrade: {:?}",
                self.opts.max_raw_term_len,
                self.oversize_skips
            );
        }
        let stats = VixWriterStats {
            row_count,
            term_count,
            index_size,
            docs_size,
            oversize_skipped,
            min_ts,
            max_ts,
        };
        Ok((output, index_output, stats))
    }

    /// Build the INDEX-side blobs (terms/plist/dict + per-file blooms) from
    /// this writer's accumulated term + bloom state over `row_count` doc
    /// ids. Shared by [`Self::finish_inner`] (coupled docs+index build) and
    /// [`Self::finish_index_sidecar`] (sidecar-only heal) so the two emit
    /// byte-identical index blobs for the same pushes.
    ///
    /// Assembles the SIDECAR blob list (format "3": the data object carries
    /// ONLY `docs`; every index blob lives in the `.vxi` sidecar). Empty
    /// dict/terms tables are omitted entirely; the reader treats a missing
    /// `dict`/`terms` pair as "no terms". Blob order clusters the small,
    /// hot blobs (`dict` block index, `bloom`) at the TAIL, next to the
    /// footer, so the eager tail fetch of a cold sidecar open covers them
    /// in one read. Readers locate blobs by tag, so order carries no
    /// meaning.
    #[allow(clippy::type_complexity)]
    fn assemble_index_blobs(
        &mut self,
        row_count: u64,
    ) -> Result<(Vec<(&'static str, &'static str, BlobPart)>, u64)> {
        let (index_blobs, term_count, bloom_acc) = match self.merged_index.take() {
            Some(prebuilt) => {
                if !self.terms.is_empty() {
                    return Err(VixError::Writer(
                        "internal: a merge-mode writer accumulated pushed terms".to_string(),
                    ));
                }
                if row_count != prebuilt.expected_rows {
                    return Err(VixError::Writer(format!(
                        "merge-mode writer stored {row_count} docs rows, but the merged index \
                         covers {} rows",
                        prebuilt.expected_rows
                    )));
                }
                (
                    prebuilt.blobs.map(IndexBlobParts::from),
                    prebuilt.term_count,
                    prebuilt.bloom,
                )
            }
            None => {
                // #52/M7 first-encode AUTO demotion: with the full term map
                // accumulated (and not spilled), per-field distinct counts
                // are exact — apply the shared AUTO rule BEFORE any blob or
                // bloom assembly, so a demoted-at-birth field produces the
                // identical sidecar a construction-list demotion would.
                self.auto_demote_bloom_only_at_finish(row_count);
                // Stream the globally sorted terms through the sink(s),
                // which cut postings row blocks by byte budget and
                // dictionary row groups by raw-term-byte budget. With spill
                // runs (a budget-crossing rebuild), the runs and the final
                // resident map k-way merge here in the same order — the
                // sink sees the identical term stream either way, so the
                // blobs are byte-identical to the unspilled path.
                //
                // M17 item 4: the UNSPILLED map (keys in memory — exact
                // split, no sampling risk) is range-partitioned at REAL key
                // quantiles snapped to FIELD boundaries and built by up to
                // `merge_kway_threads` workers, one TermSink per range —
                // postings encode + dict-block build + bloom hashing were
                // the serial dominator of a gen-1 rebuild once the docs
                // re-encode was gone. Field-boundary bounds keep the dict
                // region byte-identical (the sequential sink also cuts
                // blocks at every field change) and the re-cutting assembly
                // ([`write_index_blobs_recut`]) restores the terms blob's
                // continuous row-block accumulation, so the sidecar is
                // BYTE-IDENTICAL for any worker count (pinned R=1 vs R=8);
                // per-range bloom accumulators merge by set union (the M12
                // pattern, byte-identical SBBF).
                let bloom_pairs: Vec<(u16, String)> = self
                    .opts
                    .bloom_field_names
                    .iter()
                    .filter_map(|n| self.term_field_ids.get(n).map(|id| (*id, n.clone())))
                    // #52: bloom-only fields have no terms — tracking them
                    // per-field would publish an EMPTY (reject-all) filter
                    // and wrongly drop; the composite carries their values
                    .filter(|(id, _)| !self.bloom_only.contains_key(id))
                    .collect();
                let composite_pairs = self.composite_pairs();
                // #48: one reserved section covering (field name, value)
                // for every ELIGIBLE term field — any-field equality
                // pruning (see [`Self::composite_pairs`] for what
                // eligibility excludes and why). #52 bloom-only fields'
                // push-time hashes fold in AFTER the blob build (below).
                let postings_chunk_bytes = self.opts.postings_chunk_bytes;
                let plist_min_docs = self.opts.postings_plist_min_docs;
                let encode_threads = self.opts.encode_threads;
                let kway_threads = self.opts.merge_kway_threads;
                let make_acc = || {
                    let mut acc = crate::bloom::BloomHashAcc::from_pairs(bloom_pairs.clone());
                    if !composite_pairs.is_empty() {
                        acc.enable_composite(composite_pairs.iter().cloned());
                    }
                    acc
                };
                let make_sink = || {
                    TermSink::new(postings_chunk_bytes)
                        .with_bloom(make_acc())
                        .with_plist_min_docs(plist_min_docs)
                };
                // the `terms` blob's schema (mirrors TermSink::new's)
                let terms_blob_schema: SchemaRef = Arc::new(Schema::new(vec![
                    Field::new("doc_count", DataType::UInt32, false),
                    Field::new("postings", DataType::Binary, false),
                ]));
                let resident = std::mem::replace(
                    &mut self.terms,
                    TermAccumulator::new(self.term_fields.len()),
                );
                let (blobs, count, bloom) = match self.term_spill.take() {
                    None => {
                        let resident = resident.into_sorted_shards();
                        let threads = if encode_threads == 0 {
                            std::thread::available_parallelism().map_or(1, |n| n.get())
                        } else {
                            encode_threads
                        };
                        let kway = if kway_threads == 0 {
                            std::thread::available_parallelism()
                                .map_or(1, |n| n.get())
                                .min(8)
                        } else {
                            kway_threads
                        }
                        .min(threads);
                        // over-partition 4x and let workers pull ranges off
                        // a shared cursor (field-boundary snapping skews
                        // range sizes — same treatment as the k-way merge)
                        let ranges = if kway > 1 {
                            rebuild_partition_ranges(&resident, kway.saturating_mul(4))
                        } else {
                            Vec::new()
                        };
                        let (index, count, bloom) = if ranges.is_empty() {
                            // sequential: exactly the pre-M17 path
                            let mut sink = make_sink();
                            push_sorted_shards(&resident, &mut sink, row_count)?;
                            write_index_blobs(vec![sink.into_parts()?], encode_threads)?
                        } else {
                            let started = std::time::Instant::now();
                            let workers = kway.min(ranges.len());
                            let resident = Arc::new(resident);
                            let bloom_pairs = Arc::new(bloom_pairs.clone());
                            let composite_pairs = Arc::new(composite_pairs.clone());
                            let handle = crate::cpu_executor::shared_vortex_execution_handle()?;
                            let tasks: Vec<_> = ranges
                                .iter()
                                .cloned()
                                .map(|range| {
                                    let resident = Arc::clone(&resident);
                                    let bloom_pairs = Arc::clone(&bloom_pairs);
                                    let composite_pairs = Arc::clone(&composite_pairs);
                                    handle.spawn_cpu(move || {
                                        let mut acc = crate::bloom::BloomHashAcc::from_pairs(
                                            bloom_pairs.as_ref().clone(),
                                        );
                                        if !composite_pairs.is_empty() {
                                            acc.enable_composite(composite_pairs.iter().cloned());
                                        }
                                        let mut sink = TermSink::new(postings_chunk_bytes)
                                            .with_bloom(acc)
                                            .with_plist_min_docs(plist_min_docs);
                                        push_sorted_shards(&resident[range], &mut sink, row_count)?;
                                        sink.into_parts()
                                    })
                                })
                                .collect();
                            let results =
                                futures::executor::block_on(futures::future::join_all(tasks));
                            let mut parts = Vec::with_capacity(results.len());
                            let mut first_error = None;
                            for result in results {
                                match result {
                                    Ok(part) => parts.push(part),
                                    Err(error) => {
                                        first_error.get_or_insert(error);
                                    }
                                }
                            }
                            if let Some(error) = first_error {
                                return Err(error);
                            }
                            log::debug!(
                                "vix rebuild: shared-pool index-blob build ({} ranges, \
                                 per-merge target {workers}) in {:?}",
                                ranges.len(),
                                started.elapsed()
                            );
                            write_index_blobs_recut(parts, postings_chunk_bytes, encode_threads)?
                        };
                        (index.map(IndexBlobParts::from), count, bloom)
                    }
                    Some(mut spilled) => {
                        // spilled maps stream from disk in one key order —
                        // stays sequential (the in-memory partitioning above
                        // has no exact split points to offer).
                        //
                        // A budget-crossing rebuild's term stream is
                        // proportional to the group's TOTAL distinct-term
                        // vocabulary, so NOTHING vocabulary-sized may
                        // accumulate here: the sink's byte regions spool to
                        // unlinked temp files on the spill volume, closed
                        // term batches stream into an incremental vortex
                        // terms-blob writer (byte-identical to the one-shot
                        // write_vortex_blob for the same batch sequence —
                        // same pushes, same strategy), and the container
                        // assembly later streams the spooled blobs back
                        // (build_container_parts). Resident peak: the term
                        // map remnant + one open batch + the final
                        // container buffer, instead of ~3x the index size.
                        let spool_dir = self
                            .opts
                            .term_spill_dir
                            .clone()
                            .expect("a spilled map implies term_spill_dir");
                        // Drain the final resident shard set as one more run.
                        // This avoids materializing a second globally sorted
                        // resident representation during the k-way merge.
                        let mut resident = resident;
                        if !resident.is_empty() {
                            spilled.write_run(&mut resident)?;
                        }
                        let mut sink = make_sink().with_spool(&spool_dir)?;
                        let mut spooler =
                            TermsBlobSpooler::spawn(&spool_dir, Arc::clone(&terms_blob_schema))?;
                        let (runs, _spill_dir) = spilled.into_run_readers()?;
                        spill::merge_spilled_terms(runs, |key, ids| {
                            sink.push_ids(key, &ids, row_count)?;
                            for batch in sink.take_closed_batches() {
                                spooler.push(batch)?;
                            }
                            Ok(())
                        })?;
                        let parts = sink.into_spooled_parts()?;
                        for batch in parts.tail_batches {
                            spooler.push(batch)?;
                        }
                        if parts.term_count == 0 {
                            // unreachable in practice (a spill run implies
                            // terms), kept for parity with write_index_blobs
                            drop(spooler);
                            (None, 0, parts.bloom)
                        } else {
                            let terms = spooler.finish()?;
                            // single sink: offsets/ordinals are global as-is
                            let mut index = crate::dict_blocks::IndexBuilder::new();
                            for (first_key, offset, first_ordinal) in &parts.dict_meta {
                                index.push_block(first_key, *offset, *first_ordinal)?;
                            }
                            (
                                Some(IndexBlobParts {
                                    dict: index.finish(),
                                    dict_blocks: parts.dict_blocks,
                                    terms,
                                    plist: parts.plist,
                                }),
                                parts.term_count,
                                parts.bloom,
                            )
                        }
                    }
                };
                (blobs, count, bloom)
            }
        };

        let mut blobs: Vec<(&'static str, &'static str, BlobPart)> = Vec::new();
        if let Some(index) = index_blobs {
            blobs.push((BLOB_TYPE_TERMS, BLOB_TAG_TERMS, index.terms));
            // The out-of-row postings region: RAW concatenated
            // `encode_record` bytes (pointer-addressed, deliberately not a
            // Vortex file), present only when at least one pointer cell
            // exists.
            if let Some(plist) = index.plist {
                blobs.push((BLOB_TYPE_PLIST, BLOB_TAG_PLIST, plist));
            }
            blobs.push((
                BLOB_TYPE_DICT_BLOCKS,
                BLOB_TAG_DICT_BLOCKS,
                index.dict_blocks,
            ));
            blobs.push((BLOB_TYPE_DICT, BLOB_TAG_DICT, BlobPart::Mem(index.dict)));
        }
        // Per-file value blooms (byproduct of term emission, both paths).
        // #52: fold bloom-only values in for BOTH build and merge modes —
        // build mode observed them at push time; merge mode hashed them off
        // the streamed docs columns (the only source that exists when the
        // inputs were already bloom-only). Hashes drain; entries stay for
        // field_entries typing.
        let mut bloom_acc = bloom_acc;
        for (fid, (name, hashes)) in self.bloom_only.iter_mut() {
            bloom_acc.absorb_composite_hashes(*fid, name, std::mem::take(hashes));
        }
        let bloom_started = std::time::Instant::now();
        // M12: the composite section over a big merge holds tens of millions
        // of hashes — parallel bit-setting under the encode budget
        // (byte-identical to sequential for any thread count)
        let file_blooms = bloom_acc.build_threaded(self.opts.bloom_fpp, self.opts.encode_threads);
        if !file_blooms.is_empty() {
            let bloom_blob = crate::bloom::serialize_file_blooms(&file_blooms)?;
            log::debug!(
                "vix finish: SBBF bloom build {:?} ({} sections, {} bytes)",
                bloom_started.elapsed(),
                file_blooms.len(),
                bloom_blob.len()
            );
            blobs.push((BLOB_TYPE_BLOOM, BLOB_TAG_BLOOM, BlobPart::Mem(bloom_blob)));
        }
        Ok((blobs, term_count))
    }

    /// Assemble the `.vxi` INDEX-sidecar container. Emitted IFF the file is
    /// indexed by design: #40/#42 index-off builds produce NO sidecar
    /// (index_size = 0 — the file_list marker warmup and the bloom queue
    /// key on), while an indexed file ALWAYS gets one, even with an
    /// empty dictionary — its `fields` table still types the term plan,
    /// so dictionary-absence proofs stay valid (term_count == 0 proves
    /// absence on an indexed file; without a sidecar nothing does).
    fn build_index_sidecar_container(
        &self,
        row_count: u64,
        term_count: u64,
        blobs: Vec<(&'static str, &'static str, BlobPart)>,
    ) -> Result<Option<Vec<u8>>> {
        if !self.opts.index_enabled {
            debug_assert!(blobs.is_empty(), "an index-off writer produced index blobs");
            return Ok(None);
        }
        let mut index_properties = vec![
            (PROP_VERSION.to_string(), VIX_FORMAT_VERSION.to_string()),
            // Stamped on BOTH objects: readers verify the pair agrees,
            // catching a sidecar mispaired with a foreign data object
            // before its postings misaddress the stored rows.
            (PROP_ROW_COUNT.to_string(), row_count.to_string()),
            (PROP_TERM_COUNT.to_string(), term_count.to_string()),
            (
                PROP_FIELDS.to_string(),
                serde_json::to_string(&self.field_entries())?,
            ),
            (
                PROP_PARTIAL_FIELDS.to_string(),
                serde_json::to_string(&self.partial_fields)?,
            ),
            (PROP_TOKENIZER.to_string(), TOKENIZER_ID.to_string()),
            (PROP_DICT_LAYOUT.to_string(), DICT_LAYOUT_BLOCKS.to_string()),
            // Stamped unconditionally: readers hard-error on an absent
            // or foreign key_layout instead of silently misreading the
            // field-major dictionary
            // (container::require_supported_index_format).
            (PROP_KEY_LAYOUT.to_string(), KEY_LAYOUT_FID_V2.to_string()),
        ];
        // Plist capability marker: written IFF the feature was enabled.
        // Present ⇒ pointer cells may exist and `doc_count >= threshold`
        // selects them; absent ⇒ every postings cell is inline. Written
        // even when no term crossed the threshold (no `plist` blob then)
        // — capability, not blob presence, is what the reader
        // dispatches on.
        if self.opts.postings_plist_min_docs > 0 {
            index_properties.push((
                PROP_PLIST_MIN_DOCS.to_string(),
                self.opts.postings_plist_min_docs.to_string(),
            ));
        }
        Ok(Some(build_container_parts(index_properties, blobs)?))
    }

    /// Sidecar-only heal (M3, DESIGN-V2 §5): finish ONLY the `.vxi` index
    /// sidecar over rows fed through the index-only pushes, verifying the
    /// scan covered `expected_rows` EXACTLY — the row count of the
    /// UNTOUCHED data object, so the postings' doc ids address its stored
    /// rows by construction. No docs store is assembled and no data-object
    /// bytes exist: staging any docs row is a misuse and errors.
    ///
    /// Returns the sidecar container bytes plus stats (`index_size` = the
    /// new `FileMeta::index_size`; `docs_size`/`min_ts`/`max_ts` are 0 —
    /// the data object's row metadata is unchanged by a heal).
    pub fn finish_index_sidecar(
        mut self,
        expected_rows: u64,
    ) -> anyhow::Result<(Vec<u8>, VixWriterStats)> {
        if let Some(error) = &self.init_error {
            return Err(VixError::Writer(error.clone()).into());
        }
        if !self.opts.index_enabled {
            return Err(VixError::Writer(
                "finish_index_sidecar on an index-off writer (an index-off plan has no sidecar \
                 to rebuild)"
                    .to_string(),
            )
            .into());
        }
        if self.merged_index.is_some() {
            return Err(VixError::Writer(
                "finish_index_sidecar on a merge-mode writer".to_string(),
            )
            .into());
        }
        if self.row_count > 0 || self.encoded_run.is_some() {
            return Err(VixError::Writer(format!(
                "finish_index_sidecar with {} staged docs rows — the sidecar-only heal never \
                 rewrites the data object",
                self.row_count
            ))
            .into());
        }
        let indexed = self.index_only_rows.unwrap_or(0);
        if indexed != expected_rows {
            return Err(VixError::Writer(format!(
                "index-only scan covered {indexed} rows but the data object stores \
                 {expected_rows} — the postings' doc ids would misaddress the stored rows; \
                 refusing to finish"
            ))
            .into());
        }
        let (blobs, term_count) = self.assemble_index_blobs(expected_rows)?;
        let container = self
            .build_index_sidecar_container(expected_rows, term_count, blobs)?
            .expect("index-enabled writers always produce a sidecar");
        let oversize_skipped: u64 = self.oversize_skips.values().sum();
        let stats = VixWriterStats {
            row_count: expected_rows,
            term_count,
            index_size: container.len() as u64,
            docs_size: 0,
            oversize_skipped,
            min_ts: 0,
            max_ts: 0,
        };
        Ok((container, stats))
    }

    /// Per-field oversize-skip counts accumulated by this writer's pushes
    /// (what would become the `oversize_skips` DATA-object property). The
    /// sidecar-only heal compares this against the stored file's existing
    /// allowance: a NEWLY skipped field cannot be recorded on the untouched
    /// data object, so such a heal must fall back to the docs rewrite.
    pub fn oversize_skips(&self) -> &BTreeMap<String, u64> {
        &self.oversize_skips
    }

    /// #48: the composite-bloom coverage set — `(field id, name)` for every
    /// term field whose dictionary holds its COMPLETE raw values. Empty when
    /// the option is off. Exclusions are correctness, not tuning: the
    /// composite's guard keys tell the pruner "a value miss on this field is
    /// authoritative", so
    ///
    /// - fts fields are out (their dictionary entries are TOKENS — probing a raw value against
    ///   tokens would read "definitely not" for values the file holds), and
    /// - merge-demoted fields are out (some input carried rows without term capability, so the
    ///   merged dictionary's values are incomplete).
    ///
    /// The reader-side equivalent ([`crate::VixReader::term_fields`]) is
    /// already this set — its map only holds `term`-typed entries.
    fn composite_pairs(&self) -> Vec<(u16, String)> {
        if !self.opts.bloom_composite {
            return Vec::new();
        }
        self.term_field_ids
            .iter()
            .filter(|(name, _)| {
                !self.fts_fields.contains(*name)
                    && !self.demoted_fields.contains(*name)
                    // non-string value terms are canonical-tagged bytes the
                    // pruner's raw-literal probe can never match — coverage
                    // would turn every such miss into a wrong drop
                    && !self.non_string_term_fields.contains(*name)
            })
            .map(|(name, id)| (*id, name.clone()))
            .collect()
    }

    /// The `fields` property: value-indexed fields first (array index ==
    /// field id), then stored-only entries (`_timestamp`, non-term
    /// column-store fields) appended after them. Key terms and the
    /// `_source`/`_original` columns get no entries.
    ///
    /// An fts field's entry is `types:["fts"]` — **without** `"term"`, since
    /// its raw whole values are not indexed (readers must skip per-field
    /// value lookups on it and keep the scan-side filter). `"term"` and
    /// `"fts"` are mutually exclusive. A merge-DEMOTED field keeps its
    /// positional entry (its id may still suffix orphaned dictionary terms)
    /// but claims no lookup capability — an empty `types` list unless it is
    /// also column-stored.
    fn field_entries(&self) -> Vec<FieldEntry> {
        let mut entries: Vec<FieldEntry> = self
            .term_fields
            .iter()
            .map(|name| {
                let mut types = if self.fts_fields.contains(name) {
                    vec![FIELD_TYPE_FTS.to_string()]
                } else if self.demoted_fields.contains(name) {
                    Vec::new()
                } else if self
                    .term_field_ids
                    .get(name)
                    .is_some_and(|id| self.bloom_only.contains_key(id))
                {
                    // #52: values in the composite bloom + docs columns only
                    vec![FIELD_TYPE_BLOOM.to_string()]
                } else {
                    vec![FIELD_TYPE_TERM.to_string()]
                };
                if self.cs_fields.contains(name) {
                    types.push(FIELD_TYPE_CS.to_string());
                }
                FieldEntry {
                    name: name.clone(),
                    types,
                }
            })
            .collect();
        for field in self.docs_schema.fields() {
            let name = field.name();
            if self.term_field_ids.contains_key(name)
                || name == SOURCE_COL_NAME
                || name == ORIGINAL_DATA_COL_NAME
            {
                continue;
            }
            entries.push(FieldEntry {
                name: name.clone(),
                types: vec![FIELD_TYPE_CS.to_string()],
            });
        }
        entries
    }
}

/// Streaming builder of the `_timestamp` zone table — one `(row_count,
/// ts_min, ts_max)` entry per `rows_per_chunk`-sized window of the stored
/// rows, in push order — PLUS the per-column chunk-stats table (H2, DESIGN
/// §4), folded over the SAME windows so stats entry `i` covers exactly zone
/// entry `i`'s rows. Folded as the batches stream to the encoder — no blob
/// read-back, no batch retention.
///
/// The reader never needs the entries to match the *projected* `_timestamp`
/// read's physical chunks (that read coalesces to ~1 MiB ≈ many blocks): the
/// fast paths only require each entry to bound its own contiguous row range
/// and the entries to cover every row, and they decode a residual chunk's
/// rows by row-index point read. `rows_per_chunk == 0` (an empty file) folds
/// nothing and yields no entries.
struct ZoneMapFolder {
    rows_per_chunk: usize,
    entries: Vec<ZoneEntry>,
    count: u64,
    ts_min: i64,
    ts_max: i64,
    stats: ColumnStatsFolder,
    /// §4 REGION-table tracker (tracked only for concat outputs): row counts
    /// of the file's maximal internally-`_timestamp`-DESC runs, in stored
    /// order. Decoded rows extend/split runs by VALUE (an increase vs the
    /// previous stored row starts a new region — exact, whatever order the
    /// caller pushed); spliced encoded runs append the caller-declared
    /// decomposition. `None` = poisoned (an input without a proven
    /// decomposition, or the cap): the property is omitted, fail-open.
    regions: Option<Vec<u64>>,
    /// Rows in the currently open decoded run.
    region_rows: u64,
    /// `_timestamp` of the last stored row of the open decoded run.
    region_last_ts: Option<i64>,
    track_regions: bool,
}

/// Writer-side cap on tracked regions: a decomposition wider than this is
/// dropped (the property is omitted). Bounds the property size and the
/// pathological fully-unsorted case (every row its own region).
const WRITER_REGION_CAP: usize = 4096;

impl ZoneMapFolder {
    fn new(
        rows_per_chunk: usize,
        docs_schema: &Schema,
        stats_min_density: f64,
        stats_max_bytes: usize,
        track_regions: bool,
    ) -> Self {
        Self {
            rows_per_chunk,
            entries: Vec::new(),
            count: 0,
            ts_min: i64::MAX,
            ts_max: i64::MIN,
            stats: ColumnStatsFolder::new(
                docs_schema,
                &[TIMESTAMP_COL_NAME, SOURCE_COL_NAME, ORIGINAL_DATA_COL_NAME],
                stats_min_density,
                stats_max_bytes,
            ),
            regions: track_regions.then(Vec::new),
            region_rows: 0,
            region_last_ts: None,
            track_regions,
        }
    }

    /// Close the open decoded desc run into one region entry.
    fn close_open_region(&mut self) {
        if self.region_rows == 0 {
            return;
        }
        if let Some(regions) = self.regions.as_mut() {
            if regions.len() >= WRITER_REGION_CAP {
                self.regions = None; // poisoned: too fragmented to describe
            } else {
                regions.push(self.region_rows);
            }
        }
        self.region_rows = 0;
        self.region_last_ts = None;
    }

    fn fold(&mut self, batch: &RecordBatch) -> Result<()> {
        if self.rows_per_chunk == 0 || batch.num_rows() == 0 {
            return Ok(());
        }
        let column = batch.column_by_name(TIMESTAMP_COL_NAME).ok_or_else(|| {
            VixError::Writer(format!("internal: docs batch lacks {TIMESTAMP_COL_NAME}"))
        })?;
        let values = column
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                VixError::Writer(format!(
                    "internal: {TIMESTAMP_COL_NAME} is not an i64 column"
                ))
            })?;
        if values.null_count() > 0 {
            return Err(VixError::Writer(
                "internal: _timestamp has null rows; cannot bound its zone".to_string(),
            ));
        }
        // window-sliced folding: the zone bounds and the per-column stats
        // advance over identical row windows
        let rows = batch.num_rows();
        let mut offset = 0usize;
        while offset < rows {
            let space = self.rows_per_chunk - self.count as usize;
            let take = space.min(rows - offset);
            for &value in &values.values()[offset..offset + take] {
                self.ts_min = self.ts_min.min(value);
                self.ts_max = self.ts_max.max(value);
                // §4 region tracking: an INCREASE vs the previous stored row
                // ends the open desc run (equal timestamps continue it)
                if self.track_regions && self.regions.is_some() {
                    if let Some(last) = self.region_last_ts
                        && value > last
                    {
                        self.close_open_region();
                    }
                    self.region_rows += 1;
                    self.region_last_ts = Some(value);
                }
            }
            self.stats.fold_window(batch, offset, take);
            self.count += take as u64;
            offset += take;
            if self.count as usize == self.rows_per_chunk {
                self.close_window();
            }
        }
        Ok(())
    }

    /// Close the open window into one zone entry + one stats row per
    /// column (a short entry is valid — readers only require each entry to
    /// bound its own contiguous row range and the entries to cover every
    /// row in order).
    fn close_window(&mut self) {
        if self.count == 0 {
            return;
        }
        self.entries.push((self.count, self.ts_min, self.ts_max));
        self.stats.close_window(self.count);
        self.count = 0;
        self.ts_min = i64::MAX;
        self.ts_max = i64::MIN;
    }

    /// #51c: close the open window early so entries spliced from a
    /// passthrough input can follow in row order.
    fn flush_open_window(&mut self) {
        self.close_window();
    }

    /// #51c: splice a passthrough input's zone entries VERBATIM — the rows
    /// they describe are appended next, in the same order, so each entry
    /// keeps bounding its own contiguous row range — together with the
    /// input's per-column chunk stats (aligned 1:1 with the entries; a
    /// column the input lacks contributes zero-presence rows, a stats-less
    /// column contributes UNKNOWN rows). The caller flushes the open window
    /// first and has validated the entries against the run.
    ///
    /// `run_regions` (§4): the run's proven internally-DESC decomposition
    /// (row counts, validated by the caller to sum to the run) — appended
    /// to the region table; `None` = no proven decomposition, which POISONS
    /// the table (the property is omitted; readers fail open to the full
    /// sort). A spliced run never merges into the preceding region — the
    /// tracker cannot prove cross-boundary order without decoding.
    fn append_spliced(
        &mut self,
        entries: &[ZoneEntry],
        spliced: &SpliceableStats,
        run_regions: Option<&[u64]>,
    ) {
        debug_assert_eq!(self.count, 0, "flush_open_window before splicing");
        if self.track_regions {
            self.close_open_region();
            match (run_regions, self.regions.as_mut()) {
                (Some(runs), Some(regions)) => {
                    if regions.len() + runs.len() > WRITER_REGION_CAP {
                        self.regions = None;
                    } else {
                        regions.extend_from_slice(runs);
                    }
                }
                (None, Some(_)) => self.regions = None,
                _ => {}
            }
        }
        let rows: u64 = entries.iter().map(|(rows, ..)| rows).sum();
        self.entries.extend_from_slice(entries);
        self.stats.append_spliced(entries.len(), rows, spliced);
    }

    /// Close out: the zone table, the per-column file presence counts, the
    /// serialized `stats` blob (`None` when no column qualified) and the §4
    /// region table (`None` unless tracked and proven).
    #[allow(clippy::type_complexity)]
    fn finish(
        mut self,
    ) -> (
        Vec<ZoneEntry>,
        Vec<(String, Option<u64>)>,
        Option<Vec<u8>>,
        Option<Vec<u64>>,
    ) {
        self.close_window();
        self.close_open_region();
        let (presence, blob) = self.stats.finish();
        (self.entries, presence, blob, self.regions.take())
    }
}

/// #51c: `None` when two arrow docs schemas describe the SAME stored docs
/// blob shape — names, order, nullability and STORED types all equal;
/// otherwise the first difference, named. The comparison happens at the
/// vortex dtype level because the stored blob erases arrow representation
/// choices (`Utf8`, `LargeUtf8` and `Utf8View` all store as vortex `Utf8`,
/// and a file read back reports the VIEW form): two files whose docs dtypes
/// are equal hold interchangeable encoded chunks, which is exactly the
/// passthrough requirement.
pub fn docs_schema_mismatch_reason(input: &Schema, output: &Schema) -> Option<String> {
    use vortex::{arrow::FromArrowType, dtype::DType};
    if DType::from_arrow(input) == DType::from_arrow(output) {
        return None;
    }
    if input.fields().len() != output.fields().len() {
        return Some(format!(
            "docs schema has {} columns, the output stores {} (passthrough requires exact \
             schema identity)",
            input.fields().len(),
            output.fields().len()
        ));
    }
    for (theirs, ours) in input.fields().iter().zip(output.fields()) {
        if theirs.name() != ours.name()
            || DType::from_arrow(theirs.as_ref()) != DType::from_arrow(ours.as_ref())
        {
            return Some(format!(
                "docs column {:?} ({}, nullable {}) does not match the output column {:?} \
                 ({}, nullable {})",
                theirs.name(),
                theirs.data_type(),
                theirs.is_nullable(),
                ours.name(),
                ours.data_type(),
                ours.is_nullable()
            ));
        }
    }
    Some("docs schemas store different dtypes".to_string())
}

/// Rows per `docs`-blob chunk: the uncompressed-byte budget divided by the
/// average row's PRESENT-VALUE bytes, clamped to `[64, max_rows]` (the
/// [`VixWriterOptions::docs_chunk_max_rows`] ceiling; `0` = the historical
/// 65,536). Computed over the sample batches ([`DOCS_ENCODE_SAMPLE_BYTES`])
/// that lock the streaming encoder's chunking.
///
/// H1 (DESIGN §3): the row weight is the sum of the row's NON-NULL value
/// byte lengths plus [`PRESENT_VALUE_OVERHEAD_BYTES`] per present value —
/// never whole-row arrow width. A null slot costs nothing: sparse column
/// data is null-suppressed at encode time, so present bytes are the honest
/// proxy for decoded chunk weight, and a 1,500-column mostly-null schema
/// sizes exactly like a narrow schema carrying the same values (the
/// historical failure: 2,557 nullable Utf8 columns ≈ 10.5 KiB/row of arrow
/// padding even all-null collapsed rows-per-chunk and multiplied every
/// per-chunk stats table by the shrunken chunk size).
///
/// The blob's chunks are the decompression unit of a matched-row point
/// read, so they follow this byte budget instead of the data file's
/// row-group row count (with ~KB `_source` rows, a 128Ki-row chunk would
/// make every point read decode hundreds of MB). The floor is 64 rows so
/// the budget governs wide rows too (a 1024-row floor used to force ~4 MiB
/// decodes for ~4 KiB rows regardless of a smaller budget); vortex's own
/// pipeline still coalesces sub-1 MiB chunks up to ~1 MiB (multiples of
/// this row count), which bounds the effective decode unit from below. An
/// empty file (schema-only blob) keeps vortex's default chunking.
pub(crate) fn docs_rows_per_chunk(
    budget_bytes: usize,
    max_rows: usize,
    batches: &[RecordBatch],
) -> usize {
    let budget_bytes = if budget_bytes == 0 {
        DEFAULT_DOCS_CHUNK_BYTES
    } else {
        budget_bytes
    };
    // the ceiling never sinks below the floor — clamp() panics on an
    // inverted range, and a sub-64-row cap was never a meaningful chunk
    let max_rows = if max_rows == 0 {
        DEFAULT_DOCS_CHUNK_MAX_ROWS
    } else {
        max_rows.max(DOCS_CHUNK_MIN_ROWS)
    };
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    if rows == 0 {
        return 0;
    }
    let bytes: usize = batches.iter().map(batch_present_value_bytes).sum();
    let avg_row_bytes = (bytes / rows).max(1);
    (budget_bytes / avg_row_bytes).clamp(DOCS_CHUNK_MIN_ROWS, max_rows)
}

/// Byte weight charged per PRESENT (non-null) value on top of its raw value
/// bytes: a stand-in for per-value offset/validity/metadata overhead in the
/// decoded form. Small on purpose — it must never dominate real value bytes,
/// only keep many-tiny-values rows from weighing zero.
const PRESENT_VALUE_OVERHEAD_BYTES: usize = 4;

/// H1 present-value byte accounting of one docs batch: for every column,
/// the sum of the NON-NULL values' byte lengths plus
/// [`PRESENT_VALUE_OVERHEAD_BYTES`] per present value. Null slots cost
/// nothing — arrow's per-slot padding (offsets, validity words) must never
/// count, or wide sparse schemas collapse rows-per-chunk (the H1 failure).
pub(crate) fn batch_present_value_bytes(batch: &RecordBatch) -> usize {
    batch
        .columns()
        .iter()
        .map(|column| column_present_value_bytes(column.as_ref()))
        .sum()
}

fn column_present_value_bytes(column: &dyn Array) -> usize {
    let present = column.len() - column.null_count();
    if present == 0 {
        return 0;
    }
    // Variable-length value bytes come from the offsets span (O(1); null
    // slots have zero-length spans in every builder-produced array) or the
    // view lengths; fixed-width types pay width × present.
    let value_bytes = match column.data_type() {
        DataType::Utf8 => {
            let array = column
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Utf8 downcast");
            offsets_span(array.value_offsets())
        }
        DataType::LargeUtf8 => {
            let array = column
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .expect("LargeUtf8 downcast");
            offsets_span(array.value_offsets())
        }
        DataType::Binary => {
            let array = column
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("Binary downcast");
            offsets_span(array.value_offsets())
        }
        DataType::LargeBinary => {
            let array = column
                .as_any()
                .downcast_ref::<arrow::array::LargeBinaryArray>()
                .expect("LargeBinary downcast");
            offsets_span(array.value_offsets())
        }
        DataType::Utf8View => {
            let array = column
                .as_any()
                .downcast_ref::<StringViewArray>()
                .expect("Utf8View downcast");
            view_value_bytes(array.views(), column)
        }
        DataType::BinaryView => {
            let array = column
                .as_any()
                .downcast_ref::<arrow::array::BinaryViewArray>()
                .expect("BinaryView downcast");
            view_value_bytes(array.views(), column)
        }
        DataType::Boolean => present,
        data_type => match data_type.primitive_width() {
            Some(width) => present * width,
            // exotic types (nested, dictionary, ...): fall back to the
            // arrow footprint — better too heavy than zero
            None => column.get_array_memory_size(),
        },
    };
    value_bytes + present * PRESENT_VALUE_OVERHEAD_BYTES
}

/// Total value bytes of an offsets-encoded array: the span between the
/// first and last offset. Null slots contribute their (normally empty)
/// spans — an O(1) heuristic, not an exact per-valid-row sum.
fn offsets_span<O: arrow::array::OffsetSizeTrait>(offsets: &[O]) -> usize {
    match (offsets.first(), offsets.last()) {
        (Some(first), Some(last)) => (*last - *first).as_usize(),
        _ => 0,
    }
}

/// Total value bytes of a view array: the sum of the VALID views' lengths
/// (low 32 bits of each raw view) — nulls excluded, one pass over a
/// primitive buffer.
fn view_value_bytes(views: &[u128], column: &dyn Array) -> usize {
    if column.null_count() == 0 {
        return views.iter().map(|view| (*view as u32) as usize).sum();
    }
    views
        .iter()
        .enumerate()
        .filter(|(row, _)| column.is_valid(*row))
        .map(|(_, view)| (*view as u32) as usize)
        .sum()
}

/// For float-typed columns, the per-row "emits a key term" mask: valid AND
/// finite (NaN/±Inf are treated as null — see
/// [`VixWriter::index_key_terms`]). `None` for every other type: plain
/// validity applies.
fn finite_float_mask(column: &dyn Array) -> Option<Vec<bool>> {
    use arrow::array::{Float16Array, Float32Array, Float64Array};
    match column.data_type() {
        DataType::Float16 => {
            let array = column.as_any().downcast_ref::<Float16Array>()?;
            Some(
                (0..array.len())
                    .map(|row| array.is_valid(row) && array.value(row).to_f32().is_finite())
                    .collect(),
            )
        }
        DataType::Float32 => {
            let array = column.as_any().downcast_ref::<Float32Array>()?;
            Some(
                (0..array.len())
                    .map(|row| array.is_valid(row) && array.value(row).is_finite())
                    .collect(),
            )
        }
        DataType::Float64 => {
            let array = column.as_any().downcast_ref::<Float64Array>()?;
            Some(
                (0..array.len())
                    .map(|row| array.is_valid(row) && array.value(row).is_finite())
                    .collect(),
            )
        }
        _ => None,
    }
}

/// Append `doc` to the postings of `key`, deduping consecutive pushes of the
/// same doc (raw term == token, or the same token twice in one value).
/// Emit one tagged canonical numeric/bool value term: the token is
/// `\x01{canonical text}` (see [`crate::numeric`] for why the tag exists).
fn push_numeric_term(
    terms: &mut TermAccumulator,
    tag_scratch: &mut Vec<u8>,
    canonical: &str,
    field_id: u16,
    doc: u32,
) {
    tag_scratch.clear();
    tag_scratch.reserve(canonical.len() + 1);
    tag_scratch.push(NUMERIC_TERM_TAG);
    tag_scratch.extend_from_slice(canonical.as_bytes());
    terms.push(field_id, tag_scratch, doc);
}

/// Fetch the batch column backing `field`, casting where needed
/// (e.g. a timestamp-typed `_timestamp` to `i64`).
fn batch_column_as(batch: &RecordBatch, field: &Field) -> Result<ArrowArrayRef> {
    let column = batch.column_by_name(field.name()).ok_or_else(|| {
        VixError::Writer(format!(
            "batch is missing column {:?} required by the document/column store",
            field.name()
        ))
    })?;
    array_cast_as(column, field)
}

/// Cast `column` to the arrow type of `field` (no-op when it already
/// matches).
fn array_cast_as(column: &ArrowArrayRef, field: &Field) -> Result<ArrowArrayRef> {
    if column.data_type() == field.data_type() {
        Ok(Arc::clone(column))
    } else {
        cast(column, field.data_type()).map_err(|e| {
            VixError::Writer(format!(
                "column {:?} cannot be stored as {:?}: {e}",
                field.name(),
                field.data_type()
            ))
        })
    }
}

fn flush_terms_batch(
    schema: &SchemaRef,
    doc_counts: &mut Vec<u32>,
    postings_builder: &mut BinaryBuilder,
    out: &mut Vec<RecordBatch>,
) -> Result<()> {
    if doc_counts.is_empty() {
        return Ok(());
    }
    let doc_counts = UInt32Array::from(std::mem::take(doc_counts));
    let postings = postings_builder.finish();
    out.push(RecordBatch::try_new(
        Arc::clone(schema),
        vec![Arc::new(doc_counts), Arc::new(postings)],
    )?);
    Ok(())
}

/// Byte sink of one sink-produced blob region (`dict_blocks` / `plist`): in
/// memory — the historical path, still used by every unspilled build — or
/// written through to an UNLINKED temp file on the spill volume. A spooled
/// region produces byte-identical blob bytes; only their residence differs.
/// Spooling matters because these regions are proportional to the group's
/// TOTAL distinct-term vocabulary (dict_blocks ≈ every distinct key's
/// bytes), which for a budget-crossing rebuild is unbounded by the term
/// map's spill budget.
pub(crate) enum RegionSink {
    Mem(Vec<u8>),
    Spooled {
        writer: std::io::BufWriter<std::fs::File>,
        len: u64,
    },
}

impl RegionSink {
    fn new_spooled(dir: &std::path::Path) -> Result<Self> {
        // unlinked temp file: freed by the OS on drop/crash, nothing to sweep
        let file = tempfile::tempfile_in(dir)
            .map_err(|e| VixError::Writer(format!("create blob spool in {dir:?}: {e}")))?;
        Ok(RegionSink::Spooled {
            writer: std::io::BufWriter::with_capacity(1024 * 1024, file),
            len: 0,
        })
    }

    fn len(&self) -> u64 {
        match self {
            RegionSink::Mem(data) => data.len() as u64,
            RegionSink::Spooled { len, .. } => *len,
        }
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn write_all(&mut self, bytes: &[u8]) -> Result<()> {
        match self {
            RegionSink::Mem(data) => {
                data.extend_from_slice(bytes);
                Ok(())
            }
            RegionSink::Spooled { writer, len } => {
                use std::io::Write;
                writer
                    .write_all(bytes)
                    .map_err(|e| VixError::Writer(format!("write blob spool: {e}")))?;
                *len += bytes.len() as u64;
                Ok(())
            }
        }
    }

    /// Close the region into a container-ready [`BlobPart`] (spooled files
    /// rewind to the payload start).
    fn into_part(self) -> Result<BlobPart> {
        match self {
            RegionSink::Mem(data) => Ok(BlobPart::Mem(data)),
            RegionSink::Spooled { writer, len } => {
                use std::io::{Seek, Write};
                let mut writer = writer;
                writer
                    .flush()
                    .map_err(|e| VixError::Writer(format!("flush blob spool: {e}")))?;
                let mut file = writer
                    .into_inner()
                    .map_err(|e| VixError::Writer(format!("close blob spool: {e}")))?;
                file.seek(std::io::SeekFrom::Start(0))
                    .map_err(|e| VixError::Writer(format!("rewind blob spool: {e}")))?;
                Ok(BlobPart::Spooled { file, len })
            }
        }
    }
}

/// Streaming encoder of the `dict`/`terms` blobs: consumes one
/// `(composite key, doc_count, final postings blob)` triple per term, in
/// strictly ascending key order, cutting postings row blocks by byte budget
/// and dictionary row groups by raw-term-byte budget. Shared by the push
/// path (terms accumulated in the writer's map) and the compaction index
/// merge (terms streamed off the inputs' dictionaries), so both produce
/// identical encodings.
pub(crate) struct TermSink {
    postings_chunk_bytes: usize,
    terms_schema: SchemaRef,
    term_batches: Vec<RecordBatch>,
    doc_counts: Vec<u32>,
    postings_builder: BinaryBuilder,
    block_bytes: usize,
    /// The open dictionary block (see [`crate::dict_blocks`]): keys cut at
    /// [`crate::dict_blocks::BLOCK_TARGET_BYTES`] of raw key bytes and at
    /// every field boundary, so one block never spans fields and a field
    /// probe touches exactly its own blocks.
    dict_block: crate::dict_blocks::BlockBuilder,
    dict_block_first_key: Vec<u8>,
    dict_block_first_ordinal: u64,
    /// This sink's concatenated encoded blocks (offsets sink-local;
    /// [`write_index_blobs`] rebases on concatenation).
    dict_blocks: RegionSink,
    /// `(first_key, sink-local blocks offset, sink-local first ordinal)`
    /// per flushed block, in key order.
    dict_meta: Vec<(Vec<u8>, u64, u64)>,
    /// First/last key pushed through this sink (parallel-merge range
    /// ordering backstop in [`write_index_blobs`]).
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    term_count: u64,
    /// Per-file value-bloom accumulation (empty = zero-cost no-op). Both
    /// build paths stream every distinct term through [`Self::push`], so
    /// this is the single bloom hook for normal builds AND merges. Bloom
    /// observation is PINNED to the v1 byte form (group `.bf` continuity),
    /// so keys are converted before hashing.
    bloom: crate::bloom::BloomHashAcc,
    bloom_key_scratch: Vec<u8>,
    /// See [`VixWriterOptions::postings_plist_min_docs`]; `0` = every cell
    /// stays inline (the historical encoding, byte-identical).
    plist_min_docs: u32,
    /// This sink's out-of-row postings region: concatenated
    /// [`postings::encode_record`] bytes, addressed by the pointer cells
    /// pushed through [`Self::push_plist`]. Offsets are SINK-LOCAL —
    /// [`write_index_blobs`] rebases them when it concatenates multiple
    /// sinks' regions into the single `plist` blob.
    plist: RegionSink,
    /// Reused across terms; avoids two heap allocations per postings list.
    postings_encode_scratch: Vec<u8>,
    postings_record_scratch: Vec<u8>,
}

impl TermSink {
    pub(crate) fn new(postings_chunk_bytes: usize) -> Self {
        let terms_schema = Arc::new(Schema::new(vec![
            Field::new("doc_count", DataType::UInt32, false),
            Field::new("postings", DataType::Binary, false),
        ]));
        Self {
            postings_chunk_bytes,
            terms_schema,
            term_batches: Vec::new(),
            doc_counts: Vec::new(),
            postings_builder: BinaryBuilder::new(),
            block_bytes: 0,
            dict_block: crate::dict_blocks::BlockBuilder::new(),
            dict_block_first_key: Vec::new(),
            dict_block_first_ordinal: 0,
            dict_blocks: RegionSink::Mem(Vec::new()),
            dict_meta: Vec::new(),
            first_key: Vec::new(),
            last_key: Vec::new(),
            term_count: 0,
            bloom: crate::bloom::BloomHashAcc::default(),
            bloom_key_scratch: Vec::new(),
            plist_min_docs: 0,
            plist: RegionSink::Mem(Vec::new()),
            postings_encode_scratch: Vec::new(),
            postings_record_scratch: Vec::new(),
        }
    }

    /// Spool this sink's byte regions (`dict_blocks`, `plist`) to unlinked
    /// temp files under `dir` instead of accumulating them in memory —
    /// byte-identical blob bytes, bounded residence. For the spilled-rebuild
    /// finish, whose regions are vocabulary-proportional. Must be called
    /// before the first push.
    pub(crate) fn with_spool(mut self, dir: &std::path::Path) -> Result<Self> {
        debug_assert!(self.term_count == 0, "with_spool after pushes");
        self.dict_blocks = RegionSink::new_spooled(dir)?;
        self.plist = RegionSink::new_spooled(dir)?;
        Ok(self)
    }

    /// Close the open dictionary block into the sink's blocks region.
    fn flush_dict_block(&mut self) -> Result<()> {
        if self.dict_block.is_empty() {
            return Ok(());
        }
        let offset = self.dict_blocks.len();
        let bytes = self.dict_block.finish();
        self.dict_blocks.write_all(&bytes)?;
        self.dict_meta.push((
            std::mem::take(&mut self.dict_block_first_key),
            offset,
            self.dict_block_first_ordinal,
        ));
        Ok(())
    }

    pub(crate) fn with_bloom(mut self, bloom: crate::bloom::BloomHashAcc) -> Self {
        self.bloom = bloom;
        self
    }

    /// #52: route a bloom-only field's dictionary key (FIELD-MAJOR v2 form)
    /// into the bloom accumulation WITHOUT it reaching the dictionary/
    /// postings — how legacy indexed inputs' values survive a merge into a
    /// bloom-only output.
    pub(crate) fn observe_bloom_only_key(&mut self, key: &[u8]) {
        self.bloom.observe_dict_key(key);
    }

    /// Enable out-of-row postings at/above `min_docs` docs (see
    /// [`VixWriterOptions::postings_plist_min_docs`]); `0` keeps every cell
    /// inline.
    pub(crate) fn with_plist_min_docs(mut self, min_docs: u32) -> Self {
        self.plist_min_docs = min_docs;
        self
    }

    /// Whether a NON-dense term with `doc_count` docs goes out-of-row.
    /// Callers must check dense elision FIRST — a term in every row keeps
    /// the empty cell regardless of the threshold.
    pub(crate) fn plist_eligible(&self, doc_count: u64) -> bool {
        self.plist_min_docs > 0 && doc_count >= u64::from(self.plist_min_docs)
    }

    /// Push one term whose postings live OUT-OF-ROW: `record` (the
    /// [`postings::encode_record`] bytes) is appended to this sink's plist
    /// region and the terms cell becomes the 12-byte pointer to it. Only
    /// for terms passing [`Self::plist_eligible`], and never for
    /// dense-elided terms (both are the caller's contract; the reader
    /// re-derives the same decisions from `doc_count` alone).
    pub(crate) fn push_plist(&mut self, key: &[u8], doc_count: u32, record: &[u8]) -> Result<()> {
        debug_assert!(self.plist_eligible(u64::from(doc_count)));
        let len = u32::try_from(record.len()).map_err(|_| {
            VixError::Writer(format!(
                "plist record of {} bytes overflows the pointer cell's u32 length",
                record.len()
            ))
        })?;
        let cell = postings::encode_pointer_cell(self.plist.len(), len);
        self.plist.write_all(record)?;
        self.push(key, doc_count, &cell)
    }

    /// Encode and push one term straight from its sorted doc ids, applying
    /// the cell policy in precedence order:
    ///
    /// 1. **dense elision** — a term present in every row (`ids.len() == row_count`) keeps the
    ///    EMPTY cell regardless of the plist threshold (the reader synthesizes the all-ones bitmap
    ///    from `doc_count` alone),
    /// 2. **out-of-row** — at/above [`Self::plist_eligible`]'s threshold the
    ///    [`postings::encode_record`] bytes go to this sink's plist region and the cell is the
    ///    12-byte pointer,
    /// 3. **inline** — everything else stays the plain [`postings::encode`] blob, byte-identical to
    ///    the pre-plist encoding.
    pub(crate) fn push_ids(&mut self, key: &[u8], ids: &[u32], row_count: u64) -> Result<()> {
        if row_count > 0 && ids.len() as u64 == row_count {
            return self.push(key, ids.len() as u32, &[]);
        }
        if self.plist_eligible(ids.len() as u64) {
            let mut record = std::mem::take(&mut self.postings_record_scratch);
            let mut encoded = std::mem::take(&mut self.postings_encode_scratch);
            let encode_result = postings::encode_record_into(ids, &mut record, &mut encoded);
            if let Err(error) = encode_result {
                self.postings_record_scratch = record;
                self.postings_encode_scratch = encoded;
                return Err(error);
            }
            let result = self.push_plist(key, ids.len() as u32, &record);
            self.postings_record_scratch = record;
            self.postings_encode_scratch = encoded;
            return result;
        }
        let mut encoded = std::mem::take(&mut self.postings_encode_scratch);
        let encode_result = postings::encode_into(ids, &mut encoded);
        if let Err(error) = encode_result {
            self.postings_encode_scratch = encoded;
            return Err(error);
        }
        let result = self.push(key, ids.len() as u32, &encoded);
        self.postings_encode_scratch = encoded;
        result
    }

    pub(crate) fn push(&mut self, key: &[u8], doc_count: u32, blob: &[u8]) -> Result<()> {
        self.bloom.observe(crate::query::bloom_canonical_key(
            key,
            &mut self.bloom_key_scratch,
        ));
        self.doc_counts.push(doc_count);
        self.block_bytes += blob.len();
        self.postings_builder.append_value(blob);
        if self.block_bytes >= self.postings_chunk_bytes {
            flush_terms_batch(
                &self.terms_schema,
                &mut self.doc_counts,
                &mut self.postings_builder,
                &mut self.term_batches,
            )?;
            self.block_bytes = 0;
        }

        // Block cuts: at the byte target, and ALWAYS at a field boundary
        // (the composite key's first two bytes are the field id) — a block
        // never spans fields, so a field probe's block range is exact.
        let field_changed =
            key.len() >= 2 && self.last_key.len() >= 2 && key[..2] != self.last_key[..2];
        if !self.dict_block.is_empty()
            && (self.dict_block.raw_bytes() >= crate::dict_blocks::BLOCK_TARGET_BYTES
                || field_changed)
        {
            self.flush_dict_block()?;
        }
        if self.dict_block.is_empty() {
            self.dict_block_first_key = key.to_vec();
            self.dict_block_first_ordinal = self.term_count;
        }
        self.dict_block.push(key)?;
        if self.first_key.is_empty() {
            self.first_key = key.to_vec();
        }
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.term_count += 1;
        Ok(())
    }

    /// Drain the CLOSED term batches accumulated so far (the open one keeps
    /// filling). The spooled-rebuild finish streams these into the
    /// incremental terms-blob writer as they close, so the sink never holds
    /// more than one open batch of postings rows.
    pub(crate) fn take_closed_batches(&mut self) -> Vec<RecordBatch> {
        std::mem::take(&mut self.term_batches)
    }

    /// Close the sink without writing the blobs: the raw term batches and
    /// dictionary rows (row-group `first_ordinal`s local to this sink). The
    /// parallel index merge runs one sink per key range and assembles the
    /// blobs with [`write_index_blobs`].
    pub(crate) fn into_parts(mut self) -> Result<TermSinkParts> {
        flush_terms_batch(
            &self.terms_schema,
            &mut self.doc_counts,
            &mut self.postings_builder,
            &mut self.term_batches,
        )?;
        self.flush_dict_block()?;
        let RegionSink::Mem(dict_blocks) = self.dict_blocks else {
            return Err(VixError::Writer(
                "internal: into_parts on a spooled sink (spooled sinks close through \
                 into_spooled_parts)"
                    .to_string(),
            ));
        };
        let RegionSink::Mem(plist) = self.plist else {
            return Err(VixError::Writer(
                "internal: into_parts on a spooled sink (spooled sinks close through \
                 into_spooled_parts)"
                    .to_string(),
            ));
        };
        Ok(TermSinkParts {
            term_batches: self.term_batches,
            dict_blocks,
            dict_meta: self.dict_meta,
            first_key: self.first_key,
            last_key: self.last_key,
            term_count: self.term_count,
            bloom: self.bloom,
            plist_min_docs: self.plist_min_docs,
            plist,
        })
    }

    /// Close a SPOOLED sink (see [`Self::with_spool`]): flushes the open
    /// term batch and dictionary block, then hands back the tail batches
    /// (for the incremental terms-blob writer), the closed byte regions as
    /// container-ready [`BlobPart`]s, and the dictionary/bloom state.
    pub(crate) fn into_spooled_parts(mut self) -> Result<SpooledSinkParts> {
        flush_terms_batch(
            &self.terms_schema,
            &mut self.doc_counts,
            &mut self.postings_builder,
            &mut self.term_batches,
        )?;
        self.flush_dict_block()?;
        let plist = (!self.plist.is_empty())
            .then(|| self.plist.into_part())
            .transpose()?;
        Ok(SpooledSinkParts {
            tail_batches: self.term_batches,
            dict_blocks: self.dict_blocks.into_part()?,
            dict_meta: self.dict_meta,
            term_count: self.term_count,
            bloom: self.bloom,
            plist,
        })
    }
}

/// A closed SPOOLED [`TermSink`] (single-sink rebuild finish only — no
/// multi-part rebase, so offsets/ordinals are already global).
pub(crate) struct SpooledSinkParts {
    /// Closed term batches not yet pushed to the incremental terms writer.
    pub(crate) tail_batches: Vec<RecordBatch>,
    pub(crate) dict_blocks: BlobPart,
    /// `(first_key, blocks offset, first ordinal)` per block, in key order.
    pub(crate) dict_meta: Vec<(Vec<u8>, u64, u64)>,
    pub(crate) term_count: u64,
    pub(crate) bloom: crate::bloom::BloomHashAcc,
    /// `None` when no pointer cell was pushed (mirrors
    /// [`write_index_blobs`]'s empty-plist elision).
    pub(crate) plist: Option<BlobPart>,
}

/// A closed [`TermSink`]: everything but the blob writes.
pub(crate) struct TermSinkParts {
    term_batches: Vec<RecordBatch>,
    /// This part's encoded dictionary blocks (offsets part-local).
    dict_blocks: Vec<u8>,
    /// `(first_key, part-local offset, part-local first ordinal)` per block.
    dict_meta: Vec<(Vec<u8>, u64, u64)>,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    term_count: u64,
    pub(crate) bloom: crate::bloom::BloomHashAcc,
    /// The sink's plist threshold — every part of one build carries the
    /// same value ([`write_index_blobs`] enforces it: the rebase relies on
    /// one uniform `doc_count` predicate to spot pointer cells).
    plist_min_docs: u32,
    /// The sink's out-of-row region, offsets local to this sink.
    plist: Vec<u8>,
}

/// [`IndexBlobs`] with container-ready payloads: the vocabulary-scaled
/// blobs (`terms`/`dict_blocks`/`plist`) may be spooled ([`BlobPart`]);
/// the small dictionary block index stays in memory. In-memory builds wrap
/// their `Vec<u8>`s unchanged via `From<IndexBlobs>`.
pub(crate) struct IndexBlobParts {
    pub(crate) dict: Vec<u8>,
    pub(crate) dict_blocks: BlobPart,
    pub(crate) terms: BlobPart,
    pub(crate) plist: Option<BlobPart>,
}

impl From<IndexBlobs> for IndexBlobParts {
    fn from(blobs: IndexBlobs) -> Self {
        IndexBlobParts {
            dict: blobs.dict,
            dict_blocks: BlobPart::Mem(blobs.dict_blocks),
            terms: BlobPart::Mem(blobs.terms),
            plist: blobs.plist.map(BlobPart::Mem),
        }
    }
}

/// The encoded index blobs of one build ([`write_index_blobs`]).
pub(crate) struct IndexBlobs {
    /// The dictionary block INDEX (raw [`crate::dict_blocks`] index bytes,
    /// NOT a Vortex file).
    pub(crate) dict: Vec<u8>,
    /// The dictionary BLOCKS region (raw concatenated encoded blocks).
    pub(crate) dict_blocks: Vec<u8>,
    pub(crate) terms: Vec<u8>,
    /// The out-of-row postings region: RAW concatenated
    /// [`postings::encode_record`] bytes, addressed by the terms table's
    /// 12-byte pointer cells (deliberately NOT a Vortex file — readers
    /// slice/range-fetch `[offset..offset+len]` directly). `None` when no
    /// pointer cell exists (feature off, or no term crossed the threshold).
    pub(crate) plist: Option<Vec<u8>>,
}

/// Write the `dict`/`terms` blobs from sink parts covering consecutive,
/// disjoint, ascending key ranges: dictionary `first_ordinal`s are rebased
/// by each part's global term offset, per-part plist regions concatenate
/// with their pointer cells' OFFSETS rebased likewise, then everything is
/// encoded exactly as a single sink would. Returns `(blobs, total term
/// count)`; no terms at all -> `(None, 0)`.
#[allow(clippy::type_complexity)]
pub(crate) fn write_index_blobs(
    parts: Vec<TermSinkParts>,
    encode_threads: usize,
) -> Result<(Option<IndexBlobs>, u64, crate::bloom::BloomHashAcc)> {
    let terms_schema = Arc::new(Schema::new(vec![
        Field::new("doc_count", DataType::UInt32, false),
        Field::new("postings", DataType::Binary, false),
    ]));
    let mut term_batches: Vec<RecordBatch> = Vec::new();
    let mut index = crate::dict_blocks::IndexBuilder::new();
    let mut dict_blocks: Vec<u8> = Vec::new();
    let mut term_count = 0u64;
    let mut bloom = crate::bloom::BloomHashAcc::default();
    // Structural backstop for parallel-merge partitioning bugs: parts MUST
    // cover consecutive, disjoint, ascending key ranges. Writing them
    // unchecked produced files whose dictionary violates the reader's
    // index validation (prod corruption 2026-07-29) — fail the merge
    // instead, the job retries and the inputs stay intact.
    let mut prev_last: Option<&[u8]> = None;
    for part in &parts {
        if let (Some(prev), false) = (prev_last, part.first_key.is_empty()) {
            if part.first_key.as_slice() <= prev {
                return Err(VixError::Writer(format!(
                    "merge range parts out of order: a range starts at key {:02x?} but the previous range ended at {:02x?}",
                    &part.first_key[..part.first_key.len().min(24)],
                    &prev[..prev.len().min(24)],
                )));
            }
        }
        if !part.last_key.is_empty() {
            prev_last = Some(part.last_key.as_slice());
        }
    }
    // One build = one option set: the pointer-cell rebase below spots
    // pointer cells purely by `doc_count >= plist_min_docs`, which is only
    // sound when every part applied the same threshold.
    let plist_min_docs = parts.first().map_or(0, |part| part.plist_min_docs);
    if parts
        .iter()
        .any(|part| part.plist_min_docs != plist_min_docs)
    {
        return Err(VixError::Writer(
            "internal: merge range parts disagree on plist_min_docs".to_string(),
        ));
    }
    let mut plist: Vec<u8> = Vec::new();
    for mut part in parts {
        bloom.merge(std::mem::take(&mut part.bloom));
        // Out-of-row postings: each sink's region starts at offset 0, so
        // every pointer cell of a part that lands AFTER already-collected
        // plist bytes must be rebased by them. Pointer cells are identified
        // STRUCTURALLY (doc_count >= threshold, non-empty cell) — exactly
        // how the reader will resolve them.
        let plist_base = plist.len() as u64;
        if plist_base > 0 && !part.plist.is_empty() {
            rebase_pointer_cells(&mut part.term_batches, plist_min_docs, plist_base)?;
        }
        plist.append(&mut part.plist);
        // rebase this part's block offsets and ordinals into the global
        // regions, in order — the index builder sees one ascending stream
        let blocks_base = dict_blocks.len() as u64;
        for (first_key, offset, first_ordinal) in &part.dict_meta {
            index.push_block(first_key, blocks_base + offset, term_count + first_ordinal)?;
        }
        dict_blocks.extend_from_slice(&part.dict_blocks);
        term_batches.append(&mut part.term_batches);
        term_count += part.term_count;
    }
    if term_count == 0 {
        return Ok((None, 0, bloom));
    }
    let terms_blob = write_vortex_blob(
        &terms_schema,
        &term_batches,
        addressable_strategy(),
        encode_threads,
    )?;
    Ok((
        Some(IndexBlobs {
            dict: index.finish(),
            dict_blocks,
            terms: terms_blob,
            // non-empty ⇔ at least one pointer cell was pushed (a record is
            // never zero bytes: its skip-table header alone is 4)
            plist: (!plist.is_empty()).then_some(plist),
        }),
        term_count,
        bloom,
    ))
}

/// Stream sorted field shards while constructing the composite key in one
/// reusable buffer. Shards and their tokens are already in exact on-disk
/// order.
fn push_sorted_shards(
    shards: &[SortedTermShard],
    sink: &mut TermSink,
    row_count: u64,
) -> Result<()> {
    let mut key = Vec::new();
    for shard in shards {
        for (token, ids) in &shard.terms {
            write_composite(&mut key, token, shard.field_id);
            sink.push_ids(&key, ids, row_count)?;
        }
    }
    Ok(())
}

/// M17 item 4: shard ranges for the PARALLEL rebuild index-blob build,
/// chosen from exact resident field sizes as weighted quantiles snapped up
/// to field boundaries.
///
/// Field-boundary snapping is what makes the parallel build BYTE-identical
/// to the sequential one: [`TermSink::push`] cuts a dictionary block at
/// every 2-byte field-id change, so a range starting at a field's first
/// key begins a fresh block exactly where the sequential sink would — the
/// concatenated dict-blocks region cannot differ. (The terms blob's
/// row-block continuity is restored separately by
/// [`write_index_blobs_recut`].) M10's output-keyspace invariants hold
/// trivially: there is ONE key space (the writer's own), and every bound
/// is a real key in it. Empty when fewer than 2 field regions exist or
/// `ranges <= 1` — the caller then runs the sequential path.
fn rebuild_partition_ranges(
    shards: &[SortedTermShard],
    ranges: usize,
) -> Vec<std::ops::Range<usize>> {
    // floor: a small map (move-job L0 builds, tiny heals) gains nothing
    // from worker spawn + range bookkeeping — stay sequential
    let total: usize = shards.iter().map(|shard| shard.terms.len()).sum();
    if ranges <= 1 || total < 1024 || shards.len() <= 1 {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(ranges.min(shards.len()));
    let mut start = 0usize;
    let mut cumulative = 0usize;
    let mut next_target = total / ranges;
    for (index, shard) in shards.iter().enumerate() {
        if index > start && cumulative >= next_target && out.len() + 1 < ranges {
            out.push(start..index);
            start = index;
            next_target = total * (out.len() + 1) / ranges;
        }
        cumulative += shard.terms.len();
    }
    out.push(start..shards.len());
    out
}

/// [`write_index_blobs`] for the M17 parallel REBUILD build: identical
/// ordering backstop, dict/plist rebase and bloom merge, but the terms
/// batches are RE-CUT — every part's `(doc_count, cell)` rows stream
/// through one continuous accumulation that replays [`TermSink::push`]'s
/// flush rule (`bytes-since-flush >= postings_chunk_bytes`), so the
/// row-block boundaries (and with them the terms blob's bytes) equal the
/// single-sink sequential build EXACTLY for any range partitioning.
/// Pointer cells rebase inline during the replay (same structural
/// predicate as [`rebase_pointer_cells`]).
#[allow(clippy::type_complexity)]
pub(crate) fn write_index_blobs_recut(
    parts: Vec<TermSinkParts>,
    postings_chunk_bytes: usize,
    encode_threads: usize,
) -> Result<(Option<IndexBlobs>, u64, crate::bloom::BloomHashAcc)> {
    let terms_schema = Arc::new(Schema::new(vec![
        Field::new("doc_count", DataType::UInt32, false),
        Field::new("postings", DataType::Binary, false),
    ]));
    let mut term_batches: Vec<RecordBatch> = Vec::new();
    let mut index = crate::dict_blocks::IndexBuilder::new();
    let mut dict_blocks: Vec<u8> = Vec::new();
    let mut term_count = 0u64;
    let mut bloom = crate::bloom::BloomHashAcc::default();
    let mut prev_last: Option<&[u8]> = None;
    for part in &parts {
        if let (Some(prev), false) = (prev_last, part.first_key.is_empty()) {
            if part.first_key.as_slice() <= prev {
                return Err(VixError::Writer(format!(
                    "rebuild range parts out of order: a range starts at key {:02x?} but the previous range ended at {:02x?}",
                    &part.first_key[..part.first_key.len().min(24)],
                    &prev[..prev.len().min(24)],
                )));
            }
        }
        if !part.last_key.is_empty() {
            prev_last = Some(part.last_key.as_slice());
        }
    }
    let plist_min_docs = parts.first().map_or(0, |part| part.plist_min_docs);
    if parts
        .iter()
        .any(|part| part.plist_min_docs != plist_min_docs)
    {
        return Err(VixError::Writer(
            "internal: rebuild range parts disagree on plist_min_docs".to_string(),
        ));
    }
    let mut plist: Vec<u8> = Vec::new();
    // the continuous re-cut accumulation (TermSink::push's exact rule)
    let mut doc_counts: Vec<u32> = Vec::new();
    let mut postings_builder = BinaryBuilder::new();
    let mut block_bytes = 0usize;
    for mut part in parts {
        bloom.merge(std::mem::take(&mut part.bloom));
        let plist_base = plist.len() as u64;
        let blocks_base = dict_blocks.len() as u64;
        for (first_key, offset, first_ordinal) in &part.dict_meta {
            index.push_block(first_key, blocks_base + offset, term_count + first_ordinal)?;
        }
        dict_blocks.extend_from_slice(&part.dict_blocks);
        for batch in &part.term_batches {
            let counts = batch
                .column_by_name("doc_count")
                .and_then(|column| column.as_any().downcast_ref::<UInt32Array>())
                .ok_or_else(|| {
                    VixError::Writer(
                        "internal: terms batch lacks a u32 doc_count column".to_string(),
                    )
                })?;
            let cells = batch
                .column_by_name("postings")
                .and_then(|column| column.as_any().downcast_ref::<BinaryArray>())
                .ok_or_else(|| {
                    VixError::Writer(
                        "internal: terms batch lacks a binary postings column".to_string(),
                    )
                })?;
            for row in 0..batch.num_rows() {
                let doc_count = counts.value(row);
                let cell = cells.value(row);
                let rebased;
                let cell: &[u8] = if plist_base > 0
                    && plist_min_docs > 0
                    && doc_count >= plist_min_docs
                    && !cell.is_empty()
                {
                    let (offset, len) = postings::decode_pointer_cell(cell)
                        .map_err(|e| VixError::Writer(format!("internal: {e}")))?;
                    let offset = offset.checked_add(plist_base).ok_or_else(|| {
                        VixError::Writer(format!(
                            "plist offset {offset} + base {plist_base} overflows u64"
                        ))
                    })?;
                    rebased = postings::encode_pointer_cell(offset, len);
                    &rebased
                } else {
                    cell
                };
                doc_counts.push(doc_count);
                block_bytes += cell.len();
                postings_builder.append_value(cell);
                if block_bytes >= postings_chunk_bytes {
                    flush_terms_batch(
                        &terms_schema,
                        &mut doc_counts,
                        &mut postings_builder,
                        &mut term_batches,
                    )?;
                    block_bytes = 0;
                }
            }
        }
        plist.append(&mut part.plist);
        term_count += part.term_count;
    }
    flush_terms_batch(
        &terms_schema,
        &mut doc_counts,
        &mut postings_builder,
        &mut term_batches,
    )?;
    if term_count == 0 {
        return Ok((None, 0, bloom));
    }
    let terms_blob = write_vortex_blob(
        &terms_schema,
        &term_batches,
        addressable_strategy(),
        encode_threads,
    )?;
    Ok((
        Some(IndexBlobs {
            dict: index.finish(),
            dict_blocks,
            terms: terms_blob,
            plist: (!plist.is_empty()).then_some(plist),
        }),
        term_count,
        bloom,
    ))
}

/// Rebase the pointer cells of one sink's term batches by `plist_base`
/// bytes: a term row with `doc_count >= plist_min_docs` and a NON-EMPTY
/// postings cell is a pointer cell (dense-elided terms stay empty even
/// above the threshold; nothing below it is ever a pointer) — its
/// sink-local `u64` offset moves to the concatenated `plist` blob's space.
/// Every such cell must be exactly 12 bytes; anything else is a corrupt
/// sink and fails the build. All other cells pass through untouched, so a
/// build without pointer cells is byte-identical to the pre-plist output.
fn rebase_pointer_cells(
    batches: &mut [RecordBatch],
    plist_min_docs: u32,
    plist_base: u64,
) -> Result<()> {
    debug_assert!(plist_min_docs > 0, "a plist region requires a threshold");
    for batch in batches.iter_mut() {
        let doc_counts = batch
            .column_by_name("doc_count")
            .and_then(|column| column.as_any().downcast_ref::<UInt32Array>())
            .ok_or_else(|| {
                VixError::Writer("internal: terms batch lacks a u32 doc_count column".to_string())
            })?;
        let postings_column = batch
            .column_by_name("postings")
            .and_then(|column| column.as_any().downcast_ref::<BinaryArray>())
            .ok_or_else(|| {
                VixError::Writer("internal: terms batch lacks a binary postings column".to_string())
            })?;
        let mut rebased = BinaryBuilder::new();
        for row in 0..batch.num_rows() {
            let cell = postings_column.value(row);
            if doc_counts.value(row) >= plist_min_docs && !cell.is_empty() {
                let (offset, len) = postings::decode_pointer_cell(cell)
                    .map_err(|e| VixError::Writer(format!("internal: {e}")))?;
                let offset = offset.checked_add(plist_base).ok_or_else(|| {
                    VixError::Writer(format!(
                        "plist offset {offset} + base {plist_base} overflows u64"
                    ))
                })?;
                rebased.append_value(postings::encode_pointer_cell(offset, len));
            } else {
                rebased.append_value(cell);
            }
        }
        let doc_counts = Arc::clone(
            batch
                .column_by_name("doc_count")
                .expect("checked just above"),
        );
        *batch =
            RecordBatch::try_new(batch.schema(), vec![doc_counts, Arc::new(rebased.finish())])?;
    }
    Ok(())
}

/// In-progress dictionary row group.
/// Typed view over the numeric/bool column flavors whose values emit tagged
/// canonical terms. Narrow integers are widened losslessly on construction;
/// floats keep their own width — the canonical text of an `f32` differs from
/// the canonical text of `f32 as f64` (shortest-form semantics, see
/// [`crate::numeric::canonical_f32_text`]); `Float16` widens exactly to
/// `f32`, mirroring arrow-json's encoder.
enum NumericColumn {
    Bool(BooleanArray),
    Int(Int64Array),
    UInt(UInt64Array),
    F32(Float32Array),
    F64(Float64Array),
}

impl NumericColumn {
    fn try_new(column: &dyn Array) -> Option<Self> {
        fn cast_to<T: Array + Clone + 'static>(column: &dyn Array, ty: &DataType) -> Option<T> {
            let column = cast(column, ty).ok()?;
            column.as_any().downcast_ref::<T>().cloned()
        }
        match column.data_type() {
            DataType::Boolean => column
                .as_any()
                .downcast_ref::<BooleanArray>()
                .cloned()
                .map(Self::Bool),
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                cast_to::<Int64Array>(column, &DataType::Int64).map(Self::Int)
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                cast_to::<UInt64Array>(column, &DataType::UInt64).map(Self::UInt)
            }
            // f16 -> f32 is exact; arrow-json encodes Float16 through f32 too
            DataType::Float16 => cast_to::<Float32Array>(column, &DataType::Float32).map(Self::F32),
            DataType::Float32 => column
                .as_any()
                .downcast_ref::<Float32Array>()
                .cloned()
                .map(Self::F32),
            DataType::Float64 => column
                .as_any()
                .downcast_ref::<Float64Array>()
                .cloned()
                .map(Self::F64),
            _ => None,
        }
    }

    /// Write the canonical text of the value at `row` into `out`; `false`
    /// when the slot emits no value term (null, or a non-finite float — the
    /// arrow-json `_source` image of those is the literal `null`).
    fn canonical_into(&self, row: usize, out: &mut String) -> bool {
        out.clear();
        match self {
            Self::Bool(array) => {
                if !array.is_valid(row) {
                    return false;
                }
                out.push_str(canonical_bool_text(array.value(row)));
            }
            Self::Int(array) => {
                if !array.is_valid(row) {
                    return false;
                }
                out.push_str(&canonical_i64_text(array.value(row)));
            }
            Self::UInt(array) => {
                if !array.is_valid(row) {
                    return false;
                }
                out.push_str(&canonical_u64_text(array.value(row)));
            }
            Self::F32(array) => {
                if !array.is_valid(row) {
                    return false;
                }
                let Some(text) = canonical_f32_text(array.value(row)) else {
                    return false;
                };
                out.push_str(&text);
            }
            Self::F64(array) => {
                if !array.is_valid(row) {
                    return false;
                }
                let Some(text) = canonical_f64_text(array.value(row)) else {
                    return false;
                };
                out.push_str(&text);
            }
        }
        true
    }
}

/// Typed view over the three arrow string-array flavors.
enum StringColumn<'a> {
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Utf8View(&'a StringViewArray),
}

impl<'a> StringColumn<'a> {
    fn try_new(array: &'a dyn Array) -> Option<Self> {
        match array.data_type() {
            DataType::Utf8 => array.as_any().downcast_ref().map(Self::Utf8),
            DataType::LargeUtf8 => array.as_any().downcast_ref().map(Self::LargeUtf8),
            DataType::Utf8View => array.as_any().downcast_ref().map(Self::Utf8View),
            _ => None,
        }
    }

    /// The value at `row`, or `None` when null.
    fn value(&self, row: usize) -> Option<&'a str> {
        match self {
            Self::Utf8(array) => array.is_valid(row).then(|| array.value(row)),
            Self::LargeUtf8(array) => array.is_valid(row).then(|| array.value(row)),
            Self::Utf8View(array) => array.is_valid(row).then(|| array.value(row)),
        }
    }
}

#[cfg(test)]
mod field_cut_tests {
    use super::*;
    use crate::query::write_composite;

    /// Dictionary blocks never span a field boundary: the sink cuts the
    /// open block at every field change (and at the byte target), so a
    /// field probe's block range is exactly its own field's blocks.
    #[test]
    fn blocks_cut_at_field_boundaries() {
        let mut sink = TermSink::new(1 << 20);
        let mut key = Vec::new();
        for i in 0..40 {
            write_composite(&mut key, format!("tok{i:04}").as_bytes(), 1);
            sink.push(&key, 1, &[0u8]).unwrap();
        }
        for i in 0..2 {
            write_composite(&mut key, format!("val{i}").as_bytes(), 2);
            sink.push(&key, 1, &[0u8]).unwrap();
        }
        let parts = sink.into_parts().unwrap();
        assert!(parts.dict_meta.len() >= 2, "expected a field-boundary cut");
        // every block's keys stay within one field: check via first keys +
        // a full decode of each block
        for (i, (first_key, offset, _)) in parts.dict_meta.iter().enumerate() {
            let end = parts
                .dict_meta
                .get(i + 1)
                .map(|(_, next, _)| *next as usize)
                .unwrap_or(parts.dict_blocks.len());
            let block = &parts.dict_blocks[*offset as usize..end];
            let fid = first_key[..2].to_vec();
            crate::dict_blocks::block_scan(block, |_, k| {
                assert_eq!(k[..2], fid[..], "a block must never span two fields");
                true
            })
            .unwrap();
        }
        let last = &parts.dict_meta.last().unwrap().0;
        assert_eq!(
            &last[..2],
            &2u16.to_be_bytes(),
            "field 2 starts its own block"
        );
    }

    /// Ordinals are implicit and contiguous across blocks: block b's
    /// first_ordinal equals the running key count.
    #[test]
    fn block_ordinals_are_contiguous() {
        let mut sink = TermSink::new(1 << 20);
        let mut key = Vec::new();
        let mut total = 0u64;
        for fid in [1u16, 2, 3] {
            for i in 0..10 {
                write_composite(&mut key, format!("t{i:02}").as_bytes(), fid);
                sink.push(&key, 1, &[0u8]).unwrap();
                total += 1;
            }
        }
        let parts = sink.into_parts().unwrap();
        let mut running = 0u64;
        for (i, (_, offset, first_ordinal)) in parts.dict_meta.iter().enumerate() {
            assert_eq!(*first_ordinal, running, "block {i}");
            let end = parts
                .dict_meta
                .get(i + 1)
                .map(|(_, next, _)| *next as usize)
                .unwrap_or(parts.dict_blocks.len());
            let block = &parts.dict_blocks[*offset as usize..end];
            let mut n = 0u64;
            crate::dict_blocks::block_scan(block, |_, _| {
                n += 1;
                true
            })
            .unwrap();
            running += n;
        }
        assert_eq!(running, total);
    }
}

#[cfg(test)]
mod plist_sink_tests {
    use bytes::Bytes;

    use super::*;
    use crate::{
        container::{BlobHandle, RowSelection, column_binary, column_u64, scan_blob},
        query::write_composite,
    };

    /// Decode every term row of an encoded `terms` blob against the
    /// concatenated plist region, resolving cells exactly as the reader
    /// does: empty + `doc_count > 0` ⇒ dense (`0..row_count`); non-empty +
    /// `doc_count >= threshold` ⇒ 12-byte pointer into `plist`; everything
    /// else inline. Returns `(doc_count, ids, raw cell)` per term.
    fn decode_terms(
        terms: Vec<u8>,
        plist: &[u8],
        threshold: u32,
        row_count: u64,
    ) -> Vec<(u64, Vec<u32>, Vec<u8>)> {
        let handle = BlobHandle::Mem(Bytes::from(terms));
        let mut out = Vec::new();
        for batch in scan_blob(&handle, Some(&["doc_count", "postings"]), RowSelection::All)
            .expect("scan terms blob")
        {
            let doc_counts = column_u64(&batch, "doc_count").unwrap();
            let cells = column_binary(&batch, "postings").unwrap();
            for (row, &doc_count) in doc_counts.iter().enumerate() {
                let cell = cells.value(row);
                let mut ids = Vec::new();
                if cell.is_empty() && doc_count > 0 {
                    ids.extend(0..row_count as u32);
                } else if threshold > 0 && doc_count >= u64::from(threshold) {
                    assert_eq!(cell.len(), 12, "pointer cell must be exactly 12 bytes");
                    let (offset, len) = postings::decode_pointer_cell(cell).unwrap();
                    let record = &plist[offset as usize..(offset + u64::from(len)) as usize];
                    postings::decode_each(
                        postings::record_blob(record).unwrap(),
                        doc_count as usize,
                        |doc| {
                            ids.push(doc);
                            Ok(())
                        },
                    )
                    .unwrap();
                } else {
                    postings::decode_each(cell, doc_count as usize, |doc| {
                        ids.push(doc);
                        Ok(())
                    })
                    .unwrap();
                }
                out.push((doc_count, ids, cell.to_vec()));
            }
        }
        out
    }

    /// Multi-sink offset rebasing: a parallel merge produces one sink per
    /// key range, each accumulating a plist region that starts at ITS OWN
    /// offset 0. After [`write_index_blobs`] concatenates the parts, every
    /// pointer cell must resolve to its record inside the single blob —
    /// sink B's local offsets shifted by exactly sink A's region bytes,
    /// while inline and dense-elided cells pass through untouched.
    #[test]
    fn multi_sink_plist_rebase_resolves_all_pointers() {
        const THRESHOLD: u32 = 3;
        const ROW_COUNT: u64 = 1_000;
        let new_sink = || TermSink::new(1 << 20).with_plist_min_docs(THRESHOLD);
        let mut expected: Vec<Vec<u32>> = Vec::new();
        let mut push = |sink: &mut TermSink, fid: u16, token: &[u8], ids: Vec<u32>| {
            let mut key = Vec::new();
            write_composite(&mut key, token, fid);
            sink.push_ids(&key, &ids, ROW_COUNT).unwrap();
            expected.push(ids);
        };

        let bb: Vec<u32> = (0..800).step_by(2).collect(); // 400 ids
        let b_aa: Vec<u32> = (5..905).step_by(3).collect(); // 300 ids
        let mut sink_a = new_sink();
        push(&mut sink_a, 1, b"aa", vec![1, 5]); // inline (2 < 3)
        push(&mut sink_a, 1, b"bb", bb.clone()); // pointer at region offset 0
        push(&mut sink_a, 1, b"cc", vec![3, 7, 11, 400]); // pointer, offset > 0
        let mut sink_b = new_sink();
        push(&mut sink_b, 2, b"aa", b_aa); // pointer at LOCAL offset 0 -> rebased
        push(&mut sink_b, 2, b"dd", (0..ROW_COUNT as u32).collect()); // dense: empty cell
        push(&mut sink_b, 2, b"zz", vec![9]); // inline

        let (blobs, term_count, _bloom) = write_index_blobs(
            vec![sink_a.into_parts().unwrap(), sink_b.into_parts().unwrap()],
            0,
        )
        .unwrap();
        assert_eq!(term_count, 6);
        let IndexBlobs { terms, plist, .. } = blobs.unwrap();
        let plist = plist.expect("pointer cells were pushed");

        let decoded = decode_terms(terms, &plist, THRESHOLD, ROW_COUNT);
        assert_eq!(decoded.len(), expected.len());
        for (term, ((doc_count, ids, _), want)) in decoded.iter().zip(&expected).enumerate() {
            assert_eq!(*doc_count as usize, want.len(), "term {term} doc_count");
            assert_eq!(ids, want, "term {term} postings");
        }

        // sink B's first pointer (term 3) rebased by exactly sink A's
        // region bytes; sink A's own offsets stayed local (term 1 at 0)
        let sink_a_region = postings::encode_record(&bb).unwrap().len()
            + postings::encode_record(&[3, 7, 11, 400]).unwrap().len();
        let (offset, _) = postings::decode_pointer_cell(&decoded[1].2).unwrap();
        assert_eq!(offset, 0, "sink A's first pointer keeps offset 0");
        let (offset, _) = postings::decode_pointer_cell(&decoded[3].2).unwrap();
        assert_eq!(
            offset as usize, sink_a_region,
            "sink B's local offset 0 must rebase by sink A's region"
        );
        // dense above the threshold stayed the empty cell
        assert!(
            decoded[4].2.is_empty(),
            "dense term must keep its empty cell"
        );
    }
}
