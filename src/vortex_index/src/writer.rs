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
//! (`_timestamp`, the column-store fields with their arrow types, the
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
//! probes as a union). Fields in
//! [`VixWriterOptions::column_store_field_names`] (any type) are stored
//! natively in the `docs` blob.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
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

use crate::{
    container::{
        BLOB_TAG_BLOOM, BLOB_TAG_DICT, BLOB_TAG_DICT_BLOCKS, BLOB_TAG_PLIST, BLOB_TAG_TERMS,
        BLOB_TYPE_BLOOM, BLOB_TYPE_DICT, BLOB_TYPE_DICT_BLOCKS, BLOB_TYPE_PLIST, BLOB_TYPE_TERMS,
        DICT_LAYOUT_BLOCKS, DocsBlobEncoder, FIELD_TYPE_CS, FIELD_TYPE_FTS, FIELD_TYPE_TERM,
        FieldEntry, KEY_LAYOUT_FID_V2, PROP_DICT_LAYOUT, PROP_FIELDS, PROP_KEY_LAYOUT,
        PROP_PARTIAL_FIELDS, PROP_PLIST_MIN_DOCS, PROP_ROW_COUNT, PROP_ROW_GROUP_SIZE,
        PROP_TERM_COUNT, PROP_TOKENIZER, PROP_VERSION, PROP_ZONE_MAP, TOKENIZER_ID,
        VIX_FORMAT_VERSION, VixOutput, ZoneEntry, addressable_strategy, finish_streamed_container,
        write_vortex_blob,
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

fn is_string_family(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
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
#[derive(Debug, Clone)]
pub struct VixWriterOptions {
    /// Fields whose values are additionally tokenized for full-text search.
    pub fts_field_names: Vec<String>,
    /// Fields (any type) stored as native Vortex columns in the `docs` blob.
    pub column_store_field_names: Vec<String>,
    /// Raw-string term-indexed fields to record per-file value blooms for
    /// (the `bloom` puffin blob, built as a byproduct of term emission —
    /// see [`crate::bloom`]). Typically `trace_id`/`span_id`.
    pub bloom_field_names: Vec<String>,
    /// False-positive probability of the per-file value blooms.
    pub bloom_fpp: f64,
    /// Target byte size of one postings row block (point-read granularity).
    pub postings_chunk_bytes: usize,
    /// **Raw** (non-fts) values longer than this many bytes are skipped and
    /// the field is recorded in the `partial_fields` property. Fts fields are
    /// never gated by it: their values tokenize regardless of length (tokens
    /// are byte-bounded by [`Self::max_token_len`]), so an fts field never
    /// goes partial for oversize values.
    pub max_raw_term_len: usize,
    /// Logical row-group size recorded as a file property (a grouping
    /// constant for downstream row-id encodings). `0` = unknown. The `docs`
    /// chunks are sized by [`Self::docs_chunk_bytes`] instead.
    pub row_group_size: usize,
    /// Uncompressed-byte budget of one `docs`-blob chunk — the
    /// decompression unit of a matched-row point read. Rows per chunk =
    /// `clamp(budget / avg_row_bytes, 64, 65536)` (the low floor lets the
    /// byte budget govern even multi-KiB rows — a 1024-row floor used to
    /// inflate ~4 KiB-row chunks to hundreds of times a small budget —
    /// while the ceiling bounds decoded batch sizes). Vortex's write
    /// pipeline still coalesces sub-1 MiB chunks up to ~1 MiB (its
    /// S3-tuned segment minimum, in multiples of the row count above), so
    /// the effective decode unit is ≈ `max(budget, 1 MiB)` — plus the
    /// 64-row floor for pathological >16 KiB average rows.
    /// `0` = the 4 MiB default.
    pub docs_chunk_bytes: usize,
    /// Minimum full-text token length in **bytes** (clamped to `>= 2`;
    /// see [`crate::o2_tokenize`]).
    pub min_token_len: usize,
    /// Maximum full-text token length in **bytes** (clamped to `>= 64`,
    /// exclusive bound; see [`crate::o2_tokenize`]).
    pub max_token_len: usize,
    /// Threads for the blob encode/compress pipelines at `finish` (and for
    /// the compaction index merge's blob writes). `0`/`1` = everything on
    /// the calling thread — the default; the compactor raises it
    /// (`ZO_VIX_MERGE_THREAD_NUM`).
    pub encode_threads: usize,
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
}

/// Default [`VixWriterOptions::docs_chunk_bytes`]: 4 MiB.
pub const DEFAULT_DOCS_CHUNK_BYTES: usize = 4 * 1024 * 1024;
/// Rows-per-chunk clamp bounds of the `docs` blob (see
/// [`VixWriterOptions::docs_chunk_bytes`]). The floor is low so the byte
/// budget governs wide rows too; with the 4 MiB default budget it only
/// engages beyond ~64 KiB average rows.
const DOCS_CHUNK_MIN_ROWS: usize = 64;
const DOCS_CHUNK_MAX_ROWS: usize = 65536;

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
            column_store_field_names: Vec::new(),
            bloom_field_names: Vec::new(),
            bloom_fpp: crate::bloom::DEFAULT_FILE_BLOOM_FPP,
            postings_chunk_bytes: 128 * 1024,
            max_raw_term_len: 65532,
            row_group_size: 0,
            docs_chunk_bytes: DEFAULT_DOCS_CHUNK_BYTES,
            min_token_len: 2,
            max_token_len: 64,
            encode_threads: 0,
            docs_encode_sample_bytes: 0,
            term_spill_dir: None,
            term_spill_bytes: 0,
            output_spool_dir: None,
            postings_plist_min_docs: 0,
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
    /// Bytes of the inverted-index blobs (`dict` + `terms`) inside the
    /// container — the core-file equivalent of the old sibling index's size
    /// (`FileMeta::index_size`; observability only).
    pub index_size: u64,
    /// Bytes of the stored-records blob (`docs`).
    pub docs_size: u64,
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
    term_field_ids: HashMap<String, u16>,
    /// Term-indexed fields that also emit full-text tokens.
    fts_fields: HashSet<String>,
    /// Column-store fields present in the schema (`_timestamp` excluded).
    cs_fields: BTreeSet<String>,
    /// Arrow schema of the `docs` blob.
    docs_schema: SchemaRef,
    /// Composite term -> ascending doc ids (deduped on push).
    terms: BTreeMap<Vec<u8>, Vec<u32>>,
    /// Reusable numeric-tag buffer (`\x01{canonical}`) fed to the layout
    /// composite builder.
    tag_scratch: Vec<u8>,
    /// Estimated resident bytes of `terms` (spill trigger; see
    /// [`VixWriterOptions::term_spill_bytes`]).
    terms_bytes: usize,
    /// External-sort state: sorted runs already drained from `terms`.
    /// `None` until the first spill (and always `None` when
    /// [`VixWriterOptions::term_spill_dir`] is unset).
    term_spill: Option<spill::TermSpill>,
    partial_fields: BTreeSet<String>,
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
    /// `_timestamp` zone folding over the pushed rows, windowed by the
    /// locked rows-per-chunk. Lives and dies with `docs_encoder`.
    zone_folder: Option<ZoneMapFolder>,
    row_count: u64,
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
        let mut term_fields: Vec<String> = schema
            .fields()
            .iter()
            .filter(|field| {
                is_value_indexed_type(field.data_type())
                    && !NON_INDEXED_COLS.contains(&field.name().as_str())
            })
            .map(|field| field.name().clone())
            .collect();
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
        let term_field_ids: HashMap<String, u16> = if init_error.is_none() {
            term_fields
                .iter()
                .enumerate()
                .map(|(id, name)| (name.clone(), id as u16))
                .collect()
        } else {
            HashMap::new()
        };

        // fts marking applies to string-family fields only: tokenization is
        // a text concept. A numeric/bool field named in `fts_field_names`
        // stays a plain term field (canonical value terms), matching the
        // source-driven path where non-string values never tokenize.
        let fts_fields: HashSet<String> = opts
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

        // The `docs` blob schema: `_timestamp` first (always as i64), then
        // the column-store fields sorted by name with their original arrow
        // types, then `_source` and optionally `_original`. Requested fields
        // absent from the schema are ignored.
        let cs_fields: BTreeSet<String> = opts
            .column_store_field_names
            .iter()
            .filter(|name| {
                name.as_str() != TIMESTAMP_COL_NAME && schema.field_with_name(name).is_ok()
            })
            .cloned()
            .collect();
        let mut docs_fields: Vec<Field> = Vec::with_capacity(cs_fields.len() + 3);
        docs_fields.push(Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false));
        for name in &cs_fields {
            if let Ok(field) = schema.field_with_name(name) {
                docs_fields.push(field.clone());
            }
        }
        docs_fields.push(Field::new(SOURCE_COL_NAME, DataType::Utf8, false));
        if store_original {
            docs_fields.push(Field::new(ORIGINAL_DATA_COL_NAME, DataType::Utf8, true));
        }
        let docs_schema = Arc::new(Schema::new(docs_fields));

        Self {
            opts,
            store_original,
            term_fields,
            term_field_ids,
            fts_fields,
            cs_fields,
            docs_schema,
            terms: BTreeMap::new(),
            tag_scratch: Vec::new(),
            terms_bytes: 0,
            term_spill: None,
            partial_fields,
            sample_batches: Vec::new(),
            sample_bytes: 0,
            docs_encoder: None,
            zone_folder: None,
            row_count: 0,
            ts_range: None,
            init_error,
            scratch: Vec::new(),
            merged_index: None,
            demoted_fields: BTreeSet::new(),
            skip_ts_guard: false,
        }
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
        self.push_batch_inner(batch, source, original)?;
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
    /// `timestamps` (non-null, one per row), the schema's column-store
    /// fields looked up by name in `cs_columns` (cast to the schema type;
    /// every configured field must be supplied — pass an all-null array for
    /// a column a caller has no data for), `source` and `original` (same
    /// rules as [`Self::push_batch_with_source`]). Rows must arrive in
    /// document order.
    pub fn push_docs_rows(
        &mut self,
        timestamps: &Int64Array,
        cs_columns: &[(String, ArrowArrayRef)],
        source: &StringArray,
        original: Option<&StringArray>,
    ) -> anyhow::Result<()> {
        self.push_docs_rows_inner(timestamps, cs_columns, source, original, true)?;
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

        let merged = merge::merge_indexes(
            inputs,
            doc_maps,
            &self.term_field_ids,
            &self.opts.bloom_field_names,
            total_rows,
            self.opts.postings_chunk_bytes,
            self.opts.postings_plist_min_docs,
            threads,
        )?;
        for reader in inputs {
            self.partial_fields
                .extend(reader.partial_fields().iter().cloned());
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
        self.push_docs_rows_inner(timestamps, cs_columns, source, original, false)?;
        Ok(())
    }

    /// Build and return the complete `.vix` (puffin) file bytes. With
    /// [`VixWriterOptions::output_spool_dir`] set this reads the spool back
    /// into memory — callers that spool should use [`Self::finish_output`].
    pub fn finish(self) -> anyhow::Result<Vec<u8>> {
        self.finish_inner()?.0.into_bytes()
    }

    /// Like [`Self::finish`], additionally returning size/count stats of the
    /// produced file (e.g. `index_size` for `FileMeta` accounting).
    pub fn finish_with_stats(self) -> anyhow::Result<(Vec<u8>, VixWriterStats)> {
        let (output, stats) = self.finish_inner()?;
        Ok((output.into_bytes()?, stats))
    }

    /// Finish into a [`VixOutput`]: in-memory bytes, or — with
    /// [`VixWriterOptions::output_spool_dir`] set — a temp-file spool the
    /// container streamed into (upload from its path; it deletes on drop).
    pub fn finish_output(self) -> anyhow::Result<(VixOutput, VixWriterStats)> {
        Ok(self.finish_inner()?)
    }

    /// Test-support escape ([`crate::test_support::finish_ignoring_timestamp_guard`]):
    /// finish WITHOUT the degenerate-`_timestamp` guard, fabricating the
    /// pre-guard-era files (stored rows with `_timestamp <= 0`) that
    /// compaction-time cleansing tests need as merge inputs. Production
    /// writers must never call this — every real producer goes through
    /// [`Self::finish`]/[`Self::finish_with_stats`] and keeps the guard.
    pub(crate) fn finish_unguarded(mut self) -> Result<(Vec<u8>, VixWriterStats)> {
        self.skip_ts_guard = true;
        let (output, stats) = self.finish_inner()?;
        let bytes = output
            .into_bytes()
            .map_err(|e| VixError::Writer(format!("read back spooled output: {e}")))?;
        Ok((bytes, stats))
    }

    fn push_batch_inner(
        &mut self,
        batch: &RecordBatch,
        source: &StringArray,
        original: Option<&StringArray>,
    ) -> Result<()> {
        let num_rows = batch.num_rows();
        self.check_push_mode(true)?;
        for reserved in [SOURCE_COL_NAME, ORIGINAL_DATA_COL_NAME] {
            if batch.column_by_name(reserved).is_some() {
                return Err(VixError::Writer(format!(
                    "batch must not contain a {reserved:?} column; it is supplied through the \
                     push_batch_with_source arguments"
                )));
            }
        }
        self.check_push_inputs(num_rows, source, original)?;
        let first_doc = self.check_doc_capacity(num_rows)?;
        if num_rows == 0 {
            return Ok(());
        }

        self.index_value_terms(batch, first_doc);
        self.index_key_terms(batch, first_doc);

        let docs_batch = self.project_docs(batch, source, original)?;
        self.track_ts_range(&docs_batch)?;
        self.stage_docs_batch(docs_batch)?;
        self.row_count += num_rows as u64;
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
        self.sample_bytes += batch.get_array_memory_size();
        self.sample_batches.push(batch);
        let budget = if self.opts.docs_encode_sample_bytes == 0 {
            DOCS_ENCODE_SAMPLE_BYTES
        } else {
            self.opts.docs_encode_sample_bytes
        };
        if self.sample_bytes >= budget {
            self.start_docs_encoder()?;
        }
        Ok(())
    }

    /// Lock the docs chunking on the buffered sample, spawn the streaming
    /// encoder and hand it the sample (in push order).
    fn start_docs_encoder(&mut self) -> Result<()> {
        let rows_per_chunk = docs_rows_per_chunk(self.opts.docs_chunk_bytes, &self.sample_batches);
        let mut folder = ZoneMapFolder::new(rows_per_chunk);
        let mut encoder = DocsBlobEncoder::spawn(
            Arc::clone(&self.docs_schema),
            rows_per_chunk,
            self.opts.encode_threads,
            self.opts.output_spool_dir.clone(),
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
        self.ts_range = Some(match self.ts_range {
            Some((cur_min, cur_max)) => (cur_min.min(min), cur_max.max(max)),
            None => (min, max),
        });
        Ok(())
    }

    /// Reject push calls that do not match the writer's mode: after
    /// [`Self::merge_input_indexes`] only the unindexed docs push is valid
    /// (the index is already built), and vice versa.
    fn check_push_mode(&self, index_terms: bool) -> Result<()> {
        match (index_terms, self.merged_index.is_some()) {
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
        index_terms: bool,
    ) -> Result<()> {
        let num_rows = timestamps.len();
        self.check_push_mode(index_terms)?;
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
        let first_doc = self.check_doc_capacity(num_rows)?;
        if num_rows == 0 {
            return Ok(());
        }

        if index_terms {
            self.index_source_terms(source, first_doc)?;
        }

        let docs_batch = self.assemble_docs_rows(timestamps, cs_columns, source, original)?;
        self.track_ts_range(&docs_batch)?;
        self.stage_docs_batch(docs_batch)?;
        self.row_count += num_rows as u64;
        self.maybe_spill_terms()?;
        Ok(())
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
        if self.terms_bytes < budget || self.terms.is_empty() {
            return Ok(());
        }
        if self.term_spill.is_none() {
            self.term_spill = Some(spill::TermSpill::new(dir)?);
        }
        self.term_spill
            .as_mut()
            .expect("created above")
            .write_run(&mut self.terms)?;
        self.terms_bytes = 0;
        Ok(())
    }

    /// Source-driven term extraction: parse each `_source` object and emit
    /// the same key/value/fts terms the column-driven path derives from
    /// flattened columns (see [`Self::push_docs_rows`] for the exact rules).
    fn index_source_terms(&mut self, source: &StringArray, first_doc: u64) -> Result<()> {
        for row in 0..source.len() {
            let doc = (first_doc + row as u64) as u32;
            let text = source.value(row);
            let record: serde_json::Map<String, serde_json::Value> = serde_json::from_str(text)
                .map_err(|e| {
                    VixError::Writer(format!("_source of doc {doc} is not a JSON object: {e}"))
                })?;
            for (key, value) in &record {
                if value.is_null() {
                    // synthesis omits nulls; treat a stray one as absent
                    continue;
                }
                let key = key.as_str();
                if NON_INDEXED_COLS.contains(&key) || key == SOURCE_COL_NAME {
                    continue;
                }
                // key term: this doc has a value at `key`
                write_composite(&mut self.scratch, key.as_bytes(), KEY_FIELD_ID);
                push_term(&mut self.terms, &mut self.terms_bytes, &self.scratch, doc);

                match value {
                    // a JSON string emits its value terms — fts tokens or
                    // the raw whole value
                    serde_json::Value::String(value) => {
                        let Some(&field_id) = self.term_field_ids.get(key) else {
                            // a string value we cannot value-index (the key
                            // is not a term field of the writer schema, or
                            // overflowed the field-id space): lookups on it
                            // may miss docs
                            self.partial_fields.insert(key.to_string());
                            continue;
                        };
                        if self.fts_fields.contains(key) {
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
                                write_composite(&mut self.scratch, token.as_bytes(), field_id);
                                push_term(
                                    &mut self.terms,
                                    &mut self.terms_bytes,
                                    &self.scratch,
                                    doc,
                                );
                            }
                        } else if value.len() > self.opts.max_raw_term_len {
                            // oversize raw value: skipped, so per-field
                            // lookups may miss docs — degrade to partial
                            self.partial_fields.insert(key.to_string());
                        } else {
                            // the empty string included: `""` is a value
                            // (distinct from null) and its fid-only composite
                            // key is valid, so `field = ''` answers from the
                            // index
                            write_composite(&mut self.scratch, value.as_bytes(), field_id);
                            push_term(&mut self.terms, &mut self.terms_bytes, &self.scratch, doc);
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
                    serde_json::Value::Number(number) => {
                        if self.fts_fields.contains(key) {
                            continue; // numbers have no tokens
                        }
                        let Some(&field_id) = self.term_field_ids.get(key) else {
                            continue;
                        };
                        let Some(text) = canonical_number_text(number) else {
                            continue; // ±Inf overflow text: value-less, like null
                        };
                        if text.len() + 1 > self.opts.max_raw_term_len {
                            self.partial_fields.insert(key.to_string());
                            continue;
                        }
                        push_numeric_term(
                            &mut self.terms,
                            &mut self.terms_bytes,
                            &mut self.scratch,
                            &mut self.tag_scratch,
                            &text,
                            field_id,
                            doc,
                        );
                    }
                    serde_json::Value::Bool(flag) => {
                        if self.fts_fields.contains(key) {
                            continue;
                        }
                        let Some(&field_id) = self.term_field_ids.get(key) else {
                            continue;
                        };
                        push_numeric_term(
                            &mut self.terms,
                            &mut self.terms_bytes,
                            &mut self.scratch,
                            &mut self.tag_scratch,
                            canonical_bool_text(*flag),
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
        let mut arrays: Vec<ArrowArrayRef> = Vec::with_capacity(docs_schema.fields().len());
        for field in docs_schema.fields() {
            let array: ArrowArrayRef = match field.name().as_str() {
                TIMESTAMP_COL_NAME => Arc::new(timestamps.clone()),
                SOURCE_COL_NAME => Arc::new(source.clone()),
                ORIGINAL_DATA_COL_NAME => match original {
                    Some(values) => Arc::new(values.clone()),
                    None => Arc::new(StringArray::new_null(timestamps.len())),
                },
                name => {
                    let column = cs_columns
                        .iter()
                        .find(|(cs_name, _)| cs_name == name)
                        .map(|(_, column)| column)
                        .ok_or_else(|| {
                            VixError::Writer(format!(
                                "push_docs_rows is missing column-store column {name:?}"
                            ))
                        })?;
                    array_cast_as(column, field)?
                }
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
                            write_composite(&mut self.scratch, token.as_bytes(), field_id);
                            push_term(&mut self.terms, &mut self.terms_bytes, &self.scratch, doc);
                        }
                    }
                } else {
                    let mut partial = false;
                    for row in 0..num_rows {
                        let Some(value) = strings.value(row) else {
                            continue;
                        };
                        if value.len() > self.opts.max_raw_term_len {
                            // Oversize raw value: skip it and flag the field
                            // as partially indexed (per-field lookups could
                            // silently miss the skipped value).
                            partial = true;
                            continue;
                        }
                        let doc = (first_doc + row as u64) as u32;
                        // the empty string included: `""` is a value
                        // (distinct from null) and its fid-only composite key
                        // is valid, so `field = ''` answers from the index
                        write_composite(&mut self.scratch, value.as_bytes(), field_id);
                        push_term(&mut self.terms, &mut self.terms_bytes, &self.scratch, doc);
                    }
                    if partial {
                        self.partial_fields.insert(field_name.clone());
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
                        // uniformity with the raw-term path
                        self.partial_fields.insert(field_name.clone());
                        continue;
                    }
                    let doc = (first_doc + row as u64) as u32;
                    push_numeric_term(
                        &mut self.terms,
                        &mut self.terms_bytes,
                        &mut self.scratch,
                        &mut self.tag_scratch,
                        &text,
                        field_id,
                        doc,
                    );
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
            write_composite(&mut self.scratch, name.as_bytes(), KEY_FIELD_ID);
            let postings = self.terms.entry(self.scratch.clone()).or_default();
            let all_valid = finite.is_none() && column.null_count() == 0;
            for row in 0..num_rows {
                let keep = match &finite {
                    Some(mask) => mask[row],
                    None => all_valid || column.is_valid(row),
                };
                if keep {
                    let doc = (first_doc + row as u64) as u32;
                    if postings.last() != Some(&doc) {
                        postings.push(doc);
                    }
                }
            }
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

    fn finish_inner(mut self) -> Result<(VixOutput, VixWriterStats)> {
        if let Some(error) = &self.init_error {
            return Err(VixError::Writer(error.clone()));
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
                (prebuilt.blobs, prebuilt.term_count, prebuilt.bloom)
            }
            None => {
                // Stream the globally sorted terms once through the sink,
                // which cuts postings row blocks by byte budget and
                // dictionary row groups by raw-term-byte budget. With spill
                // runs (a budget-crossing rebuild), the runs and the final
                // resident map k-way merge here in the same order — the
                // sink sees the identical term stream either way, so the
                // blobs are byte-identical to the unspilled path.
                let bloom_pairs: Vec<(u16, String)> = self
                    .opts
                    .bloom_field_names
                    .iter()
                    .filter_map(|n| self.term_field_ids.get(n).map(|id| (*id, n.clone())))
                    .collect();
                let mut sink = TermSink::new(self.opts.postings_chunk_bytes)
                    .with_bloom(crate::bloom::BloomHashAcc::from_pairs(bloom_pairs))
                    .with_plist_min_docs(self.opts.postings_plist_min_docs);
                // The sink owns the cell policy (see [`TermSink::push_ids`]):
                // dense elision first (a term in every doc keeps the empty
                // cell), then the out-of-row plist threshold, then inline.
                let mut emit = |key: &[u8], ids: Vec<u32>| -> Result<()> {
                    sink.push_ids(key, &ids, row_count)
                };
                let resident = std::mem::take(&mut self.terms);
                self.terms_bytes = 0;
                match self.term_spill.take() {
                    None => {
                        for (key, ids) in resident {
                            emit(&key, ids)?;
                        }
                    }
                    Some(spilled) => {
                        log::debug!(
                            "vix rebuild: k-way merging {} term spill runs + resident map",
                            spilled.run_count(),
                        );
                        let (runs, _spill_dir) = spilled.into_run_readers()?;
                        spill::merge_spilled_terms(runs, resident, |key, ids| emit(&key, ids))?;
                    }
                }
                write_index_blobs(vec![sink.into_parts()?], self.opts.encode_threads)?
            }
        };

        // Assemble the blobs appended after the streamed docs blob. Empty
        // dict/terms tables are omitted entirely; the reader treats a
        // missing `dict`/`terms` pair as "no terms". The `docs` blob always
        // exists — even for an empty file it defines the stored schema
        // (`_timestamp` + `_source` at minimum).
        let mut blobs: Vec<(&'static str, &'static str, Vec<u8>)> = Vec::new();
        let mut index_size = 0u64;
        if let Some(index) = index_blobs {
            index_size = (index.dict.len() + index.dict_blocks.len() + index.terms.len()) as u64;
            blobs.push((BLOB_TYPE_DICT, BLOB_TAG_DICT, index.dict));
            blobs.push((
                BLOB_TYPE_DICT_BLOCKS,
                BLOB_TAG_DICT_BLOCKS,
                index.dict_blocks,
            ));
            blobs.push((BLOB_TYPE_TERMS, BLOB_TAG_TERMS, index.terms));
            // The out-of-row postings region: RAW concatenated
            // `encode_record` bytes (pointer-addressed, deliberately not a
            // Vortex file), present only when at least one pointer cell
            // exists. Index data, so it counts into `index_size`.
            if let Some(plist) = index.plist {
                index_size += plist.len() as u64;
                blobs.push((BLOB_TYPE_PLIST, BLOB_TAG_PLIST, plist));
            }
        }
        // Per-file value blooms (byproduct of term emission, both paths).
        // Counted into index_size: the blob is index data, and file_list's
        // `index_size > 0` is the bloom-queue eligibility filter.
        let file_blooms = bloom_acc.build(self.opts.bloom_fpp);
        if !file_blooms.is_empty() {
            let bloom_blob = crate::bloom::serialize_file_blooms(&file_blooms)?;
            index_size += bloom_blob.len() as u64;
            blobs.push((BLOB_TYPE_BLOOM, BLOB_TAG_BLOOM, bloom_blob));
        }
        let (sink, docs_size) = encoder.join()?;
        // Zone table: one `(row_count, ts_min, ts_max)` entry per docs
        // row-block, folded over the stored `_timestamp` values windowed by
        // the same `rows_per_chunk` the docs strategy blocks on. Cheap (one
        // i64 pass, no blob read-back) and derived for EVERY finish — the
        // move-job build and the compactor merge both land here, so a merged
        // file re-derives it naturally.
        let zone_map = zone_folder.finish();

        let mut properties = vec![
            (PROP_VERSION.to_string(), VIX_FORMAT_VERSION.to_string()),
            (PROP_ROW_COUNT.to_string(), row_count.to_string()),
            (PROP_TERM_COUNT.to_string(), term_count.to_string()),
            (
                PROP_ROW_GROUP_SIZE.to_string(),
                self.opts.row_group_size.to_string(),
            ),
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
            // Stamped unconditionally: readers hard-error on an absent or
            // foreign key_layout instead of silently misreading the
            // field-major dictionary (container::require_supported_format).
            (PROP_KEY_LAYOUT.to_string(), KEY_LAYOUT_FID_V2.to_string()),
        ];
        // Plist capability marker: written IFF the feature was enabled.
        // Present ⇒ pointer cells may exist and `doc_count >= threshold`
        // selects them; absent ⇒ every postings cell is inline. Written
        // even when no term crossed the threshold (no `plist` blob then) —
        // capability, not blob presence, is what the reader dispatches on.
        if self.opts.postings_plist_min_docs > 0 {
            properties.push((
                PROP_PLIST_MIN_DOCS.to_string(),
                self.opts.postings_plist_min_docs.to_string(),
            ));
        }
        // Only non-empty files get a zone table (an empty file has no chunks
        // and its decode path already returns the empty result).
        if !zone_map.is_empty() {
            properties.push((PROP_ZONE_MAP.to_string(), serde_json::to_string(&zone_map)?));
        }
        let output = finish_streamed_container(sink, docs_size, properties, blobs)?;
        let stats = VixWriterStats {
            row_count,
            term_count,
            index_size,
            docs_size,
            min_ts,
            max_ts,
        };
        Ok((output, stats))
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

/// Streaming builder of the `_timestamp` zone table: one `(row_count,
/// ts_min, ts_max)` entry per `rows_per_chunk`-sized window of the stored
/// rows, in push order — the same windowing the `docs` strategy blocks the
/// blob on ([`docs_strategy`]/[`docs_rows_per_chunk`]), so an entry lines up
/// with a docs row-block. Folded from the stored `_timestamp` values (pinned
/// non-null `i64` in the docs schema) as the batches stream to the encoder —
/// no blob read-back, no batch retention.
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
}

impl ZoneMapFolder {
    fn new(rows_per_chunk: usize) -> Self {
        Self {
            rows_per_chunk,
            entries: Vec::new(),
            count: 0,
            ts_min: i64::MAX,
            ts_max: i64::MIN,
        }
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
        for &value in values.values() {
            self.ts_min = self.ts_min.min(value);
            self.ts_max = self.ts_max.max(value);
            self.count += 1;
            if self.count as usize == self.rows_per_chunk {
                self.entries.push((self.count, self.ts_min, self.ts_max));
                self.count = 0;
                self.ts_min = i64::MAX;
                self.ts_max = i64::MIN;
            }
        }
        Ok(())
    }

    fn finish(mut self) -> Vec<ZoneEntry> {
        if self.count > 0 {
            self.entries.push((self.count, self.ts_min, self.ts_max));
        }
        self.entries
    }
}

/// Rows per `docs`-blob chunk: the uncompressed-byte budget divided by the
/// average row's bytes, clamped to `[64, 65536]`. Computed over the sample
/// batches ([`DOCS_ENCODE_SAMPLE_BYTES`]) that lock the streaming encoder's
/// chunking.
///
/// The average uses arrow's in-memory size (buffers incl. offsets/validity)
/// as the uncompressed-bytes heuristic. The blob's chunks are the
/// decompression unit of a matched-row point read, so they follow this byte
/// budget instead of the data file's row-group row count (with ~KB `_source`
/// rows, a 128Ki-row chunk would make every point read decode hundreds of
/// MB). The floor is 64 rows so the budget governs wide rows too (a
/// 1024-row floor used to force ~4 MiB decodes for ~4 KiB rows regardless
/// of a smaller budget); vortex's own pipeline still coalesces sub-1 MiB
/// chunks up to ~1 MiB (multiples of this row count), which bounds the
/// effective decode unit from below. An empty file (schema-only blob)
/// keeps vortex's default chunking.
fn docs_rows_per_chunk(budget_bytes: usize, batches: &[RecordBatch]) -> usize {
    let budget_bytes = if budget_bytes == 0 {
        DEFAULT_DOCS_CHUNK_BYTES
    } else {
        budget_bytes
    };
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    if rows == 0 {
        return 0;
    }
    let bytes: usize = batches.iter().map(RecordBatch::get_array_memory_size).sum();
    let avg_row_bytes = (bytes / rows).max(1);
    (budget_bytes / avg_row_bytes).clamp(DOCS_CHUNK_MIN_ROWS, DOCS_CHUNK_MAX_ROWS)
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
/// `bytes` tracks the map's estimated resident size — the spill trigger
/// ([`crate::spill`]); the estimate is deliberately rough (key bytes +
/// [`spill::PER_TERM_OVERHEAD`] per entry + 4 per posting).
fn push_term(terms: &mut BTreeMap<Vec<u8>, Vec<u32>>, bytes: &mut usize, key: &[u8], doc: u32) {
    if let Some(postings) = terms.get_mut(key) {
        if postings.last() != Some(&doc) {
            postings.push(doc);
            *bytes += 4;
        }
    } else {
        terms.insert(key.to_vec(), vec![doc]);
        *bytes += key.len() + spill::PER_TERM_OVERHEAD + 4;
    }
}

/// Emit one tagged canonical numeric/bool value term: the token is
/// `\x01{canonical text}` (see [`crate::numeric`] for why the tag exists).
/// `scratch` is the reusable composite-key buffer.
fn push_numeric_term(
    terms: &mut BTreeMap<Vec<u8>, Vec<u32>>,
    bytes: &mut usize,
    scratch: &mut Vec<u8>,
    tag_scratch: &mut Vec<u8>,
    canonical: &str,
    field_id: u16,
    doc: u32,
) {
    tag_scratch.clear();
    tag_scratch.reserve(canonical.len() + 1);
    tag_scratch.push(NUMERIC_TERM_TAG);
    tag_scratch.extend_from_slice(canonical.as_bytes());
    write_composite(scratch, tag_scratch, field_id);
    push_term(terms, bytes, scratch, doc);
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
    dict_blocks: Vec<u8>,
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
    plist: Vec<u8>,
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
            dict_blocks: Vec::new(),
            dict_meta: Vec::new(),
            first_key: Vec::new(),
            last_key: Vec::new(),
            term_count: 0,
            bloom: crate::bloom::BloomHashAcc::default(),
            bloom_key_scratch: Vec::new(),
            plist_min_docs: 0,
            plist: Vec::new(),
        }
    }

    /// Close the open dictionary block into the sink's blocks region.
    fn flush_dict_block(&mut self) -> Result<()> {
        if self.dict_block.is_empty() {
            return Ok(());
        }
        let offset = self.dict_blocks.len() as u64;
        let bytes = self.dict_block.finish();
        self.dict_blocks.extend_from_slice(&bytes);
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
        let cell = postings::encode_pointer_cell(self.plist.len() as u64, len);
        self.plist.extend_from_slice(record);
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
            let record = postings::encode_record(ids)?;
            return self.push_plist(key, ids.len() as u32, &record);
        }
        self.push(key, ids.len() as u32, &postings::encode(ids)?)
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
        Ok(TermSinkParts {
            term_batches: self.term_batches,
            dict_blocks: self.dict_blocks,
            dict_meta: self.dict_meta,
            first_key: self.first_key,
            last_key: self.last_key,
            term_count: self.term_count,
            bloom: self.bloom,
            plist_min_docs: self.plist_min_docs,
            plist: self.plist,
        })
    }
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
