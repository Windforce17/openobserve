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

//! `.vix` core-file reader and query evaluation.
//!
//! [`VixReader::open`] parses the puffin envelope from in-memory bytes;
//! [`VixReader::open_ranged`] parses it from a [`VixRangeSource`] instead
//! (one tail fetch for the puffin footer — two when the footer payload
//! exceeds the 64 KiB tail window). Both load only the dictionary
//! DIRECTORY at open — the three small `(first_ordinal, term_min,
//! term_max)` columns, KBs even for GB-scale files. The per-row-group
//! `fst` cells (multi-MB each) load LAZILY, point-read from the `dict`
//! blob the first time an operation touches their row group, and stay
//! resident on the reader ([`VixReader::memory_size`] grows accordingly).
//! Query evaluation resolves tokens to global term ordinals through the
//! per-row-group FSTs (pruned via the `term_min`/`term_max` directory where
//! the operation allows it — an exact-term probe loads at most ONE cell),
//! then point-reads the matching `postings` rows from the `terms` blob and
//! unions them into a per-document [`BooleanBuffer`]. On a ranged reader
//! those point reads fetch only the chunks the ordinals live in (plus the
//! blob's Vortex footer on the first access); the huge remainder of the
//! object is never downloaded. Whole-dictionary walks (`Contains`/`Regex`/
//! `Fuzzy`, [`VixReader::field_value_counts`], [`VixReader::for_each_term`])
//! batch-load every missing cell in one point-read scan — that is inherent
//! to those operations.
//!
//! Beyond the term index, core files expose:
//!
//! - the `docs` blob ([`VixReader::read_source`], [`VixReader::read_docs_column`],
//!   [`VixReader::read_docs_column_rows`]); the column-store reads ([`VixReader::read_column`], …)
//!   route to it. Ranged readers open the `docs` blob lazily — a pure index query never touches it,
//! - key terms ([`VixReader::key_exists`], [`VixReader::keys_with_prefix`],
//!   [`VixQuery::KeyExists`]) — value scans skip the reserved `\x00\xFF\xFF`-suffixed keys,
//! - dictionary-only per-value doc counts ([`VixReader::field_value_counts`], reconciled against
//!   the field's key term),
//! - dense elision: a term whose `doc_count` equals the file row count has an empty postings blob
//!   and decodes to the all-ones bitmap.
//!
//! Files whose `version` property is not the supported value are rejected
//! at open with a clear "unsupported .vix format" error; unknown extra
//! properties are ignored.
//!
//! The API is synchronous: every vortex read drives a fresh single-thread
//! runtime over the blob (in-memory reads, or ranged fetches awaited on
//! that runtime). Ranged entry points block on fetches, so they belong on
//! blocking threads — never on an async executor.

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
};

use arrow::{
    array::{
        Array, ArrayRef as ArrowArrayRef, BooleanBufferBuilder, Int64Array, StringArray,
        new_empty_array,
    },
    buffer::BooleanBuffer,
    compute::cast,
    datatypes::{DataType, Field, SchemaRef},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use levenshtein_automata::{Distance, LevenshteinAutomatonBuilder};
use tantivy_fst::{Automaton, IntoStreamer, Map, Regex, Streamer};

use crate::{
    container::{
        BlobHandle, FIELD_TYPE_CS, FIELD_TYPE_FTS, FIELD_TYPE_TERM, FieldEntry, PROP_FIELDS,
        PROP_PARTIAL_FIELDS, PROP_ROW_COUNT, PROP_ROW_GROUP_SIZE, PROP_TERM_COUNT, PROP_TOKENIZER,
        PROP_ZONE_MAP, RowSelection, VixContainer, ZoneEntry, blob_arrow_schema, column_binary,
        column_u64, parse_container, parse_container_ranged, require_supported_format, scan_blob,
        scan_blob_dict_column,
    },
    error::{Result, VixError},
    numeric::is_numeric_value_token,
    postings,
    query::{KEY_FIELD_ID, VixQuery, split_key, write_composite},
    source::VixRangeSource,
    writer::{NON_INDEXED_COLS, SOURCE_COL_NAME, TIMESTAMP_COL_NAME},
};

/// Exact per-value document counts of one field, as returned by
/// [`VixReader::field_value_counts`]: `(raw value bytes, doc_count)` pairs
/// in ascending byte order.
pub type FieldValueCounts = Vec<(Vec<u8>, u64)>;

/// Per-term callback of [`VixReader::for_each_term`]:
/// `(raw composite key, doc_count, sorted doc ids)`.
pub type TermVisitor<'a> = dyn FnMut(&[u8], u64, &[u32]) -> anyhow::Result<()> + 'a;

/// One chunk of a docs column in dictionary form, as returned by
/// [`VixReader::read_docs_column_dict`]: `codes[i]` indexes into `values`
/// (a null code = a null row). Value sets are per chunk — consecutive
/// chunks may differ, and an entry is not guaranteed to be referenced.
pub struct DocsDictChunk {
    /// Per-row dictionary code, `None` for null rows.
    pub codes: arrow::array::UInt64Array,
    /// The chunk's distinct values in their stored arrow type.
    pub values: ArrowArrayRef,
}

/// One physical `_timestamp` chunk's zone-map entry of the `docs` blob: the
/// chunk's row range `[row_offset, row_offset + row_count)` and its inclusive
/// `_timestamp` bounds `[ts_min, ts_max]`. The histogram/count/timestamp-range
/// fast paths let a whole chunk contribute to (or be excluded from) a time
/// bucket / range without decoding its values (DESIGN §6). Row offsets are
/// derived at open from the entries' running prefix sum. See
/// [`VixReader::zone_chunks`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ZoneChunk {
    /// Row id of the chunk's first document.
    pub row_offset: u64,
    /// Number of documents in the chunk.
    pub row_count: u64,
    /// Smallest `_timestamp` in the chunk (inclusive).
    pub ts_min: i64,
    /// Largest `_timestamp` in the chunk (inclusive).
    pub ts_max: i64,
}

/// One dictionary row group. The directory triple loads at open; the FST
/// cell loads lazily on first use (see [`VixReader::rg_fst`]).
pub(crate) struct RgEntry {
    pub(crate) first_ordinal: u64,
    /// Number of terms in this row group, derived from the directory
    /// (`next.first_ordinal - first_ordinal`; file `term_count` for the
    /// last group). The lazily loaded FST must match it exactly.
    pub(crate) term_count: u64,
    pub(crate) term_min: Vec<u8>,
    pub(crate) term_max: Vec<u8>,
    /// The row-group FST, point-read from the `dict` blob on first touch.
    fst: OnceLock<Map<Vec<u8>>>,
}

/// Reader over one `.vix` core file — held fully in memory
/// ([`VixReader::open`]) or fetched by ranges on demand
/// ([`VixReader::open_ranged`]).
pub struct VixReader {
    row_count: u64,
    term_count: u64,
    row_group_size: usize,
    fields: Vec<FieldEntry>,
    term_field_ids: HashMap<String, u16>,
    /// Field ids that own dictionary keys in this file (term/fts-typed
    /// entries, ascending). Any-field operations enumerate these for
    /// per-field point seeks/ranges (field-major keys cluster by fid).
    indexed_field_ids: Vec<u16>,
    partial_fields: HashSet<String>,
    /// Fields fts-marked in THIS file (token-indexed): the taint domain for
    /// any-field token queries over `partial_fields`.
    fts_fields: HashSet<String>,
    /// The `tokenizer` file property (identifies the token derivation of the
    /// file's fts terms; consulted only by the compaction index merge —
    /// a mismatch forces the rebuild, which re-tokenizes to the current
    /// [`crate::o2_tokenize`]).
    tokenizer: Option<String>,
    row_groups: Vec<RgEntry>,
    /// The `dict` blob, kept for lazy FST-cell point reads (`None` when the
    /// file has no terms).
    dict_blob: Option<BlobHandle>,
    terms_blob: Option<BlobHandle>,
    docs_blob: BlobHandle,
    /// The per-file value-bloom blob (`None` on files written before the
    /// capability existed).
    bloom_blob: Option<BlobHandle>,
    /// Arrow schema of the `docs` blob, loaded eagerly on in-memory readers
    /// and on first docs access on ranged readers.
    docs_schema: OnceLock<SchemaRef>,
    /// Resident size of the parsed directory/metadata, for external caches.
    base_memory: usize,
    /// Bytes of lazily loaded FST cells currently resident (grows
    /// monotonically; loaded cells stay for the reader's lifetime).
    fst_loaded_bytes: AtomicUsize,
    /// Per-chunk `_timestamp` zone table of the `docs` blob (`zone_map`
    /// property), in scan-iteration order with derived row offsets. `None`
    /// when the file carries no zone table (written before it landed, or a
    /// coverage mismatch made it untrustworthy) — the time-range fast paths
    /// then take the full-decode path. Loaded at open from the footer, so a
    /// fully-shortcuttable file answers a histogram/count with zero docs IO.
    zone_map: Option<Vec<ZoneChunk>>,
}

impl VixReader {
    /// Open a `.vix` file from its complete bytes.
    pub fn open(data: Bytes) -> anyhow::Result<Self> {
        Ok(Self::open_inner(data)?)
    }

    /// Open a `.vix` file over a ranged source, fetching only what queries
    /// touch: the puffin footer (one 64 KiB tail fetch, two for oversized
    /// footers) and the small dictionary DIRECTORY eagerly; the per-row-group
    /// `fst` cells and all `terms`/`docs` reads load lazily at chunk
    /// granularity. Blocks on fetches — call from a blocking thread, never on
    /// an async executor.
    pub fn open_ranged(source: Arc<dyn VixRangeSource>) -> anyhow::Result<Self> {
        let container = parse_container_ranged(&source)?;
        Ok(Self::from_container(container, false)?)
    }

    fn open_inner(data: Bytes) -> Result<Self> {
        let container = parse_container(&data)?;
        Self::from_container(container, true)
    }

    fn from_container(container: VixContainer, eager_docs_schema: bool) -> Result<Self> {
        let properties = &container.properties;
        require_supported_format(properties)?;
        let row_count = u64_prop(properties, PROP_ROW_COUNT)?;
        if row_count > u64::from(u32::MAX) {
            return Err(VixError::Malformed(format!(
                "row_count {row_count} exceeds the u32 doc-id space"
            )));
        }
        let term_count = u64_prop(properties, PROP_TERM_COUNT)?;
        let row_group_size = match properties.get(PROP_ROW_GROUP_SIZE) {
            None => 0,
            Some(raw) => raw.parse().map_err(|_| {
                VixError::Malformed(format!(
                    "property {PROP_ROW_GROUP_SIZE:?} is not an integer: {raw:?}"
                ))
            })?,
        };
        let fields: Vec<FieldEntry> =
            serde_json::from_str(required_prop(properties, PROP_FIELDS)?)?;
        let partial_fields: HashSet<String> =
            serde_json::from_str(required_prop(properties, PROP_PARTIAL_FIELDS)?)?;
        // The file's fts-marked field set, resolved once: any-field token
        // queries consult it to decide whether a partial field can actually
        // hide tokens (readers are memoized, so this is per-open, not
        // per-query).
        let fts_fields: HashSet<String> = fields
            .iter()
            .filter(|entry| entry.has_type(FIELD_TYPE_FTS))
            .map(|entry| entry.name.clone())
            .collect();
        let tokenizer = properties.get(PROP_TOKENIZER).cloned();
        let zone_map = parse_zone_map(properties.get(PROP_ZONE_MAP).map(String::as_str), row_count);

        // Entries typed `term` or `fts` own their positional field id
        // (their value terms / tokens carry it as the composite fid prefix).
        // Only `term`-typed entries enter the lookup map: fts fields
        // (tokens, no raw whole values) must not resolve for per-field
        // value lookups; their tokens are reachable through the any-field
        // scans, which never consult the map.
        let mut indexed_field_ids = Vec::new();
        let mut term_field_ids = HashMap::new();
        for (index, entry) in fields.iter().enumerate() {
            if entry.has_type(FIELD_TYPE_TERM) || entry.has_type(FIELD_TYPE_FTS) {
                let id = u16::try_from(index).map_err(|_| {
                    VixError::Malformed(format!(
                        "indexed field index {index} exceeds the u16 range"
                    ))
                })?;
                if id == KEY_FIELD_ID {
                    return Err(VixError::Malformed(
                        "field id 0xFFFF is reserved for key terms".to_string(),
                    ));
                }
                if entry.has_type(FIELD_TYPE_TERM) {
                    term_field_ids.insert(entry.name.clone(), id);
                }
                indexed_field_ids.push(id);
            }
        }

        let mut row_groups = Vec::new();
        let mut approx_memory = 1024usize;
        let mut dict_blob = None;
        if term_count > 0 {
            // Load only the small DIRECTORY columns (KBs even for GB-scale
            // files); the multi-MB `fst` cells are point-read lazily when an
            // operation first touches their row group.
            let dict = container
                .dict
                .ok_or_else(|| VixError::Malformed("missing dict blob".to_string()))?;
            if container.terms.is_none() {
                return Err(VixError::Malformed("missing terms blob".to_string()));
            }
            for batch in scan_blob(
                &dict,
                Some(&["first_ordinal", "term_min", "term_max"]),
                RowSelection::All,
            )? {
                let first_ordinals = column_u64(&batch, "first_ordinal")?;
                let term_mins = column_binary(&batch, "term_min")?;
                let term_maxs = column_binary(&batch, "term_max")?;
                for (row, &first_ordinal) in first_ordinals.iter().enumerate() {
                    let term_min = term_mins.value(row).to_vec();
                    let term_max = term_maxs.value(row).to_vec();
                    approx_memory += term_min.len() + term_max.len() + 192;
                    row_groups.push(RgEntry {
                        first_ordinal,
                        term_count: 0, // filled by validate_row_groups
                        term_min,
                        term_max,
                        fst: OnceLock::new(),
                    });
                }
            }
            validate_row_groups(&mut row_groups, term_count)?;
            dict_blob = Some(dict);
        }
        approx_memory += fields
            .iter()
            .map(|entry| entry.name.len() + 64)
            .sum::<usize>();

        // Core files always carry a `docs` blob (it defines the stored
        // schema even for zero-row files). In-memory readers load its schema
        // eagerly (zero IO); ranged readers defer it to the first docs
        // access so pure index queries never fetch docs bytes.
        let docs_blob = container
            .docs
            .ok_or_else(|| VixError::Malformed("missing docs blob".to_string()))?;

        let reader = Self {
            row_count,
            term_count,
            row_group_size,
            fields,
            term_field_ids,
            indexed_field_ids,
            partial_fields,
            fts_fields,
            tokenizer,
            row_groups,
            dict_blob,
            terms_blob: container.terms,
            docs_blob,
            bloom_blob: container.bloom,
            docs_schema: OnceLock::new(),
            base_memory: approx_memory,
            fst_loaded_bytes: AtomicUsize::new(0),
            zone_map,
        };
        if eager_docs_schema {
            reader.docs_schema_inner()?;
        }
        Ok(reader)
    }

    /// Number of documents in the indexed data file.
    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    /// The per-chunk `_timestamp` zone table of the `docs` blob — one
    /// [`ZoneChunk`] per physical chunk in scan-iteration order, partitioning
    /// `0..row_count` contiguously — or `None` when the file carries no
    /// trustworthy table (written before the zone table landed, or a coverage
    /// mismatch). The aggregation fast paths use it to fold whole chunks into
    /// time buckets / ranges without decoding their `_timestamp` values, and
    /// to skip chunks a filter proves empty; `None` means the caller must
    /// decode (`read_docs_column(_timestamp)` etc.).
    pub fn zone_chunks(&self) -> Option<&[ZoneChunk]> {
        self.zone_map.as_deref()
    }

    /// Parquet row-group size recorded at build time (`0` = unknown).
    pub fn row_group_size(&self) -> usize {
        self.row_group_size
    }

    /// Number of dictionary (term) row groups. Mostly useful for tests and
    /// diagnostics.
    pub fn term_row_group_count(&self) -> usize {
        self.row_groups.len()
    }

    /// The field id of a field whose **raw whole values** are term-indexed
    /// in this file, if any. fts-only fields (tokens, no raw values) return
    /// `None`: per-field value lookups on them must fall back to a scan.
    pub fn field_id(&self, name: &str) -> Option<u16> {
        self.term_field_ids.get(name).copied()
    }

    /// Whether per-field value lookups (exact/in/str_match/regex/prefix on a
    /// named field) can be answered from this file: the field's entry carries
    /// the `term` capability. fts-only fields (new files: `types:["fts"]`)
    /// lack it — conditions on them must take the per-file skip →
    /// add-filter-back path. Legacy files marking a field `["term","fts"]`
    /// keep the capability.
    pub fn has_term_capability(&self, name: &str) -> bool {
        self.term_field_ids.contains_key(name)
    }

    /// Whether the file knows the field at all (term-indexed *or*
    /// column-store). Use [`Self::field_id`] to test for term lookups.
    pub fn has_field(&self, name: &str) -> bool {
        self.fields.iter().any(|entry| entry.name == name)
    }

    /// Fields for which at least one value was skipped at build time
    /// (oversize values, or field-id overflow); term lookups on
    /// them may miss documents.
    pub fn partial_fields(&self) -> &HashSet<String> {
        &self.partial_fields
    }

    /// The file's fts-marked field names (token-indexed fields).
    pub fn fts_fields(&self) -> &HashSet<String> {
        &self.fts_fields
    }

    /// Evaluate a query into a bitmap with one bit per document
    /// (length == [`Self::row_count`]).
    pub fn eval(&self, query: &VixQuery) -> anyhow::Result<BooleanBuffer> {
        self.prefetch_query_fsts(query)?;
        Ok(self.eval_query(query)?)
    }

    /// Count matching documents. `All`, `Exact` and `KeyExists` avoid
    /// decoding postings (`row_count` property / `doc_count` column);
    /// everything else falls back to `eval(..).count_set_bits()`.
    pub fn count(&self, query: &VixQuery) -> anyhow::Result<u64> {
        self.prefetch_query_fsts(query)?;
        Ok(self.count_inner(query)?)
    }

    /// Batch-load every FST cell the leaves of `query` will touch — ONE
    /// dict point read per evaluation instead of one per leaf. Matters for
    /// compound ANDs on ranged readers (fewer fetch round trips), and on
    /// pre-lazy-layout files (whole-column fst chunk) it keeps the whole
    /// evaluation at a single decode of that chunk.
    fn prefetch_query_fsts(&self, query: &VixQuery) -> Result<()> {
        if self.row_groups.is_empty() {
            return Ok(());
        }
        let mut needed: Vec<usize> = Vec::new();
        self.collect_query_rg_needs(query, &mut needed);
        self.ensure_rg_fsts(&needed)
    }

    /// The row-group indices a query's leaves resolve through, computed from
    /// the directory alone (no FST access). Unknown fields contribute
    /// nothing — their leaves error (or match nothing) during evaluation as
    /// before.
    fn collect_query_rg_needs(&self, query: &VixQuery, out: &mut Vec<usize>) {
        match query {
            VixQuery::All | VixQuery::Nothing => {}
            VixQuery::And(subs) | VixQuery::Or(subs) => {
                for sub in subs {
                    self.collect_query_rg_needs(sub, out);
                }
            }
            VixQuery::Not(sub) => self.collect_query_rg_needs(sub, out),
            VixQuery::Exact { field, token } => {
                if let Some(field_id) = self.field_id(field)
                    && let Some(index) = self.rg_index_for_key(&self.composite(token, field_id))
                {
                    out.push(index);
                }
            }
            VixQuery::KeyExists { path } => {
                if let Some(index) =
                    self.rg_index_for_key(&self.composite(path.as_bytes(), KEY_FIELD_ID))
                {
                    out.push(index);
                }
            }
            // field-major: one point probe per indexed field — the
            // directory prunes each to at most one cell
            VixQuery::TokenAnyField { token } => {
                for &fid in &self.indexed_field_ids {
                    if let Some(index) = self.rg_index_for_key(&self.composite(token, fid)) {
                        out.push(index);
                    }
                }
            }
            VixQuery::Prefix { field, prefix } => {
                let fids: Vec<u16> = match field.as_deref().and_then(|f| self.field_id(f)) {
                    Some(fid) => vec![fid],
                    None => self.indexed_field_ids.clone(),
                };
                for fid in fids {
                    let (lower, upper) = Self::v2_prefix_range(fid, prefix);
                    self.rg_indices_for_range(&lower, Some((&upper, false)), out);
                }
            }
            VixQuery::Contains { field, .. } | VixQuery::Regex { field, .. } => {
                match field.as_deref().and_then(|f| self.field_id(f)) {
                    // a known field: only that field's cells
                    Some(fid) => {
                        let (lower, upper) = Self::v2_field_range(fid);
                        self.rg_indices_for_range(&lower, Some((&upper, false)), out);
                    }
                    None => out.extend(0..self.row_groups.len()),
                }
            }
            VixQuery::Fuzzy { .. } => {
                out.extend(0..self.row_groups.len());
            }
        }
    }

    /// The row group an exact key would live in per the directory, `None`
    /// when the directory proves the key absent.
    fn rg_index_for_key(&self, key: &[u8]) -> Option<usize> {
        let after = self
            .row_groups
            .partition_point(|rg| rg.term_min.as_slice() <= key);
        let index = after.checked_sub(1)?;
        (self.row_groups.get(index)?.term_max.as_slice() >= key).then_some(index)
    }

    /// The row groups whose `[term_min, term_max]` intersects
    /// `[lower, upper]`/`[lower, upper)` (the same pruning
    /// [`Self::scan_key_range`] applies).
    fn rg_indices_for_range(
        &self,
        lower: &[u8],
        upper: Option<(&[u8], bool)>,
        out: &mut Vec<usize>,
    ) {
        for (index, rg) in self.row_groups.iter().enumerate() {
            if rg.term_max.as_slice() < lower {
                continue;
            }
            if let Some((bound, inclusive)) = upper {
                let min = rg.term_min.as_slice();
                if (inclusive && min > bound) || (!inclusive && min >= bound) {
                    break;
                }
            }
            out.push(index);
        }
    }

    /// Bitmap of documents whose `_timestamp` lies in `[min_micros, max_micros)`
    /// — inclusive lower bound, exclusive upper bound, matching the query
    /// layer's time-range convention. Errors when the file has no
    /// `_timestamp` column store (and has rows).
    pub fn timestamp_range(
        &self,
        min_micros: i64,
        max_micros: i64,
    ) -> anyhow::Result<BooleanBuffer> {
        Ok(self.timestamp_range_inner(min_micros, max_micros)?)
    }

    /// Read one column-store column across all documents (from the `docs`
    /// blob).
    pub fn read_column(&self, name: &str) -> anyhow::Result<ArrowArrayRef> {
        Ok(self.read_column_inner(name)?)
    }

    /// The arrow schema of the `docs` blob: `_timestamp` first, then the
    /// column-store fields with their stored types, then `_source` and (when
    /// present) `_original`. On a ranged reader the first call opens the
    /// docs blob (its footer is fetched and cached); in-memory readers
    /// resolved it at open.
    pub fn docs_schema(&self) -> anyhow::Result<SchemaRef> {
        Ok(self.docs_schema_inner()?)
    }

    /// Rough resident size of the parsed reader in bytes. GROWS as row-group
    /// FSTs load lazily (loaded cells stay resident for the reader's
    /// lifetime) — external caches should re-read it when re-touching an
    /// entry.
    pub fn memory_size(&self) -> usize {
        self.base_memory + self.fst_loaded_bytes.load(Ordering::Relaxed)
    }

    /// Load (in one batched point-read) the FST cells of the given row-group
    /// indices that are not yet resident. Loaded cells are cached on the
    /// reader; concurrent loads of the same cell race benignly (first
    /// writer wins, sizes are counted once).
    ///
    /// Retention is strictly per requested cell — deliberately ALSO on files
    /// without per-cell dict chunks (pre-lazy layout: the whole `fst` column
    /// is one chunk, so any load decodes it whole). Retaining every decoded
    /// cell there would balloon each touched reader to its full dictionary
    /// (~40 MB on 200M-benchmark files; 1057 of them can never fit any sane
    /// reader-cache budget → permanent LRU thrash, measured 27 GB re-fetched
    /// per HOT query). Per-cell retention keeps readers proportional to what
    /// queries touch; the query-level prefetch already batches one
    /// evaluation's needs into a single decode of that chunk, and old files
    /// get per-cell chunks at their next compaction.
    pub(crate) fn ensure_rg_fsts(&self, indices: &[usize]) -> Result<()> {
        let mut missing: Vec<usize> = indices
            .iter()
            .copied()
            .filter(|&index| {
                self.row_groups
                    .get(index)
                    .is_some_and(|rg| rg.fst.get().is_none())
            })
            .collect();
        missing.sort_unstable();
        missing.dedup();
        if missing.is_empty() {
            return Ok(());
        }
        let dict_blob = self
            .dict_blob
            .as_ref()
            .ok_or_else(|| VixError::Malformed("missing dict blob".to_string()))?;
        let rows: Vec<u64> = missing.iter().map(|&index| index as u64).collect();
        let batches = scan_blob(dict_blob, Some(&["fst"]), RowSelection::Indices(rows))?;
        let mut requested = missing.iter().copied();
        for batch in &batches {
            let fsts = column_binary(batch, "fst")?;
            for row in 0..fsts.len() {
                let rg_index = requested.next().ok_or_else(|| {
                    VixError::Malformed(
                        "dict fst point read returned more rows than requested".to_string(),
                    )
                })?;
                let rg = &self.row_groups[rg_index];
                if rg.fst.get().is_some() {
                    continue; // lost a benign race to a concurrent load
                }
                let raw = fsts.value(row).to_vec();
                let raw_len = raw.len();
                let fst = Map::from_bytes(raw)
                    .map_err(|e| VixError::Malformed(format!("row-group fst: {e}")))?;
                if fst.len() as u64 != rg.term_count {
                    return Err(VixError::Malformed(format!(
                        "row group {rg_index} fst holds {} keys, the directory says {}",
                        fst.len(),
                        rg.term_count
                    )));
                }
                if rg.fst.set(fst).is_ok() {
                    self.fst_loaded_bytes
                        .fetch_add(raw_len + 64, Ordering::Relaxed);
                }
            }
        }
        if requested.next().is_some() {
            return Err(VixError::Malformed(
                "dict fst point read returned fewer rows than requested".to_string(),
            ));
        }
        Ok(())
    }

    /// The FST of one row group, loading it first when needed.
    pub(crate) fn rg_fst(&self, index: usize) -> Result<&Map<Vec<u8>>> {
        if self.row_groups[index].fst.get().is_none() {
            self.ensure_rg_fsts(&[index])?;
        }
        Ok(self.row_groups[index]
            .fst
            .get()
            .expect("ensured just above"))
    }

    /// The raw `tokenizer` file property (merge compatibility checks).
    pub(crate) fn tokenizer_prop(&self) -> Option<&str> {
        self.tokenizer.as_deref()
    }

    /// The parsed `fields` property entries, in field-id order.
    pub(crate) fn field_entries(&self) -> &[FieldEntry] {
        &self.fields
    }

    /// The dictionary row groups (compaction index merge).
    pub(crate) fn term_row_groups(&self) -> &[RgEntry] {
        &self.row_groups
    }

    /// The `terms` blob handle, when the file has any terms.
    pub(crate) fn terms_blob_handle(&self) -> Option<&BlobHandle> {
        self.terms_blob.as_ref()
    }

    /// Number of composite terms in the file (`term_count` property).
    pub fn term_count(&self) -> u64 {
        self.term_count
    }

    /// Whether this file carries a per-file value-bloom blob.
    pub fn has_file_blooms(&self) -> bool {
        self.bloom_blob.is_some()
    }

    /// The term-dictionary field id of `name`, when the field is
    /// value-indexed in this file (the bloom backfill's key for filtering
    /// composite keys by field).
    pub fn term_field_id(&self, name: &str) -> Option<u16> {
        self.term_field_ids.get(name).copied()
    }

    /// Parse the per-file value blooms (`bloom` blob) if present. On ranged
    /// readers this fetches the whole blob (small: megabytes at trace-scale
    /// cardinality) in one ranged read.
    ///
    /// A blob that FETCHES but does not PARSE is file-shaped corruption: the
    /// error carries the [`crate::bloom::UnbuildableFile`] marker so retry
    /// queues stop re-queuing it. Fetch failures stay unmarked (transient).
    pub fn file_blooms(&self) -> anyhow::Result<Option<Vec<crate::bloom::FileBloom>>> {
        let Some(handle) = self.bloom_blob.as_ref() else {
            return Ok(None);
        };
        let bytes = match handle {
            BlobHandle::Mem(b) => b.clone(),
            BlobHandle::Ranged(r) => {
                crate::source::block_fetch(r.source.as_ref(), r.range.clone())?
            }
        };
        Ok(Some(
            crate::bloom::parse_file_blooms(&bytes).map_err(unbuildable)?,
        ))
    }

    /// Stream every composite term of the file in ordinal (== key) order:
    /// `on_term(raw composite key, doc_count, sorted doc ids)`. Dense-elided
    /// postings are expanded to the explicit `0..row_count` list. The whole
    /// `terms` table is decoded sequentially — this is a verification /
    /// diagnostics API (differential merge tests, index dumps), not a query
    /// path.
    pub fn for_each_term(&self, on_term: &mut TermVisitor<'_>) -> anyhow::Result<()> {
        self.for_each_term_inner(on_term)
    }

    /// The walk's own consistency checks (`doc_count` vs `row_count`, terms
    /// rows vs dictionary keys, dense-elision shape, coverage) are pure
    /// functions of the file bytes: their failures carry the
    /// [`crate::bloom::UnbuildableFile`] marker so retry queues can tell a
    /// corrupt file from a transient fetch failure (which stays unmarked).
    fn for_each_term_inner(&self, on_term: &mut TermVisitor<'_>) -> anyhow::Result<()> {
        if self.term_count == 0 {
            return Ok(());
        }
        let terms_blob = self.terms_blob.as_ref().ok_or_else(|| {
            unbuildable(VixError::Malformed("missing terms blob".to_string()))
        })?;
        // The `terms` rows are written in global ordinal order, and streaming
        // the row-group FSTs in directory order yields keys in exactly that
        // order — zip the two streams.
        let batches = scan_blob(
            terms_blob,
            Some(&["doc_count", "postings"]),
            RowSelection::All,
        )?;
        let mut columns = Vec::with_capacity(batches.len());
        for batch in &batches {
            columns.push((
                column_u64(batch, "doc_count")?,
                column_binary(batch, "postings")?,
            ));
        }
        let (mut batch_index, mut row_index) = (0usize, 0usize);
        let mut ids: Vec<u32> = Vec::new();
        let mut seen = 0u64;
        // a full-dictionary walk touches every FST: batch-load the missing
        // cells in one point-read scan
        self.ensure_rg_fsts(&(0..self.row_groups.len()).collect::<Vec<_>>())?;
        for index in 0..self.row_groups.len() {
            let mut stream = self.rg_fst(index)?.stream();
            while let Some((key, _)) = stream.next() {
                while batch_index < columns.len() && row_index >= columns[batch_index].0.len() {
                    batch_index += 1;
                    row_index = 0;
                }
                let Some((doc_counts, postings)) = columns.get(batch_index) else {
                    return Err(unbuildable(VixError::Malformed(
                        "terms table has fewer rows than dictionary keys".to_string(),
                    )));
                };
                let doc_count = doc_counts[row_index];
                // `doc_count` is file data and sizes the reserve below:
                // validate it BEFORE it can turn one corrupt cell into a
                // multi-gigabyte allocation (which aborts the process
                // instead of unwinding), exactly as the union path does.
                if doc_count > self.row_count {
                    return Err(unbuildable(VixError::Malformed(format!(
                        "doc_count {doc_count} exceeds row_count {}",
                        self.row_count
                    ))));
                }
                let blob = postings.value(row_index);
                row_index += 1;
                ids.clear();
                if blob.is_empty() && doc_count > 0 {
                    if doc_count != self.row_count {
                        return Err(unbuildable(VixError::Malformed(format!(
                            "empty postings blob for a term with doc_count {doc_count} != \
                             row_count {} (not dense-elided, so corrupt)",
                            self.row_count
                        ))));
                    }
                    ids.extend(0..self.row_count as u32);
                } else {
                    ids.reserve(doc_count as usize);
                    // the postings blob is already resident: decode failures
                    // are corrupt bytes, not IO
                    postings::decode_each(blob, doc_count as usize, |doc| {
                        ids.push(doc);
                        Ok(())
                    })
                    .map_err(unbuildable)?;
                }
                on_term(key, doc_count, &ids).map_err(VixError::Callback)?;
                seen += 1;
            }
        }
        if seen != self.term_count {
            return Err(unbuildable(VixError::Malformed(format!(
                "terms enumeration covered {seen} keys, expected {}",
                self.term_count
            ))));
        }
        Ok(())
    }

    /// Load (or return the cached) docs-blob schema.
    fn docs_schema_inner(&self) -> Result<SchemaRef> {
        if let Some(schema) = self.docs_schema.get() {
            return Ok(Arc::clone(schema));
        }
        let schema = Arc::new(blob_arrow_schema(&self.docs_blob)?);
        for required in [TIMESTAMP_COL_NAME, SOURCE_COL_NAME] {
            if schema.field_with_name(required).is_err() {
                return Err(VixError::Malformed(format!(
                    "docs blob is missing the {required:?} column"
                )));
            }
        }
        // Concurrent loads race benignly: first writer wins, both computed
        // the same schema from the same immutable blob.
        let _ = self.docs_schema.set(Arc::clone(&schema));
        Ok(Arc::clone(self.docs_schema.get().unwrap_or(&schema)))
    }

    /// Every value-indexed field name of this file, in field-id order:
    /// `fields` property entries carrying the `term` **or** `fts` type (both
    /// own a field id; fts fields hold tokens instead of raw values). The
    /// compactor rebuilds its merged-file schema from this set, so fts
    /// fields must stay in it — dropping them would degrade their tokens to
    /// `partial_fields` after a merge.
    pub fn term_field_names(&self) -> Vec<&str> {
        self.fields
            .iter()
            .filter(|entry| entry.has_type(FIELD_TYPE_TERM) || entry.has_type(FIELD_TYPE_FTS))
            .map(|entry| entry.name.as_str())
            .collect()
    }

    /// Whether the file stores this field natively (readable via
    /// [`Self::read_column`] / [`Self::read_column_rows`]). The
    /// `_source`/`_original` columns are not column-store *fields*; read
    /// them via [`Self::read_source`] / [`Self::read_docs_column`].
    pub fn has_column_store_field(&self, name: &str) -> bool {
        self.fields
            .iter()
            .any(|entry| entry.name == name && entry.has_type(FIELD_TYPE_CS))
    }

    /// Read one column-store column at the given row indices only (vortex
    /// point reads). Indices are sorted and deduped internally; the returned
    /// array holds the values in ascending row order.
    pub fn read_column_rows(&self, name: &str, rows: &[u64]) -> anyhow::Result<ArrowArrayRef> {
        Ok(self.read_column_rows_inner(name, rows)?)
    }

    /// Read the `_source` strings of the given rows (vortex point reads via
    /// row indices). Indices are sorted and deduped internally; the returned
    /// array holds the strings in ascending row order.
    pub fn read_source(&self, row_ids: &[u64]) -> anyhow::Result<StringArray> {
        Ok(self.read_source_inner(row_ids)?)
    }

    /// Read one `docs`-blob column across all documents. Unlike
    /// [`Self::read_column`] this also reaches `_source`/`_original`.
    pub fn read_docs_column(&self, name: &str) -> anyhow::Result<ArrowArrayRef> {
        Ok(self.read_docs_column_inner(name)?)
    }

    /// Read one `docs`-blob column at the given row indices only (sorted and
    /// deduped internally, values in ascending row order).
    pub fn read_docs_column_rows(
        &self,
        name: &str,
        row_ids: &[u64],
    ) -> anyhow::Result<ArrowArrayRef> {
        Ok(self.read_docs_column_rows_inner(name, row_ids)?)
    }

    /// Read one `docs`-blob column in **dictionary form**, chunk by chunk:
    /// each chunk exposes per-row `codes` into its `values` array (null code
    /// = null row) instead of one materialized value per row. Chunks that
    /// are dictionary-encoded on disk (the norm for low-cardinality string
    /// columns) come back at their stored size; other encodings are
    /// dictionary-converted by vortex, costing about a canonical read.
    /// Group-by consumers count codes and stringify each distinct value
    /// once. Value sets are per chunk and may include unreferenced entries.
    pub fn read_docs_column_dict(&self, name: &str) -> anyhow::Result<Vec<DocsDictChunk>> {
        Ok(self.read_docs_column_dict_inner(name)?)
    }

    /// Bitmap of documents that have a non-null value at the flattened
    /// `path` (key terms; unknown paths yield the all-zeros bitmap).
    pub fn key_exists(&self, path: &str) -> anyhow::Result<BooleanBuffer> {
        Ok(self.key_exists_inner(path)?)
    }

    /// Whether ANY document of this file carries a (non-null) value at the
    /// flattened `path` — a key-term dictionary probe (directory prune plus
    /// at most one FST cell load; no postings IO). `Ok(false)` PROVES the
    /// path is absent (NULL) in every document: writers emit one key term
    /// per distinct path with any non-null value, for columns of every
    /// arrow type and regardless of term/fts/column-store marking. The
    /// internal columns the writer never key-indexes (`_timestamp`,
    /// `_o2_id`, `_original`, `_source`) report `true` — every document
    /// carries them.
    pub fn key_term_exists(&self, path: &str) -> anyhow::Result<bool> {
        if NON_INDEXED_COLS.contains(&path) || path == SOURCE_COL_NAME {
            return Ok(true);
        }
        Ok(self
            .lookup_exact(&self.composite(path.as_bytes(), KEY_FIELD_ID))?
            .is_some())
    }

    /// All indexed key paths starting with `prefix`, with their doc counts,
    /// in ascending path order (FST range scan over key terms; an empty
    /// prefix lists the file's whole path coverage).
    pub fn keys_with_prefix(&self, prefix: &str) -> anyhow::Result<Vec<(String, u64)>> {
        Ok(self.keys_with_prefix_inner(prefix)?)
    }

    /// Exact per-value document counts of `field`, straight from the term
    /// dictionary — `(raw value bytes, doc_count)` in ascending byte order.
    /// No postings and no docs blob are touched (only the `doc_count`
    /// column, which dense-elided terms still carry), so an unfiltered
    /// full-range `GROUP BY field` TopN/Distinct is answerable from this
    /// alone.
    ///
    /// Returns `Ok(None)` when the file cannot prove the counts are exact
    /// per-value counts, and the caller must fall back to the docs columns
    /// (or a scan):
    /// - the field is in `partial_fields` (skipped oversize values / field-id overflow),
    /// - the field is fts-marked in this file (its dictionary holds tokens, not raw values),
    /// - the raw-term doc counts do not sum to the field's key-term doc count — some docs carry
    ///   values the dictionary lacks (e.g. a field stored under a non-string type in this file has
    ///   key terms only). The empty string is a countable value: writers raw-index it, so `("",
    ///   doc_count)` appears like any other value.
    ///
    /// `Ok(Some(vec![]))` means the file provably contains **no** document
    /// with a value at `field` — an exact empty result.
    pub fn field_value_counts(&self, field: &str) -> anyhow::Result<Option<FieldValueCounts>> {
        Ok(self.field_value_counts_inner(field)?)
    }

    fn field_value_counts_inner(&self, field: &str) -> Result<Option<FieldValueCounts>> {
        // skipped values at build time: the dictionary misses documents
        if self.partial_fields.contains(field) {
            return Ok(None);
        }
        // tokens share the field-id space with raw values: an fts-marked
        // field's dictionary counts token incidences, not whole values
        if self
            .fields
            .iter()
            .any(|entry| entry.name == field && entry.has_type(FIELD_TYPE_FTS))
        {
            return Ok(None);
        }
        // documents carrying a (non-null) value at the field, per key term
        let field_docs = match self.lookup_exact(&self.composite(field.as_bytes(), KEY_FIELD_ID))? {
            Some(ordinal) => self.read_doc_count(ordinal)?,
            None => 0,
        };
        let Some(field_id) = self.field_id(field) else {
            // no value terms at all: exact iff no doc carries the field
            // (e.g. the field is stored under a non-string type here)
            return Ok((field_docs == 0).then(Vec::new));
        };

        // walk every row group's FST, keeping keys with this field's suffix;
        // row groups and FST streams are ascending, so values and ordinals
        // come out sorted (and unique — one term per distinct value).
        // TAGGED numeric/bool value terms are not string values: they are
        // excluded here, and their doc counts then surface as a
        // reconciliation shortfall below — a field with number-stored rows
        // correctly refuses the exact-counts fast path (mixed-type grouping
        // must go through the scan's typed projection).
        let mut values: Vec<Vec<u8>> = Vec::new();
        let mut ordinals: Vec<u64> = Vec::new();
        // field-major: the field's values are one contiguous range
        let (lower, upper) = Self::v2_field_range(field_id);
        self.scan_key_range(&lower, Some((&upper, false)), |key, ordinal| {
            if let Some((token, _)) = split_key(key)
                && !is_numeric_value_token(token)
            {
                values.push(token.to_vec());
                ordinals.push(ordinal);
            }
        })?;
        let counts = if ordinals.is_empty() {
            Vec::new()
        } else {
            self.read_doc_counts(&ordinals)?
        };
        // each doc has at most one raw value per field, so exact counts sum
        // to the key-term doc count; a shortfall means values the dictionary
        // lacks (values of another stored type, empty strings in pre-fix
        // files, ...) — empty-string terms written by current writers are
        // ordinary dictionary entries and reconcile like any other value
        if counts.iter().sum::<u64>() != field_docs {
            return Ok(None);
        }
        Ok(Some(values.into_iter().zip(counts).collect()))
    }

    fn read_source_inner(&self, rows: &[u64]) -> Result<StringArray> {
        let column = self.read_docs_column_rows_inner(SOURCE_COL_NAME, rows)?;
        let column = cast(&column, &DataType::Utf8)
            .map_err(|e| VixError::Malformed(format!("_source is not a string column: {e}")))?;
        let strings = column
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| VixError::Malformed("_source is not a string column".to_string()))?;
        if strings.null_count() > 0 {
            return Err(VixError::Malformed(
                "_source unexpectedly contains nulls".to_string(),
            ));
        }
        Ok(strings.clone())
    }

    fn key_exists_inner(&self, path: &str) -> Result<BooleanBuffer> {
        match self.lookup_exact(&self.composite(path.as_bytes(), KEY_FIELD_ID))? {
            None => Ok(BooleanBuffer::new_unset(self.row_count as usize)),
            Some(ordinal) => self.postings_union(vec![ordinal]),
        }
    }

    fn keys_with_prefix_inner(&self, prefix: &str) -> Result<Vec<(String, u64)>> {
        let prefix = prefix.as_bytes();
        let mut paths: Vec<String> = Vec::new();
        let mut ordinals: Vec<u64> = Vec::new();
        // field-major: key terms cluster at the 0xFFFF prefix; scan
        // [FFFF ++ prefix, FFFF ++ successor) — or to the end of the
        // keyspace for an empty/all-0xFF prefix (0xFFFF is the LAST fid,
        // so upper = None is exactly "to the end")
        let mut lower = vec![0xFF, 0xFF];
        lower.extend_from_slice(prefix);
        let upper = prefix_successor(prefix).map(|succ| {
            let mut bound = vec![0xFF, 0xFF];
            bound.extend_from_slice(&succ);
            bound
        });
        self.scan_key_range(
            &lower,
            upper.as_deref().map(|bound| (bound, false)),
            |key, ordinal| {
                if let Some((token, _)) = split_key(key)
                    && token.starts_with(prefix)
                {
                    paths.push(String::from_utf8_lossy(token).into_owned());
                    ordinals.push(ordinal);
                }
            },
        )?;
        if ordinals.is_empty() {
            return Ok(Vec::new());
        }
        // FST scan order is ascending and key terms are unique, so the
        // ordinals are already sorted and deduped — the point-read results
        // zip back positionally.
        let counts = self.read_doc_counts(&ordinals)?;
        Ok(paths.into_iter().zip(counts).collect())
    }

    /// The `docs` schema field of `name`.
    fn docs_field(&self, name: &str) -> Result<Field> {
        self.docs_schema_inner()?
            .field_with_name(name)
            .cloned()
            .map_err(|_| VixError::ColumnNotFound(name.to_string()))
    }

    fn read_docs_column_inner(&self, name: &str) -> Result<ArrowArrayRef> {
        let field = self.docs_field(name)?;
        if self.row_count == 0 {
            return Ok(new_empty_array(field.data_type()));
        }
        self.scan_column_all(&self.docs_blob, name)
    }

    fn read_docs_column_dict_inner(&self, name: &str) -> Result<Vec<DocsDictChunk>> {
        // existence check, with the usual ColumnNotFound error
        let _ = self.docs_field(name)?;
        if self.row_count == 0 {
            return Ok(Vec::new());
        }
        let chunks = scan_blob_dict_column(&self.docs_blob, name)?
            .into_iter()
            .map(|chunk| DocsDictChunk {
                codes: chunk.codes,
                values: chunk.values,
            })
            .collect::<Vec<_>>();
        let total: u64 = chunks.iter().map(|c| c.codes.len() as u64).sum();
        if total != self.row_count {
            return Err(VixError::Malformed(format!(
                "column {name:?} dictionary read returned {total} rows, expected {}",
                self.row_count
            )));
        }
        Ok(chunks)
    }

    fn read_docs_column_rows_inner(&self, name: &str, rows: &[u64]) -> Result<ArrowArrayRef> {
        let field = self.docs_field(name)?;
        if rows.is_empty() {
            return Ok(new_empty_array(field.data_type()));
        }
        self.check_row_bounds(rows)?;
        self.scan_column_rows(&self.docs_blob, name, rows)
    }

    fn read_column_inner(&self, name: &str) -> Result<ArrowArrayRef> {
        if !self.has_column_store_field(name) {
            return Err(VixError::ColumnNotFound(name.to_string()));
        }
        self.read_docs_column_inner(name)
    }

    fn read_column_rows_inner(&self, name: &str, rows: &[u64]) -> Result<ArrowArrayRef> {
        if !self.has_column_store_field(name) {
            return Err(VixError::ColumnNotFound(name.to_string()));
        }
        self.read_docs_column_rows_inner(name, rows)
    }

    fn check_row_bounds(&self, rows: &[u64]) -> Result<()> {
        if let Some(&max) = rows.iter().max()
            && max >= self.row_count
        {
            return Err(VixError::Malformed(format!(
                "row index {max} out of range (row_count {})",
                self.row_count
            )));
        }
        Ok(())
    }

    /// Scan one column of a stored blob across all rows and concatenate.
    fn scan_column_all(&self, blob: &BlobHandle, name: &str) -> Result<ArrowArrayRef> {
        let batches = scan_blob(blob, Some(&[name]), RowSelection::All)?;
        let column = concat_single_column(&batches, name)?;
        if column.len() as u64 != self.row_count {
            return Err(VixError::Malformed(format!(
                "column {name:?} has {} rows, expected {}",
                column.len(),
                self.row_count
            )));
        }
        Ok(column)
    }

    /// Point-read one column of a stored blob at the given rows (sorted and
    /// deduped here; the result is in ascending row order).
    fn scan_column_rows(
        &self,
        blob: &BlobHandle,
        name: &str,
        rows: &[u64],
    ) -> Result<ArrowArrayRef> {
        let mut unique = rows.to_vec();
        unique.sort_unstable();
        unique.dedup();
        let expected = unique.len();
        let batches = scan_blob(blob, Some(&[name]), RowSelection::Indices(unique))?;
        let column = concat_single_column(&batches, name)?;
        if column.len() != expected {
            return Err(VixError::Malformed(format!(
                "column {name:?} point read returned {} rows, expected {expected}",
                column.len()
            )));
        }
        Ok(column)
    }

    fn eval_query(&self, query: &VixQuery) -> Result<BooleanBuffer> {
        let len = self.row_count as usize;
        match query {
            VixQuery::All => Ok(BooleanBuffer::new_set(len)),
            VixQuery::And(subs) => self.eval_and(subs),
            VixQuery::Or(subs) => {
                let mut acc = BooleanBuffer::new_unset(len);
                for sub in subs {
                    acc = &acc | &self.eval_query(sub)?;
                }
                Ok(acc)
            }
            VixQuery::Not(sub) => Ok(!&self.eval_query(sub)?),
            leaf => {
                let ordinals = self.collect_ordinals(leaf)?;
                self.postings_union(ordinals)
            }
        }
    }

    /// Evaluate an AND with leaf short-circuiting.
    ///
    /// Leaf children resolve their term ordinals through the in-memory FSTs
    /// first — **zero postings IO**. If any leaf matches no term the
    /// intersection is provably empty and no postings are ever read (the
    /// dominant case for needle-in-haystack compounds: a per-file miss of the
    /// rare term must not pay the common term's postings decode). Remaining
    /// leaves evaluate rarest-first (fewest matched terms), AND-ing with an
    /// early exit as soon as the accumulator goes empty; composite children
    /// (`And`/`Or`/`Not`) evaluate last. An empty child list is `All`.
    fn eval_and(&self, subs: &[VixQuery]) -> Result<BooleanBuffer> {
        let len = self.row_count as usize;
        let mut leaves: Vec<Vec<u64>> = Vec::new();
        let mut composites: Vec<&VixQuery> = Vec::new();
        for sub in subs {
            match sub {
                // AND identity: contributes nothing
                VixQuery::All => {}
                VixQuery::And(_) | VixQuery::Or(_) | VixQuery::Not(_) => composites.push(sub),
                leaf => {
                    let ordinals = self.collect_ordinals(leaf)?;
                    if ordinals.is_empty() {
                        // this leaf matches no document: the AND is empty
                        return Ok(BooleanBuffer::new_unset(len));
                    }
                    leaves.push(ordinals);
                }
            }
        }
        // fewest matched terms first: the cheapest selectivity proxy that
        // needs no doc_count reads (needles resolve to a single term)
        leaves.sort_by_key(Vec::len);
        let mut acc: Option<BooleanBuffer> = None;
        for ordinals in leaves {
            let bitmap = self.postings_union(ordinals)?;
            let next = match &acc {
                Some(prev) => prev & &bitmap,
                None => bitmap,
            };
            if next.count_set_bits() == 0 {
                return Ok(next);
            }
            acc = Some(next);
        }
        for sub in composites {
            let bitmap = self.eval_query(sub)?;
            let next = match &acc {
                Some(prev) => prev & &bitmap,
                None => bitmap,
            };
            if next.count_set_bits() == 0 {
                return Ok(next);
            }
            acc = Some(next);
        }
        Ok(acc.unwrap_or_else(|| BooleanBuffer::new_set(len)))
    }

    fn count_inner(&self, query: &VixQuery) -> Result<u64> {
        match query {
            VixQuery::All => Ok(self.row_count),
            VixQuery::And(_) | VixQuery::Or(_) | VixQuery::Not(_) => {
                Ok(self.eval_query(query)?.count_set_bits() as u64)
            }
            // any leaf: resolve its term ordinals (FST-only); a single
            // matched term is counted straight from the `doc_count` column
            // (exact — one term's postings hold distinct docs), multiple
            // terms may share documents and need the postings union
            leaf => {
                let mut ordinals = self.collect_ordinals(leaf)?;
                match ordinals.len() {
                    0 => Ok(0),
                    1 => self.read_doc_count(ordinals.pop().expect("one ordinal")),
                    _ => Ok(self.postings_union(ordinals)?.count_set_bits() as u64),
                }
            }
        }
    }

    /// Resolve a leaf query to the global ordinals of its matching terms.
    fn collect_ordinals(&self, query: &VixQuery) -> Result<Vec<u64>> {
        match query {
            // the provably-empty query: a leaf matching no term, so `eval`
            // yields the all-zeros bitmap, `count` yields 0, and the AND
            // evaluator short-circuits on it like on any missing needle
            VixQuery::Nothing => Ok(Vec::new()),
            VixQuery::Exact { field, token } => {
                let field_id = self.require_field_id(field)?;
                let key = self.composite(token, field_id);
                Ok(self.lookup_exact(&key)?.into_iter().collect())
            }
            VixQuery::KeyExists { path } => {
                let key = self.composite(path.as_bytes(), KEY_FIELD_ID);
                Ok(self.lookup_exact(&key)?.into_iter().collect())
            }
            // field-major: one exact seek per indexed field (key terms
            // excluded by construction — KEY_FIELD_ID is not in the set)
            VixQuery::TokenAnyField { token } => {
                let mut ordinals = Vec::new();
                for &fid in &self.indexed_field_ids {
                    if let Some(ordinal) = self.lookup_exact(&self.composite(token, fid))? {
                        ordinals.push(ordinal);
                    }
                }
                Ok(ordinals)
            }
            // field-major: per-field contiguous ranges (one range when the
            // field is known)
            VixQuery::Prefix { field, prefix } => {
                let field_filter = self.optional_field_id(field)?;
                let mut ordinals = Vec::new();
                let fids: Vec<u16> = match field_filter {
                    Some(fid) => vec![fid],
                    None => self.indexed_field_ids.clone(),
                };
                for fid in fids {
                    let (lower, upper) = Self::v2_prefix_range(fid, prefix);
                    self.scan_key_range(&lower, Some((&upper, false)), |key, ordinal| {
                        if let Some((token, _)) = split_key(key)
                            && token.starts_with(prefix)
                        {
                            ordinals.push(ordinal);
                        }
                    })?;
                }
                Ok(ordinals)
            }
            VixQuery::Contains {
                field,
                needle,
                case_insensitive,
            } => {
                let field_filter = self.optional_field_id(field)?;
                if *case_insensitive {
                    let needle = String::from_utf8_lossy(needle).to_lowercase();
                    self.scan_all_tokens(field_filter, |token| {
                        String::from_utf8_lossy(token)
                            .to_lowercase()
                            .contains(&needle)
                    })
                } else {
                    self.scan_all_tokens(field_filter, |token| contains_bytes(token, needle))
                }
            }
            VixQuery::Regex { field, pattern } => {
                let field_filter = self.optional_field_id(field)?;
                let regex = Regex::new(pattern)
                    .map_err(|e| VixError::InvalidQuery(format!("regex {pattern:?}: {e}")))?;
                self.scan_all_tokens(field_filter, |token| automaton_matches(&regex, token))
            }
            VixQuery::Fuzzy { token, distance } => {
                if *distance > 2 {
                    return Err(VixError::InvalidQuery(format!(
                        "fuzzy distance {distance} exceeds the supported maximum of 2"
                    )));
                }
                // transposition_cost_one = false mirrors the tantivy
                // FuzzyTermQuery usage this replaces.
                let dfa = LevenshteinAutomatonBuilder::new(*distance, false).build_dfa(token);
                self.scan_all_tokens(None, |token| matches!(dfa.eval(token), Distance::Exact(_)))
            }
            VixQuery::All | VixQuery::And(_) | VixQuery::Or(_) | VixQuery::Not(_) => {
                Err(VixError::InvalidQuery(
                    "internal: composite query treated as a term leaf".to_string(),
                ))
            }
        }
    }

    fn require_field_id(&self, name: &str) -> Result<u16> {
        self.field_id(name)
            .ok_or_else(|| VixError::FieldNotIndexed(name.to_string()))
    }

    fn optional_field_id(&self, field: &Option<String>) -> Result<Option<u16>> {
        field
            .as_ref()
            .map(|name| self.require_field_id(name))
            .transpose()
    }

    /// Build the field-major composite key of `(token, field_id)`
    /// (see [`crate::query::write_composite`]).
    fn composite(&self, token: &[u8], field_id: u16) -> Vec<u8> {
        let mut key = Vec::new();
        write_composite(&mut key, token, field_id);
        key
    }

    /// The key range covering every dictionary key of `field_id`:
    /// `[fid_be, (fid+1)_be)`. Real field ids never reach 0xFFFF
    /// (MAX_REAL_FIELD_ID caps them), so the +1 cannot overflow.
    fn v2_field_range(field_id: u16) -> ([u8; 2], [u8; 2]) {
        (field_id.to_be_bytes(), (field_id + 1).to_be_bytes())
    }

    /// The key range of tokens starting with `prefix` inside `field_id`:
    /// `[fid ++ prefix, fid ++ successor(prefix))`, or the whole field when
    /// the prefix has no successor (all-0xFF) or is empty.
    fn v2_prefix_range(field_id: u16, prefix: &[u8]) -> (Vec<u8>, Vec<u8>) {
        let fid = field_id.to_be_bytes();
        let mut lower = Vec::with_capacity(prefix.len() + 2);
        lower.extend_from_slice(&fid);
        lower.extend_from_slice(prefix);
        let upper = match prefix_successor(prefix) {
            Some(succ) if !prefix.is_empty() => {
                let mut upper = Vec::with_capacity(succ.len() + 2);
                upper.extend_from_slice(&fid);
                upper.extend_from_slice(&succ);
                upper
            }
            _ => Self::v2_field_range(field_id).1.to_vec(),
        };
        (lower, upper)
    }

    /// Point lookup of one composite key via the row-group directory: the
    /// directory prunes to at most ONE row group, whose FST is loaded on
    /// first touch.
    fn lookup_exact(&self, key: &[u8]) -> Result<Option<u64>> {
        let after = self
            .row_groups
            .partition_point(|rg| rg.term_min.as_slice() <= key);
        let Some(index) = after.checked_sub(1) else {
            return Ok(None);
        };
        let Some(rg) = self.row_groups.get(index) else {
            return Ok(None);
        };
        if rg.term_max.as_slice() < key {
            return Ok(None);
        }
        Ok(self
            .rg_fst(index)?
            .get(key)
            .map(|local| rg.first_ordinal + local))
    }

    /// Stream all keys in `[lower, upper]`/`[lower, upper)` over the row
    /// groups whose `[term_min, term_max]` intersects the range. The
    /// directory prunes first, then the intersecting FSTs load in one
    /// batched point read (narrow ranges — token/prefix probes — touch a
    /// single row group).
    fn scan_key_range(
        &self,
        lower: &[u8],
        upper: Option<(&[u8], bool)>,
        mut on_key: impl FnMut(&[u8], u64),
    ) -> Result<()> {
        let mut touched: Vec<usize> = Vec::new();
        for (index, rg) in self.row_groups.iter().enumerate() {
            if rg.term_max.as_slice() < lower {
                continue;
            }
            if let Some((bound, inclusive)) = upper {
                let min = rg.term_min.as_slice();
                // Row groups are sorted: once one starts past the upper
                // bound, all later ones do too.
                if (inclusive && min > bound) || (!inclusive && min >= bound) {
                    break;
                }
            }
            touched.push(index);
        }
        self.ensure_rg_fsts(&touched)?;
        for index in touched {
            let rg = &self.row_groups[index];
            let mut range = self.rg_fst(index)?.range().ge(lower);
            if let Some((bound, inclusive)) = upper {
                range = if inclusive {
                    range.le(bound)
                } else {
                    range.lt(bound)
                };
            }
            let mut stream = range.into_stream();
            while let Some((key, local)) = stream.next() {
                on_key(key, rg.first_ordinal + local);
            }
        }
        Ok(())
    }

    /// Walk every FST key, apply `matches` to the token part (keys without a
    /// valid composite suffix — key terms, and TAGGED numeric/bool value
    /// terms — are skipped) and collect matching ordinals. The tagged terms
    /// are excluded because every caller is a string-shaped scan
    /// (`Contains`/`Regex`/`Fuzzy` over raw values and fts tokens): a
    /// substring/pattern hit inside a canonical number text is not a string
    /// match (the scan-side `json_get_str` projection maps those rows to
    /// NULL). Numeric probes are exact lookups built via
    /// [`crate::numeric::numeric_value_token`] and never come through here.
    /// Whole-dictionary walk: every missing FST cell is batch-loaded first.
    fn scan_all_tokens(
        &self,
        field_filter: Option<u16>,
        mut matches: impl FnMut(&[u8]) -> bool,
    ) -> Result<Vec<u64>> {
        // a known field: walk only that field's contiguous key range
        // instead of the whole dictionary (field-major keys cluster by fid)
        if let Some(fid) = field_filter {
            let (lower, upper) = Self::v2_field_range(fid);
            let mut ordinals = Vec::new();
            self.scan_key_range(&lower, Some((&upper, false)), |key, ordinal| {
                if let Some((token, _)) = split_key(key)
                    && !is_numeric_value_token(token)
                    && matches(token)
                {
                    ordinals.push(ordinal);
                }
            })?;
            return Ok(ordinals);
        }
        let mut ordinals = Vec::new();
        self.ensure_rg_fsts(&(0..self.row_groups.len()).collect::<Vec<_>>())?;
        for (index, rg) in self.row_groups.iter().enumerate() {
            let mut stream = self.rg_fst(index)?.stream();
            while let Some((key, local)) = stream.next() {
                if let Some((token, field_id)) = split_key(key)
                    && field_id != KEY_FIELD_ID
                    && !is_numeric_value_token(token)
                    && matches(token)
                {
                    ordinals.push(rg.first_ordinal + local);
                }
            }
        }
        Ok(ordinals)
    }

    /// Point-read the postings of the given ordinals and union them into a
    /// per-document bitmap. Dense-elided terms (`doc_count == row_count`,
    /// empty blob) short-circuit to the all-ones bitmap.
    fn postings_union(&self, mut ordinals: Vec<u64>) -> Result<BooleanBuffer> {
        let len = self.row_count as usize;
        let mut builder = BooleanBufferBuilder::new(len);
        builder.append_n(len, false);
        if ordinals.is_empty() {
            return Ok(builder.finish());
        }
        for &ordinal in &ordinals {
            if ordinal >= self.term_count {
                return Err(VixError::OrdinalOutOfRange {
                    ordinal,
                    term_count: self.term_count,
                });
            }
        }
        let terms_blob = self
            .terms_blob
            .as_ref()
            .ok_or_else(|| VixError::Malformed("missing terms blob".to_string()))?;
        ordinals.sort_unstable();
        ordinals.dedup();
        let expected_rows = ordinals.len();

        let batches = scan_blob(
            terms_blob,
            Some(&["doc_count", "postings"]),
            RowSelection::Indices(ordinals),
        )?;
        let mut seen_rows = 0usize;
        let mut saw_dense = false;
        for batch in &batches {
            let doc_counts = column_u64(batch, "doc_count")?;
            let postings = column_binary(batch, "postings")?;
            for (row, &doc_count) in doc_counts.iter().enumerate() {
                if doc_count > self.row_count {
                    return Err(VixError::Malformed(format!(
                        "doc_count {doc_count} exceeds row_count {}",
                        self.row_count
                    )));
                }
                let blob = postings.value(row);
                if blob.is_empty() && doc_count > 0 {
                    if doc_count == self.row_count {
                        // Dense elision: the term is in every doc; the
                        // all-ones bitmap is synthesized below.
                        saw_dense = true;
                        continue;
                    }
                    return Err(VixError::Malformed(format!(
                        "empty postings blob for a term with doc_count {doc_count} != \
                         row_count {} (not dense-elided, so corrupt)",
                        self.row_count
                    )));
                }
                if saw_dense {
                    // The union is already all-ones; skip further decodes.
                    continue;
                }
                postings::decode_each(blob, doc_count as usize, |doc| {
                    if u64::from(doc) >= self.row_count {
                        return Err(VixError::Malformed(format!(
                            "postings doc id {doc} out of range (row_count {})",
                            self.row_count
                        )));
                    }
                    builder.set_bit(doc as usize, true);
                    Ok(())
                })?;
            }
            seen_rows += batch.num_rows();
        }
        if seen_rows != expected_rows {
            return Err(VixError::Malformed(format!(
                "terms point read returned {seen_rows} rows, expected {expected_rows}"
            )));
        }
        if saw_dense {
            return Ok(BooleanBuffer::new_set(len));
        }
        Ok(builder.finish())
    }

    /// Read the `doc_count` column of a single ordinal (count fast path).
    fn read_doc_count(&self, ordinal: u64) -> Result<u64> {
        let counts = self.read_doc_counts(&[ordinal])?;
        Ok(counts[0])
    }

    /// Point-read the `doc_count` column of the given ordinals, which must
    /// be ascending and unique; the counts come back positionally.
    fn read_doc_counts(&self, ordinals: &[u64]) -> Result<Vec<u64>> {
        for &ordinal in ordinals {
            if ordinal >= self.term_count {
                return Err(VixError::OrdinalOutOfRange {
                    ordinal,
                    term_count: self.term_count,
                });
            }
        }
        let terms_blob = self
            .terms_blob
            .as_ref()
            .ok_or_else(|| VixError::Malformed("missing terms blob".to_string()))?;
        let batches = scan_blob(
            terms_blob,
            Some(&["doc_count"]),
            RowSelection::Indices(ordinals.to_vec()),
        )?;
        let mut counts = Vec::with_capacity(ordinals.len());
        for batch in &batches {
            counts.extend(column_u64(batch, "doc_count")?);
        }
        if counts.len() != ordinals.len() {
            return Err(VixError::Malformed(format!(
                "doc_count point read returned {} rows, expected {}",
                counts.len(),
                ordinals.len()
            )));
        }
        Ok(counts)
    }

    fn timestamp_range_inner(&self, min_micros: i64, max_micros: i64) -> Result<BooleanBuffer> {
        let len = self.row_count as usize;
        if len == 0 {
            return Ok(BooleanBuffer::new_unset(0));
        }
        if let Some(chunks) = self.zone_map.as_deref() {
            return self.timestamp_range_zoned(chunks, min_micros, max_micros);
        }
        let column = self.read_column_inner(TIMESTAMP_COL_NAME)?;
        let values = timestamps_as_i64(&column)?;
        let mut builder = BooleanBufferBuilder::new(len);
        builder.append_n(len, false);
        for row in 0..len {
            if values.is_valid(row) {
                let ts = values.value(row);
                if ts >= min_micros && ts < max_micros {
                    builder.set_bit(row, true);
                }
            }
        }
        Ok(builder.finish())
    }

    /// Zone-map [`Self::timestamp_range_inner`]: chunks fully inside
    /// `[min_micros, max_micros)` set their whole row range without decoding,
    /// chunks fully outside contribute nothing (also no decode), and only the
    /// boundary chunks (a range edge cuts through them) have their
    /// `_timestamp` decoded — for those rows only, matching the decode path's
    /// inclusive-start/exclusive-end rule exactly. A row selection over the
    /// boundary rows makes vortex fetch/decode just those chunks.
    fn timestamp_range_zoned(
        &self,
        chunks: &[ZoneChunk],
        min_micros: i64,
        max_micros: i64,
    ) -> Result<BooleanBuffer> {
        let len = self.row_count as usize;
        let mut builder = BooleanBufferBuilder::new(len);
        builder.append_n(len, false);
        let mut boundary: Vec<u64> = Vec::new();
        for chunk in chunks {
            // fully outside the range: every row stays unset
            if chunk.ts_max < min_micros || chunk.ts_min >= max_micros {
                continue;
            }
            // fully inside [min, max): set the whole contiguous row range
            if chunk.ts_min >= min_micros && chunk.ts_max < max_micros {
                for row in chunk.row_offset..chunk.row_offset + chunk.row_count {
                    builder.set_bit(row as usize, true);
                }
                continue;
            }
            // a range edge cuts through the chunk: decode its rows
            boundary.extend(chunk.row_offset..chunk.row_offset + chunk.row_count);
        }
        if !boundary.is_empty() {
            let column = self.read_docs_column_rows_inner(TIMESTAMP_COL_NAME, &boundary)?;
            let values = timestamps_as_i64(&column)?;
            // `read_docs_column_rows_inner` returns values in ascending row
            // order; `boundary` is already ascending and unique, so `values`
            // aligns with it positionally.
            for (i, &row) in boundary.iter().enumerate() {
                if values.is_valid(i) {
                    let ts = values.value(i);
                    if ts >= min_micros && ts < max_micros {
                        builder.set_bit(row as usize, true);
                    }
                }
            }
        }
        Ok(builder.finish())
    }
}

/// Test-only shape of one dumped term: raw composite key, `doc_count`,
/// decoded doc-id set.
#[cfg(test)]
pub(crate) type DebugTerm = (Vec<u8>, u64, std::collections::BTreeSet<u32>);

#[cfg(test)]
impl VixReader {
    /// Test-only: every composite term of the file — raw key bytes,
    /// `doc_count` and the decoded doc-id set — in global ordinal order.
    /// The workhorse of extraction-parity assertions.
    pub(crate) fn debug_all_terms(&self) -> Result<Vec<DebugTerm>> {
        let mut out = Vec::new();
        self.ensure_rg_fsts(&(0..self.row_groups.len()).collect::<Vec<_>>())?;
        for (index, rg) in self.row_groups.iter().enumerate() {
            let mut stream = self.rg_fst(index)?.stream();
            while let Some((key, local)) = stream.next() {
                out.push((key.to_vec(), rg.first_ordinal + local));
            }
        }
        out.sort_by_key(|(_, ordinal)| *ordinal);
        let mut terms = Vec::with_capacity(out.len());
        for (key, ordinal) in out {
            let doc_count = self.read_doc_count(ordinal)?;
            let bits = self.postings_union(vec![ordinal])?;
            let docs = bits
                .iter()
                .enumerate()
                .filter_map(|(doc, set)| set.then_some(doc as u32))
                .collect();
            terms.push((key, doc_count, docs));
        }
        Ok(terms)
    }

    /// Test-only: the raw postings-blob byte length of one composite term,
    /// `None` when the term does not exist. Pass
    /// [`KEY_FIELD_ID`](crate::query::KEY_FIELD_ID) to address key terms.
    /// Lets tests assert dense elision (length 0) directly.
    pub(crate) fn debug_postings_len(&self, token: &[u8], field_id: u16) -> Result<Option<usize>> {
        let Some(ordinal) = self.lookup_exact(&self.composite(token, field_id))? else {
            return Ok(None);
        };
        let terms_blob = self
            .terms_blob
            .as_ref()
            .ok_or_else(|| VixError::Malformed("missing terms blob".to_string()))?;
        let batches = scan_blob(
            terms_blob,
            Some(&["postings"]),
            RowSelection::Indices(vec![ordinal]),
        )?;
        let mut result = None;
        for batch in &batches {
            let postings = column_binary(batch, "postings")?;
            for row in 0..postings.len() {
                result = Some(postings.value(row).len());
            }
        }
        Ok(result)
    }
}

/// Wrap a file-shaped (deterministic) failure with the
/// [`crate::bloom::UnbuildableFile`] marker: the file's own bytes are
/// inconsistent, so a retry over the same bytes can never succeed. Never
/// applied to fetch/IO failures — those stay retryable.
fn unbuildable(err: VixError) -> anyhow::Error {
    anyhow::Error::new(err).context(crate::bloom::UnbuildableFile)
}

fn required_prop<'p>(
    properties: &'p std::collections::BTreeMap<String, String>,
    key: &str,
) -> Result<&'p str> {
    properties
        .get(key)
        .map(String::as_str)
        .ok_or_else(|| VixError::Malformed(format!("missing file property {key:?}")))
}

fn u64_prop(properties: &std::collections::BTreeMap<String, String>, key: &str) -> Result<u64> {
    let raw = required_prop(properties, key)?;
    raw.parse()
        .map_err(|_| VixError::Malformed(format!("property {key:?} is not an integer: {raw:?}")))
}

/// Extract and concatenate one column from scan result batches.
fn concat_single_column(batches: &[RecordBatch], name: &str) -> Result<ArrowArrayRef> {
    let mut arrays: Vec<ArrowArrayRef> = Vec::with_capacity(batches.len());
    for batch in batches {
        arrays.push(
            batch
                .column_by_name(name)
                .cloned()
                .ok_or_else(|| VixError::Malformed(format!("blob is missing column {name:?}")))?,
        );
    }
    let refs: Vec<&dyn Array> = arrays.iter().map(AsRef::as_ref).collect();
    if refs.is_empty() {
        return Err(VixError::Malformed(format!(
            "column store returned no data for {name:?}"
        )));
    }
    Ok(arrow::compute::concat(&refs)?)
}

/// Cast a `_timestamp` column read to a non-owning `Int64Array` view (the
/// docs schema pins it to `i64`, but a merged/older file may store a
/// castable width).
fn timestamps_as_i64(column: &ArrowArrayRef) -> Result<Int64Array> {
    let column = cast(column, &DataType::Int64)
        .map_err(|e| VixError::Malformed(format!("_timestamp is not an i64 column: {e}")))?;
    Ok(column
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| VixError::Malformed("_timestamp is not an i64 column".to_string()))?
        .clone())
}

/// Parse the `zone_map` property (a JSON array of `[row_count, ts_min,
/// ts_max]` triples) into per-chunk [`ZoneChunk`]s with derived row offsets.
///
/// Returns `None` — falling the reader back to the full-decode path, always
/// correct — when the property is absent, or present but untrustworthy: a
/// non-positive count, `ts_min > ts_max`, or a total row count that does not
/// equal the file's `row_count` (a stale or corrupt table). A trustworthy but
/// empty table (`row_count == 0`) also yields `None`; the decode path already
/// returns the empty result for a zero-row file.
fn parse_zone_map(raw: Option<&str>, row_count: u64) -> Option<Vec<ZoneChunk>> {
    let raw = raw?;
    let entries: Vec<ZoneEntry> = match serde_json::from_str(raw) {
        Ok(entries) => entries,
        Err(e) => {
            log::warn!("vix: ignoring malformed zone_map property ({e}); using the decode path");
            return None;
        }
    };
    let mut chunks = Vec::with_capacity(entries.len());
    let mut offset: u64 = 0;
    for (count, ts_min, ts_max) in entries {
        if count == 0 || ts_min > ts_max {
            log::warn!("vix: ignoring inconsistent zone_map entry; using the decode path");
            return None;
        }
        chunks.push(ZoneChunk {
            row_offset: offset,
            row_count: count,
            ts_min,
            ts_max,
        });
        offset = offset.checked_add(count)?;
    }
    if offset != row_count || chunks.is_empty() {
        if offset != row_count {
            log::warn!(
                "vix: zone_map covers {offset} rows but the file has {row_count}; using the \
                 decode path"
            );
        }
        return None;
    }
    Some(chunks)
}

/// Validate the row-group directory — sorted, non-overlapping, contiguous
/// ordinal coverage of `0..term_count` — and derive each group's term count
/// from the consecutive `first_ordinal`s (the FSTs are not loaded here; each
/// cell is checked against its derived count when it lazily loads).
fn validate_row_groups(row_groups: &mut [RgEntry], term_count: u64) -> Result<()> {
    if row_groups.is_empty() {
        if term_count > 0 {
            return Err(VixError::Malformed(format!(
                "dictionary has no row groups but term_count is {term_count}"
            )));
        }
        return Ok(());
    }
    for index in 0..row_groups.len() {
        let first_ordinal = row_groups[index].first_ordinal;
        if index == 0 && first_ordinal != 0 {
            return Err(VixError::Malformed(format!(
                "row group 0 starts at ordinal {first_ordinal}, expected 0"
            )));
        }
        let next_ordinal = match row_groups.get(index + 1) {
            Some(next) => next.first_ordinal,
            None => term_count,
        };
        if next_ordinal <= first_ordinal {
            return Err(VixError::Malformed(format!(
                "row group {index} covers ordinals {first_ordinal}..{next_ordinal} (empty or \
                 unsorted directory)"
            )));
        }
        row_groups[index].term_count = next_ordinal - first_ordinal;
        if row_groups[index].term_min > row_groups[index].term_max {
            return Err(VixError::Malformed(format!(
                "row group {index} has term_min > term_max"
            )));
        }
        if index > 0 && row_groups[index - 1].term_max >= row_groups[index].term_min {
            return Err(VixError::Malformed(format!(
                "row groups {} and {index} overlap or are unsorted",
                index - 1
            )));
        }
    }
    Ok(())
}

/// Smallest byte string strictly greater than every string with `prefix`;
/// `None` when unbounded (empty or all-`0xFF` prefix).
fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut successor = prefix.to_vec();
    while let Some(last) = successor.last_mut() {
        if *last == 0xFF {
            successor.pop();
        } else {
            *last += 1;
            return Some(successor);
        }
    }
    None
}

/// Plain byte-substring search (empty needle matches everything).
fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() {
        return true;
    }
    if needle.len() > haystack.len() {
        return false;
    }
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

/// Run a [`tantivy_fst::Automaton`] over `input` (anchored full match).
fn automaton_matches<A: Automaton>(automaton: &A, input: &[u8]) -> bool {
    let mut state = automaton.start();
    for &byte in input {
        if !automaton.can_match(&state) {
            return false;
        }
        state = automaton.accept(&state, byte);
    }
    automaton.is_match(&state)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_successor_basics() {
        assert_eq!(prefix_successor(b"ab"), Some(b"ac".to_vec()));
        assert_eq!(prefix_successor(b"a\xFF"), Some(b"b".to_vec()));
        assert_eq!(prefix_successor(b"\xFF\xFF"), None);
        assert_eq!(prefix_successor(b""), None);
    }

    #[test]
    fn contains_bytes_basics() {
        assert!(contains_bytes(b"hello world", b"lo w"));
        assert!(contains_bytes(b"abc", b""));
        assert!(!contains_bytes(b"abc", b"abcd"));
        assert!(!contains_bytes(b"abc", b"bd"));
    }
}
