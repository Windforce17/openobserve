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
use tantivy_fst::{Automaton, Regex};

use crate::{
    container::{
        BlobHandle, DICT_LAYOUT_BLOCKS, FIELD_TYPE_BLOOM, FIELD_TYPE_CS, FIELD_TYPE_FTS,
        FIELD_TYPE_TERM, FieldEntry,
        PROP_COLUMNS, PROP_DICT_LAYOUT, PROP_FIELDS, PROP_OVERSIZE_SKIPS, PROP_PARTIAL_FIELDS,
        PROP_PLIST_MIN_DOCS, PROP_ROW_COUNT, PROP_ROW_GROUP_SIZE, PROP_ROW_ORDER, PROP_TERM_COUNT,
        PROP_TOKENIZER, PROP_ZONE_MAP, RowOrder, RowSelection, VixContainer, ZoneEntry,
        blob_arrow_schema, column_binary, column_u32, column_u64, parse_container,
        parse_container_ranged, require_supported_data_format, require_supported_index_format,
        scan_blob, scan_blob_dict_column, scan_blob_streaming,
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

/// A dense term's out-of-row postings opened for rank-based consumption:
/// windowed counts and chunk-bounded walks without materializing a bitmap.
/// Obtained via [`VixReader::single_term_plist_cursor`].
pub struct PlistCursor {
    record: Bytes,
    doc_count: u64,
}

impl PlistCursor {
    /// Total ids in the list (the term's `doc_count`).
    pub fn doc_count(&self) -> u64 {
        self.doc_count
    }

    /// How many ids are strictly below `target` — the postings rank at a
    /// row cut. Decodes at most one skip group plus the tail.
    pub fn rank(&self, target: u32) -> anyhow::Result<u64> {
        Ok(postings::rank_at(
            &self.record,
            self.doc_count as usize,
            target,
        )?)
    }

    /// Every id in `[start, end)`, ascending — decodes only the touched
    /// skip groups.
    pub fn for_each_in_range(
        &self,
        start: u32,
        end: u32,
        mut on_doc: impl FnMut(u32) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        Ok(postings::for_each_in_range(
            &self.record,
            self.doc_count as usize,
            start,
            end,
            |doc| {
                on_doc(doc).map_err(VixError::Callback)?;
                Ok(())
            },
        )?)
    }
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

/// Reader over one logical core file — the `.vix` DATA object plus its
/// optional `.vxi` INDEX sidecar — held fully in memory
/// ([`VixReader::open_with_index`]) or fetched by ranges on demand
/// ([`VixReader::open_ranged_with_index`]). Opened without a sidecar the
/// reader is docs-only: no term/bloom capability, "no usable index →
/// filter-back scan" semantics.
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
    /// Per-field oversize-skip allowance from the `oversize_skips` property
    /// (empty for legacy files): the dictionary-serve reconciliation adds it
    /// to the indexed sum, so files whose ONLY value shortfall is skipped
    /// oversize values keep serving (counts omit those values — the
    /// 2026-08-12 trade); any other shortfall still refuses.
    oversize_skips: HashMap<String, u64>,
    /// `false` = opened WITHOUT an index sidecar (#40/#42 index-off files
    /// have none; a caller may also open data-only): the dictionary proves
    /// nothing — `key_term_exists`' absence proof and every capability
    /// derived from it are VOID for this reader.
    index_enabled: bool,
    /// Fields fts-marked in THIS file (token-indexed): the taint domain for
    /// any-field token queries over `partial_fields`.
    fts_fields: HashSet<String>,
    /// The `tokenizer` file property (identifies the token derivation of the
    /// file's fts terms; consulted only by the compaction index merge —
    /// a mismatch forces the rebuild, which re-tokenizes to the current
    /// [`crate::o2_tokenize`]).
    tokenizer: Option<String>,
    /// The parsed dictionary block index (see [`crate::dict_blocks`]),
    /// fetched + parsed on first dictionary touch and resident for the
    /// reader's lifetime.
    dict_index: OnceLock<crate::dict_blocks::DictIndex>,
    /// The dictionary BLOCKS region handle (raw concatenated blocks).
    dict_blocks_blob: Option<BlobHandle>,
    /// Recently fetched dictionary blocks (ranged readers): block id ->
    /// bytes, FIFO-bounded. In-memory readers slice for free and bypass it.
    block_cache: std::sync::Mutex<(
        std::collections::HashMap<usize, Bytes>,
        std::collections::VecDeque<usize>,
    )>,
    /// The `dict` blob, kept for lazy FST-cell point reads (`None` when the
    /// file has no terms).
    dict_blob: Option<BlobHandle>,
    terms_blob: Option<BlobHandle>,
    docs_blob: BlobHandle,
    /// The per-file value-bloom blob (`None` on files written before the
    /// capability existed).
    bloom_blob: Option<BlobHandle>,
    /// The out-of-row postings region (`plist` blob): raw concatenated
    /// `encode_record` bytes, pointer-addressed. `None` when the file
    /// carries no pointer cells' bytes (pre-plist file, feature off, or no
    /// term crossed the threshold).
    plist_blob: Option<BlobHandle>,
    /// The `plist_min_docs` property: `> 0` ⇒ a non-dense term with
    /// `doc_count >= plist_min_docs` has a POINTER CELL (`[u64 LE offset]
    /// [u32 LE len]` into the plist blob) instead of an inline postings
    /// blob — the ONLY pointer-vs-inline discriminator, cell bytes are
    /// never sniffed. `0` ⇒ every cell is inline (pre-plist file).
    plist_min_docs: u32,
    /// Arrow schema of the `docs` blob, loaded eagerly on in-memory readers
    /// and on first docs access on ranged readers.
    docs_schema: OnceLock<SchemaRef>,
    /// Resident size of the parsed directory/metadata, for external caches.
    base_memory: usize,
    /// Bytes of lazily loaded FST cells currently resident (grows
    /// monotonically; loaded cells stay for the reader's lifetime).
    dict_loaded_bytes: AtomicUsize,
    /// Per-column present-row counts from the data object's `columns`
    /// property (`None` count = unknown, an M1 plain-name entry).
    column_presence: Vec<(String, Option<u64>)>,
    /// The H2 per-column chunk-stats blob of the data object (absent on
    /// pre-stats and empty files).
    stats_blob: Option<BlobHandle>,
    /// Lazily decoded per-column chunk stats (`stats` blob) for the M16
    /// stats-answered aggregation arms; `None` inside = no blob or
    /// undecodable (fail open to decode paths). One small fetch on ranged
    /// readers (the blob sits in the eager tail), then resident.
    decoded_stats: OnceLock<Option<crate::stats::FileColumnStats>>,
    /// Approximate resident bytes of the decoded stats table (the encoded
    /// blob length — same order as the parsed form), folded into
    /// [`Self::memory_size`] so the reader cache accounts for it.
    stats_loaded_bytes: AtomicUsize,
    /// Per-chunk `_timestamp` zone table of the `docs` blob (`zone_map`
    /// property), in scan-iteration order with derived row offsets. `None`
    /// when the file carries no zone table (written before it landed, or a
    /// coverage mismatch made it untrustworthy) — the time-range fast paths
    /// then take the full-decode path. Loaded at open from the footer, so a
    /// fully-shortcuttable file answers a histogram/count with zero docs IO.
    zone_map: Option<Vec<ZoneChunk>>,
    /// Physical row order of the `docs` blob (`row_order` property, #51c-c).
    /// Missing == [`RowOrder::TsDesc`] (every historical file is sorted).
    /// [`RowOrder::Concat`] disables the order-dependent fast paths for
    /// this file (first-set-bits top-N candidates and everything the
    /// callers derive from stored order).
    row_order: RowOrder,
    /// §4 REGION table of a concat file (`row_regions` property, validated):
    /// per-region row counts in stored order, each region internally
    /// `_timestamp` DESC. `None` on ts_desc files (whole file = one region)
    /// and on concat files without a proven decomposition.
    row_regions: Option<Vec<u64>>,
    /// §4: the file asserts the all-present-columns invariant
    /// (`columns_complete` property). `false` when absent (fail-open).
    columns_complete: bool,
}

impl VixReader {
    /// Open a core file from its complete DATA-object bytes, WITHOUT an
    /// index sidecar: the reader carries no term/bloom capability — every
    /// conditioned evaluation takes the "no usable index → filter-back
    /// scan" path, exactly like an index-off file.
    pub fn open(data: Bytes) -> anyhow::Result<Self> {
        Self::open_with_index(data, None)
    }

    /// Open a core file from its complete DATA-object bytes plus the
    /// optional complete `.vxi` INDEX-sidecar bytes — the two-source
    /// in-memory open. `index = None` ⟺ [`Self::open`].
    pub fn open_with_index(data: Bytes, index: Option<Bytes>) -> anyhow::Result<Self> {
        let container = parse_container(&data)?;
        let index_container = match &index {
            Some(bytes) => Some(parse_container(bytes)?),
            None => None,
        };
        Ok(Self::from_containers(container, index_container, true)?)
    }

    /// Open a core file over a ranged DATA source WITHOUT a sidecar (see
    /// [`Self::open`] for the semantics). Blocks on fetches — call from a
    /// blocking thread, never on an async executor.
    pub fn open_ranged(source: Arc<dyn VixRangeSource>) -> anyhow::Result<Self> {
        Self::open_ranged_with_index(source, None)
    }

    /// Open a core file over a ranged DATA source plus an optional ranged
    /// INDEX-sidecar source, fetching only what queries touch: each object's
    /// puffin footer (one 64 KiB tail fetch, two for oversized footers) is
    /// parsed at open; the small dictionary DIRECTORY, the per-row-group
    /// `fst` cells and all `terms`/`docs`/`bloom` reads load lazily at chunk
    /// granularity from their own source. Blocks on fetches — call from a
    /// blocking thread, never on an async executor.
    pub fn open_ranged_with_index(
        source: Arc<dyn VixRangeSource>,
        index: Option<Arc<dyn VixRangeSource>>,
    ) -> anyhow::Result<Self> {
        let container = parse_container_ranged(&source)?;
        let index_container = match &index {
            Some(source) => Some(parse_container_ranged(source)?),
            None => None,
        };
        Ok(Self::from_containers(container, index_container, false)?)
    }

    fn from_containers(
        container: VixContainer,
        index_container: Option<VixContainer>,
        eager_docs_schema: bool,
    ) -> Result<Self> {
        // DATA object: docs blob + data-descriptive properties.
        let properties = &container.properties;
        require_supported_data_format(properties)?;
        let row_count = u64_prop(properties, PROP_ROW_COUNT)?;
        if row_count > u64::from(u32::MAX) {
            return Err(VixError::Malformed(format!(
                "row_count {row_count} exceeds the u32 doc-id space"
            )));
        }
        let row_group_size = match properties.get(PROP_ROW_GROUP_SIZE) {
            None => 0,
            Some(raw) => raw.parse().map_err(|_| {
                VixError::Malformed(format!(
                    "property {PROP_ROW_GROUP_SIZE:?} is not an integer: {raw:?}"
                ))
            })?,
        };
        // Oversize-skip allowance: data-side since v3 (it records what the
        // DATA holds that the term index does not, surviving sidecar-only
        // rewrites); absent when nothing was skipped — an empty map changes
        // nothing.
        let oversize_skips: HashMap<String, u64> = match properties.get(PROP_OVERSIZE_SKIPS) {
            Some(raw) => serde_json::from_str(raw).map_err(|e| {
                VixError::Malformed(format!(
                    "property {PROP_OVERSIZE_SKIPS:?} is not a field-count map: {e}"
                ))
            })?,
            None => HashMap::new(),
        };
        let zone_map = parse_zone_map(properties.get(PROP_ZONE_MAP).map(String::as_str), row_count);
        let row_order = RowOrder::from_property(properties.get(PROP_ROW_ORDER).map(String::as_str));
        let row_regions = if row_order.is_ts_desc() {
            None
        } else {
            crate::container::parse_row_regions(
                properties.get(crate::container::PROP_ROW_REGIONS).map(String::as_str),
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
        let stats_blob = container.stats;

        // INDEX sidecar: term/bloom capability + index-descriptive
        // properties. Its ABSENCE is the #40/#42 "no usable index" state:
        // no term/fts/bloom capability, dictionary proofs void, column
        // routing synthesized from the data object's `columns` list.
        let index_enabled = index_container.is_some();
        let (fields, partial_fields, term_count, tokenizer, plist_min_docs, index_container) =
            match index_container {
                Some(index) => {
                    let index_props = &index.properties;
                    require_supported_index_format(index_props)?;
                    // Pairing guard: a sidecar carries the doc-id space of
                    // exactly one data object — a mismatched pair would make
                    // the postings misaddress the stored rows.
                    let index_rows = u64_prop(index_props, PROP_ROW_COUNT)?;
                    if index_rows != row_count {
                        return Err(VixError::Malformed(format!(
                            "index sidecar covers {index_rows} rows but the data object stores \
                             {row_count} — mispaired objects"
                        )));
                    }
                    let fields: Vec<FieldEntry> =
                        serde_json::from_str(required_prop(index_props, PROP_FIELDS)?)?;
                    let partial_fields: HashSet<String> =
                        serde_json::from_str(required_prop(index_props, PROP_PARTIAL_FIELDS)?)?;
                    let term_count = u64_prop(index_props, PROP_TERM_COUNT)?;
                    let tokenizer = index_props.get(PROP_TOKENIZER).cloned();
                    // Out-of-row postings capability: the property present ⇒
                    // pointer cells may exist and `doc_count >=
                    // plist_min_docs` selects them; absent ⇒ every cell is
                    // inline. The blob itself may legitimately be absent even
                    // with the property set (no term crossed the threshold),
                    // so its existence is checked only when a pointer cell is
                    // actually resolved.
                    let plist_min_docs: u32 = match index_props.get(PROP_PLIST_MIN_DOCS) {
                        None => 0,
                        Some(raw) => raw.parse().map_err(|_| {
                            VixError::Malformed(format!(
                                "property {PROP_PLIST_MIN_DOCS:?} is not an integer: {raw:?}"
                            ))
                        })?,
                    };
                    (
                        fields,
                        partial_fields,
                        term_count,
                        tokenizer,
                        plist_min_docs,
                        Some(index),
                    )
                }
                None => {
                    // No sidecar: synthesize column-store-only field entries
                    // from the data object's docs-column list, which is
                    // exactly what an index-off file's `fields` property
                    // carried before the split (no term/fts/bloom types, so
                    // no capability is ever claimed). Entries are
                    // `[name, present_rows]` pairs since the H2 stats
                    // extension (plain names on M1 files).
                    let columns =
                        crate::stats::parse_columns_prop(required_prop(properties, PROP_COLUMNS)?)?;
                    let fields = columns
                        .into_iter()
                        .map(|(name, _)| FieldEntry {
                            name,
                            types: vec![FIELD_TYPE_CS.to_string()],
                        })
                        .collect();
                    (fields, HashSet::new(), 0, None, 0, None)
                }
            };
        // The file's fts-marked field set, resolved once: any-field token
        // queries consult it to decide whether a partial field can actually
        // hide tokens (readers are memoized, so this is per-open, not
        // per-query).
        let fts_fields: HashSet<String> = fields
            .iter()
            .filter(|entry| entry.has_type(FIELD_TYPE_FTS))
            .map(|entry| entry.name.clone())
            .collect();

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

        let mut approx_memory = 1024usize;
        let mut dict_blob = None;
        let mut dict_blocks_blob = None;
        let mut terms_blob = None;
        let mut bloom_blob = None;
        let mut plist_blob = None;
        if let Some(index) = index_container {
            if term_count > 0 {
                // The dictionary is the BLOCK layout — the only readable
                // layout. Monolithic-FST files were retired without read
                // support (ENGINE-BACKLOG #18): absent/foreign layouts
                // hard-error here.
                match index.properties.get(PROP_DICT_LAYOUT).map(String::as_str) {
                    Some(DICT_LAYOUT_BLOCKS) => {}
                    other => {
                        return Err(VixError::Malformed(format!(
                            "unsupported dict layout {other:?}: this reader only supports \
                             {DICT_LAYOUT_BLOCKS:?} (pre-block dictionaries were retired)",
                        )));
                    }
                }
                dict_blob = Some(
                    index
                        .dict
                        .ok_or_else(|| VixError::Malformed("missing dict blob".to_string()))?,
                );
                dict_blocks_blob = Some(index.dict_blocks.ok_or_else(|| {
                    VixError::Malformed("missing dict_blocks blob".to_string())
                })?);
                if index.terms.is_none() {
                    return Err(VixError::Malformed("missing terms blob".to_string()));
                }
            }
            terms_blob = index.terms;
            bloom_blob = index.bloom;
            plist_blob = index.plist;
        }
        approx_memory += fields
            .iter()
            .map(|entry| entry.name.len() + 64)
            .sum::<usize>();

        // Data objects always carry a `docs` blob (it defines the stored
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
            oversize_skips,
            index_enabled,
            fts_fields,
            tokenizer,
            dict_index: OnceLock::new(),
            dict_blocks_blob,
            block_cache: std::sync::Mutex::new((
                std::collections::HashMap::new(),
                std::collections::VecDeque::new(),
            )),
            dict_blob,
            terms_blob,
            docs_blob,
            bloom_blob,
            plist_blob,
            plist_min_docs,
            docs_schema: OnceLock::new(),
            base_memory: approx_memory,
            dict_loaded_bytes: AtomicUsize::new(0),
            column_presence,
            stats_blob,
            decoded_stats: OnceLock::new(),
            stats_loaded_bytes: AtomicUsize::new(0),
            zone_map,
            row_order,
            row_regions,
            columns_complete,
        };
        if eager_docs_schema {
            reader.docs_schema_inner()?;
        }
        Ok(reader)
    }

    /// Whether this reader carries a term index at all: `false` when it was
    /// opened without an index sidecar (#40/#42 index-off files have none),
    /// where dictionary-absence proofs are void and only condition-free
    /// evals are valid.
    pub fn has_index(&self) -> bool {
        self.index_enabled
    }

    /// Number of documents in the indexed data file.
    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    /// Physical row order of the stored docs rows (`row_order` property,
    /// #51c-c): [`RowOrder::TsDesc`] for every historical file (missing
    /// property) and every sorted writer output; [`RowOrder::Concat`] for
    /// concatenation-order merge outputs (and unknown future values — the
    /// fail-safe reading). Callers deriving ANYTHING from stored order
    /// (newest == first row, declared file sort order, first-set-bits
    /// candidates) must check this first.
    pub fn row_order(&self) -> RowOrder {
        self.row_order
    }

    /// §4: the file's internally-`_timestamp`-DESC row ranges, in stored
    /// order (the piecewise-order decomposition). One full-file range for a
    /// ts_desc file; the validated `row_regions` table for a concat file;
    /// `None` for a concat file without a proven decomposition (readers
    /// must not assume ANY order there — full sort / by-value paths only).
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

    /// Per-column present-row counts (`columns` property; `None` count =
    /// unknown — an M1 plain-name entry).
    pub fn column_presence(&self) -> &[(String, Option<u64>)] {
        &self.column_presence
    }

    /// §4: whether the file asserts the all-present-columns invariant —
    /// every field present in any row's `_source` is a docs column.
    /// `false` when absent (fail-open: no absent-column pruning).
    pub fn columns_complete(&self) -> bool {
        self.columns_complete
    }

    /// The file's spliceable stats (H2, DESIGN §4): the per-column chunk
    /// table from the `stats` blob plus the file-level presence counts.
    /// `Ok(None)` when the file carries NO stats blob (pre-stats file, or
    /// empty file) — such an input cannot feed a stats-preserving
    /// passthrough and must decode. On a ranged open this is ONE small
    /// fetch (the blob sits in the eager tail for typical files).
    pub fn spliceable_stats(&self) -> anyhow::Result<Option<crate::stats::SpliceableStats>> {
        let Some(blob) = &self.stats_blob else {
            return Ok(None);
        };
        let bytes = blob.bytes()?;
        let chunks = crate::stats::decode_stats_blob(&bytes)?;
        Ok(Some(crate::stats::SpliceableStats {
            presence: self.column_presence.clone(),
            chunks,
        }))
    }

    /// M16: the decoded per-column chunk-stats table (`stats` blob), fetched
    /// and parsed ONCE per reader (memoized readers keep it resident) —
    /// `None` when the file carries no readable stats (pre-stats/M1 files,
    /// empty files, undecodable blob: fail open, the aggregation arms
    /// decode instead). Rows align 1:1 with the zone table by construction;
    /// consumers must still gate on `chunks.len() == zone.len()` per column.
    pub fn column_chunk_stats(&self) -> Option<&crate::stats::FileColumnStats> {
        self.decoded_stats
            .get_or_init(|| {
                let blob = self.stats_blob.as_ref()?;
                let bytes = match blob.bytes() {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        log::debug!("vix: stats blob unreadable, no stats-answered arms: {e}");
                        return None;
                    }
                };
                match crate::stats::decode_stats_blob(&bytes) {
                    Ok(stats) => {
                        self.stats_loaded_bytes.store(bytes.len(), Ordering::Relaxed);
                        Some(stats)
                    }
                    Err(e) => {
                        log::debug!("vix: stats blob undecodable, no stats-answered arms: {e}");
                        None
                    }
                }
            })
            .as_ref()
    }

    /// Parquet row-group size recorded at build time (`0` = unknown).
    pub fn row_group_size(&self) -> usize {
        self.row_group_size
    }

    /// Number of dictionary (term) row groups. Mostly useful for tests and
    /// diagnostics.
    pub fn term_row_group_count(&self) -> usize {
        if self.term_count == 0 {
            return 0;
        }
        self.dict_index().map(|i| i.block_count()).unwrap_or(0)
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

    /// #52: the fields this file marks BLOOM-ONLY (values live in the
    /// composite bloom + docs columns; no dictionary/postings). The merge
    /// planner treats the marker as STICKY — an input's demotion carries
    /// into every merge plan over it, because the demoted field has no
    /// dictionary terms left for the count-driven AUTO rule to re-derive
    /// the decision from (the never-list + a heal is the un-demotion path).
    pub fn bloom_only_fields(&self) -> impl Iterator<Item = &str> + '_ {
        self.fields
            .iter()
            .filter(|entry| entry.has_type(FIELD_TYPE_BLOOM))
            .map(|entry| entry.name.as_str())
    }

    /// Fields whose VALUE TERMS are knowingly incomplete (type drift,
    /// field-id overflow, source keys outside the term plan, or a legacy
    /// file's pre-2026-08-12 oversize taint); term lookups on them may miss
    /// documents, so the query layer falls back to scanning. Oversize skips
    /// by current writers deliberately do NOT set this — they surface in
    /// [`Self::oversize_skips`] instead.
    pub fn partial_fields(&self) -> &HashSet<String> {
        &self.partial_fields
    }

    /// Per-field count of raw values the writer skipped for exceeding its
    /// `max_raw_term_len` (the `oversize_skips` property; empty for legacy
    /// files). Merges sum inputs' maps forward. The dictionary-serve paths
    /// treat these as an exact reconciliation allowance.
    pub fn oversize_skips(&self) -> &HashMap<String, u64> {
        &self.oversize_skips
    }

    /// The oversize-skip allowance for one field (`0` when absent).
    fn field_oversize_skips(&self, field: &str) -> u64 {
        self.oversize_skips.get(field).copied().unwrap_or(0)
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

    /// Warm the dictionary for `query`: parse the index (one fetch) and
    /// pull the blocks its POINT leaves resolve to into the block cache.
    /// Range/pattern leaves fetch on demand during their walks.
    ///
    /// A query WITHOUT point leaves skips the dictionary INDEX load too:
    /// `All`-condition evaluations (unconditioned SimpleSelect/TopN shapes)
    /// were paying an MB-class fetch per ranged file for a structure they
    /// never touch (#27).
    fn prefetch_query_fsts(&self, query: &VixQuery) -> Result<()> {
        if self.term_count == 0 || !Self::query_has_point_leaves(query) {
            return Ok(());
        }
        let index = self.dict_index()?;
        let mut needed: Vec<usize> = Vec::new();
        self.collect_query_block_needs(query, index, &mut needed)?;
        needed.sort_unstable();
        needed.dedup();
        // ranged readers batch every MISSING block into one fetch_many
        // round trip (the ladder/S3 issue one request); in-memory readers
        // slice for free through dict_block
        if let Some(BlobHandle::Ranged(ranged)) = self.dict_blocks_blob.as_ref() {
            let blob_len = self.dict_blocks_len()?;
            let missing: Vec<usize> = {
                let cache = self.block_cache.lock().expect("poisoned");
                needed
                    .iter()
                    .copied()
                    .filter(|b| !cache.0.contains_key(b))
                    .collect()
            };
            if missing.len() > 1 {
                let ranges: Vec<std::ops::Range<u64>> = missing
                    .iter()
                    .map(|&b| {
                        let r = index.block_range(b, blob_len);
                        ranged.range.start + r.start..ranged.range.start + r.end
                    })
                    .collect();
                let fetched = crate::source::block_fetch_many(ranged.source.as_ref(), ranges)?;
                let mut cache = self.block_cache.lock().expect("poisoned");
                for (b, bytes) in missing.into_iter().zip(fetched) {
                    self.cache_dict_block(&mut cache, b, &bytes);
                }
                return Ok(());
            }
        }
        for b in needed {
            self.dict_block(b)?;
        }
        Ok(())
    }

    /// Whether a query contains any POINT leaf (`Exact` / `KeyExists` /
    /// `TokenAnyField`) — the only leaves [`Self::prefetch_query_fsts`] can
    /// resolve to dictionary blocks up front. Range/pattern leaves walk
    /// (and fetch) lazily; `All`/`Nothing` never touch the dictionary.
    fn query_has_point_leaves(query: &VixQuery) -> bool {
        match query {
            VixQuery::Exact { .. }
            | VixQuery::KeyExists { .. }
            | VixQuery::TokenAnyField { .. } => true,
            VixQuery::And(subs) | VixQuery::Or(subs) => {
                subs.iter().any(Self::query_has_point_leaves)
            }
            VixQuery::Not(sub) => Self::query_has_point_leaves(sub),
            VixQuery::All
            | VixQuery::Nothing
            | VixQuery::Prefix { .. }
            | VixQuery::Contains { .. }
            | VixQuery::Regex { .. }
            | VixQuery::Fuzzy { .. } => false,
        }
    }

    /// The dictionary blocks a query's POINT leaves resolve to, computed
    /// from the resident index alone (no block reads). Unknown fields
    /// contribute nothing; range/pattern leaves are omitted — their walks
    /// fetch on demand.
    fn collect_query_block_needs(
        &self,
        query: &VixQuery,
        index: &crate::dict_blocks::DictIndex,
        out: &mut Vec<usize>,
    ) -> Result<()> {
        match query {
            VixQuery::All | VixQuery::Nothing => {}
            VixQuery::And(subs) | VixQuery::Or(subs) => {
                for sub in subs {
                    self.collect_query_block_needs(sub, index, out)?;
                }
            }
            VixQuery::Not(sub) => self.collect_query_block_needs(sub, index, out)?,
            VixQuery::Exact { field, token } => {
                if let Some(field_id) = self.field_id(field)
                    && let Some(b) = index.predecessor_block(&self.composite(token, field_id))?
                {
                    out.push(b);
                }
            }
            VixQuery::KeyExists { path } => {
                if let Some(b) =
                    index.predecessor_block(&self.composite(path.as_bytes(), KEY_FIELD_ID))?
                {
                    out.push(b);
                }
            }
            VixQuery::TokenAnyField { token } => {
                for &fid in &self.indexed_field_ids {
                    if let Some(b) = index.predecessor_block(&self.composite(token, fid))? {
                        out.push(b);
                    }
                }
            }
            VixQuery::Prefix { .. }
            | VixQuery::Contains { .. }
            | VixQuery::Regex { .. }
            | VixQuery::Fuzzy { .. } => {}
        }
        Ok(())
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

    /// Rough resident size of the parsed reader in bytes. GROWS as the
    /// dictionary index parses and blocks cache lazily — external caches
    /// should re-read it when re-touching an entry.
    pub fn memory_size(&self) -> usize {
        self.base_memory
            + self.dict_loaded_bytes.load(Ordering::Relaxed)
            + self.stats_loaded_bytes.load(Ordering::Relaxed)
    }

    /// The parsed dictionary block index, fetching + parsing it on first
    /// touch (whole `dict` blob — low single-digit MBs even on merged
    /// files; resident for the reader's lifetime).
    pub(crate) fn dict_index(&self) -> Result<&crate::dict_blocks::DictIndex> {
        if let Some(index) = self.dict_index.get() {
            return Ok(index);
        }
        let blob = self
            .dict_blob
            .as_ref()
            .ok_or_else(|| VixError::Malformed("missing dict blob".to_string()))?;
        let bytes = match blob {
            BlobHandle::Mem(bytes) => bytes.clone(),
            BlobHandle::Ranged(ranged) => {
                crate::source::block_fetch(ranged.source.as_ref(), ranged.range.clone())?
            }
        };
        let parsed = crate::dict_blocks::DictIndex::parse(&bytes)?;
        // structural cross-checks against the file's own term count
        let blocks = parsed.block_count();
        if blocks == 0 && self.term_count > 0 {
            return Err(VixError::Malformed(format!(
                "dict index holds no blocks for {} terms",
                self.term_count
            )));
        }
        let mut prev_ord = None;
        for b in 0..blocks {
            let (_, first_ordinal) = parsed.meta(b);
            if first_ordinal >= self.term_count.max(1)
                || prev_ord.is_some_and(|p| first_ordinal <= p) && b > 0
            {
                return Err(VixError::Malformed(format!(
                    "dict index block {b} first_ordinal {first_ordinal} out of order                      (term_count {})",
                    self.term_count
                )));
            }
            prev_ord = Some(first_ordinal);
        }
        let size = bytes.len();
        if self.dict_index.set(parsed).is_ok() {
            self.dict_loaded_bytes
                .fetch_add(size + 64, Ordering::Relaxed);
        }
        Ok(self.dict_index.get().expect("set just above"))
    }

    /// Total byte length of the dictionary blocks region.
    fn dict_blocks_len(&self) -> Result<u64> {
        Ok(match self.dict_blocks_blob.as_ref() {
            Some(BlobHandle::Mem(bytes)) => bytes.len() as u64,
            Some(BlobHandle::Ranged(ranged)) => ranged.len(),
            None => 0,
        })
    }

    /// Bytes of dictionary block `b` — zero-copy slice on in-memory
    /// readers, cache-aware range fetch on ranged readers.
    fn dict_block(&self, b: usize) -> Result<Bytes> {
        let index = self.dict_index()?;
        let range = index.block_range(b, self.dict_blocks_len()?);
        match self.dict_blocks_blob.as_ref() {
            Some(BlobHandle::Mem(bytes)) => {
                Ok(bytes.slice(range.start as usize..range.end as usize))
            }
            Some(BlobHandle::Ranged(ranged)) => {
                if let Some(hit) = self.block_cache.lock().expect("poisoned").0.get(&b) {
                    return Ok(hit.clone());
                }
                let bytes = crate::source::block_fetch(
                    ranged.source.as_ref(),
                    ranged.range.start + range.start..ranged.range.start + range.end,
                )?;
                let mut cache = self.block_cache.lock().expect("poisoned");
                self.cache_dict_block(&mut cache, b, &bytes);
                Ok(bytes)
            }
            None => Err(VixError::Malformed("missing dict_blocks blob".to_string())),
        }
    }

    /// Insert one fetched dictionary block into the FIFO-bounded block
    /// cache (caller holds the lock), accounting the reader's resident size
    /// and evicting past the cap.
    fn cache_dict_block(
        &self,
        cache: &mut (
            std::collections::HashMap<usize, Bytes>,
            std::collections::VecDeque<usize>,
        ),
        b: usize,
        bytes: &Bytes,
    ) {
        const DICT_BLOCK_CACHE_CAP: usize = 1024;
        if cache.0.insert(b, bytes.clone()).is_none() {
            cache.1.push_back(b);
            self.dict_loaded_bytes
                .fetch_add(bytes.len(), Ordering::Relaxed);
            while cache.1.len() > DICT_BLOCK_CACHE_CAP {
                if let Some(evict) = cache.1.pop_front()
                    && let Some(old) = cache.0.remove(&evict)
                {
                    self.dict_loaded_bytes
                        .fetch_sub(old.len(), Ordering::Relaxed);
                }
            }
        }
    }

    /// The WHOLE dictionary blocks region — full-walk paths (term
    /// enumeration, merges, unscoped scans) read it in one fetch instead of
    /// per-block round trips.
    pub(crate) fn dict_blocks_all_for_merge(&self) -> Result<Bytes> {
        self.dict_blocks_all()
    }

    fn dict_blocks_all(&self) -> Result<Bytes> {
        match self.dict_blocks_blob.as_ref() {
            Some(BlobHandle::Mem(bytes)) => Ok(bytes.clone()),
            Some(BlobHandle::Ranged(ranged)) => {
                crate::source::block_fetch(ranged.source.as_ref(), ranged.range.clone())
            }
            None => Err(VixError::Malformed("missing dict_blocks blob".to_string())),
        }
    }

    /// The raw `tokenizer` file property (merge compatibility checks).
    pub(crate) fn tokenizer_prop(&self) -> Option<&str> {
        self.tokenizer.as_deref()
    }

    /// The parsed `fields` property entries, in field-id order.
    pub(crate) fn field_entries(&self) -> &[FieldEntry] {
        &self.fields
    }

    /// The `terms` blob handle, when the file has any terms.
    pub(crate) fn terms_blob_handle(&self) -> Option<&BlobHandle> {
        self.terms_blob.as_ref()
    }

    /// Number of composite terms in the file (`term_count` property).
    pub fn term_count(&self) -> u64 {
        self.term_count
    }

    /// The `plist_min_docs` property: `> 0` ⇒ the file is plist-capable and
    /// a non-dense term with `doc_count >=` this threshold stores a
    /// 12-byte pointer cell instead of inline postings; `0` ⇒ every cell is
    /// inline (pre-plist file).
    pub(crate) fn plist_min_docs(&self) -> u32 {
        self.plist_min_docs
    }

    /// Whether a terms-table cell is an out-of-row POINTER CELL: the file is
    /// plist-capable, the term's `doc_count` is at/above the threshold, and
    /// the cell is non-empty (dense elision keeps the empty cell even above
    /// the threshold). The ONLY pointer-vs-inline discriminator — cell bytes
    /// are never sniffed.
    pub(crate) fn plist_pointer_cell(&self, doc_count: u64, cell: &[u8]) -> bool {
        self.plist_min_docs > 0 && doc_count >= u64::from(self.plist_min_docs) && !cell.is_empty()
    }

    /// Open a [`PlistCursor`] when `query` is a single-term leaf whose
    /// postings live out-of-row: the rank-based consumers (windowed counts,
    /// histograms) then read the skip table + touched groups instead of
    /// decoding the whole dense list into a bitmap. `None` whenever the
    /// preconditions fail — compound query, no/many matching terms, inline
    /// or dense-elided cell, plist-incapable file — and the caller falls
    /// back to the bitmap path.
    pub fn single_term_plist_cursor(
        &self,
        query: &VixQuery,
    ) -> anyhow::Result<Option<PlistCursor>> {
        if self.plist_min_docs == 0 || self.term_count == 0 {
            return Ok(None);
        }
        let ordinals = match query {
            VixQuery::All
            | VixQuery::Nothing
            | VixQuery::And(_)
            | VixQuery::Or(_)
            | VixQuery::Not(_) => return Ok(None),
            leaf => {
                self.prefetch_query_fsts(leaf)?;
                self.collect_ordinals(leaf)?
            }
        };
        let [ordinal] = ordinals[..] else {
            return Ok(None);
        };
        let terms_blob = self
            .terms_blob
            .as_ref()
            .ok_or_else(|| VixError::Malformed("missing terms blob".to_string()))?;
        let batches = scan_blob(
            terms_blob,
            Some(&["doc_count", "postings"]),
            RowSelection::Indices(vec![ordinal]),
        )?;
        for batch in &batches {
            let doc_counts = column_u64(batch, "doc_count")?;
            let postings = column_binary(batch, "postings")?;
            if batch.num_rows() > 0 {
                let doc_count = doc_counts[0];
                let cell = postings.value(0);
                if doc_count > self.row_count {
                    return Err(VixError::Malformed(format!(
                        "doc_count {doc_count} exceeds row_count {}",
                        self.row_count
                    ))
                    .into());
                }
                if !self.plist_pointer_cell(doc_count, cell) {
                    return Ok(None);
                }
                let record = self.plist_record_bytes(cell)?;
                return Ok(Some(PlistCursor { record, doc_count }));
            }
        }
        Ok(None)
    }

    /// Resolve one out-of-row postings POINTER CELL (`[u64 LE offset]
    /// [u32 LE len]`) to the [`postings::encode_record`] bytes it addresses
    /// inside the `plist` blob. The caller has already selected the cell by
    /// `doc_count >= plist_min_docs` — never by its bytes — so a wrong
    /// length or an out-of-bounds window here is file corruption. In-memory
    /// blobs slice for free; ranged blobs fetch exactly the record window.
    pub(crate) fn plist_record_bytes(&self, cell: &[u8]) -> Result<Bytes> {
        let (offset, end) = self.plist_pointer_window(cell)?;
        let handle = self
            .plist_blob
            .as_ref()
            .expect("plist_pointer_window checked the blob");
        match handle {
            BlobHandle::Mem(bytes) => Ok(bytes.slice(offset as usize..end as usize)),
            BlobHandle::Ranged(ranged) => crate::source::block_fetch(
                ranged.source.as_ref(),
                ranged.range.start + offset..ranged.range.start + end,
            ),
        }
    }

    /// Validate one pointer cell and return its `(offset, end)` window in
    /// the `plist` blob (shared by the single and batched resolvers).
    fn plist_pointer_window(&self, cell: &[u8]) -> Result<(u64, u64)> {
        let (offset, len) = postings::decode_pointer_cell(cell)?;
        let len = u64::from(len);
        let handle = self.plist_blob.as_ref().ok_or_else(|| {
            VixError::Malformed("postings pointer cell in a file without a plist blob".to_string())
        })?;
        let blob_len = match handle {
            BlobHandle::Mem(bytes) => bytes.len() as u64,
            BlobHandle::Ranged(ranged) => ranged.len(),
        };
        let end = offset.checked_add(len).filter(|&end| end <= blob_len);
        let Some(end) = end else {
            return Err(VixError::Malformed(format!(
                "plist pointer {offset}+{len} out of bounds ({blob_len}-byte plist blob)"
            )));
        };
        Ok((offset, end))
    }

    /// Resolve MANY out-of-row postings pointer windows in one batched
    /// round trip (`block_fetch_many` — the ladder/S3 issue one request)
    /// instead of a point fetch per term: the #27 coalescing lever for
    /// OR-heavy unions and dictionary-served filtered group-bys. Records
    /// return positionally.
    fn plist_records_batch(&self, windows: &[(u64, u64)]) -> Result<Vec<Bytes>> {
        if windows.is_empty() {
            return Ok(Vec::new());
        }
        let handle = self.plist_blob.as_ref().ok_or_else(|| {
            VixError::Malformed("postings pointer cell in a file without a plist blob".to_string())
        })?;
        match handle {
            BlobHandle::Mem(bytes) => Ok(windows
                .iter()
                .map(|&(offset, end)| bytes.slice(offset as usize..end as usize))
                .collect()),
            BlobHandle::Ranged(ranged) => crate::source::block_fetch_many(
                ranged.source.as_ref(),
                windows
                    .iter()
                    .map(|&(offset, end)| ranged.range.start + offset..ranged.range.start + end)
                    .collect(),
            ),
        }
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

    /// Every value-term field of this file as `(field id, name)` pairs —
    /// the #48 composite coverage set for dictionary-walk bloom builds.
    /// FTS and key fields are excluded by construction (the map only holds
    /// `term`-typed entries).
    pub fn term_fields(&self) -> impl Iterator<Item = (u16, &str)> + '_ {
        self.term_field_ids.iter().map(|(n, id)| (*id, n.as_str()))
    }

    /// #52: APPROXIMATE distinct-term count per value-term field, from the
    /// field-major dictionary's block metadata alone (no block decodes).
    /// Each field's span is bounded by `predecessor_block` binary searches
    /// on the 2-byte fid prefix, so the error is at most one block per
    /// boundary — callers gate decisions on ratios plus a large absolute
    /// floor, where block granularity is noise. Returns `(name, count)`.
    pub fn term_counts_by_field(&self) -> Result<Vec<(String, u64)>> {
        if self.term_count() == 0 || self.term_field_ids.is_empty() {
            return Ok(Vec::new());
        }
        let index = self.dict_index()?;
        let total = self.term_count();
        let mut ids: Vec<(u16, &str)> = self.term_fields().collect();
        ids.sort_unstable_by_key(|(id, _)| *id);
        // start ordinal (block-approximate) of each fid's key range
        let mut starts: Vec<(usize, u64)> = Vec::with_capacity(ids.len());
        for (pos, (id, _)) in ids.iter().enumerate() {
            let block = index.predecessor_block(&id.to_be_bytes())?.unwrap_or(0);
            starts.push((pos, index.meta(block).1));
        }
        let mut out = Vec::with_capacity(ids.len());
        for (pos, start) in &starts {
            let end = starts.get(pos + 1).map(|(_, next)| *next).unwrap_or(total);
            out.push((ids[*pos].1.to_string(), end.saturating_sub(*start)));
        }
        Ok(out)
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
        let terms_blob = self
            .terms_blob
            .as_ref()
            .ok_or_else(|| unbuildable(VixError::Malformed("missing terms blob".to_string())))?;
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
        // a full-dictionary walk: one fetch of the whole blocks region,
        // then a straight in-memory decode in ordinal order
        let dict_index = self.dict_index()?;
        let all = self.dict_blocks_all()?;
        let blob_len = all.len() as u64;
        for b in 0..dict_index.block_count() {
            let range = dict_index.block_range(b, blob_len);
            let mut iter =
                crate::dict_blocks::BlockIter::new(&all[range.start as usize..range.end as usize]);
            while let Some(key) = iter.next()? {
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
                } else if self.plist_min_docs > 0 && doc_count >= u64::from(self.plist_min_docs) {
                    // out-of-row postings: the threshold — never the cell
                    // bytes — says this non-empty cell is a pointer; resolve
                    // it through the plist blob and decode the record's blob
                    // region (stage-2 minimal resolution; the query paths
                    // learn pointer cells in stage 3)
                    let record = self.plist_record_bytes(blob)?;
                    ids.reserve(doc_count as usize);
                    postings::decode_each(
                        postings::record_blob(&record).map_err(unbuildable)?,
                        doc_count as usize,
                        |doc| {
                            ids.push(doc);
                            Ok(())
                        },
                    )
                    .map_err(unbuildable)?;
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
    /// - the field is in `partial_fields` (type drift / field-id overflow / source keys outside the
    ///   term plan / legacy pre-2026-08-12 taint),
    /// - the field is fts-marked in this file (its dictionary holds tokens, not raw values),
    /// - the raw-term doc counts PLUS the field's `oversize_skips` allowance do not sum to the
    ///   key-term doc count — some docs carry values the dictionary lacks for a reason other than
    ///   an oversize skip (e.g. a field stored under a non-string type in this file has key terms
    ///   only). The empty string is a countable value: writers raw-index it, so `("", doc_count)`
    ///   appears like any other value.
    ///
    /// Oversize-skipped values (the 2026-08-12 performance-first trade) do
    /// NOT refuse the serve: the counts simply OMIT those values, and their
    /// docs appear in no group.
    ///
    /// `Ok(Some(vec![]))` means every doc carrying `field` is accounted for
    /// with no servable value — no docs at all, or only oversize-skipped
    /// ones — an exact empty group list under the trade above.
    pub fn field_value_counts(&self, field: &str) -> anyhow::Result<Option<FieldValueCounts>> {
        Ok(self.field_value_counts_inner(field)?)
    }

    /// Filtered variant of [`VixReader::field_value_counts`]: per raw string
    /// value of `field`, the number of its documents whose bit is set in
    /// `filter` (a row bitmap of length `row_count`). Zero-hit values are
    /// omitted. Eligibility is IDENTICAL to the unfiltered variant — same
    /// pre-checks and the same doc-count reconciliation against the key
    /// term — because both depend on the dictionary holding EVERY document's
    /// value; `Ok(None)` when this file cannot prove that. Cost is one
    /// decode of the field's postings (SIMD, ≈ the field's row count in
    /// ids) — the fast serve for group-bys over files that predate the
    /// field's `column_store_fields` entry.
    ///
    /// `cap` (#29 lever 2) bounds the value enumeration: a field with more
    /// than `cap` distinct string values returns `Ok(None)` after touching
    /// at most `cap` keys, so per-file memory stays bounded no matter the
    /// field's cardinality and the caller falls back to the scan paths.
    pub fn field_value_counts_filtered(
        &self,
        field: &str,
        filter: &BooleanBuffer,
        cap: usize,
    ) -> anyhow::Result<Option<FieldValueCounts>> {
        Ok(self.field_value_counts_filtered_inner(field, filter, cap)?)
    }

    /// #29 lever 1: the top `cap` values of `field` by exact doc count,
    /// WITHOUT materializing the field's dictionary keys. Where
    /// [`VixReader::field_value_counts`] walks every key and allocates one
    /// `Vec<u8>` per distinct value (1.9GB peak for a 16M-distinct field),
    /// this streams the field's contiguous `doc_count` ordinal range into a
    /// bounded heap and resolves ONLY the winners' keys (≤ `cap` block
    /// probes, batch-fetched).
    ///
    /// Eligibility and exactness are IDENTICAL to `field_value_counts`
    /// (same gates, same key-term reconciliation): `Ok(None)` means this
    /// file cannot prove exact per-value counts and the caller must fall
    /// back. `Some((counts, truncated))`: counts ascending by key,
    /// `truncated` when the field has more than `cap` distinct string
    /// values — counts then holds the top `cap` by (`ascend` ? smallest :
    /// largest) count, ties toward the smaller key, matching the collector's
    /// `truncate_top_k` order.
    pub fn field_value_top_k(
        &self,
        field: &str,
        cap: usize,
        ascend: bool,
    ) -> anyhow::Result<Option<(FieldValueCounts, bool)>> {
        Ok(self.field_value_top_k_inner(field, cap, ascend)?)
    }

    /// #29 companion for SimpleDistinct: the FIRST (or LAST) `limit` raw
    /// string values of `field` in ascending key order — the dictionary
    /// serves head/tail directly, so only the `limit` requested keys are
    /// ever materialized. Same eligibility + reconciliation contract as
    /// [`VixReader::field_value_top_k`] (`Ok(None)` = fall back).
    pub fn field_value_head(
        &self,
        field: &str,
        limit: usize,
        from_end: bool,
    ) -> anyhow::Result<Option<Vec<Vec<u8>>>> {
        Ok(self.field_value_head_inner(field, limit, from_end)?)
    }

    /// M13 dispatch probe: how many RAW STRING value terms `field` holds in
    /// this file's dictionary — the (at most two) contiguous field-major
    /// ordinal ranges' total length. `None` when the dictionary cannot
    /// serve the field's value counts anyway (no index, fts-marked,
    /// partial) or the field is absent from the field table; the caller
    /// then keeps the docs-column path.
    ///
    /// Cost: four resident-index probes plus at most a handful of ~4KB
    /// block loads — cheap enough to gate the top-k/distinct dispatch per
    /// query. This is the DICTIONARY's distinct-value count (each stored
    /// raw string value once, numeric/bool sub-ranges excluded); the
    /// docs-column group-by's cost tracks `row_count`, so `distinct / rows`
    /// is the dispatch ratio (`field_value_top_k`'s own doc-count
    /// reconciliation still decides final eligibility — a probe here never
    /// commits correctness, only cost).
    pub fn field_distinct_string_terms(&self, field: &str) -> anyhow::Result<Option<u64>> {
        if !self.has_index() || self.partial_fields.contains(field) {
            return Ok(None);
        }
        if self
            .fields
            .iter()
            .any(|entry| entry.name == field && entry.has_type(FIELD_TYPE_FTS))
        {
            return Ok(None);
        }
        let Some(field_id) = self.field_id(field) else {
            return Ok(None);
        };
        let ranges = self.string_value_ordinal_ranges(field_id)?;
        Ok(Some(ranges.iter().map(|r| r.end - r.start).sum()))
    }

    fn field_value_head_inner(
        &self,
        field: &str,
        limit: usize,
        from_end: bool,
    ) -> Result<Option<Vec<Vec<u8>>>> {
        if self.partial_fields.contains(field) {
            return Ok(None);
        }
        if self
            .fields
            .iter()
            .any(|entry| entry.name == field && entry.has_type(FIELD_TYPE_FTS))
        {
            return Ok(None);
        }
        let field_docs = match self.lookup_exact(&self.composite(field.as_bytes(), KEY_FIELD_ID))? {
            Some(ordinal) => self.read_doc_count(ordinal)?,
            None => 0,
        };
        let Some(field_id) = self.field_id(field) else {
            return Ok((field_docs == 0).then(Vec::new));
        };
        let ranges = self.string_value_ordinal_ranges(field_id)?;
        let total_strings: u64 = ranges.iter().map(|r| r.end - r.start).sum();
        if total_strings == 0 {
            return Ok((field_docs == 0).then(Vec::new));
        }
        // exactness precondition, same as every value-count path: the
        // STRING value doc counts must sum to the key-term doc count
        if self.sum_string_value_doc_counts(&ranges)? != field_docs {
            return Ok(None);
        }
        // the first/last `limit` ordinals across the (ascending) ranges
        let take = (limit as u64).min(total_strings);
        let mut ordinals: Vec<u64> = Vec::with_capacity(take as usize);
        if from_end {
            let mut remaining = take;
            for range in ranges.iter().rev() {
                let len = range.end - range.start;
                let here = remaining.min(len);
                ordinals.extend(range.end - here..range.end);
                remaining -= here;
                if remaining == 0 {
                    break;
                }
            }
            ordinals.sort_unstable();
        } else {
            let mut remaining = take;
            for range in ranges.iter() {
                let len = range.end - range.start;
                let here = remaining.min(len);
                ordinals.extend(range.start..range.start + here);
                remaining -= here;
                if remaining == 0 {
                    break;
                }
            }
        }
        let mut keys = self.keys_for_ordinals(&ordinals)?;
        for key in &mut keys {
            if key.len() < 2 {
                return Err(VixError::Malformed(
                    "value term without a composite field id".to_string(),
                ));
            }
            key.drain(..2);
        }
        Ok(Some(keys))
    }

    /// Sum the `doc_count` column over the given ordinal ranges without
    /// touching keys or postings — the reconciliation half of the #29
    /// key-free paths.
    fn sum_string_value_doc_counts(&self, ranges: &[std::ops::Range<u64>]) -> Result<u64> {
        let terms_blob = self
            .terms_blob
            .as_ref()
            .ok_or_else(|| VixError::Malformed("missing terms blob".to_string()))?;
        let mut sum = 0u64;
        for range in ranges.iter().filter(|r| r.end > r.start) {
            let mut seen = 0u64;
            scan_blob_streaming(
                terms_blob,
                Some(&["doc_count"]),
                RowSelection::Range(range.clone()),
                None,
                None,
                0,
                &mut |batch| {
                    let doc_counts = column_u32(&batch, "doc_count")?;
                    seen += doc_counts.len() as u64;
                    sum += doc_counts.values().iter().map(|&v| v as u64).sum::<u64>();
                    Ok(())
                },
            )?;
            if seen != range.end - range.start {
                return Err(VixError::Malformed(format!(
                    "terms doc_count scan returned {seen} rows for ordinal range {range:?}",
                )));
            }
        }
        Ok(sum)
    }

    fn field_value_top_k_inner(
        &self,
        field: &str,
        cap: usize,
        ascend: bool,
    ) -> Result<Option<(FieldValueCounts, bool)>> {
        // identical eligibility gates to field_string_value_terms
        if self.partial_fields.contains(field) {
            return Ok(None);
        }
        if self
            .fields
            .iter()
            .any(|entry| entry.name == field && entry.has_type(FIELD_TYPE_FTS))
        {
            return Ok(None);
        }
        let field_docs = match self.lookup_exact(&self.composite(field.as_bytes(), KEY_FIELD_ID))? {
            Some(ordinal) => self.read_doc_count(ordinal)?,
            None => 0,
        };
        // The oversize-skip allowance: skipped values have key terms but no
        // value term, so they legitimately account for a shortfall — the
        // served top-k then omits them (the 2026-08-12 trade).
        let skips = self.field_oversize_skips(field);
        let Some(field_id) = self.field_id(field) else {
            return Ok((field_docs == skips).then(|| (Vec::new(), false)));
        };
        let ranges = self.string_value_ordinal_ranges(field_id)?;
        let total_strings: u64 = ranges.iter().map(|r| r.end - r.start).sum();
        if total_strings == 0 {
            // no string value terms at all: exact iff every doc carrying
            // the field is accounted for by the skip allowance (mirrors the
            // walk's values.is_empty() arm)
            return Ok((field_docs == skips).then(|| (Vec::new(), false)));
        }
        let terms_blob = self
            .terms_blob
            .as_ref()
            .ok_or_else(|| VixError::Malformed("missing terms blob".to_string()))?;

        // Bounded selection over (count, ordinal), weakest at the heap root.
        // Ordinals ascend in key order, so the "ties toward the smaller key"
        // contract maps exactly to "ties toward the smaller ordinal".
        use std::{cmp::Reverse, collections::BinaryHeap};
        let mut sum = 0u64;
        // pre-size the (single) heap actually used: it never grows past
        // min(cap, distinct values), so the scan loop is allocation-free
        let heap_capacity = usize::try_from(total_strings.min(cap as u64))
            .unwrap_or(cap)
            .saturating_add(1);
        let mut keep_desc: BinaryHeap<Reverse<(u64, Reverse<u64>)>> =
            BinaryHeap::with_capacity(if ascend { 0 } else { heap_capacity });
        let mut keep_asc: BinaryHeap<(u64, u64)> =
            BinaryHeap::with_capacity(if ascend { heap_capacity } else { 0 });
        for range in ranges.iter().filter(|r| r.end > r.start) {
            let mut next_ordinal = range.start;
            scan_blob_streaming(
                terms_blob,
                Some(&["doc_count"]),
                RowSelection::Range(range.clone()),
                None,
                None,
                0,
                &mut |batch| {
                    // zero-copy u32 view: column_u64 would cast-copy every
                    // batch twice, ~256MB of pure churn on a 16M-term field
                    let doc_counts = column_u32(&batch, "doc_count")?;
                    for &count in doc_counts.values().iter() {
                        let count = count as u64;
                        let ordinal = next_ordinal;
                        next_ordinal += 1;
                        sum += count;
                        if cap == 0 {
                            continue;
                        }
                        if ascend {
                            if keep_asc.len() < cap {
                                keep_asc.push((count, ordinal));
                            } else if let Some(&root) = keep_asc.peek()
                                && (count, ordinal) < root
                            {
                                keep_asc.pop();
                                keep_asc.push((count, ordinal));
                            }
                        } else if keep_desc.len() < cap {
                            keep_desc.push(Reverse((count, Reverse(ordinal))));
                        } else if let Some(&Reverse(root)) = keep_desc.peek()
                            && (count, Reverse(ordinal)) > root
                        {
                            keep_desc.pop();
                            keep_desc.push(Reverse((count, Reverse(ordinal))));
                        }
                    }
                    Ok(())
                },
            )?;
            if next_ordinal != range.end {
                return Err(VixError::Malformed(format!(
                    "terms doc_count scan returned {} rows for ordinal range {range:?}",
                    next_ordinal - range.start,
                )));
            }
        }
        // same exactness precondition as the walk: every doc carries at most
        // one raw value, so exact STRING value counts plus the oversize-skip
        // allowance sum to the key-term doc count; any OTHER shortfall
        // (numeric-typed rows, pre-fix empty-string files, ...) refuses the
        // fast path
        if sum + skips != field_docs {
            return Ok(None);
        }
        let mut winners: Vec<(u64, u64)> = if ascend {
            keep_asc.into_iter().collect()
        } else {
            keep_desc
                .into_iter()
                .map(|Reverse((count, Reverse(ordinal)))| (count, ordinal))
                .collect()
        };
        // resolve winner keys in ascending ordinal (== key) order, stripped
        // to their token part like the walk — in place (the field-id is a
        // 2-byte prefix), so winners are allocated exactly once
        winners.sort_unstable_by_key(|&(_, ordinal)| ordinal);
        let ordinals: Vec<u64> = winners.iter().map(|&(_, ordinal)| ordinal).collect();
        let mut keys = self.keys_for_ordinals(&ordinals)?;
        for key in &mut keys {
            if key.len() < 2 {
                return Err(VixError::Malformed(
                    "value term without a composite field id".to_string(),
                ));
            }
            key.drain(..2);
        }
        let counts: FieldValueCounts = keys
            .into_iter()
            .zip(winners.into_iter().map(|(count, _)| count))
            .collect();
        Ok(Some((counts, total_strings > cap as u64)))
    }

    fn field_value_counts_inner(&self, field: &str) -> Result<Option<FieldValueCounts>> {
        let Some((values, ordinals, field_docs)) = self.field_string_value_terms(field, None)?
        else {
            return Ok(None);
        };
        let skips = self.field_oversize_skips(field);
        if values.is_empty() {
            // no value terms at all: exact iff every doc carrying the field
            // is accounted for by the oversize-skip allowance (e.g. the
            // field is stored under a non-string type here)
            return Ok((field_docs == skips).then(Vec::new));
        }
        let counts = self.read_doc_counts(&ordinals)?;
        // each doc has at most one raw value per field, so exact counts plus
        // the oversize-skip allowance sum to the key-term doc count — the
        // served counts then OMIT the skipped values (2026-08-12 trade). Any
        // OTHER shortfall means values the dictionary lacks (values of
        // another stored type, empty strings in pre-fix files, ...) —
        // empty-string terms written by current writers are ordinary
        // dictionary entries and reconcile like any other value
        if counts.iter().sum::<u64>() + skips != field_docs {
            return Ok(None);
        }
        Ok(Some(values.into_iter().zip(counts).collect()))
    }

    fn field_value_counts_filtered_inner(
        &self,
        field: &str,
        filter: &BooleanBuffer,
        cap: usize,
    ) -> Result<Option<FieldValueCounts>> {
        if filter.len() as u64 != self.row_count {
            return Err(VixError::Malformed(format!(
                "filter bitmap length {} != row_count {}",
                filter.len(),
                self.row_count
            )));
        }
        let Some((values, ordinals, field_docs)) =
            self.field_string_value_terms(field, Some(cap))?
        else {
            return Ok(None);
        };
        let skips = self.field_oversize_skips(field);
        if values.is_empty() {
            return Ok((field_docs == skips).then(Vec::new));
        }
        let terms_blob = self
            .terms_blob
            .as_ref()
            .ok_or_else(|| VixError::Malformed("missing terms blob".to_string()))?;
        let batches = scan_blob(
            terms_blob,
            Some(&["doc_count", "postings"]),
            RowSelection::Indices(ordinals),
        )?;
        let mut unfiltered_sum = 0u64;
        // pass 1: validate every cell, accumulate the reconciliation sum,
        // and collect the out-of-row pointer windows for ONE batched round
        // trip (#27) instead of a point fetch per value term
        let mut pointer_windows: Vec<(u64, u64)> = Vec::new();
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
                unfiltered_sum += doc_count;
                let blob = postings.value(row);
                if blob.is_empty() && doc_count > 0 && doc_count != self.row_count {
                    return Err(VixError::Malformed(format!(
                        "empty postings blob for a term with doc_count {doc_count} != \
                         row_count {} (not dense-elided, so corrupt)",
                        self.row_count
                    )));
                }
                if self.plist_pointer_cell(doc_count, blob) {
                    pointer_windows.push(self.plist_pointer_window(blob)?);
                }
            }
        }
        let records = self.plist_records_batch(&pointer_windows)?;
        let mut records = records.iter();
        // pass 2: count each value's postings inside the filter, decoding
        // inline cells and the pre-fetched records in pass-1 order
        let mut filtered: Vec<u64> = Vec::with_capacity(values.len());
        for batch in &batches {
            let doc_counts = column_u64(batch, "doc_count")?;
            let postings = column_binary(batch, "postings")?;
            for (row, &doc_count) in doc_counts.iter().enumerate() {
                let cell = postings.value(row);
                if cell.is_empty() && doc_count == self.row_count && doc_count > 0 {
                    // dense elision: the term is in every doc
                    filtered.push(filter.count_set_bits() as u64);
                    continue;
                }
                let blob = if self.plist_pointer_cell(doc_count, cell) {
                    postings::record_blob(
                        records.next().expect("windows collected in pass 1 order"),
                    )?
                } else {
                    cell
                };
                let mut hits = 0u64;
                postings::decode_each(blob, doc_count as usize, |doc| {
                    if u64::from(doc) >= self.row_count {
                        return Err(VixError::Malformed(format!(
                            "postings doc id {doc} out of range (row_count {})",
                            self.row_count
                        )));
                    }
                    if filter.value(doc as usize) {
                        hits += 1;
                    }
                    Ok(())
                })?;
                filtered.push(hits);
            }
        }
        if filtered.len() != values.len() {
            return Err(VixError::Malformed(format!(
                "terms scan returned {} rows for {} requested ordinals",
                filtered.len(),
                values.len()
            )));
        }
        // the exactness precondition reconciles on UNFILTERED counts (the
        // filtered sum legitimately falls short of field_docs) — plus the
        // oversize-skip allowance, whose docs simply never surface in any
        // served group (2026-08-12 trade)
        if unfiltered_sum + skips != field_docs {
            return Ok(None);
        }
        Ok(Some(values.into_iter().zip(filtered).collect()))
    }

    /// Shared front half of the value-count paths: eligibility pre-checks +
    /// the field's raw STRING value terms `(values, ordinals, field_docs)`,
    /// both vecs ascending and parallel. `Ok(None)` when the dictionary
    /// cannot enumerate the field's values exactly (partial / fts-marked),
    /// or when the field has more than `cap` distinct string values (#29
    /// lever 2: the enumeration stops right there instead of materializing
    /// millions of keys, and the caller falls back to the scan paths).
    #[allow(clippy::type_complexity)]
    fn field_string_value_terms(
        &self,
        field: &str,
        cap: Option<usize>,
    ) -> Result<Option<(Vec<Vec<u8>>, Vec<u64>, u64)>> {
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
            return Ok(Some((Vec::new(), Vec::new(), field_docs)));
        };

        // walk the field's contiguous dictionary range, keeping raw string
        // value terms; blocks stream ascending, so values and ordinals come
        // out sorted (and unique — one term per distinct value). TAGGED
        // numeric/bool value terms are not string values: they are excluded
        // here, and their doc counts then surface as a reconciliation
        // shortfall in the callers — a field with number-stored rows
        // correctly refuses the exact-counts fast path (mixed-type grouping
        // must go through the scan's typed projection).
        let mut values: Vec<Vec<u8>> = Vec::new();
        let mut ordinals: Vec<u64> = Vec::new();
        let mut over_cap = false;
        // field-major: the field's values are one contiguous range
        let (lower, upper) = Self::v2_field_range(field_id);
        self.scan_key_range(&lower, Some((&upper, false)), |key, ordinal| {
            if let Some((token, _)) = split_key(key)
                && !is_numeric_value_token(token)
            {
                if cap.is_some_and(|cap| values.len() >= cap) {
                    over_cap = true;
                    return false;
                }
                values.push(token.to_vec());
                ordinals.push(ordinal);
            }
            true
        })?;
        if over_cap {
            return Ok(None);
        }
        Ok(Some((values, ordinals, field_docs)))
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
                true
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
        if !self.index_enabled && !matches!(query, VixQuery::All) {
            // insurance (#40): the routing layers keep conditions away from
            // index-off files; a slipped call degrades safely — per-file
            // eval errors leave the file on the scan branch, filter intact
            return Err(VixError::UnsupportedFormat(
                "condition eval on a file opened without an index sidecar".to_string(),
            ));
        }
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
        if !self.index_enabled && !matches!(query, VixQuery::All) {
            return Err(VixError::UnsupportedFormat(
                "condition count on a file opened without an index sidecar".to_string(),
            ));
        }
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
                    // single-field leaves over a non-fts field: a document
                    // carries exactly ONE value term of that field, so the
                    // matched terms' doc sets are pairwise DISJOINT and the
                    // count is the plain doc_count sum — no postings decode,
                    // no bitmap. (fts token terms of one doc overlap, so fts
                    // fields keep the union.) A 30-term prefix over 16M docs
                    // drops from ~21ms of postings union to column point
                    // reads.
                    _ if self.leaf_term_docs_are_disjoint(leaf) => {
                        Ok(self.read_doc_counts(&ordinals)?.iter().sum())
                    }
                    _ => Ok(self.postings_union(ordinals)?.count_set_bits() as u64),
                }
            }
        }
    }

    /// Whether a leaf's matched terms provably hold pairwise-disjoint doc
    /// sets: the leaf is scoped to ONE named field and that field is not
    /// fts-marked. Raw value terms are disjoint by construction (one value
    /// per document per field — numeric-tagged terms included, a document's
    /// value is either the string or the number); fts token terms are not.
    fn leaf_term_docs_are_disjoint(&self, leaf: &VixQuery) -> bool {
        let field = match leaf {
            VixQuery::Prefix { field: Some(f), .. }
            | VixQuery::Contains { field: Some(f), .. }
            | VixQuery::Regex { field: Some(f), .. } => f,
            _ => return false,
        };
        !self
            .fields
            .iter()
            .any(|entry| entry.name == *field && entry.has_type(FIELD_TYPE_FTS))
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
                        true
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
                    let finder = memchr::memmem::Finder::new(needle.as_bytes());
                    // reused per-token buffer: the old per-key
                    // from_utf8_lossy + to_lowercase allocated twice for
                    // EVERY dictionary key (32M allocs on a 16M-key field).
                    // ASCII tokens (the norm) lowercase into the buffer;
                    // Unicode tokens keep the exact old fold semantics.
                    let mut lowered: Vec<u8> = Vec::new();
                    self.scan_all_tokens(field_filter, |token| {
                        if token.is_ascii() {
                            lowered.clear();
                            lowered.extend(token.iter().map(|b| b.to_ascii_lowercase()));
                            finder.find(&lowered).is_some()
                        } else {
                            String::from_utf8_lossy(token)
                                .to_lowercase()
                                .contains(&needle)
                        }
                    })
                } else {
                    // one SIMD searcher for the whole scan, not a fresh
                    // scalar windows() pass per key
                    let finder = memchr::memmem::Finder::new(needle.as_slice());
                    self.scan_all_tokens(field_filter, |token| finder.find(token).is_some())
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
        if self.term_count == 0 {
            return Ok(None);
        }
        let index = self.dict_index()?;
        let Some(b) = index.predecessor_block(key)? else {
            return Ok(None);
        };
        let block = self.dict_block(b)?;
        Ok(crate::dict_blocks::block_find_exact(&block, key)?
            .map(|pos| index.meta(b).1 + pos as u64))
    }

    /// Stream all keys in `[lower, upper]`/`[lower, upper)` in order:
    /// predecessor-seek the start block, then walk forward block by block
    /// until a key passes the upper bound (narrow probes touch one block).
    ///
    /// Ranged readers bulk-load the walk's whole block span up front: the
    /// resident index bounds the last reachable block without IO, so a
    /// field-range walk costs a few MB-sized round trips instead of one
    /// ~4KB round trip per block — the #27 fetch storm (27k point reads
    /// over 78 files for one unfiltered trace-list TopN) was this loop.
    /// `on_key` returns whether to CONTINUE — `false` ends the walk early
    /// (the #29 lever-2 enumeration cap rides this).
    fn scan_key_range(
        &self,
        lower: &[u8],
        upper: Option<(&[u8], bool)>,
        mut on_key: impl FnMut(&[u8], u64) -> bool,
    ) -> Result<()> {
        if self.term_count == 0 {
            return Ok(());
        }
        let index = self.dict_index()?;
        let start = index.predecessor_block(lower)?.unwrap_or(0);
        // Last block the walk can touch: the block containing the upper
        // bound's predecessor key — later blocks' first keys already exceed
        // the bound. Unbounded walks may reach the final block. The walk
        // below keeps its own past-the-bound termination; a short span only
        // costs per-block fallback fetches, never correctness.
        let last = match upper {
            Some((bound, _)) => index.predecessor_block(bound)?.unwrap_or(start).max(start),
            None => index.block_count().saturating_sub(1),
        };
        let span = self.load_dict_block_span(start, last)?;
        for b in start..index.block_count() {
            let block = match span.as_ref().and_then(|blocks| blocks.get(&b)) {
                Some(bytes) => bytes.clone(),
                None => self.dict_block(b)?,
            };
            let first_ordinal = index.meta(b).1;
            let mut done = false;
            crate::dict_blocks::block_scan(&block, |pos, key| {
                if key < lower {
                    return true;
                }
                if let Some((bound, inclusive)) = upper
                    && ((inclusive && key > bound) || (!inclusive && key >= bound))
                {
                    done = true;
                    return false;
                }
                if !on_key(key, first_ordinal + pos as u64) {
                    done = true;
                    return false;
                }
                true
            })?;
            if done {
                break;
            }
        }
        Ok(())
    }

    /// Bulk-load the missing dictionary blocks of `start..=last` for one
    /// range walk (ranged readers only; in-memory readers slice for free).
    /// Runs of consecutive missing blocks resolve through ONE
    /// `block_fetch_many` — adjacent ranges coalesce into MB-sized round
    /// trips, cut at [`SPAN_FETCH_MAX_BYTES`] — and are sliced per block.
    /// Small spans are also published to the shared block cache so repeated
    /// walks stay warm; oversized spans stay local to this walk (they would
    /// only churn the FIFO cap).
    fn load_dict_block_span(
        &self,
        start: usize,
        last: usize,
    ) -> Result<Option<std::collections::HashMap<usize, Bytes>>> {
        const SPAN_FETCH_MAX_BYTES: u64 = 8 * 1024 * 1024;
        const SPAN_CACHE_MAX_BLOCKS: usize = 512;
        let Some(BlobHandle::Ranged(ranged)) = self.dict_blocks_blob.as_ref() else {
            return Ok(None);
        };
        if last < start {
            return Ok(None);
        }
        let index = self.dict_index()?;
        let blob_len = self.dict_blocks_len()?;
        // runs of consecutive MISSING blocks, cut at the fetch-size cap
        let mut runs: Vec<(std::ops::Range<usize>, std::ops::Range<u64>)> = Vec::new();
        {
            let cache = self.block_cache.lock().expect("poisoned");
            for b in start..=last.min(index.block_count().saturating_sub(1)) {
                if cache.0.contains_key(&b) {
                    continue;
                }
                let range = index.block_range(b, blob_len);
                match runs.last_mut() {
                    Some((blocks, bytes))
                        if blocks.end == b && range.end - bytes.start <= SPAN_FETCH_MAX_BYTES =>
                    {
                        blocks.end = b + 1;
                        bytes.end = range.end;
                    }
                    _ => runs.push((b..b + 1, range)),
                }
            }
        }
        // nothing missing, or a single block: the plain cached read is as good
        if runs.is_empty() || (runs.len() == 1 && runs[0].0.len() == 1) {
            return Ok(None);
        }
        let ranges: Vec<std::ops::Range<u64>> = runs
            .iter()
            .map(|(_, bytes)| ranged.range.start + bytes.start..ranged.range.start + bytes.end)
            .collect();
        let fetched = crate::source::block_fetch_many(ranged.source.as_ref(), ranges)?;
        let total_blocks: usize = runs.iter().map(|(blocks, _)| blocks.len()).sum();
        let mut blocks_map = std::collections::HashMap::with_capacity(total_blocks);
        for ((blocks, bytes), run) in runs.into_iter().zip(fetched) {
            for b in blocks {
                let range = index.block_range(b, blob_len);
                blocks_map.insert(
                    b,
                    run.slice(
                        (range.start - bytes.start) as usize..(range.end - bytes.start) as usize,
                    ),
                );
            }
        }
        if blocks_map.len() <= SPAN_CACHE_MAX_BLOCKS {
            let mut cache = self.block_cache.lock().expect("poisoned");
            for (&b, bytes) in &blocks_map {
                self.cache_dict_block(&mut cache, b, bytes);
            }
        }
        Ok(Some(blocks_map))
    }

    /// Bulk-load an arbitrary ASCENDING list of dictionary blocks (ranged
    /// readers only; in-memory readers slice for free): the scattered-block
    /// sibling of [`Self::load_dict_block_span`], for resolving a top-k's
    /// winner ordinals whose blocks need not be consecutive. Runs of
    /// adjacent missing blocks coalesce into one ranged read each; the
    /// whole list resolves in ONE `block_fetch_many` round trip.
    fn load_dict_blocks(
        &self,
        blocks: &[usize],
    ) -> Result<Option<std::collections::HashMap<usize, Bytes>>> {
        const SPAN_FETCH_MAX_BYTES: u64 = 8 * 1024 * 1024;
        const SPAN_CACHE_MAX_BLOCKS: usize = 512;
        let Some(BlobHandle::Ranged(ranged)) = self.dict_blocks_blob.as_ref() else {
            return Ok(None);
        };
        if blocks.is_empty() {
            return Ok(None);
        }
        let index = self.dict_index()?;
        let blob_len = self.dict_blocks_len()?;
        let mut runs: Vec<(std::ops::Range<usize>, std::ops::Range<u64>)> = Vec::new();
        {
            let cache = self.block_cache.lock().expect("poisoned");
            for &b in blocks {
                if b >= index.block_count() || cache.0.contains_key(&b) {
                    continue;
                }
                let range = index.block_range(b, blob_len);
                match runs.last_mut() {
                    Some((run_blocks, bytes))
                        if run_blocks.end == b
                            && range.end - bytes.start <= SPAN_FETCH_MAX_BYTES =>
                    {
                        run_blocks.end = b + 1;
                        bytes.end = range.end;
                    }
                    Some((run_blocks, _)) if run_blocks.contains(&b) => {}
                    _ => runs.push((b..b + 1, range)),
                }
            }
        }
        if runs.is_empty() {
            return Ok(None);
        }
        let ranges: Vec<std::ops::Range<u64>> = runs
            .iter()
            .map(|(_, bytes)| ranged.range.start + bytes.start..ranged.range.start + bytes.end)
            .collect();
        let fetched = crate::source::block_fetch_many(ranged.source.as_ref(), ranges)?;
        let total_blocks: usize = runs.iter().map(|(run_blocks, _)| run_blocks.len()).sum();
        let mut blocks_map = std::collections::HashMap::with_capacity(total_blocks);
        for ((run_blocks, bytes), run) in runs.into_iter().zip(fetched) {
            for b in run_blocks {
                let range = index.block_range(b, blob_len);
                blocks_map.insert(
                    b,
                    run.slice(
                        (range.start - bytes.start) as usize..(range.end - bytes.start) as usize,
                    ),
                );
            }
        }
        if blocks_map.len() <= SPAN_CACHE_MAX_BLOCKS {
            let mut cache = self.block_cache.lock().expect("poisoned");
            for (&b, bytes) in &blocks_map {
                self.cache_dict_block(&mut cache, b, bytes);
            }
        }
        Ok(Some(blocks_map))
    }

    /// First ordinal whose dictionary key is `>= key` (`term_count` when
    /// every key is smaller). A resident-index probe plus at most one block
    /// load.
    fn ordinal_lower_bound(&self, key: &[u8]) -> Result<u64> {
        if self.term_count == 0 {
            return Ok(0);
        }
        let index = self.dict_index()?;
        let Some(b) = index.predecessor_block(key)? else {
            return Ok(0);
        };
        let block = self.dict_block(b)?;
        let pos = crate::dict_blocks::block_lower_bound(&block, key)?;
        Ok(index.meta(b).1 + pos as u64)
    }

    /// The (at most two) contiguous ordinal ranges holding `field_id`'s RAW
    /// STRING value terms: values sorting below the numeric tag byte, and
    /// values above the tagged numeric/bool sub-range. Mirrors the per-key
    /// `is_numeric_value_token` classification of the dictionary walk —
    /// including its documented residual (a string value whose first byte
    /// IS the tag classifies as numeric on both paths).
    fn string_value_ordinal_ranges(&self, field_id: u16) -> Result<[std::ops::Range<u64>; 2]> {
        let (lower, upper) = Self::v2_field_range(field_id);
        let field_start = self.ordinal_lower_bound(&lower)?;
        let field_end = self.ordinal_lower_bound(&upper)?;
        let (num_lower, num_upper) =
            Self::v2_prefix_range(field_id, &[crate::numeric::NUMERIC_TERM_TAG]);
        let num_start = self
            .ordinal_lower_bound(&num_lower)?
            .clamp(field_start, field_end);
        let num_end = self
            .ordinal_lower_bound(&num_upper)?
            .clamp(field_start, field_end);
        Ok([field_start..num_start, num_end..field_end])
    }

    /// Resolve the dictionary keys of ASCENDING `ordinals`. Blocks are
    /// binary-searched from the resident index, missing ones batch-fetched
    /// in one round trip ([`Self::load_dict_blocks`]), and each touched
    /// block is positionally scanned once. Cost is proportional to the
    /// number of DISTINCT blocks touched (~4KB each), never to the field's
    /// full dictionary span — the whole point of resolving only a top-k's
    /// winners (#29).
    fn keys_for_ordinals(&self, ordinals: &[u64]) -> Result<Vec<Vec<u8>>> {
        if ordinals.is_empty() {
            return Ok(Vec::new());
        }
        debug_assert!(ordinals.windows(2).all(|w| w[0] < w[1]));
        let index = self.dict_index()?;
        let block_count = index.block_count();
        // block of each ordinal: last block whose first_ordinal <= ordinal
        let block_of = |ordinal: u64| -> usize {
            let mut lo = 0usize;
            let mut hi = block_count; // invariant: meta(lo..).1 <= ordinal < meta(hi..).1
            while hi - lo > 1 {
                let mid = lo + (hi - lo) / 2;
                if index.meta(mid).1 <= ordinal {
                    lo = mid;
                } else {
                    hi = mid;
                }
            }
            lo
        };
        let blocks: Vec<(usize, std::ops::Range<usize>)> = {
            // group the ascending ordinals by their block
            let mut groups: Vec<(usize, std::ops::Range<usize>)> = Vec::new();
            for (i, &ordinal) in ordinals.iter().enumerate() {
                let b = block_of(ordinal);
                match groups.last_mut() {
                    Some((gb, span)) if *gb == b => span.end = i + 1,
                    _ => groups.push((b, i..i + 1)),
                }
            }
            groups
        };
        let block_ids: Vec<usize> = blocks.iter().map(|(b, _)| *b).collect();
        let prefetched = self.load_dict_blocks(&block_ids)?;
        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(ordinals.len());
        for (b, span) in blocks {
            let block = match prefetched.as_ref().and_then(|m| m.get(&b)) {
                Some(bytes) => bytes.clone(),
                None => self.dict_block(b)?,
            };
            let first_ordinal = index.meta(b).1;
            let wanted = &ordinals[span];
            let mut next = 0usize;
            crate::dict_blocks::block_scan(&block, |pos, key| {
                while next < wanted.len() && first_ordinal + pos as u64 == wanted[next] {
                    keys.push(key.to_vec());
                    next += 1;
                }
                next < wanted.len()
            })?;
            if next != wanted.len() {
                return Err(VixError::Malformed(format!(
                    "dictionary block {b} ended before resolving {} of {} ordinals",
                    wanted.len() - next,
                    wanted.len()
                )));
            }
        }
        Ok(keys)
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
                true
            })?;
            return Ok(ordinals);
        }
        let mut ordinals = Vec::new();
        if self.term_count == 0 {
            return Ok(ordinals);
        }
        let index = self.dict_index()?;
        let all = self.dict_blocks_all()?;
        let blob_len = all.len() as u64;
        for b in 0..index.block_count() {
            let range = index.block_range(b, blob_len);
            let block = &all[range.start as usize..range.end as usize];
            let first_ordinal = index.meta(b).1;
            crate::dict_blocks::block_scan(block, |pos, key| {
                if let Some((token, field_id)) = split_key(key)
                    && field_id != KEY_FIELD_ID
                    && !is_numeric_value_token(token)
                    && matches(token)
                {
                    ordinals.push(first_ordinal + pos as u64);
                }
                true
            })?;
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
        // pass 1: validate every cell and collect the out-of-row pointer
        // windows so their records resolve in ONE batched round trip
        // instead of a point fetch per term (#27); a dense-elided term
        // short-circuits the whole union to all-ones with zero record IO
        let mut pointer_windows: Vec<(u64, u64)> = Vec::new();
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
                // out-of-row postings: the threshold — never the cell bytes
                // — selects the pointer
                if !saw_dense && self.plist_pointer_cell(doc_count, blob) {
                    pointer_windows.push(self.plist_pointer_window(blob)?);
                }
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
        let records = self.plist_records_batch(&pointer_windows)?;
        let mut records = records.iter();
        // pass 2: decode inline cells and the pre-fetched records
        for batch in &batches {
            let doc_counts = column_u64(batch, "doc_count")?;
            let postings = column_binary(batch, "postings")?;
            for (row, &doc_count) in doc_counts.iter().enumerate() {
                let cell = postings.value(row);
                if cell.is_empty() {
                    // doc_count == 0 (validated in pass 1): nothing to decode
                    continue;
                }
                let blob = if self.plist_pointer_cell(doc_count, cell) {
                    postings::record_blob(
                        records.next().expect("windows collected in pass 1 order"),
                    )?
                } else {
                    cell
                };
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
        if self.term_count > 0 {
            let index = self.dict_index()?;
            let all = self.dict_blocks_all()?;
            let blob_len = all.len() as u64;
            for b in 0..index.block_count() {
                let range = index.block_range(b, blob_len);
                let first_ordinal = index.meta(b).1;
                crate::dict_blocks::block_scan(
                    &all[range.start as usize..range.end as usize],
                    |pos, key| {
                        out.push((key.to_vec(), first_ordinal + pos as u64));
                        true
                    },
                )?;
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
pub(crate) fn parse_zone_map(raw: Option<&str>, row_count: u64) -> Option<Vec<ZoneChunk>> {
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

/// Plain byte-substring search (empty needle matches everything) — pins the
/// memmem semantics the Contains scan's hoisted `Finder` relies on.
#[cfg(test)]
fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    memchr::memmem::find(haystack, needle).is_some()
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
