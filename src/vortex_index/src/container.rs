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

//! Container plumbing: the puffin envelopes and the embedded Vortex files.
//!
//! Since format version "3" one logical core file is TWO puffin objects:
//!
//! - the DATA object (`.vix`): the `docs` blob (an embedded Vortex file) plus the data-descriptive
//!   footer properties (`version`, `row_count`, `row_group_size`, `zone_map`, `row_order`,
//!   `oversize_skips`, `columns`);
//! - the INDEX sidecar (`.vxi`): the `dict`/`dict_blocks`/`terms` blobs, the optional `plist`
//!   region and the per-file `bloom` blob, plus the index-descriptive properties (`term_count`,
//!   `tokenizer`, `dict_layout`, `key_layout`, `plist_min_docs`, `fields`, `partial_fields`).
//!   Index-off files (#40/#42) simply have NO sidecar.
//!
//! Both objects are ordinary puffin containers; each parses into a
//! [`VixContainer`] holding whatever recognized blobs it carries. This module
//! owns:
//!
//! - the property/blob-tag constants and the `fields` property entry model,
//! - writing/reading the puffin envelope over in-memory bytes ([`parse_container`]) or a ranged
//!   source ([`parse_container_ranged`]: one 64 KiB tail fetch — plus a precise refetch when the
//!   footer payload exceeds the tail window — parses the puffin footer; blobs become byte windows
//!   fetched on demand, except blobs already covered by the tail, which are sliced from it),
//! - synchronous helpers to write and scan the embedded Vortex files (vortex's async internals are
//!   driven on a [`SingleThreadRuntime`], so the public crate API stays sync). A [`BlobHandle`]
//!   scan opens in-memory blobs via `open_buffer` and ranged blobs via `open_read` over
//!   [`BlobReadAt`](crate::source::BlobReadAt) (segment reads become chunk-granular range fetches,
//!   coalesced by vortex); the blob's Vortex footer is cached on the handle so only the first scan
//!   of a blob pays the footer fetch.

use std::{collections::BTreeMap, sync::Arc};

use arrow::{
    array::{
        Array, ArrayRef as ArrowArrayRef, LargeBinaryArray, StructArray, UInt32Array, UInt64Array,
    },
    compute::cast,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use puffin::{
    FOOTER_PAYLOAD_SIZE_SIZE, FOOTER_SIZE, MAGIC, MAGIC_SIZE, MIN_FILE_SIZE, PuffinMeta,
    reader::parse_puffin_footer_from_bytes, writer::PuffinBytesWriter,
};
use serde::{Deserialize, Serialize};
use vortex::{
    VortexSessionDefault,
    array::{ArrayRef, VortexSessionExecute},
    arrow::{ArrowSessionExt, FromArrowArray, FromArrowType, ToArrowType},
    buffer::Buffer,
    compressor::BtrBlocksCompressorBuilder,
    dtype::{DType, Field as DTypeField, FieldPath},
    expr::{root, select},
    file::{OpenOptionsSessionExt, VortexFile, VortexWriteOptions, WriteStrategyBuilder},
    io::{
        runtime::{BlockingRuntime, single::SingleThreadRuntime, tokio::TokioRuntime},
        session::RuntimeSessionExt,
    },
    layout::{
        LayoutStrategy,
        layouts::{
            chunked::writer::ChunkedLayoutStrategy, collect::CollectStrategy,
            compressed::CompressingStrategy, flat::writer::FlatLayoutStrategy,
            table::TableStrategy,
        },
    },
    scan::selection::Selection,
    session::VortexSession,
};

use crate::{
    error::{Result, VixError},
    source::{RangedBlob, VixRangeSource, block_fetch},
};

/// Bytes fetched from the object tail when opening ranged: covers the puffin
/// footer (a small JSON payload) in one read for all but pathological files,
/// and doubles as a window small blobs are sliced from for free.
/// Default eager tail size; overridable via [`set_tail_fetch_size`]
/// (`ZO_VIX_EAGER_TAIL_BYTES`). Sidecars lay their small, hot blobs
/// (`dict` block index, `bloom`) LAST — nearest the footer — so a tail
/// large enough to cover them turns a cold sidecar open + term eval into
/// ONE ranged fetch. On prod, cold evals averaged ~8-9 GETs per file
/// before this was tunable.
pub const DEFAULT_TAIL_FETCH_BYTES: u64 = 64 * 1024;
static TAIL_FETCH_OVERRIDE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Set the eager tail fetch size for ranged opens (bytes; 0 keeps the
/// built-in default). Called once at process init from the engine config.
pub fn set_tail_fetch_size(bytes: u64) {
    TAIL_FETCH_OVERRIDE.store(bytes, std::sync::atomic::Ordering::Relaxed);
}

fn tail_fetch_size() -> u64 {
    match TAIL_FETCH_OVERRIDE.load(std::sync::atomic::Ordering::Relaxed) {
        0 => DEFAULT_TAIL_FETCH_BYTES,
        v => v.max(MIN_FILE_SIZE),
    }
}

/// Blob tag (puffin blob property `blob_tag`) of the dictionary blob.
pub(crate) const BLOB_TAG_DICT: &str = "dict";
/// Blob tag of the terms (doc_count + postings) blob.
pub(crate) const BLOB_TAG_TERMS: &str = "terms";
/// Blob tag of the document-store blob.
pub(crate) const BLOB_TAG_DOCS: &str = "docs";
/// Blob tag of the per-file value-bloom blob (needle pruning; see
/// `bloom.rs`). Absent on files written before the capability existed —
/// readers and the group assembler fall back to the term dictionary.
pub(crate) const BLOB_TAG_BLOOM: &str = "bloom";
/// Blob tag of the out-of-row postings region (`plist`): long postings
/// lists spill here so a ranged reader can rank/probe a term by fetching a
/// few KB (skip table + edge blocks) instead of the whole multi-MB terms
/// cell. Absent unless the file was written with
/// [`crate::VixWriterOptions::postings_plist_min_docs`] > 0.
pub(crate) const BLOB_TAG_PLIST: &str = "plist";
/// Blob tag of the DATA object's per-column chunk-stats table (H2, DESIGN
/// §4): O2-owned pruning metadata that splices through the docs-chunk
/// passthrough (see [`crate::stats`]). Sits at the object tail next to the
/// footer, so the eager tail fetch usually covers it; absent on files
/// written before the capability (readers fail open) and on empty files.
pub(crate) const BLOB_TAG_STATS: &str = "stats";

/// Puffin blob type id of the dictionary blob. Type ids are free-form
/// strings in the puffin footer; a blob is recognized by (tag, type id) and
/// everything else is skipped.
pub(crate) const BLOB_TYPE_DICT: &str = "o2-vix-dict-v2";
/// Blob tag of the dictionary BLOCKS region: raw concatenated
/// prefix-compressed key blocks, addressed by the `dict` blob's index
/// (deliberately NOT a Vortex file — readers range-fetch single blocks).
pub(crate) const BLOB_TAG_DICT_BLOCKS: &str = "dict_blocks";
pub(crate) const BLOB_TYPE_DICT_BLOCKS: &str = "o2-vix-dictblocks-v1";
/// Blob type id of the terms blob.
pub(crate) const BLOB_TYPE_TERMS: &str = "o2-vix-terms-v1";
/// Blob type id of the docs blob.
pub(crate) const BLOB_TYPE_DOCS: &str = "o2-vix-docs-v1";
/// Blob type id of the per-file value-bloom blob.
pub(crate) const BLOB_TYPE_BLOOM: &str = "o2-vix-bloom-v1";
/// Blob type id of the out-of-row postings region.
pub(crate) const BLOB_TYPE_PLIST: &str = "o2-vix-plist-v1";
/// Blob type id of the per-column chunk-stats table.
pub(crate) const BLOB_TYPE_STATS: &str = "o2-vix-stats-v1";

pub(crate) const PROP_VERSION: &str = "version";
pub(crate) const PROP_ROW_COUNT: &str = "row_count";
pub(crate) const PROP_TERM_COUNT: &str = "term_count";
pub(crate) const PROP_ROW_GROUP_SIZE: &str = "row_group_size";
pub(crate) const PROP_FIELDS: &str = "fields";
pub(crate) const PROP_PARTIAL_FIELDS: &str = "partial_fields";
/// Per-field count of raw values skipped for exceeding the writer's
/// `max_raw_term_len` (JSON object `{"field": count}`; absent when nothing
/// was skipped, including every legacy file). The dictionary-serve
/// reconciliation treats it as an exact allowance — `indexed + skipped ==
/// key-term docs` serves with the skipped values omitted (the 2026-08-12
/// performance-first trade), any other shortfall still refuses — and
/// dictionary merges SUM inputs' maps so merged files keep serving.
pub(crate) const PROP_OVERSIZE_SKIPS: &str = "oversize_skips";
pub(crate) const PROP_TOKENIZER: &str = "tokenizer";
/// DATA-object docs-column field list (format "3"): a JSON array of the
/// docs blob's columns minus the reserved `_source`/`_original` columns —
/// the file's field-presence declaration, readable without the sidecar
/// (feeds pruning, and lets a sidecar-less open still answer column-store
/// routing). Since the H2 stats extension each entry is a
/// `[name, present_rows]` pair (per-column non-null row counts, summed
/// across merges); plain-name entries (M1 files) parse with an UNKNOWN
/// count. Index-off files (#40/#42) have no sidecar at all; their readers
/// synthesize column-store field entries from this list.
pub(crate) const PROP_COLUMNS: &str = "columns";
/// Doc-count threshold at/above which a term's postings live in the `plist`
/// blob (its terms cell holds a 12-byte `u64 offset ++ u32 len` pointer
/// instead of the encoded list). Present ⇒ the file is plist-capable and the
/// READER MUST resolve pointer cells; absent ⇒ every cell is inline
/// (pre-plist format). The threshold gates by `doc_count`, which the reader
/// always has alongside the cell, so no cell-sniffing is ever needed.
pub(crate) const PROP_PLIST_MIN_DOCS: &str = "plist_min_docs";
/// Declares how the `dict` blob's `fst` column is chunked — format
/// self-description. Written as [`DICT_LAYOUT_CELLS`] by
/// [`dict_strategy`]-era writers: one chunk per row-group cell,
/// point-readable. When ABSENT the file predates lazy dict loading (its
/// whole fst column is typically ONE chunk); readers behave identically
/// either way — per-cell lazy loads with per-evaluation batching (on a
/// one-chunk file each batch decodes that chunk once). Deliberately NOT a
/// widen-to-all-cells switch: retaining every decoded cell would balloon
/// touched readers to their full dictionaries and thrash the reader cache
/// (measured on the 200M benchmark).
pub(crate) const PROP_DICT_LAYOUT: &str = "dict_layout";
pub(crate) const DICT_LAYOUT_CELLS: &str = "cells";
/// THE dictionary layout: the `dict` blob is a [`crate::dict_blocks`] block
/// INDEX (restart-compressed first keys + per-block meta) and the
/// `dict_blocks` blob holds the prefix-compressed key blocks it addresses.
/// Files declaring any other layout (or none) are unreadable — the
/// monolithic-FST layouts were retired without read support (owner call,
/// 2026-08-03; ENGINE-BACKLOG #18).
pub(crate) const DICT_LAYOUT_BLOCKS: &str = "blocks";
/// Composite-KEY layout of the `dict` blob (orthogonal to
/// [`PROP_DICT_LAYOUT`], which describes chunking). Exactly
/// [`KEY_LAYOUT_FID_V2`] — field-major `{fid}{token}` keys
/// (`query::write_composite`) — is supported; every writer stamps it.
/// ABSENT marks a pre-v2 file (the retired `{token}\x00{fid}` layout, dead
/// since the .32 cutover wipe) and is a hard open error, as is any other
/// value — a silent field-major read of a foreign dictionary would return
/// wrong results, never do that.
pub(crate) const PROP_KEY_LAYOUT: &str = "key_layout";
pub(crate) const KEY_LAYOUT_FID_V2: &str = "fid_v2";
/// Per-chunk `_timestamp` zone table of the `docs` blob (§2/§6). A JSON array
/// of `[row_count, ts_min, ts_max]` triples, one per PHYSICAL chunk of the
/// `_timestamp` column in the order a scan iterates them; row offsets are the
/// running prefix sum (derived by the reader, not stored). Lets whole chunks
/// contribute to a time bucket / range without decoding their values
/// (zone-map histogram/count fast paths). Written for every non-empty file by
/// current writers; ABSENT on files written before this landed — such files
/// fall back to the decode path unchanged (presence probe, no version
/// dispatch).
pub(crate) const PROP_ZONE_MAP: &str = "zone_map";
/// Physical row order of the `docs` blob (#51c-c). Values:
/// [`ROW_ORDER_TS_DESC`] — rows stored globally `_timestamp` DESC (the
/// storage convention every writer upheld before this property existed;
/// stamped explicitly by every writer since) — and [`ROW_ORDER_CONCAT`] —
/// a concatenation-order compaction merge output: the inputs' row runs
/// stored back-to-back, each run internally DESC but the file NOT globally
/// sorted. ABSENT means [`ROW_ORDER_TS_DESC`] (all historical files are
/// sorted — they must read exactly as before). Any UNKNOWN value is treated
/// as NOT sorted: assuming order that is not there returns wrong
/// ORDER BY/top-N results, while the reverse only costs a real sort.
pub(crate) const PROP_ROW_ORDER: &str = "row_order";
pub(crate) const ROW_ORDER_TS_DESC: &str = "ts_desc";
pub(crate) const ROW_ORDER_CONCAT: &str = "concat";
/// §4 field-presence completeness marker: `"true"` asserts that EVERY field
/// present in ANY row's `_source` is also a docs column of this file (the
/// v2 all-present-columns invariant, DESIGN §2). Only then is "column
/// absent from the `columns` list" a proof that every row is NULL for the
/// field — the file-level presence skip's soundness condition. Producers
/// whose batches uphold the invariant stamp it; a merge output carries it
/// only when EVERY input did (values hiding in an incomplete input's
/// `_source` survive a decode-merge without ever becoming columns). ABSENT
/// or any other value = incomplete (fail-open: no absent-column pruning).
pub(crate) const PROP_COLUMNS_COMPLETE: &str = "columns_complete";
/// §4/§6.2 REGION table of a [`ROW_ORDER_CONCAT`] file: a JSON array of row
/// COUNTS, one per region in stored order, summing to `row_count`. Within
/// each region the rows are `_timestamp` DESC (piecewise order), so an
/// `ORDER BY _timestamp DESC` read can k-way merge the regions instead of
/// paying a full sort. Stamped only on concat outputs whose desc-run
/// decomposition is PROVEN: the writer derives it from the actual stored
/// `_timestamp` values on the decode path and splices the inputs' own
/// region tables on the passthrough path; any input without a proven
/// decomposition poisons the property (absent = piecewise order unknown —
/// readers keep the full-sort path, fail-open). Never stamped on
/// [`ROW_ORDER_TS_DESC`] files (the whole file is one region by
/// definition). Region boundaries are ROW offsets (prefix sums) and are
/// NOT required to align with `zone_map` chunk boundaries.
pub(crate) const PROP_ROW_REGIONS: &str = "row_regions";

/// Parsed [`PROP_ROW_ORDER`] (see the constant for the write-side rules) —
/// exposed by [`crate::VixReader::row_order`] and
/// [`crate::VixDocs::row_order`] so every order-dependent read fast path
/// (declared file sort order, first/last-row stats, first-set-bits top-N
/// candidates) can refuse to trust the order of a concat file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowOrder {
    /// Globally `_timestamp` DESC (missing property == this).
    TsDesc,
    /// NOT globally sorted (concatenated runs, or an unknown future value —
    /// the fail-safe reading).
    Concat,
}

impl RowOrder {
    /// Parse the property value; `None` (historical file) is
    /// [`RowOrder::TsDesc`], unknown values are [`RowOrder::Concat`]
    /// (fail-safe: never assume order).
    pub(crate) fn from_property(raw: Option<&str>) -> Self {
        match raw {
            None | Some(ROW_ORDER_TS_DESC) => Self::TsDesc,
            Some(ROW_ORDER_CONCAT) => Self::Concat,
            Some(other) => {
                log::warn!(
                    "vix: unknown row_order property {other:?}; treating the file as NOT \
                     time-sorted (order-dependent fast paths disabled for it)"
                );
                Self::Concat
            }
        }
    }

    /// Whether the file's rows are globally `_timestamp` DESC — the gate for
    /// every order-dependent fast path.
    pub fn is_ts_desc(self) -> bool {
        matches!(self, Self::TsDesc)
    }
}

/// The `version` property value this crate writes and requires on BOTH
/// objects — the one future-evolution discriminator of the format. Readers
/// accept exactly this value; bump it on any breaking format change. "3" is
/// the sidecar split (2026-08-17): the data object carries only `docs` +
/// data-descriptive properties, the index moved to the `.vxi` sidecar.
/// Version "2" (embedded-index single container) was retired without a read
/// path — the fleet wipe made that free. Extra or unknown properties are
/// ignored generically.
pub(crate) const VIX_FORMAT_VERSION: &str = "3";
/// `tokenizer` property written by [`crate::VixWriter`] — identifies the
/// canonical [`crate::o2_tokenize`] behavior. Its ONLY consumer is
/// `check_merge_inputs`: files stamped otherwise (pre-fix `"o2-v1"`) force
/// the compaction rebuild, which re-tokenizes from `_source` and stamps the
/// output with this identifier — old files converge on their next
/// compaction. The value is an opaque identifier, not API naming: it stays
/// `"o2-v2"` so every existing file passes the merge-compat equality check
/// (renaming it would force a rebuild whenever old and new files merge).
pub(crate) const TOKENIZER_ID: &str = "o2-v2";

/// `fields` property type markers.
pub(crate) const FIELD_TYPE_TERM: &str = "term";
pub(crate) const FIELD_TYPE_FTS: &str = "fts";
pub(crate) const FIELD_TYPE_CS: &str = "cs";
/// #52 bloom-only: the field's values are in the composite bloom + docs
/// columns but NOT the term dictionary. Readers that predate the type treat
/// it as unknown = not value-indexed = filter-back scan — exactly the
/// intended semantics, so no format/floor change.
pub(crate) const FIELD_TYPE_BLOOM: &str = "bloom";

/// One entry of the `fields` file property. For term-indexed fields the array
/// index equals the field id; column-store-only entries (e.g. `_timestamp`)
/// are appended after all term entries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct FieldEntry {
    pub name: String,
    pub types: Vec<String>,
}

impl FieldEntry {
    pub fn has_type(&self, ty: &str) -> bool {
        self.types.iter().any(|t| t == ty)
    }
}

/// Require a DATA object's `version` property to be [`VIX_FORMAT_VERSION`]
/// — the single clear rejection shared by every open path. Absent or
/// different values error (pre-v3 embedded-index files carry "2" and are
/// unreadable by design — no legacy read path); any other property the
/// reader does not know is ignored.
pub(crate) fn require_supported_data_format(properties: &BTreeMap<String, String>) -> Result<()> {
    match properties.get(PROP_VERSION).map(String::as_str) {
        Some(VIX_FORMAT_VERSION) => Ok(()),
        Some(other) => Err(VixError::UnsupportedFormat(format!(
            "data object version {other:?}, reader supports {VIX_FORMAT_VERSION}"
        ))),
        None => Err(VixError::UnsupportedFormat(format!(
            "data object has no version property, reader supports {VIX_FORMAT_VERSION}"
        ))),
    }
}

/// Require an INDEX sidecar's `version` property to be
/// [`VIX_FORMAT_VERSION`] and its `key_layout` property to be
/// [`KEY_LAYOUT_FID_V2`]. The key-layout check is index-side only (the
/// dictionary lives here): ABSENT or any OTHER value is a HARD error — a
/// field-major probe against a foreign layout silently returns wrong
/// results, which must never happen.
pub(crate) fn require_supported_index_format(properties: &BTreeMap<String, String>) -> Result<()> {
    match properties.get(PROP_VERSION).map(String::as_str) {
        Some(VIX_FORMAT_VERSION) => {}
        Some(other) => {
            return Err(VixError::UnsupportedFormat(format!(
                "index sidecar version {other:?}, reader supports {VIX_FORMAT_VERSION}"
            )));
        }
        None => {
            return Err(VixError::UnsupportedFormat(format!(
                "index sidecar has no version property, reader supports {VIX_FORMAT_VERSION}"
            )));
        }
    }
    match properties.get(PROP_KEY_LAYOUT).map(String::as_str) {
        Some(KEY_LAYOUT_FID_V2) => Ok(()),
        None => Err(VixError::UnsupportedFormat(
            "index sidecar without a key_layout property is not supported; rebuild from _source"
                .to_string(),
        )),
        Some(other) => Err(VixError::UnsupportedFormat(format!(
            "key_layout {other:?} not supported by this reader"
        ))),
    }
}

/// One embedded Vortex file of the container: fully in memory, or a byte
/// window of a ranged source fetched on demand.
pub(crate) enum BlobHandle {
    /// The blob bytes, fully in memory.
    Mem(Bytes),
    /// A window of a remote object; scans fetch only the segments they touch.
    Ranged(RangedBlob),
}

impl BlobHandle {
    /// The blob's complete bytes: an in-memory slice for free, ONE ranged
    /// fetch otherwise. For small metadata blobs (`stats`-class) only —
    /// data-bearing blobs stream through their scan paths.
    pub(crate) fn bytes(&self) -> Result<Bytes> {
        match self {
            BlobHandle::Mem(bytes) => Ok(bytes.clone()),
            BlobHandle::Ranged(blob) => {
                crate::source::block_fetch(blob.source.as_ref(), blob.range.clone())
            }
        }
    }

    /// The blob's byte length (no fetch).
    pub(crate) fn len(&self) -> u64 {
        match self {
            BlobHandle::Mem(bytes) => bytes.len() as u64,
            BlobHandle::Ranged(blob) => blob.range.end - blob.range.start,
        }
    }
}

impl std::fmt::Debug for BlobHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlobHandle::Mem(bytes) => f.debug_tuple("Mem").field(&bytes.len()).finish(),
            BlobHandle::Ranged(blob) => f.debug_tuple("Ranged").field(blob).finish(),
        }
    }
}

/// The parsed puffin envelope of ONE object (a `.vix` data object or a
/// `.vxi` index sidecar): file properties plus handles to whichever
/// recognized blobs it carries — `docs` only for data objects, the index
/// blobs for sidecars.
pub(crate) struct VixContainer {
    pub properties: BTreeMap<String, String>,
    pub dict: Option<BlobHandle>,
    /// The dictionary blocks region (paired with `dict`, the block index).
    pub dict_blocks: Option<BlobHandle>,
    pub terms: Option<BlobHandle>,
    pub docs: Option<BlobHandle>,
    /// Per-file value blooms for configured needle fields (may be absent).
    pub bloom: Option<BlobHandle>,
    /// Out-of-row postings region (absent on files written without
    /// [`crate::VixWriterOptions::postings_plist_min_docs`]).
    pub plist: Option<BlobHandle>,
    /// The DATA object's per-column chunk-stats table (H2; absent on
    /// pre-stats and empty files — readers fail open).
    pub stats: Option<BlobHandle>,
}

/// Sort the recognized blobs of a parsed puffin footer into a container,
/// mapping each blob's absolute byte range through `slice`. Blobs with an
/// unknown (tag, type id) are skipped: the container format tolerates
/// additions, and whether the recognized set makes a readable file is
/// decided by the `format` property check at open.
fn container_from_meta(
    meta: PuffinMeta,
    total_len: u64,
    mut slice: impl FnMut(std::ops::Range<u64>) -> BlobHandle,
) -> Result<VixContainer> {
    let mut dict = None;
    let mut dict_blocks = None;
    let mut terms = None;
    let mut docs = None;
    let mut bloom = None;
    let mut plist = None;
    let mut stats = None;
    for blob in &meta.blobs {
        let range = blob.get_offset(None);
        if range.start > range.end || range.end > total_len {
            return Err(VixError::Malformed(format!(
                "blob range {}..{} out of bounds (file size {total_len})",
                range.start, range.end,
            )));
        }
        let tag = blob.properties.get("blob_tag").map(String::as_str);
        let target = match (tag, blob.blob_type.as_str()) {
            (Some(BLOB_TAG_DICT), BLOB_TYPE_DICT) => &mut dict,
            (Some(BLOB_TAG_DICT_BLOCKS), BLOB_TYPE_DICT_BLOCKS) => &mut dict_blocks,
            (Some(BLOB_TAG_TERMS), BLOB_TYPE_TERMS) => &mut terms,
            (Some(BLOB_TAG_DOCS), BLOB_TYPE_DOCS) => &mut docs,
            (Some(BLOB_TAG_BLOOM), BLOB_TYPE_BLOOM) => &mut bloom,
            (Some(BLOB_TAG_PLIST), BLOB_TYPE_PLIST) => &mut plist,
            (Some(BLOB_TAG_STATS), BLOB_TYPE_STATS) => &mut stats,
            _ => continue,
        };
        *target = Some(slice(range));
    }

    Ok(VixContainer {
        properties: meta.properties,
        dict,
        dict_blocks,
        terms,
        docs,
        bloom,
        plist,
        stats,
    })
}

/// Parse the puffin footer of `data` and slice out the `.vix` blobs.
pub(crate) fn parse_container(data: &Bytes) -> Result<VixContainer> {
    let meta = parse_puffin_footer_from_bytes(data)
        .map_err(|e| VixError::Malformed(format!("puffin footer: {e:#}")))?;
    container_from_meta(meta, data.len() as u64, |range| {
        BlobHandle::Mem(data.slice(range.start as usize..range.end as usize))
    })
}

/// Parse the puffin envelope of a ranged source: one tail fetch of up to
/// The configured eager-tail bytes (a second, precise fetch when the footer
/// payload exceeds them) yield the footer; blobs fully covered by the fetched
/// tail are sliced from it, all others become on-demand windows of `source`.
pub(crate) fn parse_container_ranged(source: &Arc<dyn VixRangeSource>) -> Result<VixContainer> {
    parse_container_ranged_with_tail(source, tail_fetch_size())
}

/// Parse a ranged container with a caller-selected initial tail probe.
///
/// Data-only readers use the built-in 64 KiB probe even when the process-wide
/// override is larger for sidecars: a data object has no tail-resident term
/// dictionary or bloom to amortize the extra bytes. If a pathological puffin
/// footer exceeds the probe, the parser still performs the same exact second
/// fetch as the ordinary ranged open.
pub(crate) fn parse_container_ranged_with_tail(
    source: &Arc<dyn VixRangeSource>,
    eager_tail_bytes: u64,
) -> Result<VixContainer> {
    let total = source.len();
    if total < MIN_FILE_SIZE {
        return Err(VixError::Malformed(format!(
            "file too small to be a puffin container: {total} bytes"
        )));
    }

    // Tail probe. The footer region is `HeadMagic[4] + payload + FOOTER_SIZE`
    // at the very end of the file; read the payload size out of the footer
    // tail and refetch precisely when the probe fell short.
    let mut tail_start = total.saturating_sub(eager_tail_bytes.max(MIN_FILE_SIZE));
    let mut tail = block_fetch(source.as_ref(), tail_start..total)?;
    let footer_tail = &tail[tail.len() - FOOTER_SIZE as usize..];
    if footer_tail[(FOOTER_SIZE - MAGIC_SIZE) as usize..] != MAGIC {
        return Err(VixError::Malformed(format!(
            "puffin footer magic mismatch in {}",
            source.describe()
        )));
    }
    let payload_size = u64::from(u32::from_le_bytes(
        footer_tail[..FOOTER_PAYLOAD_SIZE_SIZE as usize]
            .try_into()
            .expect("fixed 4-byte slice"),
    ));
    let footer_region = MAGIC_SIZE + payload_size + FOOTER_SIZE;
    if footer_region > total {
        return Err(VixError::Malformed(format!(
            "puffin footer payload of {payload_size} bytes exceeds the file size {total}"
        )));
    }
    if footer_region > tail.len() as u64 {
        tail_start = total - footer_region;
        tail = block_fetch(source.as_ref(), tail_start..total)?;
    }

    // The parser only looks at end-anchored offsets, so the fetched suffix
    // parses exactly like the whole file would.
    let meta = parse_puffin_footer_from_bytes(&tail)
        .map_err(|e| VixError::Malformed(format!("puffin footer: {e:#}")))?;
    container_from_meta(meta, total, |range| {
        if range.start >= tail_start {
            // Already fetched as part of the tail (tiny files): slice, free.
            let start = (range.start - tail_start) as usize;
            let end = (range.end - tail_start) as usize;
            BlobHandle::Mem(tail.slice(start..end))
        } else {
            BlobHandle::Ranged(RangedBlob::new(Arc::clone(source), range))
        }
    })
}

/// One produced blob awaiting container assembly: in memory (small blobs,
/// and every blob of the historical in-memory path) or spooled to an
/// unlinked temp file by its producer (vocabulary-proportional index blobs
/// of a budget-crossing rebuild — see `VixWriter::assemble_index_blobs`).
/// Spooled payloads stream into the container without ever residing in RAM.
pub(crate) enum BlobPart {
    Mem(Vec<u8>),
    /// The file cursor must be at the payload start (byte 0); `len` is the
    /// payload length in bytes.
    Spooled {
        file: std::fs::File,
        len: u64,
    },
}

/// Assemble the final puffin container from properties and pre-built blobs
/// (`(blob type id, blob tag, bytes)`).
pub(crate) fn build_container(
    properties: Vec<(String, String)>,
    blobs: Vec<(&'static str, &'static str, Vec<u8>)>,
) -> Result<Vec<u8>> {
    build_container_parts(
        properties,
        blobs
            .into_iter()
            .map(|(blob_type, tag, data)| (blob_type, tag, BlobPart::Mem(data)))
            .collect(),
    )
}

/// [`build_container`] over [`BlobPart`]s: in-memory blobs append exactly as
/// before; spooled blobs stream from their temp files, so the peak while
/// assembling is ONE copy of the container (the returned buffer) plus a
/// bounded copy window — instead of container + every blob simultaneously.
pub(crate) fn build_container_parts(
    properties: Vec<(String, String)>,
    blobs: Vec<(&'static str, &'static str, BlobPart)>,
) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    {
        let mut writer = PuffinBytesWriter::new(&mut buf);
        for (key, value) in properties {
            writer.set_property(key, value);
        }
        for (blob_type, tag, part) in blobs {
            match part {
                BlobPart::Mem(data) => writer
                    .add_blob(&data, blob_type, tag.to_string())
                    .map_err(|e| VixError::Writer(format!("puffin blob write: {e:#}")))?,
                BlobPart::Spooled { file, len } => writer
                    .add_blob_from(
                        std::io::BufReader::with_capacity(1024 * 1024, file),
                        len,
                        blob_type,
                        tag.to_string(),
                    )
                    .map_err(|e| VixError::Writer(format!("puffin blob stream: {e:#}")))?,
            }
        }
        writer
            .finish()
            .map_err(|e| VixError::Writer(format!("puffin finish: {e:#}")))?;
    }
    Ok(buf)
}

/// Assemble the final DATA-object puffin container around a `docs` blob that
/// was already STREAMED into the sink (which holds the puffin `MAGIC`
/// followed by exactly `docs_len` docs-blob bytes — a [`DocsBlobEncoder`]'s
/// output): the docs bytes are recorded in place — never copied — and any
/// remaining blobs + the footer append after them, each dropped as soon as
/// it is written (since the v3 sidecar split the data object carries no
/// blobs beyond `docs`, so `blobs` is empty on every production path). A
/// spooled sink keeps the whole container out of RAM.
pub(crate) fn finish_streamed_container(
    sink: ContainerSink,
    docs_len: u64,
    properties: Vec<(String, String)>,
    blobs: Vec<(&'static str, &'static str, Vec<u8>)>,
) -> Result<VixOutput> {
    fn append<W: std::io::Write>(
        sink: W,
        docs_len: u64,
        properties: Vec<(String, String)>,
        blobs: Vec<(&'static str, &'static str, Vec<u8>)>,
    ) -> Result<u64> {
        let mut writer = PuffinBytesWriter::with_written(sink, MAGIC_SIZE);
        writer.add_prewritten_blob(BLOB_TYPE_DOCS, BLOB_TAG_DOCS.to_string(), docs_len);
        for (key, value) in properties {
            writer.set_property(key, value);
        }
        for (blob_type, tag, data) in blobs {
            writer
                .add_blob(&data, blob_type, tag.to_string())
                .map_err(|e| VixError::Writer(format!("puffin blob write: {e:#}")))?;
            drop(data);
        }
        writer
            .finish()
            .map_err(|e| VixError::Writer(format!("puffin finish: {e:#}")))
    }

    match sink {
        ContainerSink::Mem(mut buf) => {
            debug_assert_eq!(buf.len() as u64, MAGIC_SIZE + docs_len);
            debug_assert_eq!(buf[..MAGIC_SIZE as usize], MAGIC);
            append(&mut buf, docs_len, properties, blobs)?;
            Ok(VixOutput::Bytes(buf))
        }
        ContainerSink::File(mut file) => {
            let mut sink = std::io::BufWriter::with_capacity(1024 * 1024, file.as_file_mut());
            let len = append(&mut sink, docs_len, properties, blobs)?;
            use std::io::Write;
            sink.flush()
                .map_err(|e| VixError::Writer(format!("flush container spool: {e}")))?;
            drop(sink);
            Ok(VixOutput::Spooled { file, len })
        }
    }
}

/// A finished `.vix` container: in memory (the move-job path and tests
/// without a spool dir) or spooled to a temp file on the local data volume
/// (the compaction paths — a multi-GB container never has to reside in
/// RAM; the file auto-deletes on drop unless persisted).
pub enum VixOutput {
    Bytes(Vec<u8>),
    Spooled {
        file: tempfile::NamedTempFile,
        len: u64,
    },
}

impl VixOutput {
    pub fn len(&self) -> u64 {
        match self {
            Self::Bytes(data) => data.len() as u64,
            Self::Spooled { len, .. } => *len,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The spool path, when spooled.
    pub fn spool_path(&self) -> Option<&std::path::Path> {
        match self {
            Self::Bytes(_) => None,
            Self::Spooled { file, .. } => Some(file.path()),
        }
    }

    /// The container bytes without consuming (clones / reads the spool
    /// back — tests and small-file conveniences; production uploads stream
    /// from the path).
    pub fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        match self {
            Self::Bytes(data) => Ok(data.clone()),
            Self::Spooled { file, .. } => Ok(std::fs::read(file.path())?),
        }
    }

    /// Materialize the container bytes (reads the spool back — tests and
    /// small-file conveniences; production uploads stream from the path).
    pub fn into_bytes(self) -> anyhow::Result<Vec<u8>> {
        match self {
            Self::Bytes(data) => Ok(data),
            Self::Spooled { file, .. } => Ok(std::fs::read(file.path())?),
        }
    }
}

impl std::fmt::Debug for VixOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bytes(data) => f.debug_tuple("Bytes").field(&data.len()).finish(),
            Self::Spooled { file, len } => f
                .debug_struct("Spooled")
                .field("path", &file.path())
                .field("len", len)
                .finish(),
        }
    }
}

/// The container sink a [`DocsBlobEncoder`] streams into: the puffin MAGIC
/// plus the docs blob land here first, the index blobs and footer append
/// after (see [`finish_streamed_container`]).
pub(crate) enum ContainerSink {
    Mem(Vec<u8>),
    File(tempfile::NamedTempFile),
}

impl ContainerSink {
    /// Create the sink and write the puffin MAGIC. `spool_dir = None` keeps
    /// the container in memory.
    fn create(spool_dir: Option<&std::path::Path>) -> Result<Self> {
        match spool_dir {
            None => Ok(Self::Mem(MAGIC.to_vec())),
            Some(dir) => {
                std::fs::create_dir_all(dir)
                    .map_err(|e| VixError::Writer(format!("create spool dir {dir:?}: {e}")))?;
                let mut file = tempfile::Builder::new()
                    .prefix("vix-out-")
                    .suffix(".vix.spool")
                    .tempfile_in(dir)
                    .map_err(|e| VixError::Writer(format!("create spool in {dir:?}: {e}")))?;
                use std::io::Write;
                file.write_all(&MAGIC)
                    .map_err(|e| VixError::Writer(format!("write spool magic: {e}")))?;
                Ok(Self::File(file))
            }
        }
    }
}

/// `io::Write` adapter counting the bytes that pass through — the docs
/// blob's length inside the container sink.
struct CountingWriter<W> {
    inner: W,
    written: u64,
}

impl<W: std::io::Write> std::io::Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.written += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// One message to a [`DocsBlobEncoder`] worker.
enum DocsEncodeMsg {
    Batch(RecordBatch),
    /// #51c: an already-encoded docs chunk (a vortex struct array read
    /// straight off an input file's docs blob) — valid only for encoders
    /// spawned with `docs_passthrough`; the passthrough strategy writes its
    /// non-canonical columns without recompressing them.
    Encoded(ArrayRef),
    Finish,
}

/// Streaming encoder of the `docs` blob: a dedicated worker thread owns the
/// incremental vortex writer and encodes batches AS THEY ARE PUSHED, writing
/// the blob bytes straight into the (`MAGIC`-prefixed) final container
/// buffer. The writer side therefore never holds the file's decoded rows —
/// in-flight memory is the bounded channel plus vortex's per-chunk strategy
/// buffers, instead of every stored batch until `finish` (a 10 GB-original
/// compaction merge used to keep ~10 GB of arrow batches alive through the
/// whole docs encode).
///
/// The channel is small on purpose: a slow encode backpressures the pushers
/// (the merge decode threads / the move job), keeping the pipeline bounded
/// end to end. Encode parallelism inside the worker mirrors
/// [`write_vortex_blob`] (`encode_threads` > 1 submits chunk compression to
/// the bounded process-wide CPU executor). Dropping the encoder without
/// [`Self::signal_finish`] aborts the worker on its next receive.
pub(crate) struct DocsBlobEncoder {
    tx: Option<std::sync::mpsc::SyncSender<DocsEncodeMsg>>,
    handle: Option<std::thread::JoinHandle<Result<(ContainerSink, u64)>>>,
}

impl DocsBlobEncoder {
    /// Spawn the worker. `rows_per_chunk` is the locked docs chunking
    /// (`0` keeps vortex's default — the empty-file shape); `spool_dir`
    /// spools the container to a temp file there instead of RAM.
    /// `docs_passthrough` (#51c) swaps [`docs_strategy`] for
    /// [`docs_passthrough_strategy`]: already-encoded chunks pushed via
    /// [`Self::push_encoded`] are written without recompression, arrow
    /// batches still compress as usual (sliced to `rows_per_chunk` windows
    /// here, since the passthrough strategy has no repartition step).
    pub(crate) fn spawn(
        schema: Arc<Schema>,
        rows_per_chunk: usize,
        encode_threads: usize,
        spool_dir: Option<std::path::PathBuf>,
        docs_passthrough: bool,
        fail_open: Arc<std::sync::atomic::AtomicU64>,
    ) -> Result<Self> {
        let (tx, rx) = std::sync::mpsc::sync_channel(2);
        let handle = std::thread::Builder::new()
            .name("vix-docs-encode".to_string())
            .spawn(move || {
                run_docs_encoder(
                    &schema,
                    rows_per_chunk,
                    encode_threads,
                    spool_dir,
                    docs_passthrough,
                    fail_open,
                    &rx,
                )
            })
            .map_err(|e| VixError::Writer(format!("spawn docs encoder thread: {e}")))?;
        Ok(Self {
            tx: Some(tx),
            handle: Some(handle),
        })
    }

    /// Queue one batch for encoding (blocks when the channel is full).
    pub(crate) fn push(&mut self, batch: RecordBatch) -> Result<()> {
        self.send(DocsEncodeMsg::Batch(batch))
    }

    /// Queue one already-encoded docs chunk (#51c). The worker rejects it
    /// unless the encoder was spawned with `docs_passthrough`.
    pub(crate) fn push_encoded(&mut self, chunk: ArrayRef) -> Result<()> {
        self.send(DocsEncodeMsg::Encoded(chunk))
    }

    fn send(&mut self, msg: DocsEncodeMsg) -> Result<()> {
        let tx = self
            .tx
            .as_ref()
            .ok_or_else(|| VixError::Writer("docs encoder already finished".to_string()))?;
        if tx.send(msg).is_err() {
            return Err(self.take_failure());
        }
        Ok(())
    }

    /// Signal that no more batches are coming; the worker drains its queue
    /// and finalizes the vortex file while the caller keeps working (index
    /// blob encode overlaps the docs tail). Idempotent.
    pub(crate) fn signal_finish(&mut self) -> Result<()> {
        let Some(tx) = self.tx.take() else {
            return Ok(());
        };
        if tx.send(DocsEncodeMsg::Finish).is_err() {
            return Err(self.take_failure());
        }
        Ok(())
    }

    /// Wait for the worker and return the container sink holding
    /// `MAGIC` + the encoded docs blob, plus the docs blob's byte length
    /// (feed both to [`finish_streamed_container`]).
    pub(crate) fn join(mut self) -> Result<(ContainerSink, u64)> {
        self.signal_finish()?;
        match self.handle.take() {
            Some(handle) => handle
                .join()
                .map_err(|_| VixError::Writer("docs encoder thread panicked".to_string()))?,
            None => Err(VixError::Writer(
                "docs encoder result already taken".to_string(),
            )),
        }
    }

    /// The worker died before accepting a message: join it and surface its
    /// real error instead of a bare channel disconnect.
    fn take_failure(&mut self) -> VixError {
        self.tx = None;
        match self.handle.take() {
            Some(handle) => match handle.join() {
                Ok(Ok(_)) => {
                    VixError::Writer("docs encoder stopped early without an error".to_string())
                }
                Ok(Err(e)) => e,
                Err(_) => VixError::Writer("docs encoder thread panicked".to_string()),
            },
            None => VixError::Writer("docs encoder already failed".to_string()),
        }
    }
}

/// The worker body: encode received batches into a `MAGIC`-prefixed buffer.
/// Parallel writers share the process CPU executor.
fn run_docs_encoder(
    schema: &Schema,
    rows_per_chunk: usize,
    encode_threads: usize,
    spool_dir: Option<std::path::PathBuf>,
    docs_passthrough: bool,
    fail_open: Arc<std::sync::atomic::AtomicU64>,
    rx: &std::sync::mpsc::Receiver<DocsEncodeMsg>,
) -> Result<(ContainerSink, u64)> {
    let runtime = SingleThreadRuntime::default();
    let mut sink = ContainerSink::create(spool_dir.as_deref())?;
    let run = |sink: &mut ContainerSink, session| -> Result<u64> {
        match sink {
            ContainerSink::Mem(buf) => {
                let before = buf.len() as u64;
                encode_docs_stream(
                    &runtime,
                    session,
                    schema,
                    rows_per_chunk,
                    docs_passthrough,
                    Arc::clone(&fail_open),
                    rx,
                    &mut *buf,
                )?;
                Ok(buf.len() as u64 - before)
            }
            ContainerSink::File(file) => {
                let mut counting = CountingWriter {
                    inner: std::io::BufWriter::with_capacity(1024 * 1024, file.as_file_mut()),
                    written: 0,
                };
                encode_docs_stream(
                    &runtime,
                    session,
                    schema,
                    rows_per_chunk,
                    docs_passthrough,
                    Arc::clone(&fail_open),
                    rx,
                    &mut counting,
                )?;
                use std::io::Write;
                counting
                    .flush()
                    .map_err(|e| VixError::Writer(format!("flush docs spool: {e}")))?;
                Ok(counting.written)
            }
        }
    };
    let result = if encode_threads > 1 {
        let session = VortexSession::default()
            .with_handle(crate::cpu_executor::shared_vortex_execution_handle()?);
        run(&mut sink, session)
    } else {
        let session = VortexSession::default().with_handle(runtime.handle());
        run(&mut sink, session)
    };
    result.map(|docs_len| (sink, docs_len))
}

#[allow(clippy::too_many_arguments)]
fn encode_docs_stream<W: std::io::Write + Unpin>(
    runtime: &SingleThreadRuntime,
    session: VortexSession,
    schema: &Schema,
    rows_per_chunk: usize,
    docs_passthrough: bool,
    fail_open: Arc<std::sync::atomic::AtomicU64>,
    rx: &std::sync::mpsc::Receiver<DocsEncodeMsg>,
    sink: &mut W,
) -> Result<()> {
    let dtype = DType::from_arrow(schema);
    // #51c: the passthrough strategy computes NO vortex file statistics —
    // computing min/max over already-encoded chunks would decode them,
    // defeating the point. Read-side consumers of the footer stats treat
    // their absence as "cannot prune" (fail-open), verified on
    // `blob_column_stats` and its callers.
    let mut options = VortexWriteOptions::new(session);
    if docs_passthrough {
        options = options
            .with_strategy(docs_passthrough_strategy(fail_open))
            .with_file_statistics(Vec::new());
    } else {
        options = options.with_strategy(docs_strategy(rows_per_chunk));
    }
    let mut writer = options.blocking(runtime).writer(&mut *sink, dtype);
    let mut finished = false;
    while let Ok(msg) = rx.recv() {
        match msg {
            DocsEncodeMsg::Batch(batch) => {
                if batch.num_rows() == 0 {
                    continue;
                }
                if docs_passthrough && rows_per_chunk > 0 {
                    // the passthrough strategy has no repartition step (it
                    // would canonicalize the encoded chunks): slice arrow
                    // batches to the locked chunking here instead, so
                    // re-encoded runs keep bounded, budget-shaped chunks
                    let rows = batch.num_rows();
                    let mut offset = 0usize;
                    while offset < rows {
                        let len = rows_per_chunk.min(rows - offset);
                        let part = batch.slice(offset, len);
                        writer.push(ArrayRef::from_arrow(&part, false)?)?;
                        offset += len;
                    }
                } else {
                    writer.push(ArrayRef::from_arrow(&batch, false)?)?;
                }
            }
            DocsEncodeMsg::Encoded(chunk) => {
                if !docs_passthrough {
                    return Err(VixError::Writer(
                        "internal: an encoded docs chunk was pushed into an encoder spawned \
                         without docs_passthrough — refusing to write it (its columns would \
                         bypass the compression pipeline unchecked)"
                            .to_string(),
                    ));
                }
                if chunk.len() == 0 {
                    continue;
                }
                writer.push(chunk)?;
            }
            DocsEncodeMsg::Finish => {
                finished = true;
                break;
            }
        }
    }
    if !finished {
        // the sender was dropped without signal_finish: the writer was
        // abandoned (an error elsewhere unwound it) — stop without paying
        // for the file finalization
        return Err(VixError::Writer(
            "docs encoder aborted: the writer was dropped before finish".to_string(),
        ));
    }
    writer.finish()?;
    Ok(())
}

/// The default compressed write strategy (BtrBlocks pipeline).
/// `row_block_size == 0` keeps vortex's default. Production dict blobs moved
/// to [`dict_strategy`]; this remains as the old-layout writer for tests
/// that prove readers handle pre-lazy-dict files.
#[cfg_attr(not(test), expect(dead_code))]
pub(crate) fn compressed_strategy(row_block_size: usize) -> Arc<dyn LayoutStrategy> {
    let mut builder = WriteStrategyBuilder::default();
    if row_block_size > 0 {
        builder = builder.with_row_block_size(row_block_size);
    }
    builder.build()
}

/// The write strategy of the `dict` blob, shaped for LAZY readers:
///
/// - the three small directory columns (`first_ordinal`, `term_min`, `term_max`) are collected into
///   one compressed chunk each — the reader loads the whole directory at open with one or two small
///   segment fetches,
/// - the `fst` column keeps one chunk **per pushed batch** (the writer pushes one dict row per
///   batch), each compressed individually — a reader point-reads exactly the row-group FSTs a query
///   touches, the same pattern the `terms` blob uses for postings.
///
/// The default pipeline ([`compressed_strategy`], used before lazy dict
/// loading landed) repartitions into 8Ki-ROW blocks with no byte bound, so a
/// typical dict (a handful of multi-MB fst cells) became ONE huge chunk and
/// any read decoded every FST. Readers handle both layouts — point reads on
/// old files just decode the one big chunk.
pub(crate) fn dict_strategy() -> Arc<dyn LayoutStrategy> {
    let compress_then_flat: Arc<dyn LayoutStrategy> = Arc::new(CompressingStrategy::new(
        FlatLayoutStrategy::default(),
        BtrBlocksCompressorBuilder::default().with_compact().build(),
    ));
    let strategy = TableStrategy::new(
        Arc::new(CollectStrategy::new(FlatLayoutStrategy::default())),
        Arc::new(CollectStrategy::new(Arc::clone(&compress_then_flat))),
    )
    .with_field_writer(
        FieldPath::from(DTypeField::Name("fst".into())),
        Arc::new(ChunkedLayoutStrategy::new(compress_then_flat)),
    );
    Arc::new(strategy)
}

/// The write strategy of the `docs` blob: the BtrBlocks pipeline
/// with the *compact* schemes added to the per-column sampler — zstd for
/// string/binary columns, pco for numerics (`BtrBlocksCompressorBuilder::
/// with_compact`, vortex's "zstd" default feature).
///
/// The docs blob is dominated by `_source` (and, when stored, `_original`):
/// ~KB-scale JSON text per row that the default string schemes barely touch
/// (FSST landed at ~1.6x on the benchmark pilot vs ~15x for parquet+zstd).
/// The sampler keeps zstd only where it wins, so low-cardinality
/// column-store fields still take their dictionary/RLE encodings.
///
/// `row_block_size` is the writer-computed rows-per-chunk
/// ([`crate::VixWriterOptions::docs_chunk_bytes`] over the average row's
/// bytes): each chunk is the decompression unit of a matched-row point
/// read, so it is capped by a byte budget instead of following the data
/// file's row-group size (128Ki rows would make every point read decode a
/// multi-hundred-MB chunk). `0` keeps vortex's default.
pub(crate) fn docs_strategy(row_block_size: usize) -> Arc<dyn LayoutStrategy> {
    let mut builder = WriteStrategyBuilder::default()
        .with_btrblocks_builder(BtrBlocksCompressorBuilder::default().with_compact());
    if row_block_size > 0 {
        builder = builder.with_row_block_size(row_block_size);
    }
    builder.build()
}

/// Whether `chunk`'s WHOLE encoding tree is decoded-family — canonical /
/// arrow-sourced / trivially-recompressible nodes only. Such a chunk is
/// fresh data (an arrow batch converted via `from_arrow`, or a canonicalized
/// column) and must be COMPRESSED by [`docs_passthrough_strategy`]; a chunk
/// carrying any compressed encoding (fsst/zstd/pco/dict/fastlanes/...) came
/// off an input file and passes through unchanged.
///
/// Note `is_canonical()` alone is NOT the right test: `from_arrow` produces
/// `vortex.varbin` for strings, which is not canonical (canonical is
/// varbinview) — treating it as passthrough would store the merged
/// `_source` text UNCOMPRESSED (caught by the #51c probe).
#[cfg_attr(not(test), expect(dead_code))]
pub(crate) fn is_decoded_family(chunk: &ArrayRef) -> bool {
    chunk
        .depth_first_traversal()
        .all(|node| is_decoded_node(&node))
}

/// M25: whether the ROOT node alone is decoded-family. A chunk whose root is
/// decoded but whose tree carries an encoded descendant is NOT "off an input
/// file" — it is a freshly DECODED window still borrowing encoded buffers.
/// The shape in the wild: canonicalizing a sliced dict-layout window yields
/// `varbinview(views raw) -> dict(values)` — a raw 16 B/row views buffer
/// over the (tiny) dictionary. Treating that as "encoded, copy verbatim"
/// stored the raw views buffer per column window: a 2,000-column heal merge
/// wrote 15.6x its input bytes (measured: 7,034 MiB docs from 450 MiB of
/// inputs, ~88 KiB/leaf of inline views at entropy 0.5 bits/B). Such chunks
/// must take the COMPRESS branch — the cascade's own canonicalize+compact
/// entry collapses the borrowed buffers first, so the result is exactly what
/// the decode path would store.
pub(crate) fn is_decoded_root(chunk: &ArrayRef) -> bool {
    is_decoded_node(chunk)
}

/// The decoded-family node set shared by [`is_decoded_family`] (whole tree)
/// and [`is_decoded_root`] (root only).
fn is_decoded_node(node: &ArrayRef) -> bool {
    use vortex::array::arrays::{
        Bool, Constant, Decimal, Extension, FixedSizeList, List, ListView, Masked, Null, Primitive,
        Struct, VarBin, VarBinView, Variant,
    };
    node.is::<Null>()
        || node.is::<Bool>()
        || node.is::<Primitive>()
        || node.is::<Decimal>()
        || node.is::<VarBin>()
        || node.is::<VarBinView>()
        || node.is::<List>()
        || node.is::<ListView>()
        || node.is::<FixedSizeList>()
        || node.is::<Struct>()
        || node.is::<Extension>()
        || node.is::<Variant>()
        || node.is::<Masked>()
        || node.is::<Constant>()
}

/// #51c write strategy of the `docs` blob for PASSTHROUGH merges: split the
/// struct into columns and write each column chunk as ONE compressed flat
/// leaf — no repartition, no zoned stats, no dict probe, because every one
/// of those steps CANONICALIZES the stream (verified against vortex-layout
/// 0.79: `RepartitionStrategy` executes each emitted block to `Canonical`),
/// which would decode + recompress the very chunks the passthrough exists
/// to copy.
///
/// The compressor plugin decides per column chunk: decoded-family chunks
/// (arrow batches from re-encoded runs, slice-guard canonicalizations) go
/// through the same BtrBlocks compact pipeline [`docs_strategy`] uses;
/// chunks that already carry a compressed encoding (read straight off an
/// input file) are written as-is. `.with_stats(&[])` skips the
/// pre-compression chunk statistics pass — computing them on encoded chunks
/// would decode them; per-chunk pruning stats are therefore absent (readers
/// fail open) while the o2-level `zone_map` property keeps the `_timestamp`
/// fast paths.
///
/// M18 STRUCTURAL fail-open: before an encoded column chunk is passed
/// through verbatim, its whole tree is checked against the vortex file
/// writer's own allowed-encoding set ([`is_ctx_serializable`]). A chunk
/// carrying any runtime-only node the write context cannot intern
/// (`vortex.slice`, `vortex.shared`, anything future) is canonicalized and
/// recompressed RIGHT HERE — one column chunk, same rows at the same
/// positions, counted in `fail_open` — instead of erroring the whole docs
/// encode and restarting the merge (the prod ".110 vortex.slice not
/// permitted by ctx" shape). The whole-merge restart remains only for
/// errors this guard cannot express (IO, dtype, run bookkeeping).
///
/// M6: the per-field routing is [`crate::clustered::ClusteredDocsStrategy`]
/// instead of `TableStrategy` — physically the file is written in
/// column-major STRIPES and consecutive decoded-family chunks of a column
/// coalesce into coarse chunks, fixing the M5 ranged-read regressions
/// (projected scans fetched the whole interleaved blob; needle selections
/// paid one round trip per chunk). See the `clustered` module docs.
pub(crate) fn docs_passthrough_strategy(
    fail_open: Arc<std::sync::atomic::AtomicU64>,
) -> Arc<dyn LayoutStrategy> {
    use vortex::array::{Canonical, ExecutionCtx, IntoArray};
    let compressor = BtrBlocksCompressorBuilder::default().with_compact().build();
    let compress_or_pass =
        move |chunk: &ArrayRef, ctx: &mut ExecutionCtx| -> vortex::error::VortexResult<ArrayRef> {
            // M25: a decoded-family ROOT takes the compress branch even when
            // encoded descendants ride below (see [`is_decoded_root`]) — the
            // cascade's canonicalize+compact entry collapses the borrowed
            // buffers, exactly what the decode path stores. Only chunks whose
            // ROOT is an encoded form (dict/fsst/zstd/...) came off an input
            // file verbatim and copy as-is.
            if is_decoded_root(chunk) {
                return compressor.compress(chunk, ctx);
            }
            if is_ctx_serializable(chunk) {
                return Ok(chunk.clone());
            }
            // M18 per-chunk fail-open (counts at info in the merge summary,
            // detail at debug — hot path)
            fail_open.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            log::debug!(
                "vix docs passthrough: column chunk ({} rows) carries encoding {:?} the write \
                 context cannot serialize; canonicalizing + re-encoding that chunk (fail-open)",
                chunk.len(),
                first_unserializable_id(chunk)
            );
            let canonical = chunk.clone().execute::<Canonical>(ctx)?.into_array();
            compressor.compress(&canonical, ctx)
        };
    Arc::new(crate::clustered::ClusteredDocsStrategy::new(
        compress_or_pass,
    ))
}

/// M18: whether every node of `array`'s tree carries an encoding the vortex
/// FILE WRITER can serialize — membership in [`vortex::file::ALLOWED_ENCODINGS`],
/// the exact id set the writer pre-seeds its `ArrayContext` with (its
/// registry restriction can only be wider: every allowed id is registered).
/// Runtime-only execution nodes (`vortex.slice`, `vortex.shared`,
/// `vortex.filter`, ...) are outside the set and fail the writer's intern
/// with "Array encoding <id> not permitted by ctx".
pub(crate) fn is_ctx_serializable(array: &ArrayRef) -> bool {
    array
        .depth_first_traversal()
        .all(|node| vortex::file::ALLOWED_ENCODINGS.contains(&node.encoding_id()))
}

/// The first non-serializable encoding id under `array` (diagnostics).
fn first_unserializable_id(array: &ArrayRef) -> Option<vortex::session::registry::Id> {
    array
        .depth_first_traversal()
        .map(|node| node.encoding_id())
        .find(|id| !vortex::file::ALLOWED_ENCODINGS.contains(id))
}

/// Whether any node of `array`'s tree is a `vortex.shared` wrapper.
pub(crate) fn contains_shared(array: &ArrayRef) -> bool {
    use vortex::array::arrays::Shared;
    array
        .depth_first_traversal()
        .any(|node| node.is::<Shared>())
}

/// M12 (prod "vortex.shared not permitted by ctx"): strip the RUNTIME
/// `vortex.shared` wrappers off a stored chunk before the verbatim copy.
///
/// The dict LAYOUT reader (vortex-layout 0.79 `DictReader::values_array`)
/// wraps the values child of every yielded `dict(codes, values)` chunk in a
/// `SharedArray` — a lazy execution cache with NO serialize impl (its vtable
/// bails "Shared array is not serializable", and its id is not in the write
/// registry, hence "not permitted by ctx"). Dict layouts are written by the
/// FIRST-ENCODE strategy ([`docs_strategy`] → vortex's default
/// `WriteStrategyBuilder`, which dict-probes columns); the passthrough
/// strategy below never writes them. Multi-chunk dict fields escape the
/// error today only because their shared values buffers trip the slice
/// guard's overlap sweep into canonicalizing; a SINGLE-chunk dict field has
/// no adjacent chunk to overlap and reached the writer with the wrapper
/// intact — every prod heal-passthrough failure was this shape.
///
/// The wrapper is transparent (`Shared::validate` pins the source's dtype +
/// len), so replacing each `Shared` node with its SOURCE array — the stored
/// encoded values, never the canonical cache — preserves the stored form
/// exactly. `None` = a `Shared` sits under a parent this rewrite cannot
/// rebuild (only dict parents are known to produce it); the caller then
/// canonicalizes that field — fail-open to the recompress path, never a
/// failed merge.
pub(crate) fn unwrap_shared(array: &ArrayRef) -> Option<ArrayRef> {
    use vortex::array::{
        IntoArray,
        arrays::{
            Dict, DictArray, Shared,
            dict::{DictArrayExt, DictArraySlotsExt},
            shared::SharedArrayExt,
        },
    };
    if !contains_shared(array) {
        return Some(array.clone());
    }
    if let Some(shared) = array.as_typed::<Shared>() {
        return unwrap_shared(shared.source());
    }
    if let Some(dict) = array.as_typed::<Dict>() {
        let all_referenced = dict.has_all_values_referenced();
        let codes = unwrap_shared(dict.codes())?;
        let values = unwrap_shared(dict.values())?;
        // SAFETY: this rebuilds an ALREADY-VALIDATED dict with the same
        // logical children — `Shared::validate` guarantees the unwrapped
        // source has the wrapper's dtype and length, so the code-bounds and
        // nullability invariants `try_new` would re-check carry over
        // verbatim; `all_values_referenced` is copied from the array that
        // asserted it at encode time.
        let rebuilt = unsafe {
            DictArray::new_unchecked(codes, values).set_all_values_referenced(all_referenced)
        };
        return Some(rebuilt.into_array());
    }
    None
}

/// M18: per-column STORED-LEAF row boundaries of a docs blob, from the
/// layout tree (footer metadata only — no data reads). Element `i` is the
/// sorted, deduped boundary set of top-level field `i` in dtype order,
/// always containing `0` and the file row count.
///
/// The walk is layout-kind-agnostic — it relies only on the
/// [`vortex::layout::LayoutChildType`] contract: `Chunk` children carry
/// their relative row offset (chunked layouts), `Transparent` children keep
/// their parent's rows (zoned data child, dict codes), `Auxiliary` children
/// carry no row semantics (zone maps, dict values) and are skipped. A node
/// with no row-bearing children is a leaf: one stored chunk spanning its
/// row count. Any shape outside this contract (a nested struct column, two
/// transparent children, a child read error) returns `None` — the caller
/// FAILS CLOSED by treating every window of every column as sliced
/// (canonicalize + recompress: always correct, never a wrong copy).
fn column_leaf_boundaries(vxf: &VortexFile) -> Option<Vec<Vec<u64>>> {
    use vortex::layout::LayoutChildType;

    fn collect(layout: &vortex::layout::LayoutRef, base: u64, out: &mut Vec<u64>) -> bool {
        let types: Vec<LayoutChildType> = layout.child_types().collect();
        let mut row_children: Vec<(u64, vortex::layout::LayoutRef)> = Vec::new();
        let mut transparent = 0usize;
        for (index, ty) in types.iter().enumerate() {
            let child_base = match ty {
                LayoutChildType::Chunk((_, offset)) => base + offset,
                LayoutChildType::Transparent(_) => {
                    transparent += 1;
                    base
                }
                LayoutChildType::Auxiliary(_) => continue,
                // a nested struct column is not a docs-blob shape
                LayoutChildType::Field(_) => return false,
            };
            let Ok(child) = layout.child(index) else {
                return false;
            };
            row_children.push((child_base, child));
        }
        // exactly one transparent child may stand for the parent's rows;
        // mixing it with chunk children (or doubling it) is ambiguous
        if transparent > 1 || (transparent == 1 && row_children.len() > 1) {
            return false;
        }
        if row_children.is_empty() {
            out.push(base);
            out.push(base + layout.row_count());
            return true;
        }
        row_children
            .into_iter()
            .all(|(child_base, child)| collect(&child, child_base, out))
    }

    let root = vxf.footer().layout().clone();
    let types: Vec<LayoutChildType> = root.child_types().collect();
    let mut out: Vec<Vec<u64>> = Vec::with_capacity(types.len());
    for (index, ty) in types.iter().enumerate() {
        if !matches!(ty, LayoutChildType::Field(_)) {
            return None;
        }
        let child = root.child(index).ok()?;
        let mut bounds = vec![0u64];
        if !collect(&child, 0, &mut bounds) {
            return None;
        }
        bounds.sort_unstable();
        bounds.dedup();
        out.push(bounds);
    }
    Some(out)
}

/// #51c encoded-chunk scan of a docs blob: iterate the file's chunks with NO
/// projection, filter, selection or limit — nothing canonicalizes — and hand
/// each struct chunk to `on_chunk` with its row count, in row order.
/// Returns the number of column-windows the slice guard canonicalized.
///
/// SLICE GUARD (correctness, not tuning — M18 rewrite): the scan splits at
/// the union of every column's chunk boundaries (plus vortex's artificial
/// ~100K-row subdivisions of wide spans), so a column whose stored chunks
/// are coarser than the split grid arrives as SLICES of one stored chunk.
/// A sliced form does NOT survive a serialize round-trip:
///
/// - encodings WITHOUT a metadata slice rule keep a runtime `vortex.slice` wrapper (`vortex.runend`
///   / `vortex.fastlanes.rle` register only execute-time slice kernels), which the file writer's
///   context cannot intern — "Array encoding vortex.slice not permitted by ctx", the prod .110
///   failure;
/// - encodings WITH a metadata rule reduce to offset-bearing forms whose serialize silently DROPS
///   the offset — the M18 probe measured a sliced `vortex.zigzag` column re-reading 96% wrong rows
///   after a verbatim copy (the M5-era pco probe was the same class).
///
/// Detection is therefore DETERMINISTIC, not heuristic: a field's window is
/// copied verbatim only when both window edges lie on that column's OWN
/// stored-leaf boundaries ([`column_leaf_boundaries`]); every other window
/// is canonicalized here and recompressed by the writer's passthrough
/// compressor — the exact work the decode path does for every column. The
/// previous buffer-overlap sweep is gone: it keyed on pointer identity of
/// shared buffers, which per-window re-decodes (fresh segment fetches,
/// alignment copies) silently break — the M18 probes showed it blind on
/// both mem and ranged sources.
///
/// M12's `unwrap_shared` still runs on ALIGNED chunks (the dict layout
/// reader Shared-wraps values on unsliced chunks too), and a final
/// [`is_ctx_serializable`] check backstops both guards: any remaining
/// non-writable node canonicalizes its column instead of reaching the
/// writer.
pub(crate) fn scan_blob_encoded_chunks(
    blob: &BlobHandle,
    on_chunk: &mut dyn FnMut(ArrayRef, usize) -> Result<()>,
) -> Result<u64> {
    use vortex::array::{
        Canonical, IntoArray,
        arrays::{Struct, StructArray, struct_::StructArrayExt},
        validity::Validity,
    };

    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let vxf = open_blob(&runtime, &session, blob)?;
    let boundaries = column_leaf_boundaries(&vxf);
    if boundaries.is_none() {
        log::debug!(
            "vix docs chunk scan: layout tree outside the known docs shapes — slice guard \
             fails closed (every column window canonicalizes + recompresses)"
        );
    }
    let scan = vxf.scan()?;

    let mut offset: u64 = 0;
    let mut canonicalized: u64 = 0;
    for array in scan.into_array_iter(&runtime)? {
        let array = array?;
        if array.len() == 0 {
            continue;
        }
        let sa = array
            .as_typed::<Struct>()
            .ok_or_else(|| {
                VixError::Malformed("docs scan did not produce struct chunks".to_string())
            })?
            .clone();
        let len = array.len();
        let (window_start, window_end) = (offset, offset + len as u64);
        offset = window_end;
        let names = sa.names().clone();
        let mut fields: Vec<ArrayRef> = sa.unmasked_fields().to_vec();
        if let Some(bounds) = &boundaries
            && bounds.len() != fields.len()
        {
            return Err(VixError::Malformed(format!(
                "docs scan chunks carry {} fields but the layout tree has {} columns",
                fields.len(),
                bounds.len()
            )));
        }
        let mut marks = vec![false; fields.len()];
        let mut rewrote = false;
        for (index, field) in fields.iter_mut().enumerate() {
            // (1) deterministic slice guard: verbatim only on a whole
            // stored leaf of THIS column
            let aligned = boundaries.as_ref().is_some_and(|bounds| {
                bounds[index].binary_search(&window_start).is_ok()
                    && bounds[index].binary_search(&window_end).is_ok()
            });
            if !aligned {
                marks[index] = true;
                canonicalized += 1;
                continue;
            }
            // (2) M12 serializability guard (see `unwrap_shared`): strip the
            // dict layout reader's runtime `vortex.shared` wrappers so the
            // stored form survives the verbatim re-serialize; a Shared node
            // the rewrite cannot reach canonicalizes the field instead.
            if contains_shared(field) {
                match unwrap_shared(field) {
                    Some(clean) => {
                        *field = clean;
                        rewrote = true;
                    }
                    None => {
                        log::debug!(
                            "vix docs chunk scan: column {:?} carries a non-serializable \
                             vortex.shared node under an unknown parent encoding; \
                             canonicalizing that column (recompress) instead of copying",
                            names[index]
                        );
                        marks[index] = true;
                        canonicalized += 1;
                        continue;
                    }
                }
            }
            // (3) backstop: any other node the file writer cannot intern
            if !is_ctx_serializable(field) {
                log::debug!(
                    "vix docs chunk scan: column {:?} carries encoding {:?} the write context \
                     cannot serialize; canonicalizing that column (recompress)",
                    names[index],
                    first_unserializable_id(field)
                );
                marks[index] = true;
                canonicalized += 1;
            }
        }
        if !rewrote && !marks.iter().any(|&m| m) {
            on_chunk(array, len)?;
            continue;
        }
        let resolved: std::result::Result<Vec<ArrayRef>, VixError> = fields
            .into_iter()
            .enumerate()
            .map(|(index, field)| {
                if marks[index] {
                    let mut ctx = session.create_execution_ctx();
                    Ok(field
                        .execute::<Canonical>(&mut ctx)
                        .map_err(|e| {
                            VixError::Malformed(format!(
                                "canonicalize sliced docs column {:?}: {e}",
                                names[index]
                            ))
                        })?
                        .into_array())
                } else {
                    Ok(field)
                }
            })
            .collect();
        let rebuilt = StructArray::try_new(names, resolved?, len, Validity::NonNullable)
            .map_err(|e| VixError::Malformed(format!("rebuild docs chunk: {e}")))?
            .into_array();
        on_chunk(rebuilt, len)?;
    }
    Ok(canonicalized)
}

/// The write strategy for the `terms` blob: split the struct into columns and
/// keep every pushed chunk as its own flat, uncompressed leaf.
///
/// The default vortex pipeline repartitions and coalesces chunks toward
/// ~1 MiB segments, which would defeat the point-read budget for postings.
/// Here the *writer* sizes the pushed row blocks (cumulative postings bytes
/// ≈ `postings_chunk_bytes`) and this strategy never merges or re-splits
/// them, so fetching the postings of one ordinal decodes one bounded chunk.
/// Postings are already delta+bitpacked, so skipping BtrBlocks compression
/// costs little; `doc_count` chunks mirror the postings row blocks.
pub(crate) fn addressable_strategy() -> Arc<dyn LayoutStrategy> {
    Arc::new(TableStrategy::new(
        Arc::new(CollectStrategy::new(FlatLayoutStrategy::default())),
        Arc::new(ChunkedLayoutStrategy::new(FlatLayoutStrategy::default())),
    ))
}

/// Write `batches` (all matching `schema`; empty ones are skipped) as an
/// in-memory Vortex file, one pushed chunk per batch.
///
/// `encode_threads > 1` runs chunk encoding/compression on the process-wide
/// VIX CPU executor (Vortex's layout writers spawn one CPU task per chunk
/// onto the session handle); `0`/`1` keeps everything on the calling thread.
/// The caller thread only pumps chunks and collects buffers either way, so
/// the produced bytes are identical.
pub(crate) fn write_vortex_blob(
    schema: &Schema,
    batches: &[RecordBatch],
    strategy: Arc<dyn LayoutStrategy>,
    encode_threads: usize,
) -> Result<Vec<u8>> {
    let runtime = SingleThreadRuntime::default();
    if encode_threads > 1 {
        let session = VortexSession::default()
            .with_handle(crate::cpu_executor::shared_vortex_execution_handle()?);
        write_vortex_blob_inner(&runtime, session, schema, batches, strategy)
    } else {
        let session = VortexSession::default().with_handle(runtime.handle());
        write_vortex_blob_inner(&runtime, session, schema, batches, strategy)
    }
}

/// Incremental writer of the `terms` blob into an UNLINKED spool file:
/// byte-identical to `write_vortex_blob(schema, batches,
/// addressable_strategy(), _)` over the same pushed batch sequence (same
/// pushes, same strategy, and the addressable strategy's flat uncompressed
/// leaves make the bytes thread-count independent), but batches stream
/// through a worker thread instead of accumulating until the k-way term
/// merge finishes — the producer never holds more than the bounded channel.
/// Mirrors [`DocsBlobEncoder`]'s worker/channel shape.
pub(crate) struct TermsBlobSpooler {
    tx: Option<std::sync::mpsc::SyncSender<RecordBatch>>,
    handle: Option<std::thread::JoinHandle<Result<BlobPart>>>,
    abort: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl TermsBlobSpooler {
    pub(crate) fn spawn(spool_dir: &std::path::Path, schema: SchemaRef) -> Result<Self> {
        std::fs::create_dir_all(spool_dir)
            .map_err(|e| VixError::Writer(format!("create terms spool dir {spool_dir:?}: {e}")))?;
        // unlinked temp file: freed by the OS on drop/crash, nothing to sweep
        let mut file = tempfile::tempfile_in(spool_dir)
            .map_err(|e| VixError::Writer(format!("create terms spool in {spool_dir:?}: {e}")))?;
        let sink = file
            .try_clone()
            .map_err(|e| VixError::Writer(format!("clone terms spool handle: {e}")))?;
        let (tx, rx) = std::sync::mpsc::sync_channel::<RecordBatch>(2);
        let abort = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let worker_abort = std::sync::Arc::clone(&abort);
        let handle = std::thread::Builder::new()
            .name("vix-terms-spool".to_string())
            .spawn(move || -> Result<BlobPart> {
                let runtime = SingleThreadRuntime::default();
                let session = VortexSession::default().with_handle(runtime.handle());
                let dtype = DType::from_arrow(schema.as_ref());
                let mut writer = VortexWriteOptions::new(session)
                    .with_strategy(addressable_strategy())
                    .blocking(&runtime)
                    .writer(std::io::BufWriter::with_capacity(1024 * 1024, sink), dtype);
                while let Ok(batch) = rx.recv() {
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    writer.push(ArrayRef::from_arrow(&batch, false)?)?;
                }
                if worker_abort.load(std::sync::atomic::Ordering::Acquire) {
                    return Err(VixError::Writer("terms spool aborted".to_string()));
                }
                // Normal close: every batch is in, so finalize the blob.
                // Abort returns above and drops the unlinked spool without
                // paying the final encode/footer work.
                writer.finish()?;
                use std::io::Seek;
                let len = file
                    .metadata()
                    .map_err(|e| VixError::Writer(format!("stat terms spool: {e}")))?
                    .len();
                file.seek(std::io::SeekFrom::Start(0))
                    .map_err(|e| VixError::Writer(format!("rewind terms spool: {e}")))?;
                Ok(BlobPart::Spooled { file, len })
            })
            .map_err(|e| VixError::Writer(format!("spawn terms spool thread: {e}")))?;
        Ok(Self {
            tx: Some(tx),
            handle: Some(handle),
            abort,
        })
    }

    /// Queue one closed term batch (blocks when the channel is full — the
    /// spool write backpressures the k-way merge).
    pub(crate) fn push(&mut self, batch: RecordBatch) -> Result<()> {
        let tx = self
            .tx
            .as_ref()
            .ok_or_else(|| VixError::Writer("terms spooler already finished".to_string()))?;
        if tx.send(batch).is_err() {
            return Err(self.take_failure());
        }
        Ok(())
    }

    /// Close the input and collect the finished blob (cursor at byte 0).
    pub(crate) fn finish(mut self) -> Result<BlobPart> {
        drop(self.tx.take());
        match self.handle.take() {
            Some(handle) => handle
                .join()
                .map_err(|_| VixError::Writer("terms spool thread panicked".to_string()))?,
            None => Err(VixError::Writer(
                "terms spool result already taken".to_string(),
            )),
        }
    }

    /// The worker died before accepting a batch: surface its real error.
    fn take_failure(&mut self) -> VixError {
        self.tx = None;
        match self.handle.take() {
            Some(handle) => match handle.join() {
                Ok(Ok(_)) => {
                    VixError::Writer("terms spooler stopped early without an error".to_string())
                }
                Ok(Err(e)) => e,
                Err(_) => VixError::Writer("terms spool thread panicked".to_string()),
            },
            None => VixError::Writer("terms spooler already failed".to_string()),
        }
    }
}

impl Drop for TermsBlobSpooler {
    fn drop(&mut self) {
        self.abort.store(true, std::sync::atomic::Ordering::Release);
        drop(self.tx.take());
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn write_vortex_blob_inner(
    runtime: &SingleThreadRuntime,
    session: VortexSession,
    schema: &Schema,
    batches: &[RecordBatch],
    strategy: Arc<dyn LayoutStrategy>,
) -> Result<Vec<u8>> {
    let dtype = DType::from_arrow(schema);
    let mut buf = Vec::new();
    let mut writer = VortexWriteOptions::new(session)
        .with_strategy(strategy)
        .blocking(runtime)
        .writer(&mut buf, dtype);
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        writer.push(ArrayRef::from_arrow(batch, false)?)?;
    }
    writer.finish()?;
    Ok(buf)
}

/// Open one embedded Vortex file for scanning: in-memory blobs via
/// `open_buffer`, ranged blobs via `open_read` over the blob window (the
/// blob's Vortex footer is cached on the handle, so only the first open of a
/// ranged blob fetches footer bytes — later opens perform zero IO until the
/// scan requests data segments).
pub(crate) fn open_blob(
    runtime: &SingleThreadRuntime,
    session: &VortexSession,
    blob: &BlobHandle,
) -> Result<VortexFile> {
    match blob {
        BlobHandle::Mem(bytes) => Ok(session.open_options().open_buffer(bytes.clone())?),
        BlobHandle::Ranged(ranged) => {
            let mut options = session.open_options().with_file_size(ranged.len());
            let had_footer = match ranged.footer() {
                Some(footer) => {
                    options = options.with_footer(footer);
                    true
                }
                None => false,
            };
            let vxf = runtime.block_on(options.open_read(ranged.read_at()))?;
            if !had_footer {
                ranged.set_footer(vxf.footer().clone());
            }
            Ok(vxf)
        }
    }
}

/// Read the arrow schema of an embedded Vortex file without scanning any
/// rows (footer/dtype only).
pub(crate) fn blob_arrow_schema(blob: &BlobHandle) -> Result<Schema> {
    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let vxf = open_blob(&runtime, &session, blob)?;
    Ok(vxf.dtype().to_arrow_schema()?)
}

/// Row selection for [`scan_blob`].
pub(crate) enum RowSelection {
    /// Every row.
    All,
    /// Point reads of the given row indices (sorted + deduped internally).
    Indices(Vec<u64>),
    /// A Vortex-native compressed row selection.
    Vortex(Selection),
    /// A Vortex-native selection restricted to one absolute row range.
    VortexRange(Selection, std::ops::Range<u64>),
    /// One contiguous half-open row range — unlike [`RowSelection::Indices`]
    /// this allocates nothing per row (vortex takes the range directly), so
    /// it is the right shape for scanning a column over millions of
    /// consecutive rows (#29's doc_count scans).
    Range(std::ops::Range<u64>),
}

/// Scan an embedded Vortex file into arrow record batches, optionally
/// projecting columns and restricting rows.
pub(crate) fn scan_blob(
    blob: &BlobHandle,
    projection: Option<&[&str]>,
    selection: RowSelection,
) -> Result<Vec<RecordBatch>> {
    let mut batches = Vec::new();
    scan_blob_streaming(blob, projection, selection, None, None, 0, &mut |batch| {
        batches.push(batch);
        Ok(())
    })?;
    Ok(batches)
}

/// Streaming variant of [`scan_blob`]: decoded chunks are handed to
/// `on_batch` one at a time (memory stays bounded by one chunk), and an
/// optional vortex filter `Expression` is pushed into the scan (zone-map
/// pruned). Row selection and the filter compose: a row is produced only
/// when it is selected *and* passes the filter.
pub(crate) fn scan_blob_streaming(
    blob: &BlobHandle,
    projection: Option<&[&str]>,
    selection: RowSelection,
    filter: Option<vortex::expr::Expression>,
    limit: Option<u64>,
    decode_threads: usize,
    on_batch: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<()> {
    // decode_threads > 1: give vortex a multi-thread handle so one big
    // file's chunks decode in parallel (we parallelize across files;
    // a single 4GB merged file used to decode on one core). Mirrors the
    // write path's pool. 0/1 keeps the single-thread shape.
    let pool = if decode_threads > 1 {
        Some(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(decode_threads)
                .thread_name("vix-decode")
                .build()
                .map_err(|e| VixError::Malformed(format!("decode thread pool: {e}")))?,
        )
    } else {
        None
    };
    let runtime = SingleThreadRuntime::default();
    // the TokioRuntime wrapper must OUTLIVE the session (its handle is
    // used for every decode task); binding it in a match arm drops it
    // early and every use panics "Handle after its runtime was dropped"
    let pool_runtime = pool
        .as_ref()
        .map(|pool| TokioRuntime::new(pool.handle().clone()));
    let session = match &pool_runtime {
        Some(rt) => VortexSession::default().with_handle(rt.handle()),
        None => VortexSession::default().with_handle(runtime.handle()),
    };
    let vxf = open_blob(&runtime, &session, blob)?;
    let mut scan = vxf.scan()?;
    if let Some(columns) = projection {
        scan = scan.with_projection(select(columns.to_vec(), root()));
    }
    match selection {
        RowSelection::All => {}
        RowSelection::Indices(mut indices) => {
            indices.sort_unstable();
            indices.dedup();
            scan = scan.with_row_indices(Buffer::from(indices));
        }
        RowSelection::Range(range) => {
            scan = scan.with_row_range(range);
        }
        RowSelection::Vortex(selection) => {
            scan = scan.with_selection(selection);
        }
        RowSelection::VortexRange(selection, range) => {
            scan = scan.with_row_range(range).with_selection(selection);
        }
    }
    scan = scan.with_some_filter(filter);
    scan = scan.with_some_limit(limit);
    let arrow_schema = scan.dtype()?.to_arrow_schema()?;
    let data_type = DataType::Struct(arrow_schema.fields().clone());
    for array in scan.into_array_iter(&runtime)? {
        on_batch(vortex_to_record_batch(&session, array?, &data_type)?)?;
    }
    if let Some(pool) = pool {
        pool.shutdown_background();
    }
    Ok(())
}

/// File-level `(min, max)` of one numeric column, straight from the vortex
/// footer's [`FileStatistics`] — no data reads (ranged blobs fetch/reuse
/// the cached footer only). `Ok(None)` when the file carries no statistics
/// for the column (older writer, non-numeric dtype, name absent): callers
/// must treat that as "cannot prune".
pub(crate) fn blob_column_stats(
    blob: &BlobHandle,
    column: &str,
) -> Result<Option<(crate::docs::NumScalar, crate::docs::NumScalar)>> {
    use vortex::{
        dtype::DType,
        expr::stats::{Precision, Stat},
    };

    use crate::docs::NumScalar;
    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let vxf = open_blob(&runtime, &session, blob)?;
    let footer = vxf.footer();
    let Some(stats) = footer.statistics() else {
        return Ok(None);
    };
    let DType::Struct(sdt, _) = footer.dtype() else {
        return Ok(None);
    };
    let Some(index) = sdt.names().iter().position(|name| name.as_ref() == column) else {
        return Ok(None);
    };
    if index >= stats.stats_sets().len() {
        return Ok(None);
    }
    let (set, dtype) = stats.get(index);
    // only EXACT stats prune (an inexact bound must never skip a file)
    let numeric = |stat: Stat| -> Option<NumScalar> {
        match dtype {
            DType::Primitive(ptype, _) if ptype.is_int() => match set.get_as::<i64>(stat, dtype) {
                Precision::Exact(v) => Some(NumScalar::I64(v)),
                _ => None,
            },
            DType::Primitive(..) => match set.get_as::<f64>(stat, dtype) {
                Precision::Exact(v) => Some(NumScalar::F64(v)),
                _ => None,
            },
            _ => None,
        }
    };
    match (numeric(Stat::Min), numeric(Stat::Max)) {
        (Some(min), Some(max)) => Ok(Some((min, max))),
        _ => Ok(None),
    }
}

/// One `_timestamp` zone entry: `(row_count, ts_min, ts_max)` of one docs
/// row-block. Row offsets are the running prefix sum (derived by the reader).
/// Public since #51c: the compaction merge splices input files' entries into
/// the output writer verbatim ([`crate::VixWriter::begin_docs_encoded_run`]).
pub type ZoneEntry = (u64, i64, i64);

/// Reader-side sanity cap on `row_regions` entries: a table larger than
/// this is treated as absent (fail-open to the full-sort path) — it would
/// be useless for a k-way merge anyway and bounds the parse allocation.
pub(crate) const ROW_REGIONS_READER_CAP: usize = 65_536;

/// §4: parse the [`PROP_ROW_REGIONS`] property into per-region row counts.
/// `None` when the property is absent, malformed, empty, carries a zero
/// count, exceeds [`ROW_REGIONS_READER_CAP`], or does not sum to
/// `row_count` exactly — the same trust rules as the zone table (a debug
/// log, then fail-open: the caller reads the file as piecewise-unknown).
pub(crate) fn parse_row_regions(raw: Option<&str>, row_count: u64) -> Option<Vec<u64>> {
    let raw = raw?;
    let regions: Vec<u64> = match serde_json::from_str(raw) {
        Ok(regions) => regions,
        Err(e) => {
            log::debug!("vix: ignoring malformed row_regions property ({e})");
            return None;
        }
    };
    if regions.is_empty() || regions.len() > ROW_REGIONS_READER_CAP {
        log::debug!(
            "vix: ignoring row_regions with {} entries (cap {ROW_REGIONS_READER_CAP})",
            regions.len()
        );
        return None;
    }
    let mut covered = 0u64;
    for &rows in &regions {
        if rows == 0 {
            log::debug!("vix: ignoring row_regions with a zero-row region");
            return None;
        }
        covered = covered.checked_add(rows)?;
    }
    if covered != row_count {
        log::debug!(
            "vix: row_regions covers {covered} rows but the file has {row_count}; ignoring it"
        );
        return None;
    }
    Some(regions)
}

/// The half-open row ranges of a region decomposition (prefix sums of the
/// per-region row counts).
pub fn region_row_ranges(regions: &[u64]) -> Vec<std::ops::Range<u64>> {
    let mut ranges = Vec::with_capacity(regions.len());
    let mut offset = 0u64;
    for &rows in regions {
        ranges.push(offset..offset + rows);
        offset += rows;
    }
    ranges
}

/// One chunk of a stored column in dictionary form, as arrow arrays:
/// `codes[i]` indexes into `values` (a null code = a null row). Dictionaries
/// are per chunk — consecutive chunks may carry different value sets, and a
/// value is not guaranteed to be referenced by any code.
pub(crate) struct DictColumnChunk {
    pub codes: arrow::array::UInt64Array,
    pub values: ArrowArrayRef,
}

/// Shared row-id allocation budget for the equality pre-pass. Workers may
/// collectively retain at most `limit` matches; the first additional match
/// marks the pass broad and makes every worker stop at its next check.
pub(crate) struct EqMatchBudget {
    limit: u64,
    claimed: std::sync::atomic::AtomicU64,
    exceeded: std::sync::atomic::AtomicBool,
}

impl EqMatchBudget {
    pub(crate) fn new(limit: u64) -> Self {
        Self {
            limit,
            claimed: std::sync::atomic::AtomicU64::new(0),
            exceeded: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub(crate) fn is_exceeded(&self) -> bool {
        self.exceeded.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn try_push(&self, row: u64, out: &mut Vec<u64>) -> bool {
        if self.is_exceeded() {
            return false;
        }
        let slot = self
            .claimed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if slot >= self.limit {
            self.exceeded
                .store(true, std::sync::atomic::Ordering::Relaxed);
            return false;
        }
        out.push(row);
        true
    }
}

/// Row ids (ascending) whose stored `name` value equals `needle`, over the
/// given ascending disjoint row `ranges`. Returns `None` as soon as the
/// shared match budget proves the point-read follow-up is too broad.
///
/// Per chunk, dictionary-aware: a chunk stored DICT-encoded (the norm for
/// the columns vortex's sampler dict-probes) resolves the needle against
/// its small distinct-values array once and then scans the u64 code array
/// for the matched code ids — no per-row string materialization or compare.
/// Non-dict chunks (high-entropy FSST/plain, or any conversion failure)
/// take the existing shape per chunk: canonical decode + per-row compare.
/// Null rows never match (equality is null-rejecting).
///
/// Runs single-threaded over its ranges. The caller splits ranges across
/// OS threads; all workers share `budget`, bounding aggregate row-id memory.
pub(crate) fn eq_string_rows_ranges(
    blob: &BlobHandle,
    name: &str,
    needle: &str,
    ranges: &[std::ops::Range<u64>],
    budget: &EqMatchBudget,
) -> Result<Option<Vec<u64>>> {
    use arrow::array::{StringArray, UInt64Array};
    use vortex::array::arrays::{
        Dict, Struct, dict::DictArraySlotsExt, shared::SharedArrayExt, struct_::StructArrayExt,
    };

    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let vxf = open_blob(&runtime, &session, blob)?;
    let mut out: Vec<u64> = Vec::new();
    // canonical string decode of one chunk (shared by the dict values read
    // and the non-dict fallback)
    let to_utf8 = |field: &ArrayRef, session: &VortexSession| -> Result<StringArray> {
        let target = Field::new("", DataType::Utf8, field.dtype().is_nullable());
        let mut ctx = session.create_execution_ctx();
        let arrow_array = session
            .arrow()
            .execute_arrow(field.clone(), Some(&target), &mut ctx)?;
        arrow_array
            .as_any()
            .downcast_ref::<StringArray>()
            .cloned()
            .ok_or_else(|| VixError::Malformed(format!("column {name:?} did not convert to Utf8")))
    };
    for range in ranges {
        if budget.is_exceeded() {
            return Ok(None);
        }
        if range.start >= range.end {
            continue;
        }
        let scan = vxf
            .scan()?
            .with_projection(select(vec![name], root()))
            .with_row_range(range.clone());
        let mut row = range.start;
        for array in scan.into_array_iter(&runtime)? {
            if budget.is_exceeded() {
                return Ok(None);
            }
            let array = array?;
            let field = array
                .as_typed::<Struct>()
                .ok_or_else(|| {
                    VixError::Malformed("projected scan did not produce a struct array".to_string())
                })?
                .unmasked_field_by_name(name)
                .map_err(|e| VixError::Malformed(format!("column {name:?}: {e}")))?
                .clone();
            let rows = field.len();
            // unwrap execution-cache wrappers so a stored dict is visible
            let stored = match field.as_typed::<vortex::array::arrays::Shared>() {
                Some(shared) => shared.source().clone(),
                None => field.clone(),
            };
            let mut handled = false;
            if let Some(dict) = stored.as_typed::<Dict>() {
                // dict-encoded on disk: resolve the needle against the
                // (small) values array once, then scan the codes
                let values = to_utf8(dict.values(), &session);
                let codes = {
                    let codes_target = Field::new("", DataType::UInt64, true);
                    let mut ctx = session.create_execution_ctx();
                    session
                        .arrow()
                        .execute_arrow(dict.codes().clone(), Some(&codes_target), &mut ctx)
                        .ok()
                        .and_then(|a| a.as_any().downcast_ref::<UInt64Array>().cloned())
                };
                if let (Ok(values), Some(codes)) = (values, codes)
                    && codes.len() == rows
                {
                    let matched: Vec<u64> = (0..values.len())
                        .filter(|&i| !values.is_null(i) && values.value(i) == needle)
                        .map(|i| i as u64)
                        .collect();
                    if !matched.is_empty() {
                        // the common case is one matched code id
                        let single = (matched.len() == 1).then(|| matched[0]);
                        for i in 0..codes.len() {
                            if codes.is_null(i) {
                                continue;
                            }
                            let code = codes.value(i);
                            let hit = match single {
                                Some(m) => code == m,
                                None => matched.contains(&code),
                            };
                            if hit && !budget.try_push(row + i as u64, &mut out) {
                                return Ok(None);
                            }
                        }
                    }
                    handled = true;
                }
            }
            if !handled {
                // non-dict encoding (or conversion failure): the existing
                // scan shape, per chunk — canonical decode + per-row compare
                let values = to_utf8(&field, &session)?;
                for i in 0..values.len() {
                    if !values.is_null(i)
                        && values.value(i) == needle
                        && !budget.try_push(row + i as u64, &mut out)
                    {
                        return Ok(None);
                    }
                }
            }
            row += rows as u64;
        }
        if row != range.end {
            return Err(VixError::Malformed(format!(
                "eq scan of {name:?} covered rows {}..{row}, expected ..{}",
                range.start, range.end
            )));
        }
    }
    Ok(Some(out))
}
/// Stream exact `(timestamp, row_id)` candidates for one string equality
/// over an ascending row range. The equality and optional timestamp clamp
/// execute inside Vortex; dictionary chunks compare the literal against
/// their distinct values once and reuse the codes. The projection contains
/// only `_timestamp` and Vortex's absolute row-index expression, so callers
/// can stop after enough ordered candidates without touching `_source`.
///
/// Returning `false` from `on_candidate` stops the scan cleanly. This is the
/// filtered-scan equivalent of a LIMIT (Vortex 0.79 rejects a ScanBuilder
/// carrying both a filter and a limit).
pub(crate) fn scan_eq_string_candidates_range(
    blob: &BlobHandle,
    name: &str,
    needle: &str,
    range: std::ops::Range<u64>,
    ts_range: Option<(i64, i64)>,
    on_candidate: &mut dyn FnMut(i64, u32) -> bool,
) -> Result<()> {
    use arrow::array::{Int64Array, UInt64Array};
    use vortex::{
        dtype::Nullability,
        expr::{and, col, eq, gt_eq, lit, lt, pack},
        layout::layouts::row_idx::row_idx,
    };

    const ROW_ID_COL: &str = "__vix_row_id";

    if range.start >= range.end {
        return Ok(());
    }
    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let vxf = open_blob(&runtime, &session, blob)?;
    let mut filter = eq(col(name), lit(needle.to_string()));
    if let Some((start, end)) = ts_range {
        let time_filter = and(
            gt_eq(col(crate::writer::TIMESTAMP_COL_NAME), lit(start)),
            lt(col(crate::writer::TIMESTAMP_COL_NAME), lit(end)),
        );
        filter = and(filter, time_filter);
    }
    let projection = pack(
        [
            (
                crate::writer::TIMESTAMP_COL_NAME,
                col(crate::writer::TIMESTAMP_COL_NAME),
            ),
            (ROW_ID_COL, row_idx()),
        ],
        Nullability::NonNullable,
    );
    let scan = vxf
        .scan()?
        .with_projection(projection)
        .with_filter(filter)
        .with_row_range(range);
    let arrow_schema = scan.dtype()?.to_arrow_schema()?;
    let data_type = DataType::Struct(arrow_schema.fields().clone());
    for array in scan.into_array_iter(&runtime)? {
        let batch = vortex_to_record_batch(&session, array?, &data_type)?;
        let timestamps = batch
            .column_by_name(crate::writer::TIMESTAMP_COL_NAME)
            .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| {
                VixError::Malformed(format!(
                    "filtered equality scan did not produce {:?} as i64",
                    crate::writer::TIMESTAMP_COL_NAME
                ))
            })?;
        let row_ids = batch
            .column_by_name(ROW_ID_COL)
            .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| {
                VixError::Malformed(
                    "filtered equality scan did not produce absolute u64 row ids".to_string(),
                )
            })?;
        if timestamps.len() != row_ids.len()
            || timestamps.null_count() > 0
            || row_ids.null_count() > 0
        {
            return Err(VixError::Malformed(format!(
                "filtered equality scan returned invalid candidate columns: timestamps={} \
                 ({} nulls), row_ids={} ({} nulls)",
                timestamps.len(),
                timestamps.null_count(),
                row_ids.len(),
                row_ids.null_count(),
            )));
        }
        for i in 0..timestamps.len() {
            let row_id = u32::try_from(row_ids.value(i)).map_err(|_| {
                VixError::Malformed(format!(
                    "filtered equality row id {} exceeds u32",
                    row_ids.value(i)
                ))
            })?;
            if !on_candidate(timestamps.value(i), row_id) {
                return Ok(());
            }
        }
    }
    Ok(())
}

/// Count rows matching one string equality over an ascending row range.
/// The filter reads the predicate column; projecting only the synthetic row
/// index prevents the count-only path from fetching or decoding `_timestamp`.
pub(crate) fn count_eq_string_matches(
    blob: &BlobHandle,
    name: &str,
    needle: &str,
    range: std::ops::Range<u64>,
) -> Result<u64> {
    use vortex::{
        dtype::Nullability,
        expr::{col, eq, lit, pack},
        layout::layouts::row_idx::row_idx,
    };

    const ROW_ID_COL: &str = "__vix_row_id";

    if range.start >= range.end {
        return Ok(0);
    }
    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let vxf = open_blob(&runtime, &session, blob)?;
    let projection = pack([(ROW_ID_COL, row_idx())], Nullability::NonNullable);
    let scan = vxf
        .scan()?
        .with_projection(projection)
        .with_filter(eq(col(name), lit(needle.to_string())))
        .with_row_range(range);
    let mut count = 0u64;
    for array in scan.into_array_iter(&runtime)? {
        let batch_rows = u64::try_from(array?.len()).map_err(|_| {
            VixError::Malformed("string equality batch length exceeds u64".to_string())
        })?;
        count = count.checked_add(batch_rows).ok_or_else(|| {
            VixError::Malformed("string equality count overflowed u64".to_string())
        })?;
    }
    Ok(count)
}

/// Scan one column of a stored blob in dictionary form, chunk by chunk,
/// WITHOUT materializing one value per row. Chunks that are
/// dictionary-encoded on disk (the norm for low-cardinality string columns
/// under the BtrBlocks sampler) expose their stored codes/values directly;
/// other encodings are converted by vortex (constants become one-entry
/// dictionaries; the general fallback dictionary-encodes the decoded chunk,
/// which costs about as much as the canonical read). Group-by style
/// consumers count codes and touch each distinct value once instead of
/// hashing one string per row.
pub(crate) fn scan_blob_dict_column(blob: &BlobHandle, name: &str) -> Result<Vec<DictColumnChunk>> {
    use arrow::array::{cast::AsArray, types::UInt64Type};
    use vortex::array::arrays::{Struct, struct_::StructArrayExt};

    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let vxf = open_blob(&runtime, &session, blob)?;
    let scan = vxf.scan()?.with_projection(select(vec![name], root()));
    // dictionary of the column's natural arrow type: values keep their
    // stored type, consumers stringify them (once per distinct value)
    let values_type = scan
        .dtype()?
        .to_arrow_schema()?
        .field_with_name(name)
        .map_err(|_| VixError::Malformed(format!("blob is missing column {name:?}")))?
        .data_type()
        .clone();
    let dict_type = DataType::Dictionary(Box::new(DataType::UInt64), Box::new(values_type));
    let mut chunks = Vec::new();
    for array in scan.into_array_iter(&runtime)? {
        let array = array?;
        // the projected scan yields single-field struct chunks
        let field = array
            .as_typed::<Struct>()
            .ok_or_else(|| {
                VixError::Malformed("projected scan did not produce a struct array".to_string())
            })?
            .unmasked_field_by_name(name)
            .map_err(|e| VixError::Malformed(format!("column {name:?}: {e}")))?
            .clone();
        let target = Field::new("", dict_type.clone(), field.dtype().is_nullable());
        let mut ctx = session.create_execution_ctx();
        let arrow_array = session
            .arrow()
            .execute_arrow(field, Some(&target), &mut ctx)?;
        let dict = arrow_array
            .as_dictionary_opt::<UInt64Type>()
            .ok_or_else(|| {
                VixError::Malformed(format!(
                    "column {name:?} did not convert to a u64-keyed dictionary"
                ))
            })?;
        chunks.push(DictColumnChunk {
            codes: dict.keys().clone(),
            values: Arc::clone(dict.values()),
        });
    }
    Ok(chunks)
}

/// M17 item 2: per-chunk encoding class of the composite-bloom coverage
/// scan (`hash_blob_column_bloom_encoded`) — the prod probe for how much of
/// the demoted-field byte volume rides each fast path.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BloomEncodingCensus {
    /// Chunks served by the DICT arm (only the chunk dictionary decoded;
    /// each referenced distinct value hashed once).
    pub dict_chunks: u64,
    /// Chunks served by the FSST arm (bulk symbol-table decompress, raw
    /// byte slices hashed — no views, no arrow, no utf8 revalidation).
    pub fsst_chunks: u64,
    /// Chunks on the fallback arm (canonical decode, per-row hashing —
    /// exactly the pre-M17 work for that chunk).
    pub other_chunks: u64,
}

impl BloomEncodingCensus {
    pub fn absorb(&mut self, other: BloomEncodingCensus) {
        self.dict_chunks += other.dict_chunks;
        self.fsst_chunks += other.fsst_chunks;
        self.other_chunks += other.other_chunks;
    }

    pub fn chunks(&self) -> u64 {
        self.dict_chunks + self.fsst_chunks + self.other_chunks
    }
}

/// M17 item 2: hash one string-family column's values off its ENCODED
/// chunks for the #52 composite-bloom coverage scan — the M12 measurement
/// left the scan decode-bandwidth-bound (6.0s of a 14s merge wall was
/// decode-to-hash of demoted ID columns), and bloom membership only needs
/// each DISTINCT value's bytes once.
///
/// Per chunk, by stored encoding class (after stripping the dict layout
/// reader's runtime `Shared` wrapper):
/// - **Dict**: decode ONLY the (small) values array + the codes; hash each value REFERENCED by a
///   valid code once — set membership is idempotent, and restricting to referenced+valid+non-null
///   values keeps the hash SET exactly equal to the per-row path (sliced or partially-referenced
///   dictionaries never leak absent values into the bloom).
/// - **FSST**: ONE bulk decompress of the chunk's compressed heap (the same call canonicalization
///   makes), then hash raw byte slices straight off it by the uncompressed-lengths walk — no view
///   building, no arrow conversion, no utf8 revalidation (values were validated at encode).
/// - **anything else**: canonical decode to a Utf8 arrow array and hash per non-null row — the
///   pre-M17 work, chunk-scoped.
///
/// Every arm feeds `observe` NON-NULL raw value bytes; the caller's sink
/// applies THE value policy (len gate + composite key + hash), so coverage
/// is bit-identical to the decoded-column scan by construction (pinned).
/// A non-string-family column hashes nothing (the decoded path's
/// [`StringColumn`] gate) and reports zero chunks.
pub(crate) fn hash_blob_column_bloom_encoded(
    blob: &BlobHandle,
    name: &str,
    observe: &mut dyn FnMut(&[u8]),
) -> Result<BloomEncodingCensus> {
    use arrow::array::StringArray;
    use vortex::{
        array::{
            arrays::{
                Dict, Struct, dict::DictArraySlotsExt, shared::SharedArrayExt,
                struct_::StructArrayExt, varbin::VarBinArrayExt,
            },
            validity::Validity,
        },
        encodings::fsst::{FSST, FSSTArrayExt},
    };

    let mut census = BloomEncodingCensus::default();
    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let vxf = open_blob(&runtime, &session, blob)?;
    let scan = vxf.scan()?.with_projection(select(vec![name], root()));
    // the decoded path hashes nothing for non-string columns
    // (StringColumn::try_new returns None) — parity demands the same here
    let stored_type = scan
        .dtype()?
        .to_arrow_schema()?
        .field_with_name(name)
        .map_err(|_| VixError::Malformed(format!("blob is missing column {name:?}")))?
        .data_type()
        .clone();
    if !matches!(
        stored_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) {
        return Ok(census);
    }
    let to_utf8 = |field: &ArrayRef, session: &VortexSession| -> Result<StringArray> {
        let target = Field::new("", DataType::Utf8, field.dtype().is_nullable());
        let mut ctx = session.create_execution_ctx();
        let arrow_array = session
            .arrow()
            .execute_arrow(field.clone(), Some(&target), &mut ctx)?;
        arrow_array
            .as_any()
            .downcast_ref::<StringArray>()
            .cloned()
            .ok_or_else(|| VixError::Malformed(format!("column {name:?} did not convert to Utf8")))
    };
    for array in scan.into_array_iter(&runtime)? {
        let array = array?;
        if array.is_empty() {
            continue;
        }
        let field = array
            .as_typed::<Struct>()
            .ok_or_else(|| {
                VixError::Malformed("projected scan did not produce a struct array".to_string())
            })?
            .unmasked_field_by_name(name)
            .map_err(|e| VixError::Malformed(format!("column {name:?}: {e}")))?
            .clone();
        let rows = field.len();
        // unwrap execution-cache wrappers so the stored encoding is visible
        let stored = match field.as_typed::<vortex::array::arrays::Shared>() {
            Some(shared) => shared.source().clone(),
            None => field.clone(),
        };

        // DICT arm: values decoded once, codes as u64 + validity
        if let Some(dict) = stored.as_typed::<Dict>() {
            let values = to_utf8(dict.values(), &session);
            let codes = {
                let codes_target = Field::new("", DataType::UInt64, true);
                let mut ctx = session.create_execution_ctx();
                session
                    .arrow()
                    .execute_arrow(dict.codes().clone(), Some(&codes_target), &mut ctx)
                    .ok()
                    .and_then(|a| a.as_any().downcast_ref::<UInt64Array>().cloned())
            };
            if let (Ok(values), Some(codes)) = (values, codes)
                && codes.len() == rows
            {
                let mut referenced = vec![false; values.len()];
                for i in 0..codes.len() {
                    if codes.is_null(i) {
                        continue;
                    }
                    let code = codes.value(i) as usize;
                    if let Some(slot) = referenced.get_mut(code) {
                        *slot = true;
                    }
                }
                for (index, seen) in referenced.iter().enumerate() {
                    if *seen && !values.is_null(index) {
                        observe(values.value(index).as_bytes());
                    }
                }
                census.dict_chunks += 1;
                continue;
            }
            // conversion failure: fall through to the canonical arm
        }

        // FSST arm: bulk decompress + lengths walk (canonicalize_fsst's
        // exact decompression, minus the view/arrow materialization)
        if let Some(fsst) = stored.as_typed::<FSST>() {
            let handled = (|| -> Result<bool> {
                let codes = fsst.codes();
                let validity = codes.varbin_validity();
                if matches!(validity, Validity::AllInvalid) {
                    return Ok(true); // every row null: nothing to hash
                }
                let mask = match &validity {
                    Validity::NonNullable | Validity::AllValid => None,
                    Validity::AllInvalid => unreachable!("handled above"),
                    Validity::Array(_) => {
                        let mut ctx = session.create_execution_ctx();
                        Some(validity.execute_mask(rows, &mut ctx).map_err(|e| {
                            VixError::Malformed(format!("column {name:?} validity: {e}"))
                        })?)
                    }
                };
                let lens = {
                    let lens_target = Field::new("", DataType::UInt64, false);
                    let mut ctx = session.create_execution_ctx();
                    session
                        .arrow()
                        .execute_arrow(
                            fsst.uncompressed_lengths().clone(),
                            Some(&lens_target),
                            &mut ctx,
                        )
                        .ok()
                        .and_then(|a| a.as_any().downcast_ref::<UInt64Array>().cloned())
                };
                let Some(lens) = lens else {
                    return Ok(false); // unexpected lengths shape: canonical arm decides
                };
                if lens.len() != rows || lens.null_count() > 0 {
                    return Ok(false);
                }
                let total: usize = lens.values().iter().map(|&l| l as usize).sum();
                let compressed = codes.sliced_bytes();
                let decompressor = fsst.decompressor();
                let mut heap: Vec<u8> = Vec::with_capacity(total + 7);
                let produced =
                    decompressor.decompress_into(compressed.as_slice(), heap.spare_capacity_mut());
                if produced != total {
                    return Ok(false); // lengths disagree: canonical arm decides
                }
                // SAFETY: decompress_into initialized exactly `produced`
                // bytes of the spare capacity checked above.
                unsafe { heap.set_len(produced) };
                let mut offset = 0usize;
                for row in 0..rows {
                    let len = lens.value(row) as usize;
                    let valid = mask.as_ref().map_or(true, |mask| mask.value(row));
                    if valid {
                        observe(&heap[offset..offset + len]);
                    }
                    offset += len;
                }
                Ok(true)
            })();
            match handled {
                Ok(true) => {
                    census.fsst_chunks += 1;
                    continue;
                }
                Ok(false) => {}
                Err(error) => {
                    log::debug!(
                        "vix bloom encoded scan: column {name:?} FSST arm fell back to \
                         canonical: {error}"
                    );
                }
            }
        }

        // canonical fallback: the pre-M17 per-row hashing, chunk-scoped
        let values = to_utf8(&field, &session)?;
        for row in 0..values.len() {
            if !values.is_null(row) {
                observe(values.value(row).as_bytes());
            }
        }
        census.other_chunks += 1;
    }
    Ok(census)
}

/// Convert one Vortex struct array chunk into an arrow [`RecordBatch`] with
/// the requested struct `data_type` (local port of
/// `config::utils::parquet::vortex_array_to_record_batch`).
fn vortex_to_record_batch(
    session: &VortexSession,
    array: ArrayRef,
    data_type: &DataType,
) -> Result<RecordBatch> {
    let mut ctx = session.create_execution_ctx();
    let target = Field::new("", data_type.clone(), array.dtype().is_nullable());
    let arrow_array = session
        .arrow()
        .execute_arrow(array, Some(&target), &mut ctx)?;
    let struct_array = arrow_array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            VixError::Malformed("vortex scan did not produce a struct array".to_string())
        })?;
    Ok(RecordBatch::from(struct_array))
}

fn get_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a ArrowArrayRef> {
    batch
        .column_by_name(name)
        .ok_or_else(|| VixError::Malformed(format!("blob is missing column {name:?}")))
}

/// Fetch a binary-family column, normalized to `LargeBinary`.
pub(crate) fn column_binary(batch: &RecordBatch, name: &str) -> Result<LargeBinaryArray> {
    let column = cast(get_column(batch, name)?, &DataType::LargeBinary)
        .map_err(|e| VixError::Malformed(format!("column {name:?} is not binary: {e}")))?;
    column
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .cloned()
        .ok_or_else(|| VixError::Malformed(format!("column {name:?} is not binary")))
}

/// Borrow a physically-u32 column's values without casting or copying —
/// the per-batch view for hot column loops (#29's doc_count scans), where
/// [`column_u64`]'s cast + `to_vec` would copy every batch twice. Hard
/// error on any other width: the terms schema has written `doc_count` as
/// `UInt32` since the format epoch (no legacy readers by design).
pub(crate) fn column_u32<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a UInt32Array> {
    let column = get_column(batch, name)?
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| {
            VixError::Malformed(format!(
                "column {name:?} is not physically u32 (terms schema drift?)"
            ))
        })?;
    if column.null_count() > 0 {
        return Err(VixError::Malformed(format!(
            "column {name:?} unexpectedly contains nulls"
        )));
    }
    Ok(column)
}

/// Fetch an unsigned integer column as non-null `u64` values.
pub(crate) fn column_u64(batch: &RecordBatch, name: &str) -> Result<Vec<u64>> {
    let column = cast(get_column(batch, name)?, &DataType::UInt64)
        .map_err(|e| VixError::Malformed(format!("column {name:?} is not an integer: {e}")))?;
    let column = column
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| VixError::Malformed(format!("column {name:?} is not an integer")))?;
    if column.null_count() > 0 {
        return Err(VixError::Malformed(format!(
            "column {name:?} unexpectedly contains nulls"
        )));
    }
    Ok(column.values().to_vec())
}

/// M17 throwaway diagnostic (test-only): print one column's stored chunk
/// encoding trees.
#[cfg(test)]
pub(crate) fn probe_column_encodings(docs: &crate::VixDocs, name: &str) {
    use vortex::array::arrays::{Struct, struct_::StructArrayExt};
    let blob = docs.docs_blob_handle();
    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let vxf = open_blob(&runtime, &session, blob).unwrap();
    let scan = vxf
        .scan()
        .unwrap()
        .with_projection(select(vec![name], root()));
    for (i, array) in scan.into_array_iter(&runtime).unwrap().enumerate() {
        let array = array.unwrap();
        let field = array
            .as_typed::<Struct>()
            .unwrap()
            .unmasked_field_by_name(name)
            .unwrap()
            .clone();
        println!(
            "column {name} chunk {i}: rows={} top={:?} display={}",
            field.len(),
            field.encoding_id(),
            field
        );
        let mut count = 0;
        for node in field.depth_first_traversal() {
            count += 1;
            if count <= 6 {
                println!("   node: {:?}", node.encoding_id());
            }
        }
        println!("   ({count} nodes)");
    }
}
