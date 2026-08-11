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

//! Container plumbing: the puffin envelope and the embedded Vortex files.
//!
//! A `.vix` file is a puffin container with up to three blobs (`dict`,
//! `terms`, `docs`), each an embedded Vortex file, plus JSON string
//! properties on the puffin footer. This module owns:
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
    datatypes::{DataType, Field, Schema},
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
    session::VortexSession,
};

use crate::{
    error::{Result, VixError},
    source::{RangedBlob, VixRangeSource, block_fetch},
};

/// Bytes fetched from the object tail when opening ranged: covers the puffin
/// footer (a small JSON payload) in one read for all but pathological files,
/// and doubles as a window small blobs are sliced from for free.
const TAIL_FETCH_SIZE: u64 = 64 * 1024;

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

pub(crate) const PROP_VERSION: &str = "version";
pub(crate) const PROP_ROW_COUNT: &str = "row_count";
pub(crate) const PROP_TERM_COUNT: &str = "term_count";
pub(crate) const PROP_ROW_GROUP_SIZE: &str = "row_group_size";
pub(crate) const PROP_FIELDS: &str = "fields";
pub(crate) const PROP_PARTIAL_FIELDS: &str = "partial_fields";
pub(crate) const PROP_TOKENIZER: &str = "tokenizer";
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

/// The `version` property value this crate writes and requires — the one
/// future-evolution discriminator of the `.vix` format. Readers accept
/// exactly this value; bump it on any breaking format change. Extra or
/// unknown properties (e.g. the retired `format` key older files carry) are
/// ignored generically.
pub(crate) const VIX_FORMAT_VERSION: &str = "2";
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

/// Require the container's `version` property to be [`VIX_FORMAT_VERSION`]
/// and its `key_layout` property to be [`KEY_LAYOUT_FID_V2`] — the single
/// clear rejection shared by every open path. Absent or different values
/// error; any other property the reader does not know is ignored.
pub(crate) fn require_supported_format(properties: &BTreeMap<String, String>) -> Result<()> {
    match properties.get(PROP_VERSION).map(String::as_str) {
        Some(VIX_FORMAT_VERSION) => {}
        Some(other) => {
            return Err(VixError::UnsupportedFormat(format!(
                "version {other:?}, reader supports {VIX_FORMAT_VERSION}"
            )));
        }
        None => {
            return Err(VixError::UnsupportedFormat(format!(
                "no version property, reader supports {VIX_FORMAT_VERSION}"
            )));
        }
    }
    // Key layout: exactly fid_v2. ABSENT marks a pre-v2 file — the retired
    // v1 layout, unreadable since its read support was dropped — and any
    // OTHER value is a future layout this build does not implement. Both
    // are HARD errors: a field-major probe against a foreign layout
    // silently returns wrong results, which must never happen.
    match properties.get(PROP_KEY_LAYOUT).map(String::as_str) {
        Some(KEY_LAYOUT_FID_V2) => Ok(()),
        None => Err(VixError::UnsupportedFormat(
            "pre-v2 .vix file (no key_layout property) is no longer supported; rebuild from \
             _source"
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

impl std::fmt::Debug for BlobHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlobHandle::Mem(bytes) => f.debug_tuple("Mem").field(&bytes.len()).finish(),
            BlobHandle::Ranged(blob) => f.debug_tuple("Ranged").field(blob).finish(),
        }
    }
}

/// The parsed puffin envelope of a `.vix` file: file properties plus handles
/// to the recognized blobs.
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
/// [`TAIL_FETCH_SIZE`] bytes (a second, precise fetch when the footer payload
/// exceeds it) yields the footer; blobs fully covered by the fetched tail are
/// sliced from it, all others become on-demand windows of `source`.
pub(crate) fn parse_container_ranged(source: &Arc<dyn VixRangeSource>) -> Result<VixContainer> {
    let total = source.len();
    if total < MIN_FILE_SIZE {
        return Err(VixError::Malformed(format!(
            "file too small to be a puffin container: {total} bytes"
        )));
    }

    // Tail probe. The footer region is `HeadMagic[4] + payload + FOOTER_SIZE`
    // at the very end of the file; read the payload size out of the footer
    // tail and refetch precisely when the probe fell short.
    let mut tail_start = total.saturating_sub(TAIL_FETCH_SIZE);
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

/// Assemble the final puffin container from properties and pre-built blobs
/// (`(blob type id, blob tag, bytes)`).
pub(crate) fn build_container(
    properties: Vec<(String, String)>,
    blobs: Vec<(&'static str, &'static str, Vec<u8>)>,
) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    {
        let mut writer = PuffinBytesWriter::new(&mut buf);
        for (key, value) in properties {
            writer.set_property(key, value);
        }
        for (blob_type, tag, data) in blobs {
            writer
                .add_blob(&data, blob_type, tag.to_string())
                .map_err(|e| VixError::Writer(format!("puffin blob write: {e:#}")))?;
        }
        writer
            .finish()
            .map_err(|e| VixError::Writer(format!("puffin finish: {e:#}")))?;
    }
    Ok(buf)
}

/// Assemble the final puffin container around a `docs` blob that was already
/// STREAMED into the sink (which holds the puffin `MAGIC` followed by exactly
/// `docs_len` docs-blob bytes — a [`DocsBlobEncoder`]'s output): the docs
/// bytes are recorded in place — never copied — and the remaining blobs +
/// footer append after them, each dropped as soon as it is written. Blob
/// order in the file is therefore `docs` first, index blobs after it,
/// clustered at the tail next to the footer; readers locate blobs by
/// tag/offset, so order carries no meaning. A spooled sink keeps the whole
/// container out of RAM.
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
/// [`write_vortex_blob`] (`encode_threads` > 1 runs chunk compression on a
/// dedicated pool). Dropping the encoder without [`Self::signal_finish`]
/// aborts the worker on its next receive.
pub(crate) struct DocsBlobEncoder {
    tx: Option<std::sync::mpsc::SyncSender<DocsEncodeMsg>>,
    handle: Option<std::thread::JoinHandle<Result<(ContainerSink, u64)>>>,
}

impl DocsBlobEncoder {
    /// Spawn the worker. `rows_per_chunk` is the locked docs chunking
    /// (`0` keeps vortex's default — the empty-file shape); `spool_dir`
    /// spools the container to a temp file there instead of RAM.
    pub(crate) fn spawn(
        schema: Arc<Schema>,
        rows_per_chunk: usize,
        encode_threads: usize,
        spool_dir: Option<std::path::PathBuf>,
    ) -> Result<Self> {
        let (tx, rx) = std::sync::mpsc::sync_channel(2);
        let handle = std::thread::Builder::new()
            .name("vix-docs-encode".to_string())
            .spawn(move || {
                run_docs_encoder(&schema, rows_per_chunk, encode_threads, spool_dir, &rx)
            })
            .map_err(|e| VixError::Writer(format!("spawn docs encoder thread: {e}")))?;
        Ok(Self {
            tx: Some(tx),
            handle: Some(handle),
        })
    }

    /// Queue one batch for encoding (blocks when the channel is full).
    pub(crate) fn push(&mut self, batch: RecordBatch) -> Result<()> {
        let tx = self
            .tx
            .as_ref()
            .ok_or_else(|| VixError::Writer("docs encoder already finished".to_string()))?;
        if tx.send(DocsEncodeMsg::Batch(batch)).is_err() {
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

/// The worker body: encode received batches into a `MAGIC`-prefixed buffer,
/// mirroring [`write_vortex_blob`]'s runtime/pool shape.
fn run_docs_encoder(
    schema: &Schema,
    rows_per_chunk: usize,
    encode_threads: usize,
    spool_dir: Option<std::path::PathBuf>,
    rx: &std::sync::mpsc::Receiver<DocsEncodeMsg>,
) -> Result<(ContainerSink, u64)> {
    let runtime = SingleThreadRuntime::default();
    let mut sink = ContainerSink::create(spool_dir.as_deref())?;
    let run = |sink: &mut ContainerSink, session| -> Result<u64> {
        match sink {
            ContainerSink::Mem(buf) => {
                let before = buf.len() as u64;
                encode_docs_stream(&runtime, session, schema, rows_per_chunk, rx, &mut *buf)?;
                Ok(buf.len() as u64 - before)
            }
            ContainerSink::File(file) => {
                let mut counting = CountingWriter {
                    inner: std::io::BufWriter::with_capacity(1024 * 1024, file.as_file_mut()),
                    written: 0,
                };
                encode_docs_stream(&runtime, session, schema, rows_per_chunk, rx, &mut counting)?;
                use std::io::Write;
                counting
                    .flush()
                    .map_err(|e| VixError::Writer(format!("flush docs spool: {e}")))?;
                Ok(counting.written)
            }
        }
    };
    let result = if encode_threads > 1 {
        let pool = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(encode_threads)
            .thread_name("vix-encode")
            .build()
            .map_err(|e| VixError::Writer(format!("encode thread pool: {e}")))?;
        let pool_runtime = TokioRuntime::new(pool.handle().clone());
        let session = VortexSession::default().with_handle(pool_runtime.handle());
        let result = run(&mut sink, session);
        // Non-blocking shutdown: safe on any thread. All encode tasks
        // completed before the writer's `finish` returned.
        pool.shutdown_background();
        result
    } else {
        let session = VortexSession::default().with_handle(runtime.handle());
        run(&mut sink, session)
    };
    result.map(|docs_len| (sink, docs_len))
}

fn encode_docs_stream<W: std::io::Write + Unpin>(
    runtime: &SingleThreadRuntime,
    session: VortexSession,
    schema: &Schema,
    rows_per_chunk: usize,
    rx: &std::sync::mpsc::Receiver<DocsEncodeMsg>,
    sink: &mut W,
) -> Result<()> {
    let dtype = DType::from_arrow(schema);
    let mut writer = VortexWriteOptions::new(session)
        .with_strategy(docs_strategy(rows_per_chunk))
        .blocking(runtime)
        .writer(&mut *sink, dtype);
    let mut finished = false;
    while let Ok(msg) = rx.recv() {
        match msg {
            DocsEncodeMsg::Batch(batch) => {
                if batch.num_rows() == 0 {
                    continue;
                }
                writer.push(ArrayRef::from_arrow(&batch, false)?)?;
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
/// `encode_threads > 1` runs the chunk encoding/compression pipeline on a
/// dedicated multi-thread pool (vortex's layout writers spawn one CPU task
/// per chunk onto the session handle); `0`/`1` keeps everything on the
/// calling thread. The caller thread only pumps chunks and collects buffers
/// either way, so the produced bytes are identical.
pub(crate) fn write_vortex_blob(
    schema: &Schema,
    batches: &[RecordBatch],
    strategy: Arc<dyn LayoutStrategy>,
    encode_threads: usize,
) -> Result<Vec<u8>> {
    let runtime = SingleThreadRuntime::default();
    if encode_threads > 1 {
        let pool = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(encode_threads)
            .thread_name("vix-encode")
            .build()
            .map_err(|e| VixError::Writer(format!("encode thread pool: {e}")))?;
        let pool_runtime = TokioRuntime::new(pool.handle().clone());
        let session = VortexSession::default().with_handle(pool_runtime.handle());
        let result = write_vortex_blob_inner(&runtime, session, schema, batches, strategy);
        // Non-blocking shutdown: safe on any thread (including inside an
        // async context, where dropping a runtime would panic). All encode
        // tasks completed before `finish` returned.
        pool.shutdown_background();
        result
    } else {
        let session = VortexSession::default().with_handle(runtime.handle());
        write_vortex_blob_inner(&runtime, session, schema, batches, strategy)
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
fn open_blob(
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
pub(crate) type ZoneEntry = (u64, i64, i64);

/// One chunk of a stored column in dictionary form, as arrow arrays:
/// `codes[i]` indexes into `values` (a null code = a null row). Dictionaries
/// are per chunk — consecutive chunks may carry different value sets, and a
/// value is not guaranteed to be referenced by any code.
pub(crate) struct DictColumnChunk {
    pub codes: arrow::array::UInt64Array,
    pub values: ArrowArrayRef,
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
