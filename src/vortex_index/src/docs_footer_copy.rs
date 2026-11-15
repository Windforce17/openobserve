// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! An optional, self-compressed docs footer bound to the sidecar's data metadata.
//! No copy bytes or parsed descriptors are retained by a reader at open.
use std::{
    io,
    ops::Range,
    sync::{Arc, OnceLock},
};

use bytes::Bytes;
use futures::{FutureExt, future::BoxFuture};
use puffin::{BlobMetadata, PuffinMeta};
use serde::{Deserialize, Serialize};
use vortex::{
    buffer::ByteBuffer,
    file::{DeserializeStep, Footer},
    session::VortexSession,
};

use crate::{
    VixRangeSource,
    container::{self, BlobHandle, VixOutput},
    error::{Result, VixError},
    source::{self, RangedBlob},
};

pub(crate) const TAG: &str = "docs_footer_copy";
pub(crate) const TYPE: &str = "o2-vix-docs-footer-copy-v1";
const MAGIC: &[u8; 8] = b"O2DCF002";
const MAX_RAW: usize = 16 * 1024 * 1024;
const MAX_HEADER: usize = 4096;
const WRITE_ENV: &str = "ZO_VIX_SIDECAR_DOCS_FOOTER_COPY";
const READ_ENV: &str = "ZO_VIX_SIDECAR_DOCS_FOOTER_COPY_READ";
static SETTINGS: OnceLock<Settings> = OnceLock::new();

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Settings {
    pub write: bool,
    pub read: bool,
}

impl Settings {
    fn parse(mut get: impl FnMut(&str) -> io::Result<Option<String>>) -> io::Result<Self> {
        let mut boolean = |name, default| match get(name)?.as_deref() {
            None => Ok(default),
            Some("true" | "1") => Ok(true),
            Some("false" | "0") => Ok(false),
            Some(value) => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{name}={value:?}: expected 0, 1, false, or true"),
            )),
        };
        Ok(Self {
            write: boolean(WRITE_ENV, false)?,
            read: boolean(READ_ENV, true)?,
        })
    }
}

pub(crate) fn settings() -> io::Result<Settings> {
    if let Some(settings) = SETTINGS.get() {
        return Ok(*settings);
    }
    let settings = Settings::parse(|name| match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{name}: {error}"),
        )),
    })?;
    let _ = SETTINGS.set(settings);
    Ok(*SETTINGS.get().expect("settings initialized"))
}

fn malformed(error: impl std::fmt::Display) -> VixError {
    VixError::Malformed(format!("docs footer copy: {error}"))
}

#[cfg(test)]
thread_local! {
    pub(super) static FINGERPRINT_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

pub(crate) fn fingerprint(meta: &PuffinMeta) -> Result<String> {
    #[cfg(test)]
    FINGERPRINT_CALLS.with(|calls| calls.set(calls.get() + 1));
    Ok(sha256::digest(serde_json::to_vec(meta)?))
}

pub(crate) fn is_copy(blob: &BlobMetadata) -> bool {
    blob.blob_type == TYPE
        && blob
            .properties
            .get("blob_tag")
            .is_some_and(|tag| tag == TAG)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Header {
    data_len: u64,
    docs_offset: u64,
    docs_len: u64,
    footer_start: u64,
    raw_len: usize,
    raw_sha256: String,
    data_footer_sha256: String,
}

fn checked_range(blob: &BlobMetadata, total: u64) -> Result<Range<u64>> {
    let end = blob
        .offset
        .checked_add(blob.length)
        .filter(|end| *end <= total)
        .ok_or_else(|| malformed("blob bounds"))?;
    Ok(blob.offset..end)
}

/// Return exactly the suffix retained by the ordinary Vortex footer path.
fn footer_start(range: &Range<u64>, suffix: &[u8]) -> Result<u64> {
    let eof = vortex::file::EOF_SIZE;
    if suffix.len() < eof {
        return Err(malformed("short EOF"));
    }
    let ps_len = u16::from_le_bytes(
        suffix[suffix.len() - eof + 2..suffix.len() - eof + 4]
            .try_into()
            .expect("two bytes"),
    ) as usize;
    if ps_len + eof > suffix.len() {
        return Err(malformed("short postscript"));
    }
    let mut decoder = Footer::deserializer(
        ByteBuffer::copy_from(&suffix[suffix.len() - ps_len - eof..]),
        VortexSession::empty(),
    )
    .with_size(range.end - range.start);
    let DeserializeStep::NeedMoreData { offset, .. } = decoder.deserialize()? else {
        return Err(malformed("invalid postscript"));
    };
    let consumed = range
        .start
        .checked_add(offset)
        .filter(|start| *start < range.end)
        .ok_or_else(|| malformed("footer offset"))?;
    Ok(consumed
        .min(range.end.saturating_sub(source::VORTEX_FOOTER_READ_BYTES))
        .max(range.start))
}

fn prepare(
    data_len: u64,
    payload: &[u8],
    mut read: impl FnMut(Range<u64>) -> Result<Bytes>,
) -> Result<Vec<u8>> {
    let meta: PuffinMeta = serde_json::from_slice(payload)?;
    let docs = meta
        .blobs
        .iter()
        .find(|b| {
            b.blob_type == container::BLOB_TYPE_DOCS
                && b.properties
                    .get("blob_tag")
                    .is_some_and(|t| t == container::BLOB_TAG_DOCS)
        })
        .ok_or_else(|| malformed("missing docs"))?;
    let range = checked_range(docs, data_len)?;
    let suffix = read(
        range
            .end
            .saturating_sub(source::VORTEX_FOOTER_READ_BYTES)
            .max(range.start)..range.end,
    )?;
    let start = footer_start(&range, &suffix)?;
    let raw_len = usize::try_from(range.end - start).map_err(malformed)?;
    if raw_len > MAX_RAW {
        return Err(malformed("footer exceeds 16 MiB"));
    }
    let raw = if raw_len <= suffix.len() {
        suffix.slice(suffix.len() - raw_len..)
    } else {
        read(start..range.end)?
    };
    let header = Header {
        data_len,
        docs_offset: range.start,
        docs_len: range.end - range.start,
        footer_start: start,
        raw_len,
        raw_sha256: sha256::digest(raw.as_ref()),
        data_footer_sha256: fingerprint(&meta)?,
    };
    encode(&header, &raw)
}

fn encode(header: &Header, raw: &[u8]) -> Result<Vec<u8>> {
    let json = serde_json::to_vec(header)?;
    let mut bytes = MAGIC.to_vec();
    bytes.extend_from_slice(&(json.len() as u32).to_le_bytes());
    bytes.extend(json);
    bytes.extend(zstd::bulk::compress(raw, 3).map_err(malformed)?);
    Ok(bytes)
}

pub(crate) fn prepare_output(output: &VixOutput, payload: &[u8]) -> Result<Option<Vec<u8>>> {
    if !settings().map_err(malformed)?.write {
        return Ok(None);
    }
    optional_write(prepare(output.len(), payload, |range| match output {
        VixOutput::Bytes(bytes) => Ok(Bytes::copy_from_slice(
            &bytes[range.start as usize..range.end as usize],
        )),
        VixOutput::Spooled { file, .. } => {
            use std::os::unix::fs::FileExt;
            let mut bytes = vec![0; (range.end - range.start) as usize];
            file.as_file()
                .read_exact_at(&mut bytes, range.start)
                .map_err(malformed)?;
            Ok(Bytes::from(bytes))
        }
    }))
}

pub(crate) fn prepare_source(
    source: &Arc<dyn VixRangeSource>,
    payload: &[u8],
) -> Result<Option<Vec<u8>>> {
    if !settings().map_err(malformed)?.write {
        return Ok(None);
    }
    optional_write(prepare(source.len(), payload, |range| {
        source::block_fetch(source.as_ref(), range)
    }))
}

fn optional_write(result: Result<Vec<u8>>) -> Result<Option<Vec<u8>>> {
    match result {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error @ (VixError::Cancelled | VixError::Callback(_))) => Err(error),
        Err(error) => {
            log::warn!("vix: omitting optional {error}");
            Ok(None)
        }
    }
}

/// Append in place; preserve every existing blob's bytes, offsets and codec.
/// The admission includes the complete terms footer, dict and outer footer.
pub(crate) fn append(index: &mut Vec<u8>, copy: Option<&[u8]>, eager_tail: u64) -> Result<bool> {
    let Some(copy) = copy else {
        return Ok(false);
    };
    let mut meta = puffin::reader::parse_puffin_footer_from_bytes(index).map_err(malformed)?;
    if meta.blobs.iter().any(is_copy) {
        return Err(malformed("cannot inherit an existing copy"));
    }
    let footer_len = u32::from_le_bytes(
        index[index.len() - 12..index.len() - 8]
            .try_into()
            .expect("four bytes"),
    ) as usize
        + 16;
    let offset = index.len() - footer_len;
    let mut hot_start = offset as u64;
    for blob in &meta.blobs {
        let range = checked_range(blob, offset as u64)?;
        match blob.blob_type.as_str() {
            container::BLOB_TYPE_DICT => hot_start = hot_start.min(range.start),
            container::BLOB_TYPE_TERMS => {
                let suffix_start = range
                    .end
                    .saturating_sub(source::VORTEX_FOOTER_READ_BYTES)
                    .max(range.start);
                hot_start = hot_start.min(footer_start(
                    &range,
                    &index[suffix_start as usize..range.end as usize],
                )?);
            }
            _ => {}
        }
    }
    if index.len() as u64 - hot_start > eager_tail {
        return Ok(false);
    }
    meta.blobs.push(BlobMetadata {
        blob_type: TYPE.into(),
        offset: offset as u64,
        length: copy.len() as u64,
        properties: [("blob_tag".into(), TAG.into())].into(),
        ..Default::default()
    });
    let compressed = index[index.len() - 8] & 1 != 0;
    let payload = serde_json::to_vec(&meta)?;
    let stored = if compressed {
        zstd::bulk::compress(&payload, puffin::FOOTER_ZSTD_LEVEL).map_err(malformed)?
    } else {
        payload
    };
    if offset as u64 + copy.len() as u64 + stored.len() as u64 + 16 - hot_start > eager_tail {
        return Ok(false);
    }
    let size = u32::try_from(stored.len()).map_err(malformed)?;
    index.truncate(offset);
    index.extend_from_slice(copy);
    index.extend_from_slice(&puffin::MAGIC);
    index.extend(stored);
    index.extend_from_slice(&size.to_le_bytes());
    index.extend_from_slice(&u32::from(compressed).to_le_bytes());
    index.extend_from_slice(&puffin::MAGIC);
    Ok(true)
}

pub(crate) fn for_source(
    index: &container::VixContainer,
    source: &Arc<dyn VixRangeSource>,
) -> bool {
    index.docs_copy_available
        && settings().is_ok_and(|settings| settings.read)
        && !source
            .describe()
            .rsplit('/')
            .next()
            .is_some_and(|key| key.starts_with("l0_"))
}

pub(crate) fn eligible(data: &container::VixContainer, index: &container::VixContainer) -> bool {
    let Some(data_fingerprint) = data.footer_fingerprint.as_ref() else {
        return false;
    };
    // Segment builders use deterministic l0_* keys and overwrite on lease retries.
    // Do not extend sidecar-first metadata trust to their docs layout.
    let mutable_key = matches!(&data.docs, Some(BlobHandle::Ranged(blob))
        if blob.source.describe().rsplit('/').next().is_some_and(|key| key.starts_with("l0_")));
    !mutable_key
        && index.docs_copy_available
        && settings().is_ok_and(|settings| settings.read)
        && index
            .properties
            .get(container::PROP_DATA_FOOTER)
            .and_then(|payload| serde_json::from_str::<PuffinMeta>(payload).ok())
            .and_then(|meta| fingerprint(&meta).ok())
            .is_some_and(|hash| &hash == data_fingerprint)
}

fn header(
    bytes: &[u8],
    data_len: u64,
    docs: &Range<u64>,
    meta: &PuffinMeta,
) -> Result<(Header, usize)> {
    if bytes.len() < 12 || &bytes[..8] != MAGIC {
        return Err(malformed("magic"));
    }
    let len = u32::from_le_bytes(bytes[8..12].try_into().expect("four bytes")) as usize;
    if len > MAX_HEADER || len + 12 > bytes.len() {
        return Err(malformed("header bound"));
    }
    let header: Header = serde_json::from_slice(&bytes[12..12 + len])?;
    if header.data_len != data_len
        || header.docs_offset != docs.start
        || header.docs_offset.checked_add(header.docs_len) != Some(docs.end)
        || header.data_footer_sha256 != fingerprint(meta)?
        || header.raw_len > MAX_RAW
        || header.raw_len == 0
        || header.footer_start < docs.start
        || header.footer_start.checked_add(header.raw_len as u64) != Some(docs.end)
    {
        return Err(malformed("identity or bounds"));
    }
    Ok((header, 12 + len))
}

/// A transient source: failed parses cannot fetch data or publish partial state.
struct CopySource {
    len: u64,
    range: Range<u64>,
    bytes: Bytes,
}
impl VixRangeSource for CopySource {
    fn len(&self) -> u64 {
        self.len
    }
    fn resident(&self, range: Range<u64>) -> Option<Bytes> {
        (range.start <= range.end && self.range.start <= range.start && range.end <= self.range.end)
            .then(|| {
                self.bytes.slice(
                    (range.start - self.range.start) as usize
                        ..(range.end - self.range.start) as usize,
                )
            })
    }
    fn fetch(&self, range: Range<u64>) -> BoxFuture<'static, anyhow::Result<Bytes>> {
        futures::future::ready(
            self.resident(range)
                .ok_or_else(|| anyhow::anyhow!("incomplete docs footer copy")),
        )
        .boxed()
    }
}

pub(crate) fn load(index: &Arc<dyn VixRangeSource>, docs: &RangedBlob) -> Result<bool> {
    let memory = source::current_reader_memory();
    let (meta, _metadata) = container::read_footer_meta(index)?;
    let Some(blob) = meta.blobs.iter().find(|blob| is_copy(blob)) else {
        return Ok(false);
    };
    if blob.compression_codec.is_some() || blob.length > (MAX_RAW + MAX_HEADER + 12) as u64 {
        return Ok(false);
    }
    let Some(payload) = meta.properties.get(container::PROP_DATA_FOOTER) else {
        return Ok(false);
    };
    let data_meta: PuffinMeta = serde_json::from_str(payload)?;
    let range = checked_range(blob, index.len())?;
    let Some(_encoded) = memory.try_reserve_with(
        blob.length as usize + MAX_HEADER,
        source::try_check_read_memory,
    )?
    else {
        return Ok(false);
    };
    let bytes = source::block_fetch(index.as_ref(), range)?;
    let (header, start) = header(&bytes, docs.source.len(), &docs.range, &data_meta)?;
    let Some(_decode) = memory.try_reserve_with(
        container::footer_memory_bound(header.raw_len).saturating_add(1024 * 1024),
        source::try_check_read_memory,
    )?
    else {
        return Ok(false);
    };
    let raw = zstd::bulk::decompress(&bytes[start..], header.raw_len).map_err(malformed)?;
    if raw.len() != header.raw_len || sha256::digest(&raw) != header.raw_sha256 {
        return Err(malformed("checksum"));
    }
    let source: Arc<dyn VixRangeSource> = Arc::new(CopySource {
        len: header.data_len,
        range: header.footer_start..docs.range.end,
        bytes: Bytes::from(raw),
    });
    let candidate = BlobHandle::Ranged(RangedBlob::with_initial_read(
        source,
        docs.range.clone(),
        header.raw_len as u64,
    ));
    drop(container::blob_arrow_schema_owned(&candidate)?);
    if let BlobHandle::Ranged(candidate) = candidate {
        docs.adopt_footer(&candidate);
    }
    Ok(true)
}

#[cfg(test)]
#[path = "docs_footer_copy_tests.rs"]
mod tests;
