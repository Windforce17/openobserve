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

//! Segment object encoding (DESIGN-SEGMENT-WAL.md).
//!
//! Every format version starts with the same raw header (all integers
//! little-endian):
//!
//! ```text
//! header (raw): magic "O2WS" | u16 version | u16 uuid_len | uuid bytes
//!             | u64 seq | i64 created_at_micros
//! ```
//!
//! Both versions carry the same data frame:
//!
//! ```text
//! data frame: u8 frame_type=1
//!           | u16 org_len | org
//!           | u16 stream_type_len | stream_type (Display string)
//!           | u16 stream_len | stream
//!           | i64 min_ts | i64 max_ts | u32 rows
//!           | u32 ipc_len | arrow IPC STREAM bytes (self-describing schema)
//!           | u32 crc32 over every preceding byte of this frame
//! ```
//!
//! Version 1 payload: ONE zstd stream of concatenated data frames with plain
//! IPC buffers, closed by an end frame `u8 frame_type=0` (no body, no crc).
//! A reader decompresses every stream's bytes to reach the one it wants.
//!
//! Version 2 payload, no outer compression:
//!
//! ```text
//! u32 directory_len | directory | u32 crc32(directory)
//! directory: u32 region_count, then per region:
//!     u16 org_len | org | u16 stream_type_len | stream_type
//!   | u16 stream_len | stream | i64 min_ts | i64 max_ts | u64 rows
//!   | u32 frames | u64 offset | u64 len
//! regions: the data frames of one stream, back to back
//! ```
//!
//! Region offsets are relative to the first byte after the directory crc.
//! Regions tile that area in directory order and end exactly at the end of
//! the object. Version 2 IPC bodies use Arrow's per-buffer zstd compression,
//! so a reader skips other streams by offset and decompresses only the
//! columns it reads.
//!
//! Decoding is all-or-nothing: any inconsistency (bad magic/version, crc
//! mismatch, truncation, bad frame type, IPC failure, unknown stream type,
//! row-count mismatch, directory/region mismatch) is a hard error naming the
//! segment and frame index. Segments are small and written atomically —
//! there is no recoverable tail.

use std::{collections::HashMap, sync::Arc};

use anyhow::{Context, anyhow, bail};
use arrow::{
    array::{
        Array, ArrayData, ArrayRef, AsArray, MutableArrayData, RecordBatchOptions, make_array,
    },
    buffer::Buffer,
    datatypes::DataType,
    error::ArrowError,
    ipc::{CompressionType, writer::IpcWriteOptions},
    record_batch::RecordBatch,
};
use config::meta::stream::StreamType;

pub const SEGMENT_MAGIC: &[u8; 4] = b"O2WS";
/// One zstd stream over every frame; plain IPC buffers.
pub const SEGMENT_VERSION_V1: u16 = 1;
/// Per-stream regions behind a raw directory; IPC buffers zstd-compressed
/// one by one.
pub const SEGMENT_VERSION_V2: u16 = 2;

const FRAME_TYPE_END: u8 = 0;
const FRAME_TYPE_DATA: u8 = 1;

#[cfg(test)]
thread_local! {
    static RAW_FRAME_HEADER_READS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[derive(Debug, Clone, PartialEq)]
pub struct SegmentHeader {
    pub node_uuid: String,
    pub seq: u64,
    pub created_at: i64,
}

#[derive(Debug, Clone)]
pub struct SegmentFrame {
    pub org: String,
    pub stream_type: StreamType,
    pub stream: String,
    pub min_ts: i64,
    pub max_ts: i64,
    /// The frame's rows with their WRITE-TIME (narrow) schema. Readers must
    /// group frames by per-batch schema before any concat — never assume one
    /// stream means one schema (the 2026-07-30 mixed-type lesson).
    pub batch: RecordBatch,
}

/// Top-level column filter for a projected decode: `None` reads every
/// column, `Some(keep)` reads the columns whose name `keep` accepts.
pub type ColumnFilter<'a> = Option<&'a dyn Fn(&str) -> bool>;

/// Encode one version-1 segment object (layout in DESIGN-SEGMENT-WAL.md).
pub fn encode_segment(
    header: &SegmentHeader,
    frames: &[SegmentFrame],
) -> Result<Vec<u8>, anyhow::Error> {
    encode_segment_version(header, frames, SEGMENT_VERSION_V1)
}

/// Encode one segment object in format `version` (1 or 2).
pub fn encode_segment_version(
    header: &SegmentHeader,
    frames: &[SegmentFrame],
    version: u16,
) -> Result<Vec<u8>, anyhow::Error> {
    let seg = segment_label(header);
    let uuid = header.node_uuid.as_bytes();
    let uuid_len = u16::try_from(uuid.len())
        .map_err(|_| anyhow!("segment {seg}: node_uuid length {} exceeds u16", uuid.len()))?;

    let mut out = Vec::with_capacity(4 + 2 + 2 + uuid.len() + 8 + 8);
    out.extend_from_slice(SEGMENT_MAGIC);
    out.extend_from_slice(&version.to_le_bytes());
    out.extend_from_slice(&uuid_len.to_le_bytes());
    out.extend_from_slice(uuid);
    out.extend_from_slice(&header.seq.to_le_bytes());
    out.extend_from_slice(&header.created_at.to_le_bytes());

    match version {
        SEGMENT_VERSION_V1 => encode_payload_v1(&mut out, frames, &seg)?,
        SEGMENT_VERSION_V2 => encode_payload_v2(&mut out, frames, &seg)?,
        other => bail!(
            "segment {seg}: cannot encode format version {other} (supported: {SEGMENT_VERSION_V1}, {SEGMENT_VERSION_V2})"
        ),
    }
    Ok(out)
}

fn segment_label(header: &SegmentHeader) -> String {
    format!("{}/{:020}", header.node_uuid, header.seq)
}

fn frame_context(seg: &str, idx: usize, frame: &SegmentFrame) -> String {
    format!(
        "segment {seg}: encode frame {idx} ({}/{}/{})",
        frame.org, frame.stream_type, frame.stream
    )
}

fn encode_payload_v1(
    out: &mut Vec<u8>,
    frames: &[SegmentFrame],
    seg: &str,
) -> Result<(), anyhow::Error> {
    let mut payload = Vec::new();
    for (idx, frame) in frames.iter().enumerate() {
        write_data_frame(&mut payload, frame, None)
            .with_context(|| frame_context(seg, idx, frame))?;
    }
    payload.push(FRAME_TYPE_END);

    let compressed = zstd::encode_all(payload.as_slice(), zstd::DEFAULT_COMPRESSION_LEVEL)
        .with_context(|| format!("segment {seg}: zstd compress failed"))?;
    out.extend_from_slice(&compressed);
    Ok(())
}

/// One stream's frames while a version-2 object is assembled.
struct RegionBuilder<'a> {
    org: &'a str,
    stream_type: StreamType,
    stream: &'a str,
    min_ts: i64,
    max_ts: i64,
    rows: u64,
    frames: u32,
    bytes: Vec<u8>,
}

fn encode_payload_v2(
    out: &mut Vec<u8>,
    frames: &[SegmentFrame],
    seg: &str,
) -> Result<(), anyhow::Error> {
    // Regions follow the first appearance of each stream; frames keep their
    // order inside a region.
    let mut regions: Vec<RegionBuilder<'_>> = Vec::new();
    let mut region_by_stream: HashMap<(&str, StreamType, &str), usize> = HashMap::new();
    for (idx, frame) in frames.iter().enumerate() {
        let key = (frame.org.as_str(), frame.stream_type, frame.stream.as_str());
        let region_idx = *region_by_stream.entry(key).or_insert_with(|| {
            regions.push(RegionBuilder {
                org: key.0,
                stream_type: key.1,
                stream: key.2,
                min_ts: i64::MAX,
                max_ts: i64::MIN,
                rows: 0,
                frames: 0,
                bytes: Vec::new(),
            });
            regions.len() - 1
        });
        let region = &mut regions[region_idx];
        write_data_frame(&mut region.bytes, frame, Some(CompressionType::ZSTD))
            .with_context(|| frame_context(seg, idx, frame))?;
        region.min_ts = region.min_ts.min(frame.min_ts);
        region.max_ts = region.max_ts.max(frame.max_ts);
        region.rows += frame.batch.num_rows() as u64;
        region.frames = region.frames.checked_add(1).ok_or_else(|| {
            anyhow!(
                "segment {seg}: stream {}/{}/{} exceeds u32 frames",
                frame.org,
                frame.stream_type,
                frame.stream
            )
        })?;
    }

    let region_count = u32::try_from(regions.len())
        .map_err(|_| anyhow!("segment {seg}: {} regions exceed u32", regions.len()))?;
    let mut directory = Vec::new();
    directory.extend_from_slice(&region_count.to_le_bytes());
    let mut offset = 0u64;
    for region in &regions {
        let identity = || {
            format!(
                "segment {seg}: directory entry {}/{}/{}",
                region.org, region.stream_type, region.stream
            )
        };
        write_len_prefixed(&mut directory, region.org.as_bytes(), "org").with_context(identity)?;
        let stream_type = region.stream_type.to_string();
        write_len_prefixed(&mut directory, stream_type.as_bytes(), "stream_type")
            .with_context(identity)?;
        write_len_prefixed(&mut directory, region.stream.as_bytes(), "stream")
            .with_context(identity)?;
        let len = region.bytes.len() as u64;
        directory.extend_from_slice(&region.min_ts.to_le_bytes());
        directory.extend_from_slice(&region.max_ts.to_le_bytes());
        directory.extend_from_slice(&region.rows.to_le_bytes());
        directory.extend_from_slice(&region.frames.to_le_bytes());
        directory.extend_from_slice(&offset.to_le_bytes());
        directory.extend_from_slice(&len.to_le_bytes());
        offset += len;
    }
    let directory_len = u32::try_from(directory.len()).map_err(|_| {
        anyhow!(
            "segment {seg}: directory length {} exceeds u32",
            directory.len()
        )
    })?;

    out.reserve(4 + directory.len() + 4 + offset as usize);
    out.extend_from_slice(&directory_len.to_le_bytes());
    out.extend_from_slice(&directory);
    out.extend_from_slice(&crc32fast::hash(&directory).to_le_bytes());
    for region in &regions {
        out.extend_from_slice(&region.bytes);
    }
    Ok(())
}

fn write_data_frame(
    buf: &mut Vec<u8>,
    frame: &SegmentFrame,
    ipc_compression: Option<CompressionType>,
) -> Result<(), anyhow::Error> {
    let start = buf.len();
    buf.push(FRAME_TYPE_DATA);
    write_len_prefixed(buf, frame.org.as_bytes(), "org")?;
    let stream_type = frame.stream_type.to_string();
    write_len_prefixed(buf, stream_type.as_bytes(), "stream_type")?;
    write_len_prefixed(buf, frame.stream.as_bytes(), "stream")?;
    buf.extend_from_slice(&frame.min_ts.to_le_bytes());
    buf.extend_from_slice(&frame.max_ts.to_le_bytes());
    let rows = u32::try_from(frame.batch.num_rows())
        .map_err(|_| anyhow!("row count {} exceeds u32", frame.batch.num_rows()))?;
    buf.extend_from_slice(&rows.to_le_bytes());

    let options = IpcWriteOptions::default()
        .try_with_compression(ipc_compression)
        .context("arrow ipc compression option rejected")?;
    let mut ipc = Vec::new();
    {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new_with_options(
            &mut ipc,
            &frame.batch.schema(),
            options,
        )
        .context("arrow ipc writer open failed")?;
        writer
            .write(&frame.batch)
            .context("arrow ipc write failed")?;
        writer.finish().context("arrow ipc finish failed")?;
    }
    let ipc_len =
        u32::try_from(ipc.len()).map_err(|_| anyhow!("ipc length {} exceeds u32", ipc.len()))?;
    buf.extend_from_slice(&ipc_len.to_le_bytes());
    buf.extend_from_slice(&ipc);

    let crc = crc32fast::hash(&buf[start..]);
    buf.extend_from_slice(&crc.to_le_bytes());
    Ok(())
}

fn write_len_prefixed(buf: &mut Vec<u8>, bytes: &[u8], what: &str) -> Result<(), anyhow::Error> {
    let len = u16::try_from(bytes.len())
        .map_err(|_| anyhow!("{what} length {} exceeds u16", bytes.len()))?;
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(bytes);
    Ok(())
}

/// Decode a segment object. Unknown version, crc mismatch, or truncation is
/// a hard error naming the failure — segments are atomic, there is no
/// recoverable tail.
pub fn decode_segment(bytes: &[u8]) -> Result<(SegmentHeader, Vec<SegmentFrame>), anyhow::Error> {
    let mut frames = Vec::new();
    let header = decode_segment_filtered(
        bytes,
        |_| true,
        |frame| {
            frames.push(frame);
            Ok(())
        },
    )?;
    Ok((header, frames))
}

/// A data frame's identity and bounds, decoded BEFORE its IPC body is
/// interpreted — the filter input of [`decode_segment_filtered`].
#[derive(Debug, Clone)]
pub struct FrameInfo {
    pub org: String,
    pub stream_type: StreamType,
    pub stream: String,
    pub min_ts: i64,
    pub max_ts: i64,
    pub rows: u32,
}

/// Streaming decode of every column: each data frame's identity goes to
/// `want`; only when it returns true is the frame's IPC body parsed into a
/// batch and handed to `on_frame`. Peak memory ≈ the largest single frame
/// (+ the zstd window for version 1) instead of the whole decompressed
/// payload.
///
/// See [`decode_segment_projected`] for the integrity rules per version.
pub fn decode_segment_filtered(
    bytes: &[u8],
    want: impl FnMut(&FrameInfo) -> bool,
    on_frame: impl FnMut(SegmentFrame) -> Result<(), anyhow::Error>,
) -> Result<SegmentHeader, anyhow::Error> {
    decode_segment_projected(bytes, None, want, on_frame)
}

/// [`decode_segment_filtered`] with a column projection: wanted frames are
/// IPC-decoded with only the top-level columns `columns` accepts (`None`
/// reads every column). A projected batch never pins the bytes of the
/// columns it dropped (see [`detach_pinned_columns`]).
///
/// Integrity: version 1 verifies the crc32 of EVERY frame, because the zstd
/// stream must be decompressed through anyway. Version 2 verifies the
/// directory crc32 and the crc32 of every WANTED frame. Its unwanted frames
/// are bounds-checked from their raw headers but never hashed: a reader pays
/// only for the streams it reads, which is the purpose of the format.
pub fn decode_segment_projected(
    bytes: &[u8],
    columns: ColumnFilter<'_>,
    want: impl FnMut(&FrameInfo) -> bool,
    on_frame: impl FnMut(SegmentFrame) -> Result<(), anyhow::Error>,
) -> Result<SegmentHeader, anyhow::Error> {
    decode_segment_projected_regions(bytes, columns, None, want, on_frame)
}

/// Optional conservative directory predicate. Return false only if no frame
/// in the region is relevant, including any frame-filter side effects.
pub type RegionFilter<'a> = Option<&'a mut dyn FnMut(&RegionInfo) -> Result<bool, anyhow::Error>>;

/// Projected query decode with an independent version-2 directory predicate.
/// The directory CRC and all region extents are always checked. Rejected
/// regions are not read: frame headers, counts, identities, bounds and CRCs
/// are not validated there. Use [`decode_segment`] for full validation.
/// Version 1 never calls `regions` and retains its original validation.
pub fn decode_segment_projected_regions(
    bytes: &[u8],
    columns: ColumnFilter<'_>,
    regions: RegionFilter<'_>,
    mut want: impl FnMut(&FrameInfo) -> bool,
    mut on_frame: impl FnMut(SegmentFrame) -> Result<(), anyhow::Error>,
) -> Result<SegmentHeader, anyhow::Error> {
    let mut r = Reader::new(bytes);
    let (version, header) = decode_header(&mut r)?;
    let seg = segment_label(&header);
    let payload = &bytes[r.pos..];
    match version {
        SEGMENT_VERSION_V1 => decode_payload_v1(payload, &seg, columns, &mut want, &mut on_frame)?,
        SEGMENT_VERSION_V2 => {
            decode_payload_v2(payload, &seg, columns, regions, &mut want, &mut on_frame)?
        }
        other => unreachable!("decode_header admitted unsupported version {other}"),
    }
    Ok(header)
}

fn decode_header(r: &mut Reader<'_>) -> Result<(u16, SegmentHeader), anyhow::Error> {
    let magic = r
        .take(4, "magic")
        .map_err(|e| anyhow!("segment decode: {e}"))?;
    if magic != SEGMENT_MAGIC {
        bail!(
            "segment decode: bad magic {:02x?}, want {:02x?} (\"O2WS\")",
            magic,
            SEGMENT_MAGIC
        );
    }
    let version = r
        .read_u16("version")
        .map_err(|e| anyhow!("segment decode: {e}"))?;
    if version != SEGMENT_VERSION_V1 && version != SEGMENT_VERSION_V2 {
        bail!(
            "segment decode: unsupported version {version}, this build reads versions {SEGMENT_VERSION_V1} and {SEGMENT_VERSION_V2}"
        );
    }
    let uuid_len = r
        .read_u16("node_uuid length")
        .map_err(|e| anyhow!("segment decode: {e}"))? as usize;
    let uuid_bytes = r
        .take(uuid_len, "node_uuid")
        .map_err(|e| anyhow!("segment decode: {e}"))?;
    let node_uuid = std::str::from_utf8(uuid_bytes)
        .map_err(|e| anyhow!("segment decode: node_uuid is not utf-8: {e}"))?
        .to_string();
    let seq = r
        .read_u64("seq")
        .map_err(|e| anyhow!("segment decode (node {node_uuid}): {e}"))?;
    let created_at = r
        .read_i64("created_at_micros")
        .map_err(|e| anyhow!("segment decode (node {node_uuid}): {e}"))?;
    Ok((
        version,
        SegmentHeader {
            node_uuid,
            seq,
            created_at,
        },
    ))
}

fn decode_payload_v1(
    payload: &[u8],
    seg: &str,
    columns: ColumnFilter<'_>,
    want: &mut impl FnMut(&FrameInfo) -> bool,
    on_frame: &mut impl FnMut(SegmentFrame) -> Result<(), anyhow::Error>,
) -> Result<(), anyhow::Error> {
    let decoder = zstd::stream::read::Decoder::new(payload)
        .map_err(|e| anyhow!("segment {seg}: zstd decoder open failed: {e}"))?;
    let mut fr = FrameStream::new(decoder);
    let mut idx = 0usize;
    loop {
        fr.begin_frame();
        let frame_type = match fr
            .read_u8_or_eof("frame type")
            .map_err(|e| anyhow!("segment {seg}: frame {idx}: {e}"))?
        {
            Some(t) => t,
            None => bail!("segment {seg}: frame {idx}: truncated before end frame"),
        };
        match frame_type {
            FRAME_TYPE_END => {
                if !fr
                    .at_eof()
                    .map_err(|e| anyhow!("segment {seg}: frame {idx}: {e}"))?
                {
                    bail!("segment {seg}: trailing bytes after end frame (frame {idx})");
                }
                return Ok(());
            }
            FRAME_TYPE_DATA => {
                decode_data_frame_streamed(&mut fr, columns, want, on_frame)
                    .map_err(|e| anyhow!("segment {seg}: frame {idx}: {e:#}"))?;
                idx += 1;
            }
            other => {
                bail!("segment {seg}: frame {idx}: unknown frame type {other} (want 1=data, 0=end)")
            }
        }
    }
}

/// Sanity cap on a single frame's IPC body; the writer cuts segments far
/// below this, so anything bigger is corruption, not data.
const MAX_FRAME_IPC_LEN: usize = 1 << 31;

fn decode_data_frame_streamed(
    fr: &mut FrameStream<impl std::io::Read>,
    columns: ColumnFilter<'_>,
    want: &mut impl FnMut(&FrameInfo) -> bool,
    on_frame: &mut impl FnMut(SegmentFrame) -> Result<(), anyhow::Error>,
) -> Result<(), anyhow::Error> {
    let org_len = fr.read_u16("org length")? as usize;
    let org = fr.read_str(org_len, "org")?;
    let stream_type_len = fr.read_u16("stream_type length")? as usize;
    let stream_type_str = fr.read_str(stream_type_len, "stream_type")?;
    let stream_len = fr.read_u16("stream length")? as usize;
    let stream = fr.read_str(stream_len, "stream")?;
    let min_ts = fr.read_i64("min_ts")?;
    let max_ts = fr.read_i64("max_ts")?;
    let rows = fr.read_u32("rows")?;
    let ipc_len = fr.read_u32("ipc length")? as usize;
    if ipc_len > MAX_FRAME_IPC_LEN {
        bail!("ipc length {ipc_len} exceeds the {MAX_FRAME_IPC_LEN} frame cap (corrupt length)");
    }
    // NOT From<&str>: that silently falls back to Logs on unknown input,
    // which would misfile another stream type's rows.
    let stream_type = parse_stream_type(&stream_type_str)
        .ok_or_else(|| anyhow!("unknown stream type {stream_type_str:?}"))?;
    let info = FrameInfo {
        org,
        stream_type,
        stream,
        min_ts,
        max_ts,
        rows,
    };
    let wanted = want(&info);

    // the IPC body streams into the frame buffer either way — the crc32
    // guards every frame, wanted or not
    let ipc_range = fr.read_exact_buffered(ipc_len, "ipc payload")?;
    let crc_stored = fr.read_u32_unbuffered("crc32")?;
    let crc_computed = crc32fast::hash(fr.frame_bytes());
    if crc_stored != crc_computed {
        bail!("crc32 mismatch: stored {crc_stored:#010x}, computed {crc_computed:#010x}");
    }
    if !wanted {
        return Ok(());
    }
    deliver_frame(info, &fr.buf()[ipc_range], columns, on_frame)
}

/// One version-2 directory entry, bounds-checked against the region area.
#[derive(Debug)]
pub struct RegionInfo {
    pub org: String,
    pub stream_type: StreamType,
    pub stream: String,
    pub min_ts: i64,
    pub max_ts: i64,
    pub rows: u64,
    pub frames: u32,
    offset: usize,
    len: usize,
}

fn decode_payload_v2(
    payload: &[u8],
    seg: &str,
    columns: ColumnFilter<'_>,
    mut region_filter: RegionFilter<'_>,
    want: &mut impl FnMut(&FrameInfo) -> bool,
    on_frame: &mut impl FnMut(SegmentFrame) -> Result<(), anyhow::Error>,
) -> Result<(), anyhow::Error> {
    let mut r = Reader::new(payload);
    let directory_len = r
        .read_u32("directory length")
        .map_err(|e| anyhow!("segment {seg}: {e}"))? as usize;
    let directory = r
        .take(directory_len, "directory")
        .map_err(|e| anyhow!("segment {seg}: {e}"))?;
    let crc_stored = r
        .read_u32("directory crc32")
        .map_err(|e| anyhow!("segment {seg}: {e}"))?;
    let crc_computed = crc32fast::hash(directory);
    if crc_stored != crc_computed {
        bail!(
            "segment {seg}: directory crc32 mismatch: stored {crc_stored:#010x}, computed {crc_computed:#010x}"
        );
    }
    let area = &payload[r.pos..];
    let regions =
        parse_directory(directory, area.len()).map_err(|e| anyhow!("segment {seg}: {e:#}"))?;

    let mut idx = 0usize;
    for (region_idx, region) in regions.iter().enumerate() {
        let identity = || {
            format!(
                "region {region_idx} ({}/{}/{})",
                region.org, region.stream_type, region.stream
            )
        };
        if let Some(keep) = region_filter.as_mut() {
            config::metrics::SEGMENT_SCAN_REGIONS_TOTAL.inc();
            if !keep(region)
                .with_context(|| format!("segment {seg}: {}: region filter failed", identity()))?
            {
                config::metrics::SEGMENT_SCAN_REGIONS_SKIPPED_TOTAL.inc();
                idx = idx
                    .checked_add(region.frames as usize)
                    .ok_or_else(|| anyhow!("segment {seg}: frame index overflow"))?;
                continue;
            }
        }
        let mut fr = Reader::new(&area[region.offset..region.offset + region.len]);
        let mut frames = 0u32;
        let mut rows = 0u64;
        while fr.remaining() > 0 {
            let frame_rows = decode_data_frame_raw(&mut fr, region, columns, want, on_frame)
                .map_err(|e| anyhow!("segment {seg}: {}: frame {idx}: {e:#}", identity()))?;
            frames = frames.saturating_add(1);
            rows += u64::from(frame_rows);
            idx += 1;
        }
        if frames != region.frames || rows != region.rows {
            bail!(
                "segment {seg}: {}: directory declares {} frames and {} rows, the region holds {frames} frames and {rows} rows",
                identity(),
                region.frames,
                region.rows
            );
        }
    }
    Ok(())
}

fn parse_directory(directory: &[u8], area_len: usize) -> Result<Vec<RegionInfo>, anyhow::Error> {
    // smallest entry: three empty strings + the fixed fields
    const MIN_ENTRY_LEN: usize = 3 * 2 + 8 + 8 + 8 + 4 + 8 + 8;
    let mut r = Reader::new(directory);
    let count = r.read_u32("directory region count")? as usize;
    let mut regions = Vec::with_capacity(count.min(directory.len() / MIN_ENTRY_LEN));
    let mut expected_offset = 0usize;
    for i in 0..count {
        let entry = |e: anyhow::Error| anyhow!("directory entry {i}: {e:#}");
        let org_len = r.read_u16("org length").map_err(entry)? as usize;
        let org = r.read_str(org_len, "org").map_err(entry)?.to_string();
        let stream_type_len = r.read_u16("stream_type length").map_err(entry)? as usize;
        let stream_type_str = r.read_str(stream_type_len, "stream_type").map_err(entry)?;
        let stream_type = parse_stream_type(stream_type_str).ok_or_else(|| {
            anyhow!("directory entry {i}: unknown stream type {stream_type_str:?}")
        })?;
        let stream_len = r.read_u16("stream length").map_err(entry)? as usize;
        let stream = r.read_str(stream_len, "stream").map_err(entry)?.to_string();
        let min_ts = r.read_i64("min_ts").map_err(entry)?;
        let max_ts = r.read_i64("max_ts").map_err(entry)?;
        let rows = r.read_u64("rows").map_err(entry)?;
        let frames = r.read_u32("frames").map_err(entry)?;
        let offset = r.read_u64("offset").map_err(entry)?;
        let len = r.read_u64("len").map_err(entry)?;
        let (Ok(offset), Ok(len)) = (usize::try_from(offset), usize::try_from(len)) else {
            bail!("directory entry {i}: extent {offset}+{len} does not fit in memory");
        };
        if offset != expected_offset {
            bail!(
                "directory entry {i}: region offset {offset}, expected {expected_offset} (regions must be contiguous and in directory order)"
            );
        }
        let end = offset
            .checked_add(len)
            .filter(|end| *end <= area_len)
            .ok_or_else(|| {
                anyhow!(
                    "directory entry {i}: truncated region: extent {offset}+{len} exceeds the {area_len}-byte region area"
                )
            })?;
        expected_offset = end;
        regions.push(RegionInfo {
            org,
            stream_type,
            stream,
            min_ts,
            max_ts,
            rows,
            frames,
            offset,
            len,
        });
    }
    if r.remaining() != 0 {
        bail!(
            "directory has {} trailing bytes after {count} entries",
            r.remaining()
        );
    }
    if expected_offset != area_len {
        bail!(
            "{} trailing bytes after the last region",
            area_len - expected_offset
        );
    }
    Ok(regions)
}

/// Decode one raw (uncompressed) version-2 data frame. Returns the row count
/// its header declares, so the caller can check the directory totals.
fn decode_data_frame_raw(
    r: &mut Reader<'_>,
    region: &RegionInfo,
    columns: ColumnFilter<'_>,
    want: &mut impl FnMut(&FrameInfo) -> bool,
    on_frame: &mut impl FnMut(SegmentFrame) -> Result<(), anyhow::Error>,
) -> Result<u32, anyhow::Error> {
    #[cfg(test)]
    RAW_FRAME_HEADER_READS.set(RAW_FRAME_HEADER_READS.get() + 1);
    let start = r.pos;
    let frame_type = r.read_u8("frame type")?;
    if frame_type != FRAME_TYPE_DATA {
        bail!("unknown frame type {frame_type} (version 2 regions hold only data frames)");
    }
    let org_len = r.read_u16("org length")? as usize;
    let org = r.read_str(org_len, "org")?;
    let stream_type_len = r.read_u16("stream_type length")? as usize;
    let stream_type_str = r.read_str(stream_type_len, "stream_type")?;
    let stream_len = r.read_u16("stream length")? as usize;
    let stream = r.read_str(stream_len, "stream")?;
    let min_ts = r.read_i64("min_ts")?;
    let max_ts = r.read_i64("max_ts")?;
    let rows = r.read_u32("rows")?;
    let ipc_len = r.read_u32("ipc length")? as usize;
    if ipc_len > MAX_FRAME_IPC_LEN {
        bail!("ipc length {ipc_len} exceeds the {MAX_FRAME_IPC_LEN} frame cap (corrupt length)");
    }
    let ipc = r.take(ipc_len, "ipc payload")?;
    let hashed = &r.buf[start..r.pos];
    let crc_stored = r.read_u32("crc32")?;

    let stream_type = parse_stream_type(stream_type_str)
        .ok_or_else(|| anyhow!("unknown stream type {stream_type_str:?}"))?;
    if org != region.org || stream_type != region.stream_type || stream != region.stream {
        bail!("frame identity {org}/{stream_type}/{stream} does not match its region");
    }
    if min_ts < region.min_ts || max_ts > region.max_ts {
        bail!(
            "frame bounds [{min_ts}, {max_ts}] fall outside the region bounds [{}, {}]",
            region.min_ts,
            region.max_ts
        );
    }
    let info = FrameInfo {
        org: org.to_string(),
        stream_type,
        stream: stream.to_string(),
        min_ts,
        max_ts,
        rows,
    };
    if !want(&info) {
        return Ok(rows);
    }
    let crc_computed = crc32fast::hash(hashed);
    if crc_stored != crc_computed {
        bail!("crc32 mismatch: stored {crc_stored:#010x}, computed {crc_computed:#010x}");
    }
    deliver_frame(info, ipc, columns, on_frame)?;
    Ok(rows)
}

/// IPC-parse one wanted frame, check its declared row count, and hand it on.
fn deliver_frame(
    info: FrameInfo,
    ipc_bytes: &[u8],
    columns: ColumnFilter<'_>,
    on_frame: &mut impl FnMut(SegmentFrame) -> Result<(), anyhow::Error>,
) -> Result<(), anyhow::Error> {
    let batch = parse_frame_batch(&info, ipc_bytes, columns)?;
    if batch.num_rows() as u64 != u64::from(info.rows) {
        bail!(
            "stream {}/{}/{}: row count mismatch: frame declares {} rows, ipc decoded {}",
            info.org,
            info.stream_type,
            info.stream,
            info.rows,
            batch.num_rows()
        );
    }
    on_frame(SegmentFrame {
        org: info.org,
        stream_type: info.stream_type,
        stream: info.stream,
        min_ts: info.min_ts,
        max_ts: info.max_ts,
        batch,
    })
}

/// Pull-reader over the decompressing stream that keeps the CURRENT frame's
/// bytes in a reusable buffer for the trailing crc32 check. `begin_frame`
/// resets the buffer; the stored crc itself is read UNbuffered (it is not
/// covered by the hash).
struct FrameStream<R: std::io::Read> {
    src: R,
    buf: Vec<u8>,
}

impl<R: std::io::Read> FrameStream<R> {
    fn new(src: R) -> Self {
        Self {
            src,
            buf: Vec::new(),
        }
    }

    fn begin_frame(&mut self) {
        self.buf.clear();
    }

    fn buf(&self) -> &[u8] {
        &self.buf
    }

    /// Every byte of the current frame read so far (type tag through ipc).
    fn frame_bytes(&self) -> &[u8] {
        &self.buf
    }

    fn read_exact_buffered(
        &mut self,
        n: usize,
        what: &str,
    ) -> Result<std::ops::Range<usize>, anyhow::Error> {
        let start = self.buf.len();
        self.buf.resize(start + n, 0);
        self.src
            .read_exact(&mut self.buf[start..])
            .map_err(|e| anyhow!("truncated reading {what}: {e}"))?;
        Ok(start..start + n)
    }

    fn read_array_buffered<const N: usize>(
        &mut self,
        what: &str,
    ) -> Result<[u8; N], anyhow::Error> {
        let range = self.read_exact_buffered(N, what)?;
        let mut out = [0u8; N];
        out.copy_from_slice(&self.buf[range]);
        Ok(out)
    }

    /// Reads one byte into the frame buffer; clean EOF before it is `None`.
    fn read_u8_or_eof(&mut self, what: &str) -> Result<Option<u8>, anyhow::Error> {
        let mut byte = [0u8; 1];
        let mut read = 0usize;
        while read < 1 {
            match self.src.read(&mut byte[read..]) {
                Ok(0) if read == 0 => return Ok(None),
                Ok(0) => bail!("truncated reading {what}: unexpected eof"),
                Ok(n) => read += n,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => bail!("truncated reading {what}: {e}"),
            }
        }
        self.buf.push(byte[0]);
        Ok(Some(byte[0]))
    }

    fn read_u16(&mut self, what: &str) -> Result<u16, anyhow::Error> {
        Ok(u16::from_le_bytes(self.read_array_buffered(what)?))
    }

    fn read_u32(&mut self, what: &str) -> Result<u32, anyhow::Error> {
        Ok(u32::from_le_bytes(self.read_array_buffered(what)?))
    }

    fn read_i64(&mut self, what: &str) -> Result<i64, anyhow::Error> {
        Ok(i64::from_le_bytes(self.read_array_buffered(what)?))
    }

    fn read_str(&mut self, n: usize, what: &str) -> Result<String, anyhow::Error> {
        let range = self.read_exact_buffered(n, what)?;
        std::str::from_utf8(&self.buf[range])
            .map(|s| s.to_string())
            .map_err(|e| anyhow!("{what} is not utf-8: {e}"))
    }

    /// The stored crc is NOT part of the hashed bytes: read it outside the
    /// frame buffer.
    fn read_u32_unbuffered(&mut self, what: &str) -> Result<u32, anyhow::Error> {
        let mut out = [0u8; 4];
        self.src
            .read_exact(&mut out)
            .map_err(|e| anyhow!("truncated reading {what}: {e}"))?;
        Ok(u32::from_le_bytes(out))
    }

    /// True when the source is exhausted (the end frame must be last).
    fn at_eof(&mut self) -> Result<bool, anyhow::Error> {
        let mut probe = [0u8; 1];
        loop {
            match self.src.read(&mut probe) {
                Ok(0) => return Ok(true),
                Ok(_) => return Ok(false),
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => bail!("probing for trailing bytes: {e}"),
            }
        }
    }
}

/// IPC-parse one wanted frame's body (identity in `info` for error text).
/// Under a projection only the accepted top-level columns are decoded.
fn parse_frame_batch(
    info: &FrameInfo,
    ipc_bytes: &[u8],
    columns: ColumnFilter<'_>,
) -> Result<RecordBatch, anyhow::Error> {
    let (org, stream_type, stream) = (&info.org, info.stream_type, &info.stream);
    let projection = match columns {
        None => None,
        Some(keep) => {
            let names = ipc_schema_field_names(ipc_bytes).with_context(|| {
                format!("stream {org}/{stream_type}/{stream}: arrow ipc schema read failed")
            })?;
            let total = names.len();
            let projection: Vec<usize> = names
                .into_iter()
                .enumerate()
                .filter(|(_, name)| keep(name))
                .map(|(i, _)| i)
                .collect();
            // a projection that keeps every column is a full decode
            (projection.len() < total).then_some(projection)
        }
    };
    let projected = projection.is_some();
    let reader =
        arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(ipc_bytes), projection)
            .with_context(|| {
                format!("stream {org}/{stream_type}/{stream}: arrow ipc open failed")
            })?;
    let schema = reader.schema();
    let mut batches = reader
        .collect::<Result<Vec<_>, _>>()
        .with_context(|| format!("stream {org}/{stream_type}/{stream}: arrow ipc decode failed"))?;
    // the encoder writes exactly one batch per frame, but merge defensively;
    // batches from one ipc stream share one schema, so concat is safe here
    let batch = match batches.len() {
        0 => RecordBatch::new_empty(schema),
        1 => batches.swap_remove(0),
        _ => arrow::compute::concat_batches(&schema, batches.iter()).with_context(|| {
            format!("stream {org}/{stream_type}/{stream}: arrow ipc concat failed")
        })?,
    };
    if !projected {
        return Ok(batch);
    }
    detach_pinned_columns(batch).with_context(|| {
        format!("stream {org}/{stream_type}/{stream}: copying projected columns failed")
    })
}

/// Top-level field names of an IPC stream's leading schema message, read
/// from the flatbuffer without converting the schema: the projection must
/// be known before `StreamReader` opens the stream.
fn ipc_schema_field_names(ipc: &[u8]) -> Result<Vec<&str>, anyhow::Error> {
    // Arrow >= 0.15 prefixes each message with a 0xFFFFFFFF continuation
    // marker; the legacy framing starts directly with the length.
    let (meta_start, meta_len) = match ipc {
        [0xFF, 0xFF, 0xFF, 0xFF, a, b, c, d, ..] => (8usize, i32::from_le_bytes([*a, *b, *c, *d])),
        [a, b, c, d, ..] => (4usize, i32::from_le_bytes([*a, *b, *c, *d])),
        _ => bail!(
            "ipc stream of {} bytes is shorter than a message prefix",
            ipc.len()
        ),
    };
    let meta_len = usize::try_from(meta_len)
        .map_err(|_| anyhow!("negative schema message length {meta_len}"))?;
    let meta = meta_start
        .checked_add(meta_len)
        .and_then(|end| ipc.get(meta_start..end))
        .ok_or_else(|| {
            anyhow!(
                "schema message length {meta_len} exceeds the {}-byte ipc stream",
                ipc.len()
            )
        })?;
    let message = arrow::ipc::root_as_message(meta)
        .map_err(|e| anyhow!("schema message is not a valid flatbuffer: {e}"))?;
    let schema = message.header_as_schema().ok_or_else(|| {
        anyhow!(
            "first ipc message is {:?}, not a schema",
            message.header_type()
        )
    })?;
    let Some(fields) = schema.fields() else {
        return Ok(Vec::new());
    };
    fields
        .iter()
        .enumerate()
        .map(|(i, field)| {
            field
                .name()
                .ok_or_else(|| anyhow!("schema field {i} has no name"))
        })
        .collect()
}

/// A column may reference at most this many bytes beyond its own slices
/// before a projected decode copies it out.
const PINNED_SLACK_BYTES: usize = 64 * 1024;

/// IPC decode returns zero-copy slices of the message body: every buffer of
/// a version-1 frame, and every buffer of a version-2 frame that Arrow
/// stored raw because compression did not shrink it. A projected batch kept
/// by a query would then pin the body bytes of every column it dropped, so
/// copy each column that references a much larger allocation than it uses.
fn detach_pinned_columns(batch: RecordBatch) -> Result<RecordBatch, ArrowError> {
    let data: Vec<ArrayData> = batch.columns().iter().map(|c| c.to_data()).collect();
    if !data.iter().any(pins_larger_allocation) {
        return Ok(batch);
    }
    let columns = batch
        .columns()
        .iter()
        .zip(&data)
        .map(|(column, data)| {
            if pins_larger_allocation(data) {
                deep_copy_array(data)
            } else {
                Ok(Arc::clone(column))
            }
        })
        .collect::<Result<Vec<_>, ArrowError>>()?;
    RecordBatch::try_new_with_options(
        batch.schema(),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
}

fn pins_larger_allocation(data: &ArrayData) -> bool {
    let pins =
        |buffer: &Buffer| buffer.capacity() > buffer.len().saturating_add(PINNED_SLACK_BYTES);
    data.buffers().iter().any(pins)
        || data.nulls().is_some_and(|nulls| pins(nulls.buffer()))
        || data.child_data().iter().any(pins_larger_allocation)
}

fn deep_copy_array(data: &ArrayData) -> Result<ArrayRef, ArrowError> {
    let mut copy = MutableArrayData::new(vec![data], false, data.len());
    copy.try_extend(0, 0, data.len())?;
    let array = make_array(copy.freeze());
    // MutableArrayData shares the variadic payload buffers of view arrays;
    // gc() rewrites them into owned, exactly sized buffers.
    Ok(match array.data_type() {
        DataType::Utf8View => Arc::new(array.as_string_view().gc()),
        DataType::BinaryView => Arc::new(array.as_binary_view().gc()),
        _ => array,
    })
}

/// Exact inverse of `StreamType`'s `Display` impl. Returns None on unknown
/// input instead of the `From<&str>` default-to-Logs fallback.
fn parse_stream_type(s: &str) -> Option<StreamType> {
    match s {
        "logs" => Some(StreamType::Logs),
        "metrics" => Some(StreamType::Metrics),
        "traces" => Some(StreamType::Traces),
        "service_graph" => Some(StreamType::ServiceGraph),
        "enrichment_tables" => Some(StreamType::EnrichmentTables),
        "file_list" => Some(StreamType::Filelist),
        "metadata" => Some(StreamType::Metadata),
        "index" => Some(StreamType::Index),
        _ => None,
    }
}

/// Bounds-checked little-endian reader; every failure names the field and
/// offset instead of panicking on short input.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn take(&mut self, n: usize, what: &str) -> Result<&'a [u8], anyhow::Error> {
        if self.remaining() < n {
            bail!(
                "truncated reading {what}: need {n} bytes at offset {}, only {} remain",
                self.pos,
                self.remaining()
            );
        }
        let out = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }

    fn read_array<const N: usize>(&mut self, what: &str) -> Result<[u8; N], anyhow::Error> {
        let mut out = [0u8; N];
        out.copy_from_slice(self.take(N, what)?);
        Ok(out)
    }

    fn read_u8(&mut self, what: &str) -> Result<u8, anyhow::Error> {
        Ok(self.read_array::<1>(what)?[0])
    }

    fn read_u16(&mut self, what: &str) -> Result<u16, anyhow::Error> {
        Ok(u16::from_le_bytes(self.read_array(what)?))
    }

    fn read_u32(&mut self, what: &str) -> Result<u32, anyhow::Error> {
        Ok(u32::from_le_bytes(self.read_array(what)?))
    }

    fn read_u64(&mut self, what: &str) -> Result<u64, anyhow::Error> {
        Ok(u64::from_le_bytes(self.read_array(what)?))
    }

    fn read_i64(&mut self, what: &str) -> Result<i64, anyhow::Error> {
        Ok(i64::from_le_bytes(self.read_array(what)?))
    }

    fn read_str(&mut self, n: usize, what: &str) -> Result<&'a str, anyhow::Error> {
        std::str::from_utf8(self.take(n, what)?).map_err(|e| anyhow!("{what} is not utf-8: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn header() -> SegmentHeader {
        SegmentHeader {
            node_uuid: "7f9c24e5-1a2b-4c3d-8e9f-000000000001".to_string(),
            seq: 42,
            created_at: 1_722_400_000_000_000,
        }
    }

    fn batch_i64(field: &str, values: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(field, DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values.to_vec()))]).unwrap()
    }

    fn batch_utf8(field: &str, values: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(field, DataType::Utf8, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values.to_vec()))]).unwrap()
    }

    fn frame(
        org: &str,
        stream_type: StreamType,
        stream: &str,
        min_ts: i64,
        max_ts: i64,
        batch: RecordBatch,
    ) -> SegmentFrame {
        SegmentFrame {
            org: org.to_string(),
            stream_type,
            stream: stream.to_string(),
            min_ts,
            max_ts,
            batch,
        }
    }

    fn header_len(h: &SegmentHeader) -> usize {
        4 + 2 + 2 + h.node_uuid.len() + 8 + 8
    }

    /// Split an encoded segment into (raw header bytes, DECOMPRESSED payload).
    fn split(encoded: &[u8], h: &SegmentHeader) -> (Vec<u8>, Vec<u8>) {
        let hlen = header_len(h);
        let plain = zstd::decode_all(&encoded[hlen..]).unwrap();
        (encoded[..hlen].to_vec(), plain)
    }

    fn reassemble(header_bytes: &[u8], plain_payload: &[u8]) -> Vec<u8> {
        let mut out = header_bytes.to_vec();
        out.extend_from_slice(
            &zstd::encode_all(plain_payload, zstd::DEFAULT_COMPRESSION_LEVEL).unwrap(),
        );
        out
    }

    /// Byte offset of frame 0's stored crc32 within the decompressed payload.
    fn frame0_crc_offset(plain: &[u8], f: &SegmentFrame) -> usize {
        let mut off = 1; // frame type
        off += 2 + f.org.len();
        off += 2 + f.stream_type.to_string().len();
        off += 2 + f.stream.len();
        off += 8 + 8 + 4; // min_ts, max_ts, rows
        let ipc_len = u32::from_le_bytes(plain[off..off + 4].try_into().unwrap()) as usize;
        off + 4 + ipc_len
    }

    #[test]
    fn round_trip_multiple_streams_and_type_flipped_schemas() {
        let h = header();
        // two frames of the SAME stream (org1/logs/app1) whose schemas share
        // field name "value" with DIFFERENT types — both must come back with
        // their own write-time schema
        let frames = vec![
            frame(
                "org1",
                StreamType::Logs,
                "app1",
                100,
                300,
                batch_i64("value", &[1, 2, 3]),
            ),
            frame(
                "org1",
                StreamType::Traces,
                "spans",
                50,
                60,
                batch_utf8("name", &["a", "b"]),
            ),
            frame(
                "org1",
                StreamType::Logs,
                "app1",
                400,
                500,
                batch_utf8("value", &["x", "y", "z", "w"]),
            ),
            frame(
                "org2",
                StreamType::Metrics,
                "cpu",
                -10,
                10,
                batch_i64("gauge", &[7]),
            ),
        ];
        let encoded = encode_segment(&h, &frames).unwrap();
        let (decoded_header, decoded) = decode_segment(&encoded).unwrap();
        assert_eq!(decoded_header, h);
        assert_eq!(decoded.len(), frames.len());
        for (got, want) in decoded.iter().zip(frames.iter()) {
            assert_eq!(got.org, want.org);
            assert_eq!(got.stream_type, want.stream_type);
            assert_eq!(got.stream, want.stream);
            assert_eq!(got.min_ts, want.min_ts);
            assert_eq!(got.max_ts, want.max_ts);
            assert_eq!(got.batch, want.batch);
        }
        // the type-flipped pair kept their own schemas
        assert_eq!(
            decoded[0].batch.schema().field(0).data_type(),
            &DataType::Int64
        );
        assert_eq!(
            decoded[2].batch.schema().field(0).data_type(),
            &DataType::Utf8
        );
        assert_eq!(decoded[0].batch.schema().field(0).name(), "value");
        assert_eq!(decoded[2].batch.schema().field(0).name(), "value");
    }

    #[test]
    fn empty_frames_round_trip() {
        let h = header();
        let encoded = encode_segment(&h, &[]).unwrap();
        let (decoded_header, decoded) = decode_segment(&encoded).unwrap();
        assert_eq!(decoded_header, h);
        assert!(decoded.is_empty());
    }

    #[test]
    fn corrupt_crc_byte_names_frame() {
        let h = header();
        let f = frame(
            "org1",
            StreamType::Logs,
            "app1",
            1,
            2,
            batch_i64("v", &[1, 2]),
        );
        let encoded = encode_segment(&h, std::slice::from_ref(&f)).unwrap();
        let (hdr, mut plain) = split(&encoded, &h);
        let crc_off = frame0_crc_offset(&plain, &f);
        plain[crc_off] ^= 0xFF;
        let err = decode_segment(&reassemble(&hdr, &plain)).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("crc32 mismatch"), "unexpected error: {msg}");
        assert!(msg.contains("frame 0"), "unexpected error: {msg}");
        assert!(msg.contains(&h.node_uuid), "unexpected error: {msg}");
    }

    #[test]
    fn corrupt_data_byte_is_caught_by_crc() {
        let h = header();
        let f = frame(
            "org1",
            StreamType::Logs,
            "app1",
            1,
            2,
            batch_i64("v", &[1, 2]),
        );
        let encoded = encode_segment(&h, std::slice::from_ref(&f)).unwrap();
        let (hdr, mut plain) = split(&encoded, &h);
        // flip a byte inside min_ts (crc-guarded, not a length field)
        let ts_off = 1 + 2 + f.org.len() + 2 + 4 /* "logs" */ + 2 + f.stream.len();
        plain[ts_off] ^= 0xFF;
        let err = decode_segment(&reassemble(&hdr, &plain)).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("crc32 mismatch"), "unexpected error: {msg}");
        assert!(msg.contains("frame 0"), "unexpected error: {msg}");
    }

    #[test]
    fn truncation_mid_frame_names_frame() {
        let h = header();
        let f = frame(
            "org1",
            StreamType::Logs,
            "app1",
            1,
            2,
            batch_i64("v", &[1, 2, 3]),
        );
        let encoded = encode_segment(&h, std::slice::from_ref(&f)).unwrap();
        let (hdr, plain) = split(&encoded, &h);
        // cut inside frame 0's org field
        let err = decode_segment(&reassemble(&hdr, &plain[..4])).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("truncated"), "unexpected error: {msg}");
        assert!(msg.contains("frame 0"), "unexpected error: {msg}");
    }

    #[test]
    fn missing_end_frame_is_truncation() {
        let h = header();
        let f = frame("org1", StreamType::Logs, "app1", 1, 2, batch_i64("v", &[9]));
        let encoded = encode_segment(&h, std::slice::from_ref(&f)).unwrap();
        let (hdr, plain) = split(&encoded, &h);
        // drop only the trailing end-frame byte: frame 0 is intact
        let err = decode_segment(&reassemble(&hdr, &plain[..plain.len() - 1])).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("truncated"), "unexpected error: {msg}");
        assert!(msg.contains("frame 1"), "unexpected error: {msg}");
    }

    #[test]
    fn truncated_outer_bytes_fail_decompress() {
        let h = header();
        let f = frame(
            "org1",
            StreamType::Logs,
            "app1",
            1,
            2,
            batch_i64("v", &[1, 2, 3]),
        );
        let mut encoded = encode_segment(&h, &[f]).unwrap();
        encoded.truncate(encoded.len() - 5);
        let err = decode_segment(&encoded).unwrap_err();
        let msg = format!("{err:#}");
        // streaming decode surfaces outer truncation wherever the read
        // stalls (mid-frame or at the decoder); either way it is a hard
        // error that names the segment
        assert!(
            msg.contains("truncated") || msg.contains("zstd"),
            "unexpected error: {msg}"
        );
        assert!(msg.contains(&h.node_uuid), "unexpected error: {msg}");
    }

    #[test]
    fn version_bump_is_rejected() {
        let h = header();
        let mut encoded = encode_segment(&h, &[]).unwrap();
        encoded[4..6].copy_from_slice(&3u16.to_le_bytes());
        let err = decode_segment(&encoded).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("unsupported version 3"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn bad_magic_is_rejected() {
        let h = header();
        let mut encoded = encode_segment(&h, &[]).unwrap();
        encoded[0] = b'X';
        let err = decode_segment(&encoded).unwrap_err();
        assert!(format!("{err:#}").contains("bad magic"));
    }

    #[test]
    fn short_input_is_rejected_not_panicking() {
        for n in 0..8 {
            let err = decode_segment(&vec![0u8; n]).unwrap_err();
            assert!(!format!("{err:#}").is_empty());
        }
    }

    // ---- hand-rolled frames for adversarial payload content ----

    fn raw_data_frame(
        org: &str,
        stream_type: &str,
        stream: &str,
        min_ts: i64,
        max_ts: i64,
        rows: u32,
        ipc: &[u8],
    ) -> Vec<u8> {
        let mut b = vec![FRAME_TYPE_DATA];
        for s in [org, stream_type, stream] {
            b.extend_from_slice(&(s.len() as u16).to_le_bytes());
            b.extend_from_slice(s.as_bytes());
        }
        b.extend_from_slice(&min_ts.to_le_bytes());
        b.extend_from_slice(&max_ts.to_le_bytes());
        b.extend_from_slice(&rows.to_le_bytes());
        b.extend_from_slice(&(ipc.len() as u32).to_le_bytes());
        b.extend_from_slice(ipc);
        let crc = crc32fast::hash(&b);
        b.extend_from_slice(&crc.to_le_bytes());
        b
    }

    fn ipc_bytes(batch: &RecordBatch) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut w = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema()).unwrap();
        w.write(batch).unwrap();
        w.finish().unwrap();
        drop(w);
        buf
    }

    fn encoded_with_payload(h: &SegmentHeader, plain_payload: &[u8]) -> Vec<u8> {
        let f = frame("o", StreamType::Logs, "s", 0, 0, batch_i64("v", &[1]));
        let real = encode_segment(h, &[f]).unwrap();
        let (hdr, _) = split(&real, h);
        reassemble(&hdr, plain_payload)
    }

    #[test]
    fn unknown_stream_type_string_is_rejected() {
        let h = header();
        let batch = batch_i64("v", &[1, 2]);
        let mut payload = raw_data_frame("org1", "bogus", "app1", 1, 2, 2, &ipc_bytes(&batch));
        payload.push(FRAME_TYPE_END);
        let err = decode_segment(&encoded_with_payload(&h, &payload)).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("unknown stream type"),
            "unexpected error: {msg}"
        );
        assert!(msg.contains("bogus"), "unexpected error: {msg}");
        assert!(msg.contains("frame 0"), "unexpected error: {msg}");
    }

    #[test]
    fn row_count_mismatch_is_rejected() {
        let h = header();
        let batch = batch_i64("v", &[1, 2]);
        let mut payload = raw_data_frame("org1", "logs", "app1", 1, 2, 3, &ipc_bytes(&batch));
        payload.push(FRAME_TYPE_END);
        let err = decode_segment(&encoded_with_payload(&h, &payload)).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("row count mismatch"),
            "unexpected error: {msg}"
        );
        assert!(msg.contains("declares 3"), "unexpected error: {msg}");
    }

    #[test]
    fn garbage_ipc_bytes_are_rejected() {
        let h = header();
        let mut payload = raw_data_frame("org1", "logs", "app1", 1, 2, 2, b"not arrow ipc at all");
        payload.push(FRAME_TYPE_END);
        let err = decode_segment(&encoded_with_payload(&h, &payload)).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("arrow ipc"), "unexpected error: {msg}");
        assert!(msg.contains("frame 0"), "unexpected error: {msg}");
    }

    #[test]
    fn unknown_frame_type_is_rejected() {
        let h = header();
        let payload = vec![7u8];
        let err = decode_segment(&encoded_with_payload(&h, &payload)).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("unknown frame type 7"),
            "unexpected error: {msg}"
        );
        assert!(msg.contains("frame 0"), "unexpected error: {msg}");
    }

    #[test]
    fn trailing_bytes_after_end_frame_are_rejected() {
        let h = header();
        let batch = batch_i64("v", &[1]);
        let mut payload = raw_data_frame("org1", "logs", "app1", 1, 2, 1, &ipc_bytes(&batch));
        payload.push(FRAME_TYPE_END);
        payload.extend_from_slice(&[1, 2, 3]);
        let err = decode_segment(&encoded_with_payload(&h, &payload)).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("trailing bytes"), "unexpected error: {msg}");
    }

    #[test]
    fn parse_stream_type_inverts_display_for_every_variant() {
        for st in [
            StreamType::Logs,
            StreamType::Metrics,
            StreamType::Traces,
            StreamType::ServiceGraph,
            StreamType::EnrichmentTables,
            StreamType::Filelist,
            StreamType::Metadata,
            StreamType::Index,
        ] {
            assert_eq!(parse_stream_type(&st.to_string()), Some(st));
        }
        assert_eq!(parse_stream_type("Logs"), None);
        assert_eq!(parse_stream_type(""), None);
    }

    /// The filtered streaming decode: unwanted frames are skipped without
    /// IPC parsing but still crc-verified; wanted frames come out identical
    /// to the collect-everything path (which is built on it).
    #[test]
    fn filtered_decode_skips_unwanted_and_verifies_all_crcs() {
        let h = header();
        let frames = vec![
            frame(
                "org1",
                StreamType::Traces,
                "default",
                10,
                20,
                batch_i64("v", &[1, 2, 3]),
            ),
            frame(
                "org1",
                StreamType::Logs,
                "default",
                10,
                20,
                batch_i64("v", &[4, 5]),
            ),
            frame(
                "org1",
                StreamType::Traces,
                "other",
                30,
                40,
                batch_i64("v", &[6]),
            ),
        ];
        let encoded = encode_segment(&h, &frames).unwrap();

        // want only traces/default: exactly one frame surfaces, identical
        // to the full decode's copy of it
        let mut seen: Vec<SegmentFrame> = Vec::new();
        let mut inspected = 0usize;
        let header_out = decode_segment_filtered(
            &encoded,
            |info| {
                inspected += 1;
                info.stream_type == StreamType::Traces && info.stream == "default"
            },
            |f| {
                seen.push(f);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(header_out, h);
        assert_eq!(inspected, 3, "every frame's identity is offered");
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].stream, "default");
        assert_eq!(seen[0].batch.num_rows(), 3);
        let (_, full) = decode_segment(&encoded).unwrap();
        assert_eq!(full[0].batch, seen[0].batch);

        // corrupt a byte inside the SECOND (unwanted) frame's region: the
        // filtered decode must still fail — every frame is crc-guarded.
        // Corrupting compressed bytes may also break zstd itself; either
        // way it must be a hard error.
        let mut broken = encoded.clone();
        let mid = broken.len() / 2;
        broken[mid] ^= 0x55;
        let res = decode_segment_filtered(&broken, |_| false, |_| Ok(()));
        assert!(res.is_err(), "corruption anywhere must fail the decode");

        // want-nothing over a clean segment: header parses, no frames, no error
        let mut none = 0usize;
        decode_segment_filtered(
            &encoded,
            |_| false,
            |_| {
                none += 1;
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(none, 0);
    }

    /// The frame identity offered to the filter carries the bounds the scan
    /// prunes on, before any IPC work.
    #[test]
    fn filtered_decode_offers_bounds_before_parsing() {
        let h = header();
        let encoded = encode_segment(
            &h,
            &[frame(
                "orgX",
                StreamType::Logs,
                "s1",
                111,
                222,
                batch_i64("v", &[7, 8]),
            )],
        )
        .unwrap();
        let mut infos: Vec<(String, i64, i64, u32)> = Vec::new();
        decode_segment_filtered(
            &encoded,
            |info| {
                infos.push((info.stream.clone(), info.min_ts, info.max_ts, info.rows));
                false
            },
            |_| Ok(()),
        )
        .unwrap();
        assert_eq!(infos, vec![("s1".to_string(), 111, 222, 2)]);
    }

    // ---- version 2: per-stream regions, per-buffer IPC compression ----

    fn batch_ts_two_strings(ts: &[i64], a: &[&str], b: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ts.to_vec())),
                Arc::new(StringArray::from(a.to_vec())),
                Arc::new(StringArray::from(b.to_vec())),
            ],
        )
        .unwrap()
    }

    fn decode_projected_all(
        encoded: &[u8],
        columns: ColumnFilter<'_>,
    ) -> Result<Vec<SegmentFrame>, anyhow::Error> {
        let mut out = Vec::new();
        decode_segment_projected(
            encoded,
            columns,
            |_| true,
            |f| {
                out.push(f);
                Ok(())
            },
        )?;
        Ok(out)
    }

    /// Raw version-2 object from hand-written directory entries and region
    /// bytes, for adversarial layouts the encoder never produces.
    #[allow(clippy::type_complexity)]
    fn raw_v2(
        h: &SegmentHeader,
        entries: &[(&str, &str, &str, i64, i64, u64, u32, u64, u64)],
        area: &[u8],
    ) -> Vec<u8> {
        let mut out = encode_segment_version(h, &[], SEGMENT_VERSION_V2).unwrap();
        out.truncate(header_len(h));
        let mut dir = (entries.len() as u32).to_le_bytes().to_vec();
        for (org, st, stream, min, max, rows, frames, offset, len) in entries {
            for s in [*org, *st, *stream] {
                dir.extend_from_slice(&(s.len() as u16).to_le_bytes());
                dir.extend_from_slice(s.as_bytes());
            }
            dir.extend_from_slice(&min.to_le_bytes());
            dir.extend_from_slice(&max.to_le_bytes());
            dir.extend_from_slice(&rows.to_le_bytes());
            dir.extend_from_slice(&frames.to_le_bytes());
            dir.extend_from_slice(&offset.to_le_bytes());
            dir.extend_from_slice(&len.to_le_bytes());
        }
        out.extend_from_slice(&(dir.len() as u32).to_le_bytes());
        out.extend_from_slice(&dir);
        out.extend_from_slice(&crc32fast::hash(&dir).to_le_bytes());
        out.extend_from_slice(area);
        out
    }

    #[test]
    fn v2_round_trip_groups_frames_by_stream() {
        let h = header();
        // interleaved streams and a type-flipped schema inside one stream
        let frames = vec![
            frame(
                "org1",
                StreamType::Logs,
                "app1",
                100,
                300,
                batch_i64("value", &[1, 2, 3]),
            ),
            frame(
                "org1",
                StreamType::Traces,
                "spans",
                50,
                60,
                batch_utf8("name", &["a", "b"]),
            ),
            frame(
                "org1",
                StreamType::Logs,
                "app1",
                400,
                500,
                batch_utf8("value", &["x", "y"]),
            ),
            frame(
                "org2",
                StreamType::Metrics,
                "cpu",
                -10,
                10,
                batch_i64("gauge", &[7]),
            ),
        ];
        let encoded = encode_segment_version(&h, &frames, SEGMENT_VERSION_V2).unwrap();
        assert_eq!(&encoded[4..6], &SEGMENT_VERSION_V2.to_le_bytes());
        let (decoded_header, decoded) = decode_segment(&encoded).unwrap();
        assert_eq!(decoded_header, h);
        // regions follow first appearance; frames keep order inside a region
        let want_order = [0usize, 2, 1, 3];
        assert_eq!(decoded.len(), want_order.len());
        for (got, &i) in decoded.iter().zip(want_order.iter()) {
            let want = &frames[i];
            assert_eq!(
                (
                    &got.org,
                    got.stream_type,
                    &got.stream,
                    got.min_ts,
                    got.max_ts
                ),
                (
                    &want.org,
                    want.stream_type,
                    &want.stream,
                    want.min_ts,
                    want.max_ts
                )
            );
            assert_eq!(got.batch, want.batch);
        }
        assert_eq!(
            decoded[0].batch.schema().field(0).data_type(),
            &DataType::Int64
        );
        assert_eq!(
            decoded[1].batch.schema().field(0).data_type(),
            &DataType::Utf8
        );
    }

    #[test]
    fn v2_empty_frames_round_trip() {
        let h = header();
        let encoded = encode_segment_version(&h, &[], SEGMENT_VERSION_V2).unwrap();
        let (decoded_header, decoded) = decode_segment(&encoded).unwrap();
        assert_eq!(decoded_header, h);
        assert!(decoded.is_empty());
    }

    #[test]
    fn unsupported_encode_version_is_rejected() {
        let err = encode_segment_version(&header(), &[], 3).unwrap_err();
        assert!(format!("{err:#}").contains("cannot encode format version 3"));
    }

    /// Both versions decode exactly the accepted columns, keep the row count
    /// when no column is accepted, and treat keep-everything as a full decode.
    #[test]
    fn projected_decode_returns_only_accepted_columns() {
        let h = header();
        let batch = batch_ts_two_strings(&[1, 2, 3], &["a1", "a2", "a3"], &["b1", "b2", "b3"]);
        let frames = vec![frame("o", StreamType::Logs, "s", 1, 3, batch.clone())];
        for version in [SEGMENT_VERSION_V1, SEGMENT_VERSION_V2] {
            let encoded = encode_segment_version(&h, &frames, version).unwrap();

            let keep_ts_and_a = |name: &str| name == "_timestamp" || name == "a";
            let got = decode_projected_all(&encoded, Some(&keep_ts_and_a)).unwrap();
            assert_eq!(got.len(), 1);
            let projected = &got[0].batch;
            assert_eq!(projected.num_columns(), 2, "version {version}");
            assert_eq!(projected.schema().field(0).name(), "_timestamp");
            assert_eq!(projected.schema().field(1).name(), "a");
            assert_eq!(projected.column(0), batch.column(0));
            assert_eq!(projected.column(1), batch.column(1));

            let keep_none = |_: &str| false;
            let got = decode_projected_all(&encoded, Some(&keep_none)).unwrap();
            assert_eq!(got[0].batch.num_columns(), 0, "version {version}");
            assert_eq!(got[0].batch.num_rows(), 3, "version {version}");

            let keep_all = |_: &str| true;
            let got = decode_projected_all(&encoded, Some(&keep_all)).unwrap();
            assert_eq!(got[0].batch, batch, "version {version}");
        }
    }

    /// A projected batch must not keep the dropped columns' bytes alive:
    /// version 1 IPC buffers are slices of one decompressed message body.
    #[test]
    fn projected_decode_does_not_pin_dropped_columns() {
        let h = header();
        let rows = 2_000usize;
        let ts: Vec<i64> = (0..rows as i64).collect();
        let small: Vec<String> = (0..rows).map(|i| format!("s{i}")).collect();
        // ~1 MB of distinct, poorly compressible text in the dropped column
        let big: Vec<String> = (0..rows)
            .map(|i| {
                format!(
                    "{:0512x}",
                    (i as u128).wrapping_mul(0x9e37_79b9_7f4a_7c15_f39c_c060_5ced_c834)
                )
            })
            .collect();
        let small_refs: Vec<&str> = small.iter().map(String::as_str).collect();
        let big_refs: Vec<&str> = big.iter().map(String::as_str).collect();
        let batch = batch_ts_two_strings(&ts, &small_refs, &big_refs);
        let frames = vec![frame(
            "o",
            StreamType::Logs,
            "s",
            0,
            rows as i64,
            batch.clone(),
        )];
        let keep = |name: &str| name != "b";
        for version in [SEGMENT_VERSION_V1, SEGMENT_VERSION_V2] {
            let encoded = encode_segment_version(&h, &frames, version).unwrap();
            let got = decode_projected_all(&encoded, Some(&keep)).unwrap();
            let projected = &got[0].batch;
            assert_eq!(projected.num_columns(), 2);
            assert_eq!(projected.column(1), batch.column(1));
            for column in projected.columns() {
                assert!(
                    !pins_larger_allocation(&column.to_data()),
                    "version {version}: a kept column still references a larger allocation"
                );
            }
            let retained: usize = projected
                .columns()
                .iter()
                .map(|c| c.get_buffer_memory_size())
                .sum();
            assert!(
                retained < 256 * 1024,
                "version {version}: projected batch retains {retained} bytes"
            );
        }
    }

    fn v2_two_stream_object(h: &SegmentHeader) -> (Vec<u8>, usize) {
        let frames = vec![
            frame(
                "o",
                StreamType::Logs,
                "first",
                1,
                2,
                batch_i64("v", &[1, 2]),
            ),
            frame(
                "o",
                StreamType::Logs,
                "second",
                3,
                4,
                batch_i64("v", &[3, 4]),
            ),
        ];
        let encoded = encode_segment_version(h, &frames, SEGMENT_VERSION_V2).unwrap();
        let dir_len_at = header_len(h);
        let dir_len =
            u32::from_le_bytes(encoded[dir_len_at..dir_len_at + 4].try_into().unwrap()) as usize;
        let area_start = dir_len_at + 4 + dir_len + 4;
        (encoded, area_start)
    }

    /// Unwanted regions are skipped without hashing; wanted frames are
    /// crc-checked, and a full decode still catches the corruption.
    #[test]
    fn v2_skips_unwanted_regions_but_checks_wanted_frames() {
        let h = header();
        let (mut encoded, area_start) = v2_two_stream_object(&h);
        // flip the last ipc byte of the FIRST region's only frame (just
        // before its 4-byte crc): only that frame's crc covers it
        let first_frame_len = {
            let mut off = area_start + 1;
            off += 2 + 1 + 2 + 4 + 2 + "first".len() + 8 + 8 + 4;
            let ipc_len = u32::from_le_bytes(encoded[off..off + 4].try_into().unwrap()) as usize;
            off + 4 + ipc_len + 4 - area_start
        };
        encoded[area_start + first_frame_len - 5] ^= 0xFF;

        let mut seen = Vec::new();
        decode_segment_filtered(
            &encoded,
            |info| info.stream == "second",
            |f| {
                seen.push(f.stream);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(seen, vec!["second".to_string()]);

        let err = decode_segment(&encoded).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("crc32 mismatch"), "unexpected error: {msg}");
        assert!(
            msg.contains("region 0 (o/logs/first)"),
            "unexpected error: {msg}"
        );
        assert!(msg.contains("frame 0"), "unexpected error: {msg}");
    }

    #[test]
    fn v2_directory_corruption_is_rejected() {
        let h = header();
        let (mut encoded, _) = v2_two_stream_object(&h);
        // first byte of the first directory entry (org length)
        encoded[header_len(&h) + 4 + 4] ^= 0x01;
        let err = decode_segment_filtered(&encoded, |_| false, |_| Ok(())).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("directory crc32 mismatch"),
            "unexpected error: {msg}"
        );
        assert!(msg.contains(&h.node_uuid), "unexpected error: {msg}");
    }

    #[test]
    fn v2_truncation_is_rejected_even_for_unwanted_regions() {
        let h = header();
        let (mut encoded, _) = v2_two_stream_object(&h);
        encoded.truncate(encoded.len() - 3);
        let err = decode_segment_filtered(&encoded, |_| false, |_| Ok(())).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("truncated region"), "unexpected error: {msg}");
    }

    #[test]
    fn v2_trailing_bytes_after_last_region_are_rejected() {
        let h = header();
        let (mut encoded, _) = v2_two_stream_object(&h);
        encoded.extend_from_slice(&[0, 0, 0]);
        let err = decode_segment(&encoded).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("trailing bytes after the last region"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn v2_frame_identity_must_match_its_region() {
        let h = header();
        let batch = batch_i64("v", &[1]);
        let region = raw_data_frame("o", "logs", "other", 1, 1, 1, &ipc_bytes(&batch));
        let encoded = raw_v2(
            &h,
            &[("o", "logs", "s", 1, 1, 1, 1, 0, region.len() as u64)],
            &region,
        );
        let err = decode_segment(&encoded).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("does not match its region"),
            "unexpected error: {msg}"
        );
        assert!(
            msg.contains("region 0 (o/logs/s)"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn v2_directory_counts_must_match_the_region() {
        let h = header();
        let batch = batch_i64("v", &[1, 2]);
        let region = raw_data_frame("o", "logs", "s", 1, 2, 2, &ipc_bytes(&batch));
        let encoded = raw_v2(
            &h,
            &[("o", "logs", "s", 1, 2, 2, 3, 0, region.len() as u64)],
            &region,
        );
        let err = decode_segment(&encoded).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("directory declares 3 frames"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn v2_regions_must_be_contiguous() {
        let h = header();
        let batch = batch_i64("v", &[1]);
        let region = raw_data_frame("o", "logs", "s", 1, 1, 1, &ipc_bytes(&batch));
        let mut area = vec![0u8; 4];
        area.extend_from_slice(&region);
        let encoded = raw_v2(
            &h,
            &[("o", "logs", "s", 1, 1, 1, 1, 4, region.len() as u64)],
            &area,
        );
        let err = decode_segment(&encoded).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("must be contiguous"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn v2_frame_bounds_must_fall_inside_the_region() {
        let h = header();
        let batch = batch_i64("v", &[1]);
        let region = raw_data_frame("o", "logs", "s", 1, 9, 1, &ipc_bytes(&batch));
        let encoded = raw_v2(
            &h,
            &[("o", "logs", "s", 1, 5, 1, 1, 0, region.len() as u64)],
            &region,
        );
        let err = decode_segment(&encoded).unwrap_err();
        assert!(format!("{err:#}").contains("outside the region bounds"));
    }

    #[test]
    fn v2_region_filter_matches_frame_filter_without_reading_skipped_headers() {
        let h = header();
        let frames = vec![
            frame(
                "o",
                StreamType::Logs,
                "other",
                -10,
                10,
                batch_i64("v", &[1]),
            ),
            frame("o", StreamType::Logs, "s", 10, 20, batch_i64("v", &[2, 3])),
            frame("x", StreamType::Logs, "s", 10, 20, batch_i64("v", &[4])),
            frame("o", StreamType::Traces, "s", 10, 20, batch_i64("v", &[5])),
            frame(
                "o",
                StreamType::Logs,
                "s",
                30,
                40,
                batch_utf8("v", &["中", "\\\""]),
            ),
        ];
        let encoded = encode_segment_version(&h, &frames, SEGMENT_VERSION_V2).unwrap();
        let wanted = |info: &FrameInfo| {
            info.org == "o" && info.stream_type == StreamType::Logs && info.stream == "s"
        };
        let mut baseline = Vec::new();
        RAW_FRAME_HEADER_READS.set(0);
        decode_segment_projected(&encoded, None, wanted, |f| {
            baseline.push(f);
            Ok(())
        })
        .unwrap();
        assert_eq!(RAW_FRAME_HEADER_READS.get(), 5);

        let mut actual = Vec::new();
        let mut considered = 0;
        let mut skipped = 0;
        RAW_FRAME_HEADER_READS.set(0);
        let decoded_header = decode_segment_projected_regions(
            &encoded,
            None,
            Some(&mut |region| {
                considered += 1;
                let keep = region.org == "o"
                    && region.stream_type == StreamType::Logs
                    && region.stream == "s";
                skipped += usize::from(!keep);
                Ok(keep)
            }),
            wanted,
            |f| {
                actual.push(f);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(decoded_header, h);
        assert_eq!((considered, skipped), (4, 3));
        assert_eq!(RAW_FRAME_HEADER_READS.get(), 2);
        assert_eq!(actual.len(), 2);
        assert_eq!(
            encode_segment_version(&h, &actual, SEGMENT_VERSION_V2).unwrap(),
            encode_segment_version(&h, &baseline, SEGMENT_VERSION_V2).unwrap()
        );
    }

    #[test]
    fn region_filter_half_open_boundaries_match_frame_filter() {
        let h = header();
        let bounds = [(i64::MIN, -1), (0, 9), (10, 10), (11, 19), (20, i64::MAX)];
        let frames = bounds
            .iter()
            .enumerate()
            .map(|(i, &(min, max))| {
                frame(
                    "o",
                    StreamType::Logs,
                    &format!("s{i}"),
                    min,
                    max,
                    batch_i64("v", &[min]),
                )
            })
            .collect::<Vec<_>>();
        for version in [SEGMENT_VERSION_V1, SEGMENT_VERSION_V2] {
            let encoded = encode_segment_version(&h, &frames, version).unwrap();
            for window in [
                (0, 0),
                (10, 20),
                (10, 10),
                (20, 10),
                (i64::MIN, 0),
                (20, i64::MAX),
            ] {
                let overlaps = |min, max| {
                    window == (0, 0) || (window.0 < window.1 && max >= window.0 && min < window.1)
                };
                for projected in [false, true] {
                    let keep_column = |_: &str| false;
                    let columns: ColumnFilter<'_> = projected.then_some(&keep_column);
                    let mut old = Vec::new();
                    decode_segment_projected(
                        &encoded,
                        columns,
                        |f| overlaps(f.min_ts, f.max_ts),
                        |f| {
                            old.push(f);
                            Ok(())
                        },
                    )
                    .unwrap();
                    let mut new = Vec::new();
                    decode_segment_projected_regions(
                        &encoded,
                        columns,
                        Some(&mut |r| Ok(overlaps(r.min_ts, r.max_ts))),
                        |f| overlaps(f.min_ts, f.max_ts),
                        |f| {
                            new.push(f);
                            Ok(())
                        },
                    )
                    .unwrap();
                    assert_eq!(
                        encode_segment_version(&h, &old, version).unwrap(),
                        encode_segment_version(&h, &new, version).unwrap(),
                        "version {version}, window {window:?}, projected {projected}"
                    );
                }
            }
        }
    }

    #[test]
    fn v1_never_calls_region_filter() {
        let h = header();
        let encoded = encode_segment(
            &h,
            &[frame(
                "o",
                StreamType::Logs,
                "s",
                1,
                1,
                batch_i64("v", &[1]),
            )],
        )
        .unwrap();
        let mut rows = 0;
        decode_segment_projected_regions(
            &encoded,
            None,
            Some(&mut |_| panic!("version 1 must not consult the directory predicate")),
            |info| {
                rows += info.rows;
                false
            },
            |_| panic!("frame filter rejected this frame"),
        )
        .unwrap();
        assert_eq!(rows, 1);
        let (hdr, mut payload) = split(&encoded, &h);
        let last = payload.len() - 2;
        payload[last] ^= 0xff;
        assert!(
            decode_segment_projected_regions(
                &reassemble(&hdr, &payload),
                None,
                Some(&mut |_| Ok(false)),
                |_| false,
                |_| Ok(())
            )
            .is_err()
        );
    }

    #[test]
    fn v2_region_filter_preserves_full_validation_entry_points() {
        let h = header();
        let batch = batch_i64("v", &[1, 2]);
        for (org, max, rows, frames, expected) in [
            ("x", 2, 2, 1, "does not match its region"),
            ("o", 1, 2, 1, "outside the region bounds"),
            ("o", 2, 3, 1, "directory declares"),
            ("o", 2, 2, 3, "directory declares"),
        ] {
            let area = raw_data_frame("o", "logs", "s", 1, 2, 2, &ipc_bytes(&batch));
            let encoded = raw_v2(
                &h,
                &[(org, "logs", "s", 1, max, rows, frames, 0, area.len() as u64)],
                &area,
            );
            decode_segment_projected_regions(
                &encoded,
                None,
                Some(&mut |_| Ok(false)),
                |_| panic!("skipped region"),
                |_| Ok(()),
            )
            .unwrap();
            let err = decode_segment(&encoded).unwrap_err();
            assert!(format!("{err:#}").contains(expected), "{err:#}");
            let err = decode_segment_filtered(&encoded, |_| false, |_| Ok(())).unwrap_err();
            assert!(format!("{err:#}").contains(expected), "{err:#}");
        }
        let (mut encoded, area_start) = v2_two_stream_object(&h);
        encoded[area_start] = 7;
        decode_segment_projected_regions(
            &encoded,
            None,
            Some(&mut |_| Ok(false)),
            |_| panic!("skipped region"),
            |_| Ok(()),
        )
        .unwrap();
        assert!(
            format!("{:#}", decode_segment(&encoded).unwrap_err()).contains("unknown frame type 7")
        );
    }

    #[test]
    fn v2_region_filter_still_checks_directory_and_selected_crc() {
        let h = header();
        let (encoded, _) = v2_two_stream_object(&h);
        let reject_all = |bytes: &[u8]| {
            decode_segment_projected_regions(
                bytes,
                None,
                Some(&mut |_| Ok(false)),
                |_| false,
                |_| Ok(()),
            )
        };
        let mut bad_directory = encoded.clone();
        bad_directory[header_len(&h) + 8] ^= 1;
        assert!(
            format!("{:#}", reject_all(&bad_directory).unwrap_err())
                .contains("directory crc32 mismatch")
        );
        assert!(
            format!(
                "{:#}",
                reject_all(&encoded[..encoded.len() - 1]).unwrap_err()
            )
            .contains("truncated region")
        );
        let mut bad_crc = encoded;
        let last = bad_crc.len() - 1;
        bad_crc[last] ^= 1;
        reject_all(&bad_crc).unwrap();
        let err = decode_segment_projected_regions(
            &bad_crc,
            None,
            Some(&mut |r| Ok(r.stream == "second")),
            |_| true,
            |_| Ok(()),
        )
        .unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("crc32 mismatch") && msg.contains("frame 1"),
            "{msg}"
        );
        assert!(format!("{:#}", decode_segment(&bad_crc).unwrap_err()).contains("crc32 mismatch"));
    }

    #[test]
    fn region_filter_error_and_empty_segments() {
        let h = header();
        for version in [SEGMENT_VERSION_V1, SEGMENT_VERSION_V2] {
            let encoded = encode_segment_version(&h, &[], version).unwrap();
            let got = decode_segment_projected_regions(
                &encoded,
                None,
                Some(&mut |_| panic!("empty segment")),
                |_| panic!("empty segment"),
                |_| Ok(()),
            )
            .unwrap();
            assert_eq!(got, h);
        }
        let (encoded, _) = v2_two_stream_object(&h);
        let err = decode_segment_projected_regions(
            &encoded,
            None,
            Some(&mut |_| bail!("selector failed")),
            |_| true,
            |_| Ok(()),
        )
        .unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("region 0") && msg.contains("selector failed"),
            "{msg}"
        );
    }

    #[test]
    fn v2_short_payloads_are_rejected_not_panicking() {
        let h = header();
        let (encoded, _) = v2_two_stream_object(&h);
        for n in header_len(&h)..encoded.len() {
            let res = decode_segment(&encoded[..n]);
            assert!(res.is_err(), "prefix of {n} bytes decoded");
        }
    }
}
