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

//! Segment object encoding (DESIGN-SEGMENT-WAL.md, format version 1).
//!
//! Layout (all integers little-endian):
//!
//! ```text
//! header (raw): magic "O2WS" | u16 version | u16 uuid_len | uuid bytes
//!             | u64 seq | i64 created_at_micros
//! payload: ONE zstd stream of concatenated frames:
//!   data frame: u8 frame_type=1
//!             | u16 org_len | org
//!             | u16 stream_type_len | stream_type (Display string)
//!             | u16 stream_len | stream
//!             | i64 min_ts | i64 max_ts | u32 rows
//!             | u32 ipc_len | arrow IPC STREAM bytes (self-describing schema)
//!             | u32 crc32 over every preceding byte of this frame
//!   end frame: u8 frame_type=0 (no body, no crc) — must be last
//! ```
//!
//! Decoding is all-or-nothing: any inconsistency (bad magic/version, crc
//! mismatch, truncation, bad frame type, IPC failure, unknown stream type,
//! row-count mismatch) is a hard error naming the segment and frame index.
//! Segments are small and written atomically — there is no recoverable tail.

use anyhow::{Context, anyhow, bail};
use arrow::record_batch::RecordBatch;
use config::meta::stream::StreamType;

pub const SEGMENT_MAGIC: &[u8; 4] = b"O2WS";
pub const SEGMENT_VERSION: u16 = 1;

const FRAME_TYPE_END: u8 = 0;
const FRAME_TYPE_DATA: u8 = 1;

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

/// Encode one segment object: raw header + one zstd stream of crc-guarded
/// frames (layout in DESIGN-SEGMENT-WAL.md).
pub fn encode_segment(
    header: &SegmentHeader,
    frames: &[SegmentFrame],
) -> Result<Vec<u8>, anyhow::Error> {
    let seg = format!("{}/{:020}", header.node_uuid, header.seq);
    let uuid = header.node_uuid.as_bytes();
    let uuid_len = u16::try_from(uuid.len())
        .map_err(|_| anyhow!("segment {seg}: node_uuid length {} exceeds u16", uuid.len()))?;

    let mut out = Vec::with_capacity(4 + 2 + 2 + uuid.len() + 8 + 8);
    out.extend_from_slice(SEGMENT_MAGIC);
    out.extend_from_slice(&SEGMENT_VERSION.to_le_bytes());
    out.extend_from_slice(&uuid_len.to_le_bytes());
    out.extend_from_slice(uuid);
    out.extend_from_slice(&header.seq.to_le_bytes());
    out.extend_from_slice(&header.created_at.to_le_bytes());

    let mut payload = Vec::new();
    for (idx, frame) in frames.iter().enumerate() {
        write_data_frame(&mut payload, frame).with_context(|| {
            format!(
                "segment {seg}: encode frame {idx} ({}/{}/{})",
                frame.org, frame.stream_type, frame.stream
            )
        })?;
    }
    payload.push(FRAME_TYPE_END);

    let compressed = zstd::encode_all(payload.as_slice(), zstd::DEFAULT_COMPRESSION_LEVEL)
        .with_context(|| format!("segment {seg}: zstd compress failed"))?;
    out.extend_from_slice(&compressed);
    Ok(out)
}

fn write_data_frame(buf: &mut Vec<u8>, frame: &SegmentFrame) -> Result<(), anyhow::Error> {
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

    let mut ipc = Vec::new();
    {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut ipc, &frame.batch.schema())
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
    let header = decode_segment_filtered(bytes, |_| true, |frame| {
        frames.push(frame);
        Ok(())
    })?;
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

/// Streaming decode: the payload decompresses INCREMENTALLY (one zstd
/// stream) and each data frame's identity goes to `want`; only when it
/// returns true is the frame's IPC body parsed into a batch and handed to
/// `on_frame`. Peak memory ≈ the largest single frame (+ the zstd window)
/// instead of the whole decompressed payload — unwanted frames' bytes are
/// decompressed through and CRC-checked but never IPC-parsed.
///
/// Integrity semantics are IDENTICAL to [`decode_segment`] (which is built
/// on this): every frame's crc32 is verified whether wanted or not, and any
/// inconsistency is a hard error naming the segment and frame index —
/// segments are small and written atomically, there is no recoverable tail.
pub fn decode_segment_filtered(
    bytes: &[u8],
    mut want: impl FnMut(&FrameInfo) -> bool,
    mut on_frame: impl FnMut(SegmentFrame) -> Result<(), anyhow::Error>,
) -> Result<SegmentHeader, anyhow::Error> {
    let mut r = Reader::new(bytes);
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
    if version != SEGMENT_VERSION {
        bail!(
            "segment decode: unsupported version {version}, this build reads version {SEGMENT_VERSION}"
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
    let header = SegmentHeader {
        node_uuid,
        seq,
        created_at,
    };
    let seg = format!("{}/{:020}", header.node_uuid, header.seq);

    let decoder = zstd::stream::read::Decoder::new(&bytes[r.pos..])
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
                return Ok(header);
            }
            FRAME_TYPE_DATA => {
                decode_data_frame_streamed(&mut fr, &mut want, &mut on_frame)
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

    let ipc_bytes = &fr.buf()[ipc_range];
    let batch = parse_frame_batch(&info, ipc_bytes)?;
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
fn parse_frame_batch(info: &FrameInfo, ipc_bytes: &[u8]) -> Result<RecordBatch, anyhow::Error> {
    let (org, stream_type, stream) = (&info.org, info.stream_type, &info.stream);
    let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(ipc_bytes), None)
        .with_context(|| {
        format!("stream {org}/{stream_type}/{stream}: arrow ipc open failed")
    })?;
    let schema = reader.schema();
    let mut batches = reader
        .collect::<Result<Vec<_>, _>>()
        .with_context(|| format!("stream {org}/{stream_type}/{stream}: arrow ipc decode failed"))?;
    // the encoder writes exactly one batch per frame, but merge defensively;
    // batches from one ipc stream share one schema, so concat is safe here
    Ok(match batches.len() {
        0 => RecordBatch::new_empty(schema),
        1 => batches.swap_remove(0),
        _ => arrow::compute::concat_batches(&schema, batches.iter()).with_context(|| {
            format!("stream {org}/{stream_type}/{stream}: arrow ipc concat failed")
        })?,
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
        encoded[4..6].copy_from_slice(&2u16.to_le_bytes());
        let err = decode_segment(&encoded).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("unsupported version 2"),
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
            frame("org1", StreamType::Traces, "default", 10, 20, batch_i64("v", &[1, 2, 3])),
            frame("org1", StreamType::Logs, "default", 10, 20, batch_i64("v", &[4, 5])),
            frame("org1", StreamType::Traces, "other", 30, 40, batch_i64("v", &[6])),
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
        decode_segment_filtered(&encoded, |_| false, |_| {
            none += 1;
            Ok(())
        })
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
            &[frame("orgX", StreamType::Logs, "s1", 111, 222, batch_i64("v", &[7, 8]))],
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
}
