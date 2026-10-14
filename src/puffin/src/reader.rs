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

use anyhow::{Result, anyhow, ensure};

use super::*;

/// Hard cap on the DECODED size of a compressed footer payload: a footer is
/// metadata (blob table + properties), so a frame header advertising more
/// than this is corrupt or hostile, never a real file.
pub const MAX_DECODED_FOOTER_BYTES: u64 = 256 * 1024 * 1024;

/// The end-anchored footer trailer, located in `data`.
struct FooterTrailer {
    flags: PuffinFooterFlags,
    /// Payload bytes AS STORED (compressed length when flagged).
    stored_len: u64,
    /// `data` offset of the first stored payload byte.
    payload_start: usize,
}

fn locate_footer(data: &[u8]) -> Result<FooterTrailer> {
    let total = data.len() as u64;
    ensure!(
        total >= MIN_FILE_SIZE,
        "file too small to be a puffin file: {total} bytes (min {MIN_FILE_SIZE})"
    );

    // Footer tail: ... PayloadSize[4] Flags[4] FootMagic[4]
    let footer = &data[(total - FOOTER_SIZE) as usize..];
    ensure!(
        footer[(FOOTER_SIZE - MAGIC_SIZE) as usize..] == MAGIC,
        "Footer MAGIC mismatch (not a puffin file?)"
    );

    let mut flags_bytes = [0u8; 4];
    flags_bytes.copy_from_slice(
        &footer
            [(FOOTER_SIZE - MAGIC_SIZE - FLAGS_SIZE) as usize..(FOOTER_SIZE - MAGIC_SIZE) as usize],
    );
    let flags = PuffinFooterFlags::from_bits(u32::from_le_bytes(flags_bytes))
        .ok_or_else(|| anyhow!("Error parsing Puffin flags from bytes"))?;

    let mut payload_size_bytes = [0u8; 4];
    payload_size_bytes.copy_from_slice(&footer[0..FOOTER_PAYLOAD_SIZE_SIZE as usize]);
    let stored_len = i32::from_le_bytes(payload_size_bytes) as u64;

    ensure!(
        total >= FOOTER_SIZE + stored_len + MAGIC_SIZE,
        "Unexpected payload size: {stored_len} vs file size {total}"
    );

    // Payload region: HeadMagic[4] Payload[stored_len]
    let head_magic_at = (total - FOOTER_SIZE - stored_len - MAGIC_SIZE) as usize;
    let payload_start = head_magic_at + MAGIC_SIZE as usize;
    ensure!(
        data[head_magic_at..payload_start] == MAGIC,
        "Payload MAGIC mismatch"
    );
    Ok(FooterTrailer {
        flags,
        stored_len,
        payload_start,
    })
}

/// The DECODED size of the footer payload located in `data` (any
/// end-anchored suffix covering the footer region): the stored length for a
/// plain footer, the zstd frame's content size for a compressed one — read
/// from the frame header, without decoding. This is what a reader's memory
/// admission must be sized on, not the stored length.
pub fn footer_payload_decoded_size(data: &[u8]) -> Result<u64> {
    let trailer = locate_footer(data)?;
    if !trailer.flags.contains(PuffinFooterFlags::COMPRESSED) {
        return Ok(trailer.stored_len);
    }
    let payload = &data[trailer.payload_start..trailer.payload_start + trailer.stored_len as usize];
    decoded_frame_size(payload)
}

fn decoded_frame_size(frame: &[u8]) -> Result<u64> {
    let size = zstd::zstd_safe::get_frame_content_size(frame)
        .map_err(|e| anyhow!("compressed puffin footer: unreadable zstd frame header: {e}"))?
        .ok_or_else(|| anyhow!("compressed puffin footer: zstd frame lacks a content size"))?;
    ensure!(
        size <= MAX_DECODED_FOOTER_BYTES,
        "compressed puffin footer advertises {size} decoded bytes (max {MAX_DECODED_FOOTER_BYTES})"
    );
    Ok(size)
}

/// The footer PAYLOAD of `data` (any end-anchored suffix covering the footer
/// region) as the [`PuffinMeta`] JSON bytes — decoded when the footer is
/// compressed, copied when plain. For producers that embed one object's
/// footer in another; the bytes re-parse with `serde_json` into
/// [`PuffinMeta`] exactly like [`parse_puffin_footer_from_bytes`] does.
pub fn footer_payload_bytes(data: &[u8]) -> Result<Vec<u8>> {
    let trailer = locate_footer(data)?;
    let payload = &data[trailer.payload_start..trailer.payload_start + trailer.stored_len as usize];
    if trailer.flags.contains(PuffinFooterFlags::COMPRESSED) {
        let capacity = decoded_frame_size(payload)? as usize;
        return zstd::bulk::decompress(payload, capacity)
            .map_err(|e| anyhow!("Error decompressing footer payload: {e}"));
    }
    Ok(payload.to_vec())
}

/// Parse the Puffin footer ([`PuffinMeta`]) out of in-memory puffin bytes.
///
/// `data` may be the complete file or any end-anchored suffix that covers
/// the footer region (`HeadMagic Payload PayloadSize Flags FootMagic`) — the
/// parser only looks at end-anchored offsets. A payload flagged
/// [`PuffinFooterFlags::COMPRESSED`] is zstd-decoded first (sized from the
/// frame header, bounded by [`MAX_DECODED_FOOTER_BYTES`]).
pub fn parse_puffin_footer_from_bytes(data: &[u8]) -> Result<PuffinMeta> {
    let trailer = locate_footer(data)?;
    let payload = &data[trailer.payload_start..trailer.payload_start + trailer.stored_len as usize];
    if trailer.flags.contains(PuffinFooterFlags::COMPRESSED) {
        let capacity = decoded_frame_size(payload)? as usize;
        let decoded = zstd::bulk::decompress(payload, capacity)
            .map_err(|e| anyhow!("Error decompressing footer payload: {e}"))?;
        return serde_json::from_slice(&decoded)
            .map_err(|e| anyhow!("Error parsing footer payload {e}"));
    }
    serde_json::from_slice(payload).map_err(|e| anyhow!("Error parsing footer payload {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writer::PuffinBytesWriter;

    #[test]
    fn test_parse_footer_round_trip() {
        let mut buf = Vec::new();
        {
            let mut writer = PuffinBytesWriter::new(&mut buf);
            writer.set_property("purpose", "round-trip");
            writer
                .add_blob(b"dictionary bytes", "o2-vix-dict-v1", "dict".to_string())
                .unwrap();
            writer
                .add_blob(b"totally unknown", "some-future-blob-v9", "x".to_string())
                .unwrap();
            writer.finish().unwrap();
        }

        let meta = parse_puffin_footer_from_bytes(&buf).unwrap();
        assert_eq!(meta.properties.get("purpose").unwrap(), "round-trip");
        assert_eq!(meta.blobs.len(), 2);
        assert_eq!(meta.blobs[0].blob_type, "o2-vix-dict-v1");
        assert_eq!(meta.blobs[0].offset, MAGIC_SIZE);
        assert_eq!(meta.blobs[0].length, b"dictionary bytes".len() as u64);
        // an unknown blob type id parses like any other
        assert_eq!(meta.blobs[1].blob_type, "some-future-blob-v9");
    }

    #[test]
    fn test_parse_footer_from_end_anchored_suffix() {
        let mut buf = Vec::new();
        {
            let mut writer = PuffinBytesWriter::new(&mut buf);
            writer
                .add_blob(&vec![0xAB; 512], "o2-vix-docs-v1", "docs".to_string())
                .unwrap();
            writer.finish().unwrap();
        }
        // Any suffix that covers the footer region parses identically.
        let suffix = &buf[buf.len() - 200..];
        let meta = parse_puffin_footer_from_bytes(suffix).unwrap();
        assert_eq!(meta.blobs.len(), 1);
        assert_eq!(meta.blobs[0].blob_type, "o2-vix-docs-v1");
    }

    #[test]
    fn test_parse_footer_rejects_garbage() {
        // too small
        assert!(parse_puffin_footer_from_bytes(b"tiny").is_err());
        // right size, wrong magic
        let garbage = vec![0u8; 64];
        let err = parse_puffin_footer_from_bytes(&garbage).unwrap_err();
        assert!(err.to_string().contains("MAGIC mismatch"), "{err}");
    }

    #[test]
    fn test_compressed_footer_round_trips_and_reports_decoded_size() {
        let mut plain = Vec::new();
        let mut compressed = Vec::new();
        let mut plain_payload = Vec::new();
        let mut compressed_payload = Vec::new();
        for (buf, compress, payload) in [
            (&mut plain, false, &mut plain_payload),
            (&mut compressed, true, &mut compressed_payload),
        ] {
            let mut writer = PuffinBytesWriter::new(&mut *buf);
            writer.set_footer_compression(compress);
            // a repetitive property so the frame is actually smaller
            writer.set_property("fields", "[\"a\",\"b\",\"c\"]".repeat(200));
            writer
                .add_blob(b"dictionary bytes", "o2-vix-dict-v1", "dict".to_string())
                .unwrap();
            let (_, returned) = writer.finish_with_payload().unwrap();
            *payload = returned;
        }
        // the returned payload is the uncompressed JSON either way
        assert_eq!(plain_payload, compressed_payload);
        assert!(
            compressed.len() < plain.len(),
            "{} vs {}",
            compressed.len(),
            plain.len()
        );

        let decoded_size = footer_payload_decoded_size(&compressed).unwrap();
        assert_eq!(decoded_size, plain_payload.len() as u64);
        assert_eq!(
            footer_payload_decoded_size(&plain).unwrap(),
            plain_payload.len() as u64
        );

        let meta = parse_puffin_footer_from_bytes(&compressed).unwrap();
        assert_eq!(meta, parse_puffin_footer_from_bytes(&plain).unwrap());
        assert_eq!(meta.blobs[0].blob_type, "o2-vix-dict-v1");
        assert_eq!(meta.blobs[0].compression_codec, None);
        // an end-anchored suffix parses identically for the compressed form
        let flags_at = compressed.len() - (MAGIC_SIZE + FLAGS_SIZE) as usize;
        assert_eq!(
            compressed[flags_at],
            PuffinFooterFlags::COMPRESSED.bits() as u8
        );
        let stored =
            u32::from_le_bytes(compressed[flags_at - 4..flags_at].try_into().unwrap()) as usize;
        let suffix = &compressed[compressed.len() - (stored + 16)..];
        assert_eq!(parse_puffin_footer_from_bytes(suffix).unwrap(), meta);
    }

    #[test]
    fn test_compressed_flag_over_plain_payload_is_rejected() {
        let mut buf = Vec::new();
        {
            let mut writer = PuffinBytesWriter::new(&mut buf);
            writer.finish().unwrap();
        }
        // Flip the COMPRESSED flag bit over a plain JSON payload: not a
        // zstd frame, so both the size probe and the parse refuse it.
        let flags_at = buf.len() - (MAGIC_SIZE + FLAGS_SIZE) as usize;
        buf[flags_at] |= PuffinFooterFlags::COMPRESSED.bits() as u8;
        assert!(footer_payload_decoded_size(&buf).is_err());
        let err = parse_puffin_footer_from_bytes(&buf).unwrap_err();
        assert!(err.to_string().contains("zstd"), "{err}");
    }

    #[test]
    fn test_blob_codec_declaration_round_trips() {
        let mut buf = Vec::new();
        {
            let mut writer = PuffinBytesWriter::new(&mut buf);
            writer
                .add_blob_with_codec(
                    b"already compressed",
                    "o2-vix-dict-v1",
                    "dict".to_string(),
                    Some(CompressionCodec::Zstd),
                )
                .unwrap();
            writer.finish().unwrap();
        }
        let meta = parse_puffin_footer_from_bytes(&buf).unwrap();
        assert_eq!(
            meta.blobs[0].compression_codec,
            Some(CompressionCodec::Zstd)
        );
        assert_eq!(meta.blobs[0].length, b"already compressed".len() as u64);
    }
}
