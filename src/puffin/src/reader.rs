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

/// Parse the Puffin footer ([`PuffinMeta`]) out of in-memory puffin bytes.
///
/// `data` may be the complete file or any end-anchored suffix that covers
/// the footer region (`HeadMagic Payload PayloadSize Flags FootMagic`) — the
/// parser only looks at end-anchored offsets. Compressed footer payloads are
/// rejected: the OpenObserve writer never produces them.
pub fn parse_puffin_footer_from_bytes(data: &[u8]) -> Result<PuffinMeta> {
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
    ensure!(
        !flags.contains(PuffinFooterFlags::COMPRESSED),
        "puffin footer payload is compressed; not supported by this reader"
    );

    let mut payload_size_bytes = [0u8; 4];
    payload_size_bytes.copy_from_slice(&footer[0..FOOTER_PAYLOAD_SIZE_SIZE as usize]);
    let payload_size = i32::from_le_bytes(payload_size_bytes) as u64;

    ensure!(
        total >= FOOTER_SIZE + payload_size + MAGIC_SIZE,
        "Unexpected payload size: {payload_size} vs file size {total}"
    );

    // Payload region: HeadMagic[4] Payload[payload_size]
    let payload_start = (total - FOOTER_SIZE - payload_size - MAGIC_SIZE) as usize;
    let json_start = payload_start + MAGIC_SIZE as usize;
    ensure!(
        data[payload_start..json_start] == MAGIC,
        "Payload MAGIC mismatch"
    );
    let payload = &data[json_start..(total - FOOTER_SIZE) as usize];

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
    fn test_parse_footer_rejects_compressed_payload() {
        let mut buf = Vec::new();
        {
            let mut writer = PuffinBytesWriter::new(&mut buf);
            writer.finish().unwrap();
        }
        // Flip the COMPRESSED flag bit in the footer tail:
        // ... PayloadSize[4] Flags[4] FootMagic[4]
        let flags_at = buf.len() - (MAGIC_SIZE + FLAGS_SIZE) as usize;
        buf[flags_at] |= PuffinFooterFlags::COMPRESSED.bits() as u8;
        let err = parse_puffin_footer_from_bytes(&buf).unwrap_err();
        assert!(err.to_string().contains("compressed"), "{err}");
    }
}
