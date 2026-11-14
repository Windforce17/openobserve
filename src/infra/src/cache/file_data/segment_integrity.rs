// Copyright 2026 OpenObserve Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::{
    fs::File,
    io::{self, BufReader, Read, Seek},
    path::Path,
};

// Keep in sync with segment_wal::format's encoder. That crate depends on infra,
// so startup cannot call its decoder. Validate framing and CRCs without building
// Arrow batches or retaining IPC bodies; v1 alone needs streaming decompression.
pub(super) fn validate(path: &Path) -> io::Result<()> {
    let file = File::open(path)?;
    let size = file.metadata()?.len();
    let mut input = BufReader::new(file);
    if read_array::<4>(&mut input)? != *b"O2WS" {
        return Err(invalid("bad segment magic"));
    }
    let version = read_u16(&mut input)?;
    read_text(&mut input)?;
    read_array::<16>(&mut input)?; // sequence and creation time
    match version {
        1 => {
            let mut decoder = zstd::stream::read::Decoder::with_buffer(&mut input)?.single_frame();
            loop {
                match read_array::<1>(&mut decoder)?[0] {
                    0 => break,
                    1 => {
                        verify_frame(&mut decoder)?;
                    }
                    _ => return Err(invalid("unknown segment frame type")),
                }
            }
            if decoder.read(&mut [0])? != 0 {
                return Err(invalid("bytes after segment end frame"));
            }
            decoder.finish();
        }
        2 => verify_v2(&mut input, size)?,
        _ => return Err(invalid("unsupported segment version")),
    }
    if input.read(&mut [0])? != 0 {
        return Err(invalid("trailing segment bytes"));
    }
    Ok(())
}

fn verify_v2(input: &mut (impl Read + Seek), size: u64) -> io::Result<()> {
    let directory_len = read_u32(input)?;
    if input
        .stream_position()?
        .checked_add(u64::from(directory_len) + 4)
        .is_none_or(|end| end > size)
    {
        return Err(invalid("segment directory exceeds file size"));
    }
    let mut directory = CrcReader::new(input.by_ref().take(u64::from(directory_len)), &[]);
    let count = read_u32(&mut directory)?;
    if count > directory_len / 50 {
        return Err(invalid("segment directory count exceeds its length"));
    }
    let mut regions = Vec::new();
    let mut expected_offset = 0u64;
    for _ in 0..count {
        for _ in 0..3 {
            read_text(&mut directory)?;
        }
        read_array::<16>(&mut directory)?; // min/max timestamps
        let rows = read_u64(&mut directory)?;
        let frames = read_u32(&mut directory)?;
        let offset = read_u64(&mut directory)?;
        let len = read_u64(&mut directory)?;
        if offset != expected_offset {
            return Err(invalid("noncontiguous segment directory"));
        }
        expected_offset = offset
            .checked_add(len)
            .ok_or_else(|| invalid("segment region length overflow"))?;
        regions.push((len, frames, rows));
    }
    if directory.inner.limit() != 0 {
        return Err(invalid("trailing segment directory bytes"));
    }
    let checksum = directory.checksum();
    if read_u32(input)? != checksum {
        return Err(invalid("segment directory CRC mismatch"));
    }
    if input.stream_position()?.checked_add(expected_offset) != Some(size) {
        return Err(invalid("segment region lengths do not match file size"));
    }
    for (len, expected_frames, expected_rows) in regions {
        let mut region = input.by_ref().take(len);
        let mut frames = 0u32;
        let mut rows = 0u64;
        while region.limit() > 0 {
            if read_array::<1>(&mut region)? != [1] {
                return Err(invalid("unknown segment region frame type"));
            }
            rows = rows
                .checked_add(u64::from(verify_frame(&mut region)?))
                .ok_or_else(|| invalid("segment row count overflow"))?;
            frames = frames
                .checked_add(1)
                .ok_or_else(|| invalid("segment frame count overflow"))?;
        }
        if frames != expected_frames || rows != expected_rows {
            return Err(invalid("segment region counts do not match directory"));
        }
    }
    Ok(())
}

// The caller consumed the data-frame type byte, which is covered by the CRC.
fn verify_frame(input: &mut impl Read) -> io::Result<u32> {
    let mut frame = CrcReader::new(input, &[1]);
    for _ in 0..3 {
        read_text(&mut frame)?;
    }
    read_array::<16>(&mut frame)?;
    let rows = read_u32(&mut frame)?;
    let ipc_len = read_u32(&mut frame)?;
    if ipc_len > 1 << 31 {
        return Err(invalid("segment IPC length exceeds format limit"));
    }
    let copied = io::copy(
        &mut frame.by_ref().take(u64::from(ipc_len)),
        &mut io::sink(),
    )?;
    if copied != u64::from(ipc_len) {
        return Err(invalid("truncated segment IPC body"));
    }
    let checksum = frame.checksum();
    if read_u32(&mut frame.inner)? != checksum {
        return Err(invalid("segment frame CRC mismatch"));
    }
    Ok(rows)
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn read_array<const N: usize>(input: &mut impl Read) -> io::Result<[u8; N]> {
    let mut bytes = [0; N];
    input.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn read_u16(input: &mut impl Read) -> io::Result<u16> {
    read_array(input).map(u16::from_le_bytes)
}

fn read_u32(input: &mut impl Read) -> io::Result<u32> {
    read_array(input).map(u32::from_le_bytes)
}

fn read_u64(input: &mut impl Read) -> io::Result<u64> {
    read_array(input).map(u64::from_le_bytes)
}

fn read_text(input: &mut impl Read) -> io::Result<()> {
    let len = read_u16(input)?;
    let mut bytes = vec![0; usize::from(len)];
    input.read_exact(&mut bytes)?;
    std::str::from_utf8(&bytes).map_err(|_| invalid("invalid segment UTF-8"))?;
    Ok(())
}

// IEEE CRC32, matching crc32fast used by the encoder. A fixed lookup table
// avoids a new infra dependency and keeps work linear in bytes read.
const CRC_TABLE: [u32; 256] = {
    let mut table = [0; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = (crc >> 1) ^ (0xedb88320u32 & 0u32.wrapping_sub(crc & 1));
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
};

struct CrcReader<R> {
    inner: R,
    crc: u32,
}

impl<R> CrcReader<R> {
    fn new(inner: R, prefix: &[u8]) -> Self {
        let mut reader = Self { inner, crc: !0 };
        reader.update(prefix);
        reader
    }

    fn update(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.crc = (self.crc >> 8) ^ CRC_TABLE[((self.crc as u8) ^ byte) as usize];
        }
    }

    fn checksum(&self) -> u32 {
        !self.crc
    }
}

impl<R: Read> Read for CrcReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let len = self.inner.read(buffer)?;
        self.update(&buffer[..len]);
        Ok(len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const V1: &[u8] = include_bytes!("fixtures/segment-v1.bin");
    const V2: &[u8] = include_bytes!("fixtures/segment-v2.bin");

    fn check(bytes: &[u8]) -> io::Result<()> {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), bytes).unwrap();
        validate(file.path())
    }

    fn header_len(bytes: &[u8]) -> usize {
        24 + usize::from(u16::from_le_bytes(bytes[6..8].try_into().unwrap()))
    }

    #[test]
    fn crc_matches_ieee_check_vector() {
        let mut crc = CrcReader::new(&b"123456789"[..], &[]);
        io::copy(&mut crc, &mut io::sink()).unwrap();
        assert_eq!(crc.checksum(), 0xcbf43926);
    }

    #[test]
    fn real_encoder_versions_pass_and_every_truncated_prefix_fails() {
        for bytes in [V1, V2] {
            check(bytes).unwrap();
            for len in 0..bytes.len() {
                assert!(
                    check(&bytes[..len]).is_err(),
                    "accepted prefix length {len}"
                );
            }
            let mut trailing = bytes.to_vec();
            trailing.push(0);
            assert!(check(&trailing).is_err());
            let mut bad_magic = bytes.to_vec();
            bad_magic[0] ^= 1;
            assert!(check(&bad_magic).is_err());
            let mut bad_version = bytes.to_vec();
            bad_version[4] = 3;
            assert!(check(&bad_version).is_err());
        }
    }

    #[test]
    fn v1_checks_frame_crc_and_requires_the_end_marker() {
        let header = header_len(V1);
        let mut plain = zstd::decode_all(&V1[header..]).unwrap();
        let crc_byte = plain.len() - 2;
        plain[crc_byte] ^= 1;
        let mut corrupt = V1[..header].to_vec();
        corrupt.extend(zstd::encode_all(plain.as_slice(), 0).unwrap());
        assert!(check(&corrupt).unwrap_err().to_string().contains("CRC"));
        plain[crc_byte] ^= 1;
        assert_eq!(plain.pop(), Some(0));
        let mut missing_end = V1[..header].to_vec();
        missing_end.extend(zstd::encode_all(plain.as_slice(), 0).unwrap());
        assert!(check(&missing_end).is_err());
    }

    #[test]
    fn v2_checks_directory_and_frame_crcs() {
        let header = header_len(V2);
        let directory_len = u32::from_le_bytes(V2[header..header + 4].try_into().unwrap()) as usize;
        for offset in [header + 4 + directory_len, V2.len() - 1] {
            let mut corrupt = V2.to_vec();
            corrupt[offset] ^= 1;
            assert!(check(&corrupt).unwrap_err().to_string().contains("CRC"));
        }
    }
}
