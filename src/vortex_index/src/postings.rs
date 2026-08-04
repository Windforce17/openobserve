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

//! Postings codec: delta + bitpacked doc-id lists.
//!
//! Doc ids are ascending, duplicate-free `u32`s. The blob layout is:
//!
//! - the deltas of the ids (`delta[0] = id[0]`, `delta[i] = id[i] - id[i-1]`),
//! - one *full block* per 128 deltas: `bit_width: u8` followed by `128 * bit_width / 8` bytes
//!   packed with [`bitpacking::BitPacker4x`] (whose block length is exactly 128),
//! - a *tail* of `< 128` remaining deltas, LEB128 VInt-encoded back to back.
//!
//! There is **no header**: the number of doc ids is stored externally in the
//! `doc_count` column of the `terms` table, so the blob starts directly with
//! the blocks. Decoding therefore always takes the expected `doc_count`.

use bitpacking::{BitPacker, BitPacker4x};

use crate::error::{Result, VixError};

/// Number of deltas per bitpacked block (the `BitPacker4x` block length).
pub const BLOCK_LEN: usize = BitPacker4x::BLOCK_LEN;

/// Encode an ascending, duplicate-free doc-id list into a postings blob.
///
/// Returns an error if the ids are not ascending.
pub fn encode(doc_ids: &[u32]) -> Result<Vec<u8>> {
    let packer = BitPacker4x::new();
    // 2 bytes/id is a decent guess for typical gap distributions.
    let mut out = Vec::with_capacity(doc_ids.len() * 2);
    let mut deltas = [0u32; BLOCK_LEN];
    let mut prev = 0u32;

    let full_blocks = doc_ids.len() / BLOCK_LEN;
    for block in doc_ids[..full_blocks * BLOCK_LEN].chunks_exact(BLOCK_LEN) {
        for (delta, &id) in deltas.iter_mut().zip(block) {
            *delta = id
                .checked_sub(prev)
                .ok_or_else(|| VixError::Malformed("doc ids are not ascending".to_string()))?;
            prev = id;
        }
        let num_bits = packer.num_bits(&deltas);
        out.push(num_bits);
        if num_bits > 0 {
            let packed_len = num_bits as usize * BLOCK_LEN / 8;
            let start = out.len();
            out.resize(start + packed_len, 0);
            let written = packer.compress(&deltas, &mut out[start..], num_bits);
            debug_assert_eq!(written, packed_len);
        }
    }

    for &id in &doc_ids[full_blocks * BLOCK_LEN..] {
        let delta = id
            .checked_sub(prev)
            .ok_or_else(|| VixError::Malformed("doc ids are not ascending".to_string()))?;
        prev = id;
        write_vint(&mut out, delta);
    }

    Ok(out)
}

/// Decode a postings blob of exactly `doc_count` ids, invoking `on_doc` for
/// each id in ascending order.
///
/// Errors on any truncation, on trailing garbage, on a bit width above 32 and
/// on doc-id overflow — a malformed blob never panics.
pub fn decode_each(
    blob: &[u8],
    doc_count: usize,
    mut on_doc: impl FnMut(u32) -> Result<()>,
) -> Result<()> {
    let packer = BitPacker4x::new();
    let mut deltas = [0u32; BLOCK_LEN];
    let mut pos = 0usize;
    // The first delta is relative to 0, so a running `prev` starting at 0
    // reconstructs every id as `prev + delta`.
    let mut prev = 0u32;

    let full_blocks = doc_count / BLOCK_LEN;
    for _ in 0..full_blocks {
        let num_bits = *blob.get(pos).ok_or_else(|| truncated("block bit width"))?;
        pos += 1;
        if num_bits > 32 {
            return Err(VixError::Malformed(format!(
                "postings block bit width {num_bits} exceeds 32"
            )));
        }
        if num_bits == 0 {
            deltas.fill(0);
        } else {
            let packed_len = num_bits as usize * BLOCK_LEN / 8;
            let packed = blob
                .get(pos..pos + packed_len)
                .ok_or_else(|| truncated("bitpacked block"))?;
            packer.decompress(packed, &mut deltas, num_bits);
            pos += packed_len;
        }
        for &delta in &deltas {
            prev = advance(prev, delta)?;
            on_doc(prev)?;
        }
    }

    for _ in 0..doc_count % BLOCK_LEN {
        let (delta, used) = read_vint(blob.get(pos..).unwrap_or_default())?;
        pos += used;
        prev = advance(prev, delta)?;
        on_doc(prev)?;
    }

    if pos != blob.len() {
        return Err(VixError::Malformed(format!(
            "postings blob has {} trailing bytes",
            blob.len() - pos
        )));
    }
    Ok(())
}

/// `prev + delta` with overflow checked.
fn advance(prev: u32, delta: u32) -> Result<u32> {
    prev.checked_add(delta)
        .ok_or_else(|| VixError::Malformed("postings doc id overflows u32".to_string()))
}

fn truncated(what: &str) -> VixError {
    VixError::Malformed(format!("postings blob truncated while reading {what}"))
}

/// Append a LEB128 VInt (7 payload bits per byte, high bit = continuation).
fn write_vint(out: &mut Vec<u8>, mut value: u32) {
    loop {
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

/// Read one LEB128 VInt; returns `(value, bytes_consumed)`.
fn read_vint(buf: &[u8]) -> Result<(u32, usize)> {
    let mut value = 0u32;
    for i in 0..5 {
        let byte = *buf.get(i).ok_or_else(|| truncated("vint tail"))?;
        // The 5th byte may only carry the 4 remaining payload bits and no
        // continuation flag, otherwise the value overflows u32.
        if i == 4 && byte & 0xF0 != 0 {
            return Err(VixError::Malformed(
                "postings vint overflows u32".to_string(),
            ));
        }
        value |= u32::from(byte & 0x7F) << (7 * i);
        if byte & 0x80 == 0 {
            return Ok((value, i + 1));
        }
    }
    Err(VixError::Malformed(
        "postings vint longer than 5 bytes".to_string(),
    ))
}

/// Deltas per skip entry: one entry every `SKIP_STRIDE` full blocks
/// (`8 * 128 = 1024` ids), ~0.8% size overhead on long lists.
pub const SKIP_STRIDE: usize = 8;

/// Encode an ascending, duplicate-free doc-id list as a PLIST RECORD: a
/// skip table followed by the [`encode`]d blob.
///
/// Record layout (all little-endian):
/// - `u32` skip entry count `S` (`ceil(full_blocks / SKIP_STRIDE)`; 0 when the list has no full
///   block),
/// - `S x { u32 first_doc_id, u32 blob_offset }` — entry `k` describes the first doc id of
///   block-group `k` (blocks `k*SKIP_STRIDE ..`) and the byte offset of that group inside the blob
///   region,
/// - the [`encode`]d blob.
///
/// [`rank_at`] uses the table to answer "how many ids < target" by decoding
/// at most one block group (+ the tail); [`record_blob`] recovers the plain
/// blob for full decodes. `doc_count` stays external, exactly like the
/// in-cell format.
pub fn encode_record(doc_ids: &[u32]) -> Result<Vec<u8>> {
    let blob = encode(doc_ids)?;
    let full_blocks = doc_ids.len() / BLOCK_LEN;
    let entries = full_blocks.div_ceil(SKIP_STRIDE);
    let mut out = Vec::with_capacity(4 + entries * 8 + blob.len());
    out.extend_from_slice(
        &u32::try_from(entries)
            .map_err(|_| VixError::Malformed("postings skip table overflows u32".to_string()))?
            .to_le_bytes(),
    );
    // walk the blob block headers to record group offsets without decoding
    let mut pos = 0usize;
    for block in 0..full_blocks {
        if block % SKIP_STRIDE == 0 {
            let first = doc_ids[block * BLOCK_LEN];
            out.extend_from_slice(&first.to_le_bytes());
            out.extend_from_slice(
                &u32::try_from(pos)
                    .map_err(|_| {
                        VixError::Malformed("postings blob offset overflows u32".to_string())
                    })?
                    .to_le_bytes(),
            );
        }
        let num_bits = blob[pos];
        pos += 1 + num_bits as usize * BLOCK_LEN / 8;
    }
    out.extend_from_slice(&blob);
    Ok(out)
}

/// The plain [`encode`]d blob region of a record (skip table stripped), for
/// full decodes via [`decode_each`].
pub fn record_blob(record: &[u8]) -> Result<&[u8]> {
    let entries = record_entries(record)?;
    record
        .get(4 + entries * 8..)
        .ok_or_else(|| truncated("plist record blob"))
}

/// The byte range of the record that [`rank_at`] for `target` touches —
/// `4 + S*8` header bytes plus one block group (callers with ranged sources
/// can fetch the header first, then exactly this window).
fn record_entries(record: &[u8]) -> Result<usize> {
    let head: [u8; 4] = record
        .get(..4)
        .ok_or_else(|| truncated("plist record header"))?
        .try_into()
        .unwrap();
    Ok(u32::from_le_bytes(head) as usize)
}

/// How many of the record's `doc_count` ids are `< target` — the postings
/// rank at a row cut. Decodes at most one `SKIP_STRIDE` block group plus the
/// vint tail instead of the whole list.
pub fn rank_at(record: &[u8], doc_count: usize, target: u32) -> Result<u64> {
    let entries = record_entries(record)?;
    let table = record
        .get(4..4 + entries * 8)
        .ok_or_else(|| truncated("plist skip table"))?;
    let blob = &record[4 + entries * 8..];
    let full_blocks = doc_count / BLOCK_LEN;

    let entry_first =
        |k: usize| -> u32 { u32::from_le_bytes(table[k * 8..k * 8 + 4].try_into().unwrap()) };
    let entry_offset = |k: usize| -> usize {
        u32::from_le_bytes(table[k * 8 + 4..k * 8 + 8].try_into().unwrap()) as usize
    };

    // last group whose first id is < target (all groups before it lie
    // entirely below target)
    let mut group = None;
    if entries > 0 {
        let (mut lo, mut hi) = (0usize, entries);
        while lo < hi {
            let mid = (lo + hi) / 2;
            if entry_first(mid) < target {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        // lo = first group with first_id >= target
        if lo > 0 {
            group = Some(lo - 1);
        }
    }

    let Some(group) = group else {
        if entries > 0 {
            // some full block exists and even group 0's first id (the
            // global first id) is >= target: nothing ranks below.
            return Ok(0);
        }
        // tail-only list (no full block): walk the vint tail from the start
        let mut rank = 0u64;
        let mut pos = 0usize;
        let mut prev = 0u32;
        for _ in 0..doc_count % BLOCK_LEN {
            let (delta, used) = read_vint(blob.get(pos..).unwrap_or_default())?;
            pos += used;
            prev = advance(prev, delta)?;
            if prev < target {
                rank += 1;
            } else {
                break;
            }
        }
        return Ok(rank);
    };

    // ids strictly before this group
    let mut rank = (group * SKIP_STRIDE * BLOCK_LEN) as u64;
    // walk this group's blocks (and, off its end, the remaining blocks up
    // to the tail — only reachable for the LAST group) counting ids < target
    let packer = BitPacker4x::new();
    let mut deltas = [0u32; BLOCK_LEN];
    let mut pos = entry_offset(group);
    let mut prev = if group == 0 {
        0u32
    } else {
        // reconstruct the running prev: the group's first id is absolute,
        // and the first delta of the group is relative to the LAST id of
        // the previous group — which we do not have. The encoder therefore
        // guarantees group-relative decode: delta[0] of a group is
        // first_id - prev_last... NOT available. Instead decode the group
        // using its recorded first id: the first delta reconstructs
        // first_id from prev_last, so seed prev with first_id and SKIP the
        // first delta reconstruction by tracking position.
        0
    };
    let first_block = group * SKIP_STRIDE;
    let last_block = ((group + 1) * SKIP_STRIDE).min(full_blocks);
    let mut seeded = group == 0;
    let group_first = entry_first(group);
    for _ in first_block..last_block {
        let num_bits = *blob.get(pos).ok_or_else(|| truncated("block bit width"))?;
        pos += 1;
        if num_bits > 32 {
            return Err(VixError::Malformed(format!(
                "postings block bit width {num_bits} exceeds 32"
            )));
        }
        if num_bits == 0 {
            deltas.fill(0);
        } else {
            let packed_len = num_bits as usize * BLOCK_LEN / 8;
            let packed = blob
                .get(pos..pos + packed_len)
                .ok_or_else(|| truncated("bitpacked block"))?;
            packer.decompress(packed, &mut deltas, num_bits);
            pos += packed_len;
        }
        for (i, &delta) in deltas.iter().enumerate() {
            if !seeded && i == 0 {
                // the group's first id is known absolutely; the stored
                // delta is relative to the previous group's last id
                prev = group_first;
                seeded = true;
            } else {
                prev = advance(prev, delta)?;
            }
            if prev < target {
                rank += 1;
            } else {
                return Ok(rank);
            }
        }
    }
    // the group ran to the end of the full blocks: continue into the tail
    if last_block == full_blocks {
        for _ in 0..doc_count % BLOCK_LEN {
            let (delta, used) = read_vint(blob.get(pos..).unwrap_or_default())?;
            pos += used;
            prev = advance(prev, delta)?;
            if prev < target {
                rank += 1;
            } else {
                return Ok(rank);
            }
        }
    }
    Ok(rank)
}

/// Invoke `on_doc` for every id of the record in `[start, end)`, in
/// ascending order. Jumps in via the skip table (like [`rank_at`]) and stops
/// at the first id `>= end` — a chunk-bounded decode touches only the block
/// groups overlapping the range instead of the whole list.
pub fn for_each_in_range(
    record: &[u8],
    doc_count: usize,
    start: u32,
    end: u32,
    mut on_doc: impl FnMut(u32) -> Result<()>,
) -> Result<()> {
    if start >= end || doc_count == 0 {
        return Ok(());
    }
    let entries = record_entries(record)?;
    let table = record
        .get(4..4 + entries * 8)
        .ok_or_else(|| truncated("plist skip table"))?;
    let blob = &record[4 + entries * 8..];
    let full_blocks = doc_count / BLOCK_LEN;

    let entry_first =
        |k: usize| -> u32 { u32::from_le_bytes(table[k * 8..k * 8 + 4].try_into().unwrap()) };
    let entry_offset = |k: usize| -> usize {
        u32::from_le_bytes(table[k * 8 + 4..k * 8 + 8].try_into().unwrap()) as usize
    };

    // last group whose first id is < start; None = begin at the very front
    // (group 0 or the tail-only list)
    let mut group = None;
    if entries > 0 {
        let (mut lo, mut hi) = (0usize, entries);
        while lo < hi {
            let mid = (lo + hi) / 2;
            if entry_first(mid) < start {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo > 0 {
            group = Some(lo - 1);
        }
    }

    let packer = BitPacker4x::new();
    let mut deltas = [0u32; BLOCK_LEN];
    let (first_block, mut pos, mut prev, mut seeded, group_first) = match group {
        Some(g) => (
            g * SKIP_STRIDE,
            entry_offset(g),
            0u32,
            g == 0,
            entry_first(g),
        ),
        None => (0, 0, 0u32, true, 0),
    };
    for _ in first_block..full_blocks {
        let num_bits = *blob.get(pos).ok_or_else(|| truncated("block bit width"))?;
        pos += 1;
        if num_bits > 32 {
            return Err(VixError::Malformed(format!(
                "postings block bit width {num_bits} exceeds 32"
            )));
        }
        if num_bits == 0 {
            deltas.fill(0);
        } else {
            let packed_len = num_bits as usize * BLOCK_LEN / 8;
            let packed = blob
                .get(pos..pos + packed_len)
                .ok_or_else(|| truncated("bitpacked block"))?;
            packer.decompress(packed, &mut deltas, num_bits);
            pos += packed_len;
        }
        for (i, &delta) in deltas.iter().enumerate() {
            if !seeded && i == 0 {
                // jump-in seeding: the group's first id is absolute in the
                // skip table; its stored delta is relative to the previous
                // group's last id, which a jump never decodes
                prev = group_first;
                seeded = true;
            } else {
                prev = advance(prev, delta)?;
            }
            if prev >= end {
                return Ok(());
            }
            if prev >= start {
                on_doc(prev)?;
            }
        }
    }
    // vint tail
    for _ in 0..doc_count % BLOCK_LEN {
        let (delta, used) = read_vint(blob.get(pos..).unwrap_or_default())?;
        pos += used;
        if !seeded {
            // tail-only list reached via a jump cannot happen (entries == 0
            // implies group == None which seeds at the front), but keep the
            // invariant explicit
            prev = group_first;
            seeded = true;
        } else {
            prev = advance(prev, delta)?;
        }
        if prev >= end {
            return Ok(());
        }
        if prev >= start {
            on_doc(prev)?;
        }
    }
    Ok(())
}

/// Byte length of an out-of-row postings POINTER CELL: `[u64 LE offset]
/// [u32 LE len]` addressing an [`encode_record`] region inside the `plist`
/// blob. A terms-table cell is a pointer cell exactly when the term's
/// `doc_count` is at/above the file's persisted `plist_min_docs` threshold
/// AND the cell is non-empty (dense elision keeps the empty cell even above
/// the threshold) — readers select by that predicate, never by cell bytes.
pub(crate) const POINTER_CELL_LEN: usize = 12;

/// Encode a pointer cell (see [`POINTER_CELL_LEN`]).
pub(crate) fn encode_pointer_cell(offset: u64, len: u32) -> [u8; POINTER_CELL_LEN] {
    let mut cell = [0u8; POINTER_CELL_LEN];
    cell[..8].copy_from_slice(&offset.to_le_bytes());
    cell[8..].copy_from_slice(&len.to_le_bytes());
    cell
}

/// Decode a pointer cell into `(offset, len)`. Errors unless the cell is
/// exactly [`POINTER_CELL_LEN`] bytes — the caller has already decided the
/// cell is a pointer (by the doc-count threshold), so any other length is
/// corruption.
pub(crate) fn decode_pointer_cell(cell: &[u8]) -> Result<(u64, u32)> {
    if cell.len() != POINTER_CELL_LEN {
        return Err(VixError::Malformed(format!(
            "postings pointer cell is {} bytes, expected {POINTER_CELL_LEN}",
            cell.len()
        )));
    }
    let offset = u64::from_le_bytes(cell[..8].try_into().expect("sized slice"));
    let len = u32::from_le_bytes(cell[8..].try_into().expect("sized slice"));
    Ok((offset, len))
}

#[cfg(test)]
mod tests {
    /// The record codec: `record_blob` recovers the exact [`encode`]d blob,
    /// and [`rank_at`] agrees with a naive count on every boundary shape —
    /// tail-only, exact block multiples, group boundaries, dense and sparse
    /// gaps, and targets below/at/above every id.
    #[test]
    fn test_record_rank_matches_naive_everywhere() {
        // deterministic pseudo-random gaps (no RNG dep)
        let mut state = 0x2545F4914F6CDD1Du64;
        let mut next = move |bound: u32| -> u32 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state % u64::from(bound.max(1))) as u32
        };
        let shapes: Vec<usize> = vec![
            0,
            1,
            5,
            127,
            128,
            129,
            1023,
            1024,
            1025,                     // tail/block/group edges
            128 * super::SKIP_STRIDE, // exactly one group
            128 * super::SKIP_STRIDE + 1,
            128 * super::SKIP_STRIDE * 3 + 77, // multi-group + tail
        ];
        for n in shapes {
            for dense in [true, false] {
                let mut ids = Vec::with_capacity(n);
                let mut cur = 0u32;
                for _ in 0..n {
                    cur += 1 + if dense { next(3) } else { next(50_000) };
                    ids.push(cur);
                }
                let record = encode_record(&ids).unwrap();
                // the blob region round-trips exactly
                assert_eq!(record_blob(&record).unwrap(), encode(&ids).unwrap());
                // full decode through the record agrees
                let mut decoded = Vec::new();
                decode_each(record_blob(&record).unwrap(), ids.len(), |id| {
                    decoded.push(id);
                    Ok(())
                })
                .unwrap();
                assert_eq!(decoded, ids);
                // rank at surgical targets: around every group boundary, the
                // global min/max, and random probes
                let mut targets: Vec<u32> = vec![0, 1];
                if let (Some(&first), Some(&last)) = (ids.first(), ids.last()) {
                    targets.extend([first, first + 1, last, last.saturating_add(1), u32::MAX]);
                }
                for g in (0..ids.len()).step_by(super::SKIP_STRIDE * BLOCK_LEN) {
                    let id = ids[g];
                    targets.extend([id.saturating_sub(1), id, id + 1]);
                }
                for _ in 0..50 {
                    targets.push(next(ids.last().copied().unwrap_or(10) + 1000));
                }
                for target in targets {
                    let naive = ids.iter().filter(|&&id| id < target).count() as u64;
                    let got = rank_at(&record, ids.len(), target).unwrap();
                    assert_eq!(got, naive, "n={n} dense={dense} target={target}");
                }
            }
        }
    }

    /// [`for_each_in_range`] agrees with a naive filter for every range
    /// shape: empty, single-id, group-aligned, group-spanning, tail-crossing,
    /// full-list, and beyond-the-ids ranges — on the same list shapes the
    /// rank property test covers.
    #[test]
    fn test_record_range_walk_matches_naive_everywhere() {
        let mut state = 0x9E3779B97F4A7C15u64;
        let mut next = move |bound: u32| -> u32 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state % u64::from(bound.max(1))) as u32
        };
        let shapes: Vec<usize> = vec![
            0,
            1,
            5,
            127,
            128,
            129,
            1023,
            1024,
            1025,
            128 * super::SKIP_STRIDE,
            128 * super::SKIP_STRIDE + 1,
            128 * super::SKIP_STRIDE * 3 + 77,
        ];
        for n in shapes {
            for dense in [true, false] {
                let mut ids = Vec::with_capacity(n);
                let mut cur = 0u32;
                for _ in 0..n {
                    cur += 1 + if dense { next(3) } else { next(50_000) };
                    ids.push(cur);
                }
                let record = encode_record(&ids).unwrap();
                let max = ids.last().copied().unwrap_or(0);
                // boundary-heavy cut set: group edges +-1, ends, overshoots
                let mut cuts: Vec<u32> = vec![0, 1, max, max.saturating_add(1), u32::MAX];
                for g in (0..ids.len()).step_by(super::SKIP_STRIDE * BLOCK_LEN) {
                    let id = ids[g];
                    cuts.extend([id.saturating_sub(1), id, id + 1]);
                }
                for _ in 0..30 {
                    cuts.push(next(max + 1000));
                }
                cuts.sort_unstable();
                cuts.dedup();
                for (i, &start) in cuts.iter().enumerate() {
                    for &end in &cuts[i..] {
                        let naive: Vec<u32> = ids
                            .iter()
                            .copied()
                            .filter(|&id| id >= start && id < end)
                            .collect();
                        let mut got = Vec::new();
                        for_each_in_range(&record, ids.len(), start, end, |id| {
                            got.push(id);
                            Ok(())
                        })
                        .unwrap();
                        assert_eq!(got, naive, "n={n} dense={dense} range=[{start},{end})");
                    }
                }
            }
        }
    }

    use super::*;

    fn decode_all(blob: &[u8], doc_count: usize) -> Result<Vec<u32>> {
        let mut out = Vec::with_capacity(doc_count);
        decode_each(blob, doc_count, |id| {
            out.push(id);
            Ok(())
        })?;
        Ok(out)
    }

    fn roundtrip(ids: &[u32]) {
        let blob = encode(ids).expect("encode");
        let decoded = decode_all(&blob, ids.len()).expect("decode");
        assert_eq!(decoded, ids, "roundtrip mismatch for {} ids", ids.len());
    }

    #[test]
    fn roundtrip_sizes() {
        roundtrip(&[]);
        roundtrip(&[0]);
        roundtrip(&[42]);
        roundtrip(&(0..127).collect::<Vec<_>>());
        roundtrip(&(0..128).collect::<Vec<_>>());
        roundtrip(&(0..129).collect::<Vec<_>>());
        roundtrip(&(0..1000).map(|i| i * 3 + 7).collect::<Vec<_>>());
    }

    #[test]
    fn roundtrip_100k_random_gaps() {
        // Deterministic pseudo-random ascending ids.
        let mut ids = Vec::with_capacity(100_000);
        let mut cur = 0u32;
        let mut state = 0x9E3779B9u32;
        for _ in 0..100_000 {
            state = state.wrapping_mul(1664525).wrapping_add(1013904223);
            cur = cur.wrapping_add(state % 37 + 1);
            ids.push(cur);
        }
        roundtrip(&ids);
    }

    #[test]
    fn roundtrip_adversarial() {
        // All consecutive (delta = 1 everywhere except the first).
        roundtrip(&(500..500 + 4096).collect::<Vec<_>>());
        // Huge gaps up to u32::MAX.
        roundtrip(&[0, 1, u32::MAX / 2, u32::MAX - 1, u32::MAX]);
        roundtrip(&[u32::MAX]);
        // First id 0 (first delta 0) inside a full block.
        roundtrip(&(0..256).collect::<Vec<_>>());
    }

    #[test]
    fn encode_rejects_descending() {
        assert!(encode(&[3, 2]).is_err());
        let mut ids: Vec<u32> = (0..200).collect();
        ids[150] = 10; // descending inside a full block
        assert!(encode(&ids).is_err());
    }

    #[test]
    fn decode_rejects_truncation_and_garbage() {
        let ids: Vec<u32> = (0..300).collect();
        let blob = encode(&ids).unwrap();
        // Truncated blob.
        assert!(decode_all(&blob[..blob.len() - 1], ids.len()).is_err());
        assert!(decode_all(&[], 1).is_err());
        // Trailing garbage.
        let mut garbage = blob.clone();
        garbage.push(0);
        assert!(decode_all(&garbage, ids.len()).is_err());
        // Wrong doc_count (too small => trailing bytes; too large => truncated).
        assert!(decode_all(&blob, ids.len() - 1).is_err());
        assert!(decode_all(&blob, ids.len() + 1).is_err());
    }

    #[test]
    fn decode_rejects_bad_bit_width_and_vint() {
        // A "full block" whose bit width byte is 33.
        assert!(decode_all(&[33], BLOCK_LEN).is_err());
        // A vint that never terminates within 5 bytes.
        assert!(decode_all(&[0x80, 0x80, 0x80, 0x80, 0x80], 1).is_err());
        // A 5-byte vint with too-high payload bits (would overflow u32).
        assert!(decode_all(&[0x80, 0x80, 0x80, 0x80, 0x10], 1).is_err());
    }
}
