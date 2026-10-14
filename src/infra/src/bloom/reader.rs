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

//! `.bf` file reader — footer only, **transposed (block-major) body**.
//!
//! The reader never holds the body. It parses the footer and exposes:
//!
//! 1. [`BloomReader::row_range`] — for a `(field, value)` lookup, the byte range of the **whole
//!    block row** (`M × 32` bytes, all files' block for this value) plus the hash to feed
//!    [`super::sbbf::check_block`]. One range per (field, value), shared by every file in the
//!    group.
//! 2. [`BloomReader::column_index`] — a file_id's column within that row.
//!
//! This is what makes the prune cost O(groups), not O(files): a single
//! contiguous read per group answers membership for all its files.

use std::ops::Range;

use hashbrown::HashMap;

use super::{
    ALGO_SBBF_GXHASH, MAGIC, MAGIC_V2, VERSION, VERSION_V2,
    sbbf::{BLOCK_BYTES, block_index, check_block, hash_value},
};

/// Hard cap on a v2 footer's DECODED size: column + file tables, presence
/// bitmaps and section member lists of one hour-group chunk (≤ a few
/// thousand files × a few thousand columns) stay far below it; a frame
/// header advertising more is corrupt.
const MAX_V2_FOOTER_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Debug, Clone, thiserror::Error)]
pub enum ReadError {
    #[error("blob too short: {0} bytes")]
    TooShort(usize),
    #[error("magic mismatch")]
    BadMagic,
    #[error("unsupported version: {0}")]
    BadVersion(u8),
    #[error("unsupported algo: {0}")]
    BadAlgo(u8),
    #[error("footer length out of range: footer_len={0}, blob_len={1}")]
    BadFooter(u32, usize),
    #[error("truncated field section")]
    Truncated,
    #[error("invalid num_blocks for field '{0}': {1}")]
    InvalidNumBlocks(String, u32),
    #[error("v2 footer decompression failed: {0}")]
    BadFooterCompression(String),
    #[error("v2 footer is internally inconsistent: {0}")]
    BadFooterShape(String),
}

#[derive(Debug)]
struct FieldSection {
    /// uniform block count for every file in this field
    num_blocks: u32,
    /// number of files (columns) = row width / 32
    file_count: u32,
    /// absolute offset of this field's transposed matrix in the file
    body_offset: u64,
    /// file_id → column index
    by_file: HashMap<u64, usize>,
    /// column order (col i ↔ file_ids[i]); kept for inspect/debug and the
    /// consolidation de-transpose
    file_ids: Vec<u64>,
    /// per column: distinct values inserted (informational, carried back
    /// into a rebuilt chunk)
    n_items: Vec<u32>,
}

/// v2 group metadata: the group file table, the column table and the
/// per-file presence bitmaps (DESIGN-COLD-MISS §4 B1/B2). Empty for a v1
/// blob, whose footer carries no presence information.
#[derive(Debug, Default)]
struct GroupFooter {
    /// file_id → group-file index (index into `presence`).
    file_pos: HashMap<u64, usize>,
    /// column name → column index (bit position inside a presence bitmap).
    column_pos: HashMap<String, usize>,
    /// one presence bitmap per group file (group-file order), each
    /// `column_count.div_ceil(8)` bytes.
    presence: Vec<Vec<u8>>,
    /// per group file: whether its presence bits are AUTHORITATIVE (the
    /// data object's `columns_complete` license). An unlicensed file's bits
    /// answer "no info".
    complete: Vec<bool>,
    column_count: usize,
}

impl GroupFooter {
    /// `Some(true)` = the column is present; `Some(false)` = the file is
    /// presence-authoritative and the column is NOT one of its columns
    /// (absent from the group table or bit clear); `None` = no info (unknown
    /// file, or a file without the license).
    fn file_has_column(&self, file_id: u64, column: &str) -> Option<bool> {
        let &pos = self.file_pos.get(&file_id)?;
        if !self.complete.get(pos).copied().unwrap_or(false) {
            return None;
        }
        let Some(&col) = self.column_pos.get(column) else {
            return Some(false);
        };
        let bitmap = self.presence.get(pos)?;
        let byte = bitmap.get(col / 8)?;
        Some(byte & (1 << (col % 8)) != 0)
    }

    fn has_column(&self, column: &str) -> bool {
        self.column_pos.contains_key(column)
    }
}

/// Parsed `.bf` footer. Body stays in object store; callers fetch one
/// block-row per `(field, value)` via [`Self::row_range`].
pub struct BloomReader {
    by_field: HashMap<String, FieldSection>,
    /// v2 group metadata; `None` for a v1 blob.
    group: Option<GroupFooter>,
}

impl std::fmt::Debug for BloomReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BloomReader")
            .field("field_count", &self.by_field.len())
            .field("v2", &self.group.is_some())
            .finish()
    }
}

impl BloomReader {
    /// Parse the footer from a full `.bf` blob (tests / whole-file callers).
    pub fn parse(blob: &[u8]) -> Result<Self, ReadError> {
        Self::parse_suffix(blob, blob.len() as u64)
    }

    /// Parse from a trailing suffix of the file. `total_size` is the full
    /// `.bf` length; `suffix` is its last `suffix.len()` bytes (must cover
    /// the tail magic + footer_len + footer payload).
    ///
    /// Dispatches on the tail magic: `"O2BF"` → v1 (plaintext footer),
    /// `"O2B2"` → v2 (zstd footer). A v1 reader (this code before the v2
    /// change) fails on the `"O2B2"` tail and keeps every file — the B5
    /// fail-open contract.
    pub fn parse_suffix(suffix: &[u8], total_size: u64) -> Result<Self, ReadError> {
        if suffix.len() < 4 + 1 + 4 + 4 + 4 {
            return Err(ReadError::TooShort(suffix.len()));
        }
        let n = suffix.len();
        let tail: [u8; 4] = suffix[n - 4..].try_into().unwrap();
        if &tail == MAGIC_V2 {
            return Self::parse_v2_suffix(suffix, total_size);
        }
        if &tail != MAGIC {
            return Err(ReadError::BadMagic);
        }

        let total = total_size as usize;
        let suffix_covers_head = suffix.len() >= total;
        if suffix_covers_head {
            if &suffix[..4] != MAGIC {
                return Err(ReadError::BadMagic);
            }
            if suffix[4] != VERSION {
                return Err(ReadError::BadVersion(suffix[4]));
            }
        }

        let footer_len = u32::from_le_bytes(suffix[n - 8..n - 4].try_into().unwrap()) as usize;
        if footer_len + 8 > n {
            return Err(ReadError::BadFooter(footer_len as u32, n));
        }
        let footer_start = n - 8 - footer_len;

        let mut p = footer_start;
        let field_count = read_u32(suffix, &mut p)? as usize;

        let mut by_field: HashMap<String, FieldSection> = HashMap::with_capacity(field_count);
        for _ in 0..field_count {
            let name_len = read_u16(suffix, &mut p)? as usize;
            if p + name_len > n {
                return Err(ReadError::Truncated);
            }
            let name = std::str::from_utf8(&suffix[p..p + name_len])
                .map_err(|_| ReadError::Truncated)?
                .to_string();
            p += name_len;
            let algo = read_u8(suffix, &mut p)?;
            if algo != ALGO_SBBF_GXHASH {
                return Err(ReadError::BadAlgo(algo));
            }
            let num_blocks = read_u32(suffix, &mut p)?;
            if num_blocks == 0 {
                return Err(ReadError::InvalidNumBlocks(name.clone(), num_blocks));
            }
            let file_count = read_u32(suffix, &mut p)?;
            let body_offset = read_u64(suffix, &mut p)?;

            // Validate the matrix fits in the full file (before the footer).
            let matrix_bytes = (num_blocks as u64)
                .checked_mul(file_count as u64)
                .and_then(|v| v.checked_mul(BLOCK_BYTES as u64));
            match matrix_bytes {
                Some(mb)
                    if body_offset
                        .checked_add(mb)
                        .map(|end| end as usize <= total - footer_len - 8)
                        .unwrap_or(false) => {}
                _ => return Err(ReadError::Truncated),
            }

            let mut by_file: HashMap<u64, usize> = HashMap::with_capacity(file_count as usize);
            let mut file_ids: Vec<u64> = Vec::with_capacity(file_count as usize);
            let mut members_n_items: Vec<u32> = Vec::with_capacity(file_count as usize);
            for col in 0..file_count as usize {
                let file_id = read_u64(suffix, &mut p)?;
                let n_items = read_u32(suffix, &mut p)?;
                by_file.insert(file_id, col);
                file_ids.push(file_id);
                members_n_items.push(n_items);
            }
            by_field.insert(
                name,
                FieldSection {
                    num_blocks,
                    file_count,
                    body_offset,
                    by_file,
                    file_ids,
                    n_items: members_n_items,
                },
            );
        }
        if p != n - 8 {
            return Err(ReadError::Truncated);
        }

        Ok(Self {
            by_field,
            group: None,
        })
    }

    /// Parse the v2 footer (zstd payload behind the `"O2B2"` tail).
    fn parse_v2_suffix(suffix: &[u8], total_size: u64) -> Result<Self, ReadError> {
        let n = suffix.len();
        let total = total_size as usize;
        // Head check only when the suffix covers the whole blob (the same
        // rule as v1): a short probe never sees the head.
        if suffix.len() >= total {
            if &suffix[..4] != MAGIC {
                return Err(ReadError::BadMagic);
            }
            if suffix[4] != VERSION_V2 {
                return Err(ReadError::BadVersion(suffix[4]));
            }
        }
        let footer_len = u32::from_le_bytes(suffix[n - 8..n - 4].try_into().unwrap()) as usize;
        if footer_len + 8 > n {
            return Err(ReadError::BadFooter(footer_len as u32, n));
        }
        let footer_start = n - 8 - footer_len;
        let frame = &suffix[footer_start..n - 8];
        // the writer compresses single-shot, so the frame header carries the
        // decoded size; bound it so a corrupt header cannot drive the
        // allocation (a footer is names + bitmaps + section tables)
        let decoded = zstd::zstd_safe::get_frame_content_size(frame)
            .map_err(|e| ReadError::BadFooterCompression(e.to_string()))?
            .ok_or_else(|| {
                ReadError::BadFooterCompression("frame lacks a content size".to_string())
            })?;
        if decoded > MAX_V2_FOOTER_BYTES {
            return Err(ReadError::BadFooterCompression(format!(
                "footer advertises {decoded} decoded bytes (max {MAX_V2_FOOTER_BYTES})"
            )));
        }
        let payload = zstd::bulk::decompress(frame, decoded as usize)
            .map_err(|e| ReadError::BadFooterCompression(e.to_string()))?;
        Self::parse_v2_payload(&payload, total)
    }

    /// Parse the decompressed v2 footer payload.
    fn parse_v2_payload(payload: &[u8], total: usize) -> Result<Self, ReadError> {
        let n = payload.len();
        let bad = |msg: String| ReadError::BadFooterShape(msg);
        let mut p = 0usize;
        let field_count = read_u32(payload, &mut p)? as usize;
        let column_count = read_u32(payload, &mut p)? as usize;

        let mut columns: Vec<String> = Vec::with_capacity(column_count.min(4096));
        let mut column_pos: HashMap<String, usize> = HashMap::with_capacity(column_count.min(4096));
        for i in 0..column_count {
            let name_len = read_u16(payload, &mut p)? as usize;
            if p + name_len > n {
                return Err(ReadError::Truncated);
            }
            let name = std::str::from_utf8(&payload[p..p + name_len])
                .map_err(|_| ReadError::Truncated)?
                .to_string();
            p += name_len;
            column_pos.insert(name.clone(), i);
            columns.push(name);
        }

        let file_count = read_u32(payload, &mut p)? as usize;
        let mut file_ids: Vec<u64> = Vec::with_capacity(file_count.min(65536));
        let mut file_pos: HashMap<u64, usize> = HashMap::with_capacity(file_count.min(65536));
        for i in 0..file_count {
            let file_id = read_u64(payload, &mut p)?;
            file_pos.insert(file_id, i);
            file_ids.push(file_id);
        }
        let mut complete: Vec<bool> = Vec::with_capacity(file_count.min(65536));
        for _ in 0..file_count {
            let flags = read_u8(payload, &mut p)?;
            complete.push(flags & super::writer::MEMBER_FLAG_COMPLETE != 0);
        }
        let bitmap_bytes = column_count.div_ceil(8);
        let mut presence: Vec<Vec<u8>> = Vec::with_capacity(file_count.min(65536));
        for _ in 0..file_count {
            if p + bitmap_bytes > n {
                return Err(ReadError::Truncated);
            }
            presence.push(payload[p..p + bitmap_bytes].to_vec());
            p += bitmap_bytes;
        }

        let mut by_field: HashMap<String, FieldSection> = HashMap::with_capacity(field_count);
        for _ in 0..field_count {
            let name_len = read_u16(payload, &mut p)? as usize;
            if p + name_len > n {
                return Err(ReadError::Truncated);
            }
            let name = std::str::from_utf8(&payload[p..p + name_len])
                .map_err(|_| ReadError::Truncated)?
                .to_string();
            p += name_len;
            let algo = read_u8(payload, &mut p)?;
            if algo != ALGO_SBBF_GXHASH {
                return Err(ReadError::BadAlgo(algo));
            }
            let num_blocks = read_u32(payload, &mut p)?;
            if num_blocks == 0 {
                return Err(ReadError::InvalidNumBlocks(name.clone(), num_blocks));
            }
            let body_offset = read_u64(payload, &mut p)?;
            let member_count = read_u32(payload, &mut p)?;

            let mut by_file: HashMap<u64, usize> = HashMap::with_capacity(member_count as usize);
            let mut members: Vec<u64> = Vec::with_capacity(member_count as usize);
            let mut members_n_items: Vec<u32> = Vec::with_capacity(member_count as usize);
            for col in 0..member_count as usize {
                let file_index = read_u32(payload, &mut p)? as usize;
                let n_items = read_u32(payload, &mut p)?;
                let file_id = *file_ids.get(file_index).ok_or_else(|| {
                    bad(format!(
                        "field '{name}' member file_index {file_index} out of range ({file_count})"
                    ))
                })?;
                by_file.insert(file_id, col);
                members.push(file_id);
                members_n_items.push(n_items);
            }

            // Validate the matrix fits in the full file, before the footer
            // region. v2's footer is compressed, so the on-disk footer length
            // is unknown here; the caller's `total` bounds the body start and
            // `matrix_bytes` bounds the end of the matrix within `total`.
            let matrix_bytes = (num_blocks as u64)
                .checked_mul(member_count as u64)
                .and_then(|v| v.checked_mul(BLOCK_BYTES as u64));
            match matrix_bytes {
                Some(mb)
                    if body_offset
                        .checked_add(mb)
                        .map(|end| end as usize <= total)
                        .unwrap_or(false) => {}
                _ => return Err(ReadError::Truncated),
            }

            by_field.insert(
                name,
                FieldSection {
                    num_blocks,
                    file_count: member_count,
                    body_offset,
                    by_file,
                    file_ids: members,
                    n_items: members_n_items,
                },
            );
        }
        if p != n {
            return Err(ReadError::Truncated);
        }

        Ok(Self {
            by_field,
            group: Some(GroupFooter {
                file_pos,
                column_pos,
                presence,
                complete,
                column_count,
            }),
        })
    }

    /// True when this blob is a v2 footer carrying a presence block.
    pub fn is_v2(&self) -> bool {
        self.group.is_some()
    }

    /// True when the group's column table lists `name` at all (i.e. at least
    /// one member file carries the column). v1 blobs return `false` — they
    /// carry no column table, so callers must keep every file (no info).
    pub fn has_column(&self, name: &str) -> bool {
        self.group.as_ref().is_some_and(|g| g.has_column(name))
    }

    /// The group column count (v2 only; 0 for v1).
    pub fn column_count(&self) -> usize {
        self.group.as_ref().map_or(0, |g| g.column_count)
    }

    /// Whether `file_id` has column `name` (DESIGN-COLD-MISS §4 B2):
    /// `Some(true)` present; `Some(false)` the file's column set is
    /// AUTHORITATIVE (its data object carries `columns_complete`) and the
    /// column is not in it — absent from the group table or bit clear — so a
    /// predicate on it can match no row of the file; `None` = no info (v1
    /// blob, unknown file, or a file without the license) — the caller keeps
    /// the file.
    pub fn file_has_column(&self, file_id: u64, name: &str) -> Option<bool> {
        self.group.as_ref()?.file_has_column(file_id, name)
    }

    /// Whether `field`'s section in this group covers `file_id` — i.e. the
    /// file is a member of that field's section. `false` for an unknown
    /// field or a non-member file.
    pub fn section_covers_file(&self, field: &str, file_id: u64) -> bool {
        self.by_field
            .get(field)
            .is_some_and(|s| s.by_file.contains_key(&file_id))
    }

    /// Every file id in the v2 group file table (empty for v1).
    pub fn group_files(&self) -> impl Iterator<Item = u64> + '_ {
        self.group.iter().flat_map(|g| g.file_pos.keys().copied())
    }

    /// A v2 member's presence declaration as the writer took it: its column
    /// names (bits set) and whether they are authoritative — what a
    /// consolidation re-emits for the file. `None` for a v1 blob or an
    /// unknown file.
    pub fn member_columns(&self, file_id: u64) -> Option<(Vec<String>, bool)> {
        let group = self.group.as_ref()?;
        let &pos = group.file_pos.get(&file_id)?;
        let bitmap = group.presence.get(pos)?;
        let complete = group.complete.get(pos).copied().unwrap_or(false);
        let mut columns: Vec<String> = group
            .column_pos
            .iter()
            .filter(|(_, col)| {
                let col = **col;
                bitmap
                    .get(col / 8)
                    .is_some_and(|byte| byte & (1 << (col % 8)) != 0)
            })
            .map(|(name, _)| name.clone())
            .collect();
        columns.sort();
        Some((columns, complete))
    }

    pub fn field_count(&self) -> usize {
        self.by_field.len()
    }

    /// Heap footprint of the parsed footer (the DECODED form — what a cache
    /// holding this reader pays): per section its name, column map and file
    /// list; for v2 the file table, column table and presence bitmaps.
    pub fn memory_size(&self) -> usize {
        const ENTRY: usize = std::mem::size_of::<(u64, usize)>() + 8; // hashbrown ctrl + slot
        let mut bytes = std::mem::size_of::<Self>();
        for (name, section) in &self.by_field {
            bytes += name.capacity()
                + std::mem::size_of::<FieldSection>()
                + section.by_file.capacity() * ENTRY
                + section.file_ids.capacity() * std::mem::size_of::<u64>();
        }
        if let Some(group) = &self.group {
            bytes += group.file_pos.capacity() * ENTRY
                + group
                    .column_pos
                    .iter()
                    .map(|(name, _)| name.capacity() + std::mem::size_of::<(String, usize)>() + 8)
                    .sum::<usize>()
                + group
                    .presence
                    .iter()
                    .map(|bitmap| bitmap.capacity() + std::mem::size_of::<Vec<u8>>())
                    .sum::<usize>()
                + group.complete.capacity();
        }
        bytes
    }

    pub fn fields(&self) -> impl Iterator<Item = &str> {
        self.by_field.keys().map(String::as_str)
    }

    /// True iff this footer carries `(field, file_id)`.
    pub fn has_entry(&self, field: &str, file_id: u64) -> bool {
        self.by_field
            .get(field)
            .is_some_and(|s| s.by_file.contains_key(&file_id))
    }

    /// Byte range of the block row that answers `value` for **every file**
    /// in this field, plus the hash for [`check_block_with_hash`].
    ///
    /// The returned range is exactly `file_count × 32` bytes. Use
    /// [`Self::column_index`] to locate a specific file's 32-byte block
    /// inside the fetched row. Returns `None` if the field is absent.
    pub fn row_range(&self, field: &str, value: &[u8]) -> Option<(Range<u64>, u64)> {
        let section = self.by_field.get(field)?;
        let h = hash_value(value);
        let bi = block_index(h, section.num_blocks) as u64;
        let row_bytes = section.file_count as u64 * BLOCK_BYTES as u64;
        let start = section.body_offset + bi * row_bytes;
        Some((start..start + row_bytes, h))
    }

    /// Column index of `file_id` within `field`'s rows (None if absent).
    pub fn column_index(&self, field: &str, file_id: u64) -> Option<usize> {
        self.by_field.get(field)?.by_file.get(&file_id).copied()
    }

    /// Run the 8-bit SBBF check on a single 32-byte block.
    #[inline]
    pub fn check_block_with_hash(block: &[u8; BLOCK_BYTES], hash: u64) -> bool {
        check_block(block, hash)
    }

    /// The body layout of every section, for a consolidation that reads a
    /// chunk's whole body and splits it back into per-file filters
    /// (DESIGN-COLD-MISS §4 B4): `(field, num_blocks, body_offset, members in
    /// column order with their n_items)`.
    pub fn section_layouts(&self) -> Vec<SectionLayout<'_>> {
        let mut out: Vec<SectionLayout<'_>> = self
            .by_field
            .iter()
            .map(|(name, s)| SectionLayout {
                field: name.as_str(),
                num_blocks: s.num_blocks,
                body_offset: s.body_offset,
                file_ids: &s.file_ids,
                n_items: &s.n_items,
            })
            .collect();
        out.sort_by_key(|layout| layout.body_offset);
        out
    }

    /// Split one section's transposed matrix (`matrix` = exactly the bytes at
    /// `body_offset .. body_offset + num_blocks × members × 32` of the
    /// object) back into per-member filter bodies, in column order: member
    /// `c`'s body is the concatenation over rows `j` of block `(j, c)`.
    pub fn detranspose_section(layout: &SectionLayout<'_>, matrix: &[u8]) -> Option<Vec<Vec<u8>>> {
        let members = layout.file_ids.len();
        let rows = layout.num_blocks as usize;
        let row_bytes = members * BLOCK_BYTES;
        if matrix.len() != rows * row_bytes {
            return None;
        }
        let mut out: Vec<Vec<u8>> = (0..members)
            .map(|_| Vec::with_capacity(rows * BLOCK_BYTES))
            .collect();
        for j in 0..rows {
            let row = &matrix[j * row_bytes..(j + 1) * row_bytes];
            for (c, body) in out.iter_mut().enumerate() {
                body.extend_from_slice(&row[c * BLOCK_BYTES..(c + 1) * BLOCK_BYTES]);
            }
        }
        Some(out)
    }

    /// Per-field inspection snapshot (CLI / debug logs).
    pub fn inspect(&self) -> Vec<FieldInspect> {
        let mut out: Vec<FieldInspect> = self
            .by_field
            .iter()
            .map(|(name, s)| FieldInspect {
                field: name.clone(),
                file_count: s.file_count as usize,
                num_blocks: s.num_blocks,
                row_bytes: s.file_count as u64 * BLOCK_BYTES as u64,
                total_body_bytes: s.num_blocks as u64 * s.file_count as u64 * BLOCK_BYTES as u64,
                file_ids: {
                    let mut v = s.file_ids.clone();
                    v.sort_unstable();
                    v
                },
            })
            .collect();
        out.sort_by(|a, b| a.field.cmp(&b.field));
        out
    }
}

/// One section's body layout (see [`BloomReader::section_layouts`]).
#[derive(Debug, Clone)]
pub struct SectionLayout<'a> {
    pub field: &'a str,
    pub num_blocks: u32,
    pub body_offset: u64,
    /// Members in COLUMN order.
    pub file_ids: &'a [u64],
    /// Per member (same order): distinct values inserted.
    pub n_items: &'a [u32],
}

impl SectionLayout<'_> {
    /// Byte length of the section's transposed matrix.
    pub fn matrix_bytes(&self) -> u64 {
        self.num_blocks as u64 * self.file_ids.len() as u64 * BLOCK_BYTES as u64
    }
}

/// Per-field summary returned by [`BloomReader::inspect`].
#[derive(Debug, Clone)]
pub struct FieldInspect {
    pub field: String,
    pub file_count: usize,
    pub num_blocks: u32,
    /// bytes read per query for this field (= `file_count × 32`)
    pub row_bytes: u64,
    pub total_body_bytes: u64,
    pub file_ids: Vec<u64>,
}

#[inline]
fn read_u8(buf: &[u8], p: &mut usize) -> Result<u8, ReadError> {
    if *p + 1 > buf.len() {
        return Err(ReadError::Truncated);
    }
    let v = buf[*p];
    *p += 1;
    Ok(v)
}

#[inline]
fn read_u16(buf: &[u8], p: &mut usize) -> Result<u16, ReadError> {
    if *p + 2 > buf.len() {
        return Err(ReadError::Truncated);
    }
    let v = u16::from_le_bytes(buf[*p..*p + 2].try_into().unwrap());
    *p += 2;
    Ok(v)
}

#[inline]
fn read_u32(buf: &[u8], p: &mut usize) -> Result<u32, ReadError> {
    if *p + 4 > buf.len() {
        return Err(ReadError::Truncated);
    }
    let v = u32::from_le_bytes(buf[*p..*p + 4].try_into().unwrap());
    *p += 4;
    Ok(v)
}

#[inline]
fn read_u64(buf: &[u8], p: &mut usize) -> Result<u64, ReadError> {
    if *p + 8 > buf.len() {
        return Err(ReadError::Truncated);
    }
    let v = u64::from_le_bytes(buf[*p..*p + 8].try_into().unwrap());
    *p += 8;
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::{
        super::writer::{BloomBuilder, BloomWriter},
        *,
    };

    /// Build a transposed blob: 2 files share field "trace_id" (B=8),
    /// 1 file has "user_id".
    fn build_blob() -> Vec<u8> {
        let mut b = BloomBuilder::new();
        let i1 = b.begin_with_blocks(101, "trace_id", 8);
        b.insert(i1, b"abc");
        b.insert(i1, b"def");
        let i2 = b.begin_with_blocks(102, "trace_id", 8);
        b.insert(i2, b"ghi");
        let i3 = b.begin_with_blocks(101, "user_id", 8);
        b.insert(i3, b"u-1");
        BloomWriter::serialize(b.finish()).expect("write succeeds")
    }

    /// Check membership the way the pruner does: row_range → slice the
    /// file's column → check_block.
    fn check(blob: &[u8], r: &BloomReader, field: &str, file_id: u64, v: &[u8]) -> Option<bool> {
        let (range, h) = r.row_range(field, v)?;
        let col = r.column_index(field, file_id)?;
        let row = &blob[range.start as usize..range.end as usize];
        let off = col * BLOCK_BYTES;
        let block: &[u8; BLOCK_BYTES] = row[off..off + BLOCK_BYTES].try_into().unwrap();
        Some(BloomReader::check_block_with_hash(block, h))
    }

    #[test]
    fn test_round_trip_membership() {
        let blob = build_blob();
        let r = BloomReader::parse(&blob).unwrap();
        assert_eq!(r.field_count(), 2);

        assert_eq!(check(&blob, &r, "trace_id", 101, b"abc"), Some(true));
        assert_eq!(check(&blob, &r, "trace_id", 101, b"def"), Some(true));
        assert_eq!(check(&blob, &r, "trace_id", 102, b"ghi"), Some(true));
        assert_eq!(check(&blob, &r, "user_id", 101, b"u-1"), Some(true));
        // file 102 does not contain abc/def
        assert_eq!(check(&blob, &r, "trace_id", 102, b"abc"), Some(false));
        assert_eq!(check(&blob, &r, "trace_id", 101, b"NOPE_xyz"), Some(false));
    }

    #[test]
    fn test_row_shared_across_files() {
        // The same value maps to the SAME row for every file in the field —
        // this is the property that lets the pruner read once per group.
        let blob = build_blob();
        let r = BloomReader::parse(&blob).unwrap();
        let (range_a, _) = r.row_range("trace_id", b"abc").unwrap();
        // file 101 and 102 share the row; only the column differs.
        assert!(r.column_index("trace_id", 101).is_some());
        assert!(r.column_index("trace_id", 102).is_some());
        assert_ne!(
            r.column_index("trace_id", 101),
            r.column_index("trace_id", 102)
        );
        // row width = file_count(2) × 32 = 64
        assert_eq!(range_a.end - range_a.start, 2 * BLOCK_BYTES as u64);
    }

    #[test]
    fn test_unknown_field_or_file_none() {
        let blob = build_blob();
        let r = BloomReader::parse(&blob).unwrap();
        assert!(r.row_range("nonexistent", b"x").is_none());
        assert!(r.column_index("trace_id", 999_999).is_none());
    }

    #[test]
    fn test_parse_suffix_round_trip() {
        let blob = build_blob();
        let total = blob.len();
        // suffix covering footer only (body is small here, but test the path)
        let probe = 512.min(total);
        let suffix = &blob[total - probe..];
        let r = BloomReader::parse_suffix(suffix, total as u64).unwrap();
        let (range, h) = r.row_range("trace_id", b"abc").unwrap();
        let col = r.column_index("trace_id", 101).unwrap();
        let row = &blob[range.start as usize..range.end as usize];
        let off = col * BLOCK_BYTES;
        let block: &[u8; BLOCK_BYTES] = row[off..off + BLOCK_BYTES].try_into().unwrap();
        assert!(BloomReader::check_block_with_hash(block, h));
    }

    #[test]
    fn test_bad_version_rejected() {
        let mut blob = build_blob();
        blob[4] = 0xFF;
        assert!(matches!(
            BloomReader::parse(&blob).unwrap_err(),
            ReadError::BadVersion(0xFF)
        ));
    }

    #[test]
    fn test_observed_fpr_within_bounds() {
        let mut b = BloomBuilder::new();
        let nb = super::super::sbbf::num_blocks_for(1000, 0.01);
        let idx = b.begin_with_blocks(7, "trace_id", nb);
        for i in 0..1000u32 {
            b.insert(idx, format!("present-{i}").as_bytes());
        }
        let blob = BloomWriter::serialize(b.finish()).unwrap();
        let r = BloomReader::parse(&blob).unwrap();
        let mut fp = 0;
        for i in 0..10_000u32 {
            if check(&blob, &r, "trace_id", 7, format!("absent-{i}").as_bytes()).unwrap_or(true) {
                fp += 1;
            }
        }
        assert!(fp < 500, "FPR > 5%: {fp}/10000");
    }

    /// DESIGN-COLD-MISS §4 B1: a v2 blob round-trips membership exactly
    /// like v1 (same transposed body), parses from a tail-suffix probe, and
    /// its footer answers the presence questions the pruner asks: the
    /// column table, per-file presence bits, section membership, and
    /// "no info" for unknown files/columns. A member with no bloom section
    /// at all still has presence bits.
    #[test]
    fn test_v2_round_trip_presence_and_membership() {
        use super::super::writer::GroupMember;

        let mut b = BloomBuilder::new();
        let i1 = b.begin_with_blocks(101, "trace_id", 8);
        b.insert(i1, b"abc");
        b.insert(i1, b"def");
        let i2 = b.begin_with_blocks(102, "trace_id", 8);
        b.insert(i2, b"ghi");
        let i3 = b.begin_with_blocks(101, "user_id", 8);
        b.insert(i3, b"u-1");
        let members = vec![
            GroupMember::new(101, vec!["trace_id".into(), "user_id".into(), "msg".into()]),
            GroupMember::new(102, vec!["trace_id".into(), "msg".into()]),
            // a member with columns but no bloom at all
            GroupMember::new(103, vec!["msg".into(), "level".into()]),
            // a member WITHOUT the columns_complete license: its bits are
            // written but never authoritative
            GroupMember::new(104, vec!["msg".into()]).with_complete(false),
        ];
        let blob = BloomWriter::serialize_v2(&members, b.finish()).unwrap();
        assert_eq!(&blob[..4], MAGIC);
        assert_eq!(blob[4], VERSION_V2);
        assert_eq!(&blob[blob.len() - 4..], MAGIC_V2);

        let r = BloomReader::parse(&blob).unwrap();
        assert!(r.is_v2());
        assert_eq!(r.field_count(), 2);
        assert_eq!(check(&blob, &r, "trace_id", 101, b"abc"), Some(true));
        assert_eq!(check(&blob, &r, "trace_id", 102, b"ghi"), Some(true));
        assert_eq!(check(&blob, &r, "trace_id", 102, b"abc"), Some(false));
        assert_eq!(check(&blob, &r, "user_id", 101, b"u-1"), Some(true));
        assert_eq!(
            check(&blob, &r, "user_id", 102, b"u-1"),
            None,
            "not a member"
        );

        // presence: column table = union; bits per file; unknown = None
        assert_eq!(r.column_count(), 4);
        for column in ["trace_id", "user_id", "msg", "level"] {
            assert!(r.has_column(column), "{column}");
        }
        assert!(!r.has_column("absent_everywhere"));
        assert_eq!(r.file_has_column(101, "user_id"), Some(true));
        assert_eq!(r.file_has_column(102, "user_id"), Some(false));
        assert_eq!(r.file_has_column(103, "level"), Some(true));
        assert_eq!(r.file_has_column(103, "trace_id"), Some(false));
        assert_eq!(r.file_has_column(999, "msg"), None, "unknown file");
        assert_eq!(
            r.file_has_column(101, "nope"),
            Some(false),
            "a column outside the group table is absent from a complete file"
        );
        assert_eq!(r.file_has_column(104, "msg"), None, "unlicensed: no info");
        assert_eq!(
            r.file_has_column(104, "trace_id"),
            None,
            "unlicensed: no info"
        );
        assert!(r.section_covers_file("trace_id", 102));
        assert!(!r.section_covers_file("trace_id", 103));
        assert!(!r.section_covers_file("user_id", 102));
        let mut files: Vec<u64> = r.group_files().collect();
        files.sort();
        assert_eq!(files, vec![101, 102, 103, 104]);

        // tail-suffix probe: the footer parses without the head
        let total = blob.len();
        let probe = 256.min(total);
        let suffix = &blob[total - probe..];
        let from_suffix = BloomReader::parse_suffix(suffix, total as u64).unwrap();
        assert!(from_suffix.is_v2());
        assert_eq!(
            from_suffix.row_range("trace_id", b"abc"),
            r.row_range("trace_id", b"abc")
        );
        assert_eq!(from_suffix.file_has_column(102, "user_id"), Some(false));

        // a v1 blob answers "no info" to every presence question
        let v1 = build_blob();
        let r1 = BloomReader::parse(&v1).unwrap();
        assert!(!r1.is_v2());
        assert!(!r1.has_column("trace_id"));
        assert_eq!(r1.file_has_column(101, "trace_id"), None);
        assert_eq!(r1.column_count(), 0);
    }

    /// B5: the v2 tail magic is what makes a v1 reader fail OPEN. The v1
    /// parse path checks the tail magic before anything else, so handing it
    /// a v2 blob is a deterministic `BadMagic` (the pruner keeps every
    /// file) — never a mis-parse of the compressed footer. Pinned by
    /// forcing the v1 path on a v2 tail.
    #[test]
    fn test_v2_tail_is_rejected_by_the_v1_parse_path() {
        use super::super::writer::GroupMember;

        let mut b = BloomBuilder::new();
        let i1 = b.begin_with_blocks(7, "trace_id", 8);
        b.insert(i1, b"abc");
        let blob =
            BloomWriter::serialize_v2(&[GroupMember::new(7, vec!["trace_id".into()])], b.finish())
                .unwrap();
        assert_ne!(MAGIC, MAGIC_V2);
        // the v1 reader's first check was `suffix[n-4..] == MAGIC`
        let n = blob.len();
        assert_ne!(&blob[n - 4..], MAGIC);
        // and a corrupt v2 tail (unknown magic) is BadMagic on the new one
        let mut unknown = blob.clone();
        unknown[n - 4..].copy_from_slice(b"O2B9");
        assert!(matches!(
            BloomReader::parse(&unknown).unwrap_err(),
            ReadError::BadMagic
        ));
        // the v2 version byte is checked when the head is in view
        let mut bad_version = blob.clone();
        bad_version[4] = VERSION;
        assert!(matches!(
            BloomReader::parse(&bad_version).unwrap_err(),
            ReadError::BadVersion(VERSION)
        ));
    }

    /// A v2 writer refuses blooms of non-members and duplicate members.
    #[test]
    fn test_v2_writer_validates_members() {
        use super::super::writer::{GroupMember, WriteError};

        let mut b = BloomBuilder::new();
        let i1 = b.begin_with_blocks(7, "trace_id", 8);
        b.insert(i1, b"abc");
        let blooms = b.finish();
        assert!(matches!(
            BloomWriter::serialize_v2(&[GroupMember::new(8, vec![])], blooms.clone()).unwrap_err(),
            WriteError::UnknownMember { file_id: 7, .. }
        ));
        assert!(matches!(
            BloomWriter::serialize_v2(
                &[GroupMember::new(7, vec![]), GroupMember::new(7, vec![])],
                blooms
            )
            .unwrap_err(),
            WriteError::DuplicateMember(7)
        ));
    }
}
