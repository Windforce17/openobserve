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

//! The block term dictionary — THE `.vix` dictionary layout.
//!
//! Sorted composite keys (`{fid u16 BE}{token}`) are cut into
//! prefix-compressed BLOCKS of ~[`BLOCK_TARGET_BYTES`] raw key bytes (a
//! block never spans a field boundary). The dictionary's read granularity
//! is therefore ONE BLOCK (~KBs), not a whole dictionary segment: an exact
//! lookup binary-searches the resident block INDEX for the predecessor
//! block and fetches only that block. The previous layout (one monolithic
//! FST per 8MB row group) made the smallest possible read the whole row
//! group — measured 23.4GB of demand for one cold token count over 55
//! merged files vs the ~KBs/file an sstable-shaped dictionary pays
//! (ENGINE-BACKLOG #18).
//!
//! Ordinals are implicit: keys are stored in global ordinal order, block
//! `b` starts at `meta[b].first_ordinal`, so the ordinal of the key at
//! position `p` of block `b` is `first_ordinal + p`.
//!
//! # Block encoding
//!
//! `[u16 LE first_key_len][first_key]` then, per subsequent key:
//! `[varint shared][varint suffix_len][suffix]` where `shared` is the
//! byte-length shared with the PREVIOUS key. Decoding is a strict forward
//! scan; blocks are small enough that scans beat in-block restart tables.
//!
//! # Index encoding (the `dict` blob)
//!
//! ```text
//! [u64 LE block_count]
//! [meta: block_count x { u64 LE blocks_offset, u64 LE first_ordinal }]
//! [first-keys region: prefix-compressed like a block, with RESTARTS]
//! [restart table: u32 LE offsets into the region, ascending]
//! [u32 LE restart_count]
//! ```
//!
//! Every [`INDEX_RESTART_INTERVAL`]-th first-key is a restart (stored
//! whole); predecessor lookup binary-searches the restart table, then
//! scans forward at most one interval. `meta[b].blocks_offset` addresses
//! the `dict_blocks` blob; a block's byte length is
//! `meta[b+1].blocks_offset - meta[b].blocks_offset` (the last block runs
//! to the blob end, whose total length the caller supplies).

use crate::error::{Result, VixError};

/// Target raw-key bytes per block. A block closes at the first key that
/// would push it past this (and always at field boundaries). Code
/// constant by design — the read side self-describes through the index.
pub(crate) const BLOCK_TARGET_BYTES: usize = 4096;

/// Every Nth first-key in the index region is stored whole as a binary
/// search restart point.
pub(crate) const INDEX_RESTART_INTERVAL: usize = 16;

fn write_varint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            break;
        }
        out.push(byte | 0x80);
    }
}

fn read_varint(data: &[u8], pos: &mut usize) -> Result<u64> {
    let mut out = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = *data
            .get(*pos)
            .ok_or_else(|| VixError::Malformed("dict varint truncated".to_string()))?;
        *pos += 1;
        out |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(out);
        }
        shift += 7;
        if shift > 63 {
            return Err(VixError::Malformed("dict varint overflow".to_string()));
        }
    }
}

/// Read the `(shared, suffix_len)` varint pair of one block entry
/// (incremental decoders that keep their own position state).
pub(crate) fn read_two_varints(data: &[u8], pos: &mut usize) -> Result<(usize, usize)> {
    let shared = read_varint(data, pos)? as usize;
    let suffix_len = read_varint(data, pos)? as usize;
    Ok((shared, suffix_len))
}

fn shared_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

// ---------------------------------------------------------------------------
// block encode/decode
// ---------------------------------------------------------------------------

/// Streaming encoder for one key block.
pub(crate) struct BlockBuilder {
    buf: Vec<u8>,
    prev: Vec<u8>,
    count: usize,
    raw_bytes: usize,
}

impl BlockBuilder {
    pub(crate) fn new() -> Self {
        Self {
            buf: Vec::with_capacity(BLOCK_TARGET_BYTES + 256),
            prev: Vec::new(),
            count: 0,
            raw_bytes: 0,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub(crate) fn count(&self) -> usize {
        self.count
    }

    /// Raw (uncompressed) key bytes pushed so far — the block-cut metric.
    pub(crate) fn raw_bytes(&self) -> usize {
        self.raw_bytes
    }

    /// Append `key` (strictly ascending across pushes).
    pub(crate) fn push(&mut self, key: &[u8]) -> Result<()> {
        if self.count == 0 {
            let len = u16::try_from(key.len())
                .map_err(|_| VixError::Malformed("dict key longer than u16".to_string()))?;
            self.buf.extend_from_slice(&len.to_le_bytes());
            self.buf.extend_from_slice(key);
        } else {
            let shared = shared_prefix_len(&self.prev, key);
            write_varint(&mut self.buf, shared as u64);
            write_varint(&mut self.buf, (key.len() - shared) as u64);
            self.buf.extend_from_slice(&key[shared..]);
        }
        self.prev.clear();
        self.prev.extend_from_slice(key);
        self.count += 1;
        self.raw_bytes += key.len();
        Ok(())
    }

    /// The encoded block; the builder resets for reuse.
    pub(crate) fn finish(&mut self) -> Vec<u8> {
        self.prev.clear();
        self.count = 0;
        self.raw_bytes = 0;
        std::mem::take(&mut self.buf)
    }
}

/// Iterate every key of an encoded block in order. `on_key(position, key)`
/// returns `false` to stop early.
pub(crate) fn block_scan(block: &[u8], mut on_key: impl FnMut(usize, &[u8]) -> bool) -> Result<()> {
    if block.is_empty() {
        return Ok(());
    }
    let mut pos = 0usize;
    let first_len = u16::from_le_bytes(
        block
            .get(0..2)
            .ok_or_else(|| VixError::Malformed("dict block header truncated".to_string()))?
            .try_into()
            .unwrap(),
    ) as usize;
    pos += 2;
    let mut key = block
        .get(pos..pos + first_len)
        .ok_or_else(|| VixError::Malformed("dict block first key truncated".to_string()))?
        .to_vec();
    pos += first_len;
    let mut index = 0usize;
    if !on_key(index, &key) {
        return Ok(());
    }
    index += 1;
    while pos < block.len() {
        let shared = read_varint(block, &mut pos)? as usize;
        let suffix_len = read_varint(block, &mut pos)? as usize;
        if shared > key.len() {
            return Err(VixError::Malformed(format!(
                "dict block shared prefix {shared} exceeds previous key length {}",
                key.len()
            )));
        }
        let suffix = block
            .get(pos..pos + suffix_len)
            .ok_or_else(|| VixError::Malformed("dict block suffix truncated".to_string()))?;
        pos += suffix_len;
        key.truncate(shared);
        key.extend_from_slice(suffix);
        if !on_key(index, &key) {
            return Ok(());
        }
        index += 1;
    }
    Ok(())
}

/// Position of `key` within the block, if present.
pub(crate) fn block_find_exact(block: &[u8], key: &[u8]) -> Result<Option<usize>> {
    let mut found = None;
    block_scan(block, |pos, k| match k.cmp(key) {
        std::cmp::Ordering::Less => true,
        std::cmp::Ordering::Equal => {
            found = Some(pos);
            false
        }
        std::cmp::Ordering::Greater => false,
    })?;
    Ok(found)
}

/// Position of the first key `>= target` (== block key count when every
/// key is smaller).
pub(crate) fn block_lower_bound(block: &[u8], target: &[u8]) -> Result<usize> {
    let mut bound = 0usize;
    let mut total = 0usize;
    let mut hit = false;
    block_scan(block, |pos, k| {
        total = pos + 1;
        if !hit && k >= target {
            bound = pos;
            hit = true;
            return false;
        }
        true
    })?;
    Ok(if hit { bound } else { total })
}

// ---------------------------------------------------------------------------
// index encode/parse
// ---------------------------------------------------------------------------

/// Accumulates `(first_key, blocks_offset, first_ordinal)` triples while a
/// sink flushes blocks, and serializes the index region.
pub(crate) struct IndexBuilder {
    metas: Vec<(u64, u64)>,
    fk_region: Vec<u8>,
    restarts: Vec<u32>,
    prev_key: Vec<u8>,
}

impl IndexBuilder {
    pub(crate) fn new() -> Self {
        Self {
            metas: Vec::new(),
            fk_region: Vec::new(),
            restarts: Vec::new(),
            prev_key: Vec::new(),
        }
    }

    pub(crate) fn block_count(&self) -> usize {
        self.metas.len()
    }

    /// Record one flushed block.
    pub(crate) fn push_block(
        &mut self,
        first_key: &[u8],
        blocks_offset: u64,
        first_ordinal: u64,
    ) -> Result<()> {
        if self.metas.len() % INDEX_RESTART_INTERVAL == 0 {
            let at = u32::try_from(self.fk_region.len())
                .map_err(|_| VixError::Malformed("dict index region overflows u32".to_string()))?;
            self.restarts.push(at);
            let len = u16::try_from(first_key.len())
                .map_err(|_| VixError::Malformed("dict key longer than u16".to_string()))?;
            self.fk_region.extend_from_slice(&len.to_le_bytes());
            self.fk_region.extend_from_slice(first_key);
        } else {
            let shared = shared_prefix_len(&self.prev_key, first_key);
            write_varint(&mut self.fk_region, shared as u64);
            write_varint(&mut self.fk_region, (first_key.len() - shared) as u64);
            self.fk_region.extend_from_slice(&first_key[shared..]);
        }
        self.prev_key.clear();
        self.prev_key.extend_from_slice(first_key);
        self.metas.push((blocks_offset, first_ordinal));
        Ok(())
    }

    /// Serialize the whole index blob.
    pub(crate) fn finish(self) -> Vec<u8> {
        let mut out = Vec::with_capacity(8 + self.metas.len() * 16 + self.fk_region.len() + 8);
        out.extend_from_slice(&(self.metas.len() as u64).to_le_bytes());
        for (off, ord) in &self.metas {
            out.extend_from_slice(&off.to_le_bytes());
            out.extend_from_slice(&ord.to_le_bytes());
        }
        out.extend_from_slice(&self.fk_region);
        for r in &self.restarts {
            out.extend_from_slice(&r.to_le_bytes());
        }
        out.extend_from_slice(&(self.restarts.len() as u32).to_le_bytes());
        out
    }
}

/// The parsed (resident) dictionary index.
pub(crate) struct DictIndex {
    /// `(blocks_offset, first_ordinal)` per block, ascending in both.
    metas: Vec<(u64, u64)>,
    fk_region: Vec<u8>,
    restarts: Vec<u32>,
}

impl DictIndex {
    pub(crate) fn parse(data: &[u8]) -> Result<Self> {
        let truncated = || VixError::Malformed("dict index truncated".to_string());
        let block_count =
            u64::from_le_bytes(data.get(0..8).ok_or_else(truncated)?.try_into().unwrap()) as usize;
        let meta_end = 8 + block_count
            .checked_mul(16)
            .ok_or_else(|| VixError::Malformed("dict index meta overflow".to_string()))?;
        let meta_bytes = data.get(8..meta_end).ok_or_else(truncated)?;
        let mut metas = Vec::with_capacity(block_count);
        for chunk in meta_bytes.chunks_exact(16) {
            metas.push((
                u64::from_le_bytes(chunk[0..8].try_into().unwrap()),
                u64::from_le_bytes(chunk[8..16].try_into().unwrap()),
            ));
        }
        if data.len() < meta_end + 4 {
            return Err(truncated());
        }
        let restart_count = u32::from_le_bytes(data[data.len() - 4..].try_into().unwrap()) as usize;
        let restarts_start = data
            .len()
            .checked_sub(4 + restart_count * 4)
            .filter(|&s| s >= meta_end)
            .ok_or_else(truncated)?;
        let mut restarts = Vec::with_capacity(restart_count);
        for chunk in data[restarts_start..data.len() - 4].chunks_exact(4) {
            restarts.push(u32::from_le_bytes(chunk.try_into().unwrap()));
        }
        let fk_region = data[meta_end..restarts_start].to_vec();
        let expected_restarts = block_count.div_ceil(INDEX_RESTART_INTERVAL);
        if restarts.len() != expected_restarts {
            return Err(VixError::Malformed(format!(
                "dict index: {} restarts for {} blocks (expected {})",
                restarts.len(),
                block_count,
                expected_restarts
            )));
        }
        Ok(Self {
            metas,
            fk_region,
            restarts,
        })
    }

    pub(crate) fn block_count(&self) -> usize {
        self.metas.len()
    }

    /// `(blocks_offset, first_ordinal)` of block `b`.
    pub(crate) fn meta(&self, b: usize) -> (u64, u64) {
        self.metas[b]
    }

    /// Byte range of block `b` inside the blocks blob (`blob_len` bounds
    /// the last block).
    pub(crate) fn block_range(&self, b: usize, blob_len: u64) -> std::ops::Range<u64> {
        let start = self.metas[b].0;
        let end = if b + 1 < self.metas.len() {
            self.metas[b + 1].0
        } else {
            blob_len
        };
        start..end
    }

    /// Number of keys in block `b`, given the file's total term count.
    pub(crate) fn block_key_count(&self, b: usize, term_count: u64) -> u64 {
        let first = self.metas[b].1;
        let next = if b + 1 < self.metas.len() {
            self.metas[b + 1].1
        } else {
            term_count
        };
        next - first
    }

    /// The restart-point first-key at restart slot `r` (stored whole).
    fn restart_key(&self, r: usize) -> Result<&[u8]> {
        let at = self.restarts[r] as usize;
        let len = u16::from_le_bytes(
            self.fk_region
                .get(at..at + 2)
                .ok_or_else(|| VixError::Malformed("dict index restart truncated".to_string()))?
                .try_into()
                .unwrap(),
        ) as usize;
        self.fk_region
            .get(at + 2..at + 2 + len)
            .ok_or_else(|| VixError::Malformed("dict index restart key truncated".to_string()))
    }

    /// Walk first-keys starting at restart slot `r`; `on_key(block_id, key)`
    /// returns `false` to stop.
    fn walk_from_restart(
        &self,
        r: usize,
        mut on_key: impl FnMut(usize, &[u8]) -> bool,
    ) -> Result<()> {
        let start_block = r * INDEX_RESTART_INTERVAL;
        let region_end = if r + 1 < self.restarts.len() {
            self.restarts[r + 1] as usize
        } else {
            self.fk_region.len()
        };
        let mut pos = self.restarts[r] as usize;
        let len = u16::from_le_bytes(
            self.fk_region
                .get(pos..pos + 2)
                .ok_or_else(|| VixError::Malformed("dict index restart truncated".to_string()))?
                .try_into()
                .unwrap(),
        ) as usize;
        pos += 2;
        let mut key = self
            .fk_region
            .get(pos..pos + len)
            .ok_or_else(|| VixError::Malformed("dict index key truncated".to_string()))?
            .to_vec();
        pos += len;
        let mut block = start_block;
        if !on_key(block, &key) {
            return Ok(());
        }
        block += 1;
        while pos < region_end {
            let shared = read_varint(&self.fk_region, &mut pos)? as usize;
            let suffix_len = read_varint(&self.fk_region, &mut pos)? as usize;
            if shared > key.len() {
                return Err(VixError::Malformed(
                    "dict index shared prefix exceeds previous key".to_string(),
                ));
            }
            let suffix = self
                .fk_region
                .get(pos..pos + suffix_len)
                .ok_or_else(|| VixError::Malformed("dict index suffix truncated".to_string()))?;
            pos += suffix_len;
            key.truncate(shared);
            key.extend_from_slice(suffix);
            if !on_key(block, &key) {
                return Ok(());
            }
            block += 1;
        }
        Ok(())
    }

    /// Walk EVERY block's first key in order (index scans for automaton
    /// pruning); `on_key(block_id, key)` returns `false` to stop.
    pub(crate) fn walk_first_keys(
        &self,
        mut on_key: impl FnMut(usize, &[u8]) -> bool,
    ) -> Result<()> {
        let mut stop = false;
        for r in 0..self.restarts.len() {
            if stop {
                break;
            }
            self.walk_from_restart(r, |b, k| {
                let cont = on_key(b, k);
                if !cont {
                    stop = true;
                }
                cont
            })?;
        }
        Ok(())
    }

    /// The block that may contain `key`: the LAST block whose first key is
    /// `<= key`. `None` when `key` sorts before the very first key.
    pub(crate) fn predecessor_block(&self, key: &[u8]) -> Result<Option<usize>> {
        if self.metas.is_empty() {
            return Ok(None);
        }
        // binary search the restart keys for the last restart <= key
        let (mut lo, mut hi) = (0usize, self.restarts.len());
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.restart_key(mid)? <= key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo == 0 {
            return Ok(None); // key precedes the first block's first key
        }
        let restart = lo - 1;
        // scan forward within the interval for the last first-key <= key
        let mut best = restart * INDEX_RESTART_INTERVAL;
        self.walk_from_restart(restart, |b, k| {
            if k <= key {
                best = b;
                true
            } else {
                false
            }
        })?;
        Ok(Some(best))
    }
}

/// Pull-style iterator over one encoded block's keys (the k-way merge
/// streams need incremental advancement; everything else uses
/// [`block_scan`]). `next()` yields `Ok(Some(key))` borrowing the internal
/// buffer until the following call.
pub(crate) struct BlockIter<'a> {
    data: &'a [u8],
    pos: usize,
    key: Vec<u8>,
    started: bool,
}

impl<'a> BlockIter<'a> {
    pub(crate) fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            pos: 0,
            key: Vec::new(),
            started: false,
        }
    }

    pub(crate) fn next(&mut self) -> Result<Option<&[u8]>> {
        if !self.started {
            self.started = true;
            if self.data.is_empty() {
                return Ok(None);
            }
            let len = u16::from_le_bytes(
                self.data
                    .get(0..2)
                    .ok_or_else(|| VixError::Malformed("dict block header truncated".to_string()))?
                    .try_into()
                    .unwrap(),
            ) as usize;
            self.pos = 2 + len;
            self.key = self
                .data
                .get(2..2 + len)
                .ok_or_else(|| VixError::Malformed("dict block first key truncated".to_string()))?
                .to_vec();
            return Ok(Some(&self.key));
        }
        if self.pos >= self.data.len() {
            return Ok(None);
        }
        let shared = read_varint(self.data, &mut self.pos)? as usize;
        let suffix_len = read_varint(self.data, &mut self.pos)? as usize;
        if shared > self.key.len() {
            return Err(VixError::Malformed(
                "dict block shared prefix exceeds previous key".to_string(),
            ));
        }
        let suffix = self
            .data
            .get(self.pos..self.pos + suffix_len)
            .ok_or_else(|| VixError::Malformed("dict block suffix truncated".to_string()))?;
        self.pos += suffix_len;
        self.key.truncate(shared);
        self.key.extend_from_slice(suffix);
        Ok(Some(&self.key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(n: usize) -> Vec<Vec<u8>> {
        // realistic composite shape: {fid u16 BE}{token}, several fields
        let mut out: Vec<Vec<u8>> = Vec::new();
        for fid in [0u16, 3, 7, 0xFFFF] {
            for i in 0..n {
                let mut k = fid.to_be_bytes().to_vec();
                k.extend_from_slice(format!("token{:08}", i * 3).as_bytes());
                out.push(k);
            }
        }
        out.sort();
        out.dedup();
        out
    }

    fn build(all: &[Vec<u8>], per_block: usize) -> (Vec<u8>, Vec<u8>) {
        let mut blocks = Vec::new();
        let mut index = IndexBuilder::new();
        let mut bb = BlockBuilder::new();
        let mut first_key: Vec<u8> = Vec::new();
        let mut first_ord = 0u64;
        for (ord, k) in all.iter().enumerate() {
            if bb.is_empty() {
                first_key = k.clone();
                first_ord = ord as u64;
            }
            bb.push(k).unwrap();
            if bb.count() >= per_block {
                let off = blocks.len() as u64;
                let bytes = bb.finish();
                index.push_block(&first_key, off, first_ord).unwrap();
                blocks.extend_from_slice(&bytes);
            }
        }
        if !bb.is_empty() {
            let off = blocks.len() as u64;
            let bytes = bb.finish();
            index.push_block(&first_key, off, first_ord).unwrap();
            blocks.extend_from_slice(&bytes);
        }
        (index.finish(), blocks)
    }

    #[test]
    fn block_roundtrip_and_bounds() {
        let all = keys(500);
        let mut bb = BlockBuilder::new();
        for k in &all[..100] {
            bb.push(k).unwrap();
        }
        let block = bb.finish();
        let mut got = Vec::new();
        block_scan(&block, |_, k| {
            got.push(k.to_vec());
            true
        })
        .unwrap();
        assert_eq!(got, &all[..100]);
        for (i, k) in all[..100].iter().enumerate() {
            assert_eq!(block_find_exact(&block, k).unwrap(), Some(i), "key {i}");
            assert_eq!(block_lower_bound(&block, k).unwrap(), i);
        }
        // absent probes: between, before, after
        let mut absent = all[50].clone();
        absent.push(0);
        assert_eq!(block_find_exact(&block, &absent).unwrap(), None);
        assert_eq!(block_lower_bound(&block, &absent).unwrap(), 51);
        assert_eq!(block_find_exact(&block, b"\x00\x00").unwrap(), None);
        assert_eq!(block_lower_bound(&block, b"\x00\x00").unwrap(), 0);
        assert_eq!(block_lower_bound(&block, b"\xff\xff\xff").unwrap(), 100);
    }

    #[test]
    fn index_predecessor_matches_naive_everywhere() {
        for per_block in [1usize, 3, 16, 64] {
            let all = keys(300);
            let (index_bytes, blocks) = build(&all, per_block);
            let index = DictIndex::parse(&index_bytes).unwrap();
            let expected_blocks = all.len().div_ceil(per_block);
            assert_eq!(
                index.block_count(),
                expected_blocks,
                "per_block={per_block}"
            );

            // every key resolves through predecessor -> exact scan
            for (ord, k) in all.iter().enumerate() {
                let b = index
                    .predecessor_block(k)
                    .unwrap()
                    .unwrap_or_else(|| panic!("no block for key {ord}"));
                let range = index.block_range(b, blocks.len() as u64);
                let block = &blocks[range.start as usize..range.end as usize];
                let pos = block_find_exact(block, k)
                    .unwrap()
                    .unwrap_or_else(|| panic!("key {ord} missing from its block"));
                assert_eq!(index.meta(b).1 + pos as u64, ord as u64);
            }
            // absent keys: predecessor never panics, exact never lies
            let mut probe = all[all.len() / 2].clone();
            probe.push(1);
            let b = index.predecessor_block(&probe).unwrap().unwrap();
            let range = index.block_range(b, blocks.len() as u64);
            assert_eq!(
                block_find_exact(&blocks[range.start as usize..range.end as usize], &probe)
                    .unwrap(),
                None
            );
            assert_eq!(index.predecessor_block(b"\x00").unwrap(), None);
            // first-key walk covers every block in order
            let mut seen = Vec::new();
            index
                .walk_first_keys(|b, _| {
                    seen.push(b);
                    true
                })
                .unwrap();
            assert_eq!(seen, (0..expected_blocks).collect::<Vec<_>>());
        }
    }

    #[test]
    fn block_iter_matches_scan() {
        let all = keys(200);
        let mut bb = BlockBuilder::new();
        for k in &all[..90] {
            bb.push(k).unwrap();
        }
        let block = bb.finish();
        let mut it = BlockIter::new(&block);
        let mut got = Vec::new();
        while let Some(k) = it.next().unwrap() {
            got.push(k.to_vec());
        }
        assert_eq!(got, &all[..90]);
        assert!(BlockIter::new(&[]).next().unwrap().is_none());
    }

    #[test]
    fn index_roundtrips_metas() {
        let all = keys(100);
        let (index_bytes, blocks) = build(&all, 7);
        let index = DictIndex::parse(&index_bytes).unwrap();
        let mut covered = 0u64;
        for b in 0..index.block_count() {
            covered += index.block_key_count(b, all.len() as u64);
            let range = index.block_range(b, blocks.len() as u64);
            assert!(range.start < range.end);
        }
        assert_eq!(covered, all.len() as u64);
    }
}
