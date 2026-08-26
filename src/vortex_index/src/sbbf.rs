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

//! Split-Block Bloom Filter (SBBF) — minimal in-house implementation.
//!
//! Moved here from `infra::bloom::sbbf` (which now re-exports this module)
//! so the `.vix` writer can build per-file value blooms without the
//! low-level crate depending on `infra`. The write path in this crate
//! accumulates value HASHES during term emission and materializes the
//! bitset at blob-assembly time ([`Sbbf::insert_hash`]); the group `.bf`
//! machinery in `infra::bloom` keeps using the identical primitives, so
//! per-file blob blocks transpose into group files byte-for-byte.
//!
//! Self-consistent: writer and reader use the same `SALT` constants, the
//! same `gxhash64(seed=0)` hash ([`hash_value`] — mirrors
//! `config::utils::hash::sum64_bytes`, parity-tested in `infra`), the same
//! fastmap block-index function. The serialized body is just
//! `num_blocks × 32` raw little-endian bytes — no header, no framing.
//!
//! **Hash choice note**: we deviate from the Parquet spec on hash function
//! (Parquet specifies XxHash64). We don't interop with any external SBBF
//! reader — only our own writer/reader pair — so the only requirement is
//! that the same hash runs on both sides of the binary.
//!
//! Reference: Apache Parquet bloom-filter spec (block layout only)
//! <https://github.com/apache/parquet-format/blob/master/BloomFilter.md>.

/// One SBBF block is 8 × u32 words = 32 bytes.
pub const BLOCK_BYTES: usize = 32;

/// Eight magic 32-bit primes from the Parquet bloom-filter spec. Each one
/// drives a separate "which bit within the block to set" calculation, so
/// every value sets 8 bits per block.
pub const SALT: [u32; 8] = [
    0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d, 0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31,
];

/// 64-bit hash used by both writer and reader: `gxhash64(seed=0)`, exactly
/// `config::utils::hash::sum64_bytes` (parity-tested where both crates are
/// visible). On platforms without the `gxhash` cargo feature this degrades
/// to `DefaultHasher`; writer and reader always run in the same binary, so
/// the two sides agree.
#[inline]
pub fn hash_value(value: &[u8]) -> u64 {
    #[cfg(feature = "gxhash")]
    let n = gxhash::gxhash64(value, 0);
    #[cfg(not(feature = "gxhash"))]
    let n = {
        use std::hash::{DefaultHasher, Hasher};
        let mut h = DefaultHasher::new();
        h.write(value);
        h.finish()
    };
    n
}

/// Map a hash to a block index using the "fastmap" trick from the spec:
/// `((hash >> 32) * num_blocks) >> 32`. Works for any `num_blocks`, not
/// just powers of two, and is far cheaper than a modulo.
///
/// A useful identity for power-of-two folding: for even `B`,
/// `block_index(h, B / 2) == block_index(h, B) >> 1` — halving the block
/// count sends each value exactly to its merged block, which is what makes
/// [`Sbbf::fold`] sound.
#[inline]
pub fn block_index(hash: u64, num_blocks: u32) -> u32 {
    let high = hash >> 32;
    ((high * num_blocks as u64) >> 32) as u32
}

/// 8 single-bit masks computed from the lower 32 bits of `hash`. These are
/// the bits to set/check inside the chosen block — one per word.
///
/// The mask depends only on the hash (never on `num_blocks`), so folding a
/// filter to a smaller block count preserves every membership answer, and
/// in the transposed group layout it is computed **once per value** and
/// reused for all files' blocks via [`check_block_with_mask`].
#[inline]
pub fn mask_from_hash(hash: u64) -> [u32; 8] {
    let key = hash as u32;
    let mut out = [0u32; 8];
    for i in 0..8 {
        let y = key.wrapping_mul(SALT[i]);
        out[i] = 1u32 << (y >> 27);
    }
    out
}

/// Decode a 32-byte SBBF block from raw little-endian bytes.
#[inline]
fn block_from_bytes(bytes: &[u8; BLOCK_BYTES]) -> [u32; 8] {
    let mut out = [0u32; 8];
    for (i, word) in out.iter_mut().enumerate() {
        let off = i * 4;
        *word = u32::from_le_bytes(bytes[off..off + 4].try_into().unwrap());
    }
    out
}

/// Single-block point check against a **precomputed mask**.
///
/// Reads each 32-bit word straight out of the 32-byte block and tests it
/// against the corresponding mask word — no intermediate `[u32; 8]` array.
/// Use this when checking many files' blocks for the same value: compute the
/// mask once with [`mask_from_hash`] and pass it here per block.
#[inline]
pub fn check_block_with_mask(block_bytes: &[u8; BLOCK_BYTES], mask: &[u32; 8]) -> bool {
    for (i, m) in mask.iter().enumerate() {
        let off = i * 4;
        let word = u32::from_le_bytes(block_bytes[off..off + 4].try_into().unwrap());
        if word & m != *m {
            return false;
        }
    }
    true
}

/// Single-block point check used by the search side.
///
/// Given just the **32 bytes of the chosen block** plus the original hash,
/// returns true iff the block has every required bit set. The caller is
/// responsible for fetching the block (e.g. via a range read on the `.bf`
/// body) and supplying the same `hash` that was used to pick the block.
/// For checking many files against one value, prefer computing the mask once
/// via [`mask_from_hash`] + [`check_block_with_mask`].
#[inline]
pub fn check_block(block_bytes: &[u8; BLOCK_BYTES], hash: u64) -> bool {
    check_block_with_mask(block_bytes, &mask_from_hash(hash))
}

/// Sizing helper. Returns the number of 32-byte SBBF blocks required to
/// hold `ndv` distinct items at the requested false-positive probability.
///
/// We round up to the next power of two so block-index math has the same
/// distribution as Parquet's own sizing (and so the on-disk size is
/// predictable across builds). Power-of-two counts are also what makes
/// [`Sbbf::fold`] available between any two sizes.
pub fn num_blocks_for(ndv: u64, fpp: f64) -> u32 {
    // Bits-per-element from the standard Bloom-filter formula. SBBF is
    // ~1.4x worse than a plain Bloom at the same FPR, but we follow the
    // Parquet sizing convention here so the on-disk layout matches what
    // the previous parquet-based writer produced.
    let ndv = ndv.max(1) as f64;
    let fpp = fpp.clamp(1e-12, 0.5);
    let bits = (-ndv * fpp.ln() / (std::f64::consts::LN_2 * std::f64::consts::LN_2)).ceil();
    let blocks_f = (bits / 256.0).ceil().max(1.0);
    // Round up to the next power of two.
    let mut blocks = blocks_f as u64;
    blocks = blocks.next_power_of_two();
    blocks.min(u32::MAX as u64) as u32
}

/// Builder-side SBBF: owns the bitset, supports `insert` + full `check`.
/// The reader side never instantiates this — it goes through
/// [`check_block`] on a single fetched block instead.
#[derive(Debug, Clone)]
pub struct Sbbf {
    blocks: Vec<[u32; 8]>,
}

impl Sbbf {
    /// Allocate an empty SBBF sized for `ndv` items at `fpp`.
    pub fn new_with_ndv_fpp(ndv: u64, fpp: f64) -> Self {
        Self::new_with_num_blocks(num_blocks_for(ndv, fpp))
    }

    /// Allocate an empty SBBF with an explicit block count. Used by the
    /// transposed `.bf` layout where every file in a group must share the
    /// same `num_blocks` so a single value maps to the same block index
    /// across all files (enabling one contiguous read per group).
    pub fn new_with_num_blocks(num_blocks: u32) -> Self {
        Self {
            blocks: vec![[0u32; 8]; num_blocks.max(1) as usize],
        }
    }

    /// Number of 32-byte blocks held.
    pub fn num_blocks(&self) -> u32 {
        self.blocks.len() as u32
    }

    /// Set the 8 bits for a **precomputed hash** in the chosen block. The
    /// `.vix` write path accumulates hashes during term emission and
    /// materializes the bitset once the distinct count (and therefore the
    /// block count) is known — this is its insertion primitive.
    pub fn insert_hash(&mut self, hash: u64) {
        let idx = block_index(hash, self.num_blocks()) as usize;
        let mask = mask_from_hash(hash);
        let block = &mut self.blocks[idx];
        for i in 0..8 {
            block[i] |= mask[i];
        }
    }

    /// Set the 8 bits for `value` in the chosen block.
    pub fn insert(&mut self, value: &[u8]) {
        self.insert_hash(hash_value(value));
    }

    /// M12: bulk-insert MANY precomputed hashes, in parallel when `threads`
    /// and the input size warrant it. BYTE-IDENTICAL to the sequential
    /// `insert_hash` loop for any thread count: each hash touches exactly
    /// one block ([`block_index`]), the block space is partitioned into
    /// contiguous disjoint per-worker ranges (hashes bucketed by the same
    /// mapping in one pass), and OR-ing bits within a block is
    /// order-independent — so the final bit pattern cannot depend on the
    /// partitioning. Small inputs (or `threads <= 1`) keep the plain loop;
    /// the threshold keeps thread setup out of the per-field small builds.
    pub fn insert_hashes(&mut self, hashes: &[u64], threads: usize) {
        /// Below this many hashes the sequential loop wins outright.
        const PARALLEL_MIN_HASHES: usize = 1 << 20;
        let num_blocks = self.num_blocks() as u64;
        let workers = threads
            .min(hashes.len() / PARALLEL_MIN_HASHES + 1)
            .min(num_blocks as usize)
            .max(1);
        if workers <= 1 || hashes.len() < PARALLEL_MIN_HASHES {
            for h in hashes {
                self.insert_hash(*h);
            }
            return;
        }

        // worker(b) = floor(b * workers / num_blocks); worker w owns blocks
        // [ceil(w*nb/W), ceil((w+1)*nb/W)) — an exact tiling of the block
        // space consistent with the bucketing below.
        let w64 = workers as u64;
        let boundary = |w: u64| -> usize { ((w * num_blocks).div_ceil(w64)) as usize };
        let mut buckets: Vec<Vec<u64>> =
            vec![Vec::with_capacity(hashes.len() / workers + 1); workers];
        for &h in hashes {
            let b = block_index(h, num_blocks as u32) as u64;
            let w = (b * w64 / num_blocks) as usize;
            buckets[w].push(h);
        }

        let mut slices: Vec<(usize, &mut [[u32; 8]])> = Vec::with_capacity(workers);
        let mut rest: &mut [[u32; 8]] = &mut self.blocks;
        let mut consumed = 0usize;
        for w in 0..workers {
            let end = boundary(w as u64 + 1);
            let (own, tail) = rest.split_at_mut(end - consumed);
            slices.push((consumed, own));
            consumed = end;
            rest = tail;
        }

        std::thread::scope(|scope| {
            for ((start, blocks), bucket) in slices.into_iter().zip(&buckets) {
                scope.spawn(move || {
                    for &h in bucket {
                        let idx = block_index(h, num_blocks as u32) as usize - start;
                        let mask = mask_from_hash(h);
                        let block = &mut blocks[idx];
                        for i in 0..8 {
                            block[i] |= mask[i];
                        }
                    }
                });
            }
        });
    }

    /// Membership test for a **precomputed hash**.
    pub fn check_hash(&self, hash: u64) -> bool {
        let idx = block_index(hash, self.num_blocks()) as usize;
        let mask = mask_from_hash(hash);
        let block = &self.blocks[idx];
        for i in 0..8 {
            if block[i] & mask[i] != mask[i] {
                return false;
            }
        }
        true
    }

    /// Membership test. False = definitely absent, true = maybe present.
    pub fn check(&self, value: &[u8]) -> bool {
        self.check_hash(hash_value(value))
    }

    /// Fold the filter down to `target_blocks` (a power-of-two divisor of
    /// the current count) by OR-ing block pairs. Sound because
    /// `block_index(h, B/2) == block_index(h, B) >> 1` and the intra-block
    /// mask never depends on the block count — every membership answer is
    /// preserved. Each halving roughly doubles the fill ratio (raises the
    /// FPR), so group assemblers should fold at most a tier or two.
    pub fn fold(&mut self, target_blocks: u32) -> Result<(), &'static str> {
        let target = target_blocks.max(1) as usize;
        let current = self.blocks.len();
        if target == current {
            return Ok(());
        }
        if target > current || !current.is_multiple_of(target) || !current.is_power_of_two() {
            return Err("fold target must be a power-of-two divisor of the current block count");
        }
        while self.blocks.len() > target {
            let half = self.blocks.len() / 2;
            for i in 0..half {
                let hi = self.blocks[2 * i + 1];
                let lo = &mut self.blocks[2 * i];
                for w in 0..8 {
                    lo[w] |= hi[w];
                }
                self.blocks[i] = self.blocks[2 * i];
            }
            self.blocks.truncate(half);
        }
        Ok(())
    }

    /// Serialize the bitset to little-endian bytes — no header, no
    /// framing. Length is exactly `num_blocks × 32`.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.blocks.len() * BLOCK_BYTES);
        for block in &self.blocks {
            for word in block {
                out.extend_from_slice(&word.to_le_bytes());
            }
        }
        out
    }

    /// Parse from bytes produced by `to_bytes`. Used by tests and the group
    /// assembler's fold path; production readers go through [`check_block`]
    /// without materializing this struct.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, &'static str> {
        if bytes.is_empty() || !bytes.len().is_multiple_of(BLOCK_BYTES) {
            return Err("SBBF bytes must be a non-zero multiple of 32");
        }
        let n = bytes.len() / BLOCK_BYTES;
        let mut blocks = Vec::with_capacity(n);
        for i in 0..n {
            let start = i * BLOCK_BYTES;
            let chunk: &[u8; BLOCK_BYTES] = bytes[start..start + BLOCK_BYTES].try_into().unwrap();
            blocks.push(block_from_bytes(chunk));
        }
        Ok(Self { blocks })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn num_blocks_matches_parquet_sizing() {
        // 1.69M items at 0.01 FPR → 65536 blocks = 2 MB body (matches the
        // user's data-bloom fixture, which the parquet-based writer produced).
        assert_eq!(num_blocks_for(1_687_010, 0.01), 65536);
    }

    #[test]
    fn num_blocks_minimum_is_one() {
        assert_eq!(num_blocks_for(0, 0.01), 1);
        assert_eq!(num_blocks_for(1, 0.01), 1);
    }

    #[test]
    fn insert_then_check_round_trips() {
        let mut s = Sbbf::new_with_ndv_fpp(10_000, 0.01);
        for i in 0..1000u32 {
            s.insert(format!("trace-{i}").as_bytes());
        }
        for i in 0..1000u32 {
            assert!(
                s.check(format!("trace-{i}").as_bytes()),
                "inserted value missing: trace-{i}"
            );
        }
    }

    #[test]
    fn insert_hash_equals_insert() {
        let mut a = Sbbf::new_with_num_blocks(64);
        let mut b = Sbbf::new_with_num_blocks(64);
        for i in 0..500u32 {
            let v = format!("trace-{i}");
            a.insert(v.as_bytes());
            b.insert_hash(hash_value(v.as_bytes()));
        }
        assert_eq!(a.to_bytes(), b.to_bytes());
    }

    #[test]
    fn check_block_agrees_with_check() {
        let mut s = Sbbf::new_with_ndv_fpp(10_000, 0.01);
        let values: Vec<String> = (0..500).map(|i| format!("v-{i}")).collect();
        for v in &values {
            s.insert(v.as_bytes());
        }
        let bytes = s.to_bytes();
        let num_blocks = s.num_blocks();
        // Every inserted value must agree between full `check` and the
        // single-block path that the reader uses.
        for v in &values {
            let h = hash_value(v.as_bytes());
            let bi = block_index(h, num_blocks) as usize;
            let off = bi * BLOCK_BYTES;
            let block: &[u8; BLOCK_BYTES] = bytes[off..off + BLOCK_BYTES].try_into().unwrap();
            assert!(check_block(block, h), "single-block check missed {v}");
            assert!(s.check(v.as_bytes()));
        }
    }

    #[test]
    fn fold_preserves_membership() {
        let mut s = Sbbf::new_with_num_blocks(256);
        let values: Vec<String> = (0..2000).map(|i| format!("t-{i}")).collect();
        for v in &values {
            s.insert(v.as_bytes());
        }
        s.fold(64).unwrap();
        assert_eq!(s.num_blocks(), 64);
        for v in &values {
            assert!(s.check(v.as_bytes()), "fold lost {v}");
            // and the folded bytes must agree with the reader's single-block
            // path at the folded size
            let bytes = s.to_bytes();
            let h = hash_value(v.as_bytes());
            let bi = block_index(h, 64) as usize;
            let block: &[u8; BLOCK_BYTES] = bytes[bi * BLOCK_BYTES..(bi + 1) * BLOCK_BYTES]
                .try_into()
                .unwrap();
            assert!(check_block(block, h));
        }
    }

    /// M12: `insert_hashes` must be BYTE-IDENTICAL to the sequential
    /// `insert_hash` loop for every thread count — including counts that
    /// don't divide the block count, more threads than blocks, and inputs
    /// below the parallel threshold. Deterministic pseudo-random hashes at
    /// a scale that actually crosses the threshold.
    #[test]
    fn m12_insert_hashes_parallel_matches_sequential() {
        // splitmix64: deterministic, well-spread 64-bit stream
        fn splitmix(state: &mut u64) -> u64 {
            *state = state.wrapping_add(0x9E3779B97F4A7C15);
            let mut z = *state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
            z ^ (z >> 31)
        }
        let mut state = 0xC0FFEE_u64;
        let hashes: Vec<u64> = (0..(1 << 21)).map(|_| splitmix(&mut state)).collect();

        for num_blocks in [1u32, 7, 64, 4096, 65536] {
            let mut sequential = Sbbf::new_with_num_blocks(num_blocks);
            for h in &hashes {
                sequential.insert_hash(*h);
            }
            let expected = sequential.to_bytes();
            for threads in [1usize, 2, 3, 8, 61, 1024] {
                let mut parallel = Sbbf::new_with_num_blocks(num_blocks);
                parallel.insert_hashes(&hashes, threads);
                assert_eq!(
                    parallel.to_bytes(),
                    expected,
                    "num_blocks={num_blocks} threads={threads} must be byte-identical"
                );
            }
        }

        // below the threshold: still identical (plain loop path)
        let small = &hashes[..1000];
        let mut sequential = Sbbf::new_with_num_blocks(64);
        for h in small {
            sequential.insert_hash(*h);
        }
        let mut threaded = Sbbf::new_with_num_blocks(64);
        threaded.insert_hashes(small, 8);
        assert_eq!(threaded.to_bytes(), sequential.to_bytes());

        // empty input: no-op
        let mut empty = Sbbf::new_with_num_blocks(64);
        empty.insert_hashes(&[], 8);
        assert_eq!(empty.to_bytes(), Sbbf::new_with_num_blocks(64).to_bytes());
    }

    #[test]
    fn fold_rejects_bad_targets() {
        let mut s = Sbbf::new_with_num_blocks(64);
        assert!(s.fold(128).is_err()); // grow
        assert!(s.fold(3).is_err()); // non-divisor
        assert!(s.fold(64).is_ok()); // no-op
    }

    #[test]
    fn to_from_bytes_round_trips() {
        let mut s = Sbbf::new_with_ndv_fpp(1024, 0.01);
        s.insert(b"abc");
        s.insert(b"def");
        let bytes = s.to_bytes();
        let s2 = Sbbf::from_bytes(&bytes).unwrap();
        assert!(s2.check(b"abc"));
        assert!(s2.check(b"def"));
    }

    #[test]
    fn from_bytes_rejects_bad_length() {
        assert!(Sbbf::from_bytes(&[0u8; 31]).is_err());
        assert!(Sbbf::from_bytes(&[]).is_err());
    }
}
