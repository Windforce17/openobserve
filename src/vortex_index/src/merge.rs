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

//! Index merge for core-file compaction: build a merged `dict`/`terms` pair
//! straight from the inputs' term dictionaries instead of re-deriving every
//! term from `_source` (see [`crate::VixWriter::merge_input_indexes`]).
//!
//! The compactor's row merge assigns every input row a new doc id; a
//! [`DocIdMap`] captures that assignment per input. The merge then:
//!
//! - streams each input's composite terms in key order (its dictionary blocks), re-prefixing every
//!   value term with the field's id in the *output* field table. Field ids are assigned by sorted
//!   field name in every core file, so remapping preserves per-input key order — a strictness guard
//!   catches the pathological exceptions and the caller falls back to a full rebuild. Terms of
//!   fields with no output id (e.g. a field the merged docs store under a non-string type) are
//!   dropped and reported so the writer can mark them `partial` — exactly what a rebuild would do;
//! - k-way merges the remapped streams; equal keys union their postings with doc ids mapped through
//!   the [`DocIdMap`]s. `doc_count` is the plain sum (doc-id spaces are disjoint). A term present
//!   in every merged row is dense-elided (empty blob) against the *merged* row count; an input's
//!   dense-elided postings are expanded through its map. When every contributing input maps by
//!   constant offset the remapped lists concatenate in offset order without sorting (and a single
//!   contributor at offset 0 reuses its encoded blob byte-for-byte); table maps decode, remap,
//!   sort, and verify distinctness;
//! - runs the merge across up to `ZO_VIX_MERGE_KWAY_THREADS` workers by partitioning the OUTPUT key
//!   space into ranges bounded by real remapped input keys ([`partition_bounds`]), each bound
//!   translated into every input's own key space ([`translate_bound`]); each range produces one
//!   [`crate::writer::TermSink`] run and the runs concatenate into the final blobs with their
//!   row-group ordinals rebased ([`crate::writer::write_index_blobs`], which hard-rejects
//!   out-of-order runs).

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    hash::BuildHasher,
};

use arrow::array::LargeBinaryArray;

use crate::{
    container::{RowSelection, column_binary, column_u64, scan_blob},
    error::{Result, VixError},
    postings,
    query::{KEY_FIELD_ID, split_key, write_composite},
    reader::VixReader,
};

/// How one merge input's doc ids translate to the merged file's doc ids.
///
/// Produced by the compactor's `_timestamp` row merge: when the inputs'
/// time ranges do not interleave, every input occupies one contiguous run of
/// the output and its map is a constant [`DocIdMap::Offset`]; otherwise the
/// full permutation is spelled out as a [`DocIdMap::Table`] (`table[old_id]
/// = new_id`).
#[derive(Debug, Clone)]
pub enum DocIdMap {
    /// `new_id = old_id + offset` — the input's rows form one contiguous run.
    Offset(u32),
    /// `new_id = table[old_id]`; the table length must equal the input's row
    /// count and the mapping must be injective across all inputs.
    Table(Vec<u32>),
}

/// The whole `terms` table of one input, decoded once up front: postings
/// stay in their encoded (delta + bitpacked) form as binary-array slices.
struct TermTable {
    doc_counts: Vec<u32>,
    postings: Vec<LargeBinaryArray>,
    /// First global ordinal of each `postings` batch.
    batch_starts: Vec<u64>,
}

impl TermTable {
    fn load(reader: &VixReader) -> Result<Self> {
        let term_count = reader.term_count();
        let mut table = TermTable {
            doc_counts: Vec::with_capacity(term_count as usize),
            postings: Vec::new(),
            batch_starts: Vec::new(),
        };
        if term_count == 0 {
            return Ok(table);
        }
        let blob = reader
            .terms_blob_handle()
            .ok_or_else(|| VixError::Malformed("missing terms blob".to_string()))?;
        let row_count = reader.row_count();
        for batch in scan_blob(blob, Some(&["doc_count", "postings"]), RowSelection::All)? {
            let doc_counts = column_u64(&batch, "doc_count")?;
            for &doc_count in &doc_counts {
                if doc_count > row_count {
                    return Err(VixError::Malformed(format!(
                        "doc_count {doc_count} exceeds row_count {row_count}"
                    )));
                }
                table.doc_counts.push(doc_count as u32);
            }
            table
                .batch_starts
                .push(table.doc_counts.len() as u64 - doc_counts.len() as u64);
            table.postings.push(column_binary(&batch, "postings")?);
        }
        if table.doc_counts.len() as u64 != term_count {
            return Err(VixError::Malformed(format!(
                "terms table has {} rows, expected {term_count}",
                table.doc_counts.len()
            )));
        }
        Ok(table)
    }

    fn doc_count(&self, ordinal: u64) -> u32 {
        self.doc_counts[ordinal as usize]
    }

    fn postings_blob(&self, ordinal: u64) -> &[u8] {
        let batch = self
            .batch_starts
            .partition_point(|&start| start <= ordinal)
            .saturating_sub(1);
        self.postings[batch].value((ordinal - self.batch_starts[batch]) as usize)
    }
}

/// One input's term stream over one key range (`[lower, upper)` given in
/// the OUTPUT key space; `None` = unbounded), with keys remapped into the
/// output id space and guarded to stay strictly ascending. The bounds are
/// translated into THIS input's own key space on construction
/// ([`translate_bound`]) so the raw-key comparisons below are exact — the
/// same output key falls on the same side of a bound in every input.
struct RemappedTermStream<'r> {
    reader: &'r VixReader,
    /// The whole dictionary blocks region (compaction inputs are in-memory
    /// blobs — this is a zero-copy clone, one per stream).
    blocks: bytes::Bytes,
    /// Current block id and the decode offset/prev-key state within it
    /// (an incremental [`crate::dict_blocks::BlockIter`] cannot borrow
    /// `blocks` inside self, so the iterator state is inlined).
    block_id: usize,
    block_pos: usize,
    block_started: bool,
    raw_key: Vec<u8>,
    block_ordinal_pos: u64,
    /// Range bounds TRANSLATED into this input's key space.
    lower: Option<Vec<u8>>,
    upper: Option<Vec<u8>>,
    /// `old field id -> Some(output field id)`; `None` = the field has no
    /// output id and its value terms are dropped.
    field_map: Vec<Option<u16>>,
    /// `old field id -> field name` (dropped-field reporting).
    field_names: Vec<String>,
    cur_key: Vec<u8>,
    prev_key: Vec<u8>,
    cur_ordinal: u64,
    started: bool,
    /// Old field ids that had at least one value term dropped.
    dropped: BTreeSet<u16>,
}

impl<'r> RemappedTermStream<'r> {
    /// `lower`/`upper` are OUTPUT-key-space range bounds (from
    /// [`partition_bounds`]); they are translated into this input's key
    /// space here, and [`Self::advance`] compares raw keys against the
    /// translations.
    fn new<S: BuildHasher>(
        reader: &'r VixReader,
        out_field_ids: &HashMap<String, u16, S>,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
    ) -> Result<Self> {
        let entries = reader.field_entries();
        let mut field_map = Vec::with_capacity(entries.len());
        let mut field_names = Vec::with_capacity(entries.len());
        for entry in entries {
            field_map.push(out_field_ids.get(&entry.name).copied());
            field_names.push(entry.name.clone());
        }
        let lower = lower.map(|b| translate_bound(b, &field_map)).transpose()?;
        let upper = upper.map(|b| translate_bound(b, &field_map)).transpose()?;
        let (start_block, blocks) = if reader.term_count() == 0 {
            // a zero-term input has no dictionary blobs at all: an
            // exhausted stream, not an error
            (usize::MAX, bytes::Bytes::new())
        } else {
            let start = match &lower {
                Some(lower) => reader.dict_index()?.predecessor_block(lower)?.unwrap_or(0),
                None => 0,
            };
            (start, reader.dict_blocks_all_for_merge()?)
        };
        Ok(Self {
            reader,
            blocks,
            block_id: start_block,
            block_pos: 0,
            block_started: false,
            raw_key: Vec::new(),
            block_ordinal_pos: 0,
            lower,
            upper,
            field_map,
            field_names,
            cur_key: Vec::new(),
            prev_key: Vec::new(),
            cur_ordinal: 0,
            started: false,
            dropped: BTreeSet::new(),
        })
    }

    /// Decode the next raw key of the current block into `self.raw_key`.
    /// `Ok(false)` = block exhausted.
    fn next_raw_key(&mut self) -> Result<bool> {
        let index = self.reader.dict_index()?;
        let range = index.block_range(self.block_id, self.blocks.len() as u64);
        let block = &self.blocks[range.start as usize..range.end as usize];
        if !self.block_started {
            self.block_started = true;
            self.block_pos = 0;
            self.block_ordinal_pos = 0;
            if block.is_empty() {
                return Ok(false);
            }
            let len = u16::from_le_bytes(
                block
                    .get(0..2)
                    .ok_or_else(|| VixError::Malformed("dict block header truncated".to_string()))?
                    .try_into()
                    .unwrap(),
            ) as usize;
            self.raw_key.clear();
            self.raw_key
                .extend_from_slice(block.get(2..2 + len).ok_or_else(|| {
                    VixError::Malformed("dict block first key truncated".to_string())
                })?);
            self.block_pos = 2 + len;
            return Ok(true);
        }
        if self.block_pos >= block.len() {
            return Ok(false);
        }
        let (shared, suffix_len) =
            crate::dict_blocks::read_two_varints(block, &mut self.block_pos)?;
        if shared > self.raw_key.len() {
            return Err(VixError::Malformed(
                "dict block shared prefix exceeds previous key".to_string(),
            ));
        }
        let suffix = block
            .get(self.block_pos..self.block_pos + suffix_len)
            .ok_or_else(|| VixError::Malformed("dict block suffix truncated".to_string()))?;
        self.block_pos += suffix_len;
        self.raw_key.truncate(shared);
        self.raw_key.extend_from_slice(suffix);
        self.block_ordinal_pos += 1;
        Ok(true)
    }

    /// Advance to the next surviving term. `Ok(false)` = exhausted. After
    /// `Ok(true)`, `cur_key`/`cur_ordinal` describe the term.
    fn advance(&mut self) -> Result<bool> {
        loop {
            if self.reader.term_count() == 0 {
                return Ok(false);
            }
            let dict_index = self.reader.dict_index()?;
            if self.block_id >= dict_index.block_count() {
                return Ok(false);
            }
            if !self.next_raw_key()? {
                self.block_id += 1;
                self.block_started = false;
                continue;
            }
            // Range bounds on the raw key space — sound because the bounds
            // are this input's OWN translations of the output-space range
            // ([`translate_bound`]): exact on every emittable key under the
            // order-preserving remap [`partition_bounds`] pre-checks, and a
            // consistent tiling for dropped-field keys (each raw key falls
            // in exactly one range's walk, so dropped reporting stays
            // complete).
            if let Some(lower) = &self.lower
                && self.raw_key.as_slice() < lower.as_slice()
            {
                continue;
            }
            if let Some(upper) = &self.upper
                && self.raw_key.as_slice() >= upper.as_slice()
            {
                return Ok(false);
            }
            let first_ordinal = dict_index.meta(self.block_id).1;
            let local = self.block_ordinal_pos;
            let key: &[u8] = &self.raw_key;
            let Some((token, field_id)) = split_key(key) else {
                return Err(VixError::Malformed(format!(
                    "dictionary key too short to carry a field-id prefix: {key:?}"
                )));
            };
            std::mem::swap(&mut self.cur_key, &mut self.prev_key);
            if field_id == KEY_FIELD_ID {
                // key terms carry the reserved marker in every file
                self.cur_key.clear();
                self.cur_key.extend_from_slice(key);
            } else {
                let mapped = self.field_map.get(field_id as usize).copied().flatten();
                let Some(new_id) = mapped else {
                    // no output field id: the term is dropped (the caller
                    // marks the field partial, like a rebuild would)
                    self.dropped.insert(field_id);
                    std::mem::swap(&mut self.cur_key, &mut self.prev_key);
                    continue;
                };
                write_composite(&mut self.cur_key, token, new_id);
            }
            if self.started && self.cur_key <= self.prev_key {
                // field-id remapping is order-preserving for real-world keys
                // (ids are assigned by sorted field name everywhere), but a
                // remap that reorders ids could still reorder keys — bail
                // out so the caller falls back to a rebuild
                return Err(VixError::Malformed(
                    "remapped term stream is not strictly ascending; the merged dictionary \
                     cannot be built from the inputs"
                        .to_string(),
                ));
            }
            self.started = true;
            self.cur_ordinal = first_ordinal + local;
            return Ok(true);
        }
    }

    /// Names of the fields that had value terms dropped.
    fn dropped_field_names(&self) -> impl Iterator<Item = &str> {
        self.dropped
            .iter()
            .filter_map(|&id| self.field_names.get(id as usize).map(String::as_str))
    }
}

/// The merged index of [`merge_indexes`].
pub(crate) struct MergedIndexResult {
    /// The index blob bytes; `None` when the inputs carry no terms.
    pub blobs: Option<crate::writer::IndexBlobs>,
    pub term_count: u64,
    /// Per-file value-bloom hashes for the MERGED file (collected by the
    /// k-way workers over the deduplicated output terms).
    pub bloom: crate::bloom::BloomHashAcc,
    /// Fields whose value terms were dropped for lack of an output field id.
    pub dropped: BTreeSet<String>,
}

/// Merge the inputs' term dictionaries into the final `dict`/`terms` blobs.
///
/// The OUTPUT key space is partitioned into disjoint ranges (bounds are real
/// input dict-block first keys remapped into the output id space — see
/// [`partition_bounds`]); each range k-way merges independently on a worker
/// thread into its own [`TermSink`], and the sinks' parts are stitched into
/// the blobs ([`write_index_blobs`] rebases the row-group ordinals and
/// hard-rejects out-of-order parts). `threads == 0` uses the machine's
/// available parallelism; `kway_threads` is the #51b range-parallelism knob
/// (`0` = `min(available_parallelism, 8)`, `1` = exactly one range — the
/// sequential path through the same code), additionally capped by `threads`
/// so it never exceeds the per-merge pool budget.
#[allow(clippy::too_many_arguments)]
pub(crate) fn merge_indexes<MapState, SetState>(
    inputs: &[&VixReader],
    doc_maps: &[DocIdMap],
    out_field_ids: &HashMap<String, u16, MapState>,
    bloom_field_names: &[String],
    composite_pairs: &[(u16, String)],
    bloom_only_fids: &HashSet<u16, SetState>,
    total_rows: u64,
    postings_chunk_bytes: usize,
    plist_min_docs: u32,
    threads: usize,
    kway_threads: usize,
) -> Result<MergedIndexResult>
where
    MapState: BuildHasher + Sync,
    SetState: BuildHasher + Sync,
{
    debug_assert_eq!(inputs.len(), doc_maps.len());
    let threads = if threads == 0 {
        std::thread::available_parallelism().map_or(1, |n| n.get())
    } else {
        threads
    };
    // #51b range parallelism: default capped at 8 (diminishing returns and
    // per-range sink overhead beyond that), and never above the per-merge
    // thread budget (`ZO_VIX_MERGE_KWAY_THREADS` stacks with
    // `ZO_VIX_MERGE_THREAD_NUM`, it does not widen it).
    let kway = if kway_threads == 0 {
        std::thread::available_parallelism()
            .map_or(1, |n| n.get())
            .min(8)
    } else {
        kway_threads
    }
    .min(threads);

    let started = std::time::Instant::now();
    let tables: Vec<TermTable> = if threads > 1 && inputs.len() > 1 {
        std::thread::scope(|scope| {
            let handles: Vec<_> = inputs
                .iter()
                .map(|reader| scope.spawn(move || TermTable::load(reader)))
                .collect();
            handles
                .into_iter()
                .map(|handle| handle.join().expect("term-table load panicked"))
                .collect::<Result<_>>()
        })?
    } else {
        inputs
            .iter()
            .map(|reader| TermTable::load(reader))
            .collect::<Result<_>>()?
    };
    log::debug!(
        "vix merge: loaded {} term tables in {:?}",
        tables.len(),
        started.elapsed()
    );

    // Over-partition (4 ranges per k-way worker) and let the workers pull
    // ranges off a shared cursor: the sampled bounds only approximate work
    // quantiles, so static one-range-per-thread assignment loses half its
    // speedup to skew. `kway <= 1` asks for one range (no bounds) — the
    // sequential path through the same code.
    let started = std::time::Instant::now();
    let bounds = if kway > 1 {
        partition_bounds(inputs, out_field_ids, kway.saturating_mul(4))?
    } else {
        Vec::new()
    };
    type KeyRange<'b> = (Option<&'b [u8]>, Option<&'b [u8]>);
    let mut ranges: Vec<KeyRange<'_>> = Vec::with_capacity(bounds.len() + 1);
    {
        let mut lower: Option<&[u8]> = None;
        for bound in &bounds {
            ranges.push((lower, Some(bound.as_slice())));
            lower = Some(bound.as_slice());
        }
        ranges.push((lower, None));
    }
    type RangeOutput = (crate::writer::TermSinkParts, BTreeSet<String>);
    let outputs: Vec<RangeOutput> = if ranges.len() > 1 {
        use std::sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
        };

        let next = AtomicUsize::new(0);
        let slots: Vec<Mutex<Option<Result<RangeOutput>>>> =
            (0..ranges.len()).map(|_| Mutex::new(None)).collect();
        std::thread::scope(|scope| {
            for _ in 0..kway.min(ranges.len()) {
                scope.spawn(|| {
                    loop {
                        let index = next.fetch_add(1, Ordering::Relaxed);
                        let Some(&(lower, upper)) = ranges.get(index) else {
                            break;
                        };
                        let result = merge_term_range(
                            inputs,
                            &tables,
                            doc_maps,
                            out_field_ids,
                            bloom_field_names,
                            composite_pairs,
                            bloom_only_fids,
                            total_rows,
                            postings_chunk_bytes,
                            plist_min_docs,
                            lower,
                            upper,
                        );
                        let failed = result.is_err();
                        *slots[index].lock().expect("range slot poisoned") = Some(result);
                        if failed {
                            break;
                        }
                    }
                });
            }
        });
        // Surface the first error (in range order). A `None` slot can only
        // remain when every worker stopped on an error before reaching it.
        let mut outputs = Vec::with_capacity(slots.len());
        let mut first_error = None;
        for slot in slots {
            match slot.into_inner().expect("range slot poisoned") {
                Some(Ok(output)) => outputs.push(output),
                Some(Err(e)) => {
                    first_error.get_or_insert(e);
                }
                None => {}
            }
        }
        if let Some(e) = first_error {
            return Err(e);
        }
        outputs
    } else {
        vec![merge_term_range(
            inputs,
            &tables,
            doc_maps,
            out_field_ids,
            bloom_field_names,
            composite_pairs,
            bloom_only_fids,
            total_rows,
            postings_chunk_bytes,
            plist_min_docs,
            None,
            None,
        )?]
    };
    let merged_at = started.elapsed();

    let mut parts = Vec::with_capacity(outputs.len());
    let mut dropped = BTreeSet::new();
    for (part, part_dropped) in outputs {
        parts.push(part);
        dropped.extend(part_dropped);
    }
    let (blobs, term_count, bloom) = crate::writer::write_index_blobs(parts, threads)?;
    log::debug!(
        "vix merge: k-way term merge ({} ranges, {} workers) {merged_at:?}, dict/terms encode \
         {:?} ({term_count} terms)",
        ranges.len(),
        kway.min(ranges.len()),
        started.elapsed() - merged_at,
    );
    Ok(MergedIndexResult {
        blobs,
        bloom,
        term_count,
        dropped,
    })
}

/// Key-range split points for the parallel k-way merge, expressed in the
/// OUTPUT key space (M10/#51b — the v1 sampler used raw input bytes as
/// shared bounds and corrupted prod dictionaries on 2026-07-29: under
/// field-major keys one raw-byte bound cuts different inputs at DIFFERENT
/// fields, so the per-range sinks stopped covering disjoint ascending
/// output ranges).
///
/// Invariants:
/// - Every bound is a REAL key: an input dict-block first key (read from the resident dict-block
///   index, no block decode) remapped into the output id space — `{output fid u16 BE}{token}`.
///   Nothing is ever fabricated by byte arithmetic.
/// - Bounds are strictly ascending and deduplicated; `ranges <= 1`, no candidates, or a
///   non-order-preserving remap yield NO bounds (one range — the sequential path). Split points are
///   weighted quantiles over the candidate blocks' key counts, so ranges approximate equal key work
///   regardless of input skew.
/// - Bounds are only emitted when EVERY input's `input fid -> output fid` map is strictly
///   increasing (checked here). Each range worker then translates a bound into each input's own key
///   space ([`translate_bound`]) — exact on every emittable key — so all instances of one output
///   key land in exactly one range.
/// - [`crate::writer::write_index_blobs`] hard-rejects out-of-order parts as the structural
///   backstop regardless.
pub(crate) fn partition_bounds<S: BuildHasher>(
    inputs: &[&VixReader],
    out_field_ids: &HashMap<String, u16, S>,
    ranges: usize,
) -> Result<Vec<Vec<u8>>> {
    if ranges <= 1 {
        return Ok(Vec::new());
    }
    // The translation (and the raw-key range filter it feeds) is only exact
    // when every input's remap is order-preserving. Ids are assigned by
    // sorted field name everywhere so this always holds in practice; a
    // pathological map falls back to the single-range path, where the
    // in-stream strictness guard still governs.
    for reader in inputs {
        let mut prev: Option<u16> = None;
        for entry in reader.field_entries() {
            let Some(&out_id) = out_field_ids.get(&entry.name) else {
                continue;
            };
            if prev.is_some_and(|p| p >= out_id) {
                return Ok(Vec::new());
            }
            prev = Some(out_id);
        }
    }
    // Candidates: every input's dict-block first keys, remapped into the
    // output key space, weighted by the block's key count.
    let mut candidates: Vec<(Vec<u8>, u64)> = Vec::new();
    for reader in inputs {
        let term_count = reader.term_count();
        if term_count == 0 {
            continue;
        }
        let entries = reader.field_entries();
        let index = reader.dict_index()?;
        let mut walk_error: Option<VixError> = None;
        index.walk_first_keys(|block, key| {
            let Some((token, fid)) = split_key(key) else {
                walk_error = Some(VixError::Malformed(format!(
                    "dict-block first key too short to carry a field-id prefix: {key:?}"
                )));
                return false;
            };
            let remapped = if fid == KEY_FIELD_ID {
                key.to_vec()
            } else {
                let out_id = entries
                    .get(fid as usize)
                    .and_then(|entry| out_field_ids.get(&entry.name));
                let Some(&out_id) = out_id else {
                    // no output id: the block starts on a dropped field's
                    // key, which never reaches the output — not a candidate
                    return true;
                };
                let mut k = Vec::new();
                write_composite(&mut k, token, out_id);
                k
            };
            candidates.push((remapped, index.block_key_count(block, term_count)));
            true
        })?;
        if let Some(e) = walk_error {
            return Err(e);
        }
    }
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    candidates.sort_unstable();
    // merge duplicate keys (equal first keys across inputs), summing weights
    let mut merged: Vec<(Vec<u8>, u64)> = Vec::with_capacity(candidates.len());
    for (key, weight) in candidates {
        match merged.last_mut() {
            Some((last, sum)) if *last == key => *sum = sum.saturating_add(weight),
            _ => merged.push((key, weight)),
        }
    }
    let total: u128 = merged.iter().map(|(_, w)| u128::from(*w)).sum();
    // R-1 weighted quantiles: bound r = the first candidate whose
    // strictly-before cumulative weight reaches total * r / ranges.
    let mut bounds: Vec<Vec<u8>> = Vec::with_capacity(ranges - 1);
    let mut cum: u128 = 0;
    let mut iter = merged.into_iter();
    let mut current = iter.next();
    for r in 1..ranges {
        let target = total * r as u128 / ranges as u128;
        while let Some((_, weight)) = &current {
            if cum >= target {
                break;
            }
            cum += u128::from(*weight);
            current = iter.next();
        }
        match &current {
            Some((key, _)) => {
                if bounds.last().is_none_or(|last| last < key) {
                    bounds.push(key.clone());
                }
            }
            None => break,
        }
    }
    Ok(bounds)
}

/// Translate an OUTPUT-key-space range bound into one input's own key
/// space: the returned byte string `T` satisfies, for every key `k` the
/// input can emit, `k >= T ⟺ remap(k) >= bound` — provided the input's
/// `input fid -> output fid` map is strictly increasing over mapped fields
/// ([`partition_bounds`] only emits bounds after checking exactly that).
/// Dropped-field keys (no output id, never emitted) still get a definite
/// side, so consecutive ranges tile each input's raw key space exactly.
///
/// Cases, scanning mapped input fids in ascending order:
/// - first fid whose output id EQUALS the bound's fid: `{fid}{bound token}` (byte-exact within the
///   field);
/// - first fid whose output id EXCEEDS it: `{fid}` (2 bytes — every key of that field and beyond
///   remaps at/above the bound);
/// - none: only the key-term region (fid [`KEY_FIELD_ID`], identical bytes in every file) can still
///   reach the bound — the bound itself when it IS a key-term bound, else the region prefix
///   `{KEY_FIELD_ID}`.
pub(crate) fn translate_bound(bound: &[u8], field_map: &[Option<u16>]) -> Result<Vec<u8>> {
    let Some((token, out_fid)) = split_key(bound) else {
        return Err(VixError::Malformed(format!(
            "range bound too short to carry a field-id prefix: {bound:?}"
        )));
    };
    for (fid, mapped) in field_map.iter().enumerate() {
        let Some(out_id) = mapped else { continue };
        debug_assert!(u16::try_from(fid).is_ok(), "field table exceeds u16 ids");
        if *out_id > out_fid {
            return Ok((fid as u16).to_be_bytes().to_vec());
        }
        if *out_id == out_fid {
            let mut k = Vec::new();
            write_composite(&mut k, token, fid as u16);
            return Ok(k);
        }
    }
    if out_fid == KEY_FIELD_ID {
        Ok(bound.to_vec())
    } else {
        Ok(KEY_FIELD_ID.to_be_bytes().to_vec())
    }
}

/// K-way merge one key range (`[lower, upper)` in the OUTPUT key space,
/// `None` = unbounded; each stream translates the bounds into its own
/// input's key space) of the inputs' remapped term streams into a fresh
/// [`TermSink`]. Postings cells are final: dense terms (`doc_count ==
/// total_rows`) are elided to the empty cell — taking precedence over the
/// plist threshold — and terms at/above `plist_min_docs` go out-of-row into
/// the sink's plist region as [`postings::encode_record`] bytes. Also
/// returns the names of the fields whose value terms were dropped for lack
/// of an output field id.
#[allow(clippy::too_many_arguments)]
fn merge_term_range<MapState: BuildHasher, SetState: BuildHasher>(
    inputs: &[&VixReader],
    tables: &[TermTable],
    doc_maps: &[DocIdMap],
    out_field_ids: &HashMap<String, u16, MapState>,
    bloom_field_names: &[String],
    composite_pairs: &[(u16, String)],
    bloom_only_fids: &HashSet<u16, SetState>,
    total_rows: u64,
    postings_chunk_bytes: usize,
    plist_min_docs: u32,
    lower: Option<&[u8]>,
    upper: Option<&[u8]>,
) -> Result<(crate::writer::TermSinkParts, BTreeSet<String>)> {
    let bloom_pairs: Vec<(u16, String)> = bloom_field_names
        .iter()
        .filter_map(|n| out_field_ids.get(n).map(|id| (*id, n.clone())))
        .collect();
    // The fast path splits input keys and rebuilds them under the output
    // field ids; the remap is order-preserving (ids are assigned by sorted
    // field name everywhere), backstopped by the strictly-ascending check
    // in RemappedTermStream::advance.
    let mut bloom_acc = crate::bloom::BloomHashAcc::from_pairs(bloom_pairs);
    if !composite_pairs.is_empty() {
        // #48: same reserved composite section as the writer build. The
        // caller computes the ELIGIBLE pairs (value-lookup-capable term
        // fields only — no fts, no merge-demoted): tokens are not raw
        // values and demoted fields carry incomplete value terms, so
        // claiming coverage for either would wrongly drop files on
        // equality probes.
        bloom_acc.enable_composite(composite_pairs.iter().cloned());
    }
    let mut sink = crate::writer::TermSink::new(postings_chunk_bytes)
        .with_bloom(bloom_acc)
        .with_plist_min_docs(plist_min_docs);
    let mut streams: Vec<RemappedTermStream<'_>> = inputs
        .iter()
        .map(|reader| RemappedTermStream::new(reader, out_field_ids, lower, upper))
        .collect::<Result<_>>()?;
    let mut alive: Vec<bool> = Vec::with_capacity(streams.len());
    for stream in &mut streams {
        alive.push(stream.advance()?);
    }

    let mut key: Vec<u8> = Vec::new();
    let mut contributors: Vec<usize> = Vec::new();
    let mut ids: Vec<u32> = Vec::new();
    let mut blob: Vec<u8> = Vec::new();
    let mut encode_scratch: Vec<u8> = Vec::new();
    loop {
        // smallest current key across the live streams (k is small: a linear
        // scan beats a heap's per-term allocations)
        contributors.clear();
        for (index, stream) in streams.iter().enumerate() {
            if !alive[index] {
                continue;
            }
            if contributors.is_empty() {
                contributors.push(index);
                continue;
            }
            match stream.cur_key.cmp(&streams[contributors[0]].cur_key) {
                std::cmp::Ordering::Less => {
                    contributors.clear();
                    contributors.push(index);
                }
                std::cmp::Ordering::Equal => contributors.push(index),
                std::cmp::Ordering::Greater => {}
            }
        }
        if contributors.is_empty() {
            break;
        }
        key.clear();
        key.extend_from_slice(&streams[contributors[0]].cur_key);

        // #52 bloom-only output fields: legacy inputs' dictionary terms for
        // them feed the bloom accumulation and NOTHING else — no postings
        // union, no dictionary entry. This is where a term-indexed history
        // converges to the bloom-only plan.
        if let Some((_, fid)) = crate::query::split_key(&key)
            && fid != crate::query::KEY_FIELD_ID
            && bloom_only_fids.contains(&fid)
        {
            sink.observe_bloom_only_key(&key);
            for &index in &contributors {
                alive[index] = streams[index].advance()?;
            }
            continue;
        }

        let mut doc_count = 0u64;
        for &index in &contributors {
            doc_count += u64::from(tables[index].doc_count(streams[index].cur_ordinal));
        }
        if doc_count > total_rows {
            return Err(VixError::Malformed(format!(
                "merged doc_count {doc_count} exceeds the merged row count {total_rows} \
                 (doc-id maps overlap?)"
            )));
        }

        blob.clear();
        if doc_count == total_rows && total_rows > 0 {
            // dense in the merged file: elide (re-checked against the merged
            // row count, independent of the inputs' density) — takes
            // precedence over the plist threshold, a dense term is never a
            // pointer cell
            sink.push(&key, doc_count as u32, &blob)?;
        } else {
            let as_record = sink.plist_eligible(doc_count);
            merge_postings(
                &streams,
                tables,
                inputs,
                doc_maps,
                &contributors,
                doc_count,
                as_record,
                &mut ids,
                &mut blob,
                &mut encode_scratch,
            )?;
            if as_record {
                sink.push_plist(&key, doc_count as u32, &blob)?;
            } else {
                sink.push(&key, doc_count as u32, &blob)?;
            }
        }

        for &index in &contributors {
            alive[index] = streams[index].advance()?;
        }
    }

    let mut dropped = BTreeSet::new();
    for stream in &streams {
        dropped.extend(stream.dropped_field_names().map(str::to_string));
    }
    Ok((sink.into_parts()?, dropped))
}

/// Union the contributors' postings into `blob`, remapping doc ids through
/// the inputs' [`DocIdMap`]s. `as_record = false` encodes the plain inline
/// [`postings::encode`] blob; `true` produces the out-of-row
/// [`postings::encode_record`] bytes (skip table + blob) instead.
#[allow(clippy::too_many_arguments)]
fn merge_postings(
    streams: &[RemappedTermStream<'_>],
    tables: &[TermTable],
    inputs: &[&VixReader],
    doc_maps: &[DocIdMap],
    contributors: &[usize],
    doc_count: u64,
    as_record: bool,
    ids: &mut Vec<u32>,
    blob: &mut Vec<u8>,
    encode_scratch: &mut Vec<u8>,
) -> Result<()> {
    // single contributor at offset 0: the cell's bytes are valid verbatim
    // when input and output agree on the representation — inline blob for an
    // inline output, resolved RECORD bytes (self-contained skip table +
    // blob, doc ids unchanged) for a record output. A representation
    // mismatch (inline input above the output threshold, or pointer input
    // below it) falls through to the decode + re-encode path.
    if let [index] = contributors
        && let DocIdMap::Offset(0) = doc_maps[*index]
    {
        let input_doc_count = u64::from(tables[*index].doc_count(streams[*index].cur_ordinal));
        let encoded = tables[*index].postings_blob(streams[*index].cur_ordinal);
        if !encoded.is_empty() {
            let input_pointer = inputs[*index].plist_pointer_cell(input_doc_count, encoded);
            match (as_record, input_pointer) {
                (false, false) => {
                    blob.extend_from_slice(encoded);
                    return Ok(());
                }
                (true, true) => {
                    let record = inputs[*index].plist_record_bytes(encoded)?;
                    blob.extend_from_slice(&record);
                    return Ok(());
                }
                _ => {} // representation mismatch: decode below
            }
        }
        // dense-elided in the input: fall through and expand it
    }

    let all_offsets = contributors
        .iter()
        .all(|&index| matches!(doc_maps[index], DocIdMap::Offset(_)));
    let mut order: Vec<usize> = contributors.to_vec();
    if all_offsets {
        // disjoint contiguous runs: appending in offset order keeps the
        // merged list ascending with no sort
        order.sort_unstable_by_key(|&index| match doc_maps[index] {
            DocIdMap::Offset(offset) => offset,
            DocIdMap::Table(_) => unreachable!("all_offsets checked above"),
        });
    }

    ids.clear();
    ids.reserve(doc_count as usize);
    for &index in &order {
        let input_rows = inputs[index].row_count();
        let input_doc_count = tables[index].doc_count(streams[index].cur_ordinal);
        let encoded = tables[index].postings_blob(streams[index].cur_ordinal);
        // out-of-row postings in the INPUT: resolve the pointer cell through
        // the input's plist blob and decode the record's blob region
        let record;
        let encoded = if inputs[index].plist_pointer_cell(u64::from(input_doc_count), encoded) {
            record = inputs[index].plist_record_bytes(encoded)?;
            postings::record_blob(&record)?
        } else {
            encoded
        };
        match (&doc_maps[index], encoded.is_empty() && input_doc_count > 0) {
            (DocIdMap::Offset(offset), true) => {
                // dense in the input: ids are 0..row_count
                check_input_dense(input_doc_count, input_rows)?;
                ids.extend(*offset..*offset + input_rows as u32);
            }
            (DocIdMap::Table(table), true) => {
                check_input_dense(input_doc_count, input_rows)?;
                ids.extend_from_slice(table);
            }
            (DocIdMap::Offset(offset), false) => {
                postings::decode_each(encoded, input_doc_count as usize, |doc| {
                    if u64::from(doc) >= input_rows {
                        return Err(doc_out_of_range(doc, input_rows));
                    }
                    ids.push(doc + *offset);
                    Ok(())
                })?;
            }
            (DocIdMap::Table(table), false) => {
                postings::decode_each(encoded, input_doc_count as usize, |doc| {
                    if u64::from(doc) >= input_rows {
                        return Err(doc_out_of_range(doc, input_rows));
                    }
                    ids.push(table[doc as usize]);
                    Ok(())
                })?;
            }
        }
    }
    if !all_offsets {
        // a permutation from a time-ordered interleave is not monotonic per
        // input in general. Production DocIdMap tables are monotonic for
        // non-interleaved inputs, however, so retain the already-sorted fast
        // path and sort only when the completed list proves it is needed.
        ensure_sorted_unique(ids)?;
    }
    if as_record {
        postings::encode_record_into(ids, blob, encode_scratch)?;
    } else {
        postings::encode_into(ids, blob)?;
    }
    Ok(())
}

/// Return whether a fallback sort was needed. Strictly monotonic production
/// table maps take the zero-sort path; permutations are sorted and overlapping
/// maps are rejected.
fn ensure_sorted_unique(ids: &mut [u32]) -> Result<bool> {
    if ids.windows(2).all(|pair| pair[0] < pair[1]) {
        return Ok(false);
    }
    ids.sort_unstable();
    if ids.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(VixError::Malformed(
            "merged postings contain a duplicate doc id (doc-id maps overlap)".to_string(),
        ));
    }
    Ok(true)
}

fn check_input_dense(doc_count: u32, input_rows: u64) -> Result<()> {
    if u64::from(doc_count) != input_rows {
        return Err(VixError::Malformed(format!(
            "empty postings blob for a term with doc_count {doc_count} != row_count \
             {input_rows} (not dense-elided, so corrupt)"
        )));
    }
    Ok(())
}

fn doc_out_of_range(doc: u32, input_rows: u64) -> VixError {
    VixError::Malformed(format!(
        "postings doc id {doc} out of range (row_count {input_rows})"
    ))
}

#[cfg(test)]
mod tests {
    use super::ensure_sorted_unique;

    #[test]
    fn monotonic_mapped_postings_skip_sort_with_fallback() {
        let mut monotonic = vec![1, 4, 9, 12];
        assert!(!ensure_sorted_unique(&mut monotonic).unwrap());
        assert_eq!(monotonic, [1, 4, 9, 12]);

        let mut interleaved = vec![1, 9, 4, 12];
        assert!(ensure_sorted_unique(&mut interleaved).unwrap());
        assert_eq!(interleaved, [1, 4, 9, 12]);

        let mut overlap = vec![1, 4, 4, 9];
        assert!(ensure_sorted_unique(&mut overlap).is_err());
    }
}
