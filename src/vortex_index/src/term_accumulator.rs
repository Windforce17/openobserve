// Copyright 2026 OpenObserve Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

//! Field-sharded, insertion-oriented term accumulation for index builds.
//!
//! Hash iteration order is deliberately never observable: every shard is
//! sorted by token before it reaches a spill run or the on-disk dictionary.

use std::collections::HashMap;

use rapidhash::fast::GlobalState;

use crate::query::KEY_FIELD_ID;

/// Keep the hasher behind one alias so benchmark results can swap it without
/// touching the accumulator. `GlobalState` avoids per-map secret generation;
/// its process-global seed is sufficient for this single-tenant in-memory
/// build state.
type FastMap<K, V> = HashMap<K, V, GlobalState>;

// Approximate occupied bucket bytes: two Vec headers plus hash/control data.
const HASH_SLOT_BYTES: usize = 56;
// Allocator metadata/rounding for the separately allocated key and postings.
const TERM_ALLOC_OVERHEAD: usize = 32;

pub(crate) struct SortedTermShard {
    pub(crate) field_id: u16,
    pub(crate) terms: Vec<(Vec<u8>, Vec<u32>)>,
}

/// One hash table per real field, plus the reserved key-presence field.
/// Postings remain `Vec<u32>` because docs arrive monotonically and appending
/// is substantially cheaper and smaller than a set per term.
pub(crate) struct TermAccumulator {
    fields: Vec<FastMap<Vec<u8>, Vec<u32>>>,
    key_terms: FastMap<Vec<u8>, Vec<u32>>,
    len: usize,
    estimated_bytes: usize,
}

impl TermAccumulator {
    pub(crate) fn new(field_count: usize) -> Self {
        let fields: Vec<_> = (0..field_count).map(|_| FastMap::default()).collect();
        let estimated_bytes = fields.capacity() * size_of::<FastMap<Vec<u8>, Vec<u32>>>()
            + size_of::<FastMap<Vec<u8>, Vec<u32>>>();
        Self {
            fields,
            key_terms: FastMap::default(),
            len: 0,
            estimated_bytes,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn estimated_bytes(&self) -> usize {
        self.estimated_bytes
    }

    pub(crate) fn field_len(&self, field_id: u16) -> usize {
        self.map(field_id).map_or(0, FastMap::len)
    }

    /// Append one doc, avoiding key allocation and a second hash-table probe
    /// on the overwhelmingly common occupied-key path.
    pub(crate) fn push(&mut self, field_id: u16, token: &[u8], doc: u32) {
        let Some(map) = self.map_mut(field_id) else {
            debug_assert!(false, "term field id {field_id} is outside the accumulator");
            return;
        };
        let (inserted, bytes) = push_map(map, token, doc);
        self.len += usize::from(inserted);
        self.estimated_bytes += bytes;
    }

    /// Append several already-ascending docs after one key lookup. Used by
    /// key-presence indexing, where one field contributes many rows at once.
    pub(crate) fn extend(
        &mut self,
        field_id: u16,
        token: &[u8],
        docs: impl IntoIterator<Item = u32>,
    ) {
        let Some(map) = self.map_mut(field_id) else {
            debug_assert!(false, "term field id {field_id} is outside the accumulator");
            return;
        };
        let (inserted, bytes) = extend_map(map, token, docs);
        self.len += usize::from(inserted);
        self.estimated_bytes += bytes;
    }

    /// Remove an entire real-field shard for AUTO bloom-only demotion.
    pub(crate) fn take_field(&mut self, field_id: u16) -> FastMap<Vec<u8>, Vec<u32>> {
        let Some(slot) = self.fields.get_mut(usize::from(field_id)) else {
            return FastMap::default();
        };
        let removed = std::mem::take(slot);
        self.len -= removed.len();
        // AUTO demotion happens only at finish, after the last spill check;
        // avoid an O(all remaining terms) recount for every selected field.
        removed
    }

    /// Drain and sort each non-empty field. The outer vector is field-major,
    /// therefore concatenating the shards exactly reproduces composite-key
    /// lexicographic order without allocating a composite key per term.
    pub(crate) fn drain_sorted_shards(&mut self) -> Vec<SortedTermShard> {
        let replacement = Self::new(self.fields.len());
        let old = std::mem::replace(self, replacement);
        old.into_sorted_shards()
    }

    pub(crate) fn into_sorted_shards(self) -> Vec<SortedTermShard> {
        let mut shards = Vec::new();
        for (field_id, map) in self.fields.into_iter().enumerate() {
            Self::push_sorted_shard(&mut shards, field_id as u16, map);
        }
        Self::push_sorted_shard(&mut shards, KEY_FIELD_ID, self.key_terms);
        shards
    }

    fn push_sorted_shard(
        shards: &mut Vec<SortedTermShard>,
        field_id: u16,
        map: FastMap<Vec<u8>, Vec<u32>>,
    ) {
        if map.is_empty() {
            return;
        }
        let mut terms: Vec<_> = map.into_iter().collect();
        terms.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        shards.push(SortedTermShard { field_id, terms });
    }

    fn map(&self, field_id: u16) -> Option<&FastMap<Vec<u8>, Vec<u32>>> {
        if field_id == KEY_FIELD_ID {
            Some(&self.key_terms)
        } else {
            self.fields.get(usize::from(field_id))
        }
    }

    fn map_mut(&mut self, field_id: u16) -> Option<&mut FastMap<Vec<u8>, Vec<u32>>> {
        if field_id == KEY_FIELD_ID {
            Some(&mut self.key_terms)
        } else {
            self.fields.get_mut(usize::from(field_id))
        }
    }
}

fn push_map(map: &mut FastMap<Vec<u8>, Vec<u32>>, token: &[u8], doc: u32) -> (bool, usize) {
    let before_map_capacity = map.capacity();
    if let Some(postings) = map.get_mut(token) {
        if postings.last() == Some(&doc) {
            return (false, 0);
        }
        let before = postings.capacity();
        postings.push(doc);
        return (false, (postings.capacity() - before) * size_of::<u32>());
    }

    let key = token.to_vec();
    let key_capacity = key.capacity();
    let postings = vec![doc];
    let postings_capacity = postings.capacity();
    map.insert(key, postings);
    (
        true,
        (map.capacity() - before_map_capacity) * HASH_SLOT_BYTES
            + key_capacity
            + postings_capacity * size_of::<u32>()
            + TERM_ALLOC_OVERHEAD,
    )
}

fn extend_map(
    map: &mut FastMap<Vec<u8>, Vec<u32>>,
    token: &[u8],
    docs: impl IntoIterator<Item = u32>,
) -> (bool, usize) {
    let before_map_capacity = map.capacity();
    if let Some(postings) = map.get_mut(token) {
        let before = postings.capacity();
        for doc in docs {
            if postings.last() != Some(&doc) {
                postings.push(doc);
            }
        }
        return (false, (postings.capacity() - before) * size_of::<u32>());
    }

    let mut postings = Vec::new();
    for doc in docs {
        if postings.last() != Some(&doc) {
            postings.push(doc);
        }
    }
    if postings.is_empty() {
        return (false, 0);
    }
    let key = token.to_vec();
    let key_capacity = key.capacity();
    let postings_capacity = postings.capacity();
    map.insert(key, postings);
    (
        true,
        (map.capacity() - before_map_capacity) * HASH_SLOT_BYTES
            + key_capacity
            + postings_capacity * size_of::<u32>()
            + TERM_ALLOC_OVERHEAD,
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn shards_sort_and_dedupe_without_changing_field_order() {
        let mut terms = TermAccumulator::new(2);
        terms.push(1, b"z", 3);
        terms.push(0, b"b", 1);
        terms.push(0, b"a", 2);
        terms.push(0, b"a", 2);
        terms.extend(KEY_FIELD_ID, b"field", [0, 1, 1, 2]);

        let shards = terms.into_sorted_shards();
        assert_eq!(
            shards.iter().map(|s| s.field_id).collect::<Vec<_>>(),
            [0, 1, KEY_FIELD_ID]
        );
        assert_eq!(shards[0].terms[0], (b"a".to_vec(), vec![2]));
        assert_eq!(shards[0].terms[1], (b"b".to_vec(), vec![1]));
        assert_eq!(shards[2].terms[0].1, vec![0, 1, 2]);
    }

    #[test]
    fn capacity_accounting_tracks_reserved_postings_space() {
        let mut terms = TermAccumulator::new(1);
        terms.extend(0, b"value", 0..1024);
        let bytes = terms.estimated_bytes();
        assert!(bytes >= 1024 * size_of::<u32>() + b"value".len());
        assert_eq!(terms.len(), 1);
    }

    /// Run with:
    /// `cargo test -p vortex_index benchmark_hash_shards_against_btree --release -- --ignored
    /// --nocapture`
    ///
    /// Corpus construction and output validation are outside both timings;
    /// the candidate timing includes its mandatory emit-time shard sort, so
    /// it is comparable with the BTreeMap's insertion-time ordering cost.
    #[test]
    #[ignore = "manual postings-accumulator microbenchmark"]
    fn benchmark_hash_shards_against_btree() {
        const EVENTS: usize = 400_000;
        const FIELDS: u16 = 8;
        const VALUES_PER_FIELD: usize = 25_000;

        let corpus: Vec<(u16, Vec<u8>, u32)> = (0..EVENTS)
            .map(|event| {
                let field = event as u16 % FIELDS;
                let value = (event / usize::from(FIELDS)) % VALUES_PER_FIELD;
                (
                    field,
                    format!("value-{value:05}").into_bytes(),
                    event as u32,
                )
            })
            .collect();

        let started = std::time::Instant::now();
        let mut candidate = TermAccumulator::new(FIELDS as usize);
        for (field, token, doc) in &corpus {
            candidate.push(*field, token, *doc);
        }
        let candidate_bytes = candidate.estimated_bytes();
        let candidate = candidate.into_sorted_shards();
        let candidate_elapsed = started.elapsed();

        let started = std::time::Instant::now();
        let mut baseline: BTreeMap<Vec<u8>, Vec<u32>> = BTreeMap::new();
        for (field, token, doc) in &corpus {
            let mut key = Vec::with_capacity(2 + token.len());
            key.extend_from_slice(&field.to_be_bytes());
            key.extend_from_slice(token);
            let postings = baseline.entry(key).or_default();
            if postings.last() != Some(doc) {
                postings.push(*doc);
            }
        }
        let baseline_elapsed = started.elapsed();

        let candidate_flat: Vec<(Vec<u8>, Vec<u32>)> = candidate
            .into_iter()
            .flat_map(|shard| {
                shard.terms.into_iter().map(move |(token, ids)| {
                    let mut key = Vec::with_capacity(2 + token.len());
                    key.extend_from_slice(&shard.field_id.to_be_bytes());
                    key.extend_from_slice(&token);
                    (key, ids)
                })
            })
            .collect();
        assert_eq!(candidate_flat, baseline.into_iter().collect::<Vec<_>>());
        eprintln!(
            "events={EVENTS} distinct={} hash_shards_with_sort={candidate_elapsed:?} \
             btree={baseline_elapsed:?} estimated_candidate_bytes={candidate_bytes}",
            candidate_flat.len(),
        );
    }
}
