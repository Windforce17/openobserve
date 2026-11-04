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

//! Per-file result cache for vix index searches. Identity includes condition,
//! optimize rule, logical file key, immutable index generation, and exact
//! index size (`vix_result_cache_*` metrics).

use std::{
    collections::HashSet,
    sync::{
        Arc, LazyLock as Lazy,
        atomic::{AtomicUsize, Ordering},
    },
};

use config::{
    meta::{inverted_index::IndexOptimizeMode, stream::RowIdBitmap},
    metrics,
};
use dashmap::DashMap;

use super::VixSearchResult;

pub static GLOBAL_CACHE: Lazy<Arc<VixResultCache>> =
    Lazy::new(|| Arc::new(VixResultCache::default()));

#[derive(Debug, Clone)]
pub enum CacheEntry {
    /// (matched row ids as a compressed bitmap, row_group_size_from_index_file).
    /// The sparse form is the reason the cache can hold the needle-query
    /// working set: a dense per-row bitmap cost ~512 KB per 4M-row file no
    /// matter how few rows matched, capping the 256 MB budget at ~500
    /// entries; compressed needle entries cost bytes.
    RowIds(Arc<RowIdBitmap>, Option<u32>),
    /// simple select: (`(_timestamp, doc_id)` candidates, row_group_size)
    SelectCandidates(Arc<Vec<(i64, u32)>>, Option<u32>),
    /// simple count optimization
    Count(usize),
    /// simple histogram optimization, stored on its own absolute grid so a
    /// hit repositions into ANY query grid sharing the bucket width and
    /// phase — dashboard windows slide, per-file counts don't. `origin` is
    /// the absolute timestamp of `counts[0]`'s bucket start; leading and
    /// trailing zero buckets are trimmed at insert.
    Histogram {
        origin: i64,
        width: i64,
        counts: Vec<u64>,
    },
    /// multi histogram optimization, stored with absolute raw-timestamp
    /// bucket starts so zero-offset sliding-window hits can be filtered and
    /// exact nonzero-offset entries can rematerialize local bucket labels.
    MultiHistogram {
        width: i64,
        phase: i64,
        rows: Vec<(i64, String, u64)>,
    },
    /// group-by top-n optimization
    TopN(Vec<(Vec<Option<String>>, u64)>),
    /// simple distinct optimization
    Distinct(HashSet<String>),
    /// the condition matched no rows in this file — the most common outcome
    /// for needle queries, and the cheapest to memoize
    NoMatch,
}

impl CacheEntry {
    /// Materialize the entry as the query's [`VixSearchResult`]. Histogram
    /// entries live on their own absolute grid and reposition into the
    /// requesting histogram grid; `None` means the entry cannot serve this
    /// query (grid mismatch — treat as a cache miss). Every other variant
    /// converts unconditionally.
    fn into_result(self, rule: Option<&IndexOptimizeMode>) -> Option<VixSearchResult> {
        Some(match self {
            CacheEntry::RowIds(row_ids, row_group_size) => VixSearchResult::RowIdsSelection {
                row_ids,
                row_group_size,
            },
            CacheEntry::SelectCandidates(candidates, row_group_size) => {
                VixSearchResult::SelectCandidates {
                    candidates,
                    row_group_size,
                }
            }
            CacheEntry::Count(count) => VixSearchResult::Count(count),
            CacheEntry::Histogram {
                origin,
                width,
                counts,
            } => {
                let Some(IndexOptimizeMode::SimpleHistogram(
                    min_value,
                    bucket_width,
                    num_buckets,
                    ts_offset,
                )) = rule
                else {
                    return None;
                };
                let q_width = (*bucket_width).max(1) as i64;
                let q_origin = min_value - ts_offset;
                // the phase-aligned key guarantees these; verify anyway —
                // serving a shifted grid silently would corrupt counts
                if width != q_width || (origin - q_origin).rem_euclid(q_width) != 0 {
                    return None;
                }
                let shift = (origin - q_origin) / q_width;
                let mut out = vec![0u64; *num_buckets];
                for (i, count) in counts.into_iter().enumerate() {
                    if count == 0 {
                        continue;
                    }
                    let index = i as i64 + shift;
                    if index >= 0 && (index as usize) < *num_buckets {
                        out[index as usize] = count;
                    } else {
                        // the entry has matched rows outside this query's
                        // grid: it answers a different effective range
                        return None;
                    }
                }
                VixSearchResult::Histogram(out)
            }
            CacheEntry::MultiHistogram { width, phase, rows } => {
                let Some(IndexOptimizeMode::SimpleMultiHistogram(
                    min_value,
                    max_value,
                    bucket_width,
                    ts_offset,
                    _,
                )) = rule
                else {
                    return None;
                };
                let q_width = i64::try_from((*bucket_width).max(1)).ok()?;
                let raw_min = min_value.checked_sub(*ts_offset)?;
                let raw_max = max_value.checked_sub(*ts_offset)?;
                let q_phase = raw_min.rem_euclid(q_width);
                if width != q_width || phase != q_phase {
                    return None;
                }

                let mut out = Vec::with_capacity(rows.len());
                for (raw_bucket, value, count) in rows {
                    // Validate every cached row before filtering. A malformed
                    // entry must miss rather than silently dropping a row
                    // that belongs to a different grid.
                    if raw_bucket.rem_euclid(q_width) != q_phase {
                        return None;
                    }
                    if raw_bucket >= raw_min && raw_bucket < raw_max {
                        out.push((raw_bucket.checked_add(*ts_offset)?, value, count));
                    }
                }
                VixSearchResult::MultiHistogram(out)
            }
            CacheEntry::TopN(top_n) => VixSearchResult::TopN(top_n),
            CacheEntry::Distinct(distinct) => VixSearchResult::Distinct(distinct),
            CacheEntry::NoMatch => VixSearchResult::NoMatch,
        })
    }
}

impl CacheEntry {
    pub fn get_memory_size(&self) -> usize {
        match self {
            CacheEntry::RowIds(packed, ..) => packed.memory_size(),
            CacheEntry::SelectCandidates(candidates, ..) => {
                candidates.capacity() * std::mem::size_of::<(i64, u32)>()
                    + std::mem::size_of::<Vec<(i64, u32)>>()
            }
            CacheEntry::Count(_) => std::mem::size_of::<usize>(),
            CacheEntry::Histogram { counts, .. } => {
                counts.capacity() * std::mem::size_of::<u64>()
                    + std::mem::size_of::<Vec<u64>>()
                    + 2 * std::mem::size_of::<i64>()
            }
            CacheEntry::MultiHistogram { rows, .. } => {
                rows.iter()
                    .map(|(_, s, _)| {
                        s.capacity() + std::mem::size_of::<i64>() + std::mem::size_of::<u64>()
                    })
                    .sum::<usize>()
                    + std::mem::size_of::<Vec<(i64, String, u64)>>()
                    + 2 * std::mem::size_of::<i64>()
            }
            CacheEntry::TopN(top_n) => {
                top_n.capacity() * std::mem::size_of::<(Vec<Option<String>>, u64)>()
                    + top_n
                        .iter()
                        .map(|(keys, _)| {
                            keys.capacity() * std::mem::size_of::<Option<String>>()
                                + keys.iter().flatten().map(String::capacity).sum::<usize>()
                        })
                        .sum::<usize>()
                    + std::mem::size_of_val(top_n)
            }
            CacheEntry::Distinct(distinct) => {
                distinct.iter().map(|s| s.capacity()).sum::<usize>()
                    + std::mem::size_of::<HashSet<String>>()
            }
            CacheEntry::NoMatch => std::mem::size_of::<CacheEntry>(),
        }
    }
}

fn entry_footprint(key: &str, entry: &CacheEntry) -> usize {
    // The FIFO accounts for its own key allocation and slot storage.
    entry.get_memory_size()
        + key.len()
        + std::mem::size_of::<String>()
        + std::mem::size_of::<usize>()
}

struct StoredEntry {
    value: CacheEntry,
    slot: usize,
}

const NO_SLOT: usize = usize::MAX;

enum FifoSlot {
    Occupied {
        key: String,
        previous: usize,
        next: usize,
    },
    Vacant {
        next: usize,
    },
}

/// Indexed links allow invalidation to unlink a slot without scanning the FIFO.
/// Vacant slots are reused; their retained capacity remains charged to the budget.
struct InsertionFifo {
    slots: Vec<FifoSlot>,
    first: usize,
    last: usize,
    free: usize,
    len: usize,
    key_bytes: usize,
    entry_bytes: usize,
}

impl Default for InsertionFifo {
    fn default() -> Self {
        Self {
            slots: Vec::new(),
            first: NO_SLOT,
            last: NO_SLOT,
            free: NO_SLOT,
            len: 0,
            key_bytes: 0,
            entry_bytes: 0,
        }
    }
}

impl InsertionFifo {
    fn len(&self) -> usize {
        self.len
    }

    fn capacity(&self) -> usize {
        self.slots.capacity()
    }

    fn memory_size(&self) -> usize {
        self.entry_bytes + self.key_bytes + self.capacity() * std::mem::size_of::<FifoSlot>()
    }

    fn push_back(&mut self, key: String) -> usize {
        self.key_bytes += key.capacity();
        let value = FifoSlot::Occupied {
            key,
            previous: self.last,
            next: NO_SLOT,
        };
        let slot = if self.free == NO_SLOT {
            self.slots.push(value);
            self.slots.len() - 1
        } else {
            let slot = self.free;
            let FifoSlot::Vacant { next } = self.slots[slot] else {
                unreachable!("free list must contain only vacant slots");
            };
            self.free = next;
            self.slots[slot] = value;
            slot
        };
        if self.last == NO_SLOT {
            self.first = slot;
        } else if let FifoSlot::Occupied { next, .. } = &mut self.slots[self.last] {
            *next = slot;
        }
        self.last = slot;
        self.len += 1;
        slot
    }

    fn remove(&mut self, slot: usize) -> String {
        let FifoSlot::Occupied {
            key,
            previous,
            next,
        } = std::mem::replace(&mut self.slots[slot], FifoSlot::Vacant { next: self.free })
        else {
            unreachable!("every map entry must own one occupied FIFO slot");
        };
        if previous == NO_SLOT {
            self.first = next;
        } else if let FifoSlot::Occupied { next: link, .. } = &mut self.slots[previous] {
            *link = next;
        }
        if next == NO_SLOT {
            self.last = previous;
        } else if let FifoSlot::Occupied { previous: link, .. } = &mut self.slots[next] {
            *link = previous;
        }
        self.free = slot;
        self.len -= 1;
        self.key_bytes -= key.capacity();
        key
    }

    // Drop the retired allocation after unlocking, not while holding the FIFO lock.
    fn release_if_empty(&mut self) -> Vec<FifoSlot> {
        if self.len != 0 {
            return Vec::new();
        }
        self.free = NO_SLOT;
        std::mem::take(&mut self.slots)
    }
}

/// Entry-count and byte-bounded FIFO cache. Overwrites keep their insertion order.
/// Every live key owns exactly one slot; invalidation unlinks that slot immediately.
pub struct VixResultCache {
    readers: DashMap<String, StoredEntry>,
    cacher: parking_lot::Mutex<InsertionFifo>,
    max_entries: usize,
    max_bytes: usize,
    bytes: AtomicUsize,
}

impl VixResultCache {
    pub fn new(max_entries: usize) -> Self {
        Self::with_budget(max_entries, usize::MAX)
    }

    pub fn with_budget(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            readers: DashMap::new(),
            cacher: parking_lot::Mutex::new(InsertionFifo::default()),
            max_entries,
            max_bytes,
            bytes: AtomicUsize::new(0),
        }
    }

    /// Look up an entry and materialize it for the query. `rule` is the
    /// query's optimize mode — histogram entries reposition into its grid
    /// (a grid the entry cannot serve reads as a miss).
    pub fn get(&self, key: &str, rule: Option<&IndexOptimizeMode>) -> Option<VixSearchResult> {
        let entry = { self.readers.get(key).map(|r| r.value().value.clone()) };

        entry.and_then(|entry| entry.into_result(rule))
    }

    /// Whether an entry exists under `key` (no materialization, no metric
    /// tick) — the M14 prefetch wave skips files whose result is already
    /// memoized, since their evaluation never opens a reader at all.
    pub fn contains(&self, key: &str) -> bool {
        self.readers.contains_key(key)
    }

    pub fn put(&self, key: String, value: CacheEntry) -> Option<CacheEntry> {
        let new_footprint = entry_footprint(&key, &value);
        let mut fifo = self.cacher.lock();
        let before = fifo.memory_size();
        let old = if let Some(mut entry) = self.readers.get_mut(&key) {
            fifo.entry_bytes -= entry_footprint(&key, &entry.value);
            Some(std::mem::replace(&mut entry.value, value))
        } else {
            // Cloning normalizes the map key capacity to its length. Keep the
            // original key in the FIFO and charge its actual allocation.
            let map_key = key.clone();
            let slot = fifo.push_back(key);
            self.readers.insert(map_key, StoredEntry { value, slot });
            None
        };
        fifo.entry_bytes += new_footprint;
        while fifo.len() > 0
            && (fifo.len() > self.max_entries || fifo.memory_size() > self.max_bytes)
        {
            let slot = fifo.first;
            let key = fifo.remove(slot);
            if let Some((key, entry)) = self.readers.remove(&key) {
                fifo.entry_bytes -= entry_footprint(&key, &entry.value);
                metrics::VIX_RESULT_CACHE_GC_TOTAL
                    .with_label_values::<&str>(&[])
                    .inc();
            }
        }
        let retired = fifo.release_if_empty();
        let after = fifo.memory_size();
        self.bytes.store(after, Ordering::Relaxed);
        drop(fifo);
        drop(retired);
        metrics::VIX_RESULT_CACHE_MEMORY_USAGE
            .with_label_values::<&str>(&[])
            .add(after as i64 - before as i64);
        old
    }

    /// Remove all snapshotted generations of the given logical data file keys.
    /// The snapshot is collected without the FIFO lock. Each removal holds that
    /// lock only long enough to unlink one entry, never to compact the queue.
    pub fn remove_file_entries<'a, I: IntoIterator<Item = &'a str>>(&self, file_keys: I) -> usize {
        let files: HashSet<&str> = file_keys.into_iter().collect();
        if files.is_empty() || self.readers.is_empty() {
            return 0;
        }
        let doomed: Vec<String> = self
            .readers
            .iter()
            .filter_map(|entry| {
                let key = entry.key();
                let file = key.split('|').next().unwrap_or(key);
                files.contains(file).then(|| key.clone())
            })
            .collect();
        let mut removed = 0usize;
        for key in doomed {
            let mut fifo = self.cacher.lock();
            let before = fifo.memory_size();
            let entry = self.readers.remove(&key);
            if let Some((key, entry)) = &entry {
                fifo.remove(entry.slot);
                fifo.entry_bytes -= entry_footprint(key, &entry.value);
                removed += 1;
            }
            let retired = fifo.release_if_empty();
            let after = fifo.memory_size();
            self.bytes.store(after, Ordering::Relaxed);
            drop(fifo);
            drop(retired);
            drop(entry);
            metrics::VIX_RESULT_CACHE_MEMORY_USAGE
                .with_label_values::<&str>(&[])
                .sub((before - after) as i64);
        }
        removed
    }

    pub fn len(&self) -> usize {
        self.readers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.readers.is_empty()
    }

    pub fn memory_size(&self) -> usize {
        self.bytes.load(Ordering::Relaxed)
    }
}

impl Default for VixResultCache {
    fn default() -> Self {
        let cfg = config::get_config();
        Self::with_budget(
            cfg.limit.inverted_index_result_cache_max_entries,
            cfg.limit
                .inverted_index_result_cache_max_size
                .saturating_mul(1024 * 1024),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn create_test_row_ids_result() -> CacheEntry {
        CacheEntry::RowIds(
            Arc::new(RowIdBitmap::from_row_ids(64, [10u32, 20, 30])),
            None,
        )
    }

    fn create_test_count_result() -> CacheEntry {
        CacheEntry::Count(42)
    }

    #[test]
    fn test_fifo_overwrites_do_not_accumulate_slots() {
        let cache = VixResultCache::new(8);
        for i in 0..10_000 {
            cache.put("file|1|100|condition".into(), CacheEntry::Count(i));
        }
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.cacher.lock().len(), 1);
        assert!(matches!(
            cache.get("file|1|100|condition", None),
            Some(VixSearchResult::Count(9_999))
        ));
    }

    #[test]
    fn test_fifo_invalidated_slot_cannot_evict_reinserted_key() {
        let cache = VixResultCache::new(2);
        cache.put("a|1|100|condition".into(), CacheEntry::Count(1));
        assert_eq!(cache.remove_file_entries(["a"]), 1);
        cache.put("b|1|100|condition".into(), CacheEntry::Count(2));
        cache.put("a|1|100|condition".into(), CacheEntry::Count(3));
        cache.put("c|2|100|condition".into(), CacheEntry::Count(4));
        assert!(cache.get("b|1|100|condition", None).is_none());
        assert!(matches!(
            cache.get("a|1|100|condition", None),
            Some(VixSearchResult::Count(3))
        ));
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_fifo_invalidation_releases_all_slots() {
        let cache = VixResultCache::new(32);
        for i in 0..10_000 {
            cache.put(
                format!("file|{}|100|condition", i % 32),
                CacheEntry::Count(i),
            );
        }
        assert_eq!(cache.remove_file_entries(["file"]), 32);
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.cacher.lock().len(), 0);
        assert_eq!(cache.cacher.lock().capacity(), 0);
        assert_eq!(cache.memory_size(), 0);
    }

    #[test]
    fn test_fifo_accounting_includes_queue_storage() {
        let cache = VixResultCache::new(8);
        cache.put("file|1|100|condition".into(), CacheEntry::Count(7));
        let key_bytes = 2 * "file|1|100|condition".len();
        assert!(cache.memory_size() >= key_bytes + std::mem::size_of::<String>() + 8);
    }

    fn assert_fifo_consistent(cache: &VixResultCache) {
        let fifo = cache.cacher.lock();
        let mut live = HashSet::new();
        let mut slot = fifo.first;
        let mut previous = NO_SLOT;
        let mut key_bytes = 0;
        let mut entry_bytes = 0;
        while slot != NO_SLOT {
            assert!(live.insert(slot), "FIFO contains a cycle");
            let FifoSlot::Occupied {
                key,
                previous: back,
                next,
            } = &fifo.slots[slot]
            else {
                panic!("FIFO links to a vacant slot");
            };
            assert_eq!(*back, previous);
            let entry = cache.readers.get(key).expect("FIFO key must be live");
            assert_eq!(entry.slot, slot);
            assert_eq!(entry.key().capacity(), entry.key().len());
            key_bytes += key.capacity();
            entry_bytes += entry_footprint(key, &entry.value);
            previous = slot;
            slot = *next;
        }
        assert_eq!(previous, fifo.last);
        assert_eq!(live.len(), fifo.len());
        assert_eq!(live.len(), cache.len());
        assert_eq!(key_bytes, fifo.key_bytes);
        assert_eq!(entry_bytes, fifo.entry_bytes);
        assert_eq!(
            cache.memory_size(),
            key_bytes + entry_bytes + fifo.capacity() * std::mem::size_of::<FifoSlot>()
        );
        let mut free = HashSet::new();
        slot = fifo.free;
        while slot != NO_SLOT {
            assert!(free.insert(slot), "free list contains a cycle");
            assert!(!live.contains(&slot));
            let FifoSlot::Vacant { next } = fifo.slots[slot] else {
                panic!("free list contains a live slot");
            };
            slot = next;
        }
        assert_eq!(live.len() + free.len(), fifo.slots.len());
        assert!(cache.memory_size() <= cache.max_bytes);
        assert!(live.len() <= cache.max_entries);
    }

    #[test]
    fn test_fifo_matches_reference_after_mixed_operations() {
        use std::collections::VecDeque;

        let cache = VixResultCache::new(7);
        let mut reference: VecDeque<(String, usize)> = VecDeque::new();
        let keys: Vec<String> = (0..24)
            .map(|i| format!("file{}|{}|100|condition", i % 8, i / 8))
            .collect();
        let mut seed = 0x1234_5678_u64;
        for step in 0..4_000 {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            let key = &keys[((seed >> 32) as usize) % keys.len()];
            if seed.is_multiple_of(5) {
                let file = key.split('|').next().unwrap();
                let before = reference.len();
                reference.retain(|(key, _)| key.split('|').next().unwrap() != file);
                assert_eq!(cache.remove_file_entries([file]), before - reference.len());
            } else {
                let expected_old =
                    if let Some((_, value)) = reference.iter_mut().find(|(k, _)| k == key) {
                        Some(std::mem::replace(value, step))
                    } else {
                        reference.push_back((key.clone(), step));
                        if reference.len() > 7 {
                            reference.pop_front();
                        }
                        None
                    };
                let old = cache.put(key.clone(), CacheEntry::Count(step));
                assert_eq!(
                    old.map(|entry| match entry {
                        CacheEntry::Count(v) => v,
                        _ => panic!("wrong old value"),
                    }),
                    expected_old
                );
            }
            for key in &keys {
                let expected = reference.iter().find(|(k, _)| k == key).map(|(_, v)| *v);
                let actual = cache.get(key, None).map(|entry| match entry {
                    VixSearchResult::Count(v) => v,
                    _ => panic!("wrong value"),
                });
                assert_eq!(actual, expected, "step {step}, key {key}");
            }
            assert_fifo_consistent(&cache);
        }
    }

    #[test]
    fn test_fifo_concurrent_overwrite_invalidate_and_get() {
        let cache = Arc::new(VixResultCache::with_budget(32, 32 * 1024));
        std::thread::scope(|scope| {
            for thread in 0..8 {
                let cache = Arc::clone(&cache);
                scope.spawn(move || {
                    for i in 0..4_000 {
                        let file = format!("file{}", i % 17);
                        let key = format!("{file}|{}|100|condition", thread % 2);
                        cache.put(key.clone(), CacheEntry::Count(i));
                        if let Some(result) = cache.get(&key, None) {
                            assert!(matches!(result, VixSearchResult::Count(_)));
                        }
                        if i % 3 == 0 {
                            cache.remove_file_entries([file.as_str()]);
                        }
                    }
                });
            }
        });
        assert_fifo_consistent(&cache);
        let files: Vec<String> = (0..17).map(|i| format!("file{i}")).collect();
        cache.remove_file_entries(files.iter().map(String::as_str));
        assert_fifo_consistent(&cache);
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.memory_size(), 0);
        assert_eq!(cache.cacher.lock().capacity(), 0);
    }

    #[test]
    fn test_fifo_retained_key_capacity_is_charged() {
        let cache = VixResultCache::new(8);
        let mut key = String::with_capacity(16 * 1024);
        key.push_str("file|1|100|condition");
        let retained = key.capacity();
        cache.put(key, CacheEntry::Count(1));
        assert!(cache.memory_size() >= retained);
        assert_fifo_consistent(&cache);
        cache.put("file|1|100|condition".into(), CacheEntry::Count(2));
        assert!(cache.memory_size() >= retained);
        assert_fifo_consistent(&cache);
        cache.remove_file_entries(["file"]);
        assert_eq!(cache.memory_size(), 0);
    }

    #[test]
    fn test_fifo_empty_and_oversized_budgets() {
        for (entries, bytes) in [(0, usize::MAX), (8, 0), (8, 1)] {
            let cache = VixResultCache::with_budget(entries, bytes);
            cache.put("file|1|100|condition".into(), CacheEntry::Count(1));
            assert_eq!(cache.len(), 0);
            assert_eq!(cache.memory_size(), 0);
            assert_eq!(cache.cacher.lock().capacity(), 0);
            assert_fifo_consistent(&cache);
        }
        let cache = VixResultCache::with_budget(8, 1024);
        cache.put("small".into(), CacheEntry::Count(1));
        cache.put(
            "large".into(),
            CacheEntry::Histogram {
                origin: 0,
                width: 1,
                counts: vec![1; 1024],
            },
        );
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.memory_size(), 0);
        cache.put("small".into(), CacheEntry::Count(2));
        assert!(matches!(
            cache.get("small", None),
            Some(VixSearchResult::Count(2))
        ));
        assert_fifo_consistent(&cache);
    }

    #[test]
    fn test_fifo_overwrite_preserves_order_and_generation_identity() {
        let cache = VixResultCache::new(2);
        cache.put("a|1|100|condition".into(), CacheEntry::Count(1));
        cache.put("b|1|100|condition".into(), CacheEntry::Count(2));
        cache.put("a|1|100|condition".into(), CacheEntry::Count(3));
        cache.put("a|2|100|condition".into(), CacheEntry::Count(4));
        assert!(cache.get("a|1|100|condition", None).is_none());
        assert!(matches!(
            cache.get("b|1|100|condition", None),
            Some(VixSearchResult::Count(2))
        ));
        assert!(matches!(
            cache.get("a|2|100|condition", None),
            Some(VixSearchResult::Count(4))
        ));
        assert_fifo_consistent(&cache);
        assert_eq!(cache.remove_file_entries(["a"]), 1);
        assert_fifo_consistent(&cache);
    }

    #[test]
    fn test_vix_result_cache_new() {
        let cache = VixResultCache::new(10);
        assert_eq!(cache.max_entries, 10);
        assert!(cache.is_empty());
    }

    #[test]
    fn test_vix_result_cache_put_and_get() {
        let cache = VixResultCache::new(10);
        let key = "test_key".to_string();

        assert!(cache.get(&key, None).is_none());
        cache.put(key.clone(), create_test_row_ids_result());
        let retrieved = cache.get(&key, None);
        assert!(retrieved.is_some());
        match retrieved.unwrap() {
            VixSearchResult::RowIdsSelection { row_ids, .. } => {
                assert_eq!(row_ids.iter().collect::<Vec<_>>(), vec![10, 20, 30]);
            }
            _ => panic!("Expected RowIdsSelection result"),
        }
    }

    #[test]
    fn test_vix_result_cache_count_roundtrip() {
        let cache = VixResultCache::new(10);
        cache.put("count_key".to_string(), create_test_count_result());

        if let Some(VixSearchResult::Count(count)) = cache.get("count_key", None) {
            assert_eq!(count, 42);
        } else {
            panic!("Expected Count result");
        }
    }

    #[test]
    fn test_vix_result_cache_eviction() {
        let cache = VixResultCache::new(5);

        for i in 0..10 {
            let key = format!("key_{i}");
            cache.put(key, create_test_count_result());
        }

        assert!(cache.get("key_0", None).is_none());
        assert!(cache.get("key_9", None).is_some());
    }

    #[test]
    fn test_vix_result_cache_overwrite_existing() {
        let cache = VixResultCache::new(10);
        let key = "test_key".to_string();

        cache.put(key.clone(), create_test_count_result());
        let old_entry = cache.put(key.clone(), create_test_row_ids_result());

        assert!(old_entry.is_some());
        if let Some(CacheEntry::Count(count)) = old_entry {
            assert_eq!(count, 42);
        } else {
            panic!("Expected Count result");
        }

        assert!(matches!(
            cache.get(&key, None),
            Some(VixSearchResult::RowIdsSelection { .. })
        ));
    }

    #[test]
    fn test_vix_result_cache_row_ids_roundtrip_with_row_group_size() {
        let cache = VixResultCache::new(10);

        let entry = CacheEntry::RowIds(
            Arc::new(RowIdBitmap::from_row_ids(64, [10u32, 20])),
            Some(1024),
        );
        cache.put("row_ids_key".to_string(), entry);

        if let Some(VixSearchResult::RowIdsSelection {
            row_ids,
            row_group_size,
        }) = cache.get("row_ids_key", None)
        {
            assert_eq!(row_ids.iter().collect::<Vec<_>>(), vec![10, 20]);
            assert_eq!(row_group_size, Some(1024));
        } else {
            panic!("Expected RowIdsSelection result");
        }
    }

    #[tokio::test]
    async fn test_vix_result_cache_concurrent_access() {
        let cache = Arc::new(VixResultCache::new(50));
        let mut handles = vec![];

        for i in 0..10 {
            let cache_clone = cache.clone();
            let handle = tokio::spawn(async move {
                let key = format!("concurrent_key_{i}");
                cache_clone.put(key.clone(), CacheEntry::Count(i));
                match cache_clone.get(&key, None) {
                    Some(VixSearchResult::Count(count)) => assert_eq!(count, i),
                    _ => panic!("Expected Count result"),
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }
    }

    #[test]
    fn test_global_cache_accessibility() {
        let global_cache = &*GLOBAL_CACHE;

        let key = "global_test_key".to_string();
        global_cache.put(key.clone(), create_test_count_result());

        if let Some(VixSearchResult::Count(count)) = global_cache.get(&key, None) {
            assert_eq!(count, 42);
        } else {
            panic!("Expected Count result");
        }
    }

    #[test]
    fn test_vix_result_cache_memory_size() {
        let cache = VixResultCache::new(10);
        assert_eq!(cache.memory_size(), 0);
        cache.put("k".to_string(), create_test_count_result());
        assert!(cache.memory_size() > 0);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_vix_result_cache_byte_budget_evicts_oldest() {
        // each Histogram entry: 100 * 8 bytes + Vec header + 2*key bytes ≈ 850
        let entry = || CacheEntry::Histogram {
            origin: 0,
            width: 60,
            counts: vec![0u64; 100],
        };
        let budget = 3 * entry_footprint("key_0", &entry());
        let cache = VixResultCache::with_budget(1000, budget);

        for i in 0..10 {
            cache.put(format!("key_{i}"), entry());
        }
        // stays within the byte budget: only the newest ~3 entries survive
        assert!(cache.memory_size() <= budget);
        assert!(cache.len() <= 3);
        // histogram entries materialize only against a matching grid
        let rule = IndexOptimizeMode::SimpleHistogram(0, 60, 100, 0);
        assert!(cache.get("key_0", Some(&rule)).is_none());
        assert!(cache.get("key_9", Some(&rule)).is_some());
    }

    #[test]
    fn test_vix_result_cache_overwrite_accounting_stays_exact() {
        let cache = VixResultCache::with_budget(1000, usize::MAX);
        let big = || CacheEntry::Histogram {
            origin: 0,
            width: 60,
            counts: vec![0u64; 1000],
        };
        let small = create_test_count_result;

        cache.put("k".to_string(), big());
        let after_big = cache.memory_size();
        cache.put("k".to_string(), small());
        let after_small = cache.memory_size();
        // replacing a big entry with a small one RELEASES the difference —
        // the same FIFO slot remains charged, but the old value is released
        assert!(after_small < after_big);
        assert_eq!(
            after_small,
            entry_footprint("k", &small())
                + "k".len()
                + cache.cacher.lock().capacity() * std::mem::size_of::<FifoSlot>(),
            "live bytes must include the entry, FIFO key and retained slot storage"
        );
        assert_eq!(cache.len(), 1);

        // Repeated eviction and slot reuse must not double-free the value.
        for i in 0..2000 {
            cache.put(format!("fill_{i}"), small());
        }
        assert!(cache.len() <= 1000);
    }

    /// `remove_file_entries` drops every generation whose cache key belongs
    /// to a purged logical data file (prefix up to `'|'`), exactly accounts
    /// the freed bytes, and leaves other files' entries untouched. Removed
    /// slots can be reused without affecting other generations.
    #[test]
    fn test_remove_file_entries_purges_by_file_key() {
        let cache = VixResultCache::new(100);
        let healed = "files/org/logs/s1/2026/08/18/00/healed.vix";
        let other = "files/org/logs/s1/2026/08/18/00/other.vix";
        // two conditions across equal-sized immutable generations, plus one
        // differently sized generation and one entry for another file
        cache.put(format!("{healed}|41|100|aaaa_n_full"), CacheEntry::Count(1));
        cache.put(format!("{healed}|42|100|bbbb_n_full"), CacheEntry::NoMatch);
        cache.put(format!("{healed}|43|164|aaaa_n_full"), CacheEntry::Count(2));
        let other_key = format!("{other}|42|100|aaaa_n_full");
        let other_key_capacity = other_key.capacity();
        cache.put(other_key, CacheEntry::Count(3));

        // unknown file: no-op
        assert_eq!(cache.remove_file_entries(["files/org/none.vix"]), 0);
        assert_eq!(cache.len(), 4);

        let removed = cache.remove_file_entries([healed]);
        assert_eq!(removed, 3, "every entry of the healed file must go");
        assert_eq!(cache.len(), 1);
        assert!(
            cache
                .get(&format!("{healed}|41|100|aaaa_n_full"), None)
                .is_none()
        );
        assert!(
            cache
                .get(&format!("{healed}|43|164|aaaa_n_full"), None)
                .is_none()
        );
        assert!(
            matches!(
                cache.get(&format!("{other}|42|100|aaaa_n_full"), None),
                Some(VixSearchResult::Count(3))
            ),
            "other files' entries stay"
        );
        assert_eq!(
            cache.memory_size(),
            entry_footprint(
                &format!("{other}|42|100|aaaa_n_full"),
                &CacheEntry::Count(3)
            ) + other_key_capacity
                + cache.cacher.lock().capacity() * std::mem::size_of::<FifoSlot>(),
            "freed entries and keys are released, but retained slots remain charged"
        );

        // Vacant slots are reused when new entries arrive under pressure.
        for i in 0..200 {
            cache.put(format!("fill_{i}|0|0|k_n_full"), CacheEntry::Count(i));
        }
        assert!(cache.len() <= 100);
    }

    #[test]
    fn test_vix_result_cache_no_match_roundtrip() {
        let cache = VixResultCache::new(10);
        cache.put("nm".to_string(), CacheEntry::NoMatch);
        assert!(matches!(
            cache.get("nm", None),
            Some(VixSearchResult::NoMatch)
        ));
        assert!(cache.memory_size() > 0);
    }

    #[test]
    fn test_vix_result_cache_fast_path_roundtrips() {
        let cache = VixResultCache::new(10);

        cache.put(
            "select".to_string(),
            CacheEntry::SelectCandidates(Arc::new(vec![(100, 7)]), Some(64)),
        );
        match cache.get("select", None) {
            Some(VixSearchResult::SelectCandidates {
                candidates,
                row_group_size,
            }) => {
                assert_eq!(*candidates, vec![(100, 7)]);
                assert_eq!(row_group_size, Some(64));
            }
            other => panic!("Expected SelectCandidates, got {other:?}"),
        }

        cache.put(
            "hist".to_string(),
            CacheEntry::Histogram {
                origin: 0,
                width: 60,
                counts: vec![1, 0, 2],
            },
        );
        let hist_rule = IndexOptimizeMode::SimpleHistogram(0, 60, 3, 0);
        assert!(matches!(
            cache.get("hist", Some(&hist_rule)),
            Some(VixSearchResult::Histogram(h)) if h == vec![1, 0, 2]
        ));

        cache.put(
            "mhist".to_string(),
            CacheEntry::MultiHistogram {
                width: 10,
                phase: 1,
                rows: vec![(1, "a".to_string(), 2)],
            },
        );
        let multi_rule = IndexOptimizeMode::SimpleMultiHistogram(1, 11, 10, 0, "level".to_string());
        assert!(matches!(
            cache.get("mhist", Some(&multi_rule)),
            Some(VixSearchResult::MultiHistogram(rows)) if rows == vec![(1, "a".to_string(), 2)]
        ));

        cache.put(
            "topn".to_string(),
            CacheEntry::TopN(vec![(vec![Some("a".to_string())], 3)]),
        );
        assert!(matches!(
            cache.get("topn", None),
            Some(VixSearchResult::TopN(rows)) if rows == vec![(vec![Some("a".to_string())], 3)]
        ));

        cache.put(
            "distinct".to_string(),
            CacheEntry::Distinct(std::collections::HashSet::from(["v".to_string()])),
        );
        assert!(matches!(
            cache.get("distinct", None),
            Some(VixSearchResult::Distinct(values)) if values.contains("v")
        ));

        // every fast-path entry reports a non-zero footprint
        assert!(cache.memory_size() > 0);
    }

    #[test]
    fn test_multi_histogram_materialization_filters_and_rejects_bad_grid() {
        let cache = VixResultCache::new(10);
        cache.put(
            "multi".to_string(),
            CacheEntry::MultiHistogram {
                width: 10,
                phase: 0,
                rows: vec![
                    (0, "outside-low".to_string(), 1),
                    (10, "a".to_string(), 2),
                    (20, "b".to_string(), 3),
                    (30, "outside-high".to_string(), 4),
                ],
            },
        );

        // A shifted zero-offset request keeps the same grid but narrows the
        // absolute range. Only rows inside [10, 30) are returned.
        let shifted = IndexOptimizeMode::SimpleMultiHistogram(10, 30, 10, 0, "level".to_string());
        assert!(matches!(
            cache.get("multi", Some(&shifted)),
            Some(VixSearchResult::MultiHistogram(rows))
                if rows == vec![(10, "a".to_string(), 2), (20, "b".to_string(), 3)]
        ));

        let wrong_phase =
            IndexOptimizeMode::SimpleMultiHistogram(11, 31, 10, 0, "level".to_string());
        let wrong_width =
            IndexOptimizeMode::SimpleMultiHistogram(10, 30, 20, 0, "level".to_string());
        assert!(cache.get("multi", Some(&wrong_phase)).is_none());
        assert!(cache.get("multi", Some(&wrong_width)).is_none());
        assert!(cache.get("multi", None).is_none());

        cache.put(
            "malformed".to_string(),
            CacheEntry::MultiHistogram {
                width: 10,
                phase: 0,
                rows: vec![(15, "misaligned".to_string(), 1)],
            },
        );
        assert!(
            cache.get("malformed", Some(&shifted)).is_none(),
            "a malformed row must make the whole entry miss"
        );

        let overflowing =
            IndexOptimizeMode::SimpleMultiHistogram(i64::MIN, i64::MAX, 10, 1, "level".to_string());
        assert!(cache.get("multi", Some(&overflowing)).is_none());
    }
}
