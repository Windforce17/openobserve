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

//! Per-file result cache for vix index searches, keyed by
//! `condition hash + optimize-rule + file key` (`vix_result_cache_*`
//! metrics).

use std::{
    collections::{HashSet, VecDeque},
    sync::{
        Arc, LazyLock as Lazy,
        atomic::{AtomicUsize, Ordering},
    },
};

use arrow::buffer::BooleanBuffer;
use config::{meta::inverted_index::IndexOptimizeMode, metrics};
use dashmap::DashMap;

use super::VixSearchResult;

pub static GLOBAL_CACHE: Lazy<Arc<VixResultCache>> =
    Lazy::new(|| Arc::new(VixResultCache::default()));

#[derive(Debug, Clone)]
pub enum CacheEntry {
    /// (matched_row_ids bitmap, row_group_size_from_index_file)
    RowIds(Arc<BooleanBuffer>, Option<u32>),
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
    /// multi histogram optimization
    MultiHistogram(Vec<(i64, String, u64)>),
    /// group-by top-n optimization
    TopN(Vec<(Vec<String>, u64)>),
    /// simple distinct optimization
    Distinct(HashSet<String>),
    /// the condition matched no rows in this file — the most common outcome
    /// for needle queries, and the cheapest to memoize
    NoMatch,
}

impl CacheEntry {
    /// Materialize the entry as the query's [`VixSearchResult`]. Histogram
    /// entries live on their own absolute grid and reposition into the
    /// query's `SimpleHistogram` grid; `None` means the entry cannot serve
    /// this query (grid mismatch — treat as a cache miss). Every other
    /// variant converts unconditionally.
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
            CacheEntry::MultiHistogram(multi_histogram) => {
                VixSearchResult::MultiHistogram(multi_histogram)
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
            CacheEntry::RowIds(packed, ..) => {
                packed.inner().len() + std::mem::size_of::<BooleanBuffer>()
            }
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
            CacheEntry::MultiHistogram(multi_histogram) => {
                multi_histogram
                    .iter()
                    .map(|(_, s, _)| {
                        s.capacity() + std::mem::size_of::<i64>() + std::mem::size_of::<u64>()
                    })
                    .sum::<usize>()
                    + std::mem::size_of::<Vec<(i64, String, u64)>>()
            }
            CacheEntry::TopN(top_n) => {
                top_n
                    .iter()
                    .map(|(keys, _)| {
                        keys.iter().map(|s| s.capacity()).sum::<usize>()
                            + std::mem::size_of::<Vec<String>>()
                            + std::mem::size_of::<u64>()
                    })
                    .sum::<usize>()
                    + std::mem::size_of::<Vec<(Vec<String>, u64)>>()
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
    // the key is stored twice: DashMap key + FIFO deque slot
    entry.get_memory_size() + 2 * key.len()
}

/// Cache created for storing the vix search result.
///
/// Bounded two ways: entry count AND a total byte budget (`max_bytes`).
/// Eviction is oldest-first over the insertion FIFO; an overwrite of a live
/// key leaves its old deque slot behind as a stale entry that pops harmlessly
/// (the map remove returns `None`), so accounting stays exact.
pub struct VixResultCache {
    readers: DashMap<String, CacheEntry>,
    cacher: parking_lot::Mutex<VecDeque<String>>,
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
            cacher: parking_lot::Mutex::new(VecDeque::new()),
            max_entries,
            max_bytes,
            bytes: AtomicUsize::new(0),
        }
    }

    /// Look up an entry and materialize it for the query. `rule` is the
    /// query's optimize mode — histogram entries reposition into its grid
    /// (a grid the entry cannot serve reads as a miss).
    pub fn get(&self, key: &str, rule: Option<&IndexOptimizeMode>) -> Option<VixSearchResult> {
        let entry = { self.readers.get(key).map(|r| r.value().clone()) };

        entry.and_then(|entry| entry.into_result(rule))
    }

    pub fn put(&self, key: String, value: CacheEntry) -> Option<CacheEntry> {
        let new_footprint = entry_footprint(&key, &value);
        let mut w = self.cacher.lock();
        w.push_back(key.clone());
        let old = self.readers.insert(key.clone(), value);
        let mut delta = new_footprint as i64;
        if let Some(old_entry) = &old {
            // the replaced entry's old deque slot goes stale; give its bytes
            // back now so the budget reflects live entries only
            delta -= entry_footprint(&key, old_entry) as i64;
        }
        let mut bytes = self
            .bytes
            .fetch_add(new_footprint, Ordering::Relaxed)
            .saturating_add(new_footprint);
        if let Some(old_entry) = &old {
            let old_fp = entry_footprint(&key, old_entry);
            bytes = self
                .bytes
                .fetch_sub(old_fp, Ordering::Relaxed)
                .saturating_sub(old_fp);
        }
        // evict oldest-first until back under both bounds; stale slots from
        // overwrites pop as no-ops. `readers.len()` (live entries), not the
        // deque length, bounds the count so stale slots don't force evictions.
        let mut evicted = 0i64;
        while (self.readers.len() > self.max_entries || bytes > self.max_bytes) && !w.is_empty() {
            let Some(k) = w.pop_front() else { break };
            if let Some((k, entry)) = self.readers.remove(&k) {
                let fp = entry_footprint(&k, &entry);
                bytes = self
                    .bytes
                    .fetch_sub(fp, Ordering::Relaxed)
                    .saturating_sub(fp);
                evicted += fp as i64;
                metrics::VIX_RESULT_CACHE_GC_TOTAL
                    .with_label_values::<&str>(&[])
                    .inc();
            }
        }
        drop(w);
        metrics::VIX_RESULT_CACHE_MEMORY_USAGE
            .with_label_values::<&str>(&[])
            .add(delta - evicted);
        old
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
            Arc::new(BooleanBuffer::from_iter(
                (0..64u32).map(|i| [10u32, 20, 30].contains(&i)),
            )),
            None,
        )
    }

    fn create_test_count_result() -> CacheEntry {
        CacheEntry::Count(42)
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
                assert_eq!(row_ids.set_indices().collect::<Vec<_>>(), vec![10, 20, 30]);
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
            Arc::new(BooleanBuffer::from_iter(
                (0..64u32).map(|i| [10u32, 20].contains(&i)),
            )),
            Some(1024),
        );
        cache.put("row_ids_key".to_string(), entry);

        if let Some(VixSearchResult::RowIdsSelection {
            row_ids,
            row_group_size,
        }) = cache.get("row_ids_key", None)
        {
            assert_eq!(row_ids.set_indices().collect::<Vec<_>>(), vec![10, 20]);
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
        // the stale deque slot must not keep the old bytes accounted
        assert!(after_small < after_big);
        assert_eq!(
            after_small,
            entry_footprint("k", &small()),
            "live bytes must equal the single live entry's footprint"
        );
        assert_eq!(cache.len(), 1);

        // the stale slot pops harmlessly and never double-frees
        for i in 0..2000 {
            cache.put(format!("fill_{i}"), small());
        }
        assert!(cache.len() <= 1000);
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
            CacheEntry::MultiHistogram(vec![(1, "a".to_string(), 2)]),
        );
        assert!(matches!(
            cache.get("mhist", None),
            Some(VixSearchResult::MultiHistogram(rows)) if rows == vec![(1, "a".to_string(), 2)]
        ));

        cache.put(
            "topn".to_string(),
            CacheEntry::TopN(vec![(vec!["a".to_string()], 3)]),
        );
        assert!(matches!(
            cache.get("topn", None),
            Some(VixSearchResult::TopN(rows)) if rows == vec![(vec!["a".to_string()], 3)]
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
}
