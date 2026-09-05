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

//! Per-file cache of parsed ranged [`VixReader`]s.
//!
//! A ranged open costs a footer tail fetch plus the dictionary-directory
//! fetch; the parsed reader then GROWS as queries lazily load row-group FST
//! cells (they stay resident on the reader), so hot queries skip even the
//! tail fetch and re-use every loaded cell. Entry sizes are therefore
//! re-synced from [`VixReader::memory_size`] on every get of the entry and
//! across all entries on every put, keeping the LRU budget honest as
//! readers grow. Sized by `ZO_VIX_READER_CACHE_MAX_SIZE` (default 10% of
//! RAM, no upper clamp; falls back to the inverted-index footer-cache knob
//! `ZO_INVERTED_INDEX_FOOTER_CACHE_MAX_SIZE` when only that one is set).
//! Eviction is LRU (a get refreshes the entry). Reader identity includes the
//! logical data key plus the immutable sidecar generation and its exact size:
//! generation prevents equal-sized heals from sharing parsed state, while
//! size remains a compatibility witness. Broadcast invalidation can still
//! purge every generation belonging to one logical data file.
//!
//! Prometheus: `vix_reader_cache_entries`, `vix_reader_cache_memory_bytes`,
//! `vix_reader_cache_{hits,misses}_total`.

use std::sync::{Arc, LazyLock as Lazy};

use config::metrics;
use hashlink::LruCache;
use vortex_index::VixReader;

pub static GLOBAL_CACHE: Lazy<VixReaderCache> =
    Lazy::new(|| VixReaderCache::new(config::get_config().limit.vix_reader_cache_max_size));

/// Immutable sidecar identity for one logical data file.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ReaderCacheKey {
    file: String,
    index_generation: i64,
    index_size: i64,
}

impl ReaderCacheKey {
    pub fn new(file: String, index_generation: i64, index_size: i64) -> Self {
        Self {
            file,
            index_generation,
            index_size,
        }
    }

    pub fn file(&self) -> &str {
        &self.file
    }

    fn memory_size(&self) -> usize {
        2 * self.file.capacity() + std::mem::size_of::<i64>() * 2
    }
}

/// LRU entries keyed by immutable sidecar identity: `(reader, entry_size)`.
type ReaderLru = LruCache<ReaderCacheKey, (Arc<VixReader>, usize)>;

/// A size-bounded LRU of parsed readers keyed by immutable sidecar identity.
pub struct VixReaderCache {
    /// LRU order + running total of entry sizes, under one lock.
    state: parking_lot::Mutex<(ReaderLru, usize)>,
    max_bytes: usize,
}

impl VixReaderCache {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            state: parking_lot::Mutex::new((LruCache::new_unbounded(), 0)),
            max_bytes,
        }
    }

    /// Get a parsed reader, refreshing its LRU position and re-syncing the
    /// entry's accounted size (the reader may have grown since the last
    /// touch — lazily loaded FST cells stay resident on it).
    pub fn get(&self, key: &ReaderCacheKey) -> Option<Arc<VixReader>> {
        let mut state = self.state.lock();
        let (lru, total) = &mut *state;
        // LruCache::get_mut touches the entry (moves it to the back = most
        // recently used)
        let found = match lru.get_mut(key) {
            Some((reader, accounted)) => {
                let reader = Arc::clone(reader);
                let current = reader.memory_size() + key.memory_size();
                let previous = std::mem::replace(accounted, current);
                *total = total.saturating_sub(previous) + current;
                Some(reader)
            }
            None => None,
        };
        match &found {
            Some(_) => metrics::VIX_READER_CACHE_HITS_TOTAL
                .with_label_values::<&str>(&[])
                .inc(),
            None => metrics::VIX_READER_CACHE_MISSES_TOTAL
                .with_label_values::<&str>(&[])
                .inc(),
        }
        found
    }

    /// Whether a reader is memoized for this exact immutable sidecar — the
    /// M14 prefetch cold check. No hit/miss metric tick and no LRU refresh:
    /// probing must not distort the cache-effectiveness counters or eviction
    /// order.
    pub fn contains(&self, key: &ReaderCacheKey) -> bool {
        self.state.lock().0.contains_key(key)
    }

    /// Insert a parsed reader (first writer wins on races), evicting the
    /// least-recently-used entries when over budget. Readers larger than the
    /// whole budget are not cached. Every insert first re-syncs all entry
    /// sizes from the live readers (lazily loaded FSTs grow them between
    /// touches), so eviction decisions track real memory.
    pub fn put(&self, key: ReaderCacheKey, reader: Arc<VixReader>) {
        let size = reader.memory_size() + key.memory_size();
        if self.max_bytes == 0 || size > self.max_bytes {
            return;
        }
        let mut state = self.state.lock();
        if state.0.contains_key(&key) {
            return;
        }
        // re-sync grown entries (an O(entries) walk of atomic loads; puts
        // happen once per cold file open)
        let mut total = 0usize;
        for (key, (entry, accounted)) in state.0.iter_mut() {
            *accounted = entry.memory_size() + key.memory_size();
            total += *accounted;
        }
        state.1 = total;
        while state.1 + size > self.max_bytes {
            let Some((_key, (_evicted, evicted_size))) = state.0.remove_lru() else {
                break;
            };
            state.1 = state.1.saturating_sub(evicted_size);
        }
        state.1 += size;
        state.0.insert(key, (reader, size));
        metrics::VIX_READER_CACHE_ENTRIES
            .with_label_values::<&str>(&[])
            .set(state.0.len() as i64);
        metrics::VIX_READER_CACHE_MEMORY_BYTES
            .with_label_values::<&str>(&[])
            .set(state.1 as i64);
    }

    /// Drop every memoized generation for one logical data file. Existing
    /// users retain their `Arc`; broadcasts use this to release obsolete
    /// parsed state without needing to know which generations a node saw.
    /// Cheap no-op when absent.
    pub fn remove(&self, file: &str) {
        let mut state = self.state.lock();
        let doomed = state
            .0
            .iter()
            .filter_map(|(key, _)| (key.file() == file).then(|| key.clone()))
            .collect::<Vec<_>>();
        let mut removed = false;
        for key in doomed {
            if let Some((_reader, size)) = state.0.remove(&key) {
                state.1 = state.1.saturating_sub(size);
                removed = true;
            }
        }
        if removed {
            metrics::VIX_READER_CACHE_ENTRIES
                .with_label_values::<&str>(&[])
                .set(state.0.len() as i64);
            metrics::VIX_READER_CACHE_MEMORY_BYTES
                .with_label_values::<&str>(&[])
                .set(state.1 as i64);
        }
    }

    pub fn len(&self) -> usize {
        self.state.lock().0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.state.lock().0.is_empty()
    }

    pub fn memory_size(&self) -> usize {
        self.state.lock().1
    }
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{Int64Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use vortex_index::{VixWriter, VixWriterOptions};

    use super::*;

    fn small_reader() -> Arc<VixReader> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("level", DataType::Utf8, true),
        ]));
        let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();
        let sources = StringArray::from(vec![r#"{"level":"a"}"#, r#"{"level":"b"}"#]);
        writer
            .push_batch_with_source(&batch, &sources, None)
            .unwrap();
        let (data, index) = writer.finish().unwrap();
        Arc::new(
            VixReader::open_with_index(bytes::Bytes::from(data), index.map(bytes::Bytes::from))
                .unwrap(),
        )
    }

    #[test]
    fn put_get_and_size_bounded_eviction() {
        let reader = small_reader();
        let key = |file: &str| ReaderCacheKey::new(file.to_string(), 7, 100);
        let entry_size = reader.memory_size() + key("file-0").memory_size();

        // room for roughly two entries
        let cache = VixReaderCache::new(entry_size * 2 + entry_size / 2);
        for i in 0..3 {
            cache.put(key(&format!("file-{i}")), Arc::clone(&reader));
        }
        // the oldest entry was evicted to fit the third
        assert!(cache.get(&key("file-0")).is_none());
        assert!(cache.get(&key("file-1")).is_some());
        assert!(cache.get(&key("file-2")).is_some());
        assert_eq!(cache.len(), 2);
        assert!(cache.memory_size() <= entry_size * 2 + entry_size / 2);

        // duplicate puts do not double-count
        cache.put(key("file-2"), Arc::clone(&reader));
        assert_eq!(cache.len(), 2);

        // oversized entries are refused outright
        let tiny = VixReaderCache::new(8);
        tiny.put(key("big"), reader);
        assert!(tiny.is_empty());
    }

    #[test]
    fn get_resyncs_grown_entry_sizes() {
        // lazily loaded FST cells grow a cached reader AFTER insertion; the
        // next get must fold the growth into the cache's accounted total
        let reader = small_reader();
        let cache = VixReaderCache::new(usize::MAX);
        let key = ReaderCacheKey::new("file-a".to_string(), 7, 100);
        cache.put(key.clone(), Arc::clone(&reader));
        let before = cache.memory_size();

        // touch an FST through the public eval path: the cell loads and
        // stays resident, growing memory_size
        let query = vortex_index::VixQuery::Exact {
            field: "level".to_string(),
            token: b"a".to_vec(),
        };
        assert_eq!(reader.eval(&query).unwrap().count_set_bits(), 1);
        assert!(reader.memory_size() + key.memory_size() > before);

        assert!(cache.get(&key).is_some());
        assert_eq!(
            cache.memory_size(),
            reader.memory_size() + key.memory_size(),
            "get must re-sync the entry to the grown reader size"
        );
        assert!(cache.memory_size() > before);
    }

    #[test]
    fn generations_are_distinct_and_remove_purges_the_logical_file() {
        let reader = small_reader();
        let cache = VixReaderCache::new(usize::MAX);
        let old = ReaderCacheKey::new("healed.vix".to_string(), 41, 100);
        let new_same_size = ReaderCacheKey::new("healed.vix".to_string(), 42, 100);
        let same_generation_different_size = ReaderCacheKey::new("healed.vix".to_string(), 41, 101);
        let other = ReaderCacheKey::new("other.vix".to_string(), 42, 100);
        cache.put(old.clone(), Arc::clone(&reader));
        assert!(cache.get(&new_same_size).is_none());
        assert!(
            cache.get(&same_generation_different_size).is_none(),
            "size remains a compatibility witness"
        );
        cache.put(new_same_size.clone(), Arc::clone(&reader));
        cache.put(other.clone(), Arc::clone(&reader));
        assert!(cache.get(&old).is_some());
        assert!(cache.get(&new_same_size).is_some());

        cache.remove("healed.vix");
        assert!(cache.get(&old).is_none());
        assert!(cache.get(&new_same_size).is_none());
        assert!(cache.get(&other).is_some(), "other logical files stay");
        assert_eq!(cache.len(), 1);

        // removing an absent key is a cheap no-op
        let before = cache.memory_size();
        cache.remove("never-cached.vix");
        assert_eq!(cache.memory_size(), before);
    }

    #[test]
    fn get_refreshes_lru_order() {
        let reader = small_reader();
        let key = |file: &str| ReaderCacheKey::new(file.to_string(), 7, 100);
        let entry_size = reader.memory_size() + key("file-0").memory_size();
        let cache = VixReaderCache::new(entry_size * 2 + entry_size / 2);

        cache.put(key("file-0"), Arc::clone(&reader));
        cache.put(key("file-1"), Arc::clone(&reader));
        // touch file-0: it becomes the most recently used
        assert!(cache.get(&key("file-0")).is_some());
        // inserting a third entry now evicts file-1, NOT file-0
        cache.put(key("file-2"), Arc::clone(&reader));
        assert!(
            cache.get(&key("file-0")).is_some(),
            "touched entry must survive"
        );
        assert!(
            cache.get(&key("file-1")).is_none(),
            "LRU entry must be evicted"
        );
        assert!(cache.get(&key("file-2")).is_some());
    }
}
