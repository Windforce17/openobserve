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

use std::{
    collections::VecDeque,
    sync::{
        Arc, LazyLock as Lazy,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use config::metrics;
use dashmap::DashMap;
use datafusion::{
    common::{
        HashMap, Statistics, TableReference,
        heap_size::{DFHeapSize, DFHeapSizeCtx},
    },
    execution::cache::{
        Cache, CacheEntryInfo, SchemaFingerprint, TableScopedPath,
        cache_manager::CachedFileMetadata,
    },
};
use object_store::{ObjectMeta, path::Path};

use super::TRACE_ID_SEPARATOR;

pub static GLOBAL_CACHE: Lazy<Arc<FileStatisticsCache>> =
    Lazy::new(|| Arc::new(FileStatisticsCache::default()));

/// Collected statistics for files
/// Cache is invalided when file size or last modification has changed
pub struct FileStatisticsCache {
    statistics: DashMap<String, (ObjectMeta, Arc<Statistics>, Arc<SchemaFingerprint>, usize)>,
    // Serializes every mutation of the map, queue, and memory accounting.
    // Readers only acquire a DashMap shard; writers acquire this lock first.
    cacher: parking_lot::Mutex<VecDeque<String>>,
    current_memory: AtomicUsize,
}

impl FileStatisticsCache {
    pub fn new() -> Self {
        Self {
            statistics: DashMap::new(),
            cacher: parking_lot::Mutex::new(VecDeque::new()),
            current_memory: AtomicUsize::new(0),
        }
    }

    pub fn len(&self) -> usize {
        self.statistics.len()
    }

    pub fn is_empty(&self) -> bool {
        self.statistics.is_empty()
    }

    pub fn queue_len(&self) -> usize {
        self.cacher.lock().len()
    }

    pub fn memory_size(&self) -> usize {
        self.current_memory.load(Ordering::Relaxed)
    }

    fn estimate_entry_size(key: &str, meta: &ObjectMeta, stats: &Statistics) -> usize {
        // Key is stored both in the DashMap and the eviction queue
        let mut size = (std::mem::size_of::<String>() + key.len()) * 2;

        size += std::mem::size_of::<ObjectMeta>();
        size += meta.location.as_ref().len();
        if let Some(ref etag) = meta.e_tag {
            size += std::mem::size_of::<String>() + etag.len();
        }
        if let Some(ref version) = meta.version {
            size += std::mem::size_of::<String>() + version.len();
        }

        size += std::mem::size_of::<Statistics>();
        size += std::mem::size_of::<usize>(); // tracked entry size field
        // Arc<Statistics> header (strong + weak counter) and DashMap bucket
        // overhead (hash + slot metadata). Conservative fixed overhead per
        // entry so the memory budget is not systematically underestimated.
        size += 64;

        for col in &stats.column_statistics {
            size += std::mem::size_of::<datafusion::common::ColumnStatistics>();
            size += col.min_value.get_value().map(|v| v.size()).unwrap_or(0);
            size += col.max_value.get_value().map(|v| v.size()).unwrap_or(0);
            size += col.sum_value.get_value().map(|v| v.size()).unwrap_or(0);
        }

        size
    }

    fn evict(&self, max_bytes: usize) {
        let start = Instant::now();
        let mut w = self.cacher.lock();
        let initial_memory = self.memory_size();
        while self.memory_size() > max_bytes && !w.is_empty() {
            let batch = (w.len() / 20).max(1).min(w.len());
            for k in w.drain(0..batch) {
                if let Some((_, (_, _, _, size))) = self.statistics.remove(&k) {
                    self.current_memory.store(
                        self.memory_size()
                            .checked_sub(size)
                            .expect("file statistics memory accounting underflow"),
                        Ordering::Relaxed,
                    );
                }
            }
        }
        drop(w);
        if initial_memory > max_bytes {
            log::warn!(
                "FileStatisticsCache is full ({initial_memory} bytes > {max_bytes} bytes), evicting oldest entries",
            );
        }
        metrics::QUERY_PARQUET_METADATA_CACHE_GC_COUNT
            .with_label_values::<&str>(&[])
            .inc();
        metrics::QUERY_PARQUET_METADATA_CACHE_GC_TIME
            .with_label_values::<&str>(&[])
            .observe(start.elapsed().as_millis() as f64);
    }

    fn format_key(&self, k: &Path) -> String {
        if let Some(mut p) = k.as_ref().find(TRACE_ID_SEPARATOR) {
            if let Some(pp) = k.as_ref()[..p].find("/schema=") {
                p = pp;
            }
            k.as_ref()[p..].to_string()
        } else {
            k.to_string()
        }
    }
}

impl Default for FileStatisticsCache {
    fn default() -> Self {
        Self::new()
    }
}

impl Cache<TableScopedPath, CachedFileMetadata> for FileStatisticsCache {
    /// Get cached metadata for file location.
    fn get(&self, k: &TableScopedPath) -> Option<CachedFileMetadata> {
        let k = self.format_key(&k.path);
        match self.statistics.get(&k) {
            Some(s) => {
                metrics::QUERY_PARQUET_METADATA_CACHE_HITS_TOTAL
                    .with_label_values::<&str>(&[])
                    .inc();
                let (meta, statistics, fingerprint, _) = s.value();
                Some(CachedFileMetadata::new(
                    meta.clone(),
                    fingerprint.clone(),
                    statistics.clone(),
                    None,
                ))
            }
            None => {
                metrics::QUERY_PARQUET_METADATA_CACHE_MISS_TOTAL
                    .with_label_values::<&str>(&[])
                    .inc();
                None
            }
        }
    }

    /// Save collected file statistics
    fn put(&self, k: &TableScopedPath, value: CachedFileMetadata) -> Option<CachedFileMetadata> {
        let k = self.format_key(&k.path);
        let entry_size = Self::estimate_entry_size(&k, &value.meta, &value.statistics)
            + std::mem::size_of::<Arc<SchemaFingerprint>>()
            // Conservatively charge each entry even when a fingerprint is shared.
            // DFHeapSize includes the Arc allocation and nested column names/types.
            + value.schema_fingerprint.heap_size(&mut DFHeapSizeCtx::default());

        let max_bytes = config::get_config()
            .limit
            .datafusion_file_stat_cache_max_size;
        let mut queue = self.cacher.lock();
        let old = self.statistics.insert(
            k.clone(),
            (
                value.meta.clone(),
                value.statistics.clone(),
                value.schema_fingerprint.clone(),
                entry_size,
            ),
        );
        let old_size = old.as_ref().map(|(_, _, _, s)| *s).unwrap_or(0);
        let memory = self
            .memory_size()
            .checked_sub(old_size)
            .and_then(|size| size.checked_add(entry_size))
            .expect("file statistics memory accounting overflow or underflow");
        self.current_memory.store(memory, Ordering::Relaxed);

        // Replacement preserves FIFO age; removal followed by insertion does not.
        if old.is_none() {
            queue.push_back(k);
        }
        drop(queue);

        if memory > max_bytes {
            self.evict(max_bytes);
        }

        old.map(|(meta, stats, fingerprint, _)| {
            CachedFileMetadata::new(meta, fingerprint, stats, None)
        })
    }

    fn remove(&self, k: &TableScopedPath) -> Option<CachedFileMetadata> {
        let k = self.format_key(&k.path);
        let mut queue = self.cacher.lock();
        self.statistics
            .remove(&k)
            .map(|(_, (meta, stats, fingerprint, size))| {
                queue.retain(|queued| queued != &k);
                self.current_memory.store(
                    self.memory_size()
                        .checked_sub(size)
                        .expect("file statistics memory accounting underflow"),
                    Ordering::Relaxed,
                );
                CachedFileMetadata::new(meta, fingerprint, stats, None)
            })
    }

    fn contains_key(&self, k: &TableScopedPath) -> bool {
        let k = self.format_key(&k.path);
        self.statistics.contains_key(&k)
    }

    fn len(&self) -> usize {
        self.statistics.len()
    }

    fn clear(&self) {
        let mut queue = self.cacher.lock();
        self.statistics.clear();
        queue.clear();
        self.current_memory.store(0, Ordering::Relaxed);
    }

    fn name(&self) -> String {
        "FileStatisticsCache".to_string()
    }

    fn cache_limit(&self) -> usize {
        config::get_config()
            .limit
            .datafusion_file_stat_cache_max_size
    }

    fn update_cache_limit(&self, _limit: usize) {
        // No-op: this is a process-wide, long-lived global cache whose eviction
        // budget is owned solely by `put()` via `datafusion_file_stat_cache_max_size`.
        // DataFusion calls this once per `CacheManager::try_new` (i.e. per session /
        // per query) with its own per-session limit;
    }

    fn cache_ttl(&self) -> Option<std::time::Duration> {
        None
    }

    fn update_cache_ttl(&self, _ttl: Option<std::time::Duration>) {}

    fn list_entries(&self) -> HashMap<TableScopedPath, CacheEntryInfo<CachedFileMetadata>> {
        self.statistics
            .iter()
            .map(|entry| {
                let path = TableScopedPath {
                    table: None,
                    path: Path::from(entry.key().as_str()),
                };
                let (meta, stats, fingerprint, size) = entry.value();
                (
                    path,
                    CacheEntryInfo {
                        value: CachedFileMetadata::new(
                            meta.clone(),
                            fingerprint.clone(),
                            stats.clone(),
                            None,
                        ),
                        size_bytes: *size,
                        hits: 0,
                        expires: None,
                    },
                )
            })
            .collect()
    }

    fn drop_table_entries(&self, _table_ref: &TableReference) -> datafusion::error::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    use super::*;

    /// Wrap a [`Path`] into the table-scoped key the cache API expects.
    fn key(location: &Path) -> TableScopedPath {
        TableScopedPath {
            table: None,
            path: location.clone(),
        }
    }

    /// Parse an RFC3339 timestamp, panicking on malformed input (test-only).
    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().into()
    }

    /// Minimal [`ObjectMeta`] for a location, fixed `last_modified`, no etag/version.
    fn object_meta(location: &str, size: u64) -> ObjectMeta {
        ObjectMeta {
            location: Path::from(location),
            last_modified: ts("2024-01-15T00:00:00+00:00"),
            size,
            e_tag: None,
            version: None,
        }
    }

    /// Unknown statistics for the given fields, ready to cache.
    fn unknown_stats(fields: Vec<Field>) -> Arc<Statistics> {
        Statistics::new_unknown(&Schema::new(fields)).into()
    }

    /// Insert an entry into the cache under its own location.
    fn put(cache: &FileStatisticsCache, meta: ObjectMeta, stats: Arc<Statistics>) {
        cache.put(
            &key(&meta.location),
            CachedFileMetadata::new(
                meta,
                Arc::new(SchemaFingerprint::from_schema(&Schema::empty())),
                stats,
                None,
            ),
        );
    }

    #[test]
    fn test_file_statistics_cache() {
        let meta = object_meta("test", 1024);
        let cache = FileStatisticsCache::default();
        assert!(cache.get(&key(&meta.location)).is_none());

        let stats = unknown_stats(vec![Field::new(
            "test_column",
            DataType::Timestamp(TimeUnit::Second, None),
            false,
        )]);
        put(&cache, meta.clone(), stats);

        // exact match is valid
        let cached = cache.get(&key(&meta.location)).expect("entry present");
        assert!(cached.is_valid_for(
            &meta,
            &Arc::new(SchemaFingerprint::from_schema(&Schema::empty()))
        ));

        // same location but file size changed -> cached but stale
        let mut changed = meta.clone();
        changed.size = 2048;
        let cached = cache.get(&key(&changed.location)).expect("entry present");
        assert!(!cached.is_valid_for(
            &changed,
            &Arc::new(SchemaFingerprint::from_schema(&Schema::empty()))
        ));

        // same location but last_modified changed -> cached but stale
        let mut changed = meta.clone();
        changed.last_modified = ts("2024-01-15T01:00:00+00:00");
        let cached = cache.get(&key(&changed.location)).expect("entry present");
        assert!(!cached.is_valid_for(
            &changed,
            &Arc::new(SchemaFingerprint::from_schema(&Schema::empty()))
        ));

        // different location -> miss
        assert!(cache.get(&key(&Path::from("test2"))).is_none());
    }

    #[test]
    fn test_memory_size_calculation() {
        let cache = FileStatisticsCache::new();
        assert_eq!(cache.memory_size(), 0, "empty cache tracks zero memory");

        for i in 0..10u64 {
            let mut meta = object_meta(&format!("test_file_{i}"), 1024 * (i + 1));
            meta.e_tag = Some(format!("etag_{i}"));
            meta.version = Some(format!("v{i}"));
            let stats = unknown_stats(vec![
                Field::new("column1", DataType::Utf8, false),
                Field::new("column2", DataType::Int64, true),
                Field::new(
                    "column3",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    false,
                ),
            ]);
            put(&cache, meta, stats);
        }

        assert!(
            cache.memory_size() > 0,
            "memory tracked after inserting entries"
        );
        assert_eq!(cache.len(), 10);

        cache.clear();
        assert_eq!(cache.memory_size(), 0, "cleared cache returns to zero");
    }

    #[test]
    fn test_cache_name() {
        assert_eq!(FileStatisticsCache::new().name(), "FileStatisticsCache");
    }

    #[test]
    fn test_cache_contains_key_and_remove() {
        let cache = FileStatisticsCache::new();
        let meta = object_meta("test_file", 512);
        let k = key(&meta.location);

        assert!(!cache.contains_key(&k));

        put(
            &cache,
            meta,
            unknown_stats(vec![Field::new("col", DataType::Utf8, false)]),
        );
        assert!(cache.contains_key(&k));
        assert_eq!(cache.len(), 1);

        assert!(cache.remove(&k).is_some());
        assert!(!cache.contains_key(&k));
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_cache_clear() {
        let cache = FileStatisticsCache::new();
        for i in 0..3u64 {
            let meta = object_meta(&format!("file_{i}"), 100 * (i + 1));
            put(
                &cache,
                meta,
                unknown_stats(vec![Field::new("col", DataType::Int64, false)]),
            );
        }

        assert_eq!(cache.len(), 3);
        cache.clear();
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_list_entries() {
        use datafusion::execution::cache::Cache as FscTrait;

        let cache = FileStatisticsCache::new();
        let meta = object_meta("list_test", 256);
        let k = key(&meta.location);

        assert!(FscTrait::list_entries(&cache).is_empty());

        put(
            &cache,
            meta,
            unknown_stats(vec![Field::new("col", DataType::Utf8, false)]),
        );
        let entries = FscTrait::list_entries(&cache);
        assert_eq!(entries.len(), 1);
        assert!(entries.contains_key(&k));
    }

    #[test]
    fn test_cache_remove_nonexistent_returns_none() {
        let cache = FileStatisticsCache::new();
        assert!(cache.remove(&key(&Path::from("does_not_exist"))).is_none());
    }

    #[test]
    fn replacement_and_fifo_eviction_account_for_schema_fingerprint() {
        let cache = FileStatisticsCache::new();
        let schema = Schema::new(vec![Field::new(
            "long_column_name".repeat(64),
            DataType::Utf8,
            true,
        )]);
        let fingerprint = Arc::new(SchemaFingerprint::from_schema(&schema));
        let stats = Arc::new(Statistics::new_unknown(&schema));
        let first = object_meta("file_a", 512);
        let second = object_meta("file_b", 512);
        for meta in [&first, &second] {
            cache.put(
                &key(&meta.location),
                CachedFileMetadata::new(
                    meta.clone(),
                    Arc::clone(&fingerprint),
                    Arc::clone(&stats),
                    None,
                ),
            );
        }
        let entry_size = cache.memory_size() / 2;
        let stats_only = FileStatisticsCache::estimate_entry_size("file_a", &first, &stats);
        assert!(entry_size > stats_only + schema.field(0).name().len());
        cache.put(
            &key(&first.location),
            CachedFileMetadata::new(first.clone(), fingerprint, stats, None),
        );
        assert_eq!(cache.memory_size(), entry_size * 2);
        assert_eq!(
            cache.cacher.lock().len(),
            2,
            "replacement must not add a FIFO entry"
        );
        cache.evict(entry_size);
        assert!(
            !cache.contains_key(&key(&first.location)),
            "replacement does not refresh FIFO age"
        );
        assert!(cache.contains_key(&key(&second.location)));
        assert_eq!(cache.memory_size(), entry_size);
        cache.remove(&key(&second.location));
        assert_eq!(cache.memory_size(), 0);
    }

    fn assert_consistent(cache: &FileStatisticsCache) {
        let queue = cache.cacher.lock();
        let unique: std::collections::HashSet<_> = queue.iter().collect();
        assert_eq!(queue.len(), unique.len(), "duplicate FIFO keys");
        assert_eq!(queue.len(), cache.statistics.len(), "orphan FIFO keys");
        let mut actual = 0usize;
        for entry in &cache.statistics {
            assert!(unique.contains(entry.key()));
            actual = actual.checked_add(entry.value().3).unwrap();
        }
        assert_eq!(cache.memory_size(), actual);
    }

    #[test]
    fn remove_reinsert_keeps_queue_bounded() {
        let cache = FileStatisticsCache::new();
        let stats = unknown_stats(vec![Field::new("col", DataType::Utf8, true)]);
        for _ in 0..100 {
            for i in 0..32 {
                let meta = object_meta(&format!("file_{i}"), 512);
                put(&cache, meta.clone(), Arc::clone(&stats));
                assert_consistent(&cache);
                put(&cache, meta.clone(), Arc::clone(&stats));
                assert_consistent(&cache);
                assert!(cache.remove(&key(&meta.location)).is_some());
                assert_consistent(&cache);
                assert!(cache.remove(&key(&meta.location)).is_none());
                put(&cache, meta, Arc::clone(&stats));
            }
            assert_eq!(cache.queue_len(), 32);
            assert_consistent(&cache);
        }
        cache.clear();
        assert_eq!(cache.queue_len(), 0);
        assert_consistent(&cache);
    }

    #[test]
    fn reinserted_key_is_not_evicted_by_its_old_queue_slot() {
        let cache = FileStatisticsCache::new();
        let stats = unknown_stats(vec![]);
        let first = object_meta("file_a", 512);
        let second = object_meta("file_b", 512);
        put(&cache, first.clone(), Arc::clone(&stats));
        let entry_size = cache.memory_size();
        put(&cache, second.clone(), Arc::clone(&stats));
        cache.remove(&key(&first.location)).unwrap();
        let mut replacement = first.clone();
        replacement.size = 1024;
        put(&cache, replacement, stats);
        assert_consistent(&cache);

        cache.evict(entry_size);
        assert!(cache.get(&key(&second.location)).is_none());
        assert_eq!(cache.get(&key(&first.location)).unwrap().meta.size, 1024);
        assert_eq!(cache.memory_size(), entry_size);
        assert_eq!(cache.queue_len(), 1);
        assert_consistent(&cache);
        cache.evict(0);
        assert!(cache.is_empty());
        assert_eq!(cache.queue_len(), 0);
        assert_consistent(&cache);
    }

    #[test]
    fn concurrent_mutations_keep_queue_and_memory_consistent() {
        let cache = Arc::new(FileStatisticsCache::new());
        let barrier = Arc::new(std::sync::Barrier::new(4));
        std::thread::scope(|scope| {
            for worker in 0..4 {
                let cache = Arc::clone(&cache);
                let barrier = Arc::clone(&barrier);
                scope.spawn(move || {
                    barrier.wait();
                    for i in 0..1000 {
                        let meta = object_meta(&format!("file_{}", i % 16), 512);
                        let stats = unknown_stats(vec![Field::new(
                            "col".repeat(i % 8 + 1),
                            DataType::Utf8,
                            true,
                        )]);
                        put(&cache, meta.clone(), stats);
                        let _ = cache.get(&key(&meta.location));
                        match (i + worker) % 4 {
                            0 => {
                                cache.remove(&key(&meta.location));
                            }
                            1 => cache.evict(4096),
                            2 => cache.clear(),
                            _ => {}
                        }
                        assert_consistent(&cache);
                    }
                });
            }
        });
        assert_consistent(&cache);
        cache.evict(0);
        assert_eq!(cache.queue_len(), 0);
        assert_eq!(cache.memory_size(), 0);
    }
}
