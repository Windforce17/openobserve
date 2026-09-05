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
//! tail fetch and re-use every loaded cell. Identity-specific growth observers
//! reconcile each admitted reader and enforce the budget without cache sweeps
//! or another get/put. Sized by `ZO_VIX_READER_CACHE_MAX_SIZE` (default 10% of
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

use std::sync::{Arc, LazyLock as Lazy, Weak};

use config::metrics;
use hashlink::LruCache;
use tokio::sync::{Mutex as OperationMutex, OwnedMutexGuard};
use vortex_index::{ReaderMemoryObserver, VixReader};

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
        2 * self.file.capacity() + std::mem::size_of::<Self>()
    }
}

struct CachedReader {
    reader: Arc<VixReader>,
    operation: Arc<OperationMutex<()>>,
    accounted: usize,
    observer: Arc<MemoryObserver>,
}

/// Waiting handles never own reader allocations. A cache eviction can destroy
/// the reader even with arbitrarily many operations queued on its mutex.
pub(super) struct ReaderHandle {
    reader: Weak<VixReader>,
    operation: Arc<OperationMutex<()>>,
    has_index: bool,
}

impl ReaderHandle {
    pub(super) fn has_index(&self) -> bool {
        self.has_index
    }

    pub(super) async fn lock(
        self,
        operation: &Arc<super::source::ReadOperation>,
    ) -> anyhow::Result<LockedReader> {
        let guard = tokio::select! {
            biased;
            _ = operation.cancelled() => return Err(vortex_index::VixError::Cancelled.into()),
            guard = self.operation.lock_owned() => guard,
        };
        Ok(LockedReader {
            reader: self.reader,
            guard,
        })
    }

    pub(super) fn try_lock(self) -> Option<LockedReader> {
        let guard = self.operation.try_lock_owned().ok()?;
        Some(LockedReader {
            reader: self.reader,
            guard,
        })
    }
}

/// The mutex is acquired before CPU/byte admission. Upgrade only after that
/// admission, inside the operation scope, and immediately charge the footprint.
pub(super) struct LockedReader {
    reader: Weak<VixReader>,
    guard: OwnedMutexGuard<()>,
}

impl LockedReader {
    pub(super) fn upgrade(self) -> anyhow::Result<Option<ReaderLease>> {
        let Some(reader) = self.reader.upgrade() else {
            return Ok(None);
        };
        let lease = ReaderLease {
            reader,
            guard: self.guard,
        };
        vortex_index::check_read_memory(lease.memory_size())?;
        Ok(Some(lease))
    }
}

/// Field order is intentional: release the last reader owner before unlocking
/// the next operation, and before the enclosing evaluation releases its permit.
pub(super) struct ReaderLease {
    reader: Arc<VixReader>,
    guard: OwnedMutexGuard<()>,
}

impl std::ops::Deref for ReaderLease {
    type Target = VixReader;
    fn deref(&self) -> &Self::Target {
        &self.reader
    }
}

impl ReaderLease {
    pub(super) fn private(reader: VixReader) -> anyhow::Result<Self> {
        let guard = Arc::new(OperationMutex::new(()))
            .try_lock_owned()
            .expect("new reader mutex is uncontended");
        let lease = Self {
            reader: Arc::new(reader),
            guard,
        };
        vortex_index::check_read_memory(lease.memory_size())?;
        Ok(lease)
    }
}

type ReaderLru = LruCache<ReaderCacheKey, CachedReader>;

struct CacheState {
    lru: ReaderLru,
    total: usize,
}

impl CacheState {
    fn update_gauges(&self) {
        metrics::VIX_READER_CACHE_ENTRIES
            .with_label_values::<&str>(&[])
            .set(self.lru.len() as i64);
        metrics::VIX_READER_CACHE_MEMORY_BYTES
            .with_label_values::<&str>(&[])
            .set(self.total as i64);
    }
}

struct CacheInner {
    state: parking_lot::Mutex<CacheState>,
    max_bytes: usize,
}

/// One admission, not just one key: a delayed callback cannot charge a
/// replacement, even when it has the same file, generation and size.
struct MemoryObserver {
    cache: Weak<CacheInner>,
    key: ReaderCacheKey,
    overhead: usize,
}

impl ReaderMemoryObserver for MemoryObserver {
    fn memory_changed(&self, reader_bytes: usize) {
        let Some(cache) = self.cache.upgrade() else {
            return;
        };
        // Declare detached owners before the guard, including for early returns.
        let mut evicted = Vec::new();
        let mut state = cache.state.lock();
        let Some(entry) = state.lru.peek(&self.key) else {
            return;
        };
        if !std::ptr::eq(Arc::as_ptr(&entry.observer), self) {
            return;
        }
        let current = reader_bytes.checked_add(self.overhead);
        let Some(current) = current.filter(|size| *size <= cache.max_bytes) else {
            let entry = state.lru.remove(&self.key).unwrap();
            state.total -= entry.accounted;
            evicted.push(entry);
            state.update_gauges();
            drop(state);
            drop(evicted);
            return;
        };
        // Retain the admission's high-water charge: concurrent publishers may
        // arrive out of order, so a smaller snapshot cannot safely undo growth.
        if current <= entry.accounted {
            return;
        }
        let delta = current - entry.accounted;
        let mut removed_self = false;
        // Reserve room before adding the delta, avoiding usize overflow even
        // when the configured budget is usize::MAX. No reader calls under lock.
        while state.total > cache.max_bytes - delta {
            let (_, entry) = state.lru.remove_lru().unwrap();
            state.total -= entry.accounted;
            removed_self = std::ptr::eq(Arc::as_ptr(&entry.observer), self);
            evicted.push(entry);
            if removed_self {
                break;
            }
        }
        if !removed_self {
            state.lru.peek_mut(&self.key).unwrap().accounted = current;
            state.total += delta;
        }
        state.update_gauges();
        drop(state);
        drop(evicted);
    }
}

fn entry_overhead(key: &ReaderCacheKey) -> usize {
    // Both owned key strings, inline entry/observer metadata and Arc counters.
    // Hash-table spare capacity and allocator overhead are not reader payload.
    key.memory_size()
        + std::mem::size_of::<CachedReader>()
        + std::mem::size_of::<MemoryObserver>()
        + std::mem::size_of::<OperationMutex<()>>()
        + 4 * std::mem::size_of::<usize>()
}

/// A size-bounded LRU of parsed readers keyed by immutable sidecar identity.
///
/// The budget/gauges describe cache-owned reader weights plus entry metadata,
/// not process RSS: active Arc users may pin evicted readers until they finish.
/// Shared readers are conservatively charged once per admitted key, retaining
/// each admission's observed high-water weight even if reader storage shrinks.
/// Growth callbacks enforce the budget before returning; they never refresh LRU.
/// Get/put/notification bookkeeping is O(1), plus O(entries actually evicted).
/// Logical-file invalidation alone walks the LRU to find all generations.
pub struct VixReaderCache {
    inner: Arc<CacheInner>,
}

impl VixReaderCache {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            inner: Arc::new(CacheInner {
                state: parking_lot::Mutex::new(CacheState {
                    lru: LruCache::new_unbounded(),
                    total: 0,
                }),
                max_bytes,
            }),
        }
    }

    /// Get a parsed reader, refreshing its LRU position.
    pub(super) fn get(&self, key: &ReaderCacheKey) -> Option<ReaderHandle> {
        let found = self
            .inner
            .state
            .lock()
            .lru
            .get_mut(key)
            .map(|entry| ReaderHandle {
                reader: Arc::downgrade(&entry.reader),
                operation: Arc::clone(&entry.operation),
                has_index: entry.reader.has_index(),
            });
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

    /// Probe an immutable sidecar without refreshing LRU or hit/miss metrics.
    pub fn contains(&self, key: &ReaderCacheKey) -> bool {
        self.inner.state.lock().lru.contains_key(key)
    }

    /// Copy immutable ordering facts without pinning or exposing a reader.
    pub fn ordering(&self, key: &ReaderCacheKey) -> Option<(bool, Option<usize>, bool)> {
        self.inner.state.lock().lru.get_mut(key).map(|entry| {
            (
                entry.reader.row_order().is_ts_desc(),
                entry.reader.ts_desc_row_ranges().map(|ranges| ranges.len()),
                entry.reader.zone_chunks().is_some(),
            )
        })
    }

    /// Publish only an already operation-locked cold reader. Duplicate opens
    /// stay private; they cannot mutate the winner through an escaping Arc.
    pub(super) fn put(
        &self,
        key: ReaderCacheKey,
        reader: VixReader,
    ) -> anyhow::Result<ReaderLease> {
        let lease = ReaderLease::private(reader)?;
        self.put_and_observe(
            key,
            Arc::clone(&lease.reader),
            Arc::clone(OwnedMutexGuard::mutex(&lease.guard)),
            |reader, observer| reader.observe_memory(Arc::downgrade(&observer)),
        )?;
        Ok(lease)
    }

    fn put_and_observe(
        &self,
        key: ReaderCacheKey,
        reader: Arc<VixReader>,
        operation: Arc<OperationMutex<()>>,
        subscribe: impl FnOnce(&VixReader, Arc<dyn ReaderMemoryObserver>) -> vortex_index::Result<()>,
    ) -> vortex_index::Result<()> {
        if self.inner.max_bytes == 0 || self.inner.state.lock().lru.contains_key(&key) {
            return Ok(());
        }
        let overhead = entry_overhead(&key);
        let observer = Arc::new(MemoryObserver {
            cache: Arc::downgrade(&self.inner),
            key: key.clone(),
            overhead,
        });
        // Registration can fail admission or cancellation. Never publish or
        // evict anything until it succeeds; its initial callback may find no entry.
        subscribe(&reader, observer.clone())?;
        let Some(size) = reader.memory_size().checked_add(overhead) else {
            return Ok(());
        };
        if size > self.inner.max_bytes {
            return Ok(());
        }
        let mut evicted = Vec::new();
        {
            let mut state = self.inner.state.lock();
            // Another cold open may have published while we subscribed.
            if state.lru.contains_key(&key) {
                return Ok(());
            }
            while state.total > self.inner.max_bytes - size {
                let (_, entry) = state.lru.remove_lru().unwrap();
                state.total -= entry.accounted;
                evicted.push(entry);
            }
            state.total += size;
            state.lru.insert(
                key,
                CachedReader {
                    reader: Arc::clone(&reader),
                    operation,
                    accounted: size,
                    observer: Arc::clone(&observer),
                },
            );
            state.update_gauges();
        }
        drop(evicted);
        // Catch growth between the sizing snapshot and publication without
        // invoking reader callbacks under the map lock.
        observer.memory_changed(reader.memory_size());
        Ok(())
    }

    /// Release every cached generation of a logical file. Existing Arc users
    /// remain usable. Last-owner destruction happens only after unlocking.
    pub fn remove(&self, file: &str) {
        let mut removed = Vec::new();
        let mut state = self.inner.state.lock();
        let doomed = state
            .lru
            .iter()
            .filter_map(|(key, _)| (key.file() == file).then(|| key.clone()))
            .collect::<Vec<_>>();
        for key in doomed {
            if let Some(entry) = state.lru.remove(&key) {
                state.total -= entry.accounted;
                removed.push(entry);
            }
        }
        if !removed.is_empty() {
            state.update_gauges();
        }
        drop(state);
        drop(removed);
    }

    pub fn len(&self) -> usize {
        self.inner.state.lock().lru.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.state.lock().lru.is_empty()
    }

    pub fn memory_size(&self) -> usize {
        self.inner.state.lock().total
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use arrow::{
        array::{Int64Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use vortex_index::{VixWriter, VixWriterOptions};

    use super::*;

    // Observer race fixtures deliberately retain independent owners. This API
    // exists only here; production insertion consumes a private VixReader.
    impl VixReaderCache {
        fn put_fixture(&self, key: ReaderCacheKey, reader: Arc<VixReader>) {
            self.put_and_observe(
                key,
                reader,
                Arc::new(OperationMutex::new(())),
                |reader, observer| reader.observe_memory(Arc::downgrade(&observer)),
            )
            .unwrap();
        }
    }

    fn reader_files(levels: [&str; 2]) -> (Vec<u8>, Option<Vec<u8>>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("level", DataType::Utf8, true),
        ]));
        let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(levels.to_vec())),
            ],
        )
        .unwrap();
        let sources = StringArray::from(
            levels
                .map(|level| format!(r#"{{"level":"{level}"}}"#))
                .to_vec(),
        );
        writer
            .push_batch_with_source(&batch, &sources, None)
            .unwrap();
        writer.finish().unwrap()
    }

    fn small_reader() -> Arc<VixReader> {
        let (data, index) = reader_files(["a", "b"]);
        Arc::new(
            VixReader::open_with_index(bytes::Bytes::from(data), index.map(bytes::Bytes::from))
                .unwrap(),
        )
    }

    #[tokio::test]
    async fn queued_operations_never_pin_evicted_reader_growth() {
        let cache = VixReaderCache::new(usize::MAX);
        let key = ReaderCacheKey::new("queued-growth.vix".to_owned(), 7, 100);
        let operation = super::super::source::ReadOperation::new(
            Arc::new(super::super::source::FetchStats::default()),
            None,
        );
        let permit = super::super::source::acquire_evaluation(&operation, 32 * 1024 * 1024)
            .await
            .unwrap();
        let lease = operation
            .run_evaluation(&permit, || {
                let (data, index) = reader_files(["a", "b"]);
                cache.put(
                    key.clone(),
                    VixReader::open_with_index(
                        bytes::Bytes::from(data),
                        index.map(bytes::Bytes::from),
                    )
                    .unwrap(),
                )
            })
            .unwrap();
        assert!(
            cache.get(&key).unwrap().try_lock().is_none(),
            "cold insertion must already be locked"
        );
        let first = cache.get(&key).unwrap();
        let weak = first.reader.clone();
        let second = cache.get(&key).unwrap();
        let cancelled = super::super::source::ReadOperation::new(
            Arc::new(super::super::source::FetchStats::default()),
            None,
        );
        let cancelled_wait = cache.get(&key).unwrap().lock(&cancelled);
        let first_wait = first.lock(&operation);
        let second_wait = second.lock(&operation);
        tokio::pin!(cancelled_wait, first_wait, second_wait);
        assert!(futures::poll!(&mut cancelled_wait).is_pending());
        assert!(futures::poll!(&mut first_wait).is_pending());
        assert!(futures::poll!(&mut second_wait).is_pending());
        cancelled.cancel();
        assert!(
            matches!(cancelled_wait.await, Err(error) if super::super::is_cancelled_read(&error))
        );
        assert!(!operation.is_cancelled());

        // Retain lazy index growth only in the active, admitted lease. Neither
        // queued future may keep these allocations alive after eviction.
        let before = lease.memory_size();
        operation.run_evaluation(&permit, || {
            assert_eq!(
                lease
                    .eval(&vortex_index::VixQuery::Exact {
                        field: "level".to_owned(),
                        token: b"a".to_vec(),
                    })
                    .unwrap()
                    .count_set_bits(),
                1
            );
        });
        assert!(
            lease.memory_size() > before,
            "fixture must retain lazy index growth"
        );
        cache.remove(key.file());
        drop(lease);
        drop(permit);
        assert!(weak.upgrade().is_none(), "waiters pinned an evicted reader");
        let first_locked = first_wait.await.unwrap();
        assert!(
            first_locked.upgrade().unwrap().is_none(),
            "expired handles must reopen"
        );
        assert!(second_wait.await.unwrap().upgrade().unwrap().is_none());
    }

    #[test]
    fn growth_respects_lru_order_without_refreshing_the_growing_entry() {
        for touch_growing in [false, true] {
            let reader = small_reader();
            let first = ReaderCacheKey::new("file-a".to_string(), 7, 100);
            let second = ReaderCacheKey::new("file-b".to_string(), 7, 100);
            let entry_size = reader.memory_size() + entry_overhead(&first);
            let cache = VixReaderCache::new(entry_size * 3);
            cache.put_fixture(first.clone(), Arc::clone(&reader));
            cache.put_fixture(second.clone(), Arc::clone(&reader));
            if touch_growing {
                assert!(cache.get(&first).is_some());
            }
            let observer = Arc::clone(&cache.inner.state.lock().lru.peek(&first).unwrap().observer);
            let growth = cache.inner.max_bytes - cache.memory_size() + 1;
            observer.memory_changed(reader.memory_size() + growth);
            assert_eq!(cache.contains(&first), touch_growing);
            assert_eq!(cache.contains(&second), !touch_growing);
            assert!(cache.memory_size() <= cache.inner.max_bytes);
        }
    }

    #[test]
    fn put_get_and_size_bounded_eviction() {
        let reader = small_reader();
        let key = |file: &str| ReaderCacheKey::new(file.to_string(), 7, 100);
        let entry_size = reader.memory_size() + entry_overhead(&key("file-0"));

        // room for roughly two entries
        let cache = VixReaderCache::new(entry_size * 2 + entry_size / 2);
        for i in 0..3 {
            cache.put_fixture(key(&format!("file-{i}")), Arc::clone(&reader));
        }
        // the oldest entry was evicted to fit the third
        assert!(cache.get(&key("file-0")).is_none());
        assert!(cache.get(&key("file-1")).is_some());
        assert!(cache.get(&key("file-2")).is_some());
        assert_eq!(cache.len(), 2);
        assert!(cache.memory_size() <= entry_size * 2 + entry_size / 2);

        // duplicate puts do not double-count
        cache.put_fixture(key("file-2"), Arc::clone(&reader));
        assert_eq!(cache.len(), 2);

        // oversized entries are refused outright
        let tiny = VixReaderCache::new(8);
        tiny.put_fixture(key("big"), reader);
        assert!(tiny.is_empty());
    }

    #[test]
    fn growth_enforces_capacity_without_another_cache_access() {
        let reader = small_reader();
        let key = ReaderCacheKey::new("file-a".to_string(), 7, 100);
        // Include observer registration storage in the baseline, then hold the
        // reader independently of cache ownership.
        let probe = VixReaderCache::new(usize::MAX);
        probe.put_fixture(key.clone(), Arc::clone(&reader));
        let budget = probe.memory_size();
        probe.remove(key.file());
        let cache = VixReaderCache::new(budget);
        cache.put_fixture(key.clone(), Arc::clone(&reader));
        assert!(cache.contains(&key));

        let query = vortex_index::VixQuery::Exact {
            field: "level".to_string(),
            token: b"a".to_vec(),
        };
        assert_eq!(reader.eval(&query).unwrap().count_set_bits(), 1);
        // The callback must evict before eval returns, not wait for get/put.
        assert_eq!(cache.memory_size(), 0);
        assert!(!cache.contains(&key));
        assert_eq!(reader.eval(&query).unwrap().count_set_bits(), 1);
        let weak = Arc::downgrade(&reader);
        drop(reader);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn registration_reconciles_growth_before_publication() {
        let reader = small_reader();
        let key = ReaderCacheKey::new("file-a".to_string(), 7, 100);
        let probe = VixReaderCache::new(usize::MAX);
        probe.put_fixture(key.clone(), Arc::clone(&reader));
        let budget = probe.memory_size();
        probe.remove(key.file());
        let cache = VixReaderCache::new(budget);
        cache
            .put_and_observe(
                key.clone(),
                Arc::clone(&reader),
                Arc::new(OperationMutex::new(())),
                |reader, observer| {
                    // Lazy growth during registration must be included in the initial
                    // publication weight, even though callbacks cannot find an entry yet.
                    let query = vortex_index::VixQuery::Exact {
                        field: "level".to_string(),
                        token: b"a".to_vec(),
                    };
                    assert_eq!(reader.eval(&query).unwrap().count_set_bits(), 1);
                    reader.observe_memory(Arc::downgrade(&observer))
                },
            )
            .unwrap();
        assert_eq!(cache.memory_size(), 0);
        assert!(!cache.contains(&key));
    }

    #[derive(Debug, thiserror::Error)]
    #[error("observer registration admission refused")]
    struct RegistrationDenied;

    struct RegistrationAdmission {
        limit: usize,
        cancel: bool,
        refused: AtomicBool,
    }

    impl vortex_index::VixReadOperation for RegistrationAdmission {
        fn is_cancelled(&self) -> bool {
            self.cancel && self.refused.load(Ordering::Acquire)
        }

        fn check_memory(&self, owned_bytes: usize) -> vortex_index::Result<()> {
            if owned_bytes > self.limit {
                self.refused.store(true, Ordering::Release);
                Err(vortex_index::VixError::Callback(RegistrationDenied.into()))
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn cold_put_propagates_observer_registration_failure() {
        for cancel in [false, true] {
            let cache = VixReaderCache::new(usize::MAX);
            let key = ReaderCacheKey::new("cold-failure".to_owned(), 7, 100);
            let reader = Arc::try_unwrap(small_reader()).ok().unwrap();
            let operation = Arc::new(RegistrationAdmission {
                // Admit the private lease but refuse the subscription's allocation.
                limit: reader.memory_size(),
                cancel,
                refused: AtomicBool::new(false),
            });
            let error = vortex_index::with_read_operation(operation.clone(), || {
                cache.put(key.clone(), reader)
            })
            .err()
            .expect("observer registration must fail cold put");
            assert!(operation.refused.load(Ordering::Acquire));
            if cancel {
                assert!(super::super::is_cancelled_read(&error));
            } else {
                assert!(error.chain().any(|cause| cause.is::<RegistrationDenied>()));
            }
            assert!(!cache.contains(&key));
            assert_eq!(cache.memory_size(), 0);
        }
    }

    #[test]
    fn failed_registration_never_publishes_or_removes_a_concurrent_winner() {
        for cancel in [false, true] {
            for publish_winner in [false, true] {
                let cache = VixReaderCache::new(usize::MAX);
                let key = ReaderCacheKey::new("registration-failure".to_owned(), 7, 100);
                let reader = small_reader();
                let winner = small_reader();
                let operation = Arc::new(RegistrationAdmission {
                    limit: reader.memory_size(),
                    cancel,
                    refused: AtomicBool::new(false),
                });
                let mut winner_bytes = 0;
                let error = cache
                    .put_and_observe(
                        key.clone(),
                        Arc::clone(&reader),
                        Arc::new(OperationMutex::new(())),
                        |reader, observer| {
                            assert!(
                                !cache.contains(&key),
                                "subscription must precede publication"
                            );
                            if publish_winner {
                                cache.put_fixture(key.clone(), Arc::clone(&winner));
                                winner_bytes = cache.memory_size();
                            }
                            vortex_index::with_read_operation(operation.clone(), || {
                                reader.observe_memory(Arc::downgrade(&observer))
                            })
                        },
                    )
                    .unwrap_err();
                assert!(operation.refused.load(Ordering::Acquire));
                let error = anyhow::Error::new(error);
                if cancel {
                    assert!(super::super::is_cancelled_read(&error));
                } else {
                    assert!(error.chain().any(|cause| cause.is::<RegistrationDenied>()));
                }
                assert_eq!(cache.contains(&key), publish_winner);
                assert_eq!(cache.memory_size(), winner_bytes);
                let query = vortex_index::VixQuery::Exact {
                    field: "level".to_owned(),
                    token: b"a".to_vec(),
                };
                let before = reader.memory_size();
                assert_eq!(reader.eval(&query).unwrap().count_set_bits(), 1);
                assert!(reader.memory_size() > before);
                assert_eq!(cache.memory_size(), winner_bytes);
                if publish_winner {
                    assert!(Weak::ptr_eq(
                        &cache.get(&key).unwrap().reader,
                        &Arc::downgrade(&winner),
                    ));
                    assert_eq!(winner.eval(&query).unwrap().count_set_bits(), 1);
                    assert!(cache.memory_size() > winner_bytes);
                    assert_eq!(
                        cache.memory_size(),
                        winner.memory_size() + entry_overhead(&key)
                    );
                } else {
                    assert!(cache.is_empty());
                }
            }
        }
    }

    #[test]
    fn retained_growth_keeps_the_admitted_key_allocation_accounted() {
        let reader = small_reader();
        let mut file = String::with_capacity(256);
        file.push_str("file-a");
        let key = ReaderCacheKey::new(file, 7, 100);
        let overhead = entry_overhead(&key);
        let lookup = key.clone();
        let cache = VixReaderCache::new(usize::MAX);
        cache.put_fixture(key, Arc::clone(&reader));
        let query = vortex_index::VixQuery::Exact {
            field: "level".to_string(),
            token: b"a".to_vec(),
        };
        assert_eq!(reader.eval(&query).unwrap().count_set_bits(), 1);
        assert_eq!(cache.memory_size(), reader.memory_size() + overhead);
        assert!(cache.contains(&lookup));
    }

    #[test]
    fn stale_growth_cannot_shrink_or_charge_a_replacement() {
        let reader = small_reader();
        let replacement = small_reader();
        let key = ReaderCacheKey::new("file-a".to_string(), 7, 100);
        let cache = VixReaderCache::new(usize::MAX);
        cache.put_fixture(key.clone(), Arc::clone(&reader));
        let observer = Arc::clone(&cache.inner.state.lock().lru.peek(&key).unwrap().observer);
        let initial = reader.memory_size();
        observer.memory_changed(initial + 1024);
        let grown = cache.memory_size();
        observer.memory_changed(initial);
        assert_eq!(cache.memory_size(), grown);

        cache.remove(key.file());
        cache.put_fixture(key.clone(), Arc::clone(&replacement));
        let new_generation = ReaderCacheKey::new(key.file().to_string(), 8, 100);
        cache.put_fixture(new_generation.clone(), Arc::clone(&replacement));
        let replacement_bytes = cache.memory_size();
        // Simulates a notification already copied out of the reader before
        // removal. Neither the same-key admission nor new generation is owned.
        observer.memory_changed(usize::MAX);
        assert_eq!(cache.memory_size(), replacement_bytes);
        assert!(Weak::ptr_eq(
            &cache.get(&key).unwrap().reader,
            &Arc::downgrade(&replacement)
        ));
        assert!(Weak::ptr_eq(
            &cache.get(&new_generation).unwrap().reader,
            &Arc::downgrade(&replacement)
        ));
    }

    #[test]
    fn duplicate_concurrent_open_cannot_replace_the_published_winner() {
        let first = small_reader();
        let second = small_reader();
        let key = ReaderCacheKey::new("file-a".to_string(), 7, 100);
        let cache = VixReaderCache::new(usize::MAX);
        std::thread::scope(|scope| {
            let barrier = Arc::new(std::sync::Barrier::new(2));
            let worker_barrier = Arc::clone(&barrier);
            let cache = &cache;
            let key = &key;
            let first = &first;
            let handle = scope.spawn(move || {
                cache
                    .put_and_observe(
                        key.clone(),
                        Arc::clone(first),
                        Arc::new(OperationMutex::new(())),
                        |reader, observer| {
                            worker_barrier.wait();
                            worker_barrier.wait();
                            reader.observe_memory(Arc::downgrade(&observer))
                        },
                    )
                    .unwrap();
            });
            barrier.wait();
            cache.put_fixture(key.clone(), Arc::clone(&second));
            barrier.wait();
            handle.join().unwrap();
        });
        assert_eq!(cache.len(), 1);
        assert!(Weak::ptr_eq(
            &cache.get(&key).unwrap().reader,
            &Arc::downgrade(&second)
        ));
        let accounted = cache.memory_size();
        let query = vortex_index::VixQuery::Exact {
            field: "level".to_string(),
            token: b"a".to_vec(),
        };
        assert_eq!(first.eval(&query).unwrap().count_set_bits(), 1);
        assert_eq!(cache.memory_size(), accounted);
    }

    #[test]
    fn delayed_registration_after_reinsertion_cannot_remove_the_successor() {
        let reader = small_reader();
        let replacement = small_reader();
        let key = ReaderCacheKey::new("file-a".to_string(), 7, 100);
        let cache = VixReaderCache::new(usize::MAX);
        cache
            .put_and_observe(
                key.clone(),
                Arc::clone(&reader),
                Arc::new(OperationMutex::new(())),
                |reader, observer| {
                    cache.remove(key.file());
                    cache.put_fixture(key.clone(), Arc::clone(&replacement));
                    let accounted = cache.memory_size();
                    reader.observe_memory(Arc::downgrade(&observer))?;
                    observer.memory_changed(usize::MAX);
                    assert_eq!(cache.memory_size(), accounted);
                    Ok(())
                },
            )
            .unwrap();
        assert!(Weak::ptr_eq(
            &cache.get(&key).unwrap().reader,
            &Arc::downgrade(&replacement)
        ));
    }

    #[test]
    fn generations_are_distinct_and_remove_purges_the_logical_file() {
        let reader = small_reader();
        let cache = VixReaderCache::new(usize::MAX);
        let old = ReaderCacheKey::new("healed.vix".to_string(), 41, 100);
        let new_same_size = ReaderCacheKey::new("healed.vix".to_string(), 42, 100);
        let same_generation_different_size = ReaderCacheKey::new("healed.vix".to_string(), 41, 101);
        let other = ReaderCacheKey::new("other.vix".to_string(), 42, 100);
        cache.put_fixture(old.clone(), Arc::clone(&reader));
        assert!(cache.get(&new_same_size).is_none());
        assert!(
            cache.get(&same_generation_different_size).is_none(),
            "size remains a compatibility witness"
        );
        cache.put_fixture(new_same_size.clone(), Arc::clone(&reader));
        cache.put_fixture(other.clone(), Arc::clone(&reader));
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
        let entry_size = reader.memory_size() + entry_overhead(&key("file-0"));
        let cache = VixReaderCache::new(entry_size * 2 + entry_size / 2);

        cache.put_fixture(key("file-0"), Arc::clone(&reader));
        cache.put_fixture(key("file-1"), Arc::clone(&reader));
        // touch file-0: it becomes the most recently used
        assert!(cache.get(&key("file-0")).is_some());
        // inserting a third entry now evicts file-1, NOT file-0
        cache.put_fixture(key("file-2"), Arc::clone(&reader));
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

    struct DropCheckedSource {
        inner: Arc<dyn vortex_index::VixRangeSource>,
        cache: Weak<CacheInner>,
        dropped: Arc<AtomicBool>,
    }

    impl vortex_index::VixRangeSource for DropCheckedSource {
        fn len(&self) -> u64 {
            self.inner.len()
        }

        fn fetch(
            &self,
            range: std::ops::Range<u64>,
        ) -> futures::future::BoxFuture<'static, anyhow::Result<bytes::Bytes>> {
            self.inner.fetch(range)
        }
    }

    impl Drop for DropCheckedSource {
        fn drop(&mut self) {
            if let Some(cache) = self.cache.upgrade() {
                assert!(
                    cache.state.try_lock().is_some(),
                    "reader destruction must not run under the cache lock"
                );
            }
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    #[test]
    fn last_owner_release_unlocks_first_and_pinned_readers_survive() {
        // Incompressible docs larger than the data-tail probe ensure the
        // ranged reader owns this source until destruction.
        let mut seed = 0x1234_5678_u64;
        let values = (0..2)
            .map(|_| {
                (0..131_072)
                    .map(|_| {
                        seed ^= seed << 13;
                        seed ^= seed >> 7;
                        seed ^= seed << 17;
                        (b'a' + (seed % 26) as u8) as char
                    })
                    .collect::<String>()
            })
            .collect::<Vec<_>>();
        let (data, _) = reader_files([&values[0], &values[1]]);
        let data = bytes::Bytes::from(data);

        for (release, pinned) in [
            ("put", false),
            ("growth", false),
            ("remove", false),
            ("remove", true),
        ] {
            let cache = VixReaderCache::new(1024 * 1024);
            let dropped = Arc::new(AtomicBool::new(false));
            let source: Arc<dyn vortex_index::VixRangeSource> = Arc::new(DropCheckedSource {
                inner: vortex_index::BytesRangeSource::new("drop-check", data.clone()),
                cache: Arc::downgrade(&cache.inner),
                dropped: Arc::clone(&dropped),
            });
            let reader = Arc::new(VixReader::open_ranged(source).unwrap());
            let key = ReaderCacheKey::new("drop-check".to_string(), 7, data.len() as i64);
            cache.put_fixture(key.clone(), Arc::clone(&reader));
            assert!(cache.contains(&key));
            assert!(!dropped.load(Ordering::SeqCst));
            let weak = Arc::downgrade(&reader);
            let pin = pinned.then(|| Arc::clone(&reader));
            drop(reader);
            match release {
                "put" => {
                    // Each admission is deliberately distinct, even though
                    // the payload is shared, and counts its own reader weight.
                    let other = small_reader();
                    let size = other.memory_size() + entry_overhead(&key);
                    for generation in 8..(8 + cache.inner.max_bytes / size + 2) {
                        cache.put_fixture(
                            ReaderCacheKey::new("other-file".to_string(), generation as i64, 100),
                            Arc::clone(&other),
                        );
                    }
                }
                "growth" => {
                    let observer =
                        Arc::clone(&cache.inner.state.lock().lru.peek(&key).unwrap().observer);
                    observer.memory_changed(usize::MAX);
                }
                "remove" => cache.remove(key.file()),
                _ => unreachable!(),
            }
            assert!(!cache.contains(&key));
            assert_eq!(dropped.load(Ordering::SeqCst), !pinned);
            if let Some(pin) = pin {
                assert_eq!(pin.row_count(), 2);
                assert!(pin.docs_schema().unwrap().index_of("level").is_ok());
                drop(pin);
            }
            assert!(weak.upgrade().is_none());
            assert!(dropped.load(Ordering::SeqCst));
        }
    }
}
