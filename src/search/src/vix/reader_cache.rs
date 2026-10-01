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

use std::sync::{
    Arc, LazyLock as Lazy, Weak,
    atomic::{AtomicUsize, Ordering},
};

use config::metrics;
use hashlink::LruCache;
use vortex_index::{ReaderMemoryObserver, VixReader};

pub static GLOBAL_CACHE: Lazy<VixReaderCache> =
    Lazy::new(|| VixReaderCache::new(config::get_config().limit.vix_reader_cache_max_size));

/// Fraction of [`struct@VixReaderCache`]'s byte budget reserved for full
/// (non-demoted) readers. Entries beyond it are demoted to the
/// metadata-only tier instead of evicted: a demoted reader keeps every
/// per-file metadata read free (puffin footer, `fields`, zone map, the
/// dict block index, the terms Vortex footer) and re-fetches only its
/// query-specific blocks/leaves (2 reads for an exact count, 4 for a
/// top-N, vs 7-8 cold), so a whole 7-day window fits the cache. Hot files
/// (refreshed by lookups) stay full; the LRU tail degrades to metadata,
/// not to a cold reopen. Measured 2026-10-01 on prod `.185` (3 GiB
/// budget): full readers ~740 KB (1,035 of them at the quarter budget),
/// demoted ~645 KB traces / ~855 KB logs; a 24 h window is ~1,300 files
/// per follower and a 7 d traces window ~7,000. A full reader saves one
/// more read than a demoted one (its cached key blocks), so an eighth of
/// the budget - ~700 full readers at 4 GiB, half a recent day - keeps the
/// most-recent dashboards' files full while 7/8 of the budget holds the
/// 7 d metadata tail.
const HOT_BUDGET_FRACTION: usize = 8;

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

/// The reader tier of one cache entry. `Full` readers retain every lazily
/// built structure (dictionary indexes, cached blocks); `Metadata` readers
/// have been demoted — they still answer every query, but re-fetch their
/// blocks/leaves through the normal paths on first use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Tier {
    Full,
    Metadata,
}

impl Tier {
    fn label(&self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Metadata => "metadata",
        }
    }
}

struct CachedReader {
    reader: Arc<VixReader>,
    /// Outstanding shared leases on this entry. Zero means the cache owns
    /// the reader exclusively — the gate for demotion and the fast path
    /// for eviction.
    leases: Arc<AtomicUsize>,
    tier: Tier,
    accounted: usize,
    observer: Arc<MemoryObserver>,
}

/// Waiting handles never own reader allocations. A cache eviction can
/// destroy the reader even with arbitrarily many operations queued behind
/// a lease release.
pub(super) struct ReaderHandle {
    reader: Weak<VixReader>,
    leases: Arc<AtomicUsize>,
    has_index: bool,
}

impl ReaderHandle {
    pub(super) fn has_index(&self) -> bool {
        self.has_index
    }

    /// Grant a shared lease immediately. Leases on the same cached reader
    /// overlap freely: concurrent evaluations of the same file are safe
    /// because every mutable reader structure is mutex- or OnceLock-
    /// protected, and each lease charges its own operation's memory budget
    /// through `check_read_memory` (double-counting across concurrent ops
    /// is intended — conservative admission). Cancellation is checked
    /// before granting, so a cancelled operation never opens a reader.
    pub(super) async fn lock(
        self,
        operation: &Arc<super::source::ReadOperation>,
    ) -> anyhow::Result<LockedReader> {
        if operation.is_cancelled() {
            return Err(vortex_index::VixError::Cancelled.into());
        }
        Ok(LockedReader {
            reader: self.reader,
            leases: Some(self.leases),
        })
    }

    pub(super) fn try_lock(self) -> Option<LockedReader> {
        Some(LockedReader {
            reader: self.reader,
            leases: Some(self.leases),
        })
    }
}

/// The lease grant happens before CPU/byte admission. Upgrade only after
/// that admission, inside the operation scope, and immediately charge the
/// footprint. Counting the lease on upgrade (not on lock) keeps the
/// demotion gate exact: an admission-refused operation never pins the
/// reader, and a lease whose reader was evicted upgrades to `None`.
pub(super) struct LockedReader {
    reader: Weak<VixReader>,
    leases: Option<Arc<AtomicUsize>>,
}

impl LockedReader {
    pub(super) fn upgrade(self) -> anyhow::Result<Option<ReaderLease>> {
        let Some(reader) = self.reader.upgrade() else {
            return Ok(None);
        };
        let leases = self.leases.clone();
        if let Some(count) = &leases {
            count.fetch_add(1, Ordering::AcqRel);
        }
        let lease = ReaderLease { reader, leases };
        vortex_index::check_read_memory(lease.memory_size())?;
        Ok(Some(lease))
    }
}

/// Field order is intentional: release the last reader owner before the
/// lease counter drops, and before the enclosing evaluation releases its
/// permit.
pub(super) struct ReaderLease {
    reader: Arc<VixReader>,
    leases: Option<Arc<AtomicUsize>>,
}

impl std::ops::Deref for ReaderLease {
    type Target = VixReader;
    fn deref(&self) -> &Self::Target {
        &self.reader
    }
}

impl Drop for ReaderLease {
    fn drop(&mut self) {
        if let Some(count) = &self.leases {
            count.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

impl ReaderLease {
    pub(super) fn private(reader: VixReader) -> anyhow::Result<Self> {
        let lease = Self {
            reader: Arc::new(reader),
            leases: None,
        };
        vortex_index::check_read_memory(lease.memory_size())?;
        Ok(lease)
    }
}

type ReaderLru = LruCache<ReaderCacheKey, CachedReader>;

struct CacheState {
    lru: ReaderLru,
    /// Cache-owned bytes: every entry's accounted weight, full or demoted.
    total: usize,
    /// Bytes of full (non-demoted) entries only, for the hot budget.
    hot: usize,
    /// Entry count per tier, maintained alongside the byte totals so gauge
    /// publication stays O(1).
    full_entries: usize,
    metadata_entries: usize,
    demotions: usize,
}

impl CacheState {
    fn new() -> Self {
        Self {
            lru: LruCache::new_unbounded(),
            total: 0,
            hot: 0,
            full_entries: 0,
            metadata_entries: 0,
            demotions: 0,
        }
    }

    fn update_gauges(&self) {
        metrics::VIX_READER_CACHE_ENTRIES
            .with_label_values::<&str>(&[])
            .set(self.lru.len() as i64);
        metrics::VIX_READER_CACHE_MEMORY_BYTES
            .with_label_values::<&str>(&[])
            .set(self.total as i64);
        metrics::VIX_READER_CACHE_TIER_ENTRIES
            .with_label_values(&[Tier::Full.label()])
            .set(self.full_entries as i64);
        metrics::VIX_READER_CACHE_TIER_ENTRIES
            .with_label_values(&[Tier::Metadata.label()])
            .set(self.metadata_entries as i64);
        metrics::VIX_READER_CACHE_TIER_BYTES
            .with_label_values(&[Tier::Full.label()])
            .set(self.hot as i64);
        metrics::VIX_READER_CACHE_TIER_BYTES
            .with_label_values(&[Tier::Metadata.label()])
            .set(self.total.saturating_sub(self.hot) as i64);
    }
}

struct CacheInner {
    state: parking_lot::Mutex<CacheState>,
    max_bytes: usize,
    /// Byte budget for full readers: `max_bytes / HOT_BUDGET_FRACTION`.
    hot_bytes: usize,
}

impl CacheInner {
    /// Demote LRU-first full entries while the hot budget is exceeded.
    /// An entry is demotable only when the cache owns the reader solely —
    /// `Arc::get_mut` proves sole strong AND weak ownership, which means
    /// no lease is outstanding (every live lease holds a strong Arc; a
    /// cached-out handle holds a Weak that would fail `get_mut`). A busy
    /// entry is skipped and the scan stops when no full entry is
    /// demotable; enforcement retries on the next mutation.
    fn demote_overflow(&self, state: &mut CacheState) {
        while state.hot > self.hot_bytes {
            let Some(key) = state
                .lru
                .iter()
                .find(|(_, entry)| {
                    entry.tier == Tier::Full && Arc::strong_count(&entry.reader) == 1
                })
                .map(|(key, _)| key.clone())
            else {
                break;
            };
            let Some(entry) = state.lru.peek_mut(&key) else {
                continue;
            };
            let released = Arc::get_mut(&mut entry.reader).map_or(0, |reader| reader.demote());
            let shrink = entry.accounted.min(released);
            // The entry leaves the full tier entirely: its remaining
            // accounted weight moves out of the hot budget even when
            // nothing lazily built was releasable (the reader was already
            // metadata-sized when full).
            state.hot -= entry.accounted;
            entry.accounted -= shrink;
            entry.tier = Tier::Metadata;
            state.total -= shrink;
            state.full_entries -= 1;
            state.metadata_entries += 1;
            state.demotions += 1;
            metrics::VIX_READER_CACHE_DEMOTIONS_TOTAL
                .with_label_values::<&str>(&[])
                .inc();
        }
    }

    /// Evict LRU-first until `extra` bytes fit the total budget, like the
    /// pre-tier loop. Evicted entries are returned for destruction after
    /// the state lock drops (never under it).
    fn evict_overflow(&self, state: &mut CacheState, extra: usize) -> Vec<CachedReader> {
        let mut evicted = Vec::new();
        while state.total > self.max_bytes - extra {
            let Some((_, entry)) = state.lru.remove_lru() else {
                break;
            };
            match entry.tier {
                Tier::Full => {
                    state.hot -= entry.accounted;
                    state.full_entries -= 1;
                }
                Tier::Metadata => state.metadata_entries -= 1,
            }
            state.total -= entry.accounted;
            evicted.push(entry);
        }
        evicted
    }
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
            match entry.tier {
                Tier::Full => {
                    state.hot -= entry.accounted;
                    state.full_entries -= 1;
                }
                Tier::Metadata => state.metadata_entries -= 1,
            }
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
            match entry.tier {
                Tier::Full => {
                    state.hot -= entry.accounted;
                    state.full_entries -= 1;
                }
                Tier::Metadata => state.metadata_entries -= 1,
            }
            state.total -= entry.accounted;
            removed_self = std::ptr::eq(Arc::as_ptr(&entry.observer), self);
            evicted.push(entry);
            if removed_self {
                break;
            }
        }
        if !removed_self {
            let demoted = state
                .lru
                .peek(&self.key)
                .is_some_and(|entry| entry.tier == Tier::Metadata);
            let entry = state.lru.peek_mut(&self.key).unwrap();
            entry.accounted = current;
            if demoted {
                // Regrowth after demotion rebuilt full-tier structures
                // (indexes, blocks); the entry re-enters the hot budget so
                // demotion can release them again under pressure.
                entry.tier = Tier::Full;
                state.metadata_entries -= 1;
                state.full_entries += 1;
                state.hot += current;
            } else {
                state.hot += delta;
            }
            state.total += delta;
        }
        // Growth may push full readers past the hot budget: demote before
        // returning (the growing entry itself holds a lease and is never
        // demotable here).
        cache.demote_overflow(&mut state);
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
        + std::mem::size_of::<AtomicUsize>()
        + 4 * std::mem::size_of::<usize>()
}

/// A size-bounded LRU of parsed readers keyed by immutable sidecar identity.
///
/// The budget/gauges describe cache-owned reader weights plus entry metadata,
/// not process RSS: active Arc users may pin evicted readers until they finish.
/// Shared readers are conservatively charged once per admitted key, retaining
/// each admission's observed high-water weight even if reader storage shrinks.
/// Growth callbacks enforce the budget before returning; they never refresh LRU.
/// Get/put/notification bookkeeping is O(1), plus O(entries actually demoted
/// or evicted). Logical-file invalidation alone walks the LRU to find all
/// generations.
pub struct VixReaderCache {
    inner: Arc<CacheInner>,
}

impl VixReaderCache {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            inner: Arc::new(CacheInner {
                state: parking_lot::Mutex::new(CacheState::new()),
                max_bytes,
                hot_bytes: max_bytes / HOT_BUDGET_FRACTION,
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
                leases: Arc::clone(&entry.leases),
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

    /// Publish an already operation-admitted cold reader. Duplicate opens
    /// stay private; they cannot mutate the winner through an escaping Arc.
    pub(super) fn put(
        &self,
        key: ReaderCacheKey,
        reader: VixReader,
    ) -> anyhow::Result<ReaderLease> {
        let lease = ReaderLease::private(reader)?;
        self.put_and_observe(key, Arc::clone(&lease.reader), |reader, observer| {
            reader.observe_memory(Arc::downgrade(&observer))
        })?;
        Ok(lease)
    }

    fn put_and_observe(
        &self,
        key: ReaderCacheKey,
        reader: Arc<VixReader>,
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
            evicted.extend(self.inner.evict_overflow(&mut state, size));
            state.total += size;
            state.hot += size;
            state.full_entries += 1;
            state.lru.insert(
                key,
                CachedReader {
                    reader: Arc::clone(&reader),
                    leases: Arc::new(AtomicUsize::new(0)),
                    tier: Tier::Full,
                    accounted: size,
                    observer: Arc::clone(&observer),
                },
            );
            // A new admission can push older full readers past the hot
            // budget; the new entry is MRU and demoted last.
            self.inner.demote_overflow(&mut state);
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
                match entry.tier {
                    Tier::Full => {
                        state.hot -= entry.accounted;
                        state.full_entries -= 1;
                    }
                    Tier::Metadata => state.metadata_entries -= 1,
                }
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
            self.put_and_observe(key, reader, |reader, observer| {
                reader.observe_memory(Arc::downgrade(&observer))
            })
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

    /// Shared leases: eviction destroys a reader once no lease pins it,
    /// handles never pin readers, a cancelled operation's lock fails
    /// without cancelling the others, and expired handles reopen.
    #[tokio::test]
    async fn shared_leases_never_pin_evicted_reader_growth() {
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
        // Handles are weak; the published reader still has the cold lease.
        let first = cache.get(&key).unwrap();
        let weak = first.reader.clone();
        let second = cache.get(&key).unwrap();
        // A cancelled operation's lock fails fast; unrelated operations run.
        let cancelled = super::super::source::ReadOperation::new(
            Arc::new(super::super::source::FetchStats::default()),
            None,
        );
        cancelled.cancel();
        assert!(
            matches!(
                cache.get(&key).unwrap().lock(&cancelled).await,
                Err(error) if super::super::is_cancelled_read(&error)
            ),
            "cancelled operations must not acquire leases"
        );
        let first_locked = first.try_lock().unwrap();
        let second_locked = second.try_lock().unwrap();
        assert!(!operation.is_cancelled());

        // Overlapping upgrades on one entry both count and both work.
        let before = lease.memory_size();
        let first_lease = first_locked.upgrade().unwrap().unwrap();
        let second_lease = second_locked.upgrade().unwrap().unwrap();
        assert_eq!(
            cache
                .inner
                .state
                .lock()
                .lru
                .peek(&key)
                .unwrap()
                .leases
                .load(Ordering::Acquire),
            2
        );
        let query = vortex_index::VixQuery::Exact {
            field: "level".to_owned(),
            token: b"a".to_vec(),
        };
        assert_eq!(
            first_lease.eval(&query).unwrap().count_set_bits()
                + second_lease.eval(&query).unwrap().count_set_bits(),
            2
        );
        drop(first_lease);
        drop(second_lease);
        assert_eq!(
            cache
                .inner
                .state
                .lock()
                .lru
                .peek(&key)
                .unwrap()
                .leases
                .load(Ordering::Acquire),
            0,
            "lease count must return to zero"
        );

        // The first lazy index build grew the reader: the second eval
        // reuses it, and no handle may keep it alive after eviction.
        assert!(
            lease.memory_size() > before,
            "fixture must retain lazy index growth"
        );
        cache.remove(key.file());
        drop(lease);
        drop(permit);
        assert!(weak.upgrade().is_none(), "handles pinned an evicted reader");
        // The eviction of a removed entry already dropped the reader; a
        // fresh lookup must miss (the next open republishes cold).
        assert!(
            cache.get(&key).is_none(),
            "expired entries must reopen cold"
        );
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
            .put_and_observe(key.clone(), Arc::clone(&reader), |reader, observer| {
                // Lazy growth during registration must be included in the initial
                // publication weight, even though callbacks cannot find an entry yet.
                let query = vortex_index::VixQuery::Exact {
                    field: "level".to_string(),
                    token: b"a".to_vec(),
                };
                assert_eq!(reader.eval(&query).unwrap().count_set_bits(), 1);
                reader.observe_memory(Arc::downgrade(&observer))
            })
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
                    .put_and_observe(key.clone(), Arc::clone(&reader), |reader, observer| {
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
                    })
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
                    .put_and_observe(key.clone(), Arc::clone(first), |reader, observer| {
                        worker_barrier.wait();
                        worker_barrier.wait();
                        reader.observe_memory(Arc::downgrade(&observer))
                    })
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
            .put_and_observe(key.clone(), Arc::clone(&reader), |reader, observer| {
                cache.remove(key.file());
                cache.put_fixture(key.clone(), Arc::clone(&replacement));
                let accounted = cache.memory_size();
                reader.observe_memory(Arc::downgrade(&observer))?;
                observer.memory_changed(usize::MAX);
                assert_eq!(cache.memory_size(), accounted);
                Ok(())
            })
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

    /// The two budgets: fill past the hot budget and entries are DEMOTED
    /// (metadata tier, still cached, shrunk), not evicted; pass the total
    /// budget and only then do entries evict. A demoted entry is
    /// byte-accounted at its shrunk weight.
    #[test]
    fn hot_budget_demotes_before_total_budget_evict() {
        let fresh = || {
            let (data, index) = reader_files(["a", "b"]);
            VixReader::open_with_index(bytes::Bytes::from(data), index.map(bytes::Bytes::from))
                .unwrap()
        };
        let key = |file: &str| ReaderCacheKey::new(file.to_string(), 7, 100);
        let probe = fresh();
        let entry_size = probe.memory_size() + entry_overhead(&key("file-0"));
        drop(probe);
        // Total budget fits 6 entries; the hot budget (1/4) fits 1.
        let cache = VixReaderCache::new(entry_size * 6);
        for i in 0..5 {
            cache.put(key(&format!("file-{i}")), fresh()).unwrap();
        }
        let (entries, metadata, full, demotions) = {
            let state = cache.inner.state.lock();
            (
                state.full_entries + state.metadata_entries,
                state.metadata_entries,
                state.full_entries,
                state.demotions,
            )
        };
        assert_eq!(entries, 5);
        assert!(metadata > 0, "overflow past hot must demote");
        assert!(full >= 1, "the hot budget still holds readers");
        assert!(cache.memory_size() <= entry_size * 6);
        assert_eq!(demotions, metadata);
        // Demoted entries are still cached: a lookup hits and uses them.
        let lease = cache
            .get(&key("file-0"))
            .unwrap()
            .try_lock()
            .unwrap()
            .upgrade()
            .unwrap()
            .unwrap();
        assert_eq!(
            lease
                .eval(&vortex_index::VixQuery::Exact {
                    field: "level".to_owned(),
                    token: b"a".to_vec()
                })
                .unwrap()
                .count_set_bits(),
            1
        );

        // Past the TOTAL budget, entries evict LRU-first.
        for i in 5..12 {
            cache.put(key(&format!("file-{i}")), fresh()).unwrap();
        }
        assert!(cache.len() <= 6, "total budget must evict");
        assert!(cache.memory_size() <= entry_size * 6);
    }

    /// An outstanding lease blocks demotion: the entry stays full while a
    /// lease holds the reader, and the hot budget skips it.
    #[test]
    fn outstanding_lease_blocks_demotion() {
        let fresh = || {
            let (data, index) = reader_files(["a", "b"]);
            VixReader::open_with_index(bytes::Bytes::from(data), index.map(bytes::Bytes::from))
                .unwrap()
        };
        let key = |file: &str| ReaderCacheKey::new(file.to_string(), 7, 100);
        let probe = fresh();
        let entry_size = probe.memory_size() + entry_overhead(&key("file-0"));
        drop(probe);
        let cache = VixReaderCache::new(entry_size * 16);
        cache.put(key("file-0"), fresh()).unwrap();
        // Hold a lease on file-0 BEFORE any pressure exists. The lease's
        // strong Arc blocks the sole-ownership demotion gate.
        let lease = cache
            .get(&key("file-0"))
            .unwrap()
            .try_lock()
            .unwrap()
            .upgrade()
            .unwrap()
            .unwrap();
        for i in 1..8 {
            cache.put(key(&format!("file-{i}")), fresh()).unwrap();
        }
        let state = cache.inner.state.lock();
        let leased = state.lru.peek(&key("file-0")).unwrap();
        assert_eq!(
            leased.tier,
            Tier::Full,
            "an outstanding lease must block demotion"
        );
        assert_eq!(leased.leases.load(Ordering::Acquire), 1);
        drop(state);
        drop(lease);
        // The next enforcement (a put) demotes it once free.
        cache.put(key("file-8"), fresh()).unwrap();
        assert_eq!(
            cache
                .inner
                .state
                .lock()
                .lru
                .peek(&key("file-0"))
                .unwrap()
                .tier,
            Tier::Metadata
        );
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
