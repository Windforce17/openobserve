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
//! Admission is by REUSE DISTANCE (see [`VixReaderCache`]); eviction within
//! the cache is LRU (a get refreshes the entry). Reader identity includes the
//! logical data key plus the immutable sidecar generation and its exact size:
//! generation prevents equal-sized heals from sharing parsed state, while
//! size remains a compatibility witness. Broadcast invalidation can still
//! purge every generation belonging to one logical data file.
//!
//! Prometheus: `vix_reader_cache_entries`, `vix_reader_cache_memory_bytes`,
//! `vix_reader_cache_{hits,misses,rejections,demotions}_total`,
//! `vix_reader_cache_evictions_total{reason="admission"|"overflow"}`.

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
/// top-N, vs 5-8 cold); a full reader also keeps its key blocks and
/// prefetched leaves (1 read). Hot files (refreshed by lookups) stay
/// full; the LRU tail degrades to metadata, not to a cold reopen.
///
/// Measured 2026-10-01 on prod (4 GiB budget): a 24 h traces window is
/// ~1,250 files per follower; full readers ~800 KB, demoted ~840 KB on the
/// 7 d traces mix (merged files carry bigger dict indexes than the 490 KB
/// L0 sample), so no budget fits a 7 d window as metadata (5.9 GB) and
/// repeated 7 d scans thrash whatever the split. A quarter for full
/// readers (~1,300 at 4 GiB) keeps a recent day's dashboards at 1 read
/// per file; at an eighth the same dashboards demoted every query and
/// paid 2 (measured 1.8 vs 1.1-1.6 reads per evaluation).
const HOT_BUDGET_FRACTION: usize = 4;

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
    /// Lookup clock value of the latest demand access (`CacheState::lookups`).
    last_lookup: u64,
    /// Lookups between the two latest demand accesses, once there have been
    /// two: the admission evidence (see `CacheInner::admit`).
    reuse: Option<u64>,
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

/// Fraction of the byte budget forming the admission WINDOW: every newly
/// published reader lands here first (FIFO among newcomers) and only
/// competes for the main cache when the window overflows. A burst of panels
/// over the same new files hits the window; a one-off scan streams through
/// it without disturbing the main cache.
const WINDOW_FRACTION: usize = 16;

/// Non-resident history size: `HISTORY_PER_ENTRY x` the resident count plus
/// a slack so small caches (tests, tiny budgets) still remember a scan's
/// worth of rejected files. A 7 d traces pass rejects ~6k files per pod
/// (~3.4k resident); its files must still be remembered on the next pass,
/// with other streams' traffic in between.
const HISTORY_PER_ENTRY: usize = 4;
const HISTORY_SLACK: usize = 1024;

fn key_hash(key: &ReaderCacheKey) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

/// Access history of a file that is NOT resident (never admitted, rejected
/// or evicted): what the next publish needs to compute its reuse distance.
#[derive(Clone, Copy)]
struct History {
    last_lookup: u64,
    reuse: Option<u64>,
}

struct CacheState {
    /// Newcomers, FIFO among themselves; bounded by `CacheInner::window_bytes`.
    window: ReaderLru,
    /// Admitted readers, LRU; `total` (window + main) is bounded by `max_bytes`.
    main: ReaderLru,
    /// Accounted bytes in `window` (the rest of `total` is `main`).
    window_total: usize,
    /// Cache-owned bytes: every entry's accounted weight, full or demoted.
    total: usize,
    /// Bytes of full (non-demoted) entries only, for the hot budget.
    hot: usize,
    /// Entry count per tier, maintained alongside the byte totals so gauge
    /// publication stays O(1).
    full_entries: usize,
    metadata_entries: usize,
    demotions: usize,
    rejections: usize,
    /// Demand lookups so far (`get`, hit or miss): the clock that recency,
    /// reuse distance and victim age are measured on.
    lookups: u64,
    /// Non-resident files by key hash, LRU among themselves; bounded to
    /// `HISTORY_PER_ENTRY x len() + HISTORY_SLACK`. Lets a file rejected
    /// or evicted a moment ago prove its reuse distance when it is asked
    /// for again.
    history: LruCache<u64, History>,
}

impl CacheState {
    fn new() -> Self {
        Self {
            window: LruCache::new_unbounded(),
            main: LruCache::new_unbounded(),
            window_total: 0,
            total: 0,
            hot: 0,
            full_entries: 0,
            metadata_entries: 0,
            demotions: 0,
            rejections: 0,
            lookups: 0,
            history: LruCache::new_unbounded(),
        }
    }

    fn len(&self) -> usize {
        self.window.len() + self.main.len()
    }

    fn contains_key(&self, key: &ReaderCacheKey) -> bool {
        self.main.contains_key(key) || self.window.contains_key(key)
    }

    fn peek(&self, key: &ReaderCacheKey) -> Option<&CachedReader> {
        self.main.peek(key).or_else(|| self.window.peek(key))
    }

    fn peek_mut(&mut self, key: &ReaderCacheKey) -> Option<&mut CachedReader> {
        if self.main.contains_key(key) {
            self.main.peek_mut(key)
        } else {
            self.window.peek_mut(key)
        }
    }

    /// A demand access of a resident `key`: record its recency and reuse
    /// distance, and refresh its LRU position in main. The window is FIFO:
    /// a hit there does not extend the newcomer's residency, so every
    /// newcomer gets the same chance to show a reuse before it is spilled,
    /// and the one spilled is always the one that has had that chance the
    /// longest.
    fn touch(&mut self, key: &ReaderCacheKey) -> Option<&mut CachedReader> {
        let now = self.lookups;
        let entry = if self.main.contains_key(key) {
            self.main.get_mut(key)
        } else {
            self.window.peek_mut(key)
        }?;
        entry.reuse = Some(now - entry.last_lookup);
        entry.last_lookup = now;
        Some(entry)
    }

    /// A demand access of a NON-resident `key`: remember it, so the publish
    /// that follows (or a later one) knows its reuse distance.
    fn record_miss(&mut self, hash: u64) {
        let now = self.lookups;
        let reuse = self.history.get(&hash).map(|h| now - h.last_lookup);
        self.history.insert(
            hash,
            History {
                last_lookup: now,
                reuse,
            },
        );
        self.trim_history();
    }

    /// A resident leaves the cache (rejected, evicted or shed): keep its
    /// recency so a prompt re-request can prove its reuse distance.
    fn remember(&mut self, key: &ReaderCacheKey, entry: &CachedReader) {
        self.history.insert(
            key_hash(key),
            History {
                last_lookup: entry.last_lookup,
                reuse: entry.reuse,
            },
        );
        self.trim_history();
    }

    fn trim_history(&mut self) {
        let cap = HISTORY_PER_ENTRY * self.len() + HISTORY_SLACK;
        while self.history.len() > cap {
            self.history.remove_lru();
        }
    }

    /// Remove `key` from its segment, keeping every total consistent.
    fn remove(&mut self, key: &ReaderCacheKey) -> Option<CachedReader> {
        let (entry, in_window) = match self.main.remove(key) {
            Some(entry) => (entry, false),
            None => (self.window.remove(key)?, true),
        };
        self.forget(&entry, in_window);
        Some(entry)
    }

    /// Account the departure of `entry` (already detached from its segment).
    fn forget(&mut self, entry: &CachedReader, in_window: bool) {
        match entry.tier {
            Tier::Full => {
                self.hot -= entry.accounted;
                self.full_entries -= 1;
            }
            Tier::Metadata => self.metadata_entries -= 1,
        }
        self.total -= entry.accounted;
        if in_window {
            self.window_total -= entry.accounted;
        }
    }

    /// The eviction victim: the main LRU front, with its age in lookups
    /// (lookups since its last demand access).
    fn victim(&self) -> Option<(ReaderCacheKey, u64)> {
        self.main
            .iter()
            .next()
            .map(|(key, entry)| (key.clone(), self.lookups - entry.last_lookup))
    }

    fn update_gauges(&self) {
        metrics::VIX_READER_CACHE_ENTRIES
            .with_label_values::<&str>(&[])
            .set(self.len() as i64);
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
    /// Byte cap of the admission window: `max_bytes / WINDOW_FRACTION`,
    /// reserved out of `max_bytes` (see `main_bytes`).
    window_bytes: usize,
}

impl CacheInner {
    /// Demote LRU-first until the full tier fits the hot budget. Only a
    /// reader with no outstanding lease AND no outstanding handle can be
    /// demoted: `Arc::get_mut` proves sole strong AND weak ownership, which
    /// is exactly "no lease, no handle in flight" (a `ReaderHandle` handed
    /// out by `get` holds a Weak that would fail `get_mut`). A busy
    /// entry is skipped and the scan stops when no full entry is
    /// demotable; enforcement retries on the next mutation.
    ///
    /// Demotion also RESYNCS the entry's accounted bytes to the reader's
    /// exact size. Growth charges are high-water marks (a reader's own FIFO
    /// block-cache shrink publishes a smaller size that `memory_changed`
    /// must ignore for ordering safety), so subtracting only what `demote`
    /// released left ghost bytes behind every cycle: prod 2026-10-02, 18 h
    /// pods - 2.29 MB accounted per demoted reader vs ~0.84 MB real, 1,870
    /// entries filling a 4 GiB budget that fits ~3,600.
    fn demote_overflow(&self, state: &mut CacheState) {
        while state.hot > self.hot_bytes {
            let Some(key) = state
                .main
                .iter()
                .find(|(_, entry)| {
                    entry.tier == Tier::Full && Arc::strong_count(&entry.reader) == 1
                })
                .map(|(key, _)| key.clone())
            else {
                break;
            };
            let Some(entry) = state.main.peek_mut(&key) else {
                continue;
            };
            // Exclusive access: demote, then take the exact size. Without
            // it the entry still leaves the full tier (nothing releasable;
            // its weight simply stops counting against the hot budget).
            let exact = Arc::get_mut(&mut entry.reader).map(|reader| {
                reader.demote();
                reader.memory_size() + entry.observer.overhead
            });
            let before = entry.accounted;
            let after = exact.unwrap_or(before);
            state.hot -= before;
            entry.accounted = after;
            entry.tier = Tier::Metadata;
            state.total = state.total + after - before;
            state.full_entries -= 1;
            state.metadata_entries += 1;
            state.demotions += 1;
            metrics::VIX_READER_CACHE_DEMOTIONS_TOTAL
                .with_label_values::<&str>(&[])
                .inc();
        }
    }

    /// Bytes main (admitted readers, full or demoted) may hold: the budget
    /// less the window's RESERVATION less `extra`. Reserving the window is
    /// what keeps newcomers alive long enough to show a reuse: with the
    /// window merely capped, a full cache evicted every newcomer at the next
    /// growth callback (prod 2026-10-02, `.192`-`.195`: 2.84 M window
    /// evictions in 18 h against 3.18 M misses - 24 h dashboards at 1 %
    /// reader hits on every steady-state pod).
    fn main_bytes(&self, extra: usize) -> usize {
        self.max_bytes
            .saturating_sub(self.window_bytes)
            .saturating_sub(extra)
    }

    /// Enforce every budget after a mutation (a publish or a growth
    /// callback): demote the hot overflow, trim main LRU-first to its share,
    /// then spill the window's FIFO front through admission while the
    /// window is over its cap. Detached entries are returned for
    /// destruction after the state lock drops (never under it).
    fn enforce(&self, state: &mut CacheState, extra: usize) -> Vec<CachedReader> {
        let mut dropped = Vec::new();
        self.demote_overflow(state);
        while state.total.saturating_sub(state.window_total) > self.main_bytes(extra) {
            let Some((key, entry)) = state.main.remove_lru() else {
                break;
            };
            state.forget(&entry, false);
            state.remember(&key, &entry);
            metrics::VIX_READER_CACHE_EVICTIONS_TOTAL
                .with_label_values(&["overflow"])
                .inc();
            dropped.push(entry);
        }
        while state.window_total > self.window_bytes {
            let Some((candidate_key, candidate)) = state.window.remove_lru() else {
                break;
            };
            state.window_total -= candidate.accounted;
            self.spill(state, candidate_key, candidate, &mut dropped);
        }
        dropped
    }

    /// Insert a newcomer into the window and enforce the budgets. The
    /// window spills its FIFO front into the main cache under REUSE-DISTANCE
    /// admission (`spill`): a candidate displaces the main LRU victim only
    /// while the candidate's reuse distance (lookups between its two latest
    /// demand accesses - a window hit, or a re-request after a
    /// rejection/eviction still in `history`) is strictly shorter than the
    /// victim's age (lookups since its last access). That is LRU's own
    /// keep/evict judgement applied to the newcomer, so:
    /// - a file asked for once (a one-off scan's) has no reuse distance and never displaces
    ///   anything - it lives in the window only;
    /// - a scan repeated over more files than fit keeps a STABLE subset: on the next pass every
    ///   candidate's reuse distance is the whole pass, never shorter than the age of a resident the
    ///   pass has touched;
    /// - a file asked for again within a dashboard's refresh interval (new hourly files, a set
    ///   evicted by a scan) displaces residents nobody has asked for that long - exactly when plain
    ///   LRU would have kept it.
    ///
    /// Rejected candidates are returned for destruction after the lock drops.
    fn admit(
        &self,
        state: &mut CacheState,
        key: ReaderCacheKey,
        mut entry: CachedReader,
    ) -> Vec<CachedReader> {
        // Carry the non-resident history (the lookup that missed, and its
        // distance from the one before) into the entry.
        let now = state.lookups;
        match state.history.remove(&key_hash(&key)) {
            Some(history) => {
                entry.last_lookup = history.last_lookup;
                entry.reuse = history.reuse;
            }
            None => {
                entry.last_lookup = now;
                entry.reuse = None;
            }
        }
        let size = entry.accounted;
        state.total += size;
        state.hot += size;
        state.full_entries += 1;
        state.window_total += size;
        state.window.insert(key, entry);
        self.enforce(state, 0)
    }

    /// Admission of one window candidate into main (see `admit`). The
    /// candidate's bytes have left `window_total` but are still part of
    /// `total`, so main plus the candidate must fit `main_bytes` - the same
    /// line `enforce` trims to. (An admission line a hot tier below it, as
    /// shipped in `.191`-`.195`, left the top quarter of the budget unused
    /// in steady state: demotion already caps the hot tier, so a winner
    /// purged main down to `max - hot - window` and nothing refilled it.)
    fn spill(
        &self,
        state: &mut CacheState,
        candidate_key: ReaderCacheKey,
        candidate: CachedReader,
        dropped: &mut Vec<CachedReader>,
    ) {
        let mut admitted = true;
        while state.total.saturating_sub(state.window_total) > self.main_bytes(0) {
            let Some((victim_key, victim_age)) = state.victim() else {
                // nothing to compete with: a lone entry may use the
                // whole hard budget, more does not fit at all
                admitted = state.total.saturating_sub(state.window_total) <= self.max_bytes;
                break;
            };
            if candidate.reuse.is_some_and(|reuse| reuse < victim_age) {
                let victim = state.main.remove(&victim_key).expect("victim present");
                state.forget(&victim, false);
                state.remember(&victim_key, &victim);
                metrics::VIX_READER_CACHE_EVICTIONS_TOTAL
                    .with_label_values(&["admission"])
                    .inc();
                dropped.push(victim);
            } else {
                admitted = false;
                break;
            }
        }
        if admitted {
            state.main.insert(candidate_key, candidate);
        } else {
            state.forget(&candidate, false);
            state.remember(&candidate_key, &candidate);
            state.rejections += 1;
            metrics::VIX_READER_CACHE_REJECTIONS_TOTAL
                .with_label_values::<&str>(&[])
                .inc();
            dropped.push(candidate);
        }
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
        let Some(entry) = state.peek(&self.key) else {
            return;
        };
        if !std::ptr::eq(Arc::as_ptr(&entry.observer), self) {
            return;
        }
        let current = reader_bytes.checked_add(self.overhead);
        let Some(current) = current.filter(|size| *size <= cache.max_bytes) else {
            let entry = state.remove(&self.key).unwrap();
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
        // Growth lives in the hot tier: account it, shrink OTHER full readers
        // first (demotion releases their lazily built state, LRU-first), and
        // trim main only for what demotion could not absorb - never the
        // window, whose bytes are reserved (`CacheInner::main_bytes`).
        {
            let in_window = state.window.contains_key(&self.key);
            let entry = state.peek_mut(&self.key).unwrap();
            let demoted = entry.tier == Tier::Metadata;
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
            state.total = state.total.saturating_add(delta);
            if in_window {
                state.window_total = state.window_total.saturating_add(delta);
            }
        }
        // The growing entry itself holds a lease and is never demoted
        // underneath its user.
        evicted.extend(cache.enforce(&mut state, 0));
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

/// A size-bounded, scan-resistant cache of parsed readers keyed by immutable
/// sidecar identity: an admission window plus a main LRU guarded by
/// reuse-distance admission (`CacheInner::admit`), with a hot/metadata tier
/// split inside main.
///
/// The budget/gauges describe cache-owned reader weights plus entry metadata,
/// not process RSS: active Arc users may pin evicted readers until they finish.
/// Shared readers are conservatively charged once per admitted key, retaining
/// each admission's observed high-water weight even if reader storage shrinks.
/// Growth callbacks enforce the budget before returning; they never refresh LRU.
/// Get/put/notification bookkeeping is O(1), plus O(entries actually demoted,
/// evicted or rejected). Logical-file invalidation alone walks both segments
/// to find all generations.
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
                window_bytes: max_bytes / WINDOW_FRACTION,
            }),
        }
    }

    /// A DEMAND access: get a parsed reader, refreshing its LRU position
    /// and recency. Every lookup, hit or miss, advances the lookup clock
    /// that admission measures reuse distance and victim age on - so call
    /// it once per file per query; probes use [`Self::peek`].
    pub(super) fn get(&self, key: &ReaderCacheKey) -> Option<ReaderHandle> {
        let found = {
            let mut state = self.inner.state.lock();
            state.lookups += 1;
            let found = state.touch(key).map(|entry| ReaderHandle {
                reader: Arc::downgrade(&entry.reader),
                leases: Arc::clone(&entry.leases),
                has_index: entry.reader.has_index(),
            });
            if found.is_none() {
                state.record_miss(key_hash(key));
            }
            found
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

    /// Probe an immutable sidecar without refreshing LRU or hit/miss metrics.
    pub fn contains(&self, key: &ReaderCacheKey) -> bool {
        self.inner.state.lock().contains_key(key)
    }

    /// A NON-demand access (warming, planning probes): the handle if the
    /// reader is resident, without refreshing LRU, recency, the lookup
    /// clock or hit/miss metrics. A query's own use of the file is the one
    /// `get` that counts.
    pub(super) fn peek(&self, key: &ReaderCacheKey) -> Option<ReaderHandle> {
        self.inner.state.lock().peek(key).map(|entry| ReaderHandle {
            reader: Arc::downgrade(&entry.reader),
            leases: Arc::clone(&entry.leases),
            has_index: entry.reader.has_index(),
        })
    }

    /// Copy immutable ordering facts without pinning or exposing a reader;
    /// a planning probe, not a demand access.
    pub fn ordering(&self, key: &ReaderCacheKey) -> Option<(bool, Option<usize>, bool)> {
        self.inner.state.lock().peek(key).map(|entry| {
            (
                entry.reader.row_order().is_ts_desc(),
                entry.reader.ts_desc_row_ranges().map(|ranges| ranges.len()),
                entry.reader.zone_chunks().is_some(),
            )
        })
    }

    /// Publish an already operation-admitted cold reader. Duplicate opens
    /// stay private; they cannot mutate the winner through an escaping Arc.
    /// The reader may be REJECTED by admission (returned lease still valid):
    /// a one-off scan's files do not displace recently reused ones.
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
        if self.inner.max_bytes == 0 || self.inner.state.lock().contains_key(&key) {
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
        let dropped;
        {
            let mut state = self.inner.state.lock();
            // Another cold open may have published while we subscribed.
            if state.contains_key(&key) {
                return Ok(());
            }
            dropped = self.inner.admit(
                &mut state,
                key,
                CachedReader {
                    reader: Arc::clone(&reader),
                    leases: Arc::new(AtomicUsize::new(0)),
                    tier: Tier::Full,
                    accounted: size,
                    observer: Arc::clone(&observer),
                    last_lookup: 0,
                    reuse: None,
                },
            );
            state.update_gauges();
        }
        drop(dropped);
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
            .main
            .iter()
            .chain(state.window.iter())
            .filter(|(key, _)| key.file() == file)
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in doomed {
            if let Some(entry) = state.remove(&key) {
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
        self.inner.state.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.state.lock().len() == 0
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
            let observer = Arc::clone(&cache.inner.state.lock().peek(&first).unwrap().observer);
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

        // room for two entries (the window reservation keeps a third out)
        let cache = VixReaderCache::new(entry_size * 5 / 2);
        // production shape: a lookup (miss) precedes every publish
        for i in 0..2 {
            assert!(cache.get(&key(&format!("file-{i}"))).is_none());
            cache.put_fixture(key(&format!("file-{i}")), Arc::clone(&reader));
        }
        // A newcomer asked for once has no reuse distance and is REJECTED:
        // one-off scans never churn the cache.
        assert!(cache.get(&key("file-2")).is_none());
        cache.put_fixture(key("file-2"), Arc::clone(&reader));
        assert!(cache.contains(&key("file-0")));
        assert!(
            !cache.contains(&key("file-2")),
            "a single lookup does not displace"
        );
        assert_eq!(cache.inner.state.lock().rejections, 1);
        // Asked for again (a reuse distance shorter than the untouched
        // victim's age), it evicts the oldest entry to fit.
        assert!(cache.get(&key("file-2")).is_none());
        cache.put_fixture(key("file-2"), Arc::clone(&reader));
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
        let observer = Arc::clone(&cache.inner.state.lock().peek(&key).unwrap().observer);
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
    /// Scan resistance: a repeated sequential scan over a working set 1.6x
    /// the cache (prod: a 7 d query over ~6,900 files per follower against
    /// ~4,200 cached readers) keeps a STABLE subset under reuse-distance
    /// admission.
    /// Plain LRU gets 0 hits on every pass after the first (each miss evicts
    /// the entry the scan needs next); here the second and third passes hit
    /// the admitted subset, and entries looked up often (a dashboard's
    /// files) displace one-time scan files.
    #[test]
    fn repeated_scan_larger_than_the_cache_keeps_a_stable_subset() {
        let reader = small_reader();
        let key = |i: usize| ReaderCacheKey::new(format!("scan-{i}"), 7, 100);
        let entry_size = reader.memory_size() + entry_overhead(&key(0));
        let capacity = 40usize;
        let working_set = 64usize;
        let cache = VixReaderCache::new(entry_size * capacity);
        // every evaluation: lookup, and publish on a miss
        let pass = |cache: &VixReaderCache| -> usize {
            let mut hits = 0;
            for i in 0..working_set {
                if cache.get(&key(i)).is_some() {
                    hits += 1;
                } else {
                    cache.put_fixture(key(i), Arc::clone(&reader));
                }
            }
            hits
        };
        assert_eq!(pass(&cache), 0, "cold pass");
        let second = pass(&cache);
        let third = pass(&cache);
        // The admitted subset (main's share: capacity less the window
        // reservation) hits on every later pass and the hit set does not
        // shrink.
        let admission = capacity - capacity / WINDOW_FRACTION;
        assert!(
            second * 10 >= admission * 8,
            "second pass must hit most of the admission budget: {second} of {admission}"
        );
        assert!(
            third >= second,
            "the stable subset must not erode: {third} < {second}"
        );
        assert!(cache.memory_size() <= entry_size * capacity);

        // A dashboard panel's file, asked for again a few lookups later
        // (a reuse distance far shorter than the scan's), earns admission
        // over the scan's files; refreshed during the next pass, it is
        // never the scan's victim.
        let hot = ReaderCacheKey::new("dashboard-file".to_string(), 7, 100);
        assert!(cache.get(&hot).is_none());
        for i in 0..4 {
            assert!(cache.get(&key(i)).is_some());
        }
        assert!(cache.get(&hot).is_none());
        cache.put_fixture(hot.clone(), Arc::clone(&reader));
        assert!(
            cache.contains(&hot),
            "a re-requested newcomer must be admitted"
        );
        for i in 0..working_set {
            if i % 16 == 8 {
                assert!(
                    cache.get(&hot).is_some(),
                    "a refreshed entry must survive a scan"
                );
            }
            if cache.get(&key(i)).is_none() {
                cache.put_fixture(key(i), Arc::clone(&reader));
            }
        }
        assert!(cache.get(&hot).is_some());
    }

    /// The production shape the policy exists for: dashboards over the
    /// recent day's files (a few replaced by merge output between queries),
    /// 7 d scans over a superset too large to fit, dashboards again.
    /// Recent files must survive scans, and a brand-new file must cost
    /// exactly one miss - never a run of rejections.
    #[test]
    fn dashboards_survive_scans_and_new_files_cost_one_miss() {
        let reader = small_reader();
        let key = |i: usize| ReaderCacheKey::new(format!("f-{i}"), 7, 100);
        let entry_size = reader.memory_size() + entry_overhead(&key(0));
        // main's share 56 entries, window 3; recent 12 of a 60-file scan
        let cache = VixReaderCache::new(entry_size * 60);
        let mut next_id = 1_000_000usize;
        let mut recent: std::collections::VecDeque<usize> = (0..12)
            .map(|_| {
                next_id += 1;
                next_id
            })
            .collect();
        let old: Vec<usize> = (0..48).collect();
        let mut seen = std::collections::HashSet::new();
        // lookup + publish-on-miss: (hits, misses on files seen before, misses on new files)
        let mut pass = |cache: &VixReaderCache, files: &[usize]| -> (usize, usize, usize) {
            let (mut hits, mut repeat_misses, mut new_misses) = (0, 0, 0);
            for &i in files {
                if cache.get(&key(i)).is_some() {
                    hits += 1;
                } else {
                    if seen.insert(i) {
                        new_misses += 1;
                    } else {
                        repeat_misses += 1;
                    }
                    cache.put_fixture(key(i), Arc::clone(&reader));
                }
            }
            (hits, repeat_misses, new_misses)
        };
        let dashboards = |recent: &std::collections::VecDeque<usize>| -> Vec<usize> {
            recent.iter().rev().copied().collect()
        };
        let scan = |recent: &std::collections::VecDeque<usize>| -> Vec<usize> {
            let mut files = dashboards(recent);
            files.extend(old.iter().rev());
            files
        };
        let mut turnover = |recent: &mut std::collections::VecDeque<usize>| {
            recent.pop_front();
            next_id += 1;
            recent.push_back(next_id);
        };

        for i in 0..3 {
            if i > 0 {
                turnover(&mut recent);
            }
            pass(&cache, &dashboards(&recent));
        }
        // Repeated scans keep a stable subset that includes the recent files.
        let mut previous = 0;
        for i in 0..4 {
            let (hits, ..) = pass(&cache, &scan(&recent));
            assert!(
                hits >= previous,
                "scan {i}: stable subset eroded {previous} -> {hits}"
            );
            assert!(hits >= recent.len(), "scan {i}: recent files lost");
            previous = hits;
        }
        assert!(
            previous >= 45,
            "stable subset too small: {previous} of 56 admitted"
        );
        // Dashboards after the scans: only the one new file per query misses.
        for i in 0..6 {
            turnover(&mut recent);
            let (hits, repeat_misses, new_misses) = pass(&cache, &dashboards(&recent));
            assert_eq!(
                (hits, repeat_misses, new_misses),
                (recent.len() - 1, 0, 1),
                "dashboard query {i} after scans"
            );
        }
        // One more scan, then dashboards: the recent set is still resident.
        pass(&cache, &scan(&recent));
        let (hits, repeat_misses, new_misses) = pass(&cache, &dashboards(&recent));
        assert_eq!((hits, repeat_misses, new_misses), (recent.len(), 0, 0));
        assert!(cache.memory_size() <= entry_size * 60);
    }

    /// A FULL cache must not evict newcomers to absorb a cached reader's
    /// growth: the window's bytes are reserved. Prod 2026-10-02 (`.192`-
    /// `.195`): every pod pinned at its budget evicted each newcomer at the
    /// next growth callback - 24 h dashboards at 1 % reader hits, 2.84 M
    /// window evictions against 3.18 M misses in 18 h.
    #[test]
    fn full_cache_growth_never_evicts_the_window() {
        let reader = small_reader();
        let key = |i: usize| ReaderCacheKey::new(format!("f-{i}"), 7, 100);
        let entry_size = reader.memory_size() + entry_overhead(&key(0));
        // 32 entries: window 2, main's share 30
        let cache = VixReaderCache::new(entry_size * 32);
        for i in 0..24 {
            assert!(cache.get(&key(i)).is_none());
            cache.put_fixture(key(i), Arc::clone(&reader));
        }
        assert_eq!(cache.len(), 24);
        // Grow an admitted reader until the cache sits at its hard budget
        // (shared readers are never demotable, so nothing absorbs growth).
        let grow = |file: usize, by: usize| {
            let observer = Arc::clone(&cache.inner.state.lock().peek(&key(file)).unwrap().observer);
            observer.memory_changed(reader.memory_size() + by);
        };
        grow(5, entry_size * 8);
        // at the budget: main trimmed to its share, the window still full
        assert!(cache.memory_size() > entry_size * 30);
        assert!(cache.memory_size() <= entry_size * 32);

        // A brand-new file is published (its first lookup missed) ...
        assert!(cache.get(&key(100)).is_none());
        cache.put_fixture(key(100), Arc::clone(&reader));
        // ... and another cached reader grows.
        grow(6, entry_size);
        assert!(
            cache.contains(&key(100)),
            "newcomer evicted for another reader's growth"
        );
        assert!(cache.get(&key(100)).is_some(), "window hit");
        assert!(cache.memory_size() <= entry_size * 32);
        // Asked for again within the window, it is admitted at spill over
        // residents nobody has asked for since.
        for i in 101..103 {
            assert!(cache.get(&key(i)).is_none());
            cache.put_fixture(key(i), Arc::clone(&reader));
        }
        assert!(cache.inner.state.lock().main.contains_key(&key(100)));
        assert!(cache.memory_size() <= entry_size * 32);
    }

    /// Demotion resyncs the accounted bytes to the reader's exact size:
    /// growth charges are high-water marks, so an entry that grew and then
    /// shrank inside the reader must not keep ghost bytes after demotion
    /// (prod 2026-10-02: 2.29 MB accounted per demoted reader vs ~0.84 MB).
    #[test]
    fn demotion_resyncs_accounted_bytes_to_the_reader() {
        let fresh = || {
            let (data, index) = reader_files(["a", "b"]);
            VixReader::open_with_index(bytes::Bytes::from(data), index.map(bytes::Bytes::from))
                .unwrap()
        };
        let key = |file: &str| ReaderCacheKey::new(file.to_string(), 7, 100);
        let probe = fresh();
        let entry_size = probe.memory_size() + entry_overhead(&key("file-0"));
        drop(probe);
        // 16 entries: window 1, hot budget 4
        let cache = VixReaderCache::new(entry_size * 16);
        cache.put(key("file-0"), fresh()).unwrap();
        // a second publish spills file-0 into main, where demotion applies
        cache.put(key("file-1"), fresh()).unwrap();
        assert!(cache.inner.state.lock().main.contains_key(&key("file-0")));
        // A growth charge of six entries that the reader does not hold any
        // more by the time the hot budget demotes it.
        let (observer, grown) = {
            let state = cache.inner.state.lock();
            let entry = state.peek(&key("file-0")).unwrap();
            (
                Arc::clone(&entry.observer),
                entry.reader.memory_size() + entry_size * 6,
            )
        };
        observer.memory_changed(grown);
        let state = cache.inner.state.lock();
        let entry = state.peek(&key("file-0")).unwrap();
        assert_eq!(entry.tier, Tier::Metadata, "hot overflow must demote");
        assert_eq!(
            entry.accounted,
            entry.reader.memory_size() + entry_overhead(&key("file-0")),
            "accounted bytes must equal the reader's exact size after demotion"
        );
        let other = state.peek(&key("file-1")).unwrap();
        assert_eq!(state.total, entry.accounted + other.accounted);
        assert_eq!(state.hot, other.accounted);
    }

    /// Newcomers get a brief residency in the window even before they are
    /// frequent: a burst of panels over the same new files hits it.
    #[test]
    fn window_serves_bursts_over_new_files_before_admission() {
        let reader = small_reader();
        let key = |i: usize| ReaderCacheKey::new(format!("w-{i}"), 7, 100);
        let entry_size = reader.memory_size() + entry_overhead(&key(0));
        // window cap = 1/16 of 64 entries = 4 entries
        let cache = VixReaderCache::new(entry_size * 64);
        // fill main with one-time files so admission is contested
        for i in 100..164 {
            assert!(cache.get(&key(i)).is_none());
            cache.put_fixture(key(i), Arc::clone(&reader));
        }
        // panel 1 publishes two new files; panel 2 re-reads them at once
        for i in 0..2 {
            assert!(cache.get(&key(i)).is_none());
            cache.put_fixture(key(i), Arc::clone(&reader));
        }
        for i in 0..2 {
            assert!(
                cache.get(&key(i)).is_some(),
                "window hit for a fresh newcomer"
            );
        }
        assert!(cache.memory_size() <= entry_size * 64);
    }

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
        // Total budget fits 8 entries; the hot budget (1/4) fits 2; main's
        // share (less the window reservation) fits 7. Production shape: a
        // lookup precedes each publish.
        let cache = VixReaderCache::new(entry_size * 8);
        for i in 0..5 {
            assert!(cache.get(&key(&format!("file-{i}"))).is_none());
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

        // Past main's share, newcomers asked for once (no reuse distance)
        // are rejected rather than churning the cache; the budget holds
        // either way.
        for i in 5..12 {
            assert!(cache.get(&key(&format!("file-{i}"))).is_none());
            cache.put(key(&format!("file-{i}")), fresh()).unwrap();
        }
        assert!(cache.len() <= 7, "main's share must bound the cache");
        assert!(cache.inner.state.lock().rejections >= 5);
        assert!(cache.memory_size() <= entry_size * 8);
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
        let leased = state.peek(&key("file-0")).unwrap();
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
            cache.inner.state.lock().peek(&key("file-0")).unwrap().tier,
            Tier::Metadata
        );
    }

    #[test]
    fn get_refreshes_lru_order() {
        let reader = small_reader();
        let key = |file: &str| ReaderCacheKey::new(file.to_string(), 7, 100);
        let entry_size = reader.memory_size() + entry_overhead(&key("file-0"));
        let cache = VixReaderCache::new(entry_size * 5 / 2);

        cache.put_fixture(key("file-0"), Arc::clone(&reader));
        cache.put_fixture(key("file-1"), Arc::clone(&reader));
        // touch file-0: it becomes the most recently used
        assert!(cache.get(&key("file-0")).is_some());
        // a third entry, asked for twice in a row (a short reuse distance),
        // evicts the LRU victim file-1, NOT the refreshed file-0
        for _ in 0..2 {
            assert!(cache.get(&key("file-2")).is_none());
        }
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
                        let other_key =
                            ReaderCacheKey::new("other-file".to_string(), generation as i64, 100);
                        // a reuse distance shorter than the victim's age
                        for _ in 0..2 {
                            assert!(cache.get(&other_key).is_none());
                        }
                        cache.put_fixture(other_key, Arc::clone(&other));
                    }
                }
                "growth" => {
                    let observer =
                        Arc::clone(&cache.inner.state.lock().peek(&key).unwrap().observer);
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
