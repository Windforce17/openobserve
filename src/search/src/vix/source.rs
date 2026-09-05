// Copyright 2026 OpenObserve Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

//! Executor-agnostic range sources. Cached sources contain only immutable
//! object identity; ephemeral reads capture the calling operation before Vortex
//! hands them to its IO executor. Admission wait and active IO timeout are separate.

use std::{
    cell::RefCell,
    ops::Range,
    sync::{
        Arc, LazyLock, Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
use futures::{FutureExt, StreamExt, TryStreamExt, future::BoxFuture};
use infra::cache::storage::{RangeRead, RangeSource};
use object_store::{ObjectStore, path::Path};
use parking_lot::Mutex;
use tokio::{
    runtime::Handle,
    sync::{Notify, OwnedSemaphorePermit, Semaphore, watch},
};
use vortex_index::VixRangeSource;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VixReadMode {
    Cached,
    Ranged,
}

pub fn vix_read_mode() -> VixReadMode {
    match config::get_config().common.vix_read_mode.as_str() {
        "cached" => VixReadMode::Cached,
        _ => VixReadMode::Ranged,
    }
}

/// Logical counters count successfully returned ranges, including cache hits.
/// Physical counters count completed coalesced reads (including gap bytes), not
/// HTTP attempts/retries. Unknown ObjectStore implementations are never called remote.
#[derive(Debug, Default)]
pub struct FetchStats {
    pub fetches: AtomicU64,
    pub bytes: AtomicU64,
    pub batches: AtomicU64,
    pub physical_fetches: AtomicU64,
    pub physical_bytes: AtomicU64,
    pub memory_fetches: AtomicU64,
    pub memory_bytes: AtomicU64,
    pub disk_fetches: AtomicU64,
    pub disk_bytes: AtomicU64,
    pub remote_fetches: AtomicU64,
    pub remote_bytes: AtomicU64,
    pub object_store_fetches: AtomicU64,
    pub object_store_bytes: AtomicU64,
    pub queue_micros: AtomicU64,
    pub active_micros: AtomicU64,
    pub evaluation_queue_micros: AtomicU64,
}

thread_local! {
    static OPERATION: RefCell<Option<Arc<ReadOperation>>> = const { RefCell::new(None) };
}

pub(super) struct ReadOperation {
    stats: Arc<FetchStats>,
    rollup: Option<Arc<FetchStats>>,
    stopped: watch::Sender<bool>,
    deadline: Option<Instant>,
    evaluation: Option<Weak<EvaluationMemory>>,
}

impl ReadOperation {
    pub(super) fn new(stats: Arc<FetchStats>, deadline: Option<Instant>) -> Arc<Self> {
        let (stopped, _) = watch::channel(false);
        Arc::new(Self {
            stats,
            rollup: None,
            stopped,
            deadline,
            evaluation: None,
        })
    }

    /// A completed file's cost can be sampled independently of unfinished
    /// lookahead, while every read still contributes once to query totals.
    pub(super) fn for_file(self: &Arc<Self>, stats: Arc<FetchStats>) -> Arc<Self> {
        let root = self.rollup.as_ref().unwrap_or(&self.stats);
        Arc::new(Self {
            rollup: (!Arc::ptr_eq(root, &stats)).then(|| Arc::clone(root)),
            stats,
            stopped: self.stopped.clone(),
            deadline: self.deadline,
            evaluation: None,
        })
    }

    fn record(&self, update: impl Fn(&FetchStats)) {
        update(&self.stats);
        if let Some(rollup) = &self.rollup {
            update(rollup);
        }
    }

    pub(super) fn owner_guard(self: &Arc<Self>) -> ReadOperationGuard {
        ReadOperationGuard(Arc::clone(self))
    }

    pub(super) fn run<T>(self: &Arc<Self>, work: impl FnOnce() -> T) -> T {
        struct Restore(Option<Arc<ReadOperation>>);
        impl Drop for Restore {
            fn drop(&mut self) {
                OPERATION.with(|slot| *slot.borrow_mut() = self.0.take());
            }
        }
        let _restore = Restore(OPERATION.with(|slot| slot.replace(Some(Arc::clone(self)))));
        vortex_index::with_read_operation(
            Arc::clone(self) as Arc<dyn vortex_index::VixReadOperation>,
            work,
        )
    }

    fn for_evaluation(self: &Arc<Self>, permit: &EvaluationPermit) -> Arc<Self> {
        Arc::new(Self {
            stats: Arc::clone(&self.stats),
            rollup: self.rollup.clone(),
            stopped: self.stopped.clone(),
            deadline: self.deadline,
            evaluation: Some(Arc::downgrade(&permit.memory)),
        })
    }

    pub(super) fn run_evaluation<T>(
        self: &Arc<Self>,
        permit: &EvaluationPermit,
        work: impl FnOnce() -> T,
    ) -> T {
        self.for_evaluation(permit).run(work)
    }

    pub(super) fn cancel(&self) {
        self.stopped.send_replace(true);
    }

    pub(super) fn is_cancelled(&self) -> bool {
        *self.stopped.borrow()
            || self
                .deadline
                .is_some_and(|deadline| Instant::now() >= deadline)
    }

    pub(super) async fn cancelled(&self) {
        let mut stopped = self.stopped.subscribe();
        let signal = async {
            loop {
                if *stopped.borrow_and_update() {
                    return;
                }
                if stopped.changed().await.is_err() {
                    return;
                }
            }
        };
        match self.deadline {
            Some(deadline) => tokio::select! {
                _ = signal => {},
                _ = tokio::time::sleep_until(deadline.into()) => {},
            },
            None => signal.await,
        }
    }
}

impl vortex_index::VixReadOperation for ReadOperation {
    fn is_cancelled(&self) -> bool {
        self.is_cancelled()
    }
    fn check_memory(&self, owned_bytes: usize) -> vortex_index::Result<()> {
        if self.is_cancelled() {
            return Err(vortex_index::VixError::Cancelled);
        }
        if let Some(memory) = self.evaluation.as_ref().and_then(Weak::upgrade) {
            memory
                .check(owned_bytes)
                .map_err(vortex_index::VixError::Callback)?;
        }
        Ok(())
    }
}

pub(super) struct ReadOperationGuard(Arc<ReadOperation>);
impl Drop for ReadOperationGuard {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

fn current_operation() -> Option<Arc<ReadOperation>> {
    OPERATION.with(|slot| slot.borrow().clone())
}

fn cancelled_error() -> anyhow::Error {
    vortex_index::VixError::Cancelled.into()
}

async fn cancelled(operation: Option<&Arc<ReadOperation>>) {
    match operation {
        Some(operation) => operation.cancelled().await,
        None => futures::future::pending().await,
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct FetchBudgetExceeded {
    requested: usize,
    budget: usize,
}
impl std::fmt::Display for FetchBudgetExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "VIX working byte reservation {} exceeds process ceiling {}",
            self.requested, self.budget
        )
    }
}
impl std::error::Error for FetchBudgetExceeded {}

/// Exact-byte reservations, without rounding or the u32 limit of acquire_many.
struct ByteGate {
    limit: usize,
    used: Mutex<usize>,
    changed: Notify,
    waiters: tokio::sync::Mutex<()>,
}
impl ByteGate {
    fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            limit: limit.max(1),
            used: Mutex::new(0),
            changed: Notify::new(),
            waiters: tokio::sync::Mutex::new(()),
        })
    }
    fn try_acquire(self: &Arc<Self>, bytes: usize) -> Option<BytePermit> {
        // Optional background work must not jump ahead of queued foreground work.
        let _queue = self.waiters.try_lock().ok()?;
        let mut used = self.used.lock();
        if bytes > self.limit - *used {
            return None;
        }
        *used += bytes;
        Some(BytePermit {
            gate: Arc::clone(self),
            bytes,
        })
    }
    async fn acquire(self: &Arc<Self>, bytes: usize) -> anyhow::Result<BytePermit> {
        if bytes > self.limit {
            return Err(FetchBudgetExceeded {
                requested: bytes,
                budget: self.limit,
            }
            .into());
        }
        let _queue = self.waiters.lock().await;
        loop {
            // Register before checking capacity; notify_waiters cannot be lost.
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let mut used = self.used.lock();
                if bytes <= self.limit - *used {
                    *used += bytes;
                    return Ok(BytePermit {
                        gate: Arc::clone(self),
                        bytes,
                    });
                }
            }
            changed.await;
        }
    }
}
struct BytePermit {
    gate: Arc<ByteGate>,
    bytes: usize,
}
impl BytePermit {
    /// Atomic, nonblocking high-water growth. Never wait while holding a lease:
    /// two admitted readers must not deadlock trying to upgrade each other.
    fn try_resize(&mut self, bytes: usize) -> anyhow::Result<()> {
        if bytes <= self.bytes {
            return Ok(());
        }
        let delta = bytes - self.bytes;
        let mut used = self.gate.used.lock();
        if delta > self.gate.limit - *used {
            return Err(FetchBudgetExceeded {
                requested: bytes,
                budget: self.gate.limit,
            }
            .into());
        }
        *used += delta;
        self.bytes = bytes;
        Ok(())
    }
}
impl Drop for BytePermit {
    fn drop(&mut self) {
        *self.gate.used.lock() -= self.bytes;
        self.gate.changed.notify_waiters();
    }
}

static FETCH_BYTES: LazyLock<Arc<ByteGate>> =
    LazyLock::new(|| ByteGate::new(config::get_config().common.vix_fetch_max_bytes));
static FETCH_COUNT: LazyLock<Option<Arc<Semaphore>>> = LazyLock::new(|| {
    let count = config::get_config().common.vix_fetch_concurrency;
    (count > 0).then(|| Arc::new(Semaphore::new(count)))
});
static EVAL_BYTES: LazyLock<Arc<ByteGate>> =
    LazyLock::new(|| ByteGate::new(evaluation_byte_budget()));
static EVAL_COUNT: LazyLock<Arc<Semaphore>> = LazyLock::new(|| {
    Arc::new(Semaphore::new(
        config::get_config().limit.vix_search_concurrency.max(1),
    ))
});

pub(super) fn evaluation_byte_budget() -> usize {
    config::get_config().common.vix_eval_max_bytes.max(1)
}

/// Only actual blocking work owns this permit. Operation/source clones hold a
/// weak admission handle, so delayed IO cannot retain a completed eval's charge.
pub(super) struct EvaluationPermit {
    _count: OwnedSemaphorePermit,
    memory: Arc<EvaluationMemory>,
}

struct EvaluationMemory {
    workspace: usize,
    bytes: Mutex<BytePermit>,
    refusal: Mutex<Option<FetchBudgetExceeded>>,
}

impl EvaluationMemory {
    fn check(&self, owned_bytes: usize) -> anyhow::Result<()> {
        let mut refusal = self.refusal.lock();
        if let Some(error) = *refusal {
            return Err(error.into());
        }
        let mut bytes = self.bytes.lock();
        let result = self
            .workspace
            .checked_add(owned_bytes)
            .ok_or_else(|| {
                anyhow::Error::from(FetchBudgetExceeded {
                    requested: usize::MAX,
                    budget: bytes.gate.limit,
                })
            })
            .and_then(|required| bytes.try_resize(required));
        if let Err(error) = &result {
            *refusal = error.downcast_ref::<FetchBudgetExceeded>().copied();
        }
        result
    }
}

impl EvaluationPermit {
    pub(super) fn reserve_owned(&self, owned_bytes: usize) -> anyhow::Result<()> {
        self.memory.check(owned_bytes)
    }

    pub(super) fn check_refusal(&self) -> anyhow::Result<()> {
        match *self.memory.refusal.lock() {
            Some(error) => Err(vortex_index::VixError::Callback(error.into()).into()),
            None => Ok(()),
        }
    }
}

pub(super) fn try_acquire_evaluation(bytes: usize) -> Option<EvaluationPermit> {
    let count = Arc::clone(&EVAL_COUNT).try_acquire_owned().ok()?;
    let permit = EVAL_BYTES.try_acquire(bytes)?;
    Some(EvaluationPermit {
        _count: count,
        memory: Arc::new(EvaluationMemory {
            workspace: bytes,
            bytes: Mutex::new(permit),
            refusal: Mutex::new(None),
        }),
    })
}

pub(super) async fn acquire_evaluation(
    operation: &Arc<ReadOperation>,
    bytes: usize,
) -> anyhow::Result<EvaluationPermit> {
    let start = Instant::now();
    if bytes > EVAL_BYTES.limit {
        return Err(FetchBudgetExceeded {
            requested: bytes,
            budget: EVAL_BYTES.limit,
        }
        .into());
    }
    let result = tokio::select! {
        biased;
        _ = operation.cancelled() => Err(cancelled_error()),
        result = async {
            let count = Arc::clone(&EVAL_COUNT).acquire_owned().await?;
            let permit = EVAL_BYTES.acquire(bytes).await?;
            Ok(EvaluationPermit { _count: count, memory: Arc::new(EvaluationMemory {
                workspace: bytes, bytes: Mutex::new(permit), refusal: Mutex::new(None),
            }) })
        } => result,
    };
    let elapsed = micros(start);
    operation.record(|stats| {
        stats
            .evaluation_queue_micros
            .fetch_add(elapsed, Ordering::Relaxed);
    });
    result
}

fn micros(start: Instant) -> u64 {
    start.elapsed().as_micros().min(u64::MAX as u128) as u64
}

fn record_physical(path: &'static str, operation: Option<&Arc<ReadOperation>>, read: &RangeRead) {
    let bytes = read.bytes.len() as u64;
    config::metrics::VIX_PHYSICAL_READS_TOTAL
        .with_label_values(&[path, read.source.as_str()])
        .inc();
    config::metrics::VIX_PHYSICAL_BYTES_TOTAL
        .with_label_values(&[path, read.source.as_str()])
        .inc_by(bytes);
    if let Some(operation) = operation {
        operation.record(|stats| {
            stats.physical_fetches.fetch_add(1, Ordering::Relaxed);
            stats.physical_bytes.fetch_add(bytes, Ordering::Relaxed);
            let (count, size) = match read.source {
                RangeSource::Memory => (&stats.memory_fetches, &stats.memory_bytes),
                RangeSource::Disk => (&stats.disk_fetches, &stats.disk_bytes),
                RangeSource::Remote => (&stats.remote_fetches, &stats.remote_bytes),
                RangeSource::ObjectStore => {
                    (&stats.object_store_fetches, &stats.object_store_bytes)
                }
            };
            count.fetch_add(1, Ordering::Relaxed);
            size.fetch_add(bytes, Ordering::Relaxed);
        });
    }
}

struct FetchTimer {
    start: Instant,
    path: &'static str,
    operation: Option<Arc<ReadOperation>>,
    active: bool,
}
impl Drop for FetchTimer {
    fn drop(&mut self) {
        let elapsed = micros(self.start);
        let phase = if self.active { "active" } else { "queue" };
        config::metrics::VIX_FETCH_TIME_MICROS_TOTAL
            .with_label_values(&[self.path, phase])
            .inc_by(elapsed);
        if let Some(operation) = &self.operation {
            operation.record(|stats| {
                let counter = if self.active {
                    &stats.active_micros
                } else {
                    &stats.queue_micros
                };
                counter.fetch_add(elapsed, Ordering::Relaxed);
            });
        }
    }
}

/// Coalesce using the object_store gap policy, but never create a physical
/// request larger than the process byte ceiling. The resulting total is the
/// reservation: slices retain their coalesced owner, including gap bytes.
fn plan_ranges(
    ranges: &[Range<u64>],
    size: u64,
    budget: usize,
) -> anyhow::Result<(Vec<Range<u64>>, usize)> {
    // Bound bookkeeping as well as payload. Duplicated/empty ranges must not
    // admit an unbounded Vec of Bytes behind a tiny coalesced byte total.
    let overhead = ranges
        .len()
        .checked_mul(2 * std::mem::size_of::<Bytes>() + 3 * std::mem::size_of::<Range<u64>>())
        .unwrap_or(usize::MAX);
    if overhead > budget {
        return Err(FetchBudgetExceeded {
            requested: overhead,
            budget,
        }
        .into());
    }
    let mut sorted = ranges.to_vec();
    for range in &sorted {
        anyhow::ensure!(
            range.start <= range.end && range.end <= size,
            "invalid VIX range {range:?} for size {size}"
        );
    }
    sorted.retain(|range| range.start != range.end);
    sorted.sort_unstable_by_key(|range| range.start);
    let mut physical: Vec<Range<u64>> = Vec::with_capacity(sorted.len());
    for range in sorted {
        if let Some(last) = physical.last_mut() {
            let end = last.end.max(range.end);
            if range.start <= last.end {
                last.end = end;
                continue;
            }
        }
        physical.push(range);
    }
    let mut bytes = physical
        .iter()
        .try_fold(overhead, |total, range| {
            usize::try_from(range.end - range.start)
                .ok()
                .and_then(|size| total.checked_add(size))
        })
        .ok_or_else(|| anyhow::anyhow!("VIX range byte sum overflows address space"))?;
    if bytes > budget {
        return Err(FetchBudgetExceeded {
            requested: bytes,
            budget,
        }
        .into());
    }
    // Spend only spare budget on coalescing gaps. A batch whose payload fits
    // must split backend requests rather than fail because optional gaps do not.
    let mut coalesced: Vec<Range<u64>> = Vec::with_capacity(physical.len());
    for range in physical {
        if let Some(last) = coalesced.last_mut() {
            let gap = range.start - last.end;
            if gap <= object_store::OBJECT_STORE_COALESCE_DEFAULT && gap <= (budget - bytes) as u64
            {
                last.end = range.end;
                bytes += gap as usize;
                continue;
            }
        }
        coalesced.push(range);
    }
    Ok((coalesced, bytes))
}

#[derive(Clone)]
enum Backend {
    Ladder {
        account: String,
        location: Path,
    },
    Store {
        store: Arc<dyn ObjectStore>,
        location: Path,
    },
}
impl Backend {
    async fn fetch(
        &self,
        range: Range<u64>,
        owner: Arc<dyn Send + Sync>,
    ) -> anyhow::Result<RangeRead> {
        let max_bytes = range.end - range.start;
        Ok(match self {
            Self::Ladder { account, location } => {
                infra::cache::storage::get_range_classified(account, location, range, Some(owner))
                    .await?
            }
            Self::Store { store, location } => RangeRead {
                bytes: infra::cache::storage::range_bytes_with_owner(
                    store
                        .get_opts(
                            location,
                            object_store::GetOptions {
                                range: Some(range.into()),
                                ..Default::default()
                            },
                        )
                        .await?,
                    Some(owner),
                    max_bytes,
                )
                .await?,
                source: RangeSource::ObjectStore,
            },
        })
    }
}

struct ActiveFetch {
    _count: Option<OwnedSemaphorePermit>,
    _bytes: Arc<BytePermit>,
    _timer: FetchTimer,
}

async fn physical_fetch(
    backend: &Backend,
    range: Range<u64>,
    path: &'static str,
    operation: Option<&Arc<ReadOperation>>,
    reservation: Arc<BytePermit>,
) -> anyhow::Result<Bytes> {
    let queue = FetchTimer {
        start: Instant::now(),
        path,
        operation: operation.cloned(),
        active: false,
    };
    let count = match FETCH_COUNT.as_ref() {
        Some(gate) => Some(Arc::clone(gate).acquire_owned().await?),
        None => None,
    };
    drop(queue);
    let active: Arc<dyn Send + Sync> = Arc::new(ActiveFetch {
        _count: count,
        _bytes: reservation,
        _timer: FetchTimer {
            start: Instant::now(),
            path,
            operation: operation.cloned(),
            active: true,
        },
    });
    let expected = range.end - range.start;
    let timeout = config::get_config().limit.vix_fetch_timeout;
    let read = if timeout == 0 {
        backend.fetch(range, active).await?
    } else {
        tokio::time::timeout(Duration::from_secs(timeout), backend.fetch(range, active))
            .await
            .map_err(|_| {
                anyhow::anyhow!("vix range fetch timed out after {timeout}s (ZO_VIX_FETCH_TIMEOUT)")
            })??
    };
    anyhow::ensure!(
        read.bytes.len() as u64 == expected,
        "short VIX range response: expected {expected}, received {}",
        read.bytes.len()
    );
    record_physical(path, operation, &read);
    Ok(read.bytes)
}

fn spawn_ranges(
    handle: &Handle,
    path: &'static str,
    operation: Option<Arc<ReadOperation>>,
    backend: Backend,
    size: u64,
    ranges: Vec<Range<u64>>,
) -> BoxFuture<'static, anyhow::Result<Vec<Bytes>>> {
    let (mut tx, rx) = tokio::sync::oneshot::channel();
    handle.spawn(async move {
        let work = async {
            let (physical, bytes) = plan_ranges(&ranges, size, FETCH_BYTES.limit)?;
            let queue = FetchTimer {
                start: Instant::now(),
                path,
                operation: operation.clone(),
                active: false,
            };
            let reservation = Arc::new(FETCH_BYTES.acquire(bytes).await?);
            drop(queue);
            // Each coalesced backend request gets its own count permit. There is
            // no outer batch permit that could deadlock subordinate admission.
            let fetched: Vec<Bytes> = futures::stream::iter(physical.iter().cloned())
                .map(|range| {
                    physical_fetch(
                        &backend,
                        range,
                        path,
                        operation.as_ref(),
                        Arc::clone(&reservation),
                    )
                })
                .buffered(10)
                .try_collect()
                .await?;
            let mut result = Vec::with_capacity(ranges.len());
            for range in &ranges {
                if range.is_empty() {
                    result.push(Bytes::new());
                    continue;
                }
                let index = physical.partition_point(|physical| physical.start <= range.start) - 1;
                let start = (range.start - physical[index].start) as usize;
                let end = (range.end - physical[index].start) as usize;
                result.push(fetched[index].slice(start..end));
            }
            let logical_bytes: u64 = result.iter().map(|bytes| bytes.len() as u64).sum();
            config::metrics::VIX_FETCH_COUNT_TOTAL
                .with_label_values(&[path])
                .inc_by(result.len() as u64);
            config::metrics::VIX_FETCH_BYTES_TOTAL
                .with_label_values(&[path])
                .inc_by(logical_bytes);
            if let Some(operation) = &operation {
                operation.record(|stats| {
                    stats.batches.fetch_add(1, Ordering::Relaxed);
                    stats
                        .fetches
                        .fetch_add(result.len() as u64, Ordering::Relaxed);
                    stats.bytes.fetch_add(logical_bytes, Ordering::Relaxed);
                });
            }
            Ok::<_, anyhow::Error>((result, reservation))
        };
        let result = tokio::select! {
            biased;
            _ = tx.closed() => return,
            _ = cancelled(operation.as_ref()) => Err(cancelled_error()),
            result = work => result,
        };
        // Reservation travels with the result until the receiver consumes it.
        let _ = tx.send(result);
    });
    async move {
        // Delivery ends the fetch lease. Reader-cache accounting and the
        // caller's evaluation/scan reservations govern buffers retained after
        // this point; cached Bytes must not pin a fetch lease and deadlock IO.
        let (result, _reservation) = rx.await.map_err(|_| cancelled_error())??;
        Ok(result)
    }
    .boxed()
}

/// Immutable reusable identity. `operation` is populated only in ephemeral
/// `for_current_operation` clones, never in the source stored by a cached reader.
#[derive(Clone)]
pub struct LadderRangeSource {
    backend: Backend,
    size: u64,
    handle: Handle,
    operation: Option<Arc<ReadOperation>>,
}
impl LadderRangeSource {
    pub fn new(account: String, file: &str, size: u64, handle: Handle) -> Self {
        Self {
            backend: Backend::Ladder {
                account,
                location: Path::from(file),
            },
            size,
            handle,
            operation: None,
        }
    }
}
impl VixRangeSource for LadderRangeSource {
    fn len(&self) -> u64 {
        self.size
    }
    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + match &self.backend {
                Backend::Ladder { account, location } => {
                    account.capacity() + location.as_ref().len()
                }
                Backend::Store { .. } => 0,
            }
    }
    fn for_current_operation(&self) -> Option<Arc<dyn VixRangeSource>> {
        let mut bound = self.clone();
        bound.operation = current_operation().or_else(|| self.operation.clone());
        bound.operation.as_ref()?;
        Some(Arc::new(bound))
    }
    fn fetch(&self, range: Range<u64>) -> BoxFuture<'static, anyhow::Result<Bytes>> {
        let future = self.fetch_many(vec![range]);
        async move { Ok(future.await?.remove(0)) }.boxed()
    }
    fn fetch_many(
        &self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'static, anyhow::Result<Vec<Bytes>>> {
        spawn_ranges(
            &self.handle,
            "search",
            current_operation().or_else(|| self.operation.clone()),
            self.backend.clone(),
            self.size,
            ranges,
        )
    }
    fn describe(&self) -> String {
        match &self.backend {
            Backend::Ladder { location, .. } => location.to_string(),
            _ => unreachable!(),
        }
    }
}

#[derive(Clone)]
pub struct StoreRangeSource {
    backend: Backend,
    size: u64,
    handle: Handle,
    operation: Option<Arc<ReadOperation>>,
}
impl StoreRangeSource {
    pub fn new(store: Arc<dyn ObjectStore>, location: Path, size: u64, handle: Handle) -> Self {
        Self {
            backend: Backend::Store { store, location },
            size,
            handle,
            operation: None,
        }
    }
}
impl VixRangeSource for StoreRangeSource {
    fn len(&self) -> u64 {
        self.size
    }
    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + match &self.backend {
                Backend::Store { location, .. } => location.as_ref().len(),
                Backend::Ladder { .. } => 0,
            }
    }
    fn for_current_operation(&self) -> Option<Arc<dyn VixRangeSource>> {
        let mut bound = self.clone();
        bound.operation = current_operation().or_else(|| self.operation.clone());
        bound.operation.as_ref()?;
        Some(Arc::new(bound))
    }
    fn fetch(&self, range: Range<u64>) -> BoxFuture<'static, anyhow::Result<Bytes>> {
        let future = self.fetch_many(vec![range]);
        async move { Ok(future.await?.remove(0)) }.boxed()
    }
    fn fetch_many(
        &self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'static, anyhow::Result<Vec<Bytes>>> {
        spawn_ranges(
            &self.handle,
            "scan",
            current_operation().or_else(|| self.operation.clone()),
            self.backend.clone(),
            self.size,
            ranges,
        )
    }
    fn describe(&self) -> String {
        match &self.backend {
            Backend::Store { location, .. } => location.to_string(),
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use object_store::{ObjectStoreExt, memory::InMemory};

    use super::*;

    fn evaluation_fixture(
        gate: &Arc<ByteGate>,
        count: &Arc<Semaphore>,
        workspace: usize,
    ) -> EvaluationPermit {
        EvaluationPermit {
            _count: Arc::clone(count).try_acquire_owned().unwrap(),
            memory: Arc::new(EvaluationMemory {
                workspace,
                bytes: Mutex::new(gate.try_acquire(workspace).unwrap()),
                refusal: Mutex::new(None),
            }),
        }
    }

    #[test]
    fn evaluation_growth_is_nonblocking_absolute_and_released_with_its_owner() {
        let gate = ByteGate::new(32);
        let count = Arc::new(Semaphore::new(2));
        let first = ReadOperation::new(Arc::new(FetchStats::default()), None);
        let live = ReadOperation::new(Arc::new(FetchStats::default()), None);
        let first_permit = evaluation_fixture(&gate, &count, 8);
        let live_permit = evaluation_fixture(&gate, &count, 8);
        let delayed_operation = first.for_evaluation(&first_permit);
        first.run_evaluation(&first_permit, || {
            vortex_index::check_read_memory(8).unwrap();
            // Discarded streaming chunks are not a cumulative charge.
            for _ in 0..10 {
                vortex_index::check_read_memory(8).unwrap();
            }
        });
        assert_eq!(*gate.used.lock(), 24);
        let worked = std::sync::atomic::AtomicBool::new(false);
        let refused: anyhow::Result<()> = first.run_evaluation(&first_permit, || {
            vortex_index::check_read_memory(17)?;
            worked.store(true, Ordering::Relaxed);
            Ok(())
        });
        assert!(
            refused
                .unwrap_err()
                .chain()
                .any(|cause| cause.is::<FetchBudgetExceeded>())
        );
        assert!(!worked.load(Ordering::Relaxed));
        assert_eq!(
            *gate.used.lock(),
            24,
            "refused growth must not leak its delta"
        );
        assert!(
            first_permit.check_refusal().is_err(),
            "Option-returning readers must not hide a refusal"
        );
        first.cancel();
        drop(first_permit);
        assert_eq!(
            *gate.used.lock(),
            8,
            "operation clones cannot own working permits"
        );
        assert_eq!(count.available_permits(), 1);
        assert!(delayed_operation.is_cancelled());
        live.run_evaluation(&live_permit, || vortex_index::check_read_memory(24))
            .unwrap();
        assert_eq!(
            *gate.used.lock(),
            32,
            "another operation remains usable after cancellation/refusal"
        );
        drop(live_permit);
        assert_eq!(*gate.used.lock(), 0);
        assert_eq!(count.available_permits(), 2);
    }

    #[test]
    fn actual_object_and_overflow_reservations_refuse_before_allocation() {
        for owned in [33, usize::MAX] {
            let gate = ByteGate::new(32);
            let count = Arc::new(Semaphore::new(1));
            let permit = evaluation_fixture(&gate, &count, 8);
            assert!(
                permit
                    .reserve_owned(owned)
                    .unwrap_err()
                    .is::<FetchBudgetExceeded>()
            );
            assert_eq!(*gate.used.lock(), 8);
            drop(permit);
            assert_eq!(*gate.used.lock(), 0);
        }
    }

    #[tokio::test]
    async fn shared_source_binds_operations_before_executor_handoff() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("operation-sharing.vix");
        store
            .put(&path, Bytes::from_static(b"0123456789").into())
            .await
            .unwrap();
        let source = StoreRangeSource::new(store, path, 10, Handle::current());
        let first = ReadOperation::new(Arc::new(FetchStats::default()), None);
        let second = ReadOperation::new(Arc::new(FetchStats::default()), None);
        let first_bound = first.run(|| source.for_current_operation().unwrap());
        let second_bound = second.run(|| source.for_current_operation().unwrap());
        first.cancel();
        assert!(
            first_bound
                .fetch(0..2)
                .await
                .unwrap_err()
                .is::<vortex_index::VixError>()
        );
        assert_eq!(
            second_bound.fetch(2..7).await.unwrap(),
            Bytes::from_static(b"23456")
        );
        assert_eq!(first.stats.bytes.load(Ordering::Relaxed), 0);
        assert_eq!(second.stats.bytes.load(Ordering::Relaxed), 5);
        assert_eq!(second.stats.object_store_bytes.load(Ordering::Relaxed), 5);
        assert_eq!(second.stats.remote_bytes.load(Ordering::Relaxed), 0);
        let file_stats = Arc::new(FetchStats::default());
        let child = second.for_file(Arc::clone(&file_stats));
        assert_eq!(
            child.run(|| source.fetch(0..2)).await.unwrap(),
            Bytes::from_static(b"01")
        );
        assert_eq!(file_stats.bytes.load(Ordering::Relaxed), 2);
        assert_eq!(second.stats.bytes.load(Ordering::Relaxed), 7);
        assert_eq!(second.stats.physical_bytes.load(Ordering::Relaxed), 7);
        second.cancel();
        assert!(child.is_cancelled());
        assert!(source.operation.is_none());
    }

    #[tokio::test]
    async fn batched_ranges_preserve_order_duplicates_and_physical_accounting() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("batch-order.vix");
        store
            .put(&path, Bytes::from_static(b"0123456789").into())
            .await
            .unwrap();
        let source = StoreRangeSource::new(store, path, 10, Handle::current());
        let operation = ReadOperation::new(Arc::new(FetchStats::default()), None);
        let result = operation
            .run(|| source.fetch_many(vec![7..10, 1..3, 1..3, 5..5]))
            .await
            .unwrap();
        assert_eq!(
            result,
            vec![
                Bytes::from_static(b"789"),
                Bytes::from_static(b"12"),
                Bytes::from_static(b"12"),
                Bytes::new()
            ]
        );
        assert_eq!(operation.stats.fetches.load(Ordering::Relaxed), 4);
        assert_eq!(operation.stats.bytes.load(Ordering::Relaxed), 7);
        assert_eq!(operation.stats.physical_fetches.load(Ordering::Relaxed), 1);
        assert_eq!(operation.stats.physical_bytes.load(Ordering::Relaxed), 9);
        assert!(
            plan_ranges(&[0..11], 11, 10)
                .unwrap_err()
                .is::<FetchBudgetExceeded>()
        );
        let overhead =
            2 * (2 * std::mem::size_of::<Bytes>() + 3 * std::mem::size_of::<Range<u64>>());
        let (physical, bytes) = plan_ranges(&[20..22, 0..2], 22, overhead + 4).unwrap();
        assert_eq!(physical, vec![0..2, 20..22]);
        assert_eq!(bytes, overhead + 4);
    }

    #[tokio::test]
    async fn cancelled_byte_wait_does_not_leak_capacity() {
        let gate = ByteGate::new(8);
        let held = gate.acquire(8).await.unwrap();
        let operation = ReadOperation::new(Arc::new(FetchStats::default()), None);
        let owner = operation.owner_guard();
        let wait = async {
            tokio::select! {
                biased;
                _ = operation.cancelled() => Err(cancelled_error()),
                result = gate.acquire(1) => result,
            }
        };
        tokio::pin!(wait);
        assert!(futures::poll!(&mut wait).is_pending());
        drop(owner);
        assert!(wait.await.is_err());
        assert_eq!(*gate.used.lock(), 8);
        drop(held);
        let restored = gate.acquire(8).await.unwrap();
        assert_eq!(restored.bytes, 8);
    }

    #[tokio::test]
    async fn cancelling_fetch_queued_on_bytes_never_issues_io() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("queued-byte-cancel.vix");
        store
            .put(&path, Bytes::from_static(b"test").into())
            .await
            .unwrap();
        let source = StoreRangeSource::new(store, path, 4, Handle::current());
        let held = FETCH_BYTES.acquire(FETCH_BYTES.limit).await.unwrap();
        let operation = ReadOperation::new(Arc::new(FetchStats::default()), None);
        let future = operation.run(|| source.fetch(0..4));
        tokio::task::yield_now().await;
        operation.cancel();
        let error = tokio::time::timeout(Duration::from_secs(5), future)
            .await
            .unwrap()
            .unwrap_err();
        assert!(error.is::<vortex_index::VixError>());
        assert_eq!(operation.stats.physical_fetches.load(Ordering::Relaxed), 0);
        drop(held);
        let live = ReadOperation::new(Arc::new(FetchStats::default()), None);
        assert_eq!(
            live.run(|| source.fetch(0..4)).await.unwrap(),
            Bytes::from_static(b"test")
        );
    }

    async fn stalled_s3_source() -> (
        StoreRangeSource,
        tokio::sync::oneshot::Receiver<()>,
        tokio::task::JoinHandle<usize>,
    ) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (started, ready) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            loop {
                let mut byte = [0];
                socket.read_exact(&mut byte).await.unwrap();
                request.push(byte[0]);
                if request.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            socket.write_all(
                b"HTTP/1.1 206 Partial Content\r\nContent-Length: 4\r\nContent-Range: bytes 0-3/4\r\nETag: \"stalled\"\r\nLast-Modified: Sat, 05 Sep 2026 00:00:00 GMT\r\n\r\n"
            ).await.unwrap();
            started.send(()).unwrap();
            // No response body: a cancelled read must close its actual socket,
            // not merely stop waiting on a detached range task.
            let mut byte = [0];
            match tokio::time::timeout(Duration::from_secs(5), socket.read(&mut byte))
                .await
                .expect("cancelled range left its connection active")
            {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => 0,
                Err(error) => panic!("reading cancelled backend socket: {error}"),
            }
        });
        let store = object_store::aws::AmazonS3Builder::new()
            .with_bucket_name("vix-cancellation-test")
            .with_region("us-east-1")
            .with_access_key_id("test")
            .with_secret_access_key("test")
            .with_skip_signature(true)
            .with_allow_http(true)
            .with_endpoint(endpoint)
            .build()
            .unwrap();
        (
            StoreRangeSource::new(
                Arc::new(store),
                Path::from("stalled.vix"),
                4,
                Handle::current(),
            ),
            ready,
            server,
        )
    }

    #[tokio::test]
    async fn cancelling_active_operation_closes_backend_io() {
        let (source, ready, server) = stalled_s3_source().await;
        let operation = ReadOperation::new(Arc::new(FetchStats::default()), None);
        let future = operation.run(|| source.fetch(0..4));
        tokio::time::timeout(Duration::from_secs(5), ready)
            .await
            .unwrap()
            .unwrap();
        operation.cancel();
        assert!(future.await.unwrap_err().is::<vortex_index::VixError>());
        assert_eq!(server.await.unwrap(), 0);
        assert_eq!(operation.stats.fetches.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn dropping_receiver_closes_backend_io_without_cancelling_other_reads() {
        let (source, ready, server) = stalled_s3_source().await;
        let operation = ReadOperation::new(Arc::new(FetchStats::default()), None);
        let future = operation.run(|| source.fetch(0..4));
        tokio::time::timeout(Duration::from_secs(5), ready)
            .await
            .unwrap()
            .unwrap();
        drop(future);
        assert_eq!(server.await.unwrap(), 0);
        assert!(!operation.is_cancelled());
    }

    #[tokio::test]
    async fn deadline_and_nested_scopes_restore_operation() {
        let outer = ReadOperation::new(Arc::new(FetchStats::default()), None);
        let inner = ReadOperation::new(Arc::new(FetchStats::default()), Some(Instant::now()));
        outer.run(|| {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                inner.run(|| panic!("unwind"))
            }));
            assert!(Arc::ptr_eq(&current_operation().unwrap(), &outer));
        });
        assert!(current_operation().is_none());
        inner.cancelled().await;
        assert!(inner.is_cancelled());
        assert!(!outer.is_cancelled());
    }
}
