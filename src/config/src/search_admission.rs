// Copyright 2026 OpenObserve Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

//! Node-local admission before follower index evaluation and data allocation.

use std::{
    sync::{Arc, LazyLock},
    time::{Duration, Instant},
};

use prometheus::{Histogram, IntGauge};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};

// The observed maximum across 455 alert requests on 19-20 queriers is 124
// files per node. 256 leaves more than 2x headroom without trusting SQL shape
// or caller tags. This is a scan-volume hint, not a bound on result memory.
pub const LIGHT_FILE_LIMIT: usize = 256;
const MAX_WAIT: Duration = Duration::from_secs(5);
const PRESSURE_POLL: Duration = Duration::from_millis(25);

#[derive(Clone, Copy, Debug, Default)]
pub struct ScanWork {
    pub files: usize,
}

impl ScanWork {
    fn weight(self, capacity: u32) -> u32 {
        if self.files > LIGHT_FILE_LIMIT {
            (self.files / LIGHT_FILE_LIMIT).min(capacity as usize) as u32
        } else {
            0
        }
    }
}

static NODE_ADMISSION: LazyLock<Admission> = LazyLock::new(|| {
    // One unit per four configured CPUs, bounded to [1,4].
    Admission::new((crate::get_config().limit.cpu_num / 4).clamp(1, 4) as u32)
});
struct ClassMetrics {
    active: IntGauge,
    queued: IntGauge,
    wait: Histogram,
}
impl ClassMetrics {
    fn new(class: &str) -> Self {
        Self {
            active: crate::metrics::FOLLOWER_ADMISSION_ACTIVE.with_label_values(&[class]),
            queued: crate::metrics::FOLLOWER_ADMISSION_QUEUED.with_label_values(&[class]),
            wait: crate::metrics::FOLLOWER_ADMISSION_WAIT_SECONDS.with_label_values(&[class]),
        }
    }
}
static LIGHT: LazyLock<ClassMetrics> = LazyLock::new(|| ClassMetrics::new("light"));
static HEAVY: LazyLock<ClassMetrics> = LazyLock::new(|| ClassMetrics::new("heavy"));

/// Held through setup and all response-stream producers. Drop is the only
/// release operation, so errors, unwinding and cancelled futures are safe.
pub struct AdmissionGuard {
    _permit: Option<OwnedSemaphorePermit>,
    metrics: &'static ClassMetrics,
}
impl Drop for AdmissionGuard {
    fn drop(&mut self) {
        self.metrics.active.dec();
    }
}
struct Waiting(Instant);
impl Drop for Waiting {
    fn drop(&mut self) {
        HEAVY.queued.dec();
        HEAVY.wait.observe(self.0.elapsed().as_secs_f64());
    }
}

struct Admission {
    budget: Arc<Semaphore>,
    capacity: u32,
    // Also orders memory-watermark waiters. No later request can take a small
    // permit while the FIFO head awaits a large permit or lower RSS.
    turn: Mutex<()>,
}
impl Admission {
    fn new(capacity: u32) -> Self {
        assert!(capacity > 0);
        Self {
            budget: Arc::new(Semaphore::new(capacity as usize)),
            capacity,
            turn: Mutex::new(()),
        }
    }
    async fn enter(
        &self,
        work: ScanWork,
        max_wait: Duration,
        pressured: impl Fn() -> bool,
    ) -> AdmissionGuard {
        let weight = work.weight(self.capacity);
        if weight == 0 {
            return light_guard();
        }
        HEAVY.queued.inc();
        let waiting = Waiting(Instant::now());
        let admitted = tokio::time::timeout(max_wait, async {
            let _turn = self.turn.lock().await;
            let permit = self
                .budget
                .clone()
                .acquire_many_owned(weight)
                .await
                .expect("node admission semaphore is never closed");
            while pressured() {
                tokio::time::sleep(PRESSURE_POLL).await;
            }
            permit
        })
        .await;
        drop(waiting);
        let permit = match admitted {
            Ok(permit) => Some(permit),
            Err(_) => {
                // Admission is a scheduling hint, not a new rejection policy.
                // A cross-node merge can otherwise deadlock behind other queries.
                // Existing MemoryCircuitBreaker/DF checks still run downstream.
                crate::metrics::FOLLOWER_ADMISSION_TIMEOUTS_TOTAL.inc();
                None
            }
        };
        HEAVY.active.inc();
        AdmissionGuard {
            _permit: permit,
            metrics: &HEAVY,
        }
    }
    #[cfg(test)]
    fn available(&self) -> usize {
        self.budget.available_permits()
    }
}
fn light_guard() -> AdmissionGuard {
    LIGHT.active.inc();
    AdmissionGuard {
        _permit: None,
        metrics: &LIGHT,
    }
}
/// Eight tenths of the existing breaker envelope (90% -> 72% of RAM).
/// Integer arithmetic matches the original envelope before lowering it.
pub fn low_watermark(total: usize, ratio: usize) -> usize {
    (total / 100).saturating_mul(ratio) / 5 * 4
}

/// Leave at least three quarters of the remaining RPC/query time for execution.
/// A shorter straggler trigger also caps admission, rather than causing the
/// queue itself to create a redundant attempt. `elapsed` includes setup so far.
pub fn wait_budget(
    request_timeout: Duration,
    elapsed: Duration,
    redispatch_after: Option<Duration>,
) -> Duration {
    let remaining = request_timeout.saturating_sub(elapsed);
    let wait = MAX_WAIT.min(remaining / 4);
    redispatch_after.map_or(wait, |delay| wait.min(delay.saturating_sub(elapsed) / 4))
}

/// Light requests perform no clock read, allocation, lock, or timer here.
/// Classification only reads the local ticket ID count; no metadata I/O is needed.
pub async fn enter(work: ScanWork, max_wait: Duration) -> AdmissionGuard {
    if work.weight(1) == 0 {
        return light_guard();
    }
    let cfg = crate::get_config();
    let threshold = low_watermark(cfg.limit.mem_total, cfg.common.memory_circuit_breaker_ratio);
    let enabled =
        cfg.common.memory_circuit_breaker_enabled && cfg.common.memory_circuit_breaker_ratio > 0;
    let rss = crate::metrics::NODE_MEMORY_USAGE.with_label_values::<&str>(&[]);
    NODE_ADMISSION
        .enter(work, max_wait.min(MAX_WAIT), || {
            enabled && rss.get().max(0) as usize > threshold
        })
        .await
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use super::*;

    const WAIT: Duration = Duration::from_millis(200);
    fn files(files: usize) -> ScanWork {
        ScanWork { files }
    }
    async fn enter(gate: &Admission, count: usize) -> AdmissionGuard {
        gate.enter(files(count), WAIT, || false).await
    }

    #[test]
    fn local_file_count_preserves_observed_alerts_without_shape_or_event_hints() {
        // Observed maxima of the four alert shapes, from actual dispatch logs.
        for local_files in [70, 70, 74, 124] {
            assert_eq!(files(local_files).weight(4), 0);
            assert!(local_files * 2 <= LIGHT_FILE_LIMIT);
        }
        // The 264-file incident is intentionally outside this scan-volume gate.
        assert_eq!(files(14).weight(4), 0);
        assert_eq!(files(LIGHT_FILE_LIMIT).weight(4), 0);
        assert_eq!(files(LIGHT_FILE_LIMIT + 1).weight(4), 1);
        assert_eq!(files(LIGHT_FILE_LIMIT * 2).weight(4), 2);
        // The later OOM wave assigns about 1,300 files to each follower.
        assert_eq!(files(1300).weight(4), 4);
        assert_eq!(files(usize::MAX).weight(4), 4);
    }

    #[test]
    fn wait_is_well_below_deadline_and_straggler_trigger() {
        assert_eq!(
            wait_budget(
                Duration::from_secs(200),
                Duration::ZERO,
                Some(Duration::from_secs(86400))
            ),
            MAX_WAIT
        );
        assert_eq!(
            wait_budget(Duration::from_secs(1), Duration::ZERO, None),
            Duration::from_millis(250)
        );
        assert_eq!(
            wait_budget(
                Duration::from_secs(200),
                Duration::ZERO,
                Some(Duration::from_secs(2))
            ),
            Duration::from_millis(500)
        );
        assert_eq!(
            wait_budget(
                Duration::from_secs(200),
                Duration::from_secs(2),
                Some(Duration::from_secs(2))
            ),
            Duration::ZERO
        );
        assert_eq!(
            wait_budget(Duration::from_secs(1), Duration::from_secs(2), None),
            Duration::ZERO
        );
    }

    #[tokio::test]
    async fn light_never_waits_for_heavy_or_memory() {
        let gate = Admission::new(1);
        let held = enter(&gate, LIGHT_FILE_LIMIT + 1).await;
        for work in [files(0), files(124), files(LIGHT_FILE_LIMIT)] {
            let light = gate
                .enter(work, Duration::ZERO, || {
                    panic!("light must not check pressure")
                })
                .await;
            assert_eq!(gate.available(), 0);
            drop(light);
        }
        drop(held);
        assert_eq!(gate.available(), 1);
    }

    #[tokio::test]
    async fn bounded_wait_must_not_introduce_a_query_error() {
        let gate = Admission::new(1);
        let held = enter(&gate, LIGHT_FILE_LIMIT * 2).await;
        let expired = gate
            .enter(
                files(LIGHT_FILE_LIMIT * 2),
                Duration::from_millis(5),
                || false,
            )
            .await;
        // The return type is now infallible. A timeout is active, without a permit.
        assert!(expired._permit.is_none());
        assert!(std::ptr::eq(expired.metrics, &*HEAVY));
        assert_eq!(gate.available(), 0);
        drop((expired, held));
        assert_eq!(gate.available(), 1);
    }

    #[tokio::test]
    async fn fifo_prevents_small_heavy_from_overtaking() {
        let gate = Admission::new(3);
        let held = enter(&gate, LIGHT_FILE_LIMIT * 2).await;
        let first = gate.enter(files(LIGHT_FILE_LIMIT * 3), WAIT, || false);
        let second = gate.enter(files(LIGHT_FILE_LIMIT + 1), WAIT, || false);
        tokio::pin!(first, second);
        assert!(futures::poll!(&mut first).is_pending());
        assert!(futures::poll!(&mut second).is_pending());
        drop(held);
        let first = first.await;
        assert!(futures::poll!(&mut second).is_pending());
        drop(first);
        drop(second.await);
        assert_eq!(gate.available(), 3);
    }

    #[tokio::test]
    async fn cancelling_waiter_and_active_task_releases_every_permit() {
        let gate = Arc::new(Admission::new(2));
        let held = enter(&gate, LIGHT_FILE_LIMIT * 2).await;
        {
            let queued = gate.enter(files(LIGHT_FILE_LIMIT * 2), WAIT, || false);
            tokio::pin!(queued);
            assert!(futures::poll!(&mut queued).is_pending());
        }
        drop(held);
        assert_eq!(gate.available(), 2);
        let task_gate = gate.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _guard = enter(&task_gate, LIGHT_FILE_LIMIT * 2).await;
            tx.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        rx.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(gate.available(), 2);
    }

    #[tokio::test]
    async fn error_and_panic_release_active_permit() {
        let gate = Arc::new(Admission::new(1));
        let task_gate = gate.clone();
        let task = tokio::spawn(async move {
            let _guard = enter(&task_gate, LIGHT_FILE_LIMIT + 1).await;
            panic!("test unwind");
        });
        assert!(task.await.unwrap_err().is_panic());
        assert_eq!(gate.available(), 1);
        async fn fail(gate: &Admission) -> Result<(), ()> {
            let _guard = enter(gate, LIGHT_FILE_LIMIT + 1).await;
            Err(())
        }
        assert!(fail(&gate).await.is_err());
        assert_eq!(gate.available(), 1);
    }

    #[tokio::test]
    async fn memory_pressure_queues_then_releases_or_fails_open() {
        let gate = Admission::new(2);
        let pressured = AtomicBool::new(true);
        let waiting = gate.enter(files(LIGHT_FILE_LIMIT + 1), WAIT, || {
            pressured.load(Ordering::Relaxed)
        });
        tokio::pin!(waiting);
        assert!(futures::poll!(&mut waiting).is_pending());
        pressured.store(false, Ordering::Relaxed);
        drop(waiting.await);
        assert_eq!(gate.available(), 2);
        let expired = gate
            .enter(
                files(LIGHT_FILE_LIMIT + 1),
                Duration::from_millis(5),
                || true,
            )
            .await;
        assert!(expired._permit.is_none());
        assert_eq!(gate.available(), 2);
        drop(expired);
    }

    #[tokio::test]
    async fn cancellation_and_panic_while_checking_pressure_release_budget() {
        let gate = Arc::new(Admission::new(2));
        {
            let waiting = gate.enter(files(LIGHT_FILE_LIMIT * 2), WAIT, || true);
            tokio::pin!(waiting);
            assert!(futures::poll!(&mut waiting).is_pending());
            assert_eq!(gate.available(), 0);
        }
        assert_eq!(gate.available(), 2);
        let task_gate = gate.clone();
        let task = tokio::spawn(async move {
            let _ = task_gate
                .enter(files(LIGHT_FILE_LIMIT * 2), WAIT, || {
                    panic!("pressure panic")
                })
                .await;
        });
        assert!(task.await.unwrap_err().is_panic());
        assert_eq!(gate.available(), 2);
    }

    #[tokio::test]
    async fn two_node_opposite_order_can_both_finish_after_timeout() {
        let a = Admission::new(1);
        let b = Admission::new(1);
        let qa = enter(&a, LIGHT_FILE_LIMIT + 1).await;
        let qb = enter(&b, LIGHT_FILE_LIMIT + 1).await;
        let (ra, rb) = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(
                enter(&b, LIGHT_FILE_LIMIT + 1),
                enter(&a, LIGHT_FILE_LIMIT + 1)
            )
        })
        .await
        .unwrap();
        assert!(ra._permit.is_none() && rb._permit.is_none());
        // Both queries can now consume the counterpart stream and complete.
        drop((qa, qb, ra, rb));
        assert_eq!((a.available(), b.available()), (1, 1));
    }

    #[tokio::test]
    async fn concurrent_mixed_load_is_bounded_before_expiry() {
        let gate = Arc::new(Admission::new(4));
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let mut tasks = tokio::task::JoinSet::new();
        for i in 0..100 {
            let (gate, active, peak) = (gate.clone(), active.clone(), peak.clone());
            tasks.spawn(async move {
                let work = files(if i % 3 == 0 {
                    124
                } else {
                    LIGHT_FILE_LIMIT * 2
                });
                let guard = gate.enter(work, Duration::from_secs(2), || false).await;
                let w = work.weight(4) as usize;
                assert!(w == 0 || guard._permit.is_some());
                let now = active.fetch_add(w, Ordering::SeqCst) + w;
                peak.fetch_max(now, Ordering::SeqCst);
                tokio::task::yield_now().await;
                active.fetch_sub(w, Ordering::SeqCst);
            });
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
        assert!(peak.load(Ordering::SeqCst) <= 4);
        assert_eq!(gate.available(), 4);
    }

    #[test]
    fn low_watermark_preserves_original_breaker_margin() {
        let total = 24_usize << 30;
        let original = total / 100 * 90;
        assert_eq!(low_watermark(total, 90), original / 5 * 4);
        assert!(low_watermark(total, 90) < original);
        assert_eq!(low_watermark(total, 0), 0);
    }

    #[test]
    #[ignore = "release microbenchmark; run in isolation with --nocapture"]
    fn admission_light_microbenchmark() {
        use std::hint::black_box;
        const N: usize = 2_000_000;
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        rt.block_on(async {
            drop(super::enter(files(124), Duration::from_secs(5)).await);
            let before = LIGHT.active.get();
            for round in 0..20 {
                for mode in if round % 2 == 0 {
                    [false, true]
                } else {
                    [true, false]
                } {
                    let start = Instant::now();
                    for _ in 0..N {
                        if mode {
                            let guard =
                                super::enter(black_box(files(124)), Duration::from_secs(5)).await;
                            black_box(&guard);
                            drop(guard);
                        } else {
                            black_box(black_box(files(124)));
                        }
                    }
                    println!(
                        "BENCH round={round} mode={} n={N} elapsed_ns={}",
                        if mode { "admitted" } else { "baseline" },
                        start.elapsed().as_nanos()
                    );
                }
            }
            assert_eq!(LIGHT.active.get(), before);
            assert_eq!(LIGHT.queued.get(), 0);
        });
    }

    #[test]
    #[ignore = "isolated release pressure simulation; PERF_ADMISSION_MODE controls baseline/new"]
    fn admission_pressure_probe() {
        let bounded = std::env::var("PERF_ADMISSION_MODE").unwrap_or_default() != "baseline";
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_time()
            .build()
            .unwrap();
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(Admission::new(4));
        let baseline = (HEAVY.active.get(), HEAVY.queued.get(), LIGHT.active.get());
        let start = Instant::now();
        rt.block_on(async {
            let mut tasks = tokio::task::JoinSet::new();
            for i in 0..16 {
                let (gate, active, peak) = (gate.clone(), active.clone(), peak.clone());
                tasks.spawn(async move {
                    let _guard = if bounded {
                        Some(
                            gate.enter(files(1300), Duration::from_secs(5), || false)
                                .await,
                        )
                    } else {
                        None
                    };
                    let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(now, Ordering::SeqCst);
                    // Deterministic 32 MiB live working set through consumer backpressure.
                    // This is a scheduling simulation, not SQL memory or throughput.
                    let mut bytes = vec![0_u8; 32 << 20];
                    for (j, v) in bytes.iter_mut().enumerate() {
                        *v = (j ^ i) as u8;
                    }
                    tokio::time::sleep(Duration::from_millis(80)).await;
                    std::hint::black_box(&bytes);
                    drop(bytes);
                    active.fetch_sub(1, Ordering::SeqCst);
                });
            }
            for _ in 0..100 {
                drop(gate.enter(files(124), Duration::ZERO, || true).await);
                tokio::task::yield_now().await;
            }
            while let Some(result) = tasks.join_next().await {
                result.unwrap();
            }
        });
        assert_eq!(gate.available(), 4);
        assert_eq!(
            baseline,
            (HEAVY.active.get(), HEAVY.queued.get(), LIGHT.active.get())
        );
        println!(
            "PRESSURE mode={} heavy=16 light=100 peak_active={} live_bytes={} wall_us={} df_reserved_peak=0",
            if bounded { "admitted" } else { "baseline" },
            peak.load(Ordering::Relaxed),
            peak.load(Ordering::Relaxed) * (32 << 20),
            start.elapsed().as_micros()
        );
    }
}
