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

//! Process-wide byte budget for compaction downloads (H3 / DESIGN-V2 §7).
//!
//! The 2026-08-17 incident: every merge job downloaded its inputs at
//! `cpu_num` concurrency, and 12 jobs ran per compactor — nothing bounded
//! the BYTES in flight across jobs, only the per-job parallelism. With
//! downloads now streaming to disk (H3) the per-download RAM is a bounded
//! buffer, and this budget is the fleet-level backstop on the aggregate
//! disk-write burst + transport buffers: one process-wide account of
//! in-flight compressed bytes that every compaction download must admit
//! into first (`ZO_COMPACT_DOWNLOAD_BUDGET_MB`, default 2048; 0 =
//! unlimited).
//!
//! Admission rules (deterministic):
//! - capacity `0` = unlimited (admit immediately);
//! - a download admits when `in_flight + bytes <= capacity`;
//! - a WORKER holding no admitted bytes always admits its next download regardless of size — every
//!   merge job can always make progress, so a file larger than the whole budget delays (until the
//!   worker's earlier downloads drain) but never deadlocks or starves.
//!
//! The per-job `Semaphore` in `cache_remote_files` still caps request
//! parallelism; concurrency knobs cap parallelism, the byte budget caps
//! bytes — both exist independently (§7).

use std::sync::{
    Arc, LazyLock,
    atomic::{AtomicU64, Ordering},
};

/// One process-wide budget instance sized from
/// `ZO_COMPACT_DOWNLOAD_BUDGET_MB` (0 = unlimited).
static GLOBAL: LazyLock<Arc<DownloadBudget>> = LazyLock::new(|| {
    Arc::new(DownloadBudget::new(
        config::get_config().compact.download_budget_mb as u64 * 1024 * 1024,
    ))
});

pub(crate) fn global() -> Arc<DownloadBudget> {
    Arc::clone(&GLOBAL)
}

/// In-flight bytes one merge job (worker) currently holds — the
/// starvation-proofing input to [`DownloadBudget::admit`]. One instance per
/// `cache_remote_files` call, shared by its download tasks.
#[derive(Default)]
pub(crate) struct WorkerBytes(AtomicU64);

impl WorkerBytes {
    fn held(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

pub(crate) struct DownloadBudget {
    /// Total budget in bytes; `0` = unlimited.
    capacity: u64,
    /// Bytes currently admitted across the whole process. All mutations run
    /// under this lock, so worker counters stay consistent with the total.
    in_flight: parking_lot::Mutex<u64>,
    /// Wakes waiters when a permit releases bytes.
    notify: tokio::sync::Notify,
}

impl DownloadBudget {
    pub(crate) fn new(capacity: u64) -> Self {
        Self {
            capacity,
            in_flight: parking_lot::Mutex::new(0),
            notify: tokio::sync::Notify::new(),
        }
    }

    /// One locked admission attempt: reserve the bytes if they fit (or the
    /// worker holds nothing — its next download always admits).
    fn try_reserve(&self, worker: &WorkerBytes, bytes: u64) -> bool {
        let mut in_flight = self.in_flight.lock();
        let fits = self.capacity == 0 || *in_flight + bytes <= self.capacity;
        if fits || worker.held() == 0 {
            *in_flight += bytes;
            worker.0.fetch_add(bytes, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Admit `bytes` for `worker`, waiting until they fit the remaining
    /// budget (or the worker holds nothing — its next download always
    /// admits). The returned permit releases the bytes on drop. Owned
    /// `Arc`s so the permit can cross task boundaries, mirroring
    /// `Semaphore::acquire_owned`.
    pub(crate) async fn admit(
        self: Arc<Self>,
        worker: Arc<WorkerBytes>,
        bytes: u64,
    ) -> BudgetPermit {
        loop {
            if self.try_reserve(&worker, bytes) {
                break;
            }
            // create the waiter, then RE-CHECK: a release between the failed
            // attempt and the waiter's creation must not be missed
            let notified = self.notify.notified();
            if self.try_reserve(&worker, bytes) {
                break;
            }
            notified.await;
        }
        BudgetPermit {
            budget: self,
            worker,
            bytes,
        }
    }

    /// Bytes currently admitted (observability / tests).
    pub(crate) fn in_flight(&self) -> u64 {
        *self.in_flight.lock()
    }
}

/// The admission of one download; dropping it returns the bytes to the
/// budget and wakes waiters.
pub(crate) struct BudgetPermit {
    budget: Arc<DownloadBudget>,
    worker: Arc<WorkerBytes>,
    bytes: u64,
}

impl Drop for BudgetPermit {
    fn drop(&mut self) {
        {
            let mut in_flight = self.budget.in_flight.lock();
            *in_flight = in_flight.saturating_sub(self.bytes);
            let held = self.worker.held();
            self.worker
                .0
                .store(held.saturating_sub(self.bytes), Ordering::Relaxed);
        }
        // notify_waiters wakes every parked admit: each re-checks under the
        // lock, so a release that frees room for several small downloads
        // admits them all in one round
        self.budget.notify.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn budget(capacity: u64) -> Arc<DownloadBudget> {
        Arc::new(DownloadBudget::new(capacity))
    }

    fn worker() -> Arc<WorkerBytes> {
        Arc::new(WorkerBytes::default())
    }

    /// Poll an admit exactly once; Some(permit) if it admitted immediately.
    fn try_admit(
        budget: &Arc<DownloadBudget>,
        worker: &Arc<WorkerBytes>,
        bytes: u64,
    ) -> Option<BudgetPermit> {
        use std::task::{Context, Poll};
        let fut = Arc::clone(budget).admit(Arc::clone(worker), bytes);
        let mut fut = Box::pin(fut);
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(permit) => Some(permit),
            Poll::Pending => None,
        }
    }

    #[tokio::test]
    async fn admits_when_bytes_fit() {
        let b = budget(100);
        let w = worker();
        let p1 = try_admit(&b, &w, 60).expect("fits");
        assert_eq!(b.in_flight(), 60);
        let p2 = try_admit(&b, &w, 40).expect("fills to capacity exactly");
        assert_eq!(b.in_flight(), 100);
        drop(p1);
        assert_eq!(b.in_flight(), 40);
        drop(p2);
        assert_eq!(b.in_flight(), 0);
    }

    #[tokio::test]
    async fn waits_until_release_frees_room() {
        let b = budget(100);
        let w1 = worker();
        let w2 = worker();
        let p1 = Arc::clone(&b).admit(Arc::clone(&w1), 80).await;
        let small = Arc::clone(&b).admit(Arc::clone(&w2), 10).await;
        // w2 already holds bytes, so its next download must FIT: 60 over
        // 90/100 parks
        assert!(try_admit(&b, &w2, 60).is_none());
        drop(p1); // frees 80 -> 10 in flight
        let p3 = try_admit(&b, &w2, 60).expect("released bytes admit the waiter");
        assert_eq!(b.in_flight(), 70);
        drop(p3);
        drop(small);
        assert_eq!(b.in_flight(), 0);
    }

    #[tokio::test]
    async fn oversize_always_admits_for_an_empty_worker() {
        let b = budget(10);
        let w = worker();
        // larger than the WHOLE budget, but the worker holds nothing
        let p = try_admit(&b, &w, 50).expect("empty worker always admits one");
        assert_eq!(b.in_flight(), 50);
        drop(p);
        assert_eq!(b.in_flight(), 0);
    }

    #[tokio::test]
    async fn oversize_waits_while_the_worker_holds_bytes() {
        let b = budget(10);
        let w = worker();
        let p1 = Arc::clone(&b).admit(Arc::clone(&w), 5).await;
        // now the worker holds bytes: an oversize download must wait...
        assert!(try_admit(&b, &w, 50).is_none());
        // ...and admits once the worker drains (held == 0 again)
        drop(p1);
        let p2 = try_admit(&b, &w, 50).expect("drained worker admits oversize");
        drop(p2);
        assert_eq!(b.in_flight(), 0);
    }

    #[tokio::test]
    async fn each_worker_first_download_admits_even_over_budget() {
        // per-worker progress guarantee: no worker deadlocks behind another
        let b = budget(10);
        let w1 = worker();
        let w2 = worker();
        let p1 = try_admit(&b, &w1, 8).expect("fits");
        let p2 = try_admit(&b, &w2, 8).expect("w2 holds nothing: admits over budget");
        assert_eq!(b.in_flight(), 16);
        drop(p1);
        drop(p2);
    }

    #[tokio::test]
    async fn zero_capacity_is_unlimited() {
        let b = budget(0);
        let w = worker();
        let p1 = try_admit(&b, &w, u64::MAX / 4).expect("unlimited");
        let p2 = try_admit(&b, &w, u64::MAX / 4).expect("unlimited");
        drop(p1);
        drop(p2);
        assert_eq!(b.in_flight(), 0);
    }

    #[tokio::test]
    async fn release_wakes_parked_admissions() {
        let b = budget(100);
        let w1 = worker();
        let w2 = worker();
        let p1 = Arc::clone(&b).admit(Arc::clone(&w1), 80).await;
        let small = Arc::clone(&b).admit(Arc::clone(&w2), 10).await;
        let waiter = {
            let b = Arc::clone(&b);
            let w2 = Arc::clone(&w2);
            tokio::spawn(async move {
                // 90 in flight + 60 > 100 and w2 holds bytes -> parks
                let big = b.admit(w2, 60).await;
                drop(big);
            })
        };
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished(), "the oversubscribed admit must park");
        drop(p1); // frees 80 -> wakes the waiter (10 + 60 <= 100)
        tokio::time::timeout(std::time::Duration::from_secs(5), waiter)
            .await
            .expect("waiter must wake after release")
            .unwrap();
        drop(small);
        assert_eq!(b.in_flight(), 0);
    }
}
