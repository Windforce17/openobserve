// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use std::{collections::VecDeque, sync::atomic::Ordering, time::Duration};

use super::{DownloadRejection, FileInfo, Lazy, QueueDownloadOutcome, get_config, metrics};

/// Ordering is also the direction of an upgrade. Classes have dedicated workers;
/// priority never means that a busy foreground class can starve a lower class.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
pub enum DownloadClass {
    DemandSmall,
    UsedSidecar,
    BroadcastLatest,
    QueryBulk,
}

impl DownloadClass {
    const ALL: [Self; 4] = [
        Self::DemandSmall,
        Self::UsedSidecar,
        Self::BroadcastLatest,
        Self::QueryBulk,
    ];

    pub fn label(self) -> &'static str {
        match self {
            Self::DemandSmall => "demand_small",
            Self::UsedSidecar => "used_sidecar",
            Self::BroadcastLatest => "broadcast_latest",
            Self::QueryBulk => "query_bulk",
        }
    }
}

struct State {
    queues: [VecDeque<FileInfo>; 4],
    closed: bool,
}

pub(super) struct ClassQueue {
    state: parking_lot::Mutex<State>,
    changed: tokio::sync::Notify,
    capacity: usize,
}

impl ClassQueue {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            state: parking_lot::Mutex::new(State {
                queues: std::array::from_fn(|_| VecDeque::new()),
                closed: false,
            }),
            changed: tokio::sync::Notify::new(),
            capacity,
        }
    }

    pub(super) fn push(&self, mut item: FileInfo) -> Result<(), DownloadRejection> {
        let mut state = self.state.lock();
        if state.closed {
            return Err(DownloadRejection::Closed);
        }
        let desired = item.reservation.requested_class.load(Ordering::Acquire);
        item.class = DownloadClass::ALL[desired];
        let queue = &mut state.queues[desired];
        if queue.len() >= self.capacity {
            return Err(DownloadRejection::Full);
        }
        item.queued_ledger.reclassify(item.class);
        queue.push_back(item);
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }

    // The desired class lives in the admission entry, including while a cache
    // probe awaits. Rebalancing here closes the duplicate-during-probe race.
    // A full destination defers an upgrade; it never drops an accepted object.
    fn promote(&self, state: &mut State) {
        for source in 1..4 {
            let mut index = 0;
            while index < state.queues[source].len() {
                let desired = state.queues[source][index]
                    .reservation
                    .requested_class
                    .load(Ordering::Acquire);
                if desired < source && state.queues[desired].len() < self.capacity {
                    let mut item = state.queues[source]
                        .remove(index)
                        .expect("checked queue index");
                    item.class = DownloadClass::ALL[desired];
                    item.queued_ledger.reclassify(item.class);
                    event(item.class, "upgraded", 0);
                    state.queues[desired].push_back(item);
                } else {
                    index += 1;
                }
            }
        }
    }

    pub(super) async fn pop(&self, class: DownloadClass) -> Option<FileInfo> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let mut state = self.state.lock();
                if state.closed {
                    return None;
                }
                self.promote(&mut state);
                if let Some(item) = state.queues[class as usize].pop_front() {
                    drop(state);
                    self.changed.notify_waiters();
                    return Some(item);
                }
            }
            changed.await;
        }
    }

    pub(super) fn wake(&self) {
        self.changed.notify_waiters();
    }

    pub(super) fn close(&self) {
        let mut state = self.state.lock();
        state.closed = true;
        for queue in &mut state.queues {
            queue.clear();
        }
        drop(state);
        self.changed.notify_waiters();
    }
}

pub(super) static QUEUE: Lazy<ClassQueue> = Lazy::new(|| {
    ClassQueue::new(
        get_config()
            .limit
            .file_download_class_queue_capacity
            .clamp(1, 10000),
    )
});

fn worker_shares(total: usize, requested: [usize; 3]) -> [usize; 4] {
    let total = total.clamp(4, 128);
    let demand = requested[0].clamp(1, 32).min(total - 3);
    let sidecar = requested[1].clamp(1, 32).min(total - demand - 2);
    let broadcast = requested[2].clamp(1, 32).min(total - demand - sidecar - 1);
    [
        demand,
        sidecar,
        broadcast,
        total - demand - sidecar - broadcast,
    ]
}

pub(super) fn run() {
    let cfg = get_config();
    let shares = worker_shares(
        cfg.limit.file_download_thread_num,
        [
            cfg.limit.file_download_demand_workers,
            cfg.limit.file_download_sidecar_workers,
            cfg.limit.file_download_broadcast_workers,
        ],
    );
    for (class, count) in DownloadClass::ALL.into_iter().zip(shares) {
        for thread in 0..count {
            tokio::spawn(super::until_shutdown(async move {
                while let Some(file) = QUEUE.pop(class).await {
                    super::process_download(thread, file).await;
                }
            }));
        }
    }
}

fn enabled() -> bool {
    get_config().limit.file_download_ledger_enabled
}

fn gauge(class: DownloadClass, state: &'static str, delta: i64) {
    if enabled() {
        metrics::FILE_DOWNLOAD_LEDGER_ITEMS
            .with_label_values(&[class.label(), state])
            .add(delta);
    }
}

fn event(class: DownloadClass, outcome: &'static str, bytes: u64) {
    if enabled() {
        metrics::FILE_DOWNLOAD_LEDGER_EVENTS
            .with_label_values(&[class.label(), outcome])
            .inc();
        metrics::FILE_DOWNLOAD_LEDGER_BYTES
            .with_label_values(&[class.label(), outcome])
            .inc_by(bytes);
    }
}

pub(super) fn record_admission(class: DownloadClass, outcome: QueueDownloadOutcome, bytes: u64) {
    event(class, "attempts", bytes);
    let label = match outcome {
        QueueDownloadOutcome::Accepted { .. } => "accepted",
        QueueDownloadOutcome::Deduplicated => "deduplicated",
        QueueDownloadOutcome::Cached => "cached",
        QueueDownloadOutcome::Skipped => "skipped",
        QueueDownloadOutcome::Rejected(DownloadRejection::InvalidSize) => "rejected_invalid_size",
        QueueDownloadOutcome::Rejected(DownloadRejection::TooLarge) => "rejected_too_large",
        QueueDownloadOutcome::Rejected(DownloadRejection::Full) => "rejected_full",
        QueueDownloadOutcome::Rejected(DownloadRejection::Closed) => "rejected_closed",
    };
    event(class, label, bytes);
}

pub(super) fn record_completion(
    class: DownloadClass,
    outcome: &'static str,
    bytes: usize,
    elapsed: Duration,
) {
    event(class, outcome, bytes as u64);
    time(class, "download", elapsed);
}

pub(super) fn time(class: DownloadClass, phase: &'static str, elapsed: Duration) {
    if enabled() {
        metrics::FILE_DOWNLOAD_LEDGER_SECONDS
            .with_label_values(&[class.label(), phase])
            .observe(elapsed.as_secs_f64());
    }
}

/// Keep gauges balanced even when a rejected send drops its owned item.
pub(super) struct QueuedLedger(Option<DownloadClass>);
impl QueuedLedger {
    pub(super) fn new(class: DownloadClass) -> Self {
        gauge(class, "queued", 1);
        Self(Some(class))
    }
    fn reclassify(&mut self, class: DownloadClass) {
        if let Some(previous) = self.0.replace(class) {
            gauge(previous, "queued", -1);
        }
        gauge(class, "queued", 1);
    }
    pub(super) fn start(mut self) {
        if let Some(class) = self.0.take() {
            gauge(class, "queued", -1);
        }
    }
}
impl Drop for QueuedLedger {
    fn drop(&mut self) {
        if let Some(class) = self.0 {
            gauge(class, "queued", -1);
        }
    }
}

pub(super) struct ActiveLedger {
    class: DownloadClass,
    finished: bool,
}
impl ActiveLedger {
    pub(super) fn new(class: DownloadClass, queued: Duration) -> Self {
        gauge(class, "active", 1);
        time(class, "queue", queued);
        Self {
            class,
            finished: false,
        }
    }
    pub(super) fn finish(&mut self) {
        self.finished = true;
    }
}
impl Drop for ActiveLedger {
    fn drop(&mut self) {
        gauge(self.class, "active", -1);
        if !self.finished {
            event(self.class, "cancelled_active", 0);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::cache::file_downloader::{DownloadAdmission, tests::queued_item};

    #[tokio::test]
    async fn class_queue_dedupe_upgrade_and_fifo() {
        let admission = Arc::new(DownloadAdmission::new(100, 10));
        let queue = ClassQueue::new(2);
        queue.push(queued_item(&admission, "bulk", 10)).unwrap();
        queue.push(queued_item(&admission, "bulk2", 10)).unwrap();
        assert_eq!(
            queue.push(queued_item(&admission, "overflow", 10)),
            Err(DownloadRejection::Full)
        );
        assert!(matches!(
            admission.reserve_charged("bulk", 10, 10, DownloadClass::DemandSmall, 0),
            Err(QueueDownloadOutcome::Deduplicated)
        ));
        let demand = queue.pop(DownloadClass::DemandSmall).await.unwrap();
        assert_eq!(demand.reservation.file.as_ref(), "bulk");
        assert_eq!(
            queue
                .pop(DownloadClass::QueryBulk)
                .await
                .unwrap()
                .reservation
                .file
                .as_ref(),
            "bulk2"
        );
        assert_eq!(admission.state.lock().files.len(), 1);
        drop(demand);
        assert!(admission.state.lock().files.is_empty());
    }

    #[tokio::test]
    async fn upgrade_during_probe_and_full_destination_retains_work() {
        let admission = Arc::new(DownloadAdmission::new(100, 10));
        let queue = ClassQueue::new(1);
        let first = queued_item(&admission, "first", 10);
        first
            .reservation
            .requested_class
            .store(0, Ordering::Release);
        queue.push(first).unwrap();
        queue.push(queued_item(&admission, "bulk", 10)).unwrap();
        assert!(
            admission
                .reserve_charged("bulk", 10, 10, DownloadClass::DemandSmall, 0)
                .is_err()
        );
        assert_eq!(
            queue
                .pop(DownloadClass::DemandSmall)
                .await
                .unwrap()
                .reservation
                .file
                .as_ref(),
            "first"
        );
        assert_eq!(
            queue
                .pop(DownloadClass::DemandSmall)
                .await
                .unwrap()
                .reservation
                .file
                .as_ref(),
            "bulk"
        );
        let pending_probe = queued_item(&admission, "probe", 10);
        assert!(
            admission
                .reserve_charged("probe", 10, 10, DownloadClass::UsedSidecar, 0)
                .is_err()
        );
        queue.push(pending_probe).unwrap();
        assert_eq!(
            queue
                .pop(DownloadClass::UsedSidecar)
                .await
                .unwrap()
                .reservation
                .file
                .as_ref(),
            "probe"
        );
    }

    #[tokio::test]
    async fn class_shutdown_drains_keys_and_wakes_idle_workers() {
        let admission = Arc::new(DownloadAdmission::new(100, 10));
        let queue = Arc::new(ClassQueue::new(1));
        queue.push(queued_item(&admission, "pending", 10)).unwrap();
        let waiter = tokio::spawn({
            let queue = queue.clone();
            async move { queue.pop(DownloadClass::DemandSmall).await }
        });
        tokio::task::yield_now().await;
        queue.close();
        assert!(
            tokio::time::timeout(Duration::from_secs(1), waiter)
                .await
                .unwrap()
                .unwrap()
                .is_none()
        );
        assert!(admission.state.lock().files.is_empty());
        assert_eq!(
            queue.push(queued_item(&admission, "closed", 10)),
            Err(DownloadRejection::Closed)
        );
        assert!(admission.state.lock().files.is_empty());
    }

    #[test]
    fn class_worker_shares_are_bounded_and_cannot_starve() {
        assert_eq!(worker_shares(32, [4, 4, 8]), [4, 4, 8, 16]);
        for total in [0, 1, 4, 8, 32, 128, usize::MAX] {
            let shares = worker_shares(total, [usize::MAX; 3]);
            assert_eq!(shares.iter().sum::<usize>(), total.clamp(4, 128));
            assert!(shares.iter().all(|share| *share > 0));
        }
    }
}
