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

use std::sync::Arc;

use config::{
    cluster::is_offline,
    meta::stream::{FileKey, StreamType},
};
use infra::file_list::FileListJobStatus;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, TryAcquireError, mpsc};

#[derive(Clone)]
pub struct MergeBatch {
    pub batch_id: usize,
    pub org_id: String,
    pub stream_type: StreamType,
    pub stream_name: String,
    pub prefix: String,
    pub files: Vec<FileKey>,
    /// Shared by every batch produced for one claimed compaction job. Lease
    /// loss or shutdown marks it so queued work can stop before doing more
    /// I/O/CPU and active phases can cooperatively exit at their next
    /// boundary.
    pub cancel: MergeCancellation,
}

#[derive(Clone, Default)]
pub struct MergeCancellation(crate::service::vix::core_writer::VixMergeCancellation);

impl MergeCancellation {
    #[inline]
    pub fn cancel(&self) {
        self.0.cancel();
    }

    #[inline]
    pub fn is_cancelled(&self) -> bool {
        if is_offline() {
            // Mirror process shutdown into the dependency-neutral token used
            // by blocking VIX workers. Once observed, cancellation remains
            // monotonic even if the outer async worker is no longer polled.
            self.0.cancel();
        }
        self.0.is_cancelled()
    }

    #[inline]
    pub fn vix_token(&self) -> crate::service::vix::core_writer::VixMergeCancellation {
        self.0.clone()
    }

    pub fn check(&self, context: &str) -> Result<(), anyhow::Error> {
        if self.is_cancelled() {
            Err(anyhow::anyhow!(
                "compaction cancelled before {context}: job lease was lost or node is shutting down"
            ))
        } else {
            Ok(())
        }
    }
}

pub struct MergeResult {
    pub batch_id: usize,
    pub new_file: FileKey,
}

/// Carries one merged batch back to `merge_by_stream`:
/// `(batch_id, new_files, merged_files)` where `merged_files` is EXACTLY the
/// input files whose rows made it into `new_files` — the only files the
/// caller may delete. Batch inputs missing from it (size-mismatch skips,
/// mid-batch size-budget cuts, dropped survivors) stay live in the file_list
/// and retry on a later cycle.
pub type MergeSender = mpsc::Sender<Result<(usize, Vec<FileKey>, Vec<FileKey>), anyhow::Error>>;

fn lease_refresh_expired(
    unconfirmed_for: tokio::time::Duration,
    lease_timeout: tokio::time::Duration,
) -> bool {
    unconfirmed_for >= lease_timeout
}

async fn wait_for_job_cancellation(cancel: &MergeCancellation) {
    while !cancel.is_cancelled() {
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }
}

/// Stop-guard for one claimed job's lease heartbeat. The heartbeat begins at
/// claim time and stops when the job leaves its scheduler slot. Transient
/// storage errors are tolerated only until the full lease timeout has elapsed
/// since the last confirmed touch; work is then cancelled before a stale
/// recovery can safely hand the generation to another owner.
pub struct JobLeaseGuard {
    _stop: mpsc::Sender<()>,
}

impl JobLeaseGuard {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        job_id: i64,
        node: String,
        lease_generation: i64,
        expected_status: FileListJobStatus,
        heartbeat_secs: u64,
        lease_timeout_secs: u64,
        cancel: MergeCancellation,
    ) -> Self {
        let (tx, mut rx) = mpsc::channel::<()>(1);
        tokio::task::spawn(async move {
            let heartbeat = tokio::time::Duration::from_secs(heartbeat_secs.max(1));
            let lease_timeout = tokio::time::Duration::from_secs(lease_timeout_secs.max(1));
            let mut last_confirmed = tokio::time::Instant::now();
            loop {
                let deadline = last_confirmed + lease_timeout;
                tokio::select! {
                    _ = tokio::time::sleep(heartbeat) => {}
                    _ = tokio::time::sleep_until(deadline) => {
                        log::error!(
                            "[COMPACTOR] lease refresh timed out job_id={job_id} generation={lease_generation}"
                        );
                        cancel.cancel();
                        return;
                    }
                    _ = rx.recv() => {
                        log::debug!(
                            "[COMPACTOR] lease heartbeat stopped job_id={job_id} generation={lease_generation}"
                        );
                        return;
                    }
                    _ = wait_for_job_cancellation(&cancel) => {
                        log::debug!(
                            "[COMPACTOR] lease heartbeat cancelled job_id={job_id} generation={lease_generation}"
                        );
                        return;
                    }
                }

                // Bound the touch call itself by the remaining confirmed
                // lease lifetime. A hung/transient store must not let stale
                // recovery reassign while this worker continues.
                let touch = tokio::select! {
                    result = infra::file_list::touch_job_lease(
                        job_id,
                        &node,
                        lease_generation,
                        expected_status,
                    ) => Some(result),
                    _ = tokio::time::sleep_until(deadline) => None,
                    _ = rx.recv() => return,
                    _ = wait_for_job_cancellation(&cancel) => return,
                };
                let Some(touch) = touch else {
                    log::error!(
                        "[COMPACTOR] lease refresh timed out job_id={job_id} generation={lease_generation}"
                    );
                    cancel.cancel();
                    return;
                };
                match touch {
                    Ok(true) => {
                        last_confirmed = tokio::time::Instant::now();
                        log::debug!(
                            "[COMPACTOR] lease heartbeat renewed job_id={job_id} generation={lease_generation}"
                        );
                    }
                    Ok(false) => {
                        log::warn!(
                            "[COMPACTOR] lease ownership lost job_id={job_id} generation={lease_generation}"
                        );
                        cancel.cancel();
                        return;
                    }
                    Err(e) => {
                        let unconfirmed_for = last_confirmed.elapsed();
                        log::error!(
                            "[COMPACTOR] lease heartbeat failed job_id={job_id} generation={lease_generation} unconfirmed_ms={}: {e}",
                            unconfirmed_for.as_millis(),
                        );
                        if lease_refresh_expired(unconfirmed_for, lease_timeout) {
                            cancel.cancel();
                            return;
                        }
                    }
                }
            }
        });
        Self { _stop: tx }
    }
}

/// A claimed merge job and its single scheduler-capacity reservation.
///
/// This type deliberately is not `Clone`: duplicating it would separate one
/// database claim from its one local scheduler permit.
pub struct MergeJob {
    pub org_id: String,
    pub stream_type: StreamType,
    pub stream_name: String,
    pub job_id: i64,
    pub offset: i64,
    pub lease_generation: i64,
    pub cancel: MergeCancellation,
    pub lease: JobLeaseGuard,
    _slot: OwnedSemaphorePermit,
}

impl MergeJob {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        org_id: String,
        stream_type: StreamType,
        stream_name: String,
        job_id: i64,
        offset: i64,
        lease_generation: i64,
        cancel: MergeCancellation,
        lease: JobLeaseGuard,
        slot: OwnedSemaphorePermit,
    ) -> Self {
        Self {
            org_id,
            stream_type,
            stream_name,
            job_id,
            offset,
            lease_generation,
            cancel,
            lease,
            _slot: slot,
        }
    }
}

/// Cloneable producer handle for one scheduler lane. The semaphore is lane
/// local, so the live lane can never borrow backlog capacity (or vice versa).
#[derive(Clone)]
pub struct JobSchedulerHandle {
    tx: mpsc::Sender<MergeJob>,
    slots: Arc<Semaphore>,
    capacity: usize,
}

impl JobSchedulerHandle {
    /// Reserve up to `limit` currently-free slots without waiting. Callers do
    /// this before claiming from the database, so every returned claim can be
    /// paired one-for-one with a permit.
    pub fn reserve(&self, limit: usize) -> Vec<OwnedSemaphorePermit> {
        let mut permits = Vec::with_capacity(limit.min(self.free_slots()));
        for _ in 0..limit {
            match self.slots.clone().try_acquire_owned() {
                Ok(permit) => permits.push(permit),
                Err(TryAcquireError::NoPermits | TryAcquireError::Closed) => break,
            }
        }
        permits
    }

    #[inline]
    pub fn free_slots(&self) -> usize {
        self.slots.available_permits()
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub async fn send(&self, job: MergeJob) -> Result<(), mpsc::error::SendError<MergeJob>> {
        self.tx.send(job).await
    }
}

/// JobScheduler is a worker that processes jobs.
pub struct JobScheduler {
    num: usize,
    rx: Arc<Mutex<mpsc::Receiver<MergeJob>>>,
    handle: JobSchedulerHandle,
    worker_tx: mpsc::Sender<(MergeSender, MergeBatch)>,
}

impl JobScheduler {
    pub fn new(num: usize, worker_tx: mpsc::Sender<(MergeSender, MergeBatch)>) -> Self {
        let capacity = num.max(1);
        let (tx, rx) = mpsc::channel::<MergeJob>(capacity);
        let slots = Arc::new(Semaphore::new(capacity));
        Self {
            num: capacity,
            rx: Arc::new(Mutex::new(rx)),
            handle: JobSchedulerHandle {
                tx,
                slots,
                capacity,
            },
            worker_tx,
        }
    }

    pub fn handle(&self) -> JobSchedulerHandle {
        self.handle.clone()
    }

    pub fn run(&mut self) -> Result<(), anyhow::Error> {
        for thread_id in 0..self.num {
            let rx = self.rx.clone();
            let worker_tx = self.worker_tx.clone();
            let handle = self.handle.clone();
            tokio::spawn(async move {
                loop {
                    if is_offline() {
                        break;
                    }
                    let ret = rx.lock().await.recv().await;
                    match ret {
                        None => {
                            log::debug!(
                                "[COMPACTOR:SCHEDULER:{thread_id}] Receiving job channel is closed"
                            );
                            break;
                        }
                        Some(job) => {
                            log::info!(
                                "[COMPACTOR] merge job started job_id={} generation={} active_slots={} free_slots={}",
                                job.job_id,
                                job.lease_generation,
                                handle.capacity().saturating_sub(handle.free_slots()),
                                handle.free_slots(),
                            );
                            if let Err(e) = super::merge::merge_by_stream(
                                worker_tx.clone(),
                                &job.org_id,
                                job.stream_type,
                                &job.stream_name,
                                job.job_id,
                                job.lease_generation,
                                job.offset,
                                &job.cancel,
                            )
                            .await
                            {
                                log::error!(
                                    "[COMPACTOR:SCHEDULER:{thread_id}] merge_by_stream [{}/{}/{}] job_id={} generation={} error: {e}",
                                    job.org_id,
                                    job.stream_type,
                                    job.stream_name,
                                    job.job_id,
                                    job.lease_generation,
                                );
                                match infra::file_list::set_job_pending_owned(
                                    job.job_id,
                                    &config::cluster::LOCAL_NODE.uuid,
                                    job.lease_generation,
                                )
                                .await
                                {
                                    Ok(true) => log::info!(
                                        "[COMPACTOR] merge job released job_id={} generation={} outcome=retry",
                                        job.job_id,
                                        job.lease_generation,
                                    ),
                                    Ok(false) => {
                                        job.cancel.cancel();
                                        log::warn!(
                                            "[COMPACTOR] merge job release missed ownership job_id={} generation={}",
                                            job.job_id,
                                            job.lease_generation,
                                        );
                                    }
                                    Err(release_error) => log::error!(
                                        "[COMPACTOR] merge job release failed job_id={} generation={}: {release_error}",
                                        job.job_id,
                                        job.lease_generation,
                                    ),
                                }
                            }
                            let key = format!(
                                "{}/{}/{}",
                                job.org_id,
                                job.stream_type.as_str(),
                                job.stream_name
                            );
                            crate::service::db::compact::stream::clear_running(&key);
                            drop(job);
                            log::info!(
                                "[COMPACTOR] merge scheduler slot released active_slots={} free_slots={}",
                                handle.capacity().saturating_sub(handle.free_slots()),
                                handle.free_slots(),
                            );
                        }
                    }
                }
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod job_scheduler_tests {
    use tokio::sync::mpsc;

    use super::*;

    #[test]
    fn scheduler_handles_share_one_capacity_pool() {
        let (worker_tx, _rx) = mpsc::channel::<(MergeSender, MergeBatch)>(1);
        let scheduler = JobScheduler::new(3, worker_tx);
        let first = scheduler.handle();
        let second = scheduler.handle();

        let held = first.reserve(usize::MAX);
        assert_eq!(held.len(), 3);
        assert_eq!(second.free_slots(), 0);
        assert!(second.reserve(1).is_empty());

        drop(held);
        assert_eq!(second.free_slots(), 3);
    }

    #[test]
    fn scheduler_permits_release_individually() {
        let (worker_tx, _rx) = mpsc::channel::<(MergeSender, MergeBatch)>(1);
        let scheduler = JobScheduler::new(2, worker_tx);
        let handle = scheduler.handle();
        let mut held = handle.reserve(2);

        assert_eq!(handle.free_slots(), 0);
        drop(held.pop());
        assert_eq!(handle.free_slots(), 1);
        assert_eq!(handle.reserve(usize::MAX).len(), 1);
    }

    #[test]
    fn merge_cancellation_is_shared_and_monotonic() {
        let cancellation = MergeCancellation::default();
        let batch_cancellation = cancellation.clone();
        assert!(!batch_cancellation.is_cancelled());

        cancellation.cancel();

        assert!(batch_cancellation.is_cancelled());
        assert!(batch_cancellation.check("test boundary").is_err());
    }

    #[test]
    fn transient_heartbeat_errors_expire_at_lease_timeout() {
        let timeout = tokio::time::Duration::from_secs(120);
        assert!(!lease_refresh_expired(
            timeout - tokio::time::Duration::from_nanos(1),
            timeout,
        ));
        assert!(lease_refresh_expired(timeout, timeout));
        assert!(lease_refresh_expired(
            timeout + tokio::time::Duration::from_secs(1),
            timeout,
        ));
    }

    #[tokio::test]
    async fn heartbeat_cancellation_wait_stops_promptly() {
        let cancel = MergeCancellation::default();
        let trigger = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
            trigger.cancel();
        });
        tokio::time::timeout(
            tokio::time::Duration::from_secs(1),
            wait_for_job_cancellation(&cancel),
        )
        .await
        .expect("heartbeat cancellation observer must not wait for the heartbeat interval");
    }
}

/// MergeWorker is a worker that merges files
pub struct MergeWorker {
    num: usize,
    rx: Arc<Mutex<mpsc::Receiver<(MergeSender, MergeBatch)>>>,
    tx: mpsc::Sender<(MergeSender, MergeBatch)>,
}

impl MergeWorker {
    pub fn new(num: usize) -> Self {
        // keep the workers fed: a fat job submits hundreds of batches; a
        // capacity-1 channel serialized submission behind the slowest batch
        let num = num.max(1);
        let (tx, rx) = mpsc::channel::<(MergeSender, MergeBatch)>(num * 2);
        let rx = Arc::new(Mutex::new(rx));
        Self { num, rx, tx }
    }

    pub fn tx(&self) -> mpsc::Sender<(MergeSender, MergeBatch)> {
        self.tx.clone()
    }

    pub fn run(&mut self) -> Result<(), anyhow::Error> {
        for thread_id in 0..self.num {
            let rx = self.rx.clone();
            tokio::spawn(async move {
                loop {
                    if is_offline() {
                        break;
                    }
                    let ret = rx.lock().await.recv().await;
                    match ret {
                        None => {
                            log::debug!(
                                "[COMPACTOR:WORKER:{thread_id}] Receiving files channel is closed"
                            );
                            break;
                        }
                        Some((tx, msg)) => {
                            if let Err(e) = msg.cancel.check("worker merge start") {
                                if let Err(send_error) = tx.send(Err(e)).await {
                                    log::error!(
                                        "[COMPACTOR:WORKER:{thread_id}] failed to report cancelled batch {}: {send_error}",
                                        msg.batch_id,
                                    );
                                }
                                continue;
                            }
                            match super::merge::merge_files(
                                thread_id,
                                &msg.org_id,
                                msg.stream_type,
                                &msg.stream_name,
                                &msg.prefix,
                                &msg.files,
                                &msg.cancel,
                            )
                            .await
                            {
                                Ok((new_files, merged_files)) => {
                                    // merged_files is the EXACT deletable set;
                                    // dropping it here is what turned partial
                                    // merges into whole-batch deletions
                                    // (2026-07-30 audit)
                                    if let Err(e) =
                                        tx.send(Ok((msg.batch_id, new_files, merged_files))).await
                                    {
                                        log::error!(
                                            "[COMPACTOR:WORKER:{thread_id}] Error sending file to merge_job: {e}"
                                        );
                                    }
                                }
                                Err(e) => {
                                    log::error!(
                                        "[COMPACTOR:WORKER:{thread_id}] Error merging files: stream: {}/{}/{}, err: {}",
                                        msg.org_id,
                                        msg.stream_type,
                                        msg.stream_name,
                                        e
                                    );
                                    if let Err(e) = tx.send(Err(e)).await {
                                        log::error!(
                                            "[COMPACTOR:WORKER:{thread_id}] Error sending error to merge_job: {e}"
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod merge_worker_tests {
    use super::*;

    #[test]
    fn test_merge_worker_new_and_tx() {
        let worker = MergeWorker::new(4);
        let tx = worker.tx();
        drop(tx);
    }

    #[test]
    fn test_merge_worker_new_single() {
        let _worker = MergeWorker::new(1);
    }
}
