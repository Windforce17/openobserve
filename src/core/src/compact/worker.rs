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
use tokio::sync::{Mutex, mpsc};

#[derive(Clone)]
pub struct MergeBatch {
    pub batch_id: usize,
    pub org_id: String,
    pub stream_type: StreamType,
    pub stream_name: String,
    pub prefix: String,
    pub files: Vec<FileKey>,
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

/// Stop-guard for one claimed job's lease heartbeat. The heartbeat task
/// spawns at CLAIM time (see `run_merge`) and keeps refreshing the job's
/// `updated_at` every `ttl_secs` until EVERY clone of the guard has dropped
/// — the guard rides inside [`MergeJob`] through the scheduler channel, so
/// the lease stays covered while the job sits buffered waiting for a worker
/// and while `merge_by_stream` runs, one continuous window from claim to
/// completion. Without it a job parked in the capacity-1 channel behind a
/// long merge had NO heartbeat, `check_running_jobs` re-pended it, and a
/// second node merged the same hour concurrently (permanent duplicate rows —
/// 2026-07-30 audit).
#[derive(Clone)]
pub struct JobLeaseGuard {
    _stop: mpsc::Sender<()>,
}

impl JobLeaseGuard {
    pub fn spawn(job_id: i64, ttl_secs: u64) -> Self {
        let (tx, mut rx) = mpsc::channel::<()>(1);
        tokio::task::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(tokio::time::Duration::from_secs(ttl_secs)) => {}
                    _ = rx.recv() => {
                        log::debug!("[COMPACTOR] job {job_id} lease heartbeat stopped");
                        return;
                    }
                }
                if let Err(e) = infra::file_list::update_running_jobs(&[job_id]).await {
                    log::error!("[COMPACTOR] job {job_id} lease heartbeat update failed: {e}");
                }
            }
        });
        Self { _stop: tx }
    }
}

#[derive(Clone)]
pub struct MergeJob {
    pub org_id: String,
    pub stream_type: StreamType,
    pub stream_name: String,
    pub job_id: i64,
    pub offset: i64,
    /// Claim-time lease heartbeat (see [`JobLeaseGuard`]); dropping the last
    /// clone — the worker's, after `merge_by_stream` returns — stops it.
    pub lease: JobLeaseGuard,
}

/// JobScheduler is a worker that processes jobs
pub struct JobScheduler {
    num: usize,
    rx: Arc<Mutex<mpsc::Receiver<MergeJob>>>,
    tx: mpsc::Sender<MergeJob>,
    worker_tx: mpsc::Sender<(MergeSender, MergeBatch)>,
}

impl JobScheduler {
    pub fn new(num: usize, worker_tx: mpsc::Sender<(MergeSender, MergeBatch)>) -> Self {
        let (tx, rx) = mpsc::channel::<MergeJob>(1);
        let rx = Arc::new(Mutex::new(rx));
        Self {
            num,
            rx,
            tx,
            worker_tx,
        }
    }

    pub fn tx(&self) -> mpsc::Sender<MergeJob> {
        self.tx.clone()
    }

    pub fn run(&mut self) -> Result<(), anyhow::Error> {
        for thread_id in 0..self.num {
            let rx = self.rx.clone();
            let worker_tx = self.worker_tx.clone();
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
                            // `job.lease` is the claim-time heartbeat guard
                            // (see run_merge): holding `job` through
                            // merge_by_stream keeps the lease refreshed until
                            // the merge — commits included — has finished;
                            // it drops with `job` at the end of this arm.
                            if let Err(e) = super::merge::merge_by_stream(
                                worker_tx.clone(),
                                &job.org_id,
                                job.stream_type,
                                &job.stream_name,
                                job.job_id,
                                job.offset,
                            )
                            .await
                            {
                                log::error!(
                                    "[COMPACTOR:SCHEDULER:{thread_id}] merge_by_stream [{}/{}/{}] error: {e}",
                                    job.org_id,
                                    job.stream_type,
                                    job.stream_name,
                                );
                            }
                            // release locked stream
                            let key = format!(
                                "{}/{}/{}",
                                job.org_id,
                                job.stream_type.as_str(),
                                job.stream_name
                            );
                            crate::service::db::compact::stream::clear_running(&key);
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
    fn test_job_scheduler_new_and_tx() {
        let (worker_tx, _rx) = mpsc::channel::<(MergeSender, MergeBatch)>(1);
        let scheduler = JobScheduler::new(3, worker_tx);
        let tx = scheduler.tx();
        drop(tx);
    }

    #[test]
    fn test_job_scheduler_multiple_tx_clones() {
        let (worker_tx, _rx) = mpsc::channel::<(MergeSender, MergeBatch)>(1);
        let scheduler = JobScheduler::new(1, worker_tx);
        let tx1 = scheduler.tx();
        let tx2 = scheduler.tx();
        drop(tx1);
        drop(tx2);
    }

    /// Heartbeat-from-claim lifetime (2026-07-30 audit): a claimed job
    /// PARKED in the scheduler channel — no worker has dequeued it — must
    /// keep its `updated_at` fresh so `check_running_jobs` cannot re-pend
    /// it onto another node mid-lease; once the worker-side owner drops the
    /// job (merge finished), the heartbeat must stop and the normal timeout
    /// path takes over. Sqlite-backed through the real file_list job API.
    #[tokio::test]
    async fn test_job_lease_guard_covers_channel_parked_job() {
        use crate::compact::jobs_test_support::retry_busy;
        let _guard = crate::compact::jobs_test_support::setup().await;
        let run = config::utils::time::now_micros();
        let org = format!("leaseorg{run}");
        let stream = format!("leasestream{run}");
        let job_id = retry_busy("add_job", || {
            infra::file_list::add_job(&org, StreamType::Logs, &stream, run)
        })
        .await;
        assert!(job_id > 0);

        // claim it, as run_merge's get_pending_jobs does. The claim is
        // table-wide, so use a large limit, keep our job and restore any
        // stranger rows untouched-in-effect (back to pending).
        let claimed = retry_busy("claim", || {
            infra::file_list::get_pending_jobs("lease-test-node", 10_000, false)
        })
        .await;
        assert!(
            claimed.iter().any(|j| j.id == job_id),
            "the fresh job must be claimable"
        );
        let strangers = claimed
            .iter()
            .map(|j| j.id)
            .filter(|id| *id != job_id)
            .collect::<Vec<_>>();
        if !strangers.is_empty() {
            retry_busy("restore stranger jobs", || {
                infra::file_list::set_job_pending(&strangers, 0, None)
            })
            .await;
        }

        // heartbeat-from-claim with a 1s tick, then park the job in a tiny
        // channel with no worker consuming it (the slow-worker scenario)
        let lease = JobLeaseGuard::spawn(job_id, 1);
        let (tx, mut rx) = mpsc::channel::<MergeJob>(1);
        tx.send(MergeJob {
            org_id: org.clone(),
            stream_type: StreamType::Logs,
            stream_name: stream.clone(),
            job_id,
            offset: run,
            lease,
        })
        .await
        .expect("park job in channel");

        // several heartbeats fire while the job sits buffered
        tokio::time::sleep(tokio::time::Duration::from_secs(4)).await;

        // a janitor pass with a 3s staleness threshold must NOT re-pend it:
        // the parked job's updated_at is at most ~1s old
        let stale_before = config::utils::time::now_micros() - 3_000_000;
        retry_busy("check_running_jobs", || {
            infra::file_list::check_running_jobs(stale_before)
        })
        .await;
        let reclaimable = retry_busy("probe claim", || {
            infra::file_list::get_pending_jobs("lease-thief", 10_000, false)
        })
        .await;
        assert!(
            !reclaimable.iter().any(|j| j.id == job_id),
            "a channel-parked job with a live lease guard must stay running"
        );
        if !reclaimable.is_empty() {
            let ids = reclaimable.iter().map(|j| j.id).collect::<Vec<_>>();
            retry_busy("restore probe-claimed jobs", || {
                infra::file_list::set_job_pending(&ids, 0, None)
            })
            .await;
        }

        // the worker dequeues and finishes: dropping the job releases the
        // last guard clone, which stops the heartbeat
        let job = rx.recv().await.expect("job parked in channel");
        drop(job);
        drop(tx);
        tokio::time::sleep(tokio::time::Duration::from_secs(4)).await;

        // the same janitor threshold now reclaims the silent job
        let stale_before = config::utils::time::now_micros() - 3_000_000;
        retry_busy("check_running_jobs after drop", || {
            infra::file_list::check_running_jobs(stale_before)
        })
        .await;
        let reclaimable = retry_busy("reclaim", || {
            infra::file_list::get_pending_jobs("lease-thief", 10_000, false)
        })
        .await;
        assert!(
            reclaimable.iter().any(|j| j.id == job_id),
            "after the guard drops the heartbeat must stop and the lease must expire"
        );

        // cleanup: our job done, strangers back to pending
        let done_ids = [job_id];
        retry_busy("cleanup set_job_done", || {
            infra::file_list::set_job_done(&done_ids)
        })
        .await;
        let strangers = reclaimable
            .iter()
            .map(|j| j.id)
            .filter(|id| *id != job_id)
            .collect::<Vec<_>>();
        if !strangers.is_empty() {
            retry_busy("restore stranger jobs", || {
                infra::file_list::set_job_pending(&strangers, 0, None)
            })
            .await;
        }
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
        let (tx, rx) = mpsc::channel::<(MergeSender, MergeBatch)>(1);
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
                            match super::merge::merge_files(
                                thread_id,
                                &msg.org_id,
                                msg.stream_type,
                                &msg.stream_name,
                                &msg.prefix,
                                &msg.files,
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
