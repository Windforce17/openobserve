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
use parking_lot::Mutex;
use tokio::sync::{
    Mutex as AsyncMutex, Notify, OwnedSemaphorePermit, Semaphore, TryAcquireError, mpsc,
};

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

/// Logical compaction lanes sharing one finite set of physical scheduler slots.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MergeAdmissionLane {
    Hot,
    Recent,
    Backlog,
}

impl MergeAdmissionLane {
    const ALL: [Self; 3] = [Self::Hot, Self::Recent, Self::Backlog];

    const fn index(self) -> usize {
        match self {
            Self::Hot => 0,
            Self::Recent => 1,
            Self::Backlog => 2,
        }
    }
}

#[derive(Clone, Copy)]
struct LanePolicy {
    entitlement: usize,
    active: bool,
    in_use: usize,
    fair_credit: i64,
}

struct AdmissionState {
    lanes: [LanePolicy; 3],
    physical_cursor: usize,
}

struct PhysicalPool {
    owner: MergeAdmissionLane,
    scheduler: JobSchedulerHandle,
}

/// Work-conserving admission across the hot, recent, and backlog lanes.
///
/// Each configured physical scheduler contributes its capacity as that lane's
/// entitlement. A lane below its entitlement is always eligible before excess
/// work. Once every active lane is at entitlement, excess slots use smooth
/// weighted round-robin with the entitlements as weights.
pub struct LaneAdmission {
    state: Mutex<AdmissionState>,
    pools: Vec<PhysicalPool>,
    capacity: usize,
    released: Notify,
}

impl LaneAdmission {
    pub fn new(pools: Vec<(MergeAdmissionLane, JobSchedulerHandle)>) -> Arc<Self> {
        let mut lanes = [LanePolicy {
            entitlement: 0,
            active: false,
            in_use: 0,
            fair_credit: 0,
        }; 3];
        let mut physical = Vec::with_capacity(pools.len());
        let mut capacity = 0usize;
        for (owner, scheduler) in pools {
            let entitlement = scheduler.capacity();
            lanes[owner.index()] = LanePolicy {
                entitlement,
                active: true,
                in_use: 0,
                fair_credit: 0,
            };
            capacity = capacity.saturating_add(entitlement);
            physical.push(PhysicalPool { owner, scheduler });
        }
        Arc::new(Self {
            state: Mutex::new(AdmissionState {
                lanes,
                physical_cursor: 0,
            }),
            pools: physical,
            capacity,
            released: Notify::new(),
        })
    }

    pub fn handle(self: &Arc<Self>, lane: MergeAdmissionLane) -> LaneSchedulerHandle {
        assert!(
            self.state.lock().lanes[lane.index()].entitlement > 0,
            "lane admission handle requires a configured physical pool"
        );
        LaneSchedulerHandle {
            admission: self.clone(),
            lane,
        }
    }

    fn begin_cycle(&self) {
        let mut state = self.state.lock();
        for lane in &mut state.lanes {
            if lane.entitlement > 0 {
                lane.active = true;
            }
        }
    }

    fn set_exhausted(&self, lane: MergeAdmissionLane) {
        let mut state = self.state.lock();
        let lane = &mut state.lanes[lane.index()];
        lane.active = false;
        lane.fair_credit = 0;
    }

    fn entitlement_choice(state: &AdmissionState) -> Option<MergeAdmissionLane> {
        let mut selected = None;
        let mut largest_deficit = 0usize;
        for lane in MergeAdmissionLane::ALL {
            let policy = state.lanes[lane.index()];
            if !policy.active {
                continue;
            }
            let deficit = policy.entitlement.saturating_sub(policy.in_use);
            if deficit > largest_deficit {
                largest_deficit = deficit;
                selected = Some(lane);
            }
        }
        selected
    }

    fn fair_choice(state: &AdmissionState) -> Option<(MergeAdmissionLane, [i64; 3])> {
        let mut credits = [
            state.lanes[0].fair_credit,
            state.lanes[1].fair_credit,
            state.lanes[2].fair_credit,
        ];
        let mut total_weight = 0i64;
        let mut selected = None;
        for lane in MergeAdmissionLane::ALL {
            let policy = state.lanes[lane.index()];
            if !policy.active || policy.entitlement == 0 {
                continue;
            }
            let weight = i64::try_from(policy.entitlement).unwrap_or(i64::MAX);
            credits[lane.index()] = credits[lane.index()].saturating_add(weight);
            total_weight = total_weight.saturating_add(weight);
            if selected.map_or(true, |current: MergeAdmissionLane| {
                credits[lane.index()] > credits[current.index()]
            }) {
                selected = Some(lane);
            }
        }
        let selected = selected?;
        credits[selected.index()] = credits[selected.index()].saturating_sub(total_weight);
        Some((selected, credits))
    }

    fn acquire_physical(
        &self,
        state: &mut AdmissionState,
        lane: MergeAdmissionLane,
    ) -> Option<(JobSchedulerHandle, OwnedSemaphorePermit)> {
        let preferred = self.pools.iter().position(|pool| pool.owner == lane);
        if let Some(index) = preferred {
            let pool = &self.pools[index];
            if let Some(permit) = pool.scheduler.try_reserve() {
                state.physical_cursor = (index + 1) % self.pools.len();
                return Some((pool.scheduler.clone(), permit));
            }
        }
        for offset in 0..self.pools.len() {
            let index = (state.physical_cursor + offset) % self.pools.len();
            if Some(index) == preferred {
                continue;
            }
            let pool = &self.pools[index];
            if let Some(permit) = pool.scheduler.try_reserve() {
                state.physical_cursor = (index + 1) % self.pools.len();
                return Some((pool.scheduler.clone(), permit));
            }
        }
        None
    }

    fn reserve(
        self: &Arc<Self>,
        lane: MergeAdmissionLane,
        limit: usize,
    ) -> Vec<LaneAdmissionPermit> {
        let mut reservations = Vec::with_capacity(limit.min(self.free_slots()));
        for _ in 0..limit {
            let mut state = self.state.lock();
            let policy = state.lanes[lane.index()];
            if !policy.active {
                break;
            }

            let fair_credits = if let Some(selected) = Self::entitlement_choice(&state) {
                if selected != lane {
                    break;
                }
                None
            } else {
                let Some((selected, credits)) = Self::fair_choice(&state) else {
                    break;
                };
                if selected != lane {
                    break;
                }
                Some(credits)
            };

            let Some((scheduler, physical)) = self.acquire_physical(&mut state, lane) else {
                break;
            };
            state.lanes[lane.index()].in_use += 1;
            if let Some(credits) = fair_credits {
                for (policy, credit) in state.lanes.iter_mut().zip(credits) {
                    policy.fair_credit = credit;
                }
            }
            drop(state);
            reservations.push(LaneAdmissionPermit {
                admission: self.clone(),
                lane,
                scheduler,
                physical: Some(physical),
            });
        }
        reservations
    }

    fn free_slots(&self) -> usize {
        self.pools
            .iter()
            .map(|pool| pool.scheduler.free_slots())
            .sum()
    }

    fn has_active_lanes(&self) -> bool {
        self.state
            .lock()
            .lanes
            .iter()
            .any(|lane| lane.active && lane.entitlement > 0)
    }

    async fn wait_for_release(&self) {
        self.released.notified().await;
    }

    #[cfg(test)]
    fn in_use(&self, lane: MergeAdmissionLane) -> usize {
        self.state.lock().lanes[lane.index()].in_use
    }
}

/// One logical admission and one physical scheduler slot.
///
/// The physical scheduler determines which of the two existing MergeWorker
/// transports executes the job. Because every physical scheduler has exactly
/// one receiver task per permit, borrowed jobs cannot accumulate in a
/// lane-local FIFO behind unrelated work while their database lease ticks.
pub struct LaneAdmissionPermit {
    admission: Arc<LaneAdmission>,
    lane: MergeAdmissionLane,
    scheduler: JobSchedulerHandle,
    physical: Option<OwnedSemaphorePermit>,
}

impl Drop for LaneAdmissionPermit {
    fn drop(&mut self) {
        let mut state = self.admission.state.lock();
        let policy = &mut state.lanes[self.lane.index()];
        policy.in_use = policy
            .in_use
            .checked_sub(1)
            .expect("lane admission count must match its RAII permits");
        drop(state);
        drop(self.physical.take());
        self.admission.released.notify_one();
    }
}

/// A claimed merge job and its single logical and physical capacity reservation.
///
/// This type deliberately is not `Clone`: duplicating it would separate one
/// database claim from its one local scheduler admission.
pub struct MergeJob {
    pub org_id: String,
    pub stream_type: StreamType,
    pub stream_name: String,
    pub job_id: i64,
    pub offset: i64,
    pub lease_generation: i64,
    pub cancel: MergeCancellation,
    pub lease: JobLeaseGuard,
    _slot: LaneAdmissionPermit,
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
        slot: LaneAdmissionPermit,
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

/// Cloneable logical-lane producer backed by the shared admission policy.
#[derive(Clone)]
pub struct LaneSchedulerHandle {
    admission: Arc<LaneAdmission>,
    lane: MergeAdmissionLane,
}

impl LaneSchedulerHandle {
    /// Start a polling cycle by treating every configured lane as potentially
    /// active. An empty or short database claim marks that lane exhausted
    /// again, allowing its unused entitlement to be borrowed.
    pub fn begin_cycle(&self) {
        self.admission.begin_cycle();
    }

    pub fn reserve(&self, limit: usize) -> Vec<LaneAdmissionPermit> {
        self.admission.reserve(self.lane, limit)
    }

    pub fn set_exhausted(&self) {
        self.admission.set_exhausted(self.lane);
    }

    pub fn free_slots(&self) -> usize {
        self.admission.free_slots()
    }

    pub fn capacity(&self) -> usize {
        self.admission.capacity
    }

    pub fn has_active_lanes(&self) -> bool {
        self.admission.has_active_lanes()
    }

    pub async fn wait_for_release(&self) {
        self.admission.wait_for_release().await;
    }

    pub fn lane(&self) -> MergeAdmissionLane {
        self.lane
    }

    pub async fn send(&self, job: MergeJob) -> Result<(), mpsc::error::SendError<MergeJob>> {
        let tx = job._slot.scheduler.tx.clone();
        tx.send(job).await
    }
}

/// Cloneable producer handle for one finite physical scheduler pool.
#[derive(Clone)]
pub struct JobSchedulerHandle {
    tx: mpsc::Sender<MergeJob>,
    slots: Arc<Semaphore>,
    capacity: usize,
}

impl JobSchedulerHandle {
    fn try_reserve(&self) -> Option<OwnedSemaphorePermit> {
        match self.slots.clone().try_acquire_owned() {
            Ok(permit) => Some(permit),
            Err(TryAcquireError::NoPermits | TryAcquireError::Closed) => None,
        }
    }

    #[inline]
    pub fn free_slots(&self) -> usize {
        self.slots.available_permits()
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

/// JobScheduler is a worker that processes jobs.
pub struct JobScheduler {
    num: usize,
    rx: Arc<AsyncMutex<mpsc::Receiver<MergeJob>>>,
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
            rx: Arc::new(AsyncMutex::new(rx)),
            handle: JobSchedulerHandle {
                tx,
                slots,
                capacity,
            },
            worker_tx,
        }
    }

    /// Standalone backlog-only logical handle used by callers that do not
    /// configure hot/recent lanes.
    pub fn handle(&self) -> LaneSchedulerHandle {
        let admission =
            LaneAdmission::new(vec![(MergeAdmissionLane::Backlog, self.physical_handle())]);
        admission.handle(MergeAdmissionLane::Backlog)
    }

    /// Physical pool handle used to assemble a shared multi-lane admission.
    pub fn physical_handle(&self) -> JobSchedulerHandle {
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

    fn admission(
        capacities: [usize; 3],
    ) -> (
        Arc<LaneAdmission>,
        [LaneSchedulerHandle; 3],
        Vec<JobScheduler>,
    ) {
        let (worker_tx, _worker_rx) = mpsc::channel::<(MergeSender, MergeBatch)>(1);
        let schedulers = vec![
            JobScheduler::new(capacities[0], worker_tx.clone()),
            JobScheduler::new(capacities[1], worker_tx.clone()),
            JobScheduler::new(capacities[2], worker_tx),
        ];
        let admission = LaneAdmission::new(vec![
            (MergeAdmissionLane::Hot, schedulers[0].physical_handle()),
            (MergeAdmissionLane::Recent, schedulers[1].physical_handle()),
            (MergeAdmissionLane::Backlog, schedulers[2].physical_handle()),
        ]);
        let handles = [
            admission.handle(MergeAdmissionLane::Hot),
            admission.handle(MergeAdmissionLane::Recent),
            admission.handle(MergeAdmissionLane::Backlog),
        ];
        (admission, handles, schedulers)
    }

    fn reserve_entitlements(
        handles: &[LaneSchedulerHandle; 3],
        capacity: usize,
    ) -> [Vec<LaneAdmissionPermit>; 3] {
        let mut held: [Vec<LaneAdmissionPermit>; 3] = std::array::from_fn(|_| Vec::new());
        while held.iter().map(Vec::len).sum::<usize>() < capacity {
            let before = held.iter().map(Vec::len).sum::<usize>();
            for (lane, lane_held) in handles.iter().zip(&mut held) {
                lane_held.extend(lane.reserve(usize::MAX));
            }
            assert!(
                held.iter().map(Vec::len).sum::<usize>() > before,
                "active entitlement fill must make progress"
            );
        }
        held
    }

    #[test]
    fn all_pending_lanes_receive_four_four_two_guarantees() {
        let (admission, [hot, recent, backlog], _schedulers) = admission([4, 4, 2]);
        let [_hot, _recent, _backlog] = reserve_entitlements(
            &[hot.clone(), recent.clone(), backlog.clone()],
            admission.capacity,
        );

        assert_eq!(admission.in_use(MergeAdmissionLane::Hot), 4);
        assert_eq!(admission.in_use(MergeAdmissionLane::Recent), 4);
        assert_eq!(admission.in_use(MergeAdmissionLane::Backlog), 2);
        assert_eq!(admission.free_slots(), 0);
    }

    #[test]
    fn sole_active_lane_borrows_all_empty_lane_capacity() {
        let (admission, [hot, recent, backlog], _schedulers) = admission([4, 4, 2]);
        let [hot_jobs, recent_jobs, mut backlog_jobs] = reserve_entitlements(
            &[hot.clone(), recent.clone(), backlog.clone()],
            admission.capacity,
        );

        hot.set_exhausted();
        recent.set_exhausted();
        drop(hot_jobs);
        drop(recent_jobs);
        backlog_jobs.extend(backlog.reserve(usize::MAX));

        assert_eq!(backlog_jobs.len(), 10);
        assert_eq!(admission.in_use(MergeAdmissionLane::Backlog), 10);
        assert_eq!(admission.free_slots(), 0);
    }

    #[test]
    fn newly_active_under_guarantee_lane_preempts_next_excess_admission() {
        let (admission, [hot, recent, backlog], _schedulers) = admission([4, 4, 2]);
        let [hot_jobs, recent_jobs, mut backlog_jobs] = reserve_entitlements(
            &[hot.clone(), recent.clone(), backlog.clone()],
            admission.capacity,
        );
        hot.set_exhausted();
        recent.set_exhausted();
        drop(hot_jobs);
        drop(recent_jobs);
        backlog_jobs.extend(backlog.reserve(8));
        assert_eq!(backlog_jobs.len(), 10);

        backlog.begin_cycle();
        drop(backlog_jobs.pop());

        assert!(backlog.reserve(1).is_empty());
        assert_eq!(hot.reserve(1).len(), 1);
    }

    #[test]
    fn excess_admission_is_weighted_fair() {
        let (admission, [hot, recent, backlog], _schedulers) = admission([6, 3, 9]);
        let [mut hot_jobs, mut recent_jobs, backlog_jobs] = reserve_entitlements(
            &[hot.clone(), recent.clone(), backlog.clone()],
            admission.capacity,
        );
        backlog.set_exhausted();
        drop(backlog_jobs);

        for _ in 0..9 {
            hot_jobs.extend(hot.reserve(1));
            recent_jobs.extend(recent.reserve(1));
        }

        assert_eq!(hot_jobs.len(), 12);
        assert_eq!(recent_jobs.len(), 6);
    }

    #[test]
    fn dropping_each_admission_releases_its_slot() {
        let (admission, [hot, recent, backlog], _schedulers) = admission([4, 4, 2]);
        recent.set_exhausted();
        backlog.set_exhausted();
        let mut held = hot.reserve(2);

        assert_eq!(admission.in_use(MergeAdmissionLane::Hot), 2);
        drop(held.pop());
        assert_eq!(admission.in_use(MergeAdmissionLane::Hot), 1);
        assert_eq!(admission.free_slots(), 9);
    }

    #[tokio::test]
    async fn dropping_admission_wakes_refill_waiter() {
        let (_admission, [hot, recent, backlog], _schedulers) = admission([4, 4, 2]);
        recent.set_exhausted();
        backlog.set_exhausted();
        let mut held = hot.reserve(1);
        let waiter = hot.clone();
        let waiting = tokio::spawn(async move {
            waiter.wait_for_release().await;
        });
        tokio::task::yield_now().await;

        drop(held.pop());

        tokio::time::timeout(tokio::time::Duration::from_secs(1), waiting)
            .await
            .expect("released slot must wake the refill loop")
            .expect("refill waiter must complete");
    }

    #[tokio::test]
    async fn cancelled_task_releases_admission() {
        let (admission, [hot, _, _], _schedulers) = admission([4, 4, 2]);
        let held = hot.reserve(1);
        let task = tokio::spawn(async move {
            let _held = held;
            std::future::pending::<()>().await;
        });
        tokio::task::yield_now().await;
        task.abort();
        let _ = task.await;

        assert_eq!(admission.in_use(MergeAdmissionLane::Hot), 0);
        assert_eq!(admission.free_slots(), 10);
    }

    #[tokio::test]
    async fn scheduler_shutdown_releases_waiting_admission() {
        let (admission, [hot, _, _], schedulers) = admission([4, 4, 2]);
        let held = hot.reserve(1);
        let closed = held[0].scheduler.tx.clone();
        let task = tokio::spawn(async move {
            closed.closed().await;
            drop(held);
        });

        drop(schedulers);
        tokio::time::timeout(tokio::time::Duration::from_secs(1), task)
            .await
            .expect("scheduler receiver shutdown must wake the waiting admission")
            .expect("shutdown admission task must complete");

        assert_eq!(admission.in_use(MergeAdmissionLane::Hot), 0);
        assert_eq!(admission.free_slots(), 10);
    }

    #[test]
    fn total_admission_never_exceeds_physical_capacity() {
        let (admission, [hot, recent, backlog], _schedulers) = admission([4, 4, 2]);
        hot.set_exhausted();
        recent.set_exhausted();
        let held = backlog.reserve(usize::MAX);

        assert_eq!(held.len(), 10);
        assert!(backlog.reserve(1).is_empty());
        assert_eq!(admission.in_use(MergeAdmissionLane::Backlog), 10);
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
    rx: Arc<AsyncMutex<mpsc::Receiver<(MergeSender, MergeBatch)>>>,
    tx: mpsc::Sender<(MergeSender, MergeBatch)>,
}

impl MergeWorker {
    pub fn new(num: usize) -> Self {
        // keep the workers fed: a fat job submits hundreds of batches; a
        // capacity-1 channel serialized submission behind the slowest batch
        let num = num.max(1);
        let (tx, rx) = mpsc::channel::<(MergeSender, MergeBatch)>(num * 2);
        let rx = Arc::new(AsyncMutex::new(rx));
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
