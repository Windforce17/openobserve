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

use chrono::{DateTime, Datelike, Duration, TimeZone, Timelike, Utc};
use config::{
    COMPACT_OLD_DATA_STREAM_SET,
    cluster::LOCAL_NODE,
    get_config,
    meta::{
        cluster::{CompactionJobType, Role},
        stream::{ALL_STREAM_TYPES, PartitionTimeLevel, StreamType},
    },
};
use infra::{
    cluster::get_node_from_consistent_hash,
    file_list::{self as infra_file_list, FileListJobOrder, FileListJobStatus, MergeJobRecord},
    schema::{get_partition_time_level, get_settings},
};
#[cfg(feature = "enterprise")]
use o2_enterprise::enterprise::common::downsampling::get_matching_downsampling_rules;

use crate::service::db;

pub mod bloom;
pub mod deleted;
pub(crate) mod download_budget;
pub mod dump;
pub mod incremental;
pub mod merge;
pub mod retention;
pub mod segments_sweep;
pub mod stats;
pub mod worker;

/// compactor retention run steps:
pub async fn run_retention() -> Result<(), anyhow::Error> {
    // generate retention jobs first
    if let Err(e) = retention::generate_jobs().await {
        log::error!("[COMPACTOR] generate retention job error: {e}");
    }

    // then run the jobs to delete the data
    let jobs = db::compact::retention::list().await?;
    for job in jobs {
        let columns = job.split('/').collect::<Vec<&str>>();
        let org_id = columns[0];
        let stream_type = StreamType::from(columns[1]);
        let stream_name = columns[2];
        let retention = columns[3];

        // here we use job to get the compactor node, so that we can use different compactor for
        // different job of same stream
        let Some(node_name) = get_node_from_consistent_hash(&job, &Role::Compactor, None).await
        else {
            continue; // no compactor node
        };
        if LOCAL_NODE.name.ne(&node_name) {
            continue; // not this node
        }

        let ret = if retention.eq("all") {
            retention::delete_all(org_id, stream_type, stream_name).await
        } else {
            let date_range = retention.split(',').collect::<Vec<&str>>();
            retention::delete_by_date(
                org_id,
                stream_type,
                stream_name,
                (date_range[0], date_range[1]),
            )
            .await
            .map_err(|e| {
                log::error!(
                    "[COMPACTOR] delete: delete [{org_id}/{stream_type}/{stream_name}] error: {e}"
                );
                e
            })
        };

        if let Err(e) = ret {
            log::error!(
                "[COMPACTOR] delete: delete [{org_id}/{stream_type}/{stream_name}] error: {e}"
            );
        }
    }

    Ok(())
}

/// Generate job for compactor
pub async fn run_generate_job(job_type: CompactionJobType) -> Result<(), anyhow::Error> {
    // M29 debt-sweep tally: hours with standing merge debt across all owned
    // streams this pass — ONE info line per pass (house log discipline),
    // per-hour detail stays at debug in the generator.
    let mut debt_hours = 0usize;
    let orgs = db::schema::list_organizations_from_cache().await;
    for org_id in orgs {
        // check backlist
        if !db::file_list::BLOCKED_ORGS.is_empty() && db::file_list::BLOCKED_ORGS.contains(&org_id)
        {
            continue;
        }
        for stream_type in ALL_STREAM_TYPES {
            let streams = db::schema::list_streams_from_cache(&org_id, stream_type).await;
            for stream_name in streams {
                let Some(node_name) =
                    get_node_from_consistent_hash(&stream_name, &Role::Compactor, None).await
                else {
                    continue; // no compactor node
                };
                if LOCAL_NODE.name.ne(&node_name) {
                    // This needs to be done in the case when there is a new node in the cluster
                    // This will change the node that holds the stream
                    // In case this node holds the stream, we release it for the designated node
                    if let Some((offset, _)) = db::compact::files::get_offset_from_cache(
                        &org_id,
                        stream_type,
                        &stream_name,
                    )
                    .await
                    {
                        // release the stream
                        db::compact::files::set_offset(
                            &org_id,
                            stream_type,
                            &stream_name,
                            offset,
                            None,
                        )
                        .await?;
                    }
                    continue; // not this node
                }

                // check if we are allowed to merge or just skip
                if db::compact::retention::is_deleting_stream(
                    &org_id,
                    stream_type,
                    &stream_name,
                    None,
                ) {
                    log::warn!(
                        "[COMPACTOR] the stream [{}/{}/{}] is deleting, just skip",
                        org_id,
                        stream_type,
                        stream_name,
                    );
                    continue;
                }

                match job_type {
                    CompactionJobType::Current => {
                        if let Err(e) =
                            merge::generate_job_by_stream(&org_id, stream_type, &stream_name).await
                        {
                            log::error!(
                                "[COMPACTOR] generate_job_by_stream [{org_id}/{stream_type}/{stream_name}] error: {e}"
                            );
                        }
                    }
                    CompactionJobType::Historical => {
                        if !COMPACT_OLD_DATA_STREAM_SET.is_empty()
                            && !COMPACT_OLD_DATA_STREAM_SET.contains(&stream_name)
                        {
                            continue;
                        }
                        if let Err(e) = merge::generate_old_data_job_by_stream(
                            &org_id,
                            stream_type,
                            &stream_name,
                        )
                        .await
                        {
                            log::error!(
                                "[COMPACTOR] generate_old_data_job_by_stream [{org_id}/{stream_type}/{stream_name}] error: {e}"
                            );
                        }
                    }
                    CompactionJobType::Debt => {
                        match merge::generate_merge_debt_job_by_stream(
                            &org_id,
                            stream_type,
                            &stream_name,
                        )
                        .await
                        {
                            Ok(n) => debt_hours += n,
                            Err(e) => {
                                log::error!(
                                    "[COMPACTOR] generate_merge_debt_job_by_stream [{org_id}/{stream_type}/{stream_name}] error: {e}"
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    if debt_hours > 0 {
        log::info!("[COMPACTOR] merge-debt sweep: {debt_hours} hours enqueued/refreshed");
    }

    Ok(())
}

/// Generate downsampling job for Metrics
#[cfg(feature = "enterprise")]
pub async fn run_generate_downsampling_job() -> Result<(), anyhow::Error> {
    let orgs = db::schema::list_organizations_from_cache().await;
    for org_id in orgs {
        // check backlist
        if !db::file_list::BLOCKED_ORGS.is_empty() && db::file_list::BLOCKED_ORGS.contains(&org_id)
        {
            continue;
        }
        let stream_type = StreamType::Metrics;
        let streams = db::schema::list_streams_from_cache(&org_id, stream_type).await;
        for stream_name in streams {
            let Some(node_name) =
                get_node_from_consistent_hash(&stream_name, &Role::Compactor, None).await
            else {
                continue; // no compactor node
            };
            let downsampling_rules = get_matching_downsampling_rules(&stream_name);
            for rule in downsampling_rules {
                if LOCAL_NODE.name.ne(&node_name) {
                    // Check if this node holds the stream
                    if let Some((offset, _)) = db::compact::downsampling::get_offset_from_cache(
                        &org_id,
                        stream_type,
                        &stream_name,
                        (rule.offset, rule.step),
                    )
                    .await
                    {
                        // release the stream
                        db::compact::downsampling::set_offset(
                            &org_id,
                            stream_type,
                            &stream_name,
                            (rule.offset, rule.step),
                            offset,
                            None,
                        )
                        .await?;
                    }
                    continue; // not this node
                }

                // check if we are allowed to merge or just skip
                if db::compact::retention::is_deleting_stream(
                    &org_id,
                    stream_type,
                    &stream_name,
                    None,
                ) {
                    log::warn!(
                        "[DOWNSAMPLING] the stream [{org_id}/{stream_type}/{stream_name}] is deleting, just skip",
                    );
                    continue;
                }

                if let Err(e) = merge::generate_downsampling_job_by_stream_and_rule(
                    &org_id,
                    stream_type,
                    &stream_name,
                    (rule.offset, rule.step),
                )
                .await
                {
                    log::error!(
                        "[DOWNSAMPLING] generate_downsampling_job_by_stream_and_rule [{org_id}/{stream_type}/{stream_name}] rule: {rule:?} error: {e}"
                    );
                }
            }
        }
    }

    Ok(())
}

const HOUR_MICROS: i64 = 3_600_000_000;

pub fn live_claim_floor(now_micros: i64, lookback_hours: i64) -> i64 {
    let current_hour = now_micros.div_euclid(HOUR_MICROS) * HOUR_MICROS;
    current_hour.saturating_sub(lookback_hours.max(1).saturating_mul(HOUR_MICROS))
}

/// A scheduler lane's database view. When live compaction is enabled callers
/// use the same hour boundary for `Backlog` and `Live`, making the two claim
/// sets disjoint by construction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MergeLane {
    /// All pending jobs, oldest enqueue first.
    All,
    /// Jobs strictly before the live floor, oldest enqueue first.
    Backlog { before: i64 },
    /// Jobs at or after the live floor, newest offset first.
    Live { from: i64 },
}

impl MergeLane {
    fn claim_spec(self) -> (FileListJobOrder, Option<i64>, Option<i64>, &'static str) {
        match self {
            Self::All => (FileListJobOrder::EnqueueOldest, None, None, "all"),
            Self::Backlog { before } => (
                FileListJobOrder::EnqueueOldest,
                None,
                Some(before),
                "backlog",
            ),
            Self::Live { from } => (FileListJobOrder::OffsetNewest, Some(from), None, "live"),
        }
    }
}

async fn transition_claim(
    job: &MergeJobRecord,
    cancel: &worker::MergeCancellation,
    done: bool,
    outcome: &'static str,
) {
    let result = if done {
        infra_file_list::set_job_done_owned(job.id, &LOCAL_NODE.uuid, job.lease_generation).await
    } else {
        infra_file_list::set_job_pending_owned(job.id, &LOCAL_NODE.uuid, job.lease_generation).await
    };
    match result {
        Ok(true) => log::info!(
            "[COMPACTOR] merge claim completed job_id={} generation={} outcome={outcome}",
            job.id,
            job.lease_generation,
        ),
        Ok(false) => {
            cancel.cancel();
            log::warn!(
                "[COMPACTOR] merge claim transition missed ownership job_id={} generation={} outcome={outcome}",
                job.id,
                job.lease_generation,
            );
        }
        Err(e) => log::error!(
            "[COMPACTOR] merge claim transition failed job_id={} generation={} outcome={outcome}: {e}",
            job.id,
            job.lease_generation,
        ),
    }
}

/// Claim and dispatch compaction work for one scheduler lane.
///
/// Scheduler permits are reserved before the database claim and moved
/// one-for-one into non-cloneable `MergeJob`s. The claim ceiling remains the
/// configured batch size, additionally bounded by merge-worker count and
/// currently-free lane capacity.
pub async fn run_merge(
    scheduler: &worker::JobSchedulerHandle,
    lane: MergeLane,
) -> Result<(), anyhow::Error> {
    let cfg = get_config();
    let batch_limit = usize::try_from(cfg.compact.batch_size.max(0)).unwrap_or(usize::MAX);
    let reserve_limit = batch_limit.min(cfg.limit.file_merge_thread_num.max(1));
    let permits = scheduler.reserve(reserve_limit);
    if permits.is_empty() {
        log::debug!(
            "[COMPACTOR] merge claim skipped lane={} active_slots={} free_slots=0",
            lane.claim_spec().3,
            scheduler.capacity(),
        );
        return Ok(());
    }

    let (order, min_offsets, max_offsets, lane_name) = lane.claim_spec();
    let claim_limit = permits.len() as i64;
    let jobs = match infra_file_list::get_pending_jobs(
        &LOCAL_NODE.uuid,
        claim_limit,
        order,
        min_offsets,
        max_offsets,
    )
    .await
    {
        Ok(jobs) => jobs,
        Err(e) => {
            drop(permits);
            return Err(e.into());
        }
    };
    log::info!(
        "[COMPACTOR] merge claim lane={lane_name} requested={claim_limit} claimed={} active_slots={} free_slots={}",
        jobs.len(),
        scheduler.capacity().saturating_sub(scheduler.free_slots()),
        scheduler.free_slots(),
    );
    if jobs.is_empty() {
        return Ok(());
    }

    let ttl = std::cmp::max(60, cfg.compact.job_run_timeout / 4) as u64;
    let now = config::utils::time::now();
    let data_lifecycle_end = now - Duration::try_days(cfg.compact.data_retention_days).unwrap();
    let mut permits = permits.into_iter();
    let mut claimed_jobs = Vec::with_capacity(jobs.len());
    let mut overflow = Vec::new();

    // Start every heartbeat immediately after the claim, before any one
    // record can block on schema/settings/ownership lookups.
    for job in jobs {
        let Some(permit) = permits.next() else {
            overflow.push(job);
            continue;
        };
        let cancel = worker::MergeCancellation::default();
        let lease = worker::JobLeaseGuard::spawn(
            job.id,
            LOCAL_NODE.uuid.clone(),
            job.lease_generation,
            FileListJobStatus::Running,
            ttl,
            cfg.compact.job_run_timeout.max(1) as u64,
            cancel.clone(),
        );
        claimed_jobs.push((job, permit, cancel, lease));
    }
    for job in overflow {
        // Defensive only: the storage API is required to honor `limit`.
        // Never retain an unreserved claim if an implementation regresses.
        let cancel = worker::MergeCancellation::default();
        transition_claim(&job, &cancel, false, "claim_overflow").await;
    }

    for (job, permit, cancel, lease) in claimed_jobs {
        if job.offsets == 0 {
            log::error!(
                "[COMPACTOR] merge job has invalid offset job_id={} generation={}",
                job.id,
                job.lease_generation,
            );
            transition_claim(&job, &cancel, true, "invalid_offset").await;
            continue;
        }
        let columns = job.stream.split('/').collect::<Vec<&str>>();
        if columns.len() != 3 {
            log::error!(
                "[COMPACTOR] merge job has invalid stream key job_id={} generation={}",
                job.id,
                job.lease_generation,
            );
            transition_claim(&job, &cancel, true, "invalid_stream").await;
            continue;
        }
        let org_id = columns[0].to_string();
        let stream_type = StreamType::from(columns[1]);
        let stream_name = columns[2].to_string();
        let stream_settings = get_settings(&org_id, &stream_name, stream_type)
            .await
            .unwrap_or_default();
        if cancel.is_cancelled() {
            log::warn!(
                "[COMPACTOR] merge claim cancelled before dispatch job_id={} generation={}",
                job.id,
                job.lease_generation,
            );
            continue;
        }
        let partition_time_level = get_partition_time_level(stream_type);
        let stream_data_retention_end = if stream_settings.data_retention > 0 {
            now - Duration::try_days(stream_settings.data_retention).unwrap()
        } else {
            data_lifecycle_end
        };
        if job.offsets <= stream_data_retention_end.timestamp_micros() {
            transition_claim(&job, &cancel, true, "retention_skip").await;
            continue;
        }
        if db::compact::retention::is_deleting_stream(&org_id, stream_type, &stream_name, None) {
            transition_claim(&job, &cancel, true, "deleting_stream").await;
            continue;
        }

        let mut stream_locked = false;
        if partition_time_level == PartitionTimeLevel::Daily {
            let Some(node_name) =
                get_node_from_consistent_hash(&stream_name, &Role::Compactor, None).await
            else {
                transition_claim(&job, &cancel, false, "no_daily_owner").await;
                continue;
            };
            if LOCAL_NODE.name.ne(&node_name) {
                transition_claim(&job, &cancel, false, "daily_owner_reject").await;
                continue;
            }
            if db::compact::stream::is_running(&job.stream) {
                transition_claim(&job, &cancel, false, "stream_busy").await;
                continue;
            }
            db::compact::stream::set_running(&job.stream);
            stream_locked = true;
        }

        if cancel.is_cancelled() {
            if stream_locked {
                db::compact::stream::clear_running(&job.stream);
            }
            continue;
        }
        let merge_job = worker::MergeJob::new(
            org_id,
            stream_type,
            stream_name,
            job.id,
            job.offsets,
            job.lease_generation,
            cancel.clone(),
            lease,
            permit,
        );
        if let Err(e) = scheduler.send(merge_job).await {
            let unsent = e.0;
            log::error!(
                "[COMPACTOR] merge dispatch failed job_id={} generation={}",
                unsent.job_id,
                unsent.lease_generation,
            );
            if stream_locked {
                db::compact::stream::clear_running(&job.stream);
            }
            drop(unsent.lease);
            transition_claim(&job, &cancel, false, "send_failure").await;
        }
    }

    Ok(())
}

/// compactor delay delete files run steps:
/// 1. get pending deleted files from file_list_deleted table, created_at > 2 hours
/// 2. delete files from storage
pub async fn run_delay_deletion() -> Result<(), anyhow::Error> {
    let now = Utc::now();
    let time_max =
        now - Duration::try_hours(get_config().compact.delete_files_delay_hours).unwrap();
    let time_max = Utc
        .with_ymd_and_hms(
            time_max.year(),
            time_max.month(),
            time_max.day(),
            time_max.hour(),
            0,
            0,
        )
        .unwrap();
    let time_max = time_max.timestamp_micros();
    let orgs = db::schema::list_organizations_from_cache().await;
    for org_id in orgs {
        loop {
            match deleted::delete(&org_id, time_max).await {
                Ok(affected) => {
                    if affected == 0 {
                        break;
                    }
                    log::debug!("[COMPACTOR] deleted from file_list_deleted {affected} files");
                }
                Err(e) => {
                    log::error!("[COMPACTOR] delete files error: {e}");
                    break;
                }
            };
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }

        // update offset
        db::compact::organization::set_offset(
            &org_id,
            "file_list_deleted",
            time_max,
            Some(&LOCAL_NODE.uuid.clone()),
        )
        .await?;
    }

    Ok(())
}

pub(crate) fn is_past_hour(offset: i64) -> bool {
    let time_now: DateTime<Utc> = Utc::now();
    let time_now_hour = Utc
        .with_ymd_and_hms(
            time_now.year(),
            time_now.month(),
            time_now.day(),
            time_now.hour(),
            0,
            0,
        )
        .unwrap()
        .timestamp_micros();
    // must wait for at least 3 * max_file_retention_time
    // -- first period: the last hour local file upload to storage, write file list
    // -- second period, the last hour file list upload to storage
    // -- third period, we can do the merge, so, at least 3 times of
    // max_file_retention_time
    offset < time_now_hour
        && time_now.timestamp_micros() - offset
            > Duration::try_seconds(get_config().limit.max_file_retention_time as i64)
                .unwrap()
                .num_microseconds()
                .unwrap()
                * 3
}

#[cfg(test)]
mod merge_lane_tests {
    use super::*;

    #[test]
    fn live_and_backlog_claim_windows_are_disjoint() {
        let floor = 1_725_000_000_000_000;
        let backlog = MergeLane::Backlog { before: floor }.claim_spec();
        let live = MergeLane::Live { from: floor }.claim_spec();

        assert_eq!(backlog.0, FileListJobOrder::EnqueueOldest);
        assert_eq!(backlog.1, None);
        assert_eq!(backlog.2, Some(floor));
        assert_eq!(live.0, FileListJobOrder::OffsetNewest);
        assert_eq!(live.1, Some(floor));
        assert_eq!(live.2, None);
    }

    #[test]
    fn live_claim_floor_is_hour_aligned() {
        let now = 10 * HOUR_MICROS + HOUR_MICROS / 2;
        assert_eq!(live_claim_floor(now, 2), 8 * HOUR_MICROS);
        assert_eq!(live_claim_floor(now, 0), 9 * HOUR_MICROS);
    }
}

/// Shared harness for compact tests that exercise process-global sqlite
/// file-list job tables. They claim/re-pend jobs table-wide, so tests across
/// modules serialize on one lock and namespace rows per run.
#[cfg(test)]
pub(crate) mod jobs_test_support {
    /// Serializes every test touching `file_list_jobs` in this crate.
    pub(crate) static FILE_LIST_JOBS_TEST_LOCK: tokio::sync::Mutex<()> =
        tokio::sync::Mutex::const_new(());

    /// Create the real file_list tables (idempotent) and take the lock.
    pub(crate) async fn setup() -> tokio::sync::MutexGuard<'static, ()> {
        let guard = FILE_LIST_JOBS_TEST_LOCK.lock().await;
        std::fs::create_dir_all(&config::get_config().common.data_db_dir)
            .expect("create data_db_dir for tests");
        infra::file_list::create_table()
            .await
            .expect("create file_list tables");
        // add_job's ON CONFLICT needs the unique (stream, offsets) index
        infra::file_list::create_table_index()
            .await
            .expect("create file_list indexes");
        guard
    }

    /// Test modules OUTSIDE this lock write to the same sqlite file through
    /// other connections (sea-orm, RO pools), so a write can still hit
    /// SQLITE_BUSY_SNAPSHOT ("database is locked") despite the RW mutex.
    /// Bounded retry on exactly that error; anything else panics with the
    /// operation name.
    pub(crate) async fn retry_busy<T, E, F, Fut>(op_name: &str, mut op: F) -> T
    where
        E: std::fmt::Display,
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
    {
        const TRIES: usize = 20;
        for attempt in 1..=TRIES {
            match op().await {
                Ok(v) => return v,
                Err(e) if e.to_string().contains("database is locked") && attempt < TRIES => {
                    tokio::time::sleep(tokio::time::Duration::from_millis(50 * attempt as u64))
                        .await;
                }
                Err(e) => panic!("{op_name} failed: {e}"),
            }
        }
        unreachable!("retry_busy returns or panics inside the loop");
    }
}
