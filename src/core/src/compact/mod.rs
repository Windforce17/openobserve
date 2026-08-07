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
    file_list as infra_file_list,
    schema::{get_partition_time_level, get_settings},
};
#[cfg(feature = "enterprise")]
use o2_enterprise::enterprise::common::downsampling::get_matching_downsampling_rules;
use tokio::sync::mpsc;

use crate::service::db;

pub mod bloom;
pub mod deleted;
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
                }
            }
        }
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

/// compactor merging
/// `min_offsets` restricts the claim to jobs at or after that hour (0 = no
/// restriction) — the live lane (#23) passes `now - lookback` so its
/// reserved slots only ever pick recent-hour jobs. `max_jobs` overrides the
/// claim size when > 0 (the live lane passes its slot count); 0 keeps the
/// default worker-sized claim.
pub async fn run_merge(
    job_tx: mpsc::Sender<worker::MergeJob>,
    min_offsets: i64,
    max_jobs: i64,
) -> Result<(), anyhow::Error> {
    let cfg = get_config();
    // Claim only what this node's merge workers can start soon: even with
    // heartbeats running from claim time (below), a worker-sized batch keeps
    // a slow node from hoarding jobs a healthy node could run. Oldest-first
    // claiming (get_pending_jobs) spreads a hot stream's hour-jobs across
    // the whole fleet.
    let claim_limit = if max_jobs > 0 {
        max_jobs
    } else {
        std::cmp::min(
            cfg.compact.batch_size,
            std::cmp::max(cfg.limit.file_merge_thread_num as i64, 1),
        )
    };
    let jobs = infra_file_list::get_pending_jobs(
        &LOCAL_NODE.uuid,
        claim_limit,
        cfg.compact.fast_mode,
        min_offsets,
    )
    .await?;
    if jobs.is_empty() {
        return Ok(());
    }

    // Heartbeat-from-claim: every claimed job gets its lease heartbeat NOW,
    // before any dispatch work. The guard is handed through the scheduler
    // channel inside MergeJob, so one continuous heartbeat covers the job
    // from CLAIM through the worker's COMMIT — a job parked in the
    // capacity-1 channel behind a long merge stays fresh instead of being
    // re-pended by check_running_jobs and double-merged by another node
    // (2026-07-30 audit). Guards of jobs that are released or done below
    // drop with this map at the end of the function.
    //
    // ttl: 1/4 of job_run_timeout — the timeout covers the whole job, and
    // refreshing at 1/2 could still cross the threshold under scheduling
    // delay, so 1/4 keeps a safety margin.
    let ttl = std::cmp::max(60, cfg.compact.job_run_timeout / 4) as u64;
    let mut leases: std::collections::HashMap<i64, worker::JobLeaseGuard> = jobs
        .iter()
        .map(|job| (job.id, worker::JobLeaseGuard::spawn(job.id, ttl)))
        .collect();

    let now = config::utils::time::now();
    let data_lifecycle_end = now - Duration::try_days(cfg.compact.data_retention_days).unwrap();

    // if the stream partition_time_level is daily we only allow one compactor
    let mut need_release_ids = Vec::new();
    let mut need_done_ids = Vec::new();
    let mut merge_jobs = Vec::with_capacity(jobs.len());
    for job in jobs {
        if job.offsets == 0 {
            log::error!("[COMPACTOR] merge job offset error: {}", job.offsets);
            continue;
        }
        let columns = job.stream.split('/').collect::<Vec<&str>>();
        assert_eq!(columns.len(), 3);
        let org_id = columns[0].to_string();
        let stream_type = StreamType::from(columns[1]);
        let stream_name = columns[2].to_string();
        let stream_settings = get_settings(&org_id, &stream_name, stream_type)
            .await
            .unwrap_or_default();
        let partition_time_level = get_partition_time_level(stream_type);
        // to avoid compacting conflict with retention, need check the data retention time
        let stream_data_retention_end = if stream_settings.data_retention > 0 {
            now - Duration::try_days(stream_settings.data_retention).unwrap()
        } else {
            data_lifecycle_end
        };
        if job.offsets <= stream_data_retention_end.timestamp_micros() {
            need_done_ids.push(job.id); // the data will be deleted by retention, just skip
            continue;
        }
        // check if we are allowed to merge or just skip
        if db::compact::retention::is_deleting_stream(&org_id, stream_type, &stream_name, None) {
            need_done_ids.push(job.id); // the data will be deleted by retention, just skip
            continue;
        }
        if partition_time_level == PartitionTimeLevel::Daily {
            // check if this stream need process by this node
            let Some(node_name) =
                get_node_from_consistent_hash(&stream_name, &Role::Compactor, None).await
            else {
                continue; // no compactor node
            };
            if LOCAL_NODE.name.ne(&node_name) {
                need_release_ids.push(job.id); // not this node
                continue;
            }

            // check if already running a job for this stream
            if db::compact::stream::is_running(&job.stream) {
                need_release_ids.push(job.id); // another job is running
                continue;
            } else {
                db::compact::stream::set_running(&job.stream);
            }
        }
        // collect the merge jobs
        let Some(lease) = leases.remove(&job.id) else {
            // a duplicate id from the claim query would have consumed its
            // lease guard already — never run the same job twice
            log::error!(
                "[COMPACTOR] claimed job {} appeared twice, skipping the duplicate",
                job.id
            );
            continue;
        };
        merge_jobs.push(worker::MergeJob {
            org_id,
            stream_type,
            stream_name,
            job_id: job.id,
            offset: job.offsets,
            lease,
        });
    }

    if !need_release_ids.is_empty() {
        // release those jobs
        if let Err(e) = infra_file_list::set_job_pending(&need_release_ids, 0, None).await {
            log::error!("[COMPACTOR] set_job_pending failed: {e}");
        }
    }

    if !need_done_ids.is_empty() {
        // set those jobs to done
        if let Err(e) = infra_file_list::set_job_done(&need_done_ids).await {
            log::error!("[COMPACTOR] set_job_done failed: {e}");
        }
    }

    // Hand each job (with its lease guard inside) to the scheduler. The old
    // batch-level heartbeat that lived only until this loop finished is gone:
    // the per-job guards spawned at claim time above cover the whole
    // claim-to-commit window.
    for job in merge_jobs {
        if let Err(e) = job_tx.send(job.clone()).await {
            log::error!(
                "[COMPACTOR] send merge job to worker failed [{}/{}/{}] error: {e}",
                job.org_id,
                job.stream_type,
                job.stream_name,
            );
            // the job never reached a worker (scheduler shut down): release
            // the claim right away instead of letting the lease time out
            if let Err(e) = infra_file_list::set_job_pending(&[job.job_id], 0, None).await {
                log::error!(
                    "[COMPACTOR] set_job_pending for undispatched job {} failed: {e}",
                    job.job_id,
                );
            }
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

/// Shared harness for the compact tests that exercise the process-global
/// sqlite file_list tables (`worker` heartbeat lifetime, `merge` commit
/// fencing): they claim/re-pend jobs table-wide, so tests across BOTH
/// modules must serialize on ONE lock, and rows are namespaced per run.
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
