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

use config::{
    cluster::LOCAL_NODE,
    get_config,
    meta::{cluster::CompactionJobType, stream::ALL_STREAM_TYPES},
    metrics, spawn_pausable_job,
};
use infra::cluster::get_node_by_uuid;
#[cfg(feature = "enterprise")]
use o2_enterprise::enterprise::common::config::get_config as get_o2_config;

use crate::service::compact;
const ENRICHMENT_TABLE_MERGE_LOCK_KEY: &str = "/compact/enrichment_table";

pub async fn run() -> Result<(), anyhow::Error> {
    if !LOCAL_NODE.is_compactor() {
        return Ok(());
    }

    let cfg = get_config();

    // Segment-WAL sweeper (DESIGN-SEGMENT-WAL.md): retires built segment
    // objects and their wal_segments rows. Deliberately not gated on
    // compact.enabled — storage reclamation must continue even when merge
    // compaction is turned off — nor on ingest_segment_mode: a flag-off
    // rollback must keep retiring already-built segments, and an idle sweep
    // over an empty table is near-free.
    compact::segments_sweep::spawn();

    if !cfg.compact.enabled {
        return Ok(());
    }
    log::info!("[COMPACTOR::JOB] Compactor is enabled");

    let mut worker = compact::worker::MergeWorker::new(cfg.limit.file_merge_thread_num);
    if let Err(e) = worker.run() {
        log::error!("[COMPACTOR::JOB] start merge worker error: {e}");
    }

    // scheduler slots (concurrent jobs) decoupled from merge workers (#23):
    // storms want few jobs × many workers each, steady state the opposite
    let job_num = if cfg.compact.job_num > 0 {
        cfg.compact.job_num
    } else {
        cfg.limit.file_merge_thread_num
    };
    let mut scheduler = compact::worker::JobScheduler::new(job_num, worker.tx());
    if let Err(e) = scheduler.run() {
        log::error!("[COMPACTOR::JOB] start merge job scheduler error: {e}");
    }

    // Live lane (#23): reserved scheduler slots + dedicated workers that
    // claim only recent-hour jobs, so a storm/heal backlog can never starve
    // the open hour's incremental merges (2026-08-06: apisix piled to 3,297
    // open-hour L0s while every shared slot ground old fat hours).
    let live_tx = if cfg.compact.live_job_num > 0 {
        let mut live_worker = compact::worker::MergeWorker::new(cfg.compact.live_worker_num.max(1));
        if let Err(e) = live_worker.run() {
            log::error!("[COMPACTOR::JOB] start live merge worker error: {e}");
        }
        let mut live_scheduler =
            compact::worker::JobScheduler::new(cfg.compact.live_job_num, live_worker.tx());
        if let Err(e) = live_scheduler.run() {
            log::error!("[COMPACTOR::JOB] start live merge job scheduler error: {e}");
        }
        Some(live_scheduler.tx())
    } else {
        None
    };
    if let Some(live_tx) = live_tx {
        spawn_pausable_job!("run_merge_live", get_config().compact.interval + 1, {
            let cfg = get_config();
            let min_offsets = config::utils::time::now_micros()
                - cfg.compact.live_lookback_hours.max(1) * 3_600_000_000;
            if let Err(e) = compact::run_merge(
                live_tx.clone(),
                min_offsets,
                cfg.compact.live_job_num as i64,
            )
            .await
            {
                log::error!("[COMPACTOR::JOB] run merge live error: {e}");
            }
        });
    }

    spawn_pausable_job!("run_generate_job", get_config().compact.interval, {
        log::debug!("[COMPACTOR::JOB] Running generate merge job");
        if let Err(e) = compact::run_generate_job(CompactionJobType::Current).await {
            log::error!("[COMPACTOR::JOB] run generate merge job error: {e}");
        }
    });

    // M29 merge-debt sweep: keep the job table stocked with every closed hour
    // that still holds mergeable small files (oldest first), so partially
    // consumed hours and late-built L0s are revisited at this cadence instead
    // of the hourly old-data pass — and the newest closed hours (the old-data
    // lane's dead zone, the hottest query window) are covered too. Idempotent
    // per (stream, hour); 0 disables.
    spawn_pausable_job!(
        "run_generate_merge_debt_job",
        get_config().compact.merge_debt_interval,
        {
            log::debug!("[COMPACTOR::JOB] Running generate merge debt job");
            if let Err(e) = compact::run_generate_job(CompactionJobType::Debt).await {
                log::error!("[COMPACTOR::JOB] run generate merge debt job error: {e}");
            }
        }
    );

    // One-shot boot pass for the historical sweep: the pausable job sleeps a
    // FULL old_data_interval (1h) before its first run, so every compactor
    // restart used to push the old-data sweep out by an hour — repeated rolls
    // starved it indefinitely and stranded pre-watermark hours. The short
    // delay lets the node ring settle so stream ownership is stable.
    tokio::task::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_secs(120)).await;
        log::info!("[COMPACTOR::JOB] boot pass: generate merge job for old data");
        if let Err(e) = compact::run_generate_job(CompactionJobType::Historical).await {
            log::error!("[COMPACTOR::JOB] boot old-data generate error: {e}");
        }
    });
    spawn_pausable_job!(
        "run_generate_old_data_job",
        get_config().compact.old_data_interval.saturating_add(1),
        {
            log::debug!("[COMPACTOR::JOB] Running generate merge job for old data");
            if let Err(e) = compact::run_generate_job(CompactionJobType::Historical).await {
                log::error!("[COMPACTOR::JOB] run generate merge job for old data error: {e}");
            }
        }
    );

    if get_config().compact.bloom_build_interval > 0 {
        spawn_pausable_job!(
            "run_bloom_build",
            get_config().compact.bloom_build_interval,
            {
                log::debug!("[COMPACTOR::JOB] Running group .bf bloom builder");
                if let Err(e) = compact::bloom::run().await {
                    log::error!("[COMPACTOR::JOB] bloom builder error: {e}");
                }
            }
        );
    }

    #[cfg(feature = "enterprise")]
    spawn_pausable_job!(
        "compactor_downsampling",
        get_o2_config().downsampling.downsampling_interval,
        {
            if get_o2_config()
                .downsampling
                .metrics_downsampling_rules
                .is_empty()
            {
                continue;
            }
            log::debug!("[COMPACTOR::JOB] Running generate downsampling job");
            if let Err(e) = compact::run_generate_downsampling_job().await {
                log::error!("[COMPACTOR::JOB] run generate downsampling job error: {e}");
            }
        }
    );

    spawn_pausable_job!("run_merge", get_config().compact.interval + 2, {
        log::debug!("[COMPACTOR::JOB] Running data merge");
        if let Err(e) = compact::run_merge(scheduler.tx().clone(), 0, 0).await {
            log::error!("[COMPACTOR::JOB] run data merge error: {e}");
        }
    });

    spawn_pausable_job!(
        "run_retention",
        get_config().compact.data_retention_interval + 3,
        {
            log::debug!("[COMPACTOR::JOB] Running data retention");
            if let Err(e) = compact::run_retention().await {
                log::error!("[COMPACTOR::JOB] run data retention error: {e}");
            }
        }
    );

    spawn_pausable_job!("run_delay_deletion", get_config().compact.interval + 4, {
        log::debug!("[COMPACTOR::JOB] Running data delay deletion");
        if let Err(e) = compact::run_delay_deletion().await {
            log::error!("[COMPACTOR::JOB] run data delay deletion error: {e}");
        }
    });

    spawn_pausable_job!(
        "compactor_sync_to_db",
        get_config().compact.sync_to_db_interval,
        {
            log::debug!("[COMPACTOR::JOB] Running sync cached compact offset to db");
            if let Err(e) = crate::service::db::compact::files::sync_cache_to_db().await {
                log::error!("[COMPACTOR::JOB] run sync cached compact offset to db error: {e}");
            }
        }
    );

    #[cfg(feature = "enterprise")]
    spawn_pausable_job!(
        "compactor_downsampling_sync_to_db",
        get_config().compact.sync_to_db_interval,
        {
            if get_o2_config()
                .downsampling
                .metrics_downsampling_rules
                .is_empty()
            {
                return;
            }
            log::debug!("[COMPACTOR::JOB] Running sync cached downsampling offset to db");
            if let Err(e) = crate::service::db::compact::downsampling::sync_cache_to_db().await {
                log::error!(
                    "[COMPACTOR::JOB] run sync cached downsampling offset to db error: {e}"
                );
            }
        }
    );

    spawn_pausable_job!(
        "compactor_check_running_jobs",
        get_config().compact.job_run_timeout,
        {
            log::debug!("[COMPACTOR::JOB] Running check running jobs");
            let timeout = get_config().compact.job_run_timeout;
            let updated_at = config::utils::time::now_micros() - (timeout * 1000 * 1000);
            if let Err(e) = infra::file_list::check_running_jobs(updated_at).await {
                log::error!("[COMPACTOR::JOB] run check running jobs error: {e}");
            }
        },
        sleep_after
    );

    spawn_pausable_job!(
        "compactor_clean_done_jobs",
        get_config().compact.job_clean_wait_time,
        {
            log::debug!("[COMPACTOR::JOB] Running clean done jobs");
            let wait_time = get_config().compact.job_clean_wait_time;
            let updated_at = config::utils::time::now_micros() - (wait_time * 1000 * 1000);
            if let Err(e) = infra::file_list::clean_done_jobs(updated_at).await {
                log::error!("[COMPACTOR::JOB] run clean done jobs error: {e}");
            }
        },
        sleep_after
    );

    spawn_pausable_job!(
        "run_compactor_pending_jobs_metric",
        get_config().compact.pending_jobs_metric_interval,
        {
            log::debug!("[COMPACTOR::JOB] Running compactor pending jobs to report metric");
            let job_status = match infra::file_list::get_pending_jobs_count().await {
                Ok(status) => status,
                Err(e) => {
                    log::error!("[COMPACTOR::JOB] run compactor pending jobs metric error: {e}");
                    continue;
                }
            };

            // reset all metrics
            let orgs = crate::service::db::schema::list_organizations_from_cache().await;
            for org in orgs {
                for stream_type in ALL_STREAM_TYPES {
                    if metrics::COMPACT_PENDING_JOBS
                        .with_label_values(&[org.as_str(), stream_type.as_str()])
                        .get()
                        > 0
                    {
                        metrics::COMPACT_PENDING_JOBS
                            .with_label_values(&[org.as_str(), stream_type.as_str()])
                            .set(0);
                    }
                }
            }

            // set new metrics
            for (org, inner_map) in job_status {
                for (stream_type, counter) in inner_map {
                    metrics::COMPACT_PENDING_JOBS
                        .with_label_values(&[org.as_str(), stream_type.as_str()])
                        .set(counter);
                }
            }
        }
    );

    tokio::task::spawn(run_enrichment_table_merge());

    Ok(())
}

// TODO: refactor here only select node when it start,
// if this node stopped, then there is no one can continue to merge
async fn run_enrichment_table_merge() -> Result<(), anyhow::Error> {
    log::info!("[COMPACTOR::JOB] Running enrichment table merge");
    let db = infra::db::get_db().await;
    let Ok(locker) = infra::dist_lock::lock(ENRICHMENT_TABLE_MERGE_LOCK_KEY, 0).await else {
        log::error!("[COMPACTOR::JOB] Failed to acquire lock for enrichment table merge");
        return Ok(());
    };
    let node_id = db
        .get(ENRICHMENT_TABLE_MERGE_LOCK_KEY)
        .await
        .unwrap_or_default();
    let node_id = String::from_utf8_lossy(&node_id);
    if !node_id.is_empty()
        && LOCAL_NODE.uuid.ne(&node_id)
        && get_node_by_uuid(&node_id).await.is_some()
    {
        // Unlock and return
        if let Err(e) = infra::dist_lock::unlock(&locker).await {
            log::error!("[COMPACTOR::JOB] Failed to release lock for enrichment table merge: {e}");
        }
        return Ok(());
    }

    log::debug!("[COMPACTOR::JOB] No node is merging enrichment table");
    // This node is the first node to merge enrichment table
    if let Err(e) = db
        .put(
            ENRICHMENT_TABLE_MERGE_LOCK_KEY,
            LOCAL_NODE.uuid.clone().into(),
            false,
            None,
        )
        .await
    {
        log::error!("[COMPACTOR::JOB] Failed to put lock for enrichment table merge: {e}");
        if let Err(e) = infra::dist_lock::unlock(&locker).await {
            log::error!("[COMPACTOR::JOB] Failed to release lock for enrichment table merge: {e}");
        }
        return Ok(());
    }

    if let Err(e) = infra::dist_lock::unlock(&locker).await {
        log::error!("[COMPACTOR::JOB] Failed to release lock for enrichment table merge: {e}");
    }
    crate::service::enrichment::storage::remote::run_merge_job().await;
    Ok(())
}
