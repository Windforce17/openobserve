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

use config::{cluster, get_config};
use tokio::sync::Mutex;

use crate::service::compact::dump;

const DUMP_JOB_MIN_INTERVAL: i64 = 30;

pub async fn run() -> Result<(), anyhow::Error> {
    if !cluster::LOCAL_NODE.is_compactor() {
        return Ok(());
    }

    let cfg = get_config();
    if !cfg.compact.file_list_dump_enabled {
        return Ok(());
    }
    if !cfg.compact.lease_generation_enabled {
        log::warn!(
            "[FILE_LIST_DUMP:JOB] file-list dump jobs are disabled until ZO_COMPACT_LEASE_GENERATION_ENABLED is enabled on the whole rollout"
        );
        return Ok(());
    }

    // spawn threads which will do the actual dumping
    let (tx, rx) = tokio::sync::mpsc::channel::<dump::DumpJob>(1);
    let rx = Arc::new(Mutex::new(rx));
    for thread_id in 0..cfg.limit.file_merge_thread_num.max(1) {
        let rx = rx.clone();
        tokio::spawn(async move {
            loop {
                let ret = rx.lock().await.recv().await;
                match ret {
                    None => {
                        log::debug!("[FILE_LIST_DUMP:JOB:{thread_id}] Receiving channel is closed");
                        break;
                    }
                    Some(job) => {
                        log::info!(
                            "[FILE_LIST_DUMP:JOB:{thread_id}] job started job_id={} generation={}",
                            job.job_id,
                            job.lease_generation,
                        );
                        if let Err(e) = crate::service::compact::dump::dump(&job).await {
                            log::error!(
                                "[FILE_LIST_DUMP:JOB:{thread_id}] dump for stream [{}/{}/{}] job_id={} generation={} offset {}: error: {e}",
                                job.org_id,
                                job.stream_type,
                                job.stream_name,
                                job.job_id,
                                job.lease_generation,
                                job.offset,
                            );
                            match infra::file_list::set_job_dumped_status_owned(
                                job.job_id,
                                &cluster::LOCAL_NODE.uuid,
                                job.lease_generation,
                                false,
                            )
                            .await
                            {
                                Ok(true) => log::info!(
                                    "[FILE_LIST_DUMP:JOB:{thread_id}] job released job_id={} generation={} outcome=retry",
                                    job.job_id,
                                    job.lease_generation,
                                ),
                                Ok(false) => {
                                    job.cancel.cancel();
                                    log::warn!(
                                        "[FILE_LIST_DUMP:JOB:{thread_id}] release missed ownership job_id={} generation={}",
                                        job.job_id,
                                        job.lease_generation,
                                    );
                                }
                                Err(release_error) => log::error!(
                                    "[FILE_LIST_DUMP:JOB:{thread_id}] release failed job_id={} generation={}: {release_error}",
                                    job.job_id,
                                    job.lease_generation,
                                ),
                            }
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

    let interval: u64 = std::cmp::max(cfg.compact.interval as i64, DUMP_JOB_MIN_INTERVAL)
        .try_into()
        .unwrap();

    // loop and keep checking on file_list_jobs for next dump jobs
    loop {
        if cluster::is_offline() {
            break;
        }

        // sleep
        tokio::time::sleep(tokio::time::Duration::from_secs(interval)).await;
        // run
        if let Err(e) = crate::service::compact::dump::run(tx.clone()).await {
            log::error!("[FILE_LIST_DUMP:JOB] error in running dump: {e}");
        }
    }

    log::info!("[FILE_LIST_DUMP:JOB] all jobs are stopped");
    Ok(())
}
