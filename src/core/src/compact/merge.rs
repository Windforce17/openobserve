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

use ::datafusion::{arrow::datatypes::Schema, error::DataFusionError};
use bytes::Bytes;
use chrono::{DateTime, Datelike, Duration, TimeZone, Utc};
use config::{
    FileFormat,
    cluster::LOCAL_NODE,
    get_config, ider, is_local_disk_storage,
    meta::stream::{
        FileKey, FileListDeleted, FileMeta, MergeStrategy, PartitionTimeLevel, StorageType,
        StreamType,
    },
    metrics,
    utils::{
        parquet::read_schema_from_bytes,
        schema_ext::SchemaExt,
        time::{day_micros, hour_micros},
    },
};
use hashbrown::{HashMap, HashSet};
#[cfg(test)]
use infra::file_list::FileListJobOrder;
use infra::{
    cache::file_data,
    cluster::get_node_by_uuid,
    dist_lock,
    file_list::{self as infra_file_list, FileListJobStatus},
    runtime::DATAFUSION_RUNTIME,
    schema::{
        get_partition_time_level, get_stream_setting_bloom_filter_fields,
        get_stream_setting_fts_fields, unwrap_stream_created_at,
    },
    storage,
};
#[cfg(feature = "enterprise")]
use o2_enterprise::enterprise::common::downsampling::get_largest_downsampling_rule;
use tokio::{
    sync::{Semaphore, mpsc},
    task::JoinHandle,
};
use vortex_index::VixOutput;

use super::worker::{MergeBatch, MergeCancellation, MergeSender};
use crate::service::{
    db, file_list,
    search::datafusion::{
        exec::TableBuilder,
        merge::{self, MergeParquetResult},
    },
};

/// SPOOL-ALWAYS backstop (H3/§7): the largest merge output the buffered
/// `storage::put` arm may carry. Anything bigger streams from a spool file
/// (one multipart part's worth is "trivially small").
const BUFFERED_UPLOAD_MAX_BYTES: u64 = 16 * 1024 * 1024;
/// A single claimed hour may contain hundreds of batches. Keep only a tiny
/// window queued/running so lease loss or shutdown does not leave an hour's
/// worth of CPU work and uploads that can no longer commit.
const JOB_BATCH_IN_FLIGHT: usize = 2;

type MergeBatchOutcome = Result<(usize, Vec<FileKey>, Vec<FileKey>), anyhow::Error>;

enum MergeBatchReceive {
    Result(MergeBatchOutcome),
    Closed,
    Cancelled,
}

async fn receive_merge_batch_result(
    rx: &mut mpsc::Receiver<MergeBatchOutcome>,
    cancel: &MergeCancellation,
) -> MergeBatchReceive {
    tokio::select! {
        result = rx.recv() => match result {
            Some(result) => MergeBatchReceive::Result(result),
            None => MergeBatchReceive::Closed,
        },
        _ = wait_for_merge_cancellation(cancel) => MergeBatchReceive::Cancelled,
    }
}

async fn send_merge_batch(
    worker_tx: &mpsc::Sender<(MergeSender, MergeBatch)>,
    result_tx: MergeSender,
    batch: MergeBatch,
    cancel: &MergeCancellation,
) -> Result<(), anyhow::Error> {
    tokio::select! {
        biased;
        _ = wait_for_merge_cancellation(cancel) => {
            Err(anyhow::anyhow!("compaction cancelled while waiting to submit batch"))
        }
        result = worker_tx.send((result_tx, batch)) => {
            result.map_err(|e| anyhow::anyhow!("send batch to worker failed: {e}"))
        }
    }
}

fn vix_cpu_capacity() -> usize {
    let machine = std::thread::available_parallelism().map_or(1, |n| n.get());
    std::cmp::max(1, machine / config::cluster::cpu_role_divisor())
}

fn vix_threads_per_merge() -> usize {
    let capacity = vix_cpu_capacity();
    match get_config().common.vix_merge_thread_num {
        0 => capacity.min(8),
        configured => configured.clamp(1, capacity),
    }
}

static VIX_CPU_GATE: std::sync::LazyLock<Arc<Semaphore>> =
    std::sync::LazyLock::new(|| Arc::new(Semaphore::new(vix_cpu_capacity())));

async fn acquire_vix_cpu(
    cancel: &MergeCancellation,
    context: &str,
) -> Result<tokio::sync::OwnedSemaphorePermit, anyhow::Error> {
    let permits = vix_threads_per_merge() as u32;
    let started = std::time::Instant::now();
    loop {
        cancel.check(context)?;
        tokio::select! {
            permit = VIX_CPU_GATE.clone().acquire_many_owned(permits) => {
                let permit = permit.map_err(|_| anyhow::anyhow!(
                    "VIX CPU admission closed while waiting for {permits} permit(s) at {context}"
                ))?;
                let waited = started.elapsed();
                if waited >= std::time::Duration::from_millis(100) {
                    log::info!(
                        "[COMPACTOR] VIX CPU admission waited {} ms for {permits}/{} permit(s) at {context}",
                        waited.as_millis(),
                        vix_cpu_capacity(),
                    );
                }
                return Ok(permit);
            }
            _ = wait_for_merge_cancellation(cancel) => {
                cancel.check(context)?;
            }
        }
    }
}

/// Generate merging job by stream
/// 1. get offset from db
/// 2. check if other node is processing
/// 3. create job or return
pub async fn generate_job_by_stream(
    org_id: &str,
    stream_type: StreamType,
    stream_name: &str,
) -> Result<(), anyhow::Error> {
    // get last compacted offset
    let (mut offset, node) = db::compact::files::get_offset(org_id, stream_type, stream_name).await;
    if !node.is_empty() && LOCAL_NODE.uuid.ne(&node) && get_node_by_uuid(&node).await.is_some() {
        return Ok(()); // other node is processing
    }

    if node.is_empty() || LOCAL_NODE.uuid.ne(&node) {
        let lock_key = format!("/compact/merge/{org_id}/{stream_type}/{stream_name}");
        let locker = dist_lock::lock(&lock_key, 0).await?;
        // check the working node again, maybe other node locked it first
        let (offset, node) = db::compact::files::get_offset(org_id, stream_type, stream_name).await;
        if !node.is_empty() && LOCAL_NODE.uuid.ne(&node) && get_node_by_uuid(&node).await.is_some()
        {
            dist_lock::unlock(&locker).await?;
            return Ok(()); // other node is processing
        }
        // set to current node
        let ret = db::compact::files::set_offset(
            org_id,
            stream_type,
            stream_name,
            offset,
            Some(&LOCAL_NODE.uuid.clone()),
        )
        .await;
        dist_lock::unlock(&locker).await?;
        drop(locker);
        ret?;
    }

    // get schema
    let schema = infra::schema::get(org_id, stream_name, stream_type).await?;
    let stream_created = unwrap_stream_created_at(&schema).unwrap_or_default();
    if offset == 0 && stream_created > 0 {
        offset = stream_created
    } else if offset == 0 {
        return Ok(()); // no data
    }

    // format to hour with zero minutes, seconds
    let offset = offset - offset % hour_micros(1);
    if !super::is_past_hour(offset) {
        return Ok(()); // the time is future, just wait
    }

    log::debug!(
        "[COMPACTOR] generate_job_by_stream [{org_id}/{stream_type}/{stream_name}] offset: {offset}"
    );

    // generate merging job
    if let Err(e) = infra_file_list::add_job(org_id, stream_type, stream_name, offset).await {
        return Err(anyhow::anyhow!(
            "[COMPACTOR] add file_list_jobs failed: {e}"
        ));
    }

    // write new offset
    let offset = offset + hour_micros(1);
    db::compact::files::set_offset(
        org_id,
        stream_type,
        stream_name,
        offset,
        Some(&LOCAL_NODE.uuid.clone()),
    )
    .await?;

    Ok(())
}

/// Generate merging job by stream
/// 1. get old data by hour
/// 2. check if other node is processing
/// 3. create job or return
pub async fn generate_old_data_job_by_stream(
    org_id: &str,
    stream_type: StreamType,
    stream_name: &str,
) -> Result<(), anyhow::Error> {
    // get last compacted offset
    let (offset, node) = db::compact::files::get_offset(org_id, stream_type, stream_name).await;
    if !node.is_empty() && LOCAL_NODE.uuid.ne(&node) && get_node_by_uuid(&node).await.is_some() {
        return Ok(()); // other node is processing
    }

    if node.is_empty() || LOCAL_NODE.uuid.ne(&node) {
        let lock_key = format!("/compact/merge/{org_id}/{stream_type}/{stream_name}");
        let locker = dist_lock::lock(&lock_key, 0).await?;
        // check the working node again, maybe other node locked it first
        let (offset, node) = db::compact::files::get_offset(org_id, stream_type, stream_name).await;
        if !node.is_empty() && LOCAL_NODE.uuid.ne(&node) && get_node_by_uuid(&node).await.is_some()
        {
            dist_lock::unlock(&locker).await?;
            return Ok(()); // other node is processing
        }
        // set to current node
        let ret = db::compact::files::set_offset(
            org_id,
            stream_type,
            stream_name,
            offset,
            Some(&LOCAL_NODE.uuid.clone()),
        )
        .await;
        dist_lock::unlock(&locker).await?;
        drop(locker);
        ret?;
    }

    if offset == 0 {
        return Ok(()); // no data
    }

    let cfg = get_config();
    let stream_settings = infra::schema::get_settings(org_id, stream_name, stream_type)
        .await
        .unwrap_or_default();
    let mut stream_data_retention_days = cfg.compact.data_retention_days;
    if stream_settings.data_retention > 0 {
        stream_data_retention_days = stream_settings.data_retention;
    }
    if stream_data_retention_days > cfg.compact.old_data_max_days {
        stream_data_retention_days = cfg.compact.old_data_max_days;
    }
    if stream_data_retention_days == 0 {
        return Ok(()); // no need to check old data
    }

    // get old data by hour, `offset - cfg.compact.old_data_min_hours hours` as old data
    let end_time = offset - hour_micros(cfg.compact.old_data_min_hours);
    let start_time = end_time
        - Duration::try_days(stream_data_retention_days)
            .unwrap()
            .num_microseconds()
            .unwrap();
    let hours = infra_file_list::query_old_data_hours(
        org_id,
        stream_type,
        stream_name,
        (start_time, end_time - 1),
        // M31b follow-up: surface lone index-less .vix files for the
        // single-file heal — but never for stream types that are
        // index-less BY DESIGN (their heal probe no-ops; the clause would
        // re-enqueue those hours forever).
        !config::is_vix_index_disabled(stream_type),
    )
    .await?;

    // generate merging job
    add_jobs_for_hours(org_id, stream_type, stream_name, &hours, "old data").await?;

    Ok(())
}

/// Parse `YYYY/MM/DD/HH` hour buckets and enqueue one merge job per hour
/// (idempotent: `add_job` dedups on the unique `(stream, offsets)` row —
/// pending/running hours are left alone, done rows are resurrected).
async fn add_jobs_for_hours(
    org_id: &str,
    stream_type: StreamType,
    stream_name: &str,
    hours: &[String],
    lane: &str,
) -> Result<(), anyhow::Error> {
    for hour in hours {
        let column = hour.split('/').collect::<Vec<_>>();
        if column.len() != 4 {
            return Err(anyhow::anyhow!(
                "Unexpected hour format in {hour}, Expected format YYYY/MM/DD/HH",
            ));
        }
        let offset = DateTime::parse_from_rfc3339(&format!(
            "{}-{}-{}T{}:00:00Z",
            column[0], column[1], column[2], column[3]
        ))?
        .with_timezone(&Utc);
        let offset = offset.timestamp_micros();
        log::debug!(
            "[COMPACTOR] generate {lane} job [{org_id}/{stream_type}/{stream_name}] hour: {hour}, offset: {offset}"
        );
        if let Err(e) = infra_file_list::add_job(org_id, stream_type, stream_name, offset).await {
            return Err(anyhow::anyhow!(
                "[COMPACTOR] add file_list_jobs for {lane} failed: {e}"
            ));
        }
    }
    Ok(())
}

/// M29 merge-debt sweep for one stream: enqueue a merge job for EVERY closed
/// hour in the retention window that still holds >=
/// `ZO_COMPACT_OLD_DATA_MIN_FILES` small (mergeable) files, OLDEST first (the
/// M13 aging discipline). This is what makes the merge pipeline
/// work-conserving: a job visit that consumed only part of its hour (the
/// merge width/byte caps bound one batch), or an hour whose L0 files were
/// built AFTER the hourly scheduled pass visited it (segment-builder lag),
/// is re-enqueued within `ZO_COMPACT_MERGE_DEBT_INTERVAL` seconds instead of
/// waiting for the next hourly old-data pass — and unlike that pass it also
/// covers the newest closed hours inside the `ZO_COMPACT_OLD_DATA_MIN_HOURS`
/// dead zone, which are exactly the hot query window the L0 sliver tax hurts
/// most. Enqueueing is idempotent (`add_job` dedups pending/running hours),
/// so the sweep never floods the job table; converged hours (fewer than
/// min_files small files, or all bytes in near-max_file_size outputs) drop
/// out of the query by themselves.
///
/// Returns the number of hours enqueued (pending/running dedup included —
/// this counts hours the query still flags as holding debt).
pub async fn generate_merge_debt_job_by_stream(
    org_id: &str,
    stream_type: StreamType,
    stream_name: &str,
) -> Result<usize, anyhow::Error> {
    // ownership was already checked by the caller (consistent hash), but keep
    // the offset-node guard the other generators use so a ring shift never
    // has two nodes sweeping the same stream
    let (offset, node) = db::compact::files::get_offset(org_id, stream_type, stream_name).await;
    if !node.is_empty() && LOCAL_NODE.uuid.ne(&node) && get_node_by_uuid(&node).await.is_some() {
        return Ok(0); // other node owns this stream
    }
    if offset == 0 {
        // no compact offset yet: fall back to the stream's creation stamp,
        // exactly like the current-lane generator
        let schema = infra::schema::get(org_id, stream_name, stream_type).await?;
        if unwrap_stream_created_at(&schema).unwrap_or_default() == 0 {
            return Ok(0); // no data
        }
    }

    let cfg = get_config();
    let stream_settings = infra::schema::get_settings(org_id, stream_name, stream_type)
        .await
        .unwrap_or_default();
    let mut retention_days = cfg.compact.data_retention_days;
    if stream_settings.data_retention > 0 {
        retention_days = stream_settings.data_retention;
    }
    // same window bound as the old-data lane: never scan further back than
    // old_data_max_days even on effectively-infinite retention
    if retention_days > cfg.compact.old_data_max_days || retention_days <= 0 {
        retention_days = cfg.compact.old_data_max_days;
    }

    // newest CLOSED hour whose files are all settled (is_past_hour's margin:
    // 3 x max_file_retention_time past the hour start) — the debt window's
    // inclusive end is that hour's last microsecond
    let now = config::utils::time::now_micros();
    let mut newest_hour = now - now % hour_micros(1) - hour_micros(1);
    let settle_margin = Duration::try_seconds(cfg.limit.max_file_retention_time as i64)
        .unwrap()
        .num_microseconds()
        .unwrap()
        * 3;
    while newest_hour > 0 && now - newest_hour <= settle_margin {
        newest_hour -= hour_micros(1);
    }
    let end_time = newest_hour + hour_micros(1) - 1;
    let start_time = now
        - Duration::try_days(retention_days)
            .unwrap()
            .num_microseconds()
            .unwrap();
    if start_time >= end_time {
        return Ok(0);
    }

    let mut hours = infra_file_list::query_old_data_hours(
        org_id,
        stream_type,
        stream_name,
        (start_time, end_time),
        // M31b follow-up: lone index-less .vix files must keep the hour in
        // debt until the single-file heal indexes them (never for
        // by-design index-less stream types — no-op churn otherwise).
        !config::is_vix_index_disabled(stream_type),
    )
    .await?;
    if hours.is_empty() {
        return Ok(0);
    }
    // oldest cohorts first (M13 aging): "YYYY/MM/DD/HH" sorts chronologically
    hours.sort_unstable();
    let count = hours.len();
    add_jobs_for_hours(org_id, stream_type, stream_name, &hours, "merge debt").await?;
    Ok(count)
}

/// Generate downsampling job by stream and rule
/// 1. get offset from db
/// 2. check if other node is processing
/// 3. create job or return
pub async fn generate_downsampling_job_by_stream_and_rule(
    org_id: &str,
    stream_type: StreamType,
    stream_name: &str,
    rule: (i64, i64), // offset, step
) -> Result<(), anyhow::Error> {
    assert!(stream_type == StreamType::Metrics);
    // get last compacted offset
    let (mut offset, node) =
        db::compact::downsampling::get_offset(org_id, stream_type, stream_name, rule).await;
    if !node.is_empty() && LOCAL_NODE.uuid.ne(&node) && get_node_by_uuid(&node).await.is_some() {
        return Ok(()); // other node is processing
    }

    if node.is_empty() || LOCAL_NODE.uuid.ne(&node) {
        let lock_key = format!(
            "/compact/downsampling/{org_id}/{stream_type}/{stream_name}/{}/{}",
            rule.0, rule.1
        );
        let locker = dist_lock::lock(&lock_key, 0).await?;
        // check the working node again, maybe other node locked it first
        let (offset, node) =
            db::compact::downsampling::get_offset(org_id, stream_type, stream_name, rule).await;
        if !node.is_empty() && LOCAL_NODE.uuid.ne(&node) && get_node_by_uuid(&node).await.is_some()
        {
            dist_lock::unlock(&locker).await?;
            return Ok(()); // other node is processing
        }
        // set to current node
        let ret = db::compact::downsampling::set_offset(
            org_id,
            stream_type,
            stream_name,
            rule,
            offset,
            Some(&LOCAL_NODE.uuid.clone()),
        )
        .await;
        dist_lock::unlock(&locker).await?;
        drop(locker);
        ret?;
    }

    // get schema
    let schema = infra::schema::get(org_id, stream_name, stream_type).await?;
    let stream_created = unwrap_stream_created_at(&schema).unwrap_or_default();
    if offset == 0 {
        offset = stream_created
    }
    if offset == 0 {
        return Ok(()); // no data
    }

    let cfg = get_config();
    // check offset
    let time_now: DateTime<Utc> = Utc::now();
    let time_now_day = Utc
        .with_ymd_and_hms(time_now.year(), time_now.month(), time_now.day(), 0, 0, 0)
        .unwrap()
        .timestamp_micros();
    // must wait for at least 3 * max_file_retention_time + 1 day
    // -- first period: the last hour local file upload to storage, write file list
    // -- second period, the last hour file list upload to storage
    // -- third period, we can do the merge, so, at least 3 times of
    // -- 1 day, downsampling is in day level
    // max_file_retention_time
    if offset >= time_now_day
        || time_now.timestamp_micros() - offset
            <= Duration::try_seconds(cfg.limit.max_file_retention_time as i64)
                .unwrap()
                .num_microseconds()
                .unwrap()
                * 3
                + day_micros(1)
        || time_now.timestamp_micros() - rule.0 * 1_000_000 < offset
    {
        return Ok(()); // the time is future, just wait
    }

    log::debug!(
        "[DOWNSAMPLING] generate_downsampling_job_by_stream_and_rule [{org_id}/{stream_type}/{stream_name}] rule: {rule:?}, offset: {offset}"
    );

    // generate downsampling job
    if let Err(e) = infra_file_list::add_job(org_id, stream_type, stream_name, offset).await {
        return Err(anyhow::anyhow!(
            "[DOWNSAMPLING] add file_list_jobs failed: {e}"
        ));
    }

    // write new offset
    let offset = offset + day_micros(1);
    // format to day with zero hour, minutes, seconds
    let offset = offset - offset % day_micros(1);
    db::compact::downsampling::set_offset(
        org_id,
        stream_type,
        stream_name,
        rule,
        offset,
        Some(&LOCAL_NODE.uuid.clone()),
    )
    .await?;

    Ok(())
}

/// compactor run steps on a stream:
/// 3. get a cluster lock for compactor stream
/// 4. read last compacted offset: year/month/day/hour
/// 5. read current hour all files
/// 6. compact small files to big files -> COMPACTOR_MAX_FILE_SIZE
/// 7. write to storage
/// 8. delete small files keys & write big files keys, use transaction
/// 9. delete small files from storage
/// 10. update last compacted offset
/// 11. release cluster lock
pub async fn merge_by_stream(
    worker_tx: mpsc::Sender<(MergeSender, MergeBatch)>,
    org_id: &str,
    stream_type: StreamType,
    stream_name: &str,
    job_id: i64,
    lease_generation: i64,
    offset: i64,
    cancel: &MergeCancellation,
) -> Result<(), anyhow::Error> {
    let cfg = get_config();
    let start = std::time::Instant::now();

    cancel.check("stream schema lookup")?;
    let schema = infra::schema::get(org_id, stream_name, stream_type).await?;
    cancel.check("stream schema lookup")?;
    if schema == Schema::empty() {
        match infra_file_list::set_job_done_owned(job_id, &LOCAL_NODE.uuid, lease_generation).await
        {
            Ok(true) => log::info!(
                "[COMPACTOR] merge job completed job_id={job_id} generation={lease_generation} outcome=deleted_stream"
            ),
            Ok(false) => {
                cancel.cancel();
                return Err(anyhow::anyhow!(
                    "job {job_id} generation {lease_generation} lost ownership on deleted-stream completion"
                ));
            }
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "set_job_done_owned failed for job {job_id} generation {lease_generation}: {e}"
                ));
            }
        }
        return Ok(());
    }

    log::debug!(
        "[COMPACTOR] merge_by_stream [{org_id}/{stream_type}/{stream_name}] offset: {offset}"
    );

    // A job whose offset hour has not yet fully passed is an incremental round on the
    // still-open current hour (enqueued by the ingester, see service::compact::incremental):
    // only seal full-size groups and carry the remainder, so each file is merged into a
    // sealed output exactly once. The scheduled hour-end pass seals whatever is left.
    let offset = offset - offset % hour_micros(1);
    let is_incremental = !super::is_past_hour(offset);

    // check offset
    let partition_time_level = get_partition_time_level(stream_type);
    let offset_time: DateTime<Utc> = Utc.timestamp_nanos(offset * 1000);
    let (date_start, date_end) = if partition_time_level == PartitionTimeLevel::Daily {
        (
            offset_time.format("%Y/%m/%d/00").to_string(),
            offset_time.format("%Y/%m/%d/23").to_string(),
        )
    } else {
        (
            offset_time.format("%Y/%m/%d/%H").to_string(),
            offset_time.format("%Y/%m/%d/%H").to_string(),
        )
    };
    // Non-incremental (closed-hour) jobs fetch full-size files too: they
    // are excluded from merge grouping below, but the healing probe must
    // see them — a corrupt ~max_file_size output is otherwise unreachable
    // by any merge forever (prod 2026-07-29).
    let files = file_list::query_for_merge(
        org_id,
        stream_type,
        stream_name,
        &date_start,
        &date_end,
        !is_incremental,
    )
    .await
    .map_err(|e| anyhow::anyhow!("query file list failed: {e}"))?;
    cancel.check("file-list query")?;

    log::debug!(
        "[COMPACTOR] merge_by_stream [{org_id}/{stream_type}/{stream_name}] date range: [{date_start},{date_end}], files: {}",
        files.len(),
    );
    if files.is_empty() {
        match infra_file_list::set_job_done_owned(job_id, &LOCAL_NODE.uuid, lease_generation).await
        {
            Ok(true) => log::info!(
                "[COMPACTOR] merge job completed job_id={job_id} generation={lease_generation} outcome=empty_stream"
            ),
            Ok(false) => {
                cancel.cancel();
                return Err(anyhow::anyhow!(
                    "job {job_id} generation {lease_generation} lost ownership on empty-stream completion"
                ));
            }
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "set_job_done_owned failed for job {job_id} generation {lease_generation}: {e}"
                ));
            }
        }
        return Ok(());
    }

    // do partition by partition key
    let mut partition_files_with_size: HashMap<String, Vec<FileKey>> = HashMap::default();
    for file in files {
        let file_name = file.key.clone();
        let prefix = file_name[..file_name.rfind('/').unwrap()].to_string();
        let partition = partition_files_with_size.entry(prefix).or_default();
        partition.push(file.to_owned());
    }

    // use multiple threads to merge
    let semaphore = std::sync::Arc::new(Semaphore::new(cfg.limit.file_merge_thread_num));
    let job_cancel = cancel.clone();
    let mut tasks = Vec::with_capacity(partition_files_with_size.len());
    for (prefix, files_with_size) in partition_files_with_size.into_iter() {
        let org_id = org_id.to_string();
        let stream_name = stream_name.to_string();
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let worker_tx = worker_tx.clone();
        let job_cancel = job_cancel.clone();
        let task: JoinHandle<Result<Vec<i64>, anyhow::Error>> = tokio::task::spawn(async move {
            job_cancel.check("partition planning")?;
            let cfg = get_config();
            let job_strategy = MergeStrategy::from(&cfg.compact.strategy);

            // Core files (.vix) and flat data files (parquet/vortex) never
            // merge together. Indexed core files split again: their
            // dictionary-passthrough path has a separate, larger byte target,
            // while flat and index-less groups retain the rebuild-safe global
            // target. Full-size files stay outside grouping; core files among
            // them remain healing-probe candidates below.
            let (core_files, flat_candidates): (Vec<FileKey>, Vec<FileKey>) = files_with_size
                .into_iter()
                .partition(|f| f.key.ends_with(config::FILE_EXT_VIX));
            // M31: sidecar-HOMOGENEOUS core grouping — never mix indexed
            // and index-less core files in one group. A mixed group rejects
            // the dictionary fast path then rebuilds every input.
            let (plain_candidates, indexed_candidates): (Vec<FileKey>, Vec<FileKey>) =
                core_files.into_iter().partition(|f| f.meta.index_size == 0);
            let global_cutoff = cfg.compact.max_file_size as i64 * 95 / 100;
            let indexed_cutoff =
                cfg.compact.max_file_size_for_merge(stream_type, true) as i64 * 95 / 100;
            let (_oversize_flat, mut flat_files): (Vec<FileKey>, Vec<FileKey>) = flat_candidates
                .into_iter()
                .partition(|f| f.meta.original_size > global_cutoff);
            let (plain_oversize, mut plain_core): (Vec<FileKey>, Vec<FileKey>) = plain_candidates
                .into_iter()
                .partition(|f| f.meta.original_size > global_cutoff);
            let (indexed_oversize, mut indexed_core): (Vec<FileKey>, Vec<FileKey>) =
                indexed_candidates
                    .into_iter()
                    .partition(|f| f.meta.original_size > indexed_cutoff);
            let oversize_core_files: Vec<FileKey> =
                plain_oversize.into_iter().chain(indexed_oversize).collect();
            // sort by file size
            for files in [&mut flat_files, &mut plain_core, &mut indexed_core] {
                match job_strategy {
                    MergeStrategy::FileSize => {
                        files.sort_by_key(|k| k.meta.original_size);
                    }
                    MergeStrategy::FileTime => {
                        files.sort_by_key(|k| k.meta.min_ts);
                    }
                    MergeStrategy::TimeRange => {
                        *files = sort_by_time_range(std::mem::take(files));
                    }
                }
            }
            let core_total = plain_core.len() + indexed_core.len();

            // downsampling applies to metrics only, which are never core files
            #[cfg(feature = "enterprise")]
            let skip_group_files = stream_type == StreamType::Metrics
                && !flat_files.is_empty()
                && get_largest_downsampling_rule(
                    &stream_name,
                    flat_files.iter().map(|f| f.meta.max_ts).max().unwrap(),
                )
                .is_some();

            #[cfg(not(feature = "enterprise"))]
            let skip_group_files = false;

            // A partition holding exactly ONE core file can never form a
            // >= 2 merge group, so a file with outdated index capabilities
            // (fts-tainted partial field, missing numeric value terms,
            // missing configured docs columns) would keep them forever —
            // only a rebuild heals it. Such files are probed cheaply below
            // (container metadata / fields table over ranged reads; a
            // current file stays a NO-OP with no docs download and no
            // file_list change) and enqueued as a single-file healing batch
            // when outdated. Skipped in incremental rounds: the hour is
            // still open, more files are coming, and the hour-end pass
            // probes once.
            let single_core_heal_candidate = core_total == 1 && !is_incremental;

            if flat_files.len() <= 1
                && core_total <= 1
                && oversize_core_files.is_empty()
                && !skip_group_files
                && !single_core_heal_candidate
            {
                return Ok(vec![]);
            }

            // group files need to merge
            let mut batch_groups = Vec::new();
            if skip_group_files {
                batch_groups.push(MergeBatch {
                    batch_id: 0,
                    org_id: org_id.clone(),
                    stream_type,
                    stream_name: stream_name.clone(),
                    prefix: prefix.clone(),
                    files: flat_files.clone(),
                    cancel: MergeCancellation::default(),
                });
            } else {
                group_files_into_batches(
                    &mut batch_groups,
                    &flat_files,
                    &org_id,
                    stream_type,
                    &stream_name,
                    &prefix,
                    cfg.compact.max_file_size as i64,
                    is_incremental,
                    &job_strategy,
                );
            }
            group_files_into_batches(
                &mut batch_groups,
                &plain_core,
                &org_id,
                stream_type,
                &stream_name,
                &prefix,
                cfg.compact.max_file_size as i64,
                is_incremental,
                &job_strategy,
            );
            group_files_into_batches(
                &mut batch_groups,
                &indexed_core,
                &org_id,
                stream_type,
                &stream_name,
                &prefix,
                cfg.compact.max_file_size_for_merge(stream_type, true) as i64,
                is_incremental,
                &job_strategy,
            );

            // Healing probe candidates: the lone file of a single-file
            // partition, PLUS every core file batching left out (a file at
            // ~max_file_size never joins a >= 2 group, so a defective one —
            // e.g. an unreadable dictionary — would otherwise stay broken
            // forever). Probes are container-metadata cheap and run only in
            // non-incremental rounds.
            let mut heal_candidates: Vec<&FileKey> = Vec::new();
            if single_core_heal_candidate {
                heal_candidates.extend(plain_core.first().or(indexed_core.first()));
            } else if !is_incremental {
                let batched: std::collections::HashSet<&str> = batch_groups
                    .iter()
                    .flat_map(|b| b.files.iter().map(|f| f.key.as_str()))
                    .collect();
                heal_candidates.extend(
                    plain_core
                        .iter()
                        .chain(indexed_core.iter())
                        .filter(|f| !batched.contains(f.key.as_str())),
                );
            }
            if !is_incremental {
                heal_candidates.extend(oversize_core_files.iter());
            }
            for candidate in heal_candidates {
                match single_core_file_heal_reason(&org_id, stream_type, &stream_name, candidate)
                    .await
                {
                    Ok(Some(reason)) => {
                        log::info!(
                            "[COMPACTOR] {org_id}/{stream_type}/{stream_name}: single-file \
                             healing rebuild of {}: {reason}",
                            candidate.key,
                        );
                        batch_groups.push(MergeBatch {
                            batch_id: batch_groups.len(),
                            org_id: org_id.clone(),
                            stream_type,
                            stream_name: stream_name.clone(),
                            prefix: prefix.clone(),
                            files: vec![candidate.clone()],
                            cancel: MergeCancellation::default(),
                        });
                    }
                    // current file: the no-op path — no batch, no docs IO
                    Ok(None) => {}
                    Err(e) => {
                        // Healing is best-effort: failing the job here would
                        // retry a possibly deterministic probe failure
                        // forever and wedge the stream's compaction. The
                        // WARN is the ops signal to re-run the sweep.
                        log::warn!(
                            "[COMPACTOR] {org_id}/{stream_type}/{stream_name}: single-file \
                             healing probe of {} failed (leaving the file as is): {e:#}",
                            candidate.key,
                        );
                    }
                }
            }

            if batch_groups.is_empty() {
                return Ok(vec![]); // no files need to merge
            }

            for batch in &mut batch_groups {
                batch.cancel = job_cancel.clone();
            }

            // send to worker
            let batch_group_len = batch_groups.len();
            let (inner_tx, mut inner_rx) = mpsc::channel(batch_group_len);
            // Commit each batch AS ITS RESULT ARRIVES (streaming, #23): a
            // long job interrupted mid-way (pod roll, lease loss, kill)
            // keeps every batch committed so far, and the re-claim re-plans
            // over the REMAINING files instead of redoing hours of work —
            // the .69 recovery lost hour-14's merges repeatedly to the old
            // collect-all-then-commit shape (2026-08-06). Each commit is
            // individually safe: it is fenced on current job ownership.
            let mut last_error = None;
            let mut lease_lost = false;
            let mut check_guard = HashSet::with_capacity(batch_groups.len());
            let mut orphan_blooms = Vec::new();
            let mut next_batch = 0usize;
            let mut in_flight = 0usize;
            while in_flight > 0 || next_batch < batch_group_len {
                while !lease_lost
                    && !job_cancel.is_cancelled()
                    && in_flight < JOB_BATCH_IN_FLIGHT
                    && next_batch < batch_group_len
                {
                    let batch = batch_groups[next_batch].clone();
                    if let Err(e) =
                        send_merge_batch(&worker_tx, inner_tx.clone(), batch, &job_cancel).await
                    {
                        log::error!("[COMPACTOR] {e}");
                        return Err(e);
                    }
                    next_batch += 1;
                    in_flight += 1;
                }
                if in_flight == 0 {
                    if next_batch < batch_group_len && last_error.is_none() {
                        last_error = Some(anyhow::anyhow!(
                            "compaction cancelled with {}/{} batches not started",
                            batch_group_len - next_batch,
                            batch_group_len,
                        ));
                    }
                    break;
                }
                let ret = match receive_merge_batch_result(&mut inner_rx, &job_cancel).await {
                    MergeBatchReceive::Result(ret) => ret,
                    MergeBatchReceive::Closed => {
                        last_error = Some(anyhow::anyhow!(
                            "compaction worker result channel closed with {in_flight} batch(es) still in flight"
                        ));
                        break;
                    }
                    MergeBatchReceive::Cancelled => {
                        last_error = Some(anyhow::anyhow!(
                            "compaction cancelled with {in_flight} batch(es) still in flight"
                        ));
                        break;
                    }
                };
                in_flight -= 1;
                let (batch_id, new_files, merged_files) = match ret {
                    Ok(v) => v,
                    Err(e) => {
                        log::error!("[COMPACTOR] merge files failed: {e}");
                        last_error = Some(e);
                        continue;
                    }
                };

                if check_guard.contains(&batch_id) {
                    log::warn!(
                        "[COMPACTOR] merge files for stream: [{org_id}/{stream_type}/{stream_name}] found error files, batch_id: {batch_id} duplicate"
                    );
                    continue;
                }
                check_guard.insert(batch_id);

                let Some(batch) = batch_groups.get(batch_id) else {
                    log::error!(
                        "[COMPACTOR] merge result for stream: [{org_id}/{stream_type}/{stream_name}] carries unknown batch_id: {batch_id}"
                    );
                    last_error = Some(anyhow::anyhow!("merge result batch_id {batch_id} unknown"));
                    continue;
                };

                // once the job lease is gone every remaining batch of this
                // job must be discarded too — log each so the uploaded
                // orphans are traceable, then let the re-claimer own them
                if lease_lost {
                    log::warn!(
                        "[COMPACTOR] job {job_id} lease lost: discarding merged output of batch {batch_id} for [{org_id}/{stream_type}/{stream_name}] (orphaned uploads: {:?})",
                        new_files.iter().map(|f| f.key.as_str()).collect::<Vec<_>>(),
                    );
                    continue;
                }

                // Delete EXACTLY the inputs whose rows made it into the
                // merged output. The batch may be a superset: size-mismatch
                // downloads are skipped, the size budget can cut a batch
                // short, and a group can shrink below two survivors —
                // deleting the whole batch in those cases threw away live
                // rows (2026-07-30 audit). Skipped files stay in the
                // file_list untouched and merge on a later cycle.
                if merged_files.len() < batch.files.len() {
                    let merged_keys: HashSet<&str> =
                        merged_files.iter().map(|f| f.key.as_str()).collect();
                    let skipped = batch
                        .files
                        .iter()
                        .map(|f| f.key.as_str())
                        .filter(|k| !merged_keys.contains(k))
                        .collect::<Vec<_>>();
                    // counts at info; the full path list only at debug — at
                    // width 128 this printed hundreds of paths per batch and
                    // the spam re-ingests into obs itself
                    log::info!(
                        "[COMPACTOR] merge batch {batch_id} for [{org_id}/{stream_type}/{stream_name}] merged {}/{} files; keeping {} unmerged for a later cycle",
                        merged_files.len(),
                        batch.files.len(),
                        skipped.len(),
                    );
                    log::debug!(
                        "[COMPACTOR] merge batch {batch_id} for [{org_id}/{stream_type}/{stream_name}] unmerged files: {skipped:?}"
                    );
                }

                // delete small files keys & write big files keys, use transaction
                let events = build_commit_events(new_files, &merged_files);
                if events.is_empty() {
                    // nothing merged and nothing to delete (e.g. too few
                    // healthy survivors): release the batch with NO
                    // file_list writes at all
                    log::info!(
                        "[COMPACTOR] merge batch {batch_id} for [{org_id}/{stream_type}/{stream_name}] produced no output and consumed no inputs; releasing it with no file_list changes"
                    );
                    continue;
                }

                // write file list to storage. A failed commit FAILS the job
                // (recorded in last_error): the merged output was uploaded
                // but the inputs stay live in file_list, so the job must
                // return to pending and re-merge — silently continuing used
                // to mark the job done and re-merge the same inputs every
                // cycle forever.
                //
                // The commit is FENCED on current job ownership (a single
                // conditional UPDATE on file_list_jobs): if the lease was
                // lost — re-pended after a heartbeat gap and possibly
                // re-claimed by another node — writing would double-commit
                // the same inputs with the new owner (permanent duplicate
                // rows), so the whole result is discarded instead and only
                // the uploaded objects are orphaned.
                match commit_batch_if_owner(
                    job_id,
                    &LOCAL_NODE.uuid,
                    lease_generation,
                    &org_id,
                    stream_type,
                    &events,
                )
                .await
                {
                    Ok(FencedCommit::Committed) => {}
                    Ok(FencedCommit::LeaseLost) => {
                        log::error!(
                            "[COMPACTOR] job {job_id} for [{org_id}/{stream_type}/{stream_name}] lost its lease before commit: DISCARDING batch {batch_id} and every remaining batch of this job; the current lease holder re-merges the hour (orphaned uploads: {:?})",
                            events
                                .iter()
                                .filter(|f| !f.deleted)
                                .map(|f| f.key.as_str())
                                .collect::<Vec<_>>(),
                        );
                        last_error = Some(anyhow::anyhow!("job {job_id} lease lost before commit"));
                        lease_lost = true;
                        job_cancel.cancel();
                        continue;
                    }
                    // fence query or file_list write failed: nothing proven
                    // lost, so later batches still fence for themselves —
                    // but the job must fail and re-run
                    Err(e) => {
                        log::error!(
                            "[COMPACTOR] job {job_id} for [{org_id}/{stream_type}/{stream_name}] commit of batch {batch_id} failed: {e}"
                        );
                        last_error = Some(e);
                        continue;
                    }
                }

                // collect orphan blooms after writing file list successfully
                // — only the files actually deleted release their blooms
                for file in merged_files.iter() {
                    if file.meta.bloom_ver > 0 {
                        orphan_blooms.push(file.meta.bloom_ver);
                    }
                }
            }
            drop(permit);
            if let Some(e) = last_error {
                return Err(e);
            }
            Ok(orphan_blooms)
        });
        tasks.push(task);
    }

    // Collect EVERY partition task before acting on any error: the old
    // `task.await??` loop returned on the first failure while sibling tasks
    // kept running detached — their commits could then race the re-claimer
    // of the re-pended job. join_all guarantees no task is still running
    // when this function returns (and the per-batch commit fence above
    // covers the re-claim race itself).
    let task_results = futures::future::join_all(tasks).await;
    let mut orphan_blooms = Vec::new();
    let mut first_error: Option<anyhow::Error> = None;
    for task_result in task_results {
        match task_result {
            Ok(Ok(blooms)) => orphan_blooms.extend(blooms),
            Ok(Err(e)) => {
                log::error!(
                    "[COMPACTOR] merge_by_stream [{org_id}/{stream_type}/{stream_name}] partition task failed: {e}"
                );
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
            Err(e) => {
                log::error!(
                    "[COMPACTOR] merge_by_stream [{org_id}/{stream_type}/{stream_name}] partition task panicked or was cancelled: {e}"
                );
                if first_error.is_none() {
                    first_error = Some(e.into());
                }
            }
        }
    }
    if let Some(e) = first_error {
        return Err(e);
    }

    let _ = (is_incremental, orphan_blooms);

    cancel.check("job completion")?;
    match infra_file_list::set_job_done_owned(job_id, &LOCAL_NODE.uuid, lease_generation).await {
        Ok(true) => log::info!(
            "[COMPACTOR] merge job completed job_id={job_id} generation={lease_generation} outcome=done elapsed_ms={}",
            start.elapsed().as_millis(),
        ),
        Ok(false) => {
            cancel.cancel();
            return Err(anyhow::anyhow!(
                "job {job_id} generation {lease_generation} lost ownership before completion"
            ));
        }
        Err(e) => {
            return Err(anyhow::anyhow!(
                "set_job_done_owned failed for job {job_id} generation {lease_generation}: {e}"
            ));
        }
    }

    // metrics
    let time = start.elapsed().as_secs_f64();
    metrics::COMPACT_USED_TIME
        .with_label_values(&[org_id, stream_type.as_str()])
        .inc_by(time);

    Ok(())
}

/// Cut `files` (already sorted by the job strategy) into merge batches
/// bounded by the supplied class-specific `max_file_size` and
/// `compact.max_group_files`, appending them to `batch_groups`. Indexed core
/// callers pass the larger dictionary-passthrough target; flat/index-less
/// callers pass the global rebuild-safe target. In incremental mode the
/// below-budget trailing remainder is carried to the next round instead of
/// being sealed (see `merge_by_stream`). Lists of one file produce no batch.
#[allow(clippy::too_many_arguments)]
fn group_files_into_batches(
    batch_groups: &mut Vec<MergeBatch>,
    files: &[FileKey],
    org_id: &str,
    stream_type: StreamType,
    stream_name: &str,
    prefix: &str,
    max_file_size: i64,
    is_incremental: bool,
    job_strategy: &MergeStrategy,
) {
    let cfg = get_config();
    // M29: cut groups at max_file_count too — merge_files consumes at most
    // that many files per batch (#51 width cap), so a wider group's tail was
    // dispatched, dropped on the floor, and re-planned + re-queried on the
    // NEXT visit of the hour (prod 2026-08-24: 208 "keeping N unmerged"
    // partial batches per 30m). Cutting here makes every dispatched batch
    // fully consumable in one pass; output sizes are unchanged (the consumer
    // sealed exactly this many files per output before as well).
    let max_group_len = match (cfg.compact.max_group_files, cfg.compact.max_file_count) {
        (0, 0) => 0,
        (g, 0) => g,
        (0, c) => c as usize,
        (g, c) => std::cmp::min(g, c as usize),
    };
    let mut new_file_list = Vec::new();
    let mut new_file_size = 0;
    for file in files.iter() {
        if new_file_size + file.meta.original_size > max_file_size
            || (max_group_len > 0 && new_file_list.len() >= max_group_len)
        {
            if new_file_list.len() <= 1 {
                if *job_strategy == MergeStrategy::FileSize {
                    break;
                }
                new_file_list.clear();
                new_file_size = file.meta.original_size;
                new_file_list.push(file.clone());
                continue; // replace previous file with current file
            }
            batch_groups.push(MergeBatch {
                batch_id: batch_groups.len(),
                org_id: org_id.to_string(),
                stream_type,
                stream_name: stream_name.to_string(),
                prefix: prefix.to_string(),
                files: new_file_list.clone(),
                cancel: MergeCancellation::default(),
            });
            new_file_size = 0;
            new_file_list.clear();
        }
        new_file_size += file.meta.original_size;
        new_file_list.push(file.clone());
    }
    // The trailing batch is always below max_file_size (the loop flushes a group
    // only when adding the next file would exceed it). In incremental mode we do
    // NOT seal this remainder: more files will arrive in the still-open hour, and
    // sealing now would force re-merging it later (write amplification). Carry it
    // to the next round; the scheduled hour-end pass seals whatever is left.
    if new_file_list.len() > 1 && !is_incremental {
        batch_groups.push(MergeBatch {
            batch_id: batch_groups.len(),
            org_id: org_id.to_string(),
            stream_type,
            stream_name: stream_name.to_string(),
            prefix: prefix.to_string(),
            files: new_file_list.clone(),
            cancel: MergeCancellation::default(),
        });
    }
}

/// M20b: which compactor parquet merges plan their `ORDER BY` at a single
/// partition (see `DataFusionContextBuilder::single_partition`) — extends
/// M13's segment-builder treatment to the compactor invocation.
///
/// Blast radius deliberately limited to METADATA-class streams: their merge
/// groups are size-capped, so the bounded inputs gain nothing from parallel
/// sort, and the repartitioned min-floor plan was prod's compactor killer
/// (116MB→6GB DataFusion spikes in ~2s merging
/// default/metadata/trace_list_index; 98 spikes >2GB per 30min, ~24
/// compactor OOM kills in 40min on .111): one ExternalSorter spills
/// correctly where RepartitionExec buffers unspillably. Data streams
/// (logs/traces/metrics/index) keep the multi-partition plan they have
/// always used until a matching failure class justifies widening.
fn compact_single_partition_sort(stream_type: StreamType) -> bool {
    stream_type == StreamType::Metadata
}

// merge small files into big file, upload to storage, returns the big file key and merged files
// params:
// - thread_id: the id of the thread
// - org_id: the id of the organization
// - stream_type: the type of the stream
// - stream_name: the name of the stream
// - prefix: the prefix of the files
// - files_with_size: the files to merge
// returns:
// - new_files: the merged output files
// - retain_file_list: EXACTLY the input files whose rows made it into new_files — the only files
//   the caller may delete. Inputs left out (size-budget cut, size-mismatch skip, dropped-invalid,
//   lone survivor) are NOT in it: they stay live in the file_list and retry later.
pub async fn merge_files(
    thread_id: usize,
    org_id: &str,
    stream_type: StreamType,
    stream_name: &str,
    prefix: &str,
    files_with_size: &[FileKey],
    cancel: &MergeCancellation,
) -> Result<(Vec<FileKey>, Vec<FileKey>), anyhow::Error> {
    let start = std::time::Instant::now();
    cancel.check("merge input planning")?;

    // batch selection groups same-kind files only (see merge_by_stream); a
    // mixed group would corrupt whichever writer ran, so reject it loudly
    let is_core_group = files_with_size
        .first()
        .is_some_and(|f| f.key.ends_with(config::FILE_EXT_VIX));
    if files_with_size
        .iter()
        .any(|f| f.key.ends_with(config::FILE_EXT_VIX) != is_core_group)
    {
        return Err(anyhow::anyhow!(
            "merge_files got a mixed core/flat file group: {:?}",
            files_with_size.iter().map(|f| &f.key).collect::<Vec<_>>()
        ));
    }

    #[cfg(feature = "enterprise")]
    let is_match_downsampling_rule = get_largest_downsampling_rule(
        stream_name,
        files_with_size.iter().map(|f| f.meta.max_ts).max().unwrap(),
    )
    .is_some();

    #[cfg(not(feature = "enterprise"))]
    let is_match_downsampling_rule = false;

    // A single-file CORE batch is a deliberate healing rebuild
    // (merge_by_stream's capability probe enqueues it): let it through the
    // >= 2 guards and the size budget — the rebuilt output replaces the
    // input at roughly its own size, so the group-size cap does not apply.
    let is_single_core_heal = is_core_group && files_with_size.len() == 1;
    let is_indexed_core_group =
        is_core_group && files_with_size.iter().all(|file| file.meta.index_size > 0);

    if files_with_size.len() <= 1 && !is_match_downsampling_rule && !is_single_core_heal {
        return Ok((Vec::new(), Vec::new()));
    }

    let mut new_file_size = 0;
    let mut new_compressed_file_size = 0;
    let mut new_file_list = Vec::new();
    let cfg = get_config();
    let max_file_size = cfg
        .compact
        .max_file_size_for_merge(stream_type, is_indexed_core_group) as i64;
    for file in files_with_size.iter() {
        if (new_file_size + file.meta.original_size > max_file_size
            || new_compressed_file_size + file.meta.compressed_size > max_file_size
            // #51: bound the merge WIDTH too — bytes alone let sliver-debt
            // hours stack 1,600+ files into one k-way merge (memory tracks
            // width; heap CPU superlinear). The remainder merges next pass.
            || (cfg.compact.max_file_count > 0
                && new_file_list.len() >= cfg.compact.max_file_count as usize))
            && !is_match_downsampling_rule
            && !is_single_core_heal
        {
            break;
        }
        new_file_size += file.meta.original_size;
        new_compressed_file_size += file.meta.compressed_size;
        new_file_list.push(file.clone());
        // metrics
        metrics::COMPACT_MERGED_FILES
            .with_label_values(&[org_id, stream_type.as_str()])
            .inc();
        metrics::COMPACT_MERGED_BYTES
            .with_label_values(&[org_id, stream_type.as_str()])
            .inc_by(file.meta.original_size as u64);
    }
    // no files need to merge
    if new_file_list.len() <= 1 && !is_match_downsampling_rule && !is_single_core_heal {
        return Ok((Vec::new(), Vec::new()));
    }

    // Cache merge inputs and VIX sidecars under a bounded byte/request
    // budget. Keep this phase separate from merge CPU in production
    // telemetry so object-store latency cannot masquerade as encoder cost.
    let prefetch_started = std::time::Instant::now();
    let deleted_files = cache_remote_files(&new_file_list).await?;
    cancel.check("merge input prefetch")?;
    metrics::COMPACT_VIX_PHASE_DURATION
        .with_label_values(&["prefetch", if is_core_group { "core" } else { "flat" }])
        .observe(prefetch_started.elapsed().as_secs_f64());
    log::info!(
        "[COMPACTOR:WORKER:{thread_id}] prefetched {} merge input data object(s) and eligible VIX sidecar(s), took: {} ms",
        new_file_list.len(),
        start.elapsed().as_millis()
    );
    if !deleted_files.is_empty() {
        new_file_list.retain(|f| !deleted_files.contains(&f.key));
    }
    // (a heal whose only file was dropped as invalid has nothing left to
    // rebuild — cache_remote_files already removed it from the file_list)
    if new_file_list.len() <= 1
        && !is_match_downsampling_rule
        && !(is_single_core_heal && new_file_list.len() == 1)
    {
        // Not enough healthy inputs left to merge: return an EMPTY merged
        // set so the caller releases the batch with no file_list writes.
        // Returning the batch here used to commit a PURE DELETION of every
        // input — including the healthy survivor — with no replacement
        // output (permanent loss, 2026-07-30 audit).
        return Ok((Vec::new(), Vec::new()));
    }

    // From here on new_file_list is EXACTLY the input set the merge
    // consumes; the snapshot is what the caller may delete after commit.
    let retain_file_list = new_file_list.clone();

    // get time range and stats for these files in a single iteration
    let (min_ts, max_ts, total_records, new_file_size) = new_file_list.iter().fold(
        (i64::MAX, i64::MIN, 0, 0),
        |(min_ts, max_ts, records, size), file| {
            (
                min_ts.min(file.meta.min_ts),
                max_ts.max(file.meta.max_ts),
                records + file.meta.records,
                size + file.meta.original_size,
            )
        },
    );
    let min_ts = if min_ts == i64::MAX { 0 } else { min_ts };
    let max_ts = if max_ts == i64::MIN { 0 } else { max_ts };
    let new_file_meta = FileMeta {
        min_ts,
        max_ts,
        records: total_records,
        original_size: new_file_size,
        compressed_size: 0,
        flattened: false,
        index_size: 0,
        bloom_ver: 0,
    };
    if new_file_meta.records == 0 {
        return Err(anyhow::anyhow!("merge_files error: records is 0"));
    }

    // get latest version of schema
    let latest_schema = infra::schema::get(org_id, stream_name, stream_type).await?;
    let stream_settings = infra::schema::unwrap_stream_settings(&latest_schema);
    let bloom_filter_fields = get_stream_setting_bloom_filter_fields(&stream_settings);
    let full_text_search_fields = get_stream_setting_fts_fields(&stream_settings);
    let storage_type = stream_settings
        .map(|s| s.storage_type)
        .unwrap_or(StorageType::Normal);
    let latest_schema = Arc::new(latest_schema);

    // core files: k-way merge by _timestamp without DataFusion, index
    // rebuilt from _source with the current settings
    if is_core_group {
        return merge_core_group(
            thread_id,
            org_id,
            stream_type,
            stream_name,
            prefix,
            new_file_list,
            retain_file_list,
            new_file_meta,
            latest_schema,
            full_text_search_fields,
            bloom_filter_fields,
            storage_type,
            is_single_core_heal,
            cancel,
            start,
        )
        .await;
    }

    // read schema from parquet file and group files by schema
    let mut schemas = HashMap::new();
    let files = new_file_list.clone();
    let mut fi = 0;
    for file in new_file_list.iter() {
        cancel.check("flat-file schema scan")?;
        fi += 1;
        log::info!(
            "[COMPACTOR:WORKER:{thread_id}:{fi}] merge small file: {}",
            file.key
        );
        let buf = file_data::get(&file.account, &file.key, None).await?;
        let file_format = FileFormat::from_extension(&file.key)
            .ok_or_else(|| anyhow::anyhow!("invalid file format: {}", file.key))?;
        let schema = match read_schema_from_bytes(file_format, &buf).await {
            Ok(schema) => schema,
            Err(e) => {
                log::error!(
                    "[COMPACTOR:WORKER:{thread_id}:{fi}] read schema error for file: {}, err: {e}",
                    file.key
                );
                return Err(e);
            }
        };
        let schema = schema.as_ref().clone().with_metadata(Default::default());
        let schema_key = schema.hash_key();
        if !schemas.contains_key(&schema_key) {
            schemas.insert(schema_key.clone(), schema);
        }
    }

    // generate the parquet schema
    let all_fields = schemas
        .values()
        .flat_map(|s| s.fields().iter().map(|f| f.name().to_string()))
        .collect::<HashSet<_>>();
    let schema = Arc::new(latest_schema.retain(all_fields));

    // generate datafusion tables
    let trace_id = ider::generate();
    let session = config::meta::search::Session {
        id: trace_id.to_string(),
        storage_type: config::meta::search::StorageType::Memory,
        work_group: None,
        target_partitions: 2,
    };

    let tables = match TableBuilder::new()
        .sorted_by_time(true)
        .build(session, files.clone(), schema.clone())
        .await
    {
        Ok(tables) => tables,
        Err(e) => {
            log::error!("create_parquet_table err: {e}, files: {files:?}, schema: {schema:?}");
            return Err(DataFusionError::Plan(format!("create_parquet_table err: {e}")).into());
        }
    };

    // M20b: METADATA-class merges plan single-partition (see
    // compact_single_partition_sort for the prod rationale)
    let single_partition_sort = compact_single_partition_sort(stream_type);
    let merge_result = {
        cancel.check("flat-file DataFusion merge")?;
        let stream_name = stream_name.to_string();
        DATAFUSION_RUNTIME
            .spawn(async move {
                merge::merge_parquet_files(
                    stream_type,
                    &stream_name,
                    schema,
                    tables,
                    &bloom_filter_fields,
                    new_file_meta,
                    false,
                    single_partition_sort,
                )
                .await
            })
            .await?
    };
    cancel.check("flat-file upload")?;

    // clear session data
    crate::service::search::datafusion::storage::file_list::clear(&trace_id);

    let files = new_file_list.into_iter().map(|f| f.key).collect::<Vec<_>>();
    let buf = match merge_result {
        Ok(v) => v,
        Err(e) => {
            log::error!("merge_parquet_files err: {e}, files: {files:?}");
            return Err(DataFusionError::Plan(format!("merge_parquet_files err: {e}")).into());
        }
    };

    let mut new_files = Vec::new();
    match buf {
        MergeParquetResult::Single {
            buf,
            file_meta: new_file_meta,
            file_format,
        } => {
            if new_file_meta.compressed_size == 0 {
                return Err(anyhow::anyhow!(
                    "merge_parquet_files error: compressed_size is 0"
                ));
            }

            let id = ider::generate_file_name();
            let new_file_key = format!("{prefix}/{id}{}", file_format.extension());
            log::info!(
                "[COMPACTOR:WORKER:{thread_id}] merged {} files into a new file: {new_file_key}, original_size: {}, compressed_size: {}, took: {} ms",
                retain_file_list.len(),
                new_file_meta.original_size,
                new_file_meta.compressed_size,
                start.elapsed().as_millis(),
            );

            // upload file to storage
            let buf = Bytes::from(buf);
            if cfg.cache_latest_files.enabled
                && cfg.cache_latest_files.cache_parquet
                && cfg.cache_latest_files.download_from_node
            {
                infra::cache::file_data::disk::set(&new_file_key, buf.clone()).await?;
                log::debug!("merge_files {new_file_key} file_data::disk::set success");
            }

            // TODO: check how compliance will interact with org storage
            let account = storage::get_account(org_id, &new_file_key).unwrap_or_default();
            put_merged_output(
                &account,
                &new_file_key,
                buf.clone(),
                cfg.s3.feature_force_infrequent_access && storage_type.is_compliance(),
                cancel,
            )
            .await?;

            // legacy flat outputs (metrics + pre-core logs/traces parquet)
            // get no inverted index: the v1 sidecar builder was removed, and
            // index-less files are answered by the scan path
            new_files.push(FileKey::new(0, account, new_file_key, new_file_meta, false));
        }
        MergeParquetResult::Multiple {
            bufs,
            file_metas,
            file_format,
        } => {
            for (buf, file_meta) in bufs.into_iter().zip(file_metas) {
                let mut new_file_meta = file_meta;
                new_file_meta.compressed_size = buf.len() as i64;
                if new_file_meta.compressed_size == 0 {
                    return Err(anyhow::anyhow!(
                        "merge_parquet_files error: compressed_size is 0"
                    ));
                }

                let id = ider::generate_file_name();
                let new_file_key = format!("{prefix}/{id}{}", file_format.extension());

                // upload file to storage
                let buf = Bytes::from(buf);
                if cfg.cache_latest_files.enabled
                    && cfg.cache_latest_files.cache_parquet
                    && cfg.cache_latest_files.download_from_node
                {
                    infra::cache::file_data::disk::set(&new_file_key, buf.clone()).await?;
                    log::debug!("merge_files {new_file_key} file_data::disk::set success");
                }

                // TODO: check how compliance will interact with org storage
                let account = storage::get_account(org_id, &new_file_key).unwrap_or_default();
                put_merged_output(
                    &account,
                    &new_file_key,
                    buf.clone(),
                    cfg.s3.feature_force_infrequent_access && storage_type.is_compliance(),
                    cancel,
                )
                .await?;

                new_files.push(FileKey::new(0, account, new_file_key, new_file_meta, false));
            }
            log::info!(
                "[COMPACTOR:WORKER:{thread_id}] merged {} files into a new file: {:?}, original_size: {}, compressed_size: {}, took: {} ms",
                retain_file_list.len(),
                new_files.iter().map(|f| f.key.as_str()).collect::<Vec<_>>(),
                new_files.iter().map(|f| f.meta.original_size).sum::<i64>(),
                new_files
                    .iter()
                    .map(|f| f.meta.compressed_size)
                    .sum::<i64>(),
                start.elapsed().as_millis(),
            );
        }
    };

    Ok((new_files, retain_file_list))
}

/// Upload one merged object with a bounded retry (3 attempts, doubling
/// backoff): a transient object-store failure must not throw away a
/// multi-second merge — the whole merge would otherwise re-run from scratch.
async fn put_merged_output(
    account: &str,
    new_file_key: &str,
    buf: Bytes,
    compliance: bool,
    cancel: &MergeCancellation,
) -> Result<(), anyhow::Error> {
    const MAX_ATTEMPTS: usize = 3;
    let mut backoff = tokio::time::Duration::from_millis(500);
    let mut last_err: Option<anyhow::Error> = None;
    for attempt in 1..=MAX_ATTEMPTS {
        cancel.check(&format!("upload attempt {attempt} for {new_file_key}"))?;
        let ret = if compliance {
            storage::put_with_compliance(account, new_file_key, buf.clone()).await
        } else {
            storage::put(account, new_file_key, buf.clone()).await
        };
        match ret {
            Ok(()) => return Ok(()),
            Err(e) => {
                log::warn!(
                    "[COMPACTOR] upload of merged file {new_file_key} failed (attempt {attempt}/{MAX_ATTEMPTS}): {e}",
                );
                last_err = Some(e.into());
                if attempt < MAX_ATTEMPTS {
                    tokio::select! {
                        _ = tokio::time::sleep(backoff) => {}
                        _ = wait_for_merge_cancellation(cancel) => {
                            cancel.check(&format!("upload retry backoff for {new_file_key}"))?;
                        }
                    }
                    backoff *= 2;
                }
            }
        }
    }
    Err(last_err.expect("at least one upload attempt ran"))
}

/// [`put_merged_output`] for a SPOOLED merge output: stream the local file
/// to object storage (bounded multipart, no in-memory copy of the object)
/// with the same bounded retry. The spool persists across attempts, so a
/// retry re-streams from disk instead of re-running the merge.
async fn put_merged_output_file(
    account: &str,
    new_file_key: &str,
    spool: &std::path::Path,
    compliance: bool,
    cancel: &MergeCancellation,
) -> Result<(), anyhow::Error> {
    const MAX_ATTEMPTS: usize = 3;
    let mut backoff = tokio::time::Duration::from_millis(500);
    let mut last_err: Option<anyhow::Error> = None;
    for attempt in 1..=MAX_ATTEMPTS {
        cancel.check(&format!(
            "streaming upload attempt {attempt} for {new_file_key}"
        ))?;
        let ret = if compliance {
            storage::put_file_with_compliance(account, new_file_key, spool).await
        } else {
            storage::put_file(account, new_file_key, spool).await
        };
        match ret {
            Ok(()) => return Ok(()),
            Err(e) => {
                log::warn!(
                    "[COMPACTOR] streaming upload of merged file {new_file_key} failed (attempt {attempt}/{MAX_ATTEMPTS}): {e}",
                );
                last_err = Some(e.into());
                if attempt < MAX_ATTEMPTS {
                    tokio::select! {
                        _ = tokio::time::sleep(backoff) => {}
                        _ = wait_for_merge_cancellation(cancel) => {
                            cancel.check(&format!("streaming upload retry backoff for {new_file_key}"))?;
                        }
                    }
                    backoff *= 2;
                }
            }
        }
    }
    Err(last_err.expect("at least one upload attempt ran"))
}

/// Await cancellation without a permanently-running helper task. The short
/// poll is used only during bounded retry backoff; CPU-heavy loops perform
/// direct atomic checks at their natural batch/window boundaries.
async fn wait_for_merge_cancellation(cancel: &MergeCancellation) {
    while !cancel.is_cancelled() {
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }
}
#[inline]
fn indexed_trace_group_exceeds_global_rebuild_limit(
    stream_type: StreamType,
    all_inputs_indexed: bool,
    original_size: i64,
    compressed_size: i64,
    global_limit: i64,
) -> bool {
    stream_type == StreamType::Traces
        && all_inputs_indexed
        && (original_size > global_limit || compressed_size > global_limit)
}

/// Merge one same-kind group of core `.vix` files into a single core
/// file and upload it. The inputs come through the same disk-cache ladder as
/// parquet compaction; the CPU-bound k-way merge + index rebuild
/// (`vix::core_writer::merge_core_files`) runs on a blocking thread.
///
/// `force_rebuild` marks a single-file healing batch. Its sidecar-only repair
/// runs first; a required docs rewrite may use the full rebuild only while
/// both input byte measures fit the global rebuild-safe ceiling.
#[allow(clippy::too_many_arguments)]
async fn merge_core_group(
    thread_id: usize,
    org_id: &str,
    stream_type: StreamType,
    stream_name: &str,
    prefix: &str,
    new_file_list: Vec<FileKey>,
    retain_file_list: Vec<FileKey>,
    mut new_file_meta: FileMeta,
    latest_schema: Arc<Schema>,
    full_text_search_fields: Vec<String>,
    bloom_filter_fields: Vec<String>,
    storage_type: StorageType,
    force_rebuild: bool,
    cancel: &MergeCancellation,
    start: std::time::Instant,
) -> Result<(Vec<FileKey>, Vec<FileKey>), anyhow::Error> {
    let cfg = get_config();
    cancel.check("core merge planning")?;
    let all_inputs_indexed = new_file_list.iter().all(|file| file.meta.index_size > 0);
    let input_compressed_size = new_file_list.iter().fold(0_i64, |total, file| {
        total.saturating_add(file.meta.compressed_size)
    });
    let exceeds_global_rebuild_limit = indexed_trace_group_exceeds_global_rebuild_limit(
        stream_type,
        all_inputs_indexed,
        new_file_meta.original_size,
        input_compressed_size,
        cfg.compact.max_file_size as i64,
    );

    // M3 SIDECAR-ONLY HEAL (DESIGN-V2 §5): a single-file healing batch
    // rewrites ONLY the `.vxi` index sidecar — same sidecar key, data
    // object untouched, the EXISTING file_list row updated in place — and
    // commits no add/delete events at all. Only a heal that genuinely
    // rewrites docs (degenerate-_timestamp cleansing; an oversize-skip set
    // the data-side allowance cannot cover) falls through to the
    // whole-file rebuild below. Classification reasons are unchanged
    // (single_core_file_heal_reason); only the execution changed.
    if force_rebuild && new_file_list.len() == 1 {
        let healed = sidecar_only_heal(
            thread_id,
            org_id,
            stream_type,
            stream_name,
            &new_file_list[0],
            Arc::clone(&latest_schema),
            full_text_search_fields.clone(),
            bloom_filter_fields.clone(),
            cfg.s3.feature_force_infrequent_access && storage_type.is_compliance(),
            cancel,
            start,
        )
        .await?;
        if healed {
            // healed in place: release the batch with no file_list events
            // (the row update + broadcast already happened)
            return Ok((Vec::new(), Vec::new()));
        }
    }
    if force_rebuild && exceeds_global_rebuild_limit {
        return Err(anyhow::anyhow!(
            "[COMPACTOR:WORKER:{thread_id}] refusing oversized indexed trace rebuild after \
             sidecar-only healing required a docs rewrite: inputs={}, original_size={}, \
             compressed_size={}, global_rebuild_limit={}",
            new_file_list.len(),
            new_file_meta.original_size,
            input_compressed_size,
            cfg.compact.max_file_size,
        ));
    }

    // The merge reads its inputs by RANGE through the cache ladder
    // (memory/disk cache first — cache_remote_files just filled the disk
    // cache — with transparent remote fallback if the cache evicts a file
    // mid-merge): input files are never materialized whole in memory. The
    // ranged source is the healing probe's, fetch-metered under `compact`;
    // for `.vix` files `compressed_size` is the exact object size.
    let handle = tokio::runtime::Handle::current();
    let inputs: Vec<crate::service::vix::core_writer::MergeInput> = new_file_list
        .iter()
        .map(|file| {
            let source: Arc<dyn vortex_index::VixRangeSource> = Arc::new(HealProbeRangeSource {
                account: file.account.clone(),
                location: object_store::path::Path::from(file.key.as_str()),
                size: file.meta.compressed_size as u64,
                handle: handle.clone(),
                cancel: Some(cancel.clone()),
            });
            // v3 split: the index sidecar is its own object; index_size is
            // its exact size and 0 means no sidecar (docs-only rebuild).
            let index_source: Option<Arc<dyn vortex_index::VixRangeSource>> =
                config::vix_sidecar_key(&file.key)
                    .filter(|_| file.meta.index_size > 0)
                    .map(|sidecar_key| {
                        Arc::new(HealProbeRangeSource {
                            account: file.account.clone(),
                            location: object_store::path::Path::from(sidecar_key.as_str()),
                            size: file.meta.index_size as u64,
                            handle: handle.clone(),
                            cancel: Some(cancel.clone()),
                        }) as Arc<dyn vortex_index::VixRangeSource>
                    });
            (file.key.clone(), source, index_source)
        })
        .collect();

    // M31 index-defer policy: a NON-FINAL group — every input index-less
    // (L0s and previously deferred outputs; the homogeneous grouping cuts
    // groups that way) summing under the configured line — writes a
    // column-store-only output: its index would be discarded by the next
    // hop anyway. Never for healing batches (their whole point is building
    // the index) and never when any input already carries a sidecar (a
    // deferred output over it would DROP that capability).
    let defer_below_bytes = cfg
        .common
        .vix_merge_index_defer_below_mb
        .saturating_mul(1024 * 1024) as i64;
    let index_deferred = !force_rebuild
        && defer_below_bytes > 0
        && new_file_list.len() > 1
        && new_file_meta.original_size < defer_below_bytes
        && new_file_list.iter().all(|f| f.meta.index_size == 0);
    // The trace-only enlarged target is safe only on the indexed merge path.
    // A normal-size batch may still rebuild to heal an incompatible input;
    // a batch above the global rebuild-safe ceiling must fail rather than
    // silently multiplying rebuild memory.
    let require_indexed_merge = !force_rebuild && exceeds_global_rebuild_limit;

    let cpu_permit = acquire_vix_cpu(cancel, "core merge CPU admission").await?;
    let merge_started = std::time::Instant::now();
    cancel.check("core merge CPU phase")?;
    let vix_cancellation = cancel.vix_token();
    let mut merge_task = tokio::task::spawn_blocking(move || {
        if force_rebuild {
            crate::service::vix::core_writer::merge_core_files_rebuild_with_cancellation(
                stream_type,
                &inputs,
                &latest_schema,
                &full_text_search_fields,
                &bloom_filter_fields,
                &vix_cancellation,
            )
        } else if index_deferred {
            crate::service::vix::core_writer::merge_core_files_index_deferred_with_cancellation(
                stream_type,
                &inputs,
                &latest_schema,
                &full_text_search_fields,
                &bloom_filter_fields,
                &vix_cancellation,
            )
        } else if require_indexed_merge {
            crate::service::vix::core_writer::merge_core_files_indexed_only_with_cancellation(
                stream_type,
                &inputs,
                &latest_schema,
                &full_text_search_fields,
                &bloom_filter_fields,
                &vix_cancellation,
            )
        } else {
            crate::service::vix::core_writer::merge_core_files_with_cancellation(
                stream_type,
                &inputs,
                &latest_schema,
                &full_text_search_fields,
                &bloom_filter_fields,
                &vix_cancellation,
            )
        }
    });
    let merge_join = tokio::select! {
        result = &mut merge_task => result,
        _ = wait_for_merge_cancellation(cancel) => {
            // `spawn_blocking` cannot be force-aborted once running. The
            // shared token makes it leave at the next bounded VIX boundary;
            // await it so shutdown never detaches a CPU-heavy merge.
            cancel.cancel();
            merge_task.await
        }
    };
    let result = match merge_join? {
        Ok(result) => result,
        Err(e) => {
            // M19: a mid-merge range fetch hitting an externally deleted
            // object (S3 lifecycle expiry) would otherwise fail EVERY retry
            // of this job forever — the not-found path above only runs when
            // the disk-cache pre-download runs (it is skipped for local
            // storage / over-skip_size batches, and a cached file can be
            // evicted mid-merge). Reconcile here: HEAD each input and remove
            // the file_list row (and object pair, idempotent) of every
            // vanished one, then fail the job — its retry claims only
            // surviving rows.
            if storage::is_not_found_error(&e) {
                let removed = reconcile_missing_merge_inputs(&retain_file_list).await;
                if removed > 0 {
                    log::warn!(
                        "[COMPACTOR:WORKER:{thread_id}] {org_id}/{stream_type}/{stream_name}: merge failed on a missing object; removed {removed} file_list rows of {} inputs whose objects are gone (deleted externally), the job retry proceeds without them",
                        retain_file_list.len(),
                    );
                }
            }
            return Err(e);
        }
    };
    drop(cpu_permit);
    let merge_shape = if result.used_index_merge {
        "index_merge"
    } else if result.docs_passthrough_inputs > 0 {
        "docs_copy_rebuild"
    } else if result.terms_from_columns {
        "column_rebuild"
    } else {
        "source_rebuild"
    };
    metrics::COMPACT_VIX_PHASE_DURATION
        .with_label_values(&["merge", merge_shape])
        .observe(merge_started.elapsed().as_secs_f64());
    cancel.check("core merge upload")?;

    // Compaction-time cleansing: the merge DROPS stored rows whose
    // `_timestamp` is degenerate (<= 0) — pre-guard-era files hide such
    // rows behind healthy-looking file_list metadata, so only a data read
    // (i.e. a merge) can find them, and without cleansing every merge
    // touching one fails the writer's finish guard forever. ONE loud WARN +
    // counter per merge output.
    if result.dropped_rows > 0 {
        metrics::COMPACT_DROPPED_ZERO_TS_ROWS
            .with_label_values(&[org_id, stream_type.as_str(), stream_name])
            .inc_by(result.dropped_rows);
        if result.stats.row_count == 0 {
            // Every input row was poison: there is nothing to write. Commit
            // the merge as "inputs deleted, no output file" — the caller's
            // event loop pushes only the input deletes (empty new_files is
            // the supported shape: keys are filtered on emptiness), and
            // batch_process handles a delete-only batch (the adds INSERT is
            // skipped entirely), moving the inputs to file_list_deleted for
            // GC. Uploading the zero-row file instead would publish a
            // useless object with a records=0 meta.
            log::warn!(
                "[COMPACTOR:WORKER:{thread_id}] {org_id}/{stream_type}/{stream_name}: every row \
                 of {} input core files under {prefix} carries a degenerate _timestamp <= 0 \
                 ({} rows dropped by cleansing); deleting the inputs with no merged output",
                retain_file_list.len(),
                result.dropped_rows,
            );
            return Ok((Vec::new(), retain_file_list));
        }
        log::warn!(
            "[COMPACTOR:WORKER:{thread_id}] {org_id}/{stream_type}/{stream_name}: dropped {} \
             rows with a degenerate _timestamp <= 0 while merging {} core files under {prefix} \
             (pre-guard stored data cleansed at compaction)",
            result.dropped_rows,
            retain_file_list.len(),
        );
        // the folded input metas counted the dropped rows; align records so
        // the meta fold below sees agreement (its WARN stays an anomaly
        // signal for genuine meta-vs-data divergence)
        new_file_meta.records -= result.dropped_rows as i64;
    }

    // sizes + the authoritative records/min_ts/max_ts from the DATA the
    // merge wrote, not from the inputs' file_list rows — inputs with
    // degenerate ranges (min_ts = 0 from the historical WAL-meta bug) would
    // otherwise poison the merged row, while re-deriving here heals them at
    // compaction. A meta that is STILL degenerate errors here: failing the
    // merge (the job returns to pending) beats committing a row that
    // poisons pruning and wedges the file_list write.
    crate::service::vix::core_writer::apply_core_stats_to_meta(
        &mut new_file_meta,
        result.output.len() as usize,
        &result.stats,
        &format!("[COMPACTOR:WORKER:{thread_id}] {prefix}"),
    )?;
    if new_file_meta.compressed_size == 0 {
        return Err(anyhow::anyhow!(
            "merge_core_files error: compressed_size is 0"
        ));
    }

    let id = ider::generate_file_name();
    let new_file_key = format!("{prefix}/{id}{}", FileFormat::Vix.extension());
    log::info!(
        "[COMPACTOR:WORKER:{thread_id}] merged {} core files into a new file: {new_file_key}, original_size: {}, compressed_size: {}, index_merge: {}, docs_passthrough: {}, concat_order: {}, terms_from_columns: {}, order_entries: {}, staged_empty_arrays: {}, interleaved_columns: {}, took: {} ms",
        retain_file_list.len(),
        new_file_meta.original_size,
        new_file_meta.compressed_size,
        result.used_index_merge,
        result.docs_passthrough_inputs,
        result.concat_order,
        result.terms_from_columns,
        result.perf.order_entries_materialized,
        result.perf.staged_empty_arrays,
        result.perf.interleaved_columns,
        start.elapsed().as_millis(),
    );

    // Upload to storage (the core file is the data file: cache_parquet
    // gates the local-cache copy, exactly like a merged parquet object).
    // Production merges SPOOL the container to the data volume — the
    // upload streams from the spool file and the merged multi-GB object
    // never resides in RAM; the spool deletes when `result.output` drops.
    let account = storage::get_account(org_id, &new_file_key).unwrap_or_default();
    let compliance = cfg.s3.feature_force_infrequent_access && storage_type.is_compliance();
    let cache_locally = cfg.cache_latest_files.enabled
        && cfg.cache_latest_files.cache_parquet
        && cfg.cache_latest_files.download_from_node;
    match &result.output {
        VixOutput::Bytes(_) => {
            // SPOOL-ALWAYS (H3/§7): build_merge_plan sets output_spool_dir
            // unconditionally, so a merge output arriving in RAM means the
            // invariant drifted — alarm in debug builds, and in release
            // refuse the buffered upload arm for anything beyond a trivial
            // buffer: spool it to the scratch dir and stream (the same
            // multipart put_file the spooled arm uses).
            debug_assert!(
                false,
                "merge output arrived in RAM ({} bytes): build_merge_plan must spool merge \
                 outputs",
                result.output.len()
            );
            let buf = Bytes::from(result.output.to_bytes()?);
            if buf.len() as u64 >= BUFFERED_UPLOAD_MAX_BYTES {
                let scratch = std::path::Path::new(&cfg.common.data_dir).join("vix_spill");
                tokio::fs::create_dir_all(&scratch).await?;
                let spool = scratch.join(format!("{}.upload.spool", ider::generate()));
                tokio::fs::write(&spool, &buf).await?;
                drop(buf);
                let ret = async {
                    if cache_locally {
                        infra::cache::file_data::disk::set_from_local_file(&new_file_key, &spool)
                            .await?;
                    }
                    put_merged_output_file(&account, &new_file_key, &spool, compliance, cancel)
                        .await
                }
                .await;
                let _ = tokio::fs::remove_file(&spool).await;
                ret?;
            } else {
                if cache_locally {
                    infra::cache::file_data::disk::set(&new_file_key, buf.clone()).await?;
                    log::debug!("merge_files {new_file_key} file_data::disk::set success");
                }
                put_merged_output(&account, &new_file_key, buf, compliance, cancel).await?;
            }
        }
        VixOutput::Spooled { .. } => {
            let spool = result
                .output
                .spool_path()
                .expect("spooled output has a path");
            if cache_locally {
                // file-to-file copy: the merged object never transits RAM
                // (the pre-M3 read-back buffered the whole object)
                infra::cache::file_data::disk::set_from_local_file(&new_file_key, spool).await?;
                log::debug!("merge_files {new_file_key} file_data::disk::set success");
            }
            put_merged_output_file(&account, &new_file_key, spool, compliance, cancel).await?;
        }
    }
    drop(result.output);

    // v3 split: upload the `.vxi` index sidecar AFTER the data object and
    // BEFORE the file_list row commits — a crash in between leaves orphan
    // objects without a row, exactly today's semantics. Same account as the
    // data object (one logical file, one placement).
    if let Some(index_bytes) = result.index {
        let sidecar_key = config::vix_sidecar_key(&new_file_key)
            .expect("merge outputs are .vix keys by construction");
        debug_assert_eq!(index_bytes.len() as u64, new_file_meta.index_size as u64);
        let buf = Bytes::from(index_bytes);
        if cache_locally {
            infra::cache::file_data::disk::set(&sidecar_key, buf.clone()).await?;
        }
        put_merged_output(&account, &sidecar_key, buf, compliance, cancel).await?;
    }

    Ok((
        vec![FileKey::new(0, account, new_file_key, new_file_meta, false)],
        retain_file_list,
    ))
}

/// Execute the M3 sidecar-only heal for ONE core file (DESIGN-V2 §5):
/// rebuild the `.vxi` with current settings over the UNTOUCHED data
/// object, overwrite the SAME sidecar key, and update the EXISTING
/// file_list row (`index_size` + `bloom_ver = 0`) — no new file id, no
/// data-key change, no data upload. Returns Ok(true) when healed in place
/// (or the index-off heal dropped the sidecar), Ok(false) when the file
/// genuinely needs the docs-rewriting rebuild.
///
/// Consistency: the row update carries NO job-ownership fence — unlike an
/// add+delete commit it cannot duplicate or lose rows. Two racing healers
/// write equivalent sidecars (same docs, same settings) and the row; the
/// worst interleave leaves the row's `index_size` disagreeing with the
/// object, which readers fail-open on and the next sweep re-classifies
/// `NeedsRebuild` — convergent. Staleness is acceptable by design: docs
/// are unchanged, so a reader briefly on the old cached sidecar serves
/// pre-heal (correct) results; the broadcast below evicts/refreshes.
///
/// `.bf` flow: `bloom_ver = 0` + `index_size > 0` re-enters the file into
/// the bloom assembler queue, which transposes the NEW sidecar's bloom
/// blob into a fresh `.bf` chunk (the pruner treats `bloom_ver <= 0` as
/// no-bloom meanwhile) — identical to how a freshly merged file enters
/// the queue; bloom_ver semantics unchanged.
#[allow(clippy::too_many_arguments)]
async fn sidecar_only_heal(
    thread_id: usize,
    org_id: &str,
    stream_type: StreamType,
    stream_name: &str,
    file: &FileKey,
    latest_schema: Arc<Schema>,
    full_text_search_fields: Vec<String>,
    bloom_filter_fields: Vec<String>,
    compliance: bool,
    cancel: &MergeCancellation,
    start: std::time::Instant,
) -> Result<bool, anyhow::Error> {
    use crate::service::vix::core_writer::{SidecarHealOutcome, rebuild_core_file_sidecar};

    cancel.check("sidecar heal planning")?;

    let handle = tokio::runtime::Handle::current();
    let source: Arc<dyn vortex_index::VixRangeSource> = Arc::new(HealProbeRangeSource {
        account: file.account.clone(),
        location: object_store::path::Path::from(file.key.as_str()),
        size: file.meta.compressed_size as u64,
        handle: handle.clone(),
        cancel: Some(cancel.clone()),
    });
    let index_source: Option<Arc<dyn vortex_index::VixRangeSource>> =
        config::vix_sidecar_key(&file.key)
            .filter(|_| file.meta.index_size > 0)
            .map(|sidecar_key| {
                Arc::new(HealProbeRangeSource {
                    account: file.account.clone(),
                    location: object_store::path::Path::from(sidecar_key.as_str()),
                    size: file.meta.index_size as u64,
                    handle,
                    cancel: Some(cancel.clone()),
                }) as Arc<dyn vortex_index::VixRangeSource>
            });
    let input = (file.key.clone(), source, index_source);
    let cpu_permit = acquire_vix_cpu(cancel, "sidecar heal CPU admission").await?;
    let mut heal_task = tokio::task::spawn_blocking(move || {
        rebuild_core_file_sidecar(
            stream_type,
            &input,
            &latest_schema,
            &full_text_search_fields,
            &bloom_filter_fields,
        )
    });
    let heal_join = tokio::select! {
        result = &mut heal_task => result,
        _ = wait_for_merge_cancellation(cancel) => {
            cancel.cancel();
            heal_task.await
        }
    };
    let outcome = heal_join??;
    drop(cpu_permit);
    cancel.check("sidecar heal upload")?;

    let sidecar_key = config::vix_sidecar_key(&file.key)
        .ok_or_else(|| anyhow::anyhow!("healing batches carry .vix keys: {}", file.key))?;
    let new_index_size = match outcome {
        SidecarHealOutcome::NeedsDocsRewrite(reason) => {
            log::info!(
                "[COMPACTOR:WORKER:{thread_id}] {org_id}/{stream_type}/{stream_name}: \
                 sidecar-only heal of {} needs a docs rewrite ({reason}); taking the whole-file \
                 rebuild",
                file.key,
            );
            return Ok(false);
        }
        SidecarHealOutcome::Rebuilt { index, stats } => {
            debug_assert_eq!(index.len() as u64, stats.index_size);
            let index_size = index.len() as i64;
            // overwrite the SAME sidecar key (bounded retry). A crash after
            // the PUT and before the row update leaves the row's index_size
            // pointing into the new object at the old length: readers
            // fail-open on the unreadable pair and the next sweep
            // re-classifies NeedsRebuild — convergent, never corrupt (doc
            // ids address the unchanged docs either way).
            put_merged_output(
                &file.account,
                &sidecar_key,
                Bytes::from(index),
                compliance,
                cancel,
            )
            .await?;
            index_size
        }
        // Index-off policy heal: metadata-only. The row zeroes FIRST
        // (readers gate the sidecar fetch on index_size), the object
        // deletes after; a crash in between orphans a `.vxi` for the
        // lifecycle GC.
        SidecarHealOutcome::DropSidecar => 0,
    };

    // Update the EXISTING row in place (remote, then the local mirror):
    // index_size to the new sidecar's size and bloom_ver back to 0.
    infra_file_list::update_index_size_for_heal(&file.key, new_index_size).await?;
    if let Err(e) = infra_file_list::LOCAL_CACHE
        .update_index_size_for_heal(&file.key, new_index_size)
        .await
    {
        log::warn!(
            "[COMPACTOR:WORKER:{thread_id}] sidecar heal local-cache update for {} failed: {e}",
            file.key,
        );
    }

    if new_index_size == 0
        && let Err(e) = storage::del(vec![(file.account.as_str(), sidecar_key.as_str())]).await
    {
        log::warn!(
            "[COMPACTOR:WORKER:{thread_id}] sidecar heal delete of {sidecar_key} failed \
             (orphan .vxi, lifecycle GC covers it): {e}",
        );
    }

    // Evict this node's own stale cache entries so later local ranged
    // reads (probes, follow-up merges) see the new bytes.
    let _ = file_data::disk::remove(&sidecar_key).await;
    let _ = file_data::memory::remove(&sidecar_key).await;

    // Broadcast the updated row: querier event handlers evict their cached
    // sidecar bytes + the memoized reader and re-download (api event.rs).
    let mut updated = file.clone();
    updated.meta.index_size = new_index_size;
    updated.meta.bloom_ver = 0;
    updated.deleted = false;
    if let Err(e) = db::file_list::broadcast::send(std::slice::from_ref(&updated)).await {
        log::error!(
            "[COMPACTOR:WORKER:{thread_id}] sidecar heal broadcast for {} failed: {e}",
            file.key,
        );
    }

    log::info!(
        "[COMPACTOR:WORKER:{thread_id}] {org_id}/{stream_type}/{stream_name}: healed {} \
         sidecar-only: data key unchanged, index_size {} -> {new_index_size}, took {} ms",
        file.key,
        file.meta.index_size,
        start.elapsed().as_millis(),
    );
    Ok(true)
}

/// One stored `.vix` object opened by byte ranges through the compactor's
/// cache ladder (`infra::cache::storage::get_range`: memory/disk cache
/// first, then the remote store) — the healing probe's IO. `vortex_index`
/// polls fetch futures on its own single-thread executor (no tokio
/// reactor), so the real IO runs on the captured tokio handle and hands the
/// result back over a oneshot channel; every fetch is bounded by
/// `ZO_VIX_FETCH_TIMEOUT` and ticks the `vix_fetch_*` metrics under the
/// `compact` label.
pub(crate) struct HealProbeRangeSource {
    pub(crate) account: String,
    pub(crate) location: object_store::path::Path,
    pub(crate) size: u64,
    pub(crate) handle: tokio::runtime::Handle,
    pub(crate) cancel: Option<MergeCancellation>,
}

impl vortex_index::VixRangeSource for HealProbeRangeSource {
    fn len(&self) -> u64 {
        self.size
    }

    fn fetch(
        &self,
        range: std::ops::Range<u64>,
    ) -> futures::future::BoxFuture<'static, anyhow::Result<Bytes>> {
        use futures::FutureExt;
        if self
            .cancel
            .as_ref()
            .is_some_and(MergeCancellation::is_cancelled)
        {
            return futures::future::ready(Err(anyhow::anyhow!(
                "vix range fetch of {} cancelled before request",
                self.location,
            )))
            .boxed();
        }
        let account = self.account.clone();
        let location = self.location.clone();
        let cancel = self.cancel.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.handle.spawn(async move {
            let fut = infra::cache::storage::get_range(&account, &location, range);
            let timeout_secs = get_config().limit.vix_fetch_timeout;
            let fetch = async move {
                if timeout_secs > 0 {
                    match tokio::time::timeout(std::time::Duration::from_secs(timeout_secs), fut)
                        .await
                    {
                        Ok(result) => result.map_err(anyhow::Error::from),
                        Err(_) => Err(anyhow::anyhow!(
                            "vix range fetch timed out after {timeout_secs}s (ZO_VIX_FETCH_TIMEOUT)"
                        )),
                    }
                } else {
                    fut.await.map_err(anyhow::Error::from)
                }
            };
            let result = match cancel {
                Some(cancel) => tokio::select! {
                    result = fetch => result,
                    _ = wait_for_merge_cancellation(&cancel) => Err(anyhow::anyhow!(
                        "vix range fetch cancelled during request"
                    )),
                },
                None => fetch.await,
            };
            if let Ok(bytes) = &result {
                metrics::VIX_FETCH_COUNT_TOTAL
                    .with_label_values(&["compact"])
                    .inc();
                metrics::VIX_FETCH_BYTES_TOTAL
                    .with_label_values(&["compact"])
                    .inc_by(bytes.len() as u64);
            }
            // the receiver may be gone (probe abandoned); nothing to do then
            let _ = tx.send(result);
        });
        async move {
            rx.await
                .map_err(|_| anyhow::anyhow!("range fetch task was cancelled"))?
        }
        .boxed()
    }

    fn describe(&self) -> String {
        self.location.to_string()
    }
}

/// Decide whether the single core file of a partition needs the healing
/// rebuild: open it over ranged reads and classify it against the stream's
/// CURRENT schema and settings (`core_writer::classify_core_file` — the
/// same capability checks the merge paths enforce; the same settings
/// resolution `merge_files` uses). `Ok(Some(reason))` enqueues the
/// single-file batch; `Ok(None)` is the no-op path — the file is current,
/// nothing is downloaded beyond container metadata, the job completes with
/// no file_list change.
async fn single_core_file_heal_reason(
    org_id: &str,
    stream_type: StreamType,
    stream_name: &str,
    file: &FileKey,
) -> Result<Option<String>, anyhow::Error> {
    use crate::service::vix::core_writer::{CoreFileStatus, classify_core_file};

    let latest_schema = infra::schema::get(org_id, stream_name, stream_type).await?;
    let stream_settings = infra::schema::unwrap_stream_settings(&latest_schema);
    let bloom_filter_fields = get_stream_setting_bloom_filter_fields(&stream_settings);
    let full_text_search_fields = get_stream_setting_fts_fields(&stream_settings);

    let handle = tokio::runtime::Handle::current();
    let source: Arc<dyn vortex_index::VixRangeSource> = Arc::new(HealProbeRangeSource {
        account: file.account.clone(),
        location: object_store::path::Path::from(file.key.as_str()),
        // a .vix FileMeta's compressed_size is the exact DATA-object size
        size: file.meta.compressed_size as u64,
        handle: handle.clone(),
        cancel: None,
    });
    // v3 split: the term dictionary lives in the `.vxi` sidecar
    // (index_size = its exact size; 0 = no sidecar, classify routes such
    // files to the rebuild)
    let index_source: Option<Arc<dyn vortex_index::VixRangeSource>> =
        config::vix_sidecar_key(&file.key)
            .filter(|_| file.meta.index_size > 0)
            .map(|sidecar_key| {
                Arc::new(HealProbeRangeSource {
                    account: file.account.clone(),
                    location: object_store::path::Path::from(sidecar_key.as_str()),
                    size: file.meta.index_size as u64,
                    handle,
                    cancel: None,
                }) as Arc<dyn vortex_index::VixRangeSource>
            });
    let key = file.key.clone();
    let status = tokio::task::spawn_blocking(move || {
        classify_core_file(
            stream_type,
            &key,
            source,
            index_source,
            &latest_schema,
            &full_text_search_fields,
            &bloom_filter_fields,
        )
    })
    .await??;
    Ok(match status {
        CoreFileStatus::Current => None,
        CoreFileStatus::NeedsRebuild(reason) => Some(reason),
    })
}

/// Build the file_list commit events for one merged batch: adds for the
/// non-empty new files plus deletes for EXACTLY the inputs whose rows made
/// it into the merged output. Batch inputs that were skipped (size-mismatch
/// downloads, size-budget cuts, lone survivors) must not be passed in
/// `merged_files` — they stay live and retry on a later cycle. An empty
/// result means the batch is released with no file_list writes at all.
fn build_commit_events(new_files: Vec<FileKey>, merged_files: &[FileKey]) -> Vec<FileKey> {
    let mut events = Vec::with_capacity(new_files.len() + merged_files.len());
    for new_file in new_files {
        if !new_file.key.is_empty() {
            events.push(new_file);
        }
    }
    for file in merged_files {
        events.push(FileKey {
            deleted: true,
            selection: None,
            row_group_size: None,
            ..file.clone()
        });
    }
    events.sort_by(|a, b| a.key.cmp(&b.key));
    events
}

/// Outcome of one fenced batch commit.
enum FencedCommit {
    /// ownership confirmed, events written
    Committed,
    /// the fence hit zero rows: the job is not (node, Running) anymore —
    /// nothing was written and the caller must discard the merge result
    LeaseLost,
}

/// Commit one merged batch's file_list events ONLY while `node` still owns
/// the RUNNING job row (defense in depth for the heartbeat lease,
/// 2026-07-30 audit): a single conditional UPDATE on file_list_jobs decides
/// it. `Err` means the fence query or the write failed — with the fence
/// query failed nothing was written; with the write failed ownership WAS
/// confirmed and the normal fail-the-job retry path applies.
async fn commit_batch_if_owner(
    job_id: i64,
    node: &str,
    lease_generation: i64,
    org_id: &str,
    stream_type: StreamType,
    events: &[FileKey],
) -> Result<FencedCommit, anyhow::Error> {
    let owned = infra_file_list::touch_job_lease(
        job_id,
        node,
        lease_generation,
        FileListJobStatus::Running,
    )
    .await
    .map_err(|e| {
        anyhow::anyhow!(
            "generation fence for job {job_id} generation {lease_generation} failed (commit discarded): {e}"
        )
    })?;
    if !owned {
        return Ok(FencedCommit::LeaseLost);
    }
    write_file_list(org_id, stream_type, events).await?;
    Ok(FencedCommit::Committed)
}

async fn write_file_list(
    org_id: &str,
    stream_type: StreamType,
    events: &[FileKey],
) -> Result<(), anyhow::Error> {
    if events.is_empty() {
        return Ok(());
    }

    let del_items = events
        .iter()
        .filter(|v| v.deleted)
        .map(|v| FileListDeleted {
            id: 0,
            account: v.account.clone(),
            file: v.key.clone(),
            // vestigial always-false: the deleted-file sweeper derives
            // the .vxi sidecar key from every .vix key unconditionally
            // (compact::deleted), so the flag is never consulted
            index_file: false,
            flattened: v.meta.flattened,
        })
        .collect::<Vec<_>>();

    // Commit to the DB with BOUNDED retries and doubling backoff. A
    // DETERMINISTIC error (SQL syntax error, our own meta validation —
    // `Error::is_deterministic_db_error`) bails immediately: retrying can
    // never succeed, and the old blind 1s loop kept whole worker pools
    // wedged re-running the same failing statement. On exhaustion (or bail)
    // the caller fails the merge job, which returns to pending via the job
    // system instead of spinning the worker.
    const MAX_ATTEMPTS: usize = 5;
    let cfg = get_config();
    let mut success = false;
    let mut mark_deleted_done = false;
    let mut last_error: Option<infra::errors::Error> = None;
    let mut backoff = tokio::time::Duration::from_secs(1);
    let created_at = config::utils::time::now_micros();
    for attempt in 1..=MAX_ATTEMPTS {
        if attempt > 1 {
            tokio::time::sleep(backoff).await;
            backoff *= 2;
        }
        if !mark_deleted_done {
            if let Err(e) = infra::file_list::batch_process(events).await {
                let deterministic = e.is_deterministic_db_error();
                log::error!(
                    "[COMPACTOR] batch_process to db failed (attempt {attempt}/{MAX_ATTEMPTS}{}): {e}",
                    if deterministic {
                        ", deterministic — not retrying"
                    } else {
                        ""
                    },
                );
                last_error = Some(e);
                if deterministic {
                    break;
                }
                continue;
            }
            mark_deleted_done = true;
        }
        if !del_items.is_empty()
            && let Err(e) = infra_file_list::batch_add_deleted(org_id, created_at, &del_items).await
        {
            let deterministic = e.is_deterministic_db_error();
            log::error!(
                "[COMPACTOR] batch_add_deleted to db failed (attempt {attempt}/{MAX_ATTEMPTS}{}): {e}",
                if deterministic {
                    ", deterministic — not retrying"
                } else {
                    ""
                },
            );
            last_error = Some(e);
            if deterministic {
                break;
            }
            continue;
        }
        success = true;
        break;
    }

    // handle dump_stats for file_list type streams
    if success && stream_type == StreamType::Filelist && cfg.compact.file_list_dump_enabled {
        let (deleted_files, new_files): (Vec<_>, Vec<_>) = events.iter().partition(|e| e.deleted);
        super::dump::handle_dump_stats_on_merge(&deleted_files, &new_files).await;
    }

    if success {
        // send broadcast to other nodes
        if cfg.cache_latest_files.enabled {
            // get id for all the new files
            let file_ids = infra_file_list::query_ids_by_files(events).await?;
            let mut events = events.to_vec();
            for event in events.iter_mut() {
                if let Some(id) = file_ids.get(&event.key) {
                    event.id = *id;
                }
            }
            if let Err(e) = db::file_list::broadcast::send(&events).await {
                log::error!("[COMPACTOR] send broadcast for file_list failed: {e}");
            }
        }
    } else {
        return Err(anyhow::anyhow!(
            "file_list batch write to db failed: {}",
            last_error.map_or_else(|| "unknown error".to_string(), |e| e.to_string()),
        ));
    }

    Ok(())
}

/// M19: HEAD every input of a failed merge and remove the file_list row —
/// plus the object pair via [`file_list::delete_parquet_file`] (idempotent,
/// per-key not-found tolerated) — of every input whose DATA object no
/// longer exists in the store (deleted externally, e.g. S3 lifecycle
/// expiry). Returns how many rows were removed. Only typed
/// `object_store::Error::NotFound` HEAD failures count; transient HEAD
/// errors leave the row alone (the job retry re-checks).
async fn reconcile_missing_merge_inputs(files: &[FileKey]) -> usize {
    let mut removed = 0;
    for file in files {
        match storage::head(&file.account, &file.key).await {
            Ok(_) => {}
            Err(object_store::Error::NotFound { .. }) => {
                log::warn!(
                    "[COMPACT] merge input {} no longer exists in object store (deleted externally), deleting entry from file_list",
                    file.key,
                );
                if let Err(e) =
                    file_list::delete_parquet_file(&file.account, &file.key, false).await
                {
                    log::error!("[COMPACT] delete from file_list err: {e}");
                } else {
                    removed += 1;
                }
            }
            Err(e) => {
                log::warn!(
                    "[COMPACT] merge input existence check failed for {} (row kept): {e}",
                    file.key,
                );
            }
        }
    }
    removed
}

async fn cache_remote_files(files: &[FileKey]) -> Result<Vec<String>, anyhow::Error> {
    let cfg = get_config();
    // Indexed VIX merges consume the sidecar almost as eagerly as the data
    // footer/docs object.  Prefetch both through the same bounded cache
    // ladder; previously only data objects were warmed and dictionary merge
    // paid many remote range requests for `.vxi`.
    let scan_size = files
        .iter()
        .map(|f| f.meta.compressed_size.saturating_add(f.meta.index_size))
        .sum::<i64>();
    if is_local_disk_storage()
        || !cfg.disk_cache.enabled
        || scan_size >= cfg.disk_cache.skip_size as i64
    {
        return Ok(Vec::new());
    };

    let mut objects = Vec::with_capacity(files.len().saturating_mul(2));
    for file in files {
        objects.push((
            file.account.clone(),
            file.key.clone(),
            file.meta.compressed_size as usize,
            true,
        ));
        if file.meta.index_size > 0
            && let Some(sidecar) = config::vix_sidecar_key(&file.key)
        {
            objects.push((
                file.account.clone(),
                sidecar,
                file.meta.index_size as usize,
                false,
            ));
        }
    }

    let mut tasks = Vec::with_capacity(objects.len());
    let semaphore = std::sync::Arc::new(Semaphore::new(cfg.limit.cpu_num));
    // H3: the semaphore caps this job's request PARALLELISM; the
    // process-wide byte budget caps in-flight BYTES across all merge jobs
    // (admission by compressed_size; a worker holding nothing always admits
    // one download, so oversize files delay but never starve).
    let download_budget = super::download_budget::global();
    let worker_bytes = std::sync::Arc::new(super::download_budget::WorkerBytes::default());
    for (file_account, file_name, file_size, required_data_object) in objects {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let budget = download_budget.clone();
        let worker = worker_bytes.clone();
        let task: tokio::task::JoinHandle<Option<String>> = tokio::task::spawn(async move {
            let ret = if !file_data::disk::exist(&file_name).await {
                // admit the bytes into the global budget only for a real
                // download; released when the download finishes (drop)
                let _admitted = budget.admit(worker, file_size as u64).await;
                file_data::disk::download(&file_account, &file_name, Some(file_size)).await
            } else {
                Ok(0)
            };
            // A missing DATA object invalidates its file-list row. A sidecar
            // miss is deliberately non-destructive here: the merge/heal path
            // owns the capability fallback and will rebuild it, whereas
            // deleting the live data row would lose records.
            let file_name = match ret {
                Ok(data_len) => {
                    if data_len > 0 && data_len != file_size {
                        log::warn!(
                            "[COMPACT] download file {file_name} found size mismatch, expected: {file_size}, actual: {data_len}, will skip it",
                        );
                        let _ = file_data::disk::remove(&file_name).await;
                        required_data_object.then_some(file_name)
                    } else {
                        None
                    }
                }
                Err(e) => {
                    let detail = e.to_string();
                    if required_data_object
                        && (detail.to_lowercase().contains("not found")
                            || detail.to_lowercase().contains("data size is zero"))
                    {
                        // delete file from file list AND the object pair:
                        // file_list_only=false also issues the storage
                        // deletes — the data object is already gone (that is
                        // the 404), but a `.vix` input's `.vxi` index sidecar
                        // may still exist (a heal rewrites it later than the
                        // data object, resetting its lifecycle age), and the
                        // engine owns the pair (M19). storage::del tolerates
                        // per-key not-found.
                        log::error!("[COMPACT] found invalid file: {file_name}, will delete it");
                        if let Err(e) =
                            file_list::delete_parquet_file(&file_account, &file_name, false).await
                        {
                            log::error!("[COMPACT] delete from file_list err: {e}");
                        }
                        Some(file_name)
                    } else {
                        let object_kind = if required_data_object {
                            "data"
                        } else {
                            "index sidecar"
                        };
                        log::warn!(
                            "[COMPACT] prefetch {object_kind} {file_name} to cache failed; merge range reads will retry/fallback: {detail}"
                        );
                        // remove downloaded file
                        let _ = file_data::disk::remove(&file_name).await;
                        None
                    }
                }
            };
            drop(permit);
            file_name
        });
        tasks.push(task);
    }

    let mut delete_files = Vec::new();
    for task in tasks {
        match task.await {
            Ok(file) => {
                if let Some(file) = file {
                    delete_files.push(file);
                }
            }
            Err(e) => {
                log::error!("[COMPACTOR] load file task err: {e}");
            }
        }
    }

    Ok(delete_files)
}

/// sort by time range without overlapping
fn sort_by_time_range(mut file_list: Vec<FileKey>) -> Vec<FileKey> {
    use std::{cmp::Reverse, collections::BinaryHeap};

    let files_num = file_list.len();
    file_list.sort_by_key(|f| f.meta.min_ts);
    let mut groups: Vec<Vec<FileKey>> = Vec::with_capacity(files_num);
    // The former first-fit scan walked every existing group for every file,
    // which turns a heavily overlapping partition into O(files * groups).
    // A min-heap exposes the group whose tail ends first; if that tail still
    // overlaps the next file, every other group overlaps it as well.  Group
    // ids keep the final flattening deterministic without moving the groups.
    let mut available: BinaryHeap<Reverse<(i64, usize)>> = BinaryHeap::new();
    for file in file_list {
        if available
            .peek()
            .is_some_and(|Reverse((max_ts, _))| file.meta.min_ts >= *max_ts)
        {
            let Reverse((_, group_id)) = available
                .pop()
                .expect("peek proved that a time-range group is available");
            let next_max = file.meta.max_ts;
            groups[group_id].push(file);
            available.push(Reverse((next_max, group_id)));
        } else {
            let next_max = file.meta.max_ts;
            let group_id = groups.len();
            groups.push(vec![file]);
            available.push(Reverse((next_max, group_id)));
        }
    }
    let mut files = Vec::with_capacity(files_num);
    for group in groups {
        files.extend(group);
    }
    files
}

#[cfg(test)]
mod tests {
    use config::meta::stream::{FileKey, FileMeta};

    use super::*;

    // Helper function to create test FileKey
    fn create_file_key(key: &str, min_ts: i64, max_ts: i64, original_size: i64) -> FileKey {
        FileKey {
            id: 0,
            account: "test_account".to_string(),
            key: key.to_string(),
            meta: FileMeta {
                min_ts,
                max_ts,
                records: 100,
                original_size,
                compressed_size: original_size / 2, // assume 50% compression
                index_size: 0,
                flattened: false,
                bloom_ver: 0,
            },
            deleted: false,
            selection: None,
            row_group_size: None,
            selection_exact: false,
        }
    }

    #[tokio::test]
    async fn merge_batch_result_wait_stops_on_cancellation() {
        let (_tx, mut rx) = mpsc::channel::<MergeBatchOutcome>(1);
        let cancel = MergeCancellation::default();
        let trigger = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
            trigger.cancel();
        });

        let outcome = tokio::time::timeout(
            tokio::time::Duration::from_secs(1),
            receive_merge_batch_result(&mut rx, &cancel),
        )
        .await
        .expect("cancelled result wait must not deadlock");
        assert!(matches!(outcome, MergeBatchReceive::Cancelled));
    }

    #[tokio::test]
    async fn merge_batch_submission_stops_on_cancellation() {
        let cancel = MergeCancellation::default();
        let make_batch = |batch_id, cancel: MergeCancellation| MergeBatch {
            batch_id,
            org_id: "org".to_string(),
            stream_type: StreamType::Logs,
            stream_name: "stream".to_string(),
            prefix: "files/org/logs/stream/2026/01/01/00/".to_string(),
            files: Vec::new(),
            cancel,
        };
        let (result_tx, _result_rx) = mpsc::channel::<MergeBatchOutcome>(1);
        let (worker_tx, mut worker_rx) = mpsc::channel::<(MergeSender, MergeBatch)>(1);
        worker_tx
            .send((result_tx.clone(), make_batch(1, cancel.clone())))
            .await
            .expect("fill worker queue");

        let trigger = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
            trigger.cancel();
        });
        let error = tokio::time::timeout(
            tokio::time::Duration::from_secs(1),
            send_merge_batch(
                &worker_tx,
                result_tx,
                make_batch(2, cancel.clone()),
                &cancel,
            ),
        )
        .await
        .expect("cancelled worker send must not deadlock")
        .expect_err("cancelled worker send must fail");

        assert!(error.to_string().contains("cancelled"));
        let (_, queued) = worker_rx.recv().await.expect("original queued batch");
        assert_eq!(queued.batch_id, 1);
        assert!(
            worker_rx.try_recv().is_err(),
            "cancelled batch must not be queued"
        );
    }

    /// M20b pin: the compactor's parquet merge plans single-partition for
    /// METADATA-class streams ONLY (the prod trace_list_index OOM class);
    /// every data stream type keeps the multi-partition plan. The plan shape
    /// that `true` buys (target_partitions=1, one SortExec, zero
    /// RepartitionExec) is pinned in
    /// search::datafusion::merge::m13_single_partition_merge_plan_has_no_repartition.
    #[test]
    fn test_m20b_compactor_single_partition_sort_metadata_only() {
        assert!(compact_single_partition_sort(StreamType::Metadata));
        for st in [
            StreamType::Logs,
            StreamType::Traces,
            StreamType::Metrics,
            StreamType::Index,
            StreamType::Filelist,
            StreamType::EnrichmentTables,
            StreamType::ServiceGraph,
        ] {
            assert!(
                !compact_single_partition_sort(st),
                "{st} must keep the multi-partition merge plan"
            );
        }
    }

    /// M29 debt sweep: closed hours holding >= old_data_min_files small
    /// files get a merge job (oldest first), the old-data dead zone included;
    /// hours below the threshold don't; a DONE hour that still holds debt is
    /// resurrected by the next sweep. Sqlite-backed through the real
    /// file_list + job APIs.
    #[tokio::test]
    async fn test_m29_merge_debt_sweep_enqueues_and_resurrects() {
        use crate::compact::jobs_test_support::{retry_busy, setup};
        let _guard = setup().await;
        let cfg = get_config();
        let min_files = cfg.compact.old_data_min_files.max(1) as usize;

        let run = config::utils::time::now_micros();
        let org = format!("m29dorg{run}");
        let stream = format!("m29dstream{run}");
        let node = config::cluster::LOCAL_NODE.uuid.clone();

        // give the stream a compact offset so the sweep sees data without a
        // registered schema (cache-only write, same as the live generators)
        db::compact::files::set_offset(&org, StreamType::Logs, &stream, run, Some(&node))
            .await
            .expect("set compact offset");

        // four seeded cohorts:
        //  - 26h ago: deep backlog, min_files + 2 files -> MUST be enqueued
        //  - 2h ago: inside the old-data lane's default dead zone (old_data_min_hours) -> the debt
        //    sweep MUST cover it anyway
        //  - 5h ago: min_files - 1 INDEX-LESS .vix files -> M31b/.123: the lone-unindexed clause
        //    MUST enqueue it below the floor (these files need the heal visit; pre-.123 this cohort
        //    proved the floor excluded it — that exclusion WAS the convergence wedge)
        //  - 8h ago: min_files - 1 INDEXED .vix files -> the floor still governs indexed files,
        //    MUST NOT enqueue
        let hour = hour_micros(1);
        let now_hour = run - run % hour;
        let dense_old = now_hour - 26 * hour;
        let dense_hot = now_hour - 2 * hour;
        let sparse = now_hour - 5 * hour;
        let sparse_indexed = now_hour - 8 * hour;
        let mk_files = |hour_start: i64, n: usize, index_size: i64| -> Vec<FileKey> {
            let t = Utc.timestamp_nanos(hour_start * 1000);
            (0..n)
                .map(|i| {
                    FileKey::new(
                        0,
                        String::new(),
                        format!(
                            "files/{org}/logs/{stream}/{}/l0_m29_{hour_start}_{i}.vix",
                            t.format("%Y/%m/%d/%H")
                        ),
                        config::meta::stream::FileMeta {
                            min_ts: hour_start + 1,
                            max_ts: hour_start + 10_000_000,
                            records: 100,
                            original_size: 4096,
                            compressed_size: 1024,
                            index_size,
                            ..Default::default()
                        },
                        false,
                    )
                })
                .collect()
        };
        let mut seeded = mk_files(dense_old, min_files + 2, 0);
        seeded.extend(mk_files(dense_hot, min_files + 2, 0));
        if min_files > 1 {
            seeded.extend(mk_files(sparse, min_files - 1, 0));
            seeded.extend(mk_files(sparse_indexed, min_files - 1, 64));
        }
        retry_busy("batch_process seed", || {
            infra_file_list::batch_process(&seeded)
        })
        .await;

        // first sweep: both dense cohorts enqueue, oldest first
        let enqueued = generate_merge_debt_job_by_stream(&org, StreamType::Logs, &stream)
            .await
            .expect("debt sweep");
        assert_eq!(
            enqueued, 3,
            "the two dense cohorts + the below-floor INDEX-LESS cohort carry debt \
             (the indexed below-floor cohort must not)"
        );

        let stream_key = format!("{org}/logs/{stream}");
        let claimed = retry_busy("claim", || {
            infra_file_list::get_pending_jobs(
                "m29-debt-test",
                10_000,
                FileListJobOrder::EnqueueOldest,
                None,
                None,
            )
        })
        .await;
        let mine: Vec<_> = claimed.iter().filter(|j| j.stream == stream_key).collect();
        let offsets: Vec<i64> = mine.iter().map(|j| j.offsets).collect();
        assert_eq!(
            offsets,
            vec![dense_old, sparse, dense_hot],
            "debt hours enqueued oldest cohort first (id ASC claim order); the \
             indexed sparse cohort is absent"
        );
        // release strangers claimed alongside ours
        for stranger in claimed.iter().filter(|j| j.stream != stream_key) {
            let id = stranger.id;
            let generation = stranger.lease_generation;
            let released = retry_busy("release stranger", || {
                infra_file_list::set_job_pending_owned(id, "m29-debt-test", generation)
            })
            .await;
            assert!(released, "test must still own stranger claim {id}");
        }

        // a finished hour that still holds debt is resurrected next sweep
        for job in &mine {
            let id = job.id;
            let generation = job.lease_generation;
            let done = retry_busy("set_job_done_owned", || {
                infra_file_list::set_job_done_owned(id, "m29-debt-test", generation)
            })
            .await;
            assert!(done, "test must still own debt claim {id}");
        }
        let enqueued = generate_merge_debt_job_by_stream(&org, StreamType::Logs, &stream)
            .await
            .expect("debt sweep resurrect");
        assert_eq!(enqueued, 3, "done hours with standing debt resurrect");
        let claimed = retry_busy("re-claim", || {
            infra_file_list::get_pending_jobs(
                "m29-debt-test",
                10_000,
                FileListJobOrder::EnqueueOldest,
                None,
                None,
            )
        })
        .await;
        let mine2: Vec<i64> = claimed
            .iter()
            .filter(|j| j.stream == stream_key)
            .map(|j| j.offsets)
            .collect();
        assert_eq!(
            mine2,
            vec![dense_old, sparse, dense_hot],
            "resurrected rows are claimable again"
        );

        // cleanup: our jobs done, strangers restored
        for job in &claimed {
            let id = job.id;
            let generation = job.lease_generation;
            if job.stream == stream_key {
                let done = retry_busy("cleanup done", || {
                    infra_file_list::set_job_done_owned(id, "m29-debt-test", generation)
                })
                .await;
                assert!(done, "test must still own cleanup claim {id}");
            } else {
                let released = retry_busy("cleanup release", || {
                    infra_file_list::set_job_pending_owned(id, "m29-debt-test", generation)
                })
                .await;
                assert!(released, "test must still own cleanup stranger {id}");
            }
        }
    }

    /// M29: the group cutter honors `compact.max_file_count` (the #51 merge
    /// WIDTH cap merge_files enforces), so every dispatched batch is fully
    /// consumable in one pass — the old cutter (bytes + max_group_files=10k
    /// only) built 700-file batches of which merge_files took 128 and left
    /// the tail to be re-planned on the NEXT visit of the hour.
    #[test]
    fn test_m29_group_cutter_honors_max_file_count() {
        let cfg = get_config();
        let width = cfg.compact.max_file_count as usize; // default 128
        assert!(width > 0, "default max_file_count must be a real width cap");
        // 2 * width + 44 tiny files: bytes never bind (default 2GB budget)
        let n = 2 * width + 44;
        let files: Vec<FileKey> = (0..n)
            .map(|i| create_file_key(&format!("f{i:04}.vix"), 1000 + i as i64, 2000, 1024))
            .collect();

        // closed hour (non-incremental): everything seals, cut at the width
        let mut batches = Vec::new();
        group_files_into_batches(
            &mut batches,
            &files,
            "org",
            StreamType::Logs,
            "s1",
            "files/org/logs/s1/2026/08/24/00",
            cfg.compact.max_file_size as i64,
            false,
            &MergeStrategy::FileTime,
        );
        assert_eq!(batches.len(), 3, "2 full width batches + sealed remainder");
        assert_eq!(batches[0].files.len(), width);
        assert_eq!(batches[1].files.len(), width);
        assert_eq!(batches[2].files.len(), 44);
        let total: usize = batches.iter().map(|b| b.files.len()).sum();
        assert_eq!(total, n, "a closed hour dispatches every file exactly once");

        // incremental (open hour): full-width groups seal, the below-width
        // trailing remainder carries to the next round
        let mut batches = Vec::new();
        group_files_into_batches(
            &mut batches,
            &files,
            "org",
            StreamType::Logs,
            "s1",
            "files/org/logs/s1/2026/08/24/00",
            cfg.compact.max_file_size as i64,
            true,
            &MergeStrategy::FileTime,
        );
        assert_eq!(batches.len(), 2, "incremental keeps the trailing remainder");
        assert!(batches.iter().all(|b| b.files.len() == width));
    }

    #[test]
    fn indexed_core_group_target_reduces_batches_without_widening_rebuilds() {
        let files: Vec<FileKey> = (0..4)
            .map(|i| create_file_key(&format!("f{i}.vix"), i, i + 1, 400))
            .collect();
        let collect = |max_file_size| {
            let mut batches = Vec::new();
            group_files_into_batches(
                &mut batches,
                &files,
                "org",
                StreamType::Traces,
                "default",
                "files/org/traces/default/2026/09/01/00",
                max_file_size,
                false,
                &MergeStrategy::FileTime,
            );
            batches
        };

        let rebuild_batches = collect(1_000);
        assert_eq!(
            rebuild_batches
                .iter()
                .map(|batch| batch.files.len())
                .collect::<Vec<_>>(),
            vec![2, 2]
        );
        let indexed_batches = collect(4_000);
        assert_eq!(indexed_batches.len(), 1);
        assert_eq!(indexed_batches[0].files.len(), 4);
    }

    #[test]
    fn indexed_trace_strict_path_covers_original_and_compressed_caps() {
        assert!(indexed_trace_group_exceeds_global_rebuild_limit(
            StreamType::Traces,
            true,
            1_200,
            800,
            1_000,
        ));
        assert!(indexed_trace_group_exceeds_global_rebuild_limit(
            StreamType::Traces,
            true,
            800,
            1_200,
            1_000,
        ));
        assert!(!indexed_trace_group_exceeds_global_rebuild_limit(
            StreamType::Logs,
            true,
            1_200,
            1_200,
            1_000,
        ));
        assert!(!indexed_trace_group_exceeds_global_rebuild_limit(
            StreamType::Traces,
            false,
            1_200,
            1_200,
            1_000,
        ));
    }

    #[test]
    fn test_sort_by_time_range_edge_case_adjacent_files() {
        let files = vec![
            create_file_key("file1.parquet", 1000, 2000, 1024),
            create_file_key("file2.parquet", 2000, 3000, 1024), // exactly adjacent
            create_file_key("file3.parquet", 3000, 4000, 1024), // exactly adjacent
        ];
        let result = sort_by_time_range(files);
        assert_eq!(result.len(), 3);

        // Adjacent files should be able to be in the same group
        assert_eq!(result[0].key, "file1.parquet");
        assert_eq!(result[1].key, "file2.parquet");
        assert_eq!(result[2].key, "file3.parquet");
    }

    // Test helper function creation
    #[test]
    fn test_create_file_key_helper() {
        let file_key = create_file_key("test.parquet", 1000, 2000, 1024);
        assert_eq!(file_key.key, "test.parquet");
        assert_eq!(file_key.meta.min_ts, 1000);
        assert_eq!(file_key.meta.max_ts, 2000);
        assert_eq!(file_key.meta.original_size, 1024);
        assert_eq!(file_key.meta.compressed_size, 512); // 50% compression
        assert_eq!(file_key.meta.records, 100);
        assert!(!file_key.meta.flattened);
        assert_eq!(file_key.id, 0);
        assert_eq!(file_key.account, "test_account");
        assert!(!file_key.deleted);
        assert!(file_key.selection.is_none());
    }

    // Boundary tests for sort_by_time_range
    #[test]
    fn test_sort_by_time_range_negative_timestamps() {
        let files = vec![
            create_file_key("file1.parquet", -2000, -1000, 1024),
            create_file_key("file2.parquet", -1000, 0, 1024),
            create_file_key("file3.parquet", 0, 1000, 1024),
        ];
        let result = sort_by_time_range(files);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].key, "file1.parquet");
        assert_eq!(result[1].key, "file2.parquet");
        assert_eq!(result[2].key, "file3.parquet");
    }

    #[test]
    fn test_sort_by_time_range_large_timestamps() {
        let files = vec![
            create_file_key("file1.parquet", i64::MAX - 2000, i64::MAX - 1000, 1024),
            create_file_key("file2.parquet", i64::MAX - 1000, i64::MAX, 1024),
        ];
        let result = sort_by_time_range(files);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].key, "file1.parquet");
        assert_eq!(result[1].key, "file2.parquet");
    }

    // Edge case where min_ts equals max_ts (point in time)
    #[test]
    fn test_sort_by_time_range_point_in_time() {
        let files = vec![
            create_file_key("file1.parquet", 1000, 1000, 1024), // Point in time
            create_file_key("file2.parquet", 1000, 2000, 1024), // Overlaps with file1
            create_file_key("file3.parquet", 2000, 2000, 1024), // Point in time, adjacent
        ];
        let result = sort_by_time_range(files);
        assert_eq!(result.len(), 3);

        // Verify all files are present
        let keys: Vec<&String> = result.iter().map(|f| &f.key).collect();
        assert!(keys.contains(&&"file1.parquet".to_string()));
        assert!(keys.contains(&&"file2.parquet".to_string()));
        assert!(keys.contains(&&"file3.parquet".to_string()));
    }

    #[test]
    fn test_sort_by_time_range_many_files_random_order() {
        let files = vec![
            create_file_key("file_f.parquet", 6000, 7000, 1024),
            create_file_key("file_b.parquet", 2000, 3000, 1024),
            create_file_key("file_d.parquet", 4000, 5000, 1024),
            create_file_key("file_a.parquet", 1000, 2000, 1024),
            create_file_key("file_c.parquet", 3000, 4000, 1024),
            create_file_key("file_e.parquet", 5000, 6000, 1024),
        ];
        let result = sort_by_time_range(files);
        assert_eq!(result.len(), 6);

        // Should be sorted by min_ts (all adjacent files)
        assert_eq!(result[0].key, "file_a.parquet");
        assert_eq!(result[1].key, "file_b.parquet");
        assert_eq!(result[2].key, "file_c.parquet");
        assert_eq!(result[3].key, "file_d.parquet");
        assert_eq!(result[4].key, "file_e.parquet");
        assert_eq!(result[5].key, "file_f.parquet");
    }

    #[test]
    fn test_sort_by_time_range_gaps_between_files() {
        let files = vec![
            create_file_key("file1.parquet", 1000, 2000, 1024),
            create_file_key("file2.parquet", 5000, 6000, 1024), // gap after file1
            create_file_key("file3.parquet", 3000, 4000, 1024), // fits in gap
        ];
        let result = sort_by_time_range(files);
        assert_eq!(result.len(), 3);

        // Should be sorted by min_ts
        assert_eq!(result[0].key, "file1.parquet");
        assert_eq!(result[1].key, "file3.parquet");
        assert_eq!(result[2].key, "file2.parquet");
    }

    #[test]
    fn test_sort_by_time_range_empty_list() {
        let files = vec![];
        let result = sort_by_time_range(files);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_sort_by_time_range_single_file() {
        let files = vec![create_file_key("file1.parquet", 1000, 2000, 1024)];
        let result = sort_by_time_range(files);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].key, "file1.parquet");
        assert_eq!(result[0].meta.min_ts, 1000);
        assert_eq!(result[0].meta.max_ts, 2000);
    }

    #[test]
    fn test_sort_by_time_range_already_sorted_non_overlapping() {
        let files = vec![
            create_file_key("file1.parquet", 1000, 2000, 1024),
            create_file_key("file2.parquet", 2000, 3000, 1024),
            create_file_key("file3.parquet", 3000, 4000, 1024),
        ];
        let result = sort_by_time_range(files);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].key, "file1.parquet");
        assert_eq!(result[1].key, "file2.parquet");
        assert_eq!(result[2].key, "file3.parquet");
    }

    #[test]
    fn test_sort_by_time_range_unsorted_non_overlapping() {
        let files = vec![
            create_file_key("file3.parquet", 3000, 4000, 1024),
            create_file_key("file1.parquet", 1000, 2000, 1024),
            create_file_key("file2.parquet", 2000, 3000, 1024),
        ];
        let result = sort_by_time_range(files);
        assert_eq!(result.len(), 3);
        // Should be sorted by min_ts
        assert_eq!(result[0].key, "file1.parquet");
        assert_eq!(result[1].key, "file2.parquet");
        assert_eq!(result[2].key, "file3.parquet");
    }

    #[test]
    fn test_sort_by_time_range_overlapping_files() {
        let files = vec![
            create_file_key("file1.parquet", 1000, 2500, 1024), // overlaps with file2
            create_file_key("file2.parquet", 2000, 3000, 1024), // overlaps with file1
            create_file_key("file3.parquet", 3500, 4000, 1024), // non-overlapping
        ];
        let result = sort_by_time_range(files);
        assert_eq!(result.len(), 3);

        // First file should be file1 (min_ts = 1000)
        assert_eq!(result[0].key, "file1.parquet");

        // Due to overlapping, file3 should come next (can fit in same group as file1)
        // file2 would be in a separate group since it overlaps with file1
        let mut found_file2 = false;
        let mut found_file3 = false;
        for file in &result {
            if file.key == "file2.parquet" {
                found_file2 = true;
            }
            if file.key == "file3.parquet" {
                found_file3 = true;
            }
        }
        assert!(found_file2);
        assert!(found_file3);
    }

    #[test]
    fn test_sort_by_time_range_complex_overlapping() {
        let files = vec![
            create_file_key("file1.parquet", 1000, 1500, 1024),
            create_file_key("file2.parquet", 1200, 1800, 1024), // overlaps with file1
            create_file_key("file3.parquet", 1600, 2000, 1024), // overlaps with file2
            create_file_key("file4.parquet", 2000, 2500, 1024), // adjacent to file3
            create_file_key("file5.parquet", 3000, 3500, 1024), // separate group
        ];
        let result = sort_by_time_range(files);
        assert_eq!(result.len(), 5);

        // Verify all files are present
        let keys: Vec<&String> = result.iter().map(|f| &f.key).collect();
        assert!(keys.contains(&&"file1.parquet".to_string()));
        assert!(keys.contains(&&"file2.parquet".to_string()));
        assert!(keys.contains(&&"file3.parquet".to_string()));
        assert!(keys.contains(&&"file4.parquet".to_string()));
        assert!(keys.contains(&&"file5.parquet".to_string()));
    }

    #[test]
    fn test_sort_by_time_range_identical_timestamps() {
        let files = vec![
            create_file_key("file1.parquet", 1000, 2000, 1024),
            create_file_key("file2.parquet", 1000, 2000, 512),
            create_file_key("file3.parquet", 1000, 2000, 2048),
        ];
        let result = sort_by_time_range(files);
        assert_eq!(result.len(), 3);

        // All files have same timestamp, so they should all be in separate groups
        // due to overlap, but ordering should be maintained based on original order after sorting
        let keys: Vec<&String> = result.iter().map(|f| &f.key).collect();
        assert_eq!(keys.len(), 3);
        assert!(keys.contains(&&"file1.parquet".to_string()));
        assert!(keys.contains(&&"file2.parquet".to_string()));
        assert!(keys.contains(&&"file3.parquet".to_string()));
    }

    // ── Additional sort_by_time_range edge-case tests ────────────────────────

    /// Two files share the same min_ts but have different max_ts values.
    /// The file with the smaller max_ts ends first; the second file starts at
    /// the same time and thus overlaps → they must land in different groups.
    #[test]
    fn test_sort_by_time_range_same_min_ts_different_max_ts() {
        let files = vec![
            create_file_key("file_a.parquet", 1000, 3000, 1024),
            create_file_key("file_b.parquet", 1000, 2000, 1024),
        ];
        let result = sort_by_time_range(files);
        assert_eq!(result.len(), 2);
        let keys: Vec<&str> = result.iter().map(|f| f.key.as_str()).collect();
        assert!(keys.contains(&"file_a.parquet"));
        assert!(keys.contains(&"file_b.parquet"));
        // Both share min_ts=1000, so they overlap: each ends up in its own group.
        // Consecutive elements in the output must not have a later file whose
        // min_ts < the predecessor's max_ts within the same group-chain.
        // We just verify the invariant: result preserves all files.
        assert_eq!(result.len(), 2);
    }

    /// All files are completely overlapping (same time range).
    /// Every file goes into a separate group; total count must be preserved.
    #[test]
    fn test_sort_by_time_range_all_identical_ranges() {
        let files = vec![
            create_file_key("file1.parquet", 500, 1500, 1024),
            create_file_key("file2.parquet", 500, 1500, 512),
            create_file_key("file3.parquet", 500, 1500, 2048),
            create_file_key("file4.parquet", 500, 1500, 768),
        ];
        let result = sort_by_time_range(files);
        assert_eq!(result.len(), 4);
        let keys: Vec<&str> = result.iter().map(|f| f.key.as_str()).collect();
        assert!(keys.contains(&"file1.parquet"));
        assert!(keys.contains(&"file2.parquet"));
        assert!(keys.contains(&"file3.parquet"));
        assert!(keys.contains(&"file4.parquet"));
    }

    /// Files whose min_ts == max_ts == 0 (zero timestamps).
    #[test]
    fn test_sort_by_time_range_zero_timestamps() {
        let files = vec![
            create_file_key("file1.parquet", 0, 0, 1024),
            create_file_key("file2.parquet", 0, 1000, 1024),
        ];
        let result = sort_by_time_range(files);
        assert_eq!(result.len(), 2);
        // First file is whichever has min_ts=0; both should survive.
        let keys: Vec<&str> = result.iter().map(|f| f.key.as_str()).collect();
        assert!(keys.contains(&"file1.parquet"));
        assert!(keys.contains(&"file2.parquet"));
    }

    /// Two non-overlapping groups: group A has files [1000,2000] and [2000,3000];
    /// group B has files [1500,2500] and [2500,3500].  The algorithm must
    /// produce exactly 4 files in the output.
    #[test]
    fn test_sort_by_time_range_two_interleaved_groups() {
        // After sort by min_ts:
        //   A1: [1000,2000], B1: [1500,2500], A2: [2000,3000], B2: [2500,3500]
        // A1 opens group-0.  B1 overlaps A1 (1500 < 2000) → opens group-1.
        // A2: min_ts=2000 >= group-0's last max=2000 → fits group-0.
        // B2: min_ts=2500 >= group-1's last max=2500 → fits group-1.
        let files = vec![
            create_file_key("A1.parquet", 1000, 2000, 1024),
            create_file_key("B1.parquet", 1500, 2500, 1024),
            create_file_key("A2.parquet", 2000, 3000, 1024),
            create_file_key("B2.parquet", 2500, 3500, 1024),
        ];
        let result = sort_by_time_range(files);
        assert_eq!(result.len(), 4);
        let keys: Vec<&str> = result.iter().map(|f| f.key.as_str()).collect();
        assert!(keys.contains(&"A1.parquet"));
        assert!(keys.contains(&"A2.parquet"));
        assert!(keys.contains(&"B1.parquet"));
        assert!(keys.contains(&"B2.parquet"));
        // Group-0 chain: A1 then A2 (adjacent).  Verify they appear consecutively.
        let a1_pos = result.iter().position(|f| f.key == "A1.parquet").unwrap();
        let a2_pos = result.iter().position(|f| f.key == "A2.parquet").unwrap();
        assert_eq!(
            a2_pos,
            a1_pos + 1,
            "A1 and A2 should be in the same group (consecutive)"
        );
    }

    /// A chain of files where each one's min_ts equals the previous file's max_ts
    /// (strictly adjacent, no gap).  All should end up in one group.
    #[test]
    fn test_sort_by_time_range_strictly_adjacent_chain() {
        let files = vec![
            create_file_key("f1.parquet", 100, 200, 512),
            create_file_key("f3.parquet", 300, 400, 512),
            create_file_key("f2.parquet", 200, 300, 512),
            create_file_key("f4.parquet", 400, 500, 512),
        ];
        let result = sort_by_time_range(files);
        assert_eq!(result.len(), 4);
        // All are adjacent (min_ts == prev max_ts) so they chain into one group.
        assert_eq!(result[0].key, "f1.parquet");
        assert_eq!(result[1].key, "f2.parquet");
        assert_eq!(result[2].key, "f3.parquet");
        assert_eq!(result[3].key, "f4.parquet");
    }

    /// A single file where min_ts == max_ts (point-in-time, zero-width range).
    #[test]
    fn test_sort_by_time_range_single_point_file() {
        let files = vec![create_file_key("point.parquet", 42, 42, 256)];
        let result = sort_by_time_range(files);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].key, "point.parquet");
        assert_eq!(result[0].meta.min_ts, 42);
        assert_eq!(result[0].meta.max_ts, 42);
    }

    // ── sort_by_time_range: additional branch-coverage tests ─────────────────

    /// Two files that don't overlap and have a gap between them.  The second
    /// file's min_ts is strictly greater than the first file's max_ts, so both
    /// land in the same chain (group-0) and appear in order.
    #[test]
    fn test_sort_by_time_range_two_non_overlapping_with_gap() {
        let files = vec![
            create_file_key("early.parquet", 1000, 2000, 512),
            create_file_key("late.parquet", 5000, 6000, 512),
        ];
        let result = sort_by_time_range(files);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].key, "early.parquet");
        assert_eq!(result[1].key, "late.parquet");
    }

    /// Files presented in strictly descending min_ts order.  After the internal
    /// sort by min_ts the algorithm must produce the same output as if they had
    /// been given in ascending order.
    #[test]
    fn test_sort_by_time_range_descending_input_order() {
        let files = vec![
            create_file_key("f4.parquet", 4000, 5000, 512),
            create_file_key("f3.parquet", 3000, 4000, 512),
            create_file_key("f2.parquet", 2000, 3000, 512),
            create_file_key("f1.parquet", 1000, 2000, 512),
        ];
        let result = sort_by_time_range(files);
        assert_eq!(result.len(), 4);
        // Adjacent chain: all files should appear sorted by min_ts.
        assert_eq!(result[0].key, "f1.parquet");
        assert_eq!(result[1].key, "f2.parquet");
        assert_eq!(result[2].key, "f3.parquet");
        assert_eq!(result[3].key, "f4.parquet");
    }

    /// File where min_ts == max_ts == i64::MIN (boundary value).
    #[test]
    fn test_sort_by_time_range_min_i64_timestamps() {
        let files = vec![
            create_file_key("a.parquet", i64::MIN, i64::MIN, 256),
            create_file_key("b.parquet", i64::MIN, 0, 256),
        ];
        let result = sort_by_time_range(files);
        assert_eq!(result.len(), 2);
        let keys: Vec<&str> = result.iter().map(|f| f.key.as_str()).collect();
        assert!(keys.contains(&"a.parquet"));
        assert!(keys.contains(&"b.parquet"));
    }

    /// A file whose min_ts is greater than its max_ts (malformed / inverted range).
    /// The algorithm must still return all files without panicking.
    #[test]
    fn test_sort_by_time_range_inverted_range_no_panic() {
        let files = vec![
            create_file_key("good.parquet", 1000, 3000, 512),
            create_file_key("bad.parquet", 5000, 2000, 512), // inverted: min > max
        ];
        let result = sort_by_time_range(files);
        assert_eq!(result.len(), 2);
        let keys: Vec<&str> = result.iter().map(|f| f.key.as_str()).collect();
        assert!(keys.contains(&"good.parquet"));
        assert!(keys.contains(&"bad.parquet"));
    }

    /// Many fully-overlapping files create as many groups as there are files.
    /// Verify that the total count is always preserved regardless of input size.
    #[test]
    fn test_sort_by_time_range_ten_fully_overlapping_files() {
        let files: Vec<FileKey> = (0..10)
            .map(|i| create_file_key(&format!("f{i}.parquet"), 0, 1000, 256))
            .collect();
        let result = sort_by_time_range(files);
        assert_eq!(result.len(), 10, "All files must be preserved");
    }

    /// Non-overlapping files interleaved with overlapping ones.
    /// Ensures multiple groups can be built simultaneously and
    /// files correctly land in the first group that can accept them.
    #[test]
    fn test_sort_by_time_range_mixed_overlap_and_gaps() {
        // After sort by min_ts:
        //  A: [100, 200], B: [150, 300], C: [200, 400], D: [300, 500], E: [600, 700]
        // Group-0 starts with A.
        // B (min=150 < A.max=200) → new group-1.
        // C (min=200 >= A.max=200) → fits group-0 (A chain extends to max=400).
        // D (min=300 < C.max=400) in group-0 fails; min=300 >= B.max=300 → fits group-1.
        // E (min=600 >= C.max=400) → fits group-0.
        let files = vec![
            create_file_key("A.parquet", 100, 200, 512),
            create_file_key("B.parquet", 150, 300, 512),
            create_file_key("C.parquet", 200, 400, 512),
            create_file_key("D.parquet", 300, 500, 512),
            create_file_key("E.parquet", 600, 700, 512),
        ];
        let result = sort_by_time_range(files);
        assert_eq!(result.len(), 5);
        let keys: Vec<&str> = result.iter().map(|f| f.key.as_str()).collect();
        assert!(keys.contains(&"A.parquet"));
        assert!(keys.contains(&"B.parquet"));
        assert!(keys.contains(&"C.parquet"));
        assert!(keys.contains(&"D.parquet"));
        assert!(keys.contains(&"E.parquet"));
    }

    /// Verify `sort_by_time_range` with exactly two files where the second
    /// file's min_ts equals the first file's max_ts — the boundary condition
    /// `min_ts >= max_ts` in the group-fit predicate.
    #[test]
    fn test_sort_by_time_range_exact_boundary_two_files() {
        let files = vec![
            create_file_key("first.parquet", 1000, 2000, 512),
            create_file_key("second.parquet", 2000, 3000, 512), // min == prev max
        ];
        let result = sort_by_time_range(files);
        assert_eq!(result.len(), 2);
        // min_ts(second) == max_ts(first), so `file.meta.min_ts >= f.meta.max_ts`
        // evaluates to true → second file joins first file's group.
        assert_eq!(result[0].key, "first.parquet");
        assert_eq!(result[1].key, "second.parquet");
    }

    /// A file whose min_ts is one less than the previous group's max_ts must
    /// NOT join that group (it overlaps by 1 microsecond).
    #[test]
    fn test_sort_by_time_range_one_unit_overlap() {
        let files = vec![
            create_file_key("first.parquet", 1000, 2000, 512),
            create_file_key("overlap.parquet", 1999, 3000, 512), // 1999 < 2000
        ];
        let result = sort_by_time_range(files);
        assert_eq!(result.len(), 2);
        // overlap.parquet must be in a different group from first.parquet.
        // The first element should be first.parquet (lower min_ts).
        assert_eq!(result[0].key, "first.parquet");
        // Both files must appear; overlap.parquet goes to a new group.
        let keys: Vec<&str> = result.iter().map(|f| f.key.as_str()).collect();
        assert!(keys.contains(&"overlap.parquet"));
    }

    // ── FIX-B (2026-07-30 audit): deletion-set exactness ─────────────────────

    /// A batch where one file was skipped (size-mismatch download, size
    /// budget cut, ...) must delete ONLY the files that made it into the
    /// merged output; the skipped file gets no delete event.
    #[test]
    fn test_build_commit_events_deletes_only_merged_files() {
        let batch = [
            create_file_key("files/o/logs/s/2026/01/01/00/a.parquet", 1000, 2000, 1024),
            create_file_key("files/o/logs/s/2026/01/01/00/b.parquet", 2000, 3000, 1024),
            create_file_key(
                "files/o/logs/s/2026/01/01/00/skip.parquet",
                3000,
                4000,
                1024,
            ),
        ];
        // merge consumed a + b, skipped skip.parquet
        let merged = &batch[..2];
        let new_file = create_file_key(
            "files/o/logs/s/2026/01/01/00/merged.parquet",
            1000,
            3000,
            2048,
        );
        let events = build_commit_events(vec![new_file.clone()], merged);

        assert_eq!(events.len(), 3, "one add + exactly two deletes");
        let adds = events
            .iter()
            .filter(|e| !e.deleted)
            .map(|e| e.key.as_str())
            .collect::<Vec<_>>();
        assert_eq!(adds, vec![new_file.key.as_str()]);
        let deletes = events
            .iter()
            .filter(|e| e.deleted)
            .map(|e| e.key.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            deletes,
            vec![
                "files/o/logs/s/2026/01/01/00/a.parquet",
                "files/o/logs/s/2026/01/01/00/b.parquet",
            ]
        );
        assert!(
            !events
                .iter()
                .any(|e| e.key == "files/o/logs/s/2026/01/01/00/skip.parquet"),
            "the skipped file must stay live — no event at all"
        );
    }

    /// The <=1-survivor case: `merge_files` returns empty new_files AND an
    /// empty merged set, so the batch releases with NO file_list writes —
    /// no events means `write_file_list` is never reached.
    #[test]
    fn test_build_commit_events_survivor_release_writes_nothing() {
        let events = build_commit_events(Vec::new(), &[]);
        assert!(
            events.is_empty(),
            "a released batch must produce zero file_list events"
        );
    }

    /// Empty-keyed new files are filtered (the all-poison cleanse path
    /// commits inputs-deleted with no output); the deletes must survive.
    #[test]
    fn test_build_commit_events_pure_deletion_keeps_deletes_only() {
        let merged = vec![create_file_key(
            "files/o/logs/s/2026/01/01/00/poison.parquet",
            1000,
            2000,
            1024,
        )];
        let events = build_commit_events(Vec::new(), &merged);
        assert_eq!(events.len(), 1);
        assert!(events[0].deleted);
        assert_eq!(events[0].key, "files/o/logs/s/2026/01/01/00/poison.parquet");
    }

    // ── M19: merge-input reconciliation on externally deleted objects ───────

    /// A failed merge whose inputs include an externally deleted object
    /// (S3 lifecycle expiry) reconciles by HEAD: rows of VANISHED inputs are
    /// removed (pair-delete, idempotent), rows of present inputs stay — so
    /// the job's retry claims only surviving files instead of failing
    /// forever. Sqlite file_list + local-disk object store.
    #[tokio::test]
    async fn m19_reconcile_missing_merge_inputs_removes_only_gone_rows() {
        use crate::compact::jobs_test_support::retry_busy;
        let _guard = crate::compact::jobs_test_support::setup().await;
        std::fs::create_dir_all(&get_config().common.data_stream_dir)
            .expect("create data_stream_dir for tests");

        let run = config::utils::time::now_micros();
        let org = format!("m19recorg{run}");
        let present_key = format!("files/{org}/logs/s1/2021/01/02/00/present.vix");
        let gone_key = format!("files/{org}/logs/s1/2021/01/02/00/gone.vix");
        infra::storage::put("", &present_key, Bytes::from_static(b"still-here"))
            .await
            .expect("put present object");
        // gone_key: row only, no object — the lifecycle-expired shape

        let mk = |key: &str| {
            FileKey::new(
                0,
                String::new(),
                key.to_string(),
                FileMeta {
                    min_ts: 1,
                    max_ts: 2,
                    records: 10,
                    original_size: 100,
                    compressed_size: 10,
                    ..Default::default()
                },
                false,
            )
        };
        let files = vec![mk(&present_key), mk(&gone_key)];
        retry_busy("seed file_list rows", || infra_file_list::batch_add(&files)).await;

        let removed = reconcile_missing_merge_inputs(&files).await;
        assert_eq!(removed, 1, "exactly the vanished input reconciles");
        assert!(
            infra_file_list::contains(&present_key).await.unwrap(),
            "an input whose object exists must keep its row"
        );
        assert!(
            !infra_file_list::contains(&gone_key).await.unwrap(),
            "an input whose object is gone must lose its row"
        );

        // cleanup the surviving row + object
        retry_busy("cleanup surviving row", || {
            file_list::delete_parquet_file("", &present_key, false)
        })
        .await;
    }

    // ── FIX-A (2026-07-30 audit): commit fencing ─────────────────────────────

    fn commit_test_file(org: &str, stream: &str, name: &str) -> FileKey {
        create_file_key(
            &format!("files/{org}/logs/{stream}/2026/01/01/00/{name}.parquet"),
            1_700_000_000_000_000,
            1_700_000_001_000_000,
            1024,
        )
    }

    /// Steal the lease mid-merge => ZERO file_list writes from the loser.
    /// Claim as node-a, commit one fenced batch (lands), let the lease time
    /// out and node-b re-claim, then node-a's late commit must be fully
    /// discarded: no add row, no file_list_deleted row. Sqlite-backed
    /// through the real job API.
    #[tokio::test]
    async fn test_commit_fencing_loser_writes_nothing() {
        use crate::compact::jobs_test_support::retry_busy;
        let _guard = crate::compact::jobs_test_support::setup().await;
        let run = config::utils::time::now_micros();
        let org = format!("fenceorg{run}");
        let stream = format!("fencestream{run}");
        let job_id = retry_busy("add_job", || {
            infra_file_list::add_job(&org, StreamType::Logs, &stream, run)
        })
        .await;

        // claim for node-a (table-wide claim: keep ours, restore strangers)
        let claimed = retry_busy("claim", || {
            infra_file_list::get_pending_jobs(
                "fence-node-a",
                10_000,
                FileListJobOrder::EnqueueOldest,
                None,
                None,
            )
        })
        .await;
        let generation_a = claimed
            .iter()
            .find(|j| j.id == job_id)
            .expect("our job must be claimed")
            .lease_generation;
        for stranger in claimed.iter().filter(|j| j.id != job_id) {
            let id = stranger.id;
            let generation = stranger.lease_generation;
            let released = retry_busy("restore stranger job", || {
                infra_file_list::set_job_pending_owned(id, "fence-node-a", generation)
            })
            .await;
            assert!(released, "node-a must still own stranger claim {id}");
        }

        // the running owner's fenced commit lands: one add + one delete
        let old_a = commit_test_file(&org, &stream, "old_a");
        let new_a = commit_test_file(&org, &stream, "new_a");
        let events_a = build_commit_events(vec![new_a.clone()], std::slice::from_ref(&old_a));
        let outcome = retry_busy("owner commit", || {
            commit_batch_if_owner(
                job_id,
                "fence-node-a",
                generation_a,
                &org,
                StreamType::Logs,
                &events_a,
            )
        })
        .await;
        match outcome {
            FencedCommit::Committed => {}
            FencedCommit::LeaseLost => panic!("the running owner must pass the fence"),
        }
        assert!(
            infra::file_list::contains(&new_a.key)
                .await
                .expect("contains"),
            "the owner's add must land"
        );

        // the lease times out; node-b re-claims the re-pended job
        let past_everything = config::utils::time::now_micros() + 1;
        retry_busy("time the lease out", || {
            infra::file_list::check_running_jobs(past_everything)
        })
        .await;
        let reclaimed = retry_busy("re-claim", || {
            infra_file_list::get_pending_jobs(
                "fence-node-b",
                10_000,
                FileListJobOrder::EnqueueOldest,
                None,
                None,
            )
        })
        .await;
        let generation_b = reclaimed
            .iter()
            .find(|j| j.id == job_id)
            .expect("node-b must have re-claimed the job")
            .lease_generation;
        assert_ne!(
            generation_a, generation_b,
            "a stale reset and re-claim must advance the generation"
        );
        for stranger in reclaimed.iter().filter(|j| j.id != job_id) {
            let id = stranger.id;
            let generation = stranger.lease_generation;
            let released = retry_busy("restore stranger job", || {
                infra_file_list::set_job_pending_owned(id, "fence-node-b", generation)
            })
            .await;
            assert!(released, "node-b must still own stranger claim {id}");
        }

        // node-a finishes its merge late: the fenced commit must DISCARD —
        // zero file_list writes from the loser
        let old_b = commit_test_file(&org, &stream, "old_b");
        let new_b = commit_test_file(&org, &stream, "new_b");
        let events_b = build_commit_events(vec![new_b.clone()], std::slice::from_ref(&old_b));
        let outcome = retry_busy("loser commit attempt", || {
            commit_batch_if_owner(
                job_id,
                "fence-node-a",
                generation_a,
                &org,
                StreamType::Logs,
                &events_b,
            )
        })
        .await;
        match outcome {
            FencedCommit::LeaseLost => {}
            FencedCommit::Committed => panic!("a stolen lease must not commit"),
        }
        assert!(
            !infra::file_list::contains(&new_b.key)
                .await
                .expect("contains"),
            "the loser's add must NOT land"
        );
        let deleted_rows = infra::file_list::list_deleted()
            .await
            .expect("list_deleted");
        assert!(
            !deleted_rows.iter().any(|d| d.file == old_b.key),
            "the loser's delete must NOT land"
        );

        // the new owner still commits fine
        let outcome = retry_busy("winner commit", || {
            commit_batch_if_owner(
                job_id,
                "fence-node-b",
                generation_b,
                &org,
                StreamType::Logs,
                &events_b,
            )
        })
        .await;
        match outcome {
            FencedCommit::Committed => {}
            FencedCommit::LeaseLost => panic!("the current owner must pass the fence"),
        }
        assert!(
            infra::file_list::contains(&new_b.key)
                .await
                .expect("contains"),
            "the winner's add must land"
        );

        // cleanup
        let done = retry_busy("cleanup set_job_done_owned", || {
            infra_file_list::set_job_done_owned(job_id, "fence-node-b", generation_b)
        })
        .await;
        assert!(done, "node-b must still own the fenced job at cleanup");
    }
}
