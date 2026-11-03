// Copyright 2026 OpenObserve Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::collections::HashSet;

use config::{get_config, is_local_disk_storage, meta::search::ScanStats};
use infra::cache::{file_data, file_downloader};

/// Whether cache misses should try bounded, independently owned cache fills.
///
/// Exact equality histograms can answer remote-cold files from narrow native
/// docs columns, but still need the cache probe to prefer sidecars that are
/// already local. `CheckOnly` preserves that probe without warming sidecars
/// the query will not read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CacheMissPolicy {
    Enqueue,
    CheckOnly,
}

impl CacheMissPolicy {
    #[inline]
    fn enqueue_misses(self) -> bool {
        matches!(self, Self::Enqueue)
    }
}

/// Linear interpolation: cached_ratio=0 -> query_thread_num, cached_ratio=1 -> cpu_num.
pub fn calc_target_partitions(cpu_num: usize, query_thread_num: usize, cached_ratio: f64) -> usize {
    (cpu_num as i64
        + ((query_thread_num as i64 - cpu_num as i64) as f64 * (1.0 - cached_ratio)) as i64)
        as usize
}

/// Select admission from the original cohort size, not each surviving file's
/// size. Late sidecar warming must not evade the existing query-size gates.
pub(crate) fn warm_cache_type(compressed_size: i64) -> file_data::CacheType {
    let cfg = get_config();
    if cfg.memory_cache.enabled && compressed_size < cfg.memory_cache.skip_size as i64 {
        file_data::CacheType::Memory
    } else if !is_local_disk_storage()
        && cfg.disk_cache.enabled
        && compressed_size < cfg.disk_cache.skip_size as i64
    {
        file_data::CacheType::Disk
    } else {
        file_data::CacheType::None
    }
}

/// Queries read `files/` objects by range, and the block tier caches exactly
/// those ranges. A query-triggered whole-object download of a large object into
/// the disk tier therefore only re-fetches bytes the block tier already serves.
/// Small objects keep the whole-object download: one GET is cheaper than
/// several ranged reads. "Small" is the same definition as the `DemandSmall`
/// download class. The memory tier and disk-only setups without a block tier
/// keep the whole-object download, because nothing else would cache the object.
pub(crate) fn leaves_to_block_tier(
    cache_type: file_data::CacheType,
    key: &str,
    size: i64,
    block_tier_active: bool,
    small_object_bytes: usize,
) -> bool {
    let small = size > 0 && (size as u64) <= small_object_bytes as u64;
    cache_type == file_data::CacheType::Disk
        && block_tier_active
        && key.starts_with("files/")
        && !small
}

fn leave_to_block_tier(cache_type: file_data::CacheType, key: &str, size: i64) -> bool {
    leaves_to_block_tier(
        cache_type,
        key,
        size,
        file_data::block::global().is_some(),
        get_config().limit.file_download_small_object_bytes,
    )
}

fn record_block_tier_skip(class: &str, size: i64) {
    if get_config().limit.file_download_ledger_enabled {
        config::metrics::FILE_DOWNLOAD_LEDGER_EVENTS
            .with_label_values(&[class, "block_tier"])
            .inc();
        config::metrics::FILE_DOWNLOAD_LEDGER_BYTES
            .with_label_values(&[class, "block_tier"])
            .inc_by(size.max(0) as u64);
    }
}

/// Admit only a sidecar the chosen foreground path actually used. The downloader
/// retains its count/byte/dedup/age/size gates and independent admitted ownership.
pub(crate) async fn warm_used_sidecar(
    trace_id: &str,
    file: &config::meta::stream::FileKey,
    cache_type: file_data::CacheType,
) {
    if cache_type == file_data::CacheType::None
        || file.meta.index_size <= 0
        || !file_downloader::should_download(file.meta.records)
    {
        return;
    }
    let Some(key) = config::vix_sidecar_key(&file.key, file.meta.index_generation) else {
        return;
    };
    file_downloader::note_query_activity();
    if leave_to_block_tier(cache_type, &key, file.meta.index_size) {
        record_block_tier_skip("used_sidecar", file.meta.index_size);
        log::debug!(
            "[trace_id {trace_id}] search->vix: used sidecar left to block tier file={key} size={}",
            file.meta.index_size
        );
        return;
    }
    let outcome = file_downloader::queue_download_class(
        trace_id,
        file.id,
        &file.account,
        &key,
        file.meta.index_size,
        file.meta.max_ts,
        cache_type,
        file_downloader::DownloadClass::UsedSidecar,
    )
    .await;
    log::debug!(
        "[trace_id {trace_id}] search->vix: used sidecar warm admission file={key} outcome={outcome:?}"
    );
}

#[tracing::instrument(name = "service:search:grpc:storage:cache_files", skip_all)]
pub async fn cache_files(
    trace_id: &str,
    files: &[(i64, &String, &String, i64, i64, i64)],
    scan_stats: &mut ScanStats,
    file_type: &str,
) -> (file_data::CacheType, u64, u64) {
    cache_files_with_policy(
        trace_id,
        files,
        scan_stats,
        file_type,
        CacheMissPolicy::Enqueue,
    )
    .await
}

pub(crate) async fn cache_files_with_policy(
    trace_id: &str,
    files: &[(i64, &String, &String, i64, i64, i64)],
    scan_stats: &mut ScanStats,
    file_type: &str,
    miss_policy: CacheMissPolicy,
) -> (file_data::CacheType, u64, u64) {
    file_downloader::note_query_activity();
    let mut cached_files = HashSet::with_capacity(files.len());
    let (mut cache_hits, mut cache_misses) = (0, 0);

    let start = std::time::Instant::now();
    for (_id, _account, file, _size, max_ts, _records) in files.iter() {
        if file_data::memory::exist(file).await {
            scan_stats.querier_memory_cached_files += 1;
            cached_files.insert(file);
            cache_hits += 1;
        } else if file_data::disk::exist(file).await {
            scan_stats.querier_disk_cached_files += 1;
            cached_files.insert(file);
            cache_hits += 1;
        } else {
            cache_misses += 1;
        }

        let stream_type = if file_type == "index" {
            config::meta::stream::StreamType::Index
        } else if file.contains("/logs/") {
            config::meta::stream::StreamType::Logs
        } else if file.contains("/metrics/") {
            config::meta::stream::StreamType::Metrics
        } else if file.contains("/traces/") {
            config::meta::stream::StreamType::Traces
        } else {
            config::meta::stream::StreamType::Logs
        };

        let current_time = chrono::Utc::now().timestamp_micros();
        let file_age_seconds = (current_time - max_ts) / 1_000_000;
        let file_age_hours = file_age_seconds as f64 / 3600.0;

        if file_age_hours > 0.0 {
            config::metrics::FILE_ACCESS_TIME
                .with_label_values(&[&stream_type.to_string()])
                .observe(file_age_hours);
        }
    }

    let check_cache_took = start.elapsed().as_millis() as usize;
    if check_cache_took > 1000 {
        log::warn!(
            "[trace_id {trace_id}] search->storage: check file cache took: {check_cache_took} ms",
        );
    }

    // A check-only caller consumes the hit/miss counts and ScanStats above to
    // retain its local-cache fast paths. Return before selecting a cache type
    // or attempting optional warming admission.
    if !miss_policy.enqueue_misses() {
        return (file_data::CacheType::None, cache_hits, cache_misses);
    }

    let files_num = files.len() as i64;
    if files_num == scan_stats.querier_memory_cached_files + scan_stats.querier_disk_cached_files {
        return (file_data::CacheType::Disk, cache_hits, cache_misses);
    }

    let cache_type = warm_cache_type(scan_stats.compressed_size);
    if cache_type == file_data::CacheType::None {
        return (file_data::CacheType::None, cache_hits, cache_misses);
    };

    let cfg = get_config();
    let mut admission = QueryAdmission::new(
        cfg.limit.file_download_query_max_files,
        cfg.limit.file_download_query_max_bytes,
    );
    let mut accepted = 0usize;
    let mut accepted_bytes = 0u64;
    let mut deduplicated = 0usize;
    let mut skipped = 0usize;
    let mut cached = 0usize;
    let mut rejected = 0usize;
    let mut rejected_bytes = 0u64;
    let mut block_tier = 0usize;
    let mut block_tier_bytes = 0u64;
    for (id, account, file, size, ts, records) in files {
        if cached_files.contains(file) {
            cached += 1;
            continue;
        }
        if !file_downloader::should_download(*records) {
            skipped += 1;
            continue;
        }
        // Checked before the per-query quota so that objects left to the block
        // tier do not use up the quota of small objects in the same query.
        if leave_to_block_tier(cache_type, file, *size) {
            block_tier += 1;
            block_tier_bytes = block_tier_bytes.saturating_add((*size).max(0) as u64);
            record_block_tier_skip("query_bulk", *size);
            continue;
        }
        if !admission.allows((*size).max(0) as usize) {
            skipped += 1;
            if cfg.limit.file_download_ledger_enabled {
                config::metrics::FILE_DOWNLOAD_LEDGER_EVENTS
                    .with_label_values(&["query_bulk", "query_limit"])
                    .inc();
                config::metrics::FILE_DOWNLOAD_LEDGER_BYTES
                    .with_label_values(&["query_bulk", "query_limit"])
                    .inc_by((*size).max(0) as u64);
            }
            continue;
        }
        let class =
            if *size > 0 && (*size as u64) <= cfg.limit.file_download_small_object_bytes as u64 {
                file_downloader::DownloadClass::DemandSmall
            } else {
                file_downloader::DownloadClass::QueryBulk
            };
        // Borrow candidates directly: no detached producer or unsent Vec can
        // survive this query. Only admitted workers have an independent lifetime.
        match file_downloader::queue_download_class(
            trace_id, *id, account, file, *size, *ts, cache_type, class,
        )
        .await
        {
            file_downloader::QueueDownloadOutcome::Accepted { bytes } => {
                admission.accept(bytes);
                accepted += 1;
                accepted_bytes = accepted_bytes.saturating_add(bytes as u64);
            }
            file_downloader::QueueDownloadOutcome::Deduplicated => deduplicated += 1,
            file_downloader::QueueDownloadOutcome::Skipped => skipped += 1,
            file_downloader::QueueDownloadOutcome::Cached => cached += 1,
            file_downloader::QueueDownloadOutcome::Rejected(reason) => {
                rejected += 1;
                rejected_bytes = rejected_bytes.saturating_add((*size).max(0) as u64);
                log::debug!(
                    "[trace_id {trace_id}] search->storage: background download rejected file={file} size={size} reason={reason:?}"
                );
            }
        }
    }
    log::info!(
        "[trace_id {trace_id}] search->storage: background download admission file_type={file_type} cache={cache_type:?} accepted={accepted} accepted_bytes={accepted_bytes} deduplicated={deduplicated} skipped={skipped} cached={cached} rejected={rejected} rejected_bytes={rejected_bytes} block_tier={block_tier} block_tier_bytes={block_tier_bytes}"
    );

    if scan_stats.querier_memory_cached_files + scan_stats.querier_disk_cached_files < files_num / 2
    {
        (file_data::CacheType::None, cache_hits, cache_misses)
    } else {
        (cache_type, cache_hits, cache_misses)
    }
}

/// Quotas bound newly accepted optional work, not query correctness. Cache hits,
/// duplicates and rejected reservations do not consume the cohort budget.
struct QueryAdmission {
    files: usize,
    bytes: usize,
    max_files: usize,
    max_bytes: usize,
}
impl QueryAdmission {
    fn new(max_files: usize, max_bytes: usize) -> Self {
        Self {
            files: 0,
            bytes: 0,
            max_files,
            max_bytes,
        }
    }
    fn allows(&self, size: usize) -> bool {
        (self.max_files == 0 || self.files < self.max_files)
            && (self.max_bytes == 0 || size <= self.max_bytes.saturating_sub(self.bytes))
    }
    fn accept(&mut self, bytes: usize) {
        self.files = self.files.saturating_add(1);
        self.bytes = self.bytes.saturating_add(bytes);
    }
}

#[cfg(test)]
mod tests {
    use infra::cache::file_data::CacheType;

    use super::{QueryAdmission, calc_target_partitions, leaves_to_block_tier};

    #[test]
    fn large_files_objects_are_left_to_the_block_tier_only_on_disk_with_blocks() {
        const SMALL: usize = 8 << 20;
        let data = "files/default/logs/app/2026/11/02/01/a.vix";
        let sidecar = "files/default/logs/app/2026/11/02/01/a.vxi";
        let large = SMALL as i64 + 1;

        assert!(leaves_to_block_tier(
            CacheType::Disk,
            data,
            large,
            true,
            SMALL
        ));
        assert!(leaves_to_block_tier(
            CacheType::Disk,
            sidecar,
            large,
            true,
            SMALL
        ));
        // Unknown sizes are not small, which matches the DemandSmall class rule.
        assert!(leaves_to_block_tier(CacheType::Disk, data, 0, true, SMALL));
        assert!(leaves_to_block_tier(CacheType::Disk, data, -1, true, SMALL));

        // Small objects keep the whole-object download, including the boundary.
        assert!(!leaves_to_block_tier(
            CacheType::Disk,
            data,
            SMALL as i64,
            true,
            SMALL
        ));
        assert!(!leaves_to_block_tier(CacheType::Disk, data, 1, true, SMALL));
        // Nothing else caches the object without a disk block tier.
        assert!(!leaves_to_block_tier(
            CacheType::Disk,
            data,
            large,
            false,
            SMALL
        ));
        assert!(!leaves_to_block_tier(
            CacheType::Memory,
            data,
            large,
            true,
            SMALL
        ));
        assert!(!leaves_to_block_tier(
            CacheType::None,
            data,
            large,
            true,
            SMALL
        ));
        // The block tier serves only `files/` keys.
        assert!(!leaves_to_block_tier(
            CacheType::Disk,
            "wal_segments/node/00000000000000000001",
            large,
            true,
            SMALL
        ));
    }

    #[test]
    fn query_admission_file_and_byte_limits_count_only_accepted() {
        let mut admission = QueryAdmission::new(2, 100);
        for _ in 0..100 {
            assert!(admission.allows(60));
        }
        admission.accept(60);
        assert!(!admission.allows(41));
        assert!(admission.allows(40));
        admission.accept(40);
        assert!(!admission.allows(1));
        assert!(!admission.allows(0));
    }

    #[test]
    fn query_admission_rollback_and_overflow_are_explicit() {
        let mut disabled = QueryAdmission::new(0, 0);
        disabled.accept(usize::MAX);
        assert!(disabled.allows(usize::MAX));
        let mut bytes_only = QueryAdmission::new(0, 100);
        bytes_only.accept(99);
        assert!(!bytes_only.allows(usize::MAX));
        assert!(bytes_only.allows(1));
        let mut files_only = QueryAdmission::new(1, 0);
        assert!(files_only.allows(usize::MAX));
        files_only.accept(usize::MAX);
        assert!(!files_only.allows(1));
    }

    #[test]
    fn target_partitions_interpolates_cache_ratio() {
        assert_eq!(calc_target_partitions(8, 32, 0.0), 32);
        assert_eq!(calc_target_partitions(8, 32, 0.5), 20);
        assert_eq!(calc_target_partitions(8, 32, 1.0), 8);
    }
}
