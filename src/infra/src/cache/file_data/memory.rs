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

use std::{
    ops::Range,
    sync::{
        Arc, LazyLock as Lazy,
        atomic::{AtomicUsize, Ordering},
    },
};

use bytes::Bytes;
use config::{get_config, metrics, spawn_pausable_job, utils::time::BASE_TIME};
use futures::StreamExt;
use moka::{policy::EvictionPolicy, sync::Cache};
use object_store::{GetOptions, GetResult, GetResultPayload, ObjectMeta};

static MEMORY_CACHE: Lazy<FileData> = Lazy::new(FileData::new);

#[derive(Default)]
struct SetTransition {
    bypassed: Option<Bytes>,
    // Tests assert the no-op-on-existing-key rule through this flag.
    #[cfg_attr(not(test), allow(dead_code))]
    inserted: bool,
}

struct CacheAccounting {
    used_bytes: AtomicUsize,
    entry_count: AtomicUsize,
}

impl CacheAccounting {
    fn add(&self, size: usize) {
        // Resident bytes are bounded by process memory, far below usize::MAX.
        self.used_bytes.fetch_add(size, Ordering::AcqRel);
        self.entry_count.fetch_add(1, Ordering::AcqRel);
    }

    fn remove(&self, size: usize, key: &str) {
        if self
            .used_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_sub(size)
            })
            .is_err()
        {
            log::error!(
                "File memory cache accounting lost {size} bytes while removing {key}; cache state needs review"
            );
        }
        if self
            .entry_count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                count.checked_sub(1)
            })
            .is_err()
        {
            log::error!(
                "File memory cache accounting lost an entry while removing {key}; cache state needs review"
            );
        }
    }

    fn used_bytes(&self) -> usize {
        self.used_bytes.load(Ordering::Acquire)
    }

    fn entry_count(&self) -> usize {
        self.entry_count.load(Ordering::Acquire)
    }
}

/// A single weighted cache for every memory-cache key.
///
/// Moka's entry API makes check-and-insert atomic per key. This preserves the
/// existing rule that a second cache fill for an existing key is a no-op
/// without a cache-wide lock.
pub struct FileData {
    cache: Cache<String, Bytes>,
    total_bytes: usize,
    accounting: Arc<CacheAccounting>,
    maintenance: super::MaintenanceGate,
}

impl Default for FileData {
    fn default() -> Self {
        Self::new()
    }
}

impl FileData {
    pub fn new() -> FileData {
        let cfg = get_config();
        let total_bytes = cfg
            .memory_cache
            .max_size
            .saturating_mul(cfg.memory_cache.bucket_num);
        Self::with_capacity(&cfg.memory_cache.cache_strategy, total_bytes)
    }

    pub fn with_cache_strategy(strategy: &str) -> FileData {
        let cfg = get_config();
        let total_bytes = cfg
            .memory_cache
            .max_size
            .saturating_mul(cfg.memory_cache.bucket_num);
        Self::with_capacity(strategy, total_bytes)
    }

    #[cfg(test)]
    pub fn with_cache_strategy_and_max_size(strategy: &str, max_size: usize) -> FileData {
        Self::with_capacity(strategy, max_size)
    }

    #[cfg(test)]
    fn with_cache_strategy_and_limits(
        strategy: &str,
        max_size: usize,
        _release_size: usize,
    ) -> FileData {
        Self::with_capacity(strategy, max_size)
    }

    fn with_capacity(_strategy: &str, total_bytes: usize) -> FileData {
        let accounting = Arc::new(CacheAccounting {
            used_bytes: AtomicUsize::new(0),
            entry_count: AtomicUsize::new(0),
        });
        let listener_accounting = Arc::clone(&accounting);
        let cache = Cache::builder()
            .max_capacity(capacity_kib(total_bytes))
            .weigher(|key: &String, data: &Bytes| weight_kib(entry_size(key, data)))
            .eviction_policy(EvictionPolicy::lru())
            .eviction_listener(move |key, data, _cause| {
                let size = entry_size(key.as_ref(), &data);
                listener_accounting.remove(size, key.as_ref());
                update_metrics(key.as_ref(), size, false);
            })
            .build();

        FileData {
            cache,
            total_bytes,
            accounting,
            maintenance: super::MaintenanceGate::default(),
        }
    }

    async fn exist(&self, file: &str) -> bool {
        // Downloader probes must not make a cold entry recent.
        self.cache.contains_key(file)
    }

    async fn get(&self, file: &str, range: Option<Range<u64>>) -> Option<Bytes> {
        // `get` records a read hit, so the configured strategy is access-ordered LRU.
        let data = self.cache.get(file)?;
        Some(if let Some(range) = range {
            data.slice(range.start as usize..range.end as usize)
        } else {
            data
        })
    }

    async fn get_size(&self, file: &str) -> Option<usize> {
        // Size is read from the cached value. This is also a data read and records recency.
        self.cache.get(file).map(|data| data.len())
    }

    fn set(&self, file: &str, data: Bytes) -> Result<SetTransition, anyhow::Error> {
        let data_size = entry_size(file, &data);
        // Moka disables a zero-capacity cache without notifying the listener,
        // so sub-KiB budgets bypass instead of leaking accounting.
        if data_size > self.total_bytes || self.total_bytes < 1024 {
            log::info!(
                "File memory cache bypassing {data_size} byte entry larger than {0} byte cache",
                self.total_bytes
            );
            return Ok(SetTransition {
                bypassed: Some(data),
                ..Default::default()
            });
        }

        // The init closure runs only when the key is absent. It accounts before
        // the value is published, so an eviction listener always finds a credit.
        let entry = self.cache.entry_by_ref(file).or_insert_with(|| {
            self.accounting.add(data_size);
            update_metrics(file, data_size, true);
            data
        });
        if !entry.is_fresh() {
            return Ok(SetTransition::default());
        }

        // Moka applies writes lazily. One pass bounds overshoot without reviving
        // the old synchronous, release-size-based whole-cache garbage collection.
        // Single-flight: concurrent writers must not queue on moka's lock.
        if self.accounting.used_bytes() > self.total_bytes {
            self.maintenance.try_run(|| self.cache.run_pending_tasks());
        }

        Ok(SetTransition {
            inserted: true,
            ..Default::default()
        })
    }

    fn remove(&self, file: &str) -> Result<bool, anyhow::Error> {
        log::debug!("File memory cache remove file {file}");
        // Moka notifies the listener synchronously for explicit removal, so
        // accounting is exact when this returns.
        Ok(self.cache.remove(file).is_some())
    }

    fn size(&self) -> usize {
        self.accounting.used_bytes()
    }

    fn len(&self) -> usize {
        self.accounting.entry_count()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn run_pending_tasks(&self) {
        self.cache.run_pending_tasks();
    }
}

fn entry_size(file: &str, data: &Bytes) -> usize {
    file.len().saturating_add(data.len())
}

fn capacity_kib(bytes: usize) -> u64 {
    // Round down so rounded-up entry weights cannot exceed the byte budget.
    u64::try_from(bytes / 1024).unwrap_or(u64::MAX)
}

fn weight_kib(bytes: usize) -> u32 {
    let kib = bytes.saturating_add(1023) / 1024;
    u32::try_from(kib.max(1)).unwrap_or(u32::MAX)
}

fn update_metrics(file: &str, data_size: usize, added: bool) {
    let mut columns = file.split('/');
    if columns.next() != Some("files") {
        return;
    }
    let (Some(org_id), Some(stream_type)) = (columns.next(), columns.next()) else {
        return;
    };
    let files = metrics::QUERY_MEMORY_CACHE_FILES.with_label_values(&[org_id, stream_type]);
    let bytes = metrics::QUERY_MEMORY_CACHE_USED_BYTES.with_label_values(&[org_id, stream_type]);
    if added {
        files.inc();
        bytes.add(data_size as i64);
    } else {
        files.dec();
        bytes.sub(data_size as i64);
    }
}

async fn apply_set_transition(transition: SetTransition) -> Result<(), anyhow::Error> {
    // A bypassed `Bytes` retains the caller's allocation. Dropping it performs
    // no copy and leaves disk or object storage authoritative.
    drop(transition.bypassed);
    Ok(())
}

pub async fn init() -> Result<(), anyhow::Error> {
    let cfg = get_config();
    let _ = &*MEMORY_CACHE;
    log::info!(
        "File memory cache configured strategy '{}' now uses access-ordered LRU",
        cfg.memory_cache.cache_strategy
    );

    if cfg.memory_cache.gc_interval > 0 {
        spawn_pausable_job!("memory_cache_gc", cfg.memory_cache.gc_interval, {
            if let Err(e) = gc().await {
                log::error!("memory cache pending-task run failed: {e}");
            }
        });
    }
    Ok(())
}

pub async fn get_opts(file: &str, options: GetOptions) -> object_store::Result<GetResult> {
    let Some(data) = get(file, None).await else {
        return Err(object_store::Error::NotFound {
            path: file.to_string(),
            source: Box::new(std::io::Error::other("file not found")),
        });
    };

    let meta = ObjectMeta {
        location: file.into(),
        last_modified: *BASE_TIME,
        size: data.len() as u64,
        e_tag: Some(format!(
            "{:x}-{:x}",
            BASE_TIME.timestamp_micros(),
            data.len()
        )),
        version: None,
    };
    options.check_preconditions(&meta)?;

    let (range, data) = match options.range {
        Some(range) => {
            let r = range.as_range(data.len() as u64).map_err(|e| {
                object_store::Error::Precondition {
                    path: file.to_string(),
                    source: Box::new(e),
                }
            })?;
            (r.clone(), data.slice(r.start as usize..r.end as usize))
        }
        None => (0..data.len() as u64, data),
    };
    let stream = futures::stream::once(futures::future::ready(Ok(data)));

    Ok(GetResult {
        payload: GetResultPayload::Stream(stream.boxed()),
        attributes: Default::default(),
        meta,
        range,
    })
}

#[inline]
pub async fn get(file: &str, range: Option<Range<u64>>) -> Option<Bytes> {
    if !get_config().memory_cache.enabled {
        return None;
    }
    MEMORY_CACHE.get(file, range).await
}

#[inline]
pub async fn get_size(file: &str) -> Option<usize> {
    if !get_config().memory_cache.enabled {
        return None;
    }
    MEMORY_CACHE.get_size(file).await
}

/// Slice the cached in-memory `Bytes` once for each requested range.
/// Returns `None` if the file isn't in the memory cache or any range is
/// out of bounds — callers should fall through to disk / remote.
///
/// This is the batched counterpart to [`get_opts`] used by the search
/// hot path: one cache lookup yields N small slices with zero IO.
pub async fn get_ranges(file: &str, ranges: &[Range<u64>]) -> Option<Vec<Bytes>> {
    if !get_config().memory_cache.enabled || ranges.is_empty() {
        return None;
    }
    let data = get(file, None).await?;
    let len = data.len() as u64;
    let mut out = Vec::with_capacity(ranges.len());
    for r in ranges {
        if r.start > r.end || r.end > len {
            return None;
        }
        out.push(data.slice(r.start as usize..r.end as usize));
    }
    Some(out)
}

#[inline]
pub async fn exist(file: &str) -> bool {
    if !get_config().memory_cache.enabled {
        return false;
    }
    MEMORY_CACHE.exist(file).await
}

#[inline]
pub async fn set(file: &str, data: Bytes) -> Result<(), anyhow::Error> {
    if !get_config().memory_cache.enabled {
        return Ok(());
    }
    let transition = MEMORY_CACHE.set(file, data)?;
    apply_set_transition(transition).await
}

#[inline]
pub async fn remove(file: &str) -> Result<(), anyhow::Error> {
    if !get_config().memory_cache.enabled {
        return Ok(());
    }
    MEMORY_CACHE.remove(file)?;
    Ok(())
}

async fn gc() -> Result<(), anyhow::Error> {
    if !get_config().memory_cache.enabled {
        return Ok(());
    }
    MEMORY_CACHE.run_pending_tasks();
    Ok(())
}

#[inline]
pub async fn stats() -> (usize, usize, usize) {
    (
        MEMORY_CACHE.total_bytes,
        MEMORY_CACHE.size(),
        MEMORY_CACHE.len(),
    )
}

#[inline]
pub async fn is_empty() -> bool {
    MEMORY_CACHE.is_empty()
}

pub async fn download(
    account: &str,
    file: &str,
    size: Option<usize>,
) -> Result<usize, anyhow::Error> {
    let (data_len, data_bytes) = super::download_from_storage(account, file, size).await?;
    if let Err(e) = set(file, data_bytes).await {
        return Err(anyhow::anyhow!(
            "set file {} to memory cache failed: {}",
            file,
            e
        ));
    };
    Ok(data_len)
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use super::*;

    fn bytes_for_entry_size(key: &str, entry_size: usize) -> Bytes {
        assert!(entry_size >= key.len());
        Bytes::from(vec![b'x'; entry_size - key.len()])
    }

    fn test_cache(max_size: usize) -> FileData {
        FileData::with_cache_strategy_and_max_size("lru", max_size)
    }

    #[tokio::test]
    async fn test_lru_cache_set_file() {
        let file_data = test_cache(4 * 1024);
        for i in 0..50 {
            let file_key = format!("files/default/logs/memory/lru-set-{i}.parquet");
            let transition = file_data
                .set(&file_key, bytes_for_entry_size(&file_key, 1024))
                .unwrap();
            assert!(transition.inserted);
            assert!(transition.bypassed.is_none());
        }
        file_data.run_pending_tasks();
        assert!(file_data.size() <= 4 * 1024);
    }

    #[tokio::test]
    async fn test_lru_cache_get_file() {
        let file_data = test_cache(2 * 1024);
        let file_key = "files/default/logs/memory/lru-get.parquet";
        let content = Bytes::from("Some text");

        file_data.set(file_key, content.clone()).unwrap();
        assert_eq!(file_data.get(file_key, None).await.unwrap(), content);

        let transition = file_data
            .set(file_key, Bytes::from_static(b"replacement"))
            .unwrap();
        assert!(!transition.inserted);
        assert!(file_data.exist(file_key).await);
        assert_eq!(file_data.get(file_key, None).await.unwrap(), content);
        assert!(file_data.size() > 0);
    }

    #[tokio::test]
    async fn test_lru_cache_miss() {
        let file_data = test_cache(1024);
        let file_key1 = "lru-miss-one";
        let file_key2 = "lru-miss-two";
        let content1 = bytes_for_entry_size(file_key1, 1024);
        let content2 = bytes_for_entry_size(file_key2, 1024);

        file_data.set(file_key1, content1).unwrap();
        assert!(file_data.get(file_key1, None).await.is_some());
        file_data.set(file_key2, content2).unwrap();
        file_data.run_pending_tasks();

        assert!(file_data.get(file_key2, None).await.is_some());
        assert!(file_data.get(file_key1, None).await.is_none());
    }

    #[tokio::test]
    async fn test_fifo_cache_set_file() {
        let file_data = FileData::with_cache_strategy_and_max_size("fifo", 4 * 1024);
        for i in 0..50 {
            let file_key = format!("files/default/logs/memory/fifo-set-{i}.parquet");
            assert!(
                file_data
                    .set(&file_key, bytes_for_entry_size(&file_key, 1024))
                    .unwrap()
                    .inserted
            );
        }
        file_data.run_pending_tasks();
        assert!(file_data.size() <= 4 * 1024);
    }

    #[tokio::test]
    async fn test_fifo_cache_get_file() {
        let file_data = FileData::with_cache_strategy_and_max_size("fifo", 2 * 1024);
        let file_key = "files/default/logs/memory/fifo-get.parquet";
        let content = Bytes::from("Some text");

        file_data.set(file_key, content.clone()).unwrap();
        assert_eq!(file_data.get(file_key, None).await.unwrap(), content);
        assert!(
            !file_data
                .set(file_key, Bytes::from_static(b"replacement"))
                .unwrap()
                .inserted
        );
        assert!(file_data.exist(file_key).await);
        assert_eq!(file_data.get(file_key, None).await.unwrap(), content);
    }

    #[tokio::test]
    async fn test_fifo_cache_miss() {
        let file_data = FileData::with_cache_strategy_and_max_size("fifo", 1024);
        let file_key1 = "fifo-miss-one";
        let file_key2 = "fifo-miss-two";
        file_data
            .set(file_key1, bytes_for_entry_size(file_key1, 1024))
            .unwrap();
        file_data
            .set(file_key2, bytes_for_entry_size(file_key2, 1024))
            .unwrap();
        file_data.run_pending_tasks();

        assert!(file_data.get(file_key2, None).await.is_some());
        assert!(file_data.get(file_key1, None).await.is_none());
    }

    #[tokio::test]
    async fn test_stats_basic() {
        let (total_size, used_size, _item_len) = stats().await;
        assert!(used_size <= total_size.saturating_add(1024));
        let cfg = get_config();
        assert_eq!(
            total_size,
            cfg.memory_cache
                .max_size
                .saturating_mul(cfg.memory_cache.bucket_num)
        );
    }

    #[tokio::test]
    async fn test_stats_with_data() {
        let file_data = test_cache(4 * 1024);
        let test_key = "files/default/logs/memory/test-stats.parquet";
        let content = bytes_for_entry_size(test_key, 1024);
        file_data.set(test_key, content).unwrap();

        assert_eq!(file_data.size(), 1024);
        assert_eq!(file_data.len(), 1);
    }

    #[tokio::test]
    async fn test_stats_aggregates_across_buckets() {
        let file_data = test_cache(4 * 1024);
        for i in 0..3 {
            let file_key = format!("files/default/logs/memory/global-stats-{i}.parquet");
            file_data
                .set(&file_key, bytes_for_entry_size(&file_key, 1024))
                .unwrap();
        }
        file_data.run_pending_tasks();

        assert_eq!(file_data.size(), 3 * 1024);
        assert_eq!(file_data.len(), 3);
    }

    #[tokio::test]
    async fn test_stats_consistency() {
        let file_data = test_cache(4 * 1024);
        let file_key = "files/default/logs/memory/stats-consistency.parquet";
        file_data
            .set(file_key, bytes_for_entry_size(file_key, 1024))
            .unwrap();
        let (used1, len1) = (file_data.size(), file_data.len());
        let (used2, len2) = (file_data.size(), file_data.len());

        assert_eq!(used1, used2);
        assert_eq!(len1, len2);
    }

    #[tokio::test]
    async fn test_stats_empty_cache() {
        let file_data = test_cache(1024);
        assert_eq!(file_data.size(), 0);
        assert_eq!(file_data.len(), 0);
    }

    #[tokio::test]
    async fn test_stats_with_different_sized_data() {
        let file_data = test_cache(4 * 1024);
        let small_key = "files/test/logs/small.parquet";
        let large_key = "files/test/logs/large.parquet";
        file_data
            .set(small_key, bytes_for_entry_size(small_key, 1024))
            .unwrap();
        file_data
            .set(large_key, bytes_for_entry_size(large_key, 2 * 1024))
            .unwrap();

        assert_eq!(file_data.size(), 3 * 1024);
        assert_eq!(file_data.len(), 2);
    }

    #[tokio::test]
    async fn test_set_releases_only_bounded_space() {
        let keys = ["bounded-a", "bounded-b", "bounded-c", "bounded-d"];
        let file_data = FileData::with_cache_strategy_and_limits("fifo", 3 * 1024, 10);
        for key in &keys[..3] {
            assert!(
                file_data
                    .set(key, bytes_for_entry_size(key, 1024))
                    .unwrap()
                    .inserted
            );
        }

        assert!(
            file_data
                .set(keys[3], bytes_for_entry_size(keys[3], 1024))
                .unwrap()
                .inserted
        );
        file_data.run_pending_tasks();

        assert_eq!(file_data.size(), 3 * 1024);
        assert_eq!(file_data.len(), 3);
        assert!(!file_data.exist(keys[0]).await);
        for key in &keys[1..] {
            assert!(file_data.exist(key).await);
        }
    }

    #[tokio::test]
    async fn test_set_honors_release_size_without_flushing_bucket() {
        let file_data = FileData::with_cache_strategy_and_limits("fifo", 10 * 1024, 50);
        let keys: Vec<String> = (0..11).map(|i| format!("release-size-{i:02}")).collect();
        for key in &keys {
            file_data.set(key, bytes_for_entry_size(key, 1024)).unwrap();
        }
        file_data.run_pending_tasks();

        // `release_size` is intentionally ignored. Moka evicts only enough LRU
        // entries to meet the weighted global capacity.
        assert_eq!(file_data.size(), 10 * 1024);
        assert_eq!(file_data.len(), 10);
        assert!(!file_data.exist(&keys[0]).await);
        assert!(file_data.exist(&keys[10]).await);
    }

    #[test]
    fn test_oversized_entry_bypasses_memory_without_copying() {
        let key = "oversized-entry";
        let data = Bytes::from(vec![b'x'; 32]);
        let data_ptr = data.as_ptr();
        let file_data = FileData::with_cache_strategy_and_limits("fifo", 32, 8);

        let transition = file_data.set(key, data).unwrap();

        let bypassed_data = transition.bypassed.unwrap();
        assert_eq!(bypassed_data.as_ptr(), data_ptr);
        assert_eq!(file_data.size(), 0);
        assert_eq!(file_data.len(), 0);
        assert!(!file_data.cache.contains_key(key));
    }

    #[tokio::test]
    async fn test_eviction_and_remove_keep_accounting_exact() {
        let org_id = format!("memory-gc-accounting-{}", config::utils::time::now_micros());
        let keys = [
            format!("files/{org_id}/logs/a"),
            format!("files/{org_id}/logs/b"),
            format!("files/{org_id}/logs/c"),
        ];
        let entry_size = 1024;
        let files_metric = metrics::QUERY_MEMORY_CACHE_FILES.with_label_values(&[&org_id, "logs"]);
        let bytes_metric =
            metrics::QUERY_MEMORY_CACHE_USED_BYTES.with_label_values(&[&org_id, "logs"]);
        let initial_files = files_metric.get();
        let initial_bytes = bytes_metric.get();
        let file_data = FileData::with_cache_strategy_and_limits("fifo", entry_size * 2, 0);

        for key in &keys[..2] {
            file_data
                .set(key, bytes_for_entry_size(key, entry_size))
                .unwrap();
        }
        assert_eq!(file_data.size(), entry_size * 2);
        assert_eq!(file_data.len(), 2);
        assert_eq!(files_metric.get(), initial_files + 2);
        assert_eq!(bytes_metric.get(), initial_bytes + (entry_size * 2) as i64);

        file_data
            .set(&keys[2], bytes_for_entry_size(&keys[2], entry_size))
            .unwrap();
        file_data.run_pending_tasks();
        assert_eq!(file_data.size(), entry_size * 2);
        assert_eq!(file_data.len(), 2);
        assert_eq!(files_metric.get(), initial_files + 2);
        assert_eq!(bytes_metric.get(), initial_bytes + (entry_size * 2) as i64);

        assert!(file_data.remove(&keys[1]).unwrap());
        assert!(file_data.remove(&keys[2]).unwrap());
        assert_eq!(file_data.size(), 0);
        assert_eq!(file_data.len(), 0);
        assert_eq!(files_metric.get(), initial_files);
        assert_eq!(bytes_metric.get(), initial_bytes);
    }

    #[tokio::test]
    async fn test_reader_proceeds_while_slow_spill_is_outside_lock() {
        let first_key = "slow-spill-a";
        let second_key = "slow-spill-b";
        let cache = Arc::new(test_cache(1024));
        cache
            .set(first_key, bytes_for_entry_size(first_key, 1024))
            .unwrap();

        let writer_cache = Arc::clone(&cache);
        let writer = tokio::task::spawn_blocking(move || {
            writer_cache
                .set(second_key, bytes_for_entry_size(second_key, 1024))
                .unwrap();
            writer_cache.run_pending_tasks();
        });
        let reader_cache = Arc::clone(&cache);
        let read = tokio::time::timeout(Duration::from_secs(1), async move {
            loop {
                if reader_cache.get(first_key, None).await.is_some()
                    || reader_cache.get(second_key, None).await.is_some()
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;

        assert!(read.is_ok(), "reader blocked while Moka evicted an entry");
        writer.await.unwrap();
    }

    #[test]
    fn test_file_data_new_is_empty() {
        let fd = FileData::with_cache_strategy("lru");
        assert!(fd.is_empty());
        assert_eq!(fd.len(), 0);
        assert_eq!(fd.size(), 0);
    }

    #[test]
    fn test_file_data_default_is_empty() {
        let fd = FileData::default();
        assert!(fd.is_empty());
        assert_eq!(fd.size(), 0);
    }

    #[tokio::test]
    async fn test_global_capacity_evicts_across_all_keys() {
        let file_data = test_cache(4 * 1024);
        for i in 0..8 {
            let key = format!("files/default/logs/global-capacity-{i}.parquet");
            file_data
                .set(&key, bytes_for_entry_size(&key, 1024))
                .unwrap();
        }
        file_data.run_pending_tasks();

        assert!(file_data.size() <= 4 * 1024);
        assert_eq!(file_data.len(), 4);
    }

    #[tokio::test]
    async fn test_get_recency_survives_eviction() {
        let file_data = test_cache(3 * 1024);
        let keys = ["recency-a", "recency-b", "recency-c", "recency-d"];
        for key in &keys[..3] {
            file_data.set(key, bytes_for_entry_size(key, 1024)).unwrap();
        }
        // Moka applies queued reads before queued writes. Publish the inserts
        // first so the read below is recorded against an admitted entry.
        file_data.run_pending_tasks();
        assert!(file_data.get(keys[0], None).await.is_some());
        file_data.run_pending_tasks();
        file_data
            .set(keys[3], bytes_for_entry_size(keys[3], 1024))
            .unwrap();
        file_data.run_pending_tasks();

        assert!(file_data.exist(keys[0]).await);
        assert!(!file_data.exist(keys[1]).await);
        assert!(file_data.exist(keys[2]).await);
        assert!(file_data.exist(keys[3]).await);
    }

    #[tokio::test]
    async fn test_small_cache_eviction_does_not_flush_all_entries() {
        let file_data = test_cache(4 * 1024);
        let keys: Vec<String> = (0..5).map(|i| format!("result-small-{i}")).collect();
        for key in &keys {
            file_data.set(key, bytes_for_entry_size(key, 1024)).unwrap();
        }
        file_data.run_pending_tasks();

        assert_eq!(file_data.len(), 4);
        assert_eq!(file_data.size(), 4 * 1024);
        assert!(file_data.exist(&keys[4]).await);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrency_stress_reconciles_exact_accounting() {
        let cache = Arc::new(test_cache(32 * 1024));
        let tasks = (0..16).map(|task_id| {
            let cache = Arc::clone(&cache);
            tokio::spawn(async move {
                for op in 0..256 {
                    let key = format!("stress-{}", (task_id * 97 + op) % 64);
                    match op % 4 {
                        0 => {
                            cache.set(&key, bytes_for_entry_size(&key, 1024)).unwrap();
                        }
                        1 => {
                            let _ = cache.get(&key, None).await;
                        }
                        2 => {
                            let _ = cache.exist(&key).await;
                        }
                        _ => {
                            cache.remove(&key).unwrap();
                        }
                    }
                    if op % 16 == 0 {
                        tokio::task::yield_now().await;
                    }
                }
            })
        });
        for task in tasks {
            tokio::time::timeout(Duration::from_secs(10), task)
                .await
                .expect("cache stress task timed out")
                .unwrap();
        }
        cache.run_pending_tasks();

        let entries: Vec<_> = cache.cache.iter().collect();
        let entry_bytes: usize = entries
            .iter()
            .map(|(key, data)| entry_size(key.as_ref(), data))
            .sum();
        assert_eq!(cache.size(), entry_bytes);
        assert_eq!(cache.len(), entries.len());
        assert_eq!(cache.cache.entry_count() as usize, entries.len());
        assert!(cache.size() <= 32 * 1024);
    }
}
