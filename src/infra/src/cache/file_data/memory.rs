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
    cmp::{max, min},
    future::Future,
    ops::Range,
    sync::LazyLock as Lazy,
};

use bytes::Bytes;
use config::{
    RwHashMap, get_config, metrics, spawn_pausable_job,
    utils::{
        hash::{Sum64, gxhash},
        time::BASE_TIME,
    },
};
use futures::StreamExt;
use object_store::{GetOptions, GetResult, GetResultPayload, ObjectMeta};
use tokio::sync::RwLock;

use super::CacheStrategy;

static FILES: Lazy<Vec<RwLock<FileData>>> = Lazy::new(|| {
    let cfg = get_config();
    let mut files = Vec::with_capacity(cfg.memory_cache.bucket_num);
    for _ in 0..cfg.memory_cache.bucket_num {
        files.push(RwLock::new(FileData::new()));
    }
    files
});
static DATA: Lazy<Vec<RwHashMap<String, Bytes>>> = Lazy::new(|| {
    let cfg = get_config();
    let mut data = Vec::with_capacity(cfg.memory_cache.bucket_num);
    for _ in 0..cfg.memory_cache.bucket_num {
        data.push(Default::default());
    }
    data
});

type SpillEntry = (String, Bytes);

#[derive(Default)]
struct SetTransition {
    evicted: Vec<SpillEntry>,
    bypassed: Option<SpillEntry>,
}

pub struct FileData {
    cur_size: usize,
    data: CacheStrategy,
    #[cfg(test)]
    max_size: Option<usize>,
    #[cfg(test)]
    release_size: Option<usize>,
}

impl Default for FileData {
    fn default() -> Self {
        Self::new()
    }
}

impl FileData {
    pub fn new() -> FileData {
        let cfg = get_config();
        FileData::with_cache_strategy(&cfg.memory_cache.cache_strategy)
    }

    pub fn with_cache_strategy(strategy: &str) -> FileData {
        FileData {
            cur_size: 0,
            data: CacheStrategy::new(strategy),
            #[cfg(test)]
            max_size: None,
            #[cfg(test)]
            release_size: None,
        }
    }

    #[cfg(test)]
    pub fn with_cache_strategy_and_max_size(strategy: &str, max_size: usize) -> FileData {
        FileData {
            cur_size: 0,
            data: CacheStrategy::new(strategy),
            max_size: Some(max_size),
            release_size: Some(0),
        }
    }

    #[cfg(test)]
    fn with_cache_strategy_and_limits(
        strategy: &str,
        max_size: usize,
        release_size: usize,
    ) -> FileData {
        FileData {
            cur_size: 0,
            data: CacheStrategy::new(strategy),
            max_size: Some(max_size),
            release_size: Some(release_size),
        }
    }

    async fn exist(&self, file: &str) -> bool {
        self.data.contains_key(file)
    }

    async fn get(&self, file: &str, range: Option<Range<u64>>) -> Option<Bytes> {
        let idx = get_bucket_idx(file);
        let data = DATA[idx].get(file)?;
        Some(if let Some(range) = range {
            data.value().slice(range.start as usize..range.end as usize)
        } else {
            data.value().clone()
        })
    }

    async fn get_size(&self, file: &str) -> Option<usize> {
        let idx = get_bucket_idx(file);
        let data = DATA[idx].get(file)?;
        Some(data.value().len())
    }

    fn set(&mut self, file: &str, data: Bytes) -> Result<SetTransition, anyhow::Error> {
        if self.data.contains_key(file) {
            return Ok(SetTransition::default());
        }

        let data_size = file.len().saturating_add(data.len());
        #[cfg(test)]
        let max_size = self.max_size.unwrap_or(get_config().memory_cache.max_size);
        #[cfg(not(test))]
        let max_size = get_config().memory_cache.max_size;

        // A single entry that cannot fit in an empty bucket must never enter
        // memory. Return ownership so the caller can dispose of the
        // best-effort cache fill after releasing the metadata lock.
        if data_size > max_size {
            log::info!(
                "File memory cache bypassing {data_size} byte entry larger than {max_size} byte bucket"
            );
            return Ok(SetTransition {
                bypassed: Some((file.to_string(), data)),
                ..Default::default()
            });
        }

        let required_release = self
            .cur_size
            .saturating_add(data_size)
            .saturating_sub(max_size);
        let evicted = if required_release > 0 {
            #[cfg(test)]
            let configured_release = self
                .release_size
                .unwrap_or(get_config().memory_cache.release_size);
            #[cfg(not(test))]
            let configured_release = get_config().memory_cache.release_size;
            let release_target = min(self.cur_size, max(configured_release, required_release));
            log::info!(
                "File memory cache is full, releasing up to {release_target} bytes for {data_size} byte entry"
            );
            self.gc(release_target)
        } else {
            Vec::new()
        };

        // A corrupt/incomplete strategy must not turn a failed eviction into
        // an over-capacity insertion.
        if self.cur_size > max_size - data_size {
            log::warn!(
                "File memory cache could not release enough space; bypassing {data_size} byte entry"
            );
            return Ok(SetTransition {
                evicted,
                bypassed: Some((file.to_string(), data)),
            });
        }

        self.cur_size += data_size;
        self.data.insert(file.to_string(), data_size);
        let idx = get_bucket_idx(file);
        DATA[idx].insert(file.to_string(), data);
        update_metrics(file, data_size, true);

        debug_assert!(self.cur_size <= max_size);
        Ok(SetTransition {
            evicted,
            bypassed: None,
        })
    }

    fn gc(&mut self, need_release_size: usize) -> Vec<SpillEntry> {
        log::info!(
            "File memory cache start gc {}/{}, need to release {} bytes",
            self.cur_size,
            get_config().memory_cache.max_size,
            need_release_size
        );
        let mut release_size = 0;
        let mut spills = Vec::new();
        while release_size < need_release_size {
            let Some((key, data_size)) = self.data.remove() else {
                log::warn!("File memory cache is corrupt, it shouldn't be none");
                break;
            };

            let idx = get_bucket_idx(&key);
            if let Some((key, data)) = DATA[idx].remove(&key) {
                spills.push((key, data));
            }
            self.cur_size -= data_size;
            release_size += data_size;
            update_metrics(&key, data_size, false);
        }
        log::info!("File memory cache gc done, released {release_size} bytes");
        spills
    }

    fn remove(&mut self, file: &str) -> bool {
        log::debug!("File memory cache remove file {file}");

        let Some((key, data_size)) = self.data.remove_key(file) else {
            return false;
        };
        self.cur_size -= data_size;

        let idx = get_bucket_idx(&key);
        DATA[idx].remove(&key);
        update_metrics(&key, data_size, false);
        true
    }

    fn size(&self) -> usize {
        self.cur_size
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
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
    // Cache placement is best-effort. Dropping detached entries outside the
    // metadata lock keeps both memory and disk-cache bounds intact and cannot
    // resurrect a key after an invalidation. Object storage and any existing
    // disk-cache entry remain authoritative.
    drop(transition.evicted);
    drop(transition.bypassed);
    Ok(())
}

async fn set_with_spill<F, Fut>(
    files: &RwLock<FileData>,
    file: &str,
    data: Bytes,
    spill: F,
) -> Result<(), anyhow::Error>
where
    F: FnOnce(SetTransition) -> Fut,
    Fut: Future<Output = Result<(), anyhow::Error>>,
{
    let transition = {
        let mut files = files.write().await;
        files.set(file, data)?
    };
    spill(transition).await
}

pub async fn init() -> Result<(), anyhow::Error> {
    for file in FILES.iter() {
        _ = file.read().await.get("", None).await;
    }

    spawn_pausable_job!("memory_cache_gc", get_config().memory_cache.gc_interval, {
        if let Err(e) = gc().await {
            log::error!("memory cache gc error: {e}");
        }
    });
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
    let idx = get_bucket_idx(file);
    let files = FILES[idx].read().await;
    files.get(file, range).await
}

#[inline]
pub async fn get_size(file: &str) -> Option<usize> {
    if !get_config().memory_cache.enabled {
        return None;
    }
    let idx = get_bucket_idx(file);
    let files = FILES[idx].read().await;
    files.get_size(file).await
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
    let idx = get_bucket_idx(file);
    let files = FILES[idx].read().await;
    files.exist(file).await
}

#[inline]
pub async fn set(file: &str, data: Bytes) -> Result<(), anyhow::Error> {
    if !get_config().memory_cache.enabled {
        return Ok(());
    }
    let idx = get_bucket_idx(file);
    set_with_spill(&FILES[idx], file, data, apply_set_transition).await
}

#[inline]
pub async fn remove(file: &str) -> Result<(), anyhow::Error> {
    if !get_config().memory_cache.enabled {
        return Ok(());
    }
    let idx = get_bucket_idx(file);
    let mut files = FILES[idx].write().await;
    files.remove(file);
    Ok(())
}

async fn gc() -> Result<(), anyhow::Error> {
    let cfg = get_config();
    if !cfg.memory_cache.enabled {
        return Ok(());
    }

    for file in FILES.iter() {
        let evicted = {
            let mut files = file.write().await;
            if files.cur_size.saturating_add(cfg.memory_cache.release_size)
                < cfg.memory_cache.max_size
            {
                continue;
            }
            files.gc(cfg.memory_cache.gc_size)
        };
        // Periodic eviction deliberately does not demote to disk. Disk and
        // remote storage remain authoritative, and no I/O follows this lock.
        drop(evicted);
    }

    Ok(())
}

#[inline]
pub async fn stats() -> (usize, usize, usize) {
    let mut total_size = 0;
    let mut used_size = 0;
    let mut item_len = 0;
    for file in FILES.iter() {
        let r = file.read().await;
        total_size += get_config().memory_cache.max_size;
        used_size += r.size();
        item_len += r.len();
    }
    (total_size, used_size, item_len)
}

#[inline]
pub async fn is_empty() -> bool {
    for file in FILES.iter() {
        let r = file.read().await;
        if !r.is_empty() {
            return false;
        }
    }
    true
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

fn get_bucket_idx(file: &str) -> usize {
    let cfg = get_config();
    if cfg.memory_cache.bucket_num <= 1 {
        0
    } else {
        let h = gxhash::new().sum64(file);
        (h as usize) % cfg.memory_cache.bucket_num
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_lru_cache_set_file() {
        let mut file_data = FileData::with_cache_strategy_and_max_size("lru", 1024);
        let content = Bytes::from("Some text Need to store in cache");
        for i in 0..50 {
            let file_key = format!(
                "files/default/logs/memory/2022/10/03/10/6982652937134804993_1_{i}.parquet"
            );
            let resp = file_data.set(&file_key, content.clone());
            assert!(resp.is_ok());
        }
    }

    #[tokio::test]
    async fn test_lru_cache_get_file() {
        let mut file_data =
            FileData::with_cache_strategy_and_max_size("lru", get_config().memory_cache.max_size);
        let file_key = "files/default/logs/memory/2022/10/03/10/6982652937134804993_2_1.parquet";
        let content = Bytes::from("Some text");

        file_data.set(file_key, content.clone()).unwrap();
        assert_eq!(file_data.get(file_key, None).await.unwrap(), content);

        file_data.set(file_key, content.clone()).unwrap();
        assert!(file_data.exist(file_key).await);
        assert_eq!(file_data.get(file_key, None).await.unwrap(), content);
        assert!(file_data.size() > 0);
    }

    #[tokio::test]
    async fn test_lru_cache_miss() {
        let mut file_data = FileData::with_cache_strategy_and_max_size("lru", 100);
        let file_key1 = "files/default/logs/memory/2022/10/03/10/6982652937134804993_3_1.parquet";
        let file_key2 = "files/default/logs/memory/2022/10/03/10/6982652937134804993_3_2.parquet";
        let content = Bytes::from("Some text");
        // set one key
        file_data.set(file_key1, content.clone()).unwrap();
        assert_eq!(file_data.get(file_key1, None).await.unwrap(), content);
        // set another key, will release first key
        file_data.set(file_key2, content.clone()).unwrap();
        assert_eq!(file_data.get(file_key2, None).await.unwrap(), content);
        // get first key, should get error
        assert!(file_data.get(file_key1, None).await.is_none());
    }

    #[tokio::test]
    async fn test_fifo_cache_set_file() {
        let mut file_data = FileData::with_cache_strategy_and_max_size("fifo", 1024);
        let content = Bytes::from("Some text Need to store in cache");
        for i in 0..50 {
            let file_key = format!(
                "files/default/logs/memory/2022/10/03/10/6982652937134804993_4_{i}.parquet"
            );
            let resp = file_data.set(&file_key, content.clone());
            assert!(resp.is_ok());
        }
    }

    #[tokio::test]
    async fn test_fifo_cache_get_file() {
        let mut file_data =
            FileData::with_cache_strategy_and_max_size("fifo", get_config().memory_cache.max_size);
        let file_key = "files/default/logs/memory/2022/10/03/10/6982652937134804993_5_1.parquet";
        let content = Bytes::from("Some text");

        file_data.set(file_key, content.clone()).unwrap();
        assert_eq!(file_data.get(file_key, None).await.unwrap(), content);

        file_data.set(file_key, content.clone()).unwrap();
        assert!(file_data.exist(file_key).await);
        assert_eq!(file_data.get(file_key, None).await.unwrap(), content);
        assert!(file_data.size() > 0);
    }

    #[tokio::test]
    async fn test_fifo_cache_miss() {
        let mut file_data = FileData::with_cache_strategy_and_max_size("fifo", 100);
        let file_key1 = "files/default/logs/memory/2022/10/03/10/6982652937134804993_6_1.parquet";
        let file_key2 = "files/default/logs/memory/2022/10/03/10/6982652937134804993_6_2.parquet";
        let content = Bytes::from("Some text");
        // set one key
        file_data.set(file_key1, content.clone()).unwrap();
        assert_eq!(file_data.get(file_key1, None).await.unwrap(), content);
        // set another key, will release first key
        file_data.set(file_key2, content.clone()).unwrap();
        assert_eq!(file_data.get(file_key2, None).await.unwrap(), content);
        // get first key, should get error
        assert!(file_data.get(file_key1, None).await.is_none());
    }

    #[tokio::test]
    async fn test_stats_basic() {
        // Test that stats function returns correct tuple structure
        let (total_size, used_size, _item_len) = stats().await;

        // used_size should not exceed total_size
        assert!(used_size <= total_size);

        // total_size should be equal to max_size * bucket_num
        let cfg = get_config();
        let expected_total = cfg.memory_cache.max_size * cfg.memory_cache.bucket_num;
        assert_eq!(total_size, expected_total);
    }

    #[tokio::test]
    async fn test_stats_with_data() {
        // Get initial stats
        let (initial_total, initial_used, initial_len) = stats().await;

        // Add data to cache
        let test_key = format!(
            "files/default/logs/memory/2022/10/03/10/{}_test_stats.parquet",
            config::utils::time::now_micros()
        );
        let content = Bytes::from("Test content for stats function");

        // Use the set function to add data
        let bucket_id = gxhash::new().sum64(&test_key);
        let bucket_id = (bucket_id as usize) % FILES.len();
        let mut file_data = FILES[bucket_id].write().await;
        let _ = file_data.set(&test_key, content.clone());
        drop(file_data);

        // Get stats after adding data
        let (after_total, after_used, after_len) = stats().await;

        // Verify that used_size and item_len increased
        assert!(after_used >= initial_used);
        assert!(after_len >= initial_len);

        // Total size should remain the same
        assert_eq!(after_total, initial_total);
    }

    #[tokio::test]
    async fn test_stats_aggregates_across_buckets() {
        // This test verifies that stats correctly aggregates data across all bucket files
        let test_id = config::utils::time::now_micros();
        let content = Bytes::from("Test content for aggregation");

        // Get initial stats
        let (_, initial_used, initial_len) = stats().await;

        // Add data to multiple buckets by using different keys
        for i in 0..3 {
            let file_key = format!(
                "files/default/logs/memory/2022/10/03/10/{}_{}_test.parquet",
                test_id, i
            );

            let bucket_id = gxhash::new().sum64(&file_key);
            let bucket_id = (bucket_id as usize) % FILES.len();
            let mut file_data = FILES[bucket_id].write().await;
            let _ = file_data.set(&file_key, content.clone());
            drop(file_data);
        }

        // Get stats after adding data to multiple buckets
        let (_, after_used, after_len) = stats().await;

        // Verify that stats increased
        assert!(after_used >= initial_used);
        assert!(after_len >= initial_len);
    }

    #[tokio::test]
    async fn test_stats_consistency() {
        // Verify that stats returns valid values across multiple calls
        // Note: In a concurrent test environment, values may increase due to other tests
        let (total1, used1, len1) = stats().await;
        let (total2, used2, len2) = stats().await;

        // Total size should remain consistent (based on config)
        assert_eq!(total1, total2);

        // Used size and length should not decrease (may increase due to concurrent tests)
        assert!(used2 >= used1 || used1 - used2 < 1000); // Allow minor decrease due to GC
        assert!(len2 >= len1 || len1 - len2 < 10); // Allow minor decrease due to GC
    }

    #[tokio::test]
    async fn test_stats_empty_cache() {
        // Test stats on a fresh FileData instance
        let file_data = FileData::with_cache_strategy_and_max_size("lru", 1024);

        let size = file_data.size();
        let len = file_data.len();

        // For an empty cache:
        assert_eq!(size, 0); // Should have no data
        assert_eq!(len, 0); // Should have no items
    }

    #[tokio::test]
    async fn test_stats_with_different_sized_data() {
        // Test that stats correctly accounts for different sized data
        let test_id = config::utils::time::now_micros();

        // Get initial stats
        let (_, initial_used, initial_len) = stats().await;

        // Add small data
        let small_key = format!("files/test/{}_small.parquet", test_id);
        let small_content = Bytes::from("Small");
        let bucket_id = gxhash::new().sum64(&small_key);
        let bucket_id = (bucket_id as usize) % FILES.len();
        let mut file_data = FILES[bucket_id].write().await;
        let _ = file_data.set(&small_key, small_content.clone());
        drop(file_data);

        // Add large data
        let large_key = format!("files/test/{}_large.parquet", test_id);
        let large_content = Bytes::from(vec![0u8; 1000]);
        let bucket_id = gxhash::new().sum64(&large_key);
        let bucket_id = (bucket_id as usize) % FILES.len();
        let mut file_data = FILES[bucket_id].write().await;
        let _ = file_data.set(&large_key, large_content.clone());
        drop(file_data);

        // Get stats after adding both small and large data
        let (_, after_used, after_len) = stats().await;

        // Verify that stats increased by at least the size of the data
        let min_expected_increase =
            small_key.len() + small_content.len() + large_key.len() + large_content.len();
        assert!(after_used >= initial_used + min_expected_increase);
        assert!(after_len >= initial_len + 2); // Added 2 items
    }

    fn bytes_for_entry_size(key: &str, entry_size: usize) -> Bytes {
        assert!(entry_size >= key.len());
        Bytes::from(vec![b'x'; entry_size - key.len()])
    }

    #[tokio::test]
    async fn test_set_releases_only_bounded_space() {
        let keys = ["bounded-a", "bounded-b", "bounded-c", "bounded-d"];
        let mut file_data = FileData::with_cache_strategy_and_limits("fifo", 90, 10);

        for key in &keys[..3] {
            let transition = file_data.set(key, bytes_for_entry_size(key, 30)).unwrap();
            assert!(transition.evicted.is_empty());
            assert!(transition.bypassed.is_none());
        }
        assert_eq!(file_data.size(), 90);

        let transition = file_data
            .set(keys[3], bytes_for_entry_size(keys[3], 20))
            .unwrap();

        assert_eq!(transition.evicted.len(), 1);
        assert_eq!(transition.evicted[0].0, keys[0]);
        assert!(transition.bypassed.is_none());
        assert_eq!(file_data.size(), 80);
        assert_eq!(file_data.len(), 3);
        assert!(!file_data.exist(keys[0]).await);
        for key in &keys[1..] {
            assert!(file_data.exist(key).await);
            assert!(file_data.remove(key));
        }
    }

    #[tokio::test]
    async fn test_set_honors_release_size_without_flushing_bucket() {
        let keys = [
            "release-size-00",
            "release-size-01",
            "release-size-02",
            "release-size-03",
            "release-size-04",
            "release-size-05",
            "release-size-06",
            "release-size-07",
            "release-size-08",
            "release-size-09",
            "release-size-10",
        ];
        let mut file_data = FileData::with_cache_strategy_and_limits("fifo", 300, 50);
        for key in &keys[..10] {
            let transition = file_data.set(key, bytes_for_entry_size(key, 30)).unwrap();
            assert!(transition.evicted.is_empty());
            assert!(transition.bypassed.is_none());
        }

        let transition = file_data
            .set(keys[10], bytes_for_entry_size(keys[10], 30))
            .unwrap();

        // The 50-byte configured release is rounded up only by whole entries.
        assert_eq!(transition.evicted.len(), 2);
        assert!(transition.bypassed.is_none());
        assert_eq!(file_data.size(), 270);
        assert_eq!(file_data.len(), 9);
        for key in &keys[2..] {
            assert!(file_data.remove(key));
        }
    }

    #[test]
    fn test_oversized_entry_bypasses_memory_without_copying() {
        let key = "oversized-entry";
        let data = Bytes::from(vec![b'x'; 32]);
        let data_ptr = data.as_ptr();
        let mut file_data = FileData::with_cache_strategy_and_limits("fifo", 32, 8);

        let transition = file_data.set(key, data).unwrap();

        assert!(transition.evicted.is_empty());
        let (bypassed_key, bypassed_data) = transition.bypassed.unwrap();
        assert_eq!(bypassed_key, key);
        assert_eq!(bypassed_data.as_ptr(), data_ptr);
        assert_eq!(file_data.size(), 0);
        assert_eq!(file_data.len(), 0);
        assert!(!DATA[get_bucket_idx(key)].contains_key(key));
    }

    #[tokio::test]
    async fn test_eviction_and_remove_keep_accounting_exact() {
        let org_id = format!("memory-gc-accounting-{}", config::utils::time::now_micros());
        let keys = [
            format!("files/{org_id}/logs/a"),
            format!("files/{org_id}/logs/b"),
            format!("files/{org_id}/logs/c"),
        ];
        let payload = Bytes::from_static(b"accounting");
        let entry_size = keys[0].len() + payload.len();
        assert!(keys.iter().all(|key| key.len() == keys[0].len()));

        let files_metric = metrics::QUERY_MEMORY_CACHE_FILES.with_label_values(&[&org_id, "logs"]);
        let bytes_metric =
            metrics::QUERY_MEMORY_CACHE_USED_BYTES.with_label_values(&[&org_id, "logs"]);
        let initial_files = files_metric.get();
        let initial_bytes = bytes_metric.get();
        let mut file_data = FileData::with_cache_strategy_and_limits("fifo", entry_size * 2, 0);

        let first = file_data.set(&keys[0], payload.clone()).unwrap();
        let second = file_data.set(&keys[1], payload.clone()).unwrap();
        assert!(first.evicted.is_empty() && first.bypassed.is_none());
        assert!(second.evicted.is_empty() && second.bypassed.is_none());
        assert_eq!(file_data.size(), entry_size * 2);
        assert_eq!(file_data.len(), 2);
        assert_eq!(files_metric.get(), initial_files + 2);
        assert_eq!(bytes_metric.get(), initial_bytes + (entry_size * 2) as i64);

        let transition = file_data.set(&keys[2], payload).unwrap();
        assert_eq!(transition.evicted.len(), 1);
        assert_eq!(transition.evicted[0].0, keys[0]);
        assert!(transition.bypassed.is_none());
        assert_eq!(file_data.size(), entry_size * 2);
        assert_eq!(file_data.len(), 2);
        assert_eq!(files_metric.get(), initial_files + 2);
        assert_eq!(bytes_metric.get(), initial_bytes + (entry_size * 2) as i64);

        assert!(file_data.remove(&keys[1]));
        assert!(file_data.remove(&keys[2]));
        assert_eq!(file_data.size(), 0);
        assert_eq!(file_data.len(), 0);
        assert_eq!(files_metric.get(), initial_files);
        assert_eq!(bytes_metric.get(), initial_bytes);
    }

    #[tokio::test]
    async fn test_reader_proceeds_while_slow_spill_is_outside_lock() {
        let first_key = "slow-spill-a";
        let second_key = "slow-spill-b";
        let entry_size = first_key.len() + 16;
        let cache = std::sync::Arc::new(RwLock::new(FileData::with_cache_strategy_and_limits(
            "fifo", entry_size, 0,
        )));
        set_with_spill(
            cache.as_ref(),
            first_key,
            Bytes::from(vec![b'a'; 16]),
            |_| async { Ok(()) },
        )
        .await
        .unwrap();

        let (spill_started_tx, spill_started_rx) = tokio::sync::oneshot::channel();
        let (finish_spill_tx, finish_spill_rx) = tokio::sync::oneshot::channel();
        let writer_cache = cache.clone();
        let writer = tokio::spawn(async move {
            set_with_spill(
                writer_cache.as_ref(),
                second_key,
                Bytes::from(vec![b'b'; 16]),
                |transition| async move {
                    assert_eq!(transition.evicted.len(), 1);
                    assert_eq!(transition.evicted[0].0, first_key);
                    assert!(transition.bypassed.is_none());
                    spill_started_tx.send(()).unwrap();
                    finish_spill_rx.await.unwrap();
                    Ok(())
                },
            )
            .await
            .unwrap();
        });

        spill_started_rx.await.unwrap();
        let exists = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            let files = cache.read().await;
            files.exist(second_key).await
        })
        .await
        .expect("reader blocked behind disk spill");
        assert!(exists);

        finish_spill_tx.send(()).unwrap();
        writer.await.unwrap();
        assert!(cache.write().await.remove(second_key));
    }

    #[test]
    fn test_get_bucket_idx_valid_range() {
        let idx = get_bucket_idx("some/file/path.parquet");
        let cfg = config::get_config();
        let max = cfg.memory_cache.bucket_num.max(1);
        assert!(idx < max);
    }

    #[test]
    fn test_get_bucket_idx_empty_string() {
        let idx = get_bucket_idx("");
        let cfg = config::get_config();
        let max = cfg.memory_cache.bucket_num.max(1);
        assert!(idx < max);
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
}
