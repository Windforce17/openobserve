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
    fmt,
    ops::Range,
    path::{Path, PathBuf},
    sync::{
        Arc, LazyLock as Lazy, Mutex, MutexGuard,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::SystemTime,
};

use bytes::Bytes;
use config::{
    RwAHashMap, get_config, metrics, spawn_pausable_job,
    utils::{
        file::*,
        hash::{Sum64, gxhash},
        time::{HourFormat, get_ymdh_from_micros, now_micros},
    },
};
use moka::{notification::RemovalCause, policy::EvictionPolicy, sync::Cache};
use object_store::{GetOptions, GetRange, GetResult, GetResultPayload, ObjectMeta};
use tokio::sync::{RwLock, mpsc};

use crate::cache::meta::ResultCacheMeta;
#[cfg(not(test))]
use crate::storage;

// Whole-file capacity is global; stripes serialize only filesystem publication/removal.
static FILES: Lazy<Arc<FileData>> = Lazy::new(|| FileData::new(FileType::Data));
static RESULT_FILES: Lazy<Arc<FileData>> = Lazy::new(|| FileData::new(FileType::Result));
static AGGREGATION_FILES: Lazy<Arc<FileData>> = Lazy::new(|| FileData::new(FileType::Aggregation));
#[cfg(test)]
static TEST_CACHE_ROOT: Lazy<tempfile::TempDir> =
    Lazy::new(|| tempfile::tempdir().expect("create test cache directory"));
#[cfg(test)]
static TEST_TMP_ROOT: Lazy<tempfile::TempDir> =
    Lazy::new(|| tempfile::tempdir().expect("create test cache tmp directory"));
static DELETIONS: Lazy<DeleteQueue> = Lazy::new(DeleteQueue::new);
const STRIPES: usize = 1024;
const DELETE_BATCH_SIZE: usize = 256;

pub static QUERY_RESULT_CACHE: Lazy<RwAHashMap<String, Vec<ResultCacheMeta>>> =
    Lazy::new(Default::default);

pub static METRICS_RESULT_CACHE: Lazy<RwLock<Vec<String>>> = Lazy::new(|| RwLock::new(Vec::new()));

pub static LOADING_FROM_DISK_NUM: Lazy<AtomicUsize> = Lazy::new(|| AtomicUsize::new(0));
pub static LOADING_FROM_DISK_DONE: Lazy<AtomicBool> = Lazy::new(|| AtomicBool::new(false));

/// Owns a disk-cache temporary path until it is renamed. Drop uses a
/// synchronous unlink so cancellation at any await cannot strand
/// segment-sized `.tmp` files until the next process restart.
#[derive(Clone)]
struct TempFileGuard {
    path: std::sync::Arc<TempFilePath>,
}

struct TempFilePath(String);

impl TempFileGuard {
    fn new(path: String) -> Self {
        Self {
            path: std::sync::Arc::new(TempFilePath(path)),
        }
    }

    fn path(&self) -> &str {
        &self.path.0
    }
}

impl std::ops::Deref for TempFileGuard {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.path()
    }
}

impl AsRef<std::ffi::OsStr> for TempFileGuard {
    fn as_ref(&self) -> &std::ffi::OsStr {
        self.path().as_ref()
    }
}

impl Drop for TempFilePath {
    fn drop(&mut self) {
        match std::fs::remove_file(&self.0) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                log::warn!(
                    "[FileData::Disk] remove abandoned temporary file {} failed: {error}",
                    self.0
                );
            }
        }
    }
}

struct FileData {
    max_size: usize,
    used_bytes: AtomicUsize,
    entries: AtomicUsize,
    pending_deletes: AtomicUsize,
    root_dir: String,
    multi_dir: Vec<String>,
    file_type: FileType,
    data: Cache<String, DiskEntry>,
    stripes: [Mutex<()>; STRIPES],
    maintenance: super::MaintenanceGate,
}

#[derive(Clone)]
struct DiskEntry {
    size: usize,
}

struct DeleteRequest {
    key: Arc<String>,
    files: Arc<FileData>,
}

impl Drop for DeleteRequest {
    fn drop(&mut self) {
        // Also balance queued requests dropped during runtime shutdown or send failure.
        self.files.pending_deletes.fetch_sub(1, Ordering::Relaxed);
        metrics::DISK_CACHE_PENDING_DELETES
            .with_label_values(&[self.files.file_type.label()])
            .dec();
    }
}

struct DeleteQueue {
    sender: mpsc::UnboundedSender<DeleteRequest>,
    receiver: Mutex<Option<mpsc::UnboundedReceiver<DeleteRequest>>>,
}

impl DeleteQueue {
    fn new() -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        Self {
            sender,
            receiver: Mutex::new(Some(receiver)),
        }
    }

    fn start(&self) {
        let mut receiver = self.receiver.lock().unwrap_or_else(|error| {
            log::error!("disk cache deleter startup mutex poisoned; recovering: {error}");
            error.into_inner()
        });
        if let Some(receiver) = receiver.take() {
            tokio::spawn(delete_evictions(receiver));
        }
    }
}

async fn delete_evictions(mut receiver: mpsc::UnboundedReceiver<DeleteRequest>) {
    // A single consumer bounds blocking IO concurrency. Closing the sender drains
    // the backlog; process/runtime termination may leave files for startup load.
    loop {
        let mut batch = Vec::with_capacity(DELETE_BATCH_SIZE);
        if receiver.recv_many(&mut batch, DELETE_BATCH_SIZE).await == 0 {
            break;
        }
        let batch = match tokio::task::spawn_blocking(move || {
            let mut query_keys = Vec::new();
            for (index, request) in batch.iter().enumerate() {
                let files = &request.files;
                let _stripe = files.lock_key(&request.key);
                // The same stripe protects rename+insert. An older queued eviction
                // must never unlink a newly published generation of this key.
                if files.data.contains_key(request.key.as_str()) {
                    continue;
                }
                let path = files.get_file_path(&request.key);
                if let Err(error) = std::fs::remove_file(&path)
                    && error.kind() != std::io::ErrorKind::NotFound
                {
                    metrics::DISK_CACHE_DELETE_ERRORS_TOTAL
                        .with_label_values(&[files.file_type.label()]).inc();
                    log::error!("[CacheType:{}] disk cache eviction unlink {path} failed: {error}; file remains until a later removal or startup load", files.file_type);
                }
                if request.key.starts_with("results/") {
                    let columns: Vec<_> = request.key.split('/').collect();
                    if columns.len() >= 5 {
                        query_keys.push((index, format!("{}_{}_{}_{}", columns[1], columns[2], columns[3], columns[4])));
                    }
                }
            }
            (batch, query_keys)
        }).await {
            Ok(batch) => batch,
            Err(error) => {
                log::error!("disk cache eviction batch failed: {error}; inspect remaining cache files");
                continue;
            }
        };
        let (requests, query_keys) = batch;
        if !query_keys.is_empty() {
            let mut cache = QUERY_RESULT_CACHE.write().await;
            for (index, key) in query_keys {
                let request = &requests[index];
                let _stripe = request.files.lock_key(&request.key);
                // Recheck after the async metadata lock: a publish may have won
                // since unlink. No stripe is held while awaiting that lock.
                if !request.files.data.contains_key(request.key.as_str()) {
                    cache.remove(&key);
                }
            }
        }
        // Pending includes query metadata cleanup, so zero means the batch is done.
        drop(requests);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FileGeneration {
    size: u64,
    e_tag: String,
    modified: Option<SystemTime>,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
}

impl FileGeneration {
    fn from_metadata(metadata: &std::fs::Metadata) -> Self {
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt;

        Self {
            size: metadata.len(),
            e_tag: get_etag(metadata),
            modified: metadata.modified().ok(),
            #[cfg(unix)]
            device: metadata.dev(),
            #[cfg(unix)]
            inode: metadata.ino(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum FileType {
    Data,
    Result,
    Aggregation,
}

impl fmt::Display for FileType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FileType::Data => write!(f, "data"),
            FileType::Result => write!(f, "result"),
            FileType::Aggregation => write!(f, "aggregation"),
        }
    }
}

impl FileType {
    fn label(self) -> &'static str {
        match self {
            Self::Data => "data",
            Self::Result => "result",
            Self::Aggregation => "aggregation",
        }
    }
}

fn weight_kib(size: usize) -> u32 {
    size.div_ceil(1024).max(1).min(u32::MAX as usize) as u32
}

impl FileData {
    fn new(file_type: FileType) -> Arc<Self> {
        let cfg = get_config();
        let per_bucket_size = match file_type {
            FileType::Data => cfg.disk_cache.max_size,
            FileType::Result => cfg.disk_cache.result_max_size,
            FileType::Aggregation => cfg.disk_cache.aggregation_max_size,
        };
        #[cfg(not(test))]
        let root_dir = format!(
            "{}{}",
            cfg.common.data_cache_dir,
            storage::format_key("", true)
        );
        #[cfg(test)]
        let root_dir = format!("{}/", TEST_CACHE_ROOT.path().display());
        Self::with_capacity(
            file_type,
            per_bucket_size.saturating_mul(cfg.disk_cache.bucket_num),
            root_dir,
            cfg.disk_cache
                .multi_dir
                .split(',')
                .filter(|s| !s.trim().is_empty())
                .map(|s| s.trim().to_string())
                .collect(),
            DELETIONS.sender.clone(),
        )
    }

    fn with_capacity(
        file_type: FileType,
        max_size: usize,
        root_dir: String,
        multi_dir: Vec<String>,
        deletes: mpsc::UnboundedSender<DeleteRequest>,
    ) -> Arc<Self> {
        Arc::new_cyclic(|weak: &std::sync::Weak<Self>| {
            let weak = weak.clone();
            let data = Cache::builder()
                // Round the budget down so rounded entry weights cannot exceed
                // the byte budget after maintenance. Sub-KiB budgets bypass IO.
                .max_capacity((max_size / 1024) as u64)
                .weigher(|_key: &String, entry: &DiskEntry| weight_kib(entry.size))
                .eviction_policy(EvictionPolicy::lru())
                .eviction_listener(move |key: Arc<String>, entry: DiskEntry, cause| {
                    let Some(files) = weak.upgrade() else { return; };
                    // Credits precede insert, so every callback has a matching credit,
                    // even when moka invokes it from another writer's housekeeping.
                    files.used_bytes.fetch_sub(entry.size, Ordering::Relaxed);
                    files.entries.fetch_sub(1, Ordering::Relaxed);
                    update_metrics(&key, -(entry.size as i64), -1);
                    if matches!(cause, RemovalCause::Size | RemovalCause::Expired) {
                        metrics::DISK_CACHE_EVICTED_BYTES_TOTAL
                            .with_label_values(&[file_type.label()]).inc_by(entry.size as u64);
                        files.pending_deletes.fetch_add(1, Ordering::Relaxed);
                        metrics::DISK_CACHE_PENDING_DELETES
                            .with_label_values(&[file_type.label()]).inc();
                        if let Err(error) = deletes.send(DeleteRequest { key, files }) {
                            metrics::DISK_CACHE_DELETE_ERRORS_TOTAL
                                .with_label_values(&[file_type.label()]).inc();
                            log::error!("[CacheType:{file_type}] disk cache deleter unavailable for {}; restart to reload remaining files", error.0.key);
                        }
                    }
                    // Explicit removal already unlinked the path. Replaced already
                    // published its new inode. Neither cause may enqueue an unlink.
                })
                .build();
            Self {
                max_size,
                used_bytes: AtomicUsize::new(0),
                entries: AtomicUsize::new(0),
                pending_deletes: AtomicUsize::new(0),
                root_dir,
                multi_dir,
                file_type,
                data,
                stripes: std::array::from_fn(|_| Mutex::new(())),
                maintenance: super::MaintenanceGate::default(),
            }
        })
    }

    fn lock_key(&self, file: &str) -> MutexGuard<'_, ()> {
        self.stripes[stripe_index(file)]
            .lock()
            .unwrap_or_else(|error| {
                log::error!("disk cache stripe poisoned for {file}; recovering lock: {error}");
                error.into_inner()
            })
    }

    async fn exist(&self, file: &str) -> bool {
        self.data.contains_key(file)
    }

    fn get_file_path(&self, file: &str) -> String {
        format!("{}{}{}", self.root_dir, self.choose_multi_dir(file), file)
    }

    async fn get(&self, file: &str, range: Option<Range<u64>>) -> Option<Bytes> {
        self.data.get(file);
        let file_path = self.get_file_path(file);
        tokio::task::spawn_blocking(move || match get_file_contents(&file_path, range) {
            Ok(data) => Some(Bytes::from(data)),
            Err(_) => None,
        })
        .await
        .ok()
        .flatten()
    }

    async fn get_size(&self, file: &str) -> Option<usize> {
        let file_path = self.get_file_path(file);
        match get_file_size(&file_path) {
            Ok(v) => Some(v as usize),
            Err(_) => None,
        }
    }

    async fn set(
        &self,
        file: &str,
        tmp_file: &str,
        data_size: usize,
    ) -> Result<bool, anyhow::Error> {
        // Moka disables zero-capacity maps without notifying the listener.
        // Bypass even empty files rather than publishing an unaccounted orphan.
        if data_size > self.max_size || self.max_size < 1024 {
            log::debug!(
                "[CacheType:{}] bypass disk cache entry {file}: {data_size} bytes cannot fit capacity {} bytes in KiB weights",
                self.file_type,
                self.max_size
            );
            unlink_tmp(tmp_file)?;
            return Ok(false);
        }
        let start = std::time::Instant::now();
        let file_path = self.get_file_path(file);
        let parent = Path::new(&file_path)
            .parent()
            .ok_or_else(|| anyhow::anyhow!("disk cache path has no parent: {file_path}"))?;
        tokio::fs::create_dir_all(parent).await.map_err(|error| {
            anyhow::anyhow!(
                "create disk cache parent {} failed: {error}",
                parent.display()
            )
        })?;
        {
            let lock_start = std::time::Instant::now();
            let _stripe = self.lock_key(file);
            if lock_start.elapsed().as_millis() > 100 {
                log::info!(
                    "disk->cache: set file {file} get lock took: {} ms",
                    lock_start.elapsed().as_millis()
                );
            }
            if self.data.contains_key(file) {
                if let Err(error) = unlink_tmp(tmp_file) {
                    log::warn!(
                        "[CacheType:{}] duplicate disk cache tmp cleanup failed: {error}; TempFileGuard will retry on drop",
                        self.file_type
                    );
                }
                return Ok(false);
            }
            // No await can separate rename from index publication. TempFileGuard
            // owns the temporary inode until this cancellation-free section ends.
            std::fs::rename(tmp_file, &file_path).map_err(|error| anyhow::anyhow!("[CacheType:{}] File disk cache rename tmp file {tmp_file} to real file {file_path} error: {error}", self.file_type))?;
            self.insert(file, data_size);
        }
        self.enforce_capacity();
        if start.elapsed().as_millis() > 100 {
            log::info!(
                "[CacheType:{}] File disk cache rename file {file_path} took: {} ms",
                self.file_type,
                start.elapsed().as_millis()
            );
        }
        Ok(true)
    }

    // Call only while holding the key stripe. Moka callbacks never take stripes.
    fn insert(&self, file: &str, data_size: usize) {
        self.used_bytes.fetch_add(data_size, Ordering::Relaxed);
        self.entries.fetch_add(1, Ordering::Relaxed);
        update_metrics(file, data_size as i64, 1);
        self.data
            .insert(file.to_string(), DiskEntry { size: data_size });
    }

    fn enforce_capacity(&self) {
        if self.used_bytes.load(Ordering::Relaxed) > self.max_size {
            // One pass bounds overshoot to in-flight inserts without blocking on IO.
            // Single-flight: concurrent writers must not queue on moka's lock.
            self.maintenance.try_run(|| self.data.run_pending_tasks());
        }
    }

    fn remove_index_entry(&self, file: &str) {
        self.data.invalidate(file);
    }

    /// Unlink a physical entry and update its index without an await point
    /// between the two operations. If `expected` is supplied, only the exact
    /// physical generation observed by the caller may be removed.
    fn remove(&self, file: &str, expected: Option<&FileGeneration>) -> Result<bool, anyhow::Error> {
        log::debug!(
            "[CacheType:{}] File disk cache remove file {file}",
            self.file_type,
        );

        let _stripe = self.lock_key(file);
        let file_path = self.get_file_path(file);
        match std::fs::metadata(&file_path) {
            Ok(metadata) => {
                if expected
                    .is_some_and(|expected| FileGeneration::from_metadata(&metadata) != *expected)
                {
                    return Ok(false);
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                self.remove_index_entry(file);
                return Ok(true);
            }
            Err(error) => {
                return Err(anyhow::anyhow!(
                    "[CacheType:{}] File disk cache stat file {file_path} before removal error: {error}",
                    self.file_type,
                ));
            }
        }

        match std::fs::remove_file(&file_path) {
            Ok(()) => {
                self.remove_index_entry(file);
                Ok(true)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                self.remove_index_entry(file);
                Ok(true)
            }
            Err(error) => Err(anyhow::anyhow!(
                "[CacheType:{}] File disk cache remove file {file_path} error: {error}",
                self.file_type,
            )),
        }
    }

    /// Clear an index entry only if the physical path is still absent. This
    /// closes the failed-open/concurrent-fill race without unlinking the fill.
    fn remove_index_if_missing(&self, file: &str) -> Result<bool, anyhow::Error> {
        let _stripe = self.lock_key(file);
        let file_path = self.get_file_path(file);
        match std::fs::metadata(&file_path) {
            Ok(_) => Ok(false),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                self.remove_index_entry(file);
                Ok(true)
            }
            Err(error) => Err(anyhow::anyhow!(
                "[CacheType:{}] File disk cache stat file {file_path} before stale-index removal error: {error}",
                self.file_type,
            )),
        }
    }

    fn choose_multi_dir(&self, file: &str) -> String {
        if self.multi_dir.is_empty() {
            return "".to_string();
        }

        let h = gxhash::new().sum64(file);
        let index = h % (self.multi_dir.len() as u64);
        format!("{}/", self.multi_dir.get(index as usize).unwrap())
    }

    fn size(&self) -> (usize, usize) {
        (self.max_size, self.used_bytes.load(Ordering::Relaxed))
    }

    fn len(&self) -> usize {
        self.entries.load(Ordering::Relaxed)
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

fn stripe_index(file: &str) -> usize {
    gxhash::new().sum64(file) as usize % STRIPES
}

fn unlink_tmp(path: &str) -> Result<(), anyhow::Error> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(anyhow::anyhow!(
            "remove unused disk cache temporary file {path} failed: {error}"
        )),
    }
}

fn update_metrics(file: &str, bytes: i64, entries: i64) {
    let columns: Vec<_> = file.split('/').collect();
    match columns.as_slice() {
        ["files", org, stream_type, ..] => {
            metrics::QUERY_DISK_CACHE_FILES
                .with_label_values(&[org, stream_type])
                .add(entries);
            metrics::QUERY_DISK_CACHE_USED_BYTES
                .with_label_values(&[org, stream_type])
                .add(bytes);
        }
        ["results", org, stream_type, ..] => {
            metrics::QUERY_DISK_RESULT_CACHE_USED_BYTES
                .with_label_values(&[org, stream_type, "results"])
                .add(bytes);
        }
        ["metrics_results", org, ..] => {
            metrics::QUERY_DISK_METRICS_CACHE_USED_BYTES
                .with_label_values(&[org])
                .add(bytes);
        }
        ["aggregations", org, stream_type, ..] => {
            metrics::QUERY_DISK_RESULT_CACHE_USED_BYTES
                .with_label_values(&[org, stream_type, "aggregations"])
                .add(bytes);
        }
        _ => {}
    }
}

pub async fn init() -> Result<(), anyhow::Error> {
    let cfg = get_config();
    if let Err(error) = std::fs::remove_dir_all(&cfg.common.data_tmp_dir)
        && error.kind() != std::io::ErrorKind::NotFound
    {
        log::error!(
            "clean disk cache tmp directory {} failed: {error}",
            cfg.common.data_tmp_dir
        );
    }
    std::fs::create_dir_all(&cfg.common.data_tmp_dir).map_err(|error| {
        anyhow::anyhow!(
            "create disk cache tmp directory {} failed: {error}",
            cfg.common.data_tmp_dir
        )
    })?;
    std::fs::create_dir_all(&FILES.root_dir).map_err(|error| {
        anyhow::anyhow!(
            "create disk cache directory {} failed: {error}",
            FILES.root_dir
        )
    })?;
    // Start before load: loading beyond capacity can enqueue evictions immediately.
    DELETIONS.start();
    log::info!(
        "Disk cache configured strategy '{}'; using moka access-ordered LRU",
        cfg.disk_cache.cache_strategy
    );

    #[cfg(unix)]
    if cfg.disk_cache.enabled && cfg.limit.file_download_device_check_enabled {
        let mut roots = vec![PathBuf::from(&FILES.root_dir)];
        roots.extend(
            FILES
                .multi_dir
                .iter()
                .map(|dir| PathBuf::from(format!("{}{dir}", FILES.root_dir))),
        );
        for cache in roots {
            super::download_io::check_same_device(
                Path::new(&cfg.common.data_tmp_dir),
                &cache,
                cfg.limit.file_download_same_device_required,
            )?;
        }
    }
    tokio::spawn(async {
        log::info!("Loading disk cache start");
        match tokio::fs::canonicalize(&FILES.root_dir).await {
            Ok(root) => {
                if let Err(error) = load(&root, [&FILES, &RESULT_FILES, &AGGREGATION_FILES]).await {
                    log::error!("load disk cache {} failed: {error}", root.display());
                }
            }
            Err(error) => log::error!(
                "canonicalize disk cache root {} failed: {error}",
                FILES.root_dir
            ),
        }
        log::info!(
            "Loading disk cache done, total files: {}",
            LOADING_FROM_DISK_NUM.load(Ordering::Relaxed)
        );
        LOADING_FROM_DISK_DONE.store(true, Ordering::SeqCst);
    });
    if cfg.disk_cache.gc_interval > 0 {
        spawn_pausable_job!("disk_cache_gc", cfg.disk_cache.gc_interval, {
            for files in [&FILES, &RESULT_FILES, &AGGREGATION_FILES] {
                files.data.run_pending_tasks();
            }
        });
    }
    Ok(())
}

fn cache_for_key(file: &str) -> &'static FileData {
    if file.starts_with("files") {
        &FILES
    } else if file.starts_with("aggregations") {
        &AGGREGATION_FILES
    } else {
        &RESULT_FILES
    }
}

#[inline]
fn get_file_reader(file: &str) -> Option<&FileData> {
    get_config().disk_cache.enabled.then(|| cache_for_key(file))
}

// `InvalidGetRange` is not part of the object-store API surface we rely on.
// Preserve `as_range`'s validation order, then identify only the request
// shapes that it reports as StartTooLarge.
fn is_start_too_large(requested: &GetRange, size: u64) -> bool {
    if requested.is_valid().is_err() {
        return false;
    }
    match requested {
        GetRange::Bounded(range) => range.start >= size,
        GetRange::Offset(offset) => *offset >= size,
        GetRange::Suffix(_) => false,
    }
}

fn resolve_get_range(
    file: &str,
    requested: Option<&GetRange>,
    size: u64,
) -> object_store::Result<Range<u64>> {
    match requested {
        Some(range) => range.as_range(size).map_err(|source| {
            if is_start_too_large(range, size) {
                object_store::Error::NotFound {
                    path: file.to_string(),
                    source: Box::new(source),
                }
            } else {
                object_store::Error::Precondition {
                    path: file.to_string(),
                    source: Box::new(source),
                }
            }
        }),
        None => Ok(0..size),
    }
}

#[cfg(test)]
async fn remove_generation(file: &str, generation: &FileGeneration) -> Result<bool, anyhow::Error> {
    cache_for_key(file).remove(file, Some(generation))
}

async fn clear_index_if_missing(file: &str) -> Result<bool, anyhow::Error> {
    cache_for_key(file).remove_index_if_missing(file)
}

pub async fn get_opts(file: &str, options: GetOptions) -> object_store::Result<GetResult> {
    let Some(files) = get_file_reader(file) else {
        return Err(object_store::Error::NotFound {
            path: file.to_string(),
            source: Box::new(std::io::Error::other("file not found")),
        });
    };
    get_opts_from(files, file, options).await
}

async fn get_opts_from(
    files: &FileData,
    file: &str,
    options: GetOptions,
) -> object_store::Result<GetResult> {
    files.data.get(file);
    let path = PathBuf::from(files.get_file_path(file));
    let (metadata, fp) = match std::fs::File::open(&path).and_then(|f| Ok((f.metadata()?, f))) {
        Ok(opened) => opened,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if let Err(cleanup_error) = files.remove_index_if_missing(file) {
                log::error!(
                    "File disk cache failed to clear missing physical entry {file}: {cleanup_error}"
                );
                return Err(object_store::Error::Generic {
                    store: "disk cache",
                    source: Box::new(std::io::Error::other(cleanup_error.to_string())),
                });
            }
            return Err(object_store::Error::NotFound {
                path: file.to_string(),
                source: Box::new(error),
            });
        }
        Err(error) => {
            return Err(object_store::Error::NotFound {
                path: file.to_string(),
                source: Box::new(error),
            });
        }
    };

    let generation = FileGeneration::from_metadata(&metadata);
    let last_modified = last_modified(&metadata);
    let meta = ObjectMeta {
        location: file.into(),
        last_modified,
        size: generation.size,
        e_tag: Some(generation.e_tag.clone()),
        version: None,
    };
    options.check_preconditions(&meta)?;

    let range = match resolve_get_range(file, options.range.as_ref(), meta.size) {
        Ok(range) => range,
        Err(error)
            if options
                .range
                .as_ref()
                .is_some_and(|range| is_start_too_large(range, meta.size)) =>
        {
            log::warn!(
                "File disk cache entry is stale for {file}: requested {:?}, cached size {}; \
                 evicting and falling back to object storage",
                options.range,
                meta.size
            );
            match files.remove(file, Some(&generation)) {
                Ok(true) => {}
                Ok(false) => {
                    log::debug!(
                        "File disk cache preserved a newer generation of stale entry {file}"
                    );
                }
                Err(remove_error) => {
                    log::error!(
                        "File disk cache failed to evict stale generation {file}: requested {:?}, \
                         cached size {}: {remove_error}",
                        options.range,
                        meta.size
                    );
                    return Err(object_store::Error::Generic {
                        store: "disk cache",
                        source: Box::new(std::io::Error::other(format!(
                            "failed to evict {file} after requested range {:?} exceeded cached \
                             size {}: {remove_error}",
                            options.range, meta.size
                        ))),
                    });
                }
            }
            return Err(error);
        }
        Err(error) => return Err(error),
    };

    Ok(GetResult {
        payload: GetResultPayload::File(fp, path),
        attributes: Default::default(),
        range,
        meta,
    })
}

#[inline]
pub async fn get(file: &str, range: Option<Range<u64>>) -> Option<Bytes> {
    let files = get_file_reader(file)?;
    files.get(file, range).await
}

#[inline]
pub async fn get_size(file: &str) -> Option<usize> {
    let files = get_file_reader(file)?;
    files.get_size(file).await
}

/// Batched range read against the disk cache: one `File::open` followed
/// by N `pread`s, all inside one `block_in_place`. Returns one `Bytes`
/// per input range, in input order. This is the hot path for the search
/// side's `.bf` block fetches — without it each range turns into a
/// separate `File::open` and async task spawn, which dominates wall
/// clock for queries touching many buckets.
///
/// Returns `Err(NotFound)` when the file isn't in the disk cache; the
/// caller falls back to remote storage.
#[cfg(unix)]
pub async fn get_ranges(file: &str, ranges: &[Range<u64>]) -> object_store::Result<Vec<Bytes>> {
    let Some(files) = get_file_reader(file) else {
        return Err(object_store::Error::NotFound {
            path: file.to_string(),
            source: Box::new(std::io::Error::other("file not found")),
        });
    };
    get_ranges_from(files, file, ranges).await
}

#[cfg(unix)]
async fn get_ranges_from(
    files: &FileData,
    file: &str,
    ranges: &[Range<u64>],
) -> object_store::Result<Vec<Bytes>> {
    files.data.get(file);
    let path = PathBuf::from(files.get_file_path(file));
    let ranges_owned: Vec<Range<u64>> = ranges.to_vec();
    let file_label = file.to_string();

    tokio::task::block_in_place(move || -> object_store::Result<Vec<Bytes>> {
        let f = std::fs::File::open(&path).map_err(|e| object_store::Error::NotFound {
            path: file_label.clone(),
            source: Box::new(e),
        })?;
        let mut out = Vec::with_capacity(ranges_owned.len());
        for r in &ranges_owned {
            if r.start > r.end {
                return Err(crate::storage::Error::BadRange(file_label.clone()).into());
            }
            let len = (r.end - r.start) as usize;
            let buf =
                super::pread_vec(&f, r.start, len).map_err(|e| object_store::Error::Generic {
                    store: "DiskCache",
                    source: Box::new(e),
                })?;
            out.push(Bytes::from(buf));
        }
        Ok(out)
    })
}

#[cfg(not(unix))]
pub async fn get_ranges(file: &str, ranges: &[Range<u64>]) -> object_store::Result<Vec<Bytes>> {
    // Non-unix fallback: serial `get` calls. Still reuses the same on-disk file
    // entry but pays per-range open overhead. Acceptable — the hot path is unix.
    let mut out = Vec::with_capacity(ranges.len());
    for r in ranges {
        let bytes =
            get(file, Some(r.clone()))
                .await
                .ok_or_else(|| object_store::Error::NotFound {
                    path: file.to_string(),
                    source: Box::new(std::io::Error::other("file not found")),
                })?;
        out.push(bytes);
    }
    Ok(out)
}

#[inline]
pub fn get_file_path(file: &str) -> Option<String> {
    let files = get_file_reader(file)?;
    Some(files.get_file_path(file))
}

#[inline]
pub async fn exist(file: &str) -> bool {
    if !get_config().disk_cache.enabled {
        return false;
    }
    let start = std::time::Instant::now();
    let files = cache_for_key(file);
    if !files.exist(file).await {
        return false;
    }

    let exist_took = start.elapsed().as_millis() as usize;
    if exist_took > 100 {
        log::info!("disk->cache: check file {file} exist took: {exist_took} ms");
    }

    // check if the file is really exist
    if get_size(file).await.is_some() {
        return true;
    }

    // file is not exist, need remove it from cache index
    if let Err(error) = clear_index_if_missing(file).await {
        log::error!("disk cache failed to clear missing index entry {file}: {error}");
    }
    let remove_took = start.elapsed().as_millis() as usize;
    if remove_took > 100 {
        log::info!("disk->cache: check file {file} exist remove took: {remove_took} ms");
    }

    // finally return false
    false
}

#[inline]
pub async fn set(file: &str, data: Bytes) -> Result<(), anyhow::Error> {
    if !get_config().disk_cache.enabled {
        return Ok(());
    }

    // write to tmp file
    let data_size = data.len();
    let (file, tmp_file) = write_tmp_file(file, data).await?;
    set_from_tmp_file(&file, tmp_file, data_size).await
}

/// Seed the disk cache from a LOCAL file (e.g. a compaction spool) without
/// transiting RAM (H3). Prefer a hard link so a compaction output on the same
/// filesystem is promoted without another full-file read/write pass; fall
/// back to a file copy across devices or on filesystems that reject links.
/// The cache's rename-into-place tail consumes only the temporary link/copy,
/// never the caller-owned source spool.
pub async fn set_from_local_file(
    file: &str,
    source: &std::path::Path,
) -> Result<(), anyhow::Error> {
    if !get_config().disk_cache.enabled {
        return Ok(());
    }
    let tmp_file = alloc_tmp_file_path().await?;
    let size = match tokio::fs::hard_link(source, tmp_file.path()).await {
        Ok(()) => match tokio::fs::metadata(source).await {
            Ok(metadata) => metadata.len() as usize,
            Err(error) => {
                let _ = tokio::fs::remove_file(tmp_file.path()).await;
                return Err(anyhow::anyhow!(
                    "[FileData::Disk] stat hard-linked local file {} failed; removed temporary link {}: {error}",
                    source.display(),
                    tmp_file.path()
                ));
            }
        },
        Err(link_error) => match tokio::fs::copy(source, tmp_file.path()).await {
            Ok(size) => {
                log::debug!(
                    "[FileData::Disk] hard-link promotion of {} failed ({link_error}); copied {} bytes instead",
                    source.display(),
                    size,
                );
                size as usize
            }
            Err(copy_error) => {
                let _ = tokio::fs::remove_file(tmp_file.path()).await;
                return Err(anyhow::anyhow!(
                    "[FileData::Disk] promote local file {} to cache tmp {} failed: hard link: {link_error}; copy fallback: {copy_error}",
                    source.display(),
                    tmp_file.path()
                ));
            }
        },
    };
    set_from_tmp_file(file, tmp_file, size).await
}

/// Move an already-written tmp file into the disk cache under `file` — the
/// tail of [`set`], and the direct entry point for the H3 streamed download
/// (the object body was streamed into `tmp_file`; nothing here re-reads it).
/// The tmp file is consumed: renamed into the cache, or removed when the
/// entry already exists / the cache is disabled.
async fn set_from_tmp_file(
    file: &str,
    tmp_file: TempFileGuard,
    data_size: usize,
) -> Result<(), anyhow::Error> {
    if !get_config().disk_cache.enabled {
        // Guard cleanup reports failures without changing disabled-cache success.
        drop(tmp_file);
        return Ok(());
    }
    let start = std::time::Instant::now();
    let published = cache_for_key(file)
        .set(file, tmp_file.path(), data_size)
        .await?;
    if start.elapsed().as_millis() > 100 {
        log::info!(
            "disk->cache: set file {file} took: {} ms",
            start.elapsed().as_millis()
        );
    }
    if published
        && file.starts_with("files")
        && let Some(blocks) = super::block::global()
    {
        // The whole file now serves every range; its block entry is redundant.
        blocks.remove(file);
    }
    Ok(())
}

#[inline]
pub async fn remove(file: &str) -> Result<(), anyhow::Error> {
    if !get_config().disk_cache.enabled {
        return Ok(());
    }
    cache_for_key(file).remove(file, None).map(|_| ())
}

async fn load(root_dir: &Path, caches: [&Arc<FileData>; 3]) -> Result<(), anyhow::Error> {
    let mut scan_dirs = vec![root_dir.to_path_buf()];
    let mut loaded = Vec::new();
    let mut metrics_cache = Vec::new();
    while let Some(scan_dir) = scan_dirs.pop() {
        let mut entries = tokio::fs::read_dir(&scan_dir).await.map_err(|error| {
            anyhow::anyhow!(
                "read disk cache directory {} failed: {error}",
                scan_dir.display()
            )
        })?;
        while let Some(entry) = entries.next_entry().await? {
            let path = match tokio::fs::canonicalize(entry.path()).await {
                Ok(path) => path,
                Err(error) => {
                    log::error!(
                        "canonicalize cache path {} failed: {error}",
                        entry.path().display()
                    );
                    continue;
                }
            };
            let metadata = match entry.metadata().await {
                Ok(metadata) => metadata,
                Err(error) => {
                    log::error!("stat cache path {} failed: {error}", path.display());
                    continue;
                }
            };
            if metadata.is_dir() {
                scan_dirs.push(path);
                continue;
            }
            if path.extension().is_some_and(|ext| ext == "tmp")
                || path.to_string_lossy().ends_with("_tmp.arrow")
            {
                if let Err(error) = tokio::fs::remove_file(&path).await {
                    log::warn!(
                        "remove cache temporary file {} failed: {error}",
                        path.display()
                    );
                }
                continue;
            }
            let relative = path.strip_prefix(root_dir).map_err(|error| {
                anyhow::anyhow!(
                    "cache file {} is outside {}: {error}",
                    path.display(),
                    root_dir.display()
                )
            })?;
            let mut key = relative.to_string_lossy().replace('\\', "/");
            if !caches[0].multi_dir.is_empty() {
                key = key.split('/').skip(1).collect::<Vec<_>>().join("/");
            }
            let index = if key.starts_with("files") {
                0
            } else if key.starts_with("aggregations") {
                2
            } else {
                1
            };
            if caches[index].data.contains_key(&key) {
                continue;
            }
            let total = LOADING_FROM_DISK_NUM.fetch_add(1, Ordering::Relaxed);
            if total.is_multiple_of(1000) {
                log::info!("Loading disk cache {total}");
            }
            let size = metadata.len() as usize;
            if key.starts_with("metrics_results") {
                // Historical startup behavior: metrics results are readable by path,
                // but only their separate metadata list is populated.
                update_metrics(&key, size as i64, 0);
                metrics_cache.push(key);
                continue;
            }
            if (key.starts_with("results") && parse_result_cache_key(&key).is_none())
                || (key.starts_with("aggregations") && parse_aggregation_cache_key(&key).is_none())
            {
                log::error!("parse disk cache key failed: {key}");
                continue;
            }
            if !key.starts_with("files")
                && !key.starts_with("results")
                && !key.starts_with("aggregations")
            {
                continue;
            }
            let modified = metadata.modified().unwrap_or_else(|error| {
                log::warn!(
                    "read cache mtime {} failed: {error}; treating it as oldest",
                    path.display()
                );
                SystemTime::UNIX_EPOCH
            });
            loaded.push((key, size, modified, index));
        }
    }
    loaded.sort_by_key(|(_, _, modified, _)| *modified);
    METRICS_RESULT_CACHE.write().await.extend(metrics_cache);
    for (key, size, _, index) in loaded {
        let files = caches[index];
        // Acquire metadata before the stripe, matching the deleter's lock order.
        // Metadata and index publication cannot race with eviction cleanup, and
        // skipped/oversized files never leave a query metadata entry behind.
        let mut queries = if key.starts_with("results") {
            Some(QUERY_RESULT_CACHE.write().await)
        } else {
            None
        };
        {
            let _stripe = files.lock_key(&key);
            if files.data.contains_key(&key) {
                continue;
            }
            let path = files.get_file_path(&key);
            // A concurrent explicit remove may have deleted the scanned generation.
            let metadata = match std::fs::metadata(&path) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => {
                    log::error!("stat loaded cache file {path} failed: {error}");
                    continue;
                }
            };
            let size = if metadata.len() as usize == size {
                size
            } else {
                metadata.len() as usize
            };
            if size > files.max_size || files.max_size < 1024 {
                log::debug!(
                    "[CacheType:{}] bypass loaded disk cache entry {key}: {size} bytes cannot fit capacity {} bytes in KiB weights",
                    files.file_type,
                    files.max_size
                );
                // One undeletable file must not abort indexing of every other file.
                if let Err(error) = unlink_tmp(&path) {
                    log::error!(
                        "[CacheType:{}] remove oversized loaded cache file failed: {error}",
                        files.file_type
                    );
                }
                continue;
            }
            files.insert(&key, size);
            if let Some(queries) = &mut queries
                && let Some((_, _, query_key, meta)) = parse_result_cache_key(&key)
            {
                queries.entry(query_key).or_default().push(meta);
            }
            if key.starts_with("aggregations") {
                // Keep the existing startup aggregation metric label behavior.
                let columns: Vec<_> = key.split('/').collect();
                metrics::QUERY_DISK_CACHE_USED_BYTES
                    .with_label_values(&[columns[1], columns[2]])
                    .add(size as i64);
            }
        }
        drop(queries);
        files.enforce_capacity();
    }
    for files in caches {
        files.data.run_pending_tasks();
    }
    Ok(())
}

#[inline]
pub async fn stats(file_type: FileType) -> (usize, usize, usize) {
    let files = match file_type {
        FileType::Data => &FILES,
        FileType::Result => &RESULT_FILES,
        FileType::Aggregation => &AGGREGATION_FILES,
    };
    let (capacity, used) = files.size();
    (capacity, used, files.len())
}

#[inline]
pub async fn is_empty(file_type: FileType) -> bool {
    let files = match file_type {
        FileType::Data => &FILES,
        FileType::Result => &RESULT_FILES,
        FileType::Aggregation => &AGGREGATION_FILES,
    };
    files.is_empty()
}

#[inline]
pub async fn get_dir() -> String {
    FILES.root_dir.clone()
}

/// Fill the disk cache from object storage. H3: the object body STREAMS
/// into the cache's tmp file in bounded chunks and the finished tmp file is
/// renamed into the cache — the object never resides in RAM whole (the
/// pre-H3 `res.bytes()` buffering at compaction fan-out is what OOM'd the
/// 24Gi compactors on 2026-08-17).
pub async fn download(
    account: &str,
    file: &str,
    size: Option<usize>,
) -> Result<usize, anyhow::Error> {
    let tmp_file = alloc_tmp_file_path().await?;
    let data_len = super::download_from_storage_to_file(
        account,
        file,
        size,
        std::path::Path::new(tmp_file.path()),
    )
    .await?;
    set_from_tmp_file(file, tmp_file, data_len)
        .await
        .map_err(|e| anyhow::anyhow!("set file {file} to disk cache failed: {e}"))?;
    Ok(data_len)
}

/// Stream a background object under an exact registered-byte reservation.
/// Header mismatches and body overruns fail before publication; the temporary
/// path remains owned through every await, including cache publication.
pub(crate) async fn download_exact(
    account: &str,
    file: &str,
    expected_size: usize,
    lifetime: std::sync::Arc<dyn Send + Sync>,
) -> Result<usize, anyhow::Error> {
    let tmp_file = alloc_tmp_file_path().await?;
    let data_len = super::download_from_storage_to_file_exact(
        account,
        file,
        expected_size,
        std::path::Path::new(tmp_file.path()),
        std::sync::Arc::new((lifetime.clone(), tmp_file.clone())),
    )
    .await?;
    let publish_started = std::time::Instant::now();
    let published = set_from_tmp_file(file, tmp_file, data_len).await;
    super::download_io::Settings::configured()
        .metrics
        .time("publish", publish_started.elapsed());
    published.map_err(|e| anyhow::anyhow!("set file {file} to disk cache failed: {e}"))?;
    drop(lifetime);
    Ok(data_len)
}

/// Publish an admitted peer response without detaching uncharged disk writes.
pub(crate) async fn set_admitted(
    file: &str,
    data: Bytes,
    lifetime: std::sync::Arc<dyn Send + Sync>,
) -> Result<(), anyhow::Error> {
    let tmp_file = alloc_tmp_file_path().await?;
    let data_len = data.len();
    let mut sink = super::DownloadSink::exact_file(
        PathBuf::from(tmp_file.path()),
        std::sync::Arc::new((lifetime.clone(), tmp_file.clone())),
    );
    if let super::DownloadSink::PipelinedFile { writer, .. } = &mut sink {
        writer.set_size(data_len as u64);
    }
    sink.reset().await?;
    sink.write_chunk(data).await?;
    sink.finish_attempt().await?;
    drop(sink);
    set_from_tmp_file(file, tmp_file, data_len).await
}

// parse the result cache key from the file name
// returns (org_id, stream_type, query_key, ResultCacheMeta)
// results/default/logs/default/16042959487540176184_30_zo_sql_key/
// 1744081170000000_1744081170000000_1_0.json
pub fn parse_result_cache_key(file: &str) -> Option<(String, String, String, ResultCacheMeta)> {
    let columns = file.split('/').collect::<Vec<&str>>();
    if columns.len() < 6 {
        return None;
    }
    let org_id = columns[1].to_string();
    let stream_type = columns[2].to_string();
    let query_key = format!(
        "{}_{}_{}_{}",
        columns[1], columns[2], columns[3], columns[4]
    );

    let meta = columns[5].split('_').collect::<Vec<&str>>();
    let is_aggregate = meta[2] == "1";
    let is_descending = meta[3] == "1";
    let meta = ResultCacheMeta {
        start_time: meta[0].parse().unwrap(),
        end_time: meta[1].parse().unwrap(),
        is_aggregate,
        is_descending,
    };

    Some((org_id, stream_type, query_key, meta))
}

// parse the aggregation cache key from the file name
// returns (org_id, stream_type, query_key, ResultCacheMeta)
// aggregations/default/logs/default/16042959487540176184/1744081170000000_1744081170000000.arrow
pub fn parse_aggregation_cache_key(
    file: &str,
) -> Option<(String, String, String, ResultCacheMeta)> {
    let columns = file.split('/').collect::<Vec<&str>>();
    if columns.len() < 6 {
        return None;
    }
    let org_id = columns[1].to_string();
    let stream_type = columns[2].to_string();
    let query_key = format!(
        "{}_{}_{}_{}",
        columns[1], columns[2], columns[3], columns[4]
    );

    // Remove file extension before parsing
    let filename = columns[5];
    let filename_without_ext = if let Some(dot_pos) = filename.rfind('.') {
        &filename[..dot_pos]
    } else {
        filename
    };

    let meta = filename_without_ext.split('_').collect::<Vec<&str>>();
    if meta.len() < 2 {
        return None;
    }

    let meta = ResultCacheMeta {
        start_time: meta[0].parse().unwrap(),
        end_time: meta[1].parse().unwrap(),
        is_aggregate: true,
        // NOTE: aggregate record batches don't honor order by
        is_descending: false,
    };
    Some((org_id, stream_type, query_key, meta))
}

fn last_modified(metadata: &std::fs::Metadata) -> chrono::DateTime<chrono::Utc> {
    metadata
        .modified()
        .expect("Modified file time should be supported on this platform")
        .into()
}

fn get_etag(metadata: &std::fs::Metadata) -> String {
    let size = metadata.len();
    let mtime = metadata
        .modified()
        .ok()
        .and_then(|mtime| mtime.duration_since(SystemTime::UNIX_EPOCH).ok())
        .unwrap_or_default()
        .as_micros();
    format!("{mtime:x}-{size:x}")
}

/// Allocate a unique tmp-file path under the cache's tmp dir (creating the
/// dir). Streamed downloads (H3) write into this path directly; the buffered
/// [`write_tmp_file`] fills it with in-memory bytes.
async fn alloc_tmp_file_path() -> Result<TempFileGuard, anyhow::Error> {
    #[cfg(not(test))]
    let root = get_config().common.data_tmp_dir.clone();
    #[cfg(test)]
    let root = TEST_TMP_ROOT.path().to_string_lossy().into_owned();
    let tmp_path = format!(
        "{}/{}",
        root,
        get_ymdh_from_micros(now_micros(), HourFormat::Real)
    );
    if let Err(e) = std::fs::create_dir_all(&tmp_path) {
        return Err(anyhow::anyhow!(
            "[FileData::Disk] create tmp dir {}, failed: {}",
            tmp_path,
            e
        ));
    }
    let tmp_path = tokio::fs::canonicalize(&tmp_path).await.unwrap();
    let tmp_file = tmp_path.join(format!("{}.tmp", config::ider::generate()));
    Ok(TempFileGuard::new(tmp_file.to_str().unwrap().to_string()))
}

// Write data to a temporary random file and return the file path
async fn write_tmp_file(file: &str, data: Bytes) -> Result<(String, TempFileGuard), anyhow::Error> {
    let tmp_file = alloc_tmp_file_path().await?;
    if let Err(e) = config::utils::async_file::put_file_contents(tmp_file.path(), &data).await {
        return Err(anyhow::anyhow!(
            "[FileData::Disk] write tmp file {}, failed: {}",
            tmp_file.path(),
            e
        ));
    }
    Ok((file.to_string(), tmp_file))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestCache {
        files: Arc<FileData>,
        root: tempfile::TempDir,
        queue: DeleteQueue,
    }

    impl std::ops::Deref for TestCache {
        type Target = FileData;
        fn deref(&self) -> &Self::Target {
            &self.files
        }
    }

    impl TestCache {
        fn new(file_type: FileType, capacity: usize, start: bool, multi_dir: Vec<String>) -> Self {
            let root = tempfile::tempdir().unwrap();
            let queue = DeleteQueue::new();
            let files = FileData::with_capacity(
                file_type,
                capacity,
                format!("{}/", root.path().display()),
                multi_dir,
                queue.sender.clone(),
            );
            if start {
                queue.start();
            }
            Self { files, root, queue }
        }

        async fn put(&self, key: &str, bytes: &[u8]) {
            let tmp = tempfile::NamedTempFile::new_in(self.root.path()).unwrap();
            std::fs::write(tmp.path(), bytes).unwrap();
            self.files
                .set(key, tmp.path().to_str().unwrap(), bytes.len())
                .await
                .unwrap();
        }

        async fn quiesce(&self) {
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                loop {
                    self.data.run_pending_tasks();
                    if self.pending_deletes.load(Ordering::Relaxed) == 0 {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("disk cache eviction deleter did not quiesce");
        }

        fn reconcile(&self) {
            let entries: Vec<_> = self.data.iter().collect();
            let indexed_bytes: usize = entries.iter().map(|(_, entry)| entry.size).sum();
            let mut disk_bytes = 0;
            let mut disk_files = 0;
            let mut dirs = vec![self.root.path().to_path_buf()];
            while let Some(dir) = dirs.pop() {
                for entry in std::fs::read_dir(dir).unwrap() {
                    let path = entry.unwrap().path();
                    if path.is_dir() {
                        dirs.push(path);
                    } else {
                        disk_bytes += path.metadata().unwrap().len() as usize;
                        disk_files += 1;
                    }
                }
            }
            assert_eq!(
                self.size().1,
                indexed_bytes,
                "exact counter must equal index"
            );
            assert_eq!(indexed_bytes, disk_bytes, "eviction must not strand files");
            assert_eq!(self.len(), entries.len());
            assert_eq!(
                entries.len(),
                disk_files,
                "zero-byte orphan files must also be detected"
            );
            assert!(indexed_bytes <= self.max_size);
            for (key, entry) in entries {
                assert_eq!(
                    std::fs::metadata(self.get_file_path(&key)).unwrap().len() as usize,
                    entry.size
                );
            }
        }
    }

    fn test_cache(file_type: FileType, capacity: usize, _strategy: &str) -> TestCache {
        TestCache::new(
            file_type,
            capacity,
            tokio::runtime::Handle::try_current().is_ok(),
            Vec::new(),
        )
    }

    fn unique_disk_range_test_key(name: &str) -> String {
        format!(
            "aggregations/default/traces/disk-range-tests/{name}-{}.vxi",
            config::ider::generate()
        )
    }

    #[tokio::test]
    async fn cancelling_exact_stream_removes_the_owned_temporary_file() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("cancelled.tmp");
        let worker_path = path.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let worker = tokio::spawn(async move {
            let tmp = TempFileGuard::new(worker_path.to_string_lossy().into_owned());
            let mut sink =
                super::super::DownloadSink::exact_file(worker_path, std::sync::Arc::new(tmp));
            let started_tx = parking_lot::Mutex::new(Some(started_tx));
            super::super::download_with_retries_policy(
                || {
                    let started_tx = started_tx.lock().take();
                    async move {
                        let body = futures::stream::once(async move {
                            // Polled only after the sink has created its temp file.
                            started_tx.unwrap().send(()).unwrap();
                            std::future::pending::<object_store::Result<Bytes>>().await
                        });
                        Ok(object_store::GetResult {
                            payload: object_store::GetResultPayload::Stream(Box::pin(body)),
                            meta: object_store::ObjectMeta {
                                location: object_store::path::Path::from("cancelled.vxi"),
                                last_modified: chrono::Utc::now(),
                                size: 10,
                                e_tag: None,
                                version: None,
                            },
                            range: 0..10,
                            attributes: Default::default(),
                        })
                    }
                },
                "cancelled.vxi",
                Some(10),
                &mut sink,
                super::super::ExpectedSizePolicy::Exact,
            )
            .await
        });
        started_rx.await.unwrap();
        assert!(path.exists());
        worker.abort();
        assert!(worker.await.unwrap_err().is_cancelled());
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn oversized_public_get_evicts_a_short_global_disk_entry() {
        let file = unique_disk_range_test_key("stale");
        set(&file, Bytes::from_static(b"short")).await.unwrap();
        let path = get_file_path(&file).unwrap();
        assert!(exist(&file).await);
        assert!(Path::new(&path).is_file());

        let error = match get_opts(
            &file,
            GetOptions {
                range: Some(GetRange::Bounded(8..12)),
                ..Default::default()
            },
        )
        .await
        {
            Ok(_) => panic!("oversized stale-cache range unexpectedly succeeded"),
            Err(error) => error,
        };

        assert!(matches!(&error, object_store::Error::NotFound { .. }));
        assert!(
            error.to_string().contains("starting at 8")
                && error.to_string().contains("only 5 bytes"),
            "{error}"
        );
        assert!(!exist(&file).await);
        assert!(!Path::new(&path).exists());
    }

    #[tokio::test]
    async fn inconsistent_range_does_not_evict_the_disk_entry() {
        let file = unique_disk_range_test_key("inconsistent");
        set(&file, Bytes::from_static(b"short")).await.unwrap();
        let path = get_file_path(&file).unwrap();

        let error = match get_opts(
            &file,
            GetOptions {
                range: Some(GetRange::Bounded(8..8)),
                ..Default::default()
            },
        )
        .await
        {
            Ok(_) => panic!("inconsistent range unexpectedly succeeded"),
            Err(error) => error,
        };

        assert!(matches!(&error, object_store::Error::Precondition { .. }));
        assert!(exist(&file).await);
        assert!(Path::new(&path).is_file());
        remove(&file).await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stale_readers_cannot_remove_a_same_size_newer_generation() {
        let file = unique_disk_range_test_key("generation");
        set(&file, Bytes::from_static(b"old")).await.unwrap();
        let path = get_file_path(&file).unwrap();
        let first_reader_file = std::fs::File::open(&path).unwrap();
        let first_metadata = first_reader_file.metadata().unwrap();
        let old_modified = first_metadata.modified().unwrap();
        let first_stale_reader = FileGeneration::from_metadata(&first_metadata);
        let second_reader_file = std::fs::File::open(&path).unwrap();
        let second_stale_reader =
            FileGeneration::from_metadata(&second_reader_file.metadata().unwrap());

        assert!(remove_generation(&file, &first_stale_reader).await.unwrap());
        drop(first_reader_file);
        set(&file, Bytes::from_static(b"new")).await.unwrap();
        std::fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(old_modified))
            .unwrap();
        let new_generation = FileGeneration::from_metadata(&std::fs::metadata(&path).unwrap());
        assert_eq!(first_stale_reader.size, new_generation.size);
        assert_eq!(first_stale_reader.e_tag, new_generation.e_tag);
        assert_ne!(first_stale_reader.inode, new_generation.inode);

        assert!(
            !remove_generation(&file, &second_stale_reader)
                .await
                .unwrap()
        );
        drop(second_reader_file);
        assert!(exist(&file).await);
        assert_eq!(get(&file, None).await.unwrap(), Bytes::from_static(b"new"));
        remove(&file).await.unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn unlink_failure_retains_index_accounting_for_retry() {
        let files = test_cache(FileType::Data, 1024, "lru");
        let file = "files/default/traces/unlink-failure.vxi";
        let path = files.get_file_path(file);
        std::fs::create_dir_all(&path).unwrap();
        files.insert(file, 17);
        let generation = FileGeneration::from_metadata(&std::fs::metadata(&path).unwrap());

        let error = files.remove(file, Some(&generation)).unwrap_err();
        assert!(error.to_string().contains("remove file"), "{error}");
        assert!(files.data.contains_key(file));
        assert_eq!(files.size().1, 17);

        std::fs::remove_dir(&path).unwrap();
        assert!(files.remove_index_if_missing(file).unwrap());
        assert!(!files.data.contains_key(file));
        files.data.run_pending_tasks();
        assert_eq!(files.size().1, 0);
    }

    #[test]
    fn disk_range_still_clamps_an_end_beyond_the_cached_file() {
        assert_eq!(
            resolve_get_range(
                "files/default/traces/default/example.vxi",
                Some(&GetRange::Bounded(4..20)),
                10,
            )
            .unwrap(),
            4..10
        );
    }

    #[tokio::test]
    async fn abandoned_temporary_file_is_removed_on_drop() {
        let file_key = "wal_segments/test/cancelled-cache-fill.seg";
        let (_, tmp_file) = write_tmp_file(file_key, Bytes::from_static(b"segment"))
            .await
            .unwrap();
        let path = tmp_file.path().to_string();
        assert!(Path::new(&path).exists());

        drop(tmp_file);
        assert!(!Path::new(&path).exists());
    }

    #[tokio::test]
    async fn test_disk_lru_cache_set_file() {
        let file_data = test_cache(FileType::Data, 1024, "lru");
        let content = Bytes::from("Some text Need to store in cache");
        let data_size = content.len();
        for i in 0..50 {
            let file_key = format!(
                "files/default/logs/disk/2022/10/03/10/6982652937134804993_1_{}.parquet",
                i
            );
            let (_file_key, tmp_file) = write_tmp_file(&file_key, content.clone()).await.unwrap();
            let resp = file_data.set(&file_key, &tmp_file, data_size).await;
            assert!(resp.is_ok());
        }
        file_data.quiesce().await;
        file_data.reconcile();
    }

    #[tokio::test]
    async fn test_disk_lru_cache_get_file() {
        let file_data = test_cache(FileType::Data, 64 * 1024, "lru");
        let file_key = "files/default/logs/disk/2022/10/03/10/6982652937134804993_2_1.parquet";
        let content = Bytes::from("Some text");
        let data_size = content.len();
        let (file_key, tmp_file) = write_tmp_file(&file_key, content.clone()).await.unwrap();

        file_data
            .set(&file_key, &tmp_file, data_size)
            .await
            .unwrap();
        assert!(file_data.exist(&file_key).await);

        let (file_key, tmp_file) = write_tmp_file(&file_key, content.clone()).await.unwrap();
        file_data
            .set(&file_key, &tmp_file, data_size)
            .await
            .unwrap();
        assert!(file_data.exist(&file_key).await);
        assert!(file_data.size().0 > 0);
    }

    #[tokio::test]
    async fn test_disk_lru_cache_miss() {
        let file_data = test_cache(FileType::Data, 1024, "lru");
        let file_key1 = "files/default/logs/disk/2022/10/03/10/6982652937134804993_3_1.parquet";
        let file_key2 = "files/default/logs/disk/2022/10/03/10/6982652937134804993_3_2.parquet";
        let content = Bytes::from(vec![42; 1024]);
        let data_size = content.len();
        let (file_key1, tmp_file) = write_tmp_file(&file_key1, content.clone()).await.unwrap();
        // set one key
        file_data
            .set(&file_key1, &tmp_file, data_size)
            .await
            .unwrap();
        assert!(file_data.exist(&file_key1).await);
        // set another key, will release first key
        let (file_key2, tmp_file) = write_tmp_file(&file_key2, content.clone()).await.unwrap();
        file_data
            .set(&file_key2, &tmp_file, data_size)
            .await
            .unwrap();
        assert!(file_data.exist(&file_key2).await);
        // get first key, should get error
        assert!(!file_data.exist(&file_key1).await);
        file_data.quiesce().await;
        file_data.reconcile();
    }

    #[tokio::test]
    async fn test_disk_fifo_cache_set_file() {
        let file_data = test_cache(FileType::Data, 1024, "fifo");
        let content = Bytes::from("Some text Need to store in cache");
        let data_size = content.len();
        for i in 0..50 {
            let file_key = format!(
                "files/default/logs/disk/2022/10/03/10/6982652937134804993_4_{}.parquet",
                i
            );
            let (_file_key, tmp_file) = write_tmp_file(&file_key, content.clone()).await.unwrap();
            let resp = file_data.set(&file_key, &tmp_file, data_size).await;
            if let Some(e) = resp.as_ref().err() {
                println!("set file_key: {} error: {:?}", file_key, e);
            }
            assert!(resp.is_ok());
        }
        file_data.quiesce().await;
        file_data.reconcile();
    }

    #[tokio::test]
    async fn test_disk_fifo_cache_get_file() {
        let file_data = test_cache(FileType::Data, 64 * 1024, "fifo");
        let file_key = "files/default/logs/disk/2022/10/03/10/6982652937134804993_5_1.parquet";
        let content = Bytes::from("Some text");
        let data_size = content.len();
        let (file_key, tmp_file) = write_tmp_file(&file_key, content.clone()).await.unwrap();

        file_data
            .set(&file_key, &tmp_file, data_size)
            .await
            .unwrap();
        assert!(file_data.exist(&file_key).await);

        let (file_key, tmp_file) = write_tmp_file(&file_key, content.clone()).await.unwrap();
        file_data
            .set(&file_key, &tmp_file, data_size)
            .await
            .unwrap();
        assert!(file_data.exist(&file_key).await);
        assert!(file_data.size().0 > 0);
    }

    #[tokio::test]
    async fn test_disk_fifo_cache_miss() {
        let file_data = test_cache(FileType::Data, 1024, "fifo");
        let file_key1 = "files/default/logs/disk/2022/10/03/10/6982652937134804993_6_1.parquet";
        let file_key2 = "files/default/logs/disk/2022/10/03/10/6982652937134804993_6_2.parquet";
        let content = Bytes::from(vec![42; 1024]);
        let data_size = content.len();
        let (file_key1, tmp_file) = write_tmp_file(&file_key1, content.clone()).await.unwrap();
        // set one key
        file_data
            .set(&file_key1, &tmp_file, data_size)
            .await
            .unwrap();
        assert!(file_data.exist(&file_key1).await);
        // set another key, will release first key
        let (file_key2, tmp_file) = write_tmp_file(&file_key2, content.clone()).await.unwrap();
        file_data
            .set(&file_key2, &tmp_file, data_size)
            .await
            .unwrap();
        assert!(file_data.exist(&file_key2).await);
        // get first key, should get error
        assert!(!file_data.exist(&file_key1).await);
        file_data.quiesce().await;
        file_data.reconcile();
    }

    #[tokio::test]
    async fn test_disk_multi_dir() {
        let multi_dir: Vec<String> = "dir1 , dir2 , dir3"
            .split(',')
            .filter(|s| !s.trim().is_empty())
            .map(|s| s.trim().to_string())
            .collect();

        let file_data = TestCache::new(FileType::Data, 64 * 1024, true, multi_dir);
        let file_key = "files/default/logs/disk/2022/10/03/10/6982652937134804993_7_1.parquet";
        let content = Bytes::from("Some text");
        let data_size = content.len();
        let (file_key, tmp_file) = write_tmp_file(&file_key, content.clone()).await.unwrap();

        file_data
            .set(&file_key, &tmp_file, data_size)
            .await
            .unwrap();
        assert!(file_data.exist(&file_key).await);

        let (file_key, tmp_file) = write_tmp_file(&file_key, content.clone()).await.unwrap();
        file_data
            .set(&file_key, &tmp_file, data_size)
            .await
            .unwrap();
        assert!(file_data.exist(&file_key).await);
        assert!(file_data.size().0 > 0);

        assert_eq!(file_data.get(&file_key, None).await, Some(content))
    }

    #[tokio::test]
    async fn test_disk_parse_result_cache_key() {
        let file_key = "results/default/logs/default/16042959487540176184_30_zo_sql_key/1744081170000000_1744081170000000_1_0.json";
        let Some((org_id, stream_type, query_key, meta)) = parse_result_cache_key(file_key) else {
            panic!("parse result cache key error");
        };
        assert_eq!(org_id, "default");
        assert_eq!(stream_type, "logs");
        assert_eq!(
            query_key,
            "default_logs_default_16042959487540176184_30_zo_sql_key"
        );
        assert_eq!(meta.start_time, 1744081170000000);
        assert_eq!(meta.end_time, 1744081170000000);
        assert!(meta.is_aggregate);
        assert!(!meta.is_descending);
    }

    #[tokio::test]
    async fn test_parse_aggregation_cache_key() {
        let file_key = "aggregations/default/logs/default/16042959487540176184/1744081170000000_1744081170000000.arrow";
        let Some((org_id, stream_type, query_key, meta)) = parse_aggregation_cache_key(file_key)
        else {
            panic!("parse aggregation cache key error");
        };
        assert_eq!(org_id, "default");
        assert_eq!(stream_type, "logs");
        assert_eq!(query_key, "default_logs_default_16042959487540176184");
        assert_eq!(meta.start_time, 1744081170000000);
        assert_eq!(meta.end_time, 1744081170000000);
    }

    #[tokio::test]
    async fn test_disk_write_tmp_file() {
        let file_key = "files/default/logs/disk/2022/10/03/10/test_file.parquet";
        let test_data = Bytes::from("test content for temporary file");

        // Test successful write
        let result = write_tmp_file(file_key, test_data.clone()).await;
        assert!(result.is_ok());

        let (returned_file_key, tmp_file_path) = result.unwrap();
        assert_eq!(returned_file_key, file_key);

        // Verify the temporary file exists and contains the correct data
        let tmp_path = Path::new(&tmp_file_path);
        assert!(tmp_path.exists());
        assert!(tmp_path.is_file());

        // Read back the file content to verify it matches
        let file_content = std::fs::read(tmp_path).unwrap();
        assert_eq!(file_content, test_data);

        // Clean up
        let _ = std::fs::remove_file(tmp_path);
    }

    #[tokio::test]
    async fn test_disk_write_tmp_file_with_empty_data() {
        let file_key = "files/default/logs/disk/2022/10/03/10/empty_file.parquet";
        let empty_data = Bytes::new();

        let result = write_tmp_file(file_key, empty_data).await;
        assert!(result.is_ok());

        let (returned_file_key, tmp_file_path) = result.unwrap();
        assert_eq!(returned_file_key, file_key);

        // Verify the temporary file exists but is empty
        let tmp_path = Path::new(&tmp_file_path);
        assert!(tmp_path.exists());
        assert_eq!(tmp_path.metadata().unwrap().len(), 0);

        // Clean up
        let _ = std::fs::remove_file(tmp_path);
    }

    #[tokio::test]
    async fn test_disk_write_tmp_file_with_large_data() {
        let file_key = "files/default/logs/disk/2022/10/03/10/large_file.parquet";
        let large_data = Bytes::from(vec![b'a'; 1024 * 1024]); // 1MB of data

        let result = write_tmp_file(file_key, large_data.clone()).await;
        assert!(result.is_ok());

        let (returned_file_key, tmp_file_path) = result.unwrap();
        assert_eq!(returned_file_key, file_key);

        // Verify the temporary file exists and has correct size
        let tmp_path = Path::new(&tmp_file_path);
        assert!(tmp_path.exists());
        assert_eq!(tmp_path.metadata().unwrap().len(), 1024 * 1024);

        // Read back the file content to verify it matches
        let file_content = std::fs::read(tmp_path).unwrap();
        assert_eq!(file_content, large_data);

        // Clean up
        let _ = std::fs::remove_file(tmp_path);
    }

    #[tokio::test]
    async fn test_stats_data_file_type() {
        // Test that stats function returns correct tuple structure for Data type
        let (total_size, used_size, _item_len) = stats(FileType::Data).await;

        // used_size should not exceed total_size
        assert!(used_size <= total_size);
    }

    #[tokio::test]
    async fn test_stats_result_file_type() {
        // Test that stats function returns correct tuple structure for Result type
        let (total_size, used_size, _item_len) = stats(FileType::Result).await;

        // used_size should not exceed total_size
        assert!(used_size <= total_size);
    }

    #[tokio::test]
    async fn test_stats_aggregation_file_type() {
        // Test that stats function returns correct tuple structure for Aggregation type
        let (total_size, used_size, _item_len) = stats(FileType::Aggregation).await;

        // used_size should not exceed total_size
        assert!(used_size <= total_size);
    }

    #[tokio::test]
    async fn test_stats_with_data() {
        let cache = test_cache(FileType::Data, 4096, "lru");
        cache
            .put("files/stats/logs/value", b"Test content for stats function")
            .await;
        cache.quiesce().await;
        assert_eq!(
            cache.size(),
            (4096, b"Test content for stats function".len())
        );
        assert_eq!(cache.len(), 1);
        cache.reconcile();
    }

    #[tokio::test]
    async fn test_stats_aggregates_global_keys() {
        let cache = test_cache(FileType::Result, 4096, "lru");
        for i in 0..3 {
            cache
                .put(
                    &format!("results/stats/logs/stream/query{i}/1_2_0_0.json"),
                    &[1; 512],
                )
                .await;
        }
        cache.quiesce().await;
        assert_eq!(cache.size(), (4096, 1536));
        assert_eq!(cache.len(), 3);
        cache.reconcile();
    }

    #[tokio::test]
    async fn test_stats_empty_cache() {
        // Test stats on an empty cache type (using a fresh FileData)
        let file_data = test_cache(FileType::Data, 1024, "lru");

        let (max_size, cur_size) = file_data.size();
        let len = file_data.len();

        // For an empty cache:
        assert!(max_size > 0); // Should have capacity set
        assert_eq!(cur_size, 0); // Should have no data
        assert_eq!(len, 0); // Should have no items
    }

    #[tokio::test]
    async fn test_stats_consistency() {
        // Verify that stats returns valid values across multiple calls
        // Note: In a concurrent test environment, values may change due to other tests
        let (total1, used1, len1) = stats(FileType::Aggregation).await;
        let (total2, used2, len2) = stats(FileType::Aggregation).await;

        // Total size should remain consistent
        assert_eq!(total1, total2);

        // Used size and length should not decrease significantly (may change due to concurrent
        // tests) Allow some variance due to cache operations (GC, additions)
        let used_diff = if used2 > used1 {
            used2 - used1
        } else {
            used1 - used2
        };
        let len_diff = if len2 > len1 {
            len2 - len1
        } else {
            len1 - len2
        };
        assert!(used_diff < 10000 || used1 == 0 || used2 == 0); // Allow reasonable variance
        assert!(len_diff < 100 || len1 == 0 || len2 == 0); // Allow reasonable variance
    }

    #[test]
    fn test_parse_result_cache_key_valid() {
        let file =
            "results/myorg/logs/default/query_key/1744081170000000_1744081180000000_0_1.json";
        let result = parse_result_cache_key(file);
        assert!(result.is_some());
        let (org_id, stream_type, query_key, meta) = result.unwrap();
        assert_eq!(org_id, "myorg");
        assert_eq!(stream_type, "logs");
        assert_eq!(query_key, "myorg_logs_default_query_key");
        assert_eq!(meta.start_time, 1744081170000000);
        assert_eq!(meta.end_time, 1744081180000000);
        assert!(!meta.is_aggregate);
    }

    #[test]
    fn test_parse_result_cache_key_too_short_returns_none() {
        assert!(parse_result_cache_key("too/short").is_none());
        assert!(parse_result_cache_key("a/b/c/d/e").is_none());
    }

    #[test]
    fn test_parse_aggregation_cache_key_valid() {
        let file = "aggregations/myorg/metrics/default/16042959487540176184/1744081170000000_1744081180000000.arrow";
        let result = parse_aggregation_cache_key(file);
        assert!(result.is_some());
        let (org_id, stream_type, query_key, meta) = result.unwrap();
        assert_eq!(org_id, "myorg");
        assert_eq!(stream_type, "metrics");
        assert_eq!(query_key, "myorg_metrics_default_16042959487540176184");
        assert_eq!(meta.start_time, 1744081170000000);
        assert_eq!(meta.end_time, 1744081180000000);
        assert!(meta.is_aggregate);
        assert!(!meta.is_descending);
    }

    #[test]
    fn test_parse_aggregation_cache_key_too_short_returns_none() {
        assert!(parse_aggregation_cache_key("too/short").is_none());
    }

    #[test]
    fn test_file_type_display_data() {
        assert_eq!(FileType::Data.to_string(), "data");
    }

    #[test]
    fn test_file_type_display_result() {
        assert_eq!(FileType::Result.to_string(), "result");
    }

    #[test]
    fn test_file_type_display_aggregation() {
        assert_eq!(FileType::Aggregation.to_string(), "aggregation");
    }

    #[test]
    fn test_get_etag_format() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), b"hello").unwrap();
        let meta = std::fs::metadata(tmp.path()).unwrap();
        let etag = get_etag(&meta);
        // etag format: "{mtime_hex}-{size_hex}", size of "hello" is 5 = 0x5
        assert!(
            etag.ends_with("-5"),
            "etag should end with '-5', got {etag}"
        );
        assert!(etag.contains('-'));
    }

    #[test]
    fn test_last_modified_returns_datetime() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), b"test").unwrap();
        let meta = std::fs::metadata(tmp.path()).unwrap();
        let dt = last_modified(&meta);
        // should be a recent timestamp (after year 2020)
        assert!(dt.timestamp() > 1_577_836_800);
    }

    #[test]
    fn test_stripe_index_returns_valid_index() {
        let idx = stripe_index("some/file/path.parquet");
        assert!(idx < STRIPES);
    }

    #[test]
    fn test_stripe_index_empty_string() {
        let idx = stripe_index("");
        assert!(idx < STRIPES);
    }

    #[tokio::test]
    async fn global_capacity_does_not_partition_hot_keys() {
        let cache = test_cache(FileType::Data, 16 * 1024, "lru");
        // Deliberately collide in the old 16-bucket scheme. Global capacity must
        // admit all eight instead of evicting at the former one-KiB bucket limit.
        let keys: Vec<_> = (0..10000)
            .map(|i| format!("files/global/logs/test/{i}"))
            .filter(|key| gxhash::new().sum64(key) % 16 == 0)
            .take(8)
            .collect();
        for key in &keys {
            cache.put(key, &[1; 1024]).await;
        }
        cache.quiesce().await;
        for key in &keys {
            assert!(cache.exist(key).await);
        }
        for i in 0..48 {
            cache
                .put(&format!("files/global/logs/churn/{i}"), &[2; 1024])
                .await;
        }
        cache.quiesce().await;
        assert_eq!(cache.size().1, 16 * 1024);
        cache.reconcile();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn disk_reads_touch_lru_but_exist_does_not() {
        for read_kind in 0..3 {
            let cache = test_cache(FileType::Data, 3 * 1024, "time_lru");
            let keys = [
                "files/touch/logs/a",
                "files/touch/logs/b",
                "files/touch/logs/c",
                "files/touch/logs/d",
            ];
            for key in &keys[..3] {
                cache.put(key, &[1; 1024]).await;
            }
            cache.data.run_pending_tasks();
            if read_kind == 1 {
                assert_eq!(
                    get_opts_from(&cache, keys[0], GetOptions::default())
                        .await
                        .unwrap()
                        .bytes()
                        .await
                        .unwrap()
                        .len(),
                    1024
                );
            } else if read_kind == 2 {
                #[cfg(unix)]
                assert_eq!(
                    get_ranges_from(&cache, keys[0], &[0..64, 128..192])
                        .await
                        .unwrap(),
                    vec![Bytes::from(vec![1; 64]); 2]
                );
                #[cfg(not(unix))]
                assert_eq!(cache.get(keys[0], None).await.unwrap().len(), 1024);
            } else {
                assert_eq!(cache.get(keys[0], None).await.unwrap().len(), 1024);
            }
            assert!(cache.exist(keys[1]).await);
            cache.data.run_pending_tasks();
            cache.put(keys[3], &[2; 1024]).await;
            cache.quiesce().await;
            assert!(cache.exist(keys[0]).await);
            assert!(!cache.exist(keys[1]).await);
            cache.reconcile();
        }
    }

    #[tokio::test]
    async fn result_capacity_evicts_only_overflow_and_cleans_one_query() {
        let cache = test_cache(FileType::Result, 8 * 1024, "fifo");
        let org = format!("disk-result-{}", config::ider::generate());
        let keys: Vec<_> = (0..9)
            .map(|i| format!("results/{org}/logs/stream/query{i}/1_2_0_0.json"))
            .collect();
        for key in &keys {
            let (_, _, query_key, meta) = parse_result_cache_key(key).unwrap();
            QUERY_RESULT_CACHE
                .write()
                .await
                .insert(query_key, vec![meta]);
            cache.put(key, &[1; 1024]).await;
            cache.data.run_pending_tasks();
        }
        cache.quiesce().await;
        assert_eq!(cache.len(), 8);
        assert!(!Path::new(&cache.get_file_path(&keys[0])).exists());
        for (i, key) in keys.iter().enumerate() {
            let (_, _, query_key, _) = parse_result_cache_key(key).unwrap();
            assert_eq!(
                QUERY_RESULT_CACHE.read().await.contains_key(&query_key),
                i != 0
            );
            QUERY_RESULT_CACHE.write().await.remove(&query_key);
        }
        cache.reconcile();
    }

    #[tokio::test]
    async fn queued_eviction_preserves_reinserted_inode() {
        let cache = TestCache::new(FileType::Data, 2 * 1024, false, Vec::new());
        let keys = [
            "files/reinsert/logs/a",
            "files/reinsert/logs/b",
            "files/reinsert/logs/c",
        ];
        for key in &keys {
            cache.put(key, &[1; 1024]).await;
            cache.data.run_pending_tasks();
        }
        assert!(!cache.data.contains_key(keys[0]));
        assert!(
            Path::new(&cache.get_file_path(keys[0])).exists(),
            "deleter is paused"
        );
        cache.put(keys[0], &[9; 1024]).await;
        cache.data.run_pending_tasks();
        cache.queue.start();
        cache.quiesce().await;
        assert_eq!(
            cache.get(keys[0], None).await.unwrap(),
            Bytes::from(vec![9; 1024])
        );
        cache.reconcile();
    }

    #[tokio::test]
    async fn explicit_remove_and_replaced_do_not_enqueue_unlinks() {
        let cache = TestCache::new(FileType::Data, 2 * 1024, false, Vec::new());
        let key = "files/explicit/logs/a";
        cache.put(key, &[1; 1024]).await;
        {
            let _stripe = cache.lock_key(key);
            cache.insert(key, 1024);
        }
        cache.data.run_pending_tasks();
        assert_eq!(cache.size().1, 1024);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.pending_deletes.load(Ordering::Relaxed), 0);
        cache.remove(key, None).unwrap();
        let path = cache.get_file_path(key);
        std::fs::write(&path, b"unindexed new inode").unwrap();
        cache.data.run_pending_tasks();
        cache.queue.start();
        cache.quiesce().await;
        assert_eq!(std::fs::read(path).unwrap(), b"unindexed new inode");
        assert_eq!(cache.size().1, 0);
        assert_eq!(cache.len(), 0);
    }

    #[tokio::test]
    async fn oversized_disk_entry_removes_tmp_without_evicting() {
        let cache = test_cache(FileType::Data, 1024, "lru");
        cache.put("files/oversize/logs/small", &[1; 1024]).await;
        let tmp = tempfile::NamedTempFile::new_in(cache.root.path()).unwrap();
        std::fs::write(tmp.path(), vec![2; 1025]).unwrap();
        assert!(
            !cache
                .set(
                    "files/oversize/logs/large",
                    tmp.path().to_str().unwrap(),
                    1025
                )
                .await
                .unwrap()
        );
        assert!(!tmp.path().exists());
        cache.quiesce().await;
        assert!(cache.exist("files/oversize/logs/small").await);
        cache.reconcile();
        assert_eq!(weight_kib(0), 1);
        assert_eq!(weight_kib(1025), 2);
        assert_eq!(weight_kib(usize::MAX), u32::MAX);
    }

    #[tokio::test]
    async fn load_orders_by_mtime_and_preserves_metrics_results_handling() {
        let data = test_cache(FileType::Data, 2 * 1024, "lru");
        let results = FileData::with_capacity(
            FileType::Result,
            2 * 1024,
            data.root_dir.clone(),
            Vec::new(),
            data.queue.sender.clone(),
        );
        let aggregations = FileData::with_capacity(
            FileType::Aggregation,
            2 * 1024,
            data.root_dir.clone(),
            Vec::new(),
            data.queue.sender.clone(),
        );
        let keys = [
            "files/load/logs/z-old",
            "files/load/logs/a-middle",
            "files/load/logs/m-new",
        ];
        for (i, key) in keys.iter().enumerate() {
            let path = data.get_file_path(key);
            std::fs::create_dir_all(Path::new(&path).parent().unwrap()).unwrap();
            std::fs::write(&path, vec![i as u8; 1024]).unwrap();
            std::fs::File::options()
                .write(true)
                .open(&path)
                .unwrap()
                .set_times(std::fs::FileTimes::new().set_modified(
                    SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(100 + i as u64),
                ))
                .unwrap();
        }
        let metric_key = format!("metrics_results/load-{}/value", config::ider::generate());
        let metric_path = data.get_file_path(&metric_key);
        std::fs::create_dir_all(Path::new(&metric_path).parent().unwrap()).unwrap();
        std::fs::write(&metric_path, b"metric").unwrap();
        let abandoned = data.root.path().join("abandoned.tmp");
        std::fs::write(&abandoned, b"tmp").unwrap();
        let root = data.root.path().canonicalize().unwrap();
        load(&root, [&data.files, &results, &aggregations])
            .await
            .unwrap();
        data.quiesce().await;
        assert!(!data.exist(keys[0]).await);
        assert!(data.exist(keys[1]).await);
        assert!(data.exist(keys[2]).await);
        assert!(!Path::new(&data.get_file_path(keys[0])).exists());
        assert!(!abandoned.exists());
        assert!(!results.data.contains_key(&metric_key));
        assert!(METRICS_RESULT_CACHE.read().await.contains(&metric_key));
        METRICS_RESULT_CACHE
            .write()
            .await
            .retain(|key| key != &metric_key);
        std::fs::remove_file(metric_path).unwrap();
        data.reconcile();
    }

    #[tokio::test]
    async fn sub_kib_capacity_bypasses_without_leaking_accounting() {
        for capacity in [0, 512, 1023] {
            let cache = test_cache(FileType::Data, capacity, "lru");
            cache.put("files/tiny/logs/empty", b"").await;
            cache.put("files/tiny/logs/value", b"x").await;
            cache.quiesce().await;
            assert_eq!(cache.size().1, 0);
            assert_eq!(cache.len(), 0);
            cache.reconcile();
        }
    }

    #[tokio::test]
    async fn zero_byte_entries_have_weight_and_leave_no_orphans() {
        let cache = test_cache(FileType::Data, 2048, "lru");
        for i in 0..4 {
            cache.put(&format!("files/empty/logs/{i}"), b"").await;
        }
        cache.quiesce().await;
        assert_eq!(cache.size().1, 0);
        assert_eq!(cache.len(), 2);
        cache.reconcile();
    }

    #[tokio::test]
    async fn missing_index_cleanup_preserves_concurrent_publish() {
        let cache = test_cache(FileType::Data, 2048, "lru");
        let key = "files/missing/logs/new";
        assert!(!Path::new(&cache.get_file_path(key)).exists());
        cache.put(key, b"published after failed open").await;
        assert!(!cache.remove_index_if_missing(key).unwrap());
        assert!(cache.exist(key).await);
        cache.quiesce().await;
        cache.reconcile();
    }

    #[tokio::test]
    async fn eviction_unlink_failure_is_counted_and_backlog_drains() {
        let cache = test_cache(FileType::Aggregation, 1024, "lru");
        let key = "aggregations/delete-error/logs/stream/query/1_2.arrow";
        let path = cache.get_file_path(key);
        std::fs::create_dir_all(&path).unwrap();
        cache.insert(key, 1024);
        cache.data.run_pending_tasks();
        let errors = metrics::DISK_CACHE_DELETE_ERRORS_TOTAL.with_label_values(&["aggregation"]);
        let before = errors.get();
        cache
            .put(
                "aggregations/delete-error/logs/stream/new/1_2.arrow",
                &[1; 1024],
            )
            .await;
        cache.quiesce().await;
        assert!(errors.get() > before);
        assert_eq!(cache.pending_deletes.load(Ordering::Relaxed), 0);
        assert!(!cache.data.contains_key(key));
        assert!(Path::new(&path).is_dir());
        std::fs::remove_dir(path).unwrap();
        cache.reconcile();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn disk_concurrent_churn_reconciles_exact_bytes() {
        let cache = test_cache(FileType::Data, 16 * 1024, "lru");
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let mut tasks = tokio::task::JoinSet::new();
            for task in 0..16u64 {
                let files = cache.files.clone();
                let root = cache.root.path().to_path_buf();
                tasks.spawn(async move {
                    let mut rng = task + 1;
                    for op in 0..256 {
                        rng ^= rng << 13;
                        rng ^= rng >> 7;
                        rng ^= rng << 17;
                        let key = format!("files/stress/logs/test/{}", rng % 64);
                        match (rng >> 8) % 4 {
                            0 => {
                                let tmp = root.join(format!("{task}-{op}.tmp"));
                                let size = (rng as usize % 3 + 1) * 512;
                                std::fs::write(&tmp, vec![task as u8; size]).unwrap();
                                let guard = TempFileGuard::new(tmp.to_string_lossy().into_owned());
                                files.set(&key, guard.path(), size).await.unwrap();
                            }
                            1 => {
                                files.get(&key, None).await;
                            }
                            2 => {
                                files.exist(&key).await;
                            }
                            _ => {
                                files.remove(&key, None).unwrap();
                            }
                        }
                    }
                });
            }
            while let Some(task) = tasks.join_next().await {
                task.unwrap();
            }
            cache.quiesce().await;
        })
        .await
        .expect("concurrent disk cache operations deadlocked");
        cache.reconcile();
    }
}
