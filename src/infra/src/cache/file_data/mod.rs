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

pub mod delete;
pub mod disk;
pub mod memory;

use std::{
    collections::{BTreeMap, VecDeque},
    ops::Range,
    path::{Path, PathBuf},
};

use bytes::Bytes;
use config::utils::time::{HourFormat, get_ymdh_from_micros};
use futures::StreamExt;
use hashbrown::HashSet;
use hashlink::lru_cache::LruCache;
use object_store::{GetOptions, GetResult};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

const DOWNLOAD_RETRY_TIMES: usize = 3;
/// Disk-write batching for streamed downloads (H3): body chunks arrive at
/// the transport's granularity and are coalesced into writes of at most
/// this size — the per-download RAM bound.
const DOWNLOAD_BUFFER_SIZE: usize = 8 * 1024 * 1024;
const INITIAL_CACHE_SIZE: usize = 128;
pub const TRACE_ID_FOR_CACHE_LATEST_FILE: &str = "cache_latest_file";

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum CacheType {
    Disk,
    Memory,
    None,
}

enum CacheStrategy {
    Lru(LruCache<String, usize>),
    Fifo(VecDeque<(String, usize)>, HashSet<String>),
    TimeLru(
        BTreeMap<u64, usize>,
        Vec<LruCache<String, usize>>,
        HashSet<String>,
    ),
}

enum FileType {
    Parquet,
    /// Puffin container — the envelope of `.vix` core files.
    Puffin,
    Vortex,
}

impl CacheStrategy {
    fn new(name: &str) -> Self {
        match name.to_lowercase().as_str() {
            "lru" => CacheStrategy::Lru(LruCache::new_unbounded()),
            "fifo" => CacheStrategy::Fifo(
                VecDeque::with_capacity(INITIAL_CACHE_SIZE),
                HashSet::with_capacity(INITIAL_CACHE_SIZE),
            ),
            "time_lru" => CacheStrategy::TimeLru(
                BTreeMap::new(),
                Vec::new(),
                HashSet::with_capacity(INITIAL_CACHE_SIZE),
            ),
            _ => CacheStrategy::Lru(LruCache::new_unbounded()),
        }
    }

    fn insert(&mut self, key: String, size: usize) {
        match self {
            CacheStrategy::Lru(cache) => {
                cache.insert(key, size);
            }
            CacheStrategy::Fifo(queue, set) => {
                set.insert(key.clone());
                queue.push_back((key, size));
            }
            CacheStrategy::TimeLru(map, cache, set) => {
                let time = get_file_time(&key).unwrap_or(0);
                set.insert(key.clone());
                let idx = map.entry(time).or_insert_with(|| {
                    cache.push(LruCache::new_unbounded());
                    cache.len() - 1
                });
                cache[*idx].insert(key, size);
            }
        }
    }

    fn remove(&mut self) -> Option<(String, usize)> {
        match self {
            CacheStrategy::Lru(cache) => cache.remove_lru(),
            CacheStrategy::Fifo(queue, set) => {
                if queue.is_empty() {
                    return None;
                }
                queue.pop_front().map(|(key, size)| {
                    set.remove(&key);
                    (key, size)
                })
            }
            CacheStrategy::TimeLru(map, cache, set) => {
                if map.is_empty() {
                    return None;
                }
                let mut idx = None;
                for val in map.values() {
                    if !cache[*val].is_empty() {
                        idx = Some(*val);
                        break;
                    }
                }
                let idx = idx?;
                let (key, size) = cache[idx].remove_lru()?;
                set.remove(&key);
                Some((key, size))
            }
        }
    }

    fn remove_key(&mut self, key: &str) -> Option<(String, usize)> {
        match self {
            CacheStrategy::Lru(cache) => cache.remove_entry(key),
            CacheStrategy::Fifo(queue, set) => {
                if queue.is_empty() {
                    return None;
                }
                let mut index = 0;
                while index < queue.len() {
                    if queue[index].0 == key {
                        let (k, v) = queue.remove(index).unwrap();
                        set.remove(&k);
                        return Some((k, v));
                    }
                    index += 1;
                }
                None
            }
            CacheStrategy::TimeLru(map, cache, set) => {
                if map.is_empty() {
                    return None;
                }
                let time = get_file_time(key).unwrap_or(0);
                let idx = map.get(&time).copied()?;
                let (key, size) = cache[idx].remove_entry(key)?;
                set.remove(&key);
                Some((key, size))
            }
        }
    }

    fn contains_key(&self, key: &str) -> bool {
        match self {
            CacheStrategy::Lru(cache) => cache.contains_key(key),
            CacheStrategy::Fifo(_, set) => set.contains(key),
            CacheStrategy::TimeLru(_, _, set) => set.contains(key),
        }
    }

    fn len(&self) -> usize {
        match self {
            CacheStrategy::Lru(cache) => cache.len(),
            CacheStrategy::Fifo(queue, _) => queue.len(),
            CacheStrategy::TimeLru(_, _, set) => set.len(),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            CacheStrategy::Lru(cache) => cache.is_empty(),
            CacheStrategy::Fifo(queue, _) => queue.is_empty(),
            CacheStrategy::TimeLru(map, ..) => map.is_empty(),
        }
    }
}

pub async fn init() -> Result<(), anyhow::Error> {
    disk::init().await?;
    memory::init().await?;
    Ok(())
}

pub async fn download(
    account: &str,
    file: &str,
    size: Option<usize>,
) -> Result<usize, anyhow::Error> {
    let cfg = config::get_config();
    if cfg.memory_cache.enabled {
        memory::download(account, file, size).await
    } else if cfg.disk_cache.enabled {
        disk::download(account, file, size).await
    } else {
        Ok(0)
    }
}

/// Whether a storage key names a file_list-tracked stream data file (so a
/// size mismatch must be reconciled against the file_list tables).
///
/// `.parquet`/`.vortex`/`.vix` are all data files — a core `.vix` IS the
/// tracked stream file. Its `.vxi` INDEX SIDECAR (v3 split) is NOT tracked:
/// it has no file_list row of its own (its size is the data row's
/// `index_size` column), so it is cacheable but never reconciled here.
fn is_file_list_tracked_data_file(file: &str) -> bool {
    file.ends_with(".parquet") || file.ends_with(".vortex") || file.ends_with(".vix")
}

async fn validate_file(bytes: &[u8], ftype: FileType) -> Result<(), anyhow::Error> {
    match ftype {
        FileType::Parquet => {
            let b = Bytes::copy_from_slice(bytes);
            let mut reader = parquet::file::metadata::ParquetMetaDataReader::new();
            reader.try_parse(&b)?;
        }
        FileType::Puffin => {
            if bytes.len() < 12 {
                return Err(anyhow::anyhow!("invalid puffin file"));
            }
            let footer = &bytes[bytes.len() - 12..bytes.len()];
            if footer[8..12] != [0x50, 0x46, 0x41, 0x31] {
                return Err(anyhow::anyhow!("puffin footer magic mismatch"));
            }
            let payload_size = i32::from_le_bytes(footer[0..4].try_into().unwrap());
            if bytes.len() < 12 + payload_size as usize {
                return Err(anyhow::anyhow!("payload size mismatch"));
            }
        }
        FileType::Vortex => {
            if bytes.len() < 12 {
                return Err(anyhow::anyhow!("invalid vortex file"));
            }
            const VORTEX_MAGIC: &[u8; 4] = b"VTXF";
            if &bytes[..4] != VORTEX_MAGIC || &bytes[bytes.len() - 4..] != VORTEX_MAGIC {
                return Err(anyhow::anyhow!("vortex magic bytes mismatch"));
            }
        }
    }
    Ok(())
}

/// Where a download's body lands while it is verified.
///
/// The MEMORY cache path buffers (its objects are small — callers route
/// anything big to disk via `memory_cache.skip_size`). The DISK cache path
/// streams into the cache's tmp file in bounded chunks so the object never
/// transits RAM whole — condition H3, the 2026-08-17 compactor-OOM fix
/// (merge inputs used to be buffered whole via `res.bytes()` at cpu_num
/// concurrency per job).
enum DownloadSink {
    Buffer(Vec<u8>),
    File {
        path: PathBuf,
        writer: Option<tokio::io::BufWriter<tokio::fs::File>>,
        written: u64,
    },
}

impl DownloadSink {
    fn buffer() -> Self {
        DownloadSink::Buffer(Vec::new())
    }

    fn file(path: PathBuf) -> Self {
        DownloadSink::File {
            path,
            writer: None,
            written: 0,
        }
    }

    /// Prepare the sink for a (re-)download attempt: previous partial
    /// content is discarded.
    async fn reset(&mut self) -> Result<(), anyhow::Error> {
        match self {
            DownloadSink::Buffer(buf) => buf.clear(),
            DownloadSink::File {
                path,
                writer,
                written,
            } => {
                // create() truncates an earlier partial attempt
                let file = tokio::fs::File::create(&*path).await.map_err(|e| {
                    anyhow::anyhow!("create download tmp file {}: {e}", path.display())
                })?;
                *writer = Some(tokio::io::BufWriter::with_capacity(
                    DOWNLOAD_BUFFER_SIZE,
                    file,
                ));
                *written = 0;
            }
        }
        Ok(())
    }

    async fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), anyhow::Error> {
        match self {
            DownloadSink::Buffer(buf) => buf.extend_from_slice(chunk),
            DownloadSink::File {
                path,
                writer,
                written,
            } => {
                let writer = writer
                    .as_mut()
                    .ok_or_else(|| anyhow::anyhow!("download sink used before reset"))?;
                writer.write_all(chunk).await.map_err(|e| {
                    anyhow::anyhow!("write download tmp file {}: {e}", path.display())
                })?;
                *written += chunk.len() as u64;
            }
        }
        Ok(())
    }

    /// Flush a file sink after the body ended (buffered bytes reach disk
    /// before the length check and any validation read).
    async fn finish_attempt(&mut self) -> Result<(), anyhow::Error> {
        if let DownloadSink::File { path, writer, .. } = self
            && let Some(writer) = writer.as_mut()
        {
            writer.flush().await.map_err(|e| {
                anyhow::anyhow!("flush download tmp file {}: {e}", path.display())
            })?;
        }
        Ok(())
    }

    fn len(&self) -> u64 {
        match self {
            DownloadSink::Buffer(buf) => buf.len() as u64,
            DownloadSink::File { written, .. } => *written,
        }
    }

    /// Structural validation for the db-size reconciliation arm: buffers
    /// validate in place, file sinks validate by ranged reads (footer /
    /// magic probes) — never by reading the object back into RAM.
    async fn validate(&self, ftype: FileType) -> Result<(), anyhow::Error> {
        match self {
            DownloadSink::Buffer(buf) => validate_file(buf, ftype).await,
            DownloadSink::File { path, .. } => validate_file_ranged(path, ftype).await,
        }
    }
}

/// [`validate_file`] over a file on disk using ranged reads only (H3: the
/// reconciliation probe must not buffer the object either).
async fn validate_file_ranged(path: &Path, ftype: FileType) -> Result<(), anyhow::Error> {
    let mut file = tokio::fs::File::open(path).await?;
    let len = file.metadata().await?.len();
    match ftype {
        FileType::Parquet => {
            // footer tail first; NeedMoreData tells us the exact suffix the
            // metadata needs — mirror parquet's documented retry pattern
            let mut tail = std::cmp::min(len, 64 * 1024);
            loop {
                file.seek(std::io::SeekFrom::Start(len - tail)).await?;
                let mut buf = vec![0u8; tail as usize];
                file.read_exact(&mut buf).await?;
                let mut reader = parquet::file::metadata::ParquetMetaDataReader::new();
                match reader.try_parse_sized(&Bytes::from(buf), len) {
                    Ok(()) => return Ok(()),
                    Err(parquet::errors::ParquetError::NeedMoreData(needed))
                        if (needed as u64) <= len && (needed as u64) > tail =>
                    {
                        tail = needed as u64;
                    }
                    Err(e) => return Err(e.into()),
                }
            }
        }
        FileType::Puffin => {
            if len < 12 {
                return Err(anyhow::anyhow!("invalid puffin file"));
            }
            let mut footer = [0u8; 12];
            file.seek(std::io::SeekFrom::Start(len - 12)).await?;
            file.read_exact(&mut footer).await?;
            if footer[8..12] != [0x50, 0x46, 0x41, 0x31] {
                return Err(anyhow::anyhow!("puffin footer magic mismatch"));
            }
            let payload_size = i32::from_le_bytes(footer[0..4].try_into().unwrap());
            if len < 12 + payload_size as u64 {
                return Err(anyhow::anyhow!("payload size mismatch"));
            }
        }
        FileType::Vortex => {
            if len < 12 {
                return Err(anyhow::anyhow!("invalid vortex file"));
            }
            const VORTEX_MAGIC: &[u8; 4] = b"VTXF";
            let mut head = [0u8; 4];
            file.read_exact(&mut head).await?;
            let mut tail = [0u8; 4];
            file.seek(std::io::SeekFrom::Start(len - 4)).await?;
            file.read_exact(&mut tail).await?;
            if &head != VORTEX_MAGIC || &tail != VORTEX_MAGIC {
                return Err(anyhow::anyhow!("vortex magic bytes mismatch"));
            }
        }
    }
    Ok(())
}

/// The shared download contract over any [`DownloadSink`], preserved
/// exactly from the pre-H3 buffered implementation:
/// - up to [`DOWNLOAD_RETRY_TIMES`] attempts with doubling backoff when the body length
///   disagrees with the blob store's own header size (partial download);
/// - a body length that then disagrees with the file_list `size` reconciles: a structurally
///   valid file corrects the db row (`update_compressed_size`), a corrupt one removes it
///   and errors — both only for file_list-tracked data files;
/// - M19: a clean 404 on a file_list-tracked data file reconciles TOO — the object was
///   deleted externally (S3 lifecycle expiry) while its row is still live, so the row is
///   removed exactly like the corrupt-file arm. A 404 is a stronger signal than a size
///   mismatch (S3 reads are strongly consistent): no retry, no validation probe. Without
///   this, every query listing the row re-enqueues the download forever.
/// - other transport errors propagate immediately (they were never retried here).
///
/// `fetch` re-opens the object per attempt (injectable for tests).
#[derive(Clone, Copy, PartialEq, Eq)]
enum ExpectedSizePolicy {
    Reconcile,
    Exact,
}

async fn download_with_retries<F, Fut>(
    fetch: F,
    file: &str,
    size: Option<usize>,
    sink: &mut DownloadSink,
) -> Result<usize, anyhow::Error>
where
    F: Fn() -> Fut,
    Fut: Future<Output = object_store::Result<GetResult>>,
{
    download_with_retries_policy(fetch, file, size, sink, ExpectedSizePolicy::Reconcile).await
}

async fn download_with_retries_policy<F, Fut>(
    fetch: F,
    file: &str,
    size: Option<usize>,
    sink: &mut DownloadSink,
    size_policy: ExpectedSizePolicy,
) -> Result<usize, anyhow::Error>
where
    F: Fn() -> Fut,
    Fut: Future<Output = object_store::Result<GetResult>>,
{
    let mut data_len: u64 = 0;
    let mut retry_time = 1;
    let mut expected_blob_size = 0;
    for i in 0..DOWNLOAD_RETRY_TIMES {
        // get the initial headers
        let res = match fetch().await {
            Ok(res) => res,
            Err(object_store::Error::NotFound { .. })
                if is_file_list_tracked_data_file(file) =>
            {
                log::warn!(
                    "download file {file} not found in object store (deleted externally, e.g. lifecycle expiry), deleting entry from file_list"
                );
                crate::file_list::remove(file).await?;
                crate::file_list::LOCAL_CACHE.remove(file).await?;
                // the message keeps the "not found" needle: the compactor's
                // merge input reconciliation string-matches it
                return Err(anyhow::anyhow!(
                    "file {file} not found in object store, file_list entry removed"
                ));
            }
            Err(e) => return Err(e.into()),
        };
        // this is the size blob store has
        expected_blob_size = res.meta.size;
        if expected_blob_size == 0 {
            return Err(anyhow::anyhow!("file {} data size is zero", file));
        }
        if size_policy == ExpectedSizePolicy::Exact
            && let Some(size) = size
            && expected_blob_size != size as u64
        {
            return Err(anyhow::anyhow!(
                "file {file} object-store size {expected_blob_size} differs from registered size {size}; refusing to buffer beyond the caller's byte permit"
            ));
        }

        // stream the body into the sink in bounded chunks (H3: never
        // materialize the object in RAM on the file-sink path)
        sink.reset().await?;
        let mut body = res.into_stream();
        while let Some(chunk) = body.next().await {
            let chunk = chunk?;
            sink.write_chunk(&chunk).await?;
        }
        sink.finish_attempt().await?;
        data_len = sink.len();

        // if the downloaded length is not equal to what the blog store
        // sent in headers, we might have a partial download, so we log
        // and retry
        if data_len != expected_blob_size {
            let msg = if i == DOWNLOAD_RETRY_TIMES - 1 {
                format!("after {DOWNLOAD_RETRY_TIMES} retries")
            } else {
                "will retry".to_string()
            };
            log::warn!(
                "download file {file} found size mismatch with blob store header, expected: {expected_blob_size}, actual: {data_len}, {msg}",
            );
            tokio::time::sleep(tokio::time::Duration::from_secs(retry_time)).await;
            retry_time *= 2;
            continue;
        } else {
            // size matches
            break;
        }
    }
    // if even after retries, the download size does not match, we skip it
    // no point in validating or setting the value
    if data_len != expected_blob_size {
        return Err(anyhow::anyhow!(
            "file {file} could not be downloaded completely: expected {expected_blob_size}, got {data_len} skipping"
        ));
    }
    let data_len = data_len as usize;

    // now the size we downloaded matches what blob store has or tried the max attempts, we check
    // if it matches with what we have in db or not. Also because the size matches blob store/we
    // have exceeded try attempts, there is no sense in retrying here, because that is the size
    // we are going to get every time.
    match size {
        None => Ok(data_len),
        Some(size) => {
            if data_len == size {
                Ok(data_len)
            } else {
                // the entry in db does not match what there is actually in the blob store
                // so we check if the footer is valid. If it is, then the db entry is invalid
                // and we reset it. If footer is invalid, the store has a corrupted file
                // so we mark it as deleted, and return error.
                let is_data_file = is_file_list_tracked_data_file(file);
                let valid_parquet =
                    file.ends_with(".parquet") && sink.validate(FileType::Parquet).await.is_ok();
                let valid_vortex =
                    file.ends_with(".vortex") && sink.validate(FileType::Vortex).await.is_ok();
                // core .vix data files AND their .vxi index sidecars are
                // puffin containers (the sidecar has no file_list row —
                // is_data_file is false for it — but a structurally valid
                // download must still be cached, not rejected as corrupt)
                let valid_vix = (file.ends_with(".vix") || file.ends_with(".vxi"))
                    && sink.validate(FileType::Puffin).await.is_ok();
                if valid_parquet || valid_vortex || valid_vix {
                    log::warn!(
                        "download file {file} found size mismatch, remote : {expected_blob_size}, db: {size}, correcting db as valid file",
                    );
                    // only reconcile file_list-tracked data files
                    if is_data_file {
                        crate::file_list::update_compressed_size(file, data_len as i64).await?;
                        crate::file_list::LOCAL_CACHE
                            .update_compressed_size(file, data_len as i64)
                            .await?;
                    }
                    Ok(data_len)
                } else {
                    log::warn!(
                        "download file {file} found corrupt file, remote: {expected_blob_size}, db: {size}, deleting entry from file_list "
                    );
                    // only reconcile file_list-tracked data files
                    if is_data_file {
                        crate::file_list::remove(file).await?;
                        crate::file_list::LOCAL_CACHE.remove(file).await?;
                    }
                    Err(anyhow::anyhow!("file {file} is corrupted in blob store"))
                }
            }
        }
    }
}

/// Buffered download — the MEMORY cache fill path (small objects only;
/// callers bound it with `memory_cache.skip_size`).
async fn download_from_storage(
    account: &str,
    file: &str,
    size: Option<usize>,
) -> Result<(usize, bytes::Bytes), anyhow::Error> {
    let mut sink = DownloadSink::buffer();
    let data_len =
        download_with_retries(|| crate::storage::get(account, file), file, size, &mut sink)
            .await?;
    let DownloadSink::Buffer(buf) = sink else {
        unreachable!("buffer sink stays a buffer");
    };
    Ok((data_len, bytes::Bytes::from(buf)))
}

/// Buffered download for metadata whose registered size is an allocation
/// invariant (segment-WAL objects). The object-store header is compared
/// before the response body is collected, so a corrupt undersized metadata
/// row cannot bypass the caller's in-flight byte budget.
pub async fn download_from_storage_exact(
    account: &str,
    file: &str,
    expected_size: usize,
) -> Result<(usize, bytes::Bytes), anyhow::Error> {
    let mut sink = DownloadSink::buffer();
    let data_len = download_with_retries_policy(
        || crate::storage::get(account, file),
        file,
        Some(expected_size),
        &mut sink,
        ExpectedSizePolicy::Exact,
    )
    .await?;
    let DownloadSink::Buffer(buf) = sink else {
        unreachable!("buffer sink stays a buffer");
    };
    Ok((data_len, bytes::Bytes::from(buf)))
}

/// Streamed download — the DISK cache fill path (H3): the body streams
/// into `tmp_path` in bounded chunks and the object never transits RAM
/// whole. Same retry / size-verification / file_list-reconciliation
/// contract as [`download_from_storage`]; the caller renames the tmp file
/// into the cache (and cleans it up on error).
async fn download_from_storage_to_file(
    account: &str,
    file: &str,
    size: Option<usize>,
    tmp_path: &Path,
) -> Result<usize, anyhow::Error> {
    let mut sink = DownloadSink::file(tmp_path.to_path_buf());
    download_with_retries(|| crate::storage::get(account, file), file, size, &mut sink).await
}

/// set the data to the cache
///
/// store the data to the memory cache or disk cache
pub async fn set(key: &str, data: bytes::Bytes) -> Result<(), anyhow::Error> {
    let cfg = config::get_config();
    // set the data to the memory cache
    if cfg.memory_cache.enabled {
        memory::set(key, data).await
    } else if cfg.disk_cache.enabled {
        disk::set(key, data).await
    } else {
        Ok(())
    }
}

pub async fn get(
    account: &str,
    file: &str,
    range: Option<Range<u64>>,
) -> object_store::Result<bytes::Bytes> {
    let options = GetOptions {
        range: range.map(|r| r.into()),
        ..Default::default()
    };
    get_opts(account, file, options, true).await?.bytes().await
}

pub async fn get_opts(
    account: &str,
    file: &str,
    options: GetOptions,
    remote: bool,
) -> object_store::Result<GetResult> {
    let cfg = config::get_config();
    // get from memory cache
    if cfg.memory_cache.enabled
        && let Ok(ret) = memory::get_opts(file, options.clone()).await
    {
        return Ok(ret);
    }
    // get from disk cache
    if cfg.disk_cache.enabled
        && let Ok(ret) = disk::get_opts(file, options.clone()).await
    {
        return Ok(ret);
    }

    // get from storage
    if remote {
        return crate::storage::get_opts(account, file, options).await;
    }

    Err(object_store::Error::NotFound {
        path: file.to_string(),
        source: Box::new(std::io::Error::other(file)),
    })
}

pub async fn get_size(account: &str, file: &str) -> object_store::Result<usize> {
    get_size_opts(account, file, true).await
}

pub async fn get_size_opts(account: &str, file: &str, remote: bool) -> object_store::Result<usize> {
    let cfg = config::get_config();
    // get from memory cache
    if cfg.memory_cache.enabled
        && let Some(v) = memory::get_size(file).await
    {
        return Ok(v);
    }
    // get from disk cache
    if cfg.disk_cache.enabled
        && let Some(v) = disk::get_size(file).await
    {
        return Ok(v);
    }

    // get from storage
    if remote {
        let meta = crate::storage::head(account, file).await?;
        return Ok(meta.size as usize);
    }

    Err(object_store::Error::NotFound {
        path: file.to_string(),
        source: Box::new(std::io::Error::new(std::io::ErrorKind::NotFound, file)),
    })
}

/// Batched range read across the cache ladder.
///
/// `memory → disk → remote storage`, returning one `Bytes` per input
/// range in input order. The hit-path stays inside a single file
/// handle: memory cache slices its in-memory `Bytes`, disk cache does
/// one `File::open` + N `pread`s. Only on a full cache miss do we go
/// to remote storage (which itself implements batched `get_ranges`
/// for local FS and any object_store backend).
///
/// `remote = false` is the search-side semantic — never hit S3 on a
/// miss, return NotFound so the caller can degrade gracefully.
pub async fn get_ranges_opts(
    account: &str,
    file: &str,
    ranges: &[Range<u64>],
    remote: bool,
) -> object_store::Result<Vec<Bytes>> {
    let cfg = config::get_config();
    if cfg.memory_cache.enabled
        && let Some(v) = memory::get_ranges(file, ranges).await
    {
        return Ok(v);
    }
    if cfg.disk_cache.enabled
        && let Ok(v) = disk::get_ranges(file, ranges).await
    {
        return Ok(v);
    }

    if remote {
        return crate::storage::get_ranges(account, file, ranges).await;
    }

    Err(object_store::Error::NotFound {
        path: file.to_string(),
        source: Box::new(std::io::Error::other(file)),
    })
}

/// get the file time from the file name
///
/// metrics_cache:
/// metrics_results/default/2025/04/08/06/
/// 17caf18281f2a17c76a803a9cd59a207_1744091424000000_1744091426789749_1744089728661252.pb
/// log_cache:
/// results/default/logs/default/16042959487540176184_30_zo_sql_key/
/// 1744081170000000_1744081170000000_1_0.json
/// parquet_cache:
/// files/default/logs/disk/2025/04/08/06/7315292721030106704.parquet
/// aggregation cache:
/// aggregations/default/logs/default/16042959487540176184/1744081170000000_1744081170000000.arrow
fn get_file_time(file: &str) -> Option<u64> {
    let parts = file.split('/').collect::<Vec<_>>();
    if parts.len() < 6 {
        return None;
    }
    let date = match parts[0] {
        "metrics_results" => {
            format!("{}{}{}{}", parts[2], parts[3], parts[4], parts[5])
        }
        "results" => {
            let (_, _, _, meta) = disk::parse_result_cache_key(file)?;
            get_ymdh_from_micros(meta.start_time, HourFormat::Real).replace("/", "")
        }
        "files" => {
            if parts.len() < 8 {
                return None;
            }
            format!("{}{}{}{}", parts[4], parts[5], parts[6], parts[7])
        }
        "aggregations" => {
            let (_, _, _, meta) = disk::parse_aggregation_cache_key(file)?;
            get_ymdh_from_micros(meta.start_time, HourFormat::Real).replace("/", "")
        }
        _ => {
            return None;
        }
    };
    date.parse::<u64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_file_list_tracked_data_file() {
        // flat data files: always tracked
        assert!(is_file_list_tracked_data_file(
            "files/default/logs/b/2025/04/08/06/1.parquet"
        ));
        assert!(is_file_list_tracked_data_file(
            "files/default/logs/b/2025/04/08/06/1.vortex"
        ));
        // v2 core files: a .vix IS the tracked stream file
        assert!(is_file_list_tracked_data_file(
            "files/default/logs/b/2025/04/08/06/1.vix"
        ));
        assert!(is_file_list_tracked_data_file(
            "files/default/traces/b/2025/04/08/06/1.vix"
        ));
        // anything else is not
        assert!(!is_file_list_tracked_data_file(
            "files/default/logs/b/2025/04/08/06/1.json"
        ));
    }

    #[test]
    fn test_file_data_lru_cache_miss() {
        let mut cache = CacheStrategy::new("lru");
        let key1 = "files/default/logs/b/2025/04/08/06/1.parquet";
        let key2 = "files/default/logs/b/2025/04/08/06/2.parquet";
        cache.insert(key1.to_string(), 1);
        cache.insert(key2.to_string(), 2);
        cache.contains_key(key1);
        cache.remove(); // contains_key() does not mark the key1 as used -> removed
        assert!(!cache.contains_key(key1));
        assert!(cache.contains_key(key2));
    }

    #[test]
    fn test_file_data_fifo_cache_miss() {
        let mut cache = CacheStrategy::new("fifo");
        let key1 = "files/default/logs/b/2025/04/08/06/1.parquet";
        let key2 = "files/default/logs/b/2025/04/08/06/2.parquet";
        cache.insert(key1.to_string(), 1);
        cache.insert(key2.to_string(), 2);
        cache.contains_key(key1);
        cache.remove();
        assert!(!cache.contains_key(key1));
        assert!(cache.contains_key(key2));
    }

    #[test]
    fn test_file_data_time_lru_cache_miss() {
        let mut cache = CacheStrategy::new("time_lru");
        let key_small = "files/default/logs/b/2025/04/08/01/1.parquet";
        let key_big = "files/default/logs/b/2099/04/08/02/2.parquet";
        let key_other = "files/default/logs/b/2025/04/08/03/2.parquet";
        cache.insert(key_small.to_string(), 1);
        cache.insert(key_big.to_string(), 2);
        cache.insert(key_other.to_string(), 3);
        cache.contains_key(key_small);
        cache.remove();
        cache.remove();
        assert!(!cache.contains_key(key_small));
        assert!(!cache.contains_key(key_other));
        assert!(cache.contains_key(key_big));
    }

    #[test]
    fn test_file_data_get_file_time() {
        let file = "metrics_results/default/2025/04/08/06/17caf18281f2a17c76a803a9cd59a207_1744091424000000_1744091426789749_1744089728661252.pb";
        let time = get_file_time(file);
        assert_eq!(time, Some(2025040806));

        let file = "results/default/logs/default/16042959487540176184_30_zo_sql_key/1744081170000000_1744081170000000_1_0.json";
        let time = get_file_time(file);
        assert_eq!(time, Some(2025040802));

        let file = "files/default/logs/disk/2022/10/03/10/7315292721030106704.parquet";
        let time = get_file_time(file);
        assert_eq!(time, Some(2022100310));

        let file = "aggregations/default/logs/default/16042959487540176184/1744081170000000_1744081170000000.arrow";
        let time = get_file_time(file);
        assert_eq!(time, Some(2025040802));
    }

    #[test]
    fn test_cache_type_equality_and_copy() {
        let a = CacheType::Disk;
        let b = a;
        assert_eq!(a, b);
        assert_ne!(CacheType::Disk, CacheType::Memory);
        assert_ne!(CacheType::Memory, CacheType::None);
        assert_ne!(CacheType::Disk, CacheType::None);
    }

    #[test]
    fn test_cache_strategy_unknown_defaults_to_lru() {
        let mut lru = CacheStrategy::new("lru");
        let mut unknown = CacheStrategy::new("something_unknown");
        let key = "files/default/logs/b/2025/04/08/06/1.parquet";
        lru.insert(key.to_string(), 10);
        unknown.insert(key.to_string(), 10);
        assert!(lru.contains_key(key));
        assert!(unknown.contains_key(key));
        assert_eq!(lru.len(), unknown.len());
    }

    #[test]
    fn test_cache_strategy_is_empty_and_len() {
        let mut cache = CacheStrategy::new("fifo");
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
        cache.insert(
            "files/default/logs/b/2025/04/08/06/x.parquet".to_string(),
            1,
        );
        assert!(!cache.is_empty());
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_cache_strategy_lru_is_empty_and_len() {
        let mut cache = CacheStrategy::new("lru");
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
        cache.insert(
            "files/default/logs/b/2025/04/08/06/x.parquet".to_string(),
            5,
        );
        assert!(!cache.is_empty());
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_cache_strategy_remove_key_lru() {
        let mut cache = CacheStrategy::new("lru");
        let key = "files/default/logs/b/2025/04/08/06/k.parquet";
        cache.insert(key.to_string(), 42);
        assert!(cache.contains_key(key));
        let removed = cache.remove_key(key);
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().1, 42);
        assert!(!cache.contains_key(key));
    }

    #[test]
    fn test_cache_strategy_remove_key_fifo_empty() {
        let mut cache = CacheStrategy::new("fifo");
        let removed = cache.remove_key("nonexistent");
        assert!(removed.is_none());
    }

    #[test]
    fn test_cache_strategy_remove_key_fifo_missing_key() {
        let mut cache = CacheStrategy::new("fifo");
        cache.insert(
            "files/default/logs/b/2025/04/08/06/a.parquet".to_string(),
            1,
        );
        let removed = cache.remove_key("files/default/logs/b/2025/04/08/06/b.parquet");
        assert!(removed.is_none());
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_cache_strategy_remove_on_empty_returns_none() {
        let mut lru = CacheStrategy::new("lru");
        assert!(lru.remove().is_none());
        let mut fifo = CacheStrategy::new("fifo");
        assert!(fifo.remove().is_none());
        let mut time_lru = CacheStrategy::new("time_lru");
        assert!(time_lru.remove().is_none());
    }

    // ---- H3 streamed download core -------------------------------------

    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    /// A synthetic [`GetResult`] whose header advertises `header_size` and
    /// whose body is `chunks` — the injectable seam the retry loop tests
    /// drive (real storage never produces deterministic partial bodies).
    fn synthetic_get_result(chunks: Vec<Bytes>, header_size: u64) -> GetResult {
        let stream = futures::stream::iter(chunks.into_iter().map(Ok));
        GetResult {
            payload: object_store::GetResultPayload::Stream(Box::pin(stream)),
            meta: object_store::ObjectMeta {
                location: object_store::path::Path::from("synthetic"),
                last_modified: chrono::Utc::now(),
                size: header_size,
                e_tag: None,
                version: None,
            },
            range: 0..header_size,
            attributes: Default::default(),
        }
    }


    #[tokio::test]
    async fn exact_size_policy_rejects_header_before_buffering_body() {
        let mut sink = DownloadSink::buffer();
        let error = download_with_retries_policy(
            || async {
                Ok(synthetic_get_result(
                    vec![Bytes::from_static(b"must not be buffered")],
                    19,
                ))
            },
            "wal_segments/node/1.seg",
            Some(1),
            &mut sink,
            ExpectedSizePolicy::Exact,
        )
        .await
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("refusing to buffer beyond the caller's byte permit"),
            "{error}"
        );
        assert_eq!(sink.len(), 0);
    }
    fn scratch_file(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "o2-m3-download-{}-{}",
            name,
            config::ider::generate()
        ))
    }

    /// A minimal structurally valid puffin: zero-length payload + footer
    /// magic (exactly what `validate_file` checks).
    fn minimal_puffin() -> Vec<u8> {
        vec![0, 0, 0, 0, 0, 0, 0, 0, 0x50, 0x46, 0x41, 0x31]
    }

    #[tokio::test]
    async fn streamed_download_happy_path_writes_file_not_ram() {
        let path = scratch_file("happy");
        let body: Vec<Bytes> = vec![
            Bytes::from_static(b"hello "),
            Bytes::from_static(b"streamed "),
            Bytes::from_static(b"world"),
        ];
        let total: usize = body.iter().map(|b| b.len()).sum();
        let fetches = Arc::new(AtomicUsize::new(0));
        let fetches_in = Arc::clone(&fetches);
        let body_in = body.clone();
        let mut sink = DownloadSink::file(path.clone());
        let len = download_with_retries(
            move || {
                fetches_in.fetch_add(1, Ordering::SeqCst);
                let chunks = body_in.clone();
                async move { Ok(synthetic_get_result(chunks, total as u64)) }
            },
            "files/default/logs/s/2025/04/08/06/1.bin",
            Some(total),
            &mut sink,
        )
        .await
        .unwrap();
        assert_eq!(len, total);
        assert_eq!(fetches.load(Ordering::SeqCst), 1);
        let written = tokio::fs::read(&path).await.unwrap();
        assert_eq!(written, b"hello streamed world");
        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test(start_paused = true)]
    async fn streamed_download_retries_short_body_then_succeeds() {
        let path = scratch_file("retry");
        let fetches = Arc::new(AtomicUsize::new(0));
        let fetches_in = Arc::clone(&fetches);
        let mut sink = DownloadSink::file(path.clone());
        // header says 10 bytes; attempts 1-2 deliver 7, attempt 3 delivers 10
        let len = download_with_retries(
            move || {
                let attempt = fetches_in.fetch_add(1, Ordering::SeqCst) + 1;
                async move {
                    let body = if attempt < 3 {
                        vec![Bytes::from_static(b"partial")]
                    } else {
                        vec![Bytes::from_static(b"full-body!")]
                    };
                    Ok(synthetic_get_result(body, 10))
                }
            },
            "files/default/logs/s/2025/04/08/06/2.bin",
            None,
            &mut sink,
        )
        .await
        .unwrap();
        assert_eq!(len, 10);
        assert_eq!(fetches.load(Ordering::SeqCst), 3);
        // the retry TRUNCATED the earlier partial attempt: exactly the last body
        let written = tokio::fs::read(&path).await.unwrap();
        assert_eq!(written, b"full-body!");
        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test(start_paused = true)]
    async fn streamed_download_fails_after_all_retries() {
        let path = scratch_file("fail");
        let fetches = Arc::new(AtomicUsize::new(0));
        let fetches_in = Arc::clone(&fetches);
        let mut sink = DownloadSink::file(path.clone());
        let err = download_with_retries(
            move || {
                fetches_in.fetch_add(1, Ordering::SeqCst);
                async move { Ok(synthetic_get_result(vec![Bytes::from_static(b"short")], 10)) }
            },
            "files/default/logs/s/2025/04/08/06/3.bin",
            None,
            &mut sink,
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string().contains("could not be downloaded completely"),
            "unexpected error: {err}"
        );
        assert_eq!(fetches.load(Ordering::SeqCst), DOWNLOAD_RETRY_TIMES);
        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn streamed_download_zero_size_errors() {
        let path = scratch_file("zero");
        let mut sink = DownloadSink::file(path.clone());
        let err = download_with_retries(
            || async { Ok(synthetic_get_result(vec![], 0)) },
            "files/default/logs/s/2025/04/08/06/4.bin",
            None,
            &mut sink,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("data size is zero"));
        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn streamed_db_size_mismatch_valid_puffin_reconciles() {
        // a `.vxi` sidecar is puffin-validated but NOT file_list-tracked, so
        // the reconciliation arm runs without touching any meta DB: a valid
        // container with a wrong db size must still cache (Ok), exactly the
        // buffered path's contract
        let path = scratch_file("mismatch-valid");
        let body = minimal_puffin();
        let total = body.len();
        let mut sink = DownloadSink::file(path.clone());
        let len = download_with_retries(
            move || {
                let body = Bytes::from(body.clone());
                async move { Ok(synthetic_get_result(vec![body], total as u64)) }
            },
            "files/default/logs/s/2025/04/08/06/5.vxi",
            Some(total + 7), // db row disagrees
            &mut sink,
        )
        .await
        .unwrap();
        assert_eq!(len, total);
        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn streamed_db_size_mismatch_corrupt_errors() {
        // unvalidatable extension + db-size mismatch = the corrupt arm; the
        // key is untracked so no file_list reconciliation happens in-test
        let path = scratch_file("mismatch-corrupt");
        let mut sink = DownloadSink::file(path.clone());
        let err = download_with_retries(
            || async { Ok(synthetic_get_result(vec![Bytes::from_static(b"garbage")], 7)) },
            "files/default/logs/s/2025/04/08/06/6.bin",
            Some(99),
            &mut sink,
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string().contains("corrupted in blob store"),
            "unexpected error: {err}"
        );
        let _ = tokio::fs::remove_file(&path).await;
    }

    /// M19: a clean 404 (object deleted externally, e.g. S3 lifecycle
    /// expiry) on a file_list-TRACKED data file must remove its file_list
    /// row — the row is what keeps queries/merges re-hitting the dead
    /// object forever — and error with a "not found" message (the merge
    /// input reconciliation string-matches it). No retry: one fetch only.
    #[tokio::test]
    async fn streamed_404_of_tracked_file_removes_file_list_row() {
        // real sqlite file_list tables (process-global; rows namespaced by a
        // unique per-run file name)
        std::fs::create_dir_all(&config::get_config().common.data_db_dir)
            .expect("create data_db_dir for tests");
        crate::file_list::create_table()
            .await
            .expect("create file_list tables");
        let key = format!(
            "files/m19org/logs/m19stream/2026/08/19/00/gone_{}.vix",
            config::ider::generate()
        );
        crate::file_list::add(
            "",
            &key,
            &config::meta::stream::FileMeta {
                min_ts: 1,
                max_ts: 2,
                records: 10,
                original_size: 100,
                compressed_size: 50,
                ..Default::default()
            },
        )
        .await
        .expect("seed file_list row");
        assert!(crate::file_list::contains(&key).await.unwrap());

        let path = scratch_file("gone-tracked");
        let fetches = Arc::new(AtomicUsize::new(0));
        let fetches_in = Arc::clone(&fetches);
        let key_in = key.clone();
        let mut sink = DownloadSink::file(path.clone());
        let err = download_with_retries(
            move || {
                fetches_in.fetch_add(1, Ordering::SeqCst);
                let path = key_in.clone();
                async move {
                    Err(object_store::Error::NotFound {
                        path,
                        source: Box::new(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            "expired",
                        )),
                    })
                }
            },
            &key,
            Some(50),
            &mut sink,
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string().to_lowercase().contains("not found"),
            "the error must keep the merge-reconciliation needle: {err}"
        );
        assert_eq!(
            fetches.load(Ordering::SeqCst),
            1,
            "a 404 is definitive — no retries"
        );
        assert!(
            !crate::file_list::contains(&key).await.unwrap(),
            "the stale file_list row must be removed"
        );
        let _ = tokio::fs::remove_file(&path).await;
    }

    /// M19: a 404 on an UNTRACKED key (`.vxi` index sidecar — no file_list
    /// row of its own) propagates as a plain error, touching no meta table;
    /// the reader side fails open on a missing sidecar.
    #[tokio::test]
    async fn streamed_404_of_untracked_sidecar_propagates() {
        let path = scratch_file("gone-sidecar");
        let mut sink = DownloadSink::file(path.clone());
        let err = download_with_retries(
            || async {
                Err(object_store::Error::NotFound {
                    path: "files/o/logs/s/2026/08/19/00/x.vxi".to_string(),
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "expired",
                    )),
                })
            },
            "files/o/logs/s/2026/08/19/00/x.vxi",
            Some(50),
            &mut sink,
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string().to_lowercase().contains("not found"),
            "unexpected error: {err}"
        );
        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn buffered_sink_matches_contract() {
        // the memory-cache path shares the same core; prove the buffer sink
        // returns the exact bytes
        let mut sink = DownloadSink::buffer();
        let len = download_with_retries(
            || async {
                Ok(synthetic_get_result(
                    vec![Bytes::from_static(b"abc"), Bytes::from_static(b"def")],
                    6,
                ))
            },
            "files/default/logs/s/2025/04/08/06/7.bin",
            Some(6),
            &mut sink,
        )
        .await
        .unwrap();
        assert_eq!(len, 6);
        let DownloadSink::Buffer(buf) = sink else {
            panic!("buffer sink changed variant");
        };
        assert_eq!(buf, b"abcdef");
    }

    #[tokio::test]
    async fn validate_file_ranged_probes_magic_by_range() {
        // puffin
        let puffin = scratch_file("validate-puffin");
        tokio::fs::write(&puffin, minimal_puffin()).await.unwrap();
        validate_file_ranged(&puffin, FileType::Puffin)
            .await
            .unwrap();
        // vortex
        let vortex = scratch_file("validate-vortex");
        tokio::fs::write(&vortex, b"VTXF----VTXF").await.unwrap();
        validate_file_ranged(&vortex, FileType::Vortex)
            .await
            .unwrap();
        // corrupt cases
        let bad = scratch_file("validate-bad");
        tokio::fs::write(&bad, b"neither a puffin nor a vortex")
            .await
            .unwrap();
        assert!(validate_file_ranged(&bad, FileType::Puffin).await.is_err());
        assert!(validate_file_ranged(&bad, FileType::Vortex).await.is_err());
        for f in [&puffin, &vortex, &bad] {
            let _ = tokio::fs::remove_file(f).await;
        }
    }

    #[tokio::test]
    async fn validate_file_ranged_parses_parquet_footer() {
        // a zero-row-group parquet is still a complete footer + metadata
        let schema = std::sync::Arc::new(
            parquet::schema::types::Type::group_type_builder("schema")
                .with_fields(vec![std::sync::Arc::new(
                    parquet::schema::types::Type::primitive_type_builder(
                        "id",
                        parquet::basic::Type::INT32,
                    )
                    .build()
                    .unwrap(),
                )])
                .build()
                .unwrap(),
        );
        let props =
            std::sync::Arc::new(parquet::file::properties::WriterProperties::builder().build());
        let mut buf = Vec::new();
        let writer =
            parquet::file::writer::SerializedFileWriter::new(&mut buf, schema, props).unwrap();
        writer.close().unwrap();

        let path = scratch_file("validate-parquet");
        tokio::fs::write(&path, &buf).await.unwrap();
        validate_file_ranged(&path, FileType::Parquet)
            .await
            .unwrap();
        // truncating the footer must fail the probe
        let truncated = scratch_file("validate-parquet-trunc");
        tokio::fs::write(&truncated, &buf[..buf.len() - 4])
            .await
            .unwrap();
        assert!(
            validate_file_ranged(&truncated, FileType::Parquet)
                .await
                .is_err()
        );
        for f in [&path, &truncated] {
            let _ = tokio::fs::remove_file(f).await;
        }
    }

    #[test]
    fn test_get_file_time_unknown_prefix_returns_none() {
        let file = "unknown/default/logs/b/2025/04/08/06/1.parquet";
        assert_eq!(get_file_time(file), None);
    }

    #[test]
    fn test_get_file_time_too_short_path_returns_none() {
        let file = "files/a/b/c";
        assert_eq!(get_file_time(file), None);
    }
}
