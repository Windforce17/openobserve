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

//! Block-granular disk cache tier behind the whole-file disk cache.
//!
//! The whole-file tiers cache objects in full, but a query touches 2-3% of
//! a `.vix` data file (its Vortex footer plus the projected columns'
//! chunks) and ~2% of an index sidecar, so a whole-file fill moves 27-60x
//! the bytes the query reads (prod counters, 2026-10-10). This tier caches
//! exactly the block-aligned ranges that range reads miss, write-through
//! from object storage: a cold read fetches the missing blocks (one GET per
//! contiguous run), writes them into a sparse file and serves the request;
//! a warm read is one `pread`.
//!
//! On-disk layout: one sparse file per object under `<root>/<key>`. Bytes
//! `[0, object_size)` mirror the object at identical offsets (holes where
//! blocks were never fetched); a trailer at `align_up(object_size, 4096)`
//! records the presence bitmap. The in-memory bitmap is authoritative while
//! the process runs; the trailer is rewritten by a background flusher
//! (`fdatasync` of the data first, then the trailer) and only read back by
//! [`BlockCache::load`] at startup. Bits are set after the data write
//! returned, so a persisted bit implies durable data; a torn or corrupt
//! trailer fails its checksum and discards the entry (refetched later),
//! it can never claim a block that was not written.
//!
//! Locking: bucket indexes are touched only for O(1) map operations and
//! never held across IO or `await`; all file IO (`pread`/`pwrite`/unlink)
//! runs on the blocking pool outside the locks, so a slow disk or an
//! eviction sweep cannot stall unrelated readers - unlike the whole-file
//! tier, whose set-triggered GC unlinks files under its bucket write lock.

use std::{
    ops::Range,
    path::{Path, PathBuf},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use config::{get_config, metrics, utils::hash::sum64_bytes};
use futures::{StreamExt, TryStreamExt, future::BoxFuture};
use hashbrown::HashMap;
use parking_lot::{Mutex, RwLock};
use tokio::sync::mpsc;

use super::CacheStrategy;

const TRAILER_MAGIC: [u8; 8] = *b"O2BLKC01";
const TRAILER_HEADER_LEN: usize = 40;
const TRAILER_ALIGN: u64 = 4096;
/// Coalescing delay of the trailer flusher: fills landing within this window
/// share one `fdatasync` + trailer write per entry.
const FLUSH_DELAY: Duration = Duration::from_secs(1);
/// Missing runs separated by at most this many bytes of present blocks are
/// fetched as one span (re-fetching the present blocks in between): request
/// count over bytes on object storage, like the object_store coalescer.
const COALESCE_GAP_BYTES: u64 = 1024 * 1024;
/// Concurrent fills per request.
const FILL_CONCURRENCY: usize = 10;

static GLOBAL: OnceLock<Arc<BlockCache>> = OnceLock::new();

/// The process-wide tier, present when `ZO_DISK_CACHE_BLOCK_ENABLED` and the
/// disk cache are both on and [`init`] ran.
pub fn global() -> Option<&'static Arc<BlockCache>> {
    GLOBAL.get()
}

pub async fn init() -> Result<(), anyhow::Error> {
    let cfg = get_config();
    if !cfg.disk_cache.enabled || !cfg.disk_cache.block_enabled {
        return Ok(());
    }
    let root = PathBuf::from(format!("{}blocks/", cfg.common.data_cache_dir));
    std::fs::create_dir_all(&root)?;
    let per_bucket = cfg.disk_cache.block_max_size as u64;
    let cache = BlockCache::new(
        root,
        cfg.disk_cache.block_size as u64,
        per_bucket,
        (per_bucket / 10).max(cfg.disk_cache.block_size as u64),
        &cfg.disk_cache.cache_strategy,
        cfg.disk_cache.bucket_num,
    );
    if GLOBAL.set(Arc::clone(&cache)).is_err() {
        return Ok(());
    }
    tokio::task::spawn(async move {
        log::info!("Loading disk block cache start");
        match cache.load().await {
            Ok(entries) => log::info!("Loading disk block cache done, entries: {entries}"),
            Err(e) => log::error!("load disk block cache error: {e}"),
        }
    });
    Ok(())
}

/// Bytes fetched from object storage for a block-aligned range plus the
/// object's total size, which a cold entry learns from its first fetch.
pub struct Fetched {
    pub bytes: Bytes,
    pub object_size: u64,
}

pub type FetchFuture = BoxFuture<'static, object_store::Result<Fetched>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Every block of the range was present.
    Hit,
    /// Some blocks were fetched.
    Partial,
    /// Every block was fetched (cold object or cold region).
    Miss,
}

impl Outcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::Partial => "partial",
            Self::Miss => "miss",
        }
    }
}

#[derive(Debug)]
pub struct BlockRead {
    pub bytes: Bytes,
    pub object_size: u64,
    pub outcome: Outcome,
    /// Block-aligned bytes fetched from object storage for this request.
    pub remote_bytes: u64,
}

/// [`BlockRead`] for several ranges of one object; `bytes` is in request order.
#[derive(Debug)]
pub struct BlockReads {
    pub bytes: Vec<Bytes>,
    pub object_size: u64,
    pub outcome: Outcome,
    pub remote_bytes: u64,
}

/// Sort and merge overlapping or adjacent ranges.
fn merge_ranges(ranges: impl Iterator<Item = Range<u64>>) -> Vec<Range<u64>> {
    let mut sorted: Vec<Range<u64>> = ranges.collect();
    sorted.sort_by_key(|r| r.start);
    let mut merged: Vec<Range<u64>> = Vec::with_capacity(sorted.len());
    for range in sorted {
        match merged.last_mut() {
            Some(last) if range.start <= last.end => last.end = last.end.max(range.end),
            _ => merged.push(range),
        }
    }
    merged
}

fn overlap(a: &Range<u64>, b: &Range<u64>) -> u64 {
    a.end.min(b.end).saturating_sub(a.start.max(b.start))
}

pub struct BlockCache {
    root: PathBuf,
    block_size: u64,
    buckets: Vec<Bucket>,
    dirty: mpsc::UnboundedSender<Arc<Entry>>,
}

struct Bucket {
    max_size: u64,
    release_size: u64,
    cur_size: AtomicU64,
    evicting: AtomicBool,
    index: RwLock<Index>,
}

struct Index {
    entries: HashMap<Arc<str>, Arc<Entry>>,
    order: CacheStrategy,
}

pub struct Entry {
    key: Arc<str>,
    path: PathBuf,
    object_size: u64,
    block_size: u64,
    state: Mutex<State>,
    /// Sitting in the flusher queue; cleared when the flusher takes the trailer.
    queued: AtomicBool,
}

struct State {
    bits: Vec<u64>,
    present_bytes: u64,
    evicted: bool,
}

impl Entry {
    fn new(key: Arc<str>, path: PathBuf, object_size: u64, block_size: u64) -> Self {
        let blocks = block_count(object_size, block_size);
        Self {
            key,
            path,
            object_size,
            block_size,
            state: Mutex::new(State {
                bits: vec![0u64; blocks.div_ceil(64) as usize],
                present_bytes: 0,
                evicted: false,
            }),
            queued: AtomicBool::new(false),
        }
    }

    fn block_bytes(&self, block: u64) -> u64 {
        ((block + 1) * self.block_size).min(self.object_size) - block * self.block_size
    }

    /// Contiguous runs of absent blocks over the union of `block_ranges`,
    /// treating `covered` (a run whose bytes the caller already holds) as
    /// present. Runs closer than the coalescing gap merge into one span.
    fn missing_runs(
        &self,
        block_ranges: &[Range<u64>],
        covered: Option<&Range<u64>>,
    ) -> Vec<Range<u64>> {
        let state = self.state.lock();
        let mut runs: Vec<Range<u64>> = Vec::new();
        for range in merge_ranges(block_ranges.iter().cloned()) {
            for block in range {
                if is_set(&state.bits, block) || covered.is_some_and(|c| c.contains(&block)) {
                    continue;
                }
                match runs.last_mut() {
                    Some(last) if last.end == block => last.end += 1,
                    _ => runs.push(block..block + 1),
                }
            }
        }
        drop(state);
        let gap_blocks = COALESCE_GAP_BYTES.div_ceil(self.block_size);
        let mut merged: Vec<Range<u64>> = Vec::with_capacity(runs.len());
        for run in runs {
            match merged.last_mut() {
                Some(last) if run.start - last.end <= gap_blocks => last.end = run.end,
                _ => merged.push(run),
            }
        }
        merged
    }

    /// Mark `blocks` present after their bytes were written; returns the
    /// newly accounted bytes (zero for already-present blocks or an evicted
    /// entry, whose file is gone or going).
    fn mark_present(&self, blocks: Range<u64>) -> u64 {
        let mut state = self.state.lock();
        if state.evicted {
            return 0;
        }
        let mut added = 0u64;
        for block in blocks {
            if !is_set(&state.bits, block) {
                set_bit(&mut state.bits, block);
                added += self.block_bytes(block);
            }
        }
        state.present_bytes += added;
        added
    }

    fn trailer_offset(&self) -> u64 {
        self.object_size.div_ceil(TRAILER_ALIGN) * TRAILER_ALIGN
    }

    /// Serialized trailer for the flusher, `None` once evicted.
    fn trailer(&self) -> Option<Vec<u8>> {
        let state = self.state.lock();
        if state.evicted {
            return None;
        }
        Some(encode_trailer(
            self.block_size,
            self.object_size,
            &state.bits,
        ))
    }
}

fn block_count(object_size: u64, block_size: u64) -> u64 {
    object_size.div_ceil(block_size)
}

fn is_set(bits: &[u64], block: u64) -> bool {
    bits[(block / 64) as usize] & (1u64 << (block % 64)) != 0
}

fn set_bit(bits: &mut [u64], block: u64) {
    bits[(block / 64) as usize] |= 1u64 << (block % 64);
}

fn bitmap_bytes(block_count: u64) -> usize {
    block_count.div_ceil(8) as usize
}

/// `[bitmap][header 40 B]`; the header is last so `load` finds it from the
/// file end: magic, block_size u32, pad u32, object_size u64, bitmap_len u32,
/// pad u32, checksum u64 over everything before it.
fn encode_trailer(block_size: u64, object_size: u64, bits: &[u64]) -> Vec<u8> {
    let bitmap_len = bitmap_bytes(block_count(object_size, block_size));
    let mut out = Vec::with_capacity(bitmap_len + TRAILER_HEADER_LEN);
    for word in bits {
        out.extend_from_slice(&word.to_le_bytes());
    }
    out.truncate(bitmap_len);
    out.resize(bitmap_len, 0);
    out.extend_from_slice(&TRAILER_MAGIC);
    out.extend_from_slice(&(block_size as u32).to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
    out.extend_from_slice(&object_size.to_le_bytes());
    out.extend_from_slice(&(bitmap_len as u32).to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
    let checksum = sum64_bytes(&out);
    out.extend_from_slice(&checksum.to_le_bytes());
    out
}

struct Trailer {
    block_size: u64,
    object_size: u64,
    bits: Vec<u64>,
}

/// Parse the trailer of a cache file; `None` for anything that is not a
/// complete, checksummed trailer consistent with the file length.
fn read_trailer(file: &std::fs::File) -> Option<Trailer> {
    let len = file.metadata().ok()?.len();
    if len < TRAILER_HEADER_LEN as u64 {
        return None;
    }
    let mut header = [0u8; TRAILER_HEADER_LEN];
    pread_exact(file, &mut header, len - TRAILER_HEADER_LEN as u64).ok()?;
    if header[..8] != TRAILER_MAGIC {
        return None;
    }
    let block_size = u32::from_le_bytes(header[8..12].try_into().unwrap()) as u64;
    let object_size = u64::from_le_bytes(header[16..24].try_into().unwrap());
    let bitmap_len = u32::from_le_bytes(header[24..28].try_into().unwrap()) as usize;
    let checksum = u64::from_le_bytes(header[32..40].try_into().unwrap());
    if block_size == 0 || !block_size.is_power_of_two() || object_size == 0 {
        return None;
    }
    let blocks = block_count(object_size, block_size);
    if bitmap_len != bitmap_bytes(blocks) {
        return None;
    }
    let trailer_offset = object_size.div_ceil(TRAILER_ALIGN) * TRAILER_ALIGN;
    if len != trailer_offset + bitmap_len as u64 + TRAILER_HEADER_LEN as u64 {
        return None;
    }
    let mut payload = vec![0u8; bitmap_len + TRAILER_HEADER_LEN - 8];
    pread_exact(file, &mut payload, trailer_offset).ok()?;
    if sum64_bytes(&payload) != checksum {
        return None;
    }
    let mut bits = vec![0u64; blocks.div_ceil(64) as usize];
    for (i, byte) in payload[..bitmap_len].iter().enumerate() {
        bits[i / 8] |= (*byte as u64) << (8 * (i % 8));
    }
    // Bits past the last block would inflate accounting; a trailer that has
    // them did not come from this writer.
    for block in blocks..(bits.len() as u64 * 64) {
        if is_set(&bits, block) {
            return None;
        }
    }
    Some(Trailer {
        block_size,
        object_size,
        bits,
    })
}

#[cfg(unix)]
fn pread_exact(file: &std::fs::File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.read_exact_at(buf, offset)
}

#[cfg(unix)]
fn pwrite_all(file: &std::fs::File, buf: &[u8], offset: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.write_all_at(buf, offset)
}

#[cfg(windows)]
fn pread_exact(file: &std::fs::File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::os::windows::fs::FileExt;
    let mut done = 0usize;
    while done < buf.len() {
        let n = file.seek_read(&mut buf[done..], offset + done as u64)?;
        if n == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        done += n;
    }
    Ok(())
}

#[cfg(windows)]
fn pwrite_all(file: &std::fs::File, buf: &[u8], offset: u64) -> std::io::Result<()> {
    use std::os::windows::fs::FileExt;
    let mut done = 0usize;
    while done < buf.len() {
        let n = file.seek_write(&buf[done..], offset + done as u64)?;
        if n == 0 {
            return Err(std::io::ErrorKind::WriteZero.into());
        }
        done += n;
    }
    Ok(())
}

fn key_labels(key: &str) -> [&str; 2] {
    let mut parts = key.split('/');
    match (parts.next(), parts.next(), parts.next()) {
        (Some("files"), Some(org), Some(stream_type)) => [org, stream_type],
        _ => ["unknown", "unknown"],
    }
}

fn generic(
    store: &'static str,
    error: impl std::error::Error + Send + Sync + 'static,
) -> object_store::Error {
    object_store::Error::Generic {
        store,
        source: Box::new(error),
    }
}

impl BlockCache {
    /// `max_size`/`release_size` are PER BUCKET, like the whole-file tier's
    /// config after its per-bucket split. Spawns the trailer flusher, so it
    /// needs a Tokio runtime.
    pub fn new(
        root: PathBuf,
        block_size: u64,
        max_size: u64,
        release_size: u64,
        strategy: &str,
        bucket_num: usize,
    ) -> Arc<Self> {
        assert!(block_size.is_power_of_two() && block_size >= 512);
        let (dirty, rx) = mpsc::unbounded_channel();
        let buckets = (0..bucket_num.max(1))
            .map(|_| Bucket {
                max_size,
                release_size: release_size.max(block_size),
                cur_size: AtomicU64::new(0),
                evicting: AtomicBool::new(false),
                index: RwLock::new(Index {
                    entries: HashMap::new(),
                    order: CacheStrategy::new(strategy),
                }),
            })
            .collect();
        tokio::task::spawn(flusher(rx));
        Arc::new(Self {
            root,
            block_size,
            buckets,
            dirty,
        })
    }

    pub fn block_size(&self) -> u64 {
        self.block_size
    }

    fn bucket(&self, key: &str) -> &Bucket {
        if self.buckets.len() == 1 {
            return &self.buckets[0];
        }
        let h = sum64_bytes(key.as_bytes());
        &self.buckets[(h % self.buckets.len() as u64) as usize]
    }

    fn path_for(&self, key: &str) -> PathBuf {
        self.root.join(key)
    }

    /// `(max bytes, used bytes, entries)` summed over buckets.
    pub fn stats(&self) -> (u64, u64, usize) {
        let mut max = 0;
        let mut used = 0;
        let mut entries = 0;
        for bucket in &self.buckets {
            max += bucket.max_size;
            used += bucket.cur_size.load(Ordering::Relaxed);
            entries += bucket.index.read().entries.len();
        }
        (max, used, entries)
    }

    fn get_entry(&self, key: &str) -> Option<Arc<Entry>> {
        self.bucket(key).index.read().entries.get(key).cloned()
    }

    /// Insert an empty entry for a cold object, or adopt the one a concurrent
    /// cold reader inserted first.
    fn insert_or_get(&self, key: &str, object_size: u64) -> Arc<Entry> {
        let bucket = self.bucket(key);
        let mut index = bucket.index.write();
        if let Some(existing) = index.entries.get(key) {
            if existing.object_size != object_size {
                log::warn!(
                    "[BlockCache] object {key} size changed {} -> {object_size}; keeping the cached generation",
                    existing.object_size
                );
            }
            return Arc::clone(existing);
        }
        let key: Arc<str> = Arc::from(key);
        let entry = Arc::new(Entry::new(
            Arc::clone(&key),
            self.path_for(&key),
            object_size,
            self.block_size,
        ));
        index.order.insert(key.to_string(), 0);
        index.entries.insert(key, Arc::clone(&entry));
        let [org, stream_type] = key_labels(&entry.key);
        metrics::QUERY_DISK_BLOCK_CACHE_FILES
            .with_label_values(&[org, stream_type])
            .inc();
        entry
    }

    /// Serve `requested` of object `key`: present blocks from the cache file,
    /// missing blocks through `fetch` (block-aligned ranges, clipped to the
    /// object once its size is known) and written through. `requested.end`
    /// past the object end is clipped like `GetRange::as_range`; a start at
    /// or past it is `NotFound`. `owner` is held through the blocking IO.
    pub async fn read_through(
        &self,
        key: &str,
        requested: Range<u64>,
        owner: Option<Arc<dyn Send + Sync>>,
        fetch: &(dyn Fn(Range<u64>) -> FetchFuture + Sync),
    ) -> object_store::Result<BlockRead> {
        let mut reads = self
            .read_through_many(key, std::slice::from_ref(&requested), owner, fetch)
            .await?;
        Ok(BlockRead {
            bytes: reads.bytes.pop().unwrap_or_default(),
            object_size: reads.object_size,
            outcome: reads.outcome,
            remote_bytes: reads.remote_bytes,
        })
    }

    /// [`Self::read_through`] for several ranges of one object at once: the
    /// missing blocks of all ranges are fetched together (a block shared by
    /// two ranges is fetched once), then every range is read from the file.
    /// Results are in request order.
    pub async fn read_through_many(
        &self,
        key: &str,
        requested: &[Range<u64>],
        owner: Option<Arc<dyn Send + Sync>>,
        fetch: &(dyn Fn(Range<u64>) -> FetchFuture + Sync),
    ) -> object_store::Result<BlockReads> {
        if requested.iter().any(|r| r.start > r.end) {
            return Err(crate::storage::Error::BadRange(key.to_string()).into());
        }
        if requested.is_empty() {
            return Ok(BlockReads {
                bytes: Vec::new(),
                object_size: 0,
                outcome: Outcome::Hit,
                remote_bytes: 0,
            });
        }
        // The cold fetch covers the lowest non-empty range; with only empty
        // ranges it reads block 0 (a zero-length GET is not a valid request).
        let first = requested
            .iter()
            .filter(|r| !r.is_empty())
            .min_by_key(|r| r.start)
            .cloned()
            .unwrap_or(0..1);
        let bs = self.block_size;
        let mut fills: Vec<(Range<u64>, Bytes)> = Vec::new();
        let mut remote_bytes = 0u64;
        let (entry, created) = match self.get_entry(key) {
            Some(entry) => (entry, false),
            None => {
                // Cold object: the first fetch also tells us its size. Past the
                // end the store clips the response (`as_range` semantics).
                let run = first.start / bs..first.end.div_ceil(bs);
                let fetched = fetch(run.start * bs..run.end * bs).await?;
                let object_size = fetched.object_size;
                let run = run.start..run.end.min(block_count(object_size, bs));
                let expected = (run.end * bs)
                    .min(object_size)
                    .saturating_sub(run.start * bs);
                if fetched.bytes.len() as u64 != expected {
                    return Err(short_response(key, expected, fetched.bytes.len()));
                }
                remote_bytes += expected;
                let entry = self.insert_or_get(key, object_size);
                fills.push((run, fetched.bytes));
                (entry, true)
            }
        };
        let object_size = entry.object_size;
        let mut ranges = Vec::with_capacity(requested.len());
        for r in requested {
            if r.start > object_size || (r.start == object_size && !r.is_empty()) {
                return Err(object_store::Error::NotFound {
                    path: key.to_string(),
                    source: Box::new(std::io::Error::other(format!(
                        "range start {} beyond object size {object_size}",
                        r.start
                    ))),
                });
            }
            ranges.push(r.start..r.end.min(object_size));
        }
        let wanted = merge_ranges(ranges.iter().filter(|r| !r.is_empty()).cloned());
        let wanted_blocks: Vec<Range<u64>> = wanted
            .iter()
            .map(|r| r.start / bs..r.end.div_ceil(bs))
            .collect();
        let covered = fills.first().map(|(run, _)| run.clone());
        let missing = entry.missing_runs(&wanted_blocks, covered.as_ref());
        if !missing.is_empty() {
            // Futures are created eagerly: a lazy map over `&missing` held
            // across the await makes this future's type non-spawnable.
            let fetches: Vec<FetchFuture> = missing
                .iter()
                .map(|run| fetch(run.start * bs..(run.end * bs).min(object_size)))
                .collect();
            let results: Vec<Fetched> = futures::stream::iter(fetches)
                .buffered(FILL_CONCURRENCY)
                .try_collect()
                .await?;
            for (run, fetched) in missing.into_iter().zip(results) {
                let expected = (run.end * bs).min(object_size) - run.start * bs;
                if fetched.bytes.len() as u64 != expected {
                    return Err(short_response(key, expected, fetched.bytes.len()));
                }
                remote_bytes += expected;
                fills.push((run, fetched.bytes));
            }
        }
        let wanted_block_count: u64 = merge_ranges(wanted_blocks.iter().cloned())
            .iter()
            .map(|r| r.end - r.start)
            .sum();
        let filled_blocks: u64 = fills
            .iter()
            .flat_map(|(run, _)| wanted_blocks.iter().map(move |w| overlap(run, w)))
            .sum();
        let outcome = if fills.is_empty() {
            Outcome::Hit
        } else if filled_blocks >= wanted_block_count {
            Outcome::Miss
        } else {
            Outcome::Partial
        };
        let wanted_bytes: u64 = wanted.iter().map(|r| r.end - r.start).sum();
        // Logical bytes of the request that fills (not present blocks) supplied.
        let miss_logical: u64 = fills
            .iter()
            .flat_map(|(run, _)| {
                let run_bytes = run.start * bs..(run.end * bs).min(object_size);
                wanted.iter().map(move |w| overlap(&run_bytes, w))
            })
            .sum();

        let io = IoJob {
            path: entry.path.clone(),
            create: created || !fills.is_empty(),
            writes: fills
                .iter()
                .map(|(run, bytes)| (run.start * bs, bytes.clone()))
                .collect(),
            reads: ranges.clone(),
            owner,
        };
        let read = tokio::task::spawn_blocking(move || io.run())
            .await
            .map_err(|e| generic("disk block cache", e))?;
        let bytes = match read {
            Ok(bytes) => bytes,
            Err(error) => {
                // Evicted between lookup and IO (NotFound), or the local disk
                // misbehaved: a cache must never fail the query. Drop the entry
                // so the next reader starts cold, serve this one from object
                // storage without caching it.
                if error.kind() == std::io::ErrorKind::NotFound {
                    log::debug!("[BlockCache] {key} evicted during read, serving remote");
                } else {
                    log::warn!("[BlockCache] {key} local IO failed, serving remote: {error}");
                }
                self.drop_entry(&entry);
                // Zero-length ranges are not valid store requests.
                let fetches: Vec<FetchFuture> = ranges
                    .iter()
                    .filter(|range| !range.is_empty())
                    .map(|range| fetch(range.clone()))
                    .collect();
                let mut results = futures::stream::iter(fetches)
                    .buffered(FILL_CONCURRENCY)
                    .try_collect::<Vec<Fetched>>()
                    .await?
                    .into_iter();
                let mut bytes = Vec::with_capacity(ranges.len());
                for range in &ranges {
                    if range.is_empty() {
                        bytes.push(Bytes::new());
                        continue;
                    }
                    let fetched = results
                        .next()
                        .ok_or_else(|| short_response(key, range.end - range.start, 0))?;
                    let len = fetched.bytes.len() as u64;
                    if len != range.end - range.start {
                        return Err(short_response(key, range.end - range.start, len as usize));
                    }
                    remote_bytes += len;
                    bytes.push(fetched.bytes);
                }
                metrics::QUERY_DISK_BLOCK_CACHE_REQUESTS_TOTAL
                    .with_label_values(&["miss"])
                    .inc();
                metrics::QUERY_DISK_BLOCK_CACHE_BYTES_TOTAL
                    .with_label_values(&["remote"])
                    .inc_by(remote_bytes);
                return Ok(BlockReads {
                    bytes,
                    object_size,
                    outcome: Outcome::Miss,
                    remote_bytes,
                });
            }
        };
        if !fills.is_empty() && entry.state.lock().evicted && self.get_entry(key).is_none() {
            // Evicted while we were filling: the sweeper's unlink may have
            // preceded our create, leaving an orphan file.
            unlink_later(vec![Arc::clone(&entry)]);
        }

        let added: u64 = fills
            .iter()
            .map(|(run, _)| entry.mark_present(run.clone()))
            .sum();
        if added > 0 {
            let bucket = self.bucket(key);
            let cur = bucket.cur_size.fetch_add(added, Ordering::Relaxed) + added;
            let [org, stream_type] = key_labels(&entry.key);
            metrics::QUERY_DISK_BLOCK_CACHE_USED_BYTES
                .with_label_values(&[org, stream_type])
                .add(added as i64);
            self.enqueue_dirty(&entry);
            if cur > bucket.max_size {
                self.evict(bucket, bucket.release_size.max(added));
            }
        }
        metrics::QUERY_DISK_BLOCK_CACHE_REQUESTS_TOTAL
            .with_label_values(&[outcome.as_str()])
            .inc();
        metrics::QUERY_DISK_BLOCK_CACHE_BYTES_TOTAL
            .with_label_values(&["block"])
            .inc_by(wanted_bytes.saturating_sub(miss_logical));
        metrics::QUERY_DISK_BLOCK_CACHE_BYTES_TOTAL
            .with_label_values(&["remote"])
            .inc_by(remote_bytes);
        Ok(BlockReads {
            bytes,
            object_size,
            outcome,
            remote_bytes,
        })
    }

    /// Whether a request of `total_bytes` belongs in this tier: one request
    /// must not displace half a bucket.
    pub fn admits(&self, total_bytes: u64) -> bool {
        total_bytes <= self.buckets[0].max_size / 2
    }

    /// Size of an object this tier already knows, for callers that hold a
    /// relative (`Suffix`/`Offset`) range.
    pub fn object_size(&self, key: &str) -> Option<u64> {
        self.get_entry(key).map(|entry| entry.object_size)
    }

    fn enqueue_dirty(&self, entry: &Arc<Entry>) {
        if !entry.queued.swap(true, Ordering::AcqRel) {
            let _ = self.dirty.send(Arc::clone(entry));
        }
    }

    /// Drop the entry for `key` (e.g. the whole file was cached and the blocks
    /// are redundant). The unlink runs on the blocking pool.
    pub fn remove(&self, key: &str) {
        let bucket = self.bucket(key);
        let entry = {
            let mut index = bucket.index.write();
            index.order.remove_key(key);
            index.entries.remove(key)
        };
        if let Some(entry) = entry {
            let released = self.retire(&entry);
            bucket.cur_size.fetch_sub(released, Ordering::Relaxed);
            unlink_later(vec![entry]);
        }
    }

    /// Drop `entry` only if it is still the indexed generation of its key:
    /// a newer entry inserted after an eviction must survive.
    fn drop_entry(&self, entry: &Arc<Entry>) {
        let bucket = self.bucket(&entry.key);
        let removed = {
            let mut index = bucket.index.write();
            match index.entries.get(&*entry.key) {
                Some(current) if Arc::ptr_eq(current, entry) => {
                    index.order.remove_key(&entry.key);
                    index.entries.remove(&*entry.key)
                }
                _ => None,
            }
        };
        if let Some(entry) = removed {
            let released = self.retire(&entry);
            bucket.cur_size.fetch_sub(released, Ordering::Relaxed);
            unlink_later(vec![entry]);
        }
    }

    /// Mark an entry evicted (no further accounting) and return its bytes;
    /// must run after it left the index.
    fn retire(&self, entry: &Entry) -> u64 {
        let released = {
            let mut state = entry.state.lock();
            state.evicted = true;
            state.present_bytes
        };
        let [org, stream_type] = key_labels(&entry.key);
        metrics::QUERY_DISK_BLOCK_CACHE_FILES
            .with_label_values(&[org, stream_type])
            .dec();
        metrics::QUERY_DISK_BLOCK_CACHE_USED_BYTES
            .with_label_values(&[org, stream_type])
            .sub(released as i64);
        released
    }

    /// Evict in strategy order until `need` bytes are released. One sweeper
    /// per bucket at a time; index entries leave under the write lock, files
    /// are unlinked afterwards off the lock.
    fn evict(&self, bucket: &Bucket, need: u64) {
        if bucket
            .evicting
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let start = std::time::Instant::now();
        let mut victims = Vec::new();
        let mut released = 0u64;
        {
            let mut index = bucket.index.write();
            while released < need {
                let Some((key, _)) = index.order.remove() else {
                    break;
                };
                if let Some(entry) = index.entries.remove(key.as_str()) {
                    released += self.retire(&entry);
                    victims.push(entry);
                }
            }
        }
        bucket.cur_size.fetch_sub(released, Ordering::Relaxed);
        bucket.evicting.store(false, Ordering::Release);
        log::info!(
            "[BlockCache] evicted {} entries, released {released} bytes, took {} ms",
            victims.len(),
            start.elapsed().as_millis()
        );
        unlink_later(victims);
    }

    /// Write every pending trailer now (graceful shutdown, tests).
    pub async fn flush(&self) {
        let mut work = Vec::new();
        for bucket in &self.buckets {
            let entries: Vec<Arc<Entry>> = bucket.index.read().entries.values().cloned().collect();
            for entry in entries {
                entry.queued.store(false, Ordering::Release);
                if let Some(trailer) = entry.trailer() {
                    work.push((entry.path.clone(), entry.trailer_offset(), trailer));
                }
            }
        }
        let _ = tokio::task::spawn_blocking(move || write_trailers(work)).await;
    }

    /// Rebuild the index from the cache directory: files with a valid
    /// trailer become entries, anything else is removed (unless a live
    /// entry already owns the path).
    pub async fn load(self: &Arc<Self>) -> Result<usize, anyhow::Error> {
        let cache = Arc::clone(self);
        tokio::task::spawn_blocking(move || cache.load_blocking()).await?
    }

    fn load_blocking(&self) -> Result<usize, anyhow::Error> {
        let mut loaded = 0usize;
        let mut stack = vec![self.root.clone()];
        while let Some(dir) = stack.pop() {
            let entries = match std::fs::read_dir(&dir) {
                Ok(entries) => entries,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            };
            for dir_entry in entries {
                let dir_entry = dir_entry?;
                let path = dir_entry.path();
                if dir_entry.file_type()?.is_dir() {
                    stack.push(path);
                    continue;
                }
                let Some(key) = path
                    .strip_prefix(&self.root)
                    .ok()
                    .and_then(|p| p.to_str())
                    .map(|k| k.replace('\\', "/"))
                else {
                    continue;
                };
                if self.adopt_file(&key, &path) {
                    loaded += 1;
                }
            }
        }
        Ok(loaded)
    }

    /// Index one file found at startup; returns whether it became an entry.
    fn adopt_file(&self, key: &str, path: &Path) -> bool {
        let bucket = self.bucket(key);
        if bucket.index.read().entries.contains_key(key) {
            return false;
        }
        let trailer = std::fs::File::open(path)
            .ok()
            .and_then(|file| read_trailer(&file))
            .filter(|t| t.block_size == self.block_size);
        let Some(trailer) = trailer else {
            // Not ours, torn, or written with another block size.
            if !bucket.index.read().entries.contains_key(key)
                && let Err(e) = std::fs::remove_file(path)
            {
                log::warn!(
                    "[BlockCache] remove invalid cache file {}: {e}",
                    path.display()
                );
            }
            return false;
        };
        let blocks = block_count(trailer.object_size, self.block_size);
        let mut present_bytes = 0u64;
        for block in 0..blocks {
            if is_set(&trailer.bits, block) {
                present_bytes += ((block + 1) * self.block_size).min(trailer.object_size)
                    - block * self.block_size;
            }
        }
        let key: Arc<str> = Arc::from(key);
        let entry = Arc::new(Entry {
            key: Arc::clone(&key),
            path: path.to_path_buf(),
            object_size: trailer.object_size,
            block_size: self.block_size,
            state: Mutex::new(State {
                bits: trailer.bits,
                present_bytes,
                evicted: false,
            }),
            queued: AtomicBool::new(false),
        });
        let mut index = bucket.index.write();
        if index.entries.contains_key(&*key) {
            return false;
        }
        index.order.insert(key.to_string(), 0);
        index.entries.insert(Arc::clone(&key), entry);
        drop(index);
        bucket.cur_size.fetch_add(present_bytes, Ordering::Relaxed);
        let [org, stream_type] = key_labels(&key);
        metrics::QUERY_DISK_BLOCK_CACHE_FILES
            .with_label_values(&[org, stream_type])
            .inc();
        metrics::QUERY_DISK_BLOCK_CACHE_USED_BYTES
            .with_label_values(&[org, stream_type])
            .add(present_bytes as i64);
        true
    }
}

fn short_response(key: &str, expected: u64, got: usize) -> object_store::Error {
    generic(
        "disk block cache",
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("{key}: range response of {got} bytes, expected {expected}"),
        ),
    )
}

struct IoJob {
    path: PathBuf,
    create: bool,
    /// `(offset, bytes)` block runs to write before reading.
    writes: Vec<(u64, Bytes)>,
    reads: Vec<Range<u64>>,
    owner: Option<Arc<dyn Send + Sync>>,
}

impl IoJob {
    fn run(self) -> std::io::Result<Vec<Bytes>> {
        let _owner = self.owner;
        let file = if self.create {
            if let Some(parent) = self.path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&self.path)?
        } else {
            std::fs::File::open(&self.path)?
        };
        for (offset, bytes) in &self.writes {
            pwrite_all(&file, bytes, *offset)?;
        }
        let mut out = Vec::with_capacity(self.reads.len());
        for read in &self.reads {
            let len = usize::try_from(read.end - read.start)
                .map_err(|_| std::io::Error::other("range exceeds address space"))?;
            let mut buf = vec![0u8; len];
            pread_exact(&file, &mut buf, read.start)?;
            out.push(Bytes::from(buf));
        }
        Ok(out)
    }
}

fn unlink_later(entries: Vec<Arc<Entry>>) {
    if entries.is_empty() {
        return;
    }
    let paths: Vec<PathBuf> = entries.iter().map(|e| e.path.clone()).collect();
    tokio::task::spawn_blocking(move || {
        for path in paths {
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => log::warn!("[BlockCache] remove {}: {e}", path.display()),
            }
        }
    });
}

async fn flusher(mut rx: mpsc::UnboundedReceiver<Arc<Entry>>) {
    let mut batch: Vec<Arc<Entry>> = Vec::new();
    while let Some(first) = rx.recv().await {
        batch.push(first);
        tokio::time::sleep(FLUSH_DELAY).await;
        while let Ok(entry) = rx.try_recv() {
            batch.push(entry);
        }
        let work: Vec<(PathBuf, u64, Vec<u8>)> = batch
            .drain(..)
            .filter_map(|entry| {
                entry.queued.store(false, Ordering::Release);
                entry
                    .trailer()
                    .map(|trailer| (entry.path.clone(), entry.trailer_offset(), trailer))
            })
            .collect();
        if work.is_empty() {
            continue;
        }
        let _ = tokio::task::spawn_blocking(move || write_trailers(work)).await;
    }
}

/// Data durability precedes the trailer: a persisted bit implies the block
/// bytes reached the device.
fn write_trailers(work: Vec<(PathBuf, u64, Vec<u8>)>) {
    for (path, offset, trailer) in work {
        let file = match std::fs::OpenOptions::new().write(true).open(&path) {
            Ok(file) => file,
            // Evicted after it was queued.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => {
                log::warn!("[BlockCache] open {} for trailer: {e}", path.display());
                continue;
            }
        };
        if let Err(e) = file
            .sync_data()
            .and_then(|()| pwrite_all(&file, &trailer, offset))
        {
            log::warn!("[BlockCache] write trailer {}: {e}", path.display());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use futures::FutureExt;

    use super::*;

    const BS: u64 = 64 * 1024;

    /// A deterministic "object store": `fetch` returns the clipped range and
    /// counts calls/bytes like S3 would see them.
    struct Remote {
        data: Bytes,
        calls: AtomicUsize,
        bytes: AtomicU64,
    }

    impl Remote {
        fn new(len: usize) -> Arc<Self> {
            let data: Vec<u8> = (0..len).map(|i| (i * 7 % 251) as u8).collect();
            Arc::new(Self {
                data: Bytes::from(data),
                calls: AtomicUsize::new(0),
                bytes: AtomicU64::new(0),
            })
        }

        fn fetcher(self: &Arc<Self>) -> impl Fn(Range<u64>) -> FetchFuture + Sync + use<> {
            let remote = Arc::clone(self);
            move |range: Range<u64>| {
                let remote = Arc::clone(&remote);
                async move {
                    let size = remote.data.len() as u64;
                    if range.start >= size {
                        return Err(object_store::Error::NotFound {
                            path: "remote".into(),
                            source: Box::new(std::io::Error::other("start too large")),
                        });
                    }
                    let end = range.end.min(size);
                    remote.calls.fetch_add(1, Ordering::Relaxed);
                    remote.bytes.fetch_add(end - range.start, Ordering::Relaxed);
                    Ok(Fetched {
                        bytes: remote.data.slice(range.start as usize..end as usize),
                        object_size: size,
                    })
                }
                .boxed()
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::Relaxed)
        }
    }

    fn cache(dir: &Path, max_size: u64) -> Arc<BlockCache> {
        BlockCache::new(dir.to_path_buf(), BS, max_size, BS, "time_lru", 1)
    }

    const KEY: &str = "files/org/logs/stream/2026/10/08/11/a.vix";

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cold_read_fills_aligned_blocks_then_hits() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache(dir.path(), 1 << 30);
        let remote = Remote::new(1 << 20);
        let fetch = remote.fetcher();

        let read = cache
            .read_through(KEY, 100..1000, None, &fetch)
            .await
            .unwrap();
        assert_eq!(read.outcome, Outcome::Miss);
        assert_eq!(read.bytes, remote.data.slice(100..1000));
        assert_eq!(read.object_size, 1 << 20);
        assert_eq!(read.remote_bytes, BS, "one aligned block");
        assert_eq!(remote.calls(), 1);
        assert_eq!(cache.stats(), (1 << 30, BS, 1));

        let read = cache
            .read_through(KEY, 100..1000, None, &fetch)
            .await
            .unwrap();
        assert_eq!(read.outcome, Outcome::Hit);
        assert_eq!(read.bytes, remote.data.slice(100..1000));
        assert_eq!(read.remote_bytes, 0);
        assert_eq!(remote.calls(), 1, "a hit never touches the remote");

        // Straddles the present block and the next one.
        let read = cache
            .read_through(KEY, 60_000..70_000, None, &fetch)
            .await
            .unwrap();
        assert_eq!(read.outcome, Outcome::Partial);
        assert_eq!(read.bytes, remote.data.slice(60_000..70_000));
        assert_eq!(read.remote_bytes, BS);
        assert_eq!(remote.calls(), 2);
        assert_eq!(cache.stats().1, 2 * BS);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn last_block_is_clipped_and_range_end_past_eof_is_clipped() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache(dir.path(), 1 << 30);
        let remote = Remote::new(100_000);
        let fetch = remote.fetcher();

        let read = cache
            .read_through(KEY, 90_000..100_000, None, &fetch)
            .await
            .unwrap();
        assert_eq!(read.bytes, remote.data.slice(90_000..100_000));
        assert_eq!(
            read.remote_bytes,
            100_000 - BS,
            "last block clipped to the object"
        );
        assert_eq!(cache.stats().1, 100_000 - BS);

        let read = cache
            .read_through(KEY, 90_000..200_000, None, &fetch)
            .await
            .unwrap();
        assert_eq!(read.outcome, Outcome::Hit);
        assert_eq!(read.bytes, remote.data.slice(90_000..100_000));

        let err = cache
            .read_through(KEY, 200_000..200_010, None, &fetch)
            .await
            .unwrap_err();
        assert!(matches!(err, object_store::Error::NotFound { .. }), "{err}");
        let empty = cache
            .read_through(KEY, 100_000..100_000, None, &fetch)
            .await
            .unwrap();
        assert!(empty.bytes.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_cold_readers_share_one_entry_and_account_once() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache(dir.path(), 1 << 30);
        let remote = Remote::new(1 << 20);
        let fetch: Arc<dyn Fn(Range<u64>) -> FetchFuture + Send + Sync> =
            Arc::new(remote.fetcher());

        let mut tasks = Vec::new();
        for i in 0..16u64 {
            let cache = Arc::clone(&cache);
            let fetch = Arc::clone(&fetch);
            let remote = Arc::clone(&remote);
            tasks.push(tokio::spawn(async move {
                let range = (i * 20_000)..(i * 20_000 + 50_000);
                let read = cache
                    .read_through(KEY, range.clone(), None, &*fetch)
                    .await
                    .unwrap();
                assert_eq!(
                    read.bytes,
                    remote.data.slice(range.start as usize..range.end as usize)
                );
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        // Union of touched blocks: offsets 0..350_000 -> blocks 0..6 (6 blocks).
        let (_, used, entries) = cache.stats();
        assert_eq!(entries, 1);
        assert_eq!(used, 6 * BS, "overlapping fills never double count");
        let file = std::fs::File::open(dir.path().join(KEY)).unwrap();
        let mut buf = vec![0u8; 6 * BS as usize];
        pread_exact(&file, &mut buf, 0).unwrap();
        assert_eq!(&buf[..], &remote.data[..6 * BS as usize]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn trailer_survives_reload_and_restored_blocks_hit() {
        let dir = tempfile::tempdir().unwrap();
        let remote = Remote::new(300_000);
        let fetch = remote.fetcher();
        {
            let cache = cache(dir.path(), 1 << 30);
            cache.read_through(KEY, 0..10, None, &fetch).await.unwrap();
            cache
                .read_through(KEY, 250_000..300_000, None, &fetch)
                .await
                .unwrap();
            cache.flush().await;
        }
        let calls_before = remote.calls();
        let cache = cache(dir.path(), 1 << 30);
        assert_eq!(cache.load().await.unwrap(), 1);
        let (_, used, entries) = cache.stats();
        assert_eq!(entries, 1);
        // Blocks 0 (full), 3 (full) and the clipped tail block 4.
        assert_eq!(used, BS + BS + (300_000 - 4 * BS));
        let read = cache.read_through(KEY, 0..10, None, &fetch).await.unwrap();
        assert_eq!(read.outcome, Outcome::Hit);
        assert_eq!(read.bytes, remote.data.slice(0..10));
        let read = cache
            .read_through(KEY, 260_000..299_000, None, &fetch)
            .await
            .unwrap();
        assert_eq!(read.outcome, Outcome::Hit);
        assert_eq!(read.bytes, remote.data.slice(260_000..299_000));
        assert_eq!(remote.calls(), calls_before);
        // A region never fetched is still a miss after reload.
        let read = cache
            .read_through(KEY, 100_000..100_100, None, &fetch)
            .await
            .unwrap();
        assert_eq!(read.outcome, Outcome::Miss);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn corrupt_or_foreign_files_are_discarded_on_load() {
        let dir = tempfile::tempdir().unwrap();
        let remote = Remote::new(200_000);
        let fetch = remote.fetcher();
        {
            let cache = cache(dir.path(), 1 << 30);
            cache.read_through(KEY, 0..10, None, &fetch).await.unwrap();
            cache.flush().await;
        }
        // Flip a bitmap byte: checksum mismatch.
        let path = dir.path().join(KEY);
        {
            let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            let offset = 200_000u64.div_ceil(TRAILER_ALIGN) * TRAILER_ALIGN;
            pwrite_all(&file, &[0xFF], offset).unwrap();
        }
        let foreign = dir
            .path()
            .join("files/org/logs/stream/2026/10/08/11/junk.vix");
        std::fs::write(&foreign, b"not a cache file").unwrap();
        let unfinished = dir
            .path()
            .join("files/org/logs/stream/2026/10/08/11/unfinished.vix");
        std::fs::write(&unfinished, vec![0u8; 100]).unwrap();

        let cache = cache(dir.path(), 1 << 30);
        assert_eq!(cache.load().await.unwrap(), 0);
        assert_eq!(cache.stats(), (1 << 30, 0, 0));
        assert!(!path.exists());
        assert!(!foreign.exists());
        assert!(!unfinished.exists());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn different_block_size_discards_entries_on_load() {
        let dir = tempfile::tempdir().unwrap();
        let remote = Remote::new(200_000);
        let fetch = remote.fetcher();
        {
            let cache = cache(dir.path(), 1 << 30);
            cache.read_through(KEY, 0..10, None, &fetch).await.unwrap();
            cache.flush().await;
        }
        let cache = BlockCache::new(dir.path().to_path_buf(), BS * 4, 1 << 30, BS, "time_lru", 1);
        assert_eq!(cache.load().await.unwrap(), 0);
        assert!(!dir.path().join(KEY).exists());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn eviction_releases_oldest_hour_first_and_unlinks() {
        let dir = tempfile::tempdir().unwrap();
        // Room for 3 blocks per bucket.
        let cache = cache(dir.path(), 3 * BS);
        let remote = Remote::new(1 << 20);
        let fetch = remote.fetcher();
        let old = "files/org/logs/stream/2026/10/08/09/old.vix";
        let mid = "files/org/logs/stream/2026/10/08/10/mid.vix";
        let new = "files/org/logs/stream/2026/10/08/11/new.vix";
        for key in [old, mid, new] {
            cache.read_through(key, 0..10, None, &fetch).await.unwrap();
        }
        assert_eq!(cache.stats(), (3 * BS, 3 * BS, 3));
        // Fourth block exceeds the budget: the oldest data hour goes.
        cache
            .read_through(new, BS..BS + 10, None, &fetch)
            .await
            .unwrap();
        let (_, used, entries) = cache.stats();
        assert!(used <= 3 * BS, "used {used}");
        assert_eq!(entries, 2);
        assert!(cache.get_entry(old).is_none());
        assert!(cache.get_entry(mid).is_some() && cache.get_entry(new).is_some());
        // Unlink is asynchronous; wait for it.
        for _ in 0..100 {
            if !dir.path().join(old).exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(!dir.path().join(old).exists());
        // The evicted object comes back cold.
        let read = cache.read_through(old, 0..10, None, &fetch).await.unwrap();
        assert_eq!(read.outcome, Outcome::Miss);
        assert_eq!(read.bytes, remote.data.slice(0..10));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn remove_drops_entry_and_file() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache(dir.path(), 1 << 30);
        let remote = Remote::new(200_000);
        let fetch = remote.fetcher();
        cache.read_through(KEY, 0..10, None, &fetch).await.unwrap();
        cache.remove(KEY);
        assert_eq!(cache.stats(), (1 << 30, 0, 0));
        for _ in 0..100 {
            if !dir.path().join(KEY).exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(!dir.path().join(KEY).exists());
        cache.flush().await;
        assert!(
            !dir.path().join(KEY).exists(),
            "flush must not resurrect a removed entry"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn nearby_missing_runs_coalesce_and_distant_ones_do_not() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache(dir.path(), 1 << 30);
        // 64 blocks; the coalescing gap is 1 MiB = 16 blocks.
        let remote = Remote::new(4 << 20);
        let fetch = remote.fetcher();
        // Present: blocks 0, 2, 4.
        for block in [0u64, 2, 4] {
            cache
                .read_through(KEY, block * BS..block * BS + 1, None, &fetch)
                .await
                .unwrap();
        }
        let calls = remote.calls();
        // Blocks 0..6 -> missing {1}, {3}, {5}, one block apart: one span
        // 1..6 re-fetching 2 and 4.
        let read = cache
            .read_through(KEY, 0..6 * BS, None, &fetch)
            .await
            .unwrap();
        assert_eq!(read.outcome, Outcome::Partial);
        assert_eq!(remote.calls() - calls, 1);
        assert_eq!(read.remote_bytes, 5 * BS);
        assert_eq!(read.bytes, remote.data.slice(0..6 * BS as usize));
        assert_eq!(
            cache.stats().1,
            6 * BS,
            "re-fetched blocks are not double counted"
        );

        // Missing {30} and {50} are 19 blocks apart: two GETs.
        let calls = remote.calls();
        let reads = cache
            .read_through_many(
                KEY,
                &[50 * BS + 5..50 * BS + 15, 30 * BS..30 * BS + 10],
                None,
                &fetch,
            )
            .await
            .unwrap();
        assert_eq!(reads.outcome, Outcome::Miss);
        assert_eq!(remote.calls() - calls, 2);
        assert_eq!(reads.remote_bytes, 2 * BS);
        assert_eq!(reads.bytes.len(), 2, "results follow request order");
        assert_eq!(
            reads.bytes[0],
            remote
                .data
                .slice(50 * BS as usize + 5..50 * BS as usize + 15)
        );
        assert_eq!(
            reads.bytes[1],
            remote.data.slice(30 * BS as usize..30 * BS as usize + 10)
        );
        assert_eq!(cache.stats().1, 8 * BS);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn many_ranges_sharing_a_block_fetch_it_once() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache(dir.path(), 1 << 30);
        let remote = Remote::new(1 << 20);
        let fetch = remote.fetcher();
        // Cold object: four ranges inside block 2, plus a range straddling
        // blocks 7 and 8 and an empty range.
        let ranges = [
            2 * BS + 10..2 * BS + 20,
            2 * BS + 30..2 * BS + 40,
            8 * BS - 100..8 * BS + 100,
            2 * BS + 5..2 * BS + 500,
            2 * BS..2 * BS,
        ];
        let reads = cache
            .read_through_many(KEY, &ranges, None, &fetch)
            .await
            .unwrap();
        assert_eq!(reads.outcome, Outcome::Miss);
        // Cold fetch of block 2 (learns the size), then one span for 7..9
        // (gap from 3 to 7 is within the coalescing distance only if there
        // were a missing run in between; here it is a single run).
        assert_eq!(remote.calls(), 2);
        assert_eq!(reads.remote_bytes, 3 * BS);
        for (range, bytes) in ranges.iter().zip(&reads.bytes) {
            assert_eq!(
                *bytes,
                remote.data.slice(range.start as usize..range.end as usize)
            );
        }
        assert_eq!(cache.stats().1, 3 * BS);

        // Everything present now: a second call is a pure hit.
        let calls = remote.calls();
        let reads = cache
            .read_through_many(KEY, &ranges, None, &fetch)
            .await
            .unwrap();
        assert_eq!(reads.outcome, Outcome::Hit);
        assert_eq!(remote.calls(), calls);
        assert_eq!(reads.remote_bytes, 0);
    }
}
