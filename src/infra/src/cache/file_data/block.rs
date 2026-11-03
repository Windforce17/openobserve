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
//! exactly the block-aligned ranges that range reads miss, write-behind
//! from object storage: a cold read fetches the missing blocks (one GET per
//! contiguous run), assembles every requested range from what is in hand -
//! a range inside one fetched run is a zero-copy slice of it, a warm range
//! is one `pread` of present blocks, a range touching both is stitched into
//! one allocation of its length - and returns; it never reads a fill back
//! out of the file, which still has a hole where that fill has not landed.
//! The fetched runs go to a byte-bounded queue drained by [`FILL_WRITERS`]
//! blocking writers that `pwrite` them into the sparse file and only then
//! mark the blocks present.
//!
//! Back-pressure drops, never blocks: a fill whose bytes would push the
//! queue past `ZO_DISK_CACHE_BLOCK_FILL_QUEUE_SIZE` is not queued, its
//! blocks stay absent and a later reader fetches them again. The earlier
//! write-through design (`.215`, 2026-10-12) pinned prod's gp3 data volume
//! at its 4,096 IOPS (~800 MB/s of fills, util 86-90%) during a cold 7-day
//! point lookup, and the hit path's local preads queued behind those fills
//! took 105-120 ms; now a read waits on no write.
//!
//! On-disk layout: one sparse file per object under `<root>/<key>`, whose
//! logical length is the object's size; bytes `[0, object_size)` mirror the
//! object at identical offsets, holes where blocks were never fetched.
//! There is no per-fill durability machinery and no fsync/fdatasync
//! anywhere in this tier: a block is served only while the in-memory bitmap
//! marks it present, and a restart reconstructs that bitmap from the
//! filesystem's own hole map instead of from anything this tier wrote.
//!
//! Crash safety without fsync: on ext4 `data=ordered` and XFS, a range the
//! filesystem reports as data after a power loss holds the bytes that were
//! written; a write that never reached the disk is reported as a hole (an
//! unwritten or never-allocated extent), not as stale data. A process crash
//! keeps the dirty pages, so it loses nothing. Reconstruction (below) adopts
//! only blocks fully covered by data extents, so a root carried over a node
//! crash or onto another node (a reattached volume) stays trustworthy without
//! knowing whether the previous run shut down cleanly. A loop-device ext4
//! power-cut test adopted 16,521 blocks after 40 cuts, none of them wrong.
//!
//! Lineage: [`init`] parks a `.lineage` marker (layout, block size, kernel
//! boot id) in the root and wipes any root whose layout or block size
//! differs, or whose marker is missing, before the tier is reachable. The
//! boot id is recorded but not compared: it only keeps older binaries,
//! which do compare it, wiping a root that moved to another node. The marker
//! is a plain write, no fsync: a lost marker only causes a wipe, the safe
//! direction.
//!
//! Reconstruction: [`BlockCache::load`] walks the root and, per file, walks
//! `lseek(SEEK_DATA)`/`lseek(SEEK_HOLE)` from offset 0; a block is present
//! iff its whole byte range `[b*bs, min((b+1)*bs, object_size))` is covered
//! by data extents. A partially written block has a hole for its unwritten
//! tail, so it can never be adopted - only a block whose every page is
//! data is. [`init`] self-tests the root once (`.probe`) that the
//! filesystem reports holes at all; if it does not, the root is treated as
//! stale: the tier still works, it just starts empty after every restart.
//!
//! Locking: bucket indexes are touched only for O(1) map operations and
//! never held across IO or `await`; all file IO (`pread`/`pwrite`/unlink)
//! runs on the blocking pool outside the locks, so a slow disk or an
//! eviction sweep cannot stall unrelated readers - unlike the whole-file
//! tier, whose set-triggered GC unlinks files under its bucket write lock.
//! An evicted entry's file is unlinked by path after the entry left the
//! index; until that unlink ran, no entry is created or adopted for the key
//! (its reads are served uncached), so a successor never fills, then serves
//! the holes of, a file that is about to be deleted.
//! A reader opens the file by path too, so it re-checks its entry after the
//! read: bytes read once the entry was evicted may come from a successor's
//! recreated file and are discarded for a remote read.

use std::{
    collections::VecDeque,
    ops::Range,
    path::{Path, PathBuf},
    sync::{
        Arc, OnceLock, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use bytes::Bytes;
use config::{get_config, metrics, utils::hash::sum64_bytes};
use futures::{StreamExt, TryStreamExt, future::BoxFuture};
use hashbrown::HashMap;
use parking_lot::{Condvar, Mutex, RwLock};

use super::CacheStrategy;

/// Name of the lineage marker file inside the block cache root.
const LINEAGE_MARKER: &str = ".lineage";
/// On-disk layout generation recorded in the marker: a root whose marker
/// does not say this (or whose block size differs) is stale and wiped at
/// [`init`].
const LINEAGE_LAYOUT: &str = "block-sparse-v2";
/// Boot id written into the marker on platforms without one (non-Linux).
const NO_BOOT_ID: &str = "none";
/// Missing runs separated by at most this many bytes of present blocks are
/// fetched as one span (re-fetching the present blocks in between): request
/// count over bytes on object storage, like the object_store coalescer.
const COALESCE_GAP_BYTES: u64 = 1024 * 1024;
/// Concurrent fills per request.
const FILL_CONCURRENCY: usize = 10;
/// Blocking-pool threads draining the fill queue. A `pwrite` is a copy into
/// the page cache until the kernel's dirty-page throttling parks the
/// writer, so a few writers keep up with the fetch rate (prod: ~800 MB/s of
/// fills against a 1,024 MB/s volume) while a saturated volume can stall at
/// most this many of the pool's threads; more writers would only queue
/// deeper inside the kernel on the same device.
const FILL_WRITERS: usize = 4;

static GLOBAL: OnceLock<Arc<BlockCache>> = OnceLock::new();

/// The process-wide tier, present when `ZO_DISK_CACHE_BLOCK_ENABLED` and the
/// disk cache are both on and [`init`] ran.
pub fn global() -> Option<&'static Arc<BlockCache>> {
    GLOBAL.get()
}

/// Stop the fill writers of the process-wide tier. They are blocking-pool
/// tasks of the runtime that ran [`init`], and dropping a runtime waits for
/// its blocking tasks; [`GLOBAL`] is never dropped, so [`BlockCache`]'s
/// `Drop` never closes their queue. Each writer finishes the fill it is
/// writing and exits; queued fills are dropped unwritten (their blocks stay
/// holes, which a restart never adopts). Call it once nothing reads through
/// the tier any more, before that runtime is dropped.
pub fn shutdown() {
    if let Some(cache) = GLOBAL.get() {
        cache.fills.close();
    }
}

pub async fn init() -> Result<(), anyhow::Error> {
    let cfg = get_config();
    if !cfg.disk_cache.enabled || !cfg.disk_cache.block_enabled {
        return Ok(());
    }
    let root = PathBuf::from(format!("{}blocks/", cfg.common.data_cache_dir));
    let block_size = cfg.disk_cache.block_size as u64;
    // Synchronously, before `GLOBAL.set` makes the tier reachable: a root
    // with another layout or block size must never serve a byte.
    prepare_root(&root, block_size, read_boot_id().as_deref())?;
    let per_bucket = cfg.disk_cache.block_max_size as u64;
    let cache = BlockCache::new(
        root,
        block_size,
        per_bucket,
        (per_bucket / 10).max(block_size),
        &cfg.disk_cache.cache_strategy,
        cfg.disk_cache.bucket_num,
        cfg.disk_cache.block_fill_queue_size as u64,
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

/// The kernel's boot id, recorded in the lineage marker for diagnostics and
/// for older binaries: Linux exposes it at `/proc/sys/kernel/random/boot_id`.
/// Elsewhere there is none.
#[cfg(target_os = "linux")]
fn read_boot_id() -> Option<String> {
    let id = std::fs::read_to_string("/proc/sys/kernel/random/boot_id").ok()?;
    let id = id.trim();
    (!id.is_empty()).then(|| id.to_string())
}

#[cfg(not(target_os = "linux"))]
fn read_boot_id() -> Option<String> {
    None
}

/// What [`prepare_root`] did to the root: whether it was recreated, and
/// whether the filesystem reports holes (`false` also off unix, where
/// [`BlockCache::load`] adopts nothing anyway).
#[derive(Debug)]
pub struct RootState {
    pub recreated: bool,
    pub hole_reporting: bool,
}

/// Make `root` usable from this process: wipe it when its lineage marker
/// is missing or its layout or block size disagrees, write the current
/// marker (with this boot's id), then self-test that the filesystem reports
/// holes - without that, [`BlockCache::load`] could adopt blocks that were
/// never written.
///
/// Wiping renames the root to `<parent>/<name>.stale-<unix_nanos>` and
/// removes it on the blocking pool, so a big root never blocks startup.
/// Needs a Tokio runtime for that removal.
fn prepare_root(root: &Path, block_size: u64, boot_id: Option<&str>) -> std::io::Result<RootState> {
    let mut recreated = false;
    if root.exists() {
        if let Some(reason) = stale_reason(root, block_size) {
            wipe_root(root, &reason)?;
            recreated = true;
        }
    } else {
        log::info!("[BlockCache] creating block cache root {}", root.display());
    }
    std::fs::create_dir_all(root)?;
    write_lineage(root, block_size, boot_id)?;
    #[cfg(unix)]
    let hole_reporting = match hole_probe(root) {
        Ok(true) => true,
        Ok(false) => {
            log::warn!(
                "[BlockCache] {} does not report holes (SEEK_DATA/SEEK_HOLE); wiping it: the block cache starts empty after every restart",
                root.display()
            );
            wipe_root(root, "the filesystem does not report holes")?;
            recreated = true;
            std::fs::create_dir_all(root)?;
            write_lineage(root, block_size, boot_id)?;
            false
        }
        Err(e) => {
            log::warn!(
                "[BlockCache] hole self-test on {} failed: {e}; wiping it: the block cache starts empty after every restart",
                root.display()
            );
            wipe_root(root, "the hole self-test failed")?;
            recreated = true;
            std::fs::create_dir_all(root)?;
            write_lineage(root, block_size, boot_id)?;
            false
        }
    };
    #[cfg(not(unix))]
    let hole_reporting = false;
    Ok(RootState {
        recreated,
        hole_reporting,
    })
}

/// Why the existing root cannot be reused; `None` when it can be. The boot
/// id is deliberately not compared: hole-map reconstruction already rejects
/// every block a crash could have damaged (see the module docs).
fn stale_reason(root: &Path, block_size: u64) -> Option<String> {
    match read_lineage(root) {
        None => Some("the lineage marker is missing or unreadable".to_string()),
        Some(lineage) if lineage.layout != LINEAGE_LAYOUT => Some(format!(
            "its layout {} is not {LINEAGE_LAYOUT}",
            lineage.layout
        )),
        Some(lineage) if lineage.block_size != block_size => Some(format!(
            "its block size {} is not {block_size}",
            lineage.block_size
        )),
        Some(_) => None,
    }
}

/// Rename `root` aside (it becomes `<parent>/<name>.stale-<unix_nanos>`,
/// removed on the blocking pool) and leave an empty root in its place.
fn wipe_root(root: &Path, reason: &str) -> std::io::Result<bool> {
    if !root.exists() {
        return Ok(false);
    }
    let name = root
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "blocks".to_string());
    let stale = root
        .parent()
        .map(|parent| parent.join(format!("{name}.stale-{}", unix_nanos())))
        .unwrap_or_else(|| PathBuf::from(format!("{name}.stale-{}", unix_nanos())));
    std::fs::rename(root, &stale)?;
    log::info!(
        "[BlockCache] wiped the block cache root ({reason}): {} renamed to {}",
        root.display(),
        stale.display()
    );
    tokio::task::spawn_blocking(move || {
        if let Err(e) = std::fs::remove_dir_all(&stale) {
            log::warn!(
                "[BlockCache] remove stale block cache root {}: {e}",
                stale.display()
            );
        }
    });
    Ok(true)
}

fn unix_nanos() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_nanos())
        .unwrap_or_default()
}

/// The root's recorded lineage: layout and block size. The marker's boot id
/// line is required by the format but its value is not used.
struct Lineage {
    layout: String,
    block_size: u64,
}

/// Parse the lineage marker; `None` when it is missing, torn or malformed -
/// anything this writer did not produce.
fn read_lineage(root: &Path) -> Option<Lineage> {
    let text = std::fs::read_to_string(root.join(LINEAGE_MARKER)).ok()?;
    let mut lines = text.lines();
    let layout = lines.next()?;
    let block_size = lines.next()?.strip_prefix("block_size=")?.parse().ok()?;
    let boot_id = lines.next()?.strip_prefix("boot_id=")?;
    if lines.next().is_some() || boot_id.is_empty() {
        return None;
    }
    Some(Lineage {
        layout: layout.to_string(),
        block_size,
    })
}

/// Write the lineage marker: a plain write, no fsync - a lost marker only
/// causes a wipe next boot, the safe direction.
fn write_lineage(root: &Path, block_size: u64, boot_id: Option<&str>) -> std::io::Result<()> {
    std::fs::write(
        root.join(LINEAGE_MARKER),
        format!(
            "{LINEAGE_LAYOUT}\nblock_size={block_size}\nboot_id={}\n",
            boot_id.unwrap_or(NO_BOOT_ID)
        ),
    )
}

/// Self-test that this root's filesystem keeps holes after writeback: write
/// the second half of a small sparse probe file, flush it, and require the
/// untouched first half to read as a hole and the written half as data. A
/// filesystem that reports everything as data, or that zero-fills small
/// holes when it writes the file back (APFS does for files up to at least
/// 8 MiB), would make [`present_blocks`] adopt blocks that were never
/// written. The probe is small and flushed because cached objects are often
/// small and are read back long after writeback; ext4 and XFS pass it.
/// The whole-file tier reuses it before trusting files left by a crash.
#[cfg(unix)]
pub(super) fn hole_probe(root: &Path) -> std::io::Result<bool> {
    const HALF: u64 = 64 * 1024;
    let path = root.join(".probe");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)?;
    let probe = (|| {
        use std::os::fd::AsRawFd;
        file.set_len(2 * HALF)?;
        pwrite_all(&file, &vec![0u8; HALF as usize], HALF)?;
        file.sync_data()?;
        let fd = file.as_raw_fd();
        Ok(lseek_whence(fd, 0, libc::SEEK_DATA)? == Some(HALF)
            && lseek_whence(fd, HALF, libc::SEEK_HOLE)? == Some(2 * HALF))
    })();
    std::fs::remove_file(&path)?;
    probe
}

/// `lseek(fd, offset, whence)`; `Ok(None)` for ENXIO (nothing left to find).
#[cfg(unix)]
pub(super) fn lseek_whence(
    fd: std::os::fd::RawFd,
    offset: u64,
    whence: libc::c_int,
) -> std::io::Result<Option<u64>> {
    let offset = libc::off_t::try_from(offset)
        .map_err(|_| std::io::Error::other("seek offset overflows off_t"))?;
    let position = unsafe { libc::lseek(fd, offset, whence) };
    if position >= 0 {
        return Ok(Some(u64::try_from(position).unwrap_or(u64::MAX)));
    }
    let error = std::io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ENXIO) {
        Ok(None)
    } else {
        Err(error)
    }
}

/// `(object_size, present bits, present bytes)` of a cache file, found by
/// walking its data extents with `SEEK_DATA`/`SEEK_HOLE` from offset 0: a
/// block is present iff its whole byte range
/// `[b*bs, min((b+1)*bs, object_size))` is covered by data extents, so a
/// torn block (its unwritten tail is a hole) never is.
#[cfg(unix)]
fn present_blocks(file: &std::fs::File, block_size: u64) -> std::io::Result<(u64, Vec<u64>, u64)> {
    use std::os::fd::AsRawFd;
    let object_size = file.metadata()?.len();
    let blocks = block_count(object_size, block_size);
    let mut bits = vec![0u64; blocks.div_ceil(64) as usize];
    let mut present_bytes = 0u64;
    let fd = file.as_raw_fd();
    let mut offset = 0u64;
    while offset < object_size {
        // ENXIO: no data extent at or after `offset`, the rest is holes.
        let Some(data) = lseek_whence(fd, offset, libc::SEEK_DATA)? else {
            break;
        };
        let Some(hole) = lseek_whence(fd, data, libc::SEEK_HOLE)? else {
            break;
        };
        let end = hole.min(object_size);
        // the first block starting inside this extent; a block the extent
        // only partially covers (torn write) is skipped, not adopted
        let mut block = data.div_ceil(block_size);
        while block < blocks {
            let block_end = ((block + 1) * block_size).min(object_size);
            if block_end > end {
                break;
            }
            set_bit(&mut bits, block);
            present_bytes += block_end - block * block_size;
            block += 1;
        }
        offset = end.max(data + 1);
    }
    Ok((object_size, bits, present_bytes))
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

/// How one requested range is assembled; see [`plan_ranges`].
enum Plan {
    /// In hand without touching the cache file: an empty range, a range
    /// wholly inside one fill run (a zero-copy slice of the fetched bytes),
    /// or one spanning only fill runs (copied once into its own buffer).
    Ready(Bytes),
    /// Wholly inside present blocks: one `pread` of the range.
    Disk(Range<u64>),
    /// Touches present blocks and fill runs: one allocation of the range's
    /// length, filled piece by piece in offset order.
    Stitch {
        range: Range<u64>,
        pieces: Vec<Piece>,
    },
}

enum Piece {
    /// Already sliced to the piece out of the fetched run.
    Fill(Bytes),
    /// Present blocks, `pread` from the cache file.
    Disk(Range<u64>),
}

impl Plan {
    /// The whole range when the plan needs the cache file, `None` for
    /// [`Plan::Ready`]; the fallback re-fetches exactly these.
    fn unserved(&self) -> Option<&Range<u64>> {
        match self {
            Self::Ready(_) => None,
            Self::Disk(range) | Self::Stitch { range, .. } => Some(range),
        }
    }
}

/// Plan every requested range from the fetched fill runs and the blocks the
/// entry already has - never from a fill read back out of the file, which
/// is being written behind this read (if at all) and still has a hole where
/// the fill has not landed. Walking a range in offset order, each stretch
/// inside a run's byte span (`run.start*bs .. min(run.end*bs, object_size)`)
/// is a [`Piece::Fill`] sliced from the fetched bytes and each stretch
/// between runs is a [`Piece::Disk`] over present blocks (`missing_runs`
/// guarantees every wanted block is one or the other). One piece is a
/// [`Plan::Ready`] or [`Plan::Disk`]; several pieces that are all fills are
/// copied into one buffer now; a mix is a [`Plan::Stitch`] for the IO.
/// Plans line up with `requested`; empty ranges are `Ready(Bytes::new())`.
fn plan_ranges(
    requested: &[Range<u64>],
    fills: &[(Range<u64>, Bytes)],
    bs: u64,
    object_size: u64,
) -> Vec<Plan> {
    let mut runs: Vec<(Range<u64>, &Bytes)> = fills
        .iter()
        .map(|(run, bytes)| (run.start * bs..(run.end * bs).min(object_size), bytes))
        .collect();
    runs.sort_by_key(|(run, _)| run.start);
    // The next piece of a range starting at `pos`: `runs[*i]` is the first
    // run whose span ends after `pos` (fill runs are disjoint and sorted).
    let next_piece = |i: &mut usize, pos: &mut u64, end: u64| -> Piece {
        match runs.get(*i) {
            Some((run, bytes)) if run.start <= *pos => {
                let piece_end = run.end.min(end);
                let piece = Piece::Fill(
                    bytes.slice((*pos - run.start) as usize..(piece_end - run.start) as usize),
                );
                *i += 1;
                *pos = piece_end;
                piece
            }
            next => {
                let piece_end = next.map_or(end, |(run, _)| run.start.min(end));
                let piece = Piece::Disk(*pos..piece_end);
                *pos = piece_end;
                piece
            }
        }
    };
    let mut plans = Vec::with_capacity(requested.len());
    for r in requested {
        if r.is_empty() {
            plans.push(Plan::Ready(Bytes::new()));
            continue;
        }
        let mut pos = r.start;
        let mut i = runs.partition_point(|(run, _)| run.end <= pos);
        let first = next_piece(&mut i, &mut pos, r.end);
        if pos == r.end {
            plans.push(match first {
                Piece::Fill(bytes) => Plan::Ready(bytes),
                Piece::Disk(range) => Plan::Disk(range),
            });
            continue;
        }
        let mut pieces = vec![first];
        while pos < r.end {
            pieces.push(next_piece(&mut i, &mut pos, r.end));
        }
        if pieces.iter().all(|piece| matches!(piece, Piece::Fill(_))) {
            let mut buf = Vec::with_capacity((r.end - r.start) as usize);
            for piece in pieces {
                if let Piece::Fill(bytes) = piece {
                    buf.extend_from_slice(&bytes);
                }
            }
            plans.push(Plan::Ready(Bytes::from(buf)));
        } else {
            plans.push(Plan::Stitch {
                range: r.clone(),
                pieces,
            });
        }
    }
    plans
}

/// Carry out `plans` against the cache file at `path`: `Ready` passes
/// through (a refcount bump), `Disk` is one `pread` into an exact-size
/// buffer, `Stitch` fills one allocation of the range's length piece by
/// piece. Any error (NotFound = evicted under us) fails the whole read;
/// the caller then re-fetches the unserved ranges.
fn read_plans(path: &Path, plans: &[Plan]) -> std::io::Result<Vec<Bytes>> {
    let file = std::fs::File::open(path)?;
    let len_of = |range: &Range<u64>| {
        usize::try_from(range.end - range.start)
            .map_err(|_| std::io::Error::other("range exceeds address space"))
    };
    plans
        .iter()
        .map(|plan| match plan {
            Plan::Ready(bytes) => Ok(bytes.clone()),
            Plan::Disk(range) => Ok(Bytes::from(super::pread_vec(
                &file,
                range.start,
                len_of(range)?,
            )?)),
            Plan::Stitch { range, pieces } => {
                let mut buf = Vec::with_capacity(len_of(range)?);
                for piece in pieces {
                    match piece {
                        Piece::Fill(bytes) => buf.extend_from_slice(bytes),
                        Piece::Disk(range) => {
                            super::pread_append(&file, &mut buf, range.start, len_of(range)?)?
                        }
                    }
                }
                Ok(Bytes::from(buf))
            }
        })
        .collect()
}

/// One fetched fill on its way to the cache file: the entry it belongs to,
/// its block runs with their bytes, and the admission `owner` of the read
/// that fetched them, held until the write returned.
struct FillJob {
    entry: Arc<Entry>,
    writes: Vec<(Range<u64>, Bytes)>,
    /// Sum of the runs' lengths, the job's share of the queue budget.
    bytes: u64,
    owner: Option<Arc<dyn Send + Sync>>,
}

/// The write-behind queue: fills waiting for the [`FILL_WRITERS`], bounded
/// in bytes. Over budget a fill is dropped rather than making its read
/// wait; the blocks stay absent and a later reader fetches them again.
struct FillQueue {
    budget: u64,
    pending_bytes: AtomicU64,
    state: Mutex<FillQueueState>,
    ready: Condvar,
}

struct FillQueueState {
    jobs: VecDeque<FillJob>,
    closed: bool,
}

impl FillQueue {
    fn new(budget: u64) -> Arc<Self> {
        Arc::new(Self {
            budget,
            pending_bytes: AtomicU64::new(0),
            state: Mutex::new(FillQueueState {
                jobs: VecDeque::new(),
                closed: false,
            }),
            ready: Condvar::new(),
        })
    }

    /// Queue `job`, or drop it (counted) when its bytes would push the
    /// queue over budget. Never blocks.
    fn push(&self, job: FillJob) {
        let bytes = job.bytes;
        let admitted = self
            .pending_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |pending| {
                (pending + bytes <= self.budget).then_some(pending + bytes)
            })
            .is_ok();
        if !admitted {
            metrics::QUERY_DISK_BLOCK_CACHE_FILLS_TOTAL
                .with_label_values(&["dropped"])
                .inc();
            metrics::QUERY_DISK_BLOCK_CACHE_FILL_BYTES_TOTAL
                .with_label_values(&["dropped"])
                .inc_by(bytes);
            return;
        }
        metrics::QUERY_DISK_BLOCK_CACHE_FILL_QUEUE_BYTES.add(bytes as i64);
        self.state.lock().jobs.push_back(job);
        self.ready.notify_one();
    }

    /// Block for the next job; `None` once the queue is closed.
    fn pop(&self) -> Option<FillJob> {
        let mut state = self.state.lock();
        loop {
            if state.closed {
                return None;
            }
            if let Some(job) = state.jobs.pop_front() {
                return Some(job);
            }
            self.ready.wait(&mut state);
        }
    }

    /// Release a finished job's bytes, whatever its outcome.
    fn finish(&self, bytes: u64) {
        self.pending_bytes.fetch_sub(bytes, Ordering::AcqRel);
        metrics::QUERY_DISK_BLOCK_CACHE_FILL_QUEUE_BYTES.sub(bytes as i64);
    }

    fn close(&self) {
        self.state.lock().closed = true;
        self.ready.notify_all();
    }
}

/// A fill writer: drains the queue until it is closed (the cache was
/// dropped). Each job upgrades the cache handle only for its own duration,
/// so a parked writer never keeps the cache alive.
fn fill_writer(queue: Arc<FillQueue>, cache: Weak<BlockCache>) {
    while let Some(job) = queue.pop() {
        let Some(cache) = cache.upgrade() else {
            return;
        };
        cache.write_fill(job);
    }
}

pub struct BlockCache {
    root: PathBuf,
    block_size: u64,
    buckets: Vec<Bucket>,
    fills: Arc<FillQueue>,
}

impl Drop for BlockCache {
    fn drop(&mut self) {
        // Release the writers' blocking-pool threads: a runtime shutdown
        // waits for blocking tasks forever.
        self.fills.close();
    }
}

struct Bucket {
    max_size: u64,
    release_size: u64,
    cur_size: AtomicU64,
    evicting: AtomicBool,
    index: RwLock<Index>,
    /// Keys whose cache file a deferred unlink ([`unlink_later`]) has yet to
    /// remove, with the number of such unlinks. The unlink removes the file
    /// by path, and every entry of a key uses the same path, so no entry is
    /// created or adopted for a key listed here: it would fill a file the
    /// unlink then deletes, and serve the recreated file's holes as zeros.
    /// Keys are added under the `index` write lock, in the critical section
    /// that takes their entry out; lock order is `index` then `unlinking`.
    unlinking: Arc<Mutex<HashMap<Arc<str>, usize>>>,
}

impl Bucket {
    /// Record a pending unlink for each entry's file. Call with this bucket's
    /// index write-locked, in the critical section that removed the entries.
    fn pend_unlinks(&self, entries: &[Arc<Entry>]) {
        for entry in entries {
            self.pend_unlink(&entry.key);
        }
    }

    /// Record one pending unlink of `key`'s file. Call with this bucket's
    /// index write-locked.
    fn pend_unlink(&self, key: &Arc<str>) {
        *self.unlinking.lock().entry(Arc::clone(key)).or_insert(0) += 1;
    }

    /// Whether a deferred unlink of `key`'s file is still pending. Call with
    /// this bucket's index write-locked.
    fn unlink_pending(&self, key: &str) -> bool {
        self.unlinking.lock().contains_key(key)
    }
}

/// Release one pending unlink of `key` once its remove ran.
fn release_unlink(unlinking: &Mutex<HashMap<Arc<str>, usize>>, key: &str) {
    let mut pending = unlinking.lock();
    if let Some(count) = pending.get_mut(key) {
        *count -= 1;
        if *count == 0 {
            pending.remove(key);
        }
    }
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

#[cfg(unix)]
fn pwrite_all(file: &std::fs::File, buf: &[u8], offset: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.write_all_at(buf, offset)
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
    /// config after its per-bucket split; `fill_queue_bytes` bounds the
    /// write-behind queue for the whole cache. Must run inside a Tokio
    /// runtime: the [`FILL_WRITERS`] are blocking-pool tasks that live as
    /// long as the cache.
    pub fn new(
        root: PathBuf,
        block_size: u64,
        max_size: u64,
        release_size: u64,
        strategy: &str,
        bucket_num: usize,
        fill_queue_bytes: u64,
    ) -> Arc<Self> {
        assert!(block_size.is_power_of_two() && block_size >= 512);
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
                unlinking: Arc::new(Mutex::new(HashMap::new())),
            })
            .collect();
        let fills = FillQueue::new(fill_queue_bytes);
        let cache = Arc::new(Self {
            root,
            block_size,
            buckets,
            fills: Arc::clone(&fills),
        });
        for _ in 0..FILL_WRITERS {
            let queue = Arc::clone(&fills);
            let cache = Arc::downgrade(&cache);
            tokio::task::spawn_blocking(move || fill_writer(queue, cache));
        }
        cache
    }

    pub fn block_size(&self) -> u64 {
        self.block_size
    }

    /// Bytes of fetched fills queued for or being written by the fill
    /// writers; zero once every queued fill landed, failed or was skipped.
    pub fn pending_fill_bytes(&self) -> u64 {
        self.fills.pending_bytes.load(Ordering::Acquire)
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
    /// cold reader inserted first. `None` while a deferred unlink of the
    /// key's previous file is pending (see [`Bucket::unlinking`]).
    fn insert_or_get(&self, key: &str, object_size: u64) -> Option<Arc<Entry>> {
        let bucket = self.bucket(key);
        let mut index = bucket.index.write();
        if let Some(existing) = index.entries.get(key) {
            if existing.object_size != object_size {
                log::warn!(
                    "[BlockCache] object {key} size changed {} -> {object_size}; keeping the cached generation",
                    existing.object_size
                );
            }
            return Some(Arc::clone(existing));
        }
        if bucket.unlink_pending(key) {
            return None;
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
        Some(entry)
    }

    /// Serve `requested` of object `key`: present blocks from the cache file,
    /// missing blocks through `fetch` (block-aligned ranges, clipped to the
    /// object once its size is known), the fetched runs queued for the fill
    /// writers behind the read. `requested.end` past the object end is
    /// clipped like `GetRange::as_range`; a start at or past it is
    /// `NotFound`. `owner` is held through the blocking IO, the fill's write
    /// included.
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
    /// two ranges is fetched once), then every range is assembled from the
    /// fetched runs and the cache file's present blocks ([`plan_ranges`]).
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
        let entry = match self.get_entry(key) {
            Some(entry) => entry,
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
                fills.push((run, fetched.bytes));
                match self.insert_or_get(key, object_size) {
                    Some(entry) => entry,
                    None => {
                        // The previous file of this key is still being
                        // unlinked: serve without caching.
                        log::debug!(
                            "[BlockCache] {key} unlink pending, serving remote without caching"
                        );
                        let ranges = clip_ranges(key, requested, object_size)?;
                        let plans = plan_ranges(&ranges, &fills, bs, object_size);
                        return serve_remote(key, plans, object_size, remote_bytes, fetch).await;
                    }
                }
            }
        };
        let object_size = entry.object_size;
        let ranges = clip_ranges(key, requested, object_size)?;
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
        // Every range is assembled from what is in hand - the fetched runs
        // and the file's present blocks - never from a fill read back out of
        // the file: the fills are written behind this read (if at all), so
        // the file still has a hole where one has not landed.
        let plans = plan_ranges(&ranges, &fills, bs, object_size);
        // Hand the fetched runs to the fill writers before the local read so
        // the write overlaps it. Over the queue budget the job is dropped:
        // the blocks stay absent and a later reader fetches them again.
        let fill_bytes: u64 = fills.iter().map(|(_, bytes)| bytes.len() as u64).sum();
        if fill_bytes > 0 {
            self.fills.push(FillJob {
                entry: Arc::clone(&entry),
                writes: fills,
                bytes: fill_bytes,
                owner: owner.clone(),
            });
        }
        let bytes = if plans.iter().any(|plan| plan.unserved().is_some()) {
            let reader = Arc::clone(&entry);
            let (read, plans, evicted) = tokio::task::spawn_blocking(move || {
                let _owner = owner;
                let read = read_plans(&reader.path, &plans);
                // Checked after the read: the path names this entry's file
                // only until its eviction schedules the unlink. Once the
                // unlink ran, a successor may have recreated the file, with
                // holes where this entry's present blocks were.
                let evicted = reader.state.lock().evicted;
                (read, plans, evicted)
            })
            .await
            .map_err(|e| generic("disk block cache", e))?;
            match read {
                Ok(_) if evicted => {
                    log::debug!("[BlockCache] {key} evicted during read, serving remote");
                    return serve_remote(key, plans, object_size, remote_bytes, fetch).await;
                }
                Ok(bytes) => bytes,
                Err(error) => {
                    // Evicted between lookup and IO (NotFound), or the local
                    // disk misbehaved: a cache must never fail the query.
                    // Drop the entry so the next reader starts cold (a queued
                    // fill for it is skipped), serve this one from object
                    // storage without caching it. Ranges already in hand keep
                    // their bytes — only the plans that needed the file are
                    // re-fetched, in request order.
                    if error.kind() == std::io::ErrorKind::NotFound {
                        log::debug!("[BlockCache] {key} evicted during read, serving remote");
                    } else {
                        log::warn!("[BlockCache] {key} local IO failed, serving remote: {error}");
                    }
                    self.drop_entry(&entry);
                    return serve_remote(key, plans, object_size, remote_bytes, fetch).await;
                }
            }
        } else {
            plans
                .into_iter()
                .map(|plan| match plan {
                    Plan::Ready(bytes) => bytes,
                    Plan::Disk(_) | Plan::Stitch { .. } => unreachable!("no plan needs the file"),
                })
                .collect()
        };
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

    /// Drop the entry for `key` (e.g. the whole file was cached and the blocks
    /// are redundant). The unlink runs on the blocking pool.
    pub fn remove(&self, key: &str) {
        let bucket = self.bucket(key);
        let entry = {
            let mut index = bucket.index.write();
            index.order.remove_key(key);
            let entry = index.entries.remove(key);
            if let Some(entry) = &entry {
                bucket.pend_unlinks(std::slice::from_ref(entry));
            }
            entry
        };
        if let Some(entry) = entry {
            let released = self.retire(&entry);
            bucket.cur_size.fetch_sub(released, Ordering::Relaxed);
            unlink_later(bucket, vec![entry]);
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
                    bucket.pend_unlinks(std::slice::from_ref(entry));
                    index.entries.remove(&*entry.key)
                }
                _ => None,
            }
        };
        if let Some(entry) = removed {
            let released = self.retire(&entry);
            bucket.cur_size.fetch_sub(released, Ordering::Relaxed);
            unlink_later(bucket, vec![entry]);
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
            bucket.pend_unlinks(&victims);
        }
        bucket.cur_size.fetch_sub(released, Ordering::Relaxed);
        bucket.evicting.store(false, Ordering::Release);
        log::info!(
            "[BlockCache] evicted {} entries, released {released} bytes, took {} ms",
            victims.len(),
            start.elapsed().as_millis()
        );
        unlink_later(bucket, victims);
    }

    /// Rebuild the index from the cache directory: each file's present
    /// blocks are reconstructed from the filesystem's data extents
    /// ([`present_blocks`]); files with nothing present (empty object, no
    /// data, unreadable) are removed - unless a live entry already owns the
    /// path. Off unix nothing is adopted: the whole tier is unix-only.
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
                if key.split('/').all(|part| !part.starts_with('.')) {
                    // Skip the root's own lineage marker and self-test
                    // probe: a cache key never has a dotfile component.
                    if self.adopt_file(&key, &path) {
                        loaded += 1;
                    }
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
        // Not ours, torn (a hole where a block was half written), written
        // with another block size, or unreadable: remove it.
        let adopted = std::fs::File::open(path).ok().and_then(|file| {
            #[cfg(unix)]
            {
                present_blocks(&file, self.block_size).ok()
            }
            #[cfg(not(unix))]
            {
                let _ = &file;
                None
            }
        });
        let Some((object_size, bits, present_bytes)) = adopted else {
            remove_unowned(bucket, key, path, "invalid");
            return false;
        };
        if object_size == 0 || present_bytes == 0 {
            remove_unowned(bucket, key, path, "empty");
            return false;
        }
        let key: Arc<str> = Arc::from(key);
        let entry = Arc::new(Entry {
            key: Arc::clone(&key),
            path: path.to_path_buf(),
            object_size,
            block_size: self.block_size,
            state: Mutex::new(State {
                bits,
                present_bytes,
                evicted: false,
            }),
        });
        let mut index = bucket.index.write();
        if index.entries.contains_key(&*key) || bucket.unlink_pending(&key) {
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

    /// Run one queued fill on a writer thread: `pwrite` its runs into the
    /// cache file, then make the blocks servable. Skipped outright when the
    /// entry was evicted meanwhile (its file is gone or going). A write
    /// error drops the entry so the next reader starts cold; no retry.
    fn write_fill(&self, job: FillJob) {
        let FillJob {
            entry,
            writes,
            bytes,
            owner,
        } = job;
        let outcome = if entry.state.lock().evicted {
            "skipped"
        } else if let Err(error) = write_runs(&entry, &writes) {
            log::warn!(
                "[BlockCache] {} fill write failed, dropping the entry: {error}",
                entry.key
            );
            self.drop_entry(&entry);
            "failed"
        } else {
            if entry.state.lock().evicted {
                // Evicted while the fill was in flight: the sweeper's unlink
                // may have preceded our create, leaving an orphan file. Remove
                // it only while no newer entry owns the path; the pending
                // unlink then keeps new entries out until the file is gone.
                let bucket = self.bucket(&entry.key);
                let orphan = {
                    let index = bucket.index.write();
                    let orphan = !index.entries.contains_key(&*entry.key);
                    if orphan {
                        bucket.pend_unlinks(std::slice::from_ref(&entry));
                    }
                    orphan
                };
                if orphan {
                    unlink_later(bucket, vec![Arc::clone(&entry)]);
                }
            }
            let added: u64 = writes
                .iter()
                .map(|(run, _)| entry.mark_present(run.clone()))
                .sum();
            if added > 0 {
                let bucket = self.bucket(&entry.key);
                let cur = bucket.cur_size.fetch_add(added, Ordering::Relaxed) + added;
                let [org, stream_type] = key_labels(&entry.key);
                metrics::QUERY_DISK_BLOCK_CACHE_USED_BYTES
                    .with_label_values(&[org, stream_type])
                    .add(added as i64);
                if cur > bucket.max_size {
                    self.evict(bucket, bucket.release_size.max(added));
                }
            }
            "written"
        };
        drop(owner);
        metrics::QUERY_DISK_BLOCK_CACHE_FILLS_TOTAL
            .with_label_values(&[outcome])
            .inc();
        metrics::QUERY_DISK_BLOCK_CACHE_FILL_BYTES_TOTAL
            .with_label_values(&[outcome])
            .inc_by(bytes);
        // Last: a zero `pending_fill_bytes` means every queued fill is fully
        // accounted (and any orphan unlink is already scheduled).
        self.fills.finish(bytes);
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

/// Clip `requested` to the object like `GetRange::as_range`; a start at or
/// past the end of a non-empty range is `NotFound`.
fn clip_ranges(
    key: &str,
    requested: &[Range<u64>],
    object_size: u64,
) -> object_store::Result<Vec<Range<u64>>> {
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
    Ok(ranges)
}

/// Serve `plans` without the cache file: ranges already in hand keep their
/// bytes, every range that needed the file is fetched as is, in request
/// order. `remote_bytes` counts what was fetched before. A miss.
async fn serve_remote(
    key: &str,
    plans: Vec<Plan>,
    object_size: u64,
    mut remote_bytes: u64,
    fetch: &(dyn Fn(Range<u64>) -> FetchFuture + Sync),
) -> object_store::Result<BlockReads> {
    // Zero-length ranges are always `Ready`, so every fetch here is a valid
    // request.
    let fetches: Vec<FetchFuture> = plans
        .iter()
        .filter_map(|plan| plan.unserved().map(|range| fetch(range.clone())))
        .collect();
    let mut results = futures::stream::iter(fetches)
        .buffered(FILL_CONCURRENCY)
        .try_collect::<Vec<Fetched>>()
        .await?
        .into_iter();
    let mut bytes = Vec::with_capacity(plans.len());
    for plan in plans {
        let range = match plan {
            Plan::Ready(ready) => {
                bytes.push(ready);
                continue;
            }
            Plan::Disk(range) | Plan::Stitch { range, .. } => range,
        };
        let want = range.end - range.start;
        let fetched = results.next().ok_or_else(|| short_response(key, want, 0))?;
        let len = fetched.bytes.len() as u64;
        if len != want {
            return Err(short_response(key, want, len as usize));
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
    Ok(BlockReads {
        bytes,
        object_size,
        outcome: Outcome::Miss,
        remote_bytes,
    })
}

/// Write `writes` (block runs, bytes) into `entry`'s cache file, creating
/// it if needed: the file mirrors the object exactly, its logical length is
/// the object's size, so every never-fetched offset is a hole - which is
/// what [`present_blocks`] reads back. The file may also be gone (unlinked
/// by an eviction sweep that raced the lookup); it is recreated and the
/// caller's orphan check cleans up.
fn write_runs(entry: &Entry, writes: &[(Range<u64>, Bytes)]) -> std::io::Result<()> {
    if let Some(parent) = entry.path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(&entry.path)?;
    file.set_len(entry.object_size)?;
    for (run, bytes) in writes {
        pwrite_all(&file, bytes, run.start * entry.block_size)?;
    }
    Ok(())
}

/// Remove a file found at startup that no entry owns (`what` names why it
/// is not adopted). Startup loading runs alongside queries, so the check and
/// the remove go through the pending-unlink guard like an eviction: a cold
/// reader could otherwise insert an entry for the key and fill this very
/// file between them, then serve the holes of the file recreated after it.
fn remove_unowned(bucket: &Bucket, key: &str, path: &Path, what: &str) {
    let key: Arc<str> = {
        let index = bucket.index.write();
        if index.entries.contains_key(key) {
            return;
        }
        let key: Arc<str> = Arc::from(key);
        bucket.pend_unlink(&key);
        key
    };
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => log::warn!(
            "[BlockCache] remove {what} cache file {}: {e}",
            path.display()
        ),
    }
    release_unlink(&bucket.unlinking, &key);
}

/// Remove the cache files of `entries` on the blocking pool, then release
/// their keys from `bucket`'s pending unlinks. The caller registered them
/// with [`Bucket::pend_unlinks`] in the critical section that took them out
/// of the index, so no new entry for these keys exists until this runs.
fn unlink_later(bucket: &Bucket, entries: Vec<Arc<Entry>>) {
    if entries.is_empty() {
        return;
    }
    let unlinking = Arc::clone(&bucket.unlinking);
    let files: Vec<(Arc<str>, PathBuf)> = entries
        .iter()
        .map(|e| (Arc::clone(&e.key), e.path.clone()))
        .collect();
    tokio::task::spawn_blocking(move || {
        for (key, path) in files {
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => log::warn!("[BlockCache] remove {}: {e}", path.display()),
            }
            // Released after a failed remove too: a file left in place keeps
            // the object's own bytes, and a new entry serves only the blocks
            // it writes itself.
            release_unlink(&unlinking, &key);
        }
    });
}

#[cfg(test)]
mod tests {
    use std::{sync::atomic::AtomicUsize, time::Duration};

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
            // A fast repeating pattern with the same 251-byte cycle as the
            // one-byte-at-a-time form it replaces.
            let cycle: Vec<u8> = (0..251u32).map(|i| (i * 7 % 251) as u8).collect();
            let mut data = Vec::with_capacity(len);
            while data.len() + cycle.len() <= len {
                data.extend_from_slice(&cycle);
            }
            data.extend_from_slice(&cycle[..len % cycle.len()]);
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

    /// Prod's default fill queue budget; big enough that no test fill is
    /// dropped unless it asks for a tiny budget.
    const FILL_BUDGET: u64 = 256 << 20;

    /// `prepare_root` is the [`init`] seam: tests inject a fixed boot id so
    /// they never touch `/proc`.
    fn cache_with_boot_id(dir: &Path, max_size: u64, boot_id: &str) -> Arc<BlockCache> {
        prepare_root(dir, BS, Some(boot_id)).unwrap();
        BlockCache::new(
            dir.to_path_buf(),
            BS,
            max_size,
            BS,
            "time_lru",
            1,
            FILL_BUDGET,
        )
    }

    fn cache_with_block_size(dir: &Path, max_size: u64, bs: u64) -> Arc<BlockCache> {
        prepare_root(dir, bs, Some("test-boot")).unwrap();
        BlockCache::new(
            dir.to_path_buf(),
            bs,
            max_size,
            bs,
            "time_lru",
            1,
            FILL_BUDGET,
        )
    }

    /// A cache whose fill queue admits at most `fill_budget` bytes.
    fn cache_with_fill_budget(dir: &Path, max_size: u64, fill_budget: u64) -> Arc<BlockCache> {
        prepare_root(dir, BS, Some("test-boot")).unwrap();
        BlockCache::new(
            dir.to_path_buf(),
            BS,
            max_size,
            BS,
            "time_lru",
            1,
            fill_budget,
        )
    }

    fn cache(dir: &Path, max_size: u64) -> Arc<BlockCache> {
        cache_with_boot_id(dir, max_size, "test-boot")
    }

    /// Wait until every queued fill landed (or was skipped/failed): a read
    /// returns before its fill is written, so a test that then expects a
    /// hit, accounted bytes or the file's contents settles the queue first.
    /// Reload adoption needs a filesystem that keeps holes after writeback
    /// (ext4, XFS). APFS zero-fills small holes, so there the cache wipes its
    /// root instead and these tests are skipped; they run on Linux.
    fn keeps_holes(dir: &Path) -> bool {
        let keeps = hole_probe(dir).unwrap();
        // Linux test filesystems (ext4, XFS, overlayfs, tmpfs) keep holes; a
        // failing probe there is a regression, not a reason to skip.
        #[cfg(target_os = "linux")]
        assert!(keeps, "{} must keep holes after writeback", dir.display());
        if !keeps {
            eprintln!(
                "skipped: {} does not keep holes after writeback",
                dir.display()
            );
        }
        keeps
    }

    async fn settle(cache: &BlockCache) {
        for _ in 0..1000 {
            if cache.pending_fill_bytes() == 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("fill queue did not drain");
    }

    /// Wait for an asynchronous unlink.
    async fn wait_gone(path: &Path) {
        for _ in 0..200 {
            if !path.exists() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("{} still exists", path.display());
    }

    const KEY: &str = "files/org/logs/stream/2026/10/08/11/a.vix";

    /// The server keeps the tier in [`GLOBAL`], so it outlives the runtime
    /// that owns its fill writers: that runtime can only shut down after
    /// [`shutdown`] closes the queue. A fill written before stays served.
    #[test]
    fn closing_the_fill_queue_lets_the_owning_runtime_shut_down() {
        let dir = tempfile::tempdir().unwrap();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let remote = Remote::new(1 << 20);
        let fetch = remote.fetcher();
        let cache = rt.block_on(async {
            let cache = cache(dir.path(), 1 << 30);
            cache
                .read_through(KEY, 0..1000, None, &fetch)
                .await
                .unwrap();
            settle(&cache).await;
            cache
        });

        cache.fills.close();
        let (dropped_tx, dropped_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            drop(rt);
            dropped_tx.send(()).ok();
        });
        assert!(
            dropped_rx.recv_timeout(Duration::from_secs(10)).is_ok(),
            "dropping the runtime still waits on the fill writers"
        );

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let read = rt
            .block_on(cache.read_through(KEY, 0..1000, None, &fetch))
            .unwrap();
        assert_eq!(read.outcome, Outcome::Hit);
        assert_eq!(read.bytes, remote.data.slice(0..1000));
        assert_eq!(remote.calls(), 1);
    }

    /// An eviction unlinks the victim's file later, off the index lock, by
    /// path - and the next entry for the key uses the same path. Here that
    /// unlink is held back until the successor entry has filled the old
    /// inode; a later fill then recreates the path. The successor must still
    /// serve the object's bytes, never the recreated file's holes (zeros).
    #[test]
    fn a_late_unlink_of_an_evicted_entry_never_serves_holes_to_its_successor() {
        let dir = tempfile::tempdir().unwrap();
        // The fill writers park on all blocking threads but one; holding that
        // last one queues the eviction's unlink until the test releases it.
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(FILL_WRITERS + 1)
            .enable_all()
            .build()
            .unwrap();
        let size = 64 * BS;
        let tail = size - 12 * BS..size;
        let tail_bytes =
            |remote: &Remote| remote.data.slice(tail.start as usize..tail.end as usize);
        let remote = Remote::new(size as usize);
        let fetch = remote.fetcher();
        rt.block_on(async {
            let cache = cache(dir.path(), 1 << 30);
            let path = cache.path_for(KEY);
            cache
                .read_through(KEY, tail.clone(), None, &fetch)
                .await
                .unwrap();
            settle(&cache).await;

            let (started_tx, started_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
            let blocker = tokio::task::spawn_blocking(move || {
                started_tx.send(()).ok();
                release_rx.recv().ok();
            });
            started_rx.await.unwrap();
            cache.evict(cache.bucket(KEY), u64::MAX);
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert!(
                path.exists(),
                "the unlink must still be queued for this test to reproduce the race"
            );

            let read = cache
                .read_through(KEY, tail.clone(), None, &fetch)
                .await
                .unwrap();
            assert_eq!(read.bytes, tail_bytes(&remote));
            settle(&cache).await;
            release_tx.send(()).unwrap();
            blocker.await.unwrap();
            wait_gone(&path).await;

            let head = cache.read_through(KEY, 0..BS, None, &fetch).await.unwrap();
            assert_eq!(head.bytes, remote.data.slice(0..BS as usize));
            settle(&cache).await;

            let read = cache
                .read_through(KEY, tail.clone(), None, &fetch)
                .await
                .unwrap();
            assert!(
                read.bytes == tail_bytes(&remote),
                "tail served from the cache does not match the object ({} of {} bytes are zero)",
                read.bytes.iter().filter(|b| **b == 0).count(),
                read.bytes.len()
            );
            // Once the unlink is done the key caches again.
            settle(&cache).await;
            let again = cache
                .read_through(KEY, tail.clone(), None, &fetch)
                .await
                .unwrap();
            assert_eq!(again.outcome, Outcome::Hit);
            assert_eq!(again.bytes, tail_bytes(&remote));
        });
    }

    /// A reader that found blocks present opens the cache file by path only
    /// after its remote fetches. If its entry is evicted meanwhile, the
    /// unlink runs and a successor recreates the file, the path names a file
    /// with holes where the reader's present blocks were. The reader must
    /// still return the object's bytes, never those holes (zeros).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_reader_evicted_mid_read_never_serves_a_successors_holes() {
        let dir = tempfile::tempdir().unwrap();
        let size = 64 * BS;
        let tail = size - 12 * BS..size;
        let remote = Remote::new(size as usize);
        let fetch = remote.fetcher();
        let cache = cache(dir.path(), 1 << 30);
        let path = cache.path_for(KEY);
        cache
            .read_through(KEY, tail.clone(), None, &fetch)
            .await
            .unwrap();
        settle(&cache).await;

        // The reader's fetch of block 0 parks until released: its tail is
        // planned from the file, and the file read waits behind the fetch.
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let gated = {
            let inner = remote.fetcher();
            let (entered, release) = (Arc::clone(&entered), Arc::clone(&release));
            move |range: Range<u64>| -> FetchFuture {
                let fetched = inner(range.clone());
                let (entered, release) = (Arc::clone(&entered), Arc::clone(&release));
                async move {
                    if range.start == 0 {
                        entered.add_permits(1);
                        release.acquire().await.unwrap().forget();
                    }
                    fetched.await
                }
                .boxed()
            }
        };
        let reader = {
            let cache = Arc::clone(&cache);
            let ranges = [0..BS, tail.clone()];
            tokio::spawn(async move { cache.read_through_many(KEY, &ranges, None, &gated).await })
        };
        entered.acquire().await.unwrap().forget();

        cache.evict(cache.bucket(KEY), u64::MAX);
        wait_gone(&path).await;
        for _ in 0..200 {
            if !cache.bucket(KEY).unlink_pending(KEY) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            !cache.bucket(KEY).unlink_pending(KEY),
            "the eviction's unlink did not finish"
        );
        // A successor recreates the file with one block of its own.
        cache
            .read_through(KEY, 30 * BS..31 * BS, None, &fetch)
            .await
            .unwrap();
        settle(&cache).await;
        assert!(path.exists(), "the successor must have recreated the file");

        release.add_permits(1);
        let read = reader.await.unwrap().unwrap();
        assert_eq!(read.bytes[0], remote.data.slice(0..BS as usize));
        let want = remote.data.slice(tail.start as usize..tail.end as usize);
        assert!(
            read.bytes[1] == want,
            "tail does not match the object ({} of {} bytes are zero)",
            read.bytes[1].iter().filter(|b| **b == 0).count(),
            read.bytes[1].len()
        );
    }

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
        // The fill is write-behind: presence and accounting follow the write.
        settle(&cache).await;
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
        settle(&cache).await;
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
        settle(&cache).await;
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
        settle(&cache).await;
        // Union of touched blocks: offsets 0..350_000 -> blocks 0..6 (6 blocks).
        let (_, used, entries) = cache.stats();
        assert_eq!(entries, 1);
        assert_eq!(used, 6 * BS, "overlapping fills never double count");
        let file = std::fs::File::open(dir.path().join(KEY)).unwrap();
        let buf = crate::cache::file_data::pread_vec(&file, 0, 6 * BS as usize).unwrap();
        assert_eq!(&buf[..], &remote.data[..6 * BS as usize]);
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fill_survives_reload_and_restored_blocks_hit() {
        // Hole geometry that survives a close(): APFS back-fills holes
        // smaller than ~16 MiB when a write lands past them (prod's ext4
        // never does), so the gap between the two fetched blocks is
        // 32 MiB (512 blocks) here.
        let dir = tempfile::tempdir().unwrap();
        if !keeps_holes(dir.path()) {
            return;
        }
        let gap = 512 * BS;
        let tail = 12345; // a short last block
        let object_size = 2 * gap + tail as u64;
        let remote = Remote::new(object_size as usize);
        let fetch = remote.fetcher();
        {
            let cache = cache(dir.path(), 1 << 30);
            cache.read_through(KEY, 0..10, None, &fetch).await.unwrap();
            cache
                .read_through(KEY, gap..gap + 10, None, &fetch)
                .await
                .unwrap();
            cache
                .read_through(KEY, object_size - 10..object_size, None, &fetch)
                .await
                .unwrap();
            // Let the write-behind fills land before the "process" ends.
            settle(&cache).await;
        }
        // The fills only ever put data in the page cache; a second process
        // reconstructs presence from the filesystem's own extents.
        let calls_before = remote.calls();
        let cache = cache(dir.path(), 1 << 30);
        assert_eq!(cache.load().await.unwrap(), 1);
        let (_, used, entries) = cache.stats();
        assert_eq!(entries, 1);
        // Block 0, block 512, and the clipped short tail block 1024.
        assert_eq!(used, BS + BS + tail as u64);
        let read = cache.read_through(KEY, 0..10, None, &fetch).await.unwrap();
        assert_eq!(read.outcome, Outcome::Hit);
        assert_eq!(read.bytes, remote.data.slice(0..10));
        let read = cache
            .read_through(KEY, gap..gap + 10, None, &fetch)
            .await
            .unwrap();
        assert_eq!(read.outcome, Outcome::Hit);
        assert_eq!(
            read.bytes,
            remote.data.slice(gap as usize..gap as usize + 10)
        );
        assert_eq!(remote.calls(), calls_before);
        // A region never fetched is still a miss after reload.
        let read = cache
            .read_through(KEY, 2 * BS..2 * BS + 100, None, &fetch)
            .await
            .unwrap();
        assert_eq!(read.outcome, Outcome::Miss);
        settle(&cache).await;
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn partially_written_block_is_absent_but_neighbours_present() {
        // A 32 MiB block size: the torn block's unwritten tail is then a
        // 32 MiB hole, which APFS keeps (it back-fills smaller holes on
        // close; prod's ext4 keeps any hole).
        let dir = tempfile::tempdir().unwrap();
        if !keeps_holes(dir.path()) {
            return;
        }
        let bs = 32 * 1024 * 1024;
        let object_size = 3 * bs;
        let remote = Remote::new(object_size as usize);
        let fetch = remote.fetcher();
        let cache = cache_with_block_size(dir.path(), 1 << 30, bs);
        cache.read_through(KEY, 0..10, None, &fetch).await.unwrap();
        cache
            .read_through(KEY, 2 * bs..2 * bs + 10, None, &fetch)
            .await
            .unwrap();
        settle(&cache).await;
        // Tear block 1: write only its head into the cache file, its tail
        // stays a hole.
        let path = dir.path().join(KEY);
        {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            pwrite_all(&file, &vec![0u8; 4096], bs).unwrap();
        }
        let file = std::fs::File::open(&path).unwrap();
        let (found_size, bits, present_bytes) = present_blocks(&file, bs).unwrap();
        assert_eq!(found_size, object_size);
        assert!(is_set(&bits, 0) && is_set(&bits, 2));
        assert!(!is_set(&bits, 1), "a torn block must not be present");
        assert_eq!(present_bytes, 2 * bs);

        // A later read refetches only the torn block.
        let calls = remote.calls();
        let read = cache
            .read_through(KEY, bs + 10..bs + 20, None, &fetch)
            .await
            .unwrap();
        assert_eq!(read.outcome, Outcome::Miss);
        assert_eq!(remote.calls() - calls, 1, "only the torn block refetches");
        assert_eq!(
            read.bytes,
            remote.data.slice(bs as usize + 10..bs as usize + 20)
        );
        settle(&cache).await;

        // And `load` on a fresh cache adopts the same shape.
        let reloaded = cache_with_block_size(dir.path(), 1 << 30, bs);
        assert_eq!(reloaded.load().await.unwrap(), 1);
        let (_, used, _) = reloaded.stats();
        assert_eq!(used, 3 * bs, "the refetched torn block is now whole");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn empty_and_zero_length_files_are_discarded_on_load() {
        let dir = tempfile::tempdir().unwrap();
        if !keeps_holes(dir.path()) {
            return;
        }
        let cache = cache(dir.path(), 1 << 30);
        // A sparse file with no data: nothing to adopt.
        let empty = dir
            .path()
            .join("files/org/logs/stream/2026/10/08/11/empty.vix");
        std::fs::create_dir_all(empty.parent().unwrap()).unwrap();
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&empty)
            .unwrap()
            .set_len(100)
            .unwrap();
        let zero = dir
            .path()
            .join("files/org/logs/stream/2026/10/08/11/zero.vix");
        std::fs::write(&zero, b"").unwrap();

        assert_eq!(cache.load().await.unwrap(), 0);
        assert_eq!(cache.stats(), (1 << 30, 0, 0));
        assert!(!empty.exists());
        assert!(!zero.exists());
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn short_last_block_adopts_with_exact_present_bytes() {
        let dir = tempfile::tempdir().unwrap();
        if !keeps_holes(dir.path()) {
            return;
        }
        // 512 blocks of gap, then a short tail block (32 KiB): the last
        // block is clipped to the object size.
        let gap = 512 * BS;
        let object_size = gap + BS / 2;
        let remote = Remote::new(object_size as usize);
        let fetch = remote.fetcher();
        {
            let cache = cache(dir.path(), 1 << 30);
            cache.read_through(KEY, 0..10, None, &fetch).await.unwrap();
            cache
                .read_through(KEY, gap..gap + 10, None, &fetch)
                .await
                .unwrap();
            settle(&cache).await;
        }
        let cache = cache(dir.path(), 1 << 30);
        assert_eq!(cache.load().await.unwrap(), 1);
        let (_, used, entries) = cache.stats();
        assert_eq!(entries, 1);
        assert_eq!(used, BS + BS / 2, "the short tail block is exact");
        let read = cache
            .read_through(KEY, gap..object_size, None, &fetch)
            .await
            .unwrap();
        assert_eq!(read.outcome, Outcome::Hit);
        assert_eq!(
            read.bytes,
            remote.data.slice(gap as usize..object_size as usize)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn boot_id_change_keeps_the_root() {
        let dir = tempfile::tempdir().unwrap();
        if !keeps_holes(dir.path()) {
            return;
        }
        let remote = Remote::new(200_000);
        let fetch = remote.fetcher();
        let root = dir.path().join("blocks");
        let before = {
            let cache = cache_with_boot_id(&root, 1 << 30, "boot-a");
            cache.read_through(KEY, 0..10, None, &fetch).await.unwrap();
            settle(&cache).await;
            assert!(root.join(KEY).exists());
            cache.stats()
        };
        // A volume reattached on another node (or after a node reboot) sees a
        // different kernel boot id. Hole-map reconstruction is what makes the
        // blocks trustworthy, so the root and its blocks are kept.
        let cache = cache_with_boot_id(&root, 1 << 30, "boot-b");
        assert_eq!(cache.load().await.unwrap(), 1);
        assert!(root.join(KEY).exists(), "the cached file is kept");
        assert_eq!(cache.stats(), before, "every cached block is adopted again");
        let read = cache.read_through(KEY, 0..10, None, &fetch).await.unwrap();
        assert_eq!(read.outcome, Outcome::Hit);
        assert_eq!(read.bytes, remote.data.slice(0..10));
        let marker = std::fs::read_to_string(root.join(LINEAGE_MARKER)).unwrap();
        assert!(
            marker.ends_with("boot_id=boot-b\n"),
            "the marker records the current boot for older binaries: {marker}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn marker_with_other_block_size_wipes_the_root() {
        let dir = tempfile::tempdir().unwrap();
        let remote = Remote::new(200_000);
        let fetch = remote.fetcher();
        let root = dir.path().join("blocks");
        {
            let cache = cache_with_boot_id(&root, 1 << 30, "boot-a");
            cache.read_through(KEY, 0..10, None, &fetch).await.unwrap();
            // A fill landing after the wipe would recreate the file.
            settle(&cache).await;
        }
        // A block-size change remaps every hole boundary: stale.
        let state = prepare_root(&root, BS * 4, Some("boot-a")).unwrap();
        assert!(state.recreated);
        assert!(!root.join(KEY).exists());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn missing_marker_wipes_the_root() {
        let dir = tempfile::tempdir().unwrap();
        let remote = Remote::new(200_000);
        let fetch = remote.fetcher();
        let root = dir.path().join("blocks");
        {
            let cache = cache_with_boot_id(&root, 1 << 30, "boot-a");
            cache.read_through(KEY, 0..10, None, &fetch).await.unwrap();
            settle(&cache).await;
        }
        std::fs::remove_file(root.join(LINEAGE_MARKER)).unwrap();
        let state = prepare_root(&root, BS, Some("boot-a")).unwrap();
        assert!(state.recreated);
        assert!(!root.join(KEY).exists());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unknown_boot_id_keeps_the_root() {
        let dir = tempfile::tempdir().unwrap();
        if !keeps_holes(dir.path()) {
            return;
        }
        let remote = Remote::new(200_000);
        let fetch = remote.fetcher();
        let root = dir.path().join("blocks");
        {
            let cache = cache_with_boot_id(&root, 1 << 30, "boot-a");
            cache.read_through(KEY, 0..10, None, &fetch).await.unwrap();
            settle(&cache).await;
        }
        // A platform without a boot id reuses the root like any other.
        let state = prepare_root(&root, BS, None).unwrap();
        assert!(!state.recreated, "a None boot id keeps the root");
        assert!(root.join(KEY).exists());
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
        settle(&cache).await;
        assert_eq!(cache.stats(), (3 * BS, 3 * BS, 3));
        // Fourth block exceeds the budget: the oldest data hour goes - the
        // writer that lands the fill runs the sweep.
        cache
            .read_through(new, BS..BS + 10, None, &fetch)
            .await
            .unwrap();
        settle(&cache).await;
        let (_, used, entries) = cache.stats();
        assert!(used <= 3 * BS, "used {used}");
        assert_eq!(entries, 2);
        assert!(cache.get_entry(old).is_none());
        assert!(cache.get_entry(mid).is_some() && cache.get_entry(new).is_some());
        wait_gone(&dir.path().join(old)).await;
        // The evicted object comes back cold.
        let read = cache.read_through(old, 0..10, None, &fetch).await.unwrap();
        assert_eq!(read.outcome, Outcome::Miss);
        assert_eq!(read.bytes, remote.data.slice(0..10));
        settle(&cache).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn remove_drops_entry_and_file() {
        let dir = tempfile::tempdir().unwrap();
        if !keeps_holes(dir.path()) {
            return;
        }
        let cache = cache(dir.path(), 1 << 30);
        let remote = Remote::new(200_000);
        let fetch = remote.fetcher();
        cache.read_through(KEY, 0..10, None, &fetch).await.unwrap();
        settle(&cache).await;
        cache.remove(KEY);
        assert_eq!(cache.stats(), (1 << 30, 0, 0));
        wait_gone(&dir.path().join(KEY)).await;
        // A removed file is gone for good: nothing rewrites it later.
        let reloaded = cache_with_boot_id(dir.path(), 1 << 30, "test-boot");
        assert_eq!(reloaded.load().await.unwrap(), 0);
        assert!(!dir.path().join(KEY).exists());
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
        settle(&cache).await;
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
        settle(&cache).await;
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
        settle(&cache).await;
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
        settle(&cache).await;
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cold_miss_ranges_are_served_zero_copy_from_the_fills() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache(dir.path(), 1 << 30);
        let remote = Remote::new(1 << 20);
        let fetch = remote.fetcher();
        // Cold object, three non-empty ranges inside blocks 1 and 2 plus an
        // empty one. Deterministically, the cold probe fetches run 1..2 and
        // the missing-run pass fetches 2..3, so every non-empty range lies
        // wholly inside one fill run.
        let ranges = [
            BS + 10..BS + 100,
            2 * BS + 30..2 * BS + 60,
            BS + 500..BS + 600,
            2 * BS..2 * BS,
        ];
        // The exact fills the flow above builds: prove they leave no range
        // for the file — the read touches no disk for this request.
        let plans = plan_ranges(
            &ranges,
            &[
                (1..2, remote.data.slice(BS as usize..2 * BS as usize)),
                (2..3, remote.data.slice(2 * BS as usize..3 * BS as usize)),
            ],
            BS,
            remote.data.len() as u64,
        );
        assert_eq!(plans.len(), ranges.len());
        assert!(
            plans.iter().all(|plan| plan.unserved().is_none()),
            "no file path for covered ranges"
        );

        let reads = cache
            .read_through_many(KEY, &ranges, None, &fetch)
            .await
            .unwrap();
        assert_eq!(reads.outcome, Outcome::Miss);
        assert_eq!(reads.remote_bytes, 2 * BS);
        assert_eq!(reads.bytes.len(), ranges.len());
        for (range, bytes) in ranges.iter().zip(&reads.bytes) {
            assert_eq!(
                *bytes,
                remote.data.slice(range.start as usize..range.end as usize)
            );
            if range.is_empty() {
                continue;
            }
            // Pointer identity: the served bytes point into the fetched
            // buffer. `Remote::data` is one live Vec-backed allocation and a
            // pread would land in a fresh one, so this proves the range was
            // served from the fill, not read back from the file.
            let offset = bytes.as_ptr() as usize - remote.data.as_ptr() as usize;
            assert!(
                offset < remote.data.len(),
                "served bytes must point into the fetched fill"
            );
        }
        // The write-behind fill still puts the fetched blocks in the file.
        settle(&cache).await;
        let file = std::fs::File::open(dir.path().join(KEY)).unwrap();
        let buf = crate::cache::file_data::pread_vec(&file, BS, 2 * BS as usize).unwrap();
        assert_eq!(&buf[..], &remote.data[BS as usize..3 * BS as usize]);
        assert_eq!(cache.stats().1, 2 * BS);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn partial_range_straddling_present_block_and_fill_is_stitched() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache(dir.path(), 1 << 30);
        let remote = Remote::new((4 * BS) as usize);
        let fetch = remote.fetcher();
        // Warm block 0 alone and let its fill land.
        cache.read_through(KEY, 0..10, None, &fetch).await.unwrap();
        settle(&cache).await;
        // This range straddles the present block 0 and the missing block 1:
        // no single fill run covers it, so it is stitched from one pread of
        // block 0's bytes and the fetched run, in memory.
        let range = 10..BS + 10;
        let read = cache
            .read_through(KEY, range.clone(), None, &fetch)
            .await
            .unwrap();
        assert_eq!(read.outcome, Outcome::Partial);
        assert_eq!(read.remote_bytes, BS);
        assert_eq!(
            read.bytes,
            remote.data.slice(range.start as usize..range.end as usize)
        );
        // Once the fill landed the same range is one pread.
        settle(&cache).await;
        let calls = remote.calls();
        let read = cache
            .read_through(KEY, range.clone(), None, &fetch)
            .await
            .unwrap();
        assert_eq!(read.outcome, Outcome::Hit);
        assert_eq!(remote.calls(), calls);
        assert_eq!(
            read.bytes,
            remote.data.slice(range.start as usize..range.end as usize)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_io_failure_re_fetches_only_unserved_ranges() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache(dir.path(), 1 << 30);
        let remote = Remote::new(4 << 20);
        let fetch = remote.fetcher();
        // Warm block 2 so the request below mixes a present-block range
        // with fill-covered ones.
        cache
            .read_through(KEY, 2 * BS + 30..2 * BS + 60, None, &fetch)
            .await
            .unwrap();
        settle(&cache).await;
        let calls = remote.calls();
        // Break the cache file: the entry's path becomes a directory, so the
        // pread fails and the fallback serves from the remote (the queued
        // fill fails on it too and drops the entry, no retry).
        std::fs::remove_file(dir.path().join(KEY)).unwrap();
        std::fs::create_dir(dir.path().join(KEY)).unwrap();

        // Block 1 and block 30 are missing and 28 blocks apart, so they are
        // two fill runs (no coalescing). The block-2 range touches a present
        // block inside neither run: it is the only pread-shaped range, and
        // the fallback must re-fetch exactly it.
        let ranges = [
            BS + 10..BS + 100,
            2 * BS + 30..2 * BS + 60,
            30 * BS + 5..30 * BS + 25,
        ];
        let reads = cache
            .read_through_many(KEY, &ranges, None, &fetch)
            .await
            .unwrap();
        assert_eq!(reads.outcome, Outcome::Miss);
        // Two fills plus exactly one fallback fetch for the unserved range.
        assert_eq!(remote.calls() - calls, 3);
        assert_eq!(reads.remote_bytes, 2 * BS + 30);
        for (range, bytes) in ranges.iter().zip(&reads.bytes) {
            assert_eq!(
                *bytes,
                remote.data.slice(range.start as usize..range.end as usize)
            );
        }
        assert_eq!(reads.object_size, 4 << 20);
        settle(&cache).await;
        assert!(
            cache.get_entry(KEY).is_none(),
            "the broken entry is dropped"
        );
    }

    /// The piece plan, on fixed fills: fills blocks 1, 2 and 21 of a 32-block
    /// object, everything else counts as present.
    #[test]
    fn plan_ranges_slices_fills_preads_present_blocks_and_stitches_mixes() {
        let remote = Remote::new((32 * BS) as usize);
        let size = remote.data.len() as u64;
        let slice = |blocks: Range<u64>| {
            remote
                .data
                .slice((blocks.start * BS) as usize..(blocks.end * BS) as usize)
        };
        // Deliberately unsorted: the plan sorts the runs itself.
        let fills = [
            (21..22, slice(21..22)),
            (1..2, slice(1..2)),
            (2..3, slice(2..3)),
        ];
        let ranges = [
            BS + 10..BS + 100,      // inside run 1..2
            2 * BS..2 * BS,         // empty
            BS + 500..2 * BS + 500, // spans the adjacent runs 1..2 and 2..3
            5 * BS..5 * BS + 10,    // present blocks only
            10..BS + 10,            // present block 0 then run 1..2
            2 * BS + 10..22 * BS,   // run 2..3, present 3..21, run 21..22
        ];
        let plans = plan_ranges(&ranges, &fills, BS, size);
        assert_eq!(plans.len(), ranges.len());
        let expect =
            |range: &Range<u64>| remote.data.slice(range.start as usize..range.end as usize);
        let inside_remote = |bytes: &Bytes| {
            // A copied buffer may sit below the remote's allocation: wrap
            // instead of panicking, a wrapped offset is simply out of range.
            let offset = (bytes.as_ptr() as usize).wrapping_sub(remote.data.as_ptr() as usize);
            offset < remote.data.len()
        };

        let Plan::Ready(bytes) = &plans[0] else {
            panic!("inside one run is ready")
        };
        assert_eq!(*bytes, expect(&ranges[0]));
        assert!(inside_remote(bytes), "zero copy out of the run");

        let Plan::Ready(bytes) = &plans[1] else {
            panic!("empty is ready")
        };
        assert!(bytes.is_empty());

        let Plan::Ready(bytes) = &plans[2] else {
            panic!("two adjacent runs are copied into one buffer")
        };
        assert_eq!(*bytes, expect(&ranges[2]));
        assert!(!inside_remote(bytes), "one copy, no file");

        assert!(
            matches!(&plans[3], Plan::Disk(range) if *range == ranges[3]),
            "present blocks only: one pread"
        );

        let Plan::Stitch { range, pieces } = &plans[4] else {
            panic!("present then fill is stitched")
        };
        assert_eq!(*range, ranges[4]);
        assert_eq!(pieces.len(), 2);
        assert!(matches!(&pieces[0], Piece::Disk(r) if *r == (10..BS)));
        assert!(
            matches!(&pieces[1], Piece::Fill(b) if *b == remote.data.slice(BS as usize..BS as usize + 10))
        );

        let Plan::Stitch { range, pieces } = &plans[5] else {
            panic!("fill, present, fill is stitched")
        };
        assert_eq!(*range, ranges[5]);
        assert_eq!(pieces.len(), 3);
        assert!(
            matches!(&pieces[0], Piece::Fill(b) if *b == remote.data.slice(2 * BS as usize + 10..3 * BS as usize))
        );
        assert!(matches!(&pieces[1], Piece::Disk(r) if *r == (3 * BS..21 * BS)));
        assert!(
            matches!(&pieces[2], Piece::Fill(b) if *b == remote.data.slice(21 * BS as usize..22 * BS as usize))
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn straddle_of_present_block_and_unlanded_fill_is_assembled_in_memory() {
        let dir = tempfile::tempdir().unwrap();
        // The budget admits the first fill (two blocks) and not the second
        // (three blocks), so the second read's fill never reaches the file:
        // had the read pread its bytes back it would have read a hole.
        let cache = cache_with_fill_budget(dir.path(), 1 << 30, 2 * BS);
        let remote = Remote::new((8 * BS) as usize);
        let fetch = remote.fetcher();
        cache
            .read_through(KEY, 0..2 * BS, None, &fetch)
            .await
            .unwrap();
        settle(&cache).await;
        assert_eq!(cache.stats().1, 2 * BS);

        let dropped = metrics::QUERY_DISK_BLOCK_CACHE_FILLS_TOTAL
            .with_label_values(&["dropped"])
            .get();
        let range = BS + 10..5 * BS - 10;
        let read = cache
            .read_through(KEY, range.clone(), None, &fetch)
            .await
            .unwrap();
        assert_eq!(read.outcome, Outcome::Partial);
        assert_eq!(read.remote_bytes, 3 * BS);
        assert_eq!(
            read.bytes,
            remote.data.slice(range.start as usize..range.end as usize)
        );
        assert_eq!(cache.pending_fill_bytes(), 0, "nothing was queued");
        assert!(
            metrics::QUERY_DISK_BLOCK_CACHE_FILLS_TOTAL
                .with_label_values(&["dropped"])
                .get()
                > dropped
        );
        // The file still has a hole where the dropped fill would have gone,
        // so the bytes above could only have come from memory.
        let file = std::fs::File::open(dir.path().join(KEY)).unwrap();
        let hole = crate::cache::file_data::pread_vec(&file, 2 * BS, BS as usize).unwrap();
        assert!(
            hole.iter().all(|&b| b == 0),
            "the dropped fill never landed"
        );
        assert_eq!(cache.stats().1, 2 * BS, "dropped blocks are not accounted");
        // Blocks 2..5 stay absent: the same range fetches them again.
        let calls = remote.calls();
        let read = cache
            .read_through(KEY, range.clone(), None, &fetch)
            .await
            .unwrap();
        assert_eq!(read.outcome, Outcome::Partial);
        assert_eq!(remote.calls(), calls + 1);
        assert_eq!(
            read.bytes,
            remote.data.slice(range.start as usize..range.end as usize)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn adjacent_fill_runs_on_a_cold_object_are_stitched_without_the_file() {
        let dir = tempfile::tempdir().unwrap();
        // No fill is ever admitted, so no cache file ever exists: a read
        // that touched the file would fall back and cost a third GET.
        let cache = cache_with_fill_budget(dir.path(), 1 << 30, 1);
        let remote = Remote::new((4 * BS) as usize);
        let fetch = remote.fetcher();
        // The cold run covers the lowest range (block 0); the second range
        // runs past it into the missing-run fetch 1..3, so it spans two
        // adjacent fill runs.
        let ranges = [0..10, BS - 5..2 * BS + 5];
        let reads = cache
            .read_through_many(KEY, &ranges, None, &fetch)
            .await
            .unwrap();
        assert_eq!(reads.outcome, Outcome::Miss);
        assert_eq!(remote.calls(), 2, "two fills, no fallback");
        assert_eq!(reads.remote_bytes, 3 * BS);
        for (range, bytes) in ranges.iter().zip(&reads.bytes) {
            assert_eq!(
                *bytes,
                remote.data.slice(range.start as usize..range.end as usize)
            );
        }
        assert_eq!(cache.pending_fill_bytes(), 0);
        assert!(
            !dir.path().join(KEY).exists(),
            "a dropped fill creates no file"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fill_over_the_queue_budget_is_dropped_and_the_read_still_served() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache_with_fill_budget(dir.path(), 1 << 30, BS / 2);
        let remote = Remote::new(1 << 20);
        let fetch = remote.fetcher();
        // Other tests in this process drop fills too: compare as deltas.
        let dropped = metrics::QUERY_DISK_BLOCK_CACHE_FILLS_TOTAL
            .with_label_values(&["dropped"])
            .get();
        let dropped_bytes = metrics::QUERY_DISK_BLOCK_CACHE_FILL_BYTES_TOTAL
            .with_label_values(&["dropped"])
            .get();

        let read = cache
            .read_through(KEY, 100..1000, None, &fetch)
            .await
            .unwrap();
        assert_eq!(read.outcome, Outcome::Miss);
        assert_eq!(read.bytes, remote.data.slice(100..1000));
        assert_eq!(read.remote_bytes, BS);
        assert_eq!(
            cache.pending_fill_bytes(),
            0,
            "a dropped fill is never queued"
        );
        assert!(
            metrics::QUERY_DISK_BLOCK_CACHE_FILLS_TOTAL
                .with_label_values(&["dropped"])
                .get()
                >= dropped + 1
        );
        assert!(
            metrics::QUERY_DISK_BLOCK_CACHE_FILL_BYTES_TOTAL
                .with_label_values(&["dropped"])
                .get()
                >= dropped_bytes + BS
        );
        // The entry exists but owns nothing: no bit, no bytes, no file.
        let entry = cache.get_entry(KEY).expect("the cold entry is indexed");
        assert!(!is_set(&entry.state.lock().bits, 0));
        assert_eq!(cache.stats(), (1 << 30, 0, 1));
        assert!(!dir.path().join(KEY).exists());
        // So the same range is a miss again.
        let read = cache
            .read_through(KEY, 100..1000, None, &fetch)
            .await
            .unwrap();
        assert_eq!(read.outcome, Outcome::Miss);
        assert_eq!(read.bytes, remote.data.slice(100..1000));
        assert_eq!(remote.calls(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn fill_racing_a_remove_leaves_no_bit_and_no_file() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache(dir.path(), 1 << 30);
        let remote = Remote::new(1 << 20);
        let fetch = remote.fetcher();
        // Several rounds to land on different interleavings: the remove may
        // precede the writer's eviction check (fill skipped), split its
        // write from its accounting (bit refused, orphan unlinked) or follow
        // it (plain remove).
        for round in 0..16 {
            let key = format!("files/org/logs/stream/2026/10/08/11/race-{round}.vix");
            cache.read_through(&key, 0..10, None, &fetch).await.unwrap();
            let entry = cache.get_entry(&key).expect("indexed by the read");
            cache.remove(&key);
            settle(&cache).await;
            assert!(cache.get_entry(&key).is_none());
            let state = entry.state.lock();
            assert!(
                !is_set(&state.bits, 0),
                "round {round}: no bit after remove"
            );
            assert_eq!(state.present_bytes, 0);
            drop(state);
            wait_gone(&dir.path().join(&key)).await;
        }
        assert_eq!(cache.stats(), (1 << 30, 0, 0));
    }
}
