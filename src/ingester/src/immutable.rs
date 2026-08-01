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
    future::Future,
    io,
    path::{Path, PathBuf},
    sync::{
        Arc, LazyLock as Lazy, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
};

use config::{
    RwAHashSet, metrics,
    stats::{CacheStatsAsync, MemorySize},
};
use hashbrown::HashSet;
use snafu::ResultExt;
use tokio::{fs, sync::mpsc};

use crate::{
    ReadRecordBatchEntry,
    durability::{fsync_dir, write_file_atomic_durable},
    entry::PersistStat,
    errors::{
        DeleteFileSnafu, OpenDirSnafu, RenameFileSnafu, Result, TokioMpscSendSnafu, WriteFileSnafu,
    },
    memtable::MemTable,
    rwmap::RwIndexMap,
    writer::WriterKey,
};

pub(crate) static IMMUTABLES: Lazy<RwIndexMap<PathBuf, Arc<Immutable>>> =
    Lazy::new(RwIndexMap::default);

static PROCESSING_TABLES: Lazy<RwAHashSet<PathBuf>> = Lazy::new(Default::default);

pub(crate) struct Immutable {
    idx: usize,
    key: WriterKey,
    memtable: MemTable,
    staged: OnceLock<Arc<Staged>>,
}

/// The .par files one persist attempt wrote and fsynced, kept so a later
/// attempt promotes the same files. Dumping the memtable again would mint new
/// `ider::generate()` file names, i.e. a second copy of the same rows.
struct Staged {
    paths: Vec<PathBuf>,
    stat: PersistStat,
    /// Per-file commit progress, parallel to `paths`: set once this process
    /// has renamed the file to .parquet. It is the proof a retry needs that
    /// the file was promoted -- the filesystem cannot provide it, because the
    /// mover uploads and then reaps a promoted .parquet, making "already
    /// promoted" and "never written" look identical on disk.
    promoted: Vec<AtomicBool>,
}

impl Staged {
    fn new(paths: Vec<PathBuf>, stat: PersistStat) -> Self {
        let promoted = paths.iter().map(|_| AtomicBool::new(false)).collect();
        Self {
            paths,
            stat,
            promoted,
        }
    }
}

// PersistStat lives outside this change and is neither Clone nor Copy.
fn copy_stat(stat: &PersistStat) -> PersistStat {
    PersistStat {
        json_size: stat.json_size,
        arrow_size: stat.arrow_size,
        file_num: stat.file_num,
        batch_num: stat.batch_num,
        records: stat.records,
    }
}

impl MemorySize for Immutable {
    fn mem_size(&self) -> usize {
        std::mem::size_of::<Immutable>() + self.key.mem_size() + self.memtable.mem_size()
    }
}

pub async fn read_from_immutable(
    trace_id: &str,
    org_id: &str,
    stream_type: &str,
    stream_name: &str,
    time_range: Option<(i64, i64)>,
    partition_filters: &[(String, Vec<String>)],
    memtable_ids: &HashSet<u64>,
) -> Result<(Vec<u64>, Vec<ReadRecordBatchEntry>)> {
    let shared_memtable = config::get_config().common.feature_shared_memtable_enabled;
    let r = IMMUTABLES.read().await;
    let mut ids = Vec::with_capacity(r.len());
    let mut batches = Vec::with_capacity(r.len());
    for (_, i) in r.iter() {
        if stream_type == i.key.stream_type.as_ref()
            && (shared_memtable || org_id == i.key.org_id.as_ref())
        {
            let (id, batche) =
                i.memtable
                    .read(org_id, stream_name, time_range, partition_filters)?;
            if memtable_ids.contains(&id) {
                log::debug!(
                    "[trace_id {trace_id}] skip immutable memtable id: {id} already in memtable",
                );
                continue;
            }
            ids.push(id);
            batches.extend(batche);
        }
    }
    Ok((ids, batches))
}

impl Immutable {
    pub(crate) fn new(idx: usize, key: WriterKey, memtable: MemTable) -> Self {
        Self {
            idx,
            key,
            memtable,
            staged: OnceLock::new(),
        }
    }

    /// Dump the memtable to .par files at most once per Immutable, whatever
    /// happens after: every later attempt promotes the files of the first one.
    ///
    /// The dump is all-or-nothing: a memtable spanning several hour-partitions
    /// writes one .par per partition and registers each in
    /// WAL_PARQUET_METADATA as it goes, so a failure partway would strand the
    /// finished files and their metadata -- the retry re-dumps under fresh
    /// `ider::generate()` names and can never adopt them. On failure every
    /// .par this memtable produced is deleted and unregistered, so the retry
    /// starts from a clean slate. (Resuming the partial state instead would
    /// need per-file progress threaded through MemTable/Stream persist; the
    /// dump is cheap to repeat because the memtable is immutable, and a
    /// failure here is a disk-level event where a resumed retry would fail
    /// just the same.)
    async fn stage(&self) -> Result<Arc<Staged>> {
        if let Some(staged) = self.staged.get() {
            return Ok(staged.clone());
        }
        let (schema_size, paths) = match self
            .memtable
            .persist(
                self.memtable.id(),
                self.idx,
                &self.key.org_id,
                &self.key.stream_type,
            )
            .await
        {
            Ok(v) => v,
            Err(e) => {
                let wal_root = PathBuf::from(&config::get_config().common.data_wal_dir);
                cleanup_partial_stage(&wal_root, self.memtable.id()).await;
                return Err(e);
            }
        };
        let mut stat = PersistStat {
            arrow_size: schema_size,
            ..Default::default()
        };
        let mut file_paths = Vec::with_capacity(paths.len());
        for (path, file_stat) in paths {
            stat += file_stat;
            file_paths.push(path);
        }
        // a racing attempt may have staged first; its files are the ones the
        // lock file must name
        Ok(self
            .staged
            .get_or_init(|| Arc::new(Staged::new(file_paths, stat)))
            .clone())
    }

    pub(crate) async fn persist(&self, wal_path: &Path) -> Result<PersistStat> {
        let staged = self.stage().await?;
        commit_staged_files(&FsCommitOps, wal_path, &staged.paths, &staged.promoted).await?;
        Ok(copy_stat(&staged.stat))
    }
}

/// Best-effort undo of a partial memtable dump: delete every .par file this
/// memtable id produced and unregister its WAL_PARQUET_METADATA entry, so a
/// failed `stage()` leaves nothing behind for the retry to orphan. The id is
/// recoverable from the file name (`generate_filename_with_time_range` embeds
/// it, `get_memtable_id_from_file_name` is the pinned parser), which is what
/// lets this find files across every org/stream/hour directory the dump
/// touched. Only `.par` files are scanned: committed data is `.parquet` and is
/// never eligible. Errors are logged, not returned -- the caller is already on
/// an error path and boot recovery deletes stray .par files as the backstop.
async fn cleanup_partial_stage(wal_root: &Path, memtable_id: u64) {
    let files_root = wal_root.join("files");
    let par_files = match crate::wal::wal_scan_files(&files_root, "par").await {
        Ok(v) => v,
        Err(e) => {
            log::error!(
                "[INGESTER:MEM] scan for partial stage cleanup of memtable {memtable_id} failed: {e}"
            );
            return;
        }
    };
    for path in par_files {
        let Some(file_name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if config::utils::parquet::get_memtable_id_from_file_name(file_name) != memtable_id {
            continue;
        }
        // unregister the metadata the dump added under the would-be parquet key
        let mut file_key = path.clone();
        file_key.set_extension("parquet");
        if let Ok(file_key) = file_key.strip_prefix(wal_root) {
            let file_key = file_key
                .to_string_lossy()
                .replace('\\', "/")
                .trim_start_matches('/')
                .to_string();
            let removed = super::WAL_PARQUET_METADATA
                .write()
                .await
                .remove(&file_key)
                .is_some();
            if removed {
                // undo the wal-used accounting from Partition::persist; the
                // key is files/{org}/{stream_type}/...
                let parts: Vec<&str> = file_key.split('/').collect();
                if parts.len() > 2 {
                    let size = tokio::fs::metadata(&path)
                        .await
                        .map(|m| m.len())
                        .unwrap_or(0);
                    metrics::INGEST_WAL_USED_BYTES
                        .with_label_values(&[parts[1], parts[2]])
                        .sub(size as i64);
                }
            }
        }
        match tokio::fs::remove_file(&path).await {
            Ok(()) => {
                log::warn!(
                    "[INGESTER:MEM] removed partially staged file of memtable {memtable_id}: {}",
                    path.display()
                );
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => {
                log::error!(
                    "[INGESTER:MEM] failed to remove partially staged file {}: {e}",
                    path.display()
                );
            }
        }
    }
}

/// The filesystem effects of a persist commit, injected so the crash-window
/// ordering can be asserted in tests without killing a process.
trait CommitOps {
    fn sync_dir(&self, dir: &Path) -> impl Future<Output = Result<()>> + Send;
    fn write_lock(&self, lock_path: &Path, body: &[u8]) -> impl Future<Output = Result<()>> + Send;
    fn remove_wal(&self, wal_path: &Path) -> impl Future<Output = Result<()>> + Send;
    fn rename_par(
        &self,
        par_path: &Path,
        parquet_path: &Path,
    ) -> impl Future<Output = Result<()>> + Send;
    fn remove_lock(&self, lock_path: &Path) -> impl Future<Output = Result<()>> + Send;
}

struct FsCommitOps;

impl CommitOps for FsCommitOps {
    async fn sync_dir(&self, dir: &Path) -> Result<()> {
        match fsync_dir(dir).await {
            Ok(()) => Ok(()),
            // a directory that no longer exists has nothing to make durable;
            // whether that cost us a file is decided by rename_par, which sees
            // the individual paths
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            // OpenDirError is the closest existing variant: fsyncing a
            // directory does open it
            Err(e) => Err(e).context(OpenDirSnafu { path: dir }),
        }
    }

    async fn write_lock(&self, lock_path: &Path, body: &[u8]) -> Result<()> {
        // the lock's existence is what authorizes deleting the wal, so it must
        // never be observable with partial content: a plain O_CREAT|O_TRUNC
        // write survives a crash as an empty or truncated file that boot
        // recovery would read as a commit record. The atomic variant stages
        // the bytes under <lock>.tmp, fsyncs, then renames into place;
        // commit_staged_files fsyncs the directory right after, which makes
        // the rename durable before the wal delete may run.
        write_file_atomic_durable(lock_path, body)
            .await
            .context(WriteFileSnafu { path: lock_path })
    }

    async fn remove_wal(&self, wal_path: &Path) -> Result<()> {
        match fs::remove_file(wal_path).await {
            Ok(()) => Ok(()),
            // a retry of an attempt that got past this step: already gone is
            // exactly the state this step wants
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e).context(DeleteFileSnafu { path: wal_path }),
        }
    }

    async fn rename_par(&self, par_path: &Path, parquet_path: &Path) -> Result<()> {
        match fs::rename(par_path, parquet_path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                // already promoted by an earlier attempt or by boot recovery
                if fs::try_exists(parquet_path).await.unwrap_or(false) {
                    return Ok(());
                }
                // the stream-delete flow removes a stream's wal tree wholesale:
                // a .par file whose directory went with it has nothing left to
                // promote, and erroring forever here pins the memtable in RAM
                // until the node is restarted
                if let Some(parent) = par_path.parent()
                    && !fs::try_exists(parent).await.unwrap_or(true)
                {
                    log::warn!(
                        "par file directory removed before persist finished, skip: {}",
                        par_path.display()
                    );
                    return Ok(());
                }
                // the file existed when the lock named it: this is a loss, not
                // an already-done step
                Err(e).context(RenameFileSnafu { path: par_path })
            }
            Err(e) => Err(e).context(RenameFileSnafu { path: par_path }),
        }
    }

    async fn remove_lock(&self, lock_path: &Path) -> Result<()> {
        match fs::remove_file(lock_path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e).context(DeleteFileSnafu { path: lock_path }),
        }
    }
}

/// Crash-safe promotion of the staged .par files of one WAL file.
///
/// Ordering invariant, enforced here and mirrored by the boot recovery in
/// `wal::check_uncompleted_parquet_files`: nothing may destroy the only durable
/// record of the data before its replacement is itself durable.
///  1. the .par bytes are fsynced by `Partition::persist`; their directory entries are fsynced
///     here, before anything names them
///  2. the .lock file listing those .par files is fsynced, and so is its directory: past step 3 it
///     is the only record that they are complete
///  3. the .wal file is deleted -- the destructive step, and the point of no return for replay
///  4. .par -> .parquet, then the directories are fsynced again, so the promotion survives losing
///     the lock file
///  5. the .lock file is deleted; this one need not be durable, because a crash before it replays
///     steps 3-5, which are idempotent
///
/// Every step tolerates its own already-done state, so a retry converges
/// instead of failing forever on a wal file a previous attempt already deleted.
///
/// `promoted` is the per-file commit progress of this process, parallel to
/// `par_paths`: a file marked promoted is skipped on retry instead of
/// re-derived from the filesystem, because once the mover uploads and reaps
/// the .parquet the filesystem can no longer distinguish "already promoted"
/// from "never written". The lock still names every file and both sync_dir
/// passes still cover every directory, so a crash that loses the in-memory
/// marks falls back to boot recovery with full information.
async fn commit_staged_files<O: CommitOps>(
    ops: &O,
    wal_path: &Path,
    par_paths: &[PathBuf],
    promoted: &[AtomicBool],
) -> Result<()> {
    debug_assert_eq!(par_paths.len(), promoted.len());
    let lock_path = wal_path.with_extension("lock");

    // an empty memtable stages no files: there is nothing a lock would
    // protect, and writing one would recreate the empty-lock state that boot
    // recovery must treat as a partial write. Deleting the wal alone is safe
    // -- if it held anything that mattered, staging would have produced files
    // -- and the stale-lock removal converges any leftover from an older
    // binary's attempt.
    if par_paths.is_empty() {
        ops.remove_wal(wal_path).await?;
        return ops.remove_lock(&lock_path).await;
    }

    let mut dirs: Vec<&Path> = Vec::with_capacity(par_paths.len());
    let mut seen: HashSet<&Path> = HashSet::with_capacity(par_paths.len());
    for path in par_paths {
        if let Some(parent) = path.parent()
            && seen.insert(parent)
        {
            dirs.push(parent);
        }
    }
    for dir in dirs.iter() {
        ops.sync_dir(dir).await?;
    }

    let lock_data = par_paths
        .iter()
        .map(|p| p.to_string_lossy())
        .collect::<Vec<_>>()
        .join("\n");
    ops.write_lock(&lock_path, lock_data.as_bytes()).await?;
    if let Some(parent) = lock_path.parent() {
        ops.sync_dir(parent).await?;
    }

    ops.remove_wal(wal_path).await?;

    for (path, done) in par_paths.iter().zip(promoted.iter()) {
        if done.load(Ordering::Acquire) {
            continue; // promoted by an earlier attempt in this process
        }
        ops.rename_par(path, &path.with_extension("parquet"))
            .await?;
        done.store(true, Ordering::Release);
    }
    for dir in dirs.iter() {
        ops.sync_dir(dir).await?;
    }

    ops.remove_lock(&lock_path).await
}

pub(crate) async fn persist(tx: mpsc::Sender<PathBuf>) -> Result<()> {
    let r = IMMUTABLES.read().await;
    let n = r.len();
    let mut paths = Vec::with_capacity(n);
    for item in r.iter() {
        if paths.len() >= n {
            break;
        }
        paths.push(item.0.clone());
    }
    drop(r);
    for path in paths {
        // mark before handing off: a worker can finish the whole persist, and
        // its own removal from this set, before this task is scheduled again,
        // which left the path marked as processing forever -- the set only
        // grows, and check_persist_done never sees it drained again
        if !PROCESSING_TABLES.write().await.insert(path.clone()) {
            continue; // already processing
        }
        if let Err(e) = tx.send(path.clone()).await {
            PROCESSING_TABLES.write().await.remove(&path);
            return Err(e).context(TokioMpscSendSnafu);
        }
    }

    IMMUTABLES.write().await.shrink_to_fit();
    PROCESSING_TABLES.write().await.shrink_to_fit();

    Ok(())
}

pub(crate) async fn persist_table(idx: usize, path: PathBuf) -> Result<()> {
    let start = std::time::Instant::now();
    let r = IMMUTABLES.read().await;
    let Some(immutable) = r.get(&path) else {
        return Ok(());
    };
    let immutable = immutable.clone();
    drop(r);

    log::info!(
        "[INGESTER:MEM:{idx}] starts persist file: {}, took: {} ms",
        path.to_string_lossy(),
        start.elapsed().as_millis(),
    );

    // persist entry to local disk
    let start = std::time::Instant::now();
    let ret = immutable.persist(&path).await;
    let stat = match ret {
        Ok(v) => v,
        Err(e) => {
            // remove from processing tables
            PROCESSING_TABLES.write().await.remove(&path);
            return Err(e);
        }
    };
    log::info!(
        "[INGESTER:MEM:{idx}] finish persist file: {}, json_size: {}, arrow_size: {}, file_num: {} batch_num: {}, records: {}, took: {} ms",
        path.to_string_lossy(),
        stat.json_size,
        stat.arrow_size,
        stat.file_num,
        stat.batch_num,
        stat.records,
        start.elapsed().as_millis(),
    );

    // remove entry
    let mut rw = IMMUTABLES.write().await;
    rw.swap_remove(&path);
    drop(rw);

    // remove from processing tables
    PROCESSING_TABLES.write().await.remove(&path);

    // update metrics
    metrics::INGEST_MEMTABLE_BYTES
        .with_label_values::<&str>(&[])
        .sub(stat.json_size);
    metrics::INGEST_MEMTABLE_ARROW_BYTES
        .with_label_values::<&str>(&[])
        .sub(stat.arrow_size as i64);
    metrics::INGEST_MEMTABLE_FILES
        .with_label_values::<&str>(&[])
        .dec();

    Ok(())
}

/// Whether every immutable memtable known to this node has reached disk, i.e.
/// nothing is queued for persist and nothing is mid-persist. Callers use it as
/// the gate before removing a stream's WAL directory: while either set is
/// non-empty a persist can still create files under it.
///
/// `_seq_id` is ignored. It comes from `get_max_writer_seq_id` (WAL seq space:
/// one counter per writer, seeded from `Utc::now()` and bumped per wal file),
/// while immutables are keyed by memtable id (one global counter, seeded once
/// from `now_micros()` and bumped per memtable). The two are not comparable, so
/// any ordering test between them answers an arbitrary question -- the previous
/// `min_id < seq_id` returned true exactly when a persist was still pending.
/// The parameter stays for API compatibility with the caller in
/// `db::schema::flush_cache_for_stream`.
pub async fn check_persist_done(_seq_id: u64) -> bool {
    // never held at the same time: `persist_table` takes IMMUTABLES then
    // PROCESSING_TABLES. An in-flight table is in at least one of them for the
    // whole window, and all of its file work happens before it leaves
    // IMMUTABLES, so reading them in this order cannot report done too early.
    if !IMMUTABLES.read().await.is_empty() {
        return false;
    }
    PROCESSING_TABLES.read().await.is_empty()
}

pub async fn get_immutables_cache_stats() -> (usize, usize, usize) {
    IMMUTABLES.stats().await
}

pub async fn get_processing_tables_cache_stats() -> (usize, usize, usize) {
    PROCESSING_TABLES.stats().await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("oo-ingester-immutable-{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    // IMMUTABLES and PROCESSING_TABLES are process wide: tests that put entries
    // in them must not run while another one asserts on their emptiness
    static GLOBAL_TABLES: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn no_progress(n: usize) -> Vec<AtomicBool> {
        (0..n).map(|_| AtomicBool::new(false)).collect()
    }

    #[derive(Default)]
    struct RecordingOps {
        calls: std::sync::Mutex<Vec<String>>,
    }

    impl RecordingOps {
        fn record(&self, call: String) {
            self.calls.lock().expect("test lock").push(call);
        }

        fn calls(&self) -> Vec<String> {
            self.calls.lock().expect("test lock").clone()
        }
    }

    impl CommitOps for RecordingOps {
        async fn sync_dir(&self, dir: &Path) -> Result<()> {
            self.record(format!("sync_dir:{}", dir.display()));
            Ok(())
        }

        async fn write_lock(&self, lock_path: &Path, body: &[u8]) -> Result<()> {
            self.record(format!(
                "write_lock:{}:{}",
                lock_path.display(),
                String::from_utf8_lossy(body)
            ));
            Ok(())
        }

        async fn remove_wal(&self, wal_path: &Path) -> Result<()> {
            self.record(format!("remove_wal:{}", wal_path.display()));
            Ok(())
        }

        async fn rename_par(&self, par_path: &Path, parquet_path: &Path) -> Result<()> {
            self.record(format!(
                "rename_par:{}->{}",
                par_path.display(),
                parquet_path.display()
            ));
            Ok(())
        }

        async fn remove_lock(&self, lock_path: &Path) -> Result<()> {
            self.record(format!("remove_lock:{}", lock_path.display()));
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_commit_staged_files_syncs_before_the_destructive_step() {
        let ops = RecordingOps::default();
        let wal = PathBuf::from("/wal/logs/0/org/logs/7.wal");
        let par_a = PathBuf::from("/wal/files/org/logs/s/0/h1/a.par");
        let par_b = PathBuf::from("/wal/files/org/logs/s/0/h1/b.par");
        let par_c = PathBuf::from("/wal/files/org/logs/s/0/h2/c.par");
        let paths = vec![par_a.clone(), par_b.clone(), par_c.clone()];

        commit_staged_files(&ops, &wal, &paths, &no_progress(paths.len()))
            .await
            .unwrap();

        assert_eq!(
            ops.calls(),
            vec![
                // 1. the dirs holding the fsynced .par files, deduplicated
                "sync_dir:/wal/files/org/logs/s/0/h1".to_string(),
                "sync_dir:/wal/files/org/logs/s/0/h2".to_string(),
                // 2. the lock naming them, durable with its own dir
                format!(
                    "write_lock:/wal/logs/0/org/logs/7.lock:{}\n{}\n{}",
                    par_a.display(),
                    par_b.display(),
                    par_c.display()
                ),
                "sync_dir:/wal/logs/0/org/logs".to_string(),
                // 3. only now may the wal file go
                "remove_wal:/wal/logs/0/org/logs/7.wal".to_string(),
                // 4. promote, then make the promotion durable
                "rename_par:/wal/files/org/logs/s/0/h1/a.par->/wal/files/org/logs/s/0/h1/a.parquet"
                    .to_string(),
                "rename_par:/wal/files/org/logs/s/0/h1/b.par->/wal/files/org/logs/s/0/h1/b.parquet"
                    .to_string(),
                "rename_par:/wal/files/org/logs/s/0/h2/c.par->/wal/files/org/logs/s/0/h2/c.parquet"
                    .to_string(),
                "sync_dir:/wal/files/org/logs/s/0/h1".to_string(),
                "sync_dir:/wal/files/org/logs/s/0/h2".to_string(),
                // 5. the lock last
                "remove_lock:/wal/logs/0/org/logs/7.lock".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn test_commit_staged_files_is_idempotent_on_retry() {
        let dir = test_dir("commit-retry");
        let wal = dir.join("7.wal");
        std::fs::write(&wal, b"wal").unwrap();
        let par_dir = dir.join("files/org/logs/s/0/h1");
        std::fs::create_dir_all(&par_dir).unwrap();
        let par = par_dir.join("a.par");
        std::fs::write(&par, b"parquet-bytes").unwrap();
        let parquet = par.with_extension("parquet");

        // fresh progress on each attempt: this exercises the filesystem
        // fallback ("target exists"), not the in-process marks
        commit_staged_files(&FsCommitOps, &wal, std::slice::from_ref(&par), &no_progress(1))
            .await
            .unwrap();
        assert!(parquet.is_file());
        assert!(!wal.exists());
        assert!(!wal.with_extension("lock").exists());

        // the wedge: every retry used to die here on remove_file(wal) NotFound
        commit_staged_files(&FsCommitOps, &wal, std::slice::from_ref(&par), &no_progress(1))
            .await
            .unwrap();
        assert_eq!(std::fs::read(&parquet).unwrap(), b"parquet-bytes");
        assert_eq!(std::fs::read_dir(&par_dir).unwrap().count(), 1);
        assert!(!wal.with_extension("lock").exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_commit_staged_files_reports_a_lost_file() {
        let dir = test_dir("commit-lost");
        let wal = dir.join("8.wal");
        std::fs::write(&wal, b"wal").unwrap();
        let par = dir.join("gone.par");

        let err = commit_staged_files(&FsCommitOps, &wal, std::slice::from_ref(&par), &no_progress(1))
            .await
            .unwrap_err();
        // neither .par nor .parquet exists: not an already-done step
        assert!(matches!(err, crate::errors::Error::RenameFileError { .. }));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_commit_staged_files_converges_when_the_stream_dir_is_deleted() {
        let dir = test_dir("commit-stream-deleted");
        let wal = dir.join("11.wal");
        std::fs::write(&wal, b"wal").unwrap();
        // the stream-delete flow removed files/<org>/<stream_type>/<stream>
        let par = dir.join("files/org/logs/s/0/h1/a.par");

        commit_staged_files(&FsCommitOps, &wal, std::slice::from_ref(&par), &no_progress(1))
            .await
            .unwrap();
        assert!(!wal.exists());
        assert!(!wal.with_extension("lock").exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_commit_staged_files_empty_stage_needs_no_lock() {
        let ops = RecordingOps::default();
        let wal = PathBuf::from("/wal/logs/0/org/logs/13.wal");

        commit_staged_files(&ops, &wal, &[], &no_progress(0))
            .await
            .unwrap();

        // no lock is written for an empty stage: an empty lock is exactly the
        // partial-write state boot recovery must treat as absent
        assert_eq!(
            ops.calls(),
            vec![
                "remove_wal:/wal/logs/0/org/logs/13.wal".to_string(),
                "remove_lock:/wal/logs/0/org/logs/13.lock".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn test_fs_write_lock_is_atomic_with_no_tmp_leftover() {
        let dir = test_dir("write-lock-atomic");
        let lock = dir.join("5.lock");

        FsCommitOps
            .write_lock(&lock, b"files/a.par\nfiles/b.par")
            .await
            .unwrap();

        assert_eq!(std::fs::read(&lock).unwrap(), b"files/a.par\nfiles/b.par");
        // the staging name never outlives the write: a crash can leave a
        // .lock.tmp (cleaned at boot) but the .lock itself is rename-complete
        assert!(!dir.join("5.lock.tmp").exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Delegates to an inner recorder but fails the first rename of one path,
    /// standing in for a transient IO error on that file.
    struct FailRenameOnce {
        inner: RecordingOps,
        fail_path: PathBuf,
        failed: AtomicBool,
    }

    impl CommitOps for FailRenameOnce {
        async fn sync_dir(&self, dir: &Path) -> Result<()> {
            self.inner.sync_dir(dir).await
        }

        async fn write_lock(&self, lock_path: &Path, body: &[u8]) -> Result<()> {
            self.inner.write_lock(lock_path, body).await
        }

        async fn remove_wal(&self, wal_path: &Path) -> Result<()> {
            self.inner.remove_wal(wal_path).await
        }

        async fn rename_par(&self, par_path: &Path, parquet_path: &Path) -> Result<()> {
            if par_path == self.fail_path && !self.failed.swap(true, Ordering::SeqCst) {
                self.inner.record(format!("rename_par_fail:{}", par_path.display()));
                return Err(crate::errors::Error::RenameFileError {
                    source: io::Error::other("injected rename failure"),
                    path: par_path.to_path_buf(),
                });
            }
            self.inner.rename_par(par_path, parquet_path).await
        }

        async fn remove_lock(&self, lock_path: &Path) -> Result<()> {
            self.inner.remove_lock(lock_path).await
        }
    }

    #[tokio::test]
    async fn test_commit_staged_files_skips_files_the_process_already_promoted() {
        let wal = PathBuf::from("/wal/logs/0/org/logs/14.wal");
        let par_a = PathBuf::from("/wal/files/org/logs/s/0/h1/a.par");
        let par_b = PathBuf::from("/wal/files/org/logs/s/0/h1/b.par");
        let paths = vec![par_a.clone(), par_b.clone()];
        let promoted = no_progress(2);
        let ops = FailRenameOnce {
            inner: RecordingOps::default(),
            fail_path: par_b.clone(),
            failed: AtomicBool::new(false),
        };

        // attempt 1: a promotes and is recorded, b fails, the error propagates
        assert!(
            commit_staged_files(&ops, &wal, &paths, &promoted)
                .await
                .is_err()
        );
        assert!(promoted[0].load(Ordering::SeqCst));
        assert!(!promoted[1].load(Ordering::SeqCst));

        // between attempts the mover may upload and reap a.parquet, so only
        // the recorded progress can prove a was promoted: the retry must not
        // rename it again
        commit_staged_files(&ops, &wal, &paths, &promoted)
            .await
            .unwrap();
        assert!(promoted[1].load(Ordering::SeqCst));
        let a_renames = ops
            .inner
            .calls()
            .iter()
            .filter(|c| c.starts_with(&format!("rename_par:{}", par_a.display())))
            .count();
        assert_eq!(a_renames, 1);
    }

    #[tokio::test]
    async fn test_commit_retry_after_mover_reaped_the_promoted_parquet() {
        let dir = test_dir("commit-mover-reap");
        let par_dir = dir.join("files/org/logs/s/0/h1");
        std::fs::create_dir_all(&par_dir).unwrap();
        // attempt 1 deleted the wal, promoted a.par -> a.parquet, then failed
        // on b.par; the mover then uploaded and reaped a.parquet, so neither
        // a.par nor a.parquet exists -- on disk that is indistinguishable from
        // a lost file
        let par_a = par_dir.join("a.par");
        let par_b = par_dir.join("b.par");
        std::fs::write(&par_b, b"parquet-bytes-b").unwrap();
        let wal = dir.join("15.wal");
        let paths = vec![par_a.clone(), par_b.clone()];
        let promoted = no_progress(2);
        promoted[0].store(true, Ordering::SeqCst); // attempt 1's recorded progress

        commit_staged_files(&FsCommitOps, &wal, &paths, &promoted)
            .await
            .unwrap();

        assert_eq!(
            std::fs::read(par_b.with_extension("parquet")).unwrap(),
            b"parquet-bytes-b"
        );
        assert!(!par_a.with_extension("parquet").exists());
        assert!(!wal.with_extension("lock").exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_cleanup_partial_stage_scopes_to_the_memtable_id() {
        let root = test_dir("cleanup-partial");
        let h1 = root.join("files/org/logs/s/0/h1");
        let h2 = root.join("files/org/logs/s/0/h2");
        std::fs::create_dir_all(&h1).unwrap();
        std::fs::create_dir_all(&h2).unwrap();
        // two partitions of memtable 42 dumped before the failure, one .par of
        // another memtable, and a committed .parquet that must never be touched
        let mine_a = h1.join("1000.2000.abc_42.par");
        let mine_b = h2.join("1500.2500.def_42.par");
        let other = h1.join("1000.2000.ghi_43.par");
        let committed = h1.join("900.1900.jkl_42.parquet");
        for f in [&mine_a, &mine_b, &other, &committed] {
            std::fs::write(f, b"bytes").unwrap();
        }
        let key_a = "files/org/logs/s/0/h1/1000.2000.abc_42.parquet";
        let key_other = "files/org/logs/s/0/h1/1000.2000.ghi_43.parquet";
        crate::WAL_PARQUET_METADATA
            .write()
            .await
            .insert(key_a.to_string(), Default::default());
        crate::WAL_PARQUET_METADATA
            .write()
            .await
            .insert(key_other.to_string(), Default::default());

        cleanup_partial_stage(&root, 42).await;

        assert!(!mine_a.exists());
        assert!(!mine_b.exists());
        assert!(other.exists());
        assert!(committed.exists());
        {
            let r = crate::WAL_PARQUET_METADATA.read().await;
            assert!(!r.contains_key(key_a));
            assert!(r.contains_key(key_other));
        }

        crate::WAL_PARQUET_METADATA.write().await.remove(key_other);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn test_persist_twice_converges() {
        let dir = test_dir("persist-twice");
        let wal = dir.join("9.wal");
        std::fs::write(&wal, b"wal").unwrap();
        let immutable = Immutable::new(
            0,
            WriterKey::new_replay("org", "logs"),
            crate::memtable::MemTable::new(),
        );

        assert!(immutable.persist(&wal).await.is_ok());
        assert!(!wal.exists());
        // a retry of an already committed persist must succeed, or the memtable
        // stays pinned in RAM for the lifetime of the process
        assert!(immutable.persist(&wal).await.is_ok());
        assert!(!wal.with_extension("lock").exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_persist_retry_reuses_staged_files() {
        let dir = test_dir("persist-staged");
        let wal = dir.join("10.wal");
        std::fs::write(&wal, b"wal").unwrap();
        let par_dir = dir.join("files/org/logs/s/0/h1");
        std::fs::create_dir_all(&par_dir).unwrap();
        let par = par_dir.join("a.par");
        std::fs::write(&par, b"parquet-bytes").unwrap();

        let immutable = Immutable::new(
            0,
            WriterKey::new_replay("org", "logs"),
            crate::memtable::MemTable::new(),
        );
        assert!(
            immutable
                .staged
                .set(Arc::new(Staged::new(
                    vec![par.clone()],
                    PersistStat {
                        json_size: 7,
                        arrow_size: 9,
                        file_num: 1,
                        batch_num: 2,
                        records: 3,
                    },
                )))
                .is_ok()
        );

        let first = immutable.persist(&wal).await.unwrap();
        let second = immutable.persist(&wal).await.unwrap();

        // same stat both times: the caller decrements the memtable metrics with
        // it, and a second dump would have produced a second set of files
        assert_eq!(first.json_size, second.json_size);
        assert_eq!(first.arrow_size, second.arrow_size);
        assert_eq!(first.records, 3);
        assert_eq!(second.records, 3);
        assert_eq!(std::fs::read_dir(&par_dir).unwrap().count(), 1);
        assert!(par.with_extension("parquet").is_file());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_persist_marks_processing_before_handing_off() {
        let _guard = GLOBAL_TABLES.lock().await;
        let path = PathBuf::from("/tmp/oo-ingester-persist-handoff.wal");
        let immutable = Arc::new(Immutable::new(
            0,
            WriterKey::new_replay("org", "logs"),
            crate::memtable::MemTable::new(),
        ));
        IMMUTABLES.write().await.insert(path.clone(), immutable);
        let (tx, mut rx) = mpsc::channel(4);

        persist(tx.clone()).await.unwrap();
        assert!(PROCESSING_TABLES.read().await.contains(&path));
        assert_eq!(rx.recv().await.unwrap(), path);

        // the mark is what keeps the next scan from queueing it twice
        persist(tx).await.unwrap();
        assert!(rx.try_recv().is_err());

        IMMUTABLES.write().await.swap_remove(&path);
        PROCESSING_TABLES.write().await.remove(&path);
    }

    #[tokio::test]
    async fn test_check_persist_done_truth_table() {
        let _guard = GLOBAL_TABLES.lock().await;
        let path = PathBuf::from("/tmp/oo-ingester-check-persist-done.wal");
        // drained => done, whatever id space the caller's number came from
        assert!(check_persist_done(0).await);
        assert!(check_persist_done(u64::MAX).await);

        let immutable = Arc::new(Immutable::new(
            0,
            WriterKey::new_replay("org", "logs"),
            crate::memtable::MemTable::new(),
        ));
        IMMUTABLES.write().await.insert(path.clone(), immutable);
        // queued for persist => not done
        assert!(!check_persist_done(0).await);
        assert!(!check_persist_done(u64::MAX).await);
        IMMUTABLES.write().await.swap_remove(&path);

        // mid-persist, already out of IMMUTABLES => still not done
        PROCESSING_TABLES.write().await.insert(path.clone());
        assert!(!check_persist_done(u64::MAX).await);
        PROCESSING_TABLES.write().await.remove(&path);

        assert!(check_persist_done(u64::MAX).await);
    }

    #[tokio::test]
    async fn test_get_immutables_cache_stats() {
        let _guard = GLOBAL_TABLES.lock().await;
        // the stats tuple is (len, capacity, mem_size); a container's length
        // can never exceed its capacity. The old assertion had the pair
        // backwards and only held while no test had ever touched the global
        // table (a removed entry leaves its capacity behind).
        let (len, capacity, mem_size) = get_immutables_cache_stats().await;
        assert!(len <= capacity);
        // mem_size counts the container struct itself plus its entries
        assert!(mem_size > 0);
    }

    #[tokio::test]
    async fn test_get_immutables_cache_stats_consistency() {
        // the guard keeps other tests from mutating the table between samples
        let _guard = GLOBAL_TABLES.lock().await;
        let (len1, cap1, mem1) = get_immutables_cache_stats().await;
        let (len2, cap2, mem2) = get_immutables_cache_stats().await;

        // nothing can change while the guard is held
        assert_eq!(len1, len2);
        assert_eq!(cap1, cap2);
        assert_eq!(mem1, mem2);
    }

    #[tokio::test]
    async fn test_get_processing_tables_cache_stats() {
        let _guard = GLOBAL_TABLES.lock().await;
        // (len, capacity, mem_size), same invariants as the immutables stats
        let (len, capacity, mem_size) = get_processing_tables_cache_stats().await;
        assert!(len <= capacity);
        assert!(mem_size > 0);
    }

    #[tokio::test]
    async fn test_get_processing_tables_cache_stats_consistency() {
        let _guard = GLOBAL_TABLES.lock().await;
        let (len1, cap1, mem1) = get_processing_tables_cache_stats().await;
        let (len2, cap2, mem2) = get_processing_tables_cache_stats().await;

        // nothing can change while the guard is held
        assert_eq!(len1, len2);
        assert_eq!(cap1, cap2);
        assert_eq!(mem1, mem2);
    }

    #[tokio::test]
    async fn test_both_cache_stats_functions() {
        let _guard = GLOBAL_TABLES.lock().await;
        // Test that both functions work correctly when called together
        let immutables_stats = get_immutables_cache_stats().await;
        let processing_stats = get_processing_tables_cache_stats().await;

        // (len, capacity, mem_size): len can never exceed capacity
        assert!(immutables_stats.0 <= immutables_stats.1);
        assert!(processing_stats.0 <= processing_stats.1);

        // Both functions should be callable independently
        let _ = get_immutables_cache_stats().await;
        let _ = get_processing_tables_cache_stats().await;
    }
}
