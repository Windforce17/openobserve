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

mod entry;
pub mod errors;
mod immutable;
mod memtable;
mod partition;
mod rwmap;
mod stream;
mod wal;
mod writer;

use std::{
    fs::create_dir_all,
    path::PathBuf,
    sync::{Arc, LazyLock as Lazy},
};

use arrow_schema::Schema;
use config::RwAHashMap;
pub use entry::Entry;
pub use immutable::{
    check_persist_done, get_immutables_cache_stats, get_processing_tables_cache_stats,
    read_from_immutable,
};
use snafu::ResultExt;
use tokio::sync::{Mutex, mpsc};
pub use wal::collect_wal_parquet_metrics;
pub use writer::{
    Writer, check_disk_circuit_breaker, check_memory_circuit_breaker, check_memtable_size,
    flush_all, get_max_writer_seq_id, get_writer, read_from_memtable,
};

use crate::errors::OpenDirSnafu;

pub(crate) type ReadRecordBatchEntry = (Arc<Schema>, Vec<Arc<entry::RecordBatchEntry>>);

/// Crash-safety primitives shared by the local persist chain.
///
/// Turning a memtable into a queryable parquet file is
/// `.par write -> .lock write -> .wal delete -> .par rename -> .lock delete`
/// (`immutable::commit_staged_files`). Every step destroys the record the
/// previous step relied on, so each one must be on stable storage before the
/// next runs: otherwise a power loss can leave a deleted WAL file next to a
/// parquet file that never reached the disk.
pub(crate) mod durability {
    use std::{
        io,
        path::{Path, PathBuf},
    };

    use tokio::{
        fs::{File, OpenOptions},
        io::AsyncWriteExt,
    };

    /// Write `data` to `path`, returning only once the bytes are on stable
    /// storage. `tokio::fs::File` buffers writes and its `Drop` neither flushes
    /// nor syncs, so both calls are required.
    ///
    /// NOT atomic: `open(O_CREAT|O_TRUNC)` survives a crash on its own, so
    /// `path` can be observed empty or partial until the fsync lands. Callers
    /// whose readers treat the file's *existence* as a fact (the .lock files)
    /// must use [`write_file_atomic_durable`] instead.
    pub(crate) async fn write_file_durable(path: &Path, data: &[u8]) -> io::Result<()> {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .await?;
        f.write_all(data).await?;
        f.flush().await?;
        f.sync_all().await
    }

    /// Write `data` so that `path` existing implies its content is complete
    /// and durable: the bytes are written and fsynced under a `.tmp` sibling
    /// name first, then renamed over `path`. rename(2) is atomic, so a crash
    /// at any point leaves either the old state or the full new content at
    /// `path`, never a truncated one. The rename itself is only durable once
    /// the caller fsyncs the parent directory; until then the worst case is
    /// that `path` reverts to its previous state, which is still all-or-none.
    pub(crate) async fn write_file_atomic_durable(path: &Path, data: &[u8]) -> io::Result<()> {
        let mut tmp_name = path.as_os_str().to_owned();
        tmp_name.push(".tmp");
        let tmp_path = PathBuf::from(tmp_name);
        write_file_durable(&tmp_path, data).await?;
        tokio::fs::rename(&tmp_path, path).await
    }

    /// fsync a directory so entries created or renamed inside it survive a
    /// power loss: syncing a file makes its contents durable, not the link that
    /// names it.
    pub(crate) async fn fsync_dir(dir: &Path) -> io::Result<()> {
        // POSIX only: Windows cannot open a directory as a file and has no
        // equivalent call, so the guarantee is degraded there, not faked.
        if cfg!(not(unix)) {
            return Ok(());
        }
        File::open(dir).await?.sync_all().await
    }
}

pub static WAL_PARQUET_METADATA: Lazy<RwAHashMap<String, config::meta::stream::FileMeta>> =
    Lazy::new(Default::default);

pub static WAL_DIR_DEFAULT_PREFIX: &str = "logs";

// writer signal
pub enum WriterSignal {
    Produce,
    Rotate,
    Close,
}

/// One message on a writer's queue: the signal, its payload, the per-request
/// fsync flag, and -- for `Produce` sent from `write_batch` in queue mode --
/// the channel that carries the write's real outcome back to the ingest
/// request. Without it an enqueue would be indistinguishable from a durable
/// write and consumer failures could only be logged, so the ack the client
/// gets would be a lie.
pub(crate) type WriterQueueItem = (
    WriterSignal,
    ProcessedBatch,
    bool,
    Option<tokio::sync::oneshot::Sender<errors::Result<()>>>,
);

/// Pre-processed write batch ready for IO operations
///
/// This structure contains all data pre-processed and ready for direct IO,
/// moving CPU-intensive work (JSON to Arrow conversion) out of the consume loop.
pub struct ProcessedBatch {
    /// Original entries for metadata
    pub entries: Vec<Entry>,
    /// Serialized bytes for WAL writing
    pub bytes_entries: Vec<Vec<u8>>,
    /// Arrow RecordBatch entries for Memtable writing
    pub batch_entries: Vec<Arc<entry::RecordBatchEntry>>,
    /// Total serialized WAL bytes for rotation check
    pub entries_wal_size: usize,
    /// Total Arrow size for rotation check
    pub entries_arrow_size: usize,
}

impl ProcessedBatch {
    /// Create an empty ProcessedBatch for control signals (Rotate, Close)
    pub fn empty() -> Self {
        Self {
            entries: Vec::new(),
            bytes_entries: Vec::new(),
            batch_entries: Vec::new(),
            entries_wal_size: 0,
            entries_arrow_size: 0,
        }
    }
}

pub async fn init() -> errors::Result<()> {
    if !config::cluster::LOCAL_NODE.is_ingester() {
        return Ok(());
    }

    log::info!("Start ingester init");

    // replay wal files to create immutable
    let cfg = config::get_config();
    let wal_dir = PathBuf::from(&cfg.common.data_wal_dir).join(WAL_DIR_DEFAULT_PREFIX);
    create_dir_all(&wal_dir).context(OpenDirSnafu {
        path: wal_dir.clone(),
    })?;

    // check uncompleted parquet files, need delete those files
    wal::check_uncompleted_parquet_files().await?;

    // replay wal files
    tokio::task::spawn(async move {
        log::info!("Scanning wal files from {wal_dir:?}");
        let wal_files = wal::wal_scan_files(&wal_dir, "wal")
            .await
            .unwrap_or_default();
        log::info!("Found {} wal files to replay", wal_files.len());
        if let Err(e) = wal::replay_wal_files(wal_dir, wal_files).await {
            log::error!("replay wal files error: {e}");
        }
        log::info!("Replay wal files done");
    });

    // start a job to flush memtable to immutable
    tokio::task::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(
                config::get_config().limit.max_file_retention_time,
            ))
            .await;
            // check memtable ttl
            if let Err(e) = writer::check_ttl().await {
                log::error!("memtable check ttl error: {e}");
            }
        }
    });

    // start a job to flush memtable to immutable
    tokio::task::spawn(async move {
        if let Err(e) = run().await {
            log::error!("immutable persist error: {e}");
        }
    });

    log::info!("Ingesters init done");

    Ok(())
}

async fn run() -> errors::Result<()> {
    // start persist worker
    let cfg = config::get_config();
    let (tx, rx) = mpsc::channel::<PathBuf>(cfg.limit.mem_dump_thread_num);
    let rx = Arc::new(Mutex::new(rx));
    for thread_id in 0..cfg.limit.mem_dump_thread_num {
        let rx = rx.clone();
        tokio::spawn(async move {
            loop {
                let ret = rx.lock().await.recv().await;
                match ret {
                    None => {
                        log::debug!("[INGESTER:MEM] Receiving memtable channel is closed");
                        break;
                    }
                    Some(path) => {
                        if let Err(e) = immutable::persist_table(thread_id, path).await {
                            log::error!("[INGESTER:MEM:{thread_id}] Error persist memtable: {e}");
                        }
                    }
                }
            }
        });
    }

    // start a job to dump immutable data to disk
    loop {
        if config::cluster::is_offline() {
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(
            config::get_config().limit.mem_persist_interval,
        ))
        .await;
        // persist immutable data to disk
        if let Err(e) = immutable::persist(tx.clone()).await {
            log::error!("immutable persist error: {e}");
        }
        // shrink metadata cache
        WAL_PARQUET_METADATA.write().await.shrink_to_fit();
    }

    log::info!("[INGESTER:MEM] immutable persist is stopped");
    Ok(())
}

// check if the file is a wal file
// wal file format:
// files/{org}/{stype}/{stream}/{thread_id}/{year}/{month}/{day}/{hour}/{schema_key}/{file_name}
pub fn is_wal_file(file: &str) -> bool {
    let columns = file.split('/').collect::<Vec<_>>();
    !(columns.len() < 11
        // thread_id is impossible over 1000
        || columns[4].len() == 4
        // schema_key is 16 bytes, and not contains "="
        || columns[9].len() != 16
        || columns[9].contains("="))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("oo-ingester-lib-{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[tokio::test]
    async fn test_write_file_durable_writes_and_truncates() {
        let dir = test_dir("durable-write");
        let path = dir.join("a.lock");
        durability::write_file_durable(&path, b"first-and-longer")
            .await
            .unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"first-and-longer");
        // a retry rewrites the same file: no leftover tail from the longer body
        durability::write_file_durable(&path, b"second")
            .await
            .unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"second");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_write_file_durable_propagates_error() {
        let dir = test_dir("durable-write-err");
        let path = dir.join("missing").join("a.lock");
        let err = durability::write_file_durable(&path, b"x")
            .await
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_write_file_atomic_durable_leaves_no_tmp_and_overwrites() {
        let dir = test_dir("atomic-write");
        let path = dir.join("a.lock");
        durability::write_file_atomic_durable(&path, b"first-and-longer")
            .await
            .unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"first-and-longer");
        // the temp name must not survive a successful write
        assert!(!dir.join("a.lock.tmp").exists());
        // a retry replaces the whole file, no leftover tail
        durability::write_file_atomic_durable(&path, b"second")
            .await
            .unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"second");
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_write_file_atomic_durable_failure_leaves_target_absent() {
        let dir = test_dir("atomic-write-err");
        let path = dir.join("missing").join("a.lock");
        let err = durability::write_file_atomic_durable(&path, b"x")
            .await
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
        // the crash-window property: a failed write never publishes the name
        assert!(!path.exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_fsync_dir_existing_and_missing() {
        let dir = test_dir("fsync-dir");
        assert!(durability::fsync_dir(&dir).await.is_ok());
        let missing = dir.join("nope");
        let err = durability::fsync_dir(&missing).await;
        if cfg!(unix) {
            assert_eq!(err.unwrap_err().kind(), std::io::ErrorKind::NotFound);
        } else {
            assert!(err.is_ok());
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_is_wal_file_wal_file() {
        assert!(is_wal_file(
            "files/org/stype/stream/0/2025/03/24/00/2adf99cbc1277d5c/file.parquet"
        ));
        assert!(is_wal_file(
            "files/org/stype/stream/0/2025/03/24/00/2adf99cbc1277d5c/a=b/file.parquet"
        ));
    }

    #[test]
    fn test_is_wal_file_storage_file() {
        assert!(!is_wal_file(
            "files/org/stype/stream/2025/03/24/00/file.parquet"
        ));
        assert!(!is_wal_file(
            "files/org/stype/stream/2025/03/24/00/a=b/file.parquet"
        ));
    }

    #[test]
    fn test_is_wal_file_not_local_mode() {
        assert!(is_wal_file(
            "files/org/stype/stream/0/2025/03/24/00/2adf99cbc1277d5c/file.parquet"
        ));
        assert!(!is_wal_file(
            "files/org/stype/stream/2025/03/24/00/file.parquet"
        ));
        assert!(!is_wal_file(
            "files/org/stype/stream/2025/03/24/00/a=b/file.parquet"
        ));
    }
}
