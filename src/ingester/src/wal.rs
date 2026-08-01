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
    fs::create_dir_all,
    path::{Path, PathBuf},
    sync::Arc,
};

use config::{
    get_config, metrics,
    utils::{
        async_walkdir::WalkDir, record_batch_ext::RecordBatchExt,
        schema::infer_json_schema_from_values, schema_ext::SchemaExt,
    },
};
use futures::StreamExt;
use hashbrown::HashMap;
use snafu::ResultExt;

use crate::{entry::RecordBatchEntry, errors::*, immutable, memtable, writer::WriterKey};

// check uncompleted parquet files
// the wal file process have 5 steps, all fsynced in the order documented on
// immutable::commit_staged_files:
// 1. write the memory file into disk with .par file extension, fsync file and dir
// 2. create a lock file with those file names, fsync file and dir
// 3. delete the wal file
// 4. rename the .par files to .parquet, fsync the dirs
// 5. delete the lock file
//
// so, there are some cases that the process is not completed:
// 1. the process is killed before step 2, so there are some .par files and have no lock file, need
//    delete those files
// 2. the process is killed before step 3, so there are some .par files and have lock file, the
//    files actually wrote to disk completely, need to continue step 3, 4 and 5
// 3. the process is killed before step 4, so there are some .par files and have lock file, the
//    files actually wrote to disk completely, need to continue step 4 and 5
// 4. the process is killed before step 5, so there are some .parquet files and have lock file, the
//    files actually wrote to disk completely, need to continue step 5
//
// the lock file is written to a temp name, fsynced and renamed into place
// (write_file_atomic_durable), so a lock that exists under its final name is
// complete: it is proof that every .par file it names is complete. A
// zero-length or unparseable lock can only be the in-place write of an older
// binary crashing mid-write -- it proves nothing and is treated as absent.
// Recovery replays the same ordering: promote and fsync first, delete the wal
// file only then, and drop the lock last.
pub(crate) async fn check_uncompleted_parquet_files() -> Result<()> {
    let cfg = config::get_config();
    // 1. get all .lock files
    let wal_dir = PathBuf::from(&cfg.common.data_wal_dir).join(crate::WAL_DIR_DEFAULT_PREFIX);
    // create wal dir if not exists
    create_dir_all(&wal_dir).context(OpenDirSnafu {
        path: wal_dir.clone(),
    })?;
    log::info!("Scanning lock files from {wal_dir:?}");
    let lock_files = wal_scan_files(&wal_dir, "lock").await.unwrap_or_default();
    log::info!("Found {} lock files", lock_files.len());

    // 2. finish the persist each lock file describes
    for lock_file in lock_files.iter() {
        log::warn!("found uncompleted wal file: {lock_file:?}");
        finish_locked_persist(lock_file).await?;
    }

    // 3. sweep staging leftovers of the atomic lock write: a crash between
    // writing <n>.lock.tmp and renaming it over <n>.lock leaves the temp file
    // behind. It never became a lock, so it promoted nothing -- the wal it
    // belonged to replays normally -- but it must not accumulate.
    cleanup_stale_lock_tmp_files(&wal_dir).await?;

    // 4. delete all the .par files
    let parquet_dir = PathBuf::from(&cfg.common.data_wal_dir).join("files");
    // create wal dir if not exists
    create_dir_all(&parquet_dir).context(OpenDirSnafu {
        path: parquet_dir.clone(),
    })?;
    let par_files = wal_scan_files(parquet_dir, "par").await.unwrap_or_default();
    for par_file in par_files.iter() {
        log::warn!("delete uncompleted par file: {par_file:?}");
        std::fs::remove_file(par_file).context(DeleteFileSnafu { path: par_file })?;
    }

    log::info!("Check uncompleted parquet files done");

    Ok(())
}

// finish steps 3, 4 and 5 for one lock file, in the order that keeps a crash
// during recovery recoverable: promote the .par files and fsync their dirs,
// only then delete the .wal file that could still replay them, and delete the
// lock file last so a crash before it just re-runs these same idempotent steps
async fn finish_locked_persist(lock_file: &Path) -> Result<()> {
    let bytes = std::fs::read(lock_file).context(OpenFileSnafu { path: lock_file })?;
    // a lock is complete by construction (temp write + atomic rename), so an
    // empty or unparseable one is a partial in-place write from an older
    // binary crashing between create and fsync. It commits nothing: treat it
    // as absent -- drop the bogus lock, keep the wal for replay, and let the
    // stray-.par sweep collect whatever files it may have named.
    let content = String::from_utf8(bytes).unwrap_or_default();
    let par_files: Vec<PathBuf> = content
        .lines()
        .filter(|line| !line.is_empty())
        .map(PathBuf::from)
        .collect();
    let parseable = !par_files.is_empty()
        && par_files
            .iter()
            .all(|p| p.extension().is_some_and(|ext| ext == "par"));
    if !parseable {
        log::error!(
            "lock file {lock_file:?} is empty or unparseable ({} bytes), treating it as absent: \
             the wal file is kept and will be replayed",
            content.len()
        );
        return remove_file_tolerant(lock_file);
    }

    // classify every file before touching any: promoting only part of a lock
    // whose other files are gone would leave the surviving rows both promoted
    // and replayable
    let mut to_rename: Vec<&PathBuf> = Vec::new();
    let mut lost: Vec<&PathBuf> = Vec::new();
    for par_file in par_files.iter() {
        if par_file.is_file() {
            to_rename.push(par_file);
        } else if !par_file.with_extension("parquet").is_file() {
            lost.push(par_file);
        }
        // else: already promoted by the crashed attempt or an earlier recovery
    }

    let wal_file = lock_file.with_extension("wal");
    if !lost.is_empty() {
        for par_file in lost.iter() {
            log::error!("lock file {lock_file:?} refers to a missing file: {par_file:?}");
        }
        if wal_file.is_file() {
            // the wal is the ONLY remaining copy of those rows: it must never
            // be deleted here. Promote nothing -- replay re-creates all of
            // this wal's data and the boot sweep removes the leftover .par
            // files -- and drop the lock so the replay scan picks the wal up.
            log::error!(
                "keeping wal file {wal_file:?} for replay: {} of {} locked files are missing",
                lost.len(),
                par_files.len()
            );
            return remove_file_tolerant(lock_file);
        }
        // no wal left to replay: promote what survived and say what did not.
        // If the mover uploaded the missing files before the crash the data is
        // in object storage; otherwise it is lost, and failing the boot would
        // not bring it back.
        log::error!(
            "wal file {wal_file:?} is already deleted and {} of {} locked files are missing: \
             promoting the survivors; the missing ones are either already uploaded or lost",
            lost.len(),
            par_files.len()
        );
    }

    let mut dirs: Vec<&Path> = Vec::with_capacity(par_files.len());
    for par_file in to_rename {
        let parquet_file = par_file.with_extension("parquet");
        log::warn!("rename par file: {par_file:?} to parquet");
        std::fs::rename(par_file, &parquet_file).context(RenameFileSnafu { path: par_file })?;
    }
    // fsync the directory of every named file, not just the renamed ones: the
    // crashed attempt may have promoted a file without reaching its own dir
    // fsync, and that promotion must be durable before the wal goes
    for par_file in par_files.iter() {
        if let Some(parent) = par_file.parent()
            && !dirs.contains(&parent)
        {
            dirs.push(parent);
        }
    }
    for dir in dirs {
        if let Err(e) = crate::durability::fsync_dir(dir).await
            && e.kind() != std::io::ErrorKind::NotFound
        {
            return Err(Error::OpenDirError {
                source: e,
                path: dir.to_path_buf(),
            });
        }
    }

    log::warn!("delete processed wal file: {wal_file:?}");
    remove_file_tolerant(&wal_file)?;

    log::warn!("delete lock file: {lock_file:?}");
    remove_file_tolerant(lock_file)
}

// remove a file, treating already-gone as done: recovery steps re-run after a
// crash and must converge on files a previous pass already deleted
fn remove_file_tolerant(path: &Path) -> Result<()> {
    if let Err(e) = std::fs::remove_file(path)
        && e.kind() != std::io::ErrorKind::NotFound
    {
        return Err(Error::DeleteFileError {
            source: e,
            path: path.to_path_buf(),
        });
    }
    Ok(())
}

// delete stray <n>.lock.tmp files under the wal dir: staging leftovers of the
// atomic lock write whose rename never happened. Anything else with a .tmp
// extension is left alone.
async fn cleanup_stale_lock_tmp_files(wal_dir: &Path) -> Result<()> {
    let tmp_files = wal_scan_files(wal_dir, "tmp").await.unwrap_or_default();
    for tmp_file in tmp_files.iter() {
        if !tmp_file.to_string_lossy().ends_with(".lock.tmp") {
            continue;
        }
        log::warn!("delete stale lock tmp file: {tmp_file:?}");
        remove_file_tolerant(tmp_file)?;
    }
    Ok(())
}

// replay wal files to create immutable
pub(crate) async fn replay_wal_files(wal_dir: PathBuf, wal_files: Vec<PathBuf>) -> Result<()> {
    if wal_files.is_empty() {
        return Ok(());
    }
    for wal_file in wal_files.iter() {
        log::warn!("replay wal file: {wal_file:?} starting...");
        let file_str = wal_file
            .strip_prefix(&wal_dir)
            .unwrap()
            .to_str()
            .unwrap()
            .replace('\\', "/")
            .to_string();
        let file_columns = file_str.split('/').collect::<Vec<_>>();
        let stream_type = file_columns[file_columns.len() - 2];
        let org_id = file_columns[file_columns.len() - 3];
        let idx: usize = file_columns[file_columns.len() - 4]
            .parse()
            .unwrap_or_default();
        let key = WriterKey::new_replay(org_id, stream_type);
        let mut memtable = memtable::MemTable::new();
        let mut reader = match wal::Reader::from_path(wal_file) {
            Ok(v) => v,
            Err(e) => {
                log::error!("Unable to open the wal file err: {e}, skip");
                continue;
            }
        };
        let mut total = 0;
        let mut i = 0;
        loop {
            if i > 0 && i % 1000 == 0 {
                log::warn!("replay wal file: {wal_file:?}, entries: {i}, records: {total}");
            }
            let entry = match reader.read_entry() {
                Ok(entry) => entry,
                Err(wal::Error::UnableToReadData { source }) => {
                    log::error!("Unable to read entry from: {source}, skip the entry");
                    continue;
                }
                Err(wal::Error::LengthMismatch { expected, actual }) => {
                    log::error!(
                        "Unable to read entry: Length mismatch: expected {expected}, actual {actual}, skip the entry"
                    );
                    continue;
                }
                Err(wal::Error::ChecksumMismatch { expected, actual }) => {
                    log::error!(
                        "Unable to read entry: Checksum mismatch: expected {expected}, actual {actual}, skip the entry"
                    );
                    continue;
                }
                Err(e) => {
                    return Err(Error::WalError { source: e });
                }
            };
            let Some(entry_bytes) = entry else {
                break;
            };
            let mut entry = match super::Entry::from_bytes(&entry_bytes) {
                Ok(v) => v,
                Err(Error::ReadDataError { source }) => {
                    log::error!("Unable to read entry from: {source}, skip the entry");
                    continue;
                }
                Err(e) => {
                    return Err(e);
                }
            };
            i += 1;

            // entries in Arrow IPC format carry the RecordBatch (with schema),
            // so they can be written to the memtable directly
            if let Some(batch) = entry.batch.take() {
                total += batch.num_rows();
                let schema = batch.schema();
                let arrow_size = batch.size();
                let batch_entry = RecordBatchEntry::new(
                    key.stream_type.clone(),
                    batch,
                    entry.data_size,
                    arrow_size,
                );
                memtable.write(schema, entry, batch_entry)?;
                continue;
            }

            total += entry.data.len();

            // Use Entry org_id if available, otherwise fall back to file path
            let org_id = if !entry.org_id.is_empty() {
                entry.org_id.as_ref()
            } else {
                org_id
            };

            let stream_name = entry.stream.as_ref();
            let infer_schema =
                infer_json_schema_from_values(stream_name, stream_type, entry.data.iter().cloned())
                    .context(InferJsonSchemaSnafu)?;
            let latest_schema = infra::schema::get_cache(org_id, &entry.stream, stream_type.into())
                .await
                .map_err(|e| Error::ExternalError {
                    source: Box::new(e),
                })?;
            entry.schema_key = latest_schema.hash_key().into();
            let infer_schema = Arc::new(infer_schema.cloned_from(latest_schema.schema()));
            let batch = entry.into_batch(key.stream_type.clone(), infer_schema.clone())?;
            memtable.write(infer_schema, entry, batch)?;
        }

        // directly dump the memtable to disk
        let start = std::time::Instant::now();
        let wal_path = wal_file.to_owned();
        let immutable = immutable::Immutable::new(idx, key, memtable);
        let stat = match immutable.persist(&wal_path).await {
            Ok(v) => v,
            Err(e) => {
                log::error!("persist wal file: {wal_file:?} to disk error: {e}");
                continue;
            }
        };

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

        log::warn!(
            "replay wal file: {:?} done, json_size: {}, arrow_size: {}, file_num: {} batch_num: {}, took: {} ms",
            wal_path.to_string_lossy(),
            stat.json_size,
            stat.arrow_size,
            stat.file_num,
            stat.batch_num,
            start.elapsed().as_millis(),
        );
    }

    Ok(())
}

pub(crate) async fn wal_scan_files(
    root_dir: impl Into<PathBuf>,
    ext: &str,
) -> Result<Vec<PathBuf>> {
    Ok(WalkDir::new(root_dir.into())
        .filter_map(|entry| async move {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.is_file() {
                let path_ext = path
                    .extension()
                    .and_then(|s| s.to_str())
                    .unwrap_or_default();
                if path_ext == ext { Some(path) } else { None }
            } else {
                None
            }
        })
        .collect()
        .await)
}

/// Collect parquet file metrics from the files directory
/// Counts parquet files that are pending upload to object store
pub async fn collect_wal_parquet_metrics() -> Result<()> {
    let cfg = get_config();
    let parquet_dir = PathBuf::from(&cfg.common.data_wal_dir).join("files");

    // Get all parquet files
    let parquet_files = match wal_scan_files(parquet_dir, "parquet").await {
        Ok(files) => files,
        Err(_) => return Ok(()), // Directory doesn't exist or no files
    };

    // Count parquet files by org_id and stream_type
    let mut parquet_counts: HashMap<(String, String), i64> = HashMap::new();

    for file_path in parquet_files {
        // Parse the file path to extract org_id and stream_type
        // Path format: files/org_id/stream_type/stream_name/...
        let path_str = file_path.to_string_lossy();
        let parts: Vec<&str> = path_str.split('/').collect();

        // Find the "files" directory and extract org_id and stream_type from there
        if let Some(files_idx) = parts.iter().position(|&p| p == "files")
            && parts.len() > files_idx + 2
        {
            let org_id = parts[files_idx + 1];
            let stream_type = parts[files_idx + 2];

            if !org_id.is_empty() && !stream_type.is_empty() {
                let key = (org_id.to_string(), stream_type.to_string());
                *parquet_counts.entry(key).or_insert(0) += 1;
            }
        }
    }

    // Update metrics with current counts
    for ((org_id, stream_type), count) in parquet_counts {
        metrics::INGEST_PARQUET_FILES
            .with_label_values(&[&org_id, &stream_type])
            .set(count);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("oo-ingester-wal-{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write_lock(lock_file: &Path, par_files: &[PathBuf]) {
        let body = par_files
            .iter()
            .map(|p| p.to_string_lossy().to_string())
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(lock_file, body).unwrap();
    }

    #[tokio::test]
    async fn test_finish_locked_persist_promotes_before_deleting_wal() {
        let dir = test_dir("recover");
        let par_dir = dir.join("files/org/logs/s/0/h1");
        std::fs::create_dir_all(&par_dir).unwrap();
        let par = par_dir.join("a.par");
        std::fs::write(&par, b"parquet-bytes").unwrap();
        let wal = dir.join("1.wal");
        std::fs::write(&wal, b"wal").unwrap();
        let lock = dir.join("1.lock");
        write_lock(&lock, std::slice::from_ref(&par));

        finish_locked_persist(&lock).await.unwrap();

        assert_eq!(
            std::fs::read(par.with_extension("parquet")).unwrap(),
            b"parquet-bytes"
        );
        assert!(!par.exists());
        assert!(!wal.exists());
        assert!(!lock.exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_finish_locked_persist_tolerates_already_done_steps() {
        let dir = test_dir("recover-partial");
        let par_dir = dir.join("files/org/logs/s/0/h1");
        std::fs::create_dir_all(&par_dir).unwrap();
        // crashed after step 4: the .parquet is there, the .wal is already gone
        let par = par_dir.join("a.par");
        let parquet = par.with_extension("parquet");
        std::fs::write(&parquet, b"parquet-bytes").unwrap();
        let lock = dir.join("2.lock");
        write_lock(&lock, std::slice::from_ref(&par));

        finish_locked_persist(&lock).await.unwrap();

        assert_eq!(std::fs::read(&parquet).unwrap(), b"parquet-bytes");
        assert_eq!(std::fs::read_dir(&par_dir).unwrap().count(), 1);
        assert!(!lock.exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_finish_locked_persist_blank_lock_keeps_the_wal() {
        let dir = test_dir("recover-blank");
        let lock = dir.join("3.lock");
        std::fs::write(&lock, b"\n\n").unwrap();
        let wal = dir.join("3.wal");
        std::fs::write(&wal, b"wal").unwrap();

        finish_locked_persist(&lock).await.unwrap();

        // a lock without a single named file proves nothing: it is a partial
        // write, so the wal survives for replay and only the lock goes
        assert!(wal.exists());
        assert!(!lock.exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_finish_locked_persist_empty_lock_keeps_the_wal() {
        let dir = test_dir("recover-empty-lock");
        // the F1 crash window: open(O_CREAT|O_TRUNC) survived, the content did
        // not -- the lock exists with zero bytes
        let lock = dir.join("4.lock");
        std::fs::write(&lock, b"").unwrap();
        let wal = dir.join("4.wal");
        std::fs::write(&wal, b"wal").unwrap();

        finish_locked_persist(&lock).await.unwrap();

        assert!(wal.exists());
        assert!(!lock.exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_finish_locked_persist_truncated_lock_keeps_the_wal() {
        let dir = test_dir("recover-truncated-lock");
        let par_dir = dir.join("files/org/logs/s/0/h1");
        std::fs::create_dir_all(&par_dir).unwrap();
        let par = par_dir.join("a.par");
        std::fs::write(&par, b"parquet-bytes").unwrap();
        // a partial flush cut the second path mid-way: the line does not name
        // a .par file, so the whole lock is untrustworthy
        let lock = dir.join("5.lock");
        std::fs::write(
            &lock,
            format!("{}\n{}", par.display(), par_dir.join("b.pa").display()),
        )
        .unwrap();
        let wal = dir.join("5.wal");
        std::fs::write(&wal, b"wal").unwrap();

        finish_locked_persist(&lock).await.unwrap();

        assert!(wal.exists());
        assert!(!lock.exists());
        // nothing was promoted off an untrusted lock; the replay owns the data
        // and the boot sweep collects the stray .par
        assert!(par.exists());
        assert!(!par.with_extension("parquet").exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_finish_locked_persist_non_utf8_lock_keeps_the_wal() {
        let dir = test_dir("recover-binary-lock");
        let lock = dir.join("6.lock");
        std::fs::write(&lock, [0xff, 0xfe, 0x00, 0x41]).unwrap();
        let wal = dir.join("6.wal");
        std::fs::write(&wal, b"wal").unwrap();

        finish_locked_persist(&lock).await.unwrap();

        assert!(wal.exists());
        assert!(!lock.exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_finish_locked_persist_keeps_wal_when_a_locked_file_is_lost() {
        let dir = test_dir("recover-lost-file");
        let par_dir = dir.join("files/org/logs/s/0/h1");
        std::fs::create_dir_all(&par_dir).unwrap();
        let par_a = par_dir.join("a.par");
        std::fs::write(&par_a, b"parquet-bytes").unwrap();
        // b has neither .par nor .parquet left: the wal is the only copy
        let par_b = par_dir.join("b.par");
        let lock = dir.join("7.lock");
        write_lock(&lock, &[par_a.clone(), par_b.clone()]);
        let wal = dir.join("7.wal");
        std::fs::write(&wal, b"wal").unwrap();

        finish_locked_persist(&lock).await.unwrap();

        // the wal survives for replay, and no file of this lock was promoted:
        // replay re-creates them all, a promoted survivor would be a duplicate
        assert!(wal.exists());
        assert!(!lock.exists());
        assert!(par_a.exists());
        assert!(!par_a.with_extension("parquet").exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_finish_locked_persist_promotes_survivors_when_wal_is_gone() {
        let dir = test_dir("recover-lost-no-wal");
        let par_dir = dir.join("files/org/logs/s/0/h1");
        std::fs::create_dir_all(&par_dir).unwrap();
        let par_a = par_dir.join("a.par");
        std::fs::write(&par_a, b"parquet-bytes").unwrap();
        // b is gone (mover uploaded and reaped it, or it is lost) and so is
        // the wal: promoting a is the only way to keep its rows queryable
        let par_b = par_dir.join("b.par");
        let lock = dir.join("8.lock");
        write_lock(&lock, &[par_a.clone(), par_b.clone()]);

        finish_locked_persist(&lock).await.unwrap();

        assert_eq!(
            std::fs::read(par_a.with_extension("parquet")).unwrap(),
            b"parquet-bytes"
        );
        assert!(!par_a.exists());
        assert!(!lock.exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_cleanup_stale_lock_tmp_files_only_touches_lock_tmps() {
        let dir = test_dir("cleanup-lock-tmp");
        let sub = dir.join("0/org/logs");
        std::fs::create_dir_all(&sub).unwrap();
        let stale = sub.join("9.lock.tmp");
        std::fs::write(&stale, b"partial").unwrap();
        let unrelated = sub.join("other.tmp");
        std::fs::write(&unrelated, b"keep").unwrap();
        let lock = sub.join("9.lock");
        std::fs::write(&lock, b"files/a.par").unwrap();

        cleanup_stale_lock_tmp_files(&dir).await.unwrap();

        assert!(!stale.exists());
        assert!(unrelated.exists());
        assert!(lock.exists());

        let _ = std::fs::remove_dir_all(&dir);
    }
}
