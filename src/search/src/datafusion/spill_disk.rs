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

//! Process-wide DataFusion spill storage (`ZO_DATAFUSION_SPILL_DIR`).
//!
//! Unset, every query context keeps DataFusion's default disk manager: a
//! private directory under the OS temp dir with its own 100 GiB limit. In a
//! querier pod that is the container's writable layer on the node root disk.
//! Set, all contexts share one manager rooted at the configured directory
//! (the data volume) under one total limit, `ZO_DATAFUSION_SPILL_MAX_SIZE`.

use std::{
    fs,
    io::ErrorKind,
    path::Path,
    sync::{Arc, LazyLock},
    time::Duration,
};

use datafusion::execution::{
    disk_manager::{DiskManager, DiskManagerMode},
    runtime_env::RuntimeEnvBuilder,
};

/// Prefix of the directory DataFusion creates under each configured root
/// (`datafusion-execution` disk_manager.rs: `Builder::new().prefix("datafusion-")`).
const DATAFUSION_DIR_PREFIX: &str = "datafusion-";
/// Held with an exclusive lock for the life of the process, so leftovers are
/// removed only when no other process uses the directory.
const LOCK_FILE_NAME: &str = ".openobserve-spill.lock";
/// Free space left to the other users of the filesystem when the limit is
/// clamped: 5% of the filesystem, at least this much.
const MIN_FREE_RESERVE: u64 = 1 << 30;
const METRICS_INTERVAL: Duration = Duration::from_secs(5);

/// `(total, available)` bytes of the filesystem holding a path.
type FilesystemSpace = Option<(u64, u64)>;

struct SharedSpill {
    manager: Arc<DiskManager>,
    _lock: fs::File,
}

#[derive(Debug, PartialEq, Eq)]
struct Leftovers {
    entries: usize,
    bytes: u64,
}

static SHARED: LazyLock<Option<SharedSpill>> = LazyLock::new(|| {
    let cfg = config::get_config();
    let dir = cfg.limit.datafusion_spill_dir.as_str();
    if dir.is_empty() {
        return None;
    }
    match open(
        Path::new(dir),
        cfg.limit.datafusion_spill_max_size as u64,
        filesystem_space,
    ) {
        Ok(shared) => {
            spawn_metrics(shared.manager.clone());
            Some(shared)
        }
        Err(e) => {
            log::error!(
                "[DATAFUSION] shared spill directory {dir:?} unavailable, queries keep the per-context OS temp directory: {e}"
            );
            None
        }
    }
});

/// Attach the process-wide spill manager when `ZO_DATAFUSION_SPILL_DIR` is set.
pub(super) fn configure(mut builder: RuntimeEnvBuilder) -> RuntimeEnvBuilder {
    if let Some(shared) = SHARED.as_ref() {
        attach(&mut builder, &shared.manager);
    }
    builder
}

fn attach(builder: &mut RuntimeEnvBuilder, manager: &Arc<DiskManager>) {
    // In DF56 a disk manager builder takes precedence over a manager: clear it.
    builder.disk_manager_builder = None;
    builder.disk_manager = Some(manager.clone());
}

fn open(
    dir: &Path,
    configured_limit: u64,
    space: impl FnOnce(&Path) -> FilesystemSpace,
) -> Result<SharedSpill, String> {
    fs::create_dir_all(dir).map_err(|e| format!("create {dir:?}: {e}"))?;
    let lock_path = dir.join(LOCK_FILE_NAME);
    let lock = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .map_err(|e| format!("open {lock_path:?}: {e}"))?;
    lock.try_lock().map_err(|e| match e {
        fs::TryLockError::WouldBlock => {
            format!("{lock_path:?} is locked by another process using the same directory")
        }
        fs::TryLockError::Error(e) => format!("lock {lock_path:?}: {e}"),
    })?;
    let leftovers = remove_leftovers(dir)?;
    let space = space(dir);
    let limit = clamp_limit(configured_limit, space);
    let manager = DiskManager::builder()
        .with_mode(DiskManagerMode::Directories(vec![dir.to_path_buf()]))
        .with_max_temp_directory_size(limit)
        .build()
        .map_err(|e| format!("build disk manager for {dir:?}: {e}"))?;
    let manager = Arc::new(manager);
    // Fail at first use on an unwritable directory, not in the middle of a query.
    drop(
        manager
            .create_tmp_file("openobserve-spill-probe")
            .map_err(|e| format!("create a spill file in {dir:?}: {e}"))?,
    );
    log::info!(
        "[DATAFUSION] shared spill directory {dir:?}: limit={limit} bytes (configured {configured_limit}, filesystem total/available {space:?}), removed {} leftover entries ({} bytes)",
        leftovers.entries,
        leftovers.bytes
    );
    Ok(SharedSpill {
        manager,
        _lock: lock,
    })
}

/// Remove the directories a previous process left behind (killed before its
/// spill files were dropped). Only DataFusion-owned entries are touched.
fn remove_leftovers(dir: &Path) -> Result<Leftovers, String> {
    let mut leftovers = Leftovers {
        entries: 0,
        bytes: 0,
    };
    let entries = fs::read_dir(dir).map_err(|e| format!("read {dir:?}: {e}"))?;
    for entry in entries {
        let entry = entry.map_err(|e| format!("read {dir:?}: {e}"))?;
        if !entry
            .file_name()
            .to_string_lossy()
            .starts_with(DATAFUSION_DIR_PREFIX)
        {
            continue;
        }
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|e| format!("stat {path:?}: {e}"))?;
        let bytes = if file_type.is_dir() {
            entry_bytes(&path)
        } else {
            entry.metadata().map(|m| m.len()).unwrap_or(0)
        };
        let removed = if file_type.is_dir() {
            fs::remove_dir_all(&path)
        } else {
            fs::remove_file(&path)
        };
        match removed {
            Ok(()) => {
                leftovers.entries += 1;
                leftovers.bytes += bytes;
            }
            Err(e) if e.kind() == ErrorKind::NotFound => {}
            Err(e) => return Err(format!("remove leftover {path:?}: {e}")),
        }
    }
    Ok(leftovers)
}

fn entry_bytes(dir: &Path) -> u64 {
    config::utils::file::get_dir_size(&dir.to_string_lossy()) as u64
}

/// The configured limit, lowered to the free space of the filesystem minus a
/// reserve when that is smaller. Unknown space keeps the configured limit.
fn clamp_limit(configured: u64, space: FilesystemSpace) -> u64 {
    let Some((total, available)) = space else {
        return configured;
    };
    let reserve = (total / 20).max(MIN_FREE_RESERVE);
    configured.min(available.saturating_sub(reserve))
}

fn filesystem_space(dir: &Path) -> FilesystemSpace {
    let dir = dir.canonicalize().ok()?;
    config::utils::sysinfo::disk::get_disk_usage()
        .into_iter()
        .filter(|disk| dir.starts_with(&disk.mount_point))
        .max_by_key(|disk| disk.mount_point.len())
        .map(|disk| (disk.total_space, disk.available_space))
}

fn spawn_metrics(manager: Arc<DiskManager>) {
    let gauge = &config::metrics::DATAFUSION_SPILL_DISK;
    gauge
        .with_label_values(&["limit_bytes"])
        .set(i64::try_from(manager.max_temp_directory_size()).unwrap_or(i64::MAX));
    let spawned = std::thread::Builder::new()
        .name("df-spill-metrics".to_string())
        .spawn(move || {
            loop {
                let progress = manager.spilling_progress();
                gauge
                    .with_label_values(&["used_bytes"])
                    .set(i64::try_from(progress.current_bytes).unwrap_or(i64::MAX));
                gauge
                    .with_label_values(&["active_files"])
                    .set(i64::try_from(progress.active_files_count).unwrap_or(i64::MAX));
                std::thread::sleep(METRICS_INTERVAL);
            }
        });
    if let Err(e) = spawned {
        log::error!("[DATAFUSION] shared spill metrics thread not started: {e}");
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::Write,
        path::PathBuf,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use super::*;

    fn root() -> PathBuf {
        static ID: AtomicUsize = AtomicUsize::new(0);
        std::env::temp_dir().join(format!(
            "oo-df-spill-test-{}-{}",
            std::process::id(),
            ID.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn no_space(_: &Path) -> FilesystemSpace {
        None
    }

    #[test]
    fn unset_keeps_a_private_default_manager_per_context() {
        let a = RuntimeEnvBuilder::new().build().unwrap();
        let b = RuntimeEnvBuilder::new().build().unwrap();
        assert!(!Arc::ptr_eq(&a.disk_manager, &b.disk_manager));
        assert_eq!(a.disk_manager.max_temp_directory_size(), 100u64 << 30);
    }

    #[test]
    fn contexts_share_one_manager_and_one_limit() {
        let root = root();
        let shared = open(&root, 128, no_space).unwrap();
        let mut a = RuntimeEnvBuilder::new();
        attach(&mut a, &shared.manager);
        let mut b = RuntimeEnvBuilder::new().with_max_temp_directory_size(99999);
        attach(&mut b, &shared.manager);
        let (a, b) = (a.build().unwrap(), b.build().unwrap());
        assert!(Arc::ptr_eq(&a.disk_manager, &b.disk_manager));
        assert_eq!(b.disk_manager.max_temp_directory_size(), 128);

        let fa = a.disk_manager.create_tmp_file("a").unwrap();
        let fb = b.disk_manager.create_tmp_file("b").unwrap();
        let path = fa.path().unwrap().to_path_buf();
        assert!(path.starts_with(&root));
        let mut wa = fa.open_writer().unwrap();
        let mut wb = fb.open_writer().unwrap();
        wa.write_all(&[1; 80]).unwrap();
        assert!(wb.write_all(&[2; 80]).is_err(), "the limit is shared");
        wb.write_all(&[2; 48]).unwrap();
        assert_eq!(shared.manager.used_disk_space(), 128);
        drop((wa, wb, fa, fb));
        assert!(!path.exists());
        assert_eq!(shared.manager.spilling_progress().current_bytes, 0);
        drop((a, b, shared));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn only_datafusion_leftovers_are_removed() {
        let root = root();
        fs::create_dir_all(root.join("datafusion-old/nested")).unwrap();
        fs::write(root.join("datafusion-old/nested/spill.arrow"), [7; 1000]).unwrap();
        fs::write(root.join("datafusion-stray-file"), [7; 24]).unwrap();
        fs::create_dir_all(root.join("other")).unwrap();
        fs::write(root.join("keep.txt"), b"keep").unwrap();

        let shared = open(&root, 1 << 20, no_space).unwrap();
        assert!(!root.join("datafusion-old").exists());
        assert!(!root.join("datafusion-stray-file").exists());
        assert!(root.join("other").is_dir());
        assert!(root.join("keep.txt").is_file());
        assert!(root.join(LOCK_FILE_NAME).is_file());
        let prefixed = fs::read_dir(&root)
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with(DATAFUSION_DIR_PREFIX)
            })
            .count();
        assert_eq!(
            prefixed, 1,
            "only the running manager's own directory has the prefix"
        );
        drop(shared);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn leftover_accounting_counts_removed_entries_and_bytes() {
        let root = root();
        fs::create_dir_all(root.join("datafusion-a")).unwrap();
        fs::write(root.join("datafusion-a/x"), [1; 1000]).unwrap();
        fs::write(root.join("datafusion-b"), [1; 24]).unwrap();
        assert_eq!(
            remove_leftovers(&root).unwrap(),
            Leftovers {
                entries: 2,
                bytes: 1024
            }
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn a_locked_directory_is_refused_and_its_files_kept() {
        let root = root();
        let first = open(&root, 1 << 20, no_space).unwrap();
        let file = first.manager.create_tmp_file("held").unwrap();
        let held = file.path().unwrap().to_path_buf();
        let err = open(&root, 1 << 20, no_space).err().unwrap();
        assert!(err.contains("locked by another process"), "{err}");
        assert!(
            held.exists(),
            "a refused open must not remove live spill files"
        );
        drop((file, first));
        let reopened = open(&root, 1 << 20, no_space);
        assert!(reopened.is_ok(), "the lock is released with its owner");
        drop(reopened);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn an_unusable_directory_fails_at_open() {
        let root = root();
        fs::write(&root, b"not a directory").unwrap();
        assert!(open(&root, 128, no_space).is_err());
        fs::remove_file(root).unwrap();
    }

    #[test]
    fn the_limit_is_clamped_to_free_space_minus_a_reserve() {
        let gib = 1u64 << 30;
        assert_eq!(clamp_limit(192 * gib, None), 192 * gib);
        assert_eq!(
            clamp_limit(192 * gib, Some((2000 * gib, 500 * gib))),
            192 * gib
        );
        // 5% of 2000 GiB = 100 GiB reserve.
        assert_eq!(
            clamp_limit(192 * gib, Some((2000 * gib, 250 * gib))),
            150 * gib
        );
        // The reserve is at least 1 GiB on small filesystems.
        assert_eq!(clamp_limit(8 * gib, Some((4 * gib, 3 * gib))), 2 * gib);
        assert_eq!(clamp_limit(8 * gib, Some((2000 * gib, 50 * gib))), 0);
    }
}
