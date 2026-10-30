// Copyright 2026 OpenObserve Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

//! Opt-in coverage of small logs L0 remainders. Never adopt an object merely
//! because HEAD finds it: only the primary file-list generation is published.

use std::{future::Future, time::Duration};

use anyhow::Context;
use config::{
    get_config,
    meta::stream::{FileKey, StreamType},
};
use object_store::ObjectMeta;
use tokio::{sync::Mutex, time::Instant};

use super::{merge::wait_for_merge_cancellation, worker::MergeCancellation};

static NEXT_PROBE: Mutex<Option<Instant>> = Mutex::const_new(None);

pub(super) fn enabled(stream_type: StreamType) -> bool {
    stream_type == StreamType::Logs
        && !config::is_vix_index_disabled(stream_type)
        && get_config().common.vix_logs_index_coverage_minutes > 0
}

#[derive(Debug)]
pub(super) enum Coverage {
    /// No sidecar object at the authoritative generation.
    Missing(FileKey),
    /// An object exists, but no metadata transaction published it. Rebuild
    /// with a new generation rather than exposing potentially incomplete bytes.
    Unpublished(FileKey),
    /// Primary metadata has an index. Refresh readers; do not rebuild.
    StaleMetadata(FileKey),
    NotDue,
    Retired,
}

async fn admit<'a>(
    next: &'a Mutex<Option<Instant>>,
    spacing: Duration,
    cancel: &MergeCancellation,
) -> anyhow::Result<tokio::sync::MutexGuard<'a, Option<Instant>>> {
    cancel.check("index coverage admission")?;
    let mut guard = tokio::select! {
        guard = next.lock() => guard,
        _ = wait_for_merge_cancellation(cancel) => {
            anyhow::bail!("index coverage cancelled while waiting for probe admission")
        }
    };
    if let Some(deadline) = *guard {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => {},
            _ = wait_for_merge_cancellation(cancel) => {
                anyhow::bail!("index coverage cancelled during probe rate limit")
            }
        }
    }
    cancel.check("index coverage probe")?;
    *guard = Some(Instant::now() + spacing);
    Ok(guard)
}

/// The guard serializes primary reads and HEADs across every partition/job
/// in this process. Existing merge workers and memory admission bound repairs.
pub(super) async fn probe(file: &FileKey, cancel: &MergeCancellation) -> anyhow::Result<Coverage> {
    let cfg = get_config();
    let rate = cfg.common.vix_logs_index_coverage_probes_per_minute.max(1);
    let _guard = admit(
        &NEXT_PROBE,
        Duration::from_secs_f64(60.0 / f64::from(rate)),
        cancel,
    )
    .await?;
    let operation = async {
        let current = infra::file_list::get_index_coverage_file(file)
            .await
            .with_context(|| format!("read primary index coverage metadata for {}", file.key))?;
        inspect(
            file,
            current,
            config::utils::time::now_micros(),
            cfg.common.vix_logs_index_coverage_minutes,
            |key| {
                let account = file.account.clone();
                async move { infra::storage::head(&account, &key).await }
            },
        )
        .await
    };
    bounded(
        file,
        cancel,
        Duration::from_secs(cfg.limit.vix_fetch_timeout.max(1)),
        operation,
    )
    .await
}

async fn bounded<T>(
    file: &FileKey,
    cancel: &MergeCancellation,
    timeout: Duration,
    operation: impl Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
    cancel.check("index coverage operation")?;
    tokio::select! {
        biased;
        _ = wait_for_merge_cancellation(cancel) => {
            anyhow::bail!("index coverage operation cancelled for {}", file.key)
        }
        result = tokio::time::timeout(timeout, operation) => {
            result.with_context(|| format!("index coverage operation timed out for {}", file.key))?
        }
    }
}

async fn inspect<F, Fut>(
    snapshot: &FileKey,
    current: Option<FileKey>,
    now_micros: i64,
    target_minutes: u64,
    mut head: F,
) -> anyhow::Result<Coverage>
where
    F: FnMut(String) -> Fut,
    Fut: Future<Output = object_store::Result<ObjectMeta>>,
{
    let Some(current) = current else {
        return Ok(Coverage::Retired);
    };
    if current.deleted {
        return Ok(Coverage::Retired);
    }
    anyhow::ensure!(
        current.id == snapshot.id
            && current.key == snapshot.key
            && current.account == snapshot.account,
        "index coverage identity changed for {} (id {})",
        snapshot.key,
        snapshot.id
    );
    if current.meta.index_size > 0 {
        let sidecar = config::vix_sidecar_key(&current.key, current.meta.index_generation)
            .with_context(|| format!("invalid published sidecar identity {}", current.key))?;
        return match head(sidecar.clone()).await {
            Ok(meta) => {
                anyhow::ensure!(
                    i64::try_from(meta.size).ok() == Some(current.meta.index_size),
                    "published index coverage sidecar size mismatch for {sidecar}"
                );
                Ok(Coverage::StaleMetadata(current))
            }
            Err(object_store::Error::NotFound { .. }) => {
                log::warn!(
                    "[COMPACTOR] index coverage classification=published_index_missing file={} sidecar={sidecar}",
                    current.key
                );
                Ok(Coverage::Missing(current))
            }
            Err(error) => Err(error).with_context(|| format!("HEAD published sidecar {sidecar}")),
        };
    }
    if target_minutes == 0 {
        return Ok(Coverage::NotDue);
    }
    let data = head(current.key.clone())
        .await
        .with_context(|| format!("HEAD index coverage data {}", current.key))?;
    anyhow::ensure!(
        i64::try_from(data.size).ok() == Some(current.meta.compressed_size),
        "index coverage data size mismatch for {}",
        current.key
    );
    // Event timestamps and partition hours are not file creation clocks.
    let age = now_micros.saturating_sub(data.last_modified.timestamp_micros());
    let deadline = i64::try_from(target_minutes)
        .unwrap_or(i64::MAX)
        .saturating_mul(60_000_000);
    if age < deadline {
        return Ok(Coverage::NotDue);
    }
    let sidecar = config::vix_sidecar_key(&current.key, current.meta.index_generation)
        .with_context(|| format!("invalid coverage sidecar identity {}", current.key))?;
    match head(sidecar.clone()).await {
        Ok(_) => {
            log::info!(
                "[COMPACTOR] index coverage classification=unpublished_object file={} sidecar={sidecar}; never adopt without CAS",
                current.key
            );
            Ok(Coverage::Unpublished(current))
        }
        Err(object_store::Error::NotFound { .. }) => {
            log::info!(
                "[COMPACTOR] index coverage classification=missing_index file={} sidecar={sidecar} age_micros={age}",
                current.key
            );
            Ok(Coverage::Missing(current))
        }
        Err(error) => Err(error).with_context(|| format!("HEAD index coverage sidecar {sidecar}")),
    }
}

/// Re-broadcast only authoritative metadata. A HEAD-only orphan is never
/// attached. This is the same cache invalidation used after sidecar CAS.
pub(super) async fn refresh(file: &FileKey, cancel: &MergeCancellation) -> anyhow::Result<()> {
    bounded(
        file,
        cancel,
        Duration::from_secs(get_config().limit.vix_fetch_timeout.max(1)),
        refresh_metadata(file),
    )
    .await
}

async fn refresh_metadata(file: &FileKey) -> anyhow::Result<()> {
    if config::cluster::LOCAL_NODE.is_querier() {
        if !get_config().common.local_mode {
            infra::file_list::LOCAL_CACHE
                .remove(&file.key)
                .await
                .with_context(|| {
                    format!("invalidate stale index coverage metadata {}", file.key)
                })?;
        }
        ::search::vix::reader_cache::GLOBAL_CACHE.remove(&file.key);
        ::search::vix::cache::GLOBAL_CACHE.remove_file_entries(std::iter::once(file.key.as_str()));
    }
    crate::service::db::file_list::broadcast::send(std::slice::from_ref(file))
        .await
        .with_context(|| format!("broadcast current index coverage metadata {}", file.key))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use object_store::{ObjectStoreExt, memory::InMemory, path::Path};

    use super::*;

    async fn fixture() -> (Arc<InMemory>, FileKey, i64) {
        let store = Arc::new(InMemory::new());
        let mut file = FileKey::from_file_name("files/org/logs/stream/2026/10/30/00/l0.vix");
        file.id = 42;
        file.meta.compressed_size = 4;
        // Future event time deliberately differs from object creation time.
        file.meta.min_ts = i64::MAX - 1;
        file.meta.max_ts = i64::MAX;
        store
            .put(
                &Path::from(file.key.as_str()),
                Bytes::from_static(b"data").into(),
            )
            .await
            .unwrap();
        let modified = store
            .head(&Path::from(file.key.as_str()))
            .await
            .unwrap()
            .last_modified
            .timestamp_micros();
        (store, file, modified)
    }

    async fn check(
        store: Arc<InMemory>,
        file: &FileKey,
        current: Option<FileKey>,
        now: i64,
        minutes: u64,
    ) -> anyhow::Result<Coverage> {
        inspect(file, current, now, minutes, |key| {
            let store = store.clone();
            async move { store.head(&Path::from(key)).await }
        })
        .await
    }

    #[tokio::test]
    async fn coverage_deadline_uses_object_age_and_preserves_disabled_policy() {
        let (store, file, modified) = fixture().await;
        for now in [modified - 1, modified, modified + 299_999_999] {
            assert!(matches!(
                check(store.clone(), &file, Some(file.clone()), now, 5)
                    .await
                    .unwrap(),
                Coverage::NotDue
            ));
        }
        assert!(matches!(
            check(
                store.clone(),
                &file,
                Some(file.clone()),
                modified + 300_000_000,
                5
            )
            .await
            .unwrap(),
            Coverage::Missing(_)
        ));
        assert!(matches!(
            check(store, &file, Some(file.clone()), i64::MAX, 0)
                .await
                .unwrap(),
            Coverage::NotDue
        ));
    }

    #[tokio::test]
    async fn coverage_distinguishes_published_metadata_from_unpublished_objects() {
        let (store, file, modified) = fixture().await;
        let sidecar = config::vix_sidecar_key(&file.key, 0).unwrap();
        store
            .put(&Path::from(sidecar), Bytes::from_static(b"orphan").into())
            .await
            .unwrap();
        assert!(matches!(
            check(
                store.clone(),
                &file,
                Some(file.clone()),
                modified + 300_000_000,
                5
            )
            .await
            .unwrap(),
            Coverage::Unpublished(_)
        ));
        let mut current = file.clone();
        current.meta.index_generation = 99;
        current.meta.index_size = 123;
        assert!(matches!(
            check(store.clone(), &file, Some(current.clone()), modified, 5)
                .await
                .unwrap(),
            Coverage::Missing(_)
        ));
        store
            .put(
                &Path::from(config::vix_sidecar_key(&file.key, 99).unwrap()),
                Bytes::from(vec![0; 123]).into(),
            )
            .await
            .unwrap();
        let result = check(store.clone(), &file, Some(current.clone()), modified, 5)
            .await
            .unwrap();
        let Coverage::StaleMetadata(fresh) = result else {
            panic!("expected stale metadata")
        };
        assert_eq!(fresh, current);
        assert!(matches!(
            check(store, &file, None, modified, 5).await.unwrap(),
            Coverage::Retired
        ));
        assert_eq!(
            file.meta.index_size, 0,
            "probe never mutates the visible snapshot"
        );
    }

    #[tokio::test]
    async fn coverage_probe_failures_do_not_publish_or_hide_data() {
        let (store, file, modified) = fixture().await;
        let mut wrong = file.clone();
        wrong.meta.compressed_size = 999;
        assert!(
            check(store.clone(), &file, Some(wrong), modified + 300_000_000, 5)
                .await
                .unwrap_err()
                .to_string()
                .contains("size mismatch")
        );
        let error = inspect(&file, Some(file.clone()), modified, 5, |_key| async {
            Err(object_store::Error::Generic {
                store: "test",
                source: std::io::Error::other("injected HEAD failure").into(),
            })
        })
        .await
        .unwrap_err();
        assert!(format!("{error:#}").contains("injected HEAD failure"));
        assert_eq!(
            store
                .get(&Path::from(file.key))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            Bytes::from_static(b"data")
        );
    }

    #[tokio::test]
    async fn coverage_admission_is_serial_and_cancellable() {
        let next = Mutex::new(None);
        let cancel = MergeCancellation::default();
        let guard = admit(&next, Duration::from_secs(60), &cancel)
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(
                Duration::from_millis(10),
                admit(&next, Duration::ZERO, &cancel)
            )
            .await
            .is_err()
        );
        drop(guard);
        let cancelled = MergeCancellation::default();
        cancelled.cancel();
        assert!(admit(&next, Duration::ZERO, &cancelled).await.is_err());
        let waiting = admit(&next, Duration::ZERO, &cancel);
        let stop = async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            cancel.cancel();
        };
        let (result, ()) = tokio::join!(waiting, stop);
        assert!(result.is_err());
        assert!(next.try_lock().is_ok(), "cancellation releases the limiter");
    }

    #[tokio::test]
    async fn coverage_admission_spaces_successive_probes() {
        let next = Mutex::new(None);
        let cancel = MergeCancellation::default();
        let spacing = Duration::from_millis(30);
        let start = Instant::now();
        drop(admit(&next, spacing, &cancel).await.unwrap());
        drop(admit(&next, spacing, &cancel).await.unwrap());
        assert!(start.elapsed() >= spacing);
    }

    #[tokio::test]
    async fn coverage_timeout_and_cancellation_drop_inflight_resources() {
        let (_, file, _) = fixture().await;
        let lock = Mutex::new(());
        for cancelled in [false, true] {
            let cancel = MergeCancellation::default();
            let operation = async {
                let _guard = lock.lock().await;
                std::future::pending::<anyhow::Result<()>>().await
            };
            let timeout = if cancelled {
                Duration::from_secs(5)
            } else {
                Duration::from_millis(20)
            };
            let stop = async {
                if cancelled {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    cancel.cancel();
                }
            };
            let (result, ()) = tokio::join!(bounded(&file, &cancel, timeout, operation), stop);
            let error = format!("{:#}", result.unwrap_err());
            assert!(error.contains(if cancelled { "cancelled" } else { "timed out" }));
            assert!(error.contains(&file.key));
            assert!(lock.try_lock().is_ok(), "drop releases in-flight resources");
        }
    }

    #[tokio::test]
    async fn coverage_rejects_changed_identity_and_published_size() {
        let (store, file, modified) = fixture().await;
        let mut current = file.clone();
        current.id += 1;
        assert!(
            check(store.clone(), &file, Some(current), modified, 5)
                .await
                .unwrap_err()
                .to_string()
                .contains("identity changed")
        );
        let mut current = file.clone();
        current.meta.index_size = 123;
        store
            .put(
                &Path::from(config::vix_sidecar_key(&file.key, 0).unwrap()),
                Bytes::from_static(b"short").into(),
            )
            .await
            .unwrap();
        assert!(
            check(store, &file, Some(current), modified, 5)
                .await
                .unwrap_err()
                .to_string()
                .contains("size mismatch")
        );
    }
}
