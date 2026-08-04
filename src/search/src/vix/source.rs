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

//! [`VixRangeSource`] implementations over this process's storage layers,
//! plus the `ZO_VIX_READ_MODE` switch.
//!
//! `vortex_index` requires source futures to be executor-agnostic (they are
//! polled on vortex's single-thread executor, which has no tokio reactor).
//! Both implementations therefore spawn the real IO onto a captured tokio
//! [`Handle`] and hand the result back over a oneshot channel.
//!
//! Robustness/observability added here:
//! - every fetch is bounded by `ZO_VIX_FETCH_TIMEOUT` (a hung S3 connection becomes an error the
//!   per-file retry/degradation path handles instead of stalling the query),
//! - fetch count/bytes tick the global `VIX_FETCH_*` metrics (label `search` for the index-eval
//!   ladder, `scan` for the DataFusion docs scan) and, for [`LadderRangeSource`], the per-query
//!   [`FetchStats`] that vix_search reports in its ScanStats/log line.
//!
//! History: this source used to persist the whole term-dictionary blob into
//! the disk cache (`{file_key}::vix-dict`) because every ranged open fetched
//! the entire dict (~33 MiB per 200M-benchmark file). Lazy dictionary
//! loading (vortex_index) made opens directory-only and FST cells
//! point-reads, so the write-back — ~26 GB of synchronous disk writes on a
//! cold 1057-file query, on the query path — was removed with it.

use std::{
    ops::Range,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use futures::{FutureExt, future::BoxFuture};
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use tokio::runtime::Handle;
use vortex_index::VixRangeSource;

/// How `.vix` containers are read from storage (`ZO_VIX_READ_MODE`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VixReadMode {
    /// Download the whole object through the file cache ladder, then open
    /// the bytes in memory (the pre-F2 behavior).
    Cached,
    /// Open the object over range fetches: puffin footer + dictionary
    /// eagerly, postings/docs chunks on demand. Cold queries stop
    /// downloading whole objects; every fetch still goes through the
    /// memory/disk cache ladder, so hot (cached) files are served locally.
    Ranged,
}

/// The configured read mode. The config layer guarantees the value is
/// `cached` or `ranged` (default `ranged`).
pub fn vix_read_mode() -> VixReadMode {
    match config::get_config().common.vix_read_mode.as_str() {
        "cached" => VixReadMode::Cached,
        _ => VixReadMode::Ranged,
    }
}

/// Per-query fetch accounting: number of range fetches issued and bytes
/// fetched. Threaded from the sources into vix_search's ScanStats and the
/// search-inspector log line.
#[derive(Debug, Default)]
pub struct FetchStats {
    pub fetches: AtomicU64,
    pub bytes: AtomicU64,
}

/// The configured per-fetch timeout (`ZO_VIX_FETCH_TIMEOUT`, seconds).
/// `None` when disabled (0).
fn vix_fetch_timeout() -> Option<Duration> {
    let secs = config::get_config().limit.vix_fetch_timeout;
    (secs > 0).then(|| Duration::from_secs(secs))
}

/// Global in-flight fetch gate (`ZO_VIX_FETCH_CONCURRENCY`). Wide eval
/// fan-out (eval_concurrency defaults to 4x cores) used to put dozens of
/// multi-MB dictionary fetches on the device at once; each fetch's WALL
/// time then included queuing behind the others, ZO_VIX_FETCH_TIMEOUT
/// fired on healthy IO, and the per-file fallback converted an
/// index-answerable query into a full scan (measured: 50/55 files fell
/// back, 453s of a 486s cold count — ENGINE-BACKLOG #18). The permit is
/// acquired BEFORE the timeout window opens, so queue wait can never
/// manufacture a timeout; the timeout bounds only the active fetch.
fn fetch_gate() -> Option<&'static tokio::sync::Semaphore> {
    use std::sync::OnceLock;
    static GATE: OnceLock<Option<tokio::sync::Semaphore>> = OnceLock::new();
    GATE.get_or_init(|| {
        let permits = config::get_config().common.vix_fetch_concurrency;
        (permits > 0).then(|| tokio::sync::Semaphore::new(permits))
    })
    .as_ref()
}

/// Run `fut` on `handle` gated by `ZO_VIX_FETCH_CONCURRENCY` and bounded by
/// `ZO_VIX_FETCH_TIMEOUT` (active fetch only — queue wait is untimed), tick
/// the fetch metrics, and return an executor-agnostic future for its result
/// (a oneshot receiver works on any executor).
fn spawn_fetch(
    handle: &Handle,
    metric_path: &'static str,
    stats: Option<Arc<FetchStats>>,
    fut: impl Future<Output = anyhow::Result<Bytes>> + Send + 'static,
) -> BoxFuture<'static, anyhow::Result<Bytes>> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    handle.spawn(async move {
        let _permit = match fetch_gate() {
            Some(gate) => match gate.acquire().await {
                Ok(permit) => Some(permit),
                // the gate is never closed; treat a close as uncapped
                Err(_) => None,
            },
            None => None,
        };
        let result = match vix_fetch_timeout() {
            Some(timeout) => match tokio::time::timeout(timeout, fut).await {
                Ok(result) => result,
                Err(_) => Err(anyhow::anyhow!(
                    "vix range fetch timed out after {}s (ZO_VIX_FETCH_TIMEOUT)",
                    timeout.as_secs()
                )),
            },
            None => fut.await,
        };
        if let Ok(bytes) = &result {
            config::metrics::VIX_FETCH_COUNT_TOTAL
                .with_label_values(&[metric_path])
                .inc();
            config::metrics::VIX_FETCH_BYTES_TOTAL
                .with_label_values(&[metric_path])
                .inc_by(bytes.len() as u64);
            if let Some(stats) = &stats {
                stats.fetches.fetch_add(1, Ordering::Relaxed);
                stats.bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
            }
        }
        // The receiver may be gone (scan aborted); nothing to do then.
        let _ = tx.send(result);
    });
    async move {
        rx.await
            .map_err(|_| anyhow::anyhow!("range fetch task was cancelled"))?
    }
    .boxed()
}

/// [`spawn_fetch`] for a batched multi-range request: one gate permit, one
/// timeout window, metrics/stats tick once per contained range.
fn spawn_fetch_many(
    handle: &Handle,
    metric_path: &'static str,
    stats: Option<Arc<FetchStats>>,
    fut: impl Future<Output = anyhow::Result<Vec<Bytes>>> + Send + 'static,
) -> BoxFuture<'static, anyhow::Result<Vec<Bytes>>> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    handle.spawn(async move {
        let _permit = match fetch_gate() {
            Some(gate) => match gate.acquire().await {
                Ok(permit) => Some(permit),
                Err(_) => None,
            },
            None => None,
        };
        let result = match vix_fetch_timeout() {
            Some(timeout) => match tokio::time::timeout(timeout, fut).await {
                Ok(result) => result,
                Err(_) => Err(anyhow::anyhow!(
                    "vix range fetch timed out after {}s (ZO_VIX_FETCH_TIMEOUT)",
                    timeout.as_secs()
                )),
            },
            None => fut.await,
        };
        if let Ok(all) = &result {
            let bytes: u64 = all.iter().map(|b| b.len() as u64).sum();
            config::metrics::VIX_FETCH_COUNT_TOTAL
                .with_label_values(&[metric_path])
                .inc_by(all.len() as u64);
            config::metrics::VIX_FETCH_BYTES_TOTAL
                .with_label_values(&[metric_path])
                .inc_by(bytes);
            if let Some(stats) = &stats {
                stats.fetches.fetch_add(all.len() as u64, Ordering::Relaxed);
                stats.bytes.fetch_add(bytes, Ordering::Relaxed);
            }
        }
        let _ = tx.send(result);
    });
    async move {
        rx.await
            .map_err(|_| anyhow::anyhow!("range fetch task was cancelled"))?
    }
    .boxed()
}

/// A `.vix` object reached through the infra cache ladder
/// (`memory cache → disk cache → remote object store`, range-capable at
/// every level). Used by the index evaluation path (`vix_search`).
pub struct LadderRangeSource {
    account: String,
    location: Path,
    size: u64,
    handle: Handle,
    /// Per-query fetch accounting; fetches issued through a reader memoized
    /// by an earlier query tick that query's stats (plus, always, the global
    /// metrics).
    stats: Option<Arc<FetchStats>>,
}

impl LadderRangeSource {
    pub fn new(
        account: String,
        file: &str,
        size: u64,
        handle: Handle,
        stats: Option<Arc<FetchStats>>,
    ) -> Self {
        Self {
            account,
            location: Path::from(file),
            size,
            handle,
            stats,
        }
    }
}

impl VixRangeSource for LadderRangeSource {
    fn len(&self) -> u64 {
        self.size
    }

    fn fetch(&self, range: Range<u64>) -> BoxFuture<'static, anyhow::Result<Bytes>> {
        let account = self.account.clone();
        let location = self.location.clone();
        spawn_fetch(&self.handle, "search", self.stats.clone(), async move {
            infra::cache::storage::get_range(&account, &location, range)
                .await
                .map_err(anyhow::Error::from)
        })
    }

    fn fetch_many(
        &self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'static, anyhow::Result<Vec<Bytes>>> {
        let account = self.account.clone();
        let location = self.location.clone();
        spawn_fetch_many(&self.handle, "search", self.stats.clone(), async move {
            infra::cache::storage::get_ranges(&account, &location, &ranges)
                .await
                .map_err(anyhow::Error::from)
        })
    }

    fn describe(&self) -> String {
        self.location.to_string()
    }
}

/// A `.vix` object reached through a DataFusion-registered [`ObjectStore`]
/// (the `memory:///` store delegates to the same cache ladder). Used by the
/// core-file scan path (`VixCoreFormat` / `VixCoreOpener`).
pub struct StoreRangeSource {
    store: Arc<dyn ObjectStore>,
    location: Path,
    size: u64,
    handle: Handle,
}

impl StoreRangeSource {
    pub fn new(store: Arc<dyn ObjectStore>, location: Path, size: u64, handle: Handle) -> Self {
        Self {
            store,
            location,
            size,
            handle,
        }
    }
}

impl VixRangeSource for StoreRangeSource {
    fn len(&self) -> u64 {
        self.size
    }

    fn fetch(&self, range: Range<u64>) -> BoxFuture<'static, anyhow::Result<Bytes>> {
        let store = Arc::clone(&self.store);
        let location = self.location.clone();
        spawn_fetch(&self.handle, "scan", None, async move {
            store
                .get_range(&location, range)
                .await
                .map_err(anyhow::Error::from)
        })
    }

    fn describe(&self) -> String {
        self.location.to_string()
    }
}

#[cfg(test)]
mod tests {
    use object_store::{ObjectStoreExt, memory::InMemory};
    use vortex_index::VixRangeSource as _;

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn store_range_source_fetches_ranges() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("f1.vix");
        store
            .put(&path, Bytes::from_static(b"hello world").into())
            .await
            .unwrap();
        let source = StoreRangeSource::new(store, path, 11, Handle::current());
        assert_eq!(source.len(), 11);
        // Block on the executor-agnostic future from a blocking thread, the
        // way vortex_index drives it.
        let bytes =
            tokio::task::spawn_blocking(move || futures::executor::block_on(source.fetch(6..11)))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(&bytes[..], b"world");
    }

    #[test]
    fn read_mode_parses() {
        // default config value must map to Ranged
        assert_eq!(vix_read_mode(), VixReadMode::Ranged);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn fetch_ticks_per_query_stats() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("f2.vix");
        store
            .put(&path, Bytes::from_static(b"0123456789").into())
            .await
            .unwrap();
        let stats = Arc::new(FetchStats::default());
        let stats_clone = Some(Arc::clone(&stats));
        let handle = Handle::current();
        let bytes = tokio::task::spawn_blocking(move || {
            futures::executor::block_on(spawn_fetch(&handle, "scan", stats_clone, async move {
                store
                    .get_range(&path, 2..7)
                    .await
                    .map_err(anyhow::Error::from)
            }))
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(&bytes[..], b"23456");
        assert_eq!(stats.fetches.load(Ordering::Relaxed), 1);
        assert_eq!(stats.bytes.load(Ordering::Relaxed), 5);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn fetch_timeout_turns_hang_into_error() {
        // a future that never resolves must be cut off by the timeout
        let handle = Handle::current();
        let fut = spawn_fetch(&handle, "scan", None, async move {
            // vix_fetch_timeout is seconds-granular; pretend-hang forever
            futures::future::pending::<()>().await;
            unreachable!()
        });
        let result = tokio::time::timeout(Duration::from_secs(90), fut)
            .await
            .expect("ZO_VIX_FETCH_TIMEOUT default (30s) must fire before 90s");
        let err = result.expect_err("hung fetch must error");
        assert!(err.to_string().contains("ZO_VIX_FETCH_TIMEOUT"), "{err}");
    }
}
