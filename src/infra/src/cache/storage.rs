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

use std::{ops::Range, sync::LazyLock as Lazy};

use async_trait::async_trait;
use bytes::Bytes;
use config::utils::time::BASE_TIME;
use futures::{StreamExt, stream::BoxStream};
use object_store::{
    Error, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, path::Path,
};

use crate::{
    cache::file_data,
    storage::{self, ObjectStoreExt},
};

/// File system with cache
#[derive(Debug, Default)]
pub struct CacheFS {}

static DEFAULT: Lazy<Box<dyn ObjectStoreExt>> = Lazy::new(CacheFS::new_store);

impl std::fmt::Display for CacheFS {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", Self::name())
    }
}

impl CacheFS {
    pub fn name() -> &'static str {
        "CacheFS"
    }

    pub fn new_store() -> Box<dyn ObjectStoreExt> {
        Box::new(Self {})
    }
}

#[async_trait]
impl ObjectStoreExt for CacheFS {
    fn get_account(&self, org_id: &str, file: &str) -> Option<String> {
        storage::get_account(org_id, file)
    }

    async fn add_account(&self, key: String, acc: Box<dyn ObjectStore>) {
        storage::add_account(&key, acc).await;
    }

    async fn put(
        &self,
        _account: &str,
        _location: &Path,
        _payload: PutPayload,
    ) -> Result<PutResult> {
        Err(Error::NotImplemented {
            operation: "put".to_string(),
            implementer: Self::name().to_string(),
        })
    }

    async fn put_opts(
        &self,
        _account: &str,
        _location: &Path,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> Result<PutResult> {
        Err(Error::NotImplemented {
            operation: "put_opts".to_string(),
            implementer: Self::name().to_string(),
        })
    }

    async fn put_multipart(
        &self,
        _account: &str,
        _location: &Path,
    ) -> Result<Box<dyn MultipartUpload>> {
        Err(Error::NotImplemented {
            operation: "put_multipart".to_string(),
            implementer: Self::name().to_string(),
        })
    }

    async fn put_multipart_opts(
        &self,
        _account: &str,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        Err(Error::NotImplemented {
            operation: "put_multipart_opts".to_string(),
            implementer: Self::name().to_string(),
        })
    }

    async fn get(&self, account: &str, location: &Path) -> Result<GetResult> {
        let path = location.to_string();
        let options = GetOptions::default();
        if let Ok(res) = file_data::get_opts(account, &path, options, false).await {
            return Ok(res);
        }
        // default to storage
        storage::get(account, &path).await
    }

    async fn get_opts(
        &self,
        account: &str,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult> {
        let path = location.to_string();
        if let Ok(res) = file_data::get_opts(account, &path, options.clone(), false).await {
            return Ok(res);
        }
        // default to storage
        storage::get_opts(account, &path, options).await
    }

    async fn get_range(&self, account: &str, location: &Path, range: Range<u64>) -> Result<Bytes> {
        Ok(get_range_classified(account, location, range, None)
            .await?
            .bytes)
    }

    async fn get_ranges(
        &self,
        account: &str,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> Result<Vec<Bytes>> {
        if ranges.is_empty() {
            return Ok(Vec::new());
        }
        let path = location.to_string();
        // Single cache lookup for ALL ranges: memory cache → in-memory slice,
        // disk cache → one File::open + N preads. Falls back to remote on
        // cache miss (which itself does batched ranges per backend).
        if let Ok(v) = file_data::get_ranges_opts(account, &path, ranges, false).await {
            return Ok(v);
        }

        // default to storage
        storage::get_ranges(account, &path, ranges).await
    }

    async fn head(&self, account: &str, location: &Path) -> Result<ObjectMeta> {
        let path = location.to_string();
        if let Ok(size) = file_data::get_size_opts(account, &path, false).await {
            return Ok(ObjectMeta {
                location: location.clone(),
                last_modified: *BASE_TIME,
                size: size as u64,
                e_tag: Some(format!("{:x}-{:x}", BASE_TIME.timestamp_micros(), size)),
                version: None,
            });
        }
        // default to storage
        storage::head(account, &path).await
    }

    async fn delete(&self, _account: &str, _location: &Path) -> Result<()> {
        Err(Error::NotImplemented {
            operation: "delete".to_string(),
            implementer: Self::name().to_string(),
        })
    }

    async fn delete_stream(
        &self,
        _account: &str,
        _locations: BoxStream<'static, Result<Path>>,
    ) -> Result<Vec<Path>> {
        Err(Error::NotImplemented {
            operation: "delete_stream".to_string(),
            implementer: Self::name().to_string(),
        })
    }

    fn list(
        &self,
        _account: &str,
        _prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        futures::stream::once(async {
            Err(Error::NotImplemented {
                operation: "list".to_string(),
                implementer: Self::name().to_string(),
            })
        })
        .boxed()
    }

    fn list_with_offset(
        &self,
        _account: &str,
        _prefix: Option<&Path>,
        _offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        futures::stream::once(async {
            Err(Error::NotImplemented {
                operation: "list_with_offset".to_string(),
                implementer: Self::name().to_string(),
            })
        })
        .boxed()
    }

    async fn list_with_delimiter(
        &self,
        _account: &str,
        _prefix: Option<&Path>,
    ) -> Result<ListResult> {
        Err(Error::NotImplemented {
            operation: "list_with_delimiter".to_string(),
            implementer: Self::name().to_string(),
        })
    }

    async fn copy(&self, _account: &str, _from: &Path, _to: &Path) -> Result<()> {
        Err(Error::NotImplemented {
            operation: "copy".to_string(),
            implementer: Self::name().to_string(),
        })
    }

    async fn rename(&self, _account: &str, _from: &Path, _to: &Path) -> Result<()> {
        Err(Error::NotImplemented {
            operation: "rename".to_string(),
            implementer: Self::name().to_string(),
        })
    }

    async fn copy_if_not_exists(&self, _account: &str, _from: &Path, _to: &Path) -> Result<()> {
        Err(Error::NotImplemented {
            operation: "copy_if_not_exists".to_string(),
            implementer: Self::name().to_string(),
        })
    }

    async fn rename_if_not_exists(&self, _account: &str, _from: &Path, _to: &Path) -> Result<()> {
        Err(Error::NotImplemented {
            operation: "rename_if_not_exists".to_string(),
            implementer: Self::name().to_string(),
        })
    }
}

pub async fn get(account: &str, path: &Path) -> Result<GetResult> {
    DEFAULT.get(account, path).await
}

pub async fn get_opts(account: &str, path: &Path, options: GetOptions) -> Result<GetResult> {
    DEFAULT.get_opts(account, path, options).await
}

pub async fn get_range(account: &str, location: &Path, range: Range<u64>) -> Result<bytes::Bytes> {
    DEFAULT.get_range(account, location, range).await
}

pub async fn get_ranges(
    account: &str,
    location: &Path,
    ranges: &[Range<u64>],
) -> Result<Vec<bytes::Bytes>> {
    DEFAULT.get_ranges(account, location, ranges).await
}

/// The tier that actually supplied a completed range, not a preflight cache
/// membership guess (which can race eviction).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RangeSource {
    Memory,
    Disk,
    Remote,
    /// An arbitrary registered ObjectStore; its internal cache policy is unknown.
    ObjectStore,
}

impl RangeSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Disk => "disk",
            Self::Remote => "remote",
            Self::ObjectStore => "object_store",
        }
    }
}

pub struct RangeRead {
    pub bytes: Bytes,
    pub source: RangeSource,
}

/// Execute one physical range through the same cache ladder as `get_opts`.
/// Callers that batch/coalesce must admit each physical request themselves.
pub async fn get_range_classified(
    account: &str,
    location: &Path,
    range: Range<u64>,
    owner: Option<std::sync::Arc<dyn Send + Sync>>,
) -> Result<RangeRead> {
    if range.start > range.end {
        return Err(crate::storage::Error::BadRange(location.to_string()).into());
    }
    let max_bytes = range.end - range.start;
    let file = location.as_ref();
    let options = GetOptions {
        range: Some(range.into()),
        ..Default::default()
    };
    let cfg = config::get_config();
    if cfg.memory_cache.enabled
        && let Ok(result) = file_data::memory::get_opts(file, options.clone()).await
    {
        // A cache range is a slice of the whole-file owner. Arrow/Vortex may
        // retain it long after cache eviction; detach partial hits so a tiny
        // admitted range cannot keep that entire allocation alive. Metadata,
        // not Bytes uniqueness, proves whether this is a whole-file hit.
        let partial = result.range.start != 0 || result.range.end != result.meta.size;
        let bytes = range_bytes_with_owner(result, owner, max_bytes).await?;
        return Ok(RangeRead {
            bytes: if partial {
                Bytes::copy_from_slice(&bytes)
            } else {
                bytes
            },
            source: RangeSource::Memory,
        });
    }
    if cfg.disk_cache.enabled
        && let Ok(result) = file_data::disk::get_opts(file, options.clone()).await
    {
        return Ok(RangeRead {
            bytes: range_bytes_with_owner(result, owner, max_bytes).await?,
            source: RangeSource::Disk,
        });
    }
    Ok(RangeRead {
        bytes: range_bytes_with_owner(
            storage::get_opts(account, file, options).await?,
            owner,
            max_bytes,
        )
        .await?,
        source: RangeSource::Remote,
    })
}

/// Keep admission alive inside non-preemptible local file IO even if its async
/// waiter is cancelled. Remote streams remain cancellable by dropping the future.
pub async fn range_bytes_with_owner(
    result: GetResult,
    owner: Option<std::sync::Arc<dyn Send + Sync>>,
    max_bytes: u64,
) -> Result<Bytes> {
    let invalid_body = || object_store::Error::Generic {
        store: "VIX admitted read",
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "range response exceeds its admitted byte length",
        )),
    };
    let limit = result
        .range
        .end
        .checked_sub(result.range.start)
        .filter(|&len| len <= max_bytes)
        .and_then(|len| usize::try_from(len).ok())
        .ok_or_else(invalid_body)?;
    match result {
        GetResult {
            payload: object_store::GetResultPayload::File(mut file, path),
            range,
            ..
        } => {
            let task = tokio::task::spawn_blocking(move || {
                use std::io::{Read, Seek, SeekFrom};
                let _owner = owner;
                let mut read = || -> std::io::Result<Bytes> {
                    let len = usize::try_from(range.end - range.start)
                        .map_err(|_| std::io::Error::other("range exceeds address space"))?;
                    file.seek(SeekFrom::Start(range.start))?;
                    let mut bytes = vec![0; len];
                    file.read_exact(&mut bytes)?;
                    Ok(Bytes::from(bytes))
                };
                read().map_err(|error| object_store::Error::Generic {
                    store: "VIX admitted file read",
                    source: Box::new(std::io::Error::new(
                        error.kind(),
                        format!("{}: {error}", path.display()),
                    )),
                })
            });
            struct AbortQueued(tokio::task::AbortHandle);
            impl Drop for AbortQueued {
                fn drop(&mut self) {
                    self.0.abort();
                }
            }
            // Abort prevents a not-yet-running blocking read from starting.
            // Already-running reads retain `owner` until their actual completion.
            let _abort_queued = AbortQueued(task.abort_handle());
            task.await.map_err(|error| object_store::Error::Generic {
                store: "VIX admitted file read",
                source: Box::new(error),
            })?
        }
        GetResult {
            payload: object_store::GetResultPayload::Stream(mut stream),
            ..
        } => {
            let _owner = owner;
            let Some(first) = stream.next().await.transpose()? else {
                return Ok(Bytes::new());
            };
            if first.len() > limit {
                return Err(invalid_body());
            }
            let Some(second) = stream.next().await.transpose()? else {
                return Ok(first);
            };
            if second.len() > limit - first.len() {
                return Err(invalid_body());
            }
            // Preserve the single-chunk zero-copy path. On multiple chunks,
            // reserve once and check before every append; Content-Length and
            // Content-Range are untrusted hints, not permission to grow.
            let mut bytes = Vec::with_capacity(limit);
            bytes.extend_from_slice(&first);
            bytes.extend_from_slice(&second);
            drop(first);
            drop(second);
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                if chunk.len() > limit - bytes.len() {
                    return Err(invalid_body());
                }
                bytes.extend_from_slice(&chunk);
            }
            Ok(Bytes::from(bytes))
        }
    }
}

pub async fn head(account: &str, location: &Path) -> Result<ObjectMeta> {
    DEFAULT.head(account, location).await
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use bytes::Bytes;

    use super::*;

    #[tokio::test]
    async fn memory_partial_ranges_release_evicted_backing_owners() {
        use std::sync::{Arc, Weak};
        // Central cache validation enables this tier; never mutate process-wide
        // cache configuration underneath concurrently running tests.
        if !config::get_config().memory_cache.enabled {
            return;
        }
        struct Allocation {
            data: Vec<u8>,
            _lifetime: Arc<()>,
        }
        impl AsRef<[u8]> for Allocation {
            fn as_ref(&self) -> &[u8] {
                &self.data
            }
        }
        fn tracked(value: u8) -> (Bytes, Weak<()>) {
            let lifetime = Arc::new(());
            let weak = Arc::downgrade(&lifetime);
            (
                Bytes::from_owner(Allocation {
                    data: vec![value; 64 * 1024],
                    _lifetime: lifetime,
                }),
                weak,
            )
        }
        let path = Path::from("files/vix-memory-owner/logs/stream/2026/09/05/00/range.vxi");
        let (bytes, partial_owner) = tracked(b'p');
        file_data::memory::set(path.as_ref(), bytes).await.unwrap();
        let partial = get_range_classified("unused", &path, 11..15, None)
            .await
            .unwrap();
        assert_eq!(partial.source, RangeSource::Memory);
        assert_eq!(partial.bytes, Bytes::from_static(b"pppp"));
        assert!(
            partial_owner.upgrade().is_some(),
            "cache still owns the full file"
        );
        file_data::memory::remove(path.as_ref()).await.unwrap();
        assert!(
            partial_owner.upgrade().is_none(),
            "partial result retained the evicted whole-file owner"
        );
        assert_eq!(partial.bytes, Bytes::from_static(b"pppp"));

        let (bytes, whole_owner) = tracked(b'w');
        let original = bytes.as_ptr();
        let size = bytes.len() as u64;
        file_data::memory::set(path.as_ref(), bytes).await.unwrap();
        let whole = get_range_classified("unused", &path, 0..size, None)
            .await
            .unwrap();
        assert_eq!(whole.source, RangeSource::Memory);
        assert_eq!(
            whole.bytes.as_ptr(),
            original,
            "whole-file hits must remain zero-copy"
        );
        file_data::memory::remove(path.as_ref()).await.unwrap();
        assert!(
            whole_owner.upgrade().is_some(),
            "whole-file result owns its admitted full allocation"
        );
        drop(whole);
        assert!(whole_owner.upgrade().is_none());
    }

    #[tokio::test]
    async fn admitted_stream_rejects_oversized_body_before_reading_more() {
        let body = futures::stream::iter([
            Ok(Bytes::from_static(b"12")),
            Ok(Bytes::from_static(b"345")),
        ])
        .chain(futures::stream::once(async {
            panic!("oversized response must stop before polling additional body");
        }))
        .boxed();
        let result = GetResult {
            payload: object_store::GetResultPayload::Stream(body),
            range: 0..4,
            meta: ObjectMeta {
                location: Path::from("oversized-response"),
                last_modified: *BASE_TIME,
                size: 4,
                e_tag: None,
                version: None,
            },
            attributes: Default::default(),
        };
        assert!(range_bytes_with_owner(result, None, 4).await.is_err());
    }

    #[tokio::test]
    async fn admitted_stream_rejects_oversized_range_before_polling_body() {
        let body = futures::stream::once(async {
            panic!("unadmitted response metadata must be rejected before polling body");
        })
        .boxed();
        let result = GetResult {
            payload: object_store::GetResultPayload::Stream(body),
            range: 0..5,
            meta: ObjectMeta {
                location: Path::from("oversized-range"),
                last_modified: *BASE_TIME,
                size: 5,
                e_tag: None,
                version: None,
            },
            attributes: Default::default(),
        };
        assert!(range_bytes_with_owner(result, None, 4).await.is_err());
    }

    #[test]
    fn dropping_queued_file_read_aborts_it_and_releases_owner() {
        use std::{
            io::{Seek, Write},
            sync::Arc,
            time::Duration,
        };

        use futures::FutureExt;
        struct Owner(Option<tokio::sync::oneshot::Sender<()>>);
        impl Drop for Owner {
            fn drop(&mut self) {
                let _ = self.0.take().unwrap().send(());
            }
        }
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"test").unwrap();
        file.rewind().unwrap();
        let result = GetResult {
            payload: object_store::GetResultPayload::File(
                file.as_file().try_clone().unwrap(),
                file.path().to_owned(),
            ),
            range: 0..4,
            meta: ObjectMeta {
                location: Path::from("queued-read"),
                last_modified: *BASE_TIME,
                size: 4,
                e_tag: None,
                version: None,
            },
            attributes: Default::default(),
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async move {
            let (started, ready) = std::sync::mpsc::channel();
            let (release, blocked) = std::sync::mpsc::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                started.send(()).unwrap();
                blocked.recv().unwrap();
            });
            ready.recv().unwrap();
            let (owner, dropped) = tokio::sync::oneshot::channel();
            let mut read =
                range_bytes_with_owner(result, Some(Arc::new(Owner(Some(owner)))), 4).boxed();
            assert!(futures::poll!(&mut read).is_pending());
            drop(read);
            release.send(()).unwrap();
            blocker.await.unwrap();
            tokio::time::timeout(Duration::from_secs(5), dropped)
                .await
                .unwrap()
                .unwrap();
            // try_clone shares the file cursor. A detached queued read would
            // have advanced it even though nobody awaited its result.
            assert_eq!(file.stream_position().unwrap(), 0);
        });
    }

    #[tokio::test]
    async fn classified_ranges_report_the_tier_that_supplied_the_bytes() {
        use object_store::ObjectStoreExt as _;
        let org = "vix-range-classification";
        let account = format!("{org}:default");
        let path =
            Path::from("files/vix-range-classification/logs/stream/2026/09/05/00/source.vxi");
        let remote = object_store::memory::InMemory::new();
        remote
            .put(&path, Bytes::from_static(b"remote-value").into())
            .await
            .unwrap();
        storage::add_account(org, Box::new(remote)).await;
        file_data::memory::remove(path.as_ref()).await.unwrap();
        file_data::disk::remove(path.as_ref()).await.unwrap();

        let read = get_range_classified(&account, &path, 0..6, None)
            .await
            .unwrap();
        assert_eq!(read.source, RangeSource::Remote);
        assert_eq!(read.bytes, Bytes::from_static(b"remote"));

        // Exercise enabled production tiers without mutating global config under
        // concurrent tests. The central cache smoke enables both tiers.
        if config::get_config().disk_cache.enabled {
            file_data::disk::set(path.as_ref(), Bytes::from_static(b"disk--value"))
                .await
                .unwrap();
            let read = get_range_classified(&account, &path, 0..6, None)
                .await
                .unwrap();
            assert_eq!(read.source, RangeSource::Disk);
            assert_eq!(read.bytes, Bytes::from_static(b"disk--"));
        }
        if config::get_config().memory_cache.enabled {
            file_data::memory::set(path.as_ref(), Bytes::from_static(b"memory-value"))
                .await
                .unwrap();
            let read = get_range_classified(&account, &path, 0..6, None)
                .await
                .unwrap();
            assert_eq!(read.source, RangeSource::Memory);
            assert_eq!(read.bytes, Bytes::from_static(b"memory"));
        }
        file_data::memory::remove(path.as_ref()).await.unwrap();
        file_data::disk::remove(path.as_ref()).await.unwrap();
        let read = get_range_classified(&account, &path, 0..6, None)
            .await
            .unwrap();
        assert_eq!(read.source, RangeSource::Remote);
        assert_eq!(read.bytes, Bytes::from_static(b"remote"));
    }

    #[test]
    fn test_cache_fs_display() {
        let cache_fs = CacheFS {};
        assert_eq!(cache_fs.to_string(), "CacheFS");
    }

    #[test]
    fn test_cache_fs_new_store() {
        let store = CacheFS::new_store();
        assert_eq!(store.to_string(), "CacheFS");
    }

    #[tokio::test]
    async fn test_cache_fs_put() {
        let cache_fs = CacheFS {};
        let location = Path::from("test/file.txt");
        let payload = PutPayload::from(Bytes::from("test data"));

        let result = cache_fs.put("default", &location, payload).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::NotImplemented { .. }));
    }

    #[tokio::test]
    async fn test_cache_fs_put_opts() {
        let cache_fs = CacheFS {};
        let location = Path::from("test/file.txt");
        let payload = PutPayload::from(Bytes::from("test data"));
        let opts = PutOptions::default();

        let result = cache_fs.put_opts("default", &location, payload, opts).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::NotImplemented { .. }));
    }

    #[tokio::test]
    async fn test_cache_fs_put_multipart() {
        let cache_fs = CacheFS {};
        let location = Path::from("test/file.txt");

        let result = cache_fs.put_multipart("default", &location).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::NotImplemented { .. }));
    }

    #[tokio::test]
    async fn test_cache_fs_put_multipart_opts() {
        let cache_fs = CacheFS {};
        let location = Path::from("test/file.txt");
        let opts = PutMultipartOptions::default();

        let result = cache_fs
            .put_multipart_opts("default", &location, opts)
            .await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::NotImplemented { .. }));
    }

    #[tokio::test]
    async fn test_cache_fs_get_range_invalid() {
        let cache_fs = CacheFS {};
        let location = Path::from("test/file.txt");
        let range = Range { start: 10, end: 5 }; // Invalid range

        let result = cache_fs.get_range("default", &location, range).await;
        assert!(result.is_err());
        // Should return a BadRange error
        assert!(matches!(result.unwrap_err(), Error::Generic { .. }));
    }

    #[tokio::test]
    async fn test_cache_fs_delete() {
        let cache_fs = CacheFS {};
        let location = Path::from("test/file.txt");

        let result = cache_fs.delete("default", &location).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::NotImplemented { .. }));
    }

    #[tokio::test]
    async fn test_cache_fs_delete_stream() {
        let cache_fs = CacheFS {};
        let locations = futures::stream::once(async { Ok(Path::from("test/file.txt")) }).boxed();

        let result = cache_fs.delete_stream("default", locations).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::NotImplemented { .. }));
    }

    #[tokio::test]
    async fn test_cache_fs_copy() {
        let cache_fs = CacheFS {};
        let from = Path::from("test/from.txt");
        let to = Path::from("test/to.txt");

        let result = cache_fs.copy("default", &from, &to).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::NotImplemented { .. }));
    }

    #[tokio::test]
    async fn test_cache_fs_rename() {
        let cache_fs = CacheFS {};
        let from = Path::from("test/from.txt");
        let to = Path::from("test/to.txt");

        let result = cache_fs.rename("default", &from, &to).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::NotImplemented { .. }));
    }

    #[tokio::test]
    async fn test_cache_fs_copy_if_not_exists() {
        let cache_fs = CacheFS {};
        let from = Path::from("test/from.txt");
        let to = Path::from("test/to.txt");

        let result = cache_fs.copy_if_not_exists("default", &from, &to).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::NotImplemented { .. }));
    }

    #[tokio::test]
    async fn test_cache_fs_rename_if_not_exists() {
        let cache_fs = CacheFS {};
        let from = Path::from("test/from.txt");
        let to = Path::from("test/to.txt");

        let result = cache_fs.rename_if_not_exists("default", &from, &to).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::NotImplemented { .. }));
    }
}
