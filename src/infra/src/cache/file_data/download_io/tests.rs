// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use std::{
    collections::HashMap,
    sync::atomic::{AtomicUsize, Ordering},
};

use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory};
use sha1::{Digest, Sha1};

use super::*;
use crate::cache::file_data::{DownloadSink, ExpectedSizePolicy, download_with_retries_policy};

fn settings() -> Settings {
    Settings {
        pipeline: true,
        buffer: 64 * 1024,
        ranges: true,
        minimum: 1,
        segment: 256 * 1024,
        concurrency: 4,
        timeout: Duration::from_secs(5),
        metrics: IoMetrics(false),
        allocation_observer: None,
        range_read_only: false,
        before_range_write: None,
    }
}

fn data(size: usize) -> Bytes {
    Bytes::from(
        (0..size)
            // Include high offset bits: identical 64 KiB blocks could hide a
            // misplaced 8 MiB range even when the whole-file SHA is compared.
            .map(|i| (i.wrapping_mul(31) ^ (i >> 8) ^ (i >> 16) ^ (i >> 24)) as u8)
            .collect::<Vec<_>>(),
    )
}

fn pipeline(path: &Path, lifetime: Arc<dyn Send + Sync>, options: Settings) -> DownloadSink {
    DownloadSink::PipelinedFile {
        path: path.into(),
        writer: PipelineWriter::new(path.into(), lifetime, options.buffer, options.metrics),
        written: 0,
    }
}

fn legacy(path: &Path, lifetime: Arc<dyn Send + Sync>) -> DownloadSink {
    DownloadSink::ExactFile {
        path: path.into(),
        writer: None,
        written: 0,
        lifetime,
    }
}

#[derive(Clone, Copy, Default)]
enum Fault {
    #[default]
    None,
    Once,
    BodyOnce,
    Always,
    Short,
    Overrun,
    ChangedEtag,
    WrongRange,
    NoEtag,
    Stall,
    BodyStall,
}

struct Store {
    inner: InMemory,
    attempts: parking_lot::Mutex<HashMap<u64, usize>>,
    fault: Fault,
    delay: Duration,
    active: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    chunk: usize,
}

struct Active(Arc<AtomicUsize>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Store {
    async fn new(bytes: Bytes, fault: Fault) -> Self {
        let inner = InMemory::new();
        inner.put(&"object".into(), bytes.into()).await.unwrap();
        Self {
            inner,
            attempts: Default::default(),
            fault,
            delay: Duration::ZERO,
            active: Default::default(),
            peak: Default::default(),
            chunk: 16 * 1024,
        }
    }

    async fn get(&self, options: GetOptions) -> object_store::Result<GetResult> {
        if options.head {
            let mut response = self.inner.get_opts(&"object".into(), options).await?;
            if matches!(self.fault, Fault::NoEtag) {
                response.meta.e_tag = None;
            }
            return Ok(response);
        }
        let start = match &options.range {
            Some(GetRange::Bounded(range)) => range.start,
            _ => 0,
        };
        let attempt = {
            let mut counts = self.attempts.lock();
            let entry = counts.entry(start).or_default();
            *entry += 1;
            *entry
        };
        let current = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(current, Ordering::SeqCst);
        let active = Active(self.active.clone());
        if matches!(self.fault, Fault::Stall) {
            std::future::pending::<()>().await;
        }
        if start == 0
            && (matches!(self.fault, Fault::Always)
                || matches!(self.fault, Fault::Once) && attempt == 1)
        {
            return Err(object_store::Error::Generic {
                store: "injected local store",
                source: "injected GET failure".into(),
            });
        }
        if start > 0 && matches!(self.fault, Fault::ChangedEtag) {
            // The real InMemory implementation enforces If-Match against the replacement.
            let size = self.inner.head(&"object".into()).await?.size as usize;
            self.inner
                .put(&"object".into(), Bytes::from(vec![42; size]).into())
                .await?;
        }
        if options.range.is_some() {
            assert!(options.if_match.is_some(), "every range must be pinned");
        }
        let mut response = self.inner.get_opts(&"object".into(), options).await?;
        let mut bytes = response.bytes().await?;
        // Reopen only to preserve metadata while constructing an injected body.
        response = self
            .inner
            .get_opts(
                &"object".into(),
                GetOptions {
                    range: Some(GetRange::Bounded(start..start + bytes.len() as u64)),
                    ..Default::default()
                },
            )
            .await?;
        if matches!(self.fault, Fault::Short) {
            bytes.truncate(bytes.len().saturating_sub(1));
        }
        if matches!(self.fault, Fault::Overrun) {
            let mut longer = bytes.to_vec();
            longer.push(1);
            bytes = longer.into();
        }
        if matches!(self.fault, Fault::WrongRange) {
            response.range.start += 1;
        }
        let chunk_size = self.chunk;
        let delay = self.delay;
        let body_error = start == 0 && attempt == 1 && matches!(self.fault, Fault::BodyOnce);
        let body_stall = matches!(self.fault, Fault::BodyStall);
        response.payload =
            object_store::GetResultPayload::Stream(Box::pin(futures::stream::unfold(
                (bytes, 0usize, active),
                move |(bytes, position, active)| async move {
                    if position == bytes.len() {
                        return None;
                    }
                    if position > 0 && body_stall {
                        std::future::pending::<()>().await;
                    }
                    if position > 0 && body_error {
                        return Some((
                            Err(object_store::Error::Generic {
                                store: "injected local store",
                                source: "injected body failure".into(),
                            }),
                            (bytes.clone(), bytes.len(), active),
                        ));
                    }
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                    let end = bytes.len().min(position + chunk_size);
                    Some((Ok(bytes.slice(position..end)), (bytes, end, active)))
                },
            )));
        Ok(response)
    }
}

async fn assert_removed(path: &Path) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while path.exists() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("physical writer and temp-file lifetime must terminate");
}

#[tokio::test]
async fn pipelined_content_matches_legacy_at_buffer_boundaries() {
    for size in [1, 64 * 1024, 64 * 1024 + 1, 4 * 64 * 1024 + 17] {
        let expected = data(size);
        let store = Store::new(expected.clone(), Fault::None).await;
        for pipelined in [false, true] {
            let file = Arc::new(tempfile::NamedTempFile::new().unwrap());
            let path = file.path().to_path_buf();
            let mut sink = if pipelined {
                pipeline(&path, file.clone(), settings())
            } else {
                legacy(&path, file.clone())
            };
            let length = download_with_retries_policy(
                || store.get(GetOptions::default()),
                "object",
                Some(size),
                &mut sink,
                ExpectedSizePolicy::Exact,
            )
            .await
            .unwrap();
            assert_eq!(length, size);
            assert_eq!(
                Sha1::digest(std::fs::read(&path).unwrap()),
                Sha1::digest(&expected)
            );
            drop(sink);
            drop(file);
            assert_removed(&path).await;
        }
    }
}

#[tokio::test]
async fn range_content_matches_legacy_and_handles_partial_last_segment() {
    // Exercise a production-sized object, plus the last-segment edge case.
    for size in [64 * 1024 * 1024, 64 * 1024 * 1024 + 17] {
        let expected = data(size);
        let store = Store::new(expected.clone(), Fault::None).await;
        let options = Settings {
            segment: 8 * 1024 * 1024,
            buffer: 8 * 1024 * 1024,
            ..settings()
        };
        assert_ne!(
            Sha1::digest(&expected[..options.segment]),
            Sha1::digest(&expected[options.segment..2 * options.segment]),
            "the fixture must detect swapped adjacent segments"
        );
        let file = Arc::new(tempfile::NamedTempFile::new().unwrap());
        let path = file.path().to_path_buf();
        assert_eq!(
            download_ranges(|opts| store.get(opts), &path, size, file.clone(), options)
                .await
                .unwrap(),
            Some(size)
        );
        assert_eq!(
            Sha1::digest(std::fs::read(&path).unwrap()),
            Sha1::digest(&expected)
        );
        // The disk tier's startup check deletes any cache file with a hole,
        // so a complete download must have written its whole preallocation.
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            let written = std::fs::File::open(&path).unwrap();
            assert_eq!(
                crate::cache::file_data::block::lseek_whence(
                    written.as_raw_fd(),
                    0,
                    libc::SEEK_HOLE
                )
                .unwrap(),
                Some(size as u64),
                "a complete ranged download has no hole before EOF"
            );
        }
        assert!(store.peak.load(Ordering::SeqCst) <= options.concurrency);
        assert_eq!(store.active.load(Ordering::SeqCst), 0);
        assert!(store.attempts.lock().values().all(|&count| count == 1));
        drop(file);
        assert_removed(&path).await;
    }
}

#[tokio::test]
async fn range_retries_only_failed_segment_once() {
    let options = settings();
    let bytes = data(options.segment * 5 + 13);
    let size = bytes.len();
    let store = Store::new(bytes.clone(), Fault::Once).await;
    let file = Arc::new(tempfile::NamedTempFile::new().unwrap());
    download_ranges(
        |opts| store.get(opts),
        file.path(),
        size,
        file.clone(),
        options,
    )
    .await
    .unwrap();
    assert_eq!(std::fs::read(file.path()).unwrap(), bytes);
    let attempts = store.attempts.lock();
    assert_eq!(attempts[&0], 2);
    assert!(
        attempts
            .iter()
            .filter(|(start, _)| **start != 0)
            .all(|(_, &count)| count == 1)
    );
}

#[tokio::test]
async fn range_failures_drop_other_streams_and_remove_tmp() {
    for fault in [
        Fault::Always,
        Fault::Short,
        Fault::Overrun,
        Fault::ChangedEtag,
        Fault::WrongRange,
    ] {
        let options = settings();
        let bytes = data(options.segment * 5 + 13);
        let size = bytes.len();
        let expected_hash = Sha1::digest(&bytes);
        let store = Store::new(bytes, fault).await;
        let file = Arc::new(tempfile::NamedTempFile::new().unwrap());
        let path = file.path().to_path_buf();
        let result =
            download_ranges(|opts| store.get(opts), &path, size, file.clone(), options).await;
        assert!(result.is_err());
        assert!(store.attempts.lock().values().all(|&count| count <= 2));
        assert_eq!(store.active.load(Ordering::SeqCst), 0);
        assert_ne!(Sha1::digest(std::fs::read(&path).unwrap()), expected_hash);
        drop(file);
        assert_removed(&path).await;
    }
}

#[tokio::test]
async fn range_timeout_retries_once_then_cleans_up() {
    let options = Settings {
        timeout: Duration::from_millis(10),
        ..settings()
    };
    let store = Store::new(data(options.segment), Fault::Stall).await;
    let file = Arc::new(tempfile::NamedTempFile::new().unwrap());
    let path = file.path().to_path_buf();
    let result = download_ranges(
        |opts| store.get(opts),
        &path,
        options.segment,
        file.clone(),
        options,
    )
    .await;
    assert!(format!("{:#}", result.unwrap_err()).contains("timeout"));
    assert_eq!(store.attempts.lock()[&0], 2);
    assert_eq!(store.active.load(Ordering::SeqCst), 0);
    drop(file);
    assert_removed(&path).await;
}

#[tokio::test]
async fn range_body_retry_preserves_sha_and_body_timeout_cleans_up() {
    for fault in [Fault::BodyOnce, Fault::BodyStall] {
        let options = Settings {
            timeout: Duration::from_millis(50),
            ..settings()
        };
        let expected = data(options.segment * 2 + 17);
        let store = Store::new(expected.clone(), fault).await;
        let file = Arc::new(tempfile::NamedTempFile::new().unwrap());
        let path = file.path().to_path_buf();
        let result = download_ranges(
            |opts| store.get(opts),
            &path,
            expected.len(),
            file.clone(),
            options,
        )
        .await;
        if matches!(fault, Fault::BodyOnce) {
            assert_eq!(result.unwrap(), Some(expected.len()));
            assert_eq!(
                Sha1::digest(std::fs::read(&path).unwrap()),
                Sha1::digest(&expected)
            );
        } else {
            assert!(format!("{:#}", result.unwrap_err()).contains("timeout"));
        }
        assert_eq!(store.attempts.lock()[&0], 2);
        assert_eq!(store.active.load(Ordering::SeqCst), 0);
        drop(file);
        assert_removed(&path).await;
    }
}

#[tokio::test]
async fn range_cancellation_releases_streams_and_temp_file() {
    let options = settings();
    let store = Arc::new(Store::new(data(options.segment * 8), Fault::Stall).await);
    let file = Arc::new(tempfile::NamedTempFile::new().unwrap());
    let path = file.path().to_path_buf();
    let worker_path = path.clone();
    let worker_store = store.clone();
    let worker = tokio::spawn(async move {
        download_ranges(
            |opts| worker_store.get(opts),
            &worker_path,
            options.segment * 8,
            file,
            options,
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while store.active.load(Ordering::SeqCst) < options.concurrency {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    worker.abort();
    assert!(worker.await.unwrap_err().is_cancelled());
    assert_eq!(store.active.load(Ordering::SeqCst), 0);
    assert_removed(&path).await;
}

#[tokio::test]
async fn missing_etag_falls_back_without_range_requests_or_tmp_writes() {
    let store = Store::new(data(1024), Fault::NoEtag).await;
    let file = Arc::new(tempfile::NamedTempFile::new().unwrap());
    assert_eq!(
        download_ranges(
            |opts| store.get(opts),
            file.path(),
            1024,
            file.clone(),
            settings()
        )
        .await
        .unwrap(),
        None
    );
    assert!(store.attempts.lock().is_empty());
    assert_eq!(std::fs::metadata(file.path()).unwrap().len(), 0);
}

#[tokio::test]
async fn header_mismatch_fails_without_body_or_temp_write() {
    let store = Store::new(data(1024), Fault::None).await;
    for ranges in [false, true] {
        let file = Arc::new(tempfile::NamedTempFile::new().unwrap());
        let path = file.path().to_path_buf();
        if ranges {
            assert!(
                download_ranges(|opts| store.get(opts), &path, 512, file.clone(), settings())
                    .await
                    .is_err()
            );
        } else {
            let mut sink = pipeline(&path, file.clone(), settings());
            assert!(
                download_with_retries_policy(
                    || store.get(GetOptions::default()),
                    "object",
                    Some(512),
                    &mut sink,
                    ExpectedSizePolicy::Exact
                )
                .await
                .is_err()
            );
        }
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
        drop(file);
        assert_removed(&path).await;
    }
}

#[tokio::test(start_paused = true)]
async fn pipelined_short_body_retries_reset_without_tmp_leak() {
    let store = Store::new(data(1024), Fault::Short).await;
    let file = Arc::new(tempfile::NamedTempFile::new().unwrap());
    let path = file.path().to_path_buf();
    let mut sink = pipeline(&path, file.clone(), settings());
    assert!(
        download_with_retries_policy(
            || store.get(GetOptions::default()),
            "object",
            Some(1024),
            &mut sink,
            ExpectedSizePolicy::Exact
        )
        .await
        .is_err()
    );
    assert_eq!(store.attempts.lock()[&0], 3);
    assert_eq!(
        std::fs::metadata(&path).unwrap().len(),
        1023,
        "retries truncate rather than append"
    );
    drop(sink);
    drop(file);
    assert_removed(&path).await;
}

#[tokio::test]
async fn pipelined_cancel_after_dispatched_write_removes_tmp() {
    let file = Arc::new(tempfile::NamedTempFile::new().unwrap());
    let path = file.path().to_path_buf();
    let weak = Arc::downgrade(&file);
    let worker_path = path.clone();
    let (sent, ready) = tokio::sync::oneshot::channel();
    let worker = tokio::spawn(async move {
        let mut writer = PipelineWriter::new(worker_path, file, 64 * 1024, IoMetrics(false));
        writer.set_size(8 * 64 * 1024);
        writer.reset().await.unwrap();
        writer.write_chunk(data(64 * 1024 + 123)).await.unwrap();
        sent.send(()).unwrap();
        std::future::pending::<()>().await;
        drop(writer);
    });
    ready.await.unwrap();
    worker.abort();
    assert!(worker.await.unwrap_err().is_cancelled());
    assert_removed(&path).await;
    assert!(
        weak.upgrade().is_none(),
        "writer must exit and release admission"
    );
}

#[tokio::test(start_paused = true)]
async fn pipeline_short_attempt_then_success_preserves_sha() {
    let options = settings();
    let expected = data(options.buffer * 3 + 17);
    let short = Store::new(expected.clone(), Fault::Short).await;
    let complete = Store::new(expected.clone(), Fault::None).await;
    let attempts = AtomicUsize::new(0);
    let file = Arc::new(tempfile::NamedTempFile::new().unwrap());
    let path = file.path().to_path_buf();
    let mut sink = pipeline(&path, file.clone(), options);
    let length = download_with_retries_policy(
        || async {
            if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                short.get(GetOptions::default()).await
            } else {
                complete.get(GetOptions::default()).await
            }
        },
        "object",
        Some(expected.len()),
        &mut sink,
        ExpectedSizePolicy::Exact,
    )
    .await
    .unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(length, expected.len());
    assert_eq!(
        Sha1::digest(std::fs::read(&path).unwrap()),
        Sha1::digest(&expected)
    );
    drop(sink);
    drop(file);
    assert_removed(&path).await;
}

#[tokio::test]
async fn pipeline_propagates_physical_write_error() {
    let file = Arc::new(tempfile::NamedTempFile::new().unwrap());
    let path = file.path().to_path_buf();
    let mut writer = PipelineWriter::new(file.path().into(), file.clone(), 1024, IoMetrics(false));
    writer.reset().await.unwrap();
    // An actual read-only descriptor forces write_all to fail on every platform.
    writer.writer.as_mut().unwrap().file = std::fs::File::open(file.path()).unwrap();
    writer.write_chunk(data(1024)).await.unwrap();
    assert!(writer.finish().await.is_err());
    assert_eq!(
        Sha1::digest(std::fs::read(&path).unwrap()),
        Sha1::digest([])
    );
    drop(writer);
    drop(file);
    assert_removed(&path).await;
}

#[tokio::test]
async fn range_physical_write_failure_is_not_retried_and_removes_tmp() {
    let options = Settings {
        concurrency: 1,
        range_read_only: true,
        ..settings()
    };
    let expected = data(options.segment * 2 + 17);
    let store = Store::new(expected.clone(), Fault::None).await;
    let file = Arc::new(tempfile::NamedTempFile::new().unwrap());
    let path = file.path().to_path_buf();
    let error = download_ranges(
        |opts| store.get(opts),
        &path,
        expected.len(),
        file.clone(),
        options,
    )
    .await
    .unwrap_err();
    assert!(format!("{error:#}").contains("write range 0.."));
    assert_eq!(*store.attempts.lock(), HashMap::from([(0, 1)]));
    assert_ne!(
        Sha1::digest(std::fs::read(&path).unwrap()),
        Sha1::digest(&expected)
    );
    drop(file);
    assert_removed(&path).await;
}

#[tokio::test]
async fn pipeline_allocations_fit_admission_for_tiny_and_boundary_objects() {
    let options = Settings {
        ranges: false,
        ..settings()
    };
    for size in [
        1,
        options.buffer - 1,
        options.buffer,
        options.buffer + 1,
        options.buffer * 7 + 1,
    ] {
        let file = Arc::new(tempfile::NamedTempFile::new().unwrap());
        let mut writer = PipelineWriter::new(
            file.path().into(),
            file.clone(),
            options.buffer,
            options.metrics,
        );
        writer.set_size(size as u64);
        writer.reset().await.unwrap();
        let budget = options.in_flight_bytes(size as u64);
        for position in (0..size).step_by(137) {
            writer
                .write_chunk(data(137.min(size - position)))
                .await
                .unwrap();
            // All allocated buffers, including a detached physical write, are
            // measured by Vec capacity, not by the reservation bookkeeping.
            let allocated = writer.pending.capacity() + writer.writing_capacity;
            assert!(allocated as u64 <= budget);
        }
        writer.finish().await.unwrap();
        assert_eq!(std::fs::metadata(file.path()).unwrap().len(), size as u64);
    }
}

#[test]
fn in_flight_budget_covers_ranges_and_missing_etag_fallback() {
    let options = settings();
    assert_eq!(options.in_flight_bytes(0), 0);
    assert_eq!(options.in_flight_bytes(10), 10);
    assert_eq!(
        options.in_flight_bytes(u64::MAX),
        (options.segment * options.concurrency) as u64
    );
    let options = Settings {
        segment: 1,
        concurrency: 1,
        ..options
    };
    assert_eq!(
        options.in_flight_bytes(u64::MAX),
        (options.buffer * 2) as u64
    );
    assert!(disk_download_in_flight_bytes(1) >= 1);
}

#[cfg(unix)]
#[test]
fn cache_device_check_accepts_local_paths_and_rejects_cross_device_ids() {
    let tmp = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let counter = config::metrics::FILE_DOWNLOAD_DEVICE_CHECK_ERRORS.with_label_values::<&str>(&[]);
    let before = counter.get();
    check_same_device(tmp.path(), cache.path(), true).unwrap();
    assert!(check_device_ids(1, 2, tmp.path(), cache.path(), true).is_err());
    check_device_ids(1, 2, tmp.path(), cache.path(), false).unwrap();
    // Even a failed stat is advisory unless the operator requested fail-fast.
    let missing = tmp.path().join("missing");
    check_same_device(&missing, cache.path(), false).unwrap();
    assert!(check_same_device(&missing, cache.path(), true).is_err());
    assert_eq!(counter.get() - before, 4);
}

#[tokio::test]
#[ignore = "local delayed-store throughput reference; not an S3 or EBS measurement"]
async fn throughput_microbenchmark() {
    let size = 64 * 1024 * 1024 + 17;
    let expected = data(size);
    let mut store = Store::new(expected.clone(), Fault::None).await;
    store.chunk = 64 * 1024;
    let base = Settings {
        buffer: 8 * 1024 * 1024,
        segment: 8 * 1024 * 1024,
        timeout: Duration::from_secs(60),
        ..settings()
    };
    for delay in [Duration::ZERO, Duration::from_micros(100)] {
        store.delay = delay;
        println!("source delay per 64 KiB chunk: {} us", delay.as_micros());
        for (name, ranges, concurrency) in [
            ("legacy", false, 0),
            ("pipeline", false, 0),
            ("range-1", true, 1),
            ("range-4", true, 4),
            ("range-8", true, 8),
        ] {
            let file = Arc::new(tempfile::NamedTempFile::new().unwrap());
            let began = Instant::now();
            if ranges {
                download_ranges(
                    |opts| store.get(opts),
                    file.path(),
                    size,
                    file.clone(),
                    Settings {
                        concurrency,
                        ..base
                    },
                )
                .await
                .unwrap();
            } else {
                let mut sink = if name == "legacy" {
                    legacy(file.path(), file.clone())
                } else {
                    pipeline(file.path(), file.clone(), base)
                };
                download_with_retries_policy(
                    || store.get(GetOptions::default()),
                    "object",
                    Some(size),
                    &mut sink,
                    ExpectedSizePolicy::Exact,
                )
                .await
                .unwrap();
            }
            let elapsed = began.elapsed().as_secs_f64();
            println!(
                "{name}: {:.2} MB/s ({size} bytes, {elapsed:.4} s)",
                size as f64 / elapsed / 1_000_000.0
            );
            assert_eq!(
                Sha1::digest(std::fs::read(file.path()).unwrap()),
                Sha1::digest(&expected)
            );
        }
    }
}

#[cfg(unix)]
#[tokio::test]
async fn pipelined_cancel_keeps_admission_until_blocked_physical_write_exits() {
    use std::{
        io::Read,
        os::{fd::OwnedFd, unix::net::UnixStream},
    };

    const BUFFER: usize = 8 * 1024 * 1024;
    let file = Arc::new(tempfile::NamedTempFile::new().unwrap());
    let path = file.path().to_path_buf();
    let weak = Arc::downgrade(&file);
    let (socket, mut reader) = UnixStream::pair().unwrap();
    let mut writer = PipelineWriter::new(path.clone(), file, BUFFER, IoMetrics(false));
    writer.set_size(BUFFER as u64 * 2);
    writer.reset().await.unwrap();
    writer.writer.as_mut().unwrap().file = std::fs::File::from(OwnedFd::from(socket));
    let (sent, ready) = tokio::sync::oneshot::channel();
    let worker = tokio::spawn(async move {
        writer.write_chunk(data(BUFFER)).await.unwrap();
        // The network can fill the other buffer while the first write is blocked.
        writer.write_chunk(data(123)).await.unwrap();
        sent.send(()).unwrap();
        std::future::pending::<()>().await;
        drop(writer);
    });
    ready.await.unwrap();
    worker.abort();
    assert!(worker.await.unwrap_err().is_cancelled());
    assert!(
        weak.upgrade().is_some(),
        "physical write still owns admission"
    );
    assert!(path.exists());
    tokio::task::spawn_blocking(move || {
        let mut bytes = vec![0; BUFFER];
        reader.read_exact(&mut bytes).unwrap();
        assert_eq!(Sha1::digest(bytes), Sha1::digest(data(BUFFER)));
    })
    .await
    .unwrap();
    assert_removed(&path).await;
    assert!(weak.upgrade().is_none());
}

#[tokio::test]
async fn range_live_buffer_capacity_stays_within_admission_through_retries() {
    use std::sync::atomic::AtomicI64;
    static LIVE: AtomicI64 = AtomicI64::new(0);
    static PEAK: AtomicI64 = AtomicI64::new(0);
    fn observe(delta: i64) {
        let now = LIVE.fetch_add(delta, Ordering::SeqCst) + delta;
        assert!(now >= 0);
        PEAK.fetch_max(now, Ordering::SeqCst);
    }
    for concurrency in [1, 4, 8] {
        for fault in [Fault::None, Fault::Once, Fault::BodyOnce, Fault::Always] {
            let options = Settings {
                concurrency,
                allocation_observer: Some(observe),
                ..settings()
            };
            let size = options.segment * 9 + 17;
            let mut store = Store::new(data(size), fault).await;
            store.delay = Duration::from_micros(1);
            let file = Arc::new(tempfile::NamedTempFile::new().unwrap());
            let path = file.path().to_path_buf();
            LIVE.store(0, Ordering::SeqCst);
            PEAK.store(0, Ordering::SeqCst);
            let result =
                download_ranges(|opts| store.get(opts), &path, size, file.clone(), options).await;
            assert_eq!(result.is_err(), matches!(fault, Fault::Always));
            if result.is_ok() {
                assert_eq!(
                    Sha1::digest(std::fs::read(&path).unwrap()),
                    Sha1::digest(data(size))
                );
            }
            drop(file);
            assert_removed(&path).await;
            assert_eq!(LIVE.load(Ordering::SeqCst), 0);
            assert!(PEAK.load(Ordering::SeqCst) as u64 <= options.in_flight_bytes(size as u64));
            // A failed first GET can terminate before any body is allocated.
            if !matches!(fault, Fault::Always) {
                assert!(PEAK.load(Ordering::SeqCst) > 0);
            }
            if concurrency == 1 && matches!(fault, Fault::Always) {
                assert_eq!(PEAK.load(Ordering::SeqCst), 0);
            }
        }
    }
}

#[cfg(unix)]
#[tokio::test]
async fn range_cancel_keeps_admission_until_physical_write_exits() {
    use std::sync::{Condvar, Mutex, atomic::AtomicBool};

    static ENTERED: AtomicBool = AtomicBool::new(false);
    static GATE: (Mutex<bool>, Condvar) = (Mutex::new(false), Condvar::new());
    fn before_write() {
        ENTERED.store(true, Ordering::SeqCst);
        let mut released = GATE.0.lock().unwrap();
        while !*released {
            released = GATE.1.wait(released).unwrap();
        }
    }
    struct Release;
    impl Drop for Release {
        fn drop(&mut self) {
            *GATE.0.lock().unwrap() = true;
            GATE.1.notify_all();
        }
    }
    let release = Release;
    let options = Settings {
        concurrency: 1,
        before_range_write: Some(before_write),
        ..settings()
    };
    let expected = data(options.segment * 2 + 17);
    let size = expected.len();
    let store = Arc::new(Store::new(expected.clone(), Fault::None).await);
    let file = Arc::new(tempfile::NamedTempFile::new().unwrap());
    let path = file.path().to_path_buf();
    let weak = Arc::downgrade(&file);
    let worker_path = path.clone();
    let worker_store = store.clone();
    let worker = tokio::spawn(async move {
        download_ranges(
            |opts| worker_store.get(opts),
            &worker_path,
            size,
            file,
            options,
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while !ENTERED.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // Keep a test-only link to inspect bytes after the real temporary path is removed.
    let evidence = tempfile::tempdir().unwrap();
    let evidence_path = evidence.path().join("unpublished");
    std::fs::hard_link(&path, &evidence_path).unwrap();
    worker.abort();
    assert!(worker.await.unwrap_err().is_cancelled());
    assert!(
        weak.upgrade().is_some(),
        "blocking writer must retain admission"
    );
    assert!(path.exists());
    assert_eq!(*store.attempts.lock(), HashMap::from([(0, 1)]));
    drop(release);
    assert_removed(&path).await;
    assert!(weak.upgrade().is_none());
    let partial = std::fs::read(evidence_path).unwrap();
    assert_eq!(
        Sha1::digest(&partial[..options.segment]),
        Sha1::digest(&expected[..options.segment])
    );
    assert!(partial[options.segment..].iter().all(|&byte| byte == 0));
    assert_ne!(Sha1::digest(&partial), Sha1::digest(&expected));
}
