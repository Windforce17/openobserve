// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Bounded background-download buffers. Blocking work owns the temporary-file
//! lifetime, so dropping an async waiter cannot release admission before IO ends.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, ensure};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use object_store::{GetOptions, GetRange, GetResult};

use super::ExactFileWriter;

#[derive(Clone, Copy)]
pub(super) struct Settings {
    pub pipeline: bool,
    pub buffer: usize,
    pub ranges: bool,
    pub minimum: u64,
    pub segment: usize,
    pub concurrency: usize,
    pub timeout: Duration,
    pub metrics: IoMetrics,
    #[cfg(test)]
    pub allocation_observer: Option<fn(i64)>,
    #[cfg(test)]
    pub range_read_only: bool,
    #[cfg(test)]
    pub before_range_write: Option<fn()>,
}

impl Settings {
    pub fn configured() -> Self {
        let cfg = config::get_config();
        let limit = &cfg.limit;
        Self {
            pipeline: limit.file_download_pipelined_write,
            // Clamp at the point of use too: tests and reloads can bypass init.
            buffer: limit
                .file_download_write_buffer_bytes
                .clamp(64 * 1024, 64 * 1024 * 1024),
            ranges: cfg!(unix) && limit.file_download_range_enabled,
            minimum: limit.file_download_range_min_object_bytes.max(1),
            segment: limit
                .file_download_range_segment_bytes
                .clamp(64 * 1024, 64 * 1024 * 1024),
            concurrency: limit.file_download_range_concurrency.clamp(1, 8),
            timeout: Duration::from_secs(limit.file_download_range_segment_timeout_secs.max(1)),
            metrics: IoMetrics(limit.file_download_io_metrics_enabled),
            #[cfg(test)]
            allocation_observer: None,
            #[cfg(test)]
            range_read_only: false,
            #[cfg(test)]
            before_range_write: None,
        }
    }

    pub fn use_ranges(self, size: u64) -> bool {
        self.ranges && size >= self.minimum
    }

    pub fn in_flight_bytes(self, size: u64) -> u64 {
        if self.use_ranges(size) {
            // Reserve the single-stream fallback as well (e.g. a store without ETag).
            if !self.pipeline {
                return size;
            }
            size.min((self.segment as u64 * self.concurrency as u64).max(self.buffer as u64 * 2))
        } else if self.pipeline {
            size.min(self.buffer as u64 * 2)
        } else {
            // Legacy stores can return the entire body in one Bytes.
            size
        }
    }
}

/// Upper bound for downloader-owned payload buffers, not TLS, transport frames,
/// allocator metadata or the OS page cache. A pipelined stream uses at most two
/// write buffers. Ranges use at most concurrency * segment bytes, including writes.
/// A missing-ETag fallback reserves max(range buffers, two stream buffers).
/// The legacy unbuffered writer conservatively reserves the complete object.
/// Zero means an empty object; unknown-size callers must reserve independently.
pub fn disk_download_in_flight_bytes(object_size: u64) -> u64 {
    Settings::configured().in_flight_bytes(object_size)
}

#[derive(Clone, Copy)]
pub(super) struct IoMetrics(pub bool);

impl IoMetrics {
    pub fn chunk(self, bytes: usize) {
        if self.0 {
            // Histogram sample_count is also the body-chunk counter.
            config::metrics::FILE_DOWNLOAD_BODY_CHUNK_BYTES
                .with_label_values::<&str>(&[])
                .observe(bytes as f64);
        }
    }

    pub fn time(self, phase: &'static str, elapsed: Duration) {
        if self.0 {
            config::metrics::FILE_DOWNLOAD_IO_SECONDS
                .with_label_values(&[phase])
                .observe(elapsed.as_secs_f64());
        }
    }

    pub fn retry(self) {
        if self.0 {
            config::metrics::FILE_DOWNLOAD_RANGE_RETRIES
                .with_label_values::<&str>(&[])
                .inc();
        }
    }
}

type WriteJob = tokio::task::JoinHandle<(ExactFileWriter, Vec<u8>, std::io::Result<()>)>;

/// One accumulating buffer and one physical write. A completed write recycles
/// its allocation; no queue or idle blocking thread exists. Cancellation drops
/// the accumulating buffer immediately. At most one physical write can remain.
pub(super) struct PipelineWriter {
    path: PathBuf,
    lifetime: Arc<dyn Send + Sync>,
    writer: Option<ExactFileWriter>,
    job: Option<WriteJob>,
    pending: Vec<u8>,
    #[cfg(test)]
    writing_capacity: usize,
    capacity: usize,
    size: u64,
    allocation_left: usize,
    metrics: IoMetrics,
}

impl PipelineWriter {
    pub fn new(
        path: PathBuf,
        lifetime: Arc<dyn Send + Sync>,
        capacity: usize,
        metrics: IoMetrics,
    ) -> Self {
        Self {
            path,
            lifetime,
            writer: None,
            job: None,
            pending: Vec::new(),
            #[cfg(test)]
            writing_capacity: 0,
            capacity,
            size: u64::MAX,
            allocation_left: 0,
            metrics,
        }
    }

    pub fn set_size(&mut self, size: u64) {
        self.size = size;
    }

    pub async fn reset(&mut self) -> anyhow::Result<()> {
        // Join before truncating: an old write must never race the next attempt.
        self.join().await?;
        self.writer.take();
        self.pending = Vec::new();
        self.allocation_left = self.size.min(self.capacity as u64 * 2) as usize;
        let path = self.path.clone();
        let lifetime = self.lifetime.clone();
        self.writer = Some(
            tokio::task::spawn_blocking(move || {
                let file = std::fs::File::create(&path)
                    .with_context(|| format!("create pipelined download {}", path.display()))?;
                Ok::<_, anyhow::Error>(ExactFileWriter {
                    file,
                    _lifetime: lifetime,
                })
            })
            .await
            .context("join pipelined file creation")??,
        );
        Ok(())
    }

    async fn join(&mut self) -> anyhow::Result<Vec<u8>> {
        let Some(job) = self.job.take() else {
            return Ok(Vec::new());
        };
        let (writer, mut buffer, result) = job.await.context("join pipelined download write")?;
        self.writer = Some(writer);
        #[cfg(test)]
        {
            self.writing_capacity = 0;
        }
        result.with_context(|| format!("write pipelined download {}", self.path.display()))?;
        buffer.clear();
        Ok(buffer)
    }

    async fn dispatch(&mut self) -> anyhow::Result<()> {
        let spare = self.join().await?;
        let mut writer = self
            .writer
            .take()
            .context("pipelined sink used before reset")?;
        let buffer = std::mem::replace(&mut self.pending, spare);
        let metrics = self.metrics;
        #[cfg(test)]
        {
            self.writing_capacity = buffer.capacity();
        }
        self.job = Some(tokio::task::spawn_blocking(move || {
            let start = Instant::now();
            let result = std::io::Write::write_all(&mut writer.file, &buffer);
            metrics.time("tmp_write", start.elapsed());
            if let Err(error) = &result {
                log::error!("pipelined download physical write failed: {error}");
            }
            (writer, buffer, result)
        }));
        Ok(())
    }

    pub async fn write_chunk(&mut self, chunk: Bytes) -> anyhow::Result<()> {
        let mut remaining = chunk.as_ref();
        while !remaining.is_empty() {
            let count = remaining.len().min(self.capacity - self.pending.len());
            if self.pending.capacity() == 0 {
                let capacity = self.capacity.min(self.allocation_left);
                self.pending = Vec::with_capacity(capacity);
                self.allocation_left -= capacity;
            }
            ensure!(
                self.pending.len() + count <= self.pending.capacity(),
                "pipelined body exceeds reserved size"
            );
            self.pending.extend_from_slice(&remaining[..count]);
            remaining = &remaining[count..];
            if self.pending.len() == self.capacity {
                self.dispatch().await?;
            }
        }
        Ok(())
    }

    pub async fn finish(&mut self) -> anyhow::Result<()> {
        if !self.pending.is_empty() {
            self.dispatch().await?;
        }
        self.pending = self.join().await?;
        Ok(())
    }
}

// The complete range buffer moves into the physical writer without cloning.
struct SegmentBuffer {
    data: Vec<u8>,
    #[cfg(test)]
    observer: Option<fn(i64)>,
}

impl SegmentBuffer {
    fn new(size: usize, _settings: Settings) -> Self {
        let data = Vec::with_capacity(size);
        #[cfg(test)]
        if let Some(observer) = _settings.allocation_observer {
            observer(data.capacity() as i64);
        }
        Self {
            data,
            #[cfg(test)]
            observer: _settings.allocation_observer,
        }
    }
}

#[cfg(test)]
impl Drop for SegmentBuffer {
    fn drop(&mut self) {
        if let Some(observer) = self.observer {
            observer(-(self.data.capacity() as i64));
        }
    }
}

/// HEAD pins a generation before any parallel request. No ETag means a single
/// stream fallback, not unguarded ranges. Errors never trigger a whole-file
/// retry. Each range gets at most two network attempts; a write is never retried.
#[cfg(unix)]
pub(super) async fn download_ranges<F, Fut>(
    fetch: F,
    path: &Path,
    size: usize,
    lifetime: Arc<dyn Send + Sync>,
    settings: Settings,
) -> anyhow::Result<Option<usize>>
where
    F: Fn(GetOptions) -> Fut,
    Fut: Future<Output = object_store::Result<GetResult>>,
{
    let head = tokio::time::timeout(
        settings.timeout,
        fetch(GetOptions {
            head: true,
            ..Default::default()
        }),
    )
    .await
    .context("range download HEAD timeout")??;
    ensure!(
        head.meta.size == size as u64,
        "range download registered size {size} differs from HEAD size {}",
        head.meta.size
    );
    let Some(etag) = head.meta.e_tag.clone().filter(|tag| !tag.is_empty()) else {
        return Ok(None);
    };
    drop(head);
    let owned_path = path.to_path_buf();
    let owned_lifetime = lifetime.clone();
    let writer = tokio::task::spawn_blocking(move || {
        let file = std::fs::File::create(&owned_path)
            .with_context(|| format!("create range download {}", owned_path.display()))?;
        file.set_len(size as u64)
            .context("preallocate range download")?;
        #[cfg(test)]
        let file = if settings.range_read_only {
            std::fs::File::open(&owned_path)?
        } else {
            file
        };
        Ok::<_, anyhow::Error>(Arc::new(ExactFileWriter {
            file,
            _lifetime: owned_lifetime,
        }))
    })
    .await
    .context("join range download creation")??;

    let completed = futures::stream::iter((0..size).step_by(settings.segment))
        .map(|start| {
            let fetch = &fetch;
            let etag = &etag;
            let writer = writer.clone();
            async move {
                let end = size.min(start.saturating_add(settings.segment));
                let range = start as u64..end as u64;
                let mut data = None;
                for attempt in 0..2 {
                    let read = async {
                        let response = fetch(GetOptions {
                            range: Some(GetRange::Bounded(range.clone())),
                            if_match: Some(etag.clone()),
                            ..Default::default()
                        })
                        .await
                        .context("GET range")?;
                        ensure!(
                            response.meta.size == size as u64,
                            "range object size changed"
                        );
                        ensure!(
                            response.meta.e_tag.as_ref() == Some(etag),
                            "range object ETag changed"
                        );
                        ensure!(
                            response.range == range,
                            "store returned unexpected range {:?}, expected {range:?}",
                            response.range
                        );
                        let mut body = response.into_stream();
                        let mut buffer = SegmentBuffer::new(end - start, settings);
                        loop {
                            let began = Instant::now();
                            let next = body.next().await;
                            settings.metrics.time("body_wait", began.elapsed());
                            let Some(chunk) = next else {
                                break;
                            };
                            let chunk = chunk.context("read range body")?;
                            settings.metrics.chunk(chunk.len());
                            ensure!(
                                chunk.len() <= end - start - buffer.data.len(),
                                "range body exceeds expected size"
                            );
                            buffer.data.extend_from_slice(&chunk);
                        }
                        ensure!(
                            buffer.data.len() == end - start,
                            "short range body: expected {}, got {}",
                            end - start,
                            buffer.data.len()
                        );
                        Ok::<_, anyhow::Error>(buffer)
                    };
                    let result = tokio::time::timeout(settings.timeout, read)
                        .await
                        .context("range request/body timeout")
                        .and_then(|r| r);
                    match result {
                        Ok(buffer) => {
                            data = Some(buffer);
                            break;
                        }
                        Err(error) if attempt == 0 => {
                            settings.metrics.retry();
                            log::warn!("retry range {range:?} once: {error:#}");
                        }
                        Err(error) => {
                            return Err(
                                error.context(format!("range {range:?} failed after two attempts"))
                            );
                        }
                    }
                }
                let buffer = data.context("range attempts produced no buffer")?;
                // Keep this slot and its charge until the physical write ends.
                // Timing out a write would permit a retry to race an old write.
                let waiting = Instant::now();
                let result = tokio::task::spawn_blocking(move || {
                    use std::os::unix::fs::FileExt;
                    #[cfg(test)]
                    if let Some(before_write) = settings.before_range_write {
                        before_write();
                    }
                    let began = Instant::now();
                    let result = writer.file.write_all_at(&buffer.data, start as u64);
                    settings.metrics.time("tmp_write", began.elapsed());
                    if let Err(error) = &result {
                        log::error!("range download physical write at {start} failed: {error}");
                    }
                    result.with_context(|| format!("write range {start}..{end}"))?;
                    Ok::<_, anyhow::Error>(buffer.data.len())
                })
                .await
                .context("join range download write")?;
                settings.metrics.time("sink_wait", waiting.elapsed());
                result
            }
        })
        .buffer_unordered(settings.concurrency)
        .try_fold(0usize, |total, length| async move { Ok(total + length) })
        .await?;
    ensure!(
        completed == size,
        "range download completed {completed} bytes, expected {size}"
    );
    Ok(Some(completed))
}

#[cfg(not(unix))]
pub(super) async fn download_ranges<F, Fut>(
    _fetch: F,
    _path: &Path,
    _size: usize,
    _lifetime: Arc<dyn Send + Sync>,
    _settings: Settings,
) -> anyhow::Result<Option<usize>>
where
    F: Fn(GetOptions) -> Fut,
    Fut: Future<Output = object_store::Result<GetResult>>,
{
    Ok(None)
}

/// A cross-device rename is never atomic. Observe by default because existing
/// deployments have not all verified their tmp/cache mount layout.
#[cfg(unix)]
pub(super) fn check_same_device(tmp: &Path, cache: &Path, fail_fast: bool) -> anyhow::Result<()> {
    use std::os::unix::fs::MetadataExt;
    let devices = (|| {
        std::fs::create_dir_all(cache)
            .with_context(|| format!("create cache directory {}", cache.display()))?;
        let tmp_device = std::fs::metadata(tmp)
            .with_context(|| format!("stat tmp directory {}", tmp.display()))?
            .dev();
        let cache_device = std::fs::metadata(cache)
            .with_context(|| format!("stat cache directory {}", cache.display()))?
            .dev();
        Ok::<_, anyhow::Error>((tmp_device, cache_device))
    })();
    match devices {
        Ok((tmp_device, cache_device)) => {
            check_device_ids(tmp_device, cache_device, tmp, cache, fail_fast)
        }
        Err(error) => device_check_error(error, fail_fast),
    }
}

#[cfg(unix)]
fn check_device_ids(
    tmp_device: u64,
    cache_device: u64,
    tmp: &Path,
    cache: &Path,
    fail_fast: bool,
) -> anyhow::Result<()> {
    if tmp_device != cache_device {
        return device_check_error(
            anyhow::anyhow!(
                "download tmp {} (device {tmp_device}) and cache {} (device {cache_device}) must share a filesystem for atomic publication",
                tmp.display(),
                cache.display()
            ),
            fail_fast,
        );
    }
    Ok(())
}

#[cfg(unix)]
fn device_check_error(error: anyhow::Error, fail_fast: bool) -> anyhow::Result<()> {
    config::metrics::FILE_DOWNLOAD_DEVICE_CHECK_ERRORS
        .with_label_values::<&str>(&[])
        .inc();
    log::error!("download device check failed: {error:#}");
    if fail_fast { Err(error) } else { Ok(()) }
}

#[cfg(test)]
mod tests;
