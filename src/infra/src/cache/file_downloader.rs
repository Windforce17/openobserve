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
    collections::VecDeque,
    sync::{
        Arc, LazyLock as Lazy,
        atomic::{AtomicUsize, Ordering},
    },
};

use config::{
    cluster::LOCAL_NODE,
    get_config,
    meta::cluster::{Role, RoleGroup, get_internal_grpc_token},
    metrics,
    utils::time::{day_micros, now_micros},
};
use futures::StreamExt;
use hashbrown::HashMap;

#[cfg(test)]
mod budget_tests;
mod governor;
mod scheduler;
pub(crate) use governor::throttle;
use proto::cluster_rpc::{SimpleFileList, event_client::EventClient};
pub use scheduler::DownloadClass;
use tokio::sync::{
    Mutex,
    mpsc::{Receiver, Sender},
};
use tonic::{codec::CompressionEncoding, metadata::MetadataValue};

use crate::{cache::file_data, cluster};

/// The result of optional background warming, not a claim that a download completed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueueDownloadOutcome {
    Accepted { bytes: usize },
    Deduplicated,
    Skipped,
    Cached,
    Rejected(DownloadRejection),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DownloadRejection {
    InvalidSize,
    TooLarge,
    Full,
    Closed,
}

#[derive(Default)]
struct AdmissionState {
    /// Queued and active keys: the dedupe identity and the count bound.
    files: HashMap<Arc<str>, Arc<AtomicUsize>>,
    /// Payload buffer charges of active workers. Queued metadata is count-bounded;
    /// memory and peer downloads retain conservative whole-object charges.
    active_bytes: usize,
}

struct DownloadAdmission {
    state: parking_lot::Mutex<AdmissionState>,
    /// Wakes workers waiting for active-byte headroom.
    released: tokio::sync::Notify,
    max_bytes: usize,
    max_files: usize,
    fair: bool,
    permits: Arc<tokio::sync::Semaphore>,
    closed: std::sync::atomic::AtomicBool,
}

impl DownloadAdmission {
    fn new(max_bytes: usize, max_files: usize) -> Self {
        let max_bytes = max_bytes.min(u32::MAX as usize);
        Self {
            state: parking_lot::Mutex::new(AdmissionState::default()),
            released: tokio::sync::Notify::new(),
            max_bytes,
            max_files,
            fair: false,
            permits: Arc::new(tokio::sync::Semaphore::new(
                max_bytes.min(u32::MAX as usize),
            )),
            closed: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Admit a key into the queue: dedupe + count bound + the one size that
    /// can never activate. Byte headroom is a worker-side wait
    /// ([`DownloadReservation::activate`]), never an enqueue rejection — a
    /// broadcast burst used to drop every fill past ~4 objects of a 1 GiB
    /// budget, and dropped fills were never retried, so those sidecars
    /// stayed remote for every later query.
    #[cfg(test)]
    fn reserve(
        self: &Arc<Self>,
        file: &str,
        bytes: usize,
    ) -> Result<DownloadReservation, QueueDownloadOutcome> {
        self.reserve_charged(file, bytes, bytes, DownloadClass::QueryBulk, 0)
    }

    fn reserve_charged(
        self: &Arc<Self>,
        file: &str,
        bytes: usize,
        charge: usize,
        class: DownloadClass,
        max_object: usize,
    ) -> Result<DownloadReservation, QueueDownloadOutcome> {
        let mut state = self.state.lock();
        if self.closed.load(Ordering::Acquire) {
            return Err(QueueDownloadOutcome::Rejected(DownloadRejection::Closed));
        }
        if let Some(requested) = state.files.get(file) {
            requested.fetch_min(class as usize, Ordering::AcqRel);
            return Err(QueueDownloadOutcome::Deduplicated);
        }
        if bytes == 0 {
            return Err(QueueDownloadOutcome::Rejected(
                DownloadRejection::InvalidSize,
            ));
        }
        if charge > self.max_bytes || (max_object > 0 && bytes > max_object) {
            return Err(QueueDownloadOutcome::Rejected(DownloadRejection::TooLarge));
        }
        if state.files.len() >= self.max_files {
            return Err(QueueDownloadOutcome::Rejected(DownloadRejection::Full));
        }
        // Clone a key only after admission. A single reservation covers cache
        // probing, queueing and the actual worker, without a dedupe gap.
        let file: Arc<str> = file.into();
        let requested_class = Arc::new(AtomicUsize::new(class as usize));
        state.files.insert(file.clone(), requested_class.clone());
        Ok(DownloadReservation {
            admission: self.clone(),
            file,
            bytes,
            charge,
            requested_class,
            permit: parking_lot::Mutex::new(None),
            activation: Mutex::new(()),
            active: std::sync::atomic::AtomicBool::new(false),
            priority_metric: None,
        })
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.permits.close();
        self.released.notify_waiters();
    }

    fn try_activate(&self, bytes: usize) -> bool {
        let mut state = self.state.lock();
        if bytes > self.max_bytes - state.active_bytes {
            return false;
        }
        state.active_bytes += bytes;
        true
    }
}

struct DownloadReservation {
    admission: Arc<DownloadAdmission>,
    file: Arc<str>,
    bytes: usize,
    charge: usize,
    requested_class: Arc<AtomicUsize>,
    permit: parking_lot::Mutex<Option<tokio::sync::OwnedSemaphorePermit>>,
    activation: Mutex<()>,
    /// Set once a worker owns the download and its bytes are charged.
    active: std::sync::atomic::AtomicBool,
    priority_metric: Option<bool>,
}

impl DownloadReservation {
    /// Charge this download's bytes against the active budget, waiting for
    /// headroom released by finishing downloads. Idempotent.
    async fn activate(&self) -> Result<(), DownloadRejection> {
        let _activation = self.activation.lock().await;
        if self.active.load(Ordering::Acquire) {
            return Ok(());
        }
        if self.admission.fair {
            // Tokio's weighted semaphore is FIFO: small arrivals cannot bypass
            // an earlier large waiter. Cancelling acquire removes its ticket.
            let permit = self
                .admission
                .permits
                .clone()
                .acquire_many_owned(self.charge as u32)
                .await
                .map_err(|_| DownloadRejection::Closed)?;
            self.admission.state.lock().active_bytes += self.charge;
            *self.permit.lock() = Some(permit);
            self.active.store(true, Ordering::Release);
            return Ok(());
        }
        loop {
            // register before checking so a release between the check and
            // the await cannot be missed
            let released = self.admission.released.notified();
            tokio::pin!(released);
            released.as_mut().enable();
            if self.admission.closed.load(Ordering::Acquire) {
                return Err(DownloadRejection::Closed);
            }
            if self.admission.try_activate(self.charge) {
                self.active
                    .store(true, std::sync::atomic::Ordering::Release);
                return Ok(());
            }
            released.await;
        }
    }

    fn record_queue(&mut self, priority: bool) {
        self.priority_metric = Some(priority);
        if priority {
            metrics::FILE_DOWNLOADER_PRIORITY_QUEUE_SIZE
                .with_label_values::<&str>(&[])
                .inc();
        } else {
            metrics::FILE_DOWNLOADER_NORMAL_QUEUE_SIZE
                .with_label_values::<&str>(&[])
                .inc();
        }
    }
}

impl Drop for DownloadReservation {
    fn drop(&mut self) {
        {
            let mut state = self.admission.state.lock();
            state.files.remove(self.file.as_ref());
            if self.active.load(std::sync::atomic::Ordering::Acquire) {
                state.active_bytes -= self.charge;
            }
        }
        self.admission.released.notify_waiters();
        // Preserve the existing gauge's queued + active meaning, including on
        // failed sends, worker cancellation and unwind.
        match self.priority_metric {
            Some(true) => metrics::FILE_DOWNLOADER_PRIORITY_QUEUE_SIZE
                .with_label_values::<&str>(&[])
                .dec(),
            Some(false) => metrics::FILE_DOWNLOADER_NORMAL_QUEUE_SIZE
                .with_label_values::<&str>(&[])
                .dec(),
            None => {}
        }
    }
}

struct FileInfo {
    trace_id: String,
    id: i64,
    account: String,
    cache: file_data::CacheType,
    reservation: Arc<DownloadReservation>,
    class: DownloadClass,
    queued_at: std::time::Instant,
    queued_ledger: scheduler::QueuedLedger,
}

struct DownloadQueue {
    sender: Sender<FileInfo>,
    receiver: Arc<Mutex<Receiver<FileInfo>>>,
}

impl DownloadQueue {
    fn new(sender: Sender<FileInfo>, receiver: Arc<Mutex<Receiver<FileInfo>>>) -> Self {
        Self { sender, receiver }
    }

    fn try_send(&self, item: FileInfo) -> Result<(), DownloadRejection> {
        self.sender.try_send(item).map_err(|error| match error {
            tokio::sync::mpsc::error::TrySendError::Full(_) => DownloadRejection::Full,
            tokio::sync::mpsc::error::TrySendError::Closed(_) => DownloadRejection::Closed,
        })
    }
}

struct PriorityDownloadQueue {
    stack: parking_lot::Mutex<VecDeque<FileInfo>>,
    notify: tokio::sync::Notify,
    max_size: usize,
    closed: std::sync::atomic::AtomicBool,
}

impl PriorityDownloadQueue {
    fn new(max_size: usize) -> Self {
        Self {
            stack: parking_lot::Mutex::new(VecDeque::with_capacity(max_size)),
            notify: tokio::sync::Notify::new(),
            max_size,
            closed: std::sync::atomic::AtomicBool::new(false),
        }
    }

    // New arrivals are rejected at capacity; accepted recent work remains LIFO.
    fn push(&self, file_info: FileInfo) -> bool {
        let mut stack = self.stack.lock();
        if self.closed.load(Ordering::Acquire) || stack.len() >= self.max_size {
            return false;
        }
        stack.push_back(file_info);
        drop(stack);
        self.notify.notify_one();
        true
    }

    fn close(&self) {
        let mut stack = self.stack.lock();
        self.closed.store(true, Ordering::Release);
        stack.clear();
        self.notify.notify_waiters();
    }

    // Blocks until an item is available. LIFO via pop_back.
    // notified() is created before the lock check to avoid missed-wakeup race.
    // Chains notify_one() so all workers drain the queue under burst load.
    async fn pop(&self) -> Option<FileInfo> {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut stack = self.stack.lock();
                if self.closed.load(Ordering::Acquire) {
                    return None;
                }
                if let Some(item) = stack.pop_back() {
                    if !stack.is_empty() {
                        self.notify.notify_one();
                    }
                    return Some(item);
                }
            }
            notified.await;
        }
    }
}

const FILE_DOWNLOAD_QUEUE_SIZE: usize = 10000;
static FILE_DOWNLOAD_CHANNEL: Lazy<DownloadQueue> = Lazy::new(|| {
    let (tx, rx) = tokio::sync::mpsc::channel::<FileInfo>(FILE_DOWNLOAD_QUEUE_SIZE);
    DownloadQueue::new(tx, Arc::new(Mutex::new(rx)))
});

static PRIORITY_FILE_DOWNLOAD_CHANNEL: Lazy<PriorityDownloadQueue> =
    Lazy::new(|| PriorityDownloadQueue::new(FILE_DOWNLOAD_QUEUE_SIZE));

static DOWNLOAD_ADMISSION: Lazy<Arc<DownloadAdmission>> = Lazy::new(|| {
    let cfg = get_config();
    let mut admission = DownloadAdmission::new(
        cfg.common
            .cache_latest_files_download_max_bytes
            .min(u32::MAX as usize),
        FILE_DOWNLOAD_QUEUE_SIZE * 2,
    );
    admission.fair = cfg.limit.file_download_in_flight_budget;
    Arc::new(admission)
});

static RUN_STARTED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

pub async fn run() -> Result<(), anyhow::Error> {
    if RUN_STARTED.swap(true, Ordering::AcqRel) {
        return Ok(());
    }
    let cfg = get_config();
    if cfg.limit.file_download_scheduler_enabled {
        scheduler::run();
    } else {
        // Separate fixed worker pools preserve normal FIFO and recent-file LIFO
        // service. Both pools share one queued + active object-byte budget.
        for thread in 0..cfg.limit.file_download_thread_num {
            let rx = FILE_DOWNLOAD_CHANNEL.receiver.clone();
            tokio::spawn(async move {
                until_shutdown(async move {
                    loop {
                        let ret = rx.lock().await.recv().await;
                        let Some(file) = ret else {
                            log::debug!(
                                "[FILE_CACHE_DOWNLOAD:JOB:NORMAL] Receiving channel is closed"
                            );
                            break;
                        };
                        process_download(thread, file).await;
                    }
                })
                .await;
            });
        }
        for thread in 0..cfg.limit.file_download_priority_queue_thread_num {
            tokio::spawn(async move {
                until_shutdown(async move {
                    loop {
                        let Some(file) = PRIORITY_FILE_DOWNLOAD_CHANNEL.pop().await else {
                            break;
                        };
                        process_download(thread, file).await;
                    }
                })
                .await;
            });
        }
    }
    // This watcher also cancels idle workers and drains queued reservations.
    tokio::spawn(async {
        while !config::cluster::is_offline() {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        shutdown().await;
    });
    Ok(())
}

static STOP: Lazy<tokio::sync::watch::Sender<bool>> =
    Lazy::new(|| tokio::sync::watch::channel(false).0);

pub async fn shutdown() {
    STOP.send_replace(true);
    DOWNLOAD_ADMISSION.close();
    scheduler::QUEUE.close();
    let mut rx = FILE_DOWNLOAD_CHANNEL.receiver.lock().await;
    rx.close();
    while rx.try_recv().is_ok() {}
    PRIORITY_FILE_DOWNLOAD_CHANNEL.close();
}

async fn until_shutdown(future: impl Future<Output = ()>) {
    let mut stop = STOP.subscribe();
    if *stop.borrow() {
        return;
    }
    tokio::select! { _ = future => {}, _ = stop.changed() => {} }
}

async fn process_download(thread: usize, file: FileInfo) {
    let FileInfo {
        trace_id,
        id,
        account,
        cache,
        reservation,
        class,
        queued_at,
        queued_ledger,
    } = file;
    queued_ledger.start();
    let mut ledger = scheduler::ActiveLedger::new(class, queued_at.elapsed());
    let name = reservation.file.as_ref();
    let size = reservation.bytes;
    // Queued items hold no object bytes; the byte budget bounds what workers
    // hold. Wait here (behind earlier activations) instead of rejecting.
    let budget_wait = std::time::Instant::now();
    if reservation.activate().await.is_err() {
        return;
    }
    scheduler::time(class, "budget", budget_wait.elapsed());
    let started = std::time::Instant::now();
    match governor::BACKGROUND
        .scope(
            true,
            download_file(
                thread,
                &trace_id,
                id,
                &account,
                name,
                size,
                cache,
                reservation.clone(),
            ),
        )
        .await
    {
        Ok(data_len) => {
            ledger.finish();
            scheduler::record_completion(
                class,
                if data_len == 0 {
                    "already_cached"
                } else {
                    "completed"
                },
                data_len,
                started.elapsed(),
            );
            if data_len > 0 && data_len != size {
                log::warn!(
                    "[FILE_CACHE_DOWNLOAD:JOB] download file {name} found size mismatch, expected: {size}, actual: {data_len}"
                );
            }
        }
        Err(e) => {
            ledger.finish();
            scheduler::record_completion(class, "failed", 0, started.elapsed());
            log::error!(
                "[FILE_CACHE_DOWNLOAD:JOB] download file {name} to cache {cache:?} err: {e}"
            );
        }
    }
    // Explicitly retain the byte charge and dedupe identity until IO finishes.
    drop(reservation);
}

async fn download_file(
    thread: usize,
    trace_id: &str,
    file_id: i64,
    account: &str,
    file_name: &str,
    file_size: usize,
    cache_type: file_data::CacheType,
    reservation: Arc<DownloadReservation>,
) -> Result<usize, anyhow::Error> {
    let cfg = get_config();

    // Peer lookup/download failure still permits the ordinary object-store
    // fallback, but is observable rather than being silently discarded.
    if cfg.cache_latest_files.download_from_node {
        match download_file_with_consistent_hash(
            file_id,
            file_name,
            cache_type,
            reservation.clone(),
        )
        .await
        {
            Ok(true) => return Ok(file_size),
            Ok(false) => {}
            Err(error) => log::warn!(
                "[FILE_CACHE_DOWNLOAD:JOB] peer download failed for {file_name}, falling back to object storage: {error}"
            ),
        }
    }

    // download from object store
    let start = std::time::Instant::now();
    let ret = match cache_type {
        file_data::CacheType::Memory => {
            let mut disk_exists = false;
            let mem_exists = file_data::memory::exist(file_name).await;
            if !mem_exists && !cfg.memory_cache.skip_disk_check {
                disk_exists = file_data::disk::exist(file_name).await;
            }
            if !mem_exists && (cfg.memory_cache.skip_disk_check || !disk_exists) {
                let (data_len, data) =
                    file_data::download_from_storage_exact(account, file_name, file_size).await?;
                file_data::memory::set(file_name, data).await?;
                Ok(data_len)
            } else {
                Ok(0)
            }
        }
        file_data::CacheType::Disk => {
            if !file_data::disk::exist(file_name).await {
                file_data::disk::download_exact(account, file_name, file_size, reservation).await
            } else {
                Ok(0)
            }
        }
        _ => Ok(0),
    };
    log::debug!(
        "[FILE_CACHE_DOWNLOAD:JOB:{thread}] [trace_id {trace_id}] download file: {file_name}, ret: {:?}, took: {} ms",
        ret,
        start.elapsed().as_millis()
    );
    ret
}

async fn download_file_with_consistent_hash(
    file_id: i64,
    file_name: &str,
    cache_type: file_data::CacheType,
    reservation: Arc<DownloadReservation>,
) -> Result<bool, anyhow::Error> {
    let role_group = if LOCAL_NODE.is_interactive_querier() {
        RoleGroup::Interactive
    } else {
        RoleGroup::Background
    };
    let Some(node_name) = cluster::get_node_from_consistent_hash(
        &file_id.to_string(),
        &Role::Querier,
        Some(role_group),
    )
    .await
    else {
        return Ok(false);
    };
    // get node by file_id
    let Some(node) = cluster::get_cached_node_by_name(&node_name).await else {
        return Ok(false);
    };
    download_from_node(&node.grpc_addr, file_name, cache_type, reservation).await
}

/// Fetch one explicitly admitted object; no direct batch bypass of admission.
async fn download_from_node(
    addr: &str,
    file: &str,
    cache_type: file_data::CacheType,
    reservation: Arc<DownloadReservation>,
) -> Result<bool, anyhow::Error> {
    let cfg = get_config();
    if reservation.bytes > (cfg.cache_latest_files.download_node_size * 1024 * 1024) as usize {
        return Ok(false);
    }
    let token: MetadataValue<_> = get_internal_grpc_token()
        .parse()
        .map_err(|_| anyhow::anyhow!("Invalid token"))?;
    let channel = crate::client::grpc::get_cached_channel(addr).await?;
    let client = EventClient::with_interceptor(channel, move |mut req: tonic::Request<()>| {
        req.metadata_mut().insert("authorization", token.clone());
        Ok(req)
    });
    let request = tonic::Request::new(SimpleFileList {
        files: vec![file.to_owned()],
    });
    let resp = client
        .send_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Gzip)
        .max_decoding_message_size(cfg.grpc.max_message_size * 1024 * 1024)
        .max_encoding_message_size(cfg.grpc.max_message_size * 1024 * 1024)
        .get_files(request)
        .await
        .map_err(|error| anyhow::anyhow!("Failed to get file from {addr}: {error}"))?;
    let mut data = bytes::BytesMut::new();
    let mut stream = resp.into_inner();
    while let Some(response) = stream.next().await {
        let response = match response {
            Ok(response) => response,
            Err(error) if error.code() == tonic::Code::NotFound => return Ok(false),
            Err(error) => return Err(error.into()),
        };
        for content in response.entries {
            if content.filename != file {
                return Err(anyhow::anyhow!(
                    "peer {addr} returned unrequested file {}",
                    content.filename
                ));
            }
            if content.content.len() > reservation.bytes.saturating_sub(data.len()) {
                return Err(anyhow::anyhow!(
                    "peer {addr} returned file {file} beyond registered size {}",
                    reservation.bytes
                ));
            }
            throttle(content.content.len()).await;
            data.extend_from_slice(&content.content);
        }
    }
    if data.len() != reservation.bytes {
        return Err(anyhow::anyhow!(
            "peer {addr} returned short file {file}: expected {}, got {}",
            reservation.bytes,
            data.len()
        ));
    }
    let data = data.freeze();
    match cache_type {
        file_data::CacheType::Disk => {
            file_data::disk::set_admitted(file, data, reservation).await?;
            Ok(file_data::disk::exist(file).await)
        }
        file_data::CacheType::Memory => {
            file_data::memory::set(file, data).await?;
            Ok(file_data::memory::exist(file).await)
        }
        file_data::CacheType::None => Ok(false),
    }
}

/// Try to admit independently owned warming. This never waits for queue capacity
/// and retains no owned candidate data before byte/count admission. Cache probes
/// may await, but their reservation is bounded and released on cancellation.
pub async fn queue_download(
    trace_id: &str,
    id: i64,
    account: &str,
    file: &str,
    size: i64,
    ts: i64,
    cache_type: file_data::CacheType,
) -> QueueDownloadOutcome {
    let class = if trace_id == file_data::TRACE_ID_FOR_CACHE_LATEST_FILE {
        if should_prioritize_file(
            ts,
            get_config().limit.file_download_priority_queue_window_secs,
        ) {
            DownloadClass::BroadcastLatest
        } else {
            DownloadClass::QueryBulk
        }
    } else {
        DownloadClass::QueryBulk
    };
    queue_download_class(trace_id, id, account, file, size, ts, cache_type, class).await
}

#[allow(clippy::too_many_arguments)]
pub async fn queue_download_class(
    trace_id: &str,
    id: i64,
    account: &str,
    file: &str,
    size: i64,
    ts: i64,
    cache_type: file_data::CacheType,
    class: DownloadClass,
) -> QueueDownloadOutcome {
    let outcome =
        queue_download_inner(trace_id, id, account, file, size, ts, cache_type, class).await;
    scheduler::record_admission(class, outcome, size.max(0) as u64);
    if outcome == QueueDownloadOutcome::Deduplicated {
        scheduler::QUEUE.wake();
    }
    outcome
}

#[allow(clippy::too_many_arguments)]
async fn queue_download_inner(
    trace_id: &str,
    id: i64,
    account: &str,
    file: &str,
    size: i64,
    ts: i64,
    cache_type: file_data::CacheType,
    class: DownloadClass,
) -> QueueDownloadOutcome {
    if cache_type == file_data::CacheType::None || exceeds_cache_max_age(ts, cache_type) {
        return QueueDownloadOutcome::Skipped;
    }
    let Ok(size) = usize::try_from(size) else {
        return QueueDownloadOutcome::Rejected(DownloadRejection::InvalidSize);
    };
    let cfg = get_config();
    let charge = admission_charge(
        size,
        cache_type,
        cfg.limit.file_download_in_flight_budget,
        cfg.cache_latest_files.download_from_node,
    );
    let max_object = if cfg.limit.file_download_in_flight_budget {
        cfg.limit.file_download_max_object_bytes
    } else {
        0
    };
    let mut reservation =
        match DOWNLOAD_ADMISSION.reserve_charged(file, size, charge, class, max_object) {
            Ok(reservation) => reservation,
            Err(outcome) => return outcome,
        };
    let cfg = get_config();
    let cached = match cache_type {
        file_data::CacheType::Memory => {
            file_data::memory::exist(file).await
                || (!cfg.memory_cache.skip_disk_check && file_data::disk::exist(file).await)
        }
        file_data::CacheType::Disk => file_data::disk::exist(file).await,
        file_data::CacheType::None => unreachable!(),
    };
    if cached {
        return QueueDownloadOutcome::Cached;
    }
    let priority = cfg.limit.file_download_enable_priority_queue
        && should_prioritize_file(ts, cfg.limit.file_download_priority_queue_window_secs);
    reservation.record_queue(priority);
    let item = FileInfo {
        trace_id: trace_id.to_owned(),
        id,
        account: account.to_owned(),
        cache: cache_type,
        reservation: Arc::new(reservation),
        class,
        queued_at: std::time::Instant::now(),
        queued_ledger: scheduler::QueuedLedger::new(class),
    };
    if cfg.limit.file_download_scheduler_enabled {
        if let Err(reason) = scheduler::QUEUE.push(item) {
            return QueueDownloadOutcome::Rejected(reason);
        }
    } else if priority {
        if !PRIORITY_FILE_DOWNLOAD_CHANNEL.push(item) {
            return QueueDownloadOutcome::Rejected(DownloadRejection::Full);
        }
    } else if let Err(reason) = FILE_DOWNLOAD_CHANNEL.try_send(item) {
        return QueueDownloadOutcome::Rejected(reason);
    }
    QueueDownloadOutcome::Accepted { bytes: size }
}

fn admission_charge(
    size: usize,
    cache: file_data::CacheType,
    in_flight: bool,
    peer: bool,
) -> usize {
    if in_flight && cache == file_data::CacheType::Disk && !peer {
        crate::cache::disk_download_in_flight_bytes(size as u64) as usize
    } else {
        size
    }
}

/// Mark an admission-time foreground signal without storing query identities.
pub fn note_query_activity() {
    governor::note_query_activity();
}

/// Returns true when the record count is unknown or the file contains enough
/// records to be worth downloading into the cache.
pub fn should_download(records: i64) -> bool {
    // A zero value can mean the record count was not populated by an older
    // gRPC sender. Treat it as unknown rather than as an undersized file.
    records == 0 || records >= get_config().limit.file_download_min_records
}

// if the file timestamp is in the past window, it should be prioritized
fn should_prioritize_file(ts: i64, window_secs: i64) -> bool {
    let window_micros = window_secs * 1_000_000;
    let now = now_micros();
    ts > now - window_micros
}

/// Returns true if the file's data is older than the cache max age and should
/// not be downloaded into the cache.
pub fn exceeds_cache_max_age(ts: i64, cache_type: file_data::CacheType) -> bool {
    let cfg = get_config();
    let max_age_days = match cache_type {
        file_data::CacheType::Memory => cfg.memory_cache.max_age_days,
        file_data::CacheType::Disk => cfg.disk_cache.max_age_days,
        file_data::CacheType::None => 0,
    };
    exceeds_max_age(ts, max_age_days)
}

// if the file data is older than max_age_days, it should not be cached.
// max_age_days == 0 means no limit, ts <= 0 means the timestamp is unknown.
fn exceeds_max_age(ts: i64, max_age_days: i64) -> bool {
    if max_age_days <= 0 || ts <= 0 {
        return false;
    }
    ts < now_micros() - day_micros(max_age_days)
}

#[cfg(test)]
mod tests {
    use config::utils::time::{day_micros, hour_micros, now_micros};

    use super::{
        Arc, DownloadAdmission, DownloadQueue, DownloadRejection, FileInfo, Mutex,
        PriorityDownloadQueue, QueueDownloadOutcome, exceeds_max_age, file_data, should_download,
    };

    #[test]
    fn test_should_download() {
        assert!(should_download(0));
        let minimum = config::get_config().limit.file_download_min_records;
        if minimum > 1 {
            assert!(!should_download(minimum - 1));
        }
        assert!(should_download(minimum));
    }

    #[test]
    fn test_exceeds_max_age_disabled() {
        // max_age_days == 0 means no limit, nothing is too old
        let one_year_ago = now_micros() - day_micros(365);
        assert!(!exceeds_max_age(one_year_ago, 0));
        assert!(!exceeds_max_age(one_year_ago, -1));
    }

    #[test]
    fn test_exceeds_max_age_unknown_ts() {
        // unknown timestamp should not be skipped
        assert!(!exceeds_max_age(0, 3));
        assert!(!exceeds_max_age(-1, 3));
    }

    #[test]
    fn test_exceeds_max_age_recent_file() {
        let one_hour_ago = now_micros() - hour_micros(1);
        assert!(!exceeds_max_age(one_hour_ago, 3));
    }

    #[test]
    fn test_exceeds_max_age_old_file() {
        let thirty_days_ago = now_micros() - day_micros(30);
        assert!(exceeds_max_age(thirty_days_ago, 3));
    }

    pub(super) fn queued_item(
        admission: &Arc<DownloadAdmission>,
        file: &str,
        bytes: usize,
    ) -> FileInfo {
        FileInfo {
            trace_id: "queue-regression".into(),
            id: 1,
            account: "org".into(),
            cache: file_data::CacheType::Disk,
            reservation: Arc::new(admission.reserve(file, bytes).unwrap()),
            class: super::DownloadClass::QueryBulk,
            queued_at: std::time::Instant::now(),
            queued_ledger: super::scheduler::QueuedLedger::new(super::DownloadClass::QueryBulk),
        }
    }

    #[tokio::test]
    async fn stalled_normal_queue_rejects_without_retaining_candidates() {
        let admission = Arc::new(DownloadAdmission::new(100, 10));
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let queue = DownloadQueue::new(tx, Arc::new(Mutex::new(rx)));
        queue
            .try_send(queued_item(&admission, "accepted", 40))
            .unwrap();
        for _ in 0..1000 {
            // Reusing the rejected key must remain a capacity rejection, never
            // a phantom duplicate left behind by a failed send.
            let item = queued_item(&admission, "rejected", 40);
            assert_eq!(queue.try_send(item), Err(DownloadRejection::Full));
        }
        let active = queue.receiver.lock().await.recv().await.unwrap();
        assert!(matches!(
            admission.reserve("accepted", 40),
            Err(QueueDownloadOutcome::Deduplicated)
        ));
        // Receiving is not completion: bytes stay charged while a worker owns
        // the item even though the queue now has capacity — but that only
        // gates ACTIVATION of later work, never its admission to the queue.
        active.reservation.activate().await.unwrap();
        let later = admission.reserve("too-much-active-work", 61).unwrap();
        assert!(!admission.try_activate(later.bytes));
        drop(active);
        assert!(admission.try_activate(later.bytes));
        drop(later);
        drop(admission.reserve("accepted", 100).unwrap());
        queue.receiver.lock().await.close();
        assert_eq!(
            queue.try_send(queued_item(&admission, "closed", 100)),
            Err(DownloadRejection::Closed)
        );
        drop(admission.reserve("closed", 100).unwrap());
    }

    #[tokio::test]
    async fn priority_queue_is_bounded_lifo_and_shares_active_bytes() {
        let admission = Arc::new(DownloadAdmission::new(100, 10));
        let queue = PriorityDownloadQueue::new(2);
        assert!(queue.push(queued_item(&admission, "older", 30)));
        assert!(queue.push(queued_item(&admission, "newer", 30)));
        for _ in 0..1000 {
            assert!(!queue.push(queued_item(&admission, "rejected", 30)));
        }
        let newer = queue.pop().await.unwrap();
        assert_eq!(newer.reservation.file.as_ref(), "newer");
        newer.reservation.activate().await.unwrap();
        // a queued 71-byte fill waits for the 30 active bytes, and wakes on
        // their release without polling
        let waiting = queued_item(&admission, "over-budget", 71);
        assert!(!admission.try_activate(waiting.reservation.bytes));
        let waiter = {
            let reservation = Arc::clone(&waiting.reservation);
            tokio::spawn(async move { reservation.activate().await.unwrap() })
        };
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        drop(newer);
        tokio::time::timeout(std::time::Duration::from_secs(5), waiter)
            .await
            .expect("activation wakes on release")
            .unwrap();
        assert!(
            waiting
                .reservation
                .active
                .load(std::sync::atomic::Ordering::Acquire)
        );
        let older = queue.pop().await.unwrap();
        assert_eq!(older.reservation.file.as_ref(), "older");
        drop(older);
        drop(waiting);
        drop(admission.reserve("rejected", 100).unwrap());
    }

    /// Shutdown must not strand an idle priority worker or leave queued
    /// reservations holding dedupe keys, and late producers must be refused.
    #[tokio::test]
    async fn priority_queue_close_wakes_idle_workers_and_refuses_pushes() {
        let admission = Arc::new(DownloadAdmission::new(100, 10));
        let queue = Arc::new(PriorityDownloadQueue::new(4));
        let idle = {
            let queue = Arc::clone(&queue);
            tokio::spawn(async move { queue.pop().await.is_none() })
        };
        tokio::task::yield_now().await;
        assert!(!idle.is_finished());
        assert!(queue.push(queued_item(&admission, "queued", 10)));
        let first = queue.pop().await.unwrap();
        drop(first);
        assert!(queue.push(queued_item(&admission, "stranded", 10)));
        queue.close();
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(5), idle)
                .await
                .expect("close wakes idle pop")
                .unwrap()
        );
        // The queued item was dropped by close, so its key is free again.
        drop(admission.reserve("stranded", 10).unwrap());
        assert!(!queue.push(queued_item(&admission, "late", 10)));
        drop(admission.reserve("late", 10).unwrap());
        assert!(queue.pop().await.is_none());
    }

    /// Queue admission never rejects on byte headroom: a burst larger than
    /// the whole budget queues in full (bounded by count) and drains as
    /// workers release active bytes.
    #[tokio::test]
    async fn bursts_beyond_the_byte_budget_queue_instead_of_dropping() {
        let admission = Arc::new(DownloadAdmission::new(100, 10));
        let burst: Vec<FileInfo> = (0..8)
            .map(|index| queued_item(&admission, &format!("burst-{index}"), 60))
            .collect();
        assert_eq!(admission.state.lock().files.len(), 8);
        assert_eq!(admission.state.lock().active_bytes, 0);
        burst[0].reservation.activate().await.unwrap();
        assert!(!admission.try_activate(60));
        drop(burst);
        assert_eq!(admission.state.lock().active_bytes, 0);
        assert!(admission.state.lock().files.is_empty());
    }

    #[test]
    fn byte_and_count_admission_rejections_do_not_reserve_keys() {
        let admission = Arc::new(DownloadAdmission::new(100, 1));
        assert!(matches!(
            admission.reserve("retry", 101),
            Err(QueueDownloadOutcome::Rejected(DownloadRejection::TooLarge))
        ));
        assert!(matches!(
            admission.reserve("retry", 0),
            Err(QueueDownloadOutcome::Rejected(
                DownloadRejection::InvalidSize
            ))
        ));
        let first = admission.reserve("retry", 1).unwrap();
        assert!(matches!(
            admission.reserve("second", 1),
            Err(QueueDownloadOutcome::Rejected(DownloadRejection::Full))
        ));
        drop(first);
        drop(admission.reserve("second", 100).unwrap());
    }

    #[tokio::test]
    async fn cancelled_active_worker_releases_bytes_and_dedupe() {
        let admission = Arc::new(DownloadAdmission::new(100, 1));
        let item = queued_item(&admission, "cancelled", 100);
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let worker = tokio::spawn(async move {
            let _item = item;
            started_tx.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        started_rx.await.unwrap();
        assert!(matches!(
            admission.reserve("cancelled", 100),
            Err(QueueDownloadOutcome::Deduplicated)
        ));
        worker.abort();
        assert!(worker.await.unwrap_err().is_cancelled());
        drop(admission.reserve("cancelled", 100).unwrap());
    }

    #[test]
    fn failed_worker_unwind_releases_bytes_and_dedupe() {
        let admission = Arc::new(DownloadAdmission::new(100, 1));
        let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _item = queued_item(&admission, "failed", 100);
            panic!("simulated worker failure");
        }));
        assert!(failed.is_err());
        drop(admission.reserve("failed", 100).unwrap());
    }
}
