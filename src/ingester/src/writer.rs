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
    path::PathBuf,
    sync::{
        Arc, LazyLock as Lazy,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
    time::Instant,
};

use arrow_schema::Schema;
use chrono::{Duration, Utc};
use config::{
    MEM_TABLE_INDIVIDUAL_STREAMS, get_config, metrics,
    stats::MemorySize,
    utils::hash::{Sum64, gxhash},
};
use hashbrown::HashSet;
use infra::runtime::WAL_RUNTIME;
use snafu::ResultExt;
use tokio::sync::{RwLock, mpsc, oneshot};
use wal::{Writer as WalWriter, build_file_path};

use crate::{
    ReadRecordBatchEntry, WriterSignal,
    entry::Entry,
    errors::*,
    immutable::{IMMUTABLES, Immutable},
    memtable::MemTable,
    rwmap::RwMap,
};

static WRITERS: Lazy<Vec<RwMap<WriterKey, Arc<Writer>>>> = Lazy::new(|| {
    let cfg = get_config();
    let writer_num = cfg.limit.mem_table_bucket_num + MEM_TABLE_INDIVIDUAL_STREAMS.len();
    let mut writers = Vec::with_capacity(writer_num);
    for _ in 0..writer_num {
        writers.push(RwMap::default());
    }
    writers
});

pub struct Writer {
    idx: usize,
    key: WriterKey,
    wal: Arc<RwLock<WalWriter>>,
    memtable: Arc<RwLock<MemTable>>,
    next_seq: AtomicU64,
    created_at: AtomicI64,
    write_queue: Arc<mpsc::Sender<crate::WriterQueueItem>>,
}

// check total memtable size
pub fn check_memtable_size() -> Result<()> {
    let cur_mem = metrics::INGEST_MEMTABLE_ARROW_BYTES
        .with_label_values::<&str>(&[])
        .get();
    if cur_mem >= get_config().limit.mem_table_max_size as i64 {
        Err(Error::MemoryTableOverflowError {})
    } else {
        Ok(())
    }
}

// check total memory size
//
// The sampled process RSS (NODE_MEMORY_USAGE, updated every second) is
// topped up with the projected bytes of admitted in-flight ingest requests
// (see crate::admission), so allocations that are about to happen count
// against the envelope before they show up in RSS. With no reservations
// outstanding the trip point is exactly the historical one.
pub fn check_memory_circuit_breaker() -> Result<()> {
    let cfg = get_config();
    if !cfg.common.memory_circuit_breaker_enabled || cfg.common.memory_circuit_breaker_ratio == 0 {
        return Ok(());
    }
    let cur_mem = metrics::NODE_MEMORY_USAGE
        .with_label_values::<&str>(&[])
        .get() as usize;
    let cur_mem = cur_mem.saturating_add(crate::admission::reserved_bytes());
    // single source of truth for the envelope arithmetic — shared with the
    // pre-body admission path so both trip at exactly the same point
    if cur_mem
        > crate::admission::envelope_from(
            cfg.limit.mem_total,
            cfg.common.memory_circuit_breaker_ratio,
        )
    {
        Err(Error::MemoryCircuitBreakerError {})
    } else {
        Ok(())
    }
}

// check disk space availability
// Threshold interpretation (similar to memory circuit breaker):
// - Values < 100: treated as percentage of disk used (e.g., 90 = trigger when 90% full)
// - Values >= 100: treated as absolute MB remaining (e.g., 500 = trigger when < 500MB free)
// Reads from atomic metrics updated every 60 seconds to avoid expensive syscalls
pub fn check_disk_circuit_breaker() -> Result<()> {
    let cfg = get_config();
    if !cfg.common.disk_circuit_breaker_enabled {
        return Ok(());
    }

    let threshold = cfg.common.disk_circuit_breaker_threshold;
    let total_space = metrics::NODE_DISK_TOTAL
        .with_label_values::<&str>(&[])
        .get() as u64;
    let used_space = metrics::NODE_DISK_USAGE
        .with_label_values::<&str>(&[])
        .get() as u64;

    let triggered = if threshold < 100 {
        // Percentage mode: trigger when disk usage exceeds threshold%
        // e.g., threshold=90 means trigger when disk is >90% full
        used_space > total_space / 100 * threshold as u64
    } else {
        // Absolute MB mode: trigger when free space is less than threshold MB
        let available_space = total_space.saturating_sub(used_space);
        available_space < (threshold as u64) * 1024 * 1024
    };

    if triggered {
        Err(Error::DiskCircuitBreakerError {})
    } else {
        Ok(())
    }
}

fn get_table_idx(thread_id: usize, org_id: &str, stream_name: &str) -> usize {
    if let Some(idx) = MEM_TABLE_INDIVIDUAL_STREAMS.get(stream_name) {
        *idx
    } else if get_config().common.feature_shared_memtable_enabled {
        // When shared memtable is enabled, hash by thread_id and org_id
        let hash_key = format!("{thread_id}_{org_id}");
        let hash_id = gxhash::new().sum64(&hash_key);
        hash_id as usize % (WRITERS.len() - MEM_TABLE_INDIVIDUAL_STREAMS.len())
    } else {
        // Original behavior: hash by thread_id and stream_name
        let hash_key = format!("{thread_id}_{stream_name}");
        let hash_id = gxhash::new().sum64(&hash_key);
        hash_id as usize % (WRITERS.len() - MEM_TABLE_INDIVIDUAL_STREAMS.len())
    }
}

/// Get a writer for a given org_id and stream_type
pub async fn get_writer(
    thread_id: usize,
    org_id: &str,
    stream_type: &str,
    stream_name: &str,
) -> Arc<Writer> {
    let start = std::time::Instant::now();
    let idx = get_table_idx(thread_id, org_id, stream_name);
    let key = WriterKey::new(idx, org_id, stream_type);
    let r = WRITERS[idx].read().await;
    let data = r.get(&key);
    if start.elapsed().as_millis() > 500 {
        log::warn!(
            "get_writer from read cache took: {} ms",
            start.elapsed().as_millis()
        );
    }
    let mut is_existing_writer_channel_closed = false;
    if let Some(w) = data {
        if !w.is_channel_closed() {
            return w.clone();
        }
        is_existing_writer_channel_closed = true;
    }
    drop(r);

    if is_existing_writer_channel_closed {
        log::warn!(
            "[INGESTER:MEM:{idx}] Writer channel closed for {org_id}/{stream_type}, removing from cache",
        );
        let mut w = WRITERS[idx].write().await;
        w.remove(&key);
        drop(w);
    }

    // slow path
    let start = std::time::Instant::now();
    let mut rw = WRITERS[idx].write().await;
    let w = rw
        .entry(key.clone())
        .or_insert_with(|| Writer::new(idx, key));
    if start.elapsed().as_millis() > 500 {
        log::warn!(
            "get_writer from write cache took: {} ms",
            start.elapsed().as_millis()
        );
    }
    w.clone()
}

pub async fn read_from_memtable(
    org_id: &str,
    stream_type: &str,
    stream_name: &str,
    time_range: Option<(i64, i64)>,
    partition_filters: &[(String, Vec<String>)],
) -> Result<(HashSet<u64>, Vec<ReadRecordBatchEntry>)> {
    let cfg = get_config();
    // fast past
    if cfg.limit.mem_table_bucket_num <= 1 {
        let idx = get_table_idx(0, org_id, stream_name);
        let key = WriterKey::new(idx, org_id, stream_type);
        let w = WRITERS[idx].read().await;
        return match w.get(&key) {
            Some(r) => {
                let (id, batches) = r
                    .read(org_id, stream_name, time_range, partition_filters)
                    .await?;
                Ok((HashSet::from([id]), batches))
            }
            None => Ok((HashSet::new(), Vec::new())),
        };
    }

    // slow path
    let mut ids = HashSet::new();
    let mut batches = Vec::new();
    let mut visited = HashSet::with_capacity(cfg.limit.mem_table_bucket_num);
    for thread_id in 0..cfg.limit.http_worker_num {
        let idx = get_table_idx(thread_id, org_id, stream_name);
        if visited.contains(&idx) {
            continue;
        }
        visited.insert(idx);
        let key = WriterKey::new(idx, org_id, stream_type);
        let w = WRITERS[idx].read().await;
        if let Some(r) = w.get(&key)
            && let Ok((id, data)) = r
                .read(org_id, stream_name, time_range, partition_filters)
                .await
        {
            ids.insert(id);
            batches.extend(data);
        }
    }
    Ok((ids, batches))
}

pub async fn check_ttl() -> Result<()> {
    for w in WRITERS.iter() {
        let w = w.read().await;
        for r in w.values() {
            if let Err(e) = r
                .write_queue
                .send((
                    WriterSignal::Rotate,
                    crate::ProcessedBatch::empty(),
                    false,
                    None,
                ))
                .await
            {
                log::error!("[INGESTER:MEM:{}] writer queue rotate error: {e}", r.idx);
            }
        }
    }
    Ok(())
}

pub async fn flush_all() -> Result<()> {
    log::info!("[INGESTER:MEM] start flush all writers");
    for w in WRITERS.iter() {
        let mut w = w.write().await;
        let keys = w.keys().cloned().collect::<Vec<_>>();
        for key in keys {
            if let Some(r) = w.remove(&key) {
                r.flush().await?; // close writer
                metrics::INGEST_MEMTABLE_FILES
                    .with_label_values::<&str>(&[])
                    .dec();
            }
        }
    }
    log::info!("[INGESTER:MEM] flush all writers done");
    Ok(())
}

// get the max seq id of all writers
pub async fn get_max_writer_seq_id() -> u64 {
    let mut max_seq_id = 0;
    for w in WRITERS.iter() {
        let w = w.read().await;
        for r in w.values() {
            // next_seq is the next seq id to be used, so we need to subtract 1
            max_seq_id = max_seq_id.max(r.next_seq.load(Ordering::Relaxed) - 1);
        }
    }
    max_seq_id
}

impl Writer {
    pub(crate) fn new(idx: usize, key: WriterKey) -> Arc<Writer> {
        let now = Utc::now().timestamp_micros();
        let cfg = get_config();
        let next_seq = AtomicU64::new(now as u64);
        let wal_id = next_seq.fetch_add(1, Ordering::SeqCst);
        let wal_dir = PathBuf::from(&cfg.common.data_wal_dir)
            .join("logs")
            .join(idx.to_string());
        log::info!(
            "[INGESTER:MEM:{idx}] create file: {}/{}/{}/{}.wal",
            wal_dir.display(),
            key.org_id,
            key.stream_type,
            wal_id
        );

        let (tx, rx) = mpsc::channel(cfg.limit.wal_write_queue_size);

        let writer = Self {
            idx,
            key: key.clone(),
            wal: Arc::new(RwLock::new(
                WalWriter::new(
                    build_file_path(wal_dir, &key.org_id, &key.stream_type, wal_id.to_string()),
                    cfg.limit.max_file_size_on_disk as u64,
                    cfg.limit.wal_write_buffer_size,
                    None,
                )
                .expect("wal file create error")
                .0,
            )),
            memtable: Arc::new(RwLock::new(MemTable::new())),
            next_seq,
            created_at: AtomicI64::new(now),
            write_queue: Arc::new(tx),
        };
        let writer = Arc::new(writer);
        let writer_clone = writer.clone();

        log::info!("[INGESTER:MEM:{idx}] writer queue start consuming");

        // Spawn consumer tasks on the shared WAL runtime, or use the default runtime
        if let Some(rt) = WAL_RUNTIME.as_ref() {
            rt.spawn(Self::consume_loop(writer, rx, idx));
        } else {
            tokio::spawn(Self::consume_loop(writer, rx, idx));
        }

        writer_clone
    }

    async fn consume_loop(
        writer: Arc<Writer>,
        mut rx: mpsc::Receiver<crate::WriterQueueItem>,
        idx: usize,
    ) {
        let mut total: usize = 0;
        loop {
            match rx.recv().await {
                None => break,
                Some((sign, batch, fsync, ack)) => match sign {
                    WriterSignal::Close => break,
                    WriterSignal::Rotate => {
                        let ret = writer.rotate(0, 0).await;
                        if let Err(e) = &ret {
                            log::error!("[INGESTER:MEM:{idx}] writer rotate error: {e}");
                        }
                        if let Some(ack) = ack {
                            let _ = ack.send(ret);
                        }
                    }
                    WriterSignal::Produce => {
                        // the outcome belongs to whoever acks the client: with
                        // an ack channel the error travels back to write_batch
                        // (and from there to the ingest response); only a
                        // caller that vanished leaves logging as the fallback
                        let ret = writer.consume_processed(batch, fsync).await;
                        match ack {
                            Some(ack) => {
                                if let Err(ret) = ack.send(ret)
                                    && let Err(e) = ret
                                {
                                    log::error!(
                                        "[INGESTER:MEM:{idx}] writer consume batch error (ack receiver dropped): {e}"
                                    );
                                }
                            }
                            None => {
                                if let Err(e) = ret {
                                    log::error!(
                                        "[INGESTER:MEM:{idx}] writer consume batch error: {e}"
                                    );
                                }
                            }
                        }
                    }
                },
            }
            total += 1;
            if total.is_multiple_of(1000) {
                log::info!(
                    "[INGESTER:MEM:{idx}] writer queue consuming, total: {}, in queue: {}",
                    total,
                    rx.len()
                );
            }
        }
        log::info!("[INGESTER:MEM:{idx}] writer queue closed");
    }

    pub fn get_key_str(&self) -> String {
        format!("{}/{}", self.key.org_id, self.key.stream_type)
    }

    pub fn is_channel_closed(&self) -> bool {
        self.write_queue.is_closed()
    }

    // check_ttl is used to check if the memtable has expired
    pub async fn write(&self, schema: Arc<Schema>, mut entry: Entry, fsync: bool) -> Result<()> {
        if entry.data.is_empty() {
            return Ok(());
        }

        entry.schema = Some(schema);
        self.write_batch(vec![entry], fsync).await
    }

    pub async fn write_batch(&self, entries: Vec<Entry>, fsync: bool) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        // Pre-process data BEFORE sending to queue
        // This moves CPU-intensive work (JSON to Arrow conversion) out of the consume loop,
        // allowing consume to focus purely on IO operations
        let processed_batch = self.preprocess_batch(entries)?;

        let cfg = get_config();
        if !cfg.common.wal_write_queue_enabled {
            return self.consume_processed(processed_batch, fsync).await;
        }

        self.enqueue_and_wait(processed_batch, fsync).await
    }

    /// Queue-mode write: hand the batch to the consumer and wait for the
    /// write's real outcome. Returning at enqueue would silently downgrade the
    /// ack-after-durable-write invariant to ack-after-enqueue -- the client
    /// would get a 200 for a batch a consumer failure then only logged. The
    /// queue still smooths bursts across requests; each request just does not
    /// count as done until its own write is.
    async fn enqueue_and_wait(&self, batch: crate::ProcessedBatch, fsync: bool) -> Result<()> {
        let (ack_tx, ack_rx) = oneshot::channel();
        let item = (WriterSignal::Produce, batch, fsync, Some(ack_tx));
        if get_config().common.wal_write_queue_full_reject {
            if let Err(e) = self.write_queue.try_send(item) {
                log::error!(
                    "[INGESTER:MEM:{}] write queue full, reject write: {}",
                    self.idx,
                    e
                );
                return Err(Error::WalError {
                    source: wal::Error::WriteQueueFull { idx: self.idx },
                });
            }
        } else {
            self.write_queue.send(item).await.map_err(|e| {
                Error::ExternalError {
                    source: Box::new(std::io::Error::other(format!(
                        "[INGESTER:MEM:{}] writer queue send failed, the write was not performed: {e}",
                        self.idx
                    ))),
                }
            })?;
        }
        match ack_rx.await {
            Ok(ret) => ret,
            // the consumer dropped the ack without reporting (panic or
            // shutdown mid-write): the write may or may not have happened, so
            // it must not be acked as success
            Err(_) => Err(Error::ExternalError {
                source: Box::new(std::io::Error::other(format!(
                    "[INGESTER:MEM:{}] writer queue dropped the write before reporting completion, \
                     the write may not be durable",
                    self.idx
                ))),
            }),
        }
    }

    fn preprocess_batch(&self, mut entries: Vec<Entry>) -> Result<crate::ProcessedBatch> {
        let _start_preprocess_batch = Instant::now();
        // data_size == 0 is treated as an empty entry downstream
        for entry in entries.iter_mut() {
            entry.normalize_data_size();
        }

        // Bulk convert to Arrow RecordBatch
        let batch_entries = entries
            .iter()
            .map(|entry| {
                entry.into_batch(self.key.stream_type.clone(), entry.schema.clone().unwrap())
            })
            .collect::<Result<Vec<_>>>()?;

        // Serialize entries to bytes for WAL writing, reusing the RecordBatch
        // in Arrow IPC format instead of serializing the data back to JSON
        let bytes_entries = entries
            .iter()
            .zip(batch_entries.iter())
            .map(|(entry, batch)| entry.into_bytes_arrow(&batch.data))
            .collect::<Result<Vec<_>>>()?;

        // Calculate total sizes for rotation check: the WAL grows by the
        // serialized bytes, the memtable by the Arrow in-memory size
        let entries_wal_size = bytes_entries.iter().map(Vec::len).sum();
        let entries_arrow_size = batch_entries
            .iter()
            .map(|entry| entry.data_arrow_size)
            .sum();

        // Move entries into ProcessedBatch
        // Clear the heavy data field after conversion to avoid memory duplication
        // The JSON data is already in bytes_entries and Arrow format in batch_entries
        for entry in entries.iter_mut() {
            let _ = std::mem::take(&mut entry.data);
        }

        let start_preprocess_batch_duration = _start_preprocess_batch.elapsed();
        if start_preprocess_batch_duration.as_millis() > 100 {
            log::warn!("start_preprocess_batch_duration: {start_preprocess_batch_duration:?}");
        }
        Ok(crate::ProcessedBatch {
            entries,
            bytes_entries,
            batch_entries,
            entries_wal_size,
            entries_arrow_size,
        })
    }

    async fn consume_processed(&self, batch: crate::ProcessedBatch, fsync: bool) -> Result<()> {
        if batch.entries.is_empty() {
            return Ok(());
        }
        let _start_consume_processed = Instant::now();
        // Check rotation
        self.rotate(batch.entries_wal_size, batch.entries_arrow_size)
            .await?;

        // Write into WAL - pure IO, no CPU-intensive processing
        let start = std::time::Instant::now();
        let mut wal = self.wal.write().await;
        let wal_lock_time = start.elapsed().as_millis() as f64;
        metrics::INGEST_WAL_LOCK_TIME
            .with_label_values(&[&self.key.org_id])
            .observe(wal_lock_time);
        let _start_wal_processed = Instant::now();
        for entry in batch.bytes_entries {
            if entry.is_empty() {
                continue;
            }
            wal.write(&entry).context(WalSnafu)?;
            tokio::task::coop::consume_budget().await;
        }
        drop(wal);
        let start_wal_processed_duration = _start_wal_processed.elapsed();
        if start_wal_processed_duration.as_millis() > 100 {
            log::warn!("start_wal_processed_duration: {start_wal_processed_duration:?}");
        }

        // Write into Memtable - pure IO, no CPU-intensive processing
        let start = std::time::Instant::now();
        let mut mem = self.memtable.write().await;
        let mem_lock_time = start.elapsed().as_millis() as f64;
        metrics::INGEST_MEMTABLE_LOCK_TIME
            .with_label_values(&[&self.key.org_id])
            .observe(mem_lock_time);
        let _start_mem_processed = Instant::now();
        for (entry, batch_entry) in batch.entries.into_iter().zip(batch.batch_entries) {
            if batch_entry.data.num_rows() == 0 {
                continue;
            }
            mem.write(entry.schema.clone().unwrap(), entry, batch_entry)?;
            tokio::task::coop::consume_budget().await;
        }
        drop(mem);
        let start_mem_processed_duration = _start_mem_processed.elapsed();
        if start_mem_processed_duration.as_millis() > 100 {
            log::warn!("start_mem_processed_duration: {start_mem_processed_duration:?}");
        }

        // Check fsync
        if fsync {
            let mut wal = self.wal.write().await;
            wal.sync().context(WalSnafu)?;
            drop(wal);
        }

        let start_consume_processed_duration = _start_consume_processed.elapsed();
        if start_consume_processed_duration.as_millis() > 500 {
            log::warn!("start_consume_processed_duration: {start_consume_processed_duration:?}");
        }

        Ok(())
    }

    // rotate is used to rotate the wal and memtable if the size exceeds the threshold
    async fn rotate(&self, entry_bytes_size: usize, entry_batch_size: usize) -> Result<()> {
        if !self.check_wal_threshold(self.wal.read().await.size(), entry_bytes_size)
            && !self.check_mem_threshold(self.memtable.read().await.size(), entry_batch_size)
        {
            return Ok(());
        }

        // rotation wal
        let start = std::time::Instant::now();
        let mut wal = self.wal.write().await;
        let wal_lock_time = start.elapsed().as_millis() as f64;
        metrics::INGEST_WAL_LOCK_TIME
            .with_label_values(&[&self.key.org_id])
            .observe(wal_lock_time);
        if !self.check_wal_threshold(wal.size(), entry_bytes_size) {
            return Ok(()); // check again to avoid race condition
        }
        let cfg = get_config();
        let wal_id = self.next_seq.fetch_add(1, Ordering::SeqCst);
        let wal_dir = PathBuf::from(&cfg.common.data_wal_dir)
            .join("logs")
            .join(self.idx.to_string());
        log::info!(
            "[INGESTER:MEM] create file: {}/{}/{}/{}.wal",
            wal_dir.display(),
            self.key.org_id,
            self.key.stream_type,
            wal_id
        );
        let (new_wal, _header_size) = WalWriter::new(
            build_file_path(
                wal_dir,
                &self.key.org_id,
                &self.key.stream_type,
                wal_id.to_string(),
            ),
            cfg.limit.max_file_size_on_disk as u64,
            cfg.limit.wal_write_buffer_size,
            None,
        )
        .context(WalSnafu)?;
        // the rotated file is the only durable record of the memtable rotated
        // with it until the persist chain replaces it with parquet, so it is
        // fsynced here whatever ZO_WAL_FSYNC_DISABLED says. The fsync runs on
        // a blocking thread: the wal lock stays held (nothing appends
        // meanwhile, the swap still happens strictly after the bytes are
        // durable), but the async worker parks on an await instead of
        // stalling in the syscall for the whole flush.
        sync_wal_off_thread(&mut wal).await?;
        let old_wal = std::mem::replace(&mut *wal, new_wal);
        drop(wal);

        // rotation memtable
        let new_mem = MemTable::new();
        let start = std::time::Instant::now();
        let mut mem = self.memtable.write().await;
        let mem_lock_time = start.elapsed().as_millis() as f64;
        metrics::INGEST_MEMTABLE_LOCK_TIME
            .with_label_values(&[&self.key.org_id])
            .observe(mem_lock_time);
        let old_mem = std::mem::replace(&mut *mem, new_mem);
        drop(mem);

        // update created_at
        self.created_at
            .store(Utc::now().timestamp_micros(), Ordering::Release);

        let path = old_wal.path().clone();
        let path_str = path.display().to_string();
        let table = Arc::new(Immutable::new(self.idx, self.key.clone(), old_mem));
        log::info!("[INGESTER:MEM] start add to IMMUTABLES, file: {path_str}");
        IMMUTABLES.write().await.insert(path, table);
        log::info!("[INGESTER:MEM] dones add to IMMUTABLES, file: {path_str}");

        Ok(())
    }

    pub async fn flush(&self) -> Result<()> {
        // wait for all messages to be processed
        if let Err(e) = self
            .write_queue
            .send((
                WriterSignal::Close,
                crate::ProcessedBatch::empty(),
                true,
                None,
            ))
            .await
        {
            log::error!("[INGESTER:MEM:{}] close writer error: {}", self.idx, e);
        }
        self.write_queue.closed().await;
        log::info!("[INGESTER:MEM:{}] writer queue closed", self.idx);

        // rotation wal: same as rotate(), the memtable handed to IMMUTABLES
        // below is only recoverable from this file until it is persisted
        let mut wal = self.wal.write().await;
        sync_wal_off_thread(&mut wal).await?;
        let path = wal.path().clone();
        drop(wal);

        // rotation memtable
        let mut mem = self.memtable.write().await;
        let new_mem = MemTable::new();
        let old_mem = std::mem::replace(&mut *mem, new_mem);
        drop(mem);

        let table = Arc::new(Immutable::new(self.idx, self.key.clone(), old_mem));
        IMMUTABLES.write().await.insert(path, table);
        Ok(())
    }

    pub async fn read(
        &self,
        org_id: &str,
        stream_name: &str,
        time_range: Option<(i64, i64)>,
        partition_filters: &[(String, Vec<String>)],
    ) -> Result<(u64, Vec<ReadRecordBatchEntry>)> {
        let memtable = self.memtable.read().await;
        memtable.read(org_id, stream_name, time_range, partition_filters)
    }

    /// Check if the wal file size is over the threshold or the file is too old
    fn check_wal_threshold(&self, written_size: (usize, usize), data_size: usize) -> bool {
        let cfg = get_config();
        let (compressed_size, uncompressed_size) = written_size;
        compressed_size > wal::FILE_TYPE_IDENTIFIER_LEN
            && (compressed_size + data_size > cfg.limit.max_file_size_on_disk
                || uncompressed_size + data_size > cfg.limit.max_file_size_on_disk
                || self.created_at.load(Ordering::Relaxed)
                    + Duration::try_seconds(cfg.limit.max_file_retention_time as i64)
                        .unwrap()
                        .num_microseconds()
                        .unwrap()
                    <= Utc::now().timestamp_micros())
    }

    /// Check if the memtable size is over the threshold
    fn check_mem_threshold(&self, written_size: (usize, usize), data_size: usize) -> bool {
        let cfg = get_config();
        let (json_size, arrow_size) = written_size;
        json_size > 0
            && (json_size + data_size > cfg.limit.max_file_size_in_memory
                || arrow_size + data_size > cfg.limit.max_file_size_in_memory)
    }
}

/// Durable-sync a wal file without stalling the async worker: the buffered
/// bytes are flushed inline (cheap, page-cache only), then the blocking
/// `fsync` runs on the blocking pool via a cloned handle to the same open
/// file. The caller MUST keep holding the wal write lock across the await --
/// that is what guarantees no byte is appended between the flush and the
/// fsync, so the durability point is exactly where an inline `sync_all` would
/// put it. On failure the writer stays marked unsynced and nothing was
/// swapped, so a retry repeats the whole step.
async fn sync_wal_off_thread(wal: &mut WalWriter) -> Result<()> {
    let wal_file = wal.sync_all_split().context(WalSnafu)?;
    let path = wal.path().clone();
    tokio::task::spawn_blocking(move || wal_file.sync_all())
        .await
        .context(TokioJoinSnafu)?
        .context(WriteFileSnafu { path })?;
    wal.confirm_synced();
    Ok(())
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub(crate) struct WriterKey {
    pub(crate) org_id: Arc<str>,
    pub(crate) stream_type: Arc<str>,
}

impl WriterKey {
    pub(crate) fn new<T>(bucket_idx: usize, org_id: T, stream_type: T) -> Self
    where
        T: AsRef<str>,
    {
        let org_id = if get_config().common.feature_shared_memtable_enabled {
            Arc::from(format!("shared_org_{bucket_idx}"))
        } else {
            Arc::from(org_id.as_ref())
        };
        Self {
            org_id,
            stream_type: Arc::from(stream_type.as_ref()),
        }
    }

    pub(crate) fn new_replay(org_id: &str, stream_type: &str) -> Self {
        Self {
            org_id: Arc::from(org_id),
            stream_type: Arc::from(stream_type),
        }
    }
}

impl MemorySize for WriterKey {
    fn mem_size(&self) -> usize {
        std::mem::size_of::<WriterKey>() + self.org_id.len() + self.stream_type.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("oo-ingester-writer-{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[tokio::test]
    async fn test_sync_wal_off_thread_makes_the_bytes_durable() {
        let dir = test_dir("sync-durable");
        let path = dir.join("1.wal");
        let p: &std::path::Path = path.as_path();
        let (mut wal, _) = WalWriter::new(p, 0, 4096, None).unwrap();
        wal.write(b"rotating soon").unwrap();

        sync_wal_off_thread(&mut wal).await.unwrap();
        let (written, _) = wal.size();
        assert!(std::fs::metadata(&path).unwrap().len() >= written as u64);

        // repeatable, and the writer keeps accepting writes afterwards
        wal.write(b"more").unwrap();
        sync_wal_off_thread(&mut wal).await.unwrap();

        drop(wal);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_sync_wal_off_thread_runs_the_fsync_on_the_blocking_pool() {
        // single async worker, single blocking thread: if the fsync ran inline
        // on the worker the first poll below would complete it; queued behind
        // an occupied blocking pool it must come back Pending instead
        let rt = tokio::runtime::Builder::new_current_thread()
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let dir = test_dir("sync-off-thread");
            let path = dir.join("2.wal");
            let p: &std::path::Path = path.as_path();
            let (mut wal, _) = WalWriter::new(p, 0, 4096, None).unwrap();
            wal.write(b"rotating soon").unwrap();

            // occupy the only blocking thread until released
            let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
            let blocker = tokio::task::spawn_blocking(move || {
                release_rx.recv().unwrap();
            });

            {
                let mut sync_fut = std::pin::pin!(sync_wal_off_thread(&mut wal));
                let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
                assert!(
                    std::future::Future::poll(sync_fut.as_mut(), &mut cx).is_pending(),
                    "the fsync completed on the async worker: it is not running on the blocking pool"
                );

                release_tx.send(()).unwrap();
                blocker.await.unwrap();
                sync_fut.await.unwrap();
            }

            drop(wal);
            let _ = std::fs::remove_dir_all(&dir);
        });
    }

    #[tokio::test]
    async fn test_enqueue_and_wait_reports_the_consumer_result() {
        let writer = Writer::new(0, WriterKey::new_replay("queue-ack-org", "logs"));
        let wal_path = writer.wal.read().await.path().clone();

        // the ack arrives only after the consumer ran the write; an empty
        // batch completes with Ok, and that Ok travels back to the caller
        writer
            .enqueue_and_wait(crate::ProcessedBatch::empty(), false)
            .await
            .unwrap();

        let _ = std::fs::remove_file(&wal_path);
    }

    #[tokio::test]
    async fn test_enqueue_and_wait_errors_when_the_queue_is_gone() {
        let writer = Writer::new(0, WriterKey::new_replay("queue-ack-closed", "logs"));
        let wal_path = writer.wal.read().await.path().clone();

        // shut the consumer down, then try to enqueue: the caller must get an
        // error, never a silent Ok for a write nobody performed
        writer
            .write_queue
            .send((
                WriterSignal::Close,
                crate::ProcessedBatch::empty(),
                false,
                None,
            ))
            .await
            .unwrap();
        writer.write_queue.closed().await;

        let err = writer
            .enqueue_and_wait(crate::ProcessedBatch::empty(), false)
            .await;
        assert!(err.is_err());

        let _ = std::fs::remove_file(&wal_path);
    }

    #[test]
    fn test_writer_key_new_replay_sets_fields() {
        let key = WriterKey::new_replay("myorg", "logs");
        assert_eq!(key.org_id.as_ref(), "myorg");
        assert_eq!(key.stream_type.as_ref(), "logs");
    }

    #[test]
    fn test_writer_key_mem_size_at_least_struct_size() {
        let key = WriterKey::new_replay("org", "metrics");
        assert!(key.mem_size() >= std::mem::size_of::<WriterKey>());
    }

    #[test]
    fn test_writer_key_mem_size_includes_string_lengths() {
        let key = WriterKey::new_replay("abc", "xyz");
        let min_expected = std::mem::size_of::<WriterKey>() + "abc".len() + "xyz".len();
        assert_eq!(key.mem_size(), min_expected);
    }
}
