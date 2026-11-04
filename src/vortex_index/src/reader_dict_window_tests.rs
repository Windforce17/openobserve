// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use std::sync::atomic::AtomicBool;

use arrow::datatypes::Schema;
use futures::{FutureExt, future::BoxFuture};

use super::*;
use crate::source::{RangedBlob, VixReadOperation, with_read_operation};

struct WindowSource {
    bytes: Bytes,
    reads: Mutex<Vec<Range<u64>>>,
    cancel: AtomicBool,
}

impl VixRangeSource for WindowSource {
    fn len(&self) -> u64 {
        self.bytes.len() as u64
    }

    fn fetch(&self, range: Range<u64>) -> BoxFuture<'static, anyhow::Result<Bytes>> {
        self.reads.lock().push(range.clone());
        let bytes = Bytes::copy_from_slice(&self.bytes[range.start as usize..range.end as usize]);
        futures::future::ready(Ok(bytes)).boxed()
    }
}

fn fixture(count: usize, block_bytes: usize) -> (VixReader, Arc<WindowSource>, Vec<Vec<u8>>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["x"])),
        ],
    )
    .unwrap();
    let mut writer = crate::VixWriter::new(
        &schema,
        crate::VixWriterOptions {
            encode_threads: 1,
            ..Default::default()
        },
        false,
    );
    writer
        .push_batch_with_source(&batch, &StringArray::from(vec!["{}"]), None)
        .unwrap();
    let (data, index) = writer.finish().unwrap();
    let mut reader = VixReader::open_with_index(data.into(), index.map(Bytes::from)).unwrap();
    let fid = reader.field_id("value").unwrap();
    let mut directory = crate::dict_blocks::IndexBuilder::new();
    let mut encoded = Vec::new();
    let mut keys = Vec::new();
    for b in 0..count {
        let mut key = reader.composite(format!("value/{b:08}/α").as_bytes(), fid);
        key.resize(block_bytes - 2, b'x');
        directory
            .push_block(&key, encoded.len() as u64, b as u64)
            .unwrap();
        let mut block = crate::dict_blocks::BlockBuilder::new();
        block.push(&key).unwrap();
        encoded.extend(block.finish());
        keys.push(key);
    }
    let (directory, _) = directory.finish_with_pages();
    let source = Arc::new(WindowSource {
        bytes: encoded.into(),
        reads: Mutex::new(Vec::new()),
        cancel: AtomicBool::new(false),
    });
    reader.dict_blob = Some(BlobHandle::Mem(directory.into()));
    reader.dict_index = OnceLock::new();
    reader.dict_field_pages = None;
    reader.field_indexes.lock().clear();
    reader.dict_blocks_blob = Some(BlobHandle::Ranged(RangedBlob::new(
        source.clone(),
        0..source.len(),
    )));
    reader.term_count = count as u64;
    (reader, source, keys)
}

fn walk(reader: &VixReader) -> Result<Vec<(u64, Vec<u8>)>> {
    let (lower, upper) = VixReader::v2_field_range(reader.field_id("value").unwrap());
    let mut actual = Vec::new();
    reader.scan_key_range(&lower, Some((&upper, false)), |key, ordinal| {
        actual.push((ordinal, key.to_vec()));
        true
    })?;
    Ok(actual)
}

#[test]
fn dict_window_large_walk_reads_each_block_once_cold_and_warm() {
    let count = 1536;
    let block_bytes = 20_000;
    let (reader, source, keys) = fixture(count, block_bytes);
    let expected: Vec<_> = keys
        .into_iter()
        .enumerate()
        .map(|(i, key)| (i as u64, key))
        .collect();
    for _ in 0..2 {
        source.reads.lock().clear();
        assert_eq!(walk(&reader).unwrap(), expected);
        let reads = source.reads.lock();
        let loaded: u64 = reads.iter().map(|r| r.end - r.start).sum();
        assert!(
            loaded <= (count * block_bytes) as u64,
            "loaded {loaded} bytes for {} unique bytes",
            count * block_bytes
        );
        assert!(reads.iter().all(|r| r.end - r.start <= 8 * 1024 * 1024));
        assert!(reader.block_cache.lock().0.len() <= 1024);
    }
}

#[test]
fn dict_window_stops_at_upper_bound_and_callback() {
    let (reader, source, keys) = fixture(1100, 20_000);
    let mut actual = Vec::new();
    reader
        .scan_key_range(&keys[500], Some((&keys[510], false)), |_, ordinal| {
            actual.push(ordinal);
            true
        })
        .unwrap();
    assert_eq!(actual, (500..510).collect::<Vec<_>>());
    source.reads.lock().clear();
    let mut actual = Vec::new();
    reader
        .scan_key_range(&keys[0], None, |_, ordinal| {
            actual.push(ordinal);
            false
        })
        .unwrap();
    assert_eq!(actual, vec![0]);
    assert!(
        source
            .reads
            .lock()
            .iter()
            .map(|r| r.end - r.start)
            .sum::<u64>()
            <= 8 * 1024 * 1024
    );
}

struct Budget {
    limit: usize,
    source: Arc<WindowSource>,
    cancel_after_read: bool,
}

impl VixReadOperation for Budget {
    fn is_cancelled(&self) -> bool {
        self.source.cancel.load(Ordering::Acquire)
            || (self.cancel_after_read && !self.source.reads.lock().is_empty())
    }
    fn check_memory(&self, bytes: usize) -> Result<()> {
        if bytes > self.limit {
            Err(VixError::Malformed("window budget refused".into()))
        } else {
            Ok(())
        }
    }
}

#[test]
fn dict_window_cancelled_fetch_releases_pending_memory() {
    let (reader, source, _) = fixture(1100, 20_000);
    reader
        .field_index(reader.field_id("value").unwrap())
        .unwrap();
    let before = reader.memory.total.load(Ordering::Acquire);
    let operation = Arc::new(Budget {
        limit: usize::MAX,
        source: source.clone(),
        cancel_after_read: true,
    });
    let error = with_read_operation(operation, || walk(&reader)).unwrap_err();
    assert!(matches!(error, VixError::Cancelled));
    assert_eq!(reader.memory.total.load(Ordering::Acquire), before);
    assert_eq!(walk(&reader).unwrap().len(), 1100);
}

#[test]
fn dict_window_budget_refuses_before_fetch_and_recovers() {
    let (reader, source, _) = fixture(1100, 20_000);
    reader
        .field_index(reader.field_id("value").unwrap())
        .unwrap();
    let before = reader.memory.total.load(Ordering::Acquire);
    let operation = Arc::new(Budget {
        limit: before + 1024,
        source: source.clone(),
        cancel_after_read: false,
    });
    let error = with_read_operation(operation, || walk(&reader)).unwrap_err();
    assert!(
        matches!(error, VixError::Malformed(ref message) if message == "window budget refused")
    );
    assert!(source.reads.lock().is_empty());
    assert_eq!(reader.memory.total.load(Ordering::Acquire), before);
    assert_eq!(walk(&reader).unwrap().len(), 1100);
}

#[test]
fn dict_window_small_blocks_are_capped_and_reserved_until_consumed() {
    let (reader, source, _) = fixture(1200, 64);
    let index = reader
        .field_index(reader.field_id("value").unwrap())
        .unwrap();
    let before = reader.memory.total.load(Ordering::Acquire);
    let window = reader
        .load_dict_block_span(&index, 0, 1199)
        .unwrap()
        .unwrap();
    assert_eq!(window.value.len(), 512);
    assert!(reader.block_cache.lock().0.is_empty());
    assert!(reader.memory.total.load(Ordering::Acquire) >= before + 512 * 64);
    assert_eq!(
        source
            .reads
            .lock()
            .iter()
            .map(|r| r.end - r.start)
            .sum::<u64>(),
        512 * 64
    );
    drop(window);
    assert_eq!(reader.memory.total.load(Ordering::Acquire), before);
    assert_eq!(walk(&reader).unwrap().len(), 1200);
    assert_eq!(
        reader.memory.total.load(Ordering::Acquire),
        reader.memory.size()
    );
}

#[test]
fn dict_window_cache_hit_does_not_prefetch_before_early_stop() {
    let (reader, source, keys) = fixture(1100, 20_000);
    let index = reader
        .field_index(reader.field_id("value").unwrap())
        .unwrap();
    reader.dict_block(&index, 0).unwrap();
    source.reads.lock().clear();
    let mut count = 0;
    reader
        .scan_key_range(&keys[0], None, |key, ordinal| {
            assert_eq!(key, keys[0]);
            assert_eq!(ordinal, 0);
            count += 1;
            false
        })
        .unwrap();
    assert_eq!(count, 1);
    assert!(source.reads.lock().is_empty());
}

#[test]
fn dict_window_cancel_during_consumption_releases_window() {
    let (reader, source, keys) = fixture(1100, 20_000);
    reader
        .field_index(reader.field_id("value").unwrap())
        .unwrap();
    let operation = Arc::new(Budget {
        limit: usize::MAX,
        source: source.clone(),
        cancel_after_read: false,
    });
    let mut seen = Vec::new();
    let error = with_read_operation(operation, || {
        reader.scan_key_range(&keys[0], None, |_, ordinal| {
            seen.push(ordinal);
            source.cancel.store(true, Ordering::Release);
            true
        })
    })
    .unwrap_err();
    assert!(matches!(error, VixError::Cancelled));
    assert_eq!(seen, vec![0]);
    assert_eq!(
        reader.memory.total.load(Ordering::Acquire),
        reader.memory.size()
    );
    source.cancel.store(false, Ordering::Release);
    assert_eq!(walk(&reader).unwrap().len(), 1100);
}

#[test]
fn dict_window_sparse_cache_does_not_drop_unconsumed_blocks() {
    let count = 1536;
    let block_bytes = 20_000;
    let (reader, source, keys) = fixture(count, block_bytes);
    let index = reader
        .field_index(reader.field_id("value").unwrap())
        .unwrap();
    for b in (0..count).step_by(2) {
        reader.dict_block(&index, b).unwrap();
    }
    source.reads.lock().clear();
    let expected: Vec<_> = keys
        .into_iter()
        .enumerate()
        .map(|(i, key)| (i as u64, key))
        .collect();
    assert_eq!(walk(&reader).unwrap(), expected);
    let reads = source.reads.lock();
    let loaded: u64 = reads.iter().map(|r| r.end - r.start).sum();
    assert!(loaded <= (count * block_bytes) as u64);
    for b in 0..count {
        let start = (b * block_bytes) as u64;
        let end = start + block_bytes as u64;
        let loads = reads
            .iter()
            .filter(|r| r.start <= start && r.end >= end)
            .count();
        assert!(loads <= 1, "block {b} loaded {loads} times");
    }
    assert_eq!(
        reader.memory.total.load(Ordering::Acquire),
        reader.memory.size()
    );
}
