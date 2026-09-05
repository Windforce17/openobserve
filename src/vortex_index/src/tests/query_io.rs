// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use std::{
    ops::Range,
    sync::{
        LazyLock, Weak,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use arrow::array::{DictionaryArray, UInt64Array, types::UInt64Type};
use futures::future::BoxFuture;
use parking_lot::Mutex;

use super::*;
use crate::{
    ReaderMemoryObserver, VixRangeSource, VixReadOperation,
    container::{self, BLOB_TAG_DICT, BLOB_TYPE_DICT, BlobHandle},
    with_read_operation,
};

/// Simulates a cache ladder returning a small slice of a whole cached object.
/// The backing object returned by each fetch has a separate drop witness.
struct FetchedOwner {
    bytes: Vec<u8>,
    live: Arc<AtomicUsize>,
}
impl AsRef<[u8]> for FetchedOwner {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl Drop for FetchedOwner {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}
struct LoggedSource {
    data: Bytes,
    ranges: Mutex<Vec<Range<u64>>>,
    live: Arc<AtomicUsize>,
}
impl LoggedSource {
    fn new(data: Bytes) -> Arc<Self> {
        Arc::new(Self {
            data,
            ranges: Mutex::new(Vec::new()),
            live: Arc::new(AtomicUsize::new(0)),
        })
    }
    fn ranges(&self) -> Vec<Range<u64>> {
        self.ranges.lock().clone()
    }
}
impl VixRangeSource for LoggedSource {
    fn len(&self) -> u64 {
        self.data.len() as u64
    }
    fn fetch(&self, range: Range<u64>) -> BoxFuture<'static, anyhow::Result<Bytes>> {
        self.ranges.lock().push(range.clone());
        let data = if range.start > range.end || range.end > self.len() {
            Err(anyhow::anyhow!("invalid fixture range"))
        } else {
            self.live.fetch_add(1, Ordering::SeqCst);
            let owner = Bytes::from_owner(FetchedOwner {
                bytes: self.data.to_vec(),
                live: self.live.clone(),
            });
            Ok(owner.slice(range.start as usize..range.end as usize))
        };
        Box::pin(async move { data })
    }
}
fn ranged_container(source: &Arc<LoggedSource>, tail: u64) -> container::VixContainer {
    let source: Arc<dyn VixRangeSource> = source.clone();
    container::parse_container_ranged_with_tail(&source, tail).unwrap()
}
fn blob_range(data: &Bytes, tag: &str) -> Range<u64> {
    puffin::reader::parse_puffin_footer_from_bytes(data)
        .unwrap()
        .blobs
        .iter()
        .find(|blob| {
            blob.properties
                .get("blob_tag")
                .is_some_and(|value| value == tag)
        })
        .unwrap()
        .get_offset(None)
}

#[test]
fn eager_tail_fetches_only_missing_blob_intervals() {
    let payload: Vec<u8> = (0..8192).map(|i| (i % 251) as u8).collect();
    let data = Bytes::from(
        container::build_container(
            Vec::new(),
            vec![(BLOB_TYPE_DICT, BLOB_TAG_DICT, payload.clone())],
        )
        .unwrap(),
    );
    let range = blob_range(&data, BLOB_TAG_DICT);
    let footer_bytes = data.len() as u64 - range.end;
    for covered in [0, 137, payload.len() as u64] {
        let source = LoggedSource::new(data.clone());
        let parsed = ranged_container(&source, footer_bytes + covered);
        let dict = parsed.dict.unwrap();
        let tail_start = range.end - covered;
        assert_eq!(source.ranges(), vec![tail_start..data.len() as u64]);
        assert_eq!(
            source.live.load(Ordering::SeqCst),
            0,
            "tail must detach from the whole fetched owner"
        );
        let bytes = dict.bytes().unwrap();
        assert_eq!(bytes.as_ref(), payload.as_slice());
        let expected = if covered == payload.len() as u64 {
            vec![tail_start..data.len() as u64]
        } else {
            vec![tail_start..data.len() as u64, range.start..tail_start]
        };
        assert_eq!(source.ranges(), expected);
        drop(bytes);
        assert_eq!(source.live.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn partial_tail_batched_reads_preserve_positions_and_empty_ranges() {
    let data = Bytes::from(
        container::build_container(
            Vec::new(),
            vec![(
                BLOB_TYPE_DICT,
                BLOB_TAG_DICT,
                (0..8192).map(|i| (i % 251) as u8).collect(),
            )],
        )
        .unwrap(),
    );
    let blob = blob_range(&data, BLOB_TAG_DICT);
    let source = LoggedSource::new(data.clone());
    let split = blob.end - 137;
    let parsed = ranged_container(&source, data.len() as u64 - split);
    let BlobHandle::Ranged(dict) = parsed.dict.unwrap() else {
        panic!("partial blob must remain ranged")
    };
    let requests = vec![
        blob.start..blob.start,
        split - 2..split + 3,
        split..split,
        split + 2..split + 8,
        blob.end..blob.end,
        blob.start..blob.start + 4,
    ];
    let result = crate::source::block_fetch_many(dict.source.as_ref(), requests.clone()).unwrap();
    for (actual, range) in result.iter().zip(&requests) {
        assert_eq!(
            actual.as_ref(),
            &data[range.start as usize..range.end as usize]
        );
    }
    assert_eq!(
        source.ranges(),
        vec![
            split..data.len() as u64,
            split - 2..split,
            blob.start..blob.start + 4
        ]
    );
}

#[test]
fn oversized_footer_fetches_prefix_once_and_rejects_corruption() {
    let data = Bytes::from(
        container::build_container(
            vec![("large".to_string(), "x".repeat(4096))],
            vec![(BLOB_TYPE_DICT, BLOB_TAG_DICT, vec![17; 512])],
        )
        .unwrap(),
    );
    let footer_start = blob_range(&data, BLOB_TAG_DICT).end;
    let source = LoggedSource::new(data.clone());
    let parsed = ranged_container(&source, 64);
    assert_eq!(parsed.properties["large"], "x".repeat(4096));
    assert_eq!(
        source.ranges(),
        vec![
            data.len() as u64 - 64..data.len() as u64,
            footer_start..data.len() as u64 - 64
        ]
    );
    assert_eq!(parsed.dict.unwrap().bytes().unwrap().as_ref(), &[17; 512]);
    for corrupt_length in [false, true] {
        let mut corrupt = data.to_vec();
        if corrupt_length {
            let offset = corrupt.len() - puffin::FOOTER_SIZE as usize;
            corrupt[offset..offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        } else {
            *corrupt.last_mut().unwrap() ^= 1;
        }
        let source = LoggedSource::new(Bytes::from(corrupt));
        let erased: Arc<dyn VixRangeSource> = source.clone();
        assert!(container::parse_container_ranged_with_tail(&erased, 64).is_err());
        assert_eq!(
            source.ranges(),
            vec![data.len() as u64 - 64..data.len() as u64]
        );
    }
}

/// Explicit dictionary batches guarantee changing values/code assignments;
/// the real Vortex writer stores these as separate addressable leaves.
fn changing_dictionary_data(chunk_count: usize) -> Bytes {
    let value_type = DataType::Dictionary(Box::new(DataType::UInt64), Box::new(DataType::Utf8));
    let schema = Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("group", value_type, true),
        Field::new("_source", DataType::Utf8, false),
    ]);
    let dictionaries = [
        vec![Some("alpha"), Some("")],
        vec![Some("beta"), None],
        vec![Some("gamma"), Some("alpha")],
    ];
    let batches: Vec<_> = (0..chunk_count)
        .map(|chunk| {
            let values = dictionaries[chunk % dictionaries.len()].clone();
            let codes = UInt64Array::from(vec![Some(0), Some(1), None, Some(0)]);
            let group =
                DictionaryArray::<UInt64Type>::try_new(codes, Arc::new(StringArray::from(values)))
                    .unwrap();
            RecordBatch::try_new(
                Arc::new(schema.clone()),
                vec![
                    Arc::new(Int64Array::from_iter_values(
                        (0..4).map(|row| 100 - (chunk * 4 + row) as i64),
                    )),
                    Arc::new(group),
                    Arc::new(StringArray::from(vec!["{}"; 4])),
                ],
            )
            .unwrap()
        })
        .collect();
    let blob =
        container::write_vortex_blob(&schema, &batches, container::addressable_strategy(), 1)
            .unwrap();
    Bytes::from(
        container::build_container(
            vec![
                ("version".to_string(), "3".to_string()),
                ("row_count".to_string(), (chunk_count * 4).to_string()),
                (
                    "columns".to_string(),
                    "[\"_timestamp\",\"group\"]".to_string(),
                ),
            ],
            vec![(container::BLOB_TYPE_DOCS, container::BLOB_TAG_DOCS, blob)],
        )
        .unwrap(),
    )
}

fn dict_strings(batch: &crate::DocsDictBatch) -> Vec<Option<String>> {
    let values = arrow::compute::cast(&batch.values, &DataType::Utf8).unwrap();
    let values = values.as_any().downcast_ref::<StringArray>().unwrap();
    batch
        .codes
        .iter()
        .map(|code| {
            code.and_then(|code| {
                let code = code as usize;
                (!values.is_null(code)).then(|| values.value(code).to_string())
            })
        })
        .collect()
}

#[test]
fn dictionary_visitor_preserves_clipped_rows_nulls_and_changing_codes() {
    let data = changing_dictionary_data(3);
    let expected = [
        Some(""),
        None,
        Some("alpha"),
        Some("beta"),
        None,
        None,
        Some("beta"),
        Some("gamma"),
        Some("alpha"),
        None,
    ];
    for ranged in [false, true] {
        let reader = if ranged {
            VixReader::open_ranged(LoggedSource::new(data.clone())).unwrap()
        } else {
            VixReader::open(data.clone()).unwrap()
        };
        let mut rows = Vec::new();
        let mut timestamps = Vec::new();
        reader
            .visit_docs_dict_chunks("group", 1..11, true, &mut |batch| {
                assert_eq!(batch.row_offset, 1 + rows.len() as u64);
                assert_eq!(batch.codes.len(), batch.timestamps.as_ref().unwrap().len());
                rows.extend(dict_strings(&batch));
                timestamps.extend(batch.timestamps.unwrap().values().iter().copied());
                Ok(())
            })
            .unwrap();
        assert_eq!(rows, expected.map(|value| value.map(str::to_string)));
        assert_eq!(timestamps, (1..11).map(|row| 100 - row).collect::<Vec<_>>());
        let mut without_time = Vec::new();
        reader
            .visit_docs_dict_chunks("group", 1..11, false, &mut |batch| {
                assert!(batch.timestamps.is_none());
                without_time.extend(dict_strings(&batch));
                Ok(())
            })
            .unwrap();
        assert_eq!(without_time, rows);
        reader
            .visit_docs_dict_chunks("_timestamp", 3..9, true, &mut |batch| {
                let values = batch.values.as_any().downcast_ref::<Int64Array>().unwrap();
                for (i, code) in batch.codes.values().iter().enumerate() {
                    assert_eq!(
                        values.value(*code as usize),
                        batch.timestamps.as_ref().unwrap().value(i)
                    );
                }
                Ok(())
            })
            .unwrap();
        reader
            .visit_docs_dict_chunks("group", 5..5, true, &mut |_| {
                panic!("empty range yielded rows")
            })
            .unwrap();
        assert!(
            reader
                .visit_docs_dict_chunks("group", 6..5, false, &mut |_| Ok(()))
                .is_err()
        );
        assert!(
            reader
                .visit_docs_dict_chunks("group", 0..13, false, &mut |_| Ok(()))
                .is_err()
        );
    }
}

#[derive(Debug)]
struct VisitorStopped;
impl std::fmt::Display for VisitorStopped {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("visitor stopped")
    }
}
impl std::error::Error for VisitorStopped {}
struct Operation(AtomicBool);
impl VixReadOperation for Operation {
    fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

#[test]
fn visitor_error_and_cancellation_leave_reader_reusable() {
    let source = LoggedSource::new(changing_dictionary_data(3));
    let reader = VixReader::open_ranged(source.clone()).unwrap();
    let cancelled = Arc::new(Operation(AtomicBool::new(true)));
    let before = source.ranges();
    let error = with_read_operation(cancelled, || {
        reader.visit_docs_dict_chunks("group", 0..12, false, &mut |_| {
            panic!("cancelled operation invoked visitor")
        })
    })
    .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<crate::VixError>(),
        Some(crate::VixError::Cancelled)
    ));
    assert_eq!(source.ranges(), before);
    let mut calls = 0;
    let error = reader
        .visit_docs_dict_chunks("group", 0..12, false, &mut |_| {
            calls += 1;
            Err(VisitorStopped.into())
        })
        .unwrap_err();
    assert!(error.downcast_ref::<VisitorStopped>().is_some());
    assert_eq!(calls, 1);
    let operation = Arc::new(Operation(AtomicBool::new(false)));
    calls = 0;
    let error = with_read_operation(operation.clone(), || {
        reader.visit_docs_dict_chunks("group", 0..12, false, &mut |_| {
            calls += 1;
            operation.0.store(true, Ordering::Release);
            Ok(())
        })
    })
    .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<crate::VixError>(),
        Some(crate::VixError::Cancelled)
    ));
    assert_eq!(calls, 1);
    let mut fresh_rows = Vec::new();
    reader
        .visit_docs_dict_chunks("group", 0..12, false, &mut |batch| {
            fresh_rows.extend(dict_strings(&batch));
            Ok(())
        })
        .unwrap();
    assert_eq!(
        fresh_rows,
        [
            Some("alpha"),
            Some(""),
            None,
            Some("alpha"),
            Some("beta"),
            None,
            None,
            Some("beta"),
            Some("gamma"),
            Some("alpha"),
            None,
            Some("gamma")
        ]
        .map(|v| v.map(str::to_string))
    );
}

static INDEXED: LazyLock<(Bytes, Bytes)> = LazyLock::new(|| {
    let rows = 8192;
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("id", DataType::Utf8, false),
        Field::new("code", DataType::Int64, false),
    ]));
    let mut rng = StdRng::seed_from_u64(0x517a11);
    let ids: Vec<_> = (0..rows).map(|row| format!("id-{row:05}")).collect();
    let sources: Vec<_> = (0..rows)
        .map(|_| {
            format!(
                "{{\"padding\":\"{:032x}{:032x}\"}}",
                rng.random::<u128>(),
                rng.random::<u128>()
            )
        })
        .collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|row| 1_000_000 - row as i64),
            )),
            Arc::new(StringArray::from_iter_values(ids)),
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|row| row as i64),
            )),
        ],
    )
    .unwrap();
    let mut writer = VixWriter::new(
        &schema,
        VixWriterOptions {
            docs_chunk_max_rows: 128,
            bloom_field_names: vec!["id".to_string()],
            bloom_fpp: 0.0000000001,
            ..Default::default()
        },
        false,
    );
    writer
        .push_batch_with_source(&batch, &StringArray::from_iter_values(sources), None)
        .unwrap();
    let (data, index) = writer.finish().unwrap();
    (Bytes::from(data), Bytes::from(index.unwrap()))
});

fn indexed_reader() -> (Arc<VixReader>, Arc<LoggedSource>, Arc<LoggedSource>) {
    let data = LoggedSource::new(INDEXED.0.clone());
    let index = LoggedSource::new(INDEXED.1.clone());
    let reader =
        Arc::new(VixReader::open_ranged_with_index(data.clone(), Some(index.clone())).unwrap());
    (reader, data, index)
}

#[test]
fn directory_after_large_bloom_avoids_another_directory_fetch() {
    let (_, bytes) = &*INDEXED;
    let directory = blob_range(bytes, BLOB_TAG_DICT);
    let bloom = blob_range(bytes, container::BLOB_TAG_BLOOM);
    assert_eq!(bloom.end, directory.start);
    let tail = bytes.len() as u64 - directory.start;
    assert!(
        bloom.end - bloom.start > tail,
        "fixture Bloom must exceed directory plus footer"
    );
    let source = LoggedSource::new(bytes.clone());
    let parsed = ranged_container(&source, tail);
    assert_eq!(
        parsed.dict.unwrap().bytes().unwrap().as_ref(),
        &bytes[directory.start as usize..directory.end as usize]
    );
    assert_eq!(source.ranges(), vec![directory.start..bytes.len() as u64]);
    let (reader, ..) = indexed_reader();
    let memory = VixReader::open_with_index(INDEXED.0.clone(), Some(bytes.clone())).unwrap();
    assert_eq!(eval_set(&reader, &exact("id", "id-04096")), docs(&[4096]));
    assert_eq!(reader.file_blooms().unwrap(), memory.file_blooms().unwrap());
    assert_eq!(
        reader.field_value_counts("id").unwrap(),
        memory.field_value_counts("id").unwrap()
    );
    assert_eq!(
        reader.field_value_counts("id").unwrap().unwrap(),
        (0..8192)
            .map(|row| (format!("id-{row:05}").into_bytes(), 1u64))
            .collect::<Vec<_>>()
    );
}

struct Observer {
    reader: Weak<VixReader>,
    sizes: Mutex<Vec<usize>>,
    reenter: bool,
}
impl ReaderMemoryObserver for Observer {
    fn memory_changed(&self, bytes: usize) {
        let reader = self.reader.upgrade().unwrap();
        assert!(reader.memory_size() >= bytes);
        self.sizes.lock().push(bytes);
        if self.reenter {
            let schema = reader.docs_schema().unwrap();
            assert!(schema.field_with_name("_timestamp").is_ok());
        }
    }
}
fn subscribe(reader: &Arc<VixReader>, reenter: bool) -> Arc<Observer> {
    let observer = Arc::new(Observer {
        reader: Arc::downgrade(reader),
        sizes: Mutex::new(Vec::new()),
        reenter,
    });
    let erased: Arc<dyn ReaderMemoryObserver> = observer.clone();
    reader.observe_memory(Arc::downgrade(&erased)).unwrap();
    observer
}
fn assert_growth(reader: &Arc<VixReader>, observer: &Observer, work: impl FnOnce()) {
    let before = reader.memory_size();
    work();
    let after = reader.memory_size();
    assert!(after > before, "lazy retained allocation must be accounted");
    assert_eq!(observer.sizes.lock().last().copied(), Some(after));
}

#[test]
fn committed_reader_growth_is_notified_once_per_allocation() {
    let (reader, data, index) = indexed_reader();
    let cancelled = Arc::new(Operation(AtomicBool::new(true)));
    assert!(with_read_operation(cancelled, || reader.column_chunk_stats()).is_none());
    let observer = subscribe(&reader, false);
    assert_growth(&reader, &observer, || {
        assert!(reader.term_row_group_count() > 0);
    });
    assert_growth(&reader, &observer, || {
        assert!(
            reader
                .docs_schema()
                .unwrap()
                .field_with_name("code")
                .is_ok()
        );
    });
    assert_growth(&reader, &observer, || {
        let stats = reader.column_chunk_stats().unwrap();
        assert_eq!(
            stats.columns["code"]
                .chunks
                .iter()
                .flatten()
                .map(|chunk| chunk.present)
                .sum::<u64>(),
            8192
        );
    });
    assert_growth(&reader, &observer, || {
        assert_eq!(eval_set(&reader, &exact("id", "id-04096")), docs(&[4096]));
    });
    let stable = reader.memory_size();
    let notifications = observer.sizes.lock().clone();
    let reads = (data.ranges(), index.ranges());
    reader.term_row_group_count();
    reader.docs_schema().unwrap();
    reader.column_chunk_stats().unwrap();
    assert_eq!(eval_set(&reader, &exact("id", "id-04096")), docs(&[4096]));
    assert_eq!(reader.memory_size(), stable);
    assert_eq!(*observer.sizes.lock(), notifications);
    // The repeated term evaluation may read postings again, but schema,
    // directory and stats must not refetch their metadata.
    assert_eq!(data.ranges(), reads.0);
    assert_eq!(data.live.load(Ordering::SeqCst), 0);
    assert_eq!(
        index.live.load(Ordering::SeqCst),
        0,
        "cached dictionary blocks must detach whole fetched owners"
    );
}

#[test]
fn memory_observer_can_reenter_schema_without_deadlocking() {
    LazyLock::force(&INDEXED);
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let (reader, ..) = indexed_reader();
        let observer = subscribe(&reader, true);
        let before = reader.memory_size();
        assert_eq!(eval_set(&reader, &exact("id", "id-04096")), docs(&[4096]));
        assert!(reader.memory_size() > before);
        let schema = reader.docs_schema().unwrap();
        tx.send((
            schema.field_with_name("code").unwrap().data_type().clone(),
            observer.sizes.lock().last().copied(),
            reader.memory_size(),
        ))
        .unwrap();
    });
    let (dtype, notified, actual) = rx
        .recv_timeout(Duration::from_secs(30))
        .expect("reentrant observer deadlocked");
    assert_eq!(dtype, DataType::Int64);
    assert_eq!(notified, Some(actual));
}

#[test]
fn visitor_abort_does_not_read_every_projected_chunk() {
    // More stored leaves than the bounded native worker lookahead, even on
    // large build hosts. Tiny leaves keep the regression fixture inexpensive.
    let chunks = std::thread::available_parallelism().unwrap().get() * 8 + 64;
    let data = changing_dictionary_data(chunks);
    let read = |abort: bool| {
        let source = LoggedSource::new(data.clone());
        // A deliberately small probe prevents a tiny fixture from being
        // served entirely by the container's eager tail.
        let parsed = ranged_container(&source, 64);
        let blob = parsed.docs.unwrap();
        let before = source.ranges().len();
        let mut rows = 0;
        let result = container::visit_blob_dict_chunks(
            &blob,
            "group",
            0..(chunks * 4) as u64,
            true,
            &mut |batch| {
                rows += batch.codes.len();
                if abort {
                    Err(VisitorStopped.into())
                } else {
                    Ok(())
                }
            },
        );
        if abort {
            assert!(
                matches!(&result, Err(crate::VixError::Callback(error)) if error.downcast_ref::<VisitorStopped>().is_some())
            );
            assert!(rows < chunks * 4);
        } else {
            result.unwrap();
            assert_eq!(rows, chunks * 4);
        }
        source.ranges()[before..]
            .iter()
            .map(|range| range.end - range.start)
            .sum::<u64>()
    };
    let early_bytes = read(true);
    let full_bytes = read(false);
    assert!(
        early_bytes < full_bytes,
        "aborting visitor read all projected chunks: {early_bytes}/{full_bytes}"
    );
}
