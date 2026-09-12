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

/// Keep the writer's real 64-row zone/stats axis, but store each 1024-row
/// projection chunk in one native leaf. This is the coalesced-merge shape:
/// multiple pruning holes can share a physical leaf.
fn fragmented_zone_data() -> Bytes {
    let rows = 4096;
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("duration", DataType::Int64, false),
        Field::new("gate", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|row| 10_000 - row),
            )),
            Arc::new(Int64Array::from_iter_values(0..rows)),
            Arc::new(StringArray::from_iter_values(
                (0..rows).map(|row| if (row / 64) % 4 == 1 { "a" } else { "z" }),
            )),
        ],
    )
    .unwrap();
    let mut writer = VixWriter::new(
        &schema,
        VixWriterOptions {
            docs_chunk_max_rows: 64,
            encode_threads: 1,
            ..Default::default()
        },
        false,
    );
    writer
        .push_batch_with_source(&batch, &StringArray::from(vec!["{}"; rows as usize]), None)
        .unwrap();
    let (data, _) = writer.finish().unwrap();
    let parsed = container::parse_container(&Bytes::from(data)).unwrap();
    let coarse: Vec<_> = (0..rows as usize)
        .step_by(1024)
        .map(|offset| batch.slice(offset, 1024))
        .collect();
    let native =
        container::write_vortex_blob(&schema, &coarse, container::addressable_strategy(), 1)
            .unwrap();
    let mut properties: Vec<_> = parsed.properties.into_iter().collect();
    // Keep every projected leaf outside the eager Puffin tail.
    properties.push(("padding".into(), "x".repeat(128 * 1024)));
    container::build_container(
        properties,
        vec![
            (container::BLOB_TYPE_DOCS, container::BLOB_TAG_DOCS, native),
            (
                container::BLOB_TYPE_STATS,
                container::BLOB_TAG_STATS,
                parsed.stats.unwrap().bytes().unwrap().to_vec(),
            ),
        ],
    )
    .unwrap()
    .into()
}

#[test]
fn fragmented_zone_projection_reads_shared_leaves_once() {
    use crate::docs::{BoundValue, ColumnBound};

    let data = fragmented_zone_data();
    let blob_start = blob_range(&data, container::BLOB_TAG_DOCS).start;
    let source = LoggedSource::new(data);
    let docs = crate::VixDocs::open_ranged(source.clone()).unwrap();
    // A non-equality string bound isolates zone pruning from the separate
    // dictionary equality prepass; string bounds are not native row filters.
    let gate = ColumnBound {
        column: "gate".into(),
        min: Some((BoundValue::Str("m".into()), true)),
        max: None,
    };
    let ranges = docs
        .pruned_scan_ranges(None, std::slice::from_ref(&gate))
        .unwrap();
    let leaves = docs.column_leaf_extents("duration").unwrap();
    assert_eq!(docs.zone_chunks().unwrap().len(), 64);
    assert_eq!(leaves.len(), 4, "fixture must have coarse projected leaves");
    assert!(ranges.len() > leaves.len());
    let expected: Vec<i64> = (0..4096).filter(|row| (row / 64) % 4 != 1).collect();
    let projection = ["duration".to_string(), "duration".to_string()];
    source.ranges.lock().clear();
    let mut got = Vec::new();
    docs.scan_docs_opts(
        Some(&projection),
        None,
        None,
        &[gate.clone()],
        None,
        0,
        &mut |batch| {
            assert_eq!(batch.num_columns(), 2);
            assert_eq!(batch.column(0), batch.column(1));
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            got.extend(values.values().iter().copied());
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(got, expected, "holes must not be widened or rows reordered");
    let reads = source.ranges();
    for (offset, len) in leaves {
        let leaf = blob_start + offset..blob_start + offset + len;
        // The footer suffix cache may already cover part of a data leaf.
        // Any remaining bytes fetched by this scan must not overlap.
        let mut fetched: Vec<_> = reads
            .iter()
            .filter_map(|read| {
                let start = read.start.max(leaf.start);
                let end = read.end.min(leaf.end);
                (start < end).then_some(start..end)
            })
            .collect();
        fetched.sort_unstable_by_key(|range| range.start);
        assert!(
            fetched.windows(2).all(|pair| pair[0].end <= pair[1].start),
            "projected leaf {leaf:?} must not be reread across fragmented zones; reads={reads:?}"
        );
    }

    // The supported global limit crosses several holes. Numeric/timestamp
    // filtering is exercised separately: native filter+limit is unsupported.
    got.clear();
    docs.scan_docs_opts(
        Some(&projection[..1]),
        None,
        None,
        &[gate.clone()],
        Some(200),
        0,
        &mut |batch| {
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            got.extend(values.values().iter().copied());
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(got, expected[..200]);
    got.clear();
    let numeric = ColumnBound {
        column: "duration".into(),
        min: Some((BoundValue::I64(117), false)),
        max: Some((BoundValue::I64(2100), true)),
    };
    docs.scan_docs_opts(
        Some(&projection[..1]),
        None,
        Some((8000, 9900)),
        &[gate.clone(), numeric],
        None,
        0,
        &mut |batch| {
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            got.extend(values.values().iter().copied());
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(
        got,
        expected
            .iter()
            .copied()
            .filter(|row| *row > 117 && *row <= 2000)
            .collect::<Vec<_>>()
    );

    // Refuse one admission only. Falling back after a real reservation error
    // would then succeed, incorrectly hiding the caller's budget failure.
    struct RejectGrowth {
        baseline: AtomicUsize,
        rejected: AtomicBool,
    }
    impl VixReadOperation for RejectGrowth {
        fn is_cancelled(&self) -> bool {
            false
        }

        fn check_memory(&self, bytes: usize) -> std::result::Result<(), crate::VixError> {
            let baseline = self.baseline.fetch_min(bytes, Ordering::AcqRel);
            if bytes > baseline && !self.rejected.swap(true, Ordering::AcqRel) {
                return Err(crate::VixError::Callback(VisitorStopped.into()));
            }
            Ok(())
        }
    }
    let operation = Arc::new(RejectGrowth {
        baseline: AtomicUsize::new(usize::MAX),
        rejected: AtomicBool::new(false),
    });
    let reads_before = source.ranges();
    let error = with_read_operation(operation.clone(), || {
        docs.scan_docs_opts(
            Some(&projection[..1]),
            None,
            None,
            &[gate.clone()],
            None,
            0,
            &mut |_| panic!("refused selection admission must not deliver rows"),
        )
    })
    .unwrap_err();
    assert!(operation.rejected.load(Ordering::Acquire));
    assert!(error.chain().any(|cause| cause.is::<VisitorStopped>()));
    assert_eq!(source.ranges(), reads_before);

    let error = docs
        .scan_docs_opts(
            Some(&projection[..1]),
            None,
            None,
            &[gate.clone()],
            None,
            0,
            &mut |_| Err(VisitorStopped.into()),
        )
        .unwrap_err();
    assert!(error.chain().any(|cause| cause.is::<VisitorStopped>()));
    let operation = Arc::new(Operation(AtomicBool::new(false)));
    let mut calls = 0;
    let error = with_read_operation(operation.clone(), || {
        docs.scan_docs_opts(
            Some(&projection[..1]),
            None,
            None,
            &[gate.clone()],
            None,
            0,
            &mut |_| {
                calls += 1;
                operation.0.store(true, Ordering::Release);
                Ok(())
            },
        )
    })
    .unwrap_err();
    assert!(error.chain().any(|cause| matches!(
        cause.downcast_ref::<crate::VixError>(),
        Some(crate::VixError::Cancelled)
    )));
    assert_eq!(calls, 1);
    got.clear();
    docs.scan_docs_opts(
        Some(&projection[..1]),
        None,
        None,
        &[gate],
        None,
        0,
        &mut |batch| {
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            got.extend(values.values().iter().copied());
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(
        got, expected,
        "aborts must leave no stale selection or cancellation"
    );
}

/// Constant native chunks let the public VixDocs API exercise its u64 row
/// domain without constructing an Arrow array or row-id vector of that size.
fn large_offset_zone_data(gap: u64) -> Bytes {
    use vortex::{
        VortexSessionDefault,
        array::{
            IntoArray,
            arrays::{ConstantArray, StructArray},
            validity::Validity,
        },
        file::VortexWriteOptions,
        io::{
            runtime::{BlockingRuntime, single::SingleThreadRuntime},
            session::RuntimeSessionExt,
        },
        scalar::Scalar,
        session::VortexSession,
    };

    let chunks = [(u64::from(u32::MAX) + 18, 0i64), (4, 11), (gap, 0), (4, 22)];
    let arrays: Vec<_> = chunks
        .iter()
        .map(|(rows, marker)| {
            let rows = usize::try_from(*rows).unwrap();
            StructArray::try_new(
                ["_timestamp", "marker"].into_iter().collect(),
                vec![
                    ConstantArray::new(Scalar::from(if *marker == 0 { 0i64 } else { 10i64 }), rows)
                        .into_array(),
                    ConstantArray::new(Scalar::from(*marker), rows).into_array(),
                ],
                rows,
                Validity::NonNullable,
            )
            .unwrap()
            .into_array()
        })
        .collect();
    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let mut native = Vec::new();
    let mut writer = VortexWriteOptions::new(session)
        .with_strategy(container::addressable_strategy())
        .blocking(&runtime)
        .writer(&mut native, arrays[0].dtype().clone());
    for array in arrays {
        writer.push(array).unwrap();
    }
    writer.finish().unwrap();
    let zones: Vec<_> = chunks
        .iter()
        .map(|(rows, marker)| {
            let ts = if *marker == 0 { 0i64 } else { 10i64 };
            (*rows, ts, ts)
        })
        .collect();
    container::build_container(
        vec![
            ("version".into(), "3".into()),
            (
                "row_count".into(),
                chunks.iter().map(|(rows, _)| rows).sum::<u64>().to_string(),
            ),
            ("row_order".into(), "concat".into()),
            ("columns".into(), "[\"_timestamp\",\"marker\"]".into()),
            ("zone_map".into(), serde_json::to_string(&zones).unwrap()),
        ],
        vec![(container::BLOB_TYPE_DOCS, container::BLOB_TAG_DOCS, native)],
    )
    .unwrap()
    .into()
}

#[test]
fn fragmented_zone_sparse_u64_and_huge_gap_stay_bounded() {
    // The first case makes the sparse included side cheaper than expanding
    // gap containers; the second must retain the streaming guard fallback.
    // Both start above 2^32 and would lose the selected rows if narrowed.
    for gap in [1 << 20, 1 << 40] {
        let docs = crate::VixDocs::open(large_offset_zone_data(gap)).unwrap();
        let first = u64::from(u32::MAX) + 18;
        assert_eq!(
            docs.pruned_scan_ranges(Some((10, 11)), &[]),
            Some(vec![first..first + 4, first + 4 + gap..first + 8 + gap]),
        );
        let mut got = Vec::new();
        docs.scan_docs(
            Some(&["marker".to_string()]),
            None,
            Some((10, 11)),
            &mut |batch| {
                let values = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                got.extend(values.values().iter().copied());
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(got, [11, 11, 11, 11, 22, 22, 22, 22]);
    }
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

#[test]
fn point_projection_preserves_alignment_nulls_and_column_order() {
    let data = changing_dictionary_data(3);
    let rows = [1, 2, 4, 5, 8, 9, 11];
    for ranged in [false, true] {
        let reader = if ranged {
            VixReader::open_ranged(LoggedSource::new(data.clone())).unwrap()
        } else {
            VixReader::open(data.clone()).unwrap()
        };
        // Reverse stored order, crossing dictionary boundaries and both
        // null-code and null-dictionary-value rows.
        let batch = reader
            .read_docs_columns_rows(&["group", "_timestamp"], &rows)
            .unwrap();
        assert_eq!(batch.schema().field(0).name(), "group");
        assert_eq!(batch.schema().field(1).name(), "_timestamp");
        let groups = arrow::compute::cast(batch.column(0), &DataType::Utf8).unwrap();
        let groups = groups.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(
            groups.iter().collect::<Vec<_>>(),
            vec![
                Some(""),
                None,
                Some("beta"),
                None,
                Some("gamma"),
                Some("alpha"),
                Some("gamma")
            ]
        );
        let timestamps = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(
            timestamps.values().as_ref(),
            rows.map(|row| 100 - row as i64)
        );
        let empty = reader
            .read_docs_columns_rows(&["group", "_timestamp"], &[])
            .unwrap();
        assert_eq!(empty.num_rows(), 0);
        assert_eq!(empty.schema(), batch.schema());
        // The older API still normalizes unordered duplicate rows.
        let legacy = reader
            .read_docs_column_rows("_timestamp", &[9, 1, 9])
            .unwrap();
        let legacy = legacy.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(legacy.values().as_ref(), &[99, 91]);
    }
}

#[test]
fn point_projection_rejects_invalid_selection_and_cancelled_operations() {
    let source = LoggedSource::new(changing_dictionary_data(3));
    let reader = VixReader::open_ranged(source.clone()).unwrap();
    for rows in [&[1, 1][..], &[2, 1], &[12]] {
        assert!(reader.read_docs_columns_rows(&["group"], rows).is_err());
    }
    assert!(
        reader
            .read_docs_columns_rows(&["group"], &(0..65_537).collect::<Vec<_>>())
            .is_err()
    );
    assert!(matches!(
        reader.read_docs_columns_rows(&[], &[]),
        Err(crate::VixError::InvalidQuery(_))
    ));
    assert!(matches!(
        reader.read_docs_columns_rows(&["group", "group"], &[1]),
        Err(crate::VixError::InvalidQuery(_))
    ));
    assert!(matches!(
        reader.read_docs_columns_rows(&["group", "missing"], &[]),
        Err(crate::VixError::ColumnNotFound(_))
    ));
    let before = source.ranges();
    let cancelled = Arc::new(Operation(AtomicBool::new(true)));
    let error = with_read_operation(cancelled, || {
        reader.read_docs_columns_rows(&["group", "_timestamp"], &[0, 4])
    })
    .unwrap_err();
    assert!(matches!(error, crate::VixError::Cancelled));
    assert_eq!(source.ranges(), before);
    let batch = reader
        .read_docs_columns_rows(&["_timestamp"], &[0, 4])
        .unwrap();
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.values().as_ref(), &[100, 96]);
}

#[test]
fn point_projection_accepts_full_bound_and_rejects_incomplete_docs() {
    let rows = 65_536;
    let schema = Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("_source", DataType::Utf8, false),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int64Array::from_iter_values(0..rows)),
            Arc::new(StringArray::from(vec!["{}"; rows as usize])),
        ],
    )
    .unwrap();
    let blob =
        container::write_vortex_blob(&schema, &[batch], container::addressable_strategy(), 1)
            .unwrap();
    let build = |declared_rows: i64| {
        Bytes::from(
            container::build_container(
                vec![
                    ("version".to_string(), "3".to_string()),
                    ("row_count".to_string(), declared_rows.to_string()),
                    ("columns".to_string(), "[\"_timestamp\"]".to_string()),
                ],
                vec![(
                    container::BLOB_TYPE_DOCS,
                    container::BLOB_TAG_DOCS,
                    blob.clone(),
                )],
            )
            .unwrap(),
        )
    };
    let reader = VixReader::open(build(rows)).unwrap();
    let row_ids: Vec<u64> = (0..rows as u64).collect();
    let projected = reader
        .read_docs_columns_rows(&["_timestamp"], &row_ids)
        .unwrap();
    let values = projected
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.values().as_ref(), &(0..rows).collect::<Vec<_>>());
    // The container advertises one more row than the native docs actually
    // contain. Point reads must not return a short, misaligned success.
    let corrupt = VixReader::open(build(rows + 1)).unwrap();
    assert!(
        corrupt
            .read_docs_columns_rows(&["_timestamp"], &[rows as u64 - 1, rows as u64])
            .is_err()
    );
}

#[test]
fn exact_in_duplicate_source_keys_keeps_union_semantics() {
    // Full-presence, nonnullable scalar docs do NOT prove term disjointness:
    // the source-driven writer accepts duplicate JSON keys and does not give
    // the stored column precedence over source-derived terms.
    let schema = Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("group", DataType::Utf8, false),
    ]);
    let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
    writer
        .push_docs_rows(
            &Int64Array::from(vec![3, 2, 1]),
            &[(
                "group".to_string(),
                Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
            )],
            &StringArray::from(vec![
                r#"{"group":"a","group":"b"}"#,
                r#"{"group":"b"}"#,
                r#"{"group":"c"}"#,
            ]),
            None,
        )
        .unwrap();
    let (data, index) = writer.finish().unwrap();
    for proof in [None, Some("false"), Some("TRUE"), Some("not-a-proof")] {
        let index = repack_with_properties(index.clone().unwrap(), |properties| {
            properties.retain(|(name, _)| name != crate::reader::RAW_VALUE_TERMS_DISJOINT_PROPERTY);
            if let Some(proof) = proof {
                properties.push((
                    crate::reader::RAW_VALUE_TERMS_DISJOINT_PROPERTY.to_string(),
                    proof.to_string(),
                ));
            }
        });
        let reader = open_built(data.clone(), Some(index));
        let query = VixQuery::Or(vec![exact("group", "a"), exact("group", "b")]);
        assert_eq!(reader.eval(&query).unwrap().count_set_bits(), 2);
        assert_eq!(reader.count(&query).unwrap(), 2);
        let duplicate = VixQuery::Or(vec![
            exact("group", "b"),
            exact("group", "a"),
            exact("group", "b"),
            exact("group", "missing"),
        ]);
        assert_eq!(reader.count(&duplicate).unwrap(), 2);
        assert_eq!(reader.count(&prefix(Some("group"), "")).unwrap(), 3);
    }
}

#[test]
fn exact_in_counts_match_bitmap_for_typed_literals_and_overlapping_shapes() {
    let reader = build_docs_dataset(false);
    let queries = [
        VixQuery::Or(vec![
            exact("svc", "api"),
            exact("svc", "auth"),
            exact("svc", "api"),
            exact("svc", "missing"),
        ]),
        VixQuery::Or(vec![
            exact_numeric("code", "1"),
            exact_numeric("code", "2"),
            exact_numeric("code", "1"),
        ]),
        // Mixed-field predicates overlap on documents and must never sum.
        VixQuery::Or(vec![exact("svc", "api"), exact("level", "error")]),
        // FTS tokens overlap even within one field.
        VixQuery::Or(vec![any_token("error"), any_token("db")]),
        VixQuery::Or(vec![
            exact("svc", "api"),
            VixQuery::Or(vec![exact("svc", "api"), exact("svc", "auth")]),
        ]),
    ];
    for query in queries {
        assert_eq!(
            reader.count(&query).unwrap(),
            reader.eval(&query).unwrap().count_set_bits() as u64,
            "{query:?}"
        );
    }
    assert!(
        reader
            .count(&VixQuery::Or(vec![
                exact("log", "error"),
                exact("log", "db")
            ]))
            .is_err()
    );
}

#[test]
fn unproven_exact_in_one_ordinal_preserves_postings_corruption() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("group", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![3, 2, 1])),
            Arc::new(StringArray::from(vec!["a", "a", "b"])),
        ],
    )
    .unwrap();
    let mut writer = VixWriter::new(
        &schema,
        VixWriterOptions {
            postings_plist_min_docs: 1,
            ..Default::default()
        },
        false,
    );
    writer
        .push_batch_with_source(&batch, &StringArray::from(vec!["{}"; 3]), None)
        .unwrap();
    let (data, index) = writer.finish().unwrap();
    let mut index = repack_with_properties(index.unwrap(), |properties| {
        properties.retain(|(name, _)| name != crate::reader::RAW_VALUE_TERMS_DISJOINT_PROPERTY);
    });
    // Keep the dictionary and doc_count intact, but make the pointed-to
    // postings records unreadable. A metadata-only count would hide this.
    let plist = crate::test_support::blob_byte_range(&index, "plist").unwrap();
    index[plist].fill(0xFF);
    let reader = open_built(data, Some(index));
    let query = VixQuery::Or(vec![exact("group", "a"), exact("group", "missing")]);
    let bitmap_error = reader.eval(&query).unwrap_err();
    let count_error = reader.count(&query).unwrap_err();
    match (
        bitmap_error.downcast_ref::<crate::VixError>(),
        count_error.downcast_ref::<crate::VixError>(),
    ) {
        (Some(crate::VixError::Malformed(bitmap)), Some(crate::VixError::Malformed(count))) => {
            assert_eq!(count, bitmap);
        }
        errors => panic!("expected matching postings corruption errors, got {errors:?}"),
    }
}

#[test]
fn certified_exact_in_avoids_ranged_postings_but_legacy_keeps_union() {
    let rows = 131_072usize;
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("group", DataType::Utf8, false),
    ]));
    let mut rng = StdRng::seed_from_u64(0x1_c0_017);
    let groups: Vec<String> = (0..rows)
        .map(|_| format!("g{:04}", rng.random_range(0..2048)))
        .collect();
    let expected = groups
        .iter()
        .filter(|value| matches!(value.as_str(), "g0000" | "g0001"))
        .count();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values((1..=rows as i64).rev())),
            Arc::new(StringArray::from_iter_values(groups)),
        ],
    )
    .unwrap();
    let mut writer = VixWriter::new(
        &schema,
        VixWriterOptions {
            postings_plist_min_docs: 8,
            ..Default::default()
        },
        false,
    );
    writer
        .push_batch_with_source(&batch, &StringArray::from(vec!["{}"; rows]), None)
        .unwrap();
    let (data, index) = writer.finish().unwrap();
    let index = index.unwrap();
    let query = VixQuery::Or(vec![
        exact("group", "g0001"),
        exact("group", "g0000"),
        exact("group", "g0001"),
        exact("group", "missing"),
    ]);
    for certified in [true, false] {
        let index = if certified {
            index.clone()
        } else {
            repack_with_properties(index.clone(), |properties| {
                properties
                    .retain(|(name, _)| name != crate::reader::RAW_VALUE_TERMS_DISJOINT_PROPERTY);
            })
        };
        let index = Bytes::from(index);
        let plist = blob_range(&index, "plist");
        // The query targets early terms; enough plist lies outside the eager
        // tail to distinguish metadata-only counts from real postings IO.
        assert!(plist.end - plist.start > 256 * 1024);
        let data_source = LoggedSource::new(Bytes::from(data.clone()));
        let index_source = LoggedSource::new(index);
        let reader =
            VixReader::open_ranged_with_index(data_source.clone(), Some(index_source.clone()))
                .unwrap();
        data_source.ranges.lock().clear();
        index_source.ranges.lock().clear();
        assert_eq!(reader.count(&query).unwrap(), expected as u64);
        let touched_plist = index_source
            .ranges()
            .iter()
            .any(|range| range.start < plist.end && range.end > plist.start);
        assert_eq!(touched_plist, !certified);
        assert!(data_source.ranges().is_empty());
        assert_eq!(reader.eval(&query).unwrap().count_set_bits(), expected);
        assert!(
            index_source
                .ranges()
                .iter()
                .any(|range| { range.start < plist.end && range.end > plist.start })
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
