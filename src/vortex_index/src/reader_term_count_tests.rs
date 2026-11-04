// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use std::sync::LazyLock;

use futures::future::BoxFuture;

use super::*;

struct Source {
    bytes: Bytes,
    reads: Mutex<Vec<Range<u64>>>,
    resident_from: u64,
}

impl VixRangeSource for Source {
    fn len(&self) -> u64 {
        self.bytes.len() as u64
    }

    fn fetch(&self, range: Range<u64>) -> BoxFuture<'static, anyhow::Result<Bytes>> {
        self.reads.lock().push(range.clone());
        let bytes = self.bytes.slice(range.start as usize..range.end as usize);
        Box::pin(async move { Ok(bytes) })
    }

    fn resident(&self, range: Range<u64>) -> Option<Bytes> {
        (range.start >= self.resident_from && range.end <= self.len())
            .then(|| self.bytes.slice(range.start as usize..range.end as usize))
    }
}

static FIXTURE: LazyLock<(Bytes, Bytes)> = LazyLock::new(|| {
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let values: Vec<String> = (0..2048)
        .flat_map(|i| std::iter::repeat_n(format!("v{i:04}"), i % 7 + 1))
        .collect();
    let rows = values.len();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|i| (rows - i) as i64),
            )),
            Arc::new(StringArray::from_iter_values(values)),
        ],
    )
    .unwrap();
    let mut writer = crate::VixWriter::new(
        &schema,
        crate::VixWriterOptions {
            postings_chunk_bytes: 512,
            encode_threads: 1,
            ..Default::default()
        },
        false,
    );
    writer
        .push_batch_with_source(&batch, &StringArray::from(vec!["{}"; rows]), None)
        .unwrap();
    let (data, index) = writer.finish().unwrap();
    (data.into(), index.unwrap().into())
});

fn ranged_blob(blob: &mut Option<BlobHandle>, resident: bool) -> Arc<Source> {
    let bytes = blob.as_ref().unwrap().bytes().unwrap();
    let source = Arc::new(Source {
        resident_from: if resident { 0 } else { bytes.len() as u64 },
        bytes,
        reads: Mutex::new(Vec::new()),
    });
    *blob = Some(BlobHandle::Ranged(crate::source::RangedBlob::new(
        source.clone(),
        0..source.len(),
    )));
    source
}

fn open(legacy: bool, resident_terms: bool) -> (VixReader, Arc<Source>, Arc<Source>) {
    open_with_blocks(legacy, resident_terms, false)
}

fn open_with_blocks(
    legacy: bool,
    resident_terms: bool,
    resident_blocks: bool,
) -> (VixReader, Arc<Source>, Arc<Source>) {
    let index = if legacy {
        crate::test_support::strip_property_for_tests(
            &FIXTURE.1,
            crate::container::PROP_DICT_FIELD_PAGES,
        )
        .unwrap()
        .into()
    } else {
        FIXTURE.1.clone()
    };
    let mut reader = VixReader::open_with_index(FIXTURE.0.clone(), Some(index)).unwrap();
    let blocks = ranged_blob(&mut reader.dict_blocks_blob, resident_blocks);
    let terms = ranged_blob(&mut reader.terms_blob, resident_terms);
    (reader, blocks, terms)
}

fn exact(token: &str) -> VixQuery {
    VixQuery::Exact {
        field: "value".into(),
        token: token.as_bytes().to_vec(),
    }
}

// The baseline's point-leaf path, kept only as a differential oracle.
fn eager_count(reader: &VixReader, query: &VixQuery) -> u64 {
    eager_count_result(reader, query).unwrap()
}

fn eager_count_result(reader: &VixReader, query: &VixQuery) -> Result<u64> {
    let mut prefetch = reader.begin_prefetch();
    let (fid, key) = match query {
        VixQuery::Exact { field, token } => {
            let fid = reader.require_field_id(field)?;
            (fid, reader.composite(token, fid))
        }
        VixQuery::KeyExists { path } => (
            KEY_FIELD_ID,
            reader.composite(path.as_bytes(), KEY_FIELD_ID),
        ),
        _ => panic!("oracle accepts point leaves only"),
    };
    if let Some(prefetch) = prefetch.as_mut() {
        reader.prefetch_field_bundle(
            prefetch,
            &[(fid, key.as_slice())],
            Some(PREFETCH_LEAF_CONCURRENCY),
        )?;
    }
    let ordinals = reader.collect_ordinals(query, None)?;
    match ordinals.value.as_slice() {
        [] => Ok(0),
        [ordinal] => reader.read_doc_count(*ordinal),
        _ => panic!("point resolved to multiple ordinals"),
    }
}

#[test]
fn resident_missing_point_count_never_opens_or_reads_terms() {
    for legacy in [false, true] {
        for resident in [false, true] {
            for query in [
                exact("missing"),
                VixQuery::KeyExists {
                    path: "missing".into(),
                },
            ] {
                let (reader, _, terms) = open_with_blocks(legacy, resident, true);
                assert_eq!(reader.count(&query).unwrap(), 0);
                assert!(
                    terms.reads.lock().is_empty(),
                    "missing point fetched terms: {:?}",
                    terms.reads.lock()
                );
                let BlobHandle::Ranged(terms) = reader.terms_blob.as_ref().unwrap() else {
                    unreachable!()
                };
                assert!(
                    !terms.footer_cached(),
                    "missing point opened the native terms footer"
                );
            }
        }
    }
}

#[test]
fn cold_points_and_resident_hits_preserve_eager_read_ranges() {
    for legacy in [false, true] {
        for resident_terms in [false, true] {
            for resident_blocks in [false, true] {
                for query in [exact("v0000"), exact("v1023"), exact("v2047")] {
                    let (reader, blocks, terms) =
                        open_with_blocks(legacy, resident_terms, resident_blocks);
                    let (eager, eager_blocks, eager_terms) =
                        open_with_blocks(legacy, resident_terms, resident_blocks);
                    assert_eq!(reader.count(&query).unwrap(), eager_count(&eager, &query));
                    assert_eq!(*blocks.reads.lock(), *eager_blocks.reads.lock());
                    assert_eq!(*terms.reads.lock(), *eager_terms.reads.lock());
                }
            }
            // A cold in-range miss must keep key + terms in the same bundle.
            let query = exact("v1023-missing");
            let (reader, blocks, terms) = open(legacy, resident_terms);
            let (eager, eager_blocks, eager_terms) = open(legacy, resident_terms);
            assert_eq!(reader.count(&query).unwrap(), eager_count(&eager, &query));
            assert_eq!(*blocks.reads.lock(), *eager_blocks.reads.lock());
            assert_eq!(*terms.reads.lock(), *eager_terms.reads.lock());
        }
    }
}

#[test]
fn cached_missing_point_skips_terms_without_fetching_another_block() {
    for legacy in [false, true] {
        let (reader, blocks, terms) = open(legacy, false);
        let fid = reader.require_field_id("value").unwrap();
        let key = reader.composite(b"v1023-missing", fid);
        assert_eq!(reader.lookup_exact(&key).unwrap(), None);
        let before = blocks.reads.lock().clone();
        assert_eq!(reader.count(&exact("v1023-missing")).unwrap(), 0);
        assert_eq!(*blocks.reads.lock(), before);
        assert!(terms.reads.lock().is_empty());
        let BlobHandle::Ranged(terms) = reader.terms_blob.as_ref().unwrap() else {
            unreachable!()
        };
        assert!(!terms.footer_cached());
    }
}

#[test]
fn resident_point_probe_keeps_memory_refusal_and_cancellation() {
    struct Operation {
        limit: usize,
        cancelled: bool,
    }
    impl crate::source::VixReadOperation for Operation {
        fn is_cancelled(&self) -> bool {
            self.cancelled
        }
        fn check_memory(&self, bytes: usize) -> Result<()> {
            if bytes > self.limit {
                Err(VixError::Callback(anyhow::anyhow!("probe budget refused")))
            } else {
                Ok(())
            }
        }
    }
    let (reader, blocks, terms) = open_with_blocks(false, false, true);
    reader
        .field_index(reader.require_field_id("value").unwrap())
        .unwrap();
    for cancelled in [false, true] {
        let operation = Arc::new(Operation {
            limit: reader.memory_size(),
            cancelled,
        });
        let error =
            crate::source::with_read_operation(operation, || reader.count(&exact("v1023-missing")))
                .unwrap_err();
        if cancelled {
            assert!(matches!(
                error.downcast_ref::<VixError>(),
                Some(VixError::Cancelled)
            ));
        } else {
            assert!(error.to_string().contains("probe budget refused"));
        }
        assert!(blocks.reads.lock().is_empty());
        assert!(terms.reads.lock().is_empty());
    }
    assert_eq!(reader.count(&exact("v1023-missing")).unwrap(), 0);
}

#[test]
fn resident_point_probe_does_not_treat_malformed_keys_as_missing() {
    let (mut reader, blocks, terms) = open_with_blocks(false, false, true);
    let key = reader.composite(b"v1023-missing", reader.require_field_id("value").unwrap());
    let index = reader
        .field_index(reader.require_field_id("value").unwrap())
        .unwrap();
    let candidate = index.predecessor_block(&key).unwrap().unwrap();
    let range = index.block_range(candidate, reader.dict_blocks_len().unwrap());
    let mut bytes = blocks.bytes.to_vec();
    // First key length exceeds the candidate block, which must remain an error.
    bytes[range.start as usize..range.start as usize + 2].copy_from_slice(&u16::MAX.to_le_bytes());
    drop(index);
    reader.dict_blocks_blob = Some(BlobHandle::Ranged(crate::source::RangedBlob::new(
        Arc::new(Source {
            bytes: bytes.into(),
            reads: Mutex::new(Vec::new()),
            resident_from: 0,
        }),
        0..blocks.len(),
    )));
    let error = reader.count(&exact("v1023-missing")).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<VixError>(),
        Some(VixError::Malformed(_))
    ));
    assert!(terms.reads.lock().is_empty());
}

#[test]
fn point_counts_match_eager_and_bitmap_on_legacy_and_current_layouts() {
    for legacy in [false, true] {
        for resident in [false, true] {
            for query in [
                exact("missing"),
                exact("v0000"),
                exact("v1023"),
                exact("v2047"),
                VixQuery::KeyExists {
                    path: "value".into(),
                },
            ] {
                let (reader, ..) = open(legacy, resident);
                let (eager, ..) = open(legacy, resident);
                let expected = eager_count(&eager, &query);
                assert_eq!(reader.count(&query).unwrap(), expected);
                assert_eq!(
                    reader.eval(&query).unwrap().count_set_bits() as u64,
                    expected
                );
                assert_eq!(reader.count(&query).unwrap(), expected, "reused reader");
            }
        }
    }
}

#[test]
fn batch_counts_cross_leaves_and_preserve_duplicates_and_empty_inputs() {
    let query = VixQuery::Or(vec![
        exact("v0000"),
        exact("v1023"),
        exact("v2047"),
        exact("v1023"),
        exact("missing"),
    ]);
    for legacy in [false, true] {
        for resident in [false, true] {
            let (reader, ..) = open(legacy, resident);
            assert_eq!(reader.count(&VixQuery::Or(vec![])).unwrap(), 0);
            let expected = reader.eval(&query).unwrap().count_set_bits() as u64;
            let (cold, ..) = open(legacy, resident);
            assert_eq!(cold.count(&query).unwrap(), expected);
            assert_eq!(cold.count(&query).unwrap(), expected);
            assert_eq!(
                cold.count(&VixQuery::Or(vec![exact("missing"), exact("absent")]))
                    .unwrap(),
                0
            );
        }
    }
}

#[test]
fn count_keeps_field_coverage_errors_and_partial_metadata() {
    let (mut reader, ..) = open(false, false);
    let query = VixQuery::Exact {
        field: "not_indexed".into(),
        token: b"missing".to_vec(),
    };
    assert!(
        matches!(reader.count(&query).unwrap_err().downcast_ref::<VixError>(), Some(VixError::FieldNotIndexed(field)) if field == "not_indexed")
    );
    reader.partial_fields.insert("value".into());
    reader.oversize_skips.insert("value".into(), 7);
    assert_eq!(reader.count(&exact("missing")).unwrap(), 0);
    assert!(reader.partial_fields().contains("value"));
    assert_eq!(reader.field_oversize_skips("value"), 7);
    assert!(reader.field_value_counts("value").unwrap().is_none());
}

#[test]
fn point_count_does_not_change_timestamp_window_boundaries() {
    let (reader, ..) = open(false, false);
    let query = exact("v0001");
    let hits = reader.eval(&query).unwrap();
    assert_eq!(reader.count(&query).unwrap(), 2);
    let times = reader.read_column("_timestamp").unwrap();
    let times = times.as_any().downcast_ref::<Int64Array>().unwrap();
    let rows: Vec<_> = hits.set_indices().collect();
    let lo = times.value(rows[1]);
    let hi = times.value(rows[0]);
    for (lo, hi) in [(lo, hi), (lo, hi + 1), (hi, hi), (hi + 1, hi + 2)] {
        let window = reader.timestamp_range(lo, hi).unwrap();
        let expected = rows
            .iter()
            .filter(|&&row| times.value(row) >= lo && times.value(row) < hi)
            .count();
        assert_eq!(
            rows.iter().filter(|&&row| window.value(row)).count(),
            expected
        );
    }
}

#[test]
fn count_leaf_planning_is_bounded_and_reusable_across_spans() {
    let (reader, ..) = open(false, true);
    let BlobHandle::Ranged(terms) = reader.terms_blob.as_ref().unwrap() else {
        unreachable!()
    };
    let mut prefetch = reader.begin_prefetch().unwrap();
    assert!(
        reader
            .plan_leaf_windows(&mut prefetch, terms, &[], None)
            .unwrap()
            .is_empty()
    );
    assert!(prefetch.leaves.is_none());
    let leaves =
        crate::container::blob_column_leaves(reader.terms_blob.as_ref().unwrap(), "doc_count")
            .unwrap()
            .unwrap();
    assert!(leaves.len() > 1, "fixture must cross count leaves");
    // Use a cold data source but retain the native footer loaded above.
    let (reader, ..) = open(false, false);
    crate::container::blob_arrow_schema_owned(reader.terms_blob.as_ref().unwrap()).unwrap();
    let BlobHandle::Ranged(terms) = reader.terms_blob.as_ref().unwrap() else {
        unreachable!()
    };
    let mut prefetch = reader.begin_prefetch().unwrap();
    let spans = [leaves[0].rows.clone(), leaves.last().unwrap().rows.clone()];
    let first = reader
        .plan_leaf_windows(&mut prefetch, terms, &spans[..1], Some(1))
        .unwrap();
    assert!(first.len() <= 1);
    let all = reader
        .plan_leaf_windows(&mut prefetch, terms, &spans, None)
        .unwrap();
    assert!(
        all.iter().map(|range| range.end - range.start).sum::<u64>()
            <= PREFETCH_DOC_COUNT_MAX_BYTES
    );
    for window in &all {
        let bytes =
            Bytes::copy_from_slice(&terms.source.resident(window.clone()).unwrap_or_else(|| {
                crate::source::block_fetch(terms.source.as_ref(), window.clone()).unwrap()
            }));
        reader
            .retain_prefetched(&mut prefetch, window.clone(), bytes)
            .unwrap();
    }
    assert!(
        reader
            .plan_leaf_windows(&mut prefetch, terms, &spans, None)
            .unwrap()
            .is_empty()
    );
}

#[derive(Clone, Copy, Debug)]
enum BlockResidence {
    Mem,
    Tail,
    Cache,
    Cold,
}

fn set_block_residence(reader: &mut VixReader, mode: BlockResidence) -> Option<Arc<Source>> {
    match mode {
        BlockResidence::Mem => None,
        BlockResidence::Tail => Some(ranged_blob(&mut reader.dict_blocks_blob, true)),
        BlockResidence::Cache | BlockResidence::Cold => {
            let source = ranged_blob(&mut reader.dict_blocks_blob, false);
            if matches!(mode, BlockResidence::Cache) {
                let index = reader.dict_index().unwrap();
                for block in 0..index.block_count() {
                    reader.dict_block(index, block).unwrap();
                }
                source.reads.lock().clear();
            }
            Some(source)
        }
    }
}

fn synthetic_point_reader(
    legacy: bool,
    mode: BlockResidence,
    blocks: Bytes,
    directory: crate::dict_blocks::IndexBuilder,
    term_count: u64,
) -> (VixReader, Option<Arc<Source>>, Arc<Source>) {
    let mut reader =
        VixReader::open_with_index(FIXTURE.0.clone(), Some(FIXTURE.1.clone())).unwrap();
    let (dict, pages) = directory.finish_with_pages();
    reader.dict_blob = Some(BlobHandle::Mem(dict.into()));
    reader.dict_field_pages = (!legacy).then_some(pages);
    reader.dict_blocks_blob = Some(BlobHandle::Mem(blocks));
    reader.term_count = term_count;
    let source = set_block_residence(&mut reader, mode);
    let terms = ranged_blob(&mut reader.terms_blob, false);
    (reader, source, terms)
}

#[test]
fn resident_point_probe_rejects_zero_and_one_byte_keys() {
    for legacy in [false, true] {
        for mode in [
            BlockResidence::Mem,
            BlockResidence::Tail,
            BlockResidence::Cache,
        ] {
            for malformed in [vec![], vec![0xff]] {
                for later in [false, true] {
                    let make = |mode| {
                        let (reader, ..) = open(legacy, false);
                        let fid = reader.require_field_id("value").unwrap();
                        let first = reader.composite(b"v0000", fid);
                        let mut block = crate::dict_blocks::BlockBuilder::new();
                        if later {
                            block.push(&first).unwrap();
                        }
                        block.push(&malformed).unwrap();
                        let mut directory = crate::dict_blocks::IndexBuilder::new();
                        directory.push_block(&first, 0, 0).unwrap();
                        synthetic_point_reader(
                            legacy,
                            mode,
                            block.finish().into(),
                            directory,
                            1 + u64::from(later),
                        )
                    };
                    let query = exact("v1023-missing");
                    let (eager, ..) = make(BlockResidence::Mem);
                    assert!(matches!(
                        eager_count_result(&eager, &query),
                        Err(VixError::Malformed(_))
                    ));
                    let (reader, blocks, terms) = make(mode);
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        reader.count(&query)
                    }));
                    assert!(
                        result.is_ok(),
                        "panic: {legacy} {mode:?} {malformed:?} {later}"
                    );
                    let result = result.unwrap();
                    assert!(
                        matches!(
                            result
                                .as_ref()
                                .err()
                                .and_then(|e| e.downcast_ref::<VixError>()),
                            Some(VixError::Malformed(_))
                        ),
                        "expected Malformed: {legacy} {mode:?} {malformed:?} {later}: {result:?}"
                    );
                    assert!(blocks.is_none_or(|source| source.reads.lock().is_empty()));
                    assert!(terms.reads.lock().is_empty());
                }
            }
        }
    }
}

#[test]
fn resident_point_probe_rejects_invalid_ranges_before_slicing() {
    for legacy in [false, true] {
        for mode in [
            BlockResidence::Mem,
            BlockResidence::Tail,
            BlockResidence::Cold,
        ] {
            for offsets in [vec![5], vec![0, 5], vec![u64::MAX]] {
                let make = |mode| {
                    let (reader, ..) = open(legacy, false);
                    let fid = reader.require_field_id("value").unwrap();
                    let mut directory = crate::dict_blocks::IndexBuilder::new();
                    for (ordinal, &offset) in offsets.iter().enumerate() {
                        let key =
                            reader.composite(if ordinal == 0 { b"v0000" } else { b"v2000" }, fid);
                        directory.push_block(&key, offset, ordinal as u64).unwrap();
                    }
                    synthetic_point_reader(
                        legacy,
                        mode,
                        Bytes::from_static(&[0; 4]),
                        directory,
                        offsets.len() as u64,
                    )
                };
                // Mem blocks retain the baseline's checked point planner;
                // ranged terms still activate the optimized count probe.
                let (eager, ..) = make(BlockResidence::Mem);
                let query = exact("v0000");
                assert!(matches!(
                    eager_count_result(&eager, &query),
                    Err(VixError::Malformed(_))
                ));
                let (reader, blocks, terms) = make(mode);
                let result =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| reader.count(&query)));
                assert!(result.is_ok(), "panic: {legacy} {mode:?} {offsets:?}");
                let result = result.unwrap();
                assert!(
                    matches!(
                        result
                            .as_ref()
                            .err()
                            .and_then(|e| e.downcast_ref::<VixError>()),
                        Some(VixError::Malformed(_))
                    ),
                    "expected Malformed: {legacy} {mode:?} {offsets:?}: {result:?}"
                );
                assert!(blocks.is_none_or(|source| source.reads.lock().is_empty()));
                assert!(terms.reads.lock().is_empty());
            }
        }
    }
}

static MULTIBLOCK_FIXTURE: LazyLock<(Bytes, Bytes)> = LazyLock::new(|| {
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let rows = 1200;
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values((1..=rows).map(i64::from))),
            Arc::new(StringArray::from_iter_values(
                (0..rows).map(|i| format!("v{i:04}{}", "x".repeat(1024))),
            )),
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
        .push_batch_with_source(&batch, &StringArray::from(vec!["{}"; rows as usize]), None)
        .unwrap();
    let (data, index) = writer.finish().unwrap();
    (data.into(), index.unwrap().into())
});

#[test]
fn resident_point_probe_matches_eval_across_dictionary_restart_boundaries() {
    for legacy in [false, true] {
        let make = |mode| {
            let index = if legacy {
                crate::test_support::strip_property_for_tests(
                    &MULTIBLOCK_FIXTURE.1,
                    crate::container::PROP_DICT_FIELD_PAGES,
                )
                .unwrap()
                .into()
            } else {
                MULTIBLOCK_FIXTURE.1.clone()
            };
            let mut reader =
                VixReader::open_with_index(MULTIBLOCK_FIXTURE.0.clone(), Some(index)).unwrap();
            let blocks = set_block_residence(&mut reader, mode);
            let terms = ranged_blob(&mut reader.terms_blob, false);
            (reader, blocks, terms)
        };
        let (oracle, ..) = make(BlockResidence::Mem);
        let fid = oracle.require_field_id("value").unwrap();
        let index = oracle.field_index(fid).unwrap();
        let mut boundaries = Vec::new();
        for block in index.field_blocks() {
            let bytes = oracle.dict_block(&index, block).unwrap();
            let mut keys = Vec::new();
            crate::dict_blocks::block_scan(&bytes, |_, key| {
                let (token, field) = split_key(key).unwrap();
                if field == fid {
                    keys.push(token.to_vec());
                }
                true
            })
            .unwrap();
            if !keys.is_empty() {
                boundaries.push((keys.first().unwrap().clone(), keys.last().unwrap().clone()));
            }
        }
        assert!(boundaries.len() > crate::dict_blocks::INDEX_RESTART_INTERVAL);
        let mut cases = Vec::new();
        for (first, last) in &boundaries {
            cases.push((first.clone(), 1));
            cases.push((last.clone(), 1));
        }
        for pair in boundaries.windows(2) {
            let mut missing = pair[0].1.clone();
            missing.push(0);
            assert!(missing < pair[1].0);
            cases.push((missing, 0));
        }
        cases.push((Vec::new(), 0));
        cases.push((vec![0xff], 0));
        for mode in [
            BlockResidence::Mem,
            BlockResidence::Tail,
            BlockResidence::Cache,
        ] {
            let (reader, blocks, terms) = make(mode);
            let (eager, eager_blocks, eager_terms) = make(mode);
            for (token, expected) in &cases {
                let query = VixQuery::Exact {
                    field: "value".into(),
                    token: token.clone(),
                };
                let block_before = blocks.as_ref().map(|s| s.reads.lock().len()).unwrap_or(0);
                let terms_before = terms.reads.lock().len();
                let eager_block_before = eager_blocks
                    .as_ref()
                    .map(|s| s.reads.lock().len())
                    .unwrap_or(0);
                let eager_terms_before = eager_terms.reads.lock().len();
                assert_eq!(
                    reader.count(&query).unwrap(),
                    *expected,
                    "{legacy} {mode:?}"
                );
                assert_eq!(eager_count(&eager, &query), *expected);
                assert_eq!(
                    oracle.eval(&query).unwrap().count_set_bits() as u64,
                    *expected
                );
                if *expected > 0 {
                    assert_eq!(
                        blocks
                            .as_ref()
                            .map(|s| s.reads.lock()[block_before..].to_vec()),
                        eager_blocks
                            .as_ref()
                            .map(|s| s.reads.lock()[eager_block_before..].to_vec()),
                    );
                    assert_eq!(
                        terms.reads.lock()[terms_before..],
                        eager_terms.reads.lock()[eager_terms_before..]
                    );
                }
            }
        }
        eprintln!(
            "multiblock legacy={legacy} blocks={} cases={} residence_modes=3",
            boundaries.len(),
            cases.len()
        );
    }
}

#[test]
fn resident_point_probe_preserves_point_scan_stop_boundary() {
    for legacy in [false, true] {
        for mode in [
            BlockResidence::Mem,
            BlockResidence::Tail,
            BlockResidence::Cache,
        ] {
            for token in ["v0500", "v1000"] {
                let make = || {
                    let (reader, ..) = open(legacy, false);
                    let fid = reader.require_field_id("value").unwrap();
                    let mut block = crate::dict_blocks::BlockBuilder::new();
                    block.push(&reader.composite(b"v1000", fid)).unwrap();
                    block.push(&[0xff]).unwrap();
                    let mut directory = crate::dict_blocks::IndexBuilder::new();
                    directory
                        .push_block(&reader.composite(b"v0000", fid), 0, 0)
                        .unwrap();
                    synthetic_point_reader(legacy, mode, block.finish().into(), directory, 2)
                };
                let (reader, ..) = make();
                let (eager, ..) = make();
                let query = exact(token);
                assert_eq!(reader.count(&query).unwrap(), eager_count(&eager, &query));
            }
        }
    }
}
