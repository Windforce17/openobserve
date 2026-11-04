// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use arrow::datatypes::Schema;

use super::*;

fn contains(field: Option<&str>, needle: &[u8], insensitive: bool) -> VixQuery {
    VixQuery::Contains {
        field: field.map(str::to_owned),
        needle: needle.to_vec(),
        case_insensitive: insensitive,
    }
}

fn fixture() -> (Bytes, Bytes) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("a", DataType::Utf8, true),
        Field::new("b", DataType::Utf8, true),
        Field::new("fts", DataType::Utf8, true),
        Field::new("number", DataType::Int64, true),
    ]));
    let oversized = "alpha".repeat(100);
    let a = vec![
        Some("alpha beta"),
        Some("βeta中文"),
        Some(""),
        None,
        Some("ALPHA"),
        Some("banana"),
        Some("overlap alpha"),
        Some(oversized.as_str()),
        Some("βeta中文"),
        Some("a\0b"),
    ];
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(1..=10)),
            Arc::new(StringArray::from(a)),
            Arc::new(StringArray::from(vec!["different beta"; 10])),
            Arc::new(StringArray::from(vec!["alpha beta"; 10])),
            Arc::new(Int64Array::from(vec![123; 10])),
        ],
    )
    .unwrap();
    let mut writer = crate::VixWriter::new(
        &schema,
        crate::VixWriterOptions {
            max_raw_term_len: 128,
            fts_field_names: vec!["fts".into()],
            encode_threads: 1,
            ..Default::default()
        },
        false,
    );
    writer
        .push_batch_with_source(&batch, &StringArray::from(vec!["{}"; 10]), None)
        .unwrap();
    let (data, index) = writer.finish().unwrap();
    (Bytes::from(data), Bytes::from(index.unwrap()))
}

// This oracle never submits a composite to the optimized evaluator.
fn unfused(reader: &VixReader, query: &VixQuery) -> Result<BooleanBuffer> {
    match query {
        VixQuery::Or(children) => {
            let mut result = BooleanBuffer::new_unset(reader.row_count as usize);
            for child in children {
                result = &result | &unfused(reader, child)?;
            }
            Ok(result)
        }
        VixQuery::And(children) => {
            let mut result = BooleanBuffer::new_set(reader.row_count as usize);
            for child in children {
                result = &result & &unfused(reader, child)?;
            }
            Ok(result)
        }
        VixQuery::Not(child) => Ok(!&unfused(reader, child)?),
        leaf => reader.eval_query(leaf, None),
    }
}

fn assert_equivalent(reader: &VixReader, query: &VixQuery) {
    let expected = unfused(reader, query).unwrap();
    assert_eq!(reader.eval(query).unwrap(), expected, "{query:?}");
    assert_eq!(
        reader.count(query).unwrap(),
        expected.count_set_bits() as u64,
        "count: {query:?}"
    );
}

#[test]
fn contains_any_matches_independent_leaf_bitmaps() {
    let (data, index) = fixture();
    let reader = VixReader::open_with_index(data, Some(index)).unwrap();
    let needles: &[&[u8]] = &[
        b"alpha",
        b"beta",
        "中文".as_bytes(),
        "β".as_bytes(),
        b"",
        b"ana",
        b"ban",
        b"a\0b",
        b"missing",
        b"\xff",
        b"ALPHA",
    ];
    for &a in needles {
        for &b in needles {
            assert_equivalent(
                &reader,
                &VixQuery::Or(vec![
                    contains(Some("a"), a, false),
                    contains(Some("a"), b, false),
                ]),
            );
        }
    }
    let query = VixQuery::Or(vec![
        contains(Some("a"), b"alpha", false),
        contains(Some("a"), b"alpha", false),
        contains(Some("a"), b"ana", false),
        contains(Some("a"), b"ban", false),
    ]);
    assert_equivalent(&reader, &query);
    assert_eq!(reader.eval(&query).unwrap().count_set_bits(), 3);
    let all_strings = VixQuery::Or(vec![
        contains(Some("a"), b"", false),
        contains(Some("a"), b"missing", false),
    ]);
    let bits = reader.eval(&all_strings).unwrap();
    assert_eq!(bits.count_set_bits(), 8);
    assert!(!bits.value(3)); // NULL
    assert!(!bits.value(7)); // Oversized raw value has no dictionary term.
    assert_eq!(reader.field_oversize_skips("a"), 1);
    assert!(reader.partial_fields().is_empty());
}

#[test]
fn contains_any_keeps_boolean_and_field_boundaries() {
    let (data, index) = fixture();
    let reader = VixReader::open_with_index(data, Some(index)).unwrap();
    let pair = VixQuery::Or(vec![
        contains(Some("a"), b"alpha", false),
        contains(Some("a"), "中文".as_bytes(), false),
    ]);
    let cases = [
        VixQuery::And(vec![pair.clone(), contains(Some("a"), b"beta", false)]),
        VixQuery::Not(Box::new(pair.clone())),
        VixQuery::Or(vec![
            pair.clone(),
            VixQuery::And(vec![
                contains(Some("a"), b"ana", false),
                contains(Some("a"), b"ban", false),
            ]),
            VixQuery::Not(Box::new(contains(Some("b"), b"beta", false))),
        ]),
        VixQuery::Or(vec![
            contains(Some("a"), b"alpha", false),
            contains(Some("b"), b"beta", false),
            contains(Some("a"), b"ana", false),
        ]),
        VixQuery::Or(vec![
            contains(Some("a"), b"alpha", true),
            contains(Some("a"), "中文".as_bytes(), false),
        ]),
        VixQuery::Or(vec![
            contains(None, b"alpha", false),
            contains(None, b"beta", false),
        ]),
        VixQuery::Or(vec![
            contains(Some("number"), b"", false),
            contains(Some("number"), b"123", false),
        ]),
        VixQuery::Or(vec![
            contains(Some("b"), b"different", false),
            contains(Some("b"), b"beta", false),
        ]),
    ];
    for query in cases {
        assert_equivalent(&reader, &query);
    }
    let scoped = VixQuery::FullText {
        fields: vec!["fts".into()],
        query: Box::new(pair.clone()),
    };
    assert_eq!(reader.eval(&scoped).unwrap(), reader.eval(&pair).unwrap());
    for field in ["fts", "absent"] {
        let query = VixQuery::Or(vec![
            contains(Some(field), b"alpha", false),
            contains(Some(field), b"beta", false),
        ]);
        assert!(matches!(
            unfused(&reader, &query),
            Err(VixError::FieldNotIndexed(_))
        ));
        assert!(reader.eval(&query).unwrap_err().to_string().contains(field));
    }
}

#[test]
fn contains_any_preserves_legacy_partial_and_oversize_metadata() {
    let (data, index) = fixture();
    let legacy =
        crate::test_support::strip_property_for_tests(&index, "dict_field_pages_v1").unwrap();
    let partial = crate::test_support::repack_with_partial_fields(&legacy, &["a"]).unwrap();
    for (index, is_partial) in [
        (index, false),
        (Bytes::from(legacy), false),
        (Bytes::from(partial), true),
    ] {
        let reader = VixReader::open_with_index(data.clone(), Some(index)).unwrap();
        assert_eq!(reader.partial_fields().contains("a"), is_partial);
        assert_eq!(reader.field_oversize_skips("a"), 1);
        for needles in [
            vec![b"alpha".as_slice(), b"beta"],
            vec![b"", b"alpha"],
            vec![b"absent", b"missing"],
        ] {
            assert_equivalent(
                &reader,
                &VixQuery::Or(
                    needles
                        .iter()
                        .map(|needle| contains(Some("a"), needle, false))
                        .collect(),
                ),
            );
        }
        assert_eq!(reader.partial_fields().contains("a"), is_partial);
        assert_eq!(reader.field_oversize_skips("a"), 1);
    }
}

#[test]
fn contains_any_large_groups_keep_exact_results() {
    let (data, index) = fixture();
    let reader = VixReader::open_with_index(data, Some(index)).unwrap();
    for count in [1, 2, 16, 17, 64] {
        let query = VixQuery::Or(
            (0..count)
                .map(|i| contains(Some("a"), if i % 2 == 0 { b"alpha" } else { b"ana" }, false))
                .collect(),
        );
        assert_equivalent(&reader, &query);
    }
    let query = VixQuery::Or(vec![
        contains(Some("a"), &vec![b'x'; 4097], false),
        contains(Some("a"), b"alpha", false),
    ]);
    assert_equivalent(&reader, &query);
}

thread_local! {
    static WORK: std::cell::Cell<(usize, usize)> = const { std::cell::Cell::new((0, 0)) };
}

pub(super) fn key_decoded() {
    WORK.with(|work| {
        let (keys, unions) = work.get();
        work.set((keys + 1, unions));
    });
}

pub(super) fn postings_union() {
    WORK.with(|work| {
        let (keys, unions) = work.get();
        work.set((keys, unions + 1));
    });
}

fn work(reader: &VixReader, query: &VixQuery, fused: bool) -> (BooleanBuffer, (usize, usize)) {
    WORK.with(|work| work.set((0, 0)));
    let result = if fused {
        reader.eval(query).unwrap()
    } else {
        unfused(reader, query).unwrap()
    };
    (result, WORK.with(|work| work.get()))
}

#[test]
fn contains_any_visits_keys_once_and_unions_once() {
    let (data, index) = fixture();
    let reader = VixReader::open_with_index(data, Some(index)).unwrap();
    let query = VixQuery::Or(vec![
        contains(Some("a"), b"alpha", false),
        contains(Some("a"), b"ana", false),
    ]);
    let (old, (old_keys, old_unions)) = work(&reader, &query, false);
    let (new, (new_keys, new_unions)) = work(&reader, &query, true);
    assert_eq!(old, new);
    assert!(new_keys > 0);
    assert_eq!(old_keys, new_keys * 2, "one dictionary walk per OR group");
    assert_eq!(old_unions, 2);
    assert_eq!(new_unions, 1);
}

#[test]
fn contains_any_refused_groups_keep_original_work() {
    let (data, index) = fixture();
    let reader = VixReader::open_with_index(data.clone(), Some(index.clone())).unwrap();
    let pair = vec![
        contains(Some("a"), b"alpha", false),
        contains(Some("a"), b"ana", false),
    ];
    let queries = [
        VixQuery::Not(Box::new(VixQuery::Or(pair.clone()))),
        VixQuery::Or((0..17).map(|_| pair[0].clone()).collect()),
        VixQuery::Or(vec![
            contains(Some("a"), &vec![b'x'; 4097], false),
            pair[0].clone(),
        ]),
        VixQuery::Or(vec![
            pair[0].clone(),
            contains(Some("b"), b"beta", false),
            pair[1].clone(),
        ]),
        VixQuery::Or(vec![
            contains(Some("a"), b"alpha", true),
            contains(Some("a"), b"ana", true),
        ]),
    ];
    for query in queries {
        let (old, old_work) = work(&reader, &query, false);
        let (new, new_work) = work(&reader, &query, true);
        assert_eq!(old, new);
        assert_eq!(old_work, new_work, "{query:?}");
    }
    let partial = crate::test_support::repack_with_partial_fields(&index, &["a"]).unwrap();
    let legacy_fts = crate::test_support::repack_properties(&index, |properties| {
        let entry = properties
            .iter_mut()
            .find(|(key, _)| key == "fields")
            .unwrap();
        let mut fields: serde_json::Value = serde_json::from_str(&entry.1)?;
        for field in fields.as_array_mut().unwrap() {
            if field["name"] == "a" {
                field["types"]
                    .as_array_mut()
                    .unwrap()
                    .push(serde_json::json!("fts"));
            }
        }
        entry.1 = serde_json::to_string(&fields)?;
        Ok(())
    })
    .unwrap();
    for index in [partial, legacy_fts] {
        let reader = VixReader::open_with_index(data.clone(), Some(Bytes::from(index))).unwrap();
        let query = VixQuery::Or(pair.clone());
        let (old, old_work) = work(&reader, &query, false);
        let (new, new_work) = work(&reader, &query, true);
        assert_eq!(old, new);
        assert_eq!(old_work, new_work);
    }
}

#[test]
fn contains_any_finders_and_ordinals_obey_memory_admission() {
    use std::sync::atomic::AtomicUsize;
    struct Limit {
        limit: usize,
        peak: AtomicUsize,
    }
    impl crate::VixReadOperation for Limit {
        fn is_cancelled(&self) -> bool {
            false
        }
        fn check_memory(&self, bytes: usize) -> Result<()> {
            self.peak.fetch_max(bytes, Ordering::Relaxed);
            if bytes > self.limit {
                return Err(VixError::Callback(anyhow::anyhow!(
                    "contains-any admission denied"
                )));
            }
            Ok(())
        }
    }
    let (data, index) = fixture();
    let reader = VixReader::open_with_index(data, Some(index)).unwrap();
    let leaves = vec![
        contains(Some("a"), b"alpha", false),
        contains(Some("a"), b"ana", false),
    ];
    let baseline = reader.memory.total.load(Ordering::Acquire);
    let finder_bytes = 2 * std::mem::size_of::<memchr::memmem::Finder<'_>>();
    let denied = Arc::new(Limit {
        limit: baseline + finder_bytes - 1,
        peak: AtomicUsize::new(0),
    });
    let result =
        crate::source::with_read_operation(denied.clone(), || reader.collect_contains_any(&leaves));
    assert!(matches!(result, Err(VixError::Callback(_))));
    assert_eq!(denied.peak.load(Ordering::Relaxed), baseline + finder_bytes);
    assert_eq!(reader.memory.total.load(Ordering::Acquire), baseline);

    let allowed = Arc::new(Limit {
        limit: usize::MAX,
        peak: AtomicUsize::new(0),
    });
    let ordinals = crate::source::with_read_operation(allowed.clone(), || {
        reader.collect_contains_any(&leaves)
    })
    .unwrap()
    .unwrap();
    assert_eq!(ordinals.value.len(), 3);
    let retained = ordinals.value.capacity() * std::mem::size_of::<u64>();
    assert!(allowed.peak.load(Ordering::Relaxed) >= baseline + finder_bytes + retained);
    drop(ordinals);
    assert_eq!(
        reader.memory.total.load(Ordering::Acquire),
        reader.memory.size()
    );
}

#[test]
fn contains_any_honors_cancellation() {
    struct Cancelled;
    impl crate::VixReadOperation for Cancelled {
        fn is_cancelled(&self) -> bool {
            true
        }
    }
    let (data, index) = fixture();
    let reader = VixReader::open_with_index(data, Some(index)).unwrap();
    let leaves = vec![
        contains(Some("a"), b"alpha", false),
        contains(Some("a"), b"ana", false),
    ];
    let result = crate::source::with_read_operation(Arc::new(Cancelled), || {
        reader.collect_contains_any(&leaves)
    });
    assert!(matches!(result, Err(VixError::Cancelled)));
}

#[test]
#[ignore = "requires contains-any-samples.json from the local contains_or_probe build"]
fn contains_any_local_sample_work() {
    let samples: serde_json::Value =
        serde_json::from_slice(&std::fs::read("contains-any-samples.json").unwrap()).unwrap();
    for sample in samples.as_array().unwrap() {
        let data = Bytes::from(std::fs::read(sample["data"].as_str().unwrap()).unwrap());
        let index = Bytes::from(std::fs::read(sample["index"].as_str().unwrap()).unwrap());
        let queries: serde_json::Value =
            serde_json::from_slice(&std::fs::read(sample["queries"].as_str().unwrap()).unwrap())
                .unwrap();
        for legacy in [false, true] {
            let index = if legacy {
                Bytes::from(
                    crate::test_support::strip_property_for_tests(&index, "dict_field_pages_v1")
                        .unwrap(),
                )
            } else {
                index.clone()
            };
            let reader = VixReader::open_with_index(data.clone(), Some(index)).unwrap();
            for query in queries.as_array().unwrap() {
                let leaves: Vec<_> = query["needles"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|needle| {
                        contains(
                            Some(query["field"].as_str().unwrap()),
                            needle.as_str().unwrap().as_bytes(),
                            false,
                        )
                    })
                    .collect();
                let mut old_ordinals = 0;
                for leaf in &leaves {
                    old_ordinals += reader.collect_ordinals(leaf, None).unwrap().value.len();
                }
                let new_ordinals = reader
                    .collect_contains_any(&leaves)
                    .unwrap()
                    .unwrap()
                    .value
                    .len();
                let expression = VixQuery::Or(leaves);
                let (old, (old_keys, old_unions)) = work(&reader, &expression, false);
                let (new, (new_keys, new_unions)) = work(&reader, &expression, true);
                assert_eq!(old, new, "{}", query["id"]);
                assert_eq!(
                    old_keys,
                    new_keys * query["needles"].as_array().unwrap().len()
                );
                assert_eq!(new_unions, 1);
                println!(
                    "CONTAINS_ANY_WORK {}",
                    serde_json::json!({
                        "id": query["id"], "legacy": legacy,
                        "old_keys": old_keys, "new_keys": new_keys,
                        "old_unions": old_unions, "new_unions": new_unions,
                        "old_matching_ordinals": old_ordinals, "new_matching_ordinals": new_ordinals,
                        "matched_rows": new.count_set_bits(), "equal": true,
                    })
                );
            }
        }
    }
}

#[test]
fn contains_any_dense_leaf_does_not_hide_corrupt_other_leaf() {
    use arrow::array::{BinaryArray, UInt64Array};
    let (data, index) = fixture();
    let mut reader = VixReader::open_with_index(data, Some(index)).unwrap();
    let fid = reader.field_id("a").unwrap();
    let dense = reader
        .lookup_exact(&reader.composite(b"ALPHA", fid))
        .unwrap()
        .unwrap() as usize;
    let corrupt = reader
        .lookup_exact(&reader.composite(b"banana", fid))
        .unwrap()
        .unwrap() as usize;
    let mut counts = vec![0u64; reader.term_count as usize];
    let mut cells = vec![Vec::new(); reader.term_count as usize];
    counts[dense] = reader.row_count;
    counts[corrupt] = 1;
    cells[corrupt] = postings::encode(&[reader.row_count as u32]).unwrap();
    let schema = Schema::new(vec![
        Field::new("doc_count", DataType::UInt64, false),
        Field::new("postings", DataType::Binary, false),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(UInt64Array::from(counts)),
            Arc::new(BinaryArray::from_iter_values(
                cells.iter().map(Vec::as_slice),
            )),
        ],
    )
    .unwrap();
    reader.terms_blob = Some(BlobHandle::Mem(Bytes::from(
        crate::container::write_vortex_blob(
            &schema,
            &[batch],
            crate::container::addressable_strategy(),
            1,
        )
        .unwrap(),
    )));
    let query = VixQuery::Or(vec![
        contains(Some("a"), b"ALPHA", false),
        contains(Some("a"), b"banana", false),
    ]);
    assert!(matches!(
        unfused(&reader, &query),
        Err(VixError::Malformed(_))
    ));
    assert!(matches!(
        reader.eval_query(&query, None),
        Err(VixError::Malformed(_))
    ));
}
