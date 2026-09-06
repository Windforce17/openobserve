// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::{DocIdMap, reader::RAW_VALUE_TERMS_DISJOINT_PROPERTY};

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("group", DataType::Utf8, true),
    ])
}

fn group_in() -> VixQuery {
    VixQuery::Or(vec![exact("group", "a"), exact("group", "b")])
}

fn assert_count(reader: &VixReader, query: &VixQuery, expected: u64) {
    assert_eq!(reader.count(query).unwrap(), expected);
    assert_eq!(
        reader.eval(query).unwrap().count_set_bits() as u64,
        expected
    );
}

fn source_pair(rows: &[&str], opts: VixWriterOptions) -> (Vec<u8>, Option<Vec<u8>>) {
    let mut writer = VixWriter::new(&schema(), opts, false);
    for (row, source) in rows.iter().enumerate() {
        writer
            .push_docs_rows(
                &Int64Array::from(vec![10_000 - row as i64]),
                &[],
                &StringArray::from(vec![*source]),
                None,
            )
            .unwrap();
    }
    writer.finish().unwrap()
}

#[test]
fn duplicate_incidence_stays_unproven_across_spills_and_finish_routes() {
    let dir = tempfile::tempdir().unwrap();
    for spooled in [false, true] {
        let mut writer = VixWriter::new(
            &schema(),
            VixWriterOptions {
                term_spill_dir: spooled.then(|| dir.path().to_path_buf()),
                term_spill_bytes: 1,
                output_spool_dir: spooled.then(|| dir.path().to_path_buf()),
                ..Default::default()
            },
            false,
        );
        // A third occurrence must not overwrite/re-enable the duplicate
        // latch. Later pushes and drained term shards must not reset it.
        for (row, source) in [
            r#"{"group":"a","group":"b","group":"a"}"#,
            r#"{"group":"b"}"#,
            r#"{"group":"c"}"#,
        ]
        .into_iter()
        .enumerate()
        {
            writer
                .push_docs_rows(
                    &Int64Array::from(vec![100 - row as i64]),
                    &[],
                    &StringArray::from(vec![source]),
                    None,
                )
                .unwrap();
        }
        let reader = if spooled {
            let (data, index, _) = writer.finish_output().unwrap();
            open_built(data.into_bytes().unwrap(), index)
        } else {
            let (data, index, _) = writer.finish_with_stats().unwrap();
            open_built(data, index)
        };
        assert!(!reader.has_disjoint_value_terms());
        assert_count(&reader, &group_in(), 2); // summed term counts would be 3
        assert_count(&reader, &prefix(Some("group"), ""), 3);
    }
}

#[test]
fn duplicate_columns_do_not_certify_values_that_were_never_indexed() {
    let duplicate_schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("group", DataType::Utf8, true),
        Field::new("group", DataType::Utf8, true),
    ]));
    // Value indexing selects the first column by name. Key indexing visits
    // both columns, so only one-row batches keep its duplicate IDs adjacent.
    let batch = RecordBatch::try_new(
        Arc::clone(&duplicate_schema),
        vec![
            Arc::new(Int64Array::from(vec![100, 99])),
            Arc::new(StringArray::from(vec!["a", "c"])),
            Arc::new(StringArray::from(vec!["b", "b"])),
        ],
    )
    .unwrap();
    let mut writer = VixWriter::new(&duplicate_schema, VixWriterOptions::default(), false);
    for row in 0..batch.num_rows() {
        writer
            .push_batch_with_source(&batch.slice(row, 1), &StringArray::from(vec!["{}"]), None)
            .unwrap();
    }
    // Switching to the source API advances the SAME document cursor.
    // Repeated cs names are storage-only; source terms are authoritative.
    let columns: Vec<(String, ArrayRef)> = vec![
        ("group".into(), Arc::new(StringArray::from(vec!["b"]))),
        ("group".into(), Arc::new(StringArray::from(vec!["a"]))),
    ];
    writer
        .push_docs_rows(
            &Int64Array::from(vec![98]),
            &columns,
            &StringArray::from(vec![r#"{"group":"b"}"#]),
            None,
        )
        .unwrap();
    let reader = finish_open(writer);
    assert!(reader.has_disjoint_value_terms());
    assert_count(&reader, &group_in(), 2);
    assert_eq!(eval_set(&reader, &exact("group", "b")), docs(&[2]));
}

#[test]
fn skipped_values_and_fts_do_not_revoke_raw_term_proof() {
    let schema = Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("group", DataType::Utf8, true),
        Field::new("number", DataType::Float64, true),
        Field::new("flag", DataType::Boolean, true),
        Field::new("message", DataType::Utf8, true),
    ]);
    let mut writer = VixWriter::new(
        &schema,
        VixWriterOptions {
            max_raw_term_len: 8,
            fts_field_names: vec!["message".into()],
            ..Default::default()
        },
        false,
    );
    // Only actual raw postings participate: null/oversize/unsupported
    // values and repeated FTS tokens cannot overlap two raw term sets.
    let source = StringArray::from(vec![
        r#"{"group":null,"group":"a","number":1.0,"flag":true,"message":"alpha beta alpha"}"#,
        r#"{"group":"oversized-value","group":"b","number":2.0,"flag":false,"message":"alpha beta"}"#,
        r#"{"group":[],"number":1e999,"message":"beta gamma"}"#,
        r#"{"group":""}"#,
    ]);
    writer
        .push_docs_rows(&Int64Array::from(vec![100, 99, 98, 97]), &[], &source, None)
        .unwrap();
    let reader = finish_open(writer);
    assert!(reader.has_disjoint_value_terms());
    assert_count(&reader, &group_in(), 2);
    assert_count(&reader, &exact("group", ""), 1);
    assert_count(
        &reader,
        &VixQuery::Or(vec![
            exact_numeric("number", "1.0"),
            exact_numeric("number", "2.0"),
        ]),
        2,
    );
    assert_count(
        &reader,
        &VixQuery::Or(vec![
            exact_numeric("flag", "true"),
            exact_numeric("flag", "false"),
        ]),
        2,
    );
}

#[test]
fn sidecar_rebuild_proves_only_the_terms_it_reindexes() {
    let schema = Arc::new(schema());
    let source = StringArray::from(vec![r#"{"group":"a","group":"b"}"#, r#"{"group":"c"}"#]);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![100, 99])),
            Arc::new(StringArray::from(vec!["a", "c"])),
        ],
    )
    .unwrap();
    let mut original = VixWriter::new(&schema, VixWriterOptions::default(), false);
    original
        .push_batch_with_source(&batch, &source, None)
        .unwrap();
    let (data, index) = original.finish().unwrap();
    let legacy_index = repack_with_properties(index.unwrap(), |properties| {
        properties.retain(|(key, _)| key != RAW_VALUE_TERMS_DISJOINT_PROPERTY);
    });
    let legacy = open_built(data.clone(), Some(legacy_index));
    assert!(!legacy.has_disjoint_value_terms());
    assert_count(&legacy, &group_in(), 1);

    for from_columns in [false, true] {
        let mut heal = VixWriter::new(
            &schema,
            VixWriterOptions {
                docs_passthrough: true,
                ..Default::default()
            },
            false,
        );
        if from_columns {
            heal.push_batch_with_source_index_only(&batch, &source, None)
                .unwrap();
        } else {
            heal.push_docs_rows_index_only(&Int64Array::from(vec![100, 99]), &[], &source, None)
                .unwrap();
        }
        let (sidecar, _) = heal.finish_index_sidecar(2).unwrap();
        let reader = open_built(data.clone(), Some(sidecar));
        // A real column rebuild may establish fresh proof, but source
        // reindexing must observe the duplicate even with scalar docs.
        assert_eq!(reader.has_disjoint_value_terms(), from_columns);
        assert_count(&reader, &group_in(), 1);
        assert_eq!(
            reader.count(&exact("group", "b")).unwrap(),
            u64::from(!from_columns)
        );
    }
}

fn merge_readers(readers: &[&VixReader], maps: &[DocIdMap], spooled: bool) -> VixReader {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = VixWriter::new(
        &schema(),
        VixWriterOptions {
            concat_row_order: true,
            merge_kway_threads: 2,
            term_spill_dir: spooled.then(|| dir.path().to_path_buf()),
            output_spool_dir: spooled.then(|| dir.path().to_path_buf()),
            ..Default::default()
        },
        false,
    );
    writer.merge_input_indexes(readers, maps, 2).unwrap();
    let rows = readers
        .iter()
        .map(|reader| reader.row_count() as usize)
        .sum::<usize>();
    writer
        .push_docs_rows_unindexed(
            &Int64Array::from_iter_values((1..=rows as i64).rev()),
            &[],
            &StringArray::from(vec!["{}"; rows]),
            None,
        )
        .unwrap();
    let (data, index, _) = writer.finish_output().unwrap();
    open_built(data.into_bytes().unwrap(), index)
}

#[test]
fn offset_merges_require_every_input_proof_including_encoded_copy_routes() {
    let (data, index) = source_pair(
        &[r#"{"group":"a"}"#, r#"{"group":"b"}"#],
        VixWriterOptions::default(),
    );
    let proven = open_built(data.clone(), index.clone());
    let legacy = open_built(
        data,
        Some(repack_with_properties(index.unwrap(), |properties| {
            properties.retain(|(key, _)| key != RAW_VALUE_TERMS_DISJOINT_PROPERTY);
        })),
    );
    let (data, index) = source_pair(
        &[r#"{"group":"a","group":"b"}"#, r#"{"group":"c"}"#],
        VixWriterOptions::default(),
    );
    let overlapping = open_built(data, index);
    for spooled in [false, true] {
        let copied = merge_readers(&[&proven], &[DocIdMap::Offset(0)], spooled);
        assert!(copied.has_disjoint_value_terms());
        assert_count(&copied, &group_in(), 2);
        let legacy_copy = merge_readers(&[&legacy], &[DocIdMap::Offset(0)], spooled);
        assert!(!legacy_copy.has_disjoint_value_terms());
        assert_count(&legacy_copy, &group_in(), 2);
        let all_proven = merge_readers(
            &[&proven, &proven],
            &[DocIdMap::Offset(0), DocIdMap::Offset(2)],
            spooled,
        );
        assert!(all_proven.has_disjoint_value_terms());
        assert_count(&all_proven, &group_in(), 4);
        for inputs in [
            [&proven, &legacy],
            [&legacy, &proven],
            [&overlapping, &proven],
            [&proven, &overlapping],
        ] {
            let merged = merge_readers(
                &inputs,
                &[DocIdMap::Offset(0), DocIdMap::Offset(2)],
                spooled,
            );
            assert!(!merged.has_disjoint_value_terms());
            let expected = inputs
                .iter()
                .map(|reader| reader.count(&group_in()).unwrap())
                .sum();
            assert_count(&merged, &group_in(), expected);
        }
    }
}

#[test]
fn table_maps_cannot_upgrade_per_term_checks_into_cross_term_disjointness() {
    let (data, index) = source_pair(
        &[r#"{"group":"a"}"#, r#"{"group":"b"}"#],
        VixWriterOptions::default(),
    );
    let proven = open_built(data, index);
    for spooled in [false, true] {
        let permutation = merge_readers(&[&proven], &[DocIdMap::Table(vec![1, 0])], spooled);
        assert!(!permutation.has_disjoint_value_terms());
        assert_count(&permutation, &group_in(), 2);
        // Existing map admission checks range/length, and the postings
        // merger checks uniqueness per term only. Each singleton value
        // term passes, while the dense key term is elided. Preserve that
        // behavior without falsely certifying these overlapping sets.
        let collision = merge_readers(&[&proven], &[DocIdMap::Table(vec![0, 0])], spooled);
        assert!(!collision.has_disjoint_value_terms());
        assert_count(&collision, &group_in(), 1); // metadata sum would be 2
    }
}

#[test]
fn failed_push_retry_cannot_reuse_a_document_with_fresh_proof() {
    let mut writer = VixWriter::new(&schema(), VixWriterOptions::default(), false);
    // Storage validation fails AFTER the source row has emitted terms.
    // Retrying reuses the public document cursor without rolling back those
    // terms. Adjacent duplicate key IDs dedupe, leaving valid raw overlap.
    let unknown: Vec<(String, ArrayRef)> =
        vec![("unknown".into(), Arc::new(StringArray::from(vec!["x"])))];
    assert!(
        writer
            .push_docs_rows(
                &Int64Array::from(vec![100]),
                &unknown,
                &StringArray::from(vec![r#"{"group":"a"}"#]),
                None,
            )
            .is_err()
    );
    writer
        .push_docs_rows(
            &Int64Array::from(vec![100, 99]),
            &[],
            &StringArray::from(vec![r#"{"group":"b"}"#, r#"{"group":"d"}"#]),
            None,
        )
        .unwrap();
    let reader = finish_open(writer);
    assert!(!reader.has_disjoint_value_terms());
    // a and b both match doc 0. Their metadata sum is 2, no greater than
    // row_count, so only the missing proof prevents overcount.
    assert_eq!(reader.row_count(), 2);
    assert_count(&reader, &group_in(), 1);
    assert_eq!(eval_set(&reader, &group_in()), docs(&[0]));
}

fn failed_shorter_missing_retry(failed_sources: [&str; 3]) -> VixReader {
    let mut writer = VixWriter::new(&schema(), VixWriterOptions::default(), false);
    let unknown: Vec<(String, ArrayRef)> =
        vec![("unknown".into(), Arc::new(StringArray::from(vec!["x"; 3])))];
    // Source indexing runs before the unknown stored column fails validation.
    // No docs were committed, so the retry starts at doc 0.
    let error = writer
        .push_docs_rows(
            &Int64Array::from(vec![100, 99, 98]),
            &unknown,
            &StringArray::from(failed_sources.to_vec()),
            None,
        )
        .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<crate::VixError>(),
        Some(crate::VixError::Writer(_))
    ));
    // Missing-only sources emit no raw values to expose the reused cursor.
    // The nullable group column allows this shorter docs batch to commit.
    writer
        .push_docs_rows(
            &Int64Array::from(vec![100, 99]),
            &[],
            &StringArray::from(vec!["{}", "{}"]),
            None,
        )
        .unwrap();
    finish_open(writer)
}

#[test]
fn failed_shorter_retry_count_preserves_out_of_range_postings_error() {
    let reader = failed_shorter_missing_retry(["{}", "{}", r#"{"group":"b"}"#]);
    assert_eq!(reader.row_count(), 2);
    let query = VixQuery::Or(vec![exact("group", "b"), exact("group", "missing")]);
    // The retained b posting is doc 2, outside the committed two-row file.
    // A metadata-only count must not hide the error raised by bitmap eval.
    let bitmap_error = reader.eval(&query).unwrap_err();
    let count_error = reader.count(&query).unwrap_err();
    assert!(matches!(
        bitmap_error.downcast_ref::<crate::VixError>(),
        Some(crate::VixError::Malformed(_))
    ));
    assert!(matches!(
        count_error.downcast_ref::<crate::VixError>(),
        Some(crate::VixError::Malformed(_))
    ));
}

#[test]
fn failed_shorter_retry_offset_merge_does_not_certify_overlapping_values() {
    let retried = failed_shorter_missing_retry([r#"{"group":"c"}"#, "{}", r#"{"group":"b"}"#]);
    assert_eq!(retried.row_count(), 2);
    let (data, index) = source_pair(&[r#"{"group":"a"}"#], VixWriterOptions::default());
    let valid = open_built(data, index);
    // The retained group key incidences make the first input's key dense,
    // allowing the real merge to finish. Offset zero copies b's posting at
    // doc 2 unchanged; the valid input's a also maps to doc 2.
    let merged = merge_readers(
        &[&retried, &valid],
        &[DocIdMap::Offset(0), DocIdMap::Offset(2)],
        false,
    );
    assert_eq!(merged.row_count(), 3);
    let query = group_in();
    assert_eq!(merged.eval(&query).unwrap().count_set_bits(), 1);
    assert_eq!(merged.count(&query).unwrap(), 1);
}

#[test]
fn duplicate_typed_source_values_share_the_raw_incidence_guard() {
    let (data, index) = source_pair(
        &[
            r#"{"group":"a","group":2,"group":true}"#,
            r#"{"group":"c"}"#,
        ],
        VixWriterOptions::default(),
    );
    let reader = open_built(data, index);
    assert!(!reader.has_disjoint_value_terms());
    assert_count(
        &reader,
        &VixQuery::Or(vec![
            exact("group", "a"),
            exact_numeric("group", "2"),
            exact_numeric("group", "true"),
        ]),
        1,
    );
}

#[test]
fn encoded_docs_passthrough_does_not_mint_legacy_proof() {
    let (data, index) = source_pair(
        &[r#"{"group":"a","group":"b"}"#, r#"{"group":"c"}"#],
        VixWriterOptions::default(),
    );
    let index = repack_with_properties(index.unwrap(), |properties| {
        properties.retain(|(key, _)| key != RAW_VALUE_TERMS_DISJOINT_PROPERTY);
    });
    let reader = open_built(data.clone(), Some(index));
    let docs = crate::VixDocs::open(Bytes::from(data)).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut writer = VixWriter::new(
        &schema(),
        VixWriterOptions {
            docs_passthrough: true,
            term_spill_dir: Some(dir.path().to_path_buf()),
            output_spool_dir: Some(dir.path().to_path_buf()),
            ..Default::default()
        },
        false,
    );
    writer
        .merge_input_indexes(&[&reader], &[DocIdMap::Offset(0)], 1)
        .unwrap();
    let entries: Vec<crate::ZoneEntry> = docs
        .zone_chunks()
        .unwrap()
        .iter()
        .map(|zone| (zone.row_count, zone.ts_min, zone.ts_max))
        .collect();
    let stats = docs.spliceable_stats().unwrap().unwrap();
    writer
        .begin_docs_encoded_run(2, 9_999, 10_000, &entries, &stats, Some(&[2]))
        .unwrap();
    docs.scan_docs_encoded_chunks(&mut |chunk| writer.push_docs_encoded_chunk(chunk))
        .unwrap();
    writer.finish_docs_encoded_run().unwrap();
    let (data, index, _) = writer.finish_output().unwrap();
    let output = open_built(data.into_bytes().unwrap(), index);
    assert!(!output.has_disjoint_value_terms());
    assert_count(&output, &group_in(), 1);
}
