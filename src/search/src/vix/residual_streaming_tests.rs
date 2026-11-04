// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use std::sync::Arc;

use arrow::{
    array::{Array, BooleanArray, BooleanBufferBuilder, Int64Array, RecordBatch, StringArray},
    buffer::BooleanBuffer,
    datatypes::{Field, Schema},
};
use arrow_schema::DataType;
use bytes::Bytes;
use vortex_index::{VixDocs, VixReader, VixWriter, VixWriterOptions};

use super::super::residual::*;
use crate::index::{Condition, IndexCondition};

fn fixture(rows: usize) -> (Bytes, Bytes) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("body", DataType::Utf8, true),
        Field::new("other", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(
                (0..rows)
                    .map(|row| 1_000_000 - row as i64)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                (0..rows)
                    .map(|row| match row % 13 {
                        0 => None,
                        1 => Some("你好 世界 alpha beta CAFÉ"),
                        2 => Some("alpha gap beta"),
                        _ => Some("beta alpha"),
                    })
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                (0..rows)
                    .map(|row| (row % 29 == 0).then_some("ALPHA BETA 你好 世界"))
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    let mut writer = VixWriter::new(
        &schema,
        VixWriterOptions {
            fts_field_names: vec!["body".into(), "other".into()],
            docs_chunk_bytes: 8192,
            ..Default::default()
        },
        false,
    );
    writer
        .push_batch_with_source(&batch, &StringArray::from(vec!["{}"; rows]), None)
        .unwrap();
    let (data, index) = writer.finish().unwrap();
    (Bytes::from(data), Bytes::from(index.unwrap()))
}

fn full_scan(data: Bytes, condition: &IndexCondition, scope: &[String]) -> BooleanBuffer {
    let docs = VixDocs::open(data).unwrap();
    let mut exact = BooleanBufferBuilder::new(0);
    docs.scan_docs(None, None, None, &mut |batch| {
        let expression = condition.to_physical_expr(
            batch.schema().as_ref(),
            &scope
                .iter()
                .filter(|field| batch.schema().index_of(field).is_ok())
                .cloned()
                .collect::<Vec<_>>(),
        )?;
        let values = expression.evaluate(&batch)?.into_array(batch.num_rows())?;
        let mask = values.as_any().downcast_ref::<BooleanArray>().unwrap();
        for row in 0..batch.num_rows() {
            exact.append(mask.is_valid(row) && mask.value(row));
        }
        Ok(())
    })
    .unwrap();
    exact.finish()
}

#[test]
fn streaming_phrase_matches_full_scan_at_4096_boundary() {
    let scope = vec!["body".into(), "other".into(), "absent".into()];
    for rows in [4095, 4096, 4097, 8193] {
        let (data, index) = fixture(rows);
        let reader = VixReader::open_with_index(data.clone(), Some(index)).unwrap();
        for phrase in ["alpha beta", "你好 世界", "not present"] {
            let condition = IndexCondition {
                conditions: vec![Condition::MatchAll(phrase.into())],
            };
            let expected = full_scan(data.clone(), &condition, &scope);
            let candidates = BooleanBuffer::new_set(rows);
            let actual = refine_superset(
                &reader,
                &condition,
                &[0],
                &scope,
                &candidates,
                Shape::Select,
            )
            .unwrap()
            .expect("select must stream a narrow predicate past the old total-row cap");
            assert_eq!(actual, expected, "rows={rows}, phrase={phrase}");
        }
    }
}

#[test]
fn streaming_phrase_topn_only_uses_verified_rows() {
    use config::meta::inverted_index::IndexOptimizeMode;

    use crate::vix::{RawVixResult, evaluate_vix_index};

    let (data, index) = fixture(8193);
    let reader = VixReader::open_with_index(data.clone(), Some(index)).unwrap();
    let scope = vec!["body".into(), "other".into()];
    let condition = IndexCondition {
        conditions: vec![Condition::MatchAll("alpha beta".into())],
    };
    let expected = full_scan(data, &condition, &scope);
    for ascend in [true, false] {
        let mut truth: Vec<(i64, u32)> = expected
            .set_indices()
            .map(|row| (1_000_000 - row as i64, row as u32))
            .collect();
        if ascend {
            truth.reverse();
        }
        truth.truncate(5);
        match evaluate_vix_index(
            "streaming-phrase",
            &reader,
            &condition,
            Some(IndexOptimizeMode::SimpleSelect(5, ascend)),
            (0, 2_000_000),
            true,
            None,
            None,
            Some(&scope),
        )
        .unwrap()
        {
            RawVixResult::SelectCandidates { candidates, .. } => assert_eq!(candidates, truth),
            other => panic!("expected verified candidates, got {other:?}"),
        }
    }
}

#[test]
fn streaming_budget_refuses_without_publishing_prefix() {
    // The projected layout must produce multiple callbacks, not just zone chunks.
    let count = 200_001;
    let (data, index) = fixture(count);
    let reader = VixReader::open_with_index(data, Some(index)).unwrap();
    let rows = BooleanBuffer::new_set(count);
    let original = rows.clone();
    let scope = vec!["body".into(), "other".into()];
    let condition = IndexCondition {
        conditions: vec![Condition::MatchAll("alpha beta".into())],
    };
    assert_eq!(
        refine_select_streaming(&reader, &condition, &[0], &scope, &rows, 0).unwrap(),
        Err(Refusal::TooManyBytes)
    );
    let scratch = count * 16 + count.div_ceil(8) * 2;
    let mut chunks = Vec::new();
    reader
        .filter_docs_columns_rows(&["body"], &rows, RESIDUAL_SELECT_MAX_BYTES, &mut |batch| {
            chunks.push((batch.num_rows(), batch.get_array_memory_size()));
            Ok(BooleanArray::from(vec![true; batch.num_rows()]))
        })
        .unwrap()
        .unwrap();
    assert!(
        chunks.len() > 1,
        "fixture must have multiple projected scan chunks: {chunks:?}"
    );
    let mut visited = 0;
    let exact = reader
        .filter_docs_columns_rows(
            &["body"],
            &rows,
            (scratch + chunks[0].1) as u64,
            &mut |batch| {
                visited += batch.num_rows();
                Ok(BooleanArray::from(vec![true; batch.num_rows()]))
            },
        )
        .unwrap();
    assert_eq!(
        visited, chunks[0].0,
        "budget must stop after one verified chunk"
    );
    assert!(
        visited > 0 && visited < rows.len(),
        "budget must stop after a verified prefix, visited={visited}"
    );
    assert!(
        exact.is_none(),
        "a verified prefix is never a complete answer"
    );
    assert_eq!(rows, original);
}

#[test]
fn streaming_cancel_after_first_chunk_does_not_publish_exact_rows() {
    use std::sync::atomic::{AtomicBool, Ordering};

    use vortex_index::{VixError, VixReadOperation, with_read_operation};
    struct Operation(AtomicBool);
    impl VixReadOperation for Operation {
        fn is_cancelled(&self) -> bool {
            self.0.load(Ordering::Relaxed)
        }
    }
    let count = 200_001;
    let (data, index) = fixture(count);
    let reader = VixReader::open_with_index(data, Some(index)).unwrap();
    let operation = Arc::new(Operation(AtomicBool::new(false)));
    let mut visited = 0;
    let error = with_read_operation(operation.clone(), || {
        reader.filter_docs_columns_rows(
            &["body"],
            &BooleanBuffer::new_set(count),
            RESIDUAL_SELECT_MAX_BYTES,
            &mut |batch| {
                visited += batch.num_rows();
                operation.0.store(true, Ordering::Relaxed);
                Ok(BooleanArray::from(vec![true; batch.num_rows()]))
            },
        )
    })
    .unwrap_err();
    assert!(visited > 0 && visited < count);
    assert!(
        matches!(error.downcast_ref::<VixError>(), Some(VixError::Cancelled)),
        "{error:#}"
    );
    operation.0.store(false, Ordering::Relaxed);
    let condition = IndexCondition {
        conditions: vec![Condition::MatchAll("alpha beta".into())],
    };
    assert!(
        refine_superset(
            &reader,
            &condition,
            &[0],
            &["body".into()],
            &BooleanBuffer::new_set(count),
            Shape::Select
        )
        .unwrap()
        .is_ok(),
        "cancellation must not poison the retained reader"
    );
}

#[test]
fn streaming_memory_and_callback_errors_are_not_budget_refusals() {
    use vortex_index::{VixError, VixReadOperation, with_read_operation};
    struct Refuse;
    impl VixReadOperation for Refuse {
        fn is_cancelled(&self) -> bool {
            false
        }
        fn check_memory(&self, _bytes: usize) -> vortex_index::Result<()> {
            Err(VixError::Callback(anyhow::anyhow!(
                "test memory admission refusal"
            )))
        }
    }
    let (data, index) = fixture(8193);
    let reader = VixReader::open_with_index(data, Some(index)).unwrap();
    let rows = BooleanBuffer::new_set(8193);
    let error = with_read_operation(Arc::new(Refuse), || {
        reader.filter_docs_columns_rows(&["body"], &rows, RESIDUAL_SELECT_MAX_BYTES, &mut |_| {
            panic!("must not evaluate after admission refusal")
        })
    })
    .unwrap_err();
    assert!(format!("{error:#}").contains("test memory admission refusal"));
    let error = reader
        .filter_docs_columns_rows(&["body"], &rows, RESIDUAL_SELECT_MAX_BYTES, &mut |_| {
            Err(anyhow::anyhow!("test predicate failure"))
        })
        .unwrap_err();
    assert!(format!("{error:#}").contains("test predicate failure"));
    assert!(
        reader
            .filter_docs_columns_rows(&["body"], &rows, RESIDUAL_SELECT_MAX_BYTES, &mut |_| Ok(
                BooleanArray::from(vec![true])
            ))
            .is_err()
    );
    assert!(
        reader
            .filter_docs_columns_rows(
                &["body"],
                &BooleanBuffer::new_set(8192),
                RESIDUAL_SELECT_MAX_BYTES,
                &mut |_| unreachable!()
            )
            .is_err()
    );
}

#[test]
fn streaming_unsupported_predicates_keep_the_original_fallback() {
    let (data, index) = fixture(8193);
    let reader = VixReader::open_with_index(data, Some(index)).unwrap();
    let rows = BooleanBuffer::new_set(8193);
    for (condition, expected) in [
        (
            Condition::Equal("missing".into(), "alpha".into()),
            Refusal::UnsupportedColumn("missing".into()),
        ),
        (
            Condition::Equal("_timestamp".into(), "1".into()),
            Refusal::UnsupportedColumn("_timestamp".into()),
        ),
    ] {
        assert_eq!(
            refine_superset(
                &reader,
                &IndexCondition {
                    conditions: vec![condition]
                },
                &[0],
                &["body".into()],
                &rows,
                Shape::Select
            )
            .unwrap(),
            Err(expected)
        );
    }
    let condition = IndexCondition {
        conditions: vec![Condition::MatchAll("alpha beta".into())],
    };
    assert_eq!(
        refine_superset(
            &reader,
            &condition,
            &[0],
            &["missing".into()],
            &rows,
            Shape::Select
        )
        .unwrap(),
        Err(Refusal::NoFullTextColumn)
    );
    assert_eq!(
        refine_superset(
            &reader,
            &condition,
            &[0],
            &["body".into()],
            &rows,
            Shape::Aggregate
        )
        .unwrap(),
        Err(Refusal::TooManyRows(8193))
    );
}

#[test]
fn streaming_sparse_rows_keep_absolute_positions() {
    let count = 200_001;
    let (data, index) = fixture(count);
    let reader = VixReader::open_with_index(data.clone(), Some(index)).unwrap();
    let scope = vec!["body".into(), "other".into()];
    let condition = IndexCondition {
        conditions: vec![Condition::MatchAll("alpha beta".into())],
    };
    let truth = full_scan(data, &condition, &scope);
    let mut selected = BooleanBufferBuilder::new(count);
    for row in 0..count {
        selected.append(row % 31 == 0 || [4095, 4096, 4097, count - 1].contains(&row));
    }
    let selected = selected.finish();
    assert!(selected.count_set_bits() > RESIDUAL_MAX_ROWS);
    let expected = &truth & &selected;
    assert!(expected.count_set_bits() > 0);
    let actual = refine_superset(&reader, &condition, &[0], &scope, &selected, Shape::Select)
        .unwrap()
        .expect("sparse rows must stream past the old row cap");
    assert_eq!(actual, expected);
    assert_eq!(&actual & &selected, actual);
}
