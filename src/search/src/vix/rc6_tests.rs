// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-or-later

use arrow::{
    array::{Array, BooleanArray, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::physical_plan::PhysicalExpr;
use vortex_index::{VixWriter, VixWriterOptions};

use super::*;
use crate::index::{AllConditionsSkipped, Condition, pruning::file_candidates_safe};

fn schema(dtype: DataType) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("body", dtype, true),
    ]))
}

fn write_file(batch: &RecordBatch, stored_type: DataType, fts: bool, max_raw: usize) -> VixReader {
    let mut writer = VixWriter::new(
        &schema(stored_type),
        VixWriterOptions {
            fts_field_names: if fts { vec!["body".into()] } else { vec![] },
            max_raw_term_len: max_raw,
            row_group_size: 32,
            ..Default::default()
        },
        false,
    );
    writer
        .push_batch_with_source(
            batch,
            &StringArray::from(vec!["{}"; batch.num_rows()]),
            None,
        )
        .unwrap();
    let (data, index) = writer.finish().unwrap();
    VixReader::open_with_index(data.into(), index.map(Into::into)).unwrap()
}

fn strings(values: Vec<Option<String>>) -> RecordBatch {
    RecordBatch::try_new(
        schema(DataType::Utf8),
        vec![
            Arc::new(Int64Array::from_iter_values(
                (0..values.len()).map(|i| i as i64 + 1),
            )),
            Arc::new(StringArray::from(values)),
        ],
    )
    .unwrap()
}

fn normalized_or(values: &[&str]) -> Condition {
    Condition::NormalizedOr(Box::new(Condition::In(
        "body".into(),
        values.iter().map(|v| v.to_string()).collect(),
        false,
    )))
}

fn index(condition: Condition) -> IndexCondition {
    IndexCondition {
        conditions: vec![condition],
    }
}

fn evaluate(reader: &VixReader, condition: &IndexCondition) -> anyhow::Result<RawVixResult> {
    evaluate_vix_index(
        "rc6-coverage",
        reader,
        condition,
        None,
        (0, i64::MAX),
        true,
        None,
        None,
        None,
    )
}

fn assert_rows(
    reader: &VixReader,
    condition: &IndexCondition,
    expr: &Arc<dyn PhysicalExpr>,
    batch: &RecordBatch,
) -> usize {
    let result = expr
        .evaluate(batch)
        .unwrap()
        .into_array(batch.num_rows())
        .unwrap();
    let truth = result.as_any().downcast_ref::<BooleanArray>().unwrap();
    let expected: Vec<usize> = (0..batch.num_rows())
        .filter(|&i| truth.is_valid(i) && truth.value(i))
        .collect();
    let candidates = match evaluate(reader, condition) {
        Ok(RawVixResult::Bitmap {
            bitmap,
            has_skipped,
            ..
        }) => {
            if !has_skipped {
                assert_eq!(bitmap.set_indices().collect::<Vec<_>>(), expected);
            }
            Some(bitmap)
        }
        Ok(RawVixResult::ExactNoMatch) => Some(BooleanBuffer::new_unset(batch.num_rows())),
        Ok(RawVixResult::PartialFields) => None,
        Err(e) if e.is::<AllConditionsSkipped>() => None,
        other => panic!("unexpected evaluation: {other:?}"),
    };
    let actual: Vec<usize> = expected
        .iter()
        .copied()
        .filter(|&i| candidates.as_ref().is_none_or(|bits| bits.value(i)))
        .collect();
    assert_eq!(actual, expected, "condition {condition:?}");
    candidates.map_or(batch.num_rows(), |bits| bits.count_set_bits())
}

#[test]
fn rc6_oversize_prefix_fails_open_before_postings() {
    let batch = strings(vec![
        Some("prefix-long-omitted".into()),
        Some("prefix".into()),
        None,
    ]);
    let reader = write_file(&batch, DataType::Utf8, false, 8);
    assert_eq!(reader.field_oversize_skips("body"), 1);
    assert!(!reader.partial_fields().contains("body"));
    let cond = index(Condition::Prefix("body".into(), "prefix".into()));
    assert!(!file_candidates_safe(&cond, &reader).unwrap());
    assert!(matches!(
        evaluate(&reader, &cond).unwrap(),
        RawVixResult::PartialFields
    ));
    let expr = cond.conditions[0]
        .to_physical_expr(&batch.schema(), &[])
        .unwrap();
    assert_eq!(assert_rows(&reader, &cond, &expr, &batch), 3);
}

#[test]
fn rc6_numeric_fts_and_raw_drift_fail_open_even_after_string_cast() {
    let input = RecordBatch::try_new(
        schema(DataType::Int64),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![Some(123), Some(-42), None])),
        ],
    )
    .unwrap();
    let scanned = strings(vec![Some("123".into()), Some("-42".into()), None]);
    for stored in [DataType::Int64, DataType::Utf8] {
        for fts in [false, true] {
            let reader = write_file(&input, stored.clone(), fts, 1024);
            assert!(!reader.partial_fields().contains("body"));
            for leaf in [
                normalized_or(&["123", "-42"]),
                Condition::Prefix("body".into(), "12".into()),
            ] {
                let cond = index(leaf);
                assert!(!file_candidates_safe(&cond, &reader).unwrap());
                assert!(matches!(
                    evaluate(&reader, &cond).unwrap(),
                    RawVixResult::PartialFields
                ));
                let expr = cond.conditions[0]
                    .to_physical_expr(&scanned.schema(), &[])
                    .unwrap();
                assert_rows(&reader, &cond, &expr, &scanned);
            }
            // The added guard does not change pre-existing equality/IN paths.
            assert!(
                file_candidates_safe(
                    &index(Condition::In("body".into(), vec!["123".into()], false)),
                    &reader
                )
                .unwrap()
            );
            // A non-numeric query also needs a stored string-type proof.
            assert_eq!(
                file_candidates_safe(&index(normalized_or(&["alpha", "beta"])), &reader).unwrap(),
                stored == DataType::Utf8
            );
        }
    }
}

#[test]
fn rc6_randomized_file_candidates_equal_full_scan() {
    let phrases = [
        "database password",
        "rotation stopped",
        "unrelated event",
        "数据库",
        "你好😀",
        "",
        "!!!",
        "a%b",
        "a_b",
        "a\\b",
    ];
    let mut state = 0xfeed_592b_u64;
    let mut next = || {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        state
    };
    let values = (0..4096)
        .map(|row| {
            if row % 31 == 0 {
                None
            } else {
                Some(format!(
                    "{} {}",
                    phrases[(next() >> 32) as usize % phrases.len()],
                    phrases[(next() >> 32) as usize % phrases.len()]
                ))
            }
        })
        .collect();
    let batch = strings(values);
    let fields = [("body".to_string(), DataType::Utf8)].into_iter().collect();
    let mut narrowed = 0;
    for fts in [false, true] {
        let reader = write_file(&batch, DataType::Utf8, fts, 1024);
        for i in 0..64 {
            let row = (next() as usize % batch.num_rows()) as u64;
            let values = reader.read_docs_column_rows("body", &[row]).unwrap();
            let values = arrow::compute::cast(&values, &DataType::Utf8).unwrap();
            let value = values.as_any().downcast_ref::<StringArray>().unwrap();
            let text = if value.is_null(0) {
                "unrelated"
            } else {
                value.value(0)
            };
            let condition = if i % 2 == 0 {
                normalized_or(&[text, "database password"])
            } else {
                Condition::Prefix("body".into(), phrases[i % phrases.len()].into())
            };
            // Empty prefixes are deliberately not extracted from LIKE '%'.
            if matches!(&condition, Condition::Prefix(_, s) if s.is_empty()) {
                continue;
            }
            let expr = condition.to_physical_expr(&batch.schema(), &[]).unwrap();
            let extracted = Condition::from_physical_expr(&expr, &fields);
            // Rebuilt IN loses provenance by design; keep the actual OR marker here.
            let condition = if matches!(condition, Condition::NormalizedOr(..)) {
                condition
            } else {
                extracted
            };
            let cond = index(condition);
            if assert_rows(&reader, &cond, &expr, &batch) < batch.num_rows() {
                narrowed += 1;
            }
        }
    }
    assert!(narrowed >= 64, "expected useful narrowing, got {narrowed}");
}

#[test]
fn rc6_guard_checks_nested_conditions_and_scalar_spellings() {
    let batch = strings(vec![Some("alpha".into())]);
    let reader = write_file(&batch, DataType::Utf8, true, 1024);
    for value in [
        "true", "false", "NaN", "inf", "Infinity", "+4", "-4", ".5", "0.5", "12e3",
    ] {
        let leaf = normalized_or(&["alpha", value]);
        for wrapper in [
            leaf.clone(),
            Condition::Not(Box::new(leaf.clone())),
            Condition::Or(vec![
                Condition::Equal("body".into(), "alpha".into()),
                leaf.clone(),
            ]),
            Condition::And(vec![leaf]),
        ] {
            assert!(
                !file_candidates_safe(&index(wrapper), &reader).unwrap(),
                "{value}"
            );
        }
    }
    for prefix in ["t", "f", "Na", "in", "-", ".", "12"] {
        assert!(
            !file_candidates_safe(
                &index(Condition::Prefix("body".into(), prefix.into())),
                &reader
            )
            .unwrap(),
            "{prefix}"
        );
    }
    assert!(file_candidates_safe(&index(normalized_or(&["alpha", "beta"])), &reader).unwrap());
    assert!(
        !file_candidates_safe(
            &index(Condition::Prefix("missing".into(), "alpha".into())),
            &reader
        )
        .unwrap()
    );
}

#[test]
fn rc6_bool_float_drift_uses_full_scan_for_scalar_text() {
    use arrow::array::{ArrayRef, Float64Array};

    let columns: Vec<ArrayRef> = vec![
        Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
        Arc::new(Float64Array::from(vec![
            Some(1.25),
            Some(-3.0),
            None,
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
        ])),
    ];
    for column in columns {
        let input = RecordBatch::try_new(
            schema(column.data_type().clone()),
            vec![
                Arc::new(Int64Array::from_iter_values(
                    (0..column.len()).map(|i| i as i64 + 1),
                )),
                column,
            ],
        )
        .unwrap();
        for fts in [false, true] {
            let reader = write_file(&input, DataType::Utf8, fts, 1024);
            let strings_column = reader.read_docs_column("body").unwrap();
            let strings_column = arrow::compute::cast(&strings_column, &DataType::Utf8).unwrap();
            let strings_column = strings_column
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let scanned = strings(
                strings_column
                    .iter()
                    .map(|s| s.map(str::to_string))
                    .collect(),
            );
            for text in strings_column.iter().flatten() {
                for condition in [
                    normalized_or(&[text, "alpha"]),
                    Condition::Prefix("body".into(), text.chars().take(1).collect()),
                ] {
                    let cond = index(condition);
                    assert!(!file_candidates_safe(&cond, &reader).unwrap(), "{text}");
                    let expr = cond.conditions[0]
                        .to_physical_expr(&scanned.schema(), &[])
                        .unwrap();
                    assert_rows(&reader, &cond, &expr, &scanned);
                }
            }
        }
    }
}
