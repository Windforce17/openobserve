// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-or-later

use arrow::{
    array::{Array, BooleanArray, Int64Array, StringArray},
    datatypes::{Field, Schema},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use datafusion::physical_plan::expressions::{CastExpr, LikeExpr};
use vortex_index::{VixReader, VixWriter, VixWriterOptions};

use super::*;
use crate::index::{Condition, FieldCap, IndexCondition, disjunction};

fn col(name: &str) -> Arc<dyn PhysicalExpr> {
    Arc::new(Column::new(name, 1))
}

fn lit(value: &str) -> Arc<dyn PhysicalExpr> {
    Arc::new(Literal::new(ScalarValue::Utf8(Some(value.to_string()))))
}

fn eq(left: Arc<dyn PhysicalExpr>, right: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
    Arc::new(BinaryExpr::new(left, Operator::Eq, right))
}

fn like(pattern: &str) -> Arc<dyn PhysicalExpr> {
    Arc::new(LikeExpr::new(false, false, col("body"), lit(pattern)))
}

fn batch(values: &[Option<String>]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("body", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from_iter_values(
                (0..values.len()).map(|i| i as i64 + 1),
            )),
            Arc::new(StringArray::from_iter(values.iter().map(|s| s.as_deref()))),
        ],
    )
    .unwrap()
}

fn truth(expr: &Arc<dyn PhysicalExpr>, batch: &RecordBatch) -> Vec<bool> {
    let result = expr
        .evaluate(batch)
        .unwrap()
        .into_array(batch.num_rows())
        .unwrap();
    let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();
    (0..batch.num_rows())
        .map(|i| !result.is_null(i) && result.value(i))
        .collect()
}

fn reader(batch: &RecordBatch, fts: bool) -> VixReader {
    let mut writer = VixWriter::new(
        &batch.schema(),
        VixWriterOptions {
            fts_field_names: if fts { vec!["body".into()] } else { vec![] },
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
    VixReader::open_with_index(Bytes::from(data), Some(Bytes::from(index.unwrap()))).unwrap()
}

#[test]
fn rc6_equal_or_recognition_fails_open() {
    let good = disjunction(vec![eq(col("body"), lit("a")), eq(lit("b"), col("body"))]);
    assert_eq!(equal_or_literals(&good).unwrap().1.len(), 2);
    let bad = [
        eq(col("other"), lit("b")),
        eq(Arc::new(Column::new("body", 2)), lit("b")),
        eq(col("body"), Arc::new(Literal::new(ScalarValue::Utf8(None)))),
        eq(
            col("body"),
            Arc::new(Literal::new(ScalarValue::Int64(Some(1)))),
        ),
        eq(
            Arc::new(CastExpr::new(col("body"), DataType::Utf8, None)),
            lit("b"),
        ),
        Arc::new(BinaryExpr::new(col("body"), Operator::NotEq, lit("b"))),
    ];
    for arm in bad {
        assert!(equal_or_literals(&disjunction(vec![eq(col("body"), lit("a")), arm])).is_none());
    }
    let too_many = disjunction(
        (0..MAX_OR_TERMS + 1)
            .map(|_| eq(col("body"), lit("a")))
            .collect(),
    );
    assert!(equal_or_literals(&too_many).is_none());
    let too_wide = disjunction(vec![
        eq(col("body"), lit("a")),
        eq(col("body"), lit(&"b".repeat(MAX_LITERAL_BYTES))),
    ]);
    assert!(equal_or_literals(&too_wide).is_none());
}

#[test]
fn rc6_prefix_escape_and_unicode_boundaries() {
    for (pattern, prefix) in [
        ("abc%", "abc"),
        ("abc%%%", "abc"),
        (r"a\%b%", "a%b"),
        (r"a\_b%", "a_b"),
        (r"a\\b%", r"a\b"),
        ("你好😀%", "你好😀"),
        ("\u{10ffff}%", "\u{10ffff}"),
        ("\0%", "\0"),
    ] {
        assert_eq!(like_prefix(pattern).as_deref(), Some(prefix), "{pattern:?}");
        assert_eq!(
            like_prefix(&prefix_pattern(prefix)).as_deref(),
            Some(prefix)
        );
    }
    for rejected in [
        "", "%", "abc", "%abc", "a_b%", "a%b%", r"a\%", r"a\", r"a\q%",
    ] {
        assert!(like_prefix(rejected).is_none(), "{rejected:?}");
    }
    assert!(like_prefix(&format!("{}%", "a".repeat(MAX_PREFIX_BYTES))).is_none());
    assert_eq!(prefix_upper_bound("\u{d7ff}"), Some("\u{e000}".into()));
    assert_eq!(prefix_upper_bound("z\u{10ffff}"), Some("{".into()));
    assert_eq!(prefix_upper_bound("\u{10ffff}"), None);
}

#[test]
fn rc6_prefix_bounds_match_arrow_randomized() {
    // Fixed seed: reproducible without global RNG state. Include surrogate
    // neighbours, maximum scalars, controls, literal wildcards and NULL.
    let alphabet = [
        'a',
        'Z',
        'é',
        '你',
        '😀',
        '\u{d7ff}',
        '\u{e000}',
        '\u{10ffff}',
        '%',
        '_',
        '\\',
        '\n',
        '\0',
    ];
    let mut seed = 0x592b_3571_u64;
    let mut next = || {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        seed
    };
    let mut values = vec![None, Some(String::new())];
    for _ in 0..4096 {
        let len = (next() % 12) as usize;
        let value: String = (0..len)
            .map(|_| alphabet[next() as usize % alphabet.len()])
            .collect();
        values.push(Some(value));
    }
    let mut prefixes: Vec<String> = alphabet.iter().map(char::to_string).collect();
    prefixes.extend(
        values
            .iter()
            .flatten()
            .filter(|s| !s.is_empty())
            .take(96)
            .cloned(),
    );
    let batch = batch(&values);
    for prefix in prefixes {
        let expr = like(&prefix_pattern(&prefix));
        let expected = truth(&expr, &batch);
        let upper = prefix_upper_bound(&prefix);
        let candidates: Vec<bool> = values
            .iter()
            .map(|v| {
                v.as_ref()
                    .is_some_and(|s| s >= &prefix && upper.as_ref().is_none_or(|upper| s < upper))
            })
            .collect();
        let rows = |mask: &[bool]| {
            mask.iter()
                .enumerate()
                .filter_map(|(i, &keep)| keep.then_some(i))
                .collect::<Vec<_>>()
        };
        let actual: Vec<bool> = expected
            .iter()
            .zip(&candidates)
            .map(|(a, b)| *a && *b)
            .collect();
        assert_eq!(rows(&actual), rows(&expected), "prefix {prefix:?}");
        assert_eq!(
            candidates, expected,
            "prefix bounds must match Arrow exactly: {prefix:?}"
        );
    }
}

#[test]
fn rc6_fts_or_candidates_match_arrow() {
    let phrases = [
        Some("database password rotation summary"),
        Some("database password rotation stopped"),
        Some("summary rotation database password extra"),
        Some("unrelated event"),
        None,
        Some("数据库轮换"),
        Some(""),
    ];
    let values: Vec<Option<String>> = (0..2048)
        .map(|i| phrases[i % phrases.len()].map(str::to_string))
        .collect();
    let batch = batch(&values);
    let expr = disjunction(vec![
        eq(col("body"), lit(phrases[0].unwrap())),
        eq(lit(phrases[1].unwrap()), col("body")),
    ]);
    let condition = Condition::from_physical_expr(
        &expr,
        &[("body".to_string(), DataType::Utf8)].into_iter().collect(),
    );
    assert!(matches!(condition, Condition::NormalizedOr(..)));
    let tok = |text: &str| vortex_index::o2_tokenize(text, 2, 64).collect::<Vec<_>>();
    let index = IndexCondition {
        conditions: vec![condition.clone()],
    };
    let (query, skipped) = index
        .to_vix_query("rc6", &|_| FieldCap::Tokens, &tok, true)
        .unwrap();
    assert!(skipped, "FTS equality candidates must reapply the filter");
    let candidates = reader(&batch, true).eval(&query).unwrap();
    let expected = truth(&expr, &batch);
    assert!(candidates.count_set_bits() < batch.num_rows());
    assert!(candidates.count_set_bits() > expected.iter().filter(|&&v| v).count());
    let actual: Vec<usize> = expected
        .iter()
        .enumerate()
        .filter_map(|(i, &v)| (v && candidates.value(i)).then_some(i))
        .collect();
    let expected_rows: Vec<usize> = expected
        .iter()
        .enumerate()
        .filter_map(|(i, &v)| v.then_some(i))
        .collect();
    assert_eq!(actual, expected_rows);
    assert_eq!(
        truth(
            &condition.to_physical_expr(&batch.schema(), &[]).unwrap(),
            &batch
        ),
        expected
    );
    // One tokenless arm disables the WHOLE superset, not just that arm.
    let tokenless = Condition::In("body".into(), vec!["database".into(), "!!!".into()], false);
    assert!(tokenless.fts_superset_query("body", &tok).is_none());
    assert!(
        index
            .to_vix_query("rc6", &|_| FieldCap::Partial, &tok, true)
            .is_err()
    );
}

#[test]
fn rc6_prefix_index_roundtrip_matches_arrow() {
    let values: Vec<Option<String>> = [
        None,
        Some(""),
        Some("abc"),
        Some("abcdef"),
        Some("ABC"),
        Some("a%b"),
        Some("a_b"),
        Some(r"a\b"),
        Some("你好😀"),
        Some("你好\n"),
        Some("\u{10ffff}tail"),
    ]
    .into_iter()
    .cycle()
    .take(1024)
    .map(|s| s.map(str::to_string))
    .collect();
    let batch = batch(&values);
    let reader = reader(&batch, false);
    let fields = [("body".to_string(), DataType::Utf8)].into_iter().collect();
    for pattern in ["abc%", r"a\%%", r"a\_%", r"a\\%", "你好%", "\u{10ffff}%"] {
        let expr = like(pattern);
        let condition = Condition::from_physical_expr(&expr, &fields);
        assert!(matches!(condition, Condition::Prefix(..)));
        assert!(!condition.can_remove_filter());
        let candidates = reader
            .eval(&condition.to_vix_query(&|_| vec![]).unwrap())
            .unwrap();
        let expected = truth(&expr, &batch);
        let rebuilt = condition.to_physical_expr(&batch.schema(), &[]).unwrap();
        assert_eq!(truth(&rebuilt, &batch), expected, "{pattern}");
        for (row, expected) in expected.into_iter().enumerate() {
            assert_eq!(candidates.value(row), expected, "{pattern}: row {row}");
        }
        let index = IndexCondition {
            conditions: vec![condition],
        };
        assert!(
            index
                .to_vix_query("rc6", &|_| FieldCap::Tokens, &|_| vec![], true)
                .is_err()
        );
        assert!(
            index
                .to_vix_query("rc6", &|_| FieldCap::Partial, &|_| vec![], true)
                .is_err()
        );
    }
}

#[test]
fn rc6_random_or_fts_never_loses_matching_rows() {
    let words = [
        "alpha",
        "beta",
        "数据库",
        "😀",
        "!!!",
        "a_b",
        r"a\b",
        "",
        "éclair",
        "ROTATION",
    ];
    let mut state = 7_u64;
    let mut next = || {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        state
    };
    let values: Vec<Option<String>> = (0..2048)
        .map(|i| {
            if i % 29 == 0 {
                None
            } else {
                Some(format!(
                    "{} {}",
                    words[next() as usize % words.len()],
                    words[next() as usize % words.len()]
                ))
            }
        })
        .collect();
    let batch = batch(&values);
    let reader = reader(&batch, true);
    let fields = [("body".to_string(), DataType::Utf8)].into_iter().collect();
    let tok = |text: &str| vortex_index::o2_tokenize(text, 2, 64).collect::<Vec<_>>();
    let mut narrowed = 0;
    for i in 1..65 {
        let left = values[i].as_deref().unwrap_or("");
        let right = values[2047 - i].as_deref().unwrap_or("");
        let expr = disjunction(vec![
            eq(col("body"), lit(left)),
            eq(col("body"), lit(right)),
        ]);
        let condition = Condition::from_physical_expr(&expr, &fields);
        let expected = truth(&expr, &batch);
        let candidate = condition
            .fts_superset_query("body", &tok)
            .map(|q| reader.eval(&q).unwrap());
        if candidate
            .as_ref()
            .is_some_and(|bits| bits.count_set_bits() < batch.num_rows())
        {
            narrowed += 1;
        }
        let actual: Vec<usize> = expected
            .iter()
            .enumerate()
            .filter_map(|(row, &matches)| {
                (matches && candidate.as_ref().is_none_or(|bits| bits.value(row))).then_some(row)
            })
            .collect();
        let expected: Vec<usize> = expected
            .iter()
            .enumerate()
            .filter_map(|(row, &matches)| matches.then_some(row))
            .collect();
        assert_eq!(actual, expected, "{left:?} OR {right:?}");
    }
    assert!(narrowed > 0);
}

#[test]
fn rc6_prefix_string_types_and_unsafe_patterns() {
    let values = vec![Some("你好%_\\tail".into()), Some("你好other".into()), None];
    let input = batch(&values);
    for dtype in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("body", dtype.clone(), true),
        ]));
        let typed = RecordBatch::try_new(
            schema,
            vec![
                input.column(0).clone(),
                arrow::compute::cast(input.column(1), &dtype).unwrap(),
            ],
        )
        .unwrap();
        let condition = Condition::Prefix("body".into(), "你好%_\\".into());
        let expr = condition.to_physical_expr(&typed.schema(), &[]).unwrap();
        assert_eq!(truth(&expr, &typed), vec![true, false, false]);
        let fields = [("body".into(), dtype)].into_iter().collect();
        assert_eq!(Condition::from_physical_expr(&expr, &fields), condition);
    }
    let fields = [("body".into(), DataType::Utf8)].into_iter().collect();
    for expr in [
        LikeExpr::new(true, false, col("body"), lit("abc%")),
        LikeExpr::new(false, true, col("body"), lit("abc%")),
        LikeExpr::new(false, false, col("body"), lit("a_b%")),
        LikeExpr::new(false, false, col("body"), lit(r"abc\%")),
        LikeExpr::new(
            false,
            false,
            col("body"),
            Arc::new(Literal::new(ScalarValue::Utf8(None))),
        ),
        LikeExpr::new(
            false,
            false,
            Arc::new(CastExpr::new(col("body"), DataType::Utf8, None)),
            lit("abc%"),
        ),
    ] {
        assert!(crate::index::index_like_prefix(&expr, &fields).is_none());
    }
    let non_string = [("body".into(), DataType::Int64)].into_iter().collect();
    assert!(
        crate::index::index_like_prefix(
            &LikeExpr::new(false, false, col("body"), lit("12%")),
            &non_string
        )
        .is_none()
    );
}
