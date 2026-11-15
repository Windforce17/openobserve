// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, Int64Array, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema},
};
use bytes::Bytes;
use config::meta::inverted_index::IndexOptimizeMode;
use datafusion::{
    datasource::{
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        object_store::{DefaultObjectStoreRegistry, ObjectStoreRegistry},
    },
    execution::{runtime_env::RuntimeEnvBuilder, session_state::SessionStateBuilder},
    prelude::{SessionConfig, SessionContext},
};
use object_store::{ObjectStoreExt, memory::InMemory, path::Path};
use vortex_index::{VixReader, VixWriter, VixWriterOptions};

use super::simplify;
use crate::{
    datafusion::vix_format::VixCoreFormat,
    index::{Condition, IndexCondition},
    vix::{RawVixResult, evaluate_vix_index, evaluate_vix_index_with_empty_topn},
};

fn eq(field: &str, value: &str) -> Condition {
    Condition::Equal(field.into(), value.into())
}

fn condition(arms: Vec<Condition>) -> IndexCondition {
    IndexCondition {
        conditions: vec![Condition::Or(arms)],
    }
}

fn fixture(complete: bool, hidden: bool, unindexed: bool) -> (Bytes, Bytes) {
    assert!(!hidden || !complete);
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("a", DataType::Utf8, true),
        Field::new("b", DataType::Utf8, true),
        Field::new("service_name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![4, 3, 2, 1])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("x"),
                Some("y"),
                None,
                Some("x"),
            ])),
            Arc::new(StringArray::from(vec![Some("z"), Some("x"), None, None])),
            Arc::new(StringArray::from(vec![
                Some("s1"),
                Some("s1"),
                Some("s2"),
                None,
            ])),
        ],
    )
    .unwrap();
    let sources = [
        r#"{"_timestamp":4,"a":"x","b":"z","service_name":"s1"}"#,
        r#"{"_timestamp":3,"a":"y","b":"x","service_name":"s1"}"#,
        if hidden {
            r#"{"_timestamp":2,"ghost":"x","service_name":"s2"}"#
        } else {
            r#"{"_timestamp":2,"service_name":"s2"}"#
        },
        r#"{"_timestamp":1,"a":"x"}"#,
    ];
    let mut writer = VixWriter::new(
        &schema,
        VixWriterOptions {
            columns_complete: complete,
            value_index_excluded_field_names: if unindexed { vec!["b".into()] } else { vec![] },
            ..Default::default()
        },
        false,
    );
    writer
        .push_batch_with_source(&batch, &StringArray::from(sources.to_vec()), None)
        .unwrap();
    let (data, index) = writer.finish().unwrap();
    (data.into(), index.unwrap().into())
}

fn eval(
    reader: &VixReader,
    original: &IndexCondition,
    enabled: bool,
    mode: Option<IndexOptimizeMode>,
) -> anyhow::Result<RawVixResult> {
    let simplified = simplify(reader, original, enabled);
    evaluate_vix_index_with_empty_topn(
        "absent-or-test",
        reader,
        simplified.as_ref().unwrap_or(original),
        mode,
        (0, 10),
        true,
        Some((1, 4)),
        None,
        None,
        true,
    )
}

async fn scan(data: &Bytes, predicate: &str) -> Vec<u8> {
    let store = Arc::new(InMemory::new());
    store
        .put(&Path::from("data/fixture.vix"), data.clone().into())
        .await
        .unwrap();
    let registry = DefaultObjectStoreRegistry::new();
    registry.register_store(&url::Url::parse("test:///").unwrap(), store);
    let runtime = RuntimeEnvBuilder::new()
        .with_object_store_registry(Arc::new(registry))
        .build()
        .unwrap();
    let state = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(1))
        .with_runtime_env(Arc::new(runtime))
        .with_default_features()
        .build();
    let ctx = SessionContext::new_with_state(state);
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("a", DataType::Utf8, true),
        Field::new("b", DataType::Utf8, true),
        Field::new("ghost", DataType::Utf8, true),
        Field::new("service_name", DataType::Utf8, true),
    ]));
    let config = ListingTableConfig::new(ListingTableUrl::parse("test:///data/").unwrap())
        .with_listing_options(ListingOptions::new(Arc::new(VixCoreFormat::new(None))))
        .with_schema(schema);
    ctx.register_table("t", Arc::new(ListingTable::try_new(config).unwrap()))
        .unwrap();
    let batches = ctx
        .sql(&format!(
            "SELECT _timestamp FROM t WHERE {predicate} ORDER BY _timestamp DESC"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let values: Vec<i64> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    serde_json::to_vec(&values).unwrap()
}

#[tokio::test]
async fn absent_or_differential_scan_and_modes() {
    let (data, index) = fixture(true, false, false);
    let reader = VixReader::open_with_index(data.clone(), Some(index)).unwrap();
    for (sql, original, reduced) in [
        (
            "a = 'x' OR ghost = 'x'",
            condition(vec![eq("a", "x"), eq("ghost", "x")]),
            eq("a", "x"),
        ),
        (
            "ghost = 'x' OR a = 'x'",
            condition(vec![eq("ghost", "x"), eq("a", "x")]),
            eq("a", "x"),
        ),
        (
            "a IN ('x','z') OR ghost = 'x'",
            condition(vec![
                Condition::In("a".into(), vec!["x".into(), "z".into()], false),
                eq("ghost", "x"),
            ]),
            Condition::In("a".into(), vec!["x".into(), "z".into()], false),
        ),
        (
            "a = 'missing' OR ghost = 'x'",
            condition(vec![eq("a", "missing"), eq("ghost", "x")]),
            eq("a", "missing"),
        ),
        (
            "a = 'x' OR b = 'x' OR ghost = 'x'",
            condition(vec![eq("a", "x"), eq("b", "x"), eq("ghost", "x")]),
            Condition::Or(vec![eq("a", "x"), eq("b", "x")]),
        ),
    ] {
        assert!(simplify(&reader, &original, false).is_none());
        let old = eval(&reader, &original, false, None);
        assert!(old.unwrap_err().is::<crate::index::AllConditionsSkipped>());
        let oracle = scan(&data, sql).await;
        let RawVixResult::Bitmap {
            bitmap,
            has_skipped: false,
            ..
        } = eval(&reader, &original, true, None).unwrap()
        else {
            panic!("expected exact bitmap");
        };
        let actual: Vec<i64> = bitmap.set_indices().map(|index| 4 - index as i64).collect();
        assert_eq!(serde_json::to_vec(&actual).unwrap(), oracle, "{sql}");
        let exact = IndexCondition {
            conditions: vec![reduced],
        };
        for mode in [
            Some(IndexOptimizeMode::SimpleSelect(2, false)),
            Some(IndexOptimizeMode::SimpleCount),
            Some(IndexOptimizeMode::SimpleTopN(
                vec!["service_name".into()],
                10,
                false,
            )),
        ] {
            let actual = eval(&reader, &original, true, mode.clone()).unwrap();
            let expected = eval(&reader, &exact, false, mode).unwrap();
            assert_eq!(
                crate::vix::ranged_parity_tests::fingerprint(&actual).as_bytes(),
                crate::vix::ranged_parity_tests::fingerprint(&expected).as_bytes(),
                "{sql}",
            );
        }
    }
    let all_absent = condition(vec![eq("ghost", "x"), eq("ghost2", "x")]);
    assert!(simplify(&reader, &all_absent, true).is_none());
    let RawVixResult::Bitmap {
        bitmap,
        has_skipped: false,
        ..
    } = eval(&reader, &all_absent, true, None).unwrap()
    else {
        panic!("expected empty bitmap");
    };
    assert_eq!(bitmap.count_set_bits(), 0);
}

#[tokio::test]
async fn absent_or_counterexamples_remain_on_original_path() {
    for (complete, hidden, unindexed) in [
        (true, false, false),
        (false, false, false),
        (false, true, false),
        (true, false, true),
    ] {
        let (data, index) = fixture(complete, hidden, unindexed);
        let reader = VixReader::open_with_index(data.clone(), Some(index)).unwrap();
        let mixed = condition(vec![eq("a", "x"), eq("ghost", "x")]);
        if !complete {
            assert!(simplify(&reader, &mixed, true).is_none());
            let expected = if hidden {
                b"[4,2,1]".as_slice()
            } else {
                b"[4,1]".as_slice()
            };
            assert_eq!(scan(&data, "a = 'x' OR ghost = 'x'").await, expected);
        }
        let unsupported = [
            (
                "NOT (a = 'x' OR ghost = 'x')",
                IndexCondition {
                    conditions: vec![Condition::Not(Box::new(mixed.conditions[0].clone()))],
                },
                b"[]".as_slice(),
            ),
            (
                "a = 'x' OR ghost IS NULL",
                condition(vec![eq("a", "x"), Condition::IsNull("ghost".into())]),
                if hidden {
                    b"[4,3,1]".as_slice()
                } else {
                    b"[4,3,2,1]".as_slice()
                },
            ),
            (
                "a = 'x' OR ghost != 'y'",
                condition(vec![
                    eq("a", "x"),
                    Condition::NotEqual("ghost".into(), "y".into()),
                ]),
                if hidden {
                    b"[4,2,1]".as_slice()
                } else {
                    b"[4,1]".as_slice()
                },
            ),
            (
                "a = 'x' OR ghost IS NOT NULL",
                condition(vec![eq("a", "x"), Condition::IsNotNull("ghost".into())]),
                if hidden {
                    b"[4,2,1]".as_slice()
                } else {
                    b"[4,1]".as_slice()
                },
            ),
        ];
        for (sql, original, expected) in unsupported {
            assert!(simplify(&reader, &original, true).is_none(), "{sql}");
            assert_eq!(scan(&data, sql).await, expected, "{sql}");
            let fingerprint = |enabled| match eval(&reader, &original, enabled, None) {
                Ok(raw) => crate::vix::ranged_parity_tests::fingerprint(&raw),
                Err(error) => format!("error:{error:#}"),
            };
            assert_eq!(fingerprint(false), fingerprint(true), "{sql}");
        }
        if unindexed {
            assert!(reader.has_field("b"));
            assert!(!reader.has_term_capability("b"));
            let original = condition(vec![eq("a", "x"), eq("b", "x"), eq("ghost", "x")]);
            assert!(simplify(&reader, &original, true).is_none());
            assert!(eval(&reader, &original, true, None).is_err());
            assert_eq!(
                scan(&data, "a = 'x' OR b = 'x' OR ghost = 'x'").await,
                b"[4,3,1]"
            );
        }
    }
}

#[test]
fn absent_or_metadata_and_shape_guards() {
    let (data, index) = fixture(true, false, false);
    for index in [
        Bytes::from(
            vortex_index::test_support::repack_with_partial_fields(&index, &["a"]).unwrap(),
        ),
        Bytes::from(
            vortex_index::test_support::repack_dropping_field_term_capability(&index, "a").unwrap(),
        ),
    ] {
        let reader = VixReader::open_with_index(data.clone(), Some(index)).unwrap();
        assert!(
            simplify(
                &reader,
                &condition(vec![eq("a", "x"), eq("ghost", "x")]),
                true
            )
            .is_none()
        );
    }
    let reader = VixReader::open_with_index(data.clone(), None).unwrap();
    assert!(
        simplify(
            &reader,
            &condition(vec![eq("a", "x"), eq("ghost", "x")]),
            true
        )
        .is_none()
    );
    let reader = VixReader::open_with_index(data, Some(index)).unwrap();
    for arms in [
        vec![eq("a", "x"), eq("b", "x")],
        vec![eq("a", "x"), eq("_id", "x")],
        vec![eq("a", "x"), eq("_source", "x")],
        vec![eq("a", "x"), Condition::In("ghost".into(), vec![], false)],
        vec![
            eq("a", "x"),
            Condition::In("ghost".into(), vec!["x".into()], true),
        ],
        vec![eq("a", "x"), Condition::And(vec![eq("ghost", "x")])],
        vec![eq("ghost", "x"); 1025],
    ] {
        assert!(simplify(&reader, &condition(arms), true).is_none());
    }
    // Exact flattened names, never case folding or replacing dots with underscores.
    for field in ["A", "a.child", "a_child"] {
        let original = condition(vec![eq("a", "x"), eq(field, "x")]);
        assert_eq!(
            simplify(&reader, &original, true).unwrap().conditions,
            vec![eq("a", "x")]
        );
    }
}

#[test]
fn absent_or_runtime_switch() {
    const KEY: &str = "ZO_VIX_ABSENT_OR_PRUNING_ENABLED";
    const CHILD: &str = "OO_ABSENT_OR_SWITCH_CHILD";
    if let Ok(expected) = std::env::var(CHILD) {
        let (data, index) = fixture(true, false, false);
        let reader = VixReader::open_with_index(data, Some(index)).unwrap();
        let result = evaluate_vix_index(
            "absent-or-runtime",
            &reader,
            &condition(vec![eq("a", "x"), eq("ghost", "x")]),
            Some(IndexOptimizeMode::SimpleCount),
            (0, 10),
            true,
            None,
            None,
            None,
        );
        if expected == "true" {
            assert!(matches!(
                result.unwrap(),
                RawVixResult::Count {
                    count: 2,
                    has_skipped: false
                }
            ));
        } else {
            assert!(
                result
                    .unwrap_err()
                    .is::<crate::index::AllConditionsSkipped>()
            );
        }
        return;
    }
    for value in [None, Some("false"), Some("true")] {
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "vix::absent_or::tests::absent_or_runtime_switch",
                "--nocapture",
            ])
            .env(CHILD, value.unwrap_or("false"))
            .env_remove(KEY);
        if let Some(value) = value {
            command.env(KEY, value);
        }
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "{value:?}: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[tokio::test]
async fn absent_or_scalar_type_widening_keeps_scan() {
    for (kind, array, source, literal) in [
        (
            DataType::Int64,
            Arc::new(Int64Array::from(vec![7])) as ArrayRef,
            r#"{"_timestamp":4,"a":7}"#,
            "7",
        ),
        (
            DataType::Boolean,
            Arc::new(arrow::array::BooleanArray::from(vec![true])) as ArrayRef,
            r#"{"_timestamp":4,"a":true}"#,
            "true",
        ),
    ] {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("a", kind, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![4])), array],
        )
        .unwrap();
        let mut writer = VixWriter::new(
            &schema,
            VixWriterOptions {
                columns_complete: true,
                ..Default::default()
            },
            false,
        );
        writer
            .push_batch_with_source(&batch, &StringArray::from(vec![source]), None)
            .unwrap();
        let (data, index) = writer.finish().unwrap();
        let data = Bytes::from(data);
        let reader = VixReader::open_with_index(data.clone(), index.map(Bytes::from)).unwrap();
        assert!(reader.has_term_capability("a"));
        let original = condition(vec![eq("a", literal), eq("ghost", "x")]);
        assert!(simplify(&reader, &original, true).is_none());
        assert_eq!(
            scan(&data, &format!("a = '{literal}' OR ghost = 'x'")).await,
            b"[4]"
        );
        // A raw string lookup cannot stand in for the cast performed by scan.
        let RawVixResult::Bitmap { bitmap, .. } = eval(
            &reader,
            &IndexCondition {
                conditions: vec![eq("a", literal)],
            },
            false,
            None,
        )
        .unwrap() else {
            panic!("expected bitmap");
        };
        assert_eq!(bitmap.count_set_bits(), 0);
    }
    for value in [
        "0", "-0", "1e3", "NaN", "inf", "-inf", "Infinity", "TRUE", "false",
    ] {
        assert!(!super::non_scalar_literal(value));
    }
    for value in [
        "x",
        "9kcyul45r23dq5q2qk0h7ect3",
        "5d9a0j73tqigq084by5jpu4nq",
        "",
    ] {
        assert!(super::non_scalar_literal(value));
    }
}

#[test]
fn absent_or_old_metadata_oversize_and_tokens_fail_open() {
    let (data, index) = fixture(true, false, false);
    let original = condition(vec![eq("a", "x"), eq("ghost", "x")]);
    let old_data = Bytes::from(
        vortex_index::test_support::strip_property_for_tests(&data, "columns_complete").unwrap(),
    );
    let no_echo = Bytes::from(
        vortex_index::test_support::strip_property_for_tests(&index, "data_footer_v1").unwrap(),
    );
    let old = VixReader::open_with_index(old_data, Some(no_echo.clone())).unwrap();
    assert!(!old.columns_complete());
    assert!(simplify(&old, &original, true).is_none());
    let oversize = Bytes::from(
        vortex_index::test_support::repack_properties(&data, |properties| {
            properties.retain(|(key, _)| key != "oversize_skips");
            properties.push(("oversize_skips".into(), "{\"a\":1}".into()));
            Ok(())
        })
        .unwrap(),
    );
    let reader = VixReader::open_with_index(oversize, Some(no_echo)).unwrap();
    assert_eq!(reader.field_oversize_skips("a"), 1);
    assert!(simplify(&reader, &original, true).is_none());
    let tokens = Bytes::from(
        vortex_index::test_support::repack_properties(&index, |properties| {
            let (_, encoded) = properties
                .iter_mut()
                .find(|(key, _)| key == "fields")
                .unwrap();
            let mut fields: serde_json::Value = serde_json::from_str(encoded)?;
            for field in fields.as_array_mut().unwrap() {
                if field["name"] == "a" {
                    field["types"] = serde_json::json!(["fts", "cs"]);
                }
            }
            *encoded = fields.to_string();
            Ok(())
        })
        .unwrap(),
    );
    let reader = VixReader::open_with_index(data, Some(tokens)).unwrap();
    assert!(!reader.has_term_capability("a"));
    assert!(reader.fts_fields().contains("a"));
    assert!(simplify(&reader, &original, true).is_none());
}
