// Copyright 2026 OpenObserve Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::{hint::black_box, sync::Arc, time::Instant};

use arrow::{
    array::{
        ArrayRef, BinaryArray, BooleanArray, Decimal128Array, DictionaryArray, Float32Array,
        Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeStringArray, ListArray,
        NullArray, StringArray, StringViewArray, StructArray, TimestampMicrosecondArray,
        UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    },
    record_batch::RecordBatchOptions,
};
use arrow_schema::{Field, Schema};
use serde_json::json;

use super::*;

fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
    let fields: Vec<_> = columns
        .iter()
        .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
        .collect();
    RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns.into_iter().map(|(_, array)| array).collect(),
    )
    .unwrap()
}

// Independent copy of the baseline response-side algorithm, not the shared helper.
fn baseline(
    batches: &[&RecordBatch],
    limit: Option<usize>,
) -> (Vec<Map<String, Value>>, StarExpandStats) {
    let mut rows = record_batches_to_json_rows(batches).unwrap();
    if let Some(limit) = limit {
        rows.truncate(limit);
    }
    let mut stats = StarExpandStats::default();
    for row in &mut rows {
        let Some(value) = row.remove(SOURCE_COL_NAME) else {
            stats.missing_source += 1;
            continue;
        };
        let Value::String(source) = value else {
            stats.non_string_source += 1;
            continue;
        };
        match serde_json::from_str::<Map<String, Value>>(&source) {
            Ok(parsed) => {
                let mut hit: Map<String, Value> = parsed
                    .into_iter()
                    .filter(|(_, value)| !value.is_null())
                    .collect();
                for (key, value) in std::mem::take(row) {
                    hit.insert(key, value);
                }
                *row = hit;
            }
            Err(error) => {
                stats.unparsable_source += 1;
                if stats.first_parse_error.is_none() {
                    stats.first_parse_error = Some(error.to_string());
                }
            }
        }
    }
    (rows, stats)
}

fn parity(
    batches: &[&RecordBatch],
    limit: Option<usize>,
) -> (Vec<Map<String, Value>>, StarExpandStats) {
    let expected = baseline(batches, limit);
    let actual = record_batches_to_star_hits(batches, limit).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(
        serde_json::to_vec(&actual.0).unwrap(),
        serde_json::to_vec(&expected.0).unwrap()
    );
    let mut legacy = record_batches_to_json_rows(batches).unwrap();
    if let Some(limit) = limit {
        legacy.truncate(limit);
    }
    assert_eq!(expand_star_source_hits(&mut legacy, true), expected.1);
    assert_eq!(
        serde_json::to_vec(&legacy).unwrap(),
        serde_json::to_vec(&expected.0).unwrap()
    );
    actual
}

#[test]
fn star_direct_strings_precedence_and_nulls() {
    let sources = vec![
        Some(
            r#"{"code":"drift","physical_null":"keep","gone":null,"nested":{"null":null},"dup":1,"dup":2,"_source":"nested source","unicode":"汉字🚀","huge":184467440737095516160}"#,
        ),
        Some(r#"{"code":0,"physical_null":null,"esc":"\"\\\n\r\t\u0000"}"#),
        None,
    ];
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(sources.clone())),
        Arc::new(LargeStringArray::from(sources.clone())),
        Arc::new(StringViewArray::from(sources)),
    ];
    for source in arrays {
        let b = batch(vec![
            ("before", Arc::new(StringArray::from(vec!["a", "b", "c"]))),
            (SOURCE_COL_NAME, source),
            (
                "code",
                Arc::new(Int64Array::from(vec![Some(200), None, Some(7)])),
            ),
            (
                "physical_null",
                Arc::new(StringArray::from(vec![None::<&str>; 3])),
            ),
            (
                "after",
                Arc::new(StringViewArray::from(vec![
                    Some("\"\n汉字🚀\u{2028}"),
                    None,
                    Some(""),
                ])),
            ),
        ]);
        assert!(supports_direct_star_hits(&b));
        let (rows, stats) = parity(&[&b], None);
        assert_eq!(rows[0]["code"], json!(200));
        assert_eq!(rows[0]["physical_null"], json!("keep"));
        assert!(!rows[0].contains_key("gone"));
        assert_eq!(rows[0]["nested"], json!({"null": null}));
        assert_eq!(rows[0]["dup"], json!(2));
        assert_eq!(rows[0][SOURCE_COL_NAME], json!("nested source"));
        assert_eq!(rows[1]["code"], json!(0));
        assert_eq!(stats.missing_source, 1);
        parity(&[&b.slice(1, 2)], None);
    }
}

#[test]
fn star_direct_all_integer_widths_and_boolean() {
    let b = batch(vec![
        (SOURCE_COL_NAME, Arc::new(StringArray::from(vec!["{}"; 3]))),
        (
            "i8",
            Arc::new(Int8Array::from(vec![Some(i8::MIN), None, Some(i8::MAX)])),
        ),
        (
            "i16",
            Arc::new(Int16Array::from(vec![Some(i16::MIN), None, Some(i16::MAX)])),
        ),
        (
            "i32",
            Arc::new(Int32Array::from(vec![Some(i32::MIN), None, Some(i32::MAX)])),
        ),
        (
            "i64",
            Arc::new(Int64Array::from(vec![Some(i64::MIN), None, Some(i64::MAX)])),
        ),
        (
            "u8",
            Arc::new(UInt8Array::from(vec![Some(0), None, Some(u8::MAX)])),
        ),
        (
            "u16",
            Arc::new(UInt16Array::from(vec![Some(0), None, Some(u16::MAX)])),
        ),
        (
            "u32",
            Arc::new(UInt32Array::from(vec![Some(0), None, Some(u32::MAX)])),
        ),
        (
            "u64",
            Arc::new(UInt64Array::from(vec![Some(0), None, Some(u64::MAX)])),
        ),
        (
            "bool",
            Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
        ),
        ("null", Arc::new(NullArray::new(3))),
        (
            "utf8",
            Arc::new(StringArray::from(vec![Some("\0\n\"\\"), None, Some("é")])),
        ),
        (
            "large",
            Arc::new(LargeStringArray::from(vec![Some("a"), None, Some("中")])),
        ),
        (
            "view",
            Arc::new(StringViewArray::from(vec![Some("a"), None, Some("🚀")])),
        ),
    ]);
    assert!(supports_direct_star_hits(&b));
    let (rows, stats) = parity(&[&b], None);
    assert!(rows[1].is_empty());
    assert_eq!(rows[2]["u64"].as_u64(), Some(u64::MAX));
    assert_eq!(rows[0]["i64"].as_i64(), Some(i64::MIN));
    assert_eq!(stats, StarExpandStats::default());
}

#[test]
fn star_direct_missing_null_and_non_string_source() {
    let missing = batch(vec![("code", Arc::new(Int64Array::from(vec![1, 2])))]);
    assert_eq!(parity(&[&missing], None).1.missing_source, 2);
    let sources: Vec<ArrayRef> = vec![
        Arc::new(NullArray::new(2)),
        Arc::new(StringArray::from(vec![None::<&str>; 2])),
        Arc::new(LargeStringArray::from(vec![None::<&str>; 2])),
        Arc::new(StringViewArray::from(vec![None::<&str>; 2])),
        Arc::new(Int64Array::from(vec![Some(42), None])),
        Arc::new(BooleanArray::from(vec![Some(true), None])),
    ];
    for source in sources {
        let non_string = matches!(source.data_type(), DataType::Int64 | DataType::Boolean);
        let b = batch(vec![
            (SOURCE_COL_NAME, source),
            ("code", Arc::new(Int64Array::from(vec![1, 2]))),
        ]);
        let (rows, stats) = parity(&[&b], None);
        assert_eq!(stats.non_string_source, usize::from(non_string));
        assert_eq!(stats.missing_source, if non_string { 1 } else { 2 });
        assert!(rows.iter().all(|row| !row.contains_key(SOURCE_COL_NAME)));
    }
}

#[test]
fn star_direct_parse_errors_and_first_error() {
    let b = batch(vec![
        (
            SOURCE_COL_NAME,
            Arc::new(StringViewArray::from(vec![
                "not-json",
                "[]",
                "null",
                "1",
                "\"text\"",
                "{\ninvalid",
                "{}",
            ])),
        ),
        ("id", Arc::new(Int64Array::from_iter_values(0..7))),
    ]);
    let (_, stats) = parity(&[&b, &b], None);
    assert_eq!(stats.unparsable_source, 12);
    assert_eq!(stats.degraded(), 12);
    assert!(stats.first_parse_error.is_some());
    for limit in [0, 1, 6, 7, 8, 14, 15] {
        parity(&[&b, &b], Some(limit));
    }
}

#[test]
fn star_direct_unsupported_types_fall_back_whole_batch() {
    let dict: DictionaryArray<Int8Type> = [Some("one"), None, Some("two")].into_iter().collect();
    let nested = StructArray::from(vec![(
        Arc::new(Field::new("nested", DataType::Int64, true)),
        Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])) as ArrayRef,
    )]);
    let unsupported: Vec<ArrayRef> = vec![
        Arc::new(Float64Array::from(vec![
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(-0.0),
        ])),
        Arc::new(Float32Array::from(vec![
            Some(1.25),
            None,
            Some(f32::NEG_INFINITY),
        ])),
        Arc::new(TimestampMicrosecondArray::from(vec![
            Some(1_704_067_200_000_001),
            None,
            Some(-1),
        ])),
        Arc::new(
            Decimal128Array::from(vec![Some(12345), None, Some(-999)])
                .with_precision_and_scale(20, 3)
                .unwrap(),
        ),
        Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), None]),
            None,
            Some(vec![]),
        ])),
        Arc::new(nested),
        Arc::new(dict),
        Arc::new(BinaryArray::from(vec![
            Some(b"\x00\xff".as_slice()),
            None,
            Some(b"x".as_slice()),
        ])),
    ];
    for array in unsupported {
        let b = batch(vec![
            (
                SOURCE_COL_NAME,
                Arc::new(StringArray::from(vec![r#"{"extra":1,"complex":"old"}"#; 3])),
            ),
            ("complex", array),
            ("id", Arc::new(Int64Array::from(vec![1, 2, 3]))),
        ]);
        assert!(!supports_direct_star_hits(&b), "{:?}", b.schema());
        parity(&[&b], None);
        parity(&[&b], Some(1));
        let as_source = batch(vec![(SOURCE_COL_NAME, b.column(1).clone())]);
        assert!(!supports_direct_star_hits(&as_source));
        parity(&[&as_source], None);
    }
}

#[test]
fn star_direct_preserves_writer_errors_after_limit() {
    use arrow::array::{Int32Builder, Int64Builder, MapBuilder};

    let mut builder = MapBuilder::new(None, Int32Builder::new(), Int64Builder::new());
    builder.keys().append_value(1);
    builder.values().append_value(2);
    builder.append(true).unwrap();
    let unsupported = batch(vec![("map", Arc::new(builder.finish()))]);
    let direct = batch(vec![(
        SOURCE_COL_NAME,
        Arc::new(StringArray::from(vec!["{}"])),
    )]);
    let old = record_batches_to_json_rows(&[&direct, &unsupported])
        .unwrap_err()
        .to_string();
    for limit in [None, Some(0), Some(1)] {
        let new = record_batches_to_star_hits(&[&direct, &unsupported], limit)
            .unwrap_err()
            .to_string();
        assert_eq!(new, old);
    }
}

#[test]
fn star_direct_duplicate_physical_names_fall_back() {
    for duplicate in ["dup", SOURCE_COL_NAME] {
        let b = batch(vec![
            (
                duplicate,
                Arc::new(StringArray::from(vec![Some(r#"{"a":1}"#), Some("{}")])),
            ),
            ("between", Arc::new(Int64Array::from(vec![1, 2]))),
            (
                duplicate,
                Arc::new(StringArray::from(vec![Some(r#"{"b":2}"#), None])),
            ),
        ]);
        assert!(!supports_direct_star_hits(&b));
        parity(&[&b], None);
    }
}

#[test]
fn star_direct_empty_and_mixed_batches_limit() {
    let direct = batch(vec![(
        SOURCE_COL_NAME,
        Arc::new(StringArray::from(vec!["{}", "bad", "[]"])),
    )]);
    let fallback = batch(vec![
        ("f", Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]))),
        (
            SOURCE_COL_NAME,
            Arc::new(StringArray::from(vec!["null", "{}", "bad"])),
        ),
    ]);
    let empty = direct.slice(0, 0);
    let empty_fallback = fallback.slice(0, 0);
    let no_columns = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(2)),
    )
    .unwrap();
    parity(&[], None);
    parity(&[&empty, &empty_fallback], None);
    parity(&[&no_columns], None);
    let batches = [
        &empty,
        &direct,
        &empty_fallback,
        &fallback,
        &no_columns,
        &direct,
    ];
    for limit in [0, 1, 3, 4, 6, 8, 9, 11, usize::MAX] {
        parity(&batches, Some(limit));
    }
    let (_, stats) = parity(&[&direct], Some(1));
    assert_eq!(stats, StarExpandStats::default());
    parity(&batches, None);
}

#[test]
fn star_direct_key_order_matches_writer() {
    for source_index in 0..4 {
        let mut columns: Vec<(&str, ArrayRef)> = vec![
            ("z", Arc::new(Int64Array::from(vec![1]))),
            ("a", Arc::new(Int64Array::from(vec![2]))),
            ("m", Arc::new(Int64Array::from(vec![3]))),
        ];
        columns.insert(
            source_index,
            (
                SOURCE_COL_NAME,
                Arc::new(StringArray::from(vec![
                    r#"{"x":1,"z":0,"n":null,"x":2,"nested":{"z":0,"a":1}}"#,
                ])),
            ),
        );
        parity(&[&batch(columns)], None);
    }
    let map: Map<String, Value> = serde_json::from_str(r#"{"z":0,"a":1}"#).unwrap();
    eprintln!(
        "serde_json Map key order: {:?}",
        map.keys().collect::<Vec<_>>()
    );
}

#[test]
fn star_direct_batch_metrics() {
    let direct = batch(vec![(
        SOURCE_COL_NAME,
        Arc::new(StringArray::from(vec!["{}"])),
    )]);
    let fallback = batch(vec![("f", Arc::new(Float64Array::from(vec![1.0])))]);
    let direct_counter = QUERY_STAR_HITS_BATCHES_TOTAL.with_label_values(&["direct"]);
    let fallback_counter = QUERY_STAR_HITS_BATCHES_TOTAL.with_label_values(&["fallback"]);
    let before_direct = direct_counter.get();
    let before_fallback = fallback_counter.get();
    record_batches_to_star_hits(&[&direct, &fallback], None).unwrap();
    assert!(direct_counter.get() > before_direct);
    assert!(fallback_counter.get() > before_fallback);
}

#[test]
#[ignore = "reproducible 1000-row conversion microbenchmark"]
fn star_direct_microbench() {
    let sources: Vec<String> = (0..1000)
        .map(|i| json!({"message": "x".repeat(1420), "id": i, "service": "api", "unicode": "汉字", "nullable": null}).to_string())
        .collect();
    let source_bytes: usize = sources.iter().map(String::len).sum();
    let b = batch(vec![
        (SOURCE_COL_NAME, Arc::new(StringArray::from(sources))),
        (
            "_timestamp",
            Arc::new(Int64Array::from_iter_values(0..1000)),
        ),
        ("id", Arc::new(UInt64Array::from_iter_values(0..1000))),
        (
            "service",
            Arc::new(StringViewArray::from(vec!["physical"; 1000])),
        ),
        ("ok", Arc::new(BooleanArray::from(vec![true; 1000]))),
        ("code", Arc::new(Int32Array::from(vec![200; 1000]))),
    ]);
    parity(&[&b], None);
    for _ in 0..5 {
        black_box(baseline(&[&b], None));
        black_box(record_batches_to_star_hits(&[&b], None).unwrap());
    }
    let iterations = 30;
    let mut old = Vec::new();
    let mut direct = Vec::new();
    for round in 0..7 {
        let mut measure = |is_direct| {
            let start = Instant::now();
            for _ in 0..iterations {
                if is_direct {
                    black_box(record_batches_to_star_hits(black_box(&[&b]), None).unwrap());
                } else {
                    black_box(baseline(black_box(&[&b]), None));
                }
            }
            let ms = start.elapsed().as_secs_f64() * 1000.0 / f64::from(iterations);
            if is_direct {
                direct.push(ms);
            } else {
                old.push(ms);
            }
        };
        measure(round % 2 == 0);
        measure(round % 2 != 0);
    }
    old.sort_by(f64::total_cmp);
    direct.sort_by(f64::total_cmp);
    eprintln!(
        "star_hits benchmark rows=1000 physical_columns=5 source_bytes={source_bytes} rounds=7 iterations={iterations} old_ms={old:?} direct_ms={direct:?} median_speedup={:.3}",
        old[3] / direct[3]
    );
}
