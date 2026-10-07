// Copyright 2026 OpenObserve Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

//! The `fts_long_token_skips` sidecar property: the writer's per-field count
//! of full-text tokens DROPPED for exceeding `max_token_len`. A dropped long
//! run is invisible to the token dictionary, but a SUBSTRING needle can hide
//! inside it — the property's presence (every fts field, zero included)
//! certifies the accounting, and `fts_tokens_complete` trusts it.

use super::*;
use crate::{DocIdMap, container::PROP_FTS_LONG_TOKEN_SKIPS};

fn skips_of(reader: &VixReader) -> Option<std::collections::BTreeMap<String, u64>> {
    reader
        .fts_long_token_skips()
        .map(|skips| skips.iter().map(|(k, v)| (k.clone(), *v)).collect())
}

/// One fts field whose value holds an alphanumeric run beyond the tokenizer's
/// exclusive max (70 bytes > 64), one fts field with none, and a plain term
/// field: the property lists BOTH fts fields (zero included), completeness
/// follows the per-field count, and non-fts fields are never complete.
#[test]
fn fts_long_token_skips_stamp_and_completeness() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("a", DataType::Utf8, true),
        Field::new("b", DataType::Utf8, true),
        Field::new("term_f", DataType::Utf8, true),
    ]));
    let opts = VixWriterOptions {
        fts_field_names: vec!["a".to_string(), "b".to_string()],
        max_token_len: 64,
        ..VixWriterOptions::default()
    };
    let long_run = "x".repeat(70);
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![100])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some(format!(
                "pre {long_run} post"
            ))])),
            Arc::new(StringArray::from(vec![Some("plain tokens here")])),
            Arc::new(StringArray::from(vec![Some("raw value")])),
        ],
    )
    .unwrap();
    let source = synthesize_source_for_test(&batch);
    let mut writer = VixWriter::new(&schema, opts, false);
    writer
        .push_batch_with_source(&batch, &source, None)
        .unwrap();
    let reader = finish_open(writer);

    assert_eq!(
        skips_of(&reader),
        Some(std::collections::BTreeMap::from([
            ("a".to_string(), 1),
            ("b".to_string(), 0),
        ]))
    );
    // the long run is invisible to the token dictionary: `a` is not
    // complete; `b` dropped nothing; non-fts fields never are
    assert!(!reader.fts_tokens_complete("a"));
    assert!(reader.fts_tokens_complete("b"));
    assert!(!reader.fts_tokens_complete("term_f"));
    // the surviving tokens indexed normally around the dropped run, and a
    // short needle INSIDE the dropped run is invisible to the token
    // dictionary — the exact hole the property's non-zero count flags
    assert_eq!(eval_set(&reader, &any_token("pre")), docs(&[0]));
    let mid_run = &long_run[10..24];
    assert_eq!(
        eval_set(&reader, &contains(None, mid_run, false)),
        docs(&[])
    );
}

/// The `_source`-driven derivation (the compaction rebuild path) counts the
/// same drops as the column-driven one — a rebuilt file re-derives its own
/// accounting.
#[test]
fn fts_long_token_skips_source_driven_rebuild() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("a", DataType::Utf8, true),
        Field::new("b", DataType::Utf8, true),
    ]));
    let opts = VixWriterOptions {
        fts_field_names: vec!["a".to_string(), "b".to_string()],
        max_token_len: 64,
        ..VixWriterOptions::default()
    };
    let long_run = "y".repeat(70);
    let sources = StringArray::from(vec![format!(r#"{{"a":"tail {long_run}","b":"fine"}}"#)]);
    let mut writer = VixWriter::new(&schema, opts, false);
    writer
        .push_docs_rows(&Int64Array::from(vec![100]), &[], &sources, None)
        .unwrap();
    let reader = finish_open(writer);
    assert_eq!(
        skips_of(&reader),
        Some(std::collections::BTreeMap::from([
            ("a".to_string(), 1),
            ("b".to_string(), 0),
        ]))
    );
    assert!(!reader.fts_tokens_complete("a"));
    assert!(reader.fts_tokens_complete("b"));
}

/// Merging index sidecars SUMs the inputs' per-field counts — the merged
/// dictionary carries exactly the inputs' tokens, so its accounting is the
/// sum. An input LACKING the property (a legacy sidecar) makes the merged
/// drops unknowable: the output must not claim accounting either, and the
/// reader treats the property as absent.
#[test]
fn merge_sums_fts_long_token_skips() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("a", DataType::Utf8, true),
    ]));
    let opts = VixWriterOptions {
        fts_field_names: vec!["a".to_string()],
        max_token_len: 64,
        ..VixWriterOptions::default()
    };
    let build = |ts: Vec<i64>, values: Vec<Option<&str>>| -> VixReader {
        let rows = ts.len();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ts)) as ArrayRef,
                Arc::new(StringArray::from(values)),
            ],
        )
        .unwrap();
        let mut writer = VixWriter::new(&schema, opts.clone(), false);
        writer
            .push_batch_with_source(&batch, &dataset_sources(0..rows), None)
            .unwrap();
        finish_open(writer)
    };
    // one 70-byte run in f1, two in f2
    let run1 = "a".repeat(70);
    let run2 = "b".repeat(70);
    let run3 = "c".repeat(70);
    let f1 = build(vec![900], vec![Some(format!("one {run1}").as_str())]);
    let f2 = build(
        vec![800, 700],
        vec![Some(format!("{run2} {run3}").as_str()), Some("plain value")],
    );
    assert_eq!(
        skips_of(&f1),
        Some(std::collections::BTreeMap::from([("a".to_string(), 1)]))
    );
    assert_eq!(
        skips_of(&f2),
        Some(std::collections::BTreeMap::from([("a".to_string(), 2)]))
    );

    let merge = |f1: &VixReader, f2: &VixReader| -> VixReader {
        let rows1 = f1.row_count() as u32;
        let total = (f1.row_count() + f2.row_count()) as usize;
        let mut merged = VixWriter::new(&schema, opts.clone(), false);
        merged
            .merge_input_indexes(
                &[f1, f2],
                &[DocIdMap::Offset(0), DocIdMap::Offset(rows1)],
                1,
            )
            .unwrap();
        merged
            .push_docs_rows_unindexed(
                &Int64Array::from_iter_values((0..total).map(|i| 1000 - i as i64)),
                &[],
                &dataset_sources(0..total),
                None,
            )
            .unwrap();
        finish_open(merged)
    };

    // accounting inputs merge: counts sum
    let summed = merge(&f1, &f2);
    assert_eq!(
        skips_of(&summed),
        Some(std::collections::BTreeMap::from([("a".to_string(), 3)]))
    );
    assert!(!summed.fts_tokens_complete("a"));

    // a legacy input (property stripped) makes the merged accounting
    // unknowable: the output carries NO property
    let legacy = {
        let mut writer = VixWriter::new(&schema, opts.clone(), false);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![600])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("legacy input")])),
            ],
        )
        .unwrap();
        writer
            .push_batch_with_source(&batch, &dataset_sources(0..1), None)
            .unwrap();
        let (data, index) = writer.finish().unwrap();
        let stripped = repack_with_properties(index.expect("sidecar"), |properties| {
            properties.retain(|(key, _)| key != PROP_FTS_LONG_TOKEN_SKIPS);
        });
        open_built(data, Some(stripped))
    };
    assert_eq!(skips_of(&legacy), None);
    let unaccounted = merge(&f1, &legacy);
    assert_eq!(skips_of(&unaccounted), None);
    assert!(!unaccounted.fts_tokens_complete("a"));
}

/// A writer with NO fts fields still stamps the property (`{}`): presence —
/// not content — is the accounting certificate, and a file whose fts plan
/// is empty provably dropped nothing.
#[test]
fn no_fts_fields_still_stamp_empty_accounting() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("level", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![100])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("info")])),
        ],
    )
    .unwrap();
    let source = synthesize_source_for_test(&batch);
    let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
    writer
        .push_batch_with_source(&batch, &source, None)
        .unwrap();
    let reader = finish_open(writer);
    assert_eq!(skips_of(&reader), Some(std::collections::BTreeMap::new()));
    // no field is fts, so none can be token-complete
    assert!(!reader.fts_tokens_complete("level"));
    assert!(!reader.fts_tokens_complete("nonexistent"));
}
