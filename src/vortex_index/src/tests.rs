// Copyright 2026 OpenObserve Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

//! Writer/reader round-trip tests over synthetic record batches.

use std::{collections::BTreeSet, sync::Arc};

use arrow::{
    array::{Array, ArrayRef, Int64Array, StringArray},
    buffer::BooleanBuffer,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use rand::{RngExt, SeedableRng, rngs::StdRng};

use crate::{VixQuery, VixReader, VixWriter, VixWriterOptions};

fn bits_to_set(bits: &BooleanBuffer) -> BTreeSet<u32> {
    bits.iter()
        .enumerate()
        .filter_map(|(index, set)| set.then_some(index as u32))
        .collect()
}

fn docs(ids: &[u32]) -> BTreeSet<u32> {
    ids.iter().copied().collect()
}

fn exact(field: &str, token: &str) -> VixQuery {
    VixQuery::Exact {
        field: field.to_string(),
        token: token.as_bytes().to_vec(),
    }
}

/// Exact probe of a TAGGED canonical numeric/bool value term.
fn exact_numeric(field: &str, canonical: &str) -> VixQuery {
    VixQuery::Exact {
        field: field.to_string(),
        token: crate::numeric_value_token(canonical),
    }
}

fn any_token(token: &str) -> VixQuery {
    VixQuery::TokenAnyField {
        token: token.as_bytes().to_vec(),
    }
}

fn prefix(field: Option<&str>, prefix: &str) -> VixQuery {
    VixQuery::Prefix {
        field: field.map(str::to_string),
        prefix: prefix.as_bytes().to_vec(),
    }
}

fn contains(field: Option<&str>, needle: &str, case_insensitive: bool) -> VixQuery {
    VixQuery::Contains {
        field: field.map(str::to_string),
        needle: needle.as_bytes().to_vec(),
        case_insensitive,
    }
}

fn regex(field: Option<&str>, pattern: &str) -> VixQuery {
    VixQuery::Regex {
        field: field.map(str::to_string),
        pattern: pattern.to_string(),
    }
}

/// The main synthetic dataset: 10 docs over 2 batches.
///
/// Schema: `_timestamp` i64, `level`/`log`/`svc` utf8, `code` i64,
/// plus the reserved `_o2_id` string column that must not be term-indexed.
/// Term field ids (sorted names): level=0, log=1, svc=2.
/// `log` is full-text; `svc` and `code` are column-store fields.
fn dataset_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("level", DataType::Utf8, true),
        Field::new("log", DataType::Utf8, true),
        Field::new("svc", DataType::Utf8, true),
        Field::new("code", DataType::Int64, false),
        Field::new("_o2_id", DataType::Utf8, true),
    ]))
}

fn dataset_batch(
    schema: &SchemaRef,
    ts: Vec<i64>,
    level: Vec<Option<&str>>,
    log: Vec<Option<&str>>,
    svc: Vec<Option<&str>>,
    code: Vec<i64>,
) -> RecordBatch {
    let rows = ts.len();
    let fill: Vec<Option<&str>> = vec![Some("reserved"); rows];
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(ts)) as ArrayRef,
            Arc::new(StringArray::from(level)),
            Arc::new(StringArray::from(log)),
            Arc::new(StringArray::from(svc)),
            Arc::new(Int64Array::from(code)),
            Arc::new(StringArray::from(fill)),
        ],
    )
    .unwrap()
}

fn dataset_options() -> VixWriterOptions {
    VixWriterOptions {
        fts_field_names: vec!["log".to_string()],
        row_group_size: 128,
        ..Default::default()
    }
}

/// Build the 10-doc dataset with the given options and open a reader on it.
fn build_dataset(opts: VixWriterOptions) -> VixReader {
    let (data, index) = build_dataset_bytes(opts);
    open_built(data, index)
}

/// Build the 10-doc dataset and return the raw `(data, sidecar)` bytes.
fn build_dataset_bytes(opts: VixWriterOptions) -> (Vec<u8>, Option<Vec<u8>>) {
    let schema = dataset_schema();
    let mut writer = VixWriter::new(&schema, opts, false);
    writer
        .push_batch_with_source(
            &dataset_batch(
                &schema,
                vec![1000, 1001, 1002, 1003, 1004, 1005],
                vec![
                    Some("info"),
                    Some("error"),
                    Some("info"),
                    None,
                    Some("warn"),
                    Some("error"),
                ],
                vec![
                    Some("Error connecting to db"),
                    Some("timeout waiting"),
                    Some("user login ok"),
                    Some(""),
                    Some("disk almost full"),
                    Some("error error error"),
                ],
                vec![
                    Some("api"),
                    Some("api"),
                    Some("auth"),
                    Some("auth"),
                    Some("db"),
                    Some("db"),
                ],
                vec![1, 2, 3, 4, 5, 6],
            ),
            &dataset_sources(0..6),
            None,
        )
        .unwrap();
    writer
        .push_batch_with_source(
            &dataset_batch(
                &schema,
                vec![1006, 1007, 1008, 1009],
                vec![Some("info"), Some("warn"), Some("error"), Some("info")],
                vec![
                    Some("Timeout again"),
                    None,
                    Some("db timeout hard"),
                    Some("all good"),
                ],
                vec![Some("api"), Some("web"), Some("web"), Some("api")],
                vec![7, 8, 9, 10],
            ),
            &dataset_sources(6..10),
            None,
        )
        .unwrap();
    writer.finish().unwrap()
}

fn eval_set(reader: &VixReader, query: &VixQuery) -> BTreeSet<u32> {
    let bits = reader.eval(query).unwrap();
    assert_eq!(bits.len() as u64, reader.row_count());
    bits_to_set(&bits)
}

/// Open a `(data, sidecar)` pair as one reader — the standard two-source
/// in-memory open every finished build round-trips through (v3 split).
fn open_built(data: Vec<u8>, index: Option<Vec<u8>>) -> VixReader {
    VixReader::open_with_index(Bytes::from(data), index.map(Bytes::from)).unwrap()
}

/// Finish a writer into its two byte streams and open them as one reader.
fn finish_open(writer: VixWriter) -> VixReader {
    let (data, index) = writer.finish().unwrap();
    open_built(data, index)
}

#[test]
fn roundtrip_metadata_and_fields() {
    let reader = build_dataset(dataset_options());
    assert_eq!(reader.row_count(), 10);
    assert_eq!(reader.row_group_size(), 128);
    // block dictionary: blocks never span fields — the 10-doc dataset's
    // key space (code/level/log/svc value fields + the key-term cluster)
    // yields one block per field group
    assert_eq!(reader.term_row_group_count(), 5);
    // Field ids follow the sorted value-indexed field names — numeric fields
    // included (their canonical value terms carry the id): code=0, level=1,
    // log=2, svc=3.
    assert_eq!(reader.field_id("code"), Some(0));
    assert_eq!(reader.field_id("level"), Some(1));
    assert_eq!(reader.field_id("svc"), Some(3));
    assert!(reader.has_term_capability("code"));
    assert!(reader.has_term_capability("level"));
    assert!(reader.has_term_capability("svc"));
    // The fts field owns id 2 for its tokens but has no raw values: no
    // term capability, so per-field value lookups must not resolve it.
    assert_eq!(reader.field_id("log"), None);
    assert!(!reader.has_term_capability("log"));
    // ... while the compaction schema set still carries it.
    assert_eq!(
        reader.term_field_names(),
        vec!["code", "level", "log", "svc"]
    );
    // Reserved columns are never term-indexed.
    assert_eq!(reader.field_id("_timestamp"), None);
    assert_eq!(reader.field_id("_o2_id"), None);
    assert_eq!(reader.field_id("_original"), None);
    // ... but column-store members are known fields — v2 all-columns:
    // every schema field (the reserved `_o2_id` included) is one.
    assert!(reader.has_field("code"));
    assert!(reader.has_field("_timestamp"));
    assert!(reader.has_field("level"));
    assert!(reader.has_field("_o2_id"));
    assert!(!reader.has_field("missing"));
    assert!(reader.partial_fields().is_empty());
}

#[test]
fn exact_queries_across_batches() {
    let reader = build_dataset(dataset_options());
    assert_eq!(
        eval_set(&reader, &exact("level", "error")),
        docs(&[1, 5, 8])
    );
    assert_eq!(eval_set(&reader, &exact("svc", "api")), docs(&[0, 1, 6, 9]));
    // Same token bytes in another field must not leak.
    assert_eq!(eval_set(&reader, &exact("svc", "error")), docs(&[]));
    assert_eq!(eval_set(&reader, &exact("level", "missing")), docs(&[]));
    // Numeric values live under TAGGED canonical tokens: the raw string
    // probe finds nothing, the tagged one resolves the doc.
    assert_eq!(eval_set(&reader, &exact("code", "1")), docs(&[]));
    assert_eq!(eval_set(&reader, &exact_numeric("code", "1")), docs(&[0]),);
    assert_eq!(eval_set(&reader, &exact_numeric("code", "10")), docs(&[9]),);
    // Unindexed fields are an error, not silence — including the fts field,
    // whose raw whole values are not indexed (tokens only).
    assert!(reader.eval(&exact("_o2_id", "reserved")).is_err());
    assert!(
        reader
            .eval(&exact("log", "Error connecting to db"))
            .is_err()
    );
}

/// `VixQuery::Nothing` — the provably-empty query a caller maps a condition
/// on a file-absent field to: it matches no document and composes exactly
/// under every combinator.
#[test]
fn nothing_matches_no_documents_and_composes() {
    let reader = build_dataset(dataset_options());
    let rows = reader.row_count();

    // alone: an all-false bitmap of row_count length; count is 0
    let bits = reader.eval(&VixQuery::Nothing).unwrap();
    assert_eq!(bits.len() as u64, rows);
    assert_eq!(bits.count_set_bits(), 0);
    assert_eq!(reader.count(&VixQuery::Nothing).unwrap(), 0);

    let errors = exact("level", "error"); // docs 1, 5, 8

    // AND: absorbing — the empty leaf short-circuits the intersection
    assert_eq!(
        eval_set(
            &reader,
            &VixQuery::And(vec![VixQuery::Nothing, errors.clone()])
        ),
        docs(&[])
    );
    assert_eq!(
        reader
            .count(&VixQuery::And(vec![errors.clone(), VixQuery::Nothing]))
            .unwrap(),
        0
    );

    // OR: identity
    assert_eq!(
        eval_set(
            &reader,
            &VixQuery::Or(vec![VixQuery::Nothing, errors.clone()])
        ),
        docs(&[1, 5, 8])
    );

    // NOT: every document
    let not_nothing = VixQuery::Not(Box::new(VixQuery::Nothing));
    assert_eq!(
        reader.eval(&not_nothing).unwrap().count_set_bits() as u64,
        rows
    );
    assert_eq!(reader.count(&not_nothing).unwrap(), rows);

    // nested composition: And[Or[Nothing, hit], Not(Nothing)] == hit
    assert_eq!(
        eval_set(
            &reader,
            &VixQuery::And(vec![
                VixQuery::Or(vec![VixQuery::Nothing, errors.clone()]),
                not_nothing,
            ])
        ),
        docs(&[1, 5, 8])
    );
}

/// `key_term_exists` is the per-file "does ANY document carry this path"
/// probe: exact for every column type, `false` proves the path is NULL in
/// every row, and the never-key-indexed internals report present.
#[test]
fn key_term_exists_probes_the_dictionary() {
    let reader = build_dataset(dataset_options());
    // string fields (term and fts) and numeric fields all carry key terms
    assert!(reader.key_term_exists("level").unwrap());
    assert!(reader.key_term_exists("log").unwrap());
    assert!(reader.key_term_exists("svc").unwrap());
    assert!(reader.key_term_exists("code").unwrap());
    // a path no document carries: proven absent
    assert!(!reader.key_term_exists("client_id").unwrap());
    assert!(!reader.key_term_exists("").unwrap());
    // internal columns are never key-indexed, but every document carries
    // them — they must never classify as absent
    assert!(reader.key_term_exists("_timestamp").unwrap());
    assert!(reader.key_term_exists("_o2_id").unwrap());
    assert!(reader.key_term_exists("_original").unwrap());
    assert!(reader.key_term_exists("_source").unwrap());
}

#[test]
fn token_any_field() {
    let reader = build_dataset(dataset_options());
    // "timeout" appears only as an fts token of `log`.
    assert_eq!(eval_set(&reader, &any_token("timeout")), docs(&[1, 6, 8]));
    // "error" appears as a `level` raw term and a `log` token.
    assert_eq!(eval_set(&reader, &any_token("error")), docs(&[0, 1, 5, 8]));
    // Only the exact token matches, not extensions.
    assert_eq!(eval_set(&reader, &any_token("time")), docs(&[]));
    // Whole values of the fts field are not terms (tokens only).
    assert_eq!(eval_set(&reader, &any_token("Timeout again")), docs(&[]));
}

#[test]
fn prefix_queries() {
    let reader = build_dataset(dataset_options());
    // The lowercased fts tokens "timeout" (docs 1,6,8); whole values of the
    // fts field are not terms.
    assert_eq!(eval_set(&reader, &prefix(None, "time")), docs(&[1, 6, 8]));
    assert_eq!(
        eval_set(&reader, &prefix(Some("svc"), "a")),
        docs(&[0, 1, 2, 3, 6, 9])
    );
    assert_eq!(eval_set(&reader, &prefix(Some("level"), "time")), docs(&[]));
    // Empty prefix selects every doc with any term in that field.
    assert_eq!(
        eval_set(&reader, &prefix(Some("level"), "")),
        docs(&[0, 1, 2, 4, 5, 6, 7, 8, 9])
    );
}

#[test]
fn contains_queries() {
    let reader = build_dataset(dataset_options());
    assert_eq!(
        eval_set(&reader, &contains(None, "meout", false)),
        docs(&[1, 6, 8])
    );
    // Case-insensitive matches "Timeout again" and "Error connecting to db".
    assert_eq!(
        eval_set(&reader, &contains(None, "TIMEOUT", true)),
        docs(&[1, 6, 8])
    );
    assert_eq!(
        eval_set(&reader, &contains(None, "error", true)),
        docs(&[0, 1, 5, 8])
    );
    assert_eq!(
        eval_set(&reader, &contains(Some("level"), "rro", false)),
        docs(&[1, 5, 8])
    );
    assert_eq!(
        eval_set(&reader, &contains(Some("svc"), "e", false)),
        docs(&[7, 8])
    );
}

#[test]
fn regex_queries() {
    let reader = build_dataset(dataset_options());
    // Anchored full-token match, case sensitive.
    assert_eq!(
        eval_set(&reader, &regex(None, "err.*")),
        docs(&[0, 1, 5, 8])
    );
    assert_eq!(
        eval_set(&reader, &regex(Some("svc"), "a(pi|uth)")),
        docs(&[0, 1, 2, 3, 6, 9])
    );
    assert_eq!(
        eval_set(&reader, &regex(Some("level"), "(error|warn)")),
        docs(&[1, 4, 5, 7, 8])
    );
    // "err" alone is not a full-token match anywhere.
    assert_eq!(eval_set(&reader, &regex(None, "err")), docs(&[]));
    assert!(reader.eval(&regex(None, "((")).is_err());
}

#[test]
fn fuzzy_queries() {
    let reader = build_dataset(dataset_options());
    let fuzzy = |token: &str, distance: u8| VixQuery::Fuzzy {
        token: token.to_string(),
        distance,
    };
    assert_eq!(eval_set(&reader, &fuzzy("timeout", 0)), docs(&[1, 6, 8]));
    // One substitution: "timeoot" ~ "timeout".
    assert_eq!(eval_set(&reader, &fuzzy("timeoot", 1)), docs(&[1, 6, 8]));
    assert_eq!(eval_set(&reader, &fuzzy("timeoot", 0)), docs(&[]));
    // Distance 2 reaches "warn" from "wa" (2 insertions) and "db" stays out.
    assert_eq!(eval_set(&reader, &fuzzy("warm", 1)), docs(&[4, 7]));
    assert!(reader.eval(&fuzzy("timeout", 3)).is_err());
}

#[test]
fn boolean_combinators() {
    let reader = build_dataset(dataset_options());
    let all: BTreeSet<u32> = (0..10).collect();
    assert_eq!(eval_set(&reader, &VixQuery::All), all);
    assert_eq!(
        eval_set(
            &reader,
            &VixQuery::And(vec![exact("level", "error"), any_token("timeout")])
        ),
        docs(&[1, 8])
    );
    assert_eq!(
        eval_set(
            &reader,
            &VixQuery::Or(vec![exact("svc", "web"), exact("level", "warn")])
        ),
        docs(&[4, 7, 8])
    );
    assert_eq!(
        eval_set(&reader, &VixQuery::Not(Box::new(exact("level", "error")))),
        docs(&[0, 2, 3, 4, 6, 7, 9])
    );
    // Neutral elements.
    assert_eq!(eval_set(&reader, &VixQuery::And(vec![])), all);
    assert_eq!(eval_set(&reader, &VixQuery::Or(vec![])), docs(&[]));
}

#[test]
fn count_fast_paths() {
    let reader = build_dataset(dataset_options());
    assert_eq!(reader.count(&VixQuery::All).unwrap(), 10);
    // Exact uses the doc_count column (no postings decode).
    assert_eq!(reader.count(&exact("level", "error")).unwrap(), 3);
    assert_eq!(reader.count(&exact("svc", "api")).unwrap(), 4);
    assert_eq!(reader.count(&exact("level", "missing")).unwrap(), 0);
    // numeric values count through their tagged canonical terms; the raw
    // string image counts nothing
    assert_eq!(reader.count(&exact_numeric("code", "1")).unwrap(), 1);
    assert_eq!(reader.count(&exact("code", "1")).unwrap(), 0);
}

#[test]
fn count_matches_eval_on_random_queries() {
    let reader = build_dataset(dataset_options());
    let vocab = [
        "error",
        "info",
        "warn",
        "timeout",
        "db",
        "api",
        "auth",
        "web",
        "user",
        "login",
        "all",
        "good",
        "disk",
        "missing",
        "Timeout again",
    ];
    // per-field lookups only resolve fields with raw value terms — the fts
    // field `log` has tokens only and would error
    let fields = ["level", "svc"];
    let mut rng = StdRng::seed_from_u64(0x5EED);
    let random_leaf = |rng: &mut StdRng| -> VixQuery {
        let token = vocab[rng.random_range(0..vocab.len())];
        match rng.random_range(0..5) {
            0 => exact(fields[rng.random_range(0..fields.len())], token),
            1 => any_token(token),
            2 => prefix(None, &token[..token.len().min(3)]),
            3 => contains(
                None,
                &token[..token.len().min(4)],
                rng.random_range(0..2) == 0,
            ),
            _ => prefix(Some(fields[rng.random_range(0..fields.len())]), &token[..1]),
        }
    };
    for round in 0..20 {
        let query = match rng.random_range(0..4) {
            0 => random_leaf(&mut rng),
            1 => VixQuery::And(vec![random_leaf(&mut rng), random_leaf(&mut rng)]),
            2 => VixQuery::Or(vec![random_leaf(&mut rng), random_leaf(&mut rng)]),
            _ => VixQuery::Not(Box::new(random_leaf(&mut rng))),
        };
        let count = reader.count(&query).unwrap();
        let evaluated = reader.eval(&query).unwrap().count_set_bits() as u64;
        assert_eq!(count, evaluated, "round {round}: {query:?}");
    }
}

#[test]
fn doc_level_dedup() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("m", DataType::Utf8, false),
    ]));
    let mut writer = VixWriter::new(
        &schema,
        VixWriterOptions {
            fts_field_names: vec!["m".to_string()],
            ..Default::default()
        },
        false,
    );
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            // doc 0: same token three times; doc 1: the token once.
            Arc::new(StringArray::from(vec!["dup dup dup", "dup"])),
        ],
    )
    .unwrap();
    writer
        .push_batch_with_source(&batch, &dataset_sources(0..2), None)
        .unwrap();
    let reader = finish_open(writer);
    assert_eq!(eval_set(&reader, &any_token("dup")), docs(&[0, 1]));
    // doc_count column agrees (each doc exactly once in the postings).
    assert_eq!(reader.count(&any_token("dup")).unwrap(), 2);
    // The whole value of the fts field is not a term of its own.
    assert_eq!(eval_set(&reader, &any_token("dup dup dup")), docs(&[]));
}

#[test]
fn multi_row_group_scans() {
    // Size 2,000 unique terms from the production dictionary target so the
    // `w` field spans several blocks without depending on a retired writer
    // row-group option.
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("w", DataType::Utf8, false),
    ]));
    let mut writer = VixWriter::new(
        &schema,
        VixWriterOptions {
            ..Default::default()
        },
        false,
    );
    const VALUE_SUFFIX_BYTES: usize = crate::dict_blocks::BLOCK_TARGET_BYTES / 256;
    let values: Vec<String> = (0..2000)
        .map(|i| format!("w{i:04}{}", "x".repeat(VALUE_SUFFIX_BYTES)))
        .collect();
    for (index, chunk) in values.chunks(500).enumerate() {
        let first = index * 500;
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from_iter_values(
                    (first..first + chunk.len()).map(|i| i as i64 + 1),
                )) as ArrayRef,
                Arc::new(StringArray::from_iter_values(chunk)),
            ],
        )
        .unwrap();
        writer
            .push_batch_with_source(&batch, &dataset_sources(first..first + chunk.len()), None)
            .unwrap();
    }
    let reader = finish_open(writer);
    let field_id = reader.field_id("w").expect("w field id");
    let mut first_key = Vec::new();
    crate::query::write_composite(&mut first_key, values[0].as_bytes(), field_id);
    let mut last_key = Vec::new();
    crate::query::write_composite(&mut last_key, values.last().unwrap().as_bytes(), field_id);
    let dict_index = reader.dict_index().expect("dictionary index");
    let first_block = dict_index
        .predecessor_block(&first_key)
        .expect("first block lookup")
        .expect("first w block");
    let last_block = dict_index
        .predecessor_block(&last_key)
        .expect("last block lookup")
        .expect("last w block");
    assert!(
        last_block > first_block,
        "w values must span dictionary blocks, got block {first_block}..={last_block}"
    );
    // Cross-block prefix scan: w1000..w1999.
    let expected: BTreeSet<u32> = (1000..2000).collect();
    assert_eq!(eval_set(&reader, &prefix(None, "w1")), expected);
    // Point lookups at both edges and in the middle.
    assert_eq!(eval_set(&reader, &exact("w", &values[0])), docs(&[0]));
    assert_eq!(eval_set(&reader, &exact("w", &values[999])), docs(&[999]));
    assert_eq!(eval_set(&reader, &exact("w", &values[1999])), docs(&[1999]));
    // Any-field token lookups also cross blocks.
    assert_eq!(eval_set(&reader, &any_token(&values[1500])), docs(&[1500]));
    // Full-dictionary walks (contains) see every block.
    let all: BTreeSet<u32> = (0..2000).collect();
    assert_eq!(eval_set(&reader, &contains(None, "w", false)), all);
}

#[test]
fn nul_byte_inside_token() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("f", DataType::Utf8, false),
    ]));
    let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec!["ab\0cd", "ab"])),
        ],
    )
    .unwrap();
    writer
        .push_batch_with_source(&batch, &dataset_sources(0..2), None)
        .unwrap();
    let reader = finish_open(writer);

    let nul_exact = VixQuery::Exact {
        field: "f".to_string(),
        token: b"ab\0cd".to_vec(),
    };
    assert_eq!(eval_set(&reader, &nul_exact), docs(&[0]));
    // The embedded \x00 must not truncate matching: "ab" only hits doc 1.
    assert_eq!(eval_set(&reader, &exact("f", "ab")), docs(&[1]));
    assert_eq!(eval_set(&reader, &any_token("ab")), docs(&[1]));
    // Prefix scans see through the NUL.
    assert_eq!(eval_set(&reader, &prefix(None, "ab")), docs(&[0, 1]));
    let nul_prefix = VixQuery::Prefix {
        field: None,
        prefix: b"ab\0c".to_vec(),
    };
    assert_eq!(eval_set(&reader, &nul_prefix), docs(&[0]));
}

/// Owner call 2026-08-12 (performance-first): an oversize raw value is
/// skipped from the term index WITHOUT degrading the field — the index
/// stays authoritative, every other value keeps exact index answers, and
/// the ACCEPTED trade is that an equality probe for the skipped literal
/// itself silently misses its row. Skips surface only through
/// `VixWriterStats::oversize_skipped`.
#[test]
fn oversize_values_skip_without_field_degrade() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("big", DataType::Utf8, false),
        Field::new("ok", DataType::Utf8, false),
    ]));
    let mut writer = VixWriter::new(
        &schema,
        VixWriterOptions {
            max_raw_term_len: 8,
            ..Default::default()
        },
        false,
    );
    let long = "x".repeat(9);
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec![long.as_str(), "short"])),
            Arc::new(StringArray::from(vec!["fine", "fine"])),
        ],
    )
    .unwrap();
    writer
        .push_batch_with_source(&batch, &dataset_sources(0..2), None)
        .unwrap();
    let (bytes, bytes_index, stats) = writer.finish_with_stats().unwrap();
    let reader = open_built(bytes, bytes_index);
    // no field degrade — the whole point of the policy
    assert!(
        reader.partial_fields().is_empty(),
        "oversize must not taint: {:?}",
        reader.partial_fields()
    );
    // the skip is counted (observability for the accepted miss)
    assert_eq!(stats.oversize_skipped, 1);
    // the ACCEPTED MISS: the oversize literal finds nothing, and because
    // the field is not partial the index answer is final (no filter-back)
    assert_eq!(eval_set(&reader, &exact("big", &long)), docs(&[]));
    // every other value answers exactly from the index
    assert_eq!(eval_set(&reader, &exact("big", "short")), docs(&[1]));
    assert_eq!(eval_set(&reader, &exact("ok", "fine")), docs(&[0, 1]));
}

/// #32 invariant, now load-bearing for the 2026-08-12 skip-without-degrade
/// policy: KEY terms are emitted even for the doc whose VALUE was skipped,
/// so `IS [NOT] NULL` (KeyExists) stays EXACT despite the un-indexed value
/// — with no partial marker, the key term is the only thing keeping
/// presence queries correct for those rows. Covers the oversize-string
/// cause and the oversize-numeric-canonical-text cause; neither degrades
/// the field anymore.
#[test]
fn key_terms_survive_oversize_value_skips() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("big", DataType::Utf8, true),
        Field::new("num", DataType::Int64, true),
    ]));
    let mut writer = VixWriter::new(
        &schema,
        VixWriterOptions {
            max_raw_term_len: 8,
            ..Default::default()
        },
        false,
    );
    let long = "x".repeat(9);
    // rows: 0 = oversize string + oversize-canonical number,
    //       1 = short string + null num, 2 = both null
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some(long.as_str()),
                Some("ok"),
                None,
            ])),
            // canonical text "123456789" is 9 bytes > max_raw_term_len 8
            Arc::new(Int64Array::from(vec![Some(123456789), None, None])),
        ],
    )
    .unwrap();
    writer
        .push_batch_with_source(&batch, &dataset_sources(0..3), None)
        .unwrap();
    let (bytes, bytes_index, stats) = writer.finish_with_stats().unwrap();
    let reader = open_built(bytes, bytes_index);
    assert!(
        reader.partial_fields().is_empty(),
        "oversize skips must not degrade any field: {:?}",
        reader.partial_fields()
    );
    assert_eq!(
        stats.oversize_skipped, 2,
        "one oversize string + one oversize numeric canonical text"
    );
    // the invariant: KeyExists sees the skipped-value docs
    let key_exists = |path: &str| VixQuery::KeyExists {
        path: path.to_string(),
    };
    assert_eq!(
        eval_set(&reader, &key_exists("big")),
        docs(&[0, 1]),
        "doc 0's key term must survive its skipped value"
    );
    assert_eq!(
        eval_set(&reader, &key_exists("num")),
        docs(&[0]),
        "doc 0's numeric key term must survive its skipped canonical text"
    );
    // IS NULL = Not(KeyExists): exact complements
    assert_eq!(
        eval_set(&reader, &VixQuery::Not(Box::new(key_exists("big")))),
        docs(&[2])
    );
}

/// REGRESSION (live image .10, "rebuilt files lose match_all"): an fts
/// field's values tokenize regardless of length — `max_raw_term_len` gates
/// RAW whole-value terms only, and tokens are byte-bounded by the
/// tokenizer's own max. An oversize fts value must contribute its tokens
/// and must NOT taint the field into `partial_fields` (any non-empty
/// partial set sends every match_all over the file back to a whole-file
/// scan). Both term derivations must agree byte-identically: the
/// column-driven path (move job) and the source-driven path (compaction
/// rebuild). Raw fields skip the oversize VALUE but no longer degrade the
/// field either (owner call 2026-08-12) — the skipped literal just misses.
#[test]
fn fts_oversize_values_tokenize_without_partial() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("body", DataType::Utf8, true),
        Field::new("tag", DataType::Utf8, true),
    ]));
    let opts = VixWriterOptions {
        fts_field_names: vec!["body".to_string()],
        max_raw_term_len: 8,
        ..Default::default()
    };
    // body row 0 is far beyond max_raw_term_len; its "heartbeat" token must
    // survive while the 70-byte z-run is dropped by the tokenizer's own
    // exclusive 64-byte max — the two bounds are independent
    let long_body = format!("heartbeat {}", "z".repeat(70));
    let long_tag = "y".repeat(9);
    let source = StringArray::from(vec![
        format!("{{\"body\":{long_body:?},\"tag\":{long_tag:?}}}"),
        r#"{"body":"ok ping","tag":"small"}"#.to_string(),
    ]);

    // column-driven derivation (the move-job build)
    let mut writer = VixWriter::new(&schema, opts.clone(), false);
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![2, 1])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some(long_body.as_str()),
                Some("ok ping"),
            ])),
            Arc::new(StringArray::from(vec![
                Some(long_tag.as_str()),
                Some("small"),
            ])),
        ],
    )
    .unwrap();
    writer
        .push_batch_with_source(&batch, &source, None)
        .unwrap();
    let column_driven = finish_open(writer);

    // source-driven derivation (the compaction rebuild) over the same rows
    let mut writer = VixWriter::new(&schema, opts, false);
    writer
        .push_docs_rows(&Int64Array::from(vec![2, 1]), &[], &source, None)
        .unwrap();
    let source_driven = finish_open(writer);

    for (context, reader) in [
        ("column-driven", &column_driven),
        ("source-driven", &source_driven),
    ] {
        // neither field goes partial: fts values tokenize regardless of
        // length, and a raw oversize value is skipped without degrade
        assert!(
            reader.partial_fields().is_empty(),
            "{context}: no field may be partial, got {:?}",
            reader.partial_fields()
        );
        // the raw field's accepted miss + intact exact answers
        assert_eq!(eval_set(reader, &exact("tag", &long_tag)), docs(&[]));
        assert_eq!(eval_set(reader, &exact("tag", "small")), docs(&[1]));
        // the oversize value's tokens are present and queryable — exactly
        // what match_all('heartbeat') consumes
        assert_eq!(
            eval_set(reader, &any_token("heartbeat")),
            docs(&[0]),
            "{context}"
        );
        assert_eq!(
            eval_set(reader, &any_token("ping")),
            docs(&[1]),
            "{context}"
        );
        // ... and the beyond-token-max z-run emitted no token
        assert_eq!(
            eval_set(reader, &any_token(&"z".repeat(70))),
            docs(&[]),
            "{context}"
        );
        // fields table: body stays fts-typed (token-indexed, no raw-value
        // capability), tag keeps its raw-term typing
        assert!(
            reader
                .field_entries()
                .iter()
                .any(|entry| entry.name == "body"
                    && entry.has_type(crate::container::FIELD_TYPE_FTS)),
            "{context}: body must stay fts-typed"
        );
        assert!(!reader.has_term_capability("body"), "{context}");
        assert!(reader.has_term_capability("tag"), "{context}");
    }

    // byte-identical term behavior across the two derivations: same keys,
    // same doc counts, same postings
    let dump = |reader: &VixReader| {
        let mut terms: Vec<(Vec<u8>, u64, Vec<u32>)> = Vec::new();
        reader
            .for_each_term(&mut |key, doc_count, ids| {
                terms.push((key.to_vec(), doc_count, ids.to_vec()));
                Ok(())
            })
            .unwrap();
        terms
    };
    assert_eq!(dump(&column_driven), dump(&source_driven));
}

#[test]
fn timestamp_range_boundaries() {
    let reader = build_dataset(dataset_options());
    // Inclusive lower bound, exclusive upper bound (tantivy parity).
    assert_eq!(
        bits_to_set(&reader.timestamp_range(1001, 1005).unwrap()),
        docs(&[1, 2, 3, 4])
    );
    assert_eq!(
        bits_to_set(&reader.timestamp_range(1000, 1010).unwrap()),
        (0..10).collect()
    );
    assert_eq!(
        bits_to_set(&reader.timestamp_range(1009, 1009).unwrap()),
        docs(&[])
    );
    assert_eq!(
        bits_to_set(&reader.timestamp_range(1009, 1010).unwrap()),
        docs(&[9])
    );
    assert_eq!(
        bits_to_set(&reader.timestamp_range(2000, 3000).unwrap()),
        docs(&[])
    );
}

#[test]
fn read_column_roundtrip() {
    let reader = build_dataset(dataset_options());

    let ts = reader.read_column("_timestamp").unwrap();
    let ts = arrow::compute::cast(&ts, &DataType::Int64).unwrap();
    let ts = ts.as_any().downcast_ref::<Int64Array>().unwrap();
    let got: Vec<i64> = ts.iter().map(|v| v.unwrap()).collect();
    assert_eq!(got, (1000..1010).collect::<Vec<i64>>());

    let code = reader.read_column("code").unwrap();
    let code = arrow::compute::cast(&code, &DataType::Int64).unwrap();
    let code = code.as_any().downcast_ref::<Int64Array>().unwrap();
    let got: Vec<i64> = code.iter().map(|v| v.unwrap()).collect();
    assert_eq!(got, (1..11).collect::<Vec<i64>>());

    let svc = reader.read_column("svc").unwrap();
    let svc = arrow::compute::cast(&svc, &DataType::Utf8).unwrap();
    let svc = svc.as_any().downcast_ref::<StringArray>().unwrap();
    let got: Vec<&str> = svc.iter().map(|v| v.unwrap()).collect();
    assert_eq!(
        got,
        vec![
            "api", "api", "auth", "auth", "db", "db", "api", "web", "web", "api"
        ]
    );

    // v2 all-columns: every schema field is a docs column; only truly
    // unknown names error.
    let level = reader.read_column("level").unwrap();
    let level = arrow::compute::cast(&level, &DataType::Utf8).unwrap();
    let level = level.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(level.value(0), "info");
    assert!(reader.read_column("missing").is_err());
}

#[test]
fn scale_200k_docs_50k_terms() {
    let start = std::time::Instant::now();
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("msg", DataType::Utf8, false),
        Field::new("tag", DataType::Utf8, false),
    ]));
    let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
    const TOTAL: usize = 200_000;
    const TERMS: usize = 50_000;
    const BATCH: usize = 20_000;
    for batch_start in (0..TOTAL).step_by(BATCH) {
        let msg: Vec<String> = (batch_start..batch_start + BATCH)
            .map(|doc| format!("term{:05}", doc % TERMS))
            .collect();
        let tag: Vec<String> = (batch_start..batch_start + BATCH)
            .map(|doc| format!("tag{}", doc % 7))
            .collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from_iter_values(
                    (batch_start..batch_start + BATCH).map(|doc| doc as i64 + 1),
                )) as ArrayRef,
                Arc::new(StringArray::from_iter_values(&msg)),
                Arc::new(StringArray::from_iter_values(&tag)),
            ],
        )
        .unwrap();
        writer
            .push_batch_with_source(
                &batch,
                &dataset_sources(batch_start..batch_start + BATCH),
                None,
            )
            .unwrap();
    }
    let (bytes, bytes_index) = writer.finish().unwrap();
    let reader = open_built(bytes, bytes_index);
    assert_eq!(reader.row_count(), TOTAL as u64);

    // Every doc index d has msg = term{d % 50000}: 4 hits per term.
    assert_eq!(reader.count(&exact("msg", "term12345")).unwrap(), 4);
    assert_eq!(
        eval_set(&reader, &exact("msg", "term12345")),
        docs(&[12345, 62345, 112345, 162345])
    );
    // Prefix over 10 terms => 40 docs.
    assert_eq!(
        reader
            .eval(&prefix(Some("msg"), "term0123"))
            .unwrap()
            .count_set_bits(),
        40
    );
    // Docs with d % 7 == 3 in [0, 200000): ceil((200000 - 3) / 7) = 28571.
    assert_eq!(reader.count(&exact("tag", "tag3")).unwrap(), 28571);
    assert_eq!(
        eval_set(&reader, &any_token("term00000")),
        docs(&[0, 50000, 100000, 150000])
    );
    assert!(
        start.elapsed() < std::time::Duration::from_secs(30),
        "scale test too slow: {:?}",
        start.elapsed()
    );
}

/// A puffin container whose `version` property is not the supported value —
/// different or absent — must fail with the single clear "unsupported .vix
/// format (version X, reader supports 3)" error, on both the reader and the
/// docs-scan entry points. `version` is the format's one evolution
/// discriminator; there is no other gate.
#[test]
fn unsupported_version_containers_are_rejected() {
    use crate::container::build_container;

    // a different `version` value fails, naming both sides
    let bytes = build_container(
        vec![
            ("version".to_string(), "1".to_string()),
            ("row_count".to_string(), "10".to_string()),
            ("term_count".to_string(), "0".to_string()),
            ("fields".to_string(), "[]".to_string()),
            ("partial_fields".to_string(), "[]".to_string()),
        ],
        vec![],
    )
    .unwrap();

    let err = VixReader::open(Bytes::from(bytes.clone()))
        .err()
        .expect("containers with an unsupported version must be rejected");
    assert!(
        err.to_string().contains("unsupported .vix format")
            && err.to_string().contains("\"1\"")
            && err.to_string().contains("reader supports 3"),
        "unexpected error: {err}"
    );
    let err = crate::VixDocs::open(Bytes::from(bytes)).unwrap_err();
    assert!(
        err.to_string().contains("unsupported .vix format"),
        "unexpected error: {err}"
    );

    // a future version fails the same way, naming the value
    let bytes = build_container(vec![("version".to_string(), "99".to_string())], vec![]).unwrap();
    let err = VixReader::open(Bytes::from(bytes))
        .err()
        .expect("unknown versions must be rejected");
    assert!(
        err.to_string().contains("unsupported .vix format") && err.to_string().contains("99"),
        "unexpected error: {err}"
    );

    // no `version` property at all fails too
    let bytes = build_container(vec![("row_count".to_string(), "0".to_string())], vec![]).unwrap();
    let err = VixReader::open(Bytes::from(bytes))
        .err()
        .expect("containers without a version property must be rejected");
    assert!(
        err.to_string().contains("unsupported .vix format")
            && err.to_string().contains("no version property"),
        "unexpected error: {err}"
    );
}

/// Read compat: extra properties a reader does not know must be ignored
/// generically. Files written before the `format` property was retired carry
/// both `version="2"` and `format="core-v2"` — since the reader checks only
/// `version`, they open and answer queries with zero special-casing (this
/// covers every existing on-disk file, e.g. the benchmark data).
#[test]
fn extra_unknown_properties_are_ignored() {
    let (data, index) = build_docs_dataset_bytes(false);
    let bytes = repack_with_properties(data, |props| {
        props.push(("format".to_string(), "core-v2".to_string()));
        props.push(("x-future-hint".to_string(), "whatever".to_string()));
    });

    let reader = open_built(bytes.clone(), index.clone());
    assert_eq!(reader.row_count(), 10);
    assert_eq!(
        eval_set(&reader, &exact("level", "error")),
        docs(&[1, 5, 8])
    );

    let docs_handle = crate::VixDocs::open(Bytes::from(bytes)).unwrap();
    assert_eq!(docs_handle.row_count(), 10);
}

/// A container carrying an extra blob with an unknown type id still opens
/// and answers queries: blob type ids are plain strings, blobs are matched
/// by `(blob_tag, type id)` and everything unrecognized is skipped — the
/// envelope tolerates additions.
#[test]
fn unknown_blob_types_are_ignored() {
    use crate::container::{BlobHandle, build_container, parse_container};

    let (data, index) = build_docs_dataset_bytes(false);
    let data = Bytes::from(data);
    let container = parse_container(&data).unwrap();
    let properties: Vec<(String, String)> = container
        .properties
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let blob_bytes = |handle: Option<BlobHandle>| match handle {
        Some(BlobHandle::Mem(bytes)) => bytes.to_vec(),
        other => panic!("expected an in-memory blob, got {other:?}"),
    };
    let bytes = build_container(
        properties,
        vec![
            // an unknown blob FIRST, so recognition cannot rely on order
            ("some-future-blob-v9", "future", b"opaque bytes".to_vec()),
            ("o2-vix-docs-v1", "docs", blob_bytes(container.docs)),
        ],
    )
    .unwrap();

    let reader = open_built(bytes, index);
    assert_eq!(
        eval_set(&reader, &exact("level", "error")),
        docs(&[1, 5, 8])
    );
    assert_eq!(reader.read_source(&[1]).unwrap().len(), 1);
}

#[test]
fn open_rejects_malformed_bytes() {
    assert!(VixReader::open(Bytes::new()).is_err());
    assert!(VixReader::open(Bytes::from_static(b"not a vix file")).is_err());

    let (bytes, index) = build_dataset_bytes(dataset_options());
    // Truncation must produce an error, never a panic.
    let truncated = Bytes::from(bytes[..bytes.len() / 2].to_vec());
    assert!(VixReader::open(truncated).is_err());
    // Corrupting the footer magic must fail cleanly too.
    let mut corrupt = bytes.clone();
    let last = corrupt.len() - 1;
    corrupt[last] ^= 0xFF;
    assert!(VixReader::open(Bytes::from(corrupt)).is_err());
    // A truncated/corrupted SIDECAR fails the pair open the same way.
    let index = index.expect("indexed dataset has a sidecar");
    let cut = Bytes::from(index[..index.len() / 2].to_vec());
    assert!(VixReader::open_with_index(Bytes::from(bytes), Some(cut)).is_err());
}

#[test]
fn read_column_rows_point_reads() {
    let reader = build_dataset(dataset_options());
    assert!(reader.has_column_store_field("svc"));
    assert!(reader.has_column_store_field("code"));
    assert!(reader.has_column_store_field("_timestamp"));
    // v2 all-columns: every schema field is a docs column
    assert!(reader.has_column_store_field("level"));
    assert!(!reader.has_column_store_field("missing"));

    // svc values at rows 1, 4, 8 (spanning both pushed batches)
    let col = reader.read_column_rows("svc", &[1, 4, 8]).unwrap();
    let col = arrow::compute::cast(&col, &DataType::Utf8).unwrap();
    let col = col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap()
        .clone();
    assert_eq!(col.len(), 3);
    assert_eq!(col.value(0), "api");
    assert_eq!(col.value(1), "db");
    assert_eq!(col.value(2), "web");

    // _timestamp point reads keep ascending row order and dedupe
    let ts = reader
        .read_column_rows("_timestamp", &[9, 0, 9, 5])
        .unwrap();
    let ts = arrow::compute::cast(&ts, &DataType::Int64).unwrap();
    let ts = ts.as_any().downcast_ref::<Int64Array>().unwrap().clone();
    assert_eq!(ts.len(), 3);
    assert_eq!(ts.value(0), 1000);
    assert_eq!(ts.value(1), 1005);
    assert_eq!(ts.value(2), 1009);

    // empty selection: type-correct empty array
    let empty = reader.read_column_rows("code", &[]).unwrap();
    assert_eq!(empty.len(), 0);

    // errors: unknown field and out-of-range row (v2 all-columns: every
    // schema field, `level` included, is point-readable)
    assert!(reader.read_column_rows("level", &[0]).is_ok());
    assert!(reader.read_column_rows("missing", &[0]).is_err());
    assert!(reader.read_column_rows("svc", &[10]).is_err());
}

// ---------------------------------------------------------------------------
// docs-dataset tests (key terms, `_source`/`_original`, docs columns)
// ---------------------------------------------------------------------------

use crate::{
    SOURCE_COL_NAME,
    query::{KEY_FIELD_ID, MAX_REAL_FIELD_ID},
};

/// The docs synthetic dataset: 10 docs over 2 batches.
///
/// Schema: `_timestamp` i64 non-null, `level`/`log` utf8 nullable, `svc`
/// utf8 non-null, `code` i64 nullable, plus the internal `_o2_id`.
/// Options (shared with the main dataset): `log` is fts, `svc`/`code` are
/// column-store. Term field ids (sorted names): level=0, log=1, svc=2.
///
/// Key-existence ground truth: level 9/10 (doc 3 null), log 9/10 (doc 7
/// null; doc 3 is the *empty string*, which counts), svc 10/10 (dense),
/// code 8/10 (docs 2 and 5 null); `_timestamp`/`_o2_id` are internal (no
/// key terms).
fn docs_dataset_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("level", DataType::Utf8, true),
        Field::new("log", DataType::Utf8, true),
        Field::new("svc", DataType::Utf8, false),
        Field::new("code", DataType::Int64, true),
        Field::new("_o2_id", DataType::Utf8, true),
    ]))
}

fn docs_dataset_batch(
    schema: &SchemaRef,
    ts: Vec<i64>,
    level: Vec<Option<&str>>,
    log: Vec<Option<&str>>,
    svc: Vec<&str>,
    code: Vec<Option<i64>>,
) -> RecordBatch {
    let rows = ts.len();
    let ids: Vec<Option<&str>> = vec![Some("reserved"); rows];
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(ts)) as ArrayRef,
            Arc::new(StringArray::from(level)),
            Arc::new(StringArray::from(log)),
            Arc::new(StringArray::from(svc)),
            Arc::new(Int64Array::from(code)),
            Arc::new(StringArray::from(ids)),
        ],
    )
    .unwrap()
}

/// Per-row `_source` strings for global docs `range`: `{"i":N}`.
fn dataset_sources(range: std::ops::Range<usize>) -> StringArray {
    StringArray::from_iter_values(range.map(|i| format!("{{\"i\":{i}}}")))
}

/// Per-row `_original` strings for global docs `range`: `orig-N`.
fn dataset_originals(range: std::ops::Range<usize>) -> StringArray {
    StringArray::from_iter_values(range.map(|i| format!("orig-{i}")))
}

fn build_docs_dataset_bytes(store_original: bool) -> (Vec<u8>, Option<Vec<u8>>) {
    let schema = docs_dataset_schema();
    let mut writer = VixWriter::new(&schema, dataset_options(), store_original);
    let batch1 = docs_dataset_batch(
        &schema,
        vec![1000, 1001, 1002, 1003, 1004, 1005],
        vec![
            Some("info"),
            Some("error"),
            Some("info"),
            None,
            Some("warn"),
            Some("error"),
        ],
        vec![
            Some("Error connecting to db"),
            Some("timeout waiting"),
            Some("user login ok"),
            Some(""),
            Some("disk almost full"),
            Some("error error error"),
        ],
        vec!["api", "api", "auth", "auth", "db", "db"],
        vec![Some(1), Some(2), None, Some(4), Some(5), None],
    );
    let sources1 = dataset_sources(0..6);
    let originals1 = dataset_originals(0..6);
    writer
        .push_batch_with_source(&batch1, &sources1, store_original.then_some(&originals1))
        .unwrap();
    let batch2 = docs_dataset_batch(
        &schema,
        vec![1006, 1007, 1008, 1009],
        vec![Some("info"), Some("warn"), Some("error"), Some("info")],
        vec![
            Some("Timeout again"),
            None,
            Some("db timeout hard"),
            Some("all good"),
        ],
        vec!["api", "web", "web", "api"],
        vec![Some(7), Some(8), Some(9), Some(10)],
    );
    let sources2 = dataset_sources(6..10);
    let originals2 = dataset_originals(6..10);
    writer
        .push_batch_with_source(&batch2, &sources2, store_original.then_some(&originals2))
        .unwrap();
    writer.finish().unwrap()
}

fn build_docs_dataset(store_original: bool) -> VixReader {
    let (data, index) = build_docs_dataset_bytes(store_original);
    open_built(data, index)
}

fn key_exists_set(reader: &VixReader, path: &str) -> BTreeSet<u32> {
    let bits = reader.key_exists(path).unwrap();
    assert_eq!(bits.len() as u64, reader.row_count());
    bits_to_set(&bits)
}

fn as_string_array(array: &dyn Array) -> StringArray {
    let array = arrow::compute::cast(array, &DataType::Utf8).unwrap();
    array
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .clone()
}

fn as_i64_array(array: &dyn Array) -> Int64Array {
    let array = arrow::compute::cast(array, &DataType::Int64).unwrap();
    array.as_any().downcast_ref::<Int64Array>().unwrap().clone()
}

#[test]
fn format_detection_and_metadata() {
    let reader = build_docs_dataset(false);
    assert_eq!(reader.row_count(), 10);
    assert_eq!(reader.row_group_size(), 128);
    assert_eq!(reader.field_id("code"), Some(0));
    assert!(reader.has_term_capability("code"));
    assert_eq!(reader.field_id("level"), Some(1));
    // fts fields hold tokens only: no term capability, no per-field lookups.
    assert_eq!(reader.field_id("log"), None);
    assert!(!reader.has_term_capability("log"));
    assert_eq!(reader.field_id("svc"), Some(3));
    assert!(reader.has_term_capability("svc"));
    assert_eq!(
        reader.term_field_names(),
        vec!["code", "level", "log", "svc"]
    );
    assert_eq!(reader.field_id("_timestamp"), None);
    assert!(reader.has_field("code"));
    assert!(reader.has_field("_timestamp"));
    // `_source`/`_original` are container columns, not fields.
    assert!(!reader.has_field(SOURCE_COL_NAME));
    assert!(!reader.has_field("_original"));
    assert!(!reader.has_column_store_field(SOURCE_COL_NAME));
    assert!(reader.partial_fields().is_empty());
}

#[test]
fn container_properties_match_spec() {
    let (bytes, index) = build_docs_dataset_bytes(true);
    let meta = puffin::reader::parse_puffin_footer_from_bytes(&bytes).unwrap();
    let index_meta =
        puffin::reader::parse_puffin_footer_from_bytes(index.as_deref().expect("sidecar")).unwrap();

    // DATA object: data-descriptive properties only (v3 split).
    let props = &meta.properties;
    assert_eq!(props["version"], "3");
    assert!(!props.contains_key("format"));
    assert_eq!(props["row_count"], "10");
    assert_eq!(props["row_group_size"], "128");
    // index-descriptive properties must NOT leak onto the data object
    for foreign in [
        "tokenizer",
        "dict_layout",
        "key_layout",
        "term_count",
        "fields",
    ] {
        assert!(
            !props.contains_key(foreign),
            "data object carries {foreign:?}"
        );
    }
    // `columns`: the docs-column field list minus `_source`/`_original` —
    // field presence WITH per-column present-row counts (H2), readable
    // without the sidecar. v2 all-columns: EVERY schema field is a docs
    // column. Counts: 10 rows total; code has 2 nulls, level/log 1 each.
    let columns: serde_json::Value = serde_json::from_str(&props["columns"]).unwrap();
    assert_eq!(
        columns,
        serde_json::json!([
            ["_timestamp", 10],
            ["_o2_id", 10],
            ["code", 8],
            ["level", 9],
            ["log", 9],
            ["svc", 10]
        ])
    );

    // INDEX sidecar: index-descriptive properties.
    let index_props = &index_meta.properties;
    assert_eq!(index_props["version"], "3");
    assert_eq!(index_props["row_count"], "10");
    assert_eq!(index_props["tokenizer"], "o2-v2");
    assert_eq!(index_props["dict_layout"], "blocks");
    assert_eq!(index_props["key_layout"], "fid_v2");
    assert_eq!(index_props["partial_fields"], "[]");
    assert!(index_props["term_count"].parse::<u64>().unwrap() > 0);

    // `fields`: value-indexed fields sorted by name (index == field id) —
    // the numeric `code` included, its canonical values are term-indexed —
    // then the stored-only entries. Key terms and `_source`/`_original` get
    // none; the fts field carries `fts` only (tokens, no raw values). v2
    // all-columns: every field additionally carries `cs`.
    let fields: serde_json::Value = serde_json::from_str(&index_props["fields"]).unwrap();
    assert_eq!(
        fields,
        serde_json::json!([
            {"name": "code", "types": ["term", "cs"]},
            {"name": "level", "types": ["term", "cs"]},
            {"name": "log", "types": ["fts", "cs"]},
            {"name": "svc", "types": ["term", "cs"]},
            {"name": "_timestamp", "types": ["cs"]},
            {"name": "_o2_id", "types": ["cs"]},
        ])
    );

    // Blob split: the data object carries ONLY `docs`; the sidecar carries
    // the index blobs with the small, hot ones (`dict`) at the tail next
    // to the footer. Readers locate blobs by tag/offset, never by position.
    let tags = |meta: &puffin::PuffinMeta| -> Vec<(String, String)> {
        meta.blobs
            .iter()
            .map(|blob| (blob.blob_type.clone(), blob.properties["blob_tag"].clone()))
            .collect()
    };
    assert_eq!(
        tags(&meta),
        vec![
            ("o2-vix-docs-v1".to_string(), "docs".to_string()),
            ("o2-vix-stats-v1".to_string(), "stats".to_string()),
        ]
    );
    assert_eq!(
        tags(&index_meta),
        vec![
            ("o2-vix-terms-v1".to_string(), "terms".to_string()),
            (
                "o2-vix-dictblocks-v1".to_string(),
                "dict_blocks".to_string()
            ),
            ("o2-vix-dict-v2".to_string(), "dict".to_string()),
        ]
    );
}

#[test]
fn term_queries_over_docs_dataset() {
    let reader = build_docs_dataset(false);
    assert_eq!(
        eval_set(&reader, &exact("level", "error")),
        docs(&[1, 5, 8])
    );
    assert_eq!(eval_set(&reader, &exact("svc", "api")), docs(&[0, 1, 6, 9]));
    assert_eq!(eval_set(&reader, &any_token("timeout")), docs(&[1, 6, 8]));
    assert_eq!(eval_set(&reader, &prefix(None, "time")), docs(&[1, 6, 8]));
    assert_eq!(
        eval_set(&reader, &contains(None, "TIMEOUT", true)),
        docs(&[1, 6, 8])
    );
    assert_eq!(
        eval_set(&reader, &regex(Some("svc"), "a(pi|uth)")),
        docs(&[0, 1, 2, 3, 6, 9])
    );
    assert_eq!(
        eval_set(
            &reader,
            &VixQuery::And(vec![exact("level", "error"), any_token("timeout")])
        ),
        docs(&[1, 8])
    );
    assert_eq!(
        eval_set(&reader, &VixQuery::Not(Box::new(exact("level", "error")))),
        docs(&[0, 2, 3, 4, 6, 7, 9])
    );
    assert_eq!(reader.count(&exact("level", "error")).unwrap(), 3);
}

#[test]
fn key_exists_bitmaps() {
    let reader = build_docs_dataset(false);
    // level: doc 3 is null.
    assert_eq!(
        key_exists_set(&reader, "level"),
        docs(&[0, 1, 2, 4, 5, 6, 7, 8, 9])
    );
    // log: doc 7 is null; doc 3 is the *empty string*, which exists.
    assert_eq!(
        key_exists_set(&reader, "log"),
        docs(&[0, 1, 2, 3, 4, 5, 6, 8, 9])
    );
    // svc: present in every doc (dense-elided term).
    assert_eq!(key_exists_set(&reader, "svc"), (0..10).collect());
    // code: a non-string column; docs 2 and 5 are null.
    assert_eq!(
        key_exists_set(&reader, "code"),
        docs(&[0, 1, 3, 4, 6, 7, 8, 9])
    );
    // Unknown paths and internal columns have no key terms.
    assert_eq!(key_exists_set(&reader, "missing"), docs(&[]));
    assert_eq!(key_exists_set(&reader, "_timestamp"), docs(&[]));
    assert_eq!(key_exists_set(&reader, "_o2_id"), docs(&[]));

    // KeyExists through eval()/count().
    let query = VixQuery::KeyExists {
        path: "code".to_string(),
    };
    assert_eq!(eval_set(&reader, &query), docs(&[0, 1, 3, 4, 6, 7, 8, 9]));
    assert_eq!(reader.count(&query).unwrap(), 8);
    assert_eq!(
        eval_set(
            &reader,
            &VixQuery::And(vec![
                VixQuery::Not(Box::new(VixQuery::KeyExists {
                    path: "code".to_string()
                })),
                VixQuery::All,
            ])
        ),
        docs(&[2, 5])
    );
}

#[test]
fn keys_with_prefix_listing() {
    let reader = build_docs_dataset(false);
    // Full coverage listing, ascending path order, true doc counts.
    assert_eq!(
        reader.keys_with_prefix("").unwrap(),
        vec![
            ("code".to_string(), 8),
            ("level".to_string(), 9),
            ("log".to_string(), 9),
            ("svc".to_string(), 10),
        ]
    );
    assert_eq!(
        reader.keys_with_prefix("l").unwrap(),
        vec![("level".to_string(), 9), ("log".to_string(), 9)]
    );
    assert_eq!(
        reader.keys_with_prefix("lo").unwrap(),
        vec![("log".to_string(), 9)]
    );
    assert_eq!(reader.keys_with_prefix("svc").unwrap().len(), 1);
    assert!(reader.keys_with_prefix("zz").unwrap().is_empty());
    // Value terms never leak into key listings ("api" is a svc value).
    assert!(reader.keys_with_prefix("api").unwrap().is_empty());
}

#[test]
fn value_scans_exclude_key_terms() {
    let reader = build_docs_dataset(false);
    // No *value* equals or contains the path names — key terms must not
    // leak into token-level scans.
    assert_eq!(eval_set(&reader, &any_token("svc")), docs(&[]));
    assert_eq!(eval_set(&reader, &any_token("code")), docs(&[]));
    assert_eq!(eval_set(&reader, &any_token("level")), docs(&[]));
    assert_eq!(eval_set(&reader, &prefix(None, "lev")), docs(&[]));
    assert_eq!(eval_set(&reader, &contains(None, "vc", false)), docs(&[]));
    assert_eq!(eval_set(&reader, &regex(None, "svc")), docs(&[]));
    assert_eq!(
        eval_set(
            &reader,
            &VixQuery::Fuzzy {
                token: "svc".to_string(),
                distance: 0,
            }
        ),
        docs(&[])
    );
    // ... while real values in the same byte range keep matching: the
    // fts token "connecting" and the key term "code" share the "co" prefix.
    assert_eq!(eval_set(&reader, &prefix(None, "co")), docs(&[0]));
    assert_eq!(
        eval_set(&reader, &contains(None, "onnect", false)),
        docs(&[0])
    );
}

#[test]
fn dense_elision_roundtrip() {
    // `env` is a raw value present in every doc (dense value term) and a
    // non-null column in every doc (dense key term); `level` is the sparse
    // control with values in 4 of 6 docs.
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("env", DataType::Utf8, false),
        Field::new("level", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])) as ArrayRef,
            Arc::new(StringArray::from(vec!["prod"; 6])),
            Arc::new(StringArray::from(vec![
                Some("info"),
                Some("info"),
                None,
                Some("warn"),
                Some("info"),
                None,
            ])),
        ],
    )
    .unwrap();
    let sources = dataset_sources(0..6);
    let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
    writer
        .push_batch_with_source(&batch, &sources, None)
        .unwrap();
    let reader = finish_open(writer);

    // Field ids (sorted string fields): env=0, level=1.
    // Dense value term and dense key terms: postings elided to zero bytes.
    assert_eq!(reader.debug_postings_len(b"prod", 0).unwrap(), Some(0));
    assert_eq!(
        reader.debug_postings_len(b"env", KEY_FIELD_ID).unwrap(),
        Some(0)
    );
    // Sparse terms keep real postings.
    let sparse_value = reader.debug_postings_len(b"info", 1).unwrap().unwrap();
    assert!(sparse_value > 0, "sparse postings must not be elided");
    let sparse_key = reader
        .debug_postings_len(b"level", KEY_FIELD_ID)
        .unwrap()
        .unwrap();
    assert!(sparse_key > 0, "sparse key postings must not be elided");
    assert_eq!(reader.debug_postings_len(b"missing", 0).unwrap(), None);

    // Elided terms still answer everything: the reader synthesizes the
    // all-ones bitmap and `doc_count` stays exact.
    let all: BTreeSet<u32> = (0..6).collect();
    assert_eq!(eval_set(&reader, &exact("env", "prod")), all);
    assert_eq!(reader.count(&exact("env", "prod")).unwrap(), 6);
    assert_eq!(key_exists_set(&reader, "env"), all);
    assert_eq!(
        reader
            .count(&VixQuery::KeyExists {
                path: "env".to_string()
            })
            .unwrap(),
        6
    );
    assert_eq!(
        eval_set(&reader, &VixQuery::Not(Box::new(exact("env", "prod")))),
        docs(&[])
    );
    assert_eq!(
        eval_set(
            &reader,
            &VixQuery::And(vec![exact("env", "prod"), exact("level", "warn")])
        ),
        docs(&[3])
    );
    // Dense + sparse union through one postings read.
    assert_eq!(
        eval_set(
            &reader,
            &VixQuery::Or(vec![exact("env", "prod"), exact("level", "info")])
        ),
        all
    );
    assert_eq!(eval_set(&reader, &exact("level", "info")), docs(&[0, 1, 4]));
    assert_eq!(key_exists_set(&reader, "level"), docs(&[0, 1, 3, 4]));
}

#[test]
fn read_source_point_reads() {
    let reader = build_docs_dataset(false);
    let sources = reader.read_source(&[0]).unwrap();
    assert_eq!(sources.len(), 1);
    assert_eq!(sources.value(0), "{\"i\":0}");

    // Duplicates collapse; results come back in ascending row order.
    let sources = reader.read_source(&[9, 0, 9, 5]).unwrap();
    let got: Vec<&str> = sources.iter().map(|v| v.unwrap()).collect();
    assert_eq!(got, vec!["{\"i\":0}", "{\"i\":5}", "{\"i\":9}"]);

    // Every row, exact strings back.
    let all_rows: Vec<u64> = (0..10).collect();
    let sources = reader.read_source(&all_rows).unwrap();
    let got: Vec<String> = sources.iter().map(|v| v.unwrap().to_string()).collect();
    let expected: Vec<String> = (0..10).map(|i| format!("{{\"i\":{i}}}")).collect();
    assert_eq!(got, expected);

    assert_eq!(reader.read_source(&[]).unwrap().len(), 0);
    assert!(reader.read_source(&[10]).is_err());
}

#[test]
fn docs_column_reads() {
    let reader = build_docs_dataset(true);

    let ts = as_i64_array(reader.read_docs_column("_timestamp").unwrap().as_ref());
    let got: Vec<i64> = ts.iter().map(|v| v.unwrap()).collect();
    assert_eq!(got, (1000..1010).collect::<Vec<i64>>());

    let svc = as_string_array(reader.read_docs_column("svc").unwrap().as_ref());
    let got: Vec<&str> = svc.iter().map(|v| v.unwrap()).collect();
    assert_eq!(
        got,
        vec![
            "api", "api", "auth", "auth", "db", "db", "api", "web", "web", "api"
        ]
    );

    // Nulls in a stored column survive the round trip.
    let code = as_i64_array(reader.read_docs_column("code").unwrap().as_ref());
    assert_eq!(code.len(), 10);
    assert!(code.is_null(2));
    assert!(code.is_null(5));
    assert_eq!(code.value(9), 10);

    // `_source` and `_original` are readable as docs columns too.
    let source = as_string_array(reader.read_docs_column(SOURCE_COL_NAME).unwrap().as_ref());
    assert_eq!(source.value(3), "{\"i\":3}");
    let original = as_string_array(reader.read_docs_column("_original").unwrap().as_ref());
    let got: Vec<&str> = original.iter().map(|v| v.unwrap()).collect();
    let expected: Vec<String> = (0..10).map(|i| format!("orig-{i}")).collect();
    assert_eq!(got, expected);

    // Point reads.
    let code = as_i64_array(
        reader
            .read_docs_column_rows("code", &[2, 9])
            .unwrap()
            .as_ref(),
    );
    assert_eq!(code.len(), 2);
    assert!(code.is_null(0));
    assert_eq!(code.value(1), 10);
    let original = as_string_array(
        reader
            .read_docs_column_rows("_original", &[3])
            .unwrap()
            .as_ref(),
    );
    assert_eq!(original.value(0), "orig-3");
    assert_eq!(reader.read_docs_column_rows("svc", &[]).unwrap().len(), 0);

    // v2 all-columns: `level` is a docs column too; unknown columns error.
    let level = as_string_array(reader.read_docs_column("level").unwrap().as_ref());
    assert_eq!(level.value(0), "info");
    assert!(level.is_null(3));
    assert!(reader.read_docs_column("missing").is_err());
    assert!(reader.read_docs_column_rows("svc", &[10]).is_err());
}

#[test]
fn column_reads_route_to_docs() {
    let reader = build_docs_dataset(false);

    // read_column / read_column_rows / timestamp_range keep working.
    let svc = as_string_array(reader.read_column("svc").unwrap().as_ref());
    assert_eq!(svc.len(), 10);
    assert_eq!(svc.value(7), "web");
    let code = as_i64_array(reader.read_column_rows("code", &[9, 0]).unwrap().as_ref());
    assert_eq!(code.len(), 2);
    assert_eq!(code.value(0), 1);
    assert_eq!(code.value(1), 10);
    assert_eq!(reader.read_column_rows("svc", &[]).unwrap().len(), 0);
    assert!(reader.has_column_store_field("svc"));
    assert!(reader.has_column_store_field("_timestamp"));
    // v2 all-columns: `level` is a docs column like every schema field
    assert!(reader.has_column_store_field("level"));
    assert!(reader.read_column("level").is_ok());
    assert!(reader.read_column_rows("svc", &[10]).is_err());

    assert_eq!(
        bits_to_set(&reader.timestamp_range(1001, 1005).unwrap()),
        docs(&[1, 2, 3, 4])
    );
    assert_eq!(
        bits_to_set(&reader.timestamp_range(1009, 1010).unwrap()),
        docs(&[9])
    );
    assert_eq!(
        bits_to_set(&reader.timestamp_range(2000, 3000).unwrap()),
        docs(&[])
    );
}

#[test]
fn original_present_and_absent() {
    // store_original = false: no `_original` docs column at all.
    let reader = build_docs_dataset(false);
    assert!(reader.read_docs_column("_original").is_err());
    assert!(reader.read_docs_column_rows("_original", &[0]).is_err());

    // store_original = true but a batch without originals: nulls.
    let schema = docs_dataset_schema();
    let mut writer = VixWriter::new(&schema, dataset_options(), true);
    let batch = docs_dataset_batch(
        &schema,
        vec![1, 2],
        vec![Some("a"), Some("b")],
        vec![None, None],
        vec!["x", "y"],
        vec![None, None],
    );
    let originals = dataset_originals(0..2);
    writer
        .push_batch_with_source(&batch, &dataset_sources(0..2), Some(&originals))
        .unwrap();
    let batch2 = docs_dataset_batch(
        &schema,
        vec![3, 4],
        vec![Some("c"), Some("d")],
        vec![None, None],
        vec!["x", "y"],
        vec![None, None],
    );
    writer
        .push_batch_with_source(&batch2, &dataset_sources(2..4), None)
        .unwrap();
    let reader = finish_open(writer);
    let original = as_string_array(reader.read_docs_column("_original").unwrap().as_ref());
    assert_eq!(original.value(0), "orig-0");
    assert_eq!(original.value(1), "orig-1");
    assert!(original.is_null(2));
    assert!(original.is_null(3));
}

#[test]
fn writer_input_validation() {
    let schema = docs_dataset_schema();
    let batch = docs_dataset_batch(
        &schema,
        vec![1, 2],
        vec![Some("a"), Some("b")],
        vec![None, None],
        vec!["x", "y"],
        vec![None, None],
    );
    let sources = dataset_sources(0..2);
    let originals = dataset_originals(0..2);

    // The schema must carry `_timestamp` and must not carry `_source` or
    // `_original` (they arrive via the arguments).
    let no_ts = Arc::new(Schema::new(vec![Field::new("f", DataType::Utf8, false)]));
    let mut writer = VixWriter::new(&no_ts, VixWriterOptions::default(), false);
    let f_batch = RecordBatch::try_new(
        Arc::clone(&no_ts),
        vec![Arc::new(StringArray::from(vec!["v"])) as ArrayRef],
    )
    .unwrap();
    let err = writer
        .push_batch_with_source(&f_batch, &dataset_sources(0..1), None)
        .unwrap_err();
    assert!(err.to_string().contains("_timestamp"), "{err}");

    // A batch missing `_timestamp` (schema has it) fails at projection.
    let mut writer = VixWriter::new(&schema, dataset_options(), false);
    let err = writer
        .push_batch_with_source(&f_batch, &dataset_sources(0..1), None)
        .unwrap_err();
    assert!(err.to_string().contains("_timestamp"), "{err}");

    // Nulls in `_timestamp` violate the non-null docs column.
    let nullable_ts = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, true),
        Field::new("f", DataType::Utf8, true),
    ]));
    let mut writer = VixWriter::new(&nullable_ts, VixWriterOptions::default(), false);
    let null_ts_batch = RecordBatch::try_new(
        Arc::clone(&nullable_ts),
        vec![
            Arc::new(Int64Array::from(vec![Some(1), None])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("x"), Some("y")])),
        ],
    )
    .unwrap();
    let err = writer
        .push_batch_with_source(&null_ts_batch, &dataset_sources(0..2), None)
        .unwrap_err();
    assert!(err.to_string().contains("_timestamp"), "{err}");

    for reserved in ["_source", "_original"] {
        let bad_schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new(reserved, DataType::Utf8, true),
        ]));
        let bad_batch = RecordBatch::try_new(
            Arc::clone(&bad_schema),
            vec![
                Arc::new(Int64Array::from(vec![1])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("x")])),
            ],
        )
        .unwrap();
        // Rejected at construction (schema) ...
        let mut writer = VixWriter::new(&bad_schema, VixWriterOptions::default(), true);
        let err = writer
            .push_batch_with_source(&bad_batch, &dataset_sources(0..1), None)
            .unwrap_err();
        assert!(err.to_string().contains(reserved), "{err}");
        // ... and per batch (a batch not matching the writer schema).
        let mut writer = VixWriter::new(&schema, dataset_options(), true);
        let err = writer
            .push_batch_with_source(&bad_batch, &dataset_sources(0..1), None)
            .unwrap_err();
        assert!(err.to_string().contains(reserved), "{err}");
    }

    // Source array shape errors.
    let mut writer = VixWriter::new(&schema, dataset_options(), true);
    assert!(
        writer
            .push_batch_with_source(&batch, &dataset_sources(0..1), None)
            .is_err(),
        "source length mismatch must fail"
    );
    let null_source = StringArray::from(vec![Some("{}"), None]);
    assert!(
        writer
            .push_batch_with_source(&batch, &null_source, None)
            .is_err(),
        "null source strings must fail"
    );
    let short_originals = dataset_originals(0..1);
    assert!(
        writer
            .push_batch_with_source(&batch, &sources, Some(&short_originals))
            .is_err(),
        "original length mismatch must fail"
    );

    // Originals against a writer built without store_original.
    let mut writer = VixWriter::new(&schema, dataset_options(), false);
    assert!(
        writer
            .push_batch_with_source(&batch, &sources, Some(&originals))
            .is_err()
    );
    // The failed pushes above must not have corrupted the writer.
    writer
        .push_batch_with_source(&batch, &sources, None)
        .unwrap();
    let reader = finish_open(writer);
    assert_eq!(reader.row_count(), 2);
}

#[test]
fn field_id_overflow_to_partial_fields() {
    // The production cap: id 0xFFFF is the key marker, real ids stop at 0xFFFE.
    assert_eq!(KEY_FIELD_ID, 0xFFFF);
    assert_eq!(MAX_REAL_FIELD_ID, 0xFFFE);

    // Synthetic cap of 2 => real field ids 0..=2, i.e. three term-indexed
    // fields; "d" and "e" overflow.
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Utf8, false),
        Field::new("c", DataType::Utf8, false),
        Field::new("d", DataType::Utf8, false),
        Field::new("e", DataType::Utf8, false),
    ]));
    let mut writer = VixWriter::new_with_field_cap(&schema, VixWriterOptions::default(), false, 2);
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec!["a0", "a1"])),
            Arc::new(StringArray::from(vec!["b0", "b1"])),
            Arc::new(StringArray::from(vec!["c0", "c1"])),
            Arc::new(StringArray::from(vec!["d0", "d1"])),
            Arc::new(StringArray::from(vec!["e0", "e1"])),
        ],
    )
    .unwrap();
    // Overflow is not an error.
    writer
        .push_batch_with_source(&batch, &dataset_sources(0..2), None)
        .unwrap();
    let reader = finish_open(writer);

    // The first three fields got ids; the overflow went to partial_fields.
    assert_eq!(reader.field_id("a"), Some(0));
    assert_eq!(reader.field_id("b"), Some(1));
    assert_eq!(reader.field_id("c"), Some(2));
    assert_eq!(reader.field_id("d"), None);
    assert_eq!(reader.field_id("e"), None);
    assert_eq!(
        reader
            .partial_fields()
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        ["d".to_string(), "e".to_string()].into_iter().collect()
    );

    // Indexed fields answer; overflowed fields error like unindexed ones.
    assert_eq!(eval_set(&reader, &exact("a", "a0")), docs(&[0]));
    assert_eq!(eval_set(&reader, &exact("c", "c1")), docs(&[1]));
    assert!(reader.eval(&exact("d", "d0")).is_err());
    // No value of the overflowed fields is in the dictionary.
    assert_eq!(eval_set(&reader, &any_token("d0")), docs(&[]));

    // Key terms bypass field ids entirely: coverage stays complete.
    assert_eq!(key_exists_set(&reader, "d"), docs(&[0, 1]));
    assert_eq!(key_exists_set(&reader, "e"), docs(&[0, 1]));
    assert_eq!(
        reader.keys_with_prefix("").unwrap(),
        vec![
            ("a".to_string(), 2),
            ("b".to_string(), 2),
            ("c".to_string(), 2),
            ("d".to_string(), 2),
            ("e".to_string(), 2),
        ]
    );
}

#[test]
fn dotted_paths_end_to_end() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("http.status_code", DataType::Utf8, false),
        Field::new("a.b.c", DataType::Utf8, true),
    ]));
    let opts = VixWriterOptions {
        fts_field_names: vec!["http.status_code".to_string()],
        ..Default::default()
    };
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec!["404 not found", "500 server error"])),
            Arc::new(StringArray::from(vec![None, Some("deep")])),
        ],
    )
    .unwrap();

    // raw + fts + key + cs paths.
    let mut writer = VixWriter::new(&schema, opts, false);
    writer
        .push_batch_with_source(&batch, &dataset_sources(0..2), None)
        .unwrap();
    let reader = finish_open(writer);
    // The dotted field is fts: tokens only, per-field lookups do not resolve.
    assert!(
        reader
            .eval(&exact("http.status_code", "404 not found"))
            .is_err()
    );
    assert_eq!(eval_set(&reader, &any_token("404")), docs(&[0]));
    assert_eq!(eval_set(&reader, &prefix(None, "500")), docs(&[1]));
    assert_eq!(key_exists_set(&reader, "a.b.c"), docs(&[1]));
    assert_eq!(key_exists_set(&reader, "http.status_code"), docs(&[0, 1]));
    // The dense dotted key term is elided like any other.
    assert_eq!(
        reader
            .debug_postings_len(b"http.status_code", KEY_FIELD_ID)
            .unwrap(),
        Some(0)
    );
    assert_eq!(
        reader.keys_with_prefix("a.b").unwrap(),
        vec![("a.b.c".to_string(), 1)]
    );
    assert_eq!(
        reader.keys_with_prefix("http").unwrap(),
        vec![("http.status_code".to_string(), 2)]
    );
    let col = as_string_array(
        reader
            .read_docs_column("http.status_code")
            .unwrap()
            .as_ref(),
    );
    assert_eq!(col.value(1), "500 server error");
    assert_eq!(reader.read_source(&[1]).unwrap().value(0), "{\"i\":1}");

    // the same dotted names through any-field token scans and cs reads
    assert_eq!(eval_set(&reader, &any_token("server")), docs(&[1]));
    assert_eq!(
        eval_set(&reader, &contains(None, "error", false)),
        docs(&[1])
    );
    let col = as_string_array(reader.read_column("http.status_code").unwrap().as_ref());
    assert_eq!(col.value(0), "404 not found");
}

#[test]
fn empty_file_and_no_cs_fields() {
    // Zero pushes: the docs blob is still written (it pins the stored
    // schema: `_timestamp` + `_source`).
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("f", DataType::Utf8, true),
    ]));
    let writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
    let (bytes, bytes_index) = writer.finish().unwrap();
    let meta = puffin::reader::parse_puffin_footer_from_bytes(&bytes).unwrap();
    let blobs: Vec<(&str, &str)> = meta
        .blobs
        .iter()
        .map(|blob| {
            (
                blob.blob_type.as_str(),
                blob.properties["blob_tag"].as_str(),
            )
        })
        .collect();
    assert_eq!(blobs, vec![("o2-vix-docs-v1", "docs")]);

    let reader = open_built(bytes, bytes_index);
    assert_eq!(reader.row_count(), 0);
    assert_eq!(reader.eval(&VixQuery::All).unwrap().len(), 0);
    assert_eq!(reader.count(&exact("f", "x")).unwrap(), 0);
    assert_eq!(reader.read_source(&[]).unwrap().len(), 0);
    assert_eq!(reader.read_docs_column(SOURCE_COL_NAME).unwrap().len(), 0);
    assert_eq!(reader.read_docs_column("_timestamp").unwrap().len(), 0);
    assert_eq!(reader.key_exists("f").unwrap().len(), 0);
    assert!(reader.keys_with_prefix("").unwrap().is_empty());
    assert_eq!(reader.timestamp_range(0, i64::MAX).unwrap().len(), 0);

    // A zero-row batch behaves the same.
    let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
    let empty_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef,
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
        ],
    )
    .unwrap();
    writer
        .push_batch_with_source(&empty_batch, &StringArray::from(Vec::<&str>::new()), None)
        .unwrap();
    let reader = finish_open(writer);
    assert_eq!(reader.row_count(), 0);
    assert_eq!(reader.read_source(&[]).unwrap().len(), 0);

    // Rows but no column-store fields: docs = `_timestamp` + `_source`.
    let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![10, 20])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("x"), None])),
        ],
    )
    .unwrap();
    writer
        .push_batch_with_source(&batch, &dataset_sources(0..2), None)
        .unwrap();
    let reader = finish_open(writer);
    assert_eq!(reader.row_count(), 2);
    let source = as_string_array(reader.read_docs_column(SOURCE_COL_NAME).unwrap().as_ref());
    assert_eq!(source.value(1), "{\"i\":1}");
    let sources = reader.read_source(&[0, 1]).unwrap();
    assert_eq!(sources.value(0), "{\"i\":0}");
    assert!(reader.has_column_store_field("_timestamp"));
    assert_eq!(
        bits_to_set(&reader.timestamp_range(10, 20).unwrap()),
        docs(&[0])
    );
    assert_eq!(key_exists_set(&reader, "f"), docs(&[0]));
    assert_eq!(eval_set(&reader, &exact("f", "x")), docs(&[0]));
}

#[test]
fn scale_200k_docs_dense_elision() {
    use puffin::reader::parse_puffin_footer_from_bytes;

    let start = std::time::Instant::now();
    const TOTAL: usize = 200_000;
    const BATCH: usize = 20_000;
    const DENSE_FIELDS: usize = 20;

    let mut fields = vec![Field::new("_timestamp", DataType::Int64, false)];
    for f in 0..DENSE_FIELDS {
        fields.push(Field::new(format!("d{f:02}"), DataType::Utf8, false));
    }
    fields.push(Field::new("sparse", DataType::Utf8, true));
    let schema = Arc::new(Schema::new(fields));

    let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
    for batch_start in (0..TOTAL).step_by(BATCH) {
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(DENSE_FIELDS + 2);
        arrays.push(Arc::new(Int64Array::from_iter_values(
            (batch_start..batch_start + BATCH).map(|doc| doc as i64 + 1),
        )));
        for f in 0..DENSE_FIELDS {
            // One constant value per field: dense in every doc.
            arrays.push(Arc::new(StringArray::from(vec![format!("v{f:02}"); BATCH])));
        }
        arrays.push(Arc::new(StringArray::from_iter(
            (batch_start..batch_start + BATCH).map(|doc| (doc % 1000 == 0).then_some("sv")),
        )));
        let batch = RecordBatch::try_new(Arc::clone(&schema), arrays).unwrap();
        let sources = dataset_sources(batch_start..batch_start + BATCH);
        writer
            .push_batch_with_source(&batch, &sources, None)
            .unwrap();
    }
    let (bytes, bytes_index) = writer.finish().unwrap();

    // Blob sizes straight from the puffin footer.
    let meta = parse_puffin_footer_from_bytes(&bytes).unwrap();
    let mut dict_size = 0u64;
    let mut terms_size = 0u64;
    let mut docs_size = 0u64;
    for blob in &meta.blobs {
        let range = blob.get_offset(None);
        let size = range.end - range.start;
        match blob.properties["blob_tag"].as_str() {
            "dict" => dict_size = size,
            "dict_blocks" => dict_size += size,
            "terms" => terms_size = size,
            "docs" => docs_size = size,
            "stats" => {} // H2 per-column chunk stats (data-object tail blob)
            other => panic!("unexpected blob {other:?}"),
        }
    }
    eprintln!(
        "scale: total {} bytes, dict {dict_size}, terms {terms_size}, docs {docs_size}, \
         built in {:?}",
        bytes.len(),
        start.elapsed()
    );
    // 20 dense value terms + 20 dense key terms are all elided; only the
    // sparse term (200 docs) and its key term carry postings. Without
    // elision the terms blob would hold 41 * ~200k doc ids (tens of MB).
    assert!(
        terms_size < 64 * 1024,
        "terms blob must stay tiny under dense elision, got {terms_size} bytes"
    );
    assert!(docs_size > 0);

    let reader = open_built(bytes, bytes_index);
    assert_eq!(reader.row_count(), TOTAL as u64);

    // Every dense value and key term: postings elided to zero bytes,
    // doc_count still exact.
    for f in 0..DENSE_FIELDS {
        let name = format!("d{f:02}");
        let value = format!("v{f:02}");
        let field_id = reader.field_id(&name).unwrap();
        assert_eq!(
            reader
                .debug_postings_len(value.as_bytes(), field_id)
                .unwrap(),
            Some(0),
            "dense value term of {name} must be elided"
        );
        assert_eq!(
            reader
                .debug_postings_len(name.as_bytes(), KEY_FIELD_ID)
                .unwrap(),
            Some(0),
            "dense key term of {name} must be elided"
        );
    }
    assert_eq!(reader.count(&exact("d05", "v05")).unwrap(), TOTAL as u64);
    assert_eq!(
        reader.eval(&exact("d05", "v05")).unwrap().count_set_bits(),
        TOTAL
    );

    // The sparse control keeps real postings and exact counts.
    let sparse_id = reader.field_id("sparse").unwrap();
    assert!(
        reader
            .debug_postings_len(b"sv", sparse_id)
            .unwrap()
            .unwrap()
            > 0
    );
    assert!(
        reader
            .debug_postings_len(b"sparse", KEY_FIELD_ID)
            .unwrap()
            .unwrap()
            > 0
    );
    assert_eq!(reader.count(&exact("sparse", "sv")).unwrap(), 200);
    assert_eq!(reader.key_exists("sparse").unwrap().count_set_bits(), 200);
    assert_eq!(
        reader.keys_with_prefix("spa").unwrap(),
        vec![("sparse".to_string(), 200)]
    );
    // Spot-check read_source at the far end.
    let sources = reader.read_source(&[199_999]).unwrap();
    assert_eq!(sources.value(0), "{\"i\":199999}");

    assert!(
        start.elapsed() < std::time::Duration::from_secs(60),
        "scale test too slow: {:?}",
        start.elapsed()
    );
}

// ---------------------------------------------------------------------------
// push_docs_rows (source-driven term extraction) and extraction parity
// ---------------------------------------------------------------------------

/// Test mirror of the production `_source` synthesis rules (see
/// `search::datafusion::source_synthesis::synthesize_source`): a single-level JSON
/// object of every batch column *including* `_timestamp` but excluding
/// `_o2_id`/`_original`, with null values omitted; numbers stay numbers.
fn synthesize_source_for_test(batch: &RecordBatch) -> StringArray {
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let mut record = serde_json::Map::new();
        for (index, field) in batch.schema_ref().fields().iter().enumerate() {
            let name = field.name().as_str();
            if name == crate::ID_COL_NAME || name == crate::ORIGINAL_DATA_COL_NAME {
                continue;
            }
            let column = batch.column(index);
            if !column.is_valid(row) {
                continue;
            }
            let value = match column.data_type() {
                DataType::Utf8 => serde_json::json!(
                    column
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .value(row)
                ),
                DataType::Int64 => serde_json::json!(
                    column
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(row)
                ),
                DataType::UInt64 => serde_json::json!(
                    column
                        .as_any()
                        .downcast_ref::<arrow::array::UInt64Array>()
                        .unwrap()
                        .value(row)
                ),
                DataType::Float64 => {
                    let value = column
                        .as_any()
                        .downcast_ref::<arrow::array::Float64Array>()
                        .unwrap()
                        .value(row);
                    if !value.is_finite() {
                        continue; // arrow-json serializes non-finite as null
                    }
                    serde_json::json!(value)
                }
                DataType::Boolean => serde_json::json!(
                    column
                        .as_any()
                        .downcast_ref::<arrow::array::BooleanArray>()
                        .unwrap()
                        .value(row)
                ),
                other => panic!("unsupported test column type {other:?}"),
            };
            record.insert(field.name().clone(), value);
        }
        rows.push(serde_json::Value::Object(record).to_string());
    }
    StringArray::from_iter_values(rows)
}

/// Pull the column-store columns (plus nothing else) out of a batch in the
/// shape `push_docs_rows` takes them.
fn cs_columns_of(batch: &RecordBatch, names: &[&str]) -> Vec<(String, ArrayRef)> {
    names
        .iter()
        .map(|name| {
            (
                name.to_string(),
                Arc::clone(batch.column_by_name(name).unwrap()),
            )
        })
        .collect()
}

fn timestamps_of(batch: &RecordBatch) -> Int64Array {
    batch
        .column_by_name("_timestamp")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .clone()
}

/// CRITICAL parity check: a file built column-driven
/// (`push_batch_with_source`) and one built source-driven (`push_docs_rows`)
/// from the same logical data must carry identical terms, doc bitmaps,
/// doc_counts and key terms — and answer queries identically.
#[test]
fn source_driven_extraction_parity() {
    let schema = docs_dataset_schema();
    let opts = dataset_options();

    let batches = [
        docs_dataset_batch(
            &schema,
            vec![1000, 1001, 1002, 1003, 1004, 1005],
            vec![
                Some("info"),
                Some("error"),
                Some("info"),
                None,
                Some("warn"),
                Some("error"),
            ],
            vec![
                Some("Error connecting to db"),
                Some("timeout waiting"),
                Some("user login ok"),
                Some(""),
                Some("disk almost full"),
                Some("error error error"),
            ],
            // one empty-string svc: both derivations must raw-index it
            vec!["api", "api", "auth", "", "db", "db"],
            vec![Some(1), Some(2), None, Some(4), Some(5), None],
        ),
        docs_dataset_batch(
            &schema,
            vec![1006, 1007, 1008, 1009],
            vec![Some("info"), Some("warn"), Some("error"), Some("info")],
            vec![
                Some("Timeout again"),
                None,
                Some("db timeout hard"),
                Some("all good"),
            ],
            vec!["api", "web", "web", "api"],
            vec![Some(7), Some(8), Some(9), Some(10)],
        ),
    ];

    let mut column_driven = VixWriter::new(&schema, opts.clone(), true);
    let mut source_driven = VixWriter::new(&schema, opts, true);
    let mut doc = 0usize;
    for batch in &batches {
        let source = synthesize_source_for_test(batch);
        let originals = dataset_originals(doc..doc + batch.num_rows());
        doc += batch.num_rows();

        column_driven
            .push_batch_with_source(batch, &source, Some(&originals))
            .unwrap();
        source_driven
            .push_docs_rows(
                &timestamps_of(batch),
                &cs_columns_of(batch, &["code", "svc"]),
                &source,
                Some(&originals),
            )
            .unwrap();
    }

    let (column_bytes, column_bytes_index, column_stats) =
        column_driven.finish_with_stats().unwrap();
    let (source_bytes, source_bytes_index, source_stats) =
        source_driven.finish_with_stats().unwrap();

    let column_reader = open_built(column_bytes, column_bytes_index);
    let source_reader = open_built(source_bytes, source_bytes_index);

    // identical term inventory: raw composite bytes (same field ids — both
    // writers were built from the same schema), doc_counts, doc bitmaps
    let column_terms = column_reader.debug_all_terms().unwrap();
    let source_terms = source_reader.debug_all_terms().unwrap();
    assert!(!column_terms.is_empty());
    assert_eq!(column_terms, source_terms);
    assert_eq!(column_stats.row_count, source_stats.row_count);
    assert_eq!(column_stats.term_count, source_stats.term_count);

    // and the readers agree on everything user-visible
    assert_eq!(column_reader.row_count(), source_reader.row_count());
    assert_eq!(
        column_reader.partial_fields(),
        source_reader.partial_fields()
    );
    let queries = [
        exact("level", "error"),
        exact("svc", "api"),
        exact("svc", ""),
        any_token("connecting"),
        any_token("timeout"),
        prefix(None, "time"),
        contains(None, "TIMEOUT", true),
        regex(Some("level"), "err.*"),
        VixQuery::KeyExists {
            path: "code".to_string(),
        },
        VixQuery::KeyExists {
            path: "log".to_string(),
        },
    ];
    for query in &queries {
        assert_eq!(
            eval_set(&column_reader, query),
            eval_set(&source_reader, query),
            "answers diverge for {query:?}"
        );
        assert_eq!(
            column_reader.count(query).unwrap(),
            source_reader.count(query).unwrap(),
            "counts diverge for {query:?}"
        );
    }
    assert_eq!(
        column_reader.keys_with_prefix("").unwrap(),
        source_reader.keys_with_prefix("").unwrap()
    );

    // stored side: identical _timestamp, cs columns, _source and _original
    for column in ["_timestamp", "svc", "code"] {
        let a = column_reader.read_docs_column(column).unwrap();
        let b = source_reader.read_docs_column(column).unwrap();
        assert_eq!(a.as_ref(), b.as_ref(), "docs column {column:?} diverges");
    }
    let rows: Vec<u64> = (0..10).collect();
    assert_eq!(
        column_reader.read_source(&rows).unwrap(),
        source_reader.read_source(&rows).unwrap()
    );
    let a = column_reader.read_docs_column("_original").unwrap();
    let b = source_reader.read_docs_column("_original").unwrap();
    assert_eq!(a.as_ref(), b.as_ref());
}

#[test]
fn push_docs_rows_value_kinds() {
    // numbers/bools get key terms only; strings get value terms; nulls are
    // absent entirely; internal keys are never key terms
    let schema = Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("msg", DataType::Utf8, true),
    ]);
    let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
    let source = StringArray::from(vec![
        r#"{"_timestamp":1000,"msg":"hello","num":42,"flag":true}"#,
        r#"{"_timestamp":1001,"msg":"world","num":4.5,"gone":null}"#,
        r#"{"_timestamp":1002,"flag":false}"#,
    ]);
    writer
        .push_docs_rows(
            &Int64Array::from(vec![1000, 1001, 1002]),
            &[],
            &source,
            None,
        )
        .unwrap();
    let (bytes, bytes_index, stats) = writer.finish_with_stats().unwrap();
    let reader = open_built(bytes, bytes_index);

    assert_eq!(reader.row_count(), 3);
    assert_eq!(stats.row_count, 3);
    // key terms: msg 0/1, num 0/1, flag 0/2, gone nowhere, _timestamp never
    assert_eq!(key_exists_set(&reader, "msg"), docs(&[0, 1]));
    assert_eq!(key_exists_set(&reader, "num"), docs(&[0, 1]));
    assert_eq!(key_exists_set(&reader, "flag"), docs(&[0, 2]));
    assert_eq!(key_exists_set(&reader, "gone"), docs(&[]));
    assert_eq!(key_exists_set(&reader, "_timestamp"), docs(&[]));
    // value terms only on the string field
    assert_eq!(eval_set(&reader, &exact("msg", "hello")), docs(&[0]));
    assert_eq!(eval_set(&reader, &exact("msg", "world")), docs(&[1]));
    assert_eq!(eval_set(&reader, &any_token("42")), docs(&[]));
    // numbers/bools are not partial — the column-driven path would not have
    // indexed them either
    assert!(reader.partial_fields().is_empty());
}

#[test]
fn push_docs_rows_unindexable_string_key_is_partial() {
    // `extra` carries string values but is not a term field of the writer
    // schema: its docs still get key terms, but the value cannot be indexed,
    // so the field is flagged partial (read side falls back to scanning)
    let schema = Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("msg", DataType::Utf8, true),
    ]);
    let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
    let source = StringArray::from(vec![r#"{"msg":"a","extra":"unseen"}"#, r#"{"msg":"b"}"#]);
    writer
        .push_docs_rows(&Int64Array::from(vec![1, 2]), &[], &source, None)
        .unwrap();
    let reader = finish_open(writer);

    assert_eq!(key_exists_set(&reader, "extra"), docs(&[0]));
    assert!(reader.partial_fields().contains("extra"));
    assert!(reader.field_id("extra").is_none());

    // oversize values are skipped WITHOUT tainting — exactly like the
    // column-driven path (owner call 2026-08-12): the key term lands, the
    // skipped literal itself misses, the field stays index-authoritative
    let mut writer = VixWriter::new(
        &schema,
        VixWriterOptions {
            max_raw_term_len: 8,
            ..Default::default()
        },
        false,
    );
    let long = "x".repeat(64);
    let source = StringArray::from(vec![format!("{{\"msg\":{long:?}}}")]);
    writer
        .push_docs_rows(&Int64Array::from(vec![1]), &[], &source, None)
        .unwrap();
    let (bytes, bytes_index, stats) = writer.finish_with_stats().unwrap();
    let reader = open_built(bytes, bytes_index);
    assert!(reader.partial_fields().is_empty());
    assert_eq!(stats.oversize_skipped, 1);
    assert_eq!(key_exists_set(&reader, "msg"), docs(&[0]));
    assert_eq!(eval_set(&reader, &exact("msg", long.as_str())), docs(&[]));
}

/// Re-pack a `.vix` file with patched string properties — simulates files
/// written by older/other writers.
fn repack_with_properties(
    bytes: Vec<u8>,
    patch: impl FnOnce(&mut Vec<(String, String)>),
) -> Vec<u8> {
    use crate::container::{BlobHandle, build_container, parse_container};

    let data = Bytes::from(bytes);
    let container = parse_container(&data).unwrap();
    let mut properties: Vec<(String, String)> = container
        .properties
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    patch(&mut properties);

    let blob_bytes = |handle: Option<BlobHandle>| match handle {
        Some(BlobHandle::Mem(bytes)) => Some(bytes.to_vec()),
        Some(BlobHandle::Ranged(_)) => unreachable!("parsed from memory"),
        None => None,
    };
    let mut blobs: Vec<(&'static str, &'static str, Vec<u8>)> = Vec::new();
    if let Some(dict) = blob_bytes(container.dict) {
        blobs.push(("o2-vix-dict-v2", "dict", dict));
    }
    if let Some(BlobHandle::Mem(blocks)) = container.dict_blocks {
        blobs.push(("o2-vix-dictblocks-v1", "dict_blocks", blocks.to_vec()));
    }
    if let Some(terms) = blob_bytes(container.terms) {
        blobs.push(("o2-vix-terms-v1", "terms", terms));
    }
    if let Some(plist) = blob_bytes(container.plist) {
        blobs.push(("o2-vix-plist-v1", "plist", plist));
    }
    if let Some(bloom) = blob_bytes(container.bloom) {
        blobs.push(("o2-vix-bloom-v1", "bloom", bloom));
    }
    if let Some(docs) = blob_bytes(container.docs) {
        blobs.push(("o2-vix-docs-v1", "docs", docs));
    }
    build_container(properties, blobs).unwrap()
}

/// The streaming docs encoder — sample budget forced to 1 byte, so the
/// encoder starts on the FIRST push and every later batch streams — must
/// produce BYTE-identical output to the buffered shape (default budget:
/// a small file buffers everything until finish). Batches are built with a
/// uniform average row size so the locked rows-per-chunk matches the
/// whole-file average; the transition may then change memory behavior only.
#[test]
fn streamed_docs_encode_is_byte_identical() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("svc", DataType::Utf8, true),
        Field::new("code", DataType::Int64, true),
    ]));
    let build = |sample_bytes: usize| {
        let opts = VixWriterOptions {
            docs_encode_sample_bytes: sample_bytes,
            ..Default::default()
        };
        let mut writer = VixWriter::new(&schema, opts, false);
        for chunk in 0..8i64 {
            // document order: `_timestamp` strictly descending across chunks
            let base = 1_000_000 - chunk * 100;
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(vec![base, base - 1, base - 2, base - 3]))
                        as ArrayRef,
                    Arc::new(StringArray::from(vec!["api", "db", "api", "web"])),
                    Arc::new(Int64Array::from(vec![200, 500, 200, 404])),
                ],
            )
            .unwrap();
            let source = StringArray::from(vec![
                r#"{"svc":"api","code":200}"#,
                r#"{"svc":"db","code":500}"#,
                r#"{"svc":"api","code":200}"#,
                r#"{"svc":"web","code":404}"#,
            ]);
            writer
                .push_batch_with_source(&batch, &source, None)
                .unwrap();
        }
        writer.finish().unwrap()
    };
    let buffered = build(0);
    let streamed = build(1);
    assert_eq!(buffered, streamed, "stream transition changed the output");

    let (streamed, streamed_index) = streamed;
    let reader = open_built(streamed, streamed_index);
    assert_eq!(reader.row_count(), 32);
    let hits = reader
        .eval(&VixQuery::Exact {
            field: "svc".to_string(),
            token: b"db".to_vec(),
        })
        .unwrap();
    assert_eq!(hits.count_set_bits(), 8);
}

/// A SPOOLED build (container streamed to a temp file instead of RAM) must
/// produce byte-identical output to the in-memory writer — the sink is the
/// only difference.
#[test]
fn spooled_output_is_byte_identical() {
    let spool_base = tempfile::tempdir().unwrap();
    let build = |spool: bool| {
        let opts = VixWriterOptions {
            fts_field_names: vec!["log".to_string()],
            row_group_size: 128,
            output_spool_dir: spool.then(|| spool_base.path().to_path_buf()),
            ..Default::default()
        };
        build_dataset_bytes(opts)
    };
    assert_eq!(build(false), build(true), "spooling changed the bytes");
}

/// A spilled build (term budget forced to 1 byte → every batch drains to a
/// sorted run; the finish k-way merges runs + resident map) must produce
/// BYTE-identical output to the unspilled writer. The dataset deliberately
/// carries: terms repeating across batches (postings spanning runs), a
/// dense term in every row (elision across runs), numeric terms, and
/// single-batch terms.
#[test]
fn spilled_terms_are_byte_identical() {
    let spill_base = tempfile::tempdir().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("env", DataType::Utf8, true),
        Field::new("svc", DataType::Utf8, true),
        Field::new("code", DataType::Int64, true),
    ]));
    let build = |spill: bool| {
        let opts = VixWriterOptions {
            term_spill_dir: spill.then(|| spill_base.path().to_path_buf()),
            term_spill_bytes: usize::from(spill), // 1 byte -> spill every batch
            ..Default::default()
        };
        let mut writer = VixWriter::new(&schema, opts, false);
        for chunk in 0..6i64 {
            let base = 1_000_000 - chunk * 10;
            let svc = ["api", "db", "api", "web"][(chunk % 4) as usize];
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(vec![base, base - 1, base - 2])) as ArrayRef,
                    // `env` is IDENTICAL in every row -> dense, elided
                    Arc::new(StringArray::from(vec!["prod", "prod", "prod"])),
                    Arc::new(StringArray::from(vec![svc, "worker", svc])),
                    Arc::new(Int64Array::from(vec![200, 500 + chunk, 200])),
                ],
            )
            .unwrap();
            let source = StringArray::from(
                (0..3)
                    .map(|row| {
                        format!(
                            r#"{{"env":"prod","svc":"{}","code":{}}}"#,
                            if row == 1 { "worker" } else { svc },
                            if row == 1 { 500 + chunk } else { 200 },
                        )
                    })
                    .collect::<Vec<_>>(),
            );
            writer
                .push_batch_with_source(&batch, &source, None)
                .unwrap();
        }
        writer.finish().unwrap()
    };
    let unspilled = build(false);
    let spilled = build(true);
    assert_eq!(unspilled, spilled, "spill changed the output bytes");

    // spot-check semantics on the spilled file: cross-run postings union,
    // dense elision, numeric terms
    let (spilled, spilled_index) = spilled;
    let reader = open_built(spilled, spilled_index);
    assert_eq!(reader.row_count(), 18);
    let api_hits = reader
        .eval(&VixQuery::Exact {
            field: "svc".to_string(),
            token: b"api".to_vec(),
        })
        .unwrap();
    assert_eq!(api_hits.count_set_bits(), 6); // chunks 0,2,4 x 2 "api" rows each
    let dense_hits = reader
        .eval(&VixQuery::Exact {
            field: "env".to_string(),
            token: b"prod".to_vec(),
        })
        .unwrap();
    assert_eq!(dense_hits.count_set_bits(), 18); // elided-dense term
}

/// Files written before the streamed-docs writer carry the historical
/// [dict, terms, docs] blob order (the repack helper reproduces it); the
/// reader locates blobs by tag/offset, so they read identically to the new
/// [docs, dict, terms] order — same term stream, same postings, same docs.
#[test]
fn legacy_blob_order_reads_identically() {
    let (bytes, bytes_index) = build_dataset_bytes(dataset_options());
    let new_reader = open_built(bytes.clone(), bytes_index.clone());
    let legacy_bytes = repack_with_properties(bytes, |_| {});
    let legacy_reader = open_built(legacy_bytes, bytes_index);

    let dump_terms = |reader: &VixReader| {
        let mut terms: Vec<(Vec<u8>, u64, Vec<u32>)> = Vec::new();
        reader
            .for_each_term(&mut |key, doc_count, postings| {
                terms.push((key.to_vec(), doc_count, postings.to_vec()));
                Ok(())
            })
            .unwrap();
        terms
    };
    assert_eq!(dump_terms(&new_reader), dump_terms(&legacy_reader));

    for query in [
        VixQuery::Exact {
            field: "level".to_string(),
            token: b"error".to_vec(),
        },
        VixQuery::KeyExists {
            path: "svc".to_string(),
        },
    ] {
        assert_eq!(
            new_reader.eval(&query).unwrap(),
            legacy_reader.eval(&query).unwrap(),
            "query {query:?} differs across blob orders"
        );
    }

    let read_ts = |reader: &VixReader| {
        reader
            .read_docs_column("_timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec()
    };
    assert_eq!(read_ts(&new_reader), read_ts(&legacy_reader));
}

/// Pilot fix A: fts fields emit tokens only — the raw whole value never
/// becomes a dictionary term.
#[test]
fn fts_fields_emit_tokens_only() {
    // `log` is fts with field id 2 in both datasets (code=0, level=1,
    // log=2, svc=3).
    for (bytes, bytes_index) in [
        build_dataset_bytes(dataset_options()),
        build_docs_dataset_bytes(false),
    ] {
        let reader = open_built(bytes, bytes_index);
        // no raw whole-value term ...
        assert_eq!(
            reader
                .debug_postings_len(b"Error connecting to db", 2)
                .unwrap(),
            None
        );
        assert_eq!(
            reader.debug_postings_len(b"timeout waiting", 2).unwrap(),
            None
        );
        // ... but the tokens are there, under the fts field's id.
        assert!(reader.debug_postings_len(b"timeout", 2).unwrap().is_some());
        assert!(reader.debug_postings_len(b"error", 2).unwrap().is_some());
        // and non-fts fields keep their raw values.
        assert!(reader.debug_postings_len(b"error", 1).unwrap().is_some());
        // the whole value is not reachable through any-field scans either.
        assert_eq!(eval_set(&reader, &any_token("timeout waiting")), docs(&[]));
        assert_eq!(eval_set(&reader, &any_token("timeout")), docs(&[1, 6, 8]));
    }
}

/// Fix wave A (tokenizer): the writer emits the canonical `o2_tokenize`
/// tokens — ASCII runs split at non-ASCII boundaries, per-char non-ASCII
/// alphanumerics, byte length filter — through BOTH extraction paths, so
/// match_all query tokens (produced by the same function on the search
/// side) line up with the index for non-ASCII text.
#[test]
fn writer_emits_canonical_tokens() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("log", DataType::Utf8, true),
    ]));
    let opts = VixWriterOptions {
        fts_field_names: vec!["log".to_string()],
        ..Default::default()
    };
    let values = ["café latte", "用户admin登录", "size 中 large"];
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![100, 99, 98])) as ArrayRef,
            Arc::new(StringArray::from(values.to_vec())),
        ],
    )
    .unwrap();
    let source = synthesize_source_for_test(&batch);

    let mut column_driven = VixWriter::new(&schema, opts.clone(), false);
    column_driven
        .push_batch_with_source(&batch, &source, None)
        .unwrap();
    let mut source_driven = VixWriter::new(&schema, opts, false);
    source_driven
        .push_docs_rows(&timestamps_of(&batch), &[], &source, None)
        .unwrap();
    let column_reader = finish_open(column_driven);
    let source_reader = finish_open(source_driven);

    // identical token inventory from both derivations
    assert_eq!(
        column_reader.debug_all_terms().unwrap(),
        source_reader.debug_all_terms().unwrap()
    );
    for reader in [&column_reader, &source_reader] {
        // the corrected (search-side) semantics ...
        assert_eq!(eval_set(reader, &any_token("caf")), docs(&[0]));
        assert_eq!(eval_set(reader, &any_token("é")), docs(&[0]));
        assert_eq!(eval_set(reader, &any_token("latte")), docs(&[0]));
        assert_eq!(eval_set(reader, &any_token("admin")), docs(&[1]));
        assert_eq!(eval_set(reader, &any_token("用")), docs(&[1]));
        assert_eq!(eval_set(reader, &any_token("中")), docs(&[2]));
        // ... and the old whole-run tokens do NOT exist
        assert_eq!(eval_set(reader, &any_token("café")), docs(&[]));
        assert_eq!(eval_set(reader, &any_token("用户admin登录")), docs(&[]));

        // a match_all consumer AND-ing o2_tokenize query tokens finds the doc
        for (doc, text) in values.iter().enumerate() {
            let query = VixQuery::And(
                crate::o2_tokenize(text, 2, 64)
                    .map(|token| any_token(&token))
                    .collect(),
            );
            assert!(
                eval_set(reader, &query).contains(&(doc as u32)),
                "match_all({text:?}) must find doc {doc}"
            );
        }
    }
}

/// Fix wave A (empty strings): an all-empty structured column is a DENSE
/// empty term — elided postings, exact doc_count — and empty terms behave
/// like any other value in scans and negations.
#[test]
fn empty_string_values_dense_elision_and_scans() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("blank", DataType::Utf8, false),
        Field::new("svc", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![100, 99, 98])) as ArrayRef,
            Arc::new(StringArray::from(vec![""; 3])),
            Arc::new(StringArray::from(vec![Some("api"), Some(""), None])),
        ],
    )
    .unwrap();
    let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
    writer
        .push_batch_with_source(&batch, &dataset_sources(0..3), None)
        .unwrap();
    let reader = finish_open(writer);

    // `blank` = "" in every doc: value term AND key term are dense-elided
    let blank_id = reader.field_id("blank").unwrap();
    assert_eq!(reader.debug_postings_len(b"", blank_id).unwrap(), Some(0));
    assert_eq!(eval_set(&reader, &exact("blank", "")), docs(&[0, 1, 2]));
    assert_eq!(reader.count(&exact("blank", "")).unwrap(), 3);
    assert_eq!(
        reader.field_value_counts("blank").unwrap(),
        Some(vec![(b"".to_vec(), 3)])
    );

    // sparse empty value on `svc`
    assert_eq!(eval_set(&reader, &exact("svc", "")), docs(&[1]));
    assert_eq!(
        eval_set(&reader, &VixQuery::Not(Box::new(exact("svc", "")))),
        docs(&[0, 2])
    );
    // the empty term participates in per-field prefix scans ("" prefix
    // matches every term of the field, the empty one included)
    assert_eq!(
        eval_set(&reader, &prefix(Some("svc"), "")),
        docs(&[0, 1]),
        "empty-prefix scan covers docs with any svc value, '' included"
    );
    // but a non-empty prefix/needle never matches the empty term
    assert_eq!(eval_set(&reader, &prefix(Some("svc"), "a")), docs(&[0]));
    assert_eq!(
        reader.field_value_counts("svc").unwrap(),
        Some(vec![(b"".to_vec(), 1), (b"api".to_vec(), 1)])
    );
}

/// Fix wave A (silent-unindexed guard): a batch storing a term field under
/// a non-string type (per-batch schema drift) cannot be value-indexed —
/// the field must land in `partial_fields` so lookups fall back to the
/// scan instead of silently missing those rows.
#[test]
fn non_string_batch_column_marks_field_partial() {
    use arrow::{array::TimestampMicrosecondArray, datatypes::TimeUnit};

    let string_schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("f", DataType::Utf8, true),
    ]));
    // numeric drift: the values ARE indexable now (tagged canonical terms)
    let numeric_schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("f", DataType::Int64, true),
    ]));
    // a type with no term derivation (its `_source` image morphs to an ISO
    // string): still partial
    let unindexable_schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("f", DataType::Timestamp(TimeUnit::Microsecond, None), true),
    ]));
    let mut writer = VixWriter::new(&string_schema, VixWriterOptions::default(), false);
    let batch1 = RecordBatch::try_new(
        Arc::clone(&string_schema),
        vec![
            Arc::new(Int64Array::from(vec![100])) as ArrayRef,
            Arc::new(StringArray::from(vec!["x"])),
        ],
    )
    .unwrap();
    let batch2 = RecordBatch::try_new(
        Arc::clone(&numeric_schema),
        vec![
            Arc::new(Int64Array::from(vec![99])) as ArrayRef,
            Arc::new(Int64Array::from(vec![7])),
        ],
    )
    .unwrap();
    let batch3 = RecordBatch::try_new(
        Arc::clone(&unindexable_schema),
        vec![
            Arc::new(Int64Array::from(vec![98])) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(vec![1_000_000i64])),
        ],
    )
    .unwrap();
    writer
        .push_batch_with_source(&batch1, &dataset_sources(0..1), None)
        .unwrap();
    writer
        .push_batch_with_source(&batch2, &dataset_sources(1..2), None)
        .unwrap();
    let reader = finish_open(writer);

    // the string batch indexed its raw value; the numeric batch indexed its
    // tagged CANONICAL value — no partial mark for either
    assert_eq!(eval_set(&reader, &exact("f", "x")), docs(&[0]));
    assert_eq!(eval_set(&reader, &exact_numeric("f", "7")), docs(&[1]));
    assert_eq!(key_exists_set(&reader, "f"), docs(&[0, 1]));
    assert!(
        reader.partial_fields().is_empty(),
        "string and numeric batches are both indexable"
    );

    // ... while a batch storing the field under a type with no term
    // derivation still degrades it to partial (scan fallback)
    let mut writer = VixWriter::new(&string_schema, VixWriterOptions::default(), false);
    writer
        .push_batch_with_source(&batch3, &dataset_sources(0..1), None)
        .unwrap();
    let reader = finish_open(writer);
    assert!(
        reader.partial_fields().contains("f"),
        "the un-indexable batch must flag the field for scan fallback"
    );
}

/// Pilot fix B: exact dictionary-only per-value doc counts.
#[test]
fn field_value_counts_exact_paths() {
    let reader = build_docs_dataset(false);

    // plain term field: every value with its doc count, byte-ascending
    assert_eq!(
        reader.field_value_counts("level").unwrap(),
        Some(vec![
            (b"error".to_vec(), 3),
            (b"info".to_vec(), 4),
            (b"warn".to_vec(), 2),
        ])
    );
    // term+cs field whose KEY term is dense-elided (svc is non-null in all
    // 10 docs): reconciliation reads its doc_count regardless
    assert_eq!(
        reader.field_value_counts("svc").unwrap(),
        Some(vec![
            (b"api".to_vec(), 4),
            (b"auth".to_vec(), 2),
            (b"db".to_vec(), 2),
            (b"web".to_vec(), 2),
        ])
    );
    // fts field: tokens share the id space -> not per-value counts
    assert_eq!(reader.field_value_counts("log").unwrap(), None);
    // numeric field: its TAGGED canonical value terms are not string
    // values — excluding them leaves a reconciliation shortfall, so the
    // exact-counts fast path refuses (typed grouping must scan)
    assert_eq!(reader.field_value_counts("code").unwrap(), None);
    // a field no document carries: provably empty, exact
    assert_eq!(reader.field_value_counts("missing").unwrap(), Some(vec![]));
}

/// Guard for the disjoint-count fast path: `count(query)` must equal
/// `eval(query).count_set_bits()` for every leaf shape — single-field term
/// leaves (the new doc_count-sum path), fts-field leaves and any-field
/// leaves (which must keep the postings union: their term doc sets
/// overlap), and numeric-mixed fields.
#[test]
fn count_matches_eval_popcount_across_leaf_shapes() {
    let reader = build_docs_dataset(false);
    let queries = vec![
        // multi-ordinal single term field: api+auth -> disjoint-sum path
        VixQuery::Prefix {
            field: Some("svc".to_string()),
            prefix: b"a".to_vec(),
        },
        // the whole field
        VixQuery::Prefix {
            field: Some("svc".to_string()),
            prefix: b"".to_vec(),
        },
        // db+web via substring, both case paths
        VixQuery::Contains {
            field: Some("svc".to_string()),
            needle: b"b".to_vec(),
            case_insensitive: false,
        },
        VixQuery::Contains {
            field: Some("svc".to_string()),
            needle: b"B".to_vec(),
            case_insensitive: true,
        },
        VixQuery::Regex {
            field: Some("svc".to_string()),
            pattern: ".*b.*".to_string(),
        },
        // numeric-typed field: tagged terms, still one value per doc
        VixQuery::Prefix {
            field: Some("code".to_string()),
            prefix: b"".to_vec(),
        },
        // any-field leaves cross fields AND reach fts token terms (which
        // overlap per doc) -> must stay on the union path. A scoped leaf on
        // a pure-fts field cannot even resolve (require_field_id refuses),
        // so field: None is the reachable overlap shape.
        VixQuery::Contains {
            field: None,
            needle: b"e".to_vec(),
            case_insensitive: false,
        },
        VixQuery::Prefix {
            field: None,
            prefix: b"a".to_vec(),
        },
    ];
    for query in queries {
        assert_eq!(
            reader.count(&query).unwrap(),
            reader.eval(&query).unwrap().count_set_bits() as u64,
            "count must equal eval popcount for {query:?}"
        );
    }
}

/// #29 differential: the key-free top-k/head paths must agree with the
/// full-walk `field_value_counts` on every eligibility shape — the same
/// Some/None decisions, the same counts, the same keys, and the same
/// truncation set as the collector's `truncate_top_k` comparator.
#[test]
fn field_value_top_k_and_head_match_walk() {
    let reader = build_docs_dataset(false);

    // untruncated: exact equality with the walk (both key-ascending)
    for field in ["level", "svc"] {
        let walk = reader.field_value_counts(field).unwrap().unwrap();
        for ascend in [false, true] {
            let (top, truncated) = reader
                .field_value_top_k(field, 1000, ascend)
                .unwrap()
                .unwrap();
            assert!(!truncated, "{field} has few distinct values");
            assert_eq!(
                top, walk,
                "untruncated top-k must equal the walk for {field}"
            );
        }
        let keys: Vec<Vec<u8>> = walk.iter().map(|(k, _)| k.clone()).collect();
        for take in [1usize, 2, 100] {
            let head = reader
                .field_value_head(field, take, false)
                .unwrap()
                .unwrap();
            assert_eq!(head, keys.iter().take(take).cloned().collect::<Vec<_>>());
            let tail = reader.field_value_head(field, take, true).unwrap().unwrap();
            let n = take.min(keys.len());
            assert_eq!(tail, keys[keys.len() - n..].to_vec());
        }
    }

    // ineligible shapes refuse on EVERY path: fts (log), numeric-typed (code)
    for field in ["log", "code"] {
        assert_eq!(reader.field_value_counts(field).unwrap(), None);
        assert_eq!(reader.field_value_top_k(field, 1000, false).unwrap(), None);
        assert_eq!(reader.field_value_head(field, 5, false).unwrap(), None);
    }
    // absent field: provably empty on every path
    assert_eq!(
        reader.field_value_top_k("missing", 1000, false).unwrap(),
        Some((vec![], false))
    );
    assert_eq!(
        reader.field_value_head("missing", 5, false).unwrap(),
        Some(vec![])
    );

    // truncation: the kept SET matches walk + the truncate_top_k comparator
    // (count desc/asc, ties toward the smaller key). svc has a 3-way count
    // tie (auth/db/web at 2), exercising the tie-break.
    for field in ["level", "svc"] {
        let walk = reader.field_value_counts(field).unwrap().unwrap();
        for ascend in [false, true] {
            let (mut top, truncated) = reader.field_value_top_k(field, 2, ascend).unwrap().unwrap();
            assert!(truncated, "{field} has more than 2 distinct values");
            assert_eq!(top.len(), 2);
            let mut oracle = walk.clone();
            if ascend {
                oracle.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
            } else {
                oracle.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            }
            oracle.truncate(2);
            // top-k returns key-ascending; compare as sorted sets
            oracle.sort();
            top.sort();
            assert_eq!(top, oracle, "{field} ascend={ascend}");
        }
    }

    // empty-string values are ordinary countable groups on the new paths too
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("f", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some(""), Some(""), Some("x")])),
        ],
    )
    .unwrap();
    let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
    writer
        .push_batch_with_source(&batch, &dataset_sources(0..3), None)
        .unwrap();
    let reader = finish_open(writer);
    assert_eq!(
        reader.field_value_top_k("f", 10, false).unwrap(),
        Some((vec![(b"".to_vec(), 2), (b"x".to_vec(), 1)], false))
    );
    assert_eq!(
        reader.field_value_head("f", 1, false).unwrap(),
        Some(vec![b"".to_vec()])
    );
}

/// Dense-elided *value* terms keep their `doc_count`, and multi-row-group
/// dictionaries aggregate across groups.
#[test]
fn field_value_counts_dense_elision_and_multi_rg() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("env", DataType::Utf8, false),
        Field::new("level", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])) as ArrayRef,
            Arc::new(StringArray::from(vec!["prod"; 6])),
            Arc::new(StringArray::from(vec![
                Some("info"),
                Some("info"),
                None,
                Some("warn"),
                Some("info"),
                None,
            ])),
        ],
    )
    .unwrap();
    // a tiny row-group budget forces the dictionary across several groups
    let opts = VixWriterOptions {
        ..Default::default()
    };
    let mut writer = VixWriter::new(&schema, opts, false);
    writer
        .push_batch_with_source(&batch, &dataset_sources(0..6), None)
        .unwrap();
    let reader = finish_open(writer);
    assert!(
        reader.term_row_group_count() > 1,
        "the dataset must span multiple dictionary row groups"
    );

    // env=prod is in every doc: its postings are dense-elided (len 0) but
    // the doc_count column still carries the exact count
    assert_eq!(reader.debug_postings_len(b"prod", 0).unwrap(), Some(0));
    assert_eq!(
        reader.field_value_counts("env").unwrap(),
        Some(vec![(b"prod".to_vec(), 6)])
    );
    assert_eq!(
        reader.field_value_counts("level").unwrap(),
        Some(vec![(b"info".to_vec(), 3), (b"warn".to_vec(), 1)])
    );
}

/// The dictionary-only serve policy: empty-string values serve as ordinary
/// raw terms; an oversize-skip shortfall serves via the per-field allowance
/// (counts omit the skipped values — the 2026-08-12 trade); the partial
/// MARKER alone still refuses; and any unexplained shortfall refuses.
#[test]
fn field_value_counts_allowance_and_refusal_policy() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("f", DataType::Utf8, true),
    ]));

    // an empty-string value is a raw term like any other: it shows up as a
    // countable "" group and reconciles against the key term exactly
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("a"), Some(""), Some("b")])),
        ],
    )
    .unwrap();
    let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
    writer
        .push_batch_with_source(&batch, &dataset_sources(0..3), None)
        .unwrap();
    let (complete_bytes, complete_bytes_index) = writer.finish().unwrap();
    let reader = open_built(complete_bytes.clone(), complete_bytes_index.clone());
    assert_eq!(
        reader.field_value_counts("f").unwrap(),
        Some(vec![
            (b"".to_vec(), 1),
            (b"a".to_vec(), 1),
            (b"b".to_vec(), 1),
        ])
    );

    // an oversize value is skipped without degrade (owner call 2026-08-12),
    // so the field carries NO partial marker — but the dictionary-only
    // serve still refuses it via doc-count reconciliation (2 key-term docs,
    // 1 valued): top-k/group-by answers stay exact by falling back to the
    // scan paths; only per-value probes take the accepted miss
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("ok"),
                Some("this value is far too long"),
            ])),
        ],
    )
    .unwrap();
    let mut writer = VixWriter::new(
        &schema,
        VixWriterOptions {
            max_raw_term_len: 8,
            ..Default::default()
        },
        false,
    );
    writer
        .push_batch_with_source(&batch, &dataset_sources(0..2), None)
        .unwrap();
    let reader = finish_open(writer);
    assert!(reader.partial_fields().is_empty());
    assert_eq!(reader.oversize_skips().get("f"), Some(&1));
    // the skip is an exact allowance: the serve stays eligible and the
    // counts OMIT the skipped value — no scan fallback for oversize
    assert_eq!(
        reader.field_value_counts("f").unwrap(),
        Some(vec![(b"ok".to_vec(), 1)]),
        "oversize allowance must keep the dictionary serve eligible"
    );
    assert_eq!(
        reader.field_value_top_k("f", 10, false).unwrap(),
        Some((vec![(b"ok".to_vec(), 1)], false)),
    );
    assert_eq!(
        reader
            .field_value_counts_filtered("f", &BooleanBuffer::new_set(2), 1000)
            .unwrap(),
        Some(vec![(b"ok".to_vec(), 1)]),
    );
    // a field whose EVERY value was oversize serves the exact empty group
    // list (all docs accounted for by the allowance)
    let schema_g = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("g", DataType::Utf8, true),
    ]));
    let mut writer = VixWriter::new(
        &schema_g,
        VixWriterOptions {
            max_raw_term_len: 8,
            ..Default::default()
        },
        false,
    );
    let g_batch = RecordBatch::try_new(
        Arc::clone(&schema_g),
        vec![
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("also far too long")])),
        ],
    )
    .unwrap();
    writer
        .push_batch_with_source(&g_batch, &dataset_sources(0..1), None)
        .unwrap();
    let g_reader = finish_open(writer);
    assert_eq!(g_reader.field_value_counts("g").unwrap(), Some(vec![]));
    assert_eq!(
        g_reader.field_value_top_k("g", 10, false).unwrap(),
        Some((vec![], false)),
    );

    // the partial MARKER alone still refuses the serve, even over a
    // dictionary whose counts reconcile — the legacy pre-2026-08-12 shape
    // (taint written by the old oversize rule, or a surviving cause) is no
    // longer buildable through the writer for oversize, so fabricate it
    // via property patching over the COMPLETE file from above.
    let tainted = crate::test_support::repack_with_partial_fields(
        complete_bytes_index.as_deref().expect("sidecar"),
        &["f"],
    )
    .unwrap();
    let reader = open_built(complete_bytes.clone(), Some(tainted));
    assert!(reader.partial_fields().contains("f"));
    assert_eq!(reader.field_value_counts("f").unwrap(), None);
}

#[test]
fn push_docs_rows_input_validation() {
    let schema = Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("svc", DataType::Utf8, true),
    ]);
    let opts = VixWriterOptions {
        ..Default::default()
    };

    // row-count mismatches
    let mut writer = VixWriter::new(&schema, opts, false);
    let err = writer
        .push_docs_rows(
            &Int64Array::from(vec![1, 2]),
            &[],
            &StringArray::from(vec!["{}"]),
            None,
        )
        .unwrap_err();
    assert!(err.to_string().contains("source array"), "{err}");

    // null timestamps
    let err = writer
        .push_docs_rows(
            &Int64Array::from(vec![Some(1), None]),
            &[(
                "svc".to_string(),
                Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
            )],
            &StringArray::from(vec!["{}", "{}"]),
            None,
        )
        .unwrap_err();
    assert!(err.to_string().contains("_timestamp"), "{err}");

    // v2 all-columns: an UNSUPPLIED docs column stores null (a merge input
    // lacking the column contributes nulls by design) ...
    writer
        .push_docs_rows(
            &Int64Array::from(vec![1]),
            &[],
            &StringArray::from(vec!["{}"]),
            None,
        )
        .unwrap();
    // ... while a supplied name that is NOT a docs column errors loudly
    // (typo/plan-drift guard: its values would otherwise vanish)
    let err = writer
        .push_docs_rows(
            &Int64Array::from(vec![1]),
            &[(
                "not_a_column".to_string(),
                Arc::new(StringArray::from(vec!["a"])) as ArrayRef,
            )],
            &StringArray::from(vec!["{}"]),
            None,
        )
        .unwrap_err();
    assert!(err.to_string().contains("not_a_column"), "{err}");

    // wrong-length cs column
    let err = writer
        .push_docs_rows(
            &Int64Array::from(vec![1]),
            &[(
                "svc".to_string(),
                Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
            )],
            &StringArray::from(vec!["{}"]),
            None,
        )
        .unwrap_err();
    assert!(err.to_string().contains("svc"), "{err}");

    // _source must be a JSON object per row
    let err = writer
        .push_docs_rows(
            &Int64Array::from(vec![1]),
            &[(
                "svc".to_string(),
                Arc::new(StringArray::from(vec!["a"])) as ArrayRef,
            )],
            &StringArray::from(vec!["[1,2]"]),
            None,
        )
        .unwrap_err();
    assert!(err.to_string().contains("JSON object"), "{err}");

    // originals rejected on a store_original = false writer
    let err = writer
        .push_docs_rows(
            &Int64Array::from(vec![1]),
            &[(
                "svc".to_string(),
                Arc::new(StringArray::from(vec!["a"])) as ArrayRef,
            )],
            &StringArray::from(vec!["{}"]),
            Some(&StringArray::from(vec!["orig"])),
        )
        .unwrap_err();
    assert!(err.to_string().contains("store_original"), "{err}");

    // zero-row chunks are fine
    writer
        .push_docs_rows(
            &Int64Array::from(Vec::<i64>::new()),
            &[(
                "svc".to_string(),
                Arc::new(StringArray::from(Vec::<Option<&str>>::new())) as ArrayRef,
            )],
            &StringArray::from(Vec::<Option<String>>::new()),
            None,
        )
        .unwrap();
}

#[test]
fn push_docs_rows_stores_docs_and_originals() {
    let schema = Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("svc", DataType::Utf8, true),
    ]);
    let opts = VixWriterOptions {
        ..Default::default()
    };
    let mut writer = VixWriter::new(&schema, opts, true);
    let source = StringArray::from(vec![r#"{"svc":"api"}"#, r#"{"svc":"db"}"#]);
    writer
        .push_docs_rows(
            &Int64Array::from(vec![10, 20]),
            &[(
                "svc".to_string(),
                Arc::new(StringArray::from(vec!["api", "db"])) as ArrayRef,
            )],
            &source,
            Some(&StringArray::from(vec![Some("raw-0"), None])),
        )
        .unwrap();
    // a second chunk without originals stores nulls
    writer
        .push_docs_rows(
            &Int64Array::from(vec![30]),
            &[(
                "svc".to_string(),
                Arc::new(StringArray::from(vec!["web"])) as ArrayRef,
            )],
            &StringArray::from(vec![r#"{"svc":"web"}"#]),
            None,
        )
        .unwrap();

    let (bytes, bytes_index, stats) = writer.finish_with_stats().unwrap();
    assert!(stats.index_size > 0);
    assert!(stats.docs_size > 0);
    // v3 split: index_size is the SIDECAR OBJECT's size, docs_size the docs
    // blob inside the data object
    assert_eq!(
        stats.index_size,
        bytes_index.as_ref().map_or(0, |b| b.len() as u64)
    );
    assert!(stats.docs_size <= bytes.len() as u64);
    assert_eq!(stats.row_count, 3);

    let reader = open_built(bytes, bytes_index);
    assert_eq!(
        as_i64_array(reader.read_docs_column("_timestamp").unwrap().as_ref())
            .values()
            .to_vec(),
        vec![10, 20, 30]
    );
    let svc = as_string_array(reader.read_docs_column("svc").unwrap().as_ref());
    assert_eq!(svc.value(0), "api");
    assert_eq!(svc.value(2), "web");
    let originals = as_string_array(reader.read_docs_column("_original").unwrap().as_ref());
    assert_eq!(originals.value(0), "raw-0");
    assert!(originals.is_null(1));
    assert!(originals.is_null(2));
    let sources = reader.read_source(&[0, 1, 2]).unwrap();
    assert_eq!(sources.value(2), r#"{"svc":"web"}"#);

    // docs_schema is exposed for the compaction reader side
    let docs_schema = reader.docs_schema().unwrap();
    assert_eq!(
        docs_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>(),
        vec!["_timestamp", "svc", "_source", "_original"]
    );
    assert_eq!(reader.term_field_names(), vec!["svc"]);
}

// ---------------------------------------------------------------------------
// VixDocs: the scan-path accessor over the `docs` blob
// ---------------------------------------------------------------------------

#[test]
fn vix_docs_open_and_scan_all() {
    let docs = crate::VixDocs::open(Bytes::from(build_docs_dataset_bytes(false).0)).unwrap();
    assert_eq!(docs.row_count(), 10);
    assert_eq!(docs.row_group_size(), 128);
    // physical docs columns (v2 all-columns): _timestamp + EVERY schema
    // field + _source
    let names: Vec<&str> = docs
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert!(names.contains(&"_timestamp"));
    assert!(names.contains(&"svc"));
    assert!(names.contains(&"code"));
    assert!(names.contains(&"_source"));
    assert!(
        names.contains(&"level"),
        "every schema field is a docs column: {names:?}"
    );
    assert!(
        !names.contains(&"_original"),
        "no _original when store_original=false"
    );

    let projection = vec!["_timestamp".to_string(), "_source".to_string()];
    let batches = docs.read_docs(Some(&projection), None, None).unwrap();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 10);
    for batch in &batches {
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>(),
            vec!["_timestamp".to_string(), "_source".to_string()]
        );
    }
    let first = &batches[0];
    let ts = as_i64_array(first.column_by_name("_timestamp").unwrap().as_ref());
    assert_eq!(ts.value(0), 1000);
    let sources = as_string_array(first.column_by_name("_source").unwrap().as_ref());
    assert_eq!(sources.value(0), "{\"i\":0}");
}

#[test]
fn vix_docs_row_selection_and_ts_filter() {
    let docs = crate::VixDocs::open(Bytes::from(build_docs_dataset_bytes(false).0)).unwrap();

    // point reads: rows 1, 3, 5 (ts 1001, 1003, 1005), out of order + duped
    let projection = vec!["_timestamp".to_string()];
    let batches = docs
        .read_docs(Some(&projection), Some(vec![5, 1, 3, 1]), None)
        .unwrap();
    let ts: Vec<i64> = batches
        .iter()
        .flat_map(|b| {
            as_i64_array(b.column_by_name("_timestamp").unwrap().as_ref())
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(ts, vec![1001, 1003, 1005]);

    // timestamp range filter [1002, 1006) -> rows 2..=5
    let batches = docs
        .read_docs(Some(&projection), None, Some((1002, 1006)))
        .unwrap();
    let ts: Vec<i64> = batches
        .iter()
        .flat_map(|b| {
            as_i64_array(b.column_by_name("_timestamp").unwrap().as_ref())
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(ts, vec![1002, 1003, 1004, 1005]);

    // selection AND filter compose
    let batches = docs
        .read_docs(Some(&projection), Some(vec![0, 3, 9]), Some((1002, 1006)))
        .unwrap();
    let ts: Vec<i64> = batches
        .iter()
        .flat_map(|b| {
            as_i64_array(b.column_by_name("_timestamp").unwrap().as_ref())
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(ts, vec![1003]);

    // unknown projected column errors
    assert!(
        docs.read_docs(Some(&["nope".to_string()]), None, None)
            .is_err()
    );
}

#[test]
fn vix_docs_broad_string_equality_aggregates_are_exact() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("svc", DataType::Utf8, false),
    ]));
    let timestamps = vec![110, 109, 108, 107, 106, 105, 104, 103, 102, 101];
    let services = vec![
        "hit", "hit", "miss", "hit", "hit", "miss", "hit", "hit", "miss", "hit",
    ];
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(timestamps)) as ArrayRef,
            Arc::new(StringArray::from(services)) as ArrayRef,
        ],
    )
    .unwrap();
    let source = StringArray::from_iter_values((0..10).map(|row| format!("{{\"row\":{row}}}")));
    let mut writer = VixWriter::new(&schema, dataset_options(), false);
    writer
        .push_batch_with_source(&batch, &source, None)
        .unwrap();
    let (data, _) = writer.finish().unwrap();
    let docs = crate::VixDocs::open(Bytes::from(data)).unwrap();
    assert!(docs.row_order().is_ts_desc());

    // Seven of ten rows match: deliberately beyond the 2% selective
    // point-read threshold that used to fall back to a wide `_source` scan.
    assert_eq!(
        docs.eq_string_top_n("svc", "hit", None, 3, false)
            .unwrap()
            .unwrap(),
        vec![(110, 0), (109, 1), (107, 3)]
    );
    assert_eq!(
        docs.eq_string_top_n("svc", "hit", None, 3, true)
            .unwrap()
            .unwrap(),
        vec![(101, 9), (103, 7), (104, 6)]
    );
    // Exact half-open clamp: 109 is excluded and 103 is included.
    assert_eq!(
        docs.eq_string_top_n("svc", "hit", Some((103, 109)), 3, false)
            .unwrap()
            .unwrap(),
        vec![(107, 3), (106, 4), (104, 6)]
    );
    assert_eq!(
        docs.eq_string_histogram("svc", "hit", None, 100, 5, 3, 0)
            .unwrap()
            .unwrap(),
        vec![3, 3, 1]
    );
    assert_eq!(docs.eq_string_count("svc", "hit").unwrap(), Some(7));
    assert_eq!(docs.eq_string_count("svc", "absent").unwrap(), Some(0));
    assert_eq!(
        docs.eq_string_histogram("svc", "hit", Some((103, 109)), 100, 5, 3, 0)
            .unwrap()
            .unwrap(),
        vec![2, 2, 0],
        "histogram applies the same half-open timestamp clamp"
    );
    assert_eq!(
        docs.eq_string_histogram("svc", "hit", None, 102, 5, 3, 2)
            .unwrap()
            .unwrap(),
        vec![3, 3, 1],
        "timezone offset shifts back to the same absolute grid"
    );
    assert!(
        docs.eq_string_top_n("missing", "hit", None, 3, false)
            .unwrap()
            .is_none()
    );
    assert!(
        docs.eq_string_histogram("missing", "hit", None, 100, 5, 3, 0)
            .unwrap()
            .is_none()
    );
    assert!(docs.eq_string_count("missing", "hit").unwrap().is_none());
    assert!(
        docs.eq_string_top_n("_timestamp", "107", None, 3, false)
            .unwrap()
            .is_none()
    );
    assert!(
        docs.eq_string_histogram("_timestamp", "107", None, 100, 5, 3, 0)
            .unwrap()
            .is_none()
    );
    assert!(docs.eq_string_count("_timestamp", "107").unwrap().is_none());
}

#[test]
fn vix_docs_rejects_unsupported_versions() {
    // version property present but not the supported value
    let bytes = crate::container::build_container(
        vec![
            ("version".to_string(), "1".to_string()),
            ("row_count".to_string(), "0".to_string()),
        ],
        vec![],
    )
    .unwrap();
    let err = crate::VixDocs::open(Bytes::from(bytes)).unwrap_err();
    assert!(
        err.to_string().contains("unsupported .vix format")
            && err.to_string().contains("reader supports 3"),
        "unexpected error: {err}"
    );
}

// ---------------------------------------------------------------------------
// Docs-blob compression & chunk sizing (PILOT FIXES round 2)
// ---------------------------------------------------------------------------

/// Log-like `_source` JSON of ~1 KB: repeated keys, low-cardinality enums, a
/// templated message and random hex ids — compressible the way real log JSON
/// is (zstd-friendly; the pilot's FSST-only docs blob landed at ~1.6x).
fn log_like_source(row: usize, ts: i64, rng: &mut StdRng) -> String {
    let namespaces = ["kube-system", "production", "monitoring", "ingress-nginx"];
    let levels = ["info", "warn", "error", "debug"];
    let ns = namespaces[row % namespaces.len()];
    let level = levels[row % levels.len()];
    let trace: String = (0..8)
        .map(|_| format!("{:08x}", rng.random::<u32>()))
        .collect();
    let took = rng.random::<u32>() % 10_000;
    format!(
        "{{\"_timestamp\":{ts},\"kubernetes.namespace_name\":\"{ns}\",\
         \"kubernetes.pod_name\":\"pod-{row:06}\",\"kubernetes.container_name\":\"app\",\
         \"kubernetes.docker_id\":\"{trace}\",\"level\":\"{level}\",\
         \"log_file_path\":\"/var/log/containers/pod-{row:06}_{ns}_app.log\",\
         \"message\":\"{level} reconciling deployment for namespace {ns}: processed request \
         {row} in {took} ms, retrying with backoff, connection pool at capacity, \
         upstream returned status ok, trace={trace}, flushing 128 events to sink, \
         checkpoint persisted, watermark advanced, lease renewed for holder pod-{row:06}\"}}"
    )
}

/// PILOT FIXES round 2, Fix A: the 200k-record pilot's docs blob compressed
/// only ~1.6x (BtrBlocks lands on FSST for ~KB JSON `_source` rows) vs ~15x
/// for parquet+zstd on the same data. [`crate::container::docs_strategy`]
/// adds the zstd/pco compact schemes to the sampler; on synthetic log-like
/// JSON the produced docs blob must be at most 1/4 of the round-1 blob
/// (round-1 = the same batches through `compressed_strategy(128Ki)`, the
/// exact pre-fix write path), and the stored rows must round-trip.
#[test]
fn docs_blob_zstd_beats_round1_by_4x() {
    let rows = 20_000usize;
    let mut rng = StdRng::seed_from_u64(0xA0A0);
    let ts: Vec<i64> = (0..rows as i64)
        .map(|i| 1_700_000_000_000_000 + i)
        .collect();
    let sources: Vec<String> = (0..rows)
        .map(|i| log_like_source(i, ts[i], &mut rng))
        .collect();
    let source_array = StringArray::from_iter_values(sources.iter().map(String::as_str));

    // Production write path (docs blob = `_timestamp` + `_source`).
    let schema = Arc::new(Schema::new(vec![Field::new(
        "_timestamp",
        DataType::Int64,
        false,
    )]));
    let opts = VixWriterOptions {
        row_group_size: 128 * 1024, // production PARQUET_MAX_ROW_GROUP_SIZE
        ..Default::default()
    };
    let mut writer = VixWriter::new(&schema, opts, false);
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(ts.clone())) as ArrayRef],
    )
    .unwrap();
    writer
        .push_batch_with_source(&batch, &source_array, None)
        .unwrap();
    let (bytes, bytes_index, stats) = writer.finish_with_stats().unwrap();

    // Round-1 baseline: the identical docs batch through the old strategy
    // (plain BtrBlocks sampler, chunks following the 128Ki row-group size).
    let docs_schema = Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("_source", DataType::Utf8, false),
    ]);
    let docs_batch = RecordBatch::try_new(
        Arc::new(docs_schema.clone()),
        vec![
            Arc::new(Int64Array::from(ts)) as ArrayRef,
            Arc::new(source_array.clone()),
        ],
    )
    .unwrap();
    let round1 = crate::container::write_vortex_blob(
        &docs_schema,
        &[docs_batch],
        crate::container::compressed_strategy(128 * 1024),
        1,
    )
    .unwrap();

    let raw_json_bytes: usize = sources.iter().map(String::len).sum();
    eprintln!(
        "[docs zstd] rows={rows} raw _source={raw_json_bytes}B round1_docs_blob={}B \
         round2_docs_blob={}B (round1 ratio {:.2}x, round2 ratio {:.2}x)",
        round1.len(),
        stats.docs_size,
        raw_json_bytes as f64 / round1.len() as f64,
        raw_json_bytes as f64 / stats.docs_size as f64,
    );
    assert!(
        stats.docs_size * 4 <= round1.len() as u64,
        "zstd'd docs blob must be <= 1/4 of the round-1 blob: {} vs {}",
        stats.docs_size,
        round1.len()
    );

    // The zstd-encoded rows round-trip bit for bit.
    let reader = open_built(bytes, bytes_index);
    let picks = [0u64, 1, 4_242, 19_999];
    let got = reader.read_source(&picks).unwrap();
    for (i, row) in picks.iter().enumerate() {
        assert_eq!(got.value(i), sources[*row as usize], "row {row} diverges");
    }
}

// ---------------------------------------------------------------------------
// Ranged reads: VixRangeSource over a mock object, fetch-count/byte budgets
// ---------------------------------------------------------------------------

mod ranged {
    use std::{
        ops::Range,
        sync::{
            Arc,
            atomic::{AtomicU64, AtomicUsize, Ordering},
        },
    };

    use futures::{FutureExt, future::BoxFuture};

    use super::*;
    use crate::{VixDocs, VixRangeSource};

    /// In-memory mock object: serves ranges from `Bytes` (ready futures) and
    /// counts every fetch and every fetched byte.
    struct CountingSource {
        data: Bytes,
        fetches: AtomicUsize,
        bytes: AtomicU64,
        batch_calls: AtomicUsize,
    }

    impl CountingSource {
        fn new(data: Bytes) -> Arc<Self> {
            Arc::new(Self {
                data,
                fetches: AtomicUsize::new(0),
                bytes: AtomicU64::new(0),
                batch_calls: AtomicUsize::new(0),
            })
        }

        fn fetches(&self) -> usize {
            self.fetches.load(Ordering::SeqCst)
        }

        fn bytes(&self) -> u64 {
            self.bytes.load(Ordering::SeqCst)
        }

        /// Number of `fetch_many` ROUND TRIPS (each still ticks `fetches`
        /// once per range, like the trait's default chaining).
        fn batch_calls(&self) -> usize {
            self.batch_calls.load(Ordering::SeqCst)
        }
    }

    impl VixRangeSource for CountingSource {
        fn len(&self) -> u64 {
            self.data.len() as u64
        }

        fn fetch(&self, range: Range<u64>) -> BoxFuture<'static, anyhow::Result<Bytes>> {
            self.fetches.fetch_add(1, Ordering::SeqCst);
            self.bytes
                .fetch_add(range.end - range.start, Ordering::SeqCst);
            let out = if range.end <= self.data.len() as u64 && range.start <= range.end {
                Ok(self.data.slice(range.start as usize..range.end as usize))
            } else {
                Err(anyhow::anyhow!("range {range:?} out of bounds"))
            };
            async move { out }.boxed()
        }

        fn fetch_many(
            &self,
            ranges: Vec<Range<u64>>,
        ) -> BoxFuture<'static, anyhow::Result<Vec<Bytes>>> {
            self.batch_calls.fetch_add(1, Ordering::SeqCst);
            // same behavior as the trait default (each range ticks
            // `fetches` via `fetch`), plus the round-trip counter above
            let futs: Vec<_> = ranges.into_iter().map(|r| self.fetch(r)).collect();
            Box::pin(async move {
                let mut out = Vec::with_capacity(futs.len());
                for fut in futs {
                    out.push(fut.await?);
                }
                Ok(out)
            })
        }

        fn describe(&self) -> String {
            "counting-mock".to_string()
        }
    }

    /// A large core file, built once and shared: 100k docs, one distinct
    /// `svc` value per doc (100k unique terms), `level` cycling three
    /// values, per-row `_source` fattened with incompressible padding (the
    /// realistic composition — the docs blob dwarfs the dictionary). Small
    /// dictionary row groups force several FSTs; postings chunks keep the
    /// writer default so point reads stay chunk-granular.
    static LARGE_CORE_FILE: std::sync::LazyLock<(Bytes, Bytes)> =
        std::sync::LazyLock::new(build_large_core_file_uncached);

    /// One fresh build of the 100k-row fixture (use the shared
    /// [`build_large_core_file`] unless the bytes must be rebuilt).
    fn build_large_core_file_uncached() -> (Bytes, Bytes) {
        let rows = 100_000usize;
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("svc", DataType::Utf8, true),
            Field::new("level", DataType::Utf8, true),
        ]));
        let opts = VixWriterOptions {
            row_group_size: 8192,
            // Pin docs chunks to the pre-Fix-B scale of this file (~8k rows
            // of ~250 arrow bytes): the budgets below assert F2's ranged-IO
            // contract at that granularity. Fix B's own budget/default is
            // covered by `docs_chunk_budget_bounds_point_read_bytes`.
            docs_chunk_bytes: 2 * 1024 * 1024,
            ..Default::default()
        };
        let mut rng = StdRng::seed_from_u64(0xF2F2);
        let levels = ["info", "warn", "error"];
        let ts: Vec<i64> = (0..rows as i64).map(|i| 1_000_000 + i).collect();
        let svc: Vec<String> = (0..rows).map(|i| format!("svc_{i:06}")).collect();
        let level: Vec<&str> = (0..rows).map(|i| levels[i % levels.len()]).collect();
        let sources: Vec<String> = (0..rows)
            .map(|i| {
                // 128 hex chars of random padding keep the docs blob from
                // compressing away, mimicking real log payloads.
                let pad: String = (0..16)
                    .map(|_| format!("{:08x}", rng.random::<u32>()))
                    .collect();
                format!(
                    "{{\"_timestamp\":{},\"svc\":\"{}\",\"level\":\"{}\",\"payload\":\"{pad}\"}}",
                    ts[i], svc[i], level[i]
                )
            })
            .collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ts)),
                Arc::new(StringArray::from_iter_values(
                    svc.iter().map(String::as_str),
                )),
                Arc::new(StringArray::from(level)),
            ],
        )
        .unwrap();
        let mut writer = VixWriter::new(&schema, opts, false);
        writer
            .push_batch_with_source(
                &batch,
                &StringArray::from_iter_values(sources.iter().map(String::as_str)),
                None,
            )
            .unwrap();
        let (data, index) = writer.finish().unwrap();
        (
            Bytes::from(data),
            Bytes::from(index.expect("indexed fixture has a sidecar")),
        )
    }

    fn build_large_core_file() -> (Bytes, Bytes) {
        LARGE_CORE_FILE.clone()
    }

    /// Per-object counting sources of one (data, sidecar) pair. The
    /// aggregate counters keep the pre-split budget assertions meaningful:
    /// a fetch is a fetch, whichever object it hits.
    struct PairSource {
        data: Arc<CountingSource>,
        index: Arc<CountingSource>,
    }

    impl PairSource {
        fn new(data: Bytes, index: Bytes) -> Self {
            Self {
                data: CountingSource::new(data),
                index: CountingSource::new(index),
            }
        }

        fn open(&self) -> VixReader {
            VixReader::open_ranged_with_index(
                Arc::clone(&self.data) as Arc<dyn VixRangeSource>,
                Some(Arc::clone(&self.index) as Arc<dyn VixRangeSource>),
            )
            .unwrap()
        }

        fn fetches(&self) -> usize {
            self.data.fetches() + self.index.fetches()
        }

        fn bytes(&self) -> u64 {
            self.data.bytes() + self.index.bytes()
        }

        fn batch_calls(&self) -> usize {
            self.data.batch_calls() + self.index.batch_calls()
        }
    }

    /// Cold open + exact-term point lookup: the Arch.md budget. Open is a
    /// tail fetch + the dict fetch; the first exact-term evaluation adds the
    /// `terms` blob footer and the touched postings chunks — at most 4 small
    /// fetches — and the total fetched bytes stay far below the file size.
    #[test]
    fn ranged_exact_term_fetch_budget_and_parity() {
        let (data, index) = build_large_core_file();
        let file_size = (data.len() + index.len()) as u64;
        let mem = VixReader::open_with_index(data.clone(), Some(index.clone())).unwrap();
        let source = PairSource::new(data, index);
        let ranged = source.open();

        // open: one tail fetch per object (both footers fit the 64 KiB
        // window) + the dict from the sidecar.
        let open_fetches = source.fetches();
        assert!(
            open_fetches <= 3,
            "open should need at most 3 fetches (2 tails + dict), used {open_fetches}"
        );
        assert!(ranged.term_row_group_count() > 1, "want multiple FSTs");

        // Exact term: dictionary is in memory; postings come from point reads.
        let query = exact("svc", "svc_042424");
        let expect = mem.eval(&query).unwrap();
        assert_eq!(expect.count_set_bits(), 1);
        let got = ranged.eval(&query).unwrap();
        assert_eq!(bits_to_set(&got), bits_to_set(&expect));
        let eval_fetches = source.fetches() - open_fetches;
        assert!(
            eval_fetches <= 4,
            "exact-term eval should need at most 4 fetches beyond open, used {eval_fetches}"
        );

        // Hot repeat: the terms-blob footer is cached on the handle now, so
        // only the postings chunks are fetched again.
        let before = source.fetches();
        let again = ranged.eval(&exact("svc", "svc_000007")).unwrap();
        assert_eq!(
            bits_to_set(&again),
            bits_to_set(&mem.eval(&exact("svc", "svc_000007")).unwrap())
        );
        let hot_fetches = source.fetches() - before;
        assert!(
            hot_fetches <= 2,
            "hot exact-term eval should reuse the cached blob footer, used {hot_fetches}"
        );

        // Count fast path reads only the doc_count chunk.
        let before = source.fetches();
        assert_eq!(ranged.count(&exact("svc", "svc_090909")).unwrap(), 1);
        assert!(source.fetches() - before <= 2);

        eprintln!(
            "[ranged budget] file={file_size}B open={open_fetches} fetches, exact-term \
             cold={eval_fetches} hot={hot_fetches} fetches, total bytes={}",
            source.bytes()
        );
        // The whole battery stayed far below a whole-file download.
        assert!(
            source.bytes() < file_size / 4,
            "fetched {} of {} bytes",
            source.bytes(),
            file_size
        );
    }

    /// #27: a condition-ALL evaluation must not touch the dictionary at
    /// all — the unconditioned SimpleSelect/TopN shapes were paying an
    /// MB-class dict-index fetch per ranged file for a structure the query
    /// never reads.
    #[test]
    fn ranged_all_query_needs_no_dictionary_io() {
        let (data, index) = build_large_core_file();
        let mem = VixReader::open_with_index(data.clone(), Some(index.clone())).unwrap();
        let source = PairSource::new(data, index);
        let ranged = source.open();
        let open_fetches = source.fetches();
        let open_bytes = source.bytes();

        let expect = mem.eval(&VixQuery::All).unwrap();
        let got = ranged.eval(&VixQuery::All).unwrap();
        assert_eq!(got.count_set_bits(), expect.count_set_bits());
        assert_eq!(got.count_set_bits(), 100_000);
        assert_eq!(
            source.fetches(),
            open_fetches,
            "condition-ALL eval must issue ZERO fetches beyond open"
        );
        assert_eq!(source.bytes(), open_bytes);

        // the straddling-window clamp stays chunk-granular: the zone table
        // is footer-resident, only boundary chunks decode their rows
        let before = source.fetches();
        let range = ranged.timestamp_range(1_000_100, 1_050_000).unwrap();
        assert_eq!(
            bits_to_set(&range),
            bits_to_set(&mem.timestamp_range(1_000_100, 1_050_000).unwrap())
        );
        let clamp_fetches = source.fetches() - before;
        assert!(
            clamp_fetches <= 6,
            "zone-clamped timestamp_range used {clamp_fetches} fetches"
        );
    }

    /// #27: a field-scoped dictionary walk (the unfiltered TopN/Distinct
    /// value enumeration) must bulk-load its block span instead of one
    /// block-sized round trip per block — 78 files x ~350 point reads was the
    /// trace-list fetch storm. The 100k-distinct `svc` field spans
    /// hundreds of blocks; the walk must stay within a few round trips.
    #[test]
    fn ranged_field_walk_coalesces_dict_block_fetches() {
        let (data, index) = build_large_core_file();
        let mem = VixReader::open_with_index(data.clone(), Some(index.clone())).unwrap();
        let source = PairSource::new(data, index);
        let ranged = source.open();

        let expect = mem
            .field_value_counts("svc")
            .unwrap()
            .expect("svc is dictionary-eligible");
        assert_eq!(expect.len(), 100_000, "one group per distinct svc value");
        let before = source.fetches();
        let got = ranged
            .field_value_counts("svc")
            .unwrap()
            .expect("svc is dictionary-eligible");
        assert_eq!(got, expect);
        let walk_fetches = source.fetches() - before;
        assert!(
            ranged.term_row_group_count() > 1 || walk_fetches < 40,
            "sanity: the fixture should span many dict blocks"
        );
        // dict index + key-term lookup + the bulk span runs + the
        // doc_count column chunks — NOT one round trip per block
        assert!(
            walk_fetches <= 48,
            "field walk should coalesce its block span, used {walk_fetches} fetches"
        );
        eprintln!(
            "[field-walk budget] fetches={walk_fetches} bytes={}",
            source.bytes()
        );
    }

    /// #29 at scale: top-k/head over the 100k-distinct fixture equal the
    /// walk-derived oracle in memory AND ranged mode, and the ranged top-k
    /// stays fetch-bounded — it must never materialize the dictionary.
    /// (Every svc count is 1, so this is also the tie-break stress: the
    /// kept set is decided purely by the smaller-key rule at scale.)
    #[test]
    fn ranged_field_value_top_k_matches_walk_at_scale() {
        let (data, index) = build_large_core_file();
        let mem = VixReader::open_with_index(data.clone(), Some(index.clone())).unwrap();
        let walk = mem.field_value_counts("svc").unwrap().unwrap();
        assert_eq!(walk.len(), 100_000);
        let cap = 1000usize;
        let oracle_set = |ascend: bool| -> Vec<(Vec<u8>, u64)> {
            let mut oracle = walk.clone();
            if ascend {
                oracle.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
            } else {
                oracle.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            }
            oracle.truncate(cap);
            oracle.sort();
            oracle
        };
        for ascend in [false, true] {
            let (mut top, truncated) = mem.field_value_top_k("svc", cap, ascend).unwrap().unwrap();
            assert!(truncated);
            top.sort();
            assert_eq!(top, oracle_set(ascend), "mem ascend={ascend}");
        }
        let keys: Vec<Vec<u8>> = walk.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(
            mem.field_value_head("svc", 7, false).unwrap().unwrap(),
            keys[..7].to_vec()
        );
        assert_eq!(
            mem.field_value_head("svc", 7, true).unwrap().unwrap(),
            keys[keys.len() - 7..].to_vec()
        );

        let source = PairSource::new(data, index);
        let ranged = source.open();
        let before = source.fetches();
        let (mut top, truncated) = ranged
            .field_value_top_k("svc", cap, false)
            .unwrap()
            .unwrap();
        assert!(truncated);
        top.sort();
        assert_eq!(top, oracle_set(false), "ranged top-k");
        let topk_fetches = source.fetches() - before;
        eprintln!(
            "[top-k budget] fetches={topk_fetches} bytes={}",
            source.bytes()
        );
        assert!(
            topk_fetches <= 64,
            "top-k must stay fetch-bounded, used {topk_fetches} fetches"
        );
        assert_eq!(
            ranged.field_value_head("svc", 7, false).unwrap().unwrap(),
            keys[..7].to_vec()
        );
    }

    /// #27: a multi-term union over out-of-row (plist) postings resolves
    /// every pointer record through ONE batched round trip instead of a
    /// point fetch per term.
    #[test]
    fn ranged_plist_union_batches_record_fetches() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("svc", DataType::Utf8, true),
        ]));
        // big enough that the PLIST blob itself lives OUTSIDE the 64 KiB
        // open tail — small trailing blobs are swallowed by the tail
        // fetch, become memory-resident slices, and would never exercise
        // the ranged batch path. 2000 fat values -> ~2000 pointer records
        // of postings, comfortably past the tail.
        let rows = 40_000usize;
        let opts = VixWriterOptions {
            postings_plist_min_docs: 8,
            ..Default::default()
        };
        // rows stored newest-first, matching the storage convention
        let ts: Vec<i64> = (0..rows as i64).map(|i| 2_000_000 - i).collect();
        let values: Vec<String> = (0..2000).map(|v| format!("val_{v:04}")).collect();
        let svc: Vec<&str> = (0..rows).map(|i| values[i % 2000].as_str()).collect();
        let mut rng = StdRng::seed_from_u64(0x715A);
        let sources: Vec<String> = (0..rows)
            .map(|i| {
                let pad: String = (0..16)
                    .map(|_| format!("{:08x}", rng.random::<u32>()))
                    .collect();
                format!(
                    "{{\"_timestamp\":{},\"svc\":\"{}\",\"pad\":\"{pad}\"}}",
                    ts[i], svc[i]
                )
            })
            .collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ts)),
                Arc::new(StringArray::from(svc)),
            ],
        )
        .unwrap();
        let mut writer = VixWriter::new(&schema, opts, false);
        writer
            .push_batch_with_source(
                &batch,
                &StringArray::from_iter_values(sources.iter().map(String::as_str)),
                None,
            )
            .unwrap();
        let (data, index) = writer.finish().unwrap();
        let data = Bytes::from(data);
        let index = Bytes::from(index.expect("indexed fixture has a sidecar"));
        assert!(
            index.len() > 64 * 1024,
            "the sidecar must outgrow the 64 KiB open tail ({} bytes) so the \
             plist blob exercises the ranged batch path",
            index.len()
        );

        let mem = VixReader::open_with_index(data.clone(), Some(index.clone())).unwrap();
        let source = PairSource::new(data, index);
        let ranged = source.open();

        // all 2000 values (20 docs each >= threshold 8) carry pointer
        // cells; the prefix union walks them through postings_union
        let query = prefix(Some("svc"), "val_");
        let expect = mem.eval(&query).unwrap();
        assert_eq!(expect.count_set_bits(), rows);
        let before_batches = source.batch_calls();
        let got = ranged.eval(&query).unwrap();
        assert_eq!(bits_to_set(&got), bits_to_set(&expect));
        assert!(
            source.batch_calls() > before_batches,
            "plist records must resolve through a batched round trip"
        );
    }

    /// Prefix scan + docs-column point reads (the fast-path chokepoint) and
    /// the timestamp-range filter: all chunk-granular, all matching the
    /// in-memory reader bit for bit.
    #[test]
    fn ranged_prefix_fastpath_column_and_timestamp_budget() {
        let (data, index) = build_large_core_file();
        let file_size = (data.len() + index.len()) as u64;
        let mem = VixReader::open_with_index(data.clone(), Some(index.clone())).unwrap();
        let source = PairSource::new(data, index);
        let ranged = source.open();
        let open_fetches = source.fetches();
        assert!(open_fetches <= 3);

        // Prefix matching 10 terms (svc_00123x): a couple of postings chunks.
        let query = prefix(Some("svc"), "svc_00123");
        let expect = mem.eval(&query).unwrap();
        assert_eq!(expect.count_set_bits(), 10);
        assert_eq!(
            bits_to_set(&ranged.eval(&query).unwrap()),
            bits_to_set(&expect)
        );
        let prefix_fetches = source.fetches() - open_fetches;
        assert!(
            prefix_fetches <= 4,
            "prefix eval should need at most 4 fetches beyond open, used {prefix_fetches}"
        );

        // Fast-path docs column point read: docs blob footer + the touched
        // chunks; the docs schema resolves lazily on this first docs access.
        let before = source.fetches();
        let rows = [5u64, 99_000];
        let got = ranged.read_docs_column_rows("level", &rows).unwrap();
        let expect_col = mem.read_docs_column_rows("level", &rows).unwrap();
        assert_eq!(format!("{got:?}"), format!("{expect_col:?}"));
        let col_fetches = source.fetches() - before;
        assert!(
            col_fetches <= 4,
            "docs-column point read should need at most 4 fetches, used {col_fetches}"
        );

        eprintln!(
            "[ranged budget] prefix={prefix_fetches} fetches, docs-column point \
             read={col_fetches} fetches"
        );
        // timestamp_range decodes the whole (coalesced) _timestamp column
        // but nothing else.
        let range = ranged.timestamp_range(1_000_100, 1_000_200).unwrap();
        assert_eq!(
            bits_to_set(&range),
            bits_to_set(&mem.timestamp_range(1_000_100, 1_000_200).unwrap())
        );

        assert!(
            source.bytes() < file_size / 4,
            "fetched {} of {} bytes",
            source.bytes(),
            file_size
        );
    }

    /// Full query-kind parity between the in-memory and the ranged reader
    /// over the shared docs dataset (key terms, dense elision, token scans),
    /// plus: a file smaller than the tail window is served entirely from the
    /// single tail fetch.
    #[test]
    fn ranged_small_file_parity_from_one_fetch() {
        let (data, index) = build_docs_dataset_bytes(true);
        let data = Bytes::from(data);
        let index = Bytes::from(index.expect("indexed dataset has a sidecar"));
        let mem = VixReader::open_with_index(data.clone(), Some(index.clone())).unwrap();
        let source = PairSource::new(data, index);
        let ranged = source.open();
        assert_eq!(
            source.fetches(),
            2,
            "a sub-64KiB pair must be fully served by one tail fetch per object"
        );

        let queries = vec![
            exact("level", "error"),
            exact("svc", "api"),
            any_token("timeout"),
            prefix(None, "err"),
            VixQuery::Contains {
                field: None,
                needle: b"time".to_vec(),
                case_insensitive: true,
            },
            VixQuery::Regex {
                field: Some("level".to_string()),
                pattern: "err.*".to_string(),
            },
            VixQuery::KeyExists {
                path: "level".to_string(),
            },
            VixQuery::And(vec![exact("svc", "api"), any_token("timeout")]),
            VixQuery::Or(vec![exact("level", "warn"), exact("level", "error")]),
            VixQuery::Not(Box::new(exact("level", "info"))),
            VixQuery::All,
        ];
        for query in &queries {
            assert_eq!(
                bits_to_set(&ranged.eval(query).unwrap()),
                bits_to_set(&mem.eval(query).unwrap()),
                "parity failure for {query:?}"
            );
            assert_eq!(
                ranged.count(query).unwrap(),
                mem.count(query).unwrap(),
                "count parity failure for {query:?}"
            );
        }
        assert_eq!(
            ranged.keys_with_prefix("").unwrap(),
            mem.keys_with_prefix("").unwrap()
        );
        assert_eq!(
            format!("{:?}", ranged.read_source(&[0, 3, 9]).unwrap()),
            format!("{:?}", mem.read_source(&[0, 3, 9]).unwrap())
        );
        assert_eq!(
            format!("{:?}", ranged.read_docs_column("code").unwrap()),
            format!("{:?}", mem.read_docs_column("code").unwrap())
        );
        assert_eq!(ranged.docs_schema().unwrap(), mem.docs_schema().unwrap());
        assert_eq!(
            bits_to_set(&ranged.timestamp_range(1002, 1008).unwrap()),
            bits_to_set(&mem.timestamp_range(1002, 1008).unwrap())
        );
        // everything above came out of the two tail fetches
        assert_eq!(source.fetches(), 2);
    }

    /// VixDocs over a ranged source: open = tail + docs-blob footer; a
    /// selective scan fetches only the touched chunks and matches the
    /// in-memory scan row for row.
    #[test]
    fn ranged_docs_scan_budget_and_parity() {
        let (data, _index) = build_large_core_file();
        let file_size = data.len() as u64;
        let mem = VixDocs::open(data.clone()).unwrap();
        let source = CountingSource::new(data);
        let ranged = VixDocs::open_ranged(Arc::clone(&source) as Arc<dyn VixRangeSource>).unwrap();
        let open_fetches = source.fetches();
        assert!(
            open_fetches <= 3,
            "docs open should need at most 3 fetches (tail + blob footer), used {open_fetches}"
        );
        assert_eq!(ranged.schema(), mem.schema());
        assert_eq!(ranged.row_count(), mem.row_count());

        let projection = vec!["level".to_string(), "_source".to_string()];
        let rows = vec![17u64, 55_555, 99_999];
        let got = ranged
            .read_docs(Some(&projection), Some(rows.clone()), None)
            .unwrap();
        let expect = mem.read_docs(Some(&projection), Some(rows), None).unwrap();
        assert_eq!(format!("{got:?}"), format!("{expect:?}"));
        let scan_fetches = source.fetches() - open_fetches;
        eprintln!(
            "[ranged budget] docs open={open_fetches} fetches, 3-row scan={scan_fetches} \
             fetches, total bytes={} of {file_size}",
            source.bytes()
        );
        assert!(
            scan_fetches <= 4,
            "3-row docs scan should need at most 4 fetches, used {scan_fetches}"
        );
        assert!(
            source.bytes() < file_size / 3,
            "fetched {} of {} bytes",
            source.bytes(),
            file_size
        );
    }

    /// Data-only opens must not inherit the wider sidecar tail. Both probes
    /// parse the same container in one request, while the compact probe saves
    /// exactly 192 KiB on a production-sized data object.
    #[test]
    fn data_only_tail_probe_avoids_sidecar_read_amplification() {
        let (data, _index) = build_large_core_file();
        assert!(data.len() > 256 * 1024);

        let compact = CountingSource::new(data.clone());
        let compact_dyn: Arc<dyn VixRangeSource> = compact.clone();
        crate::container::parse_container_ranged_with_tail(
            &compact_dyn,
            crate::DEFAULT_TAIL_FETCH_BYTES,
        )
        .unwrap();

        let sidecar_sized = CountingSource::new(data);
        let sidecar_dyn: Arc<dyn VixRangeSource> = sidecar_sized.clone();
        crate::container::parse_container_ranged_with_tail(&sidecar_dyn, 256 * 1024).unwrap();

        assert_eq!(compact.fetches(), 1);
        assert_eq!(sidecar_sized.fetches(), 1);
        assert_eq!(compact.bytes(), crate::DEFAULT_TAIL_FETCH_BYTES);
        assert_eq!(sidecar_sized.bytes(), 256 * 1024);
        assert_eq!(sidecar_sized.bytes() - compact.bytes(), 192 * 1024);
    }

    /// A one-bucket equality needs only the predicate column. The general
    /// histogram remains the parity oracle and additionally reads timestamp.
    #[test]
    fn ranged_eq_string_count_reads_less_than_histogram() {
        let (data, _index) = build_large_core_file();

        let count_source = CountingSource::new(data.clone());
        let count_docs =
            VixDocs::open_ranged_data_only(Arc::clone(&count_source) as Arc<dyn VixRangeSource>)
                .unwrap();
        let count_open_bytes = count_source.bytes();
        let count = count_docs
            .eq_string_count("level", "info")
            .unwrap()
            .unwrap();
        let count_scan_bytes = count_source.bytes() - count_open_bytes;

        let histogram_source = CountingSource::new(data);
        let histogram_docs = VixDocs::open_ranged_data_only(
            Arc::clone(&histogram_source) as Arc<dyn VixRangeSource>
        )
        .unwrap();
        let histogram_open_bytes = histogram_source.bytes();
        let histogram = histogram_docs
            .eq_string_histogram("level", "info", None, 1_000_000, 100_000, 1, 0)
            .unwrap()
            .unwrap();
        let histogram_scan_bytes = histogram_source.bytes() - histogram_open_bytes;

        assert_eq!(count, 33_334);
        assert_eq!(histogram, vec![count]);
        assert!(
            count_scan_bytes < histogram_scan_bytes,
            "count fetched {count_scan_bytes}B, histogram fetched {histogram_scan_bytes}B"
        );
    }

    /// PILOT FIXES round 2, Fix B: docs-blob chunks follow the
    /// `docs_chunk_bytes` byte budget instead of the data row-group size
    /// (128Ki rows in production — the pilot's matched-row fetches
    /// decompressed a whole 100k+-row chunk per point read, 250-800 ms).
    /// Over a 100k-row docs blob with the default 16 MiB budget (M9 flip:
    /// the corpus is sized so the encoded blob still dwarfs 4 budgets —
    /// every bound below is byte-for-byte the pre-flip calibration):
    /// - a full scan decodes many bounded chunks, each within the `[1024, 65536]`-row clamp and
    ///   none row-group-sized;
    /// - fetching ONE row over a ranged source moves at most ~one budget of bytes (the compressed
    ///   chunk), nowhere near the whole blob. Fetched bytes upper-bound decompressed bytes: the
    ///   scan decodes only the chunk it fetched.
    #[test]
    fn docs_chunk_budget_bounds_point_read_bytes() {
        let rows = 100_000usize;
        let budget = crate::DEFAULT_DOCS_CHUNK_BYTES as u64; // 16 MiB
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("level", DataType::Utf8, true),
        ]));
        let opts = VixWriterOptions {
            row_group_size: 128 * 1024, // production PARQUET_MAX_ROW_GROUP_SIZE
            ..Default::default()
        };
        // ~1.9 KiB `_source` rows, mostly random hex (encodes at ~0.5x,
        // measured): the blob stays large — >4 budgets encoded even at the
        // 16 MiB default — and chunk-compression cannot shrink a fetched
        // chunk to noise.
        let mut rng = StdRng::seed_from_u64(0xB0B0);
        let levels = ["info", "warn", "error"];
        let ts: Vec<i64> = (0..rows as i64).map(|i| 2_000_000 + i).collect();
        let level: Vec<&str> = (0..rows).map(|i| levels[i % levels.len()]).collect();
        let sources: Vec<String> = (0..rows)
            .map(|i| {
                let pad: String = (0..224)
                    .map(|_| format!("{:08x}", rng.random::<u32>()))
                    .collect();
                format!(
                    "{{\"_timestamp\":{},\"level\":\"{}\",\"message\":\"processed request \
                     {i} with status ok\",\"payload\":\"{pad}\"}}",
                    ts[i], level[i]
                )
            })
            .collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ts)),
                Arc::new(StringArray::from(level)),
            ],
        )
        .unwrap();
        let mut writer = VixWriter::new(&schema, opts, false);
        writer
            .push_batch_with_source(
                &batch,
                &StringArray::from_iter_values(sources.iter().map(String::as_str)),
                None,
            )
            .unwrap();
        let (bytes, _bytes_index, stats) = writer.finish_with_stats().unwrap();
        let data = Bytes::from(bytes);
        assert!(
            stats.docs_size > 4 * budget,
            "test premise: the docs blob ({}B) must dwarf the budget",
            stats.docs_size
        );

        // (1) chunk shape: bounded, and clearly not one row-group chunk.
        let mem = VixDocs::open(data.clone()).unwrap();
        let mut batch_rows: Vec<usize> = Vec::new();
        mem.scan_docs(None, None, None, &mut |batch| {
            batch_rows.push(batch.num_rows());
            Ok(())
        })
        .unwrap();
        assert_eq!(batch_rows.iter().sum::<usize>(), rows);
        let max_rows = batch_rows.iter().copied().max().unwrap();
        assert!(
            batch_rows.len() >= 4,
            "expected several byte-budget chunks, got {batch_rows:?}"
        );
        assert!(
            (64..=65536).contains(&max_rows),
            "chunk rows must stay within the [64, 65536] clamp, got {max_rows}"
        );

        // (2) one-row point read over a counting ranged source.
        let source = CountingSource::new(data);
        let ranged = VixDocs::open_ranged(Arc::clone(&source) as Arc<dyn VixRangeSource>).unwrap();
        let open_bytes = source.bytes();
        let projection = vec!["_source".to_string()];
        let row = 50_000u64;
        let got = ranged
            .read_docs(Some(&projection), Some(vec![row]), None)
            .unwrap();
        let expect = mem
            .read_docs(Some(&projection), Some(vec![row]), None)
            .unwrap();
        assert_eq!(format!("{got:?}"), format!("{expect:?}"));
        assert_eq!(
            as_string_array(got[0].column(0).as_ref()).value(0),
            sources[row as usize]
        );

        let read_bytes = source.bytes() - open_bytes;
        eprintln!(
            "[docs chunk budget] docs_blob={}B chunks={} (max {max_rows} rows) \
             one-row read fetched={read_bytes}B (budget {budget}B)",
            stats.docs_size,
            batch_rows.len(),
        );
        // ~one chunk: at most the uncompressed budget plus slack for the
        // chunk's metadata/validity segments — decisively not the blob.
        assert!(
            read_bytes <= budget + 512 * 1024,
            "one-row fetch moved {read_bytes}B, budget is {budget}B"
        );
        assert!(
            read_bytes < stats.docs_size / 4,
            "one-row fetch moved {read_bytes} of {}B",
            stats.docs_size
        );
    }

    /// A ranged open of an unsupported-version container fails with the same clear
    /// error as the in-memory open (the tail fetch already carries the
    /// properties).
    #[test]
    fn ranged_open_rejects_unsupported_version() {
        let bytes = crate::container::build_container(
            vec![
                ("version".to_string(), "1".to_string()),
                ("row_count".to_string(), "0".to_string()),
            ],
            vec![],
        )
        .unwrap();
        let source = CountingSource::new(Bytes::from(bytes));
        let err = VixReader::open_ranged(Arc::clone(&source) as Arc<dyn VixRangeSource>)
            .err()
            .expect("containers with an unsupported version must be rejected");
        assert!(
            err.to_string().contains("unsupported .vix format")
                && err.to_string().contains("reader supports 3"),
            "unexpected error: {err}"
        );
    }

    /// A SIDECAR whose footer LACKS the `key_layout` property carries a
    /// foreign (retired token-major) dictionary layout: both open paths
    /// must hard-error instead of misreading the dictionary.
    #[test]
    fn open_rejects_missing_key_layout_property() {
        // fabricate: rebuild the shared fixture's sidecar without the
        // key_layout property (current writers always stamp it)
        let (data, index) = build_large_core_file();
        let stripped = Bytes::from(
            crate::test_support::strip_property_for_tests(
                &index,
                crate::container::PROP_KEY_LAYOUT,
            )
            .unwrap(),
        );

        let check = |msg: String| {
            assert!(
                msg.contains("unsupported .vix format") && msg.contains("key_layout"),
                "unexpected error: {msg}"
            );
        };
        check(
            VixReader::open_with_index(data.clone(), Some(stripped.clone()))
                .err()
                .expect("a sidecar without key_layout must be rejected")
                .to_string(),
        );
        let data_source = CountingSource::new(data);
        let index_source = CountingSource::new(stripped);
        check(
            VixReader::open_ranged_with_index(
                Arc::clone(&data_source) as Arc<dyn VixRangeSource>,
                Some(Arc::clone(&index_source) as Arc<dyn VixRangeSource>),
            )
            .err()
            .expect("a ranged open without key_layout must be rejected")
            .to_string(),
        );
    }

    /// The invariant the single-file healing rebuild relies on (prod
    /// 2026-07-29: merged files with corrupt dictionaries): when the DICT
    /// blob is unreadable, the reader open fails but [`crate::VixDocs`]
    /// still opens — classify routes such files to the rebuild-from-
    /// `_source` path instead of erroring.
    #[test]
    fn docs_open_survives_corrupt_dict() {
        let (data, index) = build_large_core_file();
        // mangle the dictionary blob inside the sidecar, in place
        let dict_range = crate::test_support::blob_byte_range(&index, "dict").unwrap();
        let mut corrupted = index.to_vec();
        corrupted[dict_range.start + dict_range.len() / 2..dict_range.end].fill(0xFF);
        let corrupted = Bytes::from(corrupted);
        let data_source = CountingSource::new(data.clone());
        let index_source = CountingSource::new(corrupted);
        VixReader::open_ranged_with_index(
            Arc::clone(&data_source) as Arc<dyn VixRangeSource>,
            Some(Arc::clone(&index_source) as Arc<dyn VixRangeSource>),
        )
        .and_then(|reader| {
            // the block index parses lazily on some paths: force a
            // dictionary touch so the corruption must surface
            reader.eval(&exact("svc", "svc_000001"))
        })
        .err()
        .expect("a corrupt dictionary must fail the reader open/eval");
        crate::VixDocs::open_ranged(Arc::clone(&data_source) as Arc<dyn VixRangeSource>)
            .expect("docs must open even when the dictionary is unreadable");
    }

    // -----------------------------------------------------------------
    // M6 fetch-accounting pins: the M5 A/B caught the PASSTHROUGH merge
    // output defeating projected ranged reads (a 2-column projection
    // fetched the whole interleaved docs blob; a needle selection paid
    // one GET per surviving chunk). These pin the fixed fetch profile on
    // a passthrough-built file so the regression class cannot silently
    // return.
    // -----------------------------------------------------------------

    /// A wide-ish passthrough MERGE output: a first-encode input (many
    /// small pushed chunks, incompressible-fat `_source` dominating the
    /// blob) spliced chunk-by-chunk through a `docs_passthrough` writer —
    /// the exact path compaction merges take.
    fn build_wide_passthrough_file() -> Bytes {
        use arrow::datatypes::{DataType, Field, Schema};
        let rows = 24_000usize;
        let schema = Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("duration", DataType::Int64, true),
            Field::new("code", DataType::Int64, true),
            Field::new("svc", DataType::Utf8, true),
            Field::new("level", DataType::Utf8, true),
            Field::new("url", DataType::Utf8, true),
        ]);
        let mut rng = StdRng::seed_from_u64(0x4d36);
        let ts: Vec<i64> = (0..rows).map(|i| 2_000_000 - i as i64).collect();
        let duration: Vec<i64> = (0..rows).map(|_| rng.random_range(0..1_000_000)).collect();
        let code: Vec<i64> = (0..rows).map(|i| (i % 7) as i64).collect();
        let svc: Vec<String> = (0..rows).map(|i| format!("svc_{:02}", i % 13)).collect();
        let level: Vec<String> = (0..rows)
            .map(|i| ["info", "warn", "error"][i % 3].to_string())
            .collect();
        let url: Vec<String> = (0..rows)
            .map(|i| format!("/api/v1/item/{}", i % 97))
            .collect();
        // incompressible padding keeps `_source` the dominant blob share,
        // like real log/trace payloads
        let sources: Vec<String> = (0..rows)
            .map(|i| {
                let pad: String = (0..288)
                    .map(|_| char::from(b'a' + rng.random_range(0..26u8)))
                    .collect();
                format!("{{\"i\":{i},\"pad\":\"{pad}\"}}")
            })
            .collect();

        // first-encode input with many small chunks (the merged-file shape
        // whose encoded chunks the passthrough copies)
        let input = {
            let mut writer = VixWriter::new(
                &schema,
                VixWriterOptions {
                    docs_chunk_bytes: 64 * 1024,
                    ..Default::default()
                },
                false,
            );
            let batch = RecordBatch::try_new(
                Arc::new(schema.clone()),
                vec![
                    Arc::new(Int64Array::from(ts.clone())) as ArrayRef,
                    Arc::new(Int64Array::from(duration.clone())) as ArrayRef,
                    Arc::new(Int64Array::from(code.clone())) as ArrayRef,
                    Arc::new(StringArray::from(svc.clone())) as ArrayRef,
                    Arc::new(StringArray::from(level.clone())) as ArrayRef,
                    Arc::new(StringArray::from(url.clone())) as ArrayRef,
                ],
            )
            .unwrap();
            let source = StringArray::from(sources.clone());
            writer
                .push_batch_with_source(&batch, &source, None)
                .unwrap();
            let (data, _) = writer.finish().unwrap();
            VixDocs::open(Bytes::from(data)).unwrap()
        };
        assert!(
            input.zone_chunks().unwrap().len() >= 16,
            "fixture must have many pushed chunks, got {}",
            input.zone_chunks().unwrap().len()
        );

        // splice it through a passthrough writer (docs from encoded chunks,
        // index from the pre-push)
        let mut writer = VixWriter::new(
            &schema,
            VixWriterOptions {
                docs_passthrough: true,
                ..Default::default()
            },
            false,
        );
        writer
            .push_docs_rows_index_only(
                &Int64Array::from(ts.clone()),
                &[],
                &StringArray::from(sources.clone()),
                None,
            )
            .unwrap();
        let entries: Vec<crate::ZoneEntry> = input
            .zone_chunks()
            .unwrap()
            .iter()
            .map(|zone| (zone.row_count, zone.ts_min, zone.ts_max))
            .collect();
        let stats = input.spliceable_stats().unwrap().unwrap();
        writer
            .begin_docs_encoded_run(
                rows as u64,
                *ts.last().unwrap(),
                ts[0],
                &entries,
                &stats,
                Some(&[rows as u64]),
            )
            .unwrap();
        input
            .scan_docs_encoded_chunks(&mut |chunk| writer.push_docs_encoded_chunk(chunk))
            .unwrap();
        writer.finish_docs_encoded_run().unwrap();
        let (data, _) = writer.finish().unwrap();
        Bytes::from(data)
    }

    /// M6 pin (t1): a 2-column projection over a wide passthrough file
    /// must fetch a small fraction of the blob — the M5 regression fetched
    /// essentially ALL of it (interleaved leaves bridged by the ≤1 MiB
    /// coalescer gap into whole-blob spans).
    #[test]
    fn passthrough_projection_fetch_budget() {
        let data = build_wide_passthrough_file();
        let file_size = data.len() as u64;
        let mem = VixDocs::open(data.clone()).unwrap();
        let source = CountingSource::new(data);
        let ranged = VixDocs::open_ranged(Arc::clone(&source) as Arc<dyn VixRangeSource>).unwrap();
        let projection = ["_timestamp".to_string(), "duration".to_string()];

        for selection in [
            None,
            // dense uniform 10% selection (the M5 regression shape)
            Some((0..mem.row_count()).step_by(10).collect::<Vec<u64>>()),
        ] {
            let before_bytes = source.bytes();
            let mut ranged_rows = 0u64;
            let mut batches = 0usize;
            ranged
                .scan_docs(Some(&projection), selection.clone(), None, &mut |batch| {
                    ranged_rows += batch.num_rows() as u64;
                    batches += 1;
                    Ok(())
                })
                .unwrap();
            let fetched = source.bytes() - before_bytes;
            assert!(
                fetched < file_size * 15 / 100,
                "2-column projection (selection={}) fetched {fetched} of {file_size} bytes \
                 (>{}%) — the projected ranged read is pulling unprojected columns again",
                selection.is_some(),
                fetched * 100 / file_size,
            );
            // coalesced narrow columns also mean coarse decode batches (the
            // M5 in-memory +45% came from per-pushed-chunk batches)
            assert!(
                batches <= 8,
                "2-column scan decoded {batches} batches — run coalescing is not active"
            );
            let mut mem_rows = 0u64;
            mem.scan_docs(Some(&projection), selection, None, &mut |batch| {
                mem_rows += batch.num_rows() as u64;
                Ok(())
            })
            .unwrap();
            assert_eq!(ranged_rows, mem_rows, "ranged/in-memory row parity");
        }
    }

    /// M6 pin (t2): a needle selection must batch its fetches through the
    /// coalescer — the M5 regression issued ONE GET PER SURVIVING CHUNK
    /// (1600 round trips for 1600 rows; deadly on S3 latency).
    #[test]
    fn passthrough_needle_fetch_budget() {
        let data = build_wide_passthrough_file();
        let file_size = data.len() as u64;
        let mem = VixDocs::open(data.clone()).unwrap();
        let source = CountingSource::new(data);
        let ranged = VixDocs::open_ranged(Arc::clone(&source) as Arc<dyn VixRangeSource>).unwrap();
        let projection = ["_timestamp".to_string(), "duration".to_string()];

        // one row per thousand: 24 scattered needles
        let needles: Vec<u64> = (0..mem.row_count()).step_by(1000).collect();
        let selected = needles.len();
        let before_fetches = source.fetches();
        let before_bytes = source.bytes();
        let mut rows = 0u64;
        ranged
            .scan_docs(
                Some(&projection),
                Some(needles.clone()),
                None,
                &mut |batch| {
                    rows += batch.num_rows() as u64;
                    Ok(())
                },
            )
            .unwrap();
        let fetches = source.fetches() - before_fetches;
        let fetched = source.bytes() - before_bytes;
        assert_eq!(rows, selected as u64);
        assert!(
            fetches <= 12,
            "needle selection of {selected} rows issued {fetches} fetches — \
             selection-driven fetches are not batching through the coalescer"
        );
        assert!(
            fetches < selected,
            "fetch count {fetches} >= selected rows {selected}: the per-chunk \
             round-trip pathology is back"
        );
        assert!(
            fetched < file_size * 15 / 100,
            "needle fetched {fetched} of {file_size} bytes (>{}%)",
            fetched * 100 / file_size,
        );
        // row parity with the in-memory scan
        let mut mem_rows = 0u64;
        mem.scan_docs(Some(&projection), Some(needles), None, &mut |batch| {
            mem_rows += batch.num_rows() as u64;
            Ok(())
        })
        .unwrap();
        assert_eq!(rows, mem_rows);
    }
    /// Parallel k-way ranges share one dictionary-block load per input.
    /// Re-reading the whole ranged dictionary inside every range multiplied
    /// compactor disk/S3 bytes by roughly `4 * merge_threads`.
    #[test]
    fn parallel_index_merge_does_not_refetch_dictionary_per_range() {
        fn merge_fetch_cost(threads: usize) -> (usize, u64) {
            let (data, index) = build_large_core_file();
            let sources = [
                PairSource::new(data.clone(), index.clone()),
                PairSource::new(data, index),
            ];
            let readers: Vec<VixReader> = sources.iter().map(PairSource::open).collect();
            let refs: Vec<&VixReader> = readers.iter().collect();
            let before_fetches: usize = sources.iter().map(PairSource::fetches).sum();
            let before_bytes: u64 = sources.iter().map(PairSource::bytes).sum();
            let schema = Arc::new(Schema::new(vec![
                Field::new("_timestamp", DataType::Int64, false),
                Field::new("svc", DataType::Utf8, true),
                Field::new("level", DataType::Utf8, true),
            ]));
            let mut writer = VixWriter::new(
                &schema,
                VixWriterOptions {
                    merge_kway_threads: threads,
                    ..Default::default()
                },
                false,
            );
            let rows = readers[0].row_count() as u32;
            writer
                .merge_input_indexes(
                    &refs,
                    &[crate::DocIdMap::Offset(0), crate::DocIdMap::Offset(rows)],
                    threads,
                )
                .unwrap();
            (
                sources.iter().map(PairSource::fetches).sum::<usize>() - before_fetches,
                sources.iter().map(PairSource::bytes).sum::<u64>() - before_bytes,
            )
        }

        let sequential = merge_fetch_cost(1);
        let parallel = merge_fetch_cost(4);
        assert_eq!(
            parallel, sequential,
            "parallel key ranges must not multiply ranged dictionary reads"
        );
    }
}

/// AND evaluation short-circuits: a leaf that matches no term proves the
/// intersection empty without touching postings, in any position, in any
/// combination with composites; identities and composite children keep
/// their semantics; `count` agrees with `eval` on every shape.
#[test]
fn and_short_circuit_semantics() {
    let reader = build_dataset(dataset_options());
    let all: BTreeSet<u32> = (0..10).collect();
    let missing = exact("level", "no-such-value");
    let common = exact("svc", "api");

    // a missing leaf empties the AND regardless of position
    for query in [
        VixQuery::And(vec![missing.clone(), common.clone()]),
        VixQuery::And(vec![common.clone(), missing.clone()]),
        VixQuery::And(vec![common.clone(), any_token("no-such-token")]),
        VixQuery::And(vec![
            common.clone(),
            missing.clone(),
            VixQuery::Not(Box::new(common.clone())),
        ]),
    ] {
        assert_eq!(eval_set(&reader, &query), docs(&[]), "{query:?}");
        assert_eq!(reader.count(&query).unwrap(), 0, "{query:?}");
    }

    // All is the AND identity; a lone composite child keeps its result
    assert_eq!(
        eval_set(&reader, &VixQuery::And(vec![VixQuery::All])),
        all.clone()
    );
    assert_eq!(
        eval_set(&reader, &VixQuery::And(vec![VixQuery::All, common.clone()])),
        eval_set(&reader, &common)
    );
    assert_eq!(
        eval_set(
            &reader,
            &VixQuery::And(vec![VixQuery::Not(Box::new(missing.clone()))])
        ),
        all
    );

    // leaves and composites mix: (svc=api) AND (level=error OR level=warn)
    assert_eq!(
        eval_set(
            &reader,
            &VixQuery::And(vec![
                common.clone(),
                VixQuery::Or(vec![exact("level", "error"), exact("level", "warn")]),
            ])
        ),
        docs(&[1])
    );
    // rarest-first ordering must not change results: three leaves of
    // different selectivity
    assert_eq!(
        eval_set(
            &reader,
            &VixQuery::And(vec![
                prefix(None, ""),        // every term
                common.clone(),          // 4 docs
                exact("level", "error"), // 3 docs
            ])
        ),
        docs(&[1])
    );
}

/// Leaf `count` fast paths: a single matched term is served from the
/// `doc_count` column; multiple matched terms sharing documents still count
/// distinct docs via the postings union.
#[test]
fn count_leaf_ordinal_paths() {
    let reader = build_dataset(dataset_options());
    // single term: 'login' appears in one doc's log tokens only
    assert_eq!(reader.count(&any_token("login")).unwrap(), 1);
    // 'timeout' tokens: docs 1, 6, 8 (fields differ, same token)
    assert_eq!(reader.count(&any_token("timeout")).unwrap(), 3);
    // prefix 'err' matches the 'error' term in level AND in log tokens —
    // doc 1/5/8 carry both, doc 0 only the token: distinct docs, not the
    // doc_count sum
    let q = prefix(None, "err");
    let evaluated = reader.eval(&q).unwrap().count_set_bits() as u64;
    assert_eq!(reader.count(&q).unwrap(), evaluated);
    assert_eq!(evaluated, 4);
    // missing tokens count zero without IO
    assert_eq!(reader.count(&any_token("zzz-nope")).unwrap(), 0);
}

/// Build a core file with `rows` docs whose `svc` column cycles through a
/// small vocabulary (every ~7th row null): enough rows for several docs-blob
/// chunks, so dictionary reads must accumulate across per-chunk value sets.
fn build_multichunk_dataset(rows: usize) -> VixReader {
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("svc", DataType::Utf8, true),
    ]));
    let opts = VixWriterOptions {
        // 1-byte budget clamps to the 1024-row chunk floor: several chunks
        docs_chunk_bytes: 1,
        ..Default::default()
    };
    let mut writer = VixWriter::new(&schema, opts, false);
    let vocab = ["api", "auth", "db", "web", "cron"];
    let ts: Vec<i64> = (0..rows as i64).map(|i| 1_000_000 - i).collect();
    let svc: Vec<Option<&str>> = (0..rows)
        .map(|i| (i % 7 != 6).then(|| vocab[i % vocab.len()]))
        .collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ts.clone())) as ArrayRef,
            Arc::new(StringArray::from(svc)),
        ],
    )
    .unwrap();
    // fat sources: the layout writer coalesces chunks toward ~1 MiB, so
    // only multi-MiB docs data produces real chunk boundaries
    let pad = "x".repeat(2048);
    let sources: Vec<String> = ts
        .iter()
        .map(|t| format!("{{\"_timestamp\":{t},\"pad\":\"{pad}\"}}"))
        .collect();
    writer
        .push_batch_with_source(&batch, &StringArray::from(sources), None)
        .unwrap();
    finish_open(writer)
}

/// Dictionary-form column reads reassemble to exactly the canonical column
/// (nulls included). The reassembly loop treats chunks independently
/// (per-chunk value sets), however many the scan yields; unknown columns
/// keep the ColumnNotFound contract.
#[test]
fn docs_column_dict_matches_canonical() {
    // thousands of rows through the real dict encodings, nulls included
    let multichunk = build_multichunk_dataset(2600);
    let chunks = multichunk.read_docs_column_dict("svc").unwrap();
    let canonical = as_string_array(multichunk.read_docs_column("svc").unwrap().as_ref());
    let mut row = 0usize;
    for chunk in &chunks {
        let values = as_string_array(chunk.values.as_ref());
        for i in 0..chunk.codes.len() {
            if chunk.codes.is_null(i) {
                assert!(canonical.is_null(row), "row {row}");
            } else {
                assert_eq!(
                    values.value(chunk.codes.value(i) as usize),
                    canonical.value(row),
                    "row {row}"
                );
            }
            row += 1;
        }
    }
    assert_eq!(row as u64, multichunk.row_count());

    for reader in [build_docs_dataset(false), build_dataset(dataset_options())] {
        for column in ["svc", "code"] {
            let canonical = as_string_array(reader.read_docs_column(column).unwrap().as_ref());
            let chunks = reader.read_docs_column_dict(column).unwrap();
            let total: usize = chunks.iter().map(|c| c.codes.len()).sum();
            assert_eq!(total as u64, reader.row_count());
            let mut row = 0usize;
            for chunk in &chunks {
                let values = as_string_array(chunk.values.as_ref());
                for i in 0..chunk.codes.len() {
                    if chunk.codes.is_null(i) {
                        assert!(canonical.is_null(row), "row {row} of {column}");
                    } else {
                        let code = chunk.codes.value(i) as usize;
                        assert!(!values.is_null(code));
                        assert_eq!(
                            values.value(code),
                            canonical.value(row),
                            "row {row} of {column}"
                        );
                    }
                    row += 1;
                }
            }
        }
        assert!(reader.read_docs_column_dict("missing-column").is_err());
    }
}

/// Manual #29 harness: times the unfiltered value-enumeration walk
/// (`field_value_counts`) behind unfiltered TopN/Distinct and reports the
/// peak-RSS it costs — the "allocation bomb" baseline and its fix's
/// before/after instrument.
/// `O2_VIX_FILE=<file> [O2_VIX_FIELD=trace_id] cargo test -p vortex_index
/// --release bench_unfiltered_value_walk -- --ignored --nocapture`
#[test]
#[ignore = "manual #29 profiling against a real file (set O2_VIX_FILE)"]
fn bench_unfiltered_value_walk() {
    let Ok(path) = std::env::var("O2_VIX_FILE") else {
        eprintln!("O2_VIX_FILE not set; skipping");
        return;
    };
    let field = std::env::var("O2_VIX_FIELD").unwrap_or_else(|_| "trace_id".to_string());
    let vm_hwm_kb = || -> u64 {
        std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|s| {
                s.lines()
                    .find(|l| l.starts_with("VmHWM:"))
                    .and_then(|l| l.split_whitespace().nth(1).and_then(|v| v.parse().ok()))
            })
            .unwrap_or(0)
    };
    let data = Bytes::from(std::fs::read(&path).expect("read vix file"));
    let reader = VixReader::open(data).expect("open");
    eprintln!("file={path} rows={} field={field}", reader.row_count());
    // the #29 lever-1 path first, while the process high-water mark is
    // still low — its footprint would be invisible after the walk
    let before_kb = vm_hwm_kb();
    for i in 0..3 {
        let t = std::time::Instant::now();
        let top = reader.field_value_top_k(&field, 1000, false).unwrap();
        let wall = t.elapsed();
        let n = top.as_ref().map(|(c, truncated)| (c.len(), *truncated));
        eprintln!(
            "top_k iter={i} wall={wall:?} kept={n:?} vm_hwm_delta_kb={}",
            vm_hwm_kb().saturating_sub(before_kb)
        );
        std::hint::black_box(top);
    }
    let before_kb = vm_hwm_kb();
    for i in 0..3 {
        let t = std::time::Instant::now();
        let counts = reader.field_value_counts(&field).unwrap();
        let wall = t.elapsed();
        let n = counts.as_ref().map(|c| c.len());
        eprintln!(
            "walk  iter={i} wall={wall:?} distinct={n:?} vm_hwm_delta_kb={}",
            vm_hwm_kb().saturating_sub(before_kb)
        );
        std::hint::black_box(counts);
    }
}

/// Manual profiling harness over a REAL benchmark `.vix` file (never part of
/// CI): `O2_VIX_FILE=/path/to/file.vix cargo test -p vortex_index --release
/// bench_real_vix_file -- --ignored --nocapture`. Times the ops behind the
/// benchmark's losing queries and dumps the docs-blob encoding tree of the
/// TopN group column.
#[test]
#[ignore = "manual profiling against a real benchmark file (set O2_VIX_FILE)"]
fn bench_real_vix_file() {
    let Ok(path) = std::env::var("O2_VIX_FILE") else {
        eprintln!("O2_VIX_FILE not set; skipping");
        return;
    };
    let data = Bytes::from(std::fs::read(&path).expect("read vix file"));
    let reader = VixReader::open(data.clone()).expect("open");
    println!(
        "file={} bytes={} rows={} rgs={}",
        path,
        data.len(),
        reader.row_count(),
        reader.term_row_group_count()
    );

    let time = |name: &str, iters: usize, f: &mut dyn FnMut() -> u64| {
        let check = f(); // warm
        let start = std::time::Instant::now();
        for _ in 0..iters {
            std::hint::black_box(f());
        }
        let per = start.elapsed() / iters as u32;
        println!("{name:55} {per:>12?}/iter (result {check})");
    };

    let exact = |field: &str, token: &str| VixQuery::Exact {
        field: field.to_string(),
        token: token.as_bytes().to_vec(),
    };
    let trace = exact("trace_id", "299cb1bc60705f72a45c7d9f70a82156");
    let container = exact("kubernetes_container_name", "api-gateway-container");
    let pod = exact("kubernetes_pod_name", "api-gateway-9a2c1e6f4-8923f");
    let failed = VixQuery::TokenAnyField {
        token: b"failed".to_vec(),
    };
    let rare_tok = VixQuery::TokenAnyField {
        token: b"474dbfe4c65a971176ae239869a6ba47".to_vec(),
    };

    time("eval Exact trace_id (needle)", 20, &mut || {
        reader.eval(&trace).unwrap().count_set_bits() as u64
    });
    time("count Exact pod_name (doc_count read)", 20, &mut || {
        reader.count(&pod).unwrap()
    });
    time("eval Exact pod_name (postings)", 20, &mut || {
        reader.eval(&pod).unwrap().count_set_bits() as u64
    });
    time("eval Exact container (postings)", 20, &mut || {
        reader.eval(&container).unwrap().count_set_bits() as u64
    });
    time("eval And [container, trace]", 20, &mut || {
        reader
            .eval(&VixQuery::And(vec![container.clone(), trace.clone()]))
            .unwrap()
            .count_set_bits() as u64
    });
    time("eval And [failed, rare_token]", 20, &mut || {
        reader
            .eval(&VixQuery::And(vec![failed.clone(), rare_tok.clone()]))
            .unwrap()
            .count_set_bits() as u64
    });
    time("eval TokenAnyField failed (postings)", 20, &mut || {
        reader.eval(&failed).unwrap().count_set_bits() as u64
    });
    time("count TokenAnyField failed", 20, &mut || {
        reader.count(&failed).unwrap()
    });
    time("timestamp_range full column", 5, &mut || {
        reader
            .timestamp_range(0, i64::MAX)
            .unwrap()
            .count_set_bits() as u64
    });
    time("read_docs_column namespace (strings)", 5, &mut || {
        let col = reader
            .read_docs_column("kubernetes_namespace_name")
            .unwrap();
        col.len() as u64
    });
    // mimic collect::simple_top_n over the whole column
    time("simple_top_n namespace (cast+hash strings)", 5, &mut || {
        use arrow::array::StringArray;
        let col = reader
            .read_docs_column("kubernetes_namespace_name")
            .unwrap();
        let col = arrow::compute::cast(&col, &arrow::datatypes::DataType::Utf8).unwrap();
        let strings = col.as_any().downcast_ref::<StringArray>().unwrap();
        let mut counts: std::collections::HashMap<&str, u64> = Default::default();
        for i in 0..strings.len() {
            if !strings.is_null(i) {
                *counts.entry(strings.value(i)).or_insert(0) += 1;
            }
        }
        counts.len() as u64
    });
    // the dict-code grouping path (Fix 2)
    time("top_n namespace via dict codes", 5, &mut || {
        let chunks = reader
            .read_docs_column_dict("kubernetes_namespace_name")
            .unwrap();
        let mut counts: std::collections::HashMap<String, u64> = Default::default();
        for chunk in chunks {
            let mut per_code = vec![0u64; chunk.values.len()];
            for i in 0..chunk.codes.len() {
                if chunk.codes.is_valid(i) {
                    per_code[chunk.codes.value(i) as usize] += 1;
                }
            }
            let values = as_string_array(chunk.values.as_ref());
            for (code, n) in per_code.into_iter().enumerate() {
                if n > 0 && !values.is_null(code) {
                    *counts.entry(values.value(code).to_string()).or_insert(0) += n;
                }
            }
        }
        counts.len() as u64
    });

    // ---- ranged-mode fetch budget per op (the server's read path) ----
    {
        use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

        use futures::FutureExt;

        struct BenchSource {
            data: Bytes,
            fetches: AtomicUsize,
            bytes: AtomicU64,
        }
        impl crate::VixRangeSource for BenchSource {
            fn len(&self) -> u64 {
                self.data.len() as u64
            }
            fn fetch(
                &self,
                range: std::ops::Range<u64>,
            ) -> futures::future::BoxFuture<'static, anyhow::Result<Bytes>> {
                self.fetches.fetch_add(1, Ordering::Relaxed);
                self.bytes
                    .fetch_add(range.end - range.start, Ordering::Relaxed);
                let out = self.data.slice(range.start as usize..range.end as usize);
                async move { Ok(out) }.boxed()
            }
        }
        let source = Arc::new(BenchSource {
            data: data.clone(),
            fetches: AtomicUsize::new(0),
            bytes: AtomicU64::new(0),
        });
        let ranged =
            VixReader::open_ranged(Arc::clone(&source) as Arc<dyn crate::VixRangeSource>).unwrap();
        let fetch_probe = |name: &str, f: &mut dyn FnMut() -> u64| {
            f(); // warm (footer caches fill)
            let f0 = source.fetches.load(Ordering::Relaxed);
            let b0 = source.bytes.load(Ordering::Relaxed);
            let start = std::time::Instant::now();
            let result = f();
            let took = start.elapsed();
            println!(
                "{name:55} {took:>12?} fetches={} bytes={} (result {result})",
                source.fetches.load(Ordering::Relaxed) - f0,
                source.bytes.load(Ordering::Relaxed) - b0,
            );
        };
        fetch_probe("[ranged] count Exact pod_name", &mut || {
            ranged.count(&pod).unwrap()
        });
        fetch_probe("[ranged] eval Exact container", &mut || {
            ranged.eval(&container).unwrap().count_set_bits() as u64
        });
        fetch_probe("[ranged] eval And [container, trace]", &mut || {
            ranged
                .eval(&VixQuery::And(vec![container.clone(), trace.clone()]))
                .unwrap()
                .count_set_bits() as u64
        });
        fetch_probe("[ranged] count TokenAnyField failed", &mut || {
            ranged.count(&failed).unwrap()
        });
        fetch_probe("[ranged] top_n namespace via dict codes", &mut || {
            ranged
                .read_docs_column_dict("kubernetes_namespace_name")
                .unwrap()
                .len() as u64
        });
    }

    // ---- docs-blob encoding probe of the TopN group column ----
    {
        use vortex::{
            VortexSessionDefault,
            expr::{root, select},
            file::OpenOptionsSessionExt,
            io::{
                runtime::{BlockingRuntime, single::SingleThreadRuntime},
                session::RuntimeSessionExt,
            },
            session::VortexSession,
        };
        let container_env = crate::container::parse_container(&data).unwrap();
        let docs = container_env.docs.expect("docs blob");
        let crate::container::BlobHandle::Mem(docs_bytes) = docs else {
            panic!("expected in-memory blob");
        };
        let runtime = SingleThreadRuntime::default();
        let session = VortexSession::default().with_handle(runtime.handle());
        let vxf = session.open_options().open_buffer(docs_bytes).unwrap();
        let scan = vxf
            .scan()
            .unwrap()
            .with_projection(select(vec!["kubernetes_namespace_name"], root()));
        fn dump(prefix: &str, name: &str, array: &vortex::array::ArrayRef, depth: usize) {
            if depth > 6 {
                return;
            }
            println!(
                "{prefix}{name}: {} len={} ",
                array.encoding_id(),
                array.len()
            );
            let names = array.children_names();
            for (i, child) in array.children().iter().enumerate() {
                let child_name = names.get(i).cloned().unwrap_or_else(|| format!("{i}"));
                dump(&format!("{prefix}  "), &child_name, child, depth + 1);
            }
        }
        let mut chunk_no = 0usize;
        for array in scan.into_array_iter(&runtime).unwrap() {
            let array = array.unwrap();
            if chunk_no < 2 {
                dump("", &format!("docs-chunk-{chunk_no}"), &array, 0);
            }
            chunk_no += 1;
        }
        println!("docs chunks total: {chunk_no}");
    }
}

/// Stage-2 plist WRITER ([`VixWriterOptions::postings_plist_min_docs`]):
/// terms at/above the threshold store their postings out-of-row in the
/// `plist` blob behind a 12-byte pointer cell; dense elision keeps
/// precedence; `0` (default) is byte-identical to the pre-plist writer.
mod plist {
    use super::*;
    use crate::query::KEY_FIELD_ID;

    /// Every term of the file via the public enumeration (pointer cells
    /// resolved through the plist blob, dense terms expanded).
    fn all_terms(reader: &VixReader) -> Vec<(Vec<u8>, u64, Vec<u32>)> {
        let mut out = Vec::new();
        reader
            .for_each_term(&mut |key, doc_count, ids| {
                out.push((key.to_vec(), doc_count, ids.to_vec()));
                Ok(())
            })
            .unwrap();
        out
    }

    fn plist_options(min_docs: u32) -> VixWriterOptions {
        VixWriterOptions {
            postings_plist_min_docs: min_docs,
            ..dataset_options()
        }
    }

    /// Round-trip at threshold 4 over the 10-doc dataset: terms at/above 4
    /// docs live out-of-row (12-byte pointer cells into the `plist` blob),
    /// terms below stay inline exactly as before, dense terms stay elided,
    /// the property persists, and the term walk resolves everything back to
    /// the plist-less baseline's stream.
    #[test]
    fn plist_roundtrip_above_and_below_threshold() {
        let baseline = build_dataset(dataset_options());
        let (bytes, bytes_index) = build_dataset_bytes(plist_options(4));

        // sidecar: capability property + plist blob present
        let meta = puffin::reader::parse_puffin_footer_from_bytes(
            bytes_index.as_deref().expect("sidecar"),
        )
        .unwrap();
        assert_eq!(meta.properties["plist_min_docs"], "4");
        assert!(
            meta.blobs
                .iter()
                .any(|blob| blob.blob_type == "o2-vix-plist-v1"
                    && blob.properties["blob_tag"] == "plist"),
            "plist blob missing from the sidecar"
        );

        let reader = open_built(bytes, bytes_index);
        assert_eq!(reader.plist_min_docs(), 4);

        // cell shapes — pointer (exactly 12 bytes) at/above the threshold:
        // the `level` key term covers 9 docs, svc="api" covers 4
        let svc_id = reader.field_id("svc").unwrap();
        let level_id = reader.field_id("level").unwrap();
        assert_eq!(
            reader.debug_postings_len(b"level", KEY_FIELD_ID).unwrap(),
            Some(12)
        );
        assert_eq!(reader.debug_postings_len(b"api", svc_id).unwrap(), Some(12));
        // inline below it (level="error" covers 3 docs), byte-length-equal
        // to the baseline's inline cell
        assert_eq!(
            reader.debug_postings_len(b"error", level_id).unwrap(),
            baseline.debug_postings_len(b"error", level_id).unwrap()
        );
        // dense-elided terms keep the EMPTY cell even above the threshold
        assert_eq!(
            reader.debug_postings_len(b"svc", KEY_FIELD_ID).unwrap(),
            Some(0)
        );

        // the full walk (pointer resolution included) matches the baseline
        let terms = all_terms(&reader);
        assert_eq!(terms, all_terms(&baseline));
        // sanity: some walked term really crossed the threshold un-densely
        assert!(
            terms
                .iter()
                .any(|(_, doc_count, ids)| *doc_count >= 4 && (ids.len() as u64) < 10)
        );
    }

    /// Stage 3: the QUERY paths resolve pointer cells — `eval`
    /// (postings_union under every leaf/compound) and `count` answer
    /// identically over the plist build and the plist-less baseline, across
    /// pointer, inline, dense, and mixed shapes.
    #[test]
    fn plist_pointer_cells_answer_queries() {
        let baseline = build_dataset(dataset_options());
        let plist = build_dataset(plist_options(4));

        let queries = [
            // pointer cell (svc="api": 4 docs at threshold 4)
            VixQuery::Exact {
                field: "svc".to_string(),
                token: b"api".to_vec(),
            },
            // pointer via the key-exists path (`level` key term: 9 docs)
            VixQuery::KeyExists {
                path: "level".to_string(),
            },
            // multi-ordinal union: pointer + inline in one decode loop
            VixQuery::Or(vec![
                VixQuery::Exact {
                    field: "svc".to_string(),
                    token: b"api".to_vec(),
                },
                VixQuery::Exact {
                    field: "level".to_string(),
                    token: b"error".to_vec(),
                },
            ]),
            // compound over pointer cells
            VixQuery::And(vec![
                VixQuery::KeyExists {
                    path: "level".to_string(),
                },
                VixQuery::Exact {
                    field: "svc".to_string(),
                    token: b"api".to_vec(),
                },
            ]),
            VixQuery::Not(Box::new(VixQuery::Exact {
                field: "svc".to_string(),
                token: b"api".to_vec(),
            })),
        ];
        for query in &queries {
            assert_eq!(
                plist.eval(query).unwrap().set_indices().collect::<Vec<_>>(),
                baseline
                    .eval(query)
                    .unwrap()
                    .set_indices()
                    .collect::<Vec<_>>(),
                "eval mismatch for {query:?}"
            );
            assert_eq!(
                plist.count(query).unwrap(),
                baseline.count(query).unwrap(),
                "count mismatch for {query:?}"
            );
        }
    }

    /// Dense-elision precedence: with threshold 1 EVERY non-dense term
    /// becomes a pointer cell, yet a term present in every row keeps its
    /// empty cell — never a pointer — and the reader keeps synthesizing it
    /// from `doc_count` alone.
    #[test]
    fn plist_dense_elision_takes_precedence() {
        let reader = build_dataset(plist_options(1));
        // svc and code are non-null in all 10 docs: dense key terms
        assert_eq!(
            reader.debug_postings_len(b"svc", KEY_FIELD_ID).unwrap(),
            Some(0)
        );
        assert_eq!(
            reader.debug_postings_len(b"code", KEY_FIELD_ID).unwrap(),
            Some(0)
        );
        // while a single-doc term goes out-of-row at threshold 1
        let code_id = reader.field_id("code").unwrap();
        assert_eq!(
            reader
                .debug_postings_len(&crate::numeric_value_token("1"), code_id)
                .unwrap(),
            Some(12)
        );
        // the walk still expands dense terms and resolves every pointer
        assert_eq!(
            all_terms(&reader),
            all_terms(&build_dataset(dataset_options()))
        );
    }

    /// Feature off (`postings_plist_min_docs: 0`): no plist blob, no
    /// property, and the whole container is byte-identical to a build where
    /// the option was never set (the field's default).
    #[test]
    fn plist_disabled_is_byte_identical() {
        let default_pair = build_dataset_bytes(dataset_options());
        let off_pair = build_dataset_bytes(plist_options(0));
        assert!(
            default_pair == off_pair,
            "plist_min_docs = 0 must not change a single output byte \
             ({}+{:?} vs {}+{:?} bytes)",
            default_pair.0.len(),
            default_pair.1.as_ref().map(Vec::len),
            off_pair.0.len(),
            off_pair.1.as_ref().map(Vec::len),
        );
        let meta =
            puffin::reader::parse_puffin_footer_from_bytes(off_pair.1.as_deref().expect("sidecar"))
                .unwrap();
        assert!(!meta.properties.contains_key("plist_min_docs"));
        assert!(
            meta.blobs
                .iter()
                .all(|blob| blob.properties["blob_tag"] != "plist")
        );
    }
}

/// M10/#51b: the parallel k-way term merge must be byte-for-byte
/// indistinguishable from the sequential one at the term-stream and bloom
/// level — under every corpus shape that stressed the 2026-07-29 v1 range
/// sampler corruption. `merge_kway_threads = 1` is the sequential path
/// (exactly one range through the same code); `8` the parallel one.
mod m10_parallel_kway {
    use std::hash::{DefaultHasher, Hash, Hasher};

    use super::*;
    use crate::{
        DocIdMap,
        merge::{partition_bounds, translate_bound},
        query::{KEY_FIELD_ID, split_key, write_composite},
    };

    fn synth_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("aa", DataType::Utf8, true),
            Field::new("bb", DataType::Utf8, true),
            Field::new("cc", DataType::Utf8, true),
        ]))
    }

    fn synth_opts() -> VixWriterOptions {
        VixWriterOptions {
            bloom_field_names: vec!["bb".to_string()],
            bloom_composite: true,
            ..Default::default()
        }
    }

    /// One synthetic input of `rows` rows: per-row values from the
    /// generators (`None` = null). Returns the reader plus the raw batch
    /// and `_source` for the merged docs push.
    fn synth_input(
        opts: &VixWriterOptions,
        ts_start: i64,
        rows: usize,
        aa: impl Fn(usize) -> Option<String>,
        bb: impl Fn(usize) -> Option<String>,
        cc: impl Fn(usize) -> Option<String>,
    ) -> (VixReader, RecordBatch, StringArray) {
        let schema = synth_schema();
        // the writer refuses zero/negative timestamps: keep everything >= 1M
        let ts: Vec<i64> = (0..rows).map(|r| 1_000_000 + ts_start + r as i64).collect();
        let col = |make: &dyn Fn(usize) -> Option<String>| {
            StringArray::from((0..rows).map(make).collect::<Vec<Option<String>>>())
        };
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ts)) as ArrayRef,
                Arc::new(col(&aa)),
                Arc::new(col(&bb)),
                Arc::new(col(&cc)),
            ],
        )
        .unwrap();
        let source = synthesize_source_for_test(&batch);
        let mut writer = VixWriter::new(&schema, opts.clone(), false);
        writer
            .push_batch_with_source(&batch, &source, None)
            .unwrap();
        (finish_open(writer), batch, source)
    }

    /// Merge the inputs (concatenation-order offset maps) at the given
    /// k-way range parallelism and return the finished `(data, sidecar)`.
    fn merge_with_kway(
        inputs: &[(VixReader, RecordBatch, StringArray)],
        merged_opts: &VixWriterOptions,
        kway: usize,
    ) -> (Vec<u8>, Option<Vec<u8>>) {
        let schema = synth_schema();
        let refs: Vec<&VixReader> = inputs.iter().map(|(reader, ..)| reader).collect();
        let mut offset = 0u32;
        let doc_maps: Vec<DocIdMap> = inputs
            .iter()
            .map(|(_, batch, _)| {
                let map = DocIdMap::Offset(offset);
                offset += batch.num_rows() as u32;
                map
            })
            .collect();
        let mut merged = VixWriter::new(
            &schema,
            VixWriterOptions {
                merge_kway_threads: kway,
                ..merged_opts.clone()
            },
            false,
        );
        merged.merge_input_indexes(&refs, &doc_maps, 8).unwrap();
        for (_, batch, source) in inputs {
            merged
                .push_docs_rows_unindexed(
                    &timestamps_of(batch),
                    &cs_columns_of(batch, &["aa", "bb", "cc"]),
                    source,
                    None,
                )
                .unwrap();
        }
        merged.finish().unwrap()
    }

    /// The M7 fast-vs-rebuild digest: term count + an order-sensitive hash
    /// of every `(key, doc_count, ids)` row.
    fn term_digest(reader: &VixReader) -> (u64, u64) {
        let mut hasher = DefaultHasher::new();
        let mut count = 0u64;
        reader
            .for_each_term(&mut |key, doc_count, ids| {
                key.hash(&mut hasher);
                doc_count.hash(&mut hasher);
                ids.hash(&mut hasher);
                count += 1;
                Ok(())
            })
            .unwrap();
        (count, hasher.finish())
    }

    /// The serialized bloom blob of a finished sidecar (per-field sections
    /// plus the #48 composite section).
    fn bloom_blob(sidecar: &[u8]) -> Vec<u8> {
        let meta = puffin::reader::parse_puffin_footer_from_bytes(sidecar).unwrap();
        let blob = meta
            .blobs
            .iter()
            .find(|blob| blob.properties["blob_tag"] == "bloom")
            .expect("bloom blob");
        let range = blob.get_offset(None);
        sidecar[range.start as usize..range.end as usize].to_vec()
    }

    /// Sequential (kway=1) and parallel (kway=8) merges of the same inputs
    /// must agree on the full term stream, the bloom bytes and the partial
    /// fields.
    fn assert_kway_equivalent(
        inputs: &[(VixReader, RecordBatch, StringArray)],
        merged_opts: &VixWriterOptions,
        label: &str,
    ) -> VixReader {
        let (seq_data, seq_index) = merge_with_kway(inputs, merged_opts, 1);
        let (par_data, par_index) = merge_with_kway(inputs, merged_opts, 8);
        assert_eq!(
            bloom_blob(seq_index.as_deref().expect("sidecar")),
            bloom_blob(par_index.as_deref().expect("sidecar")),
            "{label}: bloom blobs diverge"
        );
        let seq = open_built(seq_data, seq_index);
        let par = open_built(par_data, par_index);
        assert_eq!(
            term_digest(&seq),
            term_digest(&par),
            "{label}: term digests diverge"
        );
        assert_eq!(
            seq.partial_fields(),
            par.partial_fields(),
            "{label}: partial fields diverge"
        );
        par
    }

    /// Four disjoint-token inputs (the compaction common case), with a
    /// direct build over the same rows as the independent oracle.
    #[test]
    fn disjoint_corpus_parallel_equals_sequential() {
        let opts = synth_opts();
        let inputs: Vec<_> = (0..4)
            .map(|i| {
                synth_input(
                    &opts,
                    i as i64 * 10_000,
                    900,
                    move |r| Some(format!("a{i}_{r:05}")),
                    move |r| Some(format!("b{i}_{:05}", r / 3)),
                    move |r| (r % 7 != 0).then(|| format!("c{i}_{:04}", r / 2)),
                )
            })
            .collect();
        let par = assert_kway_equivalent(&inputs, &opts, "disjoint");

        // independent oracle: a direct build over the concatenated rows
        let schema = synth_schema();
        let batches: Vec<RecordBatch> = inputs.iter().map(|(_, b, _)| b.clone()).collect();
        let concat = arrow::compute::concat_batches(&schema, &batches).unwrap();
        let source = synthesize_source_for_test(&concat);
        let mut direct = VixWriter::new(&schema, opts.clone(), false);
        direct
            .push_batch_with_source(&concat, &source, None)
            .unwrap();
        let direct = finish_open(direct);
        assert_eq!(
            term_digest(&par),
            term_digest(&direct),
            "parallel merge diverges from the direct build"
        );
    }

    /// Four inputs sharing one token universe: every output key unions
    /// postings across all inputs, so range boundaries hit multi-way keys.
    #[test]
    fn overlapping_corpus_parallel_equals_sequential() {
        let opts = synth_opts();
        let inputs: Vec<_> = (0..4)
            .map(|i| {
                synth_input(
                    &opts,
                    i as i64 * 10_000,
                    900,
                    |r| Some(format!("a_{:05}", r)),
                    |r| Some(format!("b_{:05}", r / 3)),
                    |r| (r % 5 != 0).then(|| format!("c_{:04}", r / 2)),
                )
            })
            .collect();
        assert_kway_equivalent(&inputs, &opts, "overlapping");
    }

    /// M7-style demoted-mixed corpus: term-indexed inputs merged into a
    /// bloom-only output field — the demoted fid's keys must route to their
    /// range's worker for bloom absorption (never dict/terms emission), in
    /// both modes identically.
    #[test]
    fn demoted_mixed_corpus_parallel_equals_sequential() {
        let opts = synth_opts();
        let inputs: Vec<_> = (0..3)
            .map(|i| {
                synth_input(
                    &opts,
                    i as i64 * 10_000,
                    900,
                    move |r| Some(format!("a{i}_{r:05}")),
                    |r| Some(format!("b_{:05}", r / 2)),
                    move |r| Some(format!("c{i}_{:04}", r / 3)),
                )
            })
            .collect();
        let demoting = VixWriterOptions {
            bloom_only_field_names: vec!["aa".to_string()],
            ..opts
        };
        let par = assert_kway_equivalent(&inputs, &demoting, "demoted-mixed");
        assert!(
            !par.has_term_capability("aa"),
            "aa must be demoted in the merged output"
        );
    }

    /// A bound landing EXACTLY on a fid's first key: `aa` holds 610 keys,
    /// `bb` 590, so the total*4/8 quantile falls inside `aa`'s weight and
    /// the bound snaps to `bb`'s first key (blocks cut at field
    /// boundaries, so `bb`'s first key is a candidate).
    #[test]
    fn boundary_on_fid_first_key_parallel_equals_sequential() {
        let opts = synth_opts();
        let inputs = vec![synth_input(
            &opts,
            0,
            610,
            |r| Some(format!("a_{r:05}")),
            |r| (r < 590).then(|| format!("b_{r:05}")),
            |_| None,
        )];
        // prove the adversarial placement: with 8 ranges one bound IS the
        // remapped first key of `bb` (output ids: aa=0, bb=1)
        let out_ids: std::collections::HashMap<String, u16> =
            [("aa".to_string(), 0u16), ("bb".to_string(), 1u16)]
                .into_iter()
                .collect();
        let bounds = partition_bounds(&[&inputs[0].0], &out_ids, 8).unwrap();
        let mut bb_first = Vec::new();
        write_composite(&mut bb_first, b"b_00000", 1);
        assert!(
            bounds.contains(&bb_first),
            "expected a bound exactly on bb's first key; bounds fids: {:?}",
            bounds
                .iter()
                .map(|b| split_key(b).map(|(_, fid)| fid))
                .collect::<Vec<_>>()
        );
        assert_kway_equivalent(&inputs, &opts, "fid-boundary bound");
    }

    /// One fid holding >90% of the keys: the block-key-count weighting must
    /// pull the split points into that fid, and the digests must hold.
    #[test]
    fn skewed_fid_parallel_equals_sequential() {
        let opts = synth_opts();
        // Keep the dominant field spread across enough production-sized
        // dictionary blocks to offer all seven parallel split points. The
        // field still owns >90% of distinct keys; only their byte width is
        // derived from the block target so this remains a partition test
        // when that target is tuned.
        const AA_VALUE_SUFFIX_BYTES: usize = crate::dict_blocks::BLOCK_TARGET_BYTES / 256;
        let inputs = vec![synth_input(
            &opts,
            0,
            3000,
            |r| Some(format!("a_{r:06}{}", "x".repeat(AA_VALUE_SUFFIX_BYTES))),
            |r| Some(format!("b_{:02}", r % 40)),
            |r| Some(format!("c_{:02}", r % 40)),
        )];
        let out_ids: std::collections::HashMap<String, u16> = [
            ("aa".to_string(), 0u16),
            ("bb".to_string(), 1u16),
            ("cc".to_string(), 2u16),
        ]
        .into_iter()
        .collect();
        let bounds = partition_bounds(&[&inputs[0].0], &out_ids, 8).unwrap();
        assert!(!bounds.is_empty(), "a 3000-key input must yield bounds");
        let inside_aa = bounds
            .iter()
            .filter(|b| split_key(b).is_some_and(|(_, fid)| fid == 0))
            .count();
        assert!(
            inside_aa * 10 >= bounds.len() * 8,
            "weighting must pull >=80% of bounds into the dominant fid \
             ({inside_aa}/{} landed in aa)",
            bounds.len()
        );
        assert_kway_equivalent(&inputs, &opts, "skewed fid");
    }

    /// More ranges than distinct keys: empty ranges must be harmless (the
    /// assembly skips empty parts; nothing is emitted twice or dropped).
    #[test]
    fn more_ranges_than_keys_parallel_equals_sequential() {
        let opts = synth_opts();
        let inputs = vec![synth_input(
            &opts,
            0,
            2,
            |r| Some(format!("a_{r}")),
            |_| None,
            |_| None,
        )];
        assert_kway_equivalent(&inputs, &opts, "more ranges than keys");
    }

    /// Single-input merges must partition and reassemble identically (the
    /// offset-0 single-contributor path reuses encoded cells verbatim).
    #[test]
    fn single_input_parallel_equals_sequential() {
        let opts = synth_opts();
        let inputs = vec![synth_input(
            &opts,
            0,
            1500,
            |r| Some(format!("a_{r:05}")),
            |r| Some(format!("b_{:05}", r / 4)),
            |r| (r % 3 == 0).then(|| format!("c_{:04}", r)),
        )];
        assert_kway_equivalent(&inputs, &opts, "single input");
    }

    /// The sampler's own contract: bounds are real remapped input keys,
    /// strictly ascending, deduplicated; one range yields none; a
    /// non-order-preserving remap disables partitioning entirely.
    #[test]
    fn sampler_bounds_are_real_sorted_and_gated() {
        let opts = synth_opts();
        let (r1, ..) = synth_input(
            &opts,
            0,
            900,
            |r| Some(format!("a1_{r:05}")),
            |r| Some(format!("b1_{:05}", r / 3)),
            |_| None,
        );
        let (r2, ..) = synth_input(
            &opts,
            10_000,
            900,
            |r| Some(format!("a2_{r:05}")),
            |r| Some(format!("b2_{:05}", r / 3)),
            |_| None,
        );
        let out_ids: std::collections::HashMap<String, u16> = [
            ("aa".to_string(), 0u16),
            ("bb".to_string(), 1u16),
            ("cc".to_string(), 2u16),
        ]
        .into_iter()
        .collect();
        let inputs = [&r1, &r2];

        // the full candidate universe, recomputed independently: every
        // dict-block first key of every input, remapped to output ids
        let mut candidates: std::collections::BTreeSet<Vec<u8>> = std::collections::BTreeSet::new();
        for reader in inputs {
            let entries = reader.field_entries();
            let index = reader.dict_index().unwrap();
            index
                .walk_first_keys(|_, key| {
                    let (token, fid) = split_key(key).unwrap();
                    if fid == KEY_FIELD_ID {
                        candidates.insert(key.to_vec());
                    } else if let Some(&out) =
                        entries.get(fid as usize).and_then(|e| out_ids.get(&e.name))
                    {
                        let mut k = Vec::new();
                        write_composite(&mut k, token, out);
                        candidates.insert(k);
                    }
                    true
                })
                .unwrap();
        }

        let bounds = partition_bounds(&inputs, &out_ids, 8).unwrap();
        assert!(!bounds.is_empty());
        for bound in &bounds {
            assert!(
                candidates.contains(bound),
                "bound {bound:?} is not a real remapped input key"
            );
        }
        assert!(
            bounds.windows(2).all(|pair| pair[0] < pair[1]),
            "bounds must be strictly ascending (sorted + deduplicated)"
        );

        // one range = no bounds
        assert!(partition_bounds(&inputs, &out_ids, 1).unwrap().is_empty());

        // a remap that reverses field order is not order-preserving: the
        // sampler must refuse to partition (single-range fallback)
        let reversed: std::collections::HashMap<String, u16> = [
            ("aa".to_string(), 2u16),
            ("bb".to_string(), 1u16),
            ("cc".to_string(), 0u16),
        ]
        .into_iter()
        .collect();
        assert!(
            partition_bounds(&inputs, &reversed, 8).unwrap().is_empty(),
            "non-monotone remaps must disable partitioning"
        );
    }

    /// `translate_bound`'s exactness contract, brute-forced: for every
    /// emittable input key `k` and every bound `B`,
    /// `k >= T(B) ⟺ remap(k) >= B`; translations are monotone in `B`
    /// (consecutive ranges tile each input's raw key space).
    #[test]
    fn translate_bound_is_exact_and_monotone() {
        // input fids 0..=3: 0 -> out 2, 1 -> dropped, 2 -> out 3, 3 -> out 7
        let field_map: Vec<Option<u16>> = vec![Some(2), None, Some(3), Some(7)];
        let tokens: [&[u8]; 4] = [b"", b"a", b"ab", b"z"];
        let mut emittable: Vec<(Vec<u8>, Vec<u8>)> = Vec::new(); // (raw, remapped)
        for (fid, mapped) in field_map.iter().enumerate() {
            let Some(out) = mapped else { continue };
            for token in tokens {
                let mut raw = Vec::new();
                write_composite(&mut raw, token, fid as u16);
                let mut remapped = Vec::new();
                write_composite(&mut remapped, token, *out);
                emittable.push((raw, remapped));
            }
        }
        // key terms are identity-mapped
        for token in tokens {
            let mut key = Vec::new();
            write_composite(&mut key, token, KEY_FIELD_ID);
            emittable.push((key.clone(), key));
        }

        // bounds: every remapped key, plus bounds at unmapped output fids
        let mut bounds: Vec<Vec<u8>> = emittable.iter().map(|(_, m)| m.clone()).collect();
        for fid in [0u16, 1, 4, 5, 8, 100] {
            let mut b = Vec::new();
            write_composite(&mut b, b"m", fid);
            bounds.push(b);
        }
        bounds.sort();
        bounds.dedup();

        let mut prev_t: Option<Vec<u8>> = None;
        for bound in &bounds {
            let t = translate_bound(bound, &field_map).unwrap();
            for (raw, remapped) in &emittable {
                assert_eq!(
                    raw.as_slice() >= t.as_slice(),
                    remapped.as_slice() >= bound.as_slice(),
                    "exactness violated: bound {bound:?}, T {t:?}, key {raw:?} -> {remapped:?}"
                );
            }
            if let Some(prev) = &prev_t {
                assert!(
                    prev.as_slice() <= t.as_slice(),
                    "translation must be monotone: {prev:?} then {t:?}"
                );
            }
            prev_t = Some(t);
        }

        // spot-check the three translation shapes
        let mk = |token: &[u8], fid: u16| {
            let mut k = Vec::new();
            write_composite(&mut k, token, fid);
            k
        };
        // equal out fid: byte-exact within the field
        assert_eq!(
            translate_bound(&mk(b"tok", 3), &field_map).unwrap(),
            mk(b"tok", 2)
        );
        // out fid between mapped ids (4 sits between out 3 and out 7): the
        // next mapped INPUT field's 2-byte prefix — input fid 3 carries out 7
        assert_eq!(
            translate_bound(&mk(b"tok", 4), &field_map).unwrap(),
            3u16.to_be_bytes().to_vec()
        );
        // beyond every mapped id: the key-term region prefix
        assert_eq!(
            translate_bound(&mk(b"tok", 8), &field_map).unwrap(),
            KEY_FIELD_ID.to_be_bytes().to_vec()
        );
        // a key-term bound translates to itself
        assert_eq!(
            translate_bound(&mk(b"path", KEY_FIELD_ID), &field_map).unwrap(),
            mk(b"path", KEY_FIELD_ID)
        );
    }
}

/// Compaction index merge ([`VixWriter::merge_input_indexes`] +
/// [`crate::merge`]): the merged dictionary must be indistinguishable from
/// one built directly over the merged rows.
mod index_merge {
    use arrow::compute::interleave;

    use super::*;
    use crate::{DocIdMap, query::KEY_FIELD_ID};

    /// Interleave same-schema batches into one, following `order`
    /// (`(input, row)` per output row).
    fn interleave_batches(
        schema: &SchemaRef,
        batches: &[RecordBatch],
        order: &[(usize, usize)],
    ) -> RecordBatch {
        let columns: Vec<ArrayRef> = (0..schema.fields().len())
            .map(|col| {
                let arrays: Vec<&dyn Array> =
                    batches.iter().map(|b| b.column(col).as_ref()).collect();
                interleave(&arrays, order).unwrap()
            })
            .collect();
        RecordBatch::try_new(Arc::clone(schema), columns).unwrap()
    }

    fn interleave_strings(arrays: &[&StringArray], order: &[(usize, usize)]) -> StringArray {
        let refs: Vec<&dyn Array> = arrays.iter().map(|a| *a as &dyn Array).collect();
        as_string_array(&interleave(&refs, order).unwrap())
    }

    /// `old row -> merged doc id` maps implied by an output order.
    fn maps_from_order(sizes: &[usize], order: &[(usize, usize)]) -> Vec<Vec<u32>> {
        let mut maps: Vec<Vec<u32>> = sizes.iter().map(|&n| vec![0u32; n]).collect();
        for (pos, &(input, row)) in order.iter().enumerate() {
            maps[input][row] = pos as u32;
        }
        maps
    }

    /// Every term via the public enumeration API.
    fn all_terms(reader: &VixReader) -> Vec<(Vec<u8>, u64, Vec<u32>)> {
        let mut out = Vec::new();
        reader
            .for_each_term(&mut |key, doc_count, ids| {
                out.push((key.to_vec(), doc_count, ids.to_vec()));
                Ok(())
            })
            .unwrap();
        out
    }

    /// The three merge inputs of the equivalence tests: the shared dataset
    /// schema with distinct timestamps, overlapping fts tokens ("error"
    /// spans inputs), an empty-string value, a NUL-byte value and nulls.
    fn equivalence_inputs() -> (
        SchemaRef,
        VixWriterOptions,
        Vec<RecordBatch>,
        Vec<StringArray>,
    ) {
        let schema = docs_dataset_schema();
        let opts = dataset_options();
        let batches = vec![
            docs_dataset_batch(
                &schema,
                vec![900, 800, 700],
                vec![Some("info"), Some("a\x00b"), None],
                vec![Some("Error connecting to db"), Some(""), Some("all fine")],
                // one empty-string svc: the empty raw term (3-byte composite
                // key) must survive the index merge like any other value
                vec!["api", "", "web"],
                vec![Some(1), None, Some(3)],
            ),
            docs_dataset_batch(
                &schema,
                vec![600, 500],
                vec![Some("error"), Some("info")],
                vec![Some("timeout waiting"), Some("error again")],
                vec!["db", "api"],
                vec![Some(4), Some(5)],
            ),
            docs_dataset_batch(
                &schema,
                vec![400, 300, 200],
                vec![Some("warn"), None, Some("info")],
                vec![None, Some("db error timeout"), Some("ok")],
                vec!["web", "db", "api"],
                vec![None, Some(7), Some(8)],
            ),
        ];
        let sources: Vec<StringArray> = batches.iter().map(synthesize_source_for_test).collect();
        (schema, opts, batches, sources)
    }

    fn build_input(
        schema: &SchemaRef,
        opts: &VixWriterOptions,
        batch: &RecordBatch,
        source: &StringArray,
    ) -> VixReader {
        let mut writer = VixWriter::new(schema, opts.clone(), false);
        writer.push_batch_with_source(batch, source, None).unwrap();
        finish_open(writer)
    }

    /// Merge-path plist emission: plist-less inputs merged by a writer with
    /// `postings_plist_min_docs` set produce pointer cells whose resolved
    /// postings match the plist-less merge exactly — dense re-check
    /// included (a merged-dense term stays the empty cell, never a
    /// pointer).
    #[test]
    fn merged_index_emits_plist_pointer_cells() {
        let (schema, opts, batches, sources) = equivalence_inputs();
        let readers: Vec<VixReader> = batches
            .iter()
            .zip(&sources)
            .map(|(batch, source)| build_input(&schema, &opts, batch, source))
            .collect();
        let refs: Vec<&VixReader> = readers.iter().collect();
        let sizes: Vec<usize> = batches.iter().map(RecordBatch::num_rows).collect();
        let doc_maps = vec![
            DocIdMap::Offset(0),
            DocIdMap::Offset(sizes[0] as u32),
            DocIdMap::Offset((sizes[0] + sizes[1]) as u32),
        ];
        let concat_order: Vec<(usize, usize)> = (0..3)
            .flat_map(|input| (0..sizes[input]).map(move |row| (input, row)))
            .collect();
        let merged_batch = interleave_batches(&schema, &batches, &concat_order);
        let merged_source = interleave_strings(&sources.iter().collect::<Vec<_>>(), &concat_order);

        let build_merged = |plist_min_docs: u32| -> (Vec<u8>, Option<Vec<u8>>) {
            let mut merged = VixWriter::new(
                &schema,
                VixWriterOptions {
                    postings_plist_min_docs: plist_min_docs,
                    ..opts.clone()
                },
                false,
            );
            merged.merge_input_indexes(&refs, &doc_maps, 1).unwrap();
            merged
                .push_docs_rows_unindexed(
                    &timestamps_of(&merged_batch),
                    &cs_columns_of(&merged_batch, &["svc", "code"]),
                    &merged_source,
                    None,
                )
                .unwrap();
            merged.finish().unwrap()
        };

        let baseline = {
            let (data, index) = build_merged(0);
            open_built(data, index)
        };
        let (plist_bytes, plist_index) = build_merged(2);
        let meta = puffin::reader::parse_puffin_footer_from_bytes(
            plist_index.as_deref().expect("sidecar"),
        )
        .unwrap();
        assert_eq!(meta.properties["plist_min_docs"], "2");
        assert!(
            meta.blobs
                .iter()
                .any(|blob| blob.properties["blob_tag"] == "plist"),
            "the merged sidecar must carry the plist blob"
        );
        let plist_reader = open_built(plist_bytes, plist_index);

        // the whole resolved term stream matches the plist-less merge
        assert_eq!(all_terms(&plist_reader), all_terms(&baseline));
        // a spanning term became a pointer cell (`level` key term: 6 of 8
        // rows), the merged-dense one stayed empty (`svc` key term: 8 of 8)
        assert_eq!(
            plist_reader
                .debug_postings_len(b"level", KEY_FIELD_ID)
                .unwrap(),
            Some(12)
        );
        assert_eq!(
            plist_reader
                .debug_postings_len(b"svc", KEY_FIELD_ID)
                .unwrap(),
            Some(0)
        );
    }

    /// Stage 3: plist-capable INPUTS are resolved by the dictionary merge —
    /// pointer cells decode through the input's plist blob (record bytes
    /// reused verbatim when the single-contributor representation matches)
    /// — so merging plist inputs yields EXACTLY the term stream of merging
    /// the same rows from plist-less inputs, for both a plist-less output
    /// (pointer -> inline) and a thresholded output (pointer -> record).
    /// One mixed set also runs: plain + capable inputs in one merge.
    #[test]
    fn merged_index_resolves_plist_inputs() {
        let (schema, opts, batches, sources) = equivalence_inputs();
        let plist_opts = VixWriterOptions {
            postings_plist_min_docs: 2,
            ..opts.clone()
        };
        let plain: Vec<VixReader> = batches
            .iter()
            .zip(&sources)
            .map(|(batch, source)| build_input(&schema, &opts, batch, source))
            .collect();
        let capable: Vec<VixReader> = batches
            .iter()
            .zip(&sources)
            .map(|(batch, source)| build_input(&schema, &plist_opts, batch, source))
            .collect();
        let mixed: Vec<&VixReader> = vec![&plain[0], &capable[1], &capable[2]];
        let sizes: Vec<usize> = batches.iter().map(RecordBatch::num_rows).collect();
        let doc_maps = vec![
            DocIdMap::Offset(0),
            DocIdMap::Offset(sizes[0] as u32),
            DocIdMap::Offset((sizes[0] + sizes[1]) as u32),
        ];
        let concat_order: Vec<(usize, usize)> = (0..3)
            .flat_map(|input| (0..sizes[input]).map(move |row| (input, row)))
            .collect();
        let merged_batch = interleave_batches(&schema, &batches, &concat_order);
        let merged_source = interleave_strings(&sources.iter().collect::<Vec<_>>(), &concat_order);

        // the pre-flight accepts plist-capable inputs now
        let writer = VixWriter::new(&schema, opts.clone(), false);
        assert!(
            writer
                .check_merge_inputs(&capable.iter().collect::<Vec<_>>())
                .is_ok(),
            "stage 3 resolves input pointer cells; the pre-flight must not reject them"
        );

        let merge = |inputs: &[&VixReader], out_threshold: u32| -> (Vec<u8>, Option<Vec<u8>>) {
            let mut merged = VixWriter::new(
                &schema,
                VixWriterOptions {
                    postings_plist_min_docs: out_threshold,
                    ..opts.clone()
                },
                false,
            );
            merged.merge_input_indexes(inputs, &doc_maps, 1).unwrap();
            merged
                .push_docs_rows_unindexed(
                    &timestamps_of(&merged_batch),
                    &cs_columns_of(&merged_batch, &["svc", "code"]),
                    &merged_source,
                    None,
                )
                .unwrap();
            merged.finish().unwrap()
        };

        for out_threshold in [0u32, 2] {
            let open_merge = |pair: (Vec<u8>, Option<Vec<u8>>)| open_built(pair.0, pair.1);
            let baseline = open_merge(merge(&plain.iter().collect::<Vec<_>>(), out_threshold));
            let from_capable =
                open_merge(merge(&capable.iter().collect::<Vec<_>>(), out_threshold));
            let from_mixed = open_merge(merge(&mixed, out_threshold));
            assert_eq!(
                all_terms(&from_capable),
                all_terms(&baseline),
                "plist inputs, output threshold {out_threshold}"
            );
            assert_eq!(
                all_terms(&from_mixed),
                all_terms(&baseline),
                "mixed inputs, output threshold {out_threshold}"
            );
        }
    }

    /// The core equivalence: merged-index files answer identically to a
    /// direct build over the merged rows — for both map shapes (contiguous
    /// offsets and an interleaving permutation).
    #[test]
    fn merged_index_matches_direct_build() {
        let (schema, opts, batches, sources) = equivalence_inputs();
        let readers: Vec<VixReader> = batches
            .iter()
            .zip(&sources)
            .map(|(batch, source)| build_input(&schema, &opts, batch, source))
            .collect();
        let refs: Vec<&VixReader> = readers.iter().collect();
        let sizes: Vec<usize> = batches.iter().map(RecordBatch::num_rows).collect();

        // (a) concatenation order (disjoint runs -> offset maps);
        // (b) a genuine interleave (table maps)
        let concat_order: Vec<(usize, usize)> = (0..3)
            .flat_map(|input| (0..sizes[input]).map(move |row| (input, row)))
            .collect();
        let interleaved_order: Vec<(usize, usize)> = vec![
            (0, 0),
            (1, 0),
            (2, 0),
            (0, 1),
            (1, 1),
            (2, 1),
            (0, 2),
            (2, 2),
        ];
        type MergeCase = (&'static str, Vec<(usize, usize)>, Vec<DocIdMap>);
        let cases: Vec<MergeCase> = vec![
            (
                "offsets",
                concat_order.clone(),
                vec![
                    DocIdMap::Offset(0),
                    DocIdMap::Offset(sizes[0] as u32),
                    DocIdMap::Offset((sizes[0] + sizes[1]) as u32),
                ],
            ),
            (
                "tables",
                interleaved_order.clone(),
                maps_from_order(&sizes, &interleaved_order)
                    .into_iter()
                    .map(DocIdMap::Table)
                    .collect(),
            ),
        ];

        // threads = 1 exercises the sequential merge, threads = 3 the
        // range-partitioned parallel one — both must be equivalent
        for ((name, order, doc_maps), threads) in cases
            .iter()
            .flat_map(|case| [1usize, 3].into_iter().map(move |threads| (case, threads)))
        {
            let case = format!("{name} x{threads}");
            let merged_batch = interleave_batches(&schema, &batches, order);
            let merged_source = interleave_strings(&sources.iter().collect::<Vec<_>>(), order);

            let mut merged = VixWriter::new(&schema, opts.clone(), false);
            merged.check_merge_inputs(&refs).unwrap_or_else(|reason| {
                panic!("{case}: inputs unexpectedly incompatible: {reason}")
            });
            merged
                .merge_input_indexes(&refs, doc_maps, threads)
                .unwrap();
            merged
                .push_docs_rows_unindexed(
                    &timestamps_of(&merged_batch),
                    &cs_columns_of(&merged_batch, &["svc", "code"]),
                    &merged_source,
                    None,
                )
                .unwrap();
            let (merged_bytes, merged_bytes_index, merged_stats) =
                merged.finish_with_stats().unwrap();
            let merged_reader = open_built(merged_bytes, merged_bytes_index);

            let mut reference = VixWriter::new(&schema, opts.clone(), false);
            reference
                .push_batch_with_source(&merged_batch, &merged_source, None)
                .unwrap();
            let (reference_bytes, reference_bytes_index, reference_stats) =
                reference.finish_with_stats().unwrap();
            let reference_reader = open_built(reference_bytes, reference_bytes_index);

            assert_eq!(merged_stats.row_count, reference_stats.row_count, "{case}");
            assert_eq!(
                merged_stats.term_count, reference_stats.term_count,
                "{case}"
            );
            assert_eq!(
                merged_reader.debug_all_terms().unwrap(),
                reference_reader.debug_all_terms().unwrap(),
                "{case}: term sets diverge"
            );
            // the public enumeration agrees with the debug dump
            let enumerated = all_terms(&merged_reader);
            let debug: Vec<(Vec<u8>, u64, Vec<u32>)> = merged_reader
                .debug_all_terms()
                .unwrap()
                .into_iter()
                .map(|(key, doc_count, ids)| (key, doc_count, ids.into_iter().collect()))
                .collect();
            assert_eq!(enumerated, debug, "{case}: for_each_term disagrees");
            assert_eq!(
                merged_reader.partial_fields(),
                reference_reader.partial_fields(),
                "{case}"
            );
            // docs store round-trips identically
            let rows: Vec<u64> = (0..merged_reader.row_count()).collect();
            let merged_sources = merged_reader.read_source(&rows).unwrap();
            let reference_sources = reference_reader.read_source(&rows).unwrap();
            assert_eq!(merged_sources, reference_sources, "{case}");
            for name in ["_timestamp", "svc", "code"] {
                assert_eq!(
                    format!("{:?}", merged_reader.read_docs_column(name).unwrap()),
                    format!("{:?}", reference_reader.read_docs_column(name).unwrap()),
                    "{case}: column {name}"
                );
            }
            // a query battery answers identically
            let queries = [
                exact("level", "info"),
                exact("level", "a\x00b"),
                exact("svc", "api"),
                any_token("error"),
                any_token("timeout"),
                prefix(None, "err"),
                prefix(Some("svc"), "a"),
                contains(None, "im", false),
                regex(Some("svc"), "a.*"),
                VixQuery::KeyExists {
                    path: "log".to_string(),
                },
                VixQuery::KeyExists {
                    path: "code".to_string(),
                },
                VixQuery::And(vec![exact("svc", "api"), any_token("error")]),
            ];
            for query in &queries {
                assert_eq!(
                    eval_set(&merged_reader, query),
                    eval_set(&reference_reader, query),
                    "{case}: {query:?}"
                );
                assert_eq!(
                    merged_reader.count(query).unwrap(),
                    reference_reader.count(query).unwrap(),
                    "{case}: count {query:?}"
                );
            }
            assert_eq!(
                merged_reader.keys_with_prefix("").unwrap(),
                reference_reader.keys_with_prefix("").unwrap(),
                "{case}"
            );
            assert_eq!(
                merged_reader.field_value_counts("svc").unwrap(),
                reference_reader.field_value_counts("svc").unwrap(),
                "{case}"
            );
        }
    }

    /// Field ids are remapped into the merged field table; a field the
    /// merged docs store under a type with NO term derivation (Timestamp
    /// here — numeric types keep their terms) loses its value terms and
    /// degrades to `partial_fields` — byte-identical in meaning to the
    /// source-driven rebuild of the same rows.
    #[test]
    fn merged_index_remaps_and_drops_typed_conflicts() {
        // input 1: alpha + code, both strings, both raw-indexed
        let schema1 = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("alpha", DataType::Utf8, true),
            Field::new("code", DataType::Utf8, true),
        ]));
        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema1),
            vec![
                Arc::new(Int64Array::from(vec![100, 90])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("x"), Some("y")])),
                Arc::new(StringArray::from(vec![Some("500"), Some("200")])),
            ],
        )
        .unwrap();
        let source1 = synthesize_source_for_test(&batch1);
        let mut writer1 = VixWriter::new(&schema1, VixWriterOptions::default(), false);
        writer1
            .push_batch_with_source(&batch1, &source1, None)
            .unwrap();
        let reader1 = finish_open(writer1);

        // input 2: beta only
        let schema2 = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("beta", DataType::Utf8, true),
        ]));
        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema2),
            vec![
                Arc::new(Int64Array::from(vec![80, 70])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("z"), None])),
            ],
        )
        .unwrap();
        let source2 = synthesize_source_for_test(&batch2);
        let mut writer2 = VixWriter::new(&schema2, VixWriterOptions::default(), false);
        writer2
            .push_batch_with_source(&batch2, &source2, None)
            .unwrap();
        let reader2 = finish_open(writer2);

        // merged plan: code becomes a Timestamp column-store field — a type
        // with NO term derivation (numeric types would keep the remapped
        // terms now), so its raw string terms have no output field id
        let merged_schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("alpha", DataType::Utf8, true),
            Field::new("beta", DataType::Utf8, true),
            Field::new(
                "code",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                true,
            ),
        ]));
        let opts = VixWriterOptions {
            ..Default::default()
        };
        let merged_ts = Int64Array::from(vec![100, 90, 80, 70]);
        let merged_code: ArrayRef =
            Arc::new(Int64Array::from(vec![Some(500), Some(200), None, None]));
        let merged_source = StringArray::from_iter_values(
            (0..2)
                .map(|row| source1.value(row).to_string())
                .chain((0..2).map(|row| source2.value(row).to_string())),
        );

        let refs = [&reader1, &reader2];
        let doc_maps = [DocIdMap::Offset(0), DocIdMap::Offset(2)];
        let mut merged = VixWriter::new(&merged_schema, opts.clone(), false);
        merged.check_merge_inputs(&refs).unwrap();
        merged.merge_input_indexes(&refs, &doc_maps, 2).unwrap();
        merged
            .push_docs_rows_unindexed(
                &merged_ts,
                &[("code".to_string(), Arc::clone(&merged_code))],
                &merged_source,
                None,
            )
            .unwrap();
        let merged_reader = finish_open(merged);

        // remapped lookups answer correctly
        assert_eq!(eval_set(&merged_reader, &exact("alpha", "x")), docs(&[0]));
        assert_eq!(eval_set(&merged_reader, &exact("beta", "z")), docs(&[2]));
        // the dropped field: partial, no term capability, key terms intact
        assert!(merged_reader.partial_fields().contains("code"));
        assert!(!merged_reader.has_term_capability("code"));
        assert!(merged_reader.eval(&exact("code", "500")).is_err());
        assert_eq!(key_exists_set(&merged_reader, "code"), docs(&[0, 1]));

        // ... and the whole term table equals the source-driven rebuild of
        // the same rows (what the compactor's fallback would produce)
        let mut rebuild = VixWriter::new(&merged_schema, opts, false);
        rebuild
            .push_docs_rows(
                &merged_ts,
                &[("code".to_string(), merged_code)],
                &merged_source,
                None,
            )
            .unwrap();
        let rebuild_reader = finish_open(rebuild);
        assert_eq!(
            merged_reader.debug_all_terms().unwrap(),
            rebuild_reader.debug_all_terms().unwrap()
        );
        assert_eq!(
            merged_reader.partial_fields(),
            rebuild_reader.partial_fields()
        );
    }

    /// Dense elision through a merge: re-checked against the merged row
    /// count — input-dense terms elide again when every merged row carries
    /// them, and expand through the doc-id map when they stop being dense.
    #[test]
    fn merged_dense_elision_recheck() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("svc", DataType::Utf8, false),
        ]));
        let build = |ts: Vec<i64>, svc: Vec<&str>| {
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(ts)) as ArrayRef,
                    Arc::new(StringArray::from(svc)),
                ],
            )
            .unwrap();
            let source = synthesize_source_for_test(&batch);
            let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
            writer
                .push_batch_with_source(&batch, &source, None)
                .unwrap();
            (batch, source, finish_open(writer))
        };
        let merge = |inputs: [&(RecordBatch, StringArray, VixReader); 2]| {
            let readers = [&inputs[0].2, &inputs[1].2];
            let rows0 = inputs[0].0.num_rows();
            let doc_maps = [DocIdMap::Offset(0), DocIdMap::Offset(rows0 as u32)];
            let mut merged = VixWriter::new(&schema, VixWriterOptions::default(), false);
            merged.check_merge_inputs(&readers).unwrap();
            merged.merge_input_indexes(&readers, &doc_maps, 2).unwrap();
            for (batch, source, _) in inputs {
                merged
                    .push_docs_rows_unindexed(&timestamps_of(batch), &[], source, None)
                    .unwrap();
            }
            finish_open(merged)
        };

        // both inputs dense in "api" -> merged dense -> elided again
        let a = build(vec![100, 90, 80], vec!["api", "api", "api"]);
        let b = build(vec![70, 60], vec!["api", "api"]);
        let merged = merge([&a, &b]);
        let svc_id = merged.field_id("svc").unwrap();
        assert_eq!(merged.debug_postings_len(b"api", svc_id).unwrap(), Some(0));
        assert_eq!(merged.count(&exact("svc", "api")).unwrap(), 5);
        assert_eq!(
            eval_set(&merged, &exact("svc", "api")),
            docs(&[0, 1, 2, 3, 4])
        );
        // the key term is dense too
        assert_eq!(
            merged.debug_postings_len(b"svc", KEY_FIELD_ID).unwrap(),
            Some(0)
        );

        // input-dense but merged-sparse -> expanded through the offset
        let c = build(vec![70, 60], vec!["db", "api"]);
        let merged = merge([&a, &c]);
        let svc_id = merged.field_id("svc").unwrap();
        assert_ne!(merged.debug_postings_len(b"api", svc_id).unwrap(), Some(0));
        assert_eq!(eval_set(&merged, &exact("svc", "api")), docs(&[0, 1, 2, 4]));
        assert_eq!(eval_set(&merged, &exact("svc", "db")), docs(&[3]));
    }

    /// The 2026-08-12 skip-without-degrade trade across a MERGE: the
    /// inputs' per-field oversize-skip allowances SUM into the merged
    /// file's `oversize_skips` property, so the merged dictionary serve
    /// stays eligible — a lost allowance would demote every merged file
    /// back to the scan fallback the trade exists to avoid.
    #[test]
    fn merge_sums_oversize_skip_allowances() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("f", DataType::Utf8, true),
        ]));
        let opts = VixWriterOptions {
            max_raw_term_len: 8,
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
        let long_a = "a".repeat(9);
        let long_b = "b".repeat(9);
        let f1 = build(vec![900, 800], vec![Some(long_a.as_str()), Some("ok")]);
        let f2 = build(vec![700], vec![Some(long_b.as_str())]);
        assert_eq!(f1.oversize_skips().get("f"), Some(&1));
        assert_eq!(f2.oversize_skips().get("f"), Some(&1));

        let mut merged = VixWriter::new(&schema, opts, false);
        merged
            .merge_input_indexes(&[&f1, &f2], &[DocIdMap::Offset(0), DocIdMap::Offset(2)], 1)
            .unwrap();
        merged
            .push_docs_rows_unindexed(
                &Int64Array::from(vec![900, 800, 700]),
                &[],
                &StringArray::from(vec![
                    format!("{{\"f\":{long_a:?}}}"),
                    r#"{"f":"ok"}"#.to_string(),
                    format!("{{\"f\":{long_b:?}}}"),
                ]),
                None,
            )
            .unwrap();
        let reader = finish_open(merged);
        assert_eq!(reader.oversize_skips().get("f"), Some(&2));
        assert!(reader.partial_fields().is_empty());
        // the summed allowance keeps the merged serve eligible: 1 indexed
        // value + 2 skipped == 3 key-term docs; counts omit both literals
        assert_eq!(
            reader.field_value_counts("f").unwrap(),
            Some(vec![(b"ok".to_vec(), 1)]),
        );
    }

    /// Every incompatibility [`VixWriter::check_merge_inputs`] must reject
    /// (the compactor falls back to the term rebuild on any of them).
    #[test]
    fn check_merge_inputs_rejections() {
        let schema = docs_dataset_schema();
        let opts = dataset_options();
        let (_, _, batches, sources) = equivalence_inputs();
        let good = build_input(&schema, &opts, &batches[0], &sources[0]);

        // baseline: compatible
        let writer = VixWriter::new(&schema, opts.clone(), false);
        writer.check_merge_inputs(&[&good]).unwrap();

        // foreign tokenizer property
        let (foreign_data, foreign_index) = {
            let mut w = VixWriter::new(&schema, opts.clone(), false);
            w.push_batch_with_source(&batches[0], &sources[0], None)
                .unwrap();
            w.finish().unwrap()
        };
        let foreign_index = repack_with_properties(foreign_index.expect("sidecar"), |properties| {
            properties
                .iter_mut()
                .find(|(key, _)| key == "tokenizer")
                .unwrap()
                .1 = "o2-v0".to_string();
        });
        let foreign = open_built(foreign_data, Some(foreign_index));
        let reason = writer.check_merge_inputs(&[&foreign]).unwrap_err();
        assert!(reason.contains("tokenizer"), "{reason}");

        // legacy "o2-v1" tokenizer property: its dictionary holds v1 tokens,
        // which the current writer cannot merge — forces the rebuild
        // (which re-tokenizes everything from `_source` with the canonical
        // tokenizer)
        let (legacy_data, legacy_index) = {
            let mut w = VixWriter::new(&schema, opts.clone(), false);
            w.push_batch_with_source(&batches[0], &sources[0], None)
                .unwrap();
            w.finish().unwrap()
        };
        let legacy_index = repack_with_properties(legacy_index.expect("sidecar"), |properties| {
            properties
                .iter_mut()
                .find(|(key, _)| key == "tokenizer")
                .unwrap()
                .1 = "o2-v1".to_string();
        });
        let legacy_v1 = open_built(legacy_data, Some(legacy_index));
        let reason = writer.check_merge_inputs(&[&legacy_v1]).unwrap_err();
        assert!(reason.contains("tokenizer"), "{reason}");
        assert!(reason.contains("o2-v2"), "{reason}");

        // capability conflict: input says fts, plan says term ...
        let term_plan = VixWriter::new(
            &schema,
            VixWriterOptions {
                fts_field_names: vec![],
                ..opts.clone()
            },
            false,
        );
        let reason = term_plan.check_merge_inputs(&[&good]).unwrap_err();
        assert!(reason.contains("\"log\""), "{reason}");
        // ... and the reverse: input says term, plan says fts
        let term_input = {
            let no_fts = VixWriterOptions {
                fts_field_names: vec![],
                ..opts.clone()
            };
            let mut w = VixWriter::new(&schema, no_fts, false);
            w.push_batch_with_source(&batches[0], &sources[0], None)
                .unwrap();
            finish_open(w)
        };
        let fts_plan = VixWriter::new(&schema, opts.clone(), false);
        let reason = fts_plan.check_merge_inputs(&[&term_input]).unwrap_err();
        assert!(reason.contains("\"log\""), "{reason}");

        // a partial field the input never value-indexed, which the plan
        // value-indexes: only a rebuild can recover the values
        let partial_schema = Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("msg", DataType::Utf8, true),
        ]);
        let mut w = VixWriter::new(&partial_schema, VixWriterOptions::default(), false);
        w.push_docs_rows(
            &Int64Array::from(vec![1]),
            &[],
            &StringArray::from(vec![r#"{"msg":"a","extra":"unseen"}"#]),
            None,
        )
        .unwrap();
        let partial_input = finish_open(w);
        assert!(partial_input.partial_fields().contains("extra"));
        let extra_schema = Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("msg", DataType::Utf8, true),
            Field::new("extra", DataType::Utf8, true),
        ]);
        let extra_plan = VixWriter::new(&extra_schema, VixWriterOptions::default(), false);
        let reason = extra_plan
            .check_merge_inputs(&[&partial_input])
            .unwrap_err();
        assert!(reason.contains("rebuild"), "{reason}");
        // but a plan that does not value-index it is fine (terms dropped)
        let no_extra_plan = VixWriter::new(&partial_schema, VixWriterOptions::default(), false);
        no_extra_plan.check_merge_inputs(&[&partial_input]).unwrap();

        // an input whose only irregularity is an OVERSIZE value carries no
        // taint at all since the 2026-08-12 skip-without-degrade call: its
        // dictionary simply lacks that one literal (a rebuild could not
        // index it either), so the fast path stays fully mergeable
        let oversize_opts = VixWriterOptions {
            max_raw_term_len: 8,
            ..VixWriterOptions::default()
        };
        let mut w = VixWriter::new(&partial_schema, oversize_opts.clone(), false);
        let long = "x".repeat(64);
        w.push_docs_rows(
            &Int64Array::from(vec![1]),
            &[],
            &StringArray::from(vec![format!("{{\"msg\":{long:?}}}")]),
            None,
        )
        .unwrap();
        let oversize_input = finish_open(w);
        assert!(
            oversize_input.partial_fields().is_empty(),
            "oversize values must not taint: {:?}",
            oversize_input.partial_fields()
        );
        let oversize_plan = VixWriter::new(&partial_schema, oversize_opts, false);
        oversize_plan
            .check_merge_inputs(&[&oversize_input])
            .unwrap();

        // ... but a partial field the PLAN marks fts is NOT mergeable: the
        // current writer never taints an fts field, so the marking means a
        // pre-fix input whose dictionary is missing the skipped oversize
        // values' TOKENS — only a `_source` rebuild re-derives them (and
        // drops the marking, un-tainting match_all for the merged file).
        // Fabricated via test-support property patching: the live pre-fix
        // shape is unbuildable through the current writer by design.
        let (tainted_data, tainted_index) = {
            let mut w = VixWriter::new(&schema, opts.clone(), false);
            w.push_batch_with_source(&batches[0], &sources[0], None)
                .unwrap();
            w.finish().unwrap()
        };
        let tainted_index = crate::test_support::repack_with_partial_fields(
            tainted_index.as_deref().expect("sidecar"),
            &["log"],
        )
        .unwrap();
        let tainted = open_built(tainted_data, Some(tainted_index));
        assert!(tainted.partial_fields().contains("log"));
        let fts_plan = VixWriter::new(&schema, opts.clone(), false);
        let reason = fts_plan.check_merge_inputs(&[&tainted]).unwrap_err();
        assert!(reason.contains("\"log\""), "{reason}");
        assert!(reason.contains("rebuild"), "{reason}");
    }

    /// The single-file healing probe shares the fast path's demotion
    /// detection: a term-planned field an input CARRIES without term
    /// capability (pre-numeric-value-terms files, previously demoted merge
    /// outputs — fabricated by dropping the capability from the fields
    /// table) is reported; healthy inputs report nothing; planned fields no
    /// document carries never fire.
    #[test]
    fn merge_inputs_lacking_term_capability_probe() {
        let schema = Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("msg", DataType::Utf8, true),
            Field::new("code", DataType::Int64, true),
        ]);
        let mut w = VixWriter::new(&schema, VixWriterOptions::default(), false);
        w.push_docs_rows(
            &Int64Array::from(vec![2, 1]),
            &[],
            &StringArray::from(vec![
                r#"{"msg":"a","code":500}"#,
                r#"{"msg":"b","code":200}"#,
            ]),
            None,
        )
        .unwrap();
        let (bytes, bytes_index) = w.finish().unwrap();
        let plan = VixWriter::new(&schema, VixWriterOptions::default(), false);

        // healthy: nothing lacking
        let healthy = open_built(bytes.clone(), bytes_index.clone());
        assert_eq!(
            plan.merge_inputs_lacking_term_capability(&[&healthy])
                .unwrap(),
            Vec::<String>::new()
        );

        // capability dropped (the pre-numeric-terms / demoted shape): the
        // carried field is reported
        let dropped = crate::test_support::repack_dropping_field_term_capability(
            bytes_index.as_deref().expect("sidecar"),
            "code",
        )
        .unwrap();
        let dropped = open_built(bytes.clone(), Some(dropped));
        assert!(!dropped.has_term_capability("code"));
        assert!(dropped.key_term_exists("code").unwrap());
        assert_eq!(
            plan.merge_inputs_lacking_term_capability(&[&dropped])
                .unwrap(),
            vec!["code".to_string()]
        );

        // a planned field no document carries never fires, with or without
        // capability
        let wider = Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("msg", DataType::Utf8, true),
            Field::new("code", DataType::Int64, true),
            Field::new("ghost", DataType::Utf8, true),
        ]);
        let wide_plan = VixWriter::new(&wider, VixWriterOptions::default(), false);
        assert_eq!(
            wide_plan
                .merge_inputs_lacking_term_capability(&[&healthy])
                .unwrap(),
            Vec::<String>::new()
        );
    }

    /// Merge-mode misuse and doc-id-map validation must error loudly.
    #[test]
    fn merge_mode_guards_and_map_validation() {
        let (schema, opts, batches, sources) = equivalence_inputs();
        let input = build_input(&schema, &opts, &batches[0], &sources[0]);
        let rows = input.row_count() as u32;

        // unindexed pushes require merge mode
        let mut writer = VixWriter::new(&schema, opts.clone(), false);
        let err = writer
            .push_docs_rows_unindexed(&timestamps_of(&batches[0]), &[], &sources[0], None)
            .unwrap_err();
        assert!(err.to_string().contains("merge_input_indexes"), "{err}");

        // merge must be the first operation
        let mut writer = VixWriter::new(&schema, opts.clone(), false);
        writer
            .push_batch_with_source(&batches[0], &sources[0], None)
            .unwrap();
        let err = writer
            .merge_input_indexes(&[&input], &[DocIdMap::Offset(0)], 1)
            .unwrap_err();
        assert!(err.to_string().contains("first operation"), "{err}");

        // map validation
        let mut writer = VixWriter::new(&schema, opts.clone(), false);
        let err = writer.merge_input_indexes(&[&input], &[], 1).unwrap_err();
        assert!(err.to_string().contains("doc-id maps"), "{err}");
        let err = writer
            .merge_input_indexes(&[&input], &[DocIdMap::Offset(1)], 1)
            .unwrap_err();
        assert!(err.to_string().contains("exceeds"), "{err}");
        let err = writer
            .merge_input_indexes(&[&input], &[DocIdMap::Table(vec![0])], 1)
            .unwrap_err();
        assert!(err.to_string().contains("entries"), "{err}");
        let err = writer
            .merge_input_indexes(&[&input], &[DocIdMap::Table(vec![0, 1, rows])], 1)
            .unwrap_err();
        assert!(err.to_string().contains("beyond"), "{err}");
        let err = writer
            .merge_input_indexes(
                &[&input, &input],
                &[DocIdMap::Offset(0), DocIdMap::Offset(1)],
                1,
            )
            .unwrap_err();
        assert!(err.to_string().contains("overlap"), "{err}");

        // after a successful merge: indexed pushes are rejected, and finish
        // demands exactly the merged rows
        let mut writer = VixWriter::new(&schema, opts.clone(), false);
        writer
            .merge_input_indexes(&[&input], &[DocIdMap::Offset(0)], 1)
            .unwrap();
        let err = writer
            .push_batch_with_source(&batches[0], &sources[0], None)
            .unwrap_err();
        assert!(err.to_string().contains("merge mode"), "{err}");
        let err = writer
            .push_docs_rows(&timestamps_of(&batches[0]), &[], &sources[0], None)
            .unwrap_err();
        assert!(err.to_string().contains("merge mode"), "{err}");
        let err = writer
            .merge_input_indexes(&[&input], &[DocIdMap::Offset(0)], 1)
            .unwrap_err();
        assert!(err.to_string().contains("first operation"), "{err}");
        let err = writer.finish().unwrap_err();
        assert!(err.to_string().contains("covers"), "{err}");
    }
}

/// Adversarial-review probes (write path / merge / lifecycle audit,
/// 2026-07-23). Each test either PROVES a currently-shipped behavior the
/// review flagged — kept green and commented as a bug reproduction, so a
/// future fix surfaces as a deliberate test update — or pins an invariant
/// the review verified.
mod review {
    use arrow::array::Float64Array;

    use super::*;
    use crate::VixDocs;

    /// FIXED (key-term asymmetry on non-finite floats).
    ///
    /// arrow-json serializes NaN/±Inf into `_source` as the JSON literal
    /// `null` (see `review_synthesize_source_exotic_values` in
    /// core/src/vix/source.rs) — `_source` is authoritative, so the
    /// column-driven writer now treats non-finite float slots as null too
    /// (`index_key_terms` skips them): both term derivations agree that
    /// such a row has no value at the path, and `IS NOT NULL` (KeyExists)
    /// answers identically regardless of which writer produced the file.
    /// The docs cs column still stores the real NaN/Inf value (only the
    /// key-term derivation changed).
    ///
    /// Reachable data: OTLP double attributes (protobuf carries NaN/Inf)
    /// and VRL math (exp/log overflow).
    #[test]
    fn review_key_terms_from_columns_vs_source_disagree_on_json_null() {
        let schema = Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("ratio", DataType::Float64, true),
        ]);
        // Exactly what search::datafusion::source_synthesis::synthesize_source produces for a
        // NaN/Inf float: arrow-json writes the literal `null`.
        let source = StringArray::from(vec![r#"{"_timestamp":10,"ratio":null}"#]);

        // column-driven (move job): NaN is a valid slot but non-finite ->
        // treated as null, no key term
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int64Array::from(vec![10])) as ArrayRef,
                Arc::new(Float64Array::from(vec![f64::NAN])),
            ],
        )
        .unwrap();
        let mut column_writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
        column_writer
            .push_batch_with_source(&batch, &source, None)
            .unwrap();
        let column_reader = finish_open(column_writer);

        // source-driven (compaction rebuild): the serialized `null` reads as
        // an absent field
        let mut source_writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
        source_writer
            .push_docs_rows(&Int64Array::from(vec![10]), &[], &source, None)
            .unwrap();
        let source_reader = finish_open(source_writer);

        for (context, reader) in [("column", &column_reader), ("source", &source_reader)] {
            assert_eq!(
                reader.key_exists("ratio").unwrap().count_set_bits(),
                0,
                "{context}-driven writer must treat the non-finite slot as null"
            );
        }
        // and the writers agree on the whole key coverage, not just "ratio"
        assert_eq!(
            column_reader.keys_with_prefix("").unwrap(),
            source_reader.keys_with_prefix("").unwrap()
        );

        // Sanity: for a finite float the two paths agree.
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int64Array::from(vec![10])) as ArrayRef,
                Arc::new(Float64Array::from(vec![1.5])),
            ],
        )
        .unwrap();
        let source = StringArray::from(vec![r#"{"_timestamp":10,"ratio":1.5}"#]);
        let mut column_writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
        column_writer
            .push_batch_with_source(&batch, &source, None)
            .unwrap();
        let column_reader = finish_open(column_writer);
        let mut source_writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
        source_writer
            .push_docs_rows(&Int64Array::from(vec![10]), &[], &source, None)
            .unwrap();
        let source_reader = finish_open(source_writer);
        assert_eq!(
            column_reader.key_exists("ratio").unwrap().count_set_bits(),
            source_reader.key_exists("ratio").unwrap().count_set_bits(),
            "finite values keep the two writer paths in agreement"
        );
    }

    /// FIXED (docs chunk sizing floor).
    ///
    /// `docs_chunk_bytes` is the byte budget of one docs-blob chunk — the
    /// decompression unit of a matched-row point read. The rows-per-chunk
    /// floor is now 64 (was 1024), so the budget governs wide rows: with
    /// ~4 KiB rows and a 16 KiB budget the writer cuts 64-row blocks and
    /// vortex's pipeline coalesces them only up to its ~1 MiB segment
    /// minimum (256 rows here) — the old floor forced 1024-row ~4.2 MiB
    /// decode units regardless of the budget. With the default 16 MiB
    /// budget the floor engages only past ~256 KiB average rows.
    #[test]
    fn review_docs_chunk_row_floor_overrides_byte_budget_for_wide_rows() {
        let schema = Schema::new(vec![Field::new("_timestamp", DataType::Int64, false)]);
        let rows = 1500usize;
        let wide = "x".repeat(4096);
        let opts = VixWriterOptions {
            docs_chunk_bytes: 16 * 1024, // 16 KiB budget, ~4 rows of this width
            ..Default::default()
        };
        let mut writer = VixWriter::new(&schema, opts, false);
        let ts = Int64Array::from((1..=rows as i64).rev().collect::<Vec<_>>());
        let source =
            StringArray::from_iter_values((0..rows).map(|_| format!("{{\"log\":\"{wide}\"}}")));
        writer.push_docs_rows(&ts, &[], &source, None).unwrap();
        let bytes = Bytes::from(writer.finish().unwrap().0);

        let docs = VixDocs::open(bytes).unwrap();
        let batches = docs.read_docs(None, None, None).unwrap();
        let chunk_rows: Vec<usize> = batches.iter().map(RecordBatch::num_rows).collect();
        assert_eq!(chunk_rows.iter().sum::<usize>(), rows);
        let max_chunk = chunk_rows.iter().copied().max().unwrap();
        assert!(
            (64..1024).contains(&max_chunk),
            "the writer floor (64) plus vortex's ~1 MiB coalescing must \
             decide the chunk, not the old 1024-row floor (chunks: {chunk_rows:?})"
        );
        // pin today's exact shape too (vortex ONE_MEG coalescing in
        // multiples of the 64-row block): loosen if vortex retunes it
        assert_eq!(
            max_chunk, 256,
            "~1 MiB / ~4.1 KiB rows in 64-row multiples (chunks: {chunk_rows:?})"
        );
    }

    /// Companion to the floor change: normal ~1-2 KiB rows under the
    /// default 16 MiB budget keep their budget-derived chunk row count (the
    /// budget — neither the floor nor the cap — decides), so full-scan
    /// throughput of typical files is untouched. The corpus is bigger than
    /// one budget so the budget actually cuts the file (M9: 6,000 rows fit
    /// a single 16 MiB chunk and proved nothing).
    #[test]
    fn docs_chunk_default_budget_unchanged_for_normal_rows() {
        let schema = Schema::new(vec![Field::new("_timestamp", DataType::Int64, false)]);
        let rows = 30_000usize;
        let body = "x".repeat(1500);
        let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
        let ts = Int64Array::from((1..=rows as i64).rev().collect::<Vec<_>>());
        let source =
            StringArray::from_iter_values((0..rows).map(|_| format!("{{\"log\":\"{body}\"}}")));
        writer.push_docs_rows(&ts, &[], &source, None).unwrap();
        let bytes = Bytes::from(writer.finish().unwrap().0);

        let docs = VixDocs::open(bytes).unwrap();
        let batches = docs.read_docs(None, None, None).unwrap();
        let chunk_rows: Vec<usize> = batches.iter().map(RecordBatch::num_rows).collect();
        assert_eq!(chunk_rows.iter().sum::<usize>(), rows);
        let max_chunk = chunk_rows.iter().copied().max().unwrap();
        // budget/avg ≈ 16 MiB / ~1.5 KiB ≈ 11k rows: far above both the old
        // (1024) and new (64) floor — the floor change cannot alter it
        assert!(
            max_chunk > 1024,
            "normal rows must still be budget-sized, got {max_chunk} (chunks: {chunk_rows:?})"
        );
        // and far below the file: the byte budget (not the 65,536 cap, not
        // file size) is what cut it
        assert!(
            max_chunk < rows,
            "the 16 MiB budget must cut a {rows}-row file into several chunks, \
             got one of {max_chunk} (chunks: {chunk_rows:?})"
        );
        assert!(max_chunk <= 65536);
    }

    /// Manual timing harness for the docs-chunk floor change: full-scan and
    /// one-row point-read timings over the SAME wide-row data at the old
    /// floor's chunking (1024 rows/chunk, forced via a large budget), the
    /// default budget, and the new 64-row floor.
    ///
    /// ```text
    /// cargo test --release -p vortex_index \
    ///   review::bench_docs_chunk_floor_scan -- --ignored --nocapture
    /// ```
    #[test]
    #[ignore = "manual timing harness (run --release --nocapture)"]
    fn bench_docs_chunk_floor_scan() {
        let rows = 6000usize;
        let payload = "x".repeat(8 * 1024); // ~8 KiB rows
        let schema = Schema::new(vec![Field::new("_timestamp", DataType::Int64, false)]);
        let build = |budget: usize| {
            let opts = VixWriterOptions {
                docs_chunk_bytes: budget,
                ..Default::default()
            };
            let mut writer = VixWriter::new(&schema, opts, false);
            let ts = Int64Array::from((1..=rows as i64).rev().collect::<Vec<_>>());
            let source = StringArray::from_iter_values(
                (0..rows).map(|i| format!("{{\"i\":{i},\"log\":\"{payload}\"}}")),
            );
            writer.push_docs_rows(&ts, &[], &source, None).unwrap();
            Bytes::from(writer.finish().unwrap().0)
        };

        // budget -> effective rows/chunk with ~8.2 KiB rows (the writer's
        // 64-row blocks get coalesced by vortex up to its ~1 MiB minimum):
        //   16 KiB    -> ~128 (the new floor + vortex 1 MiB coalescing),
        //   16 MiB    -> ~2000 (default budget, unchanged by the floor),
        //   8.75 MiB  -> ~1024 (the OLD floor's chunking, for comparison)
        for (label, budget) in [
            ("new floor + coalesce", 16 * 1024),
            ("default 16MiB budget", crate::DEFAULT_DOCS_CHUNK_BYTES),
            ("old floor (~1024 rows)", 8 * 1024 * 1024 + 768 * 1024),
        ] {
            let bytes = build(budget);
            let docs = VixDocs::open(bytes).unwrap();
            let mut chunk_rows: Vec<usize> = Vec::new();
            // warm-up + chunk shape
            docs.scan_docs(None, None, None, &mut |batch| {
                chunk_rows.push(batch.num_rows());
                Ok(())
            })
            .unwrap();
            assert_eq!(chunk_rows.iter().sum::<usize>(), rows);

            let runs = 5;
            let started = std::time::Instant::now();
            for _ in 0..runs {
                let mut scanned = 0usize;
                docs.scan_docs(None, None, None, &mut |batch| {
                    scanned += batch.num_rows();
                    Ok(())
                })
                .unwrap();
                assert_eq!(scanned, rows);
            }
            let full_scan = started.elapsed() / runs;

            let started = std::time::Instant::now();
            for run in 0..runs {
                let row = (rows / 2 + run as usize) as u64;
                let got = docs
                    .read_docs(Some(&["_source".to_string()]), Some(vec![row]), None)
                    .unwrap();
                assert_eq!(got.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
            }
            let point_read = started.elapsed() / runs;

            eprintln!(
                "[chunk floor bench] {label:>22}: budget={budget:>8}B chunks={} \
                 (max {} rows) full-scan={full_scan:?} ({:.0} rows/s) point-read={point_read:?}",
                chunk_rows.len(),
                chunk_rows.iter().copied().max().unwrap(),
                rows as f64 / full_scan.as_secs_f64(),
            );
        }
    }
}

// =====================================================================
// Adversarial review tests — query-evaluation correctness pass.
//
// `review_finding_*` tests PIN currently-wrong behavior (each carries a
// FINDING comment describing the correct answer); they are tripwires that
// must be updated when the underlying bug is fixed. `review_*` tests
// without `finding` VERIFY an attacked area as correct.
// =====================================================================
mod review_query_eval {
    use super::*;
    use crate::query::KEY_FIELD_ID;

    /// Build a one-batch core file: `svc` structured (raw whole-value
    /// terms), `msg` full-text (tokens only).
    fn review_file(rows: &[(i64, Option<&str>, Option<&str>)]) -> VixReader {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("msg", DataType::Utf8, true),
            Field::new("svc", DataType::Utf8, true),
        ]));
        let opts = VixWriterOptions {
            fts_field_names: vec!["msg".to_string()],
            ..Default::default()
        };
        let mut writer = VixWriter::new(&schema, opts, false);
        let ts: Vec<i64> = rows.iter().map(|(t, ..)| *t).collect();
        let msgs: Vec<Option<&str>> = rows.iter().map(|(_, m, _)| *m).collect();
        let svcs: Vec<Option<&str>> = rows.iter().map(|(_, _, s)| *s).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ts.clone())) as ArrayRef,
                Arc::new(StringArray::from(msgs)),
                Arc::new(StringArray::from(svcs)),
            ],
        )
        .unwrap();
        let sources: Vec<String> = ts
            .iter()
            .map(|t| format!(r#"{{"_timestamp":{t}}}"#))
            .collect();
        writer
            .push_batch_with_source(&batch, &StringArray::from(sources), None)
            .unwrap();
        finish_open(writer)
    }

    /// FIXED (was `review_finding_empty_string_values_invisible_and_not_
    /// partial`): an empty-string cell value IS raw-term-indexed — the
    /// 3-byte composite key `\x00{field_id}` is a valid dictionary entry —
    /// so `svc = ''` answers exactly from the index (matching the old
    /// tantivy writer, which indexed `doc.add_text(field, "")`), and the
    /// field needs no `partial_fields` fallback marker. The negation is the
    /// usual index complement: it still includes null rows (SQL `!=`
    /// excludes them), which the search layer compensates for — see the
    /// separate NotEqual finding on the search side.
    #[test]
    fn review_finding_empty_string_values_invisible_and_not_partial() {
        let reader = review_file(&[
            (100, None, Some("a")),
            (99, None, Some("")),
            (98, None, Some("b")),
            (97, None, None),
        ]);

        // the index knows doc 1 has a (non-null) svc value ...
        assert_eq!(key_exists_set(&reader, "svc"), docs(&[0, 1, 2]));
        // ... and svc is a fully term-capable, non-partial field
        assert!(reader.has_term_capability("svc"));
        assert!(
            reader.partial_fields().is_empty(),
            "the empty value is indexed; no fallback marker is needed"
        );

        // SQL `svc = ''` matches doc 1, straight from the index
        let eq_empty = exact("svc", "");
        assert_eq!(eval_set(&reader, &eq_empty), docs(&[1]));
        assert_eq!(reader.count(&eq_empty).unwrap(), 1);

        // the raw complement excludes the "" row now; the null row (3)
        // remains the caller's NotEqual null-compensation to strip
        let ne_empty = VixQuery::Not(Box::new(exact("svc", "")));
        assert_eq!(eval_set(&reader, &ne_empty), docs(&[0, 2, 3]));

        // the empty term is an ordinary dictionary citizen: per-value
        // counts include it and reconcile exactly against the key term
        assert_eq!(
            reader.field_value_counts("svc").unwrap(),
            Some(vec![
                (b"".to_vec(), 1),
                (b"a".to_vec(), 1),
                (b"b".to_vec(), 1),
            ])
        );
    }

    /// VERIFIED: `count()` short-cuts (`doc_count` column, no postings)
    /// agree with `eval().count_set_bits()` for every query shape,
    /// including composites, negations, dense-elided terms, key terms and
    /// any-field scans.
    #[test]
    fn review_count_matches_eval_across_query_shapes() {
        let reader = build_docs_dataset(false);
        let queries: Vec<VixQuery> = vec![
            VixQuery::All,
            exact("level", "error"),
            exact("level", "nosuch"),
            exact("svc", "api"),
            exact("svc", ""),
            any_token("error"),
            any_token("timeout"),
            any_token("nosuch"),
            prefix(None, "err"),
            prefix(None, ""),
            prefix(Some("level"), "e"),
            contains(None, "ime", false),
            contains(None, "IME", true),
            regex(None, "err.*"),
            regex(Some("svc"), "a.*"),
            VixQuery::Fuzzy {
                token: "erro".to_string(),
                distance: 1,
            },
            VixQuery::KeyExists {
                path: "level".to_string(),
            },
            VixQuery::KeyExists {
                path: "nosuch".to_string(),
            },
            VixQuery::Not(Box::new(exact("level", "error"))),
            VixQuery::Not(Box::new(VixQuery::All)),
            VixQuery::And(vec![]),
            VixQuery::Or(vec![]),
            VixQuery::And(vec![exact("level", "error"), exact("svc", "db")]),
            VixQuery::And(vec![
                exact("level", "error"),
                VixQuery::Not(Box::new(exact("svc", "api"))),
            ]),
            VixQuery::And(vec![
                VixQuery::Not(Box::new(exact("level", "error"))),
                VixQuery::Not(Box::new(exact("svc", "db"))),
            ]),
            VixQuery::Or(vec![
                exact("level", "warn"),
                VixQuery::And(vec![any_token("timeout"), exact("svc", "web")]),
            ]),
            VixQuery::Not(Box::new(VixQuery::Or(vec![
                exact("level", "info"),
                exact("level", "warn"),
            ]))),
        ];
        for query in &queries {
            let bits = reader.eval(query).unwrap();
            assert_eq!(bits.len() as u64, reader.row_count(), "{query:?}");
            assert_eq!(
                reader.count(query).unwrap(),
                bits.count_set_bits() as u64,
                "count() != eval().count_set_bits() for {query:?}"
            );
        }
    }

    /// VERIFIED: the rarest-first AND short-circuit is semantics-preserving
    /// with `Not`/`Or`/`All` children mixed in — including the empty-leaf
    /// early exit (a leaf with no matching term must still return the
    /// correct EMPTY intersection even when a `Not` child would match
    /// everything), all-`Not` children, `And`/`Or` conventions for empty
    /// child lists, and permutation invariance.
    #[test]
    fn review_and_short_circuit_with_composite_children() {
        let reader = build_docs_dataset(false);
        let all: BTreeSet<u32> = (0..10).collect();
        let level_error = docs(&[1, 5, 8]);
        let svc_api = docs(&[0, 1, 6, 9]);
        let svc_db = docs(&[4, 5]);

        // leaf-miss short-circuit must not skip Not children incorrectly:
        // the AND is empty regardless of what Not evaluates to
        assert_eq!(
            eval_set(
                &reader,
                &VixQuery::And(vec![
                    exact("level", "nosuch"),
                    VixQuery::Not(Box::new(exact("level", "error"))),
                ])
            ),
            docs(&[])
        );

        // a lone Not child (no leaves at all)
        assert_eq!(
            eval_set(
                &reader,
                &VixQuery::And(vec![VixQuery::Not(Box::new(exact("level", "nosuch")))])
            ),
            all
        );

        // all-Not children
        let expected: BTreeSet<u32> = all
            .iter()
            .copied()
            .filter(|d| !level_error.contains(d) && !svc_db.contains(d))
            .collect();
        assert_eq!(
            eval_set(
                &reader,
                &VixQuery::And(vec![
                    VixQuery::Not(Box::new(exact("level", "error"))),
                    VixQuery::Not(Box::new(exact("svc", "db"))),
                ])
            ),
            expected
        );

        // mixed leaf + Not: brute-force reference
        let expected: BTreeSet<u32> = level_error
            .iter()
            .copied()
            .filter(|d| !svc_api.contains(d))
            .collect();
        assert_eq!(
            eval_set(
                &reader,
                &VixQuery::And(vec![
                    exact("level", "error"),
                    VixQuery::Not(Box::new(exact("svc", "api"))),
                ])
            ),
            expected
        );

        // permutation invariance with composites in every position
        let children = [
            exact("level", "error"),
            VixQuery::Not(Box::new(exact("svc", "api"))),
            VixQuery::Or(vec![any_token("error"), any_token("timeout")]),
            VixQuery::All,
        ];
        let reference = eval_set(&reader, &VixQuery::And(children.to_vec()));
        let mut rotated = children.to_vec();
        rotated.rotate_left(2);
        assert_eq!(eval_set(&reader, &VixQuery::And(rotated)), reference);
        let mut reversed = children.to_vec();
        reversed.reverse();
        assert_eq!(eval_set(&reader, &VixQuery::And(reversed)), reference);

        // documented conventions: empty And == All, empty Or == nothing;
        // and a composite child evaluating to empty still empties the And
        assert_eq!(eval_set(&reader, &VixQuery::And(vec![])), all);
        assert_eq!(eval_set(&reader, &VixQuery::Or(vec![])), docs(&[]));
        assert_eq!(
            eval_set(
                &reader,
                &VixQuery::And(vec![VixQuery::All, VixQuery::Or(vec![])])
            ),
            docs(&[])
        );
        assert_eq!(
            eval_set(
                &reader,
                &VixQuery::And(vec![VixQuery::Not(Box::new(VixQuery::All))])
            ),
            docs(&[])
        );
    }

    /// VERIFIED: any-field token scans (`TokenAnyField`, `Prefix{field:
    /// None}`) never leak key terms — even when a document VALUE equals a
    /// field NAME (the key term `svc\0\xFF\xFF` is dense across all docs
    /// with svc set; counting it would wildly over-match) — and composite
    /// keys with embedded NULs round-trip exactly.
    #[test]
    fn review_token_scans_skip_key_terms_and_handle_nul() {
        // values deliberately equal to field names, plus a NUL-bearing value
        let reader = review_file(&[
            (100, Some("plain words"), Some("svc")),
            (99, None, Some("level")),
            (98, Some("svc here too"), Some("msg")),
            (97, None, Some("a\u{0}b")),
            (96, None, Some("zzz")),
        ]);

        // "svc" as a VALUE: doc 0 (svc column) and doc 2 (msg token);
        // the key term "svc\0\xFF\xFF" (docs 0..=4) must not leak in
        assert_eq!(eval_set(&reader, &any_token("svc")), docs(&[0, 2]));
        assert_eq!(reader.count(&any_token("svc")).unwrap(), 2);
        // "msg" as a value: doc 2's svc — not every doc with the msg key
        assert_eq!(eval_set(&reader, &any_token("msg")), docs(&[2]));
        // prefix scans skip key terms too
        assert_eq!(eval_set(&reader, &prefix(None, "sv")), docs(&[0, 2]));
        assert_eq!(eval_set(&reader, &prefix(None, "m")), docs(&[2]));

        // NUL-embedded value: exact per-field and any-field lookups
        assert_eq!(eval_set(&reader, &exact("svc", "a\u{0}b")), docs(&[3]));
        assert_eq!(eval_set(&reader, &any_token("a\u{0}b")), docs(&[3]));
        // and it must not shadow/collide with sibling tokens
        assert_eq!(eval_set(&reader, &exact("svc", "a")), docs(&[]));
        assert_eq!(eval_set(&reader, &exact("svc", "b")), docs(&[]));

        // key-existence stays exact alongside
        assert_eq!(key_exists_set(&reader, "svc"), docs(&[0, 1, 2, 3, 4]));
        assert_eq!(key_exists_set(&reader, "msg"), docs(&[0, 2]));
    }

    /// VERIFIED: exact lookups and prefix scans are correct when matches
    /// span dictionary block boundaries.
    #[test]
    fn review_prefix_and_exact_across_row_group_boundaries() {
        // Size the values from the production block target so this fixture
        // keeps spanning several blocks when the target is tuned. Comparing
        // the first and last value's predecessor blocks below proves that
        // field-boundary block cuts cannot make the assertion pass by chance.
        const VALUE_BYTES: usize = crate::dict_blocks::BLOCK_TARGET_BYTES / 8;
        let values: Vec<String> = (0..40)
            .map(|i| format!("k{i:02}{}", "x".repeat(VALUE_BYTES)))
            .collect();
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("svc", DataType::Utf8, true),
        ]));
        let opts = VixWriterOptions::default();
        let mut writer = VixWriter::new(&schema, opts, false);
        let ts: Vec<i64> = (0..40).map(|i| 1000 - i as i64).collect();
        let svc: Vec<Option<&str>> = values.iter().map(|v| Some(v.as_str())).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ts.clone())) as ArrayRef,
                Arc::new(StringArray::from(svc)),
            ],
        )
        .unwrap();
        let sources: Vec<String> = ts
            .iter()
            .map(|t| format!(r#"{{"_timestamp":{t}}}"#))
            .collect();
        writer
            .push_batch_with_source(&batch, &StringArray::from(sources), None)
            .unwrap();
        let reader = finish_open(writer);
        let field_id = reader.field_id("svc").expect("svc field id");
        let mut first_key = Vec::new();
        crate::query::write_composite(&mut first_key, values[0].as_bytes(), field_id);
        let mut last_key = Vec::new();
        crate::query::write_composite(&mut last_key, values.last().unwrap().as_bytes(), field_id);
        let dict_index = reader.dict_index().expect("dictionary index");
        let first_block = dict_index
            .predecessor_block(&first_key)
            .expect("first block lookup")
            .expect("first svc block");
        let last_block = dict_index
            .predecessor_block(&last_key)
            .expect("last block lookup")
            .expect("last svc block");
        assert!(
            last_block > first_block,
            "svc values must span dictionary blocks, got block {first_block}..={last_block}"
        );

        // Exact lookup of every value, including values in different blocks.
        for (i, value) in values.iter().enumerate() {
            assert_eq!(
                eval_set(&reader, &exact("svc", value)),
                docs(&[i as u32]),
                "exact {value}"
            );
        }
        // Prefixes spanning several blocks.
        assert_eq!(
            eval_set(&reader, &prefix(None, "k1")),
            (10..20).collect::<BTreeSet<u32>>()
        );
        assert_eq!(
            eval_set(&reader, &prefix(Some("svc"), "k3")),
            (30..40).collect::<BTreeSet<u32>>()
        );
        // the whole key space
        assert_eq!(
            eval_set(&reader, &prefix(None, "k")),
            (0..40).collect::<BTreeSet<u32>>()
        );
        // a prefix equal to a complete term
        assert_eq!(eval_set(&reader, &prefix(None, "k07")), docs(&[7]));
        // beyond the last term
        assert_eq!(eval_set(&reader, &prefix(None, "k40")), docs(&[]));
        assert_eq!(eval_set(&reader, &prefix(None, "z")), docs(&[]));
    }

    /// VERIFIED: dense-elided postings (empty blob, doc_count == row_count)
    /// interact correctly with Not / And / Or / count and with sparse terms
    /// resolved by the same leaf.
    #[test]
    fn review_dense_elision_negation_and_intersection() {
        // svc == "const" everywhere (dense value term + dense key term);
        // msg tokens sparse
        let reader = review_file(&[
            (100, Some("alpha beta"), Some("const")),
            (99, Some("beta gamma"), Some("const")),
            (98, None, Some("const")),
            (97, Some("alpha"), Some("const")),
        ]);

        // prove the elision actually engaged (empty postings blob)
        let svc_id = reader.field_id("svc").expect("svc term-indexed");
        assert_eq!(
            reader.debug_postings_len(b"const", svc_id).unwrap(),
            Some(0),
            "value term must be dense-elided"
        );
        assert_eq!(
            reader.debug_postings_len(b"svc", KEY_FIELD_ID).unwrap(),
            Some(0),
            "key term must be dense-elided"
        );

        let all = docs(&[0, 1, 2, 3]);
        let dense = exact("svc", "const");
        assert_eq!(eval_set(&reader, &dense), all);
        assert_eq!(reader.count(&dense).unwrap(), 4);
        // negation of a dense term is empty
        assert_eq!(
            eval_set(&reader, &VixQuery::Not(Box::new(dense.clone()))),
            docs(&[])
        );
        // dense AND sparse == sparse
        assert_eq!(
            eval_set(
                &reader,
                &VixQuery::And(vec![dense.clone(), any_token("alpha")])
            ),
            docs(&[0, 3])
        );
        // sparse OR dense == all
        assert_eq!(
            eval_set(&reader, &VixQuery::Or(vec![any_token("gamma"), dense])),
            all
        );
        // a leaf resolving BOTH a dense and a sparse term unions to all
        // (prefix "const"/"c..." only hits the dense term here; use the
        // any-field scan across msg tokens + svc values)
        assert_eq!(eval_set(&reader, &prefix(None, "")), all);
        assert_eq!(reader.count(&prefix(None, "")).unwrap(), 4);
        // dense key term through KeyExists and its negation
        assert_eq!(
            eval_set(
                &reader,
                &VixQuery::KeyExists {
                    path: "svc".to_string()
                }
            ),
            all
        );
        assert_eq!(
            eval_set(
                &reader,
                &VixQuery::Not(Box::new(VixQuery::KeyExists {
                    path: "svc".to_string()
                }))
            ),
            docs(&[])
        );
    }

    /// VERIFIED: regex evaluation is anchored (full-token match, like the
    /// tantivy RegexQuery it replaces) and fuzzy distances behave at the
    /// boundaries.
    #[test]
    fn review_regex_anchoring_and_fuzzy_bounds() {
        let reader = build_docs_dataset(false);
        let level_error = docs(&[1, 5, 8]);

        // "err" must NOT match the token "error" (anchored semantics)
        assert_eq!(eval_set(&reader, &regex(Some("level"), "err")), docs(&[]));
        assert_eq!(
            eval_set(&reader, &regex(Some("level"), "err.*")),
            level_error
        );
        assert_eq!(
            eval_set(&reader, &regex(Some("level"), ".*rror")),
            level_error
        );
        assert_eq!(
            eval_set(&reader, &regex(Some("level"), "error")),
            level_error
        );

        // fuzzy: distance counts edits on the token
        let fuzzy = |token: &str, distance: u8| VixQuery::Fuzzy {
            token: token.to_string(),
            distance,
        };
        // exact-only at distance 0
        assert_eq!(
            eval_set(&reader, &fuzzy("error", 0)),
            docs(&[0, 1, 5, 8]) // level values + log tokens
        );
        // "erro" -> "error" is one insertion
        assert_eq!(eval_set(&reader, &fuzzy("erro", 0)), docs(&[]));
        assert_eq!(eval_set(&reader, &fuzzy("erro", 1)), docs(&[0, 1, 5, 8]));
        // distance > 2 is rejected, not silently clamped, at the reader
        assert!(reader.eval(&fuzzy("error", 3)).is_err());
    }
}

/// Numeric/bool value terms (Layer A): every finite value emits ONE tagged
/// canonical term — value-based, spelling-insensitive — probed explicitly,
/// while every string-shaped scan keeps ignoring them.
#[test]
fn numeric_and_bool_value_terms_roundtrip() {
    use arrow::array::{BooleanArray, Float64Array, UInt64Array};

    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("credit", DataType::Float64, true),
        Field::new("code", DataType::Int64, true),
        Field::new("big", DataType::UInt64, true),
        Field::new("ok", DataType::Boolean, true),
        Field::new("svc", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![100, 99, 98, 97])) as ArrayRef,
            Arc::new(Float64Array::from(vec![
                Some(38.0),
                Some(38.5),
                Some(f64::NAN), // non-finite: key-term-less, value-term-less
                None,
            ])),
            Arc::new(Int64Array::from(vec![Some(38), Some(-5), None, Some(38)])),
            Arc::new(UInt64Array::from(vec![Some(u64::MAX), None, None, None])),
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                None,
                Some(true),
            ])),
            Arc::new(StringArray::from(vec![Some("38.0"), None, None, None])),
        ],
    )
    .unwrap();
    let source = synthesize_source_for_test(&batch);
    let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
    writer
        .push_batch_with_source(&batch, &source, None)
        .unwrap();
    let reader = finish_open(writer);

    // canonical float form: ryu shortest — the VALUE 38.0, not a spelling
    assert_eq!(
        eval_set(&reader, &exact_numeric("credit", "38.0")),
        docs(&[0])
    );
    assert_eq!(
        eval_set(&reader, &exact_numeric("credit", "38.5")),
        docs(&[1])
    );
    // int and float forms are DISTINCT terms (queries probe the union)
    assert_eq!(eval_set(&reader, &exact_numeric("credit", "38")), docs(&[]));
    assert_eq!(
        eval_set(&reader, &exact_numeric("code", "38")),
        docs(&[0, 3])
    );
    assert_eq!(eval_set(&reader, &exact_numeric("code", "-5")), docs(&[1]));
    // u64 beyond i64 keeps its exact decimal image
    assert_eq!(
        eval_set(&reader, &exact_numeric("big", "18446744073709551615")),
        docs(&[0])
    );
    // bools
    assert_eq!(
        eval_set(&reader, &exact_numeric("ok", "true")),
        docs(&[0, 3])
    );
    assert_eq!(eval_set(&reader, &exact_numeric("ok", "false")), docs(&[1]));
    // the STRING "38.0" (type drift) is a separate raw term: it does not
    // collide with the tagged float term, and vice versa
    assert_eq!(eval_set(&reader, &exact("svc", "38.0")), docs(&[0]));
    assert_eq!(eval_set(&reader, &exact("credit", "38.0")), docs(&[]));
    // non-finite floats behave like null end to end
    assert_eq!(key_exists_set(&reader, "credit"), docs(&[0, 1]));

    // string-shaped scans ignore tagged numeric terms entirely:
    // - substring/regex/fuzzy walks
    assert_eq!(
        eval_set(&reader, &contains(Some("code"), "38", false)),
        docs(&[])
    );
    assert_eq!(eval_set(&reader, &contains(None, "38", false)), docs(&[0])); // svc's raw string only
    assert_eq!(eval_set(&reader, &regex(Some("code"), ".*38.*")), docs(&[]));
    // - match_all token scans (query tokens are alnum; the tag byte sorts tagged terms outside
    //   every token range). The svc STRING "38.0" is a raw term and stays prefix-reachable (the
    //   documented match_all superset); the numeric 38.0/38 values are not.
    assert_eq!(eval_set(&reader, &any_token("38")), docs(&[]));
    assert_eq!(eval_set(&reader, &prefix(None, "38")), docs(&[0]));
    // exact-count fast path works on tagged probes
    assert_eq!(reader.count(&exact_numeric("code", "38")).unwrap(), 2);
}

/// Column-driven and source-driven extraction emit byte-identical numeric
/// terms: the canonicalization is VALUE-based on both sides (the spelling
/// variants `38.00` / `3.8e1` collapse into the ryu form the column path
/// writes).
#[test]
fn numeric_terms_agree_between_column_and_source_paths() {
    use arrow::array::{BooleanArray, Float64Array, UInt64Array};

    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("credit", DataType::Float64, true),
        Field::new("code", DataType::Int64, true),
        Field::new("big", DataType::UInt64, true),
        Field::new("ok", DataType::Boolean, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![100, 99, 98])) as ArrayRef,
            Arc::new(Float64Array::from(vec![Some(38.0), Some(1e20), Some(-0.0)])),
            Arc::new(Int64Array::from(vec![Some(38), Some(i64::MIN), None])),
            Arc::new(UInt64Array::from(vec![Some(u64::MAX), Some(0), None])),
            Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
        ],
    )
    .unwrap();
    let source = synthesize_source_for_test(&batch);

    let mut column_driven = VixWriter::new(&schema, VixWriterOptions::default(), false);
    column_driven
        .push_batch_with_source(&batch, &source, None)
        .unwrap();
    let mut source_driven = VixWriter::new(&schema, VixWriterOptions::default(), false);
    source_driven
        .push_docs_rows(&timestamps_of(&batch), &[], &source, None)
        .unwrap();

    let column_reader = finish_open(column_driven);
    let source_reader = finish_open(source_driven);
    assert_eq!(
        column_reader.debug_all_terms().unwrap(),
        source_reader.debug_all_terms().unwrap()
    );

    // spelling variants of one VALUE parse into the same canonical term the
    // column path wrote
    let spelled = StringArray::from_iter_values([
        r#"{"_timestamp":100,"credit":38.00}"#,
        r#"{"_timestamp":99,"credit":3.8e1}"#,
    ]);
    let mut variant_writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
    variant_writer
        .push_docs_rows(&Int64Array::from(vec![100, 99]), &[], &spelled, None)
        .unwrap();
    let variant_reader = finish_open(variant_writer);
    assert_eq!(
        eval_set(&variant_reader, &exact_numeric("credit", "38.0")),
        docs(&[0, 1])
    );
    assert!(variant_reader.partial_fields().is_empty());
}

/// Merge capability INTERSECTION: a term-planned field some input carries
/// (key term) without term capability there — a numeric field in a file
/// written before numeric value terms existed — is DEMOTED in the merged
/// fields table, so lookups take the skip + filter-back path instead of
/// silently missing the uncovered rows. A rebuild of the same inputs
/// restores full capability (it re-derives every term from `_source`).
#[test]
fn merge_demotes_capability_carried_without_terms() {
    use crate::DocIdMap;

    // OLD-style file: `code` exists in the rows (key terms + _source) but
    // the writer plan does not know it — no value terms, no entry. Emulated
    // by a writer schema WITHOUT the column while the batch carries it.
    let old_plan_schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("svc", DataType::Utf8, true),
    ]));
    let old_batch_schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("svc", DataType::Utf8, true),
        Field::new("code", DataType::Int64, true),
    ]));
    let old_batch = RecordBatch::try_new(
        Arc::clone(&old_batch_schema),
        vec![
            Arc::new(Int64Array::from(vec![100, 99])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("api"), Some("db")])),
            Arc::new(Int64Array::from(vec![Some(38), Some(7)])),
        ],
    )
    .unwrap();
    let old_source = synthesize_source_for_test(&old_batch);
    let mut old_writer = VixWriter::new(&old_plan_schema, VixWriterOptions::default(), false);
    old_writer
        .push_batch_with_source(&old_batch, &old_source, None)
        .unwrap();
    let old_reader = finish_open(old_writer);
    assert!(!old_reader.has_term_capability("code"));
    assert!(old_reader.key_term_exists("code").unwrap());

    // NEW-style file: `code` fully term-indexed.
    let new_schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("svc", DataType::Utf8, true),
        Field::new("code", DataType::Int64, true),
    ]));
    let new_batch = RecordBatch::try_new(
        Arc::clone(&new_schema),
        vec![
            Arc::new(Int64Array::from(vec![80, 70])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("api"), Some("web")])),
            Arc::new(Int64Array::from(vec![Some(38), Some(9)])),
        ],
    )
    .unwrap();
    let new_source = synthesize_source_for_test(&new_batch);
    let mut new_writer = VixWriter::new(&new_schema, VixWriterOptions::default(), false);
    new_writer
        .push_batch_with_source(&new_batch, &new_source, None)
        .unwrap();
    let new_reader = finish_open(new_writer);
    assert!(new_reader.has_term_capability("code"));

    // Index-merge fast path over old + new.
    let refs = [&old_reader, &new_reader];
    let doc_maps = [DocIdMap::Offset(0), DocIdMap::Offset(2)];
    let merged_ts = Int64Array::from(vec![100, 99, 80, 70]);
    let merged_source = StringArray::from_iter_values(
        (0..2)
            .map(|row| old_source.value(row).to_string())
            .chain((0..2).map(|row| new_source.value(row).to_string())),
    );
    let mut merged = VixWriter::new(&new_schema, VixWriterOptions::default(), false);
    merged.check_merge_inputs(&refs).unwrap();
    merged.merge_input_indexes(&refs, &doc_maps, 2).unwrap();
    merged
        .push_docs_rows_unindexed(&merged_ts, &[], &merged_source, None)
        .unwrap();
    let merged_reader = finish_open(merged);

    // `code` is DEMOTED: no term capability (per-field lookups error →
    // callers skip + filter back), while its key terms stay exact and the
    // string field keeps full capability.
    assert!(!merged_reader.has_term_capability("code"));
    assert!(merged_reader.eval(&exact_numeric("code", "38")).is_err());
    assert_eq!(key_exists_set(&merged_reader, "code"), docs(&[0, 1, 2, 3]));
    assert!(merged_reader.has_term_capability("svc"));
    assert_eq!(
        eval_set(&merged_reader, &exact("svc", "api")),
        docs(&[0, 2])
    );
    // not partial: the demotion is a capability statement, not a taint
    assert!(!merged_reader.partial_fields().contains("code"));

    // A REBUILD of the same rows (the compactor's fallback / a later
    // rebuild-triggering merge) re-derives from `_source` and restores full
    // capability — old rows included.
    let mut rebuild = VixWriter::new(&new_schema, VixWriterOptions::default(), false);
    rebuild
        .push_docs_rows(&merged_ts, &[], &merged_source, None)
        .unwrap();
    let rebuild_reader = finish_open(rebuild);
    assert!(rebuild_reader.has_term_capability("code"));
    assert_eq!(
        eval_set(&rebuild_reader, &exact_numeric("code", "38")),
        docs(&[0, 2])
    );

    // Merging two NEW-style files keeps full capability (no demotion).
    let refs = [&new_reader, &new_reader];
    let doc_maps = [DocIdMap::Offset(0), DocIdMap::Offset(2)];
    let both_source = StringArray::from_iter_values(
        (0..2)
            .map(|row| new_source.value(row).to_string())
            .chain((0..2).map(|row| new_source.value(row).to_string())),
    );
    let mut both_new = VixWriter::new(&new_schema, VixWriterOptions::default(), false);
    both_new.check_merge_inputs(&refs).unwrap();
    both_new.merge_input_indexes(&refs, &doc_maps, 2).unwrap();
    both_new
        .push_docs_rows_unindexed(
            &Int64Array::from(vec![80, 70, 80, 70]),
            &[],
            &both_source,
            None,
        )
        .unwrap();
    let both_reader = finish_open(both_new);
    assert!(both_reader.has_term_capability("code"));
    assert_eq!(
        eval_set(&both_reader, &exact_numeric("code", "38")),
        docs(&[0, 2])
    );
}

/// The writer tracks the `_timestamp` range of the rows it stores —
/// `VixWriterStats::{min_ts, max_ts}` is derived from the DATA, the
/// authoritative source for `FileMeta` (upstream footer metadata has been
/// observed degenerate).
#[test]
fn writer_stats_track_timestamp_range() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("svc", DataType::Utf8, true),
    ]));
    let build_batch = |ts: Vec<i64>| {
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ts.clone())) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("x"); ts.len()])),
            ],
        )
        .unwrap()
    };

    // column-driven pushes across batches
    let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
    let batch = build_batch(vec![500, 300]);
    writer
        .push_batch_with_source(&batch, &synthesize_source_for_test(&batch), None)
        .unwrap();
    let batch = build_batch(vec![900, 100]);
    writer
        .push_batch_with_source(&batch, &synthesize_source_for_test(&batch), None)
        .unwrap();
    let (_, __index, stats) = writer.finish_with_stats().unwrap();
    assert_eq!((stats.min_ts, stats.max_ts), (100, 900));

    // source-driven push
    let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
    let source = StringArray::from_iter_values([
        r#"{"_timestamp":42,"svc":"a"}"#,
        r#"{"_timestamp":7,"svc":"b"}"#,
    ]);
    writer
        .push_docs_rows(&Int64Array::from(vec![42, 7]), &[], &source, None)
        .unwrap();
    let (_, __index, stats) = writer.finish_with_stats().unwrap();
    assert_eq!((stats.min_ts, stats.max_ts), (7, 42));

    // empty file: 0/0
    let writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
    let (_, __index, stats) = writer.finish_with_stats().unwrap();
    assert_eq!((stats.min_ts, stats.max_ts), (0, 0));
}

/// HARD guard (live zero-min_ts regression): a NON-EMPTY file whose stored
/// `_timestamp` range is degenerate (any zero/negative row) must refuse to
/// finish — an error, not a warning — so a corrupt FileMeta can never reach
/// the file_list DB. Empty files stay buildable (0/0 is their legit range).
#[test]
fn writer_finish_refuses_degenerate_timestamp_range() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("svc", DataType::Utf8, true),
    ]));

    // a literal zero timestamp (the live shape: lossy upstream coercion)
    let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
    let source = StringArray::from_iter_values([
        r#"{"_timestamp":1700000000000000,"svc":"a"}"#,
        r#"{"_timestamp":0,"svc":"b"}"#,
    ]);
    writer
        .push_docs_rows(
            &Int64Array::from(vec![1_700_000_000_000_000, 0]),
            &[],
            &source,
            None,
        )
        .unwrap();
    let err = writer
        .finish()
        .expect_err("zero timestamp row must refuse to finish");
    assert!(
        err.to_string().contains("degenerate _timestamp range"),
        "unexpected error: {err}"
    );

    // a negative timestamp is equally corrupt
    let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
    let source = StringArray::from_iter_values([r#"{"_timestamp":-5,"svc":"a"}"#]);
    writer
        .push_docs_rows(&Int64Array::from(vec![-5]), &[], &source, None)
        .unwrap();
    let err = writer
        .finish_with_stats()
        .expect_err("negative timestamp row must refuse to finish");
    assert!(
        err.to_string().contains("degenerate _timestamp range"),
        "unexpected error: {err}"
    );
}

/// The test-support fabrication escape: `finish_ignoring_timestamp_guard`
/// builds the file the guard above refuses — the pre-guard-era shape whose
/// stored rows carry `_timestamp <= 0` — and the result stays a perfectly
/// readable `.vix`. Downstream compaction-cleansing tests depend on this to
/// construct poisoned merge inputs; production writers never take this path.
#[test]
fn test_support_unguarded_finish_fabricates_zero_ts_file() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("svc", DataType::Utf8, true),
    ]));
    let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
    let source = StringArray::from_iter_values([
        r#"{"_timestamp":1700000000000000,"svc":"a"}"#,
        r#"{"_timestamp":0,"svc":"b"}"#,
        r#"{"_timestamp":-5,"svc":"c"}"#,
    ]);
    writer
        .push_docs_rows(
            &Int64Array::from(vec![1_700_000_000_000_000, 0, -5]),
            &[],
            &source,
            None,
        )
        .unwrap();
    let (data, index) = crate::test_support::finish_ignoring_timestamp_guard(writer).unwrap();
    let reader =
        VixReader::open_with_index(bytes::Bytes::from(data), index.map(bytes::Bytes::from))
            .unwrap();
    assert_eq!(reader.row_count(), 3);
    let stored = reader.read_docs_column("_timestamp").unwrap();
    let stored = stored
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec();
    assert_eq!(stored, vec![1_700_000_000_000_000, 0, -5]);
}

/// Dictionary-growth watchpoint (WORKLOG): high-cardinality numeric fields
/// add one tagged canonical term per distinct value. Measure the marginal
/// index bytes per unique float term and keep them bounded.
#[test]
fn numeric_term_dictionary_growth_is_bounded() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("credit", DataType::Float64, true),
    ]));
    let rows = 20_000usize;
    let build = |with_values: bool| {
        let ts: Vec<i64> = (1..=rows as i64).rev().collect();
        let values: Vec<Option<f64>> = (0..rows)
            .map(|row| with_values.then_some(row as f64 + 0.25))
            .collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ts)) as ArrayRef,
                Arc::new(arrow::array::Float64Array::from(values)),
            ],
        )
        .unwrap();
        let source = synthesize_source_for_test(&batch);
        let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
        writer
            .push_batch_with_source(&batch, &source, None)
            .unwrap();
        let (_, __index, stats) = writer.finish_with_stats().unwrap();
        stats
    };
    let with = build(true);
    let without = build(false);
    let per_term = (with.index_size.saturating_sub(without.index_size)) as f64 / rows as f64;
    println!(
        "numeric dictionary growth: {} unique float terms, index {} -> {} bytes, {per_term:.1} B/term",
        rows, without.index_size, with.index_size
    );
    assert!(
        per_term < 64.0,
        "expected bounded per-term growth, measured {per_term:.1} B/term"
    );
}

/// Per-chunk `_timestamp` zone table (DESIGN §2/§6): roundtrip, the
/// zone-map `timestamp_range` fast path vs the decode path, and re-derivation
/// at merge.
#[cfg(test)]
mod zone_map_tests {
    use super::*;
    use crate::{DocIdMap, test_support::strip_zone_map_property};

    /// Build a `.vix` file over `ts` with `svc` as a column-store field, with
    /// `docs_chunk_bytes = 1` so the zone table blocks at the 64-row floor —
    /// several entries for the multi-row datasets here. Zones are derived from
    /// the stored `_timestamp` values, so the tiny budget shapes the zone
    /// granularity without needing large rows.
    fn build_ts_file(ts: &[i64]) -> (Vec<u8>, Option<Vec<u8>>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("svc", DataType::Utf8, true),
        ]));
        let opts = VixWriterOptions {
            docs_chunk_bytes: 1, // -> 64 rows/chunk (the row floor)
            ..Default::default()
        };
        let svc_vals = ["api", "auth", "db", "web"];
        let svc: Vec<Option<&str>> = (0..ts.len()).map(|i| Some(svc_vals[i % 4])).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ts.to_vec())) as ArrayRef,
                Arc::new(StringArray::from(svc)),
            ],
        )
        .unwrap();
        let sources: Vec<String> = (0..ts.len())
            .map(|i| format!(r#"{{"_timestamp":{},"svc":"{}"}}"#, ts[i], svc_vals[i % 4]))
            .collect();
        let sources = StringArray::from_iter_values(sources.iter().map(String::as_str));
        let mut writer = VixWriter::new(&schema, opts, false);
        writer
            .push_batch_with_source(&batch, &sources, None)
            .unwrap();
        writer.finish().unwrap()
    }

    /// The rows in `[min, max)`, computed directly from the timestamp vector.
    fn brute_range(ts: &[i64], min: i64, max: i64) -> BTreeSet<u32> {
        ts.iter()
            .enumerate()
            .filter(|&(_, &t)| t >= min && t < max)
            .map(|(i, _)| i as u32)
            .collect()
    }

    /// A spread of ranges over a `[lo, hi]` timestamp span: fully-out on both
    /// ends, fully-in, empty, single-point, and every interior cut.
    fn probe_ranges(lo: i64, hi: i64) -> Vec<(i64, i64)> {
        let mut ranges = vec![
            (lo - 100, lo - 50), // fully below
            (hi + 50, hi + 100), // fully above
            (lo, hi + 1),        // fully covering
            (lo, lo),            // empty
            (hi, hi + 1),        // last point
            (lo, lo + 1),        // first point
            (i64::MIN, i64::MAX),
        ];
        // interior cuts at every step
        for cut in (lo..=hi).step_by(((hi - lo).max(1) / 13).max(1) as usize) {
            ranges.push((lo, cut));
            ranges.push((cut, hi + 1));
            ranges.push((cut - 3, cut + 3));
        }
        ranges
    }

    /// Roundtrip: the zone table covers the file contiguously and each entry's
    /// `(row_count, ts_min, ts_max)` equals the true stats of its row range.
    #[test]
    fn zone_map_roundtrip_matches_chunk_stats() {
        let ts: Vec<i64> = (0..300).map(|i| 5000 - i).collect(); // sorted DESC
        let reader = {
            let (data, index) = build_ts_file(&ts);
            open_built(data, index)
        };
        let chunks = reader
            .zone_chunks()
            .expect("a freshly written file carries a zone table")
            .to_vec();
        assert!(
            chunks.len() >= 2,
            "test needs several chunks, got {}",
            chunks.len()
        );

        // full `_timestamp` column, for the per-range reference stats
        let column = reader.read_docs_column("_timestamp").unwrap();
        let column = arrow::compute::cast(&column, &DataType::Int64).unwrap();
        let values = column.as_any().downcast_ref::<Int64Array>().unwrap();

        let mut offset = 0u64;
        for (i, chunk) in chunks.iter().enumerate() {
            assert_eq!(chunk.row_offset, offset, "chunk {i} offset");
            assert!(chunk.row_count > 0);
            let range = chunk.row_offset as usize..(chunk.row_offset + chunk.row_count) as usize;
            let slice: Vec<i64> = range.clone().map(|r| values.value(r)).collect();
            assert_eq!(
                chunk.ts_min,
                *slice.iter().min().unwrap(),
                "chunk {i} ts_min"
            );
            assert_eq!(
                chunk.ts_max,
                *slice.iter().max().unwrap(),
                "chunk {i} ts_max"
            );
            offset += chunk.row_count;
        }
        assert_eq!(offset, reader.row_count(), "zone table covers every row");
    }

    /// Differential: for sorted, piecewise-sorted and adversarial
    /// boundary-straddling timestamp distributions, the zone-map
    /// `timestamp_range` equals both the decode path (a zone-stripped reader)
    /// and the brute-force reference — across many ranges incl. exact
    /// chunk-boundary cuts.
    #[test]
    fn zone_map_timestamp_range_matches_decode_path() {
        let sorted: Vec<i64> = (0..280).map(|i| 10_000 + i).collect();
        let piecewise: Vec<i64> = (0..140)
            .map(|i| 10_000 + i)
            .chain((0..140).map(|i| 10_050 + i)) // two overlapping sorted runs
            .collect();
        let mut lcg = 0xDEAD_BEEF_1234_5678u64;
        let adversarial: Vec<i64> = (0..280)
            .map(|_| {
                lcg = lcg.wrapping_mul(6364136223846793005).wrapping_add(1);
                10_000 + ((lcg >> 40) % 300) as i64 // heavy value repetition, chunks straddle
            })
            .collect();

        for (name, ts) in [
            ("sorted", &sorted),
            ("piecewise", &piecewise),
            ("adversarial", &adversarial),
        ] {
            let (bytes, bytes_index) = build_ts_file(ts);
            let zoned = open_built(bytes.clone(), bytes_index.clone());
            let decode = open_built(
                strip_zone_map_property(&bytes).unwrap(),
                bytes_index.clone(),
            );
            assert!(
                zoned.zone_chunks().is_some(),
                "{name}: expected a zone table"
            );
            assert!(
                decode.zone_chunks().is_none(),
                "{name}: stripped reader must use the decode path"
            );
            let lo = *ts.iter().min().unwrap();
            let hi = *ts.iter().max().unwrap();
            for (min, max) in probe_ranges(lo, hi) {
                let want = brute_range(ts, min, max);
                assert_eq!(
                    bits_to_set(&zoned.timestamp_range(min, max).unwrap()),
                    want,
                    "{name}: zoned timestamp_range [{min},{max})"
                );
                assert_eq!(
                    bits_to_set(&decode.timestamp_range(min, max).unwrap()),
                    want,
                    "{name}: decode timestamp_range [{min},{max})"
                );
            }
        }
    }

    /// The zone table is re-derived when a merged file re-encodes its docs
    /// blob — the fast index merge (disjoint offset maps) and a rebuild both
    /// produce a table matching the merged rows.
    #[test]
    fn zone_map_rederived_at_merge() {
        let schema = docs_dataset_schema();
        let opts = dataset_options();
        // two disjoint runs (input 0 newer than input 1), enough rows + noise
        // to keep several chunks in the merged blob
        let make = |base: i64| -> (RecordBatch, StringArray) {
            let n = 200usize;
            let ts: Vec<i64> = (0..n as i64).map(|i| base - i).collect();
            let level: Vec<Option<&str>> = (0..n).map(|i| Some(["info", "warn"][i % 2])).collect();
            let log: Vec<Option<&str>> = (0..n).map(|_| Some("msg")).collect();
            let svc: Vec<&str> = (0..n).map(|i| ["api", "db"][i % 2]).collect();
            let code: Vec<Option<i64>> = (0..n).map(|i| Some(i as i64)).collect();
            let batch = docs_dataset_batch(&schema, ts, level, log, svc, code);
            let source = synthesize_source_for_test(&batch);
            (batch, source)
        };
        let (b0, s0) = make(100_000);
        let (b1, s1) = make(50_000); // strictly older -> disjoint, offset maps
        let build = |b: &RecordBatch, s: &StringArray| -> VixReader {
            let mut w = VixWriter::new(&schema, opts.clone(), false);
            w.push_batch_with_source(b, s, None).unwrap();
            finish_open(w)
        };
        let r0 = build(&b0, &s0);
        let r1 = build(&b1, &s1);
        let refs = [&r0, &r1];

        // merged rows = input 0 then input 1 (contiguous offset maps)
        let mut merged = VixWriter::new(&schema, opts.clone(), false);
        merged.check_merge_inputs(&refs).unwrap();
        let doc_maps = [DocIdMap::Offset(0), DocIdMap::Offset(b0.num_rows() as u32)];
        merged.merge_input_indexes(&refs, &doc_maps, 2).unwrap();
        for (b, s) in [(&b0, &s0), (&b1, &s1)] {
            merged
                .push_docs_rows_unindexed(
                    &timestamps_of(b),
                    &cs_columns_of(b, &["svc", "code"]),
                    s,
                    None,
                )
                .unwrap();
        }
        let merged_reader = finish_open(merged);

        // the merged file carries a re-derived, correct zone table
        let chunks = merged_reader
            .zone_chunks()
            .expect("merged file re-derives the zone table");
        let total: u64 = chunks.iter().map(|c| c.row_count).sum();
        assert_eq!(total, merged_reader.row_count());
        let column = merged_reader.read_docs_column("_timestamp").unwrap();
        let column = arrow::compute::cast(&column, &DataType::Int64).unwrap();
        let values = column.as_any().downcast_ref::<Int64Array>().unwrap();
        let mut offset = 0u64;
        for chunk in chunks {
            assert_eq!(chunk.row_offset, offset);
            let range = offset as usize..(offset + chunk.row_count) as usize;
            let slice: Vec<i64> = range.map(|r| values.value(r)).collect();
            assert_eq!(chunk.ts_min, *slice.iter().min().unwrap());
            assert_eq!(chunk.ts_max, *slice.iter().max().unwrap());
            offset += chunk.row_count;
        }

        // and the zone-map timestamp_range agrees with the merged rows
        let all_ts: Vec<i64> = (0..values.len()).map(|r| values.value(r)).collect();
        for (min, max) in [(60_000, 90_000), (0, 200_000), (49_000, 49_500)] {
            assert_eq!(
                bits_to_set(&merged_reader.timestamp_range(min, max).unwrap()),
                brute_range(&all_ts, min, max),
                "merged timestamp_range [{min},{max})"
            );
        }
    }

    /// An empty file writes no zone table (no chunks) and still answers an
    /// empty `timestamp_range`.
    #[test]
    fn zone_map_absent_on_empty_file() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("svc", DataType::Utf8, true),
        ]));
        let opts = VixWriterOptions {
            ..Default::default()
        };
        let writer = VixWriter::new(&schema, opts, false);
        let reader = finish_open(writer);
        assert_eq!(reader.row_count(), 0);
        assert!(reader.zone_chunks().is_none());
        assert_eq!(reader.timestamp_range(0, 100).unwrap().len(), 0);
    }
}

/// Ad-hoc probe for a downloaded production file: dump `_source` integrity
/// stats and the row at VIX_PROBE_TS. Run:
/// VIX_PROBE_FILE=... VIX_PROBE_TS=... cargo test -p vortex_index probe_file_source_integrity --
/// --ignored --nocapture
#[test]
#[ignore = "ad-hoc production-file probe; set VIX_PROBE_FILE"]
fn probe_file_source_integrity() {
    let path = std::env::var("VIX_PROBE_FILE").expect("set VIX_PROBE_FILE");
    let bytes = std::fs::read(&path).unwrap();
    // data-only open: `_source`/`_timestamp` integrity needs no sidecar
    let reader = VixReader::open(Bytes::from(bytes)).unwrap();
    let ts = as_i64_array(reader.read_docs_column("_timestamp").unwrap().as_ref());
    let src_col = reader.read_docs_column("_source").unwrap();
    let src = as_string_array(src_col.as_ref());
    let n = ts.len();
    let (mut nulls, mut empties, mut tiny) = (0usize, 0usize, 0usize);
    for i in 0..n {
        if src.is_null(i) {
            nulls += 1;
        } else if src.value(i).is_empty() {
            empties += 1;
        } else if src.value(i).len() < 40 {
            tiny += 1;
        }
    }
    println!("rows {n}, null _source {nulls}, empty {empties}, tiny(<40B) {tiny}");
    if let Ok(t) = std::env::var("VIX_PROBE_TS") {
        let target: i64 = t.parse().unwrap();
        for i in 0..n {
            if ts.value(i) == target {
                if src.is_null(i) {
                    println!("ts {target} row {i}: _source NULL");
                } else {
                    let v = src.value(i);
                    println!(
                        "ts {target} row {i}: _source len {} head: {}",
                        v.len(),
                        v.chars().take(200).collect::<String>()
                    );
                }
            }
        }
    }
    for i in [0usize, n / 2, n.saturating_sub(1)] {
        let s = if src.is_null(i) {
            "<NULL>".to_string()
        } else {
            src.value(i).chars().take(120).collect()
        };
        println!("sample row {i} ts {}: {s}", ts.value(i));
    }
    // validate every _source parses as JSON; dump any offender
    let mut invalid = 0usize;
    for i in 0..n {
        if src.is_null(i) {
            continue;
        }
        if let Err(e) = serde_json::from_str::<serde_json::Value>(src.value(i)) {
            invalid += 1;
            if invalid <= 3 {
                let v = src.value(i);
                println!(
                    "INVALID JSON row {i} ts {}: err {e}; len {}; bytes around error: {:?}",
                    ts.value(i),
                    v.len(),
                    &v.as_bytes()[v.len().saturating_sub(80)..]
                );
            }
        }
    }
    println!("invalid-json _source rows: {invalid}");
}

/// Ad-hoc: replicate the query-side SELECTION point-read against a
/// production file — the exact opener sequence (VixDocs + rows selection +
/// ts_range + projection) that serves SimpleSelect winners.
/// VIX_PROBE_FILE=... VIX_PROBE_TS=... VIX_PROBE_ROW=3 cargo test -p \
/// vortex_index probe_selection_point_read -- --ignored --nocapture
#[test]
#[ignore = "ad-hoc production-file probe; set VIX_PROBE_FILE"]
fn probe_selection_point_read() {
    let path = std::env::var("VIX_PROBE_FILE").expect("set VIX_PROBE_FILE");
    let target_ts: i64 = std::env::var("VIX_PROBE_TS").unwrap().parse().unwrap();
    let row: u64 = std::env::var("VIX_PROBE_ROW")
        .unwrap_or("3".into())
        .parse()
        .unwrap();
    let data = std::fs::read(&path).unwrap();
    let projection = vec!["_timestamp".to_string(), "_source".to_string()];
    let window = Some((target_ts - 1_000_000, target_ts + 1_000_000));

    for (label, rows, ts_range) in [
        ("rows-only", Some(vec![row]), None),
        ("rows+tsrange", Some(vec![row]), window),
        ("tsrange-only", None, window),
    ] {
        struct ProbeSource(Bytes);
        impl crate::VixRangeSource for ProbeSource {
            fn len(&self) -> u64 {
                self.0.len() as u64
            }
            fn fetch(
                &self,
                range: std::ops::Range<u64>,
            ) -> futures::future::BoxFuture<'static, anyhow::Result<Bytes>> {
                let out = self.0.slice(range.start as usize..range.end as usize);
                Box::pin(async move { Ok(out) })
            }
        }
        let ranged = std::env::var("VIX_PROBE_RANGED").is_ok();
        let docs = if ranged {
            crate::VixDocs::open_ranged(std::sync::Arc::new(ProbeSource(Bytes::from(data.clone()))))
                .unwrap()
        } else {
            crate::VixDocs::open(Bytes::from(data.clone())).unwrap()
        };
        let batches = docs
            .read_docs(Some(&projection), rows.clone(), ts_range)
            .unwrap();
        let mut total = 0usize;
        for b in &batches {
            let ts = as_i64_array(b.column(0).as_ref());
            let src_cast =
                arrow::compute::cast(b.column(1), &arrow::datatypes::DataType::Utf8).unwrap();
            let src = as_string_array(src_cast.as_ref());
            for i in 0..b.num_rows() {
                total += 1;
                let len = if src.is_null(i) {
                    usize::MAX
                } else {
                    src.value(i).len()
                };
                let show = if src.is_null(i) {
                    "<NULL>"
                } else {
                    &src.value(i)[..src.value(i).len().min(60)]
                };
                if ts.value(i) == target_ts || src.is_null(i) || len < 40 {
                    println!(
                        "[{label}] ts {} src_len {} head {}",
                        ts.value(i),
                        if len == usize::MAX { -1i64 } else { len as i64 },
                        show
                    );
                }
            }
        }
        println!("[{label}] rows returned {total}");
    }
}

/// End-to-end per-file value blooms, normal build path: configured fields
/// get a `bloom` blob as a byproduct of term emission; unknown fields are
/// ignored; unconfigured writers emit no blob.
#[test]
fn file_blooms_written_and_probeable() {
    use crate::sbbf::{BLOCK_BYTES, block_index, check_block, hash_value};

    let reader = build_dataset(VixWriterOptions {
        bloom_field_names: vec!["level".to_string(), "no_such_field".to_string()],
        ..dataset_options()
    });
    assert!(reader.has_file_blooms());
    let blooms = reader.file_blooms().unwrap().unwrap();
    assert_eq!(
        blooms.len(),
        1,
        "only fields present in the file get blooms"
    );
    let b = &blooms[0];
    assert_eq!(b.field, "level");
    // distinct raw `level` values across the fixture: info, error, warn
    assert_eq!(b.n_items, 3);
    assert!(b.num_blocks.is_power_of_two());
    for v in ["info", "error", "warn"] {
        let h = hash_value(v.as_bytes());
        let bi = block_index(h, b.num_blocks) as usize;
        let block: &[u8; BLOCK_BYTES] = b.bytes[bi * BLOCK_BYTES..(bi + 1) * BLOCK_BYTES]
            .try_into()
            .unwrap();
        assert!(check_block(block, h), "inserted level {v} missing");
    }
    // misses overwhelmingly rejected even on a tiny filter
    let mut fp = 0;
    for i in 0..200 {
        let v = format!("absent-{i}");
        let h = hash_value(v.as_bytes());
        let bi = block_index(h, b.num_blocks) as usize;
        let block: &[u8; BLOCK_BYTES] = b.bytes[bi * BLOCK_BYTES..(bi + 1) * BLOCK_BYTES]
            .try_into()
            .unwrap();
        if check_block(block, h) {
            fp += 1;
        }
    }
    assert!(fp < 10, "false-positive storm on misses: {fp}/200");

    // unconfigured writer: no blob at all
    let reader = build_dataset(dataset_options());
    assert!(!reader.has_file_blooms());
    assert!(reader.file_blooms().unwrap().is_none());
}

/// End-to-end per-file value blooms, merge path: the k-way workers stream
/// the deduplicated output terms, so the MERGED file carries a bloom over
/// the union of the inputs' distinct values.
#[test]
fn file_blooms_survive_index_merge() {
    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    use crate::{
        DocIdMap,
        sbbf::{BLOCK_BYTES, block_index, check_block, hash_value},
    };

    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("tid", DataType::Utf8, true),
    ]));
    let opts = VixWriterOptions {
        bloom_field_names: vec!["tid".to_string()],
        ..Default::default()
    };
    let build_input = |base_ts: i64, prefix: &str| -> VixReader {
        let mut writer = VixWriter::new(&schema, opts.clone(), false);
        let ts: Vec<i64> = (0..10).map(|i| base_ts - i).collect();
        let tids: Vec<String> = (0..10).map(|i| format!("{prefix}-{i}")).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ts.clone())),
                Arc::new(StringArray::from(
                    tids.iter().map(|s| Some(s.as_str())).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        let sources: Vec<String> = ts
            .iter()
            .zip(&tids)
            .map(|(t, tid)| format!(r#"{{"_timestamp":{t},"tid":"{tid}"}}"#))
            .collect();
        writer
            .push_batch_with_source(&batch, &StringArray::from(sources), None)
            .unwrap();
        finish_open(writer)
    };

    let newer = build_input(2000, "a");
    let older = build_input(1000, "b");
    let refs = [&newer, &older];
    let doc_maps = [DocIdMap::Offset(0), DocIdMap::Offset(10)];

    let mut merged = VixWriter::new(&schema, opts, false);
    merged.check_merge_inputs(&refs).unwrap();
    merged.merge_input_indexes(&refs, &doc_maps, 2).unwrap();
    let merged_ts = Int64Array::from((0..20).map(|i| 2000 - i).collect::<Vec<i64>>());
    let merged_source = StringArray::from(
        (0..20)
            .map(|i| format!(r#"{{"_timestamp":{},"row":{i}}}"#, 2000 - i))
            .collect::<Vec<String>>(),
    );
    merged
        .push_docs_rows_unindexed(&merged_ts, &[], &merged_source, None)
        .unwrap();
    let merged_reader = finish_open(merged);

    let blooms = merged_reader.file_blooms().unwrap().unwrap();
    assert_eq!(blooms.len(), 1);
    let b = &blooms[0];
    assert_eq!(b.field, "tid");
    assert_eq!(b.n_items, 20, "union of both inputs' distinct tids");
    for prefix in ["a", "b"] {
        for i in 0..10 {
            let v = format!("{prefix}-{i}");
            let h = hash_value(v.as_bytes());
            let bi = block_index(h, b.num_blocks) as usize;
            let block: &[u8; BLOCK_BYTES] = b.bytes[bi * BLOCK_BYTES..(bi + 1) * BLOCK_BYTES]
                .try_into()
                .unwrap();
            assert!(check_block(block, h), "merged bloom missing {v}");
        }
    }
}

// ---- field-major key-form coverage --------------------------------------

fn small_cell_dataset_options() -> VixWriterOptions {
    VixWriterOptions {
        // tiny: the 10-doc dataset still cuts several field-aligned cells
        ..dataset_options()
    }
}

/// Bloom bits are PINNED to the v1 BYTE FORM (`{value}\x00{fid}`): the
/// per-file bloom a build emits is bit-identical to one accumulated from
/// v1-form keys built via `write_composite_term` directly — the group `.bf`
/// continuity contract.
#[test]
fn bloom_bits_pinned_to_v1_key_form() {
    let mut opts = small_cell_dataset_options();
    opts.bloom_field_names = vec!["svc".to_string()];
    let bloom_fpp = opts.bloom_fpp;
    let built = build_dataset(opts).file_blooms().unwrap().unwrap();

    // svc is term field id 2 (sorted names: level=0, log=1, svc=2); its
    // distinct values, ascending, as the term stream visits them.
    let mut acc = crate::bloom::BloomHashAcc::from_pairs([(2u16, "svc".to_string())]);
    let mut scratch = Vec::new();
    for value in ["api", "auth", "db", "web"] {
        crate::query::write_composite_term(&mut scratch, value.as_bytes(), 2);
        acc.observe(&scratch);
    }
    let expected = acc.build(bloom_fpp);

    assert_eq!(built.len(), 1);
    assert_eq!(expected.len(), 1);
    assert_eq!(built[0].field, "svc");
    assert_eq!(built[0].num_blocks, expected[0].num_blocks);
    assert_eq!(built[0].n_items, expected[0].n_items);
    assert_eq!(
        built[0].bytes, expected[0].bytes,
        "bloom bodies must be bit-identical to the v1-form accumulation"
    );
}

// ---- vortex pushdown levers (owner items 1-4, 2026-07-29) --------------

/// STAGE-A PROOF: the default writer persists file-level statistics —
/// column_stats returns exact numeric min/max with zero data reads. If
/// this test ever fails after a vortex upgrade, file-level pruning
/// silently degrades to "cannot prune" (correct but slow) — fix the
/// strategy, do not delete the test.
#[test]
fn docs_column_stats_from_footer() {
    let docs = crate::VixDocs::open(Bytes::from(build_docs_dataset_bytes(false).0)).unwrap();
    use crate::docs::NumScalar;
    let (min, max) = docs
        .column_stats("code")
        .unwrap()
        .expect("default writer must persist file statistics");
    // fixture codes are 1..=10 with two nulls; stats ignore nulls
    assert_eq!(min, NumScalar::I64(1));
    assert_eq!(max, NumScalar::I64(10));
    let (tmin, tmax) = docs.column_stats("_timestamp").unwrap().unwrap();
    assert_eq!(tmin, NumScalar::I64(1000));
    assert_eq!(tmax, NumScalar::I64(1009));
    // string column: no numeric stats — "cannot prune", not an error
    assert!(docs.column_stats("svc").unwrap().is_none());
    assert!(docs.column_stats("no_such_column").unwrap().is_none());
}

/// Pushed numeric bounds return exactly the rows a post-scan filter
/// keeps, across inclusive/exclusive shapes; limit stops the scan.
#[test]
fn docs_scan_bounds_and_limit() {
    use crate::docs::{BoundValue, ColumnBound};
    let docs = crate::VixDocs::open(Bytes::from(build_docs_dataset_bytes(false).0)).unwrap();
    let scan = |bounds: &[ColumnBound], limit: Option<u64>| -> Vec<i64> {
        let mut out = Vec::new();
        docs.scan_docs_opts(
            Some(&["code".to_string()]),
            None,
            None,
            bounds,
            limit,
            0,
            &mut |batch| {
                let col = batch
                    .column_by_name("code")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                out.extend(col.iter().flatten());
                Ok(())
            },
        )
        .unwrap();
        out
    };
    // 10 rows in PUSH order (the raw writer preserves it; ts-DESC sorting
    // happens in core_writer's plan) with two null codes flattened away
    let all = scan(&[], None);
    assert_eq!(all, vec![1, 2, 4, 5, 7, 8, 9, 10]);
    let bound = |min, max| ColumnBound {
        column: "code".to_string(),
        min,
        max,
    };
    // code > 4 (exclusive; null rows never match)
    let got = scan(&[bound(Some((BoundValue::I64(4), false)), None)], None);
    let want: Vec<i64> = all.iter().copied().filter(|c| *c > 4).collect();
    assert_eq!(got, want);
    // 2 <= code <= 8
    let got = scan(
        &[bound(
            Some((BoundValue::I64(2), true)),
            Some((BoundValue::I64(8), true)),
        )],
        None,
    );
    let want: Vec<i64> = all
        .iter()
        .copied()
        .filter(|c| (2..=8).contains(c))
        .collect();
    assert_eq!(got, want);
    // equality via min==max inclusive
    let got = scan(
        &[bound(
            Some((BoundValue::I64(7), true)),
            Some((BoundValue::I64(7), true)),
        )],
        None,
    );
    assert_eq!(got, vec![7]);
    // absent column: bound ignored, full result
    let got = scan(
        &[ColumnBound {
            column: "not_a_column".to_string(),
            min: Some((BoundValue::I64(1), true)),
            max: None,
        }],
        None,
    );
    assert_eq!(got, all);
    // limit counts ROWS, not non-null values: the first 3 rows carry
    // codes [1, 2, null]
    let got = scan(&[], Some(3));
    assert_eq!(got, vec![1, 2]);
    // parallel decode returns the same rows (threads > 1)
    let mut par = Vec::new();
    docs.scan_docs_opts(
        Some(&["code".to_string()]),
        None,
        None,
        &[],
        None,
        4,
        &mut |batch| {
            let col = batch
                .column_by_name("code")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            par.extend(col.iter().flatten());
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(par, all);
}

/// #40 index-off roundtrip: a writer built with `index_enabled: false`
/// produces a COLUMN-STORE-ONLY file — `index=none` stamped, no term/fts
/// entries, no partial fields (even for values that WOULD be oversize-partial
/// when indexed), zero index bytes — whose docs columns read back intact,
/// whose condition-free evals stay valid, and whose term-query evals ERROR
/// (per-file degradation to the scan branch, never a silent empty match).
#[test]
fn index_off_writer_roundtrip_column_store_only() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("big", DataType::Utf8, true),
        Field::new("code", DataType::Int64, true),
        Field::new("log", DataType::Utf8, true),
        Field::new("svc", DataType::Utf8, true),
    ]));
    // `big` row 0 exceeds max_raw_term_len: an INDEXED build skips its
    // term without degrade (2026-08-12); the index-off build has no term
    // plan at all — neither may taint
    let oversize = "x".repeat(64);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1004, 1003, 1002, 1001])),
        Arc::new(StringArray::from(vec![
            Some(oversize.as_str()),
            Some("short"),
            None,
            Some("tiny"),
        ])),
        Arc::new(Int64Array::from(vec![
            Some(500),
            Some(200),
            None,
            Some(404),
        ])),
        Arc::new(StringArray::from(vec![
            Some("error timeout db"),
            Some("all good"),
            Some("warn disk"),
            None,
        ])),
        Arc::new(StringArray::from(vec![
            Some("api"),
            Some("api"),
            Some("db"),
            Some("web"),
        ])),
    ];
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
    let sources = StringArray::from_iter_values((0..4).map(|i| format!("{{\"row\":{i}}}")));
    // every writer materializes EVERY schema field as a docs column (v2)
    let opts = |index_enabled: bool| VixWriterOptions {
        fts_field_names: vec!["log".to_string()],
        max_raw_term_len: 8,
        index_enabled,
        ..Default::default()
    };

    let mut writer = VixWriter::new(&schema, opts(false), false);
    writer
        .push_batch_with_source(&batch, &sources, None)
        .unwrap();
    let (bytes, bytes_index, stats) = writer.finish_with_stats().unwrap();

    assert_eq!(stats.row_count, 4);
    assert_eq!(stats.term_count, 0, "no terms of any kind");
    assert_eq!(stats.index_size, 0, "no dict/terms/bloom bytes");
    assert!(stats.docs_size > 0, "the docs blob is the whole payload");

    let reader = open_built(bytes, bytes_index);
    assert!(!reader.has_index(), "index=none must round-trip");
    assert_eq!(reader.term_count(), 0);
    assert!(
        reader.term_field_names().is_empty(),
        "no term field entries"
    );
    assert!(reader.fts_fields().is_empty(), "no fts field entries");
    assert!(
        reader.partial_fields().is_empty(),
        "an empty term plan can never mark partials — the oversize value is \
         simply not indexed, like everything else"
    );
    for field in ["big", "code", "log", "svc"] {
        assert!(
            !reader.has_term_capability(field),
            "{field}: no term capability on an index-off file"
        );
        assert!(
            reader.has_column_store_field(field),
            "{field}: every schema field must be a docs column"
        );
    }

    // docs columns read back intact (order preserved: pushed as-is)
    assert_eq!(
        as_i64_array(reader.read_docs_column("_timestamp").unwrap().as_ref())
            .values()
            .to_vec(),
        vec![1004, 1003, 1002, 1001]
    );
    let svc = as_string_array(reader.read_docs_column("svc").unwrap().as_ref());
    assert_eq!(
        svc.iter()
            .map(|v| v.map(str::to_string))
            .collect::<Vec<_>>(),
        vec![
            Some("api".to_string()),
            Some("api".to_string()),
            Some("db".to_string()),
            Some("web".to_string())
        ]
    );
    let code = as_i64_array(reader.read_docs_column("code").unwrap().as_ref());
    assert_eq!(
        code.iter().collect::<Vec<_>>(),
        vec![Some(500), Some(200), None, Some(404)]
    );

    // condition-free evals stay valid: All matches every row, exact count
    assert_eq!(eval_set(&reader, &VixQuery::All), docs(&[0, 1, 2, 3]));
    assert_eq!(reader.count(&VixQuery::All).unwrap(), 4);

    // ANY term-shaped eval errors — never an empty (row-dropping) result
    assert!(reader.eval(&exact("svc", "api")).is_err());
    assert!(reader.eval(&any_token("error")).is_err());
    assert!(reader.eval(&key_exists_query("svc")).is_err());
    assert!(reader.count(&exact("svc", "api")).is_err());

    // ... and the dictionary-absence probe stays void-by-construction:
    // key_term_exists finds nothing, which is exactly why readers must gate
    // on has_index() before trusting it (FieldCap guard in the query layer)
    assert!(!reader.key_term_exists("svc").unwrap());

    // control: the SAME data built with index_enabled: true opens as an
    // indexed file (absent property = legacy default). The oversize "big"
    // value skips its term WITHOUT degrading the field (2026-08-12), so
    // the indexed control is taint-free too — the builds differ only in
    // having an index at all
    let mut indexed = VixWriter::new(&schema, opts(true), false);
    indexed
        .push_batch_with_source(&batch, &sources, None)
        .unwrap();
    let indexed = finish_open(indexed);
    assert!(indexed.has_index());
    assert!(indexed.term_count() > 0);
    assert!(indexed.partial_fields().is_empty());
    assert_eq!(eval_set(&indexed, &exact("svc", "api")), docs(&[0, 1]));
    // the accepted miss: the skipped oversize literal finds nothing
    assert_eq!(eval_set(&indexed, &exact("big", &oversize)), docs(&[]));
}

/// Helper for the index-off roundtrip: a `KeyExists` query (IS NOT NULL
/// shape) — one of the condition shapes that bypass per-field capability.
fn key_exists_query(path: &str) -> VixQuery {
    VixQuery::KeyExists {
        path: path.to_string(),
    }
}

/// #51c: the encoded-run API refuses outright unless the writer was built
/// with `docs_passthrough` — a standard encoder would route pre-encoded
/// chunks through a pipeline that cannot account for them.
#[test]
fn encoded_run_requires_passthrough_writer() {
    let schema = Schema::new(vec![Field::new("_timestamp", DataType::Int64, false)]);
    let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
    let err = writer
        .begin_docs_encoded_run(
            1,
            1,
            1,
            &[(1, 1, 1)],
            &crate::SpliceableStats::default(),
            None,
        )
        .expect_err("passthrough-off writer must refuse encoded runs");
    assert!(
        err.to_string().contains("docs_passthrough"),
        "unexpected error: {err}"
    );

    // with the flag on but a BUILD-mode writer (no merged index), the push
    // mode check still refuses: encoded runs are a merge-storage path
    let mut writer = VixWriter::new(
        &schema,
        VixWriterOptions {
            docs_passthrough: true,
            ..Default::default()
        },
        false,
    );
    let err = writer
        .begin_docs_encoded_run(
            1,
            1,
            1,
            &[(1, 1, 1)],
            &crate::SpliceableStats::default(),
            None,
        )
        .expect_err("build-mode writer must refuse encoded runs");
    assert!(
        err.to_string().contains("merge_input_indexes"),
        "unexpected error: {err}"
    );

    // chunk pushes and run finishes without an open run refuse too
    let err = writer
        .finish_docs_encoded_run()
        .expect_err("no open run to finish");
    assert!(
        err.to_string().contains("without an open run"),
        "unexpected error: {err}"
    );
}

/// #51c heal: the index-only push API — the split that lets a heal rebuild
/// index rows whose stored form arrives as copied encoded chunks — enforces
/// its mode rules and the finish-time index/docs row equality.
#[test]
fn index_only_pushes_enforce_split_accounting() {
    let schema = Schema::new(vec![Field::new("_timestamp", DataType::Int64, false)]);
    let one_row = Int64Array::from(vec![1_000_000i64]);
    let one_source = StringArray::from(vec!["{}"]);

    // (1) requires a passthrough writer: without the passthrough encoder
    // there is no path for the rows' stored form
    let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
    let err = writer
        .push_docs_rows_index_only(&one_row, &[], &one_source, None)
        .expect_err("passthrough-off writer must refuse index-only pushes");
    assert!(
        err.to_string().contains("docs_passthrough"),
        "unexpected error: {err}"
    );

    // (2) requires an INDEXED writer: index-off derives no terms, so the
    // rows would simply vanish
    let mut writer = VixWriter::new(
        &schema,
        VixWriterOptions {
            docs_passthrough: true,
            index_enabled: false,
            ..Default::default()
        },
        false,
    );
    let err = writer
        .push_docs_rows_index_only(&one_row, &[], &one_source, None)
        .expect_err("index-off writer must refuse index-only pushes");
    assert!(
        err.to_string().contains("index-off"),
        "unexpected error: {err}"
    );

    // (3) once index-only rows arrived, coupled pushes are rejected (they
    // would fork the doc-id/row accounting) while the encoded-run API — the
    // split's docs-store side — opens even on this build-mode writer
    let mut writer = VixWriter::new(
        &schema,
        VixWriterOptions {
            docs_passthrough: true,
            ..Default::default()
        },
        false,
    );
    writer
        .push_docs_rows_index_only(&one_row, &[], &one_source, None)
        .expect("index-only push on a passthrough writer");
    let err = writer
        .push_docs_rows(&one_row, &[], &one_source, None)
        .expect_err("coupled push after an index-only push must be rejected");
    assert!(
        err.to_string().contains("index-only build mode"),
        "unexpected error: {err}"
    );
    writer
        .begin_docs_encoded_run(
            1,
            1_000_000,
            1_000_000,
            &[(1, 1_000_000, 1_000_000)],
            &crate::SpliceableStats::default(),
            None,
        )
        .expect("the encoded run is the index-only mode's docs store");
    let err = writer
        .finish_docs_encoded_run()
        .expect_err("the run still owes its declared row");
    assert!(
        err.to_string().contains("short 1 rows"),
        "unexpected error: {err}"
    );

    // (4) finish refuses an index that does not cover the stored rows
    // exactly (here: 1 row indexed, 0 stored)
    let mut writer = VixWriter::new(
        &schema,
        VixWriterOptions {
            docs_passthrough: true,
            ..Default::default()
        },
        false,
    );
    writer
        .push_docs_rows_index_only(&one_row, &[], &one_source, None)
        .expect("index-only push on a passthrough writer");
    let err = writer
        .finish()
        .expect_err("finish must refuse the index/docs row divergence");
    assert!(
        err.to_string().contains("misaddress the stored rows"),
        "unexpected error: {err}"
    );
}

/// #51c-c: the `row_order` property round-trips writer -> file -> reader —
/// every writer stamps it explicitly now ("ts_desc" by default, "concat"
/// under [`VixWriterOptions::concat_row_order`]) — and a file WITHOUT the
/// property (every historical file) reads as sorted: the exact
/// backward-compatibility contract (missing == ts_desc, unknown values are
/// the fail-safe Concat).
#[test]
fn row_order_property_roundtrip_and_historical_default() {
    let schema = Schema::new(vec![Field::new("_timestamp", DataType::Int64, false)]);
    let build = |concat: bool| -> (Vec<u8>, Option<Vec<u8>>) {
        let mut writer = VixWriter::new(
            &schema,
            VixWriterOptions {
                concat_row_order: concat,
                ..Default::default()
            },
            false,
        );
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int64Array::from(vec![2_000i64, 1_000, 3_000])) as ArrayRef],
        )
        .unwrap();
        let source = StringArray::from(vec!["{}", "{}", "{}"]);
        writer
            .push_batch_with_source(&batch, &source, None)
            .unwrap();
        writer.finish().unwrap()
    };

    let (sorted, sorted_index) = build(false);
    assert_eq!(
        crate::test_support::row_order_property(&sorted)
            .unwrap()
            .as_deref(),
        Some("ts_desc"),
        "default writers stamp the sorted order explicitly"
    );
    assert!(
        open_built(sorted.clone(), sorted_index.clone())
            .row_order()
            .is_ts_desc()
    );

    let (concat, concat_index) = build(true);
    assert_eq!(
        crate::test_support::row_order_property(&concat)
            .unwrap()
            .as_deref(),
        Some("concat"),
        "concat writers stamp row_order=concat"
    );
    assert_eq!(
        open_built(concat, concat_index).row_order(),
        crate::RowOrder::Concat
    );

    // the historical file: no row_order property at all -> sorted, and the
    // file opens and reads exactly as before
    let historical = crate::test_support::strip_row_order_property(&sorted).unwrap();
    assert_eq!(
        crate::test_support::row_order_property(&historical).unwrap(),
        None
    );
    let reader = open_built(historical, sorted_index);
    assert!(
        reader.row_order().is_ts_desc(),
        "missing property must read as the sorted historical default"
    );
    assert_eq!(reader.row_count(), 3);
}

/// #51c-c: `timestamp_range` over a CONCAT-order file — two DESC runs
/// back-to-back, zone windows small enough that the zone table is really
/// non-monotonic — must equal the per-row truth for windows that land
/// inside runs, straddle the run boundary, and cover everything: the zoned
/// fast path classifies each chunk independently, no global-monotonicity
/// assumption.
#[test]
fn timestamp_range_on_concat_order_file_matches_per_row_truth() {
    // run 1: [2000 .. 1901] DESC; run 2: [2050 .. 1951] DESC — concatenated
    // the file jumps UP at row 100 (1901 -> 2050)
    let ts: Vec<i64> = (0..100)
        .map(|i| 2000 - i)
        .chain((0..100).map(|i| 2050 - i))
        .collect();
    let schema = Schema::new(vec![Field::new("_timestamp", DataType::Int64, false)]);
    let mut writer = VixWriter::new(
        &schema,
        VixWriterOptions {
            concat_row_order: true,
            // 1-byte budget clamps to the 64-row chunk floor -> 4 zone
            // windows over 200 rows, non-monotonic across the run boundary
            docs_chunk_bytes: 1,
            ..Default::default()
        },
        false,
    );
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int64Array::from(ts.clone())) as ArrayRef],
    )
    .unwrap();
    let source = StringArray::from(vec!["{}"; 200]);
    writer
        .push_batch_with_source(&batch, &source, None)
        .unwrap();
    let reader = finish_open(writer);
    assert_eq!(reader.row_order(), crate::RowOrder::Concat);

    let zone = reader.zone_chunks().expect("zone table");
    assert!(zone.len() >= 3, "need several zone windows, got {zone:?}");
    assert!(
        zone.windows(2).any(|pair| pair[1].ts_max > pair[0].ts_min),
        "the zone table must be non-monotonic for this test to prove anything: {zone:?}"
    );

    for (min, max) in [
        (0i64, i64::MAX), // everything
        (1990, 2011),     // straddles the run boundary values
        (1901, 1902),     // exactly one row of run 1
        (2050, 2051),     // exactly one row of run 2
        (1951, 2001),     // dense overlap of both runs
        (3000, 4000),     // nothing
    ] {
        let got = bits_to_set(&reader.timestamp_range(min, max).unwrap());
        let expected: BTreeSet<u32> = ts
            .iter()
            .enumerate()
            .filter(|&(_, &t)| t >= min && t < max)
            .map(|(row, _)| row as u32)
            .collect();
        assert_eq!(got, expected, "range [{min}, {max}) over the concat file");
    }
}

/// §4 REGION table, decode path: a concat writer derives the desc-run
/// decomposition from the ACTUAL stored `_timestamp` values (a strict
/// increase vs the previous row starts a new region; equal timestamps
/// continue one), stamps `row_regions`, and readers expose it as validated
/// row ranges. A ts_desc writer never stamps the property (one region by
/// definition), and a concat file WITHOUT it reads as piecewise-unknown.
#[test]
fn row_regions_auto_detected_on_concat_decode_path() {
    let schema = Schema::new(vec![Field::new("_timestamp", DataType::Int64, false)]);
    let build = |concat: bool, ts: &[i64]| -> (Vec<u8>, Option<Vec<u8>>) {
        let mut writer = VixWriter::new(
            &schema,
            VixWriterOptions {
                concat_row_order: concat,
                // small zone windows so region boundaries cross chunk
                // boundaries in the 200-row shape below
                docs_chunk_bytes: 1,
                ..Default::default()
            },
            false,
        );
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int64Array::from(ts.to_vec())) as ArrayRef],
        )
        .unwrap();
        let source = StringArray::from(vec!["{}"; ts.len()]);
        writer
            .push_batch_with_source(&batch, &source, None)
            .unwrap();
        writer.finish().unwrap()
    };

    // two desc runs of 100 rows: the boundary is an increase (1901 -> 2050)
    let ts: Vec<i64> = (0..100)
        .map(|i| 2000 - i)
        .chain((0..100).map(|i| 2050 - i))
        .collect();
    let (concat, concat_index) = build(true, &ts);
    assert_eq!(
        crate::test_support::data_property(&concat, "row_regions")
            .unwrap()
            .as_deref(),
        Some("[100,100]"),
        "the decode path must derive the exact desc runs"
    );
    let reader = open_built(concat.clone(), concat_index.clone());
    assert_eq!(
        reader.ts_desc_row_ranges(),
        Some(vec![0..100, 100..200]),
        "reader exposes the validated region row ranges"
    );

    // EQUAL timestamps continue a run (non-increasing = still DESC); only a
    // strict increase splits — [5,5,4] then [7,7,7] = two regions
    let (eq, eq_index) = build(true, &[5, 5, 4, 7, 7, 7]);
    assert_eq!(
        crate::test_support::data_property(&eq, "row_regions")
            .unwrap()
            .as_deref(),
        Some("[3,3]")
    );
    assert_eq!(
        open_built(eq, eq_index).ts_desc_row_ranges(),
        Some(vec![0..3, 3..6])
    );

    // ts_desc writer: no property, one implicit full-file region
    let sorted_ts: Vec<i64> = (0..10).map(|i| 1000 - i).collect();
    let (sorted, sorted_index) = build(false, &sorted_ts);
    assert_eq!(
        crate::test_support::data_property(&sorted, "row_regions").unwrap(),
        None,
        "ts_desc files never stamp row_regions"
    );
    assert_eq!(
        open_built(sorted, sorted_index).ts_desc_row_ranges(),
        Some(vec![0..10])
    );

    // a concat file WITHOUT the property (pre-M4 concat output): piecewise
    // order unknown — readers must not assume anything
    let stripped = crate::test_support::strip_property_for_tests(&concat, "row_regions").unwrap();
    let reader = open_built(stripped, concat_index);
    assert_eq!(reader.row_order(), crate::RowOrder::Concat);
    assert_eq!(reader.ts_desc_row_ranges(), None);
}

/// §4 REGION table, malformed-property trust rules: a table that does not
/// sum to `row_count`, carries a zero-count region, or is not a JSON u64
/// array reads as ABSENT (fail-open), never as an error.
#[test]
fn row_regions_malformed_property_reads_as_absent() {
    let schema = Schema::new(vec![Field::new("_timestamp", DataType::Int64, false)]);
    let mut writer = VixWriter::new(
        &schema,
        VixWriterOptions {
            concat_row_order: true,
            ..Default::default()
        },
        false,
    );
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int64Array::from(vec![10i64, 5, 20, 15])) as ArrayRef],
    )
    .unwrap();
    let source = StringArray::from(vec!["{}"; 4]);
    writer
        .push_batch_with_source(&batch, &source, None)
        .unwrap();
    let (data, index) = writer.finish().unwrap();
    assert_eq!(
        crate::test_support::data_property(&data, "row_regions")
            .unwrap()
            .as_deref(),
        Some("[2,2]")
    );

    for bad in ["[2,3]", "[0,4]", "[]", "\"junk\"", "[2]"] {
        let tampered = crate::test_support::repack_properties(&data, |properties| {
            for (key, value) in properties.iter_mut() {
                if key == "row_regions" {
                    *value = bad.to_string();
                }
            }
            Ok(())
        })
        .unwrap();
        let reader = open_built(tampered, index.clone());
        assert_eq!(
            reader.ts_desc_row_ranges(),
            None,
            "row_regions {bad:?} must read as absent (fail-open)"
        );
    }
}

/// §4 REGION table, passthrough path: an encoded run splices the caller's
/// proven decomposition; a run WITHOUT one poisons the property (the copy
/// itself is unaffected); and a mis-summed decomposition is refused before
/// any chunk is accepted.
#[test]
fn row_regions_splice_through_encoded_runs() {
    let schema = Schema::new(vec![Field::new("_timestamp", DataType::Int64, false)]);
    // one ts_desc input file of 4 rows to copy chunks from
    let input = {
        let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int64Array::from(vec![4_000i64, 3_000, 2_000, 1_000])) as ArrayRef],
        )
        .unwrap();
        let source = StringArray::from(vec!["{}"; 4]);
        writer
            .push_batch_with_source(&batch, &source, None)
            .unwrap();
        let (data, _) = writer.finish().unwrap();
        crate::VixDocs::open(bytes::Bytes::from(data)).unwrap()
    };
    let splice_input = |writer: &mut VixWriter, regions: Option<&[u64]>| {
        let entries: Vec<crate::ZoneEntry> = input
            .zone_chunks()
            .unwrap()
            .iter()
            .map(|zone| (zone.row_count, zone.ts_min, zone.ts_max))
            .collect();
        let stats = input.spliceable_stats().unwrap().unwrap();
        writer.begin_docs_encoded_run(4, 1_000, 4_000, &entries, &stats, regions)?;
        input.scan_docs_encoded_chunks(&mut |chunk| writer.push_docs_encoded_chunk(chunk))?;
        writer.finish_docs_encoded_run()
    };
    let passthrough_writer = || {
        // the index-only pre-push moves the writer into the split mode that
        // accepts encoded runs without a full merge harness
        let mut writer = VixWriter::new(
            &schema,
            VixWriterOptions {
                docs_passthrough: true,
                concat_row_order: true,
                ..Default::default()
            },
            false,
        );
        let ts = Int64Array::from(vec![
            4_000i64, 3_000, 2_000, 1_000, 4_000, 3_000, 2_000, 1_000,
        ]);
        let source = StringArray::from(vec!["{}"; 8]);
        writer
            .push_docs_rows_index_only(&ts, &[], &source, None)
            .unwrap();
        writer
    };

    // two spliced runs, each declared as one desc run -> [4,4]
    let mut writer = passthrough_writer();
    splice_input(&mut writer, Some(&[4])).unwrap();
    splice_input(&mut writer, Some(&[4])).unwrap();
    let (data, _) = writer.finish().unwrap();
    assert_eq!(
        crate::test_support::data_property(&data, "row_regions")
            .unwrap()
            .as_deref(),
        Some("[4,4]"),
        "spliced decompositions concatenate"
    );

    // a run without a proven decomposition poisons the table
    let mut writer = passthrough_writer();
    splice_input(&mut writer, Some(&[4])).unwrap();
    splice_input(&mut writer, None).unwrap();
    let (data, _) = writer.finish().unwrap();
    assert_eq!(
        crate::test_support::data_property(&data, "row_regions").unwrap(),
        None,
        "an unproven run must poison the property (fail-open)"
    );

    // a decomposition that does not sum to the run is refused up front
    let mut writer = passthrough_writer();
    let err = splice_input(&mut writer, Some(&[3])).unwrap_err();
    assert!(
        err.to_string().contains("run_regions cover 3 rows"),
        "unexpected error: {err}"
    );
}

/// §4 chunk pruning (M4): build one 128-row file with two 64-row zone
/// chunks whose per-column stats separate cleanly, then pin
/// [`VixDocs::pruned_scan_ranges`]'s verdicts per tier: numeric range
/// pruning, zero-presence pruning of a sparse column, whole-file
/// exclusion, absent-column fail-open, and `_timestamp` zone pruning.
#[test]
fn chunk_stats_pruning_verdicts() {
    use crate::docs::{BoundValue, ColumnBound};
    let schema = Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("code", DataType::Int64, true),
        Field::new("svc", DataType::Utf8, true),
        Field::new("sparse", DataType::Int64, true),
    ]);
    let mut writer = VixWriter::new(
        &schema,
        VixWriterOptions {
            docs_chunk_bytes: 1, // 64-row chunk floor -> 2 chunks over 128 rows
            ..Default::default()
        },
        false,
    );
    // chunk 0 (rows 0..64): ts 1128..1065, code 0..63, svc "apple",
    //   sparse all NULL
    // chunk 1 (rows 64..128): ts 1064..1001, code 200..263, svc "zebra",
    //   sparse fully populated
    let ts: Vec<i64> = (0..128).map(|i| 1128 - i).collect();
    let code: Vec<Option<i64>> = (0..128)
        .map(|i| Some(if i < 64 { i } else { 200 + (i - 64) }))
        .collect();
    let svc: Vec<Option<&str>> = (0..128)
        .map(|i| Some(if i < 64 { "apple" } else { "zebra" }))
        .collect();
    let sparse: Vec<Option<i64>> = (0..128).map(|i| (i >= 64).then_some(7)).collect();
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int64Array::from(ts)) as ArrayRef,
            Arc::new(Int64Array::from(code)) as ArrayRef,
            Arc::new(StringArray::from(svc)) as ArrayRef,
            Arc::new(Int64Array::from(sparse)) as ArrayRef,
        ],
    )
    .unwrap();
    let source = StringArray::from(vec!["{}"; 128]);
    writer
        .push_batch_with_source(&batch, &source, None)
        .unwrap();
    let (data, _) = writer.finish().unwrap();
    let docs = crate::VixDocs::open(bytes::Bytes::from(data)).unwrap();
    assert_eq!(docs.zone_chunks().unwrap().len(), 2);

    let ge = |column: &str, value: BoundValue| ColumnBound {
        column: column.to_string(),
        min: Some((value, true)),
        max: None,
    };
    let eq = |column: &str, value: BoundValue| ColumnBound {
        column: column.to_string(),
        min: Some((value.clone(), true)),
        max: Some((value, true)),
    };

    // numeric range: code >= 150 excludes chunk 0
    assert_eq!(
        docs.pruned_scan_ranges(None, &[ge("code", BoundValue::I64(150))]),
        Some(vec![64..128])
    );
    // numeric range: code <= 100 excludes chunk 1
    assert_eq!(
        docs.pruned_scan_ranges(
            None,
            &[ColumnBound {
                column: "code".into(),
                min: None,
                max: Some((BoundValue::I64(100), true)),
            }]
        ),
        Some(vec![0..64])
    );
    // whole file provably empty: code > 1000
    assert_eq!(
        docs.pruned_scan_ranges(None, &[ge("code", BoundValue::I64(1001))]),
        Some(vec![])
    );
    // string bounds: svc = "mango" is between the chunks' values -> both
    // chunks excluded (apple-max < mango, zebra-min > mango)
    assert_eq!(
        docs.pruned_scan_ranges(None, &[eq("svc", BoundValue::Str("mango".into()))]),
        Some(vec![])
    );
    // svc = "apple": only chunk 0 survives
    assert_eq!(
        docs.pruned_scan_ranges(None, &[eq("svc", BoundValue::Str("apple".into()))]),
        Some(vec![0..64])
    );
    // zero-presence pruning: any null-rejecting bound on `sparse` excludes
    // chunk 0 (0 present values there), even without min/max logic
    assert_eq!(
        docs.pruned_scan_ranges(None, &[eq("sparse", BoundValue::I64(7))]),
        Some(vec![64..128])
    );
    // absent column: no basis, fail-open (scan everything)
    assert_eq!(
        docs.pruned_scan_ranges(None, &[eq("nope", BoundValue::I64(1))]),
        None
    );
    // cross-family bound (string bound on an i64 column): fail-open
    assert_eq!(
        docs.pruned_scan_ranges(None, &[eq("code", BoundValue::Str("5".into()))]),
        None
    );
    // `_timestamp` zone pruning: [1001, 1010) lives in chunk 1 only
    assert_eq!(
        docs.pruned_scan_ranges(Some((1001, 1010)), &[]),
        Some(vec![64..128])
    );
    // everything survives -> None (single full scan, no range overhead)
    assert_eq!(
        docs.pruned_scan_ranges(None, &[ge("code", BoundValue::I64(0))]),
        None
    );

    // and the SCAN respects the pruning without losing rows: bounds that
    // exclude chunk 0 return exactly chunk 1's matching rows
    let batches = {
        let mut out: Vec<RecordBatch> = Vec::new();
        docs.scan_docs_opts(
            Some(&["code".to_string()]),
            None,
            None,
            &[ge("code", BoundValue::I64(150))],
            None,
            0,
            &mut |batch| {
                out.push(batch);
                Ok(())
            },
        )
        .unwrap();
        out
    };
    let mut got: Vec<i64> = Vec::new();
    for batch in &batches {
        let code = batch.column_by_name("code").unwrap();
        let code = code.as_any().downcast_ref::<Int64Array>().unwrap();
        got.extend(code.iter().flatten());
    }
    assert_eq!(got, (200..264).collect::<Vec<i64>>());
}

/// M15 dict-aware equality filter-back: the string-equality pre-pass
/// resolves the needle against each chunk's DICTIONARY and scans code ids
/// (canonical decode + compare only for non-dict chunks); its row ids and
/// the end-to-end scan output must EXACTLY equal the per-row oracle on
/// dict-encoded, high-entropy and null-bearing columns, across thread
/// counts, with the broad-match fallback and limit/ts composition intact.
#[test]
fn m15_eq_scan_dict_aware_parity() {
    use vortex::array::arrays::Dict;

    use crate::docs::{BoundValue, ColumnBound};

    const ROWS: usize = 192; // 3 chunks x 64 rows at the chunk floor
    let schema = Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("svc", DataType::Utf8, true), // low-cardinality: dict
        Field::new("rid", DataType::Utf8, true), // per-row unique: non-dict
    ]);
    let ts: Vec<i64> = (0..ROWS as i64).map(|i| 10_000 - i).collect();
    // svc: 3 hot values + one single-occurrence needle + one empty-string
    // value + nulls
    let svc: Vec<Option<String>> = (0..ROWS)
        .map(|i| match i {
            100 => Some("needle-one".to_string()),
            140 => Some(String::new()),
            i if i % 16 == 15 => None,
            i => Some(format!("svc-{}", i % 3)),
        })
        .collect();
    // rid: unique per row, one deliberate duplicate pair (rows 37 and 150),
    // nulls sprinkled
    let rid: Vec<Option<String>> = (0..ROWS)
        .map(|i| match i {
            150 => Some("rid-0037".to_string()),
            i if i % 32 == 31 => None,
            i => Some(format!("rid-{i:04}")),
        })
        .collect();
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int64Array::from(ts.clone())) as ArrayRef,
            Arc::new(StringArray::from(
                svc.iter().map(|v| v.as_deref()).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(StringArray::from(
                rid.iter().map(|v| v.as_deref()).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .unwrap();
    let source = StringArray::from(vec!["{}"; ROWS]);
    let mut writer = VixWriter::new(
        &schema,
        VixWriterOptions {
            docs_chunk_bytes: 1, // 64-row chunk floor
            ..Default::default()
        },
        false,
    );
    writer
        .push_batch_with_source(&batch, &source, None)
        .unwrap();
    let (data, _) = writer.finish().unwrap();
    let docs = crate::VixDocs::open(Bytes::from(data)).unwrap();
    assert_eq!(docs.zone_chunks().unwrap().len(), 3);

    // the fixture must exercise the DICT arm: at least one stored chunk of
    // the file is dict-encoded (svc is the repetitive column vortex probes)
    let mut saw_dict = false;
    docs.scan_docs_encoded_chunks(&mut |chunk| {
        saw_dict |= chunk
            .array
            .depth_first_traversal()
            .any(|node| node.is::<Dict>());
        Ok(())
    })
    .unwrap();
    assert!(
        saw_dict,
        "fixture must keep a dict-encoded column or the dict arm loses coverage"
    );

    let eq = |column: &str, value: &str| ColumnBound {
        column: column.to_string(),
        min: Some((BoundValue::Str(value.to_string()), true)),
        max: Some((BoundValue::Str(value.to_string()), true)),
    };
    let oracle = |values: &[Option<String>], needle: &str| -> Vec<u64> {
        values
            .iter()
            .enumerate()
            .filter(|(_, v)| v.as_deref() == Some(needle))
            .map(|(i, _)| i as u64)
            .collect()
    };
    let prepass = |column: &str, needle: &str, threads: usize| {
        docs.eq_string_prepass(column, needle, None, &[eq(column, needle)], threads)
            .unwrap()
    };

    // needle-grade shapes: the pre-pass returns the EXACT oracle ids, at
    // every thread count (chunk-aligned splitting)
    for threads in [0usize, 1, 4, 7] {
        assert_eq!(
            prepass("svc", "needle-one", threads),
            Some(oracle(&svc, "needle-one")),
            "threads={threads}"
        );
        assert_eq!(
            prepass("rid", "rid-0037", threads),
            Some(oracle(&rid, "rid-0037")),
            "duplicate rid rows, threads={threads}"
        );
        assert_eq!(prepass("rid", "rid-0100", threads), Some(vec![100]));
        assert_eq!(
            prepass("svc", "", threads),
            Some(oracle(&svc, "")),
            "empty string is a real value, threads={threads}"
        );
        assert_eq!(
            prepass("svc", "absent-value", threads),
            Some(vec![]),
            "threads={threads}"
        );
    }
    // nulls never match (equality is null-rejecting): every oracle above
    // came from non-null values only, and the null rows are absent
    assert!(!prepass("svc", "needle-one", 4).unwrap().contains(&15));

    // broad match: svc-0 covers ~1/3 of rows -> the pre-pass declines
    assert_eq!(prepass("svc", "svc-0", 4), None);

    // end-to-end scan parity: emitted rows == oracle, for a needle (fast
    // path) AND a broad value (fallback path), with ts window and limit
    // composed
    let scan_rows = |column: &str,
                     needle: &str,
                     ts_range: Option<(i64, i64)>,
                     limit: Option<u64>|
     -> Vec<i64> {
        let mut got = Vec::new();
        docs.scan_docs_opts(
            Some(&["_timestamp".to_string(), column.to_string()]),
            None,
            ts_range,
            &[eq(column, needle)],
            limit,
            2,
            &mut |batch| {
                let ts = batch.column_by_name("_timestamp").unwrap();
                let ts = arrow::compute::cast(ts, &DataType::Int64).unwrap();
                let ts = ts.as_any().downcast_ref::<Int64Array>().unwrap();
                let col = batch.column_by_name(column).unwrap();
                let col = arrow::compute::cast(col, &DataType::Utf8).unwrap();
                let col = col.as_any().downcast_ref::<StringArray>().unwrap();
                for i in 0..batch.num_rows() {
                    // the eq bound is NOT a row filter for the fallback
                    // path (string bounds never are); apply it here the way
                    // the engine's FilterExec would
                    if !col.is_null(i) && col.value(i) == needle {
                        got.push(ts.value(i));
                    }
                }
                Ok(())
            },
        )
        .unwrap();
        got
    };
    let oracle_ts = |values: &[Option<String>], needle: &str, window: Option<(i64, i64)>| {
        oracle(values, needle)
            .into_iter()
            .map(|row| ts[row as usize])
            .filter(|t| window.is_none_or(|(lo, hi)| *t >= lo && *t < hi))
            .collect::<Vec<i64>>()
    };
    assert_eq!(
        scan_rows("svc", "needle-one", None, None),
        oracle_ts(&svc, "needle-one", None)
    );
    assert_eq!(
        scan_rows("rid", "rid-0037", None, None),
        oracle_ts(&rid, "rid-0037", None)
    );
    assert_eq!(
        scan_rows("svc", "svc-1", None, None),
        oracle_ts(&svc, "svc-1", None),
        "broad-match fallback parity"
    );
    // ts window straddling chunk 1: the pass-2 point read still applies the
    // vortex _timestamp filter to the matched rows
    let window = Some((9_800i64, 9_950i64));
    assert_eq!(
        scan_rows("svc", "needle-one", window, None),
        oracle_ts(&svc, "needle-one", window)
    );
    assert_eq!(
        scan_rows("rid", "rid-0037", window, None),
        oracle_ts(&rid, "rid-0037", window)
    );
    // limit: the first matching row only, in row order
    assert_eq!(
        scan_rows("rid", "rid-0037", None, Some(1)),
        oracle_ts(&rid, "rid-0037", None)[..1].to_vec()
    );
}

#[test]
fn equality_match_budget_bounds_rows_across_workers() {
    let budget = crate::container::EqMatchBudget::new(3);
    let mut first_worker = Vec::new();
    let mut second_worker = Vec::new();

    assert!(budget.try_push(1, &mut first_worker));
    assert!(budget.try_push(2, &mut second_worker));
    assert!(budget.try_push(3, &mut first_worker));
    assert!(!budget.try_push(4, &mut second_worker));
    assert!(budget.is_exceeded());
    assert!(!budget.try_push(5, &mut first_worker));
    assert_eq!(first_worker.len() + second_worker.len(), 3);
}

/// §4 string PREFIX bounds stay conservative: values longer than the
/// 32-byte stats prefix store a truncated min and a prefix-incremented max;
/// bounds that fall between a stored bound and the true value must KEEP the
/// chunk (admit, never reject, borderline values).
#[test]
fn chunk_stats_string_prefix_bounds_are_conservative() {
    use crate::docs::{BoundValue, ColumnBound};
    let long_value = format!("{}zzzzzzzz", "a".repeat(32)); // 40 bytes
    let schema = Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("s", DataType::Utf8, true),
    ]);
    let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int64Array::from(vec![1000i64, 999])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some(long_value.as_str()),
                Some(long_value.as_str()),
            ])) as ArrayRef,
        ],
    )
    .unwrap();
    let source = StringArray::from(vec!["{}"; 2]);
    writer
        .push_batch_with_source(&batch, &source, None)
        .unwrap();
    let (data, _) = writer.finish().unwrap();
    let docs = crate::VixDocs::open(bytes::Bytes::from(data)).unwrap();

    let eq = |value: &str| {
        vec![ColumnBound {
            column: "s".to_string(),
            min: Some((BoundValue::Str(value.to_string()), true)),
            max: Some((BoundValue::Str(value.to_string()), true)),
        }]
    };
    // the exact stored value: kept (obviously)
    assert_eq!(docs.pruned_scan_ranges(None, &eq(&long_value)), None);
    // a value between the stored PREFIX min ("a"*32) and the true value:
    // must be kept — the interval [prefix-min, incremented-max] covers it
    let between = format!("{}m", "a".repeat(32));
    assert_eq!(docs.pruned_scan_ranges(None, &eq(&between)), None);
    // the prefix itself (< true value, == stored min): kept, conservative
    assert_eq!(docs.pruned_scan_ranges(None, &eq(&"a".repeat(32))), None);
    // clearly outside the incremented upper bound: pruned
    assert_eq!(docs.pruned_scan_ranges(None, &eq("b")), Some(vec![]));
    // clearly below the prefix min: pruned
    assert_eq!(docs.pruned_scan_ranges(None, &eq("Zzz")), Some(vec![]));
}

/// §4 splice-parity on the READ side (§11's anti-goal is the v1 stats
/// loss): a PASSTHROUGH output — whose docs blob carries no vortex
/// statistics at all — prunes chunks/files through its spliced O2 stats
/// exactly like a first-encode file.
#[test]
fn chunk_stats_pruning_survives_passthrough() {
    use crate::docs::{BoundValue, ColumnBound};
    let schema = Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("code", DataType::Int64, true),
    ]);
    let build_input = |ts_hi: i64, code_base: i64| {
        let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
        let ts: Vec<i64> = (0..4).map(|i| ts_hi - i).collect();
        let code: Vec<i64> = (0..4).map(|i| code_base + i).collect();
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int64Array::from(ts)) as ArrayRef,
                Arc::new(Int64Array::from(code)) as ArrayRef,
            ],
        )
        .unwrap();
        let source = StringArray::from(vec!["{}"; 4]);
        writer
            .push_batch_with_source(&batch, &source, None)
            .unwrap();
        let (data, _) = writer.finish().unwrap();
        crate::VixDocs::open(bytes::Bytes::from(data)).unwrap()
    };
    // input A: ts 2000..1997 / code 0..3; input B: ts 1996..1993 / code 500..503
    let a = build_input(2000, 0);
    let b = build_input(1996, 500);

    let mut writer = VixWriter::new(
        &schema,
        VixWriterOptions {
            docs_passthrough: true,
            ..Default::default()
        },
        false,
    );
    let ts = Int64Array::from(vec![2000i64, 1999, 1998, 1997, 1996, 1995, 1994, 1993]);
    let source = StringArray::from(vec!["{}"; 8]);
    writer
        .push_docs_rows_index_only(&ts, &[], &source, None)
        .unwrap();
    for (input, (ts_min, ts_max)) in [(&a, (1997, 2000)), (&b, (1993, 1996))] {
        let entries: Vec<crate::ZoneEntry> = input
            .zone_chunks()
            .unwrap()
            .iter()
            .map(|zone| (zone.row_count, zone.ts_min, zone.ts_max))
            .collect();
        let stats = input.spliceable_stats().unwrap().unwrap();
        writer
            .begin_docs_encoded_run(4, ts_min, ts_max, &entries, &stats, Some(&[4]))
            .unwrap();
        input
            .scan_docs_encoded_chunks(&mut |chunk| writer.push_docs_encoded_chunk(chunk))
            .unwrap();
        writer.finish_docs_encoded_run().unwrap();
    }
    let (data, _) = writer.finish().unwrap();
    let merged = crate::VixDocs::open(bytes::Bytes::from(data)).unwrap();
    assert_eq!(merged.row_count(), 8);
    assert_eq!(merged.zone_chunks().unwrap().len(), 2, "spliced zone table");

    // the passthrough output has NO vortex file-level stats...
    assert_eq!(merged.column_stats("code").unwrap(), None);
    // ...but the spliced O2 stats prune exactly like a fresh encode:
    let ge = |value: i64| {
        vec![ColumnBound {
            column: "code".to_string(),
            min: Some((BoundValue::I64(value), true)),
            max: None,
        }]
    };
    assert_eq!(
        merged.pruned_scan_ranges(None, &ge(100)),
        Some(vec![4..8]),
        "input A's chunk pruned by the spliced stats"
    );
    assert_eq!(
        merged.pruned_scan_ranges(None, &ge(1000)),
        Some(vec![]),
        "whole passthrough file provably empty for code >= 1000"
    );
}

/// M4 exact cross-type comparator edges: i64/u64 vs f64 comparisons must
/// be EXACT — a lossy i64→f64 rounding could prune a chunk that matches.
#[test]
fn stats_bound_comparator_is_exact() {
    use std::cmp::Ordering;

    use crate::docs::{BoundValue, NumScalar, cmp_num_vs_bound};
    // 2^63 - 1 vs (2^63 - 1) as f64 (rounds UP to 2^63): the int is SMALLER
    assert_eq!(
        cmp_num_vs_bound(NumScalar::I64(i64::MAX), &BoundValue::F64(i64::MAX as f64)),
        Some(Ordering::Less)
    );
    // 2^53 + 1 vs 2^53 as f64: adjacent beyond float precision — must not
    // collapse to Equal
    let big = (1i64 << 53) + 1;
    assert_eq!(
        cmp_num_vs_bound(NumScalar::I64(big), &BoundValue::F64((1i64 << 53) as f64)),
        Some(Ordering::Greater)
    );
    // fractional bounds order strictly between adjacent ints
    assert_eq!(
        cmp_num_vs_bound(NumScalar::I64(5), &BoundValue::F64(5.5)),
        Some(Ordering::Less)
    );
    assert_eq!(
        cmp_num_vs_bound(NumScalar::I64(6), &BoundValue::F64(5.5)),
        Some(Ordering::Greater)
    );
    // negatives + fractional
    assert_eq!(
        cmp_num_vs_bound(NumScalar::I64(-6), &BoundValue::F64(-5.5)),
        Some(Ordering::Less)
    );
    // NaN bound: incomparable (keep the chunk)
    assert_eq!(
        cmp_num_vs_bound(NumScalar::F64(1.0), &BoundValue::F64(f64::NAN)),
        None
    );
    // infinities
    assert_eq!(
        cmp_num_vs_bound(NumScalar::I64(i64::MAX), &BoundValue::F64(f64::INFINITY)),
        Some(Ordering::Less)
    );
    assert_eq!(
        cmp_num_vs_bound(
            NumScalar::I64(i64::MIN),
            &BoundValue::F64(f64::NEG_INFINITY)
        ),
        Some(Ordering::Greater)
    );
    // u64 bound above i64::MAX vs an i64 stat: exact via i128
    assert_eq!(
        cmp_num_vs_bound(NumScalar::I64(-1), &BoundValue::U64(u64::MAX)),
        Some(Ordering::Less)
    );
    // string bound vs numeric stat: incomparable
    assert_eq!(
        cmp_num_vs_bound(NumScalar::I64(1), &BoundValue::Str("1".into())),
        None
    );
}

/// §6.2 M4 k-way region-merged reads: a concat file with proven regions
/// streams `ORDER BY _timestamp DESC` correctly across interleaved region
/// time-ranges, EQUAL timestamps across regions, limits (early exit), row
/// selections, and the internal `_timestamp` add-and-strip.
#[test]
fn merged_ordered_scan_over_concat_regions() {
    let schema = Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("tag", DataType::Utf8, true),
    ]);
    // three regions with INTERLEAVED ranges and CROSS-REGION EQUAL ts:
    //   r0: [500, 400, 300, 200]        (tag a)
    //   r1: [450, 400, 350]             (tag b) — 400 ties r0, ranges overlap
    //   r2: [1000, 100]                 (tag c) — brackets everything
    let ts: Vec<i64> = vec![500, 400, 300, 200, 450, 400, 350, 1000, 100];
    let tags: Vec<&str> = vec!["a", "a", "a", "a", "b", "b", "b", "c", "c"];
    let mut writer = VixWriter::new(
        &schema,
        VixWriterOptions {
            concat_row_order: true,
            docs_chunk_bytes: 1,
            ..Default::default()
        },
        false,
    );
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int64Array::from(ts.clone())) as ArrayRef,
            Arc::new(StringArray::from(tags.clone())) as ArrayRef,
        ],
    )
    .unwrap();
    let source = StringArray::from(vec!["{}"; 9]);
    writer
        .push_batch_with_source(&batch, &source, None)
        .unwrap();
    let (data, _) = writer.finish().unwrap();
    let docs = crate::VixDocs::open(bytes::Bytes::from(data)).unwrap();
    assert_eq!(
        docs.ts_desc_row_ranges(),
        Some(vec![0..4, 4..7, 7..9]),
        "the writer must have auto-detected the three desc runs"
    );

    let collect = |projection: Option<&[String]>,
                   rows: Option<Vec<u64>>,
                   ts_range: Option<(i64, i64)>,
                   limit: Option<u64>|
     -> (Vec<RecordBatch>, usize) {
        let mut out = Vec::new();
        let mut opened = 0usize;
        docs.scan_docs_ts_desc_merged(
            projection,
            rows,
            ts_range,
            &[],
            limit,
            &mut || {
                opened += 1;
                Ok(())
            },
            &mut |batch| {
                out.push(batch);
                Ok(())
            },
        )
        .unwrap();
        (out, opened)
    };
    let ts_of = |batches: &[RecordBatch]| -> Vec<i64> {
        let mut got = Vec::new();
        for batch in batches {
            let col = batch.column_by_name("_timestamp").unwrap();
            let col = col.as_any().downcast_ref::<Int64Array>().unwrap();
            got.extend(col.iter().flatten());
        }
        got
    };

    // full merged read = the global sort-desc truth, ROW COUNT PRESERVED
    // (equal timestamps kept, in deterministic region order)
    let (batches, opened) = collect(Some(&["_timestamp".to_string()]), None, None, None);
    let mut want = ts.clone();
    want.sort_unstable_by(|a, b| b.cmp(a));
    assert_eq!(ts_of(&batches), want);
    assert_eq!(opened, 3, "a full read opens every region");

    // LIMIT early exit: top-2 = [1000, 500] (all 9 rows share ONE zone
    // chunk here, so laziness proves nothing on this file — see the
    // chunk-aligned laziness check below)
    let (batches, _) = collect(Some(&["_timestamp".to_string()]), None, None, Some(2));
    assert_eq!(ts_of(&batches), vec![1000, 500]);

    // equal-ts run: top-4 covers the 400 tie from BOTH regions
    let (batches, _) = collect(Some(&["_timestamp".to_string()]), None, None, Some(4));
    assert_eq!(ts_of(&batches), vec![1000, 500, 450, 400]);
    let (batches, _) = collect(Some(&["_timestamp".to_string()]), None, None, Some(5));
    assert_eq!(ts_of(&batches), vec![1000, 500, 450, 400, 400]);

    // projection WITHOUT _timestamp: internally added, stripped on emit,
    // rows still in ts-desc order (tags prove it)
    let (batches, _) = collect(Some(&["tag".to_string()]), None, None, None);
    let mut tags_got: Vec<String> = Vec::new();
    for batch in &batches {
        assert!(
            batch.column_by_name("_timestamp").is_none(),
            "internal merge column must be stripped"
        );
        let col = batch.column_by_name("tag").unwrap();
        let col = arrow::compute::cast(col, &DataType::Utf8).unwrap();
        let col = col.as_any().downcast_ref::<StringArray>().unwrap();
        tags_got.extend((0..col.len()).map(|i| col.value(i).to_string()));
    }
    // ts desc truth: 1000c 500a 450b [400 tie] 350b 300a 200a 100c.
    // The 400 tie resolves to the region already emitting its run (r1's
    // 400 rides the 450 run) — deterministic, and any tie order is correct
    // for ORDER BY _timestamp DESC.
    assert_eq!(tags_got, vec!["c", "a", "b", "b", "a", "b", "a", "a", "c"]);

    // ts_range pushdown composes: [300, 460) -> 450, 400, 400, 350, 300
    let (batches, _) = collect(
        Some(&["_timestamp".to_string()]),
        None,
        Some((300, 460)),
        None,
    );
    assert_eq!(ts_of(&batches), vec![450, 400, 400, 350, 300]);

    // row selection: rows {0 (ts500), 5 (ts400), 8 (ts100)} merge in order
    let (batches, _) = collect(
        Some(&["_timestamp".to_string()]),
        Some(vec![0, 5, 8]),
        None,
        None,
    );
    assert_eq!(ts_of(&batches), vec![500, 400, 100]);

    // a ts_desc file (single implicit region) streams unchanged
    let sorted_docs = {
        let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
        let ts: Vec<i64> = (0..6).map(|i| 900 - i).collect();
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int64Array::from(ts)) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("s"); 6])) as ArrayRef,
            ],
        )
        .unwrap();
        let source = StringArray::from(vec!["{}"; 6]);
        writer
            .push_batch_with_source(&batch, &source, None)
            .unwrap();
        let (data, _) = writer.finish().unwrap();
        crate::VixDocs::open(bytes::Bytes::from(data)).unwrap()
    };
    let mut got = Vec::new();
    sorted_docs
        .scan_docs_ts_desc_merged(
            Some(&["_timestamp".to_string()]),
            None,
            None,
            &[],
            Some(3),
            &mut || Ok(()),
            &mut |batch| {
                got.push(batch);
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(ts_of(&got), vec![900, 899, 898]);
}

/// §6.2 lazy region opening: when regions align with distinct zone chunks,
/// a LIMIT that the newest region alone satisfies never opens (decodes)
/// the time-disjoint older regions — their zone-derived upper bounds keep
/// them parked in the heap.
#[test]
fn merged_ordered_scan_opens_regions_lazily() {
    let schema = Schema::new(vec![Field::new("_timestamp", DataType::Int64, false)]);
    // three 64-row regions (== the chunk floor, so zone chunks align),
    // each STRICTLY newer than the previous one's tail so every boundary
    // is a ts increase (adjacent decreasing runs would legally fuse):
    // r0 oldest [3000..], r1 middle [4000..], r2 newest [5000..]
    let ts: Vec<i64> = (0..64)
        .map(|i| 3000 - i)
        .chain((0..64).map(|i| 4000 - i))
        .chain((0..64).map(|i| 5000 - i))
        .collect();
    let mut writer = VixWriter::new(
        &schema,
        VixWriterOptions {
            concat_row_order: true,
            docs_chunk_bytes: 1, // 64-row chunk floor
            ..Default::default()
        },
        false,
    );
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int64Array::from(ts.clone())) as ArrayRef],
    )
    .unwrap();
    let source = StringArray::from(vec!["{}"; 192]);
    writer
        .push_batch_with_source(&batch, &source, None)
        .unwrap();
    let (data, _) = writer.finish().unwrap();
    let docs = crate::VixDocs::open(bytes::Bytes::from(data)).unwrap();
    assert_eq!(
        docs.ts_desc_row_ranges(),
        Some(vec![0..64, 64..128, 128..192])
    );
    assert_eq!(docs.zone_chunks().unwrap().len(), 3);

    let mut opened = 0usize;
    let mut got: Vec<i64> = Vec::new();
    docs.scan_docs_ts_desc_merged(
        Some(&["_timestamp".to_string()]),
        None,
        None,
        &[],
        Some(10),
        &mut || {
            opened += 1;
            Ok(())
        },
        &mut |batch| {
            let col = batch.column_by_name("_timestamp").unwrap();
            let col = col.as_any().downcast_ref::<Int64Array>().unwrap();
            got.extend(col.iter().flatten());
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(got, (0..10).map(|i| 5000 - i).collect::<Vec<i64>>());
    assert_eq!(
        opened, 1,
        "the newest region alone satisfies the limit; the older regions' \
         zone bounds must keep them unopened"
    );
}

/// §6.2: an ordered merged read of a concat file WITHOUT proven regions
/// must REFUSE (the caller sorts instead) — never emit silently unordered.
#[test]
fn merged_ordered_scan_refuses_unproven_concat() {
    let schema = Schema::new(vec![Field::new("_timestamp", DataType::Int64, false)]);
    let mut writer = VixWriter::new(
        &schema,
        VixWriterOptions {
            concat_row_order: true,
            ..Default::default()
        },
        false,
    );
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int64Array::from(vec![5i64, 9, 1])) as ArrayRef],
    )
    .unwrap();
    let source = StringArray::from(vec!["{}"; 3]);
    writer
        .push_batch_with_source(&batch, &source, None)
        .unwrap();
    let (data, _) = writer.finish().unwrap();
    // strip the region table: piecewise order now unproven
    let stripped = crate::test_support::strip_property_for_tests(&data, "row_regions").unwrap();
    let docs = crate::VixDocs::open(bytes::Bytes::from(stripped)).unwrap();
    assert_eq!(docs.ts_desc_row_ranges(), None);
    let err = docs
        .scan_docs_ts_desc_merged(None, None, None, &[], None, &mut || Ok(()), &mut |_| Ok(()))
        .expect_err("unproven concat must refuse the ordered read");
    assert!(
        err.to_string().contains("row_regions"),
        "unexpected error: {err}"
    );
}

/// H1 (DESIGN §3): rows-per-chunk derives from PRESENT-VALUE bytes, never
/// arrow width — a wide-sparse batch and a narrow batch carrying the same
/// present bytes must land in the same rows-per-chunk ballpark, and an
/// all-null 2,557-column schema (the historical collapse: ~10.5 KiB/row of
/// arrow padding) must not collapse the chunk row count.
#[test]
fn h1_rows_per_chunk_follows_present_bytes_not_arrow_width() {
    use arrow::array::new_null_array;

    use crate::writer::docs_rows_per_chunk;

    let rows = 1024usize;
    let ts: ArrayRef = Arc::new(Int64Array::from_iter_values(
        (0..rows).map(|row| 1_700_000_000_000_000 + row as i64),
    ));
    let msg: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..rows).map(|_| "m".repeat(96)),
    ));
    let source: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..rows).map(|_| "s".repeat(160)),
    ));

    let batch_of = |extra: Vec<(String, ArrayRef)>| -> RecordBatch {
        let mut fields = vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("msg", DataType::Utf8, true),
        ];
        let mut arrays = vec![Arc::clone(&ts), Arc::clone(&msg)];
        for (name, array) in extra {
            fields.push(Field::new(&name, array.data_type().clone(), true));
            arrays.push(array);
        }
        fields.push(Field::new("_source", DataType::Utf8, false));
        arrays.push(Arc::clone(&source));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
    };

    // narrow: 3 columns, all dense
    let narrow = batch_of(Vec::new());
    let narrow_rows = docs_rows_per_chunk(0, 0, std::slice::from_ref(&narrow));

    // wide-sparse (nulls carry no data): the same values plus 1,500 all-null
    // Utf8 columns — identical present bytes, so identical rows-per-chunk
    let all_null: Vec<(String, ArrayRef)> = (0..1500)
        .map(|i| {
            (
                format!("sparse_{i:04}"),
                new_null_array(&DataType::Utf8, rows),
            )
        })
        .collect();
    let wide_nulls = batch_of(all_null);
    let wide_null_rows = docs_rows_per_chunk(0, 0, std::slice::from_ref(&wide_nulls));
    assert_eq!(
        narrow_rows, wide_null_rows,
        "1,500 all-null columns must not change the chunk row count \
         (narrow {narrow_rows} vs wide {wide_null_rows})"
    );
    assert!(
        wide_nulls.get_array_memory_size() > 10 * narrow.get_array_memory_size(),
        "the wide batch must actually dwarf the narrow one in arrow bytes for \
         this test to prove anything"
    );

    // wide-sparse (values spread thin): the narrow batch's 96 msg bytes per
    // row spread as 3×32-byte values across 1,500 columns — same present
    // bytes, so the same ballpark (well within the H1 2x gate; only the
    // per-present-value overhead differs)
    let spread: Vec<(String, ArrayRef)> = (0..1500)
        .map(|column| {
            let values: StringArray = (0..rows)
                .map(|row| (column % 500 == row % 500).then(|| "v".repeat(32)))
                .collect();
            (format!("spread_{column:04}"), Arc::new(values) as ArrayRef)
        })
        .collect();
    let mut wide_spread_fields = vec![Field::new("_timestamp", DataType::Int64, false)];
    let mut wide_spread_arrays: Vec<ArrayRef> = vec![Arc::clone(&ts)];
    for (name, array) in spread {
        wide_spread_fields.push(Field::new(&name, DataType::Utf8, true));
        wide_spread_arrays.push(array);
    }
    wide_spread_fields.push(Field::new("_source", DataType::Utf8, false));
    wide_spread_arrays.push(Arc::clone(&source));
    let wide_spread = RecordBatch::try_new(
        Arc::new(Schema::new(wide_spread_fields)),
        wide_spread_arrays,
    )
    .unwrap();
    let wide_spread_rows = docs_rows_per_chunk(0, 0, std::slice::from_ref(&wide_spread));
    let (low, high) = if narrow_rows < wide_spread_rows {
        (narrow_rows, wide_spread_rows)
    } else {
        (wide_spread_rows, narrow_rows)
    };
    assert!(
        high <= low * 2,
        "equal present bytes must land within 2x rows-per-chunk: narrow \
         {narrow_rows} vs wide-spread {wide_spread_rows}"
    );

    // the historical failure shape: 2,557 nullable Utf8 columns, ALL null,
    // tiny row store — the chunk row count must saturate at the cap, not
    // collapse toward the floor
    let tiny_source: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..rows).map(|_| "s".repeat(16)),
    ));
    let mut fat_fields = vec![Field::new("_timestamp", DataType::Int64, false)];
    let mut fat_arrays: Vec<ArrayRef> = vec![Arc::clone(&ts)];
    for i in 0..2557 {
        fat_fields.push(Field::new(format!("fat_{i:04}"), DataType::Utf8, true));
        fat_arrays.push(new_null_array(&DataType::Utf8, rows));
    }
    fat_fields.push(Field::new("_source", DataType::Utf8, false));
    fat_arrays.push(tiny_source);
    let fat = RecordBatch::try_new(Arc::new(Schema::new(fat_fields)), fat_arrays).unwrap();
    assert_eq!(
        docs_rows_per_chunk(0, 0, std::slice::from_ref(&fat)),
        65536,
        "an all-null 2,557-column schema with a tiny row store must saturate \
         the rows-per-chunk cap, not collapse"
    );
}

/// M8: `docs_chunk_max_rows` — the rows-per-chunk CEILING is liftable, and
/// its default (`0` = 65,536) is byte-for-byte the pre-knob clamp.
#[test]
fn m8_docs_chunk_max_rows_caps_and_lifts() {
    use crate::writer::{DEFAULT_DOCS_CHUNK_MAX_ROWS, docs_rows_per_chunk};

    // ~1 KiB present bytes per row — the bench-corpus ballpark
    let rows = 1024usize;
    let ts: ArrayRef = Arc::new(Int64Array::from_iter_values(
        (0..rows).map(|row| 1_700_000_000_000_000 + row as i64),
    ));
    let source: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..rows).map(|_| "s".repeat(1000)),
    ));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("_source", DataType::Utf8, false),
        ])),
        vec![ts, source],
    )
    .unwrap();
    let batches = std::slice::from_ref(&batch);

    // DEFAULT UNCHANGED: `0` and the explicit historical cap agree at every
    // budget, including the saturating ones
    for budget in [0usize, 4 << 20, 16 << 20, 64 << 20, 2 << 30] {
        assert_eq!(
            docs_rows_per_chunk(budget, 0, batches),
            docs_rows_per_chunk(budget, DEFAULT_DOCS_CHUNK_MAX_ROWS, batches),
            "0 must mean the 65,536 default (budget {budget})"
        );
    }
    // a 64 MiB budget saturates the default cap (~66k-row target)...
    assert_eq!(docs_rows_per_chunk(64 << 20, 0, batches), 65536);
    // ...and a 2 GiB budget saturates identically: bigger budgets are inert
    assert_eq!(docs_rows_per_chunk(2 << 30, 0, batches), 65536);

    // OVERRIDE LIFTS: the same 2 GiB budget under a 32M-row cap follows the
    // byte budget instead (~2.1M rows at ~1 KiB/row)
    let lifted = docs_rows_per_chunk(2 << 30, 32_000_000, batches);
    assert!(
        lifted > 65536 * 10,
        "a lifted cap must let the byte budget govern, got {lifted}"
    );
    // OVERRIDE LOWERS: a small cap wins over the budget
    assert_eq!(docs_rows_per_chunk(4 << 20, 128, batches), 128);
    // the 64-row floor holds against a pathological cap
    assert_eq!(docs_rows_per_chunk(4 << 20, 1, batches), 64);
    // the options default carries the historical cap
    assert_eq!(
        VixWriterOptions::default().docs_chunk_max_rows,
        DEFAULT_DOCS_CHUNK_MAX_ROWS
    );
}

/// M8 end-to-end: `docs_chunk_max_rows` plumbs writer options → encoder →
/// zone table. Default vs explicit-65,536 outputs are BYTE-identical (the
/// knob's default changes nothing), and a lowered cap chunks the same rows
/// into cap-sized zone entries where the default kept one chunk.
#[test]
fn m8_docs_chunk_max_rows_plumbs_into_the_chunking() {
    use crate::VixDocs;

    // byte-identity: Default options vs the explicit historical cap
    let (default_data, default_index) = build_dataset_bytes(dataset_options());
    let (pinned_data, pinned_index) = build_dataset_bytes(VixWriterOptions {
        docs_chunk_max_rows: 65536,
        ..dataset_options()
    });
    assert_eq!(default_data, pinned_data, "data must be byte-identical");
    assert_eq!(
        default_index, pinned_index,
        "sidecar must be byte-identical"
    );

    // a lowered cap becomes the zone-table granularity: 1,000 rows under a
    // saturating byte budget land in ceil(1000/128) = 8 chunks
    let rows = 1000usize;
    let schema = Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("code", DataType::Int64, true),
    ]);
    let build = |max_rows: usize| -> VixDocs {
        let mut writer = VixWriter::new(
            &schema,
            VixWriterOptions {
                docs_chunk_bytes: 1 << 30,
                docs_chunk_max_rows: max_rows,
                ..Default::default()
            },
            false,
        );
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int64Array::from_iter_values(
                    (0..rows).map(|i| 2_000_000 - i as i64),
                )) as ArrayRef,
                Arc::new(Int64Array::from_iter_values((0..rows).map(|i| i as i64))) as ArrayRef,
            ],
        )
        .unwrap();
        let source = StringArray::from_iter_values((0..rows).map(|i| format!("{{\"code\":{i}}}")));
        writer
            .push_batch_with_source(&batch, &source, None)
            .unwrap();
        let (data, _) = writer.finish().unwrap();
        VixDocs::open(Bytes::from(data)).unwrap()
    };
    let capped = build(128);
    let zones = capped.zone_chunks().expect("zone table");
    assert_eq!(
        zones.len(),
        8,
        "1,000 rows at a 128-row cap = 8 zone chunks"
    );
    assert!(
        zones.iter().take(7).all(|zone| zone.row_count == 128),
        "full chunks must carry exactly the cap"
    );
    assert_eq!(zones.last().unwrap().row_count, 1000 - 7 * 128);
    // the default cap keeps the whole file in ONE chunk under the same budget
    let default_cap = build(0);
    assert_eq!(default_cap.zone_chunks().expect("zone table").len(), 1);
}

/// H2 (DESIGN §4): the DATA object carries a per-column chunk-stats blob —
/// per zone entry: present count + min/max (strings prefix-bounded,
/// numerics native) — plus present-row counts in the `columns` property.
/// Density-gated columns keep presence only; the byte cap keeps the densest
/// columns; splice-ability is exercised by the core-crate merge tests.
#[test]
fn h2_column_chunk_stats_written_and_gated() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("code", DataType::Int64, true),
        Field::new("svc", DataType::Utf8, true),
        Field::new("sparse", DataType::Utf8, true),
    ]));
    let rows = 512usize;
    let ts: Vec<i64> = (0..rows as i64).map(|i| 1_000_000 - i).collect();
    let code: Vec<Option<i64>> = (0..rows as i64).map(|i| Some(200 + (i % 100))).collect();
    let svc: Vec<Option<String>> = (0..rows).map(|i| Some(format!("svc-{}", i % 3))).collect();
    // 2% density: below the 10% default threshold
    let sparse: Vec<Option<String>> = (0..rows)
        .map(|i| (i % 50 == 0).then(|| format!("needle-{i}")))
        .collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ts.clone())),
            Arc::new(Int64Array::from(code.clone())),
            Arc::new(StringArray::from(svc.clone())),
            Arc::new(StringArray::from(sparse)),
        ],
    )
    .unwrap();
    let sources = dataset_sources(0..rows);
    let opts = VixWriterOptions {
        // several zone windows over 512 rows
        docs_chunk_bytes: 4096,
        ..Default::default()
    };
    let mut writer = VixWriter::new(&schema, opts, false);
    writer
        .push_batch_with_source(&batch, &sources, None)
        .unwrap();
    let (data, _index) = writer.finish().unwrap();

    let docs = crate::VixDocs::open(Bytes::from(data.clone())).unwrap();
    let reader = VixReader::open(Bytes::from(data)).unwrap_or_else(|_| unreachable!());
    let zone = reader.zone_chunks().expect("zone table");
    assert!(zone.len() >= 3, "need several zone windows: {}", zone.len());

    // presence counts: every docs column, exact
    let presence: std::collections::HashMap<&str, Option<u64>> = docs
        .column_presence()
        .iter()
        .map(|(name, count)| (name.as_str(), *count))
        .collect();
    assert_eq!(presence["_timestamp"], Some(rows as u64));
    assert_eq!(presence["code"], Some(rows as u64));
    assert_eq!(presence["svc"], Some(rows as u64));
    assert_eq!(presence["sparse"], Some((rows as u64).div_ceil(50)));

    let stats = docs
        .spliceable_stats()
        .unwrap()
        .expect("stats blob present");
    // dense columns carry chunk tables aligned with the zone table
    let code_stats = &stats.chunks.columns["code"];
    assert_eq!(code_stats.tag, "i64");
    assert_eq!(code_stats.chunks.len(), zone.len(), "1:1 zone alignment");
    let mut offset = 0usize;
    for (entry, zone_entry) in code_stats.chunks.iter().zip(zone) {
        let entry = entry.as_ref().expect("fresh build: no unknown entries");
        assert_eq!(entry.present, zone_entry.row_count, "code is dense");
        // exact min/max over the covered rows
        let window = &code[offset..offset + zone_entry.row_count as usize];
        let expect_min = window.iter().flatten().min().copied().unwrap();
        let expect_max = window.iter().flatten().max().copied().unwrap();
        assert_eq!(entry.min, Some(crate::StatValue::I64(expect_min)));
        assert_eq!(entry.max, Some(crate::StatValue::I64(expect_max)));
        offset += zone_entry.row_count as usize;
    }
    let svc_stats = &stats.chunks.columns["svc"];
    assert_eq!(svc_stats.tag, "str");
    let first = svc_stats.chunks[0].as_ref().unwrap();
    assert_eq!(first.min, Some(crate::StatValue::Str("svc-0".into())));
    assert_eq!(first.max, Some(crate::StatValue::Str("svc-2".into())));

    // density gate: the 2% column has NO chunk table (presence-only)
    assert!(
        !stats.chunks.columns.contains_key("sparse"),
        "below-threshold column must not pay a chunk-stats table"
    );
    // _timestamp/_source never appear (the zone table IS _timestamp's stats)
    assert!(!stats.chunks.columns.contains_key("_timestamp"));
    assert!(!stats.chunks.columns.contains_key("_source"));

    // byte cap: a tiny cap keeps at most the densest column(s), never errors
    let mut writer = VixWriter::new(
        &schema,
        VixWriterOptions {
            docs_chunk_bytes: 4096,
            stats_max_bytes: 150,
            ..Default::default()
        },
        false,
    );
    writer
        .push_batch_with_source(&batch, &dataset_sources(0..rows), None)
        .unwrap();
    let (data, _index) = writer.finish().unwrap();
    let docs = crate::VixDocs::open(Bytes::from(data)).unwrap();
    let capped = docs.spliceable_stats().unwrap();
    let kept = capped.map_or(0, |s| s.chunks.columns.len());
    assert!(
        kept < 2,
        "a 150-byte cap must shed columns (kept {kept} tables)"
    );
    // presence counts survive the cap regardless
    assert_eq!(
        docs.column_presence()
            .iter()
            .find(|(name, _)| name == "svc")
            .and_then(|(_, count)| *count),
        Some(rows as u64)
    );
}

// Manual diagnostic (kept from the M6 investigation): dumps the docs-blob
// physical layout of a real .vix file — per-column chunk segment ids, byte
// totals and offset spans — the tool that separated "projection ignored"
// from "interleaved layout defeats the coalescer". Run with:
//   M6_PROBE_FILE=/path/to/file.vix \
//     cargo test -p vortex_index m6_probe_docs_layout -- --ignored --nocapture
#[test]
#[ignore]
fn m6_probe_docs_layout() {
    use vortex::{
        VortexSessionDefault,
        file::OpenOptionsSessionExt,
        io::{
            runtime::{BlockingRuntime, single::SingleThreadRuntime},
            session::RuntimeSessionExt,
        },
        session::VortexSession,
    };
    let path = std::env::var("M6_PROBE_FILE").expect("M6_PROBE_FILE=<file.vix>");
    let data = Bytes::from(std::fs::read(&path).unwrap());
    let container = crate::container::parse_container(&data).unwrap();
    let docs = container.docs.expect("docs blob");
    let docs_bytes = docs.bytes().unwrap();
    println!("file={path} docs_blob_bytes={}", docs_bytes.len());

    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let vxf = session.open_options().open_buffer(docs_bytes).unwrap();
    let footer = vxf.footer();
    let segmap = footer.segment_map().clone();
    println!("segments_total={}", segmap.len());
    let root = footer.layout().clone();
    println!(
        "root encoding={} nchildren={}",
        root.encoding_id(),
        root.nchildren()
    );

    let names: Vec<std::sync::Arc<str>> = root.child_names().collect();
    let children = root.children().unwrap();
    for (name, child) in names.iter().zip(children.iter()) {
        // per-column: chunk count, own segment ids, offset span
        let mut seg_ids: Vec<u32> = Vec::new();
        let mut stack = vec![child.clone()];
        while let Some(node) = stack.pop() {
            for sid in node.segment_ids() {
                seg_ids.push(*sid);
            }
            if let Ok(kids) = node.children() {
                stack.extend(kids);
            }
        }
        seg_ids.sort_unstable();
        let offs: Vec<u64> = seg_ids.iter().map(|&s| segmap[s as usize].offset).collect();
        let total: u64 = seg_ids
            .iter()
            .map(|&s| segmap[s as usize].length as u64)
            .sum();
        let (lo, hi) = (
            offs.iter().min().copied().unwrap_or(0),
            offs.iter().max().copied().unwrap_or(0),
        );
        println!(
            "col={name:<24} encoding={} segs={} bytes={} first_seg_ids={:?} offset_span={lo}..{hi}",
            child.encoding_id(),
            seg_ids.len(),
            total,
            &seg_ids[..seg_ids.len().min(6)],
        );
    }
    // interleave check: offsets of the first 6 chunk-leaves of two narrow
    // columns vs their neighbors
    for probe in ["_timestamp", "duration"] {
        if let Some(pos) = names.iter().position(|n| n.as_ref() == probe) {
            let child = &children[pos];
            let mut leaf_ids: Vec<u32> = Vec::new();
            let mut stack = vec![child.clone()];
            while let Some(node) = stack.pop() {
                for sid in node.segment_ids() {
                    leaf_ids.push(*sid);
                }
                if let Ok(kids) = node.children() {
                    stack.extend(kids);
                }
            }
            leaf_ids.sort_unstable();
            let head: Vec<(u32, u64, u32)> = leaf_ids
                .iter()
                .take(6)
                .map(|&s| (s, segmap[s as usize].offset, segmap[s as usize].length))
                .collect();
            println!("col={probe} first leaves (seg,off,len)={head:?}");
        }
    }
}

// M8 chunk-size sweep diagnostics (manual, ignored — the m6 probe's sibling):
//
//     M8_PROBE_FILE=<file.vix> [M8_PROBE_TS_FRAC=0.01] [M8_PROBE_LIMIT=100] \
//       cargo test -p vortex_index --release m8_probe_chunk_geometry -- --ignored --nocapture
//
// Prints, for one data object: the zone-table chunk geometry (the
// footer-backed chunk count the sweep records), per-column encoded bytes +
// chunk-leaf counts (_source vs the rest — where compression wins land),
// zone-map pruning under a mid-file `_timestamp` window (how much a
// selective window still reads at this chunk size), LIMIT early-exit walls
// (narrow and `_source` projections), and a 1-row point read of `_source`
// (the decompression-granule cost of a needle hit).
#[test]
#[ignore]
fn m8_probe_chunk_geometry() {
    use vortex::{
        VortexSessionDefault,
        file::OpenOptionsSessionExt,
        io::{
            runtime::{BlockingRuntime, single::SingleThreadRuntime},
            session::RuntimeSessionExt,
        },
        session::VortexSession,
    };

    use crate::VixDocs;

    let path = std::env::var("M8_PROBE_FILE").expect("M8_PROBE_FILE=<file.vix>");
    let ts_frac: f64 = std::env::var("M8_PROBE_TS_FRAC")
        .ok()
        .map(|v| v.parse().unwrap())
        .unwrap_or(0.01);
    let limit: u64 = std::env::var("M8_PROBE_LIMIT")
        .ok()
        .map(|v| v.parse().unwrap())
        .unwrap_or(100);
    let data = Bytes::from(std::fs::read(&path).unwrap());
    let docs = VixDocs::open(data.clone()).unwrap();
    let total_rows = docs.row_count();
    println!("file={path} bytes={} rows={total_rows}", data.len());

    // 1) zone-table chunk geometry — THE chunk count of the sweep table
    match docs.zone_chunks() {
        Some(zones) => {
            let mut rows: Vec<u64> = zones.iter().map(|zone| zone.row_count).collect();
            rows.sort_unstable();
            println!(
                "zone_chunks={} rows/chunk min={} median={} max={}",
                zones.len(),
                rows.first().unwrap_or(&0),
                rows.get(rows.len() / 2).unwrap_or(&0),
                rows.last().unwrap_or(&0),
            );
        }
        None => println!("zone_chunks=NONE"),
    }

    // 2) per-column encoded bytes + chunk-leaf counts from the vortex footer
    let container = crate::container::parse_container(&data).unwrap();
    let docs_blob = container.docs.expect("docs blob");
    let docs_bytes = docs_blob.bytes().unwrap();
    println!("docs_blob_bytes={}", docs_bytes.len());
    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let vxf = session.open_options().open_buffer(docs_bytes).unwrap();
    let footer = vxf.footer();
    let segmap = footer.segment_map().clone();
    let root = footer.layout().clone();
    let names: Vec<std::sync::Arc<str>> = root.child_names().collect();
    let children = root.children().unwrap();
    let mut source_bytes = 0u64;
    let mut rest_bytes = 0u64;
    for (name, child) in names.iter().zip(children.iter()) {
        let mut seg_ids: Vec<u32> = Vec::new();
        let mut stack = vec![child.clone()];
        while let Some(node) = stack.pop() {
            for sid in node.segment_ids() {
                seg_ids.push(*sid);
            }
            if let Ok(kids) = node.children() {
                stack.extend(kids);
            }
        }
        let total: u64 = seg_ids
            .iter()
            .map(|&s| segmap[s as usize].length as u64)
            .sum();
        if name.as_ref() == "_source" {
            source_bytes += total;
        } else {
            rest_bytes += total;
        }
        println!("col={name:<24} segs={} bytes={total}", seg_ids.len());
    }
    println!("col-group _source={source_bytes} rest={rest_bytes}");

    // 3) zone-map pruning under a mid-file ts window of `ts_frac` span.
    // zone_ts_bounds() is None on ts_desc files BY DESIGN (sorted files use
    // boundary rows for stats) — the zone chunks still carry per-chunk ts
    // bounds, so fold the file span from the zone table directly.
    let file_span = docs.zone_ts_bounds().or_else(|| {
        docs.zone_chunks().and_then(|chunks| {
            let min = chunks.iter().map(|c| c.ts_min).min()?;
            let max = chunks.iter().map(|c| c.ts_max).max()?;
            Some((min, max))
        })
    });
    if let Some((ts_min, ts_max)) = file_span {
        let span = (ts_max - ts_min).max(1) as f64;
        let half = (span * ts_frac / 2.0) as i64;
        // anchor the window on the MID ROW's actual timestamp — the bench
        // corpus is disjoint files with 10x ts spacing, so the span midpoint
        // can land in an unpopulated gap and prune everything vacuously
        let mut center = ts_min + (span / 2.0) as i64;
        docs.scan_docs(
            Some(&["_timestamp".to_string()]),
            Some(vec![total_rows / 2]),
            None,
            &mut |batch| {
                let ts = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("_timestamp is Int64");
                if ts.len() > 0 {
                    center = ts.value(0);
                }
                Ok(())
            },
        )
        .unwrap();
        let window = (center - half, center + half + 1);
        match docs.pruned_scan_ranges(Some(window), &[]) {
            Some(ranges) => {
                let surviving: u64 = ranges.iter().map(|r| r.end - r.start).sum();
                println!(
                    "ts-window frac={ts_frac} window_us={} center={center} -> \
                     surviving_rows={surviving} ranges={}",
                    2 * half + 1,
                    ranges.len(),
                );
            }
            None => println!("ts-window frac={ts_frac} -> NO pruning basis (full scan)"),
        }
    } else {
        println!("no zone ts bounds");
    }

    // 4) LIMIT early-exit: decode cost of the first `limit` rows
    for cols in [&["_timestamp", "duration"][..], &["_source"][..]] {
        let projection: Vec<String> = cols.iter().map(|c| c.to_string()).collect();
        let mut produced = 0u64;
        let mut batches = 0u64;
        let start = std::time::Instant::now();
        docs.scan_docs_opts(
            Some(&projection),
            None,
            None,
            &[],
            Some(limit),
            0,
            &mut |batch| {
                produced += batch.num_rows() as u64;
                batches += 1;
                Ok(())
            },
        )
        .unwrap();
        println!(
            "limit={limit} proj={cols:?} wall={:?} produced={produced} batches={batches}",
            start.elapsed()
        );
    }

    // 5) 1-row point read of `_source` — the needle decompression granule
    let mid = total_rows / 2;
    let projection = vec!["_source".to_string()];
    let mut produced = 0u64;
    let start = std::time::Instant::now();
    docs.scan_docs(Some(&projection), Some(vec![mid]), None, &mut |batch| {
        produced += batch.num_rows() as u64;
        Ok(())
    })
    .unwrap();
    println!(
        "point-read row={mid} proj=[_source] wall={:?} produced={produced}",
        start.elapsed()
    );
}

/// M12 item 3 (prod "vortex.shared not permitted by ctx"): unit pins for
/// [`crate::container::unwrap_shared`]. The dict LAYOUT reader wraps its
/// values child in a non-serializable runtime `SharedArray`; the rewrite
/// must strip it while preserving the dict's stored children and its
/// `all_values_referenced` flag, and must refuse (=> canonicalize fallback)
/// any shape it cannot provably rebuild.
#[test]
fn m12_unwrap_shared_strips_dict_reader_wrappers() {
    use vortex::{
        array::{
            IntoArray,
            arrays::{
                Dict, DictArray, PrimitiveArray, SharedArray, StructArray,
                dict::{DictArrayExt, DictArraySlotsExt},
            },
            validity::Validity,
        },
        buffer::buffer,
    };

    use crate::container::{contains_shared, unwrap_shared};

    let codes = PrimitiveArray::new(buffer![0u32, 1, 0, 2], Validity::NonNullable).into_array();
    let values = PrimitiveArray::new(buffer![10i64, 20, 30], Validity::NonNullable).into_array();

    // no Shared anywhere: identity (the cheap common case)
    let clean = unwrap_shared(&values).expect("shared-free arrays pass through");
    assert!(!contains_shared(&clean));
    assert_eq!(clean.len(), 3);

    // a bare Shared wrapper unwraps to its SOURCE (stored form, not the
    // canonical cache)
    let bare = SharedArray::new(values.clone()).into_array();
    assert!(contains_shared(&bare));
    let clean = unwrap_shared(&bare).expect("bare Shared must unwrap");
    assert!(!contains_shared(&clean));
    assert_eq!(clean.len(), 3);
    assert_eq!(clean.encoding_id(), values.encoding_id());

    // THE prod shape — dict(codes, Shared(values)), the dict layout
    // reader's projection output. SAFETY of the fixture: codes 0/1/2 all
    // reference the 3 values, so the asserted flag is true in fact.
    let wrapped = unsafe {
        DictArray::new_unchecked(codes.clone(), SharedArray::new(values.clone()).into_array())
            .set_all_values_referenced(true)
    }
    .into_array();
    assert!(contains_shared(&wrapped), "fixture carries the wrapper");
    let clean = unwrap_shared(&wrapped).expect("dict-wrapped Shared must unwrap");
    assert!(
        !contains_shared(&clean),
        "no Shared may survive into the serialize path"
    );
    let dict = clean.as_typed::<Dict>().expect("stays dict-encoded");
    assert!(
        dict.has_all_values_referenced(),
        "the encode-time flag must carry over"
    );
    assert_eq!(dict.codes().len(), 4);
    assert_eq!(dict.values().len(), 3);
    assert_eq!(dict.values().encoding_id(), values.encoding_id());

    // a Shared under a parent the rewrite does not know (here: a nested
    // struct column) must return None — the caller then canonicalizes the
    // field instead of copying, never errors
    let names: vortex::dtype::FieldNames = vec![vortex::dtype::FieldName::from("inner")].into();
    let nested = StructArray::try_new(
        names,
        vec![SharedArray::new(values.clone()).into_array()],
        3,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    assert!(
        unwrap_shared(&nested).is_none(),
        "unknown parents fall back to the canonicalize path"
    );
}

/// M12 item 3 e2e: a chunk that stores a column under a DICT LAYOUT (what
/// vortex's default first-encode strategy — [`crate::container::docs_strategy`]
/// via `WriteStrategyBuilder`'s `DictStrategy` — produces for repetitive
/// columns; the passthrough strategy itself never dict-probes) must survive
/// the #51c verbatim chunk copy. The dict layout READER wraps the values
/// child of every yielded chunk in a non-serializable `vortex.shared`
/// runtime cache; single-chunk dict fields have no adjacent chunk to trip
/// the slice guard's overlap canonicalization, so before the M12 unwrap the
/// wrapper reached the writer and the copy failed with "Array encoding
/// vortex.shared not permitted by ctx" (every prod heal-passthrough WARN).
///
/// Dict-present in the scanned chunk proves the field came through the dict
/// layout reader — whose every values arm Shared-wraps — so dict-present
/// PLUS Shared-absent proves the unwrap ran (the canonicalize fallback
/// would have erased the dict encoding).
#[test]
fn m12_dict_layout_chunk_copy_roundtrip() {
    use vortex::array::arrays::Dict;

    use crate::{
        VixDocs,
        container::{contains_shared, unwrap_shared},
    };
    let _ = unwrap_shared; // referenced by the doc comment

    // Corpus tuned so BtrBlocks' probe PICKS dict for the first chunk
    // (high-entropy values with repeats — zstd wins low-card repetitive
    // text instead, verified empirically): 1024 distinct random 32-char
    // strings over 8192 rows, one chunk.
    let rows = 8192usize;
    let card = 1024usize;
    let schema = Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("service", DataType::Utf8, true),
    ]);
    let mut rng = StdRng::seed_from_u64(0x0121_2012);
    let pool: Vec<String> = (0..card)
        .map(|_| {
            (0..32)
                .map(|_| char::from(b'a' + (rng.random::<u8>() % 26)))
                .collect()
        })
        .collect();
    let ts: Vec<i64> = (0..rows).map(|i| 1_000_000 - i as i64).collect();
    let svc: Vec<&str> = (0..rows).map(|i| pool[i % card].as_str()).collect();
    let sources: Vec<String> = svc
        .iter()
        .zip(&ts)
        .map(|(s, t)| format!(r#"{{"_timestamp":{t},"service":"{s}"}}"#))
        .collect();

    // first-encode file (default docs strategy => dict probe runs)
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int64Array::from(ts.clone())) as ArrayRef,
            Arc::new(StringArray::from(svc.clone())) as ArrayRef,
        ],
    )
    .unwrap();
    let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
    writer
        .push_batch_with_source(&batch, &StringArray::from(sources.clone()), None)
        .unwrap();
    let (data, _) = writer.finish().unwrap();
    let input = VixDocs::open(Bytes::from(data)).unwrap();

    // the encoded-chunk scan: dict layout engaged, wrapper stripped
    let mut scanned = Vec::new();
    let mut saw_dict = false;
    input
        .scan_docs_encoded_chunks(&mut |chunk| {
            saw_dict |= chunk
                .array
                .depth_first_traversal()
                .any(|node| node.is::<Dict>());
            assert!(
                !contains_shared(&chunk.array),
                "no vortex.shared may reach the copy consumer"
            );
            scanned.push(chunk);
            Ok(())
        })
        .unwrap();
    assert!(
        saw_dict,
        "fixture must produce a DICT LAYOUT (repetitive column, {rows} rows) — if this fires, \
         the corpus no longer triggers vortex's dict probe and the test needs retuning"
    );

    // verbatim copy through a passthrough writer (the heal/merge shape) —
    // pre-M12 this failed at serialize time
    let mut out_writer = VixWriter::new(
        &schema,
        VixWriterOptions {
            docs_passthrough: true,
            concat_row_order: true,
            ..Default::default()
        },
        false,
    );
    let ts_arr = Int64Array::from(ts.clone());
    let cs: Vec<(String, ArrayRef)> = vec![(
        "service".to_string(),
        Arc::new(StringArray::from(svc.clone())) as ArrayRef,
    )];
    out_writer
        .push_docs_rows_index_only(&ts_arr, &cs, &StringArray::from(sources.clone()), None)
        .unwrap();
    let entries: Vec<crate::ZoneEntry> = input
        .zone_chunks()
        .unwrap()
        .iter()
        .map(|zone| (zone.row_count, zone.ts_min, zone.ts_max))
        .collect();
    let stats = input.spliceable_stats().unwrap().unwrap();
    out_writer
        .begin_docs_encoded_run(
            rows as u64,
            *ts.last().unwrap(),
            ts[0],
            &entries,
            &stats,
            None,
        )
        .unwrap();
    for chunk in scanned {
        out_writer.push_docs_encoded_chunk(chunk).unwrap();
    }
    out_writer.finish_docs_encoded_run().unwrap();
    let (out_data, _) = out_writer.finish().unwrap();

    // the copied file: rows byte-equal on read-back, dict encoding kept in
    // stored form (is_decoded_family(dict) = false => written as-is)
    let output = VixDocs::open(Bytes::from(out_data)).unwrap();
    let mut out_dict = false;
    output
        .scan_docs_encoded_chunks(&mut |chunk| {
            out_dict |= chunk
                .array
                .depth_first_traversal()
                .any(|node| node.is::<Dict>());
            assert!(!contains_shared(&chunk.array));
            Ok(())
        })
        .unwrap();
    assert!(out_dict, "the verbatim copy must keep the dict encoding");

    let read_rows = |docs: &VixDocs| -> Vec<(i64, String, String)> {
        let mut out = Vec::new();
        for batch in docs.read_docs(None, None, None).unwrap() {
            let ts = batch
                .column_by_name("_timestamp")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .clone();
            let svc =
                arrow::compute::cast(batch.column_by_name("service").unwrap(), &DataType::Utf8)
                    .unwrap();
            let svc = svc.as_any().downcast_ref::<StringArray>().unwrap().clone();
            let src = arrow::compute::cast(
                batch.column_by_name(crate::SOURCE_COL_NAME).unwrap(),
                &DataType::Utf8,
            )
            .unwrap();
            let src = src.as_any().downcast_ref::<StringArray>().unwrap().clone();
            for i in 0..batch.num_rows() {
                out.push((
                    ts.value(i),
                    svc.value(i).to_string(),
                    src.value(i).to_string(),
                ));
            }
        }
        out
    };
    assert_eq!(
        read_rows(&output),
        read_rows(&input),
        "copied rows must read back identically"
    );
}

/// M25: a heal-passthrough copy of SPARSE low-cardinality columns must not
/// bloat the output. The wide-schema shape: an input's sparse column is
/// dict-encoded and COALESCED into leaves far coarser than the scan's
/// union-grid windows (the dense `_source` column chunks finely), so every
/// window of the sparse column arrives at the writer as a canonicalized
/// SLICE — a decoded VarBinView root still borrowing the dict's encoded
/// buffers. Pre-M25 the passthrough classified that mixed tree "encoded,
/// copy verbatim" and stored the raw 16 B/row views buffer per column
/// window: a 2,000-column merge wrote 15.6x its input bytes (7,034 MiB from
/// 450 MiB). The fix keys the classification on the ROOT node
/// ([`crate::container::is_decoded_root`]): decoded root => compress branch.
/// (The M12 test above pins the opposite side: a WHOLE dict leaf — root
/// `vortex.dict` — still copies verbatim.)
#[test]
fn m25_sparse_column_copy_does_not_bloat() {
    use crate::VixDocs;

    let rows = 40_000usize;
    let sparse_cols = 8usize;
    let mut fields = vec![Field::new("_timestamp", DataType::Int64, false)];
    for c in 0..sparse_cols {
        fields.push(Field::new(
            format!("k8s_label_{c:02}"),
            DataType::Utf8,
            true,
        ));
    }
    let schema = Schema::new(fields);
    let ts: Vec<i64> = (0..rows).map(|i| 1_000_000_000 - i as i64).collect();
    // ~10% present, 32 distinct values per column — the k8s label shape
    let sparse: Vec<Vec<Option<String>>> = (0..sparse_cols)
        .map(|c| {
            (0..rows)
                .map(|i| ((i + c) % 10 == 0).then(|| format!("v{c:02}-{:02}", (i / 10) % 32)))
                .collect()
        })
        .collect();
    // dense high-entropy _source (~120 B/row) so the docs blob carries many
    // fine chunks while the sparse columns coalesce into coarse leaves
    let mut rng = StdRng::seed_from_u64(0x2500_0001);
    let sources: Vec<String> = (0..rows)
        .map(|i| {
            let noise: String = (0..96)
                .map(|_| char::from(b'a' + (rng.random::<u8>() % 26)))
                .collect();
            format!(r#"{{"_timestamp":{},"blob":"{noise}"}}"#, ts[i])
        })
        .collect();

    let mut columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(ts.clone()))];
    for values in &sparse {
        columns.push(Arc::new(StringArray::from(
            values.iter().map(|v| v.as_deref()).collect::<Vec<_>>(),
        )));
    }
    let batch = RecordBatch::try_new(Arc::new(schema.clone()), columns).unwrap();
    let mut writer = VixWriter::new(
        &schema,
        VixWriterOptions {
            // small chunks => many scan windows => the coarse sparse leaves
            // are sliced by the union grid (the wide-merge shape)
            docs_chunk_bytes: 256 * 1024,
            ..Default::default()
        },
        false,
    );
    writer
        .push_batch_with_source(&batch, &StringArray::from(sources.clone()), None)
        .unwrap();
    let (data, _) = writer.finish().unwrap();
    let input = VixDocs::open(Bytes::from(data)).unwrap();
    let input_docs = input.docs_blob_len();
    // fixture guard: the copy consumer must actually SEE the mixed shape
    // (decoded root over an encoded descendant — the sliced dict-layout
    // canonical form). If this stops firing, the corpus no longer engages
    // vortex's dict probe and the test needs retuning.
    let mut mixed = 0usize;
    input
        .scan_docs_encoded_chunks(&mut |chunk| {
            use vortex::array::arrays::{Struct, struct_::StructArrayExt};
            let sa = chunk.array.as_typed::<Struct>().unwrap().clone();
            for field in sa.unmasked_fields().iter() {
                if crate::container::is_decoded_root(field)
                    && !crate::container::is_decoded_family(field)
                {
                    mixed += 1;
                }
            }
            Ok(())
        })
        .unwrap();
    assert!(
        mixed >= 8,
        "fixture must yield sliced-dict mixed chunks (decoded root over encoded buffers), got \
         {mixed}"
    );

    // the copy shape: encoded-chunk scan -> passthrough writer
    let mut out_writer = VixWriter::new(
        &schema,
        VixWriterOptions {
            docs_passthrough: true,
            concat_row_order: true,
            ..Default::default()
        },
        false,
    );
    let ts_arr = Int64Array::from(ts.clone());
    let cs: Vec<(String, ArrayRef)> = sparse
        .iter()
        .enumerate()
        .map(|(c, values)| {
            (
                format!("k8s_label_{c:02}"),
                Arc::new(StringArray::from(
                    values.iter().map(|v| v.as_deref()).collect::<Vec<_>>(),
                )) as ArrayRef,
            )
        })
        .collect();
    out_writer
        .push_docs_rows_index_only(&ts_arr, &cs, &StringArray::from(sources.clone()), None)
        .unwrap();
    let entries: Vec<crate::ZoneEntry> = input
        .zone_chunks()
        .unwrap()
        .iter()
        .map(|zone| (zone.row_count, zone.ts_min, zone.ts_max))
        .collect();
    let stats = input.spliceable_stats().unwrap().unwrap();
    out_writer
        .begin_docs_encoded_run(
            rows as u64,
            *ts.last().unwrap(),
            ts[0],
            &entries,
            &stats,
            None,
        )
        .unwrap();
    input
        .scan_docs_encoded_chunks(&mut |chunk| out_writer.push_docs_encoded_chunk(chunk))
        .unwrap();
    out_writer.finish_docs_encoded_run().unwrap();
    let (out_data, _) = out_writer.finish().unwrap();
    let output = VixDocs::open(Bytes::from(out_data)).unwrap();
    let output_docs = output.docs_blob_len();

    // pre-M25 the sparse columns' sliced windows stored raw views:
    // 8 cols x 40k rows x 16 B ~ 5 MiB of bloat on a ~1.5 MiB input blob
    // (measured 3.5-4x). Compressed, the copy stays within noise of the
    // input.
    assert!(
        output_docs <= input_docs * 3 / 2,
        "sparse-column copy bloated: input docs blob {input_docs} B -> output {output_docs} B \
         (> 1.5x) — sliced sparse windows are being stored as raw views again"
    );

    // and the rows read back identically (values AND nulls)
    let read_rows = |docs: &VixDocs| -> Vec<(i64, Vec<Option<String>>)> {
        let mut out = Vec::new();
        for batch in docs.read_docs(None, None, None).unwrap() {
            let ts = batch
                .column_by_name("_timestamp")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .clone();
            let cols: Vec<StringArray> = (0..sparse_cols)
                .map(|c| {
                    let col = arrow::compute::cast(
                        batch.column_by_name(&format!("k8s_label_{c:02}")).unwrap(),
                        &DataType::Utf8,
                    )
                    .unwrap();
                    col.as_any().downcast_ref::<StringArray>().unwrap().clone()
                })
                .collect();
            for i in 0..batch.num_rows() {
                out.push((
                    ts.value(i),
                    cols.iter()
                        .map(|col| col.is_valid(i).then(|| col.value(i).to_string()))
                        .collect(),
                ));
            }
        }
        out
    };
    assert_eq!(
        read_rows(&output),
        read_rows(&input),
        "copied rows must read back identically (values and nulls)"
    );
}

// M12 item 3 prod-repro probe (manual, ignored). Points at a REAL prod
// `.vix` (read-only fetch) and proves the root cause + the fix on its bytes:
//
//   M12_REPRO_FILE=/tmp/claude-1000/m12/repro.vix \
//     cargo test -p vortex_index --lib m12_probe_prod_shared_wrapper -- --ignored --nocapture
//
// (1) the RAW vortex scan (the pre-M12 read path) yields chunks whose dict
//     columns carry the `vortex.shared` runtime wrapper, and serializing one
//     with the file writer's own ctx construction reproduces the exact prod
//     error "Array encoding vortex.shared not permitted by ctx";
// (2) the FIXED [`crate::container::scan_blob_encoded_chunks`] yields the
//     same chunks with the wrapper stripped, every one of which serializes.
#[test]
#[ignore]
fn m12_probe_prod_shared_wrapper() {
    use vortex::{
        VortexSessionDefault,
        array::{ArrayContext, arrays::Dict, session::ArraySessionExt},
        file::OpenOptionsSessionExt,
        io::{
            runtime::{BlockingRuntime, single::SingleThreadRuntime},
            session::RuntimeSessionExt,
        },
        session::VortexSession,
    };

    use crate::{VixDocs, container::contains_shared};

    let path = std::env::var("M12_REPRO_FILE").expect("M12_REPRO_FILE=<file.vix>");
    let data = Bytes::from(std::fs::read(&path).unwrap());

    // (1) the raw scan — what the encoded-chunk copy consumed pre-M12
    let container = crate::container::parse_container(&data).unwrap();
    let docs_bytes = container.docs.expect("docs blob").bytes().unwrap();
    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let vxf = session.open_options().open_buffer(docs_bytes).unwrap();
    let scan = vxf.scan().unwrap();
    let write_ctx =
        ArrayContext::new(Vec::new()).with_registry(session.arrays().registry().clone());
    let (mut raw_chunks, mut raw_shared, mut raw_dict, mut serialize_errors) = (0, 0, 0, 0);
    let mut first_error = String::new();
    for array in scan.into_array_iter(&runtime).unwrap() {
        let array = array.unwrap();
        if array.len() == 0 {
            continue;
        }
        raw_chunks += 1;
        raw_shared += usize::from(contains_shared(&array));
        raw_dict += usize::from(array.depth_first_traversal().any(|node| node.is::<Dict>()));
        if let Err(e) = array.serialize(&write_ctx, &session, &Default::default()) {
            serialize_errors += 1;
            if first_error.is_empty() {
                first_error = format!("{e}");
            }
        }
    }
    println!(
        "raw scan: chunks={raw_chunks} with_shared={raw_shared} with_dict={raw_dict} \
         serialize_errors={serialize_errors}\nfirst_error={first_error}"
    );

    // (2) the fixed scan: wrapper stripped, everything serializes
    let docs = VixDocs::open(data).unwrap();
    let (mut fixed_chunks, mut fixed_dict) = (0, 0);
    docs.scan_docs_encoded_chunks(&mut |chunk| {
        fixed_chunks += 1;
        assert!(
            !contains_shared(&chunk.array),
            "fixed scan must never yield a Shared node"
        );
        fixed_dict += usize::from(
            chunk
                .array
                .depth_first_traversal()
                .any(|node| node.is::<Dict>()),
        );
        chunk
            .array
            .serialize(&write_ctx, &session, &Default::default())
            .expect("fixed scan chunks must serialize");
        Ok(())
    })
    .unwrap();
    println!("fixed scan: chunks={fixed_chunks} with_dict={fixed_dict} (all serialized)");

    if raw_shared > 0 {
        assert!(
            first_error.contains("vortex.shared not permitted by ctx"),
            "expected THE prod error on the raw path, got: {first_error}"
        );
        assert!(fixed_dict > 0, "the fix must preserve the dict encodings");
    } else {
        println!("NOTE: this file's raw scan carried no Shared wrapper (no dict layout?)");
    }
}

// M12 diagnostic probe (manual, ignored — the m6/m8 probes' sibling): print
// one file pair's term/bloom facts — bloom-only markers, per-field term
// capability + partial taint, dictionary term counts, bloom section
// geometry (num_blocks / n_items / bytes). This is what separated "the M10
// coverage scan hashes `duration`" (it never did — numeric fields never
// enter bloom_only; the merge-site AUTO log was the artifact) from the
// real scan set (the four birth-demoted ID columns).
//
//   M12_PROBE_DATA=<x.vix> [M12_PROBE_IDX=<x.vxi>] \
//     cargo test -p vortex_index --lib m12_probe_file_facts -- --ignored --nocapture
#[test]
#[ignore]
fn m12_probe_file_facts() {
    let data = std::env::var("M12_PROBE_DATA").expect("M12_PROBE_DATA");
    let idx = std::env::var("M12_PROBE_IDX").ok();
    let data_bytes = Bytes::from(std::fs::read(&data).unwrap());
    let idx_bytes = idx.map(|p| Bytes::from(std::fs::read(p).unwrap()));
    let reader = VixReader::open_with_index(data_bytes, idx_bytes).unwrap();
    println!(
        "file={data} rows={} terms={}",
        reader.row_count(),
        reader.term_count()
    );
    println!(
        "bloom_only_fields={:?}",
        reader.bloom_only_fields().collect::<Vec<_>>()
    );
    for f in [
        "duration",
        "trace_id",
        "span_id",
        "http.url",
        "service_pod_name",
    ] {
        println!(
            "field {f:?}: term_capability={} partial={}",
            reader.has_term_capability(f),
            reader.partial_fields().contains(f)
        );
    }
    if let Ok(counts) = reader.term_counts_by_field() {
        for (name, count) in counts
            .iter()
            .filter(|(n, _)| n.contains("duration") || n.contains("trace"))
        {
            println!("dict terms: {name:?} = {count}");
        }
    }
    if let Ok(Some(blooms)) = reader.file_blooms() {
        for b in &blooms {
            println!(
                "bloom section field={:?} num_blocks={} n_items={} bytes={}",
                b.field,
                b.num_blocks,
                b.n_items,
                b.bytes.len()
            );
        }
    }
}

// ---------- M17: docs widen plan (gen-1 encode-once) ----------

/// M17 plan edges: identity, null-widening, and every refusal class. The
/// end-to-end chunk surgery (widened chunks round-tripping through a real
/// merge) is pinned in core_writer's gen1_docs_copy tests; here the plan
/// itself is held to its contract.
#[test]
fn m17_docs_widen_plan_edges() {
    use arrow::datatypes::{DataType, Field, Schema};

    use crate::docs_widen_plan;
    let ts = || Field::new(crate::TIMESTAMP_COL_NAME, DataType::Int64, false);
    let src = || Field::new(crate::SOURCE_COL_NAME, DataType::Utf8, false);
    let output = Schema::new(vec![
        ts(),
        Field::new("code", DataType::Int64, true),
        Field::new("region", DataType::Utf8, true),
        Field::new("svc", DataType::Utf8, true),
        src(),
    ]);

    // identity: same fields, arrow string REPRESENTATION differences erase
    // at the vortex dtype (Utf8View == Utf8 stored)
    let identity = docs_widen_plan(
        &Schema::new(vec![
            ts(),
            Field::new("code", DataType::Int64, true),
            Field::new("region", DataType::Utf8View, true),
            Field::new("svc", DataType::Utf8, true),
            src(),
        ]),
        &output,
    )
    .expect("identity plan");
    assert!(identity.is_identity());
    assert_eq!(identity.null_columns(), 0);

    // widening: a strict subset synthesizes the missing nullable columns
    let widen = docs_widen_plan(
        &Schema::new(vec![ts(), Field::new("svc", DataType::Utf8, true), src()]),
        &output,
    )
    .expect("widen plan");
    assert!(!widen.is_identity());
    assert_eq!(widen.null_columns(), 2, "code + region synthesize");

    // type flip: a shared column stored under a different dtype refuses
    let flip = docs_widen_plan(
        &Schema::new(vec![ts(), Field::new("code", DataType::Utf8, true), src()]),
        &output,
    );
    assert!(
        flip.unwrap_err().contains("type widening is a re-encode"),
        "type flips must refuse"
    );

    // an input column the output would drop refuses
    let extra = docs_widen_plan(
        &Schema::new(vec![ts(), Field::new("zonly", DataType::Utf8, true), src()]),
        &output,
    );
    assert!(extra.unwrap_err().contains("absent from the output schema"));

    // a missing NON-NULLABLE output column refuses (nothing can null-fill)
    let missing_src = docs_widen_plan(&Schema::new(vec![ts()]), &output);
    assert!(missing_src.unwrap_err().contains("non-nullable"));
}

/// M17 chunk surgery: widen real encoded chunks (scanned off a stored docs
/// blob) into a wider union and read the rows back — moved columns
/// byte-survive (stored form untouched), synthesized columns read as nulls,
/// and the widened chunks satisfy the wider writer's encoded-run dtype.
#[test]
fn m17_widen_chunks_roundtrip() {
    use arrow::{
        array::{Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };

    use crate::{VixDocs, docs_widen_plan};

    // narrow input file: _timestamp + svc (+_source)
    let schema = Arc::new(Schema::new(vec![
        Field::new(crate::TIMESTAMP_COL_NAME, DataType::Int64, false),
        Field::new("svc", DataType::Utf8, true),
    ]));
    let n = 4096;
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(
                (0..n).map(|i| 1_000_000 - i as i64).collect::<Vec<_>>(),
            )) as _,
            Arc::new(StringArray::from(
                (0..n).map(|i| format!("svc-{}", i % 7)).collect::<Vec<_>>(),
            )) as _,
        ],
    )
    .unwrap();
    let source: StringArray = (0..n)
        .map(|i| Some(format!("{{\"svc\":\"svc-{}\"}}", i % 7)))
        .collect();
    let mut writer = VixWriter::new(
        &schema,
        VixWriterOptions {
            index_enabled: false,
            ..Default::default()
        },
        false,
    );
    writer
        .push_batch_with_source(&batch, &source, None)
        .unwrap();
    let (data, _) = writer.finish().unwrap();
    let docs = VixDocs::open(Bytes::from(data)).unwrap();

    // the output union adds `code` (Int64) and `region` (Utf8)
    let out_schema = Schema::new(vec![
        Field::new(crate::TIMESTAMP_COL_NAME, DataType::Int64, false),
        Field::new("code", DataType::Int64, true),
        Field::new("region", DataType::Utf8, true),
        Field::new("svc", DataType::Utf8, true),
        Field::new(crate::SOURCE_COL_NAME, DataType::Utf8, false),
    ]);
    let plan = docs_widen_plan(docs.schema(), &out_schema).unwrap();
    assert!(!plan.is_identity());
    assert_eq!(plan.null_columns(), 2);

    // a passthrough writer over the WIDER schema accepts the widened chunks
    let out_construction = Schema::new(vec![
        Field::new(crate::TIMESTAMP_COL_NAME, DataType::Int64, false),
        Field::new("code", DataType::Int64, true),
        Field::new("region", DataType::Utf8, true),
        Field::new("svc", DataType::Utf8, true),
    ]);
    let mut out = VixWriter::new(
        &out_construction,
        VixWriterOptions {
            index_enabled: false,
            docs_passthrough: true,
            ..Default::default()
        },
        false,
    );
    let stats = docs
        .spliceable_stats()
        .unwrap()
        .expect("input carries spliceable stats");
    let zone: Vec<crate::ZoneEntry> = docs
        .zone_chunks()
        .expect("zone table")
        .iter()
        .map(|z| (z.row_count, z.ts_min, z.ts_max))
        .collect();
    out.begin_docs_encoded_run(
        n as u64,
        1_000_000 - (n as i64 - 1),
        1_000_000,
        &zone,
        &stats,
        Some(&[n as u64]),
    )
    .unwrap();
    docs.scan_docs_encoded_chunks(&mut |chunk| out.push_docs_encoded_chunk(plan.widen(chunk)?))
        .unwrap();
    out.finish_docs_encoded_run().unwrap();
    let (widened_bytes, _) = out.finish().unwrap();
    let widened = VixDocs::open(Bytes::from(widened_bytes)).unwrap();
    assert_eq!(widened.row_count(), n as u64);
    let batches = widened.read_docs(None, None, None).unwrap();
    let mut rows = 0usize;
    for batch in &batches {
        let code = batch.column_by_name("code").unwrap();
        let region = batch.column_by_name("region").unwrap();
        assert_eq!(code.null_count(), batch.num_rows(), "code is all-null");
        assert_eq!(region.null_count(), batch.num_rows(), "region is all-null");
        rows += batch.num_rows();
    }
    assert_eq!(rows, n);
    // moved columns read back exactly
    let svc_batches = widened
        .read_docs(Some(&["svc".to_string()]), None, None)
        .unwrap();
    let mut svc_values: Vec<String> = Vec::new();
    for batch in &svc_batches {
        let column =
            arrow::compute::cast(batch.column_by_name("svc").unwrap(), &DataType::Utf8).unwrap();
        let column = column
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone();
        svc_values.extend((0..column.len()).map(|i| column.value(i).to_string()));
    }
    assert_eq!(svc_values.len(), n);
    for (i, value) in svc_values.iter().enumerate() {
        assert_eq!(value, &format!("svc-{}", i % 7));
    }
}

// ---------- M17 item 4: parallel rebuild index-blob build ----------

/// The rebuild-arm parity pin: `merge_kway_threads` R=1 (the sequential
/// pre-M17 path, bit-for-bit) vs R=8 (field-boundary range partitioning +
/// per-range sinks + the re-cutting assembly) over one writer's own term
/// map must produce BYTE-IDENTICAL outputs — data object AND `.vxi` index
/// sidecar — across a corpus exercising every cell/blob class: many field
/// regions (real split points), dense-elided terms, out-of-row plist
/// pointer cells crossing range boundaries, small postings row blocks
/// (many term batches to re-cut), an fts field, per-field blooms, the
/// composite section, and a #52 bloom-only demotion.
#[test]
fn m17_rebuild_parallel_blob_build_byte_parity() {
    use arrow::{
        array::{ArrayRef as ArrowArrayRef, Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };

    let n = 3000usize;
    let field_count = 20usize;
    let mut fields = vec![Field::new(
        crate::TIMESTAMP_COL_NAME,
        DataType::Int64,
        false,
    )];
    for f in 0..field_count {
        fields.push(Field::new(format!("f{f:02}"), DataType::Utf8, true));
    }
    fields.push(Field::new("num", DataType::Int64, true));
    fields.push(Field::new("dense", DataType::Utf8, true));
    let schema = Arc::new(Schema::new(fields));
    let mut columns: Vec<ArrowArrayRef> = vec![Arc::new(Int64Array::from(
        (0..n).map(|i| 2_000_000 - i as i64).collect::<Vec<_>>(),
    ))];
    for f in 0..field_count {
        columns.push(Arc::new(StringArray::from(
            (0..n)
                .map(|i| {
                    // ~256 distinct per field; some values repeat often
                    // enough to cross the plist threshold, some are nulls
                    (i % 17 != 3).then(|| format!("f{f:02}-value-{:03}", (i * (f + 7)) % 256))
                })
                .collect::<Vec<_>>(),
        )));
    }
    columns.push(Arc::new(Int64Array::from(
        (0..n).map(|i| (i % 97) as i64 * 13).collect::<Vec<_>>(),
    )));
    columns.push(Arc::new(StringArray::from(vec![Some("always"); n])));
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
    let source: StringArray = (0..n)
        .map(|i| {
            Some(format!(
                "{{\"row\":{i},\"text\":\"error request {i} failed\"}}"
            ))
        })
        .collect();

    let build = |kway: usize| -> (Vec<u8>, Vec<u8>) {
        let mut writer = VixWriter::new(
            &schema,
            VixWriterOptions {
                fts_field_names: vec!["f00".to_string()],
                bloom_field_names: vec!["f01".to_string()],
                bloom_composite: true,
                bloom_only_field_names: vec!["f19".to_string()],
                postings_chunk_bytes: 192,
                postings_plist_min_docs: 8,
                merge_kway_threads: kway,
                ..Default::default()
            },
            false,
        );
        writer
            .push_batch_with_source(&batch, &source, None)
            .unwrap();
        let (data, index) = writer.finish().unwrap();
        (data, index.expect("indexed build emits a sidecar"))
    };

    let (data_seq, index_seq) = build(1);
    let (data_par, index_par) = build(8);
    assert_eq!(
        data_seq, data_par,
        "data object must be byte-identical across rebuild blob-build parallelism"
    );
    assert_eq!(
        index_seq.len(),
        index_par.len(),
        ".vxi length differs between R=1 and R=8"
    );
    assert_eq!(
        index_seq, index_par,
        ".vxi must be byte-identical across rebuild blob-build parallelism"
    );

    // digest-level sanity on top of the byte pin: the corpus really
    // exercised the classes the re-cut must preserve
    let reader =
        VixReader::open_with_index(Bytes::from(data_seq), Some(Bytes::from(index_seq))).unwrap();
    assert!(reader.term_count() > 2500, "multi-field term map expected");
    let mut dense_count = 0u64;
    let mut pointer_docs = 0u64;
    reader
        .for_each_term(&mut |_key, doc_count, ids| {
            if doc_count == reader.row_count() {
                dense_count += 1;
            }
            if doc_count >= 8 {
                pointer_docs += 1;
            }
            assert_eq!(doc_count as usize, ids.len());
            Ok(())
        })
        .unwrap();
    assert!(dense_count >= 1, "dense-elided term expected");
    assert!(pointer_docs > 50, "plist-eligible terms expected");
    assert!(
        reader.bloom_only_fields().any(|f| f == "f19"),
        "the #52 demotion must survive"
    );
}

// ---------- M17 item 2: composite bloom hashing off encoded chunks ----------

/// The byte-equality pin: hashing a demoted field's values off its ENCODED
/// chunks (dict → dictionary-only decode, each referenced distinct value
/// once; FSST → one bulk decompress, raw slices; other → canonical
/// per-row) must produce the EXACT hash sets — and therefore byte-identical
/// bloom blobs — as the decoded-column scan, across all three encoding
/// classes with nulls, empty strings and an oversize value.
///
/// Corpus: `dict_col` stores `vortex.dict` (the M12 recipe — 1024 distinct
/// random 32-char strings, no nulls; the BtrBlocks dict probe rejects
/// nullable shapes); `fsst_col` is REBUILT chunk-by-chunk into real
/// `vortex.fsst` through the passthrough writer (this build's compact
/// sampler prefers zstd for every synthetic string shape probed —
/// m17_probe_stored_encodings — but merge inputs legitimately carry FSST
/// from other lineages, M15b's 16M corpus measured exactly that);
/// `zs_col`/`flat_col` stay zstd/constant — the canonical fallback arm.
#[test]
fn m17_bloom_encoded_scan_byte_equality() {
    use arrow::{
        array::{ArrayRef as ArrowArrayRef, Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use vortex::{
        VortexSessionDefault,
        array::{
            IntoArray, VortexSessionExecute,
            arrays::{Struct, StructArray as VStructArray, struct_::StructArrayExt},
            validity::Validity,
        },
        encodings::fsst::{fsst_compress, fsst_train_compressor},
        io::{
            runtime::{BlockingRuntime, single::SingleThreadRuntime},
            session::RuntimeSessionExt,
        },
        session::VortexSession,
    };

    use crate::VixDocs;

    let n = 8192usize;
    let mut rng = StdRng::seed_from_u64(0x0121_2012);
    let mut rand_str = |len: usize| -> String {
        (0..len)
            .map(|_| char::from(b'a' + (rng.random::<u8>() % 26)))
            .collect()
    };
    let pool: Vec<String> = (0..1024).map(|_| rand_str(32)).collect();
    let dict_col: Vec<Option<String>> = (0..n).map(|i| Some(pool[i % 1024].clone())).collect();
    let oversize = "z".repeat(70_000);
    let fsst_col: Vec<Option<String>> = (0..n)
        .map(|i| {
            if i % 29 == 7 {
                None
            } else if i == 1234 {
                Some(oversize.clone()) // over max_raw_term_len: skipped by policy
            } else if i == 2345 {
                Some(String::new()) // empty string: hashed (policy keeps it)
            } else {
                Some(format!("{}-{i:08}", rand_str(24)))
            }
        })
        .collect();
    let zs_col: Vec<Option<String>> = (0..n)
        .map(|i| (i % 13 != 5).then(|| format!("{}-{}", rand_str(16), i % 41)))
        .collect();
    let flat_col: Vec<Option<&str>> = (0..n).map(|_| Some("constant-value")).collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new(crate::TIMESTAMP_COL_NAME, DataType::Int64, false),
        Field::new("dict_col", DataType::Utf8, true),
        Field::new("flat_col", DataType::Utf8, true),
        Field::new("fsst_col", DataType::Utf8, true),
        Field::new("zs_col", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(
                (0..n).map(|i| 3_000_000 - i as i64).collect::<Vec<_>>(),
            )) as ArrowArrayRef,
            Arc::new(StringArray::from(dict_col)),
            Arc::new(StringArray::from(flat_col)),
            Arc::new(StringArray::from(fsst_col)),
            Arc::new(StringArray::from(zs_col)),
        ],
    )
    .unwrap();
    let source: StringArray = (0..n).map(|i| Some(format!("{{\"row\":{i}}}"))).collect();
    let mut data_writer = VixWriter::new(
        &schema,
        VixWriterOptions {
            index_enabled: false,
            ..Default::default()
        },
        false,
    );
    data_writer
        .push_batch_with_source(&batch, &source, None)
        .unwrap();
    let (data, _) = data_writer.finish().unwrap();
    let first = VixDocs::open(Bytes::from(data)).unwrap();

    // chunk surgery: re-encode fsst_col's stored chunks into REAL
    // vortex.fsst and store the file through the passthrough writer (the
    // shape a merge input from an FSST-writing lineage arrives in)
    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let mut pass = VixWriter::new(
        &schema,
        VixWriterOptions {
            index_enabled: false,
            docs_passthrough: true,
            ..Default::default()
        },
        false,
    );
    let stats = first
        .spliceable_stats()
        .unwrap()
        .expect("first-encode file carries stats");
    let zone: Vec<crate::ZoneEntry> = first
        .zone_chunks()
        .expect("zone table")
        .iter()
        .map(|z| (z.row_count, z.ts_min, z.ts_max))
        .collect();
    pass.begin_docs_encoded_run(
        n as u64,
        3_000_000 - (n as i64 - 1),
        3_000_000,
        &zone,
        &stats,
        Some(&[n as u64]),
    )
    .unwrap();
    let mut fsst_rebuilt = 0usize;
    first
        .scan_docs_encoded_chunks(&mut |chunk| {
            let sa = chunk
                .array
                .as_typed::<Struct>()
                .expect("struct chunk")
                .clone();
            let names = sa.names().clone();
            let rows = chunk.rows();
            let fields: Vec<vortex::array::ArrayRef> = sa
                .unmasked_fields()
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    if names[index].as_ref() != "fsst_col" {
                        return field.clone();
                    }
                    let mut ctx = session.create_execution_ctx();
                    let canonical = field
                        .clone()
                        .execute::<vortex::array::Canonical>(&mut ctx)
                        .expect("canonicalize fsst_col")
                        .into_array();
                    let compressor =
                        fsst_train_compressor(&canonical, &mut ctx).expect("train fsst");
                    let rebuilt = fsst_compress(&canonical, &compressor, &mut ctx)
                        .expect("fsst compress")
                        .into_array();
                    fsst_rebuilt += 1;
                    rebuilt
                })
                .collect();
            let rebuilt =
                VStructArray::try_new(names, fields, rows, Validity::NonNullable).unwrap();
            pass.push_docs_encoded_chunk(crate::EncodedDocsChunk::for_tests(
                rebuilt.into_array(),
                rows,
            ))
        })
        .unwrap();
    assert!(fsst_rebuilt >= 1, "surgery must have rebuilt fsst_col");
    pass.finish_docs_encoded_run().unwrap();
    let (data, _) = pass.finish().unwrap();
    let docs = VixDocs::open(Bytes::from(data)).unwrap();

    let fields = vec![
        "dict_col".to_string(),
        "flat_col".to_string(),
        "fsst_col".to_string(),
        "zs_col".to_string(),
    ];
    let make_writer = || {
        VixWriter::new(
            &schema,
            VixWriterOptions {
                bloom_composite: true,
                bloom_only_field_names: fields.clone(),
                ..Default::default()
            },
            false,
        )
    };
    let writer_enc = make_writer();
    let mut hasher_enc = writer_enc.bloom_only_hasher(&fields);
    let mut census = crate::BloomEncodingCensus::default();
    for field in &fields {
        census.absorb(
            docs.hash_bloom_only_encoded(&mut hasher_enc, field)
                .unwrap(),
        );
    }
    assert!(
        census.dict_chunks >= 1,
        "the dict corpus must exercise the dict arm: {census:?}"
    );
    assert!(
        census.fsst_chunks >= 1,
        "the FSST-rebuilt column must exercise the FSST arm: {census:?}"
    );
    assert!(
        census.other_chunks >= 2,
        "zstd + constant columns ride the canonical arm: {census:?}"
    );

    let writer_dec = make_writer();
    let mut hasher_dec = writer_dec.bloom_only_hasher(&fields);
    docs.scan_docs(Some(&fields), None, None, &mut |batch| {
        let columns: Vec<(String, ArrowArrayRef)> = fields
            .iter()
            .map(|name| {
                (
                    name.clone(),
                    Arc::clone(batch.column_by_name(name).unwrap()),
                )
            })
            .collect();
        hasher_dec.hash_columns(&columns);
        Ok(())
    })
    .unwrap();

    let sets_enc = hasher_enc.hash_sets();
    let sets_dec = hasher_dec.hash_sets();
    for (name, dec) in &sets_dec {
        assert!(!dec.is_empty(), "{name}: decoded scan hashed nothing?");
    }
    assert_eq!(
        sets_enc, sets_dec,
        "encoded-chunk coverage must equal the decoded scan hash-for-hash"
    );

    // blob-level byte equality: identical pushes + the respective hashers
    // folded in must finish byte-identical outputs (bloom blob included)
    let build_with = |hasher: crate::BloomOnlyHasher| -> (Vec<u8>, Vec<u8>) {
        let mut writer = make_writer();
        writer
            .push_batch_with_source(&batch, &source, None)
            .unwrap();
        writer.absorb_bloom_only_hashes(hasher);
        let (data, index) = writer.finish().unwrap();
        (data, index.expect("indexed build emits a sidecar"))
    };
    let (data_a, index_a) = build_with(hasher_enc);
    let (data_b, index_b) = build_with(hasher_dec);
    assert_eq!(
        data_a, data_b,
        "data bytes must not depend on the scan path"
    );
    assert_eq!(
        index_a, index_b,
        ".vxi (bloom blob included) must be byte-identical across scan paths"
    );
}

#[test]
#[ignore]
fn m17_probe_stored_encodings() {
    use arrow::{
        array::{ArrayRef as ArrowArrayRef, Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    let n: usize = std::env::var("M17_PROBE_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8192);
    let mut rng = StdRng::seed_from_u64(0x0121_2012);
    let mut rand_str = |len: usize| -> String {
        (0..len)
            .map(|_| char::from(b'a' + (rng.random::<u8>() % 26)))
            .collect()
    };
    let pool: Vec<String> = (0..1024).map(|_| rand_str(32)).collect();
    let dict_col: Vec<Option<String>> = (0..n).map(|i| Some(pool[i % 1024].clone())).collect();
    // scale-dependent
    let dict_nulls: Vec<Option<String>> = (0..n)
        .map(|i| (i % 13 != 5).then(|| pool[i % 1024].clone()))
        .collect();
    let id24: Vec<Option<String>> = (0..n)
        .map(|i| Some(format!("{}-{i:08}", rand_str(24))))
        .collect();
    let mut hex_str = {
        let mut hex_rng = StdRng::seed_from_u64(0xFEED);
        move || -> String {
            (0..32)
                .map(|_| char::from_digit(u32::from(hex_rng.random::<u8>() % 16), 16).unwrap())
                .collect()
        }
    };
    let hex32: Vec<Option<String>> = (0..n).map(|_| Some(hex_str())).collect();
    let long64: Vec<Option<String>> = (0..n).map(|_| Some(rand_str(64))).collect();
    let prefixed: Vec<Option<String>> = (0..n)
        .map(|i| Some(format!("service-pod-name-{}-{i}", rand_str(12))))
        .collect();
    let schema = Arc::new(Schema::new(vec![
        Field::new(crate::TIMESTAMP_COL_NAME, DataType::Int64, false),
        Field::new("dict_col", DataType::Utf8, true),
        Field::new("dict_nulls", DataType::Utf8, true),
        Field::new("id24", DataType::Utf8, true),
        Field::new("hex32", DataType::Utf8, true),
        Field::new("long64", DataType::Utf8, true),
        Field::new("prefixed", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(
                (0..n).map(|i| 3_000_000 - i as i64).collect::<Vec<_>>(),
            )) as ArrowArrayRef,
            Arc::new(StringArray::from(dict_col)),
            Arc::new(StringArray::from(dict_nulls)),
            Arc::new(StringArray::from(id24)),
            Arc::new(StringArray::from(hex32)),
            Arc::new(StringArray::from(long64)),
            Arc::new(StringArray::from(prefixed)),
        ],
    )
    .unwrap();
    let source: StringArray = (0..n).map(|i| Some(format!("{{\"row\":{i}}}"))).collect();
    let mut w = VixWriter::new(
        &schema,
        VixWriterOptions {
            index_enabled: false,
            ..Default::default()
        },
        false,
    );
    w.push_batch_with_source(&batch, &source, None).unwrap();
    let (data, _) = w.finish().unwrap();
    let docs = crate::VixDocs::open(Bytes::from(data)).unwrap();
    for name in [
        "dict_col",
        "dict_nulls",
        "id24",
        "hex32",
        "long64",
        "prefixed",
    ] {
        crate::container::probe_column_encodings(&docs, name);
    }
}

/// M18 pin: the vortex 0.79 behavior the M18 guards exist for. Slicing a
/// `vortex.runend` array keeps a runtime `vortex.slice` wrapper (runend
/// registers only an EXECUTE-time slice kernel, no static reduce rule), and
/// that wrapper is neither in the file writer's allowed-encoding set nor in
/// the session array registry — serializing it is exactly the prod .110
/// "Array encoding vortex.slice not permitted by ctx". If a vortex upgrade
/// makes this pin fail, the guard's trigger changed and M18's docs need a
/// second look (the guard itself stays sound either way).
#[test]
fn m18_runend_slice_keeps_wrapper_the_write_ctx_rejects() {
    use vortex::{
        VortexSessionDefault,
        array::{
            IntoArray, VortexSessionExecute,
            arrays::{PrimitiveArray, Slice},
            session::ArraySessionExt,
            validity::Validity,
        },
        buffer::buffer,
        encodings::runend::RunEnd,
        session::VortexSession,
    };

    use crate::container::is_ctx_serializable;

    let session = VortexSession::default();
    let mut ctx = session.create_execution_ctx();
    let ends = PrimitiveArray::new(buffer![4u32, 9, 16], Validity::NonNullable).into_array();
    let values = PrimitiveArray::new(buffer![10i64, 20, 30], Validity::NonNullable).into_array();
    let runend = RunEnd::new(ends, values, &mut ctx).into_array();
    assert!(
        is_ctx_serializable(&runend),
        "a whole runend chunk is a writable stored form"
    );

    let sliced = runend.slice(2..12).unwrap();
    assert!(
        sliced
            .depth_first_traversal()
            .any(|node| node.is::<Slice>()),
        "slicing runend must keep the lazy vortex.slice wrapper (no static reduce rule)"
    );
    assert!(
        !is_ctx_serializable(&sliced),
        "the wrapper is outside the file writer's allowed-encoding set"
    );
    assert!(
        session
            .arrays()
            .registry()
            .find(&sliced.encoding_id())
            .is_none(),
        "vortex.slice is not in the session array registry — interning it fails with \
         'not permitted by ctx'"
    );
}

/// M18 pin (THE prod corruption shape, scan side): a first-encode file
/// whose narrow columns store COARSER chunks than `_source`'s yields every
/// scan window with those columns sliced out of one stored leaf. Sliced
/// forms do not survive a serialize round-trip (reduced slices drop their
/// offset — pre-M18 this exact corpus re-read 126,100 of 131,072 `status`
/// values WRONG after a verbatim copy; wrapper slices error the encoder).
/// The M18 deterministic slice guard must canonicalize exactly those
/// column-windows (counted), the copy must complete without the write-side
/// fail-open firing, and the copied file must read back row-exact.
#[test]
fn m18_sliced_scan_canonicalizes_and_copies_row_exact() {
    use vortex::{
        array::arrays::{Struct, struct_::StructArrayExt},
        encodings::zstd::Zstd,
    };

    use crate::{SOURCE_COL_NAME, VixDocs, container::is_ctx_serializable};

    let rows = 131_072usize;
    let schema = Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("status", DataType::Int64, true),
        Field::new("level", DataType::Utf8, true),
    ]);
    let ts: Vec<i64> = (0..rows).map(|i| 5_000_000 - i as i64).collect();
    // 5..=12-row runs of random values: numeric enough for a compressed
    // single-chunk column, misaligned with _source's fine byte-budget grid
    let mut rng = StdRng::seed_from_u64(0x18_18);
    let status: Vec<i64> = {
        let mut out = Vec::with_capacity(rows);
        while out.len() < rows {
            let run = 5 + (rng.random::<u8>() % 8) as usize;
            let value: i64 = rng.random::<i64>() >> 8;
            for _ in 0..run.min(rows - out.len()) {
                out.push(value);
            }
        }
        out
    };
    let level: Vec<&str> = (0..rows)
        .map(|i| ["info", "warn", "error", "debug"][(i / 512) % 4])
        .collect();
    let sources: Vec<String> = (0..rows)
        .map(|i| {
            format!(
                r#"{{"_timestamp":{},"status":{},"level":"{}","pad":"{}"}}"#,
                ts[i],
                status[i],
                level[i],
                "x".repeat(160)
            )
        })
        .collect();
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int64Array::from(ts.clone())) as ArrayRef,
            Arc::new(Int64Array::from(status.clone())) as ArrayRef,
            Arc::new(StringArray::from(level.clone())) as ArrayRef,
        ],
    )
    .unwrap();
    let mut writer = VixWriter::new(
        &schema,
        VixWriterOptions {
            index_enabled: false,
            docs_chunk_bytes: 256 * 1024,
            ..Default::default()
        },
        false,
    );
    writer
        .push_batch_with_source(&batch, &StringArray::from(sources.clone()), None)
        .unwrap();
    let (data, _) = writer.finish().unwrap();
    let docs = VixDocs::open(Bytes::from(data)).unwrap();

    // the copy: every yielded chunk is fully ctx-serializable, the guard
    // canonicalized the misaligned column-windows (counted), and the
    // write-side fail-open never fires
    let entries: Vec<crate::ZoneEntry> = docs
        .zone_chunks()
        .unwrap()
        .iter()
        .map(|zone| (zone.row_count, zone.ts_min, zone.ts_max))
        .collect();
    let stats = docs.spliceable_stats().unwrap().unwrap();
    let mut out = VixWriter::new(
        &schema,
        VixWriterOptions {
            index_enabled: false,
            docs_passthrough: true,
            concat_row_order: true,
            ..Default::default()
        },
        false,
    );
    out.begin_docs_encoded_run(
        rows as u64,
        *ts.last().unwrap(),
        ts[0],
        &entries,
        &stats,
        None,
    )
    .unwrap();
    let mut chunks = 0usize;
    let sliced_windows = docs
        .scan_docs_encoded_chunks(&mut |chunk| {
            chunks += 1;
            assert!(
                is_ctx_serializable(&chunk.array),
                "the scan must never yield a chunk the write context cannot serialize"
            );
            out.push_docs_encoded_chunk(chunk)
        })
        .unwrap();
    assert!(chunks > 1, "the corpus must split into multiple windows");
    assert!(
        sliced_windows > 0,
        "the coarse narrow columns must be detected as sliced (guard fired)"
    );
    out.finish_docs_encoded_run().unwrap();
    let failopen = out.docs_failopen_counter();
    let (out_data, _) = out.finish().unwrap();
    assert_eq!(
        failopen.load(std::sync::atomic::Ordering::Relaxed),
        0,
        "the scan-side guard must catch everything before the writer's backstop"
    );

    // row-exact read-back — THE corruption pin (pre-M18: 126,100 of
    // 131,072 status values wrong)
    let out_docs = VixDocs::open(Bytes::from(out_data)).unwrap();
    let read_all = |docs: &VixDocs| -> Vec<(i64, i64, String, String)> {
        let mut rows_out = Vec::new();
        for batch in docs.read_docs(None, None, None).unwrap() {
            let ts = arrow::compute::cast(
                batch.column_by_name("_timestamp").unwrap(),
                &DataType::Int64,
            )
            .unwrap();
            let ts = ts.as_any().downcast_ref::<Int64Array>().unwrap().clone();
            let st =
                arrow::compute::cast(batch.column_by_name("status").unwrap(), &DataType::Int64)
                    .unwrap();
            let st = st.as_any().downcast_ref::<Int64Array>().unwrap().clone();
            let lv = arrow::compute::cast(batch.column_by_name("level").unwrap(), &DataType::Utf8)
                .unwrap();
            let lv = lv.as_any().downcast_ref::<StringArray>().unwrap().clone();
            let sr = arrow::compute::cast(
                batch.column_by_name(SOURCE_COL_NAME).unwrap(),
                &DataType::Utf8,
            )
            .unwrap();
            let sr = sr.as_any().downcast_ref::<StringArray>().unwrap().clone();
            for i in 0..batch.num_rows() {
                rows_out.push((
                    ts.value(i),
                    st.value(i),
                    lv.value(i).to_string(),
                    sr.value(i).to_string(),
                ));
            }
        }
        rows_out
    };
    let input_rows = read_all(&docs);
    assert_eq!(input_rows.len(), rows);
    for (i, row) in input_rows.iter().enumerate() {
        assert_eq!(row.0, ts[i], "input file must read back the original rows");
        assert_eq!(row.1, status[i]);
    }
    let output_rows = read_all(&out_docs);
    assert_eq!(
        input_rows, output_rows,
        "the copied file must be row-exact (positions AND values)"
    );

    // the passthrough win survives: `_source` (the finest grid — never
    // sliced) keeps its stored zstd form in the output
    let mut source_zstd = 0usize;
    out_docs
        .scan_docs_encoded_chunks(&mut |chunk| {
            let sa = chunk.array.as_typed::<Struct>().unwrap().clone();
            let field = sa.unmasked_field_by_name(SOURCE_COL_NAME).unwrap();
            source_zstd += usize::from(field.depth_first_traversal().any(|node| node.is::<Zstd>()));
            Ok(())
        })
        .unwrap();
    assert!(
        source_zstd > 0,
        "_source must still copy verbatim (stored zstd form) — the aligned column keeps the \
         passthrough win"
    );
}

/// M18 pin (write-side per-chunk fail-open, the STRUCTURAL layer): a
/// hand-built `Slice(RunEnd)` column chunk — the exact prod .110 shape,
/// large enough (> the clustered 16KiB coalesce threshold) to take the
/// verbatim Ready path — pushed through the REAL passthrough writer must
/// NOT error the encode (pre-M18: "vortex.slice not permitted by ctx" at
/// finish, restarting the whole merge). The strategy canonicalizes + re-
/// encodes THAT column chunk only, counts it, and the stored rows read
/// back exactly (same rows at the same positions — the doc-id/row-position
/// invariant across a substituted chunk). A wrapper-free control copy of
/// the same rows keeps the runend stored form verbatim with a zero count.
#[test]
fn m18_writer_failopen_reencodes_slice_wrapped_chunk() {
    use vortex::{
        array::{
            IntoArray, VortexSessionExecute,
            arrays::{PrimitiveArray, SliceArray, StructArray},
            validity::Validity,
        },
        arrow::{FromArrowArray, FromArrowType},
        buffer::Buffer,
        dtype::DType,
        encodings::runend::RunEnd,
        session::VortexSession,
    };

    use crate::{SOURCE_COL_NAME, VixDocs};

    let rows = 65_536usize;
    let schema = Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("status", DataType::Int64, true),
    ]);
    let ts: Vec<i64> = (0..rows).map(|i| 9_000_000 - i as i64).collect();

    // hand-built runend over rows+pad rows, 8-row runs of distinct values;
    // the Slice takes the middle `rows` — its expected decoded values:
    let pad = 16usize;
    let nruns = (rows + pad) / 8;
    let run_values: Vec<i64> = (0..nruns as i64).map(|i| i * 7 + 3).collect();
    let expected_status: Vec<i64> = (0..rows).map(|i| run_values[(i + 8) / 8]).collect();

    let session = {
        use vortex::VortexSessionDefault;
        VortexSession::default()
    };
    let mut ctx = session.create_execution_ctx();
    let ends: Vec<u32> = (1..=nruns as u32).map(|i| i * 8).collect();
    let ends = PrimitiveArray::new(Buffer::copy_from(ends), Validity::NonNullable).into_array();
    let values = <vortex::array::ArrayRef as FromArrowArray<_>>::from_arrow(
        &Int64Array::from(run_values.clone()),
        true,
    )
    .unwrap();
    let runend = RunEnd::new(ends, values, &mut ctx).into_array();
    assert!(
        runend.nbytes() > crate::clustered::COALESCE_MAX_ENCODED_BYTES,
        "the fixture must exceed the coalesce threshold to take the verbatim Ready path"
    );

    let source: Vec<String> = (0..rows).map(|i| format!("{{\"row\":{i}}}")).collect();
    let docs_schema = Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("status", DataType::Int64, true),
        Field::new(SOURCE_COL_NAME, DataType::Utf8, false),
    ]);
    let names: vortex::dtype::FieldNames = vec![
        vortex::dtype::FieldName::from("_timestamp"),
        vortex::dtype::FieldName::from("status"),
        vortex::dtype::FieldName::from(SOURCE_COL_NAME),
    ]
    .into();
    let build_chunk = |status_field: vortex::array::ArrayRef| -> vortex::array::ArrayRef {
        let ts_field = <vortex::array::ArrayRef as FromArrowArray<_>>::from_arrow(
            &Int64Array::from(ts.clone()),
            false,
        )
        .unwrap();
        let src_field = <vortex::array::ArrayRef as FromArrowArray<_>>::from_arrow(
            &StringArray::from(source.clone()),
            false,
        )
        .unwrap();
        StructArray::try_new(
            names.clone(),
            vec![ts_field, status_field, src_field],
            rows,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array()
    };

    let copy = |status_field: vortex::array::ArrayRef| -> (u64, Vec<u8>) {
        let chunk = build_chunk(status_field);
        assert_eq!(chunk.dtype(), &DType::from_arrow(&docs_schema));
        let mut writer = VixWriter::new(
            &schema,
            VixWriterOptions {
                index_enabled: false,
                docs_passthrough: true,
                concat_row_order: true,
                ..Default::default()
            },
            false,
        );
        writer
            .begin_docs_encoded_run(
                rows as u64,
                *ts.last().unwrap(),
                ts[0],
                &[(rows as u64, *ts.last().unwrap(), ts[0])],
                &crate::SpliceableStats::default(),
                None,
            )
            .unwrap();
        writer
            .push_docs_encoded_chunk(crate::docs::EncodedDocsChunk::for_tests(chunk, rows))
            .expect("M18: a wrapper chunk must not error the push");
        writer.finish_docs_encoded_run().unwrap();
        let failopen = writer.docs_failopen_counter();
        let (data, _) = writer
            .finish()
            .expect("M18: a wrapper chunk must not error the encode (per-chunk fail-open)");
        (failopen.load(std::sync::atomic::Ordering::Relaxed), data)
    };

    // (1) the wrapper shape: per-chunk fail-open fires exactly once (one
    // column chunk), the file finishes, rows land at the same positions
    let sliced = SliceArray::try_new(runend.clone(), 8..rows + 8)
        .unwrap()
        .into_array();
    let (failopen, data) = copy(sliced);
    assert_eq!(failopen, 1, "exactly the wrapped column chunk re-encodes");
    let out = VixDocs::open(Bytes::from(data)).unwrap();
    let mut got: Vec<(i64, i64)> = Vec::new();
    for batch in out.read_docs(None, None, None).unwrap() {
        let ts_col = arrow::compute::cast(
            batch.column_by_name("_timestamp").unwrap(),
            &DataType::Int64,
        )
        .unwrap();
        let ts_col = ts_col
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone();
        let st = arrow::compute::cast(batch.column_by_name("status").unwrap(), &DataType::Int64)
            .unwrap();
        let st = st.as_any().downcast_ref::<Int64Array>().unwrap().clone();
        for i in 0..batch.num_rows() {
            got.push((ts_col.value(i), st.value(i)));
        }
    }
    assert_eq!(got.len(), rows);
    for (i, (got_ts, got_status)) in got.iter().enumerate() {
        assert_eq!(*got_ts, ts[i], "row {i}: _timestamp position must be exact");
        assert_eq!(
            *got_status, expected_status[i],
            "row {i}: the re-encoded chunk must hold the same rows at the same positions"
        );
    }

    // (2) control: the same rows as a WHOLE runend chunk (no wrapper) copy
    // verbatim — zero fail-opens, runend stored form preserved
    let whole = runend.slice(8..rows + 8).unwrap();
    let mut ctx2 = session.create_execution_ctx();
    use vortex::array::Canonical;
    let whole_canonical = whole.execute::<Canonical>(&mut ctx2).unwrap().into_array();
    // re-encode canonically into a self-contained runend chunk
    let whole_runend = vortex::encodings::runend::RunEnd::encode(whole_canonical, &mut ctx2)
        .expect("runend encode")
        .into_array();
    let (failopen, data) = copy(whole_runend);
    assert_eq!(failopen, 0, "a wrapper-free encoded chunk copies verbatim");
    let out = VixDocs::open(Bytes::from(data)).unwrap();
    let mut saw_runend = false;
    out.scan_docs_encoded_chunks(&mut |chunk| {
        saw_runend |= chunk
            .array
            .depth_first_traversal()
            .any(|node| node.is::<vortex::encodings::runend::RunEnd>());
        Ok(())
    })
    .unwrap();
    assert!(
        saw_runend,
        "the control copy must keep the runend stored form (the passthrough win)"
    );
}
