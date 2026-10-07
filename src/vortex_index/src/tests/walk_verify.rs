// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! A `Contains` / `Regex` conjunct on a term field with a LARGE dictionary
//! is verified against the candidates' docs column values instead of
//! walking every distinct value of the field, when the other conjuncts
//! leave few candidates (`VixReader::eval_and`, wave 2). Both routes must
//! agree bit for bit, and the verified route must not read the field's
//! dictionary.

use std::{ops::Range, sync::LazyLock};

use futures::future::BoxFuture;
use parking_lot::Mutex;

use super::*;
use crate::{VixRangeSource, container};

/// Record every physical read of one object.
struct LoggedSource {
    bytes: Bytes,
    reads: Mutex<Vec<Range<u64>>>,
}

impl LoggedSource {
    fn new(bytes: Bytes) -> Arc<Self> {
        Arc::new(Self {
            bytes,
            reads: Mutex::new(Vec::new()),
        })
    }

    fn bytes_within(&self, window: &Range<u64>) -> u64 {
        self.reads
            .lock()
            .iter()
            .map(|range| {
                range
                    .end
                    .min(window.end)
                    .saturating_sub(range.start.max(window.start))
            })
            .sum()
    }

    fn read_count(&self) -> usize {
        self.reads.lock().len()
    }
}

impl VixRangeSource for LoggedSource {
    fn len(&self) -> u64 {
        self.bytes.len() as u64
    }

    fn fetch(&self, range: Range<u64>) -> BoxFuture<'static, anyhow::Result<Bytes>> {
        let result = self.fetch_many(vec![range]);
        Box::pin(async move { Ok(result.await?.remove(0)) })
    }

    fn fetch_many(
        &self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'static, anyhow::Result<Vec<Bytes>>> {
        let mut reads = self.reads.lock();
        let result = ranges
            .iter()
            .map(|range| {
                assert!(range.end <= self.len());
                reads.push(range.clone());
                self.bytes.slice(range.start as usize..range.end as usize)
            })
            .collect();
        Box::pin(async move { Ok(result) })
    }
}

const ROWS: usize = 40_000;
const NEEDLE: &str = "asagent1";

struct Fixture {
    data: Bytes,
    index: Bytes,
    service: Vec<&'static str>,
    body: Vec<Option<String>>,
}

impl Fixture {
    /// Rows where `service` equals `service` and the body (a null body
    /// never matches) satisfies `matches`.
    fn expect(&self, service: &str, matches: impl Fn(&str) -> bool) -> Vec<usize> {
        (0..ROWS)
            .filter(|&row| {
                self.service[row] == service && self.body[row].as_deref().is_some_and(&matches)
            })
            .collect()
    }
}

/// `body` is near-unique (~290 raw bytes per row, ~11 MB of dictionary
/// across ~170 blocks) and incompressible — a per-row pseudo-random hex
/// filler, so the docs column is megabytes too and lies far outside the
/// data object's eager tail (a repetitive filler compressed the whole
/// object into the tail: every docs read was memory, nothing to observe).
/// `service` is `svc-a` on a handful of rows and `svc-b` elsewhere. The
/// needle appears in a few `svc-a` bodies (ASCII upper and lower case, one
/// inside a non-ASCII value — the Unicode fold branch) and in some `svc-b`
/// bodies — the latter must never leak into the `svc-a` result. One
/// `svc-a` row has a NULL body. Small docs chunks give the candidates a few
/// chunks of a many-chunk file, which is what the verification's cost
/// model is for.
fn build() -> Fixture {
    let hex_filler = |row: usize| -> String {
        // splitmix64 over the row, sixteen 16-hex-digit words
        let mut state = row as u64 ^ 0x9E37_79B9_7F4A_7C15;
        (0..16)
            .map(|_| {
                state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
                let mut z = state;
                z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
                z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
                format!("{:016x}", z ^ (z >> 31))
            })
            .collect()
    };
    let mut service = Vec::with_capacity(ROWS);
    let mut body: Vec<Option<String>> = Vec::with_capacity(ROWS);
    for row in 0..ROWS {
        let filler = hex_filler(row);
        let svc = if matches!(
            row,
            1_000 | 1_001 | 1_002 | 1_003 | 13_500 | 13_501 | 22_000
        ) {
            "svc-a"
        } else {
            "svc-b"
        };
        service.push(svc);
        body.push(match row {
            1_000 => Some(format!("ticket {} AsAgent1 opened {filler}", row)),
            1_001 => Some(format!("ticket {} nothing here {filler}", row)),
            1_002 => None,
            1_003 => Some(format!("ÜNÏCODE fold ASAGENT1 {row} {filler}")),
            13_500 => Some(format!("{filler} tail asagent1 {row}")),
            13_501 => Some(format!("{filler} ASAGENT10 is a different agent {row}")),
            22_000 => Some(format!("agent1 but not the whole needle {row} {filler}")),
            // needle rows outside the candidate set
            r if r % 977 == 0 => Some(format!("svc-b row {r} AsAgent1 {filler}")),
            r => Some(format!("payload {r:06} {filler}")),
        });
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("service", DataType::Utf8, false),
        Field::new("body", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from((1..=ROWS as i64).collect::<Vec<_>>())),
            Arc::new(StringArray::from(service.clone())),
            Arc::new(StringArray::from(body.clone())),
        ],
    )
    .unwrap();
    let mut writer = VixWriter::new(
        &schema,
        VixWriterOptions {
            docs_chunk_bytes: 64 * 1024,
            encode_threads: 1,
            ..Default::default()
        },
        false,
    );
    writer
        .push_batch_with_source(&batch, &StringArray::from(vec!["{}"; ROWS]), None)
        .unwrap();
    let (data, index) = writer.finish().unwrap();
    Fixture {
        data: Bytes::from(data),
        index: Bytes::from(index.unwrap()),
        service,
        body,
    }
}

static FIXTURE: LazyLock<Fixture> = LazyLock::new(build);

fn dictionary_range(index: &Bytes) -> Range<u64> {
    puffin::reader::parse_puffin_footer_from_bytes(index)
        .unwrap()
        .blobs
        .iter()
        .find(|blob| {
            blob.properties
                .get("blob_tag")
                .is_some_and(|tag| tag == container::BLOB_TAG_DICT_BLOCKS)
        })
        .unwrap()
        .get_offset(None)
}

/// A ranged reader over the fixture with the verification threshold set
/// (`0` = every walk reads its dictionary), reads cleared after the open.
fn open(walk_verify_min_bytes: u64) -> (VixReader, Arc<LoggedSource>, Arc<LoggedSource>) {
    let data = LoggedSource::new(FIXTURE.data.clone());
    let index = LoggedSource::new(FIXTURE.index.clone());
    let mut reader = VixReader::open_ranged_with_index(data.clone(), Some(index.clone())).unwrap();
    reader.set_walk_verify_min_bytes(walk_verify_min_bytes);
    data.reads.lock().clear();
    index.reads.lock().clear();
    (reader, data, index)
}

fn contains(field: &str, needle: &str, case_insensitive: bool) -> VixQuery {
    VixQuery::Contains {
        field: Some(field.to_string()),
        needle: needle.as_bytes().to_vec(),
        case_insensitive,
    }
}

fn rows_of(bitmap: &BooleanBuffer) -> Vec<usize> {
    bitmap.set_indices().collect()
}

#[test]
fn fixture_dictionary_is_large_and_many_chunked() {
    let (reader, ..) = open(0);
    let body = reader.field_dictionary_bytes("body").unwrap();
    assert!(
        body >= 8 * 1024 * 1024,
        "body dictionary must dwarf the docs footer read: {body} bytes"
    );
    assert!(
        reader.zone_chunks().is_some_and(|chunks| chunks.len() >= 8),
        "the fixture must span many docs chunks: {:?}",
        reader.zone_chunks().map(<[_]>::len)
    );
}

#[test]
fn selective_conjunct_verifies_the_column_instead_of_walking_the_dictionary() {
    let fixture = &*FIXTURE;
    let dictionary = dictionary_range(&fixture.index);
    let query = VixQuery::And(vec![
        exact("service", "svc-a"),
        contains("body", "AsAgent1", true),
    ]);
    let expected = fixture.expect("svc-a", |body| body.to_lowercase().contains(NEEDLE));
    assert_eq!(expected, vec![1_000, 1_003, 13_500, 13_501]);

    let (walk, walk_data, walk_index) = open(0);
    let walked = walk.eval(&query).unwrap();
    assert_eq!(rows_of(&walked), expected);
    let walked_dictionary = walk_index.bytes_within(&dictionary);
    assert!(
        walked_dictionary >= 8 * 1024 * 1024,
        "the walk reads the body dictionary: {walked_dictionary} bytes"
    );
    assert_eq!(
        walk_data.read_count(),
        0,
        "the walk never touches the data object"
    );

    let (verify, verify_data, verify_index) = open(1024 * 1024);
    let verified = verify.eval(&query).unwrap();
    assert_eq!(verified, walked);
    let verified_dictionary = verify_index.bytes_within(&dictionary);
    assert!(
        verified_dictionary <= 256 * 1024,
        "verification reads only the service point blocks of the dictionary: \
         {verified_dictionary} bytes"
    );
    assert!(
        verify_data.read_count() >= 1,
        "verification point-reads the body column from the data object"
    );
    let data_bytes: u64 = verify_data
        .reads
        .lock()
        .iter()
        .map(|range| range.end - range.start)
        .sum();
    assert!(
        data_bytes < walked_dictionary / 2,
        "verification ({data_bytes} data bytes) must undercut the walk ({walked_dictionary} \
         dictionary bytes) by the planned margin"
    );
}

#[test]
fn case_sensitive_and_regex_walks_verify_identically() {
    let fixture = &*FIXTURE;
    let cases = [
        (
            VixQuery::And(vec![
                exact("service", "svc-a"),
                contains("body", "AsAgent1", false),
            ]),
            fixture.expect("svc-a", |body| body.contains("AsAgent1")),
        ),
        (
            VixQuery::And(vec![
                exact("service", "svc-a"),
                VixQuery::Regex {
                    field: Some("body".to_string()),
                    pattern: ".*[aA][sS][aA]gent1 .*".to_string(),
                },
            ]),
            fixture.expect("svc-a", |body| {
                body.contains("AsAgent1 ") || body.contains("asagent1 ")
            }),
        ),
    ];
    for (query, expected) in cases {
        assert!(!expected.is_empty(), "{query:?} must match something");
        let (walk, ..) = open(0);
        let (verify, verify_data, _) = open(1024 * 1024);
        let walked = walk.eval(&query).unwrap();
        let verified = verify.eval(&query).unwrap();
        assert_eq!(rows_of(&walked), expected, "{query:?}");
        assert_eq!(verified, walked, "{query:?}");
        assert!(verify_data.read_count() >= 1, "{query:?} must verify");
    }
}

#[test]
fn broad_conjunct_keeps_the_walk() {
    let fixture = &*FIXTURE;
    let dictionary = dictionary_range(&fixture.index);
    // `svc-b` is nearly every row: the candidates' chunks are the whole
    // column, so verification cannot undercut the walk and is declined
    let query = VixQuery::And(vec![
        exact("service", "svc-b"),
        contains("body", "asagent1", true),
    ]);
    let expected = fixture.expect("svc-b", |body| body.to_lowercase().contains(NEEDLE));
    assert!(expected.len() > 10);
    let (verify, verify_data, verify_index) = open(1024 * 1024);
    let bitmap = verify.eval(&query).unwrap();
    assert_eq!(rows_of(&bitmap), expected);
    assert_eq!(
        verify_data.read_count(),
        0,
        "declined verification reads no column"
    );
    assert!(verify_index.bytes_within(&dictionary) >= 8 * 1024 * 1024);
}

#[test]
fn lone_walk_and_disabled_or_undersized_thresholds_keep_the_walk() {
    let fixture = &*FIXTURE;
    let dictionary = dictionary_range(&fixture.index);
    let lone = contains("body", "asagent1", true);
    let expected: Vec<usize> = (0..ROWS)
        .filter(|&row| {
            fixture.body[row]
                .as_deref()
                .is_some_and(|body| body.to_lowercase().contains(NEEDLE))
        })
        .collect();
    // nothing narrows a lone walk: the dictionary is the only route
    let (verify, verify_data, verify_index) = open(1024 * 1024);
    assert_eq!(rows_of(&verify.eval(&lone).unwrap()), expected);
    assert_eq!(
        rows_of(&verify.eval(&VixQuery::And(vec![lone.clone()])).unwrap()),
        expected
    );
    assert_eq!(verify_data.read_count(), 0);
    assert!(verify_index.bytes_within(&dictionary) >= 8 * 1024 * 1024);

    // a threshold above the field's dictionary leaves the selective shape
    // on the walk too
    let selective = VixQuery::And(vec![exact("service", "svc-a"), lone]);
    let (tall, tall_data, tall_index) = open(64 * 1024 * 1024);
    assert_eq!(
        rows_of(&tall.eval(&selective).unwrap()),
        fixture.expect("svc-a", |body| body.to_lowercase().contains(NEEDLE))
    );
    assert_eq!(tall_data.read_count(), 0);
    assert!(tall_index.bytes_within(&dictionary) >= 8 * 1024 * 1024);
}

#[test]
fn verified_walk_still_eliminates_the_file_and_counts() {
    let fixture = &*FIXTURE;
    // a needle no svc-a body carries: the verified leaf empties the AND
    let query = VixQuery::And(vec![
        exact("service", "svc-a"),
        contains("body", "no such needle anywhere", true),
    ]);
    let (verify, verify_data, _) = open(1024 * 1024);
    assert_eq!(verify.eval(&query).unwrap().count_set_bits(), 0);
    assert!(
        verify_data.read_count() >= 1,
        "the empty answer came from the column"
    );
    // the count path shares the evaluator
    let query = VixQuery::And(vec![
        exact("service", "svc-a"),
        contains("body", NEEDLE, true),
    ]);
    assert_eq!(
        verify.count(&query).unwrap() as usize,
        fixture
            .expect("svc-a", |body| body.to_lowercase().contains(NEEDLE))
            .len()
    );
}
