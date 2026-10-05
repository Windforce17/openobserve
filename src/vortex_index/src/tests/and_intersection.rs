// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! The batched AND intersection (`VixReader::eval_and` →
//! `intersect_leaves`): parity with the in-memory reader across every leaf
//! shape it plans (inline, out-of-row full, out-of-row partial, dense-elided,
//! multi-cell, nested `And`, duplicates, composites) plus the IO contract —
//! one plist wave when every record is read whole, two when skip groups are
//! selected by the accumulator, none when a narrow leaf or a token is absent.

use std::{ops::Range, sync::Arc};

use arrow::{
    array::{ArrayRef, Int64Array, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema},
};
use bytes::Bytes;
use futures::future::BoxFuture;
use parking_lot::Mutex;
use rand::{RngExt, SeedableRng, rngs::StdRng};

use super::{any_token, bits_to_set, exact};
use crate::{
    VixQuery, VixRangeSource, VixReader, VixWriter, VixWriterOptions, container, postings,
};

const ROWS: usize = 200_000;
const PLIST_MIN_DOCS: u32 = 512;
/// Sidecar eager tail small enough that no plist record is tail-resident
/// (the plist blob is the sidecar's first blob, the `svc` records its last),
/// so every postings read is observable.
const INDEX_TAIL: u64 = 4 * 1024;

/// Records every `fetch_many` call (one entry per round trip).
struct LoggedSource {
    bytes: Bytes,
    calls: Mutex<Vec<Vec<Range<u64>>>>,
}

impl LoggedSource {
    fn new(bytes: Bytes) -> Arc<Self> {
        Arc::new(Self {
            bytes,
            calls: Mutex::new(Vec::new()),
        })
    }

    /// Calls touching `blob`, each reduced to its ranges inside the blob.
    fn calls_in(&self, blob: &Range<u64>) -> Vec<Vec<Range<u64>>> {
        self.calls
            .lock()
            .iter()
            .filter_map(|call| {
                let ranges: Vec<Range<u64>> = call
                    .iter()
                    .filter(|range| range.start < blob.end && range.end > blob.start)
                    .cloned()
                    .collect();
                (!ranges.is_empty()).then_some(ranges)
            })
            .collect()
    }
}

impl VixRangeSource for LoggedSource {
    fn len(&self) -> u64 {
        self.bytes.len() as u64
    }

    fn fetch(&self, range: Range<u64>) -> BoxFuture<'static, anyhow::Result<Bytes>> {
        let many = self.fetch_many(vec![range]);
        Box::pin(async move { Ok(many.await?.remove(0)) })
    }

    fn fetch_many(
        &self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'static, anyhow::Result<Vec<Bytes>>> {
        self.calls.lock().push(ranges.clone());
        let out: Vec<Bytes> = ranges
            .iter()
            .map(|range| self.bytes.slice(range.start as usize..range.end as usize))
            .collect();
        Box::pin(async move { Ok(out) })
    }
}

struct Fixture {
    data: Bytes,
    index: Bytes,
    plist: Range<u64>,
    terms: Range<u64>,
    dict_blocks: Range<u64>,
    /// Doc ids of `alpha` / `beta` in `log` (for record-size arithmetic).
    alpha_log: Vec<u32>,
    beta_log: Vec<u32>,
}

/// Rows 50_000 and 150_000 are the two `svc-rare` rows carrying `needle`,
/// `alpha` and `beta`; the other four `svc-rare` rows carry random tokens.
const RARE_ROWS: [usize; 6] = [1_000, 50_000, 99_999, 150_000, 180_000, 199_990];

fn blob_range(index: &Bytes, tag: &str) -> Range<u64> {
    puffin::reader::parse_puffin_footer_from_bytes(index)
        .unwrap()
        .blobs
        .iter()
        .find(|blob| {
            blob.properties
                .get("blob_tag")
                .is_some_and(|value| value == tag)
        })
        .unwrap()
        .get_offset(None)
}

fn build() -> Fixture {
    let mut rng = StdRng::seed_from_u64(0x0a11d);
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("svc", DataType::Utf8, false),
        Field::new("log", DataType::Utf8, false),
        Field::new("extra", DataType::Utf8, false),
    ]));
    let mut writer = VixWriter::new(
        &schema,
        VixWriterOptions {
            fts_field_names: vec!["log".to_string(), "extra".to_string()],
            postings_plist_min_docs: PLIST_MIN_DOCS,
            ..Default::default()
        },
        false,
    );
    let mut alpha_log = Vec::new();
    let mut beta_log = Vec::new();
    let mut mid_left = 300usize;
    let batch_rows = 50_000;
    let mut emitted = 0usize;
    while emitted < ROWS {
        let n = batch_rows.min(ROWS - emitted);
        let mut ts = Vec::with_capacity(n);
        let mut svc = Vec::with_capacity(n);
        let mut log = Vec::with_capacity(n);
        let mut extra = Vec::with_capacity(n);
        let mut source = Vec::with_capacity(n);
        for i in 0..n {
            let row = emitted + i;
            ts.push(1_790_000_000_000_000i64 - row as i64 * 1_000);
            let service = if RARE_ROWS.contains(&row) {
                "svc-rare".to_string()
            } else if mid_left > 0 && rng.random::<f64>() < 0.002 {
                mid_left -= 1;
                "svc-mid".to_string()
            } else if rng.random::<f64>() < 0.30 {
                "svc-big".to_string()
            } else {
                format!("svc-{:02}", rng.random_range(0..17))
            };
            let mut line = format!("every w{:03}", rng.random_range(0..300));
            let pinned = row == 50_000 || row == 150_000;
            if pinned || rng.random::<f64>() < 0.60 {
                line.push_str(" alpha");
                alpha_log.push(row as u32);
            }
            if pinned || rng.random::<f64>() < 0.40 {
                line.push_str(" beta");
                beta_log.push(row as u32);
            }
            if rng.random::<f64>() < 0.05 {
                line.push_str(" gamma");
            }
            if pinned {
                line.push_str(" needle");
            }
            let more = if rng.random::<f64>() < 0.01 {
                "alpha"
            } else {
                "x"
            };
            source.push(format!(
                "{{\"svc\":\"{service}\",\"log\":\"{line}\",\"extra\":\"{more}\"}}"
            ));
            svc.push(service);
            log.push(line);
            extra.push(more.to_string());
        }
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(ts)),
            Arc::new(StringArray::from(svc)),
            Arc::new(StringArray::from(log)),
            Arc::new(StringArray::from(extra)),
        ];
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        writer
            .push_batch_with_source(&batch, &StringArray::from(source), None)
            .unwrap();
        emitted += n;
    }
    let (data, index) = writer.finish().unwrap();
    let index = Bytes::from(index.unwrap());
    let plist = blob_range(&index, container::BLOB_TAG_PLIST);
    let terms = blob_range(&index, container::BLOB_TAG_TERMS);
    let dict_blocks = blob_range(&index, container::BLOB_TAG_DICT_BLOCKS);
    Fixture {
        data: Bytes::from(data),
        index,
        plist,
        terms,
        dict_blocks,
        alpha_log,
        beta_log,
    }
}

/// The fixture's dense records are 100–150 KB — below the production
/// whole-record threshold (1 MiB), so the header + skip-group path is
/// exercised by lowering it to 16 KiB here. The threshold's own effect is
/// covered by `whole_record_threshold_reads_small_records_in_one_wave`.
fn open(fixture: &Fixture) -> (VixReader, Arc<LoggedSource>) {
    open_with_threshold(fixture, 16 * 1024)
}

fn open_with_threshold(fixture: &Fixture, threshold: u64) -> (VixReader, Arc<LoggedSource>) {
    let data = LoggedSource::new(fixture.data.clone());
    let index = LoggedSource::new(fixture.index.clone());
    let mut reader =
        VixReader::open_ranged_with_index_tail(data, Some(index.clone()), INDEX_TAIL).unwrap();
    reader.set_partial_record_min_bytes(threshold);
    index.calls.lock().clear();
    (reader, index)
}

fn fulltext(query: VixQuery) -> VixQuery {
    VixQuery::FullText {
        fields: vec!["log".to_string(), "extra".to_string()],
        query: Box::new(query),
    }
}

fn record_len(ids: &[u32]) -> u64 {
    postings::encode_record(ids).unwrap().len() as u64
}

fn bytes_of(calls: &[Vec<Range<u64>>]) -> u64 {
    calls
        .iter()
        .flatten()
        .map(|range| range.end - range.start)
        .sum()
}

#[test]
fn and_intersection_parity_and_io_waves() {
    let fixture = build();
    let memory =
        VixReader::open_with_index(fixture.data.clone(), Some(fixture.index.clone())).unwrap();
    assert!(
        fixture.alpha_log.len() as u32 > PLIST_MIN_DOCS * 100,
        "alpha must be a long out-of-row list"
    );
    let svc_big = exact("svc", "svc-big");
    let svc_rare = exact("svc", "svc-rare");
    let prod_shape = fulltext(VixQuery::And(vec![
        VixQuery::And(vec![
            any_token("alpha"),
            any_token("beta"),
            any_token("alpha"),
        ]),
        svc_big.clone(),
    ]));

    // every planned shape agrees with the in-memory evaluation
    let battery: Vec<(&str, VixQuery)> = vec![
        ("prod shape", prod_shape.clone()),
        (
            "rare narrow leaf + dense tokens (partial reads)",
            fulltext(VixQuery::And(vec![
                svc_rare.clone(),
                any_token("alpha"),
                any_token("beta"),
            ])),
        ),
        (
            "inline token shrinks the accumulator first",
            fulltext(VixQuery::And(vec![
                any_token("alpha"),
                any_token("needle"),
                any_token("beta"),
            ])),
        ),
        (
            "nested And, All and duplicates flatten",
            fulltext(VixQuery::And(vec![
                VixQuery::All,
                VixQuery::And(vec![
                    any_token("alpha"),
                    VixQuery::And(vec![any_token("alpha"), any_token("beta")]),
                ]),
                svc_big.clone(),
            ])),
        ),
        (
            "composites after leaves",
            fulltext(VixQuery::And(vec![
                svc_big.clone(),
                VixQuery::Or(vec![any_token("gamma"), any_token("needle")]),
                VixQuery::Not(Box::new(any_token("beta"))),
            ])),
        ),
        (
            "dense-elided token constrains nothing",
            fulltext(VixQuery::And(vec![any_token("every"), svc_big.clone()])),
        ),
        (
            "mid-size inline value with dense token",
            fulltext(VixQuery::And(vec![
                exact("svc", "svc-mid"),
                any_token("alpha"),
            ])),
        ),
        ("empty And is All", VixQuery::And(vec![])),
        (
            "Nothing empties",
            fulltext(VixQuery::And(vec![any_token("alpha"), VixQuery::Nothing])),
        ),
        (
            "prefix scan after points",
            fulltext(VixQuery::And(vec![
                svc_big.clone(),
                VixQuery::Prefix {
                    field: None,
                    prefix: b"gam".to_vec(),
                },
            ])),
        ),
    ];
    for (name, query) in &battery {
        let (reader, _) = open(&fixture);
        assert_eq!(
            bits_to_set(&reader.eval(query).unwrap()),
            bits_to_set(&memory.eval(query).unwrap()),
            "{name}"
        );
    }
    let needle = fulltext(VixQuery::And(vec![any_token("needle"), any_token("alpha")]));
    assert_eq!(
        bits_to_set(&memory.eval(&needle).unwrap())
            .into_iter()
            .collect::<Vec<_>>(),
        vec![50_000, 150_000]
    );

    // prod shape: bound (svc-big rows) dwarfs every record's group count, so
    // ONE plist wave reads every distinct record whole — the duplicate alpha
    // leaf costs nothing, and the terms table is scanned once
    let (reader, index) = open(&fixture);
    reader.eval(&prod_shape).unwrap();
    let plist = index.calls_in(&fixture.plist);
    assert_eq!(plist.len(), 1, "one plist wave, got {plist:?}");
    let mut ranges = plist[0].clone();
    let total = ranges.len();
    ranges.sort_by_key(|range| range.start);
    ranges.dedup();
    assert_eq!(ranges.len(), total, "no record is fetched twice");
    // svc-big, alpha@log, alpha@extra, beta@log
    assert_eq!(total, 4, "{ranges:?}");
    assert_eq!(
        index.calls_in(&fixture.terms).len(),
        1,
        "one terms-table scan"
    );

    // partial reads: the six svc-rare rows select at most six skip groups of
    // alpha and beta — wave A fetches their headers (plus the small
    // alpha@extra record), wave B only the touched groups
    let (reader, index) = open(&fixture);
    let partial = fulltext(VixQuery::And(vec![
        svc_rare.clone(),
        any_token("alpha"),
        any_token("beta"),
    ]));
    reader.eval(&partial).unwrap();
    let plist = index.calls_in(&fixture.plist);
    assert_eq!(plist.len(), 2, "header wave + group wave, got {plist:?}");
    let full = record_len(&fixture.alpha_log) + record_len(&fixture.beta_log);
    let fetched = bytes_of(&plist);
    assert!(
        fetched * 5 < full,
        "partial reads must fetch a small fraction of the two dense records: \
         fetched {fetched} of {full}"
    );
    assert!(
        bytes_of(&plist[0..1]) < 4 * 1024,
        "wave A carries skip headers and the small record only: {:?}",
        plist[0]
    );

    // the inline needle leaf applies first: alpha and beta then read only the
    // groups around rows 50_000 and 150_000 — wave B is two skip groups per
    // record, wave A their headers plus the small alpha@extra record
    let (reader, index) = open(&fixture);
    reader
        .eval(&fulltext(VixQuery::And(vec![
            any_token("alpha"),
            any_token("needle"),
            any_token("beta"),
        ])))
        .unwrap();
    let plist = index.calls_in(&fixture.plist);
    assert_eq!(plist.len(), 2, "{plist:?}");
    assert!(
        bytes_of(&plist[1..2]) < 4 * 1024,
        "group wave {:?}",
        plist[1]
    );
    assert!(bytes_of(&plist) * 8 < full, "{}", bytes_of(&plist));

    // a missing narrow leaf short-circuits before any token dictionary,
    // terms or postings read
    let (reader, index) = open(&fixture);
    let missing = fulltext(VixQuery::And(vec![
        VixQuery::And(vec![any_token("alpha"), any_token("beta")]),
        exact("svc", "svc-missing"),
    ]));
    assert_eq!(reader.eval(&missing).unwrap().count_set_bits(), 0);
    assert!(index.calls_in(&fixture.terms).is_empty());
    assert!(index.calls_in(&fixture.plist).is_empty());
    assert_eq!(
        index.calls.lock().len(),
        1,
        "exactly the svc dictionary probe"
    );

    // an absent token short-circuits after the dictionary: no terms scan
    let (reader, index) = open(&fixture);
    let absent = fulltext(VixQuery::And(vec![
        svc_big.clone(),
        any_token("zzznotthere"),
    ]));
    assert_eq!(reader.eval(&absent).unwrap().count_set_bits(), 0);
    assert!(index.calls_in(&fixture.terms).is_empty());
    assert!(index.calls_in(&fixture.plist).is_empty());

    // a dense-elided leaf is dropped from the plan: only svc-big is read
    let (reader, index) = open(&fixture);
    reader
        .eval(&fulltext(VixQuery::And(vec![any_token("every"), svc_big])))
        .unwrap();
    let plist = index.calls_in(&fixture.plist);
    assert_eq!(plist.len(), 1);
    assert_eq!(plist[0].len(), 1, "{plist:?}");
}

/// The search layer's residual-filter shape: an outer `FullText` over every
/// FTS field wrapping `And([And(match_all tokens), svc = X,
/// FullText { [log], And(equality-value tokens) }])`. The inner scoped
/// `FullText` is flattened into the same plan — one dictionary wave for all
/// token leaves of both scopes, one terms scan, one plist wave — and its
/// tokens resolve only in `log`: `alpha` as a token of `extra` (rows the
/// outer scope would admit) must not satisfy the `log`-scoped leaf.
#[test]
fn scoped_fulltext_children_flatten_into_one_plan() {
    let fixture = build();
    let memory =
        VixReader::open_with_index(fixture.data.clone(), Some(fixture.index.clone())).unwrap();
    let log_scoped = |query: VixQuery| VixQuery::FullText {
        fields: vec!["log".to_string()],
        query: Box::new(query),
    };
    let extra_scoped = |query: VixQuery| VixQuery::FullText {
        fields: vec!["extra".to_string()],
        query: Box::new(query),
    };
    // every row of `extra` is `alpha` or `x`: `alpha` scoped to extra is
    // the 1% complement of `x`; scoped to log it is the 60% list
    let in_extra = bits_to_set(&memory.eval(&extra_scoped(any_token("alpha"))).unwrap());
    let in_log = bits_to_set(&memory.eval(&log_scoped(any_token("alpha"))).unwrap());
    assert!(
        in_extra.len() * 20 < in_log.len(),
        "{} vs {}",
        in_extra.len(),
        in_log.len()
    );
    assert!(
        !in_extra.is_subset(&in_log),
        "the scopes must differ on some rows"
    );

    let shape = fulltext(VixQuery::And(vec![
        VixQuery::And(vec![any_token("beta"), any_token("needle")]),
        exact("svc", "svc-rare"),
        log_scoped(VixQuery::And(vec![any_token("alpha"), any_token("needle")])),
    ]));
    let expected = bits_to_set(&memory.eval(&shape).unwrap());
    assert_eq!(
        expected.into_iter().collect::<Vec<_>>(),
        vec![50_000, 150_000]
    );
    let (reader, index) = open(&fixture);
    assert_eq!(
        bits_to_set(&reader.eval(&shape).unwrap())
            .into_iter()
            .collect::<Vec<_>>(),
        vec![50_000, 150_000]
    );
    // waves: svc dictionary block, one token block fetch for beta/needle
    // (outer scope) + alpha/needle (log scope), one terms scan, plist
    // waves — never a second dictionary wave for the inner scope
    let dict = index.calls_in(&fixture.dict_blocks);
    assert_eq!(dict.len(), 2, "svc points, then every token leaf: {dict:?}");
    assert_eq!(index.calls_in(&fixture.terms).len(), 1);

    // the scoped leaf is honoured: alpha@extra rows are not candidates. A
    // row with `extra = alpha` but no `alpha` in `log` must be excluded
    // even though the outer scope would admit the token.
    let only_extra: Vec<u32> = in_extra.difference(&in_log).copied().collect();
    assert!(!only_extra.is_empty());
    let scoped = fulltext(VixQuery::And(vec![
        exact("svc", "svc-big"),
        log_scoped(any_token("alpha")),
    ]));
    let (reader, _) = open(&fixture);
    let got = bits_to_set(&reader.eval(&scoped).unwrap());
    assert_eq!(got, bits_to_set(&memory.eval(&scoped).unwrap()));
    assert!(only_extra.iter().all(|row| !got.contains(row)));

    // two named points resolve in ONE dictionary wave and a missing one
    // still short-circuits before any token read
    let (reader, index) = open(&fixture);
    let two_points = fulltext(VixQuery::And(vec![
        exact("svc", "svc-big"),
        any_token("alpha"),
        exact("svc", "svc-missing"),
    ]));
    assert_eq!(reader.eval(&two_points).unwrap().count_set_bits(), 0);
    assert_eq!(
        index.calls_in(&fixture.dict_blocks).len(),
        1,
        "one point wave"
    );
    assert!(index.calls_in(&fixture.terms).is_empty());
}

/// At the production threshold (`PARTIAL_RECORD_MIN_BYTES`, 1 MiB) the
/// fixture's 100–150 KB dense records are read whole in ONE plist wave even
/// when a rare narrow leaf bounds the accumulator to six rows — the
/// request-count-bound regime object storage lives in — and the answer is
/// unchanged.
#[test]
fn whole_record_threshold_reads_small_records_in_one_wave() {
    let fixture = build();
    let memory =
        VixReader::open_with_index(fixture.data.clone(), Some(fixture.index.clone())).unwrap();
    let query = fulltext(VixQuery::And(vec![
        exact("svc", "svc-rare"),
        any_token("alpha"),
        any_token("beta"),
    ]));
    let expected = bits_to_set(&memory.eval(&query).unwrap());
    let (reader, index) = open_with_threshold(&fixture, crate::reader::PARTIAL_RECORD_MIN_BYTES);
    assert_eq!(bits_to_set(&reader.eval(&query).unwrap()), expected);
    let plist = index.calls_in(&fixture.plist);
    assert_eq!(plist.len(), 1, "one whole-record wave, got {plist:?}");
    let full = record_len(&fixture.alpha_log) + record_len(&fixture.beta_log);
    assert!(
        bytes_of(&plist) >= full,
        "both dense records read whole: {} < {full}",
        bytes_of(&plist)
    );
}
