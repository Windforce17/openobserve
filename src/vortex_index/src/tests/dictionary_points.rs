// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use std::{
    ops::Range,
    sync::{
        LazyLock, Weak,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use futures::future::BoxFuture;
use parking_lot::Mutex;

use super::*;
use crate::{VixError, VixRangeSource, VixReadOperation, container, with_read_operation};

/// Record physical reads, including every gap a hostile batch coalescer reads.
/// Each result owns only that physical span, rather than the fixture object.
struct CoalescingSource {
    bytes: Bytes,
    reads: Mutex<Vec<Range<u64>>>,
    cancel_on_dictionary: Mutex<Option<(Range<u64>, Arc<AtomicBool>)>>,
}

impl CoalescingSource {
    fn new(bytes: Bytes) -> Arc<Self> {
        Arc::new(Self {
            bytes,
            reads: Mutex::new(Vec::new()),
            cancel_on_dictionary: Mutex::new(None),
        })
    }

    fn dictionary_reads(&self, dictionary: &Range<u64>) -> Vec<Range<u64>> {
        self.reads
            .lock()
            .iter()
            .filter_map(|range| {
                let start = range.start.max(dictionary.start);
                let end = range.end.min(dictionary.end);
                (start < end).then_some(start..end)
            })
            .collect()
    }
}

impl VixRangeSource for CoalescingSource {
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
        let Some(start) = ranges.iter().map(|range| range.start).min() else {
            return Box::pin(async { Ok(Vec::new()) });
        };
        let end = ranges.iter().map(|range| range.end).max().unwrap();
        assert!(ranges.iter().all(|range| range.start <= range.end));
        assert!(end <= self.len());
        self.reads.lock().push(start..end);
        let owner = Bytes::copy_from_slice(&self.bytes[start as usize..end as usize]);
        let result = ranges
            .iter()
            .map(|range| owner.slice((range.start - start) as usize..(range.end - start) as usize))
            .collect();
        if let Some((dictionary, cancelled)) = self.cancel_on_dictionary.lock().as_ref()
            && start < dictionary.end
            && end > dictionary.start
        {
            cancelled.store(true, Ordering::Release);
        }
        Box::pin(async move { Ok(result) })
    }
}

struct Fixture {
    data: Bytes,
    current: Bytes,
    legacy: Bytes,
}

fn build_fixture(fts_fields: Vec<String>, padding_bytes: usize) -> Fixture {
    let mut fields = vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("gate", DataType::Utf8, false),
        Field::new("raw", DataType::Utf8, false),
    ];
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![6, 5, 4, 3, 2, 1])),
        Arc::new(StringArray::from(vec!["present"; 6])),
        Arc::new(StringArray::from(vec![
            "other", "other", "gamma", "beta", "alpha", "other",
        ])),
    ];
    let padding = "z".repeat(padding_bytes);
    for name in &fts_fields {
        fields.push(Field::new(name, DataType::Utf8, false));
        let values = if name == "historical" {
            vec!["other", "other", "other", "other", "other", "alpha"]
        } else {
            vec!["alpha beta", "alpha", "beta", "gamma", "other", &padding]
        };
        columns.push(Arc::new(StringArray::from(values)));
    }
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let mut writer = VixWriter::new(
        &schema,
        VixWriterOptions {
            fts_field_names: fts_fields,
            max_token_len: padding_bytes.max(64) + 1,
            encode_threads: 1,
            ..Default::default()
        },
        false,
    );
    writer
        .push_batch_with_source(&batch, &StringArray::from(vec!["{}"; 6]), None)
        .unwrap();
    let (data, index) = writer.finish().unwrap();
    let index = index.unwrap();
    let legacy = repack_with_properties(index.clone(), |properties| {
        properties.retain(|(name, _)| name != container::PROP_DICT_FIELD_PAGES);
    });
    Fixture {
        data: Bytes::from(data),
        current: Bytes::from(index),
        legacy: Bytes::from(legacy),
    }
}

// Six meaningful rows are enough. Width, not repeated rows, supplies more
// than 1024 populated field-boundary blocks. Long tokens put the physical
// dictionary beyond one fetch batch, without relying on a cache-size constant.
static WIDE: LazyLock<Fixture> =
    LazyLock::new(|| build_fixture((0..1152).map(|i| format!("fts{i:04}")).collect(), 8192));
static SCOPED: LazyLock<Fixture> =
    LazyLock::new(|| build_fixture(vec!["active".to_string(), "historical".to_string()], 16));

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

fn open(
    fixture: &Fixture,
    legacy: bool,
) -> (
    Arc<VixReader>,
    Arc<CoalescingSource>,
    Arc<CoalescingSource>,
    Range<u64>,
) {
    let index = if legacy {
        &fixture.legacy
    } else {
        &fixture.current
    };
    let dictionary = dictionary_range(index);
    let data = CoalescingSource::new(fixture.data.clone());
    let index = CoalescingSource::new(index.clone());
    let reader =
        Arc::new(VixReader::open_ranged_with_index(data.clone(), Some(index.clone())).unwrap());
    data.reads.lock().clear();
    index.reads.lock().clear();
    (reader, data, index, dictionary)
}

fn multiword() -> VixQuery {
    VixQuery::And(vec![
        any_token("alpha"),
        any_token("beta"),
        any_token("alpha"),
    ])
}

fn assert_one_dictionary_pass(source: &CoalescingSource, dictionary: &Range<u64>) {
    let mut reads = source.dictionary_reads(dictionary);
    assert!(
        !reads.is_empty(),
        "fixture must exercise ranged dictionary IO"
    );
    reads.sort_by_key(|range| range.start);
    for pair in reads.windows(2) {
        assert!(
            pair[0].end <= pair[1].start,
            "a dictionary byte was reread in one point group: {pair:?}"
        );
    }
    for range in source.reads.lock().iter() {
        if range.start < dictionary.end && range.end > dictionary.start {
            assert!(
                range.end - range.start <= 8 * 1024 * 1024,
                "coalescing crossed the bounded dictionary fetch span: {range:?}"
            );
        }
    }
}

#[test]
fn wide_multiword_points_read_dictionary_once_for_bitmap_and_count() {
    // Inspect a different, memory-backed reader so the measured readers keep
    // both their global and per-field dictionaries cold.
    let memory = VixReader::open_with_index(WIDE.data.clone(), Some(WIDE.current.clone())).unwrap();
    assert!(memory.term_row_group_count() > 1024);
    for legacy in [false, true] {
        for count in [false, true] {
            let (reader, data, index, dictionary) = open(&WIDE, legacy);
            assert!(dictionary.end - dictionary.start > 8 * 1024 * 1024);
            if count {
                assert_eq!(reader.count(&multiword()).unwrap(), 1);
            } else {
                assert_eq!(bits_to_set(&reader.eval(&multiword()).unwrap()), docs(&[0]));
            }
            assert_one_dictionary_pass(&index, &dictionary);
            assert!(data.reads.lock().is_empty());
        }
    }
}

#[test]
fn wide_point_groups_preserve_nested_booleans_and_raw_fts_unions() {
    let cases = [
        (any_token("alpha"), docs(&[0, 1, 4])),
        (
            VixQuery::And(vec![exact("raw", "alpha"), any_token("alpha")]),
            docs(&[4]),
        ),
        (
            VixQuery::Or(vec![
                multiword(),
                VixQuery::Not(Box::new(VixQuery::Or(vec![
                    any_token("alpha"),
                    any_token("gamma"),
                ]))),
            ]),
            docs(&[0, 5]),
        ),
        (
            VixQuery::And(vec![
                VixQuery::Or(vec![any_token("alpha"), any_token("gamma")]),
                VixQuery::Not(Box::new(VixQuery::And(vec![
                    any_token("beta"),
                    any_token("gamma"),
                ]))),
            ]),
            docs(&[0, 1, 4]),
        ),
    ];
    for legacy in [false, true] {
        let (reader, ..) = open(&WIDE, legacy);
        for (query, expected) in &cases {
            assert_eq!(bits_to_set(&reader.eval(query).unwrap()), *expected);
            assert_eq!(reader.count(query).unwrap(), expected.len() as u64);
        }
    }
}

#[test]
fn missing_narrow_conjunction_does_not_prefetch_unrelated_dictionary() {
    let query = VixQuery::And(vec![exact("gate", "missing"), multiword()]);
    for legacy in [false, true] {
        for count in [false, true] {
            let (reader, data, index, dictionary) = open(&WIDE, legacy);
            if count {
                assert_eq!(reader.count(&query).unwrap(), 0);
            } else {
                assert_eq!(bits_to_set(&reader.eval(&query).unwrap()), docs(&[]));
            }
            let read_bytes: u64 = index
                .dictionary_reads(&dictionary)
                .iter()
                .map(|range| range.end - range.start)
                .sum();
            assert!(
                read_bytes < (dictionary.end - dictionary.start) / 100,
                "missing narrow equality fetched unrelated vocabulary: {read_bytes} bytes"
            );
            assert!(data.reads.lock().is_empty());
        }
    }
}

fn fulltext(fields: &[&str], query: VixQuery) -> VixQuery {
    VixQuery::FullText {
        fields: fields.iter().map(|field| (*field).to_string()).collect(),
        query: Box::new(query),
    }
}

#[test]
fn fulltext_scope_excludes_historical_and_raw_fields_without_leaking() {
    for legacy in [false, true] {
        let (reader, ..) = open(&SCOPED, legacy);
        let patterns = [
            any_token("alpha"),
            prefix(None, "alp"),
            contains(None, "lph", false),
            regex(None, "alpha"),
            VixQuery::Fuzzy {
                token: "alphx".to_string(),
                distance: 1,
            },
        ];
        for leaf in patterns {
            let query = fulltext(&["active"], leaf.clone());
            assert_eq!(bits_to_set(&reader.eval(&query).unwrap()), docs(&[0, 1]));
            assert_eq!(reader.count(&query).unwrap(), 2);
            // Reusing the reader must not retain the preceding operation's scope.
            assert_eq!(
                bits_to_set(&reader.eval(&leaf).unwrap()),
                docs(&[0, 1, 4, 5])
            );
            assert_eq!(reader.count(&leaf).unwrap(), 4);
        }
        let nested = fulltext(
            &["active"],
            VixQuery::Or(vec![
                multiword(),
                exact("raw", "alpha"),
                VixQuery::Not(Box::new(VixQuery::Or(vec![
                    any_token("alpha"),
                    any_token("beta"),
                    any_token("gamma"),
                ]))),
            ]),
        );
        assert_eq!(
            bits_to_set(&reader.eval(&nested).unwrap()),
            docs(&[0, 4, 5])
        );
        assert_eq!(reader.count(&nested).unwrap(), 3);
        let named_pattern = fulltext(&["active"], prefix(Some("raw"), "alp"));
        assert_eq!(
            bits_to_set(&reader.eval(&named_pattern).unwrap()),
            docs(&[4])
        );
        assert_eq!(reader.count(&named_pattern).unwrap(), 1);
        assert_eq!(
            bits_to_set(
                &reader
                    .eval(&fulltext(&["historical"], any_token("alpha")))
                    .unwrap()
            ),
            docs(&[5])
        );
    }
}

#[test]
fn fulltext_empty_scope_preserves_named_predicates_and_boolean_identities() {
    let (reader, ..) = open(&SCOPED, false);
    for (query, expected) in [
        (any_token("alpha"), docs(&[])),
        (prefix(None, "alp"), docs(&[])),
        (contains(None, "lph", false), docs(&[])),
        (regex(None, "alpha"), docs(&[])),
        (
            VixQuery::Fuzzy {
                token: "alpha".to_string(),
                distance: 0,
            },
            docs(&[]),
        ),
        (exact("raw", "alpha"), docs(&[4])),
        (
            VixQuery::KeyExists {
                path: "raw".to_string(),
            },
            docs(&[0, 1, 2, 3, 4, 5]),
        ),
        (VixQuery::And(vec![]), docs(&[0, 1, 2, 3, 4, 5])),
        (VixQuery::Or(vec![]), docs(&[])),
        (
            VixQuery::Not(Box::new(any_token("alpha"))),
            docs(&[0, 1, 2, 3, 4, 5]),
        ),
        (
            VixQuery::Or(vec![any_token("alpha"), exact("raw", "alpha")]),
            docs(&[4]),
        ),
    ] {
        let query = fulltext(&[], query);
        assert_eq!(bits_to_set(&reader.eval(&query).unwrap()), expected);
        assert_eq!(reader.count(&query).unwrap(), expected.len() as u64);
    }
}

#[test]
fn fulltext_rejects_unknown_and_raw_only_scope_even_for_boolean_identities() {
    let (reader, ..) = open(&SCOPED, false);
    for field in ["missing", "raw"] {
        for inner in [any_token("alpha"), VixQuery::All, VixQuery::Nothing] {
            let query = fulltext(&["active", field], inner);
            for error in [
                reader.eval(&query).unwrap_err(),
                reader.count(&query).unwrap_err(),
            ] {
                assert!(matches!(
                    error.downcast_ref::<VixError>(),
                    Some(VixError::FieldNotIndexed(name)) if name == field
                ));
            }
        }
    }
    assert_eq!(
        reader
            .count(&fulltext(&["active"], any_token("alpha")))
            .unwrap(),
        2
    );
}

struct Cancellation(Arc<AtomicBool>);
impl VixReadOperation for Cancellation {
    fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

fn assert_cancelled(error: anyhow::Error) {
    assert!(matches!(
        error.downcast_ref::<VixError>(),
        Some(VixError::Cancelled)
    ));
}

#[test]
fn dictionary_batch_cancellation_is_typed_bounded_and_reader_is_reusable() {
    for legacy in [false, true] {
        let (reader, _, index, dictionary) = open(&WIDE, legacy);
        let cancelled = Arc::new(AtomicBool::new(false));
        *index.cancel_on_dictionary.lock() = Some((dictionary.clone(), cancelled.clone()));
        assert_cancelled(
            with_read_operation(Arc::new(Cancellation(cancelled)), || {
                reader.eval(&multiword())
            })
            .unwrap_err(),
        );
        let bytes: u64 = index
            .dictionary_reads(&dictionary)
            .iter()
            .map(|r| r.end - r.start)
            .sum();
        assert!(bytes > 0 && bytes < dictionary.end - dictionary.start);
        *index.cancel_on_dictionary.lock() = None;
        assert_eq!(bits_to_set(&reader.eval(&multiword()).unwrap()), docs(&[0]));
        assert_eq!(reader.count(&multiword()).unwrap(), 1);
    }
}

#[test]
fn cancelled_cached_points_and_zero_io_counts_do_no_io_and_leave_scope_reusable() {
    let (reader, data, index, _) = open(&SCOPED, false);
    let query = fulltext(&["active"], any_token("alpha"));
    assert_eq!(reader.count(&query).unwrap(), 2);
    data.reads.lock().clear();
    index.reads.lock().clear();
    let cancelled = Arc::new(Cancellation(Arc::new(AtomicBool::new(true))));
    assert_cancelled(with_read_operation(cancelled.clone(), || reader.eval(&query)).unwrap_err());
    assert_cancelled(with_read_operation(cancelled.clone(), || reader.count(&query)).unwrap_err());
    assert_cancelled(with_read_operation(cancelled, || reader.count(&VixQuery::All)).unwrap_err());
    assert!(data.reads.lock().is_empty());
    assert!(index.reads.lock().is_empty());
    assert_eq!(reader.count(&any_token("alpha")).unwrap(), 4);
    assert_eq!(reader.count(&query).unwrap(), 2);
}

#[derive(Debug, thiserror::Error)]
#[error("dictionary point workspace denied")]
struct WorkspaceDenied;

struct TransientBudget {
    reader: Weak<VixReader>,
    allowance: usize,
    peak_pending: AtomicUsize,
}
impl VixReadOperation for TransientBudget {
    fn is_cancelled(&self) -> bool {
        false
    }
    fn check_memory(&self, bytes: usize) -> crate::error::Result<()> {
        let retained = self.reader.upgrade().unwrap().memory_size();
        let pending = bytes.saturating_sub(retained);
        self.peak_pending.fetch_max(pending, Ordering::AcqRel);
        if pending > self.allowance {
            Err(VixError::Callback(WorkspaceDenied.into()))
        } else {
            Ok(())
        }
    }
}

#[test]
fn point_batch_transient_admission_precedes_fetch_and_rolls_back_on_refusal() {
    for legacy in [false, true] {
        let (reader, _, index, dictionary) = open(&WIDE, legacy);
        let budget = Arc::new(TransientBudget {
            reader: Arc::downgrade(&reader),
            allowance: 1024 * 1024,
            peak_pending: AtomicUsize::new(0),
        });
        let error = with_read_operation(budget.clone(), || reader.eval(&multiword())).unwrap_err();
        assert!(error.chain().any(|cause| cause.is::<WorkspaceDenied>()));
        assert!(budget.peak_pending.load(Ordering::Acquire) > budget.allowance);
        assert!(index.dictionary_reads(&dictionary).is_empty());
        // Retained metadata may have grown, but rejected transient ownership
        // must be gone: a zero-work operation admits exactly current ownership.
        let exact = Arc::new(TransientBudget {
            reader: Arc::downgrade(&reader),
            allowance: 0,
            peak_pending: AtomicUsize::new(0),
        });
        assert_eq!(
            with_read_operation(exact, || reader.count(&VixQuery::All)).unwrap(),
            6
        );
        assert_eq!(bits_to_set(&reader.eval(&multiword()).unwrap()), docs(&[0]));
        assert_eq!(reader.count(&multiword()).unwrap(), 1);
    }
}
