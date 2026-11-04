// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use std::sync::atomic::AtomicBool;

use futures::{FutureExt, future::BoxFuture};

use super::*;

fn residual_fixture(rows: usize) -> (Bytes, Option<Bytes>) {
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("body", DataType::Utf8, false),
    ]));
    let strings = StringArray::from_iter_values((0..rows).map(|row| {
        let mut state = (row as u64 + 1).wrapping_mul(0x9e3779b97f4a7c15);
        let mut value = format!("row-{row:06} alpha beta ");
        for _ in 0..32 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            use std::fmt::Write;
            write!(value, "{state:016x}").unwrap();
        }
        value
    }));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|row| row as i64 + 1),
            )),
            Arc::new(strings),
        ],
    )
    .unwrap();
    let mut writer = crate::VixWriter::new(
        &schema,
        crate::VixWriterOptions {
            docs_encode_sample_bytes: 1,
            ..Default::default()
        },
        false,
    );
    writer
        .push_batch_with_source(&batch, &StringArray::from(vec!["{}"; rows]), None)
        .unwrap();
    let (data, index) = writer.finish().unwrap();
    (data.into(), index.map(Bytes::from))
}

#[test]
fn residual_first_chunk_budget_refusal_releases_all_reservations() {
    const ROWS: usize = 8193;
    let (data, index) = residual_fixture(ROWS);
    let reader = VixReader::open_with_index(data, index).unwrap();
    reader.docs_schema().unwrap();
    let full = scan_blob(&reader.docs_blob, Some(&["body"]), RowSelection::All).unwrap();
    assert_eq!(full.len(), 1);
    let decoded = full[0].get_array_memory_size();
    let rows = BooleanBuffer::new_set(ROWS);
    let scratch = ROWS * std::mem::size_of::<u64>() * 2 + ROWS.div_ceil(512) * 64 * 2;
    let retained = reader.memory.total.load(Ordering::Acquire);
    let mut callbacks = 0;
    let actual = reader
        .filter_docs_columns_rows(&["body"], &rows, (scratch + decoded) as u64, &mut |batch| {
            callbacks += 1;
            Ok(arrow::array::BooleanArray::from(vec![
                true;
                batch.num_rows()
            ]))
        })
        .unwrap();
    assert!(
        actual.is_none(),
        "output plus predicate scratch must fit the cumulative budget"
    );
    assert_eq!(
        callbacks, 0,
        "an oversized first output must not reach the predicate"
    );
    assert_eq!(reader.memory.total.load(Ordering::Acquire), retained);
    let exact = reader
        .filter_docs_columns_rows(&["body"], &rows, 64 * 1024 * 1024, &mut |batch| {
            Ok(arrow::array::BooleanArray::from(vec![
                true;
                batch.num_rows()
            ]))
        })
        .unwrap()
        .unwrap();
    assert_eq!(exact, rows);
    assert_eq!(reader.memory.total.load(Ordering::Acquire), retained);
}

#[derive(Debug)]
enum Event {
    Fetch { before: usize, bytes: usize },
    Check(usize),
}

struct ResidualSource {
    bytes: Bytes,
    memory: Mutex<Option<Weak<ReaderMemory>>>,
    events: Arc<Mutex<Vec<Event>>>,
}

impl crate::VixRangeSource for ResidualSource {
    fn len(&self) -> u64 {
        self.bytes.len() as u64
    }
    fn fetch(&self, range: Range<u64>) -> BoxFuture<'static, anyhow::Result<Bytes>> {
        let out = Bytes::copy_from_slice(&self.bytes[range.start as usize..range.end as usize]);
        if let Some(memory) = self.memory.lock().as_ref().and_then(Weak::upgrade) {
            self.events.lock().push(Event::Fetch {
                before: memory.total.load(Ordering::Acquire),
                bytes: out.len(),
            });
        }
        async move { Ok(out) }.boxed()
    }
}

struct ResidualParent {
    events: Arc<Mutex<Vec<Event>>>,
    cancelled: AtomicBool,
}

impl crate::VixReadOperation for ResidualParent {
    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
    fn check_memory(&self, bytes: usize) -> Result<()> {
        self.events.lock().push(Event::Check(bytes));
        Ok(())
    }
}

#[test]
fn residual_io_payload_is_reported_to_parent_after_fetch() {
    let (data, index) = residual_fixture(20_001);
    let events = Arc::new(Mutex::new(Vec::new()));
    let source = Arc::new(ResidualSource {
        bytes: data,
        memory: Mutex::new(None),
        events: events.clone(),
    });
    let reader = VixReader::open_ranged_with_index(
        source.clone(),
        index.map(|data| {
            Arc::new(ResidualSource {
                bytes: data,
                memory: Mutex::new(None),
                events: events.clone(),
            }) as Arc<dyn crate::VixRangeSource>
        }),
    )
    .unwrap();
    reader.docs_schema().unwrap();
    *source.memory.lock() = Some(Arc::downgrade(&reader.memory));
    let retained = reader.memory.total.load(Ordering::Acquire);
    let parent = Arc::new(ResidualParent {
        events: events.clone(),
        cancelled: AtomicBool::new(false),
    });
    let selected = BooleanBuffer::new_set(20_001);
    let result = crate::with_read_operation(parent, || {
        reader.filter_docs_columns_rows(&["body"], &selected, 64 * 1024 * 1024, &mut |batch| {
            Ok(arrow::array::BooleanArray::from(vec![
                true;
                batch.num_rows()
            ]))
        })
    })
    .unwrap()
    .unwrap();
    assert_eq!(result, selected);
    let events = events.lock();
    let mut fetched = 0;
    let mut charged = 0;
    // A coalesced payload may also own resident footer bytes. Its charge is
    // at least the fetch, at most the whole immutable object, not a later
    // decoded Arrow output charge (this low-compression fixture distinguishes them).
    for pair in events.windows(2) {
        if let Event::Fetch { before, bytes } = pair[0] {
            fetched += bytes;
            assert!(
                matches!(pair[1], Event::Check(owned) if owned >= before + bytes && owned <= before + source.bytes.len()),
                "IO must enter ReaderMemory pending immediately after fetch: {pair:?}"
            );
            charged += bytes;
        }
    }
    assert!(
        fetched > 0,
        "fixture must fetch native payload outside the resident footer"
    );
    assert!(charged >= fetched);
    drop(events);
    assert_eq!(
        reader.memory.total.load(Ordering::Acquire),
        retained,
        "IO/output pending leaked"
    );
}

#[test]
fn residual_cancel_after_output_releases_pending_and_keeps_input() {
    let (data, index) = residual_fixture(8193);
    let reader = VixReader::open_with_index(data, index).unwrap();
    reader.docs_schema().unwrap();
    let retained = reader.memory.total.load(Ordering::Acquire);
    let parent = Arc::new(ResidualParent {
        events: Arc::new(Mutex::new(Vec::new())),
        cancelled: AtomicBool::new(false),
    });
    let selected = BooleanBuffer::new_set(8193);
    let original = selected.clone();
    let result = crate::with_read_operation(parent.clone(), || {
        reader.filter_docs_columns_rows(&["body"], &selected, 64 * 1024 * 1024, &mut |batch| {
            assert!(reader.memory.total.load(Ordering::Acquire) > retained);
            parent.cancelled.store(true, Ordering::Release);
            Ok(arrow::array::BooleanArray::from(vec![
                true;
                batch.num_rows()
            ]))
        })
    });
    assert!(matches!(
        result.unwrap_err().downcast_ref::<VixError>(),
        Some(VixError::Cancelled)
    ));
    assert_eq!(selected, original);
    assert_eq!(reader.memory.total.load(Ordering::Acquire), retained);
}

#[test]
fn residual_roaring_scratch_is_admitted_before_construction() {
    let (data, index) = residual_fixture(8193);
    let reader = VixReader::open_with_index(data, index).unwrap();
    reader.docs_schema().unwrap();
    for count in [4095, 4096, 4097, 8193] {
        let mut selected = BooleanBufferBuilder::new(8193);
        selected.append_n(8193, false);
        for row in 0..count {
            selected.set_bit(row, true);
        }
        let selected = selected.finish();
        let scratch = residual_selection_bytes(&selected) + selected.len().div_ceil(512) * 128;
        let events = Arc::new(Mutex::new(Vec::new()));
        let parent = Arc::new(ResidualParent {
            events: events.clone(),
            cancelled: AtomicBool::new(false),
        });
        let retained = reader.memory.total.load(Ordering::Acquire);
        let actual = crate::with_read_operation(parent.clone(), || {
            reader.filter_docs_columns_rows(&["body"], &selected, (scratch - 1) as u64, &mut |_| {
                panic!("selection must be refused before scanning")
            })
        })
        .unwrap();
        assert!(actual.is_none());
        assert!(
            events.lock().is_empty(),
            "refuse before allocating selection scratch"
        );
        assert_eq!(reader.memory.total.load(Ordering::Acquire), retained);
        let actual = crate::with_read_operation(parent, || {
            reader.filter_docs_columns_rows(&["body"], &selected, 64 * 1024 * 1024, &mut |batch| {
                Ok(arrow::array::BooleanArray::from(vec![
                    true;
                    batch.num_rows()
                ]))
            })
        })
        .unwrap()
        .unwrap();
        assert_eq!(actual, selected);
        assert!(
            matches!(events.lock().first(), Some(Event::Check(bytes)) if *bytes >= retained + scratch)
        );
        assert_eq!(reader.memory.total.load(Ordering::Acquire), retained);
    }
}
