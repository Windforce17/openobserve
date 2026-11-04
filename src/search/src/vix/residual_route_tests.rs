// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use std::sync::atomic::{AtomicBool, AtomicUsize};

use arrow::{
    array::{BooleanArray, BooleanBufferBuilder, Int64Array, RecordBatch, StringArray},
    buffer::BooleanBuffer,
    datatypes::{DataType, Field, Schema},
};
use vortex_index::{
    VixDocs, VixRangeSource, VixReadOperation, VixReader, VixWriter, VixWriterOptions,
};

use super::*;
use crate::{
    index::{Condition, IndexCondition},
    vix::residual::{RESIDUAL_SELECT_MAX_BYTES, Refusal, refine_select_streaming},
};

const ROWS: usize = 20_001;

fn fixture(cancel: bool) -> Bytes {
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("body", DataType::Utf8, false),
    ]));
    let strings = StringArray::from_iter_values((0..ROWS).map(|row| {
        let phrase = if row % 3 == 0 {
            "alpha beta"
        } else {
            "alpha gap beta"
        };
        let mut value = format!("{phrase} {row:08} ");
        let mut state = (row as u64 + 1).wrapping_mul(0x9e3779b97f4a7c15);
        for _ in 0..if cancel { 128 } else { 32 } {
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
                (0..ROWS).map(|row| row as i64 + 1),
            )),
            Arc::new(strings),
        ],
    )
    .unwrap();
    let mut writer = VixWriter::new(
        &schema,
        VixWriterOptions {
            docs_encode_sample_bytes: 1,
            docs_chunk_max_rows: 4096,
            docs_chunk_bytes: 1,
            ..Default::default()
        },
        false,
    );
    writer
        .push_batch_with_source(&batch, &StringArray::from(vec!["{}"; ROWS]), None)
        .unwrap();
    let data = Bytes::from(writer.finish().unwrap().0);
    if cancel {
        assert!(data.len() > 16 * 1024 * 1024);
    }
    data
}

struct Control {
    armed: AtomicBool,
    cancel_mode: bool,
    fetched: AtomicUsize,
    live: AtomicUsize,
    blocked: AtomicBool,
    entered: Notify,
    release: Notify,
    pool: Arc<ByteGate>,
    competing: Mutex<Option<BytePermit>>,
}

struct Payload {
    bytes: Bytes,
    control: Arc<Control>,
}
impl AsRef<[u8]> for Payload {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl Drop for Payload {
    fn drop(&mut self) {
        self.control.live.fetch_sub(1, Ordering::AcqRel);
    }
}

struct ColdSource {
    bytes: Bytes,
    control: Arc<Control>,
}
impl VixRangeSource for ColdSource {
    fn len(&self) -> u64 {
        self.bytes.len() as u64
    }
    fn fetch(&self, range: Range<u64>) -> BoxFuture<'static, anyhow::Result<Bytes>> {
        let bytes = self.bytes.clone();
        let control = self.control.clone();
        let armed = control.armed.load(Ordering::Acquire);
        async move {
            if !armed {
                return Ok(Bytes::copy_from_slice(
                    &bytes[range.start as usize..range.end as usize],
                ));
            }
            let index = control.fetched.fetch_add(1, Ordering::AcqRel);
            if control.cancel_mode && index > 0 {
                // At least one prior payload is still held by native IO/scan.
                let released = control.release.notified();
                tokio::pin!(released);
                released.as_mut().enable();
                assert!(control.live.load(Ordering::Acquire) > 0);
                control.blocked.store(true, Ordering::Release);
                control.entered.notify_one();
                tokio::time::timeout(Duration::from_secs(15), released)
                    .await
                    .expect("in-flight payload fetch was never released");
            }
            let bytes = Bytes::copy_from_slice(&bytes[range.start as usize..range.end as usize]);
            if !control.cancel_mode && index == 0 {
                let available = {
                    let state = control.pool.state.lock();
                    control.pool.limit - state.used
                };
                *control.competing.lock() = Some(control.pool.admit(available).unwrap());
            }
            control.live.fetch_add(1, Ordering::AcqRel);
            Ok(Bytes::from_owner(Payload { bytes, control }))
        }
        .boxed()
    }
}

fn condition() -> IndexCondition {
    IndexCondition {
        conditions: vec![Condition::MatchAll("alpha beta".into())],
    }
}

fn assert_no_reader_pending(reader: &VixReader) {
    struct MemorySnapshot(AtomicUsize);
    impl VixReadOperation for MemorySnapshot {
        fn is_cancelled(&self) -> bool {
            false
        }
        fn check_memory(&self, bytes: usize) -> vortex_index::Result<()> {
            self.0.store(bytes, Ordering::Release);
            Ok(())
        }
    }
    let snapshot = Arc::new(MemorySnapshot(AtomicUsize::new(0)));
    vortex_index::with_read_operation(snapshot.clone(), || {
        reader
            .filter_docs_columns_rows(
                &["body"],
                &BooleanBuffer::new_unset(ROWS),
                RESIDUAL_SELECT_MAX_BYTES,
                &mut |_| panic!("empty selection must not scan"),
            )
            .unwrap();
    });
    // The empty selection still reserves its two bitmaps and selection envelope.
    // A subsequent evaluation's initial reserve(0) reports the unmodified tracker.
    vortex_index::with_read_operation(snapshot.clone(), || {
        reader.eval(&vortex_index::VixQuery::All).unwrap();
    });
    assert_eq!(snapshot.0.load(Ordering::Acquire), reader.memory_size());
}

fn scan_fallback(data: Bytes) -> BooleanBuffer {
    let docs = VixDocs::open(data).unwrap();
    let mut exact = BooleanBufferBuilder::new(ROWS);
    docs.scan_docs_selection_opts(
        None,
        Some(vortex::scan::selection::Selection::IncludeRoaring(
            (0..ROWS as u64).collect(),
        )),
        None,
        &[],
        None,
        0,
        &mut |batch| {
            let values = condition()
                .to_physical_expr(batch.schema().as_ref(), &["body".into()])?
                .evaluate(&batch)?
                .into_array(batch.num_rows())?;
            for value in values
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .iter()
            {
                exact.append(value == Some(true));
            }
            Ok(())
        },
    )
    .unwrap();
    exact.finish()
}

async fn run_case(cancel: bool) {
    let data = fixture(cancel);
    let pool = ByteGate::new(128 * 1024 * 1024);
    let count = Arc::new(Semaphore::new(1));
    let permit = EvaluationPermit::on(
        count.clone().try_acquire_owned().unwrap(),
        &pool,
        Duration::from_millis(1),
    );
    let control = Arc::new(Control {
        armed: AtomicBool::new(false),
        cancel_mode: cancel,
        fetched: AtomicUsize::new(0),
        live: AtomicUsize::new(0),
        blocked: AtomicBool::new(false),
        entered: Notify::new(),
        release: Notify::new(),
        pool: pool.clone(),
        competing: Mutex::new(None),
    });
    let reader = Arc::new(
        VixReader::open_ranged(Arc::new(ColdSource {
            bytes: data.clone(),
            control: control.clone(),
        }))
        .unwrap(),
    );
    let operation = ReadOperation::new(Arc::new(FetchStats::default()), None);
    let inner_refused = Arc::new(AtomicBool::new(false));
    let finished = Arc::new(AtomicBool::new(false));
    let (task_reader, task_control, task_refused, task_finished) = (
        reader.clone(),
        control.clone(),
        inner_refused.clone(),
        finished.clone(),
    );
    let evaluation =
        super::super::run_evaluation("residual-ranged-route", &operation, permit, move || {
            struct Finished(Arc<AtomicBool>);
            impl Drop for Finished {
                fn drop(&mut self) {
                    self.0.store(true, Ordering::Release);
                }
            }
            let _finished = Finished(task_finished);
            task_reader.docs_schema()?;
            task_control.armed.store(true, Ordering::Release);
            let result = refine_select_streaming(
                &task_reader,
                &condition(),
                &[0],
                &["body".into()],
                &BooleanBuffer::new_set(ROWS),
                RESIDUAL_SELECT_MAX_BYTES,
            )?;
            task_refused.store(result == Err(Refusal::TooManyBytes), Ordering::Release);
            Ok(result)
        });
    tokio::pin!(evaluation);
    let result = if cancel {
        tokio::select! {
            result = &mut evaluation => panic!("evaluation ended before in-flight cancellation: {result:?}"),
            _ = tokio::time::sleep(Duration::from_secs(15)) => panic!("fixture never held overlapping payload IO"),
            _ = control.entered.notified() => {}
        }
        assert!(control.fetched.load(Ordering::Acquire) >= 2);
        assert!(control.live.load(Ordering::Acquire) > 0);
        assert!(control.blocked.load(Ordering::Acquire));
        assert!(pool.state.lock().used > 0);
        operation.cancel();
        control.release.notify_waiters();
        evaluation.await
    } else {
        evaluation.await
    };
    let error = result.expect_err("outer evaluation must not publish an exact or partial result");
    if cancel {
        assert!(super::super::is_cancelled_read(&error), "{error:#}");
    } else {
        assert!(inner_refused.load(Ordering::Acquire));
        assert!(super::super::requires_exact_scan(&error), "{error:#}");
        assert!(error.chain().any(|e| e.is::<EvaluationMemoryExhausted>()));
    }
    // The cancelled waiter can return before spawn_blocking has unwound.
    tokio::time::timeout(Duration::from_secs(15), async {
        while !finished.load(Ordering::Acquire) || count.available_permits() != 1 {
            control.release.notify_waiters();
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    drop(control.competing.lock().take());
    assert!(control.fetched.load(Ordering::Acquire) > 0);
    assert_eq!(control.live.load(Ordering::Acquire), 0);
    assert_eq!(pool.state.lock().used, 0);
    assert_eq!(count.available_permits(), 1);
    assert_no_reader_pending(&reader);
    assert_eq!(
        scan_fallback(data),
        BooleanBuffer::collect_bool(ROWS, |row| row % 3 == 0)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn residual_outer_evaluation_rejects_ranged_io_and_preserves_complete_fallback() {
    run_case(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn residual_outer_evaluation_cancels_inflight_payload_and_releases_owners() {
    run_case(true).await;
}
