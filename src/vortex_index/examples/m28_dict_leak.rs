//! M28 repro: vortex dict-writer live-heap leak (prod compactor ~35-50MB/s
//! pod-lifetime floor). Writes N vortex files with a HIGH-CARDINALITY utf8
//! column through the DEFAULT write strategy (the prod path: Repartition ->
//! Zoned -> Dict -> Coalesce -> Compress -> Buffered -> Chunked -> Flat) and
//! samples LIVE allocated bytes (counting System allocator) after each file
//! is fully written and dropped. On the unfixed tree live bytes climb per
//! file; after the fix the curve is flat.
//!
//! usage: m28_dict_leak <files> <rows_per_file> <cardinality> [mode]
//!   mode: default   — WriteStrategyBuilder::default() (dict probing ON)
//!         nodict    — same but probe compressor can never pick Dict is not
//!                     trivially reachable; instead we use a flat-only
//!                     strategy (bisection control)

use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::Instant,
};

use arrow::{
    array::{ArrayRef as ArrowArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use vortex::{
    VortexSessionDefault,
    array::{ArrayRef, VortexSessionExecute},
    arrow::ArrowSessionExt,
    arrow::{FromArrowArray, FromArrowType},
    dtype::DType,
    file::{VortexWriteOptions, WriteStrategyBuilder},
    io::{
        runtime::{BlockingRuntime, single::SingleThreadRuntime, tokio::TokioRuntime},
        session::RuntimeSessionExt,
    },
    layout::{
        LayoutStrategy,
        layouts::{
            chunked::writer::ChunkedLayoutStrategy, collect::CollectStrategy,
            flat::writer::FlatLayoutStrategy, table::TableStrategy,
        },
    },
    session::VortexSession,
};

static LIVE_BYTES: AtomicI64 = AtomicI64::new(0);

struct CountingSys;

unsafe impl GlobalAlloc for CountingSys {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            LIVE_BYTES.fetch_add(layout.size() as i64, Ordering::Relaxed);
        }
        p
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc_zeroed(layout) };
        if !p.is_null() {
            LIVE_BYTES.fetch_add(layout.size() as i64, Ordering::Relaxed);
        }
        p
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        LIVE_BYTES.fetch_sub(layout.size() as i64, Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let p = unsafe { System.realloc(ptr, layout, new_size) };
        if !p.is_null() {
            LIVE_BYTES.fetch_add(new_size as i64 - layout.size() as i64, Ordering::Relaxed);
        }
        p
    }
}

#[global_allocator]
static GLOBAL: CountingSys = CountingSys;

fn live_mb() -> f64 {
    LIVE_BYTES.load(Ordering::Relaxed) as f64 / (1024.0 * 1024.0)
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }
}

/// One file's batch: `rows` rows, utf8 terms with consecutive repetition so
/// every chunk is unambiguously dict-friendly (repeat = rows/card copies of
/// each value, laid out consecutively), while the file still carries `card`
/// DISTINCT values and values are distinct ACROSS files — the dictionary
/// must keep absorbing new entries and rotate runs on max_bytes/max_len.
fn make_batch(schema: &Arc<Schema>, file_idx: usize, rows: usize, card: usize) -> RecordBatch {
    let repeat = (rows / card).max(1);
    let base = file_idx * card; // distinct across files
    // M28 root-cause repro: M28_BIGVAL=1 plants ONE value larger than the
    // dict layout's max_dict_bytes (1MB) midway through the file. When the
    // rotating dictionary reaches it on a FRESH builder, encode_chunk
    // encodes 0 rows, remainder() returns the whole chunk, and
    // DictStreamState::encode loops forever allocating codes+values pairs —
    // the prod compactor's 35-50MB/s live-heap floor.
    let bigval_at = if std::env::var("M28_BIGVAL").is_ok() {
        rows / 2
    } else {
        usize::MAX
    };
    let terms: Vec<String> = (0..rows)
        .map(|r| {
            if r == bigval_at {
                // > 1MiB single value (1MiB + 64KiB)
                "B".repeat((1 << 20) + (64 << 10))
            } else {
                let t = base + r / repeat;
                format!("term-{t:012}-suffix")
            }
        })
        .collect();
    let ints: Vec<i64> = (0..rows).map(|r| (file_idx * rows + r) as i64).collect();
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(ints)) as ArrowArrayRef,
            Arc::new(StringArray::from(
                terms.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn flat_only_strategy() -> Arc<dyn LayoutStrategy> {
    // bisection control: no dict layer at all
    Arc::new(TableStrategy::new(
        Arc::new(CollectStrategy::new(FlatLayoutStrategy::default())),
        Arc::new(ChunkedLayoutStrategy::new(FlatLayoutStrategy::default())),
    ))
}

/// The `write_vortex` shape from src/search/src/datafusion/vortex_support.rs:
/// a STATIC tokio runtime, `spawn_blocking` + `block_on`, session
/// `with_tokio()` (CurrentTokioRuntime — resolves to the static runtime at
/// every spawn), the ASYNC `Writer` driven by `push().await`/`finish().await`.
/// This is the prod compactor's FileFormat::Vortex merge write path
/// (trace_list_index & friends).
fn write_one_file_async_static(
    schema: &Arc<Schema>,
    static_rt: &'static tokio::runtime::Runtime,
    file_idx: usize,
    rows: usize,
    card: usize,
    chunk_rows: usize,
) -> usize {
    let batch = make_batch(schema, file_idx, rows, card);
    let dtype = DType::from_arrow(schema.as_ref());
    let (tx, mut rx) = std::sync::mpsc::sync_channel::<arrow::record_batch::RecordBatch>(2);
    let feeder = std::thread::spawn(move || {
        let rows = batch.num_rows();
        let mut offset = 0usize;
        while offset < rows {
            let len = chunk_rows.min(rows - offset);
            tx.send(batch.slice(offset, len)).expect("send");
            offset += len;
        }
    });
    let task = static_rt.spawn_blocking(move || {
        static_rt.handle().block_on(async move {
            let session = VortexSession::default().with_tokio();
            let strategy = WriteStrategyBuilder::default().build();
            let mut buf: Vec<u8> = Vec::new();
            let mut writer = VortexWriteOptions::new(session)
                .with_strategy(strategy)
                .writer(&mut buf, dtype);
            while let Ok(part) = rx.recv() {
                let chunk = ArrayRef::from_arrow(&part, false).expect("from_arrow");
                writer.push(chunk).await.expect("push");
            }
            writer.finish().await.expect("finish");
            buf.len()
        })
    });
    let len = static_rt.handle().block_on(task).expect("join");
    feeder.join().expect("feeder");
    len
}

fn write_one_file(
    schema: &Arc<Schema>,
    file_idx: usize,
    rows: usize,
    card: usize,
    chunk_rows: usize,
    mode: &str,
    shared_pool: Option<&tokio::runtime::Runtime>,
) -> usize {
    let batch = make_batch(schema, file_idx, rows, card);
    let dtype = DType::from_arrow(schema.as_ref());

    // mirrors vortex_index::container::write_vortex_blob: a SingleThreadRuntime
    // drives the blocking writer; the SESSION handle is either that runtime
    // (encode_threads<=1) or a tokio pool (encode_threads>1, prod merge shape).
    let runtime = SingleThreadRuntime::default();
    let mut own_pool = None;
    // the TokioRuntime wrapper must OUTLIVE the session (its handle is a Weak
    // to the wrapper) — same constraint container.rs works under
    let pool_wrapper;
    let session = match mode {
        "pool" => {
            own_pool = Some(
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(4)
                    .thread_name("vix-encode")
                    .build()
                    .expect("pool"),
            );
            pool_wrapper = Some(TokioRuntime::new(
                own_pool.as_ref().unwrap().handle().clone(),
            ));
            VortexSession::default().with_handle(pool_wrapper.as_ref().unwrap().handle())
        }
        "pool-shared" => {
            pool_wrapper = Some(TokioRuntime::new(
                shared_pool.expect("shared pool").handle().clone(),
            ));
            VortexSession::default().with_handle(pool_wrapper.as_ref().unwrap().handle())
        }
        _ => {
            pool_wrapper = None;
            VortexSession::default().with_handle(runtime.handle())
        }
    };
    let _keep_wrapper_alive = &pool_wrapper;
    let mut options = VortexWriteOptions::new(session);
    if mode == "nodict" {
        options = options.with_strategy(flat_only_strategy());
    } else {
        options = options.with_strategy(WriteStrategyBuilder::default().build());
    }

    let mut sink: Vec<u8> = Vec::new();
    {
        let mut writer = options.blocking(&runtime).writer(&mut sink, dtype);
        let mut offset = 0usize;
        while offset < rows {
            let len = chunk_rows.min(rows - offset);
            let part = batch.slice(offset, len);
            let chunk = ArrayRef::from_arrow(&part, false).expect("from_arrow");
            writer.push(chunk).expect("push");
            offset += len;
        }
        writer.finish().expect("finish");
    }
    if let Some(pool) = own_pool {
        pool.shutdown_background();
    }

    // M28_VERIFY=1: read the file back and prove every row (including an
    // M28_BIGVAL oversized value) scans out intact.
    if std::env::var("M28_VERIFY").is_ok() {
        use arrow::array::Array as _;
        use vortex::{buffer::ByteBuffer, file::OpenOptionsSessionExt};
        let runtime2 = SingleThreadRuntime::default();
        let session2 = VortexSession::default().with_handle(runtime2.handle());
        let vxf = session2
            .open_options()
            .open_buffer(ByteBuffer::from(sink.clone()))
            .expect("open");
        let scan = vxf.scan().expect("scan");
        let mut scanned_rows = 0usize;
        let mut max_len = 0usize;
        let mut ctx = session2.create_execution_ctx();
        for array in scan.into_array_iter(&runtime2).expect("iter") {
            let array = array.expect("chunk");
            scanned_rows += array.len();
            let arrow_array = session2
                .arrow()
                .execute_arrow(array, None, &mut ctx)
                .expect("to arrow");
            let strukt = arrow_array
                .as_any()
                .downcast_ref::<arrow::array::StructArray>()
                .expect("struct")
                .clone();
            // term column is field 1 of (_timestamp, term)
            let col = arrow::compute::cast(strukt.column(1), &arrow::datatypes::DataType::Utf8)
                .expect("cast term to utf8");
            let terms = col
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .expect("utf8 column");
            let chunk_max = (0..terms.len())
                .map(|i| terms.value(i).len())
                .max()
                .unwrap_or(0);
            max_len = max_len.max(chunk_max);
        }
        assert_eq!(scanned_rows, rows, "row count roundtrip");
        if std::env::var("M28_BIGVAL").is_ok() {
            assert_eq!(max_len, (1 << 20) + (64 << 10), "oversized value roundtrip");
        }
        eprintln!("[m28] verify file={file_idx}: rows={scanned_rows} max_value_len={max_len} OK");
    }
    sink.len()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let files: usize = args.get(1).map(|s| s.parse().unwrap()).unwrap_or(30);
    let rows: usize = args.get(2).map(|s| s.parse().unwrap()).unwrap_or(131_072);
    let card: usize = args.get(3).map(|s| s.parse().unwrap()).unwrap_or(32_768);
    let mode = args.get(4).cloned().unwrap_or_else(|| "default".to_string());
    let chunk_rows = 8192usize;

    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("term", DataType::Utf8, true),
    ]));

    eprintln!(
        "[m28] files={files} rows/file={rows} cardinality/file={card} chunk={chunk_rows} mode={mode}"
    );

    let shared_pool = (mode == "pool-shared").then(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .thread_name("vix-encode-shared")
            .build()
            .expect("shared pool")
    });

    static STATIC_RT: std::sync::LazyLock<tokio::runtime::Runtime> =
        std::sync::LazyLock::new(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .thread_name("vortex")
                .enable_all()
                .build()
                .expect("static rt")
        });

    let write = |f: usize| -> usize {
        if mode == "asyncstatic" {
            write_one_file_async_static(&schema, &STATIC_RT, f, rows, card, chunk_rows)
        } else if mode == "concstatic" {
            // 4 concurrent async-static writes (prod: ZO_FILE_MERGE_THREAD_NUM
            // merges share the one static VORTEX_RUNTIME)
            let handles: Vec<_> = (0..4)
                .map(|k| {
                    let schema = Arc::clone(&schema);
                    std::thread::spawn(move || {
                        write_one_file_async_static(
                            &schema,
                            &STATIC_RT,
                            f * 4 + k,
                            rows,
                            card,
                            chunk_rows,
                        )
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().expect("join")).sum()
        } else {
            write_one_file(&schema, f, rows, card, chunk_rows, &mode, shared_pool.as_ref())
        }
    };

    // warmup file 0: pay one-time statics (registries, interner, lazylocks)
    let t0 = Instant::now();
    let bytes = write(0);
    let live0 = live_mb();
    eprintln!(
        "[m28] warmup: {bytes}B file, live={live0:.1}MB, {:.2}s",
        t0.elapsed().as_secs_f64()
    );

    let mut prev = live0;
    for f in 1..=files {
        let bytes = write(f);
        let now = live_mb();
        eprintln!(
            "[m28] file={f:03} out={:.1}KB live={now:.1}MB d={:+.2}MB",
            bytes as f64 / 1024.0,
            now - prev
        );
        prev = now;
    }

    let live1 = live_mb();
    let per_file = (live1 - live0) / files as f64;
    eprintln!(
        "[m28] SUMMARY files={files} live0={live0:.1}MB live1={live1:.1}MB d={:.1}MB per_file={per_file:.3}MB mode={mode}",
        live1 - live0
    );
}
