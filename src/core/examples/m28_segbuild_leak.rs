//! M28 repro: the SEGMENT BUILD job's L0 `.vix` builds on prod compactors
//! (openobserve_jobs::job::segments -> core_writer::write_core_file_from_
//! sorted_batch) leak live heap attributed to the vortex dict writer
//! (M27 prod canary, report#19 r1/r2/r4/r5/r6). This drives EXACTLY that
//! call in a loop with a traces-shaped, high-cardinality corpus and samples
//! LIVE allocated bytes (counting mimalloc, the M23 instrument) after every
//! build. The M27 profiler wraps the allocator: set
//! ZO_HEAP_PROFILE_SAMPLE_EVERY_MB=8 for stack confirmation.
//!
//! usage: m28_segbuild_leak <builds> <rows> <stream_type: traces|logs>

use std::{
    alloc::{GlobalAlloc, Layout},
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::Instant,
};

use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use config::meta::stream::StreamType;

static LIVE_BYTES: AtomicI64 = AtomicI64::new(0);

struct CountingMi;

unsafe impl GlobalAlloc for CountingMi {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { mimalloc::MiMalloc.alloc(layout) };
        if !p.is_null() {
            LIVE_BYTES.fetch_add(layout.size() as i64, Ordering::Relaxed);
        }
        p
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { mimalloc::MiMalloc.alloc_zeroed(layout) };
        if !p.is_null() {
            LIVE_BYTES.fetch_add(layout.size() as i64, Ordering::Relaxed);
        }
        p
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { mimalloc::MiMalloc.dealloc(ptr, layout) };
        LIVE_BYTES.fetch_sub(layout.size() as i64, Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let p = unsafe { mimalloc::MiMalloc.realloc(ptr, layout, new_size) };
        if !p.is_null() {
            LIVE_BYTES.fetch_add(new_size as i64 - layout.size() as i64, Ordering::Relaxed);
        }
        p
    }
}

#[global_allocator]
static GLOBAL: config::heap_profile::HeapProfileAlloc<CountingMi> =
    config::heap_profile::HeapProfileAlloc::new(CountingMi);

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
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// A traces-shaped hour bucket, sorted `_timestamp` DESC (the segment
/// builder's caller contract): service_name low-card (u8 dict), trace_id /
/// span_id 32/16-hex with ~8x consecutive repetition (u16 dicts that keep
/// rotating as the vocabulary drifts per build — the prod r1 shape),
/// span_name mid-card, duration i64.
fn make_bucket(build: usize, rows: usize) -> RecordBatch {
    let mut rng = Rng(0x243F6A8885A308D3 ^ (build as u64).wrapping_mul(0x9E3779B97F4A7C15));
    let base_ts = 1_787_299_200_000_000i64 + (build as i64) * 3_600_000_000;
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("service_name", DataType::Utf8, true),
        Field::new("operation_name", DataType::Utf8, true),
        Field::new("trace_id", DataType::Utf8, true),
        Field::new("span_id", DataType::Utf8, true),
        Field::new("duration", DataType::Int64, true),
    ]));
    // DESC timestamps
    let ts: Vec<i64> = (0..rows).map(|r| base_ts + (rows - r) as i64).collect();
    // M28_BIGVAL=1: plant ONE >1MiB value (the prod trigger: an oversized
    // trace attribute / log line in a dict-probed column). On the unfixed
    // tree the build hangs forever in DictStreamState::encode, allocating a
    // codes+values pair per iteration.
    let bigval_at = if std::env::var("M28_BIGVAL").is_ok() {
        rows / 2
    } else {
        usize::MAX
    };
    let service: Vec<String> = (0..rows)
        .map(|r| {
            if r == bigval_at {
                // the oversized value lands in the LOW-CARDINALITY column the
                // dict probe reliably picks (service_name, 40 distinct)
                "B".repeat((1 << 20) + (64 << 10))
            } else {
                format!("service-{}", rng.below(40))
            }
        })
        .collect();
    let operation: Vec<String> = (0..rows)
        .map(|r| {
            if r == bigval_at {
                "B".repeat((1 << 20) + (64 << 10))
            } else {
                format!("op-{:04}", rng.below(2000))
            }
        })
        .collect();
    // consecutive ~8x repetition, vocabulary distinct across builds
    let trace: Vec<String> = (0..rows)
        .map(|r| {
            let t = (build * rows / 8) + r / 8;
            format!("{t:032x}")
        })
        .collect();
    let span: Vec<String> = (0..rows)
        .map(|r| {
            let s = (build * rows / 2) + r / 2;
            format!("{s:016x}")
        })
        .collect();
    let duration: Vec<i64> = (0..rows).map(|_| rng.below(1_000_000) as i64).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ts)) as ArrayRef,
            Arc::new(StringArray::from(
                service.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                operation.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                trace.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                span.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(duration)),
        ],
    )
    .unwrap()
}

fn ensure_env_many(pairs: &[(&str, &str)]) {
    let missing: Vec<_> = pairs
        .iter()
        .filter(|(k, v)| !std::env::var(k).map(|have| have == *v).unwrap_or(false))
        .collect();
    if missing.is_empty() {
        return;
    }
    use std::os::unix::process::CommandExt;
    let exe = std::env::current_exe().expect("current_exe");
    let mut cmd = std::process::Command::new(exe);
    cmd.args(std::env::args_os().skip(1));
    for (k, v) in pairs {
        cmd.env(k, v);
        eprintln!("re-exec with {k}={v}");
    }
    let error = cmd.exec();
    panic!("re-exec failed: {error}");
}

fn main() -> Result<(), anyhow::Error> {
    config::heap_profile::init();
    let args: Vec<String> = std::env::args().collect();
    let builds: usize = args.get(1).map(|s| s.parse().unwrap()).unwrap_or(50);
    let rows: usize = args.get(2).map(|s| s.parse().unwrap()).unwrap_or(131_072);
    let st = args.get(3).cloned().unwrap_or_else(|| "traces".to_string());
    let stream_type = match st.as_str() {
        "logs" => StreamType::Logs,
        _ => StreamType::Traces,
    };
    // prod compactor parity pins (same as the M26 harness)
    ensure_env_many(&[
        ("ZO_LOCAL_MODE", "true"),
        ("ZO_DATA_DIR", "/home/zhichen/work/m28-data/segbuild"),
        ("ZO_VIX_L0_INDEX_OFF_STREAM_TYPES", "logs"),
        ("ZO_VIX_PLIST_MIN_DOCS", "8192"),
        ("ZO_NODE_ROLE", "compactor"),
        ("ZO_MEMORY_CACHE_ENABLED", "false"),
    ]);

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async move {
            let fts: Vec<String> = vec![];
            let bloom: Vec<String> = vec![];
            eprintln!("[m28-seg] builds={builds} rows={rows} stream_type={stream_type}");

            // warmup
            let t0 = Instant::now();
            let bucket = make_bucket(0, rows);
            let bytes = bucket.get_array_memory_size();
            let result = openobserve_core::vix::core_writer::write_core_file_from_sorted_batch(
                "m28-seg-warm",
                stream_type,
                bucket,
                &fts,
                &bloom,
                false,
                bytes,
            )
            .await?;
            eprintln!(
                "[m28-seg] warmup: rows={} data={:.1}MB index={:.1}MB {:.2}s",
                result.stats.row_count,
                result.data.len() as f64 / (1024.0 * 1024.0),
                result.stats.index_size as f64 / (1024.0 * 1024.0),
                t0.elapsed().as_secs_f64(),
            );
            drop(result);
            let live0 = live_mb();
            eprintln!("[m28-seg] baseline live={live0:.1}MB");

            let mut prev = live0;
            for b in 1..=builds {
                let bucket = make_bucket(b, rows);
                let bytes = bucket.get_array_memory_size();
                let result =
                    openobserve_core::vix::core_writer::write_core_file_from_sorted_batch(
                        "m28-seg",
                        stream_type,
                        bucket,
                        &fts,
                        &bloom,
                        false,
                        bytes,
                    )
                    .await?;
                let out = result.data.len();
                // M28_DUMP=<dir>: persist each build's container bytes for
                // byte-identity comparison across trees (sha256sum outside).
                if let Ok(dir) = std::env::var("M28_DUMP") {
                    std::fs::create_dir_all(&dir)?;
                    std::fs::write(format!("{dir}/build_{b:03}.vix"), &result.data)?;
                    if let Some(index) = &result.index {
                        std::fs::write(format!("{dir}/build_{b:03}.vxi"), index)?;
                    }
                }
                drop(result);
                let now = live_mb();
                eprintln!(
                    "[m28-seg] build={b:03} out={:.1}MB live={now:.1}MB d={:+.2}MB",
                    out as f64 / (1024.0 * 1024.0),
                    now - prev
                );
                prev = now;
            }

            let live1 = live_mb();
            eprintln!(
                "[m28-seg] SUMMARY builds={builds} live0={live0:.1}MB live1={live1:.1}MB \
                 d={:.1}MB per_build={:.3}MB stream_type={stream_type}",
                live1 - live0,
                (live1 - live0) / builds as f64
            );
            Ok::<(), anyhow::Error>(())
        })
}
