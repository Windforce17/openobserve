//! Local cold-reader probe. Usage: cold_histogram_probe MANIFEST.json TAIL_BYTES REPEATS
//! Each measurement opens fresh readers. Local file pages may be warm.
//! Process CPU includes Vortex workers; wall time is not a network-latency estimate.
use std::{
    collections::BTreeMap,
    fs::File,
    ops::Range,
    os::unix::fs::FileExt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
    time::Instant,
};

use anyhow::{Context, ensure};
use arrow::{
    array::{Array, Int64Array, StringArray},
    compute::cast,
    datatypes::DataType,
};
use bytes::Bytes;
use futures::future::BoxFuture;
use serde::Deserialize;
use serde_json::{Value, json};
use vortex_index::{VixDocs, VixQuery, VixRangeSource, VixReader, set_tail_fetch_size};

#[derive(Deserialize)]
struct Sample {
    sample: usize,
    data_path: String,
    index_path: String,
}

#[derive(Default)]
struct Io {
    batches: usize,
    rounds: usize,
    reads: Vec<Value>,
}

#[derive(Default)]
struct MemoryPeak(AtomicUsize);

impl vortex_index::VixReadOperation for MemoryPeak {
    fn is_cancelled(&self) -> bool {
        false
    }
    fn check_memory(&self, owned: usize) -> vortex_index::Result<()> {
        self.0.fetch_max(owned, Ordering::Relaxed);
        Ok(())
    }
}

struct Source {
    file: Arc<File>,
    len: u64,
    kind: &'static str,
    io: Arc<Mutex<Io>>,
}

impl Source {
    fn open(
        path: &str,
        kind: &'static str,
        io: &Arc<Mutex<Io>>,
    ) -> anyhow::Result<Arc<dyn VixRangeSource>> {
        let file = File::open(path).with_context(|| path.to_owned())?;
        Ok(Arc::new(Self {
            len: file.metadata()?.len(),
            file: Arc::new(file),
            kind,
            io: Arc::clone(io),
        }))
    }

    fn read(&self, ranges: Vec<Range<u64>>) -> BoxFuture<'static, anyhow::Result<Vec<Bytes>>> {
        let file = Arc::clone(&self.file);
        let io = Arc::clone(&self.io);
        let kind = self.kind;
        Box::pin(async move {
            let (batch, round) = {
                let mut io = io.lock().unwrap();
                io.batches += 1;
                (io.batches, io.rounds + 1)
            };
            // Yield once so a bundled pair can be issued before either completes.
            // Worker scheduling can still split independent reads: rounds are an upper bound.
            let mut yielded = false;
            futures::future::poll_fn(|cx| {
                if std::mem::replace(&mut yielded, true) {
                    Poll::Ready(())
                } else {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            })
            .await;
            let mut out = Vec::new();
            for range in ranges {
                let mut bytes = vec![0; usize::try_from(range.end - range.start)?];
                file.read_exact_at(&mut bytes, range.start)?;
                io.lock()
                    .unwrap()
                    .reads
                    .push(json!({"object":kind,"start":range.start,"end":range.end,"batch":batch,"round":round}));
                out.push(Bytes::from(bytes));
            }
            let mut io = io.lock().unwrap();
            io.rounds = io.rounds.max(round);
            Ok(out)
        })
    }
}

impl VixRangeSource for Source {
    fn len(&self) -> u64 {
        self.len
    }
    fn fetch(&self, range: Range<u64>) -> BoxFuture<'static, anyhow::Result<Bytes>> {
        let read = self.read(vec![range]);
        Box::pin(async move { Ok(read.await?.remove(0)) })
    }
    fn fetch_many(
        &self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'static, anyhow::Result<Vec<Bytes>>> {
        self.read(ranges)
    }
}

#[repr(C)]
struct Timespec {
    seconds: i64,
    nanos: i64,
}
unsafe extern "C" {
    fn clock_gettime(clock: i32, time: *mut Timespec) -> i32;
}
fn cpu_ns() -> anyhow::Result<u64> {
    #[cfg(target_os = "macos")]
    const PROCESS_CLOCK: i32 = 12;
    #[cfg(not(target_os = "macos"))]
    const PROCESS_CLOCK: i32 = 2;
    let mut value = Timespec {
        seconds: 0,
        nanos: 0,
    };
    // SAFETY: clock_gettime receives a valid writable timespec for this call.
    ensure!(
        unsafe { clock_gettime(PROCESS_CLOCK, &mut value) } == 0,
        "process CPU clock failed"
    );
    Ok(u64::try_from(value.seconds)? * 1_000_000_000 + u64::try_from(value.nanos)?)
}

fn snapshot(io: &Arc<Mutex<Io>>) -> Value {
    let io = io.lock().unwrap();
    let bytes: u64 = io
        .reads
        .iter()
        .map(|r| r["end"].as_u64().unwrap() - r["start"].as_u64().unwrap())
        .sum();
    json!({"reads":io.reads.len(),"bytes":bytes,"batches":io.batches,"rounds_upper_bound":io.rounds,"ranges":io.reads})
}

fn open_index(sample: &Sample, io: &Arc<Mutex<Io>>) -> anyhow::Result<VixReader> {
    VixReader::open_ranged_with_index_sidecar_first(
        Source::open(&sample.data_path, "data", io)?,
        Some(Source::open(&sample.index_path, "index", io)?),
        true,
    )
}

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    ensure!(args.len() == 4, "expected MANIFEST.json TAIL_BYTES REPEATS");
    let samples: Vec<Sample> = serde_json::from_reader(File::open(&args[1])?)?;
    let tail: u64 = args[2].parse()?;
    let repeats: usize = args[3].parse()?;
    set_tail_fetch_size(tail);
    for sample in &samples {
        // Establish values and an independent column-scan oracle outside timed runs.
        let oracle = open_index(sample, &Arc::new(Mutex::new(Io::default())))?;
        let column = cast(&oracle.read_column("service_name")?, &DataType::Utf8)?;
        let services = column
            .as_any()
            .downcast_ref::<StringArray>()
            .context("service_name string")?;
        let timestamps = cast(&oracle.read_column("_timestamp")?, &DataType::Int64)?;
        let timestamps = timestamps
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("timestamp i64")?;
        let mut counts = BTreeMap::<String, u64>::new();
        for service in services.iter().flatten() {
            *counts.entry(service.to_owned()).or_default() += 1;
        }
        let common = if counts.contains_key("bot-framework-service") {
            "bot-framework-service".to_owned()
        } else {
            counts
                .iter()
                .max_by_key(|(_, n)| *n)
                .context("no services")?
                .0
                .clone()
        };
        let rare = counts
            .iter()
            .min_by_key(|(_, n)| *n)
            .context("no services")?
            .0
            .clone();
        let min = timestamps
            .values()
            .iter()
            .copied()
            .min()
            .context("empty timestamps")?;
        let max = timestamps
            .values()
            .iter()
            .copied()
            .max()
            .context("empty timestamps")?;
        let width = ((max - min + 1) / 4).max(1) as u64;
        let clamp = (min + (max - min) / 4, max - (max - min) / 4);
        for (class, needle) in [
            ("absent", "sandbox-runtime-v2".to_owned()),
            ("common", common),
            ("rare", rare),
        ] {
            let expected = counts.get(&needle).copied().unwrap_or(0);
            if class == "absent" {
                ensure!(
                    expected == 0,
                    "sample {} contains absent probe",
                    sample.sample
                );
            }
            for repeat in 0..repeats {
                for route in ["native", "postings"] {
                    let io = Arc::new(Mutex::new(Io::default()));
                    let wall = Instant::now();
                    let cpu = cpu_ns()?;
                    let memory = Arc::new(MemoryPeak::default());
                    let (answer, open_cpu, open_io) = vortex_index::with_read_operation(
                        memory.clone(),
                        || -> anyhow::Result<_> {
                            if route == "native" {
                                let reader = VixDocs::open_ranged_data_only(Source::open(
                                    &sample.data_path,
                                    "data",
                                    &io,
                                )?)?;
                                let open_cpu = cpu_ns()? - cpu;
                                let open_io = snapshot(&io);
                                let answer = reader
                                    .eq_string_count("service_name", &needle, None)?
                                    .context("native count unavailable")?;
                                Ok((answer, open_cpu, open_io))
                            } else {
                                let reader = open_index(sample, &io)?;
                                let open_cpu = cpu_ns()? - cpu;
                                let open_io = snapshot(&io);
                                let answer = reader.count(&VixQuery::Exact {
                                    field: "service_name".into(),
                                    token: needle.as_bytes().to_vec(),
                                })?;
                                Ok((answer, open_cpu, open_io))
                            }
                        },
                    )?;
                    let peak = memory.0.load(Ordering::Relaxed);
                    let total_cpu = cpu_ns()? - cpu;
                    ensure!(
                        answer == expected,
                        "sample {} {route} {needle}: {answer} != {expected}",
                        sample.sample
                    );
                    println!(
                        "{}",
                        json!({"sample":sample.sample,"class":class,"needle":needle,"route":route,"repeat":repeat,"tail":tail,"answer":answer,"cpu_ns":total_cpu,"open_cpu_ns":open_cpu,"evaluate_cpu_ns":total_cpu-open_cpu,"wall_ns":wall.elapsed().as_nanos(),"open_io":open_io,"io":snapshot(&io),"memory_peak":peak})
                    );
                }
            }
            // Validate multi-bucket and partial-time semantics against raw columns.
            for time_clamp in [None, Some(clamp)] {
                let mut expected_hist = vec![0u64; 5];
                for row in 0..services.len() {
                    let ts = timestamps.value(row);
                    if !services.is_null(row)
                        && services.value(row) == needle
                        && time_clamp.is_none_or(|(lo, hi)| ts >= lo && ts < hi)
                    {
                        expected_hist[((ts - min) as u64 / width) as usize] += 1;
                    }
                }
                let native = VixDocs::open_ranged_data_only(Source::open(
                    &sample.data_path,
                    "data",
                    &Arc::new(Mutex::new(Io::default())),
                )?)?;
                let native_hist = native
                    .eq_string_histogram("service_name", &needle, time_clamp, min, width, 5, 0)?
                    .context("native histogram unavailable")?;
                let indexed = open_index(sample, &Arc::new(Mutex::new(Io::default())))?;
                let bits = indexed.eval(&VixQuery::Exact {
                    field: "service_name".into(),
                    token: needle.as_bytes().to_vec(),
                })?;
                let mut indexed_hist = vec![0u64; 5];
                for row in bits.set_indices() {
                    let ts = timestamps.value(row);
                    if time_clamp.is_none_or(|(lo, hi)| ts >= lo && ts < hi) {
                        indexed_hist[((ts - min) as u64 / width) as usize] += 1;
                    }
                }
                ensure!(
                    native_hist == expected_hist && indexed_hist == expected_hist,
                    "sample {} histogram mismatch for {needle}, clamp {time_clamp:?}",
                    sample.sample
                );
                println!(
                    "{}",
                    json!({"verification":true,"sample":sample.sample,"class":class,"needle":needle,"clamp":time_clamp,"min":min,"width":width,"histogram":expected_hist})
                );
            }
        }
    }
    Ok(())
}
