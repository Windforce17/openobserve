//! M26 repro harness: prod compactors leak LIVE memory proportional to JOB
//! THROUGHPUT (~70-100 MB/s at ~12+ merges/min, 150-500 MB per job) across
//! every merge-internals fix — MIMALLOC_PURGE_DELAY=0 did NOT flatten it, so
//! it is live allocation, not allocator retention. Every earlier repro called
//! merge_core_files / the writer DIRECTLY, bypassing the compact JOB
//! machinery. This harness drives the ACTUAL job loop end-to-end against a
//! local sqlite meta store and local-disk object store:
//!
//!   claim (get_pending_jobs) -> heartbeat-from-claim (JobLeaseGuard) ->
//!   scheduler channel -> merge_by_stream (schema get, file_list query,
//!   partition tasks, worker channel) -> merge_files/merge_core_group (real
//!   merge, spool, upload) -> generation-fenced streaming commits
//!   (touch_job_lease + batch_process + batch_add_deleted +
//!   query_ids_by_files + broadcast) -> set_job_done_owned
//!
//! and samples LIVE allocated bytes (counting mimalloc wrapper, the M23
//! instrument) per completed job across hundreds of tiny jobs.
//!
//!   seed <streams> <hours> <files_per_job> <rows_per_file> <width>
//!       One-time seeding in a THROWAWAY process (its allocations never
//!       pollute the run curve): init the sqlite meta store, register each
//!       stream's schema (width 0 = the narrow 7-field logs schema; width>0 =
//!       a `width`-field union schema, prod carries ~2,164), build ONE tiny
//!       index-off .vix template through the REAL move builder, then for
//!       every (stream, hour) job upload files_per_job copies into the
//!       local-disk object store, insert their file_list rows and add_job the
//!       hour. Offsets start 6 hours in the past (non-incremental rounds).
//!
//!   run [max_rounds]
//!       The measured process: start the REAL MergeWorker + JobScheduler,
//!       then call compact::run_merge in a loop until every job is done,
//!       sampling live/RSS/pending/done every 500 ms plus a registry probe
//!       (schema caches, broadcast queue, tokio alive tasks, metrics series)
//!       every 25 completed jobs. Prints the per-job live-byte slope at the
//!       end.
//!
//!   seed-tli <hours> <files_per_job> <rows_per_file>
//!       The PROD KILLER SHAPE (found in prod logs 2026-08-21: repeated
//!       ~12.87GB = mem_total/file_merge_thread_num DataFusion pool-cap
//!       peaks on 128-file default/metadata/trace_list_index merges, one
//!       pool per merge, stacking across workers): seed hour-jobs of the
//!       METADATA stream `trace_list_index` as real PARQUET files with the
//!       prod schema (_timestamp, stream_name, service_name, trace_id).
//!       These route through merge_files' parquet arm -> TableBuilder ->
//!       DATAFUSION_RUNTIME merge_parquet_files(single_partition_sort=true,
//!       M20b) — the arm no prior repro ever exercised. Run with
//!       ZO_MEMORY_CACHE_DATAFUSION_MAX_SIZE pinned (the `run` mode pins
//!       2048 MB) so the box stays safe while the cap-fill reproduces.
//!
//! Differential toggles (env, example-side only):
//!   M26_SCHEMA_CLEAR=1   clear the infra::schema caches between claim rounds
//!   M26_SETTLE_SECS=n    end-of-run settle before the final sample (def 5)

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
};
use config::meta::stream::{FileKey, FileMeta, StreamType};
use datafusion::{catalog::TableProvider, datasource::MemTable};

// ---------------------------------------------------------------------------
// prod allocator + live-byte accounting (M23 instrument)
// ---------------------------------------------------------------------------

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

// M27: the prod global-allocator stack — the sampling heap profiler wraps
// the counting mimalloc (inert unless ZO_HEAP_PROFILE_SAMPLE_EVERY_MB is set).
#[global_allocator]
static GLOBAL: config::heap_profile::HeapProfileAlloc<CountingMi> =
    config::heap_profile::HeapProfileAlloc::new(CountingMi);

fn live_mb() -> i64 {
    LIVE_BYTES.load(Ordering::Relaxed) / (1024 * 1024)
}

fn proc_status_kb(status: &str, key: &str) -> u64 {
    status
        .lines()
        .find(|line| line.starts_with(key))
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0)
}

fn rss_mb() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    proc_status_kb(&status, "VmRSS:") / 1024
}

fn spawn_rss_sampler() {
    let t0 = Instant::now();
    std::thread::Builder::new()
        .name("m26-rss-sampler".into())
        .spawn(move || {
            loop {
                let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
                let vmrss_mb = proc_status_kb(&status, "VmRSS:") / 1024;
                eprintln!(
                    "[rss] t={:.1}s vmrss={vmrss_mb}MB vmhwm={}MB live={}MB",
                    t0.elapsed().as_secs_f64(),
                    proc_status_kb(&status, "VmHWM:") / 1024,
                    live_mb(),
                );
                if vmrss_mb > 17 * 1024 {
                    eprintln!("[rss] SAFETY ABORT: vmrss {vmrss_mb}MB > 17GB ceiling");
                    std::process::abort();
                }
                std::thread::sleep(std::time::Duration::from_millis(250));
            }
        })
        .expect("spawn sampler");
}

// ---------------------------------------------------------------------------
// deterministic corpus (M23 shapes, shrunk to per-job-tiny)
// ---------------------------------------------------------------------------

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

const TIMESTAMP_COL: &str = "_timestamp";

fn logs_schema() -> Arc<Schema> {
    let utf8 = |name: &str| Field::new(name, DataType::Utf8, true);
    Arc::new(Schema::new(vec![
        Field::new(TIMESTAMP_COL, DataType::Int64, false),
        Field::new("code", DataType::Int64, true),
        utf8("env"),
        utf8("level"),
        utf8("message"),
        utf8("pod"),
        utf8("service"),
    ]))
}

/// M25 sparse-field naming, kept identical so wide schemas look like prod's
/// k8s label/annotation registries.
fn wide_sparse_field(index: usize) -> (String, DataType) {
    let name = match index % 4 {
        0 => format!("k8s_label_app_{index:04}"),
        1 => format!("k8s_annotation_meta_{index:04}"),
        2 => format!("attr_service_field_{index:04}"),
        _ => format!("log_ctx_{index:04}"),
    };
    let data_type = if index % 5 == 4 {
        DataType::Int64
    } else {
        DataType::Utf8
    };
    (name, data_type)
}

const WIDE_CORE_FIELDS: usize = 8;

/// `width`-field union schema: `_timestamp` + 7 core fields + sparse fields,
/// non-ts sorted by name — the CURRENT stream schema the job machinery
/// fetches and clones per job (prod: ~2,164 fields).
fn wide_union_schema(width: usize) -> Arc<Schema> {
    let sparse_width = width.saturating_sub(WIDE_CORE_FIELDS);
    let mut fields: Vec<Field> = vec![
        Field::new("message", DataType::Utf8, true),
        Field::new("level", DataType::Utf8, true),
        Field::new("k8s_namespace_name", DataType::Utf8, true),
        Field::new("k8s_pod_name", DataType::Utf8, true),
        Field::new("k8s_container_name", DataType::Utf8, true),
        Field::new("status", DataType::Int64, true),
        Field::new("duration_ms", DataType::Int64, true),
    ];
    for index in 0..sparse_width {
        let (name, data_type) = wide_sparse_field(index);
        fields.push(Field::new(name, data_type, true));
    }
    fields.sort_by(|a, b| a.name().cmp(b.name()));
    let mut all = vec![Field::new(TIMESTAMP_COL, DataType::Int64, false)];
    all.extend(fields);
    Arc::new(Schema::new(all))
}

/// How many sparse fields the TEMPLATE FILE carries (narrow-WAL subset of the
/// wide union — per-file schemas are a fraction of the registry in prod).
const WIDE_FILE_SPARSE_FIELDS: usize = 40;

/// The template file's schema for the wide variant: core + the first
/// `WIDE_FILE_SPARSE_FIELDS` sparse fields (types match the union).
fn wide_file_schema() -> Arc<Schema> {
    let mut fields: Vec<Field> = vec![
        Field::new("message", DataType::Utf8, true),
        Field::new("level", DataType::Utf8, true),
        Field::new("k8s_namespace_name", DataType::Utf8, true),
        Field::new("k8s_pod_name", DataType::Utf8, true),
        Field::new("k8s_container_name", DataType::Utf8, true),
        Field::new("status", DataType::Int64, true),
        Field::new("duration_ms", DataType::Int64, true),
    ];
    for index in 0..WIDE_FILE_SPARSE_FIELDS {
        let (name, data_type) = wide_sparse_field(index);
        fields.push(Field::new(name, data_type, true));
    }
    fields.sort_by(|a, b| a.name().cmp(b.name()));
    let mut all = vec![Field::new(TIMESTAMP_COL, DataType::Int64, false)];
    all.extend(fields);
    Arc::new(Schema::new(all))
}

/// One record batch matching `schema` (works for the narrow logs schema and
/// the wide file schema): every non-ts field gets a small-vocabulary value.
///
/// M28: `M26_HICARD=<n>` (seed-time env) switches the utf8 fields to a
/// HIGH-CARDINALITY dict-friendly corpus — `n` distinct values per field per
/// file, each repeated consecutively (rows/n copies), so the vortex dict
/// probe keeps picking Dict while the dictionary itself is FAT. This is the
/// prod shape (large drifting vocabularies: pods, traces, session ids) that
/// the small 32-value vocab hid: the dict-writer leak retains ~vocab bytes
/// per merge, which at 32 values is ~0.004MB/job noise.
fn make_batch(
    schema: &Arc<Schema>,
    rng: &mut Rng,
    base_ts_us: i64,
    rows: usize,
) -> arrow::record_batch::RecordBatch {
    make_batch_ext(schema, rng, base_ts_us, rows, 1, 0, 0)
}

/// M28 extension of [`make_batch`]: `ts_stride`/`ts_phase` interleave the
/// file's timestamps against its job siblings (row r gets `base + r*stride +
/// phase`) so the k-way merge truly interleaves rows — the passthrough
/// qualification fails and the merge takes the STANDARD REBUILD (decode +
/// re-encode through the vortex dict writer, the prod-hot path the identical
/// -copy corpus short-circuits). `vocab_base` shifts the hicard vocabulary so
/// sibling files carry DISJOINT dictionaries and the merged dictionary must
/// absorb their union.
#[allow(clippy::too_many_arguments)]
fn make_batch_ext(
    schema: &Arc<Schema>,
    rng: &mut Rng,
    base_ts_us: i64,
    rows: usize,
    ts_stride: usize,
    ts_phase: usize,
    vocab_base: usize,
) -> arrow::record_batch::RecordBatch {
    let hicard: usize = std::env::var("M26_HICARD")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let words = [
        "checkout", "timeout", "retry", "connect", "upstream", "request", "handled", "queued",
        "billing", "session", "expired", "refresh", "gateway", "shard", "replica", "commit",
    ];
    let arrays: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|field| -> ArrayRef {
            match (field.name().as_str(), field.data_type()) {
                (TIMESTAMP_COL, _) => Arc::new(Int64Array::from(
                    (0..rows)
                        .map(|r| base_ts_us + (r * ts_stride + ts_phase) as i64)
                        .collect::<Vec<_>>(),
                )),
                ("message", _) => Arc::new(StringArray::from(
                    (0..rows)
                        .map(|_| {
                            let n = 8 + rng.below(9) as usize;
                            let mut text = String::with_capacity(n * 9);
                            for w in 0..n {
                                if w > 0 {
                                    text.push(' ');
                                }
                                text.push_str(words[rng.below(16) as usize]);
                            }
                            text
                        })
                        .collect::<Vec<_>>(),
                )),
                (_, DataType::Int64) => Arc::new(Int64Array::from(
                    (0..rows).map(|_| rng.below(500) as i64).collect::<Vec<_>>(),
                )),
                (name, _) => {
                    let prefix: String = name.chars().take(6).collect();
                    if hicard > 0 {
                        let repeat = (rows / hicard).max(1);
                        Arc::new(StringArray::from(
                            (0..rows)
                                .map(|r| format!("{prefix}-t{:07}-x", vocab_base + r / repeat))
                                .collect::<Vec<_>>(),
                        ))
                    } else {
                        Arc::new(StringArray::from(
                            (0..rows)
                                .map(|_| format!("{prefix}-v{}", rng.below(32)))
                                .collect::<Vec<_>>(),
                        ))
                    }
                }
            }
        })
        .collect();
    arrow::record_batch::RecordBatch::try_new(Arc::clone(schema), arrays).unwrap()
}

// ---------------------------------------------------------------------------
// infra init + seeding
// ---------------------------------------------------------------------------

const ORG: &str = "m26org";

async fn init_meta() -> Result<(), anyhow::Error> {
    let cfg = config::get_config();
    std::fs::create_dir_all(&cfg.common.data_db_dir)?;
    infra::db::init().await?;
    infra::file_list::create_table().await?;
    infra::file_list::create_table_index().await?;
    infra::schema::init().await?;
    Ok(())
}

fn stream_name(index: usize) -> String {
    format!("m26s{index:02}")
}

/// Hour-floored offset (micros) for job index `hour_index`, starting 6 hours
/// in the past and walking further back — always a fully closed hour
/// (non-incremental round), always far inside the 3650-day retention.
fn job_offset_micros(hour_index: usize) -> i64 {
    let hour = 3_600_000_000i64;
    let now = config::utils::time::now_micros();
    let hour_floor = now - now % hour;
    hour_floor - (6 + hour_index as i64) * hour
}

fn hour_prefix(stream: &str, offset_micros: i64) -> String {
    use chrono::TimeZone;
    let t = chrono::Utc.timestamp_nanos(offset_micros * 1000);
    format!("files/{ORG}/logs/{stream}/{}", t.format("%Y/%m/%d/%H"),)
}

#[allow(clippy::too_many_arguments)]
async fn cmd_seed(
    streams: usize,
    hours: usize,
    files_per_job: usize,
    rows_per_file: usize,
    width: usize,
) -> Result<(), anyhow::Error> {
    anyhow::ensure!(
        files_per_job >= 2,
        "need >= 2 files per job to form a merge group"
    );
    let started = Instant::now();
    init_meta().await?;

    // 1. register the CURRENT stream schema for every stream
    let latest_schema = if width == 0 {
        logs_schema()
    } else {
        wide_union_schema(width)
    };
    let now = config::utils::time::now_micros();
    for s in 0..streams {
        let name = stream_name(s);
        infra::schema::merge(
            ORG,
            &name,
            StreamType::Logs,
            latest_schema.as_ref(),
            Some(now),
        )
        .await
        .map_err(|e| anyhow::anyhow!("schema merge for {name}: {e}"))?;
    }
    eprintln!(
        "[seed] registered {streams} stream schemas ({} fields each)",
        latest_schema.fields().len(),
    );

    // 2. index-off .vix template(s) through the REAL move builder.
    // Default: ONE template, every job merges identical copies (fast-path
    // heavy). M28: `M26_INTERLEAVE=1` builds files_per_job DISTINCT templates
    // with row-interleaved timestamps and (with M26_HICARD) disjoint
    // vocabularies — the k-way merge cannot passthrough-copy and takes the
    // STANDARD REBUILD (decode + re-encode), the arm the prod M27 profiler
    // caught leaking inside the vortex dict writer.
    let interleave = std::env::var("M26_INTERLEAVE").ok().as_deref() == Some("1");
    let hicard: usize = std::env::var("M26_HICARD")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let n_templates = if interleave { files_per_job } else { 1 };
    let file_schema = if width == 0 {
        logs_schema()
    } else {
        wide_file_schema()
    };
    let template_base_ts = job_offset_micros(0);
    let mut templates: Vec<(bytes::Bytes, FileMeta)> = Vec::with_capacity(n_templates);
    for f in 0..n_templates {
        let mut rng = Rng(0x9E3779B97F4A7C15 ^ (f as u64).wrapping_mul(0xA24BAED4963EE407));
        let batch = make_batch_ext(
            &file_schema,
            &mut rng,
            template_base_ts,
            rows_per_file,
            n_templates,
            f,
            f * hicard,
        );
        let table: Arc<dyn TableProvider> = Arc::new(MemTable::try_new(
            Arc::clone(&file_schema),
            vec![vec![batch]],
        )?);
        let result = openobserve_core::vix::core_writer::write_core_file_from_tables(
            "m26-template",
            StreamType::Logs,
            Arc::clone(&file_schema),
            vec![table],
            &["message".to_string()],
            &[],
            false,
            0,
        )
        .await?;
        anyhow::ensure!(
            result.stats.index_size == 0 && result.index.is_none(),
            "template must be INDEX-OFF (#42) — got index_size={} (is \
             ZO_VIX_L0_INDEX_OFF_STREAM_TYPES=logs set?)",
            result.stats.index_size
        );
        let template = bytes::Bytes::from(result.data.clone());
        let template_meta = FileMeta {
            min_ts: result.stats.min_ts,
            max_ts: result.stats.max_ts,
            records: result.stats.row_count as i64,
            original_size: (result.stats.row_count as i64) * 140,
            compressed_size: template.len() as i64,
            index_size: 0,
            flattened: false,
            bloom_ver: 0,
        };
        eprintln!(
            "[seed] template[{f}]: {} rows, {} cols, {:.1} KiB data (index-off), ts=[{},{}]",
            result.stats.row_count,
            file_schema.fields().len(),
            template.len() as f64 / 1024.0,
            template_meta.min_ts,
            template_meta.max_ts,
        );
        templates.push((template, template_meta));
    }

    // 3. per (stream, hour): upload copies, insert file_list rows, add_job
    let mut total_files = 0usize;
    let mut total_jobs = 0usize;
    let mut total_bytes = 0usize;
    for s in 0..streams {
        let name = stream_name(s);
        for h in 0..hours {
            let offset = job_offset_micros(h);
            let prefix = hour_prefix(&name, offset);
            let mut adds = Vec::with_capacity(files_per_job);
            for f in 0..files_per_job {
                let (template, template_meta) = &templates[f % n_templates];
                let key = format!("{prefix}/{}.vix", config::ider::generate_file_name());
                let account = infra::storage::get_account(ORG, &key).unwrap_or_default();
                infra::storage::put(&account, &key, template.clone()).await?;
                total_bytes += template.len();
                adds.push(FileKey::new(0, account, key, template_meta.clone(), false));
            }
            infra::file_list::batch_process(&adds).await?;
            let job_id = infra::file_list::add_job(ORG, StreamType::Logs, &name, offset).await?;
            anyhow::ensure!(job_id > 0, "add_job returned {job_id}");
            total_files += files_per_job;
            total_jobs += 1;
        }
    }
    eprintln!(
        "[seed] done: {total_jobs} jobs / {total_files} file_list rows / {:.1} MiB objects, {:.1}s",
        total_bytes as f64 / (1024.0 * 1024.0),
        started.elapsed().as_secs_f64(),
    );
    Ok(())
}

/// Seed hour-jobs of the METADATA stream `trace_list_index` (the prod killer
/// shape) as real parquet files. Each file carries `rows_per_file` rows of
/// the prod schema with interleaved timestamps across the job's files (the
/// k-way DESC sort has to actually merge). Every file is distinct bytes.
async fn cmd_seed_tli(
    hours: usize,
    files_per_job: usize,
    rows_per_file: usize,
    chains: bool,
) -> Result<(), anyhow::Error> {
    anyhow::ensure!(files_per_job >= 2, "need >= 2 files per job");
    let started = Instant::now();
    init_meta().await?;

    let stream = "trace_list_index";
    // the actual default/metadata/trace_list_index schema
    // (core::metadata::trace_list_index::TraceListIndex; M20b pin)
    let schema = Arc::new(Schema::new(vec![
        Field::new(TIMESTAMP_COL, DataType::Int64, false),
        Field::new("stream_name", DataType::Utf8, false),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("trace_id", DataType::Utf8, false),
    ]));
    let now = config::utils::time::now_micros();
    infra::schema::merge(
        ORG,
        stream,
        StreamType::Metadata,
        schema.as_ref(),
        Some(now),
    )
    .await
    .map_err(|e| anyhow::anyhow!("schema merge for {stream}: {e}"))?;

    let mut total_files = 0usize;
    let mut total_bytes = 0usize;
    let mut rng = Rng(0xD1B54A32D192ED03);
    for h in 0..hours {
        let offset = job_offset_micros(h);
        let prefix = format!("files/{ORG}/metadata/{stream}/{}", {
            use chrono::TimeZone;
            chrono::Utc
                .timestamp_nanos(offset * 1000)
                .format("%Y/%m/%d/%H")
        });
        let mut adds = Vec::with_capacity(files_per_job);
        // `chains` (prod l0_multi shape, 2026-08-21 log evidence: "file
        // groups: 88, max group len: 2" on the 12.87GB merges): ~60% of the
        // files span the whole hour (mutually overlapping -> singleton
        // chains) and ~40% form disjoint PAIRS the statistics split chains
        // into 2-file groups — multi-file declared-sorted groups, the one
        // structural feature the lattice corpus lacked.
        let pair_zone_start = files_per_job * 6 / 10;
        // prod hour dirs mix small l0_multi files with BIG previous merge
        // outputs (~5-10M rows, 128k-row row groups, large pages): the first
        // M26_TLI_BIGFILES files carry rows_per_file x M26_TLI_BIGMUL rows
        let big_files: usize = std::env::var("M26_TLI_BIGFILES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        let big_mul: usize = std::env::var("M26_TLI_BIGMUL")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(30);
        for f in 0..files_per_job {
            let rows_per_file = if f < big_files {
                rows_per_file * big_mul
            } else {
                rows_per_file
            };
            let mut ts = Vec::with_capacity(rows_per_file);
            let mut sname = Vec::with_capacity(rows_per_file);
            let mut svc = Vec::with_capacity(rows_per_file);
            let mut trace = Vec::with_capacity(rows_per_file);
            for r in 0..rows_per_file {
                let t = if chains && f >= pair_zone_start {
                    // disjoint pair clusters: pair j = files (2j, 2j+1) in
                    // the pair zone; each file is a compact disjoint slice
                    let pz = f - pair_zone_start;
                    let pair = (pz / 2) as i64;
                    let half = (pz % 2) as i64;
                    offset + pair * 130_000_000 + half * 61_000_000 + r as i64
                } else {
                    // interleaved ts lattice: file f owns residue f, the
                    // DESC order round-robins through every spanning input
                    offset + (r * files_per_job + f) as i64
                };
                ts.push(t);
                sname.push("default".to_string());
                svc.push(format!("service-{}", rng.below(40)));
                let a = rng.next();
                let b = rng.next();
                trace.push(format!("{:016x}{:016x}", a, b));
            }
            let min_ts = *ts.iter().min().unwrap();
            let max_ts = *ts.iter().max().unwrap();
            let batch = arrow::record_batch::RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(ts)) as ArrayRef,
                    Arc::new(StringArray::from(sname)),
                    Arc::new(StringArray::from(svc)),
                    Arc::new(StringArray::from(trace)),
                ],
            )?;
            let file_meta = FileMeta {
                min_ts,
                max_ts,
                records: rows_per_file as i64,
                // prod-shaped original estimate (~110 B/row json)
                original_size: (rows_per_file as i64) * 110,
                compressed_size: 0,
                index_size: 0,
                flattened: false,
                bloom_ver: 0,
            };
            let mut buf = Vec::new();
            if std::env::var("M26_TLI_NOSTATS").is_ok() {
                // statistics-free parquet: neither the inner listing split
                // nor the adapter re-split can prove per-file ordering ->
                // the plan keeps the FULL buffering SortExec (the shape the
                // prod 12.8GB pool peaks imply). Everything else identical.
                let props = parquet::file::properties::WriterProperties::builder()
                    .set_statistics_enabled(parquet::file::properties::EnabledStatistics::None)
                    .set_compression(parquet::basic::Compression::ZSTD(Default::default()))
                    .build();
                let mut writer = parquet::arrow::AsyncArrowWriter::try_new(
                    &mut buf,
                    Arc::clone(&schema),
                    Some(props),
                )?;
                writer.write(&batch).await?;
                writer.close().await?;
            } else {
                let mut writer = config::utils::parquet::new_parquet_writer(
                    &mut buf,
                    &schema,
                    &[],
                    &file_meta,
                    false,
                    None,
                );
                writer.write(&batch).await?;
                writer.close().await?;
            }
            let key = format!("{prefix}/{}.parquet", config::ider::generate_file_name());
            let account = infra::storage::get_account(ORG, &key).unwrap_or_default();
            total_bytes += buf.len();
            let mut meta = file_meta.clone();
            meta.compressed_size = buf.len() as i64;
            infra::storage::put(&account, &key, bytes::Bytes::from(buf)).await?;
            adds.push(FileKey::new(0, account, key, meta, false));
        }
        infra::file_list::batch_process(&adds).await?;
        let job_id = infra::file_list::add_job(ORG, StreamType::Metadata, stream, offset).await?;
        anyhow::ensure!(job_id > 0, "add_job returned {job_id}");
        total_files += files_per_job;
    }
    eprintln!(
        "[seed-tli] done: {hours} jobs / {total_files} parquet rows-files / {:.1} MiB objects \
         ({} rows/file, ~{:.1} MiB original/job), {:.1}s",
        total_bytes as f64 / (1024.0 * 1024.0),
        rows_per_file,
        (files_per_job * rows_per_file * 110) as f64 / (1024.0 * 1024.0),
        started.elapsed().as_secs_f64(),
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// run: the measured job loop
// ---------------------------------------------------------------------------

async fn pending_jobs() -> i64 {
    match infra::file_list::get_pending_jobs_count().await {
        Ok(map) => map
            .values()
            .map(|inner| inner.values().sum::<i64>())
            .sum::<i64>(),
        Err(e) => {
            eprintln!("[run] get_pending_jobs_count failed: {e}");
            -1
        }
    }
}

async fn probe_registries(tag: &str) {
    let schemas_latest = infra::schema::STREAM_SCHEMAS_LATEST.read().await.len();
    let (schemas_streams, schemas_versions) = {
        let r = infra::schema::STREAM_SCHEMAS.read().await;
        (r.len(), r.values().map(|v| v.len()).sum::<usize>())
    };
    let settings = infra::schema::STREAM_SETTINGS.read().await.len();
    let broadcast_q = openobserve_core::db::file_list::broadcast::BROADCAST_QUEUE
        .read()
        .await
        .len();
    let dedup = openobserve_core::db::file_list::DEDUPLICATE_FILES.len();
    let deleted = openobserve_core::db::file_list::DELETED_FILES.len();
    let tasks = tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks();
    // metric SERIES count: non-comment lines of the prometheus exposition
    let series = config::metrics::gather()
        .lines()
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .count();
    eprintln!(
        "[probe {tag}] schemas_latest={schemas_latest} schema_versions={schemas_versions} \
         (streams={schemas_streams}) settings={settings} broadcast_q={broadcast_q} \
         dedup={dedup} deleted_files={deleted} alive_tasks={tasks} metric_series={series} \
         live={}MB rss={}MB",
        live_mb(),
        rss_mb(),
    );
}

async fn cmd_run(files_per_job: usize, max_rounds: usize) -> Result<(), anyhow::Error> {
    init_meta().await?;
    let cfg = config::get_config();
    let schema_clear = std::env::var("M26_SCHEMA_CLEAR").ok().as_deref() == Some("1");
    let settle_secs: u64 = std::env::var("M26_SETTLE_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5);

    // the REAL worker + scheduler wiring (jobs/compactor.rs shape)
    let mut worker =
        openobserve_core::compact::worker::MergeWorker::new(cfg.limit.file_merge_thread_num);
    worker.run()?;
    let job_num = if cfg.compact.job_num > 0 {
        cfg.compact.job_num
    } else {
        cfg.limit.file_merge_thread_num
    };
    let mut scheduler = openobserve_core::compact::worker::JobScheduler::new(job_num, worker.tx());
    scheduler.run()?;
    let scheduler_handle = scheduler.handle();

    let rows0 = infra::file_list::len().await as i64;
    let pending0 = pending_jobs().await;
    anyhow::ensure!(
        pending0 > 0,
        "nothing to do: pending={pending0} (seed first)"
    );
    eprintln!(
        "[run] start: pending_jobs={pending0} file_list_rows={rows0} files_per_job={files_per_job} \
         merge_threads={} job_slots={job_num} schema_clear={schema_clear}",
        cfg.limit.file_merge_thread_num,
    );

    // baseline AFTER init + scheduler spawn, BEFORE the first claim
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let live0 = LIVE_BYTES.load(Ordering::Relaxed);
    let rss0 = rss_mb();
    probe_registries("baseline").await;
    let t0 = Instant::now();

    let per_job_shrink = (files_per_job - 1) as i64;
    let mut last_done = 0i64;
    let mut last_progress = Instant::now();
    let mut next_probe_at = 25i64;
    let mut rounds = 0usize;
    loop {
        rounds += 1;
        if let Err(e) = openobserve_core::compact::run_merge(
            &scheduler_handle,
            openobserve_core::compact::MergeLane::All,
        )
        .await
        {
            eprintln!("[run] run_merge error: {e}");
        }
        if schema_clear {
            infra::schema::STREAM_SCHEMAS_LATEST.write().await.clear();
            infra::schema::STREAM_SCHEMAS.write().await.clear();
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;

        let rows = infra::file_list::len().await as i64;
        let done = (rows0 - rows) / per_job_shrink.max(1);
        let pending = pending_jobs().await;
        if done != last_done {
            eprintln!(
                "[job] t={:.1}s done={done} pending={pending} live={}MB rss={}MB rows={rows}",
                t0.elapsed().as_secs_f64(),
                live_mb(),
                rss_mb(),
            );
            last_done = done;
            last_progress = Instant::now();
        }
        if done >= next_probe_at {
            probe_registries(&format!("done={done}")).await;
            next_probe_at += 25;
        }
        if pending == 0 && done > 0 && last_progress.elapsed().as_secs() >= 5 {
            // all claims drained and no row movement for 5s: jobs are done
            break;
        }
        if last_progress.elapsed().as_secs() > 120 {
            eprintln!("[run] NO PROGRESS for 120s: pending={pending} done={done} — aborting run");
            break;
        }
        if max_rounds > 0 && rounds >= max_rounds {
            eprintln!("[run] max_rounds={max_rounds} reached");
            break;
        }
    }

    // settle: everything in flight drains, then the delta is honest
    tokio::time::sleep(std::time::Duration::from_secs(settle_secs)).await;
    let live1 = LIVE_BYTES.load(Ordering::Relaxed);
    let rss1 = rss_mb();
    probe_registries("final").await;
    let done = last_done.max(1);
    eprintln!(
        "[summary] jobs_done={last_done} elapsed={:.1}s live0={:.1}MB live1={:.1}MB \
         d_live={:.1}MB per_job={:.3}MB rss0={rss0}MB rss1={rss1}MB d_rss={}MB rss_per_job={:.3}MB",
        t0.elapsed().as_secs_f64(),
        live0 as f64 / (1024.0 * 1024.0),
        live1 as f64 / (1024.0 * 1024.0),
        (live1 - live0) as f64 / (1024.0 * 1024.0),
        (live1 - live0) as f64 / (1024.0 * 1024.0) / done as f64,
        rss1 as i64 - rss0 as i64,
        (rss1 as f64 - rss0 as f64) / done as f64,
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// plumbing
// ---------------------------------------------------------------------------

struct StderrLogger;
impl log::Log for StderrLogger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.level() <= log::Level::Warn
            || metadata.target().starts_with("openobserve")
            || metadata.target().starts_with("infra")
            || metadata.target().starts_with("search")
            || metadata.target().contains("heap_profile")
            || (std::env::var("M26_DF_DEBUG").is_ok() && metadata.target().contains("datafusion"))
    }
    fn log(&self, record: &log::Record) {
        let max = if std::env::var("M26_DF_DEBUG").is_ok() {
            log::Level::Debug
        } else {
            log::Level::Info
        };
        if self.enabled(record.metadata()) && record.level() <= max {
            eprintln!("[{}] {}", record.level(), record.args());
        }
    }
    fn flush(&self) {}
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
    // M27: opt-in sampling heap profiler (env-gated; inert when unset)
    config::heap_profile::init();
    static LOGGER: StderrLogger = StderrLogger;
    if log::set_logger(&LOGGER).is_ok() {
        log::set_max_level(if std::env::var("M26_DF_DEBUG").is_ok() {
            log::LevelFilter::Debug
        } else {
            log::LevelFilter::Info
        });
    }
    let args: Vec<String> = std::env::args().collect();
    let scratch = std::env::var("M26_SCRATCH")
        .unwrap_or_else(|_| "/home/zhichen/work/m26/m26-data".to_string());
    let data_dir = format!("{scratch}/engine-data");
    let pins = [
        ("ZO_LOCAL_MODE", "true"),
        ("ZO_DATA_DIR", data_dir.as_str()),
        ("ZO_VIX_L0_INDEX_OFF_STREAM_TYPES", "logs"),
        ("ZO_VIX_PLIST_MIN_DOCS", "8192"),
        // prod compactor parity (devops-argocd-prod-ops obs/kustomize):
        ("ZO_NODE_ROLE", "compactor"),
        ("ZO_FILE_MERGE_THREAD_NUM", "4"),
        ("ZO_COMPACT_JOB_NUM", "4"),
        ("ZO_MEMORY_CACHE_ENABLED", "false"),
        ("ZO_COMPACT_FAST_MODE", "true"),
        ("ZO_COMPACT_MAX_FILE_SIZE", "1024"),
        ("ZO_VIX_REBUILD_CONCURRENCY", "1"),
        // box safety: prod pins the per-merge-context pool to 12288MB on a
        // 48Gi pod (the observed 12.87GB peaks); same semantics at a
        // box-safe cap so 4 concurrent cap-fills stay under the 17GB abort
        ("ZO_MEMORY_CACHE_DATAFUSION_MAX_SIZE", "2048"),
    ];
    let mode = args.get(1).map(String::as_str);
    match mode {
        Some("seed") => {
            let streams: usize = args
                .get(2)
                .expect("seed <streams> <hours> <files_per_job> <rows_per_file> <width>")
                .parse()?;
            let hours: usize = args.get(3).expect("hours").parse()?;
            let files_per_job: usize = args.get(4).expect("files_per_job").parse()?;
            let rows_per_file: usize = args.get(5).expect("rows_per_file").parse()?;
            let width: usize = args.get(6).expect("width (0 = narrow)").parse()?;
            ensure_env_many(&pins);
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(cmd_seed(
                    streams,
                    hours,
                    files_per_job,
                    rows_per_file,
                    width,
                ))
        }
        Some(mode @ ("seed-tli" | "seed-tli-chains")) => {
            let hours: usize = args
                .get(2)
                .expect("seed-tli <hours> <files_per_job> <rows_per_file>")
                .parse()?;
            let files_per_job: usize = args.get(3).expect("files_per_job").parse()?;
            let rows_per_file: usize = args.get(4).expect("rows_per_file").parse()?;
            ensure_env_many(&pins);
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(cmd_seed_tli(
                    hours,
                    files_per_job,
                    rows_per_file,
                    mode == "seed-tli-chains",
                ))
        }
        Some("run") => {
            let files_per_job: usize = args
                .get(2)
                .expect("run <files_per_job> [max_rounds]")
                .parse()?;
            let max_rounds: usize = args.get(3).map(|s| s.parse()).transpose()?.unwrap_or(0);
            ensure_env_many(&pins);
            spawn_rss_sampler();
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(cmd_run(files_per_job, max_rounds))
        }
        _ => {
            eprintln!(
                "usage: m26_job_leak seed <streams> <hours> <files_per_job> <rows_per_file> <width> \
                 | seed-tli <hours> <files_per_job> <rows_per_file> \
                 | run <files_per_job> [max_rounds]"
            );
            std::process::exit(2);
        }
    }
}
