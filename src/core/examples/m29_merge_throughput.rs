//! M29 repro harness: the compactor merge pipeline generates far fewer merge
//! starts than the fleet can execute, leaving ~1M unmerged L0 files inside
//! the hot 24h query window (prod 2026-08-24: 1.15M `l0_*` rows vs 28k
//! merged outputs). This harness seeds a high-L0-count backlog corpus
//! (>=50k tiny index-off `.vix` L0 files across a few streams and ~24 hourly
//! cohorts) into the REAL sqlite meta store + local-disk object store, then
//! drives the REAL generation + claim + merge machinery:
//!
//!   run_generate_job(Current)            — the per-stream offset walk
//!   run_generate_job(Historical)        — the old-data resurrection lane
//!   [after M29: run_generate_job(Debt)] — the merge-debt sweep
//!   run_merge -> JobScheduler -> MergeWorker -> merge_files (rebuild path,
//!   ZO_VIX_REBUILD_CONCURRENCY=1 — the prod gate pin)
//!
//! and measures files-consumed-by-merges per cycle (the
//! `COMPACT_MERGED_FILES` counter) plus the file_list row curve.
//!
//! NO time compression: one cycle ≈ 0.5s and every lane runs at its REAL
//! cadence — the Current lane every cycle (its steady-state output is gated
//! by is_past_hour, not by cadence), the Historical lane every
//! `M29_HIST_EVERY` cycles (default 1200 ≈ 600s wall = prod's
//! ZO_COMPACT_OLD_DATA_INTERVAL pin). The BEFORE run therefore shows exactly
//! the prod pathology: one partial visit per cohort per historical round,
//! silence in between, and the newest closed hours (the old-data dead zone)
//! never visited at all.
//!
//!   seed <streams> <hours> <files_per_hour> <rows_per_file>
//!       One-time seeding (throwaway process): register each stream's schema
//!       with created_at = the oldest seeded hour, build small index-off
//!       .vix templates through the REAL core writer (8 timestamp-interleaved
//!       variants per hour so merges take the rebuild path, not a passthrough
//!       shortcut), upload files_per_hour copies per (stream, hour) and
//!       insert their file_list rows. NO jobs are seeded — job generation is
//!       what this harness measures.
//!
//!   run <streams> [max_secs]
//!       The measured process: register LOCAL_NODE in the compactor ring,
//!       prime the schema cache, start the REAL MergeWorker + JobScheduler,
//!       then loop generation + claim cycles until the corpus drains or
//!       plateaus (no consumption for M29_PLATEAU_SECS with nothing pending).

use std::{sync::Arc, time::Instant};

use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
};
use config::meta::stream::{FileKey, FileMeta, StreamType};
use datafusion::{catalog::TableProvider, datasource::MemTable};

const ORG: &str = "m29org";
const TIMESTAMP_COL: &str = "_timestamp";
/// distinct timestamp-interleaved template variants per hour: a merge group
/// mixes all of them, so the k-way merge truly interleaves rows and the
/// core merge takes the REBUILD path (the gate-controlled prod path)
const TEMPLATE_VARIANTS: usize = 8;

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

fn make_batch(
    schema: &Arc<Schema>,
    rng: &mut Rng,
    base_ts_us: i64,
    rows: usize,
    stride: usize,
    phase: usize,
) -> arrow::record_batch::RecordBatch {
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
                        .map(|r| base_ts_us + (r * stride + phase) as i64)
                        .collect::<Vec<_>>(),
                )),
                ("message", _) => Arc::new(StringArray::from(
                    (0..rows)
                        .map(|_| {
                            let n = 6 + rng.below(6) as usize;
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
                    let prefix: String = name.chars().take(4).collect();
                    Arc::new(StringArray::from(
                        (0..rows)
                            .map(|_| format!("{prefix}-v{}", rng.below(32)))
                            .collect::<Vec<_>>(),
                    ))
                }
            }
        })
        .collect();
    arrow::record_batch::RecordBatch::try_new(Arc::clone(schema), arrays).unwrap()
}

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
    format!("m29s{index:02}")
}

/// Start-of-hour (micros) for cohort `hour_index`: cohort 0 is 2 full hours
/// in the past (a CLOSED, settled hour — but inside the old-data lane's
/// default 2h dead zone), older cohorts walk further back one hour each.
fn cohort_hour_micros(hour_index: usize) -> i64 {
    let hour = 3_600_000_000i64;
    let now = config::utils::time::now_micros();
    let hour_floor = now - now % hour;
    hour_floor - (2 + hour_index as i64) * hour
}

fn hour_prefix(stream: &str, offset_micros: i64) -> String {
    use chrono::TimeZone;
    let t = chrono::Utc.timestamp_nanos(offset_micros * 1000);
    format!("files/{ORG}/logs/{stream}/{}", t.format("%Y/%m/%d/%H"))
}

async fn cmd_seed(
    streams: usize,
    hours: usize,
    files_per_hour: usize,
    rows_per_file: usize,
) -> Result<(), anyhow::Error> {
    anyhow::ensure!(files_per_hour >= 2, "need >= 2 files per hour");
    let started = Instant::now();
    init_meta().await?;

    // register stream schemas with created_at = the OLDEST seeded hour, so
    // the current-lane generator's offset walk starts at the corpus
    let schema = logs_schema();
    let created_at = cohort_hour_micros(hours - 1);
    for s in 0..streams {
        let name = stream_name(s);
        infra::schema::merge(
            ORG,
            &name,
            StreamType::Logs,
            schema.as_ref(),
            Some(created_at),
        )
        .await
        .map_err(|e| anyhow::anyhow!("schema merge for {name}: {e}"))?;
    }
    eprintln!(
        "[seed] registered {streams} stream schemas, created_at={created_at} ({} hours back)",
        hours + 2
    );

    // per (hour, variant): a real index-off .vix template built through the
    // REAL move builder, timestamps interleaved across variants so merge
    // groups cannot passthrough-copy
    let mut total_files = 0usize;
    let mut total_bytes = 0usize;
    for h in 0..hours {
        let hour_start = cohort_hour_micros(h);
        let mut templates: Vec<(bytes::Bytes, FileMeta)> = Vec::with_capacity(TEMPLATE_VARIANTS);
        for v in 0..TEMPLATE_VARIANTS {
            let mut rng =
                Rng(0x9E3779B97F4A7C15 ^ ((h * 31 + v) as u64).wrapping_mul(0xA24BAED4963EE407));
            let batch = make_batch(
                &schema,
                &mut rng,
                hour_start,
                rows_per_file,
                TEMPLATE_VARIANTS,
                v,
            );
            let table: Arc<dyn TableProvider> =
                Arc::new(MemTable::try_new(Arc::clone(&schema), vec![vec![batch]])?);
            let result = openobserve_core::vix::core_writer::write_core_file_from_tables(
                "m29-template",
                StreamType::Logs,
                Arc::clone(&schema),
                vec![table],
                &["message".to_string()],
                &[],
                false,
                0,
            )
            .await?;
            anyhow::ensure!(
                result.stats.index_size == 0 && result.index.is_none(),
                "template must be INDEX-OFF (is ZO_VIX_L0_INDEX_OFF_STREAM_TYPES=logs set?)"
            );
            let data = bytes::Bytes::from(result.data.clone());
            let meta = FileMeta {
                min_ts: result.stats.min_ts,
                max_ts: result.stats.max_ts,
                records: result.stats.row_count as i64,
                original_size: (result.stats.row_count as i64) * 140,
                compressed_size: data.len() as i64,
                index_size: 0,
                flattened: false,
                bloom_ver: 0,
            };
            templates.push((data, meta));
        }
        for s in 0..streams {
            let name = stream_name(s);
            let prefix = hour_prefix(&name, hour_start);
            let mut adds = Vec::with_capacity(files_per_hour);
            for f in 0..files_per_hour {
                let (data, meta) = &templates[f % TEMPLATE_VARIANTS];
                let key = format!("{prefix}/l0_{}.vix", config::ider::generate_file_name());
                let account = infra::storage::get_account(ORG, &key).unwrap_or_default();
                infra::storage::put(&account, &key, data.clone()).await?;
                total_bytes += data.len();
                adds.push(FileKey::new(0, account, key, meta.clone(), false));
            }
            infra::file_list::batch_process(&adds).await?;
            total_files += files_per_hour;
        }
        if h % 4 == 0 {
            eprintln!(
                "[seed] cohort {h}/{hours} done ({total_files} files so far, {:.1}s)",
                started.elapsed().as_secs_f64()
            );
        }
    }
    eprintln!(
        "[seed] done: {streams} streams x {hours} hours x {files_per_hour} files = {total_files} \
         L0 rows / {:.1} MiB objects, NO jobs seeded, {:.1}s",
        total_bytes as f64 / (1024.0 * 1024.0),
        started.elapsed().as_secs_f64(),
    );
    Ok(())
}

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

fn merged_files_counter() -> u64 {
    config::metrics::COMPACT_MERGED_FILES
        .with_label_values(&[ORG, StreamType::Logs.as_str()])
        .get()
}

async fn cmd_run(streams: usize, max_secs: u64) -> Result<(), anyhow::Error> {
    use config::meta::cluster::{CompactionJobType, Role};
    init_meta().await?;
    let cfg = config::get_config();

    // this process IS the compactor fleet's single node: put it on the
    // consistent-hash ring so the generators own every stream
    let node = config::cluster::LOCAL_NODE.clone();
    infra::cluster::add_node_to_consistent_hash(&node, &Role::Compactor, None).await;

    // prime the schema cache (list_streams_from_cache reads it)
    for s in 0..streams {
        let name = stream_name(s);
        let schema = infra::schema::get(ORG, &name, StreamType::Logs).await?;
        anyhow::ensure!(
            !schema.fields().is_empty(),
            "stream {name} has no schema — seed first"
        );
    }

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

    let hist_every: u64 = std::env::var("M29_HIST_EVERY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1200);
    // M29 debt lane at its real cadence: ZO_COMPACT_MERGE_DEBT_INTERVAL
    // (default 60s) ≈ 120 cycles. 0 disables (the BEFORE shape).
    let debt_every: u64 = std::env::var("M29_DEBT_EVERY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(120);
    let plateau_secs: u64 = std::env::var("M29_PLATEAU_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(660);

    let rows0 = infra::file_list::len().await as i64;
    let consumed0 = merged_files_counter();
    eprintln!(
        "[run] start: rows={rows0} merge_workers={} job_slots={job_num} rebuild_gate={} \
         generation lanes: current=every cycle, historical=every {hist_every} cycles, \
         debt=every {debt_every} cycles (0=off)",
        cfg.limit.file_merge_thread_num, cfg.common.vix_rebuild_concurrency,
    );

    let t0 = Instant::now();
    let mut cycle = 0u64;
    let mut last_consumed = consumed0;
    let mut last_progress = Instant::now();
    let mut consumed_curve: Vec<(f64, u64)> = vec![(0.0, 0)];
    loop {
        cycle += 1;
        if let Err(e) =
            openobserve_core::compact::run_generate_job(CompactionJobType::Current).await
        {
            eprintln!("[run] generate current error: {e}");
        }
        if cycle % hist_every == 1
            && let Err(e) =
                openobserve_core::compact::run_generate_job(CompactionJobType::Historical).await
        {
            eprintln!("[run] generate historical error: {e}");
        }
        if debt_every > 0
            && cycle % debt_every == 1
            && let Err(e) =
                openobserve_core::compact::run_generate_job(CompactionJobType::Debt).await
        {
            eprintln!("[run] generate merge-debt error: {e}");
        }
        if let Err(e) = openobserve_core::compact::run_merge(
            &scheduler_handle,
            openobserve_core::compact::MergeLane::All,
        )
        .await
        {
            eprintln!("[run] run_merge error: {e}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let consumed = merged_files_counter();
        let pending = pending_jobs().await;
        if consumed != last_consumed {
            let rows = infra::file_list::len().await as i64;
            eprintln!(
                "[cycle {cycle} t={:.0}s] consumed=+{} total={} pending={pending} rows={rows}",
                t0.elapsed().as_secs_f64(),
                consumed - last_consumed,
                consumed - consumed0,
            );
            last_consumed = consumed;
            last_progress = Instant::now();
        }
        consumed_curve.push((t0.elapsed().as_secs_f64(), consumed - consumed0));

        if last_progress.elapsed().as_secs() >= plateau_secs && pending == 0 {
            eprintln!(
                "[run] PLATEAU: no consumption for {plateau_secs}s with nothing pending — stopping"
            );
            break;
        }
        if t0.elapsed().as_secs() >= max_secs {
            eprintln!("[run] max_secs={max_secs} reached");
            break;
        }
    }

    // summary: total consumption, the sustained rate over the ACTIVE window
    // (first to last consumption), and where the corpus ended up
    let rows1 = infra::file_list::len().await as i64;
    let consumed_total = merged_files_counter() - consumed0;
    let active_start = consumed_curve
        .iter()
        .find(|(_, c)| *c > 0)
        .map(|(t, _)| *t)
        .unwrap_or(0.0);
    let active_end = consumed_curve
        .iter()
        .rev()
        .find(|(_, c)| *c == consumed_total)
        .map(|(t, _)| *t)
        .unwrap_or_else(|| t0.elapsed().as_secs_f64());
    let active = (active_end - active_start).max(0.001);
    eprintln!(
        "[summary] cycles={cycle} elapsed={:.0}s files_consumed={consumed_total} \
         rows: {rows0} -> {rows1} (drained {}) active_window={active:.0}s \
         sustained_files_per_sec={:.1}",
        t0.elapsed().as_secs_f64(),
        rows0 - rows1,
        consumed_total as f64 / active,
    );
    Ok(())
}

struct StderrLogger;
impl log::Log for StderrLogger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.level() <= log::Level::Warn
            || metadata.target().starts_with("openobserve")
            || metadata.target().starts_with("infra")
    }
    fn log(&self, record: &log::Record) {
        if self.enabled(record.metadata()) && record.level() <= log::Level::Info {
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
    static LOGGER: StderrLogger = StderrLogger;
    if log::set_logger(&LOGGER).is_ok() {
        log::set_max_level(log::LevelFilter::Info);
    }
    let args: Vec<String> = std::env::args().collect();
    let scratch =
        std::env::var("M29_SCRATCH").unwrap_or_else(|_| "/home/zhichen/work/m29-data".to_string());
    let data_dir = format!("{scratch}/engine-data");
    let pins = [
        ("ZO_LOCAL_MODE", "true"),
        ("ZO_DATA_DIR", data_dir.as_str()),
        // prod merge-path parity (obs-env configmap 2026-08-24), except the
        // worker count: 10 workers ~ the 10-pod fleet's concurrent job slots
        ("ZO_VIX_L0_INDEX_OFF_STREAM_TYPES", "logs"),
        ("ZO_VIX_PLIST_MIN_DOCS", "8192"),
        ("ZO_NODE_ROLE", "compactor"),
        ("ZO_FILE_MERGE_THREAD_NUM", "10"),
        ("ZO_COMPACT_JOB_NUM", "10"),
        ("ZO_MEMORY_CACHE_ENABLED", "false"),
        ("ZO_COMPACT_FAST_MODE", "true"),
        ("ZO_COMPACT_MAX_FILE_SIZE", "1024"),
        ("ZO_VIX_REBUILD_CONCURRENCY", "1"),
        ("ZO_MEMORY_CACHE_DATAFUSION_MAX_SIZE", "2048"),
    ];
    let mode = args.get(1).map(String::as_str);
    match mode {
        Some("seed") => {
            let streams: usize = args
                .get(2)
                .expect("seed <streams> <hours> <files_per_hour> <rows_per_file>")
                .parse()?;
            let hours: usize = args.get(3).expect("hours").parse()?;
            let files_per_hour: usize = args.get(4).expect("files_per_hour").parse()?;
            let rows_per_file: usize = args.get(5).expect("rows_per_file").parse()?;
            ensure_env_many(&pins);
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(cmd_seed(streams, hours, files_per_hour, rows_per_file))
        }
        Some("run") => {
            let streams: usize = args.get(2).expect("run <streams> [max_secs]").parse()?;
            let max_secs: u64 = args.get(3).map(|s| s.parse()).transpose()?.unwrap_or(900);
            ensure_env_many(&pins);
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(cmd_run(streams, max_secs))
        }
        _ => {
            eprintln!(
                "usage: m29_merge_throughput seed <streams> <hours> <files_per_hour> <rows_per_file> \
                 | run <streams> [max_secs]"
            );
            std::process::exit(2);
        }
    }
}
