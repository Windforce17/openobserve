// Copyright 2026 OpenObserve Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

//! Reproducible whole-file cache churn scenarios.
//!
//! Each ignored test runs in a separate process. That isolates the process-global
//! config and cache statics, so the same source works against the bucketed cache
//! and the moka replacement.

use std::{
    collections::BTreeMap,
    env,
    error::Error,
    fs::{self, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use infra::cache::file_data::{self, disk, memory};
use serde::Serialize;
use tempfile::TempDir;
use tokio::{task::JoinHandle, time::sleep};

const TASKS: usize = 32;
const DATA_WARM_ENTRIES: u64 = 32;
const RESULT_WARM_ENTRIES: u64 = 128;
const RESULT_SAMPLE_EVERY_SETS: u64 = 128;
const DEFAULT_DURATION_SECS: u64 = 30;
const MAX_DURATION_SECS: u64 = 45;
const QUIESCE_TIMEOUT_SECS: u64 = 10;

type BenchResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Clone, Copy)]
enum Operation {
    Get,
    Exist,
    Set,
    WarmSet,
    Stats,
}

impl Operation {
    const ALL: [(Self, &'static str); 5] = [
        (Self::Get, "get"),
        (Self::Exist, "exist"),
        (Self::Set, "set"),
        (Self::WarmSet, "warm_set"),
        (Self::Stats, "stats"),
    ];
}

struct Measurements {
    get: Vec<u32>,
    exist: Vec<u32>,
    set: Vec<u32>,
    warm_set: Vec<u32>,
    stats: Vec<u32>,
}

impl Measurements {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            get: Vec::with_capacity(capacity),
            exist: Vec::with_capacity(capacity / 4),
            set: Vec::with_capacity(capacity / 2),
            warm_set: Vec::new(),
            stats: Vec::new(),
        }
    }

    fn values(&self, operation: Operation) -> &[u32] {
        match operation {
            Operation::Get => &self.get,
            Operation::Exist => &self.exist,
            Operation::Set => &self.set,
            Operation::WarmSet => &self.warm_set,
            Operation::Stats => &self.stats,
        }
    }

    fn record(&mut self, operation: Operation, elapsed: Duration) {
        let micros = elapsed.as_micros().min(u128::from(u32::MAX)) as u32;
        match operation {
            Operation::Get => self.get.push(micros),
            Operation::Exist => self.exist.push(micros),
            Operation::Set => self.set.push(micros),
            Operation::WarmSet => self.warm_set.push(micros),
            Operation::Stats => self.stats.push(micros),
        }
    }

    fn merge(&mut self, mut other: Self) {
        self.get.append(&mut other.get);
        self.exist.append(&mut other.exist);
        self.set.append(&mut other.set);
        self.warm_set.append(&mut other.warm_set);
        self.stats.append(&mut other.stats);
    }

    fn output(
        &self,
    ) -> (
        BTreeMap<String, u64>,
        BTreeMap<String, Option<LatencySummary>>,
    ) {
        let mut ops = BTreeMap::new();
        let mut latency_us = BTreeMap::new();
        for (operation, name) in Operation::ALL {
            let values = self.values(operation);
            ops.insert(name.to_string(), values.len() as u64);
            latency_us.insert(name.to_string(), LatencySummary::from_samples(values));
        }
        (ops, latency_us)
    }

    fn max_stall_us(&self) -> u64 {
        Operation::ALL
            .into_iter()
            .flat_map(|(operation, _)| self.values(operation).iter().copied())
            .max()
            .unwrap_or_default() as u64
    }
}

struct TaskStats {
    measurements: Measurements,
    get_attempts: u64,
    get_hits: u64,
    fill_survivor_samples: Vec<u64>,
}

impl TaskStats {
    fn new() -> Self {
        Self {
            measurements: Measurements::with_capacity(4096),
            get_attempts: 0,
            get_hits: 0,
            fill_survivor_samples: Vec::new(),
        }
    }

    fn merge(&mut self, other: Self) {
        self.measurements.merge(other.measurements);
        self.get_attempts += other.get_attempts;
        self.get_hits += other.get_hits;
        self.fill_survivor_samples
            .extend(other.fill_survivor_samples);
    }
}

#[derive(Serialize)]
struct LatencySummary {
    samples: u64,
    p50_us: u64,
    p90_us: u64,
    p99_us: u64,
    p999_us: u64,
    max_us: u64,
}

impl LatencySummary {
    fn from_samples(values: &[u32]) -> Option<Self> {
        if values.is_empty() {
            return None;
        }
        let mut sorted = values.to_vec();
        sorted.sort_unstable();
        let percentile = |fraction: f64| -> u64 {
            let index = ((sorted.len() as f64 * fraction).ceil() as usize)
                .saturating_sub(1)
                .min(sorted.len() - 1);
            sorted[index] as u64
        };
        Some(Self {
            samples: sorted.len() as u64,
            p50_us: percentile(0.50),
            p90_us: percentile(0.90),
            p99_us: percentile(0.99),
            p999_us: percentile(0.999),
            max_us: sorted[sorted.len() - 1] as u64,
        })
    }
}

#[derive(Serialize)]
struct ScenarioRecord {
    scenario: String,
    code_label: String,
    ops: BTreeMap<String, u64>,
    latency_us: BTreeMap<String, Option<LatencySummary>>,
    hit_ratio: f64,
    final_used_bytes: u64,
    capacity_bytes: u64,
    final_entries: u64,
    on_disk_bytes: u64,
    orphan_file_count: u64,
    fill_survivor_samples: Vec<u64>,
    max_single_op_stall_us: u64,
    wall_time_secs: f64,
}

struct BenchDirs {
    temp_dir: TempDir,
    cache_dir: PathBuf,
}

impl BenchDirs {
    fn disk() -> BenchResult<Self> {
        let dirs = Self::new()?;
        configure_disk_env(dirs.temp_dir.path());
        Ok(dirs)
    }

    fn memory() -> BenchResult<Self> {
        let dirs = Self::new()?;
        configure_memory_env(dirs.temp_dir.path());
        Ok(dirs)
    }

    fn new() -> BenchResult<Self> {
        let temp_dir = tempfile::Builder::new()
            .prefix("openobserve-cache-bench-")
            .tempdir()
            .map_err(|error| bench_error(format!("create benchmark temp dir failed: {error}")))?;
        let cache_dir = temp_dir.path().join("cache");
        fs::create_dir_all(&cache_dir).map_err(|error| {
            bench_error(format!(
                "create benchmark cache dir {} failed: {error}",
                cache_dir.display()
            ))
        })?;
        Ok(Self {
            temp_dir,
            cache_dir,
        })
    }

    fn close(self) -> BenchResult {
        let path = self.temp_dir.path().to_path_buf();
        self.temp_dir.close().map_err(|error| {
            bench_error(format!(
                "remove benchmark temp dir {} failed: {error}",
                path.display()
            ))
        })
    }
}

#[derive(Clone)]
struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    fn seeded(seed: u64) -> Self {
        Self { state: seed.max(1) }
    }

    fn next(&mut self) -> u64 {
        let mut value = self.state;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.state = value;
        value
    }

    fn below(&mut self, upper: u64) -> u64 {
        debug_assert!(upper > 0);
        self.next() % upper
    }
}

/// A cubic transform is cheap and keeps most reads close to the most recent
/// keys. It is stable across old and new runs because every task has its own RNG.
fn zipf_like_offset(rng: &mut XorShift64, length: u64) -> u64 {
    if length <= 1 {
        return 0;
    }
    let uniform = (rng.next() as f64) / (u64::MAX as f64);
    ((uniform.powi(3) * length as f64) as u64).min(length.saturating_sub(1))
}

fn read_id(rng: &mut XorShift64, task_id: usize, set_count: u64, warm_entries: u64) -> u64 {
    // Four out of five reads target the shared recent warm set. The rest target
    // earlier entries from this task. This avoids scheduler-dependent key choice.
    if set_count > 0 && rng.below(5) == 0 {
        let offset = zipf_like_offset(rng, set_count);
        warm_entries + task_id as u64 + (set_count - 1 - offset) * TASKS as u64
    } else {
        warm_entries - 1 - zipf_like_offset(rng, warm_entries)
    }
}

fn disk_data_key(id: u64) -> String {
    let day = 1 + (id / 24 % 28);
    let hour = id % 24;
    format!("files/bench-org/logs/bench-stream/2026/10/{day:02}/{hour:02}/{id:020}.parquet")
}

fn disk_result_key(id: u64) -> String {
    let start = 1_700_000_000_000_000_u64 + id * 60_000_000;
    let end = start + 30_000_000;
    format!("results/bench-org/logs/bench-stream/{id:016x}_30_zo_sql_key/{start}_{end}_1_0.json")
}

fn payloads(min_size: usize, count: usize) -> Arc<Vec<Bytes>> {
    let mut values = Vec::with_capacity(count);
    for exponent in 0..count {
        let size = min_size << exponent;
        let byte = (exponent as u8).wrapping_mul(37).wrapping_add(11);
        values.push(Bytes::from(vec![byte; size]));
    }
    Arc::new(values)
}

fn set_env(key: &str, value: &str) {
    // Ignored scenarios run one per process and call this before config is read.
    unsafe {
        env::set_var(key, value);
    }
}

fn configure_common_env(root: &Path) {
    let data_dir = root.join("data");
    let cache_dir = root.join("cache");
    let tmp_dir = root.join("tmp");
    set_env("ZO_DATA_DIR", &data_dir.to_string_lossy());
    set_env("ZO_DATA_CACHE_DIR", &cache_dir.to_string_lossy());
    set_env("ZO_DATA_TMP_DIR", &tmp_dir.to_string_lossy());
    set_env("ZO_DATA_WAL_DIR", &root.join("wal").to_string_lossy());
    set_env("ZO_DISK_CACHE_BUCKET_NUM", "16");
    set_env("ZO_DISK_CACHE_BLOCK_ENABLED", "false");
    set_env("ZO_DISK_CACHE_GC_INTERVAL", "0");
    set_env("ZO_MEMORY_CACHE_BUCKET_NUM", "16");
    set_env("ZO_MEMORY_CACHE_GC_INTERVAL", "0");
}

fn configure_disk_env(root: &Path) {
    configure_common_env(root);
    set_env("ZO_DISK_CACHE_ENABLED", "true");
    set_env("ZO_DISK_CACHE_STRATEGY", "time_lru");
    set_env("ZO_DISK_CACHE_MAX_SIZE", "2048");
    set_env("ZO_DISK_RESULT_CACHE_MAX_SIZE", "64");
    set_env("ZO_DISK_AGGREGATION_CACHE_MAX_SIZE", "64");
    set_env("ZO_DISK_CACHE_RELEASE_SIZE", "0");
    set_env("ZO_MEMORY_CACHE_ENABLED", "false");
}

fn configure_memory_env(root: &Path) {
    configure_common_env(root);
    set_env("ZO_DISK_CACHE_ENABLED", "false");
    set_env("ZO_MEMORY_CACHE_ENABLED", "true");
    set_env("ZO_MEMORY_CACHE_STRATEGY", "lru");
    set_env("ZO_MEMORY_CACHE_MAX_SIZE", "512");
    set_env("ZO_MEMORY_CACHE_RELEASE_SIZE", "0");
}

fn duration_from_env() -> BenchResult<Duration> {
    let value = match env::var("CACHE_BENCH_DURATION_SECS") {
        Ok(value) => value,
        Err(env::VarError::NotPresent) => return Ok(Duration::from_secs(DEFAULT_DURATION_SECS)),
        Err(error) => {
            return Err(bench_error(format!(
                "read CACHE_BENCH_DURATION_SECS failed: {error}"
            )));
        }
    };
    let seconds = value.parse::<u64>().map_err(|error| {
        bench_error(format!(
            "CACHE_BENCH_DURATION_SECS must be an integer from 1 to {MAX_DURATION_SECS}, got {value:?}: {error}"
        ))
    })?;
    if !(1..=MAX_DURATION_SECS).contains(&seconds) {
        return Err(bench_error(format!(
            "CACHE_BENCH_DURATION_SECS must be from 1 to {MAX_DURATION_SECS}, got {seconds}"
        )));
    }
    Ok(Duration::from_secs(seconds))
}

fn bench_error(message: String) -> Box<dyn Error + Send + Sync> {
    Box::new(io::Error::other(message))
}

async fn initialize_cache() -> BenchResult {
    file_data::init()
        .await
        .map_err(|error| bench_error(format!("initialize file-data cache failed: {error}")))
}

async fn measured_disk_set(
    stats: &mut TaskStats,
    operation: Operation,
    key: &str,
    data: Bytes,
) -> BenchResult {
    let started = Instant::now();
    let result = disk::set(key, data).await;
    stats.measurements.record(operation, started.elapsed());
    result.map_err(|error| bench_error(format!("disk::set failed for {key}: {error}")))
}

async fn measured_memory_set(stats: &mut TaskStats, key: &str, data: Bytes) -> BenchResult {
    let started = Instant::now();
    let result = memory::set(key, data).await;
    stats.measurements.record(Operation::Set, started.elapsed());
    result.map_err(|error| bench_error(format!("memory::set failed for {key}: {error}")))
}

async fn measured_disk_get(stats: &mut TaskStats, key: &str) {
    let started = Instant::now();
    let hit = disk::get(key, None).await.is_some();
    stats.measurements.record(Operation::Get, started.elapsed());
    stats.get_attempts += 1;
    stats.get_hits += u64::from(hit);
}

async fn measured_memory_get(stats: &mut TaskStats, key: &str) {
    let started = Instant::now();
    let hit = memory::get(key, None).await.is_some();
    stats.measurements.record(Operation::Get, started.elapsed());
    stats.get_attempts += 1;
    stats.get_hits += u64::from(hit);
}

async fn measured_disk_exist(stats: &mut TaskStats, key: &str) {
    let started = Instant::now();
    let _ = disk::exist(key).await;
    stats
        .measurements
        .record(Operation::Exist, started.elapsed());
}

async fn measured_memory_exist(stats: &mut TaskStats, key: &str) {
    let started = Instant::now();
    let _ = memory::exist(key).await;
    stats
        .measurements
        .record(Operation::Exist, started.elapsed());
}

async fn measured_disk_stats(
    stats: &mut TaskStats,
    file_type: disk::FileType,
) -> (usize, usize, usize) {
    let started = Instant::now();
    let result = disk::stats(file_type).await;
    stats
        .measurements
        .record(Operation::Stats, started.elapsed());
    result
}

async fn measured_memory_stats(stats: &mut TaskStats) -> (usize, usize, usize) {
    let started = Instant::now();
    let result = memory::stats().await;
    stats
        .measurements
        .record(Operation::Stats, started.elapsed());
    result
}

async fn disk_data_task(
    task_id: usize,
    deadline: Instant,
    data: Arc<Vec<Bytes>>,
) -> BenchResult<TaskStats> {
    let mut rng = XorShift64::seeded(0x6d5a_56da_4f17_89c3 ^ task_id as u64);
    let mut stats = TaskStats::new();
    let mut set_count = 0;
    while Instant::now() < deadline {
        match rng.below(100) {
            0..=59 => {
                let key = disk_data_key(read_id(&mut rng, task_id, set_count, DATA_WARM_ENTRIES));
                measured_disk_get(&mut stats, &key).await;
            }
            60..=69 => {
                let key = disk_data_key(read_id(&mut rng, task_id, set_count, DATA_WARM_ENTRIES));
                measured_disk_exist(&mut stats, &key).await;
            }
            _ => {
                let id = DATA_WARM_ENTRIES + task_id as u64 + set_count * TASKS as u64;
                set_count += 1;
                let key = disk_data_key(id);
                let value = data[rng.below(data.len() as u64) as usize].clone();
                measured_disk_set(&mut stats, Operation::Set, &key, value).await?;
            }
        }
    }
    Ok(stats)
}

async fn disk_result_task(
    task_id: usize,
    deadline: Instant,
    data: Arc<Vec<Bytes>>,
    global_sets: Arc<std::sync::atomic::AtomicU64>,
) -> BenchResult<TaskStats> {
    let mut rng = XorShift64::seeded(0x1969_1021_45d9_f3b7 ^ task_id as u64);
    let mut stats = TaskStats::new();
    let mut set_count = 0;
    while Instant::now() < deadline {
        match rng.below(100) {
            0..=64 => {
                let key =
                    disk_result_key(read_id(&mut rng, task_id, set_count, RESULT_WARM_ENTRIES));
                measured_disk_get(&mut stats, &key).await;
            }
            65..=74 => {
                let key =
                    disk_result_key(read_id(&mut rng, task_id, set_count, RESULT_WARM_ENTRIES));
                measured_disk_exist(&mut stats, &key).await;
            }
            _ => {
                let id = RESULT_WARM_ENTRIES + task_id as u64 + set_count * TASKS as u64;
                set_count += 1;
                let key = disk_result_key(id);
                let value = data[rng.below(data.len() as u64) as usize].clone();
                measured_disk_set(&mut stats, Operation::Set, &key, value).await?;
                let number = global_sets.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                if number.is_multiple_of(RESULT_SAMPLE_EVERY_SETS) {
                    let (_, _, entries) =
                        measured_disk_stats(&mut stats, disk::FileType::Result).await;
                    stats.fill_survivor_samples.push(entries as u64);
                }
            }
        }
    }
    Ok(stats)
}

async fn memory_task(
    task_id: usize,
    deadline: Instant,
    data: Arc<Vec<Bytes>>,
) -> BenchResult<TaskStats> {
    let mut rng = XorShift64::seeded(0x8f21_9d0b_c36e_41a5 ^ task_id as u64);
    let mut stats = TaskStats::new();
    let mut set_count = 0;
    while Instant::now() < deadline {
        match rng.below(100) {
            0..=59 => {
                let key = disk_data_key(read_id(&mut rng, task_id, set_count, DATA_WARM_ENTRIES));
                measured_memory_get(&mut stats, &key).await;
            }
            60..=69 => {
                let key = disk_data_key(read_id(&mut rng, task_id, set_count, DATA_WARM_ENTRIES));
                measured_memory_exist(&mut stats, &key).await;
            }
            _ => {
                let id = DATA_WARM_ENTRIES + task_id as u64 + set_count * TASKS as u64;
                set_count += 1;
                let key = disk_data_key(id);
                let value = data[rng.below(data.len() as u64) as usize].clone();
                measured_memory_set(&mut stats, &key, value).await?;
            }
        }
    }
    Ok(stats)
}

async fn collect_tasks(tasks: Vec<JoinHandle<BenchResult<TaskStats>>>) -> BenchResult<TaskStats> {
    let mut combined = TaskStats::new();
    for task in tasks {
        let result = task
            .await
            .map_err(|error| bench_error(format!("benchmark task join failed: {error}")))?;
        combined.merge(result?);
    }
    Ok(combined)
}

async fn await_disk_quiescence(root: &Path, expected_bytes: u64) -> BenchResult<(u64, u64)> {
    let deadline = Instant::now() + Duration::from_secs(QUIESCE_TIMEOUT_SECS);
    loop {
        let (bytes, files) = directory_totals(root).map_err(|error| {
            bench_error(format!(
                "measure disk cache directory {} failed: {error}",
                root.display()
            ))
        })?;
        if bytes == expected_bytes {
            return Ok((bytes, files));
        }
        if Instant::now() >= deadline {
            return Err(bench_error(format!(
                "disk cache did not quiesce within {QUIESCE_TIMEOUT_SECS}s: expected {expected_bytes} indexed bytes, found {bytes} bytes in {}",
                root.display()
            )));
        }
        sleep(Duration::from_millis(50)).await;
    }
}

fn directory_totals(root: &Path) -> io::Result<(u64, u64)> {
    // Evictions unlink files asynchronously while this walk runs, so an entry
    // can vanish between read_dir and metadata. Skip it: the caller polls
    // until the total matches the index, so a skipped entry only delays that.
    fn visit(path: &Path, bytes: &mut u64, files: &mut u64) -> io::Result<()> {
        let entries = match fs::read_dir(path) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };
        for entry in entries {
            let entry = entry?;
            let file_type = match entry.file_type() {
                Ok(file_type) => file_type,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
            };
            if file_type.is_dir() {
                visit(&entry.path(), bytes, files)?;
            } else if file_type.is_file() {
                let len = match entry.metadata() {
                    Ok(metadata) => metadata.len(),
                    Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                    Err(error) => return Err(error),
                };
                *bytes = bytes.saturating_add(len);
                *files += 1;
            }
        }
        Ok(())
    }

    if !root.exists() {
        return Ok((0, 0));
    }
    let mut bytes = 0;
    let mut files = 0;
    visit(root, &mut bytes, &mut files)?;
    Ok((bytes, files))
}

// One flat record constructor keeps old and new runs byte-for-byte comparable.
#[allow(clippy::too_many_arguments)]
fn make_record(
    scenario: &str,
    stats: &TaskStats,
    capacity_bytes: usize,
    used_bytes: usize,
    entries: usize,
    on_disk_bytes: u64,
    orphan_file_count: u64,
    wall_time: Duration,
) -> ScenarioRecord {
    let (ops, latency_us) = stats.measurements.output();
    ScenarioRecord {
        scenario: scenario.to_string(),
        code_label: env::var("CACHE_BENCH_LABEL").unwrap_or_else(|_| "unlabeled".to_string()),
        ops,
        latency_us,
        hit_ratio: if stats.get_attempts == 0 {
            0.0
        } else {
            stats.get_hits as f64 / stats.get_attempts as f64
        },
        final_used_bytes: used_bytes as u64,
        capacity_bytes: capacity_bytes as u64,
        final_entries: entries as u64,
        on_disk_bytes,
        orphan_file_count,
        fill_survivor_samples: stats.fill_survivor_samples.clone(),
        max_single_op_stall_us: stats.measurements.max_stall_us(),
        wall_time_secs: wall_time.as_secs_f64(),
    }
}

fn write_record(record: &ScenarioRecord) -> BenchResult {
    let path = env::var("CACHE_BENCH_OUT").unwrap_or_else(|_| "/tmp/cache_bench.jsonl".to_string());
    let output = PathBuf::from(path);
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            bench_error(format!(
                "create benchmark output dir {} failed: {error}",
                parent.display()
            ))
        })?;
    }
    let line = serde_json::to_vec(record)
        .map_err(|error| bench_error(format!("serialize benchmark record failed: {error}")))?;
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&output)
        .map_err(|error| {
            bench_error(format!(
                "open benchmark output {} failed: {error}",
                output.display()
            ))
        })?;
    file.write_all(&line)
        .and_then(|_| file.write_all(b"\n"))
        .map_err(|error| {
            bench_error(format!(
                "append benchmark output {} failed: {error}",
                output.display()
            ))
        })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 32)]
#[ignore = "run with scripts in .work/perf1031-moka/bench"]
async fn disk_data_churn() -> BenchResult {
    let dirs = BenchDirs::disk()?;
    let duration = duration_from_env()?;
    initialize_cache().await?;
    let scenario_started = Instant::now();
    let data = payloads(256 * 1024, 7);
    let mut combined = TaskStats::new();

    for id in 0..DATA_WARM_ENTRIES {
        let key = disk_data_key(id);
        let value = data[id as usize % data.len()].clone();
        measured_disk_set(&mut combined, Operation::WarmSet, &key, value).await?;
    }

    let deadline = Instant::now() + duration;
    let tasks = (0..TASKS)
        .map(|task_id| tokio::spawn(disk_data_task(task_id, deadline, data.clone())))
        .collect();
    combined.merge(collect_tasks(tasks).await?);
    let (capacity, used, entries) = measured_disk_stats(&mut combined, disk::FileType::Data).await;
    let (on_disk_bytes, on_disk_files) =
        await_disk_quiescence(&dirs.cache_dir, used as u64).await?;
    let record = make_record(
        "disk_data_churn",
        &combined,
        capacity,
        used,
        entries,
        on_disk_bytes,
        on_disk_files.saturating_sub(entries as u64),
        scenario_started.elapsed(),
    );
    write_record(&record)?;
    dirs.close()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 32)]
#[ignore = "run with scripts in .work/perf1031-moka/bench"]
async fn disk_result_small() -> BenchResult {
    let dirs = BenchDirs::disk()?;
    let duration = duration_from_env()?;
    initialize_cache().await?;
    let scenario_started = Instant::now();
    let data = payloads(8 * 1024, 4);
    let mut combined = TaskStats::new();

    for id in 0..RESULT_WARM_ENTRIES {
        let key = disk_result_key(id);
        let value = data[id as usize % data.len()].clone();
        measured_disk_set(&mut combined, Operation::WarmSet, &key, value).await?;
    }

    let deadline = Instant::now() + duration;
    let global_sets = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let tasks = (0..TASKS)
        .map(|task_id| {
            tokio::spawn(disk_result_task(
                task_id,
                deadline,
                data.clone(),
                global_sets.clone(),
            ))
        })
        .collect();
    combined.merge(collect_tasks(tasks).await?);
    let (capacity, used, entries) =
        measured_disk_stats(&mut combined, disk::FileType::Result).await;
    let (on_disk_bytes, on_disk_files) =
        await_disk_quiescence(&dirs.cache_dir, used as u64).await?;
    let record = make_record(
        "disk_result_small",
        &combined,
        capacity,
        used,
        entries,
        on_disk_bytes,
        on_disk_files.saturating_sub(entries as u64),
        scenario_started.elapsed(),
    );
    write_record(&record)?;
    dirs.close()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 32)]
#[ignore = "run with scripts in .work/perf1031-moka/bench"]
async fn memory_churn() -> BenchResult {
    let dirs = BenchDirs::memory()?;
    let duration = duration_from_env()?;
    initialize_cache().await?;
    let scenario_started = Instant::now();
    let data = payloads(256 * 1024, 7);
    let mut combined = TaskStats::new();

    for id in 0..DATA_WARM_ENTRIES {
        let key = disk_data_key(id);
        let value = data[id as usize % data.len()].clone();
        let started = Instant::now();
        let result = memory::set(&key, value).await;
        combined
            .measurements
            .record(Operation::WarmSet, started.elapsed());
        result
            .map_err(|error| bench_error(format!("memory warm set failed for {key}: {error}")))?;
    }

    let deadline = Instant::now() + duration;
    let tasks = (0..TASKS)
        .map(|task_id| tokio::spawn(memory_task(task_id, deadline, data.clone())))
        .collect();
    combined.merge(collect_tasks(tasks).await?);
    let (capacity, used, entries) = measured_memory_stats(&mut combined).await;
    let record = make_record(
        "memory_churn",
        &combined,
        capacity,
        used,
        entries,
        0,
        0,
        scenario_started.elapsed(),
    );
    write_record(&record)?;
    dirs.close()
}
