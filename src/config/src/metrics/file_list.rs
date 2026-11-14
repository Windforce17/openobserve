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

use std::{
    future::Future,
    sync::{Arc, LazyLock, Mutex},
    time::{Duration, Instant},
};

use prometheus::{HistogramOpts, HistogramVec, IntGaugeVec, Opts, Registry};

const SLOW_THRESHOLD: Duration = Duration::from_millis(50);
const BUCKETS: &[f64] = &[0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0];

#[derive(Clone, Copy, Debug)]
#[repr(usize)]
pub enum Phase {
    Total,
    Candidates,
    CandidateAcquire,
    CandidateSql,
    CandidateConvert,
    CandidatePrune,
    Snapshot,
    SnapshotDb,
    SnapshotSchedule,
    SnapshotAcquire,
    SnapshotSql,
    SnapshotCollect,
    SnapshotDump,
    SnapshotMerge,
    SnapshotSplit,
    Append,
    Other,
}

const PHASES: &[Phase] = &[
    Phase::Total,
    Phase::Candidates,
    Phase::CandidateAcquire,
    Phase::CandidateSql,
    Phase::CandidateConvert,
    Phase::CandidatePrune,
    Phase::Snapshot,
    Phase::SnapshotDb,
    Phase::SnapshotSchedule,
    Phase::SnapshotAcquire,
    Phase::SnapshotSql,
    Phase::SnapshotCollect,
    Phase::SnapshotDump,
    Phase::SnapshotMerge,
    Phase::SnapshotSplit,
    Phase::Append,
    Phase::Other,
];

impl Phase {
    pub fn label(self) -> &'static str {
        match self {
            Self::Total => "total",
            Self::Candidates => "candidates",
            Self::CandidateAcquire => "candidate_acquire",
            Self::CandidateSql => "candidate_sql",
            Self::CandidateConvert => "candidate_convert",
            Self::CandidatePrune => "candidate_prune",
            Self::Snapshot => "snapshot",
            Self::SnapshotDb => "snapshot_db",
            Self::SnapshotSchedule => "snapshot_schedule",
            Self::SnapshotAcquire => "snapshot_acquire",
            Self::SnapshotSql => "snapshot_sql",
            Self::SnapshotCollect => "snapshot_collect",
            Self::SnapshotDump => "snapshot_dump",
            Self::SnapshotMerge => "snapshot_merge",
            Self::SnapshotSplit => "snapshot_split",
            Self::Append => "append",
            Self::Other => "other",
        }
    }

    pub fn start(self) -> Timer {
        Timer {
            phase: self,
            started: Instant::now(),
            timings: CURRENT.try_with(Arc::clone).ok(),
        }
    }
}

static SECONDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    let metric = HistogramVec::new(
        HistogramOpts::new(
            "query_file_list_phase_seconds",
            "Leader file-list wall time; nested phases and parallel task sums are not additive.",
        )
        .namespace(super::NAMESPACE)
        .const_labels(super::create_const_labels())
        .buckets(BUCKETS.to_vec()),
        &["phase"],
    )
    .expect("file-list histogram created");
    for phase in PHASES {
        metric.with_label_values(&[phase.label()]);
    }
    metric
});

static POOL: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    let metric = IntGaugeVec::new(
        Opts::new(
            "query_file_list_pool_connections",
            "Last observed read-only metadata pool size and idle connections at acquisition.",
        )
        .namespace(super::NAMESPACE)
        .const_labels(super::create_const_labels()),
        &["backend", "state"],
    )
    .expect("file-list pool gauge created");
    for backend in [Backend::Postgres, Backend::Sqlite] {
        for state in ["size", "idle"] {
            metric.with_label_values(&[backend.label(), state]);
        }
    }
    metric
});

pub(super) fn register(registry: &Registry) -> Result<(), prometheus::Error> {
    registry.register(Box::new(SECONDS.clone()))?;
    registry.register(Box::new(POOL.clone()))
}

#[derive(Clone, Copy)]
pub enum Backend {
    Postgres,
    Sqlite,
}

impl Backend {
    fn label(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::Sqlite => "sqlite",
        }
    }
}

pub fn record_pool(backend: Backend, size: u32, idle: usize) {
    POOL.with_label_values(&[backend.label(), "size"])
        .set(i64::from(size));
    POOL.with_label_values(&[backend.label(), "idle"])
        .set(i64::try_from(idle).unwrap_or(i64::MAX));
}

#[derive(Clone)]
pub struct Timings {
    durations: [Duration; PHASES.len()],
}

impl Default for Timings {
    fn default() -> Self {
        Self {
            durations: [Duration::ZERO; PHASES.len()],
        }
    }
}

impl Timings {
    pub fn duration(&self, phase: Phase) -> Duration {
        self.durations[phase as usize]
    }

    fn finish(&mut self, total: Duration) {
        self.durations[Phase::Total as usize] = total;
        self.durations[Phase::Other as usize] = total.saturating_sub(
            self.duration(Phase::Candidates)
                + self.duration(Phase::Snapshot)
                + self.duration(Phase::Append),
        );
    }

    pub fn slow_message(&self, trace_id: &str) -> Option<String> {
        if self.duration(Phase::Total) <= SLOW_THRESHOLD {
            return None;
        }
        use std::fmt::Write;
        let mut message = format!("[trace_id {trace_id}] slow file id list phase_ms");
        for phase in PHASES {
            let _ = write!(
                message,
                " {}={:.3}",
                phase.label(),
                self.duration(*phase).as_secs_f64() * 1000.0
            );
        }
        Some(message)
    }
}

tokio::task_local! {
    static CURRENT: Arc<Mutex<Timings>>;
}

fn record(timings: &Mutex<Timings>, phase: Phase, duration: Duration) {
    {
        let mut timings = timings.lock().unwrap_or_else(|e| e.into_inner());
        timings.durations[phase as usize] += duration;
    }
    SECONDS
        .with_label_values(&[phase.label()])
        .observe(duration.as_secs_f64());
}

#[must_use]
pub struct Timer {
    phase: Phase,
    started: Instant,
    timings: Option<Arc<Mutex<Timings>>>,
}

impl Drop for Timer {
    fn drop(&mut self) {
        if let Some(timings) = &self.timings {
            record(timings, self.phase, self.started.elapsed());
        }
    }
}

/// Capture before spawning: task-local state is not inherited by Tokio tasks.
pub fn inherit<F: Future>(future: F) -> impl Future<Output = F::Output> {
    let timings = CURRENT.try_with(Arc::clone).ok();
    let queued = Instant::now();
    async move {
        if let Some(timings) = timings {
            record(&timings, Phase::SnapshotSchedule, queued.elapsed());
            CURRENT.scope(timings, future).await
        } else {
            future.await
        }
    }
}

pub async fn capture<F: Future>(future: F) -> (F::Output, Timings) {
    let timings = Arc::new(Mutex::new(Timings::default()));
    let started = Instant::now();
    let result = CURRENT.scope(Arc::clone(&timings), future).await;
    let total = started.elapsed();
    let mut snapshot = timings.lock().unwrap_or_else(|e| e.into_inner()).clone();
    snapshot.finish(total);
    for phase in [Phase::Total, Phase::Other] {
        SECONDS
            .with_label_values(&[phase.label()])
            .observe(snapshot.duration(phase).as_secs_f64());
    }
    (result, snapshot)
}

pub async fn observe<F: Future>(trace_id: &str, future: F) -> F::Output {
    let (result, timings) = capture(future).await;
    if let Some(message) = timings.slow_message(trace_id) {
        log::warn!("{message}");
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metatime_registration_labels_and_buckets_are_fixed() {
        let registry = Registry::new();
        super::super::register_metrics(&registry);
        record_pool(Backend::Sqlite, 5, 3);
        let families = registry.gather();
        let histogram = families
            .iter()
            .find(|f| f.name() == "zo_query_file_list_phase_seconds")
            .expect("registered histogram");
        assert_eq!(histogram.get_metric().len(), PHASES.len());
        let labels: std::collections::BTreeSet<_> = histogram
            .get_metric()
            .iter()
            .map(|m| {
                assert_eq!(m.get_label().len(), 4);
                assert_eq!(
                    m.get_histogram()
                        .get_bucket()
                        .iter()
                        .map(|b| b.upper_bound())
                        .collect::<Vec<_>>(),
                    BUCKETS
                );
                m.get_label()
                    .iter()
                    .find(|l| l.name() == "phase")
                    .unwrap()
                    .value()
                    .to_string()
            })
            .collect();
        assert_eq!(
            labels,
            PHASES.iter().map(|p| p.label().to_string()).collect()
        );
        let pool = families
            .iter()
            .find(|f| f.name() == "zo_query_file_list_pool_connections")
            .expect("registered pool gauge");
        assert_eq!(pool.get_metric().len(), 4);
        for metric in pool.get_metric() {
            assert_eq!(metric.get_label().len(), 5);
            let label = |name| {
                metric
                    .get_label()
                    .iter()
                    .find(|l| l.name() == name)
                    .unwrap()
                    .value()
            };
            assert!(["postgres", "sqlite"].contains(&label("backend")));
            assert!(["size", "idle"].contains(&label("state")));
            if label("backend") == "sqlite" {
                let expected = if label("state") == "size" { 5.0 } else { 3.0 };
                assert_eq!(metric.get_gauge().value(), expected);
            }
        }
    }

    #[test]
    fn metatime_slow_warning_strict_threshold_and_trace() {
        let mut timings = Timings::default();
        for total in [Duration::ZERO, SLOW_THRESHOLD] {
            timings.finish(total);
            assert!(timings.slow_message("trace-123").is_none());
        }
        timings.finish(SLOW_THRESHOLD + Duration::from_micros(1));
        let message = timings.slow_message("trace-123").unwrap();
        assert!(message.starts_with("[trace_id trace-123] slow file id list phase_ms"));
        assert!(message.contains("total=50.001"));
        for phase in PHASES {
            assert!(message.contains(&format!(" {}=", phase.label())));
        }
    }

    #[test]
    fn metatime_top_level_partition_is_exact() {
        let mut timings = Timings::default();
        timings.durations[Phase::Candidates as usize] = Duration::from_micros(1234);
        timings.durations[Phase::Snapshot as usize] = Duration::from_micros(5678);
        timings.durations[Phase::Append as usize] = Duration::from_micros(901);
        timings.finish(Duration::from_millis(10));
        assert_eq!(
            timings.duration(Phase::Total),
            timings.duration(Phase::Candidates)
                + timings.duration(Phase::Snapshot)
                + timings.duration(Phase::Append)
                + timings.duration(Phase::Other)
        );
    }

    #[tokio::test]
    async fn metatime_spawn_inherits_and_concurrent_queries_are_isolated() {
        let first = capture(async {
            let _phase = Phase::Snapshot.start();
            tokio::spawn(inherit(async {
                let _sql = Phase::SnapshotSql.start();
                tokio::task::yield_now().await;
                42
            }))
            .await
            .unwrap()
        });
        let second = capture(async {
            let _phase = Phase::Candidates.start();
            tokio::task::yield_now().await;
            Err::<(), _>("unchanged error")
        });
        let ((result, first), (error, second)) = tokio::join!(first, second);
        assert_eq!(result, 42);
        assert_eq!(error, Err("unchanged error"));
        assert!(first.duration(Phase::SnapshotSql) > Duration::ZERO);
        assert!(first.duration(Phase::SnapshotSchedule) > Duration::ZERO);
        assert_eq!(first.duration(Phase::Candidates), Duration::ZERO);
        assert_eq!(second.duration(Phase::SnapshotSql), Duration::ZERO);
        assert!(second.duration(Phase::Candidates) > Duration::ZERO);
        for timings in [first, second] {
            assert_eq!(
                timings.duration(Phase::Total),
                timings.duration(Phase::Candidates)
                    + timings.duration(Phase::Snapshot)
                    + timings.duration(Phase::Append)
                    + timings.duration(Phase::Other)
            );
        }
    }
}
