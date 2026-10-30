// Copyright 2026 OpenObserve Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::{
    cmp::{Ordering, Reverse},
    collections::{BTreeMap, BinaryHeap},
    future::Future,
    time::Duration,
};

use async_trait::async_trait;
use config::{
    cluster::{LOCAL_NODE, is_offline},
    get_config,
    meta::{cluster::RoleGroup, stream::FileKey},
    metrics,
    utils::{hash::Sum64, time::now_micros},
};
use futures::StreamExt;
use hashbrown::HashSet;
use infra::cluster::{
    QUERIER_BACKGROUND_CONSISTENT_HASH, QUERIER_INTERACTIVE_CONSISTENT_HASH,
    get_cached_online_query_nodes,
};
use search::vix::WarmFileOutcome;
use tokio::time::Instant;

/// An effective ring epoch: only online members, captured under one ring lock.
/// Keeping the previous topology, not every warmed file ID, bounds history memory.
#[derive(Clone, Debug, Eq, PartialEq)]
struct RingSnapshot(BTreeMap<u64, String>);

impl RingSnapshot {
    fn from_online(
        ring: &BTreeMap<u64, String>,
        allowed: &HashSet<String>,
        local: &str,
    ) -> Option<Self> {
        if !allowed.contains(local) {
            return None;
        }
        let visible: HashSet<&str> = ring.values().map(String::as_str).collect();
        if !allowed.iter().all(|name| visible.contains(name.as_str())) {
            return None;
        }
        Some(Self(
            ring.iter()
                .filter(|(_, name)| allowed.contains(*name))
                .map(|(hash, name)| (*hash, name.clone()))
                .collect(),
        ))
    }

    fn owner(&self, id: i64) -> Option<&str> {
        let hash = config::utils::hash::gxhash::new().sum64(&id.to_string());
        self.0
            .range(hash..)
            .next()
            .or_else(|| self.0.first_key_value())
            .map(|(_, owner)| owner.as_str())
    }

    fn newly_owned(&self, previous: Option<&Self>, local: &str, file: &FileKey) -> bool {
        file.id > 0
            && file.meta.index_size > 0
            && self.owner(file.id) == Some(local)
            && previous.is_none_or(|old| old.owner(file.id) != Some(local))
    }
}

#[derive(Clone)]
struct Options {
    hours: usize,
    max_files: usize,
    concurrency: usize,
    ring_wait: Duration,
    settle: Duration,
    poll: Duration,
    retries: usize,
    backoff: Duration,
    pass_timeout: Duration,
}

impl Options {
    fn configured() -> Self {
        let cfg = get_config();
        Self::from_limit(&cfg.limit)
    }

    fn from_limit(limit: &config::config::Limit) -> Self {
        let ring_wait = Duration::from_secs(limit.warmup_ring_wait_secs.max(1));
        Self {
            hours: limit.warmup_cache_hours,
            max_files: limit.warmup_cache_max_files.min(50_000),
            concurrency: limit.warmup_cache_concurrency.clamp(1, 32),
            ring_wait,
            // Leave time for at least two observations even with conflicting knobs.
            settle: Duration::from_secs(limit.warmup_ring_settle_secs.max(1)).min(ring_wait / 2),
            poll: Duration::from_secs(limit.warmup_ring_poll_secs.max(1)),
            retries: limit.warmup_busy_retries.min(16),
            backoff: Duration::from_millis(limit.warmup_busy_backoff_ms.clamp(1, 5_000)),
            pass_timeout: Duration::from_secs(limit.warmup_pass_timeout_secs.max(1)),
        }
    }
}

#[async_trait]
trait Backend: Sync {
    async fn snapshot(&self) -> Option<RingSnapshot>;
    async fn files(
        &self,
        current: &RingSnapshot,
        previous: Option<&RingSnapshot>,
        options: &Options,
    ) -> anyhow::Result<Vec<FileKey>>;
    async fn warm(&self, file: &FileKey) -> anyhow::Result<WarmFileOutcome>;
}

struct LocalBackend;

#[async_trait]
impl Backend for LocalBackend {
    async fn snapshot(&self) -> Option<RingSnapshot> {
        let group = if LOCAL_NODE.is_interactive_querier() {
            RoleGroup::Interactive
        } else {
            RoleGroup::Background
        };
        let nodes = get_cached_online_query_nodes(Some(group)).await?;
        let allowed = nodes
            .iter()
            .filter(|node| node.is_querier())
            .map(|node| node.name.clone())
            .collect();
        let ring = match group {
            RoleGroup::Background => QUERIER_BACKGROUND_CONSISTENT_HASH.read().await,
            _ => QUERIER_INTERACTIVE_CONSISTENT_HASH.read().await,
        };
        RingSnapshot::from_online(&ring, &allowed, &LOCAL_NODE.name)
    }

    async fn files(
        &self,
        current: &RingSnapshot,
        previous: Option<&RingSnapshot>,
        options: &Options,
    ) -> anyhow::Result<Vec<FileKey>> {
        let end = now_micros();
        let hours = i64::try_from(options.hours).unwrap_or(i64::MAX);
        let min_ts = end.saturating_sub(hours.saturating_mul(3_600_000_000));
        let mut selected = NewestFiles::new(options.max_files);
        if options.max_files == 0 {
            return Ok(Vec::new());
        }
        let mut candidates = 0;
        for org in crate::service::db::schema::list_organizations_from_cache().await {
            for stream_type in [
                config::meta::stream::StreamType::Logs,
                config::meta::stream::StreamType::Metrics,
                config::meta::stream::StreamType::Traces,
            ] {
                for stream in
                    crate::service::db::schema::list_streams_from_cache(&org, stream_type).await
                {
                    let files = crate::service::file_list::query(
                        "warmup",
                        &org,
                        stream_type,
                        &stream,
                        infra::schema::get_partition_time_level(stream_type),
                        min_ts,
                        end,
                    )
                    .await
                    .map_err(|error| {
                        anyhow::anyhow!("warmup file_list {org}/{stream_type}/{stream}: {error}")
                    })?;
                    // The existing API materializes one stream. Do not retain all
                    // organizations' lists as the legacy startup path did.
                    for (index, file) in files.into_iter().enumerate() {
                        candidates += 1;
                        if current.newly_owned(previous, &LOCAL_NODE.name, &file) {
                            selected.push(file);
                        }
                        if index % 256 == 0 {
                            tokio::task::yield_now().await;
                        }
                    }
                }
            }
        }
        log::info!(
            "[WARMUP] selected {} of {candidates} candidates",
            selected.heap.len()
        );
        Ok(selected.finish())
    }

    async fn warm(&self, file: &FileKey) -> anyhow::Result<WarmFileOutcome> {
        search::vix::warm_file_outcome(
            &file.account,
            &file.key,
            file.meta.compressed_size,
            file.meta.index_size,
            file.meta.index_generation,
        )
        .await
    }
}

struct Candidate(FileKey);

impl Candidate {
    fn rank(&self) -> (i64, i64, &str) {
        (self.0.meta.max_ts, self.0.id, &self.0.key)
    }
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.rank() == other.rank()
    }
}
impl Eq for Candidate {}
impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.rank().cmp(&other.rank())
    }
}

struct NewestFiles {
    heap: BinaryHeap<Reverse<Candidate>>,
    limit: usize,
}

impl NewestFiles {
    fn new(limit: usize) -> Self {
        Self {
            heap: BinaryHeap::new(),
            limit,
        }
    }

    fn push(&mut self, file: FileKey) {
        if self.limit == 0 {
            return;
        }
        let file = Reverse(Candidate(file));
        if self.heap.len() < self.limit {
            self.heap.push(file);
        } else if self.heap.peek().is_some_and(|oldest| file < *oldest) {
            self.heap.pop();
            self.heap.push(file);
        }
    }

    fn finish(self) -> Vec<FileKey> {
        self.heap
            .into_sorted_vec()
            .into_iter()
            .map(|file| file.0.0)
            .collect()
    }
}

fn event(name: &str) {
    metrics::WARMUP_EVENTS_TOTAL
        .with_label_values(&[name])
        .inc();
}

/// Always report a pass, including cancellation while enumerating or reading.
struct Summary {
    epoch: u64,
    started: Instant,
    status: &'static str,
    selected: usize,
    completed: usize,
    counts: BTreeMap<&'static str, usize>,
}

impl Summary {
    fn new(epoch: u64) -> Self {
        Self {
            epoch,
            started: Instant::now(),
            status: "cancelled",
            selected: 0,
            completed: 0,
            counts: BTreeMap::new(),
        }
    }

    fn record(&mut self, outcome: &'static str) {
        *self.counts.entry(outcome).or_default() += 1;
        self.completed += 1;
        metrics::WARMUP_FILES_TOTAL
            .with_label_values(&[outcome])
            .inc();
    }
}

impl Drop for Summary {
    fn drop(&mut self) {
        let seconds = self.started.elapsed().as_secs_f64();
        event(self.status);
        metrics::WARMUP_PASS_SECONDS
            .with_label_values(&[self.status])
            .observe(seconds);
        log::info!(
            "[WARMUP] summary epoch={} status={} selected={} completed={} outcomes={:?} took={seconds:.3}s",
            self.epoch,
            self.status,
            self.selected,
            self.completed,
            self.counts,
        );
    }
}

async fn wait_stable<B: Backend>(backend: &B, options: &Options) -> Option<RingSnapshot> {
    tokio::time::timeout(options.ring_wait, async {
        let mut observed = None;
        let mut since = Instant::now();
        loop {
            let current = backend.snapshot().await;
            if current != observed {
                observed = current;
                since = Instant::now();
            }
            if observed.is_some() && since.elapsed() >= options.settle {
                return observed;
            }
            tokio::time::sleep(options.poll.min(options.settle)).await;
        }
    })
    .await
    .ok()
    .flatten()
}

async fn warm_with_retry<B: Backend>(
    backend: &B,
    file: &FileKey,
    options: &Options,
) -> &'static str {
    let mut retries = 0;
    let mut delay = options.backoff;
    loop {
        match backend.warm(file).await {
            Ok(WarmFileOutcome::Warmed) => return "warmed",
            Ok(WarmFileOutcome::AlreadyWarm) => return "already_warm",
            Ok(WarmFileOutcome::InvalidInput) => return "invalid_input",
            Ok(WarmFileOutcome::Busy) => {
                event("busy");
                if retries >= options.retries {
                    return "busy_exhausted";
                }
                retries += 1;
                event("retry");
                tokio::time::sleep(delay).await;
                delay = delay.saturating_mul(2).min(Duration::from_secs(5));
            }
            Err(error) => {
                log::warn!(
                    "[WARMUP] file={} generation={}: {error:#}",
                    file.key,
                    file.meta.index_generation
                );
                return "error";
            }
        }
    }
}

async fn pass<B: Backend>(
    backend: &B,
    current: &RingSnapshot,
    previous: Option<&RingSnapshot>,
    options: &Options,
    summary: &mut Summary,
) -> anyhow::Result<()> {
    let files = backend.files(current, previous, options).await?;
    summary.selected = files.len();
    // No detached per-file tasks: dropping the stream drops every read future.
    let work = futures::stream::iter(files)
        .map(|file| async move { warm_with_retry(backend, &file, options).await });
    let mut work = work.buffer_unordered(options.concurrency);
    while let Some(outcome) = work.next().await {
        summary.record(outcome);
    }
    Ok(())
}

async fn supervise<B: Backend>(backend: &B, options: &Options) {
    let mut previous = None;
    let mut epoch = 0;
    loop {
        match wait_stable(backend, options).await {
            Some(current) if previous.as_ref() != Some(&current) => {
                epoch += 1;
                let mut summary = Summary::new(epoch);
                match tokio::time::timeout(
                    options.pass_timeout,
                    pass(backend, &current, previous.as_ref(), options, &mut summary),
                )
                .await
                {
                    Ok(Ok(())) => summary.status = "complete",
                    Ok(Err(error)) => {
                        summary.status = "enumeration_error";
                        log::warn!("[WARMUP] epoch={epoch}: {error:#}");
                    }
                    Err(_) => summary.status = "timeout",
                }
                // A terminal pass (including exhausted Busy retries) is not
                // replayed forever. A later epoch adds only new ownership.
                previous = Some(current);
            }
            Some(_) => {}
            None => {
                event("ring_timeout");
                log::warn!(
                    "[WARMUP] local ring did not converge within {:?}; still watching",
                    options.ring_wait
                );
            }
        }
        tokio::time::sleep(options.poll).await;
    }
}

async fn until_cancelled<T>(
    work: impl Future<Output = T>,
    cancel: impl Future<Output = ()>,
) -> Option<T> {
    tokio::select! {
        biased;
        _ = cancel => None,
        result = work => Some(result),
    }
}

pub(super) async fn run() {
    let options = Options::configured();
    if options.hours == 0 {
        event("disabled");
        log::info!("[WARMUP] disabled: ZO_WARMUP_CACHE_HOURS=0");
        return;
    }
    event("started");
    log::info!(
        "[WARMUP] supervisor started: hours={} max_files={} concurrency={} retries={}",
        options.hours,
        options.max_files,
        options.concurrency,
        options.retries
    );
    let _ = until_cancelled(supervise(&LocalBackend, &options), async {
        while !is_offline() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    event("stopped");
    log::info!("[WARMUP] supervisor stopped: node offline");
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering as AtomicOrdering},
    };

    use tokio::sync::{Notify, Semaphore};

    use super::*;

    fn options() -> Options {
        Options {
            hours: 24,
            max_files: 10,
            concurrency: 2,
            ring_wait: Duration::from_millis(20),
            settle: Duration::from_millis(2),
            poll: Duration::from_millis(2),
            retries: 2,
            backoff: Duration::from_millis(1),
            pass_timeout: Duration::from_secs(1),
        }
    }

    fn file(id: i64) -> FileKey {
        let mut file = FileKey::from_file_name(&format!("files/org/logs/stream/{id}.vix"));
        file.id = id;
        file.meta.index_size = 100;
        file.meta.compressed_size = 100;
        file.meta.max_ts = id;
        file
    }

    fn ring(owner: &str) -> RingSnapshot {
        RingSnapshot(BTreeMap::from([(u64::MAX, owner.to_owned())]))
    }

    struct Fake {
        ring: Mutex<Option<RingSnapshot>>,
        ready_at: Instant,
        files: Vec<FileKey>,
        outcomes: Mutex<std::collections::VecDeque<anyhow::Result<WarmFileOutcome>>>,
        calls: AtomicUsize,
        opened: Mutex<Vec<i64>>,
        entered: Notify,
        block: bool,
        leases: Arc<Semaphore>,
    }

    impl Fake {
        fn new(outcomes: Vec<WarmFileOutcome>) -> Self {
            Self {
                ring: Mutex::new(Some(ring("local"))),
                ready_at: Instant::now(),
                files: vec![file(1)],
                outcomes: Mutex::new(outcomes.into_iter().map(Ok).collect()),
                calls: AtomicUsize::new(0),
                opened: Mutex::new(Vec::new()),
                entered: Notify::new(),
                block: false,
                leases: Arc::new(Semaphore::new(10)),
            }
        }
    }

    #[async_trait]
    impl Backend for Fake {
        async fn snapshot(&self) -> Option<RingSnapshot> {
            if Instant::now() < self.ready_at {
                None
            } else {
                self.ring.lock().unwrap().clone()
            }
        }

        async fn files(
            &self,
            current: &RingSnapshot,
            previous: Option<&RingSnapshot>,
            options: &Options,
        ) -> anyhow::Result<Vec<FileKey>> {
            let mut selected = NewestFiles::new(options.max_files);
            for file in &self.files {
                if current.newly_owned(previous, "local", file) {
                    selected.push(file.clone());
                }
            }
            Ok(selected.finish())
        }

        async fn warm(&self, file: &FileKey) -> anyhow::Result<WarmFileOutcome> {
            let _lease = self.leases.acquire().await.unwrap();
            self.calls.fetch_add(1, AtomicOrdering::SeqCst);
            self.opened.lock().unwrap().push(file.id);
            self.entered.notify_one();
            if self.block {
                futures::future::pending::<()>().await;
            }
            self.outcomes
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Ok(WarmFileOutcome::Warmed))
        }
    }

    #[test]
    fn warmup_ring_requires_membership_and_ring_visibility() {
        let allowed = HashSet::from(["local".to_owned(), "other".to_owned()]);
        assert!(RingSnapshot::from_online(&ring("local").0, &allowed, "local").is_none());
        assert!(
            RingSnapshot::from_online(
                &ring("other").0,
                &HashSet::from(["other".to_owned()]),
                "local"
            )
            .is_none()
        );
        let both = BTreeMap::from([
            (1, "local".to_owned()),
            (2, "other".to_owned()),
            (3, "preparing".to_owned()),
        ]);
        let snapshot = RingSnapshot::from_online(&both, &allowed, "local").unwrap();
        assert_eq!(snapshot.0.len(), 2);
    }

    #[test]
    fn warmup_options_clamp_resource_limits_and_zero_timers() {
        let mut limit = config::Limit {
            warmup_cache_max_files: usize::MAX,
            warmup_cache_concurrency: usize::MAX,
            warmup_busy_retries: usize::MAX,
            warmup_busy_backoff_ms: u64::MAX,
            warmup_ring_wait_secs: 0,
            warmup_ring_settle_secs: 0,
            warmup_ring_poll_secs: 0,
            warmup_pass_timeout_secs: 0,
            ..Default::default()
        };
        let options = Options::from_limit(&limit);
        assert_eq!(options.max_files, 50_000);
        assert_eq!(options.concurrency, 32);
        assert_eq!(options.retries, 16);
        assert_eq!(options.backoff, Duration::from_secs(5));
        assert_eq!(options.ring_wait, Duration::from_secs(1));
        assert_eq!(options.settle, Duration::from_millis(500));
        assert_eq!(options.poll, Duration::from_secs(1));
        assert_eq!(options.pass_timeout, Duration::from_secs(1));
        limit.warmup_cache_max_files = 0;
        limit.warmup_cache_concurrency = 0;
        limit.warmup_busy_retries = 0;
        limit.warmup_busy_backoff_ms = 0;
        let options = Options::from_limit(&limit);
        assert_eq!(options.max_files, 0);
        assert_eq!(options.concurrency, 1);
        assert_eq!(options.retries, 0);
        assert_eq!(options.backoff, Duration::from_millis(1));
    }

    #[tokio::test]
    async fn warmup_late_ring_after_timeout_still_warms() {
        let mut backend = Fake::new(vec![]);
        backend.ready_at = Instant::now() + Duration::from_millis(45);
        let options = options();
        let stopped = tokio::time::timeout(
            Duration::from_secs(1),
            until_cancelled(supervise(&backend, &options), backend.entered.notified()),
        )
        .await
        .unwrap();
        assert_eq!(stopped, None);
        assert_eq!(backend.calls.load(AtomicOrdering::SeqCst), 1);
        assert!(Instant::now() >= backend.ready_at);
    }

    #[tokio::test]
    async fn warmup_busy_retries_then_succeeds() {
        let backend = Fake::new(vec![
            WarmFileOutcome::Busy,
            WarmFileOutcome::Busy,
            WarmFileOutcome::Warmed,
        ]);
        assert_eq!(
            warm_with_retry(&backend, &file(1), &options()).await,
            "warmed"
        );
        assert_eq!(backend.calls.load(AtomicOrdering::SeqCst), 3);
        assert_eq!(backend.leases.available_permits(), 10);
    }

    #[tokio::test]
    async fn warmup_busy_retries_have_a_limit() {
        let backend = Fake::new(vec![WarmFileOutcome::Busy; 10]);
        assert_eq!(
            warm_with_retry(&backend, &file(1), &options()).await,
            "busy_exhausted"
        );
        assert_eq!(backend.calls.load(AtomicOrdering::SeqCst), 3);
        let mut options = options();
        options.retries = 0;
        assert_eq!(
            warm_with_retry(&backend, &file(1), &options).await,
            "busy_exhausted"
        );
        assert_eq!(backend.calls.load(AtomicOrdering::SeqCst), 4);
    }

    #[tokio::test]
    async fn warmup_terminal_skips_and_errors_do_not_retry() {
        let backend = Fake::new(vec![
            WarmFileOutcome::AlreadyWarm,
            WarmFileOutcome::InvalidInput,
        ]);
        backend
            .outcomes
            .lock()
            .unwrap()
            .push_back(Err(anyhow::anyhow!("local fixture read failed")));
        for expected in ["already_warm", "invalid_input", "error"] {
            assert_eq!(
                warm_with_retry(&backend, &file(1), &options()).await,
                expected
            );
        }
        assert_eq!(backend.calls.load(AtomicOrdering::SeqCst), 3);
    }

    #[tokio::test]
    async fn warmup_cancel_drops_inflight_work_and_bounds_concurrency() {
        let mut backend = Fake::new(vec![]);
        backend.files = (1..=10).map(file).collect();
        backend.block = true;
        let options = options();
        let current = ring("local");
        let mut summary = Summary::new(1);
        let result = until_cancelled(
            pass(&backend, &current, None, &options, &mut summary),
            async {
                while backend.calls.load(AtomicOrdering::SeqCst) < options.concurrency {
                    backend.entered.notified().await;
                }
            },
        )
        .await;
        assert!(result.is_none());
        assert_eq!(
            backend.calls.load(AtomicOrdering::SeqCst),
            options.concurrency
        );
        assert_eq!(backend.leases.available_permits(), 10);
        assert_eq!(summary.completed, 0);
    }

    #[tokio::test]
    async fn warmup_cancel_during_ring_wait_and_backoff_exits() {
        let backend = Fake::new(vec![WarmFileOutcome::Busy; 10]);
        *backend.ring.lock().unwrap() = None;
        assert!(
            until_cancelled(supervise(&backend, &options()), async {})
                .await
                .is_none()
        );
        assert_eq!(backend.calls.load(AtomicOrdering::SeqCst), 0);
        let mut options = options();
        options.backoff = Duration::from_secs(5);
        let file = file(1);
        assert!(
            until_cancelled(
                warm_with_retry(&backend, &file, &options),
                backend.entered.notified(),
            )
            .await
            .is_none()
        );
        assert_eq!(backend.calls.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(backend.leases.available_permits(), 10);
    }

    #[tokio::test]
    async fn warmup_epoch_change_only_adds_new_share() {
        let mut backend = Fake::new(vec![]);
        backend.files = (1..=100).map(file).collect();
        let mut options = options();
        options.max_files = 100;
        let before = RingSnapshot(BTreeMap::from([
            (u64::MAX / 2, "local".to_owned()),
            (u64::MAX, "other".to_owned()),
        ]));
        let after = ring("local");
        let mut first = Summary::new(1);
        pass(&backend, &before, None, &options, &mut first)
            .await
            .unwrap();
        assert!(first.completed > 0 && first.completed < 100);
        let mut second = Summary::new(2);
        pass(&backend, &after, Some(&before), &options, &mut second)
            .await
            .unwrap();
        assert_eq!(first.completed + second.completed, 100);
        {
            let opened = backend.opened.lock().unwrap();
            assert_eq!(opened.len(), 100);
            assert_eq!(opened.iter().collect::<HashSet<_>>().len(), 100);
        }
        let mut unchanged = Summary::new(3);
        pass(&backend, &after, Some(&after), &options, &mut unchanged)
            .await
            .unwrap();
        assert_eq!(unchanged.completed, 0);
    }

    #[tokio::test]
    async fn warmup_supervisor_observes_epoch_once_without_replaying_busy_files() {
        let backend = Fake::new(vec![WarmFileOutcome::Busy; 10]);
        let options = options();
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            until_cancelled(supervise(&backend, &options), async {
                while backend.calls.load(AtomicOrdering::SeqCst) < 3 {
                    backend.entered.notified().await;
                }
                tokio::time::sleep(Duration::from_millis(30)).await;
            }),
        )
        .await
        .unwrap();
        assert!(result.is_none());
        assert_eq!(backend.calls.load(AtomicOrdering::SeqCst), 3);
    }

    #[tokio::test]
    async fn warmup_supervisor_follows_ring_changes() {
        let mut backend = Fake::new(vec![]);
        backend.files = (1..=100).map(file).collect();
        let before = RingSnapshot(BTreeMap::from([
            (u64::MAX / 2, "local".to_owned()),
            (u64::MAX, "other".to_owned()),
        ]));
        let first_count = backend
            .files
            .iter()
            .filter(|file| before.owner(file.id) == Some("local"))
            .count();
        assert!(first_count > 0 && first_count < 100);
        *backend.ring.lock().unwrap() = Some(before);
        let mut options = options();
        options.max_files = 100;
        tokio::time::timeout(
            Duration::from_secs(1),
            until_cancelled(supervise(&backend, &options), async {
                while backend.calls.load(AtomicOrdering::SeqCst) < first_count {
                    backend.entered.notified().await;
                }
                *backend.ring.lock().unwrap() = Some(ring("local"));
                while backend.calls.load(AtomicOrdering::SeqCst) < 100 {
                    backend.entered.notified().await;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }),
        )
        .await
        .unwrap();
        let opened = backend.opened.lock().unwrap();
        assert_eq!(opened.len(), 100);
        assert_eq!(opened.iter().collect::<HashSet<_>>().len(), 100);
    }

    #[tokio::test]
    async fn warmup_timeout_releases_inflight_leases() {
        let mut backend = Fake::new(vec![]);
        backend.block = true;
        let current = ring("local");
        let mut summary = Summary::new(1);
        assert!(
            tokio::time::timeout(
                Duration::from_millis(5),
                pass(&backend, &current, None, &options(), &mut summary,)
            )
            .await
            .is_err()
        );
        assert_eq!(backend.leases.available_permits(), 10);
    }

    #[test]
    fn warmup_newest_queue_is_bounded_and_matches_full_sort() {
        for limit in [0, 1, 7, 100] {
            let mut queue = NewestFiles::new(limit);
            let mut expected: Vec<_> = (1..=30).rev().map(file).collect();
            for file in expected.iter().rev() {
                queue.push(file.clone());
                assert!(queue.heap.len() <= limit);
            }
            expected.truncate(limit);
            assert_eq!(queue.finish(), expected);
        }
    }

    #[test]
    fn warmup_snapshot_matches_legacy_clockwise_routing() {
        let current = RingSnapshot(BTreeMap::from([
            (10, "a".to_owned()),
            (u64::MAX / 3, "b".to_owned()),
            (u64::MAX - 10, "c".to_owned()),
        ]));
        for id in 1..=1000 {
            let hash = config::utils::hash::gxhash::new().sum64(&id.to_string());
            // Reference: legacy ring walk followed by wrap-around.
            let expected = current
                .0
                .iter()
                .find(|(point, _)| **point >= hash)
                .or_else(|| current.0.iter().next())
                .unwrap()
                .1;
            assert_eq!(current.owner(id), Some(expected.as_str()));
        }
    }
}
