// Copyright 2026 OpenObserve Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use config::{
    meta::{
        cluster::NodeInfo,
        inverted_index::IndexOptimizeMode,
        search::{ScanStats, SearchEventType},
    },
    utils::rand::generate_random_string,
};
use datafusion::{
    common::{DataFusionError, Result},
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::{EquivalenceProperties, Partitioning},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        metrics::{ExecutionPlanMetricsSet, MetricsSet},
        stream::RecordBatchStreamAdapter,
    },
};
use datafusion_proto::bytes::physical_plan_to_bytes_with_extension_codec;
use flight::common::{Metrics, RemoteScanMetrics};
use futures::{StreamExt, TryStreamExt};
use parking_lot::Mutex;
use rand::prelude::SliceRandom;

use super::node::RemoteScanNode;
use crate::datafusion::distributed_plan::{
    codec::get_physical_extension_codec,
    common::{EmptyStream, QueryContext, get_empty_stream, process_partial_err},
    decoder_stream::FlightDecoderStream,
    utils::make_flight_client,
};

/// Execution plan for empty relation with produce_one_row=false
#[derive(Debug, Clone)]
pub struct RemoteScanExec {
    input: Arc<dyn ExecutionPlan>,
    remote_scan_node: RemoteScanNode,
    partitions: usize,
    cache: Arc<PlanProperties>,
    pub scan_stats: Arc<Mutex<ScanStats>>,
    pub peak_memory: Arc<AtomicUsize>,
    pub partial_err: Arc<Mutex<String>>,
    pub cluster_metrics: Arc<Mutex<Vec<Metrics>>>,
    pub enrich_mode_node_idx: usize,
    pub metrics: ExecutionPlanMetricsSet,
    /// Re-dispatch bookkeeping shared by the partitions of one execution.
    redispatch: Arc<RedispatchState>,
}

impl RemoteScanExec {
    /// Create a new RemoteScanExec
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        mut remote_scan_node: RemoteScanNode,
    ) -> Result<Self> {
        let output_partitions = remote_scan_node.nodes.len();
        let cache = Self::compute_properties(&input, output_partitions);

        // serialize the input plan and set it as the plan for the remote scan node
        let proto = get_physical_extension_codec();
        let physical_plan_bytes =
            physical_plan_to_bytes_with_extension_codec(input.clone(), &proto)?;
        remote_scan_node.set_plan(physical_plan_bytes);

        // get the node ids for enrich mode
        let mut node_ids = remote_scan_node
            .nodes
            .iter()
            .enumerate()
            .filter_map(|(idx, n)| if n.is_querier() { Some(idx) } else { None })
            .collect::<Vec<_>>();
        // random shuffle the node ids
        node_ids.shuffle(&mut rand::rng());
        let enrich_mode_node_idx = node_ids.pop().unwrap_or_default();
        let redispatch = Arc::new(RedispatchState::new(
            &remote_scan_node,
            enrich_mode_node_idx,
        ));

        // LIMIT can stop before polling every partition; known missing WAL is still partial.
        let partial_err = Arc::new(Mutex::new(String::new()));
        if !remote_scan_node.super_cluster_info.is_super_cluster {
            for node in remote_scan_node.nodes.iter().filter(|n| n.is_ingester()) {
                if let Err(status) = flight::engine::check_generation(
                    node.query_engine_generation(),
                    "registration (ingester WAL/latest data unavailable)",
                    &node.get_name(),
                    &remote_scan_node.query_identifier.trace_id,
                ) {
                    process_partial_err(partial_err.clone(), status);
                }
            }
        }

        Ok(RemoteScanExec {
            input,
            remote_scan_node,
            partitions: output_partitions,
            cache,
            scan_stats: Arc::new(Mutex::new(ScanStats::default())),
            partial_err,
            cluster_metrics: Arc::new(Mutex::new(Vec::new())),
            peak_memory: Arc::new(AtomicUsize::new(0)),
            enrich_mode_node_idx,
            metrics: ExecutionPlanMetricsSet::new(),
            redispatch,
        })
    }

    pub fn scan_stats(&self) -> Arc<Mutex<ScanStats>> {
        self.scan_stats.clone()
    }

    pub fn partial_err(&self) -> Arc<Mutex<String>> {
        self.partial_err.clone()
    }

    pub fn cluster_metrics(&self) -> Arc<Mutex<Vec<Metrics>>> {
        self.cluster_metrics.clone()
    }

    pub fn peak_memory(&self) -> Arc<AtomicUsize> {
        self.peak_memory.clone()
    }

    pub fn with_scan_stats(mut self, scan_stats: Arc<Mutex<ScanStats>>) -> Self {
        self.scan_stats = scan_stats;
        self
    }

    pub fn with_partial_err(mut self, partial_err: Arc<Mutex<String>>) -> Self {
        self.partial_err = partial_err;
        self
    }

    pub fn with_cluster_metrics(mut self, cluster_metrics: Arc<Mutex<Vec<Metrics>>>) -> Self {
        self.cluster_metrics = cluster_metrics;
        self
    }

    pub fn with_peak_memory(mut self, peak_memory: Arc<AtomicUsize>) -> Self {
        self.peak_memory = peak_memory;
        self
    }

    pub fn with_metrics(mut self, metrics: ExecutionPlanMetricsSet) -> Self {
        self.metrics = metrics;
        self
    }

    fn output_partitioning_helper(n_partitions: usize) -> Partitioning {
        Partitioning::UnknownPartitioning(n_partitions)
    }

    /// This function creates the cache object that stores the plan properties such as schema,
    /// equivalence properties, ordering, partitioning, etc.
    ///
    /// Output partition `i` is the response of node `i`, which runs `input` and streams its
    /// output partitions back. A single input partition is streamed inline in order, so every
    /// output partition keeps the input's ordering and equivalences — the leader's
    /// `SortPreservingMergeExec` over the per-node streams relies on exactly that, and
    /// DataFusion's final plan check requires it to be declared. Several input partitions are
    /// interleaved on the follower, so no ordering is declared for them.
    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        n_partitions: usize,
    ) -> Arc<PlanProperties> {
        let eq_properties = if input.output_partitioning().partition_count() == 1 {
            input.equivalence_properties().clone()
        } else {
            EquivalenceProperties::new(input.schema())
        };
        let output_partitioning = Self::output_partitioning_helper(n_partitions);
        Arc::new(PlanProperties::new(
            eq_properties,
            // Output Partitioning
            output_partitioning,
            // Execution Mode
            EmissionType::Incremental,
            Boundedness::Bounded,
        ))
    }

    pub fn set_analyze(mut self) -> Self {
        self.remote_scan_node.search_infos.is_analyze = true;
        self
    }

    pub fn set_index_optimize_mode(mut self, index_optimize_mode: IndexOptimizeMode) -> Self {
        self.remote_scan_node.index_info.index_optimize_mode = Some(index_optimize_mode.into());
        self
    }

    #[cfg(test)]
    pub fn analyze(&self) -> bool {
        self.remote_scan_node.search_infos.is_analyze
    }

    #[cfg(test)]
    pub fn index_optimize_mode(&self) -> Option<IndexOptimizeMode> {
        self.remote_scan_node
            .index_info
            .index_optimize_mode
            .clone()
            .and_then(|x| IndexOptimizeMode::try_from(x).ok())
    }
}

impl DisplayAs for RemoteScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "RemoteScanExec: input_partitions=output_partitions={}",
            self.partitions,
        )
    }
}

impl ExecutionPlan for RemoteScanExec {
    // Still required by DF56; callers use replace_children.
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            datafusion::physical_plan::execution_plan::ReplaceChildrenOptions::new(
                datafusion::physical_plan::execution_plan::ChildrenPropertiesMode::Recompute,
            ),
        )
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        ) -> datafusion::common::Result<
            datafusion::common::tree_node::TreeNodeRecursion,
        >,
    ) -> datafusion::common::Result<datafusion::common::tree_node::TreeNodeRecursion> {
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    }

    fn name(&self) -> &'static str {
        "RemoteScanExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.input.schema())
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: datafusion::physical_plan::execution_plan::ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() || Arc::ptr_eq(&children[0], &self.input) {
            return Ok(self);
        }
        let remote_scan = Self::new(children[0].clone(), self.remote_scan_node.clone())?
            .with_scan_stats(self.scan_stats.clone())
            .with_partial_err(self.partial_err.clone())
            .with_cluster_metrics(self.cluster_metrics.clone())
            .with_peak_memory(self.peak_memory.clone())
            .with_metrics(self.metrics.clone());
        Ok(Arc::new(remote_scan))
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let baseline_metrics = RemoteScanMetrics::new(partition, &self.metrics);
        let fut = get_remote_batch(
            self.remote_scan_node.clone(),
            partition,
            partition == self.enrich_mode_node_idx,
            self.input.schema().clone(),
            self.scan_stats(),
            self.partial_err(),
            self.cluster_metrics(),
            self.peak_memory(),
            baseline_metrics,
            self.redispatch.clone(),
        );
        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema().clone(),
            stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

#[allow(clippy::too_many_arguments)]
async fn get_remote_batch(
    remote_scan_node: RemoteScanNode,
    partition: usize,
    enrich_mode: bool,
    schema: SchemaRef,
    scan_stats: Arc<Mutex<ScanStats>>,
    partial_err: Arc<Mutex<String>>,
    cluster_metrics: Arc<Mutex<Vec<Metrics>>>,
    peak_memory: Arc<AtomicUsize>,
    metrics: RemoteScanMetrics,
    redispatch: Arc<RedispatchState>,
) -> Result<SendableRecordBatchStream> {
    let start = std::time::Instant::now();
    let race_start = tokio::time::Instant::now();
    let cfg = config::get_config();
    let trace_id = remote_scan_node.query_identifier.trace_id.clone();
    let node = remote_scan_node.nodes[partition].clone();
    let is_super = remote_scan_node.super_cluster_info.is_super_cluster;
    let is_querier = node.is_querier();
    let is_ingester = node.is_ingester();
    let search_type = remote_scan_node
        .super_cluster_info
        .search_event_type
        .as_ref()
        .and_then(|s| s.as_str().try_into().ok());

    // check timeout
    let mut timeout = remote_scan_node.search_infos.timeout;
    if timeout == 0 {
        timeout = cfg.limit.query_timeout;
    }
    // check grpc timeout
    let mut grpc_timeout = timeout;
    if matches!(search_type, Some(SearchEventType::UI)) {
        if is_querier {
            grpc_timeout = std::cmp::min(grpc_timeout, cfg.limit.query_querier_timeout);
        } else if is_ingester {
            grpc_timeout = std::cmp::min(grpc_timeout, cfg.limit.query_ingester_timeout);
        }
    }

    // fast return for empty file list querier node
    if !is_super
        && is_querier
        && !is_ingester
        && !enrich_mode
        && remote_scan_node.is_file_list_empty(partition)
    {
        let empty_stream = EmptyStream::new(
            &trace_id,
            schema.clone(),
            &node.get_grpc_addr(),
            is_querier,
            partial_err.clone(),
            start,
        );
        return Ok(get_empty_stream(empty_stream));
    }

    let policy =
        (cfg.limit.query_redispatch_enabled && redispatch.is_eligible(partition)).then(|| {
            RedispatchPolicy {
                min_delay_ms: cfg.limit.query_redispatch_min_delay_ms,
                factor: cfg.limit.query_redispatch_straggler_factor,
            }
        });
    let nodes = remote_scan_node.nodes.clone();
    let shared = AttemptSinks {
        scan_stats,
        partial_err,
        cluster_metrics,
        peak_memory,
    };
    let dispatch = Arc::new(PartitionDispatch {
        remote_scan_node,
        partition,
        enrich_mode,
        schema,
        trace_id,
        timeout,
        grpc_timeout,
        search_type,
        is_super,
        start,
        // Every attempt stops at the query deadline, so a partition never outlives it, even
        // when a node stops answering without closing the connection.
        deadline: race_start + Duration::from_secs(timeout),
        metrics,
    });

    let raced = race_attempts(
        &dispatch.trace_id,
        partition,
        &nodes,
        &redispatch,
        policy,
        race_start,
        |node_idx, redispatched| run_attempt(dispatch.clone(), node_idx, redispatched),
    )
    .await;
    match raced {
        Ok((answer, node_idx)) => Ok(committed_stream(answer, node_idx, shared, dispatch)),
        Err(failed) => finish_failed(failed, shared, &dispatch),
    }
}

/// How often a running querier partition checks whether it has become a straggler.
const REDISPATCH_CHECK_INTERVAL: Duration = Duration::from_millis(100);

/// Why a partition got an extra attempt on another querier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RedispatchReason {
    /// Its node failed at the transport level before returning data.
    Failure,
    /// Its node is far slower than the partitions that already answered.
    Straggler,
}

impl std::fmt::Display for RedispatchReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Failure => write!(f, "failure"),
            Self::Straggler => write!(f, "straggler"),
        }
    }
}

/// Straggler rule for one partition.
#[derive(Debug, Clone, Copy)]
struct RedispatchPolicy {
    /// Never re-dispatch a partition that has run for less than this.
    min_delay_ms: u64,
    /// Re-dispatch when a partition has run this many times the median answer time.
    factor: f64,
}

/// Re-dispatch bookkeeping shared by all partitions of one [`RemoteScanExec`] execution.
///
/// A querier partition gets at most ONE extra attempt, on another querier, and the extra
/// attempt is never re-dispatched again. So one query sends at most twice as many requests
/// as it has querier partitions.
#[derive(Debug)]
pub struct RedispatchState {
    /// Plain querier partitions (querier role, no ingester role, not a super cluster). Only
    /// these get an extra attempt and only these nodes run one.
    eligible: Vec<bool>,
    /// Eligible partitions that have work: a non-empty file list or enrich mode. The
    /// straggler rule compares a partition with these.
    participant: Vec<bool>,
    participants: usize,
    /// Straggler re-dispatches allowed per query. Failure re-dispatches are not capped
    /// (at most one per partition): they replace work instead of duplicating it.
    straggler_budget: usize,
    inner: Mutex<RedispatchInner>,
}

#[derive(Debug)]
struct RedispatchInner {
    /// Answer times (ms since partition start) of the participant partitions that answered.
    answered_ms: Vec<u64>,
    answered: Vec<bool>,
    /// Nodes that failed or straggled in this query. They never run an extra attempt.
    suspect: Vec<bool>,
    /// Extra attempts already sent to each node in this query.
    extra_attempts: Vec<u32>,
    stragglers: usize,
}

impl RedispatchState {
    fn new(remote_scan_node: &RemoteScanNode, enrich_mode_node_idx: usize) -> Self {
        let is_super = remote_scan_node.super_cluster_info.is_super_cluster;
        let n = remote_scan_node.nodes.len();
        let eligible: Vec<bool> = remote_scan_node
            .nodes
            .iter()
            .map(|node| !is_super && node.is_querier() && !node.is_ingester())
            .collect();
        let participant: Vec<bool> = (0..n)
            .map(|i| {
                eligible[i]
                    && (i == enrich_mode_node_idx || !remote_scan_node.is_file_list_empty(i))
            })
            .collect();
        let participants = participant.iter().filter(|p| **p).count();
        Self {
            eligible,
            participant,
            participants,
            straggler_budget: participants.div_ceil(4),
            inner: Mutex::new(RedispatchInner {
                answered_ms: Vec::with_capacity(participants),
                answered: vec![false; n],
                suspect: vec![false; n],
                extra_attempts: vec![0; n],
                stragglers: 0,
            }),
        }
    }

    fn is_eligible(&self, partition: usize) -> bool {
        self.eligible.get(partition).copied().unwrap_or(false)
    }

    fn record_answer(&self, partition: usize, elapsed_ms: u64) {
        if !self.participant.get(partition).copied().unwrap_or(false) {
            return;
        }
        let mut inner = self.inner.lock();
        if !inner.answered[partition] {
            inner.answered[partition] = true;
            inner.answered_ms.push(elapsed_ms);
        }
    }

    /// True when `partition`, running for `elapsed_ms`, is a straggler and the straggler
    /// budget is not used up.
    fn is_straggler(&self, partition: usize, elapsed_ms: u64, policy: &RedispatchPolicy) -> bool {
        if !self.participant.get(partition).copied().unwrap_or(false) {
            return false;
        }
        let inner = self.inner.lock();
        if inner.stragglers >= self.straggler_budget {
            return false;
        }
        straggler_threshold_ms(
            &inner.answered_ms,
            self.participants,
            policy.min_delay_ms,
            policy.factor,
        )
        .is_some_and(|threshold| elapsed_ms >= threshold)
    }

    /// Chooses the querier that runs the extra attempt of `partition` and books it. Marks
    /// the partition's own node as suspect. Returns `None` when no node qualifies or the
    /// straggler budget is used up.
    fn pick_target(&self, partition: usize, reason: RedispatchReason) -> Option<usize> {
        let mut inner = self.inner.lock();
        if let Some(suspect) = inner.suspect.get_mut(partition) {
            *suspect = true;
        }
        if reason == RedispatchReason::Straggler && inner.stragglers >= self.straggler_budget {
            return None;
        }
        // Prefer the node with the fewest extra attempts, then a node without pending own
        // work; break ties randomly so concurrent queries do not pile onto one node.
        let target = (0..self.eligible.len())
            .filter(|&i| i != partition && self.eligible[i] && !inner.suspect[i])
            .min_by_key(|&i| {
                (
                    inner.extra_attempts[i],
                    self.participant[i] && !inner.answered[i],
                    rand::random::<u32>(),
                )
            })?;
        inner.extra_attempts[target] += 1;
        if reason == RedispatchReason::Straggler {
            inner.stragglers += 1;
        }
        Some(target)
    }
}

/// Run time after which a partition counts as a straggler: `factor` times the median answer
/// time, at least `min_delay_ms`. `None` while fewer than half of the participant partitions
/// have answered, because a median of a few early answers says little about the rest.
fn straggler_threshold_ms(
    answered_ms: &[u64],
    participants: usize,
    min_delay_ms: u64,
    factor: f64,
) -> Option<u64> {
    if participants < 2 || answered_ms.is_empty() || answered_ms.len() * 2 < participants {
        return None;
    }
    let mut sorted = answered_ms.to_vec();
    sorted.sort_unstable();
    let median = sorted[sorted.len() / 2];
    let scaled = (median as f64 * factor.max(1.0)).ceil() as u64;
    Some(scaled.max(min_delay_ms))
}

fn elapsed_ms(start: tokio::time::Instant) -> u64 {
    start.elapsed().as_millis() as u64
}

/// Runs the first attempt of `partition` on its own node and, when `policy` allows it, at
/// most one extra attempt on another querier: after a transport failure of the first
/// attempt, or when the first attempt straggles. The first attempt that answers wins.
/// Dropping the other attempt cancels its gRPC call. Returns the answer and the index of the
/// node that produced it.
async fn race_attempts<A, F, Fut>(
    trace_id: &str,
    partition: usize,
    nodes: &[Arc<dyn NodeInfo>],
    state: &RedispatchState,
    policy: Option<RedispatchPolicy>,
    start: tokio::time::Instant,
    open: F,
) -> std::result::Result<(A, usize), FailedAttempt>
where
    F: Fn(usize, bool) -> Fut,
    Fut: Future<Output = std::result::Result<A, FailedAttempt>>,
{
    let node_name = |idx: usize| nodes.get(idx).map(|n| n.get_name()).unwrap_or_default();
    let mut primary: Option<Pin<Box<Fut>>> = Some(Box::pin(open(partition, false)));
    let mut extra: Option<Pin<Box<Fut>>> = None;
    let mut extra_node: Option<usize> = None;
    let mut primary_failure: Option<FailedAttempt> = None;
    let mut ticker = tokio::time::interval(REDISPATCH_CHECK_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            result = async { primary.as_mut().unwrap().await }, if primary.is_some() => {
                primary = None;
                let failure = match result {
                    Ok(answer) => {
                        state.record_answer(partition, elapsed_ms(start));
                        return Ok((answer, partition));
                    }
                    Err(failure) => failure,
                };
                if extra_node.is_none()
                    && policy.is_some()
                    && failure.failure.is_retryable()
                    && let Some(target) = state.pick_target(partition, RedispatchReason::Failure)
                {
                    log::warn!(
                        "[trace_id {trace_id}] flight->search: re-dispatch partition {partition} from node {} to node {}, reason: {}, err: {}, elapsed: {} ms",
                        node_name(partition),
                        node_name(target),
                        RedispatchReason::Failure,
                        failure.failure,
                        elapsed_ms(start),
                    );
                    extra = Some(Box::pin(open(target, true)));
                    extra_node = Some(target);
                }
                if extra.is_none() {
                    return Err(failure);
                }
                primary_failure = Some(failure);
            }
            result = async { extra.as_mut().unwrap().await }, if extra.is_some() => {
                extra = None;
                let target = extra_node.unwrap_or(partition);
                match result {
                    Ok(answer) => {
                        state.record_answer(partition, elapsed_ms(start));
                        log::info!(
                            "[trace_id {trace_id}] flight->search: re-dispatched partition {partition} answered by node {}, elapsed: {} ms",
                            node_name(target),
                            elapsed_ms(start),
                        );
                        return Ok((answer, target));
                    }
                    Err(failure) => {
                        log::warn!(
                            "[trace_id {trace_id}] flight->search: re-dispatched partition {partition} failed on node {}, err: {}, elapsed: {} ms",
                            node_name(target),
                            failure.failure,
                            elapsed_ms(start),
                        );
                        if primary.is_none() {
                            // Report the first node's failure: it is the one the plan chose.
                            return Err(primary_failure.take().unwrap_or(failure));
                        }
                    }
                }
            }
            _ = ticker.tick(), if policy.is_some() && extra_node.is_none() && primary.is_some() => {
                let elapsed = elapsed_ms(start);
                if let Some(policy) = &policy
                    && state.is_straggler(partition, elapsed, policy)
                    && let Some(target) = state.pick_target(partition, RedispatchReason::Straggler)
                {
                    log::warn!(
                        "[trace_id {trace_id}] flight->search: re-dispatch partition {partition} from node {} to node {}, reason: {}, elapsed: {elapsed} ms",
                        node_name(partition),
                        node_name(target),
                        RedispatchReason::Straggler,
                    );
                    extra = Some(Box::pin(open(target, true)));
                    extra_node = Some(target);
                }
            }
        }
    }
}

/// Request data shared by every attempt of one partition.
struct PartitionDispatch {
    remote_scan_node: RemoteScanNode,
    partition: usize,
    enrich_mode: bool,
    schema: SchemaRef,
    trace_id: String,
    timeout: u64,
    grpc_timeout: u64,
    search_type: Option<SearchEventType>,
    is_super: bool,
    start: std::time::Instant,
    deadline: tokio::time::Instant,
    metrics: RemoteScanMetrics,
}

/// Stats and errors reported by one attempt. Each attempt writes into its own sinks, and only
/// the attempt that answers is merged into the query's sinks, so a cancelled attempt never
/// adds its scan stats or errors to the result.
#[derive(Clone, Default)]
struct AttemptSinks {
    scan_stats: Arc<Mutex<ScanStats>>,
    partial_err: Arc<Mutex<String>>,
    cluster_metrics: Arc<Mutex<Vec<Metrics>>>,
    peak_memory: Arc<AtomicUsize>,
}

impl AttemptSinks {
    /// Moves everything recorded so far into `shared`. Calling it again moves only what was
    /// recorded in between.
    fn merge_into(&self, shared: &AttemptSinks) {
        let stats = std::mem::take(&mut *self.scan_stats.lock());
        shared.scan_stats.lock().add(&stats);
        let err = std::mem::take(&mut *self.partial_err.lock());
        if !err.is_empty() {
            let mut guard = shared.partial_err.lock();
            if guard.is_empty() {
                *guard = err;
            } else {
                guard.push_str(" \n ");
                guard.push_str(&err);
            }
        }
        let metrics = std::mem::take(&mut *self.cluster_metrics.lock());
        shared.cluster_metrics.lock().extend(metrics);
        shared
            .peak_memory
            .fetch_max(self.peak_memory.load(Ordering::Relaxed), Ordering::Relaxed);
    }
}

/// Merges the winning attempt's sinks into the query's sinks when the result stream ends or
/// is dropped early (for example by a LIMIT).
struct MergeOnDrop {
    from: AttemptSinks,
    into: AttemptSinks,
}

impl Drop for MergeOnDrop {
    fn drop(&mut self) {
        self.from.merge_into(&self.into);
    }
}

#[derive(Debug)]
enum AttemptFailure {
    /// Building the client or connecting to the node failed.
    Connect(tonic::Status),
    /// `do_get` returned an error, or the query deadline passed before the node answered.
    Call(tonic::Status),
    /// No state from this attempt may enter the local engine.
    EngineGeneration(tonic::Status),
    /// The response stream broke at the transport level before the first batch. The decoder
    /// already recorded the error in the attempt's `partial_err`.
    StreamBroken(tonic::Code),
}

impl AttemptFailure {
    /// Whether another querier can answer where this node failed.
    fn is_retryable(&self) -> bool {
        match self {
            Self::Connect(_) | Self::StreamBroken(_) | Self::EngineGeneration(_) => true,
            Self::Call(status) => is_transport_code(status.code()),
        }
    }
}

impl std::fmt::Display for AttemptFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connect(status) => write!(f, "connect: {}: {}", status.code(), status.message()),
            Self::Call(status) => write!(f, "call: {}: {}", status.code(), status.message()),
            Self::EngineGeneration(status) => write!(f, "engine handshake: {}", status.message()),
            Self::StreamBroken(code) => write!(f, "stream broken before data: {code}"),
        }
    }
}

/// gRPC codes of broken connections. tonic maps a keepalive timeout, a reset or refused
/// connection and a refused stream to `Unavailable`, and other transport failures to
/// `Unknown`. Errors a follower returns on purpose use other codes (`Internal` with an error
/// code, `DeadlineExceeded`, `Cancelled` on a user cancel).
fn is_transport_code(code: tonic::Code) -> bool {
    matches!(code, tonic::Code::Unavailable | tonic::Code::Unknown)
}

struct FailedAttempt {
    node_idx: usize,
    sinks: AttemptSinks,
    failure: AttemptFailure,
}

/// An attempt that produced its first item, or ended without error. The partition commits to
/// the first such attempt.
struct AnsweredAttempt {
    sinks: AttemptSinks,
    first: Option<arrow_flight::error::Result<RecordBatch>>,
    rest: FlightDecoderStream,
}

/// Sends the partition's request to node `node_idx` and waits for its first batch.
async fn run_attempt(
    d: Arc<PartitionDispatch>,
    node_idx: usize,
    redispatched: bool,
) -> std::result::Result<AnsweredAttempt, FailedAttempt> {
    let node = d.remote_scan_node.nodes[node_idx].clone();
    let trace_id = &d.trace_id;
    let is_super = d.is_super;
    let is_querier = node.is_querier();
    let grpc_addr = node.get_grpc_addr();
    let node_name = node.get_name();
    let sinks = AttemptSinks::default();

    // Regional NodeInfo has no registration generation; its RPC still must prove it.
    if !is_super
        && let Err(status) = flight::engine::check_generation(
            node.query_engine_generation(),
            "registration",
            &node_name,
            trace_id,
        )
    {
        return Err(FailedAttempt {
            node_idx,
            sinks,
            failure: AttemptFailure::EngineGeneration(status),
        });
    }

    let mut request = d.remote_scan_node.get_flight_search_request(d.partition);
    request.set_job_id(generate_random_string(7));
    request.set_partition(d.partition);
    request.search_info.timeout = d.timeout as i64;
    request.query_identifier.enrich_mode = d.enrich_mode;
    let redispatch_note = if redispatched {
        format!(", re-dispatch of partition {}", d.partition)
    } else {
        String::new()
    };

    log::info!(
        "[trace_id {trace_id}] flight->search: request node: {grpc_addr}, name: {node_name}, query_type: {}, is_super: {is_super}, is_querier: {is_querier}, timeout: {}, files: {}{redispatch_note}",
        d.search_type.unwrap_or(SearchEventType::UI),
        d.timeout,
        request.search_info.file_id_list.len(),
    );

    let call = async {
        let (mut client, request) = make_flight_client(
            trace_id.clone(),
            &d.remote_scan_node.query_identifier.org_id,
            node.clone(),
            request,
            &d.remote_scan_node.opentelemetry_context,
            d.grpc_timeout,
        )
        .await
        .map_err(AttemptFailure::Connect)?;
        log::info!(
            "[trace_id {trace_id}] flight->search: prepare to request node: {grpc_addr}, name: {node_name}, is_super: {is_super}, is_querier: {is_querier}",
        );
        let response = client.do_get(request).await.map_err(|status| {
            if flight::engine::is_generation_error(&status) {
                AttemptFailure::EngineGeneration(status)
            } else {
                AttemptFailure::Call(status)
            }
        })?;
        flight::engine::verified_response(response, &node_name, trace_id)
            .map_err(AttemptFailure::EngineGeneration)
    };
    let deadline_failure =
        || AttemptFailure::Call(tonic::Status::new(tonic::Code::DeadlineExceeded, "timeout"));
    let stream = match tokio::time::timeout_at(d.deadline, call).await {
        Ok(Ok(stream)) => stream,
        Ok(Err(failure)) => {
            return Err(FailedAttempt {
                node_idx,
                sinks,
                failure,
            });
        }
        Err(_) => {
            return Err(FailedAttempt {
                node_idx,
                sinks,
                failure: deadline_failure(),
            });
        }
    };

    log::info!(
        "[trace_id {trace_id}] flight->search: prepare to response node: {grpc_addr}, name: {node_name}, is_super: {is_super}, is_querier: {is_querier}",
    );

    let query_context = QueryContext::new(node)
        .with_trace_id(trace_id)
        .with_is_super(is_super)
        .with_is_querier(is_querier)
        .with_scan_stats(sinks.scan_stats.clone())
        .with_partial_err(sinks.partial_err.clone())
        .with_cluster_metrics(sinks.cluster_metrics.clone())
        .with_peak_memory(sinks.peak_memory.clone())
        .with_start_time(d.start);
    let mut rest =
        FlightDecoderStream::new(stream, d.schema.clone(), d.metrics.clone(), query_context);
    let first = match tokio::time::timeout_at(d.deadline, rest.next()).await {
        Ok(first) => first,
        Err(_) => {
            return Err(FailedAttempt {
                node_idx,
                sinks,
                failure: deadline_failure(),
            });
        }
    };
    if first.is_none()
        && let Some(code) = rest.stream_error_code()
        && is_transport_code(code)
    {
        return Err(FailedAttempt {
            node_idx,
            sinks,
            failure: AttemptFailure::StreamBroken(code),
        });
    }
    Ok(AnsweredAttempt { sinks, first, rest })
}

/// Streams the committed attempt until it ends or the query deadline passes.
fn committed_stream(
    answer: AnsweredAttempt,
    node_idx: usize,
    shared: AttemptSinks,
    d: Arc<PartitionDispatch>,
) -> SendableRecordBatchStream {
    let schema = d.schema.clone();
    let AnsweredAttempt {
        sinks,
        first,
        mut rest,
    } = answer;
    // Built outside the generator so the merge also happens if the stream is never polled.
    let merge = MergeOnDrop {
        from: sinks.clone(),
        into: shared,
    };
    let stream = async_stream::stream! {
        let _merge = merge;
        if let Some(batch) = first {
            yield batch.map_err(|e| DataFusionError::Internal(format!("{e}")));
            loop {
                tokio::select! {
                    batch = rest.next() => {
                        if let Some(batch) = batch {
                            yield batch.map_err(|e| DataFusionError::Internal(format!("{e}")))
                        } else {
                            break;
                        }
                    }
                    _ = tokio::time::sleep_until(d.deadline) => {
                        let node = &d.remote_scan_node.nodes[node_idx];
                        let e = tonic::Status::new(tonic::Code::DeadlineExceeded, "timeout");
                        log::error!(
                            "[trace_id {}] flight->search: response node: {}, name: {}, is_super: {}, is_querier: {}, err: {e:?}, took: {} ms",
                            d.trace_id,
                            node.get_grpc_addr(),
                            node.get_name(),
                            d.is_super,
                            node.is_querier(),
                            d.start.elapsed().as_millis(),
                        );
                        process_partial_err(sinks.partial_err.clone(), e);
                        break;
                    }
                }
            }
        }
    };
    Box::pin(RecordBatchStreamAdapter::new(schema, stream))
}

/// Turns the final failure of a partition into the same result as before re-dispatch
/// existed: an empty stream with a partial error, or a query error for an unexpected
/// `do_get` failure.
fn finish_failed(
    failed: FailedAttempt,
    shared: AttemptSinks,
    d: &PartitionDispatch,
) -> Result<SendableRecordBatchStream> {
    let FailedAttempt {
        node_idx,
        sinks,
        failure,
    } = failed;
    sinks.merge_into(&shared);
    let node = &d.remote_scan_node.nodes[node_idx];
    let empty_stream = EmptyStream::new(
        &d.trace_id,
        d.schema.clone(),
        &node.get_grpc_addr(),
        node.is_querier(),
        shared.partial_err.clone(),
        d.start,
    );
    match failure {
        AttemptFailure::Connect(e) | AttemptFailure::EngineGeneration(e) => {
            Ok(get_empty_stream(empty_stream.with_error(e)))
        }
        AttemptFailure::StreamBroken(_) => Ok(get_empty_stream(empty_stream)),
        AttemptFailure::Call(e)
            if e.code() == tonic::Code::Cancelled
                || e.code() == tonic::Code::DeadlineExceeded
                || is_parquet_file_not_found(&e) =>
        {
            Ok(get_empty_stream(empty_stream.with_error(e)))
        }
        AttemptFailure::Call(e) => {
            log::error!(
                "[trace_id {}] flight->search: response node: {}, name: {}, is_super: {}, is_querier: {}, err: {e:?}, took: {} ms",
                d.trace_id,
                node.get_grpc_addr(),
                node.get_name(),
                d.is_super,
                node.is_querier(),
                d.start.elapsed().as_millis(),
            );
            Err(DataFusionError::Execution(e.to_string()))
        }
    }
}

pub fn is_parquet_file_not_found(e: &tonic::Status) -> bool {
    e.code() == tonic::Code::Internal && {
        let msg = e.message();
        msg.find('{')
            .and_then(|start| msg.rfind('}').map(|end| &msg[start..=end]))
            .and_then(|json_part| infra::errors::ErrorCodes::from_json(json_part).ok())
            .map(|err_code| {
                err_code.get_code()
                    == infra::errors::ErrorCodes::SearchParquetFileNotFound.get_code()
            })
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::empty::EmptyExec;

    use super::*;
    use crate::datafusion::distributed_plan::node::RemoteScanNode;

    #[derive(Debug)]
    struct TestNode;

    impl config::meta::cluster::NodeInfo for TestNode {
        fn get_grpc_addr(&self) -> String {
            "localhost:9090".to_string()
        }
        fn get_auth_token(&self) -> String {
            "token".to_string()
        }
        fn get_name(&self) -> String {
            "test-node".to_string()
        }
        fn is_local(&self) -> bool {
            true
        }
    }

    fn test_remote_node() -> RemoteScanNode {
        RemoteScanNode {
            nodes: vec![Arc::new(TestNode) as Arc<dyn config::meta::cluster::NodeInfo>].into(),
            opentelemetry_context: opentelemetry::Context::current(),
            query_identifier: Default::default(),
            search_infos: Default::default(),
            index_info: Default::default(),
            super_cluster_info: Default::default(),
        }
    }

    fn make_remote_exec() -> RemoteScanExec {
        let schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, false)]));
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema));
        RemoteScanExec::new(input, test_remote_node()).unwrap()
    }

    #[test]
    fn remote_scan_declares_input_ordering_only_for_a_single_input_partition() -> Result<()> {
        use datafusion::{
            arrow::compute::SortOptions,
            physical_expr::{LexOrdering, PhysicalSortExpr, expressions::Column},
            physical_plan::{
                repartition::RepartitionExec,
                sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
            },
        };

        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, false)]));
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("ts", 0)),
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        )])
        .unwrap();
        let partitions = Arc::new(RepartitionExec::try_new(
            Arc::new(EmptyExec::new(schema)),
            Partitioning::RoundRobinBatch(4),
        )?);
        let sorted_partitions: Arc<dyn ExecutionPlan> =
            Arc::new(SortExec::new(ordering.clone(), partitions).with_preserve_partitioning(true));
        assert_eq!(sorted_partitions.output_partitioning().partition_count(), 4);
        assert_eq!(sorted_partitions.output_ordering(), Some(&ordering));

        // The follower interleaves several partitions: the per-node stream is unordered.
        let interleaved: Arc<dyn ExecutionPlan> = Arc::new(RemoteScanExec::new(
            sorted_partitions.clone(),
            test_remote_node(),
        )?);
        assert_eq!(interleaved.output_ordering(), None);

        // One merged partition is streamed in order by every node.
        let merged: Arc<dyn ExecutionPlan> = Arc::new(SortPreservingMergeExec::new(
            ordering.clone(),
            sorted_partitions,
        ));
        let ordered: Arc<dyn ExecutionPlan> =
            Arc::new(RemoteScanExec::new(merged, test_remote_node())?);
        assert_eq!(ordered.output_ordering(), Some(&ordering));
        assert_eq!(ordered.output_partitioning().partition_count(), 1);
        Ok(())
    }

    #[test]
    fn rebuilt_remote_scan_dispatches_changed_child_and_preserves_query_state() -> Result<()> {
        let mut exec = make_remote_exec().set_analyze();
        exec.remote_scan_node.search_infos.file_id_list = vec![vec![-7, 11]].into();
        *exec.partial_err.lock() = "existing partial error".to_string();
        let exec = Arc::new(exec);
        let original_schema = exec.schema();
        let original_request = exec.remote_scan_node.get_flight_search_request(0);
        let unchanged = exec.clone().replace_children(
            vec![exec.input.clone()],
            datafusion::physical_plan::execution_plan::ReplaceChildrenOptions::new(
                datafusion::physical_plan::execution_plan::ChildrenPropertiesMode::Recompute,
            ),
        )?;
        let unchanged = unchanged.downcast_ref::<RemoteScanExec>().unwrap();
        assert_eq!(
            unchanged
                .remote_scan_node
                .get_flight_search_request(0)
                .search_info,
            original_request.search_info
        );

        let changed_schema = Arc::new(Schema::new(vec![Field::new(
            "new_col",
            DataType::Int64,
            true,
        )]));
        let changed_input: Arc<dyn ExecutionPlan> =
            Arc::new(EmptyExec::new(changed_schema.clone()));
        let rebuilt = exec.clone().replace_children(
            vec![changed_input],
            datafusion::physical_plan::execution_plan::ReplaceChildrenOptions::new(
                datafusion::physical_plan::execution_plan::ChildrenPropertiesMode::Recompute,
            ),
        )?;
        let rebuilt = rebuilt.downcast_ref::<RemoteScanExec>().unwrap();
        let request = rebuilt.remote_scan_node.get_flight_search_request(0);
        let ctx = datafusion::execution::context::SessionContext::new();
        let decoded = datafusion_proto::bytes::physical_plan_from_bytes_with_extension_codec(
            &request.search_info.plan,
            &ctx.task_ctx(),
            &get_physical_extension_codec(),
        )?;
        assert_eq!(decoded.schema(), changed_schema);
        assert_eq!(rebuilt.schema(), changed_schema);
        assert_eq!(request.search_info.file_id_list, vec![-7, 11]);
        assert!(request.search_info.is_analyze);
        assert_eq!(*rebuilt.partial_err.lock(), "existing partial error");
        *rebuilt.partial_err.lock() = "later partial error".to_string();
        assert_eq!(*exec.partial_err.lock(), "later partial error");
        assert_eq!(exec.schema(), original_schema);
        assert_eq!(
            exec.remote_scan_node
                .get_flight_search_request(0)
                .search_info,
            original_request.search_info
        );
        Ok(())
    }

    #[test]
    fn changed_child_serialization_error_is_returned_during_rebuild() {
        let exec = Arc::new(make_remote_exec());
        // RemoteScanExec is a dispatch boundary, not a serializable follower operator.
        let unsupported_child: Arc<dyn ExecutionPlan> = Arc::new(make_remote_exec());
        assert!(
            exec.replace_children(
                vec![unsupported_child],
                datafusion::physical_plan::execution_plan::ReplaceChildrenOptions::new(
                    datafusion::physical_plan::execution_plan::ChildrenPropertiesMode::Recompute
                )
            )
            .is_err()
        );
    }

    #[test]
    fn test_is_parquet_file_not_found_true() {
        let msg = r#"prefix text {"code":20006,"inner":""}"#;
        let status = tonic::Status::new(tonic::Code::Internal, msg);
        assert!(is_parquet_file_not_found(&status));
    }

    #[test]
    fn test_is_parquet_file_not_found_wrong_code() {
        let msg = r#"{"code":20001,"inner":"bad sql"}"#;
        let status = tonic::Status::new(tonic::Code::Internal, msg);
        assert!(!is_parquet_file_not_found(&status));
    }

    #[test]
    fn test_is_parquet_file_not_found_non_internal_status() {
        let status = tonic::Status::new(tonic::Code::NotFound, "not found");
        assert!(!is_parquet_file_not_found(&status));
    }

    #[test]
    fn test_is_parquet_file_not_found_no_json() {
        let status = tonic::Status::new(tonic::Code::Internal, "no json here");
        assert!(!is_parquet_file_not_found(&status));
    }

    #[derive(Debug)]
    struct FakeNode {
        name: String,
        ingester: bool,
    }

    impl config::meta::cluster::NodeInfo for FakeNode {
        fn is_querier(&self) -> bool {
            !self.ingester
        }
        fn is_ingester(&self) -> bool {
            self.ingester
        }
        fn get_grpc_addr(&self) -> String {
            format!("{}:5081", self.name)
        }
        fn get_auth_token(&self) -> String {
            String::new()
        }
        fn get_name(&self) -> String {
            self.name.clone()
        }
        fn is_local(&self) -> bool {
            false
        }
    }

    /// Node `i` is named `n{i}`; `files[i]` tells whether partition `i` has files.
    fn scan_node(files: &[bool], ingesters: &[usize]) -> RemoteScanNode {
        let nodes: Vec<Arc<dyn config::meta::cluster::NodeInfo>> = (0..files.len())
            .map(|i| {
                Arc::new(FakeNode {
                    name: format!("n{i}"),
                    ingester: ingesters.contains(&i),
                }) as Arc<dyn config::meta::cluster::NodeInfo>
            })
            .collect();
        let mut node = RemoteScanNode {
            nodes: nodes.into(),
            opentelemetry_context: opentelemetry::Context::current(),
            query_identifier: Default::default(),
            search_infos: Default::default(),
            index_info: Default::default(),
            super_cluster_info: Default::default(),
        };
        node.search_infos.file_id_list = files
            .iter()
            .map(|has| if *has { vec![1] } else { vec![] })
            .collect::<Vec<_>>()
            .into();
        node
    }

    fn failed(node_idx: usize, failure: AttemptFailure) -> FailedAttempt {
        FailedAttempt {
            node_idx,
            sinks: AttemptSinks::default(),
            failure,
        }
    }

    fn unavailable() -> AttemptFailure {
        AttemptFailure::Call(tonic::Status::unavailable("connection reset"))
    }

    fn engine_failure() -> AttemptFailure {
        AttemptFailure::EngineGeneration(
            flight::engine::check_generation(None, "response", "old-node", "engine-retry")
                .unwrap_err(),
        )
    }

    #[tokio::test]
    async fn engine_generation_failure_has_at_most_one_redispatch() {
        let node = scan_node(&[true, true, false], &[]);
        for policy in [None, Some(POLICY)] {
            let state = RedispatchState::new(&node, usize::MAX);
            let calls = Mutex::new(Vec::new());
            let result = race_attempts(
                "engine-retry",
                0,
                &node.nodes,
                &state,
                policy,
                tokio::time::Instant::now(),
                |idx, redispatched| {
                    calls.lock().push((idx, redispatched));
                    async move { Err::<(), _>(failed(idx, engine_failure())) }
                },
            )
            .await;
            assert!(result.is_err());
            assert_eq!(calls.lock().len(), if policy.is_some() { 2 } else { 1 });
            if policy.is_some() {
                assert_eq!(*calls.lock(), vec![(0, false), (2, true)]);
            }
        }
        let state = RedispatchState::new(&node, usize::MAX);
        let result = race_attempts(
            "engine-retry",
            0,
            &node.nodes,
            &state,
            Some(POLICY),
            tokio::time::Instant::now(),
            |idx, redispatched| async move {
                if redispatched {
                    Ok(idx)
                } else {
                    Err(failed(idx, engine_failure()))
                }
            },
        )
        .await;
        assert_eq!(result.ok(), Some((2, 2)));
    }

    #[tokio::test]
    async fn engine_generation_unknown_node_never_connects_and_reports_partial() {
        for ingesters in [vec![], vec![0]] {
            let remote_scan_node = scan_node(&[true], &ingesters);
            let state = RedispatchState::new(&remote_scan_node, 0);
            if !ingesters.is_empty() {
                assert!(
                    !state.is_eligible(0),
                    "WAL must never move to another ingester"
                );
                let exec = RemoteScanExec::new(
                    Arc::new(EmptyExec::new(Arc::new(Schema::empty()))),
                    remote_scan_node.clone(),
                )
                .unwrap();
                assert!(
                    exec.partial_err
                        .lock()
                        .contains("WAL/latest data unavailable"),
                    "known missing WAL is reported even if LIMIT never polls the partition"
                );
            }
            let d = Arc::new(PartitionDispatch {
                remote_scan_node,
                partition: 0,
                enrich_mode: false,
                schema: Arc::new(Schema::empty()),
                trace_id: "engine-partition".to_string(),
                timeout: 1,
                grpc_timeout: 1,
                search_type: None,
                is_super: false,
                start: std::time::Instant::now(),
                deadline: tokio::time::Instant::now() + Duration::from_secs(1),
                metrics: RemoteScanMetrics::new(0, &ExecutionPlanMetricsSet::new()),
            });
            let Err(failed) = run_attempt(d.clone(), 0, false).await else {
                panic!("unknown generation must fail before connecting");
            };
            assert!(matches!(
                failed.failure,
                AttemptFailure::EngineGeneration(_)
            ));
            let sinks = AttemptSinks::default();
            let mut stream = finish_failed(failed, sinks.clone(), &d).unwrap();
            assert!(stream.next().await.is_none());
            let error = sinks.partial_err.lock();
            for expected in [
                "query engine generation mismatch",
                "unknown (missing)",
                "n0",
                "engine-partition",
            ] {
                assert!(error.contains(expected), "{error}");
            }
        }
    }

    const POLICY: RedispatchPolicy = RedispatchPolicy {
        min_delay_ms: 50,
        factor: 2.0,
    };

    struct SetOnDrop(Arc<std::sync::atomic::AtomicBool>);

    impl Drop for SetOnDrop {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[test]
    fn straggler_threshold_waits_for_half_of_the_participants() {
        assert_eq!(straggler_threshold_ms(&[100], 4, 0, 2.0), None);
        assert_eq!(straggler_threshold_ms(&[100, 300], 4, 0, 2.0), Some(600));
        assert_eq!(
            straggler_threshold_ms(&[100, 300], 4, 2000, 2.0),
            Some(2000)
        );
        // A query with one querier partition has nothing to compare with.
        assert_eq!(straggler_threshold_ms(&[100], 1, 0, 2.0), None);
        // A factor below 1 would re-dispatch partitions faster than the median.
        assert_eq!(straggler_threshold_ms(&[100, 100], 3, 0, 0.5), Some(100));
    }

    #[test]
    fn pick_target_skips_own_node_ingesters_and_suspects_and_spreads_load() {
        // n0, n1 have files; n2, n3 are idle queriers; n4 is an ingester.
        let node = scan_node(&[true, true, false, false, true], &[4]);
        let state = RedispatchState::new(&node, usize::MAX);
        assert!(!state.is_eligible(4));
        let first = state.pick_target(0, RedispatchReason::Failure).unwrap();
        assert!(
            first == 2 || first == 3,
            "an idle querier first, got n{first}"
        );
        let second = state.pick_target(1, RedispatchReason::Failure).unwrap();
        assert!(second == 2 || second == 3, "an idle querier, got n{second}");
        assert_ne!(
            first, second,
            "the second extra attempt goes to the less loaded node"
        );
        // n0 and n1 are suspects now and n4 is an ingester.
        assert_eq!(state.pick_target(2, RedispatchReason::Failure), Some(3));
        assert_eq!(state.pick_target(3, RedispatchReason::Failure), None);
    }

    #[test]
    fn straggler_budget_caps_hedges_but_not_failure_redispatch() {
        // Four participants: a budget of one straggler re-dispatch.
        let node = scan_node(&[true, true, true, true, false, false], &[]);
        let state = RedispatchState::new(&node, usize::MAX);
        assert!(!state.is_straggler(0, 10_000, &POLICY), "no answers yet");
        state.record_answer(2, 100);
        state.record_answer(3, 100);
        assert!(!state.is_straggler(0, 150, &POLICY));
        assert!(state.is_straggler(0, 250, &POLICY));
        assert!(state.pick_target(0, RedispatchReason::Straggler).is_some());
        assert!(!state.is_straggler(1, 250, &POLICY), "budget used up");
        assert_eq!(state.pick_target(1, RedispatchReason::Straggler), None);
        assert!(state.pick_target(1, RedispatchReason::Failure).is_some());
        // A partition without files never counts as a straggler.
        assert!(!state.is_straggler(4, 10_000, &POLICY));
    }

    #[tokio::test]
    async fn transport_failure_is_redispatched_once_to_another_querier() {
        let node = scan_node(&[true, true, false], &[]);
        let state = RedispatchState::new(&node, usize::MAX);
        let calls = Arc::new(Mutex::new(Vec::new()));
        let result = race_attempts(
            "t",
            0,
            &node.nodes,
            &state,
            Some(POLICY),
            tokio::time::Instant::now(),
            |idx, redispatched| {
                calls.lock().push((idx, redispatched));
                async move {
                    if redispatched {
                        Ok(idx)
                    } else {
                        Err(failed(idx, unavailable()))
                    }
                }
            },
        )
        .await;
        let (answer, node_idx) = result.ok().expect("the extra attempt answers");
        assert_eq!((answer, node_idx), (2, 2), "the idle querier n2 answers");
        assert_eq!(*calls.lock(), vec![(0, false), (2, true)]);
    }

    #[tokio::test]
    async fn follower_errors_and_deadlines_are_not_redispatched() {
        let node = scan_node(&[true, true, false], &[]);
        for status in [
            tonic::Status::internal("bad sql"),
            tonic::Status::deadline_exceeded("timeout"),
            tonic::Status::cancelled("cancelled by user"),
        ] {
            let state = RedispatchState::new(&node, usize::MAX);
            let calls = Arc::new(Mutex::new(Vec::new()));
            let code = status.code();
            let status = Mutex::new(Some(status));
            let result = race_attempts(
                "t",
                0,
                &node.nodes,
                &state,
                Some(POLICY),
                tokio::time::Instant::now(),
                |idx, redispatched| {
                    calls.lock().push((idx, redispatched));
                    let status = status
                        .lock()
                        .take()
                        .unwrap_or_else(|| tonic::Status::ok(""));
                    async move { Err::<usize, _>(failed(idx, AttemptFailure::Call(status))) }
                },
            )
            .await;
            let Err(failure) = result else {
                panic!("{code:?} must fail")
            };
            assert_eq!(failure.node_idx, 0);
            assert_eq!(*calls.lock(), vec![(0, false)], "{code:?} is not retried");
        }
    }

    #[tokio::test]
    async fn at_most_one_extra_attempt_and_the_first_failure_is_reported() {
        let node = scan_node(&[true, true, true, false], &[]);
        let state = RedispatchState::new(&node, usize::MAX);
        let calls = Arc::new(Mutex::new(Vec::new()));
        let result = race_attempts(
            "t",
            0,
            &node.nodes,
            &state,
            Some(POLICY),
            tokio::time::Instant::now(),
            |idx, redispatched| {
                calls.lock().push((idx, redispatched));
                async move { Err::<usize, _>(failed(idx, unavailable())) }
            },
        )
        .await;
        let Err(failure) = result else {
            panic!("both attempts fail")
        };
        assert_eq!(failure.node_idx, 0, "the first node's failure is reported");
        assert_eq!(calls.lock().len(), 2, "one extra attempt, no chain");
    }

    #[tokio::test]
    async fn no_redispatch_without_policy() {
        let node = scan_node(&[true, true, false], &[]);
        let state = RedispatchState::new(&node, usize::MAX);
        let calls = Arc::new(Mutex::new(Vec::new()));
        let result = race_attempts(
            "t",
            0,
            &node.nodes,
            &state,
            None,
            tokio::time::Instant::now(),
            |idx, redispatched| {
                calls.lock().push((idx, redispatched));
                async move { Err::<usize, _>(failed(idx, unavailable())) }
            },
        )
        .await;
        assert!(result.is_err());
        assert_eq!(*calls.lock(), vec![(0, false)]);
    }

    #[tokio::test]
    async fn straggler_gets_one_extra_attempt_and_the_first_answer_wins() {
        // n1 already answered, so it is the only candidate target.
        let node = scan_node(&[true, true], &[]);
        let state = RedispatchState::new(&node, usize::MAX);
        // The other participant answered in 10 ms: the threshold is the 50 ms minimum.
        state.record_answer(1, 10);
        let calls = Arc::new(Mutex::new(Vec::new()));
        let primary_dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let started = std::time::Instant::now();
        let result = race_attempts(
            "t",
            0,
            &node.nodes,
            &state,
            Some(POLICY),
            tokio::time::Instant::now(),
            |idx, redispatched| {
                calls.lock().push((idx, redispatched));
                let guard = (!redispatched).then(|| SetOnDrop(primary_dropped.clone()));
                async move {
                    let _guard = guard;
                    if !redispatched {
                        tokio::time::sleep(Duration::from_secs(30)).await;
                    }
                    Ok::<_, FailedAttempt>(idx)
                }
            },
        )
        .await;
        let (_, node_idx) = result.ok().expect("the extra attempt answers");
        assert_eq!(node_idx, 1);
        assert!(started.elapsed() < Duration::from_secs(5));
        assert!(
            primary_dropped.load(Ordering::SeqCst),
            "the slow attempt is cancelled"
        );
        assert_eq!(*calls.lock(), vec![(0, false), (1, true)]);
    }

    #[tokio::test]
    async fn first_attempt_still_wins_when_the_extra_attempt_fails() {
        // n1 already answered, so it is the only candidate target.
        let node = scan_node(&[true, true], &[]);
        let state = RedispatchState::new(&node, usize::MAX);
        state.record_answer(1, 10);
        let calls = Arc::new(Mutex::new(Vec::new()));
        let result = race_attempts(
            "t",
            0,
            &node.nodes,
            &state,
            Some(POLICY),
            tokio::time::Instant::now(),
            |idx, redispatched| {
                calls.lock().push((idx, redispatched));
                async move {
                    if redispatched {
                        Err(failed(idx, unavailable()))
                    } else {
                        tokio::time::sleep(Duration::from_millis(400)).await;
                        Ok(idx)
                    }
                }
            },
        )
        .await;
        let (_, node_idx) = result.ok().expect("the first attempt answers");
        assert_eq!(node_idx, 0);
        assert_eq!(*calls.lock(), vec![(0, false), (1, true)]);
    }

    #[tokio::test]
    async fn extra_attempt_is_cancelled_when_the_first_attempt_answers() {
        let node = scan_node(&[true, true, false], &[]);
        let state = RedispatchState::new(&node, usize::MAX);
        state.record_answer(1, 10);
        let extra_dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let result = race_attempts(
            "t",
            0,
            &node.nodes,
            &state,
            Some(POLICY),
            tokio::time::Instant::now(),
            |idx, redispatched| {
                let guard = redispatched.then(|| SetOnDrop(extra_dropped.clone()));
                async move {
                    let _guard = guard;
                    let wait = if redispatched { 30_000 } else { 400 };
                    tokio::time::sleep(Duration::from_millis(wait)).await;
                    Ok::<_, FailedAttempt>(idx)
                }
            },
        )
        .await;
        let (_, node_idx) = result.ok().expect("the first attempt answers");
        assert_eq!(node_idx, 0);
        assert!(
            extra_dropped.load(Ordering::SeqCst),
            "the extra attempt is cancelled"
        );
    }

    #[test]
    fn attempt_sinks_merge_moves_data_once() {
        let shared = AttemptSinks::default();
        shared.partial_err.lock().push_str("earlier");
        let winner = AttemptSinks::default();
        winner.scan_stats.lock().files = 3;
        winner.partial_err.lock().push_str("winner");
        winner.peak_memory.store(7, Ordering::Relaxed);
        winner.merge_into(&shared);
        winner.merge_into(&shared);
        assert_eq!(
            shared.scan_stats.lock().files,
            3,
            "stats are not added twice"
        );
        assert_eq!(*shared.partial_err.lock(), "earlier \n winner");
        assert_eq!(shared.peak_memory.load(Ordering::Relaxed), 7);
    }

    #[test]
    fn transport_codes_are_the_only_retryable_call_errors() {
        assert!(unavailable().is_retryable());
        assert!(AttemptFailure::Call(tonic::Status::unknown("h2 error")).is_retryable());
        assert!(AttemptFailure::Connect(tonic::Status::internal("connect err")).is_retryable());
        assert!(AttemptFailure::StreamBroken(tonic::Code::Unavailable).is_retryable());
        assert!(!AttemptFailure::Call(tonic::Status::internal("bad sql")).is_retryable());
        assert!(!AttemptFailure::Call(tonic::Status::cancelled("user")).is_retryable());
        assert!(!AttemptFailure::Call(tonic::Status::deadline_exceeded("t")).is_retryable());
    }
}
