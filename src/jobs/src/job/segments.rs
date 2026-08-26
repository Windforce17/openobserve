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

//! Segment-WAL L0 builder (DESIGN-SEGMENT-WAL.md): turns uploaded segment
//! objects into per-(stream, hour) L0 data files.
//!
//! Loop shape: wait until a FULL `ZO_SEGMENT_BUILD_BATCH` is claimable (or
//! the oldest claimable segment is `ZO_SEGMENT_BUILD_MAX_WAIT_SECS` old —
//! rows stay queryable through the segment tail while they wait, so the
//! gate costs no freshness), claim the batch (leased), start the lease
//! heartbeat AT CLAIM TIME (the compactor heartbeat-gap lesson: a heartbeat
//! that covers only part of the job's life lets another node re-claim
//! mid-work), fetch + decode each object with a small bounded concurrency
//! (a segment that fails to fetch or decode is SKIPPED and left leased —
//! the lease expires and it retries; it never blocks the rest of the
//! batch), split the DECODED ids into contiguous runs, and per run: chunk
//! each stream's frames on ITS OWN decoded bytes (`chunk_run_per_stream` —
//! per-stream chunking is what keeps a stream's file count proportional to
//! its own volume instead of the fleet's segment cadence), homogenize each
//! chunk's write-time schemas ONCE (this is the designed single place
//! type-flips get resolved), split rows into hourly buckets, and build ONE
//! L0 file per (stream chunk, hour) through the exact same single-file
//! build the WAL mover uses (`write_core_file_from_tables` for logs/traces
//! `.vix`, `merge_parquet_files` for everything else) so compaction and the
//! query path stay completely unchanged. Before the FIRST upload, the batch's
//! deterministic keys are written to the claimed rows' `l0_planned` (fenced
//! by the lease; a short count discards the build with nothing uploaded), so
//! a builder crash mid-upload always leaves a durable record naming its
//! objects — the sweeper's GC collects them. Then all objects upload, and
//! all produced files register AND the segments flip Built in ONE fenced
//! `wal_segments::mark_built_with_files` transaction (which clears
//! `l0_planned` in the same statement) — a builder crash can no longer land
//! between registration and the flip, and a lost lease rolls the
//! registration back whole.
//!
//! Provenance: L0 object keys are a pure function of (the stream's chunk
//! ids, stream, hour) — `l0_{writer uuid|multi}_{chunk min id}_{chunk max
//! id}_{hour index}` — and a chunk only ever spans CONSECUTIVE decoded ids
//! (each stream's chunk ranges tile its run's whole id span), so every id
//! inside a registered key range is genuinely contained in the file: a
//! covered segment either contributed its rows for that stream or carried
//! none. The leader dedups candidates PER STREAM against that stream's own
//! registered `l0_` ranges, so different streams cutting the same run at
//! different byte boundaries is sound. A skipped id splits the runs around
//! it, stays outside every range, and remains queryable as a segment. Any build/upload failure
//! aborts the WHOLE batch before anything is registered, the segments stay leased, and
//! the expired lease retries them; the retry's identical decode set
//! re-produces identical keys, so uploads overwrite the same objects.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, anyhow};
use arrow::{
    array::{Array, ArrayRef, BooleanArray, Int64Array, new_null_array},
    compute::{cast, concat_batches, filter_record_batch},
    record_batch::RecordBatch,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use config::{
    FileFormat, TIMESTAMP_COL_NAME,
    cluster::{LOCAL_NODE, is_offline},
    get_config, ider,
    meta::stream::{FileKey, FileMeta, StreamType},
    metrics,
    utils::{
        record_batch_ext::{RecordBatchExt, sort_record_batch_by_column},
        schema_ext::SchemaExt,
        time::now_micros,
    },
};
use futures::StreamExt;
use hashbrown::HashMap;
use infra::{
    schema::{
        get_stream_setting_bloom_filter_fields,
        get_stream_setting_fts_fields,
    },
    storage,
    wal_segments::{self, ClaimOrder, SegmentMeta},
};
use segment_wal::{SegmentFrame, decode_segment};
use tokio::time::MissedTickBehavior;

use crate::service::{
    compact::incremental::incr_pending_file,
    search::datafusion::{
        merge::{MergeParquetResult, merge_parquet_files},
        table_provider::memtable::NewMemTable,
    },
    vix::core_writer,
};

const HOUR_MICROS: i64 = 3_600_000_000;
/// Claim poll interval while the backlog is empty; a full claim loops
/// immediately so a backlog drains at build speed, not poll speed.
const BUILDER_TICK_SECS: u64 = 5;
// Segments in flight for fetch+decode: M13 (1c) — was a hardcoded 2
// (`FETCH_DECODE_CONCURRENCY`), now `ZO_SEGMENT_FETCH_DECODE_CONCURRENCY`
// (default 2 keeps that behavior byte-for-byte). With items 1/1b removing
// the claim-side waits, serial-ish fetch+decode of a ~512MB super-batch's
// ~130 objects two at a time became THE drain-rate limiter on prod
// (2026-08-19: 100-160s cycles dominated by this stage).
/// Unbuilt-backlog visibility (same signal the sweeper logs): segments
/// older than this and not yet built mean builders are down or behind.
const BACKLOG_WARN_AGE_SECS: u64 = 600;
/// The backlog check+warn runs at most this often — the claim loop passes
/// every `BUILDER_TICK_SECS`, which would spam both the DB and the log.
const BACKLOG_WARN_PERIOD: Duration = Duration::from_secs(60);
/// M13 aging-lane cadence accumulator: per-mille of
/// `ZO_SEGMENT_BUILD_AGE_LANE_RATIO` accumulated once per ENGAGED claim
/// pass; each time it crosses a whole unit the pass claims oldest-first
/// (see [`age_lane_fire`]). Process-global like the builder loop itself;
/// it only advances while the lane is engaged, so steady state carries no
/// cadence state.
static AGE_LANE_ACC: AtomicU64 = AtomicU64::new(0);

/// M17 item 3: process-wide DECODED-byte admission for L0 building —
/// replaces the count-knob treadmill (`ZO_SEGMENT_BUILD_BATCH` /
/// `ZO_L0_SUPERBATCH_MB` / `ZO_SEGMENT_BUILD_CONCURRENCY` retunes chased
/// every traffic-shape change; 4 ops PRs in 24h around the .109 drain).
/// Counts bound the wrong thing: the OOM dimension is resident DECODED
/// bytes, which vary 1-10x per compressed byte by stream shape. Two
/// reservation classes share one budget:
///
/// - **Claim**: `process_claim` reserves the batch's ESTIMATED decoded bytes (Σ segment meta size ×
///   [`decode_inflation_ema`], seeded 5.0) before `fetch_and_decode`, RESIZES the reservation to
///   the post-decode actual (the frames are resident until the batch ends) and feeds the EMA with
///   the observed ratio. Released when the batch finishes.
/// - **Build**: each stream-chunk build reserves its ACTUAL decoded input bytes (the proxy for its
///   sort/synthesis working set, M12's accounting) for the duration of the build.
///   `ZO_SEGMENT_BUILD_CONCURRENCY` stays as the secondary COUNT cap.
///
/// A reservation that cannot fit WAITS — but one reservation per class
/// always admits (an oversized super-batch or a fat singleton build must
/// proceed alone, never deadlock). Budget = `ZO_SEGMENT_BUILD_MEMORY_BUDGET_MB`,
/// `0` (default) = 40% of the detected container/cgroup memory, floored at
/// 256 MiB so an undetectable limit cannot stall building.
struct BuildMemoryBudget {
    budget: u64,
    state: std::sync::Mutex<BudgetState>,
    notify: tokio::sync::Notify,
}

#[derive(Default)]
struct BudgetState {
    used: u64,
    claims: usize,
    builds: usize,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum BudgetClass {
    Claim,
    Build,
}

/// One admitted reservation; released (with waiter wakeup) on drop.
struct BudgetReservation<'a> {
    owner: &'a BuildMemoryBudget,
    bytes: u64,
    class: BudgetClass,
}

impl BuildMemoryBudget {
    fn new(budget: u64) -> Self {
        Self {
            budget,
            state: std::sync::Mutex::new(BudgetState::default()),
            notify: tokio::sync::Notify::new(),
        }
    }

    /// Admit `bytes` under `class`: immediately when it fits the budget or
    /// when no reservation of this class is active (the always-one floor);
    /// otherwise wait for releases.
    async fn acquire(&self, bytes: u64, class: BudgetClass) -> BudgetReservation<'_> {
        loop {
            // arm the waiter BEFORE the check: a release between the failed
            // check and the await would otherwise be a lost wakeup
            let notified = self.notify.notified();
            {
                let mut state = self.state.lock().expect("budget state poisoned");
                let class_active = match class {
                    BudgetClass::Claim => state.claims,
                    BudgetClass::Build => state.builds,
                };
                if class_active == 0 || state.used.saturating_add(bytes) <= self.budget {
                    state.used = state.used.saturating_add(bytes);
                    match class {
                        BudgetClass::Claim => state.claims += 1,
                        BudgetClass::Build => state.builds += 1,
                    }
                    return BudgetReservation {
                        owner: self,
                        bytes,
                        class,
                    };
                }
            }
            notified.await;
        }
    }

    /// Currently reserved bytes (tests / logging).
    fn used(&self) -> u64 {
        self.state.lock().expect("budget state poisoned").used
    }
}

impl BudgetReservation<'_> {
    /// Correct the reservation to `bytes` (the post-decode actual). The
    /// adjustment is UNCONDITIONAL in both directions — the memory is
    /// already real, so admission math must reflect it; a shrink wakes
    /// waiters.
    fn resize(&mut self, bytes: u64) {
        let mut state = self.owner.state.lock().expect("budget state poisoned");
        state.used = state.used.saturating_sub(self.bytes).saturating_add(bytes);
        let shrank = bytes < self.bytes;
        self.bytes = bytes;
        drop(state);
        if shrank {
            self.owner.notify.notify_waiters();
        }
    }
}

impl Drop for BudgetReservation<'_> {
    fn drop(&mut self) {
        let mut state = self.owner.state.lock().expect("budget state poisoned");
        state.used = state.used.saturating_sub(self.bytes);
        match self.class {
            BudgetClass::Claim => state.claims = state.claims.saturating_sub(1),
            BudgetClass::Build => state.builds = state.builds.saturating_sub(1),
        }
        drop(state);
        self.owner.notify.notify_waiters();
    }
}

/// The process-wide budget instance (resolved once from config).
static BUILD_MEMORY_BUDGET: std::sync::LazyLock<BuildMemoryBudget> =
    std::sync::LazyLock::new(|| {
        let budget = resolve_build_memory_budget_bytes();
        log::info!(
            "[SEGMENT:BUILD] memory budget: {} MB ({})",
            budget / (1024 * 1024),
            if get_config().common.segment_build_memory_budget_mb > 0 {
                "configured"
            } else {
                "auto: 40% of detected memory"
            }
        );
        BuildMemoryBudget::new(budget)
    });

fn resolve_build_memory_budget_bytes() -> u64 {
    let cfg = get_config();
    let configured_mb = cfg.common.segment_build_memory_budget_mb as u64;
    let bytes = if configured_mb > 0 {
        configured_mb.saturating_mul(1024 * 1024)
    } else {
        (cfg.limit.mem_total as u64).saturating_mul(2) / 5
    };
    bytes.max(256 * 1024 * 1024)
}

/// Compressed→decoded inflation EMA (seeded at 5.0 — conservative for the
/// first claims; observed ratios clamp to [1, 64] against skew from tiny
/// or pathological batches). Corrected after every decode with the
/// measured actual, α = 0.2.
static DECODE_INFLATION_EMA_BITS: AtomicU64 = AtomicU64::new(0);
const DECODE_INFLATION_SEED: f64 = 5.0;
const DECODE_INFLATION_ALPHA: f64 = 0.2;

fn decode_inflation_ema() -> f64 {
    let bits = DECODE_INFLATION_EMA_BITS.load(Ordering::Relaxed);
    if bits == 0 {
        DECODE_INFLATION_SEED
    } else {
        f64::from_bits(bits)
    }
}

fn observe_decode_inflation(compressed_bytes: u64, decoded_bytes: u64) {
    if compressed_bytes == 0 {
        return;
    }
    let observed = (decoded_bytes as f64 / compressed_bytes as f64).clamp(1.0, 64.0);
    let mut current = DECODE_INFLATION_EMA_BITS.load(Ordering::Relaxed);
    loop {
        let ema = if current == 0 {
            DECODE_INFLATION_SEED
        } else {
            f64::from_bits(current)
        };
        let next = ema * (1.0 - DECODE_INFLATION_ALPHA) + observed * DECODE_INFLATION_ALPHA;
        match DECODE_INFLATION_EMA_BITS.compare_exchange_weak(
            current,
            next.to_bits(),
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return,
            Err(actual) => current = actual,
        }
    }
}

/// Spawn the supervised builder. Same restart-on-error/panic pattern as
/// `job::files::spawn_supervised`: this loop is the only path segment data
/// takes to queryable L0 files, so a single unwind must not silently kill it
/// while the pod stays Ready.
pub fn run() {
    tokio::task::spawn(async {
        loop {
            match tokio::task::spawn(run_loop()).await {
                Ok(Ok(())) => {
                    log::info!("[SEGMENT:BUILD] job::segments exited cleanly");
                    break;
                }
                Ok(Err(e)) => {
                    log::error!(
                        "[SEGMENT:BUILD] job::segments exited with error, restarting in 5s: {e:#}"
                    );
                }
                Err(e) => {
                    log::error!("[SEGMENT:BUILD] job::segments panicked, restarting in 5s: {e}");
                }
            }
            if is_offline() {
                break;
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });
}

async fn run_loop() -> Result<(), anyhow::Error> {
    log::info!(
        "[SEGMENT:BUILD] L0 builder started, node={}",
        LOCAL_NODE.uuid
    );
    let mut backlog = false;
    let mut last_backlog_check: Option<Instant> = None;
    loop {
        if is_offline() {
            break;
        }
        if !backlog {
            tokio::time::sleep(Duration::from_secs(BUILDER_TICK_SECS)).await;
            if is_offline() {
                break;
            }
        }
        backlog = false;
        // backlog visibility from the builder itself (the sweeper's copy of
        // this warn only runs on compactor nodes): aged unbuilt segments at
        // the START of a pass mean this loop is not keeping up
        if last_backlog_check.is_none_or(|t| t.elapsed() >= BACKLOG_WARN_PERIOD) {
            last_backlog_check = Some(Instant::now());
            match wal_segments::count_unbuilt_older_than(BACKLOG_WARN_AGE_SECS).await {
                Ok(0) => {}
                Ok(n) => {
                    log::warn!(
                        "[SEGMENT:BUILD] backlog: unbuilt_older_10m={n} — builders down or behind"
                    );
                }
                Err(e) => {
                    log::error!("[SEGMENT:BUILD] backlog count failed: {e}");
                }
            }
        }
        let cfg = get_config();
        let batch_size = cfg.common.segment_build_batch;
        let lease_secs = cfg.common.segment_build_lease_secs;
        // Claim gate: a 1-2-segment claim emits a sliver L0 file for every
        // stream it touches, so builders that poll faster than segments
        // accumulate convert the fleet's ~1s segment cadence directly into
        // per-stream file counts (~10k files/hour/stream in prod,
        // 2026-08-07). Wait for a full batch — or the age cap, so
        // low-traffic deployments still build promptly — and claims (and
        // therefore per-stream L0 files) come out batch-sized. Rows stay
        // queryable through the segment tail the whole time. Fails OPEN on
        // a stats error: building sliver claims beats not building.
        let max_wait_secs = cfg.common.segment_build_max_wait_secs;
        // All-or-nothing floor: when the pool holds a full batch, claim
        // exactly batch-or-nothing — racing builders otherwise SPLIT one
        // batch-sized pool into sliver claims, and every sliver claim
        // emits a sliver L0 file per stream it touches (prod 2026-08-12:
        // 14+ builders turned 32-segment pools into ~7-segment claims;
        // metric streams landed 1-16-record files). Age-triggered claims
        // drop the floor: freshness beats file size on quiet streams.
        let mut min_batch = 1;
        let mut effective_batch = batch_size;
        // M13 aging lane: newest-first claiming is right in steady state
        // (fresh windows recover first) but starves the oldest cohort under
        // a STANDING backlog — the 1-day raw-object lifecycle then deletes
        // unbuilt data (prod 2026-08-18/19: oldest pending stuck 15+ hours
        // at a 74.5k backlog). While the oldest claimable segment is older
        // than the lane threshold, a configured fraction of claim passes
        // (the whole pass: initial claim + super-batch extensions) scans
        // OLDEST-first instead. Claim semantics (floor, SKIP LOCKED) are
        // identical in both lanes; disengaged behavior is byte-identical
        // to pre-M13.
        let lane_secs = cfg.common.segment_build_age_lane_secs;
        let lane_ratio = cfg.common.segment_build_age_lane_ratio;
        let lane_enabled = lane_secs > 0 && lane_ratio > 0.0;
        let mut claim_order = ClaimOrder::NewestFirst;
        // M31a late lane: hold-expired ALL-LATE segments (the fleet's
        // late-arriving rows, segregated at flush) claim as their OWN pass,
        // oldest-first — one wave coalesces them into one L0 file per
        // (stream, hour) instead of a sliver per build batch. Takes this
        // tick INSTEAD of a fresh pass; late waves surface roughly once per
        // hold window, so fresh starvation is not a real shape. The fresh
        // gates below (claim floor / max-wait / byte budget) don't apply —
        // the hold already did the batching.
        let mut late_pass = false;
        if cfg.common.segment_late_lane_hours > 0 {
            match wal_segments::has_late_claimable().await {
                Ok(true) => late_pass = true,
                Ok(false) => {}
                Err(e) => {
                    log::error!("[SEGMENT:BUILD] has_late_claimable failed: {e}");
                }
            }
        }
        if late_pass {
            claim_order = ClaimOrder::LateOldestFirst;
            log::info!(
                "[SEGMENT:BUILD] late lane: claiming hold-expired all-late segments oldest-first"
            );
        } else if max_wait_secs > 0 || lane_enabled {
            // #50: cheap existence probe each tick; the count/min/SUM
            // aggregate runs only when something is claimable. Idle fleets
            // cost one indexed LIMIT-1 row per builder-second with zero
            // added drain latency (a timing-driven backoff here delayed
            // seg-mode builds enough to flip alert triggers into retries —
            // integration_test 3304, 2026-08-13).
            match wal_segments::has_claimable(lease_secs).await {
                Ok(false) => continue,
                Ok(true) => {}
                Err(e) => {
                    log::error!("[SEGMENT:BUILD] has_claimable failed (claiming anyway): {e}");
                }
            }
            match wal_segments::claimable_stats(lease_secs).await {
                Ok((0, ..)) => continue,
                Ok((count, oldest_created_at, total_size)) => {
                    let now = now_micros();
                    // #47: byte-budget adaptive batch — derive the claim
                    // count from the live average segment size so the
                    // decode-memory bound is the BUDGET, not a row count
                    // (fat vpc-flow segments claim fewer, thin metrics
                    // segments claim more).
                    let claim_mb = cfg.common.segment_build_claim_mb;
                    if claim_mb > 0 && count > 0 && total_size > 0 {
                        let avg = (total_size / count).max(1) as usize;
                        effective_batch = ((claim_mb * 1024 * 1024) / avg).clamp(4, 256);
                    }
                    if lane_enabled
                        && age_lane_engaged(oldest_created_at, now, lane_secs)
                        && age_lane_fire(&AGE_LANE_ACC, lane_ratio)
                    {
                        claim_order = ClaimOrder::OldestFirst;
                        log::info!(
                            "[SEGMENT:BUILD] aging lane: claiming oldest-first (oldest pending \
                             {}s > lane {lane_secs}s, claimable {count})",
                            now.saturating_sub(oldest_created_at) / 1_000_000,
                        );
                    }
                    if max_wait_secs > 0 {
                        let age_triggered = now.saturating_sub(oldest_created_at)
                            >= i64::try_from(max_wait_secs)
                                .unwrap_or(i64::MAX)
                                .saturating_mul(1_000_000);
                        if (count as usize) >= effective_batch {
                            min_batch = effective_batch;
                        } else if !age_triggered {
                            continue;
                        }
                    }
                }
                Err(e) => {
                    log::error!("[SEGMENT:BUILD] claimable_stats failed (claiming anyway): {e}");
                }
            }
        }
        let claim = match wal_segments::claim_pending_with_floor(
            &LOCAL_NODE.uuid,
            effective_batch,
            min_batch,
            lease_secs,
            claim_order,
        )
        .await
        {
            Ok(v) => v,
            Err(e) => {
                log::error!("[SEGMENT:BUILD] claim_pending failed: {e}");
                continue;
            }
        };
        if claim.is_empty() {
            continue;
        }
        // Heartbeat covers the claim from THIS point to the end of the batch
        // (the fenced commit or failure) — never a partial window.
        let _heartbeat = HeartbeatGuard::spawn(
            claim.iter().map(|m| m.id).collect(),
            LOCAL_NODE.uuid.clone(),
            lease_secs,
        );

        // #54 super-batch: keep claiming and CONCATENATE while the pool has
        // full batches, until the byte budget or the age cap. One
        // process_claim over the union preserves today's all-or-nothing
        // semantics exactly — it is simply a bigger batch across time — and
        // each (stream, hour) it touches becomes ONE file instead of one
        // file per claim (the small-L0 explosion was per-claim slicing, not
        // data volume). Crash/failure re-pends the whole union via the same
        // guards + fenced release; the age cap bounds the replay window.
        //
        // M13 (1b) backlog-mode sealing: WHILE CLAIMS RETURN ROWS the loop
        // is bounded by WORK, never by the clock — claim to the byte target
        // and seal immediately. The age clock and the arrival-gap sealing
        // pace only the true steady-state trickle, i.e. after a claim comes
        // back empty AND the cheap #50 probe confirms the table is out of
        // claimable work (an empty claim WITH claimable work still present
        // is a SKIP-LOCKED race loss — retry immediately, bounded). Under a
        // 72k-segment prod backlog (2026-08-18/19) wait-paced accumulation
        // held builders at 5-8 super-batch cycles per 15min/pod; the
        // interim ops pin (prod-ops #432 clock=15s) becomes unnecessary.
        let mut claim = claim;
        let mut extra_guards: Vec<HeartbeatGuard> = Vec::new();
        let super_mb = cfg.common.segment_build_superbatch_mb;
        let super_secs = cfg.common.segment_build_superbatch_max_secs.max(10);
        if super_mb > 0 {
            accumulate_super_batch(
                &mut claim,
                &mut extra_guards,
                &LOCAL_NODE.uuid,
                effective_batch,
                lease_secs,
                claim_order,
                (super_mb as i64) * 1024 * 1024,
                super_secs,
            )
            .await;
        }

        // M12 fix 2 — self-tuning memory backoff: a batch that fails with a
        // RESOURCES-EXHAUSTED error would fail identically on an identical
        // retry (the pre-M12 loop released everything and re-claimed the
        // same union), so HALVE the batch instead — release the dropped
        // half for other builders and retry the kept half immediately.
        // Convergent (log2 attempts to the 1-segment floor, which always
        // gets a real attempt), resumable (identical deterministic keys),
        // and fenced (released rows go back to Pending; our heartbeat
        // no-ops on them — builder_node no longer matches). Non-memory
        // errors keep the release-all-now behavior below unchanged.
        let mut result = process_claim(&claim, &LOCAL_NODE.uuid).await;
        while let Err(e) = &result {
            if claim.len() <= 1 || !is_resources_exhausted(e) {
                break;
            }
            let failed_len = claim.len();
            let dropped = halve_for_retry(&mut claim);
            log::warn!(
                "[SEGMENT:BUILD] batch of {failed_len} segments failed on memory; retrying with \
                 {} (releasing {} for other builders): {e:#}",
                claim.len(),
                dropped.len()
            );
            if let Err(re) = infra::wal_segments::release_claims(&dropped, &LOCAL_NODE.uuid).await
            {
                log::error!(
                    "[SEGMENT:BUILD] releasing {} halved claims failed (lease expiry covers \
                     them): {re}",
                    dropped.len()
                );
            }
            result = process_claim(&claim, &LOCAL_NODE.uuid).await;
        }
        match result {
            Ok(stats) => {
                log::info!("[SEGMENT:BUILD] {stats}");
                // a full claim means more may be pending: drain immediately
                backlog = claim.len() >= effective_batch;
            }
            Err(e) => {
                // nothing was registered (registration and the Built flip
                // are one all-or-nothing transaction). Release the claims
                // NOW instead of waiting out the lease: a deterministic
                // failure retrying at lease cadence stalled the whole drain
                // for ~2min per attempt (prod OOM loop, 2026-07-31); an
                // immediate fenced release lets any node retry at claim
                // cadence, and a genuinely-poisoned batch stays loud in the
                // logs either way.
                log::error!(
                    "[SEGMENT:BUILD] batch of {} segments failed, releasing claims for retry: {e:#}",
                    claim.len()
                );
                let ids: Vec<i64> = claim.iter().map(|s| s.id).collect();
                if let Err(re) = infra::wal_segments::release_claims(&ids, &LOCAL_NODE.uuid).await {
                    log::error!(
                        "[SEGMENT:BUILD] releasing {} claims failed (lease expiry covers them): {re}",
                        ids.len()
                    );
                }
            }
        }
    }
    log::info!("[SEGMENT:BUILD] node offline, L0 builder exiting");
    Ok(())
}

/// Lease keeper for one claimed batch. Owned by the batch scope: spawned at
/// claim time, aborted when the guard drops (both success and failure
/// paths), so the heartbeat window exactly matches the claim's life. A
/// failed heartbeat call is logged and retried next tick — if the DB stays
/// unreachable the lease simply expires, which is the designed recovery.
struct HeartbeatGuard {
    handle: tokio::task::JoinHandle<()>,
}

impl HeartbeatGuard {
    fn spawn(ids: Vec<i64>, node: String, lease_secs: u64) -> Self {
        let period = Duration::from_secs((lease_secs / 3).max(1));
        let handle = tokio::task::spawn(async move {
            let mut ticker = tokio::time::interval(period);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                if let Err(e) = wal_segments::heartbeat(&ids, &node).await {
                    log::warn!(
                        "[SEGMENT:BUILD] lease heartbeat failed for segment ids {ids:?}: {e}"
                    );
                }
            }
        });
        Self { handle }
    }
}

impl Drop for HeartbeatGuard {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// M12 fix 2: whether an error chain is a MEMORY-pool failure (DataFusion's
/// `ResourcesExhausted`, canonical display prefix "Resources exhausted") —
/// the one failure class where retrying the identical batch is pointless
/// and HALVING it converges. Checked across the whole `anyhow` context
/// chain: the builder wraps the plan error in per-stream/per-hour context.
fn is_resources_exhausted(e: &anyhow::Error) -> bool {
    e.chain()
        .any(|cause| cause.to_string().contains("Resources exhausted"))
}

/// M12 fix 2: shrink `claim` to its first half for the memory-backoff retry
/// (floor 1 — callers gate on `len() > 1`), returning the DROPPED segment
/// ids so the caller releases them for other builders. Keeping the FIRST
/// half keeps ids contiguous-ish with the original claim order, so the
/// retry's deterministic L0 keys stay a prefix of the failed attempt's plan.
fn halve_for_retry(claim: &mut Vec<SegmentMeta>) -> Vec<i64> {
    let keep = (claim.len() / 2).max(1);
    let dropped = claim[keep..].iter().map(|s| s.id).collect();
    claim.truncate(keep);
    dropped
}

/// M13 aging-lane engagement: the lane exists only while the OLDEST
/// claimable segment (`claimable_stats`' min `created_at`, micros) is older
/// than `lane_secs` (0 = lane disabled). It disengages by itself the moment
/// the aged band drains below the threshold, restoring pure newest-first
/// claiming.
fn age_lane_engaged(oldest_created_at: i64, now: i64, lane_secs: u64) -> bool {
    lane_secs > 0
        && oldest_created_at > 0
        && now.saturating_sub(oldest_created_at)
            >= i64::try_from(lane_secs)
                .unwrap_or(i64::MAX)
                .saturating_mul(1_000_000)
}

/// One ENGAGED claim pass' lane decision: accumulate `ratio` in fixed-point
/// per-mille and fire (claim oldest-first) each time the running total
/// crosses a whole unit — ratio 0.25 fires every 4th engaged pass, 1.0
/// every engaged pass, 0 never. Deterministic for any ratio in [0, 1] and
/// exact in the long run (no drift); callers must only tick this while the
/// lane is engaged so the cadence counts engaged passes, not wall ticks.
fn age_lane_fire(acc: &AtomicU64, ratio: f64) -> bool {
    let step = (ratio.clamp(0.0, 1.0) * 1000.0).round() as u64;
    if step == 0 {
        return false;
    }
    let prev = acc.fetch_add(step, Ordering::Relaxed);
    (prev + step) / 1000 > prev / 1000
}

/// M13 (1b): consecutive immediate retries an empty extension claim gets
/// while the #50 probe still reports claimable work, before it is treated
/// as a real arrival gap. SKIP-LOCKED race losses resolve in milliseconds
/// (candidate locks live for one statement), so a small bound both breaks
/// the wait-pacing and guards against a pathological probe/claim
/// disagreement spinning hot.
const EMPTY_CLAIM_RACE_RETRIES: u32 = 3;

/// What a super-batch accumulation does when an extension claim comes back
/// EMPTY (M13 1b). See [`on_empty_extension_claim`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EmptyClaimAction {
    /// The table still has claimable work — the emptiness was a racing
    /// loss, not an arrival gap: claim again immediately (no sleep, no
    /// empty-tick), bounded by [`EMPTY_CLAIM_RACE_RETRIES`].
    RetryNow,
    /// Real arrival gap inside the age window: sleep one builder tick and
    /// re-claim (the #54 trickle-accumulation mechanism, unchanged).
    Wait,
    /// Seal the batch now: the age clock expired or two consecutive empty
    /// ticks mean traffic stopped.
    Seal,
}

/// M13 (1b) backlog-mode sealing policy, pure for the test matrix: while
/// the table has claimable work the accumulation is bounded by WORK (claim
/// again immediately toward the byte target); the age clock and the
/// two-empty-ticks arrival-gap seal pace only the true trickle — the state
/// where claims come back empty AND nothing is claimable. `race_retries`
/// resets on any non-empty claim (caller); `empty_ticks` counts only real
/// gap waits, exactly like pre-M13 #54.
fn on_empty_extension_claim(
    claimable: bool,
    race_retries: &mut u32,
    empty_ticks: &mut u32,
    elapsed: Duration,
    super_secs: u64,
) -> EmptyClaimAction {
    if claimable && *race_retries < EMPTY_CLAIM_RACE_RETRIES {
        *race_retries += 1;
        return EmptyClaimAction::RetryNow;
    }
    // the age clock caps accumulated WAITING (the crash-replay bound);
    // it never cuts a flowing accumulation — sealing by size is the point
    if elapsed >= Duration::from_secs(super_secs) {
        return EmptyClaimAction::Seal;
    }
    *empty_ticks += 1;
    if *empty_ticks >= 2 {
        return EmptyClaimAction::Seal;
    }
    EmptyClaimAction::Wait
}

/// #54 super-batch accumulation, extracted for the M13 (1b) pins: extend
/// `claim` with floor-1 claims until `budget` bytes. WHILE CLAIMS RETURN
/// ROWS the loop is bounded by WORK alone — no clock check, no sleeps —
/// so a deep backlog seals by SIZE at claim speed. Empty claims consult
/// the cheap #50 probe: claimable work still present = a SKIP-LOCKED race
/// loss (retry immediately, bounded by [`EMPTY_CLAIM_RACE_RETRIES`]);
/// otherwise the pre-M13 trickle mechanism paces — one builder-tick wait
/// per gap, sealed by two consecutive empty ticks or the `super_secs` age
/// clock (which caps accumulated WAITING and stays the crash-replay
/// bound). Every claimed extension gets a [`HeartbeatGuard`] pushed to
/// `extra_guards`. Returns the accumulated byte total.
#[allow(clippy::too_many_arguments)]
async fn accumulate_super_batch(
    claim: &mut Vec<SegmentMeta>,
    extra_guards: &mut Vec<HeartbeatGuard>,
    node: &str,
    effective_batch: usize,
    lease_secs: u64,
    claim_order: ClaimOrder,
    budget: i64,
    super_secs: u64,
) -> i64 {
    let started = Instant::now();
    let mut total: i64 = claim.iter().map(|m| m.size).sum();
    let mut empty_ticks: u32 = 0;
    let mut race_retries: u32 = 0;
    while total < budget {
        // Extensions take ANYTHING that arrived (floor=1): the union is
        // already at least one full batch, so #44 sliver-avoidance is
        // satisfied by the batch as a whole — a full-batch floor here loses
        // the race against the tick cadence and seals with zero extensions
        // (.101 on dev: no accumulation ever).
        let more = match wal_segments::claim_pending_with_floor(
            node,
            effective_batch,
            1,
            lease_secs,
            // an aging pass extends oldest-first too: the union then drains
            // a CONTIGUOUS aged band (adjacent old hours → fewer
            // (stream, hour) output slices) instead of mixing the aged
            // cohort with fresh arrivals
            claim_order,
        )
        .await
        {
            Ok(v) => v,
            Err(e) => {
                log::warn!("[SEGMENT:BUILD] super-batch extension claim failed: {e}");
                break;
            }
        };
        if more.is_empty() {
            let claimable = if race_retries < EMPTY_CLAIM_RACE_RETRIES {
                wal_segments::has_claimable(lease_secs)
                    .await
                    .unwrap_or_else(|e| {
                        log::warn!(
                            "[SEGMENT:BUILD] super-batch claimable probe failed (treating as \
                             an arrival gap): {e}"
                        );
                        false
                    })
            } else {
                false
            };
            match on_empty_extension_claim(
                claimable,
                &mut race_retries,
                &mut empty_ticks,
                started.elapsed(),
                super_secs,
            ) {
                EmptyClaimAction::RetryNow => continue,
                EmptyClaimAction::Seal => break,
                EmptyClaimAction::Wait => {
                    // True arrival gap: wait for arrivals inside the age
                    // window instead of sealing — outlasting the claim
                    // cadence is the whole mechanism (the first cut sealed
                    // here and dev reproduced pre-#54 slicing exactly). Two
                    // consecutive empty ticks (traffic stopped) or the age
                    // cap seal instead; the segment tail serves queries for
                    // everything held here.
                    tokio::time::sleep(Duration::from_secs(BUILDER_TICK_SECS)).await;
                    if is_offline() {
                        break;
                    }
                    continue;
                }
            }
        }
        race_retries = 0;
        empty_ticks = 0;
        extra_guards.push(HeartbeatGuard::spawn(
            more.iter().map(|m| m.id).collect(),
            node.to_string(),
            lease_secs,
        ));
        total += more.iter().map(|m| m.size).sum::<i64>();
        claim.extend(more);
    }
    if !extra_guards.is_empty() {
        log::info!(
            "[SEGMENT:BUILD] super-batch: {} segments / {} MB accumulated in {:?}",
            claim.len(),
            total / (1024 * 1024),
            started.elapsed()
        );
    }
    total
}

struct BatchStats {
    claimed: usize,
    built: usize,
    skipped: usize,
    /// M29: claimed segments whose objects were gone (404) and were
    /// terminally resolved this batch instead of recycling forever.
    gone: usize,
    streams: usize,
    files: usize,
    rows: i64,
    flipped: u64,
    took_ms: u128,
}

impl std::fmt::Display for BatchStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "batch done: segments in={} built={} skipped={} gone={} streams={} l0_files={} rows={} flipped={} took_ms={}",
            self.claimed,
            self.built,
            self.skipped,
            self.gone,
            self.streams,
            self.files,
            self.rows,
            self.flipped,
            self.took_ms
        )
    }
}

/// One claimed batch end-to-end: fetch/decode (per-segment skip on failure),
/// split the decoded ids into contiguous runs, chunk each run per stream on
/// the stream's own decoded bytes, build one L0 file per (stream chunk,
/// hour), write the batch's deterministic keys to the claimed rows'
/// `l0_planned` (fenced; a SHORT count means the lease was lost and the
/// build is discarded with NOTHING uploaded — an unplanned upload would be
/// invisible to the GC forever), then upload every object, then commit
/// registration AND the fenced Built flip as ONE `mark_built_with_files`
/// transaction (clearing `l0_planned` in the same statement). A short flip
/// count means the lease was lost: the transaction rolled back (zero file
/// rows) and the build is discarded — uploaded objects are NOT deleted, the
/// lease winner either overwrites them (identical decode set, identical
/// keys) or leaves them planned on the dead rows for the GC. Returns Err
/// WITHOUT registering anything when any build/plan/upload step fails — the
/// leases expire and the identical keys make the retry idempotent.
async fn process_claim(claim: &[SegmentMeta], node: &str) -> Result<BatchStats, anyhow::Error> {
    let started = Instant::now();

    // M17 admission: reserve the batch's ESTIMATED decoded bytes before
    // anything decodes — the whole super-batch's frames stay resident until
    // the batch commits, so this reservation is the process's claim-side
    // memory truth (corrected to actuals right after decode).
    let compressed_total: u64 = claim.iter().map(|m| m.size.max(0) as u64).sum();
    let estimated = (compressed_total as f64 * decode_inflation_ema()) as u64;
    let admission_started = Instant::now();
    let mut claim_reservation = BUILD_MEMORY_BUDGET
        .acquire(estimated, BudgetClass::Claim)
        .await;
    let admission_wait = admission_started.elapsed();
    if admission_wait > Duration::from_millis(50) {
        log::info!(
            "[SEGMENT:BUILD] memory admission waited {admission_wait:?} for {} MB estimated \
             decoded ({} segments)",
            estimated / (1024 * 1024),
            claim.len()
        );
    }

    let (decoded, skipped, gone) = fetch_and_decode(claim).await;
    let built_ids: Vec<i64> = decoded.iter().map(|(id, _)| *id).collect();

    // M29: terminally resolve claimed segments whose objects are GONE
    // (fetch returned NotFound — lifecycle-expired; a retry can never
    // succeed). Fenced flip to Built with zero files: exactly the state a
    // confirmed-deleted segment ends in, so the normal sweeper retires the
    // row (its object delete finds NotFound, which it counts as confirmed).
    // Rows whose lease was lost meanwhile simply don't flip — the next
    // claimer resolves them. One count line per batch; per-item at debug
    // (house log discipline — the old per-item ERROR emitted 722k lines/30m
    // fleet-wide and re-ingested into obs itself).
    let mut tombstoned = 0u64;
    if !gone.is_empty() {
        match wal_segments::mark_built(&gone, node).await {
            Ok(flipped) => {
                tombstoned = flipped;
                log::warn!(
                    "[SEGMENT:BUILD] {} of {} claimed segments' objects are gone (404/lifecycle-expired): tombstoned (Built with no files; sweeper retires the rows); ids at debug",
                    flipped,
                    gone.len(),
                );
            }
            Err(e) => {
                // leave them leased; the lease expiry retries the resolution
                log::error!(
                    "[SEGMENT:BUILD] tombstoning {} gone segments failed (lease expiry retries them): {e}",
                    gone.len(),
                );
            }
        }
    }
    if !skipped.is_empty() {
        log::warn!(
            "[SEGMENT:BUILD] {} claimed segments skipped this round on transient fetch/decode failures (lease will expire and retry); ids at debug",
            skipped.len(),
        );
    }

    // post-decode correction: resize the reservation to the REAL resident
    // bytes and teach the estimator the observed inflation
    let decoded_actual: u64 = decoded
        .iter()
        .map(|(_, frames)| frames.iter().map(|f| f.batch.size() as u64).sum::<u64>())
        .sum();
    claim_reservation.resize(decoded_actual);
    let skipped_set: std::collections::HashSet<i64> = skipped
        .iter()
        .chain(gone.iter())
        .copied()
        .collect();
    let decoded_compressed: u64 = claim
        .iter()
        .filter(|m| !skipped_set.contains(&m.id))
        .map(|m| m.size.max(0) as u64)
        .sum();
    observe_decode_inflation(decoded_compressed, decoded_actual);
    log::debug!(
        "[SEGMENT:BUILD] memory admission: estimated {} MB -> actual {} MB decoded (ema now \
         {:.2}, budget used {} MB)",
        estimated / (1024 * 1024),
        decoded_actual / (1024 * 1024),
        decode_inflation_ema(),
        BUILD_MEMORY_BUDGET.used() / (1024 * 1024),
    );

    let meta_by_id: HashMap<i64, &SegmentMeta> = claim.iter().map(|m| (m.id, m)).collect();

    // Plan every (stream chunk) build first. Chunk inputs are measured in
    // DECODED arrow bytes — the compressed column `size` under-measured
    // traces ~10x and OOM'd the pool (2026-07-31) — and the byte cap is per
    // STREAM (`chunk_run_per_stream`): capping the run's aggregate bytes
    // emitted a sliver file for every stream in every ~128MB of fleet
    // traffic. The plans are then executed with bounded concurrency;
    // oversized singletons run with the whole budget to themselves.
    struct BuildPlan {
        key_parts: L0KeyParts,
        group: StreamGroup,
        decoded_bytes: usize,
    }
    let mut plans: Vec<BuildPlan> = Vec::new();
    let mut stream_keys: BTreeSet<String> = BTreeSet::new();
    for run in split_into_id_runs(decoded) {
        for chunked in chunk_run_per_stream(run) {
            let StreamChunks {
                org,
                stream_type,
                stream,
                chunks,
            } = chunked;
            stream_keys.insert(format!("{org}/{stream_type}/{stream}"));
            for chunk in chunks {
                // a run holds only DECODED ids and a chunk's range tiles a
                // sub-span of its run, so every covered id resolves
                let chunk_metas = (chunk.start_id..=chunk.end_id)
                    .map(|id| {
                        meta_by_id.get(&id).copied().ok_or_else(|| {
                            anyhow!("chunk segment id {id} is missing from the claim")
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let key_parts = l0_key_parts(&chunk_metas)?;
                plans.push(BuildPlan {
                    key_parts,
                    group: StreamGroup {
                        org: org.clone(),
                        stream_type,
                        stream: stream.clone(),
                        batches: chunk.batches,
                    },
                    decoded_bytes: chunk.decoded_bytes,
                });
            }
        }
    }

    // Execute: small builds run ZO_SEGMENT_BUILD_CONCURRENCY at a time
    // (the M12 count cap, secondary since M17), each first RESERVING its
    // decoded input bytes against the process budget — the byte budget is
    // the binding control: builds only run wide when their bytes provably
    // fit (the always-one floor keeps an oversized build from deadlocking).
    // A build whose input alone exceeds the per-build byte cap still runs
    // SERIALLY so nothing stacks beside it.
    let (large, small): (Vec<BuildPlan>, Vec<BuildPlan>) = plans
        .into_iter()
        .partition(|p| p.decoded_bytes > BUILD_GROUP_MAX_DECODED_BYTES);
    let mut built_files: Vec<BuiltL0File> = Vec::new();
    let mut rows: i64 = 0;
    let mut small_results = futures::stream::iter(small.into_iter().map(|plan| async move {
        let _admitted = BUILD_MEMORY_BUDGET
            .acquire(plan.decoded_bytes as u64, BudgetClass::Build)
            .await;
        build_stream_files(plan.group, &plan.key_parts).await
    }))
    .buffered(build_concurrency());
    while let Some(result) = small_results.next().await {
        let stream_files = result?;
        rows += stream_files
            .iter()
            .map(|f| f.file.meta.records)
            .sum::<i64>();
        built_files.extend(stream_files);
    }
    drop(small_results);
    for plan in large {
        let _admitted = BUILD_MEMORY_BUDGET
            .acquire(plan.decoded_bytes as u64, BudgetClass::Build)
            .await;
        let stream_files = build_stream_files(plan.group, &plan.key_parts).await?;
        rows += stream_files
            .iter()
            .map(|f| f.file.meta.records)
            .sum::<i64>();
        built_files.extend(stream_files);
    }
    let files: Vec<FileKey> = built_files.iter().map(|b| b.file.clone()).collect();

    // planned keys go durable BEFORE the first PUT (GC design): every whole
    // claimed row records every key this batch will upload, fenced by the
    // lease. A short count means the lease is gone — discard with nothing
    // uploaded, because an object PUT without a plan row would be orphaned
    // with no record naming it (per-boot node uuids mean no retry would ever
    // find it). The commit below clears the marker for the flipped rows.
    if !built_files.is_empty() {
        // M29: tombstoned rows are Built now — they no longer match the
        // fenced plan write, so counting them would misread every mixed
        // batch (real + gone) as a lost lease and discard the build
        let gone_set: std::collections::HashSet<i64> = gone.iter().copied().collect();
        let claim_ids: Vec<i64> = claim
            .iter()
            .map(|m| m.id)
            .filter(|id| !gone_set.contains(id))
            .collect();
        let planned_keys: Vec<String> = files.iter().map(|f| f.key.clone()).collect();
        let planned = wal_segments::set_l0_planned(&claim_ids, node, &planned_keys)
            .await
            .with_context(|| format!("set_l0_planned for segment ids {claim_ids:?}"))?;
        if planned != claim_ids.len() as u64 {
            log::warn!(
                "[SEGMENT:BUILD] lease lost before upload: planned-keys write covered {planned} of {} \
                 claimed segments ({claim_ids:?}); build discarded, nothing uploaded — the lease \
                 winner plans and uploads its own exact-run keys",
                claim_ids.len(),
            );
            return Ok(BatchStats {
                claimed: claim.len(),
                built: built_ids.len(),
                skipped: skipped.len(),
                gone: tombstoned as usize,
                streams: stream_keys.len(),
                files: files.len(),
                rows,
                flipped: 0,
                took_ms: started.elapsed().as_millis(),
            });
        }
        // only now may objects leave this node; a failure aborts the batch
        // (nothing registered), and whatever was already PUT is named by the
        // rows' plans — the GC's job if this claim never finishes
        for built in built_files {
            upload_built_file(built).await?;
        }
    }

    // registration + fenced flip commit or roll back TOGETHER — the crash
    // window between them (the old double-count residual) no longer exists
    let mut flipped = 0;
    let mut committed = built_ids.is_empty();
    if !built_ids.is_empty() {
        flipped = wal_segments::mark_built_with_files(&built_ids, node, files.clone())
            .await
            .with_context(|| format!("mark_built_with_files for segment ids {built_ids:?}"))?;
        committed = flipped == built_ids.len() as u64;
        if !committed {
            log::warn!(
                "[SEGMENT:BUILD] lease lost: fenced commit flipped {flipped} of {} segments ({built_ids:?}); \
                 registration rolled back whole, the lease winner registers its own exact-run keys",
                built_ids.len(),
            );
        }
    }

    if committed {
        // compaction hint, mirroring the WAL mover: nudge the incremental
        // merge of each touched hour once files pile up (idempotent counter;
        // only committed registrations may hint)
        for file in &files {
            if let Some((org, stream_type, stream)) = parse_l0_stream(&file.key) {
                incr_pending_file(&org, stream_type, &stream, file.meta.min_ts).await;
            }
        }
    }

    Ok(BatchStats {
        claimed: claim.len(),
        built: built_ids.len(),
        skipped: skipped.len(),
        gone: tombstoned as usize,
        streams: stream_keys.len(),
        files: files.len(),
        rows,
        flipped,
        took_ms: started.elapsed().as_millis(),
    })
}

/// DECODED arrow bytes one stream chunk may feed into its build. The L0
/// sort runs inside the ingester's DataFusion pool (2048MB) and its
/// repartition + external sort together peak at ~3x the decoded input, so
/// chunks are capped on the size that actually matters — the compressed
/// `size` column under-measured traces ~10x and kept the OOM loop alive
/// (2026-07-31).
const BUILD_CHUNK_MAX_DECODED_BYTES: usize = 128 * 1024 * 1024;

/// A single stream-chunk build larger than this runs SERIALLY with the
/// whole pool to itself (oversized backlog segments must still build —
/// alone, not beside two siblings).
const BUILD_GROUP_MAX_DECODED_BYTES: usize = 160 * 1024 * 1024;

/// Concurrent small builds per claim (`ZO_SEGMENT_BUILD_CONCURRENCY`, M12
/// item 5 — was a hardcoded 3). Default 3 keeps today's behavior: 3 ×
/// (~128MB decoded input) resident batches multiply drain throughput while
/// staying modest on an 8Gi ingester; since M12's direct sorted-batch build
/// the DataFusion pool is not involved for logs/traces at all, so dedicated
/// builder/compactor pods can safely run higher (prod: ~370 seg/min
/// arrivals vs ~195/min fleet builds at 3-per-pod). Floor 1 (config clamp).
fn build_concurrency() -> usize {
    get_config().common.segment_build_concurrency.max(1)
}

/// One stream's identity plus its byte-capped chunks out of one contiguous
/// decoded run.
struct StreamChunks {
    org: String,
    stream_type: StreamType,
    stream: String,
    chunks: Vec<StreamChunk>,
}

/// A consecutive sub-range of a run's ids plus the stream's frames from
/// exactly those segments; `[start_id, end_id]` becomes the file's
/// provenance range.
struct StreamChunk {
    start_id: i64,
    end_id: i64,
    batches: Vec<RecordBatch>,
    decoded_bytes: usize,
}

/// Chunk one CONTIGUOUS decoded run per stream: each stream accumulates ITS
/// OWN frames in id order and closes a chunk (on whole-segment boundaries
/// only) as soon as adding the next segment's frames would exceed
/// [`BUILD_CHUNK_MAX_DECODED_BYTES`]; a single oversized segment still
/// builds alone — nothing is ever skipped. Every stream's chunk ranges tile
/// the run's WHOLE id span — the first chunk starts at the run's first id,
/// each successor starts right after its predecessor closes, the last ends
/// at the run's last id — so provenance ranges stay consecutive-and-covered
/// (a covered segment either contributed its rows for the stream or carried
/// none) while different streams cut the same run at different byte
/// boundaries; segments carrying no frames for a stream extend its open
/// chunk's range without adding bytes. Purely a function of the run's ids
/// and per-(segment, stream) decoded frame sizes, so re-claims of the same
/// decode set reproduce identical chunks and therefore identical keys.
///
/// The PER-STREAM cap is the point of this shape: capping the run's
/// aggregate bytes (the pre-2026-08-07 behavior) closed a sub-run for every
/// ~128MB of fleet-wide traffic and emitted one file for EVERY stream
/// present in it, so a stream at ~1% of traffic still got ~10k sliver L0
/// files/hour — per-file query arithmetic 10-30x'd the orbit services view
/// until compaction caught up hours later.
fn chunk_run_per_stream(run: Vec<(i64, Vec<SegmentFrame>)>) -> Vec<StreamChunks> {
    let Some(run_first) = run.first().map(|(id, _)| *id) else {
        return Vec::new();
    };
    let run_last = run.last().map(|(id, _)| *id).unwrap_or(run_first);

    struct Accum {
        org: String,
        stream_type: StreamType,
        stream: String,
        done: Vec<StreamChunk>,
        open_start: i64,
        open_batches: Vec<RecordBatch>,
        open_bytes: usize,
    }
    // BTreeMap keeps the emitted stream order deterministic
    let mut accums: BTreeMap<String, Accum> = BTreeMap::new();

    for (id, frames) in run {
        // group THIS segment's frames per stream first (preserving frame
        // order), so the close/open decision sees the segment's whole
        // contribution at once — chunks close on segment boundaries only
        let mut seg_order: Vec<String> = Vec::new();
        let mut seg_groups: HashMap<String, (Vec<SegmentFrame>, usize)> = HashMap::new();
        for frame in frames {
            let key = format!("{}/{}/{}", frame.org, frame.stream_type, frame.stream);
            let bytes = frame.batch.size();
            match seg_groups.get_mut(&key) {
                Some((group_frames, group_bytes)) => {
                    group_frames.push(frame);
                    *group_bytes = group_bytes.saturating_add(bytes);
                }
                None => {
                    seg_order.push(key.clone());
                    seg_groups.insert(key, (vec![frame], bytes));
                }
            }
        }
        for key in seg_order {
            let Some((group_frames, group_bytes)) = seg_groups.remove(&key) else {
                continue;
            };
            let first = &group_frames[0];
            let acc = accums.entry(key).or_insert_with(|| Accum {
                org: first.org.clone(),
                stream_type: first.stream_type,
                stream: first.stream.clone(),
                done: Vec::new(),
                open_start: run_first,
                open_batches: Vec::new(),
                open_bytes: 0,
            });
            if acc.open_bytes > 0
                && acc.open_bytes.saturating_add(group_bytes) > BUILD_CHUNK_MAX_DECODED_BYTES
            {
                acc.done.push(StreamChunk {
                    start_id: acc.open_start,
                    // ids inside a run are consecutive, so the previous
                    // segment is exactly id - 1
                    end_id: id - 1,
                    batches: std::mem::take(&mut acc.open_batches),
                    decoded_bytes: std::mem::replace(&mut acc.open_bytes, 0),
                });
                acc.open_start = id;
            }
            acc.open_batches
                .extend(group_frames.into_iter().map(|f| f.batch));
            acc.open_bytes = acc.open_bytes.saturating_add(group_bytes);
        }
    }

    accums
        .into_values()
        .map(|acc| {
            let Accum {
                org,
                stream_type,
                stream,
                mut done,
                open_start,
                open_batches,
                open_bytes,
            } = acc;
            // the open chunk is never empty here: an accumulator only exists
            // once frames arrived, and every close is immediately followed
            // by an extend
            if !open_batches.is_empty() {
                done.push(StreamChunk {
                    start_id: open_start,
                    end_id: run_last,
                    batches: open_batches,
                    decoded_bytes: open_bytes,
                });
            }
            StreamChunks {
                org,
                stream_type,
                stream,
                chunks: done,
            }
        })
        .filter(|s| !s.chunks.is_empty())
        .collect()
}

fn split_into_id_runs<T>(mut decoded: Vec<(i64, T)>) -> Vec<Vec<(i64, T)>> {
    decoded.sort_unstable_by_key(|(id, _)| *id);
    let mut runs: Vec<Vec<(i64, T)>> = Vec::new();
    for (id, payload) in decoded {
        match runs.last_mut() {
            Some(run)
                if run
                    .last()
                    .is_some_and(|(last, _)| last.checked_add(1) == Some(id)) =>
            {
                run.push((id, payload));
            }
            _ => runs.push(vec![(id, payload)]),
        }
    }
    runs
}

struct StreamGroup {
    org: String,
    stream_type: StreamType,
    stream: String,
    batches: Vec<RecordBatch>,
}

/// One L0 file, built but NOT yet uploaded: the registration row plus the
/// bytes (or the disk spool) to PUT. Payloads are held until every file of
/// the batch is built and the batch's planned keys are durably recorded in
/// `wal_segments.l0_planned` — only then do uploads start, so every object
/// that reaches the store is named either by the rows' plan (crashed build,
/// collected by the segment GC) or by `file_list` (committed build).
struct BuiltL0File {
    file: FileKey,
    /// In-memory container bytes; empty when `spooled` carries a disk path.
    buf: Vec<u8>,
    /// Disk-spooled container — its temp file deletes on drop, so it lives
    /// exactly until this file uploaded (or the batch failed).
    spooled: Option<core_writer::VixOutput>,
    /// The `.vxi` index-sidecar bytes (v3 split). `None` under the #42
    /// L0 index-off posture (`file.meta.index_size == 0`); uploaded right
    /// after the data object when present. The GC needs no extra planned
    /// key: collecting an orphan `.vix` also attempts its derived sidecar
    /// key (`compact::segments_sweep`).
    index: Option<Vec<u8>>,
}

/// PUT one built L0 file, consuming its payload. A failure aborts the whole
/// batch (nothing is registered yet); the planned keys stay on the claimed
/// rows, so even a crash after a partial upload leaves a durable record the
/// GC can act on.
async fn upload_built_file(built: BuiltL0File) -> Result<(), anyhow::Error> {
    let BuiltL0File {
        file,
        buf,
        spooled,
        index,
    } = built;
    match spooled.as_ref().and_then(|o| o.spool_path()) {
        Some(spool) => {
            storage::put_file(&file.account, &file.key, spool)
                .await
                .with_context(|| format!("upload spooled L0 file {}", file.key))?;
            // the NamedTempFile spool deletes when `spooled` drops
        }
        None => {
            storage::put(&file.account, &file.key, Bytes::from(buf))
                .await
                .with_context(|| format!("upload L0 file {}", file.key))?;
        }
    }
    // v3 split: the sidecar (when the build indexed) uploads AFTER the data
    // object; the file_list row commits later still, so a crash here leaves
    // only rowless orphans — the GC's derived-key delete collects them.
    if let Some(index) = index {
        let sidecar_key = config::vix_sidecar_key(&file.key)
            .expect("core L0 outputs are .vix keys by construction");
        storage::put(&file.account, &sidecar_key, Bytes::from(index))
            .await
            .with_context(|| format!("upload L0 index sidecar {sidecar_key}"))?;
    }
    log::info!(
        "[SEGMENT:BUILD] built L0 file {}, records: {}, original_size: {}, compressed_size: {}",
        file.key,
        file.meta.records,
        file.meta.original_size,
        file.meta.compressed_size,
    );
    Ok(())
}

/// Fetch and decode the claimed segments, at most
/// `ZO_SEGMENT_FETCH_DECODE_CONCURRENCY` in flight (`buffered` keeps claim
/// order, so results still zip with the claim). Returns
/// `(decoded, skipped, gone)`:
/// - `skipped` — transient failures (fetch error, decode error, path-unsafe
///   identity): left leased so the lease expires and the segment retries,
///   never crashing the batch and never contributing partial data;
/// - `gone` (M29) — the object GET returned NotFound: the S3 lifecycle (or an
///   earlier confirmed delete) removed the object, and S3 reads are strongly
///   consistent, so a retry can never succeed. The caller terminally resolves
///   these rows instead of recycling them through every future claim (the
///   kill-era zombie loop: claim -> 404 -> lease expiry -> re-claim, 722k
///   skips/30m fleet-wide on prod 2026-08-24, diluting every claim batch to
///   1-2 real segments and turning the batch-sized L0 design into per-stream
///   sliver files).
///
/// Log discipline (house rule): per-item detail at DEBUG only; the caller
/// emits one per-batch count line.
async fn fetch_and_decode(
    claim: &[SegmentMeta],
) -> (Vec<(i64, Vec<SegmentFrame>)>, Vec<i64>, Vec<i64>) {
    // futures are built eagerly (they are lazy and tiny); `buffered` polls
    // at most the configured concurrency of them at a time (floor 1
    // clamped at config load; memory scales with in-flight decoded
    // objects — see ZO_SEGMENT_FETCH_DECODE_CONCURRENCY's help)
    let pending: Vec<_> = claim.iter().map(fetch_and_decode_one).collect();
    let results: Vec<Result<Vec<SegmentFrame>, anyhow::Error>> = futures::stream::iter(pending)
        .buffered(get_config().common.segment_fetch_decode_concurrency)
        .collect()
        .await;

    let tombstone_404 = get_config().common.segment_build_404_tombstone;
    let mut decoded = Vec::with_capacity(claim.len());
    let mut skipped = Vec::new();
    let mut gone = Vec::new();
    for (meta, result) in claim.iter().zip(results) {
        match result {
            Ok(frames) => decoded.push((meta.id, frames)),
            Err(e) if tombstone_404 && infra::storage::is_not_found_error(&e) => {
                log::debug!(
                    "[SEGMENT:BUILD] segment id={} key={} object gone (404): terminally resolving: {e:#}",
                    meta.id,
                    meta.object_key
                );
                gone.push(meta.id);
            }
            Err(e) => {
                log::debug!(
                    "[SEGMENT:BUILD] segment id={} key={} skipped this round (lease will expire and retry): {e:#}",
                    meta.id,
                    meta.object_key
                );
                skipped.push(meta.id);
            }
        }
    }
    (decoded, skipped, gone)
}

/// Fetch and decode ONE segment object; any failure skips only this segment.
/// The fetch error keeps its typed source chain (`object_store::Error`
/// downcast) so the caller can classify NotFound precisely.
async fn fetch_and_decode_one(meta: &SegmentMeta) -> Result<Vec<SegmentFrame>, anyhow::Error> {
    let bytes = storage::get_bytes("", &meta.object_key)
        .await
        .map_err(|e| anyhow::Error::new(e).context("fetch failed"))?;
    // zstd + arrow ipc decode of up to ~32MB: keep it off the async workers
    let decoded = tokio::task::spawn_blocking(move || decode_segment(&bytes))
        .await
        .map_err(|e| anyhow!("decode task did not complete: {e}"))?
        .map_err(|e| anyhow!("decode failed: {e:#}"))?;
    let (_header, frames) = decoded;
    for frame in &frames {
        validate_stream_identity(&frame.org, &frame.stream)?;
    }
    Ok(frames)
}

/// Stream identities become object-key path components and `file_list`
/// stream keys; a separator or traversal token inside one would misfile the
/// rows under a different stream. Ingest sanitizes names, so hitting this
/// means corrupted or forged segment content — the segment is skipped like
/// any other decode failure.
fn validate_stream_identity(org: &str, stream: &str) -> Result<(), anyhow::Error> {
    for (what, value) in [("org", org), ("stream", stream)] {
        if value.is_empty() {
            return Err(anyhow!("frame has an empty {what}"));
        }
        if value.contains(['/', '\\']) || value == ".." {
            return Err(anyhow!(
                "frame {what} {value:?} contains a path separator or traversal token"
            ));
        }
    }
    Ok(())
}

/// The chunk-constant parts of every L0 key: writer uuid (or `multi`) and
/// the min/max DECODED segment id of one contiguous stream chunk (never the
/// claim's — a claim-derived range would falsely cover skipped or foreign
/// ids). Pure function of the chunk's members, so a re-claim that decodes
/// the same set reproduces identical keys.
#[derive(Debug, Clone, PartialEq)]
struct L0KeyParts {
    writer_uuid: String,
    min_id: i64,
    max_id: i64,
}

fn l0_key_parts(run: &[&SegmentMeta]) -> Result<L0KeyParts, anyhow::Error> {
    let mut ids = run.iter().map(|m| m.id);
    let first = ids
        .next()
        .ok_or_else(|| anyhow!("l0_key_parts: empty run"))?;
    let (min_id, max_id) = ids.fold((first, first), |(lo, hi), id| (lo.min(id), hi.max(id)));
    let uuids: BTreeSet<&str> = run.iter().map(|m| m.node_uuid.as_str()).collect();
    let writer_uuid = match uuids.iter().next() {
        Some(uuid) if uuids.len() == 1 => (*uuid).to_string(),
        _ => "multi".to_string(),
    };
    Ok(L0KeyParts {
        writer_uuid,
        min_id,
        max_id,
    })
}

/// Deterministic L0 object key:
/// `files/{org}/{type}/{stream}/{YYYY/MM/DD/HH}/l0_{uuid|multi}_{min}_{max}_{hour index}{ext}`.
/// The trailing hour index is hours-since-epoch — a pure function of the
/// hour itself, so it can never shift between retries the way a positional
/// bucket index would when a previously skipped segment adds an hour.
fn l0_object_key(
    org: &str,
    stream_type: StreamType,
    stream: &str,
    hour_start_micros: i64,
    parts: &L0KeyParts,
    extension: &str,
) -> Result<String, anyhow::Error> {
    let dt = DateTime::<Utc>::from_timestamp_micros(hour_start_micros).ok_or_else(|| {
        anyhow!(
            "{org}/{stream_type}/{stream}: hour start {hour_start_micros} is out of datetime range"
        )
    })?;
    let date_path = dt.format("%Y/%m/%d/%H");
    let hour_index = hour_start_micros.div_euclid(HOUR_MICROS);
    Ok(format!(
        "files/{org}/{stream_type}/{stream}/{date_path}/l0_{}_{}_{}_{hour_index}{extension}",
        parts.writer_uuid, parts.min_id, parts.max_id
    ))
}

/// Inverse of the key layout for the compaction hint: `(org, type, stream)`
/// out of `files/{org}/{type}/{stream}/...`.
fn parse_l0_stream(key: &str) -> Option<(String, StreamType, String)> {
    let mut parts = key.splitn(5, '/');
    let _files = parts.next()?;
    let org = parts.next()?;
    let stream_type = StreamType::from(parts.next()?);
    let stream = parts.next()?;
    parts.next()?; // date + file name must exist
    Some((org.to_string(), stream_type, stream.to_string()))
}

/// Build every (hour) L0 file for one stream: group the stream's batches by
/// their write-time schema, homogenize ONCE onto the union schema, drop
/// degenerate-`_timestamp` rows (counted + one loud WARN, mirroring the
/// mover's cleansing backstop), sort ascending, split into hourly buckets,
/// and build one file per bucket. NOTHING uploads here — the caller records
/// the planned keys first and uploads afterwards (the GC ordering).
async fn build_stream_files(
    group: StreamGroup,
    key_parts: &L0KeyParts,
) -> Result<Vec<BuiltL0File>, anyhow::Error> {
    let StreamGroup {
        org,
        stream_type,
        stream,
        batches,
    } = group;
    let ctx = format!("{org}/{stream_type}/{stream}");

    // group by per-batch schema BEFORE any concat (the 2026-07-30 mixed-type
    // lesson: one stream never means one schema)
    let mut schema_groups: Vec<(SchemaRef, Vec<RecordBatch>)> = Vec::new();
    let mut group_index: HashMap<String, usize> = HashMap::new();
    for batch in batches {
        let hash = batch.schema().hash_key();
        match group_index.get(&hash) {
            Some(&i) => schema_groups[i].1.push(batch),
            None => {
                group_index.insert(hash, schema_groups.len());
                schema_groups.push((batch.schema(), vec![batch]));
            }
        }
    }

    let union = union_schema(
        &schema_groups
            .iter()
            .map(|(s, _)| Arc::clone(s))
            .collect::<Vec<_>>(),
    );
    if union.field_with_name(TIMESTAMP_COL_NAME).is_err() {
        return Err(anyhow!(
            "{ctx}: segment frames carry no {TIMESTAMP_COL_NAME} column; refusing to build an unbucketable L0 file"
        ));
    }

    // homogenize each schema group once, then concat to one batch
    let mut homogenized = Vec::with_capacity(schema_groups.len());
    for (schema, group_batches) in schema_groups {
        let merged = concat_batches(&schema, group_batches.iter())
            .with_context(|| format!("{ctx}: concat same-schema batches"))?;
        if merged.num_rows() == 0 {
            continue;
        }
        homogenized.push(homogenize_batch(&merged, &union, &ctx)?);
    }
    if homogenized.is_empty() {
        return Ok(Vec::new());
    }
    let merged = concat_batches(&union, homogenized.iter())
        .with_context(|| format!("{ctx}: concat homogenized batches"))?;

    let (merged, dropped) = filter_degenerate_ts(merged, &ctx)?;
    if dropped > 0 {
        metrics::COMPACT_DROPPED_ZERO_TS_ROWS
            .with_label_values(&[&org, stream_type.as_str(), &stream])
            .inc_by(dropped);
        log::warn!(
            "[SEGMENT:BUILD] {ctx}: dropped {dropped} rows with a degenerate _timestamp <= 0 \
             (backstop; ingest canonicalization mints none)"
        );
    }
    if merged.num_rows() == 0 {
        log::error!(
            "[SEGMENT:BUILD] {ctx}: every row carried a degenerate _timestamp ({dropped} dropped); \
             no L0 file produced for this stream"
        );
        return Ok(Vec::new());
    }

    // M12: sort DESCENDING — the stored v2 row order — so each hourly
    // bucket feeds `write_core_file_from_sorted_batch` directly and NO
    // DataFusion sort (the prod pool-starvation shape: RepartitionExec +
    // two ExternalSorters under one SortPreservingMergeExec) runs at all.
    let sorted = sort_record_batch_by_column(merged, TIMESTAMP_COL_NAME, true, None)
        .map_err(|e| anyhow!("{ctx}: sort by {TIMESTAMP_COL_NAME} failed: {e}"))?;
    // buckets in ascending-hour order regardless of the row sort direction
    // (files/planned keys keep their pre-M12 order; rows inside stay DESC)
    let mut buckets = split_by_hour(&sorted, &ctx)?;
    buckets.sort_unstable_by_key(|(hour, _)| *hour);

    // stream settings drive the same fts/bloom/column-store/original wiring
    // the mover uses
    let stream_settings = infra::schema::get_settings(&org, &stream, stream_type).await;
    let mut files = Vec::with_capacity(buckets.len());
    for (hour_start, bucket) in buckets {
        files.push(
            build_one_file(
                &org,
                stream_type,
                &stream,
                &union,
                bucket,
                hour_start,
                key_parts,
                &stream_settings,
            )
            .await?,
        );
    }
    Ok(files)
}

/// Union schema across a stream's per-batch schemas. Field-name conflicts
/// widen with the same precedence as the schema-evolution path
/// (`infra::schema::is_widening_conversion`; when neither direction widens,
/// Utf8 wins). A field missing from any group, nullable anywhere, or whose
/// type had to change becomes nullable (missing columns are null-padded and
/// lossy casts may null). `_timestamp` is pinned to Int64 — it is the
/// engine's bucketing and sort column; a group that stored it as anything
/// else gets cast, and unparseable values fall to the degenerate-row filter.
fn union_schema(schemas: &[SchemaRef]) -> SchemaRef {
    struct UnionField {
        data_type: DataType,
        nullable: bool,
        seen: usize,
    }
    let mut fields: BTreeMap<String, UnionField> = BTreeMap::new();
    for schema in schemas {
        for field in schema.fields() {
            match fields.get_mut(field.name()) {
                None => {
                    fields.insert(
                        field.name().clone(),
                        UnionField {
                            data_type: field.data_type().clone(),
                            nullable: field.is_nullable(),
                            seen: 1,
                        },
                    );
                }
                Some(entry) => {
                    entry.seen += 1;
                    entry.nullable = entry.nullable || field.is_nullable();
                    if &entry.data_type != field.data_type() {
                        entry.data_type = widen_type(&entry.data_type, field.data_type());
                        entry.nullable = true;
                    }
                }
            }
        }
    }
    let total = schemas.len();
    let fields: Vec<Field> = fields
        .into_iter()
        .map(|(name, mut entry)| {
            if entry.seen < total {
                entry.nullable = true;
            }
            if name == TIMESTAMP_COL_NAME && entry.data_type != DataType::Int64 {
                entry.data_type = DataType::Int64;
                entry.nullable = true;
            }
            Field::new(name, entry.data_type, entry.nullable)
        })
        .collect();
    Arc::new(Schema::new(fields))
}

/// Widen `a`/`b` with the schema-evolution precedence; Utf8 when neither
/// direction is a widening conversion.
fn widen_type(a: &DataType, b: &DataType) -> DataType {
    if a == b {
        a.clone()
    } else if infra::schema::is_widening_conversion(a, b) {
        b.clone()
    } else if infra::schema::is_widening_conversion(b, a) {
        a.clone()
    } else {
        DataType::Utf8
    }
}

/// Cast one batch onto the union schema: per-column `arrow::compute::cast`
/// for type changes, `new_null_array` for columns the batch never carried.
fn homogenize_batch(
    batch: &RecordBatch,
    union: &SchemaRef,
    ctx: &str,
) -> Result<RecordBatch, anyhow::Error> {
    let rows = batch.num_rows();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(union.fields().len());
    for field in union.fields() {
        let column = match batch.column_by_name(field.name()) {
            None => new_null_array(field.data_type(), rows),
            Some(col) if col.data_type() == field.data_type() => Arc::clone(col),
            Some(col) => cast(col, field.data_type()).with_context(|| {
                format!(
                    "{ctx}: cast field {:?} from {} to {}",
                    field.name(),
                    col.data_type(),
                    field.data_type()
                )
            })?,
        };
        columns.push(column);
    }
    RecordBatch::try_new(Arc::clone(union), columns)
        .with_context(|| format!("{ctx}: assemble homogenized batch"))
}

/// Drop rows whose `_timestamp` is null or <= 0 BEFORE bucketing: such rows
/// would otherwise mint 1970-dated hour buckets (and object keys), and the
/// vix writer would drop them later anyway. Returns the kept rows and the
/// dropped count.
fn filter_degenerate_ts(
    batch: RecordBatch,
    ctx: &str,
) -> Result<(RecordBatch, u64), anyhow::Error> {
    let ts = timestamp_column(&batch, ctx)?;
    let mask: BooleanArray = ts
        .iter()
        .map(|v| Some(matches!(v, Some(x) if x > 0)))
        .collect();
    let dropped = (batch.num_rows() - mask.true_count()) as u64;
    if dropped == 0 {
        return Ok((batch, 0));
    }
    let kept = filter_record_batch(&batch, &mask)
        .with_context(|| format!("{ctx}: filter degenerate _timestamp rows"))?;
    Ok((kept, dropped))
}

fn timestamp_column<'a>(
    batch: &'a RecordBatch,
    ctx: &str,
) -> Result<&'a Int64Array, anyhow::Error> {
    batch
        .column_by_name(TIMESTAMP_COL_NAME)
        .ok_or_else(|| anyhow!("{ctx}: batch is missing {TIMESTAMP_COL_NAME}"))?
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow!("{ctx}: {TIMESTAMP_COL_NAME} is not Int64 after homogenization"))
}

/// Split a `_timestamp`-sorted batch (either direction — boundaries are
/// detected on consecutive-row hour changes, and a sorted input keeps each
/// hour contiguous) into per-hour zero-copy slices, keyed by the hour's
/// start micros. The M12 builder sorts DESCENDING (the stored v2 row
/// order), so buckets come out newest-hour-first with rows DESC inside.
/// Input rows are non-null and > 0 (the degenerate filter ran first).
fn split_by_hour(
    sorted: &RecordBatch,
    ctx: &str,
) -> Result<Vec<(i64, RecordBatch)>, anyhow::Error> {
    let rows = sorted.num_rows();
    if rows == 0 {
        return Ok(Vec::new());
    }
    let ts = timestamp_column(sorted, ctx)?;
    let values = ts.values();
    let mut buckets = Vec::new();
    let mut start_idx = 0usize;
    let mut current_hour = values[0].div_euclid(HOUR_MICROS);
    for (i, v) in values.iter().enumerate().skip(1) {
        let hour = v.div_euclid(HOUR_MICROS);
        if hour != current_hour {
            buckets.push((
                current_hour * HOUR_MICROS,
                sorted.slice(start_idx, i - start_idx),
            ));
            start_idx = i;
            current_hour = hour;
        }
    }
    buckets.push((
        current_hour * HOUR_MICROS,
        sorted.slice(start_idx, rows - start_idx),
    ));
    Ok(buckets)
}

/// M18 item 3: slice-accurate arrow memory footprint of one (possibly
/// zero-copy sliced) record batch. `RecordBatch::get_array_memory_size` on
/// an hour SLICE reports the FULL backing buffers of the decoded run, so
/// per-hour L0 buckets stamped wildly inflated `original_size` values (see
/// the call site). Sums each column's slice-window bytes
/// (`ArrayData::get_slice_memory_size`); if arrow cannot size a column
/// per-slice (no segment column type today), the whole batch prorates the
/// run's backing footprint by its row share — the backing row count read
/// off the always-present non-null Int64 `_timestamp` buffer.
fn sliced_batch_memory_size(batch: &RecordBatch, ctx: &str) -> usize {
    let mut exact = 0usize;
    for column in batch.columns() {
        match column.to_data().get_slice_memory_size() {
            Ok(bytes) => exact += bytes,
            Err(error) => {
                log::debug!(
                    "[SEGMENT:BUILD] {ctx}: column type {} has no per-slice memory size \
                     ({error}); prorating the run footprint by rows",
                    column.data_type()
                );
                let full = batch.get_array_memory_size();
                let backing_rows = batch
                    .column_by_name(TIMESTAMP_COL_NAME)
                    .and_then(|ts| {
                        ts.to_data()
                            .buffers()
                            .first()
                            .map(|buffer| buffer.len() / std::mem::size_of::<i64>())
                    })
                    .unwrap_or(0)
                    .max(batch.num_rows())
                    .max(1);
                return ((full as u128 * batch.num_rows() as u128) / backing_rows as u128)
                    as usize;
            }
        }
    }
    exact
}

/// Build ONE L0 file for a (stream, hour) bucket through the WAL mover's
/// exact single-file builds: `write_core_file_from_tables` (one
/// fully-indexed `.vix`) for logs/traces, `merge_parquet_files` for every
/// other stream type — so compaction and the query path see files
/// indistinguishable from moved WAL output. The result carries the payload;
/// the caller uploads it only after the batch's planned keys are durable.
#[allow(clippy::too_many_arguments)]
async fn build_one_file(
    org: &str,
    stream_type: StreamType,
    stream: &str,
    union: &SchemaRef,
    bucket: RecordBatch,
    hour_start: i64,
    key_parts: &L0KeyParts,
    stream_settings: &Option<config::meta::stream::StreamSettings>,
) -> Result<BuiltL0File, anyhow::Error> {
    let ctx = format!("{org}/{stream_type}/{stream}");
    let rows = bucket.num_rows();
    if rows == 0 {
        // split_by_hour never emits empty slices; guard future callers
        return Err(anyhow!("{ctx}: empty bucket for hour {hour_start}"));
    }
    let ts = timestamp_column(&bucket, &ctx)?;
    // descending sort (M12): first/last row give the data range
    let (min_ts, max_ts) = (ts.value(rows - 1), ts.value(0));
    // original_size basis: the rows' arrow memory footprint — segments carry
    // no upstream JSON size, and this feeds the same spool/merge thresholds.
    // M18: SLICE-accurate — `bucket` is a zero-copy hour slice of the whole
    // decoded run, and `get_array_memory_size` on a slice reports the FULL
    // backing buffers (a verified prod L0 file: 1 record, ~400 KB stored,
    // original_size stamped 201,757,975 ≈ the run's footprint). Inflated
    // sizes under-fill gen-1 merge groups fleet-wide (packing is by
    // original_size), so size the slice window itself.
    let input_bytes = sliced_batch_memory_size(&bucket, &ctx);
    let input_meta = FileMeta {
        min_ts,
        max_ts,
        records: rows as i64,
        original_size: input_bytes as i64,
        ..Default::default()
    };

    let bloom_fields = get_stream_setting_bloom_filter_fields(stream_settings);
    let fts_fields = get_stream_setting_fts_fields(stream_settings);
    let store_original = stream_settings
        .as_ref()
        .is_some_and(|settings| settings.store_original_data);

    let trace_id = ider::generate();
    // Metrics join the core-file path only behind the activation switch
    // (#40, ZO_VIX_METRICS_CORE_FILE_ENABLED) and write COLUMN-STORE-ONLY
    // files (ZO_VIX_INDEX_DISABLED_STREAM_TYPES).
    let use_core_file = matches!(stream_type, StreamType::Logs | StreamType::Traces)
        || (stream_type == StreamType::Metrics
            && get_config().common.vix_metrics_core_file_enabled);
    let (buf, spooled_output, index_bytes, file_meta, file_format) = if use_core_file {
        // M12: the bucket is already sorted `_timestamp` DESC (the stored
        // row order) and covers exactly this hour — the direct builder
        // needs no DataFusion plan, no repartition and no sort. The prod
        // 6 GB greedy-pool starvation (RepartitionExec buffering 3 GB it
        // cannot spill + two ExternalSorters, the second failing its FIRST
        // 122.8 MB allocation with 13.8 MB left) is unreachable here by
        // construction.
        let result = core_writer::write_core_file_from_sorted_batch(
            &trace_id,
            stream_type,
            bucket,
            &fts_fields,
            &bloom_fields,
            store_original,
            input_bytes,
        )
        .await
        .with_context(|| format!("{ctx}: build L0 .vix for hour {hour_start}"))?;
        let mut file_meta = input_meta;
        if result.dropped_rows > 0 {
            // unreachable in practice (the degenerate filter ran first);
            // mirror the mover's accounting if the writer ever disagrees
            metrics::COMPACT_DROPPED_ZERO_TS_ROWS
                .with_label_values(&[org, stream_type.as_str(), stream])
                .inc_by(result.dropped_rows);
            log::warn!(
                "[SEGMENT:BUILD] {ctx}: writer dropped {} degenerate rows AFTER the builder's own filter",
                result.dropped_rows
            );
            file_meta.records -= result.dropped_rows as i64;
        }
        if result.stats.row_count == 0 {
            return Err(anyhow!(
                "{ctx}: L0 build for hour {hour_start} stored zero of {rows} rows; refusing to register an empty file"
            ));
        }
        let compressed_len = result
            .output
            .as_ref()
            .map(|o| o.len() as usize)
            .unwrap_or(result.data.len());
        core_writer::apply_core_stats_to_meta(
            &mut file_meta,
            compressed_len,
            &result.stats,
            &format!("[SEGMENT:BUILD] {ctx}"),
        )?;
        (result.data, result.output, result.index, file_meta, FileFormat::Vix)
    } else {
        // non-core formats (metadata, index streams, metrics under the #40
        // default) keep the DataFusion merge — planned SINGLE-PARTITION
        // (M13): at prod volume default/metadata/trace_list_index buckets
        // are NOT thin, and the 2-partition `ORDER BY ... DESC` plan was the
        // fleet's last remaining "Not enough memory to continue external
        // sort" source post-.108. One bounded in-memory bucket needs no
        // parallel sort, and a single ExternalSorter spills properly (the
        // M12 fix-1 rationale); the M12 halving backoff stays the backstop.
        let hour_end = hour_start.saturating_add(HOUR_MICROS);
        let table = NewMemTable::try_new(
            Arc::clone(union),
            vec![vec![bucket]],
            Arc::clone(union),
            false,
            None,
            vec![],
            (hour_start, hour_end),
        )
        .map_err(|e| anyhow!("{ctx}: create memtable for hour {hour_start}: {e}"))?;
        let tables = vec![Arc::new(table) as _];
        let result = merge_parquet_files(
            stream_type,
            stream,
            Arc::clone(union),
            tables,
            &bloom_fields,
            input_meta,
            true,
            true, // M13: single-partition sort (see the note above)
        )
        .await
        .with_context(|| format!("{ctx}: build L0 file for hour {hour_start}"))?;
        match result {
            MergeParquetResult::Single {
                buf,
                file_meta,
                file_format,
            } => (buf, None, None, file_meta, file_format),
            MergeParquetResult::Multiple { .. } => {
                return Err(anyhow!(
                    "{ctx}: single-file L0 build unexpectedly returned multiple files"
                ));
            }
        }
    };

    if file_meta.compressed_size == 0 {
        return Err(anyhow!(
            "{ctx}: L0 build for hour {hour_start} produced compressed_size 0; refusing to register"
        ));
    }

    let key = l0_object_key(
        org,
        stream_type,
        stream,
        hour_start,
        key_parts,
        file_format.extension(),
    )?;
    let account = storage::get_account(org, &key).unwrap_or_default();
    Ok(BuiltL0File {
        file: FileKey::new(0, account, key, file_meta, false),
        buf,
        spooled: spooled_output,
        index: index_bytes,
    })
}

#[cfg(test)]
mod tests {
    use arrow::array::{Int64Array, StringArray};
    use config::{meta::stream::FileMeta, utils::parquet::parse_file_key_columns};
    use infra::wal_segments::SegmentStatus;
    use segment_wal::{SegmentHeader, encode_segment};

    use super::*;

    const T0: i64 = 1_609_459_200_000_000; // 2021-01-01T00:00:00Z micros

    fn seg_meta(id: i64, node: &str, seq: i64) -> SegmentMeta {
        SegmentMeta {
            id,
            node_uuid: node.to_string(),
            seq,
            object_key: format!("wal_segments/{node}/{seq:020}"),
            min_ts: T0,
            max_ts: T0 + 1000,
            size: 1024,
            streams: vec!["org1/logs/app1".to_string()],
            status: SegmentStatus::Building,
            builder_node: String::new(),
            created_at: T0,
            updated_at: T0,
        }
    }

    fn schema_of(fields: Vec<Field>) -> SchemaRef {
        Arc::new(Schema::new(fields))
    }

    fn ts_field() -> Field {
        Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false)
    }

    // ── M17 item 3: byte-budgeted build admission ────────────────────────

    /// Admission math units: fits admit immediately; an over-budget request
    /// WAITS until a release; the always-one floor admits an oversized
    /// reservation when its class is idle (claim and build floors are
    /// independent — a fat claim must not starve the first build);
    /// resize corrects the accounting in both directions and a shrink
    /// wakes waiters.
    #[tokio::test]
    async fn m17_budget_admission_math() {
        let budget = BuildMemoryBudget::new(1000);

        // fits: claim + build admit immediately
        let claim = budget.acquire(400, BudgetClass::Claim).await;
        let build_a = budget.acquire(500, BudgetClass::Build).await;
        assert_eq!(budget.used(), 900);

        // waits: a second build over budget stays pending...
        let pending = budget.acquire(200, BudgetClass::Build);
        let pending = tokio::time::timeout(Duration::from_millis(50), pending).await;
        assert!(pending.is_err(), "over-budget build must wait");
        // ...until a release frees room
        drop(build_a);
        let build_b = tokio::time::timeout(
            Duration::from_millis(1000),
            budget.acquire(200, BudgetClass::Build),
        )
        .await
        .expect("released bytes must admit the waiter");
        assert_eq!(budget.used(), 600);
        drop(build_b);
        drop(claim);
        assert_eq!(budget.used(), 0);

        // always-one floors: an oversized claim admits alone; the FIRST
        // build admits even while the claim holds more than the budget
        let fat_claim = budget.acquire(5000, BudgetClass::Claim).await;
        assert_eq!(budget.used(), 5000);
        let first_build = tokio::time::timeout(
            Duration::from_millis(1000),
            budget.acquire(800, BudgetClass::Build),
        )
        .await
        .expect("the first build always admits (class floor)");
        // the SECOND build must wait (class active, over budget)
        let second = tokio::time::timeout(
            Duration::from_millis(50),
            budget.acquire(10, BudgetClass::Build),
        )
        .await;
        assert!(second.is_err(), "a second build over budget must wait");
        drop(first_build);

        // resize: correcting the fat estimate down wakes waiters
        let mut fat_claim = fat_claim;
        fat_claim.resize(100);
        assert_eq!(budget.used(), 100);
        let build = tokio::time::timeout(
            Duration::from_millis(1000),
            budget.acquire(700, BudgetClass::Build),
        )
        .await
        .expect("the corrected reservation must admit the build");
        assert_eq!(budget.used(), 800);
        // resize can also grow (actual above estimate): unconditional
        fat_claim.resize(400);
        assert_eq!(budget.used(), 1100);
        drop(build);
        drop(fat_claim);
        assert_eq!(budget.used(), 0);
    }

    /// EMA correction: seeded at 5.0; observations move it by α = 0.2 and
    /// clamp to [1, 64]; zero-compressed observations are ignored.
    #[test]
    fn m17_decode_inflation_ema_correction() {
        // NOTE: the EMA is process-global; this is the only test touching it.
        assert!((decode_inflation_ema() - 5.0).abs() < 1e-9, "seed 5.0");
        observe_decode_inflation(100, 300); // observed 3.0
        let after = decode_inflation_ema();
        assert!(
            (after - 4.6).abs() < 1e-9,
            "5.0*0.8 + 3.0*0.2 = 4.6, got {after}"
        );
        observe_decode_inflation(0, 12345); // ignored
        assert!((decode_inflation_ema() - after).abs() < 1e-9);
        observe_decode_inflation(1, 1_000_000); // clamps to 64
        let clamped = decode_inflation_ema();
        assert!(
            (clamped - (after * 0.8 + 64.0 * 0.2)).abs() < 1e-9,
            "observation clamps at 64, got {clamped}"
        );
        observe_decode_inflation(1000, 500); // ratios below 1 clamp to 1
        let floored = decode_inflation_ema();
        assert!(
            (floored - (clamped * 0.8 + 1.0 * 0.2)).abs() < 1e-9,
            "observation floors at 1, got {floored}"
        );
    }

    /// The fat-shaped multi-build pin: ten 100-byte builds through the
    /// buffered(16) execution shape at a 250-byte budget — every build
    /// completes, at most two ever hold reservations concurrently (the
    /// budget binds, not the count cap), and real overlap happened.
    #[tokio::test]
    async fn m17_fat_multibuild_bounded_concurrent_bytes() {
        use std::sync::atomic::AtomicU64;
        let budget = Arc::new(BuildMemoryBudget::new(250));
        let max_used = Arc::new(AtomicU64::new(0));
        let overlapped = Arc::new(AtomicU64::new(0));
        let plans: Vec<u64> = vec![100; 10];
        let mut results = futures::stream::iter(plans.into_iter().map(|bytes| {
            let budget = Arc::clone(&budget);
            let max_used = Arc::clone(&max_used);
            let overlapped = Arc::clone(&overlapped);
            async move {
                let _admitted = budget.acquire(bytes, BudgetClass::Build).await;
                let used = budget.used();
                max_used.fetch_max(used, Ordering::Relaxed);
                if used > bytes {
                    overlapped.fetch_add(1, Ordering::Relaxed);
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
                Ok::<u64, anyhow::Error>(bytes)
            }
        }))
        .buffered(16);
        let mut completed = 0usize;
        while let Some(result) = results.next().await {
            result.unwrap();
            completed += 1;
        }
        assert_eq!(completed, 10, "every build completes under the budget");
        let max = max_used.load(Ordering::Relaxed);
        assert!(
            max <= 250,
            "concurrent reserved decoded bytes must stay under the budget, saw {max}"
        );
        assert!(
            overlapped.load(Ordering::Relaxed) > 0,
            "the budget admits real overlap (2 x 100 <= 250)"
        );
        assert_eq!(budget.used(), 0, "everything released");

        // an oversized singleton (> budget) still completes, alone
        let fat = tokio::time::timeout(
            Duration::from_millis(1000),
            budget.acquire(400, BudgetClass::Build),
        )
        .await
        .expect("the always-one floor admits an oversized build");
        assert_eq!(budget.used(), 400);
        drop(fat);
    }

    // ── schema homogenization ────────────────────────────────────────────

    #[test]
    fn test_union_schema_widens_type_flips_and_null_pads() {
        // batch 1: value:Int64, a:Utf8 — batch 2: value:Utf8, b:Int64
        let s1 = schema_of(vec![
            ts_field(),
            Field::new("value", DataType::Int64, false),
            Field::new("a", DataType::Utf8, false),
        ]);
        let s2 = schema_of(vec![
            ts_field(),
            Field::new("value", DataType::Utf8, false),
            Field::new("b", DataType::Int64, false),
        ]);
        let union = union_schema(&[Arc::clone(&s1), Arc::clone(&s2)]);

        // conflicting Int64/Utf8 widens to Utf8 (nullable: type flipped)
        let value = union.field_with_name("value").unwrap();
        assert_eq!(value.data_type(), &DataType::Utf8);
        assert!(value.is_nullable());
        // fields missing from one group become nullable
        assert!(union.field_with_name("a").unwrap().is_nullable());
        assert!(union.field_with_name("b").unwrap().is_nullable());
        // present-everywhere, same-type, non-nullable stays non-nullable
        let ts = union.field_with_name(TIMESTAMP_COL_NAME).unwrap();
        assert_eq!(ts.data_type(), &DataType::Int64);
        assert!(!ts.is_nullable());

        let b1 = RecordBatch::try_new(
            s1,
            vec![
                Arc::new(Int64Array::from(vec![T0 + 1, T0 + 2])),
                Arc::new(Int64Array::from(vec![7, 8])),
                Arc::new(StringArray::from(vec!["x", "y"])),
            ],
        )
        .unwrap();
        let b2 = RecordBatch::try_new(
            s2,
            vec![
                Arc::new(Int64Array::from(vec![T0 + 3])),
                Arc::new(StringArray::from(vec!["nine"])),
                Arc::new(Int64Array::from(vec![42])),
            ],
        )
        .unwrap();

        let h1 = homogenize_batch(&b1, &union, "test").unwrap();
        let h2 = homogenize_batch(&b2, &union, "test").unwrap();
        assert_eq!(h1.schema(), union);
        assert_eq!(h2.schema(), union);

        // Int64 values survive widening as their string forms
        let v1 = h1
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(v1.value(0), "7");
        assert_eq!(v1.value(1), "8");
        let v2 = h2
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(v2.value(0), "nine");

        // null padding for fields the batch never carried
        assert_eq!(h1.column_by_name("b").unwrap().null_count(), 2);
        assert_eq!(h2.column_by_name("a").unwrap().null_count(), 1);
        // values preserved where the field exists
        let a1 = h1
            .column_by_name("a")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(a1.value(0), "x");

        // both homogenized batches concat cleanly under the union schema
        let merged = concat_batches(&union, [&h1, &h2]).unwrap();
        assert_eq!(merged.num_rows(), 3);
    }

    #[test]
    fn test_union_schema_pins_timestamp_to_int64() {
        let s1 = schema_of(vec![Field::new(TIMESTAMP_COL_NAME, DataType::Utf8, false)]);
        let s2 = schema_of(vec![ts_field()]);
        let union = union_schema(&[Arc::clone(&s1), s2]);
        let ts = union.field_with_name(TIMESTAMP_COL_NAME).unwrap();
        assert_eq!(ts.data_type(), &DataType::Int64);
        assert!(ts.is_nullable(), "lossy cast target must be nullable");

        // numeric strings cast through; garbage nulls out and is then
        // dropped by the degenerate filter — never a panic
        let b = RecordBatch::try_new(
            s1,
            vec![Arc::new(StringArray::from(vec![
                "1609459200000001",
                "not-a-ts",
            ]))],
        )
        .unwrap();
        let h = homogenize_batch(&b, &union, "test").unwrap();
        let (kept, dropped) = filter_degenerate_ts(h, "test").unwrap();
        assert_eq!(dropped, 1);
        assert_eq!(kept.num_rows(), 1);
        let ts = timestamp_column(&kept, "test").unwrap();
        assert_eq!(ts.value(0), 1_609_459_200_000_001);
    }

    #[test]
    fn test_widen_type_precedence() {
        // widening directions follow the schema-evolution table
        assert_eq!(
            widen_type(&DataType::Int64, &DataType::Float64),
            DataType::Float64
        );
        assert_eq!(
            widen_type(&DataType::Float64, &DataType::Int64),
            DataType::Float64
        );
        assert_eq!(
            widen_type(&DataType::Boolean, &DataType::Int64),
            DataType::Int64
        );
        assert_eq!(
            widen_type(&DataType::Int64, &DataType::Utf8),
            DataType::Utf8
        );
        assert_eq!(widen_type(&DataType::Utf8, &DataType::Utf8), DataType::Utf8);
        // neither direction widens → Utf8 wins
        assert_eq!(
            widen_type(&DataType::Date32, &DataType::Boolean),
            DataType::Utf8
        );
    }

    // ── degenerate rows ──────────────────────────────────────────────────

    #[test]
    fn test_filter_degenerate_ts_drops_null_zero_negative() {
        let schema = schema_of(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, true),
            Field::new("v", DataType::Utf8, true),
        ]);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![
                    Some(T0 + 5),
                    None,
                    Some(0),
                    Some(-3),
                    Some(T0 + 9),
                ])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
            ],
        )
        .unwrap();
        let (kept, dropped) = filter_degenerate_ts(batch, "test").unwrap();
        assert_eq!(dropped, 3);
        assert_eq!(kept.num_rows(), 2);
        let v = kept
            .column_by_name("v")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(v.value(0), "a");
        assert_eq!(v.value(1), "e");
    }

    // ── hour splitting ───────────────────────────────────────────────────

    #[test]
    fn test_split_by_hour_across_boundary() {
        let hour11 = T0 + 11 * HOUR_MICROS;
        let schema = schema_of(vec![ts_field(), Field::new("v", DataType::Int64, false)]);
        // rows straddling the 10:00→11:00 boundary, plus one 3 hours later
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![
                    hour11 - 2, // 10:59:59.999998
                    hour11 - 1, // 10:59:59.999999
                    hour11,     // 11:00:00.000000 — new bucket
                    hour11 + 42,
                    hour11 + 3 * HOUR_MICROS, // 14:00 — third bucket
                ])),
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            ],
        )
        .unwrap();
        let buckets = split_by_hour(&batch, "test").unwrap();
        assert_eq!(buckets.len(), 3);
        assert_eq!(buckets[0].0, hour11 - HOUR_MICROS);
        assert_eq!(buckets[0].1.num_rows(), 2);
        assert_eq!(buckets[1].0, hour11);
        assert_eq!(buckets[1].1.num_rows(), 2);
        assert_eq!(buckets[2].0, hour11 + 3 * HOUR_MICROS);
        assert_eq!(buckets[2].1.num_rows(), 1);
        // slices carry the right rows
        let v0 = buckets[0]
            .1
            .column_by_name("v")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(v0.values(), &[1, 2]);

        // a single-hour batch yields exactly one bucket
        let one = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![hour11 + 1, hour11 + 2])),
                Arc::new(Int64Array::from(vec![1, 2])),
            ],
        )
        .unwrap();
        let buckets = split_by_hour(&one, "test").unwrap();
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].0, hour11);

        // M12: the production sort direction is DESCENDING — hours come out
        // newest-first, rows DESC inside each slice, boundaries exact
        let desc = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![
                    hour11 + 3 * HOUR_MICROS, // 14:00 — first bucket
                    hour11 + 42,
                    hour11, // 11:00:00.000000 boundary row stays in hour 11
                    hour11 - 1,
                    hour11 - 2,
                ])),
                Arc::new(Int64Array::from(vec![5, 4, 3, 2, 1])),
            ],
        )
        .unwrap();
        let buckets = split_by_hour(&desc, "test").unwrap();
        assert_eq!(buckets.len(), 3);
        assert_eq!(buckets[0].0, hour11 + 3 * HOUR_MICROS);
        assert_eq!(buckets[0].1.num_rows(), 1);
        assert_eq!(buckets[1].0, hour11);
        assert_eq!(buckets[1].1.num_rows(), 2);
        assert_eq!(buckets[2].0, hour11 - HOUR_MICROS);
        assert_eq!(buckets[2].1.num_rows(), 2);
        let v2 = buckets[2]
            .1
            .column_by_name("v")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(v2.values(), &[2, 1], "rows stay DESC inside the slice");
    }

    /// M18 item 3 pin: `original_size` accounting must be SLICE-accurate.
    /// A 1-row hour slice of a wide multi-hour run reports ~its own bytes,
    /// not the run's full backing footprint (the prod bug: 1 record, ~400 KB
    /// stored, original_size stamped ≈201 MB — the whole decoded run — which
    /// under-filled gen-1 merge groups fleet-wide); an UNSLICED batch keeps
    /// (approximately) the old arrow-footprint basis.
    #[test]
    fn test_sliced_batch_memory_size_is_slice_accurate() {
        let hour11 = T0 + 11 * HOUR_MICROS;
        let rows = 4096usize;
        let schema = schema_of(vec![ts_field(), Field::new("v", DataType::Utf8, false)]);
        // one lonely row in hour 11, the fat rest in hour 12 — DESC order,
        // so the lonely row is LAST (the prod shape: a stray row on an hour
        // boundary of a big decoded run)
        let ts: Vec<i64> = (0..rows)
            .map(|i| {
                if i == rows - 1 {
                    hour11 + 42
                } else {
                    hour11 + HOUR_MICROS + (rows - i) as i64
                }
            })
            .collect();
        let fat: Vec<String> = (0..rows).map(|i| format!("{i:0>512}")).collect();
        let run = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ts)),
                Arc::new(StringArray::from(fat)),
            ],
        )
        .unwrap();
        let run_footprint = run.get_array_memory_size();
        assert!(
            run_footprint > 2 * 1024 * 1024,
            "the run must be MB-scale for the pin to mean anything ({run_footprint})"
        );

        let buckets = split_by_hour(&run, "test").unwrap();
        assert_eq!(buckets.len(), 2);
        let lonely = &buckets[1].1;
        assert_eq!(lonely.num_rows(), 1);
        // the zero-copy slice still REPORTS the full run through the old
        // basis — that inflation is the bug
        assert!(
            lonely.get_array_memory_size() >= run_footprint / 2,
            "precondition: arrow's whole-batch footprint on a slice reports the backing run"
        );
        let sliced = sliced_batch_memory_size(lonely, "test");
        assert!(
            sliced < 16 * 1024,
            "a 1-row slice must report ~KB-scale original_size, got {sliced}"
        );

        // full (unsliced) batch: ≈ the old value — the content bytes stay
        // within the arrow footprint and carry its bulk
        let whole = sliced_batch_memory_size(&run, "test");
        assert!(
            whole <= run_footprint,
            "slice-accurate size never exceeds the arrow footprint"
        );
        assert!(
            whole >= run_footprint / 2,
            "an unsliced batch keeps the footprint basis (content-bytes bulk): {whole} vs \
             {run_footprint}"
        );
    }

    // ── M12 fix 2: memory-backoff helpers ────────────────────────────────

    #[test]
    fn test_is_resources_exhausted_matches_pool_failures_only() {
        // the exact prod shape: a DataFusion pool failure wrapped in the
        // builder's per-stream/per-hour anyhow context chain
        let pool = datafusion::error::DataFusionError::ResourcesExhausted(
            "Failed to allocate additional 122.8 MB for ExternalSorter[0] with 0.0 B already \
             allocated for this reservation - 13.8 MB remain available for the total pool"
                .to_string(),
        );
        let wrapped = anyhow::Error::new(pool)
            .context("o/logs/aws_vpc_flow_logs: build L0 .vix for hour 1787054400000000");
        assert!(is_resources_exhausted(&wrapped));

        // non-memory failures must NOT trigger the halving retry
        assert!(!is_resources_exhausted(&anyhow!("storage PUT failed: 503")));
        assert!(!is_resources_exhausted(&anyhow::Error::new(
            datafusion::error::DataFusionError::Execution("plan error".to_string())
        )));
    }

    #[test]
    fn test_halve_for_retry_converges_and_releases_the_tail() {
        let mut claim: Vec<SegmentMeta> = (1..=160).map(|i| seg_meta(i, "n", i)).collect();
        let mut sizes = vec![claim.len()];
        let mut released_total = 0usize;
        while claim.len() > 1 {
            let dropped = halve_for_retry(&mut claim);
            // the dropped set is exactly the tail beyond the kept half
            assert_eq!(dropped.len() + claim.len(), sizes[sizes.len() - 1]);
            assert_eq!(dropped[0], claim.last().unwrap().id + 1);
            released_total += dropped.len();
            sizes.push(claim.len());
        }
        assert_eq!(sizes, vec![160, 80, 40, 20, 10, 5, 2, 1], "log2 convergence");
        assert_eq!(released_total, 159, "everything not kept was released");
        assert_eq!(claim[0].id, 1, "the floor attempt still holds a segment");
        // the floor never halves to zero
        let dropped = halve_for_retry(&mut claim);
        assert!(dropped.is_empty());
        assert_eq!(claim.len(), 1);
    }

    // ── M13 aging lane ────────────────────────────────────────────────────

    /// Engagement is a pure age comparison — engages exactly at the
    /// threshold, disengages below it, and 0 disables the lane entirely.
    #[test]
    fn test_age_lane_engaged_by_oldest_age_only() {
        let now = 1_700_000_000_000_000i64;
        let lane = 21_600u64; // 6h
        let old = now - 21_600 * 1_000_000; // exactly the threshold
        assert!(age_lane_engaged(old, now, lane), "at-threshold engages");
        assert!(age_lane_engaged(old - 1, now, lane), "older engages");
        assert!(
            !age_lane_engaged(old + 1_000_000, now, lane),
            "younger than the threshold stays disengaged"
        );
        assert!(!age_lane_engaged(old, now, 0), "lane_secs=0 disables");
        assert!(
            !age_lane_engaged(0, now, lane),
            "no claimable rows (oldest=0) never engages"
        );
    }

    /// The fixed-point cadence: ratio 0.25 fires every 4th engaged pass
    /// exactly, 1.0 every pass, 0 never — and non-reciprocal ratios keep
    /// the exact long-run rate.
    #[test]
    fn test_age_lane_fire_cadence() {
        let acc = AtomicU64::new(0);
        let fired: Vec<bool> = (0..12).map(|_| age_lane_fire(&acc, 0.25)).collect();
        assert_eq!(
            fired,
            vec![
                false, false, false, true, false, false, false, true, false, false, false, true
            ],
            "0.25 = every 4th engaged pass"
        );

        let acc = AtomicU64::new(0);
        assert!((0..5).all(|_| age_lane_fire(&acc, 1.0)), "1.0 = every pass");

        let acc = AtomicU64::new(0);
        assert!((0..5).all(|_| !age_lane_fire(&acc, 0.0)), "0 never fires");

        // long-run exactness for a non-reciprocal ratio: 0.3 fires 30/100
        let acc = AtomicU64::new(0);
        let fires = (0..100).filter(|_| age_lane_fire(&acc, 0.3)).count();
        assert_eq!(fires, 30);
    }

    /// STARVATION REGRESSION (the M13 pin): an aged backlog at BALANCED
    /// capacity — every round adds exactly as many fresh segments as one
    /// claim takes — is never drained by newest-first claiming (the prod
    /// 2026-08-18/19 shape: oldest cohort pending 15+ hours while the S3
    /// lifecycle walked toward its raw objects). With the aging lane at
    /// ratio 0.25 the oldest cohort is fully claimed within
    /// `cohort/batch * 4` rounds.
    #[tokio::test]
    async fn test_age_lane_starvation_regression() {
        let _guard = setup().await;
        // drain foreign claimable rows so ordering is deterministic (they
        // stay leased under a long lease for the whole test)
        wal_segments::claim_pending("m13-lane-drain", 100_000, 3600)
            .await
            .unwrap();

        let old_node = unique_node("lane-old");
        let mut old_ids = Vec::new();
        for seq in 1..=8 {
            old_ids.push(
                wal_segments::add(&pending_seg(&old_node, seq))
                    .await
                    .unwrap(),
            );
        }
        let old_created: i64 = wal_segments::get_by_ids(&old_ids).await.unwrap()[0].created_at;
        // engagement clock: the cohort is 7h old against a 6h lane
        let now = old_created + 7 * 3_600 * 1_000_000;
        let lane_secs = 21_600u64;

        let fresh_node = unique_node("lane-fresh");
        async fn add_fresh(node: &str, seq: &mut i64, n: usize) {
            for _ in 0..n {
                *seq += 1;
                wal_segments::add(&pending_seg(node, *seq)).await.unwrap();
            }
        }
        let mut fresh_seq = 0i64;

        // CONTROL: lane disabled — 16 balanced rounds never touch the old
        // cohort (this is the structural hole, asserted)
        for _ in 0..16 {
            add_fresh(&fresh_node, &mut fresh_seq, 4).await;
            let claimed =
                wal_segments::claim_pending_with_floor("ctl", 4, 1, 3600, ClaimOrder::NewestFirst)
                    .await
                    .unwrap();
            assert_eq!(claimed.len(), 4);
            assert!(
                claimed.iter().all(|m| m.node_uuid == fresh_node),
                "newest-first under balanced arrivals must only ever claim fresh rows"
            );
            let ids: Vec<i64> = claimed.iter().map(|m| m.id).collect();
            assert_eq!(wal_segments::mark_built(&ids, "ctl").await.unwrap(), 4);
        }
        for meta in wal_segments::get_by_ids(&old_ids).await.unwrap() {
            assert_eq!(
                meta.status,
                SegmentStatus::Pending,
                "control: the oldest cohort starves under pure newest-first"
            );
        }

        // LANE: same balanced rounds, the REAL decision helpers at ratio
        // 0.25 — rounds 4 and 8 fire oldest-first and drain the cohort
        let acc = AtomicU64::new(0);
        let mut cohort_done_round = None;
        for round in 1..=16 {
            add_fresh(&fresh_node, &mut fresh_seq, 4).await;
            let (_, oldest_created_at, _) = wal_segments::claimable_stats(3600).await.unwrap();
            let order = if age_lane_engaged(oldest_created_at, now, lane_secs)
                && age_lane_fire(&acc, 0.25)
            {
                ClaimOrder::OldestFirst
            } else {
                ClaimOrder::NewestFirst
            };
            let claimed = wal_segments::claim_pending_with_floor("lane", 4, 1, 3600, order)
                .await
                .unwrap();
            assert_eq!(claimed.len(), 4);
            let ids: Vec<i64> = claimed.iter().map(|m| m.id).collect();
            assert_eq!(wal_segments::mark_built(&ids, "lane").await.unwrap(), 4);
            let built_old = wal_segments::get_by_ids(&old_ids)
                .await
                .unwrap()
                .iter()
                .filter(|m| m.status == SegmentStatus::Built)
                .count();
            if round < 4 {
                assert_eq!(built_old, 0, "the lane must not fire before its cadence");
            }
            if built_old == old_ids.len() && cohort_done_round.is_none() {
                cohort_done_round = Some(round);
            }
        }
        assert_eq!(
            cohort_done_round,
            Some(8),
            "8-segment cohort at batch 4 / ratio 0.25 drains on the 2nd fire (round 8)"
        );

        // leave no Pending leftovers for other tests: claim + build the tail
        loop {
            let rest = wal_segments::claim_pending("m13-lane-sweep", 10_000, 3600)
                .await
                .unwrap();
            let mine: Vec<i64> = rest
                .iter()
                .filter(|m| m.node_uuid == fresh_node || m.node_uuid == old_node)
                .map(|m| m.id)
                .collect();
            if mine.is_empty() {
                break;
            }
            wal_segments::mark_built(&mine, "m13-lane-sweep")
                .await
                .unwrap();
        }
    }

    // ── M13 (1b) backlog-mode super-batch sealing ─────────────────────────

    /// The empty-claim policy matrix: claimable work = bounded immediate
    /// retries (work-bounded accumulation); no claimable work = the exact
    /// pre-M13 trickle pacing (one wait, two-empty-ticks seal, age-clock
    /// seal) — and the clock caps only WAITING, never a flowing claim.
    #[test]
    fn test_on_empty_extension_claim_policy_matrix() {
        use std::time::Duration;

        // racing losses: claimable work retries immediately, bounded
        let (mut retries, mut ticks) = (0u32, 0u32);
        for expect_retry in 1..=EMPTY_CLAIM_RACE_RETRIES {
            assert_eq!(
                on_empty_extension_claim(
                    true,
                    &mut retries,
                    &mut ticks,
                    Duration::from_secs(0),
                    120
                ),
                EmptyClaimAction::RetryNow
            );
            assert_eq!((retries, ticks), (expect_retry, 0), "no tick on a race retry");
        }
        // retries exhausted: even with claimable work, fall to the gap path
        assert_eq!(
            on_empty_extension_claim(true, &mut retries, &mut ticks, Duration::from_secs(0), 120),
            EmptyClaimAction::Wait
        );
        assert_eq!(ticks, 1);

        // true trickle (nothing claimable): first gap waits, second seals
        let (mut retries, mut ticks) = (0u32, 0u32);
        assert_eq!(
            on_empty_extension_claim(false, &mut retries, &mut ticks, Duration::from_secs(1), 120),
            EmptyClaimAction::Wait
        );
        assert_eq!(
            on_empty_extension_claim(false, &mut retries, &mut ticks, Duration::from_secs(6), 120),
            EmptyClaimAction::Seal,
            "two consecutive empty ticks seal (traffic stopped)"
        );

        // the age clock seals a WAITING accumulation...
        let (mut retries, mut ticks) = (0u32, 0u32);
        assert_eq!(
            on_empty_extension_claim(
                false,
                &mut retries,
                &mut ticks,
                Duration::from_secs(120),
                120
            ),
            EmptyClaimAction::Seal
        );
        assert_eq!(ticks, 0, "clock seal is not an empty tick");
        // ...but a claimable-work retry outranks the clock: flowing
        // accumulations are bounded by work, not time
        let (mut retries, mut ticks) = (0u32, 0u32);
        assert_eq!(
            on_empty_extension_claim(
                true,
                &mut retries,
                &mut ticks,
                Duration::from_secs(999),
                120
            ),
            EmptyClaimAction::RetryNow
        );
    }

    /// DEEP-BACKLOG SEAL-BY-SIZE (the M13 1b pin): with a table full of
    /// claimable work, accumulation runs claim-after-claim to the byte
    /// budget and seals immediately — no wait-induced gaps (any single
    /// arrival-gap sleep would cost BUILDER_TICK_SECS = 5s; the whole
    /// accumulation must finish far under that), and the cycle is bounded
    /// by WORK, not by the age clock (prod 2026-08-18/19: wait-paced
    /// accumulation held builders at 5-8 super-batch cycles/15min under a
    /// 72k backlog).
    #[tokio::test]
    async fn test_super_batch_deep_backlog_seals_by_size_without_waits() {
        let _guard = setup().await;
        // drain foreign claimable rows (kept leased for the whole test)
        wal_segments::claim_pending("m13-sb-drain", 100_000, 3600)
            .await
            .unwrap();

        let node = unique_node("sb-deep");
        for seq in 1..=20 {
            wal_segments::add(&pending_seg(&node, seq)).await.unwrap();
        }

        // initial full batch of 4 (pending_seg.size = 1024)
        let mut claim =
            wal_segments::claim_pending_with_floor("sb-builder", 4, 4, 3600, ClaimOrder::NewestFirst)
                .await
                .unwrap();
        assert_eq!(claim.len(), 4);
        let mut guards = Vec::new();
        let started = std::time::Instant::now();
        let total = accumulate_super_batch(
            &mut claim,
            &mut guards,
            "sb-builder",
            4,
            3600,
            ClaimOrder::NewestFirst,
            16 * 1024, // budget: 16 segments' worth
            120,
        )
        .await;
        let wall = started.elapsed();
        assert_eq!(total, 16 * 1024, "seals exactly at the byte budget");
        assert_eq!(claim.len(), 16, "12 extension segments over 3 claims");
        assert_eq!(guards.len(), 3, "every extension claim is heartbeat-guarded");
        assert!(
            wall < Duration::from_secs(BUILDER_TICK_SECS),
            "deep-backlog accumulation must never sleep (took {wall:?})"
        );
        for m in &claim {
            assert_eq!(m.status, SegmentStatus::Building);
            assert_eq!(m.builder_node, "sb-builder");
        }

        // hygiene: build everything claimed + the 4 leftovers
        let ids: Vec<i64> = claim.iter().map(|m| m.id).collect();
        assert_eq!(
            wal_segments::mark_built(&ids, "sb-builder").await.unwrap(),
            16
        );
        let rest = wal_segments::claim_pending("m13-sb-sweep", 10_000, 3600)
            .await
            .unwrap();
        let mine: Vec<i64> = rest
            .iter()
            .filter(|m| m.node_uuid == node)
            .map(|m| m.id)
            .collect();
        assert_eq!(mine.len(), 4);
        wal_segments::mark_built(&mine, "m13-sb-sweep").await.unwrap();
    }

    // ── contiguous runs + deterministic keys ─────────────────────────────

    #[test]
    fn test_chunk_run_per_stream_caps_each_stream_on_its_own_bytes() {
        // "big" carries ~2/3 of the cap per segment; "tiny" a few rows. The
        // aggregate crosses the cap every other segment, but ONLY the big
        // stream may split — the tiny stream must come out as ONE chunk
        // spanning the whole run (the sliver-file regression, 2026-08-07).
        let big_rows = (BUILD_CHUNK_MAX_DECODED_BYTES * 2 / 3) / 8; // 8 bytes per i64 row
        let frame_of = |stream: &str, n: usize| {
            let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
            let batch =
                RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![7i64; n]))])
                    .unwrap();
            SegmentFrame {
                org: "o".into(),
                stream_type: StreamType::Logs,
                stream: stream.into(),
                min_ts: 1,
                max_ts: 2,
                batch,
            }
        };
        let run = vec![
            (1, vec![frame_of("big", big_rows), frame_of("tiny", 4)]),
            (2, vec![frame_of("big", big_rows), frame_of("tiny", 4)]),
            (3, vec![frame_of("big", big_rows), frame_of("tiny", 4)]),
        ];
        let chunked = chunk_run_per_stream(run);
        assert_eq!(chunked.len(), 2);

        let big = chunked.iter().find(|s| s.stream == "big").unwrap();
        let ranges: Vec<(i64, i64)> = big.chunks.iter().map(|c| (c.start_id, c.end_id)).collect();
        // seg1 alone (adding seg2 would overflow its cap), then seg2, then
        // seg3 — and the ranges tile the run without gaps
        assert_eq!(ranges, vec![(1, 1), (2, 2), (3, 3)]);
        assert!(big.chunks.iter().all(|c| !c.batches.is_empty()));

        let tiny = chunked.iter().find(|s| s.stream == "tiny").unwrap();
        let ranges: Vec<(i64, i64)> = tiny.chunks.iter().map(|c| (c.start_id, c.end_id)).collect();
        assert_eq!(
            ranges,
            vec![(1, 3)],
            "a small stream must not inherit its neighbors' chunk boundaries"
        );
        assert_eq!(tiny.chunks[0].batches.len(), 3);
    }

    #[test]
    fn test_chunk_run_per_stream_absent_segments_extend_ranges() {
        // stream "gappy" appears only in segments 5 and 7 of run 4..=8: its
        // single chunk must still cover the whole run [4, 8] — covered ids
        // without frames contribute no rows, so the wider range is sound and
        // keeps ranges tiling the run.
        let frame_of = |stream: &str| {
            let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
            let batch =
                RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![7i64; 2]))])
                    .unwrap();
            SegmentFrame {
                org: "o".into(),
                stream_type: StreamType::Logs,
                stream: stream.into(),
                min_ts: 1,
                max_ts: 2,
                batch,
            }
        };
        let run = vec![
            (4, vec![frame_of("steady")]),
            (5, vec![frame_of("steady"), frame_of("gappy")]),
            (6, vec![frame_of("steady")]),
            (7, vec![frame_of("gappy")]),
            (8, vec![frame_of("steady")]),
        ];
        let chunked = chunk_run_per_stream(run);
        for group in &chunked {
            assert_eq!(group.chunks.len(), 1);
            assert_eq!(
                (group.chunks[0].start_id, group.chunks[0].end_id),
                (4, 8),
                "stream {} must span the whole run",
                group.stream
            );
        }
        let gappy = chunked.iter().find(|s| s.stream == "gappy").unwrap();
        assert_eq!(gappy.chunks[0].batches.len(), 2);

        // determinism: the same run re-chunked reproduces identical ranges
        let run = vec![
            (4, vec![frame_of("steady")]),
            (5, vec![frame_of("steady"), frame_of("gappy")]),
            (6, vec![frame_of("steady")]),
            (7, vec![frame_of("gappy")]),
            (8, vec![frame_of("steady")]),
        ];
        let again = chunk_run_per_stream(run);
        let ranges = |groups: &[StreamChunks]| -> Vec<(String, Vec<(i64, i64)>)> {
            groups
                .iter()
                .map(|g| {
                    (
                        g.stream.clone(),
                        g.chunks.iter().map(|c| (c.start_id, c.end_id)).collect(),
                    )
                })
                .collect()
        };
        assert_eq!(ranges(&chunked), ranges(&again));

        // empty run → nothing
        assert!(chunk_run_per_stream(Vec::new()).is_empty());
    }

    #[test]
    fn test_chunk_run_per_stream_oversized_segment_builds_alone() {
        // one segment alone over the cap must still produce a chunk (nothing
        // is ever skipped); the next segment starts the following chunk
        let over_rows = (BUILD_CHUNK_MAX_DECODED_BYTES + 1024) / 8;
        let frame_of = |n: usize| {
            let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
            let batch =
                RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![7i64; n]))])
                    .unwrap();
            SegmentFrame {
                org: "o".into(),
                stream_type: StreamType::Logs,
                stream: "s".into(),
                min_ts: 1,
                max_ts: 2,
                batch,
            }
        };
        let run = vec![(10, vec![frame_of(over_rows)]), (11, vec![frame_of(4)])];
        let chunked = chunk_run_per_stream(run);
        assert_eq!(chunked.len(), 1);
        let ranges: Vec<(i64, i64)> = chunked[0]
            .chunks
            .iter()
            .map(|c| (c.start_id, c.end_id))
            .collect();
        assert_eq!(ranges, vec![(10, 10), (11, 11)]);
    }

    #[test]
    fn test_split_into_id_runs_shapes() {
        // contiguity only — byte capping happens in split_into_decoded_runs
        let runs = split_into_id_runs(vec![(2, "b"), (1, "a"), (3, "c")]);
        assert_eq!(
            runs,
            vec![vec![(1, "a"), (2, "b"), (3, "c")]],
            "consecutive ids form one sorted run"
        );

        // gap in the middle splits exactly there
        let runs = split_into_id_runs(vec![(6, ()), (3, ()), (7, ()), (4, ())]);
        assert_eq!(runs, vec![vec![(3, ()), (4, ())], vec![(6, ()), (7, ())]]);

        // nothing decoded (all skipped) → no runs, no files
        assert!(split_into_id_runs(Vec::<(i64, ())>::new()).is_empty());

        // every id isolated → one singleton run each
        let runs = split_into_id_runs(vec![(14, ()), (10, ()), (12, ())]);
        assert_eq!(runs, vec![vec![(10, ())], vec![(12, ())], vec![(14, ())]]);

        // i64::MAX must not overflow the contiguity probe
        let runs = split_into_id_runs(vec![(i64::MAX, ()), (i64::MAX - 1, ())]);
        assert_eq!(runs, vec![vec![(i64::MAX - 1, ()), (i64::MAX, ())]]);
    }

    #[test]
    fn test_l0_key_is_deterministic_and_parseable() {
        // one contiguous decoded run 3..=5 (arrival order scrambled)
        let m3 = seg_meta(3, "node-a", 1);
        let m4 = seg_meta(4, "node-a", 2);
        let m5 = seg_meta(5, "node-a", 3);
        let parts = l0_key_parts(&[&m5, &m3, &m4]).unwrap();
        assert_eq!(
            parts,
            L0KeyParts {
                writer_uuid: "node-a".to_string(),
                min_id: 3,
                max_id: 5,
            }
        );

        let hour11 = T0 + 11 * HOUR_MICROS; // 2021-01-01T11:00Z
        let key =
            l0_object_key("default", StreamType::Logs, "app1", hour11, &parts, ".vix").unwrap();
        assert_eq!(
            key,
            format!(
                "files/default/logs/app1/2021/01/01/11/l0_node-a_3_5_{}.vix",
                hour11 / HOUR_MICROS
            )
        );
        // same decoded set → same key
        let again =
            l0_object_key("default", StreamType::Logs, "app1", hour11, &parts, ".vix").unwrap();
        assert_eq!(key, again);
        // different hour → different key
        let other = l0_object_key(
            "default",
            StreamType::Logs,
            "app1",
            hour11 + HOUR_MICROS,
            &parts,
            ".vix",
        )
        .unwrap();
        assert_ne!(key, other);
        assert!(other.contains("/2021/01/01/12/"));

        // member order must not matter
        assert_eq!(parts, l0_key_parts(&[&m3, &m4, &m5]).unwrap());

        // the key parses under the file_list convention
        let (stream_key, date_key, file_name) = parse_file_key_columns(&key).unwrap();
        assert_eq!(stream_key, "default/logs/app1");
        assert_eq!(date_key, "2021/01/01/11");
        assert!(file_name.starts_with("l0_node-a_3_5_"));

        // the compaction-hint inverse agrees
        let (org, stream_type, stream) = parse_l0_stream(&key).unwrap();
        assert_eq!(
            (org.as_str(), stream_type, stream.as_str()),
            ("default", StreamType::Logs, "app1")
        );
    }

    /// The provenance-gap fix: a skip INSIDE the claim splits the decoded
    /// ids into runs whose key ranges exclude the skipped id, so the leader
    /// dedup can never suppress the still-unbuilt segment.
    #[test]
    fn test_mid_claim_skip_yields_runs_excluding_the_skipped_id() {
        // claim 3..=7 for one writer; id 5 fails to decode
        let metas: Vec<SegmentMeta> = (3..=7).map(|id| seg_meta(id, "node-a", id)).collect();
        let decoded: Vec<(i64, ())> = [7, 3, 6, 4].into_iter().map(|id| (id, ())).collect();

        let runs = split_into_id_runs(decoded);
        assert_eq!(runs.len(), 2, "the skip must split the claim in two");

        let hour11 = T0 + 11 * HOUR_MICROS;
        let mut keys = Vec::new();
        for run in &runs {
            let run_metas: Vec<&SegmentMeta> = run
                .iter()
                .map(|(id, _)| metas.iter().find(|m| m.id == *id).unwrap())
                .collect();
            let parts = l0_key_parts(&run_metas).unwrap();
            // every id in [min, max] is a decoded member: 5 is outside
            assert!(
                parts.min_id > 5 || parts.max_id < 5,
                "range [{}, {}] must exclude the skipped id",
                parts.min_id,
                parts.max_id
            );
            keys.push(
                l0_object_key("default", StreamType::Logs, "app1", hour11, &parts, ".vix").unwrap(),
            );
        }
        let hour_index = hour11 / HOUR_MICROS;
        assert_eq!(
            keys,
            vec![
                format!("files/default/logs/app1/2021/01/01/11/l0_node-a_3_4_{hour_index}.vix"),
                format!("files/default/logs/app1/2021/01/01/11/l0_node-a_6_7_{hour_index}.vix"),
            ]
        );

        // the same decoded set in another arrival order reproduces the keys
        let decoded: Vec<(i64, ())> = [4, 6, 3, 7].into_iter().map(|id| (id, ())).collect();
        let again: Vec<String> = split_into_id_runs(decoded)
            .iter()
            .map(|run| {
                let run_metas: Vec<&SegmentMeta> = run
                    .iter()
                    .map(|(id, _)| metas.iter().find(|m| m.id == *id).unwrap())
                    .collect();
                let parts = l0_key_parts(&run_metas).unwrap();
                l0_object_key("default", StreamType::Logs, "app1", hour11, &parts, ".vix").unwrap()
            })
            .collect();
        assert_eq!(keys, again);
    }

    #[test]
    fn test_l0_key_parts_multi_writer_and_empty_run() {
        let m1 = seg_meta(1, "node-a", 1);
        let m2 = seg_meta(2, "node-b", 1);
        let parts = l0_key_parts(&[&m1, &m2]).unwrap();
        assert_eq!(parts.writer_uuid, "multi");
        assert_eq!((parts.min_id, parts.max_id), (1, 2));
        assert!(l0_key_parts(&[]).is_err());
    }

    #[test]
    fn test_validate_stream_identity_rejects_path_tokens() {
        assert!(validate_stream_identity("org1", "app1").is_ok());
        assert!(validate_stream_identity("org1", "app/1").is_err());
        assert!(validate_stream_identity("or\\g", "app1").is_err());
        assert!(validate_stream_identity("org1", "..").is_err());
        assert!(validate_stream_identity("", "app1").is_err());
        assert!(validate_stream_identity("org1", "").is_err());
    }

    // ── sqlite-backed integration: fencing + decode isolation ───────────
    //
    // These share the process-global sqlite pools and local-disk object
    // store (same setup as the infra::wal_segments tests), so they
    // serialize on one lock and use per-run unique identities instead of
    // wiping shared tables.

    static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    async fn setup() -> tokio::sync::MutexGuard<'static, ()> {
        let guard = TEST_LOCK.lock().await;
        let cfg = get_config();
        std::fs::create_dir_all(&cfg.common.data_db_dir).expect("create data_db_dir");
        std::fs::create_dir_all(&cfg.common.data_stream_dir).expect("create data_stream_dir");
        wal_segments::create_table()
            .await
            .expect("create wal_segments table");
        infra::file_list::create_table()
            .await
            .expect("create file_list table");
        // the unique (stream, date, file) index is what the duplicate-key
        // tolerance in register_l0_files is exercised against
        infra::file_list::create_table_index()
            .await
            .expect("create file_list indexes");
        guard
    }

    fn unique_node(prefix: &str) -> String {
        format!("{prefix}-{}", config::utils::time::now_micros())
    }

    fn pending_seg(node: &str, seq: i64) -> SegmentMeta {
        SegmentMeta {
            id: 0,
            node_uuid: node.to_string(),
            seq,
            object_key: format!("wal_segments/{node}/{seq:020}"),
            min_ts: T0,
            max_ts: T0 + 1000,
            size: 1024,
            streams: vec!["org1/logs/app1".to_string()],
            status: SegmentStatus::Pending,
            builder_node: String::new(),
            created_at: 0,
            updated_at: 0,
        }
    }

    fn l0_file_key(org: &str, name: &str) -> FileKey {
        FileKey::new(
            0,
            String::new(),
            format!("files/{org}/logs/app1/2021/01/01/11/{name}.vix"),
            FileMeta {
                min_ts: T0,
                max_ts: T0 + 1000,
                records: 10,
                original_size: 1000,
                compressed_size: 100,
                ..Default::default()
            },
            false,
        )
    }

    /// M29: a claimed segment whose object GET returns NotFound (the
    /// lifecycle-expired kill-era zombie shape) is terminally resolved by
    /// `process_claim` — the row flips Built with no files and never comes
    /// back through a claim, instead of the old claim -> 404 -> lease-expiry
    /// -> re-claim loop (prod 2026-08-24: 189.7k such rows, 722k 404-skip
    /// ERROR lines/30m, every claim batch diluted to 1-2 real segments).
    #[tokio::test]
    async fn test_m29_gone_object_claims_are_tombstoned_not_retried() {
        let _guard = setup().await;
        let writer = unique_node("m29gone");
        let org = unique_node("m29gorg");
        // seq 1: a REAL segment object; seqs 2 and 3: rows registered but
        // objects gone (the lifecycle-expired kill-era shape) — the MIXED
        // batch is the prod shape ("in=64 built=1 skipped=63")
        put_segment_object(&writer, 1, &org).await;
        let id_real = wal_segments::add(&pending_seg(&writer, 1)).await.unwrap();
        let id_gone1 = wal_segments::add(&pending_seg(&writer, 2)).await.unwrap();
        let id_gone2 = wal_segments::add(&pending_seg(&writer, 3)).await.unwrap();

        let node = unique_node("m29-builder");
        let claim = wal_segments::claim_pending(&node, 10_000, 3600)
            .await
            .unwrap();
        let mine: Vec<SegmentMeta> = claim
            .iter()
            .filter(|m| m.node_uuid == writer)
            .cloned()
            .collect();
        assert_eq!(mine.len(), 3, "all three rows must be claimable once");
        let strangers: Vec<i64> = claim
            .iter()
            .filter(|m| m.node_uuid != writer)
            .map(|m| m.id)
            .collect();
        if !strangers.is_empty() {
            wal_segments::release_claims(&strangers, &node).await.unwrap();
        }

        // the REAL batch path: 404s tombstone, the real segment still builds
        // and COMMITS (the plan fence must not count tombstoned rows — a
        // mixed batch used to read as a lost lease and discard the build)
        let stats = process_claim(&mine, &node).await.expect("batch succeeds");
        assert_eq!(stats.gone, 2, "both gone rows terminally resolved");
        assert_eq!(stats.skipped, 0, "a gone object is not a transient skip");
        assert_eq!(stats.built, 1, "the real segment still builds");
        assert_eq!(stats.flipped, 1, "the real build must COMMIT despite tombstoned batchmates");
        assert!(stats.files >= 1, "the real segment's L0 file registers");

        // even with every lease expired (timeout 0) none are reclaimable:
        // gone rows are tombstoned, the real one is Built
        tokio::time::sleep(Duration::from_millis(20)).await;
        let thief = unique_node("m29-thief");
        let reclaim = wal_segments::claim_pending(&thief, 10_000, 0).await.unwrap();
        assert!(
            !reclaim
                .iter()
                .any(|m| m.id == id_real || m.id == id_gone1 || m.id == id_gone2),
            "tombstoned rows must never re-enter the claim/retry loop"
        );
        if !reclaim.is_empty() {
            let ids: Vec<i64> = reclaim.iter().map(|m| m.id).collect();
            wal_segments::release_claims(&ids, &thief).await.unwrap();
        }

        // the normal sweeper sees the tombstones as expired Built rows
        let expired = wal_segments::list_expired(0, 100_000).await.unwrap();
        for id in [id_gone1, id_gone2] {
            assert!(
                expired.iter().any(|m| m.id == id),
                "tombstoned row {id} must reach the sweeper's Built-expired set"
            );
        }
        wal_segments::delete(&[id_real, id_gone1, id_gone2])
            .await
            .unwrap();
    }

    /// claim → build → lease stolen by a second node → the loser's fenced
    /// `mark_built_with_files` rolls back WHOLE: zero flips AND zero file
    /// rows registered; the winner then commits the identical keys with
    /// files and flip landing together.
    #[tokio::test]
    async fn test_claim_register_mark_built_fencing_with_lease_steal() {
        let _guard = setup().await;
        let writer = unique_node("writer");
        let id1 = wal_segments::add(&pending_seg(&writer, 1)).await.unwrap();
        let id2 = wal_segments::add(&pending_seg(&writer, 2)).await.unwrap();

        // builder A claims everything buildable (a large limit tolerates
        // leftover rows from other runs sharing this sqlite file)
        let claim_a = wal_segments::claim_pending("builder-a", 10_000, 3600)
            .await
            .unwrap();
        let mut mine_a: Vec<i64> = claim_a
            .iter()
            .filter(|m| m.node_uuid == writer)
            .map(|m| m.id)
            .collect();
        // claims come back newest-first; this test cares about ownership,
        // not order
        mine_a.sort_unstable();
        assert_eq!(mine_a, vec![id1, id2], "A must hold both segments");

        // A's build output (deterministic exact-run keys)
        let org = unique_node("torg");
        let files = vec![
            l0_file_key(&org, "l0_w_1_2_447083"),
            l0_file_key(&org, "l0_w_1_2_447084"),
        ];

        // builder B steals the lease: claim with a zero lease timeout makes
        // every Building row stale (updated_at strictly in the past)
        tokio::time::sleep(Duration::from_millis(50)).await;
        let claim_b = wal_segments::claim_pending("builder-b", 10_000, 0)
            .await
            .unwrap();
        let mut mine_b: Vec<i64> = claim_b
            .iter()
            .filter(|m| m.node_uuid == writer)
            .map(|m| m.id)
            .collect();
        // newest-first claim order; this test asserts ownership, not order
        mine_b.sort_unstable();
        assert_eq!(mine_b, vec![id1, id2], "B must have stolen the lease");

        // A finishes late: the fenced transaction flips NOTHING and leaves
        // ZERO file rows — the loser's registration rolls back whole
        let flipped_a =
            wal_segments::mark_built_with_files(&[id1, id2], "builder-a", files.clone())
                .await
                .unwrap();
        assert_eq!(flipped_a, 0, "loser must see a short (zero) flip count");
        for file in &files {
            assert!(
                !infra::file_list::contains(&file.key).await.unwrap(),
                "{} must NOT survive the loser's rolled-back transaction",
                file.key
            );
        }

        // B owns the lease: one transaction registers the files AND flips
        let flipped_b =
            wal_segments::mark_built_with_files(&[id1, id2], "builder-b", files.clone())
                .await
                .unwrap();
        assert_eq!(flipped_b, 2);
        for file in &files {
            assert!(
                infra::file_list::contains(&file.key).await.unwrap(),
                "{} must be registered by the winner's commit",
                file.key
            );
        }

        // cleanup our wal_segments rows (file_list rows are per-run unique)
        wal_segments::delete(&[id1, id2]).await.unwrap();
    }

    /// `mark_built_with_files` must FAIL (not skip) on a degenerate meta,
    /// register nothing, and leave the claim un-flipped — a segment must
    /// never go Built off a failed registration.
    #[tokio::test]
    async fn test_mark_built_with_files_propagates_invalid_meta() {
        let _guard = setup().await;
        let writer = unique_node("badmeta");
        let id = wal_segments::add(&pending_seg(&writer, 1)).await.unwrap();
        let claim = wal_segments::claim_pending("builder-bad", 10_000, 3600)
            .await
            .unwrap();
        assert!(claim.iter().any(|m| m.id == id), "claim must hold the row");

        let org = unique_node("torg");
        let mut bad = l0_file_key(&org, "l0_bad_1_1_447083");
        bad.meta.min_ts = 0; // degenerate: records > 0 with min_ts <= 0
        let err = wal_segments::mark_built_with_files(&[id], "builder-bad", vec![bad.clone()])
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("degenerate")
                || format!("{err:#}").contains("InvalidFileMeta"),
            "unexpected error: {err:#}"
        );
        assert!(
            !infra::file_list::contains(&bad.key).await.unwrap(),
            "failed transaction must register nothing"
        );

        // the segment stayed Building: an expired lease brings it back
        let reclaimed = wal_segments::claim_pending("builder-2", 10_000, 0)
            .await
            .unwrap();
        assert!(
            reclaimed.iter().any(|m| m.id == id),
            "segment must stay unbuilt after the failed commit"
        );
        wal_segments::delete(&[id]).await.unwrap();
    }

    /// End-to-end through the REAL mover build (`write_core_file_from_tables`
    /// fed by a `NewMemTable`): a logs stream with a type-flipped field
    /// spanning an hour boundary must produce one fully-indexed `.vix` per
    /// hour under deterministic keys carrying data-derived metas — and
    /// NOTHING may reach the object store until the explicit upload step
    /// (the GC ordering: planned keys go durable between build and upload).
    #[tokio::test]
    async fn test_build_stream_files_builds_one_vix_per_hour() {
        let _guard = setup().await;
        let hour_a = T0 + 10 * HOUR_MICROS;
        let hour_b = T0 + 11 * HOUR_MICROS;
        let s_int = schema_of(vec![
            ts_field(),
            Field::new("value", DataType::Int64, false),
            Field::new("log", DataType::Utf8, false),
        ]);
        let s_str = schema_of(vec![ts_field(), Field::new("value", DataType::Utf8, false)]);
        // batch 1 (value: Int64) straddles the boundary; batch 2 (value:
        // Utf8) lands in the second hour — the single homogenization point
        // must reconcile them before the per-hour builds
        let b1 = RecordBatch::try_new(
            s_int,
            vec![
                Arc::new(Int64Array::from(vec![hour_a + 1, hour_a + 2, hour_b + 5])),
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["alpha", "beta", "gamma"])),
            ],
        )
        .unwrap();
        let b2 = RecordBatch::try_new(
            s_str,
            vec![
                Arc::new(Int64Array::from(vec![hour_b + 1])),
                Arc::new(StringArray::from(vec!["four"])),
            ],
        )
        .unwrap();

        let org = unique_node("torg");
        let group = StreamGroup {
            org: org.clone(),
            stream_type: StreamType::Logs,
            stream: "app1".to_string(),
            batches: vec![b1, b2],
        };
        let parts = L0KeyParts {
            writer_uuid: "node-a".to_string(),
            min_id: 1,
            max_id: 2,
        };
        let built = build_stream_files(group, &parts).await.unwrap();
        assert_eq!(built.len(), 2, "one L0 file per hour bucket");
        let files: Vec<FileKey> = built.iter().map(|b| b.file.clone()).collect();

        let expect_a =
            l0_object_key(&org, StreamType::Logs, "app1", hour_a, &parts, ".vix").unwrap();
        let expect_b =
            l0_object_key(&org, StreamType::Logs, "app1", hour_b, &parts, ".vix").unwrap();
        assert_eq!(files[0].key, expect_a);
        assert_eq!(files[1].key, expect_b);
        assert!(files[0].key.contains("/2021/01/01/10/"));
        assert!(files[1].key.contains("/2021/01/01/11/"));

        // metas are data-derived and hour-bounded
        assert_eq!(files[0].meta.records, 2);
        assert_eq!(files[1].meta.records, 2);
        assert_eq!(files[0].meta.min_ts, hour_a + 1);
        assert_eq!(files[0].meta.max_ts, hour_a + 2);
        assert_eq!(files[1].meta.min_ts, hour_b + 1);
        assert_eq!(files[1].meta.max_ts, hour_b + 5);

        // GC ordering invariant: building must not touch the object store —
        // the planned-keys write slots between build and upload
        for file in &files {
            assert!(
                storage::head(&file.account, &file.key).await.is_err(),
                "{} must NOT exist before the upload step",
                file.key
            );
        }

        for one in built {
            upload_built_file(one).await.unwrap();
        }
        for file in &files {
            assert!(file.meta.compressed_size > 0);
            assert!(
                file.meta.index_size > 0,
                "an indexed L0 build must upload its .vxi sidecar (index_size = its size): {}",
                file.key
            );
            // the object was really uploaded, byte length matches the meta
            let bytes = storage::get_bytes(&file.account, &file.key)
                .await
                .unwrap_or_else(|e| panic!("uploaded object {} must exist: {e}", file.key));
            assert_eq!(
                bytes.len() as i64,
                file.meta.compressed_size,
                "{}",
                file.key
            );
            // v3 split: the sidecar uploaded too, with index_size = its
            // exact object length
            let sidecar_key = config::vix_sidecar_key(&file.key).expect("L0 keys are .vix");
            let sidecar = storage::get_bytes(&file.account, &sidecar_key)
                .await
                .unwrap_or_else(|e| panic!("uploaded sidecar {sidecar_key} must exist: {e}"));
            assert_eq!(sidecar.len() as i64, file.meta.index_size, "{sidecar_key}");
        }
    }

    /// Upload a real one-frame segment object for `(writer, seq)` whose rows
    /// live in `org/logs/app1` at `T0 + 11h`.
    async fn put_segment_object(writer: &str, seq: i64, org: &str) -> String {
        let key = format!("wal_segments/{writer}/{seq:020}");
        let header = SegmentHeader {
            node_uuid: writer.to_string(),
            seq: u64::try_from(seq).expect("test seq must be non-negative"),
            created_at: T0,
        };
        let hour11 = T0 + 11 * HOUR_MICROS;
        let frames = vec![SegmentFrame {
            org: org.to_string(),
            stream_type: StreamType::Logs,
            stream: "app1".to_string(),
            min_ts: hour11 + 1,
            max_ts: hour11 + 2,
            batch: RecordBatch::try_new(
                schema_of(vec![ts_field(), Field::new("v", DataType::Int64, false)]),
                vec![
                    Arc::new(Int64Array::from(vec![hour11 + 1, hour11 + 2])),
                    Arc::new(Int64Array::from(vec![1, 2])),
                ],
            )
            .unwrap(),
        }];
        let bytes = encode_segment(&header, &frames).unwrap();
        storage::put("", &key, Bytes::from(bytes)).await.unwrap();
        key
    }

    /// The full new batch order through `process_claim`: build, planned keys
    /// durable, upload, then the fenced commit that clears the plan — ending
    /// with a Built row, a registered file, and a real object.
    #[tokio::test]
    async fn test_process_claim_plans_uploads_and_commits() {
        let _guard = setup().await;
        let writer = unique_node("order");
        let org = unique_node("torg");
        put_segment_object(&writer, 1, &org).await;
        let id = wal_segments::add(&pending_seg(&writer, 1)).await.unwrap();

        let claim = wal_segments::claim_pending("builder-ord", 10_000, 3600)
            .await
            .unwrap();
        let mine: Vec<SegmentMeta> = claim
            .into_iter()
            .filter(|m| m.node_uuid == writer)
            .collect();
        assert_eq!(mine.iter().map(|m| m.id).collect::<Vec<_>>(), vec![id]);

        let stats = process_claim(&mine, "builder-ord").await.unwrap();
        assert_eq!((stats.built, stats.files), (1, 1));
        assert_eq!(stats.flipped, 1, "fenced commit must flip the segment");

        let hour11 = T0 + 11 * HOUR_MICROS;
        let parts = L0KeyParts {
            writer_uuid: writer.clone(),
            min_id: id,
            max_id: id,
        };
        let key = l0_object_key(&org, StreamType::Logs, "app1", hour11, &parts, ".vix").unwrap();
        assert!(
            infra::file_list::contains(&key).await.unwrap(),
            "{key} must be registered"
        );
        assert!(
            storage::head("", &key).await.is_ok(),
            "{key} must exist in the store"
        );
        let row = &wal_segments::get_by_ids(&[id]).await.unwrap()[0];
        assert_eq!(row.status, SegmentStatus::Built);
        wal_segments::delete(&[id]).await.unwrap();
    }

    /// Lease stolen BETWEEN claim and the planned-keys write: the fenced
    /// plan comes back short and the whole build is discarded BEFORE any
    /// upload — no object, no file row, no flip, and the thief keeps the
    /// segment.
    #[tokio::test]
    async fn test_process_claim_discards_before_upload_when_lease_lost() {
        let _guard = setup().await;
        let writer = unique_node("steal");
        let org = unique_node("torg");
        put_segment_object(&writer, 1, &org).await;
        let id = wal_segments::add(&pending_seg(&writer, 1)).await.unwrap();

        let claim = wal_segments::claim_pending("loser", 10_000, 3600)
            .await
            .unwrap();
        let mine: Vec<SegmentMeta> = claim
            .into_iter()
            .filter(|m| m.node_uuid == writer)
            .collect();
        assert_eq!(mine.iter().map(|m| m.id).collect::<Vec<_>>(), vec![id]);

        // the thief takes every stale Building row (zero lease timeout)
        tokio::time::sleep(Duration::from_millis(50)).await;
        let stolen = wal_segments::claim_pending("thief", 10_000, 0)
            .await
            .unwrap();
        assert!(stolen.iter().any(|m| m.id == id), "thief must hold the row");

        let stats = process_claim(&mine, "loser").await.unwrap();
        assert_eq!(stats.flipped, 0, "discarded build must flip nothing");

        let hour11 = T0 + 11 * HOUR_MICROS;
        let parts = L0KeyParts {
            writer_uuid: writer.clone(),
            min_id: id,
            max_id: id,
        };
        let key = l0_object_key(&org, StreamType::Logs, "app1", hour11, &parts, ".vix").unwrap();
        assert!(
            storage::head("", &key).await.is_err(),
            "{key} must NOT be uploaded after the short plan write"
        );
        assert!(
            !infra::file_list::contains(&key).await.unwrap(),
            "{key} must NOT be registered"
        );
        let row = &wal_segments::get_by_ids(&[id]).await.unwrap()[0];
        assert_eq!(row.status, SegmentStatus::Building);
        assert_eq!(row.builder_node, "thief", "the winner keeps the segment");
        wal_segments::delete(&[id]).await.unwrap();
    }

    /// One bad segment (corrupt bytes / missing object) must be skipped and
    /// left leased while the good ones decode — never crash the batch.
    #[tokio::test]
    async fn test_fetch_and_decode_isolates_bad_segments() {
        let _guard = setup().await;
        let writer = unique_node("decoder");

        // good segment: two frames of one stream with type-flipped schemas
        let good_key = format!("wal_segments/{writer}/{:020}", 1);
        let header = SegmentHeader {
            node_uuid: writer.clone(),
            seq: 1,
            created_at: T0,
        };
        let s_int = schema_of(vec![ts_field(), Field::new("v", DataType::Int64, false)]);
        let s_str = schema_of(vec![ts_field(), Field::new("v", DataType::Utf8, false)]);
        let frames = vec![
            SegmentFrame {
                org: "org1".to_string(),
                stream_type: StreamType::Logs,
                stream: "app1".to_string(),
                min_ts: T0 + 1,
                max_ts: T0 + 2,
                batch: RecordBatch::try_new(
                    s_int,
                    vec![
                        Arc::new(Int64Array::from(vec![T0 + 1, T0 + 2])),
                        Arc::new(Int64Array::from(vec![1, 2])),
                    ],
                )
                .unwrap(),
            },
            SegmentFrame {
                org: "org1".to_string(),
                stream_type: StreamType::Logs,
                stream: "app1".to_string(),
                min_ts: T0 + 3,
                max_ts: T0 + 3,
                batch: RecordBatch::try_new(
                    s_str,
                    vec![
                        Arc::new(Int64Array::from(vec![T0 + 3])),
                        Arc::new(StringArray::from(vec!["three"])),
                    ],
                )
                .unwrap(),
            },
        ];
        let bytes = encode_segment(&header, &frames).unwrap();
        storage::put("", &good_key, Bytes::from(bytes))
            .await
            .unwrap();

        // corrupt segment: valid magic prefix, garbage tail
        let bad_key = format!("wal_segments/{writer}/{:020}", 2);
        storage::put(
            "",
            &bad_key,
            Bytes::from_static(b"O2WSgarbage-not-a-segment"),
        )
        .await
        .unwrap();

        // missing segment: registered row, no object
        let missing_key = format!("wal_segments/{writer}/{:020}", 3);

        let claim = vec![
            SegmentMeta {
                object_key: good_key,
                ..seg_meta(11, &writer, 1)
            },
            SegmentMeta {
                object_key: bad_key,
                ..seg_meta(12, &writer, 2)
            },
            SegmentMeta {
                object_key: missing_key,
                ..seg_meta(13, &writer, 3)
            },
        ];
        let (decoded, skipped, gone) = fetch_and_decode(&claim).await;
        assert_eq!(
            skipped,
            vec![12],
            "a garbage object is a transient skip (lease retry)"
        );
        assert_eq!(
            gone,
            vec![13],
            "a MISSING object (404) is terminal — M29 tombstone class"
        );
        assert_eq!(decoded.len(), 1);
        let (id, frames) = &decoded[0];
        assert_eq!(*id, 11);
        assert_eq!(frames.len(), 2);
        // frames keep their write-time (narrow, type-flipped) schemas —
        // homogenization happens later, in ONE place
        assert_eq!(
            frames[0]
                .batch
                .schema()
                .field_with_name("v")
                .unwrap()
                .data_type(),
            &DataType::Int64
        );
        assert_eq!(
            frames[1]
                .batch
                .schema()
                .field_with_name("v")
                .unwrap()
                .data_type(),
            &DataType::Utf8
        );

        // a decoded segment carrying a path-unsafe stream identity is
        // skipped exactly like a decode failure
        let evil_key = format!("wal_segments/{writer}/{:020}", 4);
        let evil = vec![SegmentFrame {
            org: "org1".to_string(),
            stream_type: StreamType::Logs,
            stream: "app/../../etc".to_string(),
            min_ts: T0 + 1,
            max_ts: T0 + 1,
            batch: RecordBatch::try_new(
                schema_of(vec![ts_field()]),
                vec![Arc::new(Int64Array::from(vec![T0 + 1]))],
            )
            .unwrap(),
        }];
        let bytes = encode_segment(&header, &evil).unwrap();
        storage::put("", &evil_key, Bytes::from(bytes))
            .await
            .unwrap();
        let claim = vec![SegmentMeta {
            object_key: evil_key,
            ..seg_meta(14, &writer, 4)
        }];
        let (decoded, skipped, gone) = fetch_and_decode(&claim).await;
        assert!(decoded.is_empty());
        assert_eq!(skipped, vec![14]);
        assert!(gone.is_empty(), "an unsafe identity is not a 404");
    }
}
