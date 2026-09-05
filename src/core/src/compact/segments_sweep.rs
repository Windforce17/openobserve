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

//! Segment sweeper (DESIGN-SEGMENT-WAL.md): retires `wal_segments` rows whose
//! L0 build finished more than `ZO_SEGMENT_RETAIN_SECS` ago. The segment
//! object is deleted FIRST, per key, and the row is removed ONLY after that
//! deletion is confirmed — a `NotFound` counts as confirmed (an earlier pass
//! deleted the object but crashed before removing the row, so the re-sweep
//! must converge). Rows behind failed deletions are left untouched and retry
//! on the next pass.
//!
//! The same supervised loop also runs a GC pass every [`GC_INTERVAL_SECS`]
//! for the two orphan classes a crashed build can leave behind:
//! - L0 objects uploaded by a builder that died before its fenced registration committed: the
//!   builder records its deterministic keys in `wal_segments.l0_planned` BEFORE uploading, so
//!   long-dead rows (3x the build lease — no live builder heartbeats that slowly) with a non-empty
//!   plan name exactly the objects to check. A key is deleted ONLY when `file_list` has no row with
//!   that exact key; each row is fenced (`gc_arm_l0_orphan`) before its keys are touched so a
//!   zombie builder's late commit or a fresh re-claim can never race the deletes.
//! - segment objects PUT by a flusher that crashed before registering the row: node uuids are
//!   per-boot, so such objects are never retried. The store listing under `wal_segments/` (client
//!   stream — the `storage::list` wrapper PANICS on error, a known audit finding) is anti-joined
//!   against the table's `object_key`s; rowless objects older than [`GC_SEGMENT_MIN_AGE_SECS`] are
//!   deleted, verified per key.
//!
//! `infra::storage::del` logs-and-swallows per-object errors (2026 audit
//! finding) — removing a row on a swallowed error would leak the object
//! forever and hide systematic delete failures — so deletion here goes
//! through the object-store client directly to get a real per-key `Result`.

use std::{collections::HashSet, future::Future, sync::LazyLock, time::Instant};

use config::{cluster::is_offline, get_config};
use futures::StreamExt;
use infra::{
    file_list,
    storage::accounts::StorageClientFactory,
    wal_segments::{self, SEGMENT_KEY_PREFIX},
};
use object_store::{ObjectStoreExt, path::Path};

/// Tick cadence.
const SWEEP_INTERVAL_SECS: u64 = 60;
/// Max rows examined per pass: bounds per-pass memory and object-store calls.
/// A tick keeps running passes until the backlog drains (see [`sweep_tick`]),
/// so this only sizes one pass, not the tick's throughput.
const SWEEP_BATCH: usize = 500;
/// Hard ceiling on passes within one tick: bounds a tick's total work even
/// when segment production outruns deletion. Hitting it is logged — a
/// persistent hit means the sweeper cannot keep up.
const MAX_PASSES_PER_TICK: usize = 20;
/// In-flight per-key object deletes within one pass.
const DELETE_CONCURRENCY: usize = 8;
/// GC cadence — its own, slower interval inside the sweep loop: crashed
/// builds are rare and the segment pass lists the object store.
const GC_INTERVAL_SECS: u64 = 600;
/// A builder lease must be dead this many times over before a row's planned
/// keys are collected: a live builder heartbeats every lease/3, so 3 full
/// leases without a touch means no live builder can be holding the row.
const GC_DEAD_LEASE_MULTIPLIER: u64 = 3;
/// Max L0-orphan rows processed per GC pass.
const GC_L0_ROW_BATCH: usize = 500;
/// Rowless segment objects must be at least this old (store `last_modified`;
/// segment keys carry no timestamp) before the GC may delete them — wide
/// enough that every flusher PUT-then-register retry cycle has long since
/// finished or died.
const GC_SEGMENT_MIN_AGE_SECS: i64 = 7200;
/// Max rowless-object deletes per GC pass.
const GC_SEGMENT_DELETE_BATCH: usize = 500;
/// `builder_node` stamp for rows the GC has armed (see
/// `wal_segments::gc_arm_l0_orphan`): fences out a zombie builder's late
/// fenced commit and keeps the row unclaimable for a lease while its keys
/// are processed. A constant is enough — concurrent GCs are serialized by
/// the arm's own aged-row predicate.
const GC_FENCE_NODE: &str = "segment-gc";

/// Segment objects live on the default storage account — the flusher uploads
/// them with an empty account name (see `segment_wal`'s uploader), and
/// org-scoped accounts never hold multi-org segment objects. The sweeper
/// keeps its own client factory because `storage::MULTI_ACCOUNTS` is private
/// and the public `storage::del` wrapper swallows per-key errors.
static SEGMENT_STORE: LazyLock<StorageClientFactory> = LazyLock::new(StorageClientFactory::new);

async fn delete_segment_object(key: String) -> Result<(), object_store::Error> {
    SEGMENT_STORE
        .get_client_by_name("")
        .delete(&Path::from(key.as_str()))
        .await
}

/// Per-key delete for PLANNED L0 objects. Unlike segment objects these live
/// under `files/{org}/...` and may hash to a non-default storage account —
/// resolve it exactly like the builder's uploader did. Accounts added at
/// runtime (org-level providers) are not in this factory and fall back to
/// the default client — the same accepted residual as every other sweeper
/// delete here.
async fn delete_l0_object(key: String) -> Result<(), object_store::Error> {
    let org = key
        .strip_prefix("files/")
        .and_then(|k| k.split('/').next())
        .unwrap_or("");
    let account = infra::storage::get_account(org, &key).unwrap_or_default();
    SEGMENT_STORE
        .get_client_by_name(&account)
        .delete(&Path::from(key.as_str()))
        .await
}

/// Spawn the sweeper under supervision. This replicates the private
/// `job::files::spawn_supervised` pattern: the sweeper is the only path that
/// reclaims segment storage, so an unwind must restart it instead of silently
/// killing it while the pod stays Ready.
pub fn spawn() {
    tokio::task::spawn(async move {
        loop {
            match tokio::task::spawn(run()).await {
                Ok(Ok(())) => {
                    log::info!("[SEGMENT:SWEEP] exited cleanly");
                    break;
                }
                Ok(Err(e)) => {
                    log::error!("[SEGMENT:SWEEP] exited with error, restarting in 5s: {e}");
                }
                Err(e) => {
                    log::error!("[SEGMENT:SWEEP] panicked, restarting in 5s: {e}");
                }
            }
            if is_offline() {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        }
    });
}

/// Sweep loop: one tick every [`SWEEP_INTERVAL_SECS`] until the node goes
/// offline. Rows are only removed after their object deletion is confirmed,
/// so a partial tick leaves nothing in a wrong state.
pub async fn run() -> Result<(), anyhow::Error> {
    log::info!(
        "[SEGMENT:SWEEP] started: interval={SWEEP_INTERVAL_SECS}s retain={}s batch={SWEEP_BATCH} max_passes={MAX_PASSES_PER_TICK}",
        get_config().common.segment_retain_secs
    );
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(SWEEP_INTERVAL_SECS));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut gc_last: Option<Instant> = None;
    loop {
        interval.tick().await;
        if is_offline() {
            break;
        }
        let retain_secs = get_config().common.segment_retain_secs;
        sweep_tick(
            retain_secs,
            SWEEP_BATCH,
            MAX_PASSES_PER_TICK,
            &delete_segment_object,
        )
        .await;
        // crashed-build GC on its own, slower cadence within the same
        // supervised loop
        if gc_last.is_none_or(|t| t.elapsed().as_secs() >= GC_INTERVAL_SECS) {
            gc_last = Some(Instant::now());
            let lease_secs = get_config().common.segment_build_lease_secs;
            gc_tick(lease_secs, &delete_l0_object, &delete_segment_object).await;
        }
        // backlog visibility: unbuilt segments aging past 10 minutes mean
        // builders are down or behind — the silent-mover-backlog failure
        // class this design replaces must never be invisible again
        match infra::wal_segments::count_unbuilt_older_than(600).await {
            Ok(0) => {}
            Ok(n) => {
                log::warn!(
                    "[SEGMENT:SWEEP] backlog: unbuilt_older_10m={n} — builders down or behind"
                );
            }
            Err(e) => {
                log::error!("[SEGMENT:SWEEP] backlog count failed: {e}");
            }
        }
    }
    log::info!("[SEGMENT:SWEEP] stopped");
    Ok(())
}

/// One tick: run [`sweep_pass`] until the backlog is drained (a pass
/// examines fewer rows than `batch`), a pass fails, a full pass confirms
/// nothing (only failed deletions remain — re-listing them within the tick
/// would hammer the object store with the same keys), or `max_passes` is
/// hit. One capped pass per tick was below realistic segment production, so
/// a tick must be able to drain a backlog; the ceiling keeps it bounded.
/// Returns `(passes_run, aggregated stats)` for tests.
async fn sweep_tick<F, Fut>(
    retain_secs: u64,
    batch: usize,
    max_passes: usize,
    delete_object: &F,
) -> (usize, SweepStats)
where
    F: Fn(String) -> Fut,
    Fut: Future<Output = Result<(), object_store::Error>>,
{
    let mut passes = 0;
    let mut total = SweepStats::default();
    let mut drained = false;
    while passes < max_passes {
        let stats = match sweep_pass(retain_secs, batch, delete_object).await {
            Ok(stats) => stats,
            Err(e) => {
                log::error!("[SEGMENT:SWEEP] pass failed, rows kept for next tick: {e}");
                break;
            }
        };
        passes += 1;
        if stats.examined > 0 {
            log::info!(
                "[SEGMENT:SWEEP] pass {passes}: examined={} deleted={} not_found={} failed={}",
                stats.examined,
                stats.deleted,
                stats.not_found,
                stats.failed
            );
        }
        let confirmed = stats.deleted + stats.not_found;
        drained = stats.examined < batch;
        total.absorb(&stats);
        if drained {
            break;
        }
        if confirmed == 0 {
            // a full page where every object delete failed: the next pass
            // would re-list exactly the same rows — leave them for the next
            // tick instead of retrying the store in a tight loop
            break;
        }
    }
    if !drained && passes >= max_passes {
        log::warn!(
            "[SEGMENT:SWEEP] pass ceiling {max_passes} hit with backlog remaining — deferring to next tick"
        );
    }
    if passes > 1 {
        log::info!(
            "[SEGMENT:SWEEP] tick summary: passes={passes} examined={} deleted={} not_found={} failed={}",
            total.examined,
            total.deleted,
            total.not_found,
            total.failed
        );
    }
    (passes, total)
}

#[derive(Debug, Default, PartialEq, Eq)]
struct SweepStats {
    examined: usize,
    deleted: usize,
    not_found: usize,
    failed: usize,
}

impl SweepStats {
    /// Fold one pass into a tick aggregate.
    fn absorb(&mut self, other: &SweepStats) {
        self.examined += other.examined;
        self.deleted += other.deleted;
        self.not_found += other.not_found;
        self.failed += other.failed;
    }
}

/// One sweep pass over at most `batch` expired built segments:
/// object delete per key -> collect confirmed ids (deleted or already gone)
/// -> remove ONLY those rows. `delete_object` is injected so tests can
/// script per-key outcomes; production passes [`delete_segment_object`].
async fn sweep_pass<F, Fut>(
    retain_secs: u64,
    batch: usize,
    delete_object: &F,
) -> Result<SweepStats, anyhow::Error>
where
    F: Fn(String) -> Fut,
    Fut: Future<Output = Result<(), object_store::Error>>,
{
    let rows = wal_segments::list_expired(retain_secs, batch)
        .await
        .map_err(|e| anyhow::anyhow!("list_expired(retain={retain_secs}s): {e}"))?;
    let mut stats = SweepStats {
        examined: rows.len(),
        ..Default::default()
    };
    if rows.is_empty() {
        return Ok(stats);
    }

    let mut confirmed: Vec<i64> = Vec::with_capacity(rows.len());
    let mut results = futures::stream::iter(rows.into_iter().map(move |row| {
        let key = row.object_key.clone();
        async move { (row, delete_object(key).await) }
    }))
    .buffer_unordered(DELETE_CONCURRENCY);
    while let Some((row, ret)) = results.next().await {
        match ret {
            Ok(()) => {
                stats.deleted += 1;
                confirmed.push(row.id);
            }
            // already gone: idempotent re-sweep after a crash between the
            // object delete and the row delete of an earlier pass
            Err(object_store::Error::NotFound { .. }) => {
                stats.not_found += 1;
                confirmed.push(row.id);
            }
            Err(e) => {
                stats.failed += 1;
                log::error!(
                    "[SEGMENT:SWEEP] object delete failed, row kept for retry: id={} object_key={} error={e}",
                    row.id,
                    row.object_key
                );
            }
        }
    }
    drop(results);

    if !confirmed.is_empty() {
        wal_segments::delete(&confirmed).await.map_err(|e| {
            anyhow::anyhow!(
                "row delete for confirmed ids {confirmed:?} failed (objects already deleted; \
                 the rows re-sweep as NotFound next pass): {e}"
            )
        })?;
    }
    Ok(stats)
}

// ─── crashed-build GC ────────────────────────────────────────────────────

#[derive(Debug, Default, PartialEq, Eq)]
struct GcStats {
    l0_orphans_deleted: usize,
    segment_orphans_deleted: usize,
    skipped_young: usize,
    failures: usize,
}

impl GcStats {
    fn absorb(&mut self, other: &GcStats) {
        self.l0_orphans_deleted += other.l0_orphans_deleted;
        self.segment_orphans_deleted += other.segment_orphans_deleted;
        self.skipped_young += other.skipped_young;
        self.failures += other.failures;
    }
}

/// One GC tick: both orphan passes, each failure-isolated (a failed pass is
/// logged, counted, and retried next tick — it never kills the loop), plus
/// the single `[SEGMENT:GC]` summary line when anything happened.
async fn gc_tick<L, LFut, S, SFut>(lease_secs: u64, delete_l0: &L, delete_segment: &S) -> GcStats
where
    L: Fn(String) -> LFut,
    LFut: Future<Output = Result<(), object_store::Error>>,
    S: Fn(String) -> SFut,
    SFut: Future<Output = Result<(), object_store::Error>>,
{
    let mut total = GcStats::default();
    let min_dead_age_secs = lease_secs.saturating_mul(GC_DEAD_LEASE_MULTIPLIER);
    match gc_l0_pass(min_dead_age_secs, GC_L0_ROW_BATCH, delete_l0).await {
        Ok(stats) => total.absorb(&stats),
        Err(e) => {
            total.failures += 1;
            log::error!("[SEGMENT:GC] L0-orphan pass failed, retrying next tick: {e:#}");
        }
    }
    match gc_segment_pass(
        GC_SEGMENT_MIN_AGE_SECS,
        GC_SEGMENT_DELETE_BATCH,
        delete_segment,
    )
    .await
    {
        Ok(stats) => total.absorb(&stats),
        Err(e) => {
            total.failures += 1;
            log::error!("[SEGMENT:GC] segment-orphan pass failed, retrying next tick: {e:#}");
        }
    }
    if total != GcStats::default() {
        log::info!(
            "[SEGMENT:GC] l0_orphans_deleted={} segment_orphans_deleted={} skipped_young={} failures={}",
            total.l0_orphans_deleted,
            total.segment_orphans_deleted,
            total.skipped_young,
            total.failures
        );
    }
    total
}

/// L0 orphans: rows whose builder died with planned keys still recorded.
/// Per aged row (SQL already excludes anything younger than
/// `min_age_secs` — a potentially-live claim is NEVER touched, so this pass
/// contributes nothing to `skipped_young`): fence the row via
/// `gc_arm_l0_orphan` (a row that changed since listing has a live owner and
/// is skipped whole), then per planned key delete the object ONLY when
/// `file_list` has no row with that exact key — a registered file is NEVER
/// deleted, and an existence-check error fails closed (no delete). A clean
/// row ends with `l0_planned` cleared (plain UPDATE — the row is long-dead
/// and armed) so re-claims start clean; any per-key failure keeps the plan
/// for a later pass instead.
async fn gc_l0_pass<F, Fut>(
    min_age_secs: u64,
    batch: usize,
    delete_object: &F,
) -> Result<GcStats, anyhow::Error>
where
    F: Fn(String) -> Fut,
    Fut: Future<Output = Result<(), object_store::Error>>,
{
    let rows = wal_segments::list_l0_orphan_rows(min_age_secs, batch)
        .await
        .map_err(|e| anyhow::anyhow!("list_l0_orphan_rows(min_age={min_age_secs}s): {e}"))?;
    let mut stats = GcStats::default();
    for (id, planned_json) in rows {
        let keys: Vec<String> = match serde_json::from_str(&planned_json) {
            Ok(keys) => keys,
            Err(e) => {
                // loud, counted, and retried every pass — never a silent
                // skip, and never a delete off keys we cannot trust
                stats.failures += 1;
                log::error!(
                    "[SEGMENT:GC] row id={id} has malformed l0_planned JSON, left untouched: {e}"
                );
                continue;
            }
        };
        match wal_segments::gc_arm_l0_orphan(id, GC_FENCE_NODE, min_age_secs).await {
            Ok(true) => {}
            // the row changed since it was listed (built, re-claimed, or a
            // concurrent GC armed it): a live owner exists — not ours
            Ok(false) => continue,
            Err(e) => {
                stats.failures += 1;
                log::error!("[SEGMENT:GC] arming row id={id} failed, kept for retry: {e}");
                continue;
            }
        }
        let mut row_failed = false;
        for key in &keys {
            match file_list::contains(key).await {
                // registered: the normal file lifecycle owns it — NEVER delete
                Ok(true) => {}
                Ok(false) => {
                    let mut key_failed = false;
                    match delete_object(key.clone()).await {
                        Ok(()) => stats.l0_orphans_deleted += 1,
                        // never uploaded (crash before this key's PUT) or
                        // already collected — both converged states
                        Err(object_store::Error::NotFound { .. }) => {}
                        Err(e) => {
                            key_failed = true;
                            stats.failures += 1;
                            log::error!(
                                "[SEGMENT:GC] L0 orphan delete failed: row id={id} key={key} error={e}"
                            );
                        }
                    }
                    // v3 split: an orphan `.vix` may have uploaded its
                    // `.vxi` sidecar before the crash (data first, sidecar
                    // second) — attempt the derived key too, NotFound
                    // tolerated. Liveness was decided on the DATA key
                    // (sidecars have no file_list row of their own). Skipped
                    // when this key's data delete failed (the retry pass
                    // covers both).
                    if !key_failed && let Some(sidecar) = config::vix_sidecar_key(key, 0) {
                        match delete_object(sidecar.clone()).await {
                            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                            Err(e) => {
                                key_failed = true;
                                stats.failures += 1;
                                log::error!(
                                    "[SEGMENT:GC] L0 orphan sidecar delete failed: row id={id} \
                                     key={sidecar} error={e}"
                                );
                            }
                        }
                    }
                    row_failed |= key_failed;
                }
                Err(e) => {
                    // fail CLOSED: without a trustworthy existence answer the
                    // key might be registered, so it must not be deleted
                    row_failed = true;
                    stats.failures += 1;
                    log::error!(
                        "[SEGMENT:GC] file_list existence check failed (no delete): row id={id} key={key} error={e}"
                    );
                }
            }
        }
        if row_failed {
            // keep l0_planned: the armed row re-ages past the threshold and
            // a later pass retries the whole key list (deletes idempotent)
            continue;
        }
        if let Err(e) = wal_segments::clear_l0_planned(id).await {
            stats.failures += 1;
            log::error!(
                "[SEGMENT:GC] clear_l0_planned id={id} failed (keys already collected; the \
                 retry re-checks file_list and converges): {e}"
            );
        }
    }
    Ok(stats)
}

/// Unregistered segment objects: list the store under `wal_segments/`
/// (client stream — real per-item `Result`s), keep rowless keys older than
/// `min_age_secs` by the store's `last_modified` (segment keys carry no
/// timestamp), and delete them verified per key, at most `delete_cap` per
/// pass.
///
/// Anti-join ordering (an object WITH a row is lifecycle-owned by the
/// normal sweeper, never by the GC): a snapshot of the table's
/// `object_key`s taken BEFORE the listing filters registered keys inline —
/// this bounds the candidate buffer to `delete_cap` and keeps a large
/// unbuilt backlog (many old-but-registered objects) from starving the cap.
/// A SECOND snapshot after the listing re-verifies the candidates, so a row
/// registered mid-listing still shields its object; a register delayed even
/// longer is excluded by the age gate (a flusher retries PUT+register on a
/// backoff of seconds — for its object to reach `min_age_secs` unregistered,
/// the flusher must be dead, which is exactly the orphan class collected
/// here). Both snapshots are deliberately the WHOLE table
/// (`object_keys_under` — small by design, see its doc); a truncated set
/// would make registered objects look orphaned.
async fn gc_segment_pass<F, Fut>(
    min_age_secs: i64,
    delete_cap: usize,
    delete_object: &F,
) -> Result<GcStats, anyhow::Error>
where
    F: Fn(String) -> Fut,
    Fut: Future<Output = Result<(), object_store::Error>>,
{
    let mut stats = GcStats::default();
    if delete_cap == 0 {
        return Ok(stats);
    }
    let db_keys: HashSet<String> = wal_segments::object_keys_under(SEGMENT_KEY_PREFIX)
        .await
        .map_err(|e| anyhow::anyhow!("object_keys_under({SEGMENT_KEY_PREFIX}): {e}"))?
        .into_iter()
        .collect();
    let now = chrono::Utc::now();
    // listed locations carry the bucket prefix on remote stores while the
    // table stores raw keys — strip it for the anti-join (and never delete
    // a key that does not positively resolve under the segment prefix)
    let bucket_prefix = get_config().s3.bucket_prefix.clone();
    let mut aged: Vec<String> = Vec::with_capacity(delete_cap.min(1024));
    {
        let mut listing = SEGMENT_STORE
            .get_client_by_name("")
            .list(Some(&Path::from(SEGMENT_KEY_PREFIX)));
        while let Some(item) = listing.next().await {
            let meta = item.map_err(|e| {
                anyhow::anyhow!("object list under {SEGMENT_KEY_PREFIX} failed: {e}")
            })?;
            let age_secs = now.signed_duration_since(meta.last_modified).num_seconds();
            if age_secs < min_age_secs {
                stats.skipped_young += 1;
                continue;
            }
            let full = meta.location.as_ref();
            let raw = if !bucket_prefix.is_empty() {
                full.strip_prefix(bucket_prefix.as_str()).unwrap_or(full)
            } else {
                full
            };
            if !raw.starts_with(SEGMENT_KEY_PREFIX) {
                stats.failures += 1;
                log::error!(
                    "[SEGMENT:GC] listed key {full:?} does not resolve under {SEGMENT_KEY_PREFIX} \
                     after bucket-prefix strip; refusing to touch it"
                );
                continue;
            }
            if db_keys.contains(raw) {
                // registered: not the GC's to touch
                continue;
            }
            aged.push(raw.to_string());
            if aged.len() >= delete_cap {
                log::info!(
                    "[SEGMENT:GC] segment-orphan candidate cap {delete_cap} hit; the next pass continues"
                );
                break;
            }
        }
    }
    if aged.is_empty() {
        return Ok(stats);
    }

    // re-verify against a fresh snapshot: a row registered while the
    // listing ran must shield its object
    let db_keys: HashSet<String> = wal_segments::object_keys_under(SEGMENT_KEY_PREFIX)
        .await
        .map_err(|e| anyhow::anyhow!("object_keys_under({SEGMENT_KEY_PREFIX}) recheck: {e}"))?
        .into_iter()
        .collect();
    let candidates: Vec<String> = aged
        .into_iter()
        .filter(|key| !db_keys.contains(key))
        .collect();

    let mut results = futures::stream::iter(candidates.into_iter().map(move |key| async move {
        let ret = delete_object(key.clone()).await;
        (key, ret)
    }))
    .buffer_unordered(DELETE_CONCURRENCY);
    while let Some((key, ret)) = results.next().await {
        match ret {
            // NotFound: another sweeper got there first — equally collected
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {
                stats.segment_orphans_deleted += 1;
            }
            Err(e) => {
                stats.failures += 1;
                log::error!(
                    "[SEGMENT:GC] segment orphan delete failed, retried next pass: key={key} error={e}"
                );
            }
        }
    }
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use config::utils::time::now_micros;
    use infra::{
        db::{ORM_CLIENT, connect_to_orm},
        wal_segments::{SEGMENT_KEY_PREFIX, SegmentMeta, SegmentStatus},
    };
    use sea_orm::{ConnectionTrait, FromQueryResult, Statement};

    use super::*;

    /// All tests share the process-global sqlite metadata db (same pattern as
    /// the `infra::wal_segments` tests), so they serialize on this lock. Rows
    /// are namespaced by [`NODE_PREFIX`] and each setup clears ONLY that
    /// namespace — other test modules' rows are never touched.
    static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    const NODE_PREFIX: &str = "swp-test-";
    const T0: i64 = 1_700_000_000_000_000;
    /// Test retain window (90 min). Fresh rows created by unrelated tests in
    /// this binary have `updated_at ~ now`, so they can never look expired to
    /// a sweep pass using this window — only rows this module deliberately
    /// ages beyond it are examined.
    const RETAIN_SECS: u64 = 5400;

    async fn orm() -> &'static sea_orm::DatabaseConnection {
        ORM_CLIENT.get_or_init(connect_to_orm).await
    }

    async fn raw_exec(sql: &str) {
        orm()
            .await
            .execute_unprepared(sql)
            .await
            .unwrap_or_else(|e| panic!("raw exec {sql:?} failed: {e}"));
    }

    async fn setup() -> tokio::sync::MutexGuard<'static, ()> {
        let guard = TEST_LOCK.lock().await;
        // mirror db_init: the sqlite file lives under data_db_dir
        std::fs::create_dir_all(&get_config().common.data_db_dir)
            .expect("create data_db_dir for tests");
        wal_segments::create_table()
            .await
            .expect("create wal_segments table");
        raw_exec(&format!(
            "DELETE FROM wal_segments WHERE node_uuid LIKE '{NODE_PREFIX}%';"
        ))
        .await;
        guard
    }

    fn seg_key(node: &str, seq: i64) -> String {
        format!("{SEGMENT_KEY_PREFIX}{node}/{seq:020}")
    }

    async fn add_row(node: &str, seq: i64) -> (i64, String) {
        let key = seg_key(node, seq);
        let meta = SegmentMeta {
            id: 0,
            node_uuid: node.to_string(),
            seq,
            object_key: key.clone(),
            min_ts: T0,
            max_ts: T0 + 1000,
            size: 128,
            streams: vec!["org1/logs/app1".to_string()],
            status: SegmentStatus::Pending,
            builder_node: String::new(),
            created_at: 0,
            updated_at: 0,
        };
        let id = wal_segments::add(&meta)
            .await
            .unwrap_or_else(|e| panic!("add segment {key} failed: {e}"));
        (id, key)
    }

    /// Register a segment and force it straight to `Built` with an
    /// `updated_at` of `built_age_secs` ago — surgical raw UPDATE by id, so
    /// no other module's rows are ever touched (claim_pending/mark_built
    /// would grab foreign pending rows).
    async fn add_built_row(node: &str, seq: i64, built_age_secs: u64) -> (i64, String) {
        let (id, key) = add_row(node, seq).await;
        let ts = now_micros() - (built_age_secs as i64) * 1_000_000;
        raw_exec(&format!(
            "UPDATE wal_segments SET status = 2, updated_at = {ts} WHERE id = {id};"
        ))
        .await;
        (id, key)
    }

    async fn row_exists(id: i64) -> bool {
        #[derive(FromQueryResult)]
        struct CountRow {
            n: i64,
        }
        let client = orm().await;
        let stmt = Statement::from_string(
            client.get_database_backend(),
            format!("SELECT count(*) AS n FROM wal_segments WHERE id = {id};"),
        );
        let row = CountRow::find_by_statement(stmt)
            .one(client)
            .await
            .unwrap_or_else(|e| panic!("count row {id} failed: {e}"))
            .expect("count query returned no row");
        row.n > 0
    }

    fn not_found_err(key: &str) -> object_store::Error {
        object_store::Error::NotFound {
            path: key.to_string(),
            source: "object already gone".into(),
        }
    }

    fn s3_err() -> object_store::Error {
        object_store::Error::Generic {
            store: "test-s3",
            source: "simulated s3 500".into(),
        }
    }

    #[tokio::test]
    async fn test_sweep_removes_only_confirmed_ids_and_failed_rows_stay() {
        let _guard = setup().await;
        let node = format!("{NODE_PREFIX}confirm");
        let (id_ok, key_ok) = add_built_row(&node, 1, 7200).await;
        let (id_gone, key_gone) = add_built_row(&node, 2, 7200).await;
        let (id_fail, key_fail) = add_built_row(&node, 3, 7200).await;

        // scripted per-key outcomes; unknown keys (a concurrent module's
        // rows, should any ever qualify) fail closed and keep their rows
        let calls: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let deleter = {
            let calls = calls.clone();
            let (key_ok, key_gone, key_fail) = (key_ok.clone(), key_gone.clone(), key_fail.clone());
            move |key: String| {
                let calls = calls.clone();
                let (key_ok, key_gone, key_fail) =
                    (key_ok.clone(), key_gone.clone(), key_fail.clone());
                async move {
                    calls.lock().unwrap().push(key.clone());
                    if key == key_ok {
                        Ok(())
                    } else if key == key_gone {
                        // NotFound counts as success: idempotent re-sweep
                        Err(not_found_err(&key))
                    } else if key == key_fail {
                        Err(s3_err())
                    } else {
                        Err(s3_err())
                    }
                }
            }
        };

        let stats = sweep_pass(RETAIN_SECS, 100, &deleter).await.unwrap();
        assert_eq!(stats.deleted, 1, "one confirmed hard delete");
        assert_eq!(stats.not_found, 1, "NotFound counted as success");
        assert_eq!(stats.failed, 1, "failed S3 delete reported");
        assert!(stats.examined >= 3, "all three aged rows examined");

        assert!(!row_exists(id_ok).await, "confirmed row must be removed");
        assert!(!row_exists(id_gone).await, "NotFound row must be removed");
        assert!(row_exists(id_fail).await, "failed delete must keep the row");

        // every examined key of ours was attempted exactly once
        let seen = calls.lock().unwrap().clone();
        for key in [&key_ok, &key_gone, &key_fail] {
            assert_eq!(
                seen.iter().filter(|k| *k == key).count(),
                1,
                "expected exactly one delete attempt for {key}"
            );
        }

        // the failed row is re-listed and converges once the store recovers
        let retry_deleter = |_key: String| async move { Ok(()) };
        let stats = sweep_pass(RETAIN_SECS, 100, &retry_deleter).await.unwrap();
        assert!(stats.deleted >= 1, "retry pass deletes the failed row");
        assert!(!row_exists(id_fail).await, "row removed after retry");
    }

    #[tokio::test]
    async fn test_sweep_skips_fresh_built_and_unbuilt_rows() {
        let _guard = setup().await;
        let node = format!("{NODE_PREFIX}skip");
        // fresh built row: inside the retain window
        let (id_fresh, _) = add_built_row(&node, 1, 60).await;
        // OLD but never built: building it is the builder's job — sweeping
        // it would destroy data that has no L0 file yet
        let (id_pending, _) = add_row(&node, 2).await;
        let old = now_micros() - 7200 * 1_000_000;
        raw_exec(&format!(
            "UPDATE wal_segments SET updated_at = {old}, created_at = {old} WHERE id = {id_pending};"
        ))
        .await;

        let deletes = Arc::new(AtomicUsize::new(0));
        let deleter = {
            let deletes = deletes.clone();
            move |_key: String| {
                let deletes = deletes.clone();
                async move {
                    deletes.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        };

        let stats = sweep_pass(RETAIN_SECS, 100, &deleter).await.unwrap();
        assert_eq!(stats.examined, 0, "nothing qualifies for the sweep");
        assert_eq!(stats, SweepStats::default());
        assert_eq!(deletes.load(Ordering::SeqCst), 0, "no delete attempted");
        assert!(row_exists(id_fresh).await);
        assert!(row_exists(id_pending).await);
    }

    #[tokio::test]
    async fn test_sweep_batch_cap_drains_oldest_first() {
        let _guard = setup().await;
        let node = format!("{NODE_PREFIX}batch");
        let (id_oldest, _) = add_built_row(&node, 1, 10800).await;
        let (id_mid, _) = add_built_row(&node, 2, 9000).await;
        let (id_newest, _) = add_built_row(&node, 3, 7200).await;

        let ok_deleter = |_key: String| async move { Ok(()) };

        // batch of 2 takes the two oldest and leaves the newest
        let stats = sweep_pass(RETAIN_SECS, 2, &ok_deleter).await.unwrap();
        assert_eq!(stats.examined, 2);
        assert_eq!(stats.deleted, 2);
        assert!(!row_exists(id_oldest).await, "oldest swept first");
        assert!(!row_exists(id_mid).await, "second-oldest swept");
        assert!(row_exists(id_newest).await, "newest deferred to next pass");

        // next pass drains the remainder
        let stats = sweep_pass(RETAIN_SECS, 2, &ok_deleter).await.unwrap();
        assert_eq!(stats.examined, 1);
        assert!(!row_exists(id_newest).await);

        // batch 0 short-circuits inside list_expired
        let stats = sweep_pass(RETAIN_SECS, 0, &ok_deleter).await.unwrap();
        assert_eq!(stats, SweepStats::default());
    }

    #[tokio::test]
    async fn test_sweep_tick_drains_backlog_across_passes() {
        let _guard = setup().await;
        let node = format!("{NODE_PREFIX}tick");
        // 5 aged rows against a batch of 2: the tick must drain them in one
        // call (2 + 2 + 1), stopping on the pass that comes back short
        let mut ids = Vec::with_capacity(5);
        for seq in 1..=5 {
            let (id, _) = add_built_row(&node, seq, 7200).await;
            ids.push(id);
        }
        let deletes = Arc::new(AtomicUsize::new(0));
        let deleter = {
            let deletes = deletes.clone();
            move |_key: String| {
                let deletes = deletes.clone();
                async move {
                    deletes.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        };

        let (passes, total) = sweep_tick(RETAIN_SECS, 2, MAX_PASSES_PER_TICK, &deleter).await;
        assert_eq!(passes, 3, "2 + 2 + 1 rows across three passes");
        assert_eq!(total.examined, 5);
        assert_eq!(total.deleted, 5);
        assert_eq!(total.not_found, 0);
        assert_eq!(total.failed, 0);
        assert_eq!(deletes.load(Ordering::SeqCst), 5, "one delete per row");
        for id in ids {
            assert!(!row_exists(id).await, "row {id} must be swept");
        }

        // a drained backlog costs exactly one (empty) pass on the next tick
        let (passes, total) = sweep_tick(RETAIN_SECS, 2, MAX_PASSES_PER_TICK, &deleter).await;
        assert_eq!(passes, 1);
        assert_eq!(total, SweepStats::default());
    }

    #[tokio::test]
    async fn test_sweep_tick_pass_ceiling_bounds_the_tick() {
        let _guard = setup().await;
        let node = format!("{NODE_PREFIX}ceil");
        let (id_a, _) = add_built_row(&node, 1, 10800).await;
        let (id_b, _) = add_built_row(&node, 2, 9000).await;
        let (id_c, _) = add_built_row(&node, 3, 7200).await;
        let ok_deleter = |_key: String| async move { Ok(()) };

        // batch 1 with a ceiling of 2: the tick stops at the ceiling with
        // backlog remaining, oldest first
        let (passes, total) = sweep_tick(RETAIN_SECS, 1, 2, &ok_deleter).await;
        assert_eq!(passes, 2, "ceiling stops the tick");
        assert_eq!(total.deleted, 2);
        assert!(!row_exists(id_a).await);
        assert!(!row_exists(id_b).await);
        assert!(row_exists(id_c).await, "ceiling defers the rest");

        // the next tick drains the remainder
        let (_, total) = sweep_tick(RETAIN_SECS, 1, 2, &ok_deleter).await;
        assert_eq!(total.deleted, 1);
        assert!(!row_exists(id_c).await);
    }

    #[tokio::test]
    async fn test_sweep_tick_stops_on_full_page_with_no_confirms() {
        let _guard = setup().await;
        let node = format!("{NODE_PREFIX}noconf");
        let (id_a, _) = add_built_row(&node, 1, 7200).await;
        let (id_b, _) = add_built_row(&node, 2, 7200).await;
        let attempts = Arc::new(AtomicUsize::new(0));
        let failing_deleter = {
            let attempts = attempts.clone();
            move |_key: String| {
                let attempts = attempts.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Err(s3_err())
                }
            }
        };

        // a full page (batch 1) where nothing confirms must stop after one
        // pass instead of re-listing the same failed row up to the ceiling
        let (passes, total) = sweep_tick(RETAIN_SECS, 1, 20, &failing_deleter).await;
        assert_eq!(passes, 1, "no-progress tick must not spin");
        assert_eq!(total.failed, 1);
        assert_eq!(total.deleted + total.not_found, 0);
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert!(row_exists(id_a).await, "failed rows stay for the next tick");
        assert!(row_exists(id_b).await);
    }

    // ─── crashed-build GC ────────────────────────────────────────────────

    /// GC-processable row: `Building` under a (dead) builder with planned
    /// keys and an `updated_at` of `age_secs` ago. Surgical raw UPDATE by id
    /// (same reason as [`add_built_row`]).
    async fn add_planned_row(node: &str, seq: i64, keys: &[String], age_secs: i64) -> i64 {
        let (id, _key) = add_row(node, seq).await;
        let planned = serde_json::to_string(keys).expect("serialize planned keys");
        let ts = now_micros() - age_secs * 1_000_000;
        raw_exec(&format!(
            "UPDATE wal_segments SET status = 1, builder_node = 'swp-gc-dead', \
             l0_planned = '{planned}', updated_at = {ts} WHERE id = {id};"
        ))
        .await;
        id
    }

    /// (status, builder_node, l0_planned) straight from the DB.
    async fn gc_row(id: i64) -> (i64, String, String) {
        #[derive(FromQueryResult)]
        struct GcRow {
            status: i64,
            builder_node: String,
            l0_planned: String,
        }
        let client = orm().await;
        let stmt = Statement::from_string(
            client.get_database_backend(),
            format!("SELECT status, builder_node, l0_planned FROM wal_segments WHERE id = {id};"),
        );
        let row = GcRow::find_by_statement(stmt)
            .one(client)
            .await
            .unwrap_or_else(|e| panic!("gc row {id} failed: {e}"))
            .unwrap_or_else(|| panic!("gc row {id} missing"));
        (row.status, row.builder_node, row.l0_planned)
    }

    async fn force_row_age(id: i64, age_secs: i64) {
        let ts = now_micros() - age_secs * 1_000_000;
        raw_exec(&format!(
            "UPDATE wal_segments SET updated_at = {ts} WHERE id = {id};"
        ))
        .await;
    }

    fn l0_key(org: &str, name: &str) -> String {
        format!("files/{org}/logs/app1/2021/01/01/11/{name}.vix")
    }

    async fn put_object(key: &str) {
        infra::storage::put("", key, bytes::Bytes::from_static(b"l0-or-segment-bytes"))
            .await
            .unwrap_or_else(|e| panic!("put {key} failed: {e}"));
    }

    async fn object_exists(key: &str) -> bool {
        infra::storage::head("", key).await.is_ok()
    }

    async fn file_list_setup() {
        infra::file_list::create_table()
            .await
            .expect("create file_list table");
        infra::file_list::create_table_index()
            .await
            .expect("create file_list indexes");
    }

    async fn register_file(key: &str) {
        let file = config::meta::stream::FileKey::new(
            0,
            String::new(),
            key.to_string(),
            config::meta::stream::FileMeta {
                min_ts: T0,
                max_ts: T0 + 1000,
                records: 10,
                original_size: 1000,
                compressed_size: 100,
                ..Default::default()
            },
            false,
        );
        infra::file_list::batch_add(&[file])
            .await
            .unwrap_or_else(|e| panic!("register {key} failed: {e}"));
    }

    /// The two safety invariants of the L0 pass in one scenario: a
    /// registered key is NEVER deleted (exact-key file_list check) and a row
    /// younger than the dead threshold is NEVER touched; the dead row's
    /// unregistered key is collected and its plan cleared through the real
    /// local-disk store.
    #[tokio::test]
    async fn test_gc_l0_orphan_deletes_only_unregistered_keys_of_dead_rows() {
        let _guard = setup().await;
        file_list_setup().await;
        let node = format!("{NODE_PREFIX}gcl0");
        let org = format!("gcorg{}", now_micros());

        let key_orph = l0_key(&org, "l0_dead_1_1_447083");
        let key_reg = l0_key(&org, "l0_dead_1_1_447084");
        let key_young = l0_key(&org, "l0_live_2_2_447083");
        put_object(&key_orph).await;
        put_object(&key_reg).await;
        put_object(&key_young).await;
        register_file(&key_reg).await;

        // dead row: 2h without a heartbeat >> 3 * lease
        let id_dead = add_planned_row(&node, 1, &[key_orph.clone(), key_reg.clone()], 7200).await;
        // live row: same shape, fresh lease (builder mid-upload RIGHT NOW)
        let id_live = add_planned_row(&node, 2, &[key_young.clone()], 0).await;

        let stats = gc_l0_pass(360, GC_L0_ROW_BATCH, &delete_l0_object)
            .await
            .unwrap();
        assert_eq!(stats.l0_orphans_deleted, 1, "only the unregistered key");
        assert_eq!(stats.failures, 0, "{stats:?}");

        assert!(!object_exists(&key_orph).await, "orphan must be deleted");
        assert!(object_exists(&key_reg).await, "registered key must survive");
        assert!(object_exists(&key_young).await, "live builder never raced");

        let (status, builder, planned) = gc_row(id_dead).await;
        assert_eq!(status, 1, "GC must not change status");
        assert_eq!(builder, GC_FENCE_NODE, "processed row must be armed");
        assert_eq!(planned, "", "processed row's plan must be cleared");
        let (_, live_builder, live_planned) = gc_row(id_live).await;
        assert_eq!(live_builder, "swp-gc-dead", "young row untouched");
        assert_eq!(
            live_planned,
            serde_json::to_string(&[key_young.clone()]).unwrap(),
            "young row's plan untouched"
        );

        // second pass: the cleared row is invisible, the live row still
        // young — nothing left to do
        let stats = gc_l0_pass(360, GC_L0_ROW_BATCH, &delete_l0_object)
            .await
            .unwrap();
        assert_eq!(stats, GcStats::default());
    }

    /// A failed object delete keeps the plan for retry; the armed (fresh)
    /// row is not re-processed inside the lease window, and once re-aged the
    /// retry converges.
    #[tokio::test]
    async fn test_gc_l0_orphan_failed_delete_keeps_plan_then_converges() {
        let _guard = setup().await;
        file_list_setup().await;
        let node = format!("{NODE_PREFIX}gcfail");
        let org = format!("gcorg{}", now_micros());
        let key = l0_key(&org, "l0_dead_9_9_447083");
        let planned = serde_json::to_string(&[key.clone()]).unwrap();
        let id = add_planned_row(&node, 1, &[key.clone()], 7200).await;

        let failing = |_key: String| async move { Err(s3_err()) };
        let stats = gc_l0_pass(360, GC_L0_ROW_BATCH, &failing).await.unwrap();
        assert_eq!(stats.failures, 1);
        assert_eq!(stats.l0_orphans_deleted, 0);
        let (_, builder, kept) = gc_row(id).await;
        assert_eq!(builder, GC_FENCE_NODE);
        assert_eq!(kept, planned, "failed row must keep its plan");

        // arming refreshed the lease: within the window the row is a live
        // owner's and must not be re-processed
        let deletes = Arc::new(AtomicUsize::new(0));
        let counting = {
            let deletes = deletes.clone();
            move |_key: String| {
                let deletes = deletes.clone();
                async move {
                    deletes.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        };
        let stats = gc_l0_pass(360, GC_L0_ROW_BATCH, &counting).await.unwrap();
        assert_eq!(stats, GcStats::default(), "armed row must age out first");
        assert_eq!(deletes.load(Ordering::SeqCst), 0);

        // once dead again, the retry collects the key and clears the plan.
        // v3 split: collecting a `.vix` orphan also attempts its derived
        // `.vxi` sidecar key — two delete calls, one counted orphan.
        force_row_age(id, 7200).await;
        let stats = gc_l0_pass(360, GC_L0_ROW_BATCH, &counting).await.unwrap();
        assert_eq!(stats.l0_orphans_deleted, 1);
        assert_eq!(deletes.load(Ordering::SeqCst), 2);
        assert_eq!(gc_row(id).await.2, "", "plan cleared after the retry");
    }

    /// A malformed plan is loud (counted failure), never a delete, never a
    /// silent clear.
    #[tokio::test]
    async fn test_gc_l0_orphan_malformed_plan_is_loud_and_untouched() {
        let _guard = setup().await;
        let node = format!("{NODE_PREFIX}gcbad");
        let (id, _) = add_row(&node, 1).await;
        let ts = now_micros() - 7200 * 1_000_000;
        raw_exec(&format!(
            "UPDATE wal_segments SET status = 1, builder_node = 'swp-gc-dead', \
             l0_planned = 'not-json', updated_at = {ts} WHERE id = {id};"
        ))
        .await;
        let deletes = Arc::new(AtomicUsize::new(0));
        let counting = {
            let deletes = deletes.clone();
            move |_key: String| {
                let deletes = deletes.clone();
                async move {
                    deletes.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        };
        let stats = gc_l0_pass(360, GC_L0_ROW_BATCH, &counting).await.unwrap();
        assert_eq!(stats.failures, 1);
        assert_eq!(deletes.load(Ordering::SeqCst), 0, "no delete off bad keys");
        assert_eq!(gc_row(id).await.2, "not-json", "row left for the operator");
    }

    /// Segment-prefix anti-join through the real local-disk store: only
    /// rowless aged objects are deleted; an object with a `wal_segments` row
    /// is lifecycle-owned by the normal sweeper and survives, and young
    /// rowless objects wait out the age gate.
    #[tokio::test]
    async fn test_gc_segment_orphans_deletes_only_rowless_aged_objects() {
        let _guard = setup().await;
        assert!(
            config::is_local_disk_storage(),
            "test requires the local-disk object store"
        );
        let node = format!("{NODE_PREFIX}gcseg");

        // rowless orphan (crash between PUT and register)
        let key_orph = seg_key(&node, 91);
        put_object(&key_orph).await;
        // registered segment: row exists, object exists
        let (id_reg, key_reg) = add_row(&node, 92).await;
        put_object(&key_reg).await;

        // age gate first: with a huge min age EVERYTHING is young — nothing
        // may be deleted, and the young orphan is counted
        let stats = gc_segment_pass(
            1_000_000_000,
            GC_SEGMENT_DELETE_BATCH,
            &delete_segment_object,
        )
        .await
        .unwrap();
        assert_eq!(stats.segment_orphans_deleted, 0);
        assert!(stats.skipped_young >= 2, "{stats:?}");
        assert!(object_exists(&key_orph).await);

        // min age 0: the rowless object goes, the registered one survives
        let stats = gc_segment_pass(0, GC_SEGMENT_DELETE_BATCH, &delete_segment_object)
            .await
            .unwrap();
        assert!(stats.segment_orphans_deleted >= 1, "{stats:?}");
        assert_eq!(stats.failures, 0, "{stats:?}");
        assert!(!object_exists(&key_orph).await, "rowless orphan deleted");
        assert!(object_exists(&key_reg).await, "registered object survives");
        assert!(row_exists(id_reg).await, "row untouched by the GC");
    }

    /// The per-pass delete cap bounds the segment pass; the next pass
    /// continues where it stopped.
    #[tokio::test]
    async fn test_gc_segment_orphans_bounded_per_pass() {
        let _guard = setup().await;
        assert!(config::is_local_disk_storage());
        let node = format!("{NODE_PREFIX}gccap");
        let key_a = seg_key(&node, 81);
        let key_b = seg_key(&node, 82);
        put_object(&key_a).await;
        put_object(&key_b).await;

        // cap 1: exactly one aged rowless object per pass (whichever the
        // listing yields first — leftovers from earlier runs also qualify)
        let stats = gc_segment_pass(0, 1, &delete_segment_object).await.unwrap();
        assert_eq!(stats.segment_orphans_deleted, 1, "{stats:?}");
        assert!(
            object_exists(&key_a).await || object_exists(&key_b).await,
            "at most one delete happened"
        );

        // an uncapped pass drains the rest
        let stats = gc_segment_pass(0, GC_SEGMENT_DELETE_BATCH, &delete_segment_object)
            .await
            .unwrap();
        assert_eq!(stats.failures, 0, "{stats:?}");
        assert!(!object_exists(&key_a).await);
        assert!(!object_exists(&key_b).await);

        // cap 0 is a no-op guard, not an accidental full pass
        put_object(&key_a).await;
        let stats = gc_segment_pass(0, 0, &delete_segment_object).await.unwrap();
        assert_eq!(stats, GcStats::default());
        assert!(object_exists(&key_a).await);
        // clean up our leftover so later runs start from nothing
        delete_segment_object(key_a).await.unwrap();
    }

    #[tokio::test]
    async fn test_sweep_real_local_disk_store_deletes_object_and_treats_missing_as_gone() {
        let _guard = setup().await;
        assert!(
            config::is_local_disk_storage(),
            "test requires the local-disk object store"
        );
        let node = format!("{NODE_PREFIX}disk");

        // row 1: object really exists on the local-disk store (written the
        // same way the flusher writes it: default account, raw key)
        let (id_real, key_real) = add_built_row(&node, 1, 7200).await;
        infra::storage::put("", &key_real, bytes::Bytes::from_static(b"segment-bytes"))
            .await
            .unwrap_or_else(|e| panic!("put {key_real} failed: {e}"));
        assert!(
            infra::storage::head("", &key_real).await.is_ok(),
            "object must exist before the sweep"
        );

        // row 2: object never written — the real store returns NotFound,
        // which must count as confirmed
        let (id_missing, key_missing) = add_built_row(&node, 2, 7200).await;

        let stats = sweep_pass(RETAIN_SECS, 100, &delete_segment_object)
            .await
            .unwrap();
        assert!(
            stats.deleted >= 1,
            "real object confirmed deleted: {stats:?}"
        );
        assert!(
            stats.not_found >= 1,
            "missing object counted as gone: {stats:?}"
        );
        assert_eq!(stats.failed, 0, "no failures expected: {stats:?}");

        assert!(!row_exists(id_real).await, "row for deleted object removed");
        assert!(
            !row_exists(id_missing).await,
            "row for missing object removed"
        );
        match infra::storage::head("", &key_real).await {
            Err(object_store::Error::NotFound { .. }) => {}
            other => panic!("object {key_real} must be gone after sweep, got {other:?}"),
        }
        // key_missing never existed; a re-check keeps the invariant honest
        assert!(infra::storage::head("", &key_missing).await.is_err());
    }
}
