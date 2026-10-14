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

//! Group `.bf` assembler — the write side of the transposed bloom layout
//! (`infra::bloom`; the read side, `bloom_pruner`, has been live since the
//! format landed).
//!
//! Per pending `(stream, hour)` bucket (file_list rows with
//! `index_size > 0 AND bloom_ver = 0`):
//!
//! - files written since the per-file bloom capability carry a `bloom` puffin blob (a byproduct of
//!   term emission) — the assembler reads the blob (one ranged fetch) and TRANSPOSES its raw SBBF
//!   blocks into the group `.bf`, never re-reading dictionaries or re-hashing values;
//! - older files fall back to ONE full term-dictionary stream that hashes additive per-field values
//!   plus composite values from term-capable fields admitted by the explicit/broad composite scope
//!   or the enabled semantic AUTO ID scope;
//! - streams with neither additive `bloom_filter_fields` nor an enabled composite/AUTO ID scope are
//!   stamped with the NO_BLOOM sentinel so the queue drains (the pruner treats `bloom_ver <= 0` as
//!   "no bloom", never forming a `.bf` path);
//! - files whose own bytes fail validation (a DETERMINISTIC failure — corrupt dictionary/terms/
//!   bloom blob, or a checked build that refuses to publish) are stamped [`BLOOM_VER_UNBUILDABLE`]:
//!   retrying can never succeed, so they leave the queue after one attempt instead of spinning
//!   forever and burning the fallback budget every pass.
//!
//! Group invariants (see `infra::bloom`): every field section shares one
//! `num_blocks B` across its files, so files are chunked by their per-field
//! B signature (writer sizing is power-of-two, so real hours produce a
//! couple of chunks at most); each chunk becomes its own `.bf` named
//! `bloom_ver = base_ts + chunk_idx`, and `update_bloom_ver` stamps the
//! chunk's file_list rows LAST — a crash before the stamp just re-queues.

use std::sync::Arc;

use config::{
    get_config,
    meta::stream::{FileKey, FileListDeleted},
    utils::time::now_micros,
};
use hashbrown::{HashMap, HashSet};
use infra::{
    bloom::{BloomWriter, FieldBloom, GroupMember},
    dist_lock,
    schema::{get_stream_setting_bloom_filter_fields, unwrap_stream_settings},
};
use vortex_index::VixReader;

use super::merge::HealProbeRangeSource;

/// `bloom_ver` sentinel for "processed, no bloom applicable" (stream has no
/// bloom fields configured, or the file has none of them). The pruner
/// passes `bloom_ver <= 0` files through untouched, and the builder queue
/// (`bloom_ver = 0`) no longer matches — the bucket drains.
const BLOOM_VER_NOT_APPLICABLE: i64 = -1;

/// `bloom_ver` sentinel for "deterministically unbuildable": the file's own
/// bytes fail validation (corrupt dictionary/terms/bloom blob, or a checked
/// build that refuses to publish), so retrying can never succeed. Without
/// the stamp such a file re-queues FOREVER — and re-burns a fallback-budget
/// slot every pass, starving healthy files. Like every `bloom_ver <= 0` the
/// pruner keeps the file un-pruned (queries stay correct, just slower for
/// needle lookups), and the pending queue (`bloom_ver = 0`) no longer
/// matches, so the poison pill is retried at most once. Stamping is a
/// single idempotent UPDATE: a crash before it lands just re-classifies the
/// file on the next pass.
const BLOOM_VER_UNBUILDABLE: i64 = -2;

/// A superseded `.bf` version whose rows all moved to new chunks is deleted
/// through the deferred `FileListDeleted` batch, never inline; the GC pass
/// catches anything a crash left behind (`ZO_COMPACT_BLOOM_GC_MIN_AGE_SECS`).
type BloomFence = (i64, i64, i64);

/// One pass of the builder: drain up to `compact.bloom_build_batch` pending
/// `(stream, hour)` buckets. Called on the compactor's job cadence.
pub async fn run() -> Result<(), anyhow::Error> {
    let cfg = get_config();
    if !cfg.common.bloom_filter_enabled {
        return Ok(());
    }
    // Only SETTLED hours: the open hour and the live merge lane
    // (`ZO_COMPACT_LIVE_LOOKBACK_HOURS`) churn — their L0s merge away within
    // the lane and every merge output re-pends at `bloom_ver = 0`, so a `.bf`
    // built there is rebuilt several times and mostly orphaned. Measured
    // 2026-09-28: the two live traces hours held 2,100 of 3,200 pending
    // files in 2 of 646 buckets; sitting at the head of the date-DESC queue
    // they turned every pass into an hour of blob fetches under one lock
    // while 29 other compactors timed out on it. The query side probes those
    // files' own sidecars (`bloom_pruner::file_probe`) until they settle.
    let settled_before =
        settled_hour_cutoff(config::utils::time::now(), cfg.compact.live_lookback_hours);
    let mut buckets = infra::file_list::query_bloom_pending_buckets(
        &settled_before,
        cfg.compact.bloom_build_batch,
    )
    .await?;
    // Every compactor runs this pass on the same cadence over the same
    // date-DESC list; walking it in a node-local order keeps them from
    // queueing on the same bucket lock and lets one pass cover the batch.
    {
        use rand::{SeedableRng, rngs::StdRng, seq::SliceRandom};
        buckets.shuffle(&mut StdRng::seed_from_u64(rand::random()));
    }
    let total = buckets.len();
    let started = std::time::Instant::now();
    FALLBACK_BUDGET.store(
        cfg.compact.bloom_build_fallback_budget,
        std::sync::atomic::Ordering::Relaxed,
    );
    REPACK_BUDGET.store(
        cfg.compact.sidecar_repack_budget,
        std::sync::atomic::Ordering::Relaxed,
    );
    let composite_scope = config::vix_bloom_composite_scope(&cfg);
    let auto_id_scope =
        cfg.common.vix_bloom_only_auto_id_only && cfg.common.vix_bloom_only_auto_ratio > 0.0;
    let bloom_only_never = Arc::new(
        cfg.common
            .vix_bloom_only_never
            .split(',')
            .map(str::trim)
            .filter(|field| !field.is_empty())
            .map(str::to_owned)
            .collect::<HashSet<_>>(),
    );
    let (mut done, mut busy, mut failed) = (0usize, 0usize, 0usize);
    for (stream_key, date) in buckets {
        let Some((org_id, stream_type, stream_name)) = parse_stream_key(&stream_key) else {
            log::warn!("[COMPACTOR:BLOOM] unparsable stream key {stream_key:?}, skipping");
            continue;
        };
        match process_bucket(
            &org_id,
            &stream_type,
            &stream_name,
            &date,
            cfg.common.vix_bloom_fpp,
            &composite_scope,
            auto_id_scope,
            &bloom_only_never,
        )
        .await
        {
            Ok(true) => done += 1,
            Ok(false) => busy += 1,
            Err(e) => {
                failed += 1;
                log::error!(
                    "[COMPACTOR:BLOOM] {stream_key}/{date}: build failed (kept queued): {e:#}"
                );
            }
        }
    }
    if total > 0 {
        log::info!(
            "[COMPACTOR:BLOOM] pass: {done}/{total} buckets processed ({busy} busy elsewhere, \
             {failed} failed) in {:?}, settled before {settled_before}",
            started.elapsed()
        );
    }
    Ok(())
}

/// The `date` (`YYYY/MM/DD/HH`) before which hours count as settled for the
/// `.bf` queue: `now − live_lookback_hours`, floored to the hour. A lookback
/// of 0 excludes only the open hour (the pre-2026-09-28 behavior).
fn settled_hour_cutoff(now: chrono::DateTime<chrono::Utc>, live_lookback_hours: i64) -> String {
    let lookback = chrono::Duration::hours(live_lookback_hours.max(0));
    (now - lookback).format("%Y/%m/%d/%H").to_string()
}

fn parse_stream_key(stream_key: &str) -> Option<(String, String, String)> {
    let mut parts = stream_key.splitn(3, '/');
    Some((
        parts.next()?.to_string(),
        parts.next()?.to_string(),
        parts.next()?.to_string(),
    ))
}

/// Returns Ok(true) when the bucket was processed, Ok(false) when its lock
/// was busy (another compactor owns it or a leaked lock is aging out).
async fn process_bucket(
    org_id: &str,
    stream_type: &str,
    stream_name: &str,
    date: &str,
    fpp: f64,
    composite_scope: &config::VixBloomCompositeScope,
    auto_id_scope: bool,
    bloom_only_never: &Arc<HashSet<String>>,
) -> Result<bool, anyhow::Error> {
    let stream_type: config::meta::stream::StreamType = stream_type.into();

    // One builder per bucket across the compactor fleet. BOUNDED wait:
    // wait-forever (0) serialized every compactor on the same bucket and
    // froze the whole fleet for ZO_NATS_LOCK_MAX_AGE whenever a rollout
    // killed a holder mid-bucket (leaked lock). Busy/leaked -> skip; the
    // bucket stays queued for a later pass.
    let lock_key = format!("/compact/bloom/{org_id}/{stream_type}/{stream_name}/{date}");
    let locker = match dist_lock::lock(&lock_key, 3).await {
        Ok(l) => l,
        Err(e) => {
            log::debug!(
                "[COMPACTOR:BLOOM] {org_id}/{stream_type}/{stream_name}/{date}: bucket lock \
                 busy ({e}), skipping this pass"
            );
            return Ok(false);
        }
    };
    let result = process_bucket_locked(
        org_id,
        stream_type,
        stream_name,
        date,
        fpp,
        composite_scope,
        auto_id_scope,
        bloom_only_never,
    )
    .await;
    dist_lock::unlock(&locker).await?;
    result.map(|_| true)
}

fn composite_scope_allows(
    composite_scope: &config::VixBloomCompositeScope,
    auto_id_scope: bool,
    bloom_only_never: &HashSet<String>,
    field: &str,
) -> bool {
    composite_scope.allows(field)
        || (auto_id_scope
            && !bloom_only_never.contains(field)
            && vortex_index::is_id_like_field_name(field))
}

fn bloom_policy_enabled(
    bloom_fields: &[String],
    composite_scope: &config::VixBloomCompositeScope,
    auto_id_scope: bool,
) -> bool {
    !bloom_fields.is_empty() || composite_scope.enabled() || auto_id_scope
}

/// Why an hour is (re)built this pass — the B4 regroup triggers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BuildTrigger {
    /// The hour has no stamped file yet: first build at settle.
    FirstBuild,
    /// `pending >= ZO_COMPACT_BLOOM_REGROUP_MIN_FILES`.
    PendingFiles,
    /// `pending / total >= ZO_COMPACT_BLOOM_REGROUP_MIN_RATIO`.
    PendingRatio,
    /// The oldest pending row waited `>= ZO_COMPACT_BLOOM_REGROUP_MAX_AGE_HOURS`.
    PendingAge,
}

/// DESIGN-COLD-MISS §4 B4 hour selection: a settled hour with pending files
/// is (re)built when the pending set is big enough in count or ratio, old
/// enough, or the hour has never been built; otherwise the pending files
/// wait (the query side probes their own sidecars meanwhile) so an hour's
/// `.bf` set is not fragmented into one chunk per merge output.
fn build_trigger(
    pending: usize,
    total: usize,
    stamped: usize,
    oldest_pending_age: Option<chrono::Duration>,
    cfg: &config::Compact,
) -> Option<BuildTrigger> {
    if pending == 0 {
        return None;
    }
    if stamped == 0 {
        return Some(BuildTrigger::FirstBuild);
    }
    if pending as i64 >= cfg.bloom_regroup_min_files.max(1) {
        return Some(BuildTrigger::PendingFiles);
    }
    if total > 0 && pending as f64 / total as f64 >= cfg.bloom_regroup_min_ratio {
        return Some(BuildTrigger::PendingRatio);
    }
    if oldest_pending_age
        .is_some_and(|age| age >= chrono::Duration::hours(cfg.bloom_regroup_max_age_hours.max(0)))
    {
        return Some(BuildTrigger::PendingAge);
    }
    None
}

/// One member of an hour build: the file's fence, its presence declaration
/// and the sections it brings into its chunk (possibly none — a file with
/// only presence still prunes by presence).
struct BuildMember {
    fence: BloomFence,
    presence: GroupMember,
    blooms: Vec<vortex_index::bloom::FileBloom>,
    /// `Some(version)`: consolidated out of that superseded chunk (CAS
    /// restamp from it); `None`: a pending file (stamp from 0).
    restamp_from: Option<i64>,
}

impl BuildMember {
    fn section_bytes(&self) -> usize {
        self.blooms.iter().map(|b| b.bytes.len()).sum()
    }
}

async fn process_bucket_locked(
    org_id: &str,
    stream_type: config::meta::stream::StreamType,
    stream_name: &str,
    date: &str,
    fpp: f64,
    composite_scope: &config::VixBloomCompositeScope,
    auto_id_scope: bool,
    bloom_only_never: &Arc<HashSet<String>>,
) -> Result<(), anyhow::Error> {
    let cfg = get_config();
    // the WHOLE hour under the lock (another compactor may have built it):
    // pending rows are the work, stamped rows decide the triggers, the
    // consolidation and the GC
    let rows = infra::file_list::query_for_bloom(org_id, stream_type, stream_name, date).await?;
    let pending: Vec<FileKey> = rows
        .iter()
        .filter(|row| row.bloom_ver == 0)
        .map(FileKey::from)
        .collect();
    if pending.is_empty() {
        return Ok(());
    }

    let latest_schema = infra::schema::get(org_id, stream_name, stream_type).await?;
    let stream_settings = unwrap_stream_settings(&latest_schema);
    let bloom_fields = get_stream_setting_bloom_filter_fields(&stream_settings);
    // A selective composite scope needs no per-stream field config because
    // eligibility is file-specific.
    if !bloom_policy_enabled(&bloom_fields, composite_scope, auto_id_scope) {
        // Nothing to build for this stream. Fence even sentinel stamps by the
        // sidecar generation and size that were classified, so a concurrent
        // heal remains pending at bloom_ver=0.
        let expected: Vec<BloomFence> = pending
            .iter()
            .map(|file| (file.id, file.meta.index_generation, file.meta.index_size))
            .collect();
        let matched =
            infra::file_list::update_bloom_ver(&expected, BLOOM_VER_NOT_APPLICABLE).await?;
        log::info!(
            "[COMPACTOR:BLOOM] {org_id}/{stream_type}/{stream_name}/{date}: no Bloom policy \
             enabled, {matched}/{} files marked not-applicable",
            expected.len()
        );
        return Ok(());
    }

    let now = now_micros();
    let stamped_rows: Vec<&infra::file_list::FileRecord> =
        rows.iter().filter(|row| row.bloom_ver > 0).collect();
    let oldest_pending_age = rows
        .iter()
        .filter(|row| row.bloom_ver == 0 && row.updated_at > 0)
        .map(|row| row.updated_at)
        .min()
        .map(|oldest| chrono::Duration::microseconds((now - oldest).max(0)));
    let Some(trigger) = build_trigger(
        pending.len(),
        rows.len(),
        stamped_rows.len(),
        oldest_pending_age,
        &cfg.compact,
    ) else {
        log::debug!(
            "[COMPACTOR:BLOOM] {org_id}/{stream_type}/{stream_name}/{date}: {} of {} files \
             pending ({} stamped), below the regroup triggers — held for a later pass (the \
             per-file probe serves them meanwhile)",
            pending.len(),
            rows.len(),
            stamped_rows.len()
        );
        return Ok(());
    };

    let started = std::time::Instant::now();
    log::info!(
        "[COMPACTOR:BLOOM] {org_id}/{stream_type}/{stream_name}/{date}: building hour \
         ({trigger:?}): {} pending of {} indexed files, {} stamped across {} chunk versions",
        pending.len(),
        rows.len(),
        stamped_rows.len(),
        stamped_rows
            .iter()
            .map(|row| row.bloom_ver)
            .collect::<HashSet<_>>()
            .len()
    );
    // Collect per-file blooms CONCURRENTLY: the sidecar-only fast path for
    // files whose sections are all writer-flagged (`bf`), the full reader
    // for everything else, one dictionary stream for blob-less old files —
    // the expensive backfill path, so it is CAPPED per pass: leftover files
    // stay bloom_ver=0 and join a later build.
    const LOAD_CONCURRENCY: usize = 8;
    use futures::{StreamExt, stream};
    let results: Vec<(
        BloomFence,
        String,
        Result<Option<LoadedFile>, anyhow::Error>,
    )> = stream::iter(pending.iter().cloned().map(|file| {
        let bloom_fields = bloom_fields.clone();
        let composite_scope = composite_scope;
        let auto_id_scope = auto_id_scope;
        let bloom_only_never = Arc::clone(bloom_only_never);
        async move {
            let loaded = load_file_blooms(
                &file,
                &bloom_fields,
                fpp,
                composite_scope,
                auto_id_scope,
                bloom_only_never,
            )
            .await;
            (
                (file.id, file.meta.index_generation, file.meta.index_size),
                file.key,
                loaded,
            )
        }
    }))
    .buffer_unordered(LOAD_CONCURRENCY)
    .collect()
    .await;
    let mut members: Vec<BuildMember> = Vec::new();
    let mut not_applicable: Vec<BloomFence> = Vec::new();
    let mut unbuildable: Vec<BloomFence> = Vec::new();
    let mut fallback_streams = 0usize;
    let mut sidecar_only = 0usize;
    let mut deferred = 0usize;
    // §5: pending files whose sidecar predates the current layout generation
    let mut repack_candidates: Vec<BloomFence> = Vec::new();
    let repack_opts = sidecar_repack_options(&cfg);
    let target_layout_gen = vortex_index::target_layout_gen(&repack_opts);
    for (fence, key, result) in results {
        match result {
            Ok(Some(LoadedFile {
                presence,
                blooms,
                source,
                layout_gen,
            })) => {
                match source {
                    LoadSource::Sidecar => sidecar_only += 1,
                    LoadSource::Dictionary => fallback_streams += 1,
                    LoadSource::Reader => {}
                }
                if layout_gen < target_layout_gen {
                    repack_candidates.push(fence);
                }
                // a file with neither a section nor an authoritative column
                // set brings nothing a `.bf` could prune on
                if blooms.is_empty() && !presence.complete {
                    not_applicable.push(fence);
                } else {
                    members.push(BuildMember {
                        fence,
                        presence,
                        blooms,
                        restamp_from: None,
                    });
                }
            }
            Ok(None) => deferred += 1,
            // DETERMINISTIC (file-shaped) failure: the file's own bytes can
            // never build, so re-queuing it would spin forever and re-burn a
            // fallback-budget slot every pass. Stamp it out of the queue —
            // this log line therefore fires ONCE per file.
            Err(e) if vortex_index::bloom::is_unbuildable(&e) => {
                unbuildable.push(fence);
                log::error!(
                    "[COMPACTOR:BLOOM] {key}: UNBUILDABLE bloom input, stamping \
                     bloom_ver={BLOOM_VER_UNBUILDABLE} (never retried; the pruner keeps \
                     scanning the file): {e:#}"
                );
            }
            Err(e) => {
                // transient (fetch/IO): this file stays bloom_ver=0 and
                // re-queues; keep building the rest of the bucket
                log::warn!(
                    "[COMPACTOR:BLOOM] {key}: per-file bloom load failed (re-queued): {e:#}"
                );
            }
        }
    }
    // Stamp poison files before chunk building. Only rows whose sidecar
    // identity still matches are counted; healed rows remain pending.
    let unbuildable_matched = if unbuildable.is_empty() {
        0
    } else {
        infra::file_list::update_bloom_ver(&unbuildable, BLOOM_VER_UNBUILDABLE).await?
    };
    // §5 old-file convergence: repack the oldest-layout sidecars within the
    // per-pass budget and publish them as new index generations. A repacked
    // file leaves THIS build (its row re-pends under the new generation and
    // joins a later build with the new layout) and the sentinel stamps
    // (fenced on the old generation) no longer match it.
    let mut repacked = 0usize;
    if !repack_candidates.is_empty() && REPACK_BUDGET.load(std::sync::atomic::Ordering::Relaxed) > 0
    {
        let by_fence: HashMap<BloomFence, &FileKey> = pending
            .iter()
            .map(|file| {
                (
                    (file.id, file.meta.index_generation, file.meta.index_size),
                    file,
                )
            })
            .collect();
        let mut done: HashSet<BloomFence> = HashSet::new();
        for fence in repack_candidates {
            if REPACK_BUDGET.fetch_sub(1, std::sync::atomic::Ordering::Relaxed) <= 0 {
                REPACK_BUDGET.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                break;
            }
            let Some(file) = by_fence.get(&fence) else {
                continue;
            };
            match repack_and_publish(org_id, stream_type, stream_name, file, &repack_opts).await {
                Ok(true) => {
                    done.insert(fence);
                    repacked += 1;
                }
                Ok(false) => {}
                Err(e) => {
                    // a DETERMINISTIC (file-shaped) failure hands its slot
                    // back, like `budgeted_backfill`: the file is stamped out
                    // of the pending set below and must not crowd healthy
                    // candidates out of a small budget; transient failures
                    // keep the slot (the work was done, it retries once the
                    // file re-pends)
                    if vortex_index::bloom::is_unbuildable(&e) {
                        REPACK_BUDGET.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    log::warn!(
                        "[COMPACTOR:BLOOM] {}: sidecar repack failed (kept as is): {e:#}",
                        file.key
                    );
                }
            }
        }
        if !done.is_empty() {
            members.retain(|member| !done.contains(&member.fence));
            not_applicable.retain(|fence| !done.contains(fence));
        }
    }

    // B4 consolidation: an hour fragmented into many small chunks folds its
    // small ones into this build (their files move by CAS restamp). Bounded
    // to one chunk's worth of members per pass — the chunk knob is the
    // assembler's memory knob (DESIGN §4 B4), and an hour the pre-B4
    // assembler left as one group per file would otherwise be read back
    // whole; the rest folds on the hour's next visit.
    let chunk_files = cfg.compact.bloom_chunk_files.max(1);
    let (consolidated, superseded) = consolidate_small_chunks(
        org_id,
        stream_type,
        stream_name,
        date,
        &stamped_rows,
        chunk_files,
        cfg.compact.bloom_max_chunks,
        chunk_files,
    )
    .await;
    let consolidated_files = consolidated.len();
    members.extend(consolidated);

    // Chunking: members sorted by their section bytes so files of similar
    // cardinality share a chunk — each field section is folded to the
    // chunk's smallest block count, and neighbours in size lose least to
    // the fold. Every chunk is one v2 `.bf` with the group file table and
    // presence bits.
    members.sort_by_key(|member| (member.section_bytes(), member.fence.0));
    let base_ver = now_micros();
    let mut chunk_idx: i64 = 0;
    let mut built = 0usize;
    let mut moved = 0usize;
    let mut new_versions: Vec<i64> = Vec::new();
    for chunk in members.chunks(chunk_files) {
        let bloom_ver = base_ver + chunk_idx;
        chunk_idx += 1;
        let field_blooms = fold_chunk_sections(chunk);
        let group: Vec<GroupMember> = chunk.iter().map(|m| m.presence.clone()).collect();
        let blob = BloomWriter::serialize_v2(&group, field_blooms)
            .map_err(|e| anyhow::anyhow!("serialize .bf: {e}"))?;
        let path =
            infra::bloom::path::bloom_path(org_id, stream_type, stream_name, date, bloom_ver);
        let account = infra::storage::get_account(org_id, &path).unwrap_or_default();
        infra::storage::put(&account, &path, bytes::Bytes::from(blob)).await?;
        new_versions.push(bloom_ver);
        // stamp LAST: pending rows from 0, consolidated rows from their old
        // version (a heal in between re-pended the row, which both leave at 0)
        let pending_fences: Vec<BloomFence> = chunk
            .iter()
            .filter(|m| m.restamp_from.is_none())
            .map(|m| m.fence)
            .collect();
        let mut matched = infra::file_list::update_bloom_ver(&pending_fences, bloom_ver).await?;
        let mut by_old: HashMap<i64, Vec<BloomFence>> = HashMap::new();
        for m in chunk {
            if let Some(old) = m.restamp_from {
                by_old.entry(old).or_default().push(m.fence);
            }
        }
        for (old, fences) in by_old {
            let restamped = infra::file_list::update_bloom_ver_from(
                org_id,
                stream_type,
                stream_name,
                date,
                &fences,
                old,
                bloom_ver,
            )
            .await?;
            moved += restamped as usize;
            matched += restamped;
        }
        if matched == 0
            && let Err(delete_error) = infra::storage::delete(&account, &path).await
        {
            let tombstone = FileListDeleted {
                id: 0,
                account: account.clone(),
                file: path.clone(),
                index_generation: 0,
                index_file: false,
                flattened: false,
            };
            infra::file_list::batch_add_deleted(
                org_id,
                now_micros(),
                std::slice::from_ref(&tombstone),
            )
            .await
            .map_err(|outbox_error| {
                anyhow::anyhow!(
                    "unreferenced bloom {path} delete failed ({delete_error}) and cleanup \
                     enqueue failed: {outbox_error}"
                )
            })?;
            log::warn!(
                "[COMPACTOR:BLOOM] unreferenced chunk {path} could not be deleted \
                 immediately; queued for deferred cleanup: {delete_error}"
            );
        }
        built += matched as usize;
    }
    let not_applicable_matched = if not_applicable.is_empty() {
        0
    } else {
        infra::file_list::update_bloom_ver(&not_applicable, BLOOM_VER_NOT_APPLICABLE).await?
    };

    // Superseded chunks whose rows all moved: deferred delete (never inline).
    // A version a concurrent heal left referenced stays; the GC below picks
    // it up once nothing references it.
    let mut retired = 0usize;
    let mut tombstones: Vec<FileListDeleted> = Vec::new();
    for old in superseded {
        if infra::file_list::bloom_ver_referenced(org_id, stream_type, stream_name, date, old)
            .await?
        {
            continue;
        }
        let path = infra::bloom::path::bloom_path(org_id, stream_type, stream_name, date, old);
        tombstones.push(FileListDeleted {
            id: 0,
            account: infra::storage::get_account(org_id, &path).unwrap_or_default(),
            file: path,
            index_generation: 0,
            index_file: false,
            flattened: false,
        });
        retired += 1;
    }
    // GC: every `.bf` of the hour no live row references, older than the
    // grace period (a crash between an object PUT and its stamp, a merge
    // that retired an input chunk's last member, ...).
    let gc = gc_unreferenced_chunks(
        org_id,
        stream_type,
        stream_name,
        date,
        &new_versions,
        now,
        cfg.compact.bloom_gc_min_age_secs,
    )
    .await;
    match gc {
        Ok(mut orphans) => tombstones.append(&mut orphans),
        Err(e) => log::warn!(
            "[COMPACTOR:BLOOM] {org_id}/{stream_type}/{stream_name}/{date}: chunk GC skipped: {e:#}"
        ),
    }
    let gc_queued = tombstones.len() - retired;
    if !tombstones.is_empty() {
        infra::file_list::batch_add_deleted(org_id, now_micros(), &tombstones).await?;
    }
    log::info!(
        "[COMPACTOR:BLOOM] {org_id}/{stream_type}/{stream_name}/{date}: built .bf for {built} \
         files ({chunk_idx} chunks of <= {chunk_files}; {sidecar_only} sidecar-only loads, \
         {fallback_streams} dictionary fallbacks, {deferred} deferred, {repacked} sidecars \
         repacked to the current layout, {consolidated_files} consolidated from small chunks, \
         {moved} restamped, {retired} superseded + {gc_queued} orphaned chunks queued for \
         deletion, {not_applicable_matched} not applicable, {unbuildable_matched} unbuildable) \
         in {:?}",
        started.elapsed()
    );
    Ok(())
}

/// Fold every field section of one chunk down to the chunk's smallest block
/// count for that field (`Sbbf::fold`: sound for any power-of-two divisor —
/// membership answers are preserved, only the fill ratio rises), so one
/// block-row of the transposed matrix answers every member. A member whose
/// section cannot be folded (not a power-of-two multiple — never produced by
/// this crate's writers) contributes no section for that field (no info).
fn fold_chunk_sections(chunk: &[BuildMember]) -> Vec<FieldBloom> {
    let mut min_blocks: HashMap<&str, u32> = HashMap::new();
    for member in chunk {
        for bloom in &member.blooms {
            min_blocks
                .entry(bloom.field.as_str())
                .and_modify(|min| *min = (*min).min(bloom.num_blocks))
                .or_insert(bloom.num_blocks);
        }
    }
    let mut out = Vec::new();
    for member in chunk {
        let file_id = member.fence.0 as u64;
        for bloom in &member.blooms {
            let target = min_blocks[bloom.field.as_str()];
            let bytes = if bloom.num_blocks == target {
                bloom.bytes.clone()
            } else {
                let folded = infra::bloom::Sbbf::from_bytes(&bloom.bytes).and_then(|mut sbbf| {
                    sbbf.fold(target)?;
                    Ok(sbbf.to_bytes())
                });
                match folded {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        log::warn!(
                            "[COMPACTOR:BLOOM] file {file_id} field {:?}: cannot fold {} -> \
                             {target} blocks ({e}); section left out (no info)",
                            bloom.field,
                            bloom.num_blocks
                        );
                        continue;
                    }
                }
            };
            out.push(FieldBloom {
                field: bloom.field.clone(),
                file_id,
                n_items: bloom.n_items,
                bytes,
            });
        }
    }
    out
}

/// B4 consolidation: when the hour carries more than `max_chunks` stamped
/// versions, read SMALL chunks (members `<= chunk_files / 4`, and every v1
/// chunk) back — footer + body in one GET, de-transposed per member — and
/// hand their still-live members to the current build with their presence
/// (the v2 footer's own bits; a v1 chunk's from each member's sidecar
/// tail). Oldest versions first, and no further chunk is opened once
/// `max_members` members are collected: the resident set stays one chunk's
/// worth however fragmented the hour is. Returns the members and the
/// versions they left. A chunk that cannot be read stays as it is.
#[allow(clippy::too_many_arguments)]
async fn consolidate_small_chunks(
    org_id: &str,
    stream_type: config::meta::stream::StreamType,
    stream_name: &str,
    date: &str,
    stamped_rows: &[&infra::file_list::FileRecord],
    chunk_files: usize,
    max_chunks: usize,
    max_members: usize,
) -> (Vec<BuildMember>, Vec<i64>) {
    let mut by_version: std::collections::BTreeMap<i64, Vec<&infra::file_list::FileRecord>> =
        std::collections::BTreeMap::new();
    for row in stamped_rows {
        by_version.entry(row.bloom_ver).or_default().push(row);
    }
    if by_version.len() <= max_chunks.max(1) {
        return (Vec::new(), Vec::new());
    }
    let small_limit = (chunk_files / 4).max(1);
    let mut members: Vec<BuildMember> = Vec::new();
    let mut superseded = Vec::new();
    let total_versions = by_version.len();
    let mut visited = 0usize;
    for (version, rows) in by_version {
        if members.len() >= max_members {
            log::info!(
                "[COMPACTOR:BLOOM] {org_id}/{stream_type}/{stream_name}/{date}: consolidation \
                 bounded at {} members this pass; {} of {total_versions} versions left for the \
                 hour's next visit",
                members.len(),
                total_versions - visited
            );
            break;
        }
        visited += 1;
        let path = infra::bloom::path::bloom_path(org_id, stream_type, stream_name, date, version);
        let account = infra::storage::get_account(org_id, &path).unwrap_or_default();
        let object = match infra::storage::get_bytes(&account, &path).await {
            Ok(bytes) => bytes,
            Err(e) => {
                log::warn!("[COMPACTOR:BLOOM] consolidation: {path} unreadable, kept as is: {e}");
                continue;
            }
        };
        let reader = match infra::bloom::BloomReader::parse(&object) {
            Ok(reader) => reader,
            Err(e) => {
                log::warn!("[COMPACTOR:BLOOM] consolidation: {path} unparsable, kept as is: {e}");
                continue;
            }
        };
        // every v1 chunk counts as small (no presence, per-section tables);
        // a v2 chunk only below the member limit
        if reader.is_v2() && rows.len() > small_limit {
            continue;
        }
        let live: HashMap<u64, &infra::file_list::FileRecord> =
            rows.iter().map(|row| (row.id as u64, *row)).collect();
        let mut sections_of: HashMap<u64, Vec<vortex_index::bloom::FileBloom>> = HashMap::new();
        let mut consistent = true;
        for layout in reader.section_layouts() {
            let start = layout.body_offset as usize;
            let end = start + layout.matrix_bytes() as usize;
            let Some(matrix) = object.get(start..end) else {
                consistent = false;
                break;
            };
            let Some(bodies) = infra::bloom::BloomReader::detranspose_section(&layout, matrix)
            else {
                consistent = false;
                break;
            };
            for ((file_id, n_items), bytes) in
                layout.file_ids.iter().zip(layout.n_items).zip(bodies)
            {
                if !live.contains_key(file_id) {
                    continue; // merged away: nothing references it any more
                }
                sections_of
                    .entry(*file_id)
                    .or_default()
                    .push(vortex_index::bloom::FileBloom {
                        field: layout.field.to_string(),
                        num_blocks: layout.num_blocks,
                        n_items: *n_items,
                        bytes,
                    });
            }
        }
        if !consistent {
            log::warn!("[COMPACTOR:BLOOM] consolidation: {path} footer/body disagree, kept as is");
            continue;
        }
        for (file_id, row) in live {
            let presence = match reader.member_columns(file_id) {
                Some((columns, complete)) => {
                    GroupMember::new(file_id, columns).with_complete(complete)
                }
                // v1 chunk: presence from the member's sidecar tail (one GET);
                // unreadable -> no info
                None => match sidecar_presence(&FileKey::from(row)).await {
                    Ok(presence) => presence,
                    Err(e) => {
                        log::debug!(
                            "[COMPACTOR:BLOOM] consolidation: no presence for file {file_id}: {e:#}"
                        );
                        GroupMember::new(file_id, Vec::new()).with_complete(false)
                    }
                },
            };
            members.push(BuildMember {
                fence: (row.id, row.index_generation, row.index_size),
                presence,
                blooms: sections_of.remove(&file_id).unwrap_or_default(),
                restamp_from: Some(version),
            });
        }
        superseded.push(version);
    }
    (members, superseded)
}

/// The `.bf` objects of the hour that no live row references (after this
/// pass's stamps) and that are older than the grace period, as deferred
/// delete tombstones. `new_versions` are the chunks just written (referenced
/// by construction, even if their stamp matched nothing — those were handled
/// inline).
async fn gc_unreferenced_chunks(
    org_id: &str,
    stream_type: config::meta::stream::StreamType,
    stream_name: &str,
    date: &str,
    new_versions: &[i64],
    now: i64,
    min_age_secs: i64,
) -> Result<Vec<FileListDeleted>, anyhow::Error> {
    let dir = infra::bloom::path::bloom_dir(org_id, stream_type, stream_name, date);
    let account = infra::storage::get_account(org_id, &dir).unwrap_or_default();
    let objects = infra::storage::list(&account, &dir).await?;
    if objects.is_empty() {
        return Ok(Vec::new());
    }
    // fresh view: the rows as they stand after this pass's stamps
    let referenced: HashSet<i64> =
        infra::file_list::query_for_bloom(org_id, stream_type, stream_name, date)
            .await?
            .iter()
            .map(|row| row.bloom_ver)
            .filter(|version| *version > 0)
            .chain(new_versions.iter().copied())
            .collect();
    let min_age_micros = min_age_secs.max(0).saturating_mul(1_000_000);
    // orphans stay listed until the delayed deletion job removes them
    // (ZO_COMPACT_DELETE_FILES_DELAY_HOURS); remember what this process
    // already queued so every pass in that window does not add the same
    // tombstone rows again (duplicates are harmless - the delete is
    // idempotent - but they are noise in file_list_deleted)
    static QUEUED: std::sync::LazyLock<parking_lot::Mutex<hashlink::LruCache<String, ()>>> =
        std::sync::LazyLock::new(|| parking_lot::Mutex::new(hashlink::LruCache::new(65_536)));
    let mut tombstones = Vec::new();
    for object in objects {
        // `{bloom_ver}.bf`; the listed key may carry the bucket prefix, so
        // the logical key is rebuilt from the version
        let Some(version) = object
            .rsplit('/')
            .next()
            .and_then(|name| name.strip_suffix(".bf"))
            .and_then(|stem| stem.parse::<i64>().ok())
            .filter(|version| *version > 0)
        else {
            continue;
        };
        if referenced.contains(&version) || now - version < min_age_micros {
            continue;
        }
        let path = infra::bloom::path::bloom_path(org_id, stream_type, stream_name, date, version);
        if QUEUED.lock().insert(path.clone(), ()).is_some() {
            continue;
        }
        tombstones.push(FileListDeleted {
            id: 0,
            account: account.clone(),
            file: path,
            index_generation: 0,
            index_file: false,
            flattened: false,
        });
    }
    Ok(tombstones)
}

/// How one pending file's sections were obtained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LoadSource {
    /// Sidecar-only: the tail + the bloom blob, every section writer-flagged.
    Sidecar,
    /// The full ranged reader (legacy sections validated against the docs
    /// schema, scope supplements).
    Reader,
    /// Rebuilt by streaming the term dictionary (backfill of old files).
    Dictionary,
}

/// One loaded pending file: its presence declaration plus the sections it
/// contributes, and the sidecar's layout generation (§5: a file below the
/// current writer's generation is a repack candidate).
struct LoadedFile {
    presence: GroupMember,
    blooms: Vec<vortex_index::bloom::FileBloom>,
    source: LoadSource,
    layout_gen: u32,
}

enum LoadedBlooms {
    /// Read from the file's own `bloom` blob (cheap ranged fetch).
    FromBlob(Vec<vortex_index::bloom::FileBloom>),
    /// Rebuilt by streaming the term dictionary (backfill of old files).
    FromDict(Vec<vortex_index::bloom::FileBloom>),
    /// Blob-less file skipped this pass (per-pass dictionary-stream cap
    /// reached) — stays queued for a later chunk.
    Deferred,
}

/// Per-pass budget of dictionary-stream fallbacks (process-wide): giant
/// backfill buckets otherwise pin a builder pass for hours and starve the
/// queue. Reset each pass.
static FALLBACK_BUDGET: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);

/// Per-pass budget of §5 sidecar repacks (process-wide, `ZO_COMPACT_SIDECAR_
/// REPACK_BUDGET`, default 0 = off): each is one sidecar download + upload.
/// Reset each pass.
static REPACK_BUDGET: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);

/// What the current writers stamp — the repack target (same knobs as
/// `core_writer_options_from_config`).
fn sidecar_repack_options(cfg: &config::Config) -> vortex_index::SidecarRepackOptions {
    vortex_index::SidecarRepackOptions {
        sidecar_tail_zstd: cfg.limit.vix_sidecar_tail_zstd,
        auto_bloom: vortex_index::bloom::AutoBloomPolicy {
            min_distinct: cfg.common.vix_bloom_auto_min_distinct,
            max_ratio: cfg.common.vix_bloom_auto_max_ratio,
            max_data_pct: cfg.common.vix_bloom_auto_max_data_pct,
        },
        bloom_fpp: cfg.common.vix_bloom_fpp,
    }
}

/// §5: repack one file's sidecar to the current layout and publish it as a
/// new index generation (the heal's publication path: create-only upload,
/// CAS, cache invalidation, broadcast; the row re-pends at `bloom_ver = 0`).
/// `Ok(true)` = a new generation was published (or the CAS lost to a
/// concurrent rewrite — either way the file left this build).
async fn repack_and_publish(
    org_id: &str,
    stream_type: config::meta::stream::StreamType,
    stream_name: &str,
    file: &FileKey,
    opts: &vortex_index::SidecarRepackOptions,
) -> Result<bool, anyhow::Error> {
    let handle = tokio::runtime::Handle::current();
    let data: Arc<dyn vortex_index::VixRangeSource> = Arc::new(HealProbeRangeSource {
        account: file.account.clone(),
        location: object_store::path::Path::from(file.key.as_str()),
        size: file.meta.compressed_size as u64,
        handle,
        cancel: None,
    });
    // The repack copies every blob and walks the whole dictionary, i.e. it
    // reads the sidecar end to end: one whole-object GET (dev 2026-10-14: a
    // ranged walk cost ~1,200 serial GETs = 100 s for a 250 MB sidecar)
    let Some(sidecar_key) = config::vix_sidecar_key(&file.key, file.meta.index_generation)
        .filter(|_| file.meta.index_size > 0)
    else {
        return Ok(false);
    };
    let started = std::time::Instant::now();
    let sidecar = infra::storage::get_bytes(&file.account, &sidecar_key)
        .await
        .map_err(|e| anyhow::anyhow!("{}: download sidecar for repack: {e}", file.key))?;
    let index = vortex_index::BytesRangeSource::new(sidecar_key, sidecar);
    let opts = *opts;
    let repack =
        tokio::task::spawn_blocking(move || vortex_index::repack_sidecar(data, index, &opts))
            .await?
            .map_err(|e| {
                // a sidecar whose own bytes cannot be repacked (missing
                // properties, unknown dictionary codec, bad zstd frame)
                // fails the same way on every attempt: carry the poison
                // marker so the caller refunds its budget slot
                if vix_error_is_deterministic(&e) {
                    e.context(vortex_index::bloom::UnbuildableFile)
                } else {
                    e
                }
            })?;
    log::info!(
        "[COMPACTOR:BLOOM] {}: repacked sidecar layout_gen {} -> {}, {} -> {} bytes, AUTO \
         sections added {:?}, took {:?}",
        file.key,
        repack.from_layout_gen,
        vortex_index::target_layout_gen(&opts),
        file.meta.index_size,
        repack.sidecar.len(),
        repack.added_sections,
        started.elapsed()
    );
    let index_size = repack.sidecar.len() as u64;
    let outcome = crate::service::vix::core_writer::SidecarHealOutcome::Rebuilt {
        index: repack.sidecar,
        stats: vortex_index::VixWriterStats {
            row_count: repack.row_count,
            term_count: repack.term_count,
            index_size,
            ..Default::default()
        },
    };
    let cfg = get_config();
    let stream_settings = infra::schema::get_settings(org_id, stream_name, stream_type).await;
    let compliance = cfg.s3.feature_force_infrequent_access
        && stream_settings
            .map(|s| s.storage_type)
            .unwrap_or(config::meta::stream::StorageType::Normal)
            .is_compliance();
    super::merge::publish_sidecar_generation(
        0,
        org_id,
        stream_type,
        stream_name,
        file,
        outcome,
        compliance,
        "repacked",
    )
    .await
}

/// The ranged source of a file's `.vxi` sidecar. v3 split: the per-file bloom
/// blob (and the dictionary the backfill streams) live in the SIDECAR,
/// fetched by range. The pending queue selects `index_size > 0` rows only, so
/// the sidecar exists for every file reaching here; index-less files keep
/// their existing sentinel path upstream.
fn sidecar_source(
    file: &FileKey,
    handle: tokio::runtime::Handle,
) -> Option<Arc<dyn vortex_index::VixRangeSource>> {
    config::vix_sidecar_key(&file.key, file.meta.index_generation)
        .filter(|_| file.meta.index_size > 0)
        .map(|sidecar_key| {
            Arc::new(HealProbeRangeSource {
                account: file.account.clone(),
                location: object_store::path::Path::from(sidecar_key.as_str()),
                size: file.meta.index_size as u64,
                handle,
                cancel: None,
            }) as Arc<dyn vortex_index::VixRangeSource>
        })
}

/// A file's presence declaration from its sidecar tail alone (one GET).
async fn sidecar_presence(file: &FileKey) -> Result<GroupMember, anyhow::Error> {
    let handle = tokio::runtime::Handle::current();
    let source =
        sidecar_source(file, handle).ok_or_else(|| anyhow::anyhow!("{}: no sidecar", file.key))?;
    let file_id = file.id as u64;
    tokio::task::spawn_blocking(move || {
        let probe = vortex_index::FileBloomProbe::open_sidecar(source)?;
        Ok(presence_of(
            file_id,
            probe.field_entries().iter().map(|e| e.name.as_str()),
            probe.columns_complete(),
        ))
    })
    .await?
}

fn presence_of<'a>(
    file_id: u64,
    columns: impl Iterator<Item = &'a str>,
    complete: bool,
) -> GroupMember {
    GroupMember::new(file_id, columns.map(str::to_string).collect()).with_complete(complete)
}

/// Load one pending file's sections and presence. `Ok(None)` = deferred
/// (the per-pass dictionary budget is spent; the file stays pending).
async fn load_file_blooms(
    file: &FileKey,
    bloom_fields: &[String],
    fpp: f64,
    composite_scope: &config::VixBloomCompositeScope,
    auto_id_scope: bool,
    bloom_only_never: Arc<HashSet<String>>,
) -> Result<Option<LoadedFile>, anyhow::Error> {
    let handle = tokio::runtime::Handle::current();
    let source: Arc<dyn vortex_index::VixRangeSource> = Arc::new(HealProbeRangeSource {
        account: file.account.clone(),
        location: object_store::path::Path::from(file.key.as_str()),
        // a .vix FileMeta's compressed_size is the exact DATA-object size
        size: file.meta.compressed_size as u64,
        handle: handle.clone(),
        cancel: None,
    });
    let index_source = sidecar_source(file, handle);
    let bloom_fields = bloom_fields.to_vec();
    let composite_scope = (*composite_scope).clone();
    let file_key = file.key.clone();
    let file_id = file.id as u64;
    tokio::task::spawn_blocking(move || {
        // B4 fast path: the sidecar tail alone (1 GET) gives the presence
        // declaration and the section table; when every per-field section is
        // writer-flagged `bf` and nothing in the requested scope needs a
        // dictionary supplement, the blob (1 more GET) is the whole load —
        // no data-object read at all.
        if let Some(index_source) = index_source.as_ref() {
            let probe = vortex_index::FileBloomProbe::open_sidecar(Arc::clone(index_source))?;
            let presence = presence_of(
                file_id,
                probe.field_entries().iter().map(|e| e.name.as_str()),
                probe.columns_complete(),
            );
            if let Some(blooms) = sidecar_only_sections(
                &probe,
                &bloom_fields,
                &composite_scope,
                auto_id_scope,
                &bloom_only_never,
                &file_key,
            )? {
                return Ok(Some(LoadedFile {
                    presence,
                    blooms,
                    source: LoadSource::Sidecar,
                    layout_gen: probe.layout_gen(),
                }));
            }
        }
        // the full reader: legacy sections, scope supplements, backfills
        let reader = VixReader::open_ranged_with_index(source, index_source)?;
        let presence = presence_of(file_id, reader.field_names(), reader.columns_complete());
        let layout_gen = reader.layout_gen();
        Ok(
            match load_blooms_sync(
                &reader,
                &bloom_fields,
                fpp,
                &composite_scope,
                auto_id_scope,
                &bloom_only_never,
                &file_key,
                &FALLBACK_BUDGET,
            )? {
                LoadedBlooms::FromBlob(blooms) => Some(LoadedFile {
                    presence,
                    blooms,
                    source: LoadSource::Reader,
                    layout_gen,
                }),
                LoadedBlooms::FromDict(blooms) => Some(LoadedFile {
                    presence,
                    blooms,
                    source: LoadSource::Dictionary,
                    layout_gen,
                }),
                LoadedBlooms::Deferred => None,
            },
        )
    })
    .await?
}

/// The sidecar-only section set of a file, when it is SUFFICIENT: the
/// composite section (retained as is, its guards are authoritative) plus
/// every per-field section whose field the writer flagged `bf` (eligible by
/// construction: complete raw-string term values). `None` when the file
/// needs the full reader — a per-field section without the flag (legacy
/// explicit field: schema-validated there), or a requested field (stream
/// bloom field, scoped ID field) with term capability that neither a flagged
/// section nor the composite's guards cover (a dictionary supplement).
fn sidecar_only_sections(
    probe: &vortex_index::FileBloomProbe,
    bloom_fields: &[String],
    composite_scope: &config::VixBloomCompositeScope,
    auto_id_scope: bool,
    bloom_only_never: &HashSet<String>,
    context: &str,
) -> Result<Option<Vec<vortex_index::bloom::FileBloom>>, anyhow::Error> {
    if !probe.has_blob() {
        return Ok(None);
    }
    let flagged: HashSet<&str> = probe.bloomed_fields().collect();
    let has_unflagged_section = probe.sections().iter().any(|section| {
        section.field != vortex_index::bloom::COMPOSITE_BLOOM_FIELD
            && !flagged.contains(section.field.as_str())
    });
    if has_unflagged_section {
        return Ok(None);
    }
    let Some(blooms) = probe.read_file_blooms()? else {
        return Ok(None);
    };
    // requested = stream bloom fields + the composite scope's fields; a
    // term-capable, non-partial requested field without coverage needs the
    // reader's supplement path
    let needs_supplement = probe.field_entries().iter().any(|entry| {
        let name = entry.name.as_str();
        let requested = bloom_fields.iter().any(|field| field == name)
            || composite_scope_allows(composite_scope, auto_id_scope, bloom_only_never, name);
        requested
            && entry.has_type(vortex_index::FIELD_TYPE_TERM)
            && !probe.partial_fields().contains(name)
            && !flagged.contains(name)
            && !composite_sections_cover_field(&blooms, name)
    });
    if needs_supplement {
        log::debug!(
            "[COMPACTOR:BLOOM] {context}: a requested term field lacks a flagged section and \
             composite coverage; taking the reader path"
        );
        return Ok(None);
    }
    // keep the composite and the flagged per-field sections; a partial
    // field's section is never trusted
    Ok(Some(
        blooms
            .into_iter()
            .filter(|bloom| {
                bloom.field == vortex_index::bloom::COMPOSITE_BLOOM_FIELD
                    || (flagged.contains(bloom.field.as_str())
                        && !probe.partial_fields().contains(bloom.field.as_str()))
            })
            .collect(),
    ))
}

/// The sync per-file load: transpose a complete blob directly, supplement a
/// blob whose current requested scope is not covered, or budget a dictionary
/// backfill for a blob-less file.
fn load_blooms_sync(
    reader: &VixReader,
    bloom_fields: &[String],
    fpp: f64,
    composite_scope: &config::VixBloomCompositeScope,
    auto_id_scope: bool,
    bloom_only_never: &HashSet<String>,
    file_key: &str,
    budget: &std::sync::atomic::AtomicI64,
) -> Result<LoadedBlooms, anyhow::Error> {
    match reader.file_blooms() {
        Ok(Some(blooms)) => {
            return retain_and_supplement_blob(
                reader,
                blooms,
                bloom_fields,
                fpp,
                composite_scope,
                auto_id_scope,
                bloom_only_never,
                file_key,
                budget,
            );
        }
        Ok(None) => {}
        // A CORRUPT blob is file-shaped, but the dictionary may still be
        // walkable: log loudly and take the backfill path — the file is
        // poisoned only if that fails deterministically too.
        Err(e) if vortex_index::bloom::is_unbuildable(&e) => {
            log::error!(
                "[COMPACTOR:BLOOM] {file_key}: corrupt per-file bloom blob, trying the \
                 dictionary backfill instead: {e:#}"
            );
        }
        // Fetch-shaped (possibly transient): re-queue.
        Err(e) => return Err(e),
    }
    // Do not consume a fallback slot when this file has no COMPLETE requested
    // raw-string term source. Numeric/type-drifted and partial fields must
    // stay fail-open rather than seeding authoritative guards.
    if !has_requested_term_capability(
        reader,
        bloom_fields,
        composite_scope,
        auto_id_scope,
        bloom_only_never,
        file_key,
    )? {
        return Ok(LoadedBlooms::FromBlob(Vec::new()));
    }
    budgeted_backfill(budget, || {
        blooms_from_dictionary(
            reader,
            bloom_fields,
            fpp,
            composite_scope,
            auto_id_scope,
            bloom_only_never,
            file_key,
        )
    })
}

/// Keep every historical composite bit intact, while adding independent
/// per-field filters for requested complete term fields that its guards do
/// not cover. A second composite cannot be ORed safely when its SBBF sizing
/// differs, and replacing the old one would lose bloom-only values for which
/// no dictionary remains.
fn retain_and_supplement_blob(
    reader: &VixReader,
    blooms: Vec<vortex_index::bloom::FileBloom>,
    bloom_fields: &[String],
    fpp: f64,
    composite_scope: &config::VixBloomCompositeScope,
    auto_id_scope: bool,
    bloom_only_never: &HashSet<String>,
    context: &str,
    budget: &std::sync::atomic::AtomicI64,
) -> Result<LoadedBlooms, anyhow::Error> {
    let (mut wanted, mut per_field): (Vec<_>, Vec<_>) = blooms
        .into_iter()
        .partition(|b| b.field == vortex_index::bloom::COMPOSITE_BLOOM_FIELD);

    // Guard-complete composite coverage needs no docs-footer fetch. Only
    // fields that still need a per-field verdict are schema-validated below:
    // additive fields always do, while scoped fields do only when the
    // retained composite does not already cover them.
    let needs_validation: Vec<&str> = reader
        .term_fields()
        .filter(|(_id, name)| {
            // S5: a field the writer flagged `bf` carries a per-field section
            // it deemed eligible (W1 AUTO or explicit) — additive like a
            // stream bloom field, never dropped on this path
            let additive = bloom_fields.iter().any(|field| field == name)
                || reader.bloomed_fields().any(|field| field == *name);
            let scoped =
                composite_scope_allows(composite_scope, auto_id_scope, bloom_only_never, name);
            if !additive && !scoped {
                return false;
            }
            if reader.partial_fields().contains(*name) {
                log::debug!(
                    "[COMPACTOR:BLOOM] {context}: requested term field {name:?} is partial; \
                     dropping any per-field section and leaving it fail-open"
                );
                return false;
            }
            additive || !composite_sections_cover_field(&wanted, name)
        })
        .map(|(_id, name)| name)
        .collect();
    let validated =
        complete_raw_string_term_pairs(reader, |name| needs_validation.contains(&name), context)?;

    // Existing per-field sections are authoritative only after the same
    // complete raw-string check as dictionary supplements. Drop every other
    // per-field section: keeping one would make the query prefer unsafe data
    // over the guarded composite fallback.
    let mut missing = Vec::new();
    for (id, field) in validated {
        if let Some(position) = per_field
            .iter()
            .position(|b| b.field.as_str() == field.as_str())
        {
            wanted.push(per_field.swap_remove(position));
        } else {
            missing.push((id, field));
        }
    }

    let requested_without_source = |field: &str| {
        (!reader.has_term_capability(field) || reader.partial_fields().contains(field))
            && !composite_sections_cover_field(&wanted, field)
            && !wanted.iter().any(|b| b.field == field)
    };
    match composite_scope {
        config::VixBloomCompositeScope::Only(fields) => {
            for field in fields
                .iter()
                .filter(|field| requested_without_source(field))
            {
                log::debug!(
                    "[COMPACTOR:BLOOM] {context}: requested composite field {field:?} has no \
                     complete term source or retained guard coverage; leaving it fail-open"
                );
            }
        }
        config::VixBloomCompositeScope::All => {
            for field in reader
                .bloom_only_fields()
                .filter(|field| requested_without_source(field))
            {
                log::debug!(
                    "[COMPACTOR:BLOOM] {context}: bloom-only field {field:?} has no retained \
                     guard coverage and no dictionary source; leaving it fail-open"
                );
            }
        }
    }
    if auto_id_scope {
        for field in reader.bloom_only_fields().filter(|field| {
            vortex_index::is_id_like_field_name(field)
                && !bloom_only_never.contains(*field)
                && requested_without_source(field)
        }) {
            log::debug!(
                "[COMPACTOR:BLOOM] {context}: AUTO ID field {field:?} has no retained guard \
                 coverage and no dictionary source; leaving it fail-open"
            );
        }
    }
    if missing.is_empty() {
        return Ok(LoadedBlooms::FromBlob(wanted));
    }

    log::info!(
        "[COMPACTOR:BLOOM] {context}: existing blob lacks safe coverage for [{}]; \
         budgeted dictionary supplement required",
        missing
            .iter()
            .map(|(_, field)| field.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );
    budgeted_backfill(budget, || {
        let mut supplemental = blooms_from_pairs(reader, missing, Vec::new(), fpp, context)?;
        wanted.append(&mut supplemental);
        Ok(wanted)
    })
}

fn has_requested_term_capability(
    reader: &VixReader,
    bloom_fields: &[String],
    composite_scope: &config::VixBloomCompositeScope,
    auto_id_scope: bool,
    bloom_only_never: &HashSet<String>,
    context: &str,
) -> Result<bool, anyhow::Error> {
    Ok(!requested_complete_term_pairs(
        reader,
        bloom_fields,
        composite_scope,
        auto_id_scope,
        bloom_only_never,
        context,
    )?
    .is_empty())
}

fn requested_complete_term_pairs(
    reader: &VixReader,
    bloom_fields: &[String],
    composite_scope: &config::VixBloomCompositeScope,
    auto_id_scope: bool,
    bloom_only_never: &HashSet<String>,
    context: &str,
) -> Result<Vec<(u16, String)>, anyhow::Error> {
    let eligible = complete_raw_string_term_pairs(
        reader,
        |name| {
            bloom_fields.iter().any(|field| field == name)
                || composite_scope_allows(composite_scope, auto_id_scope, bloom_only_never, name)
        },
        context,
    )?;
    if let config::VixBloomCompositeScope::Only(fields) = composite_scope {
        for field in fields {
            if !eligible.iter().any(|(_id, name)| name == field) {
                log::debug!(
                    "[COMPACTOR:BLOOM] {context}: requested composite field {field:?} has no \
                     complete raw-string term source; leaving it uncovered (fail-open)"
                );
            }
        }
    }
    Ok(eligible
        .into_iter()
        .filter(|(_id, name)| {
            bloom_fields.iter().any(|field| field == name)
                || composite_scope_allows(composite_scope, auto_id_scope, bloom_only_never, name)
        })
        .collect())
}

/// Whether a `VixError` is a pure function of the file's own bytes
/// (footer/dictionary parse, unsupported format) rather than a fetch/IO
/// failure. Resolving docs schema types for Bloom coverage and repacking a
/// sidecar both classify their failures with it: deterministic ones carry
/// the [`vortex_index::bloom::UnbuildableFile`] marker.
fn vix_error_is_deterministic(error: &anyhow::Error) -> bool {
    match error.downcast_ref::<vortex_index::VixError>() {
        Some(
            vortex_index::VixError::Malformed(_) | vortex_index::VixError::UnsupportedFormat(_),
        ) => true,
        Some(vortex_index::VixError::Vortex(error)) => vortex_schema_error_is_deterministic(error),
        _ => false,
    }
}

fn vortex_schema_error_is_deterministic(error: &vortex::error::VortexError) -> bool {
    use vortex::error::VortexError;

    match error {
        VortexError::Context(_, inner) => vortex_schema_error_is_deterministic(inner),
        VortexError::Shared(inner) => vortex_schema_error_is_deterministic(inner),
        // The ranged-read bridge uses `vortex_err!("fetch ...")`, which is
        // `Other`; keep those retryable while classifying parser-produced
        // `Other` errors from immutable footer bytes as poison.
        VortexError::Other(message, _) => !message.to_string().starts_with("fetch "),
        VortexError::External(..)
        | VortexError::Io(..)
        | VortexError::ObjectStore(..)
        | VortexError::Join(..) => false,
        // Dtype/footer parse, bounds, serde, Arrow and FlatBuffer errors are
        // pure functions of the immutable bytes already fetched.
        _ => true,
    }
}

fn complete_raw_string_term_pairs(
    reader: &VixReader,
    mut requested: impl FnMut(&str) -> bool,
    context: &str,
) -> Result<Vec<(u16, String)>, anyhow::Error> {
    let mut candidates = reader
        .term_fields()
        .filter(|(_id, name)| requested(name))
        .peekable();
    if candidates.peek().is_none() {
        return Ok(Vec::new());
    }
    let schema = reader.docs_schema().map_err(|e| {
        let deterministic = vix_error_is_deterministic(&e);
        let e = e.context(format!("{context}: read docs schema for Bloom coverage"));
        if deterministic {
            e.context(vortex_index::bloom::UnbuildableFile)
        } else {
            e
        }
    })?;
    Ok(candidates
        .filter_map(|(id, name)| {
            if reader.partial_fields().contains(name) {
                log::debug!(
                    "[COMPACTOR:BLOOM] {context}: term field {name:?} is partial; leaving Bloom \
                     coverage fail-open"
                );
                return None;
            }
            let Ok(field) = schema.field_with_name(name) else {
                log::debug!(
                    "[COMPACTOR:BLOOM] {context}: term field {name:?} has no docs-schema type; \
                     leaving Bloom coverage fail-open"
                );
                return None;
            };
            if !matches!(
                field.data_type(),
                arrow::datatypes::DataType::Utf8
                    | arrow::datatypes::DataType::LargeUtf8
                    | arrow::datatypes::DataType::Utf8View
            ) {
                log::debug!(
                    "[COMPACTOR:BLOOM] {context}: term field {name:?} has non-string docs type \
                     {:?}; leaving Bloom coverage fail-open",
                    field.data_type()
                );
                return None;
            }
            Some((id, name.to_string()))
        })
        .collect())
}

fn composite_sections_cover_field(blooms: &[vortex_index::bloom::FileBloom], field: &str) -> bool {
    blooms
        .iter()
        .filter(|b| b.field == vortex_index::bloom::COMPOSITE_BLOOM_FIELD)
        .any(|b| composite_covers_field(b, field))
}

fn composite_covers_field(bloom: &vortex_index::bloom::FileBloom, field: &str) -> bool {
    let mut key = Vec::new();
    (0..vortex_index::bloom::COMPOSITE_GUARD_PROBES).all(|probe| {
        vortex_index::bloom::composite_guard_key(field, probe, &mut key)
            .is_some_and(|key| file_bloom_might_contain(bloom, key))
    })
}

fn file_bloom_might_contain(bloom: &vortex_index::bloom::FileBloom, key: &[u8]) -> bool {
    use infra::bloom::sbbf::{BLOCK_BYTES, block_index, check_block, hash_value};

    if bloom.num_blocks == 0 {
        return false;
    }
    let hash = hash_value(key);
    let start = block_index(hash, bloom.num_blocks) as usize * BLOCK_BYTES;
    bloom
        .bytes
        .get(start..start + BLOCK_BYTES)
        .and_then(|block| <&[u8; BLOCK_BYTES]>::try_from(block).ok())
        .is_some_and(|block| check_block(block, hash))
}

/// Run one budget slot's worth of dictionary backfill via `build`. The slot
/// is consumed up front, and a DETERMINISTIC (file-shaped) failure hands it
/// back: the file is about to be stamped out of the queue, and a poison
/// pill must not starve healthy files of the pass budget (with `budget`
/// poisoned files ahead of them in the bucket, healthy blob-less files
/// would otherwise NEVER get a slot). Transient failures keep the slot
/// consumed — the walk did the work, and the file retries against a fresh
/// budget next pass.
fn budgeted_backfill(
    budget: &std::sync::atomic::AtomicI64,
    build: impl FnOnce() -> Result<Vec<vortex_index::bloom::FileBloom>, anyhow::Error>,
) -> Result<LoadedBlooms, anyhow::Error> {
    if budget.fetch_sub(1, std::sync::atomic::Ordering::Relaxed) <= 0 {
        return Ok(LoadedBlooms::Deferred);
    }
    match build() {
        Ok(blooms) => Ok(LoadedBlooms::FromDict(blooms)),
        Err(e) => {
            if vortex_index::bloom::is_unbuildable(&e) {
                budget.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            Err(e)
        }
    }
}

/// Rebuild a file's value blooms by streaming its term dictionary — the
/// backfill for files written before the per-file `bloom` blob. Only complete
/// raw-string term fields may seed either per-field filters or composite
/// guards.
fn blooms_from_dictionary(
    reader: &VixReader,
    bloom_fields: &[String],
    fpp: f64,
    composite_scope: &config::VixBloomCompositeScope,
    auto_id_scope: bool,
    bloom_only_never: &HashSet<String>,
    context: &str,
) -> Result<Vec<vortex_index::bloom::FileBloom>, anyhow::Error> {
    let eligible = requested_complete_term_pairs(
        reader,
        bloom_fields,
        composite_scope,
        auto_id_scope,
        bloom_only_never,
        context,
    )?;
    let pairs = eligible
        .iter()
        .filter(|(_id, name)| bloom_fields.iter().any(|field| field == name))
        .cloned()
        .collect();
    let composite_pairs = eligible
        .into_iter()
        .filter(|(_id, name)| {
            composite_scope_allows(composite_scope, auto_id_scope, bloom_only_never, name)
        })
        .collect();
    blooms_from_pairs(reader, pairs, composite_pairs, fpp, context)
}

fn blooms_from_pairs(
    reader: &VixReader,
    pairs: Vec<(u16, String)>,
    composite_pairs: Vec<(u16, String)>,
    fpp: f64,
    context: &str,
) -> Result<Vec<vortex_index::bloom::FileBloom>, anyhow::Error> {
    if pairs.is_empty() && composite_pairs.is_empty() {
        return Ok(Vec::new());
    }
    let wanted = pairs.len();
    let mut acc = vortex_index::bloom::BloomHashAcc::from_pairs(pairs);
    if !composite_pairs.is_empty() {
        acc.enable_composite(composite_pairs);
    }
    // `for_each_term` yields FIELD-MAJOR v2 keys (`{fid BE}{token}`) while
    // the bloom byte form is pinned to v1.
    reader.for_each_term(&mut |key, _doc_count, _rgs| {
        acc.observe_dict_key(key);
        Ok(())
    })?;
    let blooms = finish_backfill_acc(acc, fpp, context)?;
    let per_field_published = blooms
        .iter()
        .filter(|b| b.field != vortex_index::bloom::COMPOSITE_BLOOM_FIELD)
        .count();
    if per_field_published < wanted {
        log::warn!(
            "[COMPACTOR:BLOOM] {context}: {} of {wanted} bloom fields carry no dictionary keys; \
             no filter published for them",
            wanted - per_field_published
        );
    }
    Ok(blooms)
}

/// Finish a walked accumulation. A checked-build refusal (dropped keys,
/// corrupt short keys) is a pure function of the walked bytes —
/// deterministic for the file — so it carries the
/// [`vortex_index::bloom::UnbuildableFile`] poison marker: the caller
/// stamps the file out of the queue instead of retrying it forever.
fn finish_backfill_acc(
    acc: vortex_index::bloom::BloomHashAcc,
    fpp: f64,
    context: &str,
) -> Result<Vec<vortex_index::bloom::FileBloom>, anyhow::Error> {
    acc.build_checked(fpp).map_err(|e| {
        anyhow::Error::new(e)
            .context(vortex_index::bloom::UnbuildableFile)
            .context(format!("{context}: dictionary bloom backfill"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The `.bf` queue stops at the live merge lane: `now − lookback`
    /// floored to the hour, so a `date < cutoff` query excludes exactly the
    /// hours whose files still churn; lookback 0 excludes only the open hour.
    #[test]
    fn settled_hour_cutoff_floors_to_the_lane_boundary() {
        use chrono::TimeZone;
        let now = chrono::Utc
            .with_ymd_and_hms(2026, 9, 28, 17, 43, 21)
            .unwrap();
        assert_eq!(settled_hour_cutoff(now, 2), "2026/09/28/15");
        assert_eq!(settled_hour_cutoff(now, 0), "2026/09/28/17");
        // negative lookbacks clamp to the open-hour exclusion
        assert_eq!(settled_hour_cutoff(now, -5), "2026/09/28/17");
        // crossing a day boundary
        let early = chrono::Utc.with_ymd_and_hms(2026, 9, 29, 0, 10, 0).unwrap();
        assert_eq!(settled_hour_cutoff(early, 2), "2026/09/28/22");
    }

    /// B4 hour selection: precedence and thresholds of the four triggers.
    #[test]
    fn build_trigger_precedence_and_thresholds() {
        let cfg = config::Compact {
            bloom_regroup_min_files: 16,
            bloom_regroup_min_ratio: 0.25,
            bloom_regroup_max_age_hours: 24,
            ..Default::default()
        };
        let h = chrono::Duration::hours;
        // nothing pending: never
        assert_eq!(build_trigger(0, 100, 100, Some(h(100)), &cfg), None);
        // no stamped file yet: first build whatever the counts
        assert_eq!(
            build_trigger(1, 1, 0, Some(h(0)), &cfg),
            Some(BuildTrigger::FirstBuild)
        );
        // count trigger
        assert_eq!(
            build_trigger(16, 1000, 984, Some(h(0)), &cfg),
            Some(BuildTrigger::PendingFiles)
        );
        assert_eq!(build_trigger(15, 1000, 985, Some(h(0)), &cfg), None);
        // ratio trigger (15 of 60 = 0.25)
        assert_eq!(
            build_trigger(15, 60, 45, Some(h(0)), &cfg),
            Some(BuildTrigger::PendingRatio)
        );
        assert_eq!(build_trigger(15, 61, 46, Some(h(0)), &cfg), None);
        // age trigger
        assert_eq!(
            build_trigger(1, 1000, 999, Some(h(24)), &cfg),
            Some(BuildTrigger::PendingAge)
        );
        assert_eq!(build_trigger(1, 1000, 999, Some(h(23)), &cfg), None);
        assert_eq!(build_trigger(1, 1000, 999, None, &cfg), None);
        // a zero min-files knob still needs one pending file
        let eager = config::Compact {
            bloom_regroup_min_files: 0,
            bloom_regroup_min_ratio: 0.25,
            bloom_regroup_max_age_hours: 24,
            ..Default::default()
        };
        assert_eq!(
            build_trigger(1, 1000, 999, None, &eager),
            Some(BuildTrigger::PendingFiles)
        );
    }

    /// B4 chunk fold: within a chunk every field section lands on the
    /// chunk's smallest block count for that field, membership survives the
    /// fold, fields are folded independently and a member without a field
    /// contributes no section for it.
    #[test]
    fn fold_chunk_sections_to_the_smallest_block_count_per_field() {
        use infra::bloom::sbbf::{BLOCK_BYTES, block_index, check_block, hash_value};
        use vortex_index::bloom::FileBloom;

        let section = |field: &str, num_blocks: u32, values: &[&str]| {
            let mut sbbf = infra::bloom::Sbbf::new_with_num_blocks(num_blocks);
            for value in values {
                sbbf.insert(value.as_bytes());
            }
            FileBloom {
                field: field.to_string(),
                num_blocks: sbbf.num_blocks(),
                n_items: values.len() as u32,
                bytes: sbbf.to_bytes(),
            }
        };
        let member = |id: i64, blooms: Vec<FileBloom>| BuildMember {
            fence: (id, 1, 1),
            presence: GroupMember::new(id as u64, vec!["host".into(), "tid".into()]),
            blooms,
            restamp_from: None,
        };
        let chunk = vec![
            member(
                1,
                vec![
                    section("host", 16, &["h-1", "h-2"]),
                    section("tid", 4, &["t-1"]),
                ],
            ),
            member(
                2,
                vec![section("host", 2, &["h-3"]), section("tid", 32, &["t-2"])],
            ),
            member(3, vec![section("host", 8, &["h-4"])]),
        ];
        let folded = fold_chunk_sections(&chunk);
        let find = |file_id: u64, field: &str| {
            folded
                .iter()
                .find(|b| b.file_id == file_id && b.field == field)
                .unwrap_or_else(|| panic!("{file_id}/{field}"))
        };
        for file_id in [1u64, 2, 3] {
            assert_eq!(
                find(file_id, "host").bytes.len(),
                2 * BLOCK_BYTES,
                "host folds to 2"
            );
        }
        assert_eq!(
            find(1, "tid").bytes.len(),
            4 * BLOCK_BYTES,
            "tid folds to 4"
        );
        assert_eq!(find(2, "tid").bytes.len(), 4 * BLOCK_BYTES);
        assert!(
            !folded.iter().any(|b| b.file_id == 3 && b.field == "tid"),
            "no section for a field the member lacks"
        );
        assert_eq!(folded.len(), 5);
        // membership is preserved through the fold
        let probe = |bloom: &FieldBloom, value: &str| {
            let num_blocks = (bloom.bytes.len() / BLOCK_BYTES) as u32;
            let h = hash_value(value.as_bytes());
            let bi = block_index(h, num_blocks) as usize;
            let block: &[u8; BLOCK_BYTES] = bloom.bytes[bi * BLOCK_BYTES..(bi + 1) * BLOCK_BYTES]
                .try_into()
                .unwrap();
            check_block(block, h)
        };
        assert!(probe(find(1, "host"), "h-1"));
        assert!(probe(find(1, "host"), "h-2"));
        assert!(probe(find(2, "host"), "h-3"));
        assert!(probe(find(3, "host"), "h-4"));
        assert!(probe(find(1, "tid"), "t-1"));
        assert!(probe(find(2, "tid"), "t-2"));
        assert_eq!(find(1, "host").n_items, 2);
    }

    fn composite_scope(broad: bool, explicit_fields: &str) -> config::VixBloomCompositeScope {
        config::VixBloomCompositeScope::new(broad, explicit_fields, "")
    }

    #[test]
    fn parse_stream_key_shapes() {
        assert_eq!(
            parse_stream_key("default/traces/default"),
            Some(("default".into(), "traces".into(), "default".into()))
        );
        // stream names may contain slashes only in theory — splitn keeps them
        assert_eq!(
            parse_stream_key("org/logs/a/b"),
            Some(("org".into(), "logs".into(), "a/b".into()))
        );
        assert_eq!(parse_stream_key("only/two"), None);
    }

    #[test]
    fn only_empty_additive_and_disabled_scope_are_not_applicable() {
        let disabled = composite_scope(false, "");
        assert!(!bloom_policy_enabled(&[], &disabled, false));
        assert!(bloom_policy_enabled(
            &["service_name".to_string()],
            &disabled,
            false,
        ));
        assert!(bloom_policy_enabled(
            &[],
            &composite_scope(false, "trace_id"),
            false,
        ));
        assert!(
            bloom_policy_enabled(&[], &disabled, true),
            "empty explicit scope remains active when AUTO ID scope is enabled"
        );
    }

    /// Open one built (data, sidecar) pair.
    fn open_pair(pair: (Vec<u8>, Option<Vec<u8>>)) -> VixReader {
        VixReader::open_with_index(bytes::Bytes::from(pair.0), pair.1.map(bytes::Bytes::from))
            .unwrap()
    }

    /// A `.vix` with no `bloom` blob — the shape this path backfills.
    fn backfill_file(values: &[&str]) -> (Vec<u8>, Option<Vec<u8>>) {
        use std::sync::Arc;

        use arrow::{
            array::{ArrayRef, Int64Array, StringArray},
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        };

        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("trace_id", DataType::Utf8, true),
            Field::new("service_name", DataType::Utf8, true),
        ]));
        let timestamps: Vec<i64> = (0..values.len() as i64).map(|i| 1_000 + i).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(timestamps)) as ArrayRef,
                Arc::new(StringArray::from(values.to_vec())) as ArrayRef,
                Arc::new(StringArray::from(vec!["checkout"; values.len()])) as ArrayRef,
            ],
        )
        .unwrap();
        let source = StringArray::from_iter_values(
            values
                .iter()
                .map(|v| format!("{{\"trace_id\":\"{v}\",\"service_name\":\"checkout\"}}")),
        );
        let mut writer =
            vortex_index::VixWriter::new(&schema, vortex_index::VixWriterOptions::default(), false);
        writer
            .push_batch_with_source(&batch, &source, None)
            .unwrap();
        writer.finish().unwrap()
    }

    /// A blob-less census-shaped file with genuine IDs beside the two
    /// high-cardinality ordinary fields seen in production.
    fn auto_id_backfill_file() -> (Vec<u8>, Option<Vec<u8>>) {
        use std::sync::Arc;

        use arrow::{
            array::{ArrayRef, Int64Array, StringArray},
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        };

        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("reference.parent_trace_id", DataType::Utf8, true),
            Field::new("event_id", DataType::Utf8, true),
            Field::new("events", DataType::Utf8, true),
            Field::new("span_duration_nano", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1_000i64, 1_001])) as ArrayRef,
                Arc::new(StringArray::from(vec!["parent-a", "parent-b"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["event-a", "event-b"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["events-a", "events-b"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["100", "200"])) as ArrayRef,
            ],
        )
        .unwrap();
        let source = StringArray::from_iter_values([
            r#"{"reference.parent_trace_id":"parent-a","event_id":"event-a","events":"events-a","span_duration_nano":"100"}"#,
            r#"{"reference.parent_trace_id":"parent-b","event_id":"event-b","events":"events-b","span_duration_nano":"200"}"#,
        ]);
        let mut writer =
            vortex_index::VixWriter::new(&schema, vortex_index::VixWriterOptions::default(), false);
        writer
            .push_batch_with_source(&batch, &source, None)
            .unwrap();
        writer.finish().unwrap()
    }

    /// A legacy/type-drifted file whose configured ID name is numeric. Its
    /// term dictionary uses canonical tagged bytes, never raw query strings.
    fn numeric_id_file(with_bloom: bool) -> (Vec<u8>, Option<Vec<u8>>) {
        use std::sync::Arc;

        use arrow::{
            array::{ArrayRef, Int64Array},
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        };

        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("trace_id", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1_000i64, 1_001])) as ArrayRef,
                Arc::new(Int64Array::from(vec![41i64, 42])) as ArrayRef,
            ],
        )
        .unwrap();
        let source = arrow::array::StringArray::from_iter_values([
            r#"{"trace_id":41}"#,
            r#"{"trace_id":42}"#,
        ]);
        let options = vortex_index::VixWriterOptions {
            bloom_field_names: with_bloom
                .then(|| vec!["trace_id".to_string()])
                .unwrap_or_default(),
            ..Default::default()
        };
        let mut writer = vortex_index::VixWriter::new(&schema, options, false);
        writer
            .push_batch_with_source(&batch, &source, None)
            .unwrap();
        writer.finish().unwrap()
    }

    /// The backfill converts FIELD-MAJOR dictionary keys into the pinned
    /// bloom key form, so a value the file holds probes as a HIT with the
    /// pruner's own primitives. Before the fix the accumulation recorded
    /// nothing and the published filter rejected every value — and the
    /// group `.bf` is authoritative, so those needles silently vanished.
    #[test]
    fn dictionary_backfill_matches_values_the_file_holds() {
        use infra::bloom::sbbf::{BLOCK_BYTES, block_index, check_block, hash_value};

        let values = ["trace-a", "trace-b", "trace-c"];
        let reader = open_pair(backfill_file(&values));
        assert!(!reader.has_file_blooms(), "the backfill shape has no blob");

        let blooms = blooms_from_dictionary(
            &reader,
            &["trace_id".to_string()],
            0.001,
            &composite_scope(false, ""),
            false,
            &HashSet::new(),
            "unit-test",
        )
        .unwrap();
        assert_eq!(blooms.len(), 1);
        assert_eq!(blooms[0].field, "trace_id");
        assert_eq!(blooms[0].n_items, values.len() as u32);
        for value in values {
            let hash = hash_value(value.as_bytes());
            let index = block_index(hash, blooms[0].num_blocks) as usize;
            let block: &[u8; BLOCK_BYTES] = blooms[0].bytes
                [index * BLOCK_BYTES..(index + 1) * BLOCK_BYTES]
                .try_into()
                .unwrap();
            assert!(
                check_block(block, hash),
                "the group .bf would drop every query for {value}"
            );
        }

        // a configured field the file does not carry yields NO filter at all
        // — never an empty one, which would reject every needle for it
        assert!(
            blooms_from_dictionary(
                &reader,
                &["span_id".to_string()],
                0.001,
                &composite_scope(false, ""),
                false,
                &HashSet::new(),
                "unit-test",
            )
            .unwrap()
            .is_empty()
        );
    }

    /// A `.vix` WITH a per-file `bloom` blob (the blob-transpose shape).
    fn bloom_blob_file(values: &[&str]) -> (Vec<u8>, Option<Vec<u8>>) {
        use std::sync::Arc;

        use arrow::{
            array::{ArrayRef, Int64Array, StringArray},
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        };

        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("trace_id", DataType::Utf8, true),
        ]));
        let timestamps: Vec<i64> = (0..values.len() as i64).map(|i| 1_000 + i).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(timestamps)) as ArrayRef,
                Arc::new(StringArray::from(values.to_vec())) as ArrayRef,
            ],
        )
        .unwrap();
        let source = StringArray::from_iter_values(
            values.iter().map(|v| format!("{{\"trace_id\":\"{v}\"}}")),
        );
        let mut writer = vortex_index::VixWriter::new(
            &schema,
            vortex_index::VixWriterOptions {
                bloom_field_names: vec!["trace_id".to_string()],
                ..Default::default()
            },
            false,
        );
        writer
            .push_batch_with_source(&batch, &source, None)
            .unwrap();
        writer.finish().unwrap()
    }

    /// The poison-pill loop, budget side: a DETERMINISTIC (file-shaped)
    /// failure refunds its fallback-budget slot — the file is about to be
    /// stamped out of the queue, and burning the slot anyway is what let
    /// `budget` poison files starve every healthy blob-less file of the
    /// pass, forever. Transient failures keep the slot consumed (the walk
    /// did the work), and an exhausted budget defers WITHOUT running the
    /// walk at all.
    #[test]
    fn deterministic_backfill_failure_refunds_budget() {
        use std::sync::atomic::{AtomicI64, Ordering};

        let unbuildable = || {
            Err(anyhow::Error::new(vortex_index::VixError::Malformed(
                "doc_count 9 exceeds row_count 1".to_string(),
            ))
            .context(vortex_index::bloom::UnbuildableFile))
        };

        // deterministic failure: slot handed back
        let budget = AtomicI64::new(1);
        assert!(budgeted_backfill(&budget, unbuildable).is_err());
        assert_eq!(
            budget.load(Ordering::Relaxed),
            1,
            "a file that can never build must not consume the shared budget"
        );

        // ...so a healthy file in the same pass still gets the slot
        let reader = open_pair(backfill_file(&["trace-a"]));
        let loaded = budgeted_backfill(&budget, || {
            blooms_from_dictionary(
                &reader,
                &["trace_id".to_string()],
                0.001,
                &composite_scope(false, ""),
                false,
                &HashSet::new(),
                "unit-test",
            )
        })
        .unwrap();
        assert!(matches!(&loaded, LoadedBlooms::FromDict(b) if b.len() == 1));
        assert_eq!(budget.load(Ordering::Relaxed), 0, "a real walk consumes");

        // transient failure: slot stays consumed
        let budget = AtomicI64::new(1);
        assert!(budgeted_backfill(&budget, || Err(anyhow::anyhow!("connection reset"))).is_err());
        assert_eq!(budget.load(Ordering::Relaxed), 0);

        // exhausted budget: deferred, and the walk never runs
        let ran = std::cell::Cell::new(false);
        let loaded = budgeted_backfill(&budget, || {
            ran.set(true);
            Ok(Vec::new())
        })
        .unwrap();
        assert!(matches!(loaded, LoadedBlooms::Deferred));
        assert!(!ran.get(), "no budget, no dictionary stream");
    }

    /// `build_checked`'s refusal is a pure function of the walked bytes, so
    /// the backfill marks it UNBUILDABLE: the bucket loop stamps the file
    /// `bloom_ver = BLOOM_VER_UNBUILDABLE` instead of re-queuing it, which
    /// is what bounds the retries of a deterministic failure to one pass.
    #[test]
    fn tripwire_failures_classify_unbuildable_and_leave_the_queue() {
        let mut acc =
            vortex_index::bloom::BloomHashAcc::from_pairs([(1u16, "trace_id".to_string())]);
        // a 1-byte dictionary key cannot carry a field id: corrupt input
        acc.observe_dict_key(b"\x00");
        let err = finish_backfill_acc(acc, 0.001, "unit-test").unwrap_err();
        assert!(
            vortex_index::bloom::is_unbuildable(&err),
            "the tripwire is deterministic: {err:#}"
        );

        // the stamp takes the file out of the pending queue (`bloom_ver = 0`)
        // while staying on the pruner's "no bloom, keep the file" side
        // (`bloom_ver <= 0`), and stays distinguishable from NOT_APPLICABLE
        assert_ne!(BLOOM_VER_UNBUILDABLE, 0);
        assert!(BLOOM_VER_UNBUILDABLE < 0);
        assert_ne!(BLOOM_VER_UNBUILDABLE, BLOOM_VER_NOT_APPLICABLE);
    }

    /// A corrupt per-file `bloom` blob must not error the file out (and
    /// before the poison handling, wedge it in the queue): the dictionary is
    /// still walkable, so the load falls back to the backfill and produces a
    /// filter that matches the file's values.
    #[test]
    fn corrupt_blob_falls_back_to_the_dictionary() {
        use infra::bloom::sbbf::{BLOCK_BYTES, block_index, check_block, hash_value};

        let (data, index) = bloom_blob_file(&["trace-a", "trace-b"]);
        let mut index = index.expect("sidecar");
        let range = vortex_index::test_support::blob_byte_range(&index, "bloom").unwrap();
        for byte in &mut index[range] {
            *byte = 0xFF;
        }
        let reader = open_pair((data, Some(index)));
        assert!(reader.file_blooms().is_err(), "the blob must be corrupt");

        let budget = std::sync::atomic::AtomicI64::new(1);
        let loaded = load_blooms_sync(
            &reader,
            &["trace_id".to_string()],
            0.001,
            &composite_scope(false, ""),
            false,
            &HashSet::new(),
            "unit-test",
            &budget,
        )
        .unwrap();
        let LoadedBlooms::FromDict(blooms) = loaded else {
            panic!("expected the dictionary fallback");
        };
        assert_eq!(blooms.len(), 1);
        for value in ["trace-a", "trace-b"] {
            let hash = hash_value(value.as_bytes());
            let index = block_index(hash, blooms[0].num_blocks) as usize;
            let block: &[u8; BLOCK_BYTES] = blooms[0].bytes
                [index * BLOCK_BYTES..(index + 1) * BLOCK_BYTES]
                .try_into()
                .unwrap();
            assert!(check_block(block, hash), "backfill missed {value}");
        }

        // and an INTACT blob still short-circuits to the transpose path
        // without touching the budget
        let reader = open_pair(bloom_blob_file(&["trace-a"]));
        let budget = std::sync::atomic::AtomicI64::new(0);
        let loaded = load_blooms_sync(
            &reader,
            &["trace_id".to_string()],
            0.001,
            &composite_scope(false, ""),
            false,
            &HashSet::new(),
            "unit-test",
            &budget,
        )
        .unwrap();
        assert!(matches!(&loaded, LoadedBlooms::FromBlob(b) if b.len() == 1));
        assert_eq!(budget.load(std::sync::atomic::Ordering::Relaxed), 0);
    }

    /// #48: with the composite enabled, the SAME dictionary walk covers
    /// every term field of the file — no per-stream `bloom_filter_fields`
    /// needed, and legacy blob-less files gain any-field pruning on their
    /// next `.bf` build without a rewrite.
    #[test]
    fn dictionary_backfill_builds_composite_without_configured_fields() {
        use infra::bloom::sbbf::{BLOCK_BYTES, block_index, check_block, hash_value};
        use vortex_index::bloom::{
            COMPOSITE_BLOOM_FIELD, COMPOSITE_GUARD_PROBES, composite_guard_key, composite_value_key,
        };

        let reader = open_pair(backfill_file(&["trace-a", "trace-b"]));
        // no per-stream bloom fields at all — broad scope alone builds
        let blooms = blooms_from_dictionary(
            &reader,
            &[],
            0.001,
            &composite_scope(true, ""),
            false,
            &HashSet::new(),
            "unit-test",
        )
        .unwrap();
        assert_eq!(blooms.len(), 1);
        assert_eq!(blooms[0].field, COMPOSITE_BLOOM_FIELD);

        let probe = |key: &[u8]| {
            let hash = hash_value(key);
            let index = block_index(hash, blooms[0].num_blocks) as usize;
            let block: &[u8; BLOCK_BYTES] = blooms[0].bytes
                [index * BLOCK_BYTES..(index + 1) * BLOCK_BYTES]
                .try_into()
                .unwrap();
            check_block(block, hash)
        };
        let mut buf = Vec::new();
        // the pruner's own probe key form finds the file's values…
        assert!(probe(
            composite_value_key("trace_id", b"trace-a", &mut buf).unwrap()
        ));
        // …misses absent ones…
        assert!(!probe(
            composite_value_key("trace_id", b"absent", &mut buf).unwrap()
        ));
        // …and the covered field's guards all hit, while an uncovered
        // field's don't (that miss is what keeps files instead of dropping
        // them on fields that were never term-indexed)
        for p in 0..COMPOSITE_GUARD_PROBES {
            assert!(probe(composite_guard_key("trace_id", p, &mut buf).unwrap()));
        }
        let uncovered_hits = (0..COMPOSITE_GUARD_PROBES)
            .filter(|&p| probe(composite_guard_key("severity", p, &mut buf).unwrap()))
            .count();
        assert!(uncovered_hits < COMPOSITE_GUARD_PROBES as usize);
    }

    #[test]
    fn selective_scope_backfills_only_explicit_ids_and_keeps_additive_fields_separate() {
        use infra::bloom::sbbf::{BLOCK_BYTES, block_index, check_block, hash_value};
        use vortex_index::bloom::{
            COMPOSITE_BLOOM_FIELD, COMPOSITE_GUARD_PROBES, composite_guard_key, composite_value_key,
        };

        let reader = open_pair(backfill_file(&["trace-a", "trace-b"]));
        assert!(reader.term_field_id("trace_id").is_some());
        assert!(reader.term_field_id("service_name").is_some());

        let scope = composite_scope(false, "trace_id");
        let budget = std::sync::atomic::AtomicI64::new(1);
        let loaded = load_blooms_sync(
            &reader,
            &["service_name".to_string()],
            0.001,
            &scope,
            false,
            &HashSet::new(),
            "unit-test",
            &budget,
        )
        .unwrap();
        let LoadedBlooms::FromDict(blooms) = loaded else {
            panic!("an eligible explicit ID must trigger dictionary backfill");
        };
        assert_eq!(
            budget.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "the eligible backfill consumes one slot"
        );
        assert!(
            blooms.iter().any(|b| b.field == "service_name"),
            "additive fields retain their independent per-field bloom"
        );
        let composite = blooms
            .iter()
            .find(|b| b.field == COMPOSITE_BLOOM_FIELD)
            .expect("explicit Bloom-only ID remains composite-eligible");
        let probe = |key: &[u8]| {
            let hash = hash_value(key);
            let index = block_index(hash, composite.num_blocks) as usize;
            let block: &[u8; BLOCK_BYTES] = composite.bytes
                [index * BLOCK_BYTES..(index + 1) * BLOCK_BYTES]
                .try_into()
                .unwrap();
            check_block(block, hash)
        };
        let mut buf = Vec::new();
        assert!(probe(
            composite_value_key("trace_id", b"trace-a", &mut buf).unwrap()
        ));
        for p in 0..COMPOSITE_GUARD_PROBES {
            assert!(probe(composite_guard_key("trace_id", p, &mut buf).unwrap()));
        }
        let ordinary_guard_hits = (0..COMPOSITE_GUARD_PROBES)
            .filter(|&p| probe(composite_guard_key("service_name", p, &mut buf).unwrap()))
            .count();
        assert!(
            ordinary_guard_hits < COMPOSITE_GUARD_PROBES as usize,
            "an additive ordinary field must not gain composite coverage"
        );

        // A selective policy with no matching term capability neither walks
        // the dictionary nor consumes the shared fallback budget.
        let budget = std::sync::atomic::AtomicI64::new(1);
        let loaded = load_blooms_sync(
            &reader,
            &[],
            0.001,
            &composite_scope(false, "span_id"),
            false,
            &HashSet::new(),
            "unit-test",
            &budget,
        )
        .unwrap();
        assert!(matches!(&loaded, LoadedBlooms::FromBlob(b) if b.is_empty()));
        assert_eq!(budget.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[test]
    fn auto_id_scope_backfills_only_semantic_id_fields() {
        let reader = open_pair(auto_id_backfill_file());
        let scope = composite_scope(false, "");
        let never = HashSet::new();
        let budget = std::sync::atomic::AtomicI64::new(1);
        let loaded = load_blooms_sync(
            &reader,
            &[],
            0.001,
            &scope,
            true,
            &never,
            "unit-test",
            &budget,
        )
        .unwrap();
        let LoadedBlooms::FromDict(blooms) = loaded else {
            panic!("AUTO ID scope must trigger dictionary backfill");
        };
        assert_eq!(
            budget.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "the dictionary backfill consumes one slot"
        );
        let composite = blooms
            .iter()
            .find(|b| b.field == vortex_index::bloom::COMPOSITE_BLOOM_FIELD)
            .expect("AUTO ID scope must build a composite");

        assert!(composite_covers_field(
            composite,
            "reference.parent_trace_id"
        ));
        assert!(composite_covers_field(composite, "event_id"));
        assert!(!composite_covers_field(composite, "events"));
        assert!(!composite_covers_field(composite, "span_duration_nano"));
        assert_eq!(
            blooms.len(),
            1,
            "ordinary fields must not gain independent coverage either"
        );

        let never = HashSet::from(["event_id".to_string()]);
        let denied =
            blooms_from_dictionary(&reader, &[], 0.001, &scope, true, &never, "unit-test").unwrap();
        let composite = denied
            .iter()
            .find(|b| b.field == vortex_index::bloom::COMPOSITE_BLOOM_FIELD)
            .expect("the remaining semantic ID still builds");
        assert!(composite_covers_field(
            composite,
            "reference.parent_trace_id"
        ));
        assert!(
            !composite_covers_field(composite, "event_id"),
            "NEVER must override semantic AUTO admission"
        );
    }

    #[test]
    fn disabled_auto_id_scope_remains_explicit_only() {
        let reader = open_pair(auto_id_backfill_file());
        let never = HashSet::new();
        let explicit = composite_scope(false, "event_id");
        let blooms =
            blooms_from_dictionary(&reader, &[], 0.001, &explicit, false, &never, "unit-test")
                .unwrap();
        let composite = blooms
            .iter()
            .find(|b| b.field == vortex_index::bloom::COMPOSITE_BLOOM_FIELD)
            .expect("the explicit field remains authoritative");
        assert!(composite_covers_field(composite, "event_id"));
        assert!(!composite_covers_field(
            composite,
            "reference.parent_trace_id"
        ));
        assert!(!composite_covers_field(composite, "events"));
        assert!(!composite_covers_field(composite, "span_duration_nano"));

        assert!(
            blooms_from_dictionary(
                &reader,
                &[],
                0.001,
                &composite_scope(false, ""),
                false,
                &never,
                "unit-test",
            )
            .unwrap()
            .is_empty(),
            "gate=false with no explicit fields must remain disabled"
        );
    }

    #[test]
    fn old_broad_blob_bits_do_not_authorize_ordinary_fields_in_auto_id_scope() {
        let reader = open_pair(auto_id_backfill_file());
        let never = HashSet::new();
        let broad = blooms_from_dictionary(
            &reader,
            &[],
            0.001,
            &composite_scope(true, ""),
            false,
            &never,
            "unit-test",
        )
        .unwrap();
        let old_composite = broad
            .iter()
            .find(|b| b.field == vortex_index::bloom::COMPOSITE_BLOOM_FIELD)
            .expect("broad historical blob");
        assert!(composite_covers_field(old_composite, "events"));
        assert!(composite_covers_field(old_composite, "span_duration_nano"));

        let budget = std::sync::atomic::AtomicI64::new(0);
        let loaded = retain_and_supplement_blob(
            &reader,
            broad.clone(),
            &[],
            0.001,
            &composite_scope(false, ""),
            true,
            &never,
            "unit-test",
            &budget,
        )
        .unwrap();
        assert!(matches!(loaded, LoadedBlooms::FromBlob(retained) if retained == broad));
        assert!(composite_scope_allows(
            &composite_scope(false, ""),
            true,
            &never,
            "reference.parent_trace_id",
        ));
        assert!(composite_scope_allows(
            &composite_scope(false, ""),
            true,
            &never,
            "event_id",
        ));
        assert!(!composite_scope_allows(
            &composite_scope(false, ""),
            true,
            &never,
            "events",
        ));
        assert!(!composite_scope_allows(
            &composite_scope(false, ""),
            true,
            &never,
            "span_duration_nano",
        ));
    }

    #[test]
    fn auto_id_scope_supplements_missing_id_coverage_only() {
        use std::sync::atomic::{AtomicI64, Ordering};

        let reader = open_pair(auto_id_backfill_file());
        let never = HashSet::new();
        let old = blooms_from_dictionary(
            &reader,
            &[],
            0.001,
            &composite_scope(false, "event_id"),
            false,
            &never,
            "unit-test",
        )
        .unwrap();
        let old_composite = old[0].clone();
        assert!(composite_covers_field(&old_composite, "event_id"));
        assert!(!composite_covers_field(
            &old_composite,
            "reference.parent_trace_id"
        ));

        let budget = AtomicI64::new(1);
        let loaded = retain_and_supplement_blob(
            &reader,
            old,
            &[],
            0.001,
            &composite_scope(false, ""),
            true,
            &never,
            "unit-test",
            &budget,
        )
        .unwrap();
        let LoadedBlooms::FromDict(supplemented) = loaded else {
            panic!("missing semantic ID must trigger a safe dictionary supplement");
        };
        assert_eq!(budget.load(Ordering::Relaxed), 0);
        assert!(supplemented.iter().any(|b| b == &old_composite));
        let parent = supplemented
            .iter()
            .find(|b| b.field == "reference.parent_trace_id")
            .expect("missing AUTO ID receives an independent per-field supplement");
        assert!(file_bloom_might_contain(parent, b"parent-a"));
        assert!(!file_bloom_might_contain(parent, b"absent"));
        assert!(!supplemented.iter().any(|b| b.field == "events"));
        assert!(!supplemented.iter().any(|b| b.field == "span_duration_nano"));
    }

    #[test]
    fn numeric_type_drift_never_seeds_authoritative_bloom_coverage() {
        use std::sync::atomic::{AtomicI64, Ordering};

        let reader = open_pair(numeric_id_file(false));
        assert!(
            reader.has_term_capability("trace_id"),
            "precondition: the legacy numeric field has a term dictionary"
        );
        for scope in [
            composite_scope(false, "trace_id"),
            composite_scope(true, ""),
        ] {
            assert!(
                !has_requested_term_capability(
                    &reader,
                    &[],
                    &scope,
                    false,
                    &HashSet::new(),
                    "unit-test",
                )
                .unwrap(),
                "numeric terms cannot satisfy selective or broad raw-string scope"
            );
            assert!(
                blooms_from_dictionary(
                    &reader,
                    &[],
                    0.001,
                    &scope,
                    false,
                    &HashSet::new(),
                    "unit-test",
                )
                .unwrap()
                .is_empty(),
                "numeric canonical bytes must not seed composite guards"
            );
        }

        // The same type gate protects the additive per-field path when an ID
        // setting outlives a historical schema incarnation.
        let budget = AtomicI64::new(1);
        let loaded = load_blooms_sync(
            &reader,
            &["trace_id".to_string()],
            0.001,
            &composite_scope(false, ""),
            false,
            &HashSet::new(),
            "unit-test",
            &budget,
        )
        .unwrap();
        assert!(matches!(loaded, LoadedBlooms::FromBlob(b) if b.is_empty()));
        assert_eq!(
            budget.load(Ordering::Relaxed),
            1,
            "an unusable numeric dictionary must not consume fallback budget"
        );

        // The writer now refuses the per-field section for a non-string
        // bloom field (same gate as the composite), so a configured numeric
        // ID yields no blob at all — the assembler's own type gate above
        // stays as defence in depth for files older binaries wrote.
        assert!(
            !open_pair(numeric_id_file(true)).has_file_blooms(),
            "a numeric bloom field must not publish a per-field section"
        );
    }

    #[test]
    fn partial_id_dictionary_stays_uncovered_and_fail_open() {
        use std::sync::atomic::{AtomicI64, Ordering};

        let (data, index) = backfill_file(&["trace-a", "trace-b"]);
        let index = vortex_index::test_support::repack_with_partial_fields(
            &index.expect("sidecar"),
            &["trace_id"],
        )
        .unwrap();
        let reader = open_pair((data, Some(index)));
        assert!(reader.has_term_capability("trace_id"));
        assert!(reader.partial_fields().contains("trace_id"));

        let selective = composite_scope(false, "trace_id");
        assert!(
            !has_requested_term_capability(
                &reader,
                &[],
                &selective,
                false,
                &HashSet::new(),
                "unit-test",
            )
            .unwrap()
        );
        assert!(
            blooms_from_dictionary(
                &reader,
                &[],
                0.001,
                &selective,
                false,
                &HashSet::new(),
                "unit-test",
            )
            .unwrap()
            .is_empty(),
            "an incomplete requested ID must not seed selective guards"
        );

        // Broad mode may still cover other complete raw-string fields, but
        // the partial ID itself must remain unguarded.
        let broad = composite_scope(true, "");
        assert!(
            has_requested_term_capability(
                &reader,
                &[],
                &broad,
                false,
                &HashSet::new(),
                "unit-test",
            )
            .unwrap()
        );
        let broad_blooms = blooms_from_dictionary(
            &reader,
            &[],
            0.001,
            &broad,
            false,
            &HashSet::new(),
            "unit-test",
        )
        .unwrap();
        let composite = broad_blooms
            .iter()
            .find(|b| b.field == vortex_index::bloom::COMPOSITE_BLOOM_FIELD)
            .expect("the complete service_name field remains broadly coverable");
        assert!(composite_covers_field(composite, "service_name"));
        assert!(!composite_covers_field(composite, "trace_id"));
        let budget = AtomicI64::new(1);
        let loaded = load_blooms_sync(
            &reader,
            &[],
            0.001,
            &composite_scope(false, "trace_id"),
            false,
            &HashSet::new(),
            "unit-test",
            &budget,
        )
        .unwrap();
        assert!(matches!(loaded, LoadedBlooms::FromBlob(b) if b.is_empty()));
        assert_eq!(budget.load(Ordering::Relaxed), 1);

        let (data, index) = bloom_blob_file(&["trace-a", "trace-b"]);
        let index = vortex_index::test_support::repack_with_partial_fields(
            &index.expect("sidecar"),
            &["trace_id"],
        )
        .unwrap();
        let blob_reader = open_pair((data, Some(index)));
        let budget = AtomicI64::new(0);
        let loaded = load_blooms_sync(
            &blob_reader,
            &["trace_id".to_string()],
            0.001,
            &composite_scope(false, ""),
            false,
            &HashSet::new(),
            "unit-test",
            &budget,
        )
        .unwrap();
        assert!(
            matches!(loaded, LoadedBlooms::FromBlob(b) if b.is_empty()),
            "an old partial per-field blob must be dropped rather than republished"
        );
    }

    #[test]
    fn scope_expansion_supplements_missing_field_without_replacing_old_composite() {
        use std::sync::atomic::{AtomicI64, Ordering};

        let reader = open_pair(backfill_file(&["trace-a", "trace-b"]));
        let old = blooms_from_dictionary(
            &reader,
            &[],
            0.001,
            &composite_scope(false, "trace_id"),
            false,
            &HashSet::new(),
            "unit-test",
        )
        .unwrap();
        assert_eq!(old.len(), 1);
        let old_composite = old[0].clone();
        assert!(composite_covers_field(&old_composite, "trace_id"));
        assert!(!composite_covers_field(&old_composite, "service_name"));

        // Scope A reuses the exact bytes and does not spend a walk.
        let budget = AtomicI64::new(0);
        let reused = retain_and_supplement_blob(
            &reader,
            old.clone(),
            &[],
            0.001,
            &composite_scope(false, "trace_id"),
            false,
            &HashSet::new(),
            "unit-test",
            &budget,
        )
        .unwrap();
        assert!(matches!(reused, LoadedBlooms::FromBlob(b) if b == old));

        // Expanding A -> A,B detects B's missing guards, preserves A's whole
        // historical composite, and publishes B as a safe per-field section.
        let budget = AtomicI64::new(1);
        let expanded = retain_and_supplement_blob(
            &reader,
            vec![old_composite.clone()],
            &[],
            0.001,
            &composite_scope(false, "trace_id,service_name"),
            false,
            &HashSet::new(),
            "unit-test",
            &budget,
        )
        .unwrap();
        let LoadedBlooms::FromDict(expanded) = expanded else {
            panic!("scope expansion must budget a dictionary supplement");
        };
        assert_eq!(budget.load(Ordering::Relaxed), 0);
        assert_eq!(
            expanded
                .iter()
                .filter(|b| b.field == vortex_index::bloom::COMPOSITE_BLOOM_FIELD)
                .collect::<Vec<_>>(),
            vec![&old_composite],
            "the old composite must survive byte-for-byte"
        );
        let service = expanded
            .iter()
            .find(|b| b.field == "service_name")
            .expect("B receives an independent per-field supplement");
        assert!(file_bloom_might_contain(service, b"checkout"));
        assert!(!file_bloom_might_contain(service, b"absent"));
    }

    #[test]
    fn scope_expansion_without_complete_term_source_remains_fail_open() {
        use std::sync::atomic::{AtomicI64, Ordering};

        let pair = backfill_file(&["trace-a", "trace-b"]);
        let complete_reader = open_pair(pair.clone());
        let old = blooms_from_dictionary(
            &complete_reader,
            &[],
            0.001,
            &composite_scope(false, "trace_id"),
            false,
            &HashSet::new(),
            "unit-test",
        )
        .unwrap();
        let (data, index) = pair;
        let index = vortex_index::test_support::repack_with_partial_fields(
            &index.expect("sidecar"),
            &["service_name"],
        )
        .unwrap();
        let partial_reader = open_pair((data, Some(index)));
        assert!(partial_reader.partial_fields().contains("service_name"));

        let budget = AtomicI64::new(1);
        let loaded = retain_and_supplement_blob(
            &partial_reader,
            old.clone(),
            &[],
            0.001,
            &composite_scope(false, "trace_id,service_name"),
            false,
            &HashSet::new(),
            "unit-test",
            &budget,
        )
        .unwrap();
        let LoadedBlooms::FromBlob(retained) = loaded else {
            panic!("irrecoverable B must stay fail-open without a dictionary walk");
        };
        assert_eq!(retained, old);
        assert!(!retained.iter().any(|b| b.field == "service_name"));
        assert_eq!(budget.load(Ordering::Relaxed), 1);
    }

    /// A pre-composite blob whose per-field section already covers the
    /// requested field is immediately usable: per-field probes take
    /// precedence, so manufacturing a replacement composite would only
    /// waste a dictionary walk.
    #[test]
    fn pre_composite_per_field_coverage_short_circuits_safely() {
        use std::sync::atomic::{AtomicI64, Ordering};

        let reader = open_pair(bloom_blob_file(&["trace-a", "trace-b"]));
        assert!(reader.has_file_blooms());

        let budget = AtomicI64::new(1);
        let loaded = load_blooms_sync(
            &reader,
            &["trace_id".to_string()],
            0.001,
            &composite_scope(true, ""),
            false,
            &HashSet::new(),
            "unit-test",
            &budget,
        )
        .unwrap();
        let LoadedBlooms::FromBlob(blooms) = loaded else {
            panic!("existing per-field coverage must avoid a dictionary walk");
        };
        assert_eq!(blooms.len(), 1);
        assert_eq!(blooms[0].field, "trace_id");
        assert_eq!(budget.load(Ordering::Relaxed), 1);
    }

    /// A writer-built broad composite remains transposable after the policy
    /// narrows. Retaining historical bytes does not authorize their use:
    /// query pruning separately applies the current scope.
    #[test]
    fn old_broad_blob_is_retained_after_scope_narrows() {
        let writer = {
            use std::sync::Arc;

            use arrow::{
                array::{ArrayRef, Int64Array, StringArray},
                datatypes::{DataType, Field, Schema},
                record_batch::RecordBatch,
            };

            let schema = Arc::new(Schema::new(vec![
                Field::new("_timestamp", DataType::Int64, false),
                Field::new("trace_id", DataType::Utf8, true),
            ]));
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(vec![1_000i64, 1_001])) as ArrayRef,
                    Arc::new(StringArray::from(vec!["trace-a", "trace-b"])) as ArrayRef,
                ],
            )
            .unwrap();
            let source = StringArray::from_iter_values(
                ["trace-a", "trace-b"]
                    .iter()
                    .map(|v| format!("{{\"trace_id\":\"{v}\"}}")),
            );
            let mut writer = vortex_index::VixWriter::new(
                &schema,
                vortex_index::VixWriterOptions {
                    bloom_composite: true,
                    ..Default::default()
                },
                false,
            );
            writer
                .push_batch_with_source(&batch, &source, None)
                .unwrap();
            writer
        };
        let reader = open_pair(writer.finish().unwrap());
        assert!(reader.has_file_blooms(), "composite alone produces a blob");

        let budget = std::sync::atomic::AtomicI64::new(0);
        let loaded = load_blooms_sync(
            &reader,
            &[],
            0.001,
            &composite_scope(false, "trace_id"),
            false,
            &HashSet::new(),
            "unit-test",
            &budget,
        )
        .unwrap();
        let LoadedBlooms::FromBlob(blooms) = loaded else {
            panic!("expected the blob transpose path");
        };
        assert_eq!(blooms.len(), 1);
        assert_eq!(blooms[0].field, vortex_index::bloom::COMPOSITE_BLOOM_FIELD);
        assert!(composite_covers_field(&blooms[0], "trace_id"));
    }
}
