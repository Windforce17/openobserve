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
    bloom::{BloomWriter, FieldBloom},
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

/// Cap files per `.bf` chunk: keeps the footer well under the pruner's
/// 16 KiB suffix probe (per file ≈ 12 footer bytes per field).
const MAX_FILES_PER_BF: usize = 256;

/// One pass of the builder: drain up to `compact.bloom_build_batch` pending
/// `(stream, hour)` buckets. Called on the compactor's job cadence.
pub async fn run() -> Result<(), anyhow::Error> {
    let cfg = get_config();
    if !cfg.common.bloom_filter_enabled {
        return Ok(());
    }
    // exclude the still-open hour: it re-pends on every fresh file and
    // would hog the head of the date-DESC queue forever
    let current_hour = config::utils::time::now().format("%Y/%m/%d/%H").to_string();
    let buckets =
        infra::file_list::query_bloom_pending_buckets(&current_hour, cfg.compact.bloom_build_batch)
            .await?;
    let total = buckets.len();
    let started = std::time::Instant::now();
    FALLBACK_BUDGET.store(
        cfg.compact.bloom_build_fallback_budget,
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
             {failed} failed) in {:?}",
            started.elapsed()
        );
    }
    Ok(())
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
    // re-check under the lock (another compactor may have built it)
    let files = infra::file_list::query_for_bloom(org_id, stream_type, stream_name, date).await?;
    if files.is_empty() {
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
        let expected: Vec<(i64, i64, i64)> = files
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

    let started = std::time::Instant::now();
    log::info!(
        "[COMPACTOR:BLOOM] {org_id}/{stream_type}/{stream_name}/{date}: building bucket, {} \
         files pending",
        files.len()
    );
    // Collect per-file blooms CONCURRENTLY: blob transpose for capability
    // files (cheap ranged fetch), one dictionary stream for older ones —
    // the expensive backfill path, so it is CAPPED per pass: leftover files
    // stay bloom_ver=0 and become their own `.bf` chunk on a later pass
    // (multiple chunks per bucket are first-class in the format).
    const LOAD_CONCURRENCY: usize = 8;
    use futures::{StreamExt, stream};
    type BloomFence = (i64, i64, i64);
    let results: Vec<(BloomFence, String, Result<LoadedBlooms, anyhow::Error>)> =
        stream::iter(files.iter().cloned().map(|file| {
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
    let mut per_file: Vec<(BloomFence, Vec<vortex_index::bloom::FileBloom>)> = Vec::new();
    let mut not_applicable: Vec<BloomFence> = Vec::new();
    let mut unbuildable: Vec<BloomFence> = Vec::new();
    let mut fallback_streams = 0usize;
    let mut deferred = 0usize;
    for (fence, key, result) in results {
        match result {
            Ok(LoadedBlooms::FromBlob(blooms)) if blooms.is_empty() => {
                not_applicable.push(fence);
            }
            Ok(LoadedBlooms::FromBlob(blooms)) => per_file.push((fence, blooms)),
            Ok(LoadedBlooms::FromDict(blooms)) => {
                fallback_streams += 1;
                if blooms.is_empty() {
                    not_applicable.push(fence);
                } else {
                    per_file.push((fence, blooms));
                }
            }
            Ok(LoadedBlooms::Deferred) => deferred += 1,
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
    let _ = deferred;
    // Stamp poison files before chunk building. Only rows whose sidecar
    // identity still matches are counted; healed rows remain pending.
    let unbuildable_matched = if unbuildable.is_empty() {
        0
    } else {
        infra::file_list::update_bloom_ver(&unbuildable, BLOOM_VER_UNBUILDABLE).await?
    };

    // chunk by the per-field num_blocks signature (a field section of one
    // .bf must be block-uniform across its files)
    let mut chunks: HashMap<
        Vec<(String, u32)>,
        Vec<(BloomFence, Vec<vortex_index::bloom::FileBloom>)>,
    > = HashMap::new();
    for (fence, blooms) in per_file {
        let mut sig: Vec<(String, u32)> = blooms
            .iter()
            .map(|b| (b.field.clone(), b.num_blocks))
            .collect();
        sig.sort();
        chunks.entry(sig).or_default().push((fence, blooms));
    }

    let base_ver = now_micros();
    let mut chunk_idx: i64 = 0;
    let mut built = 0usize;
    for (_sig, mut chunk_files) in chunks {
        chunk_files.sort_by_key(|(fence, _)| fence.0);
        for sub in chunk_files.chunks(MAX_FILES_PER_BF) {
            let bloom_ver = base_ver + chunk_idx;
            chunk_idx += 1;
            let mut field_blooms: Vec<FieldBloom> = Vec::new();
            for (fence, blooms) in sub {
                for b in blooms {
                    field_blooms.push(FieldBloom {
                        field: b.field.clone(),
                        file_id: fence.0 as u64,
                        n_items: b.n_items,
                        bytes: b.bytes.clone(),
                    });
                }
            }
            let blob = BloomWriter::serialize(field_blooms)
                .map_err(|e| anyhow::anyhow!("serialize .bf: {e}"))?;
            let path =
                infra::bloom::path::bloom_path(org_id, stream_type, stream_name, date, bloom_ver);
            let account = infra::storage::get_account(org_id, &path).unwrap_or_default();
            infra::storage::put(&account, &path, bytes::Bytes::from(blob)).await?;
            let expected: Vec<BloomFence> = sub.iter().map(|(fence, _)| *fence).collect();
            let matched = infra::file_list::update_bloom_ver(&expected, bloom_ver).await?;
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
    }
    let not_applicable_matched = if not_applicable.is_empty() {
        0
    } else {
        infra::file_list::update_bloom_ver(&not_applicable, BLOOM_VER_NOT_APPLICABLE).await?
    };
    log::info!(
        "[COMPACTOR:BLOOM] {org_id}/{stream_type}/{stream_name}/{date}: built .bf for {built} \
         files ({chunk_idx} chunks, {fallback_streams} dictionary fallbacks, \
         {not_applicable_matched} not applicable, {unbuildable_matched} unbuildable) in {:?}",
        started.elapsed()
    );
    Ok(())
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

async fn load_file_blooms(
    file: &FileKey,
    bloom_fields: &[String],
    fpp: f64,
    composite_scope: &config::VixBloomCompositeScope,
    auto_id_scope: bool,
    bloom_only_never: Arc<HashSet<String>>,
) -> Result<LoadedBlooms, anyhow::Error> {
    let handle = tokio::runtime::Handle::current();
    let source: Arc<dyn vortex_index::VixRangeSource> = Arc::new(HealProbeRangeSource {
        account: file.account.clone(),
        location: object_store::path::Path::from(file.key.as_str()),
        // a .vix FileMeta's compressed_size is the exact DATA-object size
        size: file.meta.compressed_size as u64,
        handle: handle.clone(),
        cancel: None,
    });
    // v3 split: the per-file bloom blob (and the dictionary the backfill
    // streams) live in the `.vxi` SIDECAR, fetched by range. The pending
    // queue selects `index_size > 0` rows only, so the sidecar exists for
    // every file reaching here; index-less files keep their existing
    // sentinel path upstream.
    let index_source: Option<Arc<dyn vortex_index::VixRangeSource>> =
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
            });
    let bloom_fields = bloom_fields.to_vec();
    let composite_scope = (*composite_scope).clone();
    let file_key = file.key.clone();
    tokio::task::spawn_blocking(move || {
        let reader = VixReader::open_ranged_with_index(source, index_source)?;
        load_blooms_sync(
            &reader,
            &bloom_fields,
            fpp,
            &composite_scope,
            auto_id_scope,
            &bloom_only_never,
            &file_key,
            &FALLBACK_BUDGET,
        )
    })
    .await?
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
            let additive = bloom_fields.iter().any(|field| field == name);
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

/// Resolve the only dictionary fields whose raw bytes can authoritatively
/// answer query-literal Bloom probes. Missing docs schema/type information,
/// non-string types and partial dictionaries all degrade to no coverage.
fn docs_schema_error_is_deterministic(error: &anyhow::Error) -> bool {
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
        let deterministic = docs_schema_error_is_deterministic(&e);
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

        let blob_reader = open_pair(numeric_id_file(true));
        assert!(blob_reader.has_file_blooms());
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
            "an old numeric per-field blob must be dropped rather than republished"
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
