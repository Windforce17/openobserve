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
//! - older files fall back to ONE full term-dictionary stream that hashes the configured fields'
//!   values (the one-time backfill cost);
//! - files of streams with no `bloom_filter_fields` configured are stamped with the NO_BLOOM
//!   sentinel so the queue drains (the pruner treats `bloom_ver <= 0` as "no bloom", never forming
//!   a `.bf` path);
//! - files whose own bytes fail validation (a DETERMINISTIC failure — corrupt dictionary/terms/
//!   bloom blob, or a checked build that refuses to publish) are stamped
//!   [`BLOOM_VER_UNBUILDABLE`]: retrying can never succeed, so they leave the queue after one
//!   attempt instead of spinning forever and burning the fallback budget every pass.
//!
//! Group invariants (see `infra::bloom`): every field section shares one
//! `num_blocks B` across its files, so files are chunked by their per-field
//! B signature (writer sizing is power-of-two, so real hours produce a
//! couple of chunks at most); each chunk becomes its own `.bf` named
//! `bloom_ver = base_ts + chunk_idx`, and `update_bloom_ver` stamps the
//! chunk's file_list rows LAST — a crash before the stamp just re-queues.

use std::sync::Arc;

use config::{get_config, meta::stream::FileKey, utils::time::now_micros};
use hashbrown::HashMap;
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
    let (mut done, mut busy, mut failed) = (0usize, 0usize, 0usize);
    for (stream_key, date) in buckets {
        let Some((org_id, stream_type, stream_name)) = parse_stream_key(&stream_key) else {
            log::warn!("[COMPACTOR:BLOOM] unparsable stream key {stream_key:?}, skipping");
            continue;
        };
        match process_bucket(&org_id, &stream_type, &stream_name, &date).await {
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
    let result = process_bucket_locked(org_id, stream_type, stream_name, date).await;
    dist_lock::unlock(&locker).await?;
    result.map(|_| true)
}

async fn process_bucket_locked(
    org_id: &str,
    stream_type: config::meta::stream::StreamType,
    stream_name: &str,
    date: &str,
) -> Result<(), anyhow::Error> {
    // re-check under the lock (another compactor may have built it)
    let files = infra::file_list::query_for_bloom(org_id, stream_type, stream_name, date).await?;
    if files.is_empty() {
        return Ok(());
    }

    let latest_schema = infra::schema::get(org_id, stream_name, stream_type).await?;
    let stream_settings = unwrap_stream_settings(&latest_schema);
    let bloom_fields = get_stream_setting_bloom_filter_fields(&stream_settings);
    if bloom_fields.is_empty() {
        // nothing to build for this stream — drain the queue
        let ids: Vec<i64> = files.iter().map(|f| f.id).collect();
        infra::file_list::update_bloom_ver(&ids, BLOOM_VER_NOT_APPLICABLE).await?;
        log::info!(
            "[COMPACTOR:BLOOM] {org_id}/{stream_type}/{stream_name}/{date}: no bloom fields \
             configured, {} files marked not-applicable",
            ids.len()
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
    let results: Vec<(i64, String, Result<LoadedBlooms, anyhow::Error>)> =
        stream::iter(files.iter().cloned().map(|file| {
            let bloom_fields = bloom_fields.clone();
            async move {
                let loaded = load_file_blooms(&file, &bloom_fields).await;
                (file.id, file.key, loaded)
            }
        }))
        .buffer_unordered(LOAD_CONCURRENCY)
        .collect()
        .await;
    let mut per_file: Vec<(i64, Vec<vortex_index::bloom::FileBloom>)> = Vec::new();
    let mut not_applicable: Vec<i64> = Vec::new();
    let mut unbuildable: Vec<i64> = Vec::new();
    let mut fallback_streams = 0usize;
    let mut deferred = 0usize;
    for (id, key, result) in results {
        match result {
            Ok(LoadedBlooms::FromBlob(blooms)) if blooms.is_empty() => {
                not_applicable.push(id);
            }
            Ok(LoadedBlooms::FromBlob(blooms)) => per_file.push((id, blooms)),
            Ok(LoadedBlooms::FromDict(blooms)) => {
                fallback_streams += 1;
                if blooms.is_empty() {
                    not_applicable.push(id);
                } else {
                    per_file.push((id, blooms));
                }
            }
            Ok(LoadedBlooms::Deferred) => deferred += 1,
            // DETERMINISTIC (file-shaped) failure: the file's own bytes can
            // never build, so re-queuing it would spin forever and re-burn a
            // fallback-budget slot every pass. Stamp it out of the queue —
            // this log line therefore fires ONCE per file.
            Err(e) if vortex_index::bloom::is_unbuildable(&e) => {
                unbuildable.push(id);
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
    // stamp poison files BEFORE chunk building: a failure later in the pass
    // must not leave them spinning in the queue for another round
    if !unbuildable.is_empty() {
        infra::file_list::update_bloom_ver(&unbuildable, BLOOM_VER_UNBUILDABLE).await?;
    }

    // chunk by the per-field num_blocks signature (a field section of one
    // .bf must be block-uniform across its files)
    let mut chunks: HashMap<Vec<(String, u32)>, Vec<(i64, Vec<vortex_index::bloom::FileBloom>)>> =
        HashMap::new();
    for (id, blooms) in per_file {
        let mut sig: Vec<(String, u32)> = blooms
            .iter()
            .map(|b| (b.field.clone(), b.num_blocks))
            .collect();
        sig.sort();
        chunks.entry(sig).or_default().push((id, blooms));
    }

    let base_ver = now_micros();
    let mut chunk_idx: i64 = 0;
    let mut built = 0usize;
    for (_sig, mut chunk_files) in chunks {
        chunk_files.sort_by_key(|(id, _)| *id);
        for sub in chunk_files.chunks(MAX_FILES_PER_BF) {
            let bloom_ver = base_ver + chunk_idx;
            chunk_idx += 1;
            let mut field_blooms: Vec<FieldBloom> = Vec::new();
            for (id, blooms) in sub {
                for b in blooms {
                    field_blooms.push(FieldBloom {
                        field: b.field.clone(),
                        file_id: *id as u64,
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
            let ids: Vec<i64> = sub.iter().map(|(id, _)| *id).collect();
            infra::file_list::update_bloom_ver(&ids, bloom_ver).await?;
            built += ids.len();
        }
    }
    if !not_applicable.is_empty() {
        infra::file_list::update_bloom_ver(&not_applicable, BLOOM_VER_NOT_APPLICABLE).await?;
    }
    log::info!(
        "[COMPACTOR:BLOOM] {org_id}/{stream_type}/{stream_name}/{date}: built .bf for {built} \
         files ({chunk_idx} chunks, {fallback_streams} dictionary fallbacks, {} not applicable, \
         {} unbuildable) in {:?}",
        not_applicable.len(),
        unbuildable.len(),
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
) -> Result<LoadedBlooms, anyhow::Error> {
    let source: Arc<dyn vortex_index::VixRangeSource> = Arc::new(HealProbeRangeSource {
        account: file.account.clone(),
        location: object_store::path::Path::from(file.key.as_str()),
        // a .vix FileMeta's compressed_size is the exact object size
        size: file.meta.compressed_size as u64,
        handle: tokio::runtime::Handle::current(),
    });
    let bloom_fields = bloom_fields.to_vec();
    let fpp = get_config().common.vix_bloom_fpp;
    let file_key = file.key.clone();
    tokio::task::spawn_blocking(move || {
        let reader = VixReader::open_ranged(source)?;
        load_blooms_sync(&reader, &bloom_fields, fpp, &file_key, &FALLBACK_BUDGET)
    })
    .await?
}

/// The sync per-file load: blob transpose when the file carries a parseable
/// blob, otherwise a budget-scoped dictionary backfill. Split from
/// [`load_file_blooms`] so the budget/poison mechanics are unit-testable
/// without an object store.
fn load_blooms_sync(
    reader: &VixReader,
    bloom_fields: &[String],
    fpp: f64,
    file_key: &str,
    budget: &std::sync::atomic::AtomicI64,
) -> Result<LoadedBlooms, anyhow::Error> {
    match reader.file_blooms() {
        Ok(Some(blooms)) => {
            // keep only the configured fields (settings may have shrunk)
            let wanted: Vec<_> = blooms
                .into_iter()
                .filter(|b| bloom_fields.contains(&b.field))
                .collect();
            return Ok(LoadedBlooms::FromBlob(wanted));
        }
        Ok(None) => {}
        // a CORRUPT blob is file-shaped, but the dictionary may still be
        // walkable: log loudly and take the backfill path — the file is
        // poisoned only if that fails deterministically too
        Err(e) if vortex_index::bloom::is_unbuildable(&e) => {
            log::error!(
                "[COMPACTOR:BLOOM] {file_key}: corrupt per-file bloom blob, trying the \
                 dictionary backfill instead: {e:#}"
            );
        }
        // fetch-shaped (possibly transient): re-queue
        Err(e) => return Err(e),
    }
    // backfill: one full dictionary stream, hashing configured fields —
    // bounded per pass by the fallback budget
    budgeted_backfill(budget, || {
        blooms_from_dictionary(reader, bloom_fields, fpp, file_key)
    })
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
/// backfill for files written before the per-file `bloom` blob. Errors
/// PROPAGATE: the caller re-queues the file (`bloom_ver` stays 0) rather
/// than publishing a filter that cannot be trusted.
fn blooms_from_dictionary(
    reader: &VixReader,
    bloom_fields: &[String],
    fpp: f64,
    context: &str,
) -> Result<Vec<vortex_index::bloom::FileBloom>, anyhow::Error> {
    let pairs: Vec<(u16, String)> = bloom_fields
        .iter()
        .filter_map(|n| reader.term_field_id(n).map(|id| (id, n.clone())))
        .collect();
    if pairs.is_empty() {
        return Ok(Vec::new());
    }
    let wanted = pairs.len();
    let mut acc = vortex_index::bloom::BloomHashAcc::from_pairs(pairs);
    // `for_each_term` yields FIELD-MAJOR v2 keys (`{fid BE}{token}`) while
    // the bloom byte form is pinned to v1: `observe_dict_key` is the only
    // entry point that converts. A raw `observe` here records NOTHING, and
    // the empty filter that gets published rejects every value the file
    // holds.
    reader.for_each_term(&mut |key, _doc_count, _rgs| {
        acc.observe_dict_key(key);
        Ok(())
    })?;
    let blooms = finish_backfill_acc(acc, fpp, context)?;
    if blooms.len() < wanted {
        log::warn!(
            "[COMPACTOR:BLOOM] {context}: {} of {wanted} bloom fields carry no dictionary keys; \
             no filter published for them",
            wanted - blooms.len()
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

    /// A `.vix` with no `bloom` blob — the shape this path backfills.
    fn backfill_file(values: &[&str]) -> Vec<u8> {
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
        let mut writer =
            vortex_index::VixWriter::new(&schema, vortex_index::VixWriterOptions::default(), false);
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
        let reader = VixReader::open(bytes::Bytes::from(backfill_file(&values))).unwrap();
        assert!(!reader.has_file_blooms(), "the backfill shape has no blob");

        let blooms =
            blooms_from_dictionary(&reader, &["trace_id".to_string()], 0.001, "unit-test").unwrap();
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
            blooms_from_dictionary(&reader, &["span_id".to_string()], 0.001, "unit-test")
                .unwrap()
                .is_empty()
        );
    }

    /// A `.vix` WITH a per-file `bloom` blob (the blob-transpose shape).
    fn bloom_blob_file(values: &[&str]) -> Vec<u8> {
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
        let reader =
            VixReader::open(bytes::Bytes::from(backfill_file(&["trace-a"]))).unwrap();
        let loaded = budgeted_backfill(&budget, || {
            blooms_from_dictionary(&reader, &["trace_id".to_string()], 0.001, "unit-test")
        })
        .unwrap();
        assert!(matches!(&loaded, LoadedBlooms::FromDict(b) if b.len() == 1));
        assert_eq!(budget.load(Ordering::Relaxed), 0, "a real walk consumes");

        // transient failure: slot stays consumed
        let budget = AtomicI64::new(1);
        assert!(
            budgeted_backfill(&budget, || Err(anyhow::anyhow!("connection reset"))).is_err()
        );
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

        let mut data = bloom_blob_file(&["trace-a", "trace-b"]);
        let range = vortex_index::test_support::blob_byte_range(&data, "bloom").unwrap();
        for byte in &mut data[range] {
            *byte = 0xFF;
        }
        let reader = VixReader::open(bytes::Bytes::from(data)).unwrap();
        assert!(reader.file_blooms().is_err(), "the blob must be corrupt");

        let budget = std::sync::atomic::AtomicI64::new(1);
        let loaded = load_blooms_sync(
            &reader,
            &["trace_id".to_string()],
            0.001,
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
        let reader = VixReader::open(bytes::Bytes::from(bloom_blob_file(&["trace-a"]))).unwrap();
        let budget = std::sync::atomic::AtomicI64::new(0);
        let loaded = load_blooms_sync(
            &reader,
            &["trace_id".to_string()],
            0.001,
            "unit-test",
            &budget,
        )
        .unwrap();
        assert!(matches!(&loaded, LoadedBlooms::FromBlob(b) if b.len() == 1));
        assert_eq!(budget.load(std::sync::atomic::Ordering::Relaxed), 0);
    }
}
