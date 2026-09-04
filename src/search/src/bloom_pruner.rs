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

//! Search-side bloom prune layer — transposed (block-major) read.
//!
//! Given a candidate `Vec<FileKey>` and the query's `IndexCondition`,
//! this module pulls the bloom-decidable predicates out of it
//! ([`collect_decidable`]) and then:
//!
//! 1. Splits files into "has bloom" (`bloom_ver != 0`) and "no bloom" (`bloom_ver == 0`). The
//!    latter pass through untouched.
//! 2. Groups "has bloom" files by `(date, bloom_ver)` so all files sharing a `.bf` are tested with
//!    one footer fetch.
//! 3. For each group, fetches **one block row per `(predicate, value)`** — a single contiguous `M ×
//!    32`-byte range (M = files in the group) that holds every file's SBBF block for that value
//!    (see the transposed layout in `infra::bloom`). So the per-group remote cost is
//!    O(predicate×value) reads, not O(files): a single trace_id lookup is **one read per group**.
//! 4. For each file, slices its column out of the fetched row and runs the SBBF point check. A file
//!    is kept iff every predicate's bloom returns *maybe* for at least one of its values (OR within
//!    a predicate, AND across predicates).
//!
//! Any failure (fetch, parse, schema mismatch) **falls back to "keep
//! all"** for the affected group — bloom is performance, not correctness.

use std::collections::{HashMap, HashSet};

use config::{
    VixBloomCompositeScope,
    meta::stream::{FileKey, StreamType},
};
use futures::stream::{self, StreamExt};
use infra::bloom::{
    BLOOM_FOOTER_CACHE, BloomReader,
    path::bloom_path,
    sbbf::{BLOCK_BYTES, check_block_with_mask, mask_from_hash},
};
use object_store::{GetOptions, GetRange};

use super::index::{Condition, IndexCondition};

/// One bloom-decidable predicate: a field plus the candidate values to test.
/// Passes if **any** candidate's bloom check returns "maybe" (OR within a
/// predicate; predicates are AND'd across).
struct Predicate {
    field: String,
    values: Vec<String>,
    /// Whether policy permits this field to use the composite section when
    /// a file has no per-field section. Additive fields still prefer their
    /// per-field bloom; fields outside the additive set are admitted only
    /// when this fallback is enabled. Composite guards keep missing or
    /// mixed-era coverage fail-open.
    composite_fallback: bool,
}

/// Bytes pulled from the tail of a `.bf` on footer-cache miss. Big
/// enough to cover the footer payload for hour buckets up to ~150 indexed
/// (file, field) pairs (footer ≈ 24 B per file × few fields + per-field
/// header ≈ 7.5 KB at the high end). When the actual footer overflows
/// this probe, the group falls back to "keep all".
const BLOOM_SUFFIX_PROBE_BYTES: u64 = 16 * 1024;

/// Outcome of running one (date, bloom_ver) bucket. The `Ok` variant carries
/// one tuple per resolvable `(file_idx, pred_idx, value_idx)`; missing
/// entries imply "no info" for that target and are handled by the caller.
enum GroupResult {
    Ok(Vec<(usize, usize, usize, bool)>),
    Err(String, FetchError),
}

/// Errors observable by the per-group fetch helper. Folded into a `log::warn`
/// + "keep all" in the prune loop — never surfaced to the query caller.
#[derive(Debug, thiserror::Error)]
enum FetchError {
    #[error("object store: {0}")]
    Store(#[from] object_store::Error),
    #[error("bloom parse: {0:?}")]
    Parse(infra::bloom::ReadError),
    #[error("row count mismatch: expected {expected} got {got}")]
    RowMismatch { expected: usize, got: usize },
}

/// Outer concurrency cap on `.bf` buckets processed in parallel.
///
/// Mirrors the index search path (`storage.rs`): use the same
/// `query_thread_num` knob (defaults to `cpu_num * 4` in cluster mode,
/// `cpu_num` in local). No hardcoded constant — large multi-day
/// trace_id lookups need to fan out far beyond 32 buckets.
fn bloom_prefetch_concurrency() -> usize {
    config::get_config().limit.query_thread_num.max(1)
}

/// Prune `files` against the bloom-decidable parts of `index_condition`,
/// returning the surviving subset. Files with `bloom_ver == 0` are always
/// kept (no bloom info). If no condition is bloom-decidable, `files` is
/// returned unchanged without touching any `.bf`.
///
/// Additive stream bloom fields always probe their per-field sections and
/// may fall back to the composite when `composite_scope` allows them. In a
/// selective scope, `auto_id_scope` additionally admits semantic ID fields
/// not present in `auto_id_never`. Broad [`VixBloomCompositeScope::All`]
/// remains unrestricted.
///
/// `trace_id` is threaded through purely for logging.
#[allow(clippy::too_many_arguments)]
pub async fn prune(
    trace_id: &str,
    org_id: &str,
    stream_type: StreamType,
    stream_name: &str,
    files: Vec<FileKey>,
    index_condition: &IndexCondition,
    bloom_indexed_fields: Vec<String>,
    composite_scope: &VixBloomCompositeScope,
    auto_id_scope: bool,
    auto_id_never: &HashSet<String>,
) -> Vec<FileKey> {
    let bloom_indexed_fields = bloom_indexed_fields.into_iter().collect::<HashSet<_>>();
    let predicates = collect_decidable(
        index_condition,
        &bloom_indexed_fields,
        composite_scope,
        auto_id_scope,
        auto_id_never,
    );
    if predicates.is_empty() {
        return files;
    }
    let predicates = predicates.as_slice();

    // 1. Split files by whether they have a bloom_ver assigned.
    let total_input = files.len();
    let mut without_bloom: Vec<FileKey> = Vec::new();
    let mut with_bloom: Vec<FileKey> = Vec::with_capacity(files.len());
    for f in files {
        if f.meta.bloom_ver <= 0 {
            without_bloom.push(f);
        } else {
            with_bloom.push(f);
        }
    }
    if with_bloom.is_empty() {
        log::warn!(
            "[trace_id {trace_id}] search->bloom: stream {org_id}/{stream_type}/{stream_name}, \
             all {total_input} files have bloom_ver=0 — no `.bf` covers any of them. \
             Likely causes: compactor hasn't built `.bf` for this hour yet, or these files \
             produced no blooms (target field not indexed at build time, or \
             index_size=0). Falling through, no pruning applied."
        );
        return without_bloom;
    }
    log::info!(
        "[trace_id {trace_id}] search->bloom: stream {org_id}/{stream_type}/{stream_name}, \
         input={total_input} (with_bloom={}, without_bloom={}), \
         predicates=[{}]",
        with_bloom.len(),
        without_bloom.len(),
        predicates
            .iter()
            .map(|p| format!("{}({})", p.field, p.values.len()))
            .collect::<Vec<_>>()
            .join(", ")
    );

    // 2. Group by (date, bloom_ver) — one .bf per group.
    type Group = (String, i64);
    let mut groups: HashMap<Group, Vec<usize>> = HashMap::new();
    let mut date_for: Vec<Option<String>> = vec![None; with_bloom.len()];
    for (i, f) in with_bloom.iter().enumerate() {
        let date = match config::utils::parquet::parse_file_key_columns(&f.key) {
            Ok((_, d, _)) => d,
            Err(_) => continue,
        };
        date_for[i] = Some(date.clone());
        groups.entry((date, f.meta.bloom_ver)).or_default().push(i);
    }

    // 3. Plan per-group work. With the transposed layout the fetch unit is one block-row per
    //    (predicate, value) — shared by every file in the group — so we just carry the group's file
    //    indices.
    struct GroupSpec {
        group: Group,
        account: String,
        path: String,
        file_idxs: Vec<usize>,
    }
    let specs: Vec<GroupSpec> = groups
        .into_iter()
        .map(|((date, ver), idxs)| {
            let path = bloom_path(org_id, stream_type, stream_name, &date, ver);
            let account = infra::storage::get_account(org_id, &path).unwrap_or_default();
            GroupSpec {
                group: (date, ver),
                account,
                path,
                file_idxs: idxs,
            }
        })
        .collect();

    // 4. Run each group: fetch footer → compute 32-byte ranges → batched range GETs → run
    //    check_block per target. Outer concurrency is config-driven.
    let total_groups = specs.len();
    let with_bloom_ref = &with_bloom;
    let concurrency = bloom_prefetch_concurrency();
    let results: Vec<(Group, GroupResult)> = stream::iter(specs)
        .map(|spec| async move {
            let group = spec.group.clone();
            let res = run_group(
                trace_id,
                &spec.account,
                &spec.path,
                &spec.file_idxs,
                predicates,
                with_bloom_ref,
            )
            .await;
            (group, res)
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;

    // 5. Fold per-group outcomes into a per-file "did every predicate match?" table. Default = keep
    //    (no info = conservative).
    let mut per_file_match: HashMap<usize, Vec<Vec<bool>>> = HashMap::new();
    for (_, result) in &results {
        match result {
            GroupResult::Ok(outcomes) => {
                for (file_idx, pred_idx, value_idx, hit) in outcomes {
                    let entry = per_file_match.entry(*file_idx).or_insert_with(|| {
                        predicates
                            .iter()
                            .map(|p| vec![false; p.values.len()])
                            .collect()
                    });
                    entry[*pred_idx][*value_idx] = *hit;
                }
            }
            GroupResult::Err(path, e) => {
                log::warn!(
                    "[trace_id {trace_id}] search->bloom: group `{path}` failed: {e}; keeping its files"
                );
            }
        }
    }

    // Also fold "checked successfully but some predicate had no info" — those
    // files default to keep. We track this by recording which (file_idx, pred_idx)
    // pairs produced at least one checked block (i.e. `row_range` resolved a row
    // and the file had a column in it). If a pair has no checked block at all for
    // any of its values, we treat that predicate as "unknown" → keep file.
    let mut has_info: HashMap<usize, Vec<bool>> = HashMap::new();
    for (_, result) in &results {
        if let GroupResult::Ok(outcomes) = result {
            for (file_idx, pred_idx, ..) in outcomes {
                let entry = has_info
                    .entry(*file_idx)
                    .or_insert_with(|| vec![false; predicates.len()]);
                entry[*pred_idx] = true;
            }
        }
    }

    let mut kept = without_bloom;
    let mut kept_no_info = 0usize;
    let mut kept_predicate_hit = 0usize;
    let mut dropped = 0usize;
    let mut kept_unparseable_key = 0usize;
    let mut kept_bucket_failed = 0usize;
    for (idx, f) in with_bloom.into_iter().enumerate() {
        if date_for[idx].is_none() {
            kept_unparseable_key += 1;
            kept.push(f);
            continue;
        }
        let matches = match per_file_match.get(&idx) {
            Some(m) => m,
            None => {
                // Bucket failed entirely → keep.
                kept_bucket_failed += 1;
                kept.push(f);
                continue;
            }
        };
        let info = has_info
            .get(&idx)
            .cloned()
            .unwrap_or_else(|| vec![false; predicates.len()]);

        // AND across predicates, OR within each predicate's values. A predicate
        // with no info (unknown field / unknown file_id) counts as "match".
        let any_pred_lacks_info = info.iter().any(|i| !*i);
        let all_match = predicates.iter().enumerate().all(|(pi, _)| {
            if !info[pi] {
                return true; // unknown → conservatively true
            }
            matches[pi].iter().any(|hit| *hit)
        });
        if all_match {
            if any_pred_lacks_info {
                kept_no_info += 1;
            } else {
                kept_predicate_hit += 1;
            }
            kept.push(f);
        } else {
            dropped += 1;
        }
    }
    log::info!(
        "[trace_id {trace_id}] search->bloom: stream {org_id}/{stream_type}/{stream_name}, \
         groups={total_groups}, input={total_input}, kept={} \
         (predicate_hit={kept_predicate_hit}, no_info={kept_no_info}, \
         bucket_failed={kept_bucket_failed}, unparseable_key={kept_unparseable_key}, \
         no_bloom={}), dropped={dropped}",
        kept.len(),
        kept.len() - kept_predicate_hit - kept_no_info - kept_bucket_failed - kept_unparseable_key
    );
    kept
}

async fn run_group(
    trace_id: &str,
    account: &str,
    path: &str,
    file_idxs: &[usize],
    predicates: &[Predicate],
    with_bloom: &[FileKey],
) -> GroupResult {
    // Step 1: footer (cache → suffix GET on miss). `.bf` is immutable so the
    // footer cache entry is valid for the file's lifetime.
    let (total_size, suffix) = match get_footer_suffix(account, path).await {
        Ok(x) => x,
        Err(e) => return GroupResult::Err(path.to_string(), e),
    };
    let reader = match BloomReader::parse_suffix(&suffix, total_size) {
        Ok(r) => r,
        Err(e) => return GroupResult::Err(path.to_string(), FetchError::Parse(e)),
    };

    // Step 2: one block-ROW per (predicate, value) — shared across all files
    // in this group. This is the transposed-layout win: O(predicate×value)
    // reads per group instead of O(files).
    struct RowPlan {
        pred_idx: usize,
        /// `Some(vi)` = value probe; `None` = a #48 guard probe (does this
        /// file's composite cover `pred_idx`'s field at all?).
        value_idx: Option<usize>,
        range: std::ops::Range<u64>,
        /// SBBF bit-mask for this value, computed once and reused for every
        /// file's block in the row (the mask depends only on the hash, which
        /// is identical across all files in the group).
        mask: [u32; 8],
        /// Whether this fallback row probes the COMPOSITE section.
        composite_row: bool,
    }
    let composite_section = vortex_index::bloom::COMPOSITE_BLOOM_FIELD;
    let mut rows: Vec<RowPlan> = Vec::new();
    let mut guard_buf = Vec::new();
    // One guard-row set per predicate that needs COMPOSITE fallback.
    let mut push_guard_rows = |pi: usize, field: &str, rows: &mut Vec<RowPlan>| {
        for probe in 0..vortex_index::bloom::COMPOSITE_GUARD_PROBES {
            let Some(key) = vortex_index::bloom::composite_guard_key(field, probe, &mut guard_buf)
            else {
                break; // unreachable post collect_decidable filter
            };
            if let Some((range, hash)) = reader.row_range(composite_section, key) {
                rows.push(RowPlan {
                    pred_idx: pi,
                    value_idx: None,
                    range,
                    mask: mask_from_hash(hash),
                    composite_row: true,
                });
            }
        }
    };
    for (pi, pred) in predicates.iter().enumerate() {
        for (vi, value) in pred.values.iter().enumerate() {
            if let Some((range, hash)) = reader.row_range(&pred.field, value.as_bytes()) {
                rows.push(RowPlan {
                    pred_idx: pi,
                    value_idx: Some(vi),
                    range,
                    mask: mask_from_hash(hash),
                    composite_row: false,
                });
            }
            // section absent in this .bf → no row → those (file, pred)
            // pairs stay "no info" and are conservatively kept.
        }
        if pred.composite_fallback
            && !file_idxs.iter().all(|&fi| {
                reader
                    .column_index(&pred.field, with_bloom[fi].id as u64)
                    .is_some()
            })
        {
            // The per-field section does NOT cover every group file. Probe
            // the composite as the fallback for exactly those files: tagged
            // value keys plus guard rows keep missing field coverage
            // fail-open. Files covered by the per-field section keep that
            // verdict (see the outcome pass). This footer-local decision
            // adds no IO when the per-field section covers the whole group.
            for (vi, value) in pred.values.iter().enumerate() {
                let mut buf = Vec::new();
                if let Some(key) = vortex_index::bloom::composite_value_key(
                    &pred.field,
                    value.as_bytes(),
                    &mut buf,
                ) && let Some((range, hash)) = reader.row_range(composite_section, key)
                {
                    rows.push(RowPlan {
                        pred_idx: pi,
                        value_idx: Some(vi),
                        range,
                        mask: mask_from_hash(hash),
                        composite_row: true,
                    });
                }
            }
            push_guard_rows(pi, &pred.field, &mut rows);
        }
    }
    if rows.is_empty() {
        let footer_fields: Vec<&str> = reader.fields().collect();
        let requested_fields: Vec<&str> = predicates.iter().map(|p| p.field.as_str()).collect();
        log::warn!(
            "[trace_id {trace_id}] search->bloom: group `{path}`: no rows resolved — \
             footer fields=[{}], requested=[{}]. Likely cause: stream \
             `bloom_filter_fields` at build time differed from the queried \
             field. Keeping all files in this group.",
            footer_fields.join(", "),
            requested_fields.join(", "),
        );
        return GroupResult::Ok(Vec::new());
    }

    // Step 3: fetch the rows. One contiguous range each (M×32 bytes), batched
    // into a single `get_ranges` call. For the canonical single-value query
    // this is exactly ONE remote read for the whole group.
    let ranges: Vec<std::ops::Range<u64>> = rows.iter().map(|r| r.range.clone()).collect();
    let fetched = match infra::cache::storage::get_ranges(account, &path.into(), &ranges).await {
        Ok(v) => v,
        Err(e) => return GroupResult::Err(path.to_string(), FetchError::Store(e)),
    };
    if fetched.len() != rows.len() {
        return GroupResult::Err(
            path.to_string(),
            FetchError::RowMismatch {
                expected: rows.len(),
                got: fetched.len(),
            },
        );
    }

    // Step 4: for each file, slice its column out of each fetched row and run
    // the 8-bit check. A file whose column is absent from a row's section
    // stays "no info" for that predicate.
    //
    // Guard pass first: a (file, predicate) pair whose guard probes don't ALL
    // hit is NOT covered — its composite value outcomes are suppressed
    // entirely, so the caller's fold sees "no info" (keep) instead of a false
    // "definitely not". Guards gate COMPOSITE rows only: a per-field verdict
    // never depends on composite coverage (M15 — a legacy file with a
    // per-field section but no composite must keep pruning per-field).
    let check = |r: &RowPlan, row_bytes: &bytes::Bytes, file_idx: usize| -> Option<bool> {
        let file_id = with_bloom[file_idx].id as u64;
        let section = if r.composite_row {
            composite_section
        } else {
            &predicates[r.pred_idx].field
        };
        // file not a column for this section → no info
        let col = reader.column_index(section, file_id)?;
        let off = col * BLOCK_BYTES;
        // footer/body disagreement shouldn't happen; treat as no info
        if off + BLOCK_BYTES > row_bytes.len() {
            return None;
        }
        let block: &[u8; BLOCK_BYTES] = row_bytes[off..off + BLOCK_BYTES].try_into().unwrap();
        Some(check_block_with_mask(block, &r.mask))
    };
    let mut guard_miss: HashSet<(usize, usize)> = HashSet::new();
    for (r, row_bytes) in rows.iter().zip(fetched.iter()) {
        if r.value_idx.is_some() {
            continue;
        }
        for &file_idx in file_idxs {
            if check(r, row_bytes, file_idx) == Some(false) {
                guard_miss.insert((file_idx, r.pred_idx));
            }
        }
    }
    let mut outcomes: Vec<(usize, usize, usize, bool)> = Vec::new();
    for (r, row_bytes) in rows.iter().zip(fetched.iter()) {
        let Some(value_idx) = r.value_idx else {
            continue;
        };
        for &file_idx in file_idxs {
            if r.composite_row {
                // composite verdicts require proven coverage (all guards hit)
                if guard_miss.contains(&(file_idx, r.pred_idx)) {
                    continue;
                }
                // Fallback rows govern ONLY files the predicate's own
                // per-field section does not cover; per-field-covered files
                // keep their exact per-field verdict.
                if reader
                    .column_index(
                        &predicates[r.pred_idx].field,
                        with_bloom[file_idx].id as u64,
                    )
                    .is_some()
                {
                    continue;
                }
            }
            if let Some(hit) = check(r, row_bytes, file_idx) {
                outcomes.push((file_idx, r.pred_idx, value_idx, hit));
            }
        }
    }

    GroupResult::Ok(outcomes)
}

/// Footer suffix retrieval: footer cache → suffix-range GET on miss.
/// Returns (total_size, suffix_bytes) in both branches.
///
/// Two-step read: the first probe pulls a fixed tail
/// (`BLOOM_SUFFIX_PROBE_BYTES`), which covers the whole footer for the common
/// case (few fields / a few hundred files). When the footer is bigger than
/// the probe — many bloom fields, or a large `max_files_per_bf` — we re-read
/// exactly `footer_len + 8` from the tail so a big `.bf` still prunes instead
/// of silently falling back to keep-all.
async fn get_footer_suffix(account: &str, path: &str) -> Result<(u64, bytes::Bytes), FetchError> {
    if let Some((total, suffix)) = BLOOM_FOOTER_CACHE.get(path) {
        return Ok((total, suffix));
    }

    let (total, suffix) = fetch_suffix(account, path, BLOOM_SUFFIX_PROBE_BYTES).await?;
    let suffix = match footer_shortfall(&suffix, total) {
        Some(needed) => fetch_suffix(account, path, needed).await?.1,
        None => suffix,
    };

    BLOOM_FOOTER_CACHE.put(path.to_string(), total, suffix.clone());
    Ok((total, suffix))
}

/// One `Suffix(n)` GET. Returns (total_file_size, suffix_bytes).
async fn fetch_suffix(
    account: &str,
    path: &str,
    n: u64,
) -> Result<(u64, bytes::Bytes), FetchError> {
    let opts = GetOptions {
        range: Some(GetRange::Suffix(n)),
        ..Default::default()
    };
    let res = infra::cache::storage::get_opts(account, &path.into(), opts).await?;
    let total = res.meta.size;
    let suffix = res.bytes().await?;
    Ok((total, suffix))
}

/// If the footer doesn't fully fit in `suffix`, return how many trailing
/// bytes to re-read (`footer_len + footer_len_field + magic`). The last 8
/// bytes of any `.bf` are `footer_len(4) + MAGIC(4)`, and the probe always
/// includes them, so we can read `footer_len` here. Returns `None` when the
/// footer already fits, the suffix is too short to hold the trailer, or
/// `footer_len` is implausible (corrupt) — let `parse_suffix` reject those.
fn footer_shortfall(suffix: &[u8], total: u64) -> Option<u64> {
    let n = suffix.len();
    if n < 8 {
        return None;
    }
    let footer_len = u32::from_le_bytes(suffix[n - 8..n - 4].try_into().ok()?) as u64;
    let needed = footer_len + 8;
    if needed <= n as u64 || needed > total {
        return None;
    }
    Some(needed)
}

/// Returns the borrowed field when `condition` has exactly a Bloom-supported
/// shape. Recursive ORs are supported only when every leaf is a positive
/// equality/non-empty IN on the same field.
fn decidable_field(condition: &Condition) -> Option<&str> {
    match condition {
        Condition::Equal(field, _) => Some(field),
        Condition::In(field, values, false) if !values.is_empty() => Some(field),
        Condition::Or(left, right) => {
            let left = decidable_field(left)?;
            let right = decidable_field(right)?;
            (left == right).then_some(left)
        }
        _ => None,
    }
}

/// Returns whether this condition contains any predicate that the Bloom
/// pruner can actually evaluate under the stream's additive fields and the
/// composite policy. This preflight borrows the condition throughout: large
/// IN/OR payloads are neither cloned nor sorted before the caller transfers
/// ownership of its file list.
pub fn is_applicable(
    cond: &IndexCondition,
    bloom_indexed_fields: &[String],
    composite_scope: &VixBloomCompositeScope,
    auto_id_scope: bool,
    auto_id_never: &HashSet<String>,
) -> bool {
    cond.conditions.iter().any(|condition| {
        let Some(field) = decidable_field(condition) else {
            return false;
        };
        let additive = bloom_indexed_fields
            .iter()
            .any(|indexed| indexed.as_str() == field);
        additive || composite_fallback_allowed(field, composite_scope, auto_id_scope, auto_id_never)
    })
}

/// Pull the bloom-decidable predicates out of a top-level `IndexCondition`.
///
/// Each top-level (AND'd) condition is run through [`try_predicate`]; those
/// that fold cleanly become bloom checks, the rest are silently skipped (the
/// affected files pass the bloom step untouched).
///
/// `bloom_indexed_fields` is the stream's additive `bloom_filter_fields`
/// (restricted to fields present in the schema). Those fields always use
/// their per-field bloom and may use composite fallback only when
/// `composite_scope` allows them. A field outside the additive set is admitted
/// only when the broad, explicit, or AUTO-ID composite policy allows its
/// fallback.
fn collect_decidable(
    cond: &IndexCondition,
    bloom_indexed_fields: &HashSet<String>,
    composite_scope: &VixBloomCompositeScope,
    auto_id_scope: bool,
    auto_id_never: &HashSet<String>,
) -> Vec<Predicate> {
    cond.conditions
        .iter()
        .filter_map(|condition| {
            let field = decidable_field(condition)?;
            let additive = bloom_indexed_fields.contains(field);
            if !additive
                && !composite_fallback_allowed(field, composite_scope, auto_id_scope, auto_id_never)
            {
                return None;
            }
            admit_predicate(
                try_predicate(condition)?,
                additive,
                composite_scope,
                auto_id_scope,
                auto_id_never,
            )
        })
        .collect()
}

/// Whether policy and key encoding permit this field to use the composite
/// section. Broad scope intentionally remains any-field. Selective scope
/// admits explicit names and, only when enabled by the query snapshot, ID-like
/// names not blocked by the AUTO NEVER list.
fn composite_fallback_allowed(
    field: &str,
    composite_scope: &VixBloomCompositeScope,
    auto_id_scope: bool,
    auto_id_never: &HashSet<String>,
) -> bool {
    field.len() <= u16::MAX as usize
        && match composite_scope {
            VixBloomCompositeScope::All => true,
            VixBloomCompositeScope::Only(fields) => {
                fields.contains(field)
                    || (auto_id_scope
                        && !auto_id_never.contains(field)
                        && vortex_index::is_id_like_field_name(field))
            }
        }
}

/// Applies the shared additive/composite admission rule and records whether
/// the admitted predicate may fall back to the composite section.
fn admit_predicate(
    mut predicate: Predicate,
    additive: bool,
    composite_scope: &VixBloomCompositeScope,
    auto_id_scope: bool,
    auto_id_never: &HashSet<String>,
) -> Option<Predicate> {
    let composite_fallback = composite_fallback_allowed(
        &predicate.field,
        composite_scope,
        auto_id_scope,
        auto_id_never,
    );
    if !additive && !composite_fallback {
        return None;
    }
    predicate.composite_fallback = composite_fallback;
    Some(predicate)
}

/// Try to convert a single `Condition` into one bloom-decidable `Predicate`.
///
/// A bloom can prove a value is definitely *absent* from a file, never that
/// it's present — so we only fold **positive equality / IN** shapes:
///
/// - `Equal(f, v)` → `Predicate { f, [v] }`.
/// - `In(f, vs, false)` with non-empty `vs` → `Predicate { f, vs }`.
/// - `Or(l, r)` whose every leaf folds to the **same** field → one `Predicate` whose `values` is
///   the deduped union of leaves' values (semantically `f IN (...)`). Handles arbitrarily nested
///   `Or`s by recursion.
///
/// Returns `None` on anything else — `NotEqual` / `Not` / `Regex` / `StrMatch`
/// / `MatchAll` / `FuzzyMatchAll`, nested `And`, negated or empty `In`, or an
/// `Or` whose leaves don't all fold or don't share a field.
fn try_predicate(cond: &Condition) -> Option<Predicate> {
    match cond {
        Condition::Equal(field, value) => Some(Predicate {
            field: field.clone(),
            values: vec![value.clone()],
            composite_fallback: false,
        }),
        Condition::In(field, values, false) if !values.is_empty() => Some(Predicate {
            field: field.clone(),
            values: values.clone(),
            composite_fallback: false,
        }),
        Condition::Or(left, right) => {
            let lp = try_predicate(left)?;
            let rp = try_predicate(right)?;
            if lp.field != rp.field {
                return None;
            }
            let mut values = lp.values;
            values.extend(rp.values);
            // Dedup so e.g. `f = a OR f IN (a, b)` doesn't fetch the same row twice.
            values.sort();
            values.dedup();
            Some(Predicate {
                field: lp.field,
                values,
                composite_fallback: false,
            })
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use config::meta::stream::{FileKey, FileMeta};
    use infra::bloom::{BloomBuilder, BloomReader, BloomWriter};

    use super::*;

    fn fk(key: &str, bloom_ver: i64) -> FileKey {
        let mut k = FileKey::new(0, "default".into(), key.into(), FileMeta::default(), false);
        k.meta.bloom_ver = bloom_ver;
        k
    }

    fn fields(names: &[&str]) -> HashSet<String> {
        names.iter().map(|s| s.to_string()).collect()
    }
    fn cond(conditions: Vec<Condition>) -> IndexCondition {
        let mut condition = IndexCondition::new();
        for item in conditions {
            condition.add_condition(item);
        }
        condition
    }

    fn only_scope(names: &[&str]) -> VixBloomCompositeScope {
        VixBloomCompositeScope::Only(names.iter().map(|s| s.to_string()).collect())
    }

    fn assert_applicability_parity(
        condition: &IndexCondition,
        additive_fields: &[&str],
        composite_scope: &VixBloomCompositeScope,
        auto_id_scope: bool,
        auto_id_never: &[&str],
        expected: bool,
    ) {
        let additive_vec = additive_fields
            .iter()
            .map(|field| field.to_string())
            .collect::<Vec<_>>();
        let auto_id_never = fields(auto_id_never);
        assert_eq!(
            is_applicable(
                condition,
                &additive_vec,
                composite_scope,
                auto_id_scope,
                &auto_id_never,
            ),
            expected
        );
        assert_eq!(
            !collect_decidable(
                condition,
                &fields(additive_fields),
                composite_scope,
                auto_id_scope,
                &auto_id_never,
            )
            .is_empty(),
            expected,
            "borrowed preflight and owned prune planning must agree"
        );
    }

    // ---- collect_decidable dispatch ----

    #[test]
    fn test_collect_equal_on_indexed_field() {
        let c = cond(vec![Condition::Equal("trace_id".into(), "abc".into())]);
        let p = collect_decidable(
            &c,
            &fields(&["trace_id"]),
            &only_scope(&[]),
            false,
            &HashSet::new(),
        );
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].field, "trace_id");
        assert_eq!(p[0].values, vec!["abc".to_string()]);
    }

    #[test]
    fn test_collect_skips_non_indexed_field() {
        let c = cond(vec![Condition::Equal("body".into(), "abc".into())]);
        assert!(
            collect_decidable(
                &c,
                &fields(&["trace_id"]),
                &only_scope(&[]),
                false,
                &HashSet::new(),
            )
            .is_empty()
        );
    }

    #[test]
    fn test_collect_positive_in() {
        let c = cond(vec![Condition::In(
            "trace_id".into(),
            vec!["a".into(), "b".into()],
            false,
        )]);
        let p = collect_decidable(
            &c,
            &fields(&["trace_id"]),
            &only_scope(&[]),
            false,
            &HashSet::new(),
        );
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].values, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn test_collect_skips_empty_and_negated_in() {
        let empty = cond(vec![Condition::In("trace_id".into(), vec![], false)]);
        assert!(
            collect_decidable(
                &empty,
                &fields(&["trace_id"]),
                &only_scope(&[]),
                false,
                &HashSet::new(),
            )
            .is_empty()
        );
        let negated = cond(vec![Condition::In(
            "trace_id".into(),
            vec!["a".into()],
            true,
        )]);
        assert!(
            collect_decidable(
                &negated,
                &fields(&["trace_id"]),
                &only_scope(&[]),
                false,
                &HashSet::new(),
            )
            .is_empty()
        );
    }

    #[test]
    fn test_collect_skips_negation_regex_strmatch() {
        let c = cond(vec![
            Condition::NotEqual("trace_id".into(), "abc".into()),
            Condition::Regex("trace_id".into(), "^abc.*".into()),
            Condition::StrMatch("trace_id".into(), "abc".into(), true),
        ]);
        assert!(
            collect_decidable(
                &c,
                &fields(&["trace_id"]),
                &only_scope(&[]),
                false,
                &HashSet::new(),
            )
            .is_empty()
        );
    }

    // ---- same-field Or folding ----

    #[test]
    fn test_collect_same_field_or_of_equals_flattens() {
        // `trace_id = b OR trace_id = a` → one Predicate, values sorted+deduped
        // (semantically `trace_id IN (a, b)`).
        let c = cond(vec![Condition::Or(
            Box::new(Condition::Equal("trace_id".into(), "b".into())),
            Box::new(Condition::Equal("trace_id".into(), "a".into())),
        )]);
        let p = collect_decidable(
            &c,
            &fields(&["trace_id"]),
            &only_scope(&[]),
            false,
            &HashSet::new(),
        );
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].field, "trace_id");
        assert_eq!(p[0].values, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn test_collect_same_field_or_mixed_eq_and_in_flattens() {
        // `trace_id = a OR trace_id IN (b, c)` → one Predicate { values: [a, b, c] }
        let c = cond(vec![Condition::Or(
            Box::new(Condition::Equal("trace_id".into(), "a".into())),
            Box::new(Condition::In(
                "trace_id".into(),
                vec!["b".into(), "c".into()],
                false,
            )),
        )]);
        let p = collect_decidable(
            &c,
            &fields(&["trace_id"]),
            &only_scope(&[]),
            false,
            &HashSet::new(),
        );
        assert_eq!(p.len(), 1);
        assert_eq!(
            p[0].values,
            vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );
    }

    #[test]
    fn test_collect_nested_or_chain_flattens() {
        // (trace_id = 1 OR trace_id = 2) OR trace_id = 3 → one Predicate.
        let inner = Condition::Or(
            Box::new(Condition::Equal("trace_id".into(), "1".into())),
            Box::new(Condition::Equal("trace_id".into(), "2".into())),
        );
        let outer = Condition::Or(
            Box::new(inner),
            Box::new(Condition::Equal("trace_id".into(), "3".into())),
        );
        let p = collect_decidable(
            &cond(vec![outer]),
            &fields(&["trace_id"]),
            &only_scope(&[]),
            false,
            &HashSet::new(),
        );
        assert_eq!(p.len(), 1);
        assert_eq!(
            p[0].values,
            vec!["1".to_string(), "2".to_string(), "3".to_string()]
        );
    }

    #[test]
    fn test_collect_or_dedups_values() {
        // `trace_id = a OR trace_id IN (a, b)` → values = [a, b], one row each.
        let c = cond(vec![Condition::Or(
            Box::new(Condition::Equal("trace_id".into(), "a".into())),
            Box::new(Condition::In(
                "trace_id".into(),
                vec!["a".into(), "b".into()],
                false,
            )),
        )]);
        let p = collect_decidable(
            &c,
            &fields(&["trace_id"]),
            &only_scope(&[]),
            false,
            &HashSet::new(),
        );
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].values, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn test_collect_cross_field_or_skipped() {
        // `trace_id = a OR service = x` — joining across fields would weaken
        // the filter, so the whole Or is dropped.
        let c = cond(vec![Condition::Or(
            Box::new(Condition::Equal("trace_id".into(), "a".into())),
            Box::new(Condition::Equal("service".into(), "x".into())),
        )]);
        assert!(
            collect_decidable(
                &c,
                &fields(&["trace_id", "service"]),
                &only_scope(&[]),
                false,
                &HashSet::new(),
            )
            .is_empty()
        );
    }

    #[test]
    fn test_collect_or_with_negated_in_skipped() {
        // Any leaf we can't fold (here a negated In) collapses the whole Or.
        let c = cond(vec![Condition::Or(
            Box::new(Condition::Equal("trace_id".into(), "a".into())),
            Box::new(Condition::In("trace_id".into(), vec!["b".into()], true)),
        )]);
        assert!(
            collect_decidable(
                &c,
                &fields(&["trace_id"]),
                &only_scope(&[]),
                false,
                &HashSet::new(),
            )
            .is_empty()
        );
    }

    #[test]
    fn test_collect_or_on_non_indexed_field_skipped() {
        // Both branches positive Eq on the same name, but the field is not in
        // the bloom-indexed set — leaves fold to None, whole Or is dropped.
        let c = cond(vec![Condition::Or(
            Box::new(Condition::Equal("body".into(), "a".into())),
            Box::new(Condition::Equal("body".into(), "b".into())),
        )]);
        assert!(
            collect_decidable(
                &c,
                &fields(&["trace_id"]),
                &only_scope(&[]),
                false,
                &HashSet::new(),
            )
            .is_empty()
        );
    }

    #[test]
    fn test_collect_or_alongside_and_top_level() {
        // Top-level AND of (same-field Or) + (Equal on a different bloom field):
        // both produce a Predicate; pruner ANDs them.
        let c = cond(vec![
            Condition::Or(
                Box::new(Condition::Equal("trace_id".into(), "a".into())),
                Box::new(Condition::Equal("trace_id".into(), "b".into())),
            ),
            Condition::Equal("user_id".into(), "u-1".into()),
        ]);
        let p = collect_decidable(
            &c,
            &fields(&["trace_id", "user_id"]),
            &only_scope(&[]),
            false,
            &HashSet::new(),
        );
        assert_eq!(p.len(), 2);
        let trace = p.iter().find(|p| p.field == "trace_id").unwrap();
        assert_eq!(trace.values, vec!["a".to_string(), "b".to_string()]);
        let user = p.iter().find(|p| p.field == "user_id").unwrap();
        assert_eq!(user.values, vec!["u-1".to_string()]);
    }

    #[test]
    fn test_collect_multiple_top_level_ands() {
        // Top-level AND: each prunable predicate is picked; non-prunable ones
        // (here the NotEqual) are ignored.
        let c = cond(vec![
            Condition::Equal("trace_id".into(), "x".into()),
            Condition::NotEqual("body".into(), "noise".into()),
            Condition::Equal("user_id".into(), "u-1".into()),
        ]);
        let p = collect_decidable(
            &c,
            &fields(&["trace_id", "user_id"]),
            &only_scope(&[]),
            false,
            &HashSet::new(),
        );
        assert_eq!(p.len(), 2);
        assert!(p.iter().any(|x| x.field == "trace_id"));
        assert!(p.iter().any(|x| x.field == "user_id"));
    }

    /// End-to-end: writer produces a `.bf`, reader's single-block API resolves
    /// the same membership the prune logic expects.
    #[test]
    fn test_writer_reader_predicate_contract() {
        let id_a: u64 = 101;
        let id_b: u64 = 102;

        // Both files share a uniform B (transposed layout requirement).
        let nb = infra::bloom::num_blocks_for(100, 0.01);
        let mut bb = BloomBuilder::new();
        let i_a = bb.begin_with_blocks(id_a, "trace_id", nb);
        bb.insert(i_a, b"present-A");
        let i_b = bb.begin_with_blocks(id_b, "trace_id", nb);
        bb.insert(i_b, b"present-B");
        let blob = BloomWriter::serialize(bb.finish()).unwrap();
        let r = BloomReader::parse(&blob).unwrap();

        let check = |field: &str, file_id: u64, v: &[u8]| {
            let (range, h) = r.row_range(field, v).unwrap();
            let col = r.column_index(field, file_id).unwrap();
            let row = &blob[range.start as usize..range.end as usize];
            let off = col * BLOCK_BYTES;
            let block: &[u8; BLOCK_BYTES] = row[off..off + BLOCK_BYTES].try_into().unwrap();
            BloomReader::check_block_with_hash(block, h)
        };

        let pred = Predicate {
            field: "trace_id".into(),
            values: vec!["present-A".into()],
            composite_fallback: false,
        };
        // file A: any value matches → kept
        assert!(
            pred.values
                .iter()
                .any(|v| check(&pred.field, id_a, v.as_bytes()))
        );
        // file B: no value matches → would be dropped
        assert!(
            !pred
                .values
                .iter()
                .any(|v| check(&pred.field, id_b, v.as_bytes()))
        );

        // IN list with one present + one absent → still kept (OR within)
        let pred_in = Predicate {
            field: "trace_id".into(),
            values: vec!["present-A".into(), "absent-X".into()],
            composite_fallback: false,
        };
        assert!(
            pred_in
                .values
                .iter()
                .any(|v| check(&pred_in.field, id_a, v.as_bytes()))
        );

        // AND across predicates: any all-miss drops
        let pred_other = Predicate {
            field: "trace_id".into(),
            values: vec!["absent-Z".into()],
            composite_fallback: false,
        };
        let preds = [pred, pred_other];
        let kept = preds
            .iter()
            .all(|p| p.values.iter().any(|v| check(&p.field, id_a, v.as_bytes())));
        assert!(!kept);
    }

    #[test]
    fn composite_scope_controls_admission_and_additive_fallback() {
        let only_trace_id = only_scope(&["trace_id"]);
        let trace = cond(vec![Condition::Equal("trace_id".into(), "abc".into())]);
        let infer = cond(vec![Condition::Equal(
            "infer_service_name".into(),
            "api".into(),
        )]);

        let p = collect_decidable(&trace, &fields(&[]), &only_trace_id, false, &HashSet::new());
        assert_eq!(p.len(), 1, "explicit Bloom-only field is admitted");
        assert!(p[0].composite_fallback);
        assert!(
            collect_decidable(&infer, &fields(&[]), &only_trace_id, false, &HashSet::new(),)
                .is_empty(),
            "selective scope rejects unrelated fields"
        );

        let p = collect_decidable(
            &infer,
            &fields(&["infer_service_name"]),
            &only_trace_id,
            false,
            &HashSet::new(),
        );
        assert_eq!(p.len(), 1, "additive fields always remain decidable");
        assert!(
            !p[0].composite_fallback,
            "additive field outside the scope stays per-field only"
        );

        let p = collect_decidable(
            &infer,
            &fields(&[]),
            &VixBloomCompositeScope::All,
            false,
            &HashSet::new(),
        );
        assert_eq!(p.len(), 1, "All preserves legacy any-field admission");
        assert!(p[0].composite_fallback);
    }

    #[test]
    fn applicability_preflight_matches_selective_and_broad_admission() {
        let only_trace_id = only_scope(&["trace_id"]);
        let trace = cond(vec![Condition::Equal("trace_id".into(), "abc".into())]);
        let infer = cond(vec![Condition::Equal(
            "infer_service_name".into(),
            "api".into(),
        )]);

        assert_applicability_parity(&trace, &[], &only_trace_id, false, &[], true);
        assert_applicability_parity(&infer, &[], &only_trace_id, false, &[], false);
        assert_applicability_parity(
            &infer,
            &["infer_service_name"],
            &only_trace_id,
            false,
            &[],
            true,
        );
        assert_applicability_parity(&infer, &[], &VixBloomCompositeScope::All, false, &[], true);
    }

    #[test]
    fn auto_id_scope_admission_matches_owned_planner() {
        let selective = only_scope(&["trace_id"]);

        for field in ["reference.parent_trace_id", "event_id"] {
            let condition = cond(vec![Condition::Equal(field.into(), "abc".into())]);
            assert_applicability_parity(&condition, &[], &selective, false, &[], false);
            assert_applicability_parity(&condition, &[], &selective, true, &[], true);
            assert_applicability_parity(&condition, &[], &selective, true, &[field], false);
        }

        for field in ["span_duration_nano", "events", "infer_service_name"] {
            let condition = cond(vec![Condition::Equal(field.into(), "abc".into())]);
            assert_applicability_parity(&condition, &[], &selective, true, &[], false);
            assert_applicability_parity(
                &condition,
                &[],
                &VixBloomCompositeScope::All,
                false,
                &[],
                true,
            );
        }
    }

    #[test]
    fn applicability_preflight_matches_recursive_or_and_in_rejections() {
        let broad = VixBloomCompositeScope::All;
        let nested_same_field = cond(vec![Condition::Or(
            Box::new(Condition::Equal("trace_id".into(), "a".into())),
            Box::new(Condition::Or(
                Box::new(Condition::In(
                    "trace_id".into(),
                    vec!["b".into(), "c".into()],
                    false,
                )),
                Box::new(Condition::Equal("trace_id".into(), "d".into())),
            )),
        )]);
        let nested_mixed_fields = cond(vec![Condition::Or(
            Box::new(Condition::Equal("trace_id".into(), "a".into())),
            Box::new(Condition::Or(
                Box::new(Condition::Equal("trace_id".into(), "b".into())),
                Box::new(Condition::Equal("span_id".into(), "c".into())),
            )),
        )]);
        let empty_in = cond(vec![Condition::In("trace_id".into(), vec![], false)]);
        let negated_in = cond(vec![Condition::In(
            "trace_id".into(),
            vec!["a".into()],
            true,
        )]);

        assert_applicability_parity(&nested_same_field, &[], &broad, false, &[], true);
        assert_applicability_parity(&nested_mixed_fields, &[], &broad, false, &[], false);
        assert_applicability_parity(&empty_in, &["trace_id"], &broad, false, &[], false);
        assert_applicability_parity(&negated_in, &["trace_id"], &broad, false, &[], false);
    }

    #[test]
    fn applicability_preflight_borrows_large_in_payload() {
        let condition = cond(vec![Condition::In(
            "trace_id".into(),
            (0..4096).map(|value| value.to_string()).collect(),
            false,
        )]);
        let Condition::In(original_field, original_values, false) = &condition.conditions[0] else {
            panic!("test condition must remain a positive IN");
        };
        let borrowed_field = decidable_field(&condition.conditions[0]).unwrap();

        assert_eq!(original_values.len(), 4096);
        assert!(
            std::ptr::eq(borrowed_field.as_ptr(), original_field.as_ptr()),
            "the shape preflight must return the condition's borrowed field"
        );
        assert_applicability_parity(
            &condition,
            &[],
            &only_scope(&["trace_id"]),
            false,
            &[],
            true,
        );
    }

    /// Composite-section guard contract over REAL accumulator-built bytes:
    /// a value hit keeps, a covered miss drops, and both an uncovered field
    /// (guard miss) and a composite-less legacy file stay "no info" — the
    /// keep direction used by policy-scoped composite fallback.
    #[test]
    fn composite_probe_contract_with_guards() {
        use vortex_index::bloom::{
            BloomHashAcc, COMPOSITE_BLOOM_FIELD, COMPOSITE_GUARD_PROBES, composite_guard_key,
            composite_value_key,
        };

        let v1_key = |value: &[u8], id: u16| {
            let mut k = value.to_vec();
            k.push(0);
            k.extend_from_slice(&id.to_be_bytes());
            k
        };
        let build_composite = |file_id: u64, value: &str| -> infra::bloom::FieldBloom {
            let mut acc = BloomHashAcc::default();
            acc.enable_composite([(1u16, "svc".to_string())]);
            acc.observe(&v1_key(value.as_bytes(), 1));
            let blooms = acc.build(0.001);
            assert_eq!(blooms.len(), 1);
            infra::bloom::FieldBloom {
                field: blooms[0].field.clone(),
                file_id,
                n_items: blooms[0].n_items,
                bytes: blooms[0].bytes.clone(),
            }
        };
        let (id_a, id_b, id_c) = (301u64, 302, 303);
        // files A/B carry a composite covering `svc`; legacy file C carries
        // only a per-field trace_id section (pre-composite writer)
        let mut sections = vec![
            build_composite(id_a, "api-1"),
            build_composite(id_b, "api-2"),
        ];
        {
            let mut acc = BloomHashAcc::from_pairs([(1u16, "trace_id".to_string())]);
            acc.observe(&v1_key(b"t-1", 1));
            let blooms = acc.build(0.001);
            sections.push(infra::bloom::FieldBloom {
                field: blooms[0].field.clone(),
                file_id: id_c,
                n_items: blooms[0].n_items,
                bytes: blooms[0].bytes.clone(),
            });
        }
        let blob = BloomWriter::serialize(sections).unwrap();
        let r = BloomReader::parse(&blob).unwrap();

        // the run_group primitive: probe one key for one file, None = no info
        let probe = |section: &str, key: &[u8], file_id: u64| -> Option<bool> {
            let (range, hash) = r.row_range(section, key)?;
            let col = r.column_index(section, file_id)?;
            let row = &blob[range.start as usize..range.end as usize];
            let off = col * BLOCK_BYTES;
            let block: &[u8; BLOCK_BYTES] = row[off..off + BLOCK_BYTES].try_into().unwrap();
            Some(check_block_with_mask(block, &mask_from_hash(hash)))
        };
        let covered = |field: &str, file_id: u64| -> Option<bool> {
            let mut buf = Vec::new();
            let mut all = true;
            for p in 0..COMPOSITE_GUARD_PROBES {
                let key = composite_guard_key(field, p, &mut buf).unwrap();
                all &= probe(COMPOSITE_BLOOM_FIELD, key, file_id)?;
            }
            Some(all)
        };
        let composite_probe = |field: &str, value: &str, file_id: u64| -> Option<bool> {
            let mut buf = Vec::new();
            let key = composite_value_key(field, value.as_bytes(), &mut buf).unwrap();
            probe(COMPOSITE_BLOOM_FIELD, key, file_id)
        };

        // A and B are covered for scoped `svc`…
        assert_eq!(covered("svc", id_a), Some(true));
        assert_eq!(covered("svc", id_b), Some(true));
        // …so their fallback value probes are authoritative: A keeps, B drops
        assert_eq!(composite_probe("svc", "api-1", id_a), Some(true));
        assert_eq!(composite_probe("svc", "api-1", id_b), Some(false));
        // Legacy C has no composite column at all → no info → keep.
        assert_eq!(composite_probe("svc", "api-1", id_c), None);
        assert_eq!(covered("svc", id_c), None);

        // A field the file did not cover stays no-info because its guards miss.
        assert_eq!(covered("severity", id_a), Some(false));
        assert_eq!(covered("severity", id_b), Some(false));
    }

    #[tokio::test]
    async fn test_files_without_bloom_pass_through() {
        let files = vec![
            fk("files/o/logs/s/2026/05/08/14/a.parquet", 0),
            fk("files/o/logs/s/2026/05/08/14/b.parquet", 0),
        ];
        let c = cond(vec![Condition::Equal("trace_id".into(), "x".into())]);
        let kept = prune(
            "tid",
            "o",
            StreamType::Logs,
            "s",
            files.clone(),
            &c,
            vec!["trace_id".to_string()],
            &only_scope(&[]),
            false,
            &HashSet::new(),
        )
        .await;
        assert_eq!(kept.len(), files.len());
    }

    #[tokio::test]
    async fn test_no_decidable_predicate_keeps_everything() {
        let files = vec![fk("files/o/logs/s/2026/05/08/14/a.parquet", 123)];
        // A NotEqual is not bloom-decidable → prune returns input untouched.
        let c = cond(vec![Condition::NotEqual("trace_id".into(), "x".into())]);
        let kept = prune(
            "tid",
            "o",
            StreamType::Logs,
            "s",
            files.clone(),
            &c,
            vec!["trace_id".to_string()],
            &only_scope(&[]),
            false,
            &HashSet::new(),
        )
        .await;
        assert_eq!(kept.len(), 1);
    }

    #[tokio::test]
    async fn test_missing_bf_keeps_all_files_in_group() {
        let files = vec![fk("files/o/logs/missing/2026/05/08/14/a.parquet", 9_999)];
        let c = cond(vec![Condition::Equal("trace_id".into(), "x".into())]);
        let kept = prune(
            "tid",
            "o",
            StreamType::Logs,
            "missing",
            files.clone(),
            &c,
            vec!["trace_id".to_string()],
            &only_scope(&[]),
            false,
            &HashSet::new(),
        )
        .await;
        assert_eq!(kept.len(), 1);
    }

    /// End-to-end policy check against a real old broad-composite `.bf`.
    /// Selective AUTO-ID admits semantic IDs but not ordinary fields, the
    /// gate-off and broad policies remain compatible, NEVER wins for semantic
    /// admission, and a mixed-era file without coverage remains fail-open.
    #[tokio::test(flavor = "multi_thread")] // the disk-cache read path uses block_in_place
    async fn composite_scope_policy_against_old_broad_bytes() {
        use vortex_index::bloom::BloomHashAcc;

        let v1_key = |value: &[u8], id: u16| {
            let mut key = value.to_vec();
            key.push(0);
            key.extend_from_slice(&id.to_be_bytes());
            key
        };
        let broad_composite = |file_id: u64, values: [&str; 6]| {
            let mut acc = BloomHashAcc::default();
            acc.enable_composite([
                (1u16, "trace_id".to_string()),
                (2u16, "reference.parent_trace_id".to_string()),
                (3u16, "event_id".to_string()),
                (4u16, "infer_service_name".to_string()),
                (5u16, "span_duration_nano".to_string()),
                (6u16, "events".to_string()),
            ]);
            for (offset, value) in values.into_iter().enumerate() {
                acc.observe(&v1_key(value.as_bytes(), offset as u16 + 1));
            }
            let blooms = acc.build(0.001);
            assert_eq!(blooms.len(), 1);
            infra::bloom::FieldBloom {
                field: blooms[0].field.clone(),
                file_id,
                n_items: blooms[0].n_items,
                bytes: blooms[0].bytes.clone(),
            }
        };
        let legacy_unrelated = |file_id: u64| {
            let mut acc = BloomHashAcc::from_pairs([(7u16, "legacy_other".to_string())]);
            acc.observe(&v1_key(b"legacy", 7));
            let blooms = acc.build(0.001);
            infra::bloom::FieldBloom {
                field: blooms[0].field.clone(),
                file_id,
                n_items: blooms[0].n_items,
                bytes: blooms[0].bytes.clone(),
            }
        };
        let blob = BloomWriter::serialize(vec![
            broad_composite(
                301,
                ["trace-a", "parent-a", "event-a", "api-a", "100", "events-a"],
            ),
            broad_composite(
                302,
                ["trace-b", "parent-b", "event-b", "api-b", "200", "events-b"],
            ),
            legacy_unrelated(303),
        ])
        .unwrap();

        const VER: i64 = 424_242;
        let date = "2026/05/08/14";
        let stream = "s-composite-scope-e2e";
        let path = bloom_path("o", StreamType::Logs, stream, date, VER);
        let account = infra::storage::get_account("o", &path).unwrap_or_default();
        infra::storage::put(&account, &path, bytes::Bytes::from(blob))
            .await
            .expect("local test object store accepts the .bf");

        let file = |id: i64, name: &str| {
            let mut key = fk(&format!("files/o/logs/{stream}/{date}/{name}.parquet"), VER);
            key.id = id;
            key
        };
        let files = vec![file(301, "a"), file(302, "b"), file(303, "legacy")];
        let selective = only_scope(&["trace_id"]);

        // Explicit scope remains authoritative even with AUTO-ID disabled.
        let condition = cond(vec![Condition::Equal("trace_id".into(), "trace-a".into())]);
        let kept = prune(
            "tid",
            "o",
            StreamType::Logs,
            stream,
            files.clone(),
            &condition,
            Vec::new(),
            &selective,
            false,
            &HashSet::new(),
        )
        .await;
        assert_eq!(
            kept.iter().map(|file| file.id).collect::<Vec<_>>(),
            vec![301, 303],
            "explicit scope prunes covered misses and keeps missing coverage"
        );

        // Gate-off selective behavior is unchanged: non-explicit IDs do no
        // Bloom work even when old broad composite bytes cover them.
        for (field, value) in [
            ("reference.parent_trace_id", "parent-a"),
            ("event_id", "event-a"),
        ] {
            let condition = cond(vec![Condition::Equal(field.into(), value.into())]);
            let kept = prune(
                "tid",
                "o",
                StreamType::Logs,
                stream,
                files.clone(),
                &condition,
                Vec::new(),
                &selective,
                false,
                &HashSet::new(),
            )
            .await;
            assert_eq!(
                kept.len(),
                files.len(),
                "gate-off selective policy touched {field}"
            );
        }

        // AUTO-ID extends selective composite admission only to semantic IDs.
        for (field, value) in [
            ("trace_id", "trace-a"),
            ("reference.parent_trace_id", "parent-a"),
            ("event_id", "event-a"),
        ] {
            let condition = cond(vec![Condition::Equal(field.into(), value.into())]);
            let kept = prune(
                "tid",
                "o",
                StreamType::Logs,
                stream,
                files.clone(),
                &condition,
                Vec::new(),
                &selective,
                true,
                &HashSet::new(),
            )
            .await;
            assert_eq!(
                kept.iter().map(|file| file.id).collect::<Vec<_>>(),
                vec![301, 303],
                "AUTO-ID must prune covered misses and keep missing coverage for {field}"
            );
        }

        for (field, value) in [
            ("span_duration_nano", "100"),
            ("events", "events-a"),
            ("infer_service_name", "api-a"),
        ] {
            let condition = cond(vec![Condition::Equal(field.into(), value.into())]);
            let kept = prune(
                "tid",
                "o",
                StreamType::Logs,
                stream,
                files.clone(),
                &condition,
                Vec::new(),
                &selective,
                true,
                &HashSet::new(),
            )
            .await;
            assert_eq!(
                kept.len(),
                files.len(),
                "AUTO-ID selective policy must never use old broad bytes for {field}"
            );
        }

        let condition = cond(vec![Condition::Equal(
            "reference.parent_trace_id".into(),
            "parent-a".into(),
        )]);
        let kept = prune(
            "tid",
            "o",
            StreamType::Logs,
            stream,
            files.clone(),
            &condition,
            Vec::new(),
            &selective,
            true,
            &fields(&["reference.parent_trace_id"]),
        )
        .await;
        assert_eq!(
            kept.len(),
            files.len(),
            "NEVER must override semantic AUTO-ID admission"
        );

        // Broad All remains compatible with legacy any-field admission.
        let condition = cond(vec![Condition::Equal(
            "infer_service_name".into(),
            "api-a".into(),
        )]);
        let kept = prune(
            "tid",
            "o",
            StreamType::Logs,
            stream,
            files.clone(),
            &condition,
            Vec::new(),
            &VixBloomCompositeScope::All,
            false,
            &HashSet::new(),
        )
        .await;
        assert_eq!(
            kept.iter().map(|file| file.id).collect::<Vec<_>>(),
            vec![301, 303],
            "All preserves legacy any-field composite pruning"
        );
    }

    /// #52/M7 end-to-end: a field AUTO-demoted at first encode has only a
    /// composite section. It must not prune outside the effective composite
    /// scope; once explicitly scoped, fallback keeps a value hit, drops
    /// covered misses, and preserves the writer/pruner key contract.
    #[tokio::test(flavor = "multi_thread")] // the disk-cache read path uses block_in_place
    async fn demoted_at_birth_blob_prunes_end_to_end() {
        use std::sync::Arc;

        use arrow::{
            array::{ArrayRef, Int64Array, RecordBatch, StringArray},
            datatypes::{DataType, Field, Schema},
        };
        use vortex_index::{VixReader, VixWriter, VixWriterOptions};

        // one demoted-at-birth file per salt: 8 distinct trace ids / 8 rows
        // crosses the (shrunk) AUTO thresholds at finish
        let demoted_blooms = |salt: &str| {
            let schema = Arc::new(Schema::new(vec![
                Field::new("_timestamp", DataType::Int64, false),
                Field::new("trace_id", DataType::Utf8, true),
            ]));
            let ids: Vec<String> = (0..8).map(|i| format!("t-{salt}-{i:04}")).collect();
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(
                        (0..8i64).map(|i| 1_000 - i).collect::<Vec<_>>(),
                    )) as ArrayRef,
                    Arc::new(StringArray::from(
                        ids.iter().map(String::as_str).collect::<Vec<_>>(),
                    )) as ArrayRef,
                ],
            )
            .unwrap();
            let source = StringArray::from_iter_values(
                ids.iter().map(|t| format!(r#"{{"trace_id":"{t}"}}"#)),
            );
            let mut writer = VixWriter::new(
                &schema,
                VixWriterOptions {
                    bloom_composite: true,
                    bloom_only_auto_ratio: 0.5,
                    bloom_only_min_distinct: 4,
                    ..Default::default()
                },
                false,
            );
            writer
                .push_batch_with_source(&batch, &source, None)
                .unwrap();
            let (data, index) = writer.finish().unwrap();
            let reader =
                VixReader::open_with_index(bytes::Bytes::from(data), index.map(bytes::Bytes::from))
                    .unwrap();
            assert_eq!(
                reader.bloom_only_fields().collect::<Vec<_>>(),
                ["trace_id"],
                "precondition: demoted at first encode"
            );
            reader.file_blooms().unwrap().expect("per-file blob")
        };

        // the group assembler's transpose: per-file blob sections -> .bf
        let to_field_blooms = |file_id: u64, salt: &str| {
            demoted_blooms(salt)
                .into_iter()
                .map(move |b| infra::bloom::FieldBloom {
                    field: b.field,
                    file_id,
                    n_items: b.n_items,
                    bytes: b.bytes,
                })
                .collect::<Vec<_>>()
        };
        let mut sections = to_field_blooms(401, "a");
        sections.extend(to_field_blooms(402, "b"));
        let blob = BloomWriter::serialize(sections).unwrap();

        const VER: i64 = 525_252;
        let date = "2026/05/08/15";
        let path = bloom_path("o", StreamType::Logs, "s-m7-demoted", date, VER);
        let account = infra::storage::get_account("o", &path).unwrap_or_default();
        infra::storage::put(&account, &path, bytes::Bytes::from(blob))
            .await
            .expect("local test object store accepts the .bf");

        let file = |id: i64, name: &str| {
            let mut k = fk(
                &format!("files/o/logs/s-m7-demoted/{date}/{name}.parquet"),
                VER,
            );
            k.id = id;
            k
        };
        let files = vec![file(401, "a"), file(402, "b")];

        // Composite data alone is insufficient: without effective scope,
        // the demoted trace_id predicate must leave both files untouched.
        let c = cond(vec![Condition::Equal("trace_id".into(), "t-a-0003".into())]);
        let kept = prune(
            "tid",
            "o",
            StreamType::Logs,
            "s-m7-demoted",
            files.clone(),
            &c,
            Vec::new(),
            &only_scope(&[]),
            false,
            &HashSet::new(),
        )
        .await;
        assert_eq!(kept.len(), 2, "out-of-scope demoted field is untouched");

        // Once explicitly scoped, the missing per-field section falls back to
        // composite: the holder stays and the covered miss drops.
        let only_trace_id = only_scope(&["trace_id"]);
        let kept = prune(
            "tid",
            "o",
            StreamType::Logs,
            "s-m7-demoted",
            files.clone(),
            &c,
            Vec::new(),
            &only_trace_id,
            false,
            &HashSet::new(),
        )
        .await;
        let kept_ids: Vec<i64> = kept.iter().map(|f| f.id).collect();
        assert_eq!(kept_ids, vec![401], "holder kept, covered miss dropped");

        // an absent value: both files claim coverage → both drop
        let c = cond(vec![Condition::Equal(
            "trace_id".into(),
            "t-nowhere".into(),
        )]);
        let kept = prune(
            "tid",
            "o",
            StreamType::Logs,
            "s-m7-demoted",
            files.clone(),
            &c,
            Vec::new(),
            &only_trace_id,
            false,
            &HashSet::new(),
        )
        .await;
        assert!(kept.is_empty(), "covered misses drop every file");
    }

    /// M15: a CONFIGURED bloom field (stream-settings `bloom_filter_fields`)
    /// that was #52-demoted has NO per-field `.bf` section — the pruner
    /// falls back to the COMPOSITE section for exactly the uncovered files:
    /// keep/drop PARITY with a per-field-covered (non-demoted) control
    /// group, per-file verdict priority in a MIXED group, guard fail-open
    /// for uncovered fields, and the disabled-scope behavior unchanged.
    #[tokio::test(flavor = "multi_thread")] // the disk-cache read path uses block_in_place
    async fn configured_demoted_field_prunes_via_composite_fallback() {
        use vortex_index::bloom::BloomHashAcc;

        let v1_key = |value: &[u8], id: u16| {
            let mut k = value.to_vec();
            k.push(0);
            k.extend_from_slice(&id.to_be_bytes());
            k
        };
        // a non-demoted #48-era file: per-field section AND composite
        let both_sections = |file_id: u64, field: &str, value: &str| {
            let mut acc = BloomHashAcc::from_pairs([(1u16, field.to_string())]);
            acc.enable_composite([(1u16, field.to_string())]);
            acc.observe(&v1_key(value.as_bytes(), 1));
            acc.build(0.001)
                .into_iter()
                .map(|b| infra::bloom::FieldBloom {
                    field: b.field,
                    file_id,
                    n_items: b.n_items,
                    bytes: b.bytes,
                })
                .collect::<Vec<_>>()
        };
        // a #52-DEMOTED file: composite section only (no per-field bloom)
        let composite_only = |file_id: u64, field: &str, value: &str| {
            let mut acc = BloomHashAcc::default();
            acc.enable_composite([(1u16, field.to_string())]);
            acc.observe(&v1_key(value.as_bytes(), 1));
            acc.build(0.001)
                .into_iter()
                .map(|b| infra::bloom::FieldBloom {
                    field: b.field,
                    file_id,
                    n_items: b.n_items,
                    bytes: b.bytes,
                })
                .collect::<Vec<_>>()
        };

        let stream = "s-m15-demoted-configured";
        let date = "2026/05/08/16";
        let store_bf = |ver: i64, sections: Vec<infra::bloom::FieldBloom>| async move {
            let blob = BloomWriter::serialize(sections).unwrap();
            let path = bloom_path("o", StreamType::Logs, stream, date, ver);
            let account = infra::storage::get_account("o", &path).unwrap_or_default();
            infra::storage::put(&account, &path, bytes::Bytes::from(blob))
                .await
                .expect("local test object store accepts the .bf");
        };
        let file = |id: i64, ver: i64, name: &str| {
            let mut k = fk(&format!("files/o/logs/{stream}/{date}/{name}.parquet"), ver);
            k.id = id;
            k
        };

        // CONTROL group (ver 611): per-field sections present (non-demoted)
        const CONTROL_VER: i64 = 611_000;
        let mut sections = both_sections(601, "trace_id", "t-1");
        sections.extend(both_sections(602, "trace_id", "t-2"));
        store_bf(CONTROL_VER, sections).await;
        // DEMOTED group (ver 612): composite only — the M15 shape
        const DEMOTED_VER: i64 = 612_000;
        let mut sections = composite_only(603, "trace_id", "t-1");
        sections.extend(composite_only(604, "trace_id", "t-2"));
        store_bf(DEMOTED_VER, sections).await;
        // MIXED group (ver 613): one covered, one demoted
        const MIXED_VER: i64 = 613_000;
        let mut sections = both_sections(605, "trace_id", "m-1");
        sections.extend(composite_only(606, "trace_id", "m-2"));
        store_bf(MIXED_VER, sections).await;
        // UNCOVERED group (ver 614): composite covers a DIFFERENT field only
        const UNCOVERED_VER: i64 = 614_000;
        store_bf(UNCOVERED_VER, composite_only(607, "other_field", "x-1")).await;

        let all_files = || {
            vec![
                file(601, CONTROL_VER, "c1"),
                file(602, CONTROL_VER, "c2"),
                file(603, DEMOTED_VER, "d1"),
                file(604, DEMOTED_VER, "d2"),
                file(605, MIXED_VER, "m1"),
                file(606, MIXED_VER, "m2"),
                file(607, UNCOVERED_VER, "u1"),
            ]
        };
        let run = |value: &str, composite_scope: VixBloomCompositeScope| {
            let c = cond(vec![Condition::Equal("trace_id".into(), value.into())]);
            let files = all_files();
            async move {
                let mut kept: Vec<i64> = prune(
                    "tid",
                    "o",
                    StreamType::Logs,
                    stream,
                    files,
                    &c,
                    vec!["trace_id".to_string()], // CONFIGURED field
                    &composite_scope,
                    false,
                    &HashSet::new(),
                )
                .await
                .iter()
                .map(|f| f.id)
                .collect();
                kept.sort_unstable();
                kept
            }
        };

        // t-1: PARITY — the demoted holder (603) keeps and the demoted
        // non-holder (604) drops exactly like the per-field control pair;
        // mixed group: 605 drops per-field, 606 drops via composite;
        // uncovered 607 stays (guards miss → no info)
        assert_eq!(
            run("t-1", only_scope(&["trace_id"])).await,
            vec![601, 603, 607]
        );
        // t-2: the mirror image
        assert_eq!(
            run("t-2", only_scope(&["trace_id"])).await,
            vec![602, 604, 607]
        );
        // m-2 (mixed group): 605's PER-FIELD verdict governs it (miss →
        // drop) even though its composite also covers the field; 606 (no
        // per-field column) keeps via its composite hit
        assert_eq!(run("m-2", only_scope(&["trace_id"])).await, vec![606, 607]);
        // m-1: 605 per-field hit keeps; 606 composite covered miss drops
        assert_eq!(run("m-1", only_scope(&["trace_id"])).await, vec![605, 607]);
        // absent value: every covered file drops, only the uncovered stays
        assert_eq!(run("t-9", only_scope(&["trace_id"])).await, vec![607]);
        // Composite disabled: the per-field control still prunes, every
        // demoted/uncovered file is no-info kept.
        assert_eq!(
            run("t-1", only_scope(&[])).await,
            vec![601, 603, 604, 606, 607]
        );
    }

    #[tokio::test]
    async fn test_unparseable_key_kept() {
        let files = vec![fk("not-a-files-path", 1234)];
        let c = cond(vec![Condition::Equal("trace_id".into(), "x".into())]);
        let kept = prune(
            "tid",
            "o",
            StreamType::Logs,
            "s",
            files,
            &c,
            vec!["trace_id".to_string()],
            &only_scope(&[]),
            false,
            &HashSet::new(),
        )
        .await;
        assert_eq!(kept.len(), 1);
    }

    #[test]
    fn test_footer_shortfall() {
        let total = 10_000u64;
        let mut s = vec![0u8; 64]; // last 8 bytes = footer_len(4) + magic(4)
        // footer fits within the 64-byte suffix (needed = 40 + 8 = 48 <= 64)
        s[56..60].copy_from_slice(&40u32.to_le_bytes());
        assert_eq!(footer_shortfall(&s, total), None);
        // footer bigger than the suffix → re-read footer_len + 8
        s[56..60].copy_from_slice(&5000u32.to_le_bytes());
        assert_eq!(footer_shortfall(&s, total), Some(5008));
        // implausible footer_len (> total) → None, let parse reject it
        s[56..60].copy_from_slice(&20_000u32.to_le_bytes());
        assert_eq!(footer_shortfall(&s, total), None);
        // too short to even hold the trailer → None
        assert_eq!(footer_shortfall(&[0u8; 4], total), None);
    }

    /// A `.bf` whose footer exceeds a small probe must still parse once we
    /// re-read exactly `footer_shortfall` bytes from the tail — the two-step
    /// read that keeps large `.bf`s prunable instead of falling back.
    #[test]
    fn test_large_footer_two_step_parse() {
        // 300 files in one field → footer ≈ 3.6 KB, well over a 256 B probe.
        let mut bb = BloomBuilder::new();
        for fid in 0..300u64 {
            let i = bb.begin_with_blocks(fid, "trace_id", 4);
            bb.insert(i, format!("v-{fid}").as_bytes());
        }
        let blob = BloomWriter::serialize(bb.finish()).unwrap();
        let total = blob.len() as u64;

        // Small probe that does NOT cover the whole footer.
        let probe = 256.min(blob.len());
        let small = &blob[blob.len() - probe..];
        let needed = footer_shortfall(small, total).expect("footer exceeds the small probe");

        // Re-read exactly `needed` trailing bytes and parse — must succeed.
        let big = &blob[blob.len() - needed as usize..];
        let r = BloomReader::parse_suffix(big, total).expect("parse with precise footer suffix");
        assert!(r.column_index("trace_id", 0).is_some());
        assert!(r.column_index("trace_id", 299).is_some());
    }
}
