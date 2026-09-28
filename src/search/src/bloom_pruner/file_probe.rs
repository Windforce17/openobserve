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

//! Stage 2 of the bloom prune: files no group `.bf` covers yet.
//!
//! The assembler stamps `bloom_ver` per `(stream, hour)` bucket on the
//! compactor's cadence, skips the open hour, and every merge output starts
//! over at `bloom_ver = 0` — so the most recent hours of a stream are
//! exactly the files the group pass cannot see, and a needle lookup over
//! them used to fall through to a full scan (2026-09-28: 121 of 121 recent
//! traces files per follower, 2.4 TB, 12 s for 23 rows).
//!
//! Those files still carry the very same SBBF blocks inside their own
//! sidecar. This stage opens the sidecar footer once per immutable sidecar
//! identity (memoized), then answers each query with one batched fetch of
//! the addressed 32-byte blocks ([`FileBloomProbe`]). Verdicts follow the
//! group pass: AND across predicates, OR within a predicate's values, "no
//! information" keeps the file. Any failure or a stage deadline keeps the
//! affected files — bloom is performance, not correctness.

use std::{
    sync::{Arc, LazyLock},
    time::{Duration, Instant},
};

use config::meta::stream::FileKey;
use futures::stream::{self, StreamExt};
use hashlink::LruCache;
use parking_lot::Mutex;
use vortex_index::{FileBloomProbe, VixRangeSource};

use super::Predicate;
use crate::vix::{reader_cache::ReaderCacheKey, source::LadderRangeSource};

/// Section tables of recently opened sidecars, by immutable sidecar identity
/// (data file, index generation, index size). An entry is normally a few
/// hundred bytes — blob offsets and field names, never a filter body — but a
/// blob that crosses the sidecar's eager-tail boundary pins that tail copy,
/// so the cache is bounded by accounted bytes, LRU-evicted. `None` remembers
/// a sidecar without a bloom blob so pre-capability files are not re-opened
/// on every query.
const PROBE_CACHE_MAX_BYTES: usize = 64 * 1024 * 1024;
static PROBE_CACHE: LazyLock<Mutex<ProbeCache>> =
    LazyLock::new(|| Mutex::new(ProbeCache::default()));

struct ProbeCache {
    lru: LruCache<ReaderCacheKey, (Arc<Option<FileBloomProbe>>, usize)>,
    total: usize,
}

impl Default for ProbeCache {
    fn default() -> Self {
        Self {
            lru: LruCache::new_unbounded(),
            total: 0,
        }
    }
}

impl ProbeCache {
    fn get(&mut self, key: &ReaderCacheKey) -> Option<Arc<Option<FileBloomProbe>>> {
        self.lru.get(key).map(|(probe, _)| Arc::clone(probe))
    }

    fn insert(&mut self, key: ReaderCacheKey, probe: Arc<Option<FileBloomProbe>>) {
        let bytes = std::mem::size_of::<ReaderCacheKey>()
            + key.file().len()
            + probe
                .as_ref()
                .as_ref()
                .map_or(0, FileBloomProbe::retained_bytes);
        if bytes > PROBE_CACHE_MAX_BYTES {
            return; // never worth pinning; re-opened on demand
        }
        if let Some((_, old)) = self.lru.insert(key, (probe, bytes)) {
            self.total -= old;
        }
        self.total += bytes;
        while self.total > PROBE_CACHE_MAX_BYTES {
            match self.lru.remove_lru() {
                Some((_, (_, evicted))) => self.total -= evicted,
                None => break,
            }
        }
    }
}

/// Wall-clock the stage may hold a query. Files still unprobed at the
/// deadline are kept; their blocking probes finish in the background and
/// warm the cache for the next query.
const STAGE_BUDGET: Duration = Duration::from_secs(2);

/// One stage run over the files without a `.bf`.
#[derive(Debug, Default)]
pub(super) struct Outcome {
    pub kept: Vec<FileKey>,
    /// Proven absent by an authoritative filter.
    pub dropped: usize,
    /// Kept: every predicate answered "maybe".
    pub hit: usize,
    /// Kept: at least one predicate had no filter in the file.
    pub no_info: usize,
    /// Kept: the sidecar carries no bloom blob (pre-capability file).
    pub no_blob: usize,
    /// Kept: no sidecar at all (index-off L0).
    pub no_sidecar: usize,
    /// Kept: open or fetch failed.
    pub failed: usize,
    /// Kept: not probed before the stage deadline.
    pub timed_out: usize,
    pub took: Duration,
}

impl Outcome {
    fn record(&self) {
        for (outcome, count) in [
            ("dropped", self.dropped),
            ("hit", self.hit),
            ("no_info", self.no_info),
            ("no_blob", self.no_blob),
            ("no_sidecar", self.no_sidecar),
            ("failed", self.failed),
            ("timed_out", self.timed_out),
        ] {
            if count > 0 {
                config::metrics::VIX_FILE_BLOOM_PROBE_FILES_TOTAL
                    .with_label_values(&[outcome])
                    .inc_by(count as u64);
            }
        }
    }
}

enum Verdict {
    Drop,
    Hit,
    NoInfo,
    NoBlob,
    NoSidecar,
}

enum ProbeResult {
    Verdict(Verdict),
    Failed(String),
    TimedOut,
}

/// The predicate shape the blocking probe needs, owned so it can cross into
/// the blocking pool once per stage.
struct ProbePredicate {
    field: String,
    values: Vec<String>,
    composite_fallback: bool,
}

/// Only the sidecar identity crosses into the blocking task; the `FileKey`
/// stays with the caller so a deadline or failure keeps it intact.
struct Target {
    key: String,
    account: String,
    index_generation: i64,
    index_size: i64,
}

impl Target {
    fn of(file: &FileKey) -> Self {
        Self {
            key: file.key.clone(),
            account: file.account.clone(),
            index_generation: file.meta.index_generation,
            index_size: file.meta.index_size,
        }
    }
}

/// Probe every file against `predicates`, `concurrency` sidecars at a time.
pub(super) async fn probe_files(
    trace_id: &str,
    files: Vec<FileKey>,
    predicates: &[Predicate],
    concurrency: usize,
) -> Outcome {
    let started = Instant::now();
    let deadline = tokio::time::Instant::now() + STAGE_BUDGET;
    let predicates: Arc<[ProbePredicate]> = predicates
        .iter()
        .map(|p| ProbePredicate {
            field: p.field.clone(),
            values: p.values.clone(),
            composite_fallback: p.composite_fallback,
        })
        .collect();
    let handle = tokio::runtime::Handle::current();
    let mut outcome = Outcome {
        kept: Vec::with_capacity(files.len()),
        ..Outcome::default()
    };
    let results: Vec<(FileKey, ProbeResult)> = stream::iter(files)
        .map(|file| {
            let predicates = Arc::clone(&predicates);
            let handle = handle.clone();
            let target = Target::of(&file);
            async move {
                if tokio::time::Instant::now() >= deadline {
                    return (file, ProbeResult::TimedOut);
                }
                let task =
                    tokio::task::spawn_blocking(move || probe_one(&target, &predicates, handle));
                let result = match tokio::time::timeout_at(deadline, task).await {
                    Err(_) => ProbeResult::TimedOut,
                    Ok(Err(join)) => ProbeResult::Failed(join.to_string()),
                    Ok(Ok(Err(e))) => ProbeResult::Failed(format!("{e:#}")),
                    Ok(Ok(Ok(verdict))) => ProbeResult::Verdict(verdict),
                };
                (file, result)
            }
        })
        .buffer_unordered(concurrency.max(1))
        .collect()
        .await;
    let mut first_error: Option<(String, String)> = None;
    for (file, result) in results {
        match result {
            ProbeResult::Verdict(Verdict::Drop) => outcome.dropped += 1,
            ProbeResult::Verdict(verdict) => {
                match verdict {
                    Verdict::Hit => outcome.hit += 1,
                    Verdict::NoInfo => outcome.no_info += 1,
                    Verdict::NoBlob => outcome.no_blob += 1,
                    Verdict::NoSidecar => outcome.no_sidecar += 1,
                    Verdict::Drop => unreachable!("handled above"),
                }
                outcome.kept.push(file);
            }
            ProbeResult::Failed(error) => {
                outcome.failed += 1;
                log::debug!(
                    "[trace_id {trace_id}] search->bloom: per-file probe of {} failed, keeping it: {error}",
                    file.key
                );
                first_error.get_or_insert_with(|| (file.key.clone(), error));
                outcome.kept.push(file);
            }
            ProbeResult::TimedOut => {
                outcome.timed_out += 1;
                outcome.kept.push(file);
            }
        }
    }
    if let Some((key, error)) = first_error {
        log::warn!(
            "[trace_id {trace_id}] search->bloom: {} per-file bloom probes failed (files kept), first `{key}`: {error}",
            outcome.failed
        );
    }
    outcome.took = started.elapsed();
    outcome.record();
    outcome
}

/// Open (or reuse) the sidecar's bloom view and run every predicate.
/// Blocking: ranged fetches run through `block_fetch` on this thread.
fn probe_one(
    target: &Target,
    predicates: &[ProbePredicate],
    handle: tokio::runtime::Handle,
) -> anyhow::Result<Verdict> {
    if target.index_size <= 0 {
        return Ok(Verdict::NoSidecar);
    }
    let Some(sidecar_key) = config::vix_sidecar_key(&target.key, target.index_generation) else {
        return Ok(Verdict::NoSidecar);
    };
    let cache_key = ReaderCacheKey::new(
        target.key.clone(),
        target.index_generation,
        target.index_size,
    );
    let cached = PROBE_CACHE.lock().get(&cache_key);
    let probe = match cached {
        Some(probe) => probe,
        None => {
            let source: Arc<dyn VixRangeSource> = Arc::new(LadderRangeSource::new(
                target.account.clone(),
                &sidecar_key,
                target.index_size as u64,
                handle,
            ));
            let opened = Arc::new(FileBloomProbe::open_sidecar(source)?);
            PROBE_CACHE.lock().insert(cache_key, Arc::clone(&opened));
            opened
        }
    };
    let Some(probe) = probe.as_ref() else {
        return Ok(Verdict::NoBlob);
    };
    let mut any_no_info = false;
    for predicate in predicates {
        let values: Vec<&[u8]> = predicate.values.iter().map(|v| v.as_bytes()).collect();
        match probe.probe(&predicate.field, &values, predicate.composite_fallback)? {
            None => any_no_info = true,
            Some(hits) => {
                if !hits.iter().any(|hit| *hit) {
                    return Ok(Verdict::Drop);
                }
            }
        }
    }
    Ok(if any_no_info {
        Verdict::NoInfo
    } else {
        Verdict::Hit
    })
}

#[cfg(test)]
pub(super) fn probe_cache_len() -> usize {
    PROBE_CACHE.lock().lru.len()
}
