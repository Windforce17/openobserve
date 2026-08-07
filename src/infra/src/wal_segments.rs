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

//! `wal_segments` metadata table for the S3-first ingest path
//! (DESIGN-SEGMENT-WAL.md).
//!
//! One row per uploaded segment object. The flusher registers rows with
//! [`add`] (idempotent on `(node_uuid, seq)` so a retry after a crash between
//! PUT and register never creates a second row). Builders take leased claims
//! with [`claim_pending`], keep them alive with [`heartbeat`], and complete
//! them with [`mark_built_with_files`], which registers the produced L0 file
//! rows and flips the segments in ONE transaction fenced by `builder_node` —
//! a short rows-affected count means the lease was lost, the whole
//! transaction rolled back (zero file rows), and the build must be
//! discarded. The querier reads not-yet-built segments with [`query_unbuilt`]
//! and the sweeper retires built rows via [`list_expired`] + [`delete`].
//!
//! Crash GC support: BEFORE uploading anything, a builder records its
//! deterministic L0 object keys in `l0_planned` with [`set_l0_planned`]
//! (fenced like `mark_built`; a short count means the lease was lost and the
//! build must be discarded before any upload). The completion flip clears
//! `l0_planned` in the same statement, so a non-empty `l0_planned` on a
//! long-dead row is exactly the evidence of a build that crashed between
//! upload and its fenced registration — the sweeper's GC pass finds those
//! rows via [`list_l0_orphan_rows`], fences out zombie commits with
//! [`gc_arm_l0_orphan`], and resets them with [`clear_l0_planned`].
//! [`object_keys_under`] feeds the GC's anti-join for segment objects that
//! were PUT but never registered.
//!
//! Backends: postgres (prod) and sqlite (local/tests), dispatched on
//! `ZO_META_STORE` exactly like `file_list` (`nats` maps to sqlite). All
//! timestamps are UTC microseconds (`config::utils::time::now_micros`).

use config::{
    get_config,
    meta::{meta_store::MetaStore, stream::FileKey},
    utils::time::now_micros,
};
use serde::{Deserialize, Serialize};

use crate::errors::{Error, Result};

pub const SEGMENT_KEY_PREFIX: &str = "wal_segments/";

const TABLE: &str = "wal_segments";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(i16)]
pub enum SegmentStatus {
    Pending = 0,
    Building = 1,
    Built = 2,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SegmentMeta {
    pub id: i64,
    pub node_uuid: String,
    pub seq: i64,
    pub object_key: String,
    pub min_ts: i64,
    pub max_ts: i64,
    pub size: i64,
    /// JSON array of "org/stream_type/stream" identities present in the
    /// segment — pruning input for query and build.
    pub streams: Vec<String>,
    pub status: SegmentStatus,
    pub builder_node: String,
    pub created_at: i64,
    pub updated_at: i64,
}

/// Raw row as stored — `streams` is the JSON text, `status` the raw smallint.
/// Conversion to [`SegmentMeta`] is fallible and names the row on bad data.
#[derive(Debug, sqlx::FromRow)]
struct SegmentRow {
    id: i64,
    node_uuid: String,
    seq: i64,
    object_key: String,
    min_ts: i64,
    max_ts: i64,
    size: i64,
    streams: String,
    status: i16,
    builder_node: String,
    created_at: i64,
    updated_at: i64,
}

impl SegmentRow {
    fn into_meta(self) -> Result<SegmentMeta> {
        let status = match self.status {
            0 => SegmentStatus::Pending,
            1 => SegmentStatus::Building,
            2 => SegmentStatus::Built,
            other => {
                return Err(Error::Message(format!(
                    "[WAL_SEGMENTS] row id={} object_key={} has unknown status {other}",
                    self.id, self.object_key
                )));
            }
        };
        // malformed stored JSON is an error naming the row — never a silent
        // empty list (an empty list would hide the segment from queries)
        let streams: Vec<String> = serde_json::from_str(&self.streams).map_err(|e| {
            Error::Message(format!(
                "[WAL_SEGMENTS] row id={} object_key={} has malformed streams JSON: {e}",
                self.id, self.object_key
            ))
        })?;
        Ok(SegmentMeta {
            id: self.id,
            node_uuid: self.node_uuid,
            seq: self.seq,
            object_key: self.object_key,
            min_ts: self.min_ts,
            max_ts: self.max_ts,
            size: self.size,
            streams,
            status,
            builder_node: self.builder_node,
            created_at: self.created_at,
            updated_at: self.updated_at,
        })
    }
}

fn rows_into_metas(rows: Vec<SegmentRow>) -> Result<Vec<SegmentMeta>> {
    rows.into_iter().map(SegmentRow::into_meta).collect()
}

fn use_postgres() -> bool {
    matches!(
        get_config().common.meta_store.as_str().into(),
        MetaStore::PostgreSQL
    )
}

fn ids_csv(ids: &[i64]) -> String {
    // i64 formatting only — safe to inline into SQL (same convention as
    // file_list_jobs id lists)
    ids.iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn secs_to_micros(secs: u64) -> i64 {
    i64::try_from(secs)
        .unwrap_or(i64::MAX)
        .saturating_mul(1_000_000)
}

/// LIKE pattern matching the JSON-encoded array element exactly as [`add`]
/// stored it (both sides go through `serde_json`, so names needing JSON
/// escapes still round-trip). LIKE wildcards (`%`/`_`) inside a name can only
/// WIDEN the match — an over-selected segment simply yields no frames for the
/// stream at decode time; there are no false negatives.
fn stream_like_pattern(org: &str, stream_type: &str, stream: &str) -> Result<String> {
    let token = serde_json::to_string(&format!("{org}/{stream_type}/{stream}")).map_err(|e| {
        Error::Message(format!(
            "[WAL_SEGMENTS] cannot encode stream token {org}/{stream_type}/{stream}: {e}"
        ))
    })?;
    Ok(format!("%{token}%"))
}

/// Reject degenerate registrations before any SQL: an empty streams list
/// makes the segment invisible to `query_unbuilt` forever, and a poisoned
/// time range breaks overlap pruning (the `validate_file_meta_for_add`
/// lesson — a live regression class). All rejections are
/// [`Error::InvalidFileMeta`]: the same input fails the same way forever, so
/// retry loops classify it as deterministic and bail instead of spinning.
fn validate_for_add(meta: &SegmentMeta) -> Result<()> {
    if meta.node_uuid.is_empty() {
        return Err(Error::InvalidFileMeta(format!(
            "[WAL_SEGMENTS] add object_key={}: empty node_uuid",
            meta.object_key
        )));
    }
    if meta.object_key.is_empty() {
        return Err(Error::InvalidFileMeta(format!(
            "[WAL_SEGMENTS] add node_uuid={} seq={}: empty object_key",
            meta.node_uuid, meta.seq
        )));
    }
    if meta.streams.is_empty() {
        return Err(Error::InvalidFileMeta(format!(
            "[WAL_SEGMENTS] add object_key={}: empty streams list",
            meta.object_key
        )));
    }
    if meta.min_ts <= 0 || meta.max_ts < meta.min_ts {
        return Err(Error::InvalidFileMeta(format!(
            "[WAL_SEGMENTS] add object_key={}: degenerate time range [{}, {}]",
            meta.object_key, meta.min_ts, meta.max_ts
        )));
    }
    Ok(())
}

/// Create the `wal_segments` table and its indexes on the active backend.
/// Called from `db_init`; idempotent.
pub async fn create_table() -> Result<()> {
    if use_postgres() {
        postgres::create_table().await
    } else {
        sqlite::create_table().await
    }
}

/// Register an uploaded segment. Idempotent on `(node_uuid, seq)`: a retry
/// after a crash between PUT and register returns the existing row's id
/// instead of creating a second row. The row is always registered as
/// `Pending` with `created_at`/`updated_at` set here — the caller-provided
/// `id`/`status`/`builder_node`/`created_at`/`updated_at` fields are ignored
/// on insert (they are read-side fields).
pub async fn add(meta: &SegmentMeta) -> Result<i64> {
    validate_for_add(meta)?;
    let streams_json = serde_json::to_string(&meta.streams).map_err(|e| {
        Error::Message(format!(
            "[WAL_SEGMENTS] add object_key={}: cannot serialize streams: {e}",
            meta.object_key
        ))
    })?;
    if use_postgres() {
        postgres::add(meta, &streams_json).await
    } else {
        sqlite::add(meta, &streams_json).await
    }
}

/// Atomically claim up to `limit` buildable segments for `node`, oldest
/// (`created_at`) first: rows in `Pending`, plus `Building` rows whose lease
/// (`updated_at`) is older than `lease_timeout_secs` (a dead builder's claims
/// must come back). Claimed rows are returned already flipped to `Building`
/// with `builder_node = node` and a fresh `updated_at`.
pub async fn claim_pending(
    node: &str,
    limit: usize,
    lease_timeout_secs: u64,
) -> Result<Vec<SegmentMeta>> {
    if node.is_empty() {
        return Err(Error::Message(
            "[WAL_SEGMENTS] claim_pending: empty builder node".to_string(),
        ));
    }
    if limit == 0 {
        return Ok(Vec::new());
    }
    let limit = i64::try_from(limit).unwrap_or(i64::MAX);
    let now = now_micros();
    let stale_before = now.saturating_sub(secs_to_micros(lease_timeout_secs));
    if use_postgres() {
        postgres::claim_pending(node, limit, now, stale_before).await
    } else {
        sqlite::claim_pending(node, limit, now, stale_before).await
    }
}

/// Lease heartbeat for claimed segments — MUST be called from claim time,
/// not from build-start (the compactor heartbeat-gap lesson). Only touches
/// rows still `Building` under `node`; silently skips rows whose lease was
/// already lost (mark_built is where lease loss is detected and acted on).
pub async fn heartbeat(ids: &[i64], node: &str) -> Result<()> {
    if node.is_empty() {
        return Err(Error::Message(
            "[WAL_SEGMENTS] heartbeat: empty builder node".to_string(),
        ));
    }
    if ids.is_empty() {
        return Ok(());
    }
    if use_postgres() {
        postgres::heartbeat(ids, node).await
    } else {
        sqlite::heartbeat(ids, node).await
    }
}

/// Fenced immediate release: `Building` -> `Pending` for rows this node
/// still owns, so a failed batch retries at claim cadence instead of
/// stalling out its full lease (a deterministic build failure otherwise
/// loops at ~lease intervals — the 2026-07-31 OOM drain stall).
/// `l0_planned` is deliberately left untouched: a failure between upload
/// and the registration txn keeps its planned keys visible; if the retry
/// plans a DIFFERENT set before dying old-enough for GC, the first
/// attempt's objects join the accepted crashed-build orphan class.
pub async fn release_claims(ids: &[i64], node: &str) -> Result<u64> {
    if node.is_empty() {
        return Err(Error::Message(
            "[WAL_SEGMENTS] release_claims: empty builder node".to_string(),
        ));
    }
    if ids.is_empty() {
        return Ok(0);
    }
    if use_postgres() {
        postgres::release_claims(ids, node).await
    } else {
        sqlite::release_claims(ids, node).await
    }
}

/// Fenced completion: flips `Building` -> `Built` only WHERE
/// `builder_node = node AND status = Building`. Returns the number of rows
/// actually flipped — a caller seeing fewer rows than requested lost its
/// lease and must DISCARD its outputs' registration (keys are deterministic;
/// the new lease holder re-produces identical files). Flipped rows also get
/// `l0_planned` cleared — a Built row's lifecycle is owned by the normal
/// sweeper, never by the crash GC.
pub async fn mark_built(ids: &[i64], node: &str) -> Result<u64> {
    if node.is_empty() {
        return Err(Error::Message(
            "[WAL_SEGMENTS] mark_built: empty builder node".to_string(),
        ));
    }
    if ids.is_empty() {
        return Ok(0);
    }
    if use_postgres() {
        postgres::mark_built(ids, node).await
    } else {
        sqlite::mark_built(ids, node).await
    }
}

/// Fenced completion WITH the produced L0 files, committed atomically:
/// `file_list` registration and the `Building` -> `Built` flip are ONE
/// transaction, so the "builder crash between batch_add and mark_built"
/// double-count residual cannot occur. Fencing matches [`mark_built`]: rows
/// flip only WHERE `builder_node = node AND status = Building`, and when
/// fewer than `ids.len()` rows match, the WHOLE transaction rolls back —
/// ZERO file rows registered — and the short count returns; the caller must
/// discard its build (the lease winner registers its own files under its
/// own exact-run keys). A duplicate file key is a hard error, never
/// tolerated: keys are a pure function of the decoded run ids, so an
/// existing row means a COMMITTED transaction already flipped those very
/// segments Built — a fence-lost builder finishing after the winner (its
/// flip matches 0 rows and everything rolls back anyway), or a real bug.
pub async fn mark_built_with_files(ids: &[i64], node: &str, files: Vec<FileKey>) -> Result<u64> {
    if node.is_empty() {
        return Err(Error::Message(
            "[WAL_SEGMENTS] mark_built_with_files: empty builder node".to_string(),
        ));
    }
    if ids.is_empty() {
        // files without fencing ids must never register: nothing would ever
        // flip Built, and the rows could double-register on retry
        if !files.is_empty() {
            return Err(Error::Message(format!(
                "[WAL_SEGMENTS] mark_built_with_files node={node}: {} files with no segment ids",
                files.len()
            )));
        }
        return Ok(0);
    }
    if use_postgres() {
        postgres::mark_built_with_files(ids, node, &files).await
    } else {
        sqlite::mark_built_with_files(ids, node, &files).await
    }
}

/// Record the batch's deterministic L0 object keys on its claimed rows
/// BEFORE anything is uploaded — the crash-GC marker (DESIGN-SEGMENT-WAL.md
/// GC). Fenced exactly like [`mark_built`]: only rows still `Building` under
/// `node` are written, and a return smaller than `ids.len()` means the lease
/// was lost — the caller must DISCARD the build without uploading a single
/// object (nothing durable names those keys yet, so nothing would ever
/// collect them). The completion flip clears the marker in the same
/// statement; a non-empty `l0_planned` on a long-dead row is therefore
/// always a crashed build's evidence.
pub async fn set_l0_planned(ids: &[i64], node: &str, keys: &[String]) -> Result<u64> {
    if node.is_empty() {
        return Err(Error::Message(
            "[WAL_SEGMENTS] set_l0_planned: empty builder node".to_string(),
        ));
    }
    if keys.is_empty() {
        // an empty plan would mark the rows GC-visible with nothing to
        // collect; the caller must simply skip the write
        return Err(Error::Message(format!(
            "[WAL_SEGMENTS] set_l0_planned node={node}: empty key list for ids {ids:?}"
        )));
    }
    if ids.is_empty() {
        return Err(Error::Message(format!(
            "[WAL_SEGMENTS] set_l0_planned node={node}: {} keys with no segment ids",
            keys.len()
        )));
    }
    let planned_json = serde_json::to_string(keys).map_err(|e| {
        Error::Message(format!(
            "[WAL_SEGMENTS] set_l0_planned node={node}: cannot serialize keys: {e}"
        ))
    })?;
    if use_postgres() {
        postgres::set_l0_planned(ids, node, &planned_json).await
    } else {
        sqlite::set_l0_planned(ids, node, &planned_json).await
    }
}

/// GC input: rows that are NOT `Built`, still carry a non-empty `l0_planned`,
/// and whose lease timestamp (`updated_at`) is older than `min_age_secs` —
/// i.e. dead long enough that no live builder (which heartbeats every
/// lease/3) can be holding them. Returns `(id, l0_planned JSON)` pairs,
/// oldest first, at most `limit`. Rows younger than the threshold are never
/// returned — the caller must never touch a potentially-live claim.
pub async fn list_l0_orphan_rows(min_age_secs: u64, limit: usize) -> Result<Vec<(i64, String)>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let limit = i64::try_from(limit).unwrap_or(i64::MAX);
    let cutoff = now_micros().saturating_sub(secs_to_micros(min_age_secs));
    if use_postgres() {
        postgres::list_l0_orphan_rows(cutoff, limit).await
    } else {
        sqlite::list_l0_orphan_rows(cutoff, limit).await
    }
}

/// Fence one L0-orphan row for GC processing: stamps `builder_node =
/// gc_node` and refreshes `updated_at`, but ONLY while the row still looks
/// dead (`status != Built`, `l0_planned` non-empty, `updated_at` older than
/// `min_age_secs`). Returns whether the row was armed.
///
/// The stamp is what makes the subsequent object deletes safe against the
/// two writers that could otherwise race them:
/// - a ZOMBIE builder (claimed long ago, still running) committing its fenced
///   `mark_built_with_files` mid-GC — after arming, its flip matches 0 rows and the whole
///   registration rolls back, the designed lease-loss path;
/// - a FRESH re-claim: `claim_pending` only takes `Building` rows whose `updated_at` is stale, and
///   arming just refreshed it, so the row is unclaimable for a full lease — orders of magnitude
///   longer than one row's GC processing.
///
/// `false` means the row changed since it was listed (built, re-claimed, or
/// armed by a concurrent GC) — a live owner exists and the caller must skip
/// it.
pub async fn gc_arm_l0_orphan(id: i64, gc_node: &str, min_age_secs: u64) -> Result<bool> {
    if gc_node.is_empty() {
        return Err(Error::Message(
            "[WAL_SEGMENTS] gc_arm_l0_orphan: empty gc node".to_string(),
        ));
    }
    let now = now_micros();
    let cutoff = now.saturating_sub(secs_to_micros(min_age_secs));
    if use_postgres() {
        postgres::gc_arm_l0_orphan(id, gc_node, now, cutoff).await
    } else {
        sqlite::gc_arm_l0_orphan(id, gc_node, now, cutoff).await
    }
}

/// Reset one row's `l0_planned` to `''` after its keys were processed, so
/// re-claims start clean. Plain (unfenced) UPDATE by design: the caller just
/// armed the row via [`gc_arm_l0_orphan`], so no builder holds it, and the
/// row is long-dead.
pub async fn clear_l0_planned(id: i64) -> Result<()> {
    if use_postgres() {
        postgres::clear_l0_planned(id).await
    } else {
        sqlite::clear_l0_planned(id).await
    }
}

/// Every `object_key` under `prefix` — the anti-join input for the GC of
/// segment objects that were PUT but never registered. Deliberately
/// UNBOUNDED: rows retire within ~`ZO_SEGMENT_RETAIN_SECS` (1h) of their
/// build, so the live population is flush-rate x that window (thousands of
/// short strings, not millions), and a truncated key set would be the bug —
/// any registered object missing from it would look orphaned and get
/// DELETED. The prefix is matched with LIKE: `%` is rejected, and a `_`
/// (present in [`SEGMENT_KEY_PREFIX`] itself) is left as the single-char
/// wildcard — it can only WIDEN the returned set, and extra keys in an
/// anti-join input can only PREVENT deletions, never cause one.
pub async fn object_keys_under(prefix: &str) -> Result<Vec<String>> {
    if prefix.is_empty() || prefix.contains('%') {
        return Err(Error::Message(format!(
            "[WAL_SEGMENTS] object_keys_under: prefix {prefix:?} must be non-empty without a '%' wildcard"
        )));
    }
    let pattern = format!("{prefix}%");
    if use_postgres() {
        postgres::object_keys_under(&pattern).await
    } else {
        sqlite::object_keys_under(&pattern).await
    }
}

/// Segments overlapping the time range for the stream that are not yet built,
/// PLUS segments built at-or-after `include_built_after_micros` (the querier
/// passes its snapshot time minus a grace so segments built moments ago are
/// still visible — see DESIGN-SEGMENT-WAL.md dup/gap rules). Ordered by
/// `min_ts`, then id; at most `limit` rows (callers pass one more than their
/// own cap so an over-limit backlog stays detectable).
pub async fn query_unbuilt(
    org: &str,
    stream_type: &str,
    stream: &str,
    time_range: (i64, i64),
    include_built_after_micros: i64,
    limit: usize,
) -> Result<Vec<SegmentMeta>> {
    let (start, end) = time_range;
    if start > end {
        return Err(Error::Message(format!(
            "[WAL_SEGMENTS] query_unbuilt {org}/{stream_type}/{stream}: invalid time range ({start}, {end})"
        )));
    }
    if limit == 0 {
        return Ok(Vec::new());
    }
    let limit = i64::try_from(limit).unwrap_or(i64::MAX);
    let pattern = stream_like_pattern(org, stream_type, stream)?;
    if use_postgres() {
        postgres::query_unbuilt(start, end, &pattern, include_built_after_micros, limit).await
    } else {
        sqlite::query_unbuilt(start, end, &pattern, include_built_after_micros, limit).await
    }
}

/// Built segments whose lease timestamp (`updated_at`, i.e. build-completion
/// time) is older than `built_before_secs` — sweeper input, oldest first.
pub async fn list_expired(built_before_secs: u64, limit: usize) -> Result<Vec<SegmentMeta>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let limit = i64::try_from(limit).unwrap_or(i64::MAX);
    let cutoff = now_micros().saturating_sub(secs_to_micros(built_before_secs));
    if use_postgres() {
        postgres::list_expired(cutoff, limit).await
    } else {
        sqlite::list_expired(cutoff, limit).await
    }
}

/// Remove rows for segments whose objects were CONFIRMED deleted.
pub async fn delete(ids: &[i64]) -> Result<()> {
    if ids.is_empty() {
        return Ok(());
    }
    if use_postgres() {
        postgres::delete(ids).await
    } else {
        sqlite::delete(ids).await
    }
}

/// Fetch segments by id — follower-side resolution of leader-assigned
/// segment ids. Missing ids are simply absent from the result; the caller
/// decides whether that is an error.
pub async fn get_by_ids(ids: &[i64]) -> Result<Vec<SegmentMeta>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    if use_postgres() {
        postgres::get_by_ids(ids).await
    } else {
        sqlite::get_by_ids(ids).await
    }
}

/// Backlog gauge input: segments not yet built whose registration is older
/// than `age_secs`. A growing count means builders are down or behind —
/// exactly the silent-mover-backlog failure class this design replaces.
pub async fn count_unbuilt_older_than(age_secs: u64) -> Result<i64> {
    let cutoff = now_micros().saturating_sub(secs_to_micros(age_secs));
    if use_postgres() {
        postgres::count_unbuilt_older_than(cutoff).await
    } else {
        sqlite::count_unbuilt_older_than(cutoff).await
    }
}

/// Claim-gate input: `(count, oldest created_at)` over the segments a
/// [`claim_pending`] call would consider RIGHT NOW — `Pending` rows plus
/// `Building` rows whose lease (`updated_at`) is older than
/// `lease_timeout_secs` — using the exact claim predicate so the gate and
/// the claim can never disagree about what is claimable. `(0, 0)` when
/// nothing is claimable.
pub async fn claimable_stats(lease_timeout_secs: u64) -> Result<(i64, i64)> {
    let stale_before = now_micros().saturating_sub(secs_to_micros(lease_timeout_secs));
    if use_postgres() {
        postgres::claimable_stats(stale_before).await
    } else {
        sqlite::claimable_stats(stale_before).await
    }
}

mod postgres {
    use config::metrics::DB_QUERY_NUMS;

    use super::*;
    use crate::db::{
        IndexStatement,
        postgres::{CLIENT, CLIENT_DDL, CLIENT_RO, add_column, create_index},
    };

    pub(super) async fn create_table() -> Result<()> {
        let pool = CLIENT_DDL.clone();
        DB_QUERY_NUMS.with_label_values(&["create", TABLE]).inc();
        sqlx::query(
            r#"
CREATE TABLE IF NOT EXISTS wal_segments
(
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    node_uuid    VARCHAR(64)  not null,
    seq          BIGINT       not null,
    object_key   VARCHAR(512) not null,
    min_ts       BIGINT       not null,
    max_ts       BIGINT       not null,
    size         BIGINT       not null,
    streams      TEXT         not null,
    status       SMALLINT     default 0 not null,
    builder_node VARCHAR(64)  default '' not null,
    l0_planned   TEXT         default '' not null,
    created_at   BIGINT       not null,
    updated_at   BIGINT       not null
);
        "#,
        )
        .execute(&pool)
        .await
        .map_err(|e| Error::Message(format!("[WAL_SEGMENTS] create table failed: {e}")))?;

        // bring PRE-EXISTING tables (created before the GC design added the
        // planned-keys marker) up to date; ADD COLUMN IF NOT EXISTS makes
        // this idempotent on every boot
        add_column(TABLE, "l0_planned", "TEXT NOT NULL DEFAULT ''")
            .await
            .map_err(|e| {
                Error::Message(format!("[WAL_SEGMENTS] add l0_planned column failed: {e}"))
            })?;

        create_indexes().await
    }

    async fn create_indexes() -> Result<()> {
        let indices: Vec<(&str, bool, &[&str])> = vec![
            ("wal_segments_node_seq_idx", true, &["node_uuid", "seq"]),
            ("wal_segments_object_key_idx", true, &["object_key"]),
            (
                "wal_segments_status_created_at_idx",
                false,
                &["status", "created_at"],
            ),
            ("wal_segments_max_ts_idx", false, &["max_ts"]),
        ];
        for (idx, unique, fields) in indices {
            create_index(IndexStatement::new(idx, TABLE, unique, fields)).await?;
        }
        Ok(())
    }

    pub(super) async fn add(meta: &SegmentMeta, streams_json: &str) -> Result<i64> {
        let pool = CLIENT.clone();
        let now = now_micros();
        DB_QUERY_NUMS.with_label_values(&["insert", TABLE]).inc();
        let inserted: Option<i64> = sqlx::query_scalar(
            r#"INSERT INTO wal_segments
    (node_uuid, seq, object_key, min_ts, max_ts, size, streams, status, builder_node, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, '', $9, $10)
ON CONFLICT (node_uuid, seq) DO NOTHING
RETURNING id;"#,
        )
        .bind(&meta.node_uuid)
        .bind(meta.seq)
        .bind(&meta.object_key)
        .bind(meta.min_ts)
        .bind(meta.max_ts)
        .bind(meta.size)
        .bind(streams_json)
        .bind(SegmentStatus::Pending as i16)
        .bind(now)
        .bind(now)
        .fetch_optional(&pool)
        .await
        .map_err(|e| {
            Error::Message(format!(
                "[WAL_SEGMENTS] add object_key={} node_uuid={} seq={}: {e}",
                meta.object_key, meta.node_uuid, meta.seq
            ))
        })?;
        if let Some(id) = inserted {
            return Ok(id);
        }
        // conflicted: idempotent retry after a crash between PUT and register —
        // return the existing row's id
        DB_QUERY_NUMS.with_label_values(&["select", TABLE]).inc();
        let existing: Option<i64> =
            sqlx::query_scalar("SELECT id FROM wal_segments WHERE node_uuid = $1 AND seq = $2;")
                .bind(&meta.node_uuid)
                .bind(meta.seq)
                .fetch_optional(&pool)
                .await
                .map_err(|e| {
                    Error::Message(format!(
                        "[WAL_SEGMENTS] add object_key={}: post-conflict lookup (node_uuid={}, seq={}) failed: {e}",
                        meta.object_key, meta.node_uuid, meta.seq
                    ))
                })?;
        existing.ok_or_else(|| {
            Error::Message(format!(
                "[WAL_SEGMENTS] add object_key={}: conflict on (node_uuid={}, seq={}) but the existing row disappeared (concurrent delete?)",
                meta.object_key, meta.node_uuid, meta.seq
            ))
        })
    }

    pub(super) async fn claim_pending(
        node: &str,
        limit: i64,
        now: i64,
        stale_before: i64,
    ) -> Result<Vec<SegmentMeta>> {
        let pool = CLIENT.clone();
        DB_QUERY_NUMS.with_label_values(&["update", TABLE]).inc();
        // FOR UPDATE SKIP LOCKED makes concurrent claimers partition the
        // backlog instead of blocking or double-claiming.
        let mut rows: Vec<SegmentRow> = sqlx::query_as(
            r#"UPDATE wal_segments
SET status = $1, builder_node = $2, updated_at = $3
WHERE id IN (
    SELECT id FROM wal_segments
    WHERE status = $4 OR (status = $1 AND updated_at < $5)
    ORDER BY created_at DESC, id DESC
    LIMIT $6
    FOR UPDATE SKIP LOCKED
)
RETURNING *;"#,
        )
        .bind(SegmentStatus::Building as i16)
        .bind(node)
        .bind(now)
        .bind(SegmentStatus::Pending as i16)
        .bind(stale_before)
        .bind(limit)
        .fetch_all(&pool)
        .await
        .map_err(|e| Error::Message(format!("[WAL_SEGMENTS] claim_pending node={node}: {e}")))?;
        // RETURNING order is unspecified — restore newest-first (the claim
        // order: fresh segments build first so recent windows recover first
        // under backlog, the compaction fast_mode lesson; the stale-lease
        // reclaim clause keeps old segments reachable)
        rows.sort_unstable_by_key(|r| (std::cmp::Reverse(r.created_at), std::cmp::Reverse(r.id)));
        rows_into_metas(rows)
    }

    pub(super) async fn heartbeat(ids: &[i64], node: &str) -> Result<()> {
        let pool = CLIENT.clone();
        DB_QUERY_NUMS.with_label_values(&["update", TABLE]).inc();
        let sql = format!(
            "UPDATE wal_segments SET updated_at = $1 WHERE builder_node = $2 AND status = $3 AND id IN ({});",
            ids_csv(ids)
        );
        sqlx::query(&sql)
            .bind(now_micros())
            .bind(node)
            .bind(SegmentStatus::Building as i16)
            .execute(&pool)
            .await
            .map_err(|e| {
                Error::Message(format!(
                    "[WAL_SEGMENTS] heartbeat node={node} ids={ids:?}: {e}"
                ))
            })?;
        Ok(())
    }

    pub(super) async fn release_claims(ids: &[i64], node: &str) -> Result<u64> {
        let pool = CLIENT.clone();
        DB_QUERY_NUMS.with_label_values(&["update", TABLE]).inc();
        let sql = format!(
            "UPDATE wal_segments SET status = $1, builder_node = '', updated_at = $2 WHERE builder_node = $3 AND status = $4 AND id IN ({});",
            ids_csv(ids)
        );
        let ret = sqlx::query(&sql)
            .bind(SegmentStatus::Pending as i16)
            .bind(now_micros())
            .bind(node)
            .bind(SegmentStatus::Building as i16)
            .execute(&pool)
            .await
            .map_err(|e| {
                Error::Message(format!(
                    "[WAL_SEGMENTS] release_claims node={node} ids={ids:?}: {e}"
                ))
            })?;
        Ok(ret.rows_affected())
    }

    pub(super) async fn mark_built(ids: &[i64], node: &str) -> Result<u64> {
        let pool = CLIENT.clone();
        DB_QUERY_NUMS.with_label_values(&["update", TABLE]).inc();
        let sql = format!(
            "UPDATE wal_segments SET status = $1, updated_at = $2, l0_planned = '' WHERE builder_node = $3 AND status = $4 AND id IN ({});",
            ids_csv(ids)
        );
        let ret = sqlx::query(&sql)
            .bind(SegmentStatus::Built as i16)
            .bind(now_micros())
            .bind(node)
            .bind(SegmentStatus::Building as i16)
            .execute(&pool)
            .await
            .map_err(|e| {
                Error::Message(format!(
                    "[WAL_SEGMENTS] mark_built node={node} ids={ids:?}: {e}"
                ))
            })?;
        Ok(ret.rows_affected())
    }

    /// One fenced transaction: file_list INSERTs (single-source via
    /// `file_list::postgres::batch_add_with_tx`) then the fenced flip; a
    /// short flip rolls EVERYTHING back and returns the count.
    pub(super) async fn mark_built_with_files(
        ids: &[i64],
        node: &str,
        files: &[FileKey],
    ) -> Result<u64> {
        // pre-SQL gate + partition DDL, both BEFORE the transaction opens
        let add_rows = crate::file_list::prepare_batch_add(files)?;
        let pool = CLIENT.clone();
        crate::file_list::postgres::ensure_batch_add_partitions(&pool, "file_list", &add_rows)
            .await?;

        let mut tx = pool.begin().await.map_err(|e| {
            Error::Message(format!(
                "[WAL_SEGMENTS] mark_built_with_files node={node}: begin failed: {e}"
            ))
        })?;
        if let Err(e) =
            crate::file_list::postgres::batch_add_with_tx(&mut tx, "file_list", &add_rows).await
        {
            if let Err(e) = tx.rollback().await {
                log::error!("[WAL_SEGMENTS] rollback mark_built_with_files insert error: {e}");
            }
            return Err(e);
        }

        DB_QUERY_NUMS.with_label_values(&["update", TABLE]).inc();
        // the flip also clears l0_planned: a committed registration ends the
        // crash-GC's interest in these rows (same statement, same fence)
        let sql = format!(
            "UPDATE wal_segments SET status = $1, updated_at = $2, l0_planned = '' WHERE builder_node = $3 AND status = $4 AND id IN ({});",
            ids_csv(ids)
        );
        let ret = match sqlx::query(&sql)
            .bind(SegmentStatus::Built as i16)
            .bind(now_micros())
            .bind(node)
            .bind(SegmentStatus::Building as i16)
            .execute(&mut *tx)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                if let Err(e) = tx.rollback().await {
                    log::error!("[WAL_SEGMENTS] rollback mark_built_with_files flip error: {e}");
                }
                return Err(Error::Message(format!(
                    "[WAL_SEGMENTS] mark_built_with_files node={node} ids={ids:?}: {e}"
                )));
            }
        };
        let flipped = ret.rows_affected();
        if flipped != ids.len() as u64 {
            // lease lost on >= 1 id: nothing may commit — file rows for
            // partially flipped ids would double-count once the unflipped
            // ones are re-claimed and re-registered
            tx.rollback().await.map_err(|e| {
                Error::Message(format!(
                    "[WAL_SEGMENTS] mark_built_with_files node={node}: rollback after short flip ({flipped} of {}) failed: {e}",
                    ids.len()
                ))
            })?;
            return Ok(flipped);
        }
        tx.commit().await.map_err(|e| {
            Error::Message(format!(
                "[WAL_SEGMENTS] mark_built_with_files node={node}: commit failed: {e}"
            ))
        })?;
        Ok(flipped)
    }

    pub(super) async fn set_l0_planned(ids: &[i64], node: &str, planned_json: &str) -> Result<u64> {
        let pool = CLIENT.clone();
        DB_QUERY_NUMS.with_label_values(&["update", TABLE]).inc();
        let sql = format!(
            "UPDATE wal_segments SET l0_planned = $1 WHERE builder_node = $2 AND status = $3 AND id IN ({});",
            ids_csv(ids)
        );
        let ret = sqlx::query(&sql)
            .bind(planned_json)
            .bind(node)
            .bind(SegmentStatus::Building as i16)
            .execute(&pool)
            .await
            .map_err(|e| {
                Error::Message(format!(
                    "[WAL_SEGMENTS] set_l0_planned node={node} ids={ids:?}: {e}"
                ))
            })?;
        Ok(ret.rows_affected())
    }

    pub(super) async fn list_l0_orphan_rows(cutoff: i64, limit: i64) -> Result<Vec<(i64, String)>> {
        let pool = CLIENT_RO.clone();
        DB_QUERY_NUMS.with_label_values(&["select", TABLE]).inc();
        sqlx::query_as(
            r#"SELECT id, l0_planned FROM wal_segments
WHERE status != $1 AND l0_planned != '' AND updated_at < $2
ORDER BY updated_at ASC, id ASC
LIMIT $3;"#,
        )
        .bind(SegmentStatus::Built as i16)
        .bind(cutoff)
        .bind(limit)
        .fetch_all(&pool)
        .await
        .map_err(|e| {
            Error::Message(format!(
                "[WAL_SEGMENTS] list_l0_orphan_rows cutoff={cutoff}: {e}"
            ))
        })
    }

    pub(super) async fn gc_arm_l0_orphan(
        id: i64,
        gc_node: &str,
        now: i64,
        cutoff: i64,
    ) -> Result<bool> {
        let pool = CLIENT.clone();
        DB_QUERY_NUMS.with_label_values(&["update", TABLE]).inc();
        let ret = sqlx::query(
            r#"UPDATE wal_segments SET builder_node = $1, updated_at = $2
WHERE id = $3 AND status != $4 AND l0_planned != '' AND updated_at < $5;"#,
        )
        .bind(gc_node)
        .bind(now)
        .bind(id)
        .bind(SegmentStatus::Built as i16)
        .bind(cutoff)
        .execute(&pool)
        .await
        .map_err(|e| {
            Error::Message(format!(
                "[WAL_SEGMENTS] gc_arm_l0_orphan id={id} gc_node={gc_node}: {e}"
            ))
        })?;
        Ok(ret.rows_affected() > 0)
    }

    pub(super) async fn clear_l0_planned(id: i64) -> Result<()> {
        let pool = CLIENT.clone();
        DB_QUERY_NUMS.with_label_values(&["update", TABLE]).inc();
        sqlx::query("UPDATE wal_segments SET l0_planned = '' WHERE id = $1;")
            .bind(id)
            .execute(&pool)
            .await
            .map_err(|e| Error::Message(format!("[WAL_SEGMENTS] clear_l0_planned id={id}: {e}")))?;
        Ok(())
    }

    pub(super) async fn object_keys_under(pattern: &str) -> Result<Vec<String>> {
        let pool = CLIENT_RO.clone();
        DB_QUERY_NUMS.with_label_values(&["select", TABLE]).inc();
        let keys: Vec<(String,)> =
            sqlx::query_as("SELECT object_key FROM wal_segments WHERE object_key LIKE $1;")
                .bind(pattern)
                .fetch_all(&pool)
                .await
                .map_err(|e| {
                    Error::Message(format!(
                        "[WAL_SEGMENTS] object_keys_under pattern={pattern}: {e}"
                    ))
                })?;
        Ok(keys.into_iter().map(|(k,)| k).collect())
    }

    pub(super) async fn query_unbuilt(
        start: i64,
        end: i64,
        pattern: &str,
        include_built_after: i64,
        limit: i64,
    ) -> Result<Vec<SegmentMeta>> {
        let pool = CLIENT_RO.clone();
        DB_QUERY_NUMS.with_label_values(&["select", TABLE]).inc();
        let rows: Vec<SegmentRow> = sqlx::query_as(
            r#"SELECT * FROM wal_segments
WHERE max_ts >= $1 AND min_ts <= $2 AND streams LIKE $3
  AND (status != $4 OR (status = $4 AND updated_at >= $5))
ORDER BY min_ts ASC, id ASC
LIMIT $6;"#,
        )
        .bind(start)
        .bind(end)
        .bind(pattern)
        .bind(SegmentStatus::Built as i16)
        .bind(include_built_after)
        .bind(limit)
        .fetch_all(&pool)
        .await
        .map_err(|e| {
            Error::Message(format!(
                "[WAL_SEGMENTS] query_unbuilt pattern={pattern} range=({start}, {end}): {e}"
            ))
        })?;
        rows_into_metas(rows)
    }

    pub(super) async fn list_expired(cutoff: i64, limit: i64) -> Result<Vec<SegmentMeta>> {
        let pool = CLIENT_RO.clone();
        DB_QUERY_NUMS.with_label_values(&["select", TABLE]).inc();
        let rows: Vec<SegmentRow> = sqlx::query_as(
            r#"SELECT * FROM wal_segments
WHERE status = $1 AND updated_at < $2
ORDER BY updated_at ASC, id ASC
LIMIT $3;"#,
        )
        .bind(SegmentStatus::Built as i16)
        .bind(cutoff)
        .bind(limit)
        .fetch_all(&pool)
        .await
        .map_err(|e| Error::Message(format!("[WAL_SEGMENTS] list_expired cutoff={cutoff}: {e}")))?;
        rows_into_metas(rows)
    }

    pub(super) async fn delete(ids: &[i64]) -> Result<()> {
        let pool = CLIENT.clone();
        DB_QUERY_NUMS.with_label_values(&["delete", TABLE]).inc();
        let sql = format!("DELETE FROM wal_segments WHERE id IN ({});", ids_csv(ids));
        sqlx::query(&sql)
            .execute(&pool)
            .await
            .map_err(|e| Error::Message(format!("[WAL_SEGMENTS] delete ids={ids:?}: {e}")))?;
        Ok(())
    }

    pub(super) async fn get_by_ids(ids: &[i64]) -> Result<Vec<SegmentMeta>> {
        let pool = CLIENT_RO.clone();
        DB_QUERY_NUMS.with_label_values(&["select", TABLE]).inc();
        let sql = format!(
            "SELECT * FROM wal_segments WHERE id IN ({}) ORDER BY min_ts ASC, id ASC;",
            ids_csv(ids)
        );
        let rows: Vec<SegmentRow> = sqlx::query_as(&sql)
            .fetch_all(&pool)
            .await
            .map_err(|e| Error::Message(format!("[WAL_SEGMENTS] get_by_ids ids={ids:?}: {e}")))?;
        rows_into_metas(rows)
    }

    pub(super) async fn count_unbuilt_older_than(cutoff: i64) -> Result<i64> {
        let pool = CLIENT_RO.clone();
        DB_QUERY_NUMS.with_label_values(&["select", TABLE]).inc();
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM wal_segments WHERE status != $1 AND created_at < $2;",
        )
        .bind(SegmentStatus::Built as i16)
        .bind(cutoff)
        .fetch_one(&pool)
        .await
        .map_err(|e| {
            Error::Message(format!(
                "[WAL_SEGMENTS] count_unbuilt_older_than cutoff={cutoff}: {e}"
            ))
        })?;
        Ok(count)
    }

    pub(super) async fn claimable_stats(stale_before: i64) -> Result<(i64, i64)> {
        let pool = CLIENT_RO.clone();
        DB_QUERY_NUMS.with_label_values(&["select", TABLE]).inc();
        let row: (i64, i64) = sqlx::query_as(
            r#"SELECT count(*), coalesce(min(created_at), 0) FROM wal_segments
WHERE status = $1 OR (status = $2 AND updated_at < $3);"#,
        )
        .bind(SegmentStatus::Pending as i16)
        .bind(SegmentStatus::Building as i16)
        .bind(stale_before)
        .fetch_one(&pool)
        .await
        .map_err(|e| Error::Message(format!("[WAL_SEGMENTS] claimable_stats: {e}")))?;
        Ok(row)
    }
}

mod sqlite {
    use super::*;
    use crate::db::{
        IndexStatement,
        sqlite::{CLIENT_RO, CLIENT_RW, add_column, create_index},
    };

    pub(super) async fn create_table() -> Result<()> {
        {
            let client = CLIENT_RW.clone();
            let client = client.lock().await;
            sqlx::query(
                r#"
CREATE TABLE IF NOT EXISTS wal_segments
(
    id           INTEGER not null primary key autoincrement,
    node_uuid    VARCHAR(64)  not null,
    seq          BIGINT       not null,
    object_key   VARCHAR(512) not null,
    min_ts       BIGINT       not null,
    max_ts       BIGINT       not null,
    size         BIGINT       not null,
    streams      TEXT         not null,
    status       SMALLINT     default 0 not null,
    builder_node VARCHAR(64)  default '' not null,
    l0_planned   TEXT         default '' not null,
    created_at   BIGINT       not null,
    updated_at   BIGINT       not null
);
        "#,
            )
            .execute(&*client)
            .await
            .map_err(|e| Error::Message(format!("[WAL_SEGMENTS] create table failed: {e}")))?;
            // bring PRE-EXISTING tables up to date: sqlite has no ADD COLUMN
            // IF NOT EXISTS, so add_column probes `pragma table_info` and
            // ALTERs only when the column is missing (idempotent per boot)
            add_column(&client, TABLE, "l0_planned", "TEXT NOT NULL DEFAULT ''")
                .await
                .map_err(|e| {
                    Error::Message(format!("[WAL_SEGMENTS] add l0_planned column failed: {e}"))
                })?;
            // lock released before create_index (it takes the same lock)
        }

        let indices: Vec<(&str, bool, &[&str])> = vec![
            ("wal_segments_node_seq_idx", true, &["node_uuid", "seq"]),
            ("wal_segments_object_key_idx", true, &["object_key"]),
            (
                "wal_segments_status_created_at_idx",
                false,
                &["status", "created_at"],
            ),
            ("wal_segments_max_ts_idx", false, &["max_ts"]),
        ];
        for (idx, unique, fields) in indices {
            create_index(IndexStatement::new(idx, TABLE, unique, fields)).await?;
        }
        Ok(())
    }

    pub(super) async fn add(meta: &SegmentMeta, streams_json: &str) -> Result<i64> {
        let now = now_micros();
        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        // the RW mutex serializes both statements against every other
        // in-process writer, so insert + lookup is atomic here
        let ret = sqlx::query(
            r#"INSERT INTO wal_segments
    (node_uuid, seq, object_key, min_ts, max_ts, size, streams, status, builder_node, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, '', $9, $10)
ON CONFLICT (node_uuid, seq) DO NOTHING;"#,
        )
        .bind(&meta.node_uuid)
        .bind(meta.seq)
        .bind(&meta.object_key)
        .bind(meta.min_ts)
        .bind(meta.max_ts)
        .bind(meta.size)
        .bind(streams_json)
        .bind(SegmentStatus::Pending as i16)
        .bind(now)
        .bind(now)
        .execute(&*client)
        .await
        .map_err(|e| {
            Error::Message(format!(
                "[WAL_SEGMENTS] add object_key={} node_uuid={} seq={}: {e}",
                meta.object_key, meta.node_uuid, meta.seq
            ))
        })?;
        if ret.rows_affected() > 0 {
            return Ok(ret.last_insert_rowid());
        }
        // conflicted: idempotent retry after a crash between PUT and register —
        // return the existing row's id
        let existing: Option<i64> =
            sqlx::query_scalar("SELECT id FROM wal_segments WHERE node_uuid = $1 AND seq = $2;")
                .bind(&meta.node_uuid)
                .bind(meta.seq)
                .fetch_optional(&*client)
                .await
                .map_err(|e| {
                    Error::Message(format!(
                        "[WAL_SEGMENTS] add object_key={}: post-conflict lookup (node_uuid={}, seq={}) failed: {e}",
                        meta.object_key, meta.node_uuid, meta.seq
                    ))
                })?;
        existing.ok_or_else(|| {
            Error::Message(format!(
                "[WAL_SEGMENTS] add object_key={}: conflict on (node_uuid={}, seq={}) but the existing row disappeared (concurrent delete?)",
                meta.object_key, meta.node_uuid, meta.seq
            ))
        })
    }

    pub(super) async fn claim_pending(
        node: &str,
        limit: i64,
        now: i64,
        stale_before: i64,
    ) -> Result<Vec<SegmentMeta>> {
        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        // SELECT-then-UPDATE inside one transaction: the RW mutex is the
        // single writer, so the pair is atomic against other claimers
        let mut tx = client.begin().await.map_err(|e| {
            Error::Message(format!(
                "[WAL_SEGMENTS] claim_pending node={node}: begin failed: {e}"
            ))
        })?;
        let rows: Vec<SegmentRow> = match sqlx::query_as(
            r#"SELECT * FROM wal_segments
WHERE status = $1 OR (status = $2 AND updated_at < $3)
ORDER BY created_at DESC, id DESC
LIMIT $4;"#,
        )
        .bind(SegmentStatus::Pending as i16)
        .bind(SegmentStatus::Building as i16)
        .bind(stale_before)
        .bind(limit)
        .fetch_all(&mut *tx)
        .await
        {
            Ok(v) => v,
            Err(e) => {
                if let Err(e) = tx.rollback().await {
                    log::error!("[WAL_SEGMENTS] rollback claim_pending select error: {e}");
                }
                return Err(Error::Message(format!(
                    "[WAL_SEGMENTS] claim_pending node={node}: select failed: {e}"
                )));
            }
        };
        if rows.is_empty() {
            if let Err(e) = tx.rollback().await {
                log::error!("[WAL_SEGMENTS] rollback claim_pending empty error: {e}");
            }
            return Ok(Vec::new());
        }
        let csv = ids_csv(&rows.iter().map(|r| r.id).collect::<Vec<_>>());
        let sql = format!(
            "UPDATE wal_segments SET status = $1, builder_node = $2, updated_at = $3 WHERE id IN ({csv});"
        );
        if let Err(e) = sqlx::query(&sql)
            .bind(SegmentStatus::Building as i16)
            .bind(node)
            .bind(now)
            .execute(&mut *tx)
            .await
        {
            if let Err(e) = tx.rollback().await {
                log::error!("[WAL_SEGMENTS] rollback claim_pending update error: {e}");
            }
            return Err(Error::Message(format!(
                "[WAL_SEGMENTS] claim_pending node={node}: update failed: {e}"
            )));
        }
        // re-read the claimed rows so the returned state matches the DB
        let sql = format!(
            "SELECT * FROM wal_segments WHERE id IN ({csv}) ORDER BY created_at DESC, id DESC;"
        );
        let rows: Vec<SegmentRow> = match sqlx::query_as(&sql).fetch_all(&mut *tx).await {
            Ok(v) => v,
            Err(e) => {
                if let Err(e) = tx.rollback().await {
                    log::error!("[WAL_SEGMENTS] rollback claim_pending reselect error: {e}");
                }
                return Err(Error::Message(format!(
                    "[WAL_SEGMENTS] claim_pending node={node}: reselect failed: {e}"
                )));
            }
        };
        if let Err(e) = tx.commit().await {
            return Err(Error::Message(format!(
                "[WAL_SEGMENTS] claim_pending node={node}: commit failed: {e}"
            )));
        }
        rows_into_metas(rows)
    }

    pub(super) async fn heartbeat(ids: &[i64], node: &str) -> Result<()> {
        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        let sql = format!(
            "UPDATE wal_segments SET updated_at = $1 WHERE builder_node = $2 AND status = $3 AND id IN ({});",
            ids_csv(ids)
        );
        sqlx::query(&sql)
            .bind(now_micros())
            .bind(node)
            .bind(SegmentStatus::Building as i16)
            .execute(&*client)
            .await
            .map_err(|e| {
                Error::Message(format!(
                    "[WAL_SEGMENTS] heartbeat node={node} ids={ids:?}: {e}"
                ))
            })?;
        Ok(())
    }

    pub(super) async fn release_claims(ids: &[i64], node: &str) -> Result<u64> {
        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        let sql = format!(
            "UPDATE wal_segments SET status = $1, builder_node = '', updated_at = $2 WHERE builder_node = $3 AND status = $4 AND id IN ({});",
            ids_csv(ids)
        );
        let ret = sqlx::query(&sql)
            .bind(SegmentStatus::Pending as i16)
            .bind(now_micros())
            .bind(node)
            .bind(SegmentStatus::Building as i16)
            .execute(&*client)
            .await
            .map_err(|e| {
                Error::Message(format!(
                    "[WAL_SEGMENTS] release_claims node={node} ids={ids:?}: {e}"
                ))
            })?;
        Ok(ret.rows_affected())
    }

    pub(super) async fn mark_built(ids: &[i64], node: &str) -> Result<u64> {
        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        let sql = format!(
            "UPDATE wal_segments SET status = $1, updated_at = $2, l0_planned = '' WHERE builder_node = $3 AND status = $4 AND id IN ({});",
            ids_csv(ids)
        );
        let ret = sqlx::query(&sql)
            .bind(SegmentStatus::Built as i16)
            .bind(now_micros())
            .bind(node)
            .bind(SegmentStatus::Building as i16)
            .execute(&*client)
            .await
            .map_err(|e| {
                Error::Message(format!(
                    "[WAL_SEGMENTS] mark_built node={node} ids={ids:?}: {e}"
                ))
            })?;
        Ok(ret.rows_affected())
    }

    /// One fenced transaction under the single-writer lock: file_list
    /// INSERTs (single-source via `file_list::sqlite::batch_add_with_tx`)
    /// then the fenced flip; a short flip rolls EVERYTHING back and returns
    /// the count.
    pub(super) async fn mark_built_with_files(
        ids: &[i64],
        node: &str,
        files: &[FileKey],
    ) -> Result<u64> {
        // pre-SQL gate BEFORE the writer lock
        let add_rows = crate::file_list::prepare_batch_add(files)?;
        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        let mut tx = client.begin().await.map_err(|e| {
            Error::Message(format!(
                "[WAL_SEGMENTS] mark_built_with_files node={node}: begin failed: {e}"
            ))
        })?;
        if let Err(e) =
            crate::file_list::sqlite::batch_add_with_tx(&mut tx, "file_list", &add_rows).await
        {
            if let Err(e) = tx.rollback().await {
                log::error!("[WAL_SEGMENTS] rollback mark_built_with_files insert error: {e}");
            }
            return Err(e);
        }

        // the flip also clears l0_planned: a committed registration ends the
        // crash-GC's interest in these rows (same statement, same fence)
        let sql = format!(
            "UPDATE wal_segments SET status = $1, updated_at = $2, l0_planned = '' WHERE builder_node = $3 AND status = $4 AND id IN ({});",
            ids_csv(ids)
        );
        let ret = match sqlx::query(&sql)
            .bind(SegmentStatus::Built as i16)
            .bind(now_micros())
            .bind(node)
            .bind(SegmentStatus::Building as i16)
            .execute(&mut *tx)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                if let Err(e) = tx.rollback().await {
                    log::error!("[WAL_SEGMENTS] rollback mark_built_with_files flip error: {e}");
                }
                return Err(Error::Message(format!(
                    "[WAL_SEGMENTS] mark_built_with_files node={node} ids={ids:?}: {e}"
                )));
            }
        };
        let flipped = ret.rows_affected();
        if flipped != ids.len() as u64 {
            // lease lost on >= 1 id: nothing may commit — file rows for
            // partially flipped ids would double-count once the unflipped
            // ones are re-claimed and re-registered
            tx.rollback().await.map_err(|e| {
                Error::Message(format!(
                    "[WAL_SEGMENTS] mark_built_with_files node={node}: rollback after short flip ({flipped} of {}) failed: {e}",
                    ids.len()
                ))
            })?;
            return Ok(flipped);
        }
        tx.commit().await.map_err(|e| {
            Error::Message(format!(
                "[WAL_SEGMENTS] mark_built_with_files node={node}: commit failed: {e}"
            ))
        })?;
        Ok(flipped)
    }

    pub(super) async fn set_l0_planned(ids: &[i64], node: &str, planned_json: &str) -> Result<u64> {
        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        let sql = format!(
            "UPDATE wal_segments SET l0_planned = $1 WHERE builder_node = $2 AND status = $3 AND id IN ({});",
            ids_csv(ids)
        );
        let ret = sqlx::query(&sql)
            .bind(planned_json)
            .bind(node)
            .bind(SegmentStatus::Building as i16)
            .execute(&*client)
            .await
            .map_err(|e| {
                Error::Message(format!(
                    "[WAL_SEGMENTS] set_l0_planned node={node} ids={ids:?}: {e}"
                ))
            })?;
        Ok(ret.rows_affected())
    }

    pub(super) async fn list_l0_orphan_rows(cutoff: i64, limit: i64) -> Result<Vec<(i64, String)>> {
        let pool = CLIENT_RO.clone();
        sqlx::query_as(
            r#"SELECT id, l0_planned FROM wal_segments
WHERE status != $1 AND l0_planned != '' AND updated_at < $2
ORDER BY updated_at ASC, id ASC
LIMIT $3;"#,
        )
        .bind(SegmentStatus::Built as i16)
        .bind(cutoff)
        .bind(limit)
        .fetch_all(&pool)
        .await
        .map_err(|e| {
            Error::Message(format!(
                "[WAL_SEGMENTS] list_l0_orphan_rows cutoff={cutoff}: {e}"
            ))
        })
    }

    pub(super) async fn gc_arm_l0_orphan(
        id: i64,
        gc_node: &str,
        now: i64,
        cutoff: i64,
    ) -> Result<bool> {
        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        let ret = sqlx::query(
            r#"UPDATE wal_segments SET builder_node = $1, updated_at = $2
WHERE id = $3 AND status != $4 AND l0_planned != '' AND updated_at < $5;"#,
        )
        .bind(gc_node)
        .bind(now)
        .bind(id)
        .bind(SegmentStatus::Built as i16)
        .bind(cutoff)
        .execute(&*client)
        .await
        .map_err(|e| {
            Error::Message(format!(
                "[WAL_SEGMENTS] gc_arm_l0_orphan id={id} gc_node={gc_node}: {e}"
            ))
        })?;
        Ok(ret.rows_affected() > 0)
    }

    pub(super) async fn clear_l0_planned(id: i64) -> Result<()> {
        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        sqlx::query("UPDATE wal_segments SET l0_planned = '' WHERE id = $1;")
            .bind(id)
            .execute(&*client)
            .await
            .map_err(|e| Error::Message(format!("[WAL_SEGMENTS] clear_l0_planned id={id}: {e}")))?;
        Ok(())
    }

    pub(super) async fn object_keys_under(pattern: &str) -> Result<Vec<String>> {
        let pool = CLIENT_RO.clone();
        let keys: Vec<(String,)> =
            sqlx::query_as("SELECT object_key FROM wal_segments WHERE object_key LIKE $1;")
                .bind(pattern)
                .fetch_all(&pool)
                .await
                .map_err(|e| {
                    Error::Message(format!(
                        "[WAL_SEGMENTS] object_keys_under pattern={pattern}: {e}"
                    ))
                })?;
        Ok(keys.into_iter().map(|(k,)| k).collect())
    }

    pub(super) async fn query_unbuilt(
        start: i64,
        end: i64,
        pattern: &str,
        include_built_after: i64,
        limit: i64,
    ) -> Result<Vec<SegmentMeta>> {
        let pool = CLIENT_RO.clone();
        let rows: Vec<SegmentRow> = sqlx::query_as(
            r#"SELECT * FROM wal_segments
WHERE max_ts >= $1 AND min_ts <= $2 AND streams LIKE $3
  AND (status != $4 OR (status = $4 AND updated_at >= $5))
ORDER BY min_ts ASC, id ASC
LIMIT $6;"#,
        )
        .bind(start)
        .bind(end)
        .bind(pattern)
        .bind(SegmentStatus::Built as i16)
        .bind(include_built_after)
        .bind(limit)
        .fetch_all(&pool)
        .await
        .map_err(|e| {
            Error::Message(format!(
                "[WAL_SEGMENTS] query_unbuilt pattern={pattern} range=({start}, {end}): {e}"
            ))
        })?;
        rows_into_metas(rows)
    }

    pub(super) async fn list_expired(cutoff: i64, limit: i64) -> Result<Vec<SegmentMeta>> {
        let pool = CLIENT_RO.clone();
        let rows: Vec<SegmentRow> = sqlx::query_as(
            r#"SELECT * FROM wal_segments
WHERE status = $1 AND updated_at < $2
ORDER BY updated_at ASC, id ASC
LIMIT $3;"#,
        )
        .bind(SegmentStatus::Built as i16)
        .bind(cutoff)
        .bind(limit)
        .fetch_all(&pool)
        .await
        .map_err(|e| Error::Message(format!("[WAL_SEGMENTS] list_expired cutoff={cutoff}: {e}")))?;
        rows_into_metas(rows)
    }

    pub(super) async fn delete(ids: &[i64]) -> Result<()> {
        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        let sql = format!("DELETE FROM wal_segments WHERE id IN ({});", ids_csv(ids));
        sqlx::query(&sql)
            .execute(&*client)
            .await
            .map_err(|e| Error::Message(format!("[WAL_SEGMENTS] delete ids={ids:?}: {e}")))?;
        Ok(())
    }

    pub(super) async fn get_by_ids(ids: &[i64]) -> Result<Vec<SegmentMeta>> {
        let pool = CLIENT_RO.clone();
        let sql = format!(
            "SELECT * FROM wal_segments WHERE id IN ({}) ORDER BY min_ts ASC, id ASC;",
            ids_csv(ids)
        );
        let rows: Vec<SegmentRow> = sqlx::query_as(&sql)
            .fetch_all(&pool)
            .await
            .map_err(|e| Error::Message(format!("[WAL_SEGMENTS] get_by_ids ids={ids:?}: {e}")))?;
        rows_into_metas(rows)
    }

    pub(super) async fn count_unbuilt_older_than(cutoff: i64) -> Result<i64> {
        let pool = CLIENT_RO.clone();
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM wal_segments WHERE status != $1 AND created_at < $2;",
        )
        .bind(SegmentStatus::Built as i16)
        .bind(cutoff)
        .fetch_one(&pool)
        .await
        .map_err(|e| {
            Error::Message(format!(
                "[WAL_SEGMENTS] count_unbuilt_older_than cutoff={cutoff}: {e}"
            ))
        })?;
        Ok(count)
    }

    pub(super) async fn claimable_stats(stale_before: i64) -> Result<(i64, i64)> {
        let pool = CLIENT_RO.clone();
        let row: (i64, i64) = sqlx::query_as(
            r#"SELECT count(*), coalesce(min(created_at), 0) FROM wal_segments
WHERE status = $1 OR (status = $2 AND updated_at < $3);"#,
        )
        .bind(SegmentStatus::Pending as i16)
        .bind(SegmentStatus::Building as i16)
        .bind(stale_before)
        .fetch_one(&pool)
        .await
        .map_err(|e| Error::Message(format!("[WAL_SEGMENTS] claimable_stats: {e}")))?;
        Ok(row)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The whole test module shares one on-disk sqlite file through the
    /// global CLIENT_RW/CLIENT_RO pools (same as the file_list sqlite tests),
    /// so tests serialize on this lock and each wipes the table in setup.
    static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    async fn setup() -> tokio::sync::MutexGuard<'static, ()> {
        let guard = TEST_LOCK.lock().await;
        // mirror db_init: the sqlite file lives under data_db_dir
        std::fs::create_dir_all(&get_config().common.data_db_dir)
            .expect("create data_db_dir for tests");
        create_table().await.expect("create wal_segments table");
        let client = crate::db::sqlite::CLIENT_RW.clone();
        let client = client.lock().await;
        sqlx::query("DELETE FROM wal_segments;")
            .execute(&*client)
            .await
            .expect("wipe wal_segments");
        guard
    }

    fn seg(node: &str, seq: i64, min_ts: i64, max_ts: i64, streams: &[&str]) -> SegmentMeta {
        SegmentMeta {
            id: 0,
            node_uuid: node.to_string(),
            seq,
            object_key: format!("{SEGMENT_KEY_PREFIX}{node}/{seq:020}"),
            min_ts,
            max_ts,
            size: 1024,
            streams: streams.iter().map(|s| s.to_string()).collect(),
            status: SegmentStatus::Pending,
            builder_node: String::new(),
            created_at: 0,
            updated_at: 0,
        }
    }

    async fn raw_exec(sql: &str) {
        let client = crate::db::sqlite::CLIENT_RW.clone();
        let client = client.lock().await;
        sqlx::query(sql)
            .execute(&*client)
            .await
            .unwrap_or_else(|e| panic!("raw exec {sql:?} failed: {e}"));
    }

    async fn force_updated_at(id: i64, updated_at: i64) {
        raw_exec(&format!(
            "UPDATE wal_segments SET updated_at = {updated_at} WHERE id = {id};"
        ))
        .await;
    }

    async fn force_created_at(id: i64, created_at: i64) {
        raw_exec(&format!(
            "UPDATE wal_segments SET created_at = {created_at} WHERE id = {id};"
        ))
        .await;
    }

    /// (status, builder_node, updated_at) straight from the DB.
    async fn raw_row(id: i64) -> (i16, String, i64) {
        let client = crate::db::sqlite::CLIENT_RW.clone();
        let client = client.lock().await;
        sqlx::query_as::<_, (i16, String, i64)>(
            "SELECT status, builder_node, updated_at FROM wal_segments WHERE id = $1;",
        )
        .bind(id)
        .fetch_one(&*client)
        .await
        .unwrap_or_else(|e| panic!("raw row {id} failed: {e}"))
    }

    async fn raw_count() -> i64 {
        let client = crate::db::sqlite::CLIENT_RW.clone();
        let client = client.lock().await;
        sqlx::query_scalar("SELECT count(*) FROM wal_segments;")
            .fetch_one(&*client)
            .await
            .expect("raw count failed")
    }

    const T0: i64 = 1_700_000_000_000_000; // arbitrary healthy micros base

    #[tokio::test]
    async fn test_add_is_idempotent_on_node_seq() {
        let _guard = setup().await;
        let meta = seg("node-add", 1, T0, T0 + 1000, &["org1/logs/app1"]);
        let id1 = add(&meta).await.unwrap();
        assert!(id1 > 0, "first add must create the row");

        // exact retry (crash between PUT and register) returns the same id
        let id2 = add(&meta).await.unwrap();
        assert_eq!(
            id2, id1,
            "retrying the same (node_uuid, seq) must not duplicate"
        );
        assert_eq!(raw_count().await, 1);

        // conflict path even when other fields differ: keys are deterministic
        // in prod, but the arbiter is (node_uuid, seq) alone
        let mut retry = meta.clone();
        retry.object_key = format!("{}(retry)", meta.object_key);
        retry.size = 9999;
        let id3 = add(&retry).await.unwrap();
        assert_eq!(
            id3, id1,
            "conflicting add must return the existing row's id"
        );
        assert_eq!(raw_count().await, 1);

        // a different seq is a different segment
        let id4 = add(&seg("node-add", 2, T0, T0 + 1000, &["org1/logs/app1"]))
            .await
            .unwrap();
        assert_ne!(id4, id1);
        assert_eq!(raw_count().await, 2);
    }

    #[tokio::test]
    async fn test_add_rejects_degenerate_input() {
        let _guard = setup().await;

        // empty streams would hide the segment from query_unbuilt forever
        let no_streams = seg("node-bad", 1, T0, T0 + 1000, &[]);
        let err = add(&no_streams).await.unwrap_err();
        assert!(
            err.to_string().contains(&no_streams.object_key),
            "error must name the object_key: {err}"
        );
        // deterministic classification: retry loops must bail, not spin
        assert!(
            matches!(err, Error::InvalidFileMeta(_)),
            "expected InvalidFileMeta, got: {err:?}"
        );
        assert!(err.is_deterministic_db_error());

        // inverted range poisons overlap pruning
        let inverted = seg("node-bad", 2, T0 + 1000, T0, &["org1/logs/app1"]);
        let err = add(&inverted).await.unwrap_err();
        assert!(err.to_string().contains("degenerate time range"), "{err}");
        assert!(matches!(err, Error::InvalidFileMeta(_)), "{err:?}");

        // zero/negative min_ts with real data
        let zero_ts = seg("node-bad", 3, 0, T0, &["org1/logs/app1"]);
        let err = add(&zero_ts).await.unwrap_err();
        assert!(err.is_deterministic_db_error(), "{err:?}");

        let mut no_node = seg("", 4, T0, T0 + 1000, &["org1/logs/app1"]);
        no_node.object_key = "wal_segments/x/4".to_string();
        let err = add(&no_node).await.unwrap_err();
        assert!(err.is_deterministic_db_error(), "{err:?}");

        assert_eq!(raw_count().await, 0, "no degenerate row may be stored");
    }

    #[tokio::test]
    async fn test_claim_leases_newest_first_skips_fresh_reclaims_stale() {
        let _guard = setup().await;
        let id1 = add(&seg("node-c", 1, T0, T0 + 1000, &["o/logs/s"]))
            .await
            .unwrap();
        let id2 = add(&seg("node-c", 2, T0, T0 + 1000, &["o/logs/s"]))
            .await
            .unwrap();
        let id3 = add(&seg("node-c", 3, T0, T0 + 1000, &["o/logs/s"]))
            .await
            .unwrap();
        // deterministic age order: id1 oldest, id3 newest
        force_created_at(id1, T0).await;
        force_created_at(id2, T0 + 10).await;
        force_created_at(id3, T0 + 20).await;

        // limit 0 claims nothing
        assert!(claim_pending("b1", 0, 60).await.unwrap().is_empty());

        // claims the two NEWEST, already flipped to Building for b1 —
        // fresh segments build first so recent windows recover first under
        // backlog (the compaction fast_mode lesson)
        let claimed = claim_pending("b1", 2, 60).await.unwrap();
        assert_eq!(
            claimed.iter().map(|m| m.id).collect::<Vec<_>>(),
            vec![id3, id2],
            "claim must be newest-first"
        );
        for m in &claimed {
            assert_eq!(m.status, SegmentStatus::Building);
            assert_eq!(m.builder_node, "b1");
        }

        // a second claimer only gets the remaining pending row: fresh
        // Building rows are leased and must be skipped
        let claimed2 = claim_pending("b2", 10, 60).await.unwrap();
        assert_eq!(claimed2.iter().map(|m| m.id).collect::<Vec<_>>(), vec![id1]);

        // a stale lease (dead builder) is reclaimable
        force_updated_at(id3, now_micros() - 61_000_000).await;
        let reclaimed = claim_pending("b2", 10, 60).await.unwrap();
        assert_eq!(reclaimed.len(), 1, "only the stale row comes back");
        assert_eq!(reclaimed[0].id, id3);
        assert_eq!(reclaimed[0].builder_node, "b2");
        assert_eq!(reclaimed[0].status, SegmentStatus::Building);

        // id2 is still freshly leased by b1
        let (status, node, _) = raw_row(id2).await;
        assert_eq!((status, node.as_str()), (1, "b1"));

        // empty node is a caller bug, not a silent no-op
        assert!(claim_pending("", 1, 60).await.is_err());
    }

    #[tokio::test]
    async fn test_heartbeat_only_touches_own_building_rows() {
        let _guard = setup().await;
        let id = add(&seg("node-h", 1, T0, T0 + 1000, &["o/logs/s"]))
            .await
            .unwrap();
        let claimed = claim_pending("hb-node", 10, 60).await.unwrap();
        assert_eq!(claimed[0].id, id);

        let old = now_micros() - 30_000_000;
        force_updated_at(id, old).await;

        // wrong node: no effect (fencing is checked at mark_built; heartbeat
        // must simply never touch someone else's lease)
        heartbeat(&[id], "other-node").await.unwrap();
        assert_eq!(raw_row(id).await.2, old);

        // right node refreshes the lease
        heartbeat(&[id], "hb-node").await.unwrap();
        assert!(
            raw_row(id).await.2 > old,
            "heartbeat must refresh updated_at"
        );

        // empty ids is a no-op, empty node an error
        heartbeat(&[], "hb-node").await.unwrap();
        assert!(heartbeat(&[id], "").await.is_err());
    }

    #[tokio::test]
    async fn test_mark_built_is_fenced_by_builder_node() {
        let _guard = setup().await;
        let id1 = add(&seg("node-m", 1, T0, T0 + 1000, &["o/logs/s"]))
            .await
            .unwrap();
        let id2 = add(&seg("node-m", 2, T0, T0 + 1000, &["o/logs/s"]))
            .await
            .unwrap();
        let claimed = claim_pending("b1", 10, 60).await.unwrap();
        assert_eq!(claimed.len(), 2);

        // wrong node flips nothing
        assert_eq!(mark_built(&[id1, id2], "intruder").await.unwrap(), 0);
        assert_eq!(raw_row(id1).await.0, 1, "row must still be Building");

        // b2 steals id2 via an expired lease; b1's mark_built then reports a
        // SHORT count — the caller must treat that as lease-lost and discard
        force_updated_at(id2, now_micros() - 61_000_000).await;
        let stolen = claim_pending("b2", 10, 60).await.unwrap();
        assert_eq!(stolen.iter().map(|m| m.id).collect::<Vec<_>>(), vec![id2]);
        assert_eq!(
            mark_built(&[id1, id2], "b1").await.unwrap(),
            1,
            "short count signals the lost lease on id2"
        );
        assert_eq!(raw_row(id1).await.0, 2, "owned row is Built");
        assert_eq!(raw_row(id2).await.0, 1, "stolen row stays with b2");

        // repeat call flips nothing further (no longer Building under b1)
        assert_eq!(mark_built(&[id1], "b1").await.unwrap(), 0);
        // a never-claimed pending row cannot be built
        let id3 = add(&seg("node-m", 3, T0, T0 + 1000, &["o/logs/s"]))
            .await
            .unwrap();
        assert_eq!(mark_built(&[id3], "b1").await.unwrap(), 0);
        // empty ids short-circuits
        assert_eq!(mark_built(&[], "b1").await.unwrap(), 0);
    }

    // ── mark_built_with_files: registration + flip are one transaction ──

    /// Builder-produced L0 row against a per-run unique org (file_list rows
    /// are shared across the process-global sqlite file and are not wiped).
    fn l0_file(org: &str, name: &str) -> FileKey {
        FileKey::new(
            0,
            String::new(),
            format!("files/{org}/logs/app1/2023/11/14/22/{name}.vix"),
            config::meta::stream::FileMeta {
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

    async fn file_list_setup() {
        crate::file_list::create_table()
            .await
            .expect("create file_list table");
        // the unique (stream, date, file) index is what makes a duplicate
        // key inside the fenced transaction a hard error
        crate::file_list::create_table_index()
            .await
            .expect("create file_list indexes");
    }

    #[tokio::test]
    async fn test_mark_built_with_files_commits_rows_and_flip_together() {
        let _guard = setup().await;
        file_list_setup().await;
        let id1 = add(&seg("node-f", 1, T0, T0 + 1000, &["o/logs/s"]))
            .await
            .unwrap();
        let id2 = add(&seg("node-f", 2, T0, T0 + 1000, &["o/logs/s"]))
            .await
            .unwrap();
        assert_eq!(claim_pending("bf1", 10, 60).await.unwrap().len(), 2);

        let org = format!("torg{}", now_micros());
        let files = vec![
            l0_file(&org, "l0_w_1_2_472190"),
            l0_file(&org, "l0_w_1_2_472191"),
        ];
        let flipped = mark_built_with_files(&[id1, id2], "bf1", files.clone())
            .await
            .unwrap();
        assert_eq!(flipped, 2);
        for file in &files {
            assert!(
                crate::file_list::contains(&file.key).await.unwrap(),
                "{} must be registered by the committed transaction",
                file.key
            );
        }
        assert_eq!(raw_row(id1).await.0, 2, "segment must be Built");
        assert_eq!(raw_row(id2).await.0, 2, "segment must be Built");

        // a later claim re-registering the SAME keys is a hard error (keys
        // are pure functions of the decoded run ids, so this state means a
        // committed build already flipped those segments — a real bug), and
        // the error must roll the flip back too
        let id3 = add(&seg("node-f", 3, T0, T0 + 1000, &["o/logs/s"]))
            .await
            .unwrap();
        assert_eq!(claim_pending("bf1", 10, 60).await.unwrap().len(), 1);
        let err = mark_built_with_files(&[id3], "bf1", files.clone())
            .await
            .unwrap_err();
        assert!(
            err.to_string().to_lowercase().contains("unique"),
            "duplicate key must surface, got: {err}"
        );
        assert_eq!(raw_row(id3).await.0, 1, "failed txn must not flip");

        // empty files with real ids is a plain fenced flip
        assert_eq!(
            mark_built_with_files(&[id3], "bf1", Vec::new())
                .await
                .unwrap(),
            1
        );

        // degenerate metas are rejected before any SQL, deterministically
        let id4 = add(&seg("node-f", 4, T0, T0 + 1000, &["o/logs/s"]))
            .await
            .unwrap();
        assert_eq!(claim_pending("bf1", 10, 60).await.unwrap().len(), 1);
        let mut bad = l0_file(&org, "l0_w_4_4_472190");
        bad.meta.min_ts = 0; // records > 0 with min_ts <= 0
        let err = mark_built_with_files(&[id4], "bf1", vec![bad.clone()])
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidFileMeta(_)), "{err:?}");
        assert!(err.is_deterministic_db_error());
        assert!(!crate::file_list::contains(&bad.key).await.unwrap());
        assert_eq!(raw_row(id4).await.0, 1, "failed txn must not flip");

        // guards: empty node, and files without fencing ids
        assert!(mark_built_with_files(&[id4], "", Vec::new()).await.is_err());
        assert!(
            mark_built_with_files(&[], "bf1", vec![l0_file(&org, "l0_w_9_9_472190")])
                .await
                .is_err()
        );
        assert_eq!(
            mark_built_with_files(&[], "bf1", Vec::new()).await.unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn test_mark_built_with_files_fenced_rollback_registers_nothing() {
        let _guard = setup().await;
        file_list_setup().await;
        let id1 = add(&seg("node-r", 1, T0, T0 + 1000, &["o/logs/s"]))
            .await
            .unwrap();
        let id2 = add(&seg("node-r", 2, T0, T0 + 1000, &["o/logs/s"]))
            .await
            .unwrap();
        assert_eq!(claim_pending("loser", 10, 60).await.unwrap().len(), 2);

        // the winner steals ONLY id2 via an expired lease
        force_updated_at(id2, now_micros() - 61_000_000).await;
        let stolen = claim_pending("winner", 10, 60).await.unwrap();
        assert_eq!(stolen.iter().map(|m| m.id).collect::<Vec<_>>(), vec![id2]);

        // loser finishes late: the short flip (1 of 2) must roll back the
        // WHOLE transaction — zero file rows, and even the owned id1 flip
        let org = format!("torg{}", now_micros());
        let files = vec![
            l0_file(&org, "l0_w_1_2_472195"),
            l0_file(&org, "l0_w_1_2_472196"),
        ];
        let flipped = mark_built_with_files(&[id1, id2], "loser", files.clone())
            .await
            .unwrap();
        assert_eq!(flipped, 1, "short count signals the lost lease");
        for file in &files {
            assert!(
                !crate::file_list::contains(&file.key).await.unwrap(),
                "{} must NOT survive the fenced rollback",
                file.key
            );
        }
        assert_eq!(raw_row(id1).await.0, 1, "owned flip must roll back too");
        assert_eq!(raw_row(id2).await.0, 1, "stolen row stays with winner");
        assert_eq!(raw_row(id2).await.1, "winner");

        // a fully stolen lease flips nothing and registers nothing
        force_updated_at(id1, now_micros() - 61_000_000).await;
        assert_eq!(
            claim_pending("winner", 10, 60)
                .await
                .unwrap()
                .iter()
                .map(|m| m.id)
                .collect::<Vec<_>>(),
            vec![id1]
        );
        let flipped = mark_built_with_files(&[id1, id2], "loser", files.clone())
            .await
            .unwrap();
        assert_eq!(flipped, 0);
        assert!(!crate::file_list::contains(&files[0].key).await.unwrap());

        // the winner commits the same deterministic keys cleanly
        let flipped = mark_built_with_files(&[id1, id2], "winner", files.clone())
            .await
            .unwrap();
        assert_eq!(flipped, 2);
        for file in &files {
            assert!(crate::file_list::contains(&file.key).await.unwrap());
        }
        assert_eq!(raw_row(id1).await.0, 2);
        assert_eq!(raw_row(id2).await.0, 2);
    }

    #[tokio::test]
    async fn test_query_unbuilt_time_range_stream_token_and_built_grace() {
        let _guard = setup().await;
        // A: matches stream + early window; B: near-miss stream token
        // (app10 vs app1); C: matches stream, later window; D: stream is in
        // the MIDDLE of a multi-stream list
        let id_a = add(&seg("node-q", 1, T0 + 1000, T0 + 2000, &["org1/logs/app1"]))
            .await
            .unwrap();
        let _id_b = add(&seg(
            "node-q",
            2,
            T0 + 1000,
            T0 + 2000,
            &["org1/logs/app10"],
        ))
        .await
        .unwrap();
        let id_c = add(&seg("node-q", 3, T0 + 5000, T0 + 6000, &["org1/logs/app1"]))
            .await
            .unwrap();
        let id_d = add(&seg(
            "node-q",
            4,
            T0 + 1500,
            T0 + 2500,
            &["org1/metrics/cpu", "org1/logs/app1", "org1/traces/http"],
        ))
        .await
        .unwrap();

        // time range prunes C; token match must not catch app10
        let hits = query_unbuilt("org1", "logs", "app1", (T0, T0 + 3000), 0, 1000)
            .await
            .unwrap();
        assert_eq!(
            hits.iter().map(|m| m.id).collect::<Vec<_>>(),
            vec![id_a, id_d],
            "expected exact-token, in-range segments ordered by min_ts"
        );

        // wide range picks up C too
        let hits = query_unbuilt("org1", "logs", "app1", (T0, T0 + 10_000), 0, 1000)
            .await
            .unwrap();
        assert_eq!(
            hits.iter().map(|m| m.id).collect::<Vec<_>>(),
            vec![id_a, id_d, id_c]
        );

        // LIMIT truncates after the (min_ts, id) ordering; 0 short-circuits
        let hits = query_unbuilt("org1", "logs", "app1", (T0, T0 + 10_000), 0, 2)
            .await
            .unwrap();
        assert_eq!(
            hits.iter().map(|m| m.id).collect::<Vec<_>>(),
            vec![id_a, id_d],
            "limit must keep the first rows of the same ordering"
        );
        assert!(
            query_unbuilt("org1", "logs", "app1", (T0, T0 + 10_000), 0, 0)
                .await
                .unwrap()
                .is_empty()
        );
        // a limit beyond i64 saturates instead of erroring
        assert_eq!(
            query_unbuilt("org1", "logs", "app1", (T0, T0 + 10_000), 0, usize::MAX)
                .await
                .unwrap()
                .len(),
            3
        );

        // overlap boundaries: max_ts == range.start hits, max_ts < start misses
        assert_eq!(
            query_unbuilt("org1", "logs", "app1", (T0 + 2000, T0 + 3000), 0, 1000)
                .await
                .unwrap()
                .iter()
                .map(|m| m.id)
                .collect::<Vec<_>>(),
            vec![id_a, id_d]
        );
        assert!(
            !query_unbuilt("org1", "logs", "app1", (T0 + 2001, T0 + 3000), 0, 1000)
                .await
                .unwrap()
                .iter()
                .any(|m| m.id == id_a)
        );

        // inverted range is an error, not an empty result
        assert!(
            query_unbuilt("org1", "logs", "app1", (T0 + 10, T0), 0, 1000)
                .await
                .is_err()
        );

        // built-grace: build A, leave D pending
        let claimed = claim_pending("qb", 10, 60).await.unwrap();
        assert_eq!(claimed.len(), 4, "claims every pending row");
        assert_eq!(mark_built(&[id_a], "qb").await.unwrap(), 1);
        let now = now_micros();

        // built moments ago (updated_at >= grace cutoff): still visible
        let hits = query_unbuilt(
            "org1",
            "logs",
            "app1",
            (T0, T0 + 3000),
            now - 5_000_000,
            1000,
        )
        .await
        .unwrap();
        assert!(hits.iter().any(|m| m.id == id_a), "fresh built row visible");
        assert!(
            hits.iter().any(|m| m.id == id_d),
            "building row always visible"
        );

        // built long ago (before the grace cutoff): excluded
        force_updated_at(id_a, now - 3_600_000_000).await;
        let hits = query_unbuilt(
            "org1",
            "logs",
            "app1",
            (T0, T0 + 3000),
            now - 5_000_000,
            1000,
        )
        .await
        .unwrap();
        assert!(!hits.iter().any(|m| m.id == id_a), "old built row excluded");
        assert!(
            hits.iter().any(|m| m.id == id_d),
            "unbuilt rows unaffected by grace"
        );
    }

    #[tokio::test]
    async fn test_query_unbuilt_stream_token_json_escaping_round_trip() {
        let _guard = setup().await;
        // a stream name needing JSON escaping must round-trip because add()
        // and the LIKE pattern both go through serde_json
        let weird = r#"we"ird"#;
        let token = format!("org1/logs/{weird}");
        let id = add(&seg("node-w", 1, T0, T0 + 1000, &[token.as_str()]))
            .await
            .unwrap();
        let hits = query_unbuilt("org1", "logs", weird, (T0, T0 + 2000), 0, 1000)
            .await
            .unwrap();
        assert_eq!(hits.iter().map(|m| m.id).collect::<Vec<_>>(), vec![id]);
        assert_eq!(hits[0].streams, vec![token]);
    }

    #[tokio::test]
    async fn test_list_expired_and_delete_round_trip() {
        let _guard = setup().await;
        let id1 = add(&seg("node-e", 1, T0, T0 + 1000, &["o/logs/s"]))
            .await
            .unwrap();
        let id2 = add(&seg("node-e", 2, T0, T0 + 1000, &["o/logs/s"]))
            .await
            .unwrap();
        let id3 = add(&seg("node-e", 3, T0, T0 + 1000, &["o/logs/s"]))
            .await
            .unwrap();
        let claimed = claim_pending("sweep-b", 10, 60).await.unwrap();
        assert_eq!(claimed.len(), 3);
        // only id1/id2 get built; id3 stays Building and must never expire
        assert_eq!(mark_built(&[id1, id2], "sweep-b").await.unwrap(), 2);

        let now = now_micros();
        force_updated_at(id1, now - 7_200_000_000).await; // built 2h ago
        force_updated_at(id2, now - 3_600_000_000).await; // built 1h ago
        force_updated_at(id3, now - 7_200_000_000).await; // stale BUILDING row

        // cutoff 90min: only the 2h-old built row
        let expired = list_expired(5400, 10).await.unwrap();
        assert_eq!(expired.iter().map(|m| m.id).collect::<Vec<_>>(), vec![id1]);
        assert_eq!(expired[0].status, SegmentStatus::Built);

        // cutoff 30min: both built rows, oldest first; never the Building row
        let expired = list_expired(1800, 10).await.unwrap();
        assert_eq!(
            expired.iter().map(|m| m.id).collect::<Vec<_>>(),
            vec![id1, id2]
        );
        // limit applies
        assert_eq!(
            list_expired(1800, 1)
                .await
                .unwrap()
                .iter()
                .map(|m| m.id)
                .collect::<Vec<_>>(),
            vec![id1]
        );

        // delete removes exactly the given ids
        delete(&[id1]).await.unwrap();
        let expired = list_expired(1800, 10).await.unwrap();
        assert_eq!(expired.iter().map(|m| m.id).collect::<Vec<_>>(), vec![id2]);
        delete(&[id2]).await.unwrap();
        assert!(list_expired(1800, 10).await.unwrap().is_empty());
        assert_eq!(raw_count().await, 1, "only the Building row remains");

        // deleting nothing is a no-op; a deleted (node_uuid, seq) can be
        // re-registered as a fresh row
        delete(&[]).await.unwrap();
        let id_new = add(&seg("node-e", 1, T0, T0 + 1000, &["o/logs/s"]))
            .await
            .unwrap();
        assert_ne!(id_new, id1, "re-add after delete creates a new row");
    }

    #[tokio::test]
    async fn test_corrupted_rows_error_naming_the_row() {
        let _guard = setup().await;
        let id_bad_json = add(&seg("node-x", 1, T0, T0 + 1000, &["o/logs/s"]))
            .await
            .unwrap();
        raw_exec(&format!(
            "UPDATE wal_segments SET streams = '{{\"oops\"' WHERE id = {id_bad_json};"
        ))
        .await;

        // claim reaches the row via status and must fail loudly, naming it
        let err = claim_pending("bx", 10, 60).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains(&format!("id={id_bad_json}")) && msg.contains("malformed streams JSON"),
            "error must name the corrupted row: {msg}"
        );

        // unknown status value reaches the caller via query_unbuilt
        let id_bad_status = add(&seg("node-x", 2, T0, T0 + 1000, &["o/logs/s"]))
            .await
            .unwrap();
        raw_exec(&format!(
            "UPDATE wal_segments SET status = 9 WHERE id = {id_bad_status};"
        ))
        .await;
        let err = query_unbuilt("o", "logs", "s", (T0, T0 + 2000), 0, 1000)
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains(&format!("id={id_bad_status}")) && msg.contains("unknown status 9"),
            "error must name the row with the bad status: {msg}"
        );
    }

    #[test]
    fn test_stream_like_pattern_shapes() {
        // plain names: quoted token wrapped in wildcards
        assert_eq!(
            stream_like_pattern("org1", "logs", "app1").unwrap(),
            "%\"org1/logs/app1\"%"
        );
        // JSON-escaping names still produce the stored form
        assert_eq!(
            stream_like_pattern("org1", "logs", "we\"ird").unwrap(),
            "%\"org1/logs/we\\\"ird\"%"
        );
    }

    #[test]
    fn test_secs_to_micros_saturates() {
        assert_eq!(secs_to_micros(1), 1_000_000);
        assert_eq!(secs_to_micros(0), 0);
        assert_eq!(secs_to_micros(u64::MAX), i64::MAX);
        assert_eq!(secs_to_micros(i64::MAX as u64), i64::MAX);
    }

    #[tokio::test]
    async fn test_get_by_ids_returns_present_rows_and_skips_missing() {
        let _guard = setup().await;
        let a = add(&seg("node-g", 1, 100, 200, &["o/logs/s1"]))
            .await
            .unwrap();
        let b = add(&seg("node-g", 2, 300, 400, &["o/logs/s2"]))
            .await
            .unwrap();
        assert!(get_by_ids(&[]).await.unwrap().is_empty());
        let metas = get_by_ids(&[b, a, 999_999_999]).await.unwrap();
        // missing id absent, present rows ordered by min_ts
        assert_eq!(metas.iter().map(|m| m.id).collect::<Vec<_>>(), vec![a, b]);
        assert_eq!(metas[0].streams, vec!["o/logs/s1".to_string()]);
    }

    /// `l0_planned` straight from the DB.
    async fn raw_l0_planned(id: i64) -> String {
        let client = crate::db::sqlite::CLIENT_RW.clone();
        let client = client.lock().await;
        sqlx::query_scalar("SELECT l0_planned FROM wal_segments WHERE id = $1;")
            .bind(id)
            .fetch_one(&*client)
            .await
            .unwrap_or_else(|e| panic!("raw l0_planned {id} failed: {e}"))
    }

    async fn l0_planned_column_exists() -> bool {
        let client = crate::db::sqlite::CLIENT_RW.clone();
        let client = client.lock().await;
        let columns: Vec<(i64, String, String, i64, Option<String>, i64)> =
            sqlx::query_as("PRAGMA table_info(wal_segments);")
                .fetch_all(&*client)
                .await
                .expect("pragma table_info failed");
        columns.iter().any(|(_, name, ..)| name == "l0_planned")
    }

    /// The prod/dev tables were created (manually) BEFORE the GC design
    /// added `l0_planned`, so the ALTER path is the one that actually runs
    /// there: simulate that exact history with the OLD DDL, then prove
    /// `create_table` adds the column and stays idempotent.
    #[tokio::test]
    async fn test_create_table_migrates_missing_l0_planned_column() {
        let _guard = setup().await;
        // Simulate the pre-marker table shape WITHOUT dropping the table:
        // DROP TABLE desyncs the process-global sqlite index cache
        // (db::sqlite INDICES remembers the unique index as created, so a
        // recreated table silently loses it and every later ON CONFLICT add
        // fails — observed as run-order flakiness). DROP COLUMN keeps
        // indexes intact and exercises the same migration path.
        raw_exec("ALTER TABLE wal_segments DROP COLUMN l0_planned;").await;
        assert!(
            !l0_planned_column_exists().await,
            "pre-migration shape must not carry the column"
        );

        // a pre-existing row proves the ALTER back-fills the default
        raw_exec(&format!(
            "INSERT INTO wal_segments (node_uuid, seq, object_key, min_ts, max_ts, size, streams, status, builder_node, created_at, updated_at) \
             VALUES ('node-mig', 1, '{SEGMENT_KEY_PREFIX}node-mig/1', {T0}, {}, 1, '[\"o/logs/s\"]', 0, '', {T0}, {T0});",
            T0 + 1000
        ))
        .await;

        create_table().await.expect("create_table must migrate");
        assert!(l0_planned_column_exists().await, "column must be added");

        // idempotent: a second boot must not fail or duplicate anything
        create_table()
            .await
            .expect("create_table must be idempotent");
        assert!(l0_planned_column_exists().await);

        // the DROP TABLE above also dropped the indexes, and the process-
        // global sqlite index cache (db::sqlite::INDICES, initialized once
        // per process) makes create_table's create_index calls skip
        // re-creating them — restore them raw so the rest of this module
        // keeps its unique constraints (production never drops the table
        // mid-process, so the cache assumption holds there)
        raw_exec(
            "CREATE UNIQUE INDEX IF NOT EXISTS wal_segments_node_seq_idx ON wal_segments (node_uuid, seq);",
        )
        .await;
        raw_exec(
            "CREATE UNIQUE INDEX IF NOT EXISTS wal_segments_object_key_idx ON wal_segments (object_key);",
        )
        .await;
        raw_exec(
            "CREATE INDEX IF NOT EXISTS wal_segments_status_created_at_idx ON wal_segments (status, created_at);",
        )
        .await;
        raw_exec("CREATE INDEX IF NOT EXISTS wal_segments_max_ts_idx ON wal_segments (max_ts);")
            .await;

        // the pre-existing row got the NOT NULL DEFAULT ''
        let (id, planned): (i64, String) = {
            let client = crate::db::sqlite::CLIENT_RW.clone();
            let client = client.lock().await;
            sqlx::query_as("SELECT id, l0_planned FROM wal_segments WHERE node_uuid = 'node-mig';")
                .fetch_one(&*client)
                .await
                .expect("migrated row must survive")
        };
        assert_eq!(planned, "", "back-filled default must be ''");

        // and the migrated table serves the full new surface
        assert_eq!(claim_pending("mig-b", 10, 60).await.unwrap().len(), 1);
        assert_eq!(
            set_l0_planned(&[id], "mig-b", &["files/o/logs/s/k.vix".to_string()])
                .await
                .unwrap(),
            1
        );
        assert_eq!(raw_l0_planned(id).await, "[\"files/o/logs/s/k.vix\"]");
    }

    #[tokio::test]
    async fn test_set_l0_planned_is_fenced_and_flip_clears_it() {
        let _guard = setup().await;
        let id1 = add(&seg("node-p", 1, T0, T0 + 1000, &["o/logs/s"]))
            .await
            .unwrap();
        let id2 = add(&seg("node-p", 2, T0, T0 + 1000, &["o/logs/s"]))
            .await
            .unwrap();
        let keys = vec!["files/o/logs/s/2023/11/14/22/l0_node-p_1_2_472190.vix".to_string()];

        // never-claimed rows cannot be planned (status fence)
        assert_eq!(set_l0_planned(&[id1, id2], "pb1", &keys).await.unwrap(), 0);

        assert_eq!(claim_pending("pb1", 10, 60).await.unwrap().len(), 2);
        // full-count write on owned Building rows
        assert_eq!(set_l0_planned(&[id1, id2], "pb1", &keys).await.unwrap(), 2);
        let stored = serde_json::to_string(&keys).unwrap();
        assert_eq!(raw_l0_planned(id1).await, stored);
        assert_eq!(raw_l0_planned(id2).await, stored);

        // an intruder writes nothing (builder_node fence)
        assert_eq!(
            set_l0_planned(&[id1, id2], "intruder", &["files/x".to_string()])
                .await
                .unwrap(),
            0
        );
        assert_eq!(raw_l0_planned(id1).await, stored, "fence must hold");

        // lease stolen on id2: the loser's plan write comes back SHORT — the
        // discard-before-upload signal
        force_updated_at(id2, now_micros() - 61_000_000).await;
        assert_eq!(claim_pending("pb2", 10, 60).await.unwrap().len(), 1);
        assert_eq!(set_l0_planned(&[id1, id2], "pb1", &keys).await.unwrap(), 1);

        // guards: empty node / empty keys / keys without ids are caller bugs
        assert!(set_l0_planned(&[id1], "", &keys).await.is_err());
        assert!(set_l0_planned(&[id1], "pb1", &[]).await.is_err());
        assert!(set_l0_planned(&[], "pb1", &keys).await.is_err());

        // the fenced completion flip clears the marker in the same statement
        file_list_setup().await;
        let org = format!("torg{}", now_micros());
        assert_eq!(
            mark_built_with_files(&[id1], "pb1", vec![l0_file(&org, "l0_p_1_1_472190")])
                .await
                .unwrap(),
            1
        );
        assert_eq!(raw_l0_planned(id1).await, "", "flip must clear the plan");
        // the stolen row keeps pb1's stale plan (its winner never planned) —
        // exactly the crash evidence the GC pass consumes
        assert_eq!(raw_l0_planned(id2).await, stored);

        // the plain fenced flip clears too
        assert_eq!(set_l0_planned(&[id2], "pb2", &keys).await.unwrap(), 1);
        assert_eq!(mark_built(&[id2], "pb2").await.unwrap(), 1);
        assert_eq!(raw_l0_planned(id2).await, "");
    }

    #[tokio::test]
    async fn test_gc_orphan_listing_arming_and_clearing() {
        let _guard = setup().await;
        let id_dead = add(&seg("node-gc", 1, T0, T0 + 1000, &["o/logs/s"]))
            .await
            .unwrap();
        let id_fresh = add(&seg("node-gc", 2, T0, T0 + 1000, &["o/logs/s"]))
            .await
            .unwrap();
        let id_built = add(&seg("node-gc", 3, T0, T0 + 1000, &["o/logs/s"]))
            .await
            .unwrap();
        let id_no_plan = add(&seg("node-gc", 4, T0, T0 + 1000, &["o/logs/s"]))
            .await
            .unwrap();
        assert_eq!(claim_pending("gcb", 10, 60).await.unwrap().len(), 4);
        let keys = vec!["files/o/logs/s/2023/11/14/22/l0_gc_1_3_472190.vix".to_string()];
        assert_eq!(
            set_l0_planned(&[id_dead, id_fresh, id_built], "gcb", &keys)
                .await
                .unwrap(),
            3
        );
        // id_built: committed build → plan cleared, status Built
        assert_eq!(mark_built(&[id_built], "gcb").await.unwrap(), 1);
        // id_dead died 3 lease periods ago; id_fresh keeps a live heartbeat
        let dead_at = now_micros() - 400_000_000; // 400s > 3 * 120s
        force_updated_at(id_dead, dead_at).await;

        // listing: ONLY the dead, planned, unbuilt row — never fresh rows,
        // never Built rows, never rows without a plan
        let rows = list_l0_orphan_rows(360, 100).await.unwrap();
        assert_eq!(
            rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            vec![id_dead]
        );
        assert_eq!(rows[0].1, serde_json::to_string(&keys).unwrap());
        // a longer threshold excludes it (NEVER touch younger rows)
        assert!(list_l0_orphan_rows(500, 100).await.unwrap().is_empty());
        // limit 0 short-circuits
        assert!(list_l0_orphan_rows(360, 0).await.unwrap().is_empty());

        // arming succeeds exactly once while the row still looks dead
        assert!(gc_arm_l0_orphan(id_dead, "segment-gc", 360).await.unwrap());
        let (status, node, armed_at) = raw_row(id_dead).await;
        assert_eq!(status, 1, "arm must not change status");
        assert_eq!(node, "segment-gc");
        assert!(armed_at > dead_at, "arm must refresh the lease timestamp");
        // second arm (concurrent GC) sees a fresh row and backs off
        assert!(!gc_arm_l0_orphan(id_dead, "segment-gc", 360).await.unwrap());
        // fresh and Built rows can never be armed
        assert!(!gc_arm_l0_orphan(id_fresh, "segment-gc", 360).await.unwrap());
        assert!(!gc_arm_l0_orphan(id_built, "segment-gc", 360).await.unwrap());
        // empty gc node is a caller bug, not a silent no-op
        assert!(gc_arm_l0_orphan(id_dead, "", 360).await.is_err());

        // the armed row is fenced: the zombie's completion flips 0 rows and
        // a fresh claim within the lease skips it
        assert_eq!(mark_built(&[id_dead], "gcb").await.unwrap(), 0);
        assert!(
            claim_pending("thief", 10, 60)
                .await
                .unwrap()
                .iter()
                .all(|m| m.id != id_dead),
            "armed row must be unclaimable inside the lease"
        );

        // clear resets the marker; the row is gone from the orphan listing
        // even at a zero threshold (id_fresh still shows there — its plan is
        // legitimately in flight, which is why GC uses the 3-lease cutoff)
        clear_l0_planned(id_dead).await.unwrap();
        assert_eq!(raw_l0_planned(id_dead).await, "");
        assert!(
            list_l0_orphan_rows(0, 100)
                .await
                .unwrap()
                .iter()
                .all(|(id, _)| *id != id_dead)
        );
        let _ = id_no_plan;
    }

    #[tokio::test]
    async fn test_object_keys_under_returns_only_prefixed_keys() {
        let _guard = setup().await;
        let id1 = add(&seg("node-k", 1, T0, T0 + 1000, &["o/logs/s"]))
            .await
            .unwrap();
        let id2 = add(&seg("node-k", 2, T0, T0 + 1000, &["o/logs/s"]))
            .await
            .unwrap();
        // a row under a foreign prefix must not leak into the anti-join set
        let mut foreign = seg("node-k", 3, T0, T0 + 1000, &["o/logs/s"]);
        foreign.object_key = "other_prefix/node-k/3".to_string();
        let _id3 = add(&foreign).await.unwrap();

        let mut keys = object_keys_under(SEGMENT_KEY_PREFIX).await.unwrap();
        keys.sort();
        assert_eq!(
            keys,
            vec![
                format!("{SEGMENT_KEY_PREFIX}node-k/{:020}", 1),
                format!("{SEGMENT_KEY_PREFIX}node-k/{:020}", 2),
            ]
        );
        let _ = (id1, id2);

        // '%' in a prefix would stop being a prefix match — hard error
        assert!(object_keys_under("wal%").await.is_err());
        assert!(object_keys_under("").await.is_err());
    }

    #[tokio::test]
    async fn test_count_unbuilt_older_than_counts_only_aged_unbuilt() {
        let _guard = setup().await;
        let old_pending = add(&seg("node-c", 1, 100, 200, &["o/logs/s"]))
            .await
            .unwrap();
        let fresh_pending = add(&seg("node-c", 2, 100, 200, &["o/logs/s"]))
            .await
            .unwrap();
        let old_built = add(&seg("node-c", 3, 100, 200, &["o/logs/s"]))
            .await
            .unwrap();
        let aged = now_micros() - secs_to_micros(3600);
        force_created_at(old_pending, aged).await;
        force_created_at(old_built, aged).await;
        raw_exec(&format!(
            "UPDATE wal_segments SET status = 2 WHERE id = {old_built};"
        ))
        .await;
        let _ = fresh_pending;
        // only the aged, not-built row counts
        assert_eq!(count_unbuilt_older_than(600).await.unwrap(), 1);
        assert_eq!(count_unbuilt_older_than(7200).await.unwrap(), 0);
    }
}
