# Segment WAL — S3-first ingester architecture (adopted 2026-07-31)

Owner decision: adopt object-storage-first ingest. Ack ASAP (on buffer append,
NOT after PUT) — the owner explicitly tolerates ~1s of acked-data loss on node
crash; this keeps S3 latency out of the request path entirely. Legacy
memtable/WAL/mover path stays intact behind the flag (rollback = flip flag).

## Flow

```
request → parse/validate/pipeline → SegmentBuffer::append(org, type, stream, batch)
        → 200 (or per-record errors / 503 when the buffer is at its hard cap)

flusher (one per process): every ZO_SEGMENT_FLUSH_INTERVAL_MS (1000) or when
buffered bytes ≥ ZO_SEGMENT_FLUSH_SIZE_MB (32): swap buffer → encode ONE
segment object (all streams) → PUT to S3 at wal_segments/{node_uuid}/{seq:020}
→ register row in wal_segments table (idempotent) → drop from memory.
PUT/register failure: retry with backoff while the NEXT buffer keeps filling;
at ZO_SEGMENT_BUFFER_MAX_MB (128) appends return ResourceError (503).

builder (supervised job, ingester role, any node builds any segment):
claim ≤ ZO_SEGMENT_BUILD_BATCH (8) pending segments (lease
ZO_SEGMENT_BUILD_LEASE_SECS=120, heartbeat from CLAIM time — the compactor
heartbeat-gap bug class must not recur) → fetch + decode → group frames by
stream → per stream, per hour: homogenize schemas ONCE (the single place
type-flips resolve) → ONE L0 .vix via the SAME single-file build the WAL
mover uses today — full index, so compaction and the query path need zero
changes (a docs-only deferred-FST L0 is a later optimization; it needs
docs-only query support first) → upload → file_list batch add (one txn)
+ mark_built(segment ids) fenced by builder_node.
L0 object keys are DETERMINISTIC f(segment ids, stream) so a crashed build
re-uploads the same keys; duplicate file_list add is tolerated as success.

querier: the leader reads segment candidates (status != Built, NEVER Built)
BEFORE the file_list snapshot — ordering is the dup/gap invariant: the
builder commits L0 registration and mark_built in ONE fenced transaction
(mark_built_with_files), so a Built-observed segment is covered by the
later files snapshot (no gap), and a candidate whose build lands between
the two reads is dropped by l0_ filename-provenance dedup against the
snapshot (no dup). A 60s built-grace variant was tried and double-counted
the moment compaction merged an L0 away inside the grace (e2e heal test
caught it). The ordering additionally requires the wal_segments and
file_list reads to be causally consistent — true by default, where
CLIENT_RO == CLIENT (an empty ZO_META_POSTGRES_RO_DSN falls back to the RW
DSN); do NOT point ZO_META_POSTGRES_RO_DSN at an async replica with segment
mode on. With the single-transaction commit the old batch_add/mark_built
crash window no longer exists; the remaining accepted residuals are (a)
flag-off rollback leaves unbuilt segments invisible until they are built,
and (b) sync-mode acks in flight across a SIGTERM, bounded by the shutdown
window. Segments ride the existing id plumbing
as NEGATIVE ids; the follower resolves them via wal_segments::get_by_ids,
fetches via the disk cache, decodes the target stream's frames, and serves
them through NewMemTable (the lazy _source machinery, already audited
correct). Ingesters serve NO queries for segment-mode data.
(A per-request sync-ack knob existed briefly and was removed 2026-07-31 by
owner decision: one segment per request is a pathological small-file shape.
The e2e harness instead runs the flush interval at its 50ms floor.)

sweeper (compactor role): segments built AND updated_at older than
ZO_SEGMENT_RETAIN_SECS (3600): per-key S3 delete confirmed → row delete.
(storage::del swallows errors — do NOT use it here; delete per key via the
object store client and only remove rows for confirmed deletions.)
```

## Segment object format (version 1)

```
header (raw, uncompressed):
  magic  b"O2WS" | u16 version=1 | u16 uuid_len | node_uuid bytes | u64 seq
  | i64 created_at_micros | u32 zstd payload length prefix NOT stored (read to EOF)
payload: one zstd stream containing concatenated frames:
  FRAME = u8 frame_type (1=data, 0=end)
        | u16 org_len | org | u16 stream_type_len | stream_type Display string
        | u16 stream_len | stream
        | i64 min_ts | i64 max_ts | u32 rows
        | u32 ipc_len | arrow IPC stream bytes (self-describing schema)
        | u32 crc32 over every preceding byte of the frame
  (stream_type decodes via a STRICT inverse of Display — an unknown string is
  a hard error, never a default)
```
Decode rules: unknown version → hard error; crc mismatch / truncation → error
that names the segment; frames after a bad frame are NOT recovered (segments
are small and atomic — unlike the legacy WAL there is no tail-append).
Batches keep their write-time narrow schemas; the reader groups by per-batch
schema before any concat (the 2026-07-30 mixed-type lesson).

## wal_segments table

id bigserial PK; node_uuid varchar(64); seq bigint; object_key varchar(512)
unique; min_ts/max_ts bigint; size bigint; streams text (JSON array of
"org/stream_type/stream"); status smallint 0=pending 1=building 2=built;
builder_node varchar(64) default ''; created_at/updated_at bigint.
unique(node_uuid, seq); index(status, created_at); index(max_ts).
Claim: single UPDATE ... WHERE status=0 OR (status=1 AND updated_at < stale)
... RETURNING (atomic, leased). mark_built fenced: WHERE builder_node = $me
AND status=1 — 0 rows updated means the lease was lost: DISCARD the build
(keys are deterministic; the new lease holder re-produces identical files).

## Crash matrix

- node crash: loses current buffer (≤ ~1s + in-flight PUT) — accepted by owner.
- crash between PUT and register: node_uuid is per-boot → orphan object, never
  registered (S3 lifecycle prefix rule cleans it; negligible).
- builder crash: lease expires → re-claim → same deterministic keys.
- register/file_list add duplicate: treated as success (idempotent).
- S3 outage: buffers grow to cap → honest 503; nothing is acked that isn't
  either durable or inside the accepted ≤1s window.

## Config (all in config.rs, defaults chosen for prod)

ZO_INGEST_SEGMENT_MODE=false · ZO_SEGMENT_FLUSH_INTERVAL_MS=1000 ·
ZO_SEGMENT_FLUSH_SIZE_MB=32 · ZO_SEGMENT_BUFFER_MAX_MB=128 ·
ZO_SEGMENT_RETAIN_SECS=3600 · ZO_SEGMENT_BUILD_BATCH=8 ·
ZO_SEGMENT_BUILD_LEASE_SECS=120

## Module ownership (one owner per file — no cross-edits)

- src/segment_wal/ (new crate `segment-wal`): format, buffer, flusher/uploader.
- src/infra/src/wal_segments.rs: table + queries (+ migration).
- src/core write-path glue: logs/traces/metrics write_file seam + 503 mapping.
- src/jobs/src/job/segments.rs: builder job.
- src/core/src/search/grpc/segments_scan.rs + leader file-list seam: query path.
- src/core/src/compact/segments_sweep.rs (+ job wiring): sweeper.
