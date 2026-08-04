# obs engine — current state + backlog (single source of truth)

Supersedes NARROW-WAL-PLAN.md, FIELD-MAJOR-PLAN.md, DURATION-RANGE-PLAN.md
(deleted 2026-07-29; full history in git). Keep THIS file current.

## Shipped state (both envs, v0.93.0-vix-20260803.63, prefix obs-20260803/,
## db obs20260803 — BLOCK-DICT CUTOVER 2026-08-03, old prefix orphaned)

- THE BLOCK DICTIONARY is the only readable dict layout (18-RESOLVED
  below: ~4KB prefix-compressed key blocks + resident restart index;
  pre-block files hard-error; FST deleted). Keys stay field-major
  `{fid u16 BE}{token}`; match_all = per-fts-field seeks; bloom keys
  PINNED to v1 byte form forever. plist stages 1-4 in the binary, writer
  dark (ZO_VIX_PLIST_MIN_DOCS=0; enable 8192 compactor-first next).
  Fetch gate ZO_VIX_FETCH_CONCURRENCY=16, point-block fetch_many.
  Segment-scan budget counts only plan-needed columns (#20).
- Narrow WAL schemas (present fields only), spooled big move uploads
  (≥ZO_VIX_MOVE_SPOOL_MIN_BYTES=256MiB → <wal>/vix_spool), shutdown WAL
  drain (ZO_SHUTDOWN_MOVE_DEADLINE), ZO_FILE_MOVE_FIELDS_LIMIT deleted.
- Compaction: boot-time old-data pass (+120s), fleet-wide oldest-first job
  claiming bounded to worker count, ZO_COMPACT_MAX_FILE_SIZE=4096 (owner).
- Stats-served unfiltered COUNTs (fully-covered files answer from
  file_list.records; verified 32.9s → 521ms cold ≈ o2 parity).
- Upstream whole-query result cache OFF both envs (late-data staleness);
  vix per-file caches are the warm path and late-data-correct.

## Benchmark truth (2026-07-30 13:33Z, MEDIAN of 3, obs vs o2, 3h window,
## use_cache=false, healthy fleet, all hours sealed) — obs FASTER on 10/12

obs / o2 (ms): count_all 62/106 (1.7x) · histogram 231/638 (2.8x) ·
fat_histogram 268/413 (1.5x) · select51 441/836 (1.9x) · duration_range
579/1026 (1.8x) · isnull_dbsys 120/961 (8.0x) · vpc_count 21/151 (7.2x) ·
vpc_histogram 41/877 (21.4x) · k8s_histogram 37/289 (7.8x) · apisix_hist
38/146 (3.8x). LOSSES: needle_tid 362/267 (o2 1.4x — bloom fetch fan-out,
item 2) · match_all 288/120 (o2 2.4x — obs does one FST seek per fts
field, o2 one seek on its `_all` shadow column; obs dropped shadow
columns by design. Also NOT a like-for-like count: o2's traces stream
configures 6 EXTRA fts fields (api, db_query_text, db_sql_table,
sandbox_id, label_name, service_service_env) so it matches ~11% more
rows. obs's fts set = the 10 built-in defaults, of which only 4 exist in
traces: message, content, body, error).

MEASUREMENT DISCIPLINE (learned the hard way today): single-shot numbers
on this fleet are worthless — the same match_all measured 15640ms, 533ms
and 288ms within one hour. Always MEDIAN of >=3, alternate systems, and
first confirm (a) every hour in the window is sealed and (b) no ingester
is crashlooping (a crashlooping ingester makes WAL-touching queries time
out entirely — it produced two "incomplete" classes). o2's cold numbers
also flatter it: its disk cache is warm from a day of identical queries
while freshly-compacted obs objects are new to the cache.

## Benchmark truth (T+3h battery 2026-07-28, obs vs o2, use_cache=false)

Wins: fat-term filtered histogram 3.4s/94ms vs 3.4s/784ms; select51
238/73 vs 5203/609; trace_histogram warm 82ms vs 711ms; k8s + vpc classes;
COUNT cold 521ms ≈ o2 551ms (post-.34). Parity: needle warm. Losses:
duration_range 13s/3.3s vs 1.2s/0.29s (ACTIVE item below); def_match_all
counts +15% vs o2 (fts field-set diff, needs config reconciliation);
needle cold ~3x (bloom fetch fan-out).

## ACTIVE: none — .36 shipped; next lever below

.35/.36 OUTCOME (honest, prod-verified 2026-07-29): vortex levers 1-4
shipped — numeric bounds inject (logs prove it), file-level stats skip,
limit, decode-threads knob. duration_range 1h: 13-15s -> ~6s cold (2.2x,
compressed-domain eval + late materialization), count exact. NOT the
o2 1.2s target: zoned stats CANNOT prune uniformly-scattered outliers —
selectivity x 8192-row zones >= 1 match/zone even at duration>60s
(0.017%). Pruning wins only on time-correlated columns; the residual 6s
is fetching+decoding duration/_timestamp bytes of EVERY chunk.

LAYOUT HYPOTHESIS REFUTED BY LOCAL BENCH (owner's bench-first call,
2026-07-29, tests::ranged::docs_buffer_column_adjacency_bench): 1M rows /
131MB file — duration-only ranged scan fetches 2.79MB in 2 COALESCED
requests on the STOCK layout; the 2MB-vs-64MB buffer produced
byte-identical files (the buffer is not the adjacency knob; segment
coalescing already absorbs interleaving). docs_buffer_bytes plumbing kept
(inert, format-neutral) + the bench as the refutation record. The prod 6s
residual is therefore: decode CPU over ~46M rows/querier, OR real-S3
request latency, OR full scans running NON-ranged on sub-256MB files
(ZO_VIX_FULL_SCAN_RANGED_MIN_BYTES=256MB — post-compaction hour files
straddle it). PROD CONFIRMS DECODE-BOUND (2026-07-29): the same duration query at
cached_ratio=84 (bytes mostly LOCAL) still took 11.7s — not S3-bound.
NEXT: CPU-profile one querier during the query (perf + llvm-addr2line-19,
the ingester-profiling pattern) to split decompress vs filter-eval vs
materialize; then choose among (a) ZO_VIX_SCAN_DECODE_THREADS on
queriers (helps when concurrent files < cores), (b) duration-column
encoding audit (btrblocks int scheme decode speed vs parquet PLAIN —
maybe pin hot numeric cs columns to a fast encoding via
with_field_writer), (c) verify vortex zone-skip skips DECODE not just
fetch for pushed filters. LOG-MEASURED (owner's correction — logs, not API took; trace
019fad13122172a2...): plan 83-124ms/node; execution IS the time (stream
end 5.4-8.2s/node); ~60-72 files/querier through target_partitions 4-5 =
~14 files SEQUENTIAL per partition at ~500ms/file decode. Ceiling =
querier CPU allocation (2C requests -> partitions follow visible cores).
LEVER 1 SHIPPED + LOG-VERIFIED (prod #317 / dev #174, querier cpu limit
4 -> 16): per-node execution 5.4-8.2s -> 1.2-1.7s (4-5x, tracks cores);
warm end-to-end 1.86s vs o2 0.29s — gap 11x -> ~6x. Fair cold pending
cache repopulation (the roll emptied caches). Watch: 5 x 16C burst on the
shared pool (requests still 2C). LEVER 2 (if parity matters next):
perf-profile per-file decode (~100-300ms/file remains) — encoding audit
of duration/btrblocks int scheme; also consider a requests bump if
throttling shows under concurrent scans. Variance
note: always quote matched-count + files context with timings.

## Backlog (owner standing autonomy: implement without waiting)

0. IsNull SHIPPED FOR REAL in .39 (8411366403, 2026-07-29 13:24Z). The
   .37/.38 story was FALSE: both tags imaged the SAME stale pre-IsNull
   binary (identical amd64 manifest digest 23baffb0...) because
   openobserve-core stopped release-building — the docs_buffer_bytes
   bench plumbing missed core_writer's initializer — and the image
   pipeline silently reused an old target/release/openobserve. The
   ".38 verified 1.83s" was a warm 16C full scan returning a
   coincidentally-correct 0: timing-only verification. VERIFIED .39
   (13:52Z, extraction logs + scan_size, use_cache=false):
   - COUNT service_name IS NULL: 0.35s (was 14.4-18.7s / 300+GB full
     scan). Log: index_condition Some(service_name IS NULL) +
     SimpleCount, file_num 0, index fetches 0 B.
   - star ORDER BY/LIMIT 10: 4.8s cold (was 18.4s / 328GB), SimpleSelect
     + condition, file_num 0, 0 rows correct. Residual = fat KeyExists
     postings walk (~165MB index fetch/querier — service_name exists in
     ~every row; the complement is empty); repeats warm the per-file
     result cache. ~26M scan_records = ingester WAL slice (no .vix by
     design). [former follow-up (b): gating was NEVER broken]
   - COUNT "db.system.name" IS NULL (90.3% of rows): 0.4s (was
     12.9-18.7s), count EXACT — order-swapped CASE-aggregation
     cross-check on a fixed closed window drifts +14/+2 rows on ~300M
     WITH the drift direction following execution order = late-arriving
     in-window data, not indexing. [former (a)+(c): closed]
   Regression armor now in tree: wire-path tests (logical simplify →
   physical plan → proto round-trip → follower IndexRule gate+builder),
   e2e IS NULL coverage in all three shapes (vixtest err_code), and the
   provenance-gated image pipeline (deploy/: binary mtime must postdate
   HEAD commit, sha must differ from the previous tag's binary, mimalloc
   grep, git_commit label + /GIT_COMMIT file, pushed manifest digest
   asserted to change). SHIP RULE: never verify a rollout by timing —
   require the extraction log line + scan_size.

0b. WAL-window star hits SHIPPED .40 (9833b745b8, prod-verified
   2026-07-29 15:2xZ): SELECT * hits from the WAL parquet window degraded
   to _timestamp + filter columns (user-reported; 15/200 slim). Cause:
   listing stats claim the synthesized _source column is ALL NULL for
   files not storing it; DF's parquet opener constant-folds all-null
   projected columns to NULL literals from stats BEFORE the
   SourceSynthesizingExprAdapter runs. Fix: neuter _source per-file stats
   in prepare_file_scan_groups (read-side only). Verified: 200/200 full
   hits incl. a 32s-old row; 'no _source cell' warns stopped. Pinned by
   wal_parquet_star_synthesizes_source (drives the real scan path).

0c. FORMAT CLEANUP SHIPPED .41 (faa3b92f25 + 09f92c1ab7, owner-directed):
   v1 key-layout READ support fully removed (KeyLayout type deleted,
   absent key_layout property = hard open error, mixed-layout merge gate
   gone) + docs_buffer_bytes experiment plumbing removed (orphaned
   vortex-btrblocks dep dropped). KEPT deliberately: bloom v1 BYTE FORM
   (group .bf continuity — live key form, not compat), standalone vortex
   (metrics downsampling writes it), WAL/metrics parquet + _source
   synthesis, index_file DB column (always-false vestige; drop = schema
   migration, deferred). Net -923 lines.

0d. MERGE DICT CORRUPTION (2026-07-29 evening, root-caused + contained):
   the parallel-merge range sampler parsed v2 term_mins under the v1 byte
   form; a garbage parse split different inputs at different FIELDS (each
   input has its own fid table) and the concatenated dict carried
   OVERLAPPING row groups — reader open then hard-fails ("row groups N and
   N+1 overlap"), per-file eval degrades to scan, bloom backfill requeues
   forever. Six corrupt ~1GB second-pass outputs (hours 06,09,10,11,15x2;
   big merges only began today with the 4096 target; first hit 14:02Z =
   .39, the first non-stale binary). FIX .42 (5d543c608f): sampler retired
   (single-range merges) + write_index_blobs hard-rejects out-of-order
   parts. CONTAINMENT until .42 rolls: ZO_VIX_MERGE_THREAD_NUM=1 (config
   PR #324/#180) — REMOVE after .42. HEAL: automatic — merges route
   malformed-index inputs to the rebuild-from-_source fallback; verify the
   6 files get rebuilt, else force jobs.

0e. FRESH-HOUR STARVATION (same evening): boot-pass old-data jobs +
   oldest-first claiming left hours 16/17 at ~1400 files (15-20x per-file
   query fan-out: histogram 23s cold, duration 70s@3h, needle 6s warm,
   match_all 26s — measured in the 3h battery; compacted hours are FINE:
   duration 1.3s/1h). FIX: ZO_COMPACT_FAST_MODE=true (offsets DESC
   claiming, newest first) — same PR. Battery re-run pending catch-up.
   NOTE: shipped binaries ballooned 364MB->4.4GB starting .40 (full
   debug=2 DWARF via the perf env vars; earlier binaries were built
   without); slim the image (compress/strip debug in deploy/) — rolls
   pull 5.5GB currently.

0f. COMPACTION JOB STRANDING FIXED .45 (a0ad7384f8): add_job's ON
   CONFLICT DO NOTHING silently dropped the re-queue of an hour whose job
   had run while the hour was still OPEN (incremental rounds complete
   WITHOUT sealing — they carry the sub-budget remainder), so closed hours
   sat at ~1250 files until job_clean_wait_time aged the DONE row out.
   Measured cost before the fix: 3h histogram 32.5s, duration 46s,
   isnull 22s — all 15-20x file fan-out; sealed hours were 40x faster.
   add_job now resurrects DONE rows to Pending (PENDING/RUNNING untouched).
   .44's first attempt (re-pend the job instead) was WRONG and reverted:
   a permanently-Pending job parks at the head of the newest-first claim
   order and starved every older hour. ZO_COMPACT_FAST_MODE=true (newest
   first) stays.

0g. INGESTER DataFusion POOL CAPPED (prod PR #332, 2026-07-30): the pool
   AUTO-SIZES to 50% of the container limit — boot banner "Datafusion pool
   size: 16.00 GB" on a 32Gi ingester — and stacks on
   ZO_MEM_TABLE_MAX_SIZE=6144. obs-ingester-5 was OOMKilled 14x in 55min
   WHILE SERVING; every query needing its WAL timed out. Ingesters search
   only their own WAL (observed peak 4.68 MB), so
   ZO_MEMORY_CACHE_DATAFUSION_MAX_SIZE=2048 now caps it (~400x peak,
   ~14GB headroom reclaimed). Queriers keep their explicit 12288.
   LESSON: check auto-sized pools against the CONTAINER limit on every
   role, not just queriers.

0h. LAZY `_source` SHIPPED .46 (c732cfd3db23, prod-verified): NewMemTable
   holds RAW batches (strictly less resident memory — the retained JSON
   column is gone) and adapts raw->plan per streamed batch
   (adapt_memtable_projection): column refs, typed-NULL padding, _source =
   SynthesizeSourceExpr over the RAW columns, everything cast to the
   plan's exact types. adapt_batch deleted. ZO_MEM_TABLE_MAX_SIZE restored
   to 6144 (#336). LESSONS: (a) the FIRST attempt (7f40069a85, reverted)
   synthesized after the plan projection had already dropped the raw
   fields — unit tests passed, e2e caught two-field records; the memtable
   provider's batches at scan time carry only the PLAN's columns, so
   synthesis must happen where raw fields still exist. (b) an uncast
   Utf8-vs-Utf8View mismatch breaks flight IPC ('Missing variadic count
   for Utf8View column') — cast synthesized/raw exprs to the plan type.
   Both pinned by NewMemTable unit tests + the e2e record-completeness
   asserts.

0i. INGESTER-5 SAGA (2026-07-30, resolved by quarantine — ROOT CAUSE OF
   THE MOVER FAILURE STILL OPEN): the pod OOMKilled ~30x across .45/.46,
   surviving every engine fix, 0->23GB in ~16s even with intake disabled.
   Its WAL held **798 unmoved wal parquet files dating to 00:05** — the
   move pipeline had been silently failing since the midnight HPA
   scale-in/recreate — and searches over that ~800-file wide-schema tree
   allocated the 23GB. Quarantining pre-15:00 files
   (/data/wal_quarantine on the ordinal-5 PVC, 1.4GB, RESTORABLE)
   stabilized it instantly. RESOLUTION UPDATE (16:xx): the HPA scaled the
   pod away again; offline PVC inspection showed the scale-in preStop
   drain uploaded the ENTIRE live WAL tree (0 files left) — no data loss,
   and proof the mover WORKS when given room. REVISED ROOT-CAUSE THEORY:
   not a wedge — the steady-state mover fell behind a SKEWED inbound
   share (the pod wrote wal files at up to 18x its peers' rate; suspect
   the cross-cluster NLB target group concentrated traffic on it after
   the midnight churn), and the ever-growing tree made searches heavier
   until the 13:00 OOM tipping point. The 798 quarantined files were
   RESTORED into /data/wal/files on the parked PVC via a debug-pod mount:
   ordinal-5's next life replays/moves them, and its termination drain
   guarantees upload regardless. STILL OPEN: (1) verify NLB target-group
   registration is balanced across current ingesters (aws elbv2
   describe-target-health on obs-ingester-5080-internal — blocked on SSO
   at the time); (2) an ALERT on ingester wal-file count/age (>100 files
   or oldest >30min = mover falling behind) — silent accumulation was
   the whole disease.

1. def_match_all +15% count diff vs o2 — reconcile fts field sets.
   ROOT CAUSE IDENTIFIED (2026-07-30): o2's traces stream configures 6
   fts fields obs does not (see benchmark truth above). Adding them to
   obs would give parity BUT fts marking is stamped AT WRITE TIME, so
   existing .vix files would have those fields marked term-only and
   queries touching them would skip the index and scan until compaction
   rebuilds those hours — OWNER DECISION pending (it changes query
   results, not just speed).
   ALSO: match_all('error') on traces counts obs 74.8k vs o2 92.8k
   (-19%) in tonight's battery — same field-set reconciliation, now with
   a concrete repro query.
2. Needle cold ~3x — bloom fetch fan-out (batch/parallelize group .bf
   reads; active-hour .bf chunks remain the residual for fresh tids).
3. Cacheable bailed-scan results (bail is rare post-field-major; low prio).
4. Hour-job splitting across nodes (only extreme backlogs; low prio).
5. Dotted service.name residue in otel logs pipeline (collector transform).
6. Fork publishing = anonymous squash chain to Windforce17/openobserve
   branch vix-arch (remote `windforce`; author/committer `anonymous
   <anonymous@users.noreply.github.com>`, commit-tree on top of the
   previous chain head, fast-forward push, NEVER identity/session
   links). Chain head 4bfc6434d4 published; NEXT commit (tree ==
   ea5b1fef9a: block dict + fetch gate + #19 + #20) is built on local
   branch `fork-published` awaiting push — ambient gh credential has
   no write access to the fork (owner's Windforce17 token required):
   `git push windforce fork-published:vix-arch`.
7. Watch: ingester-4 OOMKilled 2x 2026-07-28 21:34Z (boot+burst, self-
   healed) — recurrence means explicit ZO_MEM_TABLE_MAX_SIZE.
8. Vortex capability review findings → see VORTEX-REVIEW.md (2026-07-29).
9. Merge parallelism on v2: partition_bounds parses term_min in the v1
   BYTE FORM; field-major keys virtually never match (NUL filter), so
   dictionary merges have run SINGLE-RANGE since .32 (pre-existing,
   surfaced during the .41 cleanup). If big-merge wall-clock matters,
   design a field-major sampling scheme — safe only if fid remaps stay
   order-preserving (nothing gates that anymore beyond the ascending
   backstop).
10. Bloom poison discipline covers walk/parse layers only (2026-08-01):
    corruption that surfaces through FETCH-MIXED layers (open_ranged
    footer parse, FST/dict blob load, vortex scan decode) is unmarked,
    so such a file re-queues every pass and burns one fallback-budget
    slot per pass (bounded per pass, but never converges). Fix = attach
    the UnbuildableFile marker at source.rs/container.rs validation
    sites, where corrupt-bytes vs fetch-failure can still be told apart.
    Ops escape hatch meanwhile: a stamped file is reset with
    UPDATE file_list SET bloom_ver=0 WHERE id=...; the 8 known corrupt
    .vix files (task: heal later) are this class.
11. ES bulk item order is grouped, not interleaved (pre-existing since
    the pipeline unification, documented 2026-08-01): parse-time and
    pre-write rejection items land before the write-path items rather
    than at their exact request positions (WITHIN the write path order
    is exact — the P1 fix). Full positional fidelity for mixed
    rejection/success bodies means threading slot indices through
    parse_bulk_body and ingest::ingest into PendingRecords.
12. Test-seam gaps accepted 2026-08-01 (not worth hot-path churn now):
    (a) enqueue_and_wait consumer-FAILURE payload untested — the Err
    travels the same ack.send(ret) statement the tested Ok does;
    (b) stage()-failure -> cleanup_partial_stage wiring unasserted —
    the cleanup helper itself is fully tested, the wiring is a match
    arm; a MemTable persist fault-injection seam would be needed.

## .47 (2026-07-31): P0 outage fixes + segment-WAL architecture (DARK)

Shipped e588175ba2 both envs. THREE bodies of work in one image:

1. **P0 fixes for the 07-30 recent-data outage**: memtable search groups
   batches by their OWN write-time schema (mixed-type concat panicked every
   ingester); concat errors propagate instead of unwinding into gRPC resets;
   mover supervised (per-batch panic containment + claim release, de-unwrap'd
   scan path, restart-on-death, load_pending_delete retry); file_list::set /
   progress PROPAGATE registration errors — the mover's release arm was dead
   code and deleted WAL sources for never-registered uploads (silent loss).
   Ingester memtable 6144→4096 (rotation-trigger fix landed; node-wide pool
   still open).

2. **Segment-WAL architecture (DESIGN-SEGMENT-WAL.md), flag-dark behind
   ZO_INGEST_SEGMENT_MODE**: ack-on-append (owner accepts ≤1s loss on crash),
   one multi-stream zstd segment object per node per second → wal_segments
   table → any-node L0 builder (leased claims, heartbeat-from-claim, ONE
   fenced txn registers L0 files + marks built) → normal compaction; leaders
   read candidates BEFORE the file snapshot (ordering, not grace, is the
   dup/gap invariant) and ship them to followers as negative ids through the
   existing proto; followers serve them via NewMemTable. Sweeper reclaims
   built segments with per-key verified deletes. ZO_SEGMENT_SYNC_INGEST =
   ack-after-durable (harness/strict streams). e2e green BOTH modes incl.
   compaction-heal parity.

3. **Adversarial review (26-agent pass) fixed 21 confirmed findings** pre-ship:
   contiguous-run L0 provenance, fenced single-txn mark_built_with_files,
   deterministic-error escape from retry-forever, shutdown post-drain
   flush_now, unconditional builder/sweeper/flusher spawns (safe rollback),
   sweeper drain loops, query LIMIT + follower bytes budget, buffer append
   validation, config validation, mover claim-release on early-Err.

**ENABLEMENT PLAN (segment mode)**: fleet must be fully on .47+ BEFORE the
flag (old followers ignore negative ids silently). Canary on DEV first:
ZO_INGEST_SEGMENT_MODE=true on dev ingesters+queriers → verify [SEGMENT:FLUSH]
ships, L0 builds, volume parity, async-ack freshness (~1-2s) — the async ack
path is NOT e2e-covered (harness runs sync) — then prod. Rollback = flip flag
off: builders/sweepers keep draining (unconditional spawns); unbuilt segments
are invisible until built (bounded by builder lag).

**Open (from the audit register, not in .47)**: P1 integrity batch for the
LEGACY path (compactor C8 double-merge fencing, C9 deletion-set exactness,
ingest 200-on-failure C3, fsync chain C5, bloom backfill C10 key fix); node-
wide DataFusion pool; needle/match_all perf items; audit register: removed by owner request (2026-07-31); findings live in the repo backlog + this file


## Segment-WAL prod enablement: three brownouts, rolled back (.49-.53, 2026-07-31)

Enabled on prod, hit three distinct throughput failures, rolled back via the
flag each time (the rollback path WORKED — designed unconditional
builders/sweepers kept draining). NOTHING LOST: all segments are durable
objects; the two flag-on windows become queryable as builds land.

1. **503 storm (.49)**: flusher slept a full tick between ships regardless
   of backlog -> ~16MB/s/node < prod inbound -> buffer cap -> fleet-wide
   ingest 503s. Fixed .50: never sleep with a take due, 4 concurrent ships,
   permit BEFORE take (an object-store stall parks nothing in memory).
2. **Builder OOM loop (.49-.52)**: L0 sort plans peak ~3x decoded input
   against the ingester's 2048MB pool. Capped by segment COUNT (.51) then by
   COMPRESSED bytes (.52) — both wrong: segments could hold a whole buffer,
   and traces compress ~10x. Fixed .53: cap on DECODED arrow bytes measured
   from the actual frames (128MB/run, >160MB builds serially), 3 concurrent
   small builds, plus take_if capping each SEGMENT at ~flush size.
3. **Query outage (.52)**: with 22k unbuilt segments, every query
   overlapping the backlog tripped the leader's 10k safety cap and ERRORED.
   The cap correctly refuses unbounded scans, but it converts a builder
   backlog into a total query outage. Rolled back; queries recovered in
   ~3min.

**Measured drain (.53, flag off, no arrivals)**: 24000 -> 13168 unbuilt in
~55min (~200/min) once the fleet converged; OOMs 55 -> **0** (fully
converged fleet, 47 build batches/5min across 26 pods). The decoded-byte
cap is the fix that actually held.

**RE-ENABLE GATE**: (a) backlog at zero — DONE 2026-07-31 (13456 built,
drained ~200/min with ZERO OOMs on .53); (b) build throughput > production
with margin — ADDRESSED in .55: measured ~25/min/ingester capacity vs
~12-25/min produced was only 1-2x, so the L0 builder now also runs on
COMPACTORS (spare CPU, 10 replicas, no ingest latency to protect), roughly
tripling fleet capacity; (c) leader cap non-fatal — DONE in .54: the query
serves the newest 10k segments + all files and reports the remainder via
is_partial naming the skipped count, so a backlog can never again black out
queries; (d) staged rollout — REMAINS: flip the flag during a low-traffic
window and watch bufferfull / OOM / unbuilt-backlog slope, rolling back on
any of them (rollback = flag off, proven 3x, ~3min to full query recovery).
Re-verify (b) empirically on the first enable: production and build rates
are both observable in the [SEGMENT:FLUSH]/[SEGMENT:BUILD] logs.

**LESSON**: every failure was a THROUGHPUT/sizing property invisible to
unit+e2e (single-node, tiny data) and to a dev canary (1/10th prod inbound).
Prod-rate load testing — or a shadow mode that ships segments WITHOUT the
query path depending on them — is a prerequisite for the next attempt.

## Segment-WAL ENABLED on prod (.55, 2026-07-31 19:00Z)

Re-enabled after .54 (non-fatal leader cap) + .55 (L0 builder on compactors).
40+ min steady state: bufferfull=0, OOM=0, unbuilt stable 120-180 (~25s
pipeline lag, not growing), ~400 segments/min produced vs ~4x build capacity,
queries non-partial. Live parity vs o2 over a settled 25-min window: apisix
+1.09%, traces +0.07%, k8s_prod_public_logs +6.83%. Segment GC clean.

Rollback (proven 3x today, ~3min to full query recovery): flip
ZO_INGEST_SEGMENT_MODE=false — builders/sweepers keep draining
unconditionally, so written segments still become queryable.

WATCH ITEMS: unbuilt backlog slope (alert if it rises past ~1000 and keeps
climbing); [SEGMENT:FLUSH] bufferfull (should stay 0); ingester WAL files
(legacy path now idle for segment-mode streams).
13. Recent-window histogram 9x slower than o2; SEALED window obs WINS
    (measured 2026-08-01, op-name histogram, median-of-3, cache off):
    sealed hour obs 34ms vs o2 47ms — engine + SimpleHistogram-from-index
    is fine. Recent 1h window obs 2172ms vs o2 234ms, scan 1.8GB vs
    317MB: the window spanned ~1000 files (unsealed hours run 680-730
    L0-heavy files vs 97 sealed) so the per-file vix term lookup
    (dict/FST+terms sections) x1000 dominates, worsened right after the
    fleet roll (fresh vix objects only ~50% disk-cached; prefetch misses
    registrations during pod downtime). Levers, biggest first:
    (a) intra-hour L0->L1 rollup cadence so the newest 1-2h hold
    hundreds not ~700 files; (b) batch/merge term lookup across small
    L0s (shared FST probe); (c) boot-time cache backfill sweep for the
    prefetcher's downtime blind spot. Note: sealed-hour bucket sums
    diverge obs -0.53% vs o2 for 07-31 09:00Z — an incident-era hour
    (.53/.54 saga); recent windows are -0.03%. Attribute before using
    that hour as a parity reference.
14. Broad-term histograms read ~3x the bytes of a column scan
    (2026-08-01: service_name=nexus-service, 25% selectivity, 12h):
    obs 11.7GB/1269ms vs o2 3.8GB/847ms, sums exactly equal. The
    SimpleHistogram-from-index path materializes postings+timestamps per
    match, which loses to a dense _timestamp column scan once terms are
    unselective (needle terms invert this: obs 50KB vs o2 14MB, obs
    faster on sealed hours). Lever: selectivity bail in the index
    optimizer — above a postings-density threshold, answer histograms
    from the timestamp column and keep the index for pruning only.
    (Prod schema curiosity, same measurement: traces carry BOTH
    service_name AND a literal "service.name" column, differing by
    0.003% of spans — some producer writes only the dotted form.)
15. Histogram-from-postings-ranks (owner idea 2026-08-01): docs are
    _timestamp-DESC sorted, so bucket edges are per-file ROW-ID CUTS and
    bucket counts are postings-rank differences at the cuts — no bitmap,
    no per-row timestamps. The zone fold already covers the timestamp
    side (chunks folding into one bucket); the missing piece is a
    POSTINGS SKIP TABLE (every K delta-blocks: first_doc_id +
    byte_offset, ~1-2% size, appended blob = old files stay readable) so
    rank(cut) = skip-table binary search + ONE block decode instead of
    materializing the whole list. Expected on the broad-term class
    (nexus 25% sel, 12h/5min: 11.7GB today): postings ~4.6MB -> ~0.1MB
    per file, total ~11.7GB -> ~2GB (edge-chunk timestamp decode becomes
    the floor). Needle terms / buckets narrower than chunk spans keep
    today's path. Supersedes the #14 'selectivity bail' idea for the
    histogram shape.
    RESOLVED 2026-08-01 (.57+.59): the mixed-predicate histogram class.
    12h nexus+duration: 60-75s FLAPPING -> 7.3-8.5s stable (5-run check,
    even ~2GB/querier fetch), 3x faster than o2's 25s on its variant.
    Two compounding fixes: (a) .57 scan-projection narrowing after
    index-condition strip (the construct_filter_exec TODO); (b) .59
    per-file fallback (selection_exact) — one partial-field file no
    longer forces the re-applied filter onto a whole part. REMAINING in
    this area: #13's recent-window L0 file-count amplification (own
    item); WHY some files mark service.name partial (type-drifted
    values at build — writer-side fix would shrink the fallback residue
    to zero; ties into task #12's underscored-variant zero-rows oddity).
16. Plan-review probe residuals (2026-08-01, 12h traces window, cache
    off): (a) ALL-rows histogram scans 12GB/2.0s — the no-condition
    SimpleHistogram should fold from zone maps at ~0 data bytes; find
    where the All-condition path falls to the scan. (b) topn-service
    (unfiltered GROUP BY service_name LIMIT 10) scans 12GB/4.0s — the
    single-field TopN dictionary path (pilot fix B) excludes fts-marked
    fields; check whether service_name is fts and whether value terms
    could still serve it. (c) ~5-10% of search API calls intermittently
    return a body with trace_id but no took/hits (observed 3x today,
    also mid-stability-run) — capture one and chase (router? queue?).
    (d) mixed-dur-only (numeric-only predicate) 10s/12GB — numeric
    conditions have no index service; the duration ColumnBound prunes
    nothing because per-chunk duration spans are wide; candidate:
    value-bucketed numeric zone maps or duration-sorted row groups.
    #16 partial resolution (.60, 2026-08-01): eval-bail now compares
    its projection against the scan alternative (window_compressed/2,
    flat cap as floor). Broad-term 5-min histograms: 2.6-3.3s with 2/5
    parts bailing -> 1.8-2.1s warm, zero bails, nondeterminism gone.
    (a)-(d) remain open; the segment e2e also flaked once today
    ("Trigger was not updated after 20 attempts" + a transient internal
    'Operation is not implemented' from a derived-stream ingest node —
    clean on rerun, zero occurrences in passing logs; if it repeats,
    chase alongside (c).)
17. _source extraction key mapping (latent, found closing task #12):
    re-applied filters on UNDERSCORED columns json-extract from _source
    whose raw keys may be DOTTED (service_name vs "service.name") —
    extraction returns NULL and silently drops rows. Pre-.59 this
    zeroed whole queries; post-.59 it only affects fallback-residue
    files (counts currently match the dotted variant), but any
    fallback-heavy query on an underscored alias of a dotted attribute
    undercounts. Fix: the extraction adapter should try the column
    name's dotted twin (the flatten inverse) when the exact key is
    absent. Also #16(a) CLOSED: all-rows histogram scan=12GB is
    accounting only — actual data fetches 0.00GB (zone-fold works);
    scan_size for index-answered files attributes compressed file size.
    #15 stages 1-3 LANDED (dark, 2026-08-02, ships in .61): stage 1 =
    record codec + rank_at() (property-tested); stage 2 = writer behind
    ZO_VIX_PLIST_MIN_DOCS (0=off; pointer cells [u64 off][u32 len],
    dense-elision precedence, multi-sink offset rebase, merge emission);
    stage 3 = postings_union + for_each_term + dictionary-merge INPUTS
    all resolve pointer cells (representation-aware single-contributor
    verbatim fast path: inline->inline, record->record). ENABLEMENT
    ORDER: read+consumer support on every pod FIRST (.62 carries all
    four stages), then flip ZO_VIX_PLIST_MIN_DOCS — compactor first,
    START AT 8192 (>= ~4096 guarantees every out-of-row record has a
    non-degenerate skip table; owner ratified the threshold design
    over all-out-of-row 2026-08-02: inline locality wins for the Zipf
    tail — needles keep one-read postings; universal pointers would
    add a dependent fetch per needle per file and ~16B/term overhead
    on millions of rare terms while not shrinking the FST at all).
    STAGE 4 LANDED (2026-08-02, commit 60c5445b0c): cuts+ranks
    consumers — postings::for_each_in_range (skip-table jump-in,
    bounded group decode, property-tested), PlistCursor
    { rank, for_each_in_range }, collect::ranked_simple_histogram +
    ranked_count_in_window (per-chunk rank diffs; single-bucket
    in-window chunks fold; only bucket/window-straddling chunks
    decode; NO global _timestamp order assumed — correct on merged
    concatenations). CAUGHT IN TESTING: the grid's last bucket can
    overshoot end_time (ceil sizing) — the ranked path must clamp to
    the query window explicitly, matching the bitmap path's
    time-range AND (dual-build equality test, 1751-vs-980 overcount).
    Remaining: stage-4b ranged-mode SUB-record reads (skip-table head
    fetch + one group per rank, so ranged readers fetch KBs per cut
    instead of the whole record) — pairs with #18(b) fts dict
    bucketing for the cold path. Original stage map (integration
    anchors) below:
    - WRITER: VixWriterOptions.postings_plist_min_docs (default 0=off);
      TermSink push must receive IDS not blobs (writer emit at
      writer.rs:~1570 has them; merge emit at merge.rs:561/566 has them
      in `ids`); pointer cell = [u64 offset][u32 len], gated by
      doc_count >= threshold (never sniff bytes; dense elision stays
      empty-cell). CAUTION: write_index_blobs combines MULTIPLE sinks
      (parallel merge, one per key range) -> per-part plist regions must
      concatenate with pointer-cell OFFSET REBASING (doc_count column
      identifies pointer cells; rebuild the Binary column). finish_inner
      blob assembly at writer.rs:~1605; property written iff enabled.
    - READER: resolve pointer cells at reader.rs:~1607 (union bitmap),
      reader.rs:~806 (for_each_term walk) via plist BlobHandle
      (Mem slice / ranged block_fetch of [offset..offset+len]); plus a
      rank(ordinal, target) API reading only skip-table + one group.
    - MERGE inputs at merge.rs:635/644 need the same resolution.
    - CONSUMER: collect.rs simple_histogram gains a cuts+ranks path
      (zone-chunk bucket edges -> row cuts -> rank diffs) when the file
      is plist-capable; falls back to today's bitmap path otherwise.
    - ROLLOUT DISCIPLINE: read support ships fleet-wide FIRST (dark),
      writer enables after (compactor merge outputs are where broad
      terms live), exactly like the v1->v2 key-layout migration.

19. LEGACY-WAL RESTART REPLAY DOUBLE-PERSIST (P1, PRE-EXISTING — found
    2026-08-02 during .61 verification, NOT a .61 regression: .60
    reproduces byte-for-byte). Repro (deterministic): legacy mode
    (ZO_INGEST_SEGMENT_MODE off), ingest 1M rows (retention 15s), let
    moves settle, count = exactly 1,000,000; SIGTERM (clean: "final
    move complete, WAL empty" logged); restart the SAME data dir; count
    = 1,136,000 (+13.6%). One raw WAL segment survives shutdown and
    boot replay re-ingests it wholesale — rows already moved to .vix
    get persisted AGAIN as new files (file_list sum(records) confirms
    meta-level duplication; not a query bug). The final move moves WAL
    parquet but the raw segment is not truncated / no replay
    safe-point offset is persisted at shutdown. Exposure: prod
    ingesters run SEGMENT mode (since .55) — but legacy is THE
    ROLLBACK PATH, so any segment-mode rollback + ingester restart
    silently duplicates until fixed. Also hits any legacy-mode
    deployment (dev tools, benches). Fix direction: persist the replay
    safe-point (moved-through offset) in the shutdown barrier after
    the final move, or truncate/rotate the raw segment once the move
    completes; boot replay must skip fully-moved segments. Verify with
    the 3-step repro above + a kill -9 variant (replay after crash
    must still dedup or re-move only unmoved tail). SEGMENT-MODE
    CONTROL (ran 2026-08-02, .61 binary): ingest 1M -> clean kill ->
    restart -> total stays EXACTLY 1,000,000 — segment mode is IMMUNE
    (ack-after-upload + builder idempotence). Prod's live path is
    safe; fix before any segment-mode rollback.

20. SEGMENT-SCAN BUDGET COUNTED DEAD COLUMNS (P0 at prod trace volume,
    FIXED in .63 — found 2026-08-03 during the .62 cutover verify, but
    PRE-EXISTING since segment enablement). Prod traces ingest ~120k
    spans/s; the ~15s live tail (builder fully caught up, oldest
    unbuilt segment 14s) decodes to >512MB of WHOLE-WIDTH batches, so
    ANY unconditioned query touching "now" (count(*), the traces UI
    default) hard-errored on the per-query scan budget — while the
    plan would only ever read `_timestamp`. Sealed-window queries and
    conditioned needles were fine (#13's row prune). Fix: project each
    kept batch to (plan projection ∪ condition fields ∪ _timestamp)
    BEFORE budget accounting (project_batch_to_needed). Subtlety: IPC
    stream decode slices all columns of a batch from ONE message-body
    buffer — `project` alone aliases it and the whole frame stays
    resident, so batches that shed columns are DETACHED with a `take`
    gather copy (only kept columns materialize; count(*) kept bytes
    become 8B/row). Zero-column projections preserve num_rows (arrow
    row_count option) for pure count plans. SECOND SUBTLETY (segment
    e2e caught it): a plan that reads `_source` (star selects) gets
    batches WHOLE — `_source` is synthesized from every stored column
    when the batch doesn't materialize it (segment frames never do);
    projecting hollowed star hits to bare `{_timestamp}`. Verified on
    prod post-.63: traces live-5m count serves.
    .64-.66 FOLLOW-THROUGH (owner hit the error again in the UI —
    star ORDER BY _timestamp DESC LIMIT reads _source, whole rows,
    537MB tail): (a) top-n trim — running n-th-newest _timestamp
    threshold (monotone; ties kept; superset semantics) trims batches
    whose surviving rows are KNOWN matches (new PrunedBatch::
    Exact/Whole/Dropped; Whole batches never trim and never feed the
    threshold); (b) THE LIMIT RIDES IndexOptimizeMode::SimpleSelect —
    empty_exec.limit() is None for the UI shape (DataFusion keeps
    fetch above the sort); live logs caught .64's trim never engaging
    (0 skipped, 258k kept records). Plumbed from the rule, DESC only;
    (c) wave-parallel fetch+decode (DECODE_WAVE=4) — segments are
    MIXED-STREAM, so even logs queries sequentially decoded the
    trace-heavy tail: measured flat 1.5-2.5s/querier under EVERY
    live-window query, ~0.5s after; (d) segment skip: once the
    threshold locks, segments with max_ts below it skip fetch/decode
    — gate is None-or-Condition::All (WHERE-less plans carry
    Some(ALL), live logs showed 0 skips until .66); (e) segment
    scan_size now = post-trim held bytes (batch-capacity summing
    double-counted the shared IPC body: 269GB claimed for a 15s
    tail). PROD BATTERY (60m window, median-of-3, cache off, 100-min-
    old all-L0 prefix vs o2's long-compacted history): ui_star_live
    obs 1629ms vs o2 2978ms (was ERROR — now WINS); count 1342/326;
    histogram 1213/321; svc_agg 1098/512; needle 1104/989; match_all
    883/501; logs_star 884/143; logs_token 775/147. Remaining obs gap
    = all-L0 sealed side (no deep merges/blooms yet — heals as
    compaction seals) + whole-tail decode on no-limit classes.
    Residual BY DESIGN: non-_timestamp ORDER BY star over the live
    tail genuinely needs the bytes (budget error text now says
    narrow/filter/fewer columns). FUTURE LEVERS: frame-level stream
    skip inside decode_segment (skip IPC parse of other streams'
    frames; zstd is one stream so bytes still decompress), DECODE_
    WAVE tuning, L0 file-list planning cost.
    .66 FINALS (same battery, prefix 3h old, compactor caught up to
    first-gen merges): ui_star_live obs 650ms vs o2 2756 (4.2x WIN,
    was ERROR at start of day); count 996/375; histogram 877/342;
    svc_agg 943/255; needle 1249/759 (was 3038/677 pre-compaction —
    heals); duration_cnt 2005/1672; match_all 991/655; logs_star
    796/106; logs_token 716/115. Skip evidence: 4 segments loaded /
    14-32 skipped per querier, tail scan 177-235ms (was 1.5-2.5s).
    Residual structure: o2 answers its memtable tail in-memory (~0)
    while obs decodes the S3 tail (~200-500ms floor), and the sealed
    side is first-generation merges (deep merges + blooms still
    building). ALSO SHIPPED IN .66 (correctness): the top-n heap
    clamps to the query window — frames keep on FRAME-level overlap,
    so rows newer than end_time (always present live) inflated the
    threshold and historical-window star+limit queries silently lost
    live-tail rows; heap observes start<=ts<end only, mask keeps the
    closed-range superset (segment e2e caught it; .65 in prod had the
    unclamped trim for ~1h — window shapes at risk only during that
    hour).

21. INDEXED-FILTER + GROUP-BY-ATTRIBUTE 10x GAP (found 2026-08-03,
    owner query: SELECT "code.function", count(*) FROM default WHERE
    service_name='llm-router' GROUP BY "code.function" ORDER BY c
    DESC LIMIT 100 — obs 12.1s vs o2 1.2s). The index worked
    (condition pushed, planning instant); 17s sat in DataFusion
    because code.function was NOT in column_store_fields — the
    group-by extracted _source JSON per matching row, and llm-router
    is the biggest service. L1 FIX APPLIED (config, prod+dev
    2026-08-03 ~20:1xZ): column_store_fields += code.function via
    settings PUT ({"column_store_fields":{"add":[...]}} — the flat
    list form 422s). New files store it typed immediately; HISTORY
    MIGRATES ONLY AS COMPACTOR MERGES REWRITE FILES (writer resolves
    cs set at write time) — expect parity with o2 as the window
    turns over. L2 (planned, structural win): IndexOptimizeMode::
    SimpleTopNTerms — recognize GROUP BY <single indexed field> +
    COUNT(*) + exact-term filter + ORDER BY count DESC LIMIT n; per
    file walk the group field's contiguous dictionary block range,
    popcount each term's postings against the filter bitmap, return
    the term->count map; leader merges maps, takes top-n. Answers
    entirely from the index (no docs columns), est. 200-500ms fleet-
    wide; with plist enabled, rank-seek the fat terms instead of
    decoding. Beats o2 structurally (they still read the column).
    WATCH: any other hot group-by attribute has the same L1 gap —
    add to cs on sight (each cs field costs one typed column per
    file). Bad-case battery: o2-ch-benchmark/prod-bench/badcases.py, baselines in
    o2-ch-benchmark/prod-bench/BASELINES.md (append-only, outside this repo).
    L2 SHIPPED as .67 (2026-08-03 ~21:3xZ, both envs): filtered
    single-field TopN/Distinct served from the term dictionary —
    field_value_counts_filtered = same eligibility + unfiltered-
    doc-count reconciliation as the unfiltered variant, then per
    value postings ∩ condition bitmap (one SIMD pass over the
    field's postings); recognizers now emit the rule for a single
    term-indexed group field WITH a condition (requires_no_filter
    apparatus deleted); per-file ineligibility still falls back via
    MissingColumn; MultiHistogram unchanged; partial-range files now
    serve exactly (postings∩time-bitmap) instead of falling back.
    MEASURED (owner query, 60m, mostly pre-cs-setting files): obs
    12,147ms -> 1,235ms median (warm 1.1-1.4s; first post-roll run
    6.8s cold), o2 770ms warm; rule engages
    (SimpleTopN(["code.function"],100,false) in follower logs); all
    12 non-null groups EXACTLY equal to the generic-path dual-build
    (forced via an extra aggregate), ordering matches o2 1:1.
    OPEN SEMANTICS (pre-existing, decide with owner then align):
    the NULL group — generic/DataFusion and o2 count rows lacking
    the field as a group (48.4M here); ALL index fast paths (incl.
    the long-shipped unfiltered ones) omit it; entangled with "":
    the dictionary counts empty-string values as a real group
    (24,293, currently rendered as a keyless hit) while the row
    store folds "" into null. Cheap exact fix if wanted: null_count
    = |bitmap| - Σ_{v!=""} counts, fold "" in, emit a null group.
    SEPARATE: obs counts ~13% above o2 uniformly across groups and
    paths at the same window = dual-write ingest-volume asymmetry
    (o2 drops?) — needs an ingest-parity audit, not a query fix.

22. LIVE-WINDOW COST REDUCTION, next wave (2026-08-04, owner-directed
    "reduce live file numbers"; measurements in o2-ch-benchmark/prod-bench/BASELINES.md):
    SHIPPED CONFIG: ZO_FILE_MERGE_THREAD_NUM prod 4→8 / dev 1→4
    (closed-hour seal was one job on one half-idle pod, bulge ~1-2h;
    prod PRs #359/#360, dev #208/#209) + ZO_VIX_PLIST_MIN_DOCS=8192
    compactor-only (writer was dark — owner caught it; readers fleet-
    wide since .62). FACTS ESTABLISHED: open-hour incremental merging
    is ALREADY ON (incremental.rs, threshold ≈9 files/node; the
    ZO_COMPACT_PENDING_FILES_TRIGGER docstring is STALE — no such
    knob exists); late old-_timestamp arrivals are safe (add_job
    re-triggers done hours; incremental counts late files); the open
    hour holds ~300 near-target files continuously, so the steady-
    state bulge should be only the remainder + late data — the 968-
    file reading was likely roll churn (bulgewatch measuring).
    ENGINE IDEAS (build after measurements):
    (a) STREAMING SEGMENT DECODE — scan currently zstd::decode_all's
        the whole payload per segment (that's why DECODE_WAVE
        multiplies memory, owner asked); the format is one zstd
        stream of per-frame-CRC'd frames → stream S3 body → zstd
        Decoder → IPC frame-by-frame → prune/project/trim → drop.
        Peak ≈ one frame; DECODE_WAVE can then scale to cores.
        Replaces the plan to merely bump the constant to 8.
    (b) TWO-LANE MERGE CLAIMING — workers claim oldest-first
        (backlog bias), so the query-hot newest closed hour waits
        behind any backlog; reserve one lane per compactor for the
        newest closed hour.
    (c) L0 BLOOMS AT SEGMENT BUILD — raw L0s (last ~10min) lack
        blooms until first merge; builder writes them ~free.
    Expected end-state: live-60m file count ≈ open-hour parts (~300,
    bloomed) + ~100 L0s + short tail; needle live → ~150-250ms.

18-RESOLVED (2026-08-03): THE BLOCK DICTIONARY SHIPPED AS THE FORMAT
    (owner: "fuck v3, build as v2, no legacy" — commits 699f03c24d +
    followups; pre-block .vix files hard-error at open; PROD DEPLOY OF
    THIS REQUIRES A DATA RESET, use the prefix-flip procedure).
    Layout: ~4KB prefix-compressed key blocks (never spanning fields) +
    resident index (16B/block meta + restart-compressed first keys,
    predecessor binary search); ordinals implicit; FST deleted
    (tantivy-fst retained only for regex/fuzzy automata run over
    decoded keys); exact lookup = index + ONE block.
    MEASURED (same box, same protocol, all caches off, page cache
    dropped; dataset 109.7M rows/202 files — HARDER than the old-dict
    run's 100M/55 files, and #19 struck the bench driver again: the
    compact restart replayed WAL, +9.7M dup rows, so values are not
    cross-comparable but perf is conservative):
    - count-full  cold  3,154ms (old dict 192,400ms -> 61x; demand
      ~130MB vs 23.4GB -> ~180x less), warm 75ms (o2: 166/78ms on
      450 small files — same band cold, parity warm)
    - hist-full   cold 167ms / warm 130ms (o2 424/366 — 2.5x better)
    - hist-straddle 126/112ms (o2 243/247)
    - and-control cold 220ms / warm 168ms (was 2,757ms gated,
      10,707ms pre-gate)
    - needle trace_id cold 24ms / warm 13ms (bloomed)
    - PER-FILE STRUCTURAL PROOF (identical 1.7GB/24.19M-term file
      class, in-memory): cold TokenAnyField 682ms (FST) -> 1.39ms
      (blocks) = 491x; warm 0.66ms -> 0.75ms (unchanged); open
      2.6ms -> 1.5ms (footer-only); 76 FSTs -> 145,444 blocks.
    - ingest rate unchanged (25.6k vs 27.1k rec/s, run variance).
    Remaining related items: (a) fetch gate LANDED (e8fb2f4111);
    prod S3 gate sizing note stands. (b) point-block get_ranges
    coalescing LANDED (01e625c261) — correctness-verified; NEUTRAL on
    the local box (3.6s vs 3.15s, noise — as the ladder audit
    predicted), it is an S3-round-trip lever. (c) COUNT-FULL COLD
    CEILING ANALYSIS (2026-08-03, after measuring a paged-index
    prototype dead-end): cold-full is INDEX-LOAD bound — ~1MB
    fk+meta per 450MB file, ~200MB aggregate at 100M-row scale;
    paging the index LOSES to bulk at these sizes on BOTH backends
    (locally 1 sequential MB beats ~300 point probes; on S3 one GET
    beats 300). Real levers, diminishing returns: (i) meta
    delta-varint (16B/block -> ~5B) + fk tightening ≈ 2x index
    shrink -> cold ~1.6s-class; (ii) full compaction convergence
    (bench compactor STALLED at 202 x ~450MB files for 2h — second
    merge generation never triggered at ZO_COMPACT_MAX_FILE_SIZE=
    4096; investigate the job generator's re-merge criteria);
    (iii) operationally, prod's persistent disk cache makes this a
    once-per-NEW-file cost (~1MB) — 3s-class fully-cold counts are
    a bench-box artifact, not a prod steady state. (d) #19 fix is
    URGENT-adjacent: it contaminated two bench datasets this week.

18-HISTORY (superseded analysis, kept one cycle for the record):
    COLD TOKEN/NEEDLE QUERIES (rewritten
    2026-08-02 after full attribution; supersedes three earlier
    partial narratives from the same day: "superlinear decode" WRONG
    — decode is linear SIMD ~27ms/16M ids; "FST dict storm is the
    491s" WRONG — the dict phase is idx_took=33s of the 486s cold
    query; both corrections measured, not argued).

    MEASURED ANATOMY of the cold cliff (100M/55 merged files, caches
    off, page cache dropped, `count(*) WHERE match_all('failed')`,
    82.4M hits):
    - took=486.5s, took_detail: idx_took=33s, search_took=453s.
    - The 453s is the DATAFUSION SCAN BRANCH: 50 of 55 files fell
      back because concurrent dictionary cell fetches (27 field-seeks
      x 55 files, 18.83 GB demanded in 1504 fetches) contended
      through the cache ladder until single fetches hit the 30s
      ZO_VIX_FETCH_TIMEOUT -> retry -> fallback -> those files were
      SCANNED (scan_size 218 GB attributed). A slow 12MB index fetch
      converts into a 2GB/file data scan ON THE SAME CONTENDED DISK
      — catastrophic economics, and the .60 eval-bail never sees it
      (the timeout path bypasses the bail comparison).
    - Warm truth: count-full 30ms (doc_count-served, zero postings),
      warm histogram 76ms (SIMD decode of all 82M ids included) —
      the index format is NOT the warm bottleneck at this scale.
    - Per-file dict truth (probe_dict_shape_of_bench_file, one 1.7GB
      merged file, 1.92M rows): 24.18M unique terms, 76 row-group
      FSTs (ZO_VIX_RG_TERM_BYTES=8MB default), cold in-memory
      TokenAnyField count 682ms, warm 0.66ms. FST lookups are fast;
      match_all costs one seek PER indexed/fts field (obs has no
      `_all` shadow field by design).
    - Fresh-file needle variant (564 un-bloomed files, trace_id
      equality): 1765 fetches / 3.24 GB / 27.2s cold — same
      fetch-demand mechanism, needle flavor; blooms fix it
      completely once built (5ms), but logs-type streams have NO
      default bloom fields (set bloom_filter_fields explicitly;
      bloom_ver=-1 "not applicable" rows need a reset to 0 after
      adding fields) and blooms only exist post-compactor.

    LEVERS, re-ranked by the attribution:
    a) LANDED (commit e8fb2f4111, 2026-08-03): global fetch gate
       ZO_VIX_FETCH_CONCURRENCY (default 16) acquired BEFORE the
       ZO_VIX_FETCH_TIMEOUT window opens — queue wait can no longer
       manufacture timeouts, so the timeout->scan fallback only fires
       on real hangs. Re-benchmarked same data, default 30s timeout:
       cold count-full 486.5s -> 192.4s (zero fallbacks/timeouts),
       two-token AND cold 10.7s -> 2.76s, counts byte-identical,
       warm unchanged. The residual 192s cold = 23.4GB dict-cell
       demand at ~122MB/s effective ladder throughput -> (b)+(c)
       below are the remaining cold levers. (A deeper form — folding
       real-hang fallback into eval-bail economics — stays open but
       is no longer the cliff.)
    b) LADDER AUDIT DONE (2026-08-03): the ladder is NOT slow —
       raw-hardware floor for the same pattern (24GB, 12MB random
       reads, 16 threads, cold page cache) measured 146MB/s = 164s
       vs the ladder's 192s for 23.4GB (~15-20% overhead, fine).
       The bench box's cloud volume IS the cold bottleneck; my
       "2GB/s NVMe" assumption was wrong. Consequences: (i) on this
       box the ONLY remaining cold lever is DEMAND reduction (fewer
       bytes per dict lookup — the rg-quantum trade, parked by
       owner); (ii) on PROD (S3 backend) aggregate GET throughput
       scales with concurrency — ZO_VIX_FETCH_CONCURRENCY=16 is
       sized for disk backends; consider 32-64 on S3 queriers when
       .62 rolls (the gate exists to stop timeout-manufacture, not
       to throttle S3). Coalescing per-field cell fetches into one
       ranged multi-get per file remains worthwhile (fewer
       round-trips on S3) but is latency-, not bandwidth-, bound
       work.
       O2 BASELINE ON THE SAME PROTOCOL (2026-08-03, its 100M/450
       files, same box/disk, caches off, page cache dropped):
       count-full cold 166ms/warm 78ms; hist-full 424/366ms;
       and-control 119/80ms. Cold exact-token demand is KBs/file
       (tantivy two-tier dict: block index + one block per lookup)
       vs our whole-rg FST loads -> ~1000x demand gap; the parked
       rg-quantum change (8x) cannot close it (owner paused it;
       probe test reverted). WE WIN ALL WARM CLASSES on the same
       protocol (counts 3x, hist-full 5.6x, hist-straddle 5x).
       => THE structural cold lever: a BLOCK-INDEXED EXACT-LOOKUP
       dictionary tier (resident/cacheable block index; one ~KB
       block read per exact lookup; FST kept for prefix/regex/fuzzy)
       — design target: cold exact-token ~= warm, 166ms-class here.
       Value caveat: o2 counts differ (80.27M vs 82.36M; AND class
       diverges more) — fts field-set + tokenizer differences, so
       cross-system values are indicative only.
    c) TOKEN-HASH SIDECAR — REJECTED BY OWNER 2026-08-03 ("we have
       a lot of terms and hash will be big"): the earlier ~2-4MB/file
       sizing used FRESH-file term counts; the measured merged file
       has 24.18M unique terms -> ~12B/term = ~290MB/file (~17% of
       file size). Dead. REPLACEMENT lever: shrink the dict fetch
       QUANTUM — dict cells are field-aligned and a point lookup
       touches one row group, but ZO_VIX_RG_TERM_BYTES=8MB means
       every field-seek drags ~8MB. Smaller rgs cut per-lookup
       demand proportionally (trade: more FSTs, less prefix-sharing,
       more directory entries — MEASURE with the 20M isolation file
       at 8MB vs 1MB before recompacting anything).
    d) BLOOM-AT-INGEST for equality-indexed fields (fresh/unsealed
       hours currently pay the needle storm until the compactor
       blooms them; compact-disabled deployments never get blooms).
    PLIST CONTEXT (#15, all four stages in tree): ranks beat the
    bitmap 2.6x (histogram) / ~36x (windowed count, 1.15ms vs
    41.6ms) per file at 16M-doc terms and remove the per-eval bitmap
    allocation; writer costs zero ingest rate. It is the
    postings-side complement to (c)'s dictionary-side fix, and the
    prerequisite for rank-seek intersections. System-level today its
    delta is small because warm decode was already ~76ms — its value
    grows with per-file postings size and QPS.
    Bench-harness note kept: the July harness runs both obs archs
    with ZO_COMPACT_ENABLED=false — env-symmetric, effect-asymmetric
    (o2-old builds its full index at ingest; obs blooms are
    compactor-deferred). Enable compaction for obs or ship (d)
    before comparing needle classes.
