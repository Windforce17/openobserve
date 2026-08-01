# obs engine — current state + backlog (single source of truth)

Supersedes NARROW-WAL-PLAN.md, FIELD-MAJOR-PLAN.md, DURATION-RANGE-PLAN.md
(deleted 2026-07-29; full history in git). Keep THIS file current.

## Shipped state (both envs, v0.93.0-vix-20260729.40, prefix obs-20260729/)

- Field-major (v2) dictionaries ARE the format: keys `{fid u16 BE}{token}`,
  1MiB field-aligned cells, per-field probe/walk ranges, match_all =
  per-fts-field seeks. v1 files readable per file (key_layout property,
  absent = v1; unknown values hard-error). Bloom keys PINNED to v1 byte
  form forever. Mixed-layout merges rebuild (= upgrade path). No env knobs.
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
6. RESOLVED 2026-08-01: vix-arch pushed to the owner's fork
   https://github.com/Windforce17/openobserve (branch vix-arch); push
   after every merge is now part of the ship procedure.
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
    #15 stage 1 LANDED (dark, 2026-08-01 evening): plist record codec
    (skip table: u32 first_doc_id + u32 blob_offset per 8 delta-blocks),
    rank_at() property-tested vs naive on every boundary shape, and the
    container plumbing (BLOB_TAG_PLIST / o2-vix-plist-v1 /
    PROP_PLIST_MIN_DOCS parse). Remaining stages, with the discovered
    integration map:
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
