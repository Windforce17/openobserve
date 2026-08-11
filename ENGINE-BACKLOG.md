# obs engine — current state + backlog (single source of truth)

Supersedes NARROW-WAL-PLAN.md, FIELD-MAJOR-PLAN.md, DURATION-RANGE-PLAN.md
(deleted 2026-07-29; full history in git). Keep THIS file current.

## Shipped state (both envs, v0.93.0-vix-20260803.63, prefix obs-20260803/,
## db obs20260803 — BLOCK-DICT CUTOVER 2026-08-03, old prefix orphaned)

- THE BLOCK DICTIONARY is the only readable dict layout (18-RESOLVED
  below: ~4KB prefix-compressed key blocks + resident restart index;
  pre-block files hard-error; FST deleted). Keys stay field-major
  `{fid u16 BE}{token}`; match_all = per-fts-field seeks; bloom keys
  PINNED to v1 byte form forever. plist stages 1-4 in the binary; writer
  LIVE compactor-only since 2026-08-04: ZO_VIX_PLIST_MIN_DOCS=8192 as a
  direct env on the compactor workload (NOT in obs-env — verified in the
  prod pod 2026-08-10; this line previously said "writer dark", stale).
  CONSEQUENCE CORRECTED (2026-08-11, prod-ops #375 review caught the
  first wording as a P0): the fleet's rollback floor is .62 and it is
  HARD — but the cause is the BLOCK-DICTIONARY cutover (no legacy read
  support; the .69 incident: "cannot read/write block-dictionary
  .vix"; pre-cutover S3 prefix deleted 2026-08-05), NOT plist. Plist's
  own reader constraint is >= .61 (stages 2-3 shipped dark in .61,
  all read+merge paths) — weaker than and subsumed by the .62 line,
  and it never sets the floor. Below .62 = total read outage, never a
  rollback target; the floor never relaxes. Pin-site notes corrected
  in prod-ops #375. Stage-4b (ranged sub-record reads) still open.
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
   branch vix-arch. REMOTE NAMING since 2026-08-07: the fork is this
   box's ONLY git remote and it is named `origin` (the upstream
   openobserve/openobserve remote was removed; `windforce` was renamed
   to `origin`). NEVER push work branches to it — publishing is ONLY
   the squash procedure: `git fetch origin vix-arch` FIRST, commit-tree
   the current tree parented on FETCH_HEAD, author/committer `anonymous
   <anonymous@users.noreply.github.com>`, generic feature-summary
   message, fast-forward push only, NEVER identity/session links, never
   force-push. Chain head da862aa6f5 published 2026-08-07 (== tree of
   local 845243a404, the dev-verified .74 state; previous head
   df60f9091b). gh on this box holds both accounts: switch with
   `gh auth switch --user Windforce17` to push, then switch back to
   wangzhichen-manus.
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

## #23 storm-mode compaction: intra-hour merge parallelism + recovery ordering (2026-08-06)
The .69 wrong-base incident recovery exposed two structural limits under
backlog storms (numbers from prod, 2026-08-06 evening):
- ONE merge job per stream-hour runs at a time: a pathological hour
  (traces hour-14 reached 10,418 files after the freeze + L0 burst)
  drains in SEQUENTIAL ~batches on a single worker while ~60 of 80
  fleet worker slots idle. Batch groups within an hour are independent
  (prefix partitions) — they could fan out across workers/nodes.
- Claim ordering is all-or-nothing: offsets-DESC starves old re-pended
  heal jobs behind current-hour churn (~500 metrics streams resurrect
  every minute; dev needed a temporary fast_mode=false flip + capacity
  to drain), while id-ASC starves the open hour. A storm needs a mixed
  policy (e.g. reserve N slots for current-hour jobs, rest oldest-first)
  or per-job priority.
Context: open-hour apisix piled to 3,297 L0s (normal saw-tooth tops
~2,200) because its single-flight incremental job cycled against
~1 file/s production; battery vs o2 lost every per-file class 25-137x
on file-count arithmetic alone (per-file cost nominal ~9ms, plan 0ms,
fast paths engaged). Fix = the two items above; nothing engine-side
regressed.

## #24 per-stream L0 chunking + claim gating: small streams stop inheriting the fleet's file cadence (2026-08-07, in tree)
Root cause of the orbit services-view regression (traces/e2b_prod_logs:
obs 2.3-6.8s vs o2 0.2s on the unfiltered percentile agg): the L0
builder capped sub-runs on the run's AGGREGATE decoded bytes (128MB),
and every sub-run emits one file per stream present in it — so every
~128MB of fleet-wide traffic emitted a sliver file for EVERY stream.
Prod pushes ~1.3TB/hour ≈ 10.6k sub-runs/hour == e2b's observed ~10k
files/hour (~2MB avg, o2 held 9-81/hour for the same stream+hours);
4k files per 30m window did the rest on per-file arithmetic (engine
per-file cost nominal — the sealed yesterday-window replay WINS vs o2
235/651ms). Builders also claimed 1-2 segments at a time (poll beats
production rate), so claims — and per-stream files — came out
sliver-sized regardless of batch capacity. Fix (this tree):
- chunk_run_per_stream: the decoded-byte cap is per STREAM inside each
  contiguity run; every stream's chunk ranges tile the run's whole id
  span, closing on whole-segment boundaries only (leader dedup is
  per-stream, so streams cutting one run at different boundaries is
  sound). Chunk boundaries stay pure functions of the decode set —
  identical re-claims still reproduce identical keys.
- claim gate: builders wait for a full ZO_SEGMENT_BUILD_BATCH — or
  ZO_SEGMENT_BUILD_MAX_WAIT_SECS (default 15, 0 = legacy
  claim-immediately) past the oldest claimable segment — before
  claiming (claimable_stats uses the exact claim predicate; fails OPEN).
  Rows stay queryable through the segment tail while gated, so the gate
  costs no freshness. ZO_SEGMENT_BUILD_BATCH default 16→32; decoded
  claim RAM ≈ batch x ZO_SEGMENT_FLUSH_SIZE_MB held through the build.
Expected prod shape (~5 segments/s fleet): one full claim per ~6.4s →
any stream's worst-case file rate ~560/hour (e2b 18x fewer, metrics
1-record L0s collapse ~30x), and open-hour incremental merging keeps up
from there (10k/hr exceeded its capacity, 560/hr does not). Residual
lever if a stream still needs fewer: cross-claim per-stream
accumulation (defer a stream's build until size/age) — needs persistent
per-segment pending-stream accounting; deliberately not taken now.
Upgrade note: a builder crash mid-batch across THIS upgrade retries
with different (per-stream) keys; the retry overwrites l0_planned so
the old uploaded-but-unregistered objects leak untracked in S3 —
bounded to one in-flight batch per builder, same class as a decode-set
change between retries (pre-existing).
Watch: wal_segments sqlite suite flaked once alongside this work
(claim_pending decoded updated_at as TEXT — shared-DB test race,
pre-existing class, 6/6 clean after; prod meta is postgres).

## #25 filter-back over-inclusion + #26 build-sort memory (2026-08-07 post-.72 watch)
#25: a UI needle (`body='<full string>' AND error_type='...'`, 15m)
index-narrows to 0 rows in ms, yet ~370-450 files/querier still went to
the scan branch with the filter added back — files whose EVALUABLE
subconditions already proved empty (and files skipped by per-file
heuristics) should be statically excluded; equality on a TOKENIZED fts
field can only be VERIFIED by decode, but an empty token-intersection
is definitive-NO. Pre-.72 this cost 4.1s cold (the shatter multiplied
it); post-.72 it is ~1.1s — the residual is this per-file scan-branch
population. Candidate: per-file verdict enum {proven-empty, needs-
verify, no-index} instead of the global is_add_filter_back + nameless
returns.
#26: the L0 build external sort OOMs the DataFusion pool on FAT streams
even after ingester pool 2048→4096 (prod PRs #370-#372, retry rate
61/20min → ~7/25min, converges but wastes work): the "~3x decoded
input" peak estimate undershoots on wide schemas (logs/default = 1,542
fields — row-format conversion + string buffers), and on compactors
concurrent MERGES legitimately hold the auto pool (24G) with no
prioritization, starving 400MB build sorts. Levers: memory-aware build
admission (size concurrency by pool headroom), per-build dedicated
reservation, or width-aware chunk cap (shrink BUILD_CHUNK_MAX_DECODED_
BYTES when schema width > N). Ship state: .72 live both envs (see
obs deployment memory / prod PRs #368-#372); the shatter itself is
DEAD (logs/default open hour 5,506→~155 files, e2b 10k→~1.6k/hr raw
converging to tens post-merge).

## #27 top_n index path: 27k-tiny-fetch storm on wide timestamp-ordered LIMIT queries (2026-08-07, dev trace 019fdc1069ce76b1af33b1573f412ed0)
A 24h `ORDER BY _timestamp DESC LIMIT` (trace-list shape, unconditioned,
index_condition Some(ALL), top_n mode) on dev traces/default spent 104s
of a 105s follower response INSIDE search->vix: `index fetches: 27117
(232.96 MB), top_n hits: 70065, file_num: 0` over 78 merged files
(index size 4.33GB, 76% memory-cached) — ~8.6KB per fetch at ~3.8ms
effective each, while the actual batch decode was 32 batches x 45ms
(~1.4s). The volume is fine; the ROUND-TRIP COUNT is the pathology.
Hypotheses to measure (do NOT assert without instrumenting): (a) the
top_n rank path resolves per-zone-chunk plist pointers/ts chunks with
POINT reads that bypass the fetch-gate batching the point-block path
got in .62; (b) no cross-FILE early-stop — newest-first waves + prune
exist for SimpleSelect SCAN, but the index top_n path appears to
evaluate every file in the window (70k candidate hits collected for a
10k limit); (c) the 1024-entry block cache thrashes across 78 files'
restart blocks at this fan-out. Levers: coalesce per-file rank reads
(get_ranges), wave the files newest-first with limit-satisfied pruning,
count-aware chunk skip. PROD-RELEVANT (.72 has the same path; prod
merged files are the same 4GB shape). Repro: unconditioned 24h
timestamp-ordered LIMIT 10000 on traces/default, cold-ish cache.
Second specimen (same family, 12:02Z trace 019fdc1a9feb...): 24h exact
trace_id=... lookup, 0 hits, 10.1s leader total — visible followers'
exact seeks were 7-8 fetches/230-440ms each, one straggler follower ate
~8.5s (logs rotated before capture; consistent with serialized cold
per-file lookups). The get_ranges coalescing lever covers this variant;
wave-pruning does not (no limit to satisfy).
Third specimen (dev watch capture 12:59Z, trace 019fdc4cd8d0... — this
was the .73 work's own baseline replay): SimpleTopN(["trace_id"], 100,
false) over 24h, leader 117.6s COLD; one follower 361 fetches/4.71MB in
102.9s = ~285ms PER FETCH (fetch-gate/S3-client contention with the
same query's parallel work) — fetch reduction pays twice: fewer round
trips AND less self-contention. Warm repeats 1.2-1.6s. top_n hits
31020 for LIMIT 100.

ROOT CAUSES (code-verified 2026-08-07, .73 work):
- SPECIMEN 1+3 WERE SimpleTopN, NOT the timestamp-ordered SimpleSelect:
  the `top_n hits:` log text is printed only by MultiResult::TopN — the
  trace-list GROUP BY trace_id shape. Its per-file eval enumerates the
  ENTIRE field dictionary via scan_key_range = ONE ~4KB point fetch per
  dict block (~350 blocks/file x 78 files = the 27k storm), serialized
  through the 16-permit gate at S3 latency.
- The plain ORDER BY _timestamp LIMIT shape (SimpleSelect) HAD waves +
  pruning on paper, but partition_vix_files' time-partition transpose
  degenerates to file_groups:1 whenever files OVERLAP in time (every
  live window: l0_multi + hour merges) — no early stop, every file
  evaluated; pruning only trimmed the scan afterwards. Even disjoint
  layouts transposed into window-spanning groups that prune poorly.
- VixQuery::All (condition-ALL) paid an MB-class dict-INDEX fetch per
  ranged file via prefetch_query_fsts — a structure All never reads
  (sealed replay: 33 fetches/23.91MB idx phase for zero benefit).
- guard_matched_rows applied its percent bail to SelectCandidates:
  a small file's <=limit exact candidates got kicked WHOLE-FILE to the
  scan branch (sealed replay scanned 150k rows for a 10k limit).
- Fetch-count attribution caveat (evidence discipline): fetches issued
  through a reader memoized by an EARLIER query tick that query's
  counters — cold runs / fresh readers are the honest measurements.

FIXED IN TREE (ships as .73):
1. scan_key_range bulk-loads its block span (resident index bounds the
   last reachable block; missing-block runs -> ONE block_fetch_many,
   8MB chunks, small spans published to the block cache) — 27k point
   reads collapse to a few MB-sized round trips per file. Same batching
   for out-of-row plist pointer records in postings_union +
   field_value_counts_filtered (one fetch_many per file instead of one
   fetch per term).
2. best_first_waves replaces the transpose for SimpleSelect: files
   sorted max_ts DESC (min_ts ASC for ascend), doubling waves 4,8,...
   capped at target_partitions; existing suffix-bound pruner fires
   after wave 1 in the common case. Overlap only weakens bounds, never
   correctness; needle worst case pays O(log) sequential rounds.
3. Metadata pre-prune (SimpleSelect + condition-ALL): fully-in-window
   files' (min_ts, records) prefix-sum a bound T; files provably below
   the top-N are dropped BEFORE cache/open/eval — zero index fetches
   for them. Plus: All-queries skip the dictionary entirely
   (prefetch_query_fsts point-leaf gate).
4. SelectCandidates percent guard removed (candidates are limit-bounded
   and exactly merged; row_count mismatch check kept).
5. MERGE ARMOR: read_timestamp_columns now hard-rejects any merge input
   violating _timestamp DESC (both merge flavors) — the DESC invariant
   is load-bearing (declared file_sort_order, first/last-row stats,
   candidate selection) but was unrecorded and unchecked. Writer-order
   precondition VERIFIED for all three writer paths (L0 build, legacy
   mover, compactor k-way merge, + WAL parquet): everything writes
   GLOBAL per-file _timestamp DESC — the kickoff's "ascending"
   assumption was inverted, so "arithmetic tails" are arithmetic HEADS.
DELIBERATELY NOT DONE: time-waving SimpleTopN — count-ordered group-bys
need every file's contribution for the merged counts to be right;
early-stopping by time would bias counts, not just order (reply to the
dev-watch note in specimen 3). The structural TopN lever is a follow-up:
doc_count-heap top-k for UNFILTERED single-field TopN on string-only
fields (stream the field's doc_count ordinal range into a k-heap, then
resolve only the k winning keys — skips the value-enumeration walk AND
its allocations entirely); also the FILTERED dictionary TopN still
decodes every value's postings (fine for low-cardinality cs fallbacks,
atrocious for trace_id-class fields) — same follow-up family.
SHIP LOG: .73 rolled to dev 2026-08-07 ~14:30Z and was rolled back ~25
min later (argocd-dev-ops #231) — NOT a #27 defect: the roll's
SIGKILLed old pods exposed #28 below (query outage on ANY version).
Evidence captured in the .73 window before the rollback: TopN
trace-list cold 6,576ms on freshly-restarted queriers vs 117,757ms
cold on .72 (17.9x), zero merge-armor firings fleet-wide, segment
pipeline normal. Re-shipped as .74 = .73 + the #28 fix (dev-ops #232,
merged after answering the review's two P1s; image digest
sha256:68e16991dcd9..., engine commit 02a9d3bf15c5).
DEV-VERIFIED .74 (2026-08-07 ~16:0xZ, fresh fleet 0-5% cached,
traces/default 24h, median-of-3, log-line evidence in scratchpad
replay/after74*):
- TopN trace-list (GROUP BY trace_id LIMIT 100, the specimen shape):
  per-follower vix eval 27,117 fetches/103s -> 167-236 fetches/1.1-1.6s
  (~130x fewer round trips, ~70x faster); bytes rose 233MB->375-541MB
  by design (bulk spans; round trips were the pathology); warm result
  cache floor visible at 0 fetches/1ms. End-to-end 117.8s -> 8.5s on
  the cold fleet (warms with cache population); file_num: 0 both ways.
- SimpleSelect live 24h LIMIT 10000: metadata pre-prune "dropped 94 of
  98 files" / "80 of 86" BEFORE any IO; file_groups 1-2 post-prune;
  is_add_filter_back FALSE (was true) with row_nums ~10-20k exact
  winners (was 60k-150k over-inclusion + whole-file scans); per-follower
  vix 3-9 fetches/79-334ms (was 21-33 fetches incl. useless MB-class
  dict-index loads). took median 907ms -> 635ms live, 1272 -> 803
  sealed (cold 1939 -> 1087).
- Merge armor: 0 firings fleet-wide since roll. Segment pipeline
  normal (build batches in=17 built=17, tail seconds-deep).
- #28 verified live: the rollback's stale registrations produced
  "health check failed 3 times, remove it" on schedule and searches
  recovered; a force-killed (grace 0) ingester caused ZERO failed
  probes across 130s. (Synthetic-kill registrations cleaned via the
  normal death path; the fix covers the roll-orphaned class.)
PROD SHIPPED 2026-08-08 ~02:15Z (#373 merged b82a576ac1 by owner
instruction, admin merge; ROLLBACK NOTE (.74) added per review P1):
20/20 engine pods Ready in ~7 min, zero restarts from the roll, one
roll-stale registration evicted by the #28 fix ("remove it" x1),
residual health failures 0 within minutes, armor firings 0.
Prod probes: SimpleSelect 24h LIMIT 10000 = 1,214ms cold / 577ms warm
(idx 382ms -> 1ms) over ~1,700 files/follower. Trace-list 1h = 4.5s /
3.1s. Trace-list 24h = 186.6s + one querier OOM -> #29 below (NOT a
regression: pre-.74 the same query needed ~580k point fetches — it was
physically impossible, now it is merely pathological).
RELEASE ANCESTOR GATE now: git merge-base --is-ancestor 02a9d3bf15c5
HEAD (fleet commit for .74; supersedes 85399cceef).
BUILD NOTE: the workspace's datafusion-functions-json sibling now lives
IN-TREE at crates/datafusion-functions-json (VENDORED.md records base
rev 0df53d71 + the local negative-number patch) — the old external
checkout carried that patch unpublished and did not survive the box
move; the review_negative_numbers e2e-level test caught it.

## #28 cluster health sweep never evicts nodes an observer never saw healthy (2026-08-07, dev outage during the .73 roll — FIXED in tree, ships .74)
Symptom: after the .73 roll, every dev search failed — first 400s
("tcp connect error ... ConnectionRefused" against a SIGKILLed old
ingester's registration), then ~130s hangs once the dead pod's IP was
reused (SYN blackhole). The #231 rollback to .72 did NOT fix it: the
rollback's own SIGKILLed .73 pods left registrations the fresh .72
fleet dialed identically (probe measured 136s hang on .72).
Root cause (infra/src/cluster/mod.rs check_nodes_status): the failure
counter only incremented for nodes already in NODES_HEALTH_CHECK, and
nodes were only ADDED there by the success branch — so an observer that
never saw the node healthy (every pod after a full-fleet roll) skipped
it forever. Prod rolls masked it: long-lived observers had the entry.
Graceful terminations mask it too (the node dereg-s itself); only
hard-killed pods (probe kills, OOM, spot reclaim, grace-expiry SIGKILL
during rolls) expose it.
Fix (b1803460c1): entry().or_insert(0) in the failure branch — dead
nodes evict after failed_times (3) sweeps (~node_heartbeat_ttl/2 each,
~45-60s total) regardless of prior observations; a live-but-slow node
self-heals via its next keep-alive Put. Regression test:
health_sweep_evicts_nodes_never_seen_healthy.
Verify on .74: the #231 repro — hard-kill (SIGKILL) one ingester pod,
run any search: expect <=1 min of failures, then "health check failed 3
times, remove it" in observer logs and searches recover.
RESIDUAL (follow-up candidates): (a) during the ~1 min pre-eviction
window fan-outs still dial the dead node — a short gRPC/HTTP CONNECT
timeout + retry-elsewhere would shrink the blast radius to ms; (b) the
NATS KV obs_nodes entry outlives the process (no lease-TTL expiry) —
eviction is per-observer view only.


## #29 unfiltered high-cardinality TopN is an allocation bomb once un-throttled (2026-08-08, prod evidence — FIXED IN TREE 2026-08-10, all three levers, ships next build)
Evidence (prod, 24h unconditioned GROUP BY trace_id LIMIT 100, trace
019fdf2e65cb71c19e2fb57f2284bdb4): 186.6s total; per-follower vix eval
9.3-26.7s with 4,969-11,343 fetches / 8.5-18.7GB over 1,659-1,759 files
(the #27 coalescing WORKING: ~3-7 fetches/file where pre-.74 needed
~350/file = ~580k total, i.e. the query was previously impossible);
top_n hits 479k-1.08M per follower; file_num 504-1,149 handed to the
scan branch (is_add_filter_back true) which ate the remaining ~160s.
obs-querier-3 OOMKilled (exit 137, 02:27:53Z) during the probe: the
value-enumeration walk allocates millions of Vec<u8> keys per file and
eval_concurrency (64) runs walks CONCURRENTLY — .74's fetch fix removed
the accidental serialization that kept peak memory low. Realistic UI
windows are fine (1h = 4.5s/3.1s, no OOM).
DO NOT probe 24h unconditioned trace-list on prod until this lands.
Levers, in order:
1. doc_count-heap top-k for unfiltered single-field TopN (stream the
   field's doc_count ordinal range into a k-heap, resolve only the k
   winning keys, gate on string-only fields, fall back to the walk
   otherwise) — kills BOTH the CPU and the allocation bomb; the #27
   deferred lever, now prod-motivated.
2. Enumeration cap in field_string_value_terms: past
   max(inverted_index_topn_max_group_num, K) values, return None (the
   caller falls back) — bounded per-file memory even where lever 1
   does not apply (filtered variant, Distinct).
3. Re-examine why 500-1,150 files/follower fell to the scan branch on
   prod (dictionary exactness reconciliation shortfalls at this scale?
   instrument the None reasons in field_value_counts before assuming).

FIX SHIPPED TO TREE (2026-08-10, local perf pass; baselines from an
8x2M-row merge_bench corpus merged to one 16M-row/3.58GB file with 16M
distinct trace_ids — the exact prod shape):
- BASELINE (the bomb, measured): ONE unfiltered field_value_counts walk
  of trace_id = ~970ms wall, +1.87GB peak RSS per file per eval; the
  perf profile is literally page-fault/clear_page_erms/realloc (the
  16M x Vec<u8> churn), x64 eval_concurrency = the querier OOM.
- LEVER 1 (implemented): reader.field_value_top_k — the field's STRING
  ordinal ranges come from the resident dict index (+<=6 boundary block
  probes; numeric-tagged sub-range excluded by construction, matching
  the walk's is_numeric_value_token semantics incl. its documented 0x01
  residual), doc_count streams over a NEW zero-alloc
  RowSelection::Range scan into a bounded (count, ordinal) heap
  (ordinal order == key order, so ties resolve identically to
  truncate_top_k), reconciliation sum == key-term doc_count kept
  verbatim, and ONLY the <=max_groups winners' keys resolve
  (keys_for_ordinals: binary-search blocks, ONE batched fetch via the
  new load_dict_blocks — scattered sibling of load_dict_block_span).
  Same-file numbers: 43ms wall (22x), +92MB peak (20x, = streamed
  doc_count decode chunks), kept=1000 exact winners.
  reader.field_value_head serves SimpleDistinct head/tail the same way
  (resolves exactly `limit` keys). Ranged-mode budget test: top-k over
  the 100k-distinct fixture = 8 fetches / 1.2MB (walk: ~48 fetches +
  full dictionary materialized) — ranged_field_value_top_k_matches_
  walk_at_scale.
- LEVER 2 (implemented): field_value_counts_filtered gained a cap
  (max(inverted_index_topn_max_group_num, k-overfetch)); the
  enumeration STOPS at cap+1 keys and returns None -> the caller falls
  back (docs column / scan). scan_key_range closures can now
  early-terminate. filtered_top_n's post-truncation became dead code
  and was removed (the cap bounds enumeration by construction).
- LEVER 3 (implemented): every dict-unavailable fallback now logs
  (field + reason class), incl. the silent-before _source re-parse
  last resort in both TopN and Distinct arms.
- GUARDS: field_value_top_k_and_head_match_walk (differential vs the
  walk on every eligibility shape: fts/numeric/absent/empty-string
  refusal parity, truncation-set parity vs the truncate_top_k
  comparator incl. the svc 3-way count tie) + the at-scale ranged test;
  all existing suites green (search 981, vortex_index 174, incl.
  test_unfiltered_collectors_match_docs_collectors and the
  cached/ranged parity harness).
- Residual: the +92MB transient is the vortex doc_count chunk decode;
  bounded and 20x better, revisit only if 64-way concurrency shows
  pressure. Invalid-UTF-8 values tie-break by raw bytes rather than
  the lossy-decoded string — divergence only when counts tie across
  values that differ solely in invalid UTF-8, accepted.

ROUND 2 (same day, cross-class A/B follow-up; query_bench.rs is the
harness, runs identically on older commits): first verified NO other
query class regressed from the #29 work (pre/post medians at parity,
Contains -7%; results byte-identical). Then two more wins landed:
- DISJOINT-COUNT: count() of a multi-ordinal SINGLE-FIELD non-fts leaf
  (Prefix/Contains/Regex with field: Some) now sums the doc_count
  column instead of postings-union + popcount — one raw value term per
  doc per field makes the term doc sets pairwise disjoint (numeric-
  tagged included; fts token terms overlap and keep the union; a
  scoped leaf on a pure-fts field cannot resolve at all, and dual-
  marked fields are gated). count Prefix over a 30-term/16M-doc field:
  21.2ms -> 583µs (36x), zero postings IO and zero bitmap. Guard:
  count_matches_eval_popcount_across_leaf_shapes (every leaf shape
  incl. the union-mandatory any-field ones).
- CONTAINS SCAN: contains_bytes was naive windows(); the Contains arm
  now hoists ONE memchr::memmem::Finder for the whole dictionary scan
  (SIMD), and the case-insensitive arm reuses a lowercase buffer with
  an ASCII fast path instead of from_utf8_lossy+to_lowercase PER KEY
  (32M allocs on a 16M-key field, identical Unicode-fold semantics
  kept for non-ASCII tokens). Full-field Contains over 16M keys:
  1.046s -> 288ms (3.6x). memchr added to vortex_index (workspace dep
  already).

SHIPPED .76 BOTH ENVS + PROD ACCEPTANCE PROBE (2026-08-11 ~04:00Z,
v0.93.0-vix-20260811.76 = engine 20862c321f; dev-ops #235 clean
Approve, prod-ops #375 Approve after a P0+3 P1 cycle ON MY ROLLBACK
NOTES, not the code — see the floor correction above; the push
script's new auto prev-tag resolved cross-date .75 correctly on its
first live run). Rollouts 13/13 dev + 21/21 prod, zero restarts.
PARITY: all six replay md5s identical .75 vs .76 on BOTH envs incl.
the new 1h GROUP BY trace_id anchor; its cold run 108->56ms dev,
1182->454ms prod; dev logs show the new path serving: "Some(ALL)
found top_n hits: 1000, index fetches: 0 (0 B), took: 0 ms".
ACCEPTANCE PROBE (the 2026-08-08 OOM query): 6h unconditioned
GROUP BY trace_id = 11.8s, RSS peak ~4GB, clean. 24h = NO OOM, NO
restarts, RSS peak 10.1GB/24Gi (on .74 this OOMKilled obs-querier-3)
— the allocation bomb is DEFUSED — but the query still exceeds the
200s flight timeout: per-follower vix eval 12-41s / ~0.5-1.55M top_n
hits / 5.8-15.5GB index fetches with is_add_filter_back=true and
108-1291 files/follower left for the scan branch, which eats the
rest. Lever-3 instrumentation fired ZERO dict-refusal lines — the
scan-branch files come from the ROUTING gates (window-straddling
files are excluded from the index-only TopN route by file_in_range,
and docs-column files take the bitmap+column path), NOT from
dictionary refusals. NEXT (#29 tail, new item-worthy): extend the
index-only TopN route to straddling files (time-clamped per-value
postings counting or zone-chunk rank cuts) and re-examine the
per-follower fetch volume (299k fetches / 9.9GB on one follower —
#27-adjacent). The 24h prod probe rule softens: memory-safe since
.76, still times out — avoid on prod dashboards, fine as a manual
probe.

## #30 roaring row-id selections: resident vix bitmaps compressed (2026-08-10, SHIPPED .75 BOTH ENVS + VERIFIED)

Every surviving index selection was a DENSE BooleanBuffer (num_rows/8
bytes regardless of match count): 240KB per 1.92M-row file to memoize 3
matched rows, capping the 256MB vix result cache at ~500 worst-case
entries (a needle query touches 564-1,700 files — ONE query could churn
the whole budget), and a wide query's SCAN_SELECTIONS registry held one
dense buffer per selected file for the scan's lifetime (1,700 x 240KB
≈ 400MB resident per in-flight query — #28/#29 memory-pressure class).

Change (roaring 0.11.4, pure Rust, already in-tree via vortex-scan =
zero new deps): new config::meta::stream::RowIdBitmap (RoaringBitmap +
num_rows universe, containers settled via optimize()). Everything
RESIDENT holds it: FileSelection::Rows, CacheEntry::RowIds,
VixSearchResult::RowIdsSelection, VixScanSelection. The reader eval
pipeline stays DENSE (SIMD AND/OR untouched, vortex_index crate
unchanged); one from_dense at the guard boundary, so only survivors
(≤ skip threshold, 35%) ever convert. Cache hits materialize via
to_dense() O(matched) — replaces the 512KB deep clone per
straddling-file hit. SimpleSelectPruner builds sparse directly (was: a
dense num_rows-sized alloc for ≤limit winning rows). Vortex scans hand
off Selection::IncludeRoaring (vortex 0.79's mask is roaring-backed;
the old Buffer<u64> materialized 8B/matched-row only for vortex to
re-compress it — an 82M-row match cost ~650MB transient). Legacy
parquet plans coalesce runs() off the sparse iter.

Measured (MEDIAN of 3, release, 1.92M-row universe, dense = 240,000B):
needle x3 = 70B (3,429x) · 2% scattered = 76,362B (3.1x) · 2% one-run =
57B (4,211x) · ~30% scattered (the guard ceiling shape) = 246,040B
(1.0x parity; optimize() clamps the run-store blowup that measured 1.4x
without it). Conversion: from_dense 18µs-8.6ms (needle→ceiling),
to_dense 67µs-2.2ms — noise vs the postings decode they bracket (82M-id
term = 76ms warm). Effective cache capacity for the needle class:
~500 entries → effectively unbounded (70B against a 256MB budget).
Diagnostic: bench_row_id_bitmap_shapes in config stream.rs tests
(--ignored --nocapture, release).

Gates run on the final tree: cargo check --workspace --tests clean;
unit config+search 2,957 pass incl. new oracles (dense↔sparse identity,
runs() vs set_slices(), poison-proof straddle-cache hit test);
integration BOTH segment modes green (EXIT=0 in-log, logs in session
scratchpad).

Deliberately NOT converted (assessed 2026-08-10): on-disk postings
codec (bitpacked deltas + skip table beat roaring serialization and
carry the rank seeks; format-frozen), SBBF blooms (dense by design),
PromQL signature sets (random u64 — roaring strictly worse), the
postings_union inner loop and eval AND/OR (dense SIMD already right at
these widths), file_id_list proto (frozen wire format). Follow-ons,
separately motivated, in rough value order: (a) RoaringTreemap for
query_by_ids' 3x HashSet<i64> builds + 2x difference() over
time-clustered bigserial ids (core/src/file_list.rs query path; mind
the negative WAL pseudo-ids); (b) writer terms-map hybrid (the
BTreeMap<Vec<u8>,Vec<u32>> hits 15-19GB on a 10GB rebuild — roaring
only above a doc-count threshold; measure the term-frequency
distribution first); (c) promql topk/bottomk HashMap<i64,
HashSet<usize>> dense-index sets. Ship note: cache admission unchanged
(max_entry_size still 512KB) but entries admit at compressed size —
expect VIX_RESULT_CACHE_MEMORY_USAGE to fall and the hit counter to
rise on dashboard workloads; verify with follower log lines per the
bench gate, never timing alone.

DEV SHIP LOG (2026-08-10 ~12:40Z, v0.93.0-vix-20260810.75 = engine
12973498ac, argocd-dev-ops PR #233, review verdict Approve/no-P0):
rollout 13/13 pods clean, 0 restarts, 0 crashloops; the only post-roll
ERROR lines were health-check-then-evict of the terminated .74 pods
(the #28 sweep working). CORRECTNESS: fixed 5-query replay (sealed
02:00-03:00Z window, script ~/obs-v75-replay.sh on ops-dev) —
needle count/select on trace 27dac0d2a32f06da, match_all histogram
(60 buckets, first 3272), ns top-n (argocd 42,261), vpc REJECT count
18,159 — ALL FIVE result md5s byte-identical .74 vs .75
(78e4aebfe1/d6b7c35d1e/d9d4391f3a/f8f1b2130d/3fb58a5a9f). EVIDENCE
LINES: warm replay served index-only — "found count: 18159 ...
index fetches: 0 (0 B), took: 0 ms", histogram hits 88816 fetches 0,
top_n hits 14 fetches 0. MEMORY: fresh .75 caches after replay +
live dashboards = 5.3-8.2 KB per querier vs 17-22 MB steady on .74
(~500 B/entry avg vs 240 KB dense needle entries). Fresh ingest live
(485k rows/10min queryable). FOLLOW-UPS: (1) review P1-2 — with
~70 B needle entries the binding cache limit flips from the 512 MB
byte budget to MAX_ENTRIES=100000 (~7 MB); soak dev, check the evict
trigger, then raise MAX_ENTRIES in its own argocd PR with evidence.
(2) push_image.py prev-tag digest gate FIXED in-tree (60374182cd):
auto-resolves newest ECR -vix- tag, aborts instead of silently
skipping — the .75 build itself needed a manual digest check because
the old NN-1 default missed across the date change. (3) prod roll
after dev soak.

PROD SHIP LOG (2026-08-10 15:35Z merge, prod-ops PR #374, verdict
Approve/no-P0; P1s fixed in-PR: pin-independent .74 rollback wording +
new .75 rollback note "target .74, no format boundary, floor stays
.24" + configmap comment drift vs dev twin): rollout 21/21 clean,
0 restarts, only #28-sweep evictions of terminated .74 pods.
CORRECTNESS: prod 5-query replay (same sealed window, script
~/obs-v75-replay.sh on ops, trace f514a6e8e0d1158b) — ALL FIVE md5s
byte-identical .74 vs .75 (4628858f87/7c0ee82564/8566c5d0fe/
b9b354339b/f6c5fa19f6; anchors: 15 spans, histo first 18003, topn
chatgpt4google-prod 625243, REJECT 400047). EVIDENCE: warm replay
index-only across all five followers ("found count: 40003...132874,
index fetches: 0 (0 B), took: 0 ms"; top_n hits 36-45 fetches 0).
Fresh ingest 28.5M rows/10min queryable. Querier caches 16-19KB,
~50% hit rate, gc_total zero (no evictions — MAX_ENTRIES bump not
yet needed on prod; dev soaks 1M via argocd-dev #234 with the
querier obs-env-rev annotation added, engine default 100k->1M in
tree). CACHE-LIMIT DERIVATION (recorded at the dev pin): per-entry
bytes vs MAX_SIZE include 2x key len (cache.rs entry_footprint);
needle ~370B/entry -> 1M ≈ 370MB, conservative 500B -> ~500MB ≈ the
512MB budget — never raise MAX_ENTRIES past 1M without raising
MAX_SIZE. Fleet state: BOTH envs v0.93.0-vix-20260810.75 = engine
commit 12973498ac (ancestor gate target).

## #31 writer term-accumulation dominates index build and merge-rebuild RSS (2026-08-10, local perf pass — MEASURED, next perf item)

From the 2026-08-10 perf pass (bench_build_core_file 1M rows, k8s-logs
shape): push(term-accum) = 3.6-4.0s vs finish(encode) = 40-50ms —
term accumulation is ~99% of build wall and does NOT scale with
encode threads (it precedes them). merge_bench over 8x2M rows:
index-merge fast path 28.4s / VmHWM 7.55GB; --rebuild 229s / VmHWM
9.66GB (70.3M terms). The BTreeMap<Vec<u8>, Vec<u32>> accumulation
(writer.rs terms map, spill.rs exists because of it) is the bound —
candidates: arena-backed keys + hash map with sort-at-finish,
per-chunk sorted runs merged at flush (the spill machinery
generalized), or reserving via the known term distribution. Needs a
dedicated profile of push_term before choosing. Perf tooling now on
the box: perf 6.12 + inferno + rustfilt (demangle AFTER collapse),
release-profiling needs CARGO_PROFILE_RELEASE_DEBUG=true AND
CARGO_PROFILE_RELEASE_STRIP=none (plain release strips). Corpus
generator: merge_bench gen (8x2M traces shape, ~23s/file); harnesses:
bench_unfiltered_value_walk (walk-vs-top_k A/B), scan_bench, matrix
log in the 2026-08-10 session scratchpad.
