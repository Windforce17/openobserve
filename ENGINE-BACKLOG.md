# obs engine — current state + backlog (single source of truth)

Supersedes NARROW-WAL-PLAN.md, FIELD-MAJOR-PLAN.md, DURATION-RANGE-PLAN.md
(deleted 2026-07-29; full history in git). Keep THIS file current.

## .120 +2h OUTCOME 2026-08-24 ~10:15Z — M29 CONVERGING
- Zombies eliminated: Building 189,688 -> 351 (real work), pending 31, sweeper retiring tombstones (22.6k rows total left in wal_segments).
- Batch amortization restored: in=61 built=61 skipped=0 gone=0, ~1.25 l0_files/segment (was 4.63 slivers, ~4x better).
- Merges 352/15m fleet-wide (debt sweep + cutter alignment live). Standing unmerged L0: 1,109,155 -> 616,984 in ~5h
  (~-98k/h net) -> steady state in ~6h. All pods 0 restarts.
- Rebuild gate STAYS 1: system converges without it; evaluate against steady state later per its contract, not now.

## M29 LANDED + .120 SHIPPED 2026-08-24 (~08:1xZ prod roll)
- M29 correction: merge workers were NOT under-fed at measure time (877 completions/30m); the 1M standing L0
  is driven by the 404 claim zombies (189,688 kill-era Building rows = 98.8% of claim batches) destroying batch
  amortization: 4.63 sliver L0/segment vs ~0.25. Plus two real merge throttles: closed-hour visitation cadence
  (dead zone excluded hot hours) and group-cutter cutting wider than the 128-file consumer cap (stranded tails).
- Fixes (engine 9f78be9a0d0d): fenced 404 tombstone (uploader PUTs before registering -> 404 = truly gone;
  kill switch ZO_SEGMENT_BUILD_404_TOMBSTONE), merge-debt sweep lane (ZO_COMPACT_MERGE_DEBT_INTERVAL=60s),
  cutter/width-cap alignment. Harness: 50k-L0 backlog 99.6% drained at 41.4 files/s sustained (330 merge
  starts/15m at gate=1) vs 18.3% plateau before; 0 partial batches. Gates green; dev #298 clean; prod #462.
- PROD FIRST-LIGHT: whole claim batches tombstoned (96/96...), per-item ERROR flood 0 (was 722k/30m),
  zombie pool 189,688 -> 108,677 within minutes of the roll. Expect L0 arrivals to drop 15-20x once the pool
  clears; ~91k files/h merge capacity then drains the 1.07M backlog in <1 day.
- Lifecycle backstop: obs rules already at Days=1 (owner tightened during a silent SSO session) — AWS-side
  program COMPLETE. Engine retention primary (verified deleting, incl. ancient misdated ranges).
- NEXT: +2h outcome read (zombies=0? sliver rate? standing L0 falling?) -> then rebuild gate 1->2 per contract.

## M29 CHARTERED 2026-08-24 ~05:1xZ (owner: "ok do it") — MERGE THROUGHPUT + CLAIM ZOMBIES
- Post-drain reality: 1,109,155 unmerged L0 (54k beyond 1d retention, ~1.056M in-window) vs 28k gen-2; fleet
  completes ~3 gen-merges + starts ~13 rebuilds per 15m vs ~110+/15m capacity at gate=1 x 10 pods — the merge
  JOB GENERATION under-feeds workers (~1/10th); the gate is not saturated, so gate revert alone is pointless.
- Secondary: [SEGMENT:BUILD] 404 claim zombies — lifecycle-expired wal_segment objects retried forever
  (kill-era rows), log-noise ERROR floods + wasted cycles.
- M29 agent in /home/zhichen/work/m29 (base 3845fe9bfb): quantify every throttle, fix job generation to
  saturate gate=1 (target >=100 gen-1 merges/15m fleet-wide, no pinned-knob raises), terminal 404-claim
  resolution with incremental drain + house log discipline; repro harness before/after; ships as .120.
- THEN: rebuild gate 1->2 per its contract (arithmetic: ~5GB/rebuild at max-file 1024), budgets re-eval.
- Lifecycle backstop 3d->1d still queued on SSO (engine retention primary since #461).

## DRAIN COMPLETE + PHASE-2 SHIPPED 2026-08-23 ~05:0xZ
- PENDING = 0 (oldest 0h) at ~04:47Z — the fleet-reset drain saga is OVER. ~23h kills=0 on .119.
- Along the way on .119: DF-cap pin reverted (#459, 12G pin permanently stuck a fat metadata-merge shape;
  auto shared pool completes them at ~12.3G, 0 sort errors since); kill-era corpse sweep ran clean
  (all invalid files dated the 08-21 storm hours, zero today-dated).
- Phase-2 (#460, roll-119d): M27 canary RETIRED per contract (attributed M28 in 25min; verified the fix),
  compactor 9->10 (cap restored); ZO_FILE_MERGE_THREAD_NUM 4->8 per written contract (met since M23/.113);
  MIMALLOC_PURGE_DELAY=0 dropped (premise falsified by M27/M28; ~+11% merge wall recovered).
- Retention flip 365->1 (#461, own deliberate PR per the pin's owner-design contract, roll-119e): ENGINE
  retention now PRIMARY at 1d (deletes rows+objects); S3 lifecycle (3d) is the BACKSTOP — tighten to 1d
  via aws (needs SSO) after the engine flip is verified deleting cleanly.
- DELIBERATELY KEPT per contracts: build budgets (8192/4096 — auto-wave headroom arithmetic tightened by
  DF-auto), rebuild gate=1 + max-file 1024 (vortex-internal transit re-measure on .119 still owed).
- Owner flags open: ALLOWED_UPTO narrowing (low stakes post-M28), #442 ingester HPA. Old-DB drops due:
  obs20260803 2026-08-24, obs20260817 ~2026-08-25 (owner call, no dump).

## PHASE-1 PIN REVERTS 2026-08-22 ~09:4xZ (contracts met on the 4h clean window)
- Prod #458: ingester breaker-75 override DROPPED (M21b verified holding kills=0 — its exact contract; global 90
  applies, retires the 503 shedding storms) + compactor fetch-decode 4->8 (M20b landed, DF cap stays). roll-119b.
- Dev #297: dev compactor profiler pin removed (validation done; prod canary carries acceptance; also kills the
  watcher PANIC false-positives from symbolized profiler stack strings — that filter caveat stands for the canary).
- 3h49m read that authorized this: 9/9+canary+5/5 pods 0 restarts; canary report#229 single life, 158TB flow,
  live_est 8-9.6GB breathing; DRAIN FLIPPED built15m=14400 vs arrivals15m=9948 (~+300/min net; .118 was -254/min at
  1/3 the throughput); pending 476k (peaked in the SSO-blocked overnight), oldest 41.5h vs 72h line (aging lane
  drains oldest-first). Lifecycle STAYS 3d until pending~0.
- REMAINING per-condition: pending~0 -> lifecycle 3->2->1, canary delete + 9->10, budgets->auto eval, workers 4->8,
  purge-pin took_ms-measured revert, rebuild-gate/max-file transit re-measure on .119, retention 365->1 own PR.

## .119 ACCEPTED 2026-08-22 ~06:20Z — PROD KILLS = 0 (the OOM war is over)
- Shipped: push 05:25Z (SSO gap delayed ~13h; builds were done 08-21 17:11Z), dev #296 05:2xZ, prod #457 merged 05:32:51Z.
- DEV: the 15h ingester WAL-replay CrashLoop spiral (176 restarts) BROKE on the roll — same bug confirmed. Residual
  dev ingester OOMs = the PRE-EXISTING undersized-ingester ingest-path class chewing 15h of WAL debt (exit 137, no
  panics, no zero-progress-guard errors, progress each life) — converges as debt drains; NOT an M28 defect.
- PROD 40min read: 9/9 main compactors 45-47min / 0 restarts (was ~15 kills/45min, ~20min median life);
  canary report#47 single life (longest ever ~25min), live_est BREATHES 1.4-13GB and drains between bursts —
  flat floor at 32.9TB alloc_flow. Ingesters clean post-roll. One .117-RS corpse (krzj8) pending GC, ignore.
- One .118 straggler PANIC during the roll: FileSegmentSource::open<BlobReadAt> (unwind-guarded) — watch for ANY
  recurrence on .119; none seen post-roll.
- NEXT: multi-hour clean window -> consolidated pin-revert PR (each pin per its written contract: workers 4->8,
  fetch-decode 4->8, budgets->auto EVALUATION, DF cap stays as true-bound, breaker 90, purge pin per took_ms
  contract, rebuild gate + max-file-size ONLY after re-measuring the vortex-internal transit on .119 — M28 removed
  the unbounded term those pins were absorbing); canary deletion + compactor 9->10 + dev profiler pin removal;
  lifecycle 3->2->1 as pending drains; retention 365->1 own PR. Drain should now FLIP (builds no longer die mid-batch).

## M28 LANDED 2026-08-21 (~17:1xZ) — ROOT CAUSE WAS A ZERO-PROGRESS LOOP, .119 IN CHAIN
- NOT retention: a >1MiB value in a dict-probed column can never enter a fresh BytesDictBuilder ->
  encode_chunk encodes 0 rows -> DictStreamState::encode retries the identical remainder on a fresh
  builder forever, allocating codes+values+builder per iteration (~50-280MB/s per stuck write, core pinned).
  Lease expiry hands the SAME poisoned data to the next pod -> fleet-synchronized churn. Entry point:
  L0 segment builds (write_core_file_from_sorted_batch) — prod merges are 100% docs-passthrough, which is
  why merge-only harnesses never saw it. M20b's wide ALLOWED_UPTO lets >1MiB values through (flagged).
- Fix: vendored vortex-array + vortex-layout 0.79.0 (crates/, [patch.crates-io] — datafusion-functions-json
  pattern): fresh dictionary ALWAYS admits its first entry (oversized = 1-entry run, closes on next value);
  vortex-layout zero-progress guard = loud write error, never a hang. Dict encoding + term indexing ON.
- Proof: unfixed repro hangs (RSS +280MB/s, gdb stack == M27 prod stacks); fixed completes, 0.000MB residue;
  1,114,112-byte value round-trips; byte-identity 20/20 sha256 (.vix+.vxi) on fixed-seed corpus; segbuild
  wall 3.60s vs 3.77s (noise). Gates green incl. integration both modes. M26 harness back to documented floor.
- Commits on canonical: 94acdbb2a4 / 850ebd062d / 9abc91907c / 93c82811ec. Chain .119 running.
- INSIGHT: the dev ingester WAL-replay CrashLoop spiral is the same bug (replay -> same >1MiB value -> loop
  -> OOM -> replay). The .119 dev roll IS the dev-recovery test — if ingesters go Ready, the parked recovery
  proposal is moot.
- Post-acceptance program (kills=0 window on .119): consolidated pin-revert PR (rebuild gate, max-file-size,
  DF cap, fetch-decode, budgets, breaker, purge pin, merge threads — each per its written contract), canary
  deletion + compactor 9->10, dev profiler pin removal, lifecycle 3->2->1, retention flip 365->1 (own PR).

## M28 CHARTERED 2026-08-21 ~14:05Z — THE FLOOR IS NAMED (canary attribution, 25 min after landing)
- Canary birth-to-death correlation: RSS 1.6->40.3GB over 16min (~40MB/s = the fleet floor slope); live_est_total
  0.7->29.8GB in LOCKSTEP, samples_live 86->453 monotonic. The floor IS live Rust heap (purge pin acquitted mimalloc).
- The climber (ranks 1,2,4,5,6 = ~21GB est, counts never drain across merges):
  vortex_layout::layouts::dict::writer — DictStreamState::encode / BytesDictBuilder::{encode_varbinview,reset}
  via DictionaryTransformer's stream-of-streams (kanal codes tx + oneshot values tx per dictionary run).
  BOTH emitted codes buffers AND reset()-emitted values arrays retained pod-lifetime. Tiny allocs (0.2-2KB), huge counts.
- M26 harness's 0.004MB/job "noise" = this leak at toy vocabulary scale. Suspects: leaked per-run task parked on
  vortex_io runtime / Arc cycle in async_stream graph / chunk collection surviving file finish. vortex 0.79 = crates.io
  dep -> fix = vendored patch (datafusion-functions-json pattern) or engine-side if provably equivalent.
- M28 agent in /home/zhichen/work/m28 (base 081bd7b57d): root-cause + fix + high-cardinality repro + byte-identity +
  gates -> ships as .119. Constraint: dict encoding + term indexing stay ON; no budget-pins-as-fix; no write-path regression.

## .118 SHIPPED 2026-08-21 (M27 sampling heap profiler, inert) + prod canary
- Engine 32a7241a1015 (M27 a88a98f235 + note). Gates all green (units, integ both modes). Pushed both registries 13:35Z.
- Dev #295: profiler ACTIVE on dev compactor — report#1 in 60s, demangled arm64 stacks, reentrant_skips=0.
  Dev top live: rank1 arrow-IPC decode held via segment_wal::decode_segment <- fetch_and_decode_one (est 9.8GB);
  rank2 vortex DictStrategy varbinview builders (rebuild transit — the gate=1 pin's quantity, now sited);
  rank3 arrow_select::take under sort_record_batch_by_column <- build_stream_files; rank4 synthesize_source realloc.
- Prod #456: .118 + obs-compactor-canary (1 replica, env 64MB) + main compactor 10->9 (cap held) + .117/.118 rollback note.
- Next: read canary attribution -> the ~35-50MB/s floor fix ships as .119. Then pin-revert program per contracts.

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

## fork publish 2026-08-11: chain head 3f52e1f0e1 (parent da862aa6f5)

Squash of the .76 tree (roaring selections #30, #29 all levers,
disjoint-count, SIMD Contains, benches, digest-gate fix) to
Windforce17/openobserve vix-arch. First publish through the repo-local
pinned credential helper — no gh account switch. Anonymous author +
committer verified via the GitHub API post-push.

## #32 dense-condition index evals burn minutes before the skip guard fires (2026-08-11, prod evidence — FIXED IN TREE same day, ships next build)

Evidence (prod, trace 019ff0bea74973308c2578a9a39cbb7e, 12:14Z): a
dashboard query (nested agg: per-minute x user_id counts under
service_name=api-aggregator-server over a wide traces window) spent
122s and 167s per follower in the vix phase decoding postings across
~1531 files at 0% cache, only for guard_matched_rows to discard at
avg percent 100 -> full scan anyway. The eval held vix semaphore slots
throughout: the work-group queue backed up to 190s total wait and the
12:13-12:17Z window shows a fleet-wide pileup (47 queue waits >=10s in
7d, most in such episodes; 14.5k searches/day baseline).
ROOT CAUSE (corrected same day after probe-driven diagnosis; the
original density hypothesis was WRONG — the condition is 0.01% dense
on its own window, svc-only counts serve index-only file_num:0):
`error_code` carries OVERSIZE raw values on some rows, so the builder
skips them and marks the field PARTIAL (writer.rs max_raw_term_len ->
partial_fields) in most files of the window. evaluate_vix_index's
uses_partial_fields check then bails THE WHOLE FILE
(Skipped{percent:100}) for any condition touching the field — even
though the query's OTHER conjuncts (service+operation equality) are a
0.01% index needle and dropping the partial conjunct is superset-safe
under top-level AND with add-filter-back. avg percent 100 = every
file PartialFields-bailed; the 122-167s = ~1531 cold reader opens per
follower just to learn that, file by file. Levers, in order:
1. Treat partial-field conditions like FtsOnly conditions: skip the
   CONJUNCT (has_skipped=true, filter re-applied), evaluate the rest —
   superset-safe for top-level AND conjuncts; keep the whole-file bail
   only for shapes where dropping is not superset-safe (inside OR/NOT).
   Turns this query into an index needle + re-filter: minutes -> s.
2. Fleet-level early bail: K consecutive whole-file Skipped{100} on
   the same condition -> skip the remaining files without opening
   them (saves the 1500 cold opens; generalizes the projected-bytes
   bail at mod.rs:288).
3. The original pre-decode density guard stays valid for TRUE dense
   conditions (doc_count bounds: Exact=doc_count/records, And=min,
   Or=sum) — cheaper skip for genuinely dense shapes.
4. Ops: alert on "total wait in queue took" >= 30s (episode smoke);
   investigate WHY error_code holds oversize values (ingest-side
   truncation or a max_raw_term_len bump may fix the data itself).

FIX SHIPPED TO TREE (2026-08-11, owner ratified "must query by invert
index; accept the correctness design"):
- IS [NOT] NULL EXACT-SERVE on partial fields: removed from the
  whole-file partial bail — key terms are emitted under every partial
  cause (oversize string, oversize numeric canonical text — both pinned
  by partial_field_key_terms_survive_value_skips; the field-id-overflow
  writer path documents it verbatim). The incident query (svc AND op AND
  error_code IS NULL) is now FULLY index-served: 0.01% needle, no skip.
- CONJUNCT-GRANULARITY partial skip: new FieldCap::Partial (checked
  before has_term_capability — a partial field still HAS terms, they are
  just incomplete); value-term conditions on partial fields skip THEIR
  conjunct (superset + re-applied filter, the existing FtsOnly
  machinery), never the file. The whole-file bail survives only for
  match_all/fuzzy over a partial FTS field (token taint has no
  named-field granularity). Guard: partial_field_conditions_serve_at_
  conjunct_granularity (e2e all four shapes incl. the lone-conjunct
  AllConditionsSkipped error path).
- FLEET SKIP BAIL: the old give-up counter RESET PER FILE GROUP (the
  actual reason the incident burned 122-167s: 1531 files rediscovered
  the bail group by group) — now query-scoped; AllConditionsSkipped
  per-file errors count toward it; and optimize modes get a skip-rate
  bail on the existing eval_bail flag (>=32 sampled, >=90% whole-file
  skips -> remaining files short-circuit to the scan branch), mirroring
  the projected-bytes bail.

## #33 _source read-path audit: two silent whole-file paths fixed, one structural surface filed (2026-08-11, owner-requested audit)

Audit (full report in the 2026-08-11 session): every query-time _source
read funnels through 3 primitives (read_source(rows) row-bounded;
read_docs_column WHOLE-COLUMN; scan_docs_opts rows-or-All). Findings:
- FIXED — C-5, GROUP BY _source / SELECT DISTINCT _source silently
  materialized the ENTIRE _source column into ONE arrow array
  (read_docs_column via simple_top_n/dict_group_counts; multi-GB alloc +
  i32-offset-overflow hazard): collect::missing_docs_column now treats
  _source as always-missing for optimize modes and single_group_field
  excludes it — the scan branch streams it per chunk instead.
- FIXED — C-1, the source_top_n/source_distinct last resort accumulated
  an UNCAPPED group map (the #29 allocation-bomb shape reintroduced:
  one string per distinct value, 16M-distinct field = GBs): hard cap
  MAX_SOURCE_GROUPS=1M -> descriptive error -> the caller's per-file
  error path degrades the file to the scan branch (DataFusion streams
  the same group-by bounded). Walk now logs rows + distinct count.
- FILED — C-3 (largest un-instrumented surface, structural): explicit
  SELECT/filter over non-column_store fields extracts from _source per
  row; match_all filter re-application projects EVERY fts field; row
  bounds are lost whenever a file has no index selection (skip
  threshold 35%, bails, give-up, eval errors, no condition). vix_format
  has ZERO fallback logging. Next levers: (a) one per-scan log line
  when needs_source && selection is None (rows + extracted fields);
  (b) narrow the match_all re-filter projection to the fts fields the
  condition can actually match; (c) revisit LIMIT pushdown into
  scan_docs (only channel backpressure stops unfiltered star scans
  today). C-4 (no-fast-path aggregations) rides (a).
- Verified bounded (no action): select-star response-side parse
  (result-size bound), memtable/parquet _source SYNTHESIS (per-batch,
  the 2026-07-30 OOM fix), segment-WAL (byte-budgeted), compaction
  (streamed, off the query path). NOTE: with #32's conjunct-skip,
  partial-field files now RETAIN index selections (superset), so their
  scan-side _source extraction became row-bounded too — the two fixes
  compound.

## #34 superset bitmap memo collided with the exact result-cache key: extra rows on repeat queries (2026-08-11, caught by manus-reviewer on prod-ops PR #380 BEFORE prod merge — FIXED IN TREE same day, ships .78; .77 never reached prod)
- Defect: the .75 pre-clamp bitmap memo (straddling files, key =
  generate_cache_key(cond, &None, file, None)) stored bitmaps WITHOUT
  the `!has_skipped` gate the main result put has. That key is
  byte-identical to the MAIN result-cache key of a covered-file
  no-rule query on the same (condition, file), and the main hit path
  (mod.rs ~:809) serves entries as exact — has_skipped=false hardcoded,
  no reader open to re-derive it. A straddling eval whose condition
  carried a skipped conjunct memoized a SUPERSET; a later covered
  no-rule query with the same condition consumed it as final rows —
  EXTRA ROWS, silently, repeat-query paths only.
- Exposure: .75/.76 reachable via fts-only-field conditions (rare);
  #32's conjunct-granularity skips made superset bitmaps routine
  (every partial-field value-term condition), so .77 widened it to the
  incident query class itself. Prod never ran .77 (PR #380 held); dev
  ran .77 ~14:03Z-EOD 2026-08-11 — exposure window noted, dev-only.
- Fix: gate the bitmap-memo put on `!has_skipped` (superset evals just
  recompute per window; they had NO memo at all pre-.77, so still a
  strict win). NoMatch memoization stays ungated — 0 superset rows
  implies 0 true rows, exact by implication. Comments at both key
  sites now state the collision invariant.
- Pin: superset_bitmap_is_never_memoized_under_the_collision_key
  (mutation-checked: removing the gate fails the test at the is_none
  assert). Invariant: ANY entry under a clamp-free no-rule key is an
  exact whole-file condition bitmap.
- Credit: the reviewer flagged it conditionally from the argocd side
  ("if the memo can't distinguish exact from superset entries, treat
  as P0") without engine-repo access. It couldn't distinguish; it now
  never needs to.

## #35 segment-scan budget: hard 512MiB cap failed filtered recent-data queries on shared streams (2026-08-11, prod user report — FIXED IN TREE same day, ships .79)
- Report: SELECT * + three AND equalities over last-15min on
  default/logs/default failed 0.03% over the hard 512MiB budget
  (537,035,584 vs 536,870,912). Root cause chain: (1) segment batches
  carry write-time present fields only; (2) the prune guard was
  all-or-nothing — ANY condition field absent from a batch's schema
  bypassed pruning for that batch ENTIRELY (even the service_name cut),
  so on a shared stream nearly every other service's batch counted
  fully against the budget while provably unable to match; (3) SELECT *
  disabled column projection; (4) the budget was a hardcoded const and
  a hard error.
- Fix (owner-directed: "ignore and warn, we need recent data"):
  - ZO_SEGMENT_SCAN_MAX_BYTES pinned knob (default 512MiB, 0 = no
    warning): crossing logs ONE warning per scan and CONTINUES.
  - Hard stop only at half the pod's cgroup memory limit (never below
    the soft knob) — ~12Gi on prod queriers, 23x the old failure point;
    "unlimited unless it would endanger the pod".
  - Drop-on-absence: a positive null-rejecting AND-conjunct
    (Equal/StrMatch/In/NumericCmp non-negated/Regex/IsNotNull) whose
    field is absent from a batch's schema drops the batch outright —
    absent field means no row can match; index and SQL semantics agree.
    Complement shapes (NotEqual, negated In/NumericCmp, IsNull, Not) and
    structural/fts shapes (Or, And, MatchAll, Fuzzy) never drop.
  - Partial-conjunct pruning: conjuncts whose fields ARE present filter
    the batch even when others must be skipped; result classifies Whole
    (never Exact — skipped conjuncts mean survivors are not known full
    matches, so top-n trimming stays off them); zero survivors of the
    evaluated subset still drop the batch.
- Tests: absent-field drop + IS-NULL/negated-In non-drop pinned in
  test_prune_batch_by_condition_saves_needle_queries; partial-conjunct
  narrowing/Whole-classification/empty-drop in
  test_prune_batch_partial_conjuncts_narrow_but_never_claim_exact; soft
  crossing continue in push_within_budget_soft_crossing_keeps_the_batch
  _and_continues; hard-ceiling error retains the original enforcement
  test (message now says "ceiling").
- For the reporting user's query shape: every non-temporal batch drops
  before budgeting (wf_task_queue_name absent), temporal batches prune
  to the queue's rows — the scan accumulates KBs, no warning fires.

## SHIP LOG v0.93.0-vix-20260811.78 (engine 1ebf3e9f1b) — #32+#33+#34, BOTH ENVS, VERIFIED (2026-08-11)
- Pipeline: .77 (7f08bf3098) built+pushed and dev-verified but SUPERSEDED
  pre-prod by the #34 review catch on prod-ops #380; .78 = .77 + the memo
  gate. Dev-ops #237 + prod-ops #380 (retargeted, REST-merged on owner
  instruction after the P1-fix push invalidated the stale approval).
  Rollouts: dev 13/13 ~14:59Z, prod 22/22 ~15:05Z, zero restarts.
- Dev exposure note: dev ran .77's memo defect ~14:03-14:59Z only; the
  .78 rollout cleared all in-memory caches, so no poisoned entry survives.
- Incident acceptance (the 2026-08-11 12:14Z class, service_name +
  operation_name + error_code IS NULL on default/traces):
  - 1h Aug-5 slice: cond_count md5 076c848792 c=34440 in 1.49s (was
    24-34s on .76); incident_sql md5 ca6e9efd6f in 1.97s (was 42-45s).
    ~20x, byte parity with the .76 ground truth.
  - Follower logs: is_add_filter_back: false on every follower (IS NULL
    served EXACTLY via key terms per #32), 0 "skip vix search" lines
    fleet-wide; repeat runs hit the result memo with index fetches: 0
    (0 B) at ~200ms — the exact-only entries #34 guarantees.
  - 24h original window: cond_count COMPLETES in 90.3s, c=965,387 (on
    .76 nothing completed — 200s flight timeout). The FULL aggregation
    form (GROUP BY minute, user_id) still exceeds the flight ceiling:
    1h 2.0s -> 6h 33.3s (completes, 29 groups) -> 24h >280s. Bounded by
    the OPEN #33 C-3 surface (per-row user_id extraction from fat
    _source docs over 965k selected rows), NOT the index path. Levers:
    C-3(b) narrowed projection, or promote user_id to a docs column for
    traces. Filed under #33 open half.
- 6-anchor prod replay: needle_count 4628858f87, needle_select
  7c0ee82564, filtered_count f6c5fa19f6, topn_traces a79e0ef7b9 —
  byte-identical. histo_matchall and topn_ns DRIFTED (8566c5d0fe ->
  c268158f58, b9b354339b -> 63432be4fb): late-arriving rows in the
  sealed 2026-08-10 02:00 window (scan_size 89165->89292, first bucket
  18003->18004, top-1 namespace count identical, md5-stable across 3
  re-runs, only that stream moved) — DATA DRIFT, not engine. New anchor
  set recorded above; anchors on busy log streams can drift when late
  data lands — prefer needle/trace anchors for hard parity.
- Ops notes: ops jump host recycled again (obs-v75-replay.sh recreated
  from transcript; obs-v77-incident.sh survived). During acceptance a
  karpenter consolidation wave (5 nodes tainted karpenter.sh/disrupted)
  replaced the router pod + querier-4 mid-flight — one empty-body curl,
  no engine fault, fleet re-formed clean (the #28 class behaved).
  Node join logs still print upstream "version: v0.92.0-rc1" — always
  verify rollouts by IMAGE TAG.
- Dev replay: all 6 dev anchors byte-identical to .77's morning set.
- Rollback: target .76 (revert bump; KNOWINGLY reinstalls the #32
  incident class). .77 is never a valid target (memo defect).

## SHIP LOG v0.93.0-vix-20260811.79 (engine efdbde67e3, image stamp 0e8afe8bc3 doc-only delta) — #35, BOTH ENVS, VERIFIED (2026-08-11)
- Pipeline: dev-ops #238 + prod-ops #381 (owner standing instruction:
  direct merge; prod repo base branch is MASTER, not main). Pins
  ZO_SEGMENT_SCAN_MAX_BYTES=536870912 in both configmaps. Rollouts:
  dev 13/13, prod 22/22, zero restarts. (SSO expiry stalled the first
  push attempt; also push_image runs aws BEFORE docker — a fail-fast
  cred check up front would have said so in 1s instead of after the
  image build.)
- Acceptance (prod, live last-15min windows on default/logs/default):
  - The reporting user's EXACT query (SELECT * + 3 AND equalities on
    per-service fields): HTTP 200 in 3.1s, no budget error. total=0 is
    CORRECT — the dump job had finished; the live window's queue
    distribution confirms /_sys/user-dump-cdn-parts-queue/5 idle.
  - Same 3-conjunct shape against an ACTIVE queue value
    (/_sys/default-worker-tq/15): cnt=7, byte-exact vs the GROUP BY
    ground truth, 1.9s — absence-drop + partial-prune are correct on
    live data, not just fast.
  - Follower scan lines: 271k-367k records examined per follower ->
    kept 0-1,526 BYTES (pre-.79 this shape kept the whole live backlog
    and died at 512MiB). Zero [SEGMENT:SCAN] soft-budget warnings.
  - 6-anchor replays byte-identical on BOTH envs (prod set incl. the
    2026-08-11 drift-updated histo/topn_ns anchors).
- Fleet ancestor gate advances: 0e8afe8bc3 (was 1ebf3e9f1b/.78).
- Rollback: revert the bump -> .78; pre-.79 builds ignore the knob pin.

## #36 query admission + cancel-on-disconnect: the OSS global query queue serialized the whole cluster (2026-08-11, owner-directed after the .78 acceptance surfaced 33.8s needles behind 33.3s scans — FIXED IN TREE, ships .80)
- Defect 1 (throughput): OSS `check_work_group` took ONE cluster-wide
  dist-lock (`/search/cluster_queue/global`) per query and flight.rs
  held it until the query finished — cluster concurrency = 1. Evidence:
  needle_select 335-709ms clean vs 33.8s behind the 6h aggregation
  (33.3s); needle_count 24.9s behind the 24h count's tail; prod
  querier.yaml fossil "1h histograms queued ~4s before running".
  Enterprise solves this with Short/Long WorkGroups — feature-gated
  out of our build; the OSS fn even receives the file list and
  ignores it.
- Fix 1 (owner call: "remove the lock; 429 at max; default 30"):
  node-local counted admission — ZO_QUERY_MAX_CONCURRENCY (default 30,
  0 = unlimited) permits per LEADER node (SQL + promql), try_acquire
  only: past the limit the request fails IMMEDIATELY with
  RatelimitExceeded -> existing mapping -> HTTP 429 (+ x-o2-error
  header). No queue exists anymore; wait_in_queue reads ~0 (field kept
  for took_detail compat). Effective cluster ceiling ~= 30 x querier
  replicas, router-spread. promql rejection no longer downgraded to a
  500 (pass-through fix).
- Defect 2 (waste): client cancel/disconnect did NOT stop the query.
  actix drops the handler future, but BOTH detach points kept running:
  mod.rs:196 tokio::spawn(cluster::http::search) and flight.rs
  DATAFUSION_RUNTIME.spawn(run_datafusion) — JoinHandle drop DETACHES.
  A canceled 24h scan burned followers to completion.
- Fix 2: AbortOnDrop guard (search::utils, next to AsyncDefer) at both
  detach points: owner-future drop -> task abort -> leader's flight
  client streams drop -> tonic RST_STREAM -> follower encoder streams
  drop (FlightEncoderStream::Drop already runs clear_session_data +
  defer-lock release — pull-based execution cancels with it). Logs
  "[trace_id] search task aborted on drop (client disconnect or
  cancel)". Deliberate abort()/join() paths stay silent.
- Bonus fix: QUERY_RUNNING_NUMS was inc'd (flight.rs) and NEVER dec'd
  on OSS — a pre-existing gauge leak. The gauge now lives in
  AdmissionGuard (inc on admit, dec on Drop) — leak-proof across
  success/error/timeout/disconnect. PENDING dec balanced on the
  admission-rejection path in flight.rs.
- Caveat (filed): the HTTP2 streaming/multi-search path still detaches
  its per-query tasks (streaming/mod.rs:909) and cancels only when a
  channel send fails — delayed teardown on disconnect. Next lever:
  AbortOnDrop tied to the response-stream lifetime there too.
- Tests: admission_rejects_past_the_limit_and_recovers_on_drop (30
  admit, 31st = RatelimitExceeded naming the limit, freed slot
  re-admits); abort_on_drop_cancels_the_task (drop -> task locals drop
  within 1s); abort_on_drop_join_completes_normally. Existing http.rs
  test already pins RatelimitExceeded -> 429.
- Config: ZO_QUERY_MAX_CONCURRENCY pinned "30" both envs;
  ZO_FEATURE_QUERY_QUEUE_ENABLED deprecated/unread since .80.

## #36 addendum (.81): OSS cancel plumbing — the dev .80 acceptance proved the drop-guards alone don't fire on H1 oneshot disconnects
- Live .80 evidence (dev, one querier): 40 concurrent memo-defeating
  queries -> EXACTLY 30 admitted + 10x HTTP 429 with the intended body
  (code 20012, names the limit and knob) — admission VERIFIED. But the
  30 admitted queries' curls timed out at 120s (client disconnect) and
  ZERO "aborted on drop" lines appeared: hyper/H1 does not drop a
  oneshot handler future mid-flight (it notices at write time), so the
  AbortOnDrop guards never trigger from H1 disconnects. The zombies
  held their permits to completion and 429'd bystanders (8 extra
  rejections observed) — dev drained on its own in ~minutes.
- Root cause of the gap: ALL cancel machinery was enterprise-gated —
  SEARCH_SERVER registry, the flight.rs abort arm (OSS: pending()
  forever), the query_manager cancel endpoints (403 on OSS), the gRPC
  cancel_query handler (unimplemented), and SearchStreamGuard's
  disconnect action (a debug log saying "requires the enterprise
  build").
- Fix (.81): minimal OSS mirror — core/search/cancel.rs abort registry
  (DashMap trace_id -> oneshot sender; RAII deregistration; prefix
  matching for "{trace}-{job}" sub-queries), flight.rs registers and
  races the receiver in its select (a dropped registration pends, never
  cancels), gRPC cancel_query fires cancel_local (both cfgs), the
  query_manager cancel endpoints and cancel_query_internal now work on
  OSS via the (un-gated) cluster fan-out, and SearchStreamGuard cancels
  on stream drop in both builds.
- What this yields: explicit cancel API works (DELETE
  /api/{org}/query_manager/{id}/cancel); UI/streaming searches
  (_search_stream, values) cancel on client disconnect via the stream
  guard; oneshot H1 disconnects remain undetectable mid-flight
  (transport limitation, documented) — bounded by admission + flight
  timeout + the cancel API. AbortOnDrop guards stay: they cover H2 and
  any genuinely-dropped future.

## SHIP LOG v0.93.0-vix-20260811.80/.81/.82 (#36 arc, engine fa6485414e) — BOTH ENVS, VERIFIED (2026-08-11)
- .80 (8e93fc7a57): global queue removed -> node-local admission,
  ZO_QUERY_MAX_CONCURRENCY=30 pinned both envs, 429 past it;
  AbortOnDrop guards; RUNNING gauge leak fixed. Dev burst proof: 40
  concurrent memo-defeating queries -> EXACTLY 30 admitted + 10x 429
  (code 20012, message names limit + knob). wait_in_queue now 0
  everywhere.
- .81 (058743384a): OSS cancel plumbing (registry, flight select arm,
  gRPC handler, endpoints, stream guard). Dev probe found the cancel
  API 404: routes were ALSO enterprise-gated.
- .82 (fa6485414e): mounts the cancel routes on OSS. LIVE PROOF (dev):
  DELETE /api/{org}/query_manager/{trace_id}/cancel -> 200
  {"is_success":true}; the in-flight query died mid-run returning 429
  {"code":20009,"message":"Search query was cancelled"}; querier logged
  "flight->search: search canceled". Prod smoke: cancel route 200.
- Streaming disconnect: guard drop -> cancel_query_internal -> the SAME
  fan-out the API proof exercised; the stream-drop-on-disconnect link
  is hyper's streaming-response contract (verified-by-construction —
  dev's data volume finishes synthetic streams in <300ms via per-file
  memos, so a live mid-stream kill wasn't reproducible there; re-probe
  on prod-scale data if ever in doubt).
- Known limitation (documented in code + notes): H1 ONESHOT client
  disconnects are transport-invisible mid-flight (hyper notices at
  write time) — such queries burn to completion holding an admission
  permit; bounded by the flight timeout, 429 admission, and the cancel
  API. Follow-up lever if it bites: per-partition permit release or a
  request-body keepalive probe.
- Probe-craft lessons (cost ~5 iterations): response cache serves
  IDENTICAL sql+window instantly; per-file result memos serve identical
  CONDITION+rule across windows; vary histogram WIDTH per request to
  force real evals. Never inline nested quotes over ssh — scp a
  script. The dev "slow query": histogram('1 second') x 30d
  match_all(error) on k8s_dev_ops_logs, 6s+ cold, ~300ms warm.
- Rollouts: dev 13/13 x3, prod 21/21 x3 (one karpenter wave mid-.81);
  owner-requested pod deletion used once to fast-forward .80 stragglers
  (compactor-0, ingester-0/1). All 6 prod anchors byte-identical after
  each roll; zero restarts. Fleet ancestor gate advances: fa6485414e.
- Rollback: .82 -> .80 as a pair (.81 alone has 404 cancel routes);
  .80 -> .79 restores the one-at-a-time cluster queue (knowing trade).

## #37 oneshot disconnect-cancel + #38 segment no-op swarm, memory-light (2026-08-11, ships .83)
- #37 (owner: "a query need be cancel while the client lose the
  connection"): H1 gives no mid-request disconnect signal for pending
  oneshot handlers, so .82's cancel chain never fired for them. Fix:
  ZO_QUERY_HTTP_HEARTBEAT_SECS (default 5, 0=off) — a /_search response
  still running past the grace switches to a STREAMED body emitting one
  space every 2s while the search future runs INSIDE the stream;
  leading whitespace is legal JSON. Client gone -> heartbeat write
  fails -> hyper drops the stream -> the search future drops -> the
  .80 AbortOnDrop chain cancels and frees the admission permit. Trade
  (documented on the knob): past the grace the status is committed 200
  and errors arrive in-body (the code field). Pinned by
  heartbeat_grace_passes_response_through +
  heartbeat_streams_spaces_then_payload (start_paused time).
- #38 (owner steered AWAY from a decoded-batch cache — "should we rely
  on cache rather than pure performance improve?" — right call: 4th
  resident cache, GBs, and #34 was a cache-semantics bug this same
  day). Prod evidence: get_ctx_and_physical_plan p50 881ms/hr decomposed
  into vix evals 1,534s (real work) + segment scans 939s across 1,518
  scans, including a per-metric-stream swarm of ~45ms zero-yield
  object walks. Root cause found IN THE REGISTRY QUERY: query_unbuilt
  matches `streams LIKE '%"org/type/stream"%'` and `_` is a LIKE
  single-char WILDCARD — every underscore-bearing stream (all metrics)
  over-selected sibling segments; the old comment even documented the
  widening as harmless. Fixes, all memory-free:
  - stream_like_pattern LIKE-escapes `\`, `%`, `_`; both backends pass
    ESCAPE '\'. Pinned by
    test_query_unbuilt_underscores_match_literally_not_as_wildcards +
    updated pattern-shape pins.
  - zero-yield classification on every scan summary line: "zero-yield
    N stream-absent + M time-pruned" (stream_frames counter splits
    registry over-match from coarse per-object time bounds) — sizing
    data for the NEXT lever if one is still needed post-escape
    (candidates: per-stream min/max in the registry row, or a tiny
    frame-directory memo — metadata, not data).
  - Decoded-batch cache: built, then REVERTED before commit (owner
    call). If the escape fix leaves real decode pain on busy log
    streams, revisit as arrow-IPC PROJECTION at decode (CPU cut, no
    resident memory) before any cache.

## PROD BENCH obs (.83, vix fork) vs o2 (upstream v0.92.0-rc2-simd), 2026-08-11 (#39 gap list)
- Method: same prod data (independent dual-ingest, counts skew <=0.8%),
  identical SQL + sealed windows, alternating, median-of-3, run lists
  kept (obs round 1 = fully COLD: the Deployment migration gave every
  querier a fresh ephemeral volume hours before; o2 caches weeks-warm).
- obs WINS (median): incident_count 58ms vs 3,161ms (54x — #32 IS NULL
  key-term exact serve; o2 scans); topn_traces 319ms vs 3,014ms (9.4x,
  #29 dictionary top-k; o2 flat ~3s every round); histo_matchall 98ms
  vs 922ms (9.4x); filtered_count 108ms vs 394ms (3.6x); warm needles
  33ms vs 190ms; topn_ns: o2 cannot GROUP BY dotted fields via _search
  (returns empty in 14ms) — obs 97ms correct.
- GAP 1 (the real loss): incident_agg 7,703ms vs 3,261ms (2.4x SLOWER)
  — index finds rows instantly, then per-row user_id extraction from
  fat _source vs o2's columnar read. duration_agg only reaches parity
  (12.3s vs 14.0s) for the same reason. FIX = #39: docs-column
  promotion for hot aggregation fields (user_id, duration) on traces —
  closes the only warm-loss class (#33 C-3 made concrete).
- GAP 2: cold-start IO 2-7x behind o2 warm caches (needle 4.1s vs
  0.57s; count24h 102s vs 51s cold — vs 325ms obs warm via memo, 55x
  the other way). Aggravated since the querier Deployment migration:
  every roll starts the fleet cache-cold. Lever: boot-time warmup of
  recent hot hours, or accept post-roll softness.
- count24h medians ~par (21.9s vs 22.7s); warm obs 325ms.

## SHIP LOG v0.93.0-vix-20260811.83/.84/.85 (#37+#38, engine 606344e388/f8cfd7f154) — BOTH ENVS (2026-08-11)
- .83: #37 heartbeat wrapper + ZO_QUERY_HTTP_HEARTBEAT_SECS=5 pinned
  both envs; #38 LIKE-escape + zero-yield classifiers. Verified on dev:
  4 leading spaces on a 14s query (parses as JSON), zero-yield lines
  show 0 stream-absent everywhere (the swarm's over-selection is gone).
- .84: #37 cache-delta AbortOnDrop guards (third detach point).
- .85: #37 engage/drop probes -> LIVE THREE-STAGE PROOF on dev
  (19:43:26 heartbeat engaged, 19:43:28 stream dropped on the killed
  client's failed write, 3x "search task aborted on drop" for the
  trace). The .84-probe's zero was a probe artifact; the instrumented
  run is definitive. Cancel coverage now: explicit API (.82,
  live-proven), streaming stream-guard (fan-out proven), oneshot H1
  disconnect (.85, live-proven).
- Querier sts->Deployment migration (prod, same day): phase A
  alongside, phase B cutover; hit the DOCUMENTED scale-to-1 last-applied
  trap for ~2 min (HPA re-raised, zero 429s) — recipe now recorded in
  the manifest. 8 orphaned 2000Gi PVCs deleted. Engine rolls now surge
  in parallel; post-roll caches start COLD (ephemeral volumes) — see
  #39 GAP 2.

## SHIP LOG v0.93.0-vix-20260811.86 (#39 cold-IO, engine c87bbce5e6) — BOTH ENVS, VERIFIED (2026-08-11)
- Warmup live: dev ~200 vix files/node in ~6s; prod ~4,000/node in
  ~350s (concurrency 4, 0 failures; ~145k 24h candidates, ~24k
  own-share/node, skips = metrics parquet with no .vix — correct).
  Post-roll queriers reach index-warm for the 24h window in ~6 min.
- Eager tail live: cold small-file evals now "index fetches: 1"
  (22-108KB) vs the 8-9 GETs/file baseline; repeats 0 fetches. Big
  merged-file dictionary walks still fetch MB-class dict blobs once,
  then ride the memoized reader.
- Anchors byte-identical on BOTH envs post-roll; zero restarts.
- Tunables noted: warmup concurrency 4 -> raise if 6-min prod warmth
  is too slow; anchor-age windows (>24h) rely on the tail lever only.
- Remaining #39 GAP 1 (docs-column promotion for user_id/duration —
  the only class o2 wins warm) is NOT in .86; awaiting owner call.

## MERGE FINDINGS 2026-08-12 (corrects #31's scope) + #40 filed
- Prod compactor hour: 936 true merges (my earlier 2,323 counted
  DataFusion sub-phases), sum 9,154s, p50 7.0s, p90 19.8s, max 80s.
  index_merge: true on 936/936 — THE DICTIONARY FAST PATH ALREADY RUNS
  AT 100%; zero full rebuilds. #31's "term-accumulation = 99%" profile
  applies to the single-file BUILD path (ingest move job), not merges.
  Remaining merge cost = docs blob re-encode + download/upload IO.
- Load: skewed (one compactor pegged at 8C, peers <1C — hot
  stream-hours serialize); composition dominated by tiny metrics
  streams (dozens of orchestrator_* families, ~1.1-1.2k merge events
  each/hour).
- #40 (owner directive): metrics streams -> column-store-only core
  files, NO inverted index. Fit: one metric family per stream,
  low-cardinality labels, whole-window aggregations — postings buy
  little; the index build/merge is pure overhead on the metrics merge
  storm. Plan in progress (writer index_enabled option + index=none
  property, mixed-era merge semantics, reader has_index()=false,
  leader routing gate by stream type, all label fields as docs
  columns, ZO_VIX_INDEX_DISABLED_STREAM_TYPES default metrics).
- Merge levers ranked (post-findings): (1) #40; (2) merge policy for
  tiny metrics streams (fewer, bigger merges); (3) docs-encode
  profiling on a prod-shaped corpus (merge_bench); (4) #31 rescoped to
  the BUILD path's term accumulation (ingester/builder CPU).

## SHIP LOG v0.93.0-vix-20260812.87 (#39 GAP 1a, engine 3dd52490e8) — BOTH ENVS (2026-08-12)
- duration is a default docs column (ZO_COLUMN_STORE_DEFAULT_FIELDS,
  pinned both envs). Write-side only; new files carry the column, old
  files degrade per-file to the scan branch. All 12 anchors
  byte-identical post-roll, zero restarts. The incident_agg /
  duration_agg classes go columnar as data turns over — re-bench vs o2
  after a day of turnover to confirm GAP 1 closure.

## SHIP LOG v0.93.0-vix-20260812.88 (#40 guards, engine 25f6ed6e48) + compactor Deployment migration — BOTH ENVS (2026-08-12)
- #40 read guards fleet-wide, ACTIVATION OFF
  (ZO_VIX_METRICS_CORE_FILE_ENABLED=false pinned both envs): metrics
  keep writing parquet; nothing on disk changed. The engine carries the
  full index-off mode: writer skips (empty term plan, no term emission,
  no dict/terms/bloom blobs, index=none property), reader voids
  dictionary-absence proofs (has_index()=false, never FieldCap::Absent,
  whole-file filter-back for real conditions, eval insurance errors),
  policy-driven merges with mixed-era healing both directions, routing
  gates (SQL use_inverted_index + handle_index_optimize + PromQL), and
  the storage.rs keep-condition fix (extracted-but-unprobed conditions
  re-apply at scan — the silent-unfiltered-rows catch). Verified: dev +
  prod anchors byte-identical, PromQL healthy, zero restarts. THE FLIP
  is a config-only PR whenever the owner wants the metrics merge-storm
  savings to start accruing; DO NOT flip until every querier runs .88+.
- Compactor sts->Deployment (same call as the querier; the sts header's
  RWO rationale predates generic ephemeral volumes): phase A alongside
  (4/4, Deployment pods verified completing merges), phase B cutover
  with the set-last-applied surgery FIRST — replicas never dipped (the
  querier scale-to-1 lesson, applied). HPA (4-10) on the Deployment and
  already scaling into the backlog. Orphaned sts PVCs deleted. NOTE:
  Argo needed a manual refresh annotation to pick up both phase commits
  and did NOT prune the removed sts (deleted manually to converge) —
  watch prune behavior on future removals.
- Fleet workloads now: querier + compactor + router = Deployments
  (surge rolls); ingester + nats = StatefulSets (real state).
- INCIDENT (2026-08-12, prod): querier phase B removed
  obs-querier-headless from git, and the Argo sync PRUNED the live
  Service at 2026-08-11 19:16:13Z (controller log: 'Pruned'
  Service/obs-querier-headless + StatefulSet/obs-querier, syncId
  1285841) — but orbit prod's datasource (Nacos ORBIT_OPENOBSERVE_URL)
  dials http://obs-querier-headless.obs:5080 DIRECTLY, the same
  out-of-repo consumer dev #222 broke on 2026-08-07; the guard comment
  lived only in the DEV repo's querier.yaml, so the prod cutover
  repeated it. Orbit's querier access was dark ~11h20m (19:16Z →
  06:35Z hot-restore), surfaced by USER REPORT, not monitoring. Fix:
  hot-applied the headless Service (app=obs-querier, 5080/5081 —
  endpoints all 5 queriers, in-cluster healthz ok, orbit logs query
  141ms), then made it permanent with the guard comment in prod-ops
  #396 (both repos now carry it). Compactor phase B pruned
  obs-compactor-headless the same way (05:42:40Z, syncId 1293174) —
  checked: no consumer, no impact. CORRECTION to yesterday's note:
  Argo ops-obs DOES prune at sync (both phase-B prunes are in the
  controller log) — "did not prune the removed sts" was wrong, or
  described a pre-refresh state. Operational rule going forward:
  deleting a manifest from this repo IS a production delete at the
  next sync — enumerate every removed object's consumers (Nacos
  datasource URLs, grafana, anything out-of-repo) BEFORE merging, not
  after.
- FINDING (2026-08-12, prod): needle equality on traces db.statement
  (UI trace search) = 12.5s cold / 9.3s warm for 6 hits over 1h, and
  the UI's auto-histogram ADDS 8.0s/7.0s on the same WHERE (its
  SimpleHistogram rule pushes the condition, but it processed the full
  window: scan_records 466M ≈ every record, vs the search's 54M).
  Cause chain (CORRECTED after code+prod deep-read, same day — the
  first version of this entry blamed L0 files; that was head-sample
  bias in the log grep): the taint is the PER-VALUE cap
  ZO_VIX_MAX_RAW_TERM_LENGTH (default 65532, config.rs:1478,
  writer.rs:1405-1420): one non-fts string value over the cap skips
  that value's term AND marks the FIELD partial for the WHOLE file.
  Prod traces: max(length(db_statement)) = 2,178,622 bytes; 227
  oversize rows in 2h over 115M spans → ~1 in 6 L0 files tainted
  (60-83k rows each) and compacted files tainted with near-certainty
  (millions of rows) — worse: dictionary merges UNION inputs'
  partial_fields (writer.rs:872-876) and prod merges take the dict
  fast path 100%, so the taint is STICKY through compaction; kept
  files in the measured query include compacted ones (e.g.
  .../06/749319600048863641691eb.vix, index_size 207MB) alongside
  l0_*. db.statement is the ONLY field fleet-wide reported partial
  in 24h. It is also not a docs column, so every kept file pays
  per-row _source extraction (#39 GAP 1 mechanism, trace path).
  There is NO L0-specific term budget — L0 and compacted files carry
  identical blob sets; L0 pays the full index write cost (l0_multi:
  ~17MB index on ~27MB compressed object ≈ 63% of L0 bytes) yet the
  needle still can't be index-served in tainted files.
  File-count context (meta, files/hour → avg orig size): steady
  sealed hours ~550 → 3.7GB; post-migration tail hour 06 = 1,866 →
  1.1GB, hour 07 = 1,865 → 339MB (in progress); 1h query fan-out =
  3,671 files + 365 segments (2,948 L0 ranges).
- #41 RESOLVED 2026-08-12 by OWNER CALL — "keep using index, no need
  care about absolute correct for that value oversized. The
  performance is first and principal design." Implemented as
  SKIP-WITHOUT-DEGRADE (the prefix-term/not-exact design below was
  REJECTED as unneeded correctness machinery): oversize raw values
  now skip term emission WITHOUT tainting the field, at all four
  writer sites (column-driven string + numeric-canonical uniformity
  guard, source-driven string + numeric — the rebuild path matches so
  compaction rebuilds do not re-taint). ACCEPTED SEMANTIC HOLE, scope
  exactly: an equality/range probe whose LITERAL is itself >64KiB
  silently misses those rows (only programmatic replay of a captured
  oversize statement can even pose such a query). Everything else
  heals or stays exact: needle equality on fields with oversize
  NEIGHBORS (the db.statement 12.5s case) becomes index-served on new
  files; IS [NOT] NULL stays exact (key terms still emitted for
  skipped rows — pinned); dict top-k/group-by serves stay ELIGIBLE
  via a per-field OVERSIZE-SKIP ALLOWANCE (follow-up owner call, same
  day: "no need scan these large value") — the writer stamps an
  oversize_skips property ({field: count}; absent on legacy files),
  dictionary MERGES SUM inputs' maps forward, and the serve
  reconciliation accepts indexed + skipped == key-term docs, serving
  counts that OMIT the skipped values (their docs surface in no
  group); any OTHER shortfall (type-mixed fields, pre-fix
  empty-string files) still refuses + scan-falls-back, and the
  partial MARKER alone still refuses (all pinned in
  field_value_counts_allowance_and_refusal_policy +
  merge_sums_oversize_skip_allowances); fts unchanged;
  merge fast path stays valid (a rebuild could not index the value
  either — differential test extended). Observability:
  VixWriterStats.oversize_skipped + INFO log per build. LEGACY files
  keep their taint (merge-union + reader partial semantics unchanged
  — partial_fields still means type-drift, field-id overflow,
  source-keys-outside-plan, or legacy oversize): recent windows heal
  as new files land; old windows stay slow until retention or a
  cleansing-sweep rebuild (the ops lever if it matters). SAME
  SESSION: ZO_WAL_NARROW_SCHEMA code default flipped false→true
  (owner call; fleet had pinned true since dev .26 / prod .28 —
  rollback lever remains). STILL RECOMMENDED upstream: cap
  db.statement at the SDK/collector (4-16KB OTel attribute limit) —
  oversize sources measured prod 3h: ALL redis spans,
  manus-node-server max 16,076,765 bytes (16MB!), monica-super-agent
  max 143KB/146 rows, manus-node-socket max 665KB, 299 rows total;
  multi-MB attributes bloat WAL/_source/S3 regardless of indexing.
  Tests: 8 rewritten/extended across vortex_index (oversize-skip,
  key-terms-survive, fts both-derivations, merge-inputs, dict-serve
  reconciliation + marker via property-patch fabrication, #40
  roundtrip control), search (partial fixture switched to the
  unknown-key cause), core (merge-vs-rebuild differential
  spot-checks, stats literal). Unit sweep green: vortex_index 177,
  core 1884, config 1976, ingester+jobs 105, search all.
- #42 CANDIDATE (owner call pending): L0 index-off core files for
  ALL stream types — hot-data columnar, index materializes at
  compaction. ~80% of the machinery shipped dark in #40: reader
  guards are per-file via the index=none property and index_size==0
  (reader.rs:305-318,447-451; vix/mod.rs:1072-1081,1405-1412;
  flight.rs:1019-1025) — stream-agnostic, reusable as-is; merge
  healing both directions exists (index-off input under indexed plan
  → IndexedMergeFailure::Fallback → full _source rebuild,
  core_writer.rs:948-957; classify_core_file mode-mismatch →
  NeedsRebuild both arms, :1105-1118), so "index appears at merge"
  is the existing heal semantic. Minimal change set (mapped): make
  writer policy an explicit parameter instead of f(stream_type)
  (core_writer.rs:365/394/900/974/1064/1185 + the two L0 call sites
  parquet.rs:1005-1020, segments.rs:1219-1235 + a default-off env);
  flip 4 per-STREAM routing gates to per-FILE (flight.rs:283-292,
  356-370; vix/mod.rs:150-164 must add index_size>0 to the candidate
  filter or index-off files get downloaded just to bail; promql
  storage.rs:229-236); decouple the "index-off ⇒ ALL fields become
  docs columns" coupling (core_writer.rs:436-446, 1225-1240) into
  its own flag with width mitigations. WIDTH FACTS (for the
  2557-field traces schema fear): per-FILE schema is the union of
  batch schemas, not the registry (segments.rs:906-944,
  parquet.rs:969-981) — and ZO_WAL_NARROW_SCHEMA (code default false,
  a rollout lever) is PINNED TRUE on both envs (dev .26, prod .28
  configmaps), so fleet batches ALREADY carry present-fields-only —
  per-file L0 width is the chunk's present-field union (hundreds),
  not the 2,557-field registry; sparse
  columnar data is genuinely null-suppressed (NullDominatedSparse
  >90% null ⇒ cost ∝ present values; all-null ⇒ ConstantArray O(1));
  the per-column METADATA floor is ~0.4-1KB/file (fields JSON, dtype,
  FileStatistics, zoned+chunked+flat layout nodes, 2 segments) and
  the killer coupling is rows_per_chunk computed from WHOLE-ROW arrow
  bytes (writer.rs:1872-1885: 2557 nullable Utf8 cols ≈ 10.5KB/row of
  arrow padding even all-null ⇒ chunk rows collapse ⇒ zone count ×
  every column's zone-stats table, super-linear) — fix = decouple
  chunk sizing from schema width + a no-zone field strategy for
  sparse columns (WriteStrategyBuilder::with_field_writer) +
  narrow-schema batches. PAYOFF: drop dict/terms/bloom from L0 (~63%
  of L0 object bytes today + ingester term-plan CPU + querier index
  cache churn — the 1h window loaded 26GB of vix index); L0 needle/
  filter queries become columnar scans instead of per-row _source
  extraction (#21 precedent: 12.1s → 1.2s when code.function became
  a column). COSTS/GATES: L0→L1 merges lose the dictionary fast path
  (every merge becomes a source rebuild — bench compactor CPU with
  merge_bench.rs before/after); match_all over LOGS L0 becomes a
  columnar contains-scan of fts columns (bench before enabling for
  logs; traces stream has no fts keys — enable traces first); HARD
  fleet-version floor required (pre-.88 readers silently drop rows on
  index-off files — no capability negotiation exists; default-off env
  is not an interlock), and integration_test.rs has ZERO index-off
  coverage today (add a mixed L0-index-off + indexed-L1 differential
  suite: match_all, IS NOT NULL, histogram/count, star). Sequencing
  note: #42 alone does NOT fix the db.statement needle (rebuilt
  merged index re-derives the taint from oversize values) — #41 is
  the needle fix, #42 is the hot-tail structural win; they compose.
## SHIP LOG v0.93.0-vix-20260812.89 (#41 skip-without-degrade + allowance, engine a3ce5fd374) — BOTH ENVS (2026-08-12)
- Content: oversize (>64KiB) raw values skip term emission WITHOUT
  field taint (all four writer sites; rebuilds match); per-field
  oversize_skips allowance property (merges SUM it) keeps dict
  top-k/group-by serves eligible with skipped values omitted;
  ZO_WAL_NARROW_SCHEMA code default true (both envs already pinned).
  Accepted hole: equality for a >64KiB literal itself silently
  misses. Legacy files keep their taint until retention/rebuild.
- Gates: unit 3,965 + vortex 178 + core-vix 37 green; integration
  BOTH segment modes green (SEG_EXIT=0/NOSEG_EXIT=0).
- Dev (PR dev-ops #248, merged by owner): full fleet on .89 by image
  tag, Synced/Healthy; replay 6/6 md5-stable ×3 (needle/select/
  filtered byte-identical to pins; histo f29bd5cbe7 / topn_ns
  e619d71d7f at stable post-drift values, topn_traces c1fc99aa18);
  wal tail 53; only #28-class dead-node dials, ceased on eviction.
- Prod (PR prod-ops #397, merged by owner; argo refresh annotation
  needed as usual): 21/21 pods on .89 (17-ingester sts rolled
  ordered, ~11 min), replay 6/6 byte-identical to the DOCUMENTED
  prod pins ×3 runs (4628858f87/7c0ee82564/c268158f58/63432be4fb/
  f6c5fa19f6/a79e0ef7b9); allowance LIVE — ingester logs "skipped
  N oversize raw value(s) ... {\"sandboxes\": N}" (logs-stream field
  'sandboxes' is prod's top oversize offender, not just traces
  db.statement); wal tail 0=66/1=548 right after the roll (post-roll
  catch-up). PRE-EXISTING error classes verified NOT .89 (8h orbit
  histograms): eks_audit_log ZO_COLS_PER_RECORD_LIMIT drops
  (~160-320/2min, GROWING with the day's surge — thousands of audit
  records/burst discarded; owner decision: raise the limit for that
  stream or accept), and aws_vpc_flow_logs L0-build "not enough
  memory for external sort" retry loops (sporadic all day,
  surge-aggravated — candidate #43: segment-build sort-memory
  headroom under surge-sized hours).
- CONTEXT that day: prod ingest surge scaled ingesters 5→17 (HPA
  satisfied at 40%/85% after), compactor HPA PINNED at max 10/10 and
  59%/50% over target → files/hour 634→1,541 (compaction lag class
  of slow recent-window queries). Meta postgres HEALTHY throughout
  (~1ms read latency; the 09:03Z 1,239-connection spike was fleet
  scale-out churn + o2 connection churn, not DB distress). HPA
  maxReplicas raise (10→16) recommended, owner call pending.
- FLEET PIN advanced: a3ce5fd374 (ancestor gate for every future
  release build).
## SHIP LOG v0.93.0-vix-20260812.90 (#42 dark + cols 64k, engine ab60c3545a) — BOTH ENVS (2026-08-12)
- #42 L0 index-off shipped DARK (ZO_VIX_L0_INDEX_OFF_STREAM_TYPES
  empty; activation = later config PR, #40-style). Gates: unit sweep +
  integration in FOUR variants (both segment modes × L0 on/off).
  Replay: dev 6/6 byte-identical to .89; prod 5/6 identical +
  needle_select 7c0ee82564→2446c6b12d, BENIGN — the 2026/08/10 anchor
  partitions were compaction-rewritten today (file ids ~37.6M vs
  37.75M current-hour max), tie-order changed; md5-stable ×3. NEW
  PROD PIN: needle_select 2446c6b12d.
- ZO_COLS_PER_RECORD_LIMIT → 65536: engine default AND both configmap
  pins (dev #250 review P0: explicit pins override engine defaults).
  eks_audit_log silent drops STOPPED at the .90 roll.
- #40 ACTIVATED both envs same day (dev #249, prod #398, config-only
  on .89): metrics streams write core files (index_size=0 confirmed in
  both file_lists), PromQL verified serving over them, zero compactor
  metrics errors. ZO_VIX_METRICS_CORE_FILE_ENABLED=true pinned — the
  .88 ship-log "activation OFF" state is SUPERSEDED.
- PRs: dev-ops #248/#249/#250, prod-ops #397/#398/#399. FLEET PIN
  advanced: ab60c3545a. Parked owner calls: #42 activation config PRs
  (dev first; measure L0 index_size=0 + hot-window latency + compactor
  rebuild cost), compactor HPA raise 10→16.
## SHIP LOG v0.93.0-vix-20260812.91 (#43 SIMD + AVX2 tier, engine 939f9bfaa7) — BOTH ENVS (2026-08-12)
- sonic-rs lazy _source parse (rebuild term derivation; parity
  differential-pinned), word-at-a-time dict prefix, x86-64-v3 compile
  tier (AVX2/FMA/BMI; NO AVX-512 — Milan SIGILLs; never resurrect
  Dockerfile.tag-simd's avx512 flags). EPYC 7R13 interleaved A/B:
  +2.3% median on the allocation-bound build floor. Local hybrid-core
  (12700H) benches need P-core pinning (taskset -c 0-11) — unpinned
  A/Bs are invalid. Term-map rework SKIPPED on evidence: flat DWARF
  profile, max symbol 3.7%.
- Gates: full matrix rebuilt+green under the new flags (unit + seg +
  noseg + L0-mode). Dev replay 6/6 byte-identical ×3, zero post-roll
  errors; prod replay 6/6 ×3 with needle_select BACK on the original
  pin 7c0ee82564 (the .90-era 2446c6b12d was transient rewrite
  tie-ordering — BOTH values are valid-seen for that anchor).
- PRs: dev-ops #251 (bump-obs-91), prod-ops #400. FLEET PIN advanced:
  939f9bfaa7.
## SHIP LOG v0.93.0-vix-20260812.92 (#44 claim floor, engine bcc3f3fd25) + #42 LIVE ON PROD + ceiling 24 (2026-08-13)
- .92 both envs: all-or-nothing claims. Verified live: L0 claim spans
  ~7 → 19-23 ids minutes post-roll (converging to 32); replay 6/6
  byte-identical ×3 on prod (needle_select back on 7c0ee82564), dev 4/6
  identical + the two documented rewrite-drifters moved together;
  ingester mem under full claims ≈ estimate (hottest 6.8Gi). NOTE the
  push incident: .92's first push died on SSO expiry but a chained
  `echo PUSH=$?` printed 0 — dev ImagePullBackOff'd on a tag that
  existed nowhere. RULE: verify pushes by the "pushed to both
  registries" LOG LINE, never exit codes.
- #42 prod activation: OWNER MERGED #402 (18:10Z 08-12) after the
  5.4×-cost hold comment — the owner call stands, #42 is LIVE both
  envs. Consequence measured: compactors re-pinned 16/16, un-healed
  hours accumulated 3,500-3,967 index-off sliver files (pre-.92
  slivers × heal lag). #404 (merged): compactor HPA 16→24 + nodepool
  590C/4720Gi → 635C/5080Gi, sized from measured rebuild throughput
  (~22 cores continuous at ~500M rows/h); compactors at 19/24 within
  minutes. LIVE CONSTRAINT recorded in the prod kustomization: while
  index-off files exist, the .88 guard floor is LOAD-BEARING;
  deactivation = env-clear + builder env-rev bumps, never an image
  rollback.
- #45 CANDIDATE: query PLAN cost on index-off windows —
  get_ctx_and_physical_plan took 5,263ms on a follower for a 1h
  post-activation window (983 files; empty result also paid 3.8s/
  follower cold index eval on the indexed minority). Suspect: wide
  per-file docs schemas (all-present-fields columnar) hitting plan
  construction / schema unions. Reproduce on dev, profile the plan
  path, fix (schema cache or plan-time docs-schema pruning).
- PRs: dev-ops #253, prod-ops #403/#404 (+#402 owner-merged). FLEET
  PIN advanced: bcc3f3fd25.
## SHIP LOG v0.93.0-vix-20260813.93 (#46+#47+#45, engine f60c3d23d4) — BOTH ENVS (2026-08-13)
- #46 column-derived heals LIVE: prod ran 9 column-derived heals in the
  first 8 min; COMPACTORS OFF THE PIN — 33%/50% at 21 pods (was 59-69%
  pinned at 16). Heal-debt hours drain to steady state once reached:
  02Z 3,500→26 index-off left, 03Z 3,967→115; fresh hours (04-06Z,
  written during the roll churn) queue at 2,700-6,470 and follow.
  Parity: replay 6/6 byte-identical to canonical pins ×2 clean runs
  (a third run's awk mis-parse was a transient retry line — raw output
  healthy). Referees: dict-control + forced-source + integration ×4.
- #47 ZO_SEGMENT_BUILD_CLAIM_MB ships DARK (0 = count mode);
  claimable_stats now returns total_size. #45 RESOLVED as diagnosis:
  the do_get "plan took" label spanned the whole follower setup incl.
  index eval — relabeled; per-stage attribution was already logged.
- PRs: dev-ops #254, prod-ops #405. FLEET PIN advanced: f60c3d23d4.
- Ops mitigations available today, no engine change: (a) promote
  db.statement / db.query.text into column_store_fields on prod
  traces — DECLINED by owner 2026-08-12 (equality-only workload;
  superseded by the #41 resolution above), (b) keep compactor HPA
  headroom so the L0 tail stays minutes-deep, (c) needle hunts:
  disable the UI histogram toggle (it re-processes the full window
  each refresh).
## SHIP LOG v0.93.0-vix-20260813.94 (claimable_stats CAST hotfix, engine 9348bffd62) — BOTH ENVS (2026-08-13)
- .93 REGRESSION (mine): pg `sum(size)` returns NUMERIC; sqlx i64 decode
  failed EVERY claimable_stats call → #44 gate failed OPEN → sliver
  storm both envs (prod 06Z: 8,115 L0 files on one metric stream). .94
  = `CAST(coalesce(sum(size),0) AS BIGINT)`. Verified on prod: "batch
  done: segments in=32 built=32"; span-32 l0_multi files back. LESSON
  (now a gate): unit suites run sqlite only — any pg-typed SQL change
  must be proven against real postgres before ship.
- Post-roll incident 08:14-08:18Z: rolling ingesters under load doubles
  per-pod intake; 512MB segment buffers filled → 503 backpressure until
  HPA scaled 7→9 and buffers drained. NOT a .94 defect; expect on every
  ingester roll under load. Small l0_multi id-spans right after = hour-
  split claims over flood backlog (claims were full-32 throughout).
- PRs: dev-ops bump-obs-94, prod-ops #406. FLEET PIN advanced: 9348bffd62.
## SHIP LOG v0.93.0-vix-20260813.95 (#48a composite bloom DARK, engine f30463f506) — BOTH ENVS (2026-08-13)
- #48a: reserved composite .bf section (tagged keys V{len}{field}{value} +
  3 guard probes/field) makes equality/IN on ANY term field
  bloom-decidable; fts/demoted fields excluded from coverage (their
  claim would wrongly drop); pruner trusts a miss only when all guards
  hit; every failure keeps files. Ships DARK (ZO_VIX_BLOOM_COMPOSITE
  pinned "false" on prod, review P0/P1: .95 rollback note + knob pins).
- Replay 6/6 md5 ×3 on prod .95 (canonical set re-pinned in memory).
  Dev roll fought a NATS quorum loss (node ip-10-20-62-231 died; nats-0
  + 4 pods orphaned Terminating -> force-deleted) and a 31.5-min stale
  pg advisory lock that idled compactors; heal debt 05-10Z ~3,300
  index-off files -> dev compactors 6->12 (dev-ops #258) to drain.
- ACTIVATION: dev-ops #257 MERGED — dev runs composite=true on all
  three roles (writers, .bf assembler, pruner). Prod activation waits
  on dev query-level verification. PRs: dev-ops #256/#257/#258,
  prod-ops #407 (user-merged).
## INCIDENT 2026-08-13: compactor rolls freeze the job-claim advisory lock
- Every compactor roll: old-gen pods (600s grace) freeze mid-claim-txn
  holding pg_advisory_xact_lock(file_list_jobs:get_pending_jobs) —
  waiters pile up (dev: 18-min holds, fleet idle; prod: 47-deep queue).
  Post-SIGKILL the sessions orphan until TCP timeout (~30 min).
- MITIGATED (both envs): idle_in_transaction_session_timeout=120s at DB
  level (claim txns are ms-scale; zero collateral) + manual orphan
  sweeps. #49 filed: shutdown must abort in-flight claim txns
  (rollback-on-cancel) instead of freezing through grace; consider
  claim-lock sharding — 24 prod compactors polling one global lock
  queue ~4 min between claims.
- NATS follow-up (review note, dev-ops #258): server is 3-node but
  ZO_NATS_REPLICAS=1 — streams die with their host node (today's 2h
  builder crashloop). Stream-replica migration needs scheduling.
## #51 Compactor CPU — analysis + fixes (2026-08-13)
Owner: "compactor super slow, more CPU than o2, optimise".
ROOT CAUSES (two distinct axes):
- SLOW (latency): the index k-way merge runs SINGLE-THREADED —
  partition_bounds() is stubbed empty because the old token-level
  raw-byte sampler was unsound under v2 field-major keys (prod dict
  corruption 2026-07-29). Index output is the LARGER half (bench: 142
  MiB index vs 87 MiB docs), so one merge pins one core for the bulk of
  its wall; the fleet only scales by running many concurrent jobs.
- CPU-HEAVY: (a) partly BY DESIGN — o2 compaction is parquet+zstd only;
  .vix additionally builds the inverted index (dict+postings+bloom),
  trading compaction CPU for query speed. (b) WASTEFUL part: byte-only
  merge batching let sliver-debt hours stack 1,600+ files into ONE
  k-way merge — memory tracks merge width, heap CPU superlinear →
  OOMKilled at 16Gi (dev 2026-08-13).
SHIPPED (#51a, safe, in .97): ZO_COMPACT_MAX_FILE_COUNT (default 128)
caps merge WIDTH; oversized groups split across passes. Plus #50 DB
polish (SKIP LOCKED claims, pool cap 32, idle-in-txn guard, builder
poll backoff).
FOLLOW-UP (#51b = ENGINE-BACKLOG item 9, sound parallel merge):
partition on OUTPUT field-id boundaries (fid = fixed 2-byte key prefix,
so ranges are trivially disjoint + ascending — immune to the token
sampler bug). Per input, translate the output-fid range back to that
input's raw fid range (order-preserving remap, a bijection on shared
fields) and seek via predecessor_block on {raw_fid_lo BE}. Split a hot
single field (trace_id) further only via that field's OWN token space
sampled from the widest input and remapped — never raw cross-input
bytes. MUST validate byte-identical vs single-range with the merge_bench
`compare` oracle across many corpuses BEFORE ship (this path caused the
prod corruption). Latency fix, NOT a total-CPU fix. Deferred until the
ARM migration + heal backlog settle — not an incident-window edit.
## #51 PROFILE RESULT (perf, symbolized, 2026-08-13)
Merge CPU is NOT the k-way term merge: docs-blob RE-COMPRESSION
dominates — CascadingCompressor choose_and_compress/compress (~5.5k
samples) + vortex-zstd encode (~7.2k, mostly _source) vs ~1.5k for the
whole index merge path. Every merge re-samples schemes per chunk and
re-encodes bytes that were already zstd'd in the inputs.
- #51c (BIG WIN, design): docs-chunk PASSTHROUGH on disjoint inputs —
  compaction groups are typically time-disjoint (the bench gen shape);
  when input time ranges don't overlap, concatenate their docs chunks
  verbatim (no decode, no re-encode) and only merge the index. Falls
  back to the streamed re-encode when ranges overlap.
- #51d (cheap): reuse the chosen scheme per column across chunks within
  one build (choose_and_compress re-samples every chunk — ~20% of merge
  CPU on sampling alone); check BtrBlocksCompressorBuilder knobs.
- #52 (owner-approved, IN PROGRESS): bloom-only high-cardinality fields
  attack the INDEX half; #51c/d attack the DOCS half. Together they
  target the bulk of compactor CPU.
## #48a numeric coverage FIX (9cea3bb337, gates prod activation)
Composite coverage is STRING-FAMILY only now: numeric term fields hash
canonical tagged terms the raw-literal probe never matches — coverage
had turned 'status=200' misses into wrong drops (dev-only exposure).
## .97 STAGED (2026-08-14, engine 48fb23604f) — BLOCKED ON AWS SSO
- Carries: #50 (SKIP LOCKED claims pg-proven, pool cap 32, idle-in-txn
  guard per connection, cheap has_claimable probe — the earlier timing
  backoff REGRESSED seg-mode alerts, caught by two identical
  integration failures and replaced), #51a width cap, #48a
  numeric-coverage fix (string-family composite — prod activation
  gate), #52 COMPLETE (bloom-only fields + merge-time AUTO demotion
  from dictionary block-meta distinct counts; two-generation merge
  convergence test).
- Gates: integration BOTH modes EXIT=0 on 48fb23604f; unit suites
  green; ancestor gate ok; both arch binaries built.
- BLOCKED: SSO expired before the ECR push. Branches ready unpushed
  (dev-ops bump-obs-97, prod-ops zhichen). Owner notified; chain
  resumes at push_image --arm64.
## #52 A/B (merge_bench, 16 files/960k rows, taskset 0-11, x3)
- Terms 5,004,804 -> 3,084,804 (-38%); index blob 141.9 -> 85.5 MiB
  (-40%); total file 228.6 -> 198.6 MiB (-13%).
- Fast-path merge wall ~2.03s -> ~2.22s (+8%): the two demoted IDs
  became CS columns (+26.5 MiB docs) and the docs zstd pipeline is the
  profiled dominator — it eats the dictionary savings ON MERGES. The
  wins land on the REBUILD/heal path (1.9M fewer term-map
  insert/sort/spill per 960k rows), the .bf size, and the query path
  (needle scan = one dict-encoded column). Docs-side cost is #51c's
  target (chunk passthrough would skip the re-encode entirely on
  disjoint merges).
- AUTO would fire on this corpus (trace_id/span_id ratio ~1.0).
## SHIP LOG v0.93.0-vix-20260814.97 (engine fc4761f31b) — BOTH ENVS (2026-08-14)
- Multi-arch (ARM fleets both envs). Replay: dev x2 self-consistent;
  prod 6/6 canonical anchors x3. FLEET PIN advanced: fc4761f31b.
- Carries #50 (SKIP LOCKED claims, pool cap 32, idle-in-txn guard,
  has_claimable probe — the backoff variant REGRESSED seg-mode alerts
  and was caught by the integration gate), #51a width cap, #48a
  numeric-coverage fix, #52 bloom-only + merge-time AUTO.
- ACTIVATIONS: dev #262 bloom-only AUTO (ratio 0.5) LIVE on writers;
  prod #411 composite activation OPEN (fix prerequisite satisfied).
- PRs: dev-ops #260/#261 (replicas = o2 parity: 3/3/1/1)/#262,
  prod-ops #410/#411.
## FLEET RESET 2026-08-17 — v2 pivot (owner call)
- Owner: format redesign allowed, query performance first, no history.
  Orbit switched back to o2 → obs has ZERO query consumers until v2.
  S3 lifecycle 1-DAY expiry on the obs prefixes IS retention.
- Post-.106 state that triggered it: #51c stack live fleet-wide 08-15
  (passthrough verified engaging on prod), then prod compactors entered
  a 24Gi OOM crash-loop — whole-object RAM downloads
  (cache_remote_files -> res.bytes()) x 12 workers x 14.7k-job queue;
  kill ~every 6 min; heal backlog 343k -> 659k. RESOLVED BY
  DECOMMISSION (A1); the mechanism is binding v2 constraint H3
  (DESIGN-V2.md §3).
- A1 DONE: querier+compactor replicas 0 + HPAs pruned BOTH envs
  (prod-ops #425, dev-ops #278; webhook auto-sync pruned in seconds).
  Only the ingest path runs (router/ingester/nats/collectors). #413's
  sizing thereby removed (obsolescence comment; it had already merged
  08-14).
- A2 DONE (08:03-08:32Z): fresh meta DB obs20260817 + ZO_S3_BUCKET_PREFIX
  obs-20260817/ BOTH envs (prod-ops #426, dev-ops #279), ingester+router
  env-rev roll clean (prod STS ~8min, dev ~3.5min, 0 crashloops). Root
  recreated from env; ZERO 401s; 22k ingest-200s/2min prod; file_list
  filling in both new DBs. VERIFIED the obs-bound collector exporters
  auth as root — the wangzhichen@manus.ai Basic header is the o2
  exporter's.
- A3 DONE (~09:0xZ): lifecycle applied+verified on BOTH buckets via
  profiles eks-prod/eks-dev — rules obs-20260803-purge (1d) +
  obs-20260817-retention-1d (1d), abort-incomplete-multipart 1d on
  both prefixes. Buckets had NO pre-existing lifecycle and versioning
  OFF; o2/ untouched. Old prefix purges within ~24-48h. Old DBs
  obs20260803: DROP due 2026-08-24 (no dump — owner call).
- OPEN: prod L0 builder gap — builder runs on ingester+compactor roles
  (job/mod.rs:639) and compactors were ~2/3 of build throughput; prod
  arrivals ~680 seg/min vs ~200/min ingester-only builds; pending
  ~1M rows/day; post-A3, unbuilt segments' objects expire before build.
  Proposed: 2 builder-only compactor pods (ZO_COMPACT_ENABLED=false, v1
  merge path never runs). AWAITING OWNER.
- V2: DESIGN-V2.md (5831dc4c79) — all-present-columns docs + _source,
  sidecar index (index_ver), splice-able stats footer, BREAKING format,
  H1-H4 holding conditions, launch = another prefix+DB cut. Rollback
  floors / replay anchors / concat brake rule: OBSOLETE after the wipe.
- H1 prototype started: chunk sizing from encoded bytes; sparse-width
  cost matrix (50/300/800/1500 cols); file-level vs chunk-level
  presence decision.
## V2 M1 SHIPPED (2026-08-17) — sidecar split (.vix data + .vxi index)
- Format v3, BREAKING, no legacy read path (post-wipe). Data object:
  docs blob + data props (row_count/row_group_size/zone_map/row_order/
  oversize_skips/columns). Sidecar: dict/dict_blocks/terms/plist/bloom
  + index props (term_count/tokenizer/dict_layout/key_layout/
  plist_min_docs/fields/partial_fields). row_count stamped on BOTH,
  verified at open (mispair guard). index=none marker deleted — no
  sidecar IS the marker (#40/#42 unchanged, index_size=0).
- file_list schema UNCHANGED: index_size = sidecar object size (now
  ADDITIVE storage; warmup/bloom-queue/index-eval gates keep keying on
  index_size>0). Sidecar key = data key ext-swapped
  (config::vix_sidecar_key; FILE_EXT_VXI).
- Producers upload data -> sidecar -> file_list row (crash between =
  orphan without a row, as before). Deletes take both keys, NotFound
  tolerated (deleted-sweeper derives .vxi unconditionally;
  file_list_deleted.index_file stays vestigial false). Segment GC
  collects an orphan .vix's derived sidecar; planned keys stay
  data-only.
- Readers: two-source opens everywhere (query eval + warmup: paired
  LadderRangeSources; merge/classify/.bf assembler: paired
  HealProbeRangeSource at index_size). Reader-cache key stays the data
  key; byte caches key the two objects independently. The "index"
  cache_files step downloads the SIDECAR now; the DataFusion docs scan
  never touches it. Bloom hazard contracts preserved byte-for-byte
  (blob layout, v1 {value}\0{fid} hash form, observe_dict_key rule).
- HEAL still rewrites both objects (sidecar-only heal = later
  milestone). Gates: build ws green; vortex_index 188, core 1905,
  search 994, infra 1072, config 1973, jobs 26, api 285 unit tests
  green; integration BOTH segment modes EXIT=0.
## V2 M2 SHIPPED (2026-08-17) — all-present-columns + spliceable stats + passthrough-native merge
- DOCS (DESIGN §2): EVERY core file's docs schema = _timestamp + every
  field present in the input batches (per-file union, never the registry;
  stored NULLABLE uniformly for cross-file dtype identity) + _source +
  _original opt-in (+_o2_id rides as a normal field). The #42 index-off
  shape is now the ONLY shape. Merges: output schema = UNION of input
  docs schemas (types from latest schema when typed, else stored); a
  column absent from an input NULL-FILLS its rows — the
  derive-from-_source materialization is gone (scan-side
  json_get(_source) fallback unchanged, now serving only fields absent
  from a file's columns; derive_cs_column_from_source survives ONLY as
  the test parity oracle).
- DELETED: schema-pin lattice (SchemaPin/qualify_schema_pin/
  pinned_writer_opts/docs_schema_pinned/docs_schema_additive_mismatch_
  reason/schema_pinned result fields), merge-plan cs-candidate
  resolution + derive arm + #46 derivation column list (preserved IS the
  derivation set; type gate tightened to plan target types), MergeSource
  term/key-term/partial probes, classify's cs-column probe,
  column_store_fields EVERYWHERE user-facing (StreamSettings field,
  UpdateStreamSettings, settings-API validate/merge/normalize, trace
  seeding DEFAULT_TRACE_COLUMN_STORE_FIELDS,
  get_stream_setting_column_store_fields,
  ZO_COLUMN_STORE_DEFAULT_FIELDS) — stored/posted JSON keys
  accept-and-drop like the defined_schema_fields precedent. #52
  bloom-only demotion stays index-side only. Query-side eligibility sets
  rekeyed to schema fields; row-store star = _timestamp+_o2_id+
  referenced+_source (settings overlay gone). Web UI toggle removal
  deferred (API drops the key harmlessly).
- H1 (§3): rows-per-chunk derives from PRESENT-VALUE bytes (non-null
  value byte lengths + 4B/value overhead; offsets-span/view-len/width×
  present accounting) — never arrow width. Floor 64 / cap 65536 kept.
  Pinned: 1,500 all-null cols = EXACT equality with narrow (15196 rows);
  equal present bytes spread over 1,500 cols within 2x (14768 vs 15196 ≈
  1.03x); the 2,557-all-null-col shape saturates the cap.
- H2 (§4): DATA-object `stats` blob (o2-vix-stats-v1, tail-adjacent) —
  per docs column, one row per zone entry (1:1) with present count +
  min/max (numerics native; strings 32B prefix, max prefix-incremented;
  NaN present-not-bounding). Rows: null=unknown | [p] | [p,min,max].
  ZO_VIX_STATS_MIN_DENSITY (0.1) density-gates chunk rows (presence
  survives); ZO_VIX_STATS_MAX_BYTES (1MiB) caps the blob densest-first;
  in-flight 4x-cap eviction bounds writer RAM. `columns` property now
  [name, present_rows] pairs (plain names parse, count unknown). Blob
  emitted on EVERY non-empty file (even zero tables) so all-sparse files
  stay passthrough-eligible. Readers: VixDocs/VixReader::
  spliceable_stats() + column_presence(). Overhead measured (move-job
  200k-row prod-traces-shape corpus): 32,229 B per ~30.5 MB file =
  0.106% of total; footer props +2,316 B/file (zone_map dominates).
- MERGE (§6.1): docs-chunk passthrough (disjoint AND concat) is the
  DEFAULT — ZO_VIX_MERGE_DOCS_PASSTHROUGH + ZO_VIX_MERGE_CONCAT_ORDER
  deleted with their gating; concat inputs always legal (loud-Fatal
  gone; a concat input under a disqualified fast path falls back to the
  rebuild's forced concatenation). Per-input qualification (schema
  identity + zone table + NOW spliceable stats) and all-or-nothing
  concat qualification stay; a stats-less input DECODES (fresh stats) —
  passthrough outputs ALWAYS carry full spliced zone_map + column stats
  + summed presence counts (v1 stats-loss regression structurally
  impossible; §11 splice-parity gates pinned in tests on disjoint,
  concat and heal shapes: file-level stats fold + exact presence
  equality vs the force_decode oracle — a new TEST-ONLY BatchCaps seam
  replacing the knob-off oracles). #52 bloom-coverage projected scan on
  passthrough inputs kept. Live splice check: 4-input disjoint merge =
  4/4 chunks copied, output stats 127,486 B ≈ Σ inputs (128,911 B).
- QUERY (§6.2): per-file sort declaration keys on the FILE's row_order —
  exec.rs splits vix files into a sorted-declared table + an undeclared
  concat table (reader-cache first, else one footer probe over the cache
  ladder, memoized process-wide; unprobeable ⇒ undeclared, fail-safe).
  Full piecewise k-way = M4.
- GATES: build ws green (nightly, mimalloc default); units green:
  vortex_index 193, core 1900, search 993, config 1971, infra 1072,
  jobs 26, api 285. Integration BOTH modes EXIT=0
  (/tmp/claude-1000/m2_integration_segment_true_final.log ok/EXIT=0;
  default mode EXIT=0 on m2_integration_segment_false_rerun.log after
  ONE known-class rerun: "Trigger was not updated after 20 attempts",
  now :3713).
- Deferred: reader-side pruning CONSUMPTION of the per-column stats
  (M3/M4 with the region table); web UI column-store toggle removal;
  M1-era stats-less files heal implicitly on their first merge (decode
  path) — no forced sweep.
## V2 M3 SHIPPED (2026-08-17) — H3 streamed/budgeted downloads + spool enforcement + sidecar-only heal
- H3 DOWNLOADS (the 2026-08-17 compactor-OOM fix, §3/§7): the disk-cache
  fill path STREAMS the GetResult body into the cache tmp file (<=8MiB
  BufWriter) and renames it in — download_from_storage split into a
  shared retry/verify core over a DownloadSink (Buffer = memory-cache
  path, unchanged, skip_size-bounded callers; File = disk path). The
  3-retry short-body semantics, header-size check and file_list db-size
  reconciliation preserved exactly; reconciliation probes on the file
  sink use RANGED footer/magic reads (parquet try_parse_sized tail
  retry) — the probe never buffers either. Allocation profile: pre-M3 a
  compactor buffered EVERY merge input whole via res.bytes() (cpu_num ×
  12 jobs × ~500MB files ⇒ 24Gi OOM); now per-download RAM = one 8MiB
  buffer. cache_remote_files + background file_downloader both covered;
  gRPC peer download stays bounded (100MB/file, querier-only). Local-
  cache seeding of merged outputs was the last whole-object read-back
  (fs::read → disk::set) — now disk::set_from_local_file (file copy).
- BYTE-BUDGET ADMISSION: ZO_COMPACT_DOWNLOAD_BUDGET_MB (default 2048;
  0=unlimited) — ONE process-wide in-flight-bytes account across ALL
  merge jobs; admit when compressed_size fits, a worker holding nothing
  always admits one (oversize delays, never deadlocks/starves). Per-job
  Semaphore(cpu_num) kept: concurrency knobs cap parallelism, the budget
  caps bytes (§7). Default rationale: with streaming this bounds the
  disk-write burst + transport buffers, not RAM — 2GiB ≈ half a 4Gi
  request headroom at 12 jobs × cpu_num streams. 7 unit tests pin the
  admission semantics.
- SPOOL-ALWAYS: verified build_merge_plan spools every core merge output
  (ContainerSink::create errors, never falls back to RAM) → put_file
  streamed multipart. Enforced: the dead VixOutput::Bytes arm
  debug_asserts + release-routes >=16MiB through a scratch spool +
  put_file (never buffered storage::put). Move-job/L0 buffered arms stay
  bounded by ZO_VIX_MOVE_SPOOL_MIN_BYTES=256MiB. The RAM-built .vxi
  (small) stays as is. Legacy parquet (DataFusion) merge arm stays
  buffered — no post-reset audience, noted not fixed.
- SIDECAR-ONLY HEAL (§5): classify reasons unchanged; execution changed.
  merge_files routes single-file heal batches through
  rebuild_core_file_sidecar: index-only scan (#46 column-derived when
  the gate holds, else _source-derived) in the file's STORED row order →
  VixWriter::finish_index_sidecar (extracted byte-identical from
  finish_inner; refuses scan-count != data row_count). New .vxi
  OVERWRITES the same key; the EXISTING row updates in place
  (file_list::update_index_size_for_heal = index_size + bloom_ver=0 →
  re-enters the .bf queue with the NEW bloom, pruner fail-opens on 0
  meanwhile); no new id, no data-key change, no data upload, no
  add/delete events, NO ownership fence (idempotent; races converge via
  fail-open + re-classify). Whole-file rewrite remains ONLY for genuine
  docs rewrites: degenerate-_timestamp cleansing, or NEW oversize skips
  the untouched data object's allowance can't record. Index-off plans
  heal by dropping the sidecar (row zeroed first, delete after; orphan
  .vxi = lifecycle GC). Cache refresh: compactor evicts its own entries;
  the update broadcast now ACTUALLY evicts on queriers (event.rs:
  size-mismatched .vxi bytes from disk+memory + the memoized reader via
  new VixReaderCache::remove — its "never stale" doc updated).
  Staleness until eviction is pre-heal-correct by design. M1-era
  stats-less data objects stay stats-less (untouched by design;
  converge at first real merge).
- #51c heal-passthrough tests KEPT (none obsolete): they pin the
  surviving docs-rewriting rebuild arm (NeedsDocsRewrite fallback,
  passthrough-copy failure restart, >=2-input rebuild merges) and the
  reference oracle; comments updated. e2e_single_file_healing_compaction
  re-pinned to M3 semantics: data object BYTE-IDENTICAL across the heal,
  sidecar rewritten (hash change), row index_size == sidecar size,
  bloom_ver=0, identical query snapshots, convergence = sidecar bytes
  stop changing. Live log line (seg-mode run): "healed .../l0_....vix
  sidecar-only: data key unchanged, index_size 3326 -> 3294, took 17ms".
- GATES: build ws green (nightly, mimalloc default); units green:
  config 1971, infra 1081, vortex_index 193, search 994, core 1911,
  jobs 26, api 286. Integration BOTH modes EXIT=0
  (/tmp/claude-1000/m3-integration-segmode.log ok/EXIT=0;
  /tmp/claude-1000/m3-integration-default.log ok/EXIT=0).
- Deferred: legacy parquet merge arm still materializes outputs in RAM
  (bounded by compact.max_file_size; zero post-reset audience);
  querier cached-read mode (file_data::get(None)) still buffers whole
  cached objects on the QUERY path (not compaction; M4 candidate with
  the region table work); UploadPartCopy pure-concat merges (§7 future).
## V2 M4 SHIPPED (2026-08-17) — query path consumes M1-M3 metadata: presence/chunk pruning, region-merged ordered reads, §9 deletions
- §4 REGION TABLE (prereq the doc promised but no writer stamped): concat
  outputs now carry `row_regions` (JSON row counts of maximal internally
  ts_desc runs; ≤4096 else omitted). Decode path derives it from the
  ACTUAL stored _timestamp values (strict increase = new region — exact
  for the forced-concat rebuild and any push order); passthrough splices
  the inputs' own decompositions via begin_docs_encoded_run's new
  run_regions param (ts_desc input = 1 run; concat input = its table;
  unproven input POISONS the property — absent = fail-open full sort).
  NOT derivable from the zone map: a rebuild chunk can straddle two input
  runs, so only writer-proven decompositions are trusted. Readers
  (VixReader+VixDocs) validate like the zone table and expose
  ts_desc_row_ranges(); VixDocs now parses the FULL zone table at open.
- PRUNING TIERS (query path, all fail-open; skip log at debug):
  T1 FIELD PRESENCE (file-level, footer-only, fires BEFORE json_get):
  inject_vix_scan_pruning (ex inject_vix_numeric_bounds, flight.rs
  follower pass) extracts NULL-REJECTED columns (=, !=, <,>,<=,>=,
  [NOT] LIKE, [NOT] IN over non-null literals, IS NOT NULL,
  str_match/match_field/fuzzy_match; OR = intersection across branches —
  catches the planner's IN→OR rewrite). file_provably_skippable skips
  when presence count == 0 (unconditional: native columns authoritative)
  or the column is ABSENT from a `columns_complete` file. NEW
  columns_complete property: producers assert the all-present invariant
  (core_writer_options), merges AND over inputs; without it absence
  proves nothing (M1-era inputs' _source may hide fields) — IS NULL /
  COALESCE / IS [NOT] DISTINCT FROM / NOT / cross-column OR pinned
  fail-open (e2e lying-file test proves the skip fires pre-json_get).
  T2 vortex-footer numeric file stats kept (first-encode files), exact
  cross-type comparator (i128/trunc — no lossy i64→f64 rounding; edges
  2^63/2^53±1/NaN/inf/u64::MAX pinned).
  T3 O2 CHUNK STATS (the M2 blob, consumed at last): ColumnBound now
  BoundValue{I64,U64,F64,Str}; VixDocs::pruned_scan_ranges folds zone ×
  per-column chunk rows per conjunct — present==0 chunks prune, min/max
  windows prune, string bounds compare against the conservative 32B
  prefix min / prefix-incremented max (borderline admits pinned);
  scan_docs_opts scans only surviving contiguous row ranges
  (RowSelection::Range per run, limit threaded); empty set == whole-file
  skip — THE pruning source for vortex-stats-less passthrough outputs
  (splice-parity read test: passthrough file prunes identically, §11).
  Numeric bounds still push as vortex row filters; strings stats-only.
- §6.2 K-WAY ORDERED READS: exec.rs probe → Sorted/ConcatMergeable/
  Opaque (memo upgraded; ZO_VIX_ORDER_MERGE_MAX_REGIONS=64, 0=off).
  Declared table = ts_desc + mergeable concat, VixCoreFormat ordered-
  aware: concat scans stream VixDocs::scan_docs_ts_desc_merged — per-
  region cursor threads (1-slot channel ≈ ≤2 decoded batches each),
  max-heap on current row ts, UNOPENED regions parked at their zone
  upper bound (LIMIT satisfied by the newest region opens exactly 1 —
  pinned), chunk pruning clips region ranges, index selections split by
  region, zero-copy slice emission, _timestamp add+strip, each open
  region grows the DataFusion reservation 3×chunk (pool pushback).
  Unproven concat under the ordered source = HARD ERROR (routing keeps
  those undeclared; #51c-c hazard test kept). e2e: declared+ordered over
  a concat file = NO SortExec AND exact top-k; cross-region EQUAL
  timestamps count-preserving with documented deterministic tie order;
  interleaved region ranges pinned. Fast path: simple_select narrows
  concat files to per-region positional candidates (≤ regions×limit ts
  reads) before the exact by-value top-k.
- §9 DELETIONS: generate_quick_mode_fields GONE with its first/last/both
  strategies + tests; CTE/join/subquery star = registry-star BOUNDED by
  the statement's referenced columns (+_timestamp/_o2_id/fts-on-match_all
  /_original rules) — H4 pinned: 5000-field-registry CTE star = 2
  columns (the #45 plan-time shape). Empty referenced set fails open to
  the full registry expansion (nested star). Row-store star UNCHANGED.
  quick_mode API field accept-and-ignore; quick_mode_* config knobs stay
  for /status only. Task-5 pin: present column reads native (no _source
  fetch), absent column json_get + NULL-correct.
- Also: fixed pre-existing racy runtime_metrics test assertion (global
  vec vs parallel harness; == before+1 → >= before+1; serial-verified).
- GATES: build ws green (nightly, mimalloc default); units green:
  vortex_index 203, search 995, core 1911
  (/tmp/claude-1000/m4-core-units-4.log EXIT=0), config 1971, infra
  1081. Integration BOTH modes EXIT=0
  (/tmp/claude-1000/m4-integration-segmode.log ok/EXIT=0;
  /tmp/claude-1000/m4-integration-default.log ok/EXIT=0).
- Deferred: opener memory estimate for eager many-region merges stays
  heuristic (3×chunk per open; lazy opening keeps real counts ~1);
  region-merge thread-per-open-region could pool (bounded by the 64
  cap); querier cached-read whole-object buffering (M3 note) still open;
  dictionary-served top-k / roaring caches audit (§12.4) untouched.
## v2 M5 — final gates + A/B (2026-08-17)
- FINAL GATES at 84821aa785 (+ the scan_bench example fix in this
  commit): build ws EXIT=0 (/tmp/claude-1000/m5/build-ws.log). Units all
  EXIT=0: vortex_index 204, core 1911, search 995, infra 1081, config
  1974, jobs 26, api 286 (/tmp/claude-1000/m5/units-*.log). First core
  run EXIT=101 — M4's ColumnBound BoundValue rename broke the
  scan_bench EXAMPLE compile (NumScalar::I64 at 2 bound sites; example
  only, no lib/test change): fixed here, suite rerun green
  (units-openobserve-core-2.log EXIT=0). Integration BOTH modes EXIT=0
  FIRST run, none of the three flake families appeared
  (/tmp/claude-1000/m5/integration-segmode.log ok 70.29s;
  integration-default.log ok 41.85s).
- A/B PROTOCOL: baseline = prebuilt release worktree at 5b9e2ddb27
  (/home/zhichen/work/obs-baseline, pre-v2). Each side gens its OWN
  corpora, IDENTICAL args (formats cross-incompatible by design):
  `merge_bench gen <dir> 8 2000000` (disjoint) + the same `--heal`
  (index-off). Medians of 3, sides ALTERNATED run-by-run, idle box.
  PARITY GATE PASSED EVERYWHERE: gen per-file rows/terms identical
  (16,000,000 / 81,532,627 summed); all four merge outputs 16,000,000
  rows / 70,286,992 terms; scan produced counts identical per variant;
  query per-class results identical; v2 merged-vs-healed row-order
  digest EQUAL (rows=16000000 terms=70286992 digest=5fc81931afbf8cfa).
  Baseline merged-vs-healed compare undefined BY DESIGN (cs-only merge
  schema vs index-off all-columns heal schema).
- MERGE fast path (8x2M disjoint): baseline 29.54s median (29.14/30.59/
  29.54), VmHWM 7,601,632 kB, out 3,758,984,499 B; v2 39.95s
  (39.05/39.95/40.18), VmHWM 8,863,236 kB, out 2,465,523,284 data +
  2,244,877,542 sidecar = 4,710,400,826 B. VERDICT: wall +35%, VmHWM
  +17%, total bytes +25.3% — the priced-in all-columns cost (docs
  2350.2 vs 1443.9 MiB; index identical 2140.9 MiB; v2 docs
  passthrough ACTIVE on all 8 inputs, baseline 0 — knob dark at that
  SHA; the wall delta tracks the +906 MiB docs write + ~0.9 GiB larger
  corpus read). Corpus per-file: v2 290.6 data +
  287.0 index vs baseline 467.4 MiB single (+23.6%).
- HEAL (8x2M index-off, merge --rebuild): baseline 145.33s median
  (152.11/145.33/144.03), VmHWM 11,829,528 kB; v2 152.90s
  (152.90/152.23/153.49), VmHWM 8,760,340 kB. VERDICT: wall +5% (the
  70M-term index rebuild dominates both sides), peak memory -26% (v2
  copies docs chunks verbatim instead of re-encoding); output bytes
  EQUAL (4,708,259,509 vs 4,710,400,826 — index-off corpora are
  all-columns on both sides). Sidecar-only heal is NOT in merge_bench
  (rebuild_core_file_sidecar has no bench hook) — NOT re-benched;
  stands on the M3 integration evidence: "healed ... sidecar-only:
  data key unchanged, index_size 3326 -> 3294, took 17ms".
- SCAN (each side's own merged output, density 0.10, medians of 3):
  bytes-full -5% (225->212ms); ranged-needle -9% (87->80ms, fetched
  40.2 MB targeted vs 44.6 — but 1600 chunk-fetches vs 20: fine
  locally, an S3 round-trip amplification to watch). REGRESSIONS,
  flagged plainly: in-memory 10% selection +45% (127->184ms) / +34%
  with 4 threads — finer chunk granularity decodes 3640 batches vs
  233; RANGED 10% selection +318% (104->435ms, 0t) / +230% (4t) and
  ranged-full +271% — v2 chunk-granular fetches pull whole all-columns
  chunks: 146 fetches / 2367 MB = essentially the ENTIRE docs blob for
  a [_timestamp,duration] projection vs baseline's projection-targeted
  20 fetches / 44.6 MB; a uniform 10% selection defeats chunk pruning
  (every chunk hit) and v2 ranged reads are no longer per-column.
  rng-allcols +78% (3.87->6.89s: decodes 16 native columns vs 6).
  open_ranged 0.51->5.11ms (footer+stats fetch 0.3->2.8 MB). Follow-up
  filed below.
- QUERY classes (O2_VIX_FILE=<side merged>, 3 repeats): all within
  +/-6% — count Exact 629->642us, eval Exact dense 1.37->1.38ms,
  Exact needle 797->843us, And 2.25->2.34ms, Prefix walk 805->855us,
  count Prefix 754->721us, Contains full-field 266->282ms. Results
  identical cross-side (532593/532593/1/0/241/16000000/6562).
- HONEST SUMMARY: v3 buys the sidecar split, all-columns docs, heal
  peak memory -26%, digest-equal heal outputs, and unchanged index
  query latency at +25% merged storage, +35% fast-merge wall, and a
  ranged-scan byte-amplification regression on dense selections
  (whole-chunk fetches). NEW DEFERRED: projection-aware / per-column
  chunk ranges for the ranged docs scan (dense-selection fetch profile
  146x2367MB -> per-column), and pooling the needle path's per-chunk
  round trips (1600 fetches) before any S3-backed rollout leans on
  ranged mode.
- Artifacts: corpora + outputs kept for inspection at
  /home/zhichen/work/vixbench-m5 (17G total: baseline corpus 3.7G +
  heal 2.3G + outs 7.9G; v2 corpus 4.6G + heal 2.3G + outs 8.8G;
  logs + analysis.txt in vixbench-m5/logs). obs-baseline worktree left
  in place at /home/zhichen/work/obs-baseline.

## v2 M6 — ranged-read regression fixes + re-measure (2026-08-17)
- DIAGNOSIS of the two M5 query-path regressions (footer-probe evidence
  on the M5 merged corpus, tests.rs::m6_probe_docs_layout kept as the
  manual diagnostic): projection WAS pushed into the vortex scan
  (container.rs scan_blob_streaming .with_projection) and honored per
  segment — the fault was (iii), the LAYOUT x COALESCER interaction.
  The passthrough TableStrategy wrote one flat leaf per (pushed chunk,
  column) in push order: 58,240 leaves, per-column seg-id stride 16,
  chunk stride ~678KB — UNDER the object-storage coalescer's 1MiB gap
  (CoalesceConfig::object_storage, 1MiB/16MiB), so a [_timestamp,
  duration] projection whose true bytes were ~45.6MB fetched the whole
  2.44GB docs blob as 16MiB spans (2464MB/16MiB = 147 ~= the observed
  146). Needle: ts(col0,128B)+gap(col1,12.5KB)+duration(12.4KB) ~= the
  observed ~25KB/fetch, selected chunks ~1.5MB apart > 1MiB -> no
  cross-chunk merge -> 1600 singleton GETs. First-encode files never
  had the problem (vortex's default pipeline buffers per column:
  corpus probe shows per-column contiguous multi-MB runs).
- FIX (write-side; src/vortex_index/src/clustered.rs, wired in
  container.rs docs_passthrough_strategy): ClusteredDocsStrategy —
  (1) STRIPES: physical segment order == SequenceId order (vortex
  BufferedSegmentSink collapses before appending), so ids are minted
  [stripe, column, chunk]: each ~160MiB-of-output stripe lands
  column-major (per-column contiguous runs; unprojected columns become
  skippable >1MiB gaps). Stripe size estimated in OUTPUT bytes with an
  online compressed/raw ratio (prior 1/4). (2) DECODED-RUN COALESCING:
  consecutive decoded-family chunks (slice-guard canonicalizations,
  re-encode runs, tiny <=16KiB encoded slices — _timestamp's
  self-contained sequence slices included) concat up to 128Ki rows /
  4MiB decoded and compress ONCE — recovering coarse decode batches
  (3640 -> 247) at no extra decode cost (those chunks were already
  recompressed one-by-one). _source-scale encoded chunks still copy
  byte-identical. Work spawns eagerly (compress on the cpu pool, only
  sink emission waits on stripe order): parked memory ~= one stripe of
  compressed bytes. Layout TREE unchanged (struct -> chunked -> flat);
  zone table / stats blob / splice / row order untouched; readers need
  zero changes. PROOF layout-only: M5-merged vs M6-merged row-order
  digest EQUAL (5fc81931afbf8cfa), and merged-vs-healed still EQUAL.
- REGRESSION PINS (fail against the old layout at 84-98% of the blob
  fetched): vortex_index tests::ranged::passthrough_projection_fetch_
  budget (2-col projection <15% of blob bytes, coarse decode batches,
  row parity) and passthrough_needle_fetch_budget (needle fetches <=12
  and < selected rows, <15% bytes); clustered::tests pin the physical
  stripe clustering + run coalescing + roundtrip. DESIGN-V2 §6.1/§7
  updated with the stripe layout rule.
- GATES at 7815ade8e4: build ws EXIT=0 (/tmp/claude-1000/m6/
  build-ws.log). Units all EXIT=0: vortex_index 207, core 1911,
  search 995, infra 1081, config 1971, jobs 26, api 286
  (/tmp/claude-1000/m6/units-*.log). Integration BOTH modes EXIT=0
  FIRST run, no flake families (integration-segmode.log ok 70.03s;
  integration-default.log ok 41.68s).
- M6 RE-MEASURE (same M5 protocol: idle box, medians of 3, sides
  alternated run-by-run, corpora REUSED from M5 — gen writes
  first-encode files through the unchanged standard writer, so corpus
  bytes are unaffected; merged/healed outputs rebuilt into out-m6/).
  ALL PARITY CHECKS PASSED (rows/terms/produced/results identical).
  Format: baseline / M5-v2 / M6-v2.
  - MERGE fast path: 28.49s / 39.95s / 30.03s (+35.2% -> +5.4%);
    VmHWM 7.65 / 8.86 / 7.93 GB (+16.6% -> +3.6%). Coarse-block
    recompression made the merge cheaper than M5's per-tiny-chunk
    compressor calls. Docs 2324.9 MiB (was 2350.2); total stored
    bytes still +24.6% vs baseline (priced-in all-columns cost).
  - HEAL: 145.21s / 152.90s / 147.11s (+5.2% -> +1.3%); VmHWM
    -25.9% -> -33.8% (7.87 GB).
  - SCAN (the M6 targets): ranged-sel-0t 102.79ms / 434.66 / 93.30
    (-9.2% vs baseline) at 30 fetches / 44.6MB vs baseline 20 / 44.6
    (M5: 146 / 2367MB) — success bar was <=~90MB and wall within 10%:
    BEAT on both. ranged-sel-4t 65.97 / 223.19 / 55.94 (-15.2%).
    ranged-needle 87.32 / 80.26 / 78.21 (-10.4%) at 30 fetches (bar:
    tens, <=2x baseline's 20 = 40; M5 was 1600) and 44.6MB == baseline.
    bytes-sel 126.63 / 184.49 / 103.22 (+45% -> -18.5%); bytes-full
    -39.2%; ranged-full +271% -> +2.2%; rng-3col -10.9% (45 fetches).
    open_ranged 0.50ms / 5.11 / 1.10 (+119% vs baseline, absolute
    trivial; footer+stats fetch 0.6MB vs 0.3, was 2.8).
  - QUERY classes: all within +/-6.6%, results identical (index path
    untouched).
- STILL REGRESSED, plainly: rng-allcols 3.84s / 6.89 / 10.67 (+178%
  vs baseline, +55% vs M5-v2). Two stacked causes: v2 decodes 16
  native columns vs baseline's 6 (all-columns by design, the M5 +78%),
  plus NEW mixed-granularity cost — projections that include _source
  keep the 4.4K-row batch grid and partial-decode the coarse narrow
  chunks per slice. Pure-_source reads are unaffected (rng-source
  +0.3%; SELECT * stays a _source-only read per DESIGN §2.1), and
  narrow-column projections are strictly faster — the trade hits only
  wide projections that mix _source WITH many native columns. Deferred
  with options noted (smaller coalesce rows cap trades allcols cost
  against narrow-scan batch counts; per-slice decode caching is a
  vortex-side fix).
- Artifacts: /home/zhichen/work/vixbench-m5/{analysis-m6.txt,logs-m6/,
  */out-m6/} alongside the M5 set (46G total now); driver + gates
  logs under /tmp/claude-1000/m6/.

## v2 M7 — #52 default-on (2026-08-17)
- SHIPPED (commits c86c477f05 + 2e3fb72394 + 8f84daa116): AUTO
  bloom-only demotion is ONE shared rule
  (vortex_index::resolve_auto_bloom_only) at TWO write sites — merge
  plans (input-dictionary counts, as before) and the writer's own
  finish (FIRST ENCODE + unspilled rebuilds; spilled term maps skip —
  partial counts would half-cover the bloom). A demoted-at-birth field
  is sidecar-BYTE-IDENTICAL to a construction-list demotion (pinned).
  Defaults flipped (env-overridable both ways): ZO_VIX_BLOOM_COMPOSITE
  false→TRUE, ZO_VIX_BLOOM_ONLY_AUTO_RATIO 0.0→0.5 (floor 65536,
  NEVER empty). STICKY convergence: build_merge_plan folds inputs'
  FIELD_TYPE_BLOOM markers into the plan and
  merge_inputs_lacking_term_capability skips plan-bloom-only fields —
  demoted inputs carry no dict terms for the count rule, so without
  both, gen-2 merges degraded the field to capability-less (coverage
  lost) and classify heal-LOOPED demoted-at-birth files (pinned:
  classify == Current, sticky merge, mixed demoted+legacy convergence,
  search-side skip+filter-back, pruner e2e demoted blob → .bf →
  hit-keep/miss-drop with ZERO per-stream config). Un-demotion =
  NEVER-list + heal. DESIGN-V2 §5 updated.
- P0 fixed mid-measure (8f84daa116, caught by the new bloom-section
  accounting): the merge fast path tracked configured bloom fields
  per-field even when demoted → EMPTY (or mixed-merge PARTIAL)
  reject-all per-field section → wrong file drops on equality. Merge
  now filters demoted fields from per-field tracking exactly like the
  build path always did; absent section = no-info (kept).
- GATES at 8f84daa116: build ws EXIT=0 (/tmp/claude-1000/m7/
  build-ws-2.log). Units EXIT=0: vortex_index 210, core 1914, search
  997, config 1971, infra 1081. Integration BOTH modes EXIT=0 first
  run, no flakes (m7/integration-segmode-2.log ok 70.18s;
  integration-default-2.log ok 41.69s).
- MEASURE (idle, medians of 3, M6 numbers = control; corpus REGEN with
  identical gen args — writer-side AUTO demotes trace_id/span_id/
  http.url/service_pod_name from birth, each ≥65536 distinct at ratio
  ~1.0):
  - corpus/file: data 304,760,020 B BYTE-IDENTICAL; sidecar 300.9→71.1
    MB (−76.4%); terms 10,191,741→2,191,741 (−78.5%, exactly 4×2M);
    gen wall 18.6→~19.7s (~+6%: finish carve + hash + composite build).
  - merge (8x2M fast path): wall 30.03→24.66s (−17.9%; triple
    24.66/24.41/27.17, first suite 24.58 median — consistent); VmHWM
    7.93→7.21 GB (−9.1%); terms 70,286,992→6,286,992 (−91.1%); index
    blob 2140.9→393.2 MiB (−81.6%); docs identical (passthrough 8/8).
  - index-share subtraction (ZO_VIX_INDEX_DISABLED_STREAM_TYPES=logs,
    once per corpus, idle): control 30.03−13.34=16.69s vs M7
    24.66−10.07=14.59s (−12.6%): the term k-way shrank −91% but the
    composite coverage scan (4 high-entropy columns × 16M × 8 inputs
    decode+hash) + the 128 MiB SBBF build are new index-side costs.
  - query classes (M6 merged = control): service classes FASTER on the
    demoted file (count Exact 647→326µs, eval Exact 1.44→1.06ms —
    smaller dictionary). DEMOTED-FIELD NEEDLE, plainly: postings
    815µs/file → bloom prune decision 0.02µs/file (miss=drop 0.01µs)
    + 635ms filter-back column scan on the ONE surviving file (4
    threads: 632ms — no parallel win on this shape); And-narrowed
    (svc postings 532K rows → point-read filter) 543ms. M4 chunk-stat
    tier prunes ZERO chunks on random 32-hex equality (expected).
    Prefix (was 807µs) and Contains (was 277ms) have NO demoted
    equivalent — engine scan fallback. The trade: single-file needle
    ~780x slower when the file holds the value; every non-holder is
    dropped for ~0.02µs without opening anything, and the index that
    served it is −81.6% bytes.
  - blooms/.bf: corpus/file 4 MiB trace-only → 16 MiB composite
    (8.54M items); merged 32 MiB trace-only → 128 MiB composite
    (65.49M items). Merged sidecar 2,244.9→412.3 MB. Merged TOTAL
    (data+sidecar) 4,683.9→2,851.3 MB (−39.1%) — vs the v1 baseline
    3,759.0 MB the v2 total is now −24.1% (M5 measured +25.3%; the
    all-columns cost is now paid for by the index diet).
  - correctness at scale: fast-merge vs full-rebuild digest EQUAL
    (rows 16M, terms 6,286,992, digest cb3c1efc20be5a93); rebuild
    131.69s.
- DEFERRED, plainly: (1) a field listed in stream-settings
  bloom_filter_fields loses .bf pruning once demoted (per-field
  section rightly absent; the pruner's composite fold only covers
  UNCONFIGURED fields) — correct but unpruned; fix = pruner-side
  composite fallback for per-field predicates, or drop demoted fields
  from the setting at v2 launch. (2) 4-thread filter-back scan shows
  no speedup on the single-column equality shape. (3) spilled (>1.5
  GiB term map) rebuilds skip first-encode AUTO and converge at their
  next merge instead.
- Artifacts: /home/zhichen/work/vixbench-m5/{analysis-m7.txt,logs-m7/,
  v2/corpus-m7/,v2/out-m7/}; gates logs /tmp/claude-1000/m7/.

## v2 M8 — docs chunk-size sweep (2026-08-18)
- QUESTION (owner): is the 4 MiB docs-chunk budget right, and should the
  vortex file just be ONE big chunk? Answer below, measured.
- KNOB (only engine change, f909d78943): `ZO_VIX_DOCS_CHUNK_MAX_ROWS`
  (default 65536) — the rows-per-chunk ceiling of the
  `clamp(budget/avg_present_row_bytes, 64, cap)` sizing is now liftable;
  `0` = the historical cap. Default proven byte-for-byte: unit-pinned
  (m8_docs_chunk_max_rows_caps_and_lifts + _plumbs_into_the_chunking), and
  at bench scale a default-env corpus regen is BYTE-IDENTICAL to corpus-m7
  (8/8 data + 8/8 sidecars) and the fresh merge BYTE-IDENTICAL to
  out-m7/merged.vix. Probe tooling: tests.rs m8_probe_chunk_geometry
  (manual) + scan_bench `rng-src-needle` variant (f54c0f5e96, 9639b0855f,
  1241bd3311).
- GATES at f909d78943 (writer touched → full gates): build ws EXIT=0
  (/tmp/claude-1000/m8/build-ws.log). Units EXIT=0: vortex_index 213 (+2
  new), config 1974, core 1914, search 997, infra 1081. Integration BOTH
  modes EXIT=0 FIRST run, no flake families (integration-segmode.log ok
  69.83s; integration-default.log ok 41.38s).
- PROTOCOL: gen 8x2M per setting (deterministic, #52 defaults live);
  medians of 3 with settings ALTERNATED between repeats; idle box; sizes
  once; RSS via GNU time -v; chunk counts from the zone table (footer).
  S1 reuses corpus-m7/out-m7 (byte-identity proven above). Settings =
  budget/max_rows → rows-per-chunk: S1 4MiB/65536→4,405; S2 16MiB/65536→
  17,623; S3 64MiB/65536→65,536 (cap-saturated); S4 2GiB/32M→2,000,000
  (whole gen file = ONE chunk).
- TABLE (S1 / S2 / S3 / S4; merged file unless noted):
  chunk count corpus-file:      455 / 114 / 31 / 1
  chunk count merged (zones):   3,640 / 912 / 248 / 8
  gen wall per file:            19.9s / 18.9 / 19.1 / 23.1 (+16%)
  gen VmHWM:                    5.67GB / 5.51 / 5.51 / 12.0 (2.12x)
  corpus data B/file:           304,760,020 / −0.28% / −0.15% / −3.04%
  merge wall:                   29.67s / 22.12 (−25%) / 18.72 (−37%) / 27.90 (−6%)
  merge VmHWM:                  7.22GB / 5.96 (−17%) / 4.61 (−36%) / 7.38 (+2%)
  merged out bytes:             2,439,014,956 / −0.06% / −0.06% / +0.61%
  ranged-sel-0t:                94.4ms / 93.7 / 87.7 / 90.7 (fetch 44.6-45.1MB all)
  ranged-needle (narrow):       80.4ms / 78.4 / 74.4 / 78.4 (bytes equal)
  rng-src-needle (1600-hit):    1.73s / 3.42 (+98%) / 4.15 (+140%) / 4.09 (+136%)
  rng-source (10%):             3.70s / 3.61 / 4.45 (+20%) / 4.36
  rng-allcols:                  10.81s / 6.94 (−36%) / 7.27 / 6.98 (−35%)
  bytes-full:                   148.9ms / 138.5 / 214.1 (+44%) / 212.8
  point-read 1 row _source:     2.27ms / 3.50 / 4.82 / 4.81 (corpus: 1.75/2.90/4.24/30.95 = 17.7x)
  LIMIT-100 _source:            3.78ms / 7.84 / 25.8 / 29.9 (corpus: 2.1/5.9/21.4/61.9 = 29x)
  LIMIT-100 narrow cols:        ~1ms at every setting (chunk-size-immune)
  ts-window over-read (72k rows): 1.04x / 1.22x / 1.82x / 27.8x
  scan suite peak RSS:          2.84GB / 2.95 (+4%) / 3.46 (+22%) / 3.48 (+22%)
  query classes + RSS:          flat everywhere (index path untouched; ≤+3.5% RSS;
                                filter-back 631→530ms at S4, bulk scan likes big chunks)
- S4 "ONE BIG CHUNK", plainly: (1) the merge does NOT splice giant chunks —
  the input scan slices them at ~65,536 rows and slice-guard canonicalizes,
  so the −3.0% L0 size win is ERASED after one merge (+0.61% vs S1-merged)
  and physical leaves land at 65Ki/131Ki rows anyway under 8 gargantuan
  zone entries; (2) a one-chunk file has NO intra-file pruning (corpus
  ts-window: NO PRUNING BASIS, 1000x over-read; merged 27.8x); (3) single-
  hit decode 17.7x (30.9ms) and LIMIT-100 _source 29x (61.9ms) on the
  corpus file; (4) gen VmHWM 2.12x; (5) the vix_format ranged-scan
  reservation is 4x budget → 2GiB budget = 8GiB PER SCAN: un-deployable.
- VERDICT: (1) size wins come ONLY from high-entropy ID columns (span_id
  −18%, trace_id −9%, service_service.version −46% at S4) and only at
  near-one-chunk; `_source` — 59% of the blob — is FLAT-TO-WORSE (+0.3%)
  at every bigger size, and S2/S3 storage is ±0.3%. (2) latency/RSS losses
  land exactly on the product's hottest read shape: matched-row `_source`
  point reads (rng-src-needle +98% at S2, +140% at S3; per-hit 2.27→3.50→
  4.82ms) plus LIMIT-head reads and pruning granularity; wins land on
  compactor economics (merge wall −25/−37%, VmHWM −17/−36%) and wide
  projections (allcols −36%). (3) 4 MiB IS the right default — it wins
  every needle/LIMIT/pruning shape and loses only bulk-scan/merge costs.
  (4) if compactor wall/memory becomes the binding constraint again, 16 MiB
  is THE defensible experiment: storage-neutral, query-classes flat, its
  only real cost is ~2x on _source point-read decode — flip it with the
  new knob, no format change, old files self-describe. (5) one-big-chunk
  is REJECTED: strictly dominated at every layer it touches.
- Artifacts: logs + parsed medians in /home/zhichen/work/vixbench-m5/
  {analysis-m8.txt,logs-m8/}; gates logs /tmp/claude-1000/m8/. Cleanup:
  deleted corpus-m8-s2/s3/s4, out-m8-s1/s2/s3/s4 and the byte-identity
  regen (~23.4GB freed); control corpus-m7 + out-m7 kept.

## v2 M9 — chunk default 16MiB (owner call)
- FLIP (f2e19e7438, owner call 2026-08-18 on the M8 table): docs chunk
  budget default 4MiB → 16MiB in BOTH definition sites in agreement —
  `ZO_VIX_DOCS_CHUNK_BYTES` (config env default, help cites the call) and
  `DEFAULT_DOCS_CHUNK_BYTES` (writer const: the config=0 fallback and the
  library `VixWriterOptions::default()`; production wires the config via
  core_writer.rs). Basis: M8 S2 — merge −25% wall / −17% VmHWM,
  storage-neutral, ~2x `_source` point-read decode; 4MiB remains the
  point-read-optimal knob. `ZO_VIX_DOCS_CHUNK_MAX_ROWS` stays 65536.
  Ranged-scan pool reservation (4×chunk) → 64MiB/scan — already sized fine
  in analysis-m8.txt; the one pool-size test (64MiB ample pool) still fits.
- Test recalibration (default-derived geometry only; every byte/fraction
  bound kept pre-flip): docs_chunk_budget_bounds_point_read_bytes corpus
  widened 384→1792 hex chars/row so the encoded blob (94.7MB measured)
  still dwarfs 4 budgets (premise margin restored; 12 chunks, 1-row fetch
  8.29MB ≤ budget+512KiB); docs_chunk_default_budget_unchanged_for_normal_
  rows 6k→30k rows (6k fit ONE 16MiB chunk = vacuous) + max_chunk<rows
  guard. M6 fetch-budget pins passed UNCHANGED (fixture pins 64KiB input
  chunks; the read coalescer is vortex object_storage, budget-independent).
- GATES at f2e19e7438 (writer default changed → full set; all EXIT=0 first
  run, no flake reruns; logs /tmp/claude-1000/m9/): build ws; units
  vortex_index 212+1, config 1971, core 1914, search 997, infra 1081;
  integration BOTH modes (segmode ok 70.26s, default ok 43.42s).
- CONFIRMATION (single run, idle box, pure defaults — no ZO_VIX env): gen
  8x2M → 0000.vix 303,912,992 B BYTE-IDENTICAL to the M8 S2 probe,
  zone_chunks=114 rows/chunk median 17,623 (=S2); merge docs_batches=912
  (=S2), wall 19.29s (S2 median 22.12s, S1 29.67s), VmHWM 5,957,692 kB
  (S2 5,968,800). Generated corpus + merged output deleted after.

## v2 M10 — #51b parallel k-way term merge (2026-08-18)
- SHIPPED (4aa0c5321a code, 888812d519 pins, 44b4d7b3f7 design,
  +merge_bench logger): `partition_bounds` is REAL — the stub after the
  2026-07-29 prod dictionary corruption is replaced by the output-keyspace
  sampler the lesson demanded. Candidates = the inputs' dict-block index
  first keys (resident walk, no block decode) remapped to OUTPUT fids;
  weighted quantiles over per-block key counts; sorted+deduped; never a
  fabricated byte string. Bounds only emitted when every input's fid remap
  is strictly increasing; each stream translates each bound into its OWN
  key space (`translate_bound` — provably exact on every emittable key, a
  consistent tiling for dropped keys), so the existing raw-key filter +
  `predecessor_block` seek are reused unchanged. Non-monotone remap ⇒ no
  bounds ⇒ the sequential path; the in-stream strictness guard and
  `write_index_blobs`' cross-range hard rejection (prev part last key <
  next part first key) stay as structural backstops. Blooms: per-range
  hash accs merged before ONE final SBBF build — byte-identical to
  sequential (deliberate deviation from the same-geometry-OR sketch:
  in-tree mechanism, strictly stronger). #52 bloom-only keys route to
  their range's worker via observe_bloom_only_key unchanged. Heal/rebuild
  and fallback-to-rebuild untouched (parallelism is fast-path-only).
- Knob `ZO_VIX_MERGE_KWAY_THREADS` (opts.merge_kway_threads): 0 default =
  min(available_parallelism, 8); 1 = exactly one range (sequential, same
  code); always capped by the ZO_VIX_MERGE_THREAD_NUM budget (stacks,
  never widens; no second pool). Deviation from the literal "R−1 splits
  for R=knob": knob counts WORKERS, ranges over-partition 4x onto the
  existing shared cursor (in-tree skew rationale kept; digest pins hold
  for any range count).
- PINS (all green, first run): tests::m10_parallel_kway — R=1 vs R=8
  digest+bloom+partials on disjoint (with direct-build oracle) /
  overlapping / demoted-mixed (M7-style) corpora; adversarial: a bound
  EXACTLY on a fid's first key (proven placement, 610/590 weighting), one
  fid >90% of keys (≥80% of bounds must land inside it — weighting
  sanity), more ranges than distinct keys (empty ranges harmless),
  single-input; sampler contract (real remapped keys, strictly ascending,
  dedup, ranges≤1 ⇒ none, non-monotone ⇒ none); translate_bound
  exactness brute-forced (k ≥ T(B) ⟺ remap(k) ≥ B over every emittable
  key x bound) + monotonicity. AT SCALE: merge_bench compare of R1 vs R8
  outputs (full term stream incl. postings + every docs column):
  equivalent — rows=16,000,000, terms=6,286,992, digest cb3c1efc20be5a93.
- GATES (all EXIT=0 first run, no flake reruns; logs /tmp/claude-1000/m10/):
  build ws; units vortex_index 221 (212+9 new), config 1971, core 1914;
  integration BOTH modes (segmode ok 70.37s, default ok 45.85s).
- MEASURE (8x2M pure-default corpus, medians of 3 alternated, idle box;
  analysis-m10.txt + logs-m10/ in vixbench-m5):
  merge wall      R1 19.36s → R8 17.84s   (−7.9%)
  VmHWM           R1 5,962,288 kB → R8 5,949,320 kB (1.00x; H3 ≤1.2x PASS)
  index share     R1 14.50s → R8 12.91s   (−11.0%; index-off 4.86/4.93s)
  k-way phase     2.232s → 0.845s (−62%, 2.64x on 8 workers; 32 ranges —
                  phase logs verify 1-range/1-worker vs 32/8 engagement)
- HONEST VERDICT: #51b's own phase is −62% but the total merge wall moves
  only −7.9% — the index-side wall now sits in the COMPOSITE COVERAGE
  SCAN, ~11.4s (docs staging 15.17s indexed vs 3.79s index-off): hashing
  demoted/bloom-only values off the streamed docs columns (birth-demoted
  IDs + a merge-time AUTO demotion of `duration`, distinct≈13.2M/16M).
  That is ~64% of the merge wall and ~88% of the index share — the next
  lever. Then SBBF build 0.91s (single-threaded, 134MB section), k-way
  0.85s, encode 0.21s, table load 0.13s. K-way scaling bound by per-range
  stream setup (inputs x ranges) + sampler walk; not worth further tuning
  while the coverage scan dominates.

## v2 M11 — cache_latest_files default-on (2026-08-18, owner call)
- SHIPPED (cdce803495): OWNER ORDER 2026-08-18 "cache_latest_files
  default to true — we need cache latest files." Flips (env-overridable
  both ways): ZO_CACHE_LATEST_FILES_ENABLED false→TRUE (senders broadcast
  new file_list rows — db::file_list::set gate + compactor write_file_list
  gate — and queriers download; also forces the file_hash query partition
  strategy in cluster search so queries land on the caching node) and
  ZO_CACHE_LATEST_FILES_DELETE_MERGE_FILES false→TRUE (deleted rows evict
  the input data object AND its .vxi sidecar). _PARQUET stays true (covers
  .vix data + .vxi sidecar, help text formalized). _DOWNLOAD_FROM_NODE
  stays FALSE — owner holds peer-to-peer fill back; queriers fill straight
  from object storage (help text cites the hold-back).
- v2-correctness (the flip turns ON paths M1/M3 wired dark; whole path
  traced): senders = ingester move (jobs parquet.rs:774 →
  db/file_list/mod.rs:83 queue → jobs files/broadcast.rs 1s drain,
  ingester-only), compactor merge (merge.rs:2051 sender gate, put+deleted
  rows), sidecar-only heal (merge.rs:1758, UNGATED put row,
  deleted=false). Routing: online queriers only, per-file consistent-hash
  owner on BOTH role-group rings (db/file_list/broadcast.rs:47-119).
  Receiver event.rs: M3 stale-sidecar eviction runs FIRST (ungated for
  queriers), then the caching block enqueues data + sidecar (index_size>0
  = v2 sidecar-exists marker, exact object size; non-.vix keys derive no
  sidecar); downloader (infra file_downloader) dedupes queued/processing,
  prioritizes fresh files (LIFO priority queue), and SKIPS keys already on
  disk — so a heal's evict-then-refetch re-downloads ONLY the rewritten
  sidecar and never touches still-valid data bytes; heal rows are puts so
  delete_merge_files can never evict them. Undersized/over-age rows skip
  the WHOLE enqueue — safe post-heal (stale bytes already evicted, next
  query fills on demand). Refactor: collection + evict-key logic extracted
  to pure collect_files_to_download / merge_evict_keys, behavior identical.
- PINS (event.rs tests extended + config tests):
  m11_new_file_event_enqueues_data_and_sidecar (both rows, sizes, id
  propagation; index_size=0 → data only; parquet key → no sidecar;
  undersized skips whole), m11_merge_event_evicts_inputs_with_sidecars,
  m11_sidecar_only_heal_refreshes_sidecar_keeps_data (THE flip-sensitive
  case: data bytes survive, stale sidecar evicted, re-enqueue lists the
  new size, put-row never reaches the evict list),
  m11_cache_parquet_off_enqueues_nothing (env-off escape hatch),
  config::tests::cache_latest_files_defaults_m11 (defaults pin incl.
  download_from_node=false "owner holds peer-to-peer fill back").
- GATES (all EXIT=0 first run, no flake reruns; logs /tmp/claude-1000/m11/):
  build ws (build-ws-final.log); units config 1972 (+1 defaults pin),
  openobserve-api 290 (+4 M11 pins), core 1914, infra 1081; integration
  BOTH modes (segmode ok 70.47s, default ok 45.88s).
- DESIGN-V2 §7: launch-default one-liner (queriers cache latest files,
  peer fill off).

## V2 PROD LAUNCH (2026-08-18)
- TAG v0.93.0-vix-20260818.107 (engine 60aa9edd10c0, both registries).
  prod-ops PR #428 (da90ad0 launch + 7f2facc review-P0 + e480435
  review-P1s), merged 11:28:35Z --admin (merge 9df42ba0a276); Argo synced
  11:28:50Z; ALL 22 obs pods on .107 by 11:44Z (~16 min; karpenter
  "Underutilized" consolidation churned ~5 pods mid-roll, all clean
  0-restart replacements — idle soak pods are consolidation bait).
- FRESH WORLD: DB obs20260818 created on obs-prod RDS (PG 17.9) BEFORE
  merge, verified connectable; prefix obs-20260818/; lifecycle rule
  obs-20260818-retention-1d ADDED merge-not-replace (obs-20260803-purge
  + obs-20260817-retention-1d kept, verified via GET after PUT).
- REPLICAS (o2 prod parity, hard cap 10/role): querier 10 (= cap; o2 runs
  10), compactor 6 (o2 runs 6), FIXED — no HPA this round; ingester HPA
  stays 5/5; router 1. Review P0 fixed pre-merge: compactor limits.memory
  RESTORED 24Gi→48Gi (e4e89f9 "sync" had silently halved it to ==requests
  — the OOM-crashloop shape that forced fleet-reset A1). FOLLOW-UP
  (review P1, out of kustomize scope): ops-obs Application
  ignoreDifferences still exempts Deployment .spec.replicas — selfHeal is
  blind to manual scales of querier/compactor now that no HPA owns them;
  drop the Deployment entry + fix the stale "HPAs own..." comment in
  obs/argocd/application.yaml.
- PINS REMOVED (audit vs config.rs @60aa9edd): keys DELETED from engine —
  ZO_VIX_RG_TERM_BYTES, ZO_COLUMN_STORE_DEFAULT_FIELDS,
  ZO_VIX_MERGE_DOCS_PASSTHROUGH, ZO_VIX_MERGE_CONCAT_ORDER; pins now
  restating v2 defaults — ZO_VIX_BLOOM_COMPOSITE=true,
  ZO_VIX_DOCS_CHUNK_BYTES (16MiB default per M9). BLOOM_ONLY_AUTO_RATIO
  was never pinned on prod (0.5 default applies). KEPT:
  ZO_VIX_PLIST_MIN_DOCS=8192 (key alive, default 0),
  ZO_COLS_PER_RECORD_LIMIT=65536, all sizing pins. .107 ROLLBACK NOTE
  retires the v1 floors AND the .98/.104/.106 knob brakes (deleted keys =
  silent no-op flips; only brake is full PR revert).
- SMOKE (through router, root basic auth, stream "default" @18.7M
  rows/1h): SELECT * LIMIT 10 → 10 hits, 726ms engine / 1.46s wall;
  equality needle host.name='<real collector pod>' → 10 hits, 2.9s;
  match_all('error') → 10 hits, 5.9s, INDEX-SERVED (use_inverted_index:
  true, index_condition (_all:error), SimpleSelect rule); histogram 5m ×
  1h → 12 buckets / 20.58M rows, 2.67s. QUIRK (engine follow-up):
  histogram GROUP BY works with canonical zo_sql_key alias but an
  arbitrary alias (AS ts) fails planning ("expanding wildcard:
  _timestamp must appear in GROUP BY") — UI/orbit send canonical form.
- FORMAT/PIPELINE EVIDENCE: L0 .vix index-off 2741 objects; 26 merged
  outputs each a .vix+.vxi PAIR (first 11:37:58Z, s3_access_logs hr10:
  56.4MB data + 7.06MB sidecar); metadata/metrics .parquet 759.
  cache_latest_files: querier disk caches hold fresh merged pairs
  (data + sidecar, per consistent-hash owner; success logs debug-level).
- COMPACTION: 16 merges in first 40 min, wall 213ms..369.8s (p50 38.8s),
  ZERO OOM; compactor mem over 14 min (7 samples @2min, max pod GiB):
  10.9/21.9/10.7/19.7/14.2/15.1/22.2 — bulge-and-release inside 48Gi
  (peak 22.2 = 2.2x headroom; the pre-launch 24Gi limit would have been
  within 2GiB of OOM at this peak).
  Engagement: 1 merge docs_passthrough:11 + concat_order:true (tiny-file
  concat, 213ms); 15 rebuild-path heals (docs_passthrough:0 — EXPECTED:
  first-gen merges over index-off L0s must derive terms). 3 WARNs "heal
  docs passthrough failed after qualification ... Array encoding
  vortex.shared not permitted by ctx" → fail-open rebuild, output
  correct. ENGINE FOLLOW-UP: register vortex.shared in the heal
  passthrough qualification ctx — prod heals currently never get the
  #51c win.
- L0 BUILDERS: engaged on all 11 builder pods. External-sort errors
  71/40min, confined to the two historical offenders (aws_vpc_flow_logs,
  trace_list_index), retry-until-fit — both register files (122/388),
  not wedged. wal_segments pending NOT yet draining at T+35min (not-built
  4254→4787 over 3.5min; built 3680→5504 ≈ 520/min) — arrivals outpace
  builds this early; WATCH next hours before calling it steady. NOT
  touched per launch orders (report only).
- HEALTH: 22 obs pods Running 0 restarts (T+33min), 0 OOMKill events,
  0×401 router+ingesters since rollout (root auth env-provisioned on the
  fresh DB, verified pattern held).
- COSMETIC: pods self-report v0.93.0-vix-20260807.74 — GIT_VERSION =
  git-describe and the canonical tree's last git TAG is .74 (describe:
  v0.93.0-vix-20260807.74-128-g60aa9edd10); true since .75, image tags
  are the release identity. Consider tagging discipline if it ever
  matters operationally.
- DB DROP LIST: obs20260817 (BOTH envs) joins the drop list ~2026-08-25,
  alongside obs20260803 (due 2026-08-24).

## V2 DEV LAUNCH (2026-08-18)

- Image v0.93.0-vix-20260818.107 multi-arch (amd64+arm64), mimalloc default. Push gates green:
  "OK: v0.93.0-vix-20260818.107 pushed to both registries (commit 60aa9edd10c0, differs from v0.93.0-vix-20260814.106)";
  ECR verified both registries, same manifest-list digest sha256:7fa99fc5dbda...2f570.
- devops-argocd-dev-ops PR #281, owner-merged 09:22:32Z (merge 17a8f81a66). Fresh DB obs20260818 +
  fresh prefix obs-20260818/ (v2 breaking format never reads interim v1 files); lifecycle rule
  obs-20260818-retention-1d merged alongside the 0803/0817 rules (GET-verified).
- Env pin audit vs v2 config registry — REMOVED: ZO_VIX_MERGE_DOCS_PASSTHROUGH, ZO_VIX_MERGE_CONCAT_ORDER,
  ZO_VIX_RG_TERM_BYTES, ZO_COLUMN_STORE_DEFAULT_FIELDS (keys deleted in v2); ZO_VIX_BLOOM_COMPOSITE,
  ZO_VIX_BLOOM_ONLY_AUTO_RATIO (now engine defaults); ZO_VIX_DOCS_CHUNK_BYTES (v1 4MiB pin dropped for
  the v2 16MiB owner default). Everything else kept. Replicas to o2 parity: querier 3, compactor 1
  (+ maxUnavailable 3->1 per the #258 pairing note); env-rev 2026-08-18-v2-launch-107 on all four roles.
- Rollout: Argo synced on merge; all roles on .107 and Ready within ~2 min, all pods on arm64 nodes.
- Ingest: migrations created 111 tables + root user in obs20260818; file_list 0 -> 533 rows in ~2.5h;
  S3 files/: 873 .vix / 38 .vxi (L0 files sidecar-less BY DESIGN under L0-index-off; sidecars appear
  with merges; old-data passes healed late hours incl. 2026/08/10, 03:00, 07:00, 08:00). Zero 401/403.
- Merge evidence (first-generation, all inputs column-store-only L0): WARN "column-store-only file (no
  index sidecar) cannot join a dictionary merge" = the designed rebuild heal;
  "merged 22 core files ... original_size: 4211917916, compressed_size: 201378934, index_merge: false,
  docs_passthrough: 0, concat_order: false, took: 85804 ms". passthrough/concat counters present in
  every merge summary; 0 so far — no indexed+indexed merge pairs yet in dev.
- Query smoke via router (aws_vpc_flow_logs busy stream, k8s_dev_ops_logs for FTS):
  (a) SELECT * DESC LIMIT 10: 200 OK, took 209 ms, 10 fresh hits;
  (b) 1-min histogram over 1h: 532 ms, 57 buckets, 7.7M rows — querier line "IndexOptimizeExec serving
      the precomputed index result (histogram hits: 119063) over 1 files", index load 184.77 MB;
  (c) equality needle interface_id=eni-084fda69769cd92be: 226 ms, 10/10 exact — bloom pruner
      "input=10 (with_bloom=1, without_bloom=9) ... kept=9, dropped=1" (bloom-proved absence dropped);
  (d) match_all('slack') on body FTS: 1258 ms, 10 hits. Plus metadata pre-prune ("dropped 13 of 15
      files"), filter-back on index-less L0s, segments_scan live branch (23 skipped by top-n).
- cache_latest_files (M11 default-on): all 3 queriers hold pre-warmed TRACES files in memory cache
  (zo_query_memory_cache_files 18/20/26) with ZERO trace queries issued = event-path downloads (data +
  sidecar); downloader queues drained (0/0).
- "vortex.shared not permitted by ctx" (prod #428 heal-passthrough WARN): dev compactor count = 0.
- CAVEAT — OOM wave 10:17-10:54 UTC during first-hour backlog processing: compactor OOMKilled x2
  (48Gi limit; second container ran 10:37:49-10:54:15), ingesters hrlm2 + pc8v9 OOMKilled x1 each
  (8Gi; the memory circuit breaker WAS rejecting with 503 MemoryCircuitBreakerError before the cgroup
  kill). Self-recovered; 65-95 min clean since at flat memory (compactor 837Mi, ingesters ~400Mi,
  queriers 1.2-2.0Gi). Suspected: 8 concurrent first-gen rebuild merges (ZO_FILE_MERGE_THREAD_NUM=8)
  over multi-GB original-size groups stacking with L0 builder claims. Not a crashloop; no data loss
  observed (ack-on-append segment WAL + lease-fenced merges; senders retried the 503s). NEEDS a
  fleet-level concurrent-merge memory bound before prod processes an equivalent backlog wave.
## v2 M12 — heal-cache correctness, coverage-scan perf, vortex.shared fix, L0/rebuild stability (2026-08-18)

- ITEM 1 CORRECTNESS (1d86862815) — result-cache heal invalidation. Root cause:
  the per-file result cache + straddling bitmap memo keyed
  {condition}_{rule}_{clamp}_{data key} with no index-version component, and
  M3's eviction sweep only touched byte caches — an answer-changing
  sidecar-only heal served pre-heal entries indefinitely. Fix, both prongs:
  (a) key layout now `{file key}|{index_size}|{hash}_{rule}_{clamp}` —
  meta.index_size (the sidecar's exact object size, the SAME freshness witness
  M3's byte-cache eviction uses) versions the key; one function feeds both
  call sites, preserving the bitmap-memo/main-key identity; (b)
  VixResultCache::remove_file_entries (one prefix-extract+set-lookup pass per
  broadcast, exact byte accounting) wired into evict_stale_sidecar_caches —
  immediacy + budget hygiene; the size component alone already makes stale
  entries unreachable even when a node missed the broadcast. PINS: key
  inequality across an index_size change (both rule arms) + stable-key reuse;
  purge accounting/isolation; core heal e2e extending
  sidecar_only_heal_restores_capabilities_without_touching_docs (post-heal
  key differs, query misses, broadcast purge evicts — and heals MUST change
  the sidecar size, the shared M3/M12 assumption); event.rs M11 heal test
  extended (broadcast purges the result cache).

- ITEM 2 PERF (1a5e424c9c + b0c7c38478) — composite-bloom coverage scan + SBBF.
  DOUBLE-HASHING VERDICT (corrected TWICE against ground truth, probe
  m12_probe_file_facts): (i) M10's "coverage scan hashes the merge-AUTO-demoted
  `duration`" was an ARTIFACT — numeric fields never pass the writer's
  string-family re-check, so `duration` never entered bloom_only and was never
  scanned; the merge-site AUTO resolver still LOGGED the demotion on every
  merge (fixed: candidates now filtered to string-family stored types — the
  log tells the truth). The 11.4s scan was the FOUR birth-demoted ID columns
  (~64M values). (ii) Real double-hashing exists only in STICKY-MIXED merges
  (an input with full term capability for an output-bloom-only field is
  absorbed by the k-way walk via composite_pairs AND was re-scanned) —
  eliminated: bloom_scan_fields_for_input restricts each input's scan to
  fields its dictionary cannot cover (bloom-marked / no capability / PARTIAL);
  coverage completeness of dict-walk ∪ restricted-scan pinned by the existing
  mixed/sticky coverage tests (legacy-input values probe true post-elimination)
  + m12_bloom_scan_fields_skip_dictionary_covered_inputs. Decode-fallback
  inputs still push-time-hash dict-covered fields (bounded to qualification
  failures; open). PARALLELISM: per-input coverage scans run concurrently on
  the merge thread budget (min(ZO_VIX_MERGE_THREAD_NUM, inputs), scoped
  threads, no second pool) via detached BloomOnlyHasher workers; sets fold by
  union — schedule-independent by construction (per-input isolation +
  commutative dedupe; the writer/hasher share ONE value-policy
  implementation). SBBF: Sbbf::insert_hashes partitions the block space into
  disjoint contiguous per-worker ranges — byte-identical for any thread count
  (pinned: m12_insert_hashes_parallel_matches_sequential, blocks 1..65536 x
  threads 1..1024) — wired as build_threaded under opts.encode_threads.
  BENCH (8x2M pure-default corpus regenerated: 0000.vix = 303,912,992 B ==
  M10 byte-for-byte; medians of 3 ALTERNATED, control first, idle box;
  control = c2d829d66e pre-item2 binary; logs vixbench-m5/logs-m12/):
    merge wall    control 18.49s (18.10/18.49/19.41) -> new 13.99s (12.25/13.99/14.09)  -24.3%
    index-off     control 6.41s / new 6.34s
    index share   control 12.08s -> new 7.65s   -36.7%
    VmHWM         control 5,973,488 kB -> new 5,980,948 kB (medians)  1.00x (H3 <=1.2x PASS)
    phases        coverage scan 11.4s serial -> 6.04s wall on 8 workers (1.9x eff —
                  memory-bandwidth-bound column decode); docs staging 16.33 -> 10.94s;
                  SBBF build 0.69 -> 0.51s; k-way ~1.05s and encode/finish unchanged
    equivalence   compare new-vs-control: digest cb3c1efc20be5a93 (= the M10 pinned
                  digest; rows 16,000,000 / terms 6,286,992); compare new-parallel vs
                  new-seqscan (ZO_VIX_MERGE_THREAD_NUM=1, wall 29.37s): equal; .vxi
                  sha256 identical across new runs AND identical to control (this
                  corpus has no sticky-mixed inputs, so the elimination changed no
                  bytes here — the win is pure parallelism)
  Honest residual: the scan is now decode-bandwidth-bound (1.9x on 8 workers);
  the next lever is hashing off the encoded chunks (per M10's note), not more
  threads.

- ITEM 3 BUG (199dd427fe) — prod "vortex.shared not permitted by ctx" heal
  passthrough failures. ROOT CAUSE (proven on real prod bytes: READ-ONLY fetch
  of files/default/logs/default/2026/08/17/16/74954578845793443848cee.vix —
  the ignored probe m12_probe_prod_shared_wrapper reproduces the EXACT error
  on its raw scan: chunks=1 with_shared=1 with_dict=1 serialize_errors=1):
  vortex-layout 0.79's DICT LAYOUT reader wraps the values child of every
  yielded chunk in a runtime SharedArray — a lazy-execution cache whose vtable
  has NO serialize impl and no registry entry ("not permitted by ctx" is the
  writer-side intern failing). Dict layouts are produced by the FIRST-ENCODE
  strategy (docs_strategy -> vortex WriteStrategyBuilder's DictStrategy probe)
  — i.e. L0 move builds AND rebuild outputs; M6's ClusteredDocsStrategy never
  dict-probes and cannot produce them. Multi-chunk dict fields escaped by
  ACCIDENT (their shared values buffers trip the M6 slice guard's overlap
  sweep into canonicalizing); a SINGLE-chunk dict field has no adjacent chunk,
  so the wrapper reached serialize and the whole heal fell open to a
  decode+re-encode rebuild — prod's WARNs sat inside 99s/159s s3_access_logs
  rebuilds that HAD qualified for the copy (the lost #51c win). FIX:
  unwrap_shared in scan_blob_encoded_chunks replaces Shared nodes with their
  SOURCE array (stored encoding; dict rebuilt via DictArray::new_unchecked
  with all_values_referenced copied — sound because Shared::validate pins the
  source's dtype+len); a Shared under an unknown parent falls open to the
  existing canonicalize path, never an error. PINS: unwrap unit (dict shape /
  bare Shared / unknown-parent None / flag carry); dict-layout single-chunk
  roundtrip e2e (corpus tuned until the BtrBlocks probe PICKS dict — 1024
  distinct random 32-char strings x 8192 rows; verbatim copy keeps the dict
  encoding, rows read back identical); the prod-bytes probe above (fixed scan:
  all chunks serialize, dict preserved).

- ITEM 4 STABILITY (c2d829d66e) — L0 external-sort fix + memory admission.
  MECHANISM (prod, aws_vpc_flow_logs 160-segment super-batch, hour
  1787054400000000): the L0 core build ran `SELECT * ORDER BY _timestamp DESC`
  through DataFusion at target_partitions=ZO_DATAFUSION_MIN_PARTITION_NUM(2):
  RepartitionExec feeding TWO ExternalSorters under SortPreservingMergeExec in
  one 6.0GB greedy pool — RepartitionExec buffered 3.0GB it cannot spill,
  ExternalSorter[1] held 3.0GB, ExternalSorter[0]'s FIRST allocation (122.8MB,
  0 bytes reserved => nothing of its own to spill) failed with 13.8MB left.
  Pool starvation from the plan shape, not capacity.
  FIX 1+3 (the sortedness assessment held, so fix 3 supersedes fix 1 for the
  core arm): the builder already sorts its super-batch — now DESCENDING (the
  stored v2 row order; split_by_hour handles either direction; buckets
  re-sorted ascending so file/plan-key order is unchanged) — and each hourly
  bucket feeds write_core_file_from_sorted_batch: the SAME extracted builder
  loop (spawn_core_file_builder), fed zero-copy 8192-row slices. NO plan, NO
  repartition, NO sort, NO pool interaction; the DESC contract is VERIFIED
  O(n) and refused loudly. Non-core L0 arms (metadata/filelist/metrics under
  #40-off) keep merge_parquet_files — thin streams; same plan shape noted
  there as a theoretical follow-up only.
  FIX 2 memory backoff: a claim failing with ResourcesExhausted (chain-matched
  against DataFusion's canonical display) retries HALVED — dropped tail ids
  released for other builders (fenced: heartbeat/release are
  builder_node+status guarded, stale guards no-op) — down to a 1-segment floor
  that always gets a real attempt; non-memory failures keep the release-all
  path. Convergent in log2 attempts; deterministic L0 keys keep every retry
  idempotent, and halving keeps the kept-half a prefix of the failed plan.
  FIX 4 encode-memory accounting VERDICT (documented, no new machinery
  needed): the writer's own buffers are already bounded — docs encode samples
  <= 256MB (DOCS_ENCODE_SAMPLE_BYTES) then STREAM to the container (spooled >=
  ZO_VIX_MOVE_SPOOL_MIN_BYTES), term map spill-budgeted; the resident unpooled
  set is the decoded super-batch (<= ZO_SEGMENT_BUILD_SUPERBATCH_MB=512,
  measured in DECODED bytes) + per-build derived state (~<= 128MB input +
  ~same-order _source synthesis) x build concurrency. The dev
  "503-but-cgroup-killed" ingesters: the breaker meters QUERY allocations
  only; L0-build memory was invisible to it AND spiked ~3x through the DF
  sort — the spike is now structurally gone and the rest is budget-bounded;
  ZO_SEGMENT_BUILD_CONCURRENCY (item 5) is the operator lever.
  REBUILD ADMISSION (the compactor half of the OOM wave): process-wide
  RebuildGate on rebuild_over_sources — direct rebuilds AND fast-path
  fallbacks; fast-path (passthrough+k-way) merges and sidecar-only heals
  (windowed, subsecond on prod) stay unthrottled. ZO_VIX_REBUILD_CONCURRENCY,
  0 = auto max(1, ZO_FILE_MERGE_THREAD_NUM/2), always >= 1; blocking acquire,
  waits > 50ms logged at info (count-style, no lists). DESIGN RATIONALE for
  the cap over a byte estimate: each rebuild's working set is individually
  bounded (window caps + term spill + spool) — the incident dimension was the
  COUNT of footprints (dev: 8 concurrent first-gen rebuilds at 48Gi); a
  concurrency cap bounds that count exactly, while original_size x factor
  estimation carries the measured 5-10x per-stream arrow-expansion error and
  still needs a floor. PINS: m12_sorted_batch_build_matches_tables_build
  (drop-in equivalence vs the DataFusion build over a 20k-row multi-slice
  corpus at shrunk caps — completes bounded with zero pool involvement — plus
  DESC-contract refusal); split_by_hour descending; is_resources_exhausted
  (REAL DataFusionError through an anyhow context chain, the pasted prod
  message); halve_for_retry (160->80->...->1, released tails, floor).

- ITEM 5 (c2d829d66e) — ZO_SEGMENT_BUILD_CONCURRENCY (default 3 = the old
  hardcoded constant; floor 1 clamped at config load): per-pod small-build
  parallelism is an operator lever (prod: ~370 seg/min arrivals vs ~195/min
  fleet builds at 3-per-pod; .108 plans ~8 on builder-compactors; ingesters
  stay low). Pin: default+override+floor config test (one test — env is
  process-global).

- GATES (logs /tmp/claude-1000/m12/): cargo build --workspace EXIT=0; units
  config 1973 (EXIT=0; first run hit a PRE-EXISTING parallel-test race on the
  config_path_manager global last-hash — root-caused, serialized with a test
  lock in 1711ccfac4, 3x rerun green — NOT one of the three known flake
  families, now removed as a future one), vortex_index 224 (+ ignored
  probes), search 999, openobserve-core 1916, openobserve-api 290,
  openobserve-jobs 28; integration BOTH modes redirected `; echo EXIT=$?`:
  segmode ok 69.70s EXIT=0 + default ok 43.56s EXIT=0 (initial tree); rerun
  on the FINAL tree after the planner truth-fix: core units EXIT=0, segmode
  ok 70.03s EXIT=0, default first run tripped KNOWN FLAKE FAMILY (3)
  (trigger.next_run_at assert, integration_test.rs ~3356 — the alert-
  scheduler family), rerun ok 43.61s EXIT=0 (gate-*-final*.log).

- OPEN/DEFERRED: decode-fallback inputs still push-time-hash dict-covered
  demoted fields (bounded, non-default shape); merge_parquet_files keeps the
  2-partition sort plan (thin streams, never observed failing — the pool
  starvation shape is theoretically reachable there); sidecar-only heals stay
  outside the rebuild gate (windowed + subsecond, revisit if prod shows
  otherwise); coverage scan is decode-bandwidth-bound at 8 workers — next
  lever is hashing off encoded chunks, not thread count. Prod repro files
  under /tmp/claude-1000/m12/ (repro.vix/.vxi) are transient — delete after
  the .108 rollout confirms the fix in prod logs.

## .108 ROLLOUT (2026-08-18)

- TAG v0.93.0-vix-20260818.108 (engine af0b4d53b0, M12; format-compatible
  with .107 — no cut, no DB change). Builds x86_64 10m16s / aarch64 11m24s;
  provenance gates green: "pushed to both registries (commit af0b4d53b056,
  differs from v0.93.0-vix-20260818.107)"; amd64 sha256:9eb68b1c22e6...,
  arm64 sha256:8b1d0ebead44....
- DEV: dev-ops PR #282, merged immediately (standing auth); all 8 obs pods
  Ready on .108 in ~25 min. 25-min builder-log window: 0 "Not enough memory
  to continue external sort", 0 "vortex.shared not permitted by ctx",
  0 panics; smoke query via router hits=5 took=299ms. Two isolated OOM
  restarts during the roll window (pre-existing dev pattern — compactor had
  6 restarts on .107 pre-roll; settled post-roll, compactor steady
  11.9Gi/48Gi).
- PROD: prod-ops PR #430, merged 15:21:11Z --admin. Roll clean: all 22 obs
  pods on .108, ZERO restarts on any .108 pod during the roll, zero
  crashloops/pull errors, zero 401s. Pre-roll state (measured): pending
  wal_segments(status=0) 33,165 @14:23:34Z -> 34,357 @14:29:03Z = +217/min
  and accelerating (was ~16.7k / +175/min ~2h earlier); sort errors 7/15min
  (5 ingester + 2 compactor, WITH the 128MB interim pin); vortex.shared
  14/15min (compactors only).
- POST-ROLL (main-session monitoring): vix L0 sort-error class ZERO — the
  only remaining "Not enough memory" hits are default/metadata/
  trace_list_index, the parquet-path case M12 explicitly deferred (backoff
  working). vortex.shared ZERO — heal passthrough engaging in heal
  summaries; the transient prod repro pair /tmp/claude-1000/m12/repro.vix
  + .vxi DELETED per the M12 note above.
- SUPERBATCH PIN RETIRED: ZO_L0_SUPERBATCH_MB interim "128" removed from
  the prod configmap (engine default 512 applies) — M12's single-partition
  external sort holds at 512; no vix sort failures post-roll.
- CAPACITY: compactor container env ZO_SEGMENT_BUILD_CONCURRENCY=8 shipped
  in #430 (6x8 + 5x3 = 63 slots vs the sized-for ~370/min arrivals). But
  arrivals grew to ~587/min during the degradation window, so pending
  STABILIZED ~74.5k (flat, not draining). Follow-up prod-ops PR #431
  (main session) shipped builder concurrency 12 (compactor) / 5 (ingester)
  + karpenter do-not-disrupt on builder pods.
- FINDING (engine work queued): the claim scan orders wal_segments by
  created_at DESC — newest-first claiming starves the oldest cohort
  indefinitely while a standing backlog exists (pending never drains
  oldest-first). Aging-lane fix queued in the engine.

## v2 M13 — aging-lane claims + backlog-mode sealing, metadata single-partition sort, dictionary-first top-k/distinct dispatch, §12.4 resolved (2026-08-19)

- ITEM 1 (8c0ec1cc9f) — DATA-LOSS INSURANCE: aging-lane segment claiming.
  The claim scan orders `created_at DESC` (right for freshness in steady
  state) — under a STANDING backlog at balanced capacity it starves the
  oldest cohort until the 1-day S3 lifecycle deletes their raw objects
  (prod 2026-08-18/19: 74.5k pending, oldest stuck at the 11:33Z launch
  cohort 15+ hours; #431's capacity surplus was the operational mitigation,
  the ordering was the structural hole). AGING LANE on the compactor
  live-lane precedent (ZO_COMPACT_LIVE_JOB_NUM — reserved slots for an age
  band): once the oldest claimable segment exceeds
  ZO_SEGMENT_BUILD_AGE_LANE_SECS (default 21600 = 6h), a
  ZO_SEGMENT_BUILD_AGE_LANE_RATIO fraction of claim passes (default 0.25 =
  every 4th; fixed-point per-mille accumulator, ticks only while engaged,
  exact long-run rate for any ratio) claims OLDEST-first — the WHOLE pass,
  super-batch extensions included, so an aging pass drains a CONTIGUOUS
  aged band (adjacent old hours → fewer (stream,hour) output slices).
  DESIGN RATIONALE for a lane over a flipped/blended global order:
  newest-first is load-bearing for query freshness (recent windows recover
  first under backlog — the compaction fast_mode lesson), so the steady
  state must stay byte-identical; a reserved fraction bounds worst-case
  drain time of the aged band (cohort/batch × 1/ratio passes) while giving
  up only 1/4 of peak drain throughput to it, engages and disengages by
  observed age with no state, and needed no scheduler — the same reasoning
  that shipped the compactor live lane. Both lanes share the exact
  candidate predicate, the ALL-OR-NOTHING CTE floor and SKIP LOCKED
  semantics; ClaimOrder threads through claim_pending_with_floor;
  newest-first SQL text stays byte-identical. Ratio clamped [0,1] at load.
  PINS: infra oldest-lane ordering/floor/lease test; config
  default+override+clamp; engagement threshold + fire cadence; STARVATION
  REGRESSION — aged 8-segment cohort + balanced arrivals (4 in / 4 claimed
  per round): pure newest-first never touches the cohort in 16 rounds
  (asserted), the lane at ratio 0.25 / batch 4 drains it exactly at round
  8 (2nd fire), never before round 4.

- ITEM 1b (8c0ec1cc9f) — backlog-mode super-batch sealing (owner
  follow-up on the ITEM 1 review). MEASURED prod 2026-08-18 16:00-16:45Z
  (240 "batch done" lines, 15 builder pods): per-pod med cycle gap 90-182s
  against med build took 53-167s — med claim-side overhead 0.2s but p90
  ~60s and ingester MEDIANS of 21-40s; batches sealed at 40-80 segments
  (sub-budget). Root cause split in two: (a) the wait-shaped overheads in
  THIS window were M12 halving-retry failed attempts (memory failures on
  metadata builds — fixed by ITEM 2; "failed on memory; retrying with"
  warns every few seconds in the same window); (b) STRUCTURALLY the #54
  accumulation was clock-and-wait paced — the age clock bounded the whole
  loop and any empty claim slept 5s — capable of pacing the pipeline
  whenever emptiness races occur, which the interim ops pin (prod-ops
  #432, ZO_L0_SUPERBATCH_MAX_SECS 120→15) worked around by shrinking the
  clock. FIX: while claims return rows the accumulation is bounded by
  WORK — claim to the MB target, seal immediately, no clock check; an
  empty claim consults the cheap #50 has_claimable probe — claimable work
  still present = SKIP-LOCKED race loss, retried immediately (bounded
  EMPTY_CLAIM_RACE_RETRIES=3 against pathological spins) — and only a
  genuinely empty table takes the pre-M13 trickle pacing (one 5s tick per
  gap, sealed by two empty ticks or the age clock, which now caps
  accumulated WAITING and stays the crash-replay bound).
  accumulate_super_batch extracted for the pins. #432's 15s pin can revert
  once this ships (ops follow-up). PINS: empty-claim policy matrix (race
  retries bounded + no tick, trickle wait/seal, clock seals waiting only,
  claimable-retry outranks the clock); deep-backlog accumulation (20-seg
  pool, budget 16 segs, batch 4) seals exactly at the byte budget in one
  pass — 3 extension claims, all heartbeat-guarded, ZERO sleeps (wall <
  5s asserted).

- ITEM 1c (9e7c73422a) — ZO_SEGMENT_FETCH_DECODE_CONCURRENCY (owner
  follow-up; the drain fix's third leg: the lane fixes WHICH cohort, 1b
  fixes HOW FAST claims chain, 1c fixes the stage after). fetch_and_decode
  pulled a claimed batch's objects through a hardcoded 2-wide `buffered` —
  with the claim waits gone, fetching+decoding a ~512MB super-batch's ~130
  objects two at a time was THE cycle-rate limiter (prod 2026-08-19:
  100-160s cycles dominated by this stage even under the 15s clock pin).
  Env-tunable now: default 2 = old behavior byte-for-byte, floor 1 clamped
  at load; memory scales with in-flight decoded objects (~flush-size arrow
  each) — compactors pin 8, ingesters stay low. PIN: config
  default+override+floor test.

- ITEM 2 (56a2c92df6) — the last DataFusion sort starvation: metadata/
  parquet-path L0 builds. Prod post-.108: the only remaining "Not enough
  memory to continue external sort" class was default/metadata/
  trace_list_index through merge_parquet_files' 2-partition plan
  (datafusion_min_partition_num=2) — the M12-deferred case, NOT thin at
  prod volume (the 16:00Z window shows ingesters halving 128→64→40 on it,
  30-90s of failed attempts per cycle). FIX (the M12 fix-1 rationale
  verbatim): DataFusionContextBuilder::single_partition — plan at exactly
  ONE partition, bypassing the min-partition floor (applied after
  create_session_config, deliberately the one caller allowed under it);
  merge_parquet_files gains single_partition_sort and ONLY the segment
  builder passes true — the compactor merge path and the ingester WAL
  move job are untouched (different context, never observed failing). The
  M12 halving backoff stays the backstop; its tests unchanged and green.
  PINS: m13_single_partition_merge_plan_has_no_repartition (target
  partitions 1, exactly one SortExec, zero RepartitionExec; default
  context keeps the floor — compactor plan untouched);
  m13_metadata_shaped_build_spills_at_floor_pool (ignored, run --release
  standalone): 4,194,304 rows / 512MB arrow of trace_list_index-shaped
  data at the floored 256MB greedy pool COMPLETES in 2.85s by SPILLING,
  row count preserved (log: "built 4194304 rows / 512 MB input in
  2.854303381s at a 256 MB pool"), EXIT=0.

- ITEM 3 (4652db67a4) — top-k/distinct dispatch re-decided, MEASURED
  FIRST. The unfiltered SimpleTopN/SimpleDistinct arms preferred the docs
  column on the stale whole-FST-walk rationale; M2 all-columns made it
  bind on every v2 file — the #29 dictionary fast path was dormant. New
  #[ignore] bench src/search/src/vix/dispatch_bench.rs (the post-#29
  classes query_bench.rs deliberately cannot carry; its cross-tree header
  respected, file untouched) against a regenerated 16M-row corpus —
  8x2M merge_bench gen + merge at ZO_VIX_PLIST_MIN_DOCS=8192 (prod
  compactor value) and ZO_VIX_BLOOM_ONLY_AUTO_RATIO=0 so the dictionary
  is exercisable at TRUE high cardinality (on prod defaults ≥65k-distinct
  ratio>0.5 fields are bloom-only demoted = dictionary-refused by
  construction). Medians of 3 binary runs × 3 in-process repeats,
  logs /tmp/claude-1000/m13/bench-plist-run{1,2,3}.log; parity asserted
  EQUAL in-bench per class:

    class                                   dictionary   docs column   speedup
    unfiltered top-k  service_name (30d)      1.26 ms      47.85 ms      38x
    unfiltered top-k  trace_id (16Md)        35.77 ms    9232.83 ms     258x
    unfiltered distinct service_name          1.53 ms      26.65 ms      17x
    unfiltered distinct trace_id             12.59 ms   10446.74 ms     830x
    distinct-count probe (ordinal ranges)     0.002-0.003 ms (both fields)
    filtered top-k  service_name             12.67 ms      19.04 ms     1.5x
    filtered top-k  trace_id              0.69 ms REFUSED (#29 cap) → docs 1278.11 ms
    filtered distinct service_name           12.87 ms      18.69 ms     1.5x
    filtered distinct trace_id            0.72 ms REFUSED (#29 cap) → docs 1270.44 ms
    simple_select wave (full / filtered)      0.68 / 0.67 ms
    ranked-plist histogram vs bitmap          0.92 vs 0.79 ms (parity EQUAL)

  DECISION: DICTIONARY FIRST unconditionally where it can prove exact
  counts — no crossover exists (the doc_count ordinal-range scan + bounded
  heap beats the O(rows) docs decode + per-distinct map even at
  distinct == rows), so no ratio threshold; refusals (fts/partial/
  bloom-only-demoted in µs; mixed-typed/empty-string after their range
  scan) fall through to the docs column, then `_source`. The stale comment
  replaced with the measured truth. Dict-refusal logs demoted info→debug
  (per-file per query on demoted fields now — hot-path spam discipline).
  New VixReader::field_distinct_string_terms (4 resident-index probes)
  powers the bench's ratio report. NOT taken: filtered-arm flip (1.5x on
  low-card is real but modest and out of the audited scope — deferred
  below); ranked-plist histogram shows no in-memory win on dense terms
  (its value stays window-straddling/ranged shapes — parity EQUAL).

- ITEM 4 (b2b592c7e0) — DESIGN-V2: §12.4 replaced with the audit
  resolution (five vix-arch commits = squash re-publications of this
  branch's granular history; superseded by the M12 cache-key correctness
  finding + this milestone's dispatch re-decision); no stale M4 deferred
  line existed to delete (checked); §7 aging-lane note (raw objects share
  the 1-day lifecycle → claim ordering is data-loss-critical); §8
  backlog-mode sealing note.

- GATES (logs /tmp/claude-1000/m13/): cargo build --workspace EXIT=0
  (pre-1c tree; 1c then compiled through the jobs/core/root rebuilds).
  Units: config 1974+3 EXIT=0, infra 1082 EXIT=0, vortex_index 224 EXIT=0
  (+ ignored probes), search 1000 EXIT=0, openobserve-core 1916 EXIT=0
  (built on the settled 1c tree). openobserve-jobs first chain run
  EXIT=101 was a MID-CHAIN EDIT RACE (1c's segments.rs landed one write
  before its config.rs field — E0609 on the fresh field, not a test
  failure); settled rerun 33 passed EXIT=0. Config settled rerun (with the
  1c test) 1975 passed EXIT=0. Integration BOTH modes redirected
  `; echo EXIT=$?`: pre-1c segmode ok 77.08s EXIT=0 + default ok 46.12s
  EXIT=0; SETTLED-TREE reruns segmode ok 69.81s EXIT=0 + default ok 41.88s
  EXIT=0. Zero failures on the settled tree — none of the three known
  flake families tripped, no reruns needed. Manual pins run --release
  standalone: spill pin EXIT=0; dispatch bench runs 1-3 EXIT=0 each.

- OPEN/DEFERRED: filtered top-k/distinct dict-first (measured 1.5x on
  low-card; flip is low-risk but out of the audited dispatch scope —
  candidate for a later milestone with its own parity pins); ops follow-up
  for the main session: revert prod-ops #432's ZO_L0_SUPERBATCH_MAX_SECS=15
  interim pin once M13 ships (1b makes it unnecessary); prod configmap may
  set the aging-lane envs explicitly if 6h/0.25 defaults need tuning under
  observed drain rates. Bench corpus kept at
  /home/zhichen/work/vixbench-m5/v2/{corpus-m13,out-m13} (7.2G) for
  re-measurement; delete after the next fleet image proves the dispatch in
  prod logs (search->vix top_n serve lines).

## v2 M14-M16 — final v2 query-path package: cold-open prefetch, demoted-needle completion, stats-answered aggregations (2026-08-19)

- M14 (912a38216f) — query-shaped cold-open prefetch, `ZO_VIX_QUERY_PREFETCH`
  default ON. Ranged vix_search batch-opens a file group's COLD files (no
  memoized reader) in ONE bounded-concurrency wave before per-file eval fans
  out: each cold file's eager tail fetches (data puffin footer + sidecar
  footer/dict directory — the ZO_VIX_EAGER_TAIL_BYTES window) overlap across
  FILES instead of serializing inside each eval task's open; opened readers
  memoize, so eval finds them Shared with zero open IO. Wave fetches take the
  global ZO_VIX_FETCH_CONCURRENCY permits and tick the query's fetch stats
  (bytes count toward the ZO_VIX_EVAL_BAIL_BYTES budget — the wave truncates
  at the threshold and can never trip the bail alone; the projection adds
  prefetched bytes FLAT instead of multiplying the wave by files_total/sample,
  which would have inflated quadratically). Planning skips warm readers and
  result-cache-answered files (without that, a hot dashboard whose files hit
  the result cache would re-fetch tails EVERY refresh forever — the result
  path returns before the reader memoizes). Postings deliberately not
  prefetched (need dict resolution). Best-effort: per-file wave errors drop,
  eval re-opens/retries as before; false = pre-M14 behavior. PINS: wave
  mechanics against the real local object store — exactly 2 fetches per cold
  file (one tail per object, nothing else), memoization, idempotent second
  wave (0 fetches), byte-budget truncation; differential vix_search
  prefetch-on == off. Reader/result caches gained metric-free contains().

- M15a (6cc260eed6) — `.bf` composite fallback for demoted CONFIGURED fields
  (the M7 deferred item 1). A field in stream-settings bloom_filter_fields
  folds per-field; #52 demotion drops the per-field section, so every probe
  was no-info and the configured demoted field lost ALL .bf pruning. Now:
  per-field predicates carry composite_fallback (composite enabled + name
  fits the key form); per GROUP, when the per-field section does not cover
  every file (footer-local column_index check — zero extra IO when it does),
  the plan adds composite value rows (tagged #48 keys) + guard rows; per
  FILE the composite verdict applies ONLY where the per-field column is
  absent, gated on all guards hitting (uncovered field = no info = keep).
  Guard misses no longer suppress per-field rows (guards gate composite rows
  only — a legacy per-field file without a composite keeps pruning). PIN
  (e2e over real .bf objects): keep/drop PARITY demoted-vs-per-field control
  on hit and miss, mixed-group per-file verdict priority, guard fail-open,
  dark flag unchanged.

- M15b (62d869eb4b) — fast filter-back scan (M7 deferred item 2). The
  demoted-needle equality scan decoded + compared one string per row with no
  thread scaling (M7: 635ms/16M, 4T 632ms). Now a pushed string EQUALITY
  bound (min==max Str — the filter-back shape) runs a dictionary-aware
  pre-pass over the zone-pruned ranges: a DICT-encoded chunk resolves the
  needle against its distinct-values array ONCE and scans the u64 code array
  (no per-row string materialization); non-dict chunks (FSST/plain — the
  random-ID reality) keep canonical decode+compare per chunk; the pass is
  CHUNK-PARALLEL across ZO_VIX_SCAN_DECODE_THREADS (contiguous chunk-aligned
  range groups per worker — the knob previously had NO effect on this
  shape), then only the matching rows point-read the projection (i.e.
  `_source` decodes for matches only — the structural win the bench's
  single-column shape does not even show). Broad matches (>2% rows) fall
  back to the plain streaming scan; ts/limit/other bounds compose; the
  engine still re-applies the predicate. MEASURED once (out-m13/merged.vix,
  16M rows, release, /tmp/claude-1000/m14/m15b-bench*.log): trace_id OLD
  624.2ms/0T + 674.4ms/4T (no scaling) → M15 629.4ms/0T, 205.8ms/4T,
  167.2ms/16T; service_pod_name 524.9/578.0 → 536.4/183.6/139.2ms. The
  single-thread number is flat because the 16M-random-ID corpus stores FSST
  (vortex only dict-encodes what its sampler picks) — the dict arm's
  constant-time resolve is unit-pinned on dict-encoded columns
  (m15_eq_scan_dict_aware_parity: pre-pass ids == per-row oracle on dict,
  high-entropy and null-bearing columns across 0/1/4/7 threads, dict-arm
  coverage asserted on the stored encoding, empty-string needle, broad-match
  fallback parity, ts-window + limit composition).

- M16 (5275b7fcb9) — stats-answered aggregations (DESIGN-V2 §4). New
  LOCAL-only optimizer modes (never on the wire; the proto oneof still
  carries TopN/Distinct only): SimpleCountField from `count(field)` (bare
  eligible column), SimpleMinMax from `min|max(field)` (bare NUMERIC column;
  strings prefix-bounded, never eligible). Arms in vix collect:
  - count(field): fully-covered condition-all files answer from the
    file-level presence count (`columns` property) outright; straddling
    files fold per-chunk `present` for window-covered chunks and decode
    boundaries; conditioned evals count validity over the matched bitmap.
    Absent column on a columns_complete file = EXACT 0 contribution (never
    a scan); without the marker = scan (`_source` may hide values).
  - min/max(field): per-chunk exact numeric min/max folds for covered
    chunks (stats-tag/family gated, NaN excluded like the stats builder);
    boundary/stats-less chunks decode; min/max(_timestamp) folds the ZONE
    table (its stats). Cross-family folds compare exactly (i128 vs f64, no
    lossy rounding); the exec adapter emits one typed partial row and
    refuses lossy conversions loudly; no matches = no partial rows.
  - chunk-decidable equality (§4): a count-shaped aggregate (count(*),
    histogram, count(field), min/max) whose WHOLE condition is one numeric
    equality/IN the index cannot serve for the file (partial fields today;
    demoted numerics cannot exist — demotion is string-family only) is
    decided per chunk: present==0 or all probes outside [min,max] = none,
    present==rows && min==max==probe = all, inconclusive chunks decode the
    ONE column — and has_skipped clears: the file is ANSWERED instead of
    degrading to the scan branch. Strict kind<->family gate (no cross-family
    coercion), unparseable literals refuse, no zone table = stand down to
    the pre-M16 AllConditionsSkipped fallback.
  - count(*)/histogram(count) time-only (bullet 1) AUDIT RESULT: already
    zone-served since M4/#33 — condition-all evals cost an all-set bitmap
    (zero IO) AND timestamp_range_zoned (covered chunks set without decode,
    boundary rows point-read), and simple_histogram's zone fold decodes
    boundary chunks only. Extended nothing; pinned via the parity battery's
    zone-stripped full-decode leg.
  - ROUTING NOTE (deferred): index-off files (#40 metrics streams, #42 L0
    index_size==0) never reach vix eval — both routing gates require a
    sidecar — so the §4 arm serves indexed files with unservable conjuncts
    only; routing index-less files into the stats arms is a separate
    (structural) follow-up if metrics-shape counts ever matter.
  PINS: collect differentials — stats-answered == full-decode for EVERY arm
  on dense / sparse-below-density-threshold / all-null / string columns,
  boundary-straddling windows, conditioned bitmaps, files with and without
  stats (M1-era); the cached/ranged/zone-stripped evaluate parity battery
  extended with the new modes across every condition and range; partial-
  field numeric-eq e2e (exact answer, has_skipped=false, clamp composition,
  zone-less fallback); columns-complete zero shortcut; detector units +
  follower proto-roundtrip extraction + exec adapter. VixReader gained a
  decoded-once column_chunk_stats() accessor (memory_size accounts it).

- GATES (logs /tmp/claude-1000/m14/): cargo build --workspace -j8 EXIT=0
  (build-ws.log). Units EXIT=0: config 1975, search 1009, vortex_index 225,
  openobserve-core 1916. Integration BOTH modes redirected `; echo EXIT=$?`:
  segmode ok 70.39s EXIT=0, default ok 47.26s EXIT=0 — FIRST run, zero
  failures, no flake families tripped, no reruns needed. M15b bench run
  --release standalone EXIT=0 (m15b-bench.log, m15b-bench-podname.log).

- DESIGN-V2 updated: §4 stats-answered arms, §5 demoted-pruning completion
  (M15a fallback + M15b dict-aware parallel filter-back with the measured
  numbers), §7 M14 prefetch wave.
## .109 ROLLOUT (2026-08-19) — M13 live: the backlog drain flipped

- TAG v0.93.0-vix-20260819.109 (engine 0496b0f10f, M13 exactly; no format
  change, v2 floor stays .107). Built in an isolated worktree
  (/home/zhichen/work/obs-rel-109 @ 0496b0f10f, fleet-pin ancestor check
  60aa9edd10 OK) — main tree untouched (other agent mid-work). Builds
  x86_64 11m05s / aarch64 13m37s, mimalloc verified in both. Push gates
  green: "OK: v0.93.0-vix-20260819.109 pushed to both registries (commit
  0496b0f10f59, differs from v0.93.0-vix-20260818.108)"; amd64
  sha256:8123e9c5b7b4..., arm64 sha256:1ac5783717694....
- DEV: dev-ops PR #283, merged 04:51:03Z; all 8 non-nats pods on .109 and
  Ready by 04:52:47Z (<2 min), zero crashloops/restarts. Error sweep
  clean (only roll-moment NATS teardown lines); smoke query via router
  hits=5 took=289ms. Pre-roll dev pods had 17-31 restarts in 13h on .108;
  fresh .109 pods clean through the verify window.
- PROD: prod-ops PR #433, merged 04:54:37Z --admin (single commit: newTag
  .109 + REMOVE ZO_L0_SUPERBATCH_MAX_SECS "15" interim pin (backlog-mode
  sealing supersedes it) + compactor ZO_SEGMENT_FETCH_DECODE_CONCURRENCY
  "8" + env-rev all roles + .109 rollback note). All 22 non-nats pods on
  .109 and Ready by 05:06:37Z (~12 min).
- SORT ERRORS ZERO: "Not enough memory to continue external sort" 0 across
  all 11 builder pods (10-min window post-roll) INCLUDING trace_list_index
  — pre-roll baseline was 21/15min, 100% trace_list_index (the M13
  metadata single-partition sort fix, last sort-starvation class,
  confirmed dead in prod).
- AGING LANE LIVE: "[SEGMENT:BUILD] aging lane: claiming oldest-first
  (oldest pending 62865s > lane 21600s, claimable 83903)" — lane engaged
  on 4/6 compactors within 8 min of the roll (defaults 21600s/0.25, no
  pin needed). OLDEST PENDING MOVING for the first time in ~17h:
  11:33:45Z (stuck since 2026-08-18) -> 12:02:31 by 05:42:06Z — +28m46s
  of cohort progress in 47 min of wall clock.
- DRAIN FLIPPED DECISIVELY: pending (status=0) pre-roll drift -60/min
  (70,545 @04:28:30 -> 69,599 @04:42:49 with the 15s superbatch pin;
  ~19h projected). Post-roll 7-min samples: 66,108 @05:07:03 -> 64,555
  -> 62,815 -> 61,691 -> 60,402 -> 58,937 @05:42:06 = -7,171/35min,
  avg -204.6/min net (window rates -222/-249/-161/-184/-209), 3.4x the
  pinned pre-roll rate. Projected time-to-zero at the sustained average:
  ~4.8h (58,937/205 ≈ 287min from 05:42Z ≈ 10:30Z), arrivals steady.
- ZOMBIE CLAIMS RECLAIMED: status=1 lease-expired (>300s) 19,418 @04:28
  across 205 distinct builder uuids (live fleet is ~11 builders — the
  rest are OOM-dead uuids accumulated on .108) -> 18,675 @05:07 ->
  16,473 @05:42 = -62.8/min decay; projected at the ~2k live working
  set in ~3.8h.
- CYCLE RATE: batch-done lines on full-window compactors 4-5/8min at
  128-160 segments per batch (528-627MB accumulated per super-batch;
  fleet ~308 segs/min consumed by compactors alone; ingesters add
  2-5 batch-dones/8m each) vs pre-roll 2-4.5 super-batches/8min of
  pin-paced slivers (the "3-5 per 8 min" measurement).
- BUILDER MEMORY — P1 FINDING (not steady during catch-up): the drain
  surge OOM-cycles builders. Compactor restarts 0@04:55 -> 23 by
  05:42:39 (28wzk 5x then NODE-EVICTED "node low on memory: available
  924Ki" and replaced by hjvnj; hgmcv 7x; v8ksh 6x; 7tbbx 4x; v7lvf 1x
  @47.2Gi/48Gi; 29fqm 0x @36Gi) — tempo ACCELERATING at window end
  (+5 in the last 6.5 min), plus ingesters OOMKilled ~every 10 min
  (8Gi pods; 05:14/05:24/05:35). Attribution: fast-cycle deaths (pods
  <5 min old) implicate the L0 build path on the FAT oldest cohort —
  12 concurrent builds x 500-630MB-compressed super-batches x 3-6x
  decode inflation >> 48Gi once 3 merge workers also run; slow deaths
  show ~1.1M-row httprequest.* merge demotions at the end. Pre-.109
  the 15s pin kept super-batches sliver-small and newest-first hit thin
  fresh segments — same OOM class that minted the 205 dead uuids, now
  intensified by doing the work. NOT the fetch/decode=8 leg (batches
  are seal-bounded upstream of it). Ingest/query health unaffected
  (router 5xx = 0, zero real write failures; work is fenced + merges
  stream per-batch commits, so net drain holds regardless). LEVERS if
  the churn outlives the fat cohort (config-only, one line + env-rev):
  compactor ZO_SEGMENT_BUILD_CONCURRENCY 12 -> 6-8, or ingester 5 -> 3,
  or ZO_L0_SUPERBATCH_MB 512 -> 256 fleet-wide; six steady builders
  likely out-drain twelve crash-looping ones.

## V2 REAL-WORLD ACCEPTANCE (2026-08-19)

- SHIP: TAG v0.93.0-vix-20260819.110 (engine 83976d1963, M14-M16 = v2
  complete). Builds x86 19m47s / arm 21m37s, mimalloc both; push gate
  "OK: v0.93.0-vix-20260819.110 pushed to both registries (commit
  83976d1963cf, differs from v0.93.0-vix-20260819.109)", digest verified
  identical in BOTH ECRs (sha256:cdebdf1f5446...). DEV PR #284 merged
  07:44:41Z, all 8 pods Ready on .110 in 93s, 0 errors. PROD PR #435
  merged 07:48:06Z --admin, all 22 pods Ready by 07:55:36Z (~7.5 min),
  0 crashloops (stale evicted 746f/28wzk pod object deleted mid-roll).
  Prod commit also carried the DRAIN RETUNE: ZO_L0_SUPERBATCH_MB 256
  (new) + compactor ZO_SEGMENT_BUILD_CONCURRENCY 6->12 (halved batches
  at full parallelism; #434's 6@512MB had collapsed the drain to
  -16..-31/min and by 07:47Z pending was GROWING +66/min).

- ACCEPTANCE (prod, live data, through router, root auth; walls =
  client wall_ms / response took; evidence = querier log lines by
  trace_id; .107 baselines from V2 PROD LAUNCH smoke):
  C1 row-store star, "default" 1h: 1371ms wall (baseline 1.46s wall /
    726ms engine; fresh hour now 237M rows vs 18.7M at launch).
    EVIDENCE SimpleSelect(10) + "simple select metadata pre-prune
    dropped 238 of 240 files".
  C2 histogram 5m x 1h "default": 32.4s / 237.4M rows = 7.3M rows/s vs
    baseline 2.67s / 20.58M = 7.7M rows/s — PER-ROW PARITY, dataset
    11.5x. EVIDENCE SimpleHistogram + merged file zone-served
    "histogram hits: 513577" in 366ms behind an M14 wave; 239/240
    files were sidecar-less L0 -> scan branch (BACKLOG, see below).
  C3 stats arms, aws_vpc_flow_logs 1h (~733M rows): count(bytes)
    722,586,169 in 14.1s; min(bytes)=28 9.2s; max(bytes)=6,033,149,753
    9.9s. BOTH SPOT CHECKS MATCH (ORDER BY bytes ASC/DESC LIMIT 1 =
    28 / 6,033,149,753); count(bytes) < count(*)=734,158,039 (null
    bytes on NODATA rows, sane). EVIDENCE M16 FIRED:
    "index_optimizer_rule: Some(SimpleCountField(\"bytes\"))" ->
    "found count: 21451704 ... took: 309 ms" (presence-stats answer),
    "Some(SimpleMinMax(\"bytes\", false))" -> "found min_max:
    Some(I64(28)) ... index fetches: 0 (0 B), took: 147 ms".
  C4 indexed needle, host.name=<real collector pod> "default" 1h:
    17.1s / 235.6M rows vs baseline 2.9s / ~20.6M — rows 11.4x, time
    5.9x = ~2x better per-row. EVIDENCE index served the LIMIT from
    the merged file in 258ms ("found select row_nums: 10"); wall is
    the 233-file L0 sweep (is_add_filter_back: true).
  C5 demoted needle (trace_id) "default": fresh hour 23.0s/236.6M —
    zero .bf coverage yet (WARN "all 237 files have bloom_ver=0"),
    full M15b filter-back ([SCAN:NARROW] columns [_source,_timestamp,
    trace_id]). Bloom-covered 8h window (18th 16:00-24:00Z): 170.7s /
    934M rows, EVIDENCE "input=2691 (with_bloom=10 ...) kept=2681
    (no_bloom=2681), dropped=10" — ALL 10 bloomed files dropped, 0
    false keeps; the 2681 kept are sidecar-less L0s (99.6% of the
    window's files) that had to scan. The .bf arm is CORRECT;
    coverage is starved by the merge backlog.
  C6 match_all('error') "default" 1h: 53.6s / 236M rows vs baseline
    5.9s / 20.6M — per-row ~par (9.1x time at 11.4x rows). EVIDENCE
    "(_all:error)" index_condition served 10 rows in 164ms from the
    merged file; L0 sweep dominates.
  C7 dictionary-first top-k/distinct, aws_vpc_flow_logs action:
    CANONICAL shapes dispatch: "Some(SimpleTopN([\"action\"], 10,
    false))" / "Some(SimpleDistinct(\"action\", 10, true))", merged
    files served from the term dictionary with index fetches: 0
    (287ms per partition). 1h 766M rows: 9.7s; 6h 3.96B rows: 12.5s
    (317M rows/s) vs the SAME 6h through the scan path 30.8s (127M
    rows/s) = 2.5x class win, L0 share bounds it. QUIRK (engine
    follow-up, same family as the .107 histogram-alias quirk):
    ORDER BY <alias-of-count> and un-ORDERed DISTINCT defeat leader
    classification -> optimizer_rule None (UI sends canonical forms).
  C8 cold window (s3_access_logs, 8-9h back, post-roll cold caches):
    7.0s / 64.1M rows. M14 EVIDENCE "prefetch wave: group 0, cold
    files 3 of 3, prefetched 3 (9 fetches, 99.98 MB), took 3673 ms"
    = 3 fetches/cold file batched in ONE wave (2 tails + dict
    directory), eval found readers Shared (total 58 fetches /
    101.26 MB on that partition).
  C9 fleet health over the whole run: 0x401, 0 panics, 0 OOM events,
    0 router 5xx, all 22 pods 0 restarts end-to-end.
  C10 dev spot: C1 301ms/10 hits; C2 1.74s/519k rows/13 buckets; C7
    canonical topk 680ms/8.7M rows with SimpleTopN dispatch line —
    on dev's DRAINED fleet the dictionary path is the whole wall.

- THE REAL-WORLD HEADLINE: every v2 query class works exactly as
  designed on merged files; wall times on fresh/recent windows are
  governed by the UNMERGED L0 BACKLOG (fresh hour ~99% sidecar-less,
  even 18th-evening hours 99.6%) — index-off L0s cannot use ANY index
  class by design, so the drain IS the perf roadmap now.

- DRAIN (retune verdict): builds 656-666/min sustained (2.1x .109's
  golden window, 3.5x #434) at ZERO OOM — compactors 21-31Gi/48Gi,
  12C busy, aging lane firing (oldest 66704s > lane). BUT arrivals
  have DOUBLED to ~690/min (vpc flow logs alone ~733M rows/h), so
  pending still creeps: 63,039 @07:55:50 -> 63,754 @08:15:41
  (+24/min net). ~59/min of build slots are consuming the 13.3k
  zombie-claim pool (reclaim decay -59/min, ~3.7h to clear), which
  then redirects to pending. The <= -150/min target is NOT met at
  peak arrivals; expect the flip off-peak or after zombie clearout —
  else next step is more slots inside the demonstrated memory
  headroom (16-18 concurrency or compactors 6->8, cap 10). Oldest
  pending advances 0.72x realtime (13:28:56 -> 13:43:22 over 20 min).
- LIFECYCLE INSURANCE: pending != 0 -> obs-20260818-retention-1d
  STAYS at Expiration Days=2 (verified via GET). Expiry margin at
  current oldest-age growth (+0.28h/h from 18.5h): ~4 days — safe,
  revisit at pending=0 (revert to Days=1 then).
- DEV CAVEAT (report-only): dev ingesters OOMKilled 4x post-roll
  (limits 8Gi, steady RSS 360-590Mi) with pending=5 — NOT drain
  churn; ingest-path spikes on the fat-schema "default"/k8s streams
  outrun the memory circuit breaker (503s fire first, senders retry,
  no loss; same shape as the .107 dev-launch caveat and .109's 5/2/4
  pre-roll restarts). ENGINE FOLLOW-UP candidate: breaker headroom /
  admission shedding on the OTLP decode path.

## v2 M17 — compactor speedup package: gen-1 encode-once, encoded-chunk bloom hashing, byte-budgeted build admission, parallel rebuild blob build (2026-08-19)

Context: compactors are encode-CPU-bound (6.5-11.7 of 12 cores busy),
inbound ~700 segs/min at 4.45MB avg ≈ 250MB/s decoded, and every byte was
encoded twice (L0 build, then the gen-1 merge re-encode). Four items,
owner-approved scope.

- ITEM 1 (984da5e6c9) — gen-1 docs-copy (encode-once, the headline).
  Rebuild-path merges (multi-input, index_merge=false over index-off L0s —
  the 145-234s prod class) re-encoded every docs byte because the #51c
  passthrough demanded exact docs-schema identity and v2 all-present-columns
  files carry PER-FILE UNIONS — essentially every prod gen-1 disqualified.
  The qualification now builds a WIDEN PLAN (vortex_index::docs_widen_plan):
  shared columns must match at the stored vortex dtype; output-only columns
  synthesize per chunk as all-null constants (encode to ~nothing — chunk
  surgery in the M6 style, never a re-encode). The stats/zone splice side
  needed NOTHING: append_spliced already synthesized zero-presence chunk
  rows for input-absent columns. Fail-open is PER INPUT now (pre-M17 one
  miss re-encoded the whole merge): a genuine type flip / stats-less /
  unreadable-index input decodes + re-encodes through store-only pushes at
  its concatenated position (the writer's index-only mode accepts StoreOnly;
  finish still refuses index/store row divergence) while every other input
  copies — counted in the merge summary ("copied N (M schema-widened),
  re-encoded K (fail-open)" — the prod fail-open probe). The same widening
  reaches the indexed fast path's disjoint/concat copies through the shared
  qualifier (schema-subset gen-2 inputs stop decoding too); the term
  derivation scan stays decoded by design (the win is the re-ENCODE).
  PINS: widen-plan edges + encoded-chunk widen roundtrip (vortex); gen-1
  differential vs the forced-decode oracle — content equivalence, §11
  stats-splice parity, exact null placement per input run, M4 region
  decomposition (regions internally DESC, cover exactly, k-way merge ==
  global sort); storage: verbatim-copy no-bloat vs Σ input docs blobs
  (measured 0.92x — the M6 coalescer shrinks tiny slices; a symmetric ±5%
  vs a re-encode is NOT a sound assertion: 0.82x vs a same-order re-encode
  from fresh scheme sampling, 0.59x vs the sorted interleave which destroys
  per-input value locality — the copy may only shrink, asserted) and
  copy ≤ decode-path output; per-input type-flip fail-open on both rebuild
  and fast paths. Tests pinning "schema mismatch disqualifies" retargeted
  to type-WIDTH flips or force_decode where they pin the decode arms.

- ITEM 2 (d4a14a765e) — composite bloom hashing off ENCODED chunks + the
  encoding-class census. The M12 coverage scan was decode-bandwidth-bound
  (6.0s of a 14s wall; 1.9x on 8 workers). Present demoted columns now hash
  per chunk by stored encoding class: DICT → decode ONLY the chunk
  dictionary + codes, hash each value referenced by a valid code once
  (referenced+valid+non-null keeps the hash SET exactly the per-row set);
  FSST → ONE bulk decompress of the compressed heap (canonicalize's own
  call), hash raw slices off the uncompressed-lengths walk — no views, no
  arrow, no utf8 revalidation; other → canonical per-row, chunk-local.
  ONE value policy everywhere (BloomOnlyHasher::raw_sink = the decoded
  path's per-value body). Fields with no docs column keep the _source scan
  (#51c-d). CENSUS: one info line per merge — "bloom coverage encoding
  census: dict=X fsst=Y other=Z chunks over N inputs" — the prod probe for
  the follow-up report. PIN: encoded-vs-decoded hash sets EQUAL and full
  .vxi (bloom blob incl. guards + per-field sections) BYTE-equal over real
  vortex.dict (the M12 recipe), real vortex.fsst (rebuilt through the
  passthrough writer — the merge-input lineage; this build's compact
  sampler picks zstd for every synthetic string shape probed,
  m17_probe_stored_encodings), zstd and constant columns, with nulls,
  empty string, oversize value.

- ITEM 3 (3c04b5a442) — byte-budgeted build admission,
  ZO_SEGMENT_BUILD_MEMORY_BUDGET_MB (default 0 = auto 40% of detected
  cgroup memory, floor 256MiB). Replaces the count-knob treadmill (4
  tuning PRs in 24h): the OOM dimension is resident DECODED bytes (1-10x
  per compressed byte by stream shape), which counts cannot bound. One
  process-wide budget, two reservation classes: CLAIM (estimated = Σ meta
  size × inflation EMA seeded 5.0 α0.2 clamp[1,64], reserved before
  fetch+decode, RESIZED to post-decode actuals — the frames stay resident
  through the batch — EMA corrected, released at batch end; waits >50ms
  logged at info, counts only) and BUILD (each stream-chunk build reserves
  its actual decoded input bytes for its duration).
  ZO_SEGMENT_BUILD_CONCURRENCY becomes the SECONDARY count cap, default
  3 → 16 — the byte budget is the binding control. ALWAYS ADMIT AT LEAST
  ONE per class (independent floors: a fat claim cannot starve the first
  build; an oversized unit proceeds alone, never deadlocks). PINS:
  admission math units (fits/waits/floors/resize-with-wakeup); EMA
  seed+α+clamps+zero-ignore; fat-shaped multi-build at a constrained
  budget through the buffered(16) shape — all complete, concurrent
  reserved bytes ≤ budget, real overlap; config default/override tests.

- ITEM 4 (2ebdaae835) — parallel rebuild index-blob build. With the docs
  re-encode gone, the serial per-term loop (postings encode + dict-block
  build + bloom hashing) is the unspilled rebuild's blob-phase dominator.
  The in-memory term map is range-partitioned at REAL key quantiles
  SNAPPED TO FIELD BOUNDARIES (exact split — keys are in memory; M10's
  output-keyspace invariants hold trivially in the writer's own key
  space); up to ZO_VIX_MERGE_KWAY_THREADS workers (capped by
  encode_threads, 4x over-partitioned onto a shared cursor) each build a
  TermSink; assembly re-cuts the terms-blob row blocks through the single
  continuous flush rule (write_index_blobs_recut) with inline plist
  pointer rebase, so the output is BYTE-IDENTICAL for any R: field
  bounds start ranges exactly where the sequential sink cuts dict blocks,
  plist regions concatenate in term order, per-range bloom accumulators
  merge by union (M12), cross-range ordering backstop kept. Spilled maps
  stay sequential (disk-order stream — no exact split points); maps under
  1024 terms skip partitioning (move-job builds). PIN:
  m17_rebuild_parallel_blob_build_byte_parity — R=1 vs R=8 data AND .vxi
  byte-equal over many field regions, dense elision, plist cells across
  ranges, tiny row blocks, fts, per-field blooms, composite, #52 demotion.

- SANITY (single runs, idle box, pure default env; logs
  vixbench-m5/logs-m17/; corpora regenerate deterministically —
  merge_bench gen --heal --overlap --vary-schema, the new per-file-union
  flag, commit 0f659fa7ea; baseline binary = pre-M17 1b8913e46e built in a
  throwaway worktree, removed):
    gen-1 8x2M (16M rows, ID-heavy, term map SPILLS):
      before 142.27s wall / 220.5s CPU / VmHWM 11.15GB / 0 copied
      after  132.20s wall / 160.6s CPU (−27%) / VmHWM 6.46GB (−42%)
             / 8 copied (all schema-widened, 0 fail-open), concat stamped,
             out 2161.6→2109.4MiB (−2.4%); multiset-equivalent
             (digest 56a020fd44af832). Phase log: derivation scan 113.6s
             of 133.8s (the decode stays by design); docs store = 3.3s
             COPY (was the re-encode); wall is term-pipeline-bound on this
             shape — the CPU column is the fleet-relevant one (compactors
             are throughput-bound on total CPU, encode threads overlap
             inside one merge's wall).
    gen-1 8x500k (4M rows, still spills): 32.76→31.89s wall,
      51.95→37.94s CPU (−27%), HWM 4.92→3.45GB; equivalent
      (3aa0452a266157a8).
    gen-1 8x125k (1M rows, UNSPILLED): 7.54→7.32s wall, 12.30→8.76s CPU
      (−29%), HWM 2.15→1.58GB; AUTO demotion fires (4 ID fields) and
      ITEM 4 ENGAGES: "parallel index-blob build (3 ranges, 3 workers) in
      141.66ms"; equivalent (62d3e0dd3dc3f8aa).
    fast path, M12 corpus regenerated BYTE-IDENTICAL (gen lines match
      logs-m12/gen.log exactly; merge digest cb3c1efc20be5a93 == the
      M10/M12 pinned digest): before 13.07s/52.0s CPU → after
      12.99s/49.4s CPU (−5%). CENSUS (the item-2 design answer):
      "dict=0 fsst=0 other=2296 chunks over 8 inputs (8 workers, 5.31s)"
      — the synthetic trace shapes store vortex.zstd for every demoted
      column, so the dict/FSST fast arms idle HERE and the scan stays
      canonical (5.31s vs M12's 6.04s). The census line in prod logs is
      the follow-up report's instrument: it will say which encoding
      classes real demoted columns ride; the fast arms are pinned
      byte-exact and cost nothing when they don't fire.

- ITEM-4 REACH (honest): ID-heavy gen-1 term maps spill at the fixed
  1.5GiB budget (both 16M and 4M-row shapes above), and spilled builds
  stay sequential — the parallel blob build engages on unspilled rebuilds
  (small/mid merges, post-demotion generations, sidecar-only heals).
  Term-spill-budget tuning (or spill-aware range partitioning) is a
  follow-up if prod phase logs show blob-build domination on spilled
  shapes.

- GATES (logs /tmp/claude-1000/m17/): cargo build --workspace -j8 EXIT=0
  (warnings all pre-existing). Units EXIT=0: config 1976+3,
  vortex_index 229 (+ ignored probes incl. m17_probe_stored_encodings),
  openobserve-core 1918, openobserve-jobs 36, search 1009. Integration
  BOTH modes redirected `; echo EXIT=$?`: segmode ok 78.25s EXIT=0,
  default ok 48.06s EXIT=0 — FIRST runs, zero failures, no flake
  families tripped, no reruns needed.

- ROLLOUT NOTES (.111, after this report): prod env cleanup rides the
  budget — remove the ZO_L0_SUPERBATCH_MB=256 pin (back to default 512;
  the claim reservation now bounds decoded residency) and relax the
  concurrency pins (engine default 16 + the budget replace the per-shape
  retunes); ZO_SEGMENT_BUILD_MEMORY_BUDGET_MB stays auto (40%) unless
  compactor RSS says otherwise. Watch in prod logs: the merge summary's
  fail-open count (item 1), the bloom census mix (item 2), "memory
  admission waited" lines (item 3), "parallel index-blob build" (item 4).
  Corpora kept for re-measurement: vixbench-m5/v2/corpus-m17{,-small,
  -tiny,-fast} + out-m17 headline pair (~14G total); delete after .111
  proves the fail-open counter ~0 and the census lands in prod.

## v2 M18 — slice-guard rewrite (vortex.slice restarts + silent sliced-copy corruption), per-chunk fail-open, slice-accurate L0 original_size (2026-08-19)

- THE BUG (.110 prod, 368 heal restarts/6h): "heal docs passthrough
  failed after qualification; restarting the standard rebuild ...
  vortex.slice not permitted by ctx". Root cause chain: the #51c scan
  splits at the UNION of all columns' chunk boundaries, so any column
  stored coarser than the grid arrives as SLICES of one stored leaf
  (FlatReader::projection_evaluation row-range slicing; vortex also
  injects artificial ~100K-row splits into wide spans).
  vortex.runend / vortex.fastlanes.rle register only EXECUTE-time slice
  kernels — their slices keep a runtime `vortex.slice` wrapper the file
  writer cannot intern (not in ALLOWED_ENCODINGS, not in the session
  array registry) — the loud error, surfacing at finish_output (bare
  chain, matching the WARN). WORSE: encodings WITH a metadata slice
  rule reduce to offset-bearing forms whose serialize silently DROPS
  the offset — the M18 probe corpus (65Ki-row zigzag column vs a fine
  _source grid) re-read 126,100 of 131,072 status values WRONG after a
  verbatim copy (window 2 onward re-read the leaf from row 0; the
  M5-era pco probe was the same class). The old buffer-overlap sweep
  catches NEITHER: each window re-decodes the leaf via a fresh
  array_future (per-window segment fetch + alignment copies), so
  adjacent windows share no buffer addresses — pointer-identity blind
  on mem AND ranged sources.

- FIX layer 1 (scan, the correctness fix): DETERMINISTIC slice guard.
  Per-column stored-LEAF row boundaries from the layout tree
  (LayoutChildType contract: Chunk children recurse at their offsets,
  Transparent at the parent's, Auxiliary skipped — dict values / zone
  maps; unknown shapes FAIL CLOSED to canonicalize-everything). A field
  window copies verbatim only when BOTH edges lie on that column's own
  leaf grid; every other window canonicalizes exactly that column
  (recompressed by the passthrough compressor — decode-path work for
  that column window only). unwrap_shared (M12) stays for aligned dict
  chunks; an is_ctx_serializable backstop marks anything else. The
  overlap sweep + one-chunk lookahead are DELETED.

- FIX layer 2 (write, structural): docs_passthrough_strategy pre-checks
  every encoded column chunk against vortex::file::ALLOWED_ENCODINGS
  (the writer's own ctx seed) before the verbatim write; a non-writable
  tree canonicalizes + re-encodes THAT COLUMN CHUNK only — same rows,
  same positions, zone/stats splice untouched (o2 splices are
  row-logical at run level) — counted per chunk. Whole-merge restart
  remains only for pre-chunk / non-encoding errors. Sits under ALL
  copy paths (heal, merge fast path, M17 widen — they funnel through
  push_docs_encoded_chunk into this strategy).

- vortex.slice unwrap decision (task layer 2, documented): NO verbatim
  unwrap exists — the wrapper carries real range semantics (unlike
  Shared's pure cache indirection), and resolving it via the execute
  kernels (RunEndSliceKernel etc.) yields offset-bearing encoded forms,
  the exact shape the probe proved unserializable-sound. Canonicalizing
  the sliced window is the cheapest sound resolution; layers 1+2 do it.

- OBSERVABILITY: merge summaries extended — "copied N (M
  schema-widened), re-encoded K (fail-open), re-encoded C chunk(s)
  (fail-open), sliced-canonicalized S column-window(s)";
  MergedCoreFile.{docs_sliced_windows,docs_failopen_chunks}. Per-chunk
  detail at debug only. Expect C≈0 in prod (scan guard catches first);
  S>0 is the normal misaligned-column signal, not a fault.

- ITEM 3 (owner-found): L0 original_size inflation — the hour bucket is
  a zero-copy SLICE and arrow's get_array_memory_size reports the FULL
  backing run (prod: 1 record / ~400KB stored / original_size
  201,757,975), under-filling gen-1 merge groups fleet-wide (packing is
  by original_size). Fixed with per-column
  ArrayData::get_slice_memory_size (prorate-by-rows fallback). Pre-fix
  file_list rows keep inflated values until merged/expired
  (self-healing <=2 days); packing improves immediately for new files.

- PINS: m18_runend_slice_keeps_wrapper_the_write_ctx_rejects (vortex
  behavior pin), m18_sliced_scan_canonicalizes_and_copies_row_exact
  (the corruption corpus: row-exact copy, sliced windows counted,
  write-side fail-open 0, _source stays stored zstd),
  m18_writer_failopen_reencodes_slice_wrapped_chunk (injected
  Slice(RunEnd) >16KiB through the real writer: no error, count 1,
  rows position-exact; wrapper-free control copies verbatim keeping
  runend), m18_heal_passthrough_sliced_columns_stay_row_exact (core
  heal: passthrough completes — no restart, doc ids position-exact vs
  forced-decode oracle, stats splice parity),
  test_sliced_batch_memory_size_is_slice_accurate (item 3). M12
  shared-unwrap pins green unchanged.

- GATES (logs /tmp/claude-1000/m18/): cargo build --workspace -j8
  EXIT=0 (warnings pre-existing). Units EXIT=0: vortex_index 232,
  openobserve-core 1919, openobserve-jobs 37. Integration BOTH modes
  redirected `; echo EXIT=$?`: segmode ok 69.62s EXIT=0, default ok
  41.43s EXIT=0 — first runs, zero failures, no flake families tripped.

- PROD FREQUENCY (read-only, obs compactor pods via orbit, last 6h at
  2026-08-19T15:3xZ): "vortex.slice not permitted by ctx" 736 lines =
  368 heal-restart WARN events (~61/h); "vortex.shared not permitted by
  ctx" 0 (M12 fix holds); heal-restart WARNs total 401 (368 slice + 33
  other reasons). Every slice restart pays a full decode+re-encode
  rebuild — the M17 encode-once win was being clawed back on exactly
  the misaligned-column files.

- ROLLOUT NOTES (.112, after this report): watch the new summary
  counters (S normal-nonzero, C≈0) and the restart WARN rate → ~33/6h
  residual (non-encoding reasons). Unrelated but observed while
  counting: the .111 compactor fleet was OOMKilling (pods 21-23m old,
  1-3 restarts, Last State OOMKilled 137, limit 12 CPU) — rollout owner
  should check before stacking .112.
## .111 ROLLOUT (2026-08-19)

Image v0.93.0-vix-20260819.111 = engine e478e894a3 (M17 on top of .110/M16;
format-compatible, no DB/prefix cut). Ancestor gates: 60aa9edd10 AND the
fleet-.110 pin 83976d1963 both pass; HEAD..83976d1963 is exactly the 5 M17
commits + backlog. Builds x86_64 + aarch64 (-j8, mimalloc default), push
gate line verified: "OK: v0.93.0-vix-20260819.111 pushed to both registries
(commit e478e894a307, differs from v0.93.0-vix-20260819.110)"; amd64
7314ce5bfd, arm64 e2b42a49e3; describe-images confirmed in both ECRs
(13:56/13:57Z). Logs /tmp/claude-1000/roll111/.

PRE-ROLL BASELINE (prod .110, 11:29-11:44Z, day peak — captured before
anything rolled): 386 builds/min; backlog gauge (unbuilt_older_10m)
98,184 -> 101,020 = +192/min GROWING; ALL 5 ingesters OOMKilled ~6x each in
3h45m WITH build-conc pin=2 (32Gi limit); ALL 6 compactors OOMKilled 3-8x
(48Gi, conc=12 + superbatch 256). The .110 "zero OOM" retune did not hold
at day peak. Dev .110 same class: compactor 11 restarts, ingesters 10/8/4,
all OOMKilled. Gen-1 walls (60min, 621 lines): >=1.5GB class n=584, median
77.9s, mean 119.3s (survivorship-biased low — pods died ~every 35min,
killing long merges), per-stream medians: k8s_prod_public 302s,
s3_access 317s, apisix 248s @ ~4.2GB; docs_passthrough full on 45/619 (7%).

DEV (Phase 2): PR dev-ops #281-style bump = #285, merged 13:58:09Z; 8/8
pods .111 + ready by 13:59:27 (~90s). 5-min verify: encode-once live
(copied 10+57+56+6, 0 fail-open), smoke query 1.88M rows / 1.1s / 2743
scan_size (stats-answered), no new error classes. One ingester OOM 14:04Z =
the pre-existing dev ingest-path spike class. The "vortex.slice not
permitted by ctx" heal-passthrough WARN is PRE-EXISTING on .110 (orbit-dev
shows hits 02:09-09:03Z, hours pre-roll, ~1-2/h, fail-open to full
rebuild) — being fixed as .112.

PROD (Phase 3): prod-ops PR #436 (ONE commit d782ddc), merged 14:08:51Z
--admin; all 26 pods (10 compactors / 5 ingesters / 10 queriers / router)
on .111 + ready by 14:24:47Z, zero crashloops/pull errors. Contents:
newTag .111 + env-rev all roles; RETIRED ZO_L0_SUPERBATCH_MB=256,
compactor ZO_SEGMENT_BUILD_CONCURRENCY=12, ingester
ZO_SEGMENT_BUILD_CONCURRENCY=2; KEPT fetch-decode 8; compactor replicas
6 -> 10 (OWNER CALL 2026-08-19: drain surge, explicitly authorized over o2
parity; 10 IS the hard cap). Render-validated via kubectl kustomize on ops
pre-merge.

M17 LIVE EVIDENCE (Phase 4, 14:10-15:00Z):
- Encode-once (item 1): 219 docs-copy merges in the first 20min sampled:
  2,781 inputs copied (1,952 schema-widened = 70%), 53 re-encoded
  fail-open = 1.87% of inputs (16/219 merges had any) — "overwhelmingly
  copied" CONFIRMED. 88% of gen-1 merges rode docs-copy (12% dp=0 never
  qualified: type flips / slice-wrapper bites). Full-passthrough merges
  7% -> 80%.
- Walls (honest): heavy-encode log streams collapsed — k8s_prod_public
  302->158s (-48%), s3_access 317->137s (-57%), apisix 248->168s (-32%),
  monica -34% at same ~4GB size; sub-second-to-seconds on small gen-1s
  (23 files/3.1GB in 941ms). traces/default went 51->85s median (+66%) at
  equal size/files: derivation-scan-bound (the old re-encode OVERLAPPED
  the scan; the copy adds serialized IO) — CPU is the win there, not wall,
  exactly the sanity's caveat. Overall >=1.5GB median ~flat
  (77.9 -> 81.6-116.7s, composition+survivorship confounded), mean
  119.3 -> 83.9s.
- Admission (item 3): ZERO "memory admission waited" lines fleet-wide in
  55min at 473-520 builds/min — budget live but uncontended (superbatch
  granularity keeps claims ~2-4GB decoded vs 19.6GiB/12.8GiB budgets).
  The estimated->actual line is debug-level (invisible at info).
- Census (item 2) + parallel blob (item 4): ZERO lines — expected:
  census fires only on the indexed fast path's coverage scan (gen-2+
  inputs with demoted columns; today's workload is ~100% gen-1 L0 drain);
  parallel blob needs unspilled maps >=1024 terms (big gen-1s spill,
  small ones skip). DEFERRED: census mix + copied-ratio deep-dive to the
  .112 verification once gen-2 merges run.
- Drain: 386/min pre -> 480/min (first 15min) -> 473-520/min sustained;
  backlog +192/min -> +22 -> -63/min (14:30-14:40) -> ~flat +8/min at
  peak churn. NET: holding ~level at day-peak arrivals (~520/min) vs
  losing 192/min pre-roll. Gauge 141,912 at 15:00Z. Lease-lost fenced
  commits: 0 in 60m. Queriers: 0 restarts, 0 panic/401 all day; smoke
  count 180.4M rows/30min window in 15.6s + topn 8.2s (fresh-window walls
  still L0-backlog-governed, known).
- Lifecycle: pending ~142k >> 2000 — obs-20260818-retention-1d STAYS at
  2 days (verified via get-bucket-lifecycle; no revert).

OOM WATCH + ACTIONS (the honest part):
- Ingesters (rule: report on 1, re-pin on 2+): ingester-3 14:22:43Z,
  ingester-1 14:33:06Z — rule fired, PR #437 re-pin
  ZO_SEGMENT_BUILD_CONCURRENCY=2 merged 14:35:56Z, all 5 re-rolled by
  ~14:42. Attribution: BOTH previous-container logs show pure ingest-path
  traffic to the last line (ingester-1 ended in MemoryCircuitBreaker
  503s), ZERO admission/build lines — the spike class predates .111 and
  is pin-independent: 4 MORE ingester kills 14:47-14:57Z WITH the pin
  (ingester-1 x2, ingester-0, ingester-4). Ingest-path spike OOM is now
  the top ingester workstream, distinct from segment builds.
- Compactors: ~24 kills 14:19-15:00Z across 10 pods (worse absolute rate
  than pre-roll churn), all OOMKilled; visible killer in last-lines:
  DataFusion merge_parquet_files pool spiking 116MB -> 6.03GB in ~2s
  (trace_list_index parquet path; 1,964 peak-lines/30min, 98 >2GB) +
  faster merge cycling stacking download/decode transit (the 2026-08-17
  mechanism, intensified by M17 speed). NOT budget-governed memory by
  design. Kills cost throughput but the drain still nets ~level-to-neg.
- PR #438 (merged 15:02:21Z): restored ZO_L0_SUPERBATCH_MB=256 — the
  #436 retirement was configmap-GLOBAL, so it had doubled per-claim
  decoded frames on ingesters too; #437 restored only half the
  .110-interim envelope. Framed as completing the prescribed rollback
  (owner-approved value from #435), env-rev 111c both builder roles.
  EARLY POST-111c: 4 kills on fresh 111c compactor pods 15:08-15:12Z —
  256 does NOT stop the compactor DF-spike class either (as expected;
  it is not claim-shaped memory). Close-out cut verification here;
  main session measured 12.7 merges/min consuming 222 L0 files/min,
  p50 wall 121s on .111.

PROD-OPS PRS: #436 (roll, one commit), #437 (ingester re-pin, watch rule),
#438 (superbatch 256 restore). DEV-OPS PR: #285. All merged --admin/
immediate per standing auth.

OPEN ITEMS QUEUED (.112+): vortex.slice ctx fix (M18 in progress — the
heal-passthrough WARN class AND part of the 1.87% docs-copy fail-opens);
ingester ingest-path spike OOM (circuit breaker outrun; pin-independent);
compactor DF parquet-merge pool spikes (trace_list_index; consider
single-partition/cap); per-role budget question (40% auto on a 32Gi
ingester leaves nothing for memtable+DF+ingest spikes — a role-aware
default or ingester-specific budget MB); census + copied-ratio deep-dive
once gen-2 merges run; compactor replicas back to 6 when pending ~0
sustains (owner surge was drain-scoped).
## v2 M19-M21b (.112 payload) — lifecycle consistency, traces clamp, parquet-merge fix, ingest admission
- M19 (merge 68569f56c2): 404-behavior matrix fixed — a deleted object's row
  previously fed an INFINITE download re-enqueue loop and failed whole
  queries; now: 404 on a tracked data file deletes the row (+LOCAL_CACHE),
  merges pair-delete (.vix+.vxi) and HEAD-reconcile vanished inputs, the
  query path DEGRADES (unknown stats / empty stream, gated to canonical
  file keys + not-found class) with background row reconciliation.
  Retention job verified v2-correct (rows first, object pair 2h later via
  the deferred sweeper); the >=3-day config floor REMOVED so
  ZO_COMPACT_DATA_RETENTION_DAYS=1 boots. DESIGN flip: engine retention
  (1d) primary, S3 lifecycle (2d) safety net. 14 pins.
- M20b (merge of m20b-redo): traces now enforce
  ZO_INGEST_ALLOWED_UPTO/_IN_FUTURE on ALL span write paths — the real
  leak was the PIPELINE-OUTPUT branch (pipeline-rewritten _timestamp
  buffered unvalidated → the 2026-04/07 ancient partitions; 312k trace-file
  fragmentation); secondary: missing-ts fallback stamped now-5h (shifting
  old partition), now stamps now. SpanTsClamp: logs-parity inclusive
  bounds, partial-success counts, TS_PARSE_FAILED metric, one info line
  per batch. Metadata streams inherit upstream (verified). Compactor
  METADATA-class parquet merges now plan single-partition (extends M13;
  the 116MB->6GB/2s trace_list_index spikes, ~24 compactor kills/40min on
  .111); spill-pinned at 608MB corpus vs floored 256MB pool.
  OWNER QUESTION outstanding: retire trace_list_index? (upstream deleted
  it; composite bloom already serves trace_id equality.) Late-span
  caveat: default stays 5h; widen ZO_INGEST_ALLOWED_UPTO if buffered
  exporters ship legitimate late spans (drops now visible via counts).
- M21b (merge of m21b-redo): pre-body ingest ADMISSION — the ingester OOM
  root cause was breaker blindness (RSS sampled 1/s; bodies decompress
  and decode BEFORE any check; N concurrent batches expand 4-30x inside
  one sample). Now: admission middleware OUTSIDE decompression on all
  ingest route stacks (413 for CL>payload-limit with the body provably
  never polled; reservation of CLxfactor against the breaker envelope,
  503+Retry-After when full), breaker adds reserved bytes to its reading
  (zero-reservation trip byte-identical, pinned), rejection error-storm
  quieted to windowed counts. Envs ZO_INGEST_ADMISSION_FACTOR_RAW=6 /
  _COMPRESSED=30. Residual: gRPC OTLP not pre-reserved (msg-size caps +
  reservation-aware breaker); admission envelope engages with the
  breaker enabled.
- Process note: all three isolation worktrees spawned with an ANCIENT
  pre-restructure base; M19 self-corrected, M20/M21 were re-implemented
  as M20b/M21b in hand-made worktrees (analyses salvaged as specs; see
  memory obs-worktree-agent-bases).
## v2 M22 — boot-time vix_spill sweep (OOM-leak scratch reclaim) (2026-08-20, folded into .112)
- PROD INCIDENT (found mid-.112-release): PVC alert >92% on a compactor;
  census: 3/13 pods at 100% (196G, 0 free), one 94%; /data/vix_spill
  orphans up to 96G/pod, restart counts 40-52 correlate. Mechanism:
  term-spill/spool files are removed by their owners on completion, but
  SIGKILL (OOM) leaks in-flight files and container restarts keep the pod
  volume — the .111 compactor kill churn accreted them. Cache ~99G/pod is
  the separate by-design disk-cache cap (~50%); orphans consumed the rest.
  Also found: 3 pods phase=Failed (node memory eviction, container RSS
  38Gi vs 24Gi request — the DF-spike class blowing past request).
- OPS (2026-08-20 ~09:3xZ): swept vix_spill files older than 2h on all 10
  running compactors (freed up to 92G/pod; fleet now 8-61% disk), deleted
  the 3 evicted corpses. No merge interruption (2h >> 121s p50 walls).
- FIX (engine): job::init() removes data_dir/vix_spill WHOLESALE at boot,
  before any build/merge loop spawns (ingester::init never touches it;
  every writer create_dir_all's on demand — merge.rs upload spool,
  vortex_index spill.rs:72, container.rs spool sink). Every future OOM
  kill self-heals its leak on the restart. Unit: vix_spill_sweep_tests
  (removes files+subdirs, keeps siblings, tolerates missing dir).
## .112 GATES + BUILD + PUSH (2026-08-20, all green) + live OOM containment
- GATES on merged HEAD 50a1462c90 (M18+M19+M20b+M21b+M22), logs
  scratchpad/gates112/: build_workspace EXIT=0; units all EXIT=0 (config,
  infra, core, api, ingester, jobs, search, vortex_index); integration
  BOTH modes EXIT=0 (segmode 98s, default 51s, first runs, no flakes).
  release_x86 633s + release_arm64 690s EXIT=0;
  push v0.93.0-vix-20260820.112 VERIFIED by "pushed to both registries"
  log line. NOT YET DEPLOYED — argocd PRs pending gh re-auth + owner
  go-ahead after OOM verification.
- OOM CONTAINMENT (owner: "clean oom first", "no more memory — optimise"):
  all 5 ingesters OOM-cycling + 9/10 compactors killed <2h on .111.
  ZERO-COST live knobs applied 05:47Z with ops-obs automated sync PAUSED
  (restore = {"automated":{"prune":true,"selfHeal":true}}):
  breaker ratio 90->75 (cm), compactor DF pool auto(24G)->12288,
  fetch-decode 8->4. Codified in prod-ops zhichen 84ea1e2 (git==live,
  no churn at re-enable); the earlier 48Gi sizing commit was DROPPED
  (owner: memory is not unlimited). PVC sweep earlier: 3 pods were 100%
  full (spill orphans, up to 96G), find -mmin +120 -delete freed them;
  3 eviction corpses removed; M22 is the engine fix.
## .112 DEPLOYED both envs 2026-08-20 (~06:25-06:45Z prod) — ANCESTOR PIN ADVANCES to 50a1462c90
- Dev: PR #286 (bot-approved after env-rev P1 fix), rolled 06:0x, soak
  clean (0 err/panic, pending~6). Prod: knob PR #439 (owner-merged) +
  bump PR #440 (bot-approved; breaker role-scoped after review: GLOBAL
  90, ingester-local 75 container pin; router code-verified breaker-blind
  — proxy route tree mounts neither breaker nor admission) + follow-up
  #441 (rollback notes .110-.112, retention pin comment; merged --admin
  over a stale-head verdict, mismatch documented). Issue #442: ingester
  HPA min=max=5 contradicts its own max-24 comment — owner call.
- OWNER FLAG outstanding: ZO_INGEST_ALLOWED_UPTO=8760h pin means M20b's
  armed ts-enforcement discards nothing <365d — the 2026-04/07 trace
  fragmentation class still passes; closing it = narrowing the pin (own
  PR; bounds legitimate backfill too).
- FIRST-HOUR .112: build rate 723-826/min (was 473-520 on .111, +50-70%;
  arrivals ~645/min) → pending DRAINING even at day traffic, 113.1k at
  06:5x. M22 PROVEN live: "swept stale vix_spill scratch at boot" x2 on
  compactor OOM restarts. Kills NOT zero yet: ~2 compactor + 2 ingester
  OOMs in the first settled window; ingester-0 died while admission was
  actively shedding 2-3ms 503s — death by in-flight bytes ("segment
  buffer full: object storage flushes are behind" 46x/5m), i.e. flush
  path lag, not intake blindness. Verification window + error/panic
  watch running (persistent monitor); .113 candidates: flush throughput
  / segment-buffer sizing, compactor residual DF-adjacent kills.
## .112 first-hours verification (2026-08-20 ~07:1xZ) — kill-class fully mapped
- M20b PROVEN: trace_list_index DF merges log "DataFusion peak memory
  usage: 0.10 MB" (was 116MB->6GB/2s spikes). M21b PROVEN direction:
  ingesters shed 2-3ms 503s instead of dying blind; kills down from
  constant-cycling to burst-driven (buffer-full class predates .112 —
  orbit histogram shows bursts all through .111; NOT a regression).
  M22 PROVEN: repeated "swept stale vix_spill scratch at boot" on OOM
  restarts. Drain: 723-826 builds/min (+50-70% vs .111), pending
  draining through day traffic.
- REMAINING compactor kill class = #42 HEAL REBUILDS:
  ZO_VIX_L0_INDEX_OFF_STREAM_TYPES=logs,traces means the L0 population
  is index-off (file_list: logs 303k/312k no-index, traces 770k/774k),
  so nearly every gen-1 merge takes the rebuild fallback ("index merge
  not applicable, rebuilding terms from _source") — the multi-GB shape,
  stacking at the M12 REBUILD_GATE default file_merge_thread_num/2 = 4
  -> 48Gi breached. Mitigation: ZO_VIX_REBUILD_CONCURRENCY=2 pin
  (prod-ops #443). M23 CANDIDATES: byte-budgeted rebuild admission
  (count decode+docs bytes, not just a permit count); ingester
  flush-on-pressure + segment-buffer bytes counted in the admission
  envelope; gRPC OTLP pre-body reservation (M21b residual).
- REBUILD GATE ESCALATION (2026-08-20 ~08:0xZ): gate=2 (prod-ops #443)
  measured insufficient — fresh gate=2 pods OOMKilled in their first
  windows, classified rebuild-shape; #444 (approved) pins
  ZO_VIX_REBUILD_CONCURRENCY=1. Full budget at 1: ~19G rebuild + ~5G
  (7 fast-path workers x0.7G) + 12G DF + transit ~= 41-44G vs 48Gi —
  clears narrowly; NO safe value above 1. M23 (byte-budgeted rebuild
  admission, dispatch-side gating) is the real fix; leases stay warm
  while workers block (heartbeat-from-claim, compact/mod.rs:308).
- BLOOM-ONLY IDS (2026-08-20 ~08:4xZ, prod-ops #445 merged):
  ZO_VIX_BLOOM_ONLY_FIELDS=trace_id,span_id fleet-wide — gate=1 still
  killed (rebuild term maps scale with rows for unique-per-row IDs; no
  permit count bounds them). Values now hash into the composite bloom
  (SBBF ndv-sized, FPR preserved); equality stays served (bloom prune +
  column scan, NEVER index-exact — pinned fallback test); postings/top-k
  on the two ID fields lost (meaningless). Applies to LOGS too
  (correlation-by-trace_id = equality, still served). STICKY on written
  files; un-demotion = ZO_VIX_BLOOM_ONLY_NEVER + single-file-sweep heal
  (documented in the configmap comment). Expected effect: traces-group
  rebuild footprint collapses; compactor kills -> ~0 is the acceptance
  signal for the whole knob set.
- M23 item (found 2026-08-20 08:0xZ): SHUTDOWN-window thread panic —
  "vix-encode ... Attempted to use a Handle after its runtime was
  dropped" (vortex-io-0.79.0 runtime/handle.rs:39) right after the
  graceful-drain flush on a TERMINATING compactor (112d roll). Thread
  panic only: process survived (restarts=0), claim re-pends via lease
  reaper. Fix shape: drop/park encode workers (join or abort scope)
  BEFORE the vortex IO runtime in the shutdown sequence. Watch: recurs
  ONLY on terminating pods = shutdown race confirmed; on a running pod
  = different bug, escalate.
- M23 MEASURED REPRO (2026-08-20 08:36-08:41Z, pod 65b99c9d96-2h6jm on
  .112+112d knobs): ONE ~4GB logs-group rebuild walks RSS LINEARLY
  29.4GB -> 47.0GB in 4.5min (~65MB/s) then OOM at 48Gi. ~4-5x
  input-bytes amplification. NOT the term map (term spill caps 1.5GB,
  engaged via term_spill_dir). Candidates to audit in
  rebuild_over_sources/writer: postings/plist accumulation for FTS
  tokens (per-occurrence row ids, unspilled?), decoded-batch retention
  across coupled pushes, docs-blob encode pipeline buffers, zone folder.
  Baseline RSS 29.4GB pre-rebuild also above expected (~18-19G: 12G DF
  cap + 8x0.7G fast-path + caches) — audit idle retention too
  (mimalloc arena return?). Interim: group size 4096->2048 (prod-ops
  #446) so one rebuild fits; bloom-only ids (#445) fixed the traces
  term-map class; gate=1 (#444) caps stacking. M23 = bound rebuild
  BYTES in-engine + fix the per-row accumulation; revert #446+#444
  knobs after.
- M23 STATIC AUDIT (2026-08-20 ~09:3xZ): postings RULED OUT as the
  rebuild accumulator — TermSpill::write_run serializes the full
  BTreeMap<key, Vec<row_id>> and empties it (spill.rs), so term+postings
  stay bounded ~1.5GB. 112e falsified group-proportionality (all 10
  halved-group compactors killed <15min). Remaining suspects, in order:
  (a) finish_output plist/dict blob assembly — BLOB_TYPE_PLIST built
  whole in RAM at finish (writer.rs ~3107-3342); (b) gate-QUEUED workers
  holding downloaded+opened sources/plans (would also explain the ~12GB
  unattributed idle baseline; 112f workers 8->4 halves it — live A/B);
  (c) stream_merge_windows decode transit; (d) allocator retention
  (mimalloc segment hold) masking frees as RSS. M23 method: repro a
  ~2GB logs-group rebuild locally with heap profiling (mimalloc stats /
  bytehound), fix the accumulator, add byte-budget admission for
  rebuilds, then revert knobs #444/#446/#447.
## v2 M23 — rebuild OOM root cause FOUND+FIXED: eager per-input decode spawn (2026-08-20, .113 payload)
- ROOT CAUSE (profiled repro, M23-REPRO-NOTE.md at repo root):
  stream_merge_windows spawned ALL inputs' decode threads upfront; on
  the dominant concatenation-shaped order every not-yet-reached input
  sat FULLY DECODED in RAM (~2-3x original bytes, filling at aggregate
  decode speed = prod's linear 65MB/s climb). Scales with FILE COUNT,
  not bytes — why the group-size halving failed: same 1.56GiB as 18
  files peaked 1877MB, as 128 files 5726MB; 256 files/3.11GiB 11065MB.
- FIX: lazy spawn on first-needed row (Vec<Option<InputCursor>> +
  get_or_insert_with, ~20 lines, no knobs). Peak 11065->4320MB (scan
  resident 11.0->2.2GB), wall unchanged, outputs BYTE-IDENTICAL
  (sha256, both corpora, data+sidecar), 63/63 core_writer units green.
  Covers ALL arms that matter: standard rebuild, heal docs-passthrough
  (prod's common arm), indexed fast path — all call the fixed streamer.
- FALSIFIED en route: finish blob assembly (bounded +1.3-2.1GB spike),
  TermSpill estimate undercount (honest at this shape), allocator
  retention, term-map growth. REAL small gap found: index_key_terms
  postings bypass terms_bytes accounting (writer.rs ~2864) — .114 item.
- FOLLOW-UPS (.114): same eager shape in stream_inputs_disjoint
  (unqualified-subset only, bounded) + heal phase-2 fail-open;
  index_key_terms accounting; shutdown vix-encode Handle panic (earlier
  M23 item); ingester flush-on-pressure + buffer-in-envelope; gRPC
  admission. Repro harness kept: src/core/examples/m23_rss_repro.rs.
- POST-.113 VERIFICATION: revert interim knobs in one PR (workers 8,
  groups 4096, drop rebuild-gate pin, drop DF 12288 cap, fetch-decode
  8, breaker global 90 + drop ingester 75 pin when M21b holds).
  Bloom-only ids stay (owner-visible semantic choice, sticky anyway).
- DEV NOTE (2026-08-20 ~11:1xZ): dev ingesters OOM-cycling (restarts
  3-6; pre-existing ingest-spike class, dev has no breaker-75/admission
  tuning) — source of the metronomic ~176/15m health-check ERROR bursts
  on dev (5s probe interval against flapping peers). Fold a dev knob
  pass (ingester breaker pin; M21b factors if needed) into the
  post-.113 steady-state PR round.
- DEV KNOB PASS RESULT (2026-08-20 ~14:5xZ, dev-ops #288): DF cap 2048 +
  memtable 2048 + breaker 75 VERIFIED partially — metadata-merge DF
  spikes clamped (pool peaks 2.17GB -> 10-33MB, spill working), but dev
  ingesters (8Gi) still OOM under churn: the remaining mass is the
  subsystem SUM (memtable+builder claims+WAL+buffers), no single knob
  term left. Parked — dev is soak, self-healing; M23b/M24 (bounded
  decode; per-role byte governance) is the scope that closes it. The
  stale "builder sorts need the DF pool" doctrine was retired in-repo
  (M12 provenance now in dev ingester.yaml comments).
## v2 M23b — bounded interleaved decode (gated row-range streams) (2026-08-20, .114 payload)
- .113's lazy spawn was defeated in prod by INTERLEAVED merge orders
  (overlapping L0 time ranges touch every input in the first window) —
  all 10 compactors + 5 ingesters killed in the 45m acceptance window.
- M23b (M23B-NOTE.md at repo root): order-scattered inputs (>=8) stream
  as GATED row-range decodes — 4096-row units via new
  VixDocs::scan_docs_row_range, one consumer grant in flight per input
  (demand + low-water prefetch; DecodeGate monotonic watermark,
  deadlock-free by construction + tiny-caps byte-identity test), units
  deep-copied (take-gather; concat single-input is a buffer-sharing
  slice). Contiguous/low-N inputs keep the M23 free-running path
  bit-for-bit. Siblings (stream_inputs_disjoint, heal phase-2) now
  drain-start-spawned. Design (b) (gating unchanged whole-file streams)
  measured WORSE (12.8GB) and was rejected empirically.
- PROOF: interleaved peak 11399 -> 7924MB, decode transit FLAT
  1.3-1.9GB across all 879 windows (hard bound ~3GB = O(N x unit));
  wall +5.5%; sha256 byte-identical both shapes; core 1929/0, vortex
  232+1/0. RESIDUAL (pre-existing, control-run-proven): a writer-side
  ~+55MB/s accumulator held to finish — the M24 target; at 2048MB
  groups the post-M23b rebuild peak fits the prod envelope with the
  interim knobs still on.
## KILL MODEL DECOMPOSED (2026-08-20 ~17:2xZ, live RSS + phase trace) — M24
- .114 acceptance still failed fleet-wide -> live 6s RSS trace with
  phase correlation on a .114 compactor decomposed the kills into TWO
  phenomena: (1) ~20GB SAWTOOTH transients per L0 build wave = the M17
  admission budget default (auto 40% of 48Gi = 19.2GB) working as
  designed — a fresh pod cycles 2->23->2GB healthily; (2) a RISING
  FLOOR ratchet ~2GB fresh -> ~30GB aged (observed live: 26->42.7GB
  staircase into OOM). Kill = routine wave + aged floor. The auto-40%
  sizing assumed a small floor — false since DF cap + workers + caches.
- Every .112-era knob shaved WAVES; none touched the FLOOR. M23/M23b
  remain correct (decode transit measured flat) — fewer co-tenants in
  the collision. Interim: ZO_SEGMENT_BUILD_MEMORY_BUDGET_MB pinned
  (compactor 8192, ingester 4096; prod-ops #450) — the bounded-bytes
  regime that ran the .110 drain at 656-666 builds/min ZERO OOM.
- M24 = THE FLOOR RATCHET: attribute (mimalloc segment retention across
  wave-shaped allocations? metadata/file_list cache growth? reader
  cache? writer-side accumulator from M23B-NOTE control run) and fix;
  then revert the budget pins to auto AND the .112-era wave knobs
  (workers 8, groups 4096, gate pin, DF cap, fetch-decode 8). Also M24:
  ingester flush-on-pressure + buffer-in-envelope; gRPC admission;
  dev 8Gi envelope closure; shutdown vix-encode panic; writer-side
  accumulator; index_key_terms accounting.
- MODEL COMPLETED (2026-08-20 ~17:4xZ): 114b budget pins verified LIVE
  ("memory budget: 8192 MB (configured)") — and FRESH-floor pods still
  die mid-rebuild. Ergo a single prod rebuild reaches ~35GB+: rebuild
  memory scales with PER-GROUP DISTINCT-TERM VOCABULARY (cloudtrail/k8s
  logs: millions of distinct values — ARNs, ids, IPs), which the M23/
  M23b repro (50k-token vocabulary) never exercised. Vocabulary-scaled
  writer terms: resident-map spill-threshold estimation at huge key
  counts, bloom sets, and above all FINISH-phase index blob assembly
  (terms/dict/plist built whole in RAM, proportional to total distinct
  terms). This is the floor-ratchet's likely sibling (allocator
  retention from repeated giant finish spikes).
- M24 charter FINALIZED: (a) bound writer term-side memory for huge
  vocabularies — streaming/chunked index blob assembly, honest spill
  accounting (key-bytes + postings), bounded finish k-way; (b) floor
  ratchet attribution; (c) then revert ALL interim pins. Until M24 the
  fleet runs self-healing churn: acceptable (drain held all day, M22
  sweeps, leases re-pend, lifecycle margins wide).
- OVERNIGHT POSTURE (2026-08-20 ~18:5xZ, knob iteration STOPPED at 8
  PRs): PROD = self-healing churn accepted — compactors cycle on
  vocabulary rebuilds (poison-group pattern: high-card groups no pod
  completes; leases re-pend, M22 sweeps, lifecycle margins 2d, drain
  degraded but data safe). DEV = ingester WAL-replay death spiral (the
  2026-07-29 shape: all pods crash together, replay+live re-OOMs on
  8Gi), zero consumers, WAL durable — PARKED, do not knob further; the
  #291 1024 correlation was coincidental (crashloop predates it; same
  shed-then-OOM signature). M24 agent running (vocabulary-bounded
  writer/finish + ratchet attribution + high-card repro) — its fix ->
  gates -> .115 -> revert ALL pins is the morning path. NOTE for .115:
  consider ZO_COMPACT_MAX_FILE_SIZE physics REVISED — group bytes DO
  bound vocabulary (rows x unique fields), unlike the file-count decode
  mass; 2048->1024 is a valid emergency bridge if prod churn worsens
  before M24 lands.
- 114c BRIDGE FAILED (T+45 read, 2026-08-20 ~20:0xZ): group 1024 did
  NOT stop the kills — all 10 gen-114c compactors + 4 ingesters killed
  in 35m; pending 135k, drain 544 vs 702/min. The vocabulary-∝-rows
  inference is now ALSO suspect (or the mass isn't group-scoped at all:
  candidates the config CANNOT reach — single-file sweep rebuilds,
  per-merge fixed overhead × faster cycling, or an accumulator the
  M23/M23b corpora never trigger). CONFIG SPACE IS EXHAUSTED WITH
  CERTAINTY (8 knob PRs + .112/.113/.114 today). M24's high-cardinality
  profiled repro is the only remaining instrument — no further prod
  changes until it reports. Fleet stays in the accepted self-healing
  envelope; watch continues.
## v2 M24 — vocabulary bounded, kill model corrected (2026-08-20, .115 payload)
- HYPOTHESIS FALSIFIED at equal bytes (M24-NOTE.md): a 26M-distinct-term
  cloudtrail-shaped rebuild peaks 2670MB vs 2343MB for the 50k-term
  corpus (1.14x, NOT >3x) — the spill already bounds the map, and the
  estimate OVERcounts at uuid-key mixes (1.62GB est vs 1.39GB real at
  the trigger; the "spills too late" theory is dead).
- REAL vocab-scaled term FIXED (byte-identical, no knobs): the spilled
  finish stacked ~3x index size (sink term batches + dict/plist Vecs +
  one-shot terms encode + all blobs + container copy) — unbounded at
  gen-1 term counts. Now: sink regions spool to UNLINKED temp files on
  the spill volume, closed term batches stream through an incremental
  vortex writer (TermsBlobSpooler, DocsBlobEncoder's channel shape),
  the sidecar container streams spooled blobs (puffin add_blob_from) —
  finish resident = ~1x index (the returned sidecar Vec). hc peaks
  2670->2378 / 4621->4556 (finish now FLAT: RSS 434MB at blobs-built);
  sha256 identical on 6 config pairs (hc/lc/il x heal/flip; il baseline
  from a clean 06e87096ff build); wall +0.7/+2.8%; tests: core 1929/0,
  vortex 232/0, puffin 33/0. index_key_terms postings now accounted
  (M23 follow-up (c); column-driven arms only).
- REAL kill mass ATTRIBUTED, NOT FIXED (vortex 0.79-internal): the
  STANDARD-arm docs re-encode holds ~the whole group until encoder
  finalize (2.3-2.8GB per 1.2GB-data group, vocab-INdependent; freed in
  <1s at signal_finish). Measured inside encode_docs_stream:
  strategy_buffered=0 throughout, bytes_written stuck at ~13% of the
  docs blob until finalize => segments compressed but NOT writable:
  default WriteStrategyBuilder sequencing — a low-card column's
  DictStrategy run (logs always have one) allocates its values sequence
  id at run START and drops it only at column EOF, so nearly every
  later segment queues in BufferedSegmentSink's collapse. The
  heal/passthrough arm streams flat (measured). This IS the M23b
  "+55MB/s writer-side climber" and why the pinned build budget never
  saved compactors: segment_build_memory_budget_mb admits L0 BUILDS
  (jobs/segments.rs) only — merges never pass it. Follow-ups:
  (a) passthrough-shaped re-encode for the standard arm (OUTPUT BYTES
  CHANGE, valid shipped encoding — owner call + own acceptance),
  (b) vortex upstream fix/upgrade. Until then rebuild gate=1 + group
  clamp 2048MB are THE load-bearing pins for this mechanism; the wave
  knobs can revert independently of it.
- Part B floor ratchet: 10 same-shape merges in ONE process — floor
  FLAT (2027-2075MB, VmHWM 2502-2505MB constant, live 0MB at every
  floor): no in-process ratchet exists in the merge path; the floor is
  mimalloc retaining the last peak's pages, REUSED perfectly by
  same-shape ops. MIMALLOC_PURGE_DELAY=0 probe: floor -> 45-57MB, peak
  -> 1866MB, ~+11% wall — real cost, NOT zero-risk, nothing shipped
  in-engine; remedy is a prod-ops env trial on COMPACTORS only. Prod's
  2->30GB staircase = heterogeneous peaks (waves/query pools/transits)
  + parked pool-thread heaps that never run mimalloc's deferred purge —
  retention envelope, not a leak (live returns to baseline everywhere).
- .115 ACCEPTANCE FAILED (T+60 read ~01:0xZ 08-21): all 10 gen-.115
  compactors + 5 ingesters killed in 45m; spool disk 9-10% (M24's
  spooled finish works as designed — vix_spill on the data volume).
  Pending 153.9k. FIVE proven-correct engine fixes insufficient =>
  the remaining unexercised scale factor is SCHEMA WIDTH: prod streams
  carry ~2,164 fields (ZO_WAL_NARROW_SCHEMA note; ZO_COLS_PER_RECORD_
  LIMIT=65536) while every repro corpus was ~a dozen columns.
  Per-column encode state (chunk buffers, stats, strategy state) x
  thousands of columns x the re-encode arm is the M25 hypothesis — TO
  BE MEASURED FIRST (wide-schema repro), not inferred. WATCH DISCIPLINE:
  never claim kill trends from the log-watch ticks (gaps manufacture
  false streaks); only pod-status counter reads count.
## v2 M25 — schema width measured; heal-copy bloat + width-scaled gated transit FIXED (2026-08-20/21, .116 payload)
- WIDTH CONFIRMED as the unexercised factor, mechanism found in OUR code,
  not (primarily) the per-column vortex state the charter guessed. Repro:
  wide sparse k8s-shaped corpora (M25-NOTE.md; gen-wide/gen-wide-il,
  per-file subset schemas, per-field 32-value vocab so the curve isolates
  width from the dead M24 vocabulary axis). Curve at ~equal bytes
  (peak MB, unfixed): heal 701(w12)/2798(w200)/10621(w2000);
  flip 1067/3480/5149; interleaved wide (128 files) flip 15034 on
  0.53 GiB data = THE fresh-pod kill shape.
- P0 STORAGE BUG (heal/copy arm, every wide low-card stream): the M18
  slice guard's canonicalized dict-layout windows (decoded VarBinView root
  over encoded dict buffers) failed the whole-tree is_decoded_family test
  -> "encoded, copy verbatim" -> RAW 16 B/row views stored per column
  window. w2000 heal merge wrote 7,034 MiB docs from 450 MiB inputs
  (15.6x, ~88 KiB/leaf at 0.5 bits/B), peak 10.6 GB; outputs feed the next
  merge generation. FIX: root-keyed classification (is_decoded_root) in
  compress_or_pass + ColumnState routing, plus compact-for-residence
  (as-pushed nbytes snapshot keeps run/stripe/ratio streams bit-identical).
  w2000 heal 10621->3486 MB peak, out 7034->465 MiB; wil heal 12434->3478,
  7230->489 MiB. Regression test pinned (fails 2.9x on pre-M25 code).
- M23b transit was ROW-bounded => width-scaled bytes (normalize null-fills
  every absent union column per chunk; 4096-row unit ~35-50 MB arrow at
  width 2k). FIX: shared per-(type,len) null arrays per input
  (NullArrayCache + MergeChunk::synthesized; deep-copy + accounting skip
  them) + byte-adaptive gated units (8 MiB target, 256-row floor, old row
  bound as ceiling — narrow inputs bit-for-bit unchanged). wil flip
  15034->8283 MB.
- GATES: sha256 24 outputs (narrow/hc/wide/il x heal/flip): 22 identical
  incl. every .vxi; the 2 bloated heal outputs change BY DESIGN
  (value-equivalence: whole-column FNV + row/term counts + identical
  sidecars). Suites: vortex_index 233/0, core 1929/0. Wall median-of-3:
  w2000 heal +7.8% (prices in compressing what was written raw), wil flip
  -8.8% (faster). Commits: 396885b3e7 harness, 3aabb06136 fix, df8d74b325
  hash-col, f7c6c600ac strip, + note.
- RESIDUAL (vortex 0.79-internal, quantified at width): standard-arm
  re-encode holds ~3.6 GB to finalize for a 474 MiB blob at width 2,000 —
  per-column coalescing (1 MiB minimum x columns; invisible to
  buffered_bytes, the vortex TODO) + M24's dict-run sequence retention
  (90.3% of blob unwritable till EOF). Options unchanged from M24:
  passthrough-shaped standard re-encode (bytes change, owner call) or
  vortex upstream. Interim: gate=1 stays load-bearing for the standard
  arm; group clamp can stay 2048 MB (masses now scale with data bytes,
  not width x files).
- PROD NOTE for rollout: expect merged-object SIZES to drop sharply on
  wide streams (bloat fix) — storage/scan wins, and second-generation
  merges stop re-reading bloat. Fresh-pod kill shape (interleaved wide
  standard rebuild) drops ~28x -> ~5.2x data-bytes peak.
## M26 CHARTER (2026-08-21 ~06:1xZ): the floor is a LIVE per-job leak in
## the JOB layer — retention falsified in prod
- 116b (MIMALLOC_PURGE_DELAY=0, verified active, option name checked
  against libmimalloc-sys 0.1.44 v2) did NOT flatten the floor: 20-min
  pods at 26-35GB, same ~25MB/s+ slope. M24's repro attribution
  (retention, live-bytes zero) does NOT transfer to prod => the prod
  floor is LIVE bytes.
- SIGNATURE: proportional to processed JOBS (~70-100MB/s at ~12+
  merges/min + builds), invariant across every data-shape fix (M20b..
  M25) and every knob — because all of them changed layers BELOW the
  job machinery, and every repro called merge_core_files/writer
  directly, BYPASSING claims/heartbeats/file_list updates/broadcast.
- M26 REPRO: run the actual compact job loop (claim -> heartbeat ->
  merge -> commit -> file_list update -> broadcast) against a local
  sqlite meta store with hundreds of SMALL jobs; watch live-bytes per
  job. Suspects: heartbeat tasks/channels not terminated per job
  (compact/mod.rs heartbeat-from-claim), per-job registry/map entries
  (file_list broadcast queues, processed-id sets), schema-cache growth
  (2,164-field schemas cloned per job), grpc/S3 client pools.
- Purge pin stays meanwhile (floor is live, pin ~neutral; contract has
  its own revert condition).
## v2 M26 — THE FLOOR SOLVED: per-context DataFusion merge pools (.117)
- Job machinery PROVEN leak-free (harness drives the real claim/
  heartbeat/merge/commit loop, 320 jobs: 0.004-0.016 MB/job; heartbeats
  terminate; registries bounded — charter suspects a/b/c/e falsified).
- THE CLIMBER: every merge_parquet_files call built a PRIVATE
  DataFusion RuntimeEnv pool sized datafusion_max_size (search/
  datafusion/exec.rs create_runtime_env) — per-CONTEXT, not
  process-wide. N concurrent metadata-parquet merges x 12.87GB pools
  (the 12288 pin; auto-24G before it behaved the same) across 4 workers
  + builder lanes >= 48Gi = the 2->46GB/11min floor. LIVE reservations
  — purge-delay cannot return them (retention falsification explained).
  Prod count-match: 28 >2GB pool-cap peaks/3h = trace_list_index
  128-file merges; ~13-18x group bytes (residual amplification vs local
  streaming documented, bounded by the shared pool regardless).
- FIX (02d56508b6): ONE process-wide SHARED_MERGE_POOL for all
  merge_parquet_files contexts; query contexts untouched; no knobs.
  4 concurrent tli merges: RSS 4497->2670MB, tracked 4x1380->2047MB
  capped, 8/8 outputs sha-identical; pin test
  m26_merge_contexts_share_one_memory_pool; search 1013/0, core
  1929/0, infra 1085/0, jobs 38/0. NOTE: post-.117 the compactor DF
  12288 pin becomes a TRUE process bound (its intended semantics).
- .117 PARTIAL WIN + M27 (2026-08-21 ~10:1xZ): shared pool ~HALVED the
  floor slope (pod lives ~11min -> ~20-22min; kills continue). Heap
  content probe: the floor is ~99% BINARY buffers (not
  metadata strings), live anonymous heap. All inference exhausted =>
  M27 = sampling heap profiler (inert-by-default wrapper around
  mimalloc, env-activated ZO_HEAP_PROFILE_SAMPLE_EVERY_MB,
  size-weighted stack sampling, live-tracked, 60s top-15-stacks log)
  shipping inside the normal image; prod activation = env pin on a
  canary. Agent building it; then .118 + canary => allocation-site
  truth, then the final fix.
- LIFECYCLE INSURANCE 2->3 DAYS (2026-08-21 ~11:0xZ, GET-verified):
  oldest pending aged 24.9->28.3h in ~4h while the drain lost ground
  (builds 254/min vs arrivals 508/min, pending 264k) — the 48h loss
  line was ~20h out, inside the M27 canary->fix->.119 window. Raised
  obs-20260818-retention-1d Days 2->3 (other rules untouched; same
  out-of-repo AWS pattern as the owner-sanctioned 1->2 raise). REVERT
  to 2 (then 1 per the standing plan) once pending drains post-fix.
