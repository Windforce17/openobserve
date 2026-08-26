# M29 — merge throughput: root cause + fixes (2026-08-24)

Goal: ~1.07M unmerged in-window L0 files (search-latency tax). Root-caused with
live prod receipts (fleet .119, 10 compactors, obs20260818 meta), fixed
in-engine, proven with an in-tree harness. No pinned knob changed
(gate=1, max_file_size=1024 untouched).

## Root causes, measured (prod 2026-08-24 05:45-06:00Z)

Z1 — PRIMARY: 404 claim-zombies dilute segment builds into sliver L0s.
  wal_segments: 189,734 status=Building rows; 189,688 older than 2 days
  (kill-era; objects lifecycle-expired) vs 2 real pending. Every build claim
  fills with them: 722,203 "skipped this round ... 404 Not Found" ERROR lines
  per 30m fleet-wide; over a 1,000-batch prod sample (30m window): 78,784
  segments claimed, 77,807 zombies (98.8%), 977 real -> 4,527 L0 files =
  4.63 sliver files PER REAL SEGMENT, where the #54 super-batch design
  amortizes 32-256 real segments into ~0.2-0.3 files/segment (~15-20x
  amplification). Result measured:
  file_list insert slope ~112k rows/hour (maxid 7,469,398 -> 7,475,988 in
  211s) vs core-merge consumption ~91k files/hour -> net L0 growth ~+40k/hour
  -> standing 1.076M in-window. The zombies also poison claimable_stats
  (byte-adaptive batch sizing + the M13 aging lane permanently engaged on a
  cohort that can never build) and cost ~400 S3 GETs/sec.

T1 — visitation cadence: a closed hour is visited once by the Current lane
  (1 job per stream-hour, offset walk), then only by the old-data lane
  (prod 600s interval; ENGINE DEFAULT 3600s) which skips the newest
  old_data_min_hours (dead zone — the hottest query window). Live jobs table:
  pending=36 vs running=212 — generation is just-in-time with zero buffer.
  Harness B1 (engine defaults, no old-data round): 9,216 of 50,400 files
  consumed (18.3%), then a permanent plateau with pending=0.

T2 — cutter/consumer mismatch: group_files_into_batches cut at bytes
  (1GB) + max_group_files (10k) while merge_files consumes at most
  max_file_count=128 files per batch (#51 width cap) — each visit merges the
  128-file head of a batch and strands the tail for the NEXT visit
  (prod: 208 "keeping N unmerged for a later cycle" lines/30m; harness:
  "merged 128/700 files; keeping 572 unmerged" per stream-hour visit).

Note vs the tasking brief: at 05:47Z the rebuild gate was NOT idle — 873
rebuild admissions/30m, all waiting >50ms (contended), 877 core-merge
completions consuming 45,426 files / 726GB per 30m. The 10x lever is the
arrival side (Z1) plus work-conserving revisits (T1/T2), not gate headroom.

## Fixes (engine)

1. Z1 — `jobs/src/job/segments.rs`: a claimed segment whose object GET
   returns typed NotFound is terminally resolved — fenced flip to Built with
   zero files (`ZO_SEGMENT_BUILD_404_TOMBSTONE=true` default; kill switch
   only). The sweeper then retires the row through its normal
   confirmed-delete path (NotFound counts as confirmed). Drain is incremental
   through the existing claim flow — no scan: at the observed 722k
   claim-touches/30m the 189.7k rows resolve in minutes, each exactly once.
   Log discipline: per-item at DEBUG, one count line per batch at WARN
   (replaces the 24k/min per-item ERROR flood that re-ingested into obs).
   `batch done:` stats gain `gone=N`.

2. T1 — merge-debt sweep (`CompactionJobType::Debt`,
   `ZO_COMPACT_MERGE_DEBT_INTERVAL` default 60s, 0=off): every sweep
   re-enqueues a merge job for EVERY closed settled hour in the retention
   window that still holds >= old_data_min_files small files, oldest hours
   first (M13 aging), dead zone included. Idempotent per (stream, hour) via
   add_job's (stream, offsets) dedup — pending/running rows untouched, done
   rows resurrected; converged hours drop out of the query. Fleet fairness
   unchanged (jobs claimed fleet-wide as before).

3. T2 — `group_files_into_batches` also cuts at max_file_count, so every
   dispatched batch is fully consumable in one pass. Output sizes unchanged
   (the consumer sealed exactly 128-file outputs before too); open-hour
   incremental rounds now seal full-width groups (the hot-window sliver cap).

## Harness (src/core/examples/m29_merge_throughput.rs)

Seeds 3 streams x 24 closed hourly cohorts x 700 tiny index-off .vix L0s
(50,400 files, 692MB; real core-writer templates, timestamp-interleaved so
merges take the REBUILD path) into sqlite + local store, NO jobs — then runs
the REAL generation lanes + run_merge + JobScheduler + MergeWorker at REAL
cadences (no time compression): 10 workers, 10 job slots, rebuild gate=1
(prod pins). Measured: COMPACT_MERGED_FILES + file_list rows.

BEFORE = commit aa6ebbb91c (base engine + harness):
- B1 (defaults; old-data round never fires inside the run): consumed 9,216
  (18.3%) in ~160s — one 128-file bite per stream-hour — then PERMANENT
  plateau, pending=0, 41,184 L0 rows stranded.
- B2 (old-data lane at prod's 600s cadence): first sweep 9,216 by t=187s,
  then SILENCE until the old-data round at t=810s adds exactly +8,832
  (69 cohort-visits x 128; the 3 dead-zone cohorts excluded), total 18,048 by
  t=933s, silence again => sustained 8,832 files / 600s round = ~14.7
  files/s, and the 2h dead-zone cohorts NEVER drain.

AFTER = commit a87d5d3e10 (fixes; debt lane at its default 60s cadence):
- One debt sweep enqueued all 72 cohorts at cycle 1 (pending=62 after the
  first claims); the corpus then drained CONTINUOUSLY at the gate's speed:
  50,700 files consumed (all 50,400 L0s + 300 re-merged intermediates),
  rows 50,400 -> 182 converged outputs (99.6% drained, dead-zone cohorts
  included), zero "keeping N unmerged" partial batches, sustained 41.4
  files/s over the whole run (~46 files/s over the active window; ~22
  merges/min = ~330 merge starts per 15m at gate=1) — then a clean plateau
  with the debt query finding nothing. Partial-batch ("keeping N unmerged")
  events: B1=72, B2=141, AFTER=0.

Before/after: vs B1 (the engine-default steady state) sustained merge starts
go 0 -> ~330/15m and the stranded backlog 41,256 -> 182 rows (227x); vs B2
(prod env pins) sustained throughput is 3.4x (12.0 -> 41.4 files/s) with the
duty cycle going 20% -> 100% and the dead zone finally draining. At gate=1
the harness saturates the pinned rebuild gate continuously — job flow is no
longer the limiter, the gate is (>= 100 gen-1 merges/15m target exceeded
3.3x). The remaining prod headroom is the arrival side: the Z1 zombie fix.

## Gates

- cargo build --workspace: PASS
- config --lib: 1981 passed. openobserve-jobs --lib: 39 passed (incl. new
  test_m29_gone_object_claims_are_tombstoned_not_retried; the pre-existing
  fetch_and_decode test updated to the 3-way contract). openobserve-core
  --lib: 1931 passed x3 consecutive (one one-off order-dependent flake of the
  pre-existing m19_reconcile_missing_merge_inputs test on the first run;
  passes standalone and in 3 reruns — documented flaky family).
- integration_test BOTH segment modes: PASS (seg-mode EXIT=0, 70.2s;
  non-seg EXIT=0, 44.0s; logs in session scratchpad integ_seg.log /
  integ_noseg.log)
- New tests: test_m29_merge_debt_sweep_enqueues_and_resurrects,
  test_m29_group_cutter_honors_max_file_count,
  test_m29_gone_object_claims_are_tombstoned_not_retried (+ updated
  test_fetch_and_decode_isolates_bad_segments).

## Prod expectation

Zombie fix alone: the 189.7k zombies resolve within minutes (722k
claim-touches/30m, each row tombstones on first touch), claim batches return
to 32-256 REAL segments, and L0 arrivals drop from 4.63 files/real-segment
(~110k/hour) to the batch-amortized ~0.2-0.3 (~5-8k/hour, 15-20x) -> at the
measured ~91k files/hour merge consumption the 1.07M backlog drains in <1 day
and the standing in-window count collapses >10x. Debt sweep + cutter alignment make the merge
side work-conserving (one visit drains an hour; hot dead-zone hours covered
within 60s) so the backlog cannot re-form when the gate has headroom.
