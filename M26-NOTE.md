# M26 — the compactor "live per-job leak": found in the DataFusion parquet
# merge arm (per-context memory pools stacking), not in the job machinery

Worktree pinned at `9f78374c6b` (post-.116 lineage). 20C/38G box, release
builds (`-j8`), mimalloc + the M23 live-bytes counting wrapper. Scratch
under `m26-data/`. Local commits (never pushed): `1983adfbdb` harness,
`8c8599001a` fix, then this note. No engine debug instrumentation was left
behind (nothing to strip — the only worktree-local edits are the example
and the fix).

## Verdict

**The job machinery does not leak.** Driving the REAL compact job loop —
claim → heartbeat-from-claim → download → merge → fenced streaming commits
→ file_list updates → broadcast → set_job_done — against a sqlite meta
store and a local-disk object store, live bytes are FLAT across 320 jobs in
both schema variants:

```
narrow (7-field schema),  320 jobs / 1920 files:  live 0.2 -> 1.6 MB  = 0.004 MB/job
wide (2,000-field schema),320 jobs / 1920 files:  live 0.2 -> 5.3 MB  = 0.016 MB/job
final registry probe (both): schemas_latest=8 (streams), schema_versions=0,
  settings=0, broadcast_q=0, dedup=0, deleted_files=0, alive_tasks=10,
  metric_series=7
```

That falsifies, with evidence, every charter suspect in the claim/commit
layer: (a) heartbeat tasks terminate on every exit (`alive_tasks` returns
to baseline 10; `JobLeaseGuard`'s select-on-closed-channel is correct),
(b) no per-job registry/map growth (broadcast queue, dedup sets, schema
caches, metric series all bounded), (c) 2,164-field-class schema clones
are transient — the wide variant adds 0.012 MB/job over narrow, (e) no
Arc cycles (everything drops).

**The prod climber lives in the arm every prior repro bypassed:**
`merge_parquet_files` — the DataFusion parquet merge used by
metadata-class compactor merges and the segment builder's flat/metadata
L0 builds. Every M20b..M25 repro called `merge_core_files`/the vix writer
directly, so this arm was never exercised.

## The prod evidence (orbit, obs compactors, 2026-08-21 03:40–06:40Z)

- `search::datafusion::peak_memory_pool` drop lines
  (`[trace_id merge_parquet_files] DataFusion peak memory usage: ...`)
  show **28 merges > 2 GB tracked in 3 h**, across ≥ 8 different compactor
  pods, many at **12.81–12.87 GB** — i.e. 99.4–99.9 % of the deployment's
  per-context pool pin (`ZO_MEMORY_CACHE_DATAFUSION_MAX_SIZE=12288` MB =
  12.885 GB; the compactor-role default `mem_total/file_merge_thread_num`
  computes the same 12.885 GB on 48Gi/4). The pool SATURATES.
- The class is `default/metadata/trace_list_index`: 128-file parquet merge
  groups (`max_file_count` cap), 0.3–1 GB original. Tracked peak scales
  ~linearly at **~13–18x the group's original bytes** until the cap clamps
  it: 49 MB → ~0.9 GB, 336 MB → 5.5–5.9 GB, 727 MB → 12.87 GB (cap).
  Example (pod cdwjz): 128-file download 06:30:01.39 → "merged 128 files
  ... trace_list_index ... original_size: 727511932, took: 3386 ms"
  06:30:04.00 → 12.87 GB pool drop 06:30:08.68.
- The deployment manifest's own note believed this class fixed ("M20b's DF
  parquet class is separately CONFIRMED fixed, 0.10 MB peaks") and sized
  the pin so "a 12G pool still holds two concurrent spikes". Both are
  falsified by the peaks above: the spikes are back at full size, and the
  **pools are PER MERGE CONTEXT and UNSHARED** — `create_runtime_env` built
  a fresh full-size pool for every `DataFusionContextBuilder::build`.
  With 4 merge workers + 2 live-lane workers + the L0 builder, the
  aggregate tracked ceiling is N × 12.9 GB on a 48 Gi pod: structurally
  negative headroom.

**This is the M26 signature.** A saturating context ramps 0 → 12.9 GB of
LIVE tracked reservations in seconds; big-group merges land every couple of
minutes per pod; overlapping ramps + mimalloc floor retention between them
climb the pod 2 GB → 46 GB over ~11 min and the OOM killer fires.
`MIMALLOC_PURGE_DELAY=0` (116b) could not flatten it — reservations are
live allocations — it only stretched pod life (20 min) until enough ramps
aligned. The slope is proportional to JOB THROUGHPUT because each job's
ramp is the slope. Every M20b..M25 fix was invariant because none touched
this arm. Pods die mid-backlog, jobs re-pend, the next pod re-claims the
same giant hours — the kill loop feeds itself (hour 2026/08/20/04 was
still being re-merged on 08/21).

## Repro (the new harness, `src/core/examples/m26_job_leak.rs`)

Runs the ACTUAL machinery end-to-end: `infra::file_list` sqlite backend,
real `MergeWorker`/`JobScheduler`, `compact::run_merge` claims,
`JobLeaseGuard` heartbeats, real merges, fenced commits, `set_job_done` —
prod-parity env pins from the prod manifests (role=compactor, 4 workers,
memory cache off, fast mode, 1024 MB groups; pool scaled to 2048 MB for
box safety).

```
cargo build -j8 --release -p openobserve-core --example m26_job_leak
# job-machinery leak curves (flat, table above)
m26_job_leak seed 8 40 6 2000 0     && m26_job_leak run 6      # narrow
m26_job_leak seed 8 40 6 800 2000   && m26_job_leak run 6      # wide
# the prod killer shape: trace_list_index metadata parquet jobs
m26_job_leak seed-tli 8 128 100000  && m26_job_leak run 128    # streaming plan
M26_TLI_NOSTATS=1 m26_job_leak seed-tli 4 128 100000 && m26_job_leak run 128
#   ^ statistics-free files deny the per-file ordering proof -> the plan
#     keeps the FULL buffering SortExec: the pool-hungry class, through
#     the real job loop
```

Honest residual: on every corpus topology I could synthesize WITH file
statistics (interleaved, chained pairs, mixed big+small), the local plan
elides the sort (adapter re-split → SortPreservingMerge, 20–80 MB peaks);
prod's ~13–18x-of-original tracked reservations mean its plan buffers.
The exact prod-side condition that denies the ordering proof was not
reproduced locally — but the fix below bounds the AGGREGATE regardless of
which plan shape any single merge takes, which is the part that kills pods.

## Differential table

```
variant                                   tracked peak (DF pool)   RSS/live peak       outcome
narrow 320 jobs (real job loop)           n/a (core merges)        flat 0.004 MB/job   no leak
wide-2000 320 jobs                        n/a (core merges)        flat 0.016 MB/job   no leak
tli streaming, 16 merges                  20–80 MB per ctx         ~4.4 GB transient   healthy
tli full-sort x4 concurrent, BEFORE       4 x ~1380 MB per ctx     RSS 4497 / live 3191 MB   the kill shape (prod: N x 12.9 GB)
tli full-sort x4 concurrent, AFTER fix    2047 MB TOTAL (shared)   RSS 2670 / live 1538 MB   bounded; all merges complete
```

## The leaker and the fix

Leaker: `src/search/src/datafusion/exec.rs` — `create_runtime_env`
(pre-fix lines 139–186) builds a **full `datafusion_max_size` memory pool
per context**, called from `DataFusionContextBuilder::build` for every
`merge_parquet_files` invocation
(`src/search/src/datafusion/merge/mod.rs:129`); saturated to the cap by
metadata-class merges, stacked across concurrent workers.

Fix (commit `8c8599001a`, no knobs, no data-path change):
- `exec.rs`: `SHARED_MERGE_POOL` — ONE process-wide tracked pool (same
  `datafusion_max_size` sizing semantics, same configured pool type) built
  lazily; `DataFusionContextBuilder::shared_merge_pool(true)` routes a
  context to it; `PeakMemoryPool` stays per-context (its log now reports
  the pool level at that context's grows — the pod-relevant number).
- `merge/mod.rs`: `merge_parquet_files` sets `.shared_merge_pool(true)`.
  Compactor merges AND segment-builder flat/metadata L0 builds share one
  bounded budget; concurrent merges now SPILL (correct, bounded, alive)
  instead of stacking fresh gigabytes (unbounded, dead). Query contexts
  keep per-context pools, byte-for-byte untouched.

Proof:
- Aggregate bound: 4 concurrent full-sort merges — tracked 4 × 1380 MB →
  capped at 2047 MB total; RSS peak 4497 → 2670 MB (−41 %); live peak
  3191 → 1538 MB (−52 %); all 8 batches complete.
- **Outputs byte-identical**: sha256 of all 8 merged parquet outputs equal
  before/after (spilling changes where intermediate data lives, not what
  is written).
- Job-machinery curves unchanged post-fix: narrow 0.004 MB/job, wide
  0.016 MB/job, streaming tli healthy (shared-pool level 186 MB with 4
  concurrent streaming merges).
- Pin test: `m26_merge_contexts_share_one_memory_pool` (exec.rs) — a
  reservation through context A is visible at context B's pool; default
  contexts stay isolated.

## Prod expectation

Compactors stop dying by pool-stacking: the aggregate DataFusion tracked
memory across ALL concurrent merges + builder is ≤ the existing
`ZO_MEMORY_CACHE_DATAFUSION_MAX_SIZE` pin (12288 MB) instead of ≤ N× it.
Giant trace_list_index merges spill and finish; the backlog drains instead
of re-claiming the same hours after every kill. The 12288 pin's own revert
condition ("revert to auto once verified") should be re-evaluated AFTER
this ships: with sharing, auto (mem/4 on role=compactor) bounds the whole
process to 25 % of the pod — a sane default. Follow-up worth filing
upstream: why the prod plan denies the per-file ordering proof for these
merges (13–18x tracked amplification vs the streamed shape) — fixing that
would additionally cut the spill/CPU cost, but it is not needed to stop
the kills.

## Tests

```
search:            1013 passed / 0 failed (6 ignored), suite exit 0;
                   m26_merge_contexts_share_one_memory_pool verified green
openobserve-core:  1929 passed / 0 failed (17 ignored)  (--lib)
infra:             1085 passed / 0 failed (36 ignored)
                   (first parallel run flaked ONCE on
                   wal_segments::test_create_table_migrates_missing_l0_planned_column —
                   an ordering flake on the shared sqlite: passes standalone
                   and on the full rerun; the fix does not touch infra)
openobserve-jobs:  38 passed / 0 failed
```
