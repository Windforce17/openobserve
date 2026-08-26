# M30 — headroom-gated rebuild admission (the gate contract's owed re-measure, closed the other way)

Date: 2026-08-26. Engine change: `RebuildGate` in `core/src/vix/core_writer.rs`
+ `ZO_VIX_REBUILD_HEADROOM_MB` (config.rs). Ships as .121.

## The prod problem (measured 2026-08-26 ~05:1xZ)

M29 (.120) fixed job flow, and its note predicted the handoff: "job flow is
no longer the limiter, the gate is." Two days later that is exactly prod:

- 381/381 merges on the busiest pod's 2h window took the REBUILD path
  (fast-path count: ZERO — the L0-index-off population guarantees it).
- The single gate slot (`ZO_VIX_REBUILD_CONCURRENCY=1`) is 100% saturated:
  ~190 rebuilds/h/pod at ~19s effective slot hold; every merge queues a
  MEAN 168s (min 118s, max 334s) behind it while its real work is seconds.
- Fleet ceiling ≈ 1,600 merges/h ≈ 23.4k input files/h consumed. Arrivals
  are comparable — so the recent-hour file population never drains:
  file_list on 2026-08-25 held 274,977 live traces/default files (54.9TB,
  L0 ~316MB avg never reaching the 1GB target) and 132,759 logs/default;
  hours 17h old still carried ~8.4k unmerged L0 files each; hour partitions
  only converge to single-digit file counts ~3 days out. Query planning over
  10-15k files/hour is the user-visible "large number of files = slow query".
- Meanwhile the pods' SAMPLED PROCESS RSS breathes 1.35-18.15GB (90 samples,
  2s cadence, busiest pod; `kubectl top`'s 22-40GB is page cache from the
  110G disk cache, not heap). The 48Gi limit leaves 30-45GB of REAL headroom
  that the count pin cannot see.

## The mechanism

The M12 count gate stays (its own comment records why a
`original_size × factor` byte estimate was rejected: 5-10x per-stream
transit variance). What changes: the count is now the hard CAP, and every
slot beyond the guaranteed first requires LIVE headroom — sampled process
RSS (`NODE_MEMORY_USAGE`, the same 1s gauge the ingest breaker consults)
plus `ZO_VIX_REBUILD_HEADROOM_MB` (default 5120, the M29 contract's
~5GB/rebuild arithmetic at max-file 1024) charged for EVERY extra rebuild in
flight, candidate included (the ingest-admission burst lesson: an admitted
rebuild's transit lags in the sampled gauge; double-charging while it
materializes errs conservative), must stay under 90% of the memory limit.
Waiters re-check on a 500ms tick — headroom opens with no gate event.

Properties:
- A pod with no headroom (fat build wave, allocator floor, whatever future
  regression) sits at 1 slot — the exact pinned regime that held kills=0.
- A breathing pod (the measured prod shape) runs the cap. Prod arithmetic:
  envelope 46.4GB (90% of 51.5GB), worst sampled peak 18.2GB + 3 extras
  x 5GB = 33.2GB — cap 4 admits through the observed worst case and
  throttles itself during genuine RSS spikes.
- `ZO_VIX_REBUILD_HEADROOM_MB=0` restores the EXACT M12 count-only gate.
- First slot never consults memory: progress guaranteed, no deadlock shape
  (permit is RAII, drops on unwind; timed cv wait, no lost wakeup).

## Expected effect

Gate ~19s/merge slot hold x 4 slots ≈ 750 merges/h/pod ≈ 7.5k/h fleet
(~92k input files/h at the observed 12.3 files/merge) vs ~25-35k/h arrivals
— recent hours drain in hours, not days. CPU: 4 rebuilds x 3 pinned merge
threads = 12 = the compactor CPU limit; throttling stretches slot hold
slightly, which only lowers the multiplier toward ~3x. Unit tests:
`rebuild_gate_tests` (5) cover count-only mode, the per-extra charge
arithmetic, saturation, zero-envelope progress, and block/release.

## Prod/dev config delta (ships with the .121 image bump)

- compactor env: `ZO_VIX_REBUILD_CONCURRENCY` 1 -> 4 (= the auto value at
  ZO_FILE_MERGE_THREAD_NUM=8; now a CAP, not a blind width).
- `ZO_VIX_REBUILD_HEADROOM_MB` unpinned (engine default 5120).

## Brakes / rollback (config-only, no image move)

1. First brake: `ZO_VIX_REBUILD_CONCURRENCY=1` — restores today's prod
   regime exactly (the headroom gate never binds at cap 1).
2. `ZO_VIX_REBUILD_HEADROOM_MB=0` — count-only M12 behavior at whatever cap.
3. Image rollback .121 -> .120 is safe (no on-disk format change), but is
   never needed for gate behavior — both knobs are read by .121 code.

## Watch on the roll (first hours)

- `rebuild admitted after ...s wait (N/4 slots busy)` — expect N to sit at
  3-4 and MEAN wait to collapse from ~168s; the line only prints at >50ms.
- RSS: `zo_node_memory_usage` on compactors — expect breathing to widen
  toward ~15-25GB peaks; envelope trips would show as N falling back to 1-2
  during spikes (that is the mechanism working, not a fault).
- Files: per-hour live file counts on default/traces/default for today's
  dates falling from ~9.5k toward hundreds within hours; pending jobs
  (zo_compact_pending_jobs) trending to a fed-but-draining plateau.
- OOM watch unchanged: any compactor kill -> brake 1 (concurrency=1) first.
