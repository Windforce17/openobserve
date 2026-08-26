# M27 — sampling heap profiler: direct allocation-site attribution for the
# prod compactor live-heap floor, shipped inert inside the normal image

Worktree pinned at `a0442f00fc` (post-.117 lineage). 20C/38G box, release
builds (`-j8`). Local commits (never pushed): `a88a98f235` profiler, then
this note. Everything ships in the normal image; prod activation is one env
pin on a canary Deployment.

## Why an instrument, not another fix

Post-M26 the compactors still accumulate a LIVE anonymous-heap floor at
~35-50 MB/s until OOM at 48Gi (~20 min pod lives). Everything inferable is
exhausted: job machinery harness-proven leak-free (M26), per-context DF
pools fixed (M26 shared pool), allocator retention falsified live
(MIMALLOC_PURGE_DELAY=0), engine caches acquitted by gauges, heap content
~99% binary buffers. The remaining move is measuring allocation sites on a
real prod pod. M27 is that instrument.

## Activation contract

- `ZO_HEAP_PROFILE_SAMPLE_EVERY_MB` unset / `0` / unparseable → compiled in
  but FULLY INERT. The wrapper adds exactly one relaxed atomic load + branch
  per alloc and per dealloc; mimalloc handles every byte, untouched.
- `ZO_HEAP_PROFILE_SAMPLE_EVERY_MB=64` (recommended prod value) → sampling
  active from process start (`config::heap_profile::init()` is the first
  statement of `main`; also wired into the M26 harness example).
- `ZO_HEAP_PROFILE_REPORT_SECS` (default 60) → report cadence.
- Reports go through `log::info!` target `config::heap_profile` (bridged to
  the tracing subscriber like every other engine log line), one line per
  stack:
  `heap-profile: rank=N live_est=XMB count=K avg_sz_kb=A stack=<f1;f2;...f8>`
  plus a header with totals
  (`alloc_flow`, `live_est_total`, `samples_live/taken`, `reentrant_skips`).
- Activation is also printed to stderr (it precedes the log subscriber).

## Design (src/config/src/heap_profile.rs, one module, no new deps beyond
## the `backtrace` crate already in the lock)

- `HeapProfileAlloc<A: GlobalAlloc>` wraps the existing
  `#[global_allocator]` site in `src/main.rs` (mimalloc and jemalloc
  variants both wrapped; generic, const-constructed).
- Sampling math: every allocation adds its size to one global atomic
  accumulator; an allocation that makes the accumulator cross k multiples
  of the interval N is sampled with weight `k*N`. Expected crossings for a
  size-S allocation = S/N, so per-stack `live_est = Σ weights of live
  samples` is an unbiased live-byte estimator for sizes both below and
  above N (a 640 MB buffer crosses ~10 intervals and carries ~its own size
  as weight; 1 KB allocations from a hot site get sampled every ~64K-th
  call and each stands for 64 MB of them).
- Live tracking: sampled `ptr -> (64-frame FNV stack hash, weight, size)`
  in a 64-shard `Mutex<HashMap>`; the record is removed when that pointer
  is freed, BEFORE the inner free (a recycled address can never
  double-attribute). `realloc` = free(old)+alloc(new) for attribution.
- Dealloc pre-filter: 2^20-bit always-set-only bitmap (128 KiB static)
  keyed by mixed pointer value. A never-sampled free costs one relaxed
  load + mask; only real samples and rare aliases take a shard lock.
  mimalloc's address reuse bounds distinct sampled addresses, so the
  filter does not saturate on long-lived processes; a saturated filter
  degrades to map lookups, never to wrong answers.
- Re-entrancy: a thread-local flag guards everything past the hot-path
  checks. Profiler-internal allocations (map growth, backtrace machinery,
  symbolization, log formatting) pass straight through and are never
  sampled — lock recursion is structurally impossible. Checked only on
  the cold paths (a sampling crossing or a filter hit), so it costs
  nothing on the hot path. Cold paths are additionally wrapped in
  `catch_unwind` (an unwind out of a GlobalAlloc impl is UB).
  FOUND-BY-PROBE hardening: the first cut built the guard with
  `entered.then_some(ReentrancyGuard)` — `then_some` constructs its
  argument EAGERLY, so on the blocked path the never-issued guard was
  dropped and its `Drop` cleared the flag the outer guard still owned;
  the profiler then sampled its own gimli symbolization cache
  (`<backtrace::...::elf::Object>::parse` ranked #1) and re-opened the
  shard-lock-recursion window. Fixed with an explicit branch; regression
  test pins that a blocked nested attempt leaves the outer guard armed.
- Stacks: raw IPs captured via `backtrace::trace` (the engine under
  `std::backtrace`) — capture is cheap and allocation-free; symbol
  resolution is lazy, only in the 60s reporter thread, with
  profiler/alloc-shim frames filtered from the printed 8.
- The stack registry keeps aggregates per distinct stack (live/total
  weight, counts); dead entries are evicted when the registry exceeds 32k
  stacks.

## Release-profile change (required for meaningful frames)

`[profile.release] strip = true` → `strip = "debuginfo"`: keeps the ELF
symbol table, still drops DWARF. `release-prod` (the image profile)
inherits it. Verified on the built binary:

```
nm m26_job_leak.m27 | wc -l                        -> 514,577 symbols
nm -C ... | grep -cE "datafusion|tokio|vortex|..." -> 94,934 meaningful
```

Size cost measured on the harness binary: 211.9 MB vs 147.5 MB fully
stripped = +61.4 MB (+43.6%), all symtab/strtab (no DWARF). No debug=1
needed — function names resolve without line tables, so the cheap tier is
enough.

## Local proof (M26 job harness, prod killer shape: trace_list_index
## parquet merges through merge_parquet_files, stats-free full-sort arm)

All runs: the REAL compact job loop (claim → heartbeat → merge →
commit → broadcast), `ZO_HEAP_PROFILE_SAMPLE_EVERY_MB=64` unless noted.

1. Contract cadence — 1024 jobs, 91.6 s wall, DEFAULT 60 s cadence:
   report#1 fires on schedule; sampling spacing is exact
   (286.9 GB flow / 64 MB = 4589 = samples_taken):

```
heap-profile: ACTIVE sample_every=64MB report_every=60s
heap-profile: report#1 sample_every=64MB alloc_flow=286.9GB live_est_total=0.0MB samples_live=0 samples_taken=4589 reentrant_skips=0
```

   `live_est_total=0` is the CORRECT reading for this harness: M26 made it
   leak-free, and post-M20b/M26 the merge arm streams/spills — 9.5 M rows/s
   churn with a ~240 MB resident working set whose buffers live
   milliseconds. A profiler tracking LIVE bytes reports ~nothing held; the
   prod floor (bytes that STAY) is exactly what it will rank.

2. Live attribution on genuinely HELD memory (64 MB sampling, seed phase
   building 2 M-row arrays): estimator honest and site-precise — it
   separates the ~45.8 MiB row vectors from the hex trace-id format!
   strings built at the same call site:

```
heap-profile: report#1 sample_every=64MB alloc_flow=34.8GB live_est_total=256.0MB samples_live=4 samples_taken=557 reentrant_skips=0
heap-profile: rank=1 live_est=64.0MB count=1 avg_sz_kb=46875.0 stack=<m26_job_leak::cmd_seed_tli::{closure#0};<tokio::runtime::park::CachedParkThread>::block_on::<...>;...>
heap-profile: rank=3 live_est=64.0MB count=1 avg_sz_kb=0.0 stack=<<alloc::string::String as core::fmt::Write>::write_str;<u64 as core::fmt::LowerHex>::fmt;core::fmt::write;alloc::fmt::format::format_inner;m26_job_leak::cmd_seed_tli::...>
```

3. Mid-merge engine stacks (8 MB sampling to resolve the ms-lived
   working set, 768 jobs, reports every 5 s): `live_est_total` tracks the
   in-flight set (100-176 MB vs ~240 MB RSS), and the ranked frames are
   the real merge pipeline — arrow take kernel under the DataFusion sort,
   parquet output spool attributed THROUGH the engine call site
   (`openobserve_core::compact::merge::merge_files`), parquet page
   buffers:

```
heap-profile: report#5 sample_every=8MB alloc_flow=142.5GB live_est_total=176.0MB samples_live=22 samples_taken=18178 reentrant_skips=3
heap-profile: rank=1 live_est=72.0MB count=9 avg_sz_kb=131.5 stack=<arrow_select::take::take_bytes::<arrow_array::types::GenericStringType<i32>, ...>;arrow_select::take::take_impl::<...>;arrow_select::take::take;...>
heap-profile: rank=3 live_est=16.0MB count=2 avg_sz_kb=9508.2 stack=<<alloc::vec::Vec<u8> as tokio::io::async_write::AsyncWrite>::poll_write;<&mut alloc::vec::Vec<u8> as parquet::arrow::async_writer::AsyncFileWriter>::write::{closure#0};<parquet::arrow::async_writer::AsyncArrowWriter<&mut alloc::vec::Vec<u8>>>::do_write::{closure#0};...;openobserve_core::compact::merge::merge_files::{closure#0}::{closure#7};<tokio::runtime::task::harness...>
heap-profile: rank=5 live_est=8.0MB count=1 avg_sz_kb=864.0 stack=<<parquet::column::writer::GenericColumnWriter<parquet::arrow::arrow_writer::byte_array::ByteArrayEncoder>>::add_data_pag…;...;<parquet::arrow::arrow_writer::ArrowRowGroupWriter>::write;...>
```

The probe run of case 3 is also what FOUND the `then_some` guard bug
(see design section): pre-fix the profiler ranked its own gimli
symbolization cache; post-fix `reentrant_skips` counts those crossings
instead and no profiler frame appears in any report.

## Overhead (median of 3, fresh seed per round, 768 jobs x 24 files x
## 20k rows, ~370M rows merged per round, quiet box)

Wall = t of the last completed-job observation (excludes the constant
drain/settle tail). Two full matrices were run — one against the
pre-hardening binary, one against the shipped binary (identical hot
paths; the fix touched only cold/report code):

```
arm                       medians        all rounds
base   (a0442, untouched) 49.7s          49.7 49.9 49.1
off    (M27, env unset)   50.1s / 51.1s  49.7 50.1 50.5 / 51.1 49.6 51.1
active (M27, env=64MB,    50.1s / 49.1s  50.2 49.6 50.1 / 48.9 49.1 49.4
        3 reports/run)
```

- OFF: +0.4 s (+0.8%) on the primary matrix; the re-run matrix read
  +1.4 s but its ACTIVE arm simultaneously read FASTER than base
  (-0.6 s), which a strictly-more-work configuration cannot be — i.e.
  run-to-run noise is ~±1.5 s (±3%) and the off-delta is inside it.
  The off hot path is one relaxed atomic load + branch per
  alloc/dealloc; every measured read is consistent with <1%.
- ACTIVE at 64 MB sampling: indistinguishable from off in both matrices
  (deltas 0.0 s and -2.0 s), FAR under the 10% budget, including the
  in-run report/symbolization cost. Sampling worked the whole time
  (4,000-6,600 samples per run; flow/64 MB spacing exact to <1%).
- Sampling cost scales with allocation flow, not wall: this workload
  churns ~6 GB/s => ~100 samples/s at 64 MB, each a raw-IP capture
  (no symbolization on the hot path).

## Suites (final runs on a quiet box, full unpiped output, counts not
## exit codes)

- `config`: lib 1981 passed / 0 failed / 3 ignored; doctests 3/0.
  Includes 4 new heap_profile tests: crossing math (size-weighted,
  split-invariant), filter no-false-negatives, re-entrancy guard
  (incl. the blocked-attempt-must-not-disarm regression), and a full
  sample lifecycle (live tracking, weight estimates, recycled-address
  overwrite, report rendering).
- `openobserve-core --lib`: 1929 passed / 0 failed / 17 ignored.
- FLAKE DISCLOSURE: the FIRST core --lib run of the day reported
  1928/1 — and my runner piped it through `tail`, which both masked the
  exit code and discarded the failing test's name (runner error,
  unrecoverable). That run executed CONCURRENTLY with a merge workload
  on the same box (prime flake conditions). The M27 diff cannot reach
  core --lib structurally: the wrapper is installed only in
  `src/main.rs` and the two examples — core's test binary uses the
  system allocator and never calls `init()`, so profiler code is
  unreachable there. Two subsequent full runs on a quiet box: 1929/0
  both times.

## Prod rollout

- Ships inert in the next payload; no behavior change without the env.
- Canary: pin `ZO_HEAP_PROFILE_SAMPLE_EVERY_MB=64` on ONE compactor
  Deployment (env only, no image change beyond the payload), grep
  `heap-profile:` in its logs after 60s. At the observed ~2 GB/s
  allocation flow that is ~30 samples/s — negligible; the floor climbs
  ~35-50 MB/s so within minutes the top ranks separate the climber from
  transient merge traffic (climber: `live_est` monotonically rising,
  stable stack; transient: churning ranks).
- Revert = drop the env pin. The profiler cannot be re-activated at
  runtime (read once at process start) — restart semantics only.
