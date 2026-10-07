# obs engine — current state + backlog (single source of truth)

Supersedes NARROW-WAL-PLAN.md, FIELD-MAJOR-PLAN.md, DURATION-RANGE-PLAN.md
(deleted 2026-07-29; full history in git). Keep THIS file current.

## 2026-09-30 — `.180` compactor: one merge target, files above half of it final, settled-based sealing; the per-class targets and the index-defer knob are gone
- Implements the 05:00–08:00Z owner proposal ("one config, merge once")
  below. vix-arch `b5046389b`, image `v0.93.0-vix-20260930.180`
  (binary `310d4cc0…`, OCI index `bfbfe707…`), GitOps #576 (`51b8d9be`),
  compactors 30/30 at 09:27–09:28Z, 0 restarts, `obs-env-rev
  2026-09-30-one-merge-target-4096`. Other roles untouched (the ingester's
  only reader of `ZO_COMPACT_MAX_FILE_SIZE` takes `min(512 MB on-disk cut,
  it)`).
- Engine (`compact/merge.rs`, `vix/core_writer.rs`, `config.rs`,
  `file_list/{postgres,sqlite}.rs`, `jobs/compactor.rs`):
  - ONE byte target for every file class: `ZO_COMPACT_MAX_FILE_SIZE`
    (prod 1024 → **4096**). Deleted `ZO_COMPACT_LOGS_INDEXED_MAX_FILE_SIZE`,
    `ZO_COMPACT_TRACES_INDEXED_MAX_FILE_SIZE`, `Compact::
    max_file_size_for_merge`, the two-target `query_for_merge` SQL and the
    per-class inputs of the auto merge-concurrency resolver (startup line
    on `.180`: `target_mib=4096 memory_slots=10 cpu_slots=3 total=3
    live_workers=2 backlog_workers=1` — unchanged capacity, the old
    formula already took `max(1024, 4096, 4096)`).
  - Files above HALF the target (2 GiB) are FINAL: `plan_partition` never
    groups them (the debt line `ZO_COMPACT_OLD_DATA_MIN_FILES` over files
    ≤ target/2 already treated them as done), so a byte is rewritten and
    indexed at most once. Index-less final files are healed in place
    (sidecar-only) the round they are seen — open hour included; that was
    the 75–130 min "2 h lag" of logs L0s (p50 2.5 GiB).
  - Three ages of an hour replace `is_incremental = !is_past_hour`: OPEN
    and CLOSED-BUT-UNSETTLED (until close + `ZO_SEGMENT_LATE_LANE_HOURS`
    = 2 h) seal only full groups and carry the remainder — a below-target
    remainder seals early only once it holds `MIN_PARTIAL_FAN_IN` = 8
    files (tiny late slices coalesce; two 1.5 GB files wait); SETTLED runs
    one sweep-up. A partition's lone sub-half core file is still probed
    from the first closed pass. Indexed final files are probed on settled
    rounds only (defective-file guard).
  - `ZO_VIX_MERGE_INDEX_DEFER_BELOW_MB` / `CoreMergeMode::IndexDeferred`
    removed (deferred outputs were index-less merged files needing a second
    heal). Only a healing single file above the target hits the
    indexed-only guard now.
  - Tests: `plan_partition` unit tests (finality, sidecar-homogeneous
    groups, fan-in floor, lone-file probe), grouping tests rewritten for
    settled/unsettled, resolver/config/file_list tests collapsed to one
    target. `compact::merge::tests` 52/52.
- Baseline `.178`, 08:50–09:20Z (kubectl logs, all 30 pods): traces
  **1,219 merges / 60 min, 970 two-input, output p50 2.88 GB, 3.14 TB
  rewritten, all passthrough**; logs 459 merges (425 two-input, p50 2.38
  GB, 1.19 TB, 391 passthrough / 68 rebuild) + 398 heals.
- First 15 min on `.180`: the open logs/default hour 09 was planned by one
  pod — 198 index-less final L0s probed (`file carries no index sidecar…`,
  ~1 s per probe) and 11 full groups of 2–5 sub-half L0s → 3.6–4.1 GB
  rebuild outputs (`index_merge: false`, ~45 s each), all draining through
  that pod's `live_workers=2` (shared with the recent lane). The hour's
  throughput ceiling is one pod's live workers — the "one job per (stream,
  hour) on one node" bound named on 09-30; the next lever is per-hour
  parallelism, not the sealing rule.
- **Clean-hour window 10:09–11:09Z (hour 10 is the first fully-`.180`
  hour), kubectl logs of all 30 pods, attribution by output hour:**
  - traces live hours: **712 merges** (hour 10: 428, avg 2.6 inputs — the
    L0s are ~1.5 GB, so 2–3 fill a 4 GiB group — **1 sub-half output**, 1.46
    TB rewritten against ~1.7 TB landed = **one generation**; `.178`
    rewrote 3.14 TB/h, ≈1.75 generations). Hours 08/09 tails: 41 sub-half
    outputs of 10–4 inputs at ~0 TB — the fan-in floor coalescing tiny
    stragglers, as intended. Plus 697 two-input merges (0.23 TB) from the
    one-time settled sweep-up of old hours' leftovers (finite historical
    debt; each old hour visited once).
  - logs live hours: 135 merges (hour 10: 81, avg 3.7 inputs, 0 sub-half,
    0.29 TB) + **955 heals/h** (p50 6.1 s, sidecar-only) — 0.39 TB rewritten
    vs `.178`'s 1.19 TB/h: the finals are indexed in place instead of
    re-paired.
  - Lag (file_list at 11:09Z): logs hour 10 at close+9 min **149 index-less
    of 606 files** (`.178` at close+14 min: 61 % of rows); hour 09 at
    close+69 min 12 of 556; hour 08 0. Traces hour 09 at close+69 min: 503
    files (15 small, 1 mid, 487 final) — the settled shape (~480) reached
    within an hour of close; hour 10 at close+9 min 578 (49 small, 50 mid).
  - Fleet: RSS p50 5.7 GiB / max 15.3 GiB, 0 restarts, 0 OOMKilled, no merge
    failures or refusals; ERROR lines were the querier restart's cluster
    health-check churn and 7 S3 GET retries. `file_list_jobs` running
    75/90, pending 671 (the sweep-up enqueued old hours).
- Owner asked at 10:15Z whether queries got slower and restarted the
  queriers (10:18Z; RSS had been 18.5–19.1 GiB per pod since `.179`, 8–14
  GiB after). Battery evidence, obs r1/r2 vs O2, `use_cache=false`:
  - during the restart (10:15–10:18Z): 57.8 s / 33 s / 15 s obs failures
    (`error decoding response body`) — the roll itself;
  - 5 min after (cold): traces count 1 h 5.2 / 5.6 s, all `idx_took`
    (sidecars refetched into empty caches), hist 7.4 → 0.9 s;
  - **30 min after (warm, 10:48Z, window 09:35–10:35Z): traces count 1 h
    1,459 / 430 ms (O2 787), hist 794 / 357 (O2 8,741), top-10 15 m 2,088 /
    674 (O2 1,186), logs count 1 h 1,005 / 396 (O2 289), logs count svc
    1,477 / 511 (O2 114), logs `SELECT * LIMIT 50` 2,493 / 1,141 (O2
    1,117)** — at or better than the 09-28 `.173` references (531 / 664 /
    446 / 2,315 ms). No `.180` query regression measured. Pre-restart
    querier logs 09:00–10:17Z: no `MemoryCircuitBreaker`, no growth
    timeouts; ERRORs were user SQL field errors plus 4 `.vxi NotFound`
    (file merged away between file_list read and sidecar fetch,
    fail-open). Node churn the same morning: NATS pods `Drifted` at 09:33Z
    and 10:20Z, two compactors + one querier replaced ~09:52Z (Karpenter).
  - What `.180` does change for queries: closed-but-unsettled hours hold
    more files until their full groups form (traces hour 09 at close+75 min
    had 879 files vs ~480 settled; at close+69 min the next hour had 503).
    Index-heavy shapes over the last 1–2 h pay linearly for that; the
    warm battery did not show it above noise.
- **24 h / 7 d battery 11:34–11:41Z (window end 11:20Z, queriers 76 min
  old, `use_cache=false`, obs r1/r2 vs O2, ms; every answer
  `partial=false`, row counts within 1.2 % of O2 = corpus difference):**
  - 24 h: traces count 1,272/507 vs 1,530; hist 1 h 804/448 vs 6,889; top-10
    services 3,907/1,069 vs 9,732; `approx_percentile_cont(duration,0.99)`
    by service 9,164/8,913 vs 21,199 (scan-bound, 8.8 B rows, now complete
    thanks to `.179`); logs count 1,454/744 vs 4,875; **logs count
    `service_name='llm-router'` 9,606/913 vs 2,352**; logs `SELECT * LIMIT
    50` 1,165/292 vs 2,570.
  - 7 d: traces count 2,415/892 vs 31,510; hist 6 h 1,076/543 vs 33,420;
    **top-10 services 51,895/1,633 vs 24,034**; logs count 1,313/608 vs
    30,460; **logs count svc 32,962/1,258 vs 38,933**; logs `SELECT * LIMIT
    50` 1,851/929 vs 7,959.
  - Attribution of the three cold outliers (follower logs, per follower):
    traces top-10 7 d — 6,920 index files, **60,207 index fetches (5.26
    GB) in 49.5 s**, `follower search setup` 49.8 s, scan 0.7 s; logs svc
    7 d — 4,726 files, 29,471 fetches (2.12 GB), 26.9 s; logs svc 24 h —
    ~1,200 files, ~14,000 fetches (750 MB), 8.0 s. r2 of the same query:
    33 / 2 / ~200 fetches → 1.1 s / 0.27 s / 0.23 s (the per-file
    evaluation cache, not the disk cache: `disk cached` was 45 % / 20 %
    both times). Arithmetic: `eval_concurrency` 64 files in parallel
    (`ZO_VIX_SEARCH_CONCURRENCY`), ~8.7 sequential range reads per file
    (footer → dictionary → postings/zone counts, 87 KB avg), ≈ 53 ms per
    remote read → 6,920 × 8.7 × 53 ms / 64 ≈ 50 s. Cold GROUP BY / filtered
    aggregates cost **≈ 7 ms per index file per follower**, linear in file
    count; the global fetch gate (`ZO_VIX_FETCH_CONCURRENCY` 256) is 75 %
    idle during it (64 files × 1 read in flight). Levers, cheapest first:
    (1) `ZO_VIX_SEARCH_CONCURRENCY` 64 → 192–256 for remote-cold
    evaluation (the gate still caps S3 in-flight at 256; the code's
    "request storm" guard was written for 5 followers at 16 in-flight);
    (2) coalesce the per-file read chain into one or two ranged reads
    (sidecar footer already names the dictionary/posting offsets); (3)
    persist the evaluation cache across restarts. None of this is `.180`.
- **Lever (1) tried and reverted, 11:58–12:14Z (GitOps #577 → #578):
  `ZO_VIX_SEARCH_CONCURRENCY` 64 → 192 did not raise cold evaluation
  throughput.** Cold-vs-cold on the same 7 d window (the roll wiped the
  ephemeral disk cache, so "after" had 0 % sidecars cached vs 45 % before):
  filtered traces count 42.5 s → 59.6–61.8 s, top-10 services 51.9 → 70.7 s.
  The `io_accounting` line explains it: 49k remote range reads, active IO
  4,518–5,415 s (**75–102 ms per read vs ~53 ms at 64**), and
  **evaluation admission wait 5,501–6,675 s** — each eval declares 32 MiB
  (`evaluation_working_bytes`: fixed workspace + bitmaps; the clamp is 0
  for fully covered files), so the 4 GiB `ZO_VIX_EVAL_MAX_BYTES` admits
  only ~120 of the 192 and the rest queue; per-pod reads/s stayed ~1,000
  either way; CPU 3.7 of 16 cores. Side effects: 31 growth timeouts =
  31 `budget_refused` fallbacks (full scans) on one pod in 12 min (0 in 3
  days at 64 on `.173`). Raising the eval budget to fit 192 would add ~4 GiB
  transient on pods that reach 19 GiB RSS of a 24 GiB limit — not taken.
  Conclusion: the cold cost is **sequential round trips per file × S3
  latency**; fan-out cannot buy it back. Read-chain map (scout, verified
  file:line): filtered count = `.vix` tail 64 KiB → `.vxi` tail 256 KiB →
  [dict field index, 2 depths, only when `dict` is not tail-resident] →
  predecessor key block → terms-blob Vortex footer 65,535 B → `doc_count`
  leaf = **5 sequential round trips**; grouped top-N adds the key-terms
  block, the key ordinal's leaf, the field boundary blocks and the field's
  leaf ranges = **~7**. Sidecar order today `[terms][plist][dict_blocks]
  [bloom][dict][footer]`: the Vortex footer sits at the END of the FIRST
  blob, unreachable by any tail over-fetch. Building (`.181`, in progress):
  parallel tails; writer reorder `[…][terms][dict][footer]` so the Vortex
  footer rides in the 256 KiB tail (readers locate blobs by tag — no format
  change, new files only); a field-scoped prefetch bundle (Vortex footer
  when not resident + the field's contiguous key-block run ≤ 1 MiB + the
  key-terms block + `doc_count` leaves ≤ 256 KiB when the footer is
  resident, handed to the Vortex scan via a pre-seeded segment cache);
  `dict` field-index probes folded into one batch. Targets: count 5 → 3
  round trips (2 on new-layout files), top-N 7 → 3 (2).
- **`.181` shipped (vix-arch `244a05746`, image `ceebcf8c…`): queriers
  13:26–13:27Z (GitOps #579), compactors 13:38Z (#580), 0 restarts.**
  Reader side (every existing file): parallel `.vix`/`.vxi` tails, the
  field-scoped prefetch bundle (key-block run ≤ 1 MiB, key-terms block,
  terms Vortex footer when not resident, `doc_count` leaves ≤ 256 KiB
  served through an operation-scoped overlay `PrefetchedWindows` — same
  thread-local pattern as `EXACT_RANGES`), `field_index` probes in one
  wave. Writer side (compactor only for now — merge outputs + logs L0
  heals are nearly every long-lived sidecar; ingester L0 sidecars follow
  with the next converged release): blob order `[plist?][dict_blocks]
  [bloom?][terms][dict][footer]`, terms footer inside the 256 KiB tail.
  Tests: depth 3/2 (legacy/new) for both shapes, parity, fetch counts never
  grow; vortex_index 351, search vix 189, core_writer+bloom 102, segments
  37 green. Known cost: a bloom small enough to have sat in the tail pays
  its 4 KiB header read on cold needle probes (traces blooms are MBs —
  unaffected).
  - **Cold-vs-cold A/B, both at 0 % disk cache (the rolls wipe the
    ephemeral cache), `ZO_VIX_SEARCH_CONCURRENCY` 64, same 7 d window:**
    traces count `service_name='nexus-service'` **85.0 → 59.9 s (−30 %)**;
    traces top-10 `span_kind` **90.8 → 55.5 s (−39 %)**; logs count svc
    **51.6 → 39.4 s (−24 %)**. All `partial=false`, identical counts.
  - What the `io_accounting` line shows now (per follower, traces count):
    47,715 remote reads (was 63,608), active IO 4,874 s = **~100 ms per
    read** (baseline 82 ms at 63k reads); gate/admission waits ≈ 0. Reads
    per file 7.0 (count) / 9.5 (top-N — the leaf prefetch adds concurrent
    reads while removing depth). The label `round_trips` I added counts
    `fetch_many` calls, which the new waves issue concurrently — renamed
    `fetch_batches` (`6ac31a866`); sequential depth is not directly
    observable in prod yet.
  - **Per-read S3 latency is the remaining wall.** Raw range GETs of 87 KB
    from the ops host (same region, boto3, 256-connection pool): 16-parallel
    p50 27 ms / 449 req/s; 64-parallel p50 53 ms, p90 91 ms / 579 req/s —
    latency scales with per-host parallelism and per-host throughput
    saturates. Queriers show the same shape: ~1,000 reads/s per pod at
    80–100 in flight and 70–104 ms per read, whether 64 or 192 files are
    evaluated. So after `.181` the cold cost is **reads × ~1 ms
    (per-pod request-rate ceiling)**, not depth: 47k reads ≈ 50 s. Levers
    in order: (1) fewer reads per file — inline `doc_count` in the
    dictionary blocks (drops the terms footer + leaf reads: 7 → ~3–4 per
    file), a bigger eager tail for the new layout (1 MiB swallows `dict` +
    terms footer + the tail of `dict_blocks` for small fields: −1–2 reads
    at +5 GB per 7 d query), whole-sidecar caching for hot streams; (2)
    find the per-host ceiling — S3 pins a client to one front-end IP per
    resolved address; spreading connections across resolved IPs or a
    per-AZ endpoint is the usual fix (unmeasured here); (3) persistent
    evaluation/reader caches across restarts so 7 d windows are not cold
    after every roll (today's three restarts each wiped 45 % → 0 %).
- **`.182` shipped (vix-arch `518f7f42f`, GitOps #581, queriers 15:40–15:42Z,
  0 restarts) with `ZO_VIX_SEARCH_CONCURRENCY` 64 → 192.** Owner ruled out
  boot warm-up and `doc_count` inlining; the remaining lever was making
  fan-out effective. Root cause of the 11:58Z failure confirmed in code:
  `evaluation_working_bytes` pre-reserved a flat 32 MiB workspace per
  evaluation (plus bitmaps/clamp), so the 4 GiB gate (3.5 GiB after the
  budget/8 growth headroom) admitted ~112. `.182` declares the measured
  footprint for admission-time-provable index-only, fully-covered shapes
  (`SimpleCount` except the chunk-stats numeric route; condition-all
  single-field `SimpleTopN`/`SimpleDistinct`; single-bucket IN
  `SimpleMultiHistogram`): 2 × (tails 320 KiB + prefetch caps 1.25 MiB +
  terms footer 64 KiB + native session 64 KiB) + bitmaps + 256 B × 1,001
  group entries = **4.11 MiB count / 4.59 MiB top-N at 770k rows → ~800
  admitted**; every other shape keeps the old declaration. Measured owned
  peaks ride on top unchanged (1.93 MiB exact count, 5.5 MiB top-N on the
  100k-row fixture); each eligible shape completes under a private gate of
  exactly declaration + owned with no growth wait. `search vix` 194 tests.
  - **Cold-vs-cold A/B (after-state colder: fresh pods, 0 % caches), same
    7 d window, `.181`@64 → `.182`@192:** traces count `service_name=
    'e2b-api'` **41.6 → 16.9 s (−59 %)**; traces top-10 `span_status`
    **54.7 → 20.7 s (−62 %)**; logs count svc **30.0 → 12.7 s (−58 %)**.
    Versus this morning's `.179` numbers (85.0 / 90.8 / 51.6 s): **−80 %**.
    `evaluation_wait_us` 5,500 s → ~1 s per query; active S3 latency
    unchanged at ~96 ms/read despite 2.5× the in-flight reads; CPU ~3
    cores of 16; no growth timeouts, no `budget_refused`.
  - New binding limit: the 256-wide `ZO_VIX_FETCH_CONCURRENCY` gate —
    `queue_us` 690 / 859 / 312 s per query (≈ 19 ms per read on top of
    96 ms active). Raise to 512 with the next querier roll (`.183`), not
    alone — each roll costs a cold cache.
  - Lazy data-container open (skip the `.vix` 64 KiB tail for index-only
    evaluations, −1 read/file) assessed and deferred: `from_containers`
    consumes row_count/row_group_size/zone_map/row_order/row_regions/
    columns/stats at construction — a deep reader refactor for ~15 % of
    reads at zero depth (the tail is fetched in parallel with the sidecar
    tail). The metadata tier (`.183`) keeps that tail, so repeated queries
    get the saving anyway.
- **Owner (16:0xZ): "concurrent queries seem to hang — can you reproduce?"
  Reproduced, attributed, fixed in `.183` (vix-arch `261251626`, GitOps
  #582, queriers 18:40–18:42Z with `ZO_VIX_FETCH_CONCURRENCY` 256 → 512).**
  No real user traffic reaches `obs-router` (17 h of router access logs
  hold only the batteries; Orbit is served by the O2 cluster), so the repro
  is synthetic: 11 dashboard-shaped 24 h queries (filtered counts,
  filtered histograms, top-N) fired at once, plus one small 1 h query every
  2 s (`/tmp/burst_small.py` on ops).
  - **Before (`.182`): small queries 0.5 s solo → 7.8 / 15.4 / 20.6 /
    23.9 s during the burst (15–48×), all inside `idx_took`; repeat run:
    avg 2.1 s, max 13.2 s.** Two mechanisms, both invisible to
    `took_detail` (`wait_in_queue` = 0):
    1. **Per-file serialization across queries.** `reader_cache::
       GLOBAL_CACHE` guarded each cached reader with an exclusive
       `tokio::sync::Mutex` taken BEFORE evaluation admission
       (`ReaderHandle::lock`, reader_cache.rs:91–104): every query
       evaluating the same recent file waited for the previous query's
       evaluation of that file. A 1 h histogram's follower spent 13.6 s in
       `follower search setup` with 5 range reads and 1.6 s of
       `evaluation_wait_us` — the other 12 s was this mutex. Dashboards
       over the same hours are exactly this pattern.
    2. **FIFO slot semaphore with full fan-out.** Each query fanned out to
       all 192 evaluation slots; a newcomer's 40 acquires queued behind
       ~2,000 pending heavy ones.
  - Fix (a) shared leases: counted, granted immediately, each charging the
    reader's footprint to its own operation (`&VixReader` use is Mutex/
    OnceLock/thread-local safe); (b) `source::FairShare`: a query's
    in-flight evaluations are capped at `max(8, slots / active_evaluating_
    queries)`, so a newcomer raises `active` and incumbents free slots
    within one evaluation (a lone query still owns every slot).
  - **After (`.183`, same burst, fresh pods): small avg 1.04 s, max 2.5 s
    (≤ 3.5× solo, no outlier); heavy 24 h queries avg 33 s vs 24 s
    (they now share fairly and were cold).** Cold 7 d battery unchanged
    (17.6 / 21.7 / 14.3 s vs 16.9 / 20.7 / 12.7 — the fetch-gate raise
    bought nothing measurable; S3 cold-object latency ~100 ms/read is the
    floor). RSS 6.8–7.7 GiB, 0 restarts; 13 growth timeouts during the
    burst (legacy 32 MiB shapes — filtered histograms — competing for the
    4 GiB gate at 192 slots; 6 in the `.182` burst).
  - **Metadata tier (also in `.183`) is inert as shipped.** A real prod
    traces reader (`/tmp/prodvix`, 1,101 fields) opens at **667 KB** before
    any evaluation, reaches 787 KB after count + top-k, and `demote()`
    frees **13.8 KB**; the logs reader (2,586 fields) opens at 1,388 KB,
    demote frees 30 KB. The weight is the metadata itself: raw footer JSON
    kept after parsing (`dict_field_pages_v1` 98/233 KB, `fields` 55/136
    KB, `columns` 27/68 KB), the 256 KiB + 64 KiB tail allocations kept
    alive by `Bytes` slices, and parsed per-field tables. Prod confirms:
    after `.183` the cache still holds 1,839 readers in 2.15 GB and a
    second 7 d query with a new condition reads 6.6 ranges/file (cold: 7).
    Diet in progress for `.184`: drop parsed-out JSON, copy only referenced
    slices out of the tails, compact field tables — target ≤ 300 KB traces
    / ≤ 600 KB logs so 2 GiB holds one follower's 7 d.
- **`.184` shipped (vix-arch `30432cce2`, GitOps #583, queriers 19:56–19:58Z):
  reader metadata diet.** Fresh ranged reader on the prod pairs: traces
  667 → **198 KB**, logs 1,388 → **394 KB** (−70 %): footer JSON dropped
  after parsing, tail allocations released, `FieldEntry.types` as a u16
  flag set with byte-identical serde, per-field indexes built lazily, the
  name→id map replaced by a probe over `fields`, serde over-reservation
  trimmed. No wire-format change, fetch budgets unchanged, vortex_index
  356 / search vix 198 / core_writer 102 / segments 37 green. Prod after
  the roll: reader cache 1,839 → **5,461 entries** in 2.15 GB (1,077 full
  at 537 MB + 4,384 metadata-only at 1.61 GB ≈ 367 KB each); burst repro
  still clean (small avg 0.88 s, max 1.7 s; RSS 7.9–8.9 GiB).
  - **But a cache hit still cost ~5 reads/file.** Read-counting the prod
    files locally (throwaway probe, deleted): a cold exact-term count was
    **9 reads** — 5 tiny dict field-index probes (8 B, 288 B, 294 B, 4 B,
    4 B; one wave, five S3 requests), one key block, the terms Vortex
    footer **plus a sequential `NeedMoreData` prefix** (39 KB traces /
    167 KB logs; the layout exceeds Vortex's 65,535 B initial window), one
    `doc_count` leaf. A demoted reader paid **7**: the dict blob (184–197
    KB) straddles the 256 KiB eager tail (footer JSON alone is 154–369 KB
    on these schemas), so every field-index rebuild re-probed it. Prod
    confirmed: back-to-back 7 d traces counts, 80 % reader-cache hits,
    5.3 → 5.1 reads/file.
  - `.185` (vix-arch `71c03c3c6`): the dict block index is read ONCE (one
    184–197 KB request when not resident and ≤ 4 MiB) and kept as metadata
    across demotion; the terms footer's initial read is 256 KiB. Prod
    files: **cold count 9 → 4 reads, demoted count 7 → 2**, identical
    counts. Demoted reader ~645 KB traces / ~855 KB logs, so the reader
    cache goes 2 → 3 GiB in the same roll (`ZO_VIX_READER_CACHE_MAX_SIZE`
    3072; RSS 8–9 GiB of 24 today; 4 GiB once a day of RSS is seen).
    Expected: cold 7 d evaluation ≈ −40 % requests; a second long-window
    query over cached files ≈ 2 reads/file (−70 %).
  - Still open, evidence-backed: (1) `budget_refused` fallbacks under
    bursts (33 across four 13-query bursts, 6–9 per heavy query per
    follower, hitting `SimpleCount`/`SimpleTopN`/`SimpleHistogram` alike) —
    the 4 GiB eval gate fills when filtered histograms (still 32 MiB
    declared) run 16-wide next to everything else; measure their peak and
    declare it, or raise the gate; (2) `ZO_VIX_EAGER_TAIL_BYTES` 256 → 512
    KiB would put footer JSON + dict inside the tail for traces (443 KB),
    saving the dict read on cold opens at +256 KB per file — requests, not
    bytes, are the ceiling, so probably worth it; unmeasured; (3) logs
    footers are 369 KB of JSON (`dict_field_pages_v1` 233 KB for 2,586
    fields) — a binary property encoding would halve the tail need.
- **`.185` shipped (vix-arch `71c03c3c6`, GitOps #584, queriers 21:10–21:12Z,
  reader cache 3 GiB).** Prod, fresh pods: cold 7 d filtered traces count
  16.0 s, top-10 `span_status` 19.4 s, logs count svc 12.1 s (`.184`: 18.1 /
  21.9 / 13.0); reads per file logs 8.0 → 6.3, traces 5.2 → 4.9 (in prod the
  256 KiB tail already served part of the dict probes). **The cached-reader
  path still does not pay off in prod**: back-to-back 7 d traces counts got
  933 reader-cache hits over 14k lookups — demoted entries average 595 KB
  (198 KB metadata + 184 KB dict + ~257 KB retained terms-footer window),
  3 GiB holds 5.2k readers, one follower's 7 d traces share is 7k files,
  and a sequential scan over a working set larger than the LRU thrashes.
  Measured on the prod files (agent, partial, reverted before commit): the
  Vortex footer bytes actually consumed are **105 KB traces / 233 KB logs**
  of the 256 KiB window — trimming to the consumed suffix (vortex-file's
  `FooterDeserializer` returns the first consumed offset from the postscript
  alone; a retained parsed `Footer` is not an option because its layout
  tree pins the opening session's runtime) takes a demoted traces reader to
  ~490 KB, so 3 GiB ≈ 6.5k and 4 GiB ≈ 8.7k readers. Alternatively drop the
  resident dict from the metadata tier (one 184 KB read per file on a hit
  instead of five probes) → ~306 KB per reader. Not shipped tonight: the
  change touched `OpeningMemory`/`open_blob` wiring and was half done at the
  deadline; the tree is at `.185`.
- **Full battery on `.185` at 21:21Z (pods 10 min old, cold caches; obs
  r1/r2 vs O2, ms, all `partial=false`):** 1 h — traces count 1,116/677 vs
  168, hist 1,038/508 vs 172, top-10 15 m 968/307 vs 108, logs count 897/290
  vs 343, logs count svc 1,228/533 vs 105, logs `SELECT * LIMIT 50` 396/231
  vs O2 error. 24 h — traces count 837/391 vs 1,176, hist 785/288 vs 6,153,
  top-10 7,211/346 vs 1,528, p99 by service 12,470/11,280 vs 19,065, logs
  count 1,095/318 vs O2 error, logs count svc 1,426/476 vs 2,764, `SELECT *`
  933/378 vs 2,609. 7 d — traces count 1,676/1,152 vs 11,191, hist 1,171/720
  vs 37,865, **top-10 26,624/1,626 vs 24,174** (was 51.9 s cold at 11:34Z),
  logs count 1,653/917 vs 46,417, **logs count svc 14,817/926 vs 63,038**
  (was 32.9 s), `SELECT *` 1,682/1,031 vs 6,883. Cold 1 h shapes on fresh
  pods are 1 s (index-evaluation cache cold), warm 0.3–0.7 s; O2 is faster
  only on warm 1 h shapes and on `logs count svc 1 h`.
- **State at 22:02Z for the 10:00 check-in:** queriers 10/10 `.185`
  (RSS 7.8–9.0 GiB, 0 restarts), compactors 30/30 `.181` (0 OOM; last hour:
  traces 427 live merges avg 3.6 inputs / 1.36 TB, logs 127 merges + 612
  heals; logs hour 20 at close+62 min: 0 index-less of 558 files), ingesters
  and router `.178`. vix-arch is 12 commits ahead of `origin/vix-arch`, none
  pushed; release branches `.180`–`.185` exist locally. Scratch helpers on
  ops: `/tmp/obsq173.py` (existing), `/tmp/qb182.py`, `/tmp/qb_all.py`,
  `/tmp/conc.py`, `/tmp/burst.py`, `/tmp/burst_small.py`.
  - Today's chain, cold 7 d filtered traces count per follower: `.179` 85.0 s
    (5 sequential S3 round trips/file × ~100 ms cold-object latency, 64
    files in flight) → `.181` 59.9 s (2–3 round trips) → `.182` 16.9 s
    (measured 4 MiB admission instead of 32 MiB, 192 files in flight) →
    `.185` 16.0 s (fewer reads; the ~1,000 range-GETs/s per pod ceiling
    now bounds it). Concurrent dashboards no longer stall small queries
    (max 1.7–2.5 s vs 24 s).
  - Next, in order of evidence: (1) finish the footer-window trim (or drop
    the resident dict) and take the reader cache to 4 GiB so a follower's
    7 d traces share fits — then a second long-window query costs 2
    reads/file; (2) declare the filtered-histogram workspace from a
    measurement (33 `budget_refused` full scans across four bursts); (3)
    `ZO_VIX_EAGER_TAIL_BYTES` 512 KiB for traces-shaped sidecars; (4)
    binary footer properties for the 2,586-field logs schema (369 KB JSON).
- **2026-10-01 05:00–07:10Z — cache optimization round 2 (`.186`, `.187`),
  owner: "继续执行缓存优化".** Prod measurement first (`/tmp/cache_probe*.py`
  on ops: per-query reader-cache hits/misses/demotions on one pod + the
  follower's `io_accounting`):
  - **The metadata tier works for the realistic case.** Repeated 24 h
    filtered traces counts over ~1,250 files per follower: 99–100 % reader
    hits, **1.1–1.8 reads/file** (block + leaf; 1 when the key block is
    still cached), 1.6–2.8 s wall vs 7–40 s cold earlier in the day; once
    the per-file result cache is warm the same dashboards are 0.8–0.9 s
    (counts) / 2.4 s (top-N) with ~40 reader lookups.
  - **Top-N on a warm reader cost 10 reads/file**: `prefetch_field_bundle`
    pushed the KEY field's whole ordinal span into the leaf plan — 1,100+
    composite keys with large postings = 7 `doc_count` leaves per file,
    identical for every field, never used (only one key ordinal is). Fixed
    (`1c52bf5ea`): prod files 10 → 4 (traces), 8 → 4 (logs).
  - **The 7 d case still thrashes**: a demoted reader on the real 7 d mix
    averages **~840 KB** (merged 1.5–4 GB files carry much bigger dict block
    indexes than the 125 MB L0 sample's 184 KB), so 4 GiB holds ~4.2k of
    the ~6.9k traces files per follower → 26–38 % hits, 3.6–4.7 reads/file
    on repeats (vs 4.7–6.0 cold). Fitting 7 d needs ~6–7 GiB — RSS is
    10–11 GiB of 24 with the 4 GiB cache (memory cache 4 GiB, DataFusion
    pool cap 12 GiB), so not taken. 7 d queries are ~1 % of traffic.
  - `.186` (vix-arch `73cf6d387`, #585, 06:14Z, cache 3 → 4 GiB): top-N key
    span fix; terms Vortex footer retained as its consumed suffix (vortex's
    `FooterDeserializer` reports the first consumed offset from the
    postscript alone; a retained parsed `Footer` would pin the opening
    session) — demoted traces reader 645 → 488 KB on the sample file,
    logs 856 → 826 KB; hot tier 1/8 (reverted in `.187`: it demoted the
    recent day's readers every query — 1.8 vs 1.1–1.6 reads/file — and no
    split fits 7 d anyway).
  - `.187` (vix-arch `3ae68b6fd`, #586, 06:53Z) + `ZO_VIX_EAGER_TAIL_BYTES`
    256 → **768 KiB**, data tail 64 → 128 KiB: measured on the prod files,
    the sidecar footer JSON (154 / 369 KB) + dict index (184 / 197 KB) +
    terms footer now ride in ONE tail read, and the logs data footer
    (71 KB) no longer needs a prefix read. **Cold exact count per file:
    traces 6 → 4.7, logs 8 → 4.6 reads** (prod `io_accounting`, fresh
    pods); cold 7 d battery 14.7 / 18.1 / 10.8 s (`.185`: 16.0 / 19.4 /
    12.1; `.179` this time yesterday: 85 / 91 / 52). +512 KB per cold open
    (11.2 GB logical for a 7 d traces count; requests, not bytes, are the
    ceiling). RSS 10.1–11.1 GiB, 0 restarts; burst repro small avg 0.37 s /
    max 1.25 s.
  - Noted for the owner: `ZO_WARMUP_CACHE_HOURS=24` (since `.86`) already
    runs a boot warm-up — each fresh querier prefetches its ~2.2–2.8k-file
    share of the last 24 h (82–154 s after start, ~30 req/s). Queries
    issued during that window compete with it (two followers showed 2.6–
    2.9 s setup vs 1.4–1.7 on the others). The owner rejected *adding*
    warm-up; whether to keep this one is their call — it is a configmap
    knob (`0` disables).
  - Next, evidence-backed: (1) scan-resistant eviction for the metadata
    tier — a repeated 7 d scan over a working set 1.6× the cache gets
    26–38 % hits under LRU; protecting entries that have been hit once
    (evict zero-hit LRU first) would converge to ~60 %; (2) the dict block
    index dominates demoted-reader bytes on merged files — a size-capped
    dict residency (keep ≤ 256 KB, re-read larger ones: +1 read on those
    files) would roughly double 7 d capacity; (3) `budget_refused` under
    bursts (filtered histograms still declare 32 MiB).
- **2026-10-01 09:30–13:30Z — reader-cache admission (`.188` → `.195`):
  frequency admission starved new files; reuse-distance admission + a
  deterministic evaluation order.** Probe = one fresh pod, A: three 24 h
  traces counts (new terms, ~1,250 files), B: six 7 d counts (never-seen
  terms, ~7.4k files), C: 24 h counts again; per-query reader-cache
  hits/misses/rejections from the pod's `/metrics`.
  - `.188` (vix-arch `d2dc7439c`, #587, 09:32Z): W-TinyLFU — 1/16 LRU
    window, 4-bit count-min sketch, a spilled candidate displaces the main
    LRU victim only when strictly more frequent. Repeated 7 d: 1 → 28 →
    30 → 28 → 31 % hits — identical to `.187` plain LRU (7, 28, 29, 27,
    29, 28): LRU was never 0 % here because the HashMap gave every pass a
    different order and the cache holds ~55 % of the set.
  - `.189` (`83f79c645`, #588, 09:59Z): growth of a hit reader demotes other
    full readers before evicting (LRU-first growth eviction was shedding
    the entries the pass needed next). Repeated 7 d: 1 → 14 → 36 → 40 →
    43 %. `.190` (`03a8e43aa`, #589, 10:26Z): sketch reset period 10× the
    expected entries (Caffeine's rule), so popularity decays in ~30 min.
  - **The regression `.189`/`.190` introduced:** dashboards after a 7 d
    scan — hits 82 / misses 1,132 / **rejections 1,011**, then 152/1,058/
    1,057, 161/1,043/984, 224/980/681, 618/589/427: five queries and still
    not back; an untouched `.190` pod at 11:13Z rejected 82 of a 24 h
    query's 85 misses with no scan at all (34.6k rejections in 30 min on
    one pod). Cause (reproduced in a local simulation with 6 new files per
    dashboard query — hits slid 122 → 68/122): under strict TinyLFU a file
    asked for the FIRST time while main is full has frequency 1 and never
    beats a resident with ≥ 1, and this hot set is continuously replaced
    by new files (ingest L0s, merge output). Frequency ties cannot tell
    "new file every dashboard needs" from "one-off scan file"; plain LRU
    admitted both at once.
  - `.191` (`04cffe6d6`, #590, 11:25Z): **reuse-distance admission.** A
    spilled candidate displaces the main LRU victim only while the lookups
    between its two latest demand accesses (a window hit, or a re-request
    found in a bounded non-resident history) are strictly fewer than the
    victim's age — LRU's own keep/evict judgement applied to the newcomer
    (LIRS's IRR-vs-recency test). A once-asked file lives in the window
    only; a scan repeated over more files than fit carries a whole pass as
    reuse distance and never beats a resident the pass touched (stable
    subset); a file asked for again within a dashboard interval displaces
    residents idle that long. Sketch + decay gone; the window is FIFO (a
    hit no longer extends residency, so every newcomer gets the same
    chance to show a reuse); non-demand accesses (`warm_file`, the
    equality-histogram sidecar probe, planner `ordering`) are `peek`s and
    no longer count as lookups/hits. Regression test
    `dashboards_survive_scans_and_new_files_cost_one_miss`. Prod: A 732
    hits/547 misses → 1,277/5 → 1,282/0, 0 rejections; C after six scans
    16/1,270 (1,112 rejected) → 176/1,109 → **1,283/2 → 100 %** (two
    queries, vs never within five). Repeated 7 d fell to 1 → 4 → 14 → 13 →
    13 → 13 %.
  - `.192` (`9de86d6e3`, #591, 11:56Z): growth eviction sheds the window
    before main; `vix_reader_cache_evictions_total{reason=admission|
    growth_window|growth_main}`; history 4× entries. Counters: growth
    evictions **0** in every pass (not the churn); admission evictions
    2.3–3.4k per 7 d pass with hits flat at 17–19 %. Cause: `eval_files`
    came from a `HashMap`, so each pass visited the files in a different
    random order — every candidate's reuse distance is then noise against
    the victims' ages and ~a quarter of the resident set churns per pass.
  - `.193` (`e50c10968`, #592, 12:28Z): deterministic newest-first order.
    Repeated 7 d: **17 → 49 → 52 → 54 → 55 → 55 %** (admission evictions
    0 / 523 / 2 / 60 / 16 / 3 — pass 2 reclaims stale residents, then
    nothing moves); C after six scans **98 % → 100 % at once** (the recent
    day's files are inside the stable set); A 2 → 97 → 100 %. But the 7 d
    walls grew 1.4–1.7× (causeway 10.9/13.5 → 18.6 s, manus-node-admin
    13.8/16.4 → 22.3 s, nexus 10.8/12.5 → 18.0 s): newest first puts the
    small L0s first and the large merged files last, a serial tail behind
    the 32-way evaluation.
  - `.194` (`0804b08ef`, #593, 12:51Z): oldest-first. Repeated 7 d 16 → 26
    → 37 → 39 → 40 → 40 % (the oldest hours are the large merged files:
    ~3,150 entries fit vs ~4,100 newest-first), C 100 % at once, walls back
    to the random order's (causeway 14.0 s, manus-node-admin 17.7 s, nexus
    13.9 s) — **except the first pass: 90 s on pod 1, 98 s on pod 2 (idx
    47–49 s).** Follower: `skip-rate bail-out: 31 of 32 sampled files
    skipped the index whole-file — remaining files go to the scan branch`,
    `is_add_filter_back: true, file_num: 6798`, 68.6M rows scanned. The
    bail-out judges the whole condition from the first `eval_concurrency`
    files; in hour order that sample is ONE hour, and a service absent
    from the window's oldest hour condemned 6.8k files to the scan branch.
    Newest-first has the mirror bias (a service that stopped recently).
  - **`.195` (`3a7e0c678`, #594, 13:20Z, live): evaluation order = a fixed
    hash of the key** — deterministic across passes (the stable subset the
    admission rule needs) and time-mixed (the bail-out sample is as
    representative as the old random order). Fresh pod: A 53 → 100 →
    100 %; repeated 7 d **17 → 31 → 51 → 51 → 51 → 51 %** with admission
    evictions 0 / 2,011 / 0 / 0 / 1 / 3 (pass 2 reclaims stale residents,
    then the set is frozen), walls 22.8 / 12.1 / 16.2 / 12.2 / 20.1 /
    20.7 s (= the random order's; 0 bail-outs); C after six scans **99 →
    100 % at once**, 0 rejections. Repeated-7 d hits by version, pass 6:
    `.187` LRU 28 · `.188` TinyLFU 28 · `.190` 43 · `.191` 13 · `.192` 17 ·
    `.193` 55 · `.194` 40 · `.195` 51 %; dashboards after a scan: `.190`
    five queries and not back, `.195` the next query.
  - What this does not change: 7 d scans still cost 28–38k remote index
    reads per pod (4.2–5.7 reads per cold evaluation), and the reader-cache
    hit rate moves the wall little; the dashboards' 24 h sets are what the
    cache carries. RSS 7.9–9.3 GiB, 0 restarts throughout.
- **2026-10-02 07:00–08:30Z — cluster review (12 h on `.195`); the `.192`
  window eviction was a steady-state regression; `.196`/`.197`.** Three
  read-only sweeps (k8s/psql, Orbit errors, Orbit query lines) + probes.
  - Fleet: 49/49 pods Running, 0 restarts, 0 OOMKilled; querier RSS
    13.9–17.6 GiB of 24, compactor max 22.7 of 60; Argo Synced/Healthy =
    master tip. Backlog healthy (pending 677 → 116 in 3 min, 0 stale
    claims, no hour > 638 files in 12 h). Compactors: 135,872 merges /
    1 failed (S3 503 after 10 retries) / 127.5 MB/s active. Compactor
    `5gkkl` died 03:23Z (fleet health-check burst, no OOM/panic in logs —
    [INFERENCE] node replacement), replaced by `7vldr`. Ingester-1/-2
    readiness-probe timeouts (×4, ×17) at 06:28/07:14Z, self-recovered.
  - Queries, last 12 h (1,197 `search->result` lines, all `.195`) vs the
    previous 12 h (725, rollout-contaminated): p50 1,170 vs 2,429 ms,
    p95 10.6 vs 22.7 s, p99 34.7 vs 36.3 s, max 58 vs 98 s; 0 5xx/429, 0
    partials, 0 queue waits. The p99 tail is a family of 150–750 h
    `SELECT *`/`match_all` scans over 60–130k files (skip-rate bail-outs
    123 → 808 with the volume; `cannot produce exact vix candidates` 584
    → 2,605 lines). New WARN class: `[SEGMENT:SCAN] live-data scan passed
    the soft budget 512 MiB and continues` ×292 (logs/default; informational).
    3 `disk.rs:1510` `canonicalize().unwrap()` panics on the `job_runtime`
    thread during the `.195` rollout minute only (tmp dir racing a restart).
  - **Regression found by the sweep:** every querier's reader cache pinned
    at 4.0 GiB with ~1,870 entries, fleet hit rate 35 %, **2.84 M
    `growth_window` evictions against 3.18 M misses**. Steady-state probe
    (18 h pod): a 24 h traces dashboard = **1 % reader hits**, 1,217 misses,
    all 1,217 newcomers evicted inside the same query; a second query with a
    new term, the same. Two defects: (1) `admit` never enforced the hard
    budget — a publish on a full cache left `total` above `max_bytes` until
    the next growth callback, and `.192`'s growth eviction shed the WINDOW
    first, i.e. the newcomers (the `.192` hypothesis — growth evictions
    churning the stable set — was disproved by its own counters the same
    day, but the change stayed); fresh-pod probes never showed it because a
    fresh cache is not full. (2) Growth charges are high-water marks (the
    reader's FIFO block-cache shrink is ignored for ordering safety) and
    demotion subtracted only what `demote()` released — ghost bytes: **2.29
    MB accounted per demoted reader vs ~0.84 MB real** (3.23 GB / 1,409
    metadata entries).
  - `.196` (vix-arch `2e784f482`, not rolled; folded into `.197`): the
    window's bytes are RESERVED (`main_bytes = max - window`); one `enforce`
    path (demote hot overflow → trim main LRU-first to its share, reason
    `overflow` → spill the window's FIFO front through reuse-distance
    admission) runs on publish and on growth; the admission line is the
    same `max - window` (the old `max - hot` line left the top quarter of
    the budget unused once demotion capped the hot tier — prod pods sat at
    3.0–3.3 GB of 4.29). Demotion resyncs `accounted` to
    `reader.memory_size() + overhead`. Tests
    `full_cache_growth_never_evicts_the_window`,
    `demotion_resyncs_accounted_bytes_to_the_reader`;
    `vix_reader_cache_evictions_total{reason}` = `admission` | `overflow`.
  - **Owner question: why did
    `SELECT date_bin(1 minute), response.status, count(*), max(latency) FROM
    apisix WHERE request.uri = '…ReportServiceAccess' AND str_match(
    request.body, 'S77x77…')` over 5 minutes of 2026-09-22 take 45.8 s
    (trace `01a0fabaabd17442aef2358b807facd7`)?** Two subagents (logs +
    code): 2 straddling 3.4 M-row files (17 GB data, 1.70 GB sidecars), one
    follower (`5nbkd`); `str_match` → `VixQuery::Contains` → a full walk of
    the field's dictionary (`scan_all_tokens` → `scan_key_range`): no FST/
    prefix shortcut for substrings, no time pruning before the walk (the
    `_timestamp` clamp is applied AFTER `eval`), and `eval_and` collects
    ordinals for every leaf before any selectivity ordering, so the cheap
    `request.uri` equality cannot bound it; `idx_optimize_mode = None`
    (two group keys + `max()`), so the 512 MiB projected-cost bail never
    applies. The read pattern was the killer: `load_dict_block_span`
    preloaded only the FIRST 8 MiB window of the field and then fell back to
    `dict_block` — **one 64 KiB ranged read per block, serially: 15,992
    fetches (= logical_ranges = fetch_batches), 964 MB, ~1.5 reads in
    flight, 45.3 s of the 45.8 s**; 13,442 of the reads came from the local
    disk cache (an aborted earlier attempt had warmed it) and still cost
    ~4 ms each in the chain. `request.body` is near-unique per row
    (`ZO_VIX_MAX_RAW_TERM_LENGTH=65532`), ~500 MiB of dictionary per file.
  - `.197` (vix-arch `5f43f869f`, #595, **live 08:17Z**, = `.196` + the
    walk fix): `scan_key_range` reloads the next 8 MiB window from the block
    at hand (test: 600k high-entropy terms, 18.7 MB of blocks — 171 fetches
    before, ≤ 11 after). Cold re-run of the same shape on an untouched
    window (03:30–03:35Z, 2 files, 6.96 M records): **62 fetches per file,
    19.5 s** (was ~8,000 / 45.8 s); the rest is 880 MB of dictionary at one
    S3 GET at a time (~24 MB/s). Reader cache on a FULL cache (warm-up fills
    it in 3 min; 5,000–6,300 entries in 4.29 GB vs 1,870): dashboards after
    scans 74 → 46 → 76 → **100 %** and stay (was 1 % forever); 7 d repeats
    71–83 % at best under concurrent traffic (`.195`: 51 %); newest-hour
    queries 96–100 %. RSS 12.2–13.5 GiB on fresh pods (the cache now really
    holds 4.3 GB of readers). Steady-state fleet hit rate: re-check after
    several hours (counters since 08:17Z; `overflow` evictions should stay
    near zero outside scans).
  - Still open for the `str_match` shape (design, not a patch): resolve the
    cheap `Exact` postings first and check the substring on the surviving
    rows from docs (or a per-field bloom/prefix probe), and let dictionary
    windows fetch in parallel (`plan_ranges` coalesces touching ranges into
    one GET, so parallelism needs a prefetch, not bigger batches).
  - **Cache rebalance (GitOps #596, queriers 09:40Z):** owner asked whether a
    ~10 GB footer/reader cache would pay. Data: ≥26 h queries are 9 % of
    queries but 79 % of file evaluations (12 h: <2 h 893 q / 0.26 M files;
    2–26 h 457 / 1.19 M; 26–80 h 71 / 1.28 M; 80–200 h 44 / 2.12 M; ≥200 h
    18 / 2.07 M); a reader hit removes ~2.7 of 4.7 reads per file plus the
    footer parse; 7 d traces ≈ 7k files ≈ 5.9 GB per follower, 7 d of all
    streams ≈ 17 GB; the memory file cache held 18 logs data files / 1.9 GB
    and served 0 of the sampled index reads. Done within the 24 Gi limit:
    `ZO_VIX_READER_CACHE_MAX_SIZE` 4096 → **6144**, `ZO_MEMORY_CACHE_MAX_SIZE`
    4096 → **2048** (128 MiB buckets still fit those files; 1024 would not).
    4 min after the roll: reader 4.7–5.9 GB / 5,355–7,192 entries per pod,
    memcache 0–1 GB, RSS 9.7–13.1 GiB. A 10 GiB cache needs a 32 Gi limit:
    nodes are 64 GiB m8g.4xlarge shared with a compactor (RSS ≤ 22.7 Gi,
    OOM gate 40) — only with anti-affinity or a dedicated node group.
  - **Compactor progress (subagent, two psql snapshots 10.5 min apart +
    Orbit):** keeping up. 24 h: 296,814 merges / 92.8 TB merged vs 78.5 TB
    arrived (1.18×), 124 MB/s active, 3 failures, 0 refusals, 0 restarts;
    79–88 of 90 slots busy (free_slots=0 on 71 % of claims), CPU 6.8 % /
    mem 18 % of limits; ~500 jobs/h drained, oldest pending offset
    advancing (2026-09-13 17:00). Live/recent lanes: every closed hour of
    traces/logs settles within 2–3 h, max 737 files/h. The agent flagged
    ~23k "unscheduled debt hours" in ~300 `default/metrics/*` streams (one
    lone 1.7 MB index-less .vix per stream-hour since 09-28 15:00) — a
    false positive: `ZO_VIX_INDEX_DISABLED_STREAM_TYPES=metrics`, so the
    sweep's lone-unindexed clause is off for metrics by design
    (`merge.rs:343/475`); one file per hour with no index wanted is the
    terminal state, not debt.
  - **Compactor consolidation (GitOps #597, rolled 11:42–11:43Z): 30 → 18
    pods, same 90 merge slots.** The fleet was slot-bound (79–88 of 90
    busy) at 7 % CPU / 18 % memory; slots are auto-derived per pod as
    `cpu_slots = (cpu_limit − merge_threads) / merge_threads` (3 at
    16C/4 threads; memory_slots 10). Change: `ZO_VIX_MERGE_THREAD_NUM` 2
    (compactor-only env override; configmap stays 4), CPU limit 16 → 12 →
    5 slots per pod (startup log: `role_cpu=12 threads=2 cpu_slots=5
    mem_slots=10 total=5 backlog=1 live=4 hot=2 recent=2`), 18 × 5 = 90;
    backlog workers 30 → 18, live 60 → 72. 16C with 2 threads would have
    been 7 slots ≈ 48 GiB peak RSS (measured ~6 GiB per busy slot + ~6
    fixed) — past the 40 GiB gate; 5 ≈ 36. Old pods released their claims
    within ~40 s (rollout done 11:43:09Z, 0 stale leases).
  - First 27 min: jobs done +329 / 15 min then ~507/h (= the pre-roll
    ~500/h — slot-bound as designed), running 84–87, pending 477 → 48 →
    331 (the periodic old-data sweep re-enqueue, oldest 09-21 16:00),
    0 stale. L0 builders: 54 % busy on 18 pods (29 % on 30), segments
    built 25.7k/h vs 23.6k/h before, `wal_segments` pending 126–144 with
    oldest 10–14 min (was 74 / 6 min) — the lane with the least headroom
    now; the gate for it is pending age growing hour over hour. Fleet
    CPU 38–44 cores on 18 pods (max 8.1 on one pod of 12), memory 113–120
    Gi (max 12.9 Gi per pod, fresh). Nodes: 69 → 64 within 30 min
    (Karpenter consolidating the 12 compactor-free nodes; 1 draining).
    24 h rollback gates unchanged: merged/arrived < 1.0, oldest pending
    offset not advancing, any pod RSS > 40 GiB, or wal_segments pending
    age rising → replicas 30 (slot settings may stay).
- **2026-10-05 — VIX slow-read root cause (10-04 report) verified; AND
  evaluation re-planned, fts-field equality narrowed to tokens, superset
  results memoized (vix-arch, local verification; not rolled).** The
  report (`vix_slow_read_root_cause_2026-10-04.md`) was checked line by
  line against HEAD and Orbit: every claim holds (line numbers off by
  1–6). Trace `01a1044bc69e…` follower `c3dInns`: pass 1 (SimpleHistogram)
  bails after 32 skips at 14 MB, pass 2 (row-id) `logical_ranges=123,001
  / 29.65 GB / remote 21.35 GB / active 3,885 s / evaluation_wait 4,930
  s`, 8,101 of 8,197 files kept, 45.1 s index + 13.5 s scan. Corrections
  to the report: a wave can carry up to 8×10 physical reads (the
  "one read in flight" mechanism is approximate; the 3.5 + 2×leaves fit
  is right); the fixed per-file cost is tails(1) + the **`body`
  `key_term_exists` probe (1, unaccounted)** + service_name block(1) +
  token block(1), not "dict directory"; `to_vix_query` emits NESTED
  `And([And(tokens), Exact(service_name)])`, so service_name's postings
  were always read before a token miss could short-circuit; row-id mode
  does have the 35 % density give-up (`guard_matched_rows`), just no
  byte-projected bail; aggregate passes never warm sidecars
  (`warm_cache = None`).
  - **Found while reproducing (not in the report):** vortex 0.79's
    `with_row_indices` without a row range plans one "exact" split per
    ≤4,000-row cluster and the ChunkedReader reads EVERY chunk inside it
    — the terms table's row blocks hold <100 rows each, so a token present
    in two FTS fields (ordinals ~90 chunks apart) read **12.3 MB** per
    leaf on a 1 M-row fixture. `scan_blob_streaming` now always pairs
    `Indices` with `with_row_range(first..last+1)` → 134 KB (two chunks).
    Whether prod sidecars hit the same span depends on the dictionary
    layout; the fix only reduces reads.
  - `VixReader::eval_and` rewritten (`reader.rs`): nested `And`s and
    field-scoped `FullText { And(..) }` children flatten into one plan with
    per-leaf scope; dictionary IO is three waves — all named points
    (`field = v`, key existence) in one `resolve_points_in` batch, then
    every token leaf of every scope in one batch, then prefix/regex/fuzzy
    scans — each wave short-circuiting on an empty leaf;
    `intersect_leaves` then issues ONE terms-table scan over the union of
    ordinals (every leaf's `doc_count`), drops duplicate and
    subset-implied leaves (`status AND status`; `deploy` under the body
    scope implies `deploy` under the all-FTS scope), applies inline (rare)
    cells first, and reads plist records in ≤2 batched waves: wave A =
    full records for the rarest leaf / small records / records dense
    relative to the accumulator bound (`bound × 4 > skip groups`), skip
    headers otherwise; wave B = only the skip groups the accumulator can
    touch (`postings::plan_groups` / `for_each_in_groups`, new).
    Plist windows go through the new `VixRangeSource::fetch_many_sparse
    (max_gap = 16 KiB)` → `LadderRangeSource` caps gap coalescing (the
    default 1 MiB policy would re-fetch whole records between selected
    groups). Tests: `tests/and_intersection.rs` (parity + IO-wave
    contract), `postings::test_group_windows_match_naive_everywhere`.
  - Harness `tests/and_io_bench.rs` (`cargo test -p vortex_index
    --release --lib and_io_bench -- --ignored --nocapture`; 1 M rows,
    40 services, `plist_min_docs=8192`, 768 KiB tail, 20 ms per round
    trip; `VIX_BENCH_FILE=x.vix VIX_BENCH_FTS=… VIX_BENCH_QUERY=…
    VIX_BENCH_SERVICE=…` runs a real sidecar). `match_all('server_status:
    DEPLOY_STATUS_SUCCESS') AND service_name = X`: **14 batches / 20.4
    waves / 63 MB → 5 / 5.0 / 1.18 MB**; `absent token + svc` 4 → 2
    batches; `rare 2 tokens + svc` 6 → 5. With the body conjunct (below)
    the full prod shape is **6 batches / 6.2 waves / 331 KB (6 candidate
    rows)** vs the pre-change 14 / 20 / 63 MB + a 470-row superset.
  - Query layer (`index.rs`, `vix/mod.rs`): `FieldCap::FtsOnly` split into
    `Tokens` (file marks the field fts — decided from the field table, no
    `key_term_exists` probe: one wave per cold file saved) and
    `Unservable`. `body = 'Sending deploy callback'` on a `Tokens` field
    no longer drops the conjunct: `Condition::fts_superset_query` maps
    equality / positive IN to `FullText { [body], And(tokens) }` — a
    SUPERSET (`has_skipped` stays, filter re-applied) that shrinks the
    candidates from "every row of the other conjuncts" to rows carrying
    the phrase's tokens and eliminates files lacking a token exactly
    (NoMatch, cached). Negations / nested shapes / str_match / regex /
    values with no tokens keep the skip. Aggregates still fall back
    (`has_skipped`), so the SimpleHistogram pass still goes to the row-id
    pass; the pass is now cheap and cacheable.
  - Result cache: `has_skipped` `RowIdsSelection` results and the
    straddling-file pre-clamp bitmap memo are stored under
    `superset_cache_key(key)` (`{key}|superset`); the main lookup falls
    back to it and returns `add_filter_back = true`; the memo read uses it
    only when the current evaluation is itself a superset. The exact key
    never holds a superset (#34 test rewritten:
    `superset_bitmap_memoizes_under_its_own_key_only`). Repeats of the
    `DEPLOY_STATUS_SUCCESS` family (98.8 % of files with candidates, 115–
    175 GB remote per run) become cache hits.
  - **Prod-file A/B (same harness, 20 ms/round trip; file
    `default/logs/default/2026/10/02/12/75117742571525980165575`, 112 MB
    data + 42 MB sidecar fetched through `ops`, 471k rows, 2,232 fields,
    FTS `body,content,data,error,message`, 127 `service_name` values; in
    this file `server` is dense in body (153k rows) and `status`/`deploy`/
    `success` rare (190–828), the opposite mix of the fixture):**

    | query | HEAD `e48a58c07` | this change |
    |---|---|---|
    | `match_all(server_status:DEPLOY_STATUS_SUCCESS) AND svc=cfworkers-deploy-cloudrun-worker AND body=Sending deploy callback` (8 hits) | 17 batches / 16.1 waves / 5.64 MB | 14 / **7.4 / 0.67 MB** |
    | same without `body` | 14 / 12.5 / 5.22 MB | 14 / 7.1 / 0.53 MB |
    | `svc=llm-router` (0 hits) | 15 / 13.9 / 5.25 MB | 13 / 6.4 / 0.70 MB |

    HEAD re-reads the same 934 KB terms span once PER LEAF (4×) plus a
    1.2 MB one — the vortex split issue × leaf count — and decodes the
    whole 122 KB `server` record; now the terms cells come in one wave of
    5 chunk reads (`with_coalesce_distance(16 KiB)`: each touched chunk's
    adjacent `doc_count`+`postings` segments merge, the untouched chunks
    between them do not — exact ranges alone cost +1 wave because 12
    segment reads exceed the 8-in-flight limit), `server` is read as a
    1.2 KB skip header + seven ~800 B groups, and the rare inline tokens
    end `llm-router` before any plist wave. Per-file remote bytes 5.2–5.6
    MB → 0.5–0.7 MB (−88 %), serial depth −50–55 %.
  - **Config finding from the real sidecar:** the terms blob's vortex
    footer (`43853120+35276`) starts 35 KB BEFORE the 768 KiB eager tail
    (`dict` 200 KB + terms footer + puffin footer of 2,232 fields ≈ 802
    KiB), so every cold open of this file pays one extra round trip for
    it. `ZO_VIX_EAGER_TAIL_BYTES=1048576` (+256 KiB per cold sidecar tail)
    would make it tail-resident; verify on a few more files before
    changing the fleet value.
  - Not done / next: (1) residual filtering INSIDE the index for
    aggregates (decode the `body` column for the ≤ N candidate rows and
    make the bitmap exact — removes the second pass and the 13.5 s scan;
    report P1); (2) row-id evaluations still declare 32 MiB
    (`evaluation_working_bytes`) — the admission gate change needs a prod
    memory measurement like `.182`'s; (3) one dev querier A/B
    (`io_accounting` `fetch_batches`/file, `logical_bytes`/file,
    `evaluation_wait_us`) before rollout. Workstation note: WARP is up but
    the EKS endpoint 172.31.125.77 is ENETUNREACH locally (private-network
    route not pushed); `ssh ops` has `kubectl` (ctx `Prod-ops`) and
    `aws s3` read access — querier pods are distroless, read their PVC
    via `kubectl debug node/… --profile=general --image=busybox`.
- **2026-10-05 13:12Z — `.198` querier rollout (the change above):
  `release/vix-20261005-198` `f46889a29` = `.197` + vix-arch
  `60641baae` (clean cherry-pick, byte-identical to the dev checkout);
  image OCI index `78d031f2…`, arm64 manifest `91239477…`, binary
  `d7da7925…`; GitOps #599 (`d9ef553d`, querier line only, server
  dry-run clean); Argo Synced 13:12:50Z, rollout done 13:14:06Z, 10/10
  pods on the new digest, 0 restarts, RSS 14.9–18.4 GiB of 24 after 40
  min; no new error class (`ResourcesExhausted` from the DataFusion pool
  ran at the same rate before). Ingester/router stay `.178`, compactor
  `.181`. Rollback: `.197`.**
  - Battery (ops `/tmp/obsq.py`, `use_cache=false`, ONE sealed 48 h
    window ending 12:35Z for both legs; baseline pods hours-warm, `.198`
    pods brand-new = empty disk/reader caches). `A48` = the report's
    family as a 1 h UI histogram: `match_all('server_status:
    DEPLOY_STATUS_SUCCESS') AND service_name='cfworkers-deploy-cloudrun-
    worker' [AND body='Sending deploy callback']`.

    | query | `.197` r1 / r2 | `.198` r1 / r2 | `.198` idx_took r2 |
    |---|---|---|---|
    | A48 body (hist+count) | 86.1 / 81.9 s | 54.3 s (first ever, all remote) · 39.3 / 37.3 s | 44–52 s → **0.46 s** |
    | A48 no body | 69.1 / 67.9 s | 29.9 / 35.5 s | 35–37 s → 0.22 s |
    | A48 `DEPLOY_STATUS_FAILED` (cold query, warm disk) | — | 49.6 / 21.0 s | 37.8 → 5.7 s |
    | B48 `svc+body LIMIT 1000` | 14.7 / 7.5 s, **partial=true, 40 hits** | 14.1 / 2.3 s, complete, 83 hits | 6.0 → 0.34 s |
    | C24 `match_all('buildkitd.sock') LIMIT 500` | 10.3 / 5.9 s | 12.5 / 2.3 s | 4.0 → 0.13 s |
    | L24 hist `match_all('error') AND svc` | 6.2 / 1.5 s | 10.1 / 1.0 s | 0.57 → 0.14 s |
    | T1h traces count | 2.3 / 0.75 s | 1.8 / 0.53 s | — |

    r1 of the small queries is slower on `.198` only because the pods were
    minutes old (all-remote); every r2 is faster. B48's baseline answer was
    INCOMPLETE (`is_partial`, 40 rows): `.198` keeps 45–63 files per
    follower instead of 95–101 (the body tokens prune) and returns the
    full 83 rows.
  - Per-follower index phase (Orbit `io_accounting` / `reduced file_list`,
    10 followers, median / max):

    | run | idx s med / max | logical ranges per follower | remote | `queue` med | `active` med | `wait` med |
    |---|---|---|---|---|---|---|
    | `.197` A48 body r1 (warm pods) | 19.7 / 44.2 | 33.8k | 54.5 GB (+24 GB disk) | 51 s | 1,417 s | 1,606 s |
    | `.197` A48 body r2 | 18.5 / 52.2 | 32.5k | 53.0 GB | 318 s | 1,101 s | 1,807 s |
    | `.198` A48 body first run (fresh pods, 0 % disk) | 22.0 / **24.6** | 43.0k (physical 37.1k) | **27.6 GB** | 1 s | 2,245 s | 2,376 s |
    | `.198` A48 body r2 (superset memo) | 0.2 / 0.5 | 12 | 0 | 0 | 0 | 0 |
    | `.198` A48 FAILED r1 (cold query, warm disk) | 10.5 / 37.8 | 28.6k (770 of 2,300 files kept) | 0 (11.5 GB disk) | **1,751 s** | 113 s | 1,002 s |

    Reading: (a) a fully cold `.198` evaluation costs about the same wall
    as a 25 %-disk-warm `.197` one at half the remote bytes and a much
    tighter follower spread (max 24.6 vs 44–52 s); (b) the index phase of
    a REPEAT is gone (0.2 s) — the superset memo serves every file; (c)
    the `FAILED` family now eliminates 2/3 of the files exactly (absent
    token → NoMatch, cached); (d) what remains on a cold query is
    admission, not IO: `wait` (EVAL gate, 32 MiB per row-id evaluation)
    and, on warm disk, `queue` (the per-pod 512 fetch permits shared with
    other tenants' remote reads — the straggler pod `gtdbf` queued 8,358 s
    against 131 s of active IO). Physical reads per follower rose ~10 %
    (37.1k vs ~33.8k: skip-group windows and the 16 KiB plist gap keep
    small ranges separate) while bytes halved; candidate rows per follower
    are unchanged (~10k) because for this family the body tokens imply the
    match_all tokens.
  - Counters on the new pods after the battery: `fast_path_fallback_total`
    = `residual filtering` ~1,000 + `skipped_file` ~10k per pod (the
    aggregate pass still bails and falls to the row-id pass, as designed),
    `eval_growth_timeouts_total` 3 and 9 on two pods (cold all-remote
    runs), no `budget_refused`; result cache ~65 % hits.
  - Next (in order): (1) plist windows back to the ladder's default
    coalescing and `SMALL_RECORD_BYTES` = 1 MiB so header+group reads
    only pay off on multi-MiB records (fewer physical reads, one wave for
    sub-MiB records); (2) row-id evaluations declare the index-only
    workspace instead of 32 MiB (the `wait` column); (3) `ZO_VIX_EAGER_
    TAIL_BYTES` 1 MiB (one round trip per cold file); (4) residual
    filtering inside the index for aggregates (removes the second pass
    and the 30–60 s scan that now dominates A48).
  - **Regression check, 13:40Z window (`/tmp/vixab/battery_reg_198.jsonl`;
    pods 30–45 min old).** 16 other shapes, r1 → r2 wall (index / scan):
    logs `SELECT * LIMIT 50` 1 h 4.7 → 3.5 s (0.1 / 3.3); `count(*)` 24 h
    0.9 → 0.6 s; `str_match(body,…)` 24 h 6.2 → 5.6 s (0.04 / 5.5,
    scan-bound as before); `match_all('deplo*')` 10.7 → 2.8 s; `IN(3) AND
    match_all` count 10.9 → 1.8 s; `(a OR b) AND match_all` count 10.1 →
    3.0 s; `body = 'error'` (the new Tokens path on a dense token, LIMIT
    50) 12.2 → 6.0 s (7.6 → 0.46 index, 5.4 scan); `re_match(body,…)` 6 h
    42 s both (index 0 — regex on an fts field is a full scan on every
    version); `hist(svc = x)` 24 h 8.7 → 2.0 s; traces count 1 h 2.6 →
    1.0 s, svc histogram 3 h 2.7 → 1.1 s, APM group-by 1 h 5.0 → 3.6 s.
    Every warm index phase ≤ 2.3 s; cold r1 index phases 6–9 s on 24 h
    windows are the empty reader/disk caches of the new pods.
  - Four aggregate shapes scan 10–20 s after a sub-second index phase;
    the follower fallback reasons are all pre-existing: `match_all('deploy
    status')` histogram and `match_all(..) AND svc != x` count →
    `aggregate predicate requires residual filtering` because
    `Condition::MatchAll(v).can_remove_filter() = is_alphanumeric(v)` —
    **every multi-word match_all (space, `:`, `_`) is treated as inexact,
    so no aggregate over it ever uses the fast path and its 30–60 s scan
    is now the whole A48 cost**; top-N over match_all → `NULL aggregate
    group requires scan`; `min/max(_timestamp) WHERE svc = x` → no
    optimizer rule (two aggregates). Owner (10-05): **by design** — a
    multi-word `match_all` is a phrase/substring predicate in SQL (the
    `is_alphanumeric` gate), so the token AND is a superset and the scan
    must re-apply it; the index-side lever is residual filtering (next
    item 4), not a semantics change.
  - Organic traffic 13:15–14:15Z (battery traces excluded) is NOT yet
    comparable to the pre-window: index phase p50 131 vs 736 ms, but
    totals p50 3.3 vs 1.4 s — every pod's 2000 GiB disk cache and reader
    cache restarted empty at 13:14Z (A48 r2 data-file disk ratio 1–25 %
    vs 23–29 % on `.197`), plus a 355-query `select+eq` burst at
    13:15–13:45 and wide-window needle `select+match_all`s (38 h,
    0 GB scan, 15–23 s = cold per-file opens). Re-run `/tmp/pop.py` and
    the regression battery after the caches have had 2–3 h before
    judging the scan-side classes.
- **2026-10-05 16:15Z — `.199` querier rollout (next items 1 + 3 + a
  new 11): `release/vix-20261005-199` `485dbd13f` = `.198` + vix-arch
  `9bee06e3a` (clean cherry-pick, byte-identical); image OCI index
  `13e31359…`, arm64 manifest `1f1afa57…`, binary `76cd4ca3…`; GitOps
  #600 (`87a9559a`, querier line only, server dry-run clean); RS
  `57b8bfd6fc` 16:15:27Z, 10/10 pods 16:15:31–16:16:47Z, 0 restarts,
  RSS 14.3–17.2 GiB of 24 after 15 min. Rollback: `.198`.**
  - Engine (vortex_index 362 / search 1119 tests; worktree identical):
    (1) plist reads are REQUEST-BOUND again — the skip-group window
    rides the ladder's default coalescing and `SMALL_RECORD_BYTES` = 1
    MiB, so header+group reads only pay off on multi-MiB records and a
    sub-MiB record is one wave; (3) the data object opens with a
    tail-sized footer read (`ZO_VIX_EAGER_TAIL_BYTES`, prod 768 KiB)
    instead of the 64 KiB probe + `NeedMoreData` second trip; (11) field
    capability is answered from the tail-resident field table with zero
    IO, and an FTS-only field's equality (`body = '…'`) is dropped from
    the index conjunct as unservable (`has_skipped`, superset, memoised)
    instead of being probed per file.
  - Battery (same `/tmp/obsq.py` plan as `.198`, 48 h window re-anchored
    to 16:10Z; pre-leg on 3 h-warm `.198` pods, post-leg on brand-new
    `.199` pods — empty disk/reader caches, r1 all remote). wall / idx ms:

    | query | `.198` r1 · r2 | `.199` r1 · r2 | hits |
    |---|---|---|---|
    | A48 body | 49,741 / 17,568 · 36,071 / 381 | 55,861 / 18,977 · 53,192 / 3,832 | 49 / 49 |
    | A48 no body | 51,894 / 17,862 · 29,571 / 301 | 54,670 / 17,305 · 55,918 / 1,446 | 49 / 49 |
    | A48 pending token (never seen) | 16,779 / — · 3,370 / 1,860 | 29,264 / 26,663 · 2,944 / 391 | 0 / 0 |
    | B48 `svc+body LIMIT 1000` | 8,557 / 5,318 · 4,997 / 350 | 12,640 / 10,704 · 6,453 / 3,207 | 90 / 90 |
    | C24 `match_all('buildkitd.sock') LIMIT 500` | 8,868 / 4,004 · 4,536 / 182 | 11,674 / 7,544 · 8,092 / 1,414 | 500 / 500 |
    | L24 hist `match_all('error') AND svc` | 12,051 / 10,541 · 1,550 / 159 | 10,975 / 9,247 · 4,038 / 849 | 49 / 49 |
    | `IN(3) AND match_all` count 24 h | 2,981 / 2,297 · 1,398 / 157 | 23,884 / 21,383 · 4,103 / 1,048 | 1 / 1 |
    | `body = 'error'` dense LIMIT 50 | 6,520 / 1,942 · 4,772 / 294 | 26,955 / 18,397 · 10,977 / 855 | 0 / 0 |
    | hist `svc = x` 24 h | 2,623 / 2,090 · 892 / 319 | 24,637 / 23,488 · 630 / 207 | 49 / 49 |
    | `str_match` 24 h (scan-bound) | 6,267 / 45 · 5,644 / 51 | 6,445 / 30 · 14,321 / 1,231 | 1 / 1 |

    The r1 columns are not a code comparison (warm vs empty pods — the
    24 h shapes at 18–23 s of index time are per-file cold opens, same as
    `.198` at 13:15Z). What IS comparable is the fully cold A48 body
    first run, `.198` fresh pods vs `.199` fresh pods, per follower
    (Orbit `io_accounting`): physical reads **37.1k → 26.0–28.4k**
    (−25 %, ~13 per file), remote bytes **~2.76 → 2.1–2.5 GB** (−10–15
    %), follower index phase **22.0 med / 24.6 max → 13.1–13.8 s**,
    leader `idx_took` 44–52 → 19 s. The wall did not move (54 → 56 s)
    because the 35–40 s residual scan dominates (item 4). `active` 1,662–
    1,853 s and `wait` 1,519–1,633 s per follower are unchanged: the EVAL
    gate (32 MiB per row-id evaluation, ~120 of 192 slots admitted) is
    now the index-phase bound (item 2 → `.200`).
  - The superset memo works but is being EVICTED BY COUNT: back-to-back
    A48 ×3 on `.199` → idx 407 / 494 / 393 ms, 0 fetches; the battery's r2
    (12 queries later) → 3,832 ms, exactly 1.0 fetch per file (2,356–
    2,600 per follower, ~100 MB). The per-file result cache is capped by
    the configmap at 100,000 ENTRIES / 512 MB; a 48 h logs query inserts
    ~2,300 per follower per pass (two passes when the aggregate pass
    bails), so ~20 distinct queries FIFO the whole cache while its bytes
    sit at 27 MB (`zo_vix_result_cache_memory_usage`; hits 16,369 of
    47,135 requests on one pod). `entry_footprint` already counts both
    key copies (~300 B per NoMatch / sparse entry) → raise the entry cap
    to 1.5 M (querier env) and let the unchanged 512 MB be the bound.
  - Counters 15 min after the rollout (fleet sums): `eval_growth_
    timeouts_total` 72 (cold all-remote battery; `.198` had 3 + 9 after
    its), `fast_path_fallback_total{budget_refused}` **12** (`.198`: 0 —
    watch on `.200`, where more evaluations are admitted concurrently);
    0 restarts, no new error class.
  - Organic 16:17–16:50Z vs `.198` 15:35–16:10Z (`/tmp/pop.py`): index
    phase p50 1,388 vs 463 ms, `groupby+eq` 5.2 vs 0.45 s — the empty
    caches again; not a verdict. Re-run after 2–3 h.
  - Per-evaluation owned-memory growth, measured in `and_io_bench` on the
    two prod sidecars (`VIX_BENCH_LOG`, `reader.memory_size()` before →
    after): **+199…+281 KB per evaluation** for every shape (match_all +
    svc + body, svc only, rare token, absent token, deploy only; 1 M-row
    synthetic too) — the row-id workspace is tail/prefetch/footer slack
    + bitmaps, not 32 MiB; that is `.200`'s declaration.
- **2026-10-05 16:49Z — `.200` querier rollout (next item 2 + the result
  cache entry cap): `release/vix-20261005-200` `bbee37dcb` = `.199` +
  vix-arch `0ff363ad9` (clean cherry-pick, byte-identical); image OCI
  index `48768e1e…`, arm64 manifest `7a3b9ed1…`, binary `9d007b65…`;
  GitOps #601 (`0073c475`: querier tag + querier-only
  `ZO_INVERTED_INDEX_RESULT_CACHE_MAX_ENTRIES=1500000`, rendered diff =
  those two lines); RS `5f97c86f96` 16:49:04Z, Argo Healthy 16:50:59Z,
  10/10 pods 16:49:09–16:50:31Z, 0 restarts, RSS 8.1–10.6 GiB of 24 after
  10 min (fresh). Rollback: `.199`, drop the env line.**
  - Engine (`search/src/vix/mod.rs`, `evaluation_working_bytes`): a plain
    row-id evaluation (`mode == None`) declares `EVAL_ROW_ID_WORKSPACE_
    BYTES` = **12 MiB** + bitmaps (+ the straddling clamp) instead of the
    32 MiB streaming-collector workspace, which only aggregate collectors
    keep. Basis: the measured +199…+281 KB owned growth per evaluation
    above, with a 40× margin for the fetched-but-unretained tail/prefetch
    buffers; 192 slots × 12 MiB = 2.3 GiB of the 4 GiB gate (`ZO_VIX_EVAL_
    MAX_BYTES`) vs 192 × 32 MiB = 6 GiB before. Test `row_id_evaluations_
    declare_the_measured_workspace` pins the 192-slot fit; workspace_tests
    23/23.
  - Battery, fresh `.200` pods (empty caches, all remote) vs the same leg
    on fresh `.199` pods three hours earlier — the only apples-to-apples
    cold comparison. A48 body per follower (10 followers, Orbit
    `io_accounting`): EVAL-gate **`wait` 1,519–1,633 s → 468–983 s
    (−57 %)**, `active` 1,662–1,853 → 1,869–2,486 s (more concurrent IO,
    each read slower), follower index phase 13.1–13.8 s → **8.6 med / 15.4
    max** (one straggler; eight of ten under 10.6 s), leader `idx_took`
    19.0 → 15.4 s, wall 55.9 → 49.3 s. Reads per file are UNCHANGED at
    13.1 (309,839 fetches / 23,593 files, 26.4 GB) — `.200` changed
    admission only. Other cold r1 index phases (`.199` → `.200`, leader
    ms): A48 pending token 26,663 → 12,978; `IN(3) AND match_all` count
    21,383 → 9,917; hist `svc = x` 23,488 → 9,272; `body = 'error'` dense
    18,397 → 9,180; L24 hist 9,247 → 7,521; B48 10,704 → 7,510; C24 7,544
    → 6,131; traces APM 4,163 → 3,653. Hits identical on every shape,
    no `is_partial`, no errors (26 rows).
  - Memo: A48 body r2 (11 queries later) → **0–37 fetches per follower**
    (132 total, 0.6 % of files — the files whose follower changed between
    runs), idx 142–1,860 ms (`.199` r2: 2,356–2,600 fetches, 3,832 ms).
    `zo_vix_result_cache_memory_usage` 213 MB after the battery (`.199`
    sat at 27 MB against the 100,000-entry cap); hits 158k of 331k
    requests fleet-wide including the cold r1 misses. The repeat's wall
    is 34 s = the residual scan, all of it.
  - Counters 15 min in (fleet sums): `eval_growth_timeouts_total` 0 and
    `fast_path_fallback_total{budget_refused}` 0 (`.199` at the same age:
    72 / 12 — the lower declaration did NOT trade admission for growth
    refusals); fallbacks `skipped_file` 150,553 + `aggregate predicate
    requires residual filtering` 12,403 + `unservable` 1,280 (the new
    item-11 reason: FTS-only equality dropped from the index conjunct).
  - Organic, fresh pods vs fresh pods (`/tmp/pop.py`, battery excluded):
    `.200` 16:52–17:06Z (n = 28) vs `.199` 16:17–16:42Z (n = 73): ALL p50
    938 vs 1,294 ms, p90 10.8 vs 16.8 s, max 15.4 vs 26.7 s;
    `agg+match_all` 5.5 vs 11.2 s, `hist+eq` 4.9 vs 11.8 s. Low n —
    re-run with warm caches (2–3 h) before judging.
  - Not changed: `ZO_VIX_EAGER_TAIL_BYTES` stays 768 KiB (7 sampled
    sidecars, 42–466 MB; only the measured apisix file overshoots the tail
    by 35 KB = one extra cold round trip). Decide from a cold-open count
    on `.200`, not from the sample. Item 6 (warming the aggregate pass)
    stays dropped: disk-cache churn risk, and the memo covers repeats.
  - Next (in order): (a) **round trips per file** — 13.1 reads per file
    for a 9-leaf AND; batch every leaf's dictionary lookup into one wave
    and every leaf's postings into one wave (target ≤ 4 waves/file). The
    `active` rise under the higher admission says the per-pod GET rate
    (~3k/s during the index phase) is the ceiling now, so fewer reads is
    the lever, not more concurrency. (b) Item 4, residual filtering inside
    the index for aggregates: the 30–40 s scan is 65–70 % of a cold A48
    and 100 % of a repeat. (c) Then the eager-tail decision.
- **2026-10-06 — `.201` engine: one dictionary wave for points + tokens,
  residual filtering INSIDE the index for aggregates (next items (a) and
  (b) above). vix-arch `76abfc1e4` + `4530dd8d3` (+ bench `b1a44670f`),
  `release/vix-20261006-201` `4a3aa2851` = `.200` + those, byte-identical;
  image binary `dc35b548…`, arm64 image id `28a613d8…` (ECR push pending
  the `eks-prod` SSO login). vortex_index 364 / search 1121 tests.**
  - **Where the time really went on `.200` (facts, not the report's
    model).** Per-GET latency on the pod is ~83 ms (`active` 2,468 s /
    29,893 fetches on the slowest follower) with only 0.5–12 % of it in
    the fetch-permit queue; a cold file's index time is round trips ×
    that: open 1 + dictionary 2 + terms cells 1 + plist record 1 = 5
    waves ≈ 0.44 s/file, 35 rounds of 64 (eval_concurrency per query is
    192 in prod, `ZO_VIX_SEARCH_CONCURRENCY`) = 15 s. The scan phase
    (32–54 s) is per-file too: `VixDocs::open_ranged` re-reads every
    file's docs footer (256 KiB probe + a 759 KB `NeedMoreData` layout on
    the 118 MB sample, 2–3 MB on 4 GiB files, two round trips) and
    decodes the predicate columns' chunks — **~26 GB / 6.7k requests per
    follower for 9,846 candidate rows** (`zo_storage_read_bytes` delta on
    one pod across a never-seen cold A48: +28.9 GB / +36.7k requests, of
    which the index phase is ~2.5 GB / 30k), under DataFusion's 63
    partitions. Both passes paid for the same `has_skipped` superset, and
    the repeat paid the scan again because a superset never memoises as
    exact (`.200` r2: idx 0.6 s, scan 28 s).
  - Also found: each querier runs a permanent background downloader —
    `zo_file_downloader_normal_queue_size` 10,007 on a 13 h-old pod, pod
    RX **5.56 TB in 13 h (~120–150 MB/s continuously)**, disk cache 997
    GB of 1.5 TB; every cold 48 h query enqueues ~148 GB of whole-file
    downloads per follower (`background download admission … accepted=
    2219 accepted_bytes=148 GB`). It shares the S3 client and the node NIC
    with query reads. Not changed here; it is the first suspect for the
    83 ms per GET and worth its own measurement (pause it on one pod, time
    the same cold query).
  - Engine (a), `vortex_index/reader.rs`: `eval_and` sizes the token
    dictionary plan from the tail-resident directory first (IO-free
    `walk_point_targets` / `planned_point_bytes`); up to
    `MERGED_POINT_WAVE_MAX_BYTES` = 256 KiB (the prod shape plans ~5
    blocks / 40 KB) the named point leaves and every token leaf go out in
    ONE block fetch; a large plan (an unscoped match_all over a 1,000-field
    schema reads a block per field) keeps points-first, so the wide-schema
    guard holds. Prod sidecar `and_io_bench` (1 ms latency): A48 4 → 3
    eval waves, same 12 reads / bytes; absent-token shapes 2 → 1.
  - Engine (b), `search/src/vix/residual.rs`: an aggregate over a
    superset bitmap point-reads the candidates' predicate columns through
    `VixReader::detached_docs` (the docs footer lives in the detached
    handle and drops with it — the cached reader's ~1 MB metadata tier is
    untouched; test `detached_docs_point_read_retains_nothing_in_the_
    reader`) and evaluates the WHOLE condition with
    `IndexCondition::to_physical_expr` — the scan branch's own expression,
    identical semantics by construction (`match_all` = `ILIKE '%v%'` over
    the present full-text columns, `_` wildcard included). The collectors
    run on the exact bitmap, the per-file result memoises as exact, the
    file never reaches the scan. Bounds: 4,096 candidates / 8 docs chunks
    per file, string-typed columns only, Regex/NumericCmp refused — every
    refusal is the old fallback with a counted reason (`residual: …`).
    Query-side shortcuts (`reader.count(&query)`, single-term plist
    cursors) are off under a refinement. Prod sidecar
    (`prod_file_residual_histogram_cost`): superset 8 → exact 8 rows,
    index 4 waves + docs footer 2 + chunks 2 = **~9 waves / 4.2 MB per
    cold file**, no second pass, no scan phase. Model for a cold A48: ~9
    × 83 ms ≈ 0.7 s/file, 2,300 files / 192 ≈ 12 rounds ≈ 9 s total vs
    49–64 s; repeats ≈ 0.5 s (exact memo).
  - Contract change (tests rewritten, not re-pinned): `multiword_full_
    text_aggregates_are_refined_to_the_exact_residual_rows` and
    `review_wave_b_skipped_condition_is_refined_to_the_exact_count`
    replace the two tests that asserted "multi-word match_all / fts
    equality aggregates refuse and go to the scan"; expected rows come
    from DataFusion's own `LIKE` evaluation of the candidates.
  - Pre-leg battery on `.200` pods 14 h old (`ops:/tmp/battery_pre201_
    on200.jsonl`, the 48 h window re-anchored): the window's entries are
    out of both caches again — A48 r1 49.4 s (idx 18.4 / scan 30.9), r2
    29.0 s (idx 0.6 / scan 28.3); `IN+match_all` 22.4 s (idx 22.1);
    hist eq 10.9 s (idx 10.8); `str_match` 24 h 22.9 s (scan 21.6).
  - Visible next steps from the same traces: the docs footer open is 2
    round trips (256 KiB probe then the layout) and the candidate chunks
    come in 2 waves (13 segment reads > the 8-in-flight limit) — a 1 MiB
    initial read for data objects and a larger in-flight window would make
    the residual 3 waves instead of 4; the per-file `to_vix_query` lines
    are debug now (were ~4,500 INFO lines per follower per 48 h query).
- **2026-10-06 08:44Z — `.201` querier rollout, ROLLED BACK 08:58Z (12 min
  live): the in-index residual filter regressed cold index time on
  production file geometry.** GitOps #602 (`634d2683`), image OCI index
  `28a613d8…`, arm64 manifest `0e29246d…`, binary `dc35b548…`; RS
  `67cf467c66` 08:44:20Z, Healthy 08:46:29Z, 10/10, 0 restarts. Rollback
  #603 (`e1f30678`) Healthy 08:57:59Z, 10/10 `.200`. Results stayed
  CORRECT throughout (every battery row's hits identical to `.200`).
  - Battery on the fresh `.201` pods (`ops:/tmp/battery_post201.jsonl`,
    20 rows before the rollback) vs the `.200` pre-leg, wall / idx /
    scan ms: A48 body **71,272 / 65,359 / 5,601** (pre 49,403 / 18,424 /
    30,883); A48 no body 80,408 / 74,105 / 5,797 (40,130 / 13,125 /
    26,791); A48 pending 37,312 / 36,447 (17,189 / 16,813); B48 31,641 /
    30,748 (7,535 / 7,018); L24 hist 18,884 / 18,608 (10,250 / 10,037);
    `IN+match_all` 17,887 / 17,653 (22,432 / 22,125); `str_match` 13,733
    (22,874). The scan phase did collapse (30.9 → 5.6 s); the index
    phase tripled, and not only for residual shapes.
  - Per follower on A48 r1 (raw follower log, re-read 10-06 10:xxZ —
    the first reading of this entry misparsed the attribution and was
    wrong): the aggregate pass **answered 2,224 of 2,337 files exactly**
    (histogram hits 9,025, correct); only 113 fell back (`residual: too
    many candidate chunks` 96, `budget_refused` 17) and the scan branch
    handled those 118 files in 3 s. The regression is the pass's
    THROUGHPUT: **46,577 reads / 6.2 GB in 65 s = 19.9 reads / 2.85 MB
    per file — exactly what the sample bench predicted** (12 index + 2
    docs-footer + ~6 chunk reads; 1.1 + 1.0 + 0.7 MB). Per-read latency
    92 ms (`active` 4,291 s), so ~8 serial waves ≈ 0.7 s per file, and
    65 s wall for 2,337 files means only **~25 evaluations were doing IO
    at any moment** while ~100 held slots: gate `wait` 3,040 s = 1.3 s
    per file of admission queueing, plus growth waits inside the
    evaluation (fleet `eval_growth_timeouts_total` 323, `budget_refused`
    323). Aggregate modes declare the 32 MiB streaming workspace (3.5
    GiB admissible / 32 MiB = 111 slots, vs 192 for the row-id pass's 12
    MiB), and every residual grows its lease further (the detached docs
    open's 1 MB footer window and the decoded chunks are reserved through
    the reader's memory → `permit.resize`), 111 × several MB > the 512
    MiB growth headroom → 500 ms `GROWTH_WAIT`s and refusals. Decode CPU
    is the other suspect: 7 projected columns × ≤ 8 chunks × 7k rows
    per file ≈ 50 MB decoded per file, ~117 GB per follower.
  - What was NOT the cause (corrections): prod `logs/default` files are
    **120–130 MB** (`aws s3 ls` 10-03/12, 10-05/12), the same geometry
    as the 118 MB sample (7,248-row chunks from the 16 MiB byte clamp),
    not 4 GiB / 65,536-row chunks; the residual's docs reads ran under
    the default 1 MiB coalescing (the 16 KiB policy is scoped to the
    terms-cell read); the skip-rate bail did not fire. The sample bench
    was predictive per file; what it cannot show is admission under
    fan-out — that needs a local many-file run against the real gate.
  - Also seen: (1) every rollout starts a download storm — `zo_file_
    downloader_normal_queue_size` 10,007 → **92,657** right after the
    `.201` pods came up (each new pod enqueues its 24 h warm-up,
    `ZO_WARMUP_CACHE_HOURS=24`; ~1.5 TB fleet-wide), so every "fresh
    pods, cold caches" battery of 10-05/10-06 ran against a saturated
    downloader — measure ≥ 1 h after a rollout, or pause the downloader
    on one pod for the A/B. (2) A `.200` pod (`5f97c86f96-ftkhr`) was
    EVICTED at 04:51Z by node memory pressure (kubelet: "node was low on
    resource: memory", exit 137), not a querier OOM; replaced
    automatically. One querier per m8g.4xlarge node — check what else
    shares those nodes.
  - State: vix-arch keeps both commits (the residual is correct and
    tested; it is not in a shipped image). `.200` is prod. Redesign
    before any re-rollout, in the order of the measured causes: (a)
    admission — bitmap collectors (SimpleCount / SimpleHistogram over a
    bitmap, zone-fold) decode no docs column and must declare the row-id
    workspace (12 MiB) plus a MEASURED residual allowance, not the 32 MiB
    streaming workspace, and the residual's own reservations must fit
    that allowance so the growth path is not used; (b) progressive
    columns — single-column conjuncts first (`body = v`), then the
    `match_all` disjunction column by column for the rows still
    undecided (a row is TRUE at its first matching column), so A48 decodes
    1 column instead of 7 and reads 1–2 chunk segments instead of ~6;
    (c) the docs footer in ONE round trip (the detached open's initial
    read sized for a 2,000-column layout, ~1 MB, instead of 256 KiB +
    `NeedMoreData`), −1 wave per file at the same bytes; (d) acceptance =
    a local many-copies run of the sample under the real `EVAL_BYTES`
    gate measuring admitted concurrency and growth refusals, not only the
    per-file bench. The dictionary-wave merge (`76abfc1e4`) is
    independent and unmeasured in prod (the battery was dominated by the
    residual); ship it with the redesign, not alone.
- **2026-10-06 09:2xZ — `.202` ingester/router hot patch (metrics
  ingest valve), carried onto vix-arch as `655c94aa2`.** GitOps #604
  (`d617360`): `ZO_INGEST_METRICS_DROP="true"` in obs-env + ingester/
  router alias `.178` → `v0.93.0-vix-20261006.202` (`release/vix-
  20261006-202` `2a0c819ad` = `.178` + the valve, nothing else). The
  valve acks every metrics ingest entry point (OTLP gRPC/HTTP, remote-
  write, `_json`) with its normal success response before parsing; default
  off. Cause: the e2b `service-metrics` collectors went 27× on 10-05
  13:00Z (42M → 1,147M records/h, 467 streams), ingesters pinned at HPA
  max. The commit was NOT on vix-arch (a `.178`-based branch), so the next
  ingester/router image cut from vix-arch would have dropped it silently —
  cherry-picked clean (24 lines, core check ok). Rollout at 09:40Z: sts
  ordered, `-4/-3/-2` on `.202`, `-1` draining, `-0` (the 11.4-core pod
  still taking the flood) next; the old pods take minutes to flush their
  WAL. Compactor stays `.181`, querier `.200`.
- **2026-10-06 — warm-up and downloader, measured (owner question: "turn
  the 24 h warm-up off, warm on demand?").** Keep `ZO_WARMUP_CACHE_HOURS=
  24`: it is metadata-only — each fresh pod opened its ring share of
  2,401–2,962 sidecar footer tails in 89–113 s (`[WARMUP] done`, 0
  failed) and left the downloader queue at 0; it is what makes a post-
  roll index phase warm. The 1.5 TB "storm" is the SCAN branch's whole-
  file downloads (`cache_files` enqueues every data file a query touches:
  2,219 files / 148 GB per follower per cold 48 h query), and it buys
  nothing measurable: on 14 h-old pods only 3–33 % of that window's
  files were still on disk (newer downloads evict them), and the two
  followers with the MOST disk-cached files had the slowest scan phase
  (32.8 / 27.9 s vs 12–13 s). A/B on the same fresh pods, downloader
  idle vs busy (never-seen 48 h windows): per-GET **80.6 vs 76.4 ms** —
  the downloader does not slow queries; ~80 ms is S3's own latency at
  ~300 concurrent range GETs per pod (bastion single-threaded 64 KiB
  range GET p50 33 / p90 105 ms). Follower index phase on fresh `.200`
  pods: 5.6–6.4 s median / 6.9–12.2 s max. Candidate: `ZO_DISK_CACHE_
  MAX_AGE_DAYS=2` (now 0 = unlimited) so ad-hoc historical queries read
  S3 directly instead of enqueuing 148 GB each; a querier env line, next
  rollout.
  - **The scan phase is decode-bound, not IO-bound**: 12–33 s per
    follower with DataFusion peak 4–6 GiB, independent of disk-cache
    hits — 65,536-row string chunks × 9 projected columns decoded for a
    few candidate rows per file. That is why `.201` (the same decode moved
    into the index phase, under its concurrency and 16 KiB coalescing) got
    worse, and why the lever is decoding LESS: progressive single-column
    residual (`body = v` first, the multi-column `match_all` only for
    survivors), smaller docs row groups for new files (8,192 vs 65,536 —
    compression cost to measure), and the disk-hit-slower anomaly to
    explain (pool throttling?).
- **2026-10-06 — the gate was charging phantom memory: footer parses ×64.
  Fixed with measured bounds; residual redesigned; `.203` candidate =
  `.200` + dictionary wave + residual + valve (vix-arch `e502a996d`,
  `84e84980d`; release `release/vix-20261006-203`, byte-identical).**
  - Measured with a counting global allocator on two production pairs
    (`vortex_index/examples/residual_alloc_probe.rs`; `VixReader::
    memory_peak()` is the new gate-visible high-water mark):

    | step | real heap peak | gate charge before | after |
    |---|---|---|---|
    | ranged open, both puffin footers (2,233-col logs) | 1.08 MB | 29.2 MB | 6.1 MB |
    | same, 539-col apisix (571 MB data) | 0.43 MB | 11.1 MB | 3.7 MB |
    | row-id eval (dict + terms + plist) | 2.5 MB | — | — |
    | detached docs footer, 996 KB window | 4.6 MB | +36.8 MB | +4.5 MB |
    | same, apisix 476 KB window | 2.3 MB | +21 MB | +4.5 MB |
    | stats blob decode 139 KB / 1,021 KB | 1.1 / 8.7 MB | 8.9 / 65 MB | 2.2 / 16.4 MB |
    | 8 rows × body column / × 7 columns | 2.8 / 4.6 MB | not charged | not charged |

    `metadata_memory_bound(encoded) = encoded × 64 + 64 KiB` was applied
    to every puffin footer, every Vortex footer open and every stats
    decode: a 14–27× over-charge, held as PENDING for the few ms a parse
    runs. Every ranged open grew its lease by ~30 MB (192 slots → 5.6 GB
    of transient demand on a 4 GiB gate — the `wait` column of every
    battery since `.198`), and `.201`'s residual added +37 MB per file on
    top of a 32 MiB declaration → 111 × 37 MB against the 512 MiB growth
    headroom = the 500 ms `GROWTH_WAIT`s, 323 timeouts/refusals, ~25
    effective concurrency. Now `footer_memory_bound` ×8 (puffin ≤ 2.2×,
    Vortex 4.6–4.8× measured → 1.7× margin) and `stats_memory_bound` ×16
    (7.9–8.5× measured → 1.9× margin). The decode of point-read columns
    is not reserved at all (vortex conversion path) — the residual's only
    gate cost is its footer.
  - Declarations: SimpleCount / SimpleHistogram over a bitmap decode no
    docs column → row-id workspace 12 MiB + `EVAL_RESIDUAL_BYTES` 8 MiB
    (the 1 MiB detached footer read × 8) = 20.2 MiB with bitmaps; a
    histogram whose file span lies in ONE bucket of the grid (the UI
    case: 1 h buckets, files spanning minutes) declares no `_timestamp`
    column (it never decodes one — `file_span` is an admission input
    now); streaming collectors keep 32 + 8 MiB. Growth per evaluation
    (`EVAL_GROWTH_ALLOWANCE_BYTES` 12 MiB, measured 6.1 open / 10.6
    residual) is charged on top by design, so the real demand is slots ×
    (declared + growth): row-id 192 × 18 MB = 3.5 GB, residual histogram
    ~130–177 × 31 MB.
  - Residual (`search/src/vix/residual.rs`): `to_vix_query_detailed`
    returns a `ConjunctVerdict` per conjunct (Exact / Superset / Phrase /
    Skipped; `to_vix_query`'s `has_skipped` unchanged) and only non-Exact
    conjuncts are re-evaluated — `service_name = x`, decided by the
    postings, is never read again. Conjuncts run narrowest-first over the
    surviving rows, ONE read each: `body = v` over the superset rows (3
    reads, 250 KB on the sample), then the `match_all` disjunction over
    all present full-text columns of the survivors in one batch (~2 MB;
    column-by-column saved bytes but cost a ~80 ms round trip per
    column). The detached docs handle memoises every fetched window
    (`PrefetchedWindows::recording`) and opens its footer in ONE 1 MiB
    read (`DETACHED_DOCS_FOOTER_READ_BYTES`; was 256 KiB + `NeedMoreData`).
    Skipping all-NULL columns via chunk stats was measured and dropped:
    the data `stats` blob starts 74 KB / 954 KB before the 128 KiB data
    tail on both files, so a cold stats decode is its own round trip.
  - Per cold file now (prod sample, `prod_file_residual_histogram_cost`):
    open 1 → dictionary 1 → terms 1 → plist 1 → docs footer 1 → body 1 →
    fts columns 1 = **7 waves**, 26 reads, 4.0 MB, gate peak 10.4 MB
    (`.201`: 9–10 waves, 20 reads, 2.85 MB, gate peak 66 MB; `.200`: 5
    waves + the 30 s scan phase). Model for a cold A48 at ~150
    concurrency: 2,337 / 150 × 7 × 83 ms ≈ 9 s, no scan phase; repeat
    ≈ 0.5 s (exact memo). Bytes per follower ≈ 9.4 GB vs 2.5 + 26 GB
    today.
  - Acceptance (the fan-out evidence `.201` lacked):
    `prod_file_residual_fanout_under_the_gate` runs as many concurrent
    residual histograms of the prod file as the REAL gate admits
    (`ZO_VIX_EVAL_MAX_BYTES=4 GiB`, 192 slots → **177 admitted**): all 177
    exact, **0 growth timeouts**, wall 308 ms, max reader peak 10.4 MB.
    `residual_refined_histogram_completes_inside_its_declaration` pins
    the per-evaluation invariant (completes under declared + allowance
    with no growth wait; peak ≤ allowance) on a ranged > 1 MiB docs
    fixture. vortex_index 364, search 1122 + 2 ignored probes.
  - Not changed, worth knowing: the row-id workspace (12 MiB) is still
    well above the measured real peak (2.5 MB) and the owned reservations
    already cover most of it — lowering it needs a measurement on a
    30 M-row file's dense postings, not a guess.
  - **Querier instability on `.200` today is co-location, not the
    engine**: 2 OOMKills (09:51Z, 10:13Z) + 3 kubelet evictions, every
    eviction message "Container querier was using 15.8–16.3 GiB, request
    is 10Gi, has larger consumption of memory". 8 of 10 querier nodes
    (61 GiB allocatable) also carry an `obs-compactor` (24 Gi request /
    60 Gi limit, 4–20 GiB RSS since the 18 × 5-slot consolidation) and 2
    an o2 querier (52 Gi limit) — 136–156 GiB of limits per node. The
    querier's 10 Gi request makes it the designated victim; each eviction
    is a fresh pod with an empty 2 TB disk cache + reader cache (and a
    warm-up storm). Fix in PR #605 with the `.203` image (one rollout):
    request 10 Gi → **18 Gi** (steady RSS 14–17 GiB), so the over-request
    compactor — a restartable merge — is evicted instead. Compactor +
    querier requests (42 Gi) still fit one node; a hard querier↔compactor
    anti-affinity would need 28 nodes on a 21-node pool (Karpenter spot) —
    the next step if evictions continue.
  - `.203` image built from `release/vix-20261006-203` `f611d2743`:
    binary `0fa9920f…`, OCI index `61539218…`, arm64 manifest
    `6c9d14f4…`, pushed. GitOps PR #605 (image + request) is OPEN, NOT
    merged — rollout timing after a same-day rollback is the owner's call;
    post-leg battery ≥ 1 h after the rollout.
- **2026-10-06 13:04Z — `.203` querier rollout (owner: "直接上然后测试").**
  GitOps #605 (`91dd9763`: image + memory request 18Gi); RS rolled
  13:04–13:07Z, 10/10 `.203` Running/Ready at 13:07:16Z (Pending pods
  placed within a minute, no Karpenter wait), 0 restarts, requests 18Gi
  on all 10, RSS 8.3–10.1 GiB fresh. Rollback: `.200`.
  - Pre-leg on 4 h-warm `.200` pods (`ops:/tmp/battery_pre203_on200.
    jsonl`, 48 h window ending 13:00Z): A48 body **38.2 s** (idx 14.7 /
    scan 23.4) → repeat **23.8 s** (idx 0.4 / scan 23.3); A48 no body
    32.4 → 20.7 s; B48 8.9 → 1.0 s; hist eq 24 h 7.4 → 0.2 s.
  - Smoke on the fresh `.203` pods 13:07–13:09Z, INSIDE the post-roll
    download storm (queue 27k, per-GET 95–120 ms vs ~83 normally): A48
    body **33.4 s** (idx 30.0 / scan 3.3) → repeat **2.8 s** (idx 1.4 /
    scan 1.1); A48 no body 28.9 → 2.6 s; B48 14.2 → 1.1 s; hist eq 7.9 →
    0.2 s. Hits identical to the pre-leg on every shape (48 / 48 / 110 /
    48), no partial.
  - Aggregate pass per follower (A48 body r1, Orbit `io_accounting`): idx
    18.4–30.0 s, **2,444–2,694 files of which 87–120 fell back**
    (`residual: too many candidate chunks`, ~4 %; those 100 took the
    scan branch in 3.3 s), **19.6 reads / 3.1 MB per file** (bench 26 /
    4.0), 7.3–8.3 GB per follower, gate `wait` 1,132–1,911 s ≈ 0.5 s per
    file (the gate is shared with the 24 h warm-up still running at
    13:07 and organic traffic → ~90 effective concurrent evaluations, not
    177). Fleet counters after the smoke: `eval_growth_timeouts_total`
    **2**, `budget_refused` **2** (`.201`: 323 / 323), `residual: too many
    candidate chunks` 4,280, no other residual refusal.
  - Reading: the scan phase is gone (23 s → 1–3 s) and repeats are 8×
    faster; the cold index phase is where `.200`'s was at the same pod
    age (storm-bound per-GET). Post-leg scheduled 14:01Z (`ops:/tmp/
    battery_post203.jsonl`) once the storm drains; organic comparison
    after that. Candidate follow-ups from the counters: RESIDUAL_MAX_
    CHUNKS 8 → 16 (the 4 % whose ~9 candidates land in > 8 chunks; one
    more segment per chunk in the same wave).
  - **Post-leg 14:01Z, pods 1 h old (`ops:/tmp/battery_post203.jsonl`)
    vs the warm-`.200` pre-leg, wall / idx / scan ms, hits identical on
    every row:**

    | query | `.200` warm r1 · r2 | `.203` r1 · r2 |
    |---|---|---|
    | A48 body | 38,153 / 14,673 / 23,362 · 23,798 / 392 / 23,342 | 1,939 / 636 / 1,226 (memo from the 13:07 smoke) · 1,327 / 507 / 744 |
    | A48 no body | 32,400 / 11,894 / 20,379 · 20,742 / 658 / 20,020 | 1,565 / 626 / 875 (memo) · 2,077 / 1,073 / 930 |
    | A48 pending token | 13,715 / 13,376 · 548 | **9,655 / 9,474** (cold) · 420 |
    | B48 | 8,880 / 8,273 · 954 | 1,026 / 212 (memo) · 925 |
    | C24 | 4,615 / 4,545 · 319 | **1,200 / 1,068** (cold) · 481 |
    | L24 hist | 6,519 / 6,331 · 400 | **5,368 / 5,225** (cold) · 318 |
    | `IN(3)+match_all` count | 9,525 / 9,367 · 489 | **7,127 / 6,996** (cold) · 276 |
    | `body = 'error'` dense | 10,073 / 7,514 / 2,516 · 2,763 | **6,435 / 4,484 / 1,899** (cold) · 2,347 |
    | hist eq 24 h | 7,394 / 7,312 · 223 | 224 (memo) · 238 |
    | `str_match` 24 h (scan-bound) | 5,122 · 4,104 | 4,939 · 5,782 |
    | traces APM 1 h | 2,579 · 1,474 | 2,803 · 1,165 |

    The smoked shapes' r1 are exact-memo hits (the result cache now holds
    the superset shapes as exact); the never-smoked cold shapes are 18–74
    % faster on 1 h-old pods than on 4 h-warm `.200` pods.
  - **Truly cold A48 on `.203`** (never-seen 48 h window ending 10-04
    00:00Z, 14:08Z, downloader queue back at 40k from the post-leg's own
    windows): wall **31.9 s** (idx 29.0 / scan 2.8) → repeat **4.3 s**.
    Per follower: **9 of 10 finish the exact aggregate in 11.9–16.6 s**
    (19.2 reads / 2.8 MB per file, 96 % files exact, per-GET 38–44 ms
    with ~35 % disk hits, gate `wait` 0.3–0.45 s per file); the wall is
    one straggler at 28.8 s whose pod spent **4,905 s in the fetch-permit
    queue** (`queue_us`; the other nine 38–861 s) — the background
    downloader on that pod (40k queued whole-file downloads) competing
    for the 512 fetch permits. That is the downloader's measured cost:
    not per-GET latency (10-06 A/B: 80.6 vs 76.4 ms) but permit
    contention on the pod with the deepest queue, +16 s on this query's
    wall. `ZO_DISK_CACHE_MAX_AGE_DAYS=2` (stop whole-file downloads for
    ad-hoc historical windows) is now justified by a number.
  - Organic 13:20–14:25Z on `.203` vs 11:30–12:55Z on warm `.200`
    (`/tmp/pop.py`, n = 87 vs 90): index phase ALL p50 **270 vs 651 ms**,
    p90 3.3 vs 6.4 s, max 36 vs 59 s; `select+match_all` p50 155 vs 872
    ms; `select+eq` 257 vs 462 ms. Different class mixes — indicative.
  - Health 1.5 h in: 0 restarts, RSS 10.3–11.8 GiB, fleet
    `eval_growth_timeouts_total` 2, `budget_refused` 2 (both from the
    13:07 smoke inside the warm-up), no evictions (request 18Gi on all
    10). `.203` stays.
  - **Checked and cleared: `select+eq` total p50 1.6 s on `.203` vs 0.46 s
    on `.200`** (organic 14:00–14:40 vs 11:30–12:10). The class is
    `trace_id = x LIMIT 5000` lookups; the two populations differ: the
    `.200` sample was 109 lookups with ≤ 10-minute windows (~400 files,
    bloom phase 0.5 s), the `.203` sample 41 + 25 lookups with **24 h
    windows** (1,250–1,390 files per follower, 1,000+ `.bf` groups) — a
    caller-pattern change in the afternoon. Like for like, 24 h `default/
    traces` lookups on `.200` (10-06 03:00–05:00Z, pods 11 h warm, no
    battery): **p50 6.1 s / p90 7.9 s**; on `.203` 14:20–14:55Z (pods 1–2
    h old, downloader at 78k): 8.1 / 10.9 s — the fresh-pod tax (cold
    `BLOOM_FOOTER_CACHE`, fetch-permit contention), not the engine. Index
    time inside those lookups is 3–174 ms per follower.
  - **Where a warm 24 h trace lookup's 6 s go (next target, not the VIX
    index)**: per follower, the bloom phase takes **1.2–4.3 s** — ~1,000
    `(date, bloom_ver)` groups for ~1,250 input files, one block-row range
    GET per group, i.e. the `.bf` grouping covers ~1.3 files per bloom on
    `default/traces` (13k files/day) and a needle lookup costs ~1,000 GETs
    per follower, 10k per query; then the WAL `segments_scan` decodes
    **0.8–1.0 M records per follower** (100–122 segments) for one trace_id
    (0.4–1.7 s); the leader waits for the slowest follower. Levers: one
    `.bf` per stream-hour (24 groups instead of 1,000) in the assembler,
    and a per-segment bloom or needle index for the WAL scan.
  - Downloader note: the queue did NOT drain after the battery — 27k →
    7k (13:27Z) → 40k (14:08, the post-leg's windows) → **78k at 14:54Z
    with no battery running**: on fresh pods organic queries over the
    empty 2 TB caches enqueue files faster than they download (10 × the
    10k per-pod cap, saturated). Permit contention (`queue_us`) is a
    steady-state tax until the caches fill; `ZO_DISK_CACHE_MAX_AGE_DAYS=2`
    remains the candidate.
- **2026-10-07 — `str_match` on a large-dictionary field: verify the
  candidates' column instead of walking the field's whole dictionary
  (owner question: "字符串匹配的性能不是很高"). Working tree on top of
  `.203`'s `e48a58c07`; vortex_index 370 tests.**
  - The shape: `str_match(request.uri, 'thirdparty_webhook/email') AND
    str_match_ignore_case(request.body, 'asagent1')` on `apisix`, 33
    distinct runs on 10-06 (6 h windows). Both conjuncts are `Contains`
    leaves → dictionary WALKS (`scan_all_tokens`): every distinct value of
    the field is read and tested. Per-field dictionary split of a prod
    pair (`75128600352533422088ead`, 10-05 12Z, tail-only probe): sidecar
    460 MB, `dict_blocks` 312 MB of which **`request.body` 243.8 MB**,
    `request.uri` 8.2 MB, 535 term fields. The uri leaf is selective (90
    rows over 6 h) and short-circuits most files before the body walk;
    on every file where it matches, the body walk read ~244 MB (~3,700
    64 KiB blocks) to decide a handful of rows.
  - Change (reader, `eval_and` wave 2): a `Contains`/`Regex` leaf on a
    named term field whose dictionary is ≥ `ZO_VIX_WALK_VERIFY_MIN_BYTES`
    (16 MiB; 0 = always walk) is HELD BACK; after the cheaper leaves
    intersect, the candidates' values of that field are point-read from
    the docs column (detached handle: 1 MiB footer + the touched chunks'
    segments, one concurrent wave) and tested with the SAME matcher the
    walk uses (`WalkMatcher`, shared by both routes — raw-value term ==
    whole column value, nulls match neither way). Zero-IO cost model
    decides per file: walk = min(blocks × 64 KiB, block share of the
    `dict_blocks` blob); verify = 1 MiB + Σ touched chunks' dictionary
    share (≥ 64 KiB each); verify only when verify × 2 ≤ walk and verify
    ≤ 64 MiB (the segments grow the eval byte-gate lease; a refusal would
    scan the file). Declined → the walk as before (+2–3 round trips: the
    held leaf intersects separately). Lone walks, legacy files without a
    field page directory, fts fields (never term-typed) are untouched.
  - A/B on a prod `apisix` pair (`7512849656796176384cce7`, 10-05 12Z,
    284 MB + 230 MB, 828,379 rows, 197 docs chunks, `request.body`
    dictionary 124.0 MB exact / 90.2 MB estimated), `walk_verify_bench`,
    20 ms simulated latency, bits identical to the in-memory eval on
    every row; uri leaf as a `Contains` like prod:

    | uri needle → candidates (chunks) | walk route bytes · waves | verify route bytes · waves |
    |---|---|---|
    | `thirdparty_webhook/email` → 0 | 5.8 MB · 2.7 | 5.8 MB · 2.8 (short-circuit, no walk either way) |
    | `wdp_cc4cce…` → 16 (13 / 197) | 133.5 MB · 31.6 | **12.5 MB · 9.4** |
    | `thirdparty_webhook` → 48 (44 / 197) | 133.5 MB · 29.5 | **27.4 MB · 10.0** (estimate 21.6 MB) |
    | `preview/resolve` → 3,100 (196 / 197) | 133.5 MB · 29.9 | 134.2 MB · 32.8 (declined → walk) |

    Of the verify route's bytes 6.2 MB is the uri walk itself (8 MB
    dictionary, 125 blocks, paid on every file) — the remaining per-file
    floor of this shape; a substring on a path-like field has no other
    index route. Synthetic regression `tests/walk_verify.rs` (40k-row,
    10.4 MB near-unique dictionary): verified route 4.0 MB vs walk 10.4 MB,
    identical bits for ci/cs `Contains` and `Regex`; broad and lone
    conjuncts keep the walk; threshold 0 / oversize keeps the walk.
  - **Shipped as `.204` (owner: "这种思路是对的。执行把。").** vix-arch
    `6560a9db1` (engine) + `dce1ad09b` (backlog); release worktree
    `release/vix-20261007-204` `d9bd66721` = `.203`'s `f611d2743` + those
    two cherry-picks, source byte-identical to vix-arch (vortex_index
    370/370 in the worktree); image binary `7f4624728e14…`, ECR
    `v0.93.0-vix-20261007.204` OCI index `sha256:1a42abacf6a9…`
    (linux/arm64 `3b4719104227…`). GitOps #609 (`ce848d1`, kustomization
    only, server dry-run clean); RS `6f8564c57b` rolled 09:31–09:33Z,
    10/10 Running/Ready, 0 restarts, Synced/Healthy; RSS 6.4–10.7 GiB
    fresh. Rollback: `.203`; in-place off switch
    `ZO_VIX_WALK_VERIFY_MIN_BYTES=0`.
  - **Pre/post on the same sealed window (10-07 01:00–07:00Z, `ops:/tmp/
    battery_pre204_on203.jsonl` 08:5xZ on 10 h-warm `.203` pods, mostly
    disk hits; `ops:/tmp/battery_post204.jsonl` 09:34Z on 2-minute-old
    `.204` pods, 100 % remote reads). Hits identical (0 / 0 / 1 row), no
    partial. Index bytes = Σ follower `io_accounting` logical bytes:**

    | query (6 h) | `.203` r1 wall · idx · index bytes · ranges | `.203` r2 | `.204` r1 (cold remote) | `.204` r2 |
    |---|---|---|---|---|
    | prod: `str_match(uri,'thirdparty_webhook/email') AND str_match_ignore_case(body,'asagent1')` | 10.0 s · 9,991 ms · **20.05 GB** · 3,527 | 367 ms | 4.1 s · 3,707 ms · **1.64 GB** · 1,499 | **98 ms** |
    | wide: uri needle `thirdparty_webhook` (15,803 uri rows) | 9.7 s · 9,626 ms · **35.8 GB** · 5,180 | 654 ms | 7.4 s · 7,417 ms · **5.69 GB** · 2,881 | **43 ms** |
    | `count(*)` uri only (185 rows) | 690 ms · 524 ms · 665 MB · 258 | — | 345 ms · 302 ms | — |

    Per follower the prod query fell from 1.3–3.7 GB / 243–604 ranges to
    121–269 MB / 106–262 ranges (12.2× fewer bytes, 2.4× fewer ranges);
    the wide one from 2.2–4.6 GB to 0.39–0.88 GB (6.3×). The 185 uri rows
    are spread over most of the window's ~80 files (20–30 per hour), so
    on `.203` nearly every file walked the body dictionary; on `.204` they
    verify 1–3 candidate rows each. The wide shape's remaining 5.7 GB is
    the files where ~200 candidates touch most chunks (declined → walk,
    by design) plus the uri walks. Fleet counters 09:35Z: `residual:*`
    0, no `budget_refused` / `eval_growth_timeouts` series; `skipped_file`
    12k–40k per pod in 3 min of organic traffic — the next class
    (`str_match` on fts-only fields is skipped → scan, see below).
  - **Beyond this (owner question "还有其他办法优化这种查询速度吗"):**
    (1) order the remaining walks cheapest-first by the same zero-IO
    estimate (today SQL order; only matters for two sub-16 MiB walks in
    the wrong order); (2) `str_match` on an fts field (`default/logs`
    `body`) is SKIPPED today → whole-file scan (`skipped_file` above):
    walk the token dictionary (a far smaller vocabulary than raw values)
    as a superset and verify candidates — the same mechanism; (3)
    anchored shapes (`LIKE '/thirdparty_webhook/email%'`) map to a
    `Prefix` range instead of the 8 MB uri walk, the per-file floor that
    is left; (4) stop raw-indexing `request.body` (1.6 M distinct ~150 B
    values per file serve only `body = '<whole json>'`): bloom-only or
    fts would cut the apisix sidecar ~53 % and every merge/build with it,
    with `str_match(body)` then served by candidate verification from the
    other conjuncts; (5) a trigram index for substring search on chosen
    fields — the only route that makes a LONE `str_match` cheap.
- **2026-10-07 — SHIPPED as `.205` (15:20Z): `str_match` on a full-text
  field is served through its tokens (owner: "优化引擎部分的str_match";
  trigram deferred); apisix `request.body` made full-text (15:24Z).**
  vix-arch `7a15e40bf` → `edb28d316` (search side only after the owner's
  simplification below). vortex_index 370, infra schema 38, search index
  99 / vix 207, workspace check clean.
  - Facts that shaped it: apisix has NO full-text field (`full_text_
    search_keys: []`, files `fts={}`, `match_all()` is rejected by the
    planner) — its `request.body` index is RAW values (202,840 distinct of
    828k rows, 135.6 MB raw / 124 MB encoded on pair `7512849656796176384
    cce7`); tokenized it is 578,496 distinct tokens / **8.4 MB**, 18.1 M
    postings. The tokenizer DROPS alphanumeric runs ≥ 64 bytes (tantivy
    `RemoveLongFilter` port, `ZO_INVERTED_INDEX_MAX_TOKEN_LENGTH` = 64):
    **3.9 % of request.body rows** (2.5 % of uri) carry such a run (JWTs,
    base64; p50 114 B, max 2,720 B), so a needle hiding inside one is not
    found through tokens — the same blind spot `match_all` has today (scan
    semantics `ILIKE '%v%'`, index narrowing = token AND).
  - **Owner decisions (2026-10-07):** `match_all` and `str_match` may be
    inexact; the 64-byte cap stays (index-build throughput). Hence NO
    per-file long-token accounting (`fts_long_token_skips` + `VixReader::
    fts_tokens_complete`, built in `d7bdb4936`/`7a15e40bf`, removed in
    `25efaed88`) — no writer change at all. And the explicit-key
    segment rule for `full_text_search_keys` (`body` ⇒ `request.body`,
    `4a389a10d`) was REVERTED (`cd8d5858c`): on prod data it is not worth
    it — `default` would gain +388 nested full-text fields (`callback_data.
    data.*` 159, `*.body` 104…), apisix +181 of which 169 are `request.
    querystring._safedog_*.log` scanner junk nobody searches. Keys stay
    exact; `request.body` is added to the full-text index by naming it
    (stream setting `full_text_search_keys: ["request.body"]`).
  - What remains (search layer, `Condition::fts_str_match_superset_query` +
    `str_match_token_superset`): on a `FieldCap::Tokens` field a `str_match`
    / `str_match_ignore_case` is no longer skipped (whole-file scan) — the
    needle is split by the tokenizer's own run rules: first run `Contains`
    (the row's token may extend left), last run `Prefix` (may extend
    right), interior runs and standalone non-ASCII chars exact
    `TokenAnyField`, runs < min token length not required, single run ⇒
    lone `Contains` — under `FullText{[field]}`, verdict `Superset` (filter
    re-applied; aggregates refine in the index via the existing residual,
    refused above 4,096 candidates as before).
  - Real-data smoke (`search::vix::query_regressions::prod_file_str_match_
    on_fts_body_cost`: the local apisix pair re-indexed with `request.body`
    fts at the 64-byte cap; 20 ms latency; row ids vs an in-memory column
    scan as ground truth, misses inside dropped tokens counted):

    | needle | truth (uri∧body / body) | lone body SELECT superset · index bytes · waves | uri∧body SELECT · bytes · waves | counts |
    |---|---|---|---|---|
    | `asagent1` | 0 / 0 | 0 · **7.1 MB** · 2.0 (raw walk was 124 MB · 27) | 0 · 13.0 MB · 6.3 | exact 0 / 0 |
    | `locale` | 4 / 46,870 | 46,870 (all true rows, 0 missed) · 8.7 MB · 3.8 | 4 (all) · 14.6 MB · 8.2 | uri∧body exact 4 (2.3 MB column); lone → `residual: too many candidate rows` → scan |
    | `task_status` | 0 / 0 | 6,452 (`task`+`status` co-occur) · 13.6 MB · 8.1 | 1 · 19.5 MB · 11.3 | uri∧body exact 0; lone → fallback |

    The lone `str_match` — the case `.204`'s walk-vs-verify cannot help —
    goes from a 124 MB raw walk to 7–14 MB of token dictionary per file.
    The uri∧body shape reads 13–20 MB (uri walk 6 MB + body token walk):
    the scoped token `Contains` is not yet eligible for `.204`'s hold-back
    (no named field) — next refinement, with footer-exact column sizes.
  - **Rollout.** `release/vix-20261007-205` `c17995b62` = `.204` + the
    vix-arch source delta (4 files, all `src/search`; tree byte-identical to
    vix-arch); image binary `0397d96f6d26…`, ECR `v0.93.0-vix-20261007.205`
    index `sha256:9242cb185caa…` (arm64 `404ecfc93229…`); GitOps #612
    (`f5de794`, kustomization only, server dry-run clean, admin merge like
    #609); RS `78dfd6597f` rolled 15:18–15:20Z, 10/10 Ready, 0 restarts,
    Synced/Healthy. Rollback: `.204`. Then `PUT /api/default/streams/
    apisix/settings?type=logs {"full_text_search_keys":{"add":["request.
    body"]}}` at 15:24:09Z (200; the first read-back was the cached `[]`,
    propagated within 20 s). Verified: the merged sidecar `7513622220774604
    800d610.vxi` written 15:32Z carries `request.body` with `types:["fts",
    "cs"]`; compactors 18/18, 0 restarts, RSS 5–27 GiB, apisix merges and
    L0 builds flowing (L0 sidecars arrive with the compactor heal as
    usual).
  - **Pre/post on the same sealed window (10-07 07:00–13:00Z, `ops:/tmp/
    battery_pre205_on204.jsonl` 15:0xZ on 5.5 h-warm `.204` pods; `ops:/tmp/
    battery_post205.jsonl` 15:21Z on 1-minute-old `.205` pods, all-remote).
    Hits identical on every row. wall · idx · scan ms:**

    | 6 h query | `.204` r1 · r2 | `.205` r1 · r2 | index bytes pre → post |
    |---|---|---|---|
    | `str_match(error,'southamerica-east1')` TopN by pc_id (fts `error`) | 3,967 (439/3,495) · 3,029 (45/2,960) | **3,082 (2,804/164) · 267 (36/147)** | 325 MB → 5.6 GB (the `southamerica` Contains walk over every file's `error` token dictionary); scan phase gone; pre: 366/366 files fell back (`unservable` 208 + `skipped_file` 158), post: 0 fallbacks |
    | `SELECT * … str_match(body,'Sending deploy callback') LIMIT 100` | 6,042 (38/5,965) · 4,622 | 6,848 (5,434/1,392) · **1,591** | 0 → 1.45 GB; **still `is_partial`** on both: the scan cap kept 63 of 399 files (pre) / 63 of 370 (post) — see follow-up (A) |
    | `count(*) … str_match_ignore_case(body,'deploy callback')` | 4,273 · 4,452 | 4,709 (857/3,829) · 3,560 | dense tokens: the superset exceeds `RESIDUAL_MAX_ROWS` per file → scan as before (+ the index work) |
    | `match_all('deploy callback')` count (unchanged path) | 6,613 | 4,491 | — |
    | apisix prod shape (unchanged until its files turn fts) | 1,931 · 423 | 2,135 · 66 | — |

    Reading: selective needles on fts fields are now index-served and
    memoised (the TopN's repeat 3.0 s → 0.27 s), dense needles neither gain
    nor lose, and the SELECT shape stays truncated for a reason that is NOT
    the index. Two follow-ups fall out of the numbers:
    - **(A) scan-cap accounting — tried as `.206`, ROLLED BACK (16:36–
      16:43Z, owner: "可以执行，但是要注意是否会有回归").** `apply_storage_
      scan_cap` charges every file left after the index step at its whole
      `compressed_size`, even when the index narrowed it to a row selection
      the scan branch late-materialises; `.206` (vix-arch `5c60cb69c`,
      reverted `093e34801`; release `6d2d13af2`, ECR `.206` index
      `sha256:32371b28c1c8…`; GitOps #613 → rollback #614) charged
      row-selected files by their point-read cost (2 MiB footer + one chunk
      share per touched chunk) and bounded dense shapes at 10 × LIMIT
      candidate rows. Pre (`.205` warm) → post (`.206`, 2-minute-old pods),
      `ops:/tmp/battery_pre206_on205.jsonl` / `battery_post206.jsonl`,
      windows ending 15:00Z: service + `str_match(body,'Sending deploy
      callback')` 24 h SELECT LIMIT 100 — kept 110 → 219 of 1,435 files,
      still `is_partial`, warm repeat **2.6 → 8.2 s** (scan 2.4 → 4.9 s);
      `str_match(body, …)` 6 h — kept 126 → 168 of 485, repeat 2.6 → 2.5 s;
      `str_match(error, …)` 24 h — not capped either way, 2 hits. The
      diagnosis was wrong in its unit: the scan branch costs **~20 ms per
      FILE** (footer open + plan + first segment) whatever the bytes — 126
      whole files took 2.4 s, 210 row-selected files 4.9 s — so admitting
      more files for the same partial LIMIT answer only lengthens the scan
      phase, and the whole-file byte budget was accidentally the right
      proxy (files are similar in size). What completes a LIMIT answer fast
      is **(C) newest-first early termination**: evaluate/verify files in
      `max_ts` order and stop once `limit` rows are in hand that are newer
      than every remaining file's `max_ts` (the condition-ALL SimpleSelect
      already prunes to the global top-N this way); for supersets that
      needs the exact per-file hit set first — the residual verification
      the aggregates use, run newest-first with a stop. Until then the cap
      stays as it was.
    - **(B) the first-run `Contains` walk.** `southamerica-east1` → `Contains
      (southamerica)` + `Prefix(east1)`: the Contains leaf walked the `error`
      token dictionary of all ~370 files (5.6 GB, 2.8 s) although the Prefix
      alone leaves few candidates. Extend `.204`'s hold-back to SCOPED
      `Contains` leaves (field from the single-field scope, token-level
      matcher: any alphanumeric run of the value contains the lowercased
      run) so the cheap leaves narrow first and the Contains is verified on
      the candidates' column; gate lone single-run walks on the token
      dictionary's size vs the file (a high-entropy body's token dictionary
      can rival the column scan).
    - Not a regression: `k8s_prod_ops_logs` (Orbit's own `str_match(body,
      trace_id)` lookups, 8.5 s / 13 h) runs with `idx_took = 0` — that
      stream never enters the index phase; its 6–11 s are the plain scan
      before and after.
  - What the apisix setting changes: new apisix files index `request.body`
    as tokens (sidecar −124 MB raw dictionary, +~30 MB tokens/postings),
    `match_all` is valid on apisix, existing files classify `NeedsRebuild`
    (`term` in input, `fts` in plan) and the `.181` compactor re-tokenizes
    them from `_source` — a bounded heal wave (~320 files/day of retention)
    during which `match_all`/`str_match` on those files take the scan path;
    `request.body = '<json>'` becomes a token superset on rebuilt files.

## 2026-09-29 — P2 shipped as `.176` and rolled back within 30 min: the per-stream side table stalled the shared meta DB (commit latency 40–60×, ingest 503s); ranges must live in the segment row
- What shipped (vix-arch `013c45010` + NATS retry `76fe754b5`, image
  `v0.93.0-vix-20260929.176`, GitOps #569 querier 06:15Z / #570 rest, all
  roles live 06:21Z): a `wal_segment_streams (segment_id FK ON DELETE
  CASCADE, stream, min_ts, max_ts)` side table, pkey `(segment_id, stream)`
  + index `(stream, segment_id)`; the ingester registers each segment as
  two autocommit statements (`INSERT wal_segments … RETURNING id`, then one
  multi-row `INSERT wal_segment_streams … ON CONFLICT DO NOTHING`, ~67
  rows/segment); the querier's `query_unbuilt` LEFT JOINs the range row
  for its stream (fail-open when absent).
- The pruning itself worked, measured on the same shapes before/after
  (leader-appended segments → loaded / zero-yield / `segments_scan` sum):
  logs count 1 h `.175` 329 loaded, **89 % zero-yield**, 1,469 ms →
  `.176` 52 loaded, 19 %, 680 ms; traces count 1 h 381 / 56 % / 1,742 ms →
  198 / 8 % / 1,189 ms; traces needle 1 h 218 loaded / 8 %. Wall times
  `logs count 1 h` r1/r2/r3 1,476 / 180 / 453 ms, traces count 2,281 /
  455 / 469 ms, needle 2,256 / 577 / 765 ms (measured 06:20–06:32Z while
  the incident below was already building — treat as indicative only).
- Incident (all UTC; Orbit `k8s_prod_ops_logs`, fields are dotted now:
  `"k8s.namespace.name"='obs'`): `slow statement` WARNs per 10 min were
  0–137 for the 7 h before (mostly compactor claims), then 06:20 **875**,
  06:30 **1,801**, 06:40 **2,349**; ingester `segment buffer full` 503s
  0 for 7 h → 06:30 1,328 → **06:40 145,762** (ingester-4 1,014 lines in
  3 min, ingester-2 44; each line one rejected write request — client
  retries/drops not measurable from here). `INSERT wal_segment_streams`
  2.3–5.8 s then 80 s, `INSERT wal_segments` 1.8–3.8 s, builder claim CTE
  and `has_claimable` **63–85 s**, `query_unbuilt` `EXPLAIN ANALYZE`
  **Planning 16,710 ms** / execution 6,540 ms with every buffer a shared
  hit (Gather Merge over a Parallel Seq Scan of the whole `wal_segments`
  heap: 9,596 outer rows, 13,408 buffers per call). `pg_stat_activity`
  waits: `LWLock:BufferContent` on `wal_segments` and
  **`IO:AuroraStorageLogAllocate`**; connections 440 → 723. Unbuilt
  segments 7,120 at 06:34.
- Aurora writer `obs-prod-2` (PostgreSQL 17.9, db.r7g.xlarge, Performance
  Insights OFF, `pg_stat_wal` unsupported), CloudWatch 1-min: CPU
  **28–39 % throughout** (one 75 % minute at 06:25) — not CPU-bound;
  **WriteIOPS 1.1 k → 5.0–5.9 k sustained** 06:30–06:45 (the DB already
  bursts to 8.5 k at the top of every hour, 12–13 k at 07:00 during the
  drain); **CommitLatency 0.5 ms → 19.8 / 4.9 / 31.1 ms** at 06:30 /
  06:40 / 06:45. Mechanism: log-allocation stall on the writer → every
  frequent committer (claims, heartbeats, registration) queues; heap/index
  buffer locks are held across the stalled `XLogInsert`, hence the
  BufferContent waits; ingester registration serialises the uploader, so
  the 512 MB segment buffer fills and appends 503.
- Rollback: GitOps #571 merged 06:45:30Z (querier → `.175`, compactor /
  ingester / router → `.174`), Argo synced 06:45:34Z, compactors and
  queriers replaced by 06:47, ingesters (STS, one at a time) 4/5 by 06:49,
  5/5 by 06:56. At 06:47:42 no statement > 2 s; 06:50 bucket 70 slow
  statements / 15 503s;
  07:02 unbuilt 1,232 (40 pending), `segment buffer full` 0 on every pod
  for 10 min, CommitLatency 0.4–0.8 ms, 0 new restarts. Slow statements
  stay ~700/10 min (mostly compactor claims) while the 7 k backlog drains
  at 1,192 building at once — recheck after the drain. Correction to the
  #571 commit message: `wal_segments` heap is 71 MB; the "595 MB" is the
  total with its 480 MB of indexes — no evidence the heap grew.
- Cascade exposure closed 07:05:02Z: the `.174` sweeper (`retain` 3,600 s)
  would have reached the `.176` cohort (13,341 Built segments, built
  06:19–07:00Z, all 1,155,931 side rows) at ~07:19Z and the DB-level `ON
  DELETE CASCADE` would have deleted ~480 child rows/s for ~40 min on the
  same DB. `ALTER TABLE wal_segment_streams DROP CONSTRAINT
  wal_segment_streams_segment_id_fkey` under `lock_timeout 2 s` succeeded
  first try (0 RI triggers left on `wal_segments`, no stalled statement).
  `DROP TABLE wal_segment_streams` (365 MB) ran 07:19:45Z under
  `lock_timeout 2 s`, first try, with 0 statements referencing it and every
  live image predating `013c45010` (36× `.174`, 10× `.175`); DB 5,217 MB.
- Source: `013c45010` reverted on vix-arch (`70d37e3c6`); the NATS retry
  stays. HEAD is releasable again.
- Why the write side tipped: baseline (post-rollback, 90 s window) whole-DB
  325 commits/s and 1,288 row writes/s; `wal_segments` alone 8.5 ins / 36
  upd (13 HOT → ~23 non-HOT/s, each rewriting **5 indexes**) / 8 del per
  second, **10 seq scans/s** (26 M lifetime, 731 G tuples read — the
  `has_claimable`/claim shapes `status = $1 AND ($4 <= 0 OR …)` never use
  `status_created_at_idx`), 176 M lifetime updates on 28 k live rows,
  480 MB of indexes incl. `wal_segments_object_key_idx` **192 MB, 0 scans
  ever**. `.176` added ~570 side rows/s (+44 % of all row writes) with 2
  index entries each, 67 scattered `(stream, segment_id)` insert points
  per statement (`stream_idx` 177 MB at ~40 % fill after 1.15 M rows) and
  an FK KEY SHARE lock on each fresh parent row. The 4–5× WriteIOPS jump
  is larger than the row count alone explains; Aurora's per-record /
  per-commit log accounting can't be attributed further without PI.
- Design decision (P2, second attempt — the `v2` spec below): **no new
  rows, tables, indexes or FKs in the meta DB per segment**; one extra
  unindexed column on the existing row, the `.175` SQL untouched, pruning
  in the querier.
  - Schema: `wal_segments.stream_ranges TEXT NOT NULL DEFAULT ''` via the
    idempotent `add_column` on boot (PG 11+ ADD COLUMN with a constant
    default is catalog-only, no rewrite; take it under `lock_timeout`).
    Value: JSON array aligned with the sorted `streams` array,
    `[[lo_s, hi_s], …]` with `lo_s = floor((stream_min − min_ts)/1e6)`,
    `hi_s = ceil((stream_max − min_ts)/1e6)` — second-granularity offsets
    rounded OUTWARD, so reconstruction `min_ts + lo_s·1e6 … min_ts +
    hi_s·1e6` can only over-include (a wrongly kept segment is today's
    zero-yield; a wrongly pruned one is missing data). Measured on prod
    with a temp table: 40 streams → ~480 B raw, ~300 B stored; a row is
    1,051 B avg today, `streams` 2,193 B raw → 543 B stored, so the tuple
    stays inline (< the 2 KB TOAST threshold). Exact-micros pairs would be
    1,460 B stored (random digits don't compress) and push rows to TOAST —
    rejected. `''` = unknown = fail open (old rows, old ingesters).
  - Ingester (`segment_wal::uploader::fold_frame_meta`): reuse the reverted
    `FoldedMeta { min_ts, max_ts, streams, stream_ranges }` fold (per-stream
    min/max over that stream's frames only, `lo.max(1)` clamp; its 4 tests
    are in `013c45010`), serialize offsets, bind ONE more parameter on the
    existing single-row `INSERT … ON CONFLICT (node_uuid, seq) DO NOTHING
    RETURNING id`. Zero extra statements, rows or index entries.
    `validate_for_add`: `stream_ranges` empty or `len == streams.len()`,
    each `lo <= hi`, `min_ts <= lo`, `hi <= max_ts`.
  - Row decode: `SegmentRow.stream_ranges: String` →
    `SegmentMeta.stream_ranges: Vec<(i64, i64)>` (micros, reconstructed);
    `SegmentMeta::range_for(stream) -> Option<(i64, i64)>` = `None` when
    the column is empty or malformed (log once, fail open).
  - Querier (`segments_scan::list_candidates`): after `query_unbuilt`,
    `retain(|m| m.range_for(stream).is_none_or(|(lo, hi)| hi >= start &&
    lo <= end))`, then `apply_query_cap`. The SQL `LIMIT MAX_QUERY_SEGMENTS
    + 1` still counts pre-prune rows: when the SQL page is full, report a
    shortfall regardless of the post-prune count (honest, rare — unbuilt
    is ≤ 1,200 during a drain, 7 k at this morning's worst, cap 10,000).
    Fix the direction while there: `ORDER BY min_ts ASC` keeps the OLDEST
    page and cuts the newest segments exactly when a backlog exists, then
    `apply_query_cap` keeps "the newest" of that page — make the SQL
    `ORDER BY max_ts DESC, id DESC` so both agree (sqlite too).
  - Mixed versions: old querier `SELECT *` into `FromRow` ignores the extra
    column; old ingester writes `''`; new querier on old rows fails open;
    every role's boot adds the column, so no ordering constraint.
  - Gates (before/after, same 5-min windows): `pg_stat_user_tables`
    wal_segments ins/upd/del per second unchanged (8.5 / 36 / 8 today),
    heap growth ≤ +40 %, CloudWatch CommitLatency ≤ 1 ms and WriteIOPS
    flat outside the :00 minute; `segments_scan` on logs count 1 h from
    329 loaded / 89 % zero-yield toward `.176`'s 52 / 19 %; traces count
    381 / 56 % → 198 / 8 %; no decode errors. Roll querier first (reads
    only), then ingester (writes), compactor last.
- Meta-DB audit 07:05–08:10Z (our database `obs20260818` on the shared
  cluster `obs-prod`; read-only apart from creating the `pg_stat_statements`
  and `pgstattuple` extension views in our DB — the library was already in
  `shared_preload_libraries`, so 37 days of per-statement stats since
  2026-08-23 03:03Z were waiting). Findings, ranked by evidence:
  1. **`system_settings` lookup, 252.7 M calls in 37 d = 79/s, 0 rows
     ever** (21 % of every statement in our DB, 2 blocks each). Trace ingest
     calls `db::system_settings::get_gen_ai_agent_mapping_config` twice per
     request path (`core/traces/mod.rs:562, 1149`) and `db::system_settings::get`
     caches only positive results ("Cache the result if found"), so a
     setting that does not exist is a DB round trip per request. Fix: cache
     the miss too (the `watch()` Put/Delete events already refresh or drop
     keys, so a tombstone is safe). Zero-risk, −79 statements/s.
  2. **`DELETE FROM file_list WHERE id = $1 AND index_generation = $2 AND
     index_size = $3`** (`file_list/postgres.rs:2421`): 6.9 M calls, 3.9 ms,
     **1,181 blocks per call** = probing all 157 partitions' id indexes
     (the sampler shows every `file_list_p_*` partition, empty ones
     included, taking ~3,140 idx scans/min at 08:00Z). Add `AND date = $4`
     (the caller has `file.key`; the `id <= 0` branch already binds `date`)
     → one partition, ~10 blocks. 447 min of DB time / 8.2 G blocks in 37 d.
  3. **`drop_empty_partitions` has never dropped anything**:
     `DateTime::parse_from_str(date_str, "%Y%m%d")` requires an offset and
     returns `Err(NotEnough)` for every name (probed locally; prod logs 7 d:
     `maintenance: completed` 4, `reindexing` 28, `dropping empty
     partition` **0**). Even fixed, `safety_days = max(ingest_allowed_upto/24
     + 1 = 366, retention 30)` keeps a year of empties. Today: `file_list`
     157 partitions / 4,341 MB, **124 empty holding 2,993 MB** (the ten
     08-19…08-28 partitions hold ~2.98 GB with 0 live rows; 114 stray
     96 kB ones from 2025-08…2026-05). Late rows for a dropped day fall into
     `file_list_default` by design (`ensure_file_list_partition` keeps
     writing to DEFAULT when creation is blocked), so dropping an empty
     partition past `data_retention_days` is safe. Fix: `NaiveDate::
     parse_from_str`, cutoff = retention + 1 day; one-time manual `DROP
     TABLE` of the ten big empties now (−2.98 GB, −10 partitions to open per
     unpruned plan).
  4. **`wal_segments` probes on generic plans**: `has_claimable` (9.97 M
     calls, 555 blocks/call; live `EXPLAIN` = Seq Scan LIMIT 1 reading 2,468
     of 9,134 heap pages while 23 rows qualify, all 9,134 when none does),
     `has_late_claimable` (8.26 M calls, 1,335 blocks/call, 497 min),
     `count_unbuilt_older_than` (`status != 2` → Seq Scan 9,134 blocks,
     1.44 M calls), `list_l0_orphan_rows` (same), `list_expired` (Seq Scan
     + sort, 30 sweepers × 1/min), `query_unbuilt` (`status != $4 OR (… >=
     $5)` with `$5 = i64::MAX` always → Parallel Seq Scan 14 k blocks per
     query per stream). The aggregate `claimable_stats` with the SAME
     predicate plans as BitmapOr over `status_created_at_idx` (1,235
     blocks, 1.2 ms) because there is no `LIMIT 1` tempting an early-exit
     seq scan. Fixes, all SQL-text only: inline the `SegmentStatus`
     constants (the planner then uses the MCV `{2,1,0}` even in a generic
     plan) and write `status IN (0, 1)` instead of `status != 2`; drop the
     tick-path `has_claimable`, `claimable_stats` is cheaper than the probe;
     `has_late_claimable` → `status = 0 AND created_at < $3` index range +
     heap filter. Expect `seq_scan` on wal_segments from 10/s to ~1/s.
  5. **`wal_segments` indexes are 96 % deleted pages** (pgstatindex:
     `object_key_idx` 192 MB = 878 leaf pages live / 23,708 deleted,
     `node_seq_idx` 146 MB = 622 / 18,027, `max_ts_idx` 53 MB, `status_
     created_at_idx` 46 MB, `pkey` 42 MB; heap 71 MB = 37.7 % live, 60.7 %
     free — page fullness is NOT why 63 % of updates are non-HOT; the
     status flips are). `object_key_idx` is UNIQUE on
     `wal_segments/{node_uuid}/{seq:020}`, i.e. the same identity
     `node_seq_idx` already enforces, 0 scans ever → `DROP INDEX
     CONCURRENTLY` (−1 of 5 index writes on 8.5 ins + 23 non-HOT upd + 8 del
     per second) and remove it from `create_indexes`; `REINDEX INDEX
     CONCURRENTLY` the other four once (→ ~5 MB each) and add them to the
     daily `reindex_non_partitioned_tables` list, which today covers only
     `file_list_deleted` and `file_list_jobs`.
  6. Historical, already gone: the pre-late-lane claim CTE `status = $4 OR
     (status = $1 AND updated_at < $5)` is still #1 by total time in the
     37-day window (965 k calls × 88 ms, **28,560 blocks/call**, 23.7 h);
     the live shape costs 1,041 blocks / 3.8 ms. Reset `pg_stat_statements`
     after the fixes so the window is clean.
  7. Hourly WriteIOPS burst = `file_list_deleted` at :00 (sampler
     08:00–08:01Z: 16,713 UPDATE + 16,713 DELETE + 2,828 INSERT per minute vs
     698 writes/min baseline; WriteIOPS 1.4 k → 14.2 k / 13.7 k for two
     minutes, CommitLatency 0.5 → 1.6 ms, CPU 58 %). That is upstream's
     `query_deleted` lease (`UPDATE … SET created_at = now` on an indexed
     column, so every row is a non-HOT update × 3 indexes) followed by the
     row deletes, run by one node at the top of the hour. Bounded and
     harmless alone; it stacked on the `.176` stall at 07:00. Optional:
     spread the batches or lease with `FOR UPDATE SKIP LOCKED` instead of
     the `created_at` bump. `stage_index_generation` also does INSERT +
     `UPDATE SET index_generation = id` per row (19.7 M updates = one per
     insert) — fold into one statement with `nextval`.
  8. `meta` reads: `SELECT … FROM meta WHERE 1=1 AND module = $1 AND key1 =
     $2 AND (key2 = $3 OR key2 LIKE $4)` 520 k calls × **140 ms mean, max
     267 s**, only 139 blocks/call, 1.8 rows/call — time is in TOAST
     decompression/transfer of large values (stream schemas), not I/O.
     Upstream shape; note only.
  9. `file_list_jobs`: `INSERT … ON CONFLICT DO NOTHING` 25.4 M calls for
     1.14 M rows (95 % no-ops, 8/s) + the existence `SELECT` 35.7 M calls
     (11/s); 0.01–0.27 ms each — round-trip noise, not load.
  10. Cluster housekeeping (owner decisions, not done): databases
      `obs20260803` (**12 GB**) and `obs20260817` (1.1 GB) have 0
      connections and no activity — 13 GB of the cluster's 25 GB; our
      pods hold 324–353 connections (343 idle; pool max = min(cpu×4, 32)
      per pool per process, idle timeout 600 s) vs O2's 186 — `ZO_META_
      CONNECTION_POOL_MAX_SIZE=8` on router/ingester/querier would shed
      ~150 idle backends (~1 GB of the r7g.xlarge's 32 GB); Performance
      Insights is still off (free 7-day tier, no restart).
- Recovery state 08:09Z: unbuilt 1,047 (951 Building = 30 builders × 32),
  no statement > 1 s, `slow statement` 100/10 min (ingester 8, compactor 71,
  querier 21) vs 2,349 at the peak and ~700 during the drain; commit
  latency 0.4–0.8 ms outside the :00 minute.
- **`.177` rolled 2026-09-29 = vix-arch `27df623dc`** (ECR index
  `sha256:6a019132…`, binary `39da83d1…`; GitOps #572 querier 09:41Z, #573
  compactor + ingester/router 09:44Z; every role on `.177` by 09:48:27Z, 0
  restarts, 0 `segment buffer full`). Contents = the P2 v2 spec above plus
  audit items 1–5:
  - `cbfd5cf63` settings cache keeps confirmed misses (`Option<SystemSetting>`).
  - `6b5f9b9a9` `file_list` delete-by-id carries `date` (1 partition instead
    of 147 in `EXPLAIN`); `drop_empty_partitions` parses with `NaiveDate`
    and cuts at `data_retention_days + 1` — upstream has the same bug
    (`93764d3ea`, still in upstream/main).
  - `27df623dc` `wal_segments.stream_ranges` (offset-seconds JSON, outward
    rounding), uploader `FoldedMeta`, querier `prune_and_cap` (fail-open,
    full-page shortfall kept), `query_unbuilt` newest-first; PG probes
    reshaped: `has_claimable` = UNION ALL of two `ORDER BY` + `LIMIT 1`
    index probes, `has_late_claimable` `ORDER BY created_at`, status
    literals inlined, `status <> 2` spelled `status < 2 OR status > 2`;
    `(status, updated_at)` index added, `object_key_idx` removed from
    `create_indexes`. Tests: infra `wal_segments` 29 (5 new: outward
    rounding + clamp, malformed decode, `range_for` alignment, `add`
    validation, sqlite round trip + poisoned column fails open),
    segment_wal uploader 7, core segments_scan 42 (2 new prune tests), jobs
    segments 37.
  - DDL done by hand under `lock_timeout 3 s`, each first try: 09:00:39Z
    `ADD COLUMN stream_ranges`, `CREATE INDEX CONCURRENTLY
    wal_segments_status_updated_at_idx`, `REINDEX INDEX CONCURRENTLY` ×4
    (42–146 MB → 0.75–2 MB each); 09:48:52Z `DROP INDEX CONCURRENTLY
    wal_segments_object_key_idx` after the roll. `wal_segments` total
    **595 MB → 53 MB**. Also 08:54Z `DROP DATABASE obs20260803` (12 GB) and
    `obs20260817` (1.1 GB) — 0 connections, no secret/configmap/GitOps
    reference; 08:57Z the ten 0-row `file_list_p_202608{19..28}` partitions
    (`count(*)` = 0 re-checked before each): `file_list` 157 → 147
    partitions, 4,345 → 1,421 MB; our database 5,151 → **2,227 MB**.
  - Statement plans, `PREPARE` + `EXPLAIN ANALYZE` on prod with the exact
    new texts (generic plan, buffers old → new): `has_claimable` 2,468 → 215
    (9,134 → 226 with nothing claimable), `has_late_claimable` 1,335 → 2,
    `claimable_stats` 1,235 → 197, `count_unbuilt_older_than` 9,134 → 81,
    `list_expired` 9,134 + sort → 135 incremental sort, `list_l0_orphan_rows`
    9,134 → 73, `query_unbuilt` 14,125 + temp files / 56 ms → 1,287 / 3 ms.
  - DB, 5-min windows before (09:22Z) → after (09:50Z): `wal_segments` seq
    scans **10.4/s → 0.09/s** (233 k → 2.8 k rows/s), its SELECTs 16/s at
    **2,022 → 103 buffers/call, 5.1 → 1.8 ms**; `system_settings` statement
    **107/s → 0**; whole-DB commits 225 → 110/s, buffer hits 86 k → 25 k/s;
    row writes unchanged (169 ins / 77 upd / 128 del per s; `wal_segments`
    8.9 / 43 / 8.3). CloudWatch through the roll: CommitLatency 0.34–0.57 ms
    (one 2.7 ms minute at 09:52Z), WriteIOPS 1.2–2.2 k, CPU 21–36 %. The
    remaining compactor `slow statement`s (59/10 min) are all upstream's
    `SELECT id, module, key1 … FROM meta` at ~4.1 s (audit item 8).
  - Ranges: 5,815 rows carried them by 09:58Z, 0 misaligned / inverted /
    out-of-segment across 141 k pairs, max `hi` overshoot 0.99 s; 76 streams
    per segment on average, 131 B stored, rows 1,265 B with vs 1,031 B
    without (inline, TOAST unchanged at 5.6 MB). Geometry: a segment spans
    1.2 h p50 / 1.8 h p90 while a stream's own range inside it is seconds
    wide (p90 < 0.01 h) — one late frame stretches every segment over every
    recent window.
  - **Tail effect** (same battery, `[end−1 h, end)` with `end` 10–15 min in
    the past, `use_cache=false`, leader `candidates` / follower loaded →
    zero-yield; pre = `.175` at 09:26Z, post = `.177` at 09:57Z with 837 of
    851 unbuilt rows ranged): logs count 1 h **595 → 49 candidates, 93 % →
    16 % zero-yield**, warm 965–990 → **138–165 ms**; traces count 1 h **692
    → 205, 71 % → 4 %**, warm 1,050–1,602 → **278–285 ms**; traces needle
    1 h **701 → 217, 70 % → 4 %**, warm 1,504–1,581 → **351–398 ms**. Better
    than `.176` measured (52 / 19 %, 198 / 8 %) because the offsets are per
    stream AND the page is newest-first. The residual zero-yield is the
    ≤ 1 s outward rounding plus segments whose stream range touches the
    window edge.
  - Health check 10:10–10:22Z (35–40 min on `.177`): 49/49 pods Running,
    0 restarts, 0 `segment buffer full`, ingesters 85,559 requests / 15 min
    all 2xx, 226–848 segments shipped per ingester per 10 min (p50 210–278
    ms, max ≤ 565 ms). Pipeline: pending 29, oldest 9 s, building 752,
    built-retained 31,630 (1 h), sweeper 21,643 deleted / 15 min, 0 L0
    orphans; builders 6,237 built / 2 skipped / 0 gone in 15 min — the 2
    skips were S3 **`503 SlowDown`** GETs on fresh-ingester segment keys
    (10 retries / 5.8 s), rebuilt 8 min later; that signature runs 1–11/h
    for days (not new). `file_list_jobs` 0 pending / 21 running. DB: 0
    statements > 1 s, 252 connections, 1,798 MB. Querier memory: kubelet
    `rss` 7.8–10.2 GiB (yesterday's 6.6–8.9 on 3-day-warm pods) while
    `kubectl top`'s working set reads 16–21 GiB and `usage` sits at the
    24 GiB limit — the difference is file page cache from the disk-cache
    fill (542 GiB written per pod in 40 min), reclaimable, not OOM
    exposure. `Resources exhausted` (12 GiB shared pool) 1 + 16 in the two
    hours vs 6–295/h over the previous day — unchanged. Battery vs O2
    (window ending 10:05Z, cold/warm): logs count 1 h 483/153 vs 70 ms;
    `SELECT * LIMIT 50` 1,243/389 vs 1,125/226; traces count 24 h
    2,100/533 vs 4,676/1,033 (complete on both); traces top-5 services 1 h
    7,707/747 vs 218/228 (the known aggregate gap, cold sidecars); needle
    1 h 1,577/1,153 vs 222/101 (cold pods; 351–398 ms at 09:57Z). Logs
    histogram with alias `b`/`x` fails ONLY because `b`, `x` (and `bucket`)
    are real columns in obs logs/default — **22,263 fields vs O2's 6,204**;
    with a non-colliding alias 767 vs 225 ms. Verdict: healthy.
- Left open from the audit: item 6 (`pg_stat_statements` reset for a clean
  window — do after a day of `.177`), item 7 (`file_list_deleted` hourly
  lease UPDATE; `stage_index_generation` INSERT+UPDATE), item 8 (`meta` reads
  4 s), Performance Insights (owner call). Pool size stays as is (owner
  call, 2026-09-29).
- **Incident, self-inflicted, found in the 11:30Z review: `cached plan must
  not change result type`, 09:00–09:24Z.** The pre-applied `ALTER TABLE
  wal_segments ADD COLUMN stream_ranges` (09:00:39Z) changed the result
  descriptor of every `SELECT * FROM wal_segments` / `RETURNING *` prepared
  statement cached by the sqlx pools of the pods still on `.174`/`.175`;
  PostgreSQL then rejects the re-planned statement until the connection is
  recycled (`max_lifetime` 1,800 s) or the pod restarts (`.177` roll
  09:41–09:48Z). Counted from logs: **1,810 errors** (compactor 1,041,
  querier 662, ingester 107 — ingesters run the builder loop too); **331
  searches failed** with `http->search: err … query_unbuilt … cached plan`
  (09:00 24, 09:05 79, 09:10 177, 09:15 49, 09:20 2 — the majority of the
  ~540 searches in that window, HTTP 500 to the caller); builder
  `claim_pending` failed 572× and `super-batch extension claim` 41× on the
  affected connections (other connections kept claiming; unbuilt peaked
  ~1k and drained by 10:15Z); sweeper `list_expired` 530 failed passes
  ("rows kept for next tick", retention delayed ≤ 30 min). Zero
  recurrence after 09:24Z. The 10:10Z health check missed it because it
  looked at the last 30 min only. Not the DDL's fault alone: the fork's own
  boot-time `add_column` would have done the same to every other pod
  during the rolling deploy. Fix `c794e94f5` (vix-arch, not yet released):
  every row-reading statement in `wal_segments.rs` names its columns
  (`COLUMNS`, both backends) so a column addition leaves the descriptor
  unchanged; upstream's `file_list`/`scheduler`/`pipeline` still use
  `SELECT *`, exposed only at upstream upgrades. Rule from now on: schema
  changes on tables read by long-lived pools ship WITH the pods that read
  them (boot-time `add_column`, no pre-apply), and the reads must not use
  `*`.
- **Open-items review (11:30Z), evidence per item:**
  1. Trace-by-id full scan — **structurally closed** since `.174` (per-file
     sidecar bloom probes): needle 1 h today scans 263 k records (was
     500 M), followers `probed=1,179 dropped=1,178 kept=1`, scan branch 2
     files; 3 h `probed=3,014 kept=3`. Residual = probe cost ∝ file count
     (0.2–0.4 ms/file warm, ~3 ms cold): 514 ms warm / 1.1 s cold vs O2
     100–220 ms. P3 (bloom-only sidecar for logs L0s, `no_sidecar`) still
     open — today `no_sidecar=0` on traces; logs needles still full-scan
     index-off L0s.
  2. Segment-WAL tail on the follower critical path — **mitigated by
     `.177`, not removed**: logs count 1 h candidates 595 → 96 (87 loaded
     across 10 followers, 776 rows, 1 time-pruned), setup p50 81 ms but the
     straggler follower 380 ms, and the tail still sits inside `follower
     search setup` before the scan, so the slowest follower's tail is the
     leader's floor. Next lever if it matters: overlap the tail fetch with
     the file scan instead of serialising it in setup.
  3. Fragmented L0 — **still structural**: last 3 closed hours 2,010 L0
     files, **752 < 1 MB, 196 with ≤ 2 records** (each builder batch writes
     ~280 files across ~265 streams, avg 15 k rows/file; late lane adds the
     2-record ones). Merge absorbs them within ~2 h (hours 07/08 are all
     > 32 MB, 0 tiny; hour 09 still 187 + 551 small; merge debt hours = 0)
     so the cost is confined to the recent window: the 1 h traces window
     carries ~1,270 files, ~550 tiny, and every needle/aggregate pays a
     probe or an open per file. Fix is in the builder (accumulate small
     streams across batches / per-stream size floor), not in merge.
  4. Builder memory — **open in code, stable in prod**: `CHUNK_MB 512` /
     `BUDGET 4096` unchanged; the budget still admits decoded input +
     planning scratch + per-plan decoded bytes only; the writer's output
     residency (`BuiltL0File.buf`, spooled to disk only when large) is
     unaccounted, so "restore 8192" stays blocked on that accounting.
     Compactors today max RSS 11.0 GB / avg 5.7 GB of 60 GiB, 0 OOMKilled
     since 09-25.
  5. Compaction vs DB — **mostly closed today**: deadlocks 2 in the whole
     37-day window (both 09-28 16Z), `pool timed out` 731/24 h of which 690
     were the `.176` incident and 38 on 09-28 16Z, 0 since the `.177` roll;
     `merge job offset error` / `failed to commit` 0 in 24 h; the 94 s
     (reported as 174 s elsewhere) `DELETE FROM file_list WHERE id = $1`
     is partition-pruned in `.177` (1 partition vs 147 in `EXPLAIN`); the
     remaining ≥ 4 s statements are upstream's `meta` reads (audit item 8)
     and `pg_advisory_xact_lock` waits. New finding: `retire_files`
     (`file_list/postgres.rs:145`) builds `DELETE FROM file_list WHERE (id
     = … AND …) OR … RETURNING …` in 300-arm chunks without `date` — 47
     variants, 3,346 calls, **10 k–61 k buffers per call**, 8–51 ms; 26 s
     total in 37 d, so low priority, same one-line fix (add `date =` to
     the `id > 0` arm).
  6. Query anomalies, 24 h: `Resources exhausted` (12 GiB shared pool)
     892 total, bursty (295 at 09-28 17Z, 230 at 02Z, 83 at 08Z, 52 at
     10Z) — unchanged by `.177`, the wide `SELECT *` shapes; the per-query
     `ZO_STORAGE_SCAN_MAX_BYTES` cap bounds one follower but ten concurrent
     wide queries still saturate the pool. `Search field not found:
     trace_id` **1,311/24 h ≈ 55/h steady** and `_all` 118/24 h are client
     queries against streams lacking the column (HTTP 400, upstream's
     20004 behaviour, no SQL logged at 400 so the caller is unidentified —
     Orbit's log correlation is the likely source); `GROUP BY` planning
     errors 99/24 h are alias collisions with real columns (`SELECT error,
     COUNT(*) …` on logs where `error` is a column) — user SQL, 400s, and
     the 22,263-field logs schema makes collisions likely.
- **Long-window battery 12:00Z (owner challenge: "our queries are not 1 h
  windows") — what actually costs seconds.** Executed query windows from the
  result-cache deltas (373 in 60 min): ≤ 15 min 34 %, ≤ 1 h 47 %, ≤ 6 h
  15 %, ≤ 24 h 3 %, > 7 d 1 % (deltas understate dashboard windows; the
  `file id snapshot` proxy: 62 % ≤ ~1 h of files, 28 % ≤ ~4 h, 9 % ≤ ~20 h).
  The segment tail is NOT smaller on long windows — every window touching
  "now" carries the whole unbuilt set (avg 849–1,037 candidates on the
  ≥ 2 k-file queries; follower `segments_scan` p90 464 ms logs / 1,310 ms
  traces, max 5.5 s over 90 min of prod) — but it is a fixed 0.1–1.3 s tax,
  ms-level next to the items below. `[end−24 h, end)` / `[end−6 h, end)`,
  `use_cache=false`, obs cold/warm vs O2 cold/warm (ms):
  - logs count 24 h 631/309 vs 5,041/1,174; logs histogram 30 m 3,459/305
    vs 3,819/3,283; logs `SELECT * LIMIT 50` 482/520 vs 6,142/770; traces
    count 1,171/830 vs E400/1,551; traces histogram 30 m 4,946/546 vs
    9,260/6,874 — obs ahead on every unfiltered shape.
  - **logs `WHERE service_name='llm-router'` count 24 h: 7,668/1,800 vs
    1,767/1,398, and obs is `partial=true`** (171,430,052 vs O2's complete
    173,982,812). Attribution: the index answered 1,017 merged files in
    126 ms (`found count: 16,224,108`); the **105 index-off L0 files**
    (`ZO_VIX_L0_INDEX_OFF_STREAM_TYPES=logs`, 198 GB original / 4.85 GB
    compressed on one follower) went to the scan branch and tripped
    `ZO_STORAGE_SCAN_MAX_BYTES` (23 skipped). Every filtered logs
    aggregate whose window reaches the last ~2 h pays this and may be
    truncated. Lever: a cheap L0 index profile for logs (value index for
    low-cardinality fields such as `service_name`/`level`, blooms for
    auto-ids, no body FTS) — the P3 measurement, now with a correctness
    motive; or shorten the L0→merge lag.
  - **traces `approx_percentile_cont(duration, 0.99)` by service 6 h:
    1,559/2,439 vs 6,789/E400, obs `partial=true` — 335 of 403 files
    skipped, the answer covers the NEWEST 68 files (3.93 GB) ≈ 10 % of the
    rows** (vida-bizserver count 82.5 M vs O2 861 M; p99 228 vs 195 ms).
    Scan-bound by nature (1 TB original / 6 h); O2 is complete at 3.8 s or
    errors. Only pre-aggregation fixes this class (per-file span-metric
    sketches: count + duration digest per service, emitted by builder/merge,
    read instead of rows) — an APM feature, not a tuning.
  - **traces top-5 services 24 h cold 15,359 ms** (warm 837 vs O2
    3,726/2,855): index eval max 14,192 ms with **125,293 index fetches**
    across the followers — the disk cache is empty after every roll and the
    24 h aggregate touches ~13 k files' sidecars. Levers: persistent disk
    cache across restarts or a boot-time warm of the last N hours' index
    objects (`cache_latest_files` already owns the file→node mapping); fewer
    files per hour helps linearly.
  - **traces needle 24 h 4,421/4,198 vs O2 E400/930**: blooms probed 4,094,
    kept 7 for an absent id (FPP 0.001 → ~4 expected false positives), and
    each kept file falls to `fast path fallbacks 1 (unservable: 1)` = a full
    scan of a 3.1 GB-original file because `trace_id` is bloom-only (no
    posting list). Cheapest lever in the whole list: `ZO_VIX_BLOOM_FPP`
    0.001 → 1e-5 for the bloom-only fields (+~70 % bloom bytes, sidecars are
    small; `bloom_ver` bump lets the `.bf` pass regenerate existing files):
    expected FPs 4 → 0.04 per 24 h needle, i.e. ~4 s → ~0.5 s. Second lever:
    per-zone blooms so a hit scans one zone, not the file.
  - Re-ranking after this: (1) bloom FPP for bloom-only fields; (2) logs L0
    cheap index (correctness + 1.4–7.7 s → ~0.2 s on filtered logs
    aggregates); (3) disk cache persistence / boot warm (cold 15 s → ~1 s
    after each roll); (4) span-metric pre-aggregation for percentiles; the
    tail prefetch (#2) and the straggler merge lane (#3) drop to after these
    — real but 0.1–1 s.
  - **Corrections after a second look (12:30Z), three findings:**
    1. The 24 h needle's seconds are NOT the fallback scan. Re-run 4,902 ms:
       per follower the `.bf` bucket stage took **2.3–3.9 s** ("bloom
       filter reduced file_list from 1,352 to 1 in 3,907 ms") because the
       bucketing has degenerated to **one `.bf` object per file**:
       traces/default hours 09-28 18Z…09-29 08Z show `files = stamped =
       distinct bloom_ver` (534/533/533 …). `compact::bloom` chunks by the
       per-field `(field, num_blocks)` signature and the AUTO-ID bloom-only
       fields vary per file, so every file is its own chunk (the comment
       says "a couple of chunks at most"). 874 group fetches per follower
       (footer + row ranges each) instead of ~24. Per-file probes for the
       ~480 unstamped recent files: 515 ms. The FP fallback scans (1–2
       files of 140–330 MB compressed, disk-cached, column-pruned) are
       **97–699 ms**, not 4 s. Fix: chunk `.bf` only by the configured
       bloom-only fields' signature (pad or ragged per-file `num_blocks` in
       the footer), 256 files per chunk → ~55 objects per 24 h → bucket
       stage ~150 ms; FPP 1e-5 stays a separate, smaller lever.
    2. top-5 services 24 h is read-count bound, not bandwidth: per follower
       ~1,300 files → **~14 k index range reads (~10.6 per file), ~600 MB**;
       90 %-disk-cached followers 2.3–3.3 s ≈ 5 k IOPS against the gp3
       volume's **provisioned 4,096 IOPS** (class `gp3-1024`: iops 4096,
       throughput 1024 MB/s) → IOPS-throttled even warm; cold followers
       [INFERENCE] 14 k remote GETs at `ZO_VIX_SEARCH_CONCURRENCY=64` ×
       ~65 ms ≈ 14 s. Levers: coalesce a file's needed index sections
       into 1–2 ranges (→ ~1.3 k reads), higher-IOPS/instance-store cache,
       persistent cache across rolls, fewer files.
    3. `ZO_STORAGE_SCAN_MAX_BYTES` (P4, `.175`, `storage.rs::
       apply_storage_scan_cap`): applied to EVERY scan-branch file set by
       Σ `compressed_size` before IO, keeps the newest files, flags
       `partial` + message, `QUERY_STORAGE_SCAN_CAPPED_TOTAL`. It was
       added for `SELECT * … LIMIT 50` on logs whose L0s are index-off
       (6 h scan = 6.58 TB / 18.7 s and pool exhaustion) — the scan branch
       downloads whole files into the disk cache before DataFusion streams
       them, so "streaming" bounds neither wall time nor the download
       volume. It is shape-blind: the same cap silently truncates
       counts/percentiles (10 % coverage on the 6 h p99). Fix: apply the
       cap only to row-returning LIMIT shapes (`SimpleSelect`); aggregates
       over unindexed files run whole (admission-bounded) or fail loudly.
- **2026-09-30 — owner decisions: keep the scan cap ONLY for `SELECT … LIMIT
  n`; pool size unchanged; `.bf` chunking by the three configured bloom-only
  fields (`trace_id, span_id, reference.parent_span_id`) is the agreed fix
  direction.** Shipped as two releases (both vix-arch, both `COLUMNS`-safe):
  - `.178` = `2d7b35536` (all roles, GitOps #574, 16:54–16:57Z, 0 restarts):
    `storage.rs::scan_cap_budget` returns the configured budget only for
    `SimpleSelect(n > 0)`; **but the call site still passed the raw config
    value** — a rejected multi-op edit dropped that hunk, the resulting
    `unused variable` warning was filtered out of the build log, and the
    prod check showed aggregates still `partial`. Lesson recorded.
  - `.179` = `c6042f579` (querier-only, GitOps #575, 17:10–17:12Z, 0
    restarts): the one-line wiring. Verified on 10/10 `.179` queriers with
    the motivating queries (`use_cache=false`, cold/warm):
    - logs `WHERE service_name='llm-router'` count 24 h: **`partial=false`**,
      175,315,666 rows (O2 177,705,749 on a 0.4 % larger record set — corpus
      difference, not truncation), 3,351 / 1,045 ms vs O2 330 / 434 ms — the
      residual is the ~105 index-off logs L0 files scanned in full (the L0
      cheap-index item).
    - traces `p99 by service` 6 h: **`partial=false`**, 2.63 B rows scanned
      (was 210 M), vida-bizserver p99 **251.5 ms vs O2 251.2 ms** (was 228 /
      180 ms on 10 % of the rows), 5,157 / 3,760 ms vs O2 4,357 / error.
    - logs `SELECT * WHERE body='<absent>' ORDER BY _timestamp DESC LIMIT 50`
      6 h: still capped (`partial=true`, "266 files (17.99 GB) of 343 were
      skipped"), 8.7–20 s cold after the roll.
    - During the last pod's replacement one run of each aggregate returned
      `partial=true` with `connect to gRPC node error` — the roll, not the
      cap; clean on the settled fleet.
- **2026-09-30 05:00–08:00Z — the L0 scan path, the "2 h lag" and what the
  merge pipeline really does (owner review; corrections to earlier numbers).**
  - Volumes, last 24 h: traces/default 42.4 TB original / 1,893 GB compressed
    / 9.1 B rows (12,861 files); logs/default 38.1 TB / 931 GB / 3.4 B rows
    (12,502 files). Per hour ≈ 1.8 TB traces, 1.6 TB logs original — the
    "260 GB/h" quoted earlier was a 45-min slice of one hour's merge output.
    L0 landing now: logs 1,208/h at **p50 2.69 GB** original; traces 750/h at
    **p50 312 MB** (straddling slices + CHUNK 512 on wide rows).
  - L0 scan path is fine: `ZO_VIX_READ_MODE=ranged` is on, the filtered 24 h
    logs count scans 36–55 L0 per follower (1.8–2.5 GB compressed) in
    **91–230 ms** of DataFusion time (one 729 ms straggler = 21 % uncached
    files on S3 range reads); the cold 3.1 s of that query is `idx_took`
    (1,120 indexed files × ~10 index reads). A cheap L0 index would save
    0.2–0.8 s warm on this shape — not the root fix. The retired cap also
    measured Σ `compressed_size` while ranged scans read one column.
  - The lag (logs/default, 05:14Z): open hour 95 % of rows still in L0; at
    close+14 min 61 %; close+74 min 1.1 %; 0 by close+130 min. The live lane
    starts ~9 min into the hour. The bound is scheduling: one job per
    (stream, hour) on one node, `ZO_COMPACT_LIVE_WORKER_NUM=1` (auto
    concurrency resolves the rest), 4.2 merges/min ≈ 8.3 L0/min arrival.
  - What actually happens to logs bytes (last 60 min): **560 L0 healed IN
    PLACE** (`single-file healing rebuild … re-derives every term from
    _source`, sidecar-only, data object untouched, 1.29 TB) vs 231 merged
    outputs (0.64 TB rewritten, 311/348 merges are 2-input passthrough of
    already-indexed ~2.5 GB files toward the 4,096 MB `LOGS_INDEXED` target
    — a 1.6× size gain for a full rewrite). Traces: **1,294 merges/h, 1,041
    of them 2-input, all passthrough, output p50 3.0 GB, 13 s each, 3.45 TB
    rewritten per hour on 1.8 TB ingested (≈ 2 generations per byte)**.
    Cause: `is_incremental = !is_past_hour(offset)` — the open hour seals
    only full groups ("each file merged exactly once"), but a CLOSED hour
    seals whatever ≥ 2 candidates each 10 s pass finds while stragglers
    keep arriving for ~2 h → a pairwise cascade. Two targets (`MAX_FILE_SIZE`
    1024 "rebuild-safe" for index-less groups, `*_INDEXED_MAX_FILE_SIZE`
    4096 for passthrough) add a second generation by design, while heal
    already rebuilds 2.7 GB index-less L0s routinely — the 1024 ceiling is
    not actually protecting anything.
  - Proposal (owner: "one config, merge once"): (1) one target
    `ZO_COMPACT_MAX_FILE_SIZE=4096`, delete `LOGS_/TRACES_INDEXED_MAX_FILE_SIZE`
    and `max_file_size_for_merge`; files > 50 % of target are done (the debt
    line already says so), so 2.7 GB logs L0s are healed once and never
    re-merged; (2) keep the incremental "seal full groups only, carry the
    remainder" rule for `LATE_LANE_HOURS` after close and do ONE sweep-up
    seal after that — kills the pairwise cascade (traces 1,294 → ~150
    merges/h, rewrite 3.45 → ~1 TB/h); (3) delete
    `ZO_VIX_MERGE_INDEX_DEFER_BELOW_MB` (creates index-less merged outputs
    that need a second heal); (4) later, bigger L0s via CHUNK_MB so traces
    L0s are not 312 MB. Net: one size knob, one index build per byte, the
    lag becomes the heal/merge scheduling latency only.

## 2026-09-28 — `.173` definitive numbers; needle lookups full-scanned the unstamped hours (fix: per-file sidecar bloom probes + a settled-only `.bf` queue)
- Fleet at 14:52Z, 3 days undisturbed: queriers 10/10 `.173` (7 pods 3 d,
  two node-churn replacements 24 h / 8 h; `restartCount` 0), RSS 6.6–8.9 GB
  of 24 GiB. Compactors 30/30 `.170` (CHUNK_MB 512 / budget 4096 since 09-25
  11:45Z): **0 OOMKilled in 3 d**, max RSS 19.7 GiB (33 % of 60 GiB, 53 % of
  the 40 GB gate), median 7.7 GiB; 6 restarts = 5× the `nats.rs:604`
  NATS-connect-at-startup panic + 1 node-level `Unknown`.
- Gate counters (`/metrics` 14:56Z): `vix_eval_growth_timeouts_total` **14 in
  3 d** (151 in 3 h on `.172`), `budget_refused` **0**, `aggregate chunk
  dictionary budget exceeded` 1; `reason="error"` 674 = ONE signature,
  `AllConditionsSkipped` (`index.rs:234`: every conjunct unservable —
  `trace_id`/`user_id`/`body` not term-indexed, or OR-mixes over absent
  session-id fields), 100 % of it one 09-27 04:43–06:08Z UI burst (813
  tallied on 10 pods − the 2 replaced pods = 674). Growth timeouts emit only
  the counter (`source.rs:462`; the `exact aggregate scan required` line is
  debug), so the 14 cannot be placed in time.
- Battery 14:57–15:01Z (sealed windows ending 14:45Z, `use_cache=false`, obs
  warm / O2 warm; O2 itself degraded: `MemoryCircuitBreakerError` on 7 of 20
  requests, querier-3 OOMKilled 09:59Z, so ratios flatter obs): traces count
  1 h **531/535 (0.99×)**, count 24 h 612/965, 1-min hist 1 h 664/700, 5-min
  hist 3 h + service 446/2,619, top-50 15 m 587/772, APM ops 1 h 1,695/2,670;
  logs count 1 h **594/289 (2.06×)**, logs count 24 h 772/408, logs 30-min
  hist 24 h 912/1,435 (alias `hb`: `bucket` is a real logs column on BOTH
  systems, so `AS bucket … GROUP BY bucket` is a planner error on both, not a
  fork fault), logs top-50 1 h 1,484/691. All obs 200, no partial, **zero
  fallbacks and zero growth timeouts in every warm run**. obs absolute vs
  `.172` 11:10Z: T1/T3 same, T4 −40 %, T2 +24 %, T5 +52 %, L1 +93 %, L2
  +92 %, L3 +134 %, L4 +34 % — every regression is a tail-scan or
  DataFusion-scan shape. Cold r1 is 1.8–3.5× r2 with `idx_took` 655–2,252 →
  22–356 ms: r1 pays the index-evaluation cache, not the file cache.
- Attribution: logs count 1 h — all 547 files index-answered in 2–4 ms per
  follower, stream 0 ms, **100 % of follower time is `segments_scan`**: 523
  segments loaded, 443 (85 %) zero-yield *time-pruned after fetch+decode*
  (`skips before-fetch/decode 0/0`); leader 583 = slowest follower 564 + 16
  file_list + 3. logs count 24 h — 6,580 files index-answered in 36–53 ms,
  segments_scan 180–586 ms, 72 % zero-yield; the follower with 0 remote
  segments is the fastest (234 vs 657). logs top-50 — `SELECT *` over
  index-less L0s (813–859 ms) behind a 481–580 ms segments_scan in setup.
  APM ops — index evaluated then abandoned on all 10 followers (`too many
  row_ids, avg percent 35.8–37.3`), column scan of 108–173 files per
  follower (293–1,673 ms; parquet cache 74–85 %, sidecar 60–72 % on r2). The
  24-hour-old follower was the slowest in every shape; leader wall = that
  follower + ≤ 100 ms.
- `file_list` (14:58/15:09/15:20Z): the 09-25 flood is gone — hours 11–14 of
  09-25 at **0/0/0/1 L0** (479–562 files each, all > 2 GiB); every closed
  hour ≥ 3 h old at 0–1 L0 (traces) / 0 index-less (logs), max file ≈ 4.29
  GB; 30-day debt 0.04–0.08 TB, minutes old. But the 1 h traces window is
  **stationary at ~1,000 files / ~700 L0** (1,060/727 → 937/650 → 1,005/695
  over 22 min; 09-25 13:50Z's 1,171/680 was already this regime; weeks-warm
  617/1 is not reachable by draining): the open hour holds the last ≈ 30 min
  of production as ≈ 600 × 1.5 GB L0s (merge lags production ≈ 30 min), the
  previous hour drains its large L0s in 1.2–1.9 h, closed hours receive
  ≈ 300 tiny stragglers each for ≈ 3 h. Builder healthy: ≈ 470 segments/min,
  ≈ 2 GB/min, build latency 100–120 s, pending < 0.2 GB;
  `unbuilt_older_10m` flat at 46–51 for 4 h (a floor no claim selects —
  undiagnosed).
- Tiny L0s are structural (compactor logs 14:27–15:27Z: 6,951 L0 built,
  **1,434 = 20.6 % ≤ 10 records**, 2,421 < 1 MB). (1) Fresh lane, 1,018/h:
  `buffer.rs:194` routes a frame late only when the frame-level `max_ts <
  now − 2 h`, and `chunk_per_stream_hour` (`segments.rs:1623`) emits one L0
  per (stream, actual hour), so a 69-segment batch carrying 2 rows for
  H−1/H−2 emits a 2-record L0 for that hour (387 at H−1, 532 at H−2; all 315
  fresh batches emitted ≥ 1). (2) The 15-min late-cohort builds, 2/h: 142–143
  all-late segments totalling 1 MB fan into 297–472 per-hour files (416
  tiny/h). A producer replay at 15:00–15:10Z put KB-sized stragglers into
  ~230 weeks-old hours (data 1–22 d old) and each made its hour debt again:
  the backlog lane rewrote 354 GB traces + 246 GB k8s_prod_public_logs +
  194 GB logs/default of 1–1.5 GB residuals in 20 min to absorb ≈ 4 MB (debt
  hours 7 → 212 → 157; pending jobs 108 → 264 → 186, 90 slots busy). M31a's
  file-count poison one layer down: the late lane coalesces segments, not
  hours.
- Merge health (12:10–15:10Z): 10,324 merges, **0** failures / refusals /
  lease losses / panics. Hour 14: 3,694 merges / 11,727 inputs / 6.95 TB,
  **199.6 MB/s active** (indexed 222.4 = 2.2× the `.145` reference; every
  lane × type ≥ 1.5×), 9.7 concurrent; 485 of 830 backlog-traces merges were
  pairwise merges of < 100 MB files for 09-07…09-24. 116 INFO `type
  widening` (`tools` Float64→Utf8) — demotions, not failures.
- **Needle lookups over the recent hours full-scanned.** From 15:05Z the UI
  trace views (`SELECT * FROM "default" WHERE trace_id = '…' ORDER BY
  _timestamp ASC LIMIT 5000` on traces + `LIMIT 2000` on logs, 1–3 h
  windows) scanned 2.46–2.73 TB in 12.6–25.7 s per lookup for ≤ 23 rows,
  15,740 ERROR lines by 15:44Z, and the **first two `ResourcesExhausted` on
  `.173`** (15:31Z, 15:32Z: 12.73 of 12.88 GB pool reserved). Controlled A/B
  17:10Z, 1 h window `[16:00, 17:00)`, one trace id, 1 hit on both: obs
  6,984 / 10,070 ms scanning **1.74 TB / 386 M rows**; O2 720 / 617 ms
  scanning **286 MB / 251 k rows** (`idx_took` 341–455 ms = upstream's
  default secondary index: `_DEFAULT_BLOOM_FILTER_FIELDS = ["trace_id",
  "session_id"]` folded into the index defaults; invisible in stream
  settings). Anatomy per follower (trace `01a0e893…`, 3 h): leader `.bf`
  prune 948 ms — `with_bloom=138 → 1 kept`, `without_bloom=134 → all kept`
  (`no_bloom=131`); VIX eval 378 ms — `fast path fallbacks 121 (error: 64,
  skipped_file: 57) of 121`; Segment-WAL tail 94 segments / 1.2 M rows /
  `kept 0` in the 3.6 s setup; DataFusion 121 files (9.9 GB compressed) ≈ 7 s.
  `_source` is NOT decoded for non-matching rows: `inject_vix_scan_pruning`
  turns the equality into `ColumnBound{min=max=Str}` and the default
  `BoundedPrepass` runs `eq_string_prepass` (docs.rs:1669) on the `trace_id`
  column alone, point-reading the projection for hits — the 7 s is the
  column decode over ~110 M rows per follower (a near-unique column: the
  dictionary IS the column). Pruning, not scanning, was the lever.
- Root cause: three facts stacked. (1) `trace_id` is bloom-only by design
  (`ZO_VIX_BLOOM_ONLY_FIELDS`; #52), so `field_capability` reports `FtsOnly`
  → `AllConditionsSkipped` for every file. (2) Every indexed `.vix` already
  writes a per-file bloom blob holding those values (composite section,
  `bloom.rs`: "a byproduct of term emission"), and the group `.bf` is only
  its hour-level transpose — but the query side never read the blob, only
  `.bf` (`bloom_ver > 0`). (3) The `.bf` assembler never reaches the recent
  hours: `query_bloom_pending_buckets` excludes the open hour, every merge
  output resets `bloom_ver = 0` (merge.rs:2766), and — measured — one pass
  ran **2,958–6,227 s** for 300 attempted buckets (79–107 processed, 193–221
  "busy elsewhere"), because the two live traces hours held 2,100 of the
  3,200 pending files in 2 of 646 buckets: at the head of the date-DESC list,
  the winner fetched ~1,000 blobs under one lock while 29 compactors timed
  out on it, then the L0s merged away and re-pended. SQL is not the cost
  (`query_for_bloom` 0.04 ms, buckets 60 ms).
- Fix, this commit set (vix-arch; not yet released):
  - `vortex_index::bloom_probe::FileBloomProbe` — sidecar footer → bloom
    blob section table (headers only, sliding 4 KiB window, bodies never
    read) → `probe(field, values, composite_fallback)`: one batched fetch of
    the addressed 32-byte SBBF blocks (guards + values), same key derivation
    as the pruner (`composite_value_key` / `composite_guard_key`), `None`
    (keep) for partial fields, uncovered composites, missing sections.
  - `bloom_pruner` stage 1b (`file_probe.rs`): files with `bloom_ver <= 0`
    and a sidecar are probed on the follower before the group pass, under
    `bloom_prefetch_concurrency()`, a 2 s stage deadline and a 64 MiB
    byte-bounded LRU of section tables (a `None` entry remembers blob-less
    sidecars). Dropped files never reach the eval loop, so no fallback
    tally, no skip-rate bail, no ERROR line. New counter
    `vix_file_bloom_probe_files_total{outcome}`; the `search->bloom` line
    gains `per-file blooms: probed/dropped/kept (hit, no_info, no_blob,
    no_sidecar, failed, timed_out)`. Index-less logs L0s
    (`ZO_VIX_L0_INDEX_OFF_STREAM_TYPES=logs`) stay `no_sidecar` — the logs
    side of a needle lookup still scans until the L0s merge (P3 below).
  - `AllConditionsSkipped` in the eval loop is a deterministic capability
    outcome: logged at debug with reason `unservable`, no longer
    `reason="error"` at ERROR level (15,740 lines in 40 min).
  - Compactor `compact::bloom::run`: the queue stops at the live merge lane
    (`settled_hour_cutoff(now, ZO_COMPACT_LIVE_LOOKBACK_HOURS)` → `date <
    now − 2 h`), and each node walks its 300-bucket batch in a shuffled
    order. The recent hours are the querier's job (stage 1b) until they
    settle; the pass log line carries `settled before <hour>`.
  - Verification: vortex_index `bloom_probe` 4 tests (real sidecar with
    demoted `trace_id`: inserted values hit through the composite, < 2 %
    false positives, policy off → None, unknown field → None; ranged
    header walk == in-memory parser; partial section untrusted; blob-less
    sidecar → None); search `bloom_pruner` 31 tests incl. the end-to-end
    `files_without_bf_prune_via_their_own_sidecar_bloom` over sidecars
    fetched through the cache ladder (holder kept, covered miss dropped, IN
    = OR, AND across predicates, index-less + blob-less kept, scope
    respected, section tables memoized); `vix::` 189 tests; openobserve-core
    `compact::bloom` 19 tests. Prod A/B to redo after the querier release:
    target ≤ 2 s for the 3 h lookup (setup 3.6 s incl. the tail), the
    `no_bloom` count → 0 on traces, and `ResourcesExhausted` gone.
- **Rolled 2026-09-28 as `v0.93.0-vix-20260928.174`** = vix-arch `c62938595`
  (ECR index `sha256:9a1b0a25…`, binary `e621edf0…`), the first image every
  role shares (owner call: converge, the `.165`/`.170`/`.173` lineages had
  drifted 11k lines apart in `vortex_index`). GitOps PRs #565 querier
  18:36Z, #566 compactor 18:41Z, #567 ingester/router 19:11Z (STS one pod
  at a time, 600 s grace).
  - Querier: 10/10 at 18:37:49Z, 0 restarts. Same trace id, same 1 h
    window `[17:20, 18:20)`, 1 hit: `.173` 10,442 / 11,600 ms scanning
    **2.23 TB / 500 M rows** → `.174` cold 3,336, warm **1,743 / 1,690 ms**
    scanning **1.5 GB / 502 k rows** (O2 1,049 ms; its other run tripped
    `MemoryCircuitBreakerError`). Per follower `input=154 (with_bloom=6,
    without_bloom=148)` → `per-file blooms: probed=148, dropped=148` (the
    holder's follower `kept=1 (hit=1)`, 292–331 ms cold), scan branch
    `load files 1` (69 MB compressed), `fast path fallbacks 1 (unservable:
    1)`. Fleet counters after 10 min: `file_bloom_probe` dropped 5,070 /
    hit 3 / no_sidecar 2,247 (logs index-off L0s — P3) / failed 0 /
    timed_out 0. Residual 1.7 s = leader `get file_list` 440 ms (fresh
    pods, file_list cache cold; 16–27 ms on warm pods) + follower setup
    640–700 ms (probe ~300 cold + segment tail 69–83 segments `kept 0`).
    Fresh-pod cold caches produced 32 growth timeouts / `budget_refused` in
    the first 10 min (the known cold pattern; recheck warm). No
    `error filtering via index`, no `ResourcesExhausted`.
    Warm re-measure 20:02–20:17Z (100 min after the roll): traces/default
    45 lookups, probed 2,686 → dropped 2,670, hit 16, no_info 0; stage p50
    **188 ms** / p90 421 ms / max 777 ms (≈ 3 ms per file — the open hour
    keeps landing sidecars each pod opens once); logs/default 55 lookups,
    probed 2,402 → no_sidecar 1,596 (index-off L0s), no_info 700 (auto-id
    predicates such as `user.id`/`user_id`/`sandbox_id` on files whose
    composite does not cover them), p50 18 ms. The cold-pod no_info burst
    (1,509 on traces 19:33–20:03Z, p50 384 ms) came from those auto-id
    lookups; not worth restricting the stage to explicit fields at 18 ms
    warm. Growth timeouts flat at 53 since 20:03Z (fresh-pod cold caches).
  - **`.175` rolled 05:03Z (GitOps #568, querier-only)** with
    `ZO_STORAGE_SCAN_MAX_BYTES=4 GiB`. Same 6 h `SELECT * … WHERE body =
    '<absent>' LIMIT 50` (logs, `[22:40, 04:40)`): `.174` **18.7 s, 6.58 TB /
    604 M rows, `is_partial=false`** → `.175` 11.0 s cold / **2.5 s warm,
    1.65 TB / 152 M rows, `is_partial=true`**, `function_error` per follower
    `storage scan budget: 149 files (10.50 GB) of 206 were skipped … results
    cover the NEWEST 57 files (3.98 GB) from 2026-09-29T02:13:49Z onward`.
    `query_storage_scan_capped_total` 30 logs + 10 traces after the two
    runs (each follower counts once; the traces counts are the traces
    table of the same star query). Needle lookup unaffected: 1 h trace id,
    47 hits, 2,178 cold / 1,191 ms warm, `is_partial=false`. 0 restarts.
  - Compactor: 30/30 at 18:42:23Z (1 restart = `nats.rs:604`), lease
    recovery done 18:58Z (90 running / 30 nodes). `.bf` passes: **27 passes,
    median 395 s, max 445 s (was 2,958–6,227 s), busy 258 of 8,100
    attempted (3 %, was 65–74 %)**; the remaining ~1.3 s/bucket is the
    serial lock+query+unlock walk of 300 buckets. Candidate window
    `[19:00, 19:10)`: 1,214 merges / 17,501 inputs / 1.46 TB, indexed active
    **206.7 MB/s** (baseline 222.4, same band; overall 146.9 with 490
    metrics merges of 14,983 tiny inputs), 0 OOM, max RSS 13.6 GB. 3
    failures / 2,161 merges in 30 min: 2 metrics `open core file` on an S3
    **503 SlowDown** burst 19:00–19:01 (48 `get_opts` errors — 30 cold
    compactors claiming at once) + 1 known `refusing a large full rebuild`.
  - Ingester/router: router at 19:1xZ; ingesters 4→3→2 by 19:41Z, 1 and 0
    following. ingester-3 19:39:30Z: 4 × 503 `segment buffer full: 536 MB of
    536 MB — object storage flushes are behind` (`aws_waf_log`, 2,296 records
    for the client to retry) while 2 of 5 ingesters were out of rotation —
    open item #64's shape, roll-time only so far.
- P4 shipped as **`.175` = vix-arch `aca0a11c2`** (querier-only change; ECR
  index `sha256:0337b985…`, live 05:03Z — evidence above):
  `ZO_STORAGE_SCAN_MAX_BYTES` (default 0) caps the follower's storage scan
  branch by Σ `compressed_size` in `storage::search` before any IO, keeps
  the NEWEST files that fit (≥ 1), re-measures `scan_stats`, and ships a
  `StorageScanShortfall` message through `PartialErrRefEarly` so the leader
  reports `is_partial=true` + `function_error` naming skipped files/bytes
  and the coverage boundary (`SegmentShortfall`'s shape; leader untouched:
  `decoder_stream.rs` → `ScanStatsVisitor`). Counter
  `query_storage_scan_capped_total{organization,stream_type}`. Prod value 4
  GiB in `querier-deployment.yaml` (the incident's 9.9 GB compressed per
  follower ≈ the whole 12.9 GB pool; 4 GiB leaves room for two concurrent
  wide scans). Tests: `search::grpc::storage::tests` 4 (newest prefix +
  message, no-op within budget / off, ≥ 1 file kept).
- Still open: P2 Segment-WAL tail pre-fetch pruning — shipped as `.176`
  and rolled back the same morning (meta-DB write stall; see the
  2026-09-29 section: ranges go inside the segment row next); P3
  bloom-only sidecar for logs L0s (2,247 `no_sidecar` probes in 10 min —
  measure builder CPU under the CHUNK_MB 512 brake first); `.bf` pass
  parallelism (300 serial bucket attempts ≈ 6 min).

## 2026-09-25 — aggregate gap vs O2: data-only counts, waiting growth, un-droppable warming (.171 → .172)
- Fix round for the root cause below (`root cause of "still slower than
  O2"`). Querier release line `release/vix-20260925-172` = the .166 snapshot
  (`0b076c91a`) + three commits, so the prod delta is exactly this work
  (vix-arch `3351e3188`, `a510385ec`, `8564a7035`):
  - `SimpleCount` over condition ALL evaluates data-only like the ALL
    histogram (row_count, zone table, docs `_timestamp` chunks — all
    data-side); the follower gate `data_only_vix_capable` admits index-less
    L0 files to it. Regression `unfiltered_straddling_count_never_opens_the_
    sidecar` runs with the sidecar object deleted.
  - The straddling clamp is charged at its point of use
    (`clamped_timestamp_bitmap`, boundary rows × 16 B + bitmap from the zone
    table) instead of 24 B/row declared before open.
  - Fast-path fallbacks are tallied per follower by reason (`fast path
    fallbacks N (reason: n, …)` at info) and in `vix_fast_path_fallback_total`.
  - The latest-files downloader bounds ACTIVE bytes only; queued fills are
    count-bounded and wait for headroom (`DownloadReservation::activate`)
    instead of being rejected — a broadcast burst used to drop every fill
    past ~4 objects of the 1 GiB budget, permanently (60 % lifetime sidecar
    miss rate).
- `.171` (first two items) on fresh pods exposed the real fallback cause at
  once: `fast path fallbacks 236 (budget_refused: 236)` — 100 % of files.
  `ByteGate::acquire` packed the evaluation budget to the last byte and
  `try_resize` refused any growth while queued evaluations waited; a
  refusal is sticky (`check_refusal`) and turns an index-answerable file
  into a DataFusion scan. Moving the clamp from declaration to growth made
  the pre-existing 20–40 % refusal rate universal.
- `.172` adds waiting growth: `ByteGate::with_growth_headroom` keeps 1/8 of
  the evaluation budget for growth (admissions stop at limit − headroom; an
  idle gate admits one oversized lease), `BytePermit::resize` waits on a
  condvar for releases up to 2 s polling cancellation, then refuses
  (`vix_eval_growth_timeouts_total`). The fetch gate is unchanged.
- Verification: search 1,102 tests, infra downloader 11, api event 5, core
  flight 17; `.166` baseline battery at 09:10Z for the A/B (obs warm / O2
  warm: count 1 h 906/539, 1-min hist 1 h 1,890/764, count 24 h 771/483,
  logs count 24 h 748/203, logs 30-min hist 24 h 3,589/699, logs count 1 h
  268/102).
- `.172` live 10:08Z (GitOps PR #562 / `47ea8db37b62`, 10/10 queriers; the 3
  restarts are the NATS-connect-at-startup panic `nats.rs:604`). Battery on
  4-minute-old pods, sealed windows ending 10:10Z, obs warm / O2 warm
  (baseline `.166` ratio → `.172` ratio):
  traces count 1 h 768/676 (1.68× → **1.14×**); 1-min histogram 1 h
  478/738 (2.47× → **0.65×**); 5-min histogram 3 h + service 1,055/1,101
  (1.05× → 0.96×); count 24 h 656/793 (1.60× → **0.83×**); logs count 24 h
  206/272 (3.68× → **0.76×**); logs 30-min histogram 24 h 826/786 (5.13× →
  **1.05×**); logs count 1 h 528/119 (2.63× → 4.44×, see below). All 200,
  no partial, equal buckets. Warm runs: **zero** fast-path fallbacks; the
  99 `budget_refused` fleet-wide came from cold r1 runs on empty caches
  (79 of 2,331 files on an 18 s all-remote logs histogram), where
  evaluations hold permits for seconds and the 2 s growth wait expires —
  the intended fail-safe. Cold r1 on brand-new pods is remote-bound
  (logs 24 h histogram 18.2 s) until the disk cache fills; the downloader
  change is what shortens that, not measurable in a 4-minute-old fleet.
- 11:10Z re-run on 66-minute-old pods, obs `.166` warm → `.172` warm: count
  1 h 906 → 532 ms, 1-min hist 1 h 1,890 → 665, 5-min hist 3 h + service
  2,084 → 746, top-50 15 m 757 → 385, count 24 h 771 → 493, logs top-50 1 h
  1,504 → 1,109, logs count 24 h 748 → 403, logs 30-min hist 24 h 3,589 →
  390, logs count 1 h 268 → 307. APM ops 1 h 689 → 2,815 is NOT the query
  change: same `skip vix search` → DataFusion scan path at 92–96 % disk
  cache on both, but 263–320 files per follower vs 100–140 — the traces
  1 h window holds 2,110 files (1,848 L0) vs 617 (1 L0) at 04:38Z, the
  builder brake's open-hour L0 multiplication (settles in the 2 h recent
  lane; reverts with the L0-writer residency fix + `CHUNK_MB` 512).
  Cold r1 on `.172` is not comparable to `.166`'s weeks-warm pods (empty
  ephemeral caches: logs 24 h histogram 11.0 s cold).
- 11:45Z owner decision: `ZO_SEGMENT_BUILD_CHUNK_MB` back to **512** with the
  compactor `ZO_SEGMENT_BUILD_MEMORY_BUDGET_MB` **8192 → 4096** (GitOps PR
  #563 / `e7008c70da07`, env-rev `2026-09-25-segment-chunk-512-budget-4096`).
  Rationale: builder memory = concurrent builds × per-build residency, and
  the budget admits decoded bytes, so halving it halves concurrent 512 MiB
  builds. 37-minute gate: max process RSS **19.4 GB** (128 brake: 17.8;
  512/8192: 31–36 with 60 GiB excursions), **0 OOMKilled**, 4 restarts all
  the NATS startup panic; L0s since the roll average 1,186 MB (355 L0 in the
  partial hour vs 2,800 in hour 11); builder backlog healthy (111 pending,
  oldest 9 min; 677 claimed, oldest 2 min; `admit_ms=0`). Build parallelism
  per batch dropped (build_sum/wall ~1–4 vs ~6) with no visible backlog
  cost at 30 pods. Rollback gate stays: RSS > 40 GB or any OOMKilled →
  CHUNK_MB 128. Restore 8192 once the L0 writer's residency is charged to
  the budget in code.
- `.173` (13:46Z, GitOps PR #564 / `9e34575fc025`, + `ZO_VIX_EVAL_MAX_BYTES`
  4 GiB on queriers): `.172`'s 2 s growth wait had become the dominant
  straggler — 151 `vix_eval_growth_timeouts_total` in 3 h (~0.2 % of ~73k
  file evaluations, 1:1 with `budget_refused`), each stalling its QUERY 2 s
  where the old instant refusal cost a 30–300 ms one-file scan (logs count
  1 h warm 2,315 ms with one follower at idx 2,023). Admitted evaluations
  grow at once against a 128 MiB shared headroom. Fix: the straddling clamp
  is declared at admission again (predictable growth queues, never competes
  for headroom; point-of-use charge + `timestamp_range_boundary_rows`
  removed), GROWTH_WAIT 2 s → 500 ms, budget 1 → 4 GiB (queriers at 6.5 GB
  RSS of 24 GiB). Battery on the 20-minute-old `.173` fleet: 0 growth
  timeouts; latencies NOT comparable (each roll resets ephemeral caches;
  O2 hit `MemoryCircuitBreakerError` in the same window and swung 2×).
- After `CHUNK_MB` 512 (11:45Z): traces L0s 1.2–1.3 GB, logs L0s ~2.6 GB;
  the 1 h traces window fell from 2,110 files (1,848 L0) at 11:10Z to 1,171
  (680 L0) at 13:50Z while the merge lanes drain the brake-period flood
  (hour 11: 2,262 → 395 L0; hour 12: 2,098 → 202; all 90 slots busy,
  ~240 traces merges / 45 min on hours 11–14). Scan-branch APM 1 h tracked
  the file count: 2,815 → 1,880 ms (13:10Z). Definitive `.173` numbers need
  the L0 flood drained and caches warm (~15:30Z).
- Remaining gap, logs count 1 h: every follower answers all 33–46 files
  from the index in 2 ms; follower time is now the Segment-WAL tail scan
  alone (63–319 ms, the straggler `fetch-wait 562 ms` on 6 remote
  segments; leader total 442 = max follower + 100). O2 answers its live
  tail from ingester memory in 2 ms. Next item: take the segment tail off
  the follower critical path (leader-side ingester/segment lane, or
  memory-resident open-hour segments on their owning follower).

## 2026-09-24 — compactor OOMKills: passthrough docs writer residency was width-scaled (.170, live)
- Production evidence: after the .170-candidate compaction-policy rollout to
  30 compactors, 5 pods were `OOMKilled`/evicted at the 60 GiB limit within
  ~1h; a live pod sat at 43 GB with three concurrent logs/default merges of
  ~0.6–0.9 GB-original inputs whose log lines were only "field X widened".
  Reproduced locally on 5 real inputs of `logs/default/2026/08/29/01`
  (3.75 GB original, 393k rows, 3,029-column union, 160 MiB compressed):
  `merge_bench merge --indexed-only --stored-schema` peaked at 5.83 GB RSS
  for a 113 MiB output; widening was irrelevant (5.85 GB with 12 widened
  columns, 5.83 GB without).
- Chain, all in `ClusteredDocsStrategy` (`src/vortex_index/src/clustered.rs`):
  every pushed struct chunk adds one decoded window to every column's open
  coalescing run; the per-column caps (128Ki rows / 4 MiB) bound one run but
  never their sum, so ~1,500 decoded string columns held ~3 GB of canonical
  16 B/row views; all columns crossed the row cap on the same struct chunk,
  so the close was a storm of ~1,500 concat tasks spawned without
  backpressure holding another ~3 GB while the CPU pool drained them; the
  widen plan's per-chunk all-null constants (1,461 per chunk here) were
  concatenated through canonicalization into materialized null arrays.
  M25's compact-for-residence cannot help (views dominate); forcing it on
  every window made it worse (8.4 GB).
- Fix, format-neutral (readers see per-column `ChunkedLayout`s exactly as
  before; row multiset + digests equal to the pre-fix output):
  - writer-wide pending budget `COALESCE_TOTAL_BYTES` (resident post-compaction
    bytes of non-constant parts): when exceeded, every run holding ≥ 1/(4n)
    of it closes at that row boundary (equal rows per leaf — flush-by-heaviest
    fragmented dictionary-friendly columns, +16% bytes on `log.file.path`);
    sparse columns below the floor keep coalescing to their caps; exempt runs
    sum to ≤ budget/4 so a close always frees the rest;
  - in-flight admission `INFLIGHT_TOTAL_BYTES` (tokio semaphore, 64 KiB
    units, permit dropped inside the CPU leaf): a close storm drains at the
    pool's pace instead of queueing every part at once;
  - a run of equal constants closes as ONE longer `ConstantArray`
    (`merge_constant_run`), never materialized row by row.
  - knob `ZO_VIX_DOCS_RESIDENT_BUDGET_MB` (default 1024) → pending budget;
    in-flight = half; plumbed `VixWriterOptions::docs_resident_budget` →
    `DocsBlobEncoder::spawn` → `docs_passthrough_strategy`.
- Measured on the same inputs (RSS / wall / docs blob / leaves / footer):
  baseline 5.83 GB / 3.78 s / 113.2 MiB / 15.2k / 0.9 MiB;
  budget 512 MiB 1.93 GB / 4.02 s / 112.4 / 37.2k / 1.9;
  **1 GiB (default) 2.62 GB / 3.68 s / 112.2 / 25.3k / 1.4**;
  2 GiB 4.26 GB / 3.60 s / 111.8 / 17.5k / 1.0. Peak per merge is now
  ~1.5× the knob plus base, independent of row count and width.
  `merge_bench leaves <file.vix>` prints the per-column leaf census.
- Verification: vortex_index 344 tests incl. new
  `clustered::total_budget_closes_heaviest_runs_first` (heavy column closes
  early, narrow column keeps row-cap leaves, one-unit in-flight budget
  completes on the single-thread driver, rows round-trip); core
  `vix::core_writer` + `compact::` suites; `merge_bench compare --multiset`
  pre-fix vs post-fix outputs equivalent.
- Production (compactor `.170`, release `66fde116d7da`, GitOps PR #559 /
  `a8ad4b252c9a`, 30/30 pods 17:22Z): three workers completing 4–6-input,
  3.5–4.3 GB-original logs/default passthrough merges every ~15 s hold the
  pod's process RSS (`zo_node_memory_usage`) at **2.2–7.1 GB** across a
  2.5-minute merge-only window (pre-fix: 43 GB with three such merges).
  40 minutes in: zero OOMKilled, zero merge-related restarts (three `Error`
  restarts are the known NATS-connect-at-startup panic, `nats.rs:604`; one
  pod was replaced after its spot node went NotReady). `kubectl top`
  shows 24–39 GB working set on these pods — that is page cache of the
  prefetched inputs, not heap.
- Residual, separate subsystem: the co-located Segment-WAL builder
  (`src/jobs/src/job/segments.rs`) still spikes each pod's RSS to
  **31–36 GB** on its batches (63–94 segments, 2.2–4.7M rows, 14–26 L0
  files of 1.1–1.6 GB original built ~6 wide: `file_build_sum_ms` 175–275 s
  in 30–44 s wall). `ZO_SEGMENT_BUILD_MEMORY_BUDGET_MB=8192` admits by
  DECODED input bytes only; the per-build writer residency (vortex default
  `docs_strategy` repartition buffers per column x wide traces/logs unions,
  plus term tables) is unaccounted and width-scaled — the same class of
  problem this entry fixed for the passthrough writer. Under the 60 GiB
  limit today only because merges dropped to ≤ 7 GB; the builder's own
  bound is the next item.
- 2026-09-25 04:30Z follow-up (11 h on .170): merge backlog DRAINED —
  `file_list_jobs` 0 pending / 13 running / 850 done (was 1,092 pending);
  logs/default debt 38 h / 76 files / 0.004 TB (was 457 h / 156,195 files /
  190 TB); merged volume 54.6 TB/h in the first hour tapering to 5–8 TB/h
  as work ran out (10–15 of 90 slot-hours busy). Every logs/default day is
  now terminal ~4 GB outputs. Zero merge errors since 20:00Z.
  Residual heal treadmill: each demoted-widened-field output gets one
  sidecar heal — measured locally on a 3.6 GB-original / 460k-row / 3,194-
  field output: 4.1 s, 1.41 GB peak; 2–9k/h tapering; serialized behind the
  1-slot rebuild gate (workers wait 10–35 s), harmless with an empty queue.
- Builder OOM attribution + brake: all five OOMKills on .170 (09-24 19:12 ×2,
  09-25 02:12, 02:23, 02:39) followed `[SEGMENT:BUILD] super-batch: 67–94
  segments / 257–365 MB` by 10–40 s; merges in flight were ordinary
  passthrough merges. GitOps PR #560 (`81c5152a4921`) applied the
  configmap's documented brake `ZO_SEGMENT_BUILD_CHUNK_MB` 512 → 128 on
  compactors (env-rev `2026-09-25-segment-chunk-128`): the same 89-segment
  super-batches now build 41–47 L0s and the fleet-wide max process RSS in
  the first sample is 17.1 GB (was 31–36 GB with 60 GiB excursions). Cost:
  traces L0s average ~290 MB original (were 1.1–1.6 GB), ~1,400 L0 files in
  the open hour until the recent lane merges them (2 h settlement); the
  1-minute traces histogram over the freshest hour went from 1.6× to 5.1×
  O2 while 1 h count (1.26×), top-N (0.6×) and logs top-50 (0.65×) held or
  improved. Owner-approved sequence: brake now, then bound the L0 writer's
  residency in code (same class as the passthrough fix) and restore 512.
- Query battery 09-25 04:25Z sealed windows, obs warm / O2 warm: traces
  count 1 h 1,127/335 ms (3.4×), 1-min histogram 1 h 886/565 (1.6×), 5-min
  histogram 3 h + service 1,721/819 (2.1×), top-50 15 m 757/886 (0.85×), APM
  ops 1 h 689/1,646 (0.42×), count 24 h 926/270 (3.4×; O2 now complete);
  logs top-50 1 h 1,504/383 (3.9×, cold 11.7 s from index-less L0s: `16 of
  17 files cannot produce exact vix candidates`), logs count 24 h 776/285
  (2.7×), logs match_all('error') count 1 h 257/193 (1.3×), logs 30-min
  histogram 24 h 2,395/635 (3.8×). All HTTP 200, no partial, equal bucket
  counts; per-hour traces counts obs/O2 = 1.007–1.019 for the last 8 sealed
  hours. Dominant obs phases unchanged: per-file sidecar evaluation
  (`index fetches: 233 (30 MB) 1.2 s` per follower, `IndexOptimizeExec over
  1,251 core files`, 13,354 files/24 h traces) plus the remote segment tail
  on cold runs (`cache memory/disk/remote 0/0/60`, fetch-wait 10.8 s sum).
- 2026-09-25 05:50Z root cause of "still slower than O2" on aggregates:
  **window edges, not data volume.** Same traces `count(*)`, warm, obs/O2:
  hour-aligned 03:00–04:00 **116 / 151 ms** (obs faster; 505 files, 0 index
  fetches — counts come from file_list `records`); offset 03:25–04:25
  **475 / 559 ms** and 1,127 / 335 in the 04:38Z battery. Every real query is
  offset ("last 1h" ends at now) and 449 of the window's 667 files straddle
  an edge (merged recent-lane outputs span ~35 min; 243 straddle the start,
  175 the end). For those the fork reads sidecar footer+zone table and the
  boundary chunks' `_timestamp` through ranged reads: 728 fetches / 115 MB
  fleet-wide for one 1h count. When the bytes are local that costs 18–35 ms
  per follower (9 of 10 followers, `remote_reads=0`); the query time is the
  10-way fan-out max(), and the slowest follower is always the one reading
  from S3: pod `hmttz` (15 min old, empty ephemeral cache, 8/50 sidecars
  cached) did 48 remote GETs, idx 318 ms, setup 403 ms → total 469 ms vs
  ~230 ms without it; at 04:38Z every follower was at 15–19 % cached for
  the freshest hour (download lag) and idx took 108–408 ms per follower.
  Mechanisms that keep the bytes remote: (a) `get_range_classified`
  (infra/cache/storage.rs) is memory→disk→remote with NO write-back, so a
  ranged read never warms anything; (b) whole-file sidecar warming is the
  broadcast downloader (1 GiB queued+active budget, no retry on reject),
  which lags the freshest hour and restarts from zero on pod replacement;
  (c) `zo_vix_reader_cache` is full at 867 entries / 2.15 GB (≈2.5 MB
  accounted per reader for KBs of useful metadata), 35 % hit rate; (d)
  lifetime sidecar disk-cache miss rate on a 17-day-old querier is 60 %
  (3,325 hit / 5,080 miss). O2 evaluates the same straddling files with
  tantivy's `_timestamp` fast field from a 100 % local-disk index (its cold
  pod r-5, 0 % cached, was likewise its straggler at 546 ms).
  Secondary obs-only costs per follower: segment-WAL tail scan 78–113 ms
  (266 open-hour segments; O2 ingesters answer from memory in 2 ms); 96 of
  the 449 straddling files (21 %) leave the SimpleCount fast path for a
  DataFusion `_timestamp` scan (27–58 ms here, ~300 ms at 04:38Z) — the
  reason is only logged at debug (`exact aggregate scan required`), likely
  candidate: `EVAL_BYTES` growth refusal (`try_resize`) under 64-way
  contention (`evaluation_wait_us` 68–167 ms on the affected followers,
  0 on the follower with zero fallbacks). Leader dispatch overhead 12–135 ms.
  Fix direction: retain per-(file, generation) footer+zone metadata in a
  memory cache charged by real bytes; write ranged sidecar reads back to
  the disk cache; make the fast-path fallback reason observable; keep the
  segment tail off the follower critical path.

## 2026-09-24 — compactor refusal storm: plan-before-prefetch, widening casts, refusal backoff (.169 candidate)
- Production evidence (Orbit, 2026-09-24 07:00–08:00Z, 16 compactors × 3 slots):
  15,641 prefetches (68,776 s ≈ 19 slot-hours/h), 13,307 failed batches
  (85%), 1,806 merges, 532 sidecar heals. Every sampled failure was
  `required indexed merge is not applicable; refusing a large full rebuild`
  over a registry type widening — logs/default `after`/`quantity` f64→utf8,
  traces/default `gen_ai.usage.total_tokens` i64→utf8 — raised AFTER the
  whole-object prefetch (2–10 objects, 1–30 s each). Jobs re-pended at once
  and were re-claimed every cycle (`generation=266` observed); onset
  2026-09-17 ~13:00Z with the .168 guarded-indexed-target rollout.
- Chain: `build_merge_plan` types output columns from the registry; the
  concat qualification was all-or-nothing per input and `docs_widen_plan`
  refused any stored-dtype difference; a concat-ordered input forced
  concatenation, so the miss became a rebuild fallback that `IndexedOnly`
  (indexed groups > 1 GiB original) must refuse; `cache_remote_files` ran
  before planning; failure was not persisted.
- Fix, format-neutral:
  - `DocsWidenPlan` casts a widened column per chunk (`widening_cast_supported`:
    number/bool → string family, integer → Float64, narrower integers → Int64;
    the same arrow cast the decode path applies). Other columns still copy
    encoded; unsupported pairs (narrowing, → bool) still take the decode path.
  - Merge-mode writer demotes term-planned fields whose input stored type is
    non-string while the output is text (`DocsTypeFlip::breaks_value_terms`):
    the input's tagged numeric terms cannot answer raw string probes against
    the cast values, so the field is filter-back until a heal re-derives it.
    Numeric widenings keep capability (canonical tagged terms stay
    query-compatible; `NumericCmp` probes the int and float spellings).
  - A concat-ordered input no longer turns a per-input qualification miss
    into a whole-merge fallback: the concat copy proceeds and unqualified
    inputs decode in place (the disjoint copy's existing per-input fallback).
  - `preflight_core_merge` / `execute_core_merge` split the compactor's CPU
    phase: `IndexedOnly` refusals (no readable sidecar, `check_merge_inputs`)
    are raised from footers alone, typed as `IndexedMergeRefused`;
    `merge_core_group` runs PLAN → PREFETCH → EXECUTE (healing batches still
    prefetch first). Refusals count in `compact_merge_refused_total`.
  - A job whose failure chain carries `IndexedMergeRefused` re-pends with
    `updated_at = now + ZO_COMPACT_REFUSAL_BACKOFF_SECS` (default 1800); the
    pending claim now filters `updated_at <= now` (postgres + sqlite). Other
    errors retry immediately as before.
- Verification: vortex_index 343 tests; core `vix::core_writer` + `compact::`
  272 tests incl. new `concat_merge_casts_widened_numeric_input`,
  `indexed_only_concat_merge_widens_numeric_to_text` (the production shape
  completes on the fast path under `IndexedOnly`, `code` demoted, rows equal
  to the rebuild oracle) and
  `indexed_only_refusal_happens_in_preflight_without_docs_reads` (lowest
  fetched offset stays inside the tail window of a multi-MB input); infra
  file_list 62 tests. Known residual, unchanged: the rebuild path derives a
  widened field's terms from `_source` (tagged numeric), so raw string probes
  on it miss numeric-origin rows — pre-existing type-widening semantics.

## 2026-09-08 — engine .166 production rollout and measured acceptance
- Querier-only image `v0.93.0-vix-20260908.166`, source `0b076c91a9bf`,
  deployed through GitOps PR #548 / merge `84107245f999`. The owner explicitly
  authorized this PR's existing merge-rule exemption after normal merge was
  rejected; repository rules were not changed. Ten queriers are Ready at
  index digest `9692385ef3fd`, old ReplicaSets are zero and querier restarts
  remain zero. Ingester/router .165 and compactor .160 are unchanged.
- Production result caching remains disabled. Querier replacement Pods own
  fresh generic ephemeral PVCs; v5 key isolation is not needed for this
  specific rollout. It remains a format marker for retained-disk upgrades,
  separate from the half-open coverage correction. No cache migration,
  manual node scaling, scheduling relaxation or resource adjustment occurred.
- Baseline completed 14/14 requests. Candidate initially completed eight
  before a histogram timed out during Spot loss of the unchanged .165
  ingester-3: all ten .166 queriers finished in about 2.1s, but the leader
  lacked that receiver. CSI later detached/re-attached its retained EBS
  volume automatically. A separate six-request recovery completed original
  workload coverage; the interrupted phase remains recorded as failed.
- Sealed service count/error totals are exactly 600,004,644 / 64,866.
  Direct/residual controls preserve 72 histogram buckets and 100 ordered
  row signatures. Recent direct/residual counts agree; the recent corpus
  gained one error span. All successful requests are nonpartial/error-free.
- Three-round observed medians: operations 6.585 to 4.262s; metrics 4.145
  to 3.566s. Not every request improves: first metrics +55.12%, recovered
  direct histogram +32.06%, and reported APM peak memory roughly +19–21%.
  Compaction, Spot replacements, cache/placement changes and shared traffic
  prevent attributing those deltas solely to the engine.
- Approximate p99 values are not identical. Two exact-count rank diagnostics
  show selected operation ranks shifting from 99.275–99.297% to
  99.324–99.353% (worse current-corpus rank error); the selected metrics bucket
  moves from 98.893% to 98.938–99.052% (closer to 99%). No arbitrary tolerance
  or historical duration-multiset identity is claimed. An initial diagnostic
  with redundant COUNT/cast-COUNT hit duplicate-field HTTP 400; that failure
  is retained, and only the diagnostic SQL was simplified before the two
  successful oracles. No baseline replay establishes it as a new regression.

## 2026-09-08 — engine planning, ownership and bounded conversion (local verification)
- Reuse trusted file statistics in the existing query-owned registry; clear
  derived storage/WAL/schema registrations through an explicit root owner.
  Plan exact-selection and residual branches once, project typed NULLs only
  for complete-schema absence, and run applicable Bloom pruning before
  eager index aggregates.
- Prepared SQL shares immutable metadata but binds each delta's time range
  separately. Dispatch matrices/plan bytes, owned validated Flight buffers,
  TDigest input scratch and final JSON/cache ownership avoid redundant copies.
- A proven single exact term can reject a file or answer all rows without
  decoding postings. Dense refusal retains the existing bitmap threshold
  and authoritative SQL residual; it is not a fitted global crossover.
  Native string equality runs before final projection when eligible.
  Whole-sidecar warming follows a cache miss and actual sidecar use.
- Ordered Arrow conversion uses admitted CPU leaves, not waiting controllers
  or IO jobs. Automatic parallelism requires at most four unsplit files,
  spare worker capacity, a ranged unselected native projection and supported
  bounded conversion trees; ordinary conversion remains the fallback.
  PCO accessors are a narrow patch to pinned vortex-pco 0.79.0, not a format
  change. Queue wait, conversion, callback/downstream work and bounded
  in-flight state are recorded separately; they are not process RSS.
- Recent Segment-WAL scans reuse query-local schema/predicate/projection
  plans, select before final gathering, and retain Exact/Deferred provenance.
  MemTable projection=None returns every output column.
- Correctness found during integration: result-cache v5 records certified
  half-open coverage. Gap fetches start at cached_end, preserving the row
  exactly at a seam. Saturated timestamp ties and incomplete histogram
  buckets remain uncovered; unsafe time axes, pagination, ordering and
  pre-cache transforms bypass caching. Legacy v4 entries are not reused.
- Actual local HTTP/gRPC/Flight proof passes 153 requests, plus the separate
  17-request cache-boundary scenario. Both immutable VIX files complete
  90 offloaded conversion leaves, peak four per file, with no inline
  fallback or cancellation. Complete-query balanced warm runs preserve
  every typed result and all p50/p95/p99 values: operations median
  117.27 to 112.61 ms, metrics 71.15 to 69.08 ms, unfiltered wide aggregate
  154.08 to 134.11 ms (four measured requests per source/workload).
  Ranges overlap; these are local current-state observations, not production
  speedups, cold-storage measurements or isolated optimization effects.
- The same API runs report higher DataFusion peak reservations: operations
  about 139 to 218 MB, metrics 134 to 213 MB, and wide 134 to 703–709 MB.
  Reader high-water admission and conversion/output ownership now consume
  pool headroom. These observations are not measured heap/RSS peaks; the
  bounded concurrency benefit must be weighed against reservation pressure.
- Final semantic gates pass: search 1,100, VIX 342, Flight 45, cache-boundary
  SQL 3 and recent Segment-WAL 40 tests. Integration tests use actual
  registered Parquet scans and production listing configuration; native
  equality checks retained/pruned rows and SQL residual results rather
  than private registry keys or fallback-key encoding.
- A separate native two/six-file matrix passes 48 complete executions and
  12 allocation diagnostics. Same-shape results, including every percentile,
  are identical between sources; triplicated populations retain their own
  unmasked percentile changes. Rust System live-allocation peak for the
  wide two-file scan is 94.95 to 106.31 MiB, versus 103.23 to 100.43 MiB
  for six files. Every diagnostic releases all pool reservations after
  collection and teardown. These allocator observations exclude direct C
  allocations and are neither production mimalloc nor scoped RSS peaks.
- Graviton3 calibration preserves exact results across index, bounded
  prepass, direct, native and native-exact paths. Warm local/memory favors
  index for the two measured service densities; the pooled remote broker
  favors direct. The broker still adds IPC/Python/cross-region overhead.
  No universal density threshold, production resource/cache/concurrency
  change, Orbit/frontend change, rollup, or production rollout is claimed.

## 2026-09-07 — CASE/full-text output mapping corrected (.162, production verified)
- `RewriteMatchPhysical` must restore the original filter output as an
  ordered list, including repeated slots. Only the expanded scan-input
  projection is a set. Removed output sort/dedup; input ordering and
  predicate-index remapping remain unchanged. This prevents positional
  parents from exchanging same-typed values or losing repeated columns.
- The .161 production acceptance exposed container/body values exchanged
  in CASE full-text rows. The faulty rewrite and its activation order were
  unchanged from .160. No stored-data mutation or client-side field swap
  was used as a remedy.
- Source `1ef991a3f257` is a one-file correction on .161's `a7a1ea1617b4`.
  Both new regressions failed before correction. The complete search suite
  passed 1,076 tests (7 ignored); all three rewrite behavior tests passed
  after strengthening the fixture so a hidden FTS input shifts the named
  predicate's column index. Repeated output slots execute under positional
  alias parents, rather than relying on schema-only assertions.
- Production queriers now run `v0.93.0-vix-20260907.162`, via GitOps PR #543
  and merge `6cb8adf9d6cb`. All ten updated/Ready/available pods match the
  published digest; old ReplicaSets are zero and no restarts occurred.
  Compactor, ingester and router remain .160; all other resource, cache,
  ConfigMap and Secret state matches the validated 15-resource render.
- Twelve bounded production requests returned HTTP 200, non-partial,
  error-free and with result-cache ratio zero. Original LIMIT 50 and
  LIMIT 10 direct/CASE projections now have identical ordered tuple hashes
  without normalization; duplicate body aliases retain both correct slots.
  The 20:04–20:05 UTC comparison returns 18,430 for both count and histogram.
- The same 2026-09-06 20:00–20:10 UTC histogram returns 93,267/91,535 in its
  two five-minute buckets, taking 2.786s first / 1.387s subsequent. These
  are end-to-end current-state measurements with new pod-ephemeral caches,
  not an isolated CPU or physical-disk benchmark. The .160 baseline timed
  out at 45s; .161 had already restored residual filtering and query speed.
- Separate known issue remains out of scope: table-qualified `_timestamp`
  can trigger duplicate automatic timestamp insertion. The original CASE
  acceptance queries use the valid literal projection and do not hit it.

## 2026-09-07 — bounded dictionary points and active FTS scope (local verification)
- Point evaluation consumes all active probes for a dictionary block in one
  forward scan. Borrowed targets stream through 4,096-target/8 MiB batches,
  splitting between blocks; indivisible larger groups require separate
  admission. Contiguous fetch runs never coalesce across unrelated gaps.
  The shared FIFO remains capped at 1,024 blocks; no format change or index
  rebuild is required.
- Removed eager whole-query dictionary prefetch. Narrow missing conjuncts
  now avoid unrelated vocabulary reads. Planner, payload, and ordinal
  ownership use the existing memory admission and cancellation scopes.
- `QueryParams.full_text_fields` carries current settings/default fields
  filtered against the latest schema. `VixQuery::FullText` scopes unnamed
  text leaves without changing named raw predicates or generic any-field
  semantics. Unknown scope or incomplete token capability retains the scan
  branch; only exact absence permits omitting an active field. Result and
  bitmap cache identities include the active scope.
- Nonexact aggregates still require the SQL residual; token candidates are
  not phrase counts. Typed source cancellation escapes capability probing.
  CPU-leaf entry state is set outside `debug_assert!`, so the existing
  nested-submission protection also executes in release builds.
- Local proof: both root regressions fail on `ce1956e` (dictionary rereads
  and 28.4 MB of unrelated reads for a missing narrow conjunction).
  The fixed release VIX library passes 324 tests (14 ignored); the search
  library passes 1,078 (7 ignored). The affected core test targets type-check.
  Coverage includes wide dictionaries, legacy/current field directories,
  malicious gap coalescing, bounded admission, cancellation/reuse, scope
  changes, and exact residual/aggregate fallback ownership.
- Two immutable production sidecars retain exactly 3,704/4,239 candidates;
  complete enumeration of their 598,820/544,892 terms and postings agrees
  with the exact candidate row IDs. Reused-reader logical ranges fall from
  11,212/10,997 to 52/47 for generic queries, and 7/6 with explicit FTS scope.
  Async local-file candidate evaluation falls from roughly 350 ms to 6 ms
  generically and 1.1–1.3 ms with scope. These are warm-OS-cache component
  measurements, not physical S3 requests or complete histogram latency:
  residual data reads, timestamp bucketing, and distributed work are excluded.
- No production deployment, configuration change, or full-day query replay.

## 2026-09-06 — native setup and exact sparse aggregation (local verification)
- Native reads share only built-in registry templates within one read
  operation. Sessions, memory/runtime state, and executors remain fresh;
  decoded layouts, payloads, and cancellation state are not shared across
  queries. Registry admission includes a CPU-scaled allowance; it is an
  engineering estimate, not a universal allocator or RSS bound.
- Sparse group/timestamp point reads use one aligned projection. A
  single-bucket collector omits timestamps only with complete-file,
  half-open-window, bucket-origin/offset, and non-null Int64 timestamp
  proofs. Partial windows and cross-bucket files keep timestamp filtering.
- Flat positive same-field exact OR deduplicates term ordinals and uses one
  postings union unless authoritative index metadata certifies disjoint
  raw-value terms. `raw_value_terms_disjoint_v1=true` permits doc-count
  summation only for non-FTS, non-partial fields. Writer incidence tracking
  revokes proof on repeated/rewound document IDs and requires every raw
  incidence to lie within the committed output row range. This rejects
  failed-longer/shorter-null-retry residue. Merges preserve proof only for
  certified inputs with disjoint offset maps and no FTS reinterpretation.
  Legacy files and unproven mappings remain on the exact union path.
- Local proof: 315 VIX-library and 182 search-VIX tests passed (14/4
  ignored), and the optimized server built successfully. The frozen
  288,379-row/83-file corpus passed the 19 HTTP query contracts,
  11 COUNT/timestamp cases, and timestamp-dependent sparse cross-bucket
  and partial-window cases on both pre-change and newly written files.
  All 83 new index sidecars carried the certificate; all old sidecars
  and all new DATA footers lacked it.
- Two measured blocks per binary produced 192 exact responses on
  identical published file metadata. Warm sparse range reads fell from
  10 to 5; dense full-window bytes fell from 64,168 to 39,896. These are
  actual backing-store counters on local disk, not response scan-size
  metadata or production S3 measurements. Parsed-reader and OS caches
  remained warm; timings do not establish cold or production speedups.
- A real heartbeat/client-gone cancellation preserved concurrent and
  subsequent exact queries; search IO counters stayed unchanged after
  the post-close settling snapshot. The native allocation probe covered
  first-use order reversal, fresh setup, actual scans, early stop, and
  repeated scans. Warm setups/scans and whole-operation teardown had zero
  post-drop requested-byte deltas. First-use global initialization left
  residuals in both constructor orders; the first native read left 88 bytes
  in either order, so this does not prove zero first-use/global retention.
- The preceding binary (`1dadd69a`) also passed all 19 query contracts
  when reading the newly certified files.
- No production deployment or Orbit change.

## 2026-09-05 — exact VIX aggregates and bounded query IO (local verification)
- Aggregate shortcuts require complete input and the original logical
  column identity. Preserve NULL/absent groups and global winners; remove
  per-file/per-bucket truncation. FILTER, DISTINCT, narrowing casts,
  transformed timestamps, and residual predicates decline shortcuts rather
  than returning row counts for a different SQL expression. Bare
  passthrough projections and the requested date-bin origin remain valid.
- Count-only dictionary scans avoid postings payloads. Optional
  `dict_field_pages_v1` directories and field-aligned terms chunks bound
  unrelated-vocabulary work; old files still use the legacy reader path.
- Query-owned cancellation and separate process-wide fetch/evaluation
  byte gates replace coarse sidecar-size admission. Reader ownership is
  shared across native-only, schema, stats, and worker paths; immutable
  encoded footer caches do not retain decoded trees or creator operations.
  Cache publication is fallible before visibility, and size-growth
  accounting is O(1). Background warming has bounded admission and owned
  temporary-file cleanup.
- Local proof: 293 VIX-library, 178 search-VIX, 41 optimizer, 116 cache,
  5 event-admission, and 1 byte-budget configuration tests passed (14/4
  pre-existing ignored tests in the first two groups).
  A frozen 288,379-row/83-file corpus passed all 19 query contracts and
  11 COUNT/timestamp boundary cases on both legacy and newly written
  layouts. Old-binary reads of new field-page files preserved every
  baseline pass/fail classification; no new format failures.
- Actual HTTP disconnects closed all 14 cold-reader and 3 warm-reader
  remote ranges, with correlated server abort logs, no later range starts,
  and exact subsequent queries. The 2M-row metadata probe returned exact
  counts at unrelated cardinalities 16, 65,536, 1M, and 2M.
- No production deployment or Orbit change. Timings are local
  current-state/warm measurements, not disk-cold or production speedups;
  disabling the result cache does not disable file/VIX/OS caches.

## M31a — INGEST LATE LANE 2026-08-26 (ships as .124; the tiny-file source fix)
- DESIGN DEVIATION from the DESIGN-MERGE.md §2 charter, for cause: the
  charter's per-segment pending-(stream,hour) pool has NO representation in
  the provenance scheme — dedup_candidates suppresses by (stream, segment
  id-range), so an L0 covering an id whose hour-H rows were withheld makes
  those rows INVISIBLE (invariant survey 2026-08-26: the visibility unit is
  the whole segment per stream; Built is terminal; no row-level _o2_id
  dedup exists to absorb any at-least-once alternative). The safe unit is
  the WHOLE SEGMENT — so the split moves UPSTREAM of the builder:
- Mechanism: (1) the ingest buffer routes frames whose hour is ≥
  ZO_SEGMENT_LATE_LANE_HOURS behind now (frames are per-(stream,hour) by
  construction) into a LATE sub-buffer that ships as its OWN all-late
  segments on slower triggers (ZO_SEGMENT_LATE_FLUSH_SIZE_MB=8 /
  _MAX_SECS=30 — the documented durability trade: ≤30s RAM window for
  LATE rows only); (2) all-late segments (classifier created_at - max_ts,
  NO schema/format change) are EXCLUDED from the fresh claim lanes + the
  #44/#50 gates and claimed by a third lane (ClaimOrder::LateOldestFirst)
  after ZO_SEGMENT_LATE_CLAIM_HOLD_SECS=900 — one wave per hold window
  coalesces the whole fleet's late rows into ONE L0 per (stream, hour)
  instead of ~one 1-record ~3KB file per hour per build batch (~3.5k/h on
  traces alone, 85% into old hours; ~300KB compressed metadata floor each
  = ~1GB/h of pure S3 metadata churn).
- Invariants kept: whole-segment claim granularity, deterministic chunk
  keys, single wal_segments producer, Built stays terminal, stale-lease
  recovery covers late claims through the fresh lanes, rows stay
  query-visible through the segment tail during the hold, hold ≪ the 1d
  lifecycle. Backlog WARN keeps counting hold-EXPIRED late segments (lane
  stall signal) and ignores held ones. Lane off (0, the default) is
  byte-for-byte today's behavior — ships DARK, enabled per env via
  configmap (both builder roles must see the env together).
- Tests: sqlite claim/probe/stats semantics (fresh excludes all-late; late
  lane claims exactly hold-expired, oldest first; lane-off degradation),
  buffer routing/triggers/shared-cap/drain. Suites green.

## .123 +1h OUTCOME 2026-08-26 ~10:5xZ — WEDGE HEAL WAVE SELF-TERMINATED
- The lone-index-less sweep clause surfaced the FULL wedge population on
  arrival (not just .122's deferred outputs — weeks of pre-existing lone
  L0s, dominated by logs/default): fleet heals peaked ~12.5k/10m, decayed
  97% to 416/10m within ~1.5h, zero probe failures, every file healed
  EXACTLY once (verified: no repeated keys). Debt-sweep pass size 186-era
  -> 1-8 steady with transient 37s while the clause worked the window.
- Group merges dipped to ~2k/h during the wave (shared workers), then
  RECOVERED TO ~8.7k/h — 5.4x the pre-M30 baseline — with 81% of rebuild
  merges on the #46 column arm (836/1037 sampled) and deferred copy-shape
  outputs flowing (18/15min, avg 27MB, no throwaway sidecars).
- Remaining before file counts DRAIN: the arrival side — M31a above.

## M31b 2026-08-26 — INDEX ONCE PER BYTE (ships as .122; charter: DESIGN-MERGE.md)
- b(0) ROOT CAUSE of the dead #46 gate, measured on real prod L0s
  (m31_gate_probe example, 5 traces files + the registry schema): strict
  DataType equality vs the registry — prod L0s store strings as Utf8View,
  the registry says Utf8; 909/918 fields "mismatched" ⇒ ZERO column-derived
  rebuilds fleet-wide (0 engagement lines vs 550 rebuilds/h/pod), every
  rebuild on the 5.4x `_source` JSON arm, and NO log line said why. Fixed:
  string-family equivalence (Utf8/LargeUtf8/Utf8View — lossless
  representation, terms derive byte-identically) + a reason INFO on every
  gate miss + `terms_from_columns` in the merged summary line. Parity
  referee extended to the drift shape: byte-identical.
- b(1) `_source` is no longer decoded on the #46 heal-passthrough scan when
  every input is spliced — it fed a length/null check only (the fattest
  column of every input, decoded for nothing on the dominant prod shape;
  M17 measured the derivation scan at 113.6s of a 133.8s gen-1 merge).
  Plan-level scan_source=false + synthesized empty source keeps the push
  contract; fail-open inputs keep full projection.
- b(2) INDEX-DEFER (`ZO_VIX_MERGE_INDEX_DEFER_BELOW_MB`, default 0=off,
  prod pin 512 = max_file_size/2): an all-index-less group summing under
  the line writes a COLUMN-STORE-ONLY output — copy-shape merge, no
  dictionary/postings/bloom, no rebuild-gate slot. The index is built ONCE:
  at the group crossing the line, or by the standing single-file heal on a
  terminal leftover (prod evidence for the waste: 39MB merge outputs
  carrying 29MB sidecars). PLUS sidecar-HOMOGENEOUS grouping
  (FileMeta.index_size split at group formation): mixed groups — the shape
  that poisons the fast path AND #46 — stop existing; indexed groups take
  the gate-free dictionary merge.
- Tests: defer round-trip (L0s → deferred copy hop → final rebuild) is
  byte-equivalent to the direct heal; drift parity; full core suite green.
- NEXT (M31a, own pass): ingest per-(stream,hour) accumulate-or-age gate —
  the 1-record late-arrival spray fix at source (DESIGN-MERGE.md §2).

## .123 2026-08-26 — M31b FOLLOW-UP (the #465/#301 review P1)
- The .122 defer had a CONVERGENCE WEDGE the prod review caught: a closed
  low-traffic hour that collapses to ONE sub-512MB index-less file falls
  under the debt predicate's min-files floor, is never revisited, and the
  single-file heal — the only thing that indexes a lone file — never runs:
  permanent column-scan for that hour. (Same wedge PRE-EXISTED for 1-file
  hours under .121: a lone L0 in a quiet hour never got indexed.)
- Fix: query_old_data_hours grew include_lone_unindexed — the sweep also
  returns hours holding ANY index-less .vix data file, floor or not.
  Self-terminating (heal stamps index_size > 0, hour drops out); callers
  pass false for by-design index-less stream types (metrics — their probe
  no-ops, the clause would re-enqueue forever); parquet rows excluded in
  SQL. postgres + sqlite twins.
- Review's other P1s (all comment/contract): max_file_size <-> defer-pin
  coupling cross-referenced BOTH sides; #42 rollback runbooks now name the
  defer env; dev comments reworded dev-first (no dangling "prod parity");
  the .122 rollback note states (1)/(2)/grouping have NO env brake (image
  rollback only; deliberate — parity-pinned byte-identical).

## .121 +2h OUTCOME 2026-08-26 ~08:3xZ — M30 VERIFIED; EQUILIBRIUM NAMED
- Fleet merges/h 1,600 -> 6,065 (+1h) -> 6,742 (+2h) SUSTAINED (4.2x). Gate runs
  4/4 slots on every pod; queue waits 168s mean -> 0.2-33s. Kills=0 through the
  whole window; RSS 3.6-23.2GB worst vs the 43.4Gi envelope — the headroom check
  never had to clamp below cap in practice. Pending jobs healthy (314/238).
- Rollout itself: ~4min to 10/10 (Karpenter had capacity), dev auto-synced clean.
- THE FINDING: 4.2x moved the fleet from falling-behind to TREADING WATER.
  traces/default per-hour live counts stayed ~flat (9.0-9.7k) because arrivals
  match consumption: ~7.1k/h bulk L0 (128-512MB, the real data) + ~3.5k/h TINY
  <1MB files (85% landing in >1h-old hour partitions, ~0 bytes — the file-count
  poison), vs ~6k files/h consumed for this stream. Throughput alone cannot
  break this equilibrium: the next step is WORK REDUCTION per byte/file —
  chartered as M31, design in DESIGN-MERGE.md (2026-08-26 architecture review).
- M30 mechanism note for the record: cap 4 binds via CPU contention (4 rebuilds
  x 3 threads = the 12C limit; per-merge slot hold stretched ~19s -> ~30-45s),
  NOT via the memory envelope. No-CPU-raise is an owner/user constraint — M31's
  levers are all CPU-reducing.

## M30 — HEADROOM-GATED REBUILD ADMISSION 2026-08-26 (ships as .121)
- Prod re-measure (the gate contract's owed step, done live): gate=1 is 100%
  saturated (381/381 merges rebuild-path on the busiest pod/2h, ~19s slot
  hold, MEAN 168s queue wait per merge), fleet ceiling ~1.6k merges/h vs
  arrivals — 2026-08-25 closed with 274,977 live traces/default files
  (10-15k/hour partitions, L0 never reaching target); hours converge ~3 days
  out. Meanwhile SAMPLED RSS breathes 1.35-18.15GB (kubectl top's 22-40GB is
  disk-cache page cache) — 30-45GB of real headroom the count pin can't see.
- Mechanism: count stays the hard CAP; every slot beyond the guaranteed
  first also needs live headroom — NODE_MEMORY_USAGE + 
  ZO_VIX_REBUILD_HEADROOM_MB (default 5120 = the contract's ~5GB/rebuild
  arithmetic) charged per extra IN FLIGHT (candidate included, ingest-
  admission burst lesson) under 90% of mem limit; waiters re-poll 500ms.
  Headroom=0 restores exact M12 count-only. First slot unconditional.
- Config delta shipped alongside: compactor ZO_VIX_REBUILD_CONCURRENCY 1->4
  (the auto value; now a cap). Expected ~3-4x merge throughput/pod
  (~750/h/pod), recent-hour file counts draining in hours. Brakes are
  config-only: concurrency=1 (exact today-regime) or headroom=0.
- Full detail + watch list: M30-NOTE.md.

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
