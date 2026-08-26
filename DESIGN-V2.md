# DESIGN-V2 — post-reset engine architecture

Owner-approved 2026-08-17. Replaces the lost DESIGN.md. This is the build
plan for the v2 format + engine; ENGINE-BACKLOG.md stays the ship log.
First goal: QUERY PERFORMANCE. Everything below is judged by that first.

## 0. Era & ground rules

- Fleet reset 2026-08-17: querier+compactor replicas 0 in BOTH envs; only
  the ingest path runs (router, ingester, nats, collectors). New meta DB
  `obs20260817`, new S3 prefix `obs-20260817/` (ZO_S3_BUCKET_PREFIX is
  applied at the storage layer — every object key gets the prefix
  prepended in `src/infra/src/storage/remote.rs:42,65-66`). Buckets
  (`bfe-quickwit-prod` us-east-1, `bfe-quickwit` ap-southeast-1) are
  SHARED with live `o2/` data — nothing outside the obs-*/ prefixes is
  ever touched.
- S3 lifecycle expiry (1 DAY, owner call) IS the retention mechanism.
  There is no engine-side retention job to design for. Design for
  correctness at any retention; optimize defaults for short.
- Orbit switched back to o2 → obs has ZERO query consumers until v2
  ships. There is no compat audience.
- v2 is a BREAKING format. No legacy read path — the wipe makes that
  free. At v2 launch another prefix+DB cut happens (obs-20260803 →
  obs-20260817 was the rehearsal; the runbook is proven), so interim
  current-format data never mixes with v2 data.
- Interim: ingesters keep writing v1 `.vix` into `obs-20260817/` purely
  to keep producer pipelines healthy; those files age out in 1 day and
  are never read.

## 1. Object model

Three object classes per stream partition, replacing the single embedded
`.vix`:

1. DATA object (`.vix2`): (near-)pure Vortex docs file + a small O2
   footer (§4). No index bytes. Any vortex-capable reader can read the
   docs region.
2. INDEX sidecar (one small puffin per data file): `dict_blocks`,
   `terms`, optional `plist`, per-file `bloom`. Referenced from
   file_list by an `index_ver`-style pointer column following the proven
   `bloom_ver` pattern (sentinels for not-applicable/unbuildable, GC by
   reference scan, fail-open on absence) — see
   src/core/src/compact/bloom.rs:32,60 for the pattern being copied.
3. GROUP bloom `.bf`: unchanged. The per-file bloom blob keeps the exact
   SBBF block layout/hash so `.bf` assembly stays a byte transpose
   (src/vortex_index/src/bloom.rs:27) and dictionary walkers keep the
   `observe_dict_key` byte-form rule (src/vortex_index/src/bloom.rs:46).

Why the split (v1 embeds the index as tail blobs in one puffin): index is
DERIVED data with a hotter lifecycle than the data it serves — tokenizer
changes, field-marking changes, #52 demotions, bloom versioning, and one
dictionary-format cutover already forced fleet-wide heals. In v1 every
index rewrite rewrites data bytes and mints a new object key, so querier
disk caches go cold for docs bytes that did not change. In v2:

- heal = write a new index object + bump the pointer. Data object key
  stable; querier caches survive index rebuilds by construction.
- index format iterates without data rewrites, ever again.
- merges rewrite both objects anyway (doc ids change) — nothing lost.
- cost: one extra small GET on cold index eval (reader cache + warmup
  absorb it; scans never touch the sidecar), 2x object count, pointer
  discipline. Accepted.

## 2. Docs schema — ALL present fields as columns, plus `_source`

Per-file docs schema: `_timestamp` + EVERY field present in the file
(typed columns; per-file union of PRESENT fields, never the registry) +
`_source` (always) + `_original` (opt-in). `column_store_fields` as a
user-facing concept is DELETED — there is no curation, no promotion
policy, no cold-field cliff.

This generalizes what the fleet already runs: #42 L0 index-off files
write every plan field as a docs column today
(src/core/src/vix/core_writer.rs:536-541), hundreds wide under
narrow-schema WAL (src/config/src/config.rs:1443), at 512MB super-batch
scale, in production. v2 stops narrowing that shape back down at
merge/heal time and extends it to all levels.

### 2.1 Read routing

- `SELECT *` stays a `_source` read: the row-store star projection
  (src/search/src/sql/schema.rs:114) and response synthesis
  (src/search/src/datafusion/source_synthesis.rs:76,138) carry over
  unchanged. Star never enumerates any schema.
- Filters/projections read native columns — vectorized decode, ranged
  fetch of only the touched columns' segments, per-column pruning (§4).
- `json_get_*(_source, ...)` (src/search/src/datafusion/vix_format.rs:26)
  survives ONLY as the fallback for a field absent from a file's schema
  (returns null-correct semantics). The evidence this routing matters:
  per-row extraction from fat `_source` bounded a 24h trace query at
  >280s (#33 C-3, ENGINE-BACKLOG.md:1641-1642); the one manual column
  promotion on record took the same shape 12.1s → 1.2s (#21,
  ENGINE-BACKLOG.md:681,2112). v2 makes that win universal and deletes
  the policy question.

### 2.2 Storage cost (accepted, measured before ship)

Every field's bytes live twice: in its column and in `_source`. Anchor:
the #52 A/B showed 2 high-cardinality ID columns = +26.5 MiB docs on a
~200 MiB file (ENGINE-BACKLOG.md:2368). Low-cardinality fields
dict/RLE-encode to ~nothing; expect docs +20-40%, total +15-30%,
dominated by the few high-cardinality strings. With 1-day retention the
absolute cost is small; the bench gate (§11) records the real number.
`_source` stays zstd via the btrblocks sampler — FSST measured ~1.6x vs
~15x for zstd on this blob and stays rejected
(src/vortex_index/src/container.rs:1011).

## 3. Holding conditions

The all-columns design is sound iff four implementation conditions hold.
Each has already failed once in this codebase; each is a hard gate.

### H1 — chunk sizing from ENCODED data bytes, never arrow width

v1 sizes rows-per-chunk from whole-row arrow bytes: 2557 nullable Utf8
cols ≈ 10.5KB/row of arrow padding even all-null → chunk rows collapse →
zone count × every column's zone-stats table, super-linear
(ENGINE-BACKLOG.md:2103, writer.rs:1872-1885 as of v1). v2 rule: the
chunk-row target derives from sampled ENCODED bytes of populated cells;
sparse columns contribute ~their presence bitmap, not a padded slot.
Gate: chunk row counts on a 1,500-field corpus must be within 2x of the
same data at 20 fields.

Chunk byte budget (`ZO_VIX_DOCS_CHUNK_BYTES`): default 16MiB (owner call
2026-08-18, M8 S2: merge −25% wall / −17% HWM, storage-neutral, ~2x
`_source` point-read decode; 4MiB remains a knob).

### H2 — pay-as-you-go per-column metadata

A field present in 1% of rows costs a presence bitmap + values, NOT a
full zone-stats table + dictionary probe. Zone stats are skipped or
coarsened below a density threshold (per-column, recorded in the footer
so readers know "no stats" from "stats say no"). Per-column fixed floors
are what made registry-wide files unviable (ENGINE-BACKLOG.md:2099-2108).
Gate: footer size grows sub-linearly with column count on the sparse
corpus.

### H3 — streamed, byte-budgeted build and merge memory

The 2026-08-17 prod incident: compactors OOM-cycled at 24Gi (kill every
~6 min, up to 81 restarts/pod) because merge inputs are downloaded WHOLE
into RAM (`cache_remote_files`, src/core/src/compact/merge.rs:997,1829 →
`res.bytes()`, src/infra/src/cache/file_data/mod.rs:267) at cpu_num
concurrency per job × 12 worker jobs × per-merge thread pools. v2 rules:
(a) remote reads stream to disk cache, never whole-object RAM transit;
(b) ONE global in-flight byte budget across all workers admits merge
work; (c) outputs always spool (v1 merge scratch already does:
src/core/src/vix/core_writer.rs:1592) — nothing multi-GB resides in RAM;
(d) wide-batch encode is column-at-a-time or bounded-batch (the L0
external sort OOM'd the DataFusion pool on FAT streams — #26,
ENGINE-BACKLOG.md:997). Never re-enable compactors on the v1 merge path.
Gate: peak RSS bounded and flat vs input width and input count.

### H4 — plan cost O(projection), never O(file width)

#45 measured multi-second PLAN time on index-off (wide) windows
(ENGINE-BACKLOG.md:2224). Planning must touch the statement's columns
only; per-file wide schemas are never enumerated at plan time —
schema mapping resolves lazily per file at open. Gate: plan time flat
between 20-field and 1,500-field streams.

## 4. O2 footer — pruning metadata that SURVIVES passthrough

v1 passthrough trades away Vortex statistics: `.with_stats(&[])` skips
the stats pass because computing them canonicalizes (= decodes) the
chunks being copied (src/vortex_index/src/container.rs:1068,1076,1092),
so passthrough outputs lose numeric file-skip and in-file chunk pruning;
only the `zone_map` property keeps `_timestamp` fast paths
(container.rs:1079,205). That is backwards: pruning metadata must be
O2-OWNED and SPLICEABLE, not a byproduct of encoding.

v2 footer (on the data object), all splice-through-passthrough by
construction (pure metadata merges, no decode):

- zone map: per-chunk `[row_count, ts_min, ts_max]` (carried from v1).
- per-column chunk stats: min/max (+null_count) per chunk, H2 density
  rules applied. Spliced from input footers on chunk copy; computed once
  at first encode.
- M16: chunk stats are ANSWERS, not just pruning — `count(field)` folds
  per-chunk presence counts (file-level presence when fully covered),
  `min/max(field)` folds per-chunk exact numeric min/max
  (`min/max(_timestamp)` folds the zone table), and a count-shaped
  aggregate whose single numeric-equality conjunct the index cannot
  serve is decided per chunk (`present==0`/outside-bounds = none,
  `present==rows && min==max==literal` = all) with only inconclusive
  chunks decoding. Boundary chunks always decode; string bounds NEVER
  answer (prefix-conservative, prune-only); columns without stats
  (density-gated, M1 files) fall through to decode — pinned differential
  vs full decode. New local-only optimizer modes SimpleCountField /
  SimpleMinMax (never on the wire; detector gates min/max to numeric
  columns).
- field-presence list: the file's column set + per-column density class.
  Enables whole-file skips for predicates on absent fields before any
  data GET.
- `row_order`: `ts_desc` | `concat` + REGION table (input provenance of
  copied chunk runs) so piecewise order is machine-readable (§6.2).

## 5. Index sidecar

Contents (one puffin, byte formats carried from v1): block dictionary
(prefix-compressed 4KiB blocks), terms table (doc_count + postings,
delta+bitpacked), optional out-of-row `plist`, per-file SBBF bloom.
Field-major `{fid u16 BE}{token}` composite keys stay.

- Role NARROWS: full-text (match_all), substring/LIKE, and
  ultra-selective needle equality. Plain equality/range on any field is
  served first by §4 stats + `.bf`/composite bloom + native column scan;
  postings win only when selectivity is extreme — keep the #52
  bloom-only demotion concept (high-cardinality fields: bloom coverage,
  no postings) as the default posture for ID-shaped fields.
- #52 demotion is DEFAULT-ON in v2 (M7): `ZO_VIX_BLOOM_COMPOSITE=true`
  and `ZO_VIX_BLOOM_ONLY_AUTO_RATIO=0.5` (floor 65536 distinct), and the
  AUTO rule runs at BOTH write sites — merge plans count input
  dictionaries, first-encode builds count the writer's own term map — so
  ID-shaped fields never enter the dictionary at all (one shared
  resolver, `vortex_index::resolve_auto_bloom_only`). The `bloom` field
  marker is STICKY across merges (demoted inputs have no terms left for
  the count rule to see); un-demotion = `ZO_VIX_BLOOM_ONLY_NEVER` + the
  heal that re-derives terms. Equality on a demoted field = composite
  bloom file-prune → native-column filter-back scan under §4 pruning.
  M15 completed the trade's edges: a CONFIGURED (`bloom_filter_fields`)
  demoted field falls back to composite-section probes per group/file
  (guard-gated, per-field verdicts keep priority — keep/drop parity
  with per-field sections pinned), and the filter-back scan is a
  dictionary-aware equality pre-pass (needle resolved against each
  chunk's dict once, code-array scan; canonical compare for non-dict
  chunks) that point-reads only the matching rows and parallelizes
  across chunks under ZO_VIX_SCAN_DECODE_THREADS (measured, 16M rows:
  624ms → 206ms/4T → 167ms/16T; the knob previously had no effect on
  this shape).
- Pointer semantics: `index_ver` column on file_list rows (bloom_ver
  pattern: sentinels, referenced-scan GC, fail-open to filter-back scan
  when absent/unreadable). A data file with no sidecar is always
  queryable — index is an accelerator, never a correctness dependency.
- Heals: rebuild terms (column-derived — the 5.4x-cheaper path is now
  universal since every field is a column), write ONE new small object,
  bump pointer. Data object untouched.
- L0: index-off stays the L0 default (#42 posture) — the sidecar appears
  at first compaction; fresh-data queries scan columns with §4 pruning.

## 6. Merge & compaction

### 6.1 Passthrough-native, not a dark flag

Chunk copy + footer splice (§4) is the DEFAULT merge path; decode/re-encode
is the exception (type widening, H1 resize, cleansing). v1's four
qualification shapes (disjoint / concat / schema-pin / heal) collapse:
schema-pin dies with fixed narrow schemas (per-file schemas are already
the union of present fields; additive widening = null-fill at read,
derive-at-merge only when H1 forces a re-encode anyway), heal passthrough
dies because heals no longer touch the data object at all (§5).

M17 gen-1 encode-once: the passthrough qualification WIDENS instead of
requiring schema identity — per-file-union inputs copy their chunks with
missing output columns synthesized as all-null constants (a genuine type
flip fails open PER INPUT to decode+re-encode, counted in the merge
summary), so multi-input rebuild merges over index-off L0s stop paying
the docs re-encode (decode stays for term derivation); the rebuild's
index-blob build is range-parallel at field boundaries
(`ZO_VIX_MERGE_KWAY_THREADS`, byte-identical for any R), and the #52
coverage scan hashes off ENCODED chunks (dict: dictionary-only decode;
FSST: bulk-decompress + raw slices; census per merge).

M6 layout rule (physical only — the layout TREE and readers are
unchanged): passthrough docs blobs are written in COLUMN-MAJOR STRIPES
(~160 MiB of output per stripe, each column's leaves contiguous within
it; src/vortex_index/src/clustered.rs), because per-push interleaved
leaves at sub-1MiB stride let the read coalescer bridge every gap — the
M5 A/B measured a 2-column projection fetching the entire 2.4 GB docs
blob, and needle selections paying one GET per chunk. Within a stripe,
consecutive decoded-family column chunks (slice-guard canonicalizations,
re-encoded runs, tiny ≤16 KiB encoded slices) coalesce to ≤128Ki-row
chunks before recompression; `_source`-scale encoded chunks still copy
byte-identical. Projected ranged reads touch one byte run per column per
stripe: fetch count = O(projected columns x blob/160MiB), bytes = the
projected columns'.

### 6.2 Piecewise order instead of a global sort

Concat-order merges stay (copy regions in per-input `ts_desc` runs) but
readers EXPLOIT the region table: `ORDER BY _timestamp DESC` runs a
k-way merge over the piecewise-sorted regions, not a full sort. v1
regressed here — with the concat knob on, queriers stop declaring the
per-file sort entirely and pay real sorts
(src/search/src/datafusion/exec.rs:539-550). v2 declares per-region
order from day one.

### 6.3 #51b parallel k-way term merge — LANDED (M10)

The v1 merge ran single-range: `partition_bounds` was a deliberate stub
after prod dictionary corruption. The lesson, verbatim as a v2 design
constraint:

> It sampled inputs' row-group `term_min`s parsed under the v1 byte form
> and used the raw byte strings as range bounds. [...] Field-major keys
> invert that: `{fid u16 BE}{token}` sorts by INPUT field id FIRST, and
> every input has its own fid table — one raw-byte bound cuts different
> inputs at DIFFERENT FIELDS, so after the per-input remap to output ids
> the per-range sinks no longer cover disjoint ascending output ranges
> and the concatenated dictionary carries OVERLAPPING row groups (prod
> corruption 2026-07-29). A valid v2 sampler must express bounds in the
> OUTPUT key space and translate them per input.

M10 ships exactly that sampler (src/vortex_index/src/merge.rs
`partition_bounds`/`translate_bound`). Landed invariants:

- **Split points are real keys in the output key space.** Candidates =
  the inputs' dict-block index first keys (resident index walk, no block
  decode), remapped to output fids; split points are weighted quantiles
  over per-block KEY COUNTS; sorted, deduplicated; nothing is ever
  fabricated by byte arithmetic.
- **Per-input translation, gated on a proven-monotone remap.** Bounds
  are emitted only when every input's `input fid -> output fid` map is
  strictly increasing; each worker then translates each bound into each
  input's own key space — provably exact on every emittable key (and a
  consistent tiling for dropped-field keys, so `partial` reporting stays
  complete). A non-monotone map yields no bounds: single range, where
  the in-stream strictness guard governs as before.
- **Assembly order is enforced, not assumed.** Range outputs (TermSink
  parts) assemble strictly in range order; `write_index_blobs` hard-
  rejects any part whose first key is not above the previous part's last
  key (the structural backstop that predates the sampler). Blooms
  accumulate as per-worker hash sets and merge before the single final
  SBBF build — byte-identical to the sequential build.
- **Knob:** `ZO_VIX_MERGE_KWAY_THREADS` — `0` (default) =
  `min(available_parallelism, 8)`; `1` = exactly one range, the
  sequential path through the same code; always capped by the
  `ZO_VIX_MERGE_THREAD_NUM` budget (stacks with it, never widens it).
  Workers pull 4x-over-partitioned ranges off a shared cursor (skew).
- Differential digest pins (tests::m10_parallel_kway): sequential vs
  parallel on disjoint / overlapping / demoted-mixed corpora plus
  adversarial splits (a bound exactly on a fid's first key, one fid
  >90% of keys, more ranges than distinct keys, single-input), and a
  direct-build oracle.

## 7. S3 IO rules

- Reads: ranged by default (footer/tail first, then touched segments);
  remote downloads stream to disk cache, never `res.bytes()` whole-object
  RAM transit (H3). A projected ranged read fetches ONLY the projected
  columns' bytes — guaranteed by the §6.1 stripe layout on passthrough
  outputs and pinned by fetch-accounting tests
  (vortex_index tests::ranged::passthrough_{projection,needle}_fetch_budget).
- M14 cold-open prefetch (`ZO_VIX_QUERY_PREFETCH`, default on): before a
  file group evaluates, COLD files (no memoized reader) batch-fetch
  their eager tails (data footer + sidecar footer/dict directory) in one
  bounded-concurrency wave — one parallel fetch round instead of
  per-file sequential open rounds. Wave fetches take the global
  ZO_VIX_FETCH_CONCURRENCY permits, count toward the eval-bail byte
  budget (added flat in the projection), and skip result-cache-answered
  files; postings are never prefetched (need dict resolution).
- One global in-flight byte budget per process across ALL pools (merge
  workers, downloaders, scans). Concurrency knobs cap parallelism;
  the byte budget caps memory. Both exist independently.
- M17 build admission: L0 building is byte-budgeted process-wide
  (`ZO_SEGMENT_BUILD_MEMORY_BUDGET_MB`, 0 = 40% of detected memory) —
  a claim reserves estimated decoded bytes (meta size × inflation EMA,
  corrected to post-decode actuals) and each stream-chunk build reserves
  its actual decoded input; always-one floors per class prevent
  deadlock; `ZO_SEGMENT_BUILD_CONCURRENCY` (default 16) is only the
  secondary count cap — the byte budget replaces the count-knob
  treadmill.
- Writes: spool-always for anything not trivially small; multipart
  streamed from the spool (16MiB parts, bounded in-flight — the v1
  `put_file` shape).
- Future (noted, not v2.0): `UploadPartCopy` for pure concatenations —
  copied docs regions never transit the compactor at all. Parts ≥5MiB
  constraint means tiny inputs need a hybrid. Nothing in v1 uses
  server-side copy anywhere.
- Querier `ZO_S3_REQUEST_TIMEOUT` drops from 3600s to O(minutes); the
  3600s default is a hang amplifier.
- Launch default (M11, owner 2026-08-18): queriers CACHE LATEST FILES —
  `ZO_CACHE_LATEST_FILES_ENABLED=true` broadcasts new rows to their
  consistent-hash querier, which downloads the `.vix` data object AND its
  `.vxi` sidecar and evicts merge inputs a broadcast replaced
  (`..._DELETE_MERGE_FILES=true`); peer-to-peer fill
  (`..._DOWNLOAD_FROM_NODE`) stays OFF — fills come straight from the store.
- Aging lane (M13): the 1-day lifecycle expiry also applies to RAW
  segment objects (`wal_segments/`), so builder claiming must never let
  a backlog cohort age toward it. Claims scan newest-first in steady
  state (fresh windows recover first); once the oldest pending segment
  exceeds `ZO_SEGMENT_BUILD_AGE_LANE_SECS` (default 6h), a reserved
  fraction of claim passes (`ZO_SEGMENT_BUILD_AGE_LANE_RATIO`, default
  every 4th) scans OLDEST-first — the compactor live-lane pattern
  (`ZO_COMPACT_LIVE_JOB_NUM`) applied to builds — so the oldest cohort
  drains under ANY standing backlog instead of starving into the
  lifecycle (prod 2026-08-18/19: oldest pending stuck 15+ hours behind
  a 74.5k newest-first backlog). Same all-or-nothing + SKIP LOCKED
  claim semantics in both lanes; disengaged steady state is unchanged.
  Its throughput twin is §8's M13 (1b) backlog-mode super-batch
  sealing: the lane fixes WHICH cohort claims take, 1b fixes HOW FAST
  claims chain while work exists.

## 8. Ingest / L0

- Narrow-schema WAL batches stay (src/config/src/config.rs:1443): batch
  schema = present fields — this is what keeps per-file unions at
  "hundreds, not registry-wide".
- L0 super-batch builds (#54) carry over: file count follows data
  volume, not claim count. M13 (1b) backlog-mode sealing: while claims
  return rows the accumulation is bounded by WORK — claim to the MB
  target and seal immediately; the `ZO_L0_SUPERBATCH_MAX_SECS` age
  clock and the two-empty-ticks arrival-gap seal pace only the true
  trickle (empty claim + the #50 probe confirming nothing claimable;
  an empty claim with claimable work present is a SKIP-LOCKED race
  loss, retried immediately, bounded). Wait-paced accumulation held
  prod at 5-8 super-batch cycles/15min/pod under a 72k backlog
  (2026-08-18/19; interim ops pin prod-ops #432 clock=15s superseded).
- The v1 L0 index-off docs shape IS the v2 shape — L0 and merged files
  are structurally identical (columns + `_source` + footer); merged
  files add the sidecar. One writer, one reader, no L0 special-casing.
- `ZO_COLS_PER_RECORD_LIMIT` (65536 backstop,
  src/config/src/config.rs:2128) stays as the pathological-record guard.

## 9. Deleted in v2

- The v1 `.vix` read path entirely (embedded-index puffin layout,
  dict-in-container open, `index=none` special cases). Breaking format,
  no compat shims, enforced by the launch prefix+DB cut.
- `column_store_fields` as user-facing config + its settings plumbing.
- quick_mode remnants (`generate_quick_mode_fields`,
  src/search/src/sql/schema.rs:165) — the row-store star made it dead
  code on the common path; v2 keeps registry-star only for
  CTE/join/subquery and bounds it by the statement's referenced columns.
- Schema-pin heal machinery and the four-shape passthrough qualification
  lattice (§6.1) — v1's legacy-convergence complexity.
- UDS/_all legacy-key tolerance (`defined_schema_fields` etc.,
  src/config/src/meta/stream.rs:1209) — nothing left to tolerate.

## 10. file_list / meta deltas

- New column: `index_ver` (sidecar pointer, §5). `index_size` keeps
  meaning "index bytes exist" for pruning-aware scheduling; `bloom_ver`
  unchanged.
- Registry diet (schema-version hash caching, field TTL/compaction) is
  REAL but SEPARATE — the meta registry still learns every field ever
  seen and costs O(width) per ingest request; file formats don't fix
  that. Own design note later; not a v2.0 gate.

## 11. Bench gates (all A/B vs v1 on benchtmp/vixbench corpora)

Matrix, median-of-3, log-line verified per the standing gate rules
(integration both segment modes; no gate passes on exit codes alone):

- file size: total, docs, footer, sidecar — on narrow (20-field),
  prod-shaped (hundreds), and fat (1,500+) corpora.
- build: CPU + peak RSS, L0 shape (H1/H3 gates live here).
- merge: CPU + peak RSS + wall, passthrough AND forced-rebuild arms
  (H3); plan time flat vs width (H4).
- query suite: needle equality (indexed + bloom-only), histogram,
  top-N, `SELECT *` tail, and WHERE-on-a-non-hot-field — the case this
  design exists for; target: no cliff vs the same field queried hot.
- pruning: file-skip and chunk-skip rates on passthrough outputs must
  EQUAL first-encode outputs (§4 splice gate — the v1 stats-loss
  regression is the anti-goal).

## 12. Open questions

1. Sparse-column encoding granularity: file-level union schema with
   null runs vs chunk-level presence (a column absent from a chunk costs
   zero bytes there). Chunk-level is strictly better for storage but
   complicates the vortex struct dtype contract — decide after an H1
   prototype on the fat corpus.
2. Binary/structural `_source` (key-interned tape/JSONB-like) to cut
   star-expansion and residual json_get parse cost: LATER experiment,
   and it is a CPU experiment ONLY — measured 2026-08-18 on 2,000 real
   prod records (k8s_prod_ops_logs + e2b_prod_logs, key-interned binary
   vs compact text, 4MiB blocks): raw −38.6%, but post-zstd −3.7% at
   level 3 and +5.0% at level 9 — zstd already removes what binary
   encoding removes, and typed binary values compress worse than
   repetitive decimal text at higher levels. Storage is NOT a reason to
   do this. Same measurement: text ratio 18.3x at zstd-3 vs 22.6x at
   zstd-9 — OWNER CALL 2026-08-18: stay at level 3; the ~−19% storage
   is not worth first-encode CPU at 1-day retention. Do not re-raise
   without a new owner call. Note: FSST was rejected for `_source` compression
   (~1.6x vs ~15x zstd, container.rs:1011) — that verdict is about
   compression, not structure; a structural encoding still pays zstd on
   top.
3. Orbit query-user re-provisioning at v2 launch: orbit is on o2 now;
   when it (or anything) points back at obs queriers, any DB-resident
   non-root user must be recreated in the then-current meta DB (creds
   live in git/Nacos, users live in the DB that gets cut at launch).
   NOTE the ingest side does NOT have this trap — verified 2026-08-17:
   the obs-bound collector exporters auth as admin@admin.com with the
   env-provisioned root password, which regenerates on every fresh DB;
   the wangzhichen@manus.ai header in the same values.yaml belongs to
   the o2-bound exporters.
4. RESOLVED by audit 2026-08-18 (at d10a047e6c): the five vix-arch
   commits are squash re-publications of work developed on this branch —
   their content is in this tree's granular history (roaring selections
   12973498ac; #27 coalesced walks/waves/pre-prune/batched plist
   8ed1e684ef; #29 key-free top-k 1562ba1875; #21 filtered group-by
   f2d5b89bfe; disjoint counts + SIMD contains 20862c321f) and every
   query-path feature is present and wired on the v2 read path. Nothing
   needs folding in or re-deriving. Two findings superseded the
   question: (a) CORRECTNESS — the per-file result cache keyed on
   condition+file with no index-version component, so M3's sidecar-only
   heals served stale answers; fixed in M12 by folding index_size into
   the key plus a broadcast purge. (b) PERF — the unfiltered
   top-k/distinct dispatch preferred the docs column on a
   whole-FST-walk rationale that field-major spans obsoleted;
   re-decided cost-based in M13 (see the bench table in
   ENGINE-BACKLOG).
5. `.bf` group scope under 1-day retention: hour-groups may be the wrong
   grain when the whole corpus is 24h; revisit group size once real
   query shapes exist.
