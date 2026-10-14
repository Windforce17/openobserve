# Cold-miss cost — AUTO additive blooms, `.bf` v2, compressed sidecar tail (proposed 2026-10-13)

Owner decisions so far (2026-10-13): write-time bloom coverage is **AUTO
additive** (per-field blooms chosen by in-file distinct count, dictionary
KEPT — not bloom-only demotion); scope is **all three layers together**
(coverage policy + `.bf` presence bits / hour regroup + sidecar tail format);
**write-side performance is a release gate** — compactor merge and L0 build
cost must be measured before anything ships, and a prod compactor canary runs
before the fleet.

Status: design for review. Nothing implemented.

## 1. Measured baseline (prod, 2026-10-13)

Cold miss on one index file today (SimpleSelect / needle equality, value absent):

| read | bytes | why |
|---|---|---|
| `.vix` data tail | 128 KiB | only `oversize_skips` (SimpleSelect miss trust), lives on the data object |
| `.vxi` sidecar tail | 768 KiB | Puffin JSON footer 396 KB (`fields` 125 KB + `dict_field_pages_v1` 217 KB, JSON-in-JSON escaped) + `dict` block index 271 KB |
| one dictionary block | ~4.4 KB avg (11.9 MB / 2,710 blocks) | term presence check |

= 3 GETs, ~960 KiB, 2 serial rounds (`default` logs medians over 24 files
from 2026/10/12 12Z; traces footer 179 KB + dict 266 KB; apisix 98 KB + 264 KB).

The `.bf` prune layer in front of it, `call_uid = '…'` over 7 days (10
followers, 12:46Z): input 9,034–10,239 files, **`.bf` groups 8,563–9,674 (one
group per file)**, dropped 2,749–3,133 (≈30 %), `bucket_failed` 6,233 / 9,143
(no coverage for the field in that group: field absent in the file, or `.bf`
built without it), per-file probes 491 → 0 dropped (`no_info` 455). Timeline of
one follower: probes done 12:46:36.9 → `load vix index files` 12:46:46.1 →
`total hits` 12:46:50.0: **≈9 s of bloom for 3.9 s of index work**
[INFERENCE: the `.bf` pass has no own `took`]. Cause: the assembler builds
1–2 files per `.bf` (every merge output re-pends alone; compactor log sample
100 % `dictionary fallbacks`, 0.07–4 s each), so the transposed "one row per
group" economy degraded to one footer probe (16 KiB) + one row per file. This
is also the likely shape behind the 2026-09-04 "22.7 s over 31,963 files"
result that turned broad composite off [INFERENCE].

Admission today (`bloom_pruner::composite_fallback_allowed`): explicit
bloom-only list ∪ ID-shaped names (`is_id_like_field_name`) — `user_id`,
`session_id`, `call_uid` reach the pruner, `domain`/`to` never do; coverage
exists only where the assembler's dictionary fallback hashed those fields.

Prod `default` logs median file (`751539261802138009641c3`, 2026/10/12 12Z):
424,089 rows, data 111.5 MB, sidecar 40.2 MB, 2,508 term fields (1,566
string), 636,005 terms, existing `bloom` blob 1,024 KB (composite of the 3
bloom-only IDs). Per-field distinct (exact, dictionary walk):

| threshold T | string fields with distinct ≥ T | Σ distinct | SBBF bytes @ FPP 0.001 | of data | of sidecar |
|---|---|---|---|---|---|
| 256 | 102 | 482,460 | 1.17 MB | 1.05 % | 2.9 % |
| 1,024 | 53 | 455,702 | 1.10 MB | 0.99 % | 2.7 % |
| 4,096 | 22 | 392,552 | 0.93 MB | 0.84 % | 2.3 % |

Top of the list: `response.time` / `request.time` 110k distinct each
(timestamps as strings, 26 % of rows — useless for equality, 0.5 MB of the
1.1 MB), then `eventid` 17.8k, `ts` 16.9k, `request_id` 16.2k, `user.id`
13.4k, `userid` 10.8k, `session_id` 9.1k, `email` 4.6k, `user_id` 3.5k,
`label.domain` 3.0k, `sandbox.id` 1.2k, `project_id` 1.1k.

Compression (zstd -3) of the tail parts: `default` footer 396 KB → **29 KB**
(-9: 24 KB); `dict` index 271 KB → 99 KB (traces 266 → 75, apisix 264 → 131).

Hardware ceiling the design must respect: queriers are `m8g.4xlarge`,
**baseline 7.5 Gbps NIC / 625 MB/s EBS** (burst 15 Gbps / 1,250 MB/s); cold
runs already sit at 827–931 MB/s NIC and 718–803 MB/s EBS writes. Fewer bytes
per file is what makes more concurrency pay.

## 2. Sidecar tail (format)

No new binary directory blob — compression alone gets the tail where it needs
to be, and the per-field page directory already exists.

- **S1. Compressed Puffin footer.** Footer flag bit 0 = payload zstd (level
  3). The reader currently rejects compressed footers (`puffin/reader.rs:47-50`);
  it accepts both. We own both ends; the deviation from Puffin's LZ4 default is
  documented in the crate doc. 396 KB → 29 KB on `default`.
- **S2. Compressed `dict` blob.** `BlobMetadata.compression_codec = zstd`
  for the block index (271 → 99 KB). Decompressed once at open; the reader
  cache keeps the decompressed index (lookups stay allocation-free).
- **S3. Miss path never opens the data object.** Writer copies
  `oversize_skips` onto the sidecar (data keeps its copy — heal's
  `NeedsDocsRewrite` check reads it there). Reader parses the data footer
  lazily (first docs/stats/schema access). Old sidecars without the property
  fall back to today's data-tail read.
- **S4. `bloom_sections_v1` footer property**: `[name, offset, num_blocks,
  n_items]` per per-field section so `FileBloomProbe` (unstamped files, 1b)
  goes footer → 32-byte block without the 4 KiB header window.
- **S5. Field flag `"bf"`** (`FieldTypeFlags::BLOOMED = 1<<4`): a term-capable
  field that has a per-field bloom section. Old readers ignore unknown wire
  types (`container.rs:572`).
- **S6. Eager tail** `ZO_VIX_EAGER_TAIL_BYTES` 768 KiB → 256 KiB (29 + 99 KB +
  margin; apisix 131 + ~20 KB) once ≥ 90 % of the last 7 days carry the new
  layout. Old-layout files under a 256 KiB tail cost one extra `NeedMoreData`
  read, not a failure.

Per cold miss after S1–S6: **2 GETs, ~260 KiB** (256 KiB tail + one ~4 KB
block) vs 3 / 960 KiB; reader-cache hit: 1 GET ≤ 64 KiB; and most absent-field
files no longer reach this path at all (§4).

## 3. Writer — AUTO additive per-field blooms

- **W1. Selection**, at each write site once per-field distinct is known:
  candidates = string-family term fields minus fts / partial /
  value-index-excluded / merge-demoted / bloom-only / `ZO_VIX_BLOOM_ONLY_NEVER`
  (the same filter as AUTO demotion, `writer.rs:2871-2890`). Selected iff
  `distinct ≥ ZO_VIX_BLOOM_AUTO_MIN_DISTINCT` (default 1,024; 0 = off) **and**
  (`distinct / rows < ZO_VIX_BLOOM_AUTO_MAX_RATIO` (0.25) **or**
  `is_id_like_field_name`). The ratio clause drops `response.time`-class
  fields (strings nobody queries by equality) — `default` goes 1.10 → ~0.6 MB
  per file. Explicit stream `bloom_filter_fields` stay additive regardless.
- **W2. Emission** reuses the per-field SBBF path (`bloom_field_names` →
  `bloom_pairs` in the k-way merge sink `merge.rs:850`; the finish loop at
  `writer.rs:3673` for first encodes). Sections land in the existing `bloom`
  blob v1 format — no blob format change. Selected fields get `"bf"`.
  - Merge site: the planner already counts input-dictionary distinct per
    field for AUTO demotion; selection happens there (the k-way hashes while
    streaming). Σ of input distinct is an upper bound (merge dedups) — errs
    toward including, acceptable.
  - First encode (traces L0, move job, rebuilds): exact distinct from the
    term map at finish; hashing rides the existing finish loop.
- **W3. Cost model** (`default` median): ≤ 456k gxhash64 + SBBF inserts per
  output file (expected tens of ms [INFERENCE until benched]); transient
  memory = section bodies (~1 MB) + per-field staging; blob +0.6–1.1 MB
  (≤ 1 % of data) → +1 % S3 PUT bytes. apisix is dominated by
  `request.body` raw values (4.5 M terms): the ratio clause excludes it when
  near-unique; the safety net is `ZO_VIX_BLOOM_AUTO_MAX_DATA_PCT` (2): fields
  are admitted smallest-distinct first until the additive bloom bytes would
  exceed 2 % of the data object.
- **W4. Not changed**: bloom-only policy (`trace_id,span_id,
  reference.parent_span_id` + AUTO-ID demotion) and the composite section
  stay exactly as they are. Broad composite stays off.

## 4. `.bf` v2 — presence bits, hour regroup, structural admission

- **B1. Footer v2** (version byte 0x02): the v1 footer plus a presence block:
  `field_table_count u32`, names (union over the group's files, from each
  file's docs schema — every column, not only indexed ones), then per file
  `file_id u64 + presence bitmap (ceil(F/8) bytes)`; the whole footer
  zstd-compressed. Estimate for a 256-file `default` chunk: names 75–150 KB +
  bitmaps 256 × ~1 KB, ≤ 100 KB compressed [INFERENCE; measure on the first
  real hour]. The pruner's 16 KiB suffix probe becomes "probe 64 KiB, second
  ranged read if the footer is longer"; parsed footers stay in
  `BLOOM_FOOTER_CACHE`.
- **B2. Prune semantics** per (group, predicate on field f): f not in the
  group's table → drop every file of the group, no row read; per file:
  presence bit 0 → drop (no row has f non-null, equality cannot match); bit
  1 and f has a section (per-field, or composite coverage for bloom-only
  fields) → row probe as today; bit 1 and no section (low cardinality) →
  keep, no info.
- **B3. Admission**: `collect_decidable` admits any `=` / `IN` / same-field
  `OR` predicate on any field — no name heuristic, no scope gate for the
  additive path. Cost is structural: a low-cardinality field has no section
  (zero row reads), presence is footer-local, and a group costs one cached
  footer + one `M × 32` row per (predicate, value). `auto_id_scope` /
  `VixBloomCompositeScope::Only` keep governing only composite (bloom-only)
  coverage. This is what retires the 2026-09-04 objection: common-value
  predicates on low-cardinality fields cost nothing, and 7 days of `default`
  are ~340 groups instead of ~9,000.
- **B4. Grouping**: the assembler builds an hour's `.bf` set when the hour
  settles (`settled_hour_cutoff`, exists) over all its indexed files, ≤
  `ZO_COMPACT_BLOOM_CHUNK_FILES` (256) per chunk (`default` ≈ 350 pairs/hour
  → 2 chunks). Files that pend later in a settled hour (re-merges, heals) are
  served by the per-file probe (1b) until a **coalescing rebuild** triggers:
  `pending ≥ ZO_COMPACT_BLOOM_REGROUP_MIN_FILES` (16) or `pending / total ≥
  ZO_COMPACT_BLOOM_REGROUP_MIN_RATIO` (0.25) or oldest pending >
  `ZO_COMPACT_BLOOM_REGROUP_MAX_AGE_HOURS` (24). A rebuild reads every file's
  `bloom` blob (offset from the sidecar footer — 2 GETs per file, or 1 with
  S4), writes new chunk `.bf`s, stamps all rows with the new `bloom_ver` LAST
  (crash before the stamp just re-queues, as today), then deletes the
  superseded `.bf` objects with the tombstone pattern at
  `compact/bloom.rs:422`.
  - Cost: a `default` hour ≈ 350 × ~2 MB blobs = ~0.7 GB read, ~1 GB of `.bf`
    written per settle (body = Σ blooms; 256 files × 2 MB ≈ 540 MB per
    chunk, resident during the transpose → the chunk knob is the memory
    knob: 128 halves it and doubles groups). Per day for `default` ≈ 18 GB
    read / 13 GB write fleet-wide; traces the same order.
  - New files always carry blobs, so the dictionary-fallback path (100 % of
    builds today, 0.07–4 s of dictionary streaming each) is left to old files
    under its existing budget — a compactor CPU saving that the perf gate
    should show.
- **B5. Compatibility**: v2 is per object; v1 and v2 groups coexist. A v1
  querier reading a v2 `.bf` fails the version check → "keep all" (existing
  fail-open). Rollout order is therefore **querier → compactor → ingester**.

## 5. Old-file convergence

- Sidecars get `layout_gen=2`. The single-file sweep (`classify_core_file`)
  classifies `layout_gen < 2` as a sidecar-only heal (column-derived
  rebuild, the existing path), budgeted by `ZO_COMPACT_LAYOUT_HEAL_BUDGET`
  (256 files per pass), newest hours first, stopping at retention. A healed
  file re-pends `bloom_ver = 0` and its hour regroups under B4. 7 days of
  `default` ≈ 60k files; at 256 / pass / 10 min fleet-wide ≈ 8 h of passes
  [INFERENCE — the heal cost per `default` file is a gate measurement].
- Old hours keep their v1 chunks until regrouped; the pruner handles the mix.

## 6. Verification gates (write-side first — owner requirement)

1. **Local write-side, before any PR merges**
   - `merge_bench` on the vixbench 8×2M pure-default corpus (M10 harness,
     medians of 3 alternated runs): merge wall ≤ +3 %, VmHWM ≤ 1.05×, index
     share reported.
   - First encode: `full_path_bench` (traces-shaped) and the segment-builder
     bench: wall ≤ +3 %, HWM ≤ 1.05×.
   - Prod pairs re-indexed with the new options (`/tmp/vixdef2` `default`,
     `/tmp/vixab` apisix): build wall, additive field list (sanity against the
     ratio rule), bloom blob / footer / dict bytes.
   - Assembler: one 256-file chunk from 2 MB blobs: wall, RSS, `.bf` size,
     footer size compressed.
   - Sidecar-only heal of one `default` file: wall and bytes (sets the heal
     budget).
2. **Local read-side**: cold-miss GETs/bytes on the re-indexed pairs
   (`eager_tail_probe` style): ≤ 2 GETs / ≤ 300 KiB; `bloom_pruner` tests for
   the v2 footer: absent field → drop, present-no-section → keep, mixed
   v1/v2 groups, v1 reader fail-open on v2; `.bf` presence built from the
   docs schema, not the fields table.
3. **Prod canary, compactor first** (skill
   `openobserve-compactor-prod-validation`): one compactor on the new image
   ≥ 2 h with `ZO_VIX_BLOOM_AUTO_MIN_DISTINCT=1024`: merge throughput
   (files/h, MB/s per slot), CPU and RSS vs peers, `[COMPACTOR:BLOOM]` pass
   wall, S3 PUT bytes; abort at merge wall/MB > +5 % or RSS > +10 %. Then
   ingesters (traces L0 builds): `segment_build` p50/p95 and builder CPU
   within ±5 % for 1 h. Then the fleet.
4. **Query A/B on a sealed window** (skill
   `openobserve-needle-lookup-bloom-prune-health` §5; parity via
   `openobserve-same-cluster-scan-oracle`): `call_uid` / `user_id` /
   `label.domain` needles over 7 days — bloom phase ≤ 0.5 s (from ~9 s),
   groups ≤ 400, ≥ 60 % of files never reach vix search (absent-field drops
   counted separately), per-follower `io_accounting` remote bytes per cold
   file ≤ 300 KiB, hits identical to the oracle.

## 7. Knobs (as implemented, 2026-10-14)

New: `ZO_VIX_BLOOM_AUTO_MIN_DISTINCT=1024` (0 off),
`ZO_VIX_BLOOM_AUTO_MAX_RATIO=0.25`, `ZO_VIX_BLOOM_AUTO_MAX_DATA_PCT=2.0`,
`ZO_VIX_SIDECAR_TAIL_ZSTD=false` (S1+S2 together; default off until every
reader parses a compressed puffin footer), `ZO_VIX_SIDECAR_FIRST_OPEN=false`
(S3 reader side), `ZO_COMPACT_BLOOM_CHUNK_FILES=256`,
`ZO_COMPACT_BLOOM_REGROUP_MIN_FILES=16`, `ZO_COMPACT_BLOOM_REGROUP_MIN_RATIO=0.25`,
`ZO_COMPACT_BLOOM_REGROUP_MAX_AGE_HOURS=24`, `ZO_COMPACT_BLOOM_MAX_CHUNKS=8`,
`ZO_COMPACT_BLOOM_GC_MIN_AGE_SECS=3600`, `ZO_COMPACT_SIDECAR_REPACK_BUDGET=0`
(§5; the design's `ZO_COMPACT_LAYOUT_HEAL_BUDGET` — a repack, not a heal, see
`.work/coldmiss/DECISIONS.md`), `ZO_BLOOM_STRUCTURAL_MIN_GROUP_FILES=32`
(B3 bound, added in review: a query admitted ONLY structurally runs its
`.bf` stage when the set is ≤ 64 groups or averages at least this many files
per group — i.e. was built in chunks; on the pre-B4 one-group-per-file set
its stamped files are kept unprobed. 0 = always).
Existing, retuned later: `ZO_VIX_EAGER_TAIL_BYTES` 768 KiB → 256 KiB (code
default already 256 KiB; the help text was stale).

## 8. Open for review

1. The 0.25 ratio clause (exclude near-unique non-ID strings such as
   `response.time`): keep, or accept the extra ~0.5 MB/file for simplicity?
2. Chunk size 256 (≈540 MB resident per chunk on the assembler) vs 128
   (twice the groups per query).
3. Heal budget / convergence speed vs compactor load — decided after gate 1
   measures one sidecar-only heal.
4. Out of scope unless wanted: `count_inner` still bundles the 256 KiB terms
   footer window on a cold leaf; the SimpleSelect/miss path never reads it.

## 9. Implementation status (2026-10-14, local; ENGINE-BACKLOG.md has the numbers)

Everything in §2–§5 is implemented on `vix-arch`, with these corrections to
the text above (all recorded in `.work/coldmiss/DECISIONS.md`):

- §2 S3 is NOT a lazy data-footer refactor of `from_containers`: the writer
  stamps the data object's footer JSON on the sidecar (`data_footer_v1`,
  every write site incl. the heal and the repack) and the sidecar-first open
  synthesizes the data container from it; `oversize_skips`/`row_group_size`
  therefore travel inside that JSON, nothing is copied field by field.
- §2 S5 `bf` is a new wire flag (`1 << 4`), distinct from `bloom`
  (bloom-only); `check_merge_inputs`/`classify_core_file` ignore it.
- §3 W1 selection happens at FINISH on exact counts on every path
  (including spilled rebuilds and the merge k-way), never from the
  block-approximate planner counts; the bloom blob is built after the docs
  encoder joins so the byte budget sees the final data size. A non-ID-like
  candidate frees its hashes as soon as it crosses the ratio cap, bounding
  the tracking memory to the fields that can still qualify.
- §4 B1 presence bits carry a per-file AUTHORITATIVE flag = the data
  object's `columns_complete` license (also stamped on the sidecar); an
  unlicensed file's bits are "no info" — the pruner never drops a file whose
  `_source` might hold a field that never became a column. The v2 tail
  magic is `O2B2` so a v1 reader fails deterministically on the suffix
  probe (the version byte is only seen on whole-blob reads).
- §4 B4 chunk folding uses `Sbbf::fold` to the chunk's smallest block count
  per field (no signature grouping); consolidation reads small chunks back
  (de-transposed) and CAS-restamps their rows (`update_bloom_ver_from`);
  the regroup triggers are computed from the hour's rows (one bounded
  `(stream, date)` SELECT), not a new aggregate statement.
- §5 is a sidecar REPACK (`vortex_index::repack_sidecar`): blobs copied
  verbatim, new stamps + AUTO sections from one dictionary walk, published
  through the heal's generation path; 150 ms on the prod `default` pair.
- §8.1 kept the 0.25 ratio clause; §8.2 kept 256 files per chunk; §8.3 the
  repack budget defaults to 0 (the measured cost is one sidecar
  download + upload + ~150 ms per file).

Gate 1 (write side) and gate 2 (read side) were run locally — results in
ENGINE-BACKLOG.md (2026-10-14 entry) and `.work/coldmiss/gates.md`; gates 3
and 4 are the prod canary and the sealed-window A/B, pending the rollout
order listed there.
