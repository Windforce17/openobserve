# DESIGN-MERGE — merge architecture review + M31 target design (2026-08-26)

Requested by the owner after .121/M30: "review the whole architecture, find
the best way to merge files." Grounded in the 2026-08-26 prod measurements
(below), the full-path code survey (ingest→L0 §2, merge shapes §3, scheduling
mapped in M29/M30 notes), and two constraints set by the owner: **no CPU
limit raise** and keep the already-identified fixes.

## 1. The measured problem: equilibrium, not throughput

Post-M30 (.121, gate 4/4, kills=0) the fleet sustains 6.7k merges/h — 4.2x —
and traces/default per-hour file counts STILL hold ~9-9.7k flat. Arrival mix
for that one stream (30m window, 2026-08-26 ~07:4xZ, ×2 for /h):

- ~7,100/h bulk L0 at 128-512MB (~2.2TB/h) — the real data; merges 3→1 to
  the 1GB target in ONE hop. Healthy.
- ~3,500/h TINY files <1MB (~85% of them landing in hour partitions >1h old,
  most 1-record ~3KB) — ~0 bytes of data, pure file-count poison. Each needs
  ~3 merge HOPS (128-wide → 0.4MB → …) to reach target, and today each hop
  builds and discards a full inverted index.
- Consumption: ~6k files/h for this stream. Net ≈ 0. The backlog neither
  grows nor drains; hour partitions converge only days out; queries over
  recent hours plan over ~9k files.

Conclusion: past M30, more merge horsepower is the wrong axis (and the CPU
budget is capped by owner constraint). The fix is WORK PER FILE and WORK PER
BYTE. Three leaks, three fixes.

## 2. Leak 1 — tiny files are minted at the source (ingest hour-split)

Mechanism (segments.rs): `split_by_hour` (:1855) slices every build batch on
EVERY distinct hour present and `build_one_file` (:1933) builds each slice
unconditionally — no size floor, no age gate, no cross-cycle memory. Traces
`_timestamp` = span START time with `ZO_INGEST_ALLOWED_UPTO=5h` past /
`24h` future, so ~30 hour buckets are legal targets per build; every cycle
that carries one late span for hour H mints another 1-record ~3KB file (the
~3KB is pure per-column metadata floor). Every existing coalescing gate
(#24 per-stream chunking, #44 claim floor, #54 superbatch, M13 aging lane)
batches along the segment/byte/claim axis — none along the hour axis.
The repo has already named the missing piece twice and not built it:
- #24 residual lever (ENGINE-BACKLOG:1115): "cross-claim per-stream
  accumulation (defer a stream's build until size/age)".
- The legacy WAL mover's per-hour accumulate-or-age gate
  (jobs/files/parquet.rs:700-741) which the segment builder never inherited.

**Fix (M31a, ingest side): per-(stream, hour) accumulate-or-age gate in the
segment builder.** Off-current-hour slices below a byte floor (e.g. 8-32MB
decoded) are parked in a persistent pending pool (per-segment pending-stream
accounting, exactly the #24 lever) and built only when the pool reaches the
floor OR its oldest row exceeds an age bound (e.g. 600s = the legacy mover's
`ZO_MAX_FILE_RETENTION_TIME` convention). Effect: the 1-record spray
(~3.5k files/h/stream) collapses to tens/h; old-hour partitions stop being
re-polluted, so merged hours STAY converged. This is the highest-leverage
single change in the whole review.

[SUPERSEDED AS DESIGNED, KEPT AS BUILT 2026-08-26 (.124): the invariant
survey killed the pending-pool shape — per-(stream,hour) withholding has no
representation in the id-range provenance (dedup_candidates suppresses by
(stream, segment id-range): an L0 covering an id whose hour-H rows were
withheld makes them INVISIBLE), Built is terminal for tail visibility, and
no row-level dedup exists to absorb an at-least-once variant. The shipped
design moves the split UPSTREAM where the unit is naturally whole-segment:
the ingest buffer segregates late frames into their own ALL-LATE segments
(classifier created_at - max_ts, no schema/format change) and a third claim
lane (LateOldestFirst) picks them up after a 900s hold — one wave per hold
window coalesces the fleet's late rows. Same accumulate-or-age effect, zero
provenance surgery. ENGINE-BACKLOG "M31a — INGEST LATE LANE" has the
details.]

## 3. Leak 2 — the index is built 2-3x per byte (merge shapes)

Cost model (survey of core_writer.rs/vortex_index, measured anchors in
ENGINE-BACKLOG):

| shape | when | cost | gate |
|---|---|---|---|
| A. indexed fast path (k-way dict merge + docs passthrough) | ALL inputs have sidecars | dict walk + postings remap + #52 bloom scan (its 64%-of-wall dominant); docs copied, never decoded | none |
| B. rebuild (+#51c docs passthrough, #46 column-derive) | any input lacks a sidecar — i.e. EVERY gen-1 merge over index-off L0s, and every mixed group | derivation scan DOMINATES (M17: 113.6s of 133.8s); docs copied | REBUILD_GATE |
| C. sidecar-only heal | single-file | 1-file derivation, no docs write | none |
| D. index-off merge (`index_enabled=false` plan) | today: only stream types in ZO_VIX_INDEX_DISABLED_STREAM_TYPES | ts columns + footer/zone/stats + ENCODED CHUNK COPY. No dictionary, no postings, no bloom, no derivation, no decode | none |

Today's flow for one traces byte: L0 (no index) → gen-1 REBUILD (builds
index #1) → often gen-2 REBUILD (mixed group ⇒ index #1 discarded, index #2
derived from scratch, at the 5.4x-costlier `_source` arm since #46 requires
all-index-off inputs). Tiny files pay a full index build per hop for outputs
that live minutes.

**Fix (M31b, merge side): index once per byte — "shape D for every
non-final hop".**
1. Add a per-plan index-off override (build_merge_plan:1798 reads
   `opts.index_enabled`; today only the stream-type global sets it). Policy:
   if the GROUP's summed original_size lands the output below
   `ZO_COMPACT_MAX_FILE_SIZE/2` (i.e. it will provably be merged again — the
   debt predicate's own "small file" line), plan index-off ⇒ shape D: pure
   encoded-chunk concat, no gate, no derivation. Tiny-file hops become
   ~free.
2. Final hop (output ≥ the line) keeps today's rebuild — which now runs
   ONCE per byte. Two cheapeners ride along:
   - drop `_source` from `merge_scan_projection` (:2879) when
     `derive_from_columns && docs passthrough` — the survey found it is
     decoded and then used only for a null/length check (writer.rs:2230) on
     exactly the dominant prod shape; the fattest column in the file,
     decoded for nothing.
   - verify #46 column-derive actually engages on the all-L0 shape in prod
     (log it), since `_source`-arm derivation is priced at 5.4x.
     [RESOLVED 2026-08-26, M31b(0): it NEVER engaged — prod L0s store
     strings as Utf8View, the registry says Utf8, and the gate's strict
     DataType equality rejected 909/918 fields. Fixed with string-family
     equivalence + a gate-miss reason log + the terms_from_columns summary
     signal; parity referee extended to the drift shape.]
3. Sidecar-homogeneous grouping: `index_size > 0` is already in FileMeta at
   group-formation time (zero IO). Never mix indexed and index-less inputs
   in one group — indexed-only groups take fast path A (no gate at all);
   index-less groups take D or the final-hop rebuild. Mixed groups (the
   worst case: full rebuild discarding good dictionaries) stop existing.
   (True partial index reuse inside a mixed rebuild is possible — blocked by
   exactly writer.rs:3143 + the merge-first invariant — but grouping makes
   it unnecessary; do not build it.)

Guard rails for D-hops:
- Region-table cap (WRITER_REGION_CAP=4096): concat outputs accumulate
  regions ≈ Σ input regions; concat-of-concat generations can overflow the
  cap and silently lose piecewise ordering. Group formation must budget
  Σ input regions ≤ cap/2 per D-merge; the final rebuild resets ordering.
- D-outputs carry spliced zone/stats (passthrough requires them), so query
  pruning is intact; they lack term indexes exactly like today's L0s — the
  interim window is minutes at current drain speed, and queriers already
  handle index-less files (that IS the L0 read semantic).
- ZO_VIX_L0_INDEX_OFF_STREAM_TYPES stays as-is (build-side); rollback knob
  for M31b is the policy env (0 = always index at merge = today).

## 4. Leak 3 — slot value: 128-wide cap wastes slots on tiny files

`ZO_COMPACT_MAX_FILE_COUNT=128` binds only for the tiny population
(128 × 3KB = 0.4MB output). With M31a the population shrinks drastically and
with M31b its hops are free-ish, so this becomes a second-order fix:
**size-classed width cap** — groups whose max input < ~4MB may take up to
1024 files (reader state scales with input size; region budget per the §3
guard). One slot pass then removes ~1k junk files. Ship only if the
tiny backlog is still visible after M31a+b.

## 5. Scheduling — reviewed, mostly right, two conditional items

The M29-era scheduling (three generation lanes + debt sweep, fleet-wide
id-ASC SKIP LOCKED claims, worker-sized claim batches, streaming fenced
commits, lease heartbeat-from-claim) survived this review intact: at 6.7k
merges/h the queue stays fed (pending 314) and no lane starves. Two items
stay chartered but CONDITIONAL:
- Intra-hour fan-out (#23): shard fat hours across pods. After M31a+b an
  hour holds ~10-30 real files — likely moot. Revisit only if a
  pathological-hour storm recurs.
- Claim-on-slot-free: replaces up to ~12s of idle per job slot; single-digit
  % — only worth it if post-M31 measurements show worker idle with
  pending > 0.

## 6. Expected arithmetic (traces/default, per hour of data)

Today: ~10.6k files arrive (7.1k bulk + 3.5k tiny); ~2.4k final-shape merges
needed but ~3-5x that many hops actually run, each hop paying an index
build; equilibrium at 6k files/h consumed.
After M31a: arrivals ≈ 7.2k/h (tiny spray gone at source).
After M31b: index builds/byte = 1 (was 2-3); non-final hops are copy-only
(no gate, ~no CPU); the final rebuild loses the wasted `_source` decode.
Net: required gate-work per data-hour drops ~2.5-4x while arrivals drop
~30% — the same fleet at the SAME CPU envelope flips from treading water to
draining, and hour partitions converge in tens of minutes, not days.

## 7. Migration / rollout order (each stage independently shippable)

- M31a ingest accumulate-or-age gate — harness: replay a late-heavy trace
  capture through the builder, assert files/hour-partition; prod acceptance:
  tiny-arrival rate on file_list (<1MB rows/h, was ~3.5k for traces).
- M31b(1) `_source` projection drop + #46 engagement logging — merge_bench
  A/B on the gen-1 shape (expect wall −20-40% on the derivation scan).
- M31b(2) per-plan index-off + size policy + sidecar-homogeneous grouping —
  merge_bench: tiny-file ladder (3KB×128) and mid-ladder (316MB×3) A/B;
  assert D-hops never acquire the gate; region budget property test.
- M31c size-classed width cap — only if tiny backlog persists post-a+b.
- Measurement gates before prod, per house rule; brakes: every stage has a
  config-only off switch; no on-disk format changes anywhere (D-outputs are
  the existing column-store-only file class).

## 8. Out of scope / rejected

- CPU limit raise — owner constraint; all levers above REDUCE CPU/byte.
- Dropping L0 entirely (buffer to 1GB at ingest) — violates durability/
  freshness bounds; L0 already one-hops to target for the bulk.
- Partial index reuse in mixed groups — obviated by sidecar-homogeneous
  grouping (§3.3).
- Merge-side ranged dictionary reads (fast-path IO) — real but the fast
  path is not the bottleneck shape; revisit when A-shape dominates.
