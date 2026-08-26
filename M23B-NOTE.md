# M23b — interleaved-order rebuild OOM: bounded decode for k-way scattered consumption

Worktree pinned at `ba774c3249` (post-M23 engine, `.113`-era). 20C/38G box,
release builds (`-j8`), mimalloc + the M23 live-bytes counting wrapper
(`[rss]` sampler: VmRSS/VmHWM/live every 500 ms). All scratch under
`m23b-data/` in this worktree.

Local commits (worktree only, nothing pushed): `85c1c97027` harness
(interleaved corpus + merge-flip), the fix commit, then this note.

## Verdict

**Reproduced, root-caused, fixed (the decode transit), and one adjacent
pre-existing accumulator isolated with evidence.**

M23's lazy spawn only helps when the merge order CONSUMES inputs one at a
time. On an interleaved k-way order (prod's overlapping same-(stream,hour)
L0 files) every input is touched within the first output window, so lazy
degenerates to eager — and the deeper structural fact is that the
free-running decode stream's unit is the STORED docs chunk (16 MiB
uncompressed budget), which for the small-L0 population is **effectively the
whole file**. Bounding "chunks in flight" therefore bounds nothing: the
whole group sits decoded in RAM again. Measured on the interleaved twin of
the M23 D corpus: **peak 11399 MB, the whole scan long** (fill to ~10 GB in
9 s, plateau 10–11.4 GB for 80 s) — same size class as the unfixed M23
baseline (11065 MB), on a tree that HAS M23.

The fix: inputs whose rows are SCATTERED across the merge order stream
through **gated row-range decodes** — `caps`-sized units decoded on consumer
grants (demand + low-water prefetch), deep-copied so every unit is
independently freeable. Peak drops **11399 MB → 7924 MB** with the decode
transit itself now **flat at ~1.3–1.9 GB** (hard-bounded at
N × ≤2 units ≈ 3 GB for 256 inputs; gauge-proven, below). Outputs
byte-identical on both corpus shapes; interleaved wall +5.5 %; the
concatenation shape is bit-for-bit untouched (same machinery, same bytes,
same peak). The residual profile above the transit is a **pre-existing
standard-rebuild writer-side accumulator** that this work isolated but did
not fix (evidence below — it climbs identically with free-running streams on
a pure concatenation corpus, so it is not a decode/transit problem).

## Repro

Everything in `src/core/examples/m23_rss_repro.rs` (extends the M23
harness; env pinned by re-exec as before; `M23_SCRATCH` defaults to this
worktree's `m23b-data/`).

```
cargo build -j8 --release -p openobserve-core --example m23_rss_repro

# interleaved twin of the M23 D corpus: same 256 files x 28125 rows /
# 1.68 GiB data / ~3.11 GiB original, but ts = base + row*256 + file —
# every file spans the SAME range, the k-way DESC order round-robins
# through all 256 inputs in every window
target/release/examples/m23_rss_repro gen-il m23b-data/corpus-il 256 28125

# merge-flip: same compactor entry, but the CURRENT stream schema types
# `code` Utf8 while the files store Int64 (real schema evolution). Needed
# because the heal passthrough absorbs an overlapping group into its
# #51c-c CONCATENATION order and masks the interleave; the type flip fails
# every input's qualification (qualified==0 -> heal off) and disables #46
# column derivation, so the merge runs the STANDARD rebuild: k-way
# interleaved order, terms from _source — prod's OOM signature
# ("rebuilding terms from _source").
target/release/examples/m23_rss_repro merge-flip m23b-data/corpus-il m23b-data/out/merged-il.vix

# concatenation reference (M23's proven shape, via the heal arm):
target/release/examples/m23_rss_repro merge <corpus-d dir> m23b-data/out/merged-d.vix
```

Path verified in logs: `row merge order over 256 inputs (disjoint: false)`,
`docs-copy rebuild disqualified: none of the 256 inputs qualified`,
`concat_order=false`, `docs_passthrough_inputs=0`.

## Step 1 — the regression baseline (lazy-spawn tree, interleaved corpus)

`m23b-data/logs/merge-il-before.log` (tree = ba774c3249, engine untouched):

```
t=  1.0s vmrss=  418MB   live=  334MB   <- window 1 touches all 256 inputs: lazy spawns ALL
t=  5.0s vmrss= 7167MB   live= 6240MB   <- free-running producers filling
t= 10.0s vmrss=10032MB   live= 8563MB   <- FILL ~COMPLETE: whole group decoded
t= 25.0s vmrss=10933MB   live= 9714MB      (plateau: interleaved consumption frees
t= 55.0s vmrss=10994MB   live= 6642MB       nothing until a whole file drains)
t= 75.0s vmrss=11329MB   live= 7450MB
merge done: 79.67s   879 windows, 7.2M rows, 50245 terms, out 1704.8 MiB
peak: vmhwm=11399MB
```

Whole-group-decoded confirmed (11399 MB ≈ 3.6x original, the M23 size
class) — M23's lazy spawn is inert on this shape. Unlike the concatenation
run there is no drain slope: every input holds its buffered decode until its
own tail is reached, so the plateau spans the entire scan.

Why the channel bound cannot help (the "structural fact", verified): the
writer sizes stored docs chunks by a 16 MiB uncompressed budget
(`docs_rows_per_chunk`, `vortex_index/src/writer.rs`); a 12 MB-original L0
file is 1–2 stored chunks, and `scan_docs`'s per-callback batch is the
stored chunk — so `sync_channel(2)` + the in-flight batch ≈ the whole file
decoded, per input. Gating those free-running streams without changing the
unit was measured as useless: an intermediate attempt that kept whole-blob
scans and parked producers between stored-chunk units peaked **12809 MB**
(WORSE than baseline — the unit IS the file).

## The fix

`src/core/src/vix/core_writer.rs` + one reader addition
(`vortex_index/src/docs.rs::scan_docs_row_range`).

1. **Order classification** (`stream_merge_windows`): one pass over the
   precomputed merge order marks each input CONTIGUOUS (its rows form one
   run — concatenation shapes) or SCATTERED. Gated streaming engages only
   when ≥ `MERGE_SCATTERED_INPUTS_MIN = 8` inputs are scattered; below
   that (and for every contiguous input) the free-running stream + M23 lazy
   spawn remain — bit-identical machinery to M23 for the fleet's dominant
   shape, and low-N interleaves keep full decode-ahead.
2. **Gated row-range producers** (`spawn_ranged_input_stream`): a scattered
   input decodes `ranged_unit_rows = caps.rows/2` (4096) rows per grant via
   the new `VixDocs::scan_docs_row_range` (a `RowSelection::Range` scan —
   only the selected rows materialize). The producer PARKS BEFORE decoding
   (`DecodeGate`) — a parked producer holds no decoded data. Units are
   split into `unit/4` parts and **deep-copied** (`take`-gather; NOT
   `concat`, whose single-input fast path returns a buffer-sharing slice),
   so consumed rows free independently of the reader's internal backing
   buffers.
3. **Consumer admission** (`InputCursor`): grants are issued only when
   nothing is in flight (`granted <= delivered + 1` — at most ONE unit in
   flight per input beyond the delivered-and-unconsumed one), from two
   sites: DEMAND (always, immediately before a blocking `recv`) and
   LOW-WATER prefetch (remaining delivered-unstaged lookahead <
   `unit/4` = 1024 rows) so the next unit's decode overlaps the remaining
   consumption instead of stalling the scan.
4. **Same-shape siblings fixed** (upfront spawn -> spawn at drain start):
   `stream_inputs_disjoint` (fast-path decode arm) and
   `rebuild_with_docs_passthrough` phase 2 (heal fail-open inputs). Both
   drain strictly input-by-input, so the channel bound is the right cap
   once the spawn is lazy — no gate needed there.

Constants (all hardcoded with provenance comments, no env knobs):
`MERGE_SCATTERED_INPUTS_MIN = 8`; unit `= (caps.rows/2).max(1024)` = 4096;
parts `= (unit/4).max(256)` = 1024; low-water `= (unit/4).max(64)` = 1024.

Cost accepted by design: a stored chunk intersecting k units decodes k
times (~x2 at prod chunk geometry of 5–10k rows/chunk; ~x6 on this
corpus's small-row 24k-row chunks). Gated inputs exist only in ≥ 8-way
interleaves where each input is consumed at ≤ 1/8 of the scan rate, so the
redundant decode rides idle cores (measured: wall +5.5 % at the 256-way
worst case, and that includes the deep copies). Read amplification of the
input bytes (~x2–6 compressed fetches) lands on the compactor's local disk
cache, not S3.

### Deadlock freedom (grant protocol)

Argued in the `DecodeGate` doc comment, exercised by the tests and the
256-way repro (~1758 grant cycles/run):

- The consumer blocks ONLY in `rx.recv()`, and every blocking recv is
  preceded by `ensure_grant` — a not-yet-exhausted producer always holds a
  grant covering the unit the consumer waits for; an exhausted producer has
  dropped `tx`, so recv returns immediately.
- A granted unit whose rows all cleansed away delivers nothing and consumes
  no grant (the producer proceeds to the next range under the same grant),
  so the consumer's delivered-units view cannot drift.
- The producer blocks only in `tx.send` (released by recv, or by cursor
  drop -> send errs) or in `await_grant` (released by a grant, or by
  `close()` in `InputCursor::drop`). Cursors are locals of the
  `thread::scope` closure, so they drop — closing gates and channels —
  before the scope joins, on success and unwind alike.

## Before/after, both corpus shapes (all byte-identity sha256-verified)

### Interleaved (corpus-il + merge-flip, 256 x 12 MB, 3.11 GiB original)

```
BEFORE  peak vmhwm=11399MB  wall 79.67s   fill to ~10GB in 9s, plateau all scan
AFTER   peak vmhwm= 7924MB  wall 84.09s   transit FLAT ~1.3-1.9GB; profile = map + pre-existing writer climber (below)
        outputs identical: .vix 3fcbf88e…, .vxi 934cc5fa… (equal before/after)
```

After-fix condensed trace (`m23b-data/logs/merge-il-final.log`):

```
t=  2.0s vmrss= 1398MB live=  678MB   <- 256 gated streams up: ~1 unit each
t=  9.8s vmrss= 2933MB live= 1298MB   <- was 10032MB here before
t= 19.8s vmrss= 3718MB live= 2215MB      (slope from here on ≈ the writer-side
t= 39.8s vmrss= 4956MB live= 3877MB       climber + term map, NOT transit)
t= 59.9s vmrss= 5919MB live= 4667MB
t= 74.9s vmrss= 7491MB live= 6096MB
merge done: 84.09s   879 windows, identical output
peak: vmhwm=7924MB
```

Transit gauge (debug line every 100 windows — counts only): pending rows
and in-flight units are CONSTANT across the whole scan, proving the bound:

```
window 100..700: 256 cursors, 262144 pending rows (~0.4GB), 256 units in flight (=1/input)
window 800:      256 cursors,      0 pending rows,          256 units in flight
```

Peak decode transit ≈ 256 x (1 delivered + ≤1 in-flight) x 4096 rows x
~1.46 KB/row ≈ ≤3 GB bound, ~1.3–1.9 GB observed — **O(N x unit)**, vs
~9–10.5 GB (whole group) before. Wall +5.5 % (79.67 -> 84.09 s; two
earlier runs of near-final code: 82.0 s, 82.8 s) — within the ≤ ~30 % bar,
and it prices in the deep copies + redundant ranged decode.

### Concatenation (M23 D corpus, plain merge -> heal arm)

```
BEFORE  peak vmhwm=4276MB  wall 86.37s   sha .vix e344a7b7…, .vxi f2fc969d…
AFTER   peak vmhwm=4327MB  wall 76.26s   sha identical (e344a7b7…, f2fc969d…)
```

No regression, byte-identical (also equal to the M23 note's shas — this
tree + corpus reproduce M23 exactly). Wall variance is box noise (M23's
canonical number was 76.0 s). The heal/concat path never sees a gated
stream (0 scattered inputs).

## Design chosen vs rejected

Chosen: **(a) small-unit streaming** — the reader supports row-range
selection (`RowSelection::Range`, already in `scan_blob_streaming`); a
thin public `VixDocs::scan_docs_row_range` exposes it. Units are true
`caps`-sized row windows even within one stored chunk/file, deep-copied to
be independently freeable — NOT decoded-then-sliced (a slice keeps the
whole decode alive; and gating the free-running stream at its natural unit
was measured at 12809 MB, worse than baseline, because that unit is the
whole file for small L0s).

Rejected: **(b) aggregate byte budget across free-running streams.** Two
reasons, one structural, one measured:
- The budget's unit of blocking is whatever the stream naturally produces —
  the stored chunk ≈ the whole small file, so the floor
  ("every mid-flight input's current unit") stays ~the whole group; the
  12809 MB intermediate attempt is essentially this design's best case.
- A bytes-only ledger cannot schedule WHICH input decodes next. Uniform
  interleaved consumption needs the input closest to exhaustion served
  first; FIFO budget wakeups approximate that only statistically and
  degrade to demand-stalls (~decode-latency per chunk boundary on the
  consumer's critical path). The grant protocol IS the fixed version of
  (b): a per-input budget of one unit, scheduled by demand/low-water.

## Isolated (NOT fixed): the standard-rebuild writer-side accumulator

With the transit flattened, the remaining interleaved profile is a linear
~+55–70 MB/s live climb, freed only at `finish`. Attribution run
(`merge-d-flip`: the CONCATENATION corpus through the same flip/standard
arm — FREE streams, zero gated code, zero interleave):

```
d-plain (heal arm):      live 0.5 -> 1.9GB over 75s (+20MB/s = the term map)   peak 4276MB
d-flip  (standard arm):  live 0.6 -> 5.6GB over 99s (+55MB/s)                  peak 6274MB  wall 99.9s
```

Same corpus, same output rows/terms — the +3.7 GB delta rides the standard
rebuild's source-derivation + docs re-encode writer arm regardless of
stream mode, and is invisible in the heal arm prod usually takes. It is
the size class of the term map's UNDER-accounted real footprint plus
whatever else the coupled push holds to finish (the M23 note's follow-up
(c), `index_key_terms` bypassing `terms_bytes`, is part of it). At prod
scale this accumulator + the (now bounded) transit + the map fits the
post-M23 fleet's residual-OOM arithmetic. Follow-up, separate change:
account/spill it like the term map. Not touched here — out of the decode
transit's scope and byte-identity risk profile.

## Tests

- New: `gated_ranged_rebuild_matches_free_rebuild_bytes` — one 26k-row
  population with unique timestamps partitioned 10-way (gated: scattered ≥
  8) and 7-way (free: below threshold), rebuilt with 64-row windows +
  `force_decode` (heal off): outputs must be BYTE-identical (data AND
  sidecar) and strictly ts-DESC. Tiny caps make it the grant-protocol
  stress: 1024-row units, 256-row parts, ~6 rows staged per input per
  window, every input mid-flight from first window to last (the "tiny
  budget" deadlock exercise).
- `cargo test -j8 --release -p openobserve-core --lib` (vix/core_writer
  suites included):
  `test result: ok. 1929 passed; 0 failed; 17 ignored; 0 measured; 0 filtered out; finished in 8.19s`
- `cargo test -j8 --release -p vortex_index` (crate touched):
  `test result: ok. 232 passed; 0 failed; 10 ignored; 0 measured; 0 filtered out; finished in 2.96s`
  (+ the crate's remaining targets: 1 passed / 0 failed)

## Files changed

- `src/core/src/vix/core_writer.rs` — `DecodeGate`, gated
  `spawn_ranged_input_stream`/`stream_input_row_ranges`, `deep_copy_array`,
  `InputCursor` admission (grants/lookahead/low-water, `Drop` closes the
  gate), order classification + dual spawn in `stream_merge_windows`,
  transit gauge (debug, counts only), lazy sibling spawns
  (`stream_inputs_disjoint`, `rebuild_with_docs_passthrough` phase 2),
  `merge_scan_projection` helper, constants + new unit test.
- `src/vortex_index/src/docs.rs` — `VixDocs::scan_docs_row_range`.
- `src/core/examples/m23_rss_repro.rs` — `gen-il` interleaved corpus mode,
  `merge-flip` schema-evolution mode, worktree-local scratch default.

## Artifacts

- Corpora: `m23b-data/corpus-il` (generated), M23's `corpus-d` reused
  read-only from the m23-repro worktree (builder unchanged since —
  verified by identical merge output shas).
- Full traces: `m23b-data/logs/merge-{il-before,il-after2,il-final,il-gauge,d-before,d-after,d-flip}.log`.
- Outputs + shas: `m23b-data/out/`.
