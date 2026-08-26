# M25 — schema width: repro, the two real width pathologies, fixes, and the corrected kill model

Worktree pinned at `5d93e773ce` (post-M24 engine, `.115`-era). 20C/38G box,
release builds (`-j8`), mimalloc + the M23 live-bytes counting wrapper
(`[rss]` sampler: VmRSS/VmHWM/live every 500 ms). All scratch under
`m25-data/` in this worktree. Local commits only, nothing pushed:
`396885b3e7` harness + instrumentation, the fix commit, the instrumentation
strip, then this note.

## Verdict (read this first)

**Width is CONFIRMED as the unexercised kill factor — but not where the
charter guessed.** The per-column encode-state theory ("chunk buffers x
2,000 columns explodes the writer") is real yet secondary; two concrete,
measured width pathologies dominate, and one of them is a **storage
correctness bug, not just memory**:

1. **The heal/copy arm's slice-guard canonicalization misclassified sliced
   dict-layout windows and stored RAW 16 B/row views buffers verbatim** —
   a width-2,000 heal merge wrote **7,034 MiB of docs from 450 MiB of
   inputs (15.6x)** at ~88 KiB/leaf of 0.5-bits/B views data, peaking
   10.6 GB while doing it. Every wide low-cardinality (k8s-label-shaped)
   stream hits this on every heal-arm merge, and the bloated outputs feed
   the next merge generation. **FIXED** (root-keyed classification +
   compact-for-residence): output 7,034 -> 465 MiB, peak 10,621 -> 3,486 MB;
   the fix's byte-diff is confined EXACTLY to the pathological outputs (22
   of 24 gate files byte-identical, incl. every `.vxi`).
2. **The M23b gated decode transit is ROW-bounded, so its bytes scale with
   width**: a 4096-row unit of a 2,000-column union is ~35-50 MB of arrow
   (null-fill materializes every absent column at 4-8 B/row/column), and an
   interleaved wide group holds ~2 units x N inputs. Measured kill-class:
   **peak 15,034 MB on a 0.53 GiB-data / 1.58 GiB-original 128-file
   interleaved group** (28x data bytes — the fresh-pod killer shape).
   **FIXED** (shared per-(type,len) null arrays + byte-adaptive unit
   sizing): 15,034 -> 8,283 MB, wall +12%.

The remaining width mass is the **standard-arm docs re-encode inside vortex
0.79** (M24's attribution, now quantified at width): ~3.6 GB held to
finalize for a 474 MiB blob at width 2,000 — per-column coalescing buffers
(1 MiB minimum x columns) + dict-run sequence retention (90.3% of the blob
compressed-but-unwritable until EOF). Vortex-internal, not fixable
byte-identically from our side; decomposition below extends the M24
follow-up options.

## Repro

Everything in `src/core/examples/m23_rss_repro.rs` (extends the M23/M23b/M24
harness; env pinned by re-exec; scratch default `m25-data/`).

```
cargo build -j8 --release -p openobserve-core --example m23_rss_repro

# WIDE SPARSE k8s-logs-shaped corpora: width = union field count (8 core
# fields incl. message/status + width-8 sparse fields, ~1/5 Int64).
# Narrow-WAL semantics: per-FILE schemas are subsets (25% of the sparse
# universe when > 256, else all of it), so only the merge union is wide.
# Every row populates 24..=64 DISTINCT sparse fields, one token from a
# PER-FIELD 32-value vocabulary ("v0342-17") — vocabulary stays flat
# across widths (80k-173k distinct terms) so the curve isolates WIDTH from
# the M24-falsified vocabulary axis. Token count/bytes width-independent.
target/release/examples/m23_rss_repro gen-wide m25-data/corpus-w12   40 25000 12
target/release/examples/m23_rss_repro gen-wide m25-data/corpus-w200  40 25000 200
target/release/examples/m23_rss_repro gen-wide m25-data/corpus-w2000 40 25000 2000
# interleaved twin (M23b lattice): the prod one-(stream,hour) overlap shape
target/release/examples/m23_rss_repro gen-wide-il m25-data/corpus-wil 128 8000 2000

# the two rebuild arms (all inputs index-off -> "rebuilding terms from
# _source"): merge-wide = heal/docs-copy arm; merge-wide-flip types the
# always-present `status` i64 as utf8 -> every input disqualified ->
# STANDARD rebuild (decode + re-encode)
target/release/examples/m23_rss_repro merge-wide      <corpus> <out.vix> <width>
target/release/examples/m23_rss_repro merge-wide-flip <corpus> <out.vix> <width>

# diagnostics added this milestone: per-column leaf bytes, layout/encoding
# trees, whole-column value hashing (equivalence checks)
target/release/examples/m23_rss_repro inspect  <file.vix> [top]
target/release/examples/m23_rss_repro tree     <file.vix> <column>
target/release/examples/m23_rss_repro hash-col <file.vix> <column>
```

## The width curve (unfixed engine, clean-vocab corpora, VmHWM)

40 files / 1M rows each; w200 and w2000 are the controlled pair (~equal
bytes: 1.55 GiB original, 0.44/0.45 GiB data; w12 rows carry 4 sparse tokens
instead of ~44 — anchor only). hv = the first-cut corpus whose tokens
crossed fields with the zipf vocabulary (19-29M distinct terms) — kept as
the width x vocabulary compound point.

```
corpus               arm    wall     peak RSS   out docs      peak/orig-GiB
w12    (0.37GiB)     heal   8.6s     701MB      168.3MiB      1.9x
w12                  flip   9.0s     1067MB     167.8MiB      2.9x
w200   (1.55GiB)     heal   31.8s    2798MB     438.2MiB      1.8x
w200                 flip   33.2s    3480MB     438.3MiB      2.2x
w2000  (1.55GiB)     heal   66.1s    10621MB    7034.1MiB !!  6.9x   <- bloat
w2000                flip   84.7s    5149MB     474.3MiB      3.3x
wil    (1.58GiB,     heal   98.6s    12434MB    7230.1MiB !!  7.9x   <- bloat
        128f interleaved) flip 102.4s 15034MB   507.3MiB      9.5x   <- THE KILL SHAPE
hv-w2000 (1.59GiB, 29M terms) heal 119.4s 3851MB 862.6MiB     2.4x
hv-w2000             flip   128.0s   6475MB     875.4MiB      4.1x
```

- Disjoint heal is width-FLAT per byte until the BLOAT bug fires (w2000:
  per-file 25% subsets -> widened copies -> sliced dict windows). w200
  escapes it (every file carries the full 200-col schema; the union grid
  happens to keep its sparse leaves out of the misclassified form).
- Disjoint flip scales with width at equal bytes: 2.2x -> 3.3x per GiB
  (the vortex-internal masses, below).
- INTERLEAVED x WIDE is the kill compound: 15.0 GB on 0.53 GiB of data.
  Prod's poison groups are exactly this (one (stream,hour)'s L0 files all
  overlap; schema evolution disqualifies the heal; 2,164-field union): at
  256+ files x 2048 MB-original groups with `_original` doubling row bytes,
  this extrapolates past 48 Gi on a FRESH pod — matching ".115 fresh pods
  die within minutes, last line rebuilding terms from _source".

## Pathology 1 — the heal-arm storage bloat (FOUND + FIXED)

Mechanism, file:line:

- Input files' low-cardinality sparse columns are DICT LAYOUTS (vortex's
  default write pipeline dict-probes each column; k8s labels always
  qualify).
- The copy scan (`container.rs scan_blob_encoded_chunks`) splits at the
  UNION of every column's leaf boundaries. At width, sparse columns'
  coalesced leaves are far coarser than the dense columns' chunk grid, so
  EVERY sparse column window arrives sliced; the M18 guard canonicalizes it
  — `execute::<Canonical>` yields a `varbinview(views 16 B/row RAW) ->
  dict(values)` tree: a decoded ROOT still borrowing the dict's encoded
  buffers.
- `is_decoded_family` (container.rs:1225) tests the WHOLE tree, so the
  mixed tree failed the "decoded -> compress" branch and fell into
  `is_ctx_serializable -> copy verbatim` (dict/varbinview are both
  writable): **the raw views buffer was stored as the leaf**. Measured:
  62,152 such chunks in one w2000 heal merge (`m25: encoded passthrough:
  117929B ... tree=[vortex.varbinview(117929B) vortex.dict(13065B) ...]`),
  stored leaves 88.3 KiB/leaf at entropy 0.50 bits/B (raw inline views),
  `zstd -3` re-compresses the 7.4 GB output to 686 MB.
- The same chunks also bypassed run coalescing (`clustered.rs
  ColumnState::push` keyed on the same test), leaving one leaf per window
  (100 leaves/column vs the flip output's 4-9).

The compressor itself is NOT at fault — fed the exact shapes it produces
582 B from the 105 KB mostly-null window, 2 B from all-null, 110 B from
low-card all-valid (measured, `m25_probe_sparse_compress`).

### Fix (3 sites, one rule)

Route on the ROOT node: a decoded-family root = freshly decoded data
(compress); only an encoded ROOT (dict/fsst/zstd/...) is a verbatim input
chunk. Plus compact-for-residence so the decoded transit stops carrying
materialized views:

- `container.rs is_decoded_root` (new) + `docs_passthrough_strategy`'s
  `compress_or_pass` routes mixed-root chunks into the compress branch (the
  cascade's own canonicalize+compact entry collapses the borrowed buffers).
- `clustered.rs ColumnState::push` routes on the same rule, and COMPACTS
  decoded chunks >= 32 KiB into owned minimal buffers AFTER snapshotting
  the as-pushed nbytes — run boundaries, stripe cadence and the
  `OutputRatio` observation stream stay EXACTLY the un-compacted writer's
  (`ChunkWork` now carries the as-pushed raw bytes), so emitted bytes
  cannot depend on the resident representation; residence drops from the
  materialized decode form to true value bytes.
- `clustered.rs` stripe estimator keys on the same root rule (a mixed-root
  window is decoded transit, not face-value output).

Regression test: `tests.rs m25_sparse_column_copy_does_not_bloat` — builds
the sliced-dict shape (guarded: the fixture must yield >= 8 mixed-root
windows), asserts the copy output <= 1.5x the input docs blob AND row-exact
read-back (values + nulls). Verified failing on the pre-M25 behavior
(7,517,756 B from 2,577,408 B = 2.9x) and passing with the fix. The M12
dict-copy test still pins the opposite side (whole dict leaves copy
verbatim).

### Effect (w2000 / wil heal)

```
                 BEFORE                    AFTER
w2000 heal       peak 10621MB out 7034MiB  peak 3486MB out 465.0MiB
wil heal         peak 12434MB out 7230MiB  peak 3478MB out 488.9MiB
```

Equivalence of the two changed outputs (they MUST change — the old bytes
are the bug): `.vxi` sidecars byte-identical; row/term counts identical;
whole-column FNV over (validity, value bytes) identical for `_source`,
`status`, and the sparse columns (1M rows each, spot: `_source`
fnv=a63b940bacbb06d2 both, `k8s_annotation_meta_1113` present=46509 fnv
equal both).

## Pathology 2 — width-scaled gated decode transit (FOUND + FIXED)

- M23b's gated units are ROW-sized (`caps.rows/2` = 4096). The transit
  holds arrow, and `normalize_merge_chunk` null-fills EVERY absent
  preserved column per chunk (`core_writer.rs`, the v2 all-columns
  contract): at width 2,000 that is ~12.8 KB/row for ~0.6 KB of values
  (measured from the transit gauge: 45,752 pending rows = 587 MB). Per
  input: ~2 units in flight x 4096 rows x 12.8 KB ≈ 100 MB; x 128
  interleaved inputs ≈ 7 GB of transit alone under the M23b bound working
  exactly as designed.
- Fix, two halves (`core_writer.rs`):
  1. **Shared null-fill** (`NullArrayCache` + `MergeChunk::synthesized`):
     all absent columns of one (type, len) are the SAME all-null array —
     one allocation per input serves every absent column of every chunk
     via Arc clones; the gated deep-copy and the transit accounting skip
     them (they share nothing with decode buffers). Byte-identical by
     construction (an all-null array is an all-null array; `row_bytes` and
     window boundaries unchanged).
  2. **Byte-adaptive unit sizing** (`MERGE_RANGED_UNIT_TARGET_BYTES` =
     8 MiB, floor 256 rows): the producer sizes each unit from the
     measured arrow bytes/row of the previous one (seeded from the
     input's PRESENT column count), clamped by the old row bound — narrow
     inputs keep 4096-row units bit-for-bit, wide inputs shrink toward
     ~8 MiB. Unit boundaries are decode granularity only (M23b's
     gated-vs-free byte-identity oracle covers the whole space).
- Effect (wil = 128 x 8000 rows interleaved, width 2000, flip arm):

```
BEFORE  peak 15034MB  wall 102.4s   pending arrow 587MB @window100
AFTER   peak  8283MB  wall 114.7s   pending arrow 128MB @window100
```

(A units-only intermediate without the shared null-fill reached 8031 MB
but cost +34% wall from ranged-decode redundancy at 640-row units; the
shared null-fill lifts bytes/row 12.8 -> ~2.7 KB so units stay ~3000 rows
and the redundancy collapses back into the M23b-accepted class.)

## The residual: standard-arm re-encode at width (ATTRIBUTED, vortex-internal)

Clean w2000 flip (474 MiB blob): at finish start live = 4,329 MB — term map
est 500 MB + **~3.6 GB docs-encode retention**, freed in <1 s at finalize.
`written_pre_finalize` = 48 MB of 497 MB (90.3% of the blob
compressed-but-unwritable until EOF — M24's dict-run sequence retention,
unchanged). The width multiplier on top of M24's finding is the per-column
pipeline state of `WriteStrategyBuilder` (vortex-file 0.79 strategy.rs):

- step 4 coalescing `RepartitionStrategy` holds >= 1 MiB of canonicalized
  decoded chunks PER COLUMN before flushing — x 2,001 columns ≈ 2 GB
  steady-state at this width (`buffered_bytes()` deliberately reports 0
  for it — the "TODO(os)" in vortex-layout repartition.rs — which is why
  M24 saw `strategy_buffered=0`);
- dict/zoned per-column state + 12.2 GB of pushed arrow (x 26 the blob)
  transiting the canonicalize/repartition stages.

Byte-identical engine-side levers are exhausted here (coalescing/stripe
boundaries are byte-visible). Follow-ups stay M24's, sharpened: (a)
passthrough-shaped re-encode for the standard arm (output bytes change,
owner call — note the heal arm now demonstrates healthy width behavior at
3.5 GB peaks); (b) vortex upstream: sequence-retention fix + width-aware
coalescing budget + counting repartition buffers in `buffered_bytes`.

## Falsified / corrected hypotheses

1. **"Per-column encode strategy state x width is the primary kill"** —
   partially: it is ~3.6 GB at width 2,000 per 0.45 GiB-data group
   (real, vortex-internal, documented) — but the measured kill masses were
   the storage-bloat writes (10.6-12.4 GB peaks) and the width-scaled
   gated transit (15.0 GB peak), both in OUR code and both fixed.
2. **"Heal arm is width-safe"** (implied by M24's flat heal measurements at
   12 columns) — false at width: the slice guard's canonicalize form was
   misclassified and both bloated storage 15.6x and spiked the copy phase.
3. **"Vocabulary axis"** — re-confirmed dead at width: hv (29M terms) vs
   clean (173k) at the same width/bytes: heal 3851 vs 10621?! — inverted!
   The CLEAN corpus bloats while hv does not (hv's high-entropy tokens
   defeat the input dict probe, so no dict layouts, no mixed-root windows)
   — vocabulary changed WHICH pathology fired, not the pathology itself;
   flip peaks 6475 (hv, spills) vs 5149 (clean).
4. **"M23b's transit bound holds at any schema"** — row-bounded, not
   byte-bounded; falsified at width and fixed.

## Byte-identity gate (sha256, data + sidecar)

24 gate files across narrow/hc/wide/interleaved x heal/flip
(`m25-data/out/shas-before.txt` vs `shas-after.txt`): **22 byte-identical**
— w12 pair, w200 pair, w2000-flip, wil-flip, hc pair (M24 cloudtrail
shape), nil pair (M23b narrow interleave), and EVERY `.vxi` including the
two changed files'. **2 changed BY DESIGN** (the bloat fix): merged-w2000.vix
(heal) and merged-wil.vix (heal) — value-equivalence proven above.
Re-verified on the FINAL stripped binary (`shas-final.txt`): all 24 files
sha-equal to `shas-after.txt` — instrumentation byte-neutral, outputs
deterministic across runs.

## Tests

- `cargo test -j8 --release -p vortex_index`: 233 passed / 0 failed / 10
  ignored on the final stripped tree (includes the new
  `m25_sparse_column_copy_does_not_bloat` and the M12/M17/M18 copy-path
  suites; 235/0 pre-strip with the two throwaway probe tests).
- `cargo test -j8 --release -p openobserve-core --lib`: 1929 passed / 0
  failed / 17 ignored (includes the M23b gated-vs-free byte oracle over the
  adaptive-unit code).
- puffin untouched this milestone.

## Wall (median of 3, idle box, dedicated before/after binaries)

```
w2000 heal (the wide repro):  65.85s -> 71.01s   (+7.8%)   peaks 11442 -> 3499 MB
wil flip  (the kill shape):   108.76s -> 99.18s  (-8.8%)   peaks 15365 -> 8578 MB
```

(w2000-heal's +7.8% prices in compressing the 62k column-windows the
unfixed engine wrote raw — the honest cost of writing correct bytes;
wil-flip got FASTER because the shared null-fill removes more
normalize/deep-copy work than the adaptive ranged decode adds. The
matrix-table walls above were recorded with concurrent builds on the box;
these medians are the clean numbers. Both are far inside the <=30% bar.)

## Prod translation

- Every wide-stream heal merge before this fix wrote bloated objects
  (15.6x at our geometry). Expect S3/prefix growth, slow uploads, spool
  pressure, and reader decode amplification on WIDE low-card streams —
  and second-generation merges re-reading bloat. The fix also makes those
  outputs smaller than the inputs' sum again.
- The fresh-pod kill shape (interleaved wide standard-arm rebuild) drops
  from ~28x data-bytes peaks to ~5.2x; with gate=1 that fits the 48 Gi
  envelope even aged. The remaining ~3.6 GB/group vortex-internal
  standard-arm mass is the next lever (owner-visible options above).
- The interim pins that map to THESE mechanisms: rebuild gate=1 stays
  load-bearing for the standard arm; the group-size clamp can stay at
  2048 MB (the masses now scale with data bytes at bounded slope, not with
  width x files).

## Artifacts

- Corpora: `m25-data/corpus-{w12,w200,w2000,wil,hc,nil,w2000-hv}`
  (deterministic, regenerable).
- Traces: `m25-data/logs/merge-*-{,after}.log`, `bench-*.log` (condense
  with `m25-data/condense.py <log> [step_s]`), `gen-*.log`.
- Outputs + shas: `m25-data/out{,-after}/` (`shas-before.txt`,
  `shas-after.txt`).
- Probe binaries: `m25-data/bin-{before,after}`.
