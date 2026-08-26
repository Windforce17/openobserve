# M23 — compactor rebuild OOM: root cause, repro, fix

Worktree pinned at `b1d073a7e2` (.112 engine). All runs on the 20C/38G box,
release build, mimalloc (prod allocator) with a counting wrapper that reports
LIVE allocated bytes next to VmRSS every 500 ms.

Local commits (worktree only, nothing pushed):
`440dfc569a` repro harness + instrumentation, `83b6316681` the fix, then this
note.

## Verdict

**Found.** The accumulator is NOT the term map, NOT the finish blob assembly,
NOT allocator retention. It is the rebuild scan's **upfront-spawned per-input
decode streams**: `stream_merge_windows` spawned one decode thread per input
*before consuming anything*, and every thread immediately decodes its whole
file into its channel. On a concatenation-shaped merge order (disjoint inputs
— the dominant compaction group shape) inputs are consumed strictly one at a
time, so every *not-yet-reached* input sits fully decoded in RAM for the
whole scan. For a group of many small L0 files that is **the entire group
decoded at once ≈ 3.3x the group's original bytes**, filling at aggregate
decode speed and freeing only as the (much slower, tokenizing) scan consumes
each input. Production: hundreds of small index-off L0 logs files per 4 GB
group ⇒ a multi-GB linear climb at decode speed ⇒ OOM mid-fill.

- Accumulator: `src/core/src/vix/core_writer.rs`, `stream_merge_windows`
  (pre-fix lines 2746–2755: eager `InputCursor` spawn for ALL inputs), fed by
  `spawn_input_stream`/`stream_input_chunks` (lines ~2523–2580): per input, a
  scoped thread opens `VixDocs`, scans the whole docs blob and pushes decoded
  `MergeChunk`s through a `sync_channel(2)`.
- Why the channel bound does not bound it: the bound is per input
  (2 chunks + 1 in flight + scan internals ≈ 40–80 MB for big files), but a
  SMALL file (≤ ~2–3 docs chunks, i.e. ≤ ~48 MB original) fits **entirely**
  inside that allowance — so with N small inputs the resident set is
  N x whole-file-decoded = the whole group.
- Fix (implemented, 1 function, no knobs): spawn each input's decode stream
  lazily, on the first row the merge order actually needs from it.
  D-corpus peak drops **11065 MB → 4320 MB** (details below), wall time
  unchanged, byte-identical outputs (sha256).

## Repro

Everything lives in `src/core/examples/m23_rss_repro.rs` (dev-only example;
`mimalloc` added to `src/core` dev-dependencies only). It re-execs itself to
pin env (`ZO_VIX_L0_INDEX_OFF_STREAM_TYPES=logs` for gen,
`ZO_VIX_PLIST_MIN_DOCS=8192` + scratch `ZO_DATA_DIR` for merge — the prod
pins).

```
cargo build -j8 --release -p openobserve-core --example m23_rss_repro

# corpus: INDEX-OFF logs files via the REAL move builder
# (write_core_file_from_tables): `message` = 20-60 zipf-sampled words from a
# 50k vocab + low-cardinality fields (level/service/pod/env/code); _source
# synthesized by the builder. Disjoint DESC time ranges.
target/release/examples/m23_rss_repro gen m23-data/corpus    18  200000   # A: 18 x 87MB-orig  (1.56 GiB)
target/release/examples/m23_rss_repro gen m23-data/corpus-b 128   28125   # B: 128 x 12MB-orig (same 1.56 GiB, same 3.6M rows)
target/release/examples/m23_rss_repro gen m23-data/corpus-d 256   28125   # D: 256 x 12MB-orig (3.11 GiB, 7.2M rows)

# merge: the compactor's exact public entry — merge_core_files(Logs,
# fts=["message"]) over ranged file sources. Index-off inputs vs the indexed
# logs plan fire the prod WARN "index merge not applicable, rebuilding terms
# from _source" and land in rebuild_over_sources (heal-passthrough arm:
# derive_from_columns=true, docs chunks copied, index from the decoded scan).
target/release/examples/m23_rss_repro merge m23-data/corpus    m23-data/out/merged-a.vix
target/release/examples/m23_rss_repro merge m23-data/corpus-b  m23-data/out/merged-b.vix
target/release/examples/m23_rss_repro merge m23-data/corpus-d  m23-data/out/merged-d-before.vix
```

## Measured numbers (before fix)

```
run A: 18 x 87MB inputs,  1.56 GiB original, 3.6M rows -> scan-phase peak 1877 MB,  VmHWM 2540 MB  (1.6x original)
run B: 128 x 12MB inputs, 1.56 GiB original, 3.6M rows -> scan-phase peak 5726 MB,  VmHWM 5726 MB  (3.7x original)
run D: 256 x 12MB inputs, 3.11 GiB original, 7.2M rows -> scan-phase peak 11065 MB, VmHWM 11065 MB (3.6x original)
```

Same bytes, same rows, same output (A vs B — outputs identical modulo chunk
boundaries; 50245 terms, 157.5 MiB index both) — only the FILE COUNT changes
the peak, and it scales linearly in N at fixed file size (B→D: 2x files+bytes
⇒ 5726→11065 MB). That kills every "proportional to term derivation volume /
input bytes" framing: the driver is `min(file_decoded, ~3 chunks) x N`, which
for small files degenerates to *whole group decoded*.

### Condensed trace, run D before fix (256 inputs, 3.11 GiB original)

```
[phase] merge start: 256 ranged inputs / 1710.8 MiB data, fts=["message"], plist_min_docs=8192
[WARN]  merge_core_files: index merge not applicable, rebuilding terms from _source: ...
[rss] t=0.0s  vmrss=15MB     live=0MB       <- process start
[rss] t=1.0s  vmrss=2299MB   live=1987MB    <- all 256 decode threads spawned; channels filling
[rss] t=1.5s  vmrss=5795MB   live=5399MB
[rss] t=2.0s  vmrss=9511MB   live=9101MB
[rss] t=2.5s  vmrss=11062MB  live=10525MB   <- FILL COMPLETE: whole group decoded resident
[rss] t=5.0s  vmrss=10840MB  live=10213MB   d_rss -125MB/s from here on
[rss] t=15.0s vmrss=9439MB   live=8984MB    (window 192, rows 1.6M/7.2M)
[rss] t=35.0s vmrss=6951MB   live=6479MB    (window 448, rows 3.7M/7.2M)
[rss] t=55.0s vmrss=4531MB   live=4100MB    (window 704, rows 5.8M/7.2M)
[rss] t=70.0s vmrss=2375MB   live=2217MB    <- scan done (70.2s, 879 windows)
m23: finish start: rows=7200000 resident_terms=50245 est_terms_bytes=1313557059 spill_runs=0
m23: index blobs built: terms=131296032B plist=197416577B dict=3079B dict_blocks=341914B
m23: index sidecar container assembled: 329059427 bytes
[phase] merge done: 74.49s  ... out 1709.1 MiB (7200000 rows, 50245 terms, index 313.8 MiB)
[phase] peak: vmhwm=11065MB live=627MB
```

Reading it:
- live-bytes tracks VmRSS within ~5 % through the whole climb ⇒ genuine live
  allocations, **not** mimalloc retention (retention is visible only at the
  end: RSS floor ~2.3 GB vs live 627 MB after everything drops).
- The climb happens **before the first window is pushed** and the trace then
  *declines* as inputs are consumed — on this box (20 fast cores, NVMe,
  ranged local files) the fill takes 2.5 s at ~3.7 GB/s. On the prod pod the
  same fill is served by fewer/slower cores through the cache ladder while
  competing with the tokenizing scan ⇒ the fill itself becomes the observed
  **linear ~65 MB/s multi-minute climb**, and the pod OOMs mid-fill.
- Prod arithmetic: 4 GB-original group of small L0 files at ~3.3x
  decoded-to-original ≈ 13 GB, plus prod rows also carry `_original`
  (roughly doubling per-row string bytes vs this corpus) and the ~1.5–2.5 GB
  term map ⇒ the measured +17.6 GB is exactly this mechanism's size class.
  It also explains why halving the group bytes did NOT rescue the fleet:
  the climb only halves (file count halves), it does not go away — and the
  compactor sat at a ~29 GB baseline.

## The accumulator, precisely

`src/core/src/vix/core_writer.rs`:

- `stream_merge_windows` (pre-fix, old lines 2746–2755): spawns
  `InputCursor::new(key, spawn_input_stream(...))` for **every** input before
  the window loop starts.
- `spawn_input_stream` (line ~2518): `sync_channel(2)` + scoped thread
  running `stream_input_chunks`, which opens the file's `VixDocs` and scans
  the ENTIRE docs blob immediately, pushing decoded+normalized `MergeChunk`s
  (each ≈ one 16 MiB-uncompressed docs chunk ⇒ ~13–17 MB of arrow + a
  `row_bytes: Vec<u32>`).
- The merge order for disjoint inputs is a concatenation
  (`contiguous_offsets` → input-by-input), so input k+1..N-1 buffer their
  full channels for the entire time inputs 0..k are being tokenized. Freed
  only at consumption. Resident transit = Σ over unconsumed inputs of
  min(whole file decoded, ~3 chunks).

Both rebuild arms go through this function (the standard rebuild's coupled
pushes AND the heal-passthrough's index-only scan — the arm prod takes), so
one fix covers both.

## Falsified hypotheses

- **(a) finish_output blob assembly**: measured at D scale — terms blob
  131 MB + plist 197 MB + dict ~0.3 MB + sidecar container copy 329 MB;
  the whole finish adds ≈ +1.3 GB for ≤ 1 s (A: HWM 2540 vs scan peak 1877).
  Real, bounded, linear in index size — not the 17.6 GB climber.
- **(b) per-window decode transit as "window" cost**: the per-window
  interleave is bounded (≤ 8192 rows) and transient; the unbounded part is
  the *idle-input* buffering, which is a spawn-policy bug, not a window-size
  bug.
- **(c) TermSpill bookkeeping undercount**: the map estimate is honest at
  this shape — est 1313 MB at finish (7.2 M rows, 50245 terms), never
  crossed the 1536 MB budget ⇒ 0 spill runs, and the post-drain live floor
  (~2.2 GB incl. map) matches. One real (small) accounting gap found while
  auditing: `index_key_terms` (src/vortex_index/src/writer.rs:2864–2875)
  appends key-term postings directly (`terms.entry(...).or_default()` +
  `postings.push(doc)`) WITHOUT touching `terms_bytes` — ≈ 4 B x rows x
  present-columns (~190 MB at D scale) invisible to the spill trigger.
  Worth fixing for spill-accuracy someday; irrelevant to this OOM.
- **(d) mimalloc retention**: live-bytes climbs in lockstep with RSS during
  the climb (10.5 GB live at the 11 GB peak) ⇒ the climb is live data.
  Retention only shows post-drain (RSS 2.3 GB vs live 0.6 GB at exit) —
  cosmetic here.
- **Terms/postings accumulation** (pre-ruled-out by the static audit, now
  measured): resident map ≈ 1.0–1.3 GB estimated at these scales, bounded by
  the spill budget; sawtooth-free and an order of magnitude too small.

## The fix (implemented)

`src/core/src/vix/core_writer.rs`, `stream_merge_windows` only — decode
streams spawn lazily on the first row the merge order needs from that input
(`Vec<Option<InputCursor>>` + `get_or_insert_with` at the staging site; the
staged-take treats `None` as "input contributes nothing to this window",
exactly like `staged == 0` before). ~15 changed lines, no new knobs, no
behavior change for genuinely interleaved k-way orders (those touch every
input within the first window and spawn everything immediately, as before).
Consumed inputs still free naturally; never-reached-yet inputs now hold
NOTHING (no thread, no channel, no decoded chunks, no open reader).

### Before/after, corpus D (256 x 12 MB, 3.11 GiB original, 7.2 M rows)

```
BEFORE  peak vmhwm=11065MB  scan: fill to 11.0GB in 2.5s, drain -125MB/s x 70s   merge wall 74.49s
AFTER   peak vmhwm=4320MB   scan: 0.6->2.2GB slow bounded growth (term map)      merge wall 75.96s
```

Condensed after-fix trace (measured):

```
[rss] t=0.0s  vmrss=13MB    live=0MB
[rss] t=5.0s  vmrss=600MB   live=509MB    <- only the ACTIVE input + writer state (was 10840MB here)
[rss] t=15.0s vmrss=846MB   live=745MB       (window 192; d_rss ~ +22MB/s = the TERM MAP growing)
[rss] t=35.0s vmrss=1351MB  live=1215MB      (window 448)
[rss] t=55.0s vmrss=1796MB  live=1738MB      (window 704)
[rss] t=70.0s vmrss=2122MB  live=2048MB   <- scan done 73.86s (was 70.2s)
m23: finish start: resident_terms=50245 est_terms_bytes=1313557059 spill_runs=0
m23: index blobs built: terms=131296032B plist=197416577B dict=3079B dict_blocks=341914B
m23: index sidecar container assembled: 329059427 bytes
[rss] t=75.0s vmrss=3440MB  live=3226MB   <- finish blob assembly spike
[phase] merge done: 75.96s (was 74.49s)   out identical: 1709.1 MiB, 50245 terms, index 313.8 MiB
[phase] peak: vmhwm=4320MB live=627MB
```

- Peak 11065 MB → 4320 MB (−61 %); scan-phase resident 11.0 GB → 2.2 GB
  (−80 %). The remaining profile is exactly the bounded parts: the term map
  (visible as the steady +22 MB/s slope, est 1313 MB at finish — just under
  the 1536 MB spill budget, so 0 runs) + active-input transit + the finish
  blob-assembly spike (+2.1 GB for ~2 s: terms 131 MB + plist 197 MB blobs,
  their arrow/encode intermediates, and the 329 MB sidecar container copy).
- Wall time unchanged (75.96 s vs 74.49 s, ~+2 % = box noise): the active
  input's decode always outruns the tokenizing consumer, so the lost
  "pre-decode every file ahead of time" pipelining was pure ballast.
- Outputs BYTE-IDENTICAL to before-fix — sha256 equal on both corpora:
  D `.vix e344a7b7…`, `.vxi f2fc969d…`; A `.vix 6a21d5b1…`, `.vxi a5211a08…`.
- Corpus A regression check (18 big inputs): peak 2540 MB → 2492 MB, wall
  34.50 s → 37.54 s (box variance) — effectively unchanged, as expected:
  big files never fit inside the channel allowance, so their idle buffering
  was already capped at ~2–3 chunks each.
- Unit tests: `cargo test -j8 --release -p openobserve-core --lib
  vix::core_writer` — 63 passed / 0 failed (3 ignored benches). This covers
  the genuinely INTERLEAVED k-way window orders and the
  rebuild-vs-fast-path differential oracles, i.e. the lazy-spawn arm my
  disjoint corpora don't exercise.

At prod scale this converts "+17.6 GB during one rebuild" into the bounded
"term map + finish + one active input" profile (a few GB), independent of
the group's file count.

## Follow-ups (same bug shape, NOT fixed here)

- `stream_inputs_disjoint` (core_writer.rs ~2953): the fast-path merge's
  decode arm spawns cursors for every non-passthrough input upfront and
  drains input-by-input — same pathology whenever many inputs take the
  decode path (e.g. index-off plans over many small files with a
  disqualified passthrough). Same lazy-spawn shape applies.
- `rebuild_with_docs_passthrough` phase 2 (core_writer.rs ~3657): fail-open
  inputs' decode streams spawn upfront; rare (fail-open only), same shape.
- `index_key_terms` postings bypass `terms_bytes` accounting (see (c)).

## Repro artifacts

- Harness: `src/core/examples/m23_rss_repro.rs` (+ mimalloc dev-dep in
  `src/core/Cargo.toml`).
- Worktree-local instrumentation (kept, `m23:` log markers): spill-run
  logging in `src/vortex_index/src/spill.rs::write_run` /
  `writer.rs::maybe_spill_terms`, finish-phase markers in
  `writer.rs::finish_inner`/`assemble_index_blobs`, window progress in
  `core_writer.rs::stream_merge_windows`.
- Full traces: `m23-data/logs/merge-{a,b,d-before,d-after,a-after}.log`
  (condense with `m23-data/condense_trace.py <log> [step_s]`).
