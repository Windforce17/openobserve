# M28 — vortex dict-writer runaway: root cause + fix

## Mechanism (proven, not attributed)

The prod compactor "35-50MB/s pod-lifetime live-heap floor" is not a
retention leak — it is an **infinite zero-progress loop** in the vortex
dictionary writer, entered whenever a single string value larger than the
dict layout's `max_bytes` budget (1MiB under `DictLayoutConstraints::default`)
reaches a dict-encoded column:

1. The current `BytesDictBuilder` cannot admit the value
   (`dict_bytes + len + 16 > max_bytes` in `encode_value`) → the run closes,
   the remainder starts AT the oversized value.
2. `vortex-layout` `DictStreamState::encode` starts a FRESH builder
   (`start_encoding`) — which refuses the same leading value for the same
   reason → `encoder.encode` encodes **0 rows** → `remainder(chunk, 0)`
   returns the WHOLE chunk → `EncodingState::Done` with an empty codes array
   and an empty dictionary → `to_be_encoded = Some(unencoded)` → repeat,
   forever.
3. Every iteration allocates: a codes `BufferMut` (chunk-len capacity), a
   `PrimitiveArray` shell, an empty `VarBinView` values array, a fresh
   builder (views/values `BufferMut`s), and 2 `SequenceId`s — all pushed
   into the loop's `res: Vec<DictionaryChunk>`, which itself doubles into a
   near-GB allocation. One stuck write spins one core and climbs live heap
   at ~50-280MB/s until the pod OOMs (~20min). The write never finishes, the
   job lease expires, another pod re-claims the same data and gets stuck on
   the same value → the fleet-wide "floor".

Exact retaining path (all frames verified live by gdb on the repro, matching
the M27 prod stacks r1/r2/r4/r5/r6 frame-for-frame):
`BytesDictBuilder<u16>::encode_value <- encode_varbinview <-
DictEncoder::encode <- vortex_layout::layouts::dict::writer::encode_chunk <-
DictStreamState::encode <- dict_encode_stream <-
DictionaryTransformer::poll_next <- DictStrategy::write_stream
(child_layouts stream! -> buffered -> try_collect)`.

Every M27 canary report family is explained by the loop, including the ones
the brief could not place: rank9 (ONE ~896MB allocation inside
`DictStreamState::encode`) = the `res` Vec's doubling; ranks 11/14
(`SequenceId::new` / `SequencePointer::advance`, ~10M live tiny ids) = the 2
labeler ids per iteration; ranks 5/8/13 = the per-iteration fresh builder;
"avg 1.9-4.9KB codes buffers, identical size, monotonic count" = the SAME
stuck chunk's codes capacity re-allocated per iteration.

### Where prod enters the loop

The compactor writes through `vortex-layout DictStrategy` only where
`WriteStrategyBuilder`-derived strategies run with dict probing: the **docs
blob of `docs_passthrough=false` VixWriter builds** — in practice the
**SEGMENT BUILD job's L0 `.vix` builds** (`openobserve_jobs::job::segments`
→ `core_writer::write_core_file_from_sorted_batch`, running continuously on
prod compactors: canary showed ~3 traces builds/sec) and the rare
standard-rebuild merges (19 restarts/6h fleet-wide). Every routine merge is
docs-passthrough (verified in prod logs: 100% of "merged N core files" lines
carry `docs_passthrough: N`) and never runs the dict writer — which is why
the M26 job-loop harness (merge-only) measured just 0.004-0.027MB/job of
unrelated noise and every merge-shaped repro was clean. The trigger datum is
a **>1MiB string value** (oversized trace attribute / log line) in a
dict-probed column of a segment WAL bucket. The same loop is reachable from
INGESTER move builds (`write_core_file_from_tables`, same docs strategy) and
the search-side `FileFormat::Vortex` writer — the vendored-crate fix covers
every path at once.

## Fix (root, in vendored crates via `[patch.crates-io]`)

- `crates/vortex-array` (0.79.0 + patch): `BytesDictBuilder` and
  `PrimitiveDictBuilder` `encode_value`/`encode_null` now **always admit the
  FIRST entry of a fresh dictionary**, even when it alone violates
  `max_len`/`max_bytes`. The oversized entry closes the run on the next
  value (a 1-entry dictionary run). Progress is guaranteed; the guard's
  condition differs from the old one ONLY in states that previously looped
  forever, so every input that previously completed encodes
  **byte-identically**.
- `crates/vortex-layout` (0.79.0 + patch): zero-progress guard in
  `DictStreamState::encode` — a fresh-encoder `Done` with empty codes AND
  empty dictionary now fails the write with a loud error instead of hanging
  the process (unreachable after the vortex-array fix; insurance against
  regression).

Engine code is untouched (repro examples + harness corpus knobs only). Term
indexing and dictionary encoding stay ON; no budgets/limits added.

## Repro / proof

Unfixed tree (base 081bd7b57d), `M28_BIGVAL=1` plants one 1MiB+64KiB value:
- vortex level (`vortex_index/examples/m28_dict_leak.rs`, real
  `WriteStrategyBuilder::default()` write): **hangs forever**; RSS
  9.4GB→11.1GB in 6s (**~280MB/s**), one core pinned; gdb stack = the prod
  stack above. Without the big value: completes, `per_file=0.000MB` in every
  runtime shape (blocking/pool/pool-shared/async-static/4x-concurrent).
- engine level (`core/examples/m28_segbuild_leak.rs`, the real
  `write_core_file_from_sorted_batch` segment-build call, traces shape):
  warmup build **never completes** (45s timeout; 2.5s normal).
- prod (M27 canary, live 14:47-14:49Z): rank1 codes stack 22.4→26.0→29.2GB
  (~55MB/s), `live_est_total` 48.5GB at death, counts monotonic — the same
  loop.

Fixed tree:
- both repros complete; `per_file/per_build = 0.000MB`; the oversized value
  round-trips (`M28_VERIFY=1`: 131072 rows scanned, `max_value_len=1114112`).
- M26 harness, 320 interleaved high-cardinality jobs end-to-end (fixed
  tree): `per_job = 0.004MB` — the documented M26 noise floor (unfixed tree,
  same corpus, 40 jobs: 0.027MB/job, i.e. fixed one-time costs amortizing;
  the bug never manifests as a finite per-job residual — it manifests as a
  stuck write allocating forever, which merge-only job loops never enter).

## Byte identity

Fixed-seed 10-build corpus (no oversized values), unfixed vs fixed trees:
**all 20 outputs (10 `.vix` + 10 `.vxi`) sha256-identical**
(combined sha256-of-sha256s `ab5d3af99c30…`; per-file list in the repo
history is unnecessary — regenerate with
`M28_DUMP=<dir> m28_segbuild_leak 10 131072 traces`).
Inputs containing oversized values have no "before" bytes (the write never
terminated); their "after" files are proven readable (round-trip above).

## Performance

`m28_segbuild_leak 10 131072 traces` wall, median of 3:
unfixed 3.77s (3.75/3.77/4.26) vs fixed 3.60s (3.57/3.60/3.64) — within
noise (the patch adds two `is_empty`/`>0` checks on the NEW-dictionary-entry
path only; the repeated-value hot path is untouched).

## Gates

- vendored vortex-array suite: `cargo test -p vortex-array --lib --features
  "_test-harness,table-display"` — **3029 passed / 0 failed / 1 ignored** (includes 3 new regression tests;
  the tarball ships an empty goldenfiles/ dir — regenerated, see VENDORED.md).
- vendored vortex-layout, touched surface: `cargo test --lib layouts::dict`
  — 12 passed / 0 failed, deterministic (includes the new
  `oversized_value_completes_instead_of_looping`; on an UNFIXED vortex-array
  that test fails via the new zero-progress error instead of hanging —
  differential verified). The full standalone suite carries a PRE-EXISTING
  flaky reader/table family ("Runtime dropped task without completing it",
  4-10 varying failures) that reproduces identically against the pristine
  crates.io vortex-array — see crates/vortex-layout/VENDORED.md.
- engine unit suite most coupled to the patch: `cargo test -p vortex_index
  --lib` — 233 passed / 0 failed.
- `cargo build --workspace --release`: exit 0.
- integration, both segment modes: `ZO_INGEST_SEGMENT_MODE=true cargo test
  --test integration_test -- --ignored` → e2e_test ok (76.4s), EXIT=0; same
  without the env var → e2e_test ok (41.7s), EXIT=0.

## Pins this unlocks / contract notes

- `ZO_VIX_REBUILD_CONCURRENCY=1` and `ZO_COMPACT_MAX_FILE_SIZE=1024` bound
  quantities that INCLUDED stuck-loop allocation whenever a rebuild hit an
  oversized value. Their contracts do not change (they bound real in-flight
  merge memory), but the unbounded term they were absorbing is gone; they
  can be revisited on their own merits after the fleet slope confirms flat.
- The M27 profiler stays inert-by-default and proved decisive; keep it in
  the image.
- Segment WAL note: >1MiB values still reach dict-probed columns despite the
  M20b ingest clamp — the writer is now robust to them regardless; if the
  clamp was expected to bound these, that gap is a separate follow-up.
