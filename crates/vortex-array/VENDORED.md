# Vendored: vortex-array

Origin: crates.io `vortex-array` 0.79.0 (registry tarball, verbatim).
Vendored 2026-08-21 (M28) — wired through `[patch.crates-io]` in the workspace
`Cargo.toml`, so every vortex 0.79 crate in the graph resolves to this copy.

Local patches on top of 0.79.0:

1. `src/builders/dict/bytes.rs` — `BytesDictBuilder::{encode_value, encode_null}`:
   the FIRST entry of a fresh dictionary is always admitted, even when it alone
   violates `max_len`/`max_bytes`. Before this, a single value larger than
   `max_dict_bytes` (1MiB under the file writer's default `DictLayoutConstraints`)
   made every fresh builder refuse the chunk's leading value; the caller
   (`vortex-layout` `DictStreamState::encode`) closed the run and retried the
   identical remainder on another fresh builder — an infinite zero-progress loop
   allocating a codes buffer + values array + builder + 2 SequenceIds per
   iteration. That loop is the prod compactor's 35-50MB/s pod-lifetime live-heap
   floor (M27 heap profiler, report ranks 1/2/4/5/6; reproduced and
   stack-confirmed in `src/vortex_index/examples/m28_dict_leak.rs` with
   `M28_BIGVAL=1`). An oversized first entry instead closes the run on the NEXT
   value, so every input that previously completed encodes byte-identically
   (the guard only changes behavior in states that previously never terminated).

2. `src/builders/dict/primitive.rs` — `PrimitiveDictBuilder::{encode_value,
   encode_null}`: same first-entry admission for the degenerate
   `max_bytes < element width` case (computed `max_dict_len == 0`).

3. Regression tests: `builders::dict::test::oversized_first_value_is_admitted`,
   `builders::dict::test::zero_max_len_still_progresses`,
   `builders::dict::primitive::test::degenerate_constraints_still_progress`.

Running this crate's own suite needs the features the published tarball's tests
assume: `cargo test -p vortex-array --lib --features "_test-harness,table-display"`
(3029 passed / 1 ignored, all green with the patch as of vendoring).
`goldenfiles/dict.metadata` is committed here because the published tarball
ships an EMPTY goldenfiles/ dir (upstream packaging omission — pre-existing,
also empty in the pristine registry copy); it was regenerated with
`UPDATE_GOLDENFILES=1` from the UNPATCHED Dict-array metadata code path and
now pins metadata stability for future local patches.

Keep this file current when patching further.
