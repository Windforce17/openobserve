# Vendored: vortex-layout

Origin: https://github.com/vortex-data/vortex rev a13851be9bd35a53dbb6886b285997673461dfb0
(PR #10309 `ji/datafusion-56`, DataFusion 56 / Arrow 60.1 preparation on top of
`develop`). Workspace member `vortex-layout`, package version `0.1.0`.

Vendored 2026-11-08 by `.work/up56/scripts/vendor_vortex.py`: package fields
and `workspace = true` dependencies are resolved from the Vortex workspace, and
intra-Vortex path dependencies are rewritten to the same pinned git source the
engine uses. The engine wires this copy in through
`[patch."https://github.com/vortex-data/vortex"]`, so every crate in the
Vortex graph resolves `vortex-layout` to this directory. Benches are not
copied.

Local patches on top of the pinned rev:

1. `src/layouts/dict/writer.rs` — `DictStreamState::encode`: zero-progress
   guard. When `start_encoding` returns `EncodingState::Done` with an empty
   codes array and an empty dictionary, a fresh encoder refused the chunk's
   leading value; retrying the identical remainder would loop forever (the
   M28 prod compactor live-heap runaway). The guard turns that state into a
   write error. Upstream's dictionary builders admit a fresh dictionary's
   first entry (vortex PR #10400), so the state is unreachable today; the
   guard is a hard stop against a future regression.

The former local regression test `oversized_value_completes_instead_of_looping`
is not carried: upstream now pins the same behavior with
`test_encode_value_larger_than_dictionary_budget_terminates` and
`test_encode_splits_oversized_value_into_its_own_dictionary` in the same
module.

The former vendored `vortex-array` (dictionary first-entry admission and
`MaskFuture::slice` whole-range sharing) is dropped: both are upstream
(vortex PRs #10400 and #9268).

Keep this file current when patching further.
