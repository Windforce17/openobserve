# Vendored: vortex-layout

Origin: crates.io `vortex-layout` 0.79.0 (registry tarball, verbatim).
Vendored 2026-08-21 (M28) — wired through `[patch.crates-io]` in the workspace
`Cargo.toml`, so every vortex 0.79 crate in the graph resolves to this copy.

Local patches on top of 0.79.0:

1. `src/layouts/dict/writer.rs` — `DictStreamState::encode`: zero-progress
   guard. When `start_encoding` returns `EncodingState::Done` with an EMPTY
   codes array AND an EMPTY dictionary, a fresh encoder refused the chunk's
   leading value; re-trying the identical remainder loops forever (the prod
   compactor live-heap runaway M28 root-caused — see
   `crates/vortex-array/VENDORED.md` for the root fix). The dict builders now
   always admit a fresh dictionary's first entry, so this state is unreachable;
   the guard turns any future zero-progress regression into a write ERROR
   instead of a silent allocation loop that hangs the process.

2. Regression test `layouts::dict::writer::tests::
   oversized_value_completes_instead_of_looping` — a chunk carrying a value
   larger than `max_bytes` must encode completely across multiple dictionary
   runs (hung forever before the vortex-array fix).

3. `Cargo.toml` (vendoring wiring only, no behavior change): dev-dependencies
   on `vortex-array` (`_test-harness`) and `vortex-io` (`tokio`) that the
   published tarball's own tests assume but publish stripped, plus a standalone
   `[patch.crates-io]` entry pointing `vortex-array` at the sibling vendored
   copy so this crate's suite runs against the patched builders when tested
   from inside `crates/vortex-layout` (it is excluded from the engine
   workspace).

Run the suite with: `cargo test -p vortex-layout --lib` (from this directory).

Keep this file current when patching further.
