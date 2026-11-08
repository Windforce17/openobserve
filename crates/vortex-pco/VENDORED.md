# Vendored: vortex-pco

Origin: https://github.com/vortex-data/vortex rev a13851be9bd35a53dbb6886b285997673461dfb0
(PR #10309 `ji/datafusion-56`). Workspace member `encodings/pco`, package
version `0.1.0`.

Vendored 2026-11-08 by `.work/up56/scripts/vendor_vortex.py` (workspace
inheritance resolved, intra-Vortex path dependencies rewritten to the pinned
git source) and wired in through
`[patch."https://github.com/vortex-data/vortex"]`. Benches are not copied.

Local patches on top of the pinned rev:

1. `src/array.rs` — `PcoData::metadata()` (new public getter) and
   `PcoData::unsliced_n_rows()` (visibility `pub(crate)` -> `pub`). The engine's
   PCO decode-cost bound (`src/vortex_index/src/conversion.rs`, `pco_bound`)
   reads the chunk/page metadata and the unsliced row count before decoding.
   No behavior change.

2. `src/lib.rs` — read-only `PcoMetadata::{header,chunks}`,
   `PcoChunkInfo::pages`, and `PcoPageInfo::n_values` accessors for the same
   pre-decode memory bound. Borrowed slices/scalar only; no allocation,
   mutation, or serialization change.

Keep this file current when patching further.
