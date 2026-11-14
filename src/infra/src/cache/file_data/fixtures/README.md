# Segment integrity fixtures

`segment-v1.bin` (250 bytes) and `segment-v2.bin` (1206 bytes) are complete WAL
segments produced by the real `segment_wal::format::encode_segment_version`
encoder and checked by its decoder. Each contains two streams with one two-row
Int64 Arrow batch each. The startup integrity tests check both formats, every
truncated prefix, framing, and CRC failures without adding a dependency from
`infra` back to `segment_wal` (which already depends on `infra`).

Regenerate from the repository root with the documented example:

```sh
cargo run -p segment_wal --example segpool_fixtures --offline -- \
  src/infra/src/cache/file_data/fixtures
```

Use the environment's required Cargo lock and build wrapper when applicable.
The generator uses fixed inputs and verifies each encoded segment before writing.
Any segment format change must update the startup validator and these fixtures
in the same change.
