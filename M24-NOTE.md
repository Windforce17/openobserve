# M24 — vocabulary-scaled rebuild memory: repro, measurements, fix, and the corrected kill model

Worktree pinned at `06e87096ff` (post-M23b engine, `.114`-era). 20C/38G box,
release builds (`-j8`), mimalloc + the M23 live-bytes counting wrapper
(`[rss]` sampler: VmRSS/VmHWM/live every 500 ms). All scratch under
`m24-data/` in this worktree. Local commits only, nothing pushed:
`2f3db48a3d` harness, `05a9dfd6c6` fix (+worktree instrumentation),
`bfadb6ec81` instrumentation strip, then this note. Byte-identity was
re-verified on the final stripped binary (hc-heal rerun: sha256 equal,
peak 2513 MB — run variance around the 2378 MB primary after-run; the
peak region is now the scan-phase term map, which is noise-sensitive).

## Verdict (read this first)

The kill model's central hypothesis — "a single rebuild of a
high-cardinality group reaches ~35 GB because writer term memory scales
with vocabulary" — is **falsified at equal bytes**: on a 26M-distinct-term
cloudtrail-shaped corpus the heal-arm rebuild peaks at **2670 MB vs
2343 MB** for the 50k-term corpus of the same size (**1.14x, not >3x**).
The existing spill machinery already bounds the term map (the estimate
*overcounts* — it spills early), and the k-way finish streams.

What actually scales:

1. **Vocabulary-scaled (bounded here, unbounded at gen-1 scales): the
   FINISH-phase blob stacking** — the spilled finish held ~3x the index
   size in RAM (sink accumulators + one-shot terms encode + all blobs +
   the container copy of all of them). At this corpus: ~2.6 GB of transient
   stacking for an 832 MiB index. At a gen-1 sweep's 100M+ terms it is
   10-20 GB. **FIXED** (spooled assembly, byte-identical): finish now peaks
   at ~ONE container copy.
2. **Group-bytes-scaled (NOT vocabulary; the real kill-mass at prod
   geometry): the STANDARD-arm docs re-encode transit inside vortex 0.79.**
   The coupled rebuild's docs encoder retains ~the whole file (compressed
   segments + pinned buffers) until finalize — transit ≈ 2.3 GB on the
   0.85 GB-data lc group and ≈ 2.8 GB on the 1.2 GB-data hc group
   (∝ data bytes; insensitive to term count: 50k vs 26M terms), freed in
   <1 s at `signal_finish`. The heal/passthrough arm streams flat. This is vortex-internal scheduling (details below), not
   fixable byte-identically from our side; attributed + follow-up options
   listed. It is also exactly why the pinned L0 build budget "worked" for
   ingest drains but never saved the compactors: the budget governs move
   builds, not the merge writer's re-encode.

## Repro

Everything in `src/core/examples/m23_rss_repro.rs` (extends the M23/M23b
harness; env pinned by re-exec; scratch default `m24-data/`).

```
cargo build -j8 --release -p openobserve-core --example m23_rss_repro

# high-cardinality cloudtrail-shaped corpus: 24 x 120k rows, ~1.77 GiB
# original / 1.17 GiB data — eventid/requestid (uuid4-shaped, unique per
# row), useridentity_arn (unique session suffix), near-unique source ip,
# message = zipf 50k vocab + ~5-6 unique hex16 tokens per row
# => 25,966,089 distinct terms (vs 50,245 in the lc corpus)
target/release/examples/m23_rss_repro gen-hc m24-data/corpus-hc 24 120000

# low-cardinality reference at equal bytes (M23's builder): 24 x 150k rows,
# ~1.56 GiB original / 0.83 GiB data, 50k vocab
target/release/examples/m23_rss_repro gen    m24-data/corpus-lc 24 150000
target/release/examples/m23_rss_repro gen-il m24-data/corpus-il 24 150000  # interleaved twin

# the two rebuild arms per corpus (all index-off inputs -> "rebuilding
# terms from _source"):
#   merge / merge-hc          -> heal-passthrough arm (docs chunks copied,
#                                index from the decoded scan) — M23's arm
#   merge-flip / merge-hc-flip -> type flip (code/httpstatus i64->utf8)
#                                disqualifies every input -> STANDARD arm
#                                (decode + coupled docs re-encode)
target/release/examples/m23_rss_repro merge-hc      m24-data/corpus-hc m24-data/out/merged-hc.vix
target/release/examples/m23_rss_repro merge-hc-flip m24-data/corpus-hc m24-data/out/merged-hcflip.vix
...

# Part B floor ratchet: n sequential merges in one process
target/release/examples/m23_rss_repro soak m24-data/corpus-lc m24-data/out/soak 10
MIMALLOC_PURGE_DELAY=0 target/release/examples/m23_rss_repro soak m24-data/corpus-lc m24-data/out/soakp 4
```

## Measured baselines (unfixed engine 06e87096ff, VmHWM)

```
corpus            arm       distinct terms  index    wall    peak RSS
lc (1.56GiB orig) heal      50,245          157.5MiB 34.5s   2343MB
lc                standard  50,245          157.5MiB 37.9s   3491MB   <- +1.1GB = docs re-encode transit
hc (1.77GiB orig) heal      25,966,089      832.0MiB 69.6s   2670MB   <- vocabulary 517x, peak +14%
hc                standard  25,966,089      832.0MiB 72.5s   4621MB   <- transit again (+2GB)
```

- hc/lc heal-arm ratio at equal bytes: **1.14x** — the charter's ">3x
  explosion" does not exist on this arm; spill + k-way streaming already
  bound the vocabulary side. (Expectation falsified; see kill-model
  section for where the prod mass actually is.)
- The +1.1-2.0 GB standard-arm delta is the vortex-held docs re-encode:
  live at "finish start" = 2951 MB (lc-flip) / 3367 MB (hc-flip) with the
  term map at 659 MB est / freshly-spilled respectively — then a 2.7-2.8 GB
  live DROP within ~0.5-1 s as the docs encoder finalizes.

### Spill-threshold honesty (charter suspect (b)) — measured at the trigger

hc corpus, both arms, `ZO_...` defaults (budget 1536 MB estimated):

```
spill run 0: entries=11,729,452  est=1617MB   live drop across drain: 2933->1610 = ~1.32GB
spill run 1: entries=11,730,693  est=1617MB   live drop across drain: 1695->305  = ~1.39GB
```

Real resident map ≈ **0.86x the estimate** at uuid/hex key mixes —
`PER_TERM_OVERHEAD = 88` OVERSHOOTS the true BTreeMap+alloc cost for
30-40-byte keys with 1-doc postings, so the map spills EARLY, never late.
The "estimate undercounts at millions of tiny keys" hypothesis is
falsified. One genuine gap existed and is now fixed: `index_key_terms`
appended key-term postings (~4 B x rows x present-columns; ~92 MB on this
corpus, hundreds of MB on wide prod schemas) without touching
`terms_bytes` — invisible to the trigger.

### Bloom-only sets / zone folders (charter suspect (c))

Fine, as suspected: no bloom-only fields exist on index-off rebuild inputs
(demotion is skipped when spilled, exactly the prod shape); zone/stats
folders are chunk-count-sized. The #48 composite bloom accumulates one u64
per distinct eligible-field term (~85 MB at 10.6M eligible terms here,
linear) — noted, not fixed: it is 8 B/term against the dictionary's ~35.

## The vocabulary-scaled accumulator that was real (and the fix)

`src/vortex_index/src/writer.rs::assemble_index_blobs`, spilled arm — the
finish used to stack, simultaneously:

1. the sink's accumulators (`TermSink`, writer.rs:4283 — fields
   `term_batches`/`dict_blocks`/`plist`): arrow term batches (~4 B
   doc_count + cell + offsets per term), `dict_blocks` (~every distinct
   key's bytes), `plist` — measured via the worktree sink gauge:
   **~982 MB buffered at 24M terms** on the unfixed run,
2. the one-shot `write_vortex_blob` terms encode (batches + encoded blob
   both live; terms blob 345 MB here),
3. every finished blob (`terms` 345 MB + `dict_blocks` 440 MB + `plist`
   46 MB + `dict` 7 MB + `bloom` 34 MB),
4. `build_container`'s copy of ALL of them into the sidecar Vec (872 MB).

Final-tree locations: spooled spilled arm
`writer.rs:3137 assemble_index_blobs` (the `Some(spilled)` arm),
`writer.rs:4207 RegionSink` / `4545 into_spooled_parts` / `4569
SpooledSinkParts`, `container.rs:1649 TermsBlobSpooler`, `container.rs:580
BlobPart` / `606 build_container_parts`, `puffin/src/writer.rs:135
add_blob_from`, key-term accounting `writer.rs:2847 index_key_terms`,
spill trigger `writer.rs:2448 maybe_spill_terms` +
`spill.rs:60 PER_TERM_OVERHEAD`.

Peak stack ≈ 3x index size — ~2.6 GB at this corpus's 832 MiB index,
10-20 GB at a gen-1 sweep's 100M+ terms. All of it vocabulary-proportional
and NOT bounded by the term-spill budget.

### Fix (byte-identical, no new knobs, spilled arm only)

- `TermSink::with_spool(dir)`: the `dict_blocks`/`plist` regions write
  through to UNLINKED temp files on the spill volume (`RegionSink`) —
  same bytes, bounded residence, OS-reclaimed on crash (nothing to sweep).
- `TermsBlobSpooler` (container.rs): closed term batches stream into an
  incremental vortex writer on a worker thread (bounded `sync_channel(2)`,
  the `DocsBlobEncoder` shape) writing the terms blob to a spool file.
  Byte-identical to the one-shot `write_vortex_blob`: same push sequence,
  same `addressable_strategy()` (flat uncompressed leaves make the bytes
  thread-count independent — the M17-pinned property). Verified: vortex's
  `CollectStrategy` (doc_count column) holds one latest sequence id, so
  postings segments flush with a ONE-chunk lag; `FLAT_LAYOUT_INLINE_ARRAY_NODE`
  is default-off (no per-segment buffer clones retained).
- `puffin::PuffinBytesWriter::add_blob_from(reader, len, ...)` +
  `build_container_parts`: spooled blobs stream into the sidecar
  container, so assembly holds ONE container copy instead of container +
  every blob. In-memory blobs (`dict`, `bloom`, every unspilled build)
  take the exact `add_blob` path (byte-equality unit-tested).
- `index_key_terms` accounts its postings into `terms_bytes` (the M23
  follow-up (c) gap). Scope note: this function serves the COLUMN-driven
  derivations (move-job builds, #46 arms). None of the merge runs here
  took that arm (every one logged the source-driven rebuild, where key
  terms were always accounted via `push_term`) — the fix is exercised by
  the move-build path and by the forced-spill unit test, and its est
  effect on these merge traces is correctly zero.
- Unspilled paths (move builds, small merges, the M17 parallel recut) are
  bit-for-bit untouched.

The spilled finish now holds: term-map remnant (spill-budget-bounded) +
one open 128 KB batch + the incremental vortex writer's bounded window +
the returned sidecar Vec (1x index — the container must still exist once
to be uploaded; spooling the sidecar end-to-end like `VixOutput` is the
listed follow-up).

### Before/after (VmHWM; sha256 identical in every cell)

```
                              BEFORE      AFTER       wall
hc heal  (26M terms, SPOOLED) 2670MB  ->  2378MB      69.6s -> 71.6s (+2.8%)
hc flip  (26M terms, SPOOLED) 4621MB  ->  4556MB      72.5s -> 72.9s (+0.7%)
lc heal  (50k, unspilled)     2343MB  ->  2559MB (*)  34.5s -> 36.8s
lc flip  (50k, unspilled)     3491MB  ->  3480MB      37.9s -> 36.7s
il heal  (interleaved)        2508MB  ->  2568MB (*)  35.5s -> 34.4s
il flip  (interleaved)        3623MB  ->  3720MB (*)  38.7s -> 39.4s
```

(*) unspilled runs execute bit-for-bit unchanged code on this path;
±100-220 MB and ±2 s are box/run variance (M23 saw the same class:
2540 vs 2492 across reruns of one binary).

The hc numbers understate the fix at prod scale because this corpus's
finish phases NO LONGER COINCIDE with a big co-tenant: the peaks moved to
the SCAN phase (the pre-spill term map on heal; the docs transit on flip).
The finish itself went from a ~2.6 GB stack (sink 982 MB + blobs 872 MB +
container 872 MB + encode transients; RSS 2.5-2.7 GB across t=63-69 on
hc-heal-before) to a FLAT profile: after-fix RSS at "index blobs built" =
434 MB, then one container copy (872 MB Vec) — at a gen-1 sweep's 100M+
terms that is the difference between 10-20 GB and ~1x index size.

Byte-identity (sha256, data + sidecar): all 6 config pairs equal —
`m24-data/out/shas-{before,after}.txt` (hc/lc x heal/flip) plus the
interleaved pair (il heal `2057c54f…`/`aa7b15c7…`, il flip
`09656b07…`/`a40a1683…` — identical before vs after; before-outputs
produced by a clean 06e87096ff engine build, so instrumentation is proven
byte-neutral too). Determinism double-checked by re-running hc-flip twice
on one binary: identical shas.

## The corrected kill model: the standard-arm docs re-encode transit

With the vocabulary side bounded, the residual — and the only
equal-bytes accumulator of kill-class size — is the coupled rebuild's
docs re-encode, i.e. the M23B-NOTE's un-attributed "writer-side
accumulator" (+55-70 MB/s, freed only at finish). Now attributed with
instrumentation INSIDE `encode_docs_stream` (container.rs:1017; the
docs-strategy writer behind `DocsBlobEncoder`):

```
hc-flip, pushes every 32 batches (m24 markers):
push 32:  pushed_bytes=330MB   written=6MB    strategy_buffered=0
push 128: pushed_bytes=1.32GB  written=68MB   strategy_buffered=0
push 224: pushed_bytes=2.32GB  written=105MB  strategy_buffered=0
push 352: pushed_bytes=3.63GB  written=151MB  strategy_buffered=0   <- scan done
[t=67.5s finish start]  live=3367MB
[t=68.0s]               live=569MB            <- 2.8GB freed in <=0.5s
```

- `strategy_buffered=0` throughout: the retention is NOT the documented
  repartition/dict/zoned buffers.
- `written` stuck at ~13% of the docs blob while 3.6 GB were pushed, then
  the whole blob hits the spool in <1 s: the segments were already
  compressed but **could not be written out** — vortex 0.79's default
  write pipeline (`WriteStrategyBuilder`: repartition -> zoned stats ->
  dict -> coalesce -> compress -> buffered -> chunked) writes segments in
  strict `SequenceId` order, and the `DictStrategy` run of any
  low-cardinality column (logs ALWAYS have them: level/env/service/...)
  holds an early-allocated sequence id until its dictionary VALUES
  finalize at column EOF (`dict/writer.rs`: `values_eof = eof.split_off()`
  at run start; low-card columns never hit the 64k-entry constraint, so
  ONE run spans the whole column). Every segment sequenced after it —
  nearly the whole file — queues in `sequence_id.collapse().await`
  (`BufferedSegmentSink`), pinning compressed buffers and their spawn-task
  state until finalize.
- The heal arm's `docs_passthrough_strategy` writes struct chunks
  chunk-major without the dict/zoned pipeline — measured flat (2343/2670
  peaks are map+finish, no transit component).
- Scale: held bytes ≈ 2.3-2.8 GB per 1.2 GB-data / ~1.6 GiB-original
  group, insensitive to vocabulary. At prod geometry (2048 MB-original
  groups, `_original` doubling row bytes) a standard-arm rebuild carries a
  ~4-7 GB transit + ~1.4-2 GB map + (pre-fix) ~1-3x-index finish. Fresh
  48 Gi pods die when this stacks with the L0 build waves (8192 MB budget
  pinned) and the aged floor — and gen-1-scale groups (M22 sweeps,
  metadata single-partition merges) multiply the transit linearly.
- Why the budget pins never helped the compactor: the M17 admission
  budget governs L0 BUILDS (move jobs — small files, so builds were
  always fine); the merge writer's re-encode never passed through it.

**Not fixed here** — the retention is inside vortex's write pipeline and
every byte-identical lever is scheduling-only (confirmed: throttling the
push side cannot help; the writer future only releases at EOF). Options
for the follow-up, in preference order:

1. Swap the STANDARD rebuild's docs encode to the passthrough-strategy
   shape (`docs_passthrough_strategy` + arrow pushes, exactly what heal
   fail-open re-encodes ship today): streams chunk-major, measured flat.
   OUTPUT BYTES CHANGE (valid, already-shipped encoding shape; no dict
   layout/zone stats on those columns) — owner-visible trade, needs its
   own acceptance.
2. Vortex upgrade once the sequencing/dict-run retention is fixed
   upstream (file an issue with the measurement above).
3. Keep the interim geometry knobs that actually bound the transit:
   rebuild gate=1 + group clamp 2048MB. These are the ONLY pins that map
   to this mechanism — the wave knobs (workers, DF cap, fetch-decode) can
   revert independently of it.

## Falsified hypotheses (with the measurements that killed them)

1. **">3x vocabulary explosion on the rebuild"** — heal arm hc/lc =
   2670/2343 MB (1.14x) at 517x the distinct terms, equal bytes.
2. **"Term-spill estimate undercounts at millions of tiny keys"** —
   real/est = 0.86 at the trigger (est 1617 MB, real ~1.39 GB); the 88-byte
   overhead overshoots for uuid-class keys; spills happen EARLY. (The one
   real accounting hole — key-term postings — is fixed.)
3. **"Bloom-only hash sets / zone folders are vocabulary risks"** — no
   bloom-only fields exist on this path; zone/stats are chunk-sized;
   composite bloom is 8 B/distinct-eligible-term (linear, small).
4. **"The M23b writer-side climber is term-derivation state"** — it is
   the docs re-encode transit: identical climb at 50k and 26M terms
   (∝ pushed docs bytes), `strategy_buffered=0`, freed at encoder
   finalize, absent on the heal arm.
5. **"mimalloc retention drives the in-merge climb"** — live tracks RSS
   within ~10% through every phase of every run here.

## Part B — the floor ratchet: attribution

10 sequential heal-arm merges of corpus-lc in ONE process (fixed engine),
inter-op floor sampled 2.5 s after each merge's outputs drop
(`m24-data/logs/soak-default.log`):

```
iter 0..9 floors: vmrss 2027-2075MB (flat, no trend)   vmhwm 2502-2505MB (constant)
live bytes at every floor: 0MB
```

**No in-process ratchet exists on the merge path.** All state is torn
down between ops (live 0), and the ~2.0 GB resident floor is mimalloc
RETAINING the peak's pages — reused perfectly by the next same-shape
merge (10 ops, zero growth; VmHWM never moved). "live flat while RSS
floor high" = allocator retention, per the charter's discriminator.

Purge probe (charter): `MIMALLOC_PURGE_DELAY=0`, 4 iterations
(`soak-purge0.log`):

```
floors: vmrss 45-57MB (was 2027-2075)    vmhwm 1855-1866MB (was 2502-2505)
wall: 39.6s cleanest iter (default mean 35.7s, ~+11%); iters 1-3 ran
      49-58s but overlapped a concurrent -j8 build — contaminated, upper
      bound only
```

Immediate purging returns essentially everything (and even trims the
in-run peak ~640 MB by returning freed pages mid-merge), at a measured
~+11% wall cost on this workload — real, NOT the charter's "zero
behavioral risk", so **nothing is implemented in-engine**. The remedy is
a deployment knob, not code: trial `MIMALLOC_PURGE_DELAY` (0, or a small
value like 250-1000 ms as a middle ground) on COMPACTORS via prod-ops —
batch-shaped pods where a ~10% merge-wall tax may be a fine price for a
~2 GB-per-idle-peak floor reduction; queriers should keep the default.

Prod translation of the staircase (2 GB fresh -> ~30 GB aged, live
observed 26 -> 42.7 GB): same-shape ops reuse retained pages perfectly
(measured), so a monotone floor climb requires NEW allocation shapes
whose peaks land in pages/size-classes the old retained set cannot
serve — the compactor's heterogeneous mix (L0 build waves at the 8-19 GB
admission envelope, DF query pools, rebuild transits, downloads) plus
long-lived PARKED pool threads whose per-thread heaps never run the
allocation slow paths that trigger mimalloc's deferred purging. The
floor therefore ratchets toward the SUM of per-shape historical maxima,
not a leak (a leak would show climbing live bytes; every measurement
here shows live returning to baseline).

## Tests

- `cargo test -j8 --release -p vortex_index`: 232 passed / 0 failed
  (+ 1 + doc-targets green). Includes `spilled_terms_are_byte_identical`,
  which now exercises the SPOOLED sink end-to-end (budget forced to 1
  byte) against the unspilled writer.
- `cargo test -j8 --release -p puffin`: 33 passed / 0 failed, including
  the new `add_blob_from` byte-equality + short-read tests.
- `cargo test -j8 --release -p openobserve-core --lib`: 1929 passed /
  0 failed / 17 ignored.

## Artifacts

- Corpora: `m24-data/corpus-{hc,lc,il}` (deterministic, regenerable).
- Traces: `m24-data/logs/merge-*-{before,after,attr}.log` (condense with
  `m24-data/condense.py <log> [step_s]`), `gen-*.log`, `soak-*.log`.
- Outputs + shas: `m24-data/out/` (`shas-before.txt`, `shas-after.txt`).
