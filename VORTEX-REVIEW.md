# Vortex 0.79 capability review — unused query-speed levers (2026-07-29)

Owner asked: "what else may speed up query for vortex we can use? full
review." Sources: crate source in ~/.cargo/registry (vortex-file,
vortex-layout ScanBuilder, vortex-datafusion, encoding crates) + our
usage (container.rs scan_blob: projection + RowSelection ONLY today).

## Ranked opportunities

1. **Filter pushdown w/ built-in stats pruning** — `ScanBuilder::
   with_filter(Expression)`; the layout scan runs `pruning_evaluation`
   internally (per-chunk stats) AND evaluates predicates on compressed
   data before decode. We never pass filters. Wins: duration_range
   (ACTIVE .35 stages B/C), plus every residual scan leg with a
   non-extractable conjunct (length(f)>N stays out; numeric/equality
   conjuncts go in). Effort: medium (arrow expr → vortex Expression
   mapping for simple conjuncts).
2. **File-level column statistics** — `FileStatsLayoutReader`/
   `FileStatistics` (vortex-file v2): per-column file min/max WITHOUT
   reading data. `duration > 5s` skips WHOLE FILES whose file-max is
   below threshold (most files, if durations cluster low) before any
   chunk work. Complements file_list's _timestamp-only pruning with
   every-column pruning. Effort: small once stage A lands; huge for
   selective numeric predicates.
3. **with_limit / with_ordered** — vortex-side limit stops decode inside
   a file. Our SimpleSelect early-stops across files but decodes full
   chunks within one; rows are _timestamp DESC so `with_limit(51)` +
   ordered gives intra-file early exit. Effort: small; helps select51
   cold and any LIMIT-shaped scan.
4. **with_split_by + with_concurrency** — intra-file parallel decode.
   We parallelize across files; a single 4GB merged file decodes on one
   task today. Matters for scan legs over few-big-files hours (post-4096
   compaction). Effort: small (plumb a concurrency hint).
5. **vortex-datafusion crate** — native TableProvider/ExecutionPlan with
   maintained pushdown (filters/projection/limit through DataFusion
   directly). Strategic: could replace much of our vix_format glue and
   inherit upstream pushdown improvements. Effort: large; treat as a
   refactor milestone, not a quick win.
6. **Encoding audit** — compressed_strategy(0) uses the btrblocks-style
   auto-compressor; verify _source lands on FSST (substring-friendly,
   smaller fetch bytes → body_substr class), floats on ALP, low-card
   strings on dict. If _source is NOT FSST today, flipping it shrinks
   the dominant scan bytes. Effort: inspection first (one layout dump),
   then a strategy tweak if needed.
7. **with_row_range** — contiguous-wave reads for select-star waves
   without materializing selection buffers. Micro; only if profiles show
   selection-buffer overhead.

## Explicitly checked and already used
Projection pushdown; RowSelection point/range reads; chunk-granular
ranged fetch (.29); per-chunk ts zone maps (our own PROP_ZONE_MAP);
encode-side multi-thread pool.

## Order of attack
.35 = stages A/B/C (items 1+2 for numeric conjuncts). Then item 3+4
(small, measurable on select/scan classes). Item 6 inspection alongside.
Item 5 parked until the engine stabilizes.
