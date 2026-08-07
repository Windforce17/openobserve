# Vendored: datafusion-functions-json

Origin: https://github.com/openobserve/datafusion-functions-json
Base rev: 0df53d710425cc91cf109e77a050bbe1e374e4fe (v0.54.2)
Vendored into the engine tree 2026-08-07 (the sibling-checkout path dep did
not survive the box move; a load-bearing local patch was nearly lost).

Local patches on top of the base rev:
1. json_get_int / json_get_float: `Peek::Minus` falls through to jiter's
   `known_int` / `known_number` so NEGATIVE numbers stored only in `_source`
   extract as values, not NULL. Pinned by the engine test
   `search::datafusion::vix_format::review_negative_numbers_from_source_extract_as_null`.

Keep this file current when patching further.
