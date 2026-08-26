// Copyright 2026 OpenObserve Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

//! M13 dispatch micro-bench (manual, never CI): the query classes
//! `query_bench.rs` deliberately cannot carry (its header pins it to
//! pre-#29 APIs for cross-tree drop-in). This file benches the CURRENT
//! collectors — the two sides of the top-k/distinct dispatch decision at
//! `vix/mod.rs` (dictionary-served vs docs-column) plus the SimpleSelect
//! wave path and the ranked-plist histogram — on a real corpus, so the
//! dispatch constant is a measured number, not a guess.
//!
//! ```text
//! O2_VIX_FILE=<file.vix> cargo test -p search --release \
//!   bench_dispatch_classes -- --ignored --nocapture
//! ```
//!
//! The sidecar is expected next to the data object (`.vxi`). Field names
//! default to the standard `merge_bench gen` corpus (`service_name` low
//! cardinality, `trace_id` high cardinality); override with
//! `O2_VIX_LOW_FIELD` / `O2_VIX_HIGH_FIELD`. Every class prints its result
//! summary alongside the timings (bench discipline: no timing without a
//! verifying output line), three timed runs each — take the median.

use arrow::buffer::BooleanBuffer;
use bytes::Bytes;
use vortex_index::{VixQuery, VixReader};

use super::collect;

fn timed<T>(label: &str, mut f: impl FnMut() -> T) -> T {
    let mut out = None;
    let mut times = Vec::with_capacity(3);
    for _ in 0..3 {
        let start = std::time::Instant::now();
        out = Some(f());
        times.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    let mut sorted = times.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    eprintln!(
        "{label}: median {:.3} ms (runs {:.3}/{:.3}/{:.3})",
        sorted[1], times[0], times[1], times[2]
    );
    out.unwrap()
}

fn all_set(len: usize) -> BooleanBuffer {
    BooleanBuffer::new_set(len)
}

#[test]
#[ignore = "manual M13 dispatch bench (set O2_VIX_FILE); medians of 3 printed per class"]
fn bench_dispatch_classes() {
    let Ok(path) = std::env::var("O2_VIX_FILE") else {
        eprintln!("O2_VIX_FILE not set; skipping");
        return;
    };
    let low = std::env::var("O2_VIX_LOW_FIELD").unwrap_or_else(|_| "service_name".to_string());
    let high = std::env::var("O2_VIX_HIGH_FIELD").unwrap_or_else(|_| "trace_id".to_string());
    let data = Bytes::from(std::fs::read(&path).expect("read vix file"));
    let index_path = path.trim_end_matches(".vix").to_string() + ".vxi";
    let index = std::fs::read(&index_path).ok().map(Bytes::from);
    eprintln!(
        "file={path} sidecar={}",
        if index.is_some() { &index_path } else { "NONE" }
    );
    let reader = VixReader::open_with_index(data, index).expect("open");
    let rows = reader.row_count();
    eprintln!("rows={rows}");

    // ── the dispatch probe itself (must be ~free) ─────────────────────────
    let low_distinct = timed(&format!("probe distinct[{low}]"), || {
        reader.field_distinct_string_terms(&low).unwrap()
    });
    let high_distinct = timed(&format!("probe distinct[{high}]"), || {
        reader.field_distinct_string_terms(&high).unwrap()
    });
    eprintln!(
        "distinct: {low}={low_distinct:?} {high}={high_distinct:?} rows={rows} \
         (ratios {:.6} / {:.6})",
        low_distinct.unwrap_or(0) as f64 / rows as f64,
        high_distinct.unwrap_or(0) as f64 / rows as f64,
    );

    let full = all_set(rows as usize);

    // ── unfiltered top-k: dictionary vs docs column, low + high card ──────
    for field in [&low, &high] {
        let dict = timed(&format!("dict topn unfiltered[{field}]"), || {
            collect::unfiltered_top_n(&reader, field, 10, false).unwrap()
        });
        match &dict {
            Some(groups) => eprintln!(
                "  dict topn[{field}]: {} groups, top={:?}",
                groups.len(),
                groups.iter().max_by_key(|(_, c)| *c)
            ),
            None => eprintln!("  dict topn[{field}]: REFUSED (fallback path)"),
        }
        let docs = timed(&format!("docs topn unfiltered[{field}]"), || {
            collect::simple_top_n(&reader, &full, std::slice::from_ref(field), 10, false).unwrap()
        });
        eprintln!(
            "  docs topn[{field}]: {} groups, top={:?}",
            docs.len(),
            docs.iter().max_by_key(|(_, c)| *c)
        );
        if let Some(dict) = dict {
            // parity: identical (value, count) multisets modulo truncation
            let mut a: Vec<_> = dict.into_iter().collect();
            let mut b: Vec<_> = docs.into_iter().collect();
            a.sort();
            b.sort();
            if a.len() == b.len() {
                assert_eq!(a, b, "dict and docs top-n must agree on [{field}]");
                eprintln!("  parity[{field}]: {} groups EQUAL", a.len());
            } else {
                eprintln!(
                    "  parity[{field}]: differing group counts {} vs {} (truncation), skipped",
                    a.len(),
                    b.len()
                );
            }
        }
    }

    // ── unfiltered distinct: dictionary head vs docs column ──────────────
    for field in [&low, &high] {
        let dict = timed(&format!("dict distinct unfiltered[{field}]"), || {
            collect::unfiltered_distinct(&reader, field, 10, true).unwrap()
        });
        match &dict {
            Some(values) => eprintln!("  dict distinct[{field}]: {} values", values.len()),
            None => eprintln!("  dict distinct[{field}]: REFUSED (fallback path)"),
        }
        let docs = timed(&format!("docs distinct unfiltered[{field}]"), || {
            collect::simple_distinct(&reader, &full, field, 10, true).unwrap()
        });
        eprintln!("  docs distinct[{field}]: {} values", docs.len());
        if let Some(dict) = dict {
            assert_eq!(dict, docs, "dict and docs distinct must agree on [{field}]");
            eprintln!("  parity[{field}]: distinct sets EQUAL");
        }
    }

    // ── filtered top-k/distinct: postings-counted dictionary vs docs ─────
    // condition: equality on the low-card field's most frequent value (the
    // realistic "WHERE service = X GROUP BY service" shape); the group
    // field enumeration is the LOW field — the high-card field refuses via
    // the #29 cap and falls back, printed honestly below.
    let top_value = reader
        .field_value_top_k(&low, 1, false)
        .unwrap()
        .and_then(|(counts, _)| counts.into_iter().next());
    let Some((needle, needle_docs)) = top_value else {
        eprintln!("no top value on {low}; filtered classes skipped");
        return;
    };
    eprintln!(
        "filter: {low} == {:?} ({needle_docs} docs)",
        String::from_utf8_lossy(&needle)
    );
    let query = VixQuery::Exact {
        field: low.clone(),
        token: needle.clone(),
    };
    let bitmap = timed("filter eval bitmap", || reader.eval(&query).unwrap());
    eprintln!("  matched={}", bitmap.count_set_bits());

    for field in [&low, &high] {
        let dict = timed(&format!("dict topn filtered[{field}]"), || {
            collect::filtered_top_n(&reader, &bitmap, field, 10, false).unwrap()
        });
        match &dict {
            Some(groups) => eprintln!(
                "  dict filtered topn[{field}]: {} groups, top={:?}",
                groups.len(),
                groups.iter().max_by_key(|(_, c)| *c)
            ),
            None => eprintln!("  dict filtered topn[{field}]: REFUSED (fallback path)"),
        }
        let docs = timed(&format!("docs topn filtered[{field}]"), || {
            collect::simple_top_n(&reader, &bitmap, std::slice::from_ref(field), 10, false)
                .unwrap()
        });
        eprintln!(
            "  docs filtered topn[{field}]: {} groups, top={:?}",
            docs.len(),
            docs.iter().max_by_key(|(_, c)| *c)
        );

        let dict = timed(&format!("dict distinct filtered[{field}]"), || {
            collect::filtered_distinct(&reader, &bitmap, field, 10, true).unwrap()
        });
        match &dict {
            Some(values) => {
                eprintln!("  dict filtered distinct[{field}]: {} values", values.len())
            }
            None => eprintln!("  dict filtered distinct[{field}]: REFUSED (fallback path)"),
        }
        let docs = timed(&format!("docs distinct filtered[{field}]"), || {
            collect::simple_distinct(&reader, &bitmap, field, 10, true).unwrap()
        });
        eprintln!("  docs filtered distinct[{field}]: {} values", docs.len());
    }

    // ── SimpleSelect wave path (per-file candidate extraction) ────────────
    let cands = timed("simple_select full-range limit 1000 DESC", || {
        collect::simple_select(&reader, &full, 1000, false).unwrap()
    });
    eprintln!(
        "  select full: {} candidates, first={:?}",
        cands.len(),
        cands.first()
    );
    let cands = timed("simple_select filtered limit 1000 DESC", || {
        collect::simple_select(&reader, &bitmap, 1000, false).unwrap()
    });
    eprintln!(
        "  select filtered: {} candidates, first={:?}",
        cands.len(),
        cands.first()
    );

    // ── ranked-plist histogram vs bitmap histogram ────────────────────────
    let Some(chunks) = reader.zone_chunks() else {
        eprintln!("no zone table; histogram classes skipped");
        return;
    };
    let (ts_min, ts_max) = chunks.iter().fold((i64::MAX, i64::MIN), |acc, c| {
        (acc.0.min(c.ts_min), acc.1.max(c.ts_max))
    });
    let num_buckets = 60usize;
    let width = (((ts_max - ts_min) as u64) / num_buckets as u64 + 1).max(1);
    let window = (ts_min, ts_max + 1);
    match reader.single_term_plist_cursor(&query).unwrap() {
        Some(cursor) => {
            let ranked = timed("ranked-plist histogram", || {
                collect::ranked_simple_histogram(
                    &reader,
                    &cursor,
                    ts_min,
                    width,
                    num_buckets,
                    0,
                    window,
                )
                .unwrap()
            });
            let total: u64 = ranked.iter().sum();
            eprintln!("  ranked histogram: {total} rows across {num_buckets} buckets");
            let bitmapped = timed("bitmap histogram (same term)", || {
                collect::simple_histogram(&reader, &bitmap, ts_min, width, num_buckets, 0).unwrap()
            });
            let total_b: u64 = bitmapped.iter().sum();
            eprintln!("  bitmap histogram: {total_b} rows");
            assert_eq!(ranked, bitmapped, "ranked and bitmap histograms must agree");
            eprintln!("  parity: histograms EQUAL");
        }
        None => eprintln!("no plist cursor for the filter term; ranked histogram skipped"),
    }
}
