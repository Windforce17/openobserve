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

//! Cross-class query micro-bench for A/B runs ACROSS COMMITS (manual, never
//! CI): every API used here predates #29, so the identical file drops into
//! an older worktree for a like-for-like comparison.
//!
//! `O2_VIX_FILE=<file.vix> cargo test -p vortex_index --release
//!  --test query_bench -- --ignored --nocapture`
//!
//! Values are sampled from the corpus itself (via `field_value_counts`,
//! present on all trees), so any generated corpus works as long as it has
//! `service_name` (low cardinality) and `trace_id` (high cardinality)
//! string fields — e.g. `merge_bench gen` output.

use bytes::Bytes;
use vortex_index::{VixQuery, VixReader};

#[test]
#[ignore = "manual A/B query bench (set O2_VIX_FILE)"]
fn bench_query_classes() {
    let Ok(path) = std::env::var("O2_VIX_FILE") else {
        eprintln!("O2_VIX_FILE not set; skipping");
        return;
    };
    let data = Bytes::from(std::fs::read(&path).expect("read vix file"));
    // v3 split: the sidecar sits next to the data object (extension
    // swapped); the dictionary walks below need it
    let index_path = path.trim_end_matches(".vix").to_string() + ".vxi";
    let index = std::fs::read(&index_path).ok().map(Bytes::from);
    let reader = VixReader::open_with_index(data.clone(), index).expect("open");
    eprintln!("file={path} rows={}", reader.row_count());
    // .bf-relevant accounting: the per-file bloom blob is what the group
    // assembler transposes — report its sections (composite included)
    match reader.file_blooms() {
        Ok(Some(blooms)) => {
            for b in &blooms {
                eprintln!(
                    "bloom section {:?}: n_items={} bytes={}",
                    b.field,
                    b.n_items,
                    b.bytes.len()
                );
            }
        }
        Ok(None) => eprintln!("bloom sections: none"),
        Err(e) => eprintln!("bloom sections: unreadable ({e:#})"),
    }

    // sample real values; the trace walk is expensive on big corpora but
    // runs identically on every tree (setup, not measured)
    let svc = reader
        .field_value_counts("service_name")
        .unwrap()
        .expect("service_name is dictionary-eligible");
    let (svc0, svc0_count) = svc.first().cloned().expect("has service values");
    eprintln!(
        "sampled service={} (count {svc0_count})",
        String::from_utf8_lossy(&svc0),
    );

    let time = |name: &str, iters: usize, f: &mut dyn FnMut() -> u64| {
        let warm = f();
        let start = std::time::Instant::now();
        for _ in 0..iters {
            std::hint::black_box(f());
        }
        let per = start.elapsed() / iters as u32;
        eprintln!("{name:50} {per:>12?}/iter (result {warm})");
    };
    let exact = |field: &str, token: &[u8]| VixQuery::Exact {
        field: field.to_string(),
        token: token.to_vec(),
    };

    time("count Exact service_name (doc_count read)", 20, &mut || {
        reader.count(&exact("service_name", &svc0)).unwrap()
    });
    time("eval Exact service_name (dense postings)", 10, &mut || {
        reader
            .eval(&exact("service_name", &svc0))
            .unwrap()
            .count_set_bits() as u64
    });

    // #52/M7: on a file whose trace_id is DEMOTED to bloom-only there is no
    // dictionary to sample or query — the v2 equality path is composite-
    // bloom file pruning + a native-column filter-back scan. Measure THAT
    // instead of the postings classes (which this branch reports as N/A).
    let Some(traces) = reader.field_value_counts("trace_id").unwrap() else {
        bench_demoted_trace_classes(&reader, &data, &svc0, &time, &exact);
        return;
    };
    let (t0, _) = traces.first().cloned().expect("has trace values");
    drop(traces);
    eprintln!("sampled trace={}", String::from_utf8_lossy(&t0));

    time("eval Exact trace_id (needle)", 20, &mut || {
        reader
            .eval(&exact("trace_id", &t0))
            .unwrap()
            .count_set_bits() as u64
    });
    time("eval And[service, trace] (rarest-first)", 20, &mut || {
        reader
            .eval(&VixQuery::And(vec![
                exact("service_name", &svc0),
                exact("trace_id", &t0),
            ]))
            .unwrap()
            .count_set_bits() as u64
    });
    let hex4 = t0[..4.min(t0.len())].to_vec();
    time(
        "eval Prefix trace_id[..4] (dict range walk)",
        10,
        &mut || {
            reader
                .eval(&VixQuery::Prefix {
                    field: Some("trace_id".to_string()),
                    prefix: hex4.clone(),
                })
                .unwrap()
                .count_set_bits() as u64
        },
    );
    time("count Prefix service_name[..3]", 10, &mut || {
        reader
            .count(&VixQuery::Prefix {
                field: Some("service_name".to_string()),
                prefix: svc0[..3.min(svc0.len())].to_vec(),
            })
            .unwrap()
    });
    // the heaviest scan_key_range class: substring scan over EVERY key of a
    // 16M-term field (block_scan + memmem per key)
    time(
        "eval Contains trace_id 4-hex (full field scan)",
        3,
        &mut || {
            reader
                .eval(&VixQuery::Contains {
                    field: Some("trace_id".to_string()),
                    needle: hex4.clone(),
                    case_insensitive: false,
                })
                .unwrap()
                .count_set_bits() as u64
        },
    );
}

/// #52/M7: what equality on a BLOOM-ONLY (demoted) field actually costs in
/// v2, measured in its three moving parts —
///
/// 1. the composite-bloom PER-FILE PRUNE decision (3 guard probes + the value probe: the pruner's
///    whole per-file cost, both directions),
/// 2. the surviving file's FILTER-BACK SCAN of the native column, with the equality bound engaging
///    the M4 chunk-stat pruning tier (random hex IDs are expected to prune ~nothing there —
///    reported once), and
/// 3. the AND shape: the dense sibling narrows by postings first and the demoted leg filters back
///    over the selected rows only.
///
/// Prefix/Contains have no demoted-field equivalent (no dictionary): the
/// engine takes the full scan fallback — reported as N/A.
fn bench_demoted_trace_classes(
    reader: &VixReader,
    data: &Bytes,
    svc0: &[u8],
    time: &dyn Fn(&str, usize, &mut dyn FnMut() -> u64),
    exact: &dyn Fn(&str, &[u8]) -> VixQuery,
) {
    use arrow::array::Array;
    use vortex_index::{
        BoundValue, ColumnBound, VixDocs,
        bloom::{
            COMPOSITE_BLOOM_FIELD, COMPOSITE_GUARD_PROBES, composite_guard_key, composite_value_key,
        },
        sbbf::{BLOCK_BYTES, block_index, check_block, hash_value},
    };

    eprintln!(
        "trace_id is BLOOM-ONLY (demoted; markers: {:?}) — measuring the v2 \
         equality path instead of the postings classes",
        reader.bloom_only_fields().collect::<Vec<_>>()
    );
    let docs = VixDocs::open(data.clone()).expect("open docs");
    let projection = vec!["trace_id".to_string()];

    // decoded string columns arrive as any utf8 family (Utf8/Large/View):
    // normalize to StringArray once per batch
    let as_utf8 = |column: &arrow::array::ArrayRef| -> arrow::array::StringArray {
        arrow::compute::cast(column, &arrow::datatypes::DataType::Utf8)
            .expect("string-family column casts to Utf8")
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("cast produced Utf8")
            .clone()
    };

    // sample the needle from the MIDDLE row's native column (setup)
    let mut sampled: Option<String> = None;
    docs.scan_docs(
        Some(&projection),
        Some(vec![reader.row_count() / 2]),
        None,
        &mut |batch| {
            let strings = as_utf8(batch.column_by_name("trace_id").expect("projected"));
            if strings.len() > 0 && !strings.is_null(0) {
                sampled = Some(strings.value(0).to_string());
            }
            Ok(())
        },
    )
    .expect("sample scan");
    let t0 = sampled.expect("mid row carries a trace value");
    eprintln!("sampled trace={t0}");

    // (1) the pruner's per-file decision: guards x3 + value probe
    let blooms = reader
        .file_blooms()
        .expect("bloom blob readable")
        .expect("demoted file carries a per-file bloom blob");
    let comp = blooms
        .iter()
        .find(|b| b.field == COMPOSITE_BLOOM_FIELD)
        .expect("composite section");
    let probe = |key: &[u8]| -> bool {
        let h = hash_value(key);
        let i = block_index(h, comp.num_blocks) as usize;
        let block: &[u8; BLOCK_BYTES] = comp.bytes[i * BLOCK_BYTES..(i + 1) * BLOCK_BYTES]
            .try_into()
            .unwrap();
        check_block(block, h)
    };
    let mut buf = Vec::new();
    let guard_keys: Vec<Vec<u8>> = (0..COMPOSITE_GUARD_PROBES)
        .map(|p| {
            composite_guard_key("trace_id", p, &mut buf)
                .unwrap()
                .to_vec()
        })
        .collect();
    let present_key = composite_value_key("trace_id", t0.as_bytes(), &mut buf)
        .unwrap()
        .to_vec();
    let absent_key = composite_value_key("trace_id", b"no-such-trace-id-value", &mut buf)
        .unwrap()
        .to_vec();
    time(
        "bloom prune decision trace_id (hit: keep)",
        100_000,
        &mut || u64::from(guard_keys.iter().all(|k| probe(k)) && probe(&present_key)),
    );
    time(
        "bloom prune decision trace_id (miss: drop)",
        100_000,
        &mut || u64::from(guard_keys.iter().all(|k| probe(k)) && probe(&absent_key)),
    );

    // (2) filter-back scan of the native column, equality bound pushed —
    // report the M4 chunk-stat tier's effect once (setup, not measured)
    let bound = ColumnBound {
        column: "trace_id".to_string(),
        min: Some((BoundValue::Str(t0.clone()), true)),
        max: Some((BoundValue::Str(t0.clone()), true)),
    };
    match docs.pruned_scan_ranges(None, std::slice::from_ref(&bound)) {
        Some(ranges) => {
            let surviving: u64 = ranges.iter().map(|r| r.end - r.start).sum();
            eprintln!(
                "M4 chunk-stat tier: {surviving} of {} rows survive the equality \
                 bound ({} ranges) — random-ID min/max windows prune little, as expected",
                reader.row_count(),
                ranges.len()
            );
        }
        None => eprintln!("M4 chunk-stat tier: no pruning basis for this bound"),
    }
    // measured comparison stays on the DECODED array type (no cast/copy in
    // the loop — vortex yields Utf8View); the cast fallback covers exotics
    let count_eq = |batch: &arrow::record_batch::RecordBatch, needle: &str| -> u64 {
        let column = batch.column_by_name("trace_id").expect("projected");
        let any = column.as_any();
        if let Some(v) = any.downcast_ref::<arrow::array::StringViewArray>() {
            v.iter().filter(|x| *x == Some(needle)).count() as u64
        } else if let Some(v) = any.downcast_ref::<arrow::array::StringArray>() {
            v.iter().filter(|x| *x == Some(needle)).count() as u64
        } else if let Some(v) = any.downcast_ref::<arrow::array::LargeStringArray>() {
            v.iter().filter(|x| *x == Some(needle)).count() as u64
        } else {
            as_utf8(column)
                .iter()
                .filter(|x| *x == Some(needle))
                .count() as u64
        }
    };
    let mut scan_eq = |threads: usize| -> u64 {
        let mut hits = 0u64;
        docs.scan_docs_opts(
            Some(&projection),
            None,
            None,
            std::slice::from_ref(&bound),
            None,
            threads,
            &mut |batch| {
                hits += count_eq(&batch, &t0);
                Ok(())
            },
        )
        .expect("filter-back scan");
        hits
    };
    time(
        "filter-back scan trace_id == t0 (0 threads)",
        3,
        &mut || scan_eq(0),
    );
    time(
        "filter-back scan trace_id == t0 (4 threads)",
        3,
        &mut || scan_eq(4),
    );

    // (3) the AND shape: postings narrow by the dense sibling, the demoted
    // leg point-reads only the selected rows
    let svc_rows: Vec<u64> = reader
        .eval(&exact("service_name", svc0))
        .expect("service postings")
        .set_indices()
        .map(|i| i as u64)
        .collect();
    eprintln!(
        "And shape: service_name postings select {} rows",
        svc_rows.len()
    );
    time(
        "And[svc postings -> trace filter-back point read]",
        3,
        &mut || {
            let mut hits = 0u64;
            docs.scan_docs(
                Some(&projection),
                Some(svc_rows.clone()),
                None,
                &mut |batch| {
                    hits += count_eq(&batch, &t0);
                    Ok(())
                },
            )
            .expect("point-read scan");
            hits
        },
    );

    eprintln!(
        "{:50} N/A — demoted field has no dictionary; the engine takes the \
         scan fallback measured above",
        "eval Prefix/Contains trace_id"
    );
}

/// M15b measurement (manual): the demoted-needle FILTER-BACK scan, before
/// vs after the dict-aware equality pre-pass, and its thread scaling.
///
/// `O2_VIX_FILE=<file.vix> cargo test -p vortex_index --release
///  --test query_bench -- --ignored bench_eq_filter_back --nocapture`
///
/// - "old shape": unbounded single-column scan + per-row compare (what the M7 bench measured at
///   635ms/16M rows, no thread scaling);
/// - "eq-bound scan": the same scan_docs_opts call WITH the equality bound — the M15 pre-pass path
///   end to end (dict resolve + code scan + point read of the matches).
#[test]
#[ignore = "manual M15b bench (set O2_VIX_FILE)"]
fn bench_eq_filter_back() {
    use arrow::array::Array;
    use vortex_index::{BoundValue, ColumnBound, SOURCE_COL_NAME, VixDocs};

    let Ok(path) = std::env::var("O2_VIX_FILE") else {
        eprintln!("O2_VIX_FILE not set; skipping");
        return;
    };
    let column = std::env::var("O2_VIX_EQ_COLUMN").unwrap_or_else(|_| "trace_id".to_string());
    let data = Bytes::from(std::fs::read(&path).expect("read vix file"));
    let docs = VixDocs::open(data).expect("open docs");
    eprintln!("file={path} rows={} column={column}", docs.row_count());

    let as_utf8 = |column: &arrow::array::ArrayRef| -> arrow::array::StringArray {
        arrow::compute::cast(column, &arrow::datatypes::DataType::Utf8)
            .expect("string-family column casts to Utf8")
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("cast produced Utf8")
            .clone()
    };
    // Allow a production needle to be supplied directly. Otherwise sample
    // several rows so nullable/sparse columns do not make the manual bench fail.
    let needle = if let Ok(value) = std::env::var("O2_VIX_EQ_VALUE") {
        value
    } else {
        let sample_rows = (0..128_u64)
            .map(|offset| offset.saturating_mul(docs.row_count()) / 128)
            .collect::<Vec<_>>();
        let mut sampled: Option<String> = None;
        let projection = vec![column.clone()];
        docs.scan_docs(Some(&projection), Some(sample_rows), None, &mut |batch| {
            let strings = as_utf8(batch.column_by_name(&column).expect("projected"));
            if sampled.is_none() {
                sampled = strings.iter().flatten().next().map(str::to_owned);
            }
            Ok(())
        })
        .expect("sample scan");
        sampled.expect("sampled rows carry a value")
    };
    eprintln!("needle={needle}");
    let projection = vec![column.clone()];
    let iterations = std::env::var("O2_VIX_BENCH_ITERS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(3);
    assert!(iterations > 0, "O2_VIX_BENCH_ITERS must be positive");

    let time = |name: &str, iters: usize, f: &mut dyn FnMut() -> u64| {
        let warm = f();
        let start = std::time::Instant::now();
        for _ in 0..iters {
            std::hint::black_box(f());
        }
        let per = start.elapsed() / iters as u32;
        eprintln!("{name:55} {per:>12?}/iter (result {warm})");
    };
    let count_eq = |batch: &arrow::record_batch::RecordBatch| -> u64 {
        let strings = as_utf8(batch.column_by_name(&column).expect("projected"));
        strings
            .iter()
            .filter(|x| x.as_deref() == Some(needle.as_str()))
            .count() as u64
    };

    // old shape: no bound pushed — full column decode + per-row compare
    let old_scan = |threads: usize| -> u64 {
        let mut hits = 0u64;
        docs.scan_docs_opts(
            Some(&projection),
            None,
            None,
            &[],
            None,
            threads,
            &mut |b| {
                hits += count_eq(&b);
                Ok(())
            },
        )
        .expect("old-shape scan");
        hits
    };
    time(
        "OLD full scan + compare (0 threads)",
        iterations,
        &mut || old_scan(0),
    );
    time(
        "OLD full scan + compare (4 threads)",
        iterations,
        &mut || old_scan(4),
    );

    // Production raw-SELECT shape: the fallback decodes `_source` beside
    // the predicate before DataFusion can apply ORDER BY/LIMIT.
    let raw_projection = vec![column.clone(), SOURCE_COL_NAME.to_string()];
    let old_raw_scan = || -> u64 {
        let mut hits = 0u64;
        docs.scan_docs_opts(
            Some(&raw_projection),
            None,
            None,
            &[],
            None,
            0,
            &mut |batch| {
                hits += count_eq(&batch);
                std::hint::black_box(batch.column_by_name(SOURCE_COL_NAME).expect("source"));
                Ok(())
            },
        )
        .expect("old raw-select scan");
        hits
    };
    time(
        "OLD raw equality + full _source decode",
        iterations,
        &mut || old_raw_scan(),
    );

    let limit = std::env::var("O2_VIX_TOP_LIMIT")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(100);
    assert!(limit > 0, "O2_VIX_TOP_LIMIT must be positive");
    let adaptive_candidates = || {
        docs.eq_string_top_n(&column, &needle, None, limit, false)
            .expect("adaptive equality candidates")
            .expect("native string column")
    };
    time(
        "NEW adaptive equality top-K (no _source)",
        iterations,
        &mut || adaptive_candidates().len() as u64,
    );
    time(
        "NEW adaptive top-K + winning _source point read",
        iterations,
        &mut || {
            let row_ids = adaptive_candidates()
                .into_iter()
                .map(|(_, row_id)| row_id as u64)
                .collect();
            let mut rows = 0u64;
            docs.scan_docs(
                Some(&[SOURCE_COL_NAME.to_string()]),
                Some(row_ids),
                None,
                &mut |batch| {
                    rows += batch.num_rows() as u64;
                    std::hint::black_box(batch.column(0));
                    Ok(())
                },
            )
            .expect("winning source point read");
            rows
        },
    );

    // new shape: the equality bound engages the M15 dict-aware pre-pass
    let bound = ColumnBound {
        column: column.clone(),
        min: Some((BoundValue::Str(needle.clone()), true)),
        max: Some((BoundValue::Str(needle.clone()), true)),
    };
    let eq_scan = |threads: usize| -> u64 {
        let mut hits = 0u64;
        docs.scan_docs_opts(
            Some(&projection),
            None,
            None,
            std::slice::from_ref(&bound),
            None,
            threads,
            &mut |b| {
                hits += count_eq(&b);
                Ok(())
            },
        )
        .expect("eq-bound scan");
        hits
    };
    time("M15 eq-bound scan (0 threads)", iterations, &mut || {
        eq_scan(0)
    });
    time("M15 eq-bound scan (4 threads)", iterations, &mut || {
        eq_scan(4)
    });
    time("M15 eq-bound scan (16 threads)", iterations, &mut || {
        eq_scan(16)
    });
}
