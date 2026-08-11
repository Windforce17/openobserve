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
    let reader = VixReader::open(data).expect("open");
    eprintln!("file={path} rows={}", reader.row_count());

    // sample real values; the trace walk is expensive on big corpora but
    // runs identically on every tree (setup, not measured)
    let svc = reader
        .field_value_counts("service_name")
        .unwrap()
        .expect("service_name is dictionary-eligible");
    let (svc0, svc0_count) = svc.first().cloned().expect("has service values");
    let traces = reader
        .field_value_counts("trace_id")
        .unwrap()
        .expect("trace_id is dictionary-eligible");
    let (t0, _) = traces.first().cloned().expect("has trace values");
    drop(traces);
    eprintln!(
        "sampled service={} (count {svc0_count}) trace={}",
        String::from_utf8_lossy(&svc0),
        String::from_utf8_lossy(&t0),
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
    time("eval Exact trace_id (needle)", 20, &mut || {
        reader.eval(&exact("trace_id", &t0)).unwrap().count_set_bits() as u64
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
    time("eval Prefix trace_id[..4] (dict range walk)", 10, &mut || {
        reader
            .eval(&VixQuery::Prefix {
                field: Some("trace_id".to_string()),
                prefix: hex4.clone(),
            })
            .unwrap()
            .count_set_bits() as u64
    });
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
    time("eval Contains trace_id 4-hex (full field scan)", 3, &mut || {
        reader
            .eval(&VixQuery::Contains {
                field: Some("trace_id".to_string()),
                needle: hex4.clone(),
                case_insensitive: false,
            })
            .unwrap()
            .count_set_bits() as u64
    });
}
