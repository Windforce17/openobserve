//! M23 repro harness: compaction REBUILD of an index-off LOGS group climbs
//! RSS far past the input size in prod (+17.6 GB on a ~4 GB-original group,
//! linear ~65 MB/s, OOM at 48 Gi). This example reproduces the exact shape
//! locally with full observability:
//!
//!   gen <dir> <files> <rows_per_file>
//!       Build an INDEX-OFF logs corpus (#42 L0 shape, via
//!       ZO_VIX_L0_INDEX_OFF_STREAM_TYPES=logs — the harness re-execs itself
//!       with the env set) through the REAL move builder
//!       (write_core_file_from_tables). Rows are log-shaped: a `message`
//!       field of 20-60 zipf-sampled words from a 50k-token vocabulary plus
//!       a few low-cardinality fields; `_source` is synthesized by the
//!       builder as in prod. Files cover DISJOINT time ranges (the
//!       concatenation-shaped group M23 was proven on).
//!
//!   gen-il <dir> <files> <rows_per_file>
//!       M23b: the same corpus except timestamps are INTERLEAVED round-robin
//!       across files (file f, global row r -> ts = base + r*files + f): all
//!       files cover the SAME range and the k-way merge order alternates
//!       through every input within every output window — the prod L0 shape
//!       (same (stream,hour) files overlap), where M23's lazy spawn
//!       degenerates back to whole-group-resident.
//!
//!   merge <dir> <out.vix>
//!       Ranged-open every corpus file (the compactor worker's input shape)
//!       and run merge_core_files(StreamType::Logs, fts=["message"]) — the
//!       SAME public entry the compactor uses. Index-off inputs against the
//!       indexed logs plan fire the "index merge not applicable, rebuilding
//!       terms from _source" fallback, i.e. the prod OOM path. Env pinned to
//!       prod: ZO_VIX_PLIST_MIN_DOCS=8192.
//!
//!   merge-flip <dir> <out.vix>
//!       M23b: same merge, but the latest (current stream) schema types
//!       `code` as Utf8 while every input stores Int64 — real-world schema
//!       evolution. The type flip fails every input's heal-passthrough
//!       qualification (docs_widen_plan rejects type flips) AND disables the
//!       #46 column derivation, so the merge runs the STANDARD rebuild:
//!       terms re-derived from `_source` over the k-way merge order — on an
//!       interleaved corpus that order is fine-grained interleaving, the
//!       shape prod OOMs on. (The heal passthrough would otherwise absorb an
//!       overlapping group into its #51c-c CONCATENATION order and mask the
//!       interleave.)
//!
//!   gen-hc <dir> <files> <rows_per_file>
//!       M24: cloudtrail/k8s-shaped HIGH-CARDINALITY corpus (disjoint DESC
//!       ranges like `gen`): three id fields unique PER ROW (uuid4-shaped
//!       eventid/requestid, an ARN with a unique session suffix), a
//!       near-unique source ip, and a message mixing zipf vocabulary with
//!       ~5 per-row-unique hex tokens — multiple MILLIONS of distinct terms
//!       per group, the scale factor prod's kill model identified and the
//!       M23/M23b 50k-vocab corpora never exercised.
//!
//!   merge-hc / merge-hc-flip <dir> <out.vix>
//!       The same two merge arms over the hc corpus (heal-passthrough /
//!       standard rebuild; the flip field is `httpstatus`).
//!
//!   gen-wide <dir> <files> <rows_per_file> <width>
//!       M25: WIDE SPARSE k8s-logs-shaped corpus — the schema-width factor
//!       every earlier corpus (~a dozen columns) left unexercised while prod
//!       logs streams carry hundreds to ~2,164 fields. `width` = total
//!       distinct fields in the corpus UNION (8 always-present core fields
//!       incl. `message`/`status` + `width-8` sparse fields, ~1/5 Int64).
//!       Narrow-WAL semantics: each FILE's schema is a per-file subset of
//!       the sparse universe (the whole universe when it is <= 256 fields,
//!       else a per-file random 25%), so per-file schemas DIFFER and only
//!       the merge plan's union is `width` wide. Each ROW populates 24..=64
//!       value tokens spread over the file's subset (mostly one token per
//!       field -> ~24-64 populated fields/row of ~2,000 — the UDS reality);
//!       token count and byte weight are width-INDEPENDENT, so corpora at
//!       different widths compare at ~equal value bytes. Disjoint DESC
//!       ranges (the M23 concatenation group shape).
//!
//!   merge-wide / merge-wide-flip <dir> <out.vix> <width>
//!       The two rebuild arms over the wide corpus (same semantics as
//!       merge / merge-flip): the latest schema is the full width-wide
//!       union; the flip types the always-present `status` Int64 field as
//!       Utf8, disqualifying every input from the heal passthrough and #46
//!       column derivation -> the STANDARD rebuild (decode + re-encode).
//!
//! Instrumentation: a sampler thread prints VmRSS/VmHWM every 500 ms with
//! elapsed time, PLUS the LIVE allocated bytes tracked by a counting wrapper
//! around mimalloc (prod's allocator) — the live-vs-RSS split is what
//! distinguishes a real accumulator from allocator retention. Phase markers
//! come from the engine's own log lines (all levels printed to stderr) plus
//! the worktree-local "m23:" markers added in vortex_index.

use std::{
    alloc::{GlobalAlloc, Layout},
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::Instant,
};

use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
};
use datafusion::{catalog::TableProvider, datasource::MemTable};

// ---------------------------------------------------------------------------
// prod allocator + live-byte accounting
// ---------------------------------------------------------------------------

/// Live allocated bytes (requested sizes). RSS - live = allocator slack +
/// retention + non-heap (stacks, mmaps).
static LIVE_BYTES: AtomicI64 = AtomicI64::new(0);

struct CountingMi;

unsafe impl GlobalAlloc for CountingMi {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { mimalloc::MiMalloc.alloc(layout) };
        if !p.is_null() {
            LIVE_BYTES.fetch_add(layout.size() as i64, Ordering::Relaxed);
        }
        p
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { mimalloc::MiMalloc.alloc_zeroed(layout) };
        if !p.is_null() {
            LIVE_BYTES.fetch_add(layout.size() as i64, Ordering::Relaxed);
        }
        p
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { mimalloc::MiMalloc.dealloc(ptr, layout) };
        LIVE_BYTES.fetch_sub(layout.size() as i64, Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let p = unsafe { mimalloc::MiMalloc.realloc(ptr, layout, new_size) };
        if !p.is_null() {
            LIVE_BYTES.fetch_add(new_size as i64 - layout.size() as i64, Ordering::Relaxed);
        }
        p
    }
}

#[global_allocator]
static GLOBAL: CountingMi = CountingMi;

// ---------------------------------------------------------------------------
// RSS sampler
// ---------------------------------------------------------------------------

fn proc_status_kb(status: &str, key: &str) -> u64 {
    status
        .lines()
        .find(|line| line.starts_with(key))
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0)
}

/// Print `[rss] t=..s vmrss=..MB vmhwm=..MB live=..MB` every 500 ms.
fn spawn_rss_sampler() {
    let t0 = Instant::now();
    std::thread::Builder::new()
        .name("m23-rss-sampler".into())
        .spawn(move || {
            loop {
                let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
                let vmrss_mb = proc_status_kb(&status, "VmRSS:") / 1024;
                eprintln!(
                    "[rss] t={:.1}s vmrss={vmrss_mb}MB vmhwm={}MB live={}MB",
                    t0.elapsed().as_secs_f64(),
                    proc_status_kb(&status, "VmHWM:") / 1024,
                    LIVE_BYTES.load(Ordering::Relaxed) / (1024 * 1024),
                );
                // box-safety ceiling: the task pins ~24 GB max process RSS,
                // but this box has ~21 GB actually available — abort at 17 GB
                // (a runaway accumulator is unambiguous long before that)
                if vmrss_mb > 17 * 1024 {
                    eprintln!("[rss] SAFETY ABORT: vmrss {vmrss_mb}MB > 17GB ceiling");
                    std::process::abort();
                }
                std::thread::sleep(std::time::Duration::from_millis(500));
            }
        })
        .expect("spawn sampler");
}

// ---------------------------------------------------------------------------
// deterministic corpus
// ---------------------------------------------------------------------------

/// xorshift64* — deterministic, dependency-free.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn f64(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

const VOCAB_SIZE: usize = 50_000;

/// 50k distinct lowercase words, 4-11 chars, deterministic.
fn build_vocab() -> Vec<String> {
    let mut rng = Rng(0xC0FFEE_D00D_1234);
    let mut vocab = Vec::with_capacity(VOCAB_SIZE);
    let mut seen = std::collections::HashSet::with_capacity(VOCAB_SIZE * 2);
    while vocab.len() < VOCAB_SIZE {
        let len = 4 + (rng.below(8) as usize); // 4..=11 chars
        let mut w = String::with_capacity(len);
        for _ in 0..len {
            w.push((b'a' + rng.below(26) as u8) as char);
        }
        if seen.insert(w.clone()) {
            vocab.push(w);
        }
    }
    vocab
}

/// Zipf(s=1.0) cumulative distribution over the vocabulary ranks — log-like
/// term frequencies: head words appear in nearly every message, the tail is
/// rare. With ZO_VIX_PLIST_MIN_DOCS=8192 this exercises BOTH pointer (plist)
/// and inline postings cells, like prod.
fn build_zipf_cdf() -> Vec<f64> {
    let mut cdf = Vec::with_capacity(VOCAB_SIZE);
    let mut acc = 0.0f64;
    for rank in 1..=VOCAB_SIZE {
        acc += 1.0 / rank as f64;
        cdf.push(acc);
    }
    let total = acc;
    for v in &mut cdf {
        *v /= total;
    }
    cdf
}

fn zipf_word<'v>(rng: &mut Rng, vocab: &'v [String], cdf: &[f64]) -> &'v str {
    let u = rng.f64();
    let idx = cdf.partition_point(|&c| c < u).min(vocab.len() - 1);
    &vocab[idx]
}

const TIMESTAMP_COL: &str = "_timestamp";
const BATCH_ROWS: usize = 8192;

fn logs_schema() -> Arc<Schema> {
    let utf8 = |name: &str| Field::new(name, DataType::Utf8, true);
    Arc::new(Schema::new(vec![
        Field::new(TIMESTAMP_COL, DataType::Int64, false),
        Field::new("code", DataType::Int64, true),
        utf8("env"),
        utf8("level"),
        utf8("message"),
        utf8("pod"),
        utf8("service"),
    ]))
}

// ---------------------------------------------------------------------------
// M24: HIGH-CARDINALITY corpus (cloudtrail/k8s-shaped) — several string
// fields unique-or-near-unique PER ROW (uuid ids, ARN strings, IPs) plus a
// message mixing zipf vocabulary with per-row-unique hex tokens. Target:
// multiple MILLIONS of distinct terms across the group, the scale factor the
// M23/M23b 50k-vocab corpus never exercised.
// ---------------------------------------------------------------------------

fn hc_schema() -> Arc<Schema> {
    let utf8 = |name: &str| Field::new(name, DataType::Utf8, true);
    Arc::new(Schema::new(vec![
        Field::new(TIMESTAMP_COL, DataType::Int64, false),
        utf8("awsregion"),
        utf8("eventid"),
        utf8("eventname"),
        utf8("eventsource"),
        Field::new("httpstatus", DataType::Int64, true),
        utf8("message"),
        utf8("requestid"),
        utf8("sourceipaddress"),
        utf8("useridentity_arn"),
    ]))
}

/// splitmix64 finalizer — a BIJECTION on u64, so distinct inputs give
/// distinct outputs: uniqueness of the id-shaped values is guaranteed by
/// feeding distinct (global row, salt) counters, no cross-file coordination.
fn mix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D049BB133111EB);
    x ^ (x >> 31)
}

fn push_hex(out: &mut String, mut v: u64, nibbles: usize) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for _ in 0..nibbles {
        out.push(HEX[(v & 0xF) as usize] as char);
        v >>= 4;
    }
}

/// uuid4-shaped 36-char id from two mixed words (bijective in `g` per salt).
fn uuid_like(g: u64, salt: u64) -> String {
    let a = mix64(g ^ salt);
    let b = mix64(g ^ salt.rotate_left(17) ^ 0xA5A5_5A5A_DEAD_BEEF);
    let mut s = String::with_capacity(36);
    push_hex(&mut s, a, 8);
    s.push('-');
    push_hex(&mut s, a >> 32, 4);
    s.push('-');
    push_hex(&mut s, 0x4000 | ((a >> 48) & 0x0FFF), 4);
    s.push('-');
    push_hex(&mut s, 0x8000 | (b & 0x3FFF), 4);
    s.push('-');
    push_hex(&mut s, b >> 16, 12);
    s
}

/// One batch of cloudtrail-shaped rows. `global_row` is the corpus-wide row
/// counter driving the unique-value derivations. Returns the batch plus the
/// summed ORIGINAL string bytes (message + id fields' values).
#[allow(clippy::too_many_arguments)]
fn make_hc_batch(
    schema: &Arc<Schema>,
    rng: &mut Rng,
    vocab: &[String],
    cdf: &[f64],
    base_ts_us: i64,
    global_row: u64,
    rows: usize,
) -> (arrow::record_batch::RecordBatch, u64) {
    let regions: Vec<String> = (0..20).map(|i| format!("us-east-{}", i + 1)).collect();
    let event_names: Vec<String> = (0..60).map(|i| format!("ApiCallEvent{i}")).collect();
    let event_sources: Vec<String> = (0..30).map(|i| format!("svc{i}.amazonaws.com")).collect();
    let mut ts = Vec::with_capacity(rows);
    let mut awsregion = Vec::with_capacity(rows);
    let mut eventid = Vec::with_capacity(rows);
    let mut eventname = Vec::with_capacity(rows);
    let mut eventsource = Vec::with_capacity(rows);
    let mut httpstatus = Vec::with_capacity(rows);
    let mut message = Vec::with_capacity(rows);
    let mut requestid = Vec::with_capacity(rows);
    let mut sourceip = Vec::with_capacity(rows);
    let mut arn = Vec::with_capacity(rows);
    let mut value_bytes = 0u64;
    for row in 0..rows {
        let g = global_row + row as u64;
        ts.push(base_ts_us + row as i64);
        awsregion.push(regions[rng.below(20) as usize].clone());
        eventname.push(event_names[rng.below(60) as usize].clone());
        eventsource.push(event_sources[rng.below(30) as usize].clone());
        httpstatus.push([200i64, 200, 200, 204, 400, 403, 500][rng.below(7) as usize]);
        // unique-per-row ids (bijective mixes of the global row counter)
        let eid = uuid_like(g, 0x0001_D00D);
        let rid = uuid_like(g, 0xFACE_0002);
        // ARN-shaped: ~90 chars, unique per row via the session suffix
        let mut arn_s = String::with_capacity(96);
        arn_s.push_str("arn:aws:sts::123456789012:assumed-role/svc-role-");
        arn_s.push_str(&format!("{:02}", g % 40));
        arn_s.push_str("/aws-sdk-session-");
        push_hex(&mut arn_s, mix64(g ^ 0xA4_0003), 16);
        // source ip: 3/4 near-unique within 10.0.0.0/8, 1/4 from a small
        // NAT pool (prod shape: many uniques + hot repeats)
        let ip = if rng.below(4) < 3 {
            let v = mix64(g ^ 0x1b_0004);
            format!("10.{}.{}.{}", (v >> 16) & 255, (v >> 8) & 255, v & 255)
        } else {
            let v = rng.below(512);
            format!("192.168.{}.{}", v >> 8, v & 255)
        };
        // message: 24..=40 words, ~5-6 of them per-row-unique hex16 tokens
        // (request/trace-id fragments), the rest zipf vocabulary. The mix
        // input is `g*8 + ordinal` — globally distinct integers (ordinal
        // ≤ 6 < 8), so the bijective mix64 guarantees globally distinct
        // tokens (a `g ^ small` form would confine inputs to a ~2^22
        // space and collapse the distinct count).
        let words = 24 + rng.below(17) as usize;
        let unique_every = words / 5;
        let mut uniq = 0u64;
        let mut text = String::with_capacity(words * 10);
        for w in 0..words {
            if w > 0 {
                text.push(' ');
            }
            if unique_every > 0 && w % unique_every == unique_every / 2 {
                push_hex(&mut text, mix64(((g << 3) | uniq) ^ 0xE0_0005), 16);
                uniq += 1;
            } else {
                text.push_str(zipf_word(rng, vocab, cdf));
            }
        }
        value_bytes += (eid.len() + rid.len() + arn_s.len() + ip.len() + text.len()) as u64;
        eventid.push(eid);
        requestid.push(rid);
        sourceip.push(ip);
        arn.push(arn_s);
        message.push(text);
    }
    let batch = arrow::record_batch::RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(ts)) as ArrayRef,
            Arc::new(StringArray::from(awsregion)),
            Arc::new(StringArray::from(eventid)),
            Arc::new(StringArray::from(eventname)),
            Arc::new(StringArray::from(eventsource)),
            Arc::new(Int64Array::from(httpstatus)),
            Arc::new(StringArray::from(message)),
            Arc::new(StringArray::from(requestid)),
            Arc::new(StringArray::from(sourceip)),
            Arc::new(StringArray::from(arn)),
        ],
    )
    .unwrap();
    (batch, value_bytes)
}

// ---------------------------------------------------------------------------
// M25: WIDE SPARSE corpus (k8s-logs-shaped). The width knob varies ONLY the
// name space the per-row value tokens land on; token count and byte weight
// per row are width-independent, so 12/200/2000-wide corpora compare at
// ~equal value bytes and the peak curve isolates SCHEMA WIDTH.
// ---------------------------------------------------------------------------

/// Always-present core fields (besides `_timestamp`): `message` (fts),
/// `status` (the merge-wide-flip field) and a handful of k8s identity
/// fields every row carries.
const WIDE_CORE_FIELDS: usize = 8;

/// Sparse-field name + type, deterministic in the field index. k8s-ish key
/// shapes (labels/annotations/attributes), ~1/5 Int64 once the sparse space
/// is big enough to afford numeric fields (a 4-field space stays all-Utf8 so
/// the tiny-width anchor keeps every token instead of dropping Int64
/// duplicate draws).
fn wide_sparse_field(index: usize, sparse_width: usize) -> (String, DataType) {
    let name = match index % 4 {
        0 => format!("k8s_label_app_{index:04}"),
        1 => format!("k8s_annotation_meta_{index:04}"),
        2 => format!("attr_service_field_{index:04}"),
        _ => format!("log_ctx_{index:04}"),
    };
    let data_type = if sparse_width >= 8 && index % 5 == 4 {
        DataType::Int64
    } else {
        DataType::Utf8
    };
    (name, data_type)
}

/// The full `width`-wide union schema (the merge plan's latest schema):
/// `_timestamp` + core fields + every sparse field, non-ts fields sorted by
/// name (the storage convention shape).
fn wide_union_schema(width: usize) -> Arc<Schema> {
    let sparse_width = width.saturating_sub(WIDE_CORE_FIELDS);
    let mut fields: Vec<Field> = vec![
        Field::new("message", DataType::Utf8, true),
        Field::new("level", DataType::Utf8, true),
        Field::new("k8s_namespace_name", DataType::Utf8, true),
        Field::new("k8s_pod_name", DataType::Utf8, true),
        Field::new("k8s_container_name", DataType::Utf8, true),
        Field::new("status", DataType::Int64, true),
        Field::new("duration_ms", DataType::Int64, true),
    ];
    for index in 0..sparse_width {
        let (name, data_type) = wide_sparse_field(index, sparse_width);
        fields.push(Field::new(name, data_type, true));
    }
    fields.sort_by(|a, b| a.name().cmp(b.name()));
    let mut all = vec![Field::new(TIMESTAMP_COL, DataType::Int64, false)];
    all.extend(fields);
    Arc::new(Schema::new(all))
}

/// The per-file subset of the sparse universe (narrow-WAL semantics: a WAL
/// window sees the fields its tenants populate, not the registry). The whole
/// universe when it is small (<= 256), else a deterministic per-file 25%
/// sample — 40 such files union back to ~the full universe.
fn wide_file_subset(file: usize, sparse_width: usize) -> Vec<u32> {
    if sparse_width == 0 {
        return Vec::new();
    }
    if sparse_width <= 256 {
        return (0..sparse_width as u32).collect();
    }
    let subset = sparse_width / 4;
    let mut rng = Rng(0xB1DE_u64
        .wrapping_mul(0x9E3779B97F4A7C15)
        .wrapping_add((file as u64 + 1).wrapping_mul(0xD1B54A32D192ED03)));
    // partial Fisher-Yates: the first `subset` slots of a shuffled index vec
    let mut indices: Vec<u32> = (0..sparse_width as u32).collect();
    for slot in 0..subset {
        let pick = slot + rng.below((sparse_width - slot) as u64) as usize;
        indices.swap(slot, pick);
    }
    indices.truncate(subset);
    indices.sort_unstable();
    indices
}

/// One wide-sparse batch: every row populates the core fields plus 24..=64
/// DISTINCT sparse fields of the file's subset (capped by the subset size),
/// one value token each. Tokens carry a PER-FIELD bounded vocabulary
/// (`v{field:04}-{0..31}` / small ints) — k8s label/annotation reality —
/// so the corpus-wide distinct-term count stays in the narrow corpora's
/// class at EVERY width and the peak curve isolates SCHEMA WIDTH from the
/// M24-falsified vocabulary axis. Returns the batch (schema = `_timestamp`
/// + core + THIS FILE's subset, non-ts sorted by name) and the summed
/// ORIGINAL bytes (values + populated keys + JSON syntax).
fn make_wide_batch(
    schema: &Arc<Schema>,
    subset: &[(usize, String, DataType)],
    slot_scratch: &mut Vec<u32>,
    rng: &mut Rng,
    vocab: &[String],
    cdf: &[f64],
    base_ts_us: i64,
    ts_stride: i64,
    rows: usize,
) -> (arrow::record_batch::RecordBatch, u64) {
    let namespaces: Vec<String> = (0..20).map(|i| format!("ns-team-{i}")).collect();
    let pods: Vec<String> = (0..200)
        .map(|i| format!("api-deploy-{i:03}-x{}", i % 7))
        .collect();
    let containers: Vec<String> = (0..30).map(|i| format!("svc-checkout-{i}")).collect();
    let mut ts = Vec::with_capacity(rows);
    let mut message = Vec::with_capacity(rows);
    let mut level = Vec::with_capacity(rows);
    let mut namespace = Vec::with_capacity(rows);
    let mut pod = Vec::with_capacity(rows);
    let mut container = Vec::with_capacity(rows);
    let mut status = Vec::with_capacity(rows);
    let mut duration = Vec::with_capacity(rows);
    // sparse columns accumulate column-major: per subset slot, the sparse
    // (row, value) pairs (rows ascending by construction)
    let mut sparse_utf8: Vec<Vec<(u32, String)>> = vec![Vec::new(); subset.len()];
    let mut sparse_int: Vec<Vec<(u32, i64)>> = vec![Vec::new(); subset.len()];
    let mut original_bytes = 0u64;
    for row in 0..rows {
        ts.push(base_ts_us + row as i64 * ts_stride);
        let words = 16 + rng.below(13) as usize; // 16..=28
        let mut text = String::with_capacity(words * 9);
        for w in 0..words {
            if w > 0 {
                text.push(' ');
            }
            text.push_str(zipf_word(rng, vocab, cdf));
        }
        original_bytes += text.len() as u64;
        message.push(text);
        level.push(["info", "info", "info", "warn", "error"][rng.below(5) as usize].to_string());
        namespace.push(namespaces[rng.below(20) as usize].clone());
        pod.push(pods[rng.below(200) as usize].clone());
        container.push(containers[rng.below(30) as usize].clone());
        status.push([200i64, 200, 200, 204, 400, 500][rng.below(6) as usize]);
        duration.push(rng.below(30_000) as i64);
        // 24..=64 DISTINCT sparse fields of the file's subset (partial
        // Fisher-Yates over the reusable slot buffer, un-swapped after the
        // row), ONE per-field-vocabulary token each. Token byte weight is
        // width-independent; only WHICH (and how many distinct) field names
        // the row's tokens land on varies with the width.
        if !subset.is_empty() {
            let k = (24 + rng.below(41) as usize).min(subset.len());
            for i in 0..k {
                let j = i + rng.below((subset.len() - i) as u64) as usize;
                slot_scratch.swap(i, j);
                let slot = slot_scratch[i] as usize;
                match subset[slot].2 {
                    DataType::Int64 => {
                        // per-field ~100-value numeric space
                        sparse_int[slot].push((row as u32, rng.below(100) as i64));
                        // key + numeric value + `"":,` syntax
                        original_bytes += (subset[slot].1.len() + 2 + 4) as u64;
                    }
                    _ => {
                        // per-field 32-value vocabulary: `v{field:04}-{0..31}`
                        let suffix = rng.below(32);
                        let mut value = String::with_capacity(9);
                        value.push('v');
                        let f = subset[slot].0 as u64;
                        for div in [1000, 100, 10, 1] {
                            value.push((b'0' + ((f / div) % 10) as u8) as char);
                        }
                        value.push('-');
                        value.push((b'0' + (suffix / 10) as u8) as char);
                        value.push((b'0' + (suffix % 10) as u8) as char);
                        original_bytes += (value.len() + subset[slot].1.len() + 6) as u64;
                        sparse_utf8[slot].push((row as u32, value));
                    }
                }
            }
            // (no un-swap needed: the buffer stays a permutation of the
            // subset, which is all the distinct-draw requires; the RNG keeps
            // the sequence deterministic)
        }
        // core keys + timestamp + syntax (~90 B/row)
        original_bytes += 90;
    }
    // assemble arrays in SCHEMA order: name -> array
    let mut by_name: std::collections::HashMap<&str, ArrayRef> = std::collections::HashMap::new();
    by_name.insert(TIMESTAMP_COL, Arc::new(Int64Array::from(ts)) as ArrayRef);
    by_name.insert("message", Arc::new(StringArray::from(message)));
    by_name.insert("level", Arc::new(StringArray::from(level)));
    by_name.insert("k8s_namespace_name", Arc::new(StringArray::from(namespace)));
    by_name.insert("k8s_pod_name", Arc::new(StringArray::from(pod)));
    by_name.insert("k8s_container_name", Arc::new(StringArray::from(container)));
    by_name.insert("status", Arc::new(Int64Array::from(status)));
    by_name.insert("duration_ms", Arc::new(Int64Array::from(duration)));
    for (slot, (_, name, data_type)) in subset.iter().enumerate() {
        let array: ArrayRef = match data_type {
            DataType::Int64 => {
                let pairs = &sparse_int[slot];
                let mut builder = arrow::array::Int64Builder::with_capacity(rows);
                let mut cursor = 0usize;
                for row in 0..rows as u32 {
                    if cursor < pairs.len() && pairs[cursor].0 == row {
                        builder.append_value(pairs[cursor].1);
                        cursor += 1;
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }
            _ => {
                let pairs = &sparse_utf8[slot];
                let bytes: usize = pairs.iter().map(|(_, value)| value.len()).sum();
                let mut builder = arrow::array::StringBuilder::with_capacity(rows, bytes);
                let mut cursor = 0usize;
                for row in 0..rows as u32 {
                    if cursor < pairs.len() && pairs[cursor].0 == row {
                        builder.append_value(&pairs[cursor].1);
                        cursor += 1;
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }
        };
        by_name.insert(name.as_str(), array);
    }
    let arrays: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|field| Arc::clone(by_name.get(field.name().as_str()).expect("schema field")))
        .collect();
    let batch = arrow::record_batch::RecordBatch::try_new(Arc::clone(schema), arrays).unwrap();
    (batch, original_bytes)
}

async fn cmd_gen_wide(
    dir: &str,
    files: usize,
    rows_per_file: usize,
    width: usize,
    interleave: bool,
) -> Result<(), anyhow::Error> {
    anyhow::ensure!(
        width > WIDE_CORE_FIELDS,
        "width must exceed the {WIDE_CORE_FIELDS} core fields"
    );
    std::fs::create_dir_all(dir)?;
    let sparse_width = width - WIDE_CORE_FIELDS;
    let vocab = build_vocab();
    let cdf = build_zipf_cdf();
    let fts = vec!["message".to_string()];
    let base_ts_us = 1_787_000_000_000_000_i64;
    let mut total_data = 0u64;
    let mut total_original = 0u64;
    let mut union_fields: std::collections::HashSet<u32> = std::collections::HashSet::new();
    for file in 0..files {
        let subset_indices = wide_file_subset(file, sparse_width);
        union_fields.extend(subset_indices.iter().copied());
        // (universe index, name, type), plus THIS FILE's schema: `_timestamp`
        // first, then core + subset sorted by name (the same layout the
        // union schema uses, restricted to the file's fields)
        let subset: Vec<(usize, String, DataType)> = subset_indices
            .iter()
            .map(|&index| {
                let (name, data_type) = wide_sparse_field(index as usize, sparse_width);
                (index as usize, name, data_type)
            })
            .collect();
        let mut fields: Vec<Field> = vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("level", DataType::Utf8, true),
            Field::new("k8s_namespace_name", DataType::Utf8, true),
            Field::new("k8s_pod_name", DataType::Utf8, true),
            Field::new("k8s_container_name", DataType::Utf8, true),
            Field::new("status", DataType::Int64, true),
            Field::new("duration_ms", DataType::Int64, true),
        ];
        for (_, name, data_type) in &subset {
            fields.push(Field::new(name, data_type.clone(), true));
        }
        fields.sort_by(|a, b| a.name().cmp(b.name()));
        let mut all = vec![Field::new(TIMESTAMP_COL, DataType::Int64, false)];
        all.extend(fields);
        let schema = Arc::new(Schema::new(all));

        let mut rng = Rng(0x9E3779B97F4A7C15 ^ (file as u64 + 1).wrapping_mul(0xA24BAED4963EE407));
        // disjoint: later files hold strictly newer ranges. interleaved
        // (M23b twin): every file spans the SAME range on a files-strided
        // lattice — the k-way merge order round-robins across all inputs.
        let (file_base, ts_stride) = if interleave {
            (base_ts_us + file as i64, files as i64)
        } else {
            (base_ts_us + (file * rows_per_file * 10) as i64, 1i64)
        };
        let mut batches = Vec::new();
        let mut left = rows_per_file;
        let mut offset = 0usize;
        let mut original_bytes = 0u64;
        let mut slot_scratch: Vec<u32> = (0..subset.len() as u32).collect();
        while left > 0 {
            let n = left.min(BATCH_ROWS);
            let (batch, ob) = make_wide_batch(
                &schema,
                &subset,
                &mut slot_scratch,
                &mut rng,
                &vocab,
                &cdf,
                file_base + offset as i64 * ts_stride,
                ts_stride,
                n,
            );
            batches.push(batch);
            original_bytes += ob;
            left -= n;
            offset += n;
        }
        let table: Arc<dyn TableProvider> =
            Arc::new(MemTable::try_new(Arc::clone(&schema), vec![batches])?);
        let started = Instant::now();
        let result = openobserve_core::vix::core_writer::write_core_file_from_tables(
            &format!("m25-gen-{file}"),
            config::meta::stream::StreamType::Logs,
            Arc::clone(&schema),
            vec![table],
            &fts,
            &[],
            false,
            0,
        )
        .await?;
        anyhow::ensure!(
            result.stats.index_size == 0 && result.index.is_none(),
            "gen-wide corpus must be INDEX-OFF (#42) — got index_size={} (is \
             ZO_VIX_L0_INDEX_OFF_STREAM_TYPES=logs set?)",
            result.stats.index_size
        );
        let path = format!("{dir}/{file:04}.vix");
        std::fs::write(&path, &result.data)?;
        total_data += result.data.len() as u64;
        total_original += original_bytes;
        eprintln!(
            "gen-wide {path}: {} rows, {} cols, {:.1} MiB data (index-off), ~{:.1} MiB original, \
             {:.1}s",
            result.stats.row_count,
            schema.fields().len(),
            result.data.len() as f64 / (1024.0 * 1024.0),
            original_bytes as f64 / (1024.0 * 1024.0),
            started.elapsed().as_secs_f64(),
        );
    }
    eprintln!(
        "gen-wide done: {files} files, width {width} (union sparse fields touched: {}/{}), \
         {:.2} GiB data, ~{:.2} GiB original",
        union_fields.len(),
        sparse_width,
        total_data as f64 / (1024.0 * 1024.0 * 1024.0),
        total_original as f64 / (1024.0 * 1024.0 * 1024.0),
    );
    Ok(())
}

/// One batch of log-shaped rows starting at `base_ts_us` (ascending
/// `ts_stride`-µs steps; the builder re-sorts DESC like the real move job).
/// `ts_stride` is 1 for the disjoint corpus and `files` for the interleaved
/// one (round-robin timestamps). Returns the batch plus the summed message
/// bytes (for the original-size estimate).
fn make_batch(
    schema: &Arc<Schema>,
    rng: &mut Rng,
    vocab: &[String],
    cdf: &[f64],
    base_ts_us: i64,
    ts_stride: i64,
    rows: usize,
) -> (arrow::record_batch::RecordBatch, u64) {
    let services: Vec<String> = (0..30).map(|i| format!("svc-checkout-{i}")).collect();
    let pods: Vec<String> = (0..200)
        .map(|i| format!("api-deploy-{i:03}-x{}", i % 7))
        .collect();
    let mut ts = Vec::with_capacity(rows);
    let mut code = Vec::with_capacity(rows);
    let mut env = Vec::with_capacity(rows);
    let mut level = Vec::with_capacity(rows);
    let mut message = Vec::with_capacity(rows);
    let mut pod = Vec::with_capacity(rows);
    let mut service = Vec::with_capacity(rows);
    let mut message_bytes = 0u64;
    for row in 0..rows {
        ts.push(base_ts_us + row as i64 * ts_stride);
        code.push([200i64, 200, 200, 204, 400, 500][rng.below(6) as usize]);
        env.push(["prod", "prod", "prod", "staging"][rng.below(4) as usize].to_string());
        level.push(["info", "info", "info", "warn", "error"][rng.below(5) as usize].to_string());
        let words = 20 + rng.below(41) as usize; // 20..=60
        let mut text = String::with_capacity(words * 9);
        for w in 0..words {
            if w > 0 {
                text.push(' ');
            }
            text.push_str(zipf_word(rng, vocab, cdf));
        }
        message_bytes += text.len() as u64;
        message.push(text);
        pod.push(pods[rng.below(200) as usize].clone());
        service.push(services[rng.below(30) as usize].clone());
    }
    let batch = arrow::record_batch::RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(ts)) as ArrayRef,
            Arc::new(Int64Array::from(code)),
            Arc::new(StringArray::from(env)),
            Arc::new(StringArray::from(level)),
            Arc::new(StringArray::from(message)),
            Arc::new(StringArray::from(pod)),
            Arc::new(StringArray::from(service)),
        ],
    )
    .unwrap();
    (batch, message_bytes)
}

async fn cmd_gen(
    dir: &str,
    files: usize,
    rows_per_file: usize,
    interleave: bool,
    high_card: bool,
) -> Result<(), anyhow::Error> {
    std::fs::create_dir_all(dir)?;
    let schema = if high_card {
        hc_schema()
    } else {
        logs_schema()
    };
    let vocab = build_vocab();
    let cdf = build_zipf_cdf();
    let fts = vec!["message".to_string()];
    let base_ts_us = 1_787_000_000_000_000_i64;
    let mut total_data = 0u64;
    let mut total_original_estimate = 0u64;
    for file in 0..files {
        let mut rng = Rng(0x9E3779B97F4A7C15 ^ (file as u64 + 1).wrapping_mul(0xA24BAED4963EE407));
        // disjoint: later files hold strictly newer ranges (the common group
        // shape M23 was proven on). interleaved: every file spans the SAME
        // range, file f owning residue f of a files-strided lattice — the
        // k-way DESC merge order then round-robins across every input
        // (unique timestamps, so the order is exact and deterministic).
        let (file_base, ts_stride) = if interleave {
            (base_ts_us + file as i64, files as i64)
        } else {
            (base_ts_us + (file * rows_per_file * 10) as i64, 1i64)
        };
        let mut batches = Vec::new();
        let mut left = rows_per_file;
        let mut offset = 0usize;
        let mut message_bytes = 0u64;
        while left > 0 {
            let n = left.min(BATCH_ROWS);
            let (batch, mb) = if high_card {
                make_hc_batch(
                    &schema,
                    &mut rng,
                    &vocab,
                    &cdf,
                    file_base + offset as i64,
                    (file * rows_per_file + offset) as u64,
                    n,
                )
            } else {
                make_batch(
                    &schema,
                    &mut rng,
                    &vocab,
                    &cdf,
                    file_base + offset as i64 * ts_stride,
                    ts_stride,
                    n,
                )
            };
            batches.push(batch);
            message_bytes += mb;
            left -= n;
            offset += n;
        }
        let table: Arc<dyn TableProvider> =
            Arc::new(MemTable::try_new(Arc::clone(&schema), vec![batches])?);
        let started = Instant::now();
        let result = openobserve_core::vix::core_writer::write_core_file_from_tables(
            &format!("m23-gen-{file}"),
            config::meta::stream::StreamType::Logs,
            Arc::clone(&schema),
            vec![table],
            &fts,
            &[],
            false,
            0,
        )
        .await?;
        anyhow::ensure!(
            result.stats.index_size == 0 && result.index.is_none(),
            "gen corpus must be INDEX-OFF (#42) — got index_size={} (is \
             ZO_VIX_L0_INDEX_OFF_STREAM_TYPES=logs set?)",
            result.stats.index_size
        );
        let path = format!("{dir}/{file:04}.vix");
        std::fs::write(&path, &result.data)?;
        // synthesized `_source` JSON ≈ summed value bytes + fixed
        // fields/keys+syntax per row (~140 B for the logs schema, ~185 B
        // for the wider hc schema) — the "original bytes"
        let original = message_bytes + result.stats.row_count * if high_card { 185 } else { 140 };
        total_data += result.data.len() as u64;
        total_original_estimate += original;
        eprintln!(
            "gen {path}: {} rows, {:.1} MiB data (index-off), ~{:.1} MiB original, {:.1}s",
            result.stats.row_count,
            result.data.len() as f64 / (1024.0 * 1024.0),
            original as f64 / (1024.0 * 1024.0),
            started.elapsed().as_secs_f64(),
        );
    }
    eprintln!(
        "gen done: {files} files, {:.2} GiB data, ~{:.2} GiB original",
        total_data as f64 / (1024.0 * 1024.0 * 1024.0),
        total_original_estimate as f64 / (1024.0 * 1024.0 * 1024.0),
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// merge (ranged inputs, the compactor's exact entry)
// ---------------------------------------------------------------------------

/// Ranged reads from a local corpus file — the twin of the compactor's
/// cache-ladder source, so the merge measures the true ranged input profile
/// (no whole-file Bytes in RAM).
struct FileRangeSource {
    name: String,
    file: std::fs::File,
    len: u64,
}

impl vortex_index::VixRangeSource for FileRangeSource {
    fn len(&self) -> u64 {
        self.len
    }

    fn fetch(
        &self,
        range: std::ops::Range<u64>,
    ) -> futures::future::BoxFuture<'static, anyhow::Result<bytes::Bytes>> {
        use std::os::unix::fs::FileExt;
        let mut buf = vec![0u8; (range.end - range.start) as usize];
        let result = self
            .file
            .read_exact_at(&mut buf, range.start)
            .map(|()| bytes::Bytes::from(buf))
            .map_err(|e| anyhow::anyhow!("read {} range {range:?}: {e}", self.name));
        Box::pin(futures::future::ready(result))
    }

    fn describe(&self) -> String {
        self.name.clone()
    }
}

fn load_inputs(
    dir: &str,
) -> Result<Vec<openobserve_core::vix::core_writer::MergeInput>, anyhow::Error> {
    let mut paths: Vec<std::path::PathBuf> = std::fs::read_dir(dir)?
        .filter_map(|entry| {
            let path = entry.unwrap().path();
            path.extension()
                .is_some_and(|ext| ext == "vix")
                .then_some(path)
        })
        .collect();
    paths.sort();
    anyhow::ensure!(!paths.is_empty(), "no .vix files in {dir:?}");
    paths
        .iter()
        .map(|path| {
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            let file = std::fs::File::open(path)?;
            let len = file.metadata()?.len();
            let source: Arc<dyn vortex_index::VixRangeSource> = Arc::new(FileRangeSource {
                name: name.clone(),
                file,
                len,
            });
            // index-off corpus: no .vxi sidecars exist
            Ok((name, source, None))
        })
        .collect()
}

fn cmd_merge(
    dir: &str,
    out: &str,
    high_card: bool,
    flip: bool,
    wide_width: Option<usize>,
) -> Result<(), anyhow::Error> {
    let mib = |bytes: usize| bytes as f64 / (1024.0 * 1024.0);
    let inputs = load_inputs(dir)?;
    let total_bytes: u64 = inputs.iter().map(|(_, data, _)| data.len()).sum();
    // *-flip: the CURRENT stream schema types one Int64 field (`code` for
    // the logs corpus, `httpstatus` for the hc corpus, the always-present
    // `status` for the wide corpus) as Utf8 while every stored file has
    // Int64 — schema evolution. build_merge_plan's preserved
    // types follow the latest schema, so the flip (a) fails every input's
    // heal-passthrough qualification (docs_widen_plan rejects type flips ->
    // qualified == 0 -> the heal arm and its #51c-c concatenation order are
    // OFF) and (b) disables #46 column derivation — the merge runs the
    // STANDARD rebuild: `_source`-derived terms over the k-way merge order
    // (interleaved for the gen-il corpus). Prod's OOM signature.
    let base_schema = match wide_width {
        Some(width) => wide_union_schema(width),
        None if high_card => hc_schema(),
        None => logs_schema(),
    };
    let flip_field = if wide_width.is_some() {
        "status"
    } else if high_card {
        "httpstatus"
    } else {
        "code"
    };
    let latest_schema = if flip {
        let flipped: Vec<Field> = base_schema
            .fields()
            .iter()
            .map(|field| {
                if field.name() == flip_field {
                    Field::new(flip_field, DataType::Utf8, true)
                } else {
                    field.as_ref().clone()
                }
            })
            .collect();
        Schema::new(flipped)
    } else {
        base_schema.as_ref().clone()
    };
    let fts = vec!["message".to_string()];
    eprintln!(
        "[phase] merge start: {} ranged inputs / {:.1} MiB data, fts={fts:?}, \
         high_card={high_card}, flip={flip}, wide_width={wide_width:?}, plist_min_docs={} (env)",
        inputs.len(),
        mib(total_bytes as usize),
        std::env::var("ZO_VIX_PLIST_MIN_DOCS").unwrap_or_default(),
    );

    let started = Instant::now();
    let result = openobserve_core::vix::core_writer::merge_core_files(
        config::meta::stream::StreamType::Logs,
        &inputs,
        &latest_schema,
        &fts,
        &[],
    )?;
    let merge_elapsed = started.elapsed();
    let out_len = result.output.len();
    if let Some(index) = &result.index {
        std::fs::write(std::path::Path::new(out).with_extension("vxi"), index)?;
    }
    match result.output {
        vortex_index::VixOutput::Bytes(data) => std::fs::write(out, &data)?,
        vortex_index::VixOutput::Spooled { file, .. } => {
            if let Err(error) = file.persist(out) {
                std::fs::copy(error.file.path(), out).map_err(|e| {
                    anyhow::anyhow!(
                        "persist spool: rename failed ({}), copy fallback failed too: {e}",
                        error.error
                    )
                })?;
            }
        }
    }
    eprintln!(
        "[phase] merge done: {merge_elapsed:.2?}  used_index_merge={}  docs_batches={}  \
         docs_passthrough_inputs={}  concat_order={}  out {:.1} MiB \
         ({} rows, {} terms, index {:.1} MiB, docs {:.1} MiB)",
        result.used_index_merge,
        result.docs_batches,
        result.docs_passthrough_inputs,
        result.concat_order,
        mib(out_len as usize),
        result.stats.row_count,
        result.stats.term_count,
        mib(result.stats.index_size as usize),
        mib(result.stats.docs_size as usize),
    );
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    eprintln!(
        "[phase] peak: vmhwm={}MB live={}MB",
        proc_status_kb(&status, "VmHWM:") / 1024,
        LIVE_BYTES.load(Ordering::Relaxed) / (1024 * 1024),
    );
    Ok(())
}

/// m25: storage-side width diagnostic — per-column leaf count/bytes of one
/// .vix docs blob, top-N by bytes plus totals.
fn cmd_inspect(path: &str, top: usize) -> Result<(), anyhow::Error> {
    let file = std::fs::File::open(path)?;
    let len = file.metadata()?.len();
    let source: Arc<dyn vortex_index::VixRangeSource> = Arc::new(FileRangeSource {
        name: path.to_string(),
        file,
        len,
    });
    let docs = vortex_index::VixDocs::open_ranged(source)?;
    let mut report = docs.leaf_report()?;
    let total_leaves: u64 = report.iter().map(|(_, l, _)| l).sum();
    let total_bytes: u64 = report.iter().map(|(_, _, b)| b).sum();
    report.sort_by_key(|(_, _, bytes)| std::cmp::Reverse(*bytes));
    eprintln!(
        "inspect {path}: {} rows, {} cols, docs_blob={} MiB, {total_leaves} leaves, \
         {:.1} MiB leaf bytes",
        docs.row_count(),
        docs.schema().fields().len(),
        docs.docs_blob_len() / (1024 * 1024),
        total_bytes as f64 / (1024.0 * 1024.0),
    );
    for (name, leaves, bytes) in report.iter().take(top) {
        eprintln!(
            "  {name}: {leaves} leaves, {:.2} MiB ({:.1} KiB/leaf)",
            *bytes as f64 / (1024.0 * 1024.0),
            *bytes as f64 / (*leaves).max(1) as f64 / 1024.0,
        );
    }
    // aggregate the tail
    let tail: u64 = report.iter().skip(top).map(|(_, _, b)| b).sum();
    let tail_leaves: u64 = report.iter().skip(top).map(|(_, l, _)| l).sum();
    eprintln!(
        "  ... remaining {} cols: {tail_leaves} leaves, {:.1} MiB",
        report.len().saturating_sub(top),
        tail as f64 / (1024.0 * 1024.0)
    );
    Ok(())
}

/// M24 Part B — the floor ratchet: run `n` sequential merges of the SAME
/// corpus in ONE process and sample the INTER-OP floor (RSS + live bytes
/// after each merge's outputs drop), attributing allocator retention
/// (live flat, RSS floor climbing) vs true growth (live climbing).
fn cmd_soak(dir: &str, out_dir: &str, n: usize, high_card: bool) -> Result<(), anyhow::Error> {
    std::fs::create_dir_all(out_dir)?;
    for iter in 0..n {
        let out = format!("{out_dir}/soak-{iter:02}.vix");
        cmd_merge(dir, &out, high_card, false, None)?;
        // outputs dropped (cmd_merge writes + drops); small settle so the
        // sampler catches the floor between ops
        std::thread::sleep(std::time::Duration::from_millis(2500));
        let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
        eprintln!(
            "[soak] iter {iter} floor: vmrss={}MB vmhwm={}MB live={}MB",
            proc_status_kb(&status, "VmRSS:") / 1024,
            proc_status_kb(&status, "VmHWM:") / 1024,
            LIVE_BYTES.load(Ordering::Relaxed) / (1024 * 1024),
        );
        // keep the disk footprint flat: only the last output stays
        if iter > 0 {
            let _ = std::fs::remove_file(format!("{out_dir}/soak-{:02}.vix", iter - 1));
            let _ = std::fs::remove_file(format!("{out_dir}/soak-{:02}.vxi", iter - 1));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// plumbing
// ---------------------------------------------------------------------------

/// Stderr logger printing EVERYTHING (the engine's phase lines are the
/// markers; the m23 worktree instrumentation logs at info).
struct StderrLogger;
impl log::Log for StderrLogger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.target().contains("vix")
            || metadata.target().starts_with("vortex_index")
            || metadata.target().starts_with("openobserve_core")
            || metadata.level() <= log::Level::Warn
    }
    fn log(&self, record: &log::Record) {
        if self.enabled(record.metadata()) {
            eprintln!("[{}] {}", record.level(), record.args());
        }
    }
    fn flush(&self) {}
}

/// Ensure every `key=value` is in the environment, re-exec'ing ONCE with all
/// of them set when any is missing (env-backed config is process-global; a
/// fresh process image is the safe way to pin knobs).
fn ensure_env_many(pairs: &[(&str, &str)]) {
    let missing: Vec<_> = pairs
        .iter()
        .filter(|(k, v)| !std::env::var(k).map(|have| have == *v).unwrap_or(false))
        .collect();
    if missing.is_empty() {
        return;
    }
    use std::os::unix::process::CommandExt;
    let exe = std::env::current_exe().expect("current_exe");
    let mut cmd = std::process::Command::new(exe);
    cmd.args(std::env::args_os().skip(1));
    for (k, v) in pairs {
        cmd.env(k, v);
        eprintln!("re-exec with {k}={v}");
    }
    let error = cmd.exec();
    panic!("re-exec failed: {error}");
}

fn main() -> Result<(), anyhow::Error> {
    static LOGGER: StderrLogger = StderrLogger;
    if log::set_logger(&LOGGER).is_ok() {
        log::set_max_level(log::LevelFilter::Debug);
    }
    let args: Vec<String> = std::env::args().collect();
    let scratch = std::env::var("M23_SCRATCH")
        .unwrap_or_else(|_| "/home/zhichen/work/m25/m25-data".to_string());
    let mode = args.get(1).map(String::as_str);
    match mode {
        Some(gen_mode @ ("gen" | "gen-il" | "gen-hc")) => {
            let dir = args
                .get(2)
                .expect("gen <dir> <files> <rows_per_file>")
                .clone();
            let files: usize = args.get(3).expect("files").parse()?;
            let rows: usize = args.get(4).expect("rows_per_file").parse()?;
            ensure_env_many(&[
                ("ZO_VIX_L0_INDEX_OFF_STREAM_TYPES", "logs"),
                ("ZO_DATA_DIR", &format!("{scratch}/engine-data")),
            ]);
            spawn_rss_sampler();
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(cmd_gen(
                    &dir,
                    files,
                    rows,
                    gen_mode == "gen-il",
                    gen_mode == "gen-hc",
                ))
        }
        Some(gen_mode @ ("gen-wide" | "gen-wide-il")) => {
            let dir = args
                .get(2)
                .expect("gen-wide <dir> <files> <rows_per_file> <width>")
                .clone();
            let files: usize = args.get(3).expect("files").parse()?;
            let rows: usize = args.get(4).expect("rows_per_file").parse()?;
            let width: usize = args.get(5).expect("width").parse()?;
            ensure_env_many(&[
                ("ZO_VIX_L0_INDEX_OFF_STREAM_TYPES", "logs"),
                ("ZO_DATA_DIR", &format!("{scratch}/engine-data")),
            ]);
            spawn_rss_sampler();
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(cmd_gen_wide(
                    &dir,
                    files,
                    rows,
                    width,
                    gen_mode == "gen-wide-il",
                ))
        }
        Some(merge @ ("merge" | "merge-flip" | "merge-hc" | "merge-hc-flip")) => {
            let dir = args.get(2).expect("merge <dir> <out.vix>").clone();
            let out = args.get(3).expect("out.vix").clone();
            ensure_env_many(&[
                // prod pins (the L0 knob is a BUILD knob — irrelevant to the
                // merge, but pinned anyway for prod parity)
                ("ZO_VIX_L0_INDEX_OFF_STREAM_TYPES", "logs"),
                ("ZO_VIX_PLIST_MIN_DOCS", "8192"),
                ("ZO_DATA_DIR", &format!("{scratch}/engine-data")),
            ]);
            spawn_rss_sampler();
            cmd_merge(
                &dir,
                &out,
                merge.starts_with("merge-hc"),
                merge.ends_with("flip"),
                None,
            )
        }
        Some(merge @ ("merge-wide" | "merge-wide-flip")) => {
            let dir = args
                .get(2)
                .expect("merge-wide <dir> <out.vix> <width>")
                .clone();
            let out = args.get(3).expect("out.vix").clone();
            let width: usize = args.get(4).expect("width").parse()?;
            ensure_env_many(&[
                ("ZO_VIX_L0_INDEX_OFF_STREAM_TYPES", "logs"),
                ("ZO_VIX_PLIST_MIN_DOCS", "8192"),
                ("ZO_DATA_DIR", &format!("{scratch}/engine-data")),
            ]);
            spawn_rss_sampler();
            cmd_merge(&dir, &out, false, merge.ends_with("flip"), Some(width))
        }
        Some("inspect") => {
            let path = args.get(2).expect("inspect <file.vix> [top]").clone();
            let top: usize = args.get(3).map(|s| s.parse()).transpose()?.unwrap_or(12);
            cmd_inspect(&path, top)
        }
        Some("hash-col") => {
            // value-level equivalence check: FNV over (validity, value bytes)
            // of one column across the whole docs blob, in row order
            let path = args.get(2).expect("hash-col <file.vix> <column>").clone();
            let column = args.get(3).expect("column").clone();
            let file = std::fs::File::open(&path)?;
            let len = file.metadata()?.len();
            let source: Arc<dyn vortex_index::VixRangeSource> = Arc::new(FileRangeSource {
                name: path.clone(),
                file,
                len,
            });
            let docs = vortex_index::VixDocs::open_ranged(source)?;
            let mut hash: u64 = 0xcbf29ce484222325;
            let mut rows = 0u64;
            let mut present = 0u64;
            let fnv = |hash: &mut u64, bytes: &[u8]| {
                for &b in bytes {
                    *hash ^= b as u64;
                    *hash = hash.wrapping_mul(0x100000001b3);
                }
            };
            docs.scan_docs(Some(&[column.clone()]), None, None, &mut |batch| {
                let col = batch
                    .column_by_name(&column)
                    .ok_or_else(|| anyhow::anyhow!("column {column:?} missing"))?;
                let col = arrow::compute::cast(col, &DataType::Utf8)
                    .or_else(|_| arrow::compute::cast(col, &DataType::Utf8))?;
                let col = col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| anyhow::anyhow!("not a string column"))?;
                use arrow::array::Array as _;
                for i in 0..col.len() {
                    rows += 1;
                    if col.is_valid(i) {
                        present += 1;
                        fnv(&mut hash, &[1]);
                        fnv(&mut hash, col.value(i).as_bytes());
                    } else {
                        fnv(&mut hash, &[0]);
                    }
                }
                Ok(())
            })?;
            println!("hash-col {path} {column}: rows={rows} present={present} fnv={hash:016x}");
            Ok(())
        }
        Some("tree") => {
            let path = args.get(2).expect("tree <file.vix> <column>").clone();
            let column = args.get(3).expect("column").clone();
            let file = std::fs::File::open(&path)?;
            let len = file.metadata()?.len();
            let source: Arc<dyn vortex_index::VixRangeSource> = Arc::new(FileRangeSource {
                name: path.clone(),
                file,
                len,
            });
            let docs = vortex_index::VixDocs::open_ranged(source)?;
            println!("{}", docs.column_layout_tree(&column)?);
            // also the first stored chunks' ARRAY encoding trees + nbytes
            // (what the leaves actually hold)
            for line in docs.column_chunk_encodings(&column, 3)? {
                println!("{line}");
            }
            // raw leaf bytes: entropy + head hexdump of the largest leaf
            let extents = docs.column_leaf_extents(&column)?;
            if let Some(&(off, len)) = extents.iter().max_by_key(|(_, len)| *len) {
                use std::os::unix::fs::FileExt;
                let file = std::fs::File::open(&path)?;
                // docs blob starts after the 4-byte container MAGIC
                let mut buf = vec![0u8; len as usize];
                file.read_exact_at(&mut buf, 4 + off)?;
                let mut counts = [0u64; 256];
                for &b in &buf {
                    counts[b as usize] += 1;
                }
                let entropy: f64 = counts
                    .iter()
                    .filter(|&&c| c > 0)
                    .map(|&c| {
                        let p = c as f64 / len as f64;
                        -p * p.log2()
                    })
                    .sum();
                let zeros = counts[0];
                println!(
                    "largest leaf: offset={off} len={len} entropy={entropy:.2} bits/B zeros={:.1}% head={:02x?}",
                    zeros as f64 * 100.0 / len as f64,
                    &buf[..64.min(buf.len())]
                );
            }
            Ok(())
        }
        Some(soak @ ("soak" | "soak-hc")) => {
            let dir = args.get(2).expect("soak <dir> <out_dir> <n>").clone();
            let out_dir = args.get(3).expect("out_dir").clone();
            let n: usize = args.get(4).expect("n").parse()?;
            ensure_env_many(&[
                ("ZO_VIX_L0_INDEX_OFF_STREAM_TYPES", "logs"),
                ("ZO_VIX_PLIST_MIN_DOCS", "8192"),
                ("ZO_DATA_DIR", &format!("{scratch}/engine-data")),
            ]);
            spawn_rss_sampler();
            cmd_soak(&dir, &out_dir, n, soak == "soak-hc")
        }
        _ => {
            eprintln!(
                "usage: m23_rss_repro gen|gen-il|gen-hc <dir> <files> <rows_per_file> | \
                 gen-wide <dir> <files> <rows_per_file> <width> | \
                 merge|merge-flip|merge-hc|merge-hc-flip <dir> <out.vix> | \
                 merge-wide|merge-wide-flip <dir> <out.vix> <width> | \
                 soak|soak-hc <dir> <out_dir> <n>"
            );
            std::process::exit(2);
        }
    }
}
