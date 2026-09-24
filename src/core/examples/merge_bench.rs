//! Compaction-merge benchmark with peak-RSS reporting — the A/B harness for
//! the streamed docs-blob encode (bounded-memory merge).
//!
//! Subcommands:
//!
//!   gen <dir> <files> <rows_per_file> [--heal] [--overlap]
//!       Build a corpus of move-job-shaped core files (the REAL move
//!       builder, `write_core_file_from_tables`, prod traces stream
//!       settings; v2: every present field is a docs column) with DISJOINT
//!       descending time ranges — the common compaction group shape. With
//!       `--heal` the corpus is INDEX-OFF (#42 L0 shape, via
//!       ZO_VIX_L0_INDEX_OFF_STREAM_TYPES; the bench re-execs itself with
//!       the env set) — merge such a corpus (a single file is prod's
//!       dominant heal) and the indexed logs plan takes the rebuild that
//!       BUILDS the index. With `--overlap` every file covers the SAME
//!       time range (the concurrently-written-L0 shape: fully overlapping
//!       timestamps, `contiguous_offsets` None) — the corpus the #51c-c
//!       concatenation-order merge exists for. With `--vary-schema` (M17)
//!       per-file column UNIONS differ (each file drops a deterministic
//!       couple of the optional columns) — the prod gen-1 shape whose
//!       merges re-encoded every byte before the widening chunk copy.
//!       (`--narrow` is retired: v2 has no narrow docs schema.)
//!       With `--type-drift`, `status_code` cycles through Utf8, Boolean,
//!       Float64, and Int64 physical columns; file 0 is Utf8, making the
//!       derived latest schema exercise Boolean/Float/Int64 -> Utf8.
//!
//!   merge <dir> <out.vix> [--rebuild]
//!       Load every corpus file fully into memory (exactly like the
//!       compactor worker) and run `merge_core_files`; prints load/merge
//!       wall, output stats, and the process peak RSS (`VmHWM`) — run one
//!       `merge` per process so the peak is the merge's. The #51c
//!       docs-chunk passthrough and the #51c-c concatenation order are the
//!       DEFAULT merge shapes now (no knobs): a disjoint corpus copies
//!       chunks, an overlapping corpus concatenates (`row_order=concat` —
//!       compare such outputs with `--multiset`, never the row-order
//!       digest), and `--rebuild` exercises the heal passthrough (index
//!       built from the decoded scan, docs chunks copied verbatim).
//!
//!   sidecar <dir> [--stored-schema] [--traces]
//!       Rebuild only the detached index for the directory's single core
//!       file, without assembling or writing a new docs object. This
//!       isolates the current column-derived term/index path.
//!   compare [--multiset] [--docs-only] [--ignore-source] <a.vix> <b.vix>
//!       Assert reader-visible equality of two merge outputs: row count,
//!       term stream and every docs column. Default mode: term keys, doc
//!       counts AND postings stream through one hasher, and the docs hash
//!       folds each COLUMN's values in row order through its own hasher
//!       (combined in sorted column order at the end) — chunk-boundary-
//!       independent but ROW-ORDER-dependent: outputs of different merge
//!       paths with the same row order (fast vs rebuild vs #51c
//!       passthrough) compare by logical content. `--multiset` (#51c-c):
//!       ORDER-INSENSITIVE content equality for outputs whose row order
//!       legitimately differs (a concat-order output vs a sorted one) —
//!       per-ROW content hashes folded commutatively, and the term stream
//!       hashed as (key, doc_count) only (postings doc ids are positions;
//!       the per-term doc_count and the row multiset pin the content).
//!
//! Typical A/B: `gen` once, build this example at the old and new code,
//! run `merge` with each binary into different outputs, `compare` them.
//!
//! Indexed logs capacity benchmark (synthetic data only):
//!   gen-logs <dir> <file_number> <original_mib>
//!       Generate ONE indexed, body-FTS log file per process. Its JSON
//!       manifest records measured JSON bytes (excluding record newlines),
//!       not an assumed bytes-per-row multiplier. Trim the final batch
//!       so the corpus remains eligible for its original-size cap.
//!   merge <dir> <out.vix> --indexed-only --stored-schema
//!       Use the production entry point that refuses rebuild fallback.
//!       Run each merge under `/usr/bin/time -l` on macOS (`-v` on Linux)
//!       for process peak RSS, independently of generation/verification.
//!   verify-logs <input_dir> <out.vix>
//!       Check row multiset and all term document-count digests against
//!       the input union, plus FTS postings against every decoded row.

use std::{
    hash::{DefaultHasher, Hash, Hasher},
    sync::Arc,
    time::Instant,
};

use arrow::{
    array::{Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
};
use datafusion::{catalog::TableProvider, datasource::MemTable};
use vortex_index::{VixDocs, VixQuery, VixReader};

// Match the production allocator; system malloc retention can otherwise
// dominate the large-merge RSS comparison.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TIMESTAMP_COL: &str = "_timestamp";
const BATCH_ROWS: usize = 8192;
const LOG_PROBES: [&str; 6] = [
    "request",
    "requestcompleted",
    "upstreamtimeout",
    "retryexhausted",
    "healthprobe",
    "absentvalidationtoken",
];

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

    fn hex(&mut self, chars: usize) -> String {
        let mut s = String::with_capacity(chars);
        for _ in 0..chars {
            s.push(char::from_digit((self.below(16)) as u32, 16).unwrap());
        }
        s
    }
}

fn spans_schema() -> Arc<Schema> {
    let utf8 = |name: &str| Field::new(name, DataType::Utf8, true);
    Arc::new(Schema::new(vec![
        Field::new(TIMESTAMP_COL, DataType::Int64, false),
        Field::new("duration", DataType::Int64, true),
        Field::new("status_code", DataType::Int64, true),
        utf8("trace_id"),
        utf8("span_id"),
        utf8("service_name"),
        utf8("operation_name"),
        utf8("span_status"),
        utf8("span_kind"),
        utf8("http.url"),
        utf8("http.method"),
        utf8("server.address"),
        utf8("service_pod_name"),
        utf8("service_service.version"),
        utf8("db.query.text"),
    ]))
}

/// One batch of trace-shaped rows starting at `base_ts_us` (ascending 1µs
/// steps; the builder re-sorts DESC like the real move job).
fn make_batch(
    schema: &Arc<Schema>,
    rng: &mut Rng,
    base_ts_us: i64,
    rows: usize,
) -> arrow::record_batch::RecordBatch {
    let services: Vec<String> = (0..30).map(|i| format!("api-service-{i}-otel")).collect();
    let operations: Vec<String> = (0..300)
        .map(|i| format!("POST /api/v1/resource_{i}/action"))
        .collect();
    let mut ts = Vec::with_capacity(rows);
    let mut duration = Vec::with_capacity(rows);
    let mut status_code = Vec::with_capacity(rows);
    let mut trace_id = Vec::with_capacity(rows);
    let mut span_id = Vec::with_capacity(rows);
    let mut service = Vec::with_capacity(rows);
    let mut operation = Vec::with_capacity(rows);
    let mut span_status = Vec::with_capacity(rows);
    let mut span_kind = Vec::with_capacity(rows);
    let mut url = Vec::with_capacity(rows);
    let mut method = Vec::with_capacity(rows);
    let mut server = Vec::with_capacity(rows);
    let mut pod = Vec::with_capacity(rows);
    let mut version = Vec::with_capacity(rows);
    let mut query: Vec<Option<String>> = Vec::with_capacity(rows);
    for row in 0..rows {
        ts.push(base_ts_us + row as i64);
        duration.push((rng.below(5_000_000)) as i64);
        status_code.push([0i64, 200, 200, 200, 500][rng.below(5) as usize]);
        trace_id.push(rng.hex(32));
        span_id.push(rng.hex(16));
        service.push(services[rng.below(30) as usize].clone());
        operation.push(operations[rng.below(300) as usize].clone());
        span_status.push(["UNSET", "OK", "ERROR"][rng.below(3) as usize].to_string());
        span_kind.push(["SPAN_KIND_CLIENT", "SPAN_KIND_SERVER"][rng.below(2) as usize].to_string());
        url.push(format!(
            "https://gw.internal/api/v1/items/{}/parts/{}?trace={}",
            rng.below(100_000),
            rng.below(1000),
            rng.hex(8)
        ));
        method.push(["GET", "POST", "PUT"][rng.below(3) as usize].to_string());
        server.push(format!("svc-{}.us-east-1.internal:3306", rng.below(40)));
        pod.push(format!("api-deploy-{}-{}", rng.hex(9), rng.hex(5)));
        version.push(format!("vprod-7.{}.{}", rng.below(20), rng.below(99)));
        query.push((rng.below(3) == 0).then(|| {
            format!(
                "SELECT id, state, updated_at FROM task_queue_{} WHERE shard = {} AND state IN \
                 ('pending','running') ORDER BY updated_at DESC LIMIT {}",
                rng.below(60),
                rng.below(512),
                1 + rng.below(200),
            )
        }));
    }
    arrow::record_batch::RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(ts)) as ArrayRef,
            Arc::new(Int64Array::from(duration)),
            Arc::new(Int64Array::from(status_code)),
            Arc::new(StringArray::from(trace_id)),
            Arc::new(StringArray::from(span_id)),
            Arc::new(StringArray::from(service)),
            Arc::new(StringArray::from(operation)),
            Arc::new(StringArray::from(span_status)),
            Arc::new(StringArray::from(span_kind)),
            Arc::new(StringArray::from(url)),
            Arc::new(StringArray::from(method)),
            Arc::new(StringArray::from(server)),
            Arc::new(StringArray::from(pod)),
            Arc::new(StringArray::from(version)),
            Arc::new(StringArray::from(query)),
        ],
    )
    .unwrap()
}

fn with_status_code_type(
    batch: arrow::record_batch::RecordBatch,
    target: &DataType,
) -> Result<arrow::record_batch::RecordBatch, anyhow::Error> {
    let status_index = batch.schema().index_of("status_code")?;
    let mut fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    fields[status_index] = Field::new("status_code", target.clone(), true);
    let mut columns = batch.columns().to_vec();
    let rows = batch.num_rows();
    columns[status_index] = match target {
        DataType::Utf8 => arrow::compute::cast(&columns[status_index], target)?,
        DataType::Boolean => Arc::new(BooleanArray::from(
            (0..rows).map(|row| row % 2 == 0).collect::<Vec<_>>(),
        )),
        DataType::Float64 => Arc::new(Float64Array::from_iter_values(
            (0..rows).map(|row| [200.5, 400.25, 500.75][row % 3]),
        )),
        DataType::Int64 => Arc::new(Int64Array::from_iter_values(
            (0..rows).map(|row| [200, 400, 500][row % 3]),
        )),
        other => anyhow::bail!("unsupported type-drift benchmark type {other:?}"),
    };
    Ok(arrow::record_batch::RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

/// Prod `default` traces stream settings (v2: every present field is a
/// docs column — there is no column-store list).
fn stream_settings() -> (Vec<String>, Vec<String>) {
    let fts: Vec<String> = vec![];
    let bloom: Vec<String> = vec!["trace_id".to_string()];
    (fts, bloom)
}

fn logs_schema() -> Arc<Schema> {
    let mut fields = vec![Field::new(TIMESTAMP_COL, DataType::Int64, false)];
    for name in [
        "body",
        "severity",
        "service_name",
        "k8s_namespace_name",
        "k8s_pod_name",
        "k8s_container_name",
        "trace_id",
        "request_id",
    ] {
        fields.push(Field::new(name, DataType::Utf8, true));
    }
    fields.push(Field::new("http_status_code", DataType::Int64, true));
    Arc::new(Schema::new(fields))
}

fn make_logs_batch(
    schema: &Arc<Schema>,
    rng: &mut Rng,
    file_number: usize,
    offset: usize,
) -> Result<arrow::record_batch::RecordBatch, anyhow::Error> {
    const DETAILS: [&str; 8] = [
        "upstream connection established; response headers received; payload decoded successfully",
        "authorization policy evaluated; tenant quota checked; request context propagated",
        "database transaction committed; connection returned to pool; replica state synchronized",
        "cache lookup completed; object metadata validated; response compression enabled",
        "worker task scheduled; queue partition selected; acknowledgement received from broker",
        "distributed trace context attached; span attributes exported; metrics batch accepted",
        "configuration revision verified; service discovery refreshed; endpoint health checked",
        "storage object fetched; checksum verified; result serialization completed",
    ];
    let mut timestamps = Vec::with_capacity(BATCH_ROWS);
    let mut strings: Vec<Vec<String>> = (0..8).map(|_| Vec::with_capacity(BATCH_ROWS)).collect();
    let mut statuses = Vec::with_capacity(BATCH_ROWS);
    for row in 0..BATCH_ROWS {
        // One second per file leaves disjoint ranges for the intended
        // 256 MiB inputs; fail below if a requested file would exceed it.
        let row_number = offset + row;
        anyhow::ensure!(
            row_number < 1_000_000,
            "log file exceeds its timestamp range"
        );
        timestamps.push(1_789_502_400_000_000 + file_number as i64 * 1_000_000 + row_number as i64);
        let outcome = match rng.below(100) {
            0 => "retryexhausted",
            1 => "healthprobe",
            2..=9 => "upstreamtimeout",
            _ => "requestcompleted",
        };
        let status = if outcome == "upstreamtimeout" || outcome == "retryexhausted" {
            503
        } else {
            200
        };
        let service = format!("api-service-{}", rng.below(30));
        let trace = rng.hex(32);
        let request = rng.hex(32);
        let mut body = format!(
            "request {outcome} service={service} method=POST route=/v1/resources/{} \
             status={status} elapsed_ms={} trace_id={trace} request_id={request}; ",
            rng.below(300),
            rng.below(5000),
        );
        for _ in 0..(6 + rng.below(6)) {
            body.push_str(DETAILS[rng.below(DETAILS.len() as u64) as usize]);
            body.push_str("; ");
        }
        strings[0].push(body);
        strings[1].push(if status == 503 { "ERROR" } else { "INFO" }.to_string());
        strings[2].push(service.clone());
        strings[3].push(["production", "platform", "observability"][rng.below(3) as usize].into());
        strings[4].push(format!("{service}-deployment-{:04}", rng.below(200)));
        strings[5].push(service);
        strings[6].push(trace);
        strings[7].push(request);
        statuses.push(status);
    }
    let mut columns = vec![Arc::new(Int64Array::from(timestamps)) as ArrayRef];
    columns.extend(
        strings
            .into_iter()
            .map(|values| Arc::new(StringArray::from(values)) as ArrayRef),
    );
    columns.push(Arc::new(Int64Array::from(statuses)));
    Ok(arrow::record_batch::RecordBatch::try_new(
        Arc::clone(schema),
        columns,
    )?)
}

#[derive(Default)]
struct ByteCounter(u64);

impl std::io::Write for ByteCounter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 += bytes.len() as u64;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn json_original_bytes(batch: &arrow::record_batch::RecordBatch) -> anyhow::Result<u64> {
    let mut writer = arrow_json::LineDelimitedWriter::new(ByteCounter::default());
    writer.write(batch)?;
    writer.finish()?;
    // Ingestion counts JSON records without newline delimiters.
    Ok(writer.into_inner().0 - batch.num_rows() as u64)
}

async fn cmd_gen_logs(dir: &str, file_number: usize, original_mib: u64) -> anyhow::Result<()> {
    anyhow::ensure!(
        original_mib > 0 && original_mib <= 512,
        "original_mib must be in 1..=512; generate bounded inputs in separate processes"
    );
    std::fs::create_dir_all(dir)?;
    let path = std::path::Path::new(dir).join(format!("{file_number:04}.vix"));
    anyhow::ensure!(
        !path.exists() && !path.with_extension("vxi").exists(),
        "refusing to overwrite {}",
        path.display()
    );
    let schema = logs_schema();
    let mut rng =
        Rng(0x9E3779B97F4A7C15 ^ (file_number as u64 + 1).wrapping_mul(0xA24BAED4963EE407));
    let mut batches = Vec::new();
    let mut rows = 0usize;
    let mut original_bytes = 0u64;
    let target_bytes = original_mib * 1024 * 1024;
    while original_bytes < target_bytes {
        let mut batch = make_logs_batch(&schema, &mut rng, file_number, rows)?;
        let mut batch_bytes = json_original_bytes(&batch)?;
        let remaining = target_bytes - original_bytes;
        let last_batch = batch_bytes > remaining;
        if last_batch {
            let keep = (remaining * batch.num_rows() as u64 / batch_bytes) as usize;
            batch = batch.slice(0, keep);
            batch_bytes = json_original_bytes(&batch)?;
            while batch_bytes > remaining && batch.num_rows() > 0 {
                batch = batch.slice(0, batch.num_rows() - 1);
                batch_bytes = json_original_bytes(&batch)?;
            }
        }
        rows += batch.num_rows();
        original_bytes += batch_bytes;
        batches.push(batch);
        if last_batch {
            break;
        }
    }
    let table: Arc<dyn TableProvider> =
        Arc::new(MemTable::try_new(Arc::clone(&schema), vec![batches])?);
    let started = Instant::now();
    let result = openobserve_core::vix::core_writer::write_core_file_from_tables(
        &format!("merge-bench-logs-{file_number}"),
        config::meta::stream::StreamType::Logs,
        schema,
        vec![table],
        &["body".to_string()],
        &["trace_id".to_string()],
        false,
        0,
    )
    .await?;
    anyhow::ensure!(
        result.stats.row_count == rows as u64 && result.dropped_rows == 0,
        "log builder lost rows"
    );
    let index = result.index.as_ref().ok_or_else(|| {
        anyhow::anyhow!("log generation produced no index; check index-off environment")
    })?;
    std::fs::write(&path, &result.data)?;
    std::fs::write(path.with_extension("vxi"), index)?;
    let manifest = serde_json::json!({
        "file": path.file_name().unwrap().to_string_lossy(),
        "file_number": file_number, "rows": rows, "original_bytes": original_bytes,
        "data_bytes": result.data.len(), "index_bytes": index.len(),
        "terms": result.stats.term_count, "fts_fields": ["body"],
        "build_seconds": started.elapsed().as_secs_f64(),
    });
    std::fs::write(
        path.with_extension("json"),
        serde_json::to_vec_pretty(&manifest)?,
    )?;
    eprintln!("gen-logs: {manifest}");
    Ok(())
}

async fn cmd_gen(
    dir: &str,
    files: usize,
    rows_per_file: usize,
    overlap: bool,
    narrow: bool,
    vary_schema: bool,
    type_drift: bool,
) -> Result<(), anyhow::Error> {
    std::fs::create_dir_all(dir)?;
    let schema = spans_schema();
    let (fts, bloom) = stream_settings();
    if narrow {
        anyhow::bail!(
            "--narrow is retired: v2 stores EVERY present field as a docs column, \
             so a narrow docs schema no longer exists"
        );
    }
    let base_ts_us = 1_785_138_000_000_000_i64;
    // disjoint ranges: file i covers [base + i*span*10, +rows) — later files
    // hold NEWER rows; each file is internally DESC after the builder sort.
    // --overlap (#51c-c): every file covers the SAME [base, base+rows) range
    // — the concurrently-written shape whose merges always interleaved.
    // --vary-schema (M17): per-file schema UNIONS differ (each file drops a
    // couple of the optional columns by a deterministic pattern) — the prod
    // gen-1 reality that disqualified every chunk copy pre-M17.
    let droppable = [
        "span_kind",
        "http.url",
        "http.method",
        "server.address",
        "service_service.version",
        "db.query.text",
    ];
    for file in 0..files {
        let mut rng = Rng(0x9E3779B97F4A7C15 ^ (file as u64 + 1).wrapping_mul(0xA24BAED4963EE407));
        let file_base = if overlap {
            base_ts_us
        } else {
            base_ts_us + (file * rows_per_file * 10) as i64
        };
        // per-file column subset: keep droppable[j] iff (file + j) % 3 != 0
        // — every field survives in 2/3 of the files, so the merge union is
        // the full schema while every pair of files differs
        let keep: Vec<usize> = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| {
                if !vary_schema {
                    return true;
                }
                match droppable.iter().position(|d| d == field.name()) {
                    Some(j) => (file + j) % 3 != 0,
                    None => true,
                }
            })
            .map(|(index, _)| index)
            .collect();
        let drift_type = [
            DataType::Utf8,
            DataType::Boolean,
            DataType::Float64,
            DataType::Int64,
        ][file % 4]
            .clone();
        let full_schema = if type_drift {
            let status_index = schema.index_of("status_code")?;
            let mut fields: Vec<Field> = schema
                .fields()
                .iter()
                .map(|field| field.as_ref().clone())
                .collect();
            fields[status_index] = Field::new("status_code", drift_type.clone(), true);
            Arc::new(Schema::new(fields))
        } else {
            Arc::clone(&schema)
        };
        let file_schema = Arc::new(full_schema.project(&keep)?);
        let mut batches = Vec::new();
        let mut left = rows_per_file;
        let mut offset = 0usize;
        while left > 0 {
            let n = left.min(BATCH_ROWS);
            let batch = make_batch(&schema, &mut rng, file_base + offset as i64, n);
            let batch = if type_drift {
                with_status_code_type(batch, &drift_type)?
            } else {
                batch
            };
            batches.push(batch.project(&keep)?);
            left -= n;
            offset += n;
        }
        let table: Arc<dyn TableProvider> =
            Arc::new(MemTable::try_new(Arc::clone(&file_schema), vec![batches])?);
        let started = Instant::now();
        let result = openobserve_core::vix::core_writer::write_core_file_from_tables(
            &format!("merge-bench-gen-{file}"),
            config::meta::stream::StreamType::Logs,
            Arc::clone(&file_schema),
            vec![table],
            &fts,
            &bloom,
            false,
            0,
        )
        .await?;
        let path = format!("{dir}/{:04}.vix", file);
        std::fs::write(&path, &result.data)?;
        // v3 split: the index sidecar is its own object next to the data
        if let Some(index) = &result.index {
            std::fs::write(format!("{dir}/{:04}.vxi", file), index)?;
        }
        eprintln!(
            "gen {path}: {} rows, {:.1} MiB data + {:.1} MiB index, {} terms in {:.1}s",
            result.stats.row_count,
            result.data.len() as f64 / (1024.0 * 1024.0),
            result.index.as_ref().map_or(0, |b| b.len()) as f64 / (1024.0 * 1024.0),
            result.stats.term_count,
            started.elapsed().as_secs_f64(),
        );
    }
    Ok(())
}

/// Ranged reads from a local corpus file — the bench twin of the
/// compactor's cache-ladder source, so `merge` measures the true ranged
/// input profile (no whole-file Bytes).
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

/// The compactor worker's exact input shape: ranged sources over the files.
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
    paths.iter().map(|path| load_input(path)).collect()
}

fn load_input(
    path: &std::path::Path,
) -> anyhow::Result<openobserve_core::vix::core_writer::MergeInput> {
    let source = |path: &std::path::Path| -> anyhow::Result<Arc<dyn vortex_index::VixRangeSource>> {
        let file = std::fs::File::open(path)?;
        let len = file.metadata()?.len();
        Ok(Arc::new(FileRangeSource {
            name: path.display().to_string(),
            file,
            len,
        }))
    };
    let index = match source(&path.with_extension("vxi")) {
        Ok(index) => Some(index),
        Err(error)
            if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) =>
        {
            None
        }
        Err(error) => return Err(error),
    };
    Ok((
        path.file_name().unwrap().to_string_lossy().into_owned(),
        source(path)?,
        index,
    ))
}

/// Derive merge-time settings from the corpus files themselves (unchanged
/// stream settings — the common compaction case), mirroring the ignored
/// in-tree bench. Two prod-faithful adjustments:
/// - term-only fields type from the bench "registry" ([`spans_schema`], the schema every corpus is
///   generated from) instead of a blanket `Utf8` — the registry type is what a real merge plan
///   resolves (`duration` is `Int64` there), and it is what decides a widened column's stored type;
/// - the CONFIGURED column-store settings ([`stream_settings`]) union into the derived cs list — a
///   no-op for corpora whose files already store those columns, and exactly prod's widening for a
///   `--narrow` corpus (#51c-d: the plan wants columns the inputs never stored).
fn derive_schema(
    inputs: &[openobserve_core::vix::core_writer::MergeInput],
    status_code_utf8: bool,
    stored_schema: bool,
    widen_utf8: &[String],
) -> (Schema, Vec<String>) {
    let registry = spans_schema();
    let mut fts: Vec<String> = Vec::new();
    // Real-file benchmarks can use the stored schema as the exact target;
    // synthetic drift fixtures keep the built-in registry authoritative.
    let mut latest_fields: Vec<Field> = if stored_schema {
        Vec::new()
    } else {
        registry
            .fields()
            .iter()
            .filter(|field| field.name() != "_source" && field.name() != "_original")
            .map(|field| {
                if status_code_utf8 && field.name() == "status_code" {
                    Field::new(field.name(), DataType::Utf8, field.is_nullable())
                } else {
                    field.as_ref().clone()
                }
            })
            .collect()
    };
    for (_, data, index) in inputs {
        let reader =
            VixReader::open_ranged_with_index(std::sync::Arc::clone(data), index.clone()).unwrap();
        for field in reader.docs_schema().unwrap().fields() {
            let name = field.name().as_str();
            if name == "_source" || name == "_original" {
                continue;
            }
            if !latest_fields.iter().any(|f| f.name() == name) {
                latest_fields.push(if stored_schema {
                    field.as_ref().clone()
                } else {
                    Field::new(name, field.data_type().clone(), name != TIMESTAMP_COL)
                });
            }
        }
        for name in reader.term_field_names() {
            if !latest_fields.iter().any(|f| f.name() == name) {
                let data_type = registry
                    .field_with_name(name)
                    .map(|f| f.data_type().clone())
                    .unwrap_or(DataType::Utf8);
                latest_fields.push(Field::new(name, data_type, true));
            }
            if !reader.has_term_capability(name) && !fts.iter().any(|f| f == name) {
                fts.push(name.to_string());
            }
        }
    }
    // `--widen-utf8=a,b`: the production registry types these fields Utf8
    // while the inputs store them under a narrower (numeric/bool) type — the
    // 2026-09-24 logs/default shape whose merges cast inside the chunk copy.
    for field in &mut latest_fields {
        if widen_utf8.iter().any(|name| name == field.name()) {
            *field = Field::new(field.name(), DataType::Utf8, true);
        }
    }
    (Schema::new(latest_fields), fts)
}

fn rss_lines() -> String {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    status
        .lines()
        .filter(|line| line.starts_with("VmHWM") || line.starts_with("VmRSS"))
        .collect::<Vec<_>>()
        .join("  ")
}

/// Make sure `key=value` is in this process's environment, re-exec'ing the
/// bench with it set when it is not. The engine config is env-backed and
/// process-global (`std::env::set_var` is unsafe in edition 2024), so the
/// safe way to flip a knob per run is a fresh process image: `exec` replaces
/// this one wholesale before any config access, and the re-exec'd child sees
/// the variable set and falls straight through.
fn ensure_env(key: &str, value: &str) {
    if std::env::var(key).map(|v| v == value).unwrap_or(false) {
        return;
    }
    use std::os::unix::process::CommandExt;
    let exe = std::env::current_exe().expect("current_exe");
    eprintln!("re-exec with {key}={value}");
    let error = std::process::Command::new(exe)
        .args(std::env::args_os().skip(1))
        .env(key, value)
        .exec();
    // exec only returns on failure
    panic!("re-exec with {key}={value} failed: {error}");
}

fn cmd_merge(
    dir: &str,
    out: &str,
    rebuild: bool,
    status_code_utf8: bool,
    require_columns: bool,
    stored_schema: bool,
    stream_type: config::meta::stream::StreamType,
    indexed_only: bool,
    widen_utf8: &[String],
) -> Result<(), anyhow::Error> {
    let out_path = std::path::Path::new(out);
    for path in [out_path.to_path_buf(), out_path.with_extension("vxi")] {
        match std::fs::symlink_metadata(&path) {
            Ok(_) => anyhow::bail!("refusing to overwrite existing output {}", path.display()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(anyhow::anyhow!(
                    "inspect output {}: {error}",
                    path.display()
                ));
            }
        }
    }
    let parent = out_path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."));
    let canonical_output = std::fs::canonicalize(parent)?.join(
        out_path
            .file_name()
            .ok_or_else(|| anyhow::anyhow!("output must name a file"))?,
    );
    anyhow::ensure!(
        !canonical_output.starts_with(std::fs::canonicalize(dir)?),
        "output must be outside the input corpus: {}",
        canonical_output.display()
    );
    let mib = |bytes: usize| bytes as f64 / (1024.0 * 1024.0);
    let started = Instant::now();
    let inputs = load_inputs(dir)?;
    let total_bytes: u64 = inputs
        .iter()
        .map(|(_, data, index)| data.len() + index.as_ref().map_or(0, |i| i.len()))
        .sum();
    let load_elapsed = started.elapsed();

    let (latest_schema, fts) = derive_schema(&inputs, status_code_utf8, stored_schema, widen_utf8);
    let bloom = if stream_type == config::meta::stream::StreamType::Traces {
        vec!["trace_id".to_string(), "span_id".to_string()]
    } else {
        vec!["trace_id".to_string()]
    };
    eprintln!(
        "opened {} files (ranged) / {:.1} MiB in {load_elapsed:.2?}; fts={fts:?}",
        inputs.len(),
        mib(total_bytes as usize),
    );

    let started = Instant::now();
    let result = if indexed_only {
        openobserve_core::vix::core_writer::merge_core_files_indexed_only_with_cancellation(
            stream_type,
            &inputs,
            &latest_schema,
            &fts,
            &bloom,
            &openobserve_core::vix::core_writer::VixMergeCancellation::new(),
        )?
    } else if rebuild {
        openobserve_core::vix::core_writer::merge_core_files_rebuild(
            stream_type,
            &inputs,
            &latest_schema,
            &fts,
            &bloom,
        )?
    } else {
        openobserve_core::vix::core_writer::merge_core_files(
            stream_type,
            &inputs,
            &latest_schema,
            &fts,
            &bloom,
        )?
    };
    let merge_elapsed = started.elapsed();
    anyhow::ensure!(
        !indexed_only || result.used_index_merge,
        "indexed-only benchmark did not use the index merge path"
    );
    if require_columns {
        anyhow::ensure!(
            result.terms_from_columns,
            "requested column-derived rebuild, but merge selected another path"
        );
    }
    let out_len = result.output.len();
    // v3 split: write the merged sidecar next to the data output
    if let Some(index) = &result.index {
        std::fs::write(std::path::Path::new(out).with_extension("vxi"), index)?;
    }
    match result.output {
        vortex_index::VixOutput::Bytes(data) => std::fs::write(out, &data)?,
        vortex_index::VixOutput::Spooled { file, .. } => {
            // persist is a rename — it cannot cross filesystems (EXDEV, e.g.
            // spool on the data volume, `out` on tmpfs): fall back to a copy
            // (the temp file then deletes itself on drop)
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
        "merge: {merge_elapsed:.2?}  used_index_merge={}  terms_from_columns={}  docs_batches={}  \
         docs_passthrough_inputs={}  concat_order={}  out {:.1} MiB \
         ({} rows, {} terms, index {:.1} MiB, docs {:.1} MiB)",
        result.used_index_merge,
        result.terms_from_columns,
        result.docs_batches,
        result.docs_passthrough_inputs,
        result.concat_order,
        mib(out_len as usize),
        result.stats.row_count,
        result.stats.term_count,
        mib(result.stats.index_size as usize),
        mib(result.stats.docs_size as usize),
    );
    eprintln!(
        "process memory after merge (includes setup): {}",
        rss_lines()
    );
    Ok(())
}

/// Docs-blob layout census of one `.vix`: leaf (flat segment) counts and
/// bytes per column, aggregated — the storage-side cost of the writer's
/// coalescing/residency budgets (finer leaves = more footer + per-leaf
/// encoding overhead).
fn cmd_leaves(path: &str) -> Result<(), anyhow::Error> {
    let (_, data, _) = load_input(std::path::Path::new(path))?;
    let docs = VixDocs::open_ranged(data)?;
    let mut report = docs.leaf_report()?;
    let columns = report.len();
    let leaves: u64 = report.iter().map(|(_, leaves, _)| leaves).sum();
    let bytes: u64 = report.iter().map(|(_, _, bytes)| bytes).sum();
    report.sort_by_key(|(_, leaves, bytes)| std::cmp::Reverse((*leaves, *bytes)));
    let rows = docs.row_count();
    eprintln!(
        "leaves: {path}  rows={rows}  columns={columns}  leaves={leaves}  leaf_bytes={:.1} MiB  \
         docs_blob={:.1} MiB  leaves/column={:.1}  rows/leaf(mean)={:.0}",
        bytes as f64 / (1024.0 * 1024.0),
        docs.docs_blob_len() as f64 / (1024.0 * 1024.0),
        leaves as f64 / columns.max(1) as f64,
        (rows as f64 * columns as f64) / leaves.max(1) as f64,
    );
    for (name, leaves, bytes) in report.iter().take(12) {
        eprintln!(
            "  {name:<40} leaves={leaves:<6} bytes={:>10.1} KiB  rows/leaf={:.0}",
            *bytes as f64 / 1024.0,
            rows as f64 / (*leaves).max(1) as f64
        );
    }
    Ok(())
}

fn cmd_sidecar(
    dir: &str,
    stored_schema: bool,
    stream_type: config::meta::stream::StreamType,
) -> Result<(), anyhow::Error> {
    let started = Instant::now();
    let inputs = load_inputs(dir)?;
    anyhow::ensure!(
        inputs.len() == 1,
        "sidecar benchmark requires exactly one input, found {}",
        inputs.len()
    );
    let load_elapsed = started.elapsed();
    let (latest_schema, fts) = derive_schema(&inputs, false, stored_schema, &[]);
    let bloom = if stream_type == config::meta::stream::StreamType::Traces {
        vec!["trace_id".to_string(), "span_id".to_string()]
    } else {
        vec!["trace_id".to_string()]
    };
    eprintln!(
        "opened one ranged file in {load_elapsed:.2?}; fields={} fts={fts:?} bloom={bloom:?}",
        latest_schema.fields().len(),
    );

    let started = Instant::now();
    let outcome = openobserve_core::vix::core_writer::rebuild_core_file_sidecar(
        stream_type,
        &inputs[0],
        &latest_schema,
        &fts,
        &bloom,
    )?;
    let rebuild_elapsed = started.elapsed();
    match outcome {
        openobserve_core::vix::core_writer::SidecarHealOutcome::Rebuilt { index, stats } => {
            eprintln!(
                "sidecar: {rebuild_elapsed:.2?}  index {:.1} MiB  stats={stats:?}",
                index.len() as f64 / (1024.0 * 1024.0),
            );
        }
        openobserve_core::vix::core_writer::SidecarHealOutcome::DropSidecar => {
            anyhow::bail!("sidecar benchmark selected index-off DropSidecar")
        }
        openobserve_core::vix::core_writer::SidecarHealOutcome::NeedsDocsRewrite(reason) => {
            anyhow::bail!("sidecar benchmark requires a docs rewrite: {reason}")
        }
    }
    eprintln!(
        "process memory after sidecar rebuild (includes setup): {}",
        rss_lines()
    );
    Ok(())
}

/// Stream-hash one file's term stream and docs columns. `multiset` (#51c-c)
/// is the ORDER-INSENSITIVE mode: per-ROW content hashes folded
/// commutatively (wrapping add) and the term stream hashed WITHOUT postings
/// doc ids — the only valid comparison between outputs whose row order
/// legitimately differs (concat-order vs sorted).
fn file_digest(
    path: &str,
    multiset: bool,
    ignore_source: bool,
) -> Result<(u64, u64, u64, u64, Vec<(String, DataType, bool)>), anyhow::Error> {
    let data = bytes::Bytes::from(std::fs::read(path)?);
    // v3 split: the index sidecar sits next to the data object
    let index = std::fs::read(std::path::Path::new(path).with_extension("vxi"))
        .ok()
        .map(bytes::Bytes::from);
    let reader = VixReader::open_with_index(data.clone(), index)?;
    let row_count = reader.row_count();

    let mut term_hasher = DefaultHasher::new();
    let mut term_count = 0u64;
    reader.for_each_term(&mut |key, doc_count, postings| {
        key.hash(&mut term_hasher);
        doc_count.hash(&mut term_hasher);
        if !multiset {
            // postings are doc-id POSITIONS — row-order-dependent by nature
            postings.hash(&mut term_hasher);
        }
        term_count += 1;
        Ok(())
    })?;

    let docs = VixDocs::open(data)?;
    let schema = docs.schema().clone();
    let mut fields: Vec<(String, DataType, bool)> = schema
        .fields()
        .iter()
        .filter(|field| !ignore_source || field.name() != "_source")
        .map(|field| {
            (
                field.name().clone(),
                field.data_type().clone(),
                field.is_nullable(),
            )
        })
        .collect();
    fields.sort_by(|a, b| a.0.cmp(&b.0));
    let columns: Vec<String> = fields.iter().map(|field| field.0.clone()).collect();
    let docs_digest = if multiset {
        // Order-insensitive docs digest: hash each ROW's content (values in
        // sorted column order) into its own hasher and fold the row hashes
        // with a commutative wrapping add — identical row MULTISETS digest
        // identically whatever the storage order.
        let mut folded: u64 = 0;
        docs.scan_docs(Some(&columns), None, None, &mut |batch| {
            let casted: Vec<ArrayRef> = columns
                .iter()
                .map(|name| {
                    let column = batch
                        .column_by_name(name)
                        .ok_or_else(|| anyhow::anyhow!("scan lost column {name}"))?;
                    Ok(arrow::compute::cast(column, &DataType::Utf8)
                        .unwrap_or_else(|_| Arc::clone(column)))
                })
                .collect::<Result<_, anyhow::Error>>()?;
            for row in 0..batch.num_rows() {
                let mut row_hasher = DefaultHasher::new();
                for column in &casted {
                    hash_value_at(column, row, &mut row_hasher);
                }
                folded = folded.wrapping_add(row_hasher.finish());
            }
            Ok(())
        })?;
        folded
    } else {
        // Chunk-boundary-INDEPENDENT docs digest: one hasher per column,
        // each folding that column's values in row order across every
        // scanned batch, combined in sorted column order at the end.
        // Hashing per batch column-by-column into one hasher (the old
        // scheme) interleaved columns at batch boundaries, so two outputs
        // holding identical rows but chunked differently (fast path vs
        // rebuild vs #51c passthrough) hashed differently.
        let mut column_hashers: Vec<DefaultHasher> =
            columns.iter().map(|_| DefaultHasher::new()).collect();
        docs.scan_docs(Some(&columns), None, None, &mut |batch| {
            for (name, hasher) in columns.iter().zip(&mut column_hashers) {
                let column = batch
                    .column_by_name(name)
                    .ok_or_else(|| anyhow::anyhow!("scan lost column {name}"))?;
                let column = arrow::compute::cast(column, &DataType::Utf8)
                    .unwrap_or_else(|_| Arc::clone(column));
                hash_column(&column, hasher);
            }
            Ok(())
        })?;
        let mut docs_hasher = DefaultHasher::new();
        for hasher in column_hashers {
            hasher.finish().hash(&mut docs_hasher);
        }
        docs_hasher.finish()
    };
    Ok((
        row_count,
        term_count,
        term_hasher.finish(),
        docs_digest,
        fields,
    ))
}

/// Hash one row's value of a (Utf8-casted where castable) column.
fn hash_value_at(column: &ArrayRef, row: usize, hasher: &mut DefaultHasher) {
    if let Some(strings) = column.as_any().downcast_ref::<StringArray>() {
        strings
            .is_valid(row)
            .then(|| strings.value(row))
            .hash(hasher);
    } else if let Some(ints) = column.as_any().downcast_ref::<Int64Array>() {
        ints.is_valid(row).then(|| ints.value(row)).hash(hasher);
    } else {
        panic!("unhashed docs column type {:?}", column.data_type());
    }
}

fn hash_column(column: &ArrayRef, hasher: &mut DefaultHasher) {
    if let Some(strings) = column.as_any().downcast_ref::<StringArray>() {
        for value in strings {
            value.hash(hasher);
        }
    } else if let Some(ints) = column.as_any().downcast_ref::<Int64Array>() {
        for value in ints {
            value.hash(hasher);
        }
    } else {
        panic!("unhashed docs column type {:?}", column.data_type());
    }
}

fn cmd_compare(
    a: &str,
    b: &str,
    multiset: bool,
    docs_only: bool,
    ignore_source: bool,
) -> Result<(), anyhow::Error> {
    let da = file_digest(a, multiset, ignore_source)?;
    let db = file_digest(b, multiset, ignore_source)?;
    anyhow::ensure!(
        da.4 == db.4,
        "docs schemas differ: {:?} vs {:?}",
        da.4,
        db.4
    );
    if docs_only {
        anyhow::ensure!(
            da.0 == db.0 && da.3 == db.3,
            "docs differ ({} mode): {a} (rows={}, docs_digest={:x}) vs {b} (rows={}, \
             docs_digest={:x})",
            if multiset { "multiset" } else { "row-order" },
            da.0,
            da.3,
            db.0,
            db.3,
        );
        eprintln!(
            "docs equivalent ({} mode): rows={}, docs_digest={:x}",
            if multiset { "multiset" } else { "row-order" },
            da.0,
            da.3,
        );
        return Ok(());
    }
    anyhow::ensure!(
        da.0 == db.0 && da.1 == db.1 && da.2 == db.2 && da.3 == db.3,
        "outputs differ ({} mode): {a} (rows={}, terms={}, term_digest={:x}, docs_digest={:x}) \
         vs {b} (rows={}, terms={}, term_digest={:x}, docs_digest={:x})",
        if multiset { "multiset" } else { "row-order" },
        da.0,
        da.1,
        da.2,
        da.3,
        db.0,
        db.1,
        db.2,
        db.3,
    );
    eprintln!(
        "outputs equivalent ({} mode): rows={}, terms={}, term_digest={:x}, docs_digest={:x}",
        if multiset { "multiset" } else { "row-order" },
        da.0,
        da.1,
        da.2,
        da.3,
    );
    Ok(())
}

#[derive(Debug, Default, PartialEq, Eq, serde::Serialize)]
struct LogsDigest {
    rows: u64,
    row_hash_sum: u64,
    row_hash_xor: u64,
    term_documents: u64,
    term_hash_sum: u64,
    fts_counts: [u64; LOG_PROBES.len()],
}

impl LogsDigest {
    fn add(&mut self, other: &Self) {
        self.rows += other.rows;
        self.row_hash_sum = self.row_hash_sum.wrapping_add(other.row_hash_sum);
        self.row_hash_xor ^= other.row_hash_xor;
        self.term_documents += other.term_documents;
        self.term_hash_sum = self.term_hash_sum.wrapping_add(other.term_hash_sum);
        for (sum, count) in self.fts_counts.iter_mut().zip(other.fts_counts) {
            *sum += count;
        }
    }
}

/// A commutative digest permits merged dictionary terms to coalesce and
/// documents to change order, while the probe bitmaps independently check
/// that postings still point at the correct decoded rows. This is a
/// probabilistic content check, not a cryptographic equality proof.
fn logs_digest(
    input: &openobserve_core::vix::core_writer::MergeInput,
) -> anyhow::Result<(LogsDigest, Vec<String>)> {
    let (name, data, index) = input;
    anyhow::ensure!(index.is_some(), "{name}: missing index sidecar");
    let reader = VixReader::open_ranged_with_index(Arc::clone(data), index.clone())?;
    anyhow::ensure!(
        reader.fts_fields().contains("body"),
        "{name}: body lost FTS capability"
    );
    anyhow::ensure!(
        reader.partial_fields().is_empty(),
        "{name}: partial index fields {:?}",
        reader.partial_fields()
    );
    let mut digest = LogsDigest::default();
    reader.for_each_term(&mut |key, doc_count, postings| {
        anyhow::ensure!(
            postings.len() as u64 == doc_count,
            "{name}: postings count differs from term metadata"
        );
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        digest.term_documents += doc_count;
        digest.term_hash_sum = digest
            .term_hash_sum
            .wrapping_add(hasher.finish().wrapping_mul(doc_count));
        Ok(())
    })?;
    let probes = LOG_PROBES
        .iter()
        .map(|token| {
            reader.eval(&VixQuery::TokenAnyField {
                token: token.as_bytes().to_vec(),
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let docs = VixDocs::open_ranged(Arc::clone(data))?;
    let mut columns: Vec<String> = docs
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    columns.sort();
    let body_index = columns
        .iter()
        .position(|name| name == "body")
        .ok_or_else(|| anyhow::anyhow!("{name}: missing body column"))?;
    docs.scan_docs(Some(&columns), None, None, &mut |batch| {
        let values = columns
            .iter()
            .map(|name| {
                let column = batch
                    .column_by_name(name)
                    .ok_or_else(|| anyhow::anyhow!("scan lost column {name}"))?;
                Ok(arrow::compute::cast(column, &DataType::Utf8)?)
            })
            .collect::<anyhow::Result<Vec<ArrayRef>>>()?;
        let bodies = values[body_index]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("body was not cast to Utf8"))?;
        for row in 0..batch.num_rows() {
            let mut hasher = DefaultHasher::new();
            for column in &values {
                hash_value_at(column, row, &mut hasher);
            }
            let hash = hasher.finish();
            digest.row_hash_sum = digest.row_hash_sum.wrapping_add(hash);
            digest.row_hash_xor ^= hash;
            for (probe, (token, bitmap)) in LOG_PROBES.iter().zip(&probes).enumerate() {
                let expected = bodies.value(row).contains(*token);
                anyhow::ensure!(
                    bitmap.value(digest.rows as usize) == expected,
                    "{name}: FTS postings mismatch at row {} for {token}",
                    digest.rows
                );
                digest.fts_counts[probe] += u64::from(expected);
            }
            digest.rows += 1;
        }
        Ok(())
    })?;
    anyhow::ensure!(
        digest.rows == reader.row_count(),
        "{name}: decoded row count differs from metadata"
    );
    // Raw composite keys contain field ids; require unchanged field order
    // before comparing their count-weighted digests.
    let mut shape = columns;
    shape.extend(
        reader
            .term_field_names()
            .into_iter()
            .map(|name| format!("term:{name}")),
    );
    Ok((digest, shape))
}

fn cmd_verify_logs(dir: &str, out: &str) -> anyhow::Result<()> {
    let inputs = load_inputs(dir)?;
    let mut expected = LogsDigest::default();
    let mut expected_shape = None;
    let mut original_bytes = 0u64;
    for input in &inputs {
        let (digest, shape) = logs_digest(input)?;
        let manifest: serde_json::Value = serde_json::from_slice(&std::fs::read(
            std::path::Path::new(dir)
                .join(&input.0)
                .with_extension("json"),
        )?)?;
        anyhow::ensure!(
            manifest["rows"].as_u64() == Some(digest.rows),
            "{}: generated row count differs from manifest",
            input.0
        );
        original_bytes += manifest["original_bytes"]
            .as_u64()
            .ok_or_else(|| anyhow::anyhow!("{}: manifest lacks original_bytes", input.0))?;
        if let Some(expected_shape) = &expected_shape {
            anyhow::ensure!(
                &shape == expected_shape,
                "{}: input schema/index field order differs",
                input.0
            );
        } else {
            expected_shape = Some(shape);
        }
        expected.add(&digest);
        eprintln!("verified input {}: {} rows", input.0, digest.rows);
    }
    let (actual, shape) = logs_digest(&load_input(std::path::Path::new(out))?)?;
    anyhow::ensure!(
        Some(shape) == expected_shape,
        "output schema/index field order differs"
    );
    anyhow::ensure!(
        actual == expected,
        "merged content/index differs: expected {expected:?}, actual {actual:?}"
    );
    eprintln!(
        "verify-logs: {}",
        serde_json::json!({ "input_files": inputs.len(), "original_bytes": original_bytes, "digest": actual, "fts_probes": LOG_PROBES })
    );
    Ok(())
}

/// Minimal stderr logger (O2_BENCH_DEBUG_LOG=1): surfaces the merge's
/// `log::debug!` phase timings (term-table load, k-way ranges/workers,
/// dict/terms encode, SBBF bloom build, index merge total).
struct StderrLogger;
impl log::Log for StderrLogger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.target().contains("vix")
            || metadata.target().starts_with("vortex_index")
            || metadata.level() <= log::Level::Warn
    }
    fn log(&self, record: &log::Record) {
        if self.enabled(record.metadata()) {
            eprintln!("[{}] {}", record.level(), record.args());
        }
    }
    fn flush(&self) {}
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    if std::env::var("O2_BENCH_DEBUG_LOG").is_ok_and(|v| v == "1") {
        static LOGGER: StderrLogger = StderrLogger;
        if log::set_logger(&LOGGER).is_ok() {
            log::set_max_level(log::LevelFilter::Debug);
        }
    }
    let args: Vec<String> = std::env::args().collect();
    let allowed_flags: &[&str] = match args.get(1).map(String::as_str) {
        Some("gen") => &[
            "--heal",
            "--overlap",
            "--narrow",
            "--vary-schema",
            "--type-drift",
        ],
        Some("merge") => &[
            "--rebuild",
            "--latest-status-code-utf8",
            "--require-columns",
            "--stored-schema",
            "--traces",
            "--indexed-only",
        ],
        Some("sidecar") => &["--stored-schema", "--traces"],
        Some("leaves") => &[],
        Some("compare") => &["--multiset", "--docs-only", "--ignore-source"],
        _ => &[],
    };
    for flag in args.iter().skip(2).filter(|arg| arg.starts_with("--")) {
        anyhow::ensure!(
            allowed_flags.contains(&flag.as_str())
                || (args.get(1).map(String::as_str) == Some("merge")
                    && flag.starts_with("--widen-utf8=")),
            "unsupported flag {flag} for {:?}",
            args.get(1)
        );
    }
    let widen_utf8: Vec<String> = args
        .iter()
        .skip(2)
        .filter_map(|arg| arg.strip_prefix("--widen-utf8="))
        .flat_map(|list| list.split(',').map(str::trim).map(str::to_string))
        .filter(|name| !name.is_empty())
        .collect();
    let flag = |name: &str| args.iter().skip(2).any(|a| a == name);
    match args.get(1).map(String::as_str) {
        Some("gen-logs") => {
            anyhow::ensure!(
                args.len() == 5,
                "gen-logs <dir> <file_number> <original_mib> accepts no flags"
            );
            cmd_gen_logs(&args[2], args[3].parse()?, args[4].parse()?).await
        }
        Some("gen") => {
            let dir = args.get(2).expect(
                "gen <dir> <files> <rows_per_file> [--heal] [--overlap] [--vary-schema] \
                     [--type-drift]",
            );
            let files: usize = args.get(3).expect("files").parse()?;
            let rows: usize = args.get(4).expect("rows_per_file").parse()?;
            if flag("--heal") || flag("--type-drift") {
                // the heal corpus: index-off L0 files (#42 shape) — the
                // build-path knob, resolved before any file is written
                ensure_env("ZO_VIX_L0_INDEX_OFF_STREAM_TYPES", "logs");
            }
            cmd_gen(
                dir,
                files,
                rows,
                flag("--overlap"),
                flag("--narrow"),
                flag("--vary-schema"),
                flag("--type-drift"),
            )
            .await
        }
        Some("merge") => {
            anyhow::ensure!(
                !flag("--indexed-only") || (!flag("--rebuild") && !flag("--require-columns")),
                "--indexed-only cannot be combined with --rebuild or --require-columns"
            );
            let dir = args.get(2).expect(
                "merge <dir> <out.vix> [--rebuild] [--latest-status-code-utf8] \
                 [--require-columns] [--stored-schema] [--traces] [--indexed-only] \
                 [--widen-utf8=field,...]",
            );
            let out = args.get(3).expect("out.vix");
            // #51c passthrough + #51c-c concatenation are the DEFAULT merge
            // shapes now — no knobs to set.
            let stream_type = if flag("--traces") {
                config::meta::stream::StreamType::Traces
            } else {
                config::meta::stream::StreamType::Logs
            };
            cmd_merge(
                dir,
                out,
                flag("--rebuild"),
                flag("--latest-status-code-utf8"),
                flag("--require-columns"),
                flag("--stored-schema"),
                stream_type,
                flag("--indexed-only"),
                &widen_utf8,
            )
        }
        Some("verify-logs") => {
            anyhow::ensure!(
                args.len() == 4,
                "verify-logs <input_dir> <out.vix> accepts no flags"
            );
            cmd_verify_logs(&args[2], &args[3])
        }
        Some("leaves") => {
            anyhow::ensure!(args.len() == 3, "leaves <file.vix> accepts no flags");
            cmd_leaves(&args[2])
        }
        Some("sidecar") => {
            let dir = args
                .get(2)
                .expect("sidecar <dir> [--stored-schema] [--traces]");
            let stream_type = if flag("--traces") {
                config::meta::stream::StreamType::Traces
            } else {
                config::meta::stream::StreamType::Logs
            };
            cmd_sidecar(dir, flag("--stored-schema"), stream_type)
        }
        Some("compare") => {
            // flags may precede the paths: compare [--multiset] [--docs-only]
            // [--ignore-source] <a> <b>
            let multiset = flag("--multiset") || args.get(2).is_some_and(|a| a == "--multiset");
            let paths: Vec<&String> = args
                .iter()
                .skip(2)
                .filter(|a| !a.starts_with("--"))
                .collect();
            let a = paths
                .first()
                .expect("compare [--multiset] [--docs-only] [--ignore-source] <a.vix> <b.vix>");
            let b = paths.get(1).expect("b.vix");
            cmd_compare(a, b, multiset, flag("--docs-only"), flag("--ignore-source"))
        }
        _ => {
            eprintln!(
                "usage: merge_bench gen <dir> <files> <rows_per_file> [--heal] [--overlap] \
                 [--vary-schema] [--type-drift] | \
                 gen-logs <dir> <file_number> <original_mib> | \
                 merge <dir> <out.vix> [--rebuild] [--latest-status-code-utf8] \
                 [--require-columns] [--stored-schema] [--traces] [--indexed-only] | \
                 verify-logs <input_dir> <out.vix> | \
                 sidecar <dir> [--stored-schema] [--traces] | \
                 compare [--multiset] [--docs-only] [--ignore-source] <a.vix> <b.vix>"
            );
            std::process::exit(2);
        }
    }
}
