//! Compaction-merge benchmark with peak-RSS reporting — the A/B harness for
//! the streamed docs-blob encode (bounded-memory merge).
//!
//! Subcommands:
//!
//!   gen <dir> <files> <rows_per_file>
//!       Build a corpus of move-job-shaped core files (the REAL move
//!       builder, `write_core_file_from_tables`, prod traces stream
//!       settings) with DISJOINT descending time ranges — the common
//!       compaction group shape.
//!
//!   merge <dir> <out.vix>
//!       Load every corpus file fully into memory (exactly like the
//!       compactor worker) and run `merge_core_files`; prints load/merge
//!       wall, output stats, and the process peak RSS (`VmHWM`) — run one
//!       `merge` per process so the peak is the merge's.
//!
//!   compare <a.vix> <b.vix>
//!       Assert reader-visible equality of two merge outputs: row count,
//!       term stream (keys, doc counts, postings — streamed through one
//!       hasher), and every docs column. NOTE: the docs hash folds values
//!       batch-by-batch, so it is only valid between outputs of the SAME
//!       merge path (identical chunk boundaries) — comparing a fast-path
//!       output against a rebuild output reports a false difference; the
//!       in-tree differential oracle covers that pair.
//!
//! Typical A/B: `gen` once, build this example at the old and new code,
//! run `merge` with each binary into different outputs, `compare` them.

use std::{
    hash::{DefaultHasher, Hash, Hasher},
    sync::Arc,
    time::Instant,
};

use arrow::{
    array::{Array, ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
};
use datafusion::{catalog::TableProvider, datasource::MemTable};
use vortex_index::{VixDocs, VixReader};

const TIMESTAMP_COL: &str = "_timestamp";
const BATCH_ROWS: usize = 8192;

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

/// Prod `default` traces stream settings.
fn stream_settings() -> (Vec<String>, Vec<String>, Vec<String>) {
    let fts: Vec<String> = vec![];
    let cs: Vec<String> = ["duration", "service_name", "operation_name", "span_status"]
        .into_iter()
        .map(String::from)
        .collect();
    let bloom: Vec<String> = vec!["trace_id".to_string()];
    (fts, cs, bloom)
}

async fn cmd_gen(dir: &str, files: usize, rows_per_file: usize) -> Result<(), anyhow::Error> {
    std::fs::create_dir_all(dir)?;
    let schema = spans_schema();
    let (fts, cs, bloom) = stream_settings();
    let base_ts_us = 1_785_138_000_000_000_i64;
    // disjoint ranges: file i covers [base + i*span*10, +rows) — later files
    // hold NEWER rows; each file is internally DESC after the builder sort
    for file in 0..files {
        let mut rng = Rng(0x9E3779B97F4A7C15 ^ (file as u64 + 1).wrapping_mul(0xA24BAED4963EE407));
        let file_base = base_ts_us + (file * rows_per_file * 10) as i64;
        let mut batches = Vec::new();
        let mut left = rows_per_file;
        let mut offset = 0usize;
        while left > 0 {
            let n = left.min(BATCH_ROWS);
            batches.push(make_batch(&schema, &mut rng, file_base + offset as i64, n));
            left -= n;
            offset += n;
        }
        let table: Arc<dyn TableProvider> =
            Arc::new(MemTable::try_new(Arc::clone(&schema), vec![batches])?);
        let started = Instant::now();
        let result = openobserve_core::vix::core_writer::write_core_file_from_tables(
            &format!("merge-bench-gen-{file}"),
            Arc::clone(&schema),
            vec![table],
            &fts,
            &cs,
            &bloom,
            false,
            0,
        )
        .await?;
        let path = format!("{dir}/{:04}.vix", file);
        std::fs::write(&path, &result.data)?;
        eprintln!(
            "gen {path}: {} rows, {:.1} MiB, {} terms in {:.1}s",
            result.stats.row_count,
            result.data.len() as f64 / (1024.0 * 1024.0),
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
    paths
        .iter()
        .map(|path| {
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            let file = std::fs::File::open(path)?;
            let len = file.metadata()?.len();
            let source: std::sync::Arc<dyn vortex_index::VixRangeSource> =
                std::sync::Arc::new(FileRangeSource {
                    name: name.clone(),
                    file,
                    len,
                });
            Ok((name, source))
        })
        .collect()
}

/// Derive merge-time settings from the corpus files themselves (unchanged
/// stream settings — the common compaction case), mirroring the ignored
/// in-tree bench.
fn derive_schema(
    inputs: &[openobserve_core::vix::core_writer::MergeInput],
) -> (Schema, Vec<String>, Vec<String>) {
    let mut fts: Vec<String> = Vec::new();
    let mut cs: Vec<String> = Vec::new();
    let mut latest_fields: Vec<Field> = Vec::new();
    for (_, data) in inputs {
        let reader = VixReader::open_ranged(std::sync::Arc::clone(data)).unwrap();
        for field in reader.docs_schema().unwrap().fields() {
            let name = field.name().as_str();
            if name == "_source" || name == "_original" {
                continue;
            }
            if !latest_fields.iter().any(|f| f.name() == name) {
                latest_fields.push(Field::new(
                    name,
                    field.data_type().clone(),
                    name != TIMESTAMP_COL,
                ));
            }
            if name != TIMESTAMP_COL && !cs.iter().any(|f| f == name) {
                cs.push(name.to_string());
            }
        }
        for name in reader.term_field_names() {
            if !latest_fields.iter().any(|f| f.name() == name) {
                latest_fields.push(Field::new(name, DataType::Utf8, true));
            }
            if !reader.has_term_capability(name) && !fts.iter().any(|f| f == name) {
                fts.push(name.to_string());
            }
        }
    }
    (Schema::new(latest_fields), fts, cs)
}

fn rss_lines() -> String {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    status
        .lines()
        .filter(|line| line.starts_with("VmHWM") || line.starts_with("VmRSS"))
        .collect::<Vec<_>>()
        .join("  ")
}

fn cmd_merge(dir: &str, out: &str, rebuild: bool) -> Result<(), anyhow::Error> {
    let mib = |bytes: usize| bytes as f64 / (1024.0 * 1024.0);
    let started = Instant::now();
    let inputs = load_inputs(dir)?;
    let total_bytes: u64 = inputs.iter().map(|(_, data)| data.len()).sum();
    let load_elapsed = started.elapsed();

    let (latest_schema, fts, cs) = derive_schema(&inputs);
    let bloom = vec!["trace_id".to_string()];
    eprintln!(
        "opened {} files (ranged) / {:.1} MiB in {load_elapsed:.2?}; fts={fts:?} cs={cs:?}",
        inputs.len(),
        mib(total_bytes as usize),
    );

    let started = Instant::now();
    let result = if rebuild {
        openobserve_core::vix::core_writer::merge_core_files_rebuild(
            &inputs,
            &latest_schema,
            &fts,
            &cs,
            &bloom,
        )?
    } else {
        openobserve_core::vix::core_writer::merge_core_files(
            &inputs,
            &latest_schema,
            &fts,
            &cs,
            &bloom,
        )?
    };
    let merge_elapsed = started.elapsed();
    let out_len = result.output.len();
    match result.output {
        vortex_index::VixOutput::Bytes(data) => std::fs::write(out, &data)?,
        vortex_index::VixOutput::Spooled { file, .. } => {
            file.persist(out)
                .map_err(|e| anyhow::anyhow!("persist spool: {e}"))?;
        }
    }
    eprintln!(
        "merge: {merge_elapsed:.2?}  used_index_merge={}  docs_batches={}  out {:.1} MiB \
         ({} rows, {} terms, index {:.1} MiB, docs {:.1} MiB)",
        result.used_index_merge,
        result.docs_batches,
        mib(out_len as usize),
        result.stats.row_count,
        result.stats.term_count,
        mib(result.stats.index_size as usize),
        mib(result.stats.docs_size as usize),
    );
    eprintln!("peak memory: {}", rss_lines());
    Ok(())
}

/// Stream-hash one file's term stream and docs columns.
fn file_digest(path: &str) -> Result<(u64, u64, u64, Vec<String>), anyhow::Error> {
    let data = bytes::Bytes::from(std::fs::read(path)?);
    let reader = VixReader::open(data.clone())?;
    let row_count = reader.row_count();

    let mut term_hasher = DefaultHasher::new();
    let mut term_count = 0u64;
    reader.for_each_term(&mut |key, doc_count, postings| {
        key.hash(&mut term_hasher);
        doc_count.hash(&mut term_hasher);
        postings.hash(&mut term_hasher);
        term_count += 1;
        Ok(())
    })?;

    let docs = VixDocs::open(data)?;
    let schema = docs.schema().clone();
    let mut columns: Vec<String> = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    columns.sort();
    let mut docs_hasher = DefaultHasher::new();
    docs.scan_docs(Some(&columns), None, None, &mut |batch| {
        for name in &columns {
            let column = batch
                .column_by_name(name)
                .ok_or_else(|| anyhow::anyhow!("scan lost column {name}"))?;
            let column = arrow::compute::cast(column, &DataType::Utf8)
                .unwrap_or_else(|_| Arc::clone(column));
            hash_column(&column, &mut docs_hasher);
        }
        Ok(())
    })?;
    Ok((
        row_count,
        term_count,
        term_hasher.finish() ^ docs_hasher.finish(),
        columns,
    ))
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

fn cmd_compare(a: &str, b: &str) -> Result<(), anyhow::Error> {
    let da = file_digest(a)?;
    let db = file_digest(b)?;
    anyhow::ensure!(
        da.3 == db.3,
        "docs schemas differ: {:?} vs {:?}",
        da.3,
        db.3
    );
    anyhow::ensure!(
        da.0 == db.0 && da.1 == db.1 && da.2 == db.2,
        "outputs differ: {a} (rows={}, terms={}, digest={:x}) vs {b} (rows={}, terms={}, \
         digest={:x})",
        da.0,
        da.1,
        da.2,
        db.0,
        db.1,
        db.2,
    );
    eprintln!(
        "outputs equivalent: rows={}, terms={}, digest={:x}",
        da.0, da.1, da.2
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("gen") => {
            let dir = args.get(2).expect("gen <dir> <files> <rows_per_file>");
            let files: usize = args.get(3).expect("files").parse()?;
            let rows: usize = args.get(4).expect("rows_per_file").parse()?;
            cmd_gen(dir, files, rows).await
        }
        Some("merge") => {
            let dir = args.get(2).expect("merge <dir> <out.vix> [--rebuild]");
            let out = args.get(3).expect("out.vix");
            let rebuild = args.get(4).is_some_and(|a| a == "--rebuild");
            cmd_merge(dir, out, rebuild)
        }
        Some("compare") => {
            let a = args.get(2).expect("compare <a.vix> <b.vix>");
            let b = args.get(3).expect("b.vix");
            cmd_compare(a, b)
        }
        _ => {
            eprintln!(
                "usage: merge_bench gen <dir> <files> <rows_per_file> | merge <dir> <out.vix> | \
                 compare <a.vix> <b.vix>"
            );
            std::process::exit(2);
        }
    }
}
