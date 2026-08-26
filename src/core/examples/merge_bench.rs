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
//!   compare [--multiset] <a.vix> <b.vix>
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

/// Prod `default` traces stream settings (v2: every present field is a
/// docs column — there is no column-store list).
fn stream_settings() -> (Vec<String>, Vec<String>) {
    let fts: Vec<String> = vec![];
    let bloom: Vec<String> = vec!["trace_id".to_string()];
    (fts, bloom)
}

async fn cmd_gen(
    dir: &str,
    files: usize,
    rows_per_file: usize,
    overlap: bool,
    narrow: bool,
    vary_schema: bool,
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
        let file_schema = Arc::new(schema.project(&keep)?);
        let mut batches = Vec::new();
        let mut left = rows_per_file;
        let mut offset = 0usize;
        while left > 0 {
            let n = left.min(BATCH_ROWS);
            let batch = make_batch(&schema, &mut rng, file_base + offset as i64, n);
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
            // v3 split: the index sidecar sits next to the data object
            let index_path = path.with_extension("vxi");
            let index: Option<std::sync::Arc<dyn vortex_index::VixRangeSource>> =
                match std::fs::File::open(&index_path) {
                    Ok(file) => {
                        let len = file.metadata()?.len();
                        Some(std::sync::Arc::new(FileRangeSource {
                            name: format!("{name}.vxi"),
                            file,
                            len,
                        }))
                    }
                    Err(_) => None,
                };
            Ok((name, source, index))
        })
        .collect()
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
) -> (Schema, Vec<String>) {
    let registry = spans_schema();
    let mut fts: Vec<String> = Vec::new();
    let mut latest_fields: Vec<Field> = Vec::new();
    for (_, data, index) in inputs {
        let reader =
            VixReader::open_ranged_with_index(std::sync::Arc::clone(data), index.clone()).unwrap();
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

fn cmd_merge(dir: &str, out: &str, rebuild: bool) -> Result<(), anyhow::Error> {
    let mib = |bytes: usize| bytes as f64 / (1024.0 * 1024.0);
    let started = Instant::now();
    let inputs = load_inputs(dir)?;
    let total_bytes: u64 = inputs
        .iter()
        .map(|(_, data, index)| data.len() + index.as_ref().map_or(0, |i| i.len()))
        .sum();
    let load_elapsed = started.elapsed();

    let (latest_schema, fts) = derive_schema(&inputs);
    let bloom = vec!["trace_id".to_string()];
    eprintln!(
        "opened {} files (ranged) / {:.1} MiB in {load_elapsed:.2?}; fts={fts:?}",
        inputs.len(),
        mib(total_bytes as usize),
    );

    let started = Instant::now();
    let result = if rebuild {
        openobserve_core::vix::core_writer::merge_core_files_rebuild(
            config::meta::stream::StreamType::Logs,
            &inputs,
            &latest_schema,
            &fts,
            &bloom,
        )?
    } else {
        openobserve_core::vix::core_writer::merge_core_files(
            config::meta::stream::StreamType::Logs,
            &inputs,
            &latest_schema,
            &fts,
            &bloom,
        )?
    };
    let merge_elapsed = started.elapsed();
    let out_len = result.output.len();
    // v3 split: write the merged sidecar next to the data output
    if let Some(index) = &result.index {
        std::fs::write(
            std::path::Path::new(out).with_extension("vxi"),
            index,
        )?;
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
        "merge: {merge_elapsed:.2?}  used_index_merge={}  docs_batches={}  \
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
    eprintln!("peak memory: {}", rss_lines());
    Ok(())
}

/// Stream-hash one file's term stream and docs columns. `multiset` (#51c-c)
/// is the ORDER-INSENSITIVE mode: per-ROW content hashes folded
/// commutatively (wrapping add) and the term stream hashed WITHOUT postings
/// doc ids — the only valid comparison between outputs whose row order
/// legitimately differs (concat-order vs sorted).
fn file_digest(path: &str, multiset: bool) -> Result<(u64, u64, u64, Vec<String>), anyhow::Error> {
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
    let mut columns: Vec<String> = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    columns.sort();
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
        term_hasher.finish() ^ docs_digest,
        columns,
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

fn cmd_compare(a: &str, b: &str, multiset: bool) -> Result<(), anyhow::Error> {
    let da = file_digest(a, multiset)?;
    let db = file_digest(b, multiset)?;
    anyhow::ensure!(
        da.3 == db.3,
        "docs schemas differ: {:?} vs {:?}",
        da.3,
        db.3
    );
    anyhow::ensure!(
        da.0 == db.0 && da.1 == db.1 && da.2 == db.2,
        "outputs differ ({} mode): {a} (rows={}, terms={}, digest={:x}) vs {b} (rows={}, \
         terms={}, digest={:x})",
        if multiset { "multiset" } else { "row-order" },
        da.0,
        da.1,
        da.2,
        db.0,
        db.1,
        db.2,
    );
    eprintln!(
        "outputs equivalent ({} mode): rows={}, terms={}, digest={:x}",
        if multiset { "multiset" } else { "row-order" },
        da.0,
        da.1,
        da.2
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
    let flag = |name: &str| args.iter().skip(2).any(|a| a == name);
    match args.get(1).map(String::as_str) {
        Some("gen") => {
            let dir = args
                .get(2)
                .expect("gen <dir> <files> <rows_per_file> [--heal] [--overlap] [--vary-schema]");
            let files: usize = args.get(3).expect("files").parse()?;
            let rows: usize = args.get(4).expect("rows_per_file").parse()?;
            if flag("--heal") {
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
            )
            .await
        }
        Some("merge") => {
            let dir = args.get(2).expect("merge <dir> <out.vix> [--rebuild]");
            let out = args.get(3).expect("out.vix");
            // #51c passthrough + #51c-c concatenation are the DEFAULT merge
            // shapes now — no knobs to set.
            cmd_merge(dir, out, flag("--rebuild"))
        }
        Some("compare") => {
            // flags may precede the paths: compare [--multiset] <a> <b>
            let multiset = flag("--multiset") || args.get(2).is_some_and(|a| a == "--multiset");
            let paths: Vec<&String> = args
                .iter()
                .skip(2)
                .filter(|a| !a.starts_with("--"))
                .collect();
            let a = paths.first().expect("compare [--multiset] <a.vix> <b.vix>");
            let b = paths.get(1).expect("b.vix");
            cmd_compare(a, b, multiset)
        }
        _ => {
            eprintln!(
                "usage: merge_bench gen <dir> <files> <rows_per_file> [--heal] [--overlap] \
                 [--vary-schema] | \
                 merge <dir> <out.vix> [--rebuild] | \
                 compare [--multiset] <a.vix> <b.vix>"
            );
            std::process::exit(2);
        }
    }
}
