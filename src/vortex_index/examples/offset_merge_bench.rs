//! Manual release benchmark for the offset-map indexed merge.
//!
//! The fixture matches the pre-shift Linux baseline corpus. It builds two
//! production-shaped indexed inputs once, then repeats the single production
//! merge shape (`threads=4`, `merge_kway_threads=1`) for seven rounds. Docs
//! emission and `finish` happen outside the timer; every complete output is
//! checked byte-for-byte and every spill directory is checked for residue.
//!
//! ```text
//! cargo run -p vortex_index --release --example offset_merge_bench
//! ```
//! Optional baseline sizes:
//! ```text
//! O2_VIX_MERGE_BENCH_ROWS=262144 O2_VIX_MERGE_BENCH_ROUNDS=7 \
//!   cargo run -p vortex_index --release --example offset_merge_bench
//! ```
//! To enforce a same-host baseline ceiling explicitly:
//! ```text
//! O2_VIX_MERGE_BENCH_MAX_MS=72.617 \
//!   cargo run -p vortex_index --release --example offset_merge_bench
//! ```

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, bail};
use arrow::{
    array::{Array, ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use vortex_index::{DocIdMap, VixReader, VixWriter, VixWriterOptions};

const DEFAULT_ROWS_PER_INPUT: usize = 131_072;
const DEFAULT_ROUNDS: usize = 7;
const MERGE_THREADS: usize = 4;
const PRODUCTION_MIN_INPUT_TERMS: u64 = 1_000_000;

type BuiltOutput = (Vec<u8>, Option<Vec<u8>>);

fn env_usize(name: &str, default: usize) -> anyhow::Result<usize> {
    match std::env::var(name) {
        Ok(value) => value
            .parse::<usize>()
            .with_context(|| format!("{name} must be a positive integer"))
            .and_then(|value| {
                if value == 0 {
                    bail!("{name} must be greater than zero");
                }
                Ok(value)
            }),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("operation_name", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("host_id", DataType::Utf8, false),
        Field::new("message", DataType::Utf8, false),
        Field::new("duration_ns", DataType::Int64, false),
        Field::new("environment", DataType::Utf8, false),
    ]))
}

fn options() -> VixWriterOptions {
    VixWriterOptions {
        fts_field_names: vec!["message".to_string()],
        bloom_field_names: vec![
            "trace_id".to_string(),
            "span_id".to_string(),
            "host_id".to_string(),
        ],
        bloom_composite: true,
        bloom_only_field_names: vec!["host_id".to_string()],
        postings_plist_min_docs: 64,
        merge_kway_threads: 1,
        row_group_size: 8_192,
        ..Default::default()
    }
}

fn build_batch(schema: &SchemaRef, input: usize, rows: usize) -> (RecordBatch, StringArray) {
    let start = input * rows;
    let timestamps = Int64Array::from(
        (0..rows)
            .map(|row| 2_000_000_000_i64 - (start + row) as i64)
            .collect::<Vec<_>>(),
    );
    let trace_ids = StringArray::from(
        (0..rows)
            .map(|row| format!("{:032x}", start + row))
            .collect::<Vec<_>>(),
    );
    let span_ids = StringArray::from(
        (0..rows)
            .map(|row| format!("{:016x}", (start + row) * 17))
            .collect::<Vec<_>>(),
    );
    let services = StringArray::from(
        (0..rows)
            .map(|row| format!("svc-{:02}", (start + row) % 64))
            .collect::<Vec<_>>(),
    );
    let operations = StringArray::from(
        (0..rows)
            .map(|row| format!("operation-{:04}", (start + row) % 4096))
            .collect::<Vec<_>>(),
    );
    let statuses = StringArray::from(
        (0..rows)
            .map(|row| match (start + row) % 19 {
                0 => "error",
                1..=3 => "unset",
                _ => "ok",
            })
            .collect::<Vec<_>>(),
    );
    let regions = StringArray::from(
        (0..rows)
            .map(|row| format!("region-{:02}", (start + row) % 16))
            .collect::<Vec<_>>(),
    );
    let host_ids = StringArray::from(
        (0..rows)
            .map(|row| format!("host-{:08x}", (start + row) % 16_384))
            .collect::<Vec<_>>(),
    );
    let messages = StringArray::from(
        (0..rows)
            .map(|row| {
                format!(
                    "request {} operation {} completed with status {}",
                    start + row,
                    (start + row) % 4096,
                    if (start + row) % 19 == 0 {
                        "error"
                    } else {
                        "ok"
                    },
                )
            })
            .collect::<Vec<_>>(),
    );
    let durations = Int64Array::from(
        (0..rows)
            .map(|row| (((start + row) * 104_729) % 10_000_000) as i64)
            .collect::<Vec<_>>(),
    );
    let environments = StringArray::from(vec!["prod"; rows]);
    let sources = StringArray::from(
        (0..rows)
            .map(|row| {
                format!(
                    r#"{{"trace_id":"{:032x}","span_id":"{:016x}","service_name":"svc-{:02}","status":"{}"}}"#,
                    start + row,
                    (start + row) * 17,
                    (start + row) % 64,
                    if (start + row) % 19 == 0 {
                        "error"
                    } else {
                        "ok"
                    },
                )
            })
            .collect::<Vec<_>>(),
    );
    let columns: Vec<ArrayRef> = vec![
        Arc::new(timestamps),
        Arc::new(trace_ids),
        Arc::new(span_ids),
        Arc::new(services),
        Arc::new(operations),
        Arc::new(statuses),
        Arc::new(regions),
        Arc::new(host_ids),
        Arc::new(messages),
        Arc::new(durations),
        Arc::new(environments),
    ];
    (
        RecordBatch::try_new(Arc::clone(schema), columns).expect("benchmark batch is valid"),
        sources,
    )
}

fn open_input(
    schema: &SchemaRef,
    options: &VixWriterOptions,
    batch: &RecordBatch,
    source: &StringArray,
) -> anyhow::Result<VixReader> {
    let mut writer = VixWriter::new(schema, options.clone(), false);
    writer.push_batch_with_source(batch, source, None)?;
    let (data, index) = writer.finish()?;
    VixReader::open_with_index(Bytes::from(data), index.map(Bytes::from))
}

fn docs_columns(batch: &RecordBatch) -> Vec<(String, ArrayRef)> {
    batch
        .schema_ref()
        .fields()
        .iter()
        .skip(1)
        .zip(batch.columns().iter().skip(1))
        .map(|(field, column)| (field.name().clone(), Arc::clone(column)))
        .collect()
}

fn merge_once(
    schema: &SchemaRef,
    options: &VixWriterOptions,
    batches: &[RecordBatch; 2],
    sources: &[StringArray; 2],
    readers: &[VixReader; 2],
) -> anyhow::Result<(Duration, BuiltOutput)> {
    let scratch = tempfile::tempdir()?;
    let mut writer = VixWriter::new(
        schema,
        VixWriterOptions {
            term_spill_dir: Some(scratch.path().to_path_buf()),
            merge_kway_threads: 1,
            ..options.clone()
        },
        false,
    );
    let input_refs = [&readers[0], &readers[1]];
    let doc_maps = [
        DocIdMap::Offset(0),
        DocIdMap::Offset(batches[0].num_rows() as u32),
    ];

    let started = Instant::now();
    writer.merge_input_indexes(&input_refs, &doc_maps, MERGE_THREADS)?;
    let elapsed = started.elapsed();

    for (batch, source) in batches.iter().zip(sources) {
        let timestamps = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("timestamp column");
        writer.push_docs_rows_unindexed(timestamps, &docs_columns(batch), source, None)?;
    }
    let output = writer.finish()?;
    let residue = std::fs::read_dir(scratch.path())?.count();
    if residue != 0 {
        bail!("offset merge left {residue} named scratch entries");
    }
    Ok((elapsed, output))
}

fn median(samples: &mut [Duration]) -> Duration {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

fn main() -> anyhow::Result<()> {
    let rows = env_usize("O2_VIX_MERGE_BENCH_ROWS", DEFAULT_ROWS_PER_INPUT)?;
    let rounds = env_usize("O2_VIX_MERGE_BENCH_ROUNDS", DEFAULT_ROUNDS)?;
    let max_ms = match std::env::var("O2_VIX_MERGE_BENCH_MAX_MS") {
        Ok(value) => {
            let parsed = value
                .parse::<f64>()
                .with_context(|| "O2_VIX_MERGE_BENCH_MAX_MS must be a positive number")?;
            if !parsed.is_finite() || parsed <= 0.0 {
                bail!("O2_VIX_MERGE_BENCH_MAX_MS must be a positive finite number");
            }
            Some(parsed)
        }
        Err(std::env::VarError::NotPresent) => None,
        Err(error) => return Err(error.into()),
    };
    if rounds < 3 {
        bail!("O2_VIX_MERGE_BENCH_ROUNDS must be at least 3");
    }
    if rows > u32::MAX as usize / 2 {
        bail!("row count is too large for merged document ids");
    }

    let schema = schema();
    let options = options();
    let first = build_batch(&schema, 0, rows);
    let second = build_batch(&schema, 1, rows);
    let batches = [first.0, second.0];
    let sources = [first.1, second.1];
    let readers = [
        open_input(&schema, &options, &batches[0], &sources[0])?,
        open_input(&schema, &options, &batches[1], &sources[1])?,
    ];
    let input_terms: u64 = readers.iter().map(VixReader::term_count).sum();
    if input_terms < PRODUCTION_MIN_INPUT_TERMS {
        bail!(
            "benchmark input has {input_terms} terms; production mode requires at least \
             {PRODUCTION_MIN_INPUT_TERMS} (increase O2_VIX_MERGE_BENCH_ROWS)"
        );
    }

    let mut times = Vec::with_capacity(rounds);
    let mut reference: Option<BuiltOutput> = None;
    for _ in 0..rounds {
        let (elapsed, output) = merge_once(&schema, &options, &batches, &sources, &readers)?;
        if let Some(expected) = &reference {
            if output != *expected {
                bail!("offset merge output changed byte-for-byte between rounds");
            }
        } else {
            reference = Some(output);
        }
        times.push(elapsed);
    }

    let measured = median(&mut times);
    let output = reference.as_ref().expect("at least one round");
    println!(
        "rows/input={rows} input_terms={input_terms} rounds={rounds} output={}+{} bytes output_stability=yes",
        output.0.len(),
        output.1.as_ref().map_or(0, Vec::len),
    );
    println!(
        "offset merge median={:.3}ms",
        measured.as_secs_f64() * 1_000.0
    );
    if let Some(max_ms) = max_ms {
        let measured_ms = measured.as_secs_f64() * 1_000.0;
        println!("caller baseline ceiling={max_ms:.3}ms");
        if measured_ms > max_ms {
            bail!(
                "offset merge median {measured_ms:.3}ms exceeds caller baseline ceiling \
                 {max_ms:.3}ms"
            );
        }
    }
    Ok(())
}
