// Copyright 2026 OpenObserve Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

//! Ignored, local-only full-sidecar benchmark over an existing `.vix`/`.vxi`
//! pair. The test never changes the input and writes output only when
//! `O2_VIX_BENCH_OUT` is explicitly set.

use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Range,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use anyhow::{Context, bail, ensure};
use arrow::{
    array::{Array, ArrayRef as ArrowArrayRef, StringArray, UInt64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use futures::future::BoxFuture;
use vortex::{
    VortexSessionDefault,
    array::{
        ArrayRef as VortexArrayRef, VortexSessionExecute,
        arrays::{
            Dict, Shared, Struct, dict::DictArraySlotsExt, shared::SharedArrayExt,
            struct_::StructArrayExt,
        },
    },
    arrow::ArrowSessionExt,
    session::VortexSession,
};

use super::*;
use crate::{BytesRangeSource, VixDocs, VixRangeSource};

const EXCLUDED_VALUE_FIELDS: [&str; 2] = ["start_time", "end_time"];
const RAW_TERM_MAX: usize = 65_532;
const ROW_GROUP_ROWS: usize = 131_072;
const POSTINGS_CHUNK_BYTES: usize = 131_072;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Variant {
    A,
    B,
    C,
}

impl Variant {
    fn parse(value: &str) -> anyhow::Result<Self> {
        match value {
            "A" => Ok(Self::A),
            "B" => Ok(Self::B),
            "C" => Ok(Self::C),
            _ => bail!("phase=env O2_VIX_BENCH_VARIANT must be A, B, or C"),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::A => "A",
            Self::B => "B",
            Self::C => "C",
        }
    }

    fn excludes_times(self) -> bool {
        self != Self::A
    }
}

struct FileRangeSource {
    name: String,
    file: std::fs::File,
    len: u64,
}

impl VixRangeSource for FileRangeSource {
    fn len(&self) -> u64 {
        self.len
    }

    fn fetch(&self, range: Range<u64>) -> BoxFuture<'static, anyhow::Result<Bytes>> {
        use std::os::unix::fs::FileExt;

        let result = (|| {
            ensure!(
                range.start <= range.end && range.end <= self.len,
                "phase=range_fetch source={} range={}..{} length={}",
                self.name,
                range.start,
                range.end,
                self.len
            );
            let len: usize = (range.end - range.start)
                .try_into()
                .context("phase=range_fetch range length does not fit usize")?;
            let mut bytes = vec![0; len];
            self.file
                .read_exact_at(&mut bytes, range.start)
                .with_context(|| {
                    format!(
                        "phase=range_fetch source={} range={}..{}",
                        self.name, range.start, range.end
                    )
                })?;
            Ok(Bytes::from(bytes))
        })();
        Box::pin(futures::future::ready(result))
    }

    fn describe(&self) -> String {
        self.name.clone()
    }
}

fn file_source(path: &Path) -> anyhow::Result<Arc<dyn VixRangeSource>> {
    let file =
        std::fs::File::open(path).with_context(|| format!("phase=open file={}", path.display()))?;
    let len = file
        .metadata()
        .with_context(|| format!("phase=stat file={}", path.display()))?
        .len();
    Ok(Arc::new(FileRangeSource {
        name: path.display().to_string(),
        file,
        len,
    }))
}

#[derive(Default)]
struct CCensus {
    chunks: u64,
    fields: u64,
    root_dict_fields: u64,
    csr_fields: u64,
    fallback_fields: u64,
    csr_distinct: u64,
    csr_present_cells: u64,
    csr_postings: u64,
}

fn input_schema(docs_schema: &SchemaRef) -> Schema {
    Schema::new(
        docs_schema
            .fields()
            .iter()
            .filter(|field| {
                field.name() != SOURCE_COL_NAME && field.name() != ORIGINAL_DATA_COL_NAME
            })
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>(),
    )
}

fn writer_options(reader: &VixReader, variant: Variant) -> VixWriterOptions {
    VixWriterOptions {
        fts_field_names: reader.fts_fields().iter().cloned().collect(),
        value_index_excluded_field_names: if variant.excludes_times() {
            EXCLUDED_VALUE_FIELDS
                .iter()
                .map(|name| (*name).to_owned())
                .collect()
        } else {
            Vec::new()
        },
        bloom_field_names: vec!["trace_id".to_string(), "span_id".to_string()],
        bloom_composite: true,
        bloom_only_field_names: reader.bloom_only_fields().map(str::to_owned).collect(),
        // Source bloom-only metadata is sticky and authoritative. In
        // particular, do not make a second count-driven demotion decision.
        bloom_only_auto_ratio: 0.0,
        postings_chunk_bytes: POSTINGS_CHUNK_BYTES,
        max_raw_term_len: RAW_TERM_MAX,
        row_group_size: ROW_GROUP_ROWS,
        min_token_len: 2,
        max_token_len: 64,
        docs_passthrough: true,
        columns_complete: reader.columns_complete(),
        ..Default::default()
    }
}

fn decode_arrow(
    session: &VortexSession,
    array: &VortexArrayRef,
    data_type: &DataType,
    nullable: bool,
    variant: Variant,
    field: &str,
    chunk: u64,
    phase: &str,
) -> anyhow::Result<ArrowArrayRef> {
    let target = Field::new("", data_type.clone(), nullable);
    let mut ctx = session.create_execution_ctx();
    session
        .arrow()
        .execute_arrow(array.clone(), Some(&target), &mut ctx)
        .with_context(|| {
            format!(
                "variant={} field={} chunk={} phase={phase}",
                variant.label(),
                field,
                chunk
            )
        })
}

fn sorted_unique(docs: &mut Vec<u32>, variant: Variant, field: &str, chunk: u64, phase: &str) {
    docs.sort_unstable();
    docs.dedup();
    assert!(
        docs.windows(2).all(|pair| pair[0] < pair[1]),
        "variant={} field={} chunk={} phase={} postings are not strictly sorted",
        variant.label(),
        field,
        chunk,
        phase
    );
}

fn emit_direct_dict(
    writer: &mut VixWriter,
    field: &Field,
    field_id: u16,
    codes: &UInt64Array,
    values: &ArrowArrayRef,
    slot_counts: &[usize],
    finite: Option<&[bool]>,
    first_doc: u64,
    variant: Variant,
    chunk: u64,
) -> anyhow::Result<(usize, usize)> {
    let field_name = field.name();
    assert_eq!(slot_counts.len(), values.len());
    let mut offsets = Vec::with_capacity(values.len() + 1);
    offsets.push(0usize);
    for &count in slot_counts {
        let next = offsets
            .last()
            .copied()
            .expect("offset zero was inserted")
            .checked_add(count)
            .with_context(|| {
                format!(
                    "variant={} field={} chunk={} phase=csr_count postings count overflow",
                    variant.label(),
                    field_name,
                    chunk
                )
            })?;
        offsets.push(next);
    }
    let present_cells = *offsets.last().expect("offset zero was inserted");
    let distinct = slot_counts.iter().filter(|count| **count != 0).count();
    assert!(present_cells > 0 && distinct.saturating_mul(50) <= present_cells);

    let mut postings = vec![0u32; present_cells];
    let mut cursors = offsets[..values.len()].to_vec();
    for row in 0..codes.len() {
        if codes.is_null(row) {
            continue;
        }
        let code = usize::try_from(codes.value(row)).expect("code was checked during CSR gate");
        assert!(
            code < values.len(),
            "variant={} field={} chunk={} phase=code_bounds code={} slots={}",
            variant.label(),
            field_name,
            chunk,
            code,
            values.len()
        );
        let present = finite.map_or_else(|| values.is_valid(code), |mask| mask[code]);
        if !present {
            continue;
        }
        let doc: u32 = (first_doc + row as u64).try_into().with_context(|| {
            format!(
                "variant={} field={} chunk={} phase=csr_fill doc id exceeds u32",
                variant.label(),
                field_name,
                chunk
            )
        })?;
        postings[cursors[code]] = doc;
        cursors[code] += 1;
    }
    for slot in 0..values.len() {
        assert!(
            postings[offsets[slot]..offsets[slot + 1]]
                .windows(2)
                .all(|pair| pair[0] < pair[1]),
            "variant={} field={} chunk={} phase=csr_slot_sort slot postings are not sorted",
            variant.label(),
            field_name,
            chunk
        );
    }

    let mut terms: BTreeMap<Vec<u8>, Vec<u32>> = BTreeMap::new();
    let mut key_docs = Vec::with_capacity(present_cells);
    if let Some(strings) = StringColumn::try_new(values.as_ref()) {
        for slot in 0..values.len() {
            let docs = &postings[offsets[slot]..offsets[slot + 1]];
            if docs.is_empty() {
                continue;
            }
            let value = strings
                .value(slot)
                .expect("CSR excludes null dictionary values");
            key_docs.extend_from_slice(docs);
            if writer.fts_fields.contains(field_name) {
                let tokens: BTreeSet<Vec<u8>> =
                    o2_tokenize(value, writer.opts.min_token_len, writer.opts.max_token_len)
                        .map(|token| token.as_bytes().to_vec())
                        .collect();
                for token in tokens {
                    terms.entry(token).or_default().extend_from_slice(docs);
                }
            } else if value.len() > writer.opts.max_raw_term_len {
                let skipped = u64::try_from(docs.len()).unwrap_or(u64::MAX);
                let counter = writer.oversize_skips.entry(field_name.clone()).or_default();
                *counter = counter.checked_add(skipped).with_context(|| {
                    format!(
                        "variant={} field={} chunk={} phase=oversize_count overflow",
                        variant.label(),
                        field_name,
                        chunk
                    )
                })?;
            } else {
                terms
                    .entry(value.as_bytes().to_vec())
                    .or_default()
                    .extend_from_slice(docs);
            }
        }
    } else if let Some(numbers) = NumericColumn::try_new(values.as_ref()) {
        let mut text = String::new();
        for slot in 0..values.len() {
            let docs = &postings[offsets[slot]..offsets[slot + 1]];
            if docs.is_empty() {
                continue;
            }
            assert!(
                numbers.canonical_into(slot, &mut text),
                "variant={} field={} chunk={} phase=numeric_token present slot has no canonical value",
                variant.label(),
                field_name,
                chunk
            );
            key_docs.extend_from_slice(docs);
            if text.len() + 1 > writer.opts.max_raw_term_len {
                let skipped = u64::try_from(docs.len()).unwrap_or(u64::MAX);
                let counter = writer.oversize_skips.entry(field_name.clone()).or_default();
                *counter = counter.checked_add(skipped).with_context(|| {
                    format!(
                        "variant={} field={} chunk={} phase=oversize_count overflow",
                        variant.label(),
                        field_name,
                        chunk
                    )
                })?;
                continue;
            }
            let mut token = Vec::with_capacity(text.len() + 1);
            token.push(NUMERIC_TERM_TAG);
            token.extend_from_slice(text.as_bytes());
            terms.entry(token).or_default().extend_from_slice(docs);
        }
    } else {
        bail!(
            "variant={} field={} chunk={} phase=direct_terms unsupported natural type",
            variant.label(),
            field_name,
            chunk
        );
    }

    for (token, docs) in &mut terms {
        sorted_unique(docs, variant, field_name, chunk, "value_postings_sort");
        writer.terms.extend(field_id, token, docs.iter().copied());
    }
    sorted_unique(
        &mut key_docs,
        variant,
        field_name,
        chunk,
        "key_postings_sort",
    );
    writer.terms.extend(
        KEY_FIELD_ID,
        field_name.as_bytes(),
        key_docs.iter().copied(),
    );

    Ok((distinct, present_cells))
}

fn scan_decoded(
    docs: &VixDocs,
    writer: &mut VixWriter,
    projection: &[String],
    expected_rows: u64,
    variant: Variant,
    peak: &mut usize,
) -> anyhow::Result<u64> {
    let mut scanned = 0u64;
    docs.scan_docs(Some(projection), None, None, &mut |batch| {
        let source = StringArray::from_iter_values((0..batch.num_rows()).map(|_| ""));
        writer
            .push_batch_with_source_index_only(&batch, &source, None)
            .with_context(|| {
                format!(
                    "variant={} field=* chunk={} phase=index_decoded",
                    variant.label(),
                    scanned
                )
            })?;
        scanned = scanned
            .checked_add(batch.num_rows() as u64)
            .context("phase=row_coverage decoded scan row count overflow")?;
        *peak = (*peak).max(writer.terms.estimated_bytes());
        Ok(())
    })
    .with_context(|| {
        format!(
            "variant={} field=* chunk=* phase=scan_docs",
            variant.label()
        )
    })?;
    assert_eq!(
        scanned,
        expected_rows,
        "variant={} phase=row_coverage decoded scan",
        variant.label()
    );
    Ok(scanned)
}

fn scan_csr(
    docs: &VixDocs,
    docs_schema: &SchemaRef,
    writer: &mut VixWriter,
    expected_rows: u64,
    variant: Variant,
    peak: &mut usize,
) -> anyhow::Result<(u64, CCensus)> {
    let session = VortexSession::default();
    let mut scanned = 0u64;
    let mut chunk_index = 0u64;
    let mut census = CCensus::default();

    docs.scan_docs_encoded_chunks(&mut |chunk| {
        writer
            .check_push_mode(DocsPushMode::IndexOnly)
            .with_context(|| {
                format!(
                    "variant={} field=* chunk={} phase=index_mode",
                    variant.label(),
                    chunk_index
                )
            })?;
        let first_doc = writer
            .next_first_doc(DocsPushMode::IndexOnly, chunk.rows())
            .with_context(|| {
                format!(
                    "variant={} field=* chunk={} phase=doc_cursor",
                    variant.label(),
                    chunk_index
                )
            })?;
        assert_eq!(
            first_doc,
            scanned,
            "variant={} phase=row_coverage chunk={} doc cursor",
            variant.label(),
            chunk_index
        );
        let strukt = chunk.array.as_typed::<Struct>().ok_or_else(|| {
            anyhow::anyhow!(
                "variant={} field=* chunk={} phase=struct_root encoded chunk is not Struct",
                variant.label(),
                chunk_index
            )
        })?;
        assert_eq!(
            strukt.len(),
            chunk.rows(),
            "variant={} phase=row_coverage chunk={} struct length",
            variant.label(),
            chunk_index
        );
        assert_eq!(
            strukt.names().len(),
            strukt.unmasked_fields().len(),
            "variant={} phase=struct_fields chunk={}",
            variant.label(),
            chunk_index
        );

        let mut fallback_fields = Vec::new();
        let mut fallback_arrays = Vec::new();
        for (name, encoded) in strukt
            .names()
            .iter()
            .zip(strukt.unmasked_fields().iter())
        {
            let name = name.as_ref();
            if NON_INDEXED_COLS.contains(&name) || name == SOURCE_COL_NAME {
                continue;
            }
            census.fields += 1;
            let schema_field = docs_schema.field_with_name(name).with_context(|| {
                format!(
                    "variant={} field={} chunk={} phase=schema_lookup",
                    variant.label(),
                    name,
                    chunk_index
                )
            })?;
            // Runtime execution caches may put exactly one transparent Shared
            // wrapper at the field root. Do not search descendants for Dict.
            let stored = match encoded.as_typed::<Shared>() {
                Some(shared) => shared.source().clone(),
                None => encoded.clone(),
            };

            let direct_role = writer
                .term_field_ids
                .get(name)
                .copied()
                .filter(|_| !writer.demoted_fields.contains(name))
                .filter(|_| !writer.value_index_excluded_fields.contains(name))
                .filter(|field_id| !writer.bloom_only.contains_key(field_id));

            let fallback = if let Some(dict) = stored.as_typed::<Dict>() {
                census.root_dict_fields += 1;
                let codes = decode_arrow(
                    &session,
                    dict.codes(),
                    &DataType::UInt64,
                    true,
                    variant,
                    name,
                    chunk_index,
                    "dict_codes",
                )?;
                let codes = codes
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .cloned()
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "variant={} field={} chunk={} phase=dict_codes result is not UInt64",
                            variant.label(),
                            name,
                            chunk_index
                        )
                    })?;
                ensure!(
                    codes.len() == chunk.rows(),
                    "variant={} field={} chunk={} phase=dict_codes rows={} expected={}",
                    variant.label(),
                    name,
                    chunk_index,
                    codes.len(),
                    chunk.rows()
                );
                let values = decode_arrow(
                    &session,
                    dict.values(),
                    schema_field.data_type(),
                    dict.values().dtype().is_nullable(),
                    variant,
                    name,
                    chunk_index,
                    "dict_values",
                )?;
                let finite = finite_float_mask(values.as_ref());
                let mut slot_counts = vec![0usize; values.len()];
                let mut present_cells = 0usize;
                for row in 0..codes.len() {
                    if codes.is_null(row) {
                        continue;
                    }
                    let code: usize = codes.value(row).try_into().with_context(|| {
                        format!(
                            "variant={} field={} chunk={} phase=code_bounds code does not fit usize",
                            variant.label(),
                            name,
                            chunk_index
                        )
                    })?;
                    assert!(
                        code < values.len(),
                        "variant={} field={} chunk={} phase=code_bounds code={} slots={}",
                        variant.label(),
                        name,
                        chunk_index,
                        code,
                        values.len()
                    );
                    let present = finite
                        .as_ref()
                        .map_or_else(|| values.is_valid(code), |mask| mask[code]);
                    if present {
                        slot_counts[code] += 1;
                        present_cells += 1;
                    }
                }
                let distinct = slot_counts.iter().filter(|count| **count != 0).count();
                let sparse = present_cells > 0
                    && (distinct as u128) * 50 <= present_cells as u128;
                if let Some(field_id) = direct_role.filter(|_| sparse) {
                    let (direct_distinct, direct_present) = emit_direct_dict(
                        writer,
                        schema_field,
                        field_id,
                        &codes,
                        &values,
                        &slot_counts,
                        finite.as_deref(),
                        first_doc,
                        variant,
                        chunk_index,
                    )?;
                    assert_eq!(direct_distinct, distinct);
                    assert_eq!(direct_present, present_cells);
                    census.csr_fields += 1;
                    census.csr_distinct += direct_distinct as u64;
                    census.csr_present_cells += direct_present as u64;
                    census.csr_postings += direct_present as u64;
                    None
                } else {
                    Some(arrow::compute::take(values.as_ref(), &codes, None).with_context(|| {
                        format!(
                            "variant={} field={} chunk={} phase=dict_fallback_take",
                            variant.label(),
                            name,
                            chunk_index
                        )
                    })?)
                }
            } else {
                Some(decode_arrow(
                    &session,
                    &stored,
                    schema_field.data_type(),
                    stored.dtype().is_nullable(),
                    variant,
                    name,
                    chunk_index,
                    "fallback_decode",
                )?)
            };

            if let Some(array) = fallback {
                census.fallback_fields += 1;
                fallback_fields.push(schema_field.clone());
                fallback_arrays.push(array);
            }
        }

        let fallback_schema = Arc::new(Schema::new(fallback_fields));
        let fallback_batch = RecordBatch::try_new(fallback_schema, fallback_arrays).with_context(|| {
            format!(
                "variant={} field=* chunk={} phase=fallback_batch",
                variant.label(),
                chunk_index
            )
        })?;
        assert_eq!(
            fallback_batch.num_rows(),
            chunk.rows(),
            "variant={} phase=row_coverage chunk={} fallback batch",
            variant.label(),
            chunk_index
        );
        writer.index_value_terms(&fallback_batch, first_doc);
        writer.index_key_terms(&fallback_batch, first_doc);
        writer.advance_index_only_cursor(chunk.rows());
        writer.maybe_auto_demote_bloom_only_early().with_context(|| {
            format!(
                "variant={} field=* chunk={} phase=auto_demotion_hook",
                variant.label(),
                chunk_index
            )
        })?;
        writer.maybe_spill_terms().with_context(|| {
            format!(
                "variant={} field=* chunk={} phase=spill_hook",
                variant.label(),
                chunk_index
            )
        })?;

        scanned = scanned
            .checked_add(chunk.rows() as u64)
            .context("phase=row_coverage encoded scan row count overflow")?;
        *peak = (*peak).max(writer.terms.estimated_bytes());
        census.chunks += 1;
        chunk_index += 1;
        Ok(())
    })
    .with_context(|| format!("variant={} field=* chunk=* phase=scan_encoded", variant.label()))?;

    assert_eq!(
        scanned,
        expected_rows,
        "variant={} phase=row_coverage encoded scan",
        variant.label()
    );
    assert_eq!(writer.index_only_rows, Some(expected_rows));
    Ok((scanned, census))
}

fn assert_variant_capabilities(
    source: &VixReader,
    output: &VixReader,
    variant: Variant,
) -> anyhow::Result<()> {
    assert_eq!(output.row_count(), source.row_count());
    let source_names: Vec<&str> = source
        .field_entries()
        .iter()
        .map(|entry| entry.name.as_str())
        .collect();
    let output_names: Vec<&str> = output
        .field_entries()
        .iter()
        .map(|entry| entry.name.as_str())
        .collect();
    assert_eq!(
        output_names,
        source_names,
        "variant={} phase=field_id_slots",
        variant.label()
    );
    assert_eq!(output.fts_fields(), source.fts_fields());
    let source_bloom: BTreeSet<&str> = source.bloom_only_fields().collect();
    let output_bloom: BTreeSet<&str> = output.bloom_only_fields().collect();
    assert_eq!(output_bloom, source_bloom);

    for field in EXCLUDED_VALUE_FIELDS {
        ensure!(
            source.has_term_capability(field),
            "variant={} field={} chunk=* phase=source_capability expected exact terms",
            variant.label(),
            field
        );
        assert!(output.has_field(field));
        assert_eq!(
            output.has_term_capability(field),
            !variant.excludes_times(),
            "variant={} field={} phase=value_capability",
            variant.label(),
            field
        );
        let entry = output
            .field_entries()
            .iter()
            .find(|entry| entry.name == field)
            .expect("field table names were checked");
        assert!(entry.types.iter().any(|kind| kind == FIELD_TYPE_CS));
        if variant.excludes_times() {
            assert!(!entry.types.iter().any(|kind| {
                kind == FIELD_TYPE_TERM || kind == FIELD_TYPE_FTS || kind == FIELD_TYPE_BLOOM
            }));
        } else {
            assert!(entry.types.iter().any(|kind| kind == FIELD_TYPE_TERM));
        }
        assert_eq!(
            output.key_exists(field)?,
            source.key_exists(field)?,
            "variant={} field={} phase=key_postings",
            variant.label(),
            field
        );
    }
    Ok(())
}

fn run_benchmark() -> anyhow::Result<()> {
    let total_started = Instant::now();
    let path = PathBuf::from(
        std::env::var_os("O2_VIX_BENCH_FILE").context("phase=env O2_VIX_BENCH_FILE is required")?,
    );
    ensure!(
        path.extension().is_some_and(|extension| extension == "vix"),
        "phase=env O2_VIX_BENCH_FILE must name a .vix file"
    );
    let variant_text = std::env::var("O2_VIX_BENCH_VARIANT")
        .context("phase=env O2_VIX_BENCH_VARIANT is required")?;
    let variant = Variant::parse(&variant_text)?;
    let output_path = std::env::var_os("O2_VIX_BENCH_OUT")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from);
    let index_path = path.with_extension("vxi");
    ensure!(
        index_path.is_file(),
        "variant={} phase=open sibling .vxi is required",
        variant.label()
    );

    let source_reader =
        VixReader::open_ranged_with_index(file_source(&path)?, Some(file_source(&index_path)?))
            .with_context(|| format!("variant={} phase=open_reader", variant.label()))?;
    ensure!(
        source_reader.has_index(),
        "phase=open_reader source has no index"
    );
    ensure!(
        source_reader.row_count() <= u64::from(u32::MAX),
        "variant={} phase=row_capacity row count exceeds u32 posting space",
        variant.label()
    );
    let docs_schema = source_reader
        .docs_schema()
        .with_context(|| format!("variant={} phase=docs_schema", variant.label()))?;
    let schema = input_schema(&docs_schema);
    let store_original = docs_schema.field_with_name(ORIGINAL_DATA_COL_NAME).is_ok();
    let options = writer_options(&source_reader, variant);
    assert_eq!(options.max_raw_term_len, RAW_TERM_MAX);
    assert_eq!(options.postings_chunk_bytes, POSTINGS_CHUNK_BYTES);
    assert_eq!(options.row_group_size, ROW_GROUP_ROWS);
    assert_eq!(options.min_token_len, 2);
    assert_eq!(options.max_token_len, 64);
    assert!(options.docs_passthrough);
    assert_eq!(options.columns_complete, source_reader.columns_complete());
    assert_eq!(options.bloom_only_auto_ratio, 0.0);

    let mut writer = VixWriter::new(&schema, options, store_original);
    for (field_id, field_name) in writer.term_fields.iter().enumerate() {
        assert_eq!(
            writer.term_field_ids.get(field_name),
            Some(&(field_id as u16))
        );
    }
    for field in EXCLUDED_VALUE_FIELDS {
        assert!(writer.term_field_ids.contains_key(field));
        assert_eq!(
            writer.value_index_excluded_fields.contains(field),
            variant.excludes_times()
        );
    }

    let docs = VixDocs::open_ranged(file_source(&path)?)
        .with_context(|| format!("variant={} phase=open_docs", variant.label()))?;
    let projection: Vec<String> = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let mut peak_logical_term_bytes = writer.terms.estimated_bytes();
    let scan_started = Instant::now();
    let (scanned, census) = match variant {
        Variant::A | Variant::B => (
            scan_decoded(
                &docs,
                &mut writer,
                &projection,
                source_reader.row_count(),
                variant,
                &mut peak_logical_term_bytes,
            )?,
            CCensus::default(),
        ),
        Variant::C => scan_csr(
            &docs,
            &docs_schema,
            &mut writer,
            source_reader.row_count(),
            variant,
            &mut peak_logical_term_bytes,
        )?,
    };
    let scan_ns = scan_started.elapsed().as_nanos();
    assert_eq!(scanned, source_reader.row_count());
    assert_eq!(writer.index_only_rows, Some(source_reader.row_count()));

    let finish_started = Instant::now();
    let (sidecar, stats) = writer
        .finish_index_sidecar(source_reader.row_count())
        .with_context(|| format!("variant={} phase=finish", variant.label()))?;
    let finish_ns = finish_started.elapsed().as_nanos();
    let total_ns = total_started.elapsed().as_nanos();
    assert_eq!(stats.row_count, source_reader.row_count());
    assert_eq!(stats.index_size, sidecar.len() as u64);

    let output_reader = VixReader::open_ranged_with_index(
        file_source(&path)?,
        Some(BytesRangeSource::new(
            format!("full-path-bench-{}.vxi", variant.label()),
            Bytes::from(sidecar.clone()),
        )),
    )
    .with_context(|| format!("variant={} phase=validate_sidecar", variant.label()))?;
    assert_variant_capabilities(&source_reader, &output_reader, variant)?;

    if let Some(output_path) = output_path {
        std::fs::write(&output_path, &sidecar).with_context(|| {
            format!(
                "variant={} phase=write_output file={}",
                variant.label(),
                output_path.display()
            )
        })?;
    }

    println!(
        "vix_full_path_bench variant={} metric=phase phase=scan ns={scan_ns}",
        variant.label()
    );
    println!(
        "vix_full_path_bench variant={} metric=phase phase=finish ns={finish_ns}",
        variant.label()
    );
    println!(
        "vix_full_path_bench variant={} metric=c_census chunks={} fields={} root_dict_fields={} csr_fields={} fallback_fields={} csr_distinct={} csr_present_cells={} csr_postings={}",
        variant.label(),
        census.chunks,
        census.fields,
        census.root_dict_fields,
        census.csr_fields,
        census.fallback_fields,
        census.csr_distinct,
        census.csr_present_cells,
        census.csr_postings
    );
    println!(
        "vix_full_path_bench variant={} metric=summary rows={} terms={} bytes={} peak_logical_term_bytes={} scan_ns={} finish_ns={} total_ns={}",
        variant.label(),
        scanned,
        stats.term_count,
        sidecar.len(),
        peak_logical_term_bytes,
        scan_ns,
        finish_ns,
        total_ns
    );
    Ok(())
}

#[test]
fn dictionary_csr_emission_matches_decoded_full_sidecar() -> anyhow::Result<()> {
    use arrow::array::Int64Array;

    const ROWS: usize = 128;
    let schema = Arc::new(Schema::new(vec![
        Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
        Field::new("svc", DataType::Utf8, false),
    ]));
    let timestamps = Int64Array::from_iter_values((0..ROWS).rev().map(|row| row as i64));
    let strings =
        StringArray::from_iter_values((0..ROWS).map(|row| if row % 2 == 0 { "api" } else { "db" }));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(timestamps), Arc::new(strings)],
    )?;
    let empty_source = StringArray::from_iter_values((0..ROWS).map(|_| ""));
    let options = VixWriterOptions {
        bloom_composite: true,
        docs_passthrough: true,
        ..VixWriterOptions::default()
    };

    let mut decoded = VixWriter::new(&schema, options.clone(), false);
    decoded.push_batch_with_source_index_only(&batch, &empty_source, None)?;
    let (decoded_sidecar, decoded_stats) = decoded.finish_index_sidecar(ROWS as u64)?;

    let mut direct = VixWriter::new(&schema, options, false);
    direct.check_push_mode(DocsPushMode::IndexOnly)?;
    let first_doc = direct.next_first_doc(DocsPushMode::IndexOnly, ROWS)?;
    let codes = UInt64Array::from_iter_values((0..ROWS).map(|row| (row % 2) as u64));
    let values: ArrowArrayRef = Arc::new(StringArray::from(vec!["api", "db"]));
    let field = schema.field_with_name("svc")?;
    let field_id = *direct
        .term_field_ids
        .get("svc")
        .context("test schema must index svc")?;
    emit_direct_dict(
        &mut direct,
        field,
        field_id,
        &codes,
        &values,
        &[ROWS / 2, ROWS / 2],
        None,
        first_doc,
        Variant::C,
        0,
    )?;
    direct.advance_index_only_cursor(ROWS);
    let (direct_sidecar, direct_stats) = direct.finish_index_sidecar(ROWS as u64)?;

    assert_eq!(direct_stats.term_count, 3);
    assert_eq!(direct_stats.term_count, decoded_stats.term_count);
    assert_eq!(direct_sidecar, decoded_sidecar);
    Ok(())
}

#[test]
#[ignore = "manual local full-sidecar A/B/C benchmark; set O2_VIX_BENCH_FILE and O2_VIX_BENCH_VARIANT"]
fn full_path_sidecar_bench() -> anyhow::Result<()> {
    run_benchmark()
}
