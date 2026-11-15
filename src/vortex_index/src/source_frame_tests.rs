// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use std::sync::{Arc, atomic::AtomicU64};

use arrow::{
    array::{Array, ArrayRef as ArrowArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use vortex::{
    VortexSessionDefault,
    array::{
        ArrayRef,
        arrays::{Struct, struct_::StructArraySlotsExt},
    },
    compressor::BtrBlocksCompressorBuilder,
    editions::{CORE_2025_10_0, EditionSessionExt, EnabledEditions},
    encodings::zstd::Zstd,
    file::{OpenOptionsSessionExt, WriteStrategyBuilder},
    io::{
        runtime::{BlockingRuntime, single::SingleThreadRuntime},
        session::RuntimeSessionExt,
    },
    session::VortexSession,
};

use crate::{
    VixWriter, VixWriterOptions,
    container::{
        BlobHandle, ContainerSink, DocsBlobEncoder, RowSelection, docs_strategy, scan_blob,
        write_vortex_blob,
    },
    flat::BufferBoundedFlatStrategy,
};

const ROWS: usize = 18_433;

fn fixture() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("_source", DataType::Utf8, false),
        Field::new("_original", DataType::Utf8, false),
    ]));
    let source = StringArray::from_iter_values((0..ROWS).map(|row| {
        format!(
            "{{\"row\":{row},\"trace_id\":\"{:016x}\",\"body\":\"{}\"}}",
            (row as u64).wrapping_mul(0x9e3779b97f4a7c15),
            "repeated long log message with multiple searchable words ".repeat(8)
        )
    }));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from_iter_values(
                (0..ROWS).map(|row| row as i64),
            )) as ArrowArrayRef,
            Arc::new(source.clone()),
            Arc::new(source),
        ],
    )
    .unwrap()
}

fn session(runtime: &SingleThreadRuntime) -> VortexSession {
    let session = VortexSession::default().with_handle(runtime.handle());
    session.register(EnabledEditions::default());
    session.enable_edition(CORE_2025_10_0).unwrap();
    session
}

fn arrays(blob: &[u8]) -> Vec<ArrayRef> {
    let runtime = SingleThreadRuntime::default();
    let session = session(&runtime);
    let file = session
        .open_options()
        .open_buffer(Bytes::copy_from_slice(blob))
        .unwrap();
    file.scan()
        .unwrap()
        .into_array_iter(&runtime)
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

fn frame_rows(blob: &[u8], field: usize) -> Vec<usize> {
    let mut rows = Vec::new();
    for root in arrays(blob) {
        let root = root.as_typed::<Struct>().unwrap();
        for node in root.fields()[field].depth_first_traversal() {
            if let Some(zstd) = node.as_opt::<Zstd>() {
                let parts = zstd
                    .data()
                    .clone()
                    .into_parts(vortex::array::validity::Validity::NonNullable);
                assert!(parts.dictionary.is_none());
                for (frame, meta) in parts.frames.iter().zip(parts.metadata.frames()) {
                    let raw = zstd::bulk::decompress(
                        frame.as_slice(),
                        meta.get_uncompressed_size().unwrap(),
                    )
                    .unwrap();
                    let mut pos = 0;
                    let mut count = 0;
                    while pos < raw.len() {
                        let length =
                            u32::from_le_bytes(raw[pos..pos + 4].try_into().unwrap()) as usize;
                        pos += 4 + length;
                        count += 1;
                    }
                    assert_eq!(pos, raw.len());
                    rows.push(count);
                }
            }
        }
    }
    assert!(!rows.is_empty(), "fixture must exercise actual Zstd frames");
    rows
}

fn encode(batch: &RecordBatch, rows: usize) -> Vec<u8> {
    let runtime = SingleThreadRuntime::default();
    let session = session(&runtime);
    write_vortex_blob(
        batch.schema().as_ref(),
        std::slice::from_ref(batch),
        docs_strategy(65_536, rows, &session),
        0,
    )
    .unwrap()
}

fn strings(blob: &[u8], selected: Vec<u64>) -> Vec<String> {
    scan_blob(
        &BlobHandle::Mem(Bytes::copy_from_slice(blob)),
        Some(&["_source"]),
        RowSelection::Indices(selected),
    )
    .unwrap()
    .into_iter()
    .flat_map(|batch| {
        let values = arrow::compute::cast(batch.column(0), &DataType::Utf8).unwrap();
        values
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|value| value.unwrap().to_owned())
            .collect::<Vec<_>>()
    })
    .collect()
}

#[test]
fn source_frame_default_matches_baseline_strategy_bytes() {
    let batch = fixture();
    let runtime = SingleThreadRuntime::default();
    let session = session(&runtime);
    let baseline = WriteStrategyBuilder::from_session(&session)
        .with_flat_strategy(Arc::new(BufferBoundedFlatStrategy::default()))
        .with_btrblocks_builder(BtrBlocksCompressorBuilder::from_session(&session).with_compact())
        .with_row_block_size(65_536)
        .build();
    let old = write_vortex_blob(
        batch.schema().as_ref(),
        std::slice::from_ref(&batch),
        baseline,
        0,
    )
    .unwrap();
    assert_eq!(old, encode(&batch, 8192));
    assert_eq!(VixWriterOptions::default().source_frame_rows, 8192);
}

#[test]
fn source_frame_small_roundtrip_and_other_column_unchanged() {
    let batch = fixture();
    let old = encode(&batch, 8192);
    let new = encode(&batch, 2048);
    assert_eq!(frame_rows(&old, 1), vec![8192, 8192, 2049]);
    assert_eq!(frame_rows(&new, 1), [vec![2048; 9], vec![1]].concat());
    assert_eq!(frame_rows(&old, 2), frame_rows(&new, 2));
    for selected in [
        vec![],
        vec![0],
        vec![0, 2047, 2048, 8191, 8192, 18432],
        vec![18432, 0, 8192, 0],
        (0..100).collect(),
        (0..ROWS as u64).collect(),
    ] {
        assert_eq!(strings(&old, selected.clone()), strings(&new, selected));
    }
}

#[test]
fn source_frame_passthrough_converts_old_chunks() {
    let batch = fixture();
    let old = encode(&batch, 8192);
    let mut writer = DocsBlobEncoder::spawn(
        batch.schema(),
        65_536,
        0,
        None,
        true,
        Arc::new(AtomicU64::new(0)),
        0,
        2048,
    )
    .unwrap();
    for array in arrays(&old) {
        writer.push_encoded(array).unwrap();
    }
    let (sink, length) = writer.join().unwrap();
    let ContainerSink::Mem(bytes) = sink else {
        panic!("memory writer")
    };
    let new = &bytes[4..4 + length as usize];
    assert!(frame_rows(new, 1).iter().all(|&rows| rows <= 2048));
    assert_eq!(frame_rows(new, 1).iter().sum::<usize>(), ROWS);
    assert_eq!(frame_rows(&old, 2), frame_rows(new, 2));
    assert_eq!(
        strings(&old, (0..ROWS as u64).collect()),
        strings(new, (0..ROWS as u64).collect())
    );
}

#[test]
fn source_frame_invalid_writer_option_fails() {
    let schema = Schema::new(vec![Field::new("_timestamp", DataType::Int64, false)]);
    for rows in [0, 1, 4096, 8193, usize::MAX] {
        let writer = VixWriter::new(
            &schema,
            VixWriterOptions {
                source_frame_rows: rows,
                ..Default::default()
            },
            false,
        );
        let error = writer.finish().unwrap_err().to_string();
        assert!(error.contains("source_frame_rows"), "{error}");
    }
}

fn merge_blob(batch: &RecordBatch, roots: Vec<ArrayRef>, rows: usize) -> Vec<u8> {
    let mut writer = DocsBlobEncoder::spawn(
        batch.schema(),
        65_536,
        0,
        None,
        true,
        Arc::new(AtomicU64::new(0)),
        0,
        rows,
    )
    .unwrap();
    for root in roots {
        writer.push_encoded(root).unwrap();
    }
    let (sink, length) = writer.join().unwrap();
    let ContainerSink::Mem(bytes) = sink else {
        panic!("memory writer")
    };
    bytes[4..4 + length as usize].to_vec()
}

fn frame_payloads(blob: &[u8]) -> Vec<Vec<u8>> {
    arrays(blob)
        .into_iter()
        .flat_map(|root| {
            root.as_::<Struct>().fields()[1]
                .depth_first_traversal()
                .flat_map(|node| {
                    node.as_opt::<Zstd>()
                        .map(|array| {
                            array
                                .data()
                                .clone()
                                .into_parts(vortex::array::validity::Validity::NonNullable)
                                .frames
                                .into_iter()
                                .map(|frame| frame.as_slice().to_vec())
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

#[test]
fn source_frame_passthrough_keeps_already_small_payloads() {
    use vortex::{
        array::{
            IntoArray, VortexSessionExecute,
            arrays::{StructArray, VarBinViewArray},
            validity::Validity,
        },
        arrow::ArrowSessionExt,
    };
    let batch = fixture();
    let runtime = SingleThreadRuntime::default();
    let session = session(&runtime);
    let mut ctx = session.create_execution_ctx();
    let values = session
        .arrow()
        .from_arrow_array(batch.column(1).clone(), false)
        .unwrap()
        .execute::<VarBinViewArray>(&mut ctx)
        .unwrap();
    // A different compression level detects accidental decode/recompression.
    let small = Zstd::from_var_bin_view_without_dict(&values, 1, 2048, &mut ctx)
        .unwrap()
        .into_array();
    assert!(crate::source_frame::has_small_frames(&small));
    assert!(!crate::source_frame::has_small_frames(
        &small.slice(1..ROWS).unwrap()
    ));
    let old = encode(&batch, 8192);
    let mut roots = arrays(&old);
    assert_eq!(roots.len(), 1);
    let root = roots.pop().unwrap();
    let fields = root.as_::<Struct>().fields().to_vec();
    let root = StructArray::try_new(
        ["_timestamp", "_source", "_original"].into(),
        vec![fields[0].clone(), small.clone(), fields[2].clone()],
        ROWS,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let expected: Vec<Vec<u8>> = small
        .as_::<Zstd>()
        .data()
        .clone()
        .into_parts(Validity::NonNullable)
        .frames
        .into_iter()
        .map(|frame| frame.as_slice().to_vec())
        .collect();
    for rows in [8192, 2048] {
        let merged = merge_blob(&batch, vec![root.clone()], rows);
        assert_eq!(frame_payloads(&merged), expected);
        assert_eq!(
            strings(&old, (0..ROWS as u64).collect()),
            strings(&merged, (0..ROWS as u64).collect())
        );
    }
}

#[test]
fn source_frame_mixed_merge_converts_only_old_frames() {
    let batch = fixture();
    let old = encode(&batch, 8192);
    let small = encode(&batch, 2048);
    let roots = arrays(&old).into_iter().chain(arrays(&small)).collect();
    let merged = merge_blob(&batch, roots, 2048);
    assert!(frame_rows(&merged, 1).iter().all(|&n| n <= 2048));
    assert_eq!(frame_rows(&merged, 1).iter().sum::<usize>(), ROWS * 2);
    let expected = strings(&old, (0..ROWS as u64).collect());
    assert_eq!(
        strings(&merged, (0..(ROWS * 2) as u64).collect()),
        [expected.clone(), expected].concat()
    );
    let again = merge_blob(&batch, arrays(&merged), 2048);
    assert_eq!(frame_payloads(&merged), frame_payloads(&again));
}

#[test]
fn source_frame_nullable_and_boundary_lengths_match() {
    for len in [1, 2047, 2048, 2049, 8191, 8192, 8193] {
        let source = StringArray::from_iter((0..len).map(|row| {
            (row % 17 != 0).then(|| format!("row={row};{}", "wide payload ".repeat(32)))
        }));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "_source",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(source.clone())]).unwrap();
        for rows in [8192, 2048] {
            let blob = encode(&batch, rows);
            let batches = scan_blob(
                &BlobHandle::Mem(Bytes::from(blob)),
                Some(&["_source"]),
                RowSelection::All,
            )
            .unwrap();
            let sources: Vec<_> = batches
                .iter()
                .map(|b| arrow::compute::cast(b.column(0), &DataType::Utf8).unwrap())
                .collect();
            let refs: Vec<&dyn Array> = sources.iter().map(|a| a.as_ref()).collect();
            let actual = arrow::compute::concat(&refs).unwrap();
            assert_eq!(
                actual.as_any().downcast_ref::<StringArray>().unwrap(),
                &source
            );
        }
    }
}
