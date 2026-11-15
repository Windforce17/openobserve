// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::{
    array::{Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parking_lot::Mutex;

use super::*;
use crate::{VixQuery, VixReader, VixWriter, VixWriterOptions};

struct CachedSource {
    name: String,
    bytes: Bytes,
    cache: Mutex<Vec<(Range<u64>, Bytes)>>,
    reads: AtomicUsize,
}
impl CachedSource {
    fn new(bytes: Vec<u8>) -> Arc<Self> {
        Arc::new(Self {
            name: "test.vix".into(),
            bytes: Bytes::from(bytes),
            cache: Mutex::new(vec![]),
            reads: AtomicUsize::new(0),
        })
    }
}
impl VixRangeSource for CachedSource {
    fn describe(&self) -> String {
        self.name.clone()
    }
    fn len(&self) -> u64 {
        self.bytes.len() as u64
    }
    fn resident(&self, range: Range<u64>) -> Option<Bytes> {
        self.cache
            .lock()
            .iter()
            .find(|(r, _)| r.start <= range.start && range.end <= r.end)
            .map(|(r, b)| b.slice((range.start - r.start) as usize..(range.end - r.start) as usize))
    }
    fn fetch(&self, range: Range<u64>) -> BoxFuture<'static, anyhow::Result<Bytes>> {
        let bytes = self.resident(range.clone()).unwrap_or_else(|| {
            self.reads.fetch_add(1, Ordering::SeqCst);
            let bytes = self.bytes.slice(range.start as usize..range.end as usize);
            self.cache.lock().push((range, bytes.clone()));
            bytes
        });
        futures::future::ready(Ok(bytes)).boxed()
    }
}

fn rewrite(index: &[u8], mut transform: impl FnMut(&mut PuffinMeta, &mut Vec<Vec<u8>>)) -> Vec<u8> {
    let mut meta = puffin::reader::parse_puffin_footer_from_bytes(index).unwrap();
    let mut blobs: Vec<_> = meta
        .blobs
        .iter()
        .map(|b| index[b.offset as usize..(b.offset + b.length) as usize].to_vec())
        .collect();
    transform(&mut meta, &mut blobs);
    let mut out = vec![];
    let mut writer = puffin::writer::PuffinBytesWriter::new(&mut out);
    for (k, v) in meta.properties {
        writer.set_property(k, v);
    }
    for (meta, bytes) in meta.blobs.iter().zip(blobs) {
        writer
            .add_blob_with_codec(
                &bytes,
                meta.blob_type.clone(),
                meta.properties["blob_tag"].clone(),
                meta.compression_codec.clone(),
            )
            .unwrap();
    }
    writer.finish().unwrap();
    out
}

fn fixture() -> (Vec<u8>, Vec<u8>, Vec<u8>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("value", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values((1..=64).rev())),
            Arc::new(StringArray::from_iter_values(
                (0..64).map(|i| format!("value-{i}")),
            )),
        ],
    )
    .unwrap();
    let mut writer = VixWriter::new(&schema, VixWriterOptions::default(), false);
    writer
        .push_batch_with_source(
            &batch,
            &StringArray::from_iter_values((0..64).map(|i| format!("{{\"value\":\"value-{i}\"}}"))),
            None,
        )
        .unwrap();
    let (data, index) = writer.finish().unwrap();
    let index = rewrite(&index.unwrap(), |meta, blobs| {
        let i = meta
            .blobs
            .iter()
            .position(|b| b.blob_type == container::BLOB_TYPE_DICT_BLOCKS)
            .unwrap();
        let padded = blobs[i].len() + 1024 * 1024;
        blobs[i].resize(padded, 0);
    });
    let payload = puffin::reader::footer_payload_bytes(&data).unwrap();
    let copy = prepare(data.len() as u64, &payload, |r| {
        Ok(Bytes::copy_from_slice(
            &data[r.start as usize..r.end as usize],
        ))
    })
    .unwrap();
    (data, index, copy)
}
fn open(
    data: &[u8],
    index: &[u8],
    sidecar_first: bool,
) -> (VixReader, Arc<CachedSource>, Arc<CachedSource>) {
    let data = CachedSource::new(data.to_vec());
    let index = CachedSource::new(index.to_vec());
    let reader = VixReader::open_ranged_with_index_mode(
        data.clone(),
        Some(index.clone()),
        128 * 1024,
        sidecar_first,
    )
    .unwrap();
    (reader, data, index)
}
fn copy_index(index: &[u8], copy: &[u8]) -> Vec<u8> {
    let mut out = index.to_vec();
    assert!(append(&mut out, Some(copy), 128 * 1024).unwrap());
    out
}
fn source_answer(reader: &VixReader) -> Vec<String> {
    let array = reader.read_source(&[0, 3, 63]).unwrap();
    (0..array.len())
        .map(|i| array.value(i).to_owned())
        .collect()
}

#[test]
fn default_off_strict_switches() {
    assert_eq!(
        Settings::parse(|_| Ok(None)).unwrap(),
        Settings {
            write: false,
            read: true
        }
    );
    for name in [WRITE_ENV, READ_ENV] {
        for value in ["0", "1", "false", "true"] {
            assert!(Settings::parse(|key| Ok((key == name).then(|| value.to_owned()))).is_ok());
        }
        for value in ["", "TRUE", "yes", "2", " false", "false "] {
            assert!(Settings::parse(|key| Ok((key == name).then(|| value.to_owned()))).is_err());
        }
    }
}

#[test]
fn no_copy_never_fingerprints() {
    let (data, index, _) = fixture();
    FINGERPRINT_CALLS.with(|calls| calls.set(0));
    for sidecar_first in [false, true] {
        let (reader, ..) = open(&data, &index, sidecar_first);
        assert_eq!(source_answer(&reader).len(), 3);
    }
    let reader = VixReader::open_with_index(
        Bytes::copy_from_slice(&data),
        Some(Bytes::copy_from_slice(&index)),
    )
    .unwrap();
    assert_eq!(source_answer(&reader).len(), 3);
    crate::VixDocs::open_ranged(CachedSource::new(data.clone())).unwrap();
    crate::VixDocs::open(Bytes::from(data)).unwrap();
    assert_eq!(FINGERPRINT_CALLS.with(|calls| calls.get()), 0);
}

#[test]
fn copy_fingerprints_only_the_needed_data_container() {
    let (data, index, copy) = fixture();
    let new = copy_index(&index, &copy);
    let index_source: Arc<dyn VixRangeSource> = CachedSource::new(new.clone());
    FINGERPRINT_CALLS.with(|calls| calls.set(0));
    let index_container =
        container::parse_container_ranged_with_tail(&index_source, 128 * 1024).unwrap();
    assert!(index_container.docs_copy_available);
    assert!(index_container.footer_fingerprint.is_none());
    assert_eq!(FINGERPRINT_CALLS.with(|calls| calls.get()), 0);
    let data_source: Arc<dyn VixRangeSource> = CachedSource::new(data.clone());
    let data_container =
        VixReader::data_container_from_sidecar(&data_source, &index_container).unwrap();
    assert!(data_container.footer_fingerprint.is_some());
    assert_eq!(FINGERPRINT_CALLS.with(|calls| calls.get()), 1);
    assert!(eligible(&data_container, &index_container));
    for sidecar_first in [false, true] {
        FINGERPRINT_CALLS.with(|calls| calls.set(0));
        let (reader, data_source, index_source) = open(&data, &new, sidecar_first);
        // One data-container hash and one comparison against the sidecar payload.
        assert_eq!(FINGERPRINT_CALLS.with(|calls| calls.get()), 2);
        let data_reads = data_source.reads.load(Ordering::SeqCst);
        let index_reads = index_source.reads.load(Ordering::SeqCst);
        reader.docs_schema().unwrap();
        let hashes = if sidecar_first { 3 } else { 2 };
        assert_eq!(FINGERPRINT_CALLS.with(|calls| calls.get()), hashes);
        reader.docs_schema().unwrap();
        assert_eq!(FINGERPRINT_CALLS.with(|calls| calls.get()), hashes);
        assert_eq!(data_source.reads.load(Ordering::SeqCst), data_reads);
        assert_eq!(index_source.reads.load(Ordering::SeqCst), index_reads);
    }
}

#[test]
fn lazy_copy_matches_legacy_results_and_cache_ownership() {
    let (data, index, copy) = fixture();
    let new = copy_index(&index, &copy);
    let ignored = rewrite(&new, |meta, _| {
        meta.blobs
            .iter_mut()
            .find(|b| is_copy(b))
            .unwrap()
            .blob_type = "o2-vix-docs-footer-skip-v1".into()
    });
    let (mut legacy, legacy_data, _) = open(&data, &ignored, true);
    let (mut reader, data_source, index_source) = open(&data, &new, true);
    assert_eq!(
        reader.memory_size(),
        legacy.memory_size(),
        "cold reader must not retain copy bytes"
    );
    assert_eq!(data_source.reads.load(Ordering::SeqCst), 0);
    let query = VixQuery::Exact {
        field: "value".into(),
        token: b"value-3".to_vec(),
    };
    assert_eq!(reader.count(&query).unwrap(), legacy.count(&query).unwrap());
    assert_eq!(
        reader.memory_size(),
        legacy.memory_size(),
        "pure index warm cache"
    );
    assert_eq!(data_source.reads.load(Ordering::SeqCst), 0);
    let index_reads = index_source.reads.load(Ordering::SeqCst);
    assert_eq!(reader.docs_schema().unwrap(), legacy.docs_schema().unwrap());
    assert_eq!(
        data_source.reads.load(Ordering::SeqCst),
        0,
        "copy must eliminate data footer IO"
    );
    assert!(legacy_data.reads.load(Ordering::SeqCst) > 0);
    assert_eq!(
        index_source.reads.load(Ordering::SeqCst),
        index_reads,
        "copy comes from the existing tail/block cache"
    );
    assert_eq!(
        reader.memory_size(),
        legacy.memory_size(),
        "warm docs cache uses the existing suffix representation"
    );
    assert_eq!(source_answer(&reader), source_answer(&legacy));
    reader.demote();
    legacy.demote();
    assert_eq!(reader.memory_size(), legacy.memory_size(), "demoted cache");
}

#[test]
fn absent_corrupt_mismatched_and_parse_failure_fall_back() {
    let (data, index, copy) = fixture();
    let (baseline, ..) = open(&data, &index, true);
    let expected = source_answer(&baseline);
    let meta = puffin::reader::parse_puffin_footer_from_bytes(&data).unwrap();
    let docs = meta
        .blobs
        .iter()
        .find(|b| b.blob_type == container::BLOB_TYPE_DOCS)
        .unwrap();
    let range = checked_range(docs, data.len() as u64).unwrap();
    let (good, start) = header(&copy, data.len() as u64, &range, &meta).unwrap();
    let raw = zstd::bulk::decompress(&copy[start..], good.raw_len).unwrap();
    let mut cases = vec![("absent", None)];
    let mut corrupt = copy.clone();
    *corrupt.last_mut().unwrap() ^= 255;
    cases.push(("compressed", Some(corrupt)));
    for kind in [
        "length",
        "offset",
        "digest",
        "data_footer",
        "bound",
        "parse",
    ] {
        let mut h = good.clone();
        let mut bytes = raw.clone();
        match kind {
            "length" => h.data_len += 1,
            "offset" => h.docs_offset += 1,
            "digest" => h.raw_sha256 = "0".repeat(64),
            "data_footer" => h.data_footer_sha256 = "0".repeat(64),
            "bound" => h.raw_len = MAX_RAW + 1,
            "parse" => {
                *bytes.last_mut().unwrap() ^= 255;
                h.raw_sha256 = sha256::digest(&bytes);
            }
            _ => unreachable!(),
        }
        cases.push((kind, Some(encode(&h, &bytes).unwrap())));
    }
    for (name, copy) in cases {
        let index = copy
            .as_ref()
            .map_or_else(|| index.clone(), |copy| copy_index(&index, copy));
        let (reader, data_source, _) = open(&data, &index, true);
        reader.docs_schema().unwrap();
        assert!(
            data_source.reads.load(Ordering::SeqCst) > 0,
            "{name} must use the authoritative docs footer"
        );
        assert_eq!(source_answer(&reader), expected, "{name}");
    }
}

#[test]
fn actual_data_footer_mismatch_disables_copy_and_same_length_metadata_is_bound() {
    let (data, index, copy) = fixture();
    let new = copy_index(&index, &copy);
    let changed = rewrite(&data, |meta, _| {
        meta.properties
            .insert("identity-test".into(), "changed".into());
    });
    let (reader, data_source, _) = open(&changed, &new, false);
    let before = data_source.reads.load(Ordering::SeqCst);
    reader.docs_schema().unwrap();
    // The data tail may itself contain the entire docs blob; it remains authoritative.
    assert!(data_source.reads.load(Ordering::SeqCst) >= before);
    let source: Arc<dyn VixRangeSource> = CachedSource::new(changed);
    let data_container = container::parse_container_ranged(&source).unwrap();
    let index_container = container::parse_container(&Bytes::from(new)).unwrap();
    assert!(!eligible(&data_container, &index_container));
    let mut meta = puffin::reader::parse_puffin_footer_from_bytes(&data).unwrap();
    let docs = meta
        .blobs
        .iter()
        .find(|b| b.blob_type == container::BLOB_TYPE_DOCS)
        .unwrap();
    let range = checked_range(docs, data.len() as u64).unwrap();
    assert!(header(&copy, data.len() as u64, &range, &meta).is_ok());
    meta.properties
        .insert("columns".into(), "[[\"foreign\",64]]".into());
    assert!(
        header(&copy, data.len() as u64, &range, &meta).is_err(),
        "equal lengths are not identity"
    );
}

#[test]
fn tail_gate_skips_without_modifying_any_byte() {
    let (_, index, copy) = fixture();
    for tail in [0, 16, 128, copy.len() as u64] {
        let mut output = index.clone();
        assert!(!append(&mut output, Some(&copy), tail).unwrap());
        assert_eq!(output, index);
    }
    let mut output = index.clone();
    assert!(!append(&mut output, None, u64::MAX).unwrap());
    assert_eq!(output, index);
    assert!(append(&mut output, Some(&copy), 128 * 1024).unwrap());
    assert!(container::parse_container(&Bytes::from(output.clone())).is_ok());
    assert!(
        append(&mut output, Some(&copy), u64::MAX).is_err(),
        "writers may not inherit a copy"
    );
}

#[test]
fn detached_reads_keep_the_reader_cache_cold() {
    let (data, index, copy) = fixture();
    let new = copy_index(&index, &copy);
    let (reader, data_source, _) = open(&data, &new, true);
    let before = reader.memory_size();
    let detached = reader.detached_docs();
    assert!(detached.schema().unwrap().field_with_name("value").is_ok());
    assert_eq!(data_source.reads.load(Ordering::SeqCst), 0);
    drop(detached);
    assert_eq!(reader.memory_size(), before);
}

#[test]
#[ignore = "subprocess startup entry"]
fn startup_child() {
    crate::initialize_reader_compact().unwrap();
}

#[test]
fn startup_rejects_invalid_environment() {
    use std::{ffi::OsString, os::unix::ffi::OsStringExt, process::Command};
    for name in [WRITE_ENV, READ_ENV] {
        for (value, success) in [
            (OsString::from("true"), true),
            (OsString::from("false"), true),
            (OsString::from("yes"), false),
            (OsString::from_vec(vec![255]), false),
        ] {
            let output = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "docs_footer_copy::tests::startup_child",
                    "--ignored",
                ])
                .env_remove(WRITE_ENV)
                .env_remove(READ_ENV)
                .env(name, value)
                .output()
                .unwrap();
            assert_eq!(
                output.status.success(),
                success,
                "{name}: {}",
                String::from_utf8_lossy(&output.stdout)
            );
        }
    }
}

#[test]
fn malformed_optional_descriptors_fall_back() {
    let (data, index, copy) = fixture();
    let new = copy_index(&index, &copy);
    for kind in ["bounds", "overflow", "codec", "duplicate"] {
        let mut meta = puffin::reader::parse_puffin_footer_from_bytes(&new).unwrap();
        let i = meta.blobs.iter().position(is_copy).unwrap();
        match kind {
            "bounds" => meta.blobs[i].offset = new.len() as u64 + 1024 * 1024,
            "overflow" => meta.blobs[i].offset = u64::MAX,
            "codec" => meta.blobs[i].compression_codec = Some(puffin::CompressionCodec::Zstd),
            "duplicate" => meta.blobs.push(meta.blobs[i].clone()),
            _ => unreachable!(),
        }
        // Replace only the Puffin metadata: a blob writer would repair offsets.
        let old_len =
            u32::from_le_bytes(new[new.len() - 12..new.len() - 8].try_into().unwrap()) as usize;
        let mut bad = new[..new.len() - old_len - 16].to_vec();
        let payload = serde_json::to_vec(&meta).unwrap();
        bad.extend_from_slice(&puffin::MAGIC);
        bad.extend_from_slice(&payload);
        bad.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        bad.extend_from_slice(&0u32.to_le_bytes());
        bad.extend_from_slice(&puffin::MAGIC);
        if matches!(kind, "bounds" | "overflow") {
            assert!(checked_range(&meta.blobs[i], bad.len() as u64).is_err());
        }
        for sidecar_first in [false, true] {
            let (reader, ..) = open(&data, &bad, sidecar_first);
            let (baseline, ..) = open(&data, &index, sidecar_first);
            assert_eq!(source_answer(&reader), source_answer(&baseline), "{kind}");
            if sidecar_first {
                assert!(
                    !container::parse_container(&Bytes::from(bad.clone()))
                        .unwrap()
                        .docs_copy_available
                );
            }
        }
    }
}

#[test]
fn deterministic_l0_keys_do_not_use_the_copy() {
    let (data, index, copy) = fixture();
    let new = copy_index(&index, &copy);
    let mut data = CachedSource::new(data);
    Arc::get_mut(&mut data).unwrap().name = "files/o/logs/s/l0_h2_retry_123.vix".into();
    FINGERPRINT_CALLS.with(|calls| calls.set(0));
    let reader = VixReader::open_ranged_with_index_mode(
        data.clone(),
        Some(CachedSource::new(new)),
        128 * 1024,
        true,
    )
    .unwrap();
    reader.docs_schema().unwrap();
    assert!(
        data.reads.load(Ordering::SeqCst) > 0,
        "known mutable keys must use the data footer"
    );
    assert_eq!(FINGERPRINT_CALLS.with(|calls| calls.get()), 0);
}

#[test]
fn memory_admission_refusal_falls_back_without_retaining_pending_bytes() {
    struct RefuseOptional {
        calls: AtomicUsize,
        reject_at: usize,
    }
    impl crate::VixReadOperation for RefuseOptional {
        fn is_cancelled(&self) -> bool {
            false
        }
        fn try_check_memory(&self, _: usize) -> Result<bool> {
            Ok(self.calls.fetch_add(1, Ordering::SeqCst) != self.reject_at)
        }
    }
    let (data, index, copy) = fixture();
    let new = copy_index(&index, &copy);
    let ignored = rewrite(&new, |meta, _| {
        meta.blobs
            .iter_mut()
            .find(|b| is_copy(b))
            .unwrap()
            .blob_type = "o2-vix-docs-footer-skip-v1".into();
    });
    for reject_at in [0, 1] {
        let (reader, data_source, _) = open(&data, &new, true);
        let (baseline, ..) = open(&data, &ignored, true);
        let operation = Arc::new(RefuseOptional {
            calls: AtomicUsize::new(0),
            reject_at,
        });
        let actual =
            crate::with_read_operation(operation.clone(), || reader.docs_schema()).unwrap();
        assert_eq!(actual, baseline.docs_schema().unwrap());
        assert!(operation.calls.load(Ordering::SeqCst) > reject_at);
        assert!(data_source.reads.load(Ordering::SeqCst) > 0);
        assert_eq!(reader.memory_size(), baseline.memory_size());
        assert_eq!(source_answer(&reader), source_answer(&baseline));
    }
}

#[test]
fn writer_enabled_paths_and_reader_kill_switch() {
    for (entry, read) in [
        ("writer_paths_child", "true"),
        ("reader_disabled_child", "false"),
    ] {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                &format!("docs_footer_copy::tests::{entry}"),
                "--ignored",
                "--nocapture",
            ])
            .env(WRITE_ENV, "true")
            .env(READ_ENV, read)
            .env("ZO_VIX_EAGER_TAIL_BYTES", "786432")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{entry}: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
#[ignore = "subprocess reader kill-switch entry"]
fn reader_disabled_child() {
    assert!(!settings().unwrap().read);
    let (data, index, _) = fixture();
    let meta = puffin::reader::parse_puffin_footer_from_bytes(&index).unwrap();
    assert_eq!(meta.blobs.iter().filter(|b| is_copy(b)).count(), 1);
    FINGERPRINT_CALLS.with(|calls| calls.set(0));
    for sidecar_first in [false, true] {
        let (reader, data_source, _) = open(&data, &index, sidecar_first);
        reader.docs_schema().unwrap();
        assert!(data_source.reads.load(Ordering::SeqCst) > 0);
        assert_eq!(source_answer(&reader).len(), 3);
    }
    assert_eq!(FINGERPRINT_CALLS.with(|calls| calls.get()), 0);
}

fn verify_written_copy(data: &[u8], index: &[u8], expected: &StringArray) {
    let data_meta = puffin::reader::parse_puffin_footer_from_bytes(data).unwrap();
    let docs = data_meta
        .blobs
        .iter()
        .find(|b| b.blob_type == container::BLOB_TYPE_DOCS)
        .unwrap();
    let range = checked_range(docs, data.len() as u64).unwrap();
    let meta = puffin::reader::parse_puffin_footer_from_bytes(index).unwrap();
    assert_eq!(meta.blobs.iter().filter(|b| is_copy(b)).count(), 1);
    let blob = meta.blobs.iter().find(|b| is_copy(b)).unwrap();
    let copy = &index[blob.offset as usize..(blob.offset + blob.length) as usize];
    let (h, start) = header(copy, data.len() as u64, &range, &data_meta).unwrap();
    let raw = zstd::bulk::decompress(&copy[start..], h.raw_len).unwrap();
    assert_eq!(raw, data[h.footer_start as usize..range.end as usize]);
    assert_eq!(sha256::digest(&raw), h.raw_sha256);
    assert_eq!(
        fingerprint(
            &serde_json::from_str::<PuffinMeta>(&meta.properties[container::PROP_DATA_FOOTER])
                .unwrap()
        )
        .unwrap(),
        h.data_footer_sha256
    );
    let reader = VixReader::open_with_index(
        Bytes::copy_from_slice(data),
        Some(Bytes::copy_from_slice(index)),
    )
    .unwrap();
    assert_eq!(
        reader
            .read_source(&(0..reader.row_count()).collect::<Vec<_>>())
            .unwrap(),
        *expected
    );
    assert_eq!(
        reader
            .count(&VixQuery::Exact {
                field: "value".into(),
                token: b"value-3".to_vec()
            })
            .unwrap(),
        1
    );
}

#[test]
#[ignore = "subprocess writer-enabled paths entry"]
fn writer_paths_child() {
    use crate::{DocIdMap, VixDocs};
    assert!(settings().unwrap().write);
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("value", DataType::Utf8, true),
    ]));
    let timestamps = Int64Array::from_iter_values((1..=64).rev());
    let values = Arc::new(StringArray::from_iter_values(
        (0..64).map(|i| format!("value-{i}")),
    ));
    let sources =
        StringArray::from_iter_values((0..64).map(|i| format!("{{\"value\":\"value-{i}\"}}")));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(timestamps.clone()), values.clone()],
    )
    .unwrap();
    let make = |batch: &RecordBatch, source: &StringArray, opts: VixWriterOptions| {
        let spooled = opts.output_spool_dir.is_some();
        let mut writer = VixWriter::new(&schema, opts, false);
        writer.push_batch_with_source(batch, source, None).unwrap();
        let (data, index, stats) = writer.finish_output().unwrap();
        assert_eq!(data.spool_path().is_some(), spooled);
        let index = index.unwrap();
        assert_eq!(stats.index_size as usize, index.len());
        (data.into_bytes().unwrap(), index)
    };
    let scratch = tempfile::tempdir().unwrap();
    for spooled in [false, true] {
        for compressed_tail in [false, true] {
            let opts = VixWriterOptions {
                output_spool_dir: spooled.then(|| scratch.path().to_path_buf()),
                term_spill_dir: spooled.then(|| scratch.path().to_path_buf()),
                sidecar_tail_zstd: compressed_tail,
                ..Default::default()
            };
            let (data, index) = make(&batch, &sources, opts.clone());
            verify_written_copy(&data, &index, &sources);
            let inputs: Vec<_> = [0, 32]
                .into_iter()
                .map(|offset| {
                    let (data, index) = make(
                        &batch.slice(offset, 32),
                        &sources.slice(offset, 32),
                        opts.clone(),
                    );
                    VixReader::open_with_index(Bytes::from(data), Some(Bytes::from(index))).unwrap()
                })
                .collect();
            let mut merged = VixWriter::new(&schema, opts.clone(), false);
            merged
                .merge_input_indexes(
                    &inputs.iter().collect::<Vec<_>>(),
                    &[DocIdMap::Offset(0), DocIdMap::Offset(32)],
                    1,
                )
                .unwrap();
            merged
                .push_docs_rows_unindexed(
                    &timestamps,
                    &[("value".into(), values.clone())],
                    &sources,
                    None,
                )
                .unwrap();
            let (merged_data, merged_index, stats) = merged.finish_output().unwrap();
            assert_eq!(merged_data.spool_path().is_some(), spooled);
            let merged_index = merged_index.unwrap();
            assert_eq!(stats.index_size as usize, merged_index.len());
            verify_written_copy(&merged_data.into_bytes().unwrap(), &merged_index, &sources);

            let docs = VixDocs::open(Bytes::from(data.clone())).unwrap();
            let pass_opts = VixWriterOptions {
                docs_passthrough: true,
                ..opts.clone()
            };
            let mut pass = VixWriter::new(&schema, pass_opts.clone(), false);
            pass.merge_input_indexes(
                &inputs.iter().collect::<Vec<_>>(),
                &[DocIdMap::Offset(0), DocIdMap::Offset(32)],
                1,
            )
            .unwrap();
            let entries: Vec<_> = docs
                .zone_chunks()
                .unwrap()
                .iter()
                .map(|zone| (zone.row_count, zone.ts_min, zone.ts_max))
                .collect();
            pass.begin_docs_encoded_run(
                64,
                1,
                64,
                &entries,
                &docs.spliceable_stats().unwrap().unwrap(),
                Some(&[64]),
            )
            .unwrap();
            docs.scan_docs_encoded_chunks(&mut |chunk| pass.push_docs_encoded_chunk(chunk))
                .unwrap();
            pass.finish_docs_encoded_run().unwrap();
            let (pass_data, pass_index, stats) = pass.finish_output().unwrap();
            assert_eq!(pass_data.spool_path().is_some(), spooled);
            let pass_index = pass_index.unwrap();
            assert_eq!(stats.index_size as usize, pass_index.len());
            verify_written_copy(&pass_data.into_bytes().unwrap(), &pass_index, &sources);

            let source: Arc<dyn VixRangeSource> = CachedSource::new(data.clone());
            let mut heal = VixWriter::new(&schema, pass_opts, false);
            heal.push_batch_with_source_index_only(&batch, &sources, None)
                .unwrap();
            heal.set_data_source(&source).unwrap();
            let (healed, stats) = heal.finish_index_sidecar(64).unwrap();
            assert_eq!(stats.index_size as usize, healed.len());
            verify_written_copy(&data, &healed, &sources);
            let repacked = crate::repack_sidecar(
                source.clone(),
                CachedSource::new(index),
                &crate::SidecarRepackOptions {
                    sidecar_tail_zstd: compressed_tail,
                    auto_bloom: Default::default(),
                    bloom_fpp: 0.001,
                },
            )
            .unwrap();
            verify_written_copy(&data, &repacked.sidecar, &sources);
            let again = crate::repack_sidecar(
                source,
                CachedSource::new(repacked.sidecar.clone()),
                &crate::SidecarRepackOptions {
                    sidecar_tail_zstd: compressed_tail,
                    auto_bloom: Default::default(),
                    bloom_fpp: 0.001,
                },
            )
            .unwrap();
            assert_eq!(
                again.sidecar, repacked.sidecar,
                "repack must replace rather than inherit a copy"
            );
        }
    }
}
