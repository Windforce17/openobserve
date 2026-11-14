// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Startup-only sizing of the first docs Vortex footer read.
use std::{env::VarError, io, sync::OnceLock};

use crate::source::{VORTEX_FOOTER_INITIAL_READ_BYTES, VORTEX_FOOTER_READ_BYTES};

const BYTES_ENV: &str = "ZO_VIX_DOCS_FOOTER_BYTES";
const ENABLED_ENV: &str = "ZO_VIX_DOCS_FOOTER_ENABLED";
const MAX_BYTES: u64 = 16 * 1024 * 1024;
static SETTINGS: OnceLock<DocsFooterConfig> = OnceLock::new();

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DocsFooterConfig {
    bytes: u64,
    enabled: bool,
}

impl Default for DocsFooterConfig {
    fn default() -> Self {
        Self {
            bytes: VORTEX_FOOTER_INITIAL_READ_BYTES,
            enabled: true,
        }
    }
}

impl DocsFooterConfig {
    fn parse_with(mut read: impl FnMut(&str) -> io::Result<Option<String>>) -> io::Result<Self> {
        let bytes = match read(BYTES_ENV)? {
            None => VORTEX_FOOTER_INITIAL_READ_BYTES,
            Some(value) => {
                let parsed = value.parse::<u64>().ok().filter(|bytes| {
                    !value.is_empty()
                        && value.bytes().all(|c| c.is_ascii_digit())
                        && (VORTEX_FOOTER_READ_BYTES..=MAX_BYTES).contains(bytes)
                });
                parsed.ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "{BYTES_ENV}={value:?}: expected decimal bytes in {VORTEX_FOOTER_READ_BYTES}..={MAX_BYTES}"
                        ),
                    )
                })?
            }
        };
        let enabled = match read(ENABLED_ENV)?.as_deref() {
            None | Some("1" | "true") => true,
            Some("0" | "false") => false,
            Some(value) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("{ENABLED_ENV}={value:?}: expected 0, 1, false, or true"),
                ));
            }
        };
        Ok(Self { bytes, enabled })
    }

    pub(crate) fn initial_read(self) -> Option<u64> {
        // The default and kill switch preserve even the old resident-tail
        // shrinking heuristic, not just the nominal 256 KiB maximum.
        (self.enabled && self.bytes != VORTEX_FOOTER_INITIAL_READ_BYTES).then_some(self.bytes)
    }
}

pub(crate) fn settings() -> io::Result<DocsFooterConfig> {
    if let Some(settings) = SETTINGS.get() {
        return Ok(*settings);
    }
    let settings = DocsFooterConfig::parse_with(|name| match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(VarError::NotPresent) => Ok(None),
        Err(VarError::NotUnicode(_)) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{name} must contain a Unicode value"),
        )),
    })?;
    let _ = SETTINGS.set(settings);
    Ok(*SETTINGS.get().expect("settings initialized"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(bytes: Option<&str>, enabled: Option<&str>) -> io::Result<DocsFooterConfig> {
        DocsFooterConfig::parse_with(|name| {
            Ok(if name == BYTES_ENV { bytes } else { enabled }.map(str::to_owned))
        })
    }

    #[test]
    fn docs_footer_config_defaults_and_kill_switch_preserve_legacy_path() {
        assert_eq!(parse(None, None).unwrap(), DocsFooterConfig::default());
        assert_eq!(parse(None, None).unwrap().initial_read(), None);
        assert_eq!(parse(Some("262144"), None).unwrap().initial_read(), None);
        for disabled in ["0", "false"] {
            assert_eq!(
                parse(Some("1048576"), Some(disabled))
                    .unwrap()
                    .initial_read(),
                None
            );
        }
        for enabled in [None, Some("1"), Some("true")] {
            assert_eq!(
                parse(Some("1048576"), enabled).unwrap().initial_read(),
                Some(1048576)
            );
        }
        for bytes in [VORTEX_FOOTER_READ_BYTES, MAX_BYTES] {
            assert_eq!(parse(Some(&bytes.to_string()), None).unwrap().bytes, bytes);
        }
    }

    #[test]
    fn docs_footer_config_rejects_invalid_values_even_when_disabled() {
        for bytes in [
            "",
            "0",
            "1",
            "65534",
            "16777217",
            "-1",
            "+1048576",
            "1MiB",
            " 1048576",
            "1048576 ",
            "18446744073709551616",
        ] {
            for enabled in [None, Some("false")] {
                let error = parse(Some(bytes), enabled).unwrap_err();
                assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
                assert!(error.to_string().contains(BYTES_ENV));
            }
        }
        for enabled in ["", "TRUE", "yes", "2", " true", "false "] {
            assert!(
                parse(None, Some(enabled))
                    .unwrap_err()
                    .to_string()
                    .contains(ENABLED_ENV)
            );
        }
        assert!(
            DocsFooterConfig::parse_with(|_| Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "non-Unicode"
            )))
            .is_err()
        );
    }
}

#[cfg(test)]
mod read_tests {
    use std::{
        ops::Range,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use bytes::Bytes;
    use futures::{FutureExt, future::BoxFuture};
    use parking_lot::Mutex;

    use super::*;
    use crate::{
        VixRangeSource, VixReadOperation,
        container::{self, BlobHandle, RowSelection},
        reader::ReaderMemory,
        source::RangedBlob,
    };

    struct Budget {
        last: AtomicUsize,
        limit: usize,
    }
    impl VixReadOperation for Budget {
        fn is_cancelled(&self) -> bool {
            false
        }
        fn check_memory(&self, bytes: usize) -> crate::Result<()> {
            self.last.store(bytes, Ordering::SeqCst);
            if bytes > self.limit {
                return Err(crate::VixError::Malformed("docs-footer-budget".into()));
            }
            Ok(())
        }
    }

    struct Source {
        data: Bytes,
        range: Range<u64>,
        reads: Mutex<Vec<Range<u64>>>,
        budget: Arc<Budget>,
        admitted: Mutex<Vec<usize>>,
        resident_start: u64,
    }
    impl VixRangeSource for Source {
        fn len(&self) -> u64 {
            self.range.end
        }
        fn fetch(&self, range: Range<u64>) -> BoxFuture<'static, anyhow::Result<Bytes>> {
            assert!(range.start >= self.range.start && range.end <= self.range.end);
            self.reads.lock().push(range.clone());
            self.admitted
                .lock()
                .push(self.budget.last.load(Ordering::SeqCst));
            let start = (range.start - self.range.start) as usize;
            let end = (range.end - self.range.start) as usize;
            futures::future::ready(Ok(Bytes::copy_from_slice(&self.data[start..end]))).boxed()
        }
        fn resident(&self, range: Range<u64>) -> Option<Bytes> {
            (range.start >= self.resident_start && range.end <= self.range.end).then(|| {
                self.data.slice(
                    (range.start - self.range.start) as usize
                        ..(range.end - self.range.start) as usize,
                )
            })
        }
    }

    fn fixture(wide: bool) -> Bytes {
        let name = if wide {
            "f".repeat(350_000)
        } else {
            "value".into()
        };
        let schema = Schema::new(vec![Field::new(name, DataType::Int64, true)]);
        let rows = if wide { 131072 } else { 8 };
        let values = Int64Array::from_iter((0..rows).map(|i| (i % 7 != 0).then_some(i)));
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(values)]).unwrap();
        Bytes::from(
            container::write_vortex_blob(&schema, &[batch], container::addressable_strategy(), 1)
                .unwrap(),
        )
    }

    fn source(data: Bytes, limit: usize) -> Arc<Source> {
        Arc::new(Source {
            range: 4096..4096 + data.len() as u64,
            data,
            reads: Mutex::new(Vec::new()),
            budget: Arc::new(Budget {
                last: AtomicUsize::new(0),
                limit,
            }),
            admitted: Mutex::new(Vec::new()),
            resident_start: u64::MAX,
        })
    }

    fn large(enabled: bool) -> DocsFooterConfig {
        DocsFooterConfig {
            bytes: 1024 * 1024,
            enabled,
        }
    }

    fn serialized(blob: &BlobHandle) -> Vec<u8> {
        let batches = container::scan_blob(blob, None, RowSelection::All).unwrap();
        let mut bytes = Vec::new();
        let mut writer =
            arrow::ipc::writer::StreamWriter::try_new(&mut bytes, &batches[0].schema()).unwrap();
        for batch in batches {
            writer.write(&batch).unwrap();
        }
        writer.finish().unwrap();
        drop(writer);
        bytes
    }

    #[test]
    fn docs_footer_large_window_reads_once_and_preserves_results_and_retention() {
        let data = fixture(true);
        assert!(data.len() > 1024 * 1024);
        let mut old_reads = None;
        let mut old_result = None;
        let mut old_retained = None;
        let mut old_memory = None;
        for config in [
            None,
            Some(DocsFooterConfig::default()),
            Some(large(true)),
            Some(large(false)),
        ] {
            let source = source(data.clone(), usize::MAX);
            let mut ranged = RangedBlob::new(source.clone(), source.range.clone());
            if let Some(config) = config {
                ranged.configure_docs_footer(config);
            }
            let memory = Arc::new(ReaderMemory::new());
            ranged.track_memory(memory.clone());
            let blob = BlobHandle::Ranged(ranged);
            crate::with_read_operation(source.budget.clone(), || {
                let (schema, opened) = container::blob_arrow_schema_owned(&blob).unwrap();
                assert_eq!(schema.fields().len(), 1);
                drop(opened);
            });
            let reads = source.reads.lock().clone();
            let BlobHandle::Ranged(ranged) = &blob else {
                unreachable!()
            };
            let retained = ranged.retained_footer_window();
            let charged = memory.reserve(0).unwrap();
            drop(charged);
            // A metadata open after publication is served entirely from the compact suffix.
            let before = source.reads.lock().len();
            drop(container::blob_arrow_schema_owned(&blob).unwrap());
            assert_eq!(source.reads.lock().len(), before);
            let result = serialized(&blob);
            let mut retained_bytes = 0;
            memory
                .reserve_with(0, |owned| {
                    retained_bytes = owned;
                    Ok(())
                })
                .unwrap();
            if config == Some(large(true)) {
                assert_eq!(
                    reads,
                    vec![source.range.end - 1024 * 1024..source.range.end]
                );
                assert!(source.admitted.lock()[0] >= container::footer_memory_bound(1024 * 1024));
            } else {
                assert_eq!(reads.len(), 2, "the fixture must require a prefix read");
                assert_eq!(reads[0], source.range.end - 262144..source.range.end);
                assert_eq!(reads[1].end, reads[0].start);
                if let Some(old_reads) = &old_reads {
                    assert_eq!(&reads, old_reads);
                } else {
                    old_reads = Some(reads);
                }
            }
            if let Some(old_result) = &old_result {
                assert_eq!(
                    &result, old_result,
                    "Arrow IPC results must be byte-identical"
                );
                assert_eq!(Some(retained), old_retained);
                assert_eq!(Some(retained_bytes), old_memory);
            } else {
                old_result = Some(result);
                old_retained = Some(retained);
                old_memory = Some(retained_bytes);
            }
        }
    }

    #[test]
    fn docs_footer_small_blob_is_clamped_and_result_is_identical() {
        let data = fixture(false);
        assert!(data.len() < VORTEX_FOOTER_READ_BYTES as usize);
        let mut results = Vec::new();
        for config in [DocsFooterConfig::default(), large(true), large(false)] {
            let source = source(data.clone(), usize::MAX);
            let mut ranged = RangedBlob::new(source.clone(), source.range.clone());
            ranged.configure_docs_footer(config);
            assert_eq!(ranged.footer_window(), source.range);
            let blob = BlobHandle::Ranged(ranged);
            results.push(serialized(&blob));
            assert_eq!(*source.reads.lock(), vec![source.range.clone()]);
        }
        assert!(results.windows(2).all(|pair| pair[0] == pair[1]));
    }

    #[test]
    fn docs_footer_default_and_disabled_keep_resident_suffix_heuristic() {
        let mut source = source(Bytes::from(vec![0; 2 * 1024 * 1024]), usize::MAX);
        let end = source.range.end;
        Arc::get_mut(&mut source).unwrap().resident_start = end - 128 * 1024;
        for config in [DocsFooterConfig::default(), large(false)] {
            let mut blob = RangedBlob::new(source.clone(), source.range.clone());
            let legacy = blob.footer_window();
            blob.configure_docs_footer(config);
            assert_eq!(legacy, end - 128 * 1024..end);
            assert_eq!(blob.footer_window(), legacy);
        }
        let mut blob = RangedBlob::new(source.clone(), source.range.clone());
        blob.configure_docs_footer(large(true));
        assert_eq!(blob.footer_window(), end - 1024 * 1024..end);
    }

    #[test]
    fn docs_footer_large_window_is_admitted_before_fetch() {
        let source = source(
            fixture(true),
            container::footer_memory_bound(1024 * 1024) - 1,
        );
        let mut ranged = RangedBlob::new(source.clone(), source.range.clone());
        ranged.configure_docs_footer(large(true));
        ranged.track_memory(Arc::new(ReaderMemory::new()));
        let blob = BlobHandle::Ranged(ranged);
        let error = crate::with_read_operation(source.budget.clone(), || {
            container::blob_arrow_schema_owned(&blob).err().unwrap()
        });
        assert!(error.to_string().contains("docs-footer-budget"));
        assert!(
            source.reads.lock().is_empty(),
            "budget refusal precedes backend IO"
        );
    }

    #[test]
    #[ignore = "subprocess entry for startup environment validation"]
    fn docs_footer_startup_child() {
        crate::initialize_reader_compact().unwrap();
    }

    #[test]
    fn docs_footer_startup_validates_environment_before_any_reads() {
        use std::{ffi::OsString, os::unix::ffi::OsStringExt, process::Command};
        for (bytes, enabled, success) in [
            (OsString::from("262144"), "true", true),
            (OsString::from("1048576"), "false", true),
            (OsString::from("1048576"), "true", true),
            (OsString::from("0"), "false", false),
            (OsString::from("1048576"), "yes", false),
            (OsString::from_vec(vec![0xff]), "true", false),
        ] {
            let output = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "docs_footer::read_tests::docs_footer_startup_child",
                    "--ignored",
                ])
                .env(BYTES_ENV, bytes)
                .env(ENABLED_ENV, enabled)
                .output()
                .unwrap();
            assert_eq!(
                output.status.success(),
                success,
                "{}",
                String::from_utf8_lossy(&output.stdout)
            );
        }
    }
}
