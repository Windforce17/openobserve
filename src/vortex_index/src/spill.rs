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

//! External-sort spilling for the writer's term accumulation.
//!
//! The build/rebuild paths accumulate `term -> doc ids` in field-sharded
//! hash maps until `finish` because the on-disk index needs terms in global
//! key order with COMPLETE postings — and terms arrive in doc order, so a
//! term's postings keep growing until the last row. For a 10 GB-original
//! compaction rebuild that map reaches 150-200M entries (~15-19 GB), which
//! used to be the compactor's worst-case memory bound.
//!
//! [`TermSpill`] bounds it: when the map's estimated bytes cross the budget,
//! the map is drained — in its natural ascending key order — into a SORTED
//! RUN file on disk, and accumulation restarts empty. At finish the runs and
//! the final resident map k-way merge back into the same `TermSink` the
//! unspilled path uses, so the produced blobs are byte-identical.
//!
//! Two invariants make the merge trivial and exact:
//! - runs are drained only at PUSH boundaries (whole batches), and doc ids grow monotonically
//!   across pushes — so for any term, the doc ranges of run 0 < run 1 < ... < resident map. Equal
//!   keys across cursors merge by CONCATENATION in cursor order (boundary-checked), never by sort.
//! - a term's postings never repeat a doc across runs for the same reason (the in-map consecutive
//!   dedupe covers within-run repeats).
//!
//! Run record layout (little-endian, no compression — the payload is read
//! back exactly once from the local spill volume):
//! `[key_len u32][key bytes][doc_count u32][doc_count x doc_id u32]`.

use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
};

use crate::{
    error::{Result, VixError},
    term_accumulator::TermAccumulator,
};

/// Default in-memory term-accumulation budget before a spill (estimated
/// resident bytes of the map): large enough that move-job builds and
/// ordinary merges never spill, small enough that a 10 GB-group rebuild
/// stays bounded (~10 runs).
pub const DEFAULT_TERM_SPILL_BYTES: usize = 1536 * 1024 * 1024;

/// One writer's spill state: a private temp directory holding sorted runs.
/// Dropping it removes the directory (and on-crash cleanup falls to the
/// caller sweeping the parent, see `core_writer`).
pub(crate) struct TermSpill {
    dir: tempfile::TempDir,
    runs: Vec<PathBuf>,
}

impl TermSpill {
    pub(crate) fn new(base: &Path) -> Result<Self> {
        std::fs::create_dir_all(base)
            .map_err(|e| VixError::Writer(format!("create spill base {base:?}: {e}")))?;
        sweep_stale(base);
        let dir = tempfile::Builder::new()
            .prefix("vix-terms-")
            .tempdir_in(base)
            .map_err(|e| VixError::Writer(format!("create spill dir under {base:?}: {e}")))?;
        Ok(Self {
            dir,
            runs: Vec::new(),
        })
    }

    /// Drain `terms`, sorting each field shard, into a new globally sorted
    /// run file. The accumulator is left empty.
    pub(crate) fn write_run(&mut self, terms: &mut TermAccumulator) -> Result<()> {
        let path = self.dir.path().join(format!("run-{:06}", self.runs.len()));
        let file = File::create(&path)
            .map_err(|e| VixError::Writer(format!("create spill run {path:?}: {e}")))?;
        let mut writer = BufWriter::with_capacity(1024 * 1024, file);
        let shards = terms.drain_sorted_shards();
        let io = |e: std::io::Error| VixError::Writer(format!("write spill run {path:?}: {e}"));
        let mut docs_bytes = Vec::with_capacity(64 * 1024);
        for shard in shards {
            let field = shard.field_id.to_be_bytes();
            for (token, ids) in shard.terms {
                let key_len = token.len().checked_add(2).ok_or_else(|| {
                    VixError::Writer("spill term key length overflows usize".to_string())
                })?;
                writer
                    .write_all(
                        &u32::try_from(key_len)
                            .map_err(|_| {
                                VixError::Writer(format!(
                                    "spill term key is too large: {key_len} bytes"
                                ))
                            })?
                            .to_le_bytes(),
                    )
                    .map_err(io)?;
                writer.write_all(&field).map_err(io)?;
                writer.write_all(&token).map_err(io)?;
                writer
                    .write_all(
                        &u32::try_from(ids.len())
                            .map_err(|_| {
                                VixError::Writer(format!(
                                    "spill postings count is too large: {}",
                                    ids.len()
                                ))
                            })?
                            .to_le_bytes(),
                    )
                    .map_err(io)?;
                for chunk in ids.chunks(docs_bytes.capacity() / size_of::<u32>()) {
                    docs_bytes.clear();
                    for &id in chunk {
                        docs_bytes.extend_from_slice(&id.to_le_bytes());
                    }
                    writer.write_all(&docs_bytes).map_err(io)?;
                }
            }
        }
        writer.flush().map_err(io)?;
        self.runs.push(path);
        Ok(())
    }

    /// Open every run for the finish merge, oldest first.
    pub(crate) fn into_run_readers(self) -> Result<(Vec<RunReader>, tempfile::TempDir)> {
        let mut readers = Vec::with_capacity(self.runs.len());
        for path in &self.runs {
            readers.push(RunReader::open(path)?);
        }
        // the TempDir must outlive the readers; hand it back to the caller
        Ok((readers, self.dir))
    }

    #[cfg(test)]
    pub(crate) fn run_count(&self) -> usize {
        self.runs.len()
    }
}

/// Best-effort removal of spill dirs a CRASHED process left behind (normal
/// exits clean up via `TempDir`). Anything under `base` older than a day is
/// garbage: no merge lives that long, and the sweep runs only when a new
/// spill starts, so it never races an in-flight sibling on the same volume.
fn sweep_stale(base: &Path) {
    let Ok(entries) = std::fs::read_dir(base) else {
        return;
    };
    for entry in entries.flatten() {
        let stale = entry
            .metadata()
            .and_then(|meta| meta.modified())
            .ok()
            .and_then(|modified| modified.elapsed().ok())
            .is_some_and(|age| age.as_secs() > 24 * 3600);
        if stale {
            let path = entry.path();
            log::warn!("vix spill: removing stale spill dir {path:?} (crashed merge leftover)");
            let _ = std::fs::remove_dir_all(&path);
        }
    }
}

/// Streaming cursor over one sorted run: `current()` peeks the smallest
/// not-yet-consumed term, `advance()` moves on.
pub(crate) struct RunReader {
    reader: BufReader<File>,
    path: PathBuf,
    current: Option<(Vec<u8>, Vec<u32>)>,
    docs_scratch: Vec<u8>,
}

impl RunReader {
    fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)
            .map_err(|e| VixError::Writer(format!("open spill run {path:?}: {e}")))?;
        let mut reader = Self {
            reader: BufReader::with_capacity(1024 * 1024, file),
            path: path.to_path_buf(),
            current: None,
            docs_scratch: vec![0; 64 * 1024],
        };
        reader.advance()?;
        Ok(reader)
    }

    pub(crate) fn current(&self) -> Option<&(Vec<u8>, Vec<u32>)> {
        self.current.as_ref()
    }

    fn take_current_docs(&mut self) -> Option<Vec<u32>> {
        self.current.as_mut().map(|(_, docs)| std::mem::take(docs))
    }

    /// Read the next record into `current` (`None` at clean EOF).
    pub(crate) fn advance(&mut self) -> Result<()> {
        let mut len_buf = [0u8; 4];
        match self.reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                self.current = None;
                return Ok(());
            }
            Err(e) => {
                return Err(VixError::Writer(format!(
                    "read spill run {:?}: {e}",
                    self.path
                )));
            }
        }
        let io = |e: std::io::Error| VixError::Writer(format!("read spill run: {e}"));
        let key_len = u32::from_le_bytes(len_buf) as usize;
        let mut key = self.current.take().map_or_else(Vec::new, |(key, _)| key);
        key.resize(key_len, 0);
        self.reader.read_exact(&mut key).map_err(io)?;
        self.reader.read_exact(&mut len_buf).map_err(io)?;
        let doc_count = u32::from_le_bytes(len_buf) as usize;
        let mut ids = Vec::with_capacity(doc_count);
        while ids.len() < doc_count {
            let count = (doc_count - ids.len()).min(self.docs_scratch.len() / size_of::<u32>());
            let bytes = &mut self.docs_scratch[..count * size_of::<u32>()];
            self.reader.read_exact(bytes).map_err(io)?;
            ids.extend(
                bytes
                    .chunks_exact(4)
                    .map(|chunk| u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]])),
            );
        }
        self.current = Some((key, ids));
        Ok(())
    }
}

/// K-way merge of spill runs in ascending key
/// order, calling `emit(key, complete_postings)` once per distinct term.
/// Equal keys concatenate their postings in cursor order (runs oldest first;
/// the final resident suffix was drained as the newest run) — the
/// caller-guaranteed doc-range ordering is
/// verified at every boundary and a violation is a hard error, because it
/// would mean the postings are not the ascending set the format requires.
pub(crate) fn merge_spilled_terms(
    mut runs: Vec<RunReader>,
    mut emit: impl FnMut(&[u8], Vec<u32>) -> Result<()>,
) -> Result<()> {
    let mut min_key = Vec::new();
    loop {
        // smallest current key across all cursors (linear scan: run counts
        // are ~budget-quotient small, never worth a heap)
        let Some(index) = runs
            .iter()
            .enumerate()
            .filter_map(|(index, run)| run.current().map(|(key, _)| (index, key)))
            .min_by(|(_, left), (_, right)| left.cmp(right))
            .map(|(index, _)| index)
        else {
            return Ok(()); // every cursor exhausted
        };
        min_key.clear();
        min_key.extend_from_slice(&runs[index].current().expect("selected live run").0);

        // gather postings for this key in cursor order
        let mut ids: Vec<u32> = Vec::new();
        for run in &mut runs {
            if run
                .current()
                .is_some_and(|(key, _)| key.as_slice() == min_key)
            {
                let mut docs = run.take_current_docs().expect("checked above");
                check_run_boundary(&min_key, &ids, &docs)?;
                if ids.is_empty() {
                    ids = std::mem::take(&mut docs);
                } else {
                    ids.append(&mut docs);
                }
                run.advance()?;
            }
        }
        emit(&min_key, ids)?;
    }
}

/// The concatenation-order invariant: the next cursor's first doc for a key
/// must be strictly greater than the previous cursor's last.
fn check_run_boundary(key: &[u8], collected: &[u32], next: &[u32]) -> Result<()> {
    if let (Some(&last), Some(&first)) = (collected.last(), next.first())
        && first <= last
    {
        return Err(VixError::Writer(format!(
            "spill runs out of doc order for term {:?}: {last} then {first} — the spill \
             boundary invariant was violated",
            String::from_utf8_lossy(key),
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spill_roundtrip_and_merge() {
        let base = tempfile::tempdir().unwrap();
        let mut spill = TermSpill::new(base.path()).unwrap();

        // run 0: docs 0..10, run 1: docs 10..20, run 2: docs 20..30
        let mut run0 = TermAccumulator::new(1);
        run0.extend(0, b"alpha", [0, 3]);
        run0.push(0, b"beta", 1);
        spill.write_run(&mut run0).unwrap();
        assert!(run0.is_empty());

        let mut run1 = TermAccumulator::new(1);
        run1.extend(0, b"alpha", [10, 12]);
        run1.push(0, b"gamma", 11);
        spill.write_run(&mut run1).unwrap();

        let mut run2 = TermAccumulator::new(1);
        run2.push(0, b"beta", 25);
        run2.push(0, b"delta", 21);
        spill.write_run(&mut run2).unwrap();

        assert_eq!(spill.run_count(), 3);
        let (runs, _dir) = spill.into_run_readers().unwrap();
        let mut merged: Vec<(Vec<u8>, Vec<u32>)> = Vec::new();
        merge_spilled_terms(runs, |key, ids| {
            merged.push((key.to_vec(), ids));
            Ok(())
        })
        .unwrap();

        assert_eq!(
            merged,
            vec![
                ([&[0, 0][..], &b"alpha"[..]].concat(), vec![0, 3, 10, 12]),
                ([&[0, 0][..], &b"beta"[..]].concat(), vec![1, 25]),
                ([&[0, 0][..], &b"delta"[..]].concat(), vec![21]),
                ([&[0, 0][..], &b"gamma"[..]].concat(), vec![11]),
            ]
        );
    }

    #[test]
    fn out_of_order_runs_are_rejected() {
        let base = tempfile::tempdir().unwrap();
        let mut spill = TermSpill::new(base.path()).unwrap();
        let mut run0 = TermAccumulator::new(1);
        run0.push(0, b"key", 5);
        spill.write_run(&mut run0).unwrap();
        let mut run1 = TermAccumulator::new(1);
        run1.push(0, b"key", 5); // duplicate doc across runs
        spill.write_run(&mut run1).unwrap();
        let (runs, _dir) = spill.into_run_readers().unwrap();
        let result = merge_spilled_terms(runs, |_, _| Ok(()));
        assert!(result.is_err());
    }
}
