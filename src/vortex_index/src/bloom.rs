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

//! Per-file value blooms (the `bloom` puffin blob).
//!
//! For configured needle fields (`VixWriterOptions::bloom_field_names`,
//! typically `trace_id`/`span_id`), the writer records one [`sbbf`] filter
//! over the field's DISTINCT raw values. The filter is a byproduct of term
//! emission: both write paths already stream every distinct composite term
//! (the normal build in its finish loop, the compaction merge in its k-way
//! workers), so accumulation costs one hash per distinct value and no extra
//! reads.
//!
//! The blob exists so the group `.bf` assembler (`infra::bloom`) can build
//! the transposed hour-group file by **byte transpose alone**: per-file
//! bodies here use the exact same SBBF block layout, block-index mapping and
//! hash as the group format, so assembling a group never re-reads term
//! dictionaries or re-hashes values. Old files simply lack the blob (the
//! container skips unknown blobs, readers probe for presence) — the
//! assembler falls back to streaming their dictionaries once.
//!
//! Values are hashed EXACTLY as the term dictionary stores them (the
//! composite key minus its `\0` + field-id suffix). For raw string fields
//! that is the query string itself, so the search-side probe
//! (`bloom_pruner`, hashing `Condition::Equal` values) matches. Numeric
//! fields store tagged canonical forms — a numeric bloom field would never
//! match the query's plain string and only waste space, so keep bloom
//! fields string-typed (stream-settings validation already keeps them off
//! fts/partition keys).
//!
//! The bloom byte form is PINNED to v1 (`{value}\0{fid BE}`), while the
//! dictionary itself is field-major v2 (`{fid BE}{token}`). Callers that
//! walk a file's dictionary ([`crate::VixReader::for_each_term`]) therefore
//! MUST use [`BloomHashAcc::observe_dict_key`], never [`BloomHashAcc::observe`]:
//! a v2 key fails the v1 split, records nothing, and the filter that gets
//! published then rejects every value the file holds.
//!
//! Blob layout (all little-endian):
//!
//! ```text
//! MAGIC "O2VB" | version u8 = 1 | field_count u32
//! per field:
//!   name_len u16 | name bytes | algo u8 = 1 (SBBF+gxhash)
//!   num_blocks u32 | n_items u32 | body: num_blocks × 32 bytes
//! ```

use hashbrown::HashMap;

use crate::{
    error::{Result, VixError},
    sbbf::{BLOCK_BYTES, Sbbf, hash_value, num_blocks_for},
};

/// Blob magic (head only; the blob sits inside the puffin envelope, which
/// carries its own integrity framing).
pub const FILE_BLOOM_MAGIC: &[u8; 4] = b"O2VB";
/// Format version this crate writes and accepts.
pub const FILE_BLOOM_VERSION: u8 = 1;
/// Algorithm id: SBBF blocks + gxhash64(seed 0) — must agree with
/// `infra::bloom::ALGO_SBBF_GXHASH`.
pub const FILE_BLOOM_ALGO_SBBF_GXHASH: u8 = 0x01;

/// Default false-positive probability for per-file value blooms. Needle
/// queries probe hundreds of files, and every false positive costs a full
/// dictionary walk of that file — spend bits generously (0.1% instead of
/// the textbook 1%).
pub const DEFAULT_FILE_BLOOM_FPP: f64 = 0.001;

/// One field's bloom in a per-file blob.
#[derive(Debug, Clone, PartialEq)]
pub struct FileBloom {
    pub field: String,
    /// Power-of-two block count ([`num_blocks_for`]).
    pub num_blocks: u32,
    /// Distinct values inserted (informational).
    pub n_items: u32,
    /// Raw SBBF body, exactly `num_blocks × 32` bytes.
    pub bytes: Vec<u8>,
}

/// Marker attached (as `anyhow` context) to errors whose cause is the file's
/// OWN BYTES — an internally inconsistent dictionary or terms table, a corrupt
/// `bloom` blob, or an accumulation the checked build refuses to publish.
/// Retrying the same bytes can never succeed, so callers that drive retry
/// queues (the group `.bf` assembler) check [`is_unbuildable`] and take the
/// file OUT of the queue instead of re-burning work on a poison pill every
/// pass. Transient failures (fetch/IO) must never carry this marker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnbuildableFile;

impl std::fmt::Display for UnbuildableFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("unbuildable file (its own bytes fail validation; a retry cannot succeed)")
    }
}

/// Whether `err` carries the [`UnbuildableFile`] marker anywhere in its
/// context chain — i.e. the failure is DETERMINISTIC for these file bytes.
/// Errors without the marker (network fetch failures, IO) are treated as
/// transient and stay retryable.
pub fn is_unbuildable(err: &anyhow::Error) -> bool {
    err.downcast_ref::<UnbuildableFile>().is_some()
}

/// Split a composite dictionary key into its value part and field id
/// (`value ++ b"\0" ++ u16 big-endian id`). Returns `None` for keys too
/// short to carry the suffix (never produced by the writers).
#[inline]
pub fn split_composite_key(key: &[u8]) -> Option<(&[u8], u16)> {
    if key.len() < 3 {
        return None;
    }
    let (value_sep, id) = key.split_at(key.len() - 2);
    let (value, sep) = value_sep.split_at(value_sep.len() - 1);
    if sep[0] != 0 {
        return None;
    }
    Some((value, u16::from_be_bytes([id[0], id[1]])))
}

/// One tracked field's accumulation.
#[derive(Debug, Default)]
struct FieldAcc {
    name: String,
    /// Distinct-value hashes, one per observed key.
    hashes: Vec<u64>,
    /// Keys the walker attributed to this field id that produced NO hash.
    /// Zero unless the dictionary key form drifts from the pinned bloom
    /// form (see [`BloomHashAcc::observe_dict_key`]) — a build carrying any
    /// of these is a bug, never a valid "nothing present" filter.
    dropped: u64,
}

/// Accumulates distinct-value hashes per bloom field during term emission.
/// Term streams visit each distinct composite key exactly once per file, so
/// the vectors hold exact distinct counts — the filters are sized from them
/// at build time (deferring the sizing is what lets streaming paths work).
#[derive(Debug, Default)]
pub struct BloomHashAcc {
    /// field id -> accumulation
    fields: HashMap<u16, FieldAcc>,
    /// Dictionary keys too short to carry a field-id prefix (a corrupt
    /// dictionary): unattributable to a field, so counted and dropped.
    short_keys: u64,
    /// Scratch buffer for the v1 rebuild in [`Self::observe_dict_key`].
    scratch: Vec<u8>,
}

/// What a build refused to publish, folded into one log/error line.
#[derive(Debug, Default)]
struct BuildIssues {
    /// `(field name, dropped key count)`.
    dropped: Vec<(String, u64)>,
    /// Tracked fields the accumulation never saw a key for.
    empty: Vec<String>,
    short_keys: u64,
}

impl BuildIssues {
    /// Whether the accumulation is INTERNALLY inconsistent: keys reached
    /// the accumulator and produced no hash. Never a legitimate empty
    /// filter — the key form does not match what the bloom hashes.
    fn is_bug(&self) -> bool {
        !self.dropped.is_empty() || self.short_keys > 0
    }

    fn describe(&self) -> String {
        let fields: Vec<String> = self
            .dropped
            .iter()
            .map(|(name, count)| format!("{name}: {count} keys"))
            .collect();
        format!(
            "bloom accumulation dropped keys ([{}] plus {} unattributable): the dictionary key \
             form does not match the pinned bloom key form, so no filter can be trusted",
            fields.join(", "),
            self.short_keys
        )
    }
}

impl BloomHashAcc {
    /// An accumulator over pre-resolved `(field id, field name)` pairs —
    /// callers resolve configured names against their own field table
    /// (names absent from a file are simply not tracked).
    pub fn from_pairs<I: IntoIterator<Item = (u16, String)>>(pairs: I) -> Self {
        let mut fields = HashMap::new();
        for (id, name) in pairs {
            fields.insert(
                id,
                FieldAcc {
                    name,
                    ..Default::default()
                },
            );
        }
        Self {
            fields,
            ..Default::default()
        }
    }

    /// Whether any field is tracked (skip the observe calls entirely when
    /// not — the common no-bloom-fields case must stay zero-cost).
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Observe one distinct term key in the PINNED bloom byte form,
    /// `{value}\0{fid BE}`. Field-major dictionary keys (what the reader
    /// yields) are NOT this form — route those through
    /// [`Self::observe_dict_key`].
    #[inline]
    pub fn observe(&mut self, composite_key: &[u8]) {
        if self.fields.is_empty() {
            return;
        }
        if let Some((value, id)) = split_composite_key(composite_key)
            && let Some(entry) = self.fields.get_mut(&id)
        {
            entry.hashes.push(hash_value(value));
        }
    }

    /// Observe one FIELD-MAJOR (v2) dictionary key, `{fid u16 BE}{token}` —
    /// the form [`crate::VixReader::for_each_term`] yields. The key is
    /// rebuilt into the pinned bloom form through the one canonical choke
    /// point ([`crate::query::bloom_canonical_key`]) before hashing, so a
    /// dictionary walk accumulates bit-identically to the writers.
    ///
    /// Keys whose rebuild does not round-trip are counted, not silently
    /// ignored: [`Self::build_checked`] refuses to publish after any of
    /// them.
    #[inline]
    pub fn observe_dict_key(&mut self, key: &[u8]) {
        if self.fields.is_empty() {
            return;
        }
        let Some((_, id)) = crate::query::split_key(key) else {
            // too short to carry a field id: corrupt dictionary
            self.short_keys = self.short_keys.saturating_add(1);
            return;
        };
        // untracked field ids (every non-bloom field, plus key terms) are
        // the common case and must stay a single lookup
        let Some(before) = self.fields.get(&id).map(|entry| entry.hashes.len()) else {
            return;
        };
        let mut scratch = std::mem::take(&mut self.scratch);
        self.observe(crate::query::bloom_canonical_key(key, &mut scratch));
        self.scratch = scratch;
        if let Some(entry) = self.fields.get_mut(&id)
            && entry.hashes.len() == before
        {
            entry.dropped = entry.dropped.saturating_add(1);
        }
    }

    /// Merge another accumulator (parallel merge workers each hold one).
    pub fn merge(&mut self, other: BloomHashAcc) {
        self.short_keys = self.short_keys.saturating_add(other.short_keys);
        for (
            id,
            FieldAcc {
                name,
                hashes,
                dropped,
            },
        ) in other.fields
        {
            let entry = self.fields.entry(id).or_insert_with(|| FieldAcc {
                name,
                ..Default::default()
            });
            entry.hashes.extend(hashes);
            entry.dropped = entry.dropped.saturating_add(dropped);
        }
    }

    /// Build the per-field filters of a walk that covered the file's WHOLE
    /// term stream — the writer's finish loop and the merge sink, both by
    /// construction.
    ///
    /// A tracked field that observed nothing still emits a 1-block filter:
    /// "field known, nothing present", so the probe rejects every value
    /// instead of falling back. That is the one LEGITIMATELY empty case —
    /// the field id exists in the output schema (it came from this build's
    /// own field table) but no value landed in this file, e.g. a merge that
    /// dropped every document carrying it.
    ///
    /// Fields with dropped keys are never published (an empty filter there
    /// would reject values the file demonstrably holds) and are logged at
    /// error level; callers that can propagate instead should use
    /// [`Self::build_checked`].
    pub fn build(self, fpp: f64) -> Vec<FileBloom> {
        let (out, issues) = self.finish(fpp, true);
        if issues.is_bug() {
            log::error!(
                "[VIX:BLOOM] refusing to publish filters: {}",
                issues.describe()
            );
        }
        out
    }

    /// Build the per-field filters of a walk over a file's dictionary
    /// ([`Self::observe_dict_key`]), refusing to publish anything that
    /// cannot be trusted:
    ///
    /// - dropped keys => hard error. The walk saw keys for the field and hashed none, so any filter
    ///   would reject values the file demonstrably holds.
    /// - no keys at all for a tracked field => the field is WITHHELD (no filter) with a warning.
    ///   Unlike [`Self::build`], a dictionary walk cannot distinguish "the file holds no value for
    ///   this field" from "the walk never reached the field's keys", and the pruner reads a missing
    ///   field as "no info" (keeps the file) but an empty filter as "no value here" (drops the
    ///   file) — so the safe side is to publish nothing.
    pub fn build_checked(self, fpp: f64) -> Result<Vec<FileBloom>> {
        let (out, issues) = self.finish(fpp, false);
        if issues.is_bug() {
            return Err(VixError::Writer(issues.describe()));
        }
        for name in &issues.empty {
            log::warn!(
                "[VIX:BLOOM] field {name:?} has no dictionary keys in this file; withheld (no \
                 filter published) rather than publishing a reject-everything filter"
            );
        }
        Ok(out)
    }

    /// Shared build: emits one filter per field, in field-name order.
    /// `publish_empty` decides what happens to a field with no observed
    /// values (see [`Self::build`] vs [`Self::build_checked`]); fields with
    /// dropped keys are withheld either way.
    fn finish(self, fpp: f64, publish_empty: bool) -> (Vec<FileBloom>, BuildIssues) {
        let mut out: Vec<FileBloom> = Vec::with_capacity(self.fields.len());
        let mut issues = BuildIssues {
            short_keys: self.short_keys,
            ..Default::default()
        };
        let mut fields: Vec<FieldAcc> = self.fields.into_values().collect();
        fields.sort_by(|a, b| a.name.cmp(&b.name));
        for FieldAcc {
            name,
            hashes,
            dropped,
        } in fields
        {
            if dropped > 0 {
                issues.dropped.push((name, dropped));
                continue;
            }
            if hashes.is_empty() {
                issues.empty.push(name.clone());
                if !publish_empty {
                    continue;
                }
            }
            let num_blocks = num_blocks_for(hashes.len() as u64, fpp);
            let mut sbbf = Sbbf::new_with_num_blocks(num_blocks);
            for h in &hashes {
                sbbf.insert_hash(*h);
            }
            out.push(FileBloom {
                field: name,
                num_blocks: sbbf.num_blocks(),
                n_items: hashes.len().min(u32::MAX as usize) as u32,
                bytes: sbbf.to_bytes(),
            });
        }
        (out, issues)
    }
}

/// Serialize per-file blooms into the `bloom` blob body.
pub fn serialize_file_blooms(blooms: &[FileBloom]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(FILE_BLOOM_MAGIC);
    out.push(FILE_BLOOM_VERSION);
    out.extend_from_slice(&(blooms.len() as u32).to_le_bytes());
    for b in blooms {
        if b.field.len() > u16::MAX as usize {
            return Err(VixError::Writer(format!(
                "bloom field name too long: {} bytes",
                b.field.len()
            )));
        }
        if b.bytes.len() != b.num_blocks as usize * BLOCK_BYTES {
            return Err(VixError::Writer(format!(
                "bloom body length {} != num_blocks {} x 32 for field {:?}",
                b.bytes.len(),
                b.num_blocks,
                b.field
            )));
        }
        out.extend_from_slice(&(b.field.len() as u16).to_le_bytes());
        out.extend_from_slice(b.field.as_bytes());
        out.push(FILE_BLOOM_ALGO_SBBF_GXHASH);
        out.extend_from_slice(&b.num_blocks.to_le_bytes());
        out.extend_from_slice(&b.n_items.to_le_bytes());
        out.extend_from_slice(&b.bytes);
    }
    Ok(out)
}

/// Parse a `bloom` blob body back into per-field filters.
pub fn parse_file_blooms(bytes: &[u8]) -> Result<Vec<FileBloom>> {
    let malformed = |msg: &str| VixError::Malformed(format!("bloom blob: {msg}"));
    let mut pos = 0usize;
    let take = |pos: &mut usize, n: usize| -> Result<&[u8]> {
        let end = pos
            .checked_add(n)
            .filter(|end| *end <= bytes.len())
            .ok_or_else(|| malformed("truncated"))?;
        let out = &bytes[*pos..end];
        *pos = end;
        Ok(out)
    };
    if take(&mut pos, 4)? != FILE_BLOOM_MAGIC {
        return Err(malformed("bad magic"));
    }
    let version = take(&mut pos, 1)?[0];
    if version != FILE_BLOOM_VERSION {
        return Err(malformed(&format!("unsupported version {version}")));
    }
    let field_count = u32::from_le_bytes(take(&mut pos, 4)?.try_into().unwrap()) as usize;
    // `field_count` is file data and sizes the allocation below: bound it by
    // what the remaining bytes could possibly hold FIRST (a corrupt count
    // would otherwise reserve gigabytes, and an allocation failure aborts
    // the process instead of unwinding).
    const MIN_FIELD_BYTES: usize = 2 + 1 + 4 + 4 + BLOCK_BYTES;
    let max_fields = bytes.len().saturating_sub(pos) / MIN_FIELD_BYTES;
    if field_count > max_fields {
        return Err(malformed(&format!(
            "field_count {field_count} exceeds the {} fields the remaining {} bytes can hold",
            max_fields,
            bytes.len() - pos
        )));
    }
    let mut out = Vec::with_capacity(field_count);
    for _ in 0..field_count {
        let name_len = u16::from_le_bytes(take(&mut pos, 2)?.try_into().unwrap()) as usize;
        let name = std::str::from_utf8(take(&mut pos, name_len)?)
            .map_err(|_| malformed("field name not utf-8"))?
            .to_string();
        let algo = take(&mut pos, 1)?[0];
        if algo != FILE_BLOOM_ALGO_SBBF_GXHASH {
            return Err(malformed(&format!("unsupported algo {algo}")));
        }
        let num_blocks = u32::from_le_bytes(take(&mut pos, 4)?.try_into().unwrap());
        if num_blocks == 0 {
            return Err(malformed("zero num_blocks"));
        }
        let n_items = u32::from_le_bytes(take(&mut pos, 4)?.try_into().unwrap());
        let body_len = num_blocks as usize * BLOCK_BYTES;
        let body = take(&mut pos, body_len)?.to_vec();
        out.push(FileBloom {
            field: name,
            num_blocks,
            n_items,
            bytes: body,
        });
    }
    if pos != bytes.len() {
        return Err(malformed("trailing bytes"));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sbbf::{block_index, check_block};

    fn composite(value: &[u8], id: u16) -> Vec<u8> {
        let mut k = value.to_vec();
        k.push(0);
        k.extend_from_slice(&id.to_be_bytes());
        k
    }

    /// The reader-side single-block probe over a filter's raw body — what
    /// `bloom_pruner` runs against the group `.bf`.
    fn probe(bloom: &FileBloom, value: &[u8]) -> bool {
        let hash = hash_value(value);
        let index = block_index(hash, bloom.num_blocks) as usize;
        let block: &[u8; BLOCK_BYTES] = bloom.bytes[index * BLOCK_BYTES..(index + 1) * BLOCK_BYTES]
            .try_into()
            .unwrap();
        check_block(block, hash)
    }

    /// A real `.vix` holding one raw-term `trace_id` column and NO per-file
    /// bloom blob — exactly the shape the group `.bf` assembler backfills by
    /// streaming the dictionary.
    fn build_backfill_file(values: &[&str]) -> Vec<u8> {
        use std::sync::Arc;

        use arrow::{
            array::{ArrayRef, Int64Array, StringArray},
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        };

        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("trace_id", DataType::Utf8, true),
        ]));
        let timestamps: Vec<i64> = (0..values.len() as i64).map(|i| 1_000 + i).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(timestamps)) as ArrayRef,
                Arc::new(StringArray::from(values.to_vec())) as ArrayRef,
            ],
        )
        .unwrap();
        let source = StringArray::from_iter_values(
            values.iter().map(|v| format!("{{\"trace_id\":\"{v}\"}}")),
        );
        let mut writer = crate::VixWriter::new(&schema, crate::VixWriterOptions::default(), false);
        writer
            .push_batch_with_source(&batch, &source, None)
            .unwrap();
        writer.finish().unwrap()
    }

    /// Re-pack a `.vix` with its `row_count` property overwritten: the
    /// cheapest way to fabricate a `doc_count` cell that exceeds the file's
    /// row count (the corrupt shape the walk must reject).
    fn repack_with_row_count(data: &[u8], row_count: u64) -> Vec<u8> {
        use crate::container::{
            BLOB_TAG_DICT, BLOB_TAG_DOCS, BLOB_TAG_TERMS, BLOB_TYPE_DICT, BLOB_TYPE_DOCS,
            BLOB_TYPE_TERMS, BlobHandle, PROP_ROW_COUNT, build_container, parse_container,
        };

        let data = bytes::Bytes::copy_from_slice(data);
        let container = parse_container(&data).unwrap();
        let properties: Vec<(String, String)> = container
            .properties
            .iter()
            .map(|(key, value)| {
                if key == PROP_ROW_COUNT {
                    (key.clone(), row_count.to_string())
                } else {
                    (key.clone(), value.clone())
                }
            })
            .collect();
        let mem = |handle: Option<BlobHandle>| match handle {
            Some(BlobHandle::Mem(bytes)) => Some(bytes.to_vec()),
            Some(BlobHandle::Ranged(_)) => unreachable!("parsed from memory"),
            None => None,
        };
        let mut blobs: Vec<(&'static str, &'static str, Vec<u8>)> = Vec::new();
        if let Some(dict) = mem(container.dict) {
            blobs.push((BLOB_TYPE_DICT, BLOB_TAG_DICT, dict));
        }
        if let Some(terms) = mem(container.terms) {
            blobs.push((BLOB_TYPE_TERMS, BLOB_TAG_TERMS, terms));
        }
        if let Some(docs) = mem(container.docs) {
            blobs.push((BLOB_TYPE_DOCS, BLOB_TAG_DOCS, docs));
        }
        build_container(properties, blobs).unwrap()
    }

    /// The group `.bf` backfill: FIELD-MAJOR keys straight off
    /// `for_each_term` must produce a filter that MATCHES the values the
    /// file holds. Feeding those keys to `observe` (the pre-fix shape)
    /// records nothing and publishes a filter that rejects every one of
    /// them — a silent wrong-results bug, since the group `.bf` is
    /// authoritative in the pruner.
    #[test]
    fn dict_key_backfill_matches_file_values() {
        let values = ["trace-a", "trace-b", "trace-c"];
        let file = build_backfill_file(&values);
        let reader = crate::VixReader::open(bytes::Bytes::from(file)).unwrap();
        let field_id = reader.term_field_id("trace_id").unwrap();

        let mut acc = BloomHashAcc::from_pairs([(field_id, "trace_id".to_string())]);
        reader
            .for_each_term(&mut |key, _doc_count, _ids| {
                acc.observe_dict_key(key);
                Ok(())
            })
            .unwrap();
        let blooms = acc.build_checked(DEFAULT_FILE_BLOOM_FPP).unwrap();
        assert_eq!(blooms.len(), 1);
        assert_eq!(blooms[0].field, "trace_id");
        assert_eq!(blooms[0].n_items, values.len() as u32);
        for value in values {
            assert!(probe(&blooms[0], value.as_bytes()), "missed {value}");
        }
        assert!(!probe(&blooms[0], b"trace-absent"));

        // the pre-fix shape, kept as the regression witness
        let mut raw = BloomHashAcc::from_pairs([(field_id, "trace_id".to_string())]);
        reader
            .for_each_term(&mut |key, _doc_count, _ids| {
                raw.observe(key);
                Ok(())
            })
            .unwrap();
        let poisoned = raw.build(DEFAULT_FILE_BLOOM_FPP);
        assert_eq!(poisoned[0].n_items, 0);
        for value in values {
            assert!(
                !probe(&poisoned[0], value.as_bytes()),
                "an empty filter rejects {value}, which the file holds"
            );
        }
    }

    /// Empty accumulations mean different things per entry point: a
    /// whole-stream build publishes the documented "field known, nothing
    /// present" filter; a dictionary walk cannot prove that, so it withholds
    /// the field instead of publishing a reject-everything filter.
    #[test]
    fn empty_field_publishes_only_from_a_whole_stream_build() {
        let pairs = [
            (1u16, "populated".to_string()),
            (2u16, "silent".to_string()),
        ];
        let mut acc = BloomHashAcc::from_pairs(pairs.clone());
        acc.observe(&composite(b"v", 1));
        let published = acc.build(DEFAULT_FILE_BLOOM_FPP);
        assert_eq!(published.len(), 2);
        let silent = published.iter().find(|b| b.field == "silent").unwrap();
        assert_eq!(silent.n_items, 0);
        assert!(!probe(silent, b"v"), "an empty filter rejects every value");

        let mut acc = BloomHashAcc::from_pairs(pairs);
        acc.observe(&composite(b"v", 1));
        let checked = acc.build_checked(DEFAULT_FILE_BLOOM_FPP).unwrap();
        assert_eq!(checked.len(), 1);
        assert_eq!(checked[0].field, "populated");
        assert!(probe(&checked[0], b"v"));
    }

    /// Keys that reach the accumulator and hash to nothing are never a valid
    /// "nothing present" filter: `build_checked` errors, `build` withholds.
    #[test]
    fn dropped_keys_are_never_published() {
        let mut acc = BloomHashAcc::from_pairs([(1u16, "trace_id".to_string())]);
        acc.observe(&composite(b"kept", 1));
        if let Some(entry) = acc.fields.get_mut(&1) {
            entry.dropped = 2;
        }
        let err = acc.build_checked(DEFAULT_FILE_BLOOM_FPP).unwrap_err();
        let message = err.to_string();
        assert!(message.contains("trace_id: 2 keys"), "{message}");

        let mut acc = BloomHashAcc::from_pairs([(1u16, "trace_id".to_string())]);
        acc.observe(&composite(b"kept", 1));
        if let Some(entry) = acc.fields.get_mut(&1) {
            entry.dropped = 1;
        }
        assert!(acc.build(DEFAULT_FILE_BLOOM_FPP).is_empty());

        // a dictionary key too short to carry a field id is corrupt input:
        // counted, and it fails the checked build rather than shrinking the
        // filter silently
        let mut acc = BloomHashAcc::from_pairs([(1u16, "trace_id".to_string())]);
        acc.observe_dict_key(b"\x00");
        assert_eq!(acc.short_keys, 1);
        assert!(acc.build_checked(DEFAULT_FILE_BLOOM_FPP).is_err());
    }

    /// A corrupt `doc_count` cell must fail the dictionary walk cleanly —
    /// the value sizes a `reserve`, and an allocation abort kills the
    /// compactor instead of unwinding. The failure is a pure function of the
    /// file bytes, so it must carry the [`UnbuildableFile`] marker: retry
    /// queues take the file out instead of spinning on it forever.
    #[test]
    fn backfill_walk_rejects_doc_count_above_row_count() {
        let file = build_backfill_file(&["dup", "dup", "solo"]);
        let poisoned = repack_with_row_count(&file, 1);
        let reader = crate::VixReader::open(bytes::Bytes::from(poisoned)).unwrap();
        let err = reader
            .for_each_term(&mut |_key, _doc_count, _ids| Ok(()))
            .unwrap_err();
        let message = format!("{err:#}");
        assert!(message.contains("exceeds row_count 1"), "{message}");
        assert!(
            is_unbuildable(&err),
            "a deterministic walk failure must classify unbuildable: {message}"
        );
    }

    /// A `bloom` blob that fetches but does not parse is file-shaped
    /// corruption (marked unbuildable); a source whose FETCHES fail is a
    /// transient outage and must NOT be marked — poisoning a healthy file on
    /// a network blip would silently forfeit its bloom coverage forever.
    #[test]
    fn unbuildable_marks_corrupt_bytes_never_fetch_failures() {
        use crate::container::{
            BLOB_TAG_BLOOM, BLOB_TAG_DICT, BLOB_TAG_DOCS, BLOB_TAG_TERMS, BLOB_TYPE_BLOOM,
            BLOB_TYPE_DICT, BLOB_TYPE_DOCS, BLOB_TYPE_TERMS, BlobHandle, build_container,
            parse_container,
        };

        // a real file with a real bloom blob...
        let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("_timestamp", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("trace_id", arrow::datatypes::DataType::Utf8, true),
        ]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            std::sync::Arc::clone(&schema),
            vec![
                std::sync::Arc::new(arrow::array::Int64Array::from(vec![1_000i64]))
                    as arrow::array::ArrayRef,
                std::sync::Arc::new(arrow::array::StringArray::from(vec!["trace-a"]))
                    as arrow::array::ArrayRef,
            ],
        )
        .unwrap();
        let source =
            arrow::array::StringArray::from_iter_values(["{\"trace_id\":\"trace-a\"}"]);
        let mut writer = crate::VixWriter::new(
            &schema,
            crate::VixWriterOptions {
                bloom_field_names: vec!["trace_id".to_string()],
                ..Default::default()
            },
            false,
        );
        writer.push_batch_with_source(&batch, &source, None).unwrap();
        let data = bytes::Bytes::from(writer.finish().unwrap());

        // ...repacked with the bloom blob's bytes replaced by garbage
        let container = parse_container(&data).unwrap();
        assert!(container.bloom.is_some(), "writer must emit the blob");
        let properties: Vec<(String, String)> = container
            .properties
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        let mem = |handle: Option<BlobHandle>| match handle {
            Some(BlobHandle::Mem(bytes)) => Some(bytes.to_vec()),
            Some(BlobHandle::Ranged(_)) => unreachable!("parsed from memory"),
            None => None,
        };
        let mut blobs: Vec<(&'static str, &'static str, Vec<u8>)> = Vec::new();
        if let Some(dict) = mem(container.dict) {
            blobs.push((BLOB_TYPE_DICT, BLOB_TAG_DICT, dict));
        }
        if let Some(terms) = mem(container.terms) {
            blobs.push((BLOB_TYPE_TERMS, BLOB_TAG_TERMS, terms));
        }
        if let Some(docs) = mem(container.docs) {
            blobs.push((BLOB_TYPE_DOCS, BLOB_TAG_DOCS, docs));
        }
        blobs.push((BLOB_TYPE_BLOOM, BLOB_TAG_BLOOM, vec![0xFF; 32]));
        let corrupt = build_container(properties, blobs).unwrap();

        let reader = crate::VixReader::open(bytes::Bytes::from(corrupt)).unwrap();
        let err = reader.file_blooms().unwrap_err();
        assert!(
            is_unbuildable(&err),
            "a corrupt blob is deterministic: {err:#}"
        );

        // transient side: every fetch fails — the open error is NOT marked
        struct DeadSource {
            len: u64,
        }
        impl crate::VixRangeSource for DeadSource {
            fn len(&self) -> u64 {
                self.len
            }
            fn fetch(
                &self,
                range: std::ops::Range<u64>,
            ) -> futures::future::BoxFuture<'static, anyhow::Result<bytes::Bytes>> {
                use futures::FutureExt;
                futures::future::ready(Err(anyhow::anyhow!(
                    "connection reset fetching {range:?}"
                )))
                .boxed()
            }
        }
        let dead: std::sync::Arc<dyn crate::VixRangeSource> = std::sync::Arc::new(DeadSource {
            len: data.len() as u64,
        });
        let err = match crate::VixReader::open_ranged(dead) {
            Ok(_) => panic!("open over a dead source must fail"),
            Err(e) => e,
        };
        assert!(
            !is_unbuildable(&err),
            "a fetch failure must stay retryable: {err:#}"
        );
    }

    /// LEGITIMATE states must never trip the fail-closed path: a file whose
    /// tracked field holds no terms builds Ok (field withheld, not an
    /// error), and a legitimately short token — the 2-byte field-major key
    /// of an EMPTY token is the shortest key a writer can produce — is
    /// observed, not counted as a short (corrupt) key.
    #[test]
    fn legitimate_empty_and_short_states_do_not_hard_error() {
        // all-null values: the field is planned but the file holds no terms
        let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("_timestamp", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("trace_id", arrow::datatypes::DataType::Utf8, true),
        ]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            std::sync::Arc::clone(&schema),
            vec![
                std::sync::Arc::new(arrow::array::Int64Array::from(vec![1_000i64, 1_001]))
                    as arrow::array::ArrayRef,
                std::sync::Arc::new(arrow::array::StringArray::from(vec![
                    None::<&str>,
                    None::<&str>,
                ])) as arrow::array::ArrayRef,
            ],
        )
        .unwrap();
        let source = arrow::array::StringArray::from_iter_values(["{}", "{}"]);
        let mut writer =
            crate::VixWriter::new(&schema, crate::VixWriterOptions::default(), false);
        writer.push_batch_with_source(&batch, &source, None).unwrap();
        let reader = crate::VixReader::open(bytes::Bytes::from(writer.finish().unwrap())).unwrap();

        let pairs: Vec<(u16, String)> = reader
            .term_field_id("trace_id")
            .map(|id| (id, "trace_id".to_string()))
            .into_iter()
            .collect();
        let mut acc = BloomHashAcc::from_pairs(pairs);
        reader
            .for_each_term(&mut |key, _doc_count, _ids| {
                acc.observe_dict_key(key);
                Ok(())
            })
            .unwrap();
        let blooms = acc
            .build_checked(DEFAULT_FILE_BLOOM_FPP)
            .expect("a termless file is legitimate, never a hard error");
        assert!(blooms.is_empty(), "nothing provable, nothing published");

        // empty token: field-major key is exactly the 2-byte fid — observed
        // (an empty raw value is a real distinct value), never "corrupt"
        let mut acc = BloomHashAcc::from_pairs([(7u16, "trace_id".to_string())]);
        acc.observe_dict_key(&7u16.to_be_bytes());
        acc.observe_dict_key(&{
            let mut key = 7u16.to_be_bytes().to_vec();
            key.push(b'a');
            key
        });
        assert_eq!(acc.short_keys, 0, "legitimate short tokens are not corrupt");
        let blooms = acc.build_checked(DEFAULT_FILE_BLOOM_FPP).unwrap();
        assert_eq!(blooms.len(), 1);
        assert_eq!(blooms[0].n_items, 2);
        assert!(probe(&blooms[0], b""), "the empty value must probe as a hit");
        assert!(probe(&blooms[0], b"a"));
    }

    #[test]
    fn parse_rejects_absurd_field_count() {
        let mut blob = Vec::new();
        blob.extend_from_slice(FILE_BLOOM_MAGIC);
        blob.push(FILE_BLOOM_VERSION);
        blob.extend_from_slice(&u32::MAX.to_le_bytes());
        let err = parse_file_blooms(&blob).unwrap_err();
        assert!(err.to_string().contains("field_count"), "{err}");
    }

    #[test]
    fn split_composite_key_round_trips() {
        let k = composite(b"abc123", 7);
        assert_eq!(split_composite_key(&k), Some((b"abc123".as_slice(), 7)));
        // empty value is valid (3-byte key)
        let k = composite(b"", 300);
        assert_eq!(split_composite_key(&k), Some((b"".as_slice(), 300)));
        assert_eq!(split_composite_key(b"ab"), None);
    }

    #[test]
    fn acc_builds_probeable_blooms() {
        let mut acc = BloomHashAcc::from_pairs([(3u16, "trace_id".to_string())]);
        assert!(!acc.is_empty());
        for i in 0..5000u32 {
            acc.observe(&composite(format!("t-{i}").as_bytes(), 3));
            acc.observe(&composite(format!("o-{i}").as_bytes(), 4)); // untracked
        }
        let blooms = acc.build(DEFAULT_FILE_BLOOM_FPP);
        assert_eq!(blooms.len(), 1);
        let b = &blooms[0];
        assert_eq!(b.field, "trace_id");
        assert_eq!(b.n_items, 5000);
        assert!(b.num_blocks.is_power_of_two());
        // every inserted value answers "maybe" through the reader-side
        // single-block path over the raw body bytes
        for i in 0..5000u32 {
            let h = hash_value(format!("t-{i}").as_bytes());
            let bi = block_index(h, b.num_blocks) as usize;
            let block: &[u8; BLOCK_BYTES] = b.bytes[bi * BLOCK_BYTES..(bi + 1) * BLOCK_BYTES]
                .try_into()
                .unwrap();
            assert!(check_block(block, h), "missed t-{i}");
        }
        // and misses overwhelmingly answer "no" at the 0.1% FPP
        let mut fp = 0;
        for i in 0..10_000u32 {
            let h = hash_value(format!("miss-{i}").as_bytes());
            let bi = block_index(h, b.num_blocks) as usize;
            let block: &[u8; BLOCK_BYTES] = b.bytes[bi * BLOCK_BYTES..(bi + 1) * BLOCK_BYTES]
                .try_into()
                .unwrap();
            if check_block(block, h) {
                fp += 1;
            }
        }
        assert!(fp < 40, "FPP way above 0.1%: {fp}/10000");
    }

    #[test]
    fn merge_concatenates_workers() {
        let mut a = BloomHashAcc::from_pairs([(1u16, "trace_id".to_string())]);
        let mut b = BloomHashAcc::from_pairs([(1u16, "trace_id".to_string())]);
        a.observe(&composite(b"one", 1));
        b.observe(&composite(b"two", 1));
        a.merge(b);
        let blooms = a.build(0.001);
        assert_eq!(blooms[0].n_items, 2);
    }

    #[test]
    fn serialize_parse_round_trips() {
        let mut acc = BloomHashAcc::from_pairs([
            (1u16, "trace_id".to_string()),
            (2u16, "span_id".to_string()),
        ]);
        for i in 0..100u32 {
            acc.observe(&composite(format!("t-{i}").as_bytes(), 1));
            acc.observe(&composite(format!("s-{i}").as_bytes(), 2));
        }
        let blooms = acc.build(0.001);
        let blob = serialize_file_blooms(&blooms).unwrap();
        let parsed = parse_file_blooms(&blob).unwrap();
        assert_eq!(parsed, blooms);
        // deterministic field order (sorted by name)
        assert_eq!(parsed[0].field, "span_id");
        assert_eq!(parsed[1].field, "trace_id");
    }

    #[test]
    fn parse_rejects_garbage() {
        assert!(parse_file_blooms(b"").is_err());
        assert!(parse_file_blooms(b"XXXX\x01\x00\x00\x00\x00").is_err());
        let mut acc = BloomHashAcc::from_pairs([(1u16, "f".to_string())]);
        acc.observe(&composite(b"v", 1));
        let blob = serialize_file_blooms(&acc.build(0.01)).unwrap();
        // truncated body
        assert!(parse_file_blooms(&blob[..blob.len() - 1]).is_err());
        // trailing junk
        let mut junk = blob.clone();
        junk.push(0);
        assert!(parse_file_blooms(&junk).is_err());
    }
}
