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

//! Block-granular probes of one sidecar's per-file bloom blob ([`crate::bloom`]).
//!
//! The group `.bf` transposes an hour's per-file blooms so a needle lookup
//! reads one block row per group. Files the assembler has not stamped yet
//! (`bloom_ver = 0`: the open hour, fresh merge outputs, late stragglers)
//! carry the very same SBBF blocks inside their own `.vxi` sidecar.
//! [`FileBloomProbe`] reads them there: the sidecar footer, the blob's
//! section headers (never a body byte), and then exactly the addressed
//! 32-byte blocks of a probe — one batched fetch per file and value set.
//!
//! Verdicts mirror the pruner's `.bf` path. A per-field section answers
//! directly unless the field is `partial` in this file (its value set is
//! knowingly incomplete — a miss would be a wrong drop). The composite
//! section answers only when every guard probe of the field hits (the writer
//! emits guards for fields it covered completely; see
//! [`crate::bloom::COMPOSITE_GUARD_PROBES`]). Everything else is "no
//! information", which callers must treat as keep.

use std::{collections::HashSet, ops::Range, sync::Arc};

use bytes::Bytes;

use crate::{
    bloom::{
        COMPOSITE_BLOOM_FIELD, COMPOSITE_GUARD_PROBES, FILE_BLOOM_ALGO_SBBF_GXHASH,
        FILE_BLOOM_MAGIC, FILE_BLOOM_VERSION, composite_guard_key, composite_value_key,
    },
    container::{
        BlobHandle, FieldEntry, PROP_BLOOM_SECTIONS, PROP_COLUMNS_COMPLETE, PROP_FIELDS,
        PROP_LAYOUT_GEN, PROP_PARTIAL_FIELDS, parse_container_ranged,
        require_supported_index_format,
    },
    error::{Result, VixError},
    sbbf::{BLOCK_BYTES, block_index, check_block, hash_value},
    source::{VixRangeSource, block_fetch, block_fetch_many},
};

/// Bytes fetched per header window while walking the blob's section table.
/// Section headers are a few dozen bytes; one window normally covers the
/// blob head and every header of a one- or two-section blob.
const HEADER_WINDOW: u64 = 4096;

/// One field section of the blob: header parsed, body left on the object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileBloomSection {
    pub field: String,
    pub num_blocks: u32,
    /// Distinct values inserted (informational).
    pub n_items: u32,
    /// The SBBF body, relative to the blob start (`num_blocks × 32` bytes).
    body: Range<u64>,
}

/// Header-only view of a sidecar's per-file bloom blob, ready to answer
/// value probes with one batched block fetch each — plus the sidecar's
/// field-presence declaration, so a probe over a sidecar WITHOUT a bloom
/// blob still answers "does this file have field f at all?".
pub struct FileBloomProbe {
    /// `None`: the sidecar carries no bloom blob (files written before the
    /// per-file bloom capability) — every value probe is "no info".
    blob: Option<BlobHandle>,
    sections: Vec<FileBloomSection>,
    /// Fields whose value terms are incomplete in this file: their per-field
    /// section (if any) must not be trusted, and the writer never claims
    /// composite coverage for them.
    partial_fields: HashSet<String>,
    /// The sidecar's parsed `fields` table (DESIGN-COLD-MISS §2 S5: stage 1b
    /// drops a field absent here before any blob read).
    field_entries: Vec<FieldEntry>,
    /// The data object's `columns_complete` license as stamped on the
    /// sidecar: only then is `field_entries` the file's AUTHORITATIVE column
    /// set (an unlicensed file may hold a field in `_source` that never
    /// became a column). Absent on sidecars written before the stamp.
    columns_complete: bool,
    /// The sidecar `layout_gen` property (`0` when absent) — see
    /// [`crate::VixReader::layout_gen`].
    layout_gen: u32,
}

impl std::fmt::Debug for FileBloomProbe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileBloomProbe")
            .field("blob_len", &self.blob.as_ref().map(BlobHandle::len))
            .field("sections", &self.sections)
            .field("partial_fields", &self.partial_fields)
            .field("columns_complete", &self.columns_complete)
            .field("layout_gen", &self.layout_gen)
            .finish()
    }
}

impl FileBloomProbe {
    /// Open the sidecar footer and resolve the bloom blob's section table
    /// (empty, with [`Self::has_blob`] false, when the sidecar carries no
    /// bloom blob — the `fields` table still answers presence). Blocking
    /// work (ranged fetches through [`block_fetch`]): call from a blocking
    /// thread.
    pub fn open_sidecar(source: Arc<dyn VixRangeSource>) -> Result<Self> {
        let container = parse_container_ranged(&source)?;
        require_supported_index_format(&container.properties)?;
        let partial_fields: HashSet<String> = match container.properties.get(PROP_PARTIAL_FIELDS) {
            Some(raw) => serde_json::from_str(raw).map_err(|e| {
                VixError::Malformed(format!("invalid {PROP_PARTIAL_FIELDS} property: {e}"))
            })?,
            None => HashSet::new(),
        };
        // DESIGN-COLD-MISS §2 S5/B2: the `fields` table is the file's
        // field-presence declaration — stage 1b drops a field absent here
        // without any blob read (when the license below holds).
        let fields_raw = container
            .properties
            .get(PROP_FIELDS)
            .ok_or_else(|| VixError::Malformed(format!("missing file property {PROP_FIELDS:?}")))?;
        let field_entries: Vec<FieldEntry> = serde_json::from_str(fields_raw)?;
        let columns_complete = container
            .properties
            .get(PROP_COLUMNS_COMPLETE)
            .is_some_and(|raw| raw == "true");
        let layout_gen = container
            .properties
            .get(PROP_LAYOUT_GEN)
            .and_then(|raw| raw.parse().ok())
            .unwrap_or(0);
        let Some(blob) = container.bloom else {
            return Ok(Self {
                blob: None,
                sections: Vec::new(),
                partial_fields,
                field_entries,
                columns_complete,
                layout_gen,
            });
        };
        // DESIGN-COLD-MISS §2 S4: prefer the footer's section table when the
        // writer stamped it; otherwise walk the blob's section headers
        // (pre-property sidecars).
        let sections = match container.properties.get(PROP_BLOOM_SECTIONS) {
            Some(raw) => parse_section_property(raw, blob.len())?,
            None => parse_sections(&blob)?,
        };
        Ok(Self {
            blob: Some(blob),
            sections,
            partial_fields,
            field_entries,
            columns_complete,
            layout_gen,
        })
    }

    /// Whether the sidecar carries a per-file bloom blob at all.
    pub fn has_blob(&self) -> bool {
        self.blob.is_some()
    }

    /// Whether [`Self::field_entries`] is the file's AUTHORITATIVE column
    /// set (the data object's `columns_complete` license, stamped on the
    /// sidecar). Without it a field missing from the table is "no info".
    pub fn columns_complete(&self) -> bool {
        self.columns_complete
    }

    /// B2 stage 1b: `Some(false)` when the file PROVABLY lacks `field`
    /// (licensed column set, field not in it) — a predicate on it matches no
    /// row; `Some(true)` when present; `None` when the table is not
    /// authoritative.
    pub fn has_field(&self, field: &str) -> Option<bool> {
        if !self.columns_complete {
            return None;
        }
        Some(self.field_entries.iter().any(|entry| entry.name == field))
    }

    /// Fields whose value terms are incomplete in this file: their per-field
    /// section (if any) must not be trusted.
    pub fn partial_fields(&self) -> &HashSet<String> {
        &self.partial_fields
    }

    /// The sidecar's `fields` table — every docs column of this file
    /// (DESIGN-COLD-MISS §2 S5).
    pub fn field_entries(&self) -> &[FieldEntry] {
        &self.field_entries
    }

    /// The sidecar `layout_gen` property (`0` when absent) — see
    /// [`crate::VixReader::layout_gen`].
    pub fn layout_gen(&self) -> u32 {
        self.layout_gen
    }

    /// The term fields this sidecar marks with a per-field SBBF section
    /// (`"bf"`, DESIGN-COLD-MISS §2 S5).
    pub fn bloomed_fields(&self) -> impl Iterator<Item = &str> + '_ {
        self.field_entries
            .iter()
            .filter(|entry| entry.types.is_bloomed())
            .map(|entry| entry.name.as_str())
    }

    /// The WHOLE bloom blob in ONE ranged read (`None` when the sidecar
    /// carries no bloom blob). Consumers that need every section body — the
    /// `.bf` assembler's per-file load — use this instead of one read per
    /// section.
    pub fn read_file_blooms(&self) -> Result<Option<Vec<crate::bloom::FileBloom>>> {
        let Some(blob) = &self.blob else {
            return Ok(None);
        };
        let bytes = blob.bytes()?;
        Ok(Some(crate::bloom::parse_file_blooms(&bytes)?))
    }

    /// Section table in blob order.
    pub fn sections(&self) -> &[FileBloomSection] {
        &self.sections
    }

    /// Heap bytes retained by this view (for caches that account by size):
    /// the section table plus whatever the blob handle pins — an in-memory
    /// blob slice, or the eager-tail copy a boundary-crossing blob keeps.
    pub fn retained_bytes(&self) -> usize {
        let handle = match &self.blob {
            Some(BlobHandle::Mem(bytes)) => bytes.len(),
            Some(BlobHandle::Ranged(ranged)) => ranged.source.retained_bytes(),
            None => 0,
        };
        std::mem::size_of::<Self>()
            + handle
            + self
                .sections
                .iter()
                .map(|s| s.field.capacity() + std::mem::size_of::<FileBloomSection>())
                .sum::<usize>()
            + self
                .partial_fields
                .iter()
                .map(|f| f.capacity() + std::mem::size_of::<String>())
                .sum::<usize>()
            + self
                .field_entries
                .iter()
                .map(|entry| entry.name.capacity() + std::mem::size_of::<FieldEntry>())
                .sum::<usize>()
    }

    /// Whether `field` has an authoritative per-field section here.
    fn per_field_section(&self, field: &str) -> Option<&FileBloomSection> {
        if self.partial_fields.contains(field) {
            return None;
        }
        self.sections.iter().find(|s| s.field == field)
    }

    /// Probe `values` of `field`.
    ///
    /// `Ok(Some(hits))` — one "maybe" verdict per value from an authoritative
    /// filter: the field's own section, or (when `composite_fallback` allows
    /// it) the composite section with all of the field's guard probes
    /// hitting. `Ok(None)` — the file carries no usable filter for the field;
    /// the caller must keep it. At most one batched block fetch.
    pub fn probe(
        &self,
        field: &str,
        values: &[&[u8]],
        composite_fallback: bool,
    ) -> Result<Option<Vec<bool>>> {
        if values.is_empty() {
            return Ok(Some(Vec::new()));
        }
        if let Some(section) = self.per_field_section(field) {
            let plan: Vec<(&FileBloomSection, u64)> =
                values.iter().map(|v| (section, hash_value(v))).collect();
            return self.check_blocks(&plan).map(Some);
        }
        if !composite_fallback {
            return Ok(None);
        }
        let Some(composite) = self
            .sections
            .iter()
            .find(|s| s.field == COMPOSITE_BLOOM_FIELD)
        else {
            return Ok(None);
        };
        let mut buf = Vec::new();
        let mut plan: Vec<(&FileBloomSection, u64)> =
            Vec::with_capacity(COMPOSITE_GUARD_PROBES as usize + values.len());
        for probe in 0..COMPOSITE_GUARD_PROBES {
            let Some(key) = composite_guard_key(field, probe, &mut buf) else {
                return Ok(None); // field name overflows the key prefix: never covered
            };
            plan.push((composite, hash_value(key)));
        }
        for value in values {
            let Some(key) = composite_value_key(field, value, &mut buf) else {
                return Ok(None);
            };
            plan.push((composite, hash_value(key)));
        }
        let mut hits = self.check_blocks(&plan)?;
        let values_hits = hits.split_off(COMPOSITE_GUARD_PROBES as usize);
        if !hits.iter().all(|guard| *guard) {
            // the composite does not claim this field for this file
            return Ok(None);
        }
        Ok(Some(values_hits))
    }

    /// One batched fetch of the blocks addressed by `plan`, then the SBBF
    /// point check per entry.
    fn check_blocks(&self, plan: &[(&FileBloomSection, u64)]) -> Result<Vec<bool>> {
        let ranges: Vec<Range<u64>> = plan
            .iter()
            .map(|(section, hash)| {
                let start = section.body.start
                    + u64::from(block_index(*hash, section.num_blocks)) * BLOCK_BYTES as u64;
                start..start + BLOCK_BYTES as u64
            })
            .collect();
        let Some(blob) = &self.blob else {
            return Err(VixError::Malformed(
                "bloom block probe on a sidecar without a bloom blob".to_string(),
            ));
        };
        let blocks = read_many(blob, ranges)?;
        Ok(blocks
            .iter()
            .zip(plan)
            .map(|(bytes, (_, hash))| {
                let block: &[u8; BLOCK_BYTES] = bytes[..]
                    .try_into()
                    .expect("fetched exactly one SBBF block");
                check_block(block, *hash)
            })
            .collect())
    }
}

/// Bytes `range` of the blob (blob-relative).
fn read(blob: &BlobHandle, range: Range<u64>) -> Result<Bytes> {
    match blob {
        BlobHandle::Mem(bytes) => Ok(bytes.slice(range.start as usize..range.end as usize)),
        BlobHandle::Ranged(ranged) => block_fetch(
            ranged.source.as_ref(),
            ranged.range.start + range.start..ranged.range.start + range.end,
        ),
    }
}

/// Several blob-relative ranges in one round trip where the source batches.
fn read_many(blob: &BlobHandle, ranges: Vec<Range<u64>>) -> Result<Vec<Bytes>> {
    match blob {
        BlobHandle::Mem(bytes) => Ok(ranges
            .into_iter()
            .map(|r| bytes.slice(r.start as usize..r.end as usize))
            .collect()),
        BlobHandle::Ranged(ranged) => block_fetch_many(
            ranged.source.as_ref(),
            ranges
                .into_iter()
                .map(|r| ranged.range.start + r.start..ranged.range.start + r.end)
                .collect(),
        ),
    }
}

/// DESIGN-COLD-MISS §2 S4: build the section table from the
/// `bloom_sections_v1` footer property — `[name, body_offset_in_blob,
/// num_blocks, n_items]` per section, in blob order. Validated against the
/// blob length so a corrupt property fails the open rather than producing a
/// block range past the blob.
fn parse_section_property(raw: &str, blob_len: u64) -> Result<Vec<FileBloomSection>> {
    let malformed = |msg: String| VixError::Malformed(format!("bloom_sections_v1: {msg}"));
    let entries: Vec<(String, u64, u32, u32)> =
        serde_json::from_str(raw).map_err(|e| malformed(format!("not a section table: {e}")))?;
    let mut sections = Vec::with_capacity(entries.len());
    for (field, offset, num_blocks, n_items) in entries {
        if num_blocks == 0 {
            return Err(malformed(format!("zero num_blocks for field {field:?}")));
        }
        let end = offset
            .checked_add(u64::from(num_blocks) * BLOCK_BYTES as u64)
            .ok_or_else(|| malformed(format!("body range overflow for field {field:?}")))?;
        if end > blob_len {
            return Err(malformed(format!(
                "body range {offset}..{end} of field {field:?} exceeds the blob ({blob_len} bytes)"
            )));
        }
        sections.push(FileBloomSection {
            field,
            num_blocks,
            n_items,
            body: offset..end,
        });
    }
    Ok(sections)
}

/// Walk the section headers of the blob (same layout as
/// [`crate::bloom::parse_file_blooms`]), fetching header windows on demand and
/// skipping every body.
fn parse_sections(blob: &BlobHandle) -> Result<Vec<FileBloomSection>> {
    let malformed = |msg: &str| VixError::Malformed(format!("bloom blob: {msg}"));
    let len = blob.len();
    let mut cursor = HeaderCursor::new(blob)?;
    let mut pos = 0u64;
    if cursor.take(pos, 4)? != FILE_BLOOM_MAGIC {
        return Err(malformed("bad magic"));
    }
    pos += 4;
    let version = cursor.take(pos, 1)?[0];
    if version != FILE_BLOOM_VERSION {
        return Err(malformed(&format!("unsupported version {version}")));
    }
    pos += 1;
    let field_count = u32::from_le_bytes(cursor.take(pos, 4)?.try_into().unwrap()) as u64;
    pos += 4;
    // `field_count` is file data: bound it by what the remaining bytes can
    // hold before sizing anything from it.
    const MIN_FIELD_BYTES: u64 = 2 + 1 + 4 + 4 + BLOCK_BYTES as u64;
    let max_fields = (len - pos) / MIN_FIELD_BYTES;
    if field_count > max_fields {
        return Err(malformed(&format!(
            "field_count {field_count} exceeds the {max_fields} fields the remaining {} bytes can hold",
            len - pos
        )));
    }
    let mut sections = Vec::with_capacity(field_count as usize);
    for _ in 0..field_count {
        let name_len = u16::from_le_bytes(cursor.take(pos, 2)?.try_into().unwrap()) as usize;
        pos += 2;
        let field = std::str::from_utf8(cursor.take(pos, name_len)?)
            .map_err(|_| malformed("field name not utf-8"))?
            .to_string();
        pos += name_len as u64;
        let algo = cursor.take(pos, 1)?[0];
        if algo != FILE_BLOOM_ALGO_SBBF_GXHASH {
            return Err(malformed(&format!("unsupported algo {algo}")));
        }
        pos += 1;
        let num_blocks = u32::from_le_bytes(cursor.take(pos, 4)?.try_into().unwrap());
        if num_blocks == 0 {
            return Err(malformed("zero num_blocks"));
        }
        pos += 4;
        let n_items = u32::from_le_bytes(cursor.take(pos, 4)?.try_into().unwrap());
        pos += 4;
        let body_end = pos
            .checked_add(u64::from(num_blocks) * BLOCK_BYTES as u64)
            .filter(|end| *end <= len)
            .ok_or_else(|| malformed("truncated"))?;
        sections.push(FileBloomSection {
            field,
            num_blocks,
            n_items,
            body: pos..body_end,
        });
        pos = body_end;
    }
    if pos != len {
        return Err(malformed("trailing bytes"));
    }
    Ok(sections)
}

/// Sliding header window over the blob: bodies are skipped by arithmetic,
/// only the bytes a header needs are fetched.
struct HeaderCursor<'a> {
    blob: &'a BlobHandle,
    len: u64,
    window_start: u64,
    window: Bytes,
}

impl<'a> HeaderCursor<'a> {
    fn new(blob: &'a BlobHandle) -> Result<Self> {
        let len = blob.len();
        let window = read(blob, 0..len.min(HEADER_WINDOW))?;
        Ok(Self {
            blob,
            len,
            window_start: 0,
            window,
        })
    }

    /// Bytes `[pos, pos + n)`, refetching a window when the current one does
    /// not cover them.
    fn take(&mut self, pos: u64, n: usize) -> Result<&[u8]> {
        let end = pos
            .checked_add(n as u64)
            .filter(|end| *end <= self.len)
            .ok_or_else(|| VixError::Malformed("bloom blob: truncated".to_string()))?;
        let window_end = self.window_start + self.window.len() as u64;
        if pos < self.window_start || end > window_end {
            let fetch_end = pos
                .saturating_add((n as u64).max(HEADER_WINDOW))
                .min(self.len);
            self.window = read(self.blob, pos..fetch_end)?;
            self.window_start = pos;
        }
        let start = (pos - self.window_start) as usize;
        Ok(&self.window[start..start + n])
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };

    use super::*;
    use crate::{
        BytesRangeSource, VixWriter, VixWriterOptions,
        bloom::{FileBloom, parse_file_blooms, serialize_file_blooms},
        sbbf::Sbbf,
        source::RangedBlob,
    };

    /// A real `.vix` pair whose sidecar carries a per-file bloom blob with
    /// `trace_id` demoted to bloom-only (the prod shape) and `svc` as a
    /// term field.
    fn build_pair(trace_ids: &[&str]) -> (Vec<u8>, Vec<u8>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("trace_id", DataType::Utf8, true),
            Field::new("svc", DataType::Utf8, true),
        ]));
        let n = trace_ids.len();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(
                    (0..n as i64).map(|i| 1_000 + i),
                )),
                Arc::new(StringArray::from(trace_ids.to_vec())),
                Arc::new(StringArray::from(vec!["api"; n])),
            ],
        )
        .unwrap();
        let source = StringArray::from_iter_values(
            trace_ids
                .iter()
                .map(|v| format!("{{\"trace_id\":\"{v}\",\"svc\":\"api\"}}")),
        );
        let opts = VixWriterOptions {
            bloom_only_field_names: vec!["trace_id".to_string()],
            ..Default::default()
        };
        let mut writer = VixWriter::new(&schema, opts, false);
        writer
            .push_batch_with_source(&batch, &source, None)
            .unwrap();
        let (data, index) = writer.finish().unwrap();
        (data, index.expect("indexed build emits a sidecar"))
    }

    fn sidecar_source(index: Vec<u8>) -> Arc<dyn VixRangeSource> {
        BytesRangeSource::new("sidecar", Bytes::from(index))
    }

    #[test]
    fn probe_matches_whole_blob_verdicts_over_a_real_sidecar() {
        let ids: Vec<String> = (0..2_000).map(|i| format!("trace-{i:05}")).collect();
        let refs: Vec<&str> = ids.iter().map(String::as_str).collect();
        let (_, index) = build_pair(&refs);
        let probe = FileBloomProbe::open_sidecar(sidecar_source(index.clone())).unwrap();
        assert!(probe.has_blob(), "bloom blob present");
        assert!(
            probe
                .sections()
                .iter()
                .any(|s| s.field == COMPOSITE_BLOOM_FIELD),
            "prod shape: bloom-only values land in the composite section, got {:?}",
            probe.sections()
        );

        // every inserted value is a hit through the composite fallback
        let present: Vec<&[u8]> = refs.iter().take(64).map(|s| s.as_bytes()).collect();
        let hits = probe.probe("trace_id", &present, true).unwrap().unwrap();
        assert!(hits.iter().all(|h| *h), "inserted values must never miss");

        // absent values: mostly misses, never more than the FPP allows
        let absent: Vec<String> = (0..1_000).map(|i| format!("absent-{i:05}")).collect();
        let absent_refs: Vec<&[u8]> = absent.iter().map(|s| s.as_bytes()).collect();
        let hits = probe
            .probe("trace_id", &absent_refs, true)
            .unwrap()
            .unwrap();
        let false_positives = hits.iter().filter(|h| **h).count();
        assert!(
            false_positives < 20,
            "{false_positives} false positives of 1,000"
        );

        // the composite is off-limits when policy forbids the fallback and
        // there is no per-field section
        assert_eq!(probe.probe("trace_id", &present, false).unwrap(), None);
        // a field the writer never covered (no guards) reads as no info
        assert_eq!(probe.probe("no_such_field", &present, true).unwrap(), None);
    }

    #[test]
    fn ranged_header_walk_matches_the_in_memory_parser() {
        // several sections with names longer than one header window forces
        // the cursor to refetch across section boundaries
        let mut blooms = Vec::new();
        for i in 0..5u32 {
            let mut sbbf = Sbbf::new_with_num_blocks(1 << (4 + i));
            for v in 0..(64 << i) {
                sbbf.insert(format!("v{i}-{v}").as_bytes());
            }
            blooms.push(FileBloom {
                field: format!("field-{}-{}", i, "x".repeat(HEADER_WINDOW as usize / 2)),
                num_blocks: 1 << (4 + i),
                n_items: 64 << i,
                bytes: sbbf.to_bytes(),
            });
        }
        let blob = serialize_file_blooms(&blooms).unwrap();
        let expected = parse_file_blooms(&blob).unwrap();
        // embed the blob at an offset inside a larger object to exercise the
        // absolute/relative range mapping
        let mut object = vec![0xAAu8; 777];
        object.extend_from_slice(&blob);
        object.extend_from_slice(&[0x55u8; 333]);
        let source = BytesRangeSource::new("object", Bytes::from(object));
        let handle = BlobHandle::Ranged(RangedBlob::new(source, 777..777 + blob.len() as u64));
        let sections = parse_sections(&handle).unwrap();
        assert_eq!(sections.len(), expected.len());
        for (section, bloom) in sections.iter().zip(&expected) {
            assert_eq!(section.field, bloom.field);
            assert_eq!(section.num_blocks, bloom.num_blocks);
            assert_eq!(section.n_items, bloom.n_items);
            assert_eq!(
                (section.body.end - section.body.start) as usize,
                bloom.bytes.len()
            );
        }
        // block reads through the ranged handle agree with the whole-blob probe
        let probe = FileBloomProbe {
            blob: Some(handle),
            sections,
            partial_fields: HashSet::new(),
            field_entries: Vec::new(),
            columns_complete: false,
            layout_gen: 0,
        };
        for (i, bloom) in expected.iter().enumerate() {
            let value = format!("v{i}-7");
            let hits = probe
                .probe(&bloom.field, &[value.as_bytes()], false)
                .unwrap()
                .unwrap();
            assert_eq!(hits, vec![true], "inserted value of section {i}");
            let absent = format!("absent-{i}");
            let hash = hash_value(absent.as_bytes());
            let bi = block_index(hash, bloom.num_blocks) as usize;
            let block: &[u8; BLOCK_BYTES] = bloom.bytes[bi * BLOCK_BYTES..(bi + 1) * BLOCK_BYTES]
                .try_into()
                .unwrap();
            let oracle = check_block(block, hash);
            let hits = probe
                .probe(&bloom.field, &[absent.as_bytes()], false)
                .unwrap()
                .unwrap();
            assert_eq!(
                hits,
                vec![oracle],
                "section {i} disagrees with the whole-blob check"
            );
        }
    }

    #[test]
    fn partial_field_section_is_not_trusted() {
        let ids: Vec<String> = (0..256).map(|i| format!("trace-{i:04}")).collect();
        let refs: Vec<&str> = ids.iter().map(String::as_str).collect();
        let (_, index) = build_pair(&refs);
        let probe = FileBloomProbe::open_sidecar(sidecar_source(index)).unwrap();
        assert!(probe.has_blob());
        let composite = probe
            .sections()
            .iter()
            .find(|s| s.field == COMPOSITE_BLOOM_FIELD)
            .unwrap()
            .clone();
        // a synthetic per-field section for a field flagged partial: the
        // probe must ignore it (and fall through to composite policy)
        let mut sections = probe.sections().to_vec();
        sections.push(FileBloomSection {
            field: "partial".to_string(),
            num_blocks: composite.num_blocks,
            n_items: composite.n_items,
            body: composite.body.clone(),
        });
        let untrusted = FileBloomProbe {
            blob: probe.blob,
            sections,
            partial_fields: HashSet::from(["partial".to_string()]),
            field_entries: Vec::new(),
            columns_complete: false,
            layout_gen: 0,
        };
        assert_eq!(
            untrusted.probe("partial", &[b"anything"], false).unwrap(),
            None
        );
        // not partial: the same section answers
        let trusted = FileBloomProbe {
            partial_fields: HashSet::new(),
            ..untrusted
        };
        assert!(
            trusted
                .probe("partial", &[b"anything"], false)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn sidecar_without_bloom_blob_reads_as_none() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("svc", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1_000, 1_001])),
                Arc::new(StringArray::from(vec!["api", "web"])),
            ],
        )
        .unwrap();
        let source = StringArray::from_iter_values(
            ["api", "web"]
                .iter()
                .map(|v| format!("{{\"svc\":\"{v}\"}}")),
        );
        let opts = VixWriterOptions {
            bloom_field_names: Vec::new(),
            bloom_only_field_names: Vec::new(),
            bloom_composite: false,
            ..Default::default()
        };
        let mut writer = VixWriter::new(&schema, opts, false);
        writer
            .push_batch_with_source(&batch, &source, None)
            .unwrap();
        let (_, index) = writer.finish().unwrap();
        let index = index.expect("sidecar");
        let probe = FileBloomProbe::open_sidecar(sidecar_source(index)).unwrap();
        assert!(!probe.has_blob());
        // no blob: every value probe is "no info" ...
        let values: [&[u8]; 1] = [b"x"];
        assert_eq!(probe.probe("svc", &values, true).unwrap(), None);
        assert_eq!(probe.read_file_blooms().unwrap(), None);
        // ... while the `fields` table still answers presence (the default
        // writer options carry no columns_complete license: no info)
        assert!(probe.field_entries().iter().any(|e| e.name == "svc"));
        assert_eq!(probe.has_field("svc"), None);
        assert_eq!(probe.has_field("nope"), None);
    }
}
