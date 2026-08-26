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

//! Hidden test-only helpers for downstream crates' tests. NOT a stable API —
//! production code must never call into this module.
//!
//! Since format v3 a core file is a `.vix` DATA object plus an optional
//! `.vxi` INDEX sidecar. Property-editing helpers here operate on ONE
//! object's bytes — pass the object that owns the property (`zone_map` /
//! `row_order` live on the data object; `tokenizer` / `fields` /
//! `partial_fields` live on the sidecar).

use bytes::Bytes;

use crate::container::{
    BLOB_TAG_BLOOM, BLOB_TAG_DICT, BLOB_TAG_DICT_BLOCKS, BLOB_TAG_DOCS, BLOB_TAG_PLIST,
    BLOB_TAG_TERMS, BLOB_TYPE_BLOOM, BLOB_TYPE_DICT, BLOB_TYPE_DICT_BLOCKS, BLOB_TYPE_DOCS,
    BLOB_TYPE_PLIST, BLOB_TYPE_TERMS, BlobHandle, FIELD_TYPE_FTS, FIELD_TYPE_TERM, FieldEntry,
    PROP_FIELDS, PROP_PARTIAL_FIELDS, PROP_ROW_ORDER, PROP_TOKENIZER, PROP_ZONE_MAP,
    build_container, parse_container,
};

/// Re-pack ONE object's container with its properties transformed by
/// `edit`, preserving every recognized blob byte-for-byte (docs for data
/// objects; dict/dict_blocks/terms/plist/bloom for sidecars).
pub fn repack_properties(
    data: &[u8],
    edit: impl FnOnce(&mut Vec<(String, String)>) -> anyhow::Result<()>,
) -> anyhow::Result<Vec<u8>> {
    let data = Bytes::copy_from_slice(data);
    let container = parse_container(&data)?;
    let mut properties: Vec<(String, String)> = container
        .properties
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    edit(&mut properties)?;

    let blob_bytes = |handle: Option<BlobHandle>| match handle {
        Some(BlobHandle::Mem(bytes)) => Some(bytes.to_vec()),
        Some(BlobHandle::Ranged(_)) => unreachable!("parsed from memory"),
        None => None,
    };
    let mut blobs: Vec<(&'static str, &'static str, Vec<u8>)> = Vec::new();
    if let Some(docs) = blob_bytes(container.docs) {
        blobs.push((BLOB_TYPE_DOCS, BLOB_TAG_DOCS, docs));
    }
    if let Some(terms) = blob_bytes(container.terms) {
        blobs.push((BLOB_TYPE_TERMS, BLOB_TAG_TERMS, terms));
    }
    if let Some(plist) = blob_bytes(container.plist) {
        blobs.push((BLOB_TYPE_PLIST, BLOB_TAG_PLIST, plist));
    }
    if let Some(blocks) = blob_bytes(container.dict_blocks) {
        blobs.push((BLOB_TYPE_DICT_BLOCKS, BLOB_TAG_DICT_BLOCKS, blocks));
    }
    if let Some(dict) = blob_bytes(container.dict) {
        blobs.push((BLOB_TYPE_DICT, BLOB_TAG_DICT, dict));
    }
    if let Some(bloom) = blob_bytes(container.bloom) {
        blobs.push((BLOB_TYPE_BLOOM, BLOB_TAG_BLOOM, bloom));
    }
    Ok(build_container(properties, blobs)?)
}

/// Re-pack ONE object with the named property removed — the generic
/// fabricator for "written before property X existed" shapes.
pub fn strip_property_for_tests(data: &[u8], property: &str) -> anyhow::Result<Vec<u8>> {
    let property = property.to_string();
    repack_properties(data, move |properties| {
        properties.retain(|(key, _)| *key != property);
        Ok(())
    })
}

/// Re-pack a DATA object with its `zone_map` property removed — simulates a
/// file written before the per-chunk `_timestamp` zone table landed, so
/// cross-crate tests can exercise the decode path (and prove it agrees with
/// the zone-map path) without any legacy writer code.
pub fn strip_zone_map_property(data: &[u8]) -> anyhow::Result<Vec<u8>> {
    repack_properties(data, |properties| {
        properties.retain(|(key, _)| key.as_str() != PROP_ZONE_MAP);
        Ok(())
    })
}

/// The `row_order` puffin property of a DATA object (#51c-c), for asserting
/// what a writer stamped without going through a reader.
pub fn row_order_property(data: &[u8]) -> anyhow::Result<Option<String>> {
    let data = Bytes::copy_from_slice(data);
    let container = parse_container(&data)?;
    Ok(container.properties.get(PROP_ROW_ORDER).cloned())
}

/// Any puffin property of a DATA object by name, for asserting what a
/// writer stamped without going through a reader (§4 `row_regions` etc.).
pub fn data_property(data: &[u8], name: &str) -> anyhow::Result<Option<String>> {
    let data = Bytes::copy_from_slice(data);
    let container = parse_container(&data)?;
    Ok(container.properties.get(name).cloned())
}

/// Re-pack a DATA object with its `row_order` property removed — simulates a
/// file written before #51c-c landed, so tests can pin the historical
/// contract (missing property == sorted `ts_desc`; such files must read
/// exactly as before).
pub fn strip_row_order_property(data: &[u8]) -> anyhow::Result<Vec<u8>> {
    repack_properties(data, |properties| {
        properties.retain(|(key, _)| key.as_str() != PROP_ROW_ORDER);
        Ok(())
    })
}

/// The byte range one tagged blob occupies inside ONE object's container —
/// for tests that corrupt a SPECIFIC blob's bytes without assuming any blob
/// order. Pass the object that holds the blob (`docs` lives in the data
/// object; `dict`/`terms`/`bloom`/`plist` live in the sidecar).
pub fn blob_byte_range(data: &[u8], tag: &str) -> anyhow::Result<std::ops::Range<usize>> {
    let meta = puffin::reader::parse_puffin_footer_from_bytes(data)?;
    let blob = meta
        .blobs
        .iter()
        .find(|blob| blob.properties.get("blob_tag").map(String::as_str) == Some(tag))
        .ok_or_else(|| anyhow::anyhow!("no blob tagged {tag:?}"))?;
    let start = usize::try_from(blob.offset)?;
    let length = usize::try_from(blob.length)?;
    Ok(start..start + length)
}

/// The `tokenizer` puffin property of an INDEX sidecar, for asserting what a
/// writer stamped without going through a reader.
pub fn tokenizer_property(index: &[u8]) -> anyhow::Result<Option<String>> {
    let index = Bytes::copy_from_slice(index);
    let container = parse_container(&index)?;
    Ok(container.properties.get(PROP_TOKENIZER).cloned())
}

/// Re-pack an INDEX sidecar with its `tokenizer` property replaced —
/// simulates sidecars written by older writers (e.g. the pre-fix `"o2-v1"`
/// tokenizer) so cross-crate tests can exercise the mismatch-forces-rebuild
/// convergence without any legacy writer code.
pub fn repack_with_tokenizer_property(index: &[u8], tokenizer: &str) -> anyhow::Result<Vec<u8>> {
    repack_properties(index, |properties| {
        match properties.iter_mut().find(|(key, _)| key == PROP_TOKENIZER) {
            Some(entry) => entry.1 = tokenizer.to_string(),
            None => properties.push((PROP_TOKENIZER.to_string(), tokenizer.to_string())),
        }
        Ok(())
    })
}

/// Finish a [`crate::VixWriter`] WITHOUT the degenerate-`_timestamp` finish
/// guard — fabricates files of the pre-guard era whose stored rows carry
/// `_timestamp <= 0` (the poison population compaction-time cleansing
/// drops), so downstream merge/move tests can build poisoned inputs without
/// any legacy writer code. Returns `(data_bytes, sidecar_bytes)` like
/// [`crate::VixWriter::finish`]. Production code must never construct such
/// files: every real producer finishes through the guarded
/// [`crate::VixWriter::finish`]/[`crate::VixWriter::finish_with_stats`].
pub fn finish_ignoring_timestamp_guard(
    writer: crate::VixWriter,
) -> anyhow::Result<(Vec<u8>, Option<Vec<u8>>)> {
    let (data, index, _) = writer.finish_unguarded()?;
    Ok((data, index))
}

/// Re-pack an INDEX sidecar with one field's `term`/`fts` capability DROPPED
/// from its `fields`-property entry (the entry keeps its positional
/// field-id slot, exactly like a merge-demoted field) — simulates sidecars
/// written before a value-term capability existed (e.g. pre-numeric-value-
/// terms files) or fast-path merge outputs that demoted the field, so
/// downstream tests can prove such files are detected as "carrying values
/// without value terms" and healed by a rebuild, without any legacy writer
/// code. The current writer always claims capability for planned fields.
pub fn repack_dropping_field_term_capability(index: &[u8], field: &str) -> anyhow::Result<Vec<u8>> {
    repack_properties(index, |properties| {
        let entry = properties
            .iter_mut()
            .find(|(key, _)| key == PROP_FIELDS)
            .ok_or_else(|| anyhow::anyhow!("sidecar has no {PROP_FIELDS:?} property"))?;
        let mut fields: Vec<FieldEntry> = serde_json::from_str(&entry.1)?;
        let target = fields
            .iter_mut()
            .find(|candidate| candidate.name == field)
            .ok_or_else(|| anyhow::anyhow!("field {field:?} has no fields-table entry"))?;
        let before = target.types.len();
        target
            .types
            .retain(|ty| ty != FIELD_TYPE_TERM && ty != FIELD_TYPE_FTS);
        if target.types.len() == before {
            anyhow::bail!("field {field:?} claims no term/fts capability to drop");
        }
        entry.1 = serde_json::to_string(&fields)?;
        Ok(())
    })
}

/// Re-pack an INDEX sidecar with its `partial_fields` property replaced —
/// simulates sidecars written before fts values tokenized unconditionally
/// (the pre-fix writer skipped oversize fts values and tainted the field),
/// so merge tests can prove such inputs force a healing rebuild without any
/// legacy writer code. The current writer never marks an fts field partial.
pub fn repack_with_partial_fields(
    index: &[u8],
    partial_fields: &[&str],
) -> anyhow::Result<Vec<u8>> {
    let encoded = serde_json::to_string(&partial_fields)?;
    repack_properties(index, |properties| {
        match properties
            .iter_mut()
            .find(|(key, _)| key == PROP_PARTIAL_FIELDS)
        {
            Some(entry) => entry.1 = encoded,
            None => properties.push((PROP_PARTIAL_FIELDS.to_string(), encoded)),
        }
        Ok(())
    })
}
