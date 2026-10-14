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

//! DESIGN-COLD-MISS §5 — the sidecar REPACK: re-emit an old-layout `.vxi`
//! (`layout_gen < 2`) in the current tail format WITHOUT re-deriving its
//! index. Every index blob is copied verbatim (one read each, no term
//! re-extraction, no docs decode); the new container gains what the current
//! writer stamps — `layout_gen`, `data_footer_v1`, `bloom_sections_v1`,
//! `columns_complete`, the `bf` field flags — stores the dict block index
//! zstd when the tail is compressed, and adds the W1 AUTO additive per-field
//! blooms from ONE dictionary walk (the same selection rule as a fresh
//! write, on exact distinct counts). Publication is the caller's (a new
//! immutable index generation, like the sidecar-only heal).

use std::{collections::BTreeMap, sync::Arc};

use crate::{
    VixRangeSource, VixReader,
    bloom::{
        AutoBloomPolicy, BloomHashAcc, COMPOSITE_BLOOM_FIELD, FileBloom, parse_file_blooms,
        serialize_file_blooms_with_sections,
    },
    container::{
        BLOB_TAG_BLOOM, BLOB_TAG_DICT, BLOB_TAG_DICT_BLOCKS, BLOB_TAG_PLIST, BLOB_TAG_TERMS,
        BLOB_TYPE_BLOOM, BLOB_TYPE_DICT, BLOB_TYPE_DICT_BLOCKS, BLOB_TYPE_PLIST, BLOB_TYPE_TERMS,
        BlobPart, FieldEntry, FieldTypeFlags, LAYOUT_GEN_PLAIN, LAYOUT_GEN_ZSTD,
        PROP_BLOOM_SECTIONS, PROP_COLUMNS_COMPLETE, PROP_DATA_FOOTER, PROP_FIELDS, PROP_LAYOUT_GEN,
        build_container_parts, parse_container_ranged, read_data_footer_payload,
        require_supported_index_format,
    },
    error::VixError,
    reader::DICT_RESIDENT_MAX_BYTES,
};

/// What a repack writes (the writer options the current producers use).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SidecarRepackOptions {
    /// S1/S2: zstd footer payload + zstd `dict` block index (`layout_gen=2`).
    pub sidecar_tail_zstd: bool,
    /// W1 AUTO additive per-field blooms (disabled policy = none added).
    pub auto_bloom: AutoBloomPolicy,
    /// False-positive rate of the added sections.
    pub bloom_fpp: f64,
}

/// A finished repack: the new sidecar bytes plus what changed, for the log.
#[derive(Debug)]
pub struct SidecarRepack {
    pub sidecar: Vec<u8>,
    pub row_count: u64,
    pub term_count: u64,
    /// `layout_gen` of the input.
    pub from_layout_gen: u32,
    /// Per-field sections added by the AUTO selection (`(field, distinct)`).
    pub added_sections: Vec<(String, u64)>,
}

/// The sidecar `layout_gen` the current writer stamps for `opts`.
pub fn target_layout_gen(opts: &SidecarRepackOptions) -> u32 {
    if opts.sidecar_tail_zstd { 2 } else { 1 }
}

/// Repack the sidecar of the `(data, index)` pair. Blocks on fetches — call
/// from a blocking thread.
pub fn repack_sidecar(
    data: Arc<dyn VixRangeSource>,
    index: Arc<dyn VixRangeSource>,
    opts: &SidecarRepackOptions,
) -> anyhow::Result<SidecarRepack> {
    let reader = VixReader::open_ranged_with_index(Arc::clone(&data), Some(Arc::clone(&index)))?;
    let from_layout_gen = reader.layout_gen();
    // the sidecar's own envelope: properties + blob handles, copied verbatim
    let container = parse_container_ranged(&index)?;
    require_supported_index_format(&container.properties)?;
    let mut properties: BTreeMap<String, String> = container.properties.clone();

    // --- blobs, byte for byte ---
    let dict = match (container.dict, container.dict_codec) {
        (None, _) => None,
        (Some(handle), None) => Some(handle.bytes()?),
        // the index is re-encoded (or stored raw) below from its decoded form
        (Some(handle), Some(puffin::CompressionCodec::Zstd)) => {
            let compressed = handle.bytes()?;
            let decoded_len = zstd::zstd_safe::get_frame_content_size(&compressed)
                .map_err(|e| VixError::Malformed(format!("dict index zstd frame header: {e}")))?
                .ok_or_else(|| {
                    VixError::Malformed("dict index zstd frame lacks a content size".to_string())
                })?;
            if decoded_len > DICT_RESIDENT_MAX_BYTES {
                return Err(VixError::Malformed(format!(
                    "compressed dict index advertises {decoded_len} decoded bytes"
                ))
                .into());
            }
            Some(bytes::Bytes::from(
                zstd::bulk::decompress(&compressed, decoded_len as usize)
                    .map_err(|e| VixError::Malformed(format!("dict index zstd: {e}")))?,
            ))
        }
        (Some(_), Some(other)) => {
            return Err(VixError::Malformed(format!(
                "unsupported dict blob codec {other:?}: this reader decodes zstd only"
            ))
            .into());
        }
    };
    let dict_blocks = container.dict_blocks.map(|h| h.bytes()).transpose()?;
    let terms = container.terms.map(|h| h.bytes()).transpose()?;
    let plist = container.plist.map(|h| h.bytes()).transpose()?;
    let mut blooms: Vec<FileBloom> = match container.bloom {
        Some(handle) => parse_file_blooms(&handle.bytes()?)?,
        None => Vec::new(),
    };

    // --- W1 AUTO additive sections from one dictionary walk ---
    let row_count = reader.row_count();
    let mut added_sections = Vec::new();
    if opts.auto_bloom.enabled() && reader.term_count() > 0 {
        let existing: std::collections::HashSet<&str> =
            blooms.iter().map(|b| b.field.as_str()).collect();
        let schema = reader.docs_schema()?;
        let string_typed = |name: &str| {
            schema.field_with_name(name).is_ok_and(|field| {
                matches!(
                    field.data_type(),
                    arrow::datatypes::DataType::Utf8
                        | arrow::datatypes::DataType::LargeUtf8
                        | arrow::datatypes::DataType::Utf8View
                )
            })
        };
        // the writer's candidate rule: complete raw-string term fields that
        // have no section yet (`term_fields` is TERM-typed only: no fts, no
        // bloom-only, no demoted)
        let candidates: Vec<(u16, String)> = reader
            .term_fields()
            .filter(|(_, name)| {
                !existing.contains(name)
                    && !reader.partial_fields().contains(*name)
                    && string_typed(name)
            })
            .map(|(id, name)| (id, name.to_string()))
            .collect();
        if !candidates.is_empty() {
            let mut acc = BloomHashAcc::from_pairs(Vec::<(u16, String)>::new());
            acc.track_auto_candidates(candidates, &opts.auto_bloom, row_count);
            reader.for_each_term(&mut |key, _doc_count, _row_groups| {
                acc.observe_dict_key(key);
                Ok(())
            })?;
            let selection =
                acc.select_auto(&opts.auto_bloom, row_count, data.len(), opts.bloom_fpp);
            // a dictionary walk publishes nothing it cannot trust (checked
            // build: dropped keys refuse, empty fields are withheld)
            let built = acc.build_checked(opts.bloom_fpp)?;
            added_sections = selection.selected;
            blooms.extend(built);
            blooms.sort_by(|a, b| a.field.cmp(&b.field));
        }
    }

    // --- properties the current writer stamps ---
    let bloom_part = if blooms.is_empty() {
        properties.remove(PROP_BLOOM_SECTIONS);
        None
    } else {
        let (blob, sections) = serialize_file_blooms_with_sections(&blooms)?;
        properties.insert(
            PROP_BLOOM_SECTIONS.to_string(),
            serde_json::to_string(&sections)?,
        );
        Some(blob)
    };
    // `bf` on every field with a per-field section
    let bloomed: std::collections::HashSet<&str> = blooms
        .iter()
        .map(|b| b.field.as_str())
        .filter(|field| *field != COMPOSITE_BLOOM_FIELD)
        .collect();
    let fields_raw = properties
        .get(PROP_FIELDS)
        .ok_or_else(|| VixError::Malformed(format!("missing file property {PROP_FIELDS:?}")))?;
    let mut fields: Vec<FieldEntry> = serde_json::from_str(fields_raw)?;
    for entry in &mut fields {
        if bloomed.contains(entry.name.as_str()) {
            entry.types.0 |= FieldTypeFlags::BLOOMED.0;
        }
    }
    properties.insert(PROP_FIELDS.to_string(), serde_json::to_string(&fields)?);
    properties.insert(
        PROP_LAYOUT_GEN.to_string(),
        if opts.sidecar_tail_zstd {
            LAYOUT_GEN_ZSTD
        } else {
            LAYOUT_GEN_PLAIN
        }
        .to_string(),
    );
    // S3: the data object's footer, as stored (the repack never touches it)
    let data_footer = read_data_footer_payload(&data)?;
    properties.insert(
        PROP_DATA_FOOTER.to_string(),
        String::from_utf8(data_footer)
            .map_err(|e| VixError::Malformed(format!("data footer payload is not UTF-8: {e}")))?,
    );
    if reader.columns_complete() {
        properties.insert(PROP_COLUMNS_COMPLETE.to_string(), "true".to_string());
    } else {
        properties.remove(PROP_COLUMNS_COMPLETE);
    }

    // --- assembly, in the writer's blob order ---
    let mut blobs: Vec<(&'static str, &'static str, BlobPart)> = Vec::new();
    if let Some(plist) = plist {
        blobs.push((BLOB_TYPE_PLIST, BLOB_TAG_PLIST, BlobPart::Shared(plist)));
    }
    if let Some(dict_blocks) = dict_blocks {
        blobs.push((
            BLOB_TYPE_DICT_BLOCKS,
            BLOB_TAG_DICT_BLOCKS,
            BlobPart::Shared(dict_blocks),
        ));
    }
    if let Some(bloom) = bloom_part {
        blobs.push((BLOB_TYPE_BLOOM, BLOB_TAG_BLOOM, BlobPart::Mem(bloom)));
    }
    if let Some(terms) = terms {
        blobs.push((BLOB_TYPE_TERMS, BLOB_TAG_TERMS, BlobPart::Shared(terms)));
    }
    if let Some(dict) = dict {
        let part = if opts.sidecar_tail_zstd && dict.len() as u64 <= DICT_RESIDENT_MAX_BYTES {
            BlobPart::ZstdMem(
                zstd::bulk::compress(&dict, crate::writer::SIDECAR_ZSTD_LEVEL)
                    .map_err(|e| VixError::Writer(format!("dict index zstd: {e}")))?,
            )
        } else {
            BlobPart::Shared(dict)
        };
        blobs.push((BLOB_TYPE_DICT, BLOB_TAG_DICT, part));
    }
    let sidecar = build_container_parts(
        properties.into_iter().collect(),
        blobs,
        opts.sidecar_tail_zstd,
    )?;
    Ok(SidecarRepack {
        sidecar,
        row_count,
        term_count: reader.term_count(),
        from_layout_gen,
        added_sections,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BytesRangeSource, VixQuery};

    fn source(name: &str, bytes: &[u8]) -> Arc<dyn VixRangeSource> {
        BytesRangeSource::new(name, bytes::Bytes::copy_from_slice(bytes))
    }

    fn exact(field: &str, token: &str) -> VixQuery {
        VixQuery::Exact {
            field: field.to_string(),
            token: token.as_bytes().to_vec(),
        }
    }

    /// A repack of an old-layout sidecar opens and answers like the original
    /// (same counts, same dictionary walk), carries every new-layout stamp,
    /// gains the AUTO sections the writer would select, stores the dict
    /// zstd and leaves the data object untouched. A second repack of the
    /// result is byte-identical (idempotent).
    #[test]
    fn repack_preserves_the_index_and_adds_the_new_layout() {
        use std::sync::Arc as StdArc;

        use arrow::{
            array::{Int64Array, StringArray},
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        };

        use crate::{VixWriter, VixWriterOptions, test_support::strip_property_for_tests};

        const ROWS: usize = 3_000;
        let schema = StdArc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("host", DataType::Utf8, true),
            Field::new("request_id", DataType::Utf8, true),
            Field::new("level", DataType::Utf8, true),
        ]));
        let ts: Vec<i64> = (0..ROWS as i64).map(|i| 1_000_000 - i).collect();
        let host: Vec<String> = (0..ROWS).map(|i| format!("host-{}", i % 150)).collect();
        let request_id: Vec<String> = (0..ROWS).map(|i| format!("req-{i:08x}")).collect();
        let level: Vec<&str> = (0..ROWS)
            .map(|i| ["info", "warn", "error"][i % 3])
            .collect();
        let sources: Vec<String> = (0..ROWS)
            .map(|i| {
                format!(
                    r#"{{"_timestamp":{},"host":"{}","request_id":"{}","level":"{}"}}"#,
                    ts[i], host[i], request_id[i], level[i]
                )
            })
            .collect();
        let batch = RecordBatch::try_new(
            StdArc::clone(&schema),
            vec![
                StdArc::new(Int64Array::from(ts)),
                StdArc::new(StringArray::from_iter_values(
                    host.iter().map(String::as_str),
                )),
                StdArc::new(StringArray::from_iter_values(
                    request_id.iter().map(String::as_str),
                )),
                StdArc::new(StringArray::from(level)),
            ],
        )
        .unwrap();
        // an old-layout writer: plain tail, no AUTO, the columns license on
        let mut writer = VixWriter::new(
            &schema,
            VixWriterOptions {
                columns_complete: true,
                ..Default::default()
            },
            false,
        );
        writer
            .push_batch_with_source(
                &batch,
                &StringArray::from_iter_values(sources.iter().map(String::as_str)),
                None,
            )
            .unwrap();
        let (data, index) = writer.finish().unwrap();
        let index = index.expect("indexed");
        // strip the properties only the new writer stamps, like a pre-S1 file
        let mut legacy = index.clone();
        for property in [
            PROP_LAYOUT_GEN,
            PROP_DATA_FOOTER,
            PROP_BLOOM_SECTIONS,
            PROP_COLUMNS_COMPLETE,
        ] {
            legacy = strip_property_for_tests(&legacy, property).unwrap();
        }
        let before = VixReader::open_with_index(
            bytes::Bytes::from(data.clone()),
            Some(bytes::Bytes::from(legacy.clone())),
        )
        .unwrap();
        assert_eq!(before.layout_gen(), 0);
        assert!(!before.has_file_blooms());

        let opts = SidecarRepackOptions {
            sidecar_tail_zstd: true,
            auto_bloom: AutoBloomPolicy {
                min_distinct: 100,
                max_ratio: 0.25,
                max_data_pct: 100.0,
            },
            bloom_fpp: 0.001,
        };
        let repack =
            repack_sidecar(source("data", &data), source("legacy", &legacy), &opts).unwrap();
        assert_eq!(repack.from_layout_gen, 0);
        assert_eq!(repack.row_count, ROWS as u64);
        assert_eq!(
            repack.added_sections,
            vec![
                ("host".to_string(), 150),
                ("request_id".to_string(), ROWS as u64)
            ],
            "host (ratio) and request_id (ID-like) selected; level below min"
        );

        let after = VixReader::open_with_index(
            bytes::Bytes::from(data.clone()),
            Some(bytes::Bytes::from(repack.sidecar.clone())),
        )
        .unwrap();
        assert_eq!(after.layout_gen(), 2);
        assert_eq!(after.row_count(), before.row_count());
        assert_eq!(after.term_count(), before.term_count());
        assert_eq!(
            after.bloomed_fields().collect::<Vec<_>>(),
            vec!["host", "request_id"]
        );
        for query in [
            exact("level", "warn"),
            exact("host", "host-7"),
            exact("request_id", "req-0000002a"),
            exact("request_id", "req-nope"),
        ] {
            assert_eq!(
                after.count(&query).unwrap(),
                before.count(&query).unwrap(),
                "{query:?}"
            );
        }
        assert_eq!(
            after.field_value_counts("host").unwrap(),
            before.field_value_counts("host").unwrap()
        );
        // the new stamps are there and the dict is stored zstd
        let meta = puffin::reader::parse_puffin_footer_from_bytes(&repack.sidecar).unwrap();
        assert!(meta.properties.contains_key(PROP_DATA_FOOTER));
        assert!(meta.properties.contains_key(PROP_BLOOM_SECTIONS));
        assert_eq!(
            meta.properties
                .get(PROP_COLUMNS_COMPLETE)
                .map(String::as_str),
            Some("true")
        );
        let dict = meta
            .blobs
            .iter()
            .find(|b| b.properties.get("blob_tag").map(String::as_str) == Some("dict"))
            .unwrap();
        assert_eq!(dict.compression_codec, Some(puffin::CompressionCodec::Zstd));
        // the added sections are real filters
        let probe =
            crate::FileBloomProbe::open_sidecar(source("repacked", &repack.sidecar)).unwrap();
        let values: [&[u8]; 2] = [b"host-7", b"host-900"];
        assert_eq!(
            probe.probe("host", &values, false).unwrap(),
            Some(vec![true, false])
        );
        assert_eq!(probe.has_field("nope"), Some(false), "licensed presence");

        // idempotent: repacking the result changes nothing
        let again = repack_sidecar(
            source("data", &data),
            source("repacked", &repack.sidecar),
            &opts,
        )
        .unwrap();
        assert_eq!(again.from_layout_gen, 2);
        assert!(again.added_sections.is_empty());
        assert_eq!(again.sidecar, repack.sidecar);
    }
}
