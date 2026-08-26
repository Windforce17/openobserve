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

//! Minimal core-file (`.vix`) container access for the generic byte readers
//! in [`super::parquet`].
//!
//! A core `.vix` file is a puffin container whose `docs` blob is a complete
//! Vortex file holding the stored records (`_timestamp`, column-store
//! fields, `_source`, optional `_original`/`_o2_id`). The full container
//! reader lives in the `vortex_index` crate, but that crate depends on
//! `config`, so the handful of generic
//! record-batch/schema helpers here re-implement just the footer walk needed
//! to slice out the `docs` blob. The puffin layout is frozen
//! (`Magic Blobs… Magic FooterPayload PayloadSize Flags Magic`, JSON footer
//! payload per the Iceberg puffin spec), so this stays in lockstep with
//! the `puffin` crate by construction; both sides are pinned by the same
//! constants below.

use std::collections::HashMap;

use serde::Deserialize;

/// Puffin magic bytes (`PFA1`).
const PUFFIN_MAGIC: [u8; 4] = [0x50, 0x46, 0x41, 0x31];
const MAGIC_SIZE: usize = 4;
const FLAGS_SIZE: usize = 4;
const PAYLOAD_SIZE_SIZE: usize = 4;
const FOOTER_SIZE: usize = MAGIC_SIZE + FLAGS_SIZE + PAYLOAD_SIZE_SIZE;
/// Footer flag bit 0: zstd-compressed footer payload. The o2 writer never
/// sets it; reject instead of silently mis-parsing.
const FLAG_FOOTER_COMPRESSED: u32 = 1;

/// Blob type of the `docs` blob.
const DOCS_BLOB_TYPE: &str = "o2-vix-docs-v1";
/// `blob_tag` property of the docs blob.
const DOCS_BLOB_TAG: &str = "docs";

/// The subset of the puffin footer payload the docs slicing needs.
#[derive(Deserialize)]
struct PuffinFooter {
    blobs: Vec<PuffinBlob>,
}

#[derive(Deserialize)]
struct PuffinBlob {
    #[serde(rename = "type")]
    blob_type: String,
    offset: u64,
    length: u64,
    #[serde(default)]
    properties: HashMap<String, String>,
}

/// Slice the `docs` blob (a complete Vortex file) out of full `.vix` bytes.
pub fn docs_blob_from_vix_bytes(data: &bytes::Bytes) -> Result<bytes::Bytes, anyhow::Error> {
    let len = data.len();
    if len < MAGIC_SIZE + FOOTER_SIZE + MAGIC_SIZE {
        return Err(anyhow::anyhow!(
            ".vix file too small to be a puffin container: {len} bytes"
        ));
    }
    if data[len - MAGIC_SIZE..] != PUFFIN_MAGIC {
        return Err(anyhow::anyhow!(
            ".vix footer magic mismatch (not a puffin container)"
        ));
    }
    let footer = &data[len - FOOTER_SIZE..];
    let flags = u32::from_le_bytes(
        footer[PAYLOAD_SIZE_SIZE..PAYLOAD_SIZE_SIZE + FLAGS_SIZE]
            .try_into()
            .unwrap(),
    );
    if flags & FLAG_FOOTER_COMPRESSED != 0 {
        return Err(anyhow::anyhow!(
            ".vix puffin footer is compressed; not supported by this reader"
        ));
    }
    let payload_size = i32::from_le_bytes(footer[..PAYLOAD_SIZE_SIZE].try_into().unwrap());
    let payload_size = usize::try_from(payload_size)
        .map_err(|_| anyhow::anyhow!(".vix puffin footer payload size is negative"))?;
    let payload_end = len - FOOTER_SIZE;
    let payload_start = payload_end
        .checked_sub(payload_size)
        .filter(|start| *start >= MAGIC_SIZE)
        .ok_or_else(|| anyhow::anyhow!(".vix puffin footer payload size out of bounds"))?;
    if data[payload_start - MAGIC_SIZE..payload_start] != PUFFIN_MAGIC {
        return Err(anyhow::anyhow!(".vix puffin payload magic mismatch"));
    }
    let footer: PuffinFooter = serde_json::from_slice(&data[payload_start..payload_end])
        .map_err(|e| anyhow::anyhow!(".vix puffin footer parse error: {e}"))?;

    let docs = footer
        .blobs
        .iter()
        .find(|blob| {
            blob.blob_type == DOCS_BLOB_TYPE
                && blob.properties.get("blob_tag").map(String::as_str) == Some(DOCS_BLOB_TAG)
        })
        .ok_or_else(|| {
            anyhow::anyhow!(
                ".vix file has no docs blob (a .vxi index sidecar fed in as a data file?)"
            )
        })?;
    let start = usize::try_from(docs.offset)
        .map_err(|_| anyhow::anyhow!(".vix docs blob offset overflow"))?;
    let end = start
        .checked_add(
            usize::try_from(docs.length)
                .map_err(|_| anyhow::anyhow!(".vix docs blob length overflow"))?,
        )
        .filter(|end| *end <= len)
        .ok_or_else(|| anyhow::anyhow!(".vix docs blob range out of bounds"))?;
    Ok(data.slice(start..end))
}
