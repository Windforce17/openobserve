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

//! `.bf` file format and helpers.
//!
//! A `.bf` is a flat object-store blob holding one Split-Block Bloom Filter
//! (SBBF — Parquet's block bit-layout, but our own hash and framing) per
//! (file, field). A single hour bucket of a stream is covered by **one or
//! more** `.bf` files: when a bucket holds many files they're sub-grouped
//! into chunks (to cap build-time memory), and each chunk is written as its
//! own `.bf` named by its `bloom_ver` (`base_ts + chunk_idx`). See
//! `bloom::path` for the naming scheme.
//!
//! Two on-disk formats share the transposed body:
//!
//! **v1** (legacy, read-only since v2 landed):
//!
//! ```text
//! 0     ────────────────────────────────────
//!       MAGIC      4B   "O2BF"
//!       VERSION    1B   0x01
//! ────────────────────────────────────────────
//!       BODY            per field: a transposed bit-matrix.
//!                       All M files of a field share num_blocks=B.
//!                       Row j (j in 0..B) = every file's block j,
//!                       back to back: [f0.bj][f1.bj]…[f(M-1).bj],
//!                       each 32 bytes → row is M×32 bytes.
//! ────────────────────────────────────────────
//!       FOOTER          (thrift-free, hand-rolled)
//!         field_count   u32 LE
//!         per field:
//!           name_len    u16 LE
//!           name        bytes
//!           algo        u8           (0x01 = SBBF + gxhash64)
//!           num_blocks  u32 LE       (B, uniform across files)
//!           file_count  u32 LE       (M)
//!           body_offset u64 LE       (start of this field's matrix)
//!           per file (column order):
//!             file_id     u64 LE
//!             n_items     u32 LE
//! ────────────────────────────────────────────
//!       FOOTER_LEN  4B  (LE)
//!       MAGIC       4B  "O2BF"
//! EOF   ────────────────────────────────────
//! ```
//!
//! **v2** (DESIGN-COLD-MISS §4 B1 — presence bits + a group file table):
//!
//! ```text
//! 0     ────────────────────────────────────
//!       MAGIC      4B   "O2BF"
//!       VERSION    1B   0x02
//! ────────────────────────────────────────────
//!       BODY            IDENTICAL to v1 per section (transposed rows,
//!                       uniform num_blocks); a section's rows are
//!                       member_count × 32 bytes wide.
//! ────────────────────────────────────────────
//!       FOOTER          zstd-compressed payload (level 3), plaintext:
//!         field_count   u32 LE
//!         column_count  u32 LE      (union of every member's columns)
//!         per column:   name_len u16 LE, name bytes
//!         file_count    u32 LE      (group file table, ids once)
//!         per file:     file_id u64 LE
//!         per file:     flags u8    (bit 0 = presence bits AUTHORITATIVE:
//!                                    the data object carries
//!                                    `columns_complete`; a clear bit means
//!                                    the bits are informational only and
//!                                    the pruner reads them as "no info")
//!         per file:     presence bitmap, ceil(column_count/8) bytes
//!         per field:
//!           name_len    u16 LE
//!           name        bytes
//!           algo        u8           (0x01 = SBBF + gxhash64)
//!           num_blocks  u32 LE       (B, uniform across members)
//!           body_offset u64 LE       (start of this field's matrix)
//!           member_count u32 LE
//!           per member (ASCENDING group-file index):
//!             file_index  u32 LE
//!             n_items     u32 LE
//! ────────────────────────────────────────────
//!       FOOTER_LEN  4B  (LE, ON-DISK = compressed length)
//!       MAGIC_V2    4B  "O2B2"
//! EOF   ────────────────────────────────────
//! ```
//!
//! **Why the new tail magic.** A v1 reader validates the version byte only
//! when its suffix covers the whole blob (the pruner reads a tail suffix),
//! so a v2 footer would be parsed as v1 and mis-read. `"O2B2"` makes the
//! v1 reader's tail check fail deterministically → keep-all (fail-open),
//! which is the B5 compatibility contract.
//!
//! **Why transposed.** `block_index = fastmap(hash(value), B)` depends
//! only on `B`. With a group-uniform `B`, one query value maps to the
//! same block index `bi` across every file. Laying the body out
//! block-major means the blocks a query needs — one per file — are a
//! single contiguous row at `bi`. So the search side reads **one
//! `M × 32` range per group** instead of one tiny range per file,
//! turning prune IO from O(files) into O(groups). This is the form that
//! can beat tantivy on S3 for fully-random high-cardinality fields,
//! where tantivy's sparse-index range pruning can't reject and it must
//! touch every file.

pub mod footer_cache;
pub mod path;
pub mod reader;
pub mod sbbf;
pub mod writer;

pub use footer_cache::{BLOOM_FOOTER_CACHE, BloomFooterCache};
pub use reader::{BloomReader, FieldInspect, ReadError, SectionLayout};
pub use sbbf::{
    BLOCK_BYTES, Sbbf, block_index, check_block, check_block_with_mask, hash_value, mask_from_hash,
    num_blocks_for,
};
pub use writer::{BloomBuilder, BloomWriter, FieldBloom, GroupMember};

/// Magic prefix and suffix for v1 `.bf` files.
pub const MAGIC: &[u8; 4] = b"O2BF";

/// Tail magic of a v2 `.bf`. Deliberately different from [`MAGIC`] so a v1
/// reader's tail check (which runs even on a short suffix probe) rejects a
/// v2 object deterministically instead of mis-parsing a compressed footer.
pub const MAGIC_V2: &[u8; 4] = b"O2B2";

/// v1 `.bf` format version (read-only since v2 landed).
pub const VERSION: u8 = 0x01;

/// v2 `.bf` format version (DESIGN-COLD-MISS §4 B1).
pub const VERSION_V2: u8 = 0x02;

/// zstd compression level for the v2 footer payload. The footer is a JSON-
/// sized blob of names + bitmaps; level 3 is the standard "fast, ~4x" tier.
pub const FOOTER_ZSTD_LEVEL: i32 = 3;

/// Algorithm tag for SBBF + gxhash64 (the project-wide default 64-bit
/// hash from `config::utils::hash`). Block layout follows the Parquet
/// SBBF spec; the hash function does not — we own both sides of the
/// `.bf` format so spec compatibility on the hash isn't required.
pub const ALGO_SBBF_GXHASH: u8 = 0x01;

// `file_list.id` is the per-file key used inside the `.bf` footer. The
// compactor's post-merge bloom builder runs *after* the file_list INSERT,
// so the assigned id is always available; the search side reads the same
// id directly off the FileKey returned by file_list query. Cast i64→u64
// is safe — file_list ids are sequential u63s.
