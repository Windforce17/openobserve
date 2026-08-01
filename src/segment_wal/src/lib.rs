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

//! Segment WAL: the S3-first ingest path (DESIGN-SEGMENT-WAL.md).
//!
//! A node buffers validated rows for all streams in memory, and every
//! `ZO_SEGMENT_FLUSH_INTERVAL_MS` (or `ZO_SEGMENT_FLUSH_SIZE_MB`, whichever
//! first) encodes ONE multi-stream segment object, uploads it to object
//! storage, and registers it in the `wal_segments` table. Clients are acked
//! at append time — the owner-accepted durability contract is "a node crash
//! may lose up to one flush interval of acked data".

pub mod buffer;
pub mod format;
pub mod uploader;

pub use buffer::{AppendError, BufferFull, SegmentBuffer, UnencodableFrame, global_buffer};
pub use format::{SegmentFrame, SegmentHeader, decode_segment, encode_segment};
pub use uploader::{flush_now, run_flusher};
