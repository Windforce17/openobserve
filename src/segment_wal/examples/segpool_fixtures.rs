// Copyright 2026 OpenObserve Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

//! Regenerate the small infra startup-integrity fixtures with the real encoder.
//! See src/infra/src/cache/file_data/fixtures/README.md for the command.

use std::{path::PathBuf, sync::Arc};

use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use config::meta::stream::StreamType;
use segment_wal::format::{SegmentFrame, SegmentHeader, encode_segment_version};

fn main() {
    let root = PathBuf::from(std::env::args().nth(1).expect("fixture output directory"));
    std::fs::create_dir_all(&root).unwrap();
    let header = SegmentHeader {
        node_uuid: "cache-test".into(),
        seq: 42,
        created_at: 1_722_400_000_000_000,
    };
    let frames = ["first", "second"].map(|stream| SegmentFrame {
        org: "cache-test".into(),
        stream_type: StreamType::Logs,
        stream: stream.into(),
        min_ts: 10,
        max_ts: 20,
        batch: RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![10, 20]))],
        )
        .unwrap(),
    });
    for version in [1, 2] {
        let bytes = encode_segment_version(&header, &frames, version).unwrap();
        segment_wal::format::decode_segment(&bytes).unwrap();
        std::fs::write(root.join(format!("segment-v{version}.bin")), bytes).unwrap();
    }
}
