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

//! Segment-mode write glue (DESIGN-SEGMENT-WAL.md).
//!
//! When `ZO_INGEST_SEGMENT_MODE` is on, `write_file` routes each prepared
//! stream buffer here instead of the memtable/WAL writer. The rows go
//! through the SAME JSON→Arrow conversion the legacy writer runs in
//! `preprocess_batch` (`ingester::Entry::into_batch`: same narrow
//! present-fields schema, same `_timestamp` column, same min/max derivation
//! that never assumes sorted input), and each resulting batch is appended to
//! the process-wide segment buffer that the flusher ships to object storage.

use std::sync::Arc;

use config::meta::stream::StreamType;
use infra::errors::{Error, Result};
use segment_wal::{
    SegmentBuffer,
    buffer::{AppendError, BufferFull},
};

/// Extract the stream type from a writer key (`Writer::get_key_str`, shaped
/// `"{org_id}/{stream_type}"`). The type is the LAST '/'-separated segment —
/// stream-type strings never contain '/' — and must round-trip through
/// `StreamType::as_str`; anything else is a named error instead of the
/// silent `StreamType::Logs` fallback `From<&str>` would give.
pub(super) fn stream_type_from_writer_key(writer_key: &str) -> Result<StreamType> {
    // rsplit always yields at least one item; unwrap_or_default is only
    // defensive against that invariant ever changing
    let type_str = writer_key.rsplit('/').next().unwrap_or_default();
    let stream_type = StreamType::from(type_str);
    if stream_type.as_str() != type_str {
        return Err(Error::IngestionError(format!(
            "segment mode: writer key {writer_key:?} does not end in a known stream type"
        )));
    }
    Ok(stream_type)
}

/// Convert `entries` (the same per-hour entries the legacy path feeds
/// `Writer::write_batch`) and append one frame per entry to `buffer`.
///
/// Errors PROPAGATE to the caller that acks: a conversion failure or a full
/// buffer fails this stream's write, so nothing is acked that was not
/// appended. `BufferFull` maps to `Error::ResourceError` — the exact variant
/// the memtable-full path (`check_memtable_size`) uses — so the HTTP
/// handlers answer 503 and shippers back off. An unencodable frame (identity
/// field beyond the segment format's u16/u32 bounds) is DETERMINISTIC and
/// maps to `Error::IngestionError` instead — a per-request failure, never a
/// retryable 503.
pub(super) fn append_entries(
    buffer: &SegmentBuffer,
    org_id: &str,
    stream_type: StreamType,
    stream_name: &str,
    entries: &[ingester::Entry],
) -> Result<()> {
    let type_str: Arc<str> = Arc::from(stream_type.as_str());
    for entry in entries {
        let Some(schema) = entry.schema.clone() else {
            // write_file always sets the schema; surface a missing one as a
            // real write failure, never a panic on the write path
            return Err(Error::IngestionError(format!(
                "segment mode: entry for stream {org_id}/{stream_type}/{stream_name} has no schema"
            )));
        };
        let converted = entry
            .into_batch(Arc::clone(&type_str), schema)
            .map_err(|e| {
                Error::IngestionError(format!(
                    "segment mode: arrow conversion failed for stream \
                 {org_id}/{stream_type}/{stream_name}: {e}"
                ))
            })?;
        if converted.data.num_rows() == 0 {
            // an empty batch carries no data to ship; skipping it loses
            // nothing and keeps zero-row frames out of segment objects
            continue;
        }
        buffer
            .append(
                org_id,
                stream_type,
                stream_name,
                converted.min_ts,
                converted.max_ts,
                converted.data.clone(),
            )
            .map_err(|e| match e {
                AppendError::Full(e) => map_buffer_full(org_id, stream_type, stream_name, e),
                AppendError::Unencodable(e) => Error::IngestionError(format!(
                    "segment mode: stream {org_id}/{stream_type}/{stream_name}: {e}"
                )),
            })?;
    }
    Ok(())
}

/// The 503 mapping: the same `Error::ResourceError` the memtable-full path
/// produces, so shippers see an identical retryable status in both modes.
fn map_buffer_full(
    org_id: &str,
    stream_type: StreamType,
    stream_name: &str,
    e: BufferFull,
) -> Error {
    Error::ResourceError(format!("stream {org_id}/{stream_type}/{stream_name}: {e}"))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow::array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use config::{
        TIMESTAMP_COL_NAME,
        utils::json::{self, estimate_json_bytes},
    };
    use segment_wal::global_buffer;

    use super::*;
    use crate::common::meta::stream::SchemaRecords;

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new(TIMESTAMP_COL_NAME, DataType::Int64, false),
            Field::new("msg", DataType::Utf8, true),
            // in the stream schema but absent from every row: the conversion
            // must treat it exactly like the legacy path does
            Field::new("never_present", DataType::Utf8, true),
        ]))
    }

    fn test_rows(ts: &[i64]) -> Vec<Arc<json::Value>> {
        ts.iter()
            .map(|t| {
                let mut m = json::Map::new();
                m.insert(
                    TIMESTAMP_COL_NAME.to_string(),
                    json::Value::Number((*t).into()),
                );
                m.insert("msg".to_string(), json::Value::String(format!("m{t}")));
                Arc::new(json::Value::Object(m))
            })
            .collect()
    }

    fn test_entry(org: &str, stream: &str, ts: &[i64]) -> ingester::Entry {
        let rows = test_rows(ts);
        let size = rows.iter().map(|r| estimate_json_bytes(r)).sum();
        ingester::Entry {
            org_id: Arc::from(org),
            stream: Arc::from(stream),
            schema: Some(test_schema()),
            schema_key: Arc::from("k"),
            partition_key: Arc::from("2026/01/01/00/default"),
            data: rows,
            data_size: size,
            batch: None,
        }
    }

    #[test]
    fn stream_type_parses_from_writer_key() {
        assert_eq!(
            stream_type_from_writer_key("default/logs").unwrap(),
            StreamType::Logs
        );
        assert_eq!(
            stream_type_from_writer_key("default/metadata").unwrap(),
            StreamType::Metadata
        );
        // only the LAST segment is the type, whatever the org part contains
        assert_eq!(
            stream_type_from_writer_key("weird/org/index").unwrap(),
            StreamType::Index
        );
        // adversarial: an unknown type must NOT silently default to logs
        let err = stream_type_from_writer_key("default/bogus").unwrap_err();
        assert!(err.to_string().contains("default/bogus"), "err: {err}");
        assert!(stream_type_from_writer_key("").is_err());
        assert!(stream_type_from_writer_key("default/").is_err());
    }

    /// The segment path must produce byte-equal batches vs the legacy
    /// conversion (`Entry::into_batch`, what `Writer::preprocess_batch`
    /// runs): same schema, same row count, same `_timestamp` values, and the
    /// min/max derived by scanning the column — the input is deliberately
    /// unsorted.
    #[test]
    fn conversion_matches_legacy_path() {
        let e = test_entry("eqorg", "eqstream", &[500, 100, 300]);
        let legacy = e
            .into_batch(Arc::from("logs"), e.schema.clone().unwrap())
            .unwrap();

        let buffer = SegmentBuffer::new();
        append_entries(
            &buffer,
            "eqorg",
            StreamType::Logs,
            "eqstream",
            std::slice::from_ref(&e),
        )
        .unwrap();
        let frames = buffer.drain();
        assert_eq!(frames.len(), 1);
        let frame = &frames[0];
        assert_eq!(frame.org, "eqorg");
        assert_eq!(frame.stream_type, StreamType::Logs);
        assert_eq!(frame.stream, "eqstream");

        assert_eq!(frame.batch.schema(), legacy.data.schema());
        assert_eq!(frame.batch, legacy.data);
        assert_eq!(frame.batch.num_rows(), 3);
        let ts_col = frame
            .batch
            .column_by_name(TIMESTAMP_COL_NAME)
            .expect("_timestamp column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("_timestamp is Int64")
            .iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(ts_col, vec![500, 100, 300]);

        // range comes from scanning the column, not from first/last row
        assert_eq!((frame.min_ts, frame.max_ts), (legacy.min_ts, legacy.max_ts));
        assert_eq!((frame.min_ts, frame.max_ts), (100, 500));
    }

    #[test]
    fn entry_without_schema_is_a_named_error() {
        let mut e = test_entry("o1", "s1", &[1]);
        e.schema = None;
        let buffer = SegmentBuffer::new();
        let err = append_entries(&buffer, "o1", StreamType::Logs, "s1", &[e]).unwrap_err();
        assert!(matches!(err, Error::IngestionError(_)), "got {err:?}");
        assert!(err.to_string().contains("o1/logs/s1"), "err: {err}");
        assert!(buffer.drain().is_empty());
    }

    #[test]
    fn buffer_full_maps_to_resource_error() {
        let e = map_buffer_full(
            "org1",
            StreamType::Logs,
            "app1",
            BufferFull {
                buffered_bytes: 7,
                max_bytes: 9,
            },
        );
        // the memtable-full path returns Error::ResourceError, which the
        // HTTP handlers turn into 503 — segment mode must ride the exact
        // same mapping
        match &e {
            Error::ResourceError(msg) => {
                assert!(msg.contains("org1/logs/app1"), "msg: {msg}");
                assert!(msg.contains('7'), "msg: {msg}");
                assert!(msg.contains('9'), "msg: {msg}");
            }
            other => panic!("expected ResourceError, got {other:?}"),
        }
    }

    /// The seam decision end to end: with `ZO_INGEST_SEGMENT_MODE` set (the
    /// env override the config system supports, same pattern as the config
    /// crate's env-override tests), `write_file` routes to the GLOBAL
    /// segment buffer and RequestStats stays correct; and with a zero buffer
    /// cap, appends surface as `Error::ResourceError`. Both phases live in
    /// ONE test so the two env mutations can never overlap each other.
    #[tokio::test]
    async fn segment_mode_seam_routes_to_buffer_and_maps_buffer_full() {
        // phase 1: flag on → write_file appends to the global buffer
        unsafe { std::env::set_var("ZO_INGEST_SEGMENT_MODE", "true") };
        config::refresh_config().unwrap();
        assert!(config::get_config().common.ingest_segment_mode);

        let stream_name = "segment_mode_seam_test_stream";
        let rows = test_rows(&[900, 100]);
        let records_size: usize = rows.iter().map(|r| estimate_json_bytes(r)).sum();
        let mut buf: HashMap<String, SchemaRecords> = HashMap::new();
        buf.insert(
            "2026/01/01/00/default".to_string(),
            SchemaRecords {
                schema_key: "k".to_string(),
                schema: test_schema(),
                records: rows,
                records_size,
            },
        );
        let writer =
            ingester::get_writer(0, "default", StreamType::Logs.as_str(), stream_name).await;
        let stats = crate::ingestion::write_file(&writer, "default", stream_name, buf, false).await;

        // restore BEFORE asserting so a failed assert cannot leak the flag
        // into concurrently running tests
        unsafe { std::env::remove_var("ZO_INGEST_SEGMENT_MODE") };
        config::refresh_config().unwrap();

        let stats = stats.unwrap();
        assert_eq!(stats.records, 2);
        assert!(stats.size > 0.0);

        // the rows are in the GLOBAL segment buffer (the flusher's source);
        // filter by our uniquely-named stream so concurrent tests cannot
        // interfere with the assertions
        let frames = global_buffer().drain();
        let mine: Vec<_> = frames.iter().filter(|f| f.stream == stream_name).collect();
        assert_eq!(mine.len(), 1);
        assert_eq!(mine[0].org, "default");
        assert_eq!(mine[0].stream_type, StreamType::Logs);
        assert_eq!(mine[0].batch.num_rows(), 2);
        assert_eq!((mine[0].min_ts, mine[0].max_ts), (100, 900));

        // phase 2: flag OFF again (write_file everywhere is legacy), the
        // SMALLEST cap config validation accepts (2MB = 2x a 1MB flush size —
        // a zero cap no longer passes check_common_config) and a single row
        // bigger than it → append_entries surfaces BufferFull as
        // ResourceError through the public config-driven append
        unsafe { std::env::set_var("ZO_SEGMENT_FLUSH_SIZE_MB", "1") };
        unsafe { std::env::set_var("ZO_SEGMENT_BUFFER_MAX_MB", "2") };
        config::refresh_config().unwrap();
        let local = SegmentBuffer::new();
        let mut e = test_entry("default", "seg503", &[1]);
        let mut row = json::Map::new();
        row.insert(
            TIMESTAMP_COL_NAME.to_string(),
            json::Value::Number(1.into()),
        );
        row.insert(
            "msg".to_string(),
            json::Value::String("x".repeat(3 * 1024 * 1024)),
        );
        e.data = vec![Arc::new(json::Value::Object(row))];
        e.data_size = e.data.iter().map(|r| estimate_json_bytes(r)).sum();
        let err = append_entries(&local, "default", StreamType::Logs, "seg503", &[e]).unwrap_err();
        unsafe { std::env::remove_var("ZO_SEGMENT_FLUSH_SIZE_MB") };
        unsafe { std::env::remove_var("ZO_SEGMENT_BUFFER_MAX_MB") };
        config::refresh_config().unwrap();
        assert!(matches!(err, Error::ResourceError(_)), "got {err:?}");
        assert!(err.to_string().contains("seg503"), "err: {err}");
        assert!(local.drain().is_empty());
    }
}
