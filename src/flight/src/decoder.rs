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

use std::{collections::HashMap, fmt::Debug, pin::Pin, sync::Arc, task::Poll};

use arrow::{array::ArrayRef, buffer::Buffer, ipc::MessageHeader};
use arrow_flight::{
    FlightData,
    error::{FlightError, Result},
};
use arrow_schema::{Schema, SchemaRef};
use datafusion::parquet::data_type::AsBytes;
use futures::{Stream, StreamExt, ready};
use tonic::Streaming;

use crate::common::{CustomMessage, FlightMessage, RemoteScanMetrics};

pub struct FlightDataDecoder {
    response: Streaming<FlightData>,
    schema: Option<SchemaRef>,
    dictionaries_by_field: HashMap<i64, ArrayRef>,
    done: bool,
    metrics: RemoteScanMetrics,
}

impl Debug for FlightDataDecoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlightDataDecoder")
            .field("response", &"<stream>")
            .field("schema", &self.schema)
            .field("dictionaries_by_field", &self.dictionaries_by_field)
            .field("done", &self.done)
            .finish()
    }
}

impl FlightDataDecoder {
    /// Create a new wrapper around the stream of [`FlightData`]
    pub fn new(
        response: Streaming<FlightData>,
        schema: Option<SchemaRef>,
        metrics: RemoteScanMetrics,
    ) -> Self {
        Self {
            response,
            schema,
            dictionaries_by_field: HashMap::new(),
            done: false,
            metrics,
        }
    }

    /// Returns the current schema for this stream
    pub fn schema(&self) -> Option<&SchemaRef> {
        self.schema.as_ref()
    }

    /// Extracts flight data from the next message
    fn extract_message(&mut self, data: FlightData) -> Result<Option<FlightMessage>> {
        let timer = self.metrics.decode_time.timer();
        let message = arrow::ipc::root_as_message(&data.data_header[..])
            .map_err(|e| FlightError::DecodeError(format!("Error decoding header: {e}")))?;

        let result = match message.header_type() {
            MessageHeader::NONE => {
                let message = serde_json::from_slice::<CustomMessage>(data.app_metadata.as_bytes())
                    .map_err(|e| {
                        FlightError::DecodeError(format!("Error decode custom message: {e}"))
                    })?;

                Ok(Some(FlightMessage::CustomMessage(message)))
            }
            MessageHeader::Schema => {
                let schema = Schema::try_from(&data)
                    .map_err(|e| FlightError::DecodeError(format!("Error decoding schema: {e}")))?;

                let schema = Arc::new(schema);

                self.schema = Some(schema.clone());
                Ok(Some(FlightMessage::Schema(schema)))
            }
            MessageHeader::DictionaryBatch => {
                let schema = if let Some(schema) = self.schema.as_ref() {
                    schema
                } else {
                    return Err(FlightError::protocol(
                        "Received DictionaryBatch prior to Schema",
                    ));
                };

                let buffer = Buffer::from(data.data_body);
                let dictionary_batch = message.header_as_dictionary_batch().ok_or_else(|| {
                    FlightError::protocol(
                        "Could not get dictionary batch from DictionaryBatch message",
                    )
                })?;

                arrow::ipc::reader::read_dictionary(
                    &buffer,
                    dictionary_batch,
                    schema,
                    &mut self.dictionaries_by_field,
                    &message.version(),
                )
                .map_err(|e| {
                    FlightError::DecodeError(format!("Error decoding ipc dictionary: {e}"))
                })?;

                Ok(None)
            }
            MessageHeader::RecordBatch => {
                let schema = if let Some(schema) = self.schema.as_ref() {
                    schema
                } else {
                    return Err(FlightError::protocol(
                        "Received RecordBatch prior to Schema",
                    ));
                };

                let batch_header = message.header_as_record_batch().ok_or_else(|| {
                    FlightError::DecodeError(format!(
                        "Error decoding ipc RecordBatch: {}",
                        arrow::error::ArrowError::ParseError(
                            "Unable to convert flight data header to a record batch".to_string(),
                        )
                    ))
                })?;
                let buffer = Buffer::from(data.data_body);
                let batch = arrow::ipc::reader::read_record_batch(
                    &buffer,
                    batch_header,
                    Arc::clone(schema),
                    &self.dictionaries_by_field,
                    None,
                    &message.version(),
                )
                .map_err(|e| {
                    FlightError::DecodeError(format!("Error decoding ipc RecordBatch: {e}"))
                })?;

                Ok(Some(FlightMessage::RecordBatch(batch)))
            }
            other => {
                let name = other.variant_name().unwrap_or("UNKNOWN");
                Err(FlightError::protocol(format!("Unexpected message: {name}")))
            }
        };
        timer.done();
        result
    }
}

impl Stream for FlightDataDecoder {
    type Item = Result<FlightMessage>;
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        loop {
            let res = ready!(self.response.poll_next_unpin(cx));

            return Poll::Ready(match res {
                None => {
                    self.done = true;
                    None // inner is exhausted
                }
                Some(data) => Some(match data {
                    Err(e) => Err(e.into()),
                    Ok(data) => match self.extract_message(data) {
                        Ok(Some(extracted)) => Ok(extracted),
                        Ok(None) => continue, // Need next input message
                        Err(e) => Err(e),
                    },
                }),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{
            ArrayRef, DictionaryArray, Int32Array, ListArray, RecordBatch, StringArray,
            StringViewArray, types::Int32Type,
        },
        ipc::{
            CompressionType, MessageHeader,
            writer::{CompressionContext, DictionaryTracker, IpcDataGenerator, IpcWriteOptions},
        },
    };
    use arrow_flight::{FlightData, SchemaAsIpc};
    use arrow_schema::{DataType, Field, Schema};
    use config::meta::search::ScanStats;
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use flatbuffers::FlatBufferBuilder;

    use super::*;
    use crate::common::CustomMessage;

    fn create_test_decoder() -> FlightDataDecoder {
        // extract_message is exercised directly; there are no transport frames to decode.
        struct NoFrames;
        impl tonic::codec::Decoder for NoFrames {
            type Item = FlightData;
            type Error = tonic::Status;

            fn decode(
                &mut self,
                _: &mut tonic::codec::DecodeBuf<'_>,
            ) -> std::result::Result<Option<Self::Item>, Self::Error> {
                unreachable!("the test stream has no frames")
            }
        }

        FlightDataDecoder::new(
            Streaming::new_empty(NoFrames, tonic::body::Body::empty()),
            None,
            RemoteScanMetrics::new(0, &ExecutionPlanMetricsSet::new()),
        )
    }

    fn encode_test_batch(batch: &RecordBatch, options: &IpcWriteOptions) -> Vec<FlightData> {
        let data_gen = IpcDataGenerator::default();
        let mut dictionary_tracker = DictionaryTracker::new(false);
        // Match encode_chunk: schema encoding assigns the IDs used by batch encoding.
        let _ = data_gen.schema_to_bytes_with_dictionary_tracker(
            &batch.schema(),
            &mut dictionary_tracker,
            options,
        );
        let (dictionaries, batch) = data_gen
            .encode(
                batch,
                &mut dictionary_tracker,
                options,
                &mut CompressionContext::default(),
            )
            .unwrap();
        dictionaries
            .into_iter()
            .chain(std::iter::once(batch))
            .map(Into::into)
            .collect()
    }

    fn decode_test_batch(decoder: &mut FlightDataDecoder, data: FlightData) -> RecordBatch {
        match decoder.extract_message(data).unwrap() {
            Some(FlightMessage::RecordBatch(batch)) => batch,
            other => panic!("Expected RecordBatch, got {other:?}"),
        }
    }

    fn create_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn create_test_record_batch() -> RecordBatch {
        let schema = create_test_schema();
        let id_array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let name_array: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Alice"), Some("Bob"), None]));

        RecordBatch::try_new(schema, vec![id_array, name_array]).unwrap()
    }

    fn create_custom_message_flight_data() -> FlightData {
        let scan_stats = ScanStats {
            files: 5,
            records: 500,
            original_size: 1024,
            compressed_size: 512,
            querier_files: 3,
            querier_memory_cached_files: 1,
            querier_disk_cached_files: 2,
            idx_scan_size: 256,
            idx_took: 50,
            file_list_took: 25,
            aggs_cache_ratio: 90,
            peak_memory_usage: 1024000,
            wait_in_queue: 0,
        };
        let custom_message = CustomMessage::ScanStats(scan_stats);
        let metadata = serde_json::to_string(&custom_message).unwrap();

        // Create NONE header
        let mut builder = FlatBufferBuilder::new();
        let mut message = arrow::ipc::MessageBuilder::new(&mut builder);
        message.add_version(arrow::ipc::MetadataVersion::V5);
        message.add_header_type(MessageHeader::NONE);
        message.add_bodyLength(0);
        let data = message.finish();
        builder.finish(data, None);
        let header = builder.finished_data().to_vec();

        FlightData::new()
            .with_data_header(header)
            .with_app_metadata(metadata.as_bytes().to_vec())
    }

    #[test]
    fn test_custom_message_serialization_deserialization() {
        let scan_stats = ScanStats {
            files: 5,
            records: 500,
            original_size: 1024,
            compressed_size: 512,
            querier_files: 3,
            querier_memory_cached_files: 1,
            querier_disk_cached_files: 2,
            idx_scan_size: 256,
            idx_took: 50,
            file_list_took: 25,
            aggs_cache_ratio: 90,
            peak_memory_usage: 1024000,
            wait_in_queue: 0,
        };
        let custom_message = CustomMessage::ScanStats(scan_stats);

        // Test serialization
        let serialized = serde_json::to_string(&custom_message).unwrap();

        // Test deserialization
        let deserialized: CustomMessage = serde_json::from_str(&serialized).unwrap();

        match deserialized {
            CustomMessage::ScanStats(stats) => {
                assert_eq!(stats.files, 5);
                assert_eq!(stats.records, 500);
                assert_eq!(stats.original_size, 1024);
                assert_eq!(stats.compressed_size, 512);
            }
            _ => panic!("Expected ScanStats variant"),
        }
    }

    #[test]
    fn test_flight_data_schema_encoding_decoding() {
        let schema = create_test_schema();
        let options = IpcWriteOptions::default();
        let flight_data: FlightData = SchemaAsIpc::new(&schema, &options).into();

        // Verify the flight data is properly formed
        assert!(!flight_data.data_header.is_empty());
        // Schema flight data may not always have a data body, only header

        // Test that we can decode it back to a schema
        let decoded_schema = Schema::try_from(&flight_data).unwrap();
        assert_eq!(decoded_schema.fields().len(), 2);
        assert_eq!(decoded_schema.field(0).name(), "id");
        assert_eq!(decoded_schema.field(1).name(), "name");
    }

    #[test]
    fn test_flight_data_record_batch_encoding_decoding() {
        let batch = create_test_record_batch();
        let options = IpcWriteOptions::default();
        let mut decoder = create_test_decoder();
        decoder
            .extract_message(SchemaAsIpc::new(batch.schema().as_ref(), &options).into())
            .unwrap();
        let decoded = decode_test_batch(
            &mut decoder,
            encode_test_batch(&batch, &options).pop().unwrap(),
        );
        drop(decoder);
        assert_eq!(decoded, batch);
    }

    #[test]
    fn test_owned_body_alignment_compression_and_dictionary_lifetime() {
        for compression in [None, Some(CompressionType::ZSTD)] {
            for alignment in [0, 1] {
                let options = IpcWriteOptions::default()
                    .try_with_compression(compression)
                    .unwrap();
                let mut decoder = create_test_decoder();
                let mut retained = Vec::new();
                for label in ["first dictionary", "replacement dictionary"] {
                    let columns: Vec<ArrayRef> = vec![
                        Arc::new(DictionaryArray::<Int32Type>::from_iter([
                            Some(label),
                            None,
                            Some(label),
                        ])),
                        Arc::new(StringViewArray::from(vec![
                            Some(format!("{label}: externally stored view")),
                            None,
                            Some("inline".to_string()),
                        ])),
                        Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                            Some(vec![Some(7), None, Some(-2)]),
                            None,
                            Some(vec![]),
                        ])),
                    ];
                    let schema = Arc::new(Schema::new(
                        columns
                            .iter()
                            .enumerate()
                            .map(|(i, column)| {
                                Field::new(format!("column_{i}"), column.data_type().clone(), true)
                            })
                            .collect::<Vec<_>>(),
                    ));
                    let expected = RecordBatch::try_new(schema, columns).unwrap();
                    if retained.is_empty() {
                        decoder
                            .extract_message(
                                SchemaAsIpc::new(expected.schema().as_ref(), &options).into(),
                            )
                            .unwrap();
                    }
                    // A fresh writer reuses the dictionary ID for the replacement values.
                    let mut messages = encode_test_batch(&expected, &options);
                    let mut data = messages.pop().unwrap();
                    for dictionary in messages {
                        assert!(decoder.extract_message(dictionary).unwrap().is_none());
                    }
                    // Exercise both aligned and misaligned slices of a larger owned allocation.
                    let mut body = vec![0; data.data_body.len() + 8];
                    let offset = (alignment + 8 - body.as_ptr() as usize % 8) % 8;
                    let end = offset + data.data_body.len();
                    body[offset..end].copy_from_slice(&data.data_body);
                    data.data_body = bytes::Bytes::from(body).slice(offset..end);
                    retained.push((decode_test_batch(&mut decoder, data), expected));
                }
                drop(decoder);
                for (decoded, expected) in retained {
                    assert_eq!(decoded, expected);
                }
            }
        }
    }

    #[test]
    fn test_record_batch_protocol_and_missing_dictionary_errors_preserve_state() {
        let values: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::from_iter([
            Some("retained"),
            None,
        ]));
        let expected = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "dictionary",
                values.data_type().clone(),
                true,
            )])),
            vec![values],
        )
        .unwrap();
        let options = IpcWriteOptions::default();
        let mut messages = encode_test_batch(&expected, &options);
        let batch = messages.pop().unwrap();
        let mut decoder = create_test_decoder();
        assert!(matches!(
            decoder.extract_message(batch.clone()),
            Err(FlightError::ProtocolError(_))
        ));
        decoder
            .extract_message(SchemaAsIpc::new(expected.schema().as_ref(), &options).into())
            .unwrap();
        let mut builder = FlatBufferBuilder::new();
        let mut header = arrow::ipc::MessageBuilder::new(&mut builder);
        header.add_version(arrow::ipc::MetadataVersion::V5);
        header.add_header_type(MessageHeader::RecordBatch);
        let header = header.finish();
        builder.finish(header, None);
        assert!(matches!(
            decoder.extract_message(
                FlightData::new().with_data_header(builder.finished_data().to_vec())
            ),
            Err(FlightError::DecodeError(_))
        ));
        assert!(matches!(
            decoder.extract_message(batch.clone()),
            Err(FlightError::DecodeError(_))
        ));
        for dictionary in messages {
            assert!(decoder.extract_message(dictionary).unwrap().is_none());
        }
        let decoded = decode_test_batch(&mut decoder, batch);
        drop(decoder);
        assert_eq!(decoded, expected);
    }

    #[test]
    fn test_custom_message_flight_data_creation() {
        let flight_data = create_custom_message_flight_data();

        // Verify structure
        assert!(!flight_data.data_header.is_empty());
        assert!(!flight_data.app_metadata.is_empty());
        assert!(flight_data.data_body.is_empty());

        // Verify we can deserialize the custom message
        let custom_message: CustomMessage =
            serde_json::from_slice(&flight_data.app_metadata).unwrap();
        match custom_message {
            CustomMessage::ScanStats(stats) => {
                assert_eq!(stats.files, 5);
                assert_eq!(stats.records, 500);
            }
            _ => panic!("Expected ScanStats variant"),
        }
    }

    #[test]
    fn test_invalid_json_in_app_metadata() {
        let mut data = create_custom_message_flight_data();
        data.app_metadata = bytes::Bytes::from_static(b"invalid json");
        let mut decoder = create_test_decoder();
        assert!(matches!(
            decoder.extract_message(data),
            Err(FlightError::DecodeError(_))
        ));
        assert!(matches!(
            decoder
                .extract_message(create_custom_message_flight_data())
                .unwrap(),
            Some(FlightMessage::CustomMessage(CustomMessage::ScanStats(_)))
        ));
    }
}
