// Copyright 2026 OpenObserve Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::{collections::VecDeque, pin::Pin, sync::Arc, task::Poll};

use arrow::{array::RecordBatch, ipc::writer::IpcWriteOptions};
use arrow_flight::{FlightData, error::FlightError};
use datafusion::execution::SendableRecordBatchStream;
use futures::{FutureExt, Stream, StreamExt, future::BoxFuture, ready};

use crate::{
    budget::FlightDataBudget,
    encoder::{
        ChunkCoalescer, FlightDataEncoder, encoded_size_bound, split_batch_for_grpc_response,
    },
};

type Encoding = BoxFuture<'static, (FlightDataEncoder, Result<Vec<FlightData>, FlightError>)>;

/// Coalesce one output partition, then reserve bytes before encoding each complete chunk.
/// Dictionary and record messages from a chunk must remain contiguous when partitions merge.
/// No byte permit is held while polling the input plan: repartition/barrier dependencies must
/// still be able to produce their first batch when another output partition is backpressured.
pub struct PartitionEncoderStream {
    inner: SendableRecordBatchStream,
    coalescer: ChunkCoalescer,
    encoder: Option<FlightDataEncoder>,
    chunks: VecDeque<RecordBatch>,
    encoding: Option<Encoding>,
    budget: Arc<FlightDataBudget>,
    max_flight_data_size: usize,
    inner_done: bool,
}

impl PartitionEncoderStream {
    pub fn new(
        inner: SendableRecordBatchStream,
        options: IpcWriteOptions,
        max_flight_data_size: usize,
        budget: Arc<FlightDataBudget>,
    ) -> Self {
        Self {
            inner,
            coalescer: ChunkCoalescer::new(),
            encoder: Some(FlightDataEncoder::new(options, max_flight_data_size)),
            chunks: VecDeque::new(),
            encoding: None,
            budget,
            max_flight_data_size,
            inner_done: false,
        }
    }
}

impl Stream for PartitionEncoderStream {
    type Item = Result<Vec<FlightData>, FlightError>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Some(encoding) = &mut this.encoding {
                let (encoder, result) = ready!(encoding.poll_unpin(cx));
                this.encoder = Some(encoder);
                this.encoding = None;
                return Poll::Ready(Some(result));
            }
            if let Some(chunk) = this.chunks.pop_front() {
                let budget = Arc::clone(&this.budget);
                let mut encoder = this.encoder.take().expect("one encoding at a time");
                let max_size = this.max_flight_data_size;
                this.encoding = Some(Box::pin(async move {
                    // Split once: the reservation bound and the encoder see the same slices.
                    let result = match split_batch_for_grpc_response(chunk, max_size) {
                        Ok(slices) => {
                            let reservation = budget.reserve(encoded_size_bound(&slices)).await;
                            encoder
                                .encode_slices(slices)
                                .and_then(|group| reservation.attach(group))
                        }
                        Err(e) => Err(e),
                    };
                    (encoder, result)
                }));
                continue;
            }
            if this.inner_done {
                return Poll::Ready(None);
            }
            match ready!(this.inner.poll_next_unpin(cx)) {
                Some(Ok(batch)) => match this.coalescer.push_batch(batch) {
                    Ok(chunks) => this.chunks.extend(chunks),
                    Err(e) => return Poll::Ready(Some(Err(e))),
                },
                Some(Err(e)) => {
                    return Poll::Ready(Some(Err(FlightError::ExternalError(Box::new(e)))));
                }
                None => {
                    this.inner_done = true;
                    match this.coalescer.finish() {
                        Ok(chunks) => this.chunks.extend(chunks),
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, time::Duration};

    use arrow::{
        array::{ArrayRef, DictionaryArray, Int32Array, StringArray, types::Int32Type},
        datatypes::{DataType, Field, Schema, SchemaRef},
        ipc::{MessageHeader, root_as_message},
    };
    use datafusion::{
        execution::TaskContext,
        physical_expr::{LexOrdering, PhysicalSortExpr, expressions::Column},
        physical_plan::{
            ExecutionPlan, collect,
            sorts::sort_preserving_merge::SortPreservingMergeExec,
            stream::RecordBatchStreamAdapter,
            streaming::{PartitionStream, StreamingTableExec},
        },
    };
    use tokio::{sync::mpsc, task::JoinSet};

    use super::*;

    fn input(batches: Vec<RecordBatch>) -> SendableRecordBatchStream {
        Box::pin(RecordBatchStreamAdapter::new(
            batches[0].schema(),
            futures::stream::iter(batches.into_iter().map(Ok)),
        ))
    }

    fn ints(start: i32, rows: i32) -> RecordBatch {
        RecordBatch::try_from_iter(vec![(
            "v",
            Arc::new(Int32Array::from_iter_values(start..start + rows)) as ArrayRef,
        )])
        .unwrap()
    }

    fn encoder(batches: Vec<RecordBatch>, budget: Arc<FlightDataBudget>) -> PartitionEncoderStream {
        PartitionEncoderStream::new(
            input(batches),
            IpcWriteOptions::default(),
            32 * 1024 * 1024,
            budget,
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn slow_consumer_bounds_queued_and_active_bytes() {
        let budget = Arc::new(FlightDataBudget::new(100_000));
        let (tx, mut rx) = mpsc::channel(8);
        let mut tasks = JoinSet::new();
        for p in 0..4 {
            let tx = tx.clone();
            let budget = Arc::clone(&budget);
            tasks.spawn(async move {
                let mut stream = encoder(vec![ints(p * 8192, 8192); 6], budget);
                while let Some(group) = stream.next().await {
                    if tx.send(group.unwrap()).await.is_err() {
                        break;
                    }
                }
            });
        }
        drop(tx);
        let first = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(budget.retained_bytes() > 0);
        let mut rows = first
            .iter()
            .map(|data| {
                root_as_message(&data.data_header)
                    .unwrap()
                    .header_as_record_batch()
                    .unwrap()
                    .length()
            })
            .sum::<i64>();
        drop(first);
        tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(group) = rx.recv().await {
                for data in &group {
                    rows += root_as_message(&data.data_header)
                        .unwrap()
                        .header_as_record_batch()
                        .unwrap()
                        .length();
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
                drop(group);
            }
            while let Some(result) = tasks.join_next().await {
                result.unwrap();
            }
        })
        .await
        .unwrap();
        assert_eq!(rows, 4 * 6 * 8192);
        assert!(budget.peak_bytes() <= 100_000);
        assert_eq!(budget.retained_bytes(), 0);
    }

    #[tokio::test]
    async fn large_input_batch_is_encoded_one_chunk_at_a_time() {
        let budget = Arc::new(FlightDataBudget::new(40_000));
        let mut stream = encoder(vec![ints(0, 8 * 8192)], Arc::clone(&budget));
        let first = stream.next().await.unwrap().unwrap();
        let first_bytes = first
            .iter()
            .map(crate::budget::flight_data_size)
            .sum::<usize>();
        assert_eq!(budget.retained_bytes(), first_bytes);
        assert!(stream.next().now_or_never().is_none());
        drop(first);
        let second = stream.next().await.unwrap().unwrap();
        drop(second);
        drop(stream);
        assert_eq!(budget.retained_bytes(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn disconnect_and_task_abort_release_waiting_and_queued_groups() {
        let budget = Arc::new(FlightDataBudget::new(40_000));
        let (tx, rx) = mpsc::channel(2);
        let mut tasks = JoinSet::new();
        for _ in 0..3 {
            let tx = tx.clone();
            let budget = Arc::clone(&budget);
            tasks.spawn(async move {
                let mut stream = encoder(vec![ints(0, 8192); 8], budget);
                while let Some(group) = stream.next().await {
                    if tx.send(group.unwrap()).await.is_err() {
                        break;
                    }
                }
            });
        }
        drop(tx);
        tokio::time::sleep(Duration::from_millis(20)).await;
        drop(rx);
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        assert_eq!(budget.retained_bytes(), 0);
        let reservation = tokio::time::timeout(Duration::from_secs(1), budget.reserve(40_000))
            .await
            .unwrap();
        drop(reservation);
    }

    #[tokio::test]
    async fn oversized_dictionary_groups_roundtrip_without_interleaving() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "label",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        )]));
        let budget = Arc::new(FlightDataBudget::new(64));
        let mut streams = Vec::new();
        for label in [
            "partition-zero-wide-dictionary",
            "partition-one-different-dictionary",
        ] {
            let array = DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![Some(0), None, Some(0)]),
                Arc::new(StringArray::from(vec![label])),
            )
            .unwrap();
            let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();
            streams.push((encoder(vec![batch.clone()], Arc::clone(&budget)), batch));
        }
        let mut dictionaries = HashMap::new();
        for (mut stream, expected) in streams {
            let group = tokio::time::timeout(Duration::from_secs(1), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert!(budget.retained_bytes() > 64);
            let mut decoded = Vec::new();
            for data in &group {
                let message = root_as_message(&data.data_header).unwrap();
                match message.header_type() {
                    MessageHeader::DictionaryBatch => {
                        arrow::ipc::reader::read_dictionary(
                            &arrow::buffer::Buffer::from(data.data_body.as_ref()),
                            message.header_as_dictionary_batch().unwrap(),
                            &schema,
                            &mut dictionaries,
                            &message.version(),
                        )
                        .unwrap();
                    }
                    MessageHeader::RecordBatch => decoded.push(
                        arrow_flight::utils::flight_data_to_arrow_batch(
                            data,
                            schema.clone(),
                            &dictionaries,
                        )
                        .unwrap(),
                    ),
                    other => panic!("unexpected message: {other:?}"),
                }
            }
            assert_eq!(decoded, vec![expected]);
            drop(group);
            assert_eq!(budget.retained_bytes(), 0);
        }
    }

    #[tokio::test]
    async fn differential_wide_nullable_views_and_slices() {
        use arrow::array::StringViewArray;

        for width in [0, 13, 1024] {
            let mut random = 0x936adbecu64;
            let strings: Vec<Option<String>> = (0..103)
                .map(|row| {
                    (row % 7 != 0).then(|| {
                        (0..width)
                            .map(|_| {
                                random ^= random << 13;
                                random ^= random >> 7;
                                random ^= random << 17;
                                (b'a' + (random % 26) as u8) as char
                            })
                            .collect()
                    })
                })
                .collect();
            for view in [false, true] {
                let values: ArrayRef = if view {
                    Arc::new(StringViewArray::from_iter(
                        strings.iter().map(|s| s.as_deref()),
                    ))
                } else {
                    Arc::new(StringArray::from_iter(strings.iter().map(|s| s.as_deref())))
                };
                let batch = RecordBatch::try_from_iter(vec![("s", values)]).unwrap();
                for compression in [None, Some(arrow::ipc::CompressionType::ZSTD)] {
                    for max_size in [0, 4096, 32 * 1024 * 1024] {
                        let options = IpcWriteOptions::default()
                            .try_with_compression(compression)
                            .unwrap();
                        let mut coalescer = ChunkCoalescer::new();
                        let mut chunks = coalescer.push_batch(batch.clone()).unwrap();
                        chunks.extend(coalescer.finish().unwrap());
                        let expected = FlightDataEncoder::new(options.clone(), max_size)
                            .encode_chunk(chunks.pop().unwrap())
                            .unwrap();
                        let budget = Arc::new(FlightDataBudget::new(4096));
                        let mut stream = PartitionEncoderStream::new(
                            input(vec![batch.clone()]),
                            options,
                            max_size,
                            Arc::clone(&budget),
                        );
                        let actual = stream.next().await.unwrap().unwrap();
                        assert_eq!(actual, expected);
                        drop(actual);
                        assert!(stream.next().await.is_none());
                        assert_eq!(budget.retained_bytes(), 0);
                    }
                }
            }
        }
    }

    #[derive(Debug)]
    struct FlightPartition {
        schema: SchemaRef,
        start: i32,
        budget: Arc<FlightDataBudget>,
    }

    impl PartitionStream for FlightPartition {
        fn schema(&self) -> &SchemaRef {
            &self.schema
        }

        fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
            let schema = self.schema.clone();
            let stream = encoder(vec![ints(self.start, 8192); 1], Arc::clone(&self.budget));
            let decoded = stream.map(move |group| {
                let group =
                    group.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
                // The real network serializes the sender Bytes before leader decoding. Copy here
                // too: sender leases must never follow zero-copy Arrow buffers into leader
                // operators.
                let fd = &group[0];
                let wire = FlightData::new()
                    .with_data_header(fd.data_header.to_vec())
                    .with_data_body(fd.data_body.to_vec());
                arrow_flight::utils::flight_data_to_arrow_batch(
                    &wire,
                    schema.clone(),
                    &HashMap::new(),
                )
                .map_err(Into::into)
            });
            Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), decoded))
        }
    }

    #[tokio::test]
    async fn sort_preserving_merge_gets_every_inputs_first_batch() {
        let schema = ints(0, 1).schema();
        let budgets: Vec<_> = (0..4)
            .map(|_| Arc::new(FlightDataBudget::new(64)))
            .collect();
        let partitions: Vec<Arc<dyn PartitionStream>> = budgets
            .iter()
            .enumerate()
            .map(|(p, budget)| {
                Arc::new(FlightPartition {
                    schema: schema.clone(),
                    start: p as i32 * 8192,
                    budget: Arc::clone(budget),
                }) as Arc<dyn PartitionStream>
            })
            .collect();
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new_default(Arc::new(
            Column::new("v", 0),
        ))])
        .unwrap();
        let source = StreamingTableExec::try_new(
            schema,
            partitions,
            None,
            vec![ordering.clone()],
            false,
            None,
        )
        .unwrap();
        let merge: Arc<dyn ExecutionPlan> =
            Arc::new(SortPreservingMergeExec::new(ordering, Arc::new(source)));
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            collect(merge, Arc::new(TaskContext::default())),
        )
        .await
        .unwrap()
        .unwrap();
        let values: Vec<i32> = result
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(values, (0..4 * 8192).collect::<Vec<_>>());
        assert!(budgets.iter().all(|budget| budget.retained_bytes() == 0));
    }
}
