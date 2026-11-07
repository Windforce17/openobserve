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

use std::{io::Cursor, sync::Arc};

use arrow::ipc::{CompressionType, writer::IpcWriteOptions};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    flight_service_server::FlightService,
};
use config::{
    cluster::LOCAL_NODE, datafusion::request::FlightSearchRequest, meta::search::ScanStats,
};
use datafusion::{
    common::{DataFusionError, Result},
    physical_plan::execute_stream_partitioned,
};
use flight::common::{MetricsInfo, PreCustomMessage};
use futures::{StreamExt, stream::BoxStream};
use futures_util::pin_mut;
use prost::Message;
use tonic::{Request, Response, Status, Streaming};
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;
#[cfg(feature = "enterprise")]
use {
    crate::service::search::SEARCH_SERVER,
    o2_enterprise::enterprise::{common::config::get_config as get_o2_config, search::TaskStatus},
};

use crate::{
    handler::grpc::{
        MetadataMap,
        flight::{
            stream::FlightEncoderStreamBuilder,
            visitor::{
                get_cluster_metrics, get_partial_err, get_peak_memory, get_peak_memory_from_ctx,
                get_scan_stats,
            },
        },
    },
    service::search::{
        grpc::flight as grpcFlight,
        inspector::{SearchInspectorFieldsBuilder, search_inspector_fields},
        work_group::DeferredLock,
    },
};

mod partition_encoder;
mod stream;
pub mod visitor;

#[derive(Default)]
pub struct FlightServiceImpl;

#[tonic::async_trait]
impl FlightService for FlightServiceImpl {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let _start = std::time::Instant::now();
        let cfg = config::get_config();

        let parent_cx = opentelemetry::global::get_text_map_propagator(|prop| {
            prop.extract(&MetadataMap(request.metadata()))
        });
        let span = tracing::info_span!("grpc:search:flight:do_get");
        let _ = span.set_parent(parent_cx);

        // Tonic's header covers waiting for response headers, not just batches.
        let rpc_timeout = request
            .metadata()
            .get("grpc-timeout")
            .and_then(|v| v.to_str().ok())
            .and_then(parse_grpc_timeout);

        // decode ticket to RemoteExecNode
        let ticket = request.into_inner();
        let mut buf = Cursor::new(ticket.ticket);
        let req = proto::cluster_rpc::FlightSearchRequest::decode(&mut buf)
            .map_err(|e| DataFusionError::Internal(format!("{e:?}")))
            .map_err(|e| Status::internal(e.to_string()))?;

        let req: FlightSearchRequest = req.into();
        let trace_id = format!(
            "{}-{}",
            req.query_identifier.trace_id, req.query_identifier.job_id
        );
        let is_super_cluster = req.super_cluster_info.is_super_cluster;
        let timeout = req.search_info.timeout as u64;
        log::info!("[trace_id {trace_id}] flight->search: do_get, timeout: {timeout}s",);

        let query_timeout = std::time::Duration::from_secs(if timeout == 0 {
            cfg.limit.query_timeout
        } else {
            timeout
        });
        let admission_wait = config::search_admission::wait_budget(
            rpc_timeout.map_or(query_timeout, |rpc| rpc.min(query_timeout)),
            _start.elapsed(),
            (cfg.limit.query_redispatch_enabled && !is_super_cluster)
                .then(|| std::time::Duration::from_millis(cfg.limit.query_redispatch_min_delay_ms)),
        );

        // Note: all async should in this place, otherwise it will break tracing
        // https://docs.rs/tracing/latest/tracing/span/struct.Span.html#in-asynchronous-code
        let req_move = req.clone();
        let trace_id_move = trace_id.clone();
        let result = async move {
            #[cfg(feature = "enterprise")]
            if is_super_cluster && !SEARCH_SERVER.contain_key(&trace_id_move).await {
                // this is for work_group check in super cluster follower leader
                SEARCH_SERVER
                    .insert(
                        trace_id_move.clone(),
                        TaskStatus::new_follower(vec![], false),
                    )
                    .await;
            }

            let result = get_ctx_and_physical_plan(&trace_id_move, &req_move, admission_wait).await;

            #[cfg(feature = "enterprise")]
            if is_super_cluster && !SEARCH_SERVER.is_leader(&trace_id_move).await {
                // this is for work_group check in super cluster follower leader
                SEARCH_SERVER.remove(&trace_id_move, false).await;
            }

            result
        }
        .instrument(span.clone())
        .await;

        // NOT plan construction alone: this spans the whole follower search
        // setup — plan decode, schema maps, the vix INDEX EVALUATION, table
        // registration and the segments scan. The per-stage durations are
        // in the "storage search completed" / segments-scan lines; read
        // those before attributing time to "planning" (#45 was misdiagnosed
        // off this label on 2026-08-13).
        log::info!(
            "{}",
            search_inspector_fields(
                format!(
                    "[trace_id {trace_id}] flight->do_get: follower search setup (plan decode + \
                     index eval + table registration) took: {} ms",
                    _start.elapsed().as_millis(),
                ),
                SearchInspectorFieldsBuilder::new()
                    .trace_id(trace_id.to_string())
                    .node_name(LOCAL_NODE.name.clone())
                    .component("flight::do_get follower search setup".to_string())
                    .search_role("follower".to_string())
                    .duration(_start.elapsed().as_millis() as usize)
                    .build()
            )
        );

        // prepare dataufion context
        let (ctx, plan, lock, scan_stats, storage_shortfall, admission) = match result {
            Ok(v) => v,
            Err(e) => {
                clear_session_data(&trace_id);
                #[cfg(feature = "enterprise")]
                if get_o2_config().work_group.max_nodes_per_query > 0 {
                    o2_enterprise::enterprise::search::admission::ledger::release(&trace_id);
                    log::error!(
                        "[trace_id {trace_id}] flight->search: do_get physical plan generate error: {e:?}",
                    );
                }
                return Err(Status::internal(e.to_string()));
            }
        };

        log::info!(
            "[trace_id {trace_id}] flight->search: executing stream, is super cluster: {is_super_cluster}"
        );

        if cfg.common.print_key_sql {
            log::info!(
                "[trace_id {trace_id}] follow physical plan, is_super_cluster_follower_leader: {is_super_cluster}"
            );
            log::info!(
                "{}",
                config::meta::plan::generate_plan_string(&trace_id, plan.as_ref())
            );
        }

        let start = std::time::Instant::now();
        let write_options: IpcWriteOptions = IpcWriteOptions::default()
            .try_with_compression(Some(CompressionType::ZSTD))
            .map_err(|e| {
                // clear session data
                clear_session_data(&trace_id);
                log::error!(
                    "[trace_id {trace_id}] flight->search: do_get create IPC write options error: {e:?}",
                );
                Status::internal(e.to_string())
            })?;

        // used for EXPLAIN ANALYZE to collect metrics after stream is done
        let metrics = req.search_info.is_analyze.then_some(MetricsInfo {
            plan: plan.clone(),
            is_super_cluster,
            func: Box::new(super_cluster_enabled),
        });

        // Get the peak memory usage from the memory pool
        // Note: We get peak memory after stream execution, so we pass it via a shared reference
        let peak_memory = get_peak_memory_from_ctx(&ctx);

        // used for super cluster follower leader to get information from follower node
        let scan_stats_ref = get_scan_stats(&plan);
        let metrics_ref = get_cluster_metrics(&plan);
        let peak_memory_ref = get_peak_memory(&plan);
        // A storage scan branch truncated by ZO_STORAGE_SCAN_MAX_BYTES is a
        // partial result the leader must learn about even if the stream later
        // dies: merge it into the early partial-error message.
        let partial_err_ref = match (get_partial_err(&plan), storage_shortfall) {
            (Some(existing), Some(shortfall)) => {
                let mut guard = existing.lock();
                if !guard.is_empty() {
                    guard.push_str("; ");
                }
                guard.push_str(&shortfall);
                drop(guard);
                Some(existing)
            }
            (None, Some(shortfall)) => Some(Arc::new(parking_lot::Mutex::new(shortfall))),
            (existing, None) => existing,
        };

        // One stream per output partition so they encode in parallel
        let streams =
            execute_stream_partitioned(plan, ctx.task_ctx()).map_err(|e| {
                clear_session_data(&trace_id);
                #[cfg(feature = "enterprise")]
                if get_o2_config().work_group.max_nodes_per_query > 0 {
                    o2_enterprise::enterprise::search::admission::ledger::release(&trace_id);
                    log::error!(
                        "[trace_id {trace_id}] flight->search: do_get physical plan execution error: {e:?}",
                    );
                }
                Status::internal(e.to_string())
            })?;

        // Parallel encoders may still be dropping after response cancellation.
        // Each producer retains the permit until its own input stream is gone.
        let streams = streams
            .into_iter()
            .map(|input| {
                if let Some(guard) = &admission {
                    Box::pin(AdmittedRecordBatchStream {
                        inner: input,
                        _admission: guard.clone(),
                    }) as datafusion::execution::SendableRecordBatchStream
                } else {
                    input
                }
            })
            .collect();

        let mut stream = FlightEncoderStreamBuilder::new(write_options, 33554432)
            .with_trace_id(trace_id.to_string())
            .with_is_super(is_super_cluster)
            .with_defer_lock(lock)
            .with_start(start)
            .with_custom_message(PreCustomMessage::ScanStats(scan_stats))
            .with_custom_message(PreCustomMessage::ScanStatsRef(scan_stats_ref))
            .with_custom_message(PreCustomMessage::Metrics(metrics))
            .with_custom_message(PreCustomMessage::MetricsRef(metrics_ref))
            .with_custom_message(PreCustomMessage::PeakMemoryRef(Some(peak_memory)))
            .with_custom_message(PreCustomMessage::PeakMemoryRef(peak_memory_ref))
            .with_custom_message(PreCustomMessage::PartialErrRefEarly(
                partial_err_ref.clone(),
            ))
            .with_custom_message(PreCustomMessage::PartialErrRef(partial_err_ref))
            .build(streams, span);

        let stream = async_stream::stream! {
            let _admission = admission;
            let timeout = tokio::time::sleep(tokio::time::Duration::from_secs(timeout));
            pin_mut!(timeout);
            loop {
                tokio::select! {
                    batch = stream.next() => {
                        if let Some(batch) = batch {
                            yield batch
                        } else {
                            break;
                        }
                    }
                    _ = &mut timeout => {
                        log::info!("[trace_id {trace_id}] flight->search: timeout");
                        break;
                    }
                }
            }
        };

        Ok(Response::new(Box::pin(stream) as Self::DoGetStream))
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Implement handshake"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Implement list_flights"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Implement get_flight_info"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("Implement poll_flight_info"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("Implement get_schema"))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("Implement do_put"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("Implement do_action"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Implement list_actions"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Implement do_exchange"))
    }
}

fn parse_grpc_timeout(value: &str) -> Option<std::time::Duration> {
    // gRPC timeout: one to eight decimal digits followed by H/M/S/m/u/n.
    if !value.is_ascii() || !(2..=9).contains(&value.len()) {
        return None;
    }
    let (digits, unit) = value.split_at(value.len() - 1);
    if !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let n = digits.parse::<u64>().ok()?;
    use std::time::Duration;
    match unit {
        "H" => Some(Duration::from_secs(n * 3600)),
        "M" => Some(Duration::from_secs(n * 60)),
        "S" => Some(Duration::from_secs(n)),
        "m" => Some(Duration::from_millis(n)),
        "u" => Some(Duration::from_micros(n)),
        "n" => Some(Duration::from_nanos(n)),
        _ => None,
    }
}

// Fields drop in declaration order: release the input before its admission.
struct AdmittedRecordBatchStream {
    inner: datafusion::execution::SendableRecordBatchStream,
    _admission: Arc<config::search_admission::AdmissionGuard>,
}

impl futures::Stream for AdmittedRecordBatchStream {
    type Item = Result<arrow::array::RecordBatch>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.get_mut().inner.as_mut().poll_next(cx)
    }
}

impl datafusion::physical_plan::RecordBatchStream for AdmittedRecordBatchStream {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.inner.schema()
    }
}

type PlanResult = (
    datafusion::prelude::SessionContext,
    Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    Option<DeferredLock>,
    ScanStats,
    // partial-results message of a truncated storage scan branch
    Option<String>,
    Option<Arc<config::search_admission::AdmissionGuard>>,
);

#[cfg(feature = "enterprise")]
#[tracing::instrument(name = "service:search:grpc:flight::enter", skip_all)]
async fn get_ctx_and_physical_plan(
    trace_id: &str,
    req: &FlightSearchRequest,
    admission_wait: std::time::Duration,
) -> Result<PlanResult, infra::errors::Error> {
    if req.super_cluster_info.is_super_cluster {
        let (ctx, physical_plan, lock, scan_stats) =
            crate::service::search::super_cluster::follower::search(trace_id, req).await?;
        Ok((ctx, physical_plan, Some(lock), scan_stats, None, None))
    } else {
        let admission = Arc::new(
            config::search_admission::enter(
                config::search_admission::ScanWork {
                    files: req.search_info.file_id_list.len(),
                },
                admission_wait,
            )
            .await,
        );
        let (ctx, physical_plan, scan_stats, storage_shortfall) =
            grpcFlight::search(trace_id, req).await?;
        Ok((
            ctx,
            physical_plan,
            None,
            scan_stats,
            storage_shortfall,
            Some(admission),
        ))
    }
}

#[cfg(not(feature = "enterprise"))]
#[tracing::instrument(name = "service:search:grpc:flight::enter", skip_all)]
async fn get_ctx_and_physical_plan(
    trace_id: &str,
    req: &FlightSearchRequest,
    admission_wait: std::time::Duration,
) -> Result<PlanResult, infra::errors::Error> {
    let admission = Arc::new(
        config::search_admission::enter(
            config::search_admission::ScanWork {
                files: req.search_info.file_id_list.len(),
            },
            admission_wait,
        )
        .await,
    );
    let (ctx, physical_plan, scan_stats, storage_shortfall) =
        grpcFlight::search(trace_id, req).await?;
    Ok((
        ctx,
        physical_plan,
        None,
        scan_stats,
        storage_shortfall,
        Some(admission),
    ))
}

fn clear_session_data(trace_id: &str) {
    // clear session data
    crate::service::search::datafusion::storage::file_list::clear(trace_id);
    // release wal lock files
    crate::common::infra::wal::release_request(trace_id);
    log::info!("Cleared session for trace_id: {trace_id}");
}

fn super_cluster_enabled() -> bool {
    #[cfg(feature = "enterprise")]
    if get_o2_config().super_cluster.enabled {
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grpc_timeout_header_preserves_all_units_and_rejects_invalid_values() {
        use std::time::Duration;
        for (input, expected) in [
            ("1H", Duration::from_secs(3600)),
            ("1M", Duration::from_secs(60)),
            ("200S", Duration::from_secs(200)),
            ("10m", Duration::from_millis(10)),
            ("10u", Duration::from_micros(10)),
            ("10n", Duration::from_nanos(10)),
            ("0n", Duration::ZERO),
            ("99999999H", Duration::from_secs(359999996400)),
        ] {
            assert_eq!(parse_grpc_timeout(input), Some(expected));
        }
        for value in ["", "S", "1s", "+1S", "1.5S", "123456789S", "1µ", "-1S"] {
            assert_eq!(parse_grpc_timeout(value), None);
        }
    }

    #[tokio::test]
    async fn admission_lives_until_last_partition_is_dropped() {
        let guard = Arc::new(
            config::search_admission::enter(
                config::search_admission::ScanWork::default(),
                std::time::Duration::ZERO,
            )
            .await,
        );
        let weak = Arc::downgrade(&guard);
        let input = || {
            Box::pin(
                datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                    Arc::new(arrow::datatypes::Schema::empty()),
                    futures::stream::pending::<Result<arrow::array::RecordBatch>>(),
                ),
            ) as datafusion::execution::SendableRecordBatchStream
        };
        let first = AdmittedRecordBatchStream {
            inner: input(),
            _admission: guard.clone(),
        };
        let second = AdmittedRecordBatchStream {
            inner: input(),
            _admission: guard.clone(),
        };
        drop(guard);
        drop(first);
        assert!(weak.upgrade().is_some());
        drop(second); // even an unpolled stream owns its permit
        assert!(weak.upgrade().is_none());
    }

    #[tokio::test]
    async fn admission_wrapper_preserves_batches_and_errors() {
        let guard = Arc::new(
            config::search_admission::enter(
                config::search_admission::ScanWork::default(),
                std::time::Duration::ZERO,
            )
            .await,
        );
        let weak = Arc::downgrade(&guard);
        let schema = Arc::new(arrow::datatypes::Schema::empty());
        let batch = arrow::array::RecordBatch::new_empty(schema.clone());
        let input = Box::pin(
            datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                schema,
                futures::stream::iter(vec![
                    Ok(batch.clone()),
                    Err(DataFusionError::Execution("test".into())),
                ]),
            ),
        );
        let mut stream = AdmittedRecordBatchStream {
            inner: input,
            _admission: guard,
        };
        assert_eq!(stream.next().await.unwrap().unwrap(), batch);
        assert!(stream.next().await.unwrap().is_err());
        assert!(stream.next().await.is_none());
        drop(stream);
        assert!(weak.upgrade().is_none());
    }

    #[tokio::test]
    async fn admission_stays_with_producer_until_cancellation_finishes() {
        let guard = Arc::new(
            config::search_admission::enter(
                config::search_admission::ScanWork::default(),
                std::time::Duration::ZERO,
            )
            .await,
        );
        let weak = Arc::downgrade(&guard);
        let stream = AdmittedRecordBatchStream {
            inner: Box::pin(
                datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                    Arc::new(arrow::datatypes::Schema::empty()),
                    futures::stream::pending::<Result<arrow::array::RecordBatch>>(),
                ),
            ),
            _admission: guard.clone(),
        };
        let (tx, rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let mut stream = stream;
            tx.send(()).unwrap();
            stream.next().await;
        });
        rx.await.unwrap();
        drop(guard); // The response disappeared before the producer finished.
        assert!(weak.upgrade().is_some());
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(weak.upgrade().is_none());
    }

    #[test]
    #[ignore = "release wrapper microbenchmark; run with --nocapture"]
    fn admission_stream_wrapper_microbenchmark() {
        use std::{
            hint::black_box,
            time::{Duration, Instant},
        };
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let schema = Arc::new(arrow::datatypes::Schema::empty());
        rt.block_on(async {
            for partitions in [1, 16, 64] {
                for round in 0..8 {
                    for admitted in if round % 2 == 0 { [false, true] } else { [true, false] } {
                        let start = Instant::now();
                        for _ in 0..10_000 {
                            let guard = if admitted {
                                Some(Arc::new(config::search_admission::enter(
                                    config::search_admission::ScanWork::default(), Duration::ZERO,
                                ).await))
                            } else { None };
                            let streams = (0..partitions).map(|_| {
                                let input = Box::pin(datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                                    schema.clone(), futures::stream::empty::<Result<arrow::array::RecordBatch>>(),
                                )) as datafusion::execution::SendableRecordBatchStream;
                                if let Some(guard) = &guard {
                                    Box::pin(AdmittedRecordBatchStream { inner: input, _admission: guard.clone() })
                                        as datafusion::execution::SendableRecordBatchStream
                                } else { input }
                            }).collect::<Vec<_>>();
                            black_box(&streams);
                            drop(streams);
                            drop(guard);
                        }
                        println!("WRAPPER partitions={partitions} round={round} mode={} n=10000 elapsed_ns={}", if admitted { "admitted" } else { "baseline" }, start.elapsed().as_nanos());
                    }
                }
            }
        });
    }

    #[test]
    fn test_flight_service_impl_default() {
        let _impl = FlightServiceImpl;
    }

    #[test]
    fn test_super_cluster_disabled_in_oss() {
        assert!(!super_cluster_enabled());
    }
}
