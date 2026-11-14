// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    ipc::writer::IpcWriteOptions,
};
use arrow_flight::{
    self as af,
    flight_service_server::{FlightService, FlightServiceServer},
};
use datafusion::{
    datasource::memory::MemorySourceConfig,
    execution::context::SessionContext,
    functions_aggregate::count::count_udaf,
    physical_expr::aggregate::AggregateExprBuilder,
    physical_plan::{
        aggregates::PhysicalGroupBy, coalesce_partitions::CoalescePartitionsExec, collect,
        expressions::lit,
    },
};
use prost::Message;
use tonic::{Request, Response, Status};

use super::*;
use crate::datafusion::distributed_plan::{NewEmptyExecVisitor, ReplaceTableScanExec};

#[path = "redundant_repartition_tests.rs"]
mod redundant_repartition_tests;

type RpcStream<T> = Pin<Box<dyn futures::Stream<Item = std::result::Result<T, Status>> + Send>>;

#[derive(Clone, Default)]
struct LocalFlight {
    requests: Arc<Mutex<Vec<proto::cluster_rpc::FlightSearchRequest>>>,
    data: Arc<Mutex<Vec<Vec<RecordBatch>>>>,
}

#[tonic::async_trait]
impl FlightService for LocalFlight {
    type HandshakeStream = RpcStream<af::HandshakeResponse>;
    type ListFlightsStream = RpcStream<af::FlightInfo>;
    type DoGetStream = RpcStream<af::FlightData>;
    type DoPutStream = RpcStream<af::PutResult>;
    type DoExchangeStream = RpcStream<af::FlightData>;
    type DoActionStream = RpcStream<af::Result>;
    type ListActionsStream = RpcStream<af::ActionType>;

    async fn do_get(
        &self,
        request: Request<af::Ticket>,
    ) -> std::result::Result<Response<Self::DoGetStream>, Status> {
        let request = proto::cluster_rpc::FlightSearchRequest::decode(request.into_inner().ticket)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        self.requests.lock().push(request.clone());
        let ctx = SessionContext::new();
        let query = request.query_identifier.as_ref().unwrap();
        let info = request.search_info.as_ref().unwrap();
        let mut plan = datafusion_proto::bytes::physical_plan_from_bytes_with_extension_codec(
            &info.plan,
            &ctx.task_ctx(),
            &get_physical_extension_codec(),
        )
        .map_err(|e| Status::internal(e.to_string()))?;
        let mut visitor = NewEmptyExecVisitor::new();
        plan.visit(&mut visitor)
            .map_err(|e| Status::internal(e.to_string()))?;
        let scan = visitor.plan();
        let stream_type = TableReference::from(scan.name())
            .get_stream_type(StreamType::from(query.stream_type.as_str()));
        let data = self.data.lock().get(query.partition as usize).cloned();
        if let Some(data) = data {
            let replacement = MemorySourceConfig::try_new_exec(
                &[data],
                scan.full_schema(),
                scan.projection().cloned(),
            )
            .map_err(|e| Status::internal(e.to_string()))?;
            plan = plan
                .rewrite(&mut ReplaceTableScanExec::new(replacement))
                .map_err(|e| Status::internal(e.to_string()))?
                .data;
        } else if query.enrich_mode && stream_type == StreamType::EnrichmentTables {
            // Model the follower's enrichment branch without an external metadata database.
            let batch =
                RecordBatch::try_new(scan.schema(), vec![Arc::new(Int64Array::from(vec![42]))])
                    .map_err(|e| Status::internal(e.to_string()))?;
            let replacement = MemorySourceConfig::try_new_exec(&[vec![batch]], scan.schema(), None)
                .map_err(|e| Status::internal(e.to_string()))?;
            plan = plan
                .rewrite(&mut ReplaceTableScanExec::new(replacement))
                .map_err(|e| Status::internal(e.to_string()))?
                .data;
        }
        let batches = collect(plan, ctx.task_ctx())
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let mut encoder = flight::encoder::FlightDataEncoder::new(IpcWriteOptions::default(), 0);
        let mut messages = Vec::new();
        for batch in batches {
            messages.extend(
                encoder
                    .encode_chunk(batch)
                    .map_err(|e| Status::internal(e.to_string()))?,
            );
        }
        let mut response = Response::new(Box::pin(futures::stream::iter(
            messages.into_iter().map(Ok),
        )) as Self::DoGetStream);
        flight::engine::set_generation(response.metadata_mut());
        Ok(response)
    }

    async fn handshake(
        &self,
        _: Request<tonic::Streaming<af::HandshakeRequest>>,
    ) -> std::result::Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn list_flights(
        &self,
        _: Request<af::Criteria>,
    ) -> std::result::Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn get_flight_info(
        &self,
        _: Request<af::FlightDescriptor>,
    ) -> std::result::Result<Response<af::FlightInfo>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn poll_flight_info(
        &self,
        _: Request<af::FlightDescriptor>,
    ) -> std::result::Result<Response<af::PollInfo>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn get_schema(
        &self,
        _: Request<af::FlightDescriptor>,
    ) -> std::result::Result<Response<af::SchemaResult>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn do_put(
        &self,
        _: Request<tonic::Streaming<af::FlightData>>,
    ) -> std::result::Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn do_exchange(
        &self,
        _: Request<tonic::Streaming<af::FlightData>>,
    ) -> std::result::Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn do_action(
        &self,
        _: Request<af::Action>,
    ) -> std::result::Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn list_actions(
        &self,
        _: Request<af::Empty>,
    ) -> std::result::Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("unused"))
    }
}

#[derive(Debug)]
struct RpcNode {
    address: String,
    roles: Vec<config::meta::cluster::Role>,
}

impl NodeInfo for RpcNode {
    fn is_querier(&self) -> bool {
        self.roles.contains(&config::meta::cluster::Role::Querier)
    }
    fn is_ingester(&self) -> bool {
        self.roles.contains(&config::meta::cluster::Role::Ingester)
    }
    fn get_grpc_addr(&self) -> String {
        self.address.clone()
    }
    fn get_auth_token(&self) -> String {
        "test-token".to_string()
    }
    fn get_name(&self) -> String {
        "idle-enrich-test".to_string()
    }
    fn query_engine_generation(&self) -> Option<&str> {
        Some(config::meta::cluster::QUERY_ENGINE_GENERATION)
    }
    fn is_local(&self) -> bool {
        false
    }
}

struct Server {
    address: String,
    service: LocalFlight,
    stop: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<std::result::Result<(), tonic::transport::Error>>,
}

impl Server {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let incoming = futures::stream::unfold(listener, |listener| async move {
            Some((listener.accept().await.map(|(socket, _)| socket), listener))
        });
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let service = LocalFlight::default();
        let rpc = FlightServiceServer::new(service.clone());
        let task = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(rpc)
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = stopped.await;
                }),
        );
        Self {
            address,
            service,
            stop,
            task,
        }
    }

    fn node(&self, roles: &[config::meta::cluster::Role], count: usize) -> RemoteScanNode {
        let nodes: Vec<Arc<dyn NodeInfo>> = (0..count)
            .map(|_| {
                Arc::new(RpcNode {
                    address: self.address.clone(),
                    roles: roles.to_vec(),
                }) as Arc<dyn NodeInfo>
            })
            .collect();
        let mut node = RemoteScanNode {
            nodes: nodes.into(),
            ..Default::default()
        };
        node.query_identifier.org_id = "test".to_string();
        node.query_identifier.stream_type = "logs".to_string();
        node.search_infos.timeout = 5;
        node.search_infos.file_id_list = vec![vec![]; count].into();
        node
    }

    fn take_requests(&self) -> Vec<proto::cluster_rpc::FlightSearchRequest> {
        std::mem::take(&mut *self.service.requests.lock())
    }

    async fn finish(self) {
        let _ = self.stop.send(());
        tokio::time::timeout(Duration::from_secs(5), self.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}

fn scan(name: &str) -> Arc<dyn ExecutionPlan> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    Arc::new(NewEmptyExec::new(
        name,
        schema.clone(),
        None,
        &[],
        None,
        false,
        schema,
    ))
}

fn remote(
    input: Arc<dyn ExecutionPlan>,
    node: RemoteScanNode,
    enabled: bool,
) -> Arc<RemoteScanExec> {
    Arc::new(RemoteScanExec::new_with_skip_idle_enrich_node(input, node, enabled).unwrap())
}

async fn run(exec: Arc<RemoteScanExec>) -> Vec<RecordBatch> {
    let batches = collect(exec.clone(), SessionContext::new().task_ctx())
        .await
        .unwrap();
    let partial_err = exec.partial_err.lock().clone();
    assert!(partial_err.is_empty(), "{partial_err}");
    batches
}

#[tokio::test]
async fn idle_enrich_plain_scan_skips_rpc_and_disabled_keeps_one() {
    let server = Server::start().await;
    for enabled in [false, true] {
        let node = server.node(&[config::meta::cluster::Role::Querier], 3);
        let exec = remote(scan("logs.events"), node, enabled);
        let participants = exec.redispatch.participants;
        let before = config::metrics::QUERY_EMPTY_QUERIER_REQUESTS_SKIPPED_TOTAL.get();
        assert!(run(exec).await.is_empty());
        assert_eq!(server.take_requests().len(), usize::from(!enabled));
        assert_eq!(participants, usize::from(!enabled));
        assert!(
            config::metrics::QUERY_EMPTY_QUERIER_REQUESTS_SKIPPED_TOTAL.get()
                >= before + if enabled { 3 } else { 2 }
        );
    }
    server.finish().await;
}

#[tokio::test]
async fn idle_enrich_enrichment_alias_and_request_type_keep_data() {
    let server = Server::start().await;
    for (name, stream_type) in [
        ("enrichment_tables.lookup", "logs"),
        ("enrich.lookup", "logs"),
        ("lookup", "enrichment_tables"),
    ] {
        for enabled in [false, true] {
            let mut node = server.node(&[config::meta::cluster::Role::Querier], 3);
            node.query_identifier.stream_type = stream_type.to_string();
            let batches = run(remote(scan(name), node, enabled)).await;
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
            assert_eq!(
                batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                42
            );
            let requests = server.take_requests();
            assert_eq!(requests.len(), 1);
            assert!(requests[0].query_identifier.as_ref().unwrap().enrich_mode);
        }
    }
    server.finish().await;
}

fn count_plan(
    mode: AggregateMode,
    input: Arc<dyn ExecutionPlan>,
    scan_schema: SchemaRef,
) -> Arc<dyn ExecutionPlan> {
    let count = Arc::new(
        AggregateExprBuilder::new(count_udaf(), vec![lit(1i32)])
            .schema(scan_schema.clone())
            .alias("count")
            .build()
            .unwrap(),
    );
    Arc::new(
        AggregateExec::try_new(
            mode,
            PhysicalGroupBy::default(),
            vec![count],
            vec![None],
            input,
            scan_schema,
        )
        .unwrap(),
    )
}

fn assert_zero_count(batches: &[RecordBatch]) {
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        0
    );
}

#[tokio::test]
async fn idle_enrich_empty_partial_count_matches_real_rpc_baseline() {
    let server = Server::start().await;
    for enabled in [false, true] {
        for reduce in [false, true] {
            let input = scan("events");
            let schema = input.schema();
            let partial = count_plan(AggregateMode::Partial, input, schema.clone());
            let partial = if reduce {
                count_plan(
                    AggregateMode::PartialReduce,
                    Arc::new(CoalescePartitionsExec::new(partial)),
                    schema.clone(),
                )
            } else {
                partial
            };
            let node = server.node(&[config::meta::cluster::Role::Querier], 3);
            let remote = remote(partial, node, enabled);
            let final_plan = count_plan(
                AggregateMode::Final,
                Arc::new(CoalescePartitionsExec::new(remote.clone())),
                schema,
            );
            let batches = collect(final_plan, SessionContext::new().task_ctx())
                .await
                .unwrap();
            assert!(remote.partial_err.lock().is_empty());
            assert_zero_count(&batches);
            assert_eq!(server.take_requests().len(), usize::from(!enabled));
        }
    }
    server.finish().await;
}

#[tokio::test]
async fn idle_enrich_single_and_final_count_keep_required_empty_rpc() {
    let server = Server::start().await;
    for enabled in [false, true] {
        for mode in [AggregateMode::Single, AggregateMode::Final] {
            let input = scan("events");
            let schema = input.schema();
            let input = if mode == AggregateMode::Final {
                count_plan(AggregateMode::Partial, input, schema.clone())
            } else {
                input
            };
            let input = count_plan(mode, input, schema);
            let node = server.node(&[config::meta::cluster::Role::Querier], 1);
            assert_zero_count(&run(remote(input, node, enabled)).await);
            assert_eq!(server.take_requests().len(), 1);
        }
    }
    server.finish().await;
}

#[tokio::test]
async fn idle_enrich_wal_mixed_role_super_cluster_and_assigned_files_still_dispatch() {
    use config::meta::cluster::Role;
    let server = Server::start().await;
    for enabled in [false, true] {
        for roles in [
            vec![Role::Ingester],
            vec![Role::Ingester, Role::Querier],
            vec![Role::Querier],
        ] {
            let mut node = server.node(&roles, 1);
            node.super_cluster_info.is_super_cluster = roles == vec![Role::Querier];
            run(remote(scan("events"), node, enabled)).await;
            assert_eq!(server.take_requests().len(), 1);
        }
        for file_id in [17, -17] {
            let mut node = server.node(&[Role::Querier], 1);
            node.search_infos.file_id_list = vec![vec![file_id]].into();
            run(remote(scan("events"), node, enabled)).await;
            let requests = server.take_requests();
            assert_eq!(requests.len(), 1);
            assert_eq!(
                requests[0].search_info.as_ref().unwrap().file_id_list,
                vec![file_id]
            );
        }
    }
    server.finish().await;
}

#[tokio::test]
async fn idle_enrich_redispatch_uses_real_idle_nodes_without_phantom_participants() {
    let server = Server::start().await;
    let mut node = server.node(&[config::meta::cluster::Role::Querier], 3);
    node.search_infos.file_id_list = vec![vec![11], vec![], vec![]].into();
    let exec = remote(scan("events"), node, true);
    assert_eq!(exec.enrich_mode_node_idx, None);
    assert_eq!(exec.redispatch.participant, vec![true, false, false]);
    assert_eq!(exec.redispatch.participants, 1);
    let target = exec
        .redispatch
        .pick_target(0, RedispatchReason::Failure)
        .unwrap();
    assert!((1..3).contains(&target));
    assert!(exec.redispatch.is_eligible(target));
    exec.redispatch.record_answer(target, 1);
    assert!(exec.redispatch.inner.lock().answered_ms.is_empty());
    exec.redispatch.record_answer(0, 7);
    assert_eq!(exec.redispatch.inner.lock().answered_ms, vec![7]);
    for enabled in [false, true] {
        let empty = remote(scan("events"), RemoteScanNode::default(), enabled);
        assert_eq!(empty.enrich_mode_node_idx, None);
        assert_eq!(empty.redispatch.participants, 0);
        assert_eq!(
            empty.redispatch.pick_target(0, RedispatchReason::Failure),
            None
        );
    }
    server.finish().await;
}

#[test]
fn idle_enrich_opaque_leaves_and_multiple_inputs_are_conservative() {
    let node = RemoteScanNode::default();
    let opaque: Arc<dyn ExecutionPlan> = Arc::new(
        datafusion::physical_plan::empty::EmptyExec::new(scan("events").schema()),
    );
    assert!(!can_skip_idle_enrich_node(&opaque, &node).unwrap());
    let union: Arc<dyn ExecutionPlan> = datafusion::physical_plan::union::UnionExec::try_new(vec![
        scan("events"),
        scan("enrich.lookup"),
    ])
    .unwrap();
    assert!(!can_skip_idle_enrich_node(&union, &node).unwrap());
}

#[test]
fn idle_enrich_analysis_output_keeps_required_node() {
    let input = scan("events");
    let analyze: Arc<dyn ExecutionPlan> = Arc::new(
        datafusion::physical_plan::analyze::AnalyzeExecBuilder::new(
            false,
            false,
            input.clone(),
            input.schema(),
        )
        .with_metric_types(vec![])
        .build(),
    );
    assert!(!can_skip_idle_enrich_node(&analyze, &RemoteScanNode::default()).unwrap());
    let analyze: Arc<dyn ExecutionPlan> = Arc::new(
        crate::datafusion::distributed_plan::distribute_analyze_exec::DistributeAnalyzeExec::new(
            false, false, input,
        ),
    );
    assert!(!can_skip_idle_enrich_node(&analyze, &RemoteScanNode::default()).unwrap());
}

#[tokio::test]
async fn idle_enrich_empty_grouping_sets_keep_total_row() {
    let server = Server::start().await;
    for enabled in [false, true] {
        for groups in [vec![vec![true]], vec![vec![false], vec![true]]] {
            let input = scan("events");
            let schema = input.schema();
            let group_by = PhysicalGroupBy::new(
                vec![(
                    datafusion::physical_plan::expressions::col("value", &schema).unwrap(),
                    "value".to_string(),
                )],
                vec![(
                    lit(datafusion::common::ScalarValue::Int64(None)),
                    "value".to_string(),
                )],
                groups,
                true,
            );
            let final_group_by = group_by.as_final();
            let count = Arc::new(
                AggregateExprBuilder::new(count_udaf(), vec![lit(1i32)])
                    .schema(schema.clone())
                    .alias("count")
                    .build()
                    .unwrap(),
            );
            let partial = Arc::new(
                AggregateExec::try_new(
                    AggregateMode::Partial,
                    group_by,
                    vec![count.clone()],
                    vec![None],
                    input,
                    schema.clone(),
                )
                .unwrap(),
            );
            let node = server.node(&[config::meta::cluster::Role::Querier], 3);
            let remote = remote(partial, node, enabled);
            let final_plan = Arc::new(
                AggregateExec::try_new(
                    AggregateMode::Final,
                    final_group_by,
                    vec![count],
                    vec![None],
                    Arc::new(CoalescePartitionsExec::new(remote.clone())),
                    schema,
                )
                .unwrap(),
            );
            let batches = collect(final_plan, SessionContext::new().task_ctx())
                .await
                .unwrap();
            assert!(remote.partial_err.lock().is_empty());
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
            let batch = batches.iter().find(|batch| batch.num_rows() > 0).unwrap();
            assert_eq!(
                batch
                    .column(batch.num_columns() - 1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                0
            );
            assert_eq!(server.take_requests().len(), 1);
        }
    }
    server.finish().await;
}
