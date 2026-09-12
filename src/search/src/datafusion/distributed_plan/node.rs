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

use std::sync::Arc;

use bytes::Bytes;
use config::{
    datafusion::request::{FlightSearchRequest, Request},
    meta::{cluster::NodeInfo, sql::TableReferenceExt},
};
use datafusion::common::TableReference;
use hashbrown::HashMap;
use proto::cluster_rpc::{IndexInfo, KvItem, QueryIdentifier, SearchInfo, SuperClusterInfo};

#[derive(Debug, Clone)]
pub struct RemoteScanNodes {
    pub req: Request,
    pub nodes: Arc<[Arc<dyn NodeInfo>]>,
    pub file_id_lists: HashMap<TableReference, Arc<[Vec<i64>]>>,
    pub equal_keys: HashMap<TableReference, Vec<KvItem>>,
    pub is_leader: bool, // for super cluster
    pub opentelemetry_context: opentelemetry::Context,
    pub sampling_config: Option<proto::cluster_rpc::SamplingConfig>,
}

impl RemoteScanNodes {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        req: Request,
        nodes: Vec<Arc<dyn NodeInfo>>,
        file_id_lists: HashMap<TableReference, Vec<Vec<i64>>>,
        equal_keys: HashMap<TableReference, Vec<KvItem>>,
        is_leader: bool,
        opentelemetry_context: opentelemetry::Context,
        sampling_config: Option<proto::cluster_rpc::SamplingConfig>,
    ) -> Self {
        Self {
            req,
            nodes: nodes.into(),
            file_id_lists: file_id_lists
                .into_iter()
                .map(|(table, partitions)| (table, partitions.into()))
                .collect(),
            equal_keys,
            is_leader,
            opentelemetry_context,
            sampling_config,
        }
    }

    pub fn get_remote_node(&self, table_name: &TableReference) -> RemoteScanNode {
        let query_identifier = QueryIdentifier {
            trace_id: self.req.trace_id.clone(),
            org_id: self.req.org_id.clone(),
            stream_type: table_name.get_stream_type(self.req.stream_type).to_string(),
            partition: 0,           // set in FlightSearchRequest
            job_id: "".to_string(), // set in FlightSearchRequest
            enrich_mode: false,     // set in RemoteScanExec
        };

        let search_infos = SearchInfos {
            plan: Bytes::new(), // set in RemoteScanNode
            file_id_list: self
                .file_id_lists
                .get(table_name)
                .cloned()
                .unwrap_or_default(),
            // Never widen this to a histogram bucket boundary: the follower
            // scan window must equal the requested range or bucket sums can
            // include records that count(*) excludes.
            start_time: self.req.time_range.as_ref().map(|x| x.0).unwrap_or(0),
            end_time: self.req.time_range.as_ref().map(|x| x.1).unwrap_or(0),
            timeout: self.req.timeout as u64,
            use_cache: self.req.use_cache,
            histogram_interval: self.req.histogram_interval,
            is_analyze: false, // set in distribute Analyze
            sampling_config: self.sampling_config.clone(),
            clear_cache: self.req.overwrite_cache,
        };

        let index_info = IndexInfo {
            equal_keys: self.equal_keys.get(table_name).unwrap_or(&vec![]).clone(),
            index_optimize_mode: None, // set in LeaderIndexOptimizerule
        };

        let super_cluster_info = SuperClusterInfo {
            is_super_cluster: self.is_leader,
            user_id: self.req.user_id.clone(),
            work_group: self.req.work_group.clone(),
            search_event_type: self.req.search_event_type.clone(),
            local_mode: self.req.local_mode,
        };

        RemoteScanNode {
            nodes: self.nodes.clone(),
            opentelemetry_context: self.opentelemetry_context.clone(),
            query_identifier,
            search_infos,
            index_info,
            super_cluster_info,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct RemoteScanNode {
    pub nodes: Arc<[Arc<dyn NodeInfo>]>,
    pub opentelemetry_context: opentelemetry::Context,
    pub query_identifier: QueryIdentifier,
    pub search_infos: SearchInfos,
    pub index_info: IndexInfo,
    pub super_cluster_info: SuperClusterInfo,
}

impl RemoteScanNode {
    pub fn get_flight_search_request(&self, partition: usize) -> FlightSearchRequest {
        FlightSearchRequest {
            query_identifier: self.query_identifier.clone(),
            search_info: self.search_infos.get_search_info(partition), // add is_querier
            index_info: self.index_info.clone(),
            super_cluster_info: self.super_cluster_info.clone(),
        }
    }

    pub fn is_file_list_empty(&self, partition: usize) -> bool {
        self.search_infos.file_id_list.is_empty()
            || self.search_infos.file_id_list[partition].is_empty()
    }

    // used in RemoteScanExec
    // need to set plan before send to follow
    pub fn set_plan(&mut self, plan: Bytes) {
        self.search_infos.plan = plan;
    }

    // need to set to true when from super cluster leader to super cluster follower
    // otherwise set to false
    // used in super cluster follower.rs
    #[cfg(feature = "enterprise")]
    pub fn set_is_super_cluster(&mut self, is_leader: bool) {
        self.super_cluster_info.is_super_cluster = is_leader;
    }

    #[cfg(feature = "enterprise")]
    pub fn from_flight_search_request(
        request: &FlightSearchRequest,
        search_infos: SearchInfos,
        nodes: Vec<Arc<dyn NodeInfo>>,
        opentelemetry_context: opentelemetry::Context,
    ) -> Self {
        Self {
            nodes: nodes.into(),
            opentelemetry_context,
            query_identifier: request.query_identifier.clone(),
            search_infos,
            index_info: request.index_info.clone(),
            super_cluster_info: request.super_cluster_info.clone(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SearchInfos {
    pub plan: Bytes,
    pub file_id_list: Arc<[Vec<i64>]>,
    pub start_time: i64,
    pub end_time: i64,
    pub timeout: u64,
    pub use_cache: bool,
    pub histogram_interval: i64,
    pub is_analyze: bool,
    pub sampling_config: Option<proto::cluster_rpc::SamplingConfig>,
    pub clear_cache: bool,
}

impl SearchInfos {
    pub fn get_search_info(&self, partition: usize) -> SearchInfo {
        let file_id_list = if self.file_id_list.is_empty() {
            vec![]
        } else {
            self.file_id_list[partition].clone()
        };
        SearchInfo {
            plan: self.plan.to_vec(),
            file_id_list,
            start_time: self.start_time,
            end_time: self.end_time,
            timeout: self.timeout as i64,
            use_cache: self.use_cache,
            histogram_interval: self.histogram_interval,
            is_analyze: self.is_analyze,
            sampling_config: self.sampling_config.clone(),
            clear_cache: self.clear_cache,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_file_list_empty_when_search_infos_empty() {
        let node = RemoteScanNode::default();
        assert!(node.is_file_list_empty(0));
    }

    #[test]
    fn test_is_file_list_empty_false_when_partition_has_files() {
        let mut node = RemoteScanNode::default();
        node.search_infos.file_id_list = vec![vec![1, 2, 3]].into();
        assert!(!node.is_file_list_empty(0));
    }

    #[test]
    fn test_is_file_list_empty_partition_with_empty_list() {
        let mut node = RemoteScanNode::default();
        node.search_infos.file_id_list = vec![vec![1, 2], vec![]].into();
        assert!(!node.is_file_list_empty(0));
        assert!(node.is_file_list_empty(1));
    }

    #[test]
    fn test_get_remote_node_keeps_requested_histogram_range() {
        let req = Request {
            time_range: Some((1_700_000_017_000_000, 1_700_000_900_000_000)),
            histogram_interval: 30,
            ..Default::default()
        };
        let nodes = RemoteScanNodes::new(
            req,
            vec![],
            HashMap::new(),
            HashMap::new(),
            false,
            opentelemetry::Context::new(),
            None,
        );

        let info = nodes.get_remote_node(&TableReference::from("logs"));

        assert_eq!(info.search_infos.start_time, 1_700_000_017_000_000);
        assert_eq!(info.search_infos.end_time, 1_700_000_900_000_000);
    }

    #[test]
    fn dispatch_requests_keep_table_partition_assignment_and_private_overrides() {
        let first = TableReference::from("first");
        let second = TableReference::from("second");
        let nodes = RemoteScanNodes::new(
            Request::default(),
            vec![],
            HashMap::from([
                (first.clone(), vec![vec![10, -20, 10], vec![], vec![-30]]),
                (second.clone(), vec![vec![40], vec![50], vec![]]),
            ]),
            HashMap::new(),
            false,
            opentelemetry::Context::new(),
            None,
        );
        let mut first_node = nodes.get_remote_node(&first);
        first_node.set_plan(Bytes::from_static(&[1, 2, 3]));
        let original = first_node.get_flight_search_request(0);
        let mut dispatched = first_node.clone().get_flight_search_request(0);
        dispatched.set_job_id("job-one".to_string());
        dispatched.set_partition(2);
        dispatched.query_identifier.enrich_mode = true;
        dispatched.search_info.plan.clear();
        dispatched.search_info.file_id_list.clear();
        dispatched.search_info.timeout = 15;
        dispatched.search_info.is_analyze = true;
        dispatched
            .index_info
            .equal_keys
            .push(KvItem::new("field", "value"));
        dispatched.super_cluster_info.is_super_cluster = true;

        let retained = first_node.get_flight_search_request(0);
        assert_eq!(retained.query_identifier, original.query_identifier);
        assert_eq!(retained.search_info, original.search_info);
        assert_eq!(retained.index_info, original.index_info);
        assert_eq!(retained.super_cluster_info, original.super_cluster_info);
        assert_eq!(original.search_info.file_id_list, vec![10, -20, 10]);
        assert!(
            first_node
                .get_flight_search_request(1)
                .search_info
                .file_id_list
                .is_empty()
        );
        assert_eq!(
            first_node
                .get_flight_search_request(2)
                .search_info
                .file_id_list,
            vec![-30]
        );
        assert_eq!(
            nodes
                .get_remote_node(&second)
                .get_flight_search_request(1)
                .search_info
                .file_id_list,
            vec![50]
        );
        assert!(
            nodes
                .get_remote_node(&TableReference::from("absent"))
                .is_file_list_empty(0)
        );
    }
}
