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

use config::meta::{inverted_index::IndexOptimizeMode, stream::FileKey};
use datafusion::{
    arrow::datatypes::SchemaRef,
    common::{
        Result,
        tree_node::{Transformed, TreeNode, TreeNodeRewriter},
    },
    physical_plan::{
        ExecutionPlan,
        aggregates::{AggregateExec, AggregateMode},
        union::UnionExec,
    },
};
#[cfg(feature = "enterprise")]
use o2_enterprise::enterprise::search::datafusion::distributed_plan::metadata_count_exec::MetadataCountExec;

use crate::{
    datafusion::plan::index_optimize_exec::IndexOptimizeExec, types::QueryParams, vix::MultiResult,
};

pub fn aggregate_optimize_rewrite(
    query: Arc<QueryParams>,
    metadata_count_file_list: Vec<FileKey>,
    index_file_list: Vec<FileKey>,
    index_result: Option<MultiResult>,
    index_optimize_mode: Option<IndexOptimizeMode>,
    physical_plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let metadata_records = metadata_count_file_list.iter().fold(0i64, |total, file| {
        total.saturating_add(file.meta.records.max(0))
    });
    let metadata_files = metadata_count_file_list.len();
    if metadata_records == 0 && index_result.is_none() {
        return Ok(physical_plan);
    }

    let mut visitor = AggregateOptimizeRewriter::new(
        query,
        index_file_list,
        index_result,
        index_optimize_mode,
        metadata_records,
        metadata_files,
    );
    Ok(physical_plan.rewrite(&mut visitor)?.data)
}

pub struct AggregateOptimizeRewriter {
    query: Arc<QueryParams>,
    file_list: Vec<FileKey>,
    /// The precomputed result covers every source removed from the scan plan.
    /// `file_list` names core VIX files for display; segment-WAL contributions
    /// can make the result non-empty even when this list is empty.
    index_result: Option<MultiResult>,
    index_optimize_mode: Option<IndexOptimizeMode>,
    #[allow(unused)]
    metadata_records: i64,
    #[allow(unused)]
    metadata_files: usize,
}

impl AggregateOptimizeRewriter {
    pub fn new(
        query: Arc<QueryParams>,
        file_list: Vec<FileKey>,
        index_result: Option<MultiResult>,
        index_optimize_mode: Option<IndexOptimizeMode>,
        metadata_records: i64,
        metadata_files: usize,
    ) -> Self {
        Self {
            query,
            file_list,
            index_result,
            index_optimize_mode,
            metadata_records,
            metadata_files,
        }
    }

    fn index_optimize_exec(&mut self, schema: SchemaRef) -> Arc<dyn ExecutionPlan> {
        Arc::new(IndexOptimizeExec::new(
            self.query.clone(),
            schema,
            std::mem::take(&mut self.file_list),
            self.index_result
                .take()
                .expect("index result should exist when index files are present"),
            self.index_optimize_mode
                .clone()
                .expect("index optimize mode should exist when index files are present"),
        ))
    }

    #[cfg(feature = "enterprise")]
    fn metadata_count_exec(&self, schema: SchemaRef) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(MetadataCountExec::new(
            schema,
            self.metadata_records,
            self.metadata_files,
        )))
    }

    fn additional_inputs(&mut self, schema: SchemaRef) -> Result<Vec<Arc<dyn ExecutionPlan>>> {
        let mut inputs = Vec::new();

        #[cfg(feature = "enterprise")]
        if self.metadata_records > 0 {
            inputs.push(self.metadata_count_exec(schema.clone())?);
        }

        if self.index_result.is_some() {
            inputs.push(self.index_optimize_exec(schema));
        }

        Ok(inputs)
    }
}

impl TreeNodeRewriter for AggregateOptimizeRewriter {
    type Node = Arc<dyn ExecutionPlan>;

    fn f_up(&mut self, node: Arc<dyn ExecutionPlan>) -> Result<Transformed<Self::Node>> {
        if let Some(aggregate) = node.downcast_ref::<AggregateExec>()
            && *aggregate.mode() == AggregateMode::Partial
        {
            let mut inputs = vec![node.clone()];
            inputs.extend(self.additional_inputs(node.schema())?);

            let new_node = UnionExec::try_new(inputs)?;
            return Ok(Transformed::complete(new_node));
        }

        Ok(Transformed::no(node))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    #[cfg(feature = "enterprise")]
    use config::meta::stream::FileMeta;
    use config::meta::stream::{FileKey, StreamType};
    use datafusion::{
        common::Result,
        functions_aggregate::count::count_udaf,
        physical_expr::aggregate::AggregateExprBuilder,
        physical_plan::{
            ExecutionPlan,
            aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy},
            empty::EmptyExec,
            expressions::lit,
            union::UnionExec,
        },
        sql::TableReference,
    };

    use super::*;

    fn query_params() -> Arc<QueryParams> {
        Arc::new(QueryParams {
            trace_id: "test".to_string(),
            org_id: "org".to_string(),
            stream: TableReference::from("logs"),
            stream_type: StreamType::Logs,
            stream_name: "logs".to_string(),
            time_range: (0, 1000),
            work_group: None,
            use_inverted_index: true,
        })
    }

    fn partial_count_exec() -> Result<Arc<dyn ExecutionPlan>> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "_timestamp",
            DataType::Int64,
            false,
        )]));
        let aggregate = Arc::new(
            AggregateExprBuilder::new(count_udaf(), vec![lit(1i32)])
                .schema(schema.clone())
                .alias("COUNT(*)")
                .build()?,
        );

        Ok(Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::default(),
            vec![aggregate],
            vec![None],
            Arc::new(EmptyExec::new(schema.clone())),
            schema,
        )?))
    }

    #[cfg(feature = "enterprise")]
    #[test]
    fn test_aggregate_optimize_rewrite_combines_metadata_and_index_inputs() -> Result<()> {
        let plan = partial_count_exec()?;
        let metadata_files = vec![FileKey {
            meta: FileMeta {
                records: 11,
                ..Default::default()
            },
            ..Default::default()
        }];

        let rewritten = aggregate_optimize_rewrite(
            query_params(),
            metadata_files,
            vec![FileKey::default()],
            Some(MultiResult::Count(7)),
            Some(IndexOptimizeMode::SimpleCount),
            plan,
        )?;

        let union = rewritten
            .downcast_ref::<UnionExec>()
            .expect("rewrite should wrap the partial aggregate in a union");
        let inputs = union.inputs();
        assert_eq!(inputs.len(), 3);
        assert_eq!(inputs[0].name(), "AggregateExec");
        assert_eq!(inputs[1].name(), "MetadataCountExec");
        assert_eq!(inputs[2].name(), "IndexOptimizeExec");

        Ok(())
    }

    #[test]
    fn test_aggregate_optimize_rewrite_index_only() -> Result<()> {
        let rewritten = aggregate_optimize_rewrite(
            query_params(),
            vec![],
            vec![FileKey::default()],
            Some(MultiResult::Count(7)),
            Some(IndexOptimizeMode::SimpleCount),
            partial_count_exec()?,
        )?;

        let union = rewritten
            .downcast_ref::<UnionExec>()
            .expect("rewrite should wrap the partial aggregate in a union");
        let inputs = union.inputs();
        assert_eq!(inputs.len(), 2);
        assert_eq!(inputs[0].name(), "AggregateExec");
        assert_eq!(inputs[1].name(), "IndexOptimizeExec");

        Ok(())
    }
    #[test]
    fn test_aggregate_optimize_rewrite_precomputed_result_without_core_files() -> Result<()> {
        let rewritten = aggregate_optimize_rewrite(
            query_params(),
            vec![],
            vec![],
            Some(MultiResult::Count(7)),
            Some(IndexOptimizeMode::SimpleCount),
            partial_count_exec()?,
        )?;

        let union = rewritten
            .downcast_ref::<UnionExec>()
            .expect("segment-only result must wrap the partial aggregate");
        let inputs = union.inputs();
        assert_eq!(inputs.len(), 2);
        assert_eq!(inputs[1].name(), "IndexOptimizeExec");
        Ok(())
    }

    /// Without a precomputed index result there is nothing for the exec to
    /// serve: the plan stays unchanged (the files were moved to the scan
    /// branch by the eager evaluation in flight.rs).
    #[test]
    fn test_aggregate_optimize_rewrite_without_result_is_a_no_op() -> Result<()> {
        let plan = partial_count_exec()?;
        let rewritten = aggregate_optimize_rewrite(
            query_params(),
            vec![],
            vec![FileKey::default()],
            None,
            Some(IndexOptimizeMode::SimpleCount),
            Arc::clone(&plan),
        )?;
        assert!(
            rewritten.downcast_ref::<UnionExec>().is_none(),
            "no index result -> no IndexOptimizeExec union"
        );
        assert_eq!(rewritten.name(), "AggregateExec");
        Ok(())
    }
}
