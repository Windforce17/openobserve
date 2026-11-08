// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! DF55 moved table-local listing settings into SessionConfig. Keep the existing
//! per-table settings without replacing the query's functions, runtime or planner.
use std::{any::Any, collections::HashMap, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    catalog::{CatalogProviderList, Session},
    common::{DFSchema, Result, config::TableOptions},
    execution::{
        TaskContext, config::SessionConfig, context::QueryPlanner, runtime_env::RuntimeEnv,
    },
    logical_expr::{
        AggregateUDF, Expr, HigherOrderUDF, LogicalPlan, ScalarUDF, WindowUDF,
        execution_props::ExecutionProps, registry::ExtensionTypeRegistryRef,
    },
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{ExecutionPlan, PhysicalExpr, operator_statistics::StatisticsRegistry},
};

pub(super) struct ScanSession<'a> {
    inner: &'a dyn Session,
    config: SessionConfig,
    table_options: TableOptions,
}

impl<'a> ScanSession<'a> {
    pub(super) fn new(inner: &'a dyn Session, partitions: usize, collect: bool) -> Self {
        Self {
            inner,
            config: inner
                .config()
                .clone()
                .with_target_partitions(partitions)
                .with_collect_statistics(collect),
            table_options: inner.table_options().clone(),
        }
    }
}

#[async_trait]
impl Session for ScanSession<'_> {
    fn session_id(&self) -> &str {
        self.inner.session_id()
    }
    fn config(&self) -> &SessionConfig {
        &self.config
    }
    fn catalog_list(&self) -> Arc<dyn CatalogProviderList> {
        self.inner.catalog_list()
    }
    fn query_planner(&self) -> Arc<dyn QueryPlanner + Send + Sync> {
        self.inner.query_planner()
    }
    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        self.inner.optimize(plan)
    }
    fn physical_optimizers(&self) -> &[Arc<dyn PhysicalOptimizerRule + Send + Sync>] {
        self.inner.physical_optimizers()
    }
    fn statistics_registry(&self) -> Option<&StatisticsRegistry> {
        self.inner.statistics_registry()
    }
    async fn create_physical_plan(&self, plan: &LogicalPlan) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.create_physical_plan(plan).await
    }
    fn create_physical_expr(&self, expr: Expr, schema: &DFSchema) -> Result<Arc<dyn PhysicalExpr>> {
        self.inner.create_physical_expr(expr, schema)
    }
    fn scalar_functions(&self) -> &HashMap<String, Arc<ScalarUDF>> {
        self.inner.scalar_functions()
    }
    fn higher_order_functions(&self) -> &HashMap<String, Arc<HigherOrderUDF>> {
        self.inner.higher_order_functions()
    }
    fn aggregate_functions(&self) -> &HashMap<String, Arc<AggregateUDF>> {
        self.inner.aggregate_functions()
    }
    fn window_functions(&self) -> &HashMap<String, Arc<WindowUDF>> {
        self.inner.window_functions()
    }
    fn extension_type_registry(&self) -> &ExtensionTypeRegistryRef {
        self.inner.extension_type_registry()
    }
    fn runtime_env(&self) -> &Arc<RuntimeEnv> {
        self.inner.runtime_env()
    }
    fn execution_props(&self) -> &ExecutionProps {
        self.inner.execution_props()
    }
    fn as_any(&self) -> &dyn Any {
        self.inner.as_any()
    }
    fn table_options(&self) -> &TableOptions {
        &self.table_options
    }
    fn table_options_mut(&mut self) -> &mut TableOptions {
        &mut self.table_options
    }
    fn task_ctx(&self) -> Arc<TaskContext> {
        Arc::new(TaskContext::from(self as &dyn Session))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::execution::context::SessionContext;

    use super::*;

    #[test]
    fn scan_options_are_table_local_and_task_context_retains_them() {
        let config = SessionConfig::new()
            .with_target_partitions(7)
            .with_collect_statistics(true)
            .with_batch_size(321);
        let ctx = SessionContext::new_with_config(config);
        let inner = ctx.state();
        let scan = ScanSession::new(&inner, 2, false);
        assert_eq!(scan.config().target_partitions(), 2);
        assert!(!scan.config_options().execution.collect_statistics);
        assert_eq!(scan.config().batch_size(), 321);
        assert_eq!(inner.config().target_partitions(), 7);
        assert!(inner.config_options().execution.collect_statistics);
        assert_eq!(scan.task_ctx().session_config().target_partitions(), 2);
        assert!(
            !scan
                .task_ctx()
                .session_config()
                .options()
                .execution
                .collect_statistics
        );
        assert_eq!(scan.session_id(), inner.session_id());
        assert!(Arc::ptr_eq(scan.runtime_env(), inner.runtime_env()));
        assert!(Arc::ptr_eq(&scan.catalog_list(), inner.catalog_list()));
        assert_eq!(
            scan.scalar_functions().len(),
            inner.scalar_functions().len()
        );
        assert_eq!(
            scan.aggregate_functions().len(),
            inner.aggregate_functions().len()
        );
        assert_eq!(
            scan.physical_optimizers().len(),
            inner.physical_optimizers().len()
        );
    }
}
