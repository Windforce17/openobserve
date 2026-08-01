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

use config::{TIMESTAMP_COL_NAME, meta::inverted_index::IndexOptimizeMode};
use datafusion::{
    common::{
        Result,
        tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor},
    },
    logical_expr::Operator,
    physical_expr::ScalarFunctionExpr,
    physical_plan::{
        ExecutionPlan, PhysicalExpr,
        aggregates::AggregateExec,
        expressions::{BinaryExpr, Literal},
        projection::ProjectionExec,
    },
    scalar::ScalarValue,
};
use hashbrown::HashSet;

use crate::datafusion::optimizer::physical_optimizer::{
    index_optimizer::utils::is_complex_plan,
    utils::{get_column_name, is_column, is_count_rows_aggregate},
};

#[rustfmt::skip]
/// SimpleHistogram(i64, u64, usize, i64): select histogram(_timestamp, '1m') as ts, count(*) as cnt from table where match_all() group by ts;
/// histogram() with a timezone rewrites to date_bin over `_timestamp@0 + offset`; the
/// extracted mode then carries the offset and its bucket edges live in local wall-clock space.
/// condition: group by histogram(_timestamp), only count(*)
///
/// Matching contract (pinned by the follower-fidelity tests below against the
/// EXACT UI-generated SQL `SELECT histogram(_timestamp) AS zo_sql_key,
/// count(*) AS zo_sql_num FROM "stream" GROUP BY zo_sql_key ORDER BY
/// zo_sql_key DESC`):
/// - a 1-arg `histogram(_timestamp)` is ALREADY resolved by the time this
///   visitor runs: the RewriteHistogram logical rule turns it into a
///   date_bin with a concrete interval literal (the request's preset
///   `histogram_interval` seconds — the streaming path pre-computes it from
///   the full query range — or `generate_histogram_interval`), and constant
///   folding reduces the cast to a `Literal(IntervalMonthDayNano)`. The
///   visitor reads the bucket width FROM THE PLAN, so it always equals what
///   the histogram() UDF produces for the same range — never recompute it.
/// - ORDER BY (any direction) and LIMIT are irrelevant: they sit above the
///   final aggregate on the leader, while the follower receives (and this
///   visitor matches) the partial-aggregate sub-plan below the
///   RemoteScanExec split.
/// - output aliases (`zo_sql_*` or anything else) are irrelevant: matching
///   is on the group/aggregate expressions.
///
/// example plan:
/// ```text
/// ProjectionExec: expr=[histogram(default._timestamp)@0 as histogram(default._timestamp), count(Int64(1))@1 as cnt]
///   AggregateExec: mode=FinalPartitioned, gby=[histogram(default._timestamp)@0 as histogram(default._timestamp)], aggr=[count(Int64(1))]
///     CoalesceBatchesExec: target_batch_size=8192
///       RepartitionExec: partitioning=Hash([histogram(default._timestamp)@0], 12), input_partitions=12
///         AggregateExec: mode=Partial, gby=[date_bin(IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 86400000000000 }, to_timestamp_micros(_timestamp@0), 978307200000000000) as histogram(default._timestamp)], aggr=[count(Int64(1))]
///           CoalesceBatchesExec: target_batch_size=8192
///             FilterExec: _timestamp@0 >= 17296550822151 AND _timestamp@0 < 172965508891538700
///               CooperativeExec
///                 NewEmptyExec: name="default", projection=["_timestamp"],
/// ```
pub fn is_simple_histogram(plan: Arc<dyn ExecutionPlan>, time_range: (i64, i64)) -> Option<IndexOptimizeMode> {
    let mut visitor = SimpleHistogramVisitor::new(time_range);
    let _ = plan.visit(&mut visitor);
    if let Some((min_value, bucket_width, num_buckets, ts_offset)) = visitor.simple_histogram {
        Some(IndexOptimizeMode::SimpleHistogram(
            min_value,
            bucket_width,
            num_buckets,
            ts_offset,
        ))
    } else {
        None
    }
}

struct SimpleHistogramVisitor {
    time_range: (i64, i64),
    pub simple_histogram: Option<(i64, u64, usize, i64)>,
}

impl SimpleHistogramVisitor {
    pub fn new(time_range: (i64, i64)) -> Self {
        Self {
            simple_histogram: None,
            time_range,
        }
    }
}

impl<'n> TreeNodeVisitor<'n> for SimpleHistogramVisitor {
    type Node = Arc<dyn ExecutionPlan>;

    fn f_down(&mut self, node: &'n Self::Node) -> Result<TreeNodeRecursion> {
        if let Some(aggregate) = node.downcast_ref::<AggregateExec>() {
            // Check if the AggregateExec matches SimpleHistogram pattern
            if aggregate.group_expr().expr().len() == 1
                && aggregate.aggr_expr().len() == 1
                && is_count_rows_aggregate(&aggregate.aggr_expr()[0])
            {
                // Check group by field
                if let Some((group_expr, _)) = aggregate.group_expr().expr().first()
                    && let Some(func) = get_data_bin(group_expr)
                    && func.args().len() == 3
                    // check second argument is _timestamp (with an optional timezone shift)
                    && let Some(ts_offset) = get_timestamp_offset(&func.args()[1])
                {
                    let args = func.args();
                    if let Some(histogram_interval) = get_histogram_interval(&args[0]) {
                        let (start_time, end_time) = self.time_range;
                        // round the bucket edges to even start
                        let rounding_by = histogram_interval as i64;
                        let min_value = start_time - start_time % rounding_by;
                        let max_value = end_time;
                        let num_buckets = ((max_value - min_value) as f64
                            / histogram_interval as f64)
                            .ceil() as usize;
                        self.simple_histogram =
                            Some((min_value, histogram_interval, num_buckets, ts_offset));
                        return Ok(TreeNodeRecursion::Continue);
                    }
                }
            }
            // If AggregateExec doesn't match SimpleHistogram pattern, stop visiting
            self.simple_histogram = None;
            return Ok(TreeNodeRecursion::Stop);
        } else if let Some(projection) = node.downcast_ref::<ProjectionExec>() {
            // Check ProjectionExec for the structure: [histogram(_timestamp), count(*)]
            let exprs = projection.expr();
            if exprs.len() == 2 {
                // First expression should be the histogram(_timestamp), second should be count(*)
                // We'll validate this in the AggregateExec
                return Ok(TreeNodeRecursion::Continue);
            }
            // If projection doesn't have exactly 2 expressions, stop visiting
            self.simple_histogram = None;
            return Ok(TreeNodeRecursion::Stop);
        } else if is_complex_plan(node) {
            // If encounter complex plan, stop visiting
            self.simple_histogram = None;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    }
}

fn get_data_bin(expr: &Arc<dyn PhysicalExpr>) -> Option<&ScalarFunctionExpr> {
    if let Some(func) = expr.downcast_ref::<ScalarFunctionExpr>()
        && func.fun().name().to_lowercase() == "date_bin"
    {
        Some(func)
    } else {
        None
    }
}

// unit: microseconds
fn get_histogram_interval(expr: &Arc<dyn PhysicalExpr>) -> Option<u64> {
    let interval = expr.downcast_ref::<Literal>()?.value();
    match interval {
        ScalarValue::IntervalMonthDayNano(Some(interval)) => {
            // convert interval to nanoseconds
            let microseconds = interval.nanoseconds / 1_000
                + interval.days as i64 * 24 * 60 * 60 * 1_000_000
                + interval.months as i64 * 30 * 24 * 60 * 60 * 1_000_000;
            Some(microseconds as u64)
        }
        _ => None,
    }
}

/// Returns the fixed timezone offset (µs east of UTC) carried by the date_bin source
/// expression: `to_timestamp_micros(_timestamp)` yields 0 and
/// `to_timestamp_micros(_timestamp + offset)` — the shape histogram() with a timezone
/// rewrites to — yields the offset. None when the source is not the timestamp column.
fn get_timestamp_offset(expr: &Arc<dyn PhysicalExpr>) -> Option<i64> {
    let func = expr.downcast_ref::<ScalarFunctionExpr>()?;
    let arg = func.args().first()?;
    if get_column_name(arg) == TIMESTAMP_COL_NAME {
        return Some(0);
    }
    let bin = arg.downcast_ref::<BinaryExpr>()?;
    if *bin.op() != Operator::Plus || get_column_name(bin.left()) != TIMESTAMP_COL_NAME {
        return None;
    }
    match bin.right().downcast_ref::<Literal>()?.value() {
        ScalarValue::Int64(Some(ts_offset)) => Some(*ts_offset),
        _ => None,
    }
}

#[rustfmt::skip]
/// SimpleMultiHistogram(i64, i64, u64, i64, String):
/// histogram() with a timezone rewrites to date_bin over `_timestamp@0 + offset`; the
/// extracted mode then carries the offset and its bucket edges live in local wall-clock space.
/// select histogram(_timestamp) as ts, level as zo_sql_breakdown, count(*) as cnt
///   from table where match_all() group by ts, zo_sql_breakdown;
/// condition: group by histogram(_timestamp) AND a secondary index field, only count(*)
///
/// The same matching contract as [`is_simple_histogram`] applies (resolved
/// interval literal, order/limit/alias irrelevance) — pinned against the
/// EXACT UI-generated breakdown SQL `SELECT histogram(_timestamp) AS
/// zo_sql_key, "<field>" AS zo_sql_breakdown, count(*) AS zo_sql_num FROM
/// "stream" GROUP BY zo_sql_key, zo_sql_breakdown ORDER BY zo_sql_key DESC`,
/// including dotted quoted breakdown fields (`"kubernetes.namespace.name"`).
/// ELIGIBILITY: `index_fields` here is the stream's `column_store_fields`
/// (DESIGN §6/§15.6) — the collector reads the breakdown values from the
/// per-file docs column, so a breakdown field that is not column-stored MUST
/// refuse (return None) and the query takes the DataFusion branch. This is
/// the live `severity` case: the UI auto-picks a breakdown field from the
/// schema by name priority, with no regard to column-store eligibility, and
/// such queries full-scan by design until the field is added to the
/// stream's `column_store_fields`.
///
/// example plan:
/// ```text
/// ProjectionExec: expr=[histogram(_timestamp)@0 as ts, level@1 as level, count(Int64(1))@2 as cnt]
///   AggregateExec: mode=FinalPartitioned, gby=[histogram(_timestamp)@0 as ts, level@1 as level], aggr=[count(Int64(1))]
///     CoalesceBatchesExec: target_batch_size=8192
///       RepartitionExec: partitioning=Hash([histogram(_timestamp)@0, level@1], 12), input_partitions=12
///         AggregateExec: mode=Partial, gby=[date_bin(...) as ts, level@1 as level], aggr=[count(Int64(1))]
///           ...
/// ```
pub fn is_simple_multi_histogram(
    plan: Arc<dyn ExecutionPlan>,
    time_range: (i64, i64),
    index_fields: HashSet<String>,
) -> Option<IndexOptimizeMode> {
    let mut visitor = SimpleMultiHistogramVisitor::new(time_range, index_fields);
    let _ = plan.visit(&mut visitor);
    if let Some((min_value, max_value, bucket_width, ts_offset, breakdown_field)) =
        visitor.simple_multi_histogram
    {
        Some(IndexOptimizeMode::SimpleMultiHistogram(
            min_value,
            max_value,
            bucket_width,
            ts_offset,
            breakdown_field,
        ))
    } else {
        None
    }
}

struct SimpleMultiHistogramVisitor {
    time_range: (i64, i64),
    index_fields: HashSet<String>,
    pub simple_multi_histogram: Option<(i64, i64, u64, i64, String)>,
}

impl SimpleMultiHistogramVisitor {
    pub fn new(time_range: (i64, i64), index_fields: HashSet<String>) -> Self {
        Self {
            simple_multi_histogram: None,
            time_range,
            index_fields,
        }
    }
}

impl<'n> TreeNodeVisitor<'n> for SimpleMultiHistogramVisitor {
    type Node = Arc<dyn ExecutionPlan>;

    fn f_down(&mut self, node: &'n Self::Node) -> Result<TreeNodeRecursion> {
        if let Some(aggregate) = node.downcast_ref::<AggregateExec>() {
            // Exactly 2 group-by expressions (histogram + breakdown) and 1 aggregate (count(*))
            if aggregate.group_expr().expr().len() == 2
                && aggregate.aggr_expr().len() == 1
                && is_count_rows_aggregate(&aggregate.aggr_expr()[0])
            {
                let groups = aggregate.group_expr().expr();
                // One must be date_bin (histogram), the other must be an index field column
                let date_bin_idx = groups
                    .iter()
                    .position(|(expr, _)| get_data_bin(expr).is_some());
                let col_idx = groups.iter().position(|(expr, _)| is_column(expr));

                if let (Some(db_idx), Some(c_idx)) = (date_bin_idx, col_idx)
                    && db_idx != c_idx
                {
                    let (col_expr, _) = &groups[c_idx];
                    let column_name = get_column_name(col_expr);
                    if self.index_fields.contains(column_name) {
                        // Extract histogram parameters from the date_bin expression
                        let func = get_data_bin(&groups[db_idx].0).unwrap();
                        if func.args().len() == 3
                            && let Some(ts_offset) = get_timestamp_offset(&func.args()[1])
                        {
                            let args = func.args();
                            if let Some(histogram_interval) = get_histogram_interval(&args[0]) {
                                let (start_time, end_time) = self.time_range;
                                let rounding_by = histogram_interval as i64;
                                let min_value = start_time - start_time % rounding_by;
                                let max_value = end_time;
                                self.simple_multi_histogram = Some((
                                    min_value,
                                    max_value,
                                    histogram_interval,
                                    ts_offset,
                                    column_name.to_string(),
                                ));
                                return Ok(TreeNodeRecursion::Continue);
                            }
                        }
                    }
                }
            }
            // If AggregateExec doesn't match, stop visiting
            self.simple_multi_histogram = None;
            return Ok(TreeNodeRecursion::Stop);
        } else if let Some(projection) = node.downcast_ref::<ProjectionExec>() {
            // Projection should have 3 expressions: timestamp, breakdown, count
            let exprs = projection.expr();
            if exprs.len() == 3 {
                return Ok(TreeNodeRecursion::Continue);
            }
            self.simple_multi_histogram = None;
            return Ok(TreeNodeRecursion::Stop);
        } else if is_complex_plan(node) {
            self.simple_multi_histogram = None;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use datafusion::{
        common::Result,
        execution::{SessionStateBuilder, runtime_env::RuntimeEnvBuilder},
        prelude::{SessionConfig, SessionContext},
    };

    use super::*;
    use crate::datafusion::{
        optimizer::{
            logical_optimizer::rewrite_histogram::RewriteHistogram,
            physical_optimizer::index_optimizer::utils::tests::get_partial_aggregate_plan,
        },
        table_provider::empty_table::NewEmptyTable,
        udf::histogram_udf,
    };

    #[tokio::test]
    async fn test_is_simple_histogram() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let start_time = 1757401694060000;
        let end_time = 1757402594060000;
        let histogram_interval = 60; // 60s
        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_target_partitions(12))
            .with_runtime_env(Arc::new(RuntimeEnvBuilder::new().build().unwrap()))
            .with_default_features()
            .with_optimizer_rule(Arc::new(RewriteHistogram::new(
                start_time,
                end_time,
                histogram_interval,
                None,
            )))
            .build();
        let ctx = SessionContext::new_with_state(state);
        let provider = NewEmptyTable::new("t", schema);
        ctx.register_table("t", Arc::new(provider)).unwrap();
        ctx.register_udf(histogram_udf::HISTOGRAM_UDF.clone());

        let cases = vec![
            (
                "SELECT histogram(_timestamp) as ts, count(*) as cnt from t group by ts",
                Some(IndexOptimizeMode::SimpleHistogram(
                    1757401680000000,
                    60000000,
                    16,
                    0,
                )),
            ),
            // an explicit timezone shifts the bucket edges into local wall-clock
            // space and the extracted mode carries the offset (issue #12564)
            (
                "SELECT histogram(_timestamp, '1 minute', '+08:00') as ts, count(*) as cnt from t group by ts",
                Some(IndexOptimizeMode::SimpleHistogram(
                    1757401680000000,
                    60000000,
                    16,
                    28800000000,
                )),
            ),
            (
                "SELECT histogram(_timestamp) as ts, count(_timestamp) as cnt from t group by ts",
                Some(IndexOptimizeMode::SimpleHistogram(
                    1757401680000000,
                    60000000,
                    16,
                    0,
                )),
            ),
            (
                "SELECT name, histogram(_timestamp) as ts, count(*) as cnt from t group by name, ts",
                None,
            ),
            (
                "SELECT histogram(_timestamp) as ts, count(name) as cnt from t group by ts",
                None,
            ),
        ];

        for (sql, expected) in cases {
            let plan = ctx.state().create_logical_plan(sql).await?;
            let physical_plan = ctx.state().create_physical_plan(&plan).await?;

            let partial_aggregate_plan =
                Arc::new(get_partial_aggregate_plan(physical_plan).unwrap()) as _;
            assert_eq!(
                expected,
                is_simple_histogram(partial_aggregate_plan, (start_time, end_time))
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_is_simple_multi_histogram() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("level", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let start_time = 1757401694060000;
        let end_time = 1757402594060000;
        let histogram_interval = 60; // 60s
        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_target_partitions(12))
            .with_runtime_env(Arc::new(RuntimeEnvBuilder::new().build().unwrap()))
            .with_default_features()
            .with_optimizer_rule(Arc::new(RewriteHistogram::new(
                start_time,
                end_time,
                histogram_interval,
                None,
            )))
            .build();
        let ctx = SessionContext::new_with_state(state);
        let provider = NewEmptyTable::new("t", schema);
        ctx.register_table("t", Arc::new(provider)).unwrap();
        ctx.register_udf(histogram_udf::HISTOGRAM_UDF.clone());

        let index_fields = HashSet::from(["level".to_string()]);

        let cases = vec![
            (
                "SELECT histogram(_timestamp) as ts, level, count(*) as cnt from t group by ts, level",
                Some(IndexOptimizeMode::SimpleMultiHistogram(
                    1757401680000000,
                    1757402594060000,
                    60000000,
                    0,
                    "level".to_string(),
                )),
            ),
            // an explicit timezone shifts the bucket edges into local wall-clock
            // space and the extracted mode carries the offset (issue #12564)
            (
                "SELECT histogram(_timestamp, '1 minute', '-05:30') as ts, level, count(*) as cnt from t group by ts, level",
                Some(IndexOptimizeMode::SimpleMultiHistogram(
                    1757401680000000,
                    1757402594060000,
                    60000000,
                    -19800000000,
                    "level".to_string(),
                )),
            ),
            (
                "SELECT histogram(_timestamp) as ts, level, count(_timestamp) as cnt from t group by ts, level",
                Some(IndexOptimizeMode::SimpleMultiHistogram(
                    1757401680000000,
                    1757402594060000,
                    60000000,
                    0,
                    "level".to_string(),
                )),
            ),
            // level not in index_fields
            (
                "SELECT histogram(_timestamp) as ts, name, count(*) as cnt from t group by ts, name",
                None,
            ),
            // count over non-timestamp field is not equivalent to count(*)
            (
                "SELECT histogram(_timestamp) as ts, level, count(name) as cnt from t group by ts, level",
                None,
            ),
            // single group by (no breakdown) - should not match multi histogram
            (
                "SELECT histogram(_timestamp) as ts, count(*) as cnt from t group by ts",
                None,
            ),
        ];

        for (sql, expected) in cases {
            let plan = ctx.state().create_logical_plan(sql).await?;
            let physical_plan = ctx.state().create_physical_plan(&plan).await?;

            let partial_aggregate_plan =
                Arc::new(get_partial_aggregate_plan(physical_plan).unwrap()) as _;
            assert_eq!(
                expected,
                is_simple_multi_histogram(
                    partial_aggregate_plan,
                    (start_time, end_time),
                    index_fields.clone(),
                ),
                "Failed for SQL: {sql}"
            );
        }

        Ok(())
    }

    // ------------------------------------------------------------------
    // Follower-fidelity tests against the EXACT UI-generated histogram SQL
    // (src/search/src/sql/histogram.rs::single_stream_histogram_query),
    // planned through the production pipeline: RewriteHistogram +
    // AddSortAndLimit, RemoteScanRule leader/follower split, flight proto
    // roundtrip, FollowerIndexOptimizerRule (see
    // super::test_harness::follower_extracted_mode).
    //
    // Written failing-first for the live "UI streaming histogram bypasses
    // the fast paths" incident; they PASS against the current detectors,
    // proving the 1-arg auto-interval form, ORDER BY zo_sql_key DESC and
    // the zo_sql_* aliases do NOT break matching, and that the breakdown
    // shape maps to SimpleMultiHistogram (dotted quoted fields included)
    // whenever the breakdown field is column-stored. The live bypass is the
    // eligibility refusal pinned by
    // test_ui_breakdown_field_not_column_stored_refuses: the UI auto-picked
    // `severity`, which is not in the stream's column_store_fields.
    // ------------------------------------------------------------------

    use crate::{
        datafusion::optimizer::physical_optimizer::index_optimizer::test_harness::follower_extracted_mode,
        sql::visitor::histogram_interval::{
            convert_histogram_interval_to_seconds, generate_histogram_interval,
            validate_and_adjust_histogram_interval,
        },
    };

    /// date_bin's origin (`2001-01-01T00:00:00` UTC) in microseconds — the
    /// bucket edges the histogram() UDF produces are `origin + k * width`.
    const DATE_BIN_ORIGIN_US: i64 = 978_307_200_000_000;

    /// The schema every UI-shape test plans against.
    fn ui_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("level", DataType::Utf8, true),
            Field::new("severity", DataType::Utf8, true),
            Field::new("kubernetes.namespace.name", DataType::Utf8, true),
        ]))
    }

    /// The EXACT SQL the UI histogram generates, via the real generator. The
    /// generator now resolves the interval from the full request range and
    /// emits it explicitly; the aliases and shape stay the UI contract.
    fn ui_histogram_sql(breakdown_field: Option<&str>, time_range: (i64, i64)) -> String {
        let sql = crate::sql::histogram::convert_to_histogram_query(
            "SELECT * FROM \"t\"",
            &["t".to_string()],
            false,
            breakdown_field,
            time_range,
            0,
        )
        .unwrap();
        // pin the generated shape this suite is contractually testing
        let interval = generate_histogram_interval(time_range);
        match breakdown_field {
            None => assert_eq!(
                sql,
                format!(
                    "SELECT histogram(_timestamp, '{interval}') AS zo_sql_key, count(*) AS \
                     zo_sql_num FROM \"t\" GROUP BY zo_sql_key ORDER BY zo_sql_key DESC"
                )
            ),
            Some(field) => assert_eq!(
                sql,
                format!(
                    "SELECT histogram(_timestamp, '{interval}') AS zo_sql_key, \"{field}\" AS \
                     zo_sql_breakdown, count(*) AS zo_sql_num FROM \"t\" GROUP BY zo_sql_key, \
                     zo_sql_breakdown ORDER BY zo_sql_key DESC"
                )
            ),
        }
        sql
    }

    /// The pre-explicit-interval UI shape: a 1-arg `histogram(_timestamp)`.
    /// Still a public SQL feature (dashboards / custom SQL) — the detectors
    /// must keep matching it.
    fn one_arg_histogram_sql(breakdown_field: Option<&str>) -> String {
        match breakdown_field {
            None => "SELECT histogram(_timestamp) AS zo_sql_key, count(*) AS zo_sql_num FROM \
                     \"t\" GROUP BY zo_sql_key ORDER BY zo_sql_key DESC"
                .to_string(),
            Some(field) => format!(
                "SELECT histogram(_timestamp) AS zo_sql_key, \"{field}\" AS zo_sql_breakdown, \
                 count(*) AS zo_sql_num FROM \"t\" GROUP BY zo_sql_key, zo_sql_breakdown ORDER \
                 BY zo_sql_key DESC"
            ),
        }
    }

    /// The bucket width (µs) production resolves for a 1-arg
    /// `histogram(_timestamp)` over `time_range` — the exact formula chain
    /// the streaming path presets (`HistogramIntervalVisitor`:
    /// generate_histogram_interval → seconds → validate_and_adjust) and
    /// RewriteHistogram's own auto fallback reduce to.
    fn expected_auto_width_micros(time_range: (i64, i64)) -> u64 {
        let interval = generate_histogram_interval(time_range);
        let secs = convert_histogram_interval_to_seconds(interval).unwrap();
        let secs = validate_and_adjust_histogram_interval(secs, time_range);
        (secs * 1_000_000) as u64
    }

    /// Assert the extracted plain-histogram params serve the histogram()
    /// UDF's buckets exactly: the width equals the production auto-interval,
    /// `min_value` is a real date_bin bucket edge (origin-aligned — a
    /// misaligned edge would land counts in wrong buckets), and
    /// `[min_value, min_value + num_buckets * width)` is the smallest such
    /// cover of the query range.
    fn assert_simple_histogram_params(
        mode: &IndexOptimizeMode,
        time_range: (i64, i64),
        expected_width: u64,
        context: &str,
    ) {
        let IndexOptimizeMode::SimpleHistogram(min_value, width, num_buckets, ts_offset) = mode
        else {
            panic!("{context}: expected SimpleHistogram, got {mode:?}");
        };
        let (start, end) = time_range;
        assert_eq!(*width, expected_width, "{context}: bucket width");
        assert_eq!(*ts_offset, 0, "{context}: ts_offset");
        let width = *width as i64;
        assert_eq!(
            (min_value - DATE_BIN_ORIGIN_US).rem_euclid(width),
            0,
            "{context}: min_value {min_value} is not a date_bin bucket edge"
        );
        assert!(
            *min_value <= start && start - min_value < width,
            "{context}: min_value {min_value} does not floor start {start} to its bucket"
        );
        let covered = min_value + (*num_buckets as i64) * width;
        assert!(
            covered >= end && covered - width < end,
            "{context}: {num_buckets} buckets from {min_value} do not tightly cover end {end}"
        );
    }

    /// The two exact UI shapes, across window sizes exercising different
    /// auto-interval steps, in BOTH SQL forms (the generated
    /// explicit-interval form and the still-public 1-arg auto form) and BOTH
    /// production configurations: the streaming path (histogram_interval
    /// preset from the full range) and the plain `_search` path
    /// (auto-resolved inside RewriteHistogram). The extracted bucket width
    /// must equal the UDF's auto-interval for the same range in every
    /// combination.
    #[tokio::test]
    async fn test_follower_extracts_exact_ui_histogram_sql_across_windows() {
        let start_time = 1_757_401_694_060_000i64; // deliberately bucket-misaligned
        let minute = 60 * 1_000_000i64;
        // (window, expected auto interval) — one window per tier of
        // generate_histogram_interval that the UI realistically produces
        let windows = [
            (10 * minute, "10 second"),
            (40 * minute, "15 second"),
            (90 * minute, "30 second"),
            (3 * 60 * minute, "1 minute"),
            (8 * 60 * minute, "1 hour"),
            (22 * 24 * 60 * minute, "3 hour"),
            (29 * 24 * 60 * minute, "6 hour"),
            (70 * 24 * 60 * minute, "1 day"),
        ];

        let index_fields = HashSet::from(["level".to_string()]);

        for (window, expected_interval) in windows {
            let time_range = (start_time, start_time + window);
            assert_eq!(
                generate_histogram_interval(time_range),
                expected_interval,
                "window {window} should exercise the {expected_interval} tier"
            );
            let width = expected_auto_width_micros(time_range);
            let preset_secs = (width / 1_000_000) as i64;

            let plain_sqls = [
                ("generated", ui_histogram_sql(None, time_range)),
                ("one-arg", one_arg_histogram_sql(None)),
            ];
            let breakdown_sqls = [
                ("generated", ui_histogram_sql(Some("level"), time_range)),
                ("one-arg", one_arg_histogram_sql(Some("level"))),
            ];

            // streaming (preset) and plain `_search` (auto) configurations
            for (config_name, preset) in [("streaming-preset", preset_secs), ("auto", 0)] {
                for (form, plain_sql) in &plain_sqls {
                    let context = format!("plain/{form}/{config_name}/{expected_interval}");
                    let mode = follower_extracted_mode(
                        plain_sql,
                        ui_schema(),
                        time_range,
                        preset,
                        index_fields.clone(),
                    )
                    .await
                    .unwrap_or_else(|| panic!("{context}: no mode extracted"));
                    assert_simple_histogram_params(&mode, time_range, width, &context);
                }

                for (form, breakdown_sql) in &breakdown_sqls {
                    let context = format!("breakdown/{form}/{config_name}/{expected_interval}");
                    let mode = follower_extracted_mode(
                        breakdown_sql,
                        ui_schema(),
                        time_range,
                        preset,
                        index_fields.clone(),
                    )
                    .await
                    .unwrap_or_else(|| panic!("{context}: no mode extracted"));
                    let IndexOptimizeMode::SimpleMultiHistogram(
                        min_value,
                        max_value,
                        got_width,
                        ts_offset,
                        field,
                    ) = &mode
                    else {
                        panic!("{context}: expected SimpleMultiHistogram, got {mode:?}");
                    };
                    assert_eq!(*got_width, width, "{context}: bucket width");
                    assert_eq!(*ts_offset, 0, "{context}: ts_offset");
                    assert_eq!(field, "level", "{context}: breakdown field");
                    assert_eq!(*max_value, time_range.1, "{context}: max_value");
                    assert_eq!(
                        (min_value - DATE_BIN_ORIGIN_US).rem_euclid(width as i64),
                        0,
                        "{context}: min_value {min_value} is not a date_bin bucket edge"
                    );
                    assert!(
                        *min_value <= time_range.0 && time_range.0 - min_value < width as i64,
                        "{context}: min_value {min_value} does not floor start"
                    );
                }
            }
        }
    }

    /// A dotted quoted breakdown field — the live `column_store_fields`
    /// shape (`"kubernetes.namespace.name"`) — maps to SimpleMultiHistogram
    /// carrying the dotted name verbatim.
    #[tokio::test]
    async fn test_follower_extracts_ui_breakdown_with_dotted_quoted_field() {
        let start_time = 1_757_401_694_060_000i64;
        let time_range = (start_time, start_time + 3 * 3600 * 1_000_000);
        let expected = Some(IndexOptimizeMode::SimpleMultiHistogram(
            1_757_401_680_000_000,
            time_range.1,
            60_000_000,
            0,
            "kubernetes.namespace.name".to_string(),
        ));
        for sql in [
            ui_histogram_sql(Some("kubernetes.namespace.name"), time_range),
            one_arg_histogram_sql(Some("kubernetes.namespace.name")),
        ] {
            let mode = follower_extracted_mode(
                &sql,
                ui_schema(),
                time_range,
                60,
                HashSet::from(["kubernetes.namespace.name".to_string()]),
            )
            .await;
            assert_eq!(mode, expected, "sql: {sql}");
        }
    }

    /// The live root cause of the "UI streaming histogram bypasses the fast
    /// paths" incident: the UI auto-picks a breakdown field by schema-name
    /// priority (`severity` on the dev stream) with no regard to
    /// column-store eligibility. A breakdown field outside the stream's
    /// `column_store_fields` MUST refuse — the collector reads the
    /// breakdown from the docs column, which no file carries for such a
    /// field — and the query takes the DataFusion branch.
    #[tokio::test]
    async fn test_ui_breakdown_field_not_column_stored_refuses() {
        let start_time = 1_757_401_694_060_000i64;
        let time_range = (start_time, start_time + 3 * 3600 * 1_000_000);
        let sql = ui_histogram_sql(Some("severity"), time_range);
        // the stream's column_store_fields (the live dev set) lack `severity`
        let column_store_fields =
            HashSet::from(["kubernetes.namespace.name".to_string(), "level".to_string()]);
        let mode =
            follower_extracted_mode(&sql, ui_schema(), time_range, 60, column_store_fields).await;
        assert_eq!(mode, None);
    }

    /// ORDER BY direction and output aliases are irrelevant to matching:
    /// the sort sits above the leader's final aggregate while the follower
    /// matches the shipped partial-aggregate sub-plan.
    #[tokio::test]
    async fn test_follower_histogram_order_and_alias_variants() {
        let start_time = 1_757_401_694_060_000i64;
        let time_range = (start_time, start_time + 3 * 3600 * 1_000_000);
        let expected = Some(IndexOptimizeMode::SimpleHistogram(
            1_757_401_680_000_000,
            60_000_000,
            181,
            0,
        ));
        let cases = [
            // the exact UI shape (DESC)
            "SELECT histogram(_timestamp) AS zo_sql_key, count(*) AS zo_sql_num FROM \"t\" GROUP \
             BY zo_sql_key ORDER BY zo_sql_key DESC",
            // ASC
            "SELECT histogram(_timestamp) AS zo_sql_key, count(*) AS zo_sql_num FROM \"t\" GROUP \
             BY zo_sql_key ORDER BY zo_sql_key",
            // no ORDER BY
            "SELECT histogram(_timestamp) AS zo_sql_key, count(*) AS zo_sql_num FROM \"t\" GROUP \
             BY zo_sql_key",
            // different aliases, explicit interval equal to the preset
            "SELECT histogram(_timestamp, '1 minute') AS ts, count(*) AS cnt FROM \"t\" GROUP BY \
             ts ORDER BY ts DESC",
        ];
        for sql in cases {
            let mode =
                follower_extracted_mode(sql, ui_schema(), time_range, 60, HashSet::new()).await;
            assert_eq!(mode, expected, "sql: {sql}");
        }

        // breakdown variants: DESC (the UI shape) and no ORDER BY agree
        let expected_multi = Some(IndexOptimizeMode::SimpleMultiHistogram(
            1_757_401_680_000_000,
            time_range.1,
            60_000_000,
            0,
            "level".to_string(),
        ));
        let multi_cases = [
            "SELECT histogram(_timestamp) AS zo_sql_key, \"level\" AS zo_sql_breakdown, count(*) \
             AS zo_sql_num FROM \"t\" GROUP BY zo_sql_key, zo_sql_breakdown ORDER BY zo_sql_key \
             DESC",
            "SELECT histogram(_timestamp) AS zo_sql_key, \"level\" AS zo_sql_breakdown, count(*) \
             AS zo_sql_num FROM \"t\" GROUP BY zo_sql_key, zo_sql_breakdown",
        ];
        for sql in multi_cases {
            let mode = follower_extracted_mode(
                sql,
                ui_schema(),
                time_range,
                60,
                HashSet::from(["level".to_string()]),
            )
            .await;
            assert_eq!(mode, expected_multi, "sql: {sql}");
        }
    }

    #[test]
    fn test_simple_histogram_visitor_initial_state() {
        let visitor = SimpleHistogramVisitor::new((1000, 2000));
        assert!(visitor.simple_histogram.is_none());
        assert_eq!(visitor.time_range, (1000, 2000));
    }

    #[test]
    fn test_simple_histogram_visitor_zero_time_range() {
        let visitor = SimpleHistogramVisitor::new((0, 0));
        assert!(visitor.simple_histogram.is_none());
        assert_eq!(visitor.time_range, (0, 0));
    }

    #[test]
    fn test_simple_histogram_visitor_negative_time_range() {
        let visitor = SimpleHistogramVisitor::new((-1000, -500));
        assert_eq!(visitor.time_range, (-1000, -500));
    }

    #[test]
    fn test_simple_multi_histogram_visitor_initial_state() {
        let visitor =
            SimpleMultiHistogramVisitor::new((1000, 2000), HashSet::from(["level".to_string()]));
        assert!(visitor.simple_multi_histogram.is_none());
        assert_eq!(visitor.time_range, (1000, 2000));
        assert_eq!(visitor.index_fields.len(), 1);
    }

    #[test]
    fn test_simple_multi_histogram_visitor_empty_index_fields() {
        let visitor = SimpleMultiHistogramVisitor::new((1000, 2000), HashSet::new());
        assert!(visitor.simple_multi_histogram.is_none());
        assert!(visitor.index_fields.is_empty());
    }
}
