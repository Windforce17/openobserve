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

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, atomic::Ordering},
};

use config::meta::search::{PARTIAL_ERROR_RESPONSE_MESSAGE, ScanStats};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanVisitor};
use infra::runtime::DATAFUSION_RUNTIME;
use sqlparser::ast::{BinaryOperator, Expr};
use tokio::sync::Mutex;

use super::datafusion::distributed_plan::remote_scan_exec::RemoteScanExec;
use crate::{CAPPED_RESULTS_MSG, sql::Sql};

type Cleanup = Pin<Box<dyn Future<Output = ()> + Send>>;

/// A utility for running an asynchronous cleanup function when a value is dropped.
pub struct AsyncDefer {
    cleanup: Option<Arc<Mutex<Cleanup>>>,
}

impl AsyncDefer {
    pub fn new<F>(cleanup: F) -> Self
    where
        F: Future<Output = ()> + Send + 'static,
    {
        AsyncDefer {
            cleanup: Some(Arc::new(Mutex::new(Box::pin(cleanup)))),
        }
    }
}

impl Drop for AsyncDefer {
    fn drop(&mut self) {
        if let Some(cleanup) = self.cleanup.take() {
            DATAFUSION_RUNTIME.spawn(async move {
                let mut cleanup = cleanup.lock().await;
                cleanup.as_mut().await;
            });
        }
    }
}

/// A spawned search task that is ABORTED if its owner future is dropped
/// before completion — the owner being dropped is how a client disconnect
/// or cancel reaches us (actix drops the handler future; every layer above
/// holds this guard instead of a bare `JoinHandle`, which would detach).
/// Aborting the leader task drops its flight client streams, which cancels
/// the follower RPCs (tonic RST_STREAM), whose encoder streams run their
/// own drop cleanup — so one drop tears the whole query down.
pub struct AbortOnDrop<T> {
    handle: tokio::task::JoinHandle<T>,
    trace_id: String,
    done: bool,
}

impl<T> AbortOnDrop<T> {
    pub fn new(handle: tokio::task::JoinHandle<T>, trace_id: impl Into<String>) -> Self {
        Self {
            handle,
            trace_id: trace_id.into(),
            done: false,
        }
    }

    /// Await completion. Cancel-safe: if the surrounding select!/future is
    /// dropped mid-poll the task is aborted by the guard's Drop.
    pub async fn join(&mut self) -> Result<T, tokio::task::JoinError> {
        let ret = (&mut self.handle).await;
        self.done = true;
        ret
    }

    pub fn abort(&mut self) {
        self.done = true; // deliberate abort: Drop stays silent
        self.handle.abort();
    }
}

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        if !self.done && !self.handle.is_finished() {
            self.handle.abort();
            log::info!(
                "[trace_id {}] search task aborted on drop (client disconnect or cancel)",
                self.trace_id
            );
        }
    }
}

#[derive(Debug)]
pub struct ScanStatsVisitor {
    pub scan_stats: ScanStats,
    pub peak_memory: usize,
    pub partial_err: String,
}

impl Default for ScanStatsVisitor {
    fn default() -> Self {
        Self::new()
    }
}

impl ScanStatsVisitor {
    pub fn new() -> Self {
        ScanStatsVisitor {
            scan_stats: ScanStats::default(),
            peak_memory: 0,
            partial_err: String::new(),
        }
    }
}

impl ExecutionPlanVisitor for ScanStatsVisitor {
    type Error = datafusion::common::DataFusionError;

    fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
        let mayby_remote_scan_exec = plan.downcast_ref::<RemoteScanExec>();
        if let Some(remote_scan_exec) = mayby_remote_scan_exec {
            {
                let guard = remote_scan_exec.scan_stats.lock();
                let stats = *guard;
                self.scan_stats.add(&stats);
            }
            {
                let guard = remote_scan_exec.partial_err.lock();
                let err = (*guard).clone();
                self.partial_err.push_str(&err);
            }
            {
                let peak_memory = remote_scan_exec.peak_memory().load(Ordering::Relaxed);
                self.peak_memory = peak_memory.max(self.peak_memory);
            }
        }
        Ok(true)
    }
}

// split an expression by AND operator
pub fn split_conjunction(expr: &Expr) -> Vec<&Expr> {
    split_conjunction_inner(expr, Vec::new())
}

fn split_conjunction_inner<'a>(expr: &'a Expr, mut exprs: Vec<&'a Expr>) -> Vec<&'a Expr> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let exprs = split_conjunction_inner(left, exprs);
            split_conjunction_inner(right, exprs)
        }
        Expr::Nested(expr) => split_conjunction_inner(expr, exprs),
        other => {
            exprs.push(other);
            exprs
        }
    }
}

// conjunction the exprs
pub fn conjunction(exprs: Vec<&Expr>) -> Option<Expr> {
    if exprs.is_empty() {
        None
    } else if exprs.len() == 1 {
        Some(exprs[0].clone())
    } else {
        // conjuction all expr in exprs
        let mut expr = exprs[0].clone();
        if matches!(
            expr,
            Expr::BinaryOp {
                op: BinaryOperator::Or,
                ..
            }
        ) {
            expr = Expr::Nested(Box::new(expr));
        }
        for e in exprs.into_iter().skip(1) {
            expr = Expr::BinaryOp {
                left: Box::new(expr),
                op: BinaryOperator::And,
                right: if matches!(
                    e,
                    Expr::BinaryOp {
                        op: BinaryOperator::Or,
                        ..
                    }
                ) {
                    Box::new(Expr::Nested(Box::new(e.clone())))
                } else {
                    Box::new(e.clone())
                },
            }
        }
        Some(expr)
    }
}

pub fn trim_quotes(s: &str) -> String {
    let s = s
        .strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .unwrap_or(s);
    s.strip_prefix('\'')
        .and_then(|s| s.strip_suffix('\''))
        .unwrap_or(s)
        .to_string()
}

pub fn is_value(e: &Expr) -> bool {
    matches!(e, Expr::Value(_))
}

pub fn is_field(e: &Expr) -> bool {
    matches!(e, Expr::Identifier(_) | Expr::CompoundIdentifier(_))
}

pub fn check_query_default_limit_exceeded(num_rows: usize, partial_err: &mut String, sql: &Sql) {
    if is_default_query_limit_exceeded(num_rows, sql) {
        let capped_err = CAPPED_RESULTS_MSG.to_string();
        if !partial_err.is_empty() {
            partial_err.push('\n');
            partial_err.push_str(&capped_err);
        } else {
            *partial_err = capped_err;
        }
    }
}

pub fn is_default_query_limit_exceeded(num_rows: usize, sql: &Sql) -> bool {
    let default_query_limit = config::get_config().limit.query_default_limit as usize;
    if sql.limit > config::QUERY_WITH_NO_LIMIT && sql.limit <= 0 {
        num_rows > default_query_limit
    } else {
        false
    }
}

pub fn is_permissable_function_error(function_error: &[String]) -> bool {
    if function_error.is_empty() {
        return true;
    }

    function_error.iter().all(|error| {
        // Empty or whitespace-only errors are cachable (no actual error)
        let trimmed = error.trim();
        if trimmed.is_empty() {
            return true;
        }

        // Check if error contains only cachable messages
        error.contains(CAPPED_RESULTS_MSG) || error.contains(PARTIAL_ERROR_RESPONSE_MESSAGE)
    })
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::{BinaryOperator, Expr, Ident, Value};

    use super::*;

    #[test]
    fn test_is_cachable_function_error() {
        let error = vec![];
        assert!(is_permissable_function_error(&error));

        let error = vec!["".to_string()];
        assert!(is_permissable_function_error(&error));

        let error = vec![
            CAPPED_RESULTS_MSG.to_string(),
            PARTIAL_ERROR_RESPONSE_MESSAGE.to_string(),
        ];
        assert!(is_permissable_function_error(&error)); // only this is cachable

        let error = vec![
            CAPPED_RESULTS_MSG.to_string(),
            PARTIAL_ERROR_RESPONSE_MESSAGE.to_string(),
            "parquet not found".to_string(),
        ];
        assert!(!is_permissable_function_error(&error));

        let error = vec![
            "parquet not found".to_string(),
            PARTIAL_ERROR_RESPONSE_MESSAGE.to_string(),
        ];
        assert!(!is_permissable_function_error(&error));

        let error = vec!["parquet not found".to_string()];
        assert!(!is_permissable_function_error(&error));

        let error = vec![
            "parquet not found".to_string(),
            CAPPED_RESULTS_MSG.to_string(),
        ];
        assert!(!is_permissable_function_error(&error));
    }

    #[test]
    fn test_split_conjunction() {
        // Test simple expression
        let expr = Expr::Value(Value::Number("1".to_string(), false).into());
        let result = split_conjunction(&expr);
        assert_eq!(result.len(), 1);
        assert!(matches!(result[0], Expr::Value(_)));

        // Test AND expression
        let left = Expr::Value(Value::Number("1".to_string(), false).into());
        let right = Expr::Value(Value::Number("2".to_string(), false).into());
        let and_expr = Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::And,
            right: Box::new(right),
        };
        let result = split_conjunction(&and_expr);
        assert_eq!(result.len(), 2);
        assert!(matches!(result[0], Expr::Value(_)));
        assert!(matches!(result[1], Expr::Value(_)));

        // Test nested AND expressions
        let nested_right = Expr::Value(Value::Number("3".to_string(), false).into());
        let nested_and = Expr::BinaryOp {
            left: Box::new(and_expr),
            op: BinaryOperator::And,
            right: Box::new(nested_right),
        };
        let result = split_conjunction(&nested_and);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_conjunction() {
        // Test empty vector
        let exprs: Vec<&Expr> = vec![];
        let result = conjunction(exprs);
        assert!(result.is_none());

        // Test single expression
        let expr = Expr::Value(Value::Number("1".to_string(), false).into());
        let exprs = vec![&expr];
        let result = conjunction(exprs);
        assert!(result.is_some());
        assert!(matches!(result.unwrap(), Expr::Value(_)));

        // Test multiple expressions
        let expr1 = Expr::Value(Value::Number("1".to_string(), false).into());
        let expr2 = Expr::Value(Value::Number("2".to_string(), false).into());
        let exprs = vec![&expr1, &expr2];
        let result = conjunction(exprs);
        assert!(result.is_some());
        if let Some(Expr::BinaryOp { op, .. }) = result {
            assert!(matches!(op, BinaryOperator::And));
        } else {
            panic!("Expected BinaryOp with And operator");
        }
    }

    #[test]
    fn test_trim_quotes() {
        let inp_exp_arr = [
            ("\"hello\"", "hello"),
            ("'world'", "world"),
            ("no_quotes", "no_quotes"),
            ("\"partial", "\"partial"),
            ("partial\"", "partial\""),
            ("", ""),
            ("\"mixed'quotes\"", "mixed'quotes"),
        ];
        for (input, expected) in inp_exp_arr {
            assert_eq!(trim_quotes(input), expected);
        }
    }

    #[test]
    fn test_is_value() {
        // Test value expression
        let value_expr = Expr::Value(Value::Number("123".to_string(), false).into());
        assert!(is_value(&value_expr));

        // Test non-value expression
        let ident_expr = Expr::Identifier(Ident::new("field".to_string()));
        assert!(!is_value(&ident_expr));
    }

    #[test]
    fn test_is_field() {
        // Test identifier expression
        let ident_expr = Expr::Identifier(Ident::new("field".to_string()));
        assert!(is_field(&ident_expr));

        // Test compound identifier expression
        let compound_expr = Expr::CompoundIdentifier(vec![
            Ident::new("table".to_string()),
            Ident::new("field".to_string()),
        ]);
        assert!(is_field(&compound_expr));

        // Test non-field expression
        let value_expr = Expr::Value(Value::Number("123".to_string(), false).into());
        assert!(!is_field(&value_expr));
    }

    #[tokio::test]
    async fn test_async_defer() {
        let cleanup_called = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let cleanup_called_clone = cleanup_called.clone();

        {
            let _defer = AsyncDefer::new(async move {
                cleanup_called_clone.store(true, std::sync::atomic::Ordering::Relaxed);
            });
            // Defer should be dropped here and cleanup should be called
        }

        // Give some time for the async cleanup to run
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        assert!(cleanup_called.load(std::sync::atomic::Ordering::Relaxed));
    }

    #[test]
    fn test_scan_stats_visitor_new() {
        let visitor = ScanStatsVisitor::new();
        assert_eq!(visitor.partial_err, "");
    }

    #[test]
    fn test_scan_stats_visitor_pre_visit() {
        let mut visitor = ScanStatsVisitor::new();

        // Create a mock execution plan that's not a RemoteScanExec
        let mock_plan = datafusion::physical_plan::empty::EmptyExec::new(Arc::new(
            datafusion::arrow::datatypes::Schema::empty(),
        ));

        let result = visitor.pre_visit(&mock_plan);
        assert!(result.is_ok_and(|v| v));
    }

    /// Dropping the guard aborts the task — pinned by watching the task's
    /// locals drop while it would otherwise run forever. This is the
    /// client-disconnect cancellation path: the handler future drops, the
    /// guard aborts the search instead of detaching it.
    #[tokio::test]
    async fn abort_on_drop_cancels_the_task() {
        use std::sync::atomic::{AtomicBool, Ordering};

        struct Probe(Arc<AtomicBool>);
        impl Drop for Probe {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let dropped = Arc::new(AtomicBool::new(false));
        let probe = Probe(Arc::clone(&dropped));
        let guard = super::AbortOnDrop::new(
            tokio::spawn(async move {
                let _probe = probe;
                std::future::pending::<()>().await;
            }),
            "test-abort",
        );
        drop(guard);

        for _ in 0..100 {
            if dropped.load(Ordering::SeqCst) {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("task was not aborted within 1s of the guard dropping");
    }

    /// A task that finished (or was deliberately aborted via the guard)
    /// must not be treated as a disconnect by the drop path.
    #[tokio::test]
    async fn abort_on_drop_join_completes_normally() {
        let mut guard = super::AbortOnDrop::new(tokio::spawn(async { 41 + 1 }), "test-join");
        let ret = guard.join().await.expect("task completes");
        assert_eq!(ret, 42);
        drop(guard);
    }
}
