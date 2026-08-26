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

//! OSS query-abort registry (#36/.81). The enterprise build tracks running
//! queries in SEARCH_SERVER and cancels them by firing per-query abort
//! senders; on OSS that machinery — and with it BOTH cancellation paths
//! (the query_manager cancel API and the client-disconnect stream guard) —
//! was feature-gated away entirely. This is the minimal OSS mirror: the
//! query LEADER registers its trace_id here for the lifetime of the flight
//! search; `cancel_local` fires the abort channel that the leader's
//! `tokio::select!` races against the datafusion task. Aborting the leader
//! is sufficient — its follower flight streams drop and pull-based
//! execution cancels with them.

use std::sync::LazyLock;

use dashmap::DashMap;
use tokio::sync::oneshot;

static REGISTRY: LazyLock<DashMap<String, oneshot::Sender<()>>> = LazyLock::new(DashMap::new);

/// RAII registration: dropping it (any exit path of the leader search)
/// removes the entry, so the registry cannot leak trace_ids.
pub struct AbortRegistration {
    trace_id: String,
}

impl Drop for AbortRegistration {
    fn drop(&mut self) {
        REGISTRY.remove(&self.trace_id);
    }
}

/// Register a leader search; the returned receiver resolves when someone
/// cancels this trace_id (or errors when the registration drops — the
/// select! treats both as "stop waiting on this arm").
pub fn register(trace_id: &str) -> (AbortRegistration, oneshot::Receiver<()>) {
    let (tx, rx) = oneshot::channel();
    REGISTRY.insert(trace_id.to_string(), tx);
    (
        AbortRegistration {
            trace_id: trace_id.to_string(),
        },
        rx,
    )
}

/// Fire the abort channel for `trace_id` and any sub-query registered
/// under a `"{trace_id}-..."` key (internal jobs suffix the leader id).
/// Returns how many registrations were cancelled.
pub fn cancel_local(trace_id: &str) -> usize {
    if trace_id.is_empty() {
        return 0;
    }
    let prefix = format!("{trace_id}-");
    let keys: Vec<String> = REGISTRY
        .iter()
        .map(|e| e.key().clone())
        .filter(|k| k == trace_id || k.starts_with(&prefix))
        .collect();
    let mut cancelled = 0;
    for key in keys {
        if let Some((_, sender)) = REGISTRY.remove(&key) {
            let _ = sender.send(());
            cancelled += 1;
        }
    }
    cancelled
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancel_fires_the_receiver_and_prefix_matches() {
        let (_reg_a, rx_a) = register("trace-cancel-1");
        let (_reg_b, rx_b) = register("trace-cancel-1-job2");
        let (_reg_c, rx_c) = register("trace-other");

        assert_eq!(cancel_local("trace-cancel-1"), 2, "exact + prefix");
        rx_a.await.expect("exact match must fire");
        rx_b.await.expect("prefix match must fire");

        // the unrelated query is untouched and its registration still works
        assert_eq!(cancel_local("trace-other"), 1);
        rx_c.await
            .expect("unrelated query fires only when targeted");
    }

    #[tokio::test]
    async fn dropping_the_registration_cleans_up() {
        let (reg, mut rx) = register("trace-drop-1");
        drop(reg);
        assert_eq!(cancel_local("trace-drop-1"), 0, "entry must be gone");
        assert!(rx.try_recv().is_err(), "sender dropped, never fired");
    }
}
