// Copyright 2026 OpenObserve Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use config::meta::cluster::QUERY_ENGINE_GENERATION;
use tonic::{Status, metadata::MetadataMap};

pub const GENERATION_HEADER: &str = "x-o2-query-engine-generation";
pub const GENERATION_ERROR: &str = "query engine generation mismatch";

pub fn set_generation(metadata: &mut MetadataMap) {
    metadata.insert(
        GENERATION_HEADER,
        tonic::metadata::MetadataValue::from_static(QUERY_ENGINE_GENERATION),
    );
}

pub fn check_generation(
    peer: Option<&str>,
    boundary: &str,
    node_name: &str,
    trace_id: &str,
) -> Result<(), Status> {
    if peer == Some(QUERY_ENGINE_GENERATION) {
        return Ok(());
    }
    let mut status = Status::failed_precondition(format!(
        "{GENERATION_ERROR}: {boundary}, local={QUERY_ENGINE_GENERATION}, peer={}, node={node_name}, trace_id={trace_id}; refusing incompatible partition",
        peer.unwrap_or("unknown (missing)"),
    ));
    set_generation(status.metadata_mut());
    Err(status)
}

pub fn validate_generation(
    metadata: &MetadataMap,
    boundary: &str,
    node_name: &str,
    trace_id: &str,
) -> Result<(), Status> {
    let mut values = metadata.get_all(GENERATION_HEADER).iter();
    let peer = values.next().map(|v| v.to_str().unwrap_or("invalid"));
    // Ambiguous headers must not turn a mismatch into a match.
    let peer = if values.next().is_some() {
        Some("duplicate")
    } else {
        peer
    };
    check_generation(peer, boundary, node_name, trace_id)
}

pub fn is_generation_error(status: &Status) -> bool {
    status.code() == tonic::Code::FailedPrecondition
        && status.metadata().contains_key(GENERATION_HEADER)
        && status.message().starts_with(GENERATION_ERROR)
}

pub fn verified_response<T>(
    response: tonic::Response<T>,
    node_name: &str,
    trace_id: &str,
) -> Result<T, Status> {
    validate_generation(response.metadata(), "response", node_name, trace_id)?;
    Ok(response.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engine_generation_same_request_and_response() {
        for boundary in ["request", "response"] {
            let mut metadata = MetadataMap::new();
            set_generation(&mut metadata);
            assert!(validate_generation(&metadata, boundary, "node-a", "trace-a").is_ok());
        }
    }

    #[test]
    fn engine_generation_missing_wrong_empty_and_duplicate_are_rejected() {
        for peer in [None, Some("df54-arrow58"), Some("")] {
            for boundary in ["request", "response"] {
                let mut metadata = MetadataMap::new();
                if let Some(peer) = peer {
                    metadata.insert(GENERATION_HEADER, peer.parse().unwrap());
                }
                let status =
                    validate_generation(&metadata, boundary, "node-b", "trace-b").unwrap_err();
                assert_eq!(status.code(), tonic::Code::FailedPrecondition);
                assert!(is_generation_error(&status));
                for expected in [
                    QUERY_ENGINE_GENERATION,
                    "node-b",
                    "trace-b",
                    boundary,
                    peer.unwrap_or("unknown (missing)"),
                ] {
                    assert!(status.message().contains(expected), "{}", status.message());
                }
            }
        }
        let mut metadata = MetadataMap::new();
        set_generation(&mut metadata);
        metadata.append(GENERATION_HEADER, "df54-arrow58".parse().unwrap());
        assert!(
            validate_generation(&metadata, "response", "n", "t")
                .unwrap_err()
                .message()
                .contains("duplicate")
        );
        assert!(!is_generation_error(&Status::failed_precondition(
            "unrelated failure"
        )));
    }

    #[test]
    fn engine_generation_response_is_rejected_without_polling_body() {
        struct MustNotPoll;
        impl futures::Stream for MustNotPoll {
            type Item = ();
            fn poll_next(
                self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Option<()>> {
                panic!("an unverified response must not be polled");
            }
        }
        let response = tonic::Response::new(MustNotPoll);
        assert!(verified_response(response, "old-follower", "t").is_err());
        let mut response = tonic::Response::new(42);
        set_generation(response.metadata_mut());
        assert_eq!(
            verified_response(response, "new-follower", "t").unwrap(),
            42
        );
    }
}
