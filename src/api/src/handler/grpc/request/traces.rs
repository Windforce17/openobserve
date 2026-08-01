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

use axum::http::StatusCode as HttpStatusCode;
use config::{meta::otlp::OtlpRequestType, metrics};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse, trace_service_server::TraceService,
};
use prost::Message;
use tonic::{Response, Status};

use crate::{
    common::meta::{http::ERROR_HEADER, ingestion::IngestUser},
    service::traces::handle_otlp_request,
};

/// Cap when draining the ingest layer's (self-produced, small) response body.
const MAX_RESPONSE_BODY: usize = 1024 * 1024;

/// The tonic code for a non-2xx ingest status: 503 (backpressure) is
/// UNAVAILABLE and 429 (throttling) is RESOURCE_EXHAUSTED so the collector
/// backs off and retries; a 4xx is the caller's fault and must not be
/// retried; anything else is INTERNAL.
fn grpc_status(http_status: HttpStatusCode, message: String) -> Status {
    match http_status {
        HttpStatusCode::SERVICE_UNAVAILABLE => Status::unavailable(message),
        HttpStatusCode::TOO_MANY_REQUESTS => Status::resource_exhausted(message),
        HttpStatusCode::FORBIDDEN => Status::permission_denied(message),
        HttpStatusCode::UNAUTHORIZED => Status::unauthenticated(message),
        s if s.is_client_error() => Status::invalid_argument(message),
        _ => Status::internal(message),
    }
}

/// Map the ingest layer's HTTP response onto the gRPC reply.
///
/// The response carries the truth about the durable write: the status code
/// (500/503 when spans were lost) and the REAL `partial_success`
/// (`rejected_spans` + error message). Discarding it and answering
/// `partial_success: None` acked a lost batch as a fully accepted export,
/// and the collector deleted its only copy.
async fn export_response_from_http(
    response: axum::response::Response,
) -> Result<tonic::Response<ExportTraceServiceResponse>, Status> {
    let http_status = response.status();
    if http_status.is_success() {
        // the gRPC request type produces a protobuf body on success:
        // propagate the real partial_success (rejected_spans + error)
        let body = axum::body::to_bytes(response.into_body(), MAX_RESPONSE_BODY)
            .await
            .map_err(|e| Status::internal(format!("failed to read trace ingest response: {e}")))?;
        let res = ExportTraceServiceResponse::decode(body).map_err(|e| {
            Status::internal(format!("failed to decode trace ingest response: {e}"))
        })?;
        return Ok(Response::new(res));
    }

    // non-2xx bodies are JSON; the write-failure path also names the error in
    // the X-Error-Message header
    let header_message = response
        .headers()
        .get(ERROR_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.to_string());
    let message = match header_message {
        Some(message) => message,
        None => {
            let body = axum::body::to_bytes(response.into_body(), MAX_RESPONSE_BODY)
                .await
                .unwrap_or_default();
            let text = String::from_utf8_lossy(&body).trim().to_string();
            if text.is_empty() {
                format!("trace ingestion failed with HTTP status {http_status}")
            } else {
                text
            }
        }
    };
    Err(grpc_status(http_status, message))
}

#[derive(Default)]
pub struct TraceServer;

#[tonic::async_trait]
impl TraceService for TraceServer {
    async fn export(
        &self,
        request: tonic::Request<ExportTraceServiceRequest>,
    ) -> Result<tonic::Response<ExportTraceServiceResponse>, tonic::Status> {
        let start = std::time::Instant::now();
        let cfg = config::get_config();

        let metadata = request.metadata().clone();
        let msg = format!(
            "Please specify organization id with header key '{}' ",
            cfg.grpc.org_header_key
        );
        if !metadata.contains_key(&cfg.grpc.org_header_key) {
            return Err(Status::invalid_argument(msg));
        }

        let in_req = request.into_inner();
        let org_id = metadata.get(&cfg.grpc.org_header_key);
        if org_id.is_none() {
            return Err(Status::invalid_argument(msg));
        }

        let stream_name = metadata.get(&cfg.grpc.stream_header_key);
        let mut in_stream_name: Option<&str> = None;
        if let Some(stream_name) = stream_name {
            in_stream_name = Some(stream_name.to_str().unwrap());
        };

        let user_email = metadata
            .get("user_id")
            .and_then(|id| id.to_str().ok())
            .unwrap_or_else(|| {
                log::warn!("[gRPC Traces] user_id not found in metadata, using empty string");
                ""
            });

        let user = IngestUser::from_user_email(user_email);

        match handle_otlp_request(
            org_id.unwrap().to_str().unwrap(),
            in_req,
            OtlpRequestType::Grpc,
            in_stream_name,
            user,
        )
        .await
        {
            Ok(response) => {
                // metrics carry the ingest layer's real status
                let code = response.status().as_u16().to_string();
                let time = start.elapsed().as_secs_f64();
                metrics::GRPC_RESPONSE_TIME
                    .with_label_values(&["/otlp/v1/traces", &code, "", "", "", ""])
                    .observe(time);
                metrics::GRPC_INCOMING_REQUESTS
                    .with_label_values(&["/otlp/v1/traces", &code, "", "", "", ""])
                    .inc();

                export_response_from_http(response).await
            }
            Err(e) => {
                log::error!("handle_trace_request err {e}");
                Err(Status::internal(e.to_string()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::response::IntoResponse;
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTracePartialSuccess;

    use super::*;

    fn proto_response(
        status: HttpStatusCode,
        res: &ExportTraceServiceResponse,
    ) -> axum::response::Response {
        let mut body = Vec::with_capacity(res.encoded_len());
        res.encode(&mut body).unwrap();
        (status, body).into_response()
    }

    #[test]
    fn test_trace_server_default() {
        let _server = TraceServer;
    }

    /// A fully accepted export stays a clean gRPC reply.
    #[tokio::test]
    async fn test_export_success_propagates_no_partial() {
        let res = ExportTraceServiceResponse::default();
        let reply = export_response_from_http(proto_response(HttpStatusCode::OK, &res))
            .await
            .expect("a 200 is a gRPC success");
        assert!(reply.into_inner().partial_success.is_none());
    }

    /// The REAL rejected span count reaches the collector — answering
    /// `partial_success: None` for a partially rejected batch hid the loss.
    #[tokio::test]
    async fn test_export_success_propagates_the_real_partial_success() {
        let res = ExportTraceServiceResponse {
            partial_success: Some(ExportTracePartialSuccess {
                rejected_spans: 3,
                error_message:
                    "Some spans were rejected due to exceeding the allowed retention period"
                        .to_string(),
            }),
        };
        let reply = export_response_from_http(proto_response(HttpStatusCode::OK, &res))
            .await
            .expect("a 200 with partial success is still a gRPC success");
        let partial = reply
            .into_inner()
            .partial_success
            .expect("partial_success must be propagated, not discarded");
        assert_eq!(partial.rejected_spans, 3);
        assert!(partial.error_message.contains("rejected"));
    }

    /// A failed durable write must be a gRPC ERROR, never an OK export
    /// response: the collector's retry queue holds the only remaining copy.
    #[tokio::test]
    async fn test_export_write_failure_is_a_grpc_error() {
        // the write-failure path reports through the error header + JSON body
        let error = "error while writing trace data: wal write failed";
        let response = (
            HttpStatusCode::INTERNAL_SERVER_ERROR,
            [(ERROR_HEADER, error)],
            format!("{{\"code\":500,\"message\":\"{error}\"}}"),
        )
            .into_response();
        let status = export_response_from_http(response)
            .await
            .expect_err("a 500 must not be acked");
        assert_eq!(status.code(), tonic::Code::Internal);
        assert!(status.message().contains("wal write failed"), "{status}");

        // backpressure (no error header, JSON body): UNAVAILABLE so the
        // collector backs off and retries, with the body detail preserved
        let response = (
            HttpStatusCode::SERVICE_UNAVAILABLE,
            "{\"code\":503,\"message\":\"memtable is full\"}".to_string(),
        )
            .into_response();
        let status = export_response_from_http(response)
            .await
            .expect_err("a 503 must not be acked");
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert!(status.message().contains("memtable is full"), "{status}");
    }

    /// A schema-invalid batch is the caller's fault: INVALID_ARGUMENT, so
    /// the collector does not retry a deterministic failure forever.
    #[tokio::test]
    async fn test_export_bad_request_is_invalid_argument() {
        let response = (
            HttpStatusCode::BAD_REQUEST,
            [(
                ERROR_HEADER,
                "error while writing trace data: invalid schema",
            )],
            "{}".to_string(),
        )
            .into_response();
        let status = export_response_from_http(response)
            .await
            .expect_err("a 400 must not be acked");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    /// A non-2xx with neither header nor body detail still fails with the
    /// HTTP status named.
    #[tokio::test]
    async fn test_export_failure_without_detail_still_errors() {
        let response = HttpStatusCode::SERVICE_UNAVAILABLE.into_response();
        let status = export_response_from_http(response)
            .await
            .expect_err("a 503 must not be acked");
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert!(status.message().contains("503"), "{status}");
    }
}
