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
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse, logs_service_server::LogsService,
};
use prost::Message;
use tonic::{Response, Status};

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
/// (500/503 when records were lost) and the REAL `partial_success`
/// (`rejected_log_records` + error message). Discarding it and answering
/// `partial_success: None` acked a lost batch as a fully accepted export,
/// and the collector deleted its only copy.
async fn export_response_from_http(
    response: axum::response::Response,
) -> Result<tonic::Response<ExportLogsServiceResponse>, Status> {
    let http_status = response.status();
    let body = axum::body::to_bytes(response.into_body(), MAX_RESPONSE_BODY)
        .await
        .map_err(|e| Status::internal(format!("failed to read log ingest response: {e}")))?;
    // the gRPC request type always produces a protobuf
    // ExportLogsServiceResponse body, on every status code
    let res = ExportLogsServiceResponse::decode(body)
        .map_err(|e| Status::internal(format!("failed to decode log ingest response: {e}")))?;

    if http_status.is_success() {
        // propagate the real partial_success: the collector retries exactly
        // the rejected records
        return Ok(Response::new(res));
    }
    let message = res
        .partial_success
        .as_ref()
        .filter(|p| !p.error_message.is_empty())
        .map(|p| {
            format!(
                "{} log record(s) rejected: {}",
                p.rejected_log_records, p.error_message
            )
        })
        .unwrap_or_else(|| format!("log ingestion failed with HTTP status {http_status}"));
    Err(grpc_status(http_status, message))
}

#[derive(Default)]
pub struct LogsServer;

#[tonic::async_trait]
impl LogsService for LogsServer {
    async fn export(
        &self,
        request: tonic::Request<ExportLogsServiceRequest>,
    ) -> Result<tonic::Response<ExportLogsServiceResponse>, tonic::Status> {
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

        let user_id = metadata.get("user_id");
        let mut user_email: &str = "";
        if let Some(user_id) = user_id {
            user_email = user_id.to_str().unwrap();
        };

        match crate::service::logs::otlp::handle_request(
            0,
            org_id.unwrap().to_str().unwrap(),
            in_req,
            in_stream_name,
            user_email,
            OtlpRequestType::Grpc,
        )
        .await
        {
            Ok(response) => {
                // metrics carry the ingest layer's real status
                let code = response.status().as_u16().to_string();
                let time = start.elapsed().as_secs_f64();
                metrics::GRPC_RESPONSE_TIME
                    .with_label_values(&["/otlp/v1/logs", &code, "", "", "", ""])
                    .observe(time);
                metrics::GRPC_INCOMING_REQUESTS
                    .with_label_values(&["/otlp/v1/logs", &code, "", "", "", ""])
                    .inc();

                export_response_from_http(response).await
            }
            // backpressure stays retryable end to end
            Err(e) if matches!(e, infra::errors::Error::ResourceError(_)) => {
                Err(Status::unavailable(e.to_string()))
            }
            Err(e) if matches!(e, infra::errors::Error::TrialPeriodExpired) => {
                Err(Status::resource_exhausted(e.to_string()))
            }
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::response::IntoResponse;
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsPartialSuccess;

    use super::*;

    fn http_response(
        status: HttpStatusCode,
        res: &ExportLogsServiceResponse,
    ) -> axum::response::Response {
        let mut body = Vec::with_capacity(res.encoded_len());
        res.encode(&mut body).unwrap();
        (status, body).into_response()
    }

    #[test]
    fn test_logs_server_default() {
        let _server = LogsServer;
    }

    /// A fully accepted export stays a clean gRPC reply.
    #[tokio::test]
    async fn test_export_success_propagates_no_partial() {
        let res = ExportLogsServiceResponse {
            partial_success: None,
        };
        let reply = export_response_from_http(http_response(HttpStatusCode::OK, &res))
            .await
            .expect("a 200 is a gRPC success");
        assert!(reply.into_inner().partial_success.is_none());
    }

    /// The REAL rejected count reaches the collector — answering
    /// `partial_success: None` for a partially rejected batch hid the loss.
    #[tokio::test]
    async fn test_export_success_propagates_the_real_partial_success() {
        let res = ExportLogsServiceResponse {
            partial_success: Some(ExportLogsPartialSuccess {
                rejected_log_records: 2,
                error_message: "Too old data".to_string(),
            }),
        };
        let reply = export_response_from_http(http_response(HttpStatusCode::OK, &res))
            .await
            .expect("a 200 with partial success is still a gRPC success");
        let partial = reply
            .into_inner()
            .partial_success
            .expect("partial_success must be propagated, not discarded");
        assert_eq!(partial.rejected_log_records, 2);
        assert!(partial.error_message.contains("Too old data"));
    }

    /// A failed durable write must be a gRPC ERROR, never an OK export
    /// response: the collector's retry queue holds the only remaining copy.
    #[tokio::test]
    async fn test_export_write_failure_is_a_grpc_error() {
        let res = ExportLogsServiceResponse {
            partial_success: Some(ExportLogsPartialSuccess {
                rejected_log_records: 5,
                error_message: "wal write failed: no space left".to_string(),
            }),
        };

        // backpressure: UNAVAILABLE so the collector backs off and retries
        let status =
            export_response_from_http(http_response(HttpStatusCode::SERVICE_UNAVAILABLE, &res))
                .await
                .expect_err("a 503 must not be acked");
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert!(status.message().contains("no space left"), "{status}");
        assert!(status.message().contains('5'), "{status}");

        // any other write failure: INTERNAL (retryable per the OTLP spec)
        let status =
            export_response_from_http(http_response(HttpStatusCode::INTERNAL_SERVER_ERROR, &res))
                .await
                .expect_err("a 500 must not be acked");
        assert_eq!(status.code(), tonic::Code::Internal);
    }

    /// A non-2xx whose body carries no error detail still fails with the
    /// HTTP status named.
    #[tokio::test]
    async fn test_export_failure_without_detail_still_errors() {
        let res = ExportLogsServiceResponse {
            partial_success: None,
        };
        let status =
            export_response_from_http(http_response(HttpStatusCode::INTERNAL_SERVER_ERROR, &res))
                .await
                .expect_err("a 500 must not be acked");
        assert_eq!(status.code(), tonic::Code::Internal);
        assert!(status.message().contains("500"), "{status}");
    }

    /// An undecodable body is an internal fault, never a fabricated
    /// full-success ack.
    #[tokio::test]
    async fn test_export_undecodable_body_is_not_an_ack() {
        let garbage = (HttpStatusCode::OK, vec![0xffu8, 0xff, 0xff, 0x01]).into_response();
        let status = export_response_from_http(garbage)
            .await
            .expect_err("garbage must not decode into an ack");
        assert_eq!(status.code(), tonic::Code::Internal);
    }
}
