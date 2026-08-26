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

//! Pre-body ingest admission middleware.
//!
//! Ingest handlers take `body: Bytes`, so the FULL (decompressed) request body
//! is buffered in memory before any handler code — and therefore before the
//! memory circuit breaker — runs. The breaker also samples process RSS only
//! once per second, so a burst of concurrent large batches can pass it on a
//! stale reading and then expand (decompress -> decode -> flatten -> arrow)
//! past the cgroup limit: the kernel OOM-kills while the breaker is still
//! returning 503s for NEW requests.
//!
//! This middleware runs BEFORE the request body is touched (it is installed
//! outside `RequestDecompressionLayer`, so it sees the wire `Content-Length`
//! and `Content-Encoding`). For ingest requests it:
//!
//! 1. rejects `Content-Length > ZO_PAYLOAD_LIMIT` with 413 immediately, without buffering the body
//!    (the body-limit layer would only reject after buffering up to the limit);
//! 2. projects the request's in-process transient (content-length x expansion factor, higher for
//!    compressed bodies) and reserves it against the memory circuit breaker envelope for the whole
//!    lifetime of the request; over-envelope requests get 503 + `Retry-After` (retryable, same
//!    semantics the breaker has always had) with the body never read;
//! 3. releases the reservation when the response completes.
//!
//! Reservations are also added to the breaker's own memory reading (see
//! `ingester::check_memory_circuit_breaker`), closing the check-then-allocate
//! race for every path that consults the breaker — including gRPC OTLP.
//!
//! Ingest-route detection reuses the authoritative
//! [`ingestion_routes`](crate::common::meta::ingestion_routes) table (the same
//! one the auth layer classifies against), so this gate can never trip a
//! lookalike route such as `POST /{org}/_search`.

use axum::{
    Json,
    extract::Request,
    http::{Method, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use config::get_config;
use ingester::admission::{self, REJECT_MEMORY, REJECT_OVERSIZE, note_rejection};

use super::decompression::X_ORIGINAL_ENCODING;
use crate::common::meta::{http::HttpResponse as MetaHttpResponse, ingestion_routes};

/// What to do with an ingest request, decided purely from headers and config.
#[derive(Debug, PartialEq, Eq)]
enum AdmissionAction {
    /// Not gated (admission disabled, or breaker disabled and size is fine).
    Pass,
    /// Can never be accepted: too large for the payload limit or the whole
    /// memory envelope. Non-retryable (413).
    RejectTooLarge,
    /// Reserve this many projected bytes against the envelope before reading
    /// the body.
    Reserve(usize),
}

fn classify(
    admission_enabled: bool,
    content_length: Option<u64>,
    compressed: bool,
    payload_limit: usize,
    expansion_factor: usize,
    compressed_factor: usize,
    envelope: Option<usize>,
) -> AdmissionAction {
    if !admission_enabled {
        return AdmissionAction::Pass;
    }
    // hard per-request cap on the wire size: the body-limit layer would
    // return the same 413, but only after buffering payload_limit bytes
    if let Some(cl) = content_length
        && cl as usize > payload_limit
    {
        return AdmissionAction::RejectTooLarge;
    }
    let Some(envelope) = envelope else {
        // memory circuit breaker disabled: no envelope to meter against
        return AdmissionAction::Pass;
    };
    let factor = if compressed {
        compressed_factor
    } else {
        expansion_factor
    };
    let projected = match content_length {
        Some(cl) => (cl as usize).saturating_mul(factor),
        // no content-length (chunked): be strict, reserve the maximum the
        // body-limit layer would let through
        None => payload_limit,
    };
    if projected > envelope {
        // this single request can never fit in the envelope; retrying at the
        // same size will never help
        return AdmissionAction::RejectTooLarge;
    }
    AdmissionAction::Reserve(projected)
}

fn reject_too_large(path: &str, content_length: Option<u64>) -> Response {
    note_rejection(REJECT_OVERSIZE);
    log::debug!(
        "[INGEST:ADMISSION] 413 for {path}: content-length {content_length:?} exceeds payload limit or memory envelope"
    );
    (
        StatusCode::PAYLOAD_TOO_LARGE,
        Json(MetaHttpResponse::error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "request body too large for the configured payload limit or memory envelope",
        )),
    )
        .into_response()
}

fn reject_memory(path: &str, projected: usize) -> Response {
    note_rejection(REJECT_MEMORY);
    log::debug!(
        "[INGEST:ADMISSION] 503 for {path}: projected {projected} bytes would exceed the memory envelope"
    );
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(header::RETRY_AFTER, "1")],
        Json(MetaHttpResponse::error(
            StatusCode::SERVICE_UNAVAILABLE,
            "MemoryCircuitBreakerError",
        )),
    )
        .into_response()
}

fn is_compressed(request: &Request) -> bool {
    let non_identity = |v: &header::HeaderValue| {
        v.to_str()
            .map(|s| !s.trim().is_empty() && !s.trim().eq_ignore_ascii_case("identity"))
            .unwrap_or(true)
    };
    request
        .headers()
        .get(header::CONTENT_ENCODING)
        .map(non_identity)
        .unwrap_or(false)
        // snappy is stripped to a marker header by the encoding preprocessor
        || request.headers().contains_key(X_ORIGINAL_ENCODING)
}

fn content_length_of(request: &Request) -> Option<u64> {
    request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok())
}

async fn admit(request: Request, next: Next) -> Response {
    let cfg = get_config();
    let content_length = content_length_of(&request);
    let action = classify(
        cfg.common.ingest_admission_enabled,
        content_length,
        is_compressed(&request),
        cfg.limit.req_payload_limit,
        cfg.common.ingest_admission_expansion_factor,
        cfg.common.ingest_admission_compressed_factor,
        admission::memory_envelope(),
    );
    match action {
        AdmissionAction::Pass => next.run(request).await,
        AdmissionAction::RejectTooLarge => reject_too_large(request.uri().path(), content_length),
        AdmissionAction::Reserve(projected) => {
            match admission::try_reserve(projected) {
                Err(_) => reject_memory(request.uri().path(), projected),
                Ok(reservation) => {
                    // hold the reservation across the whole request lifetime:
                    // body buffering, decode, flatten, arrow building
                    let response = next.run(request).await;
                    drop(reservation);
                    response
                }
            }
        }
    }
}

/// Admission for the `/api` service routes: applies only to POSTs that the
/// authoritative ingestion-route table classifies as data-ingestion writes;
/// everything else passes straight through.
///
/// Middleware layered on the nested `/api` router still sees the full request
/// path (`[base_uri/]api/{org}/...` — prefix stripping happens at routing),
/// so the org-relative view is taken after the `api/` segment, exactly like
/// `audit_middleware` does.
pub async fn ingest_admission_middleware(request: Request, next: Next) -> Response {
    if request.method() != Method::POST {
        return next.run(request).await;
    }
    let org_path = {
        let path = request.uri().path();
        path.split_once("api/").map(|(_, rest)| rest).unwrap_or("")
    };
    if !ingestion_routes::is_ingestion_write(request.method(), org_path) {
        return next.run(request).await;
    }
    admit(request, next).await
}

/// Admission for route groups where every POST is an ingest endpoint
/// (the AWS / GCP / RUM ingest routers).
pub async fn ingest_admission_middleware_all(request: Request, next: Next) -> Response {
    if request.method() != Method::POST && request.method() != Method::PUT {
        return next.run(request).await;
    }
    admit(request, next).await
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, Ordering},
    };

    use axum::{Router, body::Body, middleware, routing::post};
    use futures::stream;
    use tower::ServiceExt;

    use super::*;

    /// Tests that touch process-global state (the reservation ledger, the
    /// NODE_MEMORY_USAGE gauge, or the breaker env config) must not overlap.
    static GLOBAL_STATE_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn test_classify_disabled_passes() {
        assert_eq!(
            classify(false, Some(u64::MAX), false, 100, 6, 30, Some(1000)),
            AdmissionAction::Pass
        );
    }

    #[test]
    fn test_classify_oversize_content_length() {
        // over the payload limit: rejected even with no envelope (breaker off)
        assert_eq!(
            classify(true, Some(101), false, 100, 6, 30, None),
            AdmissionAction::RejectTooLarge
        );
        assert_eq!(
            classify(true, Some(101), false, 100, 6, 30, Some(1_000_000)),
            AdmissionAction::RejectTooLarge
        );
        // exactly at the limit is allowed
        assert_eq!(
            classify(true, Some(100), false, 100, 6, 30, None),
            AdmissionAction::Pass
        );
    }

    #[test]
    fn test_classify_projection_factors() {
        let envelope = Some(1_000_000);
        // raw body: cl * expansion factor
        assert_eq!(
            classify(true, Some(1000), false, 10_000, 6, 30, envelope),
            AdmissionAction::Reserve(6000)
        );
        // compressed body: cl * compressed factor
        assert_eq!(
            classify(true, Some(1000), true, 10_000, 6, 30, envelope),
            AdmissionAction::Reserve(30_000)
        );
        // no content-length (chunked): strict, reserve the payload limit
        assert_eq!(
            classify(true, None, false, 10_000, 6, 30, envelope),
            AdmissionAction::Reserve(10_000)
        );
    }

    #[test]
    fn test_classify_never_fits_envelope() {
        // projected alone exceeds the envelope: non-retryable
        assert_eq!(
            classify(true, Some(1000), true, 10_000, 6, 30, Some(29_999)),
            AdmissionAction::RejectTooLarge
        );
        // fits exactly
        assert_eq!(
            classify(true, Some(1000), true, 10_000, 6, 30, Some(30_000)),
            AdmissionAction::Reserve(30_000)
        );
    }

    #[test]
    fn test_classify_saturating_projection() {
        assert_eq!(
            classify(
                true,
                Some((usize::MAX / 2) as u64),
                true,
                usize::MAX,
                6,
                30,
                Some(1000)
            ),
            AdmissionAction::RejectTooLarge
        );
    }

    fn tracking_body(polled: Arc<AtomicBool>) -> Body {
        Body::from_stream(stream::once(async move {
            polled.store(true, Ordering::SeqCst);
            Ok::<_, std::convert::Infallible>(axum::body::Bytes::from_static(b"x"))
        }))
    }

    /// PIN: an oversized batch is rejected before ANY body byte is read —
    /// the process never buffers it, so RSS cannot spike from it — and the
    /// handler is never invoked.
    #[tokio::test]
    async fn test_oversize_rejected_without_reading_body_or_handler() {
        let handler_hit = Arc::new(AtomicBool::new(false));
        let body_polled = Arc::new(AtomicBool::new(false));
        let hit = handler_hit.clone();
        let app = Router::new()
            .route(
                "/api/{org_id}/_bulk",
                post(move || {
                    hit.store(true, Ordering::SeqCst);
                    async { "ok" }
                }),
            )
            .layer(middleware::from_fn(ingest_admission_middleware));

        let payload_limit = get_config().limit.req_payload_limit as u64;
        let request = axum::http::Request::builder()
            .uri("/api/default/_bulk")
            .method("POST")
            .header(header::CONTENT_LENGTH, (payload_limit + 1).to_string())
            .body(tracking_body(body_polled.clone()))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert!(!handler_hit.load(Ordering::SeqCst), "handler must not run");
        assert!(
            !body_polled.load(Ordering::SeqCst),
            "body must never be read"
        );
    }

    /// PIN: normal batches are unaffected and any reservation is fully
    /// released once the response completes.
    #[tokio::test]
    async fn test_normal_ingest_passthrough_releases_reservation() {
        let _guard = GLOBAL_STATE_LOCK
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let handler_hit = Arc::new(AtomicBool::new(false));
        let hit = handler_hit.clone();
        let app = Router::new()
            .route(
                "/api/{org_id}/_bulk",
                post(move |body: axum::body::Bytes| {
                    hit.store(true, Ordering::SeqCst);
                    async move { format!("got {} bytes", body.len()) }
                }),
            )
            .layer(middleware::from_fn(ingest_admission_middleware));

        let base_reserved = ingester::admission::reserved_bytes();
        let request = axum::http::Request::builder()
            .uri("/api/default/_bulk")
            .method("POST")
            .header(header::CONTENT_LENGTH, "4")
            .body(Body::from("test"))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(handler_hit.load(Ordering::SeqCst), "handler must run");
        assert_eq!(
            ingester::admission::reserved_bytes(),
            base_reserved,
            "reservation must be released after the response"
        );
    }

    /// PIN: non-ingest routes untouched — lookalike POST routes (search
    /// endpoints and other POSTs) are never gated, whatever their size claims.
    #[tokio::test]
    async fn test_non_ingest_post_passthrough() {
        for path in [
            "/api/default/_search",
            "/api/default/_search_multi",
            "/api/default/mystream/_around",
            "/api/default/functions/test",
            "/api/default/prometheus/api/v1/query",
        ] {
            let handler_hit = Arc::new(AtomicBool::new(false));
            let hit = handler_hit.clone();
            let app = Router::new()
                .route(
                    // one catch-all route shape per test iteration; the
                    // middleware decision is path-based, not route-based
                    "/api/{org_id}/{*rest}",
                    post(move || {
                        hit.store(true, Ordering::SeqCst);
                        async { "ok" }
                    }),
                )
                .layer(middleware::from_fn(ingest_admission_middleware));

            let request = axum::http::Request::builder()
                .uri(path)
                .method("POST")
                .header(header::CONTENT_LENGTH, u64::MAX.to_string())
                .body(Body::from(""))
                .unwrap();

            let response = app.oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK, "path {path} was gated");
            assert!(handler_hit.load(Ordering::SeqCst), "handler for {path}");
        }
    }

    /// The unconditional variant gates POSTs on any path (used for the
    /// AWS/GCP/RUM ingest route groups) but leaves GETs alone.
    #[tokio::test]
    async fn test_unconditional_variant_gates_posts_only() {
        let app = Router::new()
            .route(
                "/aws/{org_id}/{stream}/_kinesis_firehose",
                post(|| async { "ok" }),
            )
            .layer(middleware::from_fn(ingest_admission_middleware_all));

        let payload_limit = get_config().limit.req_payload_limit as u64;
        let request = axum::http::Request::builder()
            .uri("/aws/default/s/_kinesis_firehose")
            .method("POST")
            .header(header::CONTENT_LENGTH, (payload_limit + 1).to_string())
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    /// PIN (through the real middleware): with the memory breaker enabled,
    /// reservations already filling the envelope make the next ingest request
    /// 503 with Retry-After — body never polled, handler never invoked —
    /// while a lookalike non-ingest POST still passes; once the in-flight
    /// reservation releases, the same ingest request is admitted again and
    /// its own reservation is fully released after the response.
    ///
    /// All breaker env mutation lives in this ONE test (same discipline as
    /// the segment-mode seam test) and runs under GLOBAL_STATE_LOCK.
    #[tokio::test]
    async fn test_envelope_full_rejects_concurrent_and_releases() {
        let _guard = GLOBAL_STATE_LOCK
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        unsafe { std::env::set_var("ZO_MEMORY_CIRCUIT_BREAKER_ENABLED", "true") };
        config::refresh_config().unwrap();

        let run = async {
            let envelope =
                ingester::admission::memory_envelope().expect("breaker enabled -> envelope");
            // pure-reservation arithmetic: pretend RSS is zero (no stats job
            // runs in unit tests, so nothing overwrites the gauge)
            config::metrics::NODE_MEMORY_USAGE
                .with_label_values::<&str>(&[])
                .set(0);

            let build_app = |hit: Arc<AtomicBool>| {
                Router::new()
                    .route(
                        "/api/{org_id}/_bulk",
                        post(move |_body: axum::body::Bytes| {
                            hit.store(true, Ordering::SeqCst);
                            async { "ok" }
                        }),
                    )
                    .route("/api/{org_id}/_search", post(|| async { "ok" }))
                    .layer(middleware::from_fn(ingest_admission_middleware))
            };
            let ingest_req = |polled: Arc<AtomicBool>| {
                axum::http::Request::builder()
                    .uri("/api/default/_bulk")
                    .method("POST")
                    .header(header::CONTENT_LENGTH, "1024")
                    .header(header::CONTENT_ENCODING, "gzip")
                    .body(tracking_body(polled))
                    .unwrap()
            };

            // fill the whole envelope, as N concurrent in-flight requests would
            let hold = ingester::admission::try_reserve(envelope).expect("fill envelope");

            // an ingest request that would fit on its own is now rejected
            // before any body byte is read
            let handler_hit = Arc::new(AtomicBool::new(false));
            let body_polled = Arc::new(AtomicBool::new(false));
            let resp = build_app(handler_hit.clone())
                .oneshot(ingest_req(body_polled.clone()))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(
                resp.headers()
                    .get(header::RETRY_AFTER)
                    .and_then(|v| v.to_str().ok()),
                Some("1"),
                "503 must carry Retry-After"
            );
            assert!(!handler_hit.load(Ordering::SeqCst), "handler must not run");
            assert!(!body_polled.load(Ordering::SeqCst), "body must not be read");

            // a lookalike non-ingest POST is untouched even with the
            // envelope full
            let resp = build_app(Arc::new(AtomicBool::new(false)))
                .oneshot(
                    axum::http::Request::builder()
                        .uri("/api/default/_search")
                        .method("POST")
                        .header(header::CONTENT_LENGTH, "1024")
                        .body(Body::from(""))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK, "non-ingest POST gated");

            // release the in-flight reservations: the same ingest request is
            // admitted, its body is read, and its reservation fully releases
            drop(hold);
            let base = ingester::admission::reserved_bytes();
            let handler_hit = Arc::new(AtomicBool::new(false));
            let body_polled = Arc::new(AtomicBool::new(false));
            let resp = build_app(handler_hit.clone())
                .oneshot(ingest_req(body_polled.clone()))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            assert!(handler_hit.load(Ordering::SeqCst), "handler must run");
            assert!(body_polled.load(Ordering::SeqCst), "body must be read");
            assert_eq!(
                ingester::admission::reserved_bytes(),
                base,
                "reservation must be released after the response"
            );
        };
        let result = tokio::spawn(run).await;

        // restore BEFORE asserting so a failed assert cannot leak the flag
        // into other tests
        unsafe { std::env::remove_var("ZO_MEMORY_CIRCUIT_BREAKER_ENABLED") };
        config::refresh_config().unwrap();
        result.unwrap();
    }

    /// 503 rejections carry Retry-After so senders back off instead of
    /// hammering; response is a proper JSON error, not a dropped connection.
    #[tokio::test]
    async fn test_memory_rejection_shape() {
        let resp = reject_memory("/api/default/_bulk", 123);
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            resp.headers()
                .get(header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok()),
            Some("1")
        );
    }

    #[test]
    fn test_compressed_detection() {
        let req = |enc: Option<&str>, marker: bool| {
            let mut b = axum::http::Request::builder().uri("/x").method("POST");
            if let Some(enc) = enc {
                b = b.header(header::CONTENT_ENCODING, enc);
            }
            if marker {
                b = b.header(X_ORIGINAL_ENCODING, "snappy");
            }
            b.body(Body::empty()).unwrap()
        };
        assert!(!is_compressed(&req(None, false)));
        assert!(!is_compressed(&req(Some("identity"), false)));
        assert!(is_compressed(&req(Some("gzip"), false)));
        assert!(is_compressed(&req(Some("zstd"), false)));
        assert!(is_compressed(&req(None, true)));
    }
}
