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
use std::io::{BufRead, BufReader};

use axum::body::Bytes;
use config::{get_config, meta::stream::StreamType, utils::json};
use hashbrown::HashMap;
use infra::errors::{Error, Result};
use serde::Deserialize;

use crate::{
    common::meta::ingestion::{
        HecResponse, HecStatus, IngestUser, IngestionRequest, IngestionValueType,
    },
    service::ingestion::check_ingestion_allowed,
};

#[derive(Deserialize, Clone)]
struct HecEntry {
    index: Option<String>,
    time: Option<i64>,
    fields: Option<json::Value>,
    event: json::Value,
}

/// Map an ingestion error to a HEC status.
///
/// A Splunk forwarder treats 4xx as PERMANENT and drops the batch, so every
/// retryable condition must be 5xx. `retryable` marks the call sites where
/// the rejection is a server-side condition (backpressure, non-ingester node,
/// org quota) rather than malformed client data.
fn hec_error_status(e: &Error, retryable: bool) -> HecStatus {
    let code = match e {
        // the forwarder should back off, not drop
        Error::TrialPeriodExpired => 429,
        Error::ResourceError(_) => 503,
        _ if retryable => 503,
        _ => 400,
    };
    HecStatus::Custom(e.to_string(), code)
}

pub async fn ingest(
    thread_id: usize,
    org_id: &str,
    body: Bytes,
    user_email: &str,
) -> Result<HecResponse> {
    // check system resource
    if let Err(e) = check_ingestion_allowed(org_id, StreamType::Logs, None).await {
        return Ok(hec_error_status(&e, true).into());
    }

    let cfg = get_config();

    let default = &cfg.common.default_hec_stream;

    let reader = BufReader::new(body.as_ref());
    let mut streams: HashMap<String, Vec<json::Value>> = HashMap::new();

    // in case of ndjson, each line will have a json
    // for non ndjson, there will only be one item.
    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let value: HecEntry = match json::from_slice(trimmed.as_bytes()) {
            Ok(v) => v,
            Err(e) => {
                log::info!("error in ingesting hec data '{trimmed}' : {e}");
                return Ok(HecStatus::InvalidFormat.into());
            }
        };
        let mut data = match &value.event {
            json::Value::String(s) => {
                serde_json::json!({"log":s.to_owned()})
            }
            json::Value::Object(_) => value.event.to_owned(),
            _ => return Ok(HecStatus::InvalidFormat.into()),
        };
        if let Some(s) = value.time {
            data["_timestamp"] = s.into();
        }
        if let Some(fields) = value.fields
            && let Some(o) = fields.as_object()
        {
            for (f, v) in o {
                data[f] = v.to_owned()
            }
        }

        let index = if let Some(idx) = value.index {
            idx
        } else if !default.is_empty() {
            default.clone()
        } else {
            log::error!("expected default hec stream to always be present, found to be empty");
            return Ok(HecStatus::InvalidIndex.into());
        };

        streams.entry(index).or_default().push(data);
    }

    // Every index is attempted; the worst status wins. A partial failure is
    // still a failure — the forwarder holds the only other copy of the batch.
    let mut failure: Option<HecResponse> = None;
    for (stream, entries) in streams {
        let in_req = IngestionRequest::JsonValues(IngestionValueType::Hec, entries);
        let status = match super::ingest::ingest(
            thread_id,
            org_id,
            &stream,
            in_req,
            IngestUser::from_user_email(user_email.to_string()),
            None,
            false,
        )
        .await
        {
            // a non-2xx means the records of this index were NOT stored
            Ok(res) if res.code > 299 => HecStatus::Custom(
                res.error
                    .unwrap_or_else(|| format!("failed to ingest into index '{stream}'")),
                res.code,
            ),
            Ok(_) => continue,
            Err(e) => {
                log::error!("[LOGS:HEC] index {org_id}/{stream}: ingestion error: {e}");
                hec_error_status(&e, false)
            }
        };
        let res: HecResponse = status.into();
        if failure.as_ref().is_none_or(|worst| res.code > worst.code) {
            failure = Some(res);
        }
    }
    if let Some(res) = failure {
        return Ok(res);
    }

    Ok(HecStatus::Success.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Splunk forwarders treat 4xx as PERMANENT and drop the batch, so
    /// backpressure from `check_ingestion_allowed` must be a 503 — it used to
    /// be reported as `InvalidIndex` (400) and the events were lost.
    #[test]
    fn test_backpressure_is_503_not_400() {
        let res: HecResponse =
            hec_error_status(&Error::ResourceError("memtable is full".to_string()), true).into();
        assert_eq!(res.code, 503);
        assert!(res.text.contains("memtable is full"));

        // the whole `check_ingestion_allowed` family is server-side: a
        // non-ingester node or a blocked org must not read as permanent
        let res: HecResponse =
            hec_error_status(&Error::IngestionError("not an ingester".to_string()), true).into();
        assert_eq!(res.code, 503);

        // an expired trial is a back-off, not a drop
        let res: HecResponse = hec_error_status(&Error::TrialPeriodExpired, true).into();
        assert_eq!(res.code, 429);

        // a client-side rejection stays a 400
        let res: HecResponse = hec_error_status(
            &Error::IngestionError("Stream name is empty".to_string()),
            false,
        )
        .into();
        assert_eq!(res.code, 400);

        // ... but backpressure is 503 even on the non-retryable call site
        let res: HecResponse =
            hec_error_status(&Error::ResourceError("disk is full".to_string()), false).into();
        assert_eq!(res.code, 503);
    }

    #[tokio::test]
    async fn test_ingest_invalid_json() {
        // Test with invalid JSON data
        let invalid_data = r#"{"invalid": json}"#;
        let body = Bytes::from(invalid_data);
        let thread_id = 1;
        let org_id = "test-org";
        let user_email = "test@example.com";

        let result = ingest(thread_id, org_id, body, user_email).await;

        match result {
            Ok(response) => {
                // Should return InvalidFormat status for malformed JSON
                assert!(matches!(response.code, 400));
            }
            Err(e) => {
                // If it fails with an error, that's also acceptable
                assert!(!e.to_string().is_empty());
            }
        }
    }
}
