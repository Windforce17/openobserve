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
    collections::HashMap,
    sync::{Arc, atomic::Ordering},
};

use anyhow::Result;
use config::{
    cluster::LOCAL_NODE_ID,
    get_config,
    ider::SnowflakeIdGenerator,
    meta::{promql::METADATA_LABEL, stream::StreamType},
    metrics,
    utils::{
        schema::{infer_json_schema_from_map, infer_json_schema_from_map_first_seen, schema_eq},
        schema_ext::SchemaExt,
        time::now_micros,
    },
};
use datafusion::arrow::datatypes::{Field, Schema};
use infra::schema::{
    CanonicalType, STREAM_RECORD_ID_GENERATOR, STREAM_SCHEMAS_LATEST, STREAM_SETTINGS, SchemaCache,
    unwrap_stream_settings,
};
use serde_json::{Map, Value};

use super::logs::bulk::SCHEMA_CONFORMANCE_FAILED;
use crate::{
    common::meta::{authz::Authz, ingestion::StreamSchemaChk, stream::SchemaEvolution},
    service::db,
};

pub(crate) fn get_upto_discard_error() -> anyhow::Error {
    anyhow::anyhow!(
        "Too old data, only last {} hours data can be ingested. Data discarded. You can adjust ingestion max time by setting the environment variable ZO_INGEST_ALLOWED_UPTO=<max_hours>",
        get_config().limit.ingest_allowed_upto
    )
}

pub(crate) fn get_future_discard_error() -> anyhow::Error {
    anyhow::anyhow!(
        "Too far data, only future {} hours data can be ingested. Data discarded. You can adjust ingestion max time by setting the environment variable ZO_INGEST_ALLOWED_IN_FUTURE=<max_hours>",
        get_config().limit.ingest_allowed_in_future
    )
}

pub(crate) fn get_request_columns_limit_error(
    stream_name: &str,
    num_fields: usize,
) -> anyhow::Error {
    anyhow::anyhow!(
        "Got {num_fields} columns for stream {stream_name}, only {} columns accept. Data discarded. You can adjust ingestion columns limit by setting the environment variable ZO_COLS_PER_RECORD_LIMIT=<max_columns>",
        get_config().limit.req_cols_per_record_limit
    )
}

pub async fn check_for_schema(
    org_id: &str,
    stream_name: &str,
    stream_type: StreamType,
    stream_schema_map: &mut HashMap<String, SchemaCache>,
    record_vals: Vec<&Map<String, Value>>,
    record_ts: i64,
    is_derived: bool,
) -> Result<(SchemaEvolution, Option<Schema>)> {
    if !stream_schema_map.contains_key(stream_name) {
        let schema = infra::schema::get_cache(org_id, stream_name, stream_type).await?;
        stream_schema_map.insert(stream_name.to_string(), schema);
    }
    let cfg = get_config();
    let schema = stream_schema_map.get(stream_name).unwrap();

    // get infer schema
    let value_iter = record_vals.into_iter();
    let inferred_schema = if cfg.common.ingest_canonical_schema {
        infer_json_schema_from_map_first_seen(stream_name, stream_type, value_iter)?
    } else {
        infer_json_schema_from_map(stream_name, stream_type, value_iter)?
    };

    // A cached registry type is the rollout baseline. Reconcile only the
    // request-width schema (never the full stream schema) before the fast
    // comparison. `merge_pinned` repeats this rule inside the watched DB CAS,
    // closing the concurrent-new-field race when this cache is stale.
    let inferred_schema = if cfg.common.ingest_canonical_schema && !schema.is_empty() {
        let fields = inferred_schema
            .fields()
            .iter()
            .map(|candidate| {
                schema
                    .field_with_name(candidate.name())
                    .cloned()
                    .unwrap_or_else(|| candidate.clone())
            })
            .collect::<Vec<_>>();
        Schema::new(fields)
    } else {
        inferred_schema
    };

    // fast path
    if schema_eq(schema.schema(), &inferred_schema) {
        return Ok((
            SchemaEvolution {
                is_schema_changed: false,
                types_delta: None,
            },
            None,
        ));
    }

    if inferred_schema.fields.len() > cfg.limit.req_cols_per_record_limit {
        metrics::INGEST_ERRORS
            .with_label_values(&[
                org_id,
                stream_type.as_str(),
                stream_name,
                SCHEMA_CONFORMANCE_FAILED,
            ])
            .inc();
        return Err(get_request_columns_limit_error(
            &format!("{org_id}/{stream_type}/{stream_name}"),
            inferred_schema.fields.len(),
        ));
    }

    let mut need_insert_new_latest = false;
    let is_new = schema.schema().fields().is_empty();
    if !is_new {
        let (is_schema_changed, field_datatype_delta) =
            get_schema_changes(schema, &inferred_schema);
        if !is_schema_changed {
            return Ok((
                SchemaEvolution {
                    is_schema_changed: false,
                    types_delta: Some(field_datatype_delta),
                },
                Some(inferred_schema),
            ));
        }
        if !field_datatype_delta.is_empty() {
            // check if the min_ts < current_version_created_at
            let schema_metadata = schema.schema().metadata();
            if let Some(start_dt) = schema_metadata.get("start_dt") {
                let created_at = start_dt.parse().unwrap_or_default();
                if record_ts <= created_at {
                    need_insert_new_latest = true;
                }
            }
        }
    }

    // slow path
    let ret = handle_diff_schema(
        org_id,
        stream_name,
        stream_type,
        is_new,
        &inferred_schema,
        record_ts,
        stream_schema_map,
        is_derived,
    )
    .await?
    .unwrap_or(SchemaEvolution {
        is_schema_changed: false,
        types_delta: None,
    });

    // if need_insert_new_latest, create a new version with start_dt = now
    if need_insert_new_latest {
        _ = handle_diff_schema(
            org_id,
            stream_name,
            stream_type,
            is_new,
            &inferred_schema,
            now_micros(),
            stream_schema_map,
            is_derived,
        )
        .await?;
    }

    Ok((ret, Some(inferred_schema)))
}

pub async fn get_merged_schema(
    org_id: &str,
    stream_name: &str,
    stream_type: StreamType,
    inferred_schema: &Schema,
) -> Option<(Vec<Field>, Schema)> {
    let mut db_schema = infra::schema::get_from_db(org_id, stream_name, stream_type)
        .await
        .unwrap();

    let (is_schema_changed, field_datatype_delta, merged_fields) =
        infra::schema::get_merge_schema_changes(&db_schema, inferred_schema);

    if !is_schema_changed {
        return None;
    }

    let metadata = std::mem::take(&mut db_schema.metadata);
    Some((
        field_datatype_delta,
        Schema::new(merged_fields).with_metadata(metadata),
    ))
}

// handle_diff_schema is a slow path, it acquires a lock to update schema
// steps:
// 1. get schema from db, if schema is empty, set schema and return
// 2. get schema from db for update,
// 3. if db_schema is identical to inferred_schema, return (means another thread has updated schema)
// 4. if db_schema is not identical to inferred_schema, merge schema and update db
#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_diff_schema(
    org_id: &str,
    stream_name: &str,
    stream_type: StreamType,
    is_new: bool,
    inferred_schema: &Schema,
    record_ts: i64,
    stream_schema_map: &mut HashMap<String, SchemaCache>,
    is_derived: bool,
) -> Result<Option<SchemaEvolution>> {
    let start = std::time::Instant::now();
    let cfg = get_config();

    log::debug!(
        "handle_diff_schema start for [{org_id}/{stream_type}/{stream_name}] start_dt: {record_ts}"
    );

    // acquire a local_lock to ensure only one thread can update schema
    let cache_key = format!("{org_id}/{stream_type}/{stream_name}");
    let local_lock = infra::local_lock::lock(&cache_key).await?;
    let _guard = local_lock.lock().await;

    // check if the schema has been updated by another thread
    let read_cache = STREAM_SCHEMAS_LATEST.read().await;
    if let Some(updated_schema) = read_cache.get(&cache_key)
        && let (false, _) = get_schema_changes(updated_schema, inferred_schema)
    {
        return Ok(None);
    }
    drop(read_cache);

    // v2 all-present-columns: no stream type is born with injected settings
    // anymore (the trace column-store seeding died with
    // `column_store_fields` — every present field is a native column).

    // first update thread cache
    if is_new {
        let mut metadata = HashMap::with_capacity(2);
        metadata.insert("created_at".to_string(), record_ts.to_string());
        if is_derived {
            metadata.insert("is_derived".to_string(), "true".to_string());
        }
        stream_schema_map.insert(
            stream_name.to_string(),
            SchemaCache::new(inferred_schema.clone().with_metadata(metadata)),
        );
    }

    let mut retries = 0;
    let mut err: Option<anyhow::Error> = None;
    let mut ret: Option<_> = None;
    // retry x times for update schema
    while retries < cfg.limit.meta_transaction_retries {
        let schema_for_merge = if is_derived {
            let mut metadata = HashMap::with_capacity(1);
            metadata.insert("is_derived".to_string(), "true".to_string());
            &inferred_schema.clone().with_metadata(metadata)
        } else {
            inferred_schema
        };
        let merge_result = if cfg.common.ingest_canonical_schema {
            db::schema::merge_pinned(
                org_id,
                stream_name,
                stream_type,
                schema_for_merge,
                Some(record_ts),
            )
            .await
        } else {
            db::schema::merge(
                org_id,
                stream_name,
                stream_type,
                schema_for_merge,
                Some(record_ts),
            )
            .await
        };
        match merge_result {
            Err(e) => {
                log::error!(
                    "handle_diff_schema [{org_id}/{stream_type}/{stream_name}] with hash {}, start_dt {record_ts}, error: {e}, retrying...{retries}",
                    inferred_schema.hash_key(),
                );
                err = Some(e);
                retries += 1;
                continue;
            }
            Ok(v) => {
                ret = v;
                err = None;
                break;
            }
        };
    }
    if let Some(e) = err {
        log::error!(
            "handle_diff_schema [{org_id}/{stream_type}/{stream_name}] with hash {}, start_dt {record_ts}, abort after retry {retries} times, error: {e}",
            inferred_schema.hash_key(),
        );
        return Err(e);
    }
    let Some((final_schema, field_datatype_delta)) = ret else {
        return Ok(None);
    };

    if is_new {
        crate::common::utils::auth::set_ownership(
            org_id,
            stream_type.as_str(),
            Authz::new(stream_name),
        )
        .await;
    }

    let stream_setting = unwrap_stream_settings(&final_schema).unwrap_or_default();

    // update node cache
    let final_schema = SchemaCache::new(final_schema);
    let mut w = STREAM_SCHEMAS_LATEST.write().await;
    w.insert(cache_key.clone(), final_schema.clone());
    drop(w);
    if stream_setting.store_original_data
        && let dashmap::Entry::Vacant(entry) = STREAM_RECORD_ID_GENERATOR.entry(cache_key.clone())
    {
        entry.insert(SnowflakeIdGenerator::new(
            LOCAL_NODE_ID.load(Ordering::Relaxed),
        ));
    }
    let mut w = STREAM_SETTINGS.write().await;
    w.insert(cache_key.clone(), stream_setting);
    infra::schema::set_stream_settings_atomic(w.clone());
    drop(w);

    // update thread cache
    stream_schema_map.insert(stream_name.to_string(), final_schema);

    log::debug!(
        "handle_diff_schema end for [{org_id}/{stream_type}/{stream_name}] start_dt: {record_ts}, elapsed: {} ms",
        start.elapsed().as_millis()
    );

    Ok(Some(SchemaEvolution {
        is_schema_changed: true,
        types_delta: Some(field_datatype_delta),
    }))
}

#[derive(Debug)]
pub struct CanonicalizationSummary {
    pub converted: usize,
    pub nulled: usize,
    pub first_failure: Option<CanonicalizationFailure>,
    metric_counts: [u64; CANONICAL_METRIC_SLOTS],
}

const CANONICAL_SOURCE_LABELS: [&str; 7] = [
    "boolean",
    "signed_integer",
    "unsigned_integer",
    "float",
    "string",
    "array",
    "object",
];
const CANONICAL_TARGET_LABELS: [&str; 6] = [
    "boolean",
    "signed_integer",
    "unsigned_integer",
    "float",
    "string",
    "unsupported",
];
const CANONICAL_OUTCOME_LABELS: [&str; 2] = ["converted", "nulled"];
const CANONICAL_METRIC_SLOTS: usize =
    CANONICAL_SOURCE_LABELS.len() * CANONICAL_TARGET_LABELS.len() * CANONICAL_OUTCOME_LABELS.len();

impl Default for CanonicalizationSummary {
    fn default() -> Self {
        Self {
            converted: 0,
            nulled: 0,
            first_failure: None,
            metric_counts: [0; CANONICAL_METRIC_SLOTS],
        }
    }
}

impl CanonicalizationSummary {
    /// Flush at most one Prometheus lookup/increment per bounded label tuple,
    /// irrespective of the number of converted cells in the request.
    pub fn flush_metrics(&self, stream_type: StreamType) {
        for (index, count) in self.metric_counts.iter().copied().enumerate() {
            if count == 0 {
                continue;
            }
            let outcome_index = index % CANONICAL_OUTCOME_LABELS.len();
            let pair_index = index / CANONICAL_OUTCOME_LABELS.len();
            let target_index = pair_index % CANONICAL_TARGET_LABELS.len();
            let source_index = pair_index / CANONICAL_TARGET_LABELS.len();
            metrics::INGEST_SCHEMA_CASTS
                .with_label_values(&[
                    stream_type.as_str(),
                    CANONICAL_SOURCE_LABELS[source_index],
                    CANONICAL_TARGET_LABELS[target_index],
                    CANONICAL_OUTCOME_LABELS[outcome_index],
                ])
                .inc_by(count);
        }
    }
}

#[derive(Clone, Debug)]
pub struct CanonicalizationFailure {
    pub field: String,
    pub source_type: &'static str,
    pub target_type: &'static str,
}

/// Canonicalize only fields present in a JSON record using the immutable,
/// index-aligned plan carried by the schema cache. Invalid or unsupported
/// scalar conversions become null so Arrow materialization and every index
/// consumer observe the same absence.
pub fn canonicalize_record(
    stream_type: StreamType,
    schema: &SchemaCache,
    record: &mut Map<String, Value>,
) -> CanonicalizationSummary {
    let mut summary = CanonicalizationSummary::default();
    canonicalize_record_into(stream_type, schema, record, &mut summary);
    summary
}

pub fn canonicalize_record_into(
    _stream_type: StreamType,
    schema: &SchemaCache,
    record: &mut Map<String, Value>,
    summary: &mut CanonicalizationSummary,
) {
    for (field_name, value) in record.iter_mut() {
        if value.is_null() {
            continue;
        }
        let Some(target) = schema.canonical_type(field_name) else {
            // A field can only be unknown here if an out-of-band schema-cache
            // update raced this request. Leave it untouched; the request's
            // schema/CAS path already owns registering it.
            continue;
        };
        canonicalize_present_value(field_name, target, value, summary);
    }
}

/// Canonicalize one derived field without scanning the record again. Metrics
/// ingestion uses this after recomputing its series hash from already
/// normalized labels.
pub fn canonicalize_field_value(
    stream_type: StreamType,
    schema: &SchemaCache,
    field_name: &str,
    value: &mut Value,
) -> CanonicalizationSummary {
    let mut summary = CanonicalizationSummary::default();
    canonicalize_field_value_into(stream_type, schema, field_name, value, &mut summary);
    summary
}

pub fn canonicalize_field_value_into(
    _stream_type: StreamType,
    schema: &SchemaCache,
    field_name: &str,
    value: &mut Value,
    summary: &mut CanonicalizationSummary,
) {
    if value.is_null() {
        return;
    }
    if let Some(target) = schema.canonical_type(field_name) {
        canonicalize_present_value(field_name, target, value, summary);
    }
}

fn canonicalize_present_value(
    field_name: &str,
    target: CanonicalType,
    value: &mut Value,
    summary: &mut CanonicalizationSummary,
) {
    let source_type = json_scalar_type(value);
    let outcome = canonical_value(value, target);
    let outcome_index = match outcome {
        CanonicalValueOutcome::Identity => return,
        CanonicalValueOutcome::Converted => {
            summary.converted += 1;
            0
        }
        CanonicalValueOutcome::Failed => {
            *value = Value::Null;
            summary.nulled += 1;
            if summary.first_failure.is_none() {
                summary.first_failure = Some(CanonicalizationFailure {
                    field: field_name.to_string(),
                    source_type,
                    target_type: target.label(),
                });
            }
            1
        }
    };
    let source_index = canonical_source_index(source_type);
    let target_index = canonical_target_index(target);
    let metric_index = (source_index * CANONICAL_TARGET_LABELS.len() + target_index)
        * CANONICAL_OUTCOME_LABELS.len()
        + outcome_index;
    summary.metric_counts[metric_index] += 1;
}

fn canonical_source_index(label: &str) -> usize {
    match label {
        "boolean" => 0,
        "signed_integer" => 1,
        "unsigned_integer" => 2,
        "float" => 3,
        "string" => 4,
        "array" => 5,
        "object" => 6,
        _ => unreachable!("canonical source labels are closed"),
    }
}

fn canonical_target_index(target: CanonicalType) -> usize {
    match target {
        CanonicalType::Boolean => 0,
        CanonicalType::Signed(_) => 1,
        CanonicalType::Unsigned(_) => 2,
        CanonicalType::Float(_) => 3,
        CanonicalType::String => 4,
        CanonicalType::Unsupported => 5,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CanonicalValueOutcome {
    Identity,
    Converted,
    Failed,
}

fn json_scalar_type(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(number) if number.is_i64() => "signed_integer",
        Value::Number(number) if number.is_u64() => "unsigned_integer",
        Value::Number(_) => "float",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn canonical_value(value: &mut Value, target: CanonicalType) -> CanonicalValueOutcome {
    let source = json_scalar_type(value);
    let replacement = match target {
        CanonicalType::String => match value {
            Value::String(_) => return CanonicalValueOutcome::Identity,
            Value::Bool(v) => Some(Value::String(v.to_string())),
            Value::Number(v) => Some(Value::String(v.to_string())),
            _ => None,
        },
        CanonicalType::Boolean => match value {
            Value::Bool(_) => return CanonicalValueOutcome::Identity,
            Value::String(v) => v.parse::<bool>().ok().map(Value::Bool),
            Value::Number(v) => v
                .as_f64()
                .filter(|v| v.is_finite())
                .map(|v| Value::Bool(v != 0.0)),
            _ => None,
        },
        CanonicalType::Signed(bits) => signed_value(value, bits).map(|v| Value::Number(v.into())),
        CanonicalType::Unsigned(bits) => {
            unsigned_value(value, bits).map(|v| Value::Number(v.into()))
        }
        CanonicalType::Float(bits) => float_value(value, bits)
            .and_then(serde_json::Number::from_f64)
            .map(Value::Number),
        CanonicalType::Unsupported => None,
    };
    match replacement {
        Some(replacement) => {
            let outcome = if source == target.label() && *value == replacement {
                CanonicalValueOutcome::Identity
            } else {
                CanonicalValueOutcome::Converted
            };
            *value = replacement;
            outcome
        }
        None => CanonicalValueOutcome::Failed,
    }
}

fn signed_value(value: &Value, bits: u8) -> Option<i64> {
    let value = match value {
        Value::Number(v) if v.is_i64() => v.as_i64()?,
        Value::Number(v) if v.is_u64() => i64::try_from(v.as_u64()?).ok()?,
        Value::Number(v) => {
            let v = v.as_f64()?;
            (v.is_finite() && v >= i64::MIN as f64 && v < -(i64::MIN as f64))
                .then_some(v.trunc() as i64)?
        }
        Value::String(v) => v.parse().ok()?,
        Value::Bool(v) => i64::from(*v),
        _ => return None,
    };
    let (min, max) = match bits {
        8 => (i8::MIN as i64, i8::MAX as i64),
        16 => (i16::MIN as i64, i16::MAX as i64),
        32 => (i32::MIN as i64, i32::MAX as i64),
        64 => (i64::MIN, i64::MAX),
        _ => return None,
    };
    (min..=max).contains(&value).then_some(value)
}

fn unsigned_value(value: &Value, bits: u8) -> Option<u64> {
    let value = match value {
        Value::Number(v) if v.is_u64() => v.as_u64()?,
        Value::Number(v) if v.is_i64() => u64::try_from(v.as_i64()?).ok()?,
        Value::Number(v) => {
            let v = v.as_f64()?;
            (v.is_finite() && v >= 0.0 && v < u64::MAX as f64).then_some(v.trunc() as u64)?
        }
        Value::String(v) => v.parse().ok()?,
        Value::Bool(v) => u64::from(*v),
        _ => return None,
    };
    let max = match bits {
        8 => u8::MAX as u64,
        16 => u16::MAX as u64,
        32 => u32::MAX as u64,
        64 => u64::MAX,
        _ => return None,
    };
    (value <= max).then_some(value)
}

fn float_value(value: &Value, bits: u8) -> Option<f64> {
    let value = match value {
        Value::Number(v) => v.as_f64()?,
        Value::String(v) => v.parse().ok()?,
        Value::Bool(v) => u8::from(*v) as f64,
        _ => return None,
    };
    if !value.is_finite() {
        return None;
    }
    match bits {
        16 if value.abs() <= 65_504.0 => Some(value),
        32 if value.abs() <= f32::MAX as f64 => Some((value as f32) as f64),
        64 => Some(value),
        _ => None,
    }
}

pub fn get_schema_changes(schema: &SchemaCache, inferred_schema: &Schema) -> (bool, Vec<Field>) {
    let mut is_schema_changed = false;
    let mut field_datatype_delta: Vec<Field> = vec![];

    for item in inferred_schema.fields.iter() {
        let item_name = item.name();
        let item_data_type = item.data_type();

        match schema.fields_map().get(item_name) {
            None => {
                is_schema_changed = true;
            }
            Some(idx) => {
                let existing_field: Arc<Field> = schema.schema().fields()[*idx].clone();
                if existing_field.data_type() != item_data_type {
                    if infra::schema::is_widening_conversion(
                        existing_field.data_type(),
                        item_data_type,
                    ) {
                        is_schema_changed = true;
                        field_datatype_delta.push((**item).clone());
                    } else {
                        let mut meta = existing_field.metadata().clone();
                        meta.insert("zo_cast".to_owned(), true.to_string());
                        field_datatype_delta
                            .push(existing_field.as_ref().clone().with_metadata(meta));
                    }
                }
            }
        }
    }

    (is_schema_changed, field_datatype_delta)
}

pub async fn stream_schema_exists(
    org_id: &str,
    stream_name: &str,
    stream_type: StreamType,
    stream_schema_map: &mut HashMap<String, SchemaCache>,
) -> StreamSchemaChk {
    let mut schema_chk = StreamSchemaChk {
        conforms: true,
        has_fields: false,
        has_partition_keys: false,
        has_metrics_metadata: false,
    };
    let schema = match stream_schema_map.get(stream_name) {
        Some(schema) => schema.schema().clone(),
        None => {
            let schema_cache = infra::schema::get_cache(org_id, stream_name, stream_type)
                .await
                .unwrap();
            let db_schema = schema_cache.schema().clone();
            stream_schema_map.insert(stream_name.to_string(), schema_cache);
            db_schema
        }
    };
    if !schema.fields().is_empty() {
        schema_chk.has_fields = true;
    }

    let settings = unwrap_stream_settings(&schema);
    if let Some(stream_setting) = settings
        && !stream_setting.partition_keys.is_empty()
    {
        schema_chk.has_partition_keys = true;
    }
    if schema.metadata().contains_key(METADATA_LABEL) {
        schema_chk.has_metrics_metadata = true;
    }
    schema_chk
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use config::utils::json;
    use datafusion::arrow::datatypes::DataType;

    use super::*;

    #[test]
    fn canonicalization_converts_successes_and_nulls_failures() {
        let schema = SchemaCache::new(Schema::new(vec![
            Field::new("number", DataType::Int64, true),
            Field::new("flag", DataType::Boolean, true),
            Field::new("text", DataType::Utf8View, true),
            Field::new("small", DataType::UInt8, true),
        ]));
        let mut record = serde_json::json!({
            "number": "42",
            "flag": "not-a-bool",
            "text": 17,
            "small": 256,
            "unknown": "untouched"
        })
        .as_object()
        .unwrap()
        .clone();

        let summary = canonicalize_record(StreamType::Logs, &schema, &mut record);
        assert_eq!(summary.converted, 2);
        assert_eq!(summary.nulled, 2);
        assert_eq!(record["number"], serde_json::json!(42));
        assert_eq!(record["text"], serde_json::json!("17"));
        assert!(record["flag"].is_null());
        assert!(record["small"].is_null());
        assert_eq!(record["unknown"], serde_json::json!("untouched"));
    }

    #[test]
    fn canonicalization_does_not_wrap_integer_ranges_or_default_values() {
        let schema = SchemaCache::new(Schema::new(vec![
            Field::new("signed", DataType::Int64, true),
            Field::new("unsigned", DataType::UInt64, true),
            Field::new("boolean", DataType::Boolean, true),
        ]));
        let mut record = serde_json::json!({
            "signed": 18446744073709551615u64,
            "unsigned": -1,
            "boolean": "invalid"
        })
        .as_object()
        .unwrap()
        .clone();

        let summary = canonicalize_record(StreamType::Metrics, &schema, &mut record);
        assert_eq!(summary.nulled, 3);
        assert!(record.values().all(Value::is_null));
    }

    #[tokio::test]
    async fn test_check_for_schema() {
        let stream_name = "Sample";
        let org_name = "nexus";
        let record: json::Value =
            json::from_str(r#"{"Year": 1896, "City": "Athens", "_timestamp": 1234234234234}"#)
                .unwrap();

        let schema = Schema::new(vec![
            Field::new("Year", DataType::Int64, false),
            Field::new("City", DataType::Utf8, false),
            Field::new("_timestamp", DataType::Int64, false),
        ]);
        let mut map: HashMap<String, SchemaCache> = HashMap::new();

        map.insert(stream_name.to_string(), SchemaCache::new(schema));
        let (result, _) = check_for_schema(
            org_name,
            stream_name,
            StreamType::Logs,
            &mut map,
            vec![record.as_object().unwrap()],
            1234234234234,
            false,
        )
        .await
        .unwrap();
        assert!(!result.is_schema_changed);
    }

    #[tokio::test]
    async fn test_infer_schema() {
        let mut record_val: Vec<&Map<String, Value>> = vec![];

        let record1: serde_json::Value = serde_json::Value::from_str(
            r#"{"Year": 1896.99, "City": "Athens", "_timestamp": 1234234234234}"#,
        )
        .unwrap();
        record_val.push(record1.as_object().unwrap());

        let record: serde_json::Value = serde_json::Value::from_str(
            r#"{"Year": 1896, "City": "Athens", "_timestamp": 1234234234234}"#,
        )
        .unwrap();
        record_val.push(record.as_object().unwrap());
        let stream_type = StreamType::Logs;
        let value_iter = record_val.into_iter();
        infer_json_schema_from_map("test", stream_type, value_iter).unwrap();
    }
}
