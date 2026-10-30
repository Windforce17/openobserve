// Copyright 2026 OpenObserve Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

//! Same input and SQL: replace only DataFusion's partial aggregate with VIX
//! partials. Keep its final aggregate, projection, deterministic sort and LIMIT.
//! No production storage, network service or approximate collector is involved.

use arrow::{
    array::{Array, Int64Array, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema},
};
use bytes::Bytes;
use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    datasource::MemTable,
    physical_plan::{
        ExecutionPlan,
        aggregates::{AggregateExec, AggregateMode},
        collect,
    },
    prelude::{SessionConfig, SessionContext},
};
use vortex_index::{VixWriter, VixWriterOptions};

use super::*;
use crate::datafusion::plan::index_optimize_exec::IndexOptimizeExec;

type Groups = Vec<(Vec<Option<String>>, u64)>;

fn fixture(values: &[Option<String>], offset: i64) -> (RecordBatch, VixReader) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("svc", DataType::Utf8, true),
        Field::new("other", DataType::Utf8, true),
    ]));
    let timestamps: Vec<_> = (0..values.len()).map(|i| offset - i as i64).collect();
    let other: Vec<_> = (0..values.len())
        .map(|i| match i % 3 {
            0 => Some("keep"),
            1 => Some("drop"),
            _ => None,
        })
        .collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(timestamps.clone())),
            Arc::new(StringArray::from(values.to_vec())),
            Arc::new(StringArray::from(other.clone())),
        ],
    )
    .unwrap();
    // Alternate explicit JSON null and a missing source field. Both are SQL
    // NULL in the typed column; neither is the present empty string.
    let sources: Vec<_> = values
        .iter()
        .enumerate()
        .map(|(i, value)| {
            let mut source = serde_json::json!({"_timestamp": timestamps[i], "other": other[i]});
            if value.is_some() || i % 2 == 0 {
                source["svc"] = serde_json::to_value(value).unwrap();
            }
            source.to_string()
        })
        .collect();
    let mut writer = VixWriter::new(
        &schema,
        VixWriterOptions {
            row_group_size: 3,
            ..Default::default()
        },
        false,
    );
    writer
        .push_batch_with_source(&batch, &StringArray::from(sources), None)
        .unwrap();
    let (data, index) = writer.finish().unwrap();
    let reader = VixReader::open_with_index(Bytes::from(data), index.map(Bytes::from)).unwrap();
    (batch, reader)
}

fn context(batches: Vec<RecordBatch>) -> SessionContext {
    let schema = batches[0].schema();
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(2));
    ctx.register_table(
        "t",
        Arc::new(
            MemTable::try_new(
                schema,
                batches.into_iter().map(|batch| vec![batch]).collect(),
            )
            .unwrap(),
        ),
    )
    .unwrap();
    ctx
}

fn rows(batches: Vec<RecordBatch>) -> Groups {
    let mut result = Vec::new();
    for batch in batches {
        let width = batch.num_columns() - 1;
        let keys: Vec<_> = batch.columns()[..width]
            .iter()
            .map(|column| arrow::compute::cast(column, &DataType::Utf8).unwrap())
            .collect();
        let keys: Vec<_> = keys
            .iter()
            .map(|column| column.as_any().downcast_ref::<StringArray>().unwrap())
            .collect();
        let counts = arrow::compute::cast(batch.column(width), &DataType::Int64).unwrap();
        let counts = counts.as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..batch.num_rows() {
            result.push((
                keys.iter()
                    .map(|key| (!key.is_null(i)).then(|| key.value(i).to_owned()))
                    .collect(),
                counts.value(i) as u64,
            ));
        }
    }
    result
}

async fn compare_same_sql(
    batches: Vec<RecordBatch>,
    groups: Groups,
    fields: &[&str],
    predicate: &str,
) -> Groups {
    let ctx = context(batches);
    let fields_sql = fields.join(", ");
    // Tie-break all group fields, including NULL placement. A count-only order
    // has no deterministic winner at LIMIT and is not a valid rowwise oracle.
    let order = fields
        .iter()
        .map(|field| format!("{field} ASC NULLS FIRST"))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT {fields_sql}, COUNT(*) AS cnt FROM t {predicate} GROUP BY {fields_sql} ORDER BY cnt DESC, {order} LIMIT 20"
    );
    let logical = ctx.state().create_logical_plan(&sql).await.unwrap();
    let plan = ctx.state().create_physical_plan(&logical).await.unwrap();
    let baseline = rows(collect(plan.clone(), ctx.task_ctx()).await.unwrap());
    let query = Arc::new(crate::types::QueryParams {
        trace_id: "rc4-exact-baseline".into(),
        org_id: "org".into(),
        stream: "t".into(),
        stream_type: config::meta::stream::StreamType::Logs,
        stream_name: "t".into(),
        time_range: (0, 1_000_001),
        work_group: None,
        use_inverted_index: true,
        full_text_fields: None,
    });
    let mut partials = Some(groups);
    let optimized = plan
        .transform_up(|node| {
            if node
                .downcast_ref::<AggregateExec>()
                .is_some_and(|aggregate| *aggregate.mode() == AggregateMode::Partial)
            {
                let groups = partials
                    .take()
                    .expect("exactly one partial aggregate in fixture");
                Ok(Transformed::yes(Arc::new(IndexOptimizeExec::new(
                    query.clone(),
                    node.schema(),
                    vec![],
                    MultiResult::TopN(groups),
                    IndexOptimizeMode::SimpleTopN(
                        fields.iter().map(|field| (*field).to_owned()).collect(),
                        20,
                        false,
                    ),
                ))
                    as Arc<dyn ExecutionPlan>))
            } else {
                Ok(Transformed::no(node))
            }
        })
        .unwrap()
        .data;
    assert!(
        partials.is_none(),
        "fixture must replace the real partial aggregate: {sql}"
    );
    let actual = rows(collect(optimized, ctx.task_ctx()).await.unwrap());
    assert_eq!(
        actual, baseline,
        "same SQL, complete rows including validity: {sql}"
    );
    baseline
}

#[tokio::test]
async fn rc4_same_sql_nullable_empty_missing_tuples_and_filtered_boundaries() {
    let values: Vec<_> = [
        Some("api"),
        None,
        Some(""),
        None,
        Some("db"),
        Some("api"),
        Some("db"),
        Some(""),
        None,
    ]
    .into_iter()
    .map(|value| value.map(str::to_owned))
    .collect();
    let (a, ra) = fixture(&values, 100);
    let (b, rb) = fixture(&values, 200);
    for fields in [vec!["svc"], vec!["svc", "other"]] {
        for filtered in [false, true] {
            let condition = IndexCondition {
                conditions: if filtered {
                    vec![crate::index::Condition::Equal(
                        "other".into(),
                        "keep".into(),
                    )]
                } else {
                    vec![crate::index::Condition::All()]
                },
            };
            let mut groups = Vec::new();
            let range = if filtered { (95, 199) } else { (0, 1_000_001) };
            for reader in [&ra, &rb] {
                let result = evaluate_vix_index(
                    "rc4-null",
                    reader,
                    &condition,
                    Some(IndexOptimizeMode::SimpleTopN(
                        fields.iter().map(|field| (*field).into()).collect(),
                        20,
                        false,
                    )),
                    range,
                    !filtered,
                    None,
                    None,
                    None,
                )
                .unwrap();
                match result {
                    RawVixResult::TopN {
                        groups: partials,
                        has_skipped: false,
                    } => groups.extend(partials),
                    other => panic!("expected complete groups: {other:?}"),
                }
            }
            compare_same_sql(
                vec![a.clone(), b.clone()],
                groups,
                &fields,
                if filtered {
                    "WHERE other = 'keep' AND _timestamp >= 95 AND _timestamp < 199"
                } else {
                    ""
                },
            )
            .await;
        }
    }
}

#[tokio::test]
async fn rc4_over_budget_and_cached_refusal_use_the_same_exact_sql() {
    let cfg = get_config();
    let n = cfg.limit.inverted_index_topn_max_group_num + 1;
    let values: Vec<_> = (0..n).map(|i| Some(format!("svc-{i:06}"))).collect();
    let (batch, reader) = fixture(&values, 1_000_000);
    let condition = IndexCondition {
        conditions: vec![crate::index::Condition::All()],
    };
    let rule = Some(IndexOptimizeMode::SimpleTopN(vec!["svc".into()], 20, false));
    let file = FileKey {
        key: "files/org/logs/rc4-refusal/fixture.vix".into(),
        meta: config::meta::stream::FileMeta {
            index_generation: 9,
            index_size: 100,
            records: n as i64,
            compressed_size: 100,
            ..Default::default()
        },
        ..Default::default()
    };
    let key = aggregate_reject_key(&condition, &rule, &file, None, None, &cfg.limit);
    let cache = aggregate_reject::RejectCache::new(2);
    let mut probes = 0;
    for _ in 0..2 {
        if cache.get(&key).is_none() {
            probes += 1;
            let error = evaluate_vix_index(
                "rc4-over-budget",
                &reader,
                &condition,
                rule.clone(),
                (0, 1_000_001),
                true,
                None,
                None,
                None,
            )
            .unwrap_err();
            assert!(requires_exact_scan(&error));
            cache.put(key.clone(), &error);
        }
        assert!(cache.get(&key).is_some());
        // Fallback is a real DataFusion aggregation over all rows, with no
        // file-local LIMIT. Its final SQL is identical to the full scan.
        let scan = context(vec![batch.clone()]);
        let partials = rows(
            scan.sql("SELECT svc, COUNT(*) AS cnt FROM t GROUP BY svc")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap(),
        );
        compare_same_sql(
            vec![batch.clone(), batch.clone()],
            partials.iter().cloned().chain(partials.clone()).collect(),
            &["svc"],
            "",
        )
        .await;
    }
    assert_eq!(probes, 1);
    let mut changed = file.clone();
    changed.meta.index_generation += 1;
    assert!(
        cache
            .get(&aggregate_reject_key(
                &condition, &rule, &changed, None, None, &cfg.limit
            ))
            .is_none()
    );
    let budget = config::config::Limit {
        inverted_index_topn_max_group_num: cfg.limit.inverted_index_topn_max_group_num * 2,
        vix_topn_null_group_enabled: cfg.limit.vix_topn_null_group_enabled,
        vix_agg_selected_dictionary_budget_enabled: cfg
            .limit
            .vix_agg_selected_dictionary_budget_enabled,
        vix_agg_early_cardinality_check_enabled: cfg.limit.vix_agg_early_cardinality_check_enabled,
        ..Default::default()
    };
    assert!(
        cache
            .get(&aggregate_reject_key(
                &condition, &rule, &file, None, None, &budget
            ))
            .is_none()
    );
    assert!(
        cache
            .get(&aggregate_reject_key(
                &condition,
                &rule,
                &file,
                Some((1, 2)),
                None,
                &cfg.limit
            ))
            .is_none()
    );
}

#[test]
fn rc4_probe_window_stays_bounded_and_expands_in_stages() {
    assert_eq!(aggregate_window_for(0, 256), 32);
    assert_eq!(aggregate_window_for(31, 256), 32);
    assert_eq!(aggregate_window_for(32, 256), 64);
    assert_eq!(aggregate_window_for(96, 256), 128);
    assert_eq!(aggregate_window_for(224, 256), 256);
    assert_eq!(aggregate_window_for(usize::MAX, 256), 256);
    for cap in [0, 1, 17, 64, 256] {
        assert!(aggregate_window_for(0, cap) <= cap.max(1));
    }
}

#[tokio::test]
async fn rc4_dictionary_null_proof_matches_sql_for_filtered_and_absent_fields() {
    let values = [Some(""), None, Some("api"), None, Some("db"), Some("api")]
        .into_iter()
        .map(|value| value.map(str::to_owned))
        .collect::<Vec<_>>();
    let (batch, reader) = fixture(&values, 100);
    let selected = arrow::buffer::BooleanBuffer::from_iter([false, true, true, true, true, false]);
    let groups = super::collect::filtered_top_n(&reader, &selected, "svc", 20, false)
        .unwrap()
        .expect("complete raw counts prove the selected NULL complement");
    compare_same_sql(
        vec![batch],
        groups,
        &["svc"],
        "WHERE _timestamp >= 96 AND _timestamp < 100",
    )
    .await;

    let (baseline, _) = fixture(&[None, None, None], 100);
    // A schema-evolution file omits svc altogether. The scan's table schema
    // supplies NULL; an indexed absence proof may return the same NULL group.
    let stored = baseline.project(&[0, 2]).unwrap();
    let mut writer = VixWriter::new(&stored.schema(), VixWriterOptions::default(), false);
    writer
        .push_batch_with_source(
            &stored,
            &StringArray::from(vec![
                r#"{"_timestamp":100,"other":"keep"}"#,
                r#"{"_timestamp":99,"other":"drop"}"#,
                r#"{"_timestamp":98,"other":null}"#,
            ]),
            None,
        )
        .unwrap();
    let (data, index) = writer.finish().unwrap();
    let reader =
        VixReader::open_with_index(Bytes::from(data.clone()), index.map(Bytes::from)).unwrap();
    let groups = super::collect::unfiltered_top_n(&reader, "svc", 20, false)
        .unwrap()
        .unwrap();
    assert_eq!(groups, vec![(vec![None], 3)]);
    compare_same_sql(vec![baseline], groups, &["svc"], "").await;
    let unindexed = VixReader::open_with_index(Bytes::from(data), None).unwrap();
    assert!(
        super::collect::unfiltered_top_n(&unindexed, "svc", 20, false)
            .unwrap()
            .is_none()
    );
}
