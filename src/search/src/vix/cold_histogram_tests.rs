// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Local-fixture verification: pipe a JSON manifest to stdin and run the ignored
//! `real_histograms_from_stdin` test with `--nocapture --test-threads=1`.
//! Each entry has `sample`, `data_path`, and `index_path`; no remote IO is used.

use std::{collections::BTreeMap, io::Read, sync::Arc};

use anyhow::{Context, ensure};
use arrow::{
    array::{Array, Int64Array, StringArray},
    compute::cast,
    datatypes::DataType,
};
use bytes::Bytes;
use serde::Deserialize;
use vortex_index::{VixDocs, VixReader};

use super::{RawVixResult, evaluate_vix_index, ranged_parity_tests::MemRangeSource};
use crate::index::{Condition, IndexCondition};

#[derive(Deserialize)]
struct Sample {
    sample: usize,
    data_path: String,
    index_path: String,
}

#[test]
#[ignore = "requires a local sample manifest on stdin"]
fn real_histograms_from_stdin() -> anyhow::Result<()> {
    use config::meta::inverted_index::IndexOptimizeMode;

    let mut input = String::new();
    std::io::stdin().read_to_string(&mut input)?;
    let samples: Vec<Sample> = serde_json::from_str(&input)?;
    ensure!(!samples.is_empty(), "empty fixture manifest");
    for sample in samples {
        let data = Bytes::from(std::fs::read(&sample.data_path)?);
        let index = Bytes::from(std::fs::read(&sample.index_path)?);
        let open = || {
            VixReader::open_ranged_with_index_sidecar_first(
                Arc::new(MemRangeSource(data.clone())),
                Some(Arc::new(MemRangeSource(index.clone()))),
                true,
            )
        };
        let oracle = open()?;
        let services = cast(&oracle.read_column("service_name")?, &DataType::Utf8)?;
        let services = services
            .as_any()
            .downcast_ref::<StringArray>()
            .context("string services")?;
        let timestamps = cast(&oracle.read_column("_timestamp")?, &DataType::Int64)?;
        let timestamps = timestamps
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("integer timestamps")?;
        ensure!(timestamps.null_count() == 0, "null fixture timestamps");
        let min = *timestamps
            .values()
            .iter()
            .min()
            .context("empty timestamps")?;
        let max = *timestamps
            .values()
            .iter()
            .max()
            .context("empty timestamps")?;
        ensure!(max - min >= 4, "fixture must span a time clamp");
        let mut counts = BTreeMap::<String, u64>::new();
        for service in services.iter().flatten() {
            *counts.entry(service.to_owned()).or_default() += 1;
        }
        ensure!(
            !counts.contains_key("sandbox-runtime-v2"),
            "missing-value fixture contains the probe"
        );
        let common = if counts.contains_key("bot-framework-service") {
            "bot-framework-service".to_owned()
        } else {
            counts
                .iter()
                .max_by_key(|(_, count)| *count)
                .context("no services")?
                .0
                .clone()
        };
        let rare = counts
            .iter()
            .min_by_key(|(_, count)| *count)
            .context("no services")?
            .0
            .clone();
        let time_clamp = (min + (max - min) / 4, max - (max - min) / 4);
        for needle in ["sandbox-runtime-v2".to_owned(), common, rare] {
            for (width, buckets) in [
                ((max - min + 1) as u64, 1usize),
                (((max - min + 1) / 4) as u64, 5),
            ] {
                for clamp in [None, Some(time_clamp)] {
                    let range = clamp.unwrap_or((min, max + 1));
                    let mut expected = vec![0u64; buckets];
                    for row in 0..services.len() {
                        let ts = timestamps.value(row);
                        if !services.is_null(row)
                            && services.value(row) == needle
                            && ts >= range.0
                            && ts < range.1
                        {
                            expected[((ts - min) as u64 / width) as usize] += 1;
                        }
                    }
                    let mut condition = IndexCondition::new();
                    condition
                        .add_condition(Condition::Equal("service_name".into(), needle.clone()));
                    let reader = open()?;
                    let result = evaluate_vix_index(
                        "real-cold-histogram",
                        &reader,
                        &condition,
                        Some(IndexOptimizeMode::SimpleHistogram(min, width, buckets, 0)),
                        range,
                        clamp.is_none(),
                        Some((min, max)),
                        None,
                        None,
                    )?;
                    match result {
                        RawVixResult::Histogram {
                            histogram,
                            has_skipped: false,
                        } => assert_eq!(
                            histogram, expected,
                            "postings sample {} {needle} buckets={buckets} clamp={clamp:?}",
                            sample.sample
                        ),
                        other => panic!("unexpected histogram result: {other:?}"),
                    }
                    let native =
                        VixDocs::open_ranged_data_only(Arc::new(MemRangeSource(data.clone())))?;
                    let native = if buckets == 1 {
                        vec![
                            native
                                .eq_string_count("service_name", &needle, clamp)?
                                .context("native count unavailable")?,
                        ]
                    } else {
                        native
                            .eq_string_histogram(
                                "service_name",
                                &needle,
                                clamp,
                                min,
                                width,
                                buckets,
                                0,
                            )?
                            .context("native histogram unavailable")?
                    };
                    assert_eq!(
                        native, expected,
                        "native sample {} {needle} buckets={buckets} clamp={clamp:?}",
                        sample.sample
                    );
                }
            }
        }
        println!(
            "sample {}: native/postings/raw-column histograms agree (12 cases)",
            sample.sample
        );
    }
    Ok(())
}
