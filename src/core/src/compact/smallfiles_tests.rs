// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::compact::smallfiles::Policy;

const NOW: i64 = 1_794_700_800_000_000;

fn file(i: usize, size: i64, age_secs: i64) -> FileKey {
    let id = ((NOW / 1_000 - age_secs * 1_000) << 22) + i as i64;
    let mut file = FileKey::from_file_name(&format!("{id}abcd.vix"));
    file.meta.original_size = size;
    file.meta.compressed_size = size / 2;
    file.meta.index_size = 10;
    file.meta.min_ts = i as i64;
    file.meta.max_ts = i as i64 + 100;
    file
}

fn planned(
    files: Vec<FileKey>,
    target: i64,
    age: HourAge,
    policy: Option<Policy>,
) -> PartitionPlan {
    plan_partition(
        files,
        "org",
        StreamType::Logs,
        "default",
        "prefix",
        target,
        age,
        false,
        &MergeStrategy::FileTime,
        false,
        policy,
    )
}

fn policy(now_micros: i64) -> Option<Policy> {
    Some(Policy {
        now_micros,
        quiet_secs: 3600,
    })
}

fn settled() -> HourAge {
    HourAge {
        open: false,
        settled: true,
    }
}

fn fingerprint(plan: &PartitionPlan) -> (Vec<Vec<String>>, Vec<String>) {
    (
        plan.batch_groups
            .iter()
            .map(|b| b.files.iter().map(|f| f.key.clone()).collect())
            .collect(),
        plan.heal_candidates.iter().map(|f| f.key.clone()).collect(),
    )
}

#[test]
fn smallfiles_final_interleavers_are_excluded_before_both_planners() {
    let files: Vec<_> = [80, 20, 80, 25, 80, 30]
        .into_iter()
        .enumerate()
        .map(|(i, size)| file(i, size, 7200))
        .collect();
    for enabled in [false, true] {
        let plan = planned(
            files.clone(),
            100,
            settled(),
            enabled.then(|| policy(NOW).unwrap()),
        );
        assert_eq!(plan.batch_groups.len(), 1);
        assert_eq!(
            plan.batch_groups[0]
                .files
                .iter()
                .map(|f| f.meta.original_size)
                .collect::<Vec<_>>(),
            [20, 25, 30]
        );
        assert_eq!(plan.heal_candidates.len(), 3);
    }
}

#[test]
fn smallfiles_unsettled_and_unknown_names_preserve_legacy_plan() {
    let mut files = vec![file(0, 1000, 7200), file(1, 1, 0)];
    for open in [false, true] {
        let age = HourAge {
            open,
            settled: false,
        };
        assert_eq!(
            fingerprint(&planned(files.clone(), 4096, age, None)),
            fingerprint(&planned(files.clone(), 4096, age, policy(NOW)))
        );
    }
    files[0].key = "legacy.vix".into();
    assert_eq!(
        fingerprint(&planned(files.clone(), 4096, settled(), None)),
        fingerprint(&planned(files, 4096, settled(), policy(NOW)))
    );
}

#[test]
fn smallfiles_index_classes_stay_separate() {
    let mut files: Vec<_> = (0..6).map(|i| file(i, 10, 7200)).collect();
    for (i, file) in files.iter_mut().enumerate() {
        if i < 2 {
            file.meta.index_size = 0;
        } else if i < 4 {
            file.key = file.key.replace(".vix", ".parquet");
        }
    }
    let plan = planned(files, 100, settled(), policy(NOW));
    assert_eq!(plan.batch_groups.len(), 3);
    for group in plan.batch_groups {
        assert_eq!(group.files.len(), 2);
        assert_eq!(
            group.files[0].meta.index_size,
            group.files[1].meta.index_size
        );
        assert_eq!(
            group.files[0].key.ends_with(".vix"),
            group.files[1].key.ends_with(".vix")
        );
    }
}

#[test]
fn smallfiles_real_distributions_converge_without_rewriting_finals() {
    // Compressed object sizes from the supplied 2026-11-15 snapshot. The
    // 13x logs / 7x traces original sizes are explicit modelling assumptions.
    let snapshots: std::collections::BTreeMap<String, Vec<i64>> =
        serde_json::from_str(include_str!("testdata/smallfiles_sizes.json")).unwrap();
    assert_eq!(snapshots.len(), 14);
    assert_eq!(snapshots.values().map(Vec::len).sum::<usize>(), 8519);
    for (name, sizes) in snapshots {
        let ratio = if name.starts_with("traces/") { 7 } else { 13 };
        for target in [4096_i64 << 20, 8192_i64 << 20] {
            let files: Vec<_> = sizes
                .iter()
                .enumerate()
                .map(|(i, size)| {
                    let mut file = file(i, size * ratio, 7200);
                    file.meta.compressed_size = *size;
                    file
                })
                .collect();
            for enabled in [false, true] {
                let mut files = files.clone();
                let total: i64 = files.iter().map(|f| f.meta.original_size).sum();
                let finals: Vec<_> = files
                    .iter()
                    .filter(|f| f.meta.original_size > target / 2)
                    .cloned()
                    .collect();
                let mut now = NOW;
                let mut serial = 100_000;
                for _ in 0..16 {
                    let plan = planned(
                        files.clone(),
                        target,
                        settled(),
                        enabled.then(|| policy(now).unwrap()),
                    );
                    if plan.batch_groups.is_empty() {
                        break;
                    }
                    let mut consumed = HashSet::new();
                    let mut output = Vec::new();
                    for batch in plan.batch_groups {
                        assert!(batch.files.len() >= 2);
                        assert!(batch.files.len() <= merge_group_width());
                        let size: i64 = batch.files.iter().map(|f| f.meta.original_size).sum();
                        assert!(size <= target);
                        for input in &batch.files {
                            assert!(input.meta.original_size <= target / 2);
                            assert!(consumed.insert(input.key.clone()));
                            assert!(files.contains(input));
                        }
                        let mut merged = file(serial, size, 0);
                        serial += 1;
                        let id = ((now / 1_000) << 22) + serial as i64;
                        merged.key = format!("{id}abcd.vix");
                        merged.meta.compressed_size =
                            batch.files.iter().map(|f| f.meta.compressed_size).sum();
                        output.push(merged);
                    }
                    files.retain(|f| !consumed.contains(&f.key));
                    files.extend(output);
                    assert_eq!(
                        files.iter().map(|f| f.meta.original_size).sum::<i64>(),
                        total
                    );
                    assert!(finals.iter().all(|f| files.contains(f)));
                    now += 3_601_000_000;
                }
                assert!(
                    files
                        .iter()
                        .filter(|f| f.meta.original_size <= target / 2)
                        .count()
                        <= 1,
                    "{name} target={target} enabled={enabled}"
                );
            }
        }
    }
}

#[test]
fn smallfiles_continuous_arrivals_bound_rewrites() {
    let simulate = |enabled: bool| {
        let mut files = vec![file(0, 1000, 7200)];
        let mut serial = 100_000;
        let mut rewritten = 0;
        let mut merges = 0;
        for step in 0..1002 {
            if step < 1000 {
                files.push(file(serial, 1, 0));
                serial += 1;
            }
            let now = NOW
                + if step < 1000 {
                    step * 1_000_000
                } else {
                    4_601_000_000
                };
            let plan = planned(
                files.clone(),
                4096,
                settled(),
                enabled.then(|| policy(now).unwrap()),
            );
            let mut consumed = HashSet::new();
            let mut output = Vec::new();
            for batch in plan.batch_groups {
                let size: i64 = batch.files.iter().map(|f| f.meta.original_size).sum();
                if enabled && step < 1000 && size <= 2048 {
                    assert!(
                        size >= 2 * batch
                            .files
                            .iter()
                            .map(|f| f.meta.original_size)
                            .max()
                            .unwrap()
                    );
                }
                for input in batch.files {
                    assert!(consumed.insert(input.key));
                }
                let mut merged = file(serial, size, 0);
                let id = ((now / 1_000) << 22) + serial as i64;
                merged.key = format!("{id}abcd.vix");
                serial += 1;
                output.push(merged);
                rewritten += size;
                merges += 1;
            }
            files.retain(|f| !consumed.contains(&f.key));
            files.extend(output);
            assert_eq!(
                files.iter().map(|f| f.meta.original_size).sum::<i64>(),
                1000 + (step + 1).min(1000)
            );
        }
        assert_eq!(files.len(), 1);
        (merges, rewritten)
    };
    let legacy = simulate(false);
    let bounded = simulate(true);
    assert_eq!(legacy, (1000, 1_500_500));
    assert!(bounded.0 <= 501);
    assert!(bounded.1 < 10_000);
}
