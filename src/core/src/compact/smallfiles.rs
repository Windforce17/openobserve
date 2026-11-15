// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::HashSet;

use config::meta::stream::FileKey;

#[derive(Clone, Copy)]
pub(super) struct Policy {
    pub now_micros: i64,
    pub quiet_secs: u64,
}

/// Return None for names without a verifiable product-generated timestamp.
/// Such partitions retain the legacy planner, including its tail convergence.
fn created_micros(key: &str) -> Option<i64> {
    let name = key.rsplit('/').next()?.rsplit_once('.')?.0;
    if name.len() != 23 || !name.is_ascii() {
        return None;
    }
    let (number, suffix) = name.split_at(19);
    if !number.bytes().all(|b| b.is_ascii_digit()) || !suffix.bytes().all(|b| b.is_ascii_hexdigit())
    {
        return None;
    }
    let id = number.parse::<i64>().ok()?;
    (id > 0)
        .then(|| config::ider::to_timestamp_millis(id))?
        .checked_mul(1_000)
}

/// Pack only non-final, same-class inputs. A size-ordered young group must
/// either become final or double its largest input. Mature inputs can close
/// out without that growth, independently of newer arrivals in the partition.
/// Every input to a close-out must have cooled down, including previous outputs.
pub(super) fn groups(
    files: &[FileKey],
    target: i64,
    width: usize,
    policy: Policy,
) -> Option<Vec<Vec<FileKey>>> {
    let quiet_micros = i64::try_from(policy.quiet_secs)
        .ok()?
        .checked_mul(1_000_000)?;
    let mut mature = Vec::new();
    for file in files {
        if file.meta.original_size < 0
            || file.meta.compressed_size < 0
            || file.meta.original_size > target / 2
        {
            return None;
        }
        let created = created_micros(&file.key)?;
        if created > policy.now_micros {
            return None;
        }
        if policy.now_micros.saturating_sub(created) >= quiet_micros {
            mature.push((created, file.clone()));
        }
    }
    mature.sort_by(|(a_created, a), (b_created, b)| (a_created, &a.key).cmp(&(b_created, &b.key)));
    let mut result = pack(
        mature.into_iter().map(|(_, file)| file).collect(),
        target,
        width,
        true,
    );
    let consumed: HashSet<&str> = result
        .iter()
        .flatten()
        .map(|file| file.key.as_str())
        .collect();
    let mut remaining: Vec<_> = files
        .iter()
        .filter(|file| !consumed.contains(file.key.as_str()))
        .cloned()
        .collect();
    remaining.sort_by(|a, b| {
        (a.meta.original_size, a.meta.min_ts, &a.key).cmp(&(
            b.meta.original_size,
            b.meta.min_ts,
            &b.key,
        ))
    });
    result.extend(pack(remaining, target, width, false));
    Some(result)
}

fn pack(files: Vec<FileKey>, target: i64, width: usize, mature: bool) -> Vec<Vec<FileKey>> {
    let mut result = Vec::new();
    let mut pending = Vec::new();
    let smallest_compressed = files
        .iter()
        .map(|file| file.meta.compressed_size)
        .min()
        .unwrap_or(0);
    let mut original = 0_i128;
    let mut compressed = 0_i128;
    for file in files {
        if file.meta.compressed_size > target
            || (mature
                && i128::from(file.meta.compressed_size) + i128::from(smallest_compressed)
                    > i128::from(target))
        {
            continue;
        }
        if original + i128::from(file.meta.original_size) > i128::from(target)
            || compressed + i128::from(file.meta.compressed_size) > i128::from(target)
            || (width > 0 && pending.len() >= width)
        {
            // Do not discard the oldest singleton behind an incompatible
            // compressed-size interleaver. A compatible partner may follow.
            if mature && pending.len() == 1 {
                continue;
            }
            seal(&mut result, std::mem::take(&mut pending), target, mature);
            original = 0;
            compressed = 0;
        }
        original += i128::from(file.meta.original_size);
        compressed += i128::from(file.meta.compressed_size);
        pending.push(file);
    }
    seal(&mut result, pending, target, mature);
    result
}

fn seal(result: &mut Vec<Vec<FileKey>>, mut files: Vec<FileKey>, target: i64, mature: bool) {
    let mut total: i128 = files.iter().map(|f| i128::from(f.meta.original_size)).sum();
    while files.len() > 1 {
        let largest = files.last().unwrap().meta.original_size;
        if mature || total > i128::from(target / 2) || total >= 2 * i128::from(largest) {
            result.push(files);
            return;
        }
        total -= i128::from(largest);
        files.pop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: i64 = 1_794_700_800_000_000;
    const POLICY: Policy = Policy {
        now_micros: NOW,
        quiet_secs: 3_600,
    };

    fn file(i: usize, size: i64, age_secs: i64) -> FileKey {
        let id = ((NOW / 1_000 - age_secs * 1_000) << 22) + i as i64;
        let mut file = FileKey::from_file_name(&format!("{id}abcd.vix"));
        file.meta.original_size = size;
        file.meta.compressed_size = size / 2;
        file
    }

    fn sizes(groups: &[Vec<FileKey>]) -> Vec<Vec<i64>> {
        groups
            .iter()
            .map(|g| g.iter().map(|f| f.meta.original_size).collect())
            .collect()
    }

    #[test]
    fn names_are_checked_before_timestamp_conversion() {
        assert_eq!(created_micros(&file(0, 1, 0).key), Some(NOW));
        for key in [
            "legacy.vix",
            "123.vix",
            "../x",
            "9999999999999999999abcd.vix",
        ] {
            assert_eq!(created_micros(key), None);
        }
        let mut unknown = file(0, 1, 0);
        unknown.key = "legacy.vix".into();
        assert!(groups(&[unknown], 4096, 128, POLICY).is_none());
        assert!(groups(&[file(0, 1, -1)], 4096, 128, POLICY).is_none());
    }

    #[test]
    fn active_tail_requires_growth_but_quiet_tail_converges() {
        let active = [file(0, 1000, 7200), file(1, 1, 0)];
        assert!(groups(&active, 4096, 128, POLICY).unwrap().is_empty());
        let quiet = Policy {
            now_micros: NOW + 3_600_000_000,
            ..POLICY
        };
        assert_eq!(
            sizes(&groups(&active, 4096, 128, quiet).unwrap()),
            [[1000, 1]]
        );
        let equal = [file(0, 1000, 0), file(1, 1000, 0)];
        assert_eq!(
            sizes(&groups(&equal, 4096, 128, POLICY).unwrap()),
            [[1000, 1000]]
        );
    }

    #[test]
    fn continuous_arrivals_cannot_reset_mature_input_deadlines() {
        let files = [file(0, 1000, 7200), file(1, 1, 3600), file(2, 1, 0)];
        assert_eq!(
            sizes(&groups(&files, 4096, 128, POLICY).unwrap()),
            [[1000, 1]]
        );
        // A fresh close-out output cannot absorb the remaining tiny file
        // through the timeout exception again until both inputs cool down.
        let remaining = [file(3, 1001, 0), file(2, 1, 0)];
        let before_deadline = Policy {
            now_micros: NOW + 3_599_000_000,
            ..POLICY
        };
        assert!(
            groups(&remaining, 4096, 128, before_deadline)
                .unwrap()
                .is_empty()
        );
        let at_deadline = Policy {
            now_micros: NOW + 3_600_000_000,
            ..POLICY
        };
        assert_eq!(
            sizes(&groups(&remaining, 4096, 128, at_deadline).unwrap()),
            [[1, 1001]]
        );
    }

    #[test]
    fn mature_inputs_are_oldest_first_even_if_event_time_is_reversed() {
        let files = [file(0, 40, 3600), file(1, 40, 3600), file(2, 40, 7200)];
        let groups = groups(&files, 100, 128, POLICY).unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0][0], files[2]);
        assert_eq!(groups[0][1], files[0]);
    }

    #[test]
    fn protects_accumulator_while_coalescing_late_files() {
        let files = [file(0, 1000, 7200), file(1, 10, 0), file(2, 10, 0)];
        assert_eq!(
            sizes(&groups(&files, 4096, 128, POLICY).unwrap()),
            [[10, 10]]
        );
        let files = [file(0, 2048, 7200), file(1, 1, 0)];
        assert_eq!(
            sizes(&groups(&files, 4096, 128, POLICY).unwrap()),
            [[1, 2048]]
        );
    }

    #[test]
    fn every_group_grows_or_all_inputs_have_cooled_down() {
        for width in [2, 3, 128] {
            for bits in 0..4096 {
                let files: Vec<_> = (0..6)
                    .map(|i| {
                        let value = (bits >> (2 * i)) & 3;
                        file(i, [1, 10, 49, 50][value], if i % 2 == 0 { 3600 } else { 0 })
                    })
                    .collect();
                let mut consumed = HashSet::new();
                for group in groups(&files, 100, width, POLICY).unwrap() {
                    let total: i64 = group.iter().map(|f| f.meta.original_size).sum();
                    let largest = group.iter().map(|f| f.meta.original_size).max().unwrap();
                    assert!(group.len() >= 2 && group.len() <= width);
                    assert!(total <= 100);
                    assert!(
                        total > 50
                            || total >= 2 * largest
                            || group.iter().all(|f| {
                                NOW - created_micros(&f.key).unwrap() >= 3_600_000_000
                            })
                    );
                    for input in group {
                        assert!(consumed.insert(input.key));
                    }
                }
            }
        }
    }

    #[test]
    fn respects_both_byte_caps_and_width() {
        let mut files: Vec<_> = (0..300).map(|i| file(i, 1, 7200)).collect();
        assert_eq!(groups(&files, 4096, 128, POLICY).unwrap().len(), 3);
        assert_eq!(groups(&files, 4096, 2, POLICY).unwrap().len(), 150);
        assert!(groups(&files, 4096, 1, POLICY).unwrap().is_empty());
        for file in &mut files {
            file.meta.compressed_size = 2048;
        }
        assert!(
            groups(&files, 4096, 128, POLICY)
                .unwrap()
                .iter()
                .all(|g| g.len() == 2)
        );
        files[0].meta.compressed_size = 5000;
        assert!(
            groups(&files, 4096, 128, POLICY)
                .unwrap()
                .iter()
                .flatten()
                .all(|f| f.meta.compressed_size <= 4096)
        );
    }

    #[test]
    fn oversized_compressed_interleaver_does_not_split_eligible_files() {
        let mut files = [file(0, 10, 7200), file(1, 10, 7200), file(2, 10, 7200)];
        files[1].meta.compressed_size = 5000;
        let groups = groups(&files, 4096, 128, POLICY).unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0], [files[0].clone(), files[2].clone()]);
    }

    #[test]
    fn mature_compressed_interleavers_preserve_oldest_compatible_input() {
        let mut files = [file(0, 10, 7200), file(1, 10, 3600), file(2, 10, 3600)];
        for (file, size) in files.iter_mut().zip([3000, 4000, 1000]) {
            file.meta.compressed_size = size;
        }
        let groups = groups(&files, 4096, 128, POLICY).unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0], [files[0].clone(), files[2].clone()]);
    }

    #[test]
    fn mature_backlog_progresses_whenever_a_pair_fits_both_caps() {
        for shape in 0..3125 {
            let mut digits = shape;
            let files: Vec<_> = (0..5)
                .map(|i| {
                    let mut f = file(i, 10, 7200);
                    f.meta.compressed_size = [1, 40, 60, 100, 101][digits % 5];
                    digits /= 5;
                    f
                })
                .collect();
            let pair_fits =
                |a: &FileKey, b: &FileKey| a.meta.compressed_size + b.meta.compressed_size <= 100;
            let pair_exists = files
                .iter()
                .enumerate()
                .any(|(i, a)| files.iter().skip(i + 1).any(|b| pair_fits(a, b)));
            for width in [2, 3, 128] {
                let groups = groups(&files, 100, width, POLICY).unwrap();
                assert_eq!(!groups.is_empty(), pair_exists);
                if files.iter().skip(1).any(|b| pair_fits(&files[0], b)) {
                    assert!(groups.iter().flatten().any(|f| f.key == files[0].key));
                }
            }
        }
    }

    #[test]
    fn final_files_and_invalid_sizes_cannot_enter_new_groups() {
        assert!(groups(&[file(0, 2049, 7200)], 4096, 128, POLICY).is_none());
        assert!(groups(&[file(0, -1, 7200)], 4096, 128, POLICY).is_none());
    }
}
