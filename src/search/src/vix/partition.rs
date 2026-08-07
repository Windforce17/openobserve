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

use config::meta::{inverted_index::IndexOptimizeMode, stream::FileKey};

// partition the vix index files by time range
// the return file groups should execute one by one
pub(super) fn partition_vix_files(
    index_parquet_files: Vec<FileKey>,
    idx_optimize_mode: &Option<IndexOptimizeMode>,
    target_partitions: usize,
) -> Vec<Vec<FileKey>> {
    if let Some(IndexOptimizeMode::SimpleSelect(limit, ascend)) = idx_optimize_mode
        && *limit > 0
    {
        best_first_waves(index_parquet_files, *ascend, target_partitions)
    } else if index_parquet_files.is_empty() {
        Vec::new()
    } else {
        // one group: only SimpleSelect prunes between groups, every other
        // mode needs all files anyway — the eval-concurrency semaphore
        // bounds the fan-out, while multiple groups would add a wait-for-
        // the-slowest-file barrier per chunk
        vec![index_parquet_files]
    }
}

/// First wave size for [`best_first_waves`]: small enough that the common
/// "newest files satisfy the limit" query touches a handful of files, with
/// doubling keeping the worst case (a filter that never satisfies the
/// limit) at O(log) sequential rounds over the old single-round layout.
const FIRST_WAVE_SIZE: usize = 4;

/// Best-first waves for the timestamp-ordered LIMIT shape (`SimpleSelect`):
/// files sorted so the ones that can hold the best-ranked rows come first —
/// by `max_ts` descending for `ORDER BY _timestamp DESC`, by `min_ts`
/// ascending for ASC — then chunked into geometrically growing waves
/// (doubling from [`FIRST_WAVE_SIZE`] up to `target_partitions`). Waves
/// execute strictly in order and the pruner's early stop fires after the
/// first wave whose candidates already outrank every remaining file.
///
/// Overlapping file ranges only weaken the prune bound (the suffix fold in
/// `group_suffix_bounds` stays correct); the former time-partition
/// transpose instead collapsed to ONE group whenever files overlapped —
/// `file_groups: 1` with no early stop, so every file in the window was
/// evaluated for a query one file could satisfy (#27).
fn best_first_waves(
    mut files: Vec<FileKey>,
    ascend: bool,
    target_partitions: usize,
) -> Vec<Vec<FileKey>> {
    if files.is_empty() {
        return Vec::new();
    }
    if ascend {
        files.sort_unstable_by_key(|f| (f.meta.min_ts, f.meta.max_ts));
    } else {
        files.sort_unstable_by_key(|f| {
            (
                std::cmp::Reverse(f.meta.max_ts),
                std::cmp::Reverse(f.meta.min_ts),
            )
        });
    }
    let cap = target_partitions.max(FIRST_WAVE_SIZE);
    let mut waves: Vec<Vec<FileKey>> = Vec::new();
    let mut wave_size = FIRST_WAVE_SIZE.min(cap);
    let mut iter = files.into_iter();
    loop {
        let wave: Vec<FileKey> = iter.by_ref().take(wave_size).collect();
        if wave.is_empty() {
            break;
        }
        waves.push(wave);
        wave_size = (wave_size * 2).min(cap);
    }
    waves
}

#[cfg(test)]
mod tests {
    use config::meta::stream::FileMeta;

    use super::*;

    fn create_file_key(min_ts: i64, max_ts: i64) -> FileKey {
        FileKey {
            key: format!("file_{min_ts}_{max_ts}"),
            meta: FileMeta {
                min_ts,
                max_ts,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn keys(groups: &[Vec<FileKey>]) -> Vec<Vec<String>> {
        groups
            .iter()
            .map(|group| group.iter().map(|file| file.key.clone()).collect())
            .collect()
    }

    #[test]
    fn desc_waves_are_newest_first_and_double() {
        let files: Vec<FileKey> = (0..20)
            .map(|i| create_file_key(i * 10 + 1, i * 10 + 10))
            .collect();
        let groups = partition_vix_files(
            files.clone(),
            &Some(IndexOptimizeMode::SimpleSelect(10, false)),
            8,
        );
        // sizes: 4, then 8 (doubled, capped at target_partitions), then 8
        assert_eq!(
            groups.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![4, 8, 8]
        );
        // wave 0 holds the four newest files, newest first
        assert_eq!(
            keys(&groups)[0],
            vec![
                "file_191_200",
                "file_181_190",
                "file_171_180",
                "file_161_170"
            ]
        );
        // nothing lost or duplicated
        assert_eq!(groups.iter().map(Vec::len).sum::<usize>(), files.len());
        // globally non-increasing max_ts across the flattened wave order
        let flat: Vec<i64> = groups.iter().flatten().map(|f| f.meta.max_ts).collect();
        assert!(flat.windows(2).all(|w| w[0] >= w[1]));
    }

    /// The #27 regression: OVERLAPPING files must still produce multiple
    /// waves (the old time-partition transpose collapsed them into one
    /// group, so the early stop could never fire).
    #[test]
    fn desc_waves_survive_overlapping_files() {
        let files: Vec<FileKey> = (0..12)
            .map(|i| create_file_key(i, 100 + i)) // heavily overlapping
            .collect();
        let groups =
            partition_vix_files(files, &Some(IndexOptimizeMode::SimpleSelect(10, false)), 4);
        assert_eq!(
            groups.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![4, 4, 4]
        );
        // newest max_ts first even with overlap
        assert_eq!(groups[0][0].key, "file_11_111");
    }

    #[test]
    fn asc_waves_are_oldest_first() {
        let files: Vec<FileKey> = (0..6)
            .map(|i| create_file_key(i * 10 + 1, i * 10 + 10))
            .collect();
        let groups =
            partition_vix_files(files, &Some(IndexOptimizeMode::SimpleSelect(10, true)), 4);
        assert_eq!(groups.iter().map(Vec::len).collect::<Vec<_>>(), vec![4, 2]);
        assert_eq!(
            keys(&groups)[0],
            vec!["file_1_10", "file_11_20", "file_21_30", "file_31_40"]
        );
        let flat: Vec<i64> = groups.iter().flatten().map(|f| f.meta.min_ts).collect();
        assert!(flat.windows(2).all(|w| w[0] <= w[1]));
    }

    #[test]
    fn small_target_partitions_never_shrinks_below_first_wave() {
        let files: Vec<FileKey> = (0..10)
            .map(|i| create_file_key(i * 10 + 1, i * 10 + 10))
            .collect();
        let groups =
            partition_vix_files(files, &Some(IndexOptimizeMode::SimpleSelect(10, false)), 1);
        // cap = max(target_partitions, FIRST_WAVE_SIZE) = 4
        assert_eq!(
            groups.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![4, 4, 2]
        );
    }

    /// Non-select modes fan all files into ONE group: the eval-concurrency
    /// semaphore bounds parallelism, and no barrier waits on a slow file.
    #[test]
    fn test_partition_vix_files_non_select_single_group() {
        let files: Vec<FileKey> = (0..20)
            .map(|i| create_file_key(i * 10 + 1, i * 10 + 10))
            .collect();
        for mode in [
            None,
            Some(IndexOptimizeMode::SimpleCount),
            Some(IndexOptimizeMode::SimpleHistogram(0, 10, 5, 0)),
            Some(IndexOptimizeMode::SimpleTopN(
                vec!["f".to_string()],
                10,
                false,
            )),
            // a zero-limit select cannot prune either
            Some(IndexOptimizeMode::SimpleSelect(0, false)),
        ] {
            let groups = partition_vix_files(files.clone(), &mode, 4);
            assert_eq!(groups.len(), 1, "mode {mode:?}");
            assert_eq!(groups[0].len(), 20, "mode {mode:?}");
        }
        assert!(partition_vix_files(Vec::new(), &None, 4).is_empty());
        // SimpleSelect keeps its multi-wave pruning layout
        let groups = partition_vix_files(
            files.clone(),
            &Some(IndexOptimizeMode::SimpleSelect(10, false)),
            4,
        );
        assert!(groups.len() > 1);
        assert_eq!(groups.iter().map(Vec::len).sum::<usize>(), files.len());
    }

    #[test]
    fn empty_input_is_empty_for_select_too() {
        let groups = partition_vix_files(
            Vec::new(),
            &Some(IndexOptimizeMode::SimpleSelect(10, false)),
            4,
        );
        assert!(groups.is_empty());
    }
}
