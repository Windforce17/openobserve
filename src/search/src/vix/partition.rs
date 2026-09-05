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

/// First wave size for [`best_first_waves`]. A 64-file wave lets deployments
/// configured for 64-way VIX evaluation issue cold sidecar range reads
/// immediately instead of serializing behind the former 4 → 8 → 16 ramp.
/// This deliberately trades extra speculative reads in the common
/// one-file-hit case for lower latency; the node-wide fetch gate still bounds
/// aggregate object-store pressure.
const FIRST_WAVE_SIZE: usize = 64;

/// Best-first waves for the timestamp-ordered LIMIT shape (`SimpleSelect`):
/// files sorted so the ones that can hold the best-ranked rows come first —
/// by `max_ts` descending for `ORDER BY _timestamp DESC`, by `min_ts`
/// ascending for ASC — then chunked into waves starting at
/// [`FIRST_WAVE_SIZE`] and growing up to
/// `max(target_partitions, FIRST_WAVE_SIZE)`. Waves execute strictly in order
/// and the pruner's early stop fires after the first wave
/// whose candidates already outrank every remaining file.
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

    #[test]
    fn desc_waves_start_at_64_and_double() {
        let files: Vec<FileKey> = (0..200)
            .map(|i| create_file_key(i * 10 + 1, i * 10 + 10))
            .collect();
        let groups = partition_vix_files(
            files.clone(),
            &Some(IndexOptimizeMode::SimpleSelect(10, false)),
            128,
        );
        assert_eq!(
            groups.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![64, 128, 8]
        );
        assert_eq!(groups[0].first().unwrap().key, "file_1991_2000");
        assert_eq!(groups[0].last().unwrap().key, "file_1361_1370");
        assert_eq!(groups.iter().map(Vec::len).sum::<usize>(), files.len());
        assert_eq!(
            groups
                .iter()
                .flatten()
                .map(|file| file.key.as_str())
                .collect::<Vec<_>>(),
            files
                .iter()
                .rev()
                .map(|file| file.key.as_str())
                .collect::<Vec<_>>(),
        );
        let flat: Vec<i64> = groups.iter().flatten().map(|f| f.meta.max_ts).collect();
        assert!(flat.windows(2).all(|w| w[0] >= w[1]));
    }

    /// The #27 regression: OVERLAPPING files must still produce multiple
    /// waves (the old time-partition transpose collapsed them into one
    /// group, so the early stop could never fire).
    #[test]
    fn desc_waves_survive_overlapping_files() {
        let files: Vec<FileKey> = (0..192).map(|i| create_file_key(i, 100 + i)).collect();
        let groups =
            partition_vix_files(files, &Some(IndexOptimizeMode::SimpleSelect(10, false)), 64);
        assert_eq!(
            groups.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![64, 64, 64]
        );
        assert_eq!(groups[0][0].key, "file_191_291");
    }

    #[test]
    fn asc_waves_are_oldest_first() {
        let files: Vec<FileKey> = (0..70)
            .map(|i| create_file_key(i * 10 + 1, i * 10 + 10))
            .collect();
        let groups =
            partition_vix_files(files, &Some(IndexOptimizeMode::SimpleSelect(10, true)), 64);
        assert_eq!(groups.iter().map(Vec::len).collect::<Vec<_>>(), vec![64, 6]);
        assert_eq!(groups[0].first().unwrap().key, "file_1_10");
        assert_eq!(groups[0].last().unwrap().key, "file_631_640");
        let flat: Vec<i64> = groups.iter().flatten().map(|f| f.meta.min_ts).collect();
        assert!(flat.windows(2).all(|w| w[0] <= w[1]));
    }

    #[test]
    fn small_target_partitions_never_shrinks_below_first_wave() {
        let files: Vec<FileKey> = (0..130)
            .map(|i| create_file_key(i * 10 + 1, i * 10 + 10))
            .collect();
        let groups =
            partition_vix_files(files, &Some(IndexOptimizeMode::SimpleSelect(10, false)), 1);
        assert_eq!(
            groups.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![64, 64, 2]
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
        // SimpleSelect keeps its multi-wave pruning layout once the input
        // exceeds the 64-file first wave.
        let select_files: Vec<FileKey> = (0..130)
            .map(|i| create_file_key(i * 10 + 1, i * 10 + 10))
            .collect();
        let groups = partition_vix_files(
            select_files.clone(),
            &Some(IndexOptimizeMode::SimpleSelect(10, false)),
            64,
        );
        assert!(groups.len() > 1);
        assert_eq!(
            groups.iter().map(Vec::len).sum::<usize>(),
            select_files.len()
        );
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
