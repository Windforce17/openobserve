// Copyright 2026 OpenObserve Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
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

/// Order the vix index files for the evaluation driver.
///
/// For the timestamp-ordered LIMIT shape (`SimpleSelect`) the files are
/// sorted so the ones that can hold the best-ranked rows come first — by
/// `max_ts` descending for `ORDER BY _timestamp DESC`, by `min_ts`
/// ascending for ASC — so the pruner's early stop can drop every file the
/// window has not answered once the limit is satisfied. Every other mode
/// needs all files anyway and keeps the caller's deterministic order.
pub(super) fn order_vix_files(
    mut index_parquet_files: Vec<FileKey>,
    idx_optimize_mode: &Option<IndexOptimizeMode>,
) -> Vec<FileKey> {
    if let Some(IndexOptimizeMode::SimpleSelect(limit, ascend)) = idx_optimize_mode
        && *limit > 0
    {
        if *ascend {
            index_parquet_files.sort_unstable_by_key(|f| (f.meta.min_ts, f.meta.max_ts));
        } else {
            index_parquet_files.sort_unstable_by_key(|f| {
                (
                    std::cmp::Reverse(f.meta.max_ts),
                    std::cmp::Reverse(f.meta.min_ts),
                )
            });
        }
    }
    index_parquet_files
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
    fn desc_select_is_newest_first() {
        let files: Vec<FileKey> = (0..200)
            .map(|i| create_file_key(i * 10 + 1, i * 10 + 10))
            .collect();
        let ordered = order_vix_files(files, &Some(IndexOptimizeMode::SimpleSelect(10, false)));
        assert_eq!(ordered.first().unwrap().key, "file_1991_2000");
        assert_eq!(ordered.last().unwrap().key, "file_1_10");
        let flat: Vec<i64> = ordered.iter().map(|f| f.meta.max_ts).collect();
        assert!(flat.windows(2).all(|w| w[0] >= w[1]));
        assert_eq!(ordered.len(), 200);
    }

    /// The #27 regression: OVERLAPPING files must still be ordered (the
    /// old time-partition transpose collapsed them into one group, so the
    /// early stop could never fire).
    #[test]
    fn desc_select_survives_overlapping_files() {
        let files: Vec<FileKey> = (0..192).map(|i| create_file_key(i, 100 + i)).collect();
        let ordered = order_vix_files(files, &Some(IndexOptimizeMode::SimpleSelect(10, false)));
        assert_eq!(ordered.len(), 192);
        assert_eq!(ordered.first().unwrap().key, "file_191_291");
        let flat: Vec<i64> = ordered.iter().map(|f| f.meta.max_ts).collect();
        assert!(flat.windows(2).all(|w| w[0] >= w[1]));
    }

    #[test]
    fn asc_select_is_oldest_first() {
        let files: Vec<FileKey> = (0..70)
            .map(|i| create_file_key(i * 10 + 1, i * 10 + 10))
            .collect();
        let ordered = order_vix_files(files, &Some(IndexOptimizeMode::SimpleSelect(10, true)));
        assert_eq!(ordered.first().unwrap().key, "file_1_10");
        assert_eq!(ordered.last().unwrap().key, "file_691_700");
        let flat: Vec<i64> = ordered.iter().map(|f| f.meta.min_ts).collect();
        assert!(flat.windows(2).all(|w| w[0] <= w[1]));
    }

    /// Non-select modes (and a zero-limit select) cannot prune, so they
    /// keep the caller's input order — the deterministic hash order the
    /// skip-rate bail-out samples.
    #[test]
    fn non_select_modes_keep_the_input_order() {
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
            let ordered = order_vix_files(files.clone(), &mode);
            assert_eq!(
                ordered.iter().map(|f| f.key.as_str()).collect::<Vec<_>>(),
                files.iter().map(|f| f.key.as_str()).collect::<Vec<_>>(),
                "mode {mode:?}"
            );
        }
        assert!(order_vix_files(Vec::new(), &None).is_empty());
        assert!(
            order_vix_files(
                Vec::new(),
                &Some(IndexOptimizeMode::SimpleSelect(10, false))
            )
            .is_empty()
        );
    }
}
