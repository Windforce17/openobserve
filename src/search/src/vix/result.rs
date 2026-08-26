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

//! Per-file and merged results of a vix index search.
//!
//! Besides row-id bitmaps for regular filtering, the `.vix` read path
//! executes the aggregation fast paths ([`IndexOptimizeMode`]): count,
//! select (top-N by `_timestamp`), histogram, multi-histogram, top-n and
//! distinct — all evaluated over the docs-blob columns; this module owns
//! the cross-file merge semantics of those per-file results.

use std::{collections::HashSet, fmt::Display, sync::Arc};

use config::meta::{
    inverted_index::IndexOptimizeMode,
    stream::{FileKey, RowIdBitmap},
};
use hashbrown::HashMap;

use super::pruner::SimpleSelectPruner;

/// The result of searching one `.vix` file.
#[derive(Debug, Clone)]
pub enum VixSearchResult {
    RowIdsSelection {
        /// matched row ids, as a compressed bitmap over the file's rows
        row_ids: Arc<RowIdBitmap>,
        /// parquet row-group size recorded when the index was built
        row_group_size: Option<u32>,
    },
    /// simple select: the file's top-`limit` matches as `(_timestamp, doc_id)`
    /// pairs, timestamp-ordered best-first
    SelectCandidates {
        candidates: Arc<Vec<(i64, u32)>>,
        row_group_size: Option<u32>,
    },
    /// the file should be excluded without building a bitmap
    NoMatch,
    /// index search skipped for this file, with the matched percentage;
    /// the caller adds the filter back and scans the file
    Skipped { percent: usize },
    /// simple count optimization
    Count(usize),
    /// simple histogram optimization (fixed bucket counts)
    Histogram(Vec<u64>),
    /// multi histogram optimization: `(bucket key, breakdown value, count)`
    MultiHistogram(Vec<(i64, String, u64)>),
    /// group-by top-n optimization (1..=4 group fields)
    TopN(Vec<(Vec<String>, u64)>),
    /// simple distinct optimization
    Distinct(HashSet<String>),
    /// M16 min/max(field) optimization: the file's extreme value of the
    /// column over the matched rows (`None` = no non-null value matched)
    MinMax(Option<MinMaxValue>),
}

/// M16: one numeric min/max value, typed by the column's stored family.
/// Cross-family folds (per-file type drift) compare EXACTLY (i128 for the
/// integer families, lossless-checked against f64). NaN never enters a fold
/// (both the stats builder and the decode folds skip it — JSON cannot carry
/// NaN, so stored NaNs are synthetic anyway).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MinMaxValue {
    I64(i64),
    U64(u64),
    F64(f64),
}

impl MinMaxValue {
    /// Exact cross-family ordering (no lossy i64→f64 rounding — the same
    /// rules as the chunk-stats comparators). NaN (never produced by the
    /// folds) compares as `None` and the fold keeps the previous value.
    pub(super) fn cmp_exact(&self, other: &Self) -> Option<std::cmp::Ordering> {
        use MinMaxValue::*;
        use vortex_index::cmp_i128_vs_f64;
        match (self, other) {
            (I64(a), I64(b)) => Some(a.cmp(b)),
            (U64(a), U64(b)) => Some(a.cmp(b)),
            (I64(a), U64(b)) => Some((*a as i128).cmp(&(*b as i128))),
            (U64(a), I64(b)) => Some((*a as i128).cmp(&(*b as i128))),
            (F64(a), F64(b)) => a.partial_cmp(b),
            (I64(a), F64(b)) => cmp_i128_vs_f64(*a as i128, *b),
            (U64(a), F64(b)) => cmp_i128_vs_f64(*a as i128, *b),
            (F64(a), I64(b)) => cmp_i128_vs_f64(*b as i128, *a).map(std::cmp::Ordering::reverse),
            (F64(a), U64(b)) => cmp_i128_vs_f64(*b as i128, *a).map(std::cmp::Ordering::reverse),
        }
    }

    /// Fold `other` into `self` under min/max semantics; an incomparable
    /// pair (NaN) keeps `self` — unreachable for the folds' NaN-free
    /// inputs, defensive otherwise.
    pub fn fold(self, other: MinMaxValue, is_max: bool) -> MinMaxValue {
        match self.cmp_exact(&other) {
            Some(std::cmp::Ordering::Less) if is_max => other,
            Some(std::cmp::Ordering::Greater) if !is_max => other,
            _ => self,
        }
    }
}

impl VixSearchResult {
    // used for the skip-threshold accounting in vix_search
    pub fn percent(&self) -> usize {
        match self {
            Self::Skipped { percent } => *percent,
            _ => 0,
        }
    }

    pub fn get_memory_size(&self) -> usize {
        match self {
            Self::RowIdsSelection { row_ids, .. } => row_ids.memory_size(),
            Self::SelectCandidates { candidates, .. } => {
                candidates.capacity() * std::mem::size_of::<(i64, u32)>()
                    + std::mem::size_of::<Vec<(i64, u32)>>()
            }
            Self::NoMatch => 0,
            Self::Skipped { .. } => std::mem::size_of::<usize>(),
            Self::Count(_) => std::mem::size_of::<usize>(),
            Self::Histogram(histogram) => {
                histogram.capacity() * std::mem::size_of::<u64>() + std::mem::size_of::<Vec<u64>>()
            }
            Self::MultiHistogram(multi_histogram) => {
                multi_histogram
                    .iter()
                    .map(|(_, s, _)| {
                        s.capacity() + std::mem::size_of::<i64>() + std::mem::size_of::<u64>()
                    })
                    .sum::<usize>()
                    + std::mem::size_of::<Vec<(i64, String, u64)>>()
            }
            Self::TopN(top_n) => {
                top_n
                    .iter()
                    .map(|(keys, _)| {
                        keys.iter().map(|s| s.capacity()).sum::<usize>()
                            + std::mem::size_of::<Vec<String>>()
                            + std::mem::size_of::<u64>()
                    })
                    .sum::<usize>()
                    + std::mem::size_of::<Vec<(Vec<String>, u64)>>()
            }
            Self::Distinct(distinct) => {
                distinct.iter().map(|s| s.capacity()).sum::<usize>()
                    + std::mem::size_of::<HashSet<String>>()
            }
            Self::MinMax(_) => std::mem::size_of::<Option<MinMaxValue>>(),
        }
    }
}

/// Accumulates per-file results into a [`MultiResult`] (bucket-wise
/// histogram sums, top-n/distinct unions, global select candidates).
pub enum MultiResultBuilder {
    RowNums(u64),
    Count(u64),
    SimpleSelect {
        num_rows: u64,
        pruner: SimpleSelectPruner,
    },
    Histogram(Vec<Vec<u64>>),
    MultiHistogram(Vec<Vec<(i64, String, u64)>>),
    TopN(Vec<(Vec<String>, u64)>),
    Distinct(HashSet<String>),
    MinMax {
        is_max: bool,
        value: Option<MinMaxValue>,
    },
}

impl MultiResultBuilder {
    pub fn new(optimize_rule: &Option<IndexOptimizeMode>, file_groups: &[Vec<FileKey>]) -> Self {
        match optimize_rule {
            Some(IndexOptimizeMode::SimpleHistogram(..)) => Self::Histogram(vec![]),
            Some(IndexOptimizeMode::SimpleMultiHistogram(..)) => Self::MultiHistogram(vec![]),
            Some(IndexOptimizeMode::SimpleTopN(..)) => Self::TopN(vec![]),
            Some(IndexOptimizeMode::SimpleDistinct(..)) => Self::Distinct(HashSet::new()),
            Some(IndexOptimizeMode::SimpleSelect(limit, ascend)) => Self::SimpleSelect {
                num_rows: 0,
                pruner: SimpleSelectPruner::new(*limit, *ascend, file_groups),
            },
            Some(IndexOptimizeMode::SimpleCount) => Self::Count(0),
            // M16: count(field) merges exactly like count(*) — a per-file sum
            Some(IndexOptimizeMode::SimpleCountField(_)) => Self::Count(0),
            Some(IndexOptimizeMode::SimpleMinMax(_, is_max)) => Self::MinMax {
                is_max: *is_max,
                value: None,
            },
            None => Self::RowNums(0),
        }
    }

    pub fn add_row_nums(&mut self, row_nums: u64) {
        match self {
            Self::RowNums(a) => *a += row_nums,
            // SimpleSelect maybe falls back to row-id collection
            Self::SimpleSelect { num_rows, .. } => *num_rows += row_nums,
            _ => unreachable!("unsupported vix multi result"),
        }
    }

    // simple count
    pub fn add_count(&mut self, count: u64) {
        match self {
            Self::Count(a) => *a += count,
            _ => unreachable!("unsupported vix multi result"),
        }
    }

    // simple select
    pub fn add_select_candidates(
        &mut self,
        file_name: String,
        candidates: Arc<Vec<(i64, u32)>>,
        row_group_size: Option<u32>,
    ) {
        match self {
            Self::SimpleSelect { num_rows, pruner } => {
                *num_rows += candidates.len() as u64;
                pruner.record_candidates(file_name, candidates, row_group_size);
            }
            _ => unreachable!("unsupported vix multi result"),
        }
    }

    // simple histogram
    pub fn add_histogram(&mut self, histogram: Vec<u64>) {
        match self {
            Self::Histogram(a) => {
                if !histogram.is_empty() {
                    a.push(histogram);
                }
            }
            _ => unreachable!("unsupported vix multi result"),
        }
    }

    // simple multi histogram
    pub fn add_multi_histogram(&mut self, multi_histogram: Vec<(i64, String, u64)>) {
        match self {
            Self::MultiHistogram(a) => {
                if !multi_histogram.is_empty() {
                    a.push(multi_histogram);
                }
            }
            _ => unreachable!("unsupported vix multi result"),
        }
    }

    // simple top-n
    pub fn add_top_n(&mut self, top_n: Vec<(Vec<String>, u64)>) {
        match self {
            Self::TopN(a) => a.extend(top_n),
            _ => unreachable!("unsupported vix multi result"),
        }
    }

    // simple distinct
    pub fn add_distinct(&mut self, distinct: HashSet<String>) {
        match self {
            Self::Distinct(a) => a.extend(distinct),
            _ => unreachable!("unsupported vix multi result"),
        }
    }

    // M16 min/max(field): fold one file's extreme into the running one
    pub fn add_min_max(&mut self, file_value: Option<MinMaxValue>) {
        match self {
            Self::MinMax { is_max, value } => {
                if let Some(new) = file_value {
                    *value = Some(match value.take() {
                        Some(current) => current.fold(new, *is_max),
                        None => new,
                    });
                }
            }
            _ => unreachable!("unsupported vix multi result"),
        }
    }

    /// Only for simple select: whether the groups after `group_id` can be
    /// dropped because the limit is already satisfied by earlier candidates.
    pub fn should_prune_remaining_groups(&self, trace_id: &str, group_id: usize) -> bool {
        match self {
            Self::SimpleSelect { pruner, .. } => {
                pruner.should_prune_remaining_groups(trace_id, group_id)
            }
            _ => false,
        }
    }

    /// Build the merged result; for SimpleSelect this also finalizes the file
    /// selections to the global top-N.
    pub fn build(
        self,
        trace_id: &str,
        file_list_map: &mut HashMap<String, FileKey>,
    ) -> MultiResult {
        match self {
            Self::RowNums(a) => MultiResult::RowNums(a),
            Self::Count(a) => MultiResult::Count(a),
            Self::SimpleSelect {
                num_rows,
                mut pruner,
            } => {
                pruner.finalize(trace_id, file_list_map);
                MultiResult::SimpleSelect(num_rows)
            }
            Self::Histogram(histograms_hits) => {
                if histograms_hits.is_empty() {
                    return MultiResult::Histogram(vec![]);
                }
                let len = histograms_hits[0].len();
                let histogram = (0..len)
                    .map(|i| {
                        histograms_hits
                            .iter()
                            .map(|v| v.get(i).unwrap_or(&0))
                            .sum::<u64>()
                    })
                    .collect();
                MultiResult::Histogram(histogram)
            }
            Self::MultiHistogram(results) => {
                // Merge: flatten all per-file results into a single Vec
                let merged: Vec<(i64, String, u64)> = results.into_iter().flatten().collect();
                MultiResult::MultiHistogram(merged)
            }
            Self::TopN(a) => MultiResult::TopN(a),
            Self::Distinct(a) => MultiResult::Distinct(a),
            Self::MinMax { value, .. } => MultiResult::MinMax(value),
        }
    }
}

/// The merged result of a vix search over a file list.
#[derive(Debug)]
pub enum MultiResult {
    RowNums(u64),
    Count(u64),
    SimpleSelect(u64),
    Histogram(Vec<u64>),
    MultiHistogram(Vec<(i64, String, u64)>),
    TopN(Vec<(Vec<String>, u64)>),
    Distinct(HashSet<String>),
    /// M16 min/max(field): the cross-file fold (`None` = no value matched)
    MinMax(Option<MinMaxValue>),
}

impl Display for MultiResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RowNums(num) => write!(f, "row_nums: {num}"),
            Self::Count(num) => write!(f, "count: {num}"),
            Self::SimpleSelect(num) => write!(f, "select row_nums: {num}"),
            Self::Histogram(histogram) => {
                write!(f, "histogram hits: {}", histogram.iter().sum::<u64>())
            }
            Self::MultiHistogram(multi_histogram) => {
                write!(f, "multi_histogram hits: {}", multi_histogram.len())
            }
            Self::TopN(top_n) => write!(f, "top_n hits: {}", top_n.len()),
            Self::Distinct(distinct) => write!(f, "distinct hits: {}", distinct.len()),
            Self::MinMax(value) => write!(f, "min_max: {value:?}"),
        }
    }
}

impl MultiResult {
    pub fn num_rows(&self) -> usize {
        match self {
            Self::Count(a) => *a as usize,
            _ => 0,
        }
    }

    pub fn histogram(self) -> Vec<u64> {
        match self {
            Self::Histogram(a) => a,
            _ => vec![],
        }
    }

    pub fn multi_histogram(self) -> Vec<(i64, String, u64)> {
        match self {
            Self::MultiHistogram(a) => a,
            _ => vec![],
        }
    }

    pub fn top_n(self) -> Vec<(Vec<String>, u64)> {
        match self {
            Self::TopN(a) => a,
            _ => vec![],
        }
    }

    pub fn distinct(self) -> HashSet<String> {
        match self {
            Self::Distinct(a) => a,
            _ => HashSet::new(),
        }
    }

    /// M16: the folded min/max value (`None` for other results or when no
    /// row matched).
    pub fn min_max(self) -> Option<MinMaxValue> {
        match self {
            Self::MinMax(a) => a,
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_multi_result_builder_new() {
        let builder = MultiResultBuilder::new(&Some(IndexOptimizeMode::SimpleCount), &[]);
        assert!(matches!(builder, MultiResultBuilder::Count(0)));

        let builder = MultiResultBuilder::new(&None, &[]);
        assert!(matches!(builder, MultiResultBuilder::RowNums(0)));

        let builder = MultiResultBuilder::new(
            &Some(IndexOptimizeMode::SimpleHistogram(0, 1000, 10, 0)),
            &[],
        );
        assert!(matches!(builder, MultiResultBuilder::Histogram(_)));

        let builder = MultiResultBuilder::new(
            &Some(IndexOptimizeMode::SimpleMultiHistogram(
                0,
                1000,
                10,
                0,
                "level".to_string(),
            )),
            &[],
        );
        assert!(matches!(builder, MultiResultBuilder::MultiHistogram(_)));

        let builder = MultiResultBuilder::new(
            &Some(IndexOptimizeMode::SimpleTopN(
                vec!["field".to_string()],
                10,
                true,
            )),
            &[],
        );
        assert!(matches!(builder, MultiResultBuilder::TopN(_)));

        let builder = MultiResultBuilder::new(
            &Some(IndexOptimizeMode::SimpleDistinct(
                "field".to_string(),
                10,
                true,
            )),
            &[],
        );
        assert!(matches!(builder, MultiResultBuilder::Distinct(_)));

        let builder =
            MultiResultBuilder::new(&Some(IndexOptimizeMode::SimpleSelect(10, true)), &[]);
        assert!(matches!(
            builder,
            MultiResultBuilder::SimpleSelect { num_rows: 0, .. }
        ));
    }

    #[test]
    fn test_multi_result_builder_accumulates() {
        let mut builder = MultiResultBuilder::new(&None, &[]);
        builder.add_row_nums(10);
        builder.add_row_nums(5);
        match builder.build("test", &mut HashMap::new()) {
            MultiResult::RowNums(v) => assert_eq!(v, 15),
            _ => panic!("expected RowNums"),
        }

        let mut builder = MultiResultBuilder::new(&Some(IndexOptimizeMode::SimpleCount), &[]);
        builder.add_count(10);
        builder.add_count(32);
        let result = builder.build("test", &mut HashMap::new());
        assert_eq!(result.num_rows(), 42);
        match result {
            MultiResult::Count(v) => assert_eq!(v, 42),
            _ => panic!("expected Count"),
        }
    }

    /// SimpleSelect finalization must drop files whose exact evaluation
    /// produced ZERO candidates: they contribute nothing to the global
    /// top-N and must leave the scan list. Files with winning candidates
    /// keep a narrowed selection; a candidate-less file (e.g. kept through
    /// the weaker-predicate bitmap path after a skipped condition) survives
    /// while its time range can still reach the winner set, and is dropped
    /// only when it sorts entirely after the weakest winner.
    #[test]
    fn test_simple_select_build_drops_zero_candidate_files() {
        use config::meta::stream::FileMeta;

        let file = |key: &str, min_ts: i64, max_ts: i64| FileKey {
            key: key.to_string(),
            meta: FileMeta {
                min_ts,
                max_ts,
                records: 100,
                ..Default::default()
            },
            ..Default::default()
        };
        let mut files: HashMap<String, FileKey> = [
            file("with_hits", 90, 100),
            file("no_hits", 90, 100),
            file("overlaps_winners", 40, 99),
            file("sorts_after_winners", 10, 50),
        ]
        .into_iter()
        .map(|f| (f.key.clone(), f))
        .collect();

        let mut builder =
            MultiResultBuilder::new(&Some(IndexOptimizeMode::SimpleSelect(2, false)), &[]);
        builder.add_select_candidates(
            "with_hits".to_string(),
            Arc::new(vec![(100, 0), (99, 1)]),
            None,
        );
        // exact evaluation with zero matches (e.g. an absent-field condition)
        builder.add_select_candidates("no_hits".to_string(), Arc::new(vec![]), None);

        match builder.build("test", &mut files) {
            MultiResult::SimpleSelect(num_rows) => assert_eq!(num_rows, 2),
            _ => panic!("expected SimpleSelect"),
        }

        // zero-candidate file: removed from the scan list
        assert!(!files.contains_key("no_hits"));
        // winner file: kept, narrowed to its winning rows
        assert!(files.contains_key("with_hits"));
        // candidate-less file overlapping the weakest winner (ts 99): kept
        assert!(files.contains_key("overlaps_winners"));
        // candidate-less file sorting entirely after the weakest winner:
        // it cannot contribute a top-N row, dropped
        assert!(!files.contains_key("sorts_after_winners"));
        assert_eq!(files.len(), 2);
    }

    #[test]
    fn test_simple_select_builder_accepts_row_nums() {
        // the skipped-conditions fallback reports row nums for SimpleSelect
        let mut builder =
            MultiResultBuilder::new(&Some(IndexOptimizeMode::SimpleSelect(10, true)), &[]);
        builder.add_row_nums(5);
        // ascending: candidates are timestamp-ordered best-first (ts asc)
        builder.add_select_candidates("f1".to_string(), Arc::new(vec![(8, 1), (9, 0)]), None);
        match builder {
            MultiResultBuilder::SimpleSelect { num_rows, .. } => assert_eq!(num_rows, 7),
            _ => panic!("expected SimpleSelect"),
        }
    }

    #[test]
    fn test_histogram_merge_sums_buckets() {
        let mut builder =
            MultiResultBuilder::new(&Some(IndexOptimizeMode::SimpleHistogram(0, 10, 3, 0)), &[]);
        builder.add_histogram(vec![10, 20, 30]);
        builder.add_histogram(vec![5, 15, 25]);
        // empty per-file histograms are dropped
        builder.add_histogram(vec![]);
        match builder.build("test", &mut HashMap::new()) {
            MultiResult::Histogram(hist) => assert_eq!(hist, vec![15, 35, 55]),
            _ => panic!("expected Histogram"),
        }
    }

    #[test]
    fn test_histogram_merge_uses_first_length() {
        // mismatched lengths: the first histogram's length wins (old behavior)
        let mut builder =
            MultiResultBuilder::new(&Some(IndexOptimizeMode::SimpleHistogram(0, 10, 2, 0)), &[]);
        builder.add_histogram(vec![10, 20]);
        builder.add_histogram(vec![5, 15, 25]);
        match builder.build("test", &mut HashMap::new()) {
            MultiResult::Histogram(hist) => assert_eq!(hist, vec![15, 35]),
            _ => panic!("expected Histogram"),
        }
    }

    #[test]
    fn test_multi_histogram_merge_flattens() {
        let mut builder = MultiResultBuilder::new(
            &Some(IndexOptimizeMode::SimpleMultiHistogram(
                0,
                100,
                10,
                0,
                "level".to_string(),
            )),
            &[],
        );
        builder.add_multi_histogram(vec![(0, "a".to_string(), 2)]);
        builder.add_multi_histogram(vec![(0, "a".to_string(), 1), (10, "b".to_string(), 3)]);
        builder.add_multi_histogram(vec![]);
        match builder.build("test", &mut HashMap::new()) {
            MultiResult::MultiHistogram(rows) => {
                assert_eq!(
                    rows,
                    vec![
                        (0, "a".to_string(), 2),
                        (0, "a".to_string(), 1),
                        (10, "b".to_string(), 3),
                    ]
                );
            }
            _ => panic!("expected MultiHistogram"),
        }
    }

    #[test]
    fn test_top_n_merge_concatenates() {
        let mut builder = MultiResultBuilder::new(
            &Some(IndexOptimizeMode::SimpleTopN(
                vec!["f".to_string()],
                10,
                false,
            )),
            &[],
        );
        builder.add_top_n(vec![(vec!["a".to_string()], 5)]);
        builder.add_top_n(vec![(vec!["a".to_string()], 2), (vec!["b".to_string()], 1)]);
        match builder.build("test", &mut HashMap::new()) {
            MultiResult::TopN(rows) => {
                assert_eq!(rows.len(), 3);
                assert_eq!(rows[0], (vec!["a".to_string()], 5));
            }
            _ => panic!("expected TopN"),
        }
    }

    #[test]
    fn test_distinct_merge_unions() {
        let mut builder = MultiResultBuilder::new(
            &Some(IndexOptimizeMode::SimpleDistinct("f".to_string(), 10, true)),
            &[],
        );
        builder.add_distinct(HashSet::from(["a".to_string(), "b".to_string()]));
        builder.add_distinct(HashSet::from(["b".to_string(), "c".to_string()]));
        match builder.build("test", &mut HashMap::new()) {
            MultiResult::Distinct(values) => {
                assert_eq!(values.len(), 3);
                assert!(values.contains("a") && values.contains("b") && values.contains("c"));
            }
            _ => panic!("expected Distinct"),
        }
    }

    #[test]
    fn test_multi_result_accessors() {
        assert_eq!(MultiResult::Count(123).num_rows(), 123);
        assert_eq!(MultiResult::RowNums(123).num_rows(), 0);
        assert_eq!(MultiResult::SimpleSelect(123).num_rows(), 0);
        assert_eq!(MultiResult::Histogram(vec![1, 2]).histogram(), vec![1, 2]);
        assert!(MultiResult::RowNums(1).histogram().is_empty());
        assert_eq!(
            MultiResult::MultiHistogram(vec![(1, "a".to_string(), 2)]).multi_histogram(),
            vec![(1, "a".to_string(), 2)]
        );
        assert_eq!(
            MultiResult::TopN(vec![(vec!["a".to_string()], 2)]).top_n(),
            vec![(vec!["a".to_string()], 2)]
        );
        assert_eq!(
            MultiResult::Distinct(HashSet::from(["v".to_string()])).distinct(),
            HashSet::from(["v".to_string()])
        );
        assert!(MultiResult::RowNums(1).distinct().is_empty());
    }

    #[test]
    fn test_multi_result_display() {
        assert_eq!(format!("{}", MultiResult::RowNums(7)), "row_nums: 7");
        assert_eq!(format!("{}", MultiResult::Count(7)), "count: 7");
        assert_eq!(
            format!("{}", MultiResult::SimpleSelect(7)),
            "select row_nums: 7"
        );
        assert_eq!(
            format!("{}", MultiResult::Histogram(vec![10, 20, 30])),
            "histogram hits: 60"
        );
        assert_eq!(
            format!(
                "{}",
                MultiResult::MultiHistogram(vec![(1, "a".to_string(), 2)])
            ),
            "multi_histogram hits: 1"
        );
        assert_eq!(
            format!("{}", MultiResult::TopN(vec![(vec!["a".to_string()], 1)])),
            "top_n hits: 1"
        );
        assert_eq!(
            format!(
                "{}",
                MultiResult::Distinct(HashSet::from(["v".to_string()]))
            ),
            "distinct hits: 1"
        );
    }

    #[test]
    fn test_vix_search_result_percent_and_memory() {
        assert_eq!(VixSearchResult::Skipped { percent: 75 }.percent(), 75);
        assert_eq!(VixSearchResult::Count(100).percent(), 0);
        assert_eq!(VixSearchResult::NoMatch.get_memory_size(), 0);

        let result = VixSearchResult::RowIdsSelection {
            row_ids: Arc::new(RowIdBitmap::from_row_ids(0, std::iter::empty())),
            row_group_size: None,
        };
        // an empty selection costs only the fixed struct + serialized header
        assert!(result.get_memory_size() < 128);

        // the sparse invariant this migration exists for: a needle selection
        // over a large file must not cost anything near num_rows / 8
        let result = VixSearchResult::RowIdsSelection {
            row_ids: Arc::new(RowIdBitmap::from_row_ids(4_000_000, [3u32, 1_234_567])),
            row_group_size: None,
        };
        assert!(result.get_memory_size() < 256);

        let result = VixSearchResult::SelectCandidates {
            candidates: Arc::new(vec![(100i64, 1u32), (99, 2)]),
            row_group_size: None,
        };
        assert!(result.get_memory_size() >= 2 * std::mem::size_of::<(i64, u32)>());

        assert!(
            VixSearchResult::Histogram(vec![1, 2, 3]).get_memory_size()
                >= std::mem::size_of::<Vec<u64>>()
        );
        assert!(
            VixSearchResult::MultiHistogram(vec![(1, "aa".to_string(), 2)]).get_memory_size() > 0
        );
        assert!(
            VixSearchResult::TopN(vec![(vec!["aa".to_string()], 2)]).get_memory_size()
                >= std::mem::size_of::<Vec<(Vec<String>, u64)>>()
        );
        assert!(
            VixSearchResult::Distinct(HashSet::from(["aa".to_string()])).get_memory_size()
                >= std::mem::size_of::<HashSet<String>>()
        );
    }
}
