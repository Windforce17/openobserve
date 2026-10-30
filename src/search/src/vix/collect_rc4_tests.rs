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

use std::sync::Arc;

use arrow::array::UInt64Array;

use super::*;

fn limits(groups: usize) -> DictMultiHistogramLimits {
    DictMultiHistogramLimits {
        max_groups: groups,
        max_values: groups,
        ..DEFAULT_DICT_MULTI_HISTOGRAM_LIMITS
    }
}

fn chunk(codes: Vec<Option<u64>>, values: Vec<Option<&str>>) -> vortex_index::DocsDictBatch {
    vortex_index::DocsDictBatch {
        row_offset: 0,
        timestamps: None,
        codes: UInt64Array::from(codes),
        values: Arc::new(StringArray::from(values)),
    }
}

#[test]
fn rc4_selected_dictionary_budget_preserves_empty_and_null() {
    // Four dictionary entries, but only two selected SQL groups. A NULL code
    // and a NULL dictionary cell refer to the same SQL group, not to "".
    let input = || {
        chunk(
            vec![Some(0), Some(1), None, Some(2)],
            vec![Some("unused"), Some(""), None, Some("also unused")],
        )
    };
    let selected = BooleanBuffer::from_iter([false, true, true, true]);
    let mut counts = HashMap::new();
    let mut bytes = 0;
    fold_top_n_chunk(
        &mut counts,
        &mut bytes,
        limits(2),
        input(),
        &selected,
        None,
        true,
        true,
    )
    .unwrap();
    assert_eq!(
        counts,
        HashMap::from_iter([(Some(String::new()), 1), (None, 2)])
    );
    assert_eq!(bytes, 0);
    // Rollback restores the whole-dictionary rejection, not approximate output.
    assert!(
        fold_top_n_chunk(
            &mut HashMap::new(),
            &mut 0,
            limits(2),
            input(),
            &selected,
            None,
            true,
            false
        )
        .is_err()
    );
    assert!(
        fold_top_n_chunk(
            &mut HashMap::new(),
            &mut 0,
            limits(4),
            input(),
            &selected,
            None,
            false,
            true
        )
        .is_err()
    );
}

#[test]
fn rc4_null_complement_requires_valid_counts_and_its_own_budget() {
    let counts = || vec![(String::new(), 2), ("api".to_owned(), 3)];
    assert_eq!(
        nullable_dictionary_groups(counts(), 7, limits(3), true).unwrap(),
        vec![
            (vec![Some(String::new())], 2),
            (vec![Some("api".into())], 3),
            (vec![None], 2)
        ]
    );
    assert!(nullable_dictionary_groups(counts(), 7, limits(2), true).is_err());
    assert!(nullable_dictionary_groups(counts(), 7, limits(3), false).is_err());
    assert!(nullable_dictionary_groups(counts(), 4, limits(3), true).is_err());
    assert!(
        nullable_dictionary_groups(
            vec![("a".into(), u64::MAX), ("b".into(), 1)],
            u64::MAX,
            limits(3),
            true
        )
        .is_err()
    );
}

#[test]
fn rc4_selected_dictionary_rejects_corruption_and_owned_byte_overflow() {
    let selected = BooleanBuffer::new_set(1);
    for (codes, rows) in [
        (vec![Some(2)], None),
        (vec![Some(0)], Some(vec![2])),
        (vec![Some(0)], Some(vec![])),
    ] {
        assert!(
            fold_top_n_chunk(
                &mut HashMap::new(),
                &mut 0,
                limits(2),
                chunk(codes, vec![Some("a")]),
                &selected,
                rows.as_deref(),
                true,
                true
            )
            .is_err()
        );
    }
    let mut budget = limits(2);
    budget.max_value_bytes = 3;
    assert!(
        fold_top_n_chunk(
            &mut HashMap::new(),
            &mut 0,
            budget,
            chunk(vec![Some(0)], vec![Some("long")]),
            &selected,
            None,
            true,
            true
        )
        .is_err()
    );
    // A large unreferenced key must not consume the selected-key byte budget.
    let mut counts = HashMap::new();
    fold_top_n_chunk(
        &mut counts,
        &mut 0,
        budget,
        chunk(vec![Some(1)], vec![Some("unused long string"), Some("ok")]),
        &selected,
        None,
        true,
        true,
    )
    .unwrap();
    assert_eq!(counts.get(&Some("ok".into())), Some(&1));
}

#[test]
fn rc4_group_memory_at_8192_and_16384_is_bounded() {
    for cap in [8192, 16384] {
        let values: Vec<_> = (0..cap).map(|i| format!("operation-{i:016}")).collect();
        let make_chunk = || vortex_index::DocsDictBatch {
            row_offset: 0,
            timestamps: None,
            codes: UInt64Array::from_iter_values(0..cap as u64),
            values: Arc::new(StringArray::from(
                values.iter().map(String::as_str).collect::<Vec<_>>(),
            )),
        };
        let mut counts = HashMap::new();
        let mut key_bytes = 0;
        let selected = BooleanBuffer::new_set(cap);
        fold_top_n_chunk(
            &mut counts,
            &mut key_bytes,
            limits(cap),
            make_chunk(),
            &selected,
            None,
            true,
            true,
        )
        .unwrap();
        assert_eq!(counts.len(), cap);
        assert_eq!(counts.values().sum::<u64>(), cap as u64);
        let owned =
            counts.allocation_size() + counts.keys().flatten().map(String::capacity).sum::<usize>();
        // Includes actual allocated hash buckets and owned string capacities.
        // The temporary code map has at most cap entries and is dropped first.
        assert!(owned <= cap * 128, "{cap}: allocated {owned}");
        assert!(key_bytes <= limits(cap).max_value_bytes);
        eprintln!(
            "rc4 groups={cap} owned_map_bytes={owned} key_bytes={key_bytes} reader_memory_excluded=true"
        );
        let result = fold_top_n_chunk(
            &mut counts,
            &mut key_bytes,
            limits(cap),
            chunk(vec![Some(0)], vec![Some("one additional group")]),
            &BooleanBuffer::new_set(1),
            None,
            true,
            true,
        );
        assert!(result.is_err());
        assert_eq!(
            counts.len(),
            cap,
            "refusal must not allocate an extra group"
        );
    }
}

#[test]
fn rc4_chunk_cancel_stops_before_allocating_groups() {
    let operation = super::super::source::ReadOperation::new(
        Arc::new(super::super::source::FetchStats::default()),
        None,
    );
    operation.cancel();
    let mut counts = HashMap::new();
    let error = operation
        .run(|| {
            fold_top_n_chunk(
                &mut counts,
                &mut 0,
                limits(2),
                chunk(vec![Some(0)], vec![Some("api")]),
                &BooleanBuffer::new_set(1),
                None,
                true,
                true,
            )
        })
        .unwrap_err();
    assert!(super::super::is_cancelled_read(&error));
    assert!(counts.is_empty());
}

#[test]
fn rc4_selected_dictionary_supports_string_families_and_point_duplicates() {
    let dictionaries: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![Some("unused"), Some(""), None])),
        Arc::new(arrow::array::LargeStringArray::from(vec![
            Some("unused"),
            Some(""),
            None,
        ])),
        Arc::new(arrow::array::StringViewArray::from(vec![
            Some("unused"),
            Some(""),
            None,
        ])),
    ];
    for values in dictionaries {
        let mut counts = HashMap::new();
        fold_top_n_chunk(
            &mut counts,
            &mut 0,
            limits(2),
            vortex_index::DocsDictBatch {
                row_offset: 0,
                timestamps: None,
                codes: UInt64Array::from(vec![Some(0), Some(1), Some(2)]),
                values,
            },
            &BooleanBuffer::from_iter([false, true, true]),
            None,
            true,
            true,
        )
        .unwrap();
        assert_eq!(
            counts,
            HashMap::from_iter([(Some(String::new()), 1), (None, 1)])
        );
    }
    let mut counts = HashMap::new();
    fold_top_n_chunk(
        &mut counts,
        &mut 0,
        limits(1),
        chunk(vec![Some(0), Some(1), Some(2)], vec![Some("api"); 3]),
        &BooleanBuffer::new_set(3),
        Some(&[0, 1, 2]),
        true,
        true,
    )
    .unwrap();
    assert_eq!(counts, HashMap::from_iter([(Some("api".into()), 3)]));
}

#[test]
fn rc4_histogram_budget_counts_only_selected_values() {
    let mut counts = GroupCounts::new(limits(1));
    fold_dict_chunk(
        &mut counts,
        chunk(
            vec![Some(0), Some(1), Some(2)],
            vec![Some("unused"), Some("api"), Some("unused too")],
        ),
        &BooleanBuffer::from_iter([false, true, false]),
        None,
        None,
    )
    .unwrap();
    assert_eq!(counts.finish(), vec![(0, "api".into(), 1)]);
    let mut counts = GroupCounts::new(limits(1));
    fold_dict_chunk(
        &mut counts,
        chunk(vec![Some(0), Some(1), Some(2)], vec![Some("api"); 3]),
        &BooleanBuffer::new_set(3),
        Some(&[0, 1, 2]),
        None,
    )
    .unwrap();
    assert_eq!(counts.finish(), vec![(0, "api".into(), 3)]);
}
