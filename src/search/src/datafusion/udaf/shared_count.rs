// Copyright 2026 OpenObserve Inc.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! COUNT's wire-compatible grouped byte distinct implementation.
//!
//! The upstream adapter allocates a hash table per group, including empty groups.
//! Keep one table and one owned byte arena instead. Never retain input batches.
use std::{
    hash::{BuildHasher, Hash, Hasher},
    mem::size_of,
    sync::Arc,
};

use arrow::{
    array::{
        Array, ArrayRef, AsArray, BinaryArray, BinaryViewArray, BooleanArray, Int64Array,
        LargeBinaryArray, LargeStringArray, ListArray, StringArray, StringViewArray,
    },
    buffer::OffsetBuffer,
    datatypes::{DataType, Field, FieldRef},
};
use datafusion::{
    common::{Result, ScalarValue, exec_err, hash_utils::RandomState},
    functions_aggregate::count::Count,
    logical_expr::{Accumulator, AggregateUDFImpl, EmitTo, GroupsAccumulator},
};
use hashbrown::HashTable;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SharedCount {
    inner: Count,
}
impl Default for SharedCount {
    fn default() -> Self {
        Self {
            inner: Count::new(),
        }
    }
}

fn supported(args: &datafusion::logical_expr::function::AccumulatorArgs<'_>) -> bool {
    args.is_distinct
        && args.exprs.len() == 1
        && matches!(
            args.expr_fields[0].data_type(),
            DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Utf8View
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::BinaryView
        )
}

#[derive(Debug, Clone, Copy)]
struct Entry {
    hash: u64,
    group: usize,
    offset: usize,
    len: usize,
}

#[derive(Debug)]
pub struct ByteDistinctGroups {
    data_type: DataType,
    table: HashTable<Entry>,
    bytes: Vec<u8>,
    counts: Vec<i64>,
    hash_state: RandomState,
}
impl ByteDistinctGroups {
    pub fn new(data_type: DataType) -> Self {
        Self {
            data_type,
            table: HashTable::new(),
            bytes: vec![],
            counts: vec![],
            hash_state: RandomState::default(),
        }
    }
    fn insert(&mut self, group: usize, value: &[u8]) {
        let mut hasher = self.hash_state.build_hasher();
        group.hash(&mut hasher);
        value.hash(&mut hasher);
        let hash = hasher.finish();
        if self
            .table
            .find(hash, |e| {
                e.group == group && self.bytes[e.offset..e.offset + e.len] == *value
            })
            .is_some()
        {
            return;
        }
        let entry = Entry {
            hash,
            group,
            offset: self.bytes.len(),
            len: value.len(),
        };
        self.bytes.extend_from_slice(value);
        self.table.insert_unique(hash, entry, |e| e.hash);
        self.counts[group] += 1;
    }
    fn num_emit(&self, emit_to: EmitTo) -> usize {
        match emit_to {
            EmitTo::All => self.counts.len(),
            EmitTo::First(n) => n,
        }
    }
    fn remove_first(&mut self, n: usize) {
        if n == 0 {
            return;
        }
        if n == self.counts.len() {
            self.table = HashTable::new();
            self.bytes = vec![];
            self.counts = vec![];
            return;
        }
        // Group indices shift after First(n). Rehash and compact so emitted keys
        // do not remain charged to this accumulator or pin its arena indefinitely.
        let old_table = std::mem::take(&mut self.table);
        let old_bytes = std::mem::take(&mut self.bytes);
        let remaining = self.counts.len() - n;
        self.counts = vec![0; remaining];
        for e in old_table {
            if e.group >= n {
                self.insert(e.group - n, &old_bytes[e.offset..e.offset + e.len]);
            }
        }
    }
    fn update_range(&mut self, array: &dyn Array, start: usize, end: usize, group: usize) {
        macro_rules! insert_values {
            ($values:expr) => {{
                let values = $values;
                for i in start..end {
                    if !values.is_null(i) {
                        self.insert(group, values.value(i).as_ref());
                    }
                }
            }};
        }
        match array.data_type() {
            DataType::Utf8 => insert_values!(array.as_string::<i32>()),
            DataType::LargeUtf8 => insert_values!(array.as_string::<i64>()),
            DataType::Utf8View => insert_values!(array.as_string_view()),
            DataType::Binary => insert_values!(array.as_binary::<i32>()),
            DataType::LargeBinary => insert_values!(array.as_binary::<i64>()),
            DataType::BinaryView => insert_values!(array.as_binary_view()),
            _ => unreachable!("byte distinct input type was checked by Count"),
        }
    }
}

impl GroupsAccumulator for ByteDistinctGroups {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        groups: &[usize],
        filter: Option<&BooleanArray>,
        total: usize,
    ) -> Result<()> {
        self.counts.resize(total, 0);
        let values = &values[0];
        for (i, &group) in groups.iter().enumerate() {
            if filter.is_none_or(|f| !f.is_null(i) && f.value(i)) {
                self.update_range(values.as_ref(), i, i + 1, group);
            }
        }
        Ok(())
    }
    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        groups: &[usize],
        _filter: Option<&BooleanArray>,
        total: usize,
    ) -> Result<()> {
        self.counts.resize(total, 0);
        let lists = values[0].as_list::<i32>();
        let offsets = lists.value_offsets();
        for (i, &group) in groups.iter().enumerate() {
            if !lists.is_null(i) {
                self.update_range(
                    lists.values().as_ref(),
                    offsets[i] as usize,
                    offsets[i + 1] as usize,
                    group,
                );
            }
        }
        Ok(())
    }
    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let n = self.num_emit(emit_to);
        let result = Arc::new(Int64Array::from(self.counts[..n].to_vec()));
        self.remove_first(n);
        Ok(result)
    }
    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let n = self.num_emit(emit_to);
        let mut offsets = Vec::with_capacity(n + 1);
        offsets.push(0i32);
        for &count in &self.counts[..n] {
            let Some(next) = i64::from(*offsets.last().unwrap())
                .checked_add(count)
                .and_then(|v| i32::try_from(v).ok())
            else {
                return exec_err!("COUNT DISTINCT List state exceeds i32 offsets");
            };
            offsets.push(next);
        }
        let mut cursor: Vec<usize> = offsets[..n].iter().map(|v| *v as usize).collect();
        let mut ordered = vec![None; *offsets.last().unwrap() as usize];
        for e in self.table.iter() {
            if e.group < n {
                ordered[cursor[e.group]] = Some(&self.bytes[e.offset..e.offset + e.len]);
                cursor[e.group] += 1;
            }
        }
        let strings = || {
            ordered
                .iter()
                .map(|v| v.map(|b| std::str::from_utf8(b).expect("validated Arrow UTF-8")))
        };
        let values: ArrayRef = match self.data_type {
            DataType::Utf8 => Arc::new(StringArray::from_iter(strings())),
            DataType::LargeUtf8 => Arc::new(LargeStringArray::from_iter(strings())),
            DataType::Utf8View => Arc::new(StringViewArray::from_iter(strings())),
            DataType::Binary => Arc::new(BinaryArray::from_iter(ordered.iter().copied())),
            DataType::LargeBinary => Arc::new(LargeBinaryArray::from_iter(ordered.iter().copied())),
            DataType::BinaryView => Arc::new(BinaryViewArray::from_iter(ordered.iter().copied())),
            _ => unreachable!("byte distinct state type"),
        };
        let list = ListArray::try_new(
            Arc::new(Field::new_list_field(self.data_type.clone(), true)),
            OffsetBuffer::new(offsets.into()),
            values,
            None,
        )?;
        self.remove_first(n);
        Ok(vec![Arc::new(list)])
    }
    fn size(&self) -> usize {
        size_of::<Self>()
            + self.table.allocation_size()
            + self.bytes.capacity()
            + self.counts.capacity() * size_of::<i64>()
    }
}

use datafusion::logical_expr::{
    Documentation, Expr, Operator, ReversedUDAF, SetMonotonicity, Signature, StatisticsArgs,
    expr::{AggregateFunction, AggregateFunctionParams, WindowFunctionParams},
    function::{AccumulatorArgs, AggregateFunctionSimplification, StateFieldsArgs},
    utils::AggregateOrderSensitivity,
};
#[warn(clippy::missing_trait_methods)] // Delegates, so it should implement every single trait method
impl AggregateUDFImpl for SharedCount {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        self.inner.accumulator(acc_args)
    }

    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }

    fn schema_name(&self, params: &AggregateFunctionParams) -> Result<String> {
        self.inner.schema_name(params)
    }

    fn human_display(&self, params: &AggregateFunctionParams) -> Result<String> {
        self.inner.human_display(params)
    }

    fn window_function_schema_name(&self, params: &WindowFunctionParams) -> Result<String> {
        self.inner.window_function_schema_name(params)
    }

    fn display_name(&self, params: &AggregateFunctionParams) -> Result<String> {
        self.inner.display_name(params)
    }

    fn window_function_display_name(&self, params: &WindowFunctionParams) -> Result<String> {
        self.inner.window_function_display_name(params)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        self.inner.state_fields(args)
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        supported(&args) || self.inner.groups_accumulator_supported(args)
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        if supported(&args) {
            Ok(Box::new(ByteDistinctGroups::new(
                args.expr_fields[0].data_type().clone(),
            )))
        } else {
            self.inner.create_groups_accumulator(args)
        }
    }

    fn create_sliding_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        self.inner.create_sliding_accumulator(args)
    }

    fn with_beneficial_ordering(
        self: Arc<Self>,
        beneficial_ordering: bool,
    ) -> Result<Option<Arc<dyn AggregateUDFImpl>>> {
        Arc::new(Count::new()).with_beneficial_ordering(beneficial_ordering)
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        self.inner.order_sensitivity()
    }

    fn simplify(&self) -> Option<AggregateFunctionSimplification> {
        self.inner.simplify()
    }

    fn simplify_expr_op_literal(
        &self,
        agg_function: &AggregateFunction,
        arg: &Expr,
        op: Operator,
        lit: &Expr,
        arg_is_left: bool,
    ) -> Result<Option<Expr>> {
        self.inner
            .simplify_expr_op_literal(agg_function, arg, op, lit, arg_is_left)
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        self.inner.reverse_expr()
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }

    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef> {
        self.inner.return_field(arg_fields)
    }

    fn is_nullable(&self) -> bool {
        self.inner.is_nullable()
    }

    fn is_descending(&self) -> Option<bool> {
        self.inner.is_descending()
    }

    fn value_from_stats(&self, statistics_args: &StatisticsArgs) -> Option<ScalarValue> {
        self.inner.value_from_stats(statistics_args)
    }

    fn default_value(&self, data_type: &DataType) -> Result<ScalarValue> {
        self.inner.default_value(data_type)
    }

    fn supports_null_handling_clause(&self) -> bool {
        self.inner.supports_null_handling_clause()
    }

    fn supports_within_group_clause(&self) -> bool {
        self.inner.supports_within_group_clause()
    }

    fn set_monotonicity(&self, data_type: &DataType) -> SetMonotonicity {
        self.inner.set_monotonicity(data_type)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }
}

#[cfg(test)]
#[path = "shared_count_tests.rs"]
mod tests;
