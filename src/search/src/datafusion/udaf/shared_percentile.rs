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

//! Internal, configured aggregate: one TDigest, multiple ordered percentile outputs.
//! The physical codec transports configuration, not per-input-row constant arrays.

use std::{mem::size_of, sync::Arc};

use arrow::{
    array::{
        Array, ArrayRef, ArrowPrimitiveType, BooleanArray, Float16Array, Float32Array,
        Float64Array, Float64Builder, ListArray, ListBuilder, StructArray, UInt64Array,
        UInt64Builder,
    },
    compute::{filter, is_not_null, sort, take},
    datatypes::{DataType, Field, FieldRef, Fields, Float16Type},
};
use datafusion::{
    common::{Result, ScalarValue, exec_err, plan_err, types::NativeType},
    functions_aggregate::approx_percentile_cont::ApproxPercentileCont,
    logical_expr::{
        Accumulator, AggregateUDF, AggregateUDFImpl, Coercion, EmitTo, GroupsAccumulator,
        Signature, TypeSignature, TypeSignatureClass, Volatility,
        function::{AccumulatorArgs, StateFieldsArgs},
    },
};
use datafusion_functions_aggregate_common::tdigest::TDigest;

pub const NAME: &str = "__oo_shared_percentiles";
// Version 1 defines q{ordinal}, nullable Float-family children in configuration order.
// Child types derive from the sole coerced runtime argument, exactly as for the stock UDAF.
const CONFIG_HEADER: &[u8; 5] = b"OOSP\x01";

type Float16 = <Float16Type as ArrowPrimitiveType>::Native;

#[derive(Debug, PartialEq, Eq, Hash)]
struct Config {
    quantile_bits: Box<[u64]>,
    max_centroids: usize,
}

impl Config {
    fn size(&self) -> usize {
        size_of::<Self>() + size_of::<u64>() * self.quantile_bits.len()
    }

    fn fields(&self, value_type: &DataType) -> Fields {
        self.quantile_bits
            .iter()
            .enumerate()
            .map(|(i, _)| Arc::new(Field::new(format!("q{i}"), value_type.clone(), true)))
            .collect()
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct SharedPercentiles {
    signature: Signature,
    config: Arc<Config>,
}

impl SharedPercentiles {
    pub(crate) fn try_new(quantiles: Vec<f64>, max_centroids: usize) -> Result<Self> {
        if quantiles.is_empty() {
            return plan_err!("{NAME} requires at least one percentile");
        }
        if max_centroids == 0 {
            return plan_err!("{NAME} requires positive centroid precision");
        }
        if quantiles
            .iter()
            .any(|q| !q.is_finite() || !(0.0..=1.0).contains(q))
        {
            return plan_err!("{NAME} percentiles must be finite values between 0 and 1");
        }
        Ok(Self {
            signature: Signature::one_of(
                vec![TypeSignature::Coercible(vec![Coercion::new_implicit(
                    TypeSignatureClass::Float,
                    vec![TypeSignatureClass::Numeric],
                    NativeType::Float64,
                )])],
                Volatility::Immutable,
            ),
            config: Arc::new(Config {
                quantile_bits: quantiles.into_iter().map(f64::to_bits).collect(),
                max_centroids,
            }),
        })
    }

    pub(crate) fn encode_config(&self) -> Result<Vec<u8>> {
        let count = u32::try_from(self.config.quantile_bits.len()).map_err(|_| {
            datafusion::common::DataFusionError::Plan(format!("{NAME} has too many percentiles"))
        })?;
        let mut bytes = Vec::with_capacity(17 + self.config.quantile_bits.len() * 8);
        bytes.extend_from_slice(CONFIG_HEADER);
        bytes.extend_from_slice(&(self.config.max_centroids as u64).to_le_bytes());
        bytes.extend_from_slice(&count.to_le_bytes());
        for bits in &self.config.quantile_bits {
            bytes.extend_from_slice(&bits.to_le_bytes());
        }
        Ok(bytes)
    }

    pub(crate) fn decode_config(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 17 || &bytes[..5] != CONFIG_HEADER {
            return plan_err!("Invalid or unsupported {NAME} configuration version");
        }
        let max_centroids = u64::from_le_bytes(bytes[5..13].try_into().unwrap());
        let count = u32::from_le_bytes(bytes[13..17].try_into().unwrap()) as usize;
        if count == 0 || bytes.len().checked_sub(17) != count.checked_mul(8) {
            return plan_err!("Invalid {NAME} configuration length");
        }
        let max_centroids = usize::try_from(max_centroids).map_err(|_| {
            datafusion::common::DataFusionError::Plan(format!(
                "{NAME} centroid precision overflows usize"
            ))
        })?;
        let quantiles = bytes[17..]
            .chunks_exact(8)
            .map(|chunk| f64::from_bits(u64::from_le_bytes(chunk.try_into().unwrap())))
            .collect();
        Self::try_new(quantiles, max_centroids)
    }

    fn value_type<'a>(&self, args: &'a AccumulatorArgs) -> Result<&'a DataType> {
        if args.expr_fields.len() != 1 || args.is_distinct || !args.order_bys.is_empty() {
            return plan_err!("{NAME} requires one unordered, non-DISTINCT value argument");
        }
        let data_type = args.expr_fields[0].data_type();
        validate_value_type(data_type)?;
        Ok(data_type)
    }
}

pub fn create_udaf(quantiles: Vec<f64>, max_centroids: usize) -> Result<Arc<AggregateUDF>> {
    Ok(Arc::new(AggregateUDF::from(SharedPercentiles::try_new(
        quantiles,
        max_centroids,
    )?)))
}

impl AggregateUDFImpl for SharedPercentiles {
    fn name(&self) -> &str {
        NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [value_type] = arg_types else {
            return plan_err!("{NAME} requires exactly one value argument");
        };
        validate_value_type(value_type)?;
        Ok(DataType::Struct(self.config.fields(value_type)))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        // Keep the public stock digest state schema, including its list item nullability.
        ApproxPercentileCont::new().state_fields(args)
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(SharedAccumulator::new(
            Arc::clone(&self.config),
            self.value_type(&args)?.clone(),
        )))
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(SharedGroupsAccumulator::new(
            Arc::clone(&self.config),
            self.value_type(&args)?.clone(),
        )))
    }
}

fn validate_value_type(data_type: &DataType) -> Result<()> {
    if !matches!(
        data_type,
        DataType::Float16 | DataType::Float32 | DataType::Float64
    ) {
        return plan_err!("{NAME} requires a coerced floating-point value, got {data_type}");
    }
    Ok(())
}

/// Gather and sort Float64 in reusable scratch; keep Float16/32 sorting native
/// so conversion does not change their NaN ordering.
fn merge_sorted_values(
    digest: &TDigest,
    values: &ArrayRef,
    scratch: &mut Vec<f64>,
) -> Result<TDigest> {
    scratch.clear();
    if values.data_type() == &DataType::Float64 {
        let values = values.as_any().downcast_ref::<Float64Array>().unwrap();
        if values.null_count() == 0 {
            scratch.extend_from_slice(values.values());
        } else {
            scratch.extend(values.iter().flatten());
        }
        scratch.sort_unstable_by(f64::total_cmp);
        return Ok(digest.merge_sorted_f64(scratch));
    }
    let non_null = if values.null_count() == 0 {
        Arc::clone(values)
    } else {
        filter(values, &is_not_null(values)?)?
    };
    let sorted = sort(&non_null, None)?;
    match sorted.data_type() {
        DataType::Float16 => scratch.extend(
            sorted
                .as_any()
                .downcast_ref::<Float16Array>()
                .unwrap()
                .values()
                .iter()
                .map(|v| v.to_f64()),
        ),
        DataType::Float32 => scratch.extend(
            sorted
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .values()
                .iter()
                .map(|v| *v as f64),
        ),
        other => return exec_err!("{NAME} received unexpected value type {other}"),
    }
    Ok(digest.merge_sorted_f64(scratch))
}

/// Build one column per output, never a nested ScalarValue per group.
fn evaluate_digests(digests: &[TDigest], config: &Config, fields: &Fields) -> Result<StructArray> {
    let columns = config
        .quantile_bits
        .iter()
        .map(|bits| {
            let q = f64::from_bits(*bits);
            let estimates = digests
                .iter()
                .map(|digest| (digest.count() != 0.0).then(|| digest.estimate_quantile(q)));
            let column: ArrayRef = match fields[0].data_type() {
                DataType::Float16 => Arc::new(Float16Array::from_iter(
                    estimates.map(|q| q.map(Float16::from_f64)),
                )),
                DataType::Float32 => Arc::new(Float32Array::from_iter(
                    estimates.map(|q| q.map(|v| v as f32)),
                )),
                DataType::Float64 => Arc::new(Float64Array::from_iter(estimates)),
                _ => unreachable!("validated percentile output type"),
            };
            column
        })
        .collect();
    // Nulls must be in the children: get_field projects the child directly.
    Ok(StructArray::try_new(fields.clone(), columns, None)?)
}

#[derive(Debug)]
struct SharedAccumulator {
    digest: TDigest,
    config: Arc<Config>,
    fields: Fields,
    scratch: Vec<f64>,
}

impl SharedAccumulator {
    fn new(config: Arc<Config>, value_type: DataType) -> Self {
        Self {
            digest: TDigest::new(config.max_centroids),
            fields: config.fields(&value_type),
            config,
            scratch: Vec::new(),
        }
    }
}

impl Accumulator for SharedAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let [values] = values else {
            return exec_err!("{NAME} requires exactly one value array");
        };
        self.digest = merge_sorted_values(&self.digest, values, &mut self.scratch)?;
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Struct(Arc::new(evaluate_digests(
            std::slice::from_ref(&self.digest),
            &self.config,
            &self.fields,
        )?)))
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(self.digest.to_scalar_state())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        validate_states(states)?;
        let incoming = (0..states[0].len())
            .map(|row| decode_digest(states, row))
            .collect::<Result<Vec<_>>>()?;
        // Incoming states precede existing state, including empty states, as in stock.
        self.digest = TDigest::merge_digests(incoming.iter().chain(std::iter::once(&self.digest)));
        Ok(())
    }

    fn size(&self) -> usize {
        size_of::<Self>() + self.digest.size() - size_of::<TDigest>()
            + self.scratch.capacity() * size_of::<f64>()
            + self.fields.size()
            + self.config.size()
            + 2 * size_of::<usize>()
    }
}

/// Counting gather reuses framework group IDs and preserves each group's input order.
/// Scratch is bounded by the current operator's group count and largest input batch,
/// never by the total number of rows processed. No per-group Vec or hash map is needed.
#[derive(Debug, Default)]
struct Gather {
    offsets: Vec<usize>,
    cursors: Vec<usize>,
    indices: Vec<u64>,
}

impl Gather {
    fn prepare(
        &mut self,
        group_indices: &[usize],
        filter: Option<&BooleanArray>,
        total_groups: usize,
        values: Option<&dyn Array>,
    ) -> Result<()> {
        if filter.is_some_and(|f| f.len() != group_indices.len())
            || values.is_some_and(|v| v.len() != group_indices.len())
        {
            return exec_err!("{NAME} group/filter/value lengths do not match");
        }
        self.offsets.clear();
        self.offsets.resize(total_groups + 1, 0);
        let accepted = |row: usize| {
            filter.is_none_or(|f| f.is_valid(row) && f.value(row))
                && values.is_none_or(|v| v.is_valid(row))
        };
        for (row, &group) in group_indices.iter().enumerate() {
            if group >= total_groups {
                return exec_err!("{NAME} group index is out of bounds");
            }
            if accepted(row) {
                self.offsets[group + 1] += 1;
            }
        }
        for group in 0..total_groups {
            self.offsets[group + 1] += self.offsets[group];
        }
        self.cursors.clear();
        self.cursors
            .extend_from_slice(&self.offsets[..total_groups]);
        self.indices.resize(self.offsets[total_groups], 0);
        for (row, &group) in group_indices.iter().enumerate() {
            if accepted(row) {
                self.indices[self.cursors[group]] = row as u64;
                self.cursors[group] += 1;
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        (self.offsets.capacity() + self.cursors.capacity()) * size_of::<usize>()
            + self.indices.capacity() * size_of::<u64>()
    }
}

#[derive(Debug)]
struct SharedGroupsAccumulator {
    digests: Vec<TDigest>,
    digest_heap_bytes: usize,
    config: Arc<Config>,
    fields: Fields,
    gather: Gather,
    scratch: Vec<f64>,
}

impl SharedGroupsAccumulator {
    fn new(config: Arc<Config>, value_type: DataType) -> Self {
        Self {
            digests: Vec::new(),
            digest_heap_bytes: 0,
            fields: config.fields(&value_type),
            config,
            gather: Gather::default(),
            scratch: Vec::new(),
        }
    }

    fn replace_digest(&mut self, group: usize, digest: TDigest) {
        self.digest_heap_bytes -= self.digests[group].size() - size_of::<TDigest>();
        self.digest_heap_bytes += digest.size() - size_of::<TDigest>();
        self.digests[group] = digest;
    }

    fn emit(&mut self, emit_to: EmitTo) -> Vec<TDigest> {
        let emitted = emit_to.take_needed(&mut self.digests);
        for digest in &emitted {
            self.digest_heap_bytes -= digest.size() - size_of::<TDigest>();
        }
        if self.digests.is_empty() {
            self.gather = Gather::default();
            self.scratch = Vec::new();
        }
        emitted
    }
}

impl GroupsAccumulator for SharedGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        let [values] = values else {
            return exec_err!("{NAME} requires exactly one value array");
        };
        self.gather.prepare(
            group_indices,
            opt_filter,
            total_num_groups,
            Some(values.as_ref()),
        )?;
        self.digests
            .resize_with(total_num_groups, || TDigest::new(self.config.max_centroids));
        if values.data_type() == &DataType::Float64 {
            let values = values.as_any().downcast_ref::<Float64Array>().unwrap();
            for group in 0..total_num_groups {
                let start = self.gather.offsets[group];
                let end = self.gather.offsets[group + 1];
                if start == end {
                    continue;
                }
                self.scratch.clear();
                self.scratch.extend(
                    self.gather.indices[start..end]
                        .iter()
                        .map(|&row| values.value(row as usize)),
                );
                self.scratch.sort_unstable_by(f64::total_cmp);
                let digest = self.digests[group].merge_sorted_f64(&self.scratch);
                self.replace_digest(group, digest);
            }
            return Ok(());
        }
        // Transfer the reusable index buffer into Arrow and recover it after take,
        // avoiding a second batch-sized index allocation/copy.
        let indices = UInt64Array::from(std::mem::take(&mut self.gather.indices));
        let gathered = take(values, &indices, None);
        self.gather.indices = indices
            .into_parts()
            .1
            .into_inner()
            .into_vec::<u64>()
            .expect("uniquely owned gather index buffer");
        let gathered = gathered?;
        for group in 0..total_num_groups {
            let start = self.gather.offsets[group];
            let len = self.gather.offsets[group + 1] - start;
            if len == 0 {
                continue;
            }
            let digest = merge_sorted_values(
                &self.digests[group],
                &gathered.slice(start, len),
                &mut self.scratch,
            )?;
            self.replace_digest(group, digest);
        }
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let digests = self.emit(emit_to);
        Ok(Arc::new(evaluate_digests(
            &digests,
            &self.config,
            &self.fields,
        )?))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        digest_states(self.emit(emit_to).iter())
    }

    fn merge_batch(
        &mut self,
        states: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        validate_states(states)?;
        if states[0].len() != group_indices.len() {
            return exec_err!("{NAME} state/group lengths do not match");
        }
        self.gather
            .prepare(group_indices, opt_filter, total_num_groups, None)?;
        self.digests
            .resize_with(total_num_groups, || TDigest::new(self.config.max_centroids));
        let mut incoming = Vec::new();
        for group in 0..total_num_groups {
            let start = self.gather.offsets[group];
            let end = self.gather.offsets[group + 1];
            if start == end {
                continue;
            }
            incoming.clear();
            for &row in &self.gather.indices[start..end] {
                incoming.push(decode_digest(states, row as usize)?);
            }
            let digest = TDigest::merge_digests(
                incoming.iter().chain(std::iter::once(&self.digests[group])),
            );
            self.replace_digest(group, digest);
        }
        Ok(())
    }

    // Keep DataFusion's adaptive partial-aggregation bypass available. A row becomes
    // the same singleton/empty digest that stock's grouped adapter would serialize.
    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let [values] = values else {
            return exec_err!("{NAME} requires exactly one value array");
        };
        validate_value_type(values.data_type())?;
        if opt_filter.is_some_and(|f| f.len() != values.len()) {
            return exec_err!("{NAME} filter/value lengths do not match");
        }
        let values_f64 = arrow::compute::cast(values, &DataType::Float64)?;
        let values_f64 = values_f64.as_any().downcast_ref::<Float64Array>().unwrap();
        let digests = (0..values.len()).map(|row| {
            let digest = TDigest::new(self.config.max_centroids);
            if values.is_valid(row) && opt_filter.is_none_or(|f| f.is_valid(row) && f.value(row)) {
                digest.merge_sorted_f64(&[values_f64.value(row)])
            } else {
                digest
            }
        });
        // Stream through the public serializer rather than retain one digest per row.
        digest_states(digests)
    }

    fn supports_convert_to_state(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        size_of::<Self>()
            + self.digests.capacity() * size_of::<TDigest>()
            + self.digest_heap_bytes
            + self.gather.size()
            + self.scratch.capacity() * size_of::<f64>()
            + self.fields.size()
            + self.config.size()
            + 2 * size_of::<usize>()
    }
}

/// The public TDigest deserializer assumes this exact trusted state shape and can
/// panic otherwise. Check structural invariants before invoking it on wire arrays.
fn validate_states(states: &[ArrayRef]) -> Result<()> {
    if states.len() != 6 {
        return exec_err!("{NAME} requires six TDigest state columns");
    }
    let rows = states[0].len();
    for (index, array) in states.iter().enumerate() {
        let valid_type = match index {
            0 => array.data_type() == &DataType::UInt64,
            1..=4 => array.data_type() == &DataType::Float64,
            5 => {
                matches!(array.data_type(), DataType::List(field) if field.data_type() == &DataType::Float64)
            }
            _ => unreachable!(),
        };
        if !valid_type || array.len() != rows || array.null_count() != 0 {
            return exec_err!("Invalid {NAME} TDigest state column {index}");
        }
    }
    Ok(())
}

fn decode_digest(states: &[ArrayRef], row: usize) -> Result<TDigest> {
    let centroids = states[5]
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .value(row);
    if centroids.len() % 2 != 0 || centroids.null_count() != 0 {
        return exec_err!("Invalid {NAME} TDigest centroid pairs");
    }
    let max = states[3]
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(row);
    let min = states[4]
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(row);
    if min.is_finite() && max.is_finite() && max.total_cmp(&min).is_lt() {
        return exec_err!("Invalid {NAME} TDigest bounds");
    }
    let state = states
        .iter()
        .map(|array| ScalarValue::try_from_array(array, row))
        .collect::<Result<Vec<_>>>()?;
    Ok(TDigest::from_scalar_state(&state))
}

/// Serialize through the pinned public TDigest helpers. Only one digest's temporary
/// scalar/list state is live at a time; output is six contiguous Arrow columns.
fn digest_states<I, D>(digests: I) -> Result<Vec<ArrayRef>>
where
    I: IntoIterator<Item = D>,
    D: std::borrow::Borrow<TDigest>,
{
    let digests = digests.into_iter();
    let capacity = digests.size_hint().0;
    let mut max_size = UInt64Builder::with_capacity(capacity);
    let mut numbers: [Float64Builder; 4] =
        std::array::from_fn(|_| Float64Builder::with_capacity(capacity));
    let mut centroids = ListBuilder::with_capacity(Float64Builder::new(), capacity);
    for digest in digests {
        let state = digest.borrow().to_scalar_state();
        let ScalarValue::UInt64(Some(value)) = &state[0] else {
            unreachable!("TDigest state max_size")
        };
        max_size.append_value(*value);
        for (builder, value) in numbers.iter_mut().zip(&state[1..5]) {
            let ScalarValue::Float64(Some(value)) = value else {
                unreachable!("TDigest numeric state")
            };
            builder.append_value(*value);
        }
        let ScalarValue::List(array) = &state[5] else {
            unreachable!("TDigest centroid state")
        };
        let values = array
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        centroids.values().append_slice(values.values());
        centroids.append(true);
    }
    let mut arrays = Vec::with_capacity(6);
    arrays.push(Arc::new(max_size.finish()) as ArrayRef);
    arrays.extend(
        numbers
            .iter_mut()
            .map(|builder| Arc::new(builder.finish()) as ArrayRef),
    );
    arrays.push(Arc::new(centroids.finish()));
    Ok(arrays)
}

#[cfg(test)]
mod tests {
    use datafusion::{
        functions_aggregate::approx_percentile_cont::ApproxPercentileAccumulator,
        prelude::{SessionContext, col},
    };
    use datafusion_functions_aggregate_common::tdigest::DEFAULT_MAX_SIZE;

    use super::*;

    fn config(quantiles: &[f64], precision: usize) -> Arc<Config> {
        SharedPercentiles::try_new(quantiles.to_vec(), precision)
            .unwrap()
            .config
    }

    fn stock(config: &Config, value_type: &DataType) -> Vec<ApproxPercentileAccumulator> {
        config
            .quantile_bits
            .iter()
            .map(|bits| {
                ApproxPercentileAccumulator::new_with_max_size(
                    f64::from_bits(*bits),
                    value_type.clone(),
                    config.max_centroids,
                )
            })
            .collect()
    }

    fn assert_row(result: &StructArray, row: usize, expected: &mut [ApproxPercentileAccumulator]) {
        for (column, expected) in result.columns().iter().zip(expected) {
            assert_eq!(
                ScalarValue::try_from_array(column, row).unwrap(),
                expected.evaluate().unwrap()
            );
        }
    }

    fn state_columns(rows: Vec<Vec<ScalarValue>>) -> Vec<ArrayRef> {
        (0..6)
            .map(|i| ScalarValue::iter_to_array(rows.iter().map(|row| row[i].clone())).unwrap())
            .collect()
    }

    #[test]
    fn scalar_float_families_empty_null_and_repeated_batches_match_stock() {
        let config = config(&[1.0, 0.95, 0.5, 0.0, 0.5], 17);
        let input: ArrayRef =
            Arc::new(Float64Array::from_iter((0..4096).map(|i| {
                (i % 11 != 0).then_some(((i * 97) % 1301) as f64 - 650.0)
            })));
        for value_type in [DataType::Float16, DataType::Float32, DataType::Float64] {
            let input = arrow::compute::cast(&input, &value_type).unwrap();
            let mut actual = SharedAccumulator::new(Arc::clone(&config), value_type.clone());
            let mut expected = stock(&config, &value_type);
            // Empty and all-null groups expose typed NULL children, not zero estimates.
            for values in [
                arrow::array::new_empty_array(&value_type),
                arrow::array::new_null_array(&value_type, 9),
            ] {
                actual.update_batch(&[Arc::clone(&values)]).unwrap();
                for expected in &mut expected {
                    expected.update_batch(&[Arc::clone(&values)]).unwrap();
                }
                let ScalarValue::Struct(result) = actual.evaluate().unwrap() else {
                    panic!()
                };
                assert_row(&result, 0, &mut expected);
            }
            for start in (0..input.len()).step_by(137) {
                let values = input.slice(start, 137.min(input.len() - start));
                actual.update_batch(&[Arc::clone(&values)]).unwrap();
                for expected in &mut expected {
                    expected.update_batch(&[Arc::clone(&values)]).unwrap();
                }
            }
            for _ in 0..2 {
                let ScalarValue::Struct(result) = actual.evaluate().unwrap() else {
                    panic!()
                };
                assert_row(&result, 0, &mut expected);
            }
        }
    }

    #[test]
    fn scalar_partial_final_including_empty_custom_precision_matches_stock() {
        let config = config(&[0.1, 0.5, 0.99], 7);
        let mut actual = SharedAccumulator::new(Arc::clone(&config), DataType::Float64);
        let mut expected = stock(&config, &DataType::Float64);
        // The stock digest resets precision on an all-empty merge. Preserve this
        // transition, including later updates and a subsequent nonempty merge.
        for empty in [true, false] {
            let mut actual_states = Vec::new();
            let mut expected_states: Vec<Vec<Vec<ScalarValue>>> = vec![Vec::new(); expected.len()];
            for partition in 0..3 {
                let values: ArrayRef = if empty {
                    Arc::new(Float64Array::from(Vec::<Option<f64>>::new()))
                } else {
                    Arc::new(Float64Array::from_iter_values(
                        (0..400).map(|i| ((i * 31 + partition * 17) % 809) as f64),
                    ))
                };
                let mut partial = SharedAccumulator::new(Arc::clone(&config), DataType::Float64);
                partial.update_batch(&[Arc::clone(&values)]).unwrap();
                actual_states.push(partial.state().unwrap());
                for (slot, mut partial) in
                    stock(&config, &DataType::Float64).into_iter().enumerate()
                {
                    partial.update_batch(&[Arc::clone(&values)]).unwrap();
                    expected_states[slot].push(partial.state().unwrap());
                }
            }
            actual.merge_batch(&state_columns(actual_states)).unwrap();
            for (acc, states) in expected.iter_mut().zip(expected_states) {
                acc.merge_batch(&state_columns(states)).unwrap();
            }
            let extra: ArrayRef = Arc::new(Float64Array::from(vec![12.0, 73.0, 999.0]));
            actual.update_batch(&[Arc::clone(&extra)]).unwrap();
            for acc in &mut expected {
                acc.update_batch(&[Arc::clone(&extra)]).unwrap();
            }
            let ScalarValue::Struct(result) = actual.evaluate().unwrap() else {
                panic!()
            };
            assert_row(&result, 0, &mut expected);
        }
    }

    fn update_stock_groups(
        expected: &mut [Vec<ApproxPercentileAccumulator>],
        values: &ArrayRef,
        groups: &[usize],
        filter: Option<&BooleanArray>,
    ) {
        for (group, accumulators) in expected.iter_mut().enumerate() {
            let indices =
                UInt64Array::from_iter_values(groups.iter().enumerate().filter_map(|(row, &g)| {
                    (g == group && filter.is_none_or(|f| f.is_valid(row) && f.value(row)))
                        .then_some(row as u64)
                }));
            if indices.is_empty() {
                continue;
            }
            let values = take(values, &indices, None).unwrap();
            for acc in accumulators {
                acc.update_batch(&[Arc::clone(&values)]).unwrap();
            }
        }
    }

    #[test]
    fn grouped_special_floats_update_merge_and_bypass_match_stock() {
        // Compare NaN classification rather than payloads, while retaining exact
        // finite values, infinity signs, signed zero, output types and NULLs.
        fn assert_special_result(
            result: &ArrayRef,
            expected: &mut [Vec<ApproxPercentileAccumulator>],
        ) {
            let result = result.as_any().downcast_ref::<StructArray>().unwrap();
            let as_float = |value: ScalarValue| match value {
                ScalarValue::Float16(value) => value.map(|v| v.to_f64()),
                ScalarValue::Float32(value) => value.map(|v| v as f64),
                ScalarValue::Float64(value) => value,
                other => panic!("unexpected percentile output {other:?}"),
            };
            for (group, accumulators) in expected.iter_mut().enumerate() {
                for (column, acc) in result.columns().iter().zip(accumulators) {
                    let actual = ScalarValue::try_from_array(column, group).unwrap();
                    let expected = acc.evaluate().unwrap();
                    assert_eq!(actual.data_type(), expected.data_type());
                    match (as_float(actual), as_float(expected)) {
                        (None, None) => {}
                        (Some(actual), Some(expected)) if expected.is_nan() => {
                            assert!(actual.is_nan(), "expected NaN, got {actual}");
                        }
                        (Some(actual), Some(expected)) => {
                            assert_eq!(actual.to_bits(), expected.to_bits());
                        }
                        (actual, expected) => {
                            panic!("NULL mismatch: actual={actual:?}, expected={expected:?}");
                        }
                    }
                }
            }
        }

        let config = config(&[0.0, 0.1, 0.5, 0.9, 1.0], 3);
        // Interleaved finite-only, infinities, signed-NaNs, mixed-special and
        // NULL/filter-only groups ensure NaNs cannot mask finite or NULL errors.
        let values = [
            Some(-0.0),
            Some(f64::NEG_INFINITY),
            Some(f64::from_bits(0xfff8_0000_0000_0001)),
            Some(f64::NEG_INFINITY),
            None,
            Some(0.0),
            Some(f64::INFINITY),
            Some(f64::from_bits(0x7ff8_0000_0000_0001)),
            Some(f64::from_bits(0xfff8_0000_0000_0001)),
            Some(100.0),
            Some(-10.0),
            Some(-3.0),
            Some(5.0),
            Some(f64::INFINITY),
            None,
            Some(10.0),
            Some(7.0),
            Some(-5.0),
            Some(f64::from_bits(0x7ff8_0000_0000_0001)),
            Some(-100.0),
        ];
        let group_ids: Vec<_> = (0..values.len()).map(|row| row % 5).collect();
        let filter = BooleanArray::from_iter((0..values.len()).map(|row| match row {
            9 => Some(false),
            19 => None,
            _ => Some(true),
        }));
        for value_type in [DataType::Float16, DataType::Float32, DataType::Float64] {
            // Construct each native family directly, including both NaN signs;
            // casting a wider test array must not normalize the input under test.
            let values: ArrayRef = match value_type {
                DataType::Float16 => Arc::new(Float16Array::from_iter(values.iter().map(|v| {
                    v.map(|v| {
                        if v.is_nan() {
                            Float16::from_bits(if v.is_sign_negative() { 0xfe01 } else { 0x7e01 })
                        } else {
                            Float16::from_f64(v)
                        }
                    })
                }))),
                DataType::Float32 => Arc::new(Float32Array::from_iter(values.iter().map(|v| {
                    v.map(|v| {
                        if v.is_nan() {
                            f32::from_bits(if v.is_sign_negative() {
                                0xffc0_0001
                            } else {
                                0x7fc0_0001
                            })
                        } else {
                            v as f32
                        }
                    })
                }))),
                DataType::Float64 => Arc::new(Float64Array::from_iter(values.iter().copied())),
                _ => unreachable!(),
            };
            let mut direct = SharedGroupsAccumulator::new(Arc::clone(&config), value_type.clone());
            let mut merged = SharedGroupsAccumulator::new(Arc::clone(&config), value_type.clone());
            let mut bypassed =
                SharedGroupsAccumulator::new(Arc::clone(&config), value_type.clone());
            let mut expected_direct: Vec<_> = (0..5).map(|_| stock(&config, &value_type)).collect();
            let mut expected_merged: Vec<_> = (0..5).map(|_| stock(&config, &value_type)).collect();
            let mut expected_bypassed: Vec<_> =
                (0..5).map(|_| stock(&config, &value_type)).collect();
            for start in [0, 10] {
                let batch = values.slice(start, 10);
                let groups = &group_ids[start..start + 10];
                let filter = filter.slice(start, 10);
                direct
                    .update_batch(&[Arc::clone(&batch)], groups, Some(&filter), 5)
                    .unwrap();
                update_stock_groups(&mut expected_direct, &batch, groups, Some(&filter));

                let mut partial =
                    SharedGroupsAccumulator::new(Arc::clone(&config), value_type.clone());
                let mut expected_partial: Vec<_> =
                    (0..5).map(|_| stock(&config, &value_type)).collect();
                partial
                    .update_batch(&[Arc::clone(&batch)], groups, Some(&filter), 5)
                    .unwrap();
                update_stock_groups(&mut expected_partial, &batch, groups, Some(&filter));
                merged
                    .merge_batch(
                        &partial.state(EmitTo::All).unwrap(),
                        &[0, 1, 2, 3, 4],
                        None,
                        5,
                    )
                    .unwrap();
                for group in 0..5 {
                    for slot in 0..config.quantile_bits.len() {
                        let state = expected_partial[group][slot].state().unwrap();
                        expected_merged[group][slot]
                            .merge_batch(&state_columns(vec![state]))
                            .unwrap();
                    }
                }

                let states = partial
                    .convert_to_state(&[Arc::clone(&batch)], Some(&filter))
                    .unwrap();
                bypassed.merge_batch(&states, groups, None, 5).unwrap();
                for group in 0..5 {
                    for slot in 0..config.quantile_bits.len() {
                        let mut states = Vec::new();
                        for (row, &target) in groups.iter().enumerate() {
                            if target != group {
                                continue;
                            }
                            let mut singleton = stock(&config, &value_type).remove(slot);
                            if filter.is_valid(row) && filter.value(row) {
                                singleton.update_batch(&[batch.slice(row, 1)]).unwrap();
                            }
                            states.push(singleton.state().unwrap());
                        }
                        expected_bypassed[group][slot]
                            .merge_batch(&state_columns(states))
                            .unwrap();
                    }
                }
            }
            assert_special_result(&direct.evaluate(EmitTo::All).unwrap(), &mut expected_direct);
            assert_special_result(&merged.evaluate(EmitTo::All).unwrap(), &mut expected_merged);
            assert_special_result(
                &bypassed.evaluate(EmitTo::All).unwrap(),
                &mut expected_bypassed,
            );
        }
    }

    #[test]
    fn float64_sliced_special_values_preserve_stock_state_bits() {
        fn assert_state_bits(actual: &[ScalarValue], expected: &[ScalarValue]) {
            assert_eq!(actual.len(), expected.len());
            for (actual, expected) in actual.iter().zip(expected) {
                match (actual, expected) {
                    (ScalarValue::Float64(actual), ScalarValue::Float64(expected)) => {
                        assert_eq!(actual.map(f64::to_bits), expected.map(f64::to_bits));
                    }
                    (ScalarValue::List(actual), ScalarValue::List(expected)) => {
                        let bits = |array: &ListArray| {
                            array
                                .values()
                                .as_any()
                                .downcast_ref::<Float64Array>()
                                .unwrap()
                                .iter()
                                .map(|value| value.map(f64::to_bits))
                                .collect::<Vec<_>>()
                        };
                        assert_eq!(bits(actual), bits(expected));
                    }
                    _ => assert_eq!(actual, expected),
                }
            }
        }

        let config = config(&[0.5], 7);
        let palette = [
            Some(0.0),
            Some(f64::from_bits(0x7ff8_0000_0000_0002)),
            Some(f64::NEG_INFINITY),
            Some(-0.0),
            None,
            Some(f64::from_bits(0xfff8_0000_0000_0001)),
            Some(17.0),
            Some(f64::from_bits(0x7ff0_0000_0000_0001)),
            Some(f64::INFINITY),
            Some(f64::from_bits(0xfff8_0000_0000_0002)),
            Some(-31.0),
            Some(f64::from_bits(0x7ff8_0000_0000_0001)),
            Some(f64::from_bits(0xfff0_0000_0000_0001)),
        ];
        let values: ArrayRef = Arc::new(Float64Array::from_iter(
            std::iter::once(Some(9999.0))
                .chain((0..514).map(|row| palette[row % palette.len()]))
                .chain(std::iter::once(Some(-9999.0))),
        ));
        let mut scalar = SharedAccumulator::new(Arc::clone(&config), DataType::Float64);
        let mut grouped = SharedGroupsAccumulator::new(Arc::clone(&config), DataType::Float64);
        let mut expected = stock(&config, &DataType::Float64).remove(0);
        for start in [1, 258] {
            let batch = values.slice(start, 257);
            scalar.update_batch(&[Arc::clone(&batch)]).unwrap();
            grouped
                .update_batch(&[Arc::clone(&batch)], &[0; 257], None, 1)
                .unwrap();
            expected.update_batch(&[batch]).unwrap();
            assert_state_bits(&scalar.state().unwrap(), &expected.state().unwrap());
        }
        let grouped_state = grouped
            .state(EmitTo::All)
            .unwrap()
            .iter()
            .map(|array| ScalarValue::try_from_array(array, 0).unwrap())
            .collect::<Vec<_>>();
        assert_state_bits(&grouped_state, &expected.state().unwrap());
    }

    #[test]
    fn grouped_filters_emit_first_and_shifted_updates_match_stock() {
        let config = config(&[0.99, 0.0, 0.5, 1.0], DEFAULT_MAX_SIZE);
        for value_type in [DataType::Float32, DataType::Float64] {
            let mut actual = SharedGroupsAccumulator::new(Arc::clone(&config), value_type.clone());
            let mut expected: Vec<_> = (0..4).map(|_| stock(&config, &value_type)).collect();
            let values: ArrayRef = Arc::new(Float64Array::from(vec![
                Some(-9999.0),
                Some(10.0),
                Some(40.0),
                None,
                Some(999.0),
                Some(20.0),
                Some(-1.0),
                None,
                Some(9999.0),
            ]));
            let values = arrow::compute::cast(&values, &value_type)
                .unwrap()
                .slice(1, 7);
            let groups = [0, 1, 2, 3, 0, 1, 2];
            let filter = BooleanArray::from(vec![
                Some(false),
                Some(true),
                Some(true),
                Some(true),
                Some(false),
                Some(true),
                None,
                Some(true),
                Some(false),
            ])
            .slice(1, 7);
            actual
                .update_batch(&[Arc::clone(&values)], &groups, Some(&filter), 4)
                .unwrap();
            update_stock_groups(&mut expected, &values, &groups, Some(&filter));
            let first = actual.evaluate(EmitTo::First(1)).unwrap();
            let mut expected_first = expected.remove(0);
            assert_row(
                first.as_any().downcast_ref::<StructArray>().unwrap(),
                0,
                &mut expected_first,
            );

            let values: ArrayRef = Arc::new(Float64Array::from(vec![
                Some(-9999.0),
                Some(60.0),
                Some(80.0),
                None,
                Some(9999.0),
            ]));
            let values = arrow::compute::cast(&values, &value_type)
                .unwrap()
                .slice(1, 3);
            let groups = [0, 0, 1];
            actual
                .update_batch(&[Arc::clone(&values)], &groups, None, 3)
                .unwrap();
            update_stock_groups(&mut expected, &values, &groups, None);
            let result = actual.evaluate(EmitTo::All).unwrap();
            let result = result.as_any().downcast_ref::<StructArray>().unwrap();
            for (group, expected) in expected.iter_mut().enumerate() {
                assert_row(result, group, expected);
            }
            assert!(result.column(0).is_null(1));
            assert!(result.column(0).is_null(2));

            // Reuse after full release must not mutate either retained output.
            actual.update_batch(&[values], &groups, None, 2).unwrap();
            actual.evaluate(EmitTo::All).unwrap();
            assert_row(
                first.as_any().downcast_ref::<StructArray>().unwrap(),
                0,
                &mut expected_first,
            );
            for (group, expected) in expected.iter_mut().enumerate() {
                assert_row(result, group, expected);
            }
        }
    }

    #[test]
    fn grouped_state_prefix_and_partial_bypass_merge_match_stock() {
        let config = config(&[0.0, 0.5, 0.9, 1.0], 11);
        let mut partial = SharedGroupsAccumulator::new(Arc::clone(&config), DataType::Float64);
        let mut expected_partial: Vec<_> =
            (0..3).map(|_| stock(&config, &DataType::Float64)).collect();
        let values: ArrayRef = Arc::new(Float64Array::from_iter(
            (0..600).map(|i| (i % 13 != 0).then_some(((i * 59) % 997) as f64)),
        ));
        let groups: Vec<_> = (0..600).map(|i| i % 3).collect();
        partial
            .update_batch(&[Arc::clone(&values)], &groups, None, 3)
            .unwrap();
        update_stock_groups(&mut expected_partial, &values, &groups, None);
        let mut final_acc = SharedGroupsAccumulator::new(Arc::clone(&config), DataType::Float64);
        let mut expected_final: Vec<_> =
            (0..2).map(|_| stock(&config, &DataType::Float64)).collect();
        let prefix = partial.state(EmitTo::First(1)).unwrap();
        final_acc.merge_batch(&prefix, &[0], None, 2).unwrap();
        for (slot, acc) in expected_final[0].iter_mut().enumerate() {
            acc.merge_batch(&state_columns(vec![
                expected_partial[0][slot].state().unwrap(),
            ]))
            .unwrap();
        }
        // The retained groups were shifted by state(First), not discarded.
        let rest = partial.state(EmitTo::All).unwrap();
        final_acc.merge_batch(&rest, &[1, 0], None, 2).unwrap();
        for (source, target) in [(1, 1), (2, 0)] {
            for (slot, acc) in expected_final[target].iter_mut().enumerate() {
                acc.merge_batch(&state_columns(vec![
                    expected_partial[source][slot].state().unwrap(),
                ]))
                .unwrap();
            }
        }

        let values: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(1001.0),
            None,
            Some(-100.0),
            Some(42.0),
        ]));
        let filter = BooleanArray::from(vec![Some(true), Some(true), None, Some(false)]);
        let groups = [0, 1, 0, 1];
        let states = partial
            .convert_to_state(&[Arc::clone(&values)], Some(&filter))
            .unwrap();
        final_acc.merge_batch(&states, &groups, None, 2).unwrap();
        for group in 0..2 {
            for slot in 0..config.quantile_bits.len() {
                let mut rows = Vec::new();
                for (row, &target) in groups.iter().enumerate() {
                    if target != group {
                        continue;
                    }
                    let mut singleton = stock(&config, &DataType::Float64).remove(slot);
                    if filter.is_valid(row) && filter.value(row) {
                        singleton.update_batch(&[values.slice(row, 1)]).unwrap();
                    }
                    rows.push(singleton.state().unwrap());
                }
                expected_final[group][slot]
                    .merge_batch(&state_columns(rows))
                    .unwrap();
            }
        }
        let result = final_acc.evaluate(EmitTo::All).unwrap();
        let result = result.as_any().downcast_ref::<StructArray>().unwrap();
        for (group, expected) in expected_final.iter_mut().enumerate() {
            assert_row(result, group, expected);
        }
        // Retain both state batches across scratch release and a new update.
        partial
            .update_batch(&[values], &groups, Some(&filter), 2)
            .unwrap();
        partial.state(EmitTo::All).unwrap();
        for (arrays, row, source) in [(&prefix, 0, 0), (&rest, 0, 1), (&rest, 1, 2)] {
            let actual = arrays
                .iter()
                .map(|array| ScalarValue::try_from_array(array, row).unwrap())
                .collect::<Vec<_>>();
            assert_eq!(actual, expected_partial[source][0].state().unwrap());
        }
    }

    #[test]
    fn retained_memory_is_bounded_and_released_after_emission() {
        let config = config(&[0.5, 0.95, 0.99], 32);
        let mut acc = SharedGroupsAccumulator::new(config, DataType::Float64);
        let empty_size = acc.size();
        let values: ArrayRef =
            Arc::new(Float64Array::from_iter_values((0..1024).map(|i| i as f64)));
        let groups: Vec<_> = (0..1024).map(|i| i % 8).collect();
        for _ in 0..100 {
            acc.update_batch(&[Arc::clone(&values)], &groups, None, 8)
                .unwrap();
        }
        // State is bounded by centroids and batch scratch, not 102400 input values.
        assert!(acc.size() < empty_size + 128 * 1024);
        acc.state(EmitTo::All).unwrap();
        assert_eq!(acc.size(), empty_size);
    }

    #[test]
    fn configured_identity_roundtrip_and_invalid_payloads() {
        let original = SharedPercentiles::try_new(vec![1.0, -0.0, 0.5, 0.5], 37).unwrap();
        let encoded = original.encode_config().unwrap();
        let decoded = SharedPercentiles::decode_config(&encoded).unwrap();
        assert_eq!(original, decoded);
        assert_ne!(
            original,
            SharedPercentiles::try_new(vec![1.0, 0.0, 0.5, 0.5], 37).unwrap()
        );
        assert_ne!(
            original,
            SharedPercentiles::try_new(vec![1.0, -0.0, 0.5, 0.5], 38).unwrap()
        );
        for invalid in [Vec::new(), vec![f64::NAN], vec![-0.1], vec![1.1]] {
            assert!(SharedPercentiles::try_new(invalid, 37).is_err());
        }
        assert!(SharedPercentiles::try_new(vec![0.5], 0).is_err());
        assert!(SharedPercentiles::decode_config(&[]).is_err());
        assert!(SharedPercentiles::decode_config(&encoded[..encoded.len() - 1]).is_err());
        let mut unknown = encoded.clone();
        unknown[4] = 2;
        assert!(SharedPercentiles::decode_config(&unknown).is_err());
        let mut trailing = encoded.clone();
        trailing.push(0);
        assert!(SharedPercentiles::decode_config(&trailing).is_err());
        let mut invalid_quantile = encoded;
        invalid_quantile[17..25].copy_from_slice(&f64::NAN.to_bits().to_le_bytes());
        assert!(SharedPercentiles::decode_config(&invalid_quantile).is_err());
    }

    #[tokio::test]
    async fn distinct_configurations_and_integer_coercion_survive_planning() {
        let ctx = SessionContext::new();
        let batch = arrow::record_batch::RecordBatch::try_from_iter(vec![(
            "v",
            Arc::new(arrow::array::Int64Array::from(vec![1, 10, 20, 100])) as ArrayRef,
        )])
        .unwrap();
        let ascending = create_udaf(vec![0.0, 1.0], DEFAULT_MAX_SIZE).unwrap();
        let descending = create_udaf(vec![1.0, 0.0], DEFAULT_MAX_SIZE).unwrap();
        let decoded = Arc::new(AggregateUDF::from(
            SharedPercentiles::decode_config(
                &SharedPercentiles::try_new(vec![0.0, 1.0], DEFAULT_MAX_SIZE)
                    .unwrap()
                    .encode_config()
                    .unwrap(),
            )
            .unwrap(),
        ));
        let results = ctx
            .read_batch(batch)
            .unwrap()
            .aggregate(
                vec![],
                vec![
                    ascending.call(vec![col("v")]).alias("ascending"),
                    descending.call(vec![col("v")]).alias("descending"),
                    decoded.call(vec![col("v")]).alias("decoded"),
                ],
            )
            .unwrap()
            .collect()
            .await
            .unwrap();
        let result = &results[0];
        for (column, expected) in [(0, [1.0, 100.0]), (1, [100.0, 1.0]), (2, [1.0, 100.0])] {
            let values = result
                .column(column)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            for (slot, expected) in expected.into_iter().enumerate() {
                assert_eq!(
                    ScalarValue::try_from_array(values.column(slot), 0).unwrap(),
                    ScalarValue::Float64(Some(expected))
                );
            }
        }
    }
}
