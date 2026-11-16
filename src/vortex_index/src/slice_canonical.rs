// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Share null materializations within one docs scan window, without changing its shape.

use std::collections::HashMap;

use vortex::{
    array::{ArrayRef, Canonical, ExecutionCtx, IntoArray},
    dtype::DType,
    error::VortexResult,
};

/// The slice guard must still return canonical columns: their encoding and nbytes
/// determine coalescing and stripe boundaries downstream. Equal all-null constants
/// can share that representation instead of allocating one row buffer per column.
/// Keep this cache local to one window, so it cannot retain buffers across a scan.
#[derive(Default)]
pub(crate) struct SliceCanonicalizer {
    nulls: HashMap<(DType, usize), ArrayRef>,
}

impl SliceCanonicalizer {
    pub(crate) fn canonicalize(
        &mut self,
        field: ArrayRef,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<ArrayRef> {
        if field.as_constant().is_some_and(|scalar| scalar.is_null()) {
            let key = (field.dtype().clone(), field.len());
            if let Some(canonical) = self.nulls.get(&key) {
                return Ok(canonical.clone());
            }
            let canonical = field.execute::<Canonical>(ctx)?.into_array();
            self.nulls.insert(key, canonical.clone());
            return Ok(canonical);
        }
        Ok(field.execute::<Canonical>(ctx)?.into_array())
    }
}

#[cfg(test)]
mod tests {
    use vortex::{
        VortexSessionDefault,
        array::{VortexSessionExecute, arrays::ConstantArray},
        dtype::{Nullability, PType},
        scalar::Scalar,
        session::VortexSession,
    };

    use super::*;

    fn wire_bytes(array: &ArrayRef, session: &VortexSession) -> Vec<u8> {
        array
            .serialize(
                &vortex::array::ArrayContext::empty(),
                session,
                &vortex::array::serde::SerializeOptions::default(),
            )
            .unwrap()
            .into_iter()
            .flat_map(|buffer| buffer.as_ref().to_vec())
            .collect()
    }

    #[test]
    fn null_columns_share_materialization_without_changing_wire_bytes() {
        let session = VortexSession::default();
        let mut ctx = session.create_execution_ctx();
        let mut cache = SliceCanonicalizer::default();
        for dtype in [
            DType::Utf8(Nullability::Nullable),
            DType::Binary(Nullability::Nullable),
            DType::Primitive(PType::I64, Nullability::Nullable),
            DType::Bool(Nullability::Nullable),
        ] {
            let input = ConstantArray::new(Scalar::null(dtype), 8192).into_array();
            let baseline = input
                .clone()
                .execute::<Canonical>(&mut ctx)
                .unwrap()
                .into_array();
            let first = cache.canonicalize(input.clone(), &mut ctx).unwrap();
            let second = cache.canonicalize(input, &mut ctx).unwrap();
            assert!(
                ArrayRef::ptr_eq(&first, &second),
                "duplicate null row storage"
            );
            assert_eq!(
                first.nbytes(),
                baseline.nbytes(),
                "coalescing charge changed"
            );
            assert_eq!(
                wire_bytes(&first, &session),
                wire_bytes(&baseline, &session)
            );
        }
    }

    #[test]
    fn null_cache_is_scoped_by_dtype_length_and_window() {
        let session = VortexSession::default();
        let mut ctx = session.create_execution_ctx();
        let mut cache = SliceCanonicalizer::default();
        let utf8 = DType::Utf8(Nullability::Nullable);
        let input = ConstantArray::new(Scalar::null(utf8.clone()), 8192).into_array();
        let first = cache.canonicalize(input.clone(), &mut ctx).unwrap();
        let shorter = cache
            .canonicalize(
                ConstantArray::new(Scalar::null(utf8), 4096).into_array(),
                &mut ctx,
            )
            .unwrap();
        assert_eq!(shorter.len(), 4096);
        assert!(!ArrayRef::ptr_eq(&first, &shorter));
        let binary = cache
            .canonicalize(
                ConstantArray::new(Scalar::null(DType::Binary(Nullability::Nullable)), 8192)
                    .into_array(),
                &mut ctx,
            )
            .unwrap();
        assert_eq!(binary.dtype(), &DType::Binary(Nullability::Nullable));
        assert!(!ArrayRef::ptr_eq(&first, &binary));
        drop(cache);
        let next_window = SliceCanonicalizer::default()
            .canonicalize(input, &mut ctx)
            .unwrap();
        assert!(!ArrayRef::ptr_eq(&first, &next_window));
    }

    #[test]
    fn non_null_constants_are_not_replaced_by_cached_nulls() {
        let session = VortexSession::default();
        let mut ctx = session.create_execution_ctx();
        let mut cache = SliceCanonicalizer::default();
        cache
            .canonicalize(
                ConstantArray::new(Scalar::null(DType::Utf8(Nullability::Nullable)), 8192)
                    .into_array(),
                &mut ctx,
            )
            .unwrap();
        let input =
            ConstantArray::new(Scalar::utf8("present", Nullability::Nullable), 8192).into_array();
        let baseline = input
            .clone()
            .execute::<Canonical>(&mut ctx)
            .unwrap()
            .into_array();
        let actual = cache.canonicalize(input, &mut ctx).unwrap();
        assert_eq!(
            actual
                .execute_scalar(0, &mut ctx)
                .unwrap()
                .as_utf8()
                .value()
                .map(|value| value.as_str()),
            Some("present")
        );
        assert_eq!(
            wire_bytes(&actual, &session),
            wire_bytes(&baseline, &session)
        );
    }
}
