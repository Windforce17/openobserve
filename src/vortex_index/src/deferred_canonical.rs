// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! A slice-guard obligation, not a wire encoding or a canonical-array cache.
//!
//! Scanning a wide struct must not materialize every column before the writer
//! can consume even the first one. Carry the encoded source through the queue;
//! the clustered writer resolves each obligation before taking its accounting
//! snapshot, preserving the eager path's coalescing and compression decisions.

use vortex::{
    array::{
        Array, ArrayId, ArrayParts, ArrayRef, ArrayView, EmptyArrayData, ExecutionCtx,
        ExecutionResult, IntoArray, OperationsVTable, VTable, ValidityVTable, buffer::BufferHandle,
        serde::ArrayChildren, validity::Validity, with_empty_buffers,
    },
    dtype::DType,
    error::{VortexResult, vortex_bail, vortex_ensure},
    session::{VortexSession, registry::CachedId},
};

#[derive(Clone, Debug)]
pub(crate) struct DeferredCanonical;

impl DeferredCanonical {
    pub(crate) fn wrap(source: ArrayRef) -> VortexResult<ArrayRef> {
        Array::try_from_parts(ArrayParts::new(
            Self,
            source.dtype().clone(),
            source.len(),
            EmptyArrayData,
            [Some(source)].into_iter().collect(),
        ))
        .map(IntoArray::into_array)
    }

    pub(crate) fn source(array: &ArrayRef) -> Option<&ArrayRef> {
        array.is::<Self>().then(|| {
            array.slots()[0]
                .as_ref()
                .expect("validated deferred canonical source")
        })
    }
}

impl VTable for DeferredCanonical {
    type TypedArrayData = EmptyArrayData;
    type OperationsVTable = Self;
    type ValidityVTable = Self;

    fn id(&self) -> ArrayId {
        static ID: CachedId = CachedId::new("openobserve.deferred_canonical");
        *ID
    }

    fn validate(
        &self,
        _data: &EmptyArrayData,
        dtype: &DType,
        len: usize,
        slots: &[Option<ArrayRef>],
    ) -> VortexResult<()> {
        vortex_ensure!(slots.len() == 1, "expected one deferred source");
        let source = slots[0].as_ref();
        vortex_ensure!(
            source.is_some_and(|source| source.dtype() == dtype && source.len() == len),
            "deferred source dtype or length mismatch"
        );
        Ok(())
    }

    fn nbuffers(_array: ArrayView<'_, Self>) -> usize {
        0
    }

    fn buffer(_array: ArrayView<'_, Self>, _idx: usize) -> BufferHandle {
        unreachable!("deferred canonical has no buffers")
    }

    fn buffer_name(_array: ArrayView<'_, Self>, _idx: usize) -> Option<String> {
        None
    }

    fn with_buffers(
        &self,
        array: ArrayView<'_, Self>,
        buffers: &[BufferHandle],
    ) -> VortexResult<ArrayParts<Self>> {
        with_empty_buffers(self, array, buffers)
    }

    fn slot_name(_array: ArrayView<'_, Self>, _idx: usize) -> String {
        "source".into()
    }

    fn serialize(
        _array: ArrayView<'_, Self>,
        _session: &VortexSession,
    ) -> VortexResult<Option<Vec<u8>>> {
        Ok(None)
    }

    fn deserialize(
        &self,
        _dtype: &DType,
        _len: usize,
        _metadata: &[u8],
        _buffers: &[BufferHandle],
        _children: &dyn ArrayChildren,
        _session: &VortexSession,
    ) -> VortexResult<ArrayParts<Self>> {
        vortex_bail!("deferred canonical is a runtime-only array")
    }

    fn execute(array: Array<Self>, _ctx: &mut ExecutionCtx) -> VortexResult<ExecutionResult> {
        // Do not cache the expanded result in the wrapper: the queued struct
        // remains alive while its later columns wait for leaf admission.
        Ok(ExecutionResult::done(
            array.slots()[0].as_ref().expect("validated source").clone(),
        ))
    }
}

impl OperationsVTable<DeferredCanonical> for DeferredCanonical {
    type ProbeState = ();

    fn scalar_at(
        array: ArrayView<'_, Self>,
        index: usize,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<vortex::scalar::Scalar> {
        array.slots()[0]
            .as_ref()
            .expect("validated source")
            .execute_scalar(index, ctx)
    }
}

impl ValidityVTable<DeferredCanonical> for DeferredCanonical {
    fn validity(array: ArrayView<'_, Self>) -> VortexResult<Validity> {
        array.slots()[0]
            .as_ref()
            .expect("validated source")
            .validity()
    }
}

#[cfg(test)]
mod tests {
    use vortex::{
        VortexSessionDefault,
        array::{
            Canonical, VortexSessionExecute,
            arrays::{ConstantArray, PrimitiveArray, VarBinViewArray},
        },
        dtype::Nullability,
        encodings::sparse::Sparse,
        scalar::Scalar,
    };

    use super::*;

    #[test]
    fn deferred_constant_and_sparse_windows_retain_only_encoded_storage() {
        let session = VortexSession::default();
        let mut ctx = session.create_execution_ctx();
        let null = Scalar::null(DType::Utf8(Nullability::Nullable));
        let sparse = Sparse::try_new(
            PrimitiveArray::from_iter([7u32, 16391]).into_array(),
            VarBinViewArray::from_iter(
                [Some("rare"), Some("a longer patch value")],
                DType::Utf8(Nullability::Nullable),
            )
            .into_array(),
            32768,
            null.clone(),
            &mut ctx,
        )
        .unwrap()
        .into_array();
        for source in [
            ConstantArray::new(null, 32768).into_array(),
            ConstantArray::new(Scalar::utf8("constant", Nullability::Nullable), 32768).into_array(),
            sparse.clone(),
            sparse.slice(3..20000).unwrap(),
        ] {
            let deferred = DeferredCanonical::wrap(source.clone()).unwrap();
            assert!(ArrayRef::ptr_eq(
                DeferredCanonical::source(&deferred).unwrap(),
                &source
            ));
            assert_eq!(deferred.nbytes(), source.nbytes());
            assert!(deferred.nbytes() < 4096, "queued window expanded eagerly");
            assert!(!crate::container::is_ctx_serializable(&deferred));
            let baseline = source.execute::<Canonical>(&mut ctx).unwrap().into_array();
            let actual = deferred
                .clone()
                .execute::<Canonical>(&mut ctx)
                .unwrap()
                .into_array();
            assert_eq!(actual.nbytes(), baseline.nbytes());
            for index in 0..actual.len() {
                assert_eq!(
                    actual.execute_scalar(index, &mut ctx).unwrap(),
                    baseline.execute_scalar(index, &mut ctx).unwrap()
                );
            }
            // Execution must not install a dense cache on the queued source.
            assert!(deferred.nbytes() < 4096);
        }
    }

    #[test]
    fn deferred_source_metadata_is_validated() {
        let dtype = DType::Utf8(Nullability::Nullable);
        let source = ConstantArray::new(Scalar::null(dtype.clone()), 8).into_array();
        assert!(
            DeferredCanonical
                .validate(&EmptyArrayData, &dtype, 8, &[])
                .is_err()
        );
        assert!(
            DeferredCanonical
                .validate(&EmptyArrayData, &dtype, 8, &[None])
                .is_err()
        );
        assert!(
            DeferredCanonical
                .validate(&EmptyArrayData, &dtype, 9, &[Some(source.clone())])
                .is_err()
        );
        assert!(
            DeferredCanonical
                .validate(
                    &EmptyArrayData,
                    &DType::Binary(Nullability::Nullable),
                    8,
                    &[Some(source)]
                )
                .is_err()
        );
    }
}
