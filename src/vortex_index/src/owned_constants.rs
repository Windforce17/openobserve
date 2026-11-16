// Copyright 2026 OpenObserve Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Detach constant scalars from the canonical buffers used during compression.

use vortex::{
    array::{ArrayRef, IntoArray, arrays::ConstantArray},
    buffer::ByteBuffer,
    error::VortexResult,
    scalar::{Scalar, ScalarValue},
};

/// Preserve the physical encoding tree, but own each string/binary constant scalar.
/// In particular, an inlined string returned by VarBinView::execute_scalar is a
/// slice of the *whole* views allocation. A Masked(Constant) waiting for ordered
/// segment emission must not keep that allocation alive after its CPU task ends.
/// `nbytes()` cannot detect this: scalar storage is not an array buffer, and a
/// sliced buffer's visible length does not reveal its backing allocation size.
pub(crate) fn detach_constant_scalars(array: ArrayRef) -> VortexResult<ArrayRef> {
    if let Some(scalar) = array.as_constant() {
        let value = match scalar.value() {
            Some(ScalarValue::Utf8(value)) => ScalarValue::Utf8(value.as_str().into()),
            Some(ScalarValue::Binary(value)) => ScalarValue::Binary(ByteBuffer::copy_from(value)),
            _ => return Ok(array),
        };
        let scalar = Scalar::try_new(scalar.dtype().clone(), Some(value))?;
        return Ok(ConstantArray::new(scalar, array.len()).into_array());
    }
    let mut changed = false;
    let slots = array
        .slots()
        .iter()
        .map(|slot| {
            slot.as_ref()
                .map(|child| {
                    let owned = detach_constant_scalars(child.clone())?;
                    changed |= !ArrayRef::ptr_eq(child, &owned);
                    Ok(owned)
                })
                .transpose()
        })
        .collect::<VortexResult<_>>()?;
    if !changed {
        return Ok(array);
    }
    // SAFETY: only scalar backing storage changed. Every slot retains its
    // presence, dtype, length, values and encoding, including masked validity.
    unsafe { array.with_slots(slots) }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use vortex::{
        VortexSessionDefault,
        array::{
            VortexSessionExecute,
            arrays::{MaskedArray, VarBinViewArray},
            validity::Validity,
        },
        buffer::BufferString,
        compressor::BtrBlocksCompressorBuilder,
        dtype::Nullability,
        session::VortexSession,
    };

    use super::*;

    struct Owner {
        bytes: Vec<u8>,
        dropped: Arc<AtomicBool>,
    }
    impl AsRef<[u8]> for Owner {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }
    impl Drop for Owner {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    #[test]
    fn constant_and_masked_constant_release_large_scalar_owner() {
        for binary in [false, true] {
            for masked in [false, true] {
                let dropped = Arc::new(AtomicBool::new(false));
                let bytes = bytes::Bytes::from_owner(Owner {
                    bytes: vec![b'x'; 2 << 20],
                    dropped: dropped.clone(),
                });
                let buffer = ByteBuffer::from(bytes).slice(0..3);
                let scalar = if binary {
                    Scalar::binary(buffer, Nullability::NonNullable)
                } else {
                    Scalar::utf8(
                        BufferString::try_from(buffer).unwrap(),
                        Nullability::NonNullable,
                    )
                };
                let mut array = ConstantArray::new(scalar, 128 * 1024).into_array();
                let session = VortexSession::default();
                let mut ctx = session.create_execution_ctx();
                if masked {
                    array = MaskedArray::try_new(array, Validity::AllValid, &mut ctx)
                        .unwrap()
                        .into_array();
                }
                let owned = detach_constant_scalars(array).unwrap();
                assert!(
                    dropped.load(Ordering::SeqCst),
                    "finished constant retained a 2 MiB owner"
                );
                assert_eq!(owned.len(), 128 * 1024);
                assert_eq!(
                    owned.execute_scalar(0, &mut ctx).unwrap().value().unwrap(),
                    &if binary {
                        ScalarValue::Binary(ByteBuffer::copy_from(b"xxx"))
                    } else {
                        ScalarValue::Utf8("xxx".into())
                    }
                );
            }
        }
    }

    #[test]
    fn compressed_mostly_null_string_does_not_borrow_views() {
        let session = VortexSession::default();
        let mut ctx = session.create_execution_ctx();
        let input = VarBinViewArray::from_iter(
            (0..128 * 1024).map(|row| (row == 123).then_some("short")),
            vortex::dtype::DType::Utf8(Nullability::Nullable),
        )
        .into_array();
        let ranges: Vec<_> = input
            .buffers()
            .iter()
            .map(|b| {
                let start = b.as_ptr() as usize;
                start..start + b.len()
            })
            .collect();
        let compressed = BtrBlocksCompressorBuilder::from_session(&session)
            .with_compact()
            .build()
            .compress(&input, &mut ctx)
            .unwrap();
        let out = detach_constant_scalars(compressed).unwrap();
        let mut constants = 0;
        for node in out.depth_first_traversal() {
            if let Some(scalar) = node.as_constant()
                && let Some(ScalarValue::Utf8(value)) = scalar.value()
            {
                constants += 1;
                let ptr = value.inner().as_ptr() as usize;
                assert!(
                    ranges.iter().all(|r| !r.contains(&ptr)),
                    "constant borrows canonical views"
                );
            }
        }
        assert!(constants > 0, "fixture must compress to a string constant");
        assert_eq!(
            out.execute_scalar(123, &mut ctx).unwrap(),
            input.execute_scalar(123, &mut ctx).unwrap()
        );
        assert!(out.execute_scalar(124, &mut ctx).unwrap().is_null());
    }
}
