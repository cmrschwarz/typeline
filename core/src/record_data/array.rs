use metamatch::metamatch;
use num::{BigInt, BigRational};

use crate::{
    cli::call_expr::Argument,
    operators::errors::OperatorApplicationError,
    record_data::field_value_ref::FieldValueRefMut,
    utils::{force_cast, temp_vec::convert_vec_cleared},
};

use super::{
    custom_data::CustomDataBox,
    field_data::{FieldValueRepr, FixedSizeFieldValueType},
    field_value::{FieldReference, FieldValue, Object, SlicedFieldReference},
    field_value_ref::FieldValueRef,
    stream_value::StreamValueId,
};

#[derive(Debug, PartialEq, Clone)]
pub enum Array {
    Undefined(usize),
    Null(usize),
    Int(Vec<i64>),
    BigInt(Vec<BigInt>),
    Float(Vec<f64>),
    BigRational(Vec<BigRational>),
    Bytes(Vec<Vec<u8>>),
    Text(Vec<String>),
    Error(Vec<OperatorApplicationError>),
    Array(Vec<Array>),
    Object(Vec<Object>),
    StreamValueId(Vec<StreamValueId>),
    Argument(Vec<Argument>),
    FieldReference(Vec<FieldReference>),
    SlicedFieldReference(Vec<SlicedFieldReference>),
    Custom(Vec<CustomDataBox>),
    Mixed(Vec<FieldValue>),
}

impl Default for Array {
    fn default() -> Self {
        Array::Undefined(0)
    }
}

impl Array {
    pub fn len(&self) -> usize {
        metamatch!(match self {
            Array::Null(len) | Array::Undefined(len) => *len,
            #[expand(T in [
                Int, BigInt, BigRational, Bytes, Text, Error, Array, Object,
                FieldReference, SlicedFieldReference, Custom, Mixed, Float,
                StreamValueId, Argument
            ])]
            Array::T(a) => a.len(),
        })
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// returns the present variant even if the array is empty
    pub fn repr_raw(&self) -> Option<FieldValueRepr> {
        Some(metamatch!(match self {
            #[expand(T in [
                Null, Undefined,
                Int, BigInt, BigRational, Error, Array, Object,
                FieldReference, SlicedFieldReference, Custom, Float,
                StreamValueId, Argument
            ])]
            Array::T(_) => FieldValueRepr::T,
            Array::Bytes(_) => FieldValueRepr::BytesBuffer,
            Array::Text(_) => FieldValueRepr::TextBuffer,
            Array::Mixed(_) => return None,
        }))
    }
    /// returns None if the Array is empty or the `Mixed` variant is present
    pub fn repr(&self) -> Option<FieldValueRepr> {
        if self.is_empty() {
            return None;
        }
        self.repr_raw()
    }

    pub fn into_cleared_vec<T>(self) -> Vec<T> {
        metamatch!(match self {
            Array::Null(_) | Array::Undefined(_) => Vec::new(),
            #[expand(T in [
                Int, BigInt, BigRational, Error, Array, Object,
                FieldReference, SlicedFieldReference, Custom, Float,
                StreamValueId, Argument, Mixed, Text, Bytes,
            ])]
            Array::T(v) => convert_vec_cleared(v),
        })
    }
    pub fn convert_cleared(&mut self, repr: FieldValueRepr) {
        let arr = std::mem::take(self);
        *self = metamatch!(match repr {
            FieldValueRepr::Undefined => Array::Undefined(0),
            FieldValueRepr::Null => Array::Null(0),
            FieldValueRepr::TextBuffer | FieldValueRepr::TextInline => {
                Array::Text(arr.into_cleared_vec())
            }
            FieldValueRepr::BytesBuffer | FieldValueRepr::BytesInline => {
                Array::Bytes(arr.into_cleared_vec())
            }
            #[expand(T in [
                Int, BigInt, BigRational, Error, Array, Object,
                FieldReference, SlicedFieldReference, Custom, Float,
                StreamValueId, Argument,
            ])]
            FieldValueRepr::T => Array::T(arr.into_cleared_vec()),
        })
    }

    pub fn make_mixed(&mut self) -> &mut Vec<FieldValue> {
        *self = Array::Mixed(metamatch!(match std::mem::take(self) {
            Array::Mixed(v) => v,

            #[expand(T in [
                Int, Error, Array, Object,
                FieldReference, SlicedFieldReference, Custom, Float,
                StreamValueId, Text, Bytes,
            ])]
            Array::T(a) => a.into_iter().map(FieldValue::T).collect(),

            #[expand(ZST_T in [Null, Undefined])]
            Array::ZST_T(n) => {
                std::iter::repeat(FieldValue::ZST_T).take(n).collect()
            }

            #[expand(BOX_T in [
                BigInt, BigRational, Argument
            ])]
            Array::BOX_T(a) => {
                a.into_iter()
                    .map(|v| FieldValue::BOX_T(Box::new(v)))
                    .collect()
            }
        }));
        let Array::Mixed(mixed) = self else {
            unreachable!()
        };
        mixed
    }
    pub fn push_raw(&mut self, value: FieldValue) {
        metamatch!(match (self, value) {
            #[expand_pattern(ZST_T in [Null, Undefined])]
            (Array::ZST_T(n), FieldValue::ZST_T) => {
                *n += 1;
            }

            #[expand(T in [
                Int, Error, Array, Object,
                FieldReference, SlicedFieldReference, Custom, Float,
                StreamValueId, Text, Bytes,
            ])]
            (Array::T(a), FieldValue::T(v)) => {
                a.push(v);
            }

            #[expand(BOX_T in [BigInt, BigRational, Argument])]
            (Array::BOX_T(a), FieldValue::BOX_T(v)) => {
                a.push(*v);
            }

            (Array::Mixed(a), value) => {
                a.push(value);
            }

            (arr, value) => {
                debug_assert!(arr.repr() != Some(value.repr()));
                arr.make_mixed().push(value);
            }
        })
    }
    pub fn extend_raw<T: FixedSizeFieldValueType>(
        &mut self,
        iter: impl Iterator<Item = T>,
    ) {
        // SAFETY: We *know* that `T` and `Q` will be *identical* because of
        // the check on `T::REPR`. `FixedSizeFieldValueType` is an
        // unsafe trait, so assuming that nobody gave us an incorrect
        // `REPR` is sound.
        metamatch!(match self {
            #[expand(ZST_T in [Null, Undefined])]
            Array::ZST_T(n) => {
                if T::REPR == FieldValueRepr::ZST_T {
                    *n += iter.count();
                    return;
                }
            }

            #[expand((ARRAY_T, REPR_T, VALUE_T) in [
                (Int, Int, i64),
                (Float, Float, f64),
                (Text, TextBuffer, String),
                (Bytes, BytesBuffer, Vec<u8>),
                (Array, Array, Array),
                (Object, Object, Object),
                (Error, Error, OperatorApplicationError),
                (FieldReference, FieldReference, FieldReference),
                (SlicedFieldReference, SlicedFieldReference, SlicedFieldReference),
                (Custom, Custom, CustomDataBox),
                (StreamValueId, StreamValueId, StreamValueId),
            ])]
            Array::ARRAY_T(a) => {
                if T::REPR == FieldValueRepr::REPR_T {
                    debug_assert!(!T::FIELD_VALUE_BOXED);
                    a.extend(
                        iter.map(|v| unsafe { force_cast::<T, VALUE_T>(v) }),
                    );
                    return;
                }
            }

            #[expand(BOX_T in [BigInt, BigRational, Argument])]
            Array::BOX_T(a) => {
                if T::REPR == FieldValueRepr::BOX_T {
                    debug_assert!(T::FIELD_VALUE_BOXED);
                    a.extend(
                        iter.map(|v| unsafe {
                            *force_cast::<T, Box<BOX_T>>(v)
                        }),
                    );
                    return;
                }
            }
            Array::Mixed(a) => {
                a.extend(iter.map(|v| FieldValue::from_fixed_sized_type(v)));
                return;
            }
        });

        self.make_mixed()
            .extend(iter.map(|v| FieldValue::from_fixed_sized_type(v)));
    }
    pub fn canonicalize_for_repr(&mut self, repr: FieldValueRepr) {
        if self.is_empty() {
            self.convert_cleared(repr);
            return;
        }
        let len = self.len();
        if let Array::Mixed(m) = self {
            if len == 1 && repr == m[0].repr() {
                let v0 = m.pop().unwrap();
                self.convert_cleared(repr);
                self.push_raw(v0);
            }
        }
    }
    pub fn canonicalize(&mut self) {
        if self.is_empty() {
            self.convert_cleared(FieldValueRepr::Undefined);
            return;
        }
        if self.len() == 1 {
            if let Array::Mixed(m) = self {
                let v0 = m.pop().unwrap();
                self.convert_cleared(v0.repr());
                self.push_raw(v0);
            }
        }
    }
    pub fn extend<T: FixedSizeFieldValueType>(
        &mut self,
        iter: impl Iterator<Item = T>,
    ) {
        self.canonicalize_for_repr(T::REPR);
        self.extend_raw(iter.into_iter());
    }
    pub fn extend_from_field_value(
        &mut self,
        mut iter: impl Iterator<Item = FieldValue>,
    ) {
        let Some(first) = iter.next() else {
            return;
        };
        self.push(first);
        for v in iter {
            self.push_raw(v);
        }
    }

    pub fn push(&mut self, value: FieldValue) {
        self.canonicalize_for_repr(value.repr());
        self.push_raw(value);
    }
    pub fn pop(&mut self) -> Option<FieldValue> {
        Some(metamatch!(match self {
            #[expand(ZST_T in [Null, Undefined])]
            Array::ZST_T(n) => {
                if *n == 0 {
                    return None;
                }
                *n -= 1;
                FieldValue::ZST_T
            }

            #[expand(T in [
                Int, Error, Array, Object,
                FieldReference, SlicedFieldReference, Custom, Float,
                StreamValueId, Text, Bytes,
            ])]
            Array::T(v) => FieldValue::T(v.pop()?),

            #[expand(BOX_T in [BigInt, BigRational, Argument])]
            Array::BOX_T(v) => FieldValue::BOX_T(Box::new(v.pop()?)),

            Array::Mixed(v) => return v.pop(),
        }))
    }
    pub fn get(&self, index: usize) -> Option<FieldValueRef> {
        metamatch!(match self {
            #[expand(ZST_T in [Null, Undefined])]
            Array::ZST_T(len) => {
                if *len <= index {
                    return None;
                }
                Some(FieldValueRef::ZST_T)
            }
            #[expand(T in [
                Int, Error, Array, Object,
                FieldReference, SlicedFieldReference, Custom, Float,
                StreamValueId, BigInt, BigRational, Argument
            ])]
            Array::T(v) => {
                v.get(index).map(FieldValueRef::T)
            }

            #[expand(T in [Text, Bytes])]
            Array::T(v) => {
                v.get(index).map(|v| FieldValueRef::T(v))
            }

            Array::Mixed(v) => v.get(index).map(|v| v.as_ref()),
        })
    }
    pub fn get_mut(&mut self, index: usize) -> Option<FieldValueRefMut> {
        metamatch!(match self {
            #[expand(ZST_T in [Null, Undefined])]
            Array::ZST_T(len) => {
                if *len <= index {
                    return None;
                }
                Some(FieldValueRefMut::ZST_T)
            }
            #[expand(T in [
                Int, Error, Array, Object,
                FieldReference, SlicedFieldReference, Custom, Float,
                StreamValueId, BigInt, BigRational, Argument
            ])]
            Array::T(v) => {
                v.get_mut(index).map(FieldValueRefMut::T)
            }

            #[expand((REPR_T, REF_T) in [
                (Text, TextBuffer),
                (Bytes, BytesBuffer)
            ])]
            Array::REPR_T(v) => {
                v.get_mut(index).map(FieldValueRefMut::REF_T)
            }

            Array::Mixed(v) => v.get_mut(index).map(|v| v.as_ref_mut()),
        })
    }
}
