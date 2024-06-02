use num::{BigInt, BigRational};

use crate::{
    operators::errors::OperatorApplicationError,
    utils::temp_vec::convert_vec_cleared,
};

use super::{
    custom_data::CustomDataBox,
    field_data::{FieldValueRepr, FixedSizeFieldValueType},
    field_value::{FieldReference, FieldValue, Object, SlicedFieldReference},
    stream_value::StreamValueId,
};

#[derive(Debug, PartialEq, Clone)]
pub enum Array {
    Undefined(usize),
    Null(usize),
    Int(Vec<i64>),
    BigInt(Vec<BigInt>),
    Float(Vec<f64>),
    Rational(Vec<BigRational>),
    Bytes(Vec<Vec<u8>>),
    Text(Vec<String>),
    Error(Vec<OperatorApplicationError>),
    Array(Vec<Array>),
    Object(Vec<Object>),
    StreamValueId(Vec<StreamValueId>),
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
        match self {
            Array::Null(len) | Array::Undefined(len) => *len,
            Array::Int(a) => a.len(),
            Array::BigInt(a) => a.len(),
            Array::Rational(a) => a.len(),
            Array::Bytes(a) => a.len(),
            Array::Text(a) => a.len(),
            Array::Error(a) => a.len(),
            Array::Array(a) => a.len(),
            Array::Object(a) => a.len(),
            Array::FieldReference(a) => a.len(),
            Array::SlicedFieldReference(a) => a.len(),
            Array::Custom(a) => a.len(),
            Array::Mixed(a) => a.len(),
            Array::Float(a) => a.len(),
            Array::StreamValueId(a) => a.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// returns the present variant even if the array is empty
    pub fn repr_raw(&self) -> Option<FieldValueRepr> {
        Some(match self {
            Array::Null(_) => FieldValueRepr::Null,
            Array::Undefined(_) => FieldValueRepr::Undefined,
            Array::Int(_) => FieldValueRepr::Int,
            Array::BigInt(_) => FieldValueRepr::BigInt,
            Array::Rational(_) => FieldValueRepr::Rational,
            Array::Bytes(_) => FieldValueRepr::BytesBuffer,
            Array::Text(_) => FieldValueRepr::TextBuffer,
            Array::Error(_) => FieldValueRepr::Error,
            Array::Array(_) => FieldValueRepr::Array,
            Array::Object(_) => FieldValueRepr::Object,
            Array::FieldReference(_) => FieldValueRepr::FieldReference,
            Array::SlicedFieldReference(_) => {
                FieldValueRepr::SlicedFieldReference
            }
            Array::Custom(_) => FieldValueRepr::Custom,
            Array::Float(_) => FieldValueRepr::Float,
            Array::StreamValueId(_) => FieldValueRepr::StreamValueId,
            Array::Mixed(_) => return None,
        })
    }
    /// returns None if the Array is empty or the `Mixed` variant is present
    pub fn repr(&self) -> Option<FieldValueRepr> {
        if self.is_empty() {
            return None;
        }
        self.repr_raw()
    }

    pub fn into_cleared_vec<T>(self) -> Vec<T> {
        match self {
            Array::Null(_) | Array::Undefined(_) => Vec::new(),
            Array::Int(v) => convert_vec_cleared(v),
            Array::BigInt(v) => convert_vec_cleared(v),
            Array::Float(v) => convert_vec_cleared(v),
            Array::Rational(v) => convert_vec_cleared(v),
            Array::Bytes(v) => convert_vec_cleared(v),
            Array::Text(v) => convert_vec_cleared(v),
            Array::Error(v) => convert_vec_cleared(v),
            Array::Array(v) => convert_vec_cleared(v),
            Array::Object(v) => convert_vec_cleared(v),
            Array::StreamValueId(v) => convert_vec_cleared(v),
            Array::FieldReference(v) => convert_vec_cleared(v),
            Array::SlicedFieldReference(v) => convert_vec_cleared(v),
            Array::Custom(v) => convert_vec_cleared(v),
            Array::Mixed(v) => convert_vec_cleared(v),
        }
    }
    pub fn convert_cleared(&mut self, repr: FieldValueRepr) {
        let arr = std::mem::take(self);
        *self = match repr {
            FieldValueRepr::Undefined => Array::Undefined(1),
            FieldValueRepr::Null => Array::Null(1),
            FieldValueRepr::Int => Array::Int(arr.into_cleared_vec()),
            FieldValueRepr::BigInt => Array::BigInt(arr.into_cleared_vec()),
            FieldValueRepr::Float => Array::Float(arr.into_cleared_vec()),
            FieldValueRepr::Rational => {
                Array::Rational(arr.into_cleared_vec())
            }
            FieldValueRepr::TextBuffer
            | FieldValueRepr::TextInline
            | FieldValueRepr::TextFile => Array::Text(arr.into_cleared_vec()),
            FieldValueRepr::BytesBuffer
            | FieldValueRepr::BytesInline
            | FieldValueRepr::BytesFile => {
                Array::Bytes(arr.into_cleared_vec())
            }
            FieldValueRepr::Array => Array::Array(arr.into_cleared_vec()),
            FieldValueRepr::Object => Array::Object(arr.into_cleared_vec()),
            FieldValueRepr::Custom => Array::Custom(arr.into_cleared_vec()),
            FieldValueRepr::Error => Array::Error(arr.into_cleared_vec()),
            FieldValueRepr::StreamValueId => {
                Array::StreamValueId(arr.into_cleared_vec())
            }
            FieldValueRepr::FieldReference => {
                Array::FieldReference(arr.into_cleared_vec())
            }
            FieldValueRepr::SlicedFieldReference => {
                Array::SlicedFieldReference(arr.into_cleared_vec())
            }
        }
    }

    pub fn make_mixed(&mut self) -> &mut Vec<FieldValue> {
        *self = Array::Mixed(match std::mem::take(self) {
            Array::Undefined(n) => {
                std::iter::repeat(FieldValue::Undefined).take(n).collect()
            }
            Array::Null(n) => {
                std::iter::repeat(FieldValue::Null).take(n).collect()
            }
            Array::Int(a) => a.into_iter().map(FieldValue::Int).collect(),
            Array::BigInt(a) => a
                .into_iter()
                .map(|v| FieldValue::BigInt(Box::new(v)))
                .collect(),
            Array::Float(a) => a.into_iter().map(FieldValue::Float).collect(),
            Array::Rational(a) => a
                .into_iter()
                .map(|v| FieldValue::Rational(Box::new(v)))
                .collect(),
            Array::Bytes(a) => a.into_iter().map(FieldValue::Bytes).collect(),
            Array::Text(a) => a.into_iter().map(FieldValue::Text).collect(),
            Array::Error(a) => a.into_iter().map(FieldValue::Error).collect(),
            Array::Array(a) => a.into_iter().map(FieldValue::Array).collect(),
            Array::Object(a) => {
                a.into_iter().map(FieldValue::Object).collect()
            }
            Array::StreamValueId(a) => {
                a.into_iter().map(FieldValue::StreamValueId).collect()
            }
            Array::FieldReference(a) => {
                a.into_iter().map(FieldValue::FieldReference).collect()
            }
            Array::SlicedFieldReference(a) => a
                .into_iter()
                .map(FieldValue::SlicedFieldReference)
                .collect(),
            Array::Custom(a) => {
                a.into_iter().map(FieldValue::Custom).collect()
            }
            Array::Mixed(v) => v,
        });
        let Array::Mixed(mixed) = self else {
            unreachable!()
        };
        mixed
    }
    pub fn push_raw(&mut self, value: FieldValue) {
        match (self, value) {
            (Array::Undefined(n), FieldValue::Undefined)
            | (Array::Null(n), FieldValue::Null) => {
                *n += 1;
            }
            (Array::Int(a), FieldValue::Int(v)) => {
                a.push(v);
            }
            (Array::BigInt(a), FieldValue::BigInt(v)) => {
                a.push(*v);
            }
            (Array::Float(a), FieldValue::Float(v)) => {
                a.push(v);
            }
            (Array::Rational(a), FieldValue::Rational(v)) => {
                a.push(*v);
            }
            (Array::Bytes(a), FieldValue::Bytes(v)) => {
                a.push(v);
            }
            (Array::Text(a), FieldValue::Text(v)) => {
                a.push(v);
            }
            (Array::Error(a), FieldValue::Error(v)) => {
                a.push(v);
            }
            (Array::Array(a), FieldValue::Array(v)) => {
                a.push(v);
            }
            (Array::Object(a), FieldValue::Object(v)) => {
                a.push(v);
            }
            (Array::StreamValueId(a), FieldValue::StreamValueId(v)) => {
                a.push(v);
            }
            (Array::FieldReference(a), FieldValue::FieldReference(v)) => {
                a.push(v);
            }
            (
                Array::SlicedFieldReference(a),
                FieldValue::SlicedFieldReference(v),
            ) => {
                a.push(v);
            }
            (Array::Custom(a), FieldValue::Custom(v)) => {
                a.push(v);
            }
            (Array::Mixed(a), value) => {
                a.push(value);
            }
            (arr, value) => {
                arr.make_mixed().push(value);
            }
        }
    }
    pub fn extend_raw<T: FixedSizeFieldValueType>(
        &mut self,
        iter: impl Iterator<Item = T>,
    ) {
        // SAFETY: We *know* that `T` and `Q` will be *identical* because of
        // the check on `T::REPR`. `FixedSizeFieldValueType` is an
        // unsafe trait, so assuming that nobody gave us an incorrect
        // `REPR` is sound.
        use crate::utils::force_cast as xx;
        unsafe {
            match (self, T::REPR) {
                (Array::Undefined(n), FieldValueRepr::Undefined)
                | (Array::Null(n), FieldValueRepr::Null) => {
                    *n += iter.count();
                }
                (Array::Int(a), FieldValueRepr::Int) => {
                    a.extend(iter.map(|v| xx::<T, i64>(v)));
                }
                (Array::BigInt(a), FieldValueRepr::BigInt) => {
                    a.extend(iter.map(|v| xx::<T, BigInt>(v)));
                }
                (Array::Float(a), FieldValueRepr::Float) => {
                    a.extend(iter.map(|v| xx::<T, f64>(v)));
                }
                (Array::Rational(a), FieldValueRepr::Rational) => {
                    a.extend(iter.map(|v| xx::<T, BigRational>(v)));
                }
                (Array::Bytes(a), FieldValueRepr::BytesBuffer) => {
                    a.extend(iter.map(|v| xx::<T, Vec<u8>>(v)));
                }
                (Array::Text(a), FieldValueRepr::TextBuffer) => {
                    a.extend(iter.map(|v| xx::<T, String>(v)));
                }
                (Array::Error(a), FieldValueRepr::Error) => {
                    a.extend(
                        iter.map(|v| xx::<T, OperatorApplicationError>(v)),
                    );
                }
                (Array::Array(a), FieldValueRepr::Array) => {
                    a.extend(iter.map(|v| xx::<T, Array>(v)));
                }
                (Array::Object(a), FieldValueRepr::Object) => {
                    a.extend(iter.map(|v| xx::<T, Object>(v)));
                }
                (Array::StreamValueId(a), FieldValueRepr::StreamValueId) => {
                    a.extend(iter.map(|v| xx::<T, StreamValueId>(v)));
                }
                (Array::FieldReference(a), FieldValueRepr::FieldReference) => {
                    a.extend(iter.map(|v| xx::<T, FieldReference>(v)));
                }
                (
                    Array::SlicedFieldReference(a),
                    FieldValueRepr::SlicedFieldReference,
                ) => {
                    a.extend(iter.map(|v| xx::<T, SlicedFieldReference>(v)));
                }
                (Array::Custom(a), FieldValueRepr::Custom) => {
                    a.extend(iter.map(|v| xx::<T, CustomDataBox>(v)));
                }
                (Array::Mixed(a), _) => {
                    a.extend(
                        iter.map(|v| FieldValue::from_fixed_sized_type(v)),
                    );
                }
                (arr, _) => {
                    arr.make_mixed().extend(
                        iter.map(|v| FieldValue::from_fixed_sized_type(v)),
                    );
                }
            }
        }
    }
    pub fn canonicalize_for_repr(&mut self, repr: FieldValueRepr) {
        let len = self.len();
        if len == 0 {
            self.convert_cleared(repr);
            return;
        }
        if let Array::Mixed(m) = self {
            if len == 1 && repr == m[0].repr() {
                let v0 = m.pop().unwrap();
                self.convert_cleared(repr);
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

    pub fn push(&mut self, value: FieldValue) {
        self.canonicalize_for_repr(value.repr());
        self.push_raw(value);
    }
    pub fn pop(&mut self) -> Option<FieldValue> {
        Some(match self {
            Array::Undefined(n) => {
                if *n == 0 {
                    return None;
                }
                *n -= 1;
                FieldValue::Undefined
            }
            Array::Null(n) => {
                if *n == 0 {
                    return None;
                }
                *n -= 1;
                FieldValue::Null
            }
            Array::Int(v) => FieldValue::Int(v.pop()?),
            Array::BigInt(v) => FieldValue::BigInt(Box::new(v.pop()?)),
            Array::Float(v) => FieldValue::Float(v.pop()?),
            Array::Rational(v) => FieldValue::Rational(Box::new(v.pop()?)),
            Array::Bytes(v) => FieldValue::Bytes(v.pop()?),
            Array::Text(v) => FieldValue::Text(v.pop()?),
            Array::Error(v) => FieldValue::Error(v.pop()?),
            Array::Array(v) => FieldValue::Array(v.pop()?),
            Array::Object(v) => FieldValue::Object(v.pop()?),
            Array::StreamValueId(v) => FieldValue::StreamValueId(v.pop()?),
            Array::FieldReference(v) => FieldValue::FieldReference(v.pop()?),
            Array::SlicedFieldReference(v) => {
                FieldValue::SlicedFieldReference(v.pop()?)
            }
            Array::Custom(v) => FieldValue::Custom(v.pop()?),
            Array::Mixed(v) => return v.pop(),
        })
    }
}
