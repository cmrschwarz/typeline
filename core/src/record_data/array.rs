use metamatch::metamatch;
use num::{BigInt, BigRational};

use crate::{
    cli::call_expr::Argument,
    operators::errors::OperatorApplicationError,
    record_data::{
        field_value::{FieldValue, FieldValueKind, FieldValueUnboxed},
        field_value_ref::FieldValueRefMut,
    },
    utils::{force_cast, temp_vec::convert_vec_cleared},
};

use super::{
    custom_data::CustomDataBox,
    field_data::{FieldValueRepr, FixedSizeFieldValueType},
    field_value::{FieldReference, SlicedFieldReference},
    field_value_ref::FieldValueRef,
    object::Object,
    scope_manager::OpDeclRef,
    stream_value::StreamValueId,
};

#[derive(Debug, PartialEq, Clone)]
pub enum Array {
    Undefined(usize),
    Null(usize),
    Bool(Vec<bool>),
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
    OpDecl(Vec<OpDeclRef>),
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
    pub fn clear(&mut self) {
        metamatch!(match self {
            Array::Null(len) | Array::Undefined(len) => *len = 0,
            #[expand(REP in [
                Bool, Int, BigInt, BigRational, Bytes, Text, Error, Array, Object,
                FieldReference, SlicedFieldReference, Custom, Mixed, Float,
                StreamValueId, Argument, OpDecl
            ])]
            Array::REP(a) => a.clear(),
        })
    }
    pub fn len(&self) -> usize {
        metamatch!(match self {
            Array::Null(len) | Array::Undefined(len) => *len,
            #[expand(REP in [
                Bool, Int, BigInt, BigRational, Bytes, Text, Error, Array, Object,
                FieldReference, SlicedFieldReference, Custom, Mixed, Float,
                StreamValueId, Argument, OpDecl
            ])]
            Array::REP(a) => a.len(),
        })
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// returns the present variant even if the array is empty
    pub fn repr_raw(&self) -> Option<FieldValueRepr> {
        Some(metamatch!(match self {
            #[expand(REP in [
                Null, Undefined,
                Bool, Int, BigInt, BigRational, Error, Array, Object,
                FieldReference, SlicedFieldReference, Custom, Float,
                StreamValueId, Argument, OpDecl
            ])]
            Array::REP(_) => FieldValueRepr::REP,
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
            #[expand(REP in [
                Bool, Int, BigInt, BigRational, Error, Array, Object,
                FieldReference, SlicedFieldReference, Custom, Float,
                StreamValueId, Argument, Mixed, Text, Bytes, OpDecl
            ])]
            Array::REP(v) => convert_vec_cleared(v),
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
            #[expand(REP in [
                Bool, Int, BigInt, BigRational, Error, Array, Object,
                FieldReference, SlicedFieldReference, Custom, Float,
                StreamValueId, Argument, OpDecl,
            ])]
            FieldValueRepr::REP => Array::REP(arr.into_cleared_vec()),
        })
    }

    pub fn make_mixed(&mut self) -> &mut Vec<FieldValue> {
        *self = Array::Mixed(metamatch!(match std::mem::take(self) {
            Array::Mixed(v) => v,

            #[expand(REP in [
                Bool, Int, Error, Array,
                FieldReference, SlicedFieldReference, Custom, Float,
                StreamValueId, Text, Bytes, OpDecl
            ])]
            Array::REP(a) => a.into_iter().map(FieldValue::REP).collect(),

            #[expand(REP in [Null, Undefined])]
            Array::REP(n) => {
                std::iter::repeat(FieldValue::REP).take(n).collect()
            }

            #[expand(BOX_T in [
                BigInt, BigRational, Argument, Object
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
            #[expand_pattern(REP in [Null, Undefined])]
            (Array::REP(n), FieldValue::REP) => {
                *n += 1;
            }

            #[expand(REP in [
                Int, Error, Array,
                FieldReference, SlicedFieldReference, Custom, Float,
                StreamValueId, Text, Bytes,
            ])]
            (Array::REP(a), FieldValue::REP(v)) => {
                a.push(v);
            }

            #[expand(BOX_T in [BigInt, BigRational, Argument, Object])]
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
    pub fn push_raw_unboxed(&mut self, value: FieldValueUnboxed) {
        metamatch!(match (self, value) {
            #[expand_pattern(REP in [Null, Undefined])]
            (Array::REP(n), FieldValueUnboxed::REP) => {
                *n += 1;
            }

            #[expand(REP in [
                Int, Error, Array,
                FieldReference, SlicedFieldReference, Custom, Float,
                StreamValueId, Text, Bytes, BigInt, BigRational, Argument,
                Object
            ])]
            (Array::REP(a), FieldValueUnboxed::REP(v)) => {
                a.push(v);
            }

            (Array::Mixed(a), value) => {
                a.push(FieldValue::from(value));
            }

            (arr, value) => {
                debug_assert!(arr.repr() != Some(value.repr()));
                arr.make_mixed().push(FieldValue::from(value));
            }
        })
    }
    pub fn extend_raw<T: FixedSizeFieldValueType>(
        &mut self,
        iter: impl Iterator<Item = T>,
    ) {
        // SAFETY: We *know* that `T` and `Q` will be *identical* because of
        // the check on `T::REP`. `FixedSizeFieldValueType` is an
        // unsafe trait, so assuming that nobody gave us an incorrect
        // `REP` is sound.
        metamatch!(match self {
            #[expand(REP in [Null, Undefined])]
            Array::REP(n) => {
                if T::REPR == FieldValueRepr::REP {
                    *n += iter.count();
                    return;
                }
            }

            #[expand((ARRAY_T, REP_T, VALUE_T) in [
                (Int, Int, i64),
                (Bool, Bool, bool),
                (Float, Float, f64),
                (Text, TextBuffer, String),
                (Bytes, BytesBuffer, Vec<u8>),
                (Array, Array, Array),
                (Object, Object, Object),
                (Error, Error, OperatorApplicationError),
                (OpDecl, OpDecl, OpDeclRef),
                (FieldReference, FieldReference, FieldReference),
                (SlicedFieldReference, SlicedFieldReference, SlicedFieldReference),
                (Custom, Custom, CustomDataBox),
                (StreamValueId, StreamValueId, StreamValueId),
            ])]
            Array::ARRAY_T(a) => {
                if T::REPR == FieldValueRepr::REP_T {
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
    pub fn push_unboxed(&mut self, value: FieldValueUnboxed) {
        self.canonicalize_for_repr(value.repr());
        self.push_raw_unboxed(value);
    }
    fn push_ref(&mut self, value: FieldValueRef) {
        self.canonicalize_for_repr(value.repr());
        self.push_raw_unboxed(value.to_field_value_unboxed());
    }
    pub fn pop(&mut self) -> Option<FieldValue> {
        Some(metamatch!(match self {
            #[expand(REP in [Null, Undefined])]
            Array::REP(n) => {
                if *n == 0 {
                    return None;
                }
                *n -= 1;
                FieldValue::REP
            }

            #[expand(REP in [
                Bool, Int, Error, Array, OpDecl,
                FieldReference, SlicedFieldReference, Custom, Float,
                StreamValueId, Text, Bytes,
            ])]
            Array::REP(v) => {
                debug_assert!(!FieldValueKind::REP
                    .repr(true)
                    .is_field_value_boxed());
                FieldValue::REP(v.pop()?)
            }

            #[expand(REP in [BigInt, BigRational, Argument,  Object])]
            Array::REP(v) => {
                debug_assert!(FieldValueRepr::REP.is_field_value_boxed());
                FieldValue::REP(Box::new(v.pop()?))
            }

            Array::Mixed(v) => return v.pop(),
        }))
    }
    pub fn get(&self, index: usize) -> Option<FieldValueRef> {
        metamatch!(match self {
            #[expand(REP in [Null, Undefined])]
            Array::REP(len) => {
                if *len <= index {
                    return None;
                }
                Some(FieldValueRef::REP)
            }
            #[expand(REP in [
                Bool, Int, Error, Array, Object, OpDecl,
                FieldReference, SlicedFieldReference, Custom, Float,
                StreamValueId, BigInt, BigRational, Argument
            ])]
            Array::REP(v) => {
                v.get(index).map(FieldValueRef::REP)
            }

            #[expand(REP in [Text, Bytes])]
            Array::REP(v) => {
                v.get(index).map(|v| FieldValueRef::REP(v))
            }

            Array::Mixed(v) => v.get(index).map(|v| v.as_ref()),
        })
    }
    pub fn get_mut(&mut self, index: usize) -> Option<FieldValueRefMut> {
        metamatch!(match self {
            #[expand(REP in [Null, Undefined])]
            Array::REP(len) => {
                if *len <= index {
                    return None;
                }
                Some(FieldValueRefMut::REP)
            }
            #[expand(REP in [
                Bool, Int, Error, Array, Object,
                FieldReference, SlicedFieldReference, Custom, Float,
                StreamValueId, BigInt, BigRational, Argument, OpDecl
            ])]
            Array::REP(v) => {
                v.get_mut(index).map(FieldValueRefMut::REP)
            }

            #[expand((REP, REF) in [
                (Text, TextBuffer),
                (Bytes, BytesBuffer)
            ])]
            Array::REP(v) => {
                v.get_mut(index).map(FieldValueRefMut::REF)
            }

            Array::Mixed(v) => v.get_mut(index).map(|v| v.as_ref_mut()),
        })
    }

    pub fn ref_iter(&self) -> ArrayRefIter {
        ArrayRefIter {
            arr: self,
            index: 0,
        }
    }

    pub fn into_iter_unboxed(self) -> ArrayIntoIterUnboxed {
        metamatch!(match self {
            #[expand(T in [Null, Undefined])]
            Array::T(count) => ArrayIntoIterUnboxed::T(std::iter::repeat_n(
                FieldValueUnboxed::T,
                count
            )),

            #[expand(REP in [
                Text, Bytes, Mixed,
                Bool, Int, Error, Array, Object, OpDecl,
                FieldReference, SlicedFieldReference, Custom, Float,
                StreamValueId, BigInt, BigRational, Argument
            ])]
            Array::REP(arr) => {
                ArrayIntoIterUnboxed::REP(arr.into_iter())
            }
        })
    }

    fn set_ref(&mut self, idx: usize, v: FieldValueRef<'_>) {
        if self.len() == 1 {
            self.clear();
            self.push_ref(v);
            return;
        }
        metamatch!(match (self, v) {
            #[expand_pattern(REP in [Null, Undefined])]
            (Array::REP(_), FieldValueRef::REP) => (),

            #[expand(REP in [
                Int, Error, Array,
                FieldReference, SlicedFieldReference, Custom, Float,
                StreamValueId, Text, Bytes, BigInt, BigRational, Argument,
                Object
            ])]
            (Array::REP(a), FieldValueRef::REP(v)) => {
                v.clone_into(&mut a[idx]);
            }

            (Array::Mixed(a), value) => {
                a[idx] = value.to_field_value();
            }

            (arr, value) => {
                debug_assert!(arr.repr() != Some(value.repr()));
                arr.make_mixed()[idx] = v.to_field_value();
            }
        })
    }
}

impl IntoIterator for Array {
    type IntoIter = ArrayIntoIter;
    type Item = FieldValue;
    fn into_iter(self) -> ArrayIntoIter {
        metamatch!(match self {
            #[expand(T in [Null, Undefined])]
            Array::T(count) =>
                ArrayIntoIter::T(std::iter::repeat_n(FieldValue::T, count)),

            #[expand(REP in [
                Text, Bytes, Mixed,
                Bool, Int, Error, Array, Object, OpDecl,
                FieldReference, SlicedFieldReference, Custom, Float,
                StreamValueId, BigInt, BigRational, Argument
            ])]
            Array::REP(arr) => {
                ArrayIntoIter::REP(arr.into_iter())
            }
        })
    }
}

pub struct ArrayRefIter<'a> {
    arr: &'a Array,
    index: usize,
}

impl<'a> Iterator for ArrayRefIter<'a> {
    type Item = FieldValueRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let res = self.arr.get(self.index)?;
        self.index += 1;
        Some(res)
    }
}

pub enum ArrayIntoIterUnboxed {
    Null(std::iter::RepeatN<FieldValueUnboxed>),
    Undefined(std::iter::RepeatN<FieldValueUnboxed>),
    Bool(std::vec::IntoIter<bool>),
    Int(std::vec::IntoIter<i64>),
    BigInt(std::vec::IntoIter<BigInt>),
    Float(std::vec::IntoIter<f64>),
    BigRational(std::vec::IntoIter<BigRational>),
    Bytes(std::vec::IntoIter<Vec<u8>>),
    Text(std::vec::IntoIter<String>),
    Error(std::vec::IntoIter<OperatorApplicationError>),
    Array(std::vec::IntoIter<Array>),
    Object(std::vec::IntoIter<Object>),
    StreamValueId(std::vec::IntoIter<StreamValueId>),
    Argument(std::vec::IntoIter<Argument>),
    OpDecl(std::vec::IntoIter<OpDeclRef>),
    FieldReference(std::vec::IntoIter<FieldReference>),
    SlicedFieldReference(std::vec::IntoIter<SlicedFieldReference>),
    Custom(std::vec::IntoIter<CustomDataBox>),
    Mixed(std::vec::IntoIter<FieldValue>),
}

impl Iterator for ArrayIntoIterUnboxed {
    type Item = FieldValueUnboxed;

    fn next(&mut self) -> Option<Self::Item> {
        metamatch!(match self {
            #[expand_pattern(T in [Null, Undefined])]
            ArrayIntoIterUnboxed::T(iter) => iter.next(),

            #[expand(REP in [
                Text, Bytes,
                Bool, Int, Error, Array, Object, OpDecl,
                FieldReference, SlicedFieldReference, Custom, Float,
                StreamValueId, BigInt, BigRational, Argument
            ])]
            ArrayIntoIterUnboxed::REP(iter) => {
                Some(FieldValueUnboxed::REP(iter.next()?))
            }

            ArrayIntoIterUnboxed::Mixed(iter) => Some(iter.next()?.unbox()),
        })
    }
}

pub enum ArrayIntoIter {
    Null(std::iter::RepeatN<FieldValue>),
    Undefined(std::iter::RepeatN<FieldValue>),
    Bool(std::vec::IntoIter<bool>),
    Int(std::vec::IntoIter<i64>),
    BigInt(std::vec::IntoIter<BigInt>),
    Float(std::vec::IntoIter<f64>),
    BigRational(std::vec::IntoIter<BigRational>),
    Bytes(std::vec::IntoIter<Vec<u8>>),
    Text(std::vec::IntoIter<String>),
    Error(std::vec::IntoIter<OperatorApplicationError>),
    Array(std::vec::IntoIter<Array>),
    Object(std::vec::IntoIter<Object>),
    StreamValueId(std::vec::IntoIter<StreamValueId>),
    Argument(std::vec::IntoIter<Argument>),
    OpDecl(std::vec::IntoIter<OpDeclRef>),
    FieldReference(std::vec::IntoIter<FieldReference>),
    SlicedFieldReference(std::vec::IntoIter<SlicedFieldReference>),
    Custom(std::vec::IntoIter<CustomDataBox>),
    Mixed(std::vec::IntoIter<FieldValue>),
}

impl Iterator for ArrayIntoIter {
    type Item = FieldValue;

    fn next(&mut self) -> Option<Self::Item> {
        metamatch!(match self {
            #[expand_pattern(T in [Null, Undefined])]
            ArrayIntoIter::T(iter) => iter.next(),

            #[expand(REP in [
                Text, Bytes,
                Bool, Int, Error, Array, OpDecl,
                FieldReference, SlicedFieldReference, Custom, Float,
                StreamValueId
            ])]
            ArrayIntoIter::REP(iter) => {
                Some(FieldValue::REP(iter.next()?))
            }

            #[expand(REP in [
                Object, BigInt, BigRational, Argument
            ])]
            ArrayIntoIter::REP(iter) => {
                Some(FieldValue::REP(Box::new(iter.next()?)))
            }

            ArrayIntoIter::Mixed(iter) => Some(iter.next()?),
        })
    }
}

#[derive(Default)]
pub struct ArrayBuilder {
    arr: Array,
    run_lengths: Vec<usize>,
    drained_indices: Vec<usize>,
    run_len_available: usize,
}

impl ArrayBuilder {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn is_empty(&self) -> bool {
        self.arr.is_empty()
    }
    pub fn push_value(&mut self, value: FieldValueRef, rl: usize) {
        debug_assert!(rl != 0);
        self.arr.push_ref(value);
        self.run_lengths.push(rl);
        if self.run_lengths.len() == 1 {
            self.run_len_available = rl;
        } else {
            self.run_len_available = self.run_len_available.min(rl);
        }
    }
    pub fn get_drained_idx(&mut self) -> Option<usize> {
        self.drained_indices.last().copied()
    }
    pub fn replenish_drained_value(&mut self, v: FieldValueRef, rl: usize) {
        let idx = self.drained_indices.pop().unwrap();
        self.run_lengths[idx] = rl;
        self.arr.set_ref(idx, v);
        if self.drained_indices.is_empty() {
            self.run_len_available = rl;
        }
    }
    pub fn build(&self) -> Array {
        self.arr.clone()
    }
    pub fn take(&mut self) -> Array {
        self.drained_indices.clear();
        self.run_lengths.clear();
        self.run_len_available = 0;
        std::mem::take(&mut self.arr)
    }
    pub fn available_len(&self) -> usize {
        self.run_len_available
    }
    pub fn consume_len(&mut self, len: usize) {
        let mut rla = usize::MAX;
        for (idx, rl) in self.run_lengths.iter_mut().enumerate() {
            *rl -= len;
            rla = rla.min(*rl);
            if *rl == 0 {
                self.drained_indices.push(idx);
            }
        }
        self.run_len_available = rla;
    }
}
