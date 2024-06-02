use num::{BigInt, BigRational};

use crate::{
    operators::errors::OperatorApplicationError,
    record_data::field_value_ref::FieldValueSlice,
};
use metamatch::metamatch;

use super::{
    array::Array,
    custom_data::CustomDataBox,
    field_data::RunLength,
    field_value::{FieldReference, Object, SlicedFieldReference},
    field_value_ref::{FieldValueRef, ValidTypedRange},
    field_value_slice_iter::{
        FieldValueSliceIter, InlineBytesIter, InlineTextIter,
    },
    ref_iter::{AnyRefSliceIter, RangeOffsets, RefAwareTypedRange},
    stream_value::StreamValueId,
};

pub enum DynFieldValueSliceIter<'a> {
    Null(usize),
    Undefined(usize),
    Int(FieldValueSliceIter<'a, i64>),
    BigInt(FieldValueSliceIter<'a, BigInt>),
    Float(FieldValueSliceIter<'a, f64>),
    Rational(FieldValueSliceIter<'a, BigRational>),
    TextInline(InlineTextIter<'a>),
    TextBuffer(FieldValueSliceIter<'a, String>),
    BytesInline(InlineBytesIter<'a>),
    BytesBuffer(FieldValueSliceIter<'a, Vec<u8>>),
    Object(FieldValueSliceIter<'a, Object>),
    Array(FieldValueSliceIter<'a, Array>),
    Custom(FieldValueSliceIter<'a, CustomDataBox>),
    Error(FieldValueSliceIter<'a, OperatorApplicationError>),
    StreamValueId(FieldValueSliceIter<'a, StreamValueId>),
    FieldReference(FieldValueSliceIter<'a, FieldReference>),
    SlicedFieldReference(FieldValueSliceIter<'a, SlicedFieldReference>),
}

impl<'a> DynFieldValueSliceIter<'a> {
    pub fn new(range: &ValidTypedRange<'a>) -> Self {
        metamatch!(match range.data {
            #[expand(T in [Null, Undefined])]
            FieldValueSlice::T(n) => DynFieldValueSliceIter::T(n.len()),

            FieldValueSlice::TextInline(vals) =>
                DynFieldValueSliceIter::TextInline(InlineTextIter::from_range(
                    range, vals
                )),
            FieldValueSlice::BytesInline(vals) =>
                DynFieldValueSliceIter::BytesInline(
                    InlineBytesIter::from_range(range, vals)
                ),

            #[expand(T in [
                Int, BigInt, Float, Rational,
                TextBuffer, BytesBuffer,
                Object, Array, Custom, Error,
                StreamValueId, FieldReference, SlicedFieldReference,
            ])]
            FieldValueSlice::T(vals) => DynFieldValueSliceIter::T(
                FieldValueSliceIter::from_valid_range(range, vals),
            ),
        })
    }
    pub fn peek(&self) -> Option<(FieldValueRef<'a>, RunLength)> {
        metamatch!(match self {
            #[expand(T in [Null, Undefined])]
            DynFieldValueSliceIter::T(it) => {
                if *it == 0 {
                    None
                } else {
                    Some((
                        FieldValueRef::T,
                        (*it).min(RunLength::MAX as usize) as RunLength,
                    ))
                }
            }

            #[expand((IT, T) in [
                (TextInline, Text),
                (TextBuffer, Text),
                (BytesInline, Bytes),
                (BytesBuffer, Bytes)
            ])]
            DynFieldValueSliceIter::IT(it) => {
                let (v, rl) = it.peek()?;
                Some((FieldValueRef::T(v), rl))
            }

            #[expand(T in [
                Int, BigInt, Float, Rational,
                Object, Array, Custom, Error,
                StreamValueId, FieldReference, SlicedFieldReference,
            ])]
            DynFieldValueSliceIter::T(it) => {
                let (v, rl) = it.peek()?;
                Some((FieldValueRef::T(v), rl))
            }
        })
    }
    pub fn next_n_fields(&mut self, n: usize) -> usize {
        metamatch!(match self {
            DynFieldValueSliceIter::Null(count)
            | DynFieldValueSliceIter::Undefined(count) => {
                if *count >= n {
                    *count -= n;
                    return n;
                }
                let res = *count;
                *count = 0;
                res
            }
            #[expand(T in [
                Int, BigInt,Float, Rational, TextInline, TextBuffer,
                BytesInline, BytesBuffer, Object, Array, Custom, Error,
                StreamValueId, FieldReference, SlicedFieldReference,
            ])]
            DynFieldValueSliceIter::T(it) => {
                it.next_n_fields(n)
            }
        })
    }
}

impl<'a> Iterator for DynFieldValueSliceIter<'a> {
    type Item = (FieldValueRef<'a>, RunLength);
    fn next(&mut self) -> Option<(FieldValueRef<'a>, RunLength)> {
        metamatch!(match self {
            #[expand(T in [Null, Undefined])]
            DynFieldValueSliceIter::T(it) => {
                if *it == 0 {
                    None
                } else {
                    let rl = (*it).min(RunLength::MAX as usize);
                    *it -= rl;
                    Some((FieldValueRef::T, rl as RunLength))
                }
            }
            #[expand((IT, T) in [
                (TextInline, Text),
                (TextBuffer, Text),
                (BytesInline, Bytes),
                (BytesBuffer, Bytes)
            ])]
            DynFieldValueSliceIter::IT(it) => {
                let (v, rl) = it.next()?;
                Some((FieldValueRef::T(v), rl))
            }

            #[expand(T in [
                Int, BigInt, Float, Rational,
                Object, Array, Custom, Error,
                StreamValueId, FieldReference, SlicedFieldReference,
            ])]
            DynFieldValueSliceIter::T(it) => {
                let (v, rl) = it.next()?;
                Some((FieldValueRef::T(v), rl))
            }
        })
    }
}

pub struct RefAwareDynFieldValueSliceIter<'a> {
    data_iter: DynFieldValueSliceIter<'a>,
    refs: Option<AnyRefSliceIter<'a>>,
}

impl<'a> RefAwareDynFieldValueSliceIter<'a> {
    pub fn new(range: RefAwareTypedRange<'a>) -> Self {
        Self {
            data_iter: DynFieldValueSliceIter::new(&range.base),
            refs: range.refs,
        }
    }
}

impl<'a> Iterator for RefAwareDynFieldValueSliceIter<'a> {
    type Item = (FieldValueRef<'a>, RunLength, RangeOffsets);
    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.refs {
            Some(AnyRefSliceIter::FieldRef(refs_iter)) => {
                let (_fr, rl_ref) = refs_iter.peek()?;
                let (data, rl_data) = self.data_iter.peek()?;
                let run_len = rl_ref.min(rl_data);
                self.data_iter.next_n_fields(run_len as usize);
                refs_iter.next_n_fields(run_len as usize);
                Some((data, run_len, RangeOffsets::default()))
            }
            Some(AnyRefSliceIter::SlicedFieldRef(refs_iter)) => {
                let (fr, rl_ref) = refs_iter.peek()?;
                let (data, rl_data) = self.data_iter.peek()?;
                let run_len = rl_ref.min(rl_data);
                self.data_iter.next_n_fields(run_len as usize);
                refs_iter.next_n_fields(run_len as usize);
                Some((
                    data.subslice(fr.begin..fr.end),
                    run_len,
                    RangeOffsets {
                        from_begin: fr.begin,
                        from_end: data.as_slice().as_bytes().len() - fr.end,
                    },
                ))
            }
            None => {
                let (data, rl) = self.data_iter.next()?;
                Some((data, rl, RangeOffsets::default()))
            }
        }
    }
}
