use num::{BigInt, BigRational};

use crate::{
    cli::call_expr::Argument,
    operators::errors::OperatorApplicationError,
    record_data::{
        field_value_ref::FieldValueSlice,
        field_value_slice_iter::FieldValueBlock,
    },
};
use metamatch::metamatch;

use super::{
    array::Array,
    custom_data::CustomDataBox,
    field_data::RunLength,
    field_value::{FieldReference, Object, SlicedFieldReference},
    field_value_ref::{FieldValueRef, ValidTypedRange},
    field_value_slice_iter::{
        FieldValueRangeIter, InlineBytesIter, InlineTextIter,
    },
    ref_iter::{AnyRefSliceIter, RangeOffsets, RefAwareTypedRange},
    stream_value::StreamValueId,
};

pub enum DynFieldValueBlock<'a> {
    Plain(FieldValueSlice<'a>),
    WithRunLength(FieldValueRef<'a>, RunLength),
}

pub enum DynFieldValueRangeIter<'a> {
    Null(usize),
    Undefined(usize),
    Int(FieldValueRangeIter<'a, i64>),
    BigInt(FieldValueRangeIter<'a, BigInt>),
    Float(FieldValueRangeIter<'a, f64>),
    BigRational(FieldValueRangeIter<'a, BigRational>),
    TextInline(InlineTextIter<'a>),
    TextBuffer(FieldValueRangeIter<'a, String>),
    BytesInline(InlineBytesIter<'a>),
    BytesBuffer(FieldValueRangeIter<'a, Vec<u8>>),
    Object(FieldValueRangeIter<'a, Object>),
    Array(FieldValueRangeIter<'a, Array>),
    Argument(FieldValueRangeIter<'a, Argument>),
    Custom(FieldValueRangeIter<'a, CustomDataBox>),
    Error(FieldValueRangeIter<'a, OperatorApplicationError>),
    StreamValueId(FieldValueRangeIter<'a, StreamValueId>),
    FieldReference(FieldValueRangeIter<'a, FieldReference>),
    SlicedFieldReference(FieldValueRangeIter<'a, SlicedFieldReference>),
}

impl DynFieldValueBlock<'_> {
    pub fn run_len(&self) -> usize {
        match self {
            DynFieldValueBlock::Plain(p) => p.run_len(),
            DynFieldValueBlock::WithRunLength(_, rl) => *rl as usize,
        }
    }
}

impl<'a> DynFieldValueRangeIter<'a> {
    pub fn new(range: &ValidTypedRange<'a>) -> Self {
        metamatch!(match range.data {
            #[expand(REPR in [Null, Undefined])]
            FieldValueSlice::REPR(n) => DynFieldValueRangeIter::REPR(n),

            FieldValueSlice::TextInline(vals) =>
                DynFieldValueRangeIter::TextInline(InlineTextIter::from_range(
                    range, vals
                )),
            FieldValueSlice::BytesInline(vals) =>
                DynFieldValueRangeIter::BytesInline(
                    InlineBytesIter::from_range(range, vals)
                ),

            #[expand(REPR in [
                Int, BigInt, Float, BigRational,
                TextBuffer, BytesBuffer,
                Object, Array, Argument, Custom, Error,
                StreamValueId, FieldReference, SlicedFieldReference,
            ])]
            FieldValueSlice::REPR(vals) => DynFieldValueRangeIter::REPR(
                FieldValueRangeIter::from_valid_range(range, vals),
            ),
        })
    }
    pub fn peek(&self) -> Option<(FieldValueRef<'a>, RunLength)> {
        metamatch!(match self {
            #[expand(REPR in [Null, Undefined])]
            DynFieldValueRangeIter::REPR(it) => {
                if *it == 0 {
                    None
                } else {
                    Some((
                        FieldValueRef::REPR,
                        (*it).min(RunLength::MAX as usize) as RunLength,
                    ))
                }
            }

            #[expand((REPR, KIND) in [
                (TextInline, Text),
                (TextBuffer, Text),
                (BytesInline, Bytes),
                (BytesBuffer, Bytes)
            ])]
            DynFieldValueRangeIter::REPR(it) => {
                let (v, rl) = it.peek()?;
                Some((FieldValueRef::KIND(v), rl))
            }

            #[expand(REPR in [
                Int, BigInt, Float, BigRational,
                Object, Array, Argument, Custom, Error,
                StreamValueId, FieldReference, SlicedFieldReference,
            ])]
            DynFieldValueRangeIter::REPR(it) => {
                let (v, rl) = it.peek()?;
                Some((FieldValueRef::REPR(v), rl))
            }
        })
    }
    pub fn next_n_fields(&mut self, n: usize) -> usize {
        metamatch!(match self {
            DynFieldValueRangeIter::Null(count)
            | DynFieldValueRangeIter::Undefined(count) => {
                if *count >= n {
                    *count -= n;
                    return n;
                }
                let res = *count;
                *count = 0;
                res
            }
            #[expand(REPR in [
                Int, BigInt,Float, BigRational, TextInline, TextBuffer,
                BytesInline, BytesBuffer, Object, Array, Argument, Custom, Error,
                StreamValueId, FieldReference, SlicedFieldReference,
            ])]
            DynFieldValueRangeIter::REPR(it) => {
                it.next_n_fields(n)
            }
        })
    }
    pub fn next_block(&mut self) -> Option<DynFieldValueBlock> {
        metamatch!(match self {
            #[expand(REPR in [Null, Undefined])]
            DynFieldValueRangeIter::REPR(it) => {
                if *it == 0 {
                    None
                } else {
                    let rl = (*it).min(RunLength::MAX as usize);
                    *it -= rl;
                    Some(DynFieldValueBlock::Plain(FieldValueSlice::REPR(rl)))
                }
            }
            #[expand((REPR, KIND)  in [
                (TextInline, Text),
                (BytesInline, Bytes)
            ])]
            DynFieldValueRangeIter::REPR(it) => {
                let (v, rl) = it.next()?;
                Some(if rl == 1 {
                    DynFieldValueBlock::Plain(FieldValueSlice::REPR(v))
                } else {
                    DynFieldValueBlock::WithRunLength(
                        FieldValueRef::KIND(v),
                        rl,
                    )
                })
            }

            #[expand((REPR, KIND)  in [
                (TextBuffer, Text),
                (BytesBuffer, Bytes)
            ])]
            DynFieldValueRangeIter::REPR(it) => {
                Some(match it.next_block()? {
                    FieldValueBlock::Plain(v) => {
                        DynFieldValueBlock::Plain(FieldValueSlice::REPR(v))
                    }
                    FieldValueBlock::WithRunLength(v, rl) => {
                        DynFieldValueBlock::WithRunLength(
                            FieldValueRef::KIND(v),
                            rl,
                        )
                    }
                })
            }

            #[expand(REPR in [
                Int, BigInt, Float, BigRational,
                Object, Array, Argument, Custom, Error,
                StreamValueId, FieldReference, SlicedFieldReference,
            ])]
            DynFieldValueRangeIter::REPR(it) => {
                Some(match it.next_block()? {
                    FieldValueBlock::Plain(v) => {
                        DynFieldValueBlock::Plain(FieldValueSlice::REPR(v))
                    }
                    FieldValueBlock::WithRunLength(v, rl) => {
                        DynFieldValueBlock::WithRunLength(
                            FieldValueRef::REPR(v),
                            rl,
                        )
                    }
                })
            }
        })
    }
}

impl<'a> Iterator for DynFieldValueRangeIter<'a> {
    type Item = (FieldValueRef<'a>, RunLength);
    fn next(&mut self) -> Option<(FieldValueRef<'a>, RunLength)> {
        metamatch!(match self {
            #[expand(T in [Null, Undefined])]
            DynFieldValueRangeIter::T(it) => {
                if *it == 0 {
                    None
                } else {
                    let rl = (*it).min(RunLength::MAX as usize);
                    *it -= rl;
                    Some((FieldValueRef::T, rl as RunLength))
                }
            }
            #[expand((ITER, T) in [
                (TextInline, Text),
                (TextBuffer, Text),
                (BytesInline, Bytes),
                (BytesBuffer, Bytes)
            ])]
            DynFieldValueRangeIter::ITER(it) => {
                let (v, rl) = it.next()?;
                Some((FieldValueRef::T(v), rl))
            }

            #[expand(T in [
                Int, BigInt, Float, BigRational,
                Object, Array, Argument, Custom, Error,
                StreamValueId, FieldReference, SlicedFieldReference,
            ])]
            DynFieldValueRangeIter::T(it) => {
                let (v, rl) = it.next()?;
                Some((FieldValueRef::T(v), rl))
            }
        })
    }
}

pub struct RefAwareDynFieldValueRangeIter<'a> {
    data_iter: DynFieldValueRangeIter<'a>,
    refs: Option<AnyRefSliceIter<'a>>,
}

impl<'a> RefAwareDynFieldValueRangeIter<'a> {
    pub fn new(range: RefAwareTypedRange<'a>) -> Self {
        Self {
            data_iter: DynFieldValueRangeIter::new(&range.base),
            refs: range.refs,
        }
    }
    pub fn next_block(&mut self) -> Option<DynFieldValueBlock> {
        match &mut self.refs {
            Some(AnyRefSliceIter::FieldRef(refs_iter)) => {
                let block = self.data_iter.next_block()?;
                refs_iter.next_n_fields(block.run_len());
                Some(block)
            }
            Some(AnyRefSliceIter::SlicedFieldRef(refs_iter)) => {
                let (fr, rl_ref) = refs_iter.peek()?;
                let (data, rl_data) = self.data_iter.peek()?;
                let run_len = rl_ref.min(rl_data);
                self.data_iter.next_n_fields(run_len as usize);
                refs_iter.next_n_fields(run_len as usize);
                let data = data.subslice(fr.begin..fr.end);
                Some(if run_len == 1 {
                    DynFieldValueBlock::Plain(data.as_slice())
                } else {
                    DynFieldValueBlock::WithRunLength(data, run_len)
                })
            }
            None => self.data_iter.next_block(),
        }
    }
}

impl<'a> Iterator for RefAwareDynFieldValueRangeIter<'a> {
    type Item = (FieldValueRef<'a>, RunLength, RangeOffsets);
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

pub struct FieldValueSliceIter<'a> {
    slice: FieldValueSlice<'a>,
}

impl<'a> FieldValueSliceIter<'a> {
    pub fn new(slice: FieldValueSlice<'a>) -> Self {
        Self { slice }
    }
}

impl<'a> Iterator for FieldValueSliceIter<'a> {
    type Item = FieldValueRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        metamatch!(match &mut self.slice {
            FieldValueSlice::Undefined(n) | FieldValueSlice::Null(n) => {
                if *n == 0 {
                    None
                } else {
                    *n -= 1;
                    Some(FieldValueRef::Null)
                }
            }
            #[expand((REPR, KIND) in [
                (TextInline, Text), (BytesInline, Bytes)]
            )]
            FieldValueSlice::REPR(val) => {
                let res = FieldValueRef::KIND(val);
                self.slice = FieldValueSlice::Null(0);
                Some(res)
            }
            #[expand((REPR, KIND) in [
                (TextBuffer, Text),
                (BytesBuffer, Bytes),
            ])]
            FieldValueSlice::REPR(v) => {
                if v.is_empty() {
                    None
                } else {
                    let res = &v[0];
                    *v = &v[1..];
                    Some(FieldValueRef::KIND(res))
                }
            }
            #[expand(REPR in [
                Int, BigInt, Float, BigRational,
                Object, Array, Argument, Custom, Error,
                StreamValueId, FieldReference, SlicedFieldReference
            ])]
            FieldValueSlice::REPR(v) => {
                if v.is_empty() {
                    None
                } else {
                    let res = &v[0];
                    *v = &v[1..];
                    Some(FieldValueRef::REPR(res))
                }
            }
        })
    }
}
