use num::{BigInt, BigRational};

use crate::{
    cli::call_expr::Argument,
    operators::errors::OperatorApplicationError,
    record_data::{
        field_data::RUN_LEN_MAX_USIZE,
        field_value_ref::{
            DynFieldValueBlock, FieldValueBlock, FieldValueSlice,
        },
        scope_manager::OpDeclRef,
    },
};
use metamatch::metamatch;

use super::super::{
    array::Array,
    custom_data::CustomDataBox,
    field_data::RunLength,
    field_value::{FieldReference, SlicedFieldReference},
    field_value_ref::{FieldValueRef, ValidTypedRange},
    iter::{
        field_value_slice_iter::{
            FieldValueRangeIter, InlineBytesIter, InlineTextIter,
        },
        ref_iter::{AnyRefSliceIter, RangeOffsets, RefAwareTypedRange},
    },
    object::Object,
    stream_value::StreamValueId,
};

pub enum DynFieldValueRangeIter<'a> {
    Null(usize),
    Undefined(usize),
    Bool(FieldValueRangeIter<'a, bool>),
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
    OpDecl(FieldValueRangeIter<'a, OpDeclRef>),
    Custom(FieldValueRangeIter<'a, CustomDataBox>),
    Error(FieldValueRangeIter<'a, OperatorApplicationError>),
    StreamValueId(FieldValueRangeIter<'a, StreamValueId>),
    FieldReference(FieldValueRangeIter<'a, FieldReference>),
    SlicedFieldReference(FieldValueRangeIter<'a, SlicedFieldReference>),
}

impl<'a> DynFieldValueRangeIter<'a> {
    pub fn new(range: &ValidTypedRange<'a>) -> Self {
        metamatch!(match range.data {
            #[expand(for REP in [Null, Undefined])]
            FieldValueSlice::REP(n) => DynFieldValueRangeIter::REP(n),

            FieldValueSlice::TextInline(vals) =>
                DynFieldValueRangeIter::TextInline(InlineTextIter::from_range(
                    range, vals
                )),
            FieldValueSlice::BytesInline(vals) =>
                DynFieldValueRangeIter::BytesInline(
                    InlineBytesIter::from_range(range, vals)
                ),

            #[expand(for REP in [
                Bool, Int, BigInt, Float, BigRational,
                TextBuffer, BytesBuffer,
                Object, Array, Argument, Custom, Error, OpDecl,
                StreamValueId, FieldReference, SlicedFieldReference,
            ])]
            FieldValueSlice::REP(vals) => DynFieldValueRangeIter::REP(
                FieldValueRangeIter::from_valid_range(range, vals),
            ),
        })
    }
    pub fn peek(&self) -> Option<(FieldValueRef<'a>, RunLength)> {
        metamatch!(match self {
            #[expand(for REP in [Null, Undefined])]
            DynFieldValueRangeIter::REP(it) => {
                if *it == 0 {
                    None
                } else {
                    Some((
                        FieldValueRef::REP,
                        (*it).min(RUN_LEN_MAX_USIZE) as RunLength,
                    ))
                }
            }

            #[expand(for (REP, KIND) in [
                (TextInline, Text),
                (TextBuffer, Text),
                (BytesInline, Bytes),
                (BytesBuffer, Bytes)
            ])]
            DynFieldValueRangeIter::REP(it) => {
                let (v, rl) = it.peek()?;
                Some((FieldValueRef::KIND(v), rl))
            }

            #[expand(for REP in [
                Bool, Int, BigInt, Float, BigRational,
                Object, Array, Argument, Custom, Error, OpDecl,
                StreamValueId, FieldReference, SlicedFieldReference,
            ])]
            DynFieldValueRangeIter::REP(it) => {
                let (v, rl) = it.peek()?;
                Some((FieldValueRef::REP(v), rl))
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
            #[expand(for REP in [
                Bool, Int, BigInt,Float, BigRational, TextInline, TextBuffer,
                BytesInline, BytesBuffer, Object, Array,
                Argument, OpDecl, Custom, Error,
                StreamValueId, FieldReference, SlicedFieldReference,
            ])]
            DynFieldValueRangeIter::REP(it) => {
                it.next_n_fields(n)
            }
        })
    }
    pub fn next_block(&mut self) -> Option<DynFieldValueBlock> {
        metamatch!(match self {
            #[expand(for REP in [Null, Undefined])]
            DynFieldValueRangeIter::REP(it) => {
                if *it == 0 {
                    None
                } else {
                    let rl = (*it).min(RUN_LEN_MAX_USIZE);
                    *it -= rl;
                    Some(DynFieldValueBlock::Plain(FieldValueSlice::REP(rl)))
                }
            }
            #[expand(for (REP, KIND)  in [
                (TextInline, Text),
                (BytesInline, Bytes)
            ])]
            DynFieldValueRangeIter::REP(it) => {
                let (v, rl) = it.next()?;
                Some(if rl == 1 {
                    DynFieldValueBlock::Plain(FieldValueSlice::REP(v))
                } else {
                    DynFieldValueBlock::WithRunLength(
                        FieldValueRef::KIND(v),
                        rl,
                    )
                })
            }

            #[expand(for (REP, KIND)  in [
                (TextBuffer, Text),
                (BytesBuffer, Bytes)
            ])]
            DynFieldValueRangeIter::REP(it) => {
                Some(match it.next_block()? {
                    FieldValueBlock::Plain(v) => {
                        DynFieldValueBlock::Plain(FieldValueSlice::REP(v))
                    }
                    FieldValueBlock::WithRunLength(v, rl) => {
                        DynFieldValueBlock::WithRunLength(
                            FieldValueRef::KIND(v),
                            rl,
                        )
                    }
                })
            }

            #[expand(for REP in [
                Bool, Int, BigInt, Float, BigRational,
                Object, Array, Argument, OpDecl, Custom, Error,
                StreamValueId, FieldReference, SlicedFieldReference,
            ])]
            DynFieldValueRangeIter::REP(it) => {
                Some(match it.next_block()? {
                    FieldValueBlock::Plain(v) => {
                        DynFieldValueBlock::Plain(FieldValueSlice::REP(v))
                    }
                    FieldValueBlock::WithRunLength(v, rl) => {
                        DynFieldValueBlock::WithRunLength(
                            FieldValueRef::REP(v),
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
            #[expand(for REP in [Null, Undefined])]
            DynFieldValueRangeIter::REP(it) => {
                if *it == 0 {
                    None
                } else {
                    let rl = (*it).min(RUN_LEN_MAX_USIZE);
                    *it -= rl;
                    Some((FieldValueRef::REP, rl as RunLength))
                }
            }
            #[expand(for (ITER, REP) in [
                (TextInline, Text),
                (TextBuffer, Text),
                (BytesInline, Bytes),
                (BytesBuffer, Bytes)
            ])]
            DynFieldValueRangeIter::ITER(it) => {
                let (v, rl) = it.next()?;
                Some((FieldValueRef::REP(v), rl))
            }

            #[expand(for REP in [
                Bool, Int, BigInt, Float, BigRational,
                Object, Array, Argument, OpDecl, Custom, Error,
                StreamValueId, FieldReference, SlicedFieldReference,
            ])]
            DynFieldValueRangeIter::REP(it) => {
                let (v, rl) = it.next()?;
                Some((FieldValueRef::REP(v), rl))
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
