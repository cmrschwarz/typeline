use std::{mem::ManuallyDrop, ops::Range};

use metamatch::metamatch;
use num::{BigInt, BigRational};

use super::{
    array::Array,
    custom_data::CustomDataBox,
    field_data::{
        FieldValueFormat, FieldValueHeader, FieldValueRepr, FieldValueType,
        RunLength,
    },
    field_data_ref::FieldDataRef,
    field_value::{
        FieldReference, FieldValue, FieldValueKind, Object,
        SlicedFieldReference,
    },
    iter::field_value_slice_iter::FieldValueSliceIter,
    stream_value::StreamValueId,
};
use crate::{
    cli::call_expr::Argument,
    operators::{errors::OperatorApplicationError, macro_def::MacroRef},
    utils::maybe_text::MaybeTextRef,
};

#[derive(Clone, Copy)]
pub enum FieldValueRef<'a> {
    Null,
    Undefined,
    Int(&'a i64),
    BigInt(&'a BigInt),
    Float(&'a f64),
    BigRational(&'a BigRational),
    Text(&'a str),
    Bytes(&'a [u8]),
    Array(&'a Array),
    Object(&'a Object),
    Custom(&'a CustomDataBox),
    StreamValueId(&'a StreamValueId),
    Error(&'a OperatorApplicationError),
    Argument(&'a Argument),
    Macro(&'a MacroRef),
    FieldReference(&'a FieldReference),
    SlicedFieldReference(&'a SlicedFieldReference),
}

pub enum FieldValueRefMut<'a> {
    Null,
    Undefined,
    Int(&'a mut i64),
    BigInt(&'a mut BigInt),
    Float(&'a mut f64),
    BigRational(&'a mut BigRational),
    InlineText(&'a mut str),
    InlineBytes(&'a mut [u8]),
    TextBuffer(&'a mut String),
    BytesBuffer(&'a mut Vec<u8>),
    Array(&'a mut Array),
    Object(&'a mut Object),
    Custom(&'a mut CustomDataBox),
    StreamValueId(&'a mut StreamValueId),
    Error(&'a mut OperatorApplicationError),
    Argument(&'a mut Argument),
    Macro(&'a mut MacroRef),
    FieldReference(&'a mut FieldReference),
    SlicedFieldReference(&'a mut SlicedFieldReference),
}

#[derive(Clone, Copy)]
pub enum FieldValueSlice<'a> {
    Null(usize),
    Undefined(usize),
    Int(&'a [i64]),
    BigInt(&'a [BigInt]),
    Float(&'a [f64]),
    BigRational(&'a [BigRational]),
    TextInline(&'a str),
    TextBuffer(&'a [String]),
    BytesInline(&'a [u8]),
    BytesBuffer(&'a [Vec<u8>]),
    Object(&'a [Object]),
    Array(&'a [Array]),
    Custom(&'a [CustomDataBox]),
    Error(&'a [OperatorApplicationError]),
    Argument(&'a [Argument]),
    Macro(&'a [MacroRef]),
    StreamValueId(&'a [StreamValueId]),
    FieldReference(&'a [FieldReference]),
    SlicedFieldReference(&'a [SlicedFieldReference]),
}

#[derive(Clone, Copy)]
pub enum FieldValueBlock<'a, T> {
    Plain(&'a [T]),
    WithRunLength(&'a T, RunLength),
}

pub enum DynFieldValueBlock<'a> {
    Plain(FieldValueSlice<'a>),
    WithRunLength(FieldValueRef<'a>, RunLength),
}

pub struct TypedField<'a> {
    pub header: FieldValueHeader,
    pub value: FieldValueRef<'a>,
}

#[derive(Default)]
pub struct TypedRange<'a> {
    pub headers: &'a [FieldValueHeader],
    pub data: FieldValueSlice<'a>,
    pub field_count: usize,
    pub first_header_run_length_oversize: RunLength,
    pub last_header_run_length_oversize: RunLength,
}

// SAFETY: the range contained in this header is non writable outside of this
// module. Therefore, nobody outside this module can (safely) construct a
// ValidTypedRange. We can therefore assume all instances to be valid (header
// matches data)
#[derive(Default, derive_more::Deref)]
pub struct ValidTypedRange<'a>(TypedRange<'a>);

unsafe fn to_slice<'a, T: Sized, R: FieldDataRef>(
    fdr: &'a R,
    data_begin: usize,
    data_end: usize,
) -> &'a [T] {
    if data_begin == data_end {
        return &[];
    }
    let data = fdr.data();
    #[cfg(debug_assertions)]
    {
        let slice_0_len = data.slice_lengths().0;
        debug_assert!(data_begin >= slice_0_len || data_end <= slice_0_len);
    }

    unsafe {
        std::slice::from_raw_parts::<T>(
            data.ptr_from_index(data_begin).cast(),
            (data_end - data_begin) / std::mem::size_of::<T>(),
        )
    }
}
unsafe fn to_ref<'a, T: Sized, R: FieldDataRef>(
    fdr: &'a R,
    data_begin: usize,
) -> &'a T {
    unsafe { &*fdr.data().ptr_from_index(data_begin).cast() }
}

pub unsafe fn value_as_bytes<T>(v: &T) -> &[u8] {
    unsafe {
        std::slice::from_raw_parts(
            (v as *const T).cast::<u8>(),
            std::mem::size_of_val(v),
        )
    }
}
pub unsafe fn slice_as_bytes<T>(v: &[T]) -> &[u8] {
    unsafe {
        std::slice::from_raw_parts(v.as_ptr().cast(), std::mem::size_of_val(v))
    }
}

unsafe fn drop_slice<T>(slice_start_ptr: *mut u8, len: usize) {
    unsafe {
        let droppable = std::slice::from_raw_parts_mut(
            slice_start_ptr.cast::<ManuallyDrop<T>>(),
            len,
        );
        for e in droppable.iter_mut() {
            ManuallyDrop::drop(e);
        }
    }
}

impl<'a> FieldValueRef<'a> {
    pub unsafe fn new<R: FieldDataRef>(
        fdr: &'a R,
        fmt: FieldValueFormat,
        data_begin: usize,
    ) -> Self {
        unsafe {
            metamatch!(match fmt.repr {
                #[expand(T in [Null, Undefined])]
                FieldValueRepr::T => FieldValueRef::T,

                FieldValueRepr::BytesInline => FieldValueRef::Bytes(to_slice(
                    fdr,
                    data_begin,
                    data_begin + fmt.size as usize,
                )),
                FieldValueRepr::TextInline => FieldValueRef::Text(
                    std::str::from_utf8_unchecked(to_slice(
                        fdr,
                        data_begin,
                        data_begin + fmt.size as usize,
                    )),
                ),
                FieldValueRepr::BytesBuffer => {
                    FieldValueRef::Bytes(to_ref::<Vec<u8>, R>(fdr, data_begin))
                }
                FieldValueRepr::TextBuffer => {
                    FieldValueRef::Text(to_ref::<String, R>(fdr, data_begin))
                }

                #[expand(REP in [
                    Int, BigInt, Float, BigRational, StreamValueId, FieldReference,
                    SlicedFieldReference, Error, Macro,
                    Object, Array, Custom, Argument
                ])]
                FieldValueRepr::REP => {
                    FieldValueRef::REP(to_ref(fdr, data_begin))
                }
            })
        }
    }
    pub fn as_slice(&self) -> FieldValueSlice<'a> {
        use std::slice::from_ref;

        metamatch!(match self {
            #[expand(REP in [Null, Undefined])]
            FieldValueRef::REP => FieldValueSlice::REP(1),

            #[expand((KIND, REP) in [(Text, TextInline), (Bytes, BytesInline)])]
            FieldValueRef::KIND(v) => FieldValueSlice::REP(v),

            #[expand(REP in [
                Int, BigInt, Float, BigRational, StreamValueId, FieldReference,
                SlicedFieldReference, Error, Argument, Macro,
                Object, Array, Custom,
            ])]
            FieldValueRef::REP(v) => FieldValueSlice::REP(from_ref(v)),
        })
    }
    pub fn to_field_value(&self) -> FieldValue {
        metamatch!(match *self {
            #[expand(REP in [Null, Undefined])]
            FieldValueRef::REP => FieldValue::REP,

            #[expand(REP in [
                Int, Float, StreamValueId, FieldReference,
                SlicedFieldReference,
            ])]
            FieldValueRef::REP(v) => FieldValue::REP(*v),

            #[expand(REP in [Error, Array, Custom, Macro])]
            FieldValueRef::REP(v) => FieldValue::REP(v.clone()),

            #[expand((REP, CONV_FN) in [
                (Text, to_string),
                (Bytes, to_vec)
            ])]
            FieldValueRef::REP(v) => FieldValue::REP(v.CONV_FN()),

            #[expand(REP in [BigInt, BigRational, Argument, Object])]
            FieldValueRef::REP(v) => FieldValue::REP(Box::new(v.clone())),
        })
    }
    pub fn repr(&self) -> FieldValueRepr {
        self.as_slice().repr()
    }
    pub fn subslice(&self, range: Range<usize>) -> Self {
        metamatch!(match self {
            FieldValueRef::Argument(a) => a.value.as_ref().subslice(range),

            FieldValueRef::Array(_) => todo!(),

            #[expand(REP in [Text, Bytes])]
            FieldValueRef::REP(v) => FieldValueRef::REP(&v[range]),

            FieldValueRef::Null | FieldValueRef::Undefined |
            #[expand_pattern(REP in [
                Int, BigInt, Float, BigRational, Object, Custom, StreamValueId,
                Error, FieldReference, SlicedFieldReference, Macro,
            ])]
            FieldValueRef::REP(_) => {
                panic!("typed value kind {:?} is not slicable", self.repr(),)
            }
        })
    }

    pub fn text_or_bytes(&self) -> Option<&'a [u8]> {
        match self {
            FieldValueRef::Text(v) => Some(v.as_bytes()),
            FieldValueRef::Bytes(v) => Some(v),
            FieldValueRef::Argument(v) => v.value.text_or_bytes(),
            FieldValueRef::Null
            | FieldValueRef::Undefined
            | FieldValueRef::Int(_)
            | FieldValueRef::BigInt(_)
            | FieldValueRef::Float(_)
            | FieldValueRef::BigRational(_)
            | FieldValueRef::Array(_)
            | FieldValueRef::Object(_)
            | FieldValueRef::Macro(_)
            | FieldValueRef::Custom(_)
            | FieldValueRef::StreamValueId(_)
            | FieldValueRef::Error(_)
            | FieldValueRef::FieldReference(_)
            | FieldValueRef::SlicedFieldReference(_) => None,
        }
    }

    pub fn as_maybe_text_ref(&self) -> Option<MaybeTextRef> {
        match self {
            FieldValueRef::Text(v) => Some(MaybeTextRef::Text(v)),
            FieldValueRef::Bytes(v) => Some(MaybeTextRef::Bytes(v)),
            _ => None,
        }
    }

    pub fn kind(&self) -> FieldValueKind {
        self.repr().kind()
    }
}

impl<'a> From<MaybeTextRef<'a>> for FieldValueRef<'a> {
    fn from(value: MaybeTextRef<'a>) -> Self {
        match value {
            MaybeTextRef::Text(v) => FieldValueRef::Text(v),
            MaybeTextRef::Bytes(v) => FieldValueRef::Bytes(v),
        }
    }
}

impl<'a> TypedField<'a> {
    pub unsafe fn new<R: FieldDataRef>(
        fdr: &'a R,
        fmt: FieldValueFormat,
        data_begin: usize,
        run_len: RunLength,
    ) -> TypedField<'a> {
        let value = unsafe { FieldValueRef::new(fdr, fmt, data_begin) };
        TypedField {
            header: FieldValueHeader {
                fmt,
                run_length: run_len,
            },
            value,
        }
    }
}

impl<'a> Default for FieldValueSlice<'a> {
    fn default() -> Self {
        FieldValueSlice::Null(0)
    }
}

impl<'a> FieldValueSlice<'a> {
    pub unsafe fn new<R: FieldDataRef>(
        fdr: &'a R,
        fmt: FieldValueFormat,
        data_begin: usize,
        data_end: usize,
        field_count: usize,
    ) -> FieldValueSlice<'a> {
        unsafe {
            metamatch!(match fmt.repr {
                FieldValueRepr::Undefined => {
                    FieldValueSlice::Undefined(field_count)
                }
                FieldValueRepr::Null => FieldValueSlice::Null(field_count),

                FieldValueRepr::TextInline => {
                    FieldValueSlice::TextInline(std::str::from_utf8_unchecked(
                        to_slice(fdr, data_begin, data_end),
                    ))
                }

                #[expand(REP in [
                    Int, BigInt, Float, BigRational, TextBuffer, BytesInline,
                    BytesBuffer, Object, Array, Custom, Error, StreamValueId,
                    FieldReference, SlicedFieldReference, Argument, Macro
                ])]
                FieldValueRepr::REP => {
                    FieldValueSlice::REP(to_slice(fdr, data_begin, data_end))
                }
            })
        }
    }
    pub fn as_bytes(&self) -> &'a [u8] {
        unsafe {
            metamatch!(match *self {
                FieldValueSlice::Undefined(_) | FieldValueSlice::Null(_) => {
                    &[]
                }
                FieldValueSlice::BytesInline(v) => v,
                FieldValueSlice::TextInline(v) => v.as_bytes(),

                #[expand(REP in [
                    Int, BigInt, Float, BigRational, StreamValueId,
                    FieldReference, SlicedFieldReference, Error,
                    BytesBuffer, TextBuffer, Object, Array, Argument,
                    Macro, Custom
                ])]
                FieldValueSlice::REP(v) => slice_as_bytes(v),
            })
        }
    }
    pub fn repr(&self) -> FieldValueRepr {
        metamatch!(match self {
            #[expand(REP in [
                Undefined, Null, BytesInline, TextInline,
                Int, BigInt, Float, BigRational, StreamValueId, FieldReference,
                SlicedFieldReference, Error,
                BytesBuffer, TextBuffer, Object, Array, Argument, Macro, Custom
            ])]
            FieldValueSlice::REP(_) => FieldValueRepr::REP,
        })
    }
    pub fn kind(&self) -> FieldValueKind {
        self.repr().kind()
    }
    pub fn len(&self) -> usize {
        metamatch!(match self {
            FieldValueSlice::Undefined(v) | FieldValueSlice::Null(v) => *v,
            #[expand(REP in [
                BytesInline, TextInline,
                Int, BigInt, Float, BigRational, StreamValueId, FieldReference,
                SlicedFieldReference, Error,
                BytesBuffer, TextBuffer, Object, Array, Argument, Macro, Custom
            ])]
            FieldValueSlice::REP(v) => v.len(),
        })
    }
    // like `len`, but 1 for `TextInline` and `BytesInline`,
    // as those don't can't really carry multiple entries
    pub fn run_len(&self) -> usize {
        match self {
            FieldValueSlice::Undefined(v) | FieldValueSlice::Null(v) => *v,
            FieldValueSlice::TextInline(_)
            | FieldValueSlice::BytesInline(_) => 1,
            _ => self.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn matches_values<T: FieldValueType + 'static>(
        &self,
        values: &[T],
    ) -> bool {
        if T::REPR != self.repr() {
            return false;
        }
        values.len() == self.len()
    }
    pub unsafe fn drop_from_kind(
        ptr: *mut u8,
        len: usize,
        repr: FieldValueRepr,
    ) {
        unsafe {
            metamatch!(match repr {
                #[expand((REP, TYPE) in [
                    (BigInt, BigInt),
                    (BigRational, BigRational),
                    (TextBuffer, String),
                    (BytesBuffer, Vec<u8>),
                    (Object, Object),
                    (Array, Array),
                    (Argument, Argument),
                    (Macro, MacroRef),
                    (Custom, CustomDataBox),
                    (Error, OperatorApplicationError)
                ])]
                FieldValueRepr::REP => {
                    #[allow(clippy::assertions_on_constants)]
                    {
                        debug_assert!(
                            !<TYPE as FieldValueType>::TRIVIALLY_COPYABLE
                        );
                    }
                    drop_slice::<TYPE>(ptr, len)
                }
                #[expand_pattern(REP in [
                    Null, Undefined, Int, Float, TextInline, BytesInline,
                    StreamValueId, FieldReference, SlicedFieldReference,
                ])]
                FieldValueRepr::REP => {
                    debug_assert!(repr.is_trivially_copyable(), "{repr}");
                }
            })
        }
    }
}

impl<'a> IntoIterator for FieldValueSlice<'a> {
    type Item = FieldValueRef<'a>;

    type IntoIter = FieldValueSliceIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        FieldValueSliceIter::new(self)
    }
}

impl<'a> TypedRange<'a> {
    pub fn new<R: FieldDataRef>(
        fdr: &'a R,
        fmt: FieldValueFormat,
        data_begin: usize,
        data_end: usize,
        field_count: usize,
        header_begin: usize,
        header_end: usize,
        first_header_run_length_oversize: RunLength,
        last_header_run_length_oversize: RunLength,
    ) -> TypedRange<'a> {
        let (h_s1, h_s2) = fdr.headers().as_slices();
        let headers = if header_begin < h_s1.len() {
            &h_s1[header_begin..header_end]
        } else {
            &h_s2[header_begin - h_s1.len()..header_end - h_s1.len()]
        };
        let data = unsafe {
            FieldValueSlice::new(fdr, fmt, data_begin, data_end, field_count)
        };
        TypedRange {
            headers,
            data,
            field_count,
            first_header_run_length_oversize,
            last_header_run_length_oversize,
        }
    }
}

impl<'a> ValidTypedRange<'a> {
    pub unsafe fn new_unchecked(range: TypedRange<'a>) -> Self {
        Self(range)
    }
}

impl<'a, T> FieldValueBlock<'a, T> {
    pub fn len(&self) -> usize {
        match self {
            FieldValueBlock::Plain(v) => v.len(),
            FieldValueBlock::WithRunLength(_, rl) => *rl as usize,
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl DynFieldValueBlock<'_> {
    pub fn run_len(&self) -> usize {
        match self {
            DynFieldValueBlock::Plain(p) => p.run_len(),
            DynFieldValueBlock::WithRunLength(_, rl) => *rl as usize,
        }
    }
}
