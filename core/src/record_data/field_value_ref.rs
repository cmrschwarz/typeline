use std::{mem::ManuallyDrop, ops::Range};

use metamatch::metamatch;
use num::{BigInt, BigRational};

use super::{
    array::Array,
    custom_data::CustomDataBox,
    dyn_ref_iter::FieldValueSliceIter,
    field_data::{
        FieldValueFormat, FieldValueHeader, FieldValueRepr, FieldValueType,
        RunLength,
    },
    field_value::{FieldReference, FieldValue, Object, SlicedFieldReference},
    iters::FieldDataRef,
    stream_value::StreamValueId,
};
use crate::{
    cli::call_expr::Argument, operators::errors::OperatorApplicationError,
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
    StreamValueId(&'a [StreamValueId]),
    FieldReference(&'a [FieldReference]),
    SlicedFieldReference(&'a [SlicedFieldReference]),
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
pub struct ValidTypedRange<'a>(pub(super) TypedRange<'a>);

impl<'a> ValidTypedRange<'a> {
    pub unsafe fn new_unchecked(range: TypedRange<'a>) -> Self {
        ValidTypedRange(range)
    }
}

impl<'a> FieldValueRef<'a> {
    pub unsafe fn new<R: FieldDataRef<'a>>(
        fdr: R,
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

                #[expand(REPR in [
                    Int, BigInt, Float, BigRational, StreamValueId, FieldReference,
                    SlicedFieldReference, Error,
                    Object, Array, Custom, Argument
                ])]
                FieldValueRepr::REPR => {
                    FieldValueRef::REPR(to_ref(fdr, data_begin))
                }
            })
        }
    }
    pub fn as_slice(&self) -> FieldValueSlice<'a> {
        use std::slice::from_ref;

        metamatch!(match self {
            #[expand(T in [Null, Undefined])]
            FieldValueRef::T => FieldValueSlice::T(1),

            #[expand((KIND, REPR) in [(Text, TextInline), (Bytes, BytesInline)])]
            FieldValueRef::KIND(v) => FieldValueSlice::REPR(v),

            #[expand(REPR in [
                Int, BigInt, Float, BigRational, StreamValueId, FieldReference,
                SlicedFieldReference, Error, Argument,
                Object, Array, Custom,
            ])]
            FieldValueRef::REPR(v) => FieldValueSlice::REPR(from_ref(v)),
        })
    }
    pub fn to_field_value(&self) -> FieldValue {
        metamatch!(match *self {
            #[expand(T in [Null, Undefined])]
            FieldValueRef::T => FieldValue::T,

            #[expand(T in [
                Int, Float, StreamValueId, FieldReference,
                SlicedFieldReference,
            ])]
            FieldValueRef::T(v) => FieldValue::T(*v),

            #[expand(T in [Error, Object, Array, Custom,])]
            FieldValueRef::T(v) => FieldValue::T(v.clone()),

            #[expand((T, CONV_FN) in [
                (Text, to_string),
                (Bytes, to_vec)
            ])]
            FieldValueRef::T(v) => FieldValue::T(v.CONV_FN()),

            #[expand(T in [BigInt, BigRational, Argument])]
            FieldValueRef::T(v) => FieldValue::T(Box::new(v.clone())),
        })
    }
    pub fn repr(&self) -> FieldValueRepr {
        self.as_slice().repr()
    }
    pub fn subslice(&self, range: Range<usize>) -> Self {
        metamatch!(match self {
            FieldValueRef::Argument(a) => a.value.as_ref().subslice(range),

            FieldValueRef::Array(_) => todo!(),

            #[expand(T in [Text, Bytes])]
            FieldValueRef::T(v) => FieldValueRef::T(&v[range]),

            FieldValueRef::Null
            | FieldValueRef::Undefined
            | FieldValueRef::Int(_)
            | FieldValueRef::BigInt(_)
            | FieldValueRef::Float(_)
            | FieldValueRef::BigRational(_)
            | FieldValueRef::Object(_)
            | FieldValueRef::Custom(_)
            | FieldValueRef::StreamValueId(_)
            | FieldValueRef::Error(_)
            | FieldValueRef::FieldReference(_)
            | FieldValueRef::SlicedFieldReference(_) => {
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
            | FieldValueRef::Custom(_)
            | FieldValueRef::StreamValueId(_)
            | FieldValueRef::Error(_)
            | FieldValueRef::FieldReference(_)
            | FieldValueRef::SlicedFieldReference(_) => None,
        }
    }
}

impl<'a> TypedField<'a> {
    pub unsafe fn new<R: FieldDataRef<'a>>(
        fdr: R,
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

impl<'a> FieldValueSlice<'a> {
    pub unsafe fn new<R: FieldDataRef<'a>>(
        fdr: R,
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

                #[expand(REPR in [
                    Int, BigInt, Float, BigRational, TextBuffer, BytesInline,
                    BytesBuffer, Object, Array, Custom, Error, StreamValueId,
                    FieldReference, SlicedFieldReference, Argument
                ])]
                FieldValueRepr::REPR => {
                    FieldValueSlice::REPR(to_slice(fdr, data_begin, data_end))
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

                #[expand(REPR in [
                    Int, BigInt, Float, BigRational, StreamValueId,
                    FieldReference, SlicedFieldReference, Error,
                    BytesBuffer, TextBuffer, Object, Array, Argument, Custom
                ])]
                FieldValueSlice::REPR(v) => slice_as_bytes(v),
            })
        }
    }
    pub fn repr(&self) -> FieldValueRepr {
        metamatch!(match self {
            #[expand(REPR in [
                Undefined, Null, BytesInline, TextInline,
                Int, BigInt, Float, BigRational, StreamValueId, FieldReference,
                SlicedFieldReference, Error,
                BytesBuffer, TextBuffer, Object, Array, Argument, Custom
            ])]
            FieldValueSlice::REPR(_) => FieldValueRepr::REPR,
        })
    }
    pub fn len(&self) -> usize {
        metamatch!(match self {
            FieldValueSlice::Undefined(v) | FieldValueSlice::Null(v) => *v,
            #[expand(REPR in [
                BytesInline, TextInline,
                Int, BigInt, Float, BigRational, StreamValueId, FieldReference,
                SlicedFieldReference, Error,
                BytesBuffer, TextBuffer, Object, Array, Argument, Custom
            ])]
            FieldValueSlice::REPR(v) => v.len(),
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
                #[expand((REPR, TYPE) in [
                    (BigInt, BigInt),
                    (BigRational, BigRational),
                    (TextBuffer, String),
                    (BytesBuffer, Vec<u8>),
                    (Object, Object),
                    (Array, Array),
                    (Argument, Argument),
                    (Custom, CustomDataBox),
                    (Error, OperatorApplicationError)
                ])]
                FieldValueRepr::REPR => {
                    #[allow(clippy::assertions_on_constants)]
                    {
                        debug_assert!(
                            !<TYPE as FieldValueType>::TRIVIALLY_COPYABLE
                        );
                    }
                    drop_slice::<TYPE>(ptr, len)
                }
                #[expand_pattern(REPR in [
                    Null, Undefined, Int, Float, TextInline, BytesInline,
                    StreamValueId, FieldReference, SlicedFieldReference,
                ])]
                FieldValueRepr::REPR => {
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
    pub unsafe fn new<R: FieldDataRef<'a>>(
        fdr: R,
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
    pub unsafe fn new(range: TypedRange<'a>) -> Self {
        Self(range)
    }
}

unsafe fn to_slice<'a, T: Sized, R: FieldDataRef<'a>>(
    fdr: R,
    data_begin: usize,
    data_end: usize,
) -> &'a [T] {
    if data_begin == data_end {
        return &[];
    }
    unsafe {
        std::slice::from_raw_parts::<T>(
            fdr.data().ptr_from_index(data_begin).cast(),
            (data_end - data_begin) / std::mem::size_of::<T>(),
        )
    }
}

unsafe fn to_ref<'a, T: Sized, R: FieldDataRef<'a>>(
    fdr: R,
    data_begin: usize,
) -> &'a T {
    unsafe { &*fdr.data().ptr_from_index(data_begin).cast() }
}
