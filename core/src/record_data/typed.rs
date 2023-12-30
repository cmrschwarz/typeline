use std::{mem::ManuallyDrop, ops::Range, ptr::NonNull};

use num::{BigInt, BigRational};

use super::{
    custom_data::CustomDataBox,
    field_data::{
        FieldValueFormat, FieldValueHeader, FieldValueRepr, FieldValueType,
        RunLength, TextBufferFile,
    },
    field_value::{
        Array, FieldReference, Null, Object, SlicedFieldReference, Undefined,
    },
    iters::FieldDataRef,
    stream_value::StreamValueId,
};
use crate::{
    operators::errors::OperatorApplicationError,
    record_data::field_data::BytesBufferFile,
};
use std::ops::Deref;

#[derive(Clone)]
pub enum TypedValue<'a> {
    Null(Null),
    Undefined(Undefined),
    Int(&'a i64),
    BigInt(&'a BigInt),
    Float(&'a f64),
    Rational(&'a BigRational),
    TextInline(&'a str),
    BytesInline(&'a [u8]),
    TextBuffer(&'a String),
    BytesBuffer(&'a Vec<u8>),
    Array(&'a Array),
    Object(&'a Object),
    Custom(&'a CustomDataBox),
    StreamValueId(&'a StreamValueId),
    Error(&'a OperatorApplicationError),
    FieldReference(&'a FieldReference),
    SlicedFieldReference(&'a SlicedFieldReference),
}

impl<'a> TypedValue<'a> {
    pub unsafe fn new<R: FieldDataRef<'a>>(
        fdr: R,
        fmt: FieldValueFormat,
        data_begin: usize,
    ) -> Self {
        unsafe {
            match fmt.repr {
                FieldValueRepr::Null => TypedValue::Null(Null),
                FieldValueRepr::Undefined => TypedValue::Undefined(Undefined),
                FieldValueRepr::BytesInline => TypedValue::BytesInline(
                    to_slice(fdr, data_begin, data_begin + fmt.size as usize),
                ),
                FieldValueRepr::TextInline => TypedValue::TextInline(
                    std::str::from_utf8_unchecked(to_slice(
                        fdr,
                        data_begin,
                        data_begin + fmt.size as usize,
                    )),
                ),
                FieldValueRepr::Int => {
                    TypedValue::Int(to_ref(fdr, data_begin))
                }
                FieldValueRepr::BigInt => {
                    TypedValue::BigInt(to_ref(fdr, data_begin))
                }
                FieldValueRepr::Float => {
                    TypedValue::Float(to_ref(fdr, data_begin))
                }
                FieldValueRepr::Rational => {
                    TypedValue::Rational(to_ref(fdr, data_begin))
                }
                FieldValueRepr::StreamValueId => {
                    TypedValue::StreamValueId(to_ref(fdr, data_begin))
                }
                FieldValueRepr::FieldReference => {
                    TypedValue::FieldReference(to_ref(fdr, data_begin))
                }
                FieldValueRepr::SlicedFieldReference => {
                    TypedValue::SlicedFieldReference(to_ref(fdr, data_begin))
                }
                FieldValueRepr::Error => {
                    TypedValue::Error(to_ref(fdr, data_begin))
                }
                FieldValueRepr::Object => {
                    TypedValue::Object(to_ref(fdr, data_begin))
                }
                FieldValueRepr::Array => {
                    TypedValue::Array(to_ref(fdr, data_begin))
                }
                FieldValueRepr::Custom => {
                    TypedValue::Custom(to_ref(fdr, data_begin))
                }
                FieldValueRepr::BytesBuffer => {
                    TypedValue::BytesBuffer(to_ref(fdr, data_begin))
                }
                FieldValueRepr::TextBuffer => {
                    TypedValue::TextBuffer(to_ref(fdr, data_begin))
                }
                FieldValueRepr::BytesFile => todo!(),
                FieldValueRepr::TextFile => todo!(),
            }
        }
    }
    pub fn as_slice(&self) -> TypedSlice<'a> {
        use std::slice::from_ref;
        match self {
            TypedValue::Undefined(_) => TypedSlice::Undefined(&[Undefined]),
            TypedValue::Null(_) => TypedSlice::Null(&[Null]),
            TypedValue::Int(v) => TypedSlice::Int(from_ref(v)),
            TypedValue::BigInt(v) => TypedSlice::BigInt(from_ref(v)),
            TypedValue::Float(v) => TypedSlice::Float(from_ref(v)),
            TypedValue::Rational(v) => TypedSlice::Rational(from_ref(v)),
            TypedValue::StreamValueId(v) => {
                TypedSlice::StreamValueId(from_ref(v))
            }
            TypedValue::FieldReference(v) => {
                TypedSlice::FieldReference(from_ref(v))
            }
            TypedValue::SlicedFieldReference(v) => {
                TypedSlice::SlicedFieldReference(from_ref(v))
            }
            TypedValue::Error(v) => TypedSlice::Error(from_ref(v)),
            TypedValue::TextInline(v) => TypedSlice::TextInline(v),
            TypedValue::BytesInline(v) => TypedSlice::BytesInline(v),
            TypedValue::TextBuffer(v) => TypedSlice::TextBuffer(from_ref(v)),
            TypedValue::BytesBuffer(v) => TypedSlice::BytesBuffer(from_ref(v)),
            TypedValue::Object(v) => TypedSlice::Object(from_ref(v)),
            TypedValue::Array(v) => TypedSlice::Array(from_ref(v)),
            TypedValue::Custom(v) => TypedSlice::Custom(from_ref(v)),
        }
    }
    pub fn repr(&self) -> FieldValueRepr {
        self.as_slice().repr()
    }
    pub fn subslice(&self, range: Range<usize>) -> Self {
        match self {
            TypedValue::BytesInline(v) => TypedValue::BytesInline(&v[range]),
            TypedValue::TextInline(v) => TypedValue::TextInline(&v[range]),
            TypedValue::BytesBuffer(v) => TypedValue::BytesInline(&v[range]),
            TypedValue::TextBuffer(v) => TypedValue::TextInline(&v[range]),
            TypedValue::Null(_)
            | TypedValue::Undefined(_)
            | TypedValue::Int(_)
            | TypedValue::BigInt(_)
            | TypedValue::Float(_)
            | TypedValue::Rational(_)
            | TypedValue::Array(_)
            | TypedValue::Object(_)
            | TypedValue::Custom(_)
            | TypedValue::StreamValueId(_)
            | TypedValue::Error(_)
            | TypedValue::FieldReference(_)
            | TypedValue::SlicedFieldReference(_) => {
                panic!("typed value kind {:?} is not slicable", self.repr(),)
            }
        }
    }
}

pub struct TypedField<'a> {
    pub header: FieldValueHeader,
    pub value: TypedValue<'a>,
}

impl<'a> TypedField<'a> {
    pub unsafe fn new<R: FieldDataRef<'a>>(
        fdr: R,
        fmt: FieldValueFormat,
        data_begin: usize,
        run_len: RunLength,
    ) -> TypedField<'a> {
        let value = unsafe { TypedValue::new(fdr, fmt, data_begin) };
        TypedField {
            header: FieldValueHeader {
                fmt,
                run_length: run_len,
            },
            value,
        }
    }
}

#[derive(Clone, Copy)]
pub enum TypedSlice<'a> {
    Null(&'a [Null]),
    Undefined(&'a [Undefined]),
    Int(&'a [i64]),
    BigInt(&'a [BigInt]),
    Float(&'a [f64]),
    Rational(&'a [BigRational]),
    TextInline(&'a str),
    TextBuffer(&'a [String]),
    BytesInline(&'a [u8]),
    BytesBuffer(&'a [Vec<u8>]),
    Object(&'a [Object]),
    Array(&'a [Array]),
    Custom(&'a [CustomDataBox]),
    Error(&'a [OperatorApplicationError]),
    StreamValueId(&'a [StreamValueId]),
    FieldReference(&'a [FieldReference]),
    SlicedFieldReference(&'a [SlicedFieldReference]),
}

impl<'a> Default for TypedSlice<'a> {
    fn default() -> Self {
        TypedSlice::Null(&[])
    }
}

pub unsafe fn slice_as_bytes<T>(v: &[T]) -> &[u8] {
    unsafe {
        std::slice::from_raw_parts(
            v.as_ptr() as *const u8,
            std::mem::size_of_val(v),
        )
    }
}

unsafe fn drop_slice<T>(slice_start_ptr: *mut u8, len: usize) {
    unsafe {
        let droppable = std::slice::from_raw_parts_mut(
            slice_start_ptr as *mut ManuallyDrop<T>,
            len,
        );
        for e in droppable.iter_mut() {
            ManuallyDrop::drop(e);
        }
    }
}

impl<'a> TypedSlice<'a> {
    pub unsafe fn new<R: FieldDataRef<'a>>(
        fdr: R,
        fmt: FieldValueFormat,
        data_begin: usize,
        data_end: usize,
        field_count: usize,
    ) -> TypedSlice<'a> {
        unsafe {
            match fmt.repr {
                FieldValueRepr::Undefined => {
                    TypedSlice::Undefined(to_zst_slice(field_count))
                }
                FieldValueRepr::Null => {
                    TypedSlice::Null(to_zst_slice(field_count))
                }
                FieldValueRepr::BytesInline => TypedSlice::BytesInline(
                    to_slice(fdr, data_begin, data_end),
                ),
                FieldValueRepr::TextInline => {
                    TypedSlice::TextInline(std::str::from_utf8_unchecked(
                        to_slice(fdr, data_begin, data_end),
                    ))
                }
                FieldValueRepr::Int => {
                    TypedSlice::Int(to_slice(fdr, data_begin, data_end))
                }
                FieldValueRepr::BigInt => {
                    TypedSlice::BigInt(to_slice(fdr, data_begin, data_end))
                }
                FieldValueRepr::Float => {
                    TypedSlice::Float(to_slice(fdr, data_begin, data_end))
                }
                FieldValueRepr::Rational => {
                    TypedSlice::Rational(to_slice(fdr, data_begin, data_end))
                }
                FieldValueRepr::FieldReference => TypedSlice::FieldReference(
                    to_slice(fdr, data_begin, data_end),
                ),
                FieldValueRepr::SlicedFieldReference => {
                    TypedSlice::SlicedFieldReference(to_slice(
                        fdr, data_begin, data_end,
                    ))
                }
                FieldValueRepr::Error => {
                    TypedSlice::Error(to_slice(fdr, data_begin, data_end))
                }
                FieldValueRepr::Object => {
                    TypedSlice::Object(to_slice(fdr, data_begin, data_end))
                }
                FieldValueRepr::Array => {
                    TypedSlice::Array(to_slice(fdr, data_begin, data_end))
                }
                FieldValueRepr::Custom => {
                    TypedSlice::Custom(to_slice(fdr, data_begin, data_end))
                }
                FieldValueRepr::StreamValueId => TypedSlice::StreamValueId(
                    to_slice(fdr, data_begin, data_end),
                ),
                FieldValueRepr::BytesBuffer => TypedSlice::BytesBuffer(
                    to_slice(fdr, data_begin, data_end),
                ),
                FieldValueRepr::TextBuffer => {
                    TypedSlice::TextBuffer(to_slice(fdr, data_begin, data_end))
                }
                FieldValueRepr::BytesFile | FieldValueRepr::TextFile => {
                    todo!()
                }
            }
        }
    }
    pub unsafe fn as_bytes(&self) -> &[u8] {
        unsafe {
            match self {
                TypedSlice::Undefined(_) => &[],
                TypedSlice::Null(_) => &[],
                TypedSlice::Int(v) => slice_as_bytes(v),
                TypedSlice::BigInt(v) => slice_as_bytes(v),
                TypedSlice::Float(v) => slice_as_bytes(v),
                TypedSlice::Rational(v) => slice_as_bytes(v),
                TypedSlice::StreamValueId(v) => slice_as_bytes(v),
                TypedSlice::FieldReference(v) => slice_as_bytes(v),
                TypedSlice::SlicedFieldReference(v) => slice_as_bytes(v),
                TypedSlice::Error(v) => slice_as_bytes(v),
                TypedSlice::BytesInline(v) => v,
                TypedSlice::TextInline(v) => v.as_bytes(),
                TypedSlice::BytesBuffer(v) => slice_as_bytes(v),
                TypedSlice::TextBuffer(v) => slice_as_bytes(v),
                TypedSlice::Object(v) => slice_as_bytes(v),
                TypedSlice::Array(v) => slice_as_bytes(v),
                TypedSlice::Custom(v) => slice_as_bytes(v),
            }
        }
    }
    pub fn repr(&self) -> FieldValueRepr {
        match self {
            TypedSlice::Undefined(_) => FieldValueRepr::Undefined,
            TypedSlice::Null(_) => FieldValueRepr::Null,
            TypedSlice::Int(_) => FieldValueRepr::Int,
            TypedSlice::BigInt(_) => FieldValueRepr::BigInt,
            TypedSlice::Float(_) => FieldValueRepr::Float,
            TypedSlice::Rational(_) => FieldValueRepr::Rational,
            TypedSlice::StreamValueId(_) => FieldValueRepr::StreamValueId,
            TypedSlice::FieldReference(_) => FieldValueRepr::FieldReference,
            TypedSlice::SlicedFieldReference(_) => {
                FieldValueRepr::SlicedFieldReference
            }
            TypedSlice::Error(_) => FieldValueRepr::Error,
            TypedSlice::BytesInline(_) => FieldValueRepr::BytesInline,
            TypedSlice::TextInline(_) => FieldValueRepr::BytesInline,
            TypedSlice::BytesBuffer(_) => FieldValueRepr::BytesBuffer,
            TypedSlice::TextBuffer(_) => FieldValueRepr::TextBuffer,
            TypedSlice::Object(_) => FieldValueRepr::Object,
            TypedSlice::Array(_) => FieldValueRepr::Array,
            TypedSlice::Custom(_) => FieldValueRepr::Custom,
        }
    }
    pub fn len(&self) -> usize {
        match self {
            TypedSlice::Undefined(v) => v.len(),
            TypedSlice::Null(v) => v.len(),
            TypedSlice::Int(v) => v.len(),
            TypedSlice::BigInt(v) => v.len(),
            TypedSlice::Float(v) => v.len(),
            TypedSlice::Rational(v) => v.len(),
            TypedSlice::StreamValueId(v) => v.len(),
            TypedSlice::FieldReference(v) => v.len(),
            TypedSlice::SlicedFieldReference(v) => v.len(),
            TypedSlice::Error(v) => v.len(),
            TypedSlice::BytesInline(v) => v.len(),
            TypedSlice::TextInline(v) => v.len(),
            TypedSlice::TextBuffer(v) => v.len(),
            TypedSlice::BytesBuffer(v) => v.len(),
            TypedSlice::Object(v) => v.len(),
            TypedSlice::Array(v) => v.len(),
            TypedSlice::Custom(v) => v.len(),
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
        kind: FieldValueRepr,
    ) {
        unsafe {
            match kind {
                FieldValueRepr::Undefined => (),
                FieldValueRepr::Null => (),
                FieldValueRepr::Int => (),
                FieldValueRepr::BigInt => drop_slice::<BigInt>(ptr, len),
                FieldValueRepr::Float => (),
                FieldValueRepr::Rational => {
                    drop_slice::<BigRational>(ptr, len)
                }
                FieldValueRepr::TextInline => (),
                FieldValueRepr::TextBuffer => drop_slice::<String>(ptr, len),
                FieldValueRepr::TextFile => {
                    drop_slice::<TextBufferFile>(ptr, len)
                }
                FieldValueRepr::BytesInline => (),
                FieldValueRepr::BytesBuffer => drop_slice::<Vec<u8>>(ptr, len),
                FieldValueRepr::BytesFile => {
                    drop_slice::<BytesBufferFile>(ptr, len)
                }
                FieldValueRepr::Object => drop_slice::<Object>(ptr, len),
                FieldValueRepr::Array => drop_slice::<Array>(ptr, len),
                FieldValueRepr::Custom => {
                    drop_slice::<CustomDataBox>(ptr, len)
                }
                FieldValueRepr::Error => {
                    drop_slice::<OperatorApplicationError>(ptr, len)
                }
                FieldValueRepr::StreamValueId => (),
                FieldValueRepr::FieldReference => (),
                FieldValueRepr::SlicedFieldReference => (),
            }
        }
    }
}

#[derive(Default)]
pub struct TypedRange<'a> {
    pub headers: &'a [FieldValueHeader],
    pub data: TypedSlice<'a>,
    pub field_count: usize,
    pub first_header_run_length_oversize: RunLength,
    pub last_header_run_length_oversize: RunLength,
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
        let headers = &fdr.headers()[header_begin..header_end];
        let data = unsafe {
            TypedSlice::new(fdr, fmt, data_begin, data_end, field_count)
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

// SAFETY: the range contained in this header is non writable outside of this
// module. Therefore, nobody outside this module can (safely) construct a
// ValidTypedRange. We can therefore assume all instances to be valid (header
// matches data)
#[derive(Default)]
pub struct ValidTypedRange<'a>(pub(super) TypedRange<'a>);

impl<'a> Deref for ValidTypedRange<'a> {
    type Target = TypedRange<'a>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<'a> ValidTypedRange<'a> {
    pub unsafe fn new(range: TypedRange<'a>) -> Self {
        Self(range)
    }
}

unsafe fn to_zst_slice<T: Sized>(len: usize) -> &'static [T] {
    unsafe {
        std::slice::from_raw_parts(
            NonNull::dangling().as_ptr() as *const T,
            len,
        )
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
            fdr.data().as_ptr().add(data_begin) as *const T,
            (data_end - data_begin) / std::mem::size_of::<T>(),
        )
    }
}

unsafe fn to_ref<'a, T: Sized, R: FieldDataRef<'a>>(
    fdr: R,
    data_begin: usize,
) -> &'a T {
    unsafe { std::mem::transmute::<&'a u8, &'a T>(&fdr.data()[data_begin]) }
}
