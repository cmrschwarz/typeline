use std::{mem::ManuallyDrop, ops::Range};

use num::{BigInt, BigRational};

use super::{
    array::Array,
    custom_data::CustomDataBox,
    dyn_ref_iter::FieldValueSliceIter,
    field_data::{
        FieldValueFormat, FieldValueHeader, FieldValueRepr, FieldValueType,
        RunLength, TextBufferFile,
    },
    field_value::{FieldReference, FieldValue, Object, SlicedFieldReference},
    iters::FieldDataRef,
    stream_value::StreamValueId,
};
use crate::{
    operators::errors::OperatorApplicationError,
    record_data::field_data::BytesBufferFile,
};

#[derive(Clone, Copy)]
pub enum FieldValueRef<'a> {
    Null,
    Undefined,
    Int(&'a i64),
    BigInt(&'a BigInt),
    Float(&'a f64),
    Rational(&'a BigRational),
    Text(&'a str),
    Bytes(&'a [u8]),
    Array(&'a Array),
    Object(&'a Object),
    Custom(&'a CustomDataBox),
    StreamValueId(&'a StreamValueId),
    Error(&'a OperatorApplicationError),
    FieldReference(&'a FieldReference),
    SlicedFieldReference(&'a SlicedFieldReference),
}

#[derive(Clone, Copy)]
pub enum FieldValueSlice<'a> {
    Null(usize),
    Undefined(usize),
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
            match fmt.repr {
                FieldValueRepr::Null => FieldValueRef::Null,
                FieldValueRepr::Undefined => FieldValueRef::Undefined,
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
                FieldValueRepr::Int => {
                    FieldValueRef::Int(to_ref(fdr, data_begin))
                }
                FieldValueRepr::BigInt => {
                    FieldValueRef::BigInt(to_ref(fdr, data_begin))
                }
                FieldValueRepr::Float => {
                    FieldValueRef::Float(to_ref(fdr, data_begin))
                }
                FieldValueRepr::Rational => {
                    FieldValueRef::Rational(to_ref(fdr, data_begin))
                }
                FieldValueRepr::StreamValueId => {
                    FieldValueRef::StreamValueId(to_ref(fdr, data_begin))
                }
                FieldValueRepr::FieldReference => {
                    FieldValueRef::FieldReference(to_ref(fdr, data_begin))
                }
                FieldValueRepr::SlicedFieldReference => {
                    FieldValueRef::SlicedFieldReference(to_ref(
                        fdr, data_begin,
                    ))
                }
                FieldValueRepr::Error => {
                    FieldValueRef::Error(to_ref(fdr, data_begin))
                }
                FieldValueRepr::Object => {
                    FieldValueRef::Object(to_ref(fdr, data_begin))
                }
                FieldValueRepr::Array => {
                    FieldValueRef::Array(to_ref(fdr, data_begin))
                }
                FieldValueRepr::Custom => {
                    FieldValueRef::Custom(to_ref(fdr, data_begin))
                }
                FieldValueRepr::BytesBuffer => {
                    FieldValueRef::Bytes(to_ref::<Vec<u8>, R>(fdr, data_begin))
                }
                FieldValueRepr::TextBuffer => {
                    FieldValueRef::Text(to_ref::<String, R>(fdr, data_begin))
                }
                FieldValueRepr::BytesFile => todo!(),
                FieldValueRepr::TextFile => todo!(),
            }
        }
    }
    pub fn as_slice(&self) -> FieldValueSlice<'a> {
        use std::slice::from_ref;
        match self {
            FieldValueRef::Undefined => FieldValueSlice::Undefined(1),
            FieldValueRef::Null => FieldValueSlice::Null(1),
            FieldValueRef::Int(v) => FieldValueSlice::Int(from_ref(v)),
            FieldValueRef::BigInt(v) => FieldValueSlice::BigInt(from_ref(v)),
            FieldValueRef::Float(v) => FieldValueSlice::Float(from_ref(v)),
            FieldValueRef::Rational(v) => {
                FieldValueSlice::Rational(from_ref(v))
            }
            FieldValueRef::StreamValueId(v) => {
                FieldValueSlice::StreamValueId(from_ref(v))
            }
            FieldValueRef::FieldReference(v) => {
                FieldValueSlice::FieldReference(from_ref(v))
            }
            FieldValueRef::SlicedFieldReference(v) => {
                FieldValueSlice::SlicedFieldReference(from_ref(v))
            }
            FieldValueRef::Error(v) => FieldValueSlice::Error(from_ref(v)),
            FieldValueRef::Text(v) => FieldValueSlice::TextInline(v),
            FieldValueRef::Bytes(v) => FieldValueSlice::BytesInline(v),
            FieldValueRef::Object(v) => FieldValueSlice::Object(from_ref(v)),
            FieldValueRef::Array(v) => FieldValueSlice::Array(from_ref(v)),
            FieldValueRef::Custom(v) => FieldValueSlice::Custom(from_ref(v)),
        }
    }
    pub fn to_field_value(&self) -> FieldValue {
        match *self {
            FieldValueRef::Undefined => FieldValue::Undefined,
            FieldValueRef::Null => FieldValue::Null,
            FieldValueRef::Int(v) => FieldValue::Int(*v),
            FieldValueRef::BigInt(v) => {
                FieldValue::BigInt(Box::new(v.clone()))
            }
            FieldValueRef::Float(v) => FieldValue::Float(*v),
            FieldValueRef::Rational(v) => {
                FieldValue::Rational(Box::new(v.clone()))
            }
            FieldValueRef::StreamValueId(v) => FieldValue::StreamValueId(*v),
            FieldValueRef::FieldReference(v) => FieldValue::FieldReference(*v),
            FieldValueRef::SlicedFieldReference(v) => {
                FieldValue::SlicedFieldReference(*v)
            }
            FieldValueRef::Error(v) => FieldValue::Error(v.clone()),
            FieldValueRef::Text(v) => FieldValue::Text(v.to_string()),
            FieldValueRef::Bytes(v) => FieldValue::Bytes(v.to_vec()),
            FieldValueRef::Object(v) => FieldValue::Object(v.clone()),
            FieldValueRef::Array(v) => FieldValue::Array(v.clone()),
            FieldValueRef::Custom(v) => FieldValue::Custom(v.clone_dyn()),
        }
    }
    pub fn repr(&self) -> FieldValueRepr {
        self.as_slice().repr()
    }
    pub fn subslice(&self, range: Range<usize>) -> Self {
        match self {
            FieldValueRef::Bytes(v) => FieldValueRef::Bytes(&v[range]),
            FieldValueRef::Text(v) => FieldValueRef::Text(&v[range]),
            FieldValueRef::Array(_) => todo!(),
            FieldValueRef::Null
            | FieldValueRef::Undefined
            | FieldValueRef::Int(_)
            | FieldValueRef::BigInt(_)
            | FieldValueRef::Float(_)
            | FieldValueRef::Rational(_)
            | FieldValueRef::Object(_)
            | FieldValueRef::Custom(_)
            | FieldValueRef::StreamValueId(_)
            | FieldValueRef::Error(_)
            | FieldValueRef::FieldReference(_)
            | FieldValueRef::SlicedFieldReference(_) => {
                panic!("typed value kind {:?} is not slicable", self.repr(),)
            }
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
            match fmt.repr {
                FieldValueRepr::Undefined => {
                    FieldValueSlice::Undefined(field_count)
                }
                FieldValueRepr::Null => FieldValueSlice::Null(field_count),
                FieldValueRepr::BytesInline => FieldValueSlice::BytesInline(
                    to_slice(fdr, data_begin, data_end),
                ),
                FieldValueRepr::TextInline => {
                    FieldValueSlice::TextInline(std::str::from_utf8_unchecked(
                        to_slice(fdr, data_begin, data_end),
                    ))
                }
                FieldValueRepr::Int => {
                    FieldValueSlice::Int(to_slice(fdr, data_begin, data_end))
                }
                FieldValueRepr::BigInt => FieldValueSlice::BigInt(to_slice(
                    fdr, data_begin, data_end,
                )),
                FieldValueRepr::Float => {
                    FieldValueSlice::Float(to_slice(fdr, data_begin, data_end))
                }
                FieldValueRepr::Rational => FieldValueSlice::Rational(
                    to_slice(fdr, data_begin, data_end),
                ),
                FieldValueRepr::FieldReference => {
                    FieldValueSlice::FieldReference(to_slice(
                        fdr, data_begin, data_end,
                    ))
                }
                FieldValueRepr::SlicedFieldReference => {
                    FieldValueSlice::SlicedFieldReference(to_slice(
                        fdr, data_begin, data_end,
                    ))
                }
                FieldValueRepr::Error => {
                    FieldValueSlice::Error(to_slice(fdr, data_begin, data_end))
                }
                FieldValueRepr::Object => FieldValueSlice::Object(to_slice(
                    fdr, data_begin, data_end,
                )),
                FieldValueRepr::Array => {
                    FieldValueSlice::Array(to_slice(fdr, data_begin, data_end))
                }
                FieldValueRepr::Custom => FieldValueSlice::Custom(to_slice(
                    fdr, data_begin, data_end,
                )),
                FieldValueRepr::StreamValueId => {
                    FieldValueSlice::StreamValueId(to_slice(
                        fdr, data_begin, data_end,
                    ))
                }
                FieldValueRepr::BytesBuffer => FieldValueSlice::BytesBuffer(
                    to_slice(fdr, data_begin, data_end),
                ),
                FieldValueRepr::TextBuffer => FieldValueSlice::TextBuffer(
                    to_slice(fdr, data_begin, data_end),
                ),
                FieldValueRepr::BytesFile | FieldValueRepr::TextFile => {
                    todo!()
                }
            }
        }
    }
    pub fn as_bytes(&self) -> &'a [u8] {
        unsafe {
            match *self {
                FieldValueSlice::Undefined(_) | FieldValueSlice::Null(_) => {
                    &[]
                }
                FieldValueSlice::Int(v) => slice_as_bytes(v),
                FieldValueSlice::BigInt(v) => slice_as_bytes(v),
                FieldValueSlice::Float(v) => slice_as_bytes(v),
                FieldValueSlice::Rational(v) => slice_as_bytes(v),
                FieldValueSlice::StreamValueId(v) => slice_as_bytes(v),
                FieldValueSlice::FieldReference(v) => slice_as_bytes(v),
                FieldValueSlice::SlicedFieldReference(v) => slice_as_bytes(v),
                FieldValueSlice::Error(v) => slice_as_bytes(v),
                FieldValueSlice::BytesInline(v) => v,
                FieldValueSlice::TextInline(v) => v.as_bytes(),
                FieldValueSlice::BytesBuffer(v) => slice_as_bytes(v),
                FieldValueSlice::TextBuffer(v) => slice_as_bytes(v),
                FieldValueSlice::Object(v) => slice_as_bytes(v),
                FieldValueSlice::Array(v) => slice_as_bytes(v),
                FieldValueSlice::Custom(v) => slice_as_bytes(v),
            }
        }
    }
    pub fn repr(&self) -> FieldValueRepr {
        match self {
            FieldValueSlice::Undefined(_) => FieldValueRepr::Undefined,
            FieldValueSlice::Null(_) => FieldValueRepr::Null,
            FieldValueSlice::Int(_) => FieldValueRepr::Int,
            FieldValueSlice::BigInt(_) => FieldValueRepr::BigInt,
            FieldValueSlice::Float(_) => FieldValueRepr::Float,
            FieldValueSlice::Rational(_) => FieldValueRepr::Rational,
            FieldValueSlice::StreamValueId(_) => FieldValueRepr::StreamValueId,
            FieldValueSlice::FieldReference(_) => {
                FieldValueRepr::FieldReference
            }
            FieldValueSlice::SlicedFieldReference(_) => {
                FieldValueRepr::SlicedFieldReference
            }
            FieldValueSlice::Error(_) => FieldValueRepr::Error,
            FieldValueSlice::TextInline(_) => FieldValueRepr::TextInline,
            FieldValueSlice::BytesInline(_) => FieldValueRepr::BytesInline,
            FieldValueSlice::BytesBuffer(_) => FieldValueRepr::BytesBuffer,
            FieldValueSlice::TextBuffer(_) => FieldValueRepr::TextBuffer,
            FieldValueSlice::Object(_) => FieldValueRepr::Object,
            FieldValueSlice::Array(_) => FieldValueRepr::Array,
            FieldValueSlice::Custom(_) => FieldValueRepr::Custom,
        }
    }
    pub fn len(&self) -> usize {
        match self {
            FieldValueSlice::Undefined(v) | FieldValueSlice::Null(v) => *v,
            FieldValueSlice::Int(v) => v.len(),
            FieldValueSlice::BigInt(v) => v.len(),
            FieldValueSlice::Float(v) => v.len(),
            FieldValueSlice::Rational(v) => v.len(),
            FieldValueSlice::StreamValueId(v) => v.len(),
            FieldValueSlice::FieldReference(v) => v.len(),
            FieldValueSlice::SlicedFieldReference(v) => v.len(),
            FieldValueSlice::Error(v) => v.len(),
            FieldValueSlice::BytesInline(v) => v.len(),
            FieldValueSlice::TextInline(v) => v.len(),
            FieldValueSlice::TextBuffer(v) => v.len(),
            FieldValueSlice::BytesBuffer(v) => v.len(),
            FieldValueSlice::Object(v) => v.len(),
            FieldValueSlice::Array(v) => v.len(),
            FieldValueSlice::Custom(v) => v.len(),
        }
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
        kind: FieldValueRepr,
    ) {
        unsafe {
            match kind {
                FieldValueRepr::BigInt => drop_slice::<BigInt>(ptr, len),
                FieldValueRepr::Rational => {
                    drop_slice::<BigRational>(ptr, len)
                }
                FieldValueRepr::TextBuffer => drop_slice::<String>(ptr, len),
                FieldValueRepr::TextFile => {
                    drop_slice::<TextBufferFile>(ptr, len)
                }
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
                FieldValueRepr::Undefined
                | FieldValueRepr::Null
                | FieldValueRepr::Int
                | FieldValueRepr::Float
                | FieldValueRepr::TextInline
                | FieldValueRepr::BytesInline
                | FieldValueRepr::StreamValueId
                | FieldValueRepr::FieldReference
                | FieldValueRepr::SlicedFieldReference => (),
            }
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
