use std::{mem::ManuallyDrop, ptr::NonNull};

use super::{
    custom_data::CustomDataBox,
    field_data::{
        field_value_flags, FieldReference, FieldValueFlags, FieldValueFormat,
        FieldValueHeader, FieldValueKind, FieldValueType, Html, Null, Object,
        RunLength, Undefined,
    },
    iters::FieldDataRef,
    stream_value::StreamValueId,
};
use crate::operators::errors::OperatorApplicationError;
use std::ops::Deref;

pub enum TypedValue<'a> {
    Null(Null),
    Undefined(Undefined),
    Integer(&'a i64),
    StreamValueId(&'a StreamValueId),
    Reference(&'a FieldReference),
    Error(&'a OperatorApplicationError),
    Html(&'a Html),
    BytesInline(&'a [u8]),
    TextInline(&'a str),
    BytesBuffer(&'a Vec<u8>),
    Object(&'a Object),
    Custom(&'a CustomDataBox),
}

impl<'a> TypedValue<'a> {
    pub unsafe fn new<R: FieldDataRef<'a>>(
        fdr: R,
        fmt: FieldValueFormat,
        data_begin: usize,
    ) -> Self {
        unsafe {
            match fmt.kind {
                FieldValueKind::Null => TypedValue::Null(Null),
                FieldValueKind::Undefined => TypedValue::Undefined(Undefined),
                FieldValueKind::BytesInline => {
                    if fmt.flags & field_value_flags::BYTES_ARE_UTF8 != 0 {
                        TypedValue::TextInline(std::str::from_utf8_unchecked(
                            to_slice(fdr, data_begin, fmt.size as usize),
                        ))
                    } else {
                        TypedValue::BytesInline(to_slice(
                            fdr,
                            data_begin,
                            fmt.size as usize,
                        ))
                    }
                }
                FieldValueKind::Integer => {
                    TypedValue::Integer(to_ref(fdr, data_begin))
                }
                FieldValueKind::StreamValueId => {
                    TypedValue::StreamValueId(to_ref(fdr, data_begin))
                }
                FieldValueKind::Reference => {
                    TypedValue::Reference(to_ref(fdr, data_begin))
                }
                FieldValueKind::Error => {
                    TypedValue::Error(to_ref(fdr, data_begin))
                }
                FieldValueKind::Html => {
                    TypedValue::Html(to_ref(fdr, data_begin))
                }
                FieldValueKind::Object => {
                    TypedValue::Object(to_ref(fdr, data_begin))
                }
                FieldValueKind::Custom => {
                    TypedValue::Custom(to_ref(fdr, data_begin))
                }
                FieldValueKind::BytesBuffer => {
                    TypedValue::BytesBuffer(to_ref(fdr, data_begin))
                }
                FieldValueKind::BytesFile => todo!(),
            }
        }
    }
    pub fn as_slice(&self) -> TypedSlice<'a> {
        match self {
            TypedValue::Undefined(_) => TypedSlice::Undefined(&[Undefined]),
            TypedValue::Null(_) => TypedSlice::Null(&[Null]),
            TypedValue::Integer(v) => {
                TypedSlice::Integer(std::slice::from_ref(v))
            }
            TypedValue::StreamValueId(v) => {
                TypedSlice::StreamValueId(std::slice::from_ref(v))
            }
            TypedValue::Reference(v) => {
                TypedSlice::Reference(std::slice::from_ref(v))
            }
            TypedValue::Error(v) => TypedSlice::Error(std::slice::from_ref(v)),
            TypedValue::Html(v) => TypedSlice::Html(std::slice::from_ref(v)),
            TypedValue::BytesInline(v) => TypedSlice::BytesInline(v),
            TypedValue::TextInline(v) => TypedSlice::TextInline(v),
            TypedValue::BytesBuffer(v) => {
                TypedSlice::BytesBuffer(std::slice::from_ref(v))
            }
            TypedValue::Object(v) => {
                TypedSlice::Object(std::slice::from_ref(v))
            }
            TypedValue::Custom(v) => {
                TypedSlice::Custom(std::slice::from_ref(v))
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
    Undefined(&'a [Undefined]),
    Null(&'a [Null]),
    Integer(&'a [i64]),
    StreamValueId(&'a [StreamValueId]),
    Reference(&'a [FieldReference]),
    Error(&'a [OperatorApplicationError]),
    Html(&'a [Html]),
    BytesInline(&'a [u8]),
    TextInline(&'a str),
    BytesBuffer(&'a [Vec<u8>]),
    Object(&'a [Object]),
    Custom(&'a [CustomDataBox]),
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
        flag_mask: FieldValueFlags,
        data_begin: usize,
        data_end: usize,
        field_count: usize,
    ) -> TypedSlice<'a> {
        unsafe {
            match fmt.kind {
                FieldValueKind::Undefined => {
                    TypedSlice::Undefined(to_zst_slice(field_count))
                }
                FieldValueKind::Null => {
                    TypedSlice::Null(to_zst_slice(field_count))
                }
                FieldValueKind::BytesInline => {
                    if fmt.flags
                        & flag_mask
                        & field_value_flags::BYTES_ARE_UTF8
                        != 0
                    {
                        TypedSlice::TextInline(std::str::from_utf8_unchecked(
                            to_slice(fdr, data_begin, data_end),
                        ))
                    } else {
                        TypedSlice::BytesInline(to_slice(
                            fdr, data_begin, data_end,
                        ))
                    }
                }
                FieldValueKind::Integer => {
                    TypedSlice::Integer(to_slice(fdr, data_begin, data_end))
                }
                FieldValueKind::Reference => {
                    TypedSlice::Reference(to_slice(fdr, data_begin, data_end))
                }
                FieldValueKind::Error => {
                    TypedSlice::Error(to_slice(fdr, data_begin, data_end))
                }
                FieldValueKind::Html => {
                    TypedSlice::Html(to_slice(fdr, data_begin, data_end))
                }
                FieldValueKind::Object => {
                    TypedSlice::Object(to_slice(fdr, data_begin, data_end))
                }
                FieldValueKind::Custom => {
                    TypedSlice::Custom(to_slice(fdr, data_begin, data_end))
                }
                FieldValueKind::StreamValueId => TypedSlice::StreamValueId(
                    to_slice(fdr, data_begin, data_end),
                ),
                FieldValueKind::BytesBuffer => TypedSlice::BytesBuffer(
                    to_slice(fdr, data_begin, data_end),
                ),
                FieldValueKind::BytesFile => todo!(),
            }
        }
    }
    pub unsafe fn as_bytes(&self) -> &[u8] {
        unsafe {
            match self {
                TypedSlice::Undefined(_) => &[],
                TypedSlice::Null(_) => &[],
                TypedSlice::Integer(v) => slice_as_bytes(v),
                TypedSlice::StreamValueId(v) => slice_as_bytes(v),
                TypedSlice::Reference(v) => slice_as_bytes(v),
                TypedSlice::Error(v) => slice_as_bytes(v),
                TypedSlice::Html(v) => slice_as_bytes(v),
                TypedSlice::BytesInline(v) => v,
                TypedSlice::TextInline(v) => v.as_bytes(),
                TypedSlice::BytesBuffer(v) => slice_as_bytes(v),
                TypedSlice::Object(v) => slice_as_bytes(v),
                TypedSlice::Custom(v) => slice_as_bytes(v),
            }
        }
    }
    pub fn kind(&self) -> FieldValueKind {
        match self {
            TypedSlice::Undefined(_) => FieldValueKind::Undefined,
            TypedSlice::Null(_) => FieldValueKind::Null,
            TypedSlice::Integer(_) => FieldValueKind::Integer,
            TypedSlice::StreamValueId(_) => FieldValueKind::StreamValueId,
            TypedSlice::Reference(_) => FieldValueKind::Reference,
            TypedSlice::Error(_) => FieldValueKind::Error,
            TypedSlice::Html(_) => FieldValueKind::Html,
            TypedSlice::BytesInline(_) => FieldValueKind::BytesInline,
            TypedSlice::TextInline(_) => FieldValueKind::BytesInline,
            TypedSlice::BytesBuffer(_) => FieldValueKind::BytesBuffer,
            TypedSlice::Object(_) => FieldValueKind::Object,
            TypedSlice::Custom(_) => FieldValueKind::Custom,
        }
    }
    pub fn len(&self) -> usize {
        match self {
            TypedSlice::Undefined(v) => v.len(),
            TypedSlice::Null(v) => v.len(),
            TypedSlice::Integer(v) => v.len(),
            TypedSlice::StreamValueId(v) => v.len(),
            TypedSlice::Reference(v) => v.len(),
            TypedSlice::Error(v) => v.len(),
            TypedSlice::Html(v) => v.len(),
            TypedSlice::BytesInline(v) => v.len(),
            TypedSlice::TextInline(v) => v.len(),
            TypedSlice::BytesBuffer(v) => v.len(),
            TypedSlice::Object(v) => v.len(),
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
        if T::KIND != self.kind() {
            return false;
        }
        values.len() == self.len()
    }
    pub unsafe fn drop_from_kind(
        start_ptr: *mut u8,
        len: usize,
        kind: FieldValueKind,
    ) {
        type DropFn = unsafe fn(*mut u8, usize);
        #[inline(always)]
        fn drop_fn_case<T: FieldValueType>() -> (FieldValueKind, DropFn) {
            (T::KIND, drop_slice::<T>)
        }
        let drop_fns = [
            drop_fn_case::<Undefined>(),
            drop_fn_case::<Null>(),
            drop_fn_case::<i64>(),
            drop_fn_case::<StreamValueId>(),
            drop_fn_case::<FieldReference>(),
            drop_fn_case::<OperatorApplicationError>(),
            drop_fn_case::<Html>(),
            (FieldValueKind::BytesInline, drop_slice::<u8>),
            drop_fn_case::<Vec<u8>>(),
            drop_fn_case::<Object>(),
            drop_fn_case::<CustomDataBox>(),
        ];

        for (tid, drop_fn) in drop_fns {
            if tid == kind {
                unsafe {
                    drop_fn(start_ptr, len);
                }
                return;
            }
        }
        panic!("missing drop implementation in TypeSlice!")
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
        flag_mask: FieldValueFlags,
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
            TypedSlice::new(
                fdr,
                fmt,
                flag_mask,
                data_begin,
                data_end,
                field_count,
            )
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
