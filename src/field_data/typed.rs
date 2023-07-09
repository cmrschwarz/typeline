use std::{any::TypeId, ptr::NonNull};

use super::{
    field_value_flags, FieldData, FieldReference, FieldValueFlags, FieldValueFormat,
    FieldValueHeader, FieldValueKind, Html, Null, Object, RunLength, Success, Unset,
};
use crate::{operations::errors::OperatorApplicationError, stream_value::StreamValueId};
use std::ops::Deref;

pub enum TypedValue<'a> {
    Unset(Unset),
    Null(Null),
    Success(Success),
    Integer(&'a i64),
    StreamValueId(&'a StreamValueId),
    Reference(&'a FieldReference),
    Error(&'a OperatorApplicationError),
    Html(&'a Html),
    BytesInline(&'a [u8]),
    TextInline(&'a str),
    BytesBuffer(&'a Vec<u8>),
    Object(&'a Object),
}

impl<'a> TypedValue<'a> {
    pub unsafe fn new(fd: &'a FieldData, fmt: FieldValueFormat, data_begin: usize) -> Self {
        match fmt.kind {
            FieldValueKind::Unset => TypedValue::Unset(Unset),
            FieldValueKind::Null => TypedValue::Null(Null),
            FieldValueKind::Success => TypedValue::Success(Success),
            FieldValueKind::BytesInline => {
                if fmt.flags & field_value_flags::BYTES_ARE_UTF8 != 0 {
                    TypedValue::TextInline(std::str::from_utf8_unchecked(to_slice(
                        fd,
                        data_begin,
                        fmt.size as usize,
                    )))
                } else {
                    TypedValue::BytesInline(to_slice(fd, data_begin, fmt.size as usize))
                }
            }
            FieldValueKind::EntryId => todo!(),
            FieldValueKind::Integer => TypedValue::Integer(*to_ref(fd, data_begin)),
            FieldValueKind::StreamValueId => TypedValue::StreamValueId(*to_ref(fd, data_begin)),
            FieldValueKind::Reference => TypedValue::Reference(to_ref(fd, data_begin)),
            FieldValueKind::Error => TypedValue::Error(to_ref(fd, data_begin)),
            FieldValueKind::Html => TypedValue::Html(to_ref(fd, data_begin)),
            FieldValueKind::Object => TypedValue::Object(to_ref(fd, data_begin)),
            FieldValueKind::BytesBuffer => TypedValue::BytesBuffer(to_ref(fd, data_begin)),
            FieldValueKind::BytesFile => todo!(),
        }
    }
    pub fn as_slice(&self) -> TypedSlice<'a> {
        match self {
            TypedValue::Success(_) => TypedSlice::Success(&[Success]),
            TypedValue::Unset(_) => TypedSlice::Unset(&[Unset]),
            TypedValue::Null(_) => TypedSlice::Null(&[Null]),
            TypedValue::Integer(v) => TypedSlice::Integer(std::slice::from_ref(v)),
            TypedValue::StreamValueId(v) => TypedSlice::StreamValueId(std::slice::from_ref(v)),
            TypedValue::Reference(v) => TypedSlice::Reference(std::slice::from_ref(v)),
            TypedValue::Error(v) => TypedSlice::Error(std::slice::from_ref(v)),
            TypedValue::Html(v) => TypedSlice::Html(std::slice::from_ref(v)),
            TypedValue::BytesInline(v) => TypedSlice::BytesInline(v),
            TypedValue::TextInline(v) => TypedSlice::TextInline(v),
            TypedValue::BytesBuffer(v) => TypedSlice::BytesBuffer(std::slice::from_ref(v)),
            TypedValue::Object(v) => TypedSlice::Object(std::slice::from_ref(v)),
        }
    }
}

pub struct TypedField<'a> {
    pub header: FieldValueHeader,
    pub value: TypedValue<'a>,
}

impl<'a> TypedField<'a> {
    pub unsafe fn new(
        fd: &'a FieldData,
        fmt: FieldValueFormat,
        data_begin: usize,
        run_len: RunLength,
    ) -> TypedField<'a> {
        let value = TypedValue::new(fd, fmt, data_begin);
        TypedField {
            header: FieldValueHeader {
                fmt: fmt,
                run_length: run_len,
            },
            value,
        }
    }
}

#[derive(Clone, Copy)]
pub enum TypedSlice<'a> {
    Success(&'a [Success]),
    Unset(&'a [Unset]),
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
}

pub fn slice_as_bytes<T>(v: &[T]) -> &[u8] {
    unsafe {
        std::slice::from_raw_parts(v.as_ptr() as *const u8, v.len() * std::mem::size_of::<T>())
    }
}

impl<'a> TypedSlice<'a> {
    pub unsafe fn new(
        fd: &'a FieldData,
        fmt: FieldValueFormat,
        flag_mask: FieldValueFlags,
        data_begin: usize,
        data_end: usize,
        field_count: usize,
    ) -> TypedSlice<'a> {
        match fmt.kind {
            FieldValueKind::Success => TypedSlice::Success(to_zst_slice(field_count)),
            FieldValueKind::Unset => TypedSlice::Unset(to_zst_slice(field_count)),
            FieldValueKind::Null => TypedSlice::Null(to_zst_slice(field_count)),
            FieldValueKind::BytesInline => {
                if fmt.flags & flag_mask & field_value_flags::BYTES_ARE_UTF8 != 0 {
                    TypedSlice::TextInline(std::str::from_utf8_unchecked(to_slice(
                        fd, data_begin, data_end,
                    )))
                } else {
                    TypedSlice::BytesInline(to_slice(fd, data_begin, data_end))
                }
            }
            FieldValueKind::EntryId => todo!(),
            FieldValueKind::Integer => TypedSlice::Integer(to_slice(fd, data_begin, data_end)),
            FieldValueKind::Reference => TypedSlice::Reference(to_slice(fd, data_begin, data_end)),
            FieldValueKind::Error => TypedSlice::Error(to_slice(fd, data_begin, data_end)),
            FieldValueKind::Html => TypedSlice::Html(to_slice(fd, data_begin, data_end)),
            FieldValueKind::Object => TypedSlice::Object(to_slice(fd, data_begin, data_end)),
            FieldValueKind::StreamValueId => {
                TypedSlice::StreamValueId(to_slice(fd, data_begin, data_end))
            }
            FieldValueKind::BytesBuffer => {
                TypedSlice::BytesBuffer(to_slice(fd, data_begin, data_end))
            }
            FieldValueKind::BytesFile => todo!(),
        }
    }
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            TypedSlice::Unset(_) => &[],
            TypedSlice::Success(_) => &[],
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
        }
    }
    pub fn type_id(&self) -> TypeId {
        match self {
            TypedSlice::Unset(_) => TypeId::of::<Unset>(),
            TypedSlice::Success(_) => TypeId::of::<Success>(),
            TypedSlice::Null(_) => TypeId::of::<Null>(),
            TypedSlice::Integer(_) => TypeId::of::<i64>(),
            TypedSlice::StreamValueId(_) => TypeId::of::<StreamValueId>(),
            TypedSlice::Reference(_) => TypeId::of::<FieldReference>(),
            TypedSlice::Error(_) => TypeId::of::<OperatorApplicationError>(),
            TypedSlice::Html(_) => TypeId::of::<Html>(),
            TypedSlice::BytesInline(_) => TypeId::of::<u8>(),
            TypedSlice::TextInline(_) => TypeId::of::<u8>(),
            TypedSlice::BytesBuffer(_) => TypeId::of::<Vec<u8>>(),
            TypedSlice::Object(_) => TypeId::of::<Object>(),
        }
    }
    pub fn matches_values<T: 'static>(&self, values: &[T]) -> bool {
        TypeId::of::<T>() == self.type_id()
            && self.as_bytes().as_ptr_range() == slice_as_bytes(values).as_ptr_range()
    }
}

pub struct TypedRange<'a> {
    pub headers: &'a [FieldValueHeader],
    pub data: TypedSlice<'a>,
    pub field_count: usize,
    pub first_header_run_length_oversize: RunLength,
    pub last_header_run_length_oversize: RunLength,
}

impl<'a> TypedRange<'a> {
    pub unsafe fn new(
        fd: &'a FieldData,
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
        let headers = &fd.header[header_begin..header_end];
        let data = TypedSlice::new(fd, fmt, flag_mask, data_begin, data_end, field_count);
        TypedRange {
            headers,
            data,
            field_count,
            first_header_run_length_oversize,
            last_header_run_length_oversize,
        }
    }
}

// SAFETY: the range contained in this header is non writable outside of this module.
// Therefore, nobody outside this module can (safely) construct a ValidTypedRange.
// We can therefore assume all instances to be valid (header matches data)
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
    std::slice::from_raw_parts(NonNull::dangling().as_ptr() as *const T, len)
}

unsafe fn to_slice<T: Sized>(fd: &FieldData, data_begin: usize, data_end: usize) -> &[T] {
    std::slice::from_raw_parts::<T>(
        std::mem::transmute::<&u8, &T>(&fd.data[data_begin]) as *const T,
        (data_end - data_begin) / std::mem::size_of::<T>(),
    )
}

unsafe fn to_ref<T: Sized>(fd: &FieldData, data_begin: usize) -> &T {
    std::mem::transmute::<&u8, &T>(&fd.data[data_begin])
}
