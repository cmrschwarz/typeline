use std::ptr::NonNull;

use crate::{operations::errors::OperatorApplicationError, stream_value::StreamValueId};

use super::{
    field_value_flags, FieldData, FieldReference, FieldValueFormat, FieldValueHeader,
    FieldValueKind, Html, Object, RunLength,
};

pub enum TypedValue<'a> {
    Unset(()),
    Null(()),
    Integer(i64),
    StreamValueId(StreamValueId),
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
            FieldValueKind::Unset => TypedValue::Unset(()),
            FieldValueKind::Null => TypedValue::Null(()),
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
    pub fn as_slice(&'a self) -> TypedSlice<'a> {
        match self {
            TypedValue::Unset(v) => TypedSlice::Unset(std::slice::from_ref(v)),
            TypedValue::Null(v) => TypedSlice::Null(std::slice::from_ref(v)),
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
    Unset(&'a [()]),
    Null(&'a [()]),
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

impl<'a> TypedSlice<'a> {
    pub unsafe fn new(
        fd: &'a FieldData,
        fmt: FieldValueFormat,
        make_utf_bytes_text: bool,
        data_begin: usize,
        data_end: usize,
        field_count: usize,
    ) -> TypedSlice<'a> {
        match fmt.kind {
            FieldValueKind::Unset => TypedSlice::Unset(to_zst_slice(field_count)),
            FieldValueKind::Null => TypedSlice::Null(to_zst_slice(field_count)),
            FieldValueKind::BytesInline => {
                if make_utf_bytes_text && fmt.flags & field_value_flags::BYTES_ARE_UTF8 != 0 {
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
        make_utf_bytes_text: bool,
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
        let data = TypedSlice::new(
            fd,
            fmt,
            make_utf_bytes_text,
            data_begin,
            data_end,
            field_count,
        );
        TypedRange {
            headers,
            data,
            field_count,
            first_header_run_length_oversize,
            last_header_run_length_oversize,
        }
    }
}

unsafe fn to_zst_slice<T: Sized>(len: usize) -> &'static [T] {
    std::slice::from_raw_parts(NonNull::dangling().as_ptr() as *const T, len)
}

unsafe fn to_slice<T: Sized>(fd: &FieldData, data_begin: usize, data_end: usize) -> &[T] {
    std::slice::from_raw_parts(
        std::mem::transmute::<&u8, &T>(&fd.data[data_begin]) as *const T,
        (data_end - data_begin) / std::mem::size_of::<T>(),
    )
}

unsafe fn to_ref<T: Sized>(fd: &FieldData, data_begin: usize) -> &T {
    std::mem::transmute::<&u8, &T>(&fd.data[data_begin])
}
