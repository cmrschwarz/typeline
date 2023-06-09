use std::{collections::HashMap, mem::size_of, ptr::NonNull, slice};

use indexmap::IndexMap;

use crate::{
    match_value::MatchValueKind, operations::operator_base::OperatorApplicationError,
    string_store::StringStoreEntry, sync_variant::SyncVariantImpl,
};

//if the u32 overflows we just split into two values
pub type RunLength = u32;

#[derive(Clone, Copy)]
pub enum StreamKind {
    Plain,
    StreamChunk(bool),
    BufferMem(bool),
    BufferFile(bool),
}

#[derive(Clone, Copy)]
pub enum FieldValueKind {
    Unset,
    EntryId,
    Text(StreamKind),
    Bytes(StreamKind),
    Array(StreamKind), //TODO
    Error,
    Html,
    Integer, //TODO: bigint, float, decimal, ...
    Null,
    Object,
}

#[derive(Clone, Copy)]
pub enum FieldValueBytesRef<'a> {
    Plain(&'a [u8]),
    StreamChunk(&'a [u8], bool),
    BufferMem(&'a Vec<u8>, bool),
    BufferFile(u8, bool), //TODO
}

#[derive(Clone, Copy)]
pub enum FieldValueTextRef<'a> {
    Plain(&'a str),
    StreamChunk(&'a str, bool),
    BufferMem(&'a String, bool),
    BufferFile(u8, bool), //TODO
}

struct ObjectEntry {
    kind: FieldValueKind,
    offset: usize,
}

pub struct Object {
    //TODO mesasure
    keys: IndexMap<String, ObjectEntry>,
    data: Vec<u8>,
}

#[derive(Clone, Copy)]
pub enum FieldValueRef<'a> {
    Unset,
    EntryId(EntryId),
    Bytes(FieldValueBytesRef<'a>),
    Text(FieldValueTextRef<'a>),
    Error(&'a OperatorApplicationError),
    Html(u8), //TODO
    Integer(i64),
    Null,
    Object(&'a Object),
}

#[allow(dead_code)] //TODO
struct FieldValueHeader {
    kind: FieldValueKind,
    run_length: RunLength,
}

#[derive(Default)]
pub struct FieldData {
    header: Vec<FieldValueHeader>,
    data: Vec<u8>,
}

pub struct FieldDataIterator<'a> {
    header: slice::Iter<'a, FieldValueHeader>,
    data: NonNull<u8>,
}

impl<'a> Iterator for FieldDataIterator<'a> {
    type Item = (RunLength, FieldValueRef<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        self.header.next().and_then(|h| {
            Some((h.run_length, unsafe {
                let (val, size) = get_field_value_ref(h.kind, self.data.as_ptr());
                self.data =
                    NonNull::new_unchecked(self.data.as_ptr().add(h.run_length as usize * size));
                val
            }))
        })
    }
}

impl FieldData {
    pub fn clear(&mut self) {
        self.header.clear();
        self.data.clear();
    }
}

pub type EntryId = usize;

const LEN_SIZE: usize = size_of::<usize>();
const ALIGN: usize = size_of::<usize>();

fn ceil_to_align(size: usize) -> usize {
    (size + ALIGN - 1) / ALIGN * ALIGN
}

unsafe fn load_slice_len(ptr: *const u8) -> usize {
    *(ptr as *const usize)
}

unsafe fn load_slice<'a>(ptr: *const u8) -> &'a [u8] {
    let len = load_slice_len(ptr);
    std::slice::from_raw_parts(ptr.add(LEN_SIZE), len)
}

unsafe fn load_str_slice<'a>(ptr: *const u8) -> &'a str {
    std::str::from_utf8_unchecked(load_slice(ptr))
}

unsafe fn get_field_value_ref<'a>(
    kind: FieldValueKind,
    ptr: *const u8,
) -> (FieldValueRef<'a>, usize) {
    match kind {
        FieldValueKind::Unset => (FieldValueRef::Unset, 0),
        FieldValueKind::EntryId => (
            FieldValueRef::EntryId(*(ptr as *const EntryId)),
            size_of::<EntryId>(),
        ),
        FieldValueKind::Text(variant) => match variant {
            StreamKind::Plain => (
                FieldValueRef::Text(FieldValueTextRef::Plain(load_str_slice(ptr))),
                ceil_to_align(LEN_SIZE + load_slice_len(ptr)),
            ),
            StreamKind::StreamChunk(eos) => (
                FieldValueRef::Text(FieldValueTextRef::StreamChunk(load_str_slice(ptr), eos)),
                ceil_to_align(LEN_SIZE + load_slice_len(ptr)),
            ),
            StreamKind::BufferMem(eos) => (
                FieldValueRef::Text(FieldValueTextRef::BufferMem(&*(ptr as *const String), eos)),
                size_of::<String>(),
            ),
            StreamKind::BufferFile(_eos) => todo!(),
        },
        FieldValueKind::Bytes(variant) => match variant {
            StreamKind::Plain => (
                FieldValueRef::Bytes(FieldValueBytesRef::Plain(load_slice(ptr))),
                ceil_to_align(LEN_SIZE + load_slice_len(ptr)),
            ),
            StreamKind::StreamChunk(eos) => (
                FieldValueRef::Bytes(FieldValueBytesRef::StreamChunk(load_slice(ptr), eos)),
                ceil_to_align(LEN_SIZE + load_slice_len(ptr)),
            ),
            StreamKind::BufferMem(eos) => (
                FieldValueRef::Bytes(FieldValueBytesRef::BufferMem(
                    &*(ptr as *const Vec<u8>),
                    eos,
                )),
                ceil_to_align(size_of::<Vec<u8>>()),
            ),
            StreamKind::BufferFile(_eos) => todo!(),
        },
        FieldValueKind::Array(variant) => match variant {
            StreamKind::Plain => todo!(),
            StreamKind::StreamChunk(_eos) => todo!(),
            StreamKind::BufferMem(_eos) => todo!(),
            StreamKind::BufferFile(_eos) => todo!(),
        },
        FieldValueKind::Error => (
            FieldValueRef::Error(&*(ptr as *const OperatorApplicationError)),
            ceil_to_align(size_of::<OperatorApplicationError>()),
        ),
        FieldValueKind::Html => todo!(),
        FieldValueKind::Integer => (
            FieldValueRef::Integer(*(ptr as *const i64)),
            ceil_to_align(size_of::<i64>()),
        ),
        FieldValueKind::Null => (FieldValueRef::Null, 0),
        FieldValueKind::Object => (FieldValueRef::Object(&*(*ptr as *const Object)), 0),
    }
}

impl FieldData {
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}
