use std::{
    marker::PhantomData,
    mem::{size_of, ManuallyDrop},
    ptr::{drop_in_place, NonNull},
    slice,
};

use indexmap::IndexMap;

use crate::operations::operator_base::OperatorApplicationError;

//if the u32 overflows we just split into two values
pub type RunLength = u32;

#[derive(Clone, Copy, PartialEq)]
pub enum StreamKind {
    Plain,
    StreamChunk(bool),
    BufferMem(bool),
    BufferFile(bool),
}

#[derive(Clone, Copy, PartialEq)]
pub enum FieldValueKind {
    Unset,
    EntryId,
    Text(StreamKind),
    Bytes(StreamKind),
    Array(StreamKind),
    Error,
    Html,
    Integer, //TODO: bigint, float, decimal, ...
    Null,
    Object,
}

impl FieldValueKind {
    pub fn needs_drop(self) -> bool {
        use FieldValueKind::*;
        match self {
            Unset | EntryId | Integer | Null => false,
            Text(sk) | Bytes(sk) | Array(sk) => match sk {
                StreamKind::Plain | StreamKind::StreamChunk(_) => false,
                StreamKind::BufferMem(_) | StreamKind::BufferFile(_) => true,
            },
            Error => true,
            Html => true,
            Object => true,
        }
    }
    pub fn needs_copy(self) -> bool {
        self.needs_drop()
    }
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

#[derive(Clone, Copy)]
pub enum FieldValueArrayRef<'a> {
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
    #[allow(dead_code)] //TODO
    Html(u8),
    Integer(i64),
    Null,
    Object(&'a Object),
}

type FieldValueSize = u16;

#[allow(dead_code)] //TODO
#[derive(Clone, Copy)]
struct FieldValueHeader {
    kind: FieldValueKind, //TODO: pack to one byte and add convenience functions
    shared_value: bool,
    size: FieldValueSize,
    run_length: RunLength,
}

#[derive(Default)]
pub struct FieldData {
    data: Vec<u8>,
    header: Vec<FieldValueHeader>,
}

impl Clone for FieldData {
    fn clone(&self) -> Self {
        let mut res = Self {
            data: Vec::with_capacity(self.data.len()),
            header: self.header.clone(),
        };
        self.copy_n(usize::MAX, &mut [&mut res]);
        res
    }
}

pub struct FieldDataIterator<'a> {
    header: *const FieldValueHeader,
    header_r_end: *const FieldValueHeader,
    data: NonNull<u8>,
    handled_run_len: RunLength,
    _phantom_data: PhantomData<&'a FieldData>,
}

impl<'a> Iterator for FieldDataIterator<'a> {
    type Item = (RunLength, FieldValueRef<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.header == self.header_r_end {
            return None;
        }
        unsafe {
            let h = *self.header;
            let ptr = self.data.as_ptr().sub(h.size as usize);
            let val = get_field_value_ref(h, ptr);
            self.data = NonNull::new_unchecked(ptr);
            if h.shared_value {
                self.header = self.header.add(1);
                Some((h.run_length, val))
            } else {
                self.handled_run_len += 1;
                if self.handled_run_len == h.run_length {
                    self.handled_run_len = 0;
                    self.header = self.header.add(1);
                }
                Some((1, val))
            }
        }
    }
}

impl<'a> FieldDataIterator<'a> {
    pub fn new(field_data: &'a FieldData) -> FieldDataIterator<'a> {
        FieldDataIterator {
            header: field_data.header.as_ptr_range().end,
            header_r_end: field_data.header.as_ptr_range().start,
            data: unsafe { NonNull::new_unchecked(field_data.data.as_ptr() as *mut u8) },
            handled_run_len: 0,
            _phantom_data: PhantomData::default(),
        }
    }

    pub fn peek(&self) -> Option<<Self as Iterator>::Item> {
        if self.header == self.header_r_end {
            return None;
        }
        unsafe {
            let h = *self.header;
            let val = get_field_value_ref(h, self.data.as_ptr().sub(h.size as usize));
            if h.shared_value {
                Some((h.run_length, val))
            } else {
                Some((1, val))
            }
        }
    }
}
impl FieldData {
    pub fn clear(&mut self) {
        self.header.clear();
        self.data.clear();
    }
    pub fn drop_n(&mut self, n: usize) {
        let mut remaining_size = self.data.len();
        let mut remaining_drops = n;
        while remaining_drops > 0 {
            let h = self.header.last_mut().unwrap();
            if h.run_length as usize > remaining_drops {
                self.data.truncate(remaining_size);
                h.run_length -= remaining_drops as RunLength;
                return;
            }
            remaining_drops -= h.run_length as usize;
            let drop_count = if h.shared_value { 1 } else { h.run_length };

            if h.kind.needs_drop() {
                for _ in 0..drop_count {
                    remaining_size -= h.size as usize;
                    unsafe {
                        drop_data(*h, self.data.as_mut_ptr().add(remaining_size));
                    }
                }
            } else {
                remaining_size -= drop_count as usize * h.size as usize;
            }
            self.header.pop();
        }
        self.data.truncate(remaining_size);
    }

    pub fn copy_n<'a>(&self, n: usize, targets: &mut [&'a mut FieldData]) {
        let mut examination_range = self.data.len()..self.data.len();
        let mut remaining_elements = n;
        let mut header_idx = self.header.len();
        while remaining_elements > 0 && header_idx > 0 {
            header_idx -= 1;
            let mut h = self.header[header_idx];
            if header_idx == 0 {
                remaining_elements = h.run_length as usize;
            }
            if h.run_length as usize >= remaining_elements {
                h.run_length = remaining_elements as RunLength;
                remaining_elements = 0;
                for t in targets.iter_mut() {
                    if let Some(h_tgt) = t.header.last_mut() {
                        if !h.shared_value && !h_tgt.shared_value && h_tgt.kind == h.kind {
                            if let Some(rl) = h_tgt.run_length.checked_add(h.run_length) {
                                h_tgt.run_length = rl;
                                if header_idx + 1 != self.header.len() {
                                    t.header.extend(&self.header[header_idx + 1..]);
                                }
                                continue;
                            }
                        }
                    }
                    t.header.extend(&self.header[header_idx..]);
                }
            } else {
                remaining_elements -= h.run_length as usize;
            }
            if h.kind.needs_copy() {
                let size = examination_range.end - examination_range.start;
                unsafe {
                    let start = self.data.as_ptr().add(examination_range.start);
                    append_data_memcpy(start, size, targets);
                    append_data(h, start.add(size), targets)
                }
                examination_range.end = examination_range.start;
            } else {
                examination_range.start -= h.run_length as usize * h.size as usize;
            }
        }
        let size = examination_range.end - examination_range.start;
        unsafe {
            let start = self.data.as_ptr().add(examination_range.start);
            append_data_memcpy(start, size, targets);
        }
    }
    pub fn dup_nth(&mut self, n: usize, new_run_len: RunLength) {
        let mut hi = self.header.len();
        let mut i = 0;
        while i < n {
            hi -= 1;
            i += self.header[hi].run_length as usize;
        }
        let h = &mut self.header[hi];
        let mut tgt = *h;
        tgt.run_length = new_run_len;
        debug_assert!(h.run_length > 0 && (h.shared_value || h.run_length > 1));
        if h.shared_value {
            tgt.run_length -= 1;
            if let Some(rl) = h.run_length.checked_add(tgt.run_length) {
                h.run_length = rl;
            } else {
                self.header.insert(hi, tgt);
            }
        } else if i == n + 1 {
            //nth is first of this header, insert it at the start
            h.run_length -= 1;
            if h.run_length == 1 {
                h.shared_value = true;
            }
            self.header.insert(hi + 1, tgt);
        } else if i == n + h.run_length as usize {
            //nth is last of this header, insert it as the start
            h.run_length -= 1;
            if h.run_length == 1 {
                h.shared_value = true;
            }
            self.header.insert(hi, tgt);
        } else {
            // nth is sandwiched in this header, insert two
            let rl_after = (i - n) as RunLength;
            h.run_length = rl_after;
            hi += 1;
            let mut tgt2 = tgt;
            tgt2.run_length = h.run_length - rl_after;
            self.header.splice(hi..hi, [tgt, tgt2]);
        }
    }
}

pub type EntryId = usize;

const LEN_SIZE: usize = size_of::<usize>();
const ALIGN: usize = size_of::<usize>();

fn ceil_to_align(size: usize) -> usize {
    (size + ALIGN - 1) / ALIGN * ALIGN
}

unsafe fn load_slice<'a>(ptr: *const u8, size: FieldValueSize) -> &'a [u8] {
    std::slice::from_raw_parts(ptr, size as usize)
}

unsafe fn load_str_slice<'a>(ptr: *const u8, size: FieldValueSize) -> &'a str {
    std::str::from_utf8_unchecked(load_slice(ptr, size))
}

unsafe fn get_field_value_ref<'a>(fmt: FieldValueHeader, ptr: *const u8) -> FieldValueRef<'a> {
    let size = fmt.size;
    match fmt.kind {
        FieldValueKind::Unset => FieldValueRef::Unset,
        FieldValueKind::EntryId => FieldValueRef::EntryId(*(ptr as *const EntryId)),
        FieldValueKind::Text(variant) => match variant {
            StreamKind::Plain => {
                FieldValueRef::Text(FieldValueTextRef::Plain(load_str_slice(ptr, size)))
            }
            StreamKind::StreamChunk(eos) => FieldValueRef::Text(FieldValueTextRef::StreamChunk(
                load_str_slice(ptr, size),
                eos,
            )),
            StreamKind::BufferMem(eos) => {
                FieldValueRef::Text(FieldValueTextRef::BufferMem(&*(ptr as *const String), eos))
            }
            StreamKind::BufferFile(_eos) => todo!(),
        },
        FieldValueKind::Bytes(variant) => match variant {
            StreamKind::Plain => {
                FieldValueRef::Bytes(FieldValueBytesRef::Plain(load_slice(ptr, size)))
            }
            StreamKind::StreamChunk(eos) => {
                FieldValueRef::Bytes(FieldValueBytesRef::StreamChunk(load_slice(ptr, size), eos))
            }
            StreamKind::BufferMem(eos) => FieldValueRef::Bytes(FieldValueBytesRef::BufferMem(
                &*(ptr as *const Vec<u8>),
                eos,
            )),
            StreamKind::BufferFile(_eos) => todo!(),
        },
        FieldValueKind::Array(variant) => match variant {
            StreamKind::Plain => todo!(),
            StreamKind::StreamChunk(_eos) => todo!(),
            StreamKind::BufferMem(_eos) => todo!(),
            StreamKind::BufferFile(_eos) => todo!(),
        },
        FieldValueKind::Error => FieldValueRef::Error(&*(ptr as *const OperatorApplicationError)),
        FieldValueKind::Html => todo!(),
        FieldValueKind::Integer => FieldValueRef::Integer(*(ptr as *const i64)),
        FieldValueKind::Null => FieldValueRef::Null,
        FieldValueKind::Object => FieldValueRef::Object(&*(*ptr as *const Object)),
    }
}

unsafe fn drop_data(fmt: FieldValueHeader, ptr: *mut u8) {
    use FieldValueKind::*;
    unsafe {
        match fmt.kind {
            Unset | EntryId | Integer | Null => unreachable!(),
            Text(sk) | Bytes(sk) => match sk {
                StreamKind::Plain | StreamKind::StreamChunk(_) => unreachable!(),
                StreamKind::BufferMem(_) => drop_in_place(ptr as *mut Vec<u8>),
                StreamKind::BufferFile(_) => todo!(),
            },
            Array(sk) => todo!(),
            Error => drop_in_place(ptr as *mut OperatorApplicationError),
            Html => todo!(),
            Object => drop_in_place(ptr as *mut crate::field_data::Object),
        }
    }
}

unsafe fn append_data_memcpy<'a>(ptr: *const u8, size: usize, targets: &mut [&'a mut FieldData]) {
    let source = slice::from_raw_parts(ptr, size);
    for tgt in targets {
        tgt.data.extend_from_slice(source);
    }
}

unsafe fn as_u8_slice<T: Sized>(p: &T) -> &[u8] {
    slice::from_raw_parts((p as *const T) as *const u8, size_of::<T>())
}

unsafe fn extend_with_clones<T: Clone>(tgt: &mut Vec<u8>, src: *const T, count: usize) {
    let source = slice::from_raw_parts(src as *const ManuallyDrop<Vec<T>>, count);
    for v in source {
        tgt.extend_from_slice(as_u8_slice(&v.clone()));
    }
}

unsafe fn append_data<'a>(
    fmt: FieldValueHeader,
    source: *const u8,
    targets: &mut [&'a mut FieldData],
) {
    let count = if fmt.shared_value {
        1
    } else {
        fmt.run_length as usize
    };
    use FieldValueKind::*;
    for tgt in targets {
        match fmt.kind {
            Unset | EntryId | Integer | Null => unreachable!(),
            Text(sk) | Bytes(sk) => match sk {
                StreamKind::Plain | StreamKind::StreamChunk(_) => unreachable!(),
                StreamKind::BufferMem(_) => unsafe {
                    extend_with_clones(&mut tgt.data, source as *const Vec<u8>, count)
                },

                StreamKind::BufferFile(_) => todo!(),
            },
            Array(sk) => match sk {
                StreamKind::Plain | StreamKind::StreamChunk(_) => unreachable!(),
                StreamKind::BufferMem(_) => unsafe {
                    extend_with_clones(&mut tgt.data, source as *const FieldData, count)
                },

                StreamKind::BufferFile(_) => todo!(),
            },
            Error => unsafe {
                extend_with_clones(
                    &mut tgt.data,
                    source as *const OperatorApplicationError,
                    count,
                )
            },

            Html => todo!(),
            Object => {
                let source = slice::from_raw_parts(
                    source as *const ManuallyDrop<Vec<crate::field_data::Object>>,
                    count,
                );
                for v in source {
                    tgt.data.extend_from_slice(as_u8_slice(&v.clone()));
                }
            }
        }
    }
}

impl FieldData {
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}
