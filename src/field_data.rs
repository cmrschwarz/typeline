use std::{
    collections::HashMap,
    iter,
    marker::PhantomData,
    mem::{align_of, size_of, ManuallyDrop},
    ptr::{drop_in_place, NonNull},
    slice,
};

use crate::{operations::OperatorApplicationError, string_store::StringStoreEntry};

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
    Null,
    EntryId,
    Error,
    Html,
    Integer, //TODO: bigint, float, decimal, ...
    Text(StreamKind),
    Bytes(StreamKind),
    Array(StreamKind),
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
    pub fn needs_alignment(self) -> bool {
        use FieldValueKind::*;
        match self {
            Null | Unset => false,
            Text(sk) | Bytes(sk) => match sk {
                StreamKind::Plain | StreamKind::StreamChunk(_) => false,
                _ => true,
            },
            _ => true,
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
    BufferFile(&'a BytesBufferFile, bool), //TODO
}

#[derive(Clone, Copy)]
pub enum FieldValueTextRef<'a> {
    Plain(&'a str),
    StreamChunk(&'a str, bool),
    BufferMem(&'a String, bool),
    BufferFile(&'a TextBufferFile, bool), //TODO
}

#[derive(Clone, Copy)]
pub struct InlineArrayRef<'a> {
    //data lives after the headers. not storing a header slice here to keep provenance
    headers: *const FieldValueHeader,
    count: usize,
    _phantom_data: PhantomData<&'a [FieldValueHeader]>,
}

#[derive(Clone, Copy)]
pub enum FieldValueArrayRef<'a> {
    Plain(InlineArrayRef<'a>),
    StreamChunk(InlineArrayRef<'a>, bool),
    BufferMem(&'a FieldData, bool),
    BufferFile(&'a ArrayBufferFile, bool), //TODO
}

#[derive(Clone)]
struct ObjectEntry {
    kind: FieldValueKind,
    data_offset: usize,
}

#[derive(Clone)]
pub struct Object {
    data: FieldData,
    table: HashMap<StringStoreEntry, ObjectEntry>,
}

pub struct Html {
    //TODO
}

pub struct TextBufferFile {
    //TODO
}
pub struct BytesBufferFile {
    //TODO
}
pub struct ArrayBufferFile {
    //TODO
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

//used to figure out the maximum alignment
#[repr(C)]
union FieldValueUnion {
    text: ManuallyDrop<String>,
    text_file: ManuallyDrop<TextBufferFile>,
    bytes: ManuallyDrop<Vec<u8>>,
    bytes_file: ManuallyDrop<TextBufferFile>,
    array_file: ManuallyDrop<TextBufferFile>,
    object: ManuallyDrop<Object>,
}

const MAX_FIELD_ALIGN: usize = align_of::<FieldValueUnion>();

unsafe fn to_aligned_ref<'a, T>(ptr: *const u8) -> &'a T {
    const ALIGN_MASK: usize = !(MAX_FIELD_ALIGN - 1);
    &*((ptr as usize & ALIGN_MASK) as *const T)
}

type FieldValueSize = u16;

#[allow(dead_code)] //TODO
#[derive(Clone, Copy)]
struct FieldValueHeader {
    kind: FieldValueKind, //TODO: pack to one byte and add convenience functions
    shared_value: bool,
    // this does NOT include potential padding before this
    // field in case it has to be aligned
    size: FieldValueSize,
    run_length: RunLength,
}

impl FieldValueHeader {
    fn data_element_count(&self) -> usize {
        if self.shared_value {
            1
        } else {
            self.run_length as usize
        }
    }
    fn data_size(&self) -> usize {
        if self.shared_value {
            self.size as usize
        } else {
            self.size as usize * self.run_length as usize
        }
    }
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

#[derive(Clone)]
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

    pub fn bounded(self, max_len: usize) -> BoundedFieldDataIterator<'a, Self> {
        BoundedFieldDataIterator::new(max_len, self)
    }
}

#[derive(Clone)]
pub struct BoundedFieldDataIterator<'a, I: Iterator<Item = (RunLength, FieldValueRef<'a>)>> {
    iter: I,
    remaining_len: usize,
}

impl<'a, I: Iterator<Item = (RunLength, FieldValueRef<'a>)>> Iterator
    for BoundedFieldDataIterator<'a, I>
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining_len == 0 {
            return None;
        }
        match self.iter.next() {
            None => {
                self.remaining_len = 0;
                None
            }
            Some((mut len, val)) => {
                if len as usize > self.remaining_len {
                    len = self.remaining_len as RunLength;
                }
                self.remaining_len -= len as usize;
                Some((len, val))
            }
        }
    }
}

impl<'a, I: Iterator<Item = (RunLength, FieldValueRef<'a>)>> BoundedFieldDataIterator<'a, I> {
    pub fn new(max_len: usize, iter: I) -> Self {
        BoundedFieldDataIterator {
            iter,
            remaining_len: max_len,
        }
    }
}

const INLINE_STR_MAX_LEN: usize = 8192;

impl FieldData {
    pub fn clear(&mut self) {
        self.drop_n(usize::MAX);
    }
    pub fn drop_n(&mut self, n: usize) {
        let mut remaining_size = self.data.len();
        let mut remaining_drops = n;
        let mut header_idx = self.header.len();
        while remaining_drops > 0 && remaining_size > 0 {
            header_idx -= 1;
            let h = &mut self.header[header_idx];
            if h.run_length as usize > remaining_drops {
                h.run_length -= remaining_drops as RunLength;
                break;
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
            if h.kind.needs_alignment() {
                remaining_size -= remaining_size % MAX_FIELD_ALIGN;
            }
        }
        self.header.truncate(header_idx);
        self.data.truncate(remaining_size);
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
    // can't make this pub as it can be used to break the type layout
    fn pad_to_align(&mut self) {
        self.data
            .extend(iter::repeat(0).take(self.data.len() % MAX_FIELD_ALIGN));
    }
    pub fn copy_n<'a>(&self, n: usize, targets: &mut [&'a mut FieldData]) {
        let mut h;

        // search backwards to the first header that we need to copy
        let mut remaining_elements = n;
        let mut header_idx = self.header.len();
        debug_assert!(!self.header.is_empty());
        let headers_len = self.header.len();
        let data_size_total = self.data.len();
        let mut copy_data_size = 0;
        // this includes the align
        let mut copy_data_size_at_first_aligned_field = data_size_total;
        let mut first_aligned_field_idx = headers_len;
        let mut first_aligned_field_padding = 0;
        loop {
            header_idx -= 1;
            h = self.header[header_idx];
            if h.run_length as usize >= remaining_elements {
                h.run_length = remaining_elements as RunLength;
                break;
            }
            if header_idx == 0 {
                break;
            }
            remaining_elements -= h.run_length as usize;

            copy_data_size += h.data_size();
            if h.kind.needs_alignment() {
                copy_data_size_at_first_aligned_field = copy_data_size;
                first_aligned_field_idx = header_idx;
                first_aligned_field_padding = (data_size_total - copy_data_size) % MAX_FIELD_ALIGN;
                copy_data_size += first_aligned_field_padding;
            }
        }

        // copy the headers over to the targets
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

        // copy over leading unaligned data
        let data_start = data_size_total - copy_data_size;
        let first_aligned_field_start = data_size_total - copy_data_size_at_first_aligned_field;

        if first_aligned_field_start != data_start {
            unsafe {
                append_data_memcpy(
                    self.data.as_ptr().add(data_start),
                    first_aligned_field_start - data_start - first_aligned_field_padding,
                    targets,
                );
            }
            h = self.header[first_aligned_field_idx];
        }

        // if all memory was unaligned, we're done here
        if first_aligned_field_start == data_size_total {
            return;
        }

        // add padding so all targets are aligned
        for t in targets.iter_mut() {
            t.pad_to_align();
        }

        // copy over the remaining data, using the fact that source and
        // all targets are aligned at this point
        let mut uncopied_data_start = first_aligned_field_start;
        let mut header_idx = first_aligned_field_idx;
        let mut data_pos = first_aligned_field_start;
        loop {
            let field_data_size = h.data_size();
            if h.kind.needs_copy() {
                unsafe {
                    let start = self.data.as_ptr().add(uncopied_data_start);
                    append_data_memcpy(start, data_pos - uncopied_data_start, targets);
                    append_data(h, self.data.as_ptr().add(data_pos), targets)
                }
                data_pos += field_data_size;
                uncopied_data_start = data_pos;
            } else {
                data_pos += field_data_size;
            }
            header_idx += 1;
            if header_idx == headers_len {
                break;
            }
            h = self.header[header_idx];
            if h.kind.needs_alignment() {
                data_pos += data_pos % MAX_FIELD_ALIGN;
            }
        }
        unsafe {
            let start = self.data.as_ptr().add(uncopied_data_start);
            append_data_memcpy(start, data_pos - uncopied_data_start, targets);
        }
    }
    pub fn push_str(&mut self, data: &str, run_length: RunLength) {
        let inline = data.len() < INLINE_STR_MAX_LEN;
        let tgt_kind = if inline {
            FieldValueKind::Text(StreamKind::Plain)
        } else {
            FieldValueKind::Text(StreamKind::BufferMem(true))
        };
        let mut could_amend_header = false;
        if let Some(h) = self.header.last_mut() {
            if !h.shared_value && h.kind == tgt_kind {
                if let Some(rl) = h.run_length.checked_add(1) {
                    h.run_length = rl;
                    could_amend_header = true;
                }
            }
        }

        if !could_amend_header {
            self.header.push(FieldValueHeader {
                kind: tgt_kind,
                shared_value: false,
                size: if inline {
                    data.len()
                } else {
                    size_of::<String>()
                } as FieldValueSize,
                run_length,
            })
        }
        if inline {
            self.data.extend_from_slice(data.as_bytes());
        } else {
            self.pad_to_align();
            let owned_str = ManuallyDrop::new(data.to_owned());
            self.data
                .extend_from_slice(unsafe { as_u8_slice(&owned_str) });
        }
    }
    pub fn iter<'a>(&'a self) -> FieldDataIterator<'a> {
        FieldDataIterator::new(self)
    }
}

pub type EntryId = usize;

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
            Text(sk) => match sk {
                StreamKind::Plain | StreamKind::StreamChunk(_) => unreachable!(),
                StreamKind::BufferMem(_) => drop_in_place(ptr as *mut String),
                StreamKind::BufferFile(_) => drop_in_place(ptr as *mut TextBufferFile),
            },
            Bytes(sk) => match sk {
                StreamKind::Plain | StreamKind::StreamChunk(_) => unreachable!(),
                StreamKind::BufferMem(_) => drop_in_place(ptr as *mut Vec<u8>),
                StreamKind::BufferFile(_) => drop_in_place(ptr as *mut BytesBufferFile),
            },
            Array(_) => todo!(),
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
            Object => extend_with_clones(
                &mut tgt.data,
                source as *const crate::field_data::Object,
                count,
            ),
        }
    }
}

impl FieldData {
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}
