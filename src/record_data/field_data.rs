use std::{
    collections::HashMap,
    mem::{align_of, ManuallyDrop},
    ops::{Deref, DerefMut},
    u8,
};

use super::{
    match_set::MatchSetManager,
    ref_iter::{
        AutoDerefIter, RefAwareBytesBufferIter, RefAwareInlineBytesIter,
        RefAwareInlineTextIter,
    },
};
use crate::{
    operators::errors::OperatorApplicationError,
    utils::{aligned_buf::AlignedBuf, string_store::StringStoreEntry},
};

use self::field_value_flags::{BYTES_ARE_UTF8, SHARED_VALUE};

use super::{
    field::FieldId,
    iters::{FieldIterator, Iter},
    push_interface::PushInterface,
    stream_value::StreamValueId,
    typed::TypedSlice,
};

// if the u32 overflows we just split into two values
pub type RunLength = u32;

#[derive(Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum FieldValueKind {
    Success,
    Null,
    Integer, // TODO: bigint, float, decimal, ...
    StreamValueId,
    Reference,
    Error,
    Html,
    BytesInline,
    BytesBuffer,
    BytesFile,
    Object,
}

#[derive(Clone, Copy, PartialEq)]
pub enum FieldDataType {
    Success,
    Null,
    Integer,
    Error,
    Html,
    Bytes,
    Text,
    Object,
}

impl FieldValueKind {
    pub fn needs_drop(self) -> bool {
        use FieldValueKind::*;
        match self {
            Success | Null | Integer | Reference | StreamValueId => false,
            Error | Html | BytesInline | BytesBuffer | BytesFile | Object => {
                true
            }
        }
    }
    #[inline(always)]
    pub fn needs_alignment(self) -> bool {
        use FieldValueKind::*;
        !matches!(self, Success | Null | BytesInline)
    }
    #[inline]
    pub fn needs_copy(self) -> bool {
        self.needs_drop()
    }
    #[inline(always)]
    pub fn align_size_up(self, size: usize) -> usize {
        if self.needs_alignment() {
            (size + !FIELD_ALIGN_MASK) & FIELD_ALIGN_MASK
        } else {
            size
        }
    }
    #[inline(always)]
    pub fn align_size_down(self, size: usize) -> usize {
        if self.needs_alignment() {
            size & FIELD_ALIGN_MASK
        } else {
            size
        }
    }
    #[inline(always)]
    pub unsafe fn align_ptr_down(self, ptr: *mut u8) -> *mut u8 {
        if self.needs_alignment() {
            // doing this instead of the straight conversion to usize
            // to avoid loosing provenance
            unsafe { ptr.sub(ptr as usize & MAX_FIELD_ALIGN) }
        } else {
            ptr
        }
    }
    #[inline(always)]
    pub unsafe fn align_ptr_up(self, ptr: *mut u8) -> *mut u8 {
        if self.needs_alignment() {
            unsafe {
                ptr.sub((ptr as usize + MAX_FIELD_ALIGN) & MAX_FIELD_ALIGN)
            }
        } else {
            ptr
        }
    }
    pub fn size(self) -> usize {
        match self {
            FieldValueKind::Success => 0,
            FieldValueKind::Null => 0,
            FieldValueKind::Integer => std::mem::size_of::<i64>(),
            FieldValueKind::StreamValueId => {
                std::mem::size_of::<StreamValueId>()
            }
            FieldValueKind::Reference => std::mem::size_of::<FieldReference>(),
            FieldValueKind::Error => {
                std::mem::size_of::<OperatorApplicationError>()
            }
            FieldValueKind::Html => std::mem::size_of::<Html>(),
            FieldValueKind::BytesBuffer => std::mem::size_of::<Vec<u8>>(),
            FieldValueKind::BytesFile => {
                std::mem::size_of::<BytesBufferFile>()
            }
            FieldValueKind::Object => std::mem::size_of::<Object>(),
            // should not be used for size calculations
            // but is used for example in is_zst
            FieldValueKind::BytesInline => usize::MAX,
        }
    }
    pub fn is_variable_sized_type(self) -> bool {
        self == FieldValueKind::BytesInline
    }
    pub fn is_zst(self) -> bool {
        self.size() == 0
    }
    pub fn is_fixed_size_type(self) -> bool {
        !self.is_variable_sized_type() && !self.is_zst()
    }
    pub unsafe fn from_u8(v: u8) -> FieldValueKind {
        unsafe { std::mem::transmute(v) }
    }
}

pub struct Unset;
pub struct Null;
pub struct Success;

#[derive(Clone)]
#[allow(dead_code)] // TODO
struct ObjectEntry {
    kind: FieldValueKind,
    data_offset: usize,
}
#[allow(dead_code)] // TODO
#[derive(Clone)]
pub struct Object {
    data: FieldData,
    table: HashMap<StringStoreEntry, ObjectEntry>,
}

#[derive(Copy, Clone, PartialEq)]
pub struct FieldReference {
    pub field: FieldId,
    pub begin: usize,
    pub end: usize,
}

#[derive(Clone)]
pub struct Html {
    // TODO
}

#[derive(Clone)]
pub struct BytesBufferFile {
    // TODO
}

// only used to figure out the maximum alignment
#[repr(C)]
union FieldValueUnion {
    text: ManuallyDrop<String>,
    bytes: ManuallyDrop<Vec<u8>>,
    bytes_file: ManuallyDrop<BytesBufferFile>,

    // we can't put object in here because otherwise MAX_FIELD_ALIGN
    // would depend on itself (FieldData contains AlignedBuf),
    // so we add the other member of Object here
    object_table: ManuallyDrop<HashMap<StringStoreEntry, ObjectEntry>>,
}

pub const MAX_FIELD_ALIGN: usize = align_of::<FieldValueUnion>();
pub const FIELD_ALIGN_MASK: usize = !(MAX_FIELD_ALIGN - 1);

pub type FieldValueSize = u16;

pub mod field_value_flags {
    use super::MAX_FIELD_ALIGN;
    pub type FieldValueFlags = u8;
    // offset must be zero so we don't have to shift
    const_assert!(MAX_FIELD_ALIGN.is_power_of_two() && MAX_FIELD_ALIGN <= 16);
    pub const LEADING_PADDING: FieldValueFlags = 0xF; // consumes offsets 0 through 3
    pub const SAME_VALUE_AS_PREVIOUS_OFFSET: FieldValueFlags = 4;
    pub const SHARED_VALUE_OFFSET: FieldValueFlags = 5;
    pub const BYTES_ARE_UTF8_OFFSET: FieldValueFlags = 6;
    pub const DELETED_OFFSET: FieldValueFlags = 7;

    pub const SHARED_VALUE: FieldValueFlags = 1 << SHARED_VALUE_OFFSET;
    pub const BYTES_ARE_UTF8: FieldValueFlags = 1 << BYTES_ARE_UTF8_OFFSET;
    pub const DELETED: FieldValueFlags = 1 << DELETED_OFFSET;
    pub const SAME_VALUE_AS_PREVIOUS: FieldValueFlags =
        1 << SAME_VALUE_AS_PREVIOUS_OFFSET;

    pub const DEFAULT: FieldValueFlags = 0;
}
pub use field_value_flags::FieldValueFlags;
#[derive(Clone, Copy, PartialEq)]
pub struct FieldValueFormat {
    pub kind: FieldValueKind,
    pub flags: FieldValueFlags,
    // this does NOT include potential padding before this
    // field in case it has to be aligned
    pub size: FieldValueSize,
}

impl Default for FieldValueFormat {
    fn default() -> Self {
        Self {
            kind: FieldValueKind::Null,
            flags: field_value_flags::DEFAULT,
            size: 0,
        }
    }
}

#[derive(Clone, Copy, Default)]
pub struct FieldValueHeader {
    pub fmt: FieldValueFormat,
    pub run_length: RunLength,
}

impl FieldValueFormat {
    #[inline(always)]
    pub fn shared_value(self) -> bool {
        self.flags & field_value_flags::SHARED_VALUE != 0
    }
    #[inline(always)]
    pub fn set_shared_value(&mut self, val: bool) {
        self.flags &= !field_value_flags::SHARED_VALUE;
        self.flags |=
            (val as FieldValueFlags) << field_value_flags::SHARED_VALUE_OFFSET;
    }
    pub fn bytes_are_utf8(self) -> bool {
        self.flags & field_value_flags::BYTES_ARE_UTF8 != 0
    }
    pub fn set_bytes_are_utf8(&mut self, val: bool) {
        self.flags &= !field_value_flags::BYTES_ARE_UTF8;
        self.flags |= (val as FieldValueFlags)
            << field_value_flags::BYTES_ARE_UTF8_OFFSET;
    }
    #[inline(always)]
    pub fn leading_padding(self) -> usize {
        (self.flags & field_value_flags::LEADING_PADDING) as usize
    }
    pub fn set_leading_padding(&mut self, val: usize) {
        debug_assert!(
            val & !(field_value_flags::LEADING_PADDING as usize) == 0
        );
        self.flags &= !field_value_flags::LEADING_PADDING;
        self.flags |= (val as u8) & field_value_flags::LEADING_PADDING;
    }
    #[inline(always)]
    pub fn deleted(self) -> bool {
        self.flags & field_value_flags::DELETED != 0
    }
    pub fn set_deleted(&mut self, val: bool) {
        self.flags &= !field_value_flags::DELETED;
        self.flags |=
            (val as FieldValueFlags) << field_value_flags::DELETED_OFFSET;
    }
    #[inline(always)]
    pub fn same_value_as_previous(self) -> bool {
        self.flags & field_value_flags::SAME_VALUE_AS_PREVIOUS != 0
    }
    #[inline(always)]
    pub fn set_same_value_as_previous(&mut self, val: bool) {
        self.flags &= !field_value_flags::SAME_VALUE_AS_PREVIOUS;
        self.flags |= (val as FieldValueFlags)
            << field_value_flags::SAME_VALUE_AS_PREVIOUS_OFFSET;
    }
}

impl Deref for FieldValueHeader {
    type Target = FieldValueFormat;
    fn deref(&self) -> &Self::Target {
        &self.fmt
    }
}

impl DerefMut for FieldValueHeader {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.fmt
    }
}

impl FieldValueHeader {
    pub fn unique_data_element_count(&self) -> RunLength {
        if self.shared_value() {
            if self.same_value_as_previous() {
                0
            } else {
                1
            }
        } else {
            self.run_length
        }
    }
    pub fn data_element_count(&self) -> RunLength {
        if self.shared_value() {
            1
        } else {
            self.run_length
        }
    }
    pub fn data_size(&self) -> usize {
        if self.shared_value() {
            self.size as usize
        } else {
            self.size as usize * self.run_length as usize
        }
    }
    pub fn data_size_unique(&self) -> usize {
        if self.same_value_as_previous() {
            0
        } else {
            self.data_size()
        }
    }
    pub fn total_size(&self) -> usize {
        self.data_size() + self.leading_padding()
    }
    pub fn total_size_unique(&self) -> usize {
        if self.same_value_as_previous() {
            0
        } else {
            self.total_size()
        }
    }
    pub fn run_len_rem(&self) -> RunLength {
        RunLength::MAX - self.run_length
    }
}

pub const INLINE_STR_MAX_LEN: usize = 8192;

#[derive(Default)]
pub struct FieldData {
    pub(super) data: AlignedBuf<MAX_FIELD_ALIGN>,
    pub(super) headers: Vec<FieldValueHeader>,
    pub(super) field_count: usize,
}

impl Clone for FieldData {
    fn clone(&self) -> Self {
        let mut fd = Self {
            data: AlignedBuf::with_capacity(self.data.len()),
            headers: Vec::with_capacity(self.headers.len()),
            field_count: 0,
        };
        let fd_ref = &mut fd;
        FieldData::copy(self.iter(), &mut |f| f(fd_ref));
        fd
    }
}

pub struct FieldDataInternals<'a> {
    pub data: &'a mut AlignedBuf<MAX_FIELD_ALIGN>,
    pub header: &'a mut Vec<FieldValueHeader>,
    pub field_count: &'a mut usize,
}
impl FieldData {
    pub unsafe fn from_raw_parts(
        header: Vec<FieldValueHeader>,
        data: AlignedBuf<MAX_FIELD_ALIGN>,
        field_count: usize,
    ) -> Self {
        Self {
            headers: header,
            data,
            field_count,
        }
    }
    pub fn clear(&mut self) {
        let data_ptr = self.data.as_mut_ptr();
        let mut iter = self.iter();
        let mut slice_start_ptr = data_ptr;
        while let Some(range) = iter.typed_range_fwd(usize::MAX, 0) {
            unsafe {
                let type_id = range.data.type_id();
                let len = range.data.len();
                TypedSlice::drop_from_type_id(slice_start_ptr, len, type_id);
                slice_start_ptr = data_ptr.add(iter.data);
            }
        }
        self.field_count = 0;
        self.headers.clear();
        self.data.clear();
    }

    pub unsafe fn pad_to_align(&mut self) -> usize {
        let mut align = self.data.len() % MAX_FIELD_ALIGN;
        if align == 0 {
            return 0;
        }
        align = MAX_FIELD_ALIGN - align;
        self.data.resize(self.data.len() + align, 0);
        align
    }

    // this is technically safe, but will leak unless paired with a
    // header that matches the contained type ranges (which itself is not safe)
    pub fn clone_data(&self) -> AlignedBuf<MAX_FIELD_ALIGN> {
        let mut res = AlignedBuf::new();
        let mut iter = self.iter();
        while let Some(tr) =
            iter.typed_range_fwd(usize::MAX, field_value_flags::DEFAULT)
        {
            unsafe { append_data(tr.data, &mut |f| f(&mut res)) };
        }
        res
    }

    pub unsafe fn copy_data<'a>(
        mut iter: impl FieldIterator<'a>,
        targets_applicator: &mut impl FnMut(&mut dyn FnMut(&mut FieldData)),
    ) -> usize {
        let mut fields_copied = 0;
        while let Some(tr) =
            iter.typed_range_fwd(usize::MAX, field_value_flags::DEFAULT)
        {
            unsafe {
                append_data(tr.data, &mut |f: &mut dyn Fn(
                    &mut AlignedBuf<MAX_FIELD_ALIGN>,
                )| {
                    targets_applicator(&mut |fd| f(&mut fd.data))
                })
            };
            fields_copied += tr.field_count;
        }
        fields_copied
    }

    pub fn copy<'a>(
        mut iter: impl FieldIterator<'a>,
        targets_applicator: &mut impl FnMut(&mut dyn FnMut(&mut FieldData)),
    ) -> usize {
        let mut copied_fields = 0;
        while let Some(tr) =
            iter.typed_range_fwd(usize::MAX, field_value_flags::DELETED)
        {
            copied_fields += tr.field_count;
            targets_applicator(&mut |fd| {
                let first_header_idx = fd.headers.len();
                fd.headers.extend_from_slice(tr.headers);
                if tr.headers[0].kind.needs_alignment() {
                    let align = unsafe { fd.pad_to_align() };
                    fd.headers[first_header_idx].set_leading_padding(align);
                }
                fd.headers[first_header_idx].run_length -=
                    tr.first_header_run_length_oversize;
            });
            unsafe {
                append_data(tr.data, &mut |f: &mut dyn Fn(
                    &mut AlignedBuf<MAX_FIELD_ALIGN>,
                )| {
                    targets_applicator(&mut |fd| f(&mut fd.data))
                })
            };
        }
        targets_applicator(&mut |fd| fd.field_count += copied_fields);
        copied_fields
    }

    pub fn copy_resolve_refs<'a, I: FieldIterator<'a>>(
        match_set_mgr: &mut MatchSetManager,
        iter: &mut AutoDerefIter<'a, I>,
        targets_applicator: &mut impl FnMut(&mut dyn FnMut(&mut FieldData)),
    ) -> usize {
        let mut copied_fields = 0;
        // by setting the deleted flag here, we can avoid copying deleted
        // records
        while let Some(tr) = iter.typed_range_fwd(
            match_set_mgr,
            usize::MAX,
            field_value_flags::BYTES_ARE_UTF8 | field_value_flags::DELETED,
        ) {
            copied_fields += tr.base.field_count;
            if tr.refs.is_none() {
                targets_applicator(&mut |fd| {
                    let first_header_idx = fd.headers.len();
                    fd.headers.extend_from_slice(tr.base.headers);
                    if tr.base.headers[0].kind.needs_alignment() {
                        let align = unsafe { fd.pad_to_align() };
                        fd.headers[first_header_idx]
                            .set_leading_padding(align);
                    }
                    fd.headers[first_header_idx].run_length -=
                        tr.base.first_header_run_length_oversize;
                });
                unsafe {
                    append_data(tr.base.data, &mut |f: &mut dyn Fn(
                        &mut AlignedBuf<MAX_FIELD_ALIGN>,
                    )| {
                        targets_applicator(&mut |fd| f(&mut fd.data))
                    })
                };
            } else {
                match tr.base.data {
                    TypedSlice::BytesInline(data) => {
                        for (v, rl, _offset) in
                            RefAwareInlineBytesIter::from_range(&tr, data)
                        {
                            targets_applicator(&mut |fd| {
                                // TODO: maybe do a little rle here?
                                fd.headers.push(FieldValueHeader {
                                    fmt: FieldValueFormat {
                                        kind: FieldValueKind::BytesInline,
                                        flags: SHARED_VALUE,
                                        size: v.len() as FieldValueSize,
                                    },
                                    run_length: rl,
                                });
                                fd.data.extend_from_slice(v);
                            });
                        }
                    }
                    TypedSlice::TextInline(data) => {
                        for (v, rl, _offset) in
                            RefAwareInlineTextIter::from_range(&tr, data)
                        {
                            targets_applicator(&mut |fd| {
                                // TODO: maybe do a little rle here?
                                fd.headers.push(FieldValueHeader {
                                    fmt: FieldValueFormat {
                                        kind: FieldValueKind::BytesInline,
                                        flags: SHARED_VALUE | BYTES_ARE_UTF8,
                                        size: v.len() as FieldValueSize,
                                    },
                                    run_length: rl,
                                });
                                fd.data.extend_from_slice(v.as_bytes());
                            });
                        }
                    }
                    TypedSlice::BytesBuffer(data) => {
                        for (v, rl, _offset) in
                            RefAwareBytesBufferIter::from_range(&tr, data)
                        {
                            targets_applicator(&mut |fd| {
                                fd.push_bytes(v, rl as usize, true, false);
                            });
                        }
                    }
                    TypedSlice::Success(_)
                    | TypedSlice::Null(_)
                    | TypedSlice::Integer(_)
                    | TypedSlice::StreamValueId(_)
                    | TypedSlice::Reference(_)
                    | TypedSlice::Error(_)
                    | TypedSlice::Html(_)
                    | TypedSlice::Object(_) => unreachable!(),
                }
            }
        }
        targets_applicator(&mut |fd| fd.field_count += copied_fields);
        copied_fields
    }

    pub fn iter(&self) -> Iter<'_, &'_ FieldData> {
        Iter::from_start(self)
    }
    pub unsafe fn internals(&mut self) -> FieldDataInternals {
        FieldDataInternals {
            data: &mut self.data,
            header: &mut self.headers,
            field_count: &mut self.field_count,
        }
    }
    pub fn field_count(&self) -> usize {
        self.field_count
    }
}

#[inline(always)]
unsafe fn extend_with_clones<T: Clone>(
    target_applicator: &mut impl FnMut(
        &mut dyn Fn(&mut AlignedBuf<MAX_FIELD_ALIGN>),
    ),
    src: &[T],
) {
    let src_size = std::mem::size_of_val(src);
    target_applicator(&mut |data| {
        data.reserve(src_size);
        unsafe {
            let mut tgt = data.as_mut_ptr_range().end as *mut T;
            for v in src {
                std::ptr::write(tgt, v.clone());
                tgt = tgt.add(1);
            }
            data.set_len(data.len() + src_size);
        }
    });
}

#[inline(always)]
unsafe fn extend_raw<T: Sized>(
    target_applicator: &mut impl FnMut(
        &mut dyn Fn(&mut AlignedBuf<MAX_FIELD_ALIGN>),
    ),
    src: &[T],
) {
    let src_bytes = unsafe {
        std::slice::from_raw_parts(
            src.as_ptr() as *const u8,
            std::mem::size_of_val(src),
        )
    };
    target_applicator(&mut |data| data.extend_from_slice(src_bytes));
}

#[inline(always)]
unsafe fn append_data(
    ts: TypedSlice<'_>,
    target_applicator: &mut impl FnMut(
        &mut dyn Fn(&mut AlignedBuf<MAX_FIELD_ALIGN>),
    ),
) {
    unsafe {
        match ts {
            TypedSlice::Null(_) | TypedSlice::Success(_) => (),
            TypedSlice::Integer(v) => extend_raw(target_applicator, v),
            TypedSlice::StreamValueId(v) => extend_raw(target_applicator, v),
            TypedSlice::Reference(v) => extend_raw(target_applicator, v),
            TypedSlice::BytesInline(v) => extend_raw(target_applicator, v),
            TypedSlice::TextInline(v) => {
                extend_raw(target_applicator, v.as_bytes())
            }
            TypedSlice::BytesBuffer(v) => {
                extend_with_clones(target_applicator, v)
            }
            TypedSlice::Error(v) => extend_with_clones(target_applicator, v),
            TypedSlice::Html(v) => extend_with_clones(target_applicator, v),
            TypedSlice::Object(v) => extend_with_clones(target_applicator, v),
        };
    }
}

impl FieldData {
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl Drop for FieldData {
    fn drop(&mut self) {
        self.clear()
    }
}
