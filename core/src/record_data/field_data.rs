use std::{
    any::Any,
    fmt::Display,
    mem::{align_of, size_of, size_of_val, ManuallyDrop},
    ops::{Deref, DerefMut},
    u8,
};

use super::{
    custom_data::CustomDataBox,
    field_value::{
        Array, FieldReference, Null, Object, SlicedFieldReference, Undefined,
    },
    match_set::MatchSetManager,
    ref_iter::{
        AutoDerefIter, RefAwareBytesBufferIter, RefAwareInlineBytesIter,
        RefAwareInlineTextIter, RefAwareTextBufferIter,
    },
};
use crate::{
    operators::errors::OperatorApplicationError,
    utils::aligned_buf::AlignedBuf,
};

use self::field_value_flags::SHARED_VALUE;
pub use field_value_flags::FieldValueFlags;
use num::{BigInt, BigRational};

use super::{
    iters::{FieldIterator, Iter},
    push_interface::PushInterface,
    stream_value::StreamValueId,
    typed::TypedSlice,
};

// if the u32 overflows we just split into two values
pub type RunLength = u32;

// the kinds of data representations that are stored in a `FieldData` Object
// this includes StreamValueId, FieldReferences, ...
// which are not actual data types, but helper representations / indirections
// This also does not differentiate between the text and bytes type as they
// have the same data layout
#[derive(Default, Debug, Clone, Copy, PartialEq)]
pub enum FieldValueRepr {
    #[default]
    Null,
    Undefined,
    Int,
    BigInt,
    Float,
    Rational,
    TextInline,
    TextBuffer,
    TextFile,
    BytesInline,
    BytesBuffer,
    BytesFile,
    Object,
    Array,
    Custom,
    Error,
    StreamValueId,
    FieldReference,
    SlicedFieldReference,
    // ENHANCE //PERF (maybe): CustomDynamicLength, CustomDynamicLengthAligned
    // (store some subtype index at the start of the actual data)
}

#[derive(Clone)]
pub struct TextBufferFile {
    // TODO
}

#[derive(Clone)]
pub struct BytesBufferFile {
    // TODO
}

#[derive(Clone, Copy, PartialEq)]
pub struct FieldValueFormat {
    pub repr: FieldValueRepr,
    pub flags: FieldValueFlags,
    // this does NOT include potential padding before this
    // field in case it has to be aligned
    pub size: FieldValueSize,
}

#[derive(Clone, Copy, Default)]
pub struct FieldValueHeader {
    pub fmt: FieldValueFormat,
    pub run_length: RunLength,
}

#[derive(Default)]
pub struct FieldData {
    pub(super) data: FieldDataBuffer,
    pub(super) headers: Vec<FieldValueHeader>,
    pub(super) field_count: usize,
}

pub struct FieldDataInternals<'a> {
    pub data: &'a mut FieldDataBuffer,
    pub header: &'a mut Vec<FieldValueHeader>,
    pub field_count: &'a mut usize,
}

// only used to figure out the maximum alignment needed for fields
// the size cannot be used because of the the `Object` shenanegans, see below
#[repr(C)]
union FieldValueAlignmentCheckUnion {
    text: ManuallyDrop<String>,
    bytes: ManuallyDrop<Vec<u8>>,
    bytes_file: ManuallyDrop<BytesBufferFile>,
    object: ManuallyDrop<Object>,
}

pub const MAX_FIELD_ALIGN: usize = align_of::<FieldValueAlignmentCheckUnion>();
pub const FIELD_ALIGN_MASK: usize = !(MAX_FIELD_ALIGN - 1);
pub type FieldDataBuffer = AlignedBuf<MAX_FIELD_ALIGN>;

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
    pub const DELETED: FieldValueFlags = 1 << DELETED_OFFSET;
    pub const SAME_VALUE_AS_PREVIOUS: FieldValueFlags =
        1 << SAME_VALUE_AS_PREVIOUS_OFFSET;

    pub const DEFAULT: FieldValueFlags = 0;
}

// used to constrain generic functions that accept data for field values
pub unsafe trait FieldValueType: PartialEq + Any {
    const REPR: FieldValueRepr;
    const DST: bool = false;
    const ZST: bool = false;
}
pub unsafe trait FixedSizeFieldValueType:
    Clone + PartialEq + Any + Sized
{
    const REPR: FieldValueRepr;
    const FLAGS: FieldValueFlags = field_value_flags::DEFAULT;
    const ZST: bool = false;
}
unsafe impl<T: FixedSizeFieldValueType> FieldValueType for T {
    const REPR: FieldValueRepr = Self::REPR;
    const ZST: bool = Self::ZST;
    const DST: bool = false;
}

unsafe impl FixedSizeFieldValueType for Undefined {
    const REPR: FieldValueRepr = FieldValueRepr::Null;
    const ZST: bool = true;
}
unsafe impl FixedSizeFieldValueType for Null {
    const REPR: FieldValueRepr = FieldValueRepr::Undefined;
    const ZST: bool = true;
}
unsafe impl FixedSizeFieldValueType for i64 {
    const REPR: FieldValueRepr = FieldValueRepr::Int;
}
unsafe impl FixedSizeFieldValueType for StreamValueId {
    const REPR: FieldValueRepr = FieldValueRepr::StreamValueId;
}
unsafe impl FixedSizeFieldValueType for FieldReference {
    const REPR: FieldValueRepr = FieldValueRepr::FieldReference;
}
unsafe impl FixedSizeFieldValueType for SlicedFieldReference {
    const REPR: FieldValueRepr = FieldValueRepr::SlicedFieldReference;
}
unsafe impl FixedSizeFieldValueType for OperatorApplicationError {
    const REPR: FieldValueRepr = FieldValueRepr::Error;
}
unsafe impl FixedSizeFieldValueType for Vec<u8> {
    const REPR: FieldValueRepr = FieldValueRepr::BytesBuffer;
}
unsafe impl FixedSizeFieldValueType for String {
    const REPR: FieldValueRepr = FieldValueRepr::TextBuffer;
}
unsafe impl FixedSizeFieldValueType for Object {
    const REPR: FieldValueRepr = FieldValueRepr::Object;
}
unsafe impl FixedSizeFieldValueType for Array {
    const REPR: FieldValueRepr = FieldValueRepr::Array;
}
unsafe impl FixedSizeFieldValueType for BigInt {
    const REPR: FieldValueRepr = FieldValueRepr::BigInt;
}
unsafe impl FixedSizeFieldValueType for f64 {
    const REPR: FieldValueRepr = FieldValueRepr::Float;
}
unsafe impl FixedSizeFieldValueType for BigRational {
    const REPR: FieldValueRepr = FieldValueRepr::Rational;
}
unsafe impl FixedSizeFieldValueType for CustomDataBox {
    const REPR: FieldValueRepr = FieldValueRepr::Custom;
}
unsafe impl FieldValueType for [u8] {
    const REPR: FieldValueRepr = FieldValueRepr::BytesInline;
    const DST: bool = true;
}
unsafe impl FieldValueType for str {
    const REPR: FieldValueRepr = FieldValueRepr::TextInline;
    const DST: bool = true;
}

pub const INLINE_STR_MAX_LEN: usize = 8192;

impl FieldValueRepr {
    pub fn needs_drop(self) -> bool {
        match self {
            FieldValueRepr::Undefined
            | FieldValueRepr::Null
            | FieldValueRepr::Int
            | FieldValueRepr::Float
            | FieldValueRepr::FieldReference
            | FieldValueRepr::SlicedFieldReference
            | FieldValueRepr::StreamValueId
            | FieldValueRepr::TextInline
            | FieldValueRepr::BytesInline => false,
            FieldValueRepr::BigInt
            | FieldValueRepr::Rational
            | FieldValueRepr::Error
            | FieldValueRepr::TextBuffer
            | FieldValueRepr::TextFile
            | FieldValueRepr::BytesBuffer
            | FieldValueRepr::BytesFile
            | FieldValueRepr::Object
            | FieldValueRepr::Array
            | FieldValueRepr::Custom => true,
        }
    }
    #[inline(always)]
    pub fn needs_alignment(self) -> bool {
        !matches!(
            self,
            FieldValueRepr::Undefined
                | FieldValueRepr::Null
                | FieldValueRepr::BytesInline
        )
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
            FieldValueRepr::Undefined => 0,
            FieldValueRepr::Null => 0,
            FieldValueRepr::Int => size_of::<i64>(),
            FieldValueRepr::BigInt => size_of::<BigInt>(),
            FieldValueRepr::Float => size_of::<f64>(),
            FieldValueRepr::Rational => size_of::<BigRational>(),
            FieldValueRepr::StreamValueId => size_of::<StreamValueId>(),
            FieldValueRepr::FieldReference => size_of::<FieldReference>(),
            FieldValueRepr::SlicedFieldReference => {
                size_of::<SlicedFieldReference>()
            }
            FieldValueRepr::Error => size_of::<OperatorApplicationError>(),
            FieldValueRepr::TextBuffer => size_of::<String>(),
            FieldValueRepr::TextFile => size_of::<TextBufferFile>(),
            FieldValueRepr::BytesBuffer => size_of::<Vec<u8>>(),
            FieldValueRepr::BytesFile => size_of::<BytesBufferFile>(),
            FieldValueRepr::Object => size_of::<Object>(),
            FieldValueRepr::Array => size_of::<Array>(),
            FieldValueRepr::Custom => size_of::<CustomDataBox>(),
            // should not be used for size calculations
            // but is used for example in is_zst
            FieldValueRepr::TextInline | FieldValueRepr::BytesInline => {
                usize::MAX
            }
        }
    }
    pub fn is_variable_sized_type(self) -> bool {
        self == FieldValueRepr::BytesInline
            || self == FieldValueRepr::TextInline
    }
    pub fn is_zst(self) -> bool {
        self.size() == 0
    }
    pub fn is_fixed_size_type(self) -> bool {
        !self.is_variable_sized_type() && !self.is_zst()
    }
    pub unsafe fn from_u8(v: u8) -> FieldValueRepr {
        unsafe { std::mem::transmute(v) }
    }
    pub const fn to_str(&self) -> &'static str {
        match self {
            FieldValueRepr::Undefined => "undefined",
            FieldValueRepr::Null => "null",
            FieldValueRepr::Int => "int",
            FieldValueRepr::BigInt => "integer",
            FieldValueRepr::Float => "float",
            FieldValueRepr::Rational => "rational",
            FieldValueRepr::StreamValueId => "stream_value_id",
            FieldValueRepr::FieldReference => "field_reference",
            FieldValueRepr::SlicedFieldReference => "sliced_field_reference",
            FieldValueRepr::Error => "error",
            FieldValueRepr::TextInline => "text",
            FieldValueRepr::TextBuffer => "text",
            FieldValueRepr::TextFile => "text",
            FieldValueRepr::BytesInline => "bytes",
            FieldValueRepr::BytesBuffer => "bytes",
            FieldValueRepr::BytesFile => "bytes",
            FieldValueRepr::Object => "object",
            FieldValueRepr::Array => "array",
            FieldValueRepr::Custom => "custom",
        }
    }
    pub fn to_format(&self) -> FieldValueFormat {
        FieldValueFormat {
            repr: *self,
            flags: field_value_flags::DEFAULT,
            size: if self.is_variable_sized_type() {
                0
            } else {
                self.size() as FieldValueSize
            },
        }
    }
}

impl Display for FieldValueRepr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.to_str())
    }
}

impl Default for FieldValueFormat {
    fn default() -> Self {
        Self {
            repr: FieldValueRepr::Null,
            flags: field_value_flags::DEFAULT,
            size: 0,
        }
    }
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
    pub fn effective_run_length(&self) -> RunLength {
        if self.deleted() {
            0
        } else {
            self.run_length
        }
    }
}

impl Clone for FieldData {
    fn clone(&self) -> Self {
        let mut fd = Self {
            data: FieldDataBuffer::with_capacity(self.data.len()),
            headers: Vec::with_capacity(self.headers.len()),
            field_count: 0, // set by copy
        };
        let fd_ref = &mut fd;
        let mut iter = self.iter();
        FieldData::copy(&mut iter, &mut |f| f(fd_ref));
        fd
    }
}

impl FieldData {
    pub unsafe fn from_raw_parts(
        header: Vec<FieldValueHeader>,
        data: FieldDataBuffer,
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
                let kind = range.data.repr();
                let len = range.data.len();
                TypedSlice::drop_from_kind(slice_start_ptr, len, kind);
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
    pub fn append_data_to(&self, target: &mut FieldDataBuffer) {
        let mut iter = self.iter();
        while let Some(tr) =
            iter.typed_range_fwd(usize::MAX, field_value_flags::DEFAULT)
        {
            unsafe { append_data(tr.data, &mut |f| f(target)) };
        }
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
                    &mut FieldDataBuffer,
                )| {
                    targets_applicator(&mut |fd| f(&mut fd.data))
                })
            };
            fields_copied += tr.field_count;
        }
        fields_copied
    }
    pub fn append_from_other(&mut self, other: &FieldData) -> usize {
        let mut iter = other.iter();
        Self::copy(&mut iter, &mut |f| f(self))
    }
    pub fn append_from_iter<'a>(
        &mut self,
        iter: &mut impl FieldIterator<'a>,
    ) -> usize {
        Self::copy(iter, &mut |f| f(self))
    }
    pub fn copy<'a>(
        iter: &mut impl FieldIterator<'a>,
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
                if tr.headers[0].repr.needs_alignment() {
                    let align = unsafe { fd.pad_to_align() };
                    fd.headers[first_header_idx].set_leading_padding(align);
                }
                fd.headers[first_header_idx].run_length -=
                    tr.first_header_run_length_oversize;
            });
            unsafe {
                append_data(tr.data, &mut |f: &mut dyn Fn(
                    &mut FieldDataBuffer,
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
            field_value_flags::DELETED,
        ) {
            copied_fields += tr.base.field_count;
            if tr.refs.is_none() {
                targets_applicator(&mut |fd| {
                    let first_header_idx = fd.headers.len();
                    fd.headers.extend_from_slice(tr.base.headers);
                    if tr.base.headers[0].repr.needs_alignment() {
                        let align = unsafe { fd.pad_to_align() };
                        fd.headers[first_header_idx]
                            .set_leading_padding(align);
                    }
                    fd.headers[first_header_idx].run_length -=
                        tr.base.first_header_run_length_oversize;
                });
                unsafe {
                    append_data(tr.base.data, &mut |f: &mut dyn Fn(
                        &mut FieldDataBuffer,
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
                                // PERF: maybe do a little rle here?
                                fd.headers.push(FieldValueHeader {
                                    fmt: FieldValueFormat {
                                        repr: FieldValueRepr::BytesInline,
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
                                // PERF: maybe do a little rle here?
                                fd.headers.push(FieldValueHeader {
                                    fmt: FieldValueFormat {
                                        repr: FieldValueRepr::TextInline,
                                        flags: SHARED_VALUE,
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
                    TypedSlice::TextBuffer(data) => {
                        for (v, rl, _offset) in
                            RefAwareTextBufferIter::from_range(&tr, data)
                        {
                            targets_applicator(&mut |fd| {
                                fd.push_str(v, rl as usize, true, false);
                            });
                        }
                    }
                    // we currently only use field refs for text/bytes types
                    // so these never happen
                    // we might reconsider this for Object / Custom though
                    TypedSlice::Undefined(_)
                    | TypedSlice::Null(_)
                    | TypedSlice::Int(_)
                    | TypedSlice::BigInt(_)
                    | TypedSlice::Float(_)
                    | TypedSlice::Rational(_)
                    | TypedSlice::StreamValueId(_)
                    | TypedSlice::FieldReference(_)
                    | TypedSlice::SlicedFieldReference(_)
                    | TypedSlice::Error(_)
                    | TypedSlice::Custom(_)
                    | TypedSlice::Object(_)
                    | TypedSlice::Array(_) => unreachable!(),
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
    target_applicator: &mut impl FnMut(&mut dyn Fn(&mut FieldDataBuffer)),
    src: &[T],
) {
    let src_size = size_of_val(src);
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
unsafe fn extend_with_custom_clones(
    target_applicator: &mut impl FnMut(&mut dyn Fn(&mut FieldDataBuffer)),
    src: &[CustomDataBox],
) {
    let src_size = size_of::<CustomDataBox>();
    target_applicator(&mut |data| {
        data.reserve(src_size);
        unsafe {
            let mut tgt = data.as_mut_ptr_range().end as *mut CustomDataBox;
            for v in src {
                std::ptr::write(tgt, v.clone_dyn());
                tgt = tgt.add(1);
            }
            data.set_len(data.len() + src_size);
        }
    });
}

#[inline(always)]
unsafe fn extend_raw<T: Sized + Copy>(
    target_applicator: &mut impl FnMut(&mut dyn Fn(&mut FieldDataBuffer)),
    src: &[T],
) {
    let src_bytes = unsafe {
        std::slice::from_raw_parts(src.as_ptr() as *const u8, size_of_val(src))
    };
    target_applicator(&mut |data| data.extend_from_slice(src_bytes));
}

#[inline(always)]
unsafe fn append_data(
    ts: TypedSlice<'_>,
    target_applicator: &mut impl FnMut(&mut dyn Fn(&mut FieldDataBuffer)),
) {
    unsafe {
        match ts {
            TypedSlice::Null(_) | TypedSlice::Undefined(_) => (),
            TypedSlice::Int(v) => extend_raw(target_applicator, v),
            TypedSlice::BigInt(v) => extend_with_clones(target_applicator, v),
            TypedSlice::Float(v) => extend_raw(target_applicator, v),
            TypedSlice::Rational(v) => {
                extend_with_clones(target_applicator, v)
            }
            TypedSlice::StreamValueId(v) => extend_raw(target_applicator, v),
            TypedSlice::FieldReference(v) => extend_raw(target_applicator, v),
            TypedSlice::SlicedFieldReference(v) => {
                extend_raw(target_applicator, v)
            }
            TypedSlice::BytesInline(v) => extend_raw(target_applicator, v),
            TypedSlice::TextInline(v) => {
                extend_raw(target_applicator, v.as_bytes())
            }
            TypedSlice::TextBuffer(v) => {
                extend_with_clones(target_applicator, v)
            }
            TypedSlice::BytesBuffer(v) => {
                extend_with_clones(target_applicator, v)
            }
            TypedSlice::Error(v) => extend_with_clones(target_applicator, v),
            TypedSlice::Object(v) => extend_with_clones(target_applicator, v),
            TypedSlice::Array(v) => extend_with_clones(target_applicator, v),
            TypedSlice::Custom(v) => {
                extend_with_custom_clones(target_applicator, v)
            }
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
