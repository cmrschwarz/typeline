use indexmap::IndexMap;

use std::ops::Deref;

use crate::match_value_into_iter::MatchValueIntoIter;
use crate::operations::OperatorApplicationError;
use crate::string_store::StringStoreEntry;
use crate::sync_variant::{self, SyncVariant, SyncVariantImpl};
use std::mem::transmute;
use std::mem::{size_of, ManuallyDrop};

pub type MatchValueIndex = usize;

//PERF: Arc :(
pub enum MatchValueKind {
    MatchValueIndex,
    Bytes,
    Text,
    Error,
    Html,
    Integer,
    Null,
    TypedArray,
    Array,
    Object,
}

pub struct MatchValueFormat {
    pub kind: MatchValueKind,
    pub repr: SyncVariantImpl,
    pub stream: bool,
}

pub trait MatchValueType {
    type ValueType: ?Sized + 'static;
    type StorageType<SV: SyncVariant>: Deref<Target = Self::ValueType> + 'static;
    const KIND: MatchValueKind;
    #[inline(always)]
    unsafe fn storage<SV: SyncVariant>(val: &impl MatchValueAnySync) -> &Self::StorageType<SV> {
        transmute(val)
    }
    unsafe fn storage_mut<SV: SyncVariant>(
        val: &mut impl MatchValueAnySync,
    ) -> &mut Self::StorageType<SV> {
        transmute(val)
    }
    #[inline(always)]
    unsafe fn storage_md<SV: SyncVariant>(
        val: &impl MatchValueAnySync,
    ) -> &ManuallyDrop<Self::StorageType<SV>> {
        transmute(val)
    }
    unsafe fn storage_md_mut<SV: SyncVariant>(
        val: &mut impl MatchValueAnySync,
    ) -> &mut ManuallyDrop<Self::StorageType<SV>> {
        transmute(val)
    }
    #[inline(always)]
    unsafe fn value<SV: SyncVariant>(val: &impl MatchValueAnySync) -> &Self::ValueType {
        Self::storage::<SV>(val).deref()
    }
    #[inline(always)]
    unsafe fn value_by_repr(
        repr: SyncVariantImpl,
        val: &impl MatchValueAnySync,
    ) -> &Self::ValueType {
        match repr {
            SyncVariantImpl::Unique => Self::value::<sync_variant::Unique>(val),
            SyncVariantImpl::Local => Self::value::<sync_variant::Local>(val),
            SyncVariantImpl::Shared => Self::value::<sync_variant::Shared>(val),
        }
    }
}

struct MatchValueIndexType;
impl MatchValueType for MatchValueIndexType {
    type ValueType = MatchValueIndex;

    type StorageType<SV: SyncVariant> = SelfDeref<MatchValueIndex>;

    const KIND: MatchValueKind = MatchValueKind::MatchValueIndex;
}

struct NullType;
impl MatchValueType for NullType {
    type ValueType = [u8; 0];

    type StorageType<SV: SyncVariant> = SelfDeref<Self::ValueType>;

    const KIND: MatchValueKind = MatchValueKind::Null;
}

#[repr(C)]
pub struct TypedArray {
    // TODO: we could apply a similar optimization than we have in match_set to this
    // so we don't need the union
    elements_format: MatchValueFormat,
    values: ManuallyDrop<Vec<MatchValue>>,
}
impl Drop for TypedArray {
    fn drop(&mut self) {
        let _ = MatchValueIntoIter::new(
            &self.elements_format,
            unsafe { ManuallyDrop::take(&mut self.values) }.into_iter(),
        );
    }
}
impl MatchValueType for TypedArray {
    type ValueType = Self;
    type StorageType<STV: SyncVariant> = STV::Type<Self>;
    const KIND: MatchValueKind = MatchValueKind::TypedArray;
}

#[repr(C)]
pub struct Array {
    element_formats: Vec<MatchValueFormat>,
    values: ManuallyDrop<Vec<MatchValue>>,
}
impl MatchValueType for Array {
    type ValueType = Array;
    type StorageType<STV: SyncVariant> = STV::Type<Self>;
    const KIND: MatchValueKind = MatchValueKind::Array;
}
impl Drop for Array {
    fn drop(&mut self) {
        for (i, value) in unsafe { ManuallyDrop::take(&mut self.values) }
            .into_iter()
            .enumerate()
        {
            let _ = MatchValueIntoIter::new(&self.element_formats[i], std::iter::once(value));
        }
    }
}

pub type ObjectLayout = IndexMap<StringStoreEntry, MatchValueFormat>;
#[repr(C)]
pub struct Object {
    layout: ObjectLayout,
    values: Vec<ManuallyDrop<MatchValue>>,
}
impl Drop for Object {
    fn drop(&mut self) {
        for (i, shape) in self.layout.values().enumerate() {
            let _ = MatchValueIntoIter::new(
                &shape,
                std::iter::once(unsafe { ManuallyDrop::take(&mut self.values[i]) }),
            );
        }
    }
}
impl MatchValueType for Object {
    type ValueType = Self;
    type StorageType<STV: SyncVariant> = STV::Type<Self>;
    const KIND: MatchValueKind = MatchValueKind::Object;
}

//TODO: put this into separate module
pub struct Html {}
pub struct HtmlType;
impl MatchValueType for HtmlType {
    type ValueType = Html;
    type StorageType<SV: SyncVariant> = SV::Type<Html>;
    const KIND: MatchValueKind = MatchValueKind::Html;
}

pub struct BytesType;
impl MatchValueType for BytesType {
    type ValueType = [u8];
    type StorageType<SV: SyncVariant> = SV::Type<[u8]>;
    const KIND: MatchValueKind = MatchValueKind::Bytes;
}

pub struct Text;
impl MatchValueType for Text {
    type ValueType = str;
    type StorageType<SV: SyncVariant> = SV::Type<str>;
    const KIND: MatchValueKind = MatchValueKind::Text;
}

#[repr(transparent)]
pub struct SelfDeref<T>(T);
impl<T> Deref for SelfDeref<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct Integer();
impl MatchValueType for Integer {
    type ValueType = i64;
    type StorageType<SV: SyncVariant> = SelfDeref<i64>;
    const KIND: MatchValueKind = MatchValueKind::Integer;
}

pub struct Error();
impl MatchValueType for Error {
    type ValueType = OperatorApplicationError;
    type StorageType<SV: SyncVariant> = SelfDeref<OperatorApplicationError>;
    const KIND: MatchValueKind = MatchValueKind::Error;
}

#[repr(C, align(8))]
pub union MatchValueBase<SV: SyncVariant> {
    typed_array: ManuallyDrop<<TypedArray as MatchValueType>::StorageType<SV>>,
    array: ManuallyDrop<<Array as MatchValueType>::StorageType<SV>>,
    object: ManuallyDrop<<Object as MatchValueType>::StorageType<SV>>,
    html: ManuallyDrop<<HtmlType as MatchValueType>::StorageType<SV>>,
    bytes: ManuallyDrop<<BytesType as MatchValueType>::StorageType<SV>>,
    text: ManuallyDrop<<Text as MatchValueType>::StorageType<SV>>,
    integer: ManuallyDrop<<Integer as MatchValueType>::StorageType<SV>>,
    error: ManuallyDrop<<Error as MatchValueType>::StorageType<SV>>,
}

#[repr(C, align(8))]
pub union MatchValue {
    unique: ManuallyDrop<MatchValueBase<sync_variant::Unique>>,
    local: ManuallyDrop<MatchValueBase<sync_variant::Local>>,
    shared: ManuallyDrop<MatchValueBase<sync_variant::Shared>>,
}

#[repr(C, align(8))]
pub union MatchValueTypeErased {
    ensure_size: ManuallyDrop<[u8; size_of::<MatchValue>()]>,
}

pub trait MatchValueAnySync {}
impl MatchValueAnySync for MatchValue {}
impl MatchValueAnySync for MatchValueTypeErased {}

impl Drop for MatchValue {
    fn drop(&mut self) {
        panic!("MatchValue union cannot be dropped");
    }
}

impl Drop for MatchValueTypeErased {
    fn drop(&mut self) {
        panic!("MatchValue union cannot be dropped");
    }
}
