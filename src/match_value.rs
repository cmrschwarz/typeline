use indexmap::IndexMap;

use crate::match_value_into_iter::MatchValueIntoIter;
use crate::operations::operator_base::OperatorApplicationError;
use crate::string_store::StringStoreEntry;
use std::mem::transmute;
use std::ops::Deref;
use std::{
    mem::{size_of, ManuallyDrop},
    rc::Rc,
    sync::Arc,
};

//TODO: put this into separate module
pub struct Html {}

#[derive(Clone, Copy, PartialEq)]
pub enum MatchValueRepresentation {
    Local,
    Shared,
}

pub type MatchValueIndex = usize;

//PERF: Arc :(
pub enum MatchValueKind {
    Complex,
    Bytes,
    Text,
    Error,
    Html,
    Integer,
    Null,
    TypedArray(Arc<MatchValueFormat>),
    Array(Arc<[MatchValueFormat]>),
    Object(Arc<IndexMap<StringStoreEntry, MatchValueFormat>>),
}

pub struct MatchValueFormat {
    pub kind: MatchValueKind,
    pub repr: MatchValueRepresentation,
    pub stream: bool,
}

pub trait MatchValueType {
    type ValueType: ?Sized;
    type StorageTypeLocal: Deref<Target = Self::ValueType> + 'static;
    type StorageTypeShared: Deref<Target = Self::ValueType> + 'static;
    const KIND: MatchValueKind;
    #[inline(always)]
    unsafe fn storage_local(val: &MatchValue) -> &ManuallyDrop<Self::StorageTypeLocal> {
        transmute(val)
    }
    #[inline(always)]
    unsafe fn storage_shared(val: &MatchValue) -> &ManuallyDrop<Self::StorageTypeShared> {
        transmute(val)
    }
    #[inline(always)]
    unsafe fn storage_local_mut(val: &mut MatchValue) -> &mut ManuallyDrop<Self::StorageTypeLocal> {
        transmute(val)
    }
    #[inline(always)]
    unsafe fn storage_shared_mut(
        val: &mut MatchValue,
    ) -> &mut ManuallyDrop<Self::StorageTypeShared> {
        transmute(val)
    }
    #[inline(always)]
    unsafe fn value_local(val: &MatchValue) -> &Self::ValueType {
        &*Self::storage_local(val)
    }
    #[inline(always)]
    unsafe fn value_shared(val: &MatchValue) -> &Self::ValueType {
        &*Self::storage_shared(val)
    }
    #[inline(always)]
    unsafe fn value(repr: MatchValueRepresentation, val: &MatchValue) -> &Self::ValueType {
        match repr {
            MatchValueRepresentation::Local => Self::value_local(val),
            MatchValueRepresentation::Shared => Self::value_shared(val),
        }
    }
}
#[repr(C)]
pub struct TypedArray<MV: MatchValueAny> {
    elements_format: MatchValueFormat,
    values: ManuallyDrop<Vec<MV>>,
}
impl<MV: MatchValueAny> MatchValueType for TypedArray<MV> {
    type ValueType = Self;
    type StorageTypeLocal = Rc<Self>;
    type StorageTypeShared = Arc<Self>;
    const KIND: MatchValueKind = MatchValueKind::Complex;
}
impl<MV: MatchValueAny> Drop for TypedArray<MV> {
    fn drop(&mut self) {
        let _ = MatchValueIntoIter::new(
            &self.elements_format,
            unsafe { ManuallyDrop::take(&mut self.values) }.into_iter(),
        );
    }
}

#[repr(C)]
pub struct Array<MV: MatchValueAny> {
    element_formats: Arc<Vec<MatchValueFormat>>,
    values: ManuallyDrop<Vec<MV>>,
}
impl<MV: MatchValueAny> MatchValueType for Array<MV> {
    type ValueType = Self;
    type StorageTypeLocal = Rc<Self>;
    type StorageTypeShared = Arc<Self>;
    const KIND: MatchValueKind = MatchValueKind::Complex;
}
impl<MV: MatchValueAny> Drop for Array<MV> {
    fn drop(&mut self) {
        for (i, value) in unsafe { ManuallyDrop::take(&mut self.values) }
            .into_iter()
            .enumerate()
        {
            let _ = MatchValueIntoIter::new(&self.element_formats[i], std::iter::once(value));
        }
    }
}

#[repr(C)]
pub struct Object<MV: MatchValueAny> {
    element_formats: Arc<IndexMap<StringStoreEntry, MatchValueFormat>>,
    values: Vec<ManuallyDrop<MV>>,
}
impl<MV: MatchValueAny> MatchValueType for Object<MV> {
    type ValueType = Self;
    type StorageTypeLocal = Rc<Self>;
    type StorageTypeShared = Arc<Self>;
    const KIND: MatchValueKind = MatchValueKind::Complex;
}
impl<MV: MatchValueAny> Drop for Object<MV> {
    fn drop(&mut self) {
        for (i, shape) in self.element_formats.values().enumerate() {
            let _ = MatchValueIntoIter::new(
                &shape,
                std::iter::once(unsafe { ManuallyDrop::take(&mut self.values[i]) }),
            );
        }
    }
}

impl MatchValueType for Html {
    type ValueType = Html;
    type StorageTypeLocal = Rc<Html>;
    type StorageTypeShared = Arc<Html>;
    const KIND: MatchValueKind = MatchValueKind::Html;
}

pub struct Bytes;
impl MatchValueType for Bytes {
    type ValueType = [u8];
    type StorageTypeLocal = Rc<[u8]>;
    type StorageTypeShared = Arc<[u8]>;
    const KIND: MatchValueKind = MatchValueKind::Bytes;
}

pub struct Text;
impl MatchValueType for Text {
    type ValueType = str;
    type StorageTypeLocal = Rc<str>;
    type StorageTypeShared = Arc<str>;
    const KIND: MatchValueKind = MatchValueKind::Text;
}
#[repr(transparent)]
pub struct SelfDeref<T> {
    value: T,
}
impl<T> Deref for SelfDeref<T> {
    type Target = T;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}
pub struct Integer();
impl MatchValueType for Integer {
    type ValueType = i64;
    type StorageTypeLocal = SelfDeref<i64>;
    type StorageTypeShared = SelfDeref<i64>;
    const KIND: MatchValueKind = MatchValueKind::Integer;
}

pub struct Error();
impl MatchValueType for Error {
    type ValueType = OperatorApplicationError;
    type StorageTypeLocal = SelfDeref<OperatorApplicationError>;
    type StorageTypeShared = SelfDeref<OperatorApplicationError>;
    const KIND: MatchValueKind = MatchValueKind::Error;
}

pub unsafe trait MatchValueAny: 'static + Sized {
    fn as_match_value(&self) -> &MatchValue {
        unsafe { transmute(self) }
    }

    fn as_match_value_mut(&mut self) -> &mut MatchValue {
        unsafe { transmute(self) }
    }
}

#[repr(C)]
pub union MatchValueShared {
    typed_array: ManuallyDrop<<TypedArray<MatchValueShared> as MatchValueType>::StorageTypeShared>,
    array: ManuallyDrop<<Array<MatchValueShared> as MatchValueType>::StorageTypeShared>,
    object: ManuallyDrop<<Object<MatchValueShared> as MatchValueType>::StorageTypeShared>,
    html: ManuallyDrop<<Html as MatchValueType>::StorageTypeShared>,
    bytes: ManuallyDrop<<Bytes as MatchValueType>::StorageTypeShared>,
    text: ManuallyDrop<<Text as MatchValueType>::StorageTypeShared>,
    integer: ManuallyDrop<<Integer as MatchValueType>::StorageTypeShared>,
    error: ManuallyDrop<<Error as MatchValueType>::StorageTypeShared>,
    ensure_size: ManuallyDrop<[u8; size_of::<MatchValueLocal>()]>,
}
unsafe impl MatchValueAny for MatchValueShared {}

#[repr(C)]
pub union MatchValueLocal {
    typed_array: ManuallyDrop<<TypedArray<MatchValue> as MatchValueType>::StorageTypeLocal>,
    array: ManuallyDrop<<Array<MatchValue> as MatchValueType>::StorageTypeLocal>,
    object: ManuallyDrop<<Object<MatchValue> as MatchValueType>::StorageTypeLocal>,
    html: ManuallyDrop<<Html as MatchValueType>::StorageTypeLocal>,
    bytes: ManuallyDrop<<Bytes as MatchValueType>::StorageTypeLocal>,
    text: ManuallyDrop<<Text as MatchValueType>::StorageTypeLocal>,
    integer: ManuallyDrop<<Integer as MatchValueType>::StorageTypeLocal>,
    error: ManuallyDrop<<Error as MatchValueType>::StorageTypeLocal>,
}
unsafe impl MatchValueAny for MatchValueLocal {}

#[repr(C)]
pub union MatchValue {
    shared: ManuallyDrop<MatchValueShared>,
    local: ManuallyDrop<MatchValueLocal>,
}
unsafe impl MatchValueAny for MatchValue {}

impl Drop for MatchValue {
    fn drop(&mut self) {
        panic!("MatchValue union cannot be dropped");
    }
}

impl Drop for MatchValueLocal {
    fn drop(&mut self) {
        panic!("MatchValueLocal union cannot be dropped");
    }
}

impl Drop for MatchValueShared {
    fn drop(&mut self) {
        panic!("MatchValueShared union cannot be dropped");
    }
}
