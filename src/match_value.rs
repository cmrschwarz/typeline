use crate::operations::operator_base::OperatorApplicationError;
use std::mem::transmute;
use std::ops::Deref;
use std::{
    mem::{size_of, ManuallyDrop},
    rc::Rc,
    sync::Arc,
};

//TODO: put this into separate module
pub struct HtmlMatchData {}

#[derive(Clone, Copy, PartialEq)]
pub enum MatchValueKind {
    TypedArray,
    Array,
    Object,
    Bytes,
    Text,
    Error,
    Html,
    Integer,
    Null,
}

impl MatchValueKind {
    pub fn to_str(&self) -> &'static str {
        match self {
            MatchValueKind::TypedArray => "array",
            MatchValueKind::Array => "array",
            MatchValueKind::Object => "object",
            MatchValueKind::Bytes => "bytes",
            MatchValueKind::Error => "error",
            MatchValueKind::Html => "html",
            MatchValueKind::Null => "null",
            MatchValueKind::Text => "text",
            MatchValueKind::Integer => "int",
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
pub enum MatchValueRepresentation {
    Local,
    Shared,
}

#[derive(Clone, Copy, PartialEq)]
pub struct MatchValueFormat {
    pub kind: MatchValueKind,
    pub repr: MatchValueRepresentation,
    pub stream: bool,
}

impl MatchValueFormat {
    pub fn is_valid(self) -> bool {
        match self.kind {
            MatchValueKind::Bytes | MatchValueKind::Text | MatchValueKind::TypedArray => true,
            MatchValueKind::Array | MatchValueKind::Object => !self.stream,
            MatchValueKind::Html
            | MatchValueKind::Error
            | MatchValueKind::Null
            | MatchValueKind::Integer => {
                !self.stream && (self.repr == MatchValueRepresentation::Local)
            }
        }
    }
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
}

pub struct TypedArray();
impl MatchValueType for TypedArray {
    type ValueType = Vec<MatchValueShared>;
    type StorageTypeLocal = Rc<Vec<MatchValueShared>>;
    type StorageTypeShared = Arc<Vec<MatchValueShared>>;
    const KIND: MatchValueKind = MatchValueKind::TypedArray;
}

pub struct Array();
impl MatchValueType for Array {
    type ValueType = [MatchValueShared];
    type StorageTypeLocal = Rc<[MatchValueShared]>;
    type StorageTypeShared = Arc<[MatchValueShared]>;
    const KIND: MatchValueKind = MatchValueKind::Array;
}

pub struct Object();
impl MatchValueType for Object {
    type ValueType = [MatchValueShared];
    type StorageTypeLocal = Rc<[MatchValueShared]>;
    type StorageTypeShared = Arc<[MatchValueShared]>;
    const KIND: MatchValueKind = MatchValueKind::Object;
}

pub struct Html();
impl MatchValueType for Html {
    type ValueType = HtmlMatchData;
    type StorageTypeLocal = Rc<HtmlMatchData>;
    type StorageTypeShared = Arc<HtmlMatchData>;
    const KIND: MatchValueKind = MatchValueKind::Html;
}

pub struct Bytes();
impl MatchValueType for Bytes {
    type ValueType = [u8];
    type StorageTypeLocal = Rc<[u8]>;
    type StorageTypeShared = Arc<[u8]>;
    const KIND: MatchValueKind = MatchValueKind::Bytes;
}

pub struct Text();
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

#[repr(C)]
pub union MatchValueShared {
    typed_array: ManuallyDrop<<TypedArray as MatchValueType>::StorageTypeShared>,
    array: ManuallyDrop<<Array as MatchValueType>::StorageTypeShared>,
    object: ManuallyDrop<<Object as MatchValueType>::StorageTypeShared>,
    html: ManuallyDrop<<Html as MatchValueType>::StorageTypeShared>,
    bytes: ManuallyDrop<<Bytes as MatchValueType>::StorageTypeShared>,
    text: ManuallyDrop<<Text as MatchValueType>::StorageTypeShared>,
    integer: ManuallyDrop<<Integer as MatchValueType>::StorageTypeShared>,
    error: ManuallyDrop<<Error as MatchValueType>::StorageTypeShared>,
    ensure_size: ManuallyDrop<[u8; size_of::<MatchValueLocal>()]>,
}

#[repr(C)]
pub union MatchValueLocal {
    typed_array: ManuallyDrop<<TypedArray as MatchValueType>::StorageTypeLocal>,
    array: ManuallyDrop<<Array as MatchValueType>::StorageTypeLocal>,
    object: ManuallyDrop<<Object as MatchValueType>::StorageTypeLocal>,
    html: ManuallyDrop<<Html as MatchValueType>::StorageTypeLocal>,
    bytes: ManuallyDrop<<Bytes as MatchValueType>::StorageTypeLocal>,
    text: ManuallyDrop<<Text as MatchValueType>::StorageTypeLocal>,
    integer: ManuallyDrop<<Integer as MatchValueType>::StorageTypeLocal>,
    error: ManuallyDrop<<Error as MatchValueType>::StorageTypeLocal>,
}

#[repr(C)]
pub union MatchValue {
    shared: ManuallyDrop<MatchValueShared>,
    local: ManuallyDrop<MatchValueLocal>,
}

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
