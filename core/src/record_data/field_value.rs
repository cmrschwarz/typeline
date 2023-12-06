use indexmap::IndexMap;

use crate::{
    operators::errors::OperatorApplicationError,
    utils::string_store::StringStoreEntry,
};

use super::{
    custom_data::CustomDataBox, field::FieldIdOffset,
    field_data::FieldDataRepr,
};

// the different logical data types
// irrespective of representation in memory, see FieldDataRepr for that
#[derive(PartialEq, Eq, Clone, Copy)]
pub enum FieldValueKind {
    Undefined,
    Null,
    Integer,
    Error,
    Bytes,
    Text,
    Object,
    Custom,
}

#[derive(Debug, Clone)]
pub enum FieldValue {
    Null,
    Undefined,
    Int(i64),
    Bytes(Vec<u8>),
    String(String),
    Error(OperatorApplicationError),
    Array(Array),
    Object(Object),
    FieldReference(FieldReference),
    Custom(CustomDataBox),
}

pub struct Null;
pub struct Undefined;

#[derive(Debug, Clone, PartialEq)]
pub enum Object {
    KeysStored(Box<IndexMap<Box<str>, FieldValue>>),
    KeysInterned(Box<IndexMap<StringStoreEntry, FieldValue>>),
}

#[derive(Debug, PartialEq, Clone)]
pub enum Array {
    Null(usize),
    Undefined(usize),
    Int(Box<[i64]>),
    Bytes(Box<Box<[u8]>>),
    String(Box<Box<str>>),
    Error(Box<[OperatorApplicationError]>),
    Array(Box<[Array]>),
    Object(Box<[Object]>),
    FieldReference(Box<[FieldReference]>),
    Custom(Box<[CustomDataBox]>),
    Mixed(Box<[FieldValue]>),
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct FieldReference {
    pub field_id_offset: FieldIdOffset,
    pub begin: usize,
    pub end: usize,
}

impl FieldValueKind {
    pub fn to_preferred_data_repr(self) -> FieldDataRepr {
        match self {
            FieldValueKind::Undefined => FieldDataRepr::Undefined,
            FieldValueKind::Null => FieldDataRepr::Null,
            FieldValueKind::Integer => FieldDataRepr::Integer,
            FieldValueKind::Error => FieldDataRepr::Error,
            FieldValueKind::Bytes => FieldDataRepr::BytesInline,
            FieldValueKind::Text => FieldDataRepr::BytesInline,
            FieldValueKind::Object => FieldDataRepr::Object,
            FieldValueKind::Custom => FieldDataRepr::Custom,
        }
    }
    pub fn to_str(self) -> &'static str {
        match self {
            FieldValueKind::Undefined => "undefined",
            FieldValueKind::Null => "null",
            FieldValueKind::Integer => "int",
            FieldValueKind::Error => "error",
            FieldValueKind::Text => "str",
            FieldValueKind::Bytes => "bytes",
            FieldValueKind::Custom => "custom",
            FieldValueKind::Object => "object",
        }
    }
}

impl PartialEq for FieldValue {
    fn eq(&self, other: &Self) -> bool {
        match self {
            Self::Int(l) => matches!(other, Self::Int(r) if r == l),
            Self::Bytes(l) => matches!(other, Self::Bytes(r) if r == l),
            Self::String(l) => matches!(other, Self::String(r) if r == l),
            Self::Error(l) => matches!(other, Self::Error(r) if r == l),
            Self::Array(l) => matches!(other, Self::Array(r) if r == l),
            Self::Object(l) => matches!(other, Self::Object(r) if r == l),
            Self::FieldReference(l) => {
                matches!(other, Self::FieldReference(r) if r == l)
            }
            Self::Custom(l) => matches!(other, Self::Custom(r) if r == l),
            Self::Null => other == &Self::Null,
            Self::Undefined => other == &Self::Undefined,
        }
    }
}
