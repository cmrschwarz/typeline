use std::{borrow::Cow, ops::Deref};

use super::{
    array::Array,
    field_value::{FieldValue, FieldValueUnboxed, Object},
    field_value_ref::FieldValueRef,
};
use num::{BigInt, FromPrimitive};
use serde::{
    de::{
        value::{MapDeserializer, SeqDeserializer},
        IntoDeserializer, Visitor,
    },
    forward_to_deserialize_any,
    ser::{SerializeMap, SerializeSeq},
    Deserialize, Deserializer, Serialize, Serializer,
};

/// A Cow'ed Str, but the borrowed variant is serde deserializable
pub enum DeCowStr<'de> {
    Borrowed(&'de str),
    Owned(String),
}
pub struct DeCowStrVisitor;

impl<'de> From<DeCowStr<'de>> for Cow<'de, str> {
    fn from(v: DeCowStr<'de>) -> Self {
        match v {
            DeCowStr::Borrowed(v) => Cow::Borrowed(v),
            DeCowStr::Owned(v) => Cow::Owned(v),
        }
    }
}
impl<'de> From<Cow<'de, str>> for DeCowStr<'de> {
    fn from(v: Cow<'de, str>) -> Self {
        match v {
            Cow::Borrowed(v) => DeCowStr::Borrowed(v),
            Cow::Owned(v) => DeCowStr::Owned(v),
        }
    }
}

impl<'de> Visitor<'de> for DeCowStrVisitor {
    type Value = DeCowStr<'de>;

    fn expecting(
        &self,
        formatter: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        formatter.write_str("string")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(DeCowStr::Owned(v.to_owned())) // lifetime is not 'de
    }

    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(DeCowStr::Borrowed(v))
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(DeCowStr::Owned(v))
    }
}
impl<'de> Deserialize<'de> for DeCowStr<'de> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(DeCowStrVisitor)
    }
}
impl Deref for DeCowStr<'_> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        match self {
            DeCowStr::Borrowed(v) => v,
            DeCowStr::Owned(v) => v,
        }
    }
}

struct FieldValueUnboxedVisitor {}
impl<'de> Visitor<'de> for FieldValueUnboxedVisitor {
    type Value = FieldValueUnboxed;

    fn expecting(
        &self,
        formatter: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        formatter.write_str("field_value")
    }

    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(FieldValueUnboxed::Int(i64::from(v))) // TODO: bool support
    }

    fn visit_i8<E>(self, v: i8) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_i64(i64::from(v))
    }

    fn visit_i16<E>(self, v: i16) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_i64(i64::from(v))
    }

    fn visit_i32<E>(self, v: i32) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_i64(i64::from(v))
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(FieldValueUnboxed::Int(v))
    }

    fn visit_i128<E>(self, v: i128) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(FieldValueUnboxed::BigInt(BigInt::from_i128(v).unwrap()))
    }

    fn visit_u8<E>(self, v: u8) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_u64(u64::from(v))
    }

    fn visit_u16<E>(self, v: u16) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_u64(u64::from(v))
    }

    fn visit_u32<E>(self, v: u32) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_u64(u64::from(v))
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if v < i64::MAX as u64 {
            Ok(FieldValueUnboxed::Int(v as i64))
        } else {
            Ok(FieldValueUnboxed::BigInt(BigInt::from_u64(v).unwrap()))
        }
    }

    fn visit_u128<E>(self, v: u128) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(FieldValueUnboxed::BigInt(BigInt::from_u128(v).unwrap()))
    }

    fn visit_f32<E>(self, v: f32) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_f64(f64::from(v))
    }

    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(FieldValueUnboxed::Float(v))
    }

    fn visit_char<E>(self, v: char) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_str(v.encode_utf8(&mut [0u8; 4]))
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(FieldValueUnboxed::Text(v.to_string()))
    }

    fn visit_borrowed_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_str(v)
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(FieldValueUnboxed::Text(v))
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(FieldValueUnboxed::Bytes(v.to_owned()))
    }

    fn visit_borrowed_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_bytes(v)
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(FieldValueUnboxed::Bytes(v))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(FieldValueUnboxed::Null)
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let _ = deserializer;
        Err(serde::de::Error::invalid_type(
            serde::de::Unexpected::Option,
            &self,
        ))
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Err(serde::de::Error::invalid_type(
            serde::de::Unexpected::Unit,
            &self,
        ))
    }

    fn visit_newtype_struct<D>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let _ = deserializer;
        Err(serde::de::Error::invalid_type(
            serde::de::Unexpected::NewtypeStruct,
            &self,
        ))
    }

    fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        Ok(FieldValueUnboxed::Array(ArrayVisitor.visit_seq(seq)?))
    }

    fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        Ok(FieldValueUnboxed::Object(ObjectVisitor.visit_map(map)?))
    }

    fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::EnumAccess<'de>,
    {
        let _ = data;
        Err(serde::de::Error::invalid_type(
            serde::de::Unexpected::Enum,
            &self,
        ))
    }
}

pub struct ArrayVisitor;
impl<'de> Visitor<'de> for ArrayVisitor {
    type Value = Array;

    fn expecting(
        &self,
        formatter: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        formatter.write_str("array")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let mut arr = Array::default();
        while let Some(value) = seq.next_element::<FieldValueUnboxed>()? {
            arr.push_unboxed(value)
        }
        Ok(arr)
    }
}

pub struct ObjectVisitor;
impl<'de> Visitor<'de> for ObjectVisitor {
    type Value = Object;

    fn expecting(
        &self,
        formatter: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        formatter.write_str("array")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let mut obj = Object::default();
        while let Some((key, value)) = map.next_entry()? {
            obj.push_stored_key(key, value);
        }
        Ok(obj)
    }
}

impl<'de> Deserialize<'de> for FieldValueUnboxed {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(FieldValueUnboxedVisitor {})
    }
}

impl<'de> Deserialize<'de> for FieldValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(FieldValue::from(
            deserializer.deserialize_any(FieldValueUnboxedVisitor {})?,
        ))
    }
}

impl Serialize for Array {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.len()))?;
        for value in self.ref_iter() {
            seq.serialize_element(&value)?;
        }
        seq.end()
    }
}

impl<'de> Deserializer<'de> for &'de Array {
    type Error = serde::de::value::Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let de = SeqDeserializer::new(self.ref_iter());
        visitor.visit_seq(de)
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map struct enum identifier ignored_any
    }
}

impl Serialize for Object {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.len()))?;
        match self {
            Object::KeysStored(index_map) => {
                for (key, value) in index_map {
                    map.serialize_key(key)?;
                    map.serialize_value(&value)?;
                }
            }
            Object::KeysInterned(_) => {
                todo!() // TODO: remove this thing entirely
            }
        }
        map.end()
    }
}

impl Serialize for FieldValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.as_ref().serialize(serializer)
    }
}

impl Serialize for FieldValueUnboxed {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.as_ref().serialize(serializer)
    }
}

impl Serialize for FieldValueRef<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            FieldValueRef::Int(v) => v.serialize(serializer),
            FieldValueRef::BigInt(v) => v.serialize(serializer),
            FieldValueRef::Float(v) => v.serialize(serializer),
            FieldValueRef::Text(v) => v.serialize(serializer),
            FieldValueRef::Bytes(v) => v.serialize(serializer),
            FieldValueRef::Array(v) => v.serialize(serializer),
            FieldValueRef::Object(v) => v.serialize(serializer),
            FieldValueRef::Null => serializer.serialize_none(),
            FieldValueRef::Undefined => serializer.serialize_unit(),
            FieldValueRef::BigRational(ratio) => ratio.serialize(serializer),
            FieldValueRef::Custom(_) => todo!(),
            FieldValueRef::StreamValueId(_) => todo!(),
            FieldValueRef::Error(_) => todo!(),
            FieldValueRef::Argument(_) => todo!(),
            FieldValueRef::OpDecl(_) => todo!(),
            FieldValueRef::FieldReference(_) => todo!(),
            FieldValueRef::SlicedFieldReference(_) => {
                todo!()
            }
        }
    }
}

impl<'de> Deserializer<'de> for FieldValueRef<'de> {
    type Error = serde::de::value::Error;

    fn deserialize_any<V>(
        self,
        visitor: V,
    ) -> Result<V::Value, <Self as Deserializer<'de>>::Error>
    where
        V: Visitor<'de>,
    {
        match self {
            FieldValueRef::Int(v) => visitor.visit_i64(*v),
            FieldValueRef::BigInt(_) => todo!(),
            FieldValueRef::Float(v) => visitor.visit_f64(*v),
            FieldValueRef::Text(v) => visitor.visit_str(v),
            FieldValueRef::Bytes(v) => visitor.visit_bytes(v),
            FieldValueRef::Array(v) => {
                visitor.visit_seq(SeqDeserializer::new(v.ref_iter()))
            }
            FieldValueRef::Object(v) => {
                let Object::KeysStored(v) = v else { todo!() };
                visitor.visit_map(MapDeserializer::new(
                    v.iter().map(|(k, v)| (&**k, v.as_ref())),
                ))
            }
            FieldValueRef::Null => visitor.visit_none(),
            FieldValueRef::Undefined => visitor.visit_unit(),
            FieldValueRef::BigRational(_) => todo!(),
            FieldValueRef::Custom(_) => todo!(),
            FieldValueRef::StreamValueId(_) => todo!(),
            FieldValueRef::Error(_) => todo!(),
            FieldValueRef::Argument(_) => todo!(),
            FieldValueRef::OpDecl(_) => todo!(),
            FieldValueRef::FieldReference(_) => todo!(),
            FieldValueRef::SlicedFieldReference(_) => {
                todo!()
            }
        }
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map struct enum identifier ignored_any
    }
}
impl<'de> IntoDeserializer<'de> for FieldValueRef<'de> {
    type Deserializer = Self;

    fn into_deserializer(self) -> Self::Deserializer {
        self
    }
}
