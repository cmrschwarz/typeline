use std::cell::{RefCell, RefMut};

use indexmap::IndexMap;
use num::{BigInt, FromPrimitive};
use serde::{de::Visitor, Deserializer};
use typeline_core::{
    index_newtype,
    record_data::{
        field_data::{FieldData, FieldValueRepr},
        field_value::FieldValue,
        field_value_deserialize::{ArrayVisitor, DeCowStr, ObjectVisitor},
        push_interface::PushInterface,
        varying_type_inserter::VaryingTypeInserter,
    },
    utils::{
        debuggable_nonmax::DebuggableNonMaxUsize,
        index_vec::IndexVec,
        indexing_type::IndexingType,
        int_string_conversions::usize_to_str,
        stable_vec::StableVec,
        string_store::{StringStore, StringStoreEntry},
    },
};

index_newtype! {
    pub(super) struct InserterIndex(pub usize);
}

pub(super) struct PendingField {
    pub name: String,
    pub data: RefCell<FieldData>,
}

#[derive(Clone, Copy)]
pub(super) struct JsonlReadOptions<'a> {
    pub prefix_nulls: usize,
    pub lines_produced_prev: usize,
    pub lines_max: usize,
    pub dyn_access: bool,
    pub first_line_value: Option<&'a FieldValue>,
    pub zst_to_push: FieldValueRepr,
    pub objectify: bool,
}

pub(super) struct JsonlVisitor<'a, 'b> {
    pub ss: &'a mut StringStore,
    pub inserter_map: &'a mut IndexMap<StringStoreEntry, InserterIndex>,
    pub inserters: &'a mut IndexVec<
        InserterIndex,
        VaryingTypeInserter<RefMut<'b, FieldData>>,
    >,
    pub additional_fields: &'b StableVec<PendingField>,
    pub field_element_count:
        &'a mut IndexVec<InserterIndex, Option<DebuggableNonMaxUsize>>,
    pub opts: JsonlReadOptions<'a>,
    pub total_lines_produced: usize,
}

impl JsonlVisitor<'_, '_> {
    fn update_last_field_access(&mut self) -> bool {
        let Some(lfa) = &mut self.field_element_count[InserterIndex::ZERO]
        else {
            return false;
        };
        *lfa =
            DebuggableNonMaxUsize::new(self.total_lines_produced + 1).unwrap();
        true
    }
}
impl<'de> Visitor<'de> for &mut JsonlVisitor<'_, '_> {
    type Value = ();

    fn expecting(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        fmt.write_str("jsonl")
    }

    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_i64(i64::from(v))
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
        if !self.update_last_field_access() {
            return Ok(());
        }
        self.inserters[InserterIndex(0)].push_int(v, 1, true, false);
        Ok(())
    }

    fn visit_i128<E>(self, v: i128) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if !self.update_last_field_access() {
            return Ok(());
        }
        self.inserters[InserterIndex(0)].push_big_int(
            BigInt::from_i128(v).unwrap(),
            1,
            true,
            false,
        );
        Ok(())
    }

    fn visit_u8<E>(self, v: u8) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_i64(v as i64)
    }

    fn visit_u16<E>(self, v: u16) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_i64(v as i64)
    }

    fn visit_u32<E>(self, v: u32) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_i64(v as i64)
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if !self.update_last_field_access() {
            return Ok(());
        }
        if v < i64::MAX as u64 {
            self.inserters[InserterIndex(0)]
                .push_int(v as i64, 1, true, false);
        } else {
            self.inserters[InserterIndex(0)].push_big_int(
                BigInt::from_u64(v).unwrap(),
                1,
                true,
                false,
            );
        }

        Ok(())
    }

    fn visit_u128<E>(self, v: u128) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if !self.update_last_field_access() {
            return Ok(());
        }
        self.inserters[InserterIndex(0)].push_big_int(
            BigInt::from_u128(v).unwrap(),
            1,
            true,
            false,
        );
        Ok(())
    }

    fn visit_f32<E>(self, v: f32) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_f64(v as f64)
    }

    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if !self.update_last_field_access() {
            return Ok(());
        }
        self.inserters[InserterIndex(0)].push_float(v, 1, true, false);
        Ok(())
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
        if !self.update_last_field_access() {
            return Ok(());
        }
        self.inserters[InserterIndex(0)].push_inline_str(v, 1, true, false);
        Ok(())
    }

    fn visit_borrowed_str<E>(self, v: &'_ str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_str(v)
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if !self.update_last_field_access() {
            return Ok(());
        }
        self.inserters[InserterIndex(0)].push_string(v, 1, true, false);
        Ok(())
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if !self.update_last_field_access() {
            return Ok(());
        }
        self.inserters[InserterIndex(0)].push_bytes(v, 1, true, false);
        Ok(())
    }

    fn visit_borrowed_bytes<E>(self, v: &'_ [u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_bytes(v)
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if !self.update_last_field_access() {
            return Ok(());
        }
        self.inserters[InserterIndex(0)].push_bytes_buffer(v, 1, true, false);
        Ok(())
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Err(serde::de::Error::invalid_type(
            serde::de::Unexpected::Option,
            &self,
        ))
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
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
        D: Deserializer<'de>,
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
        if !self.update_last_field_access() {
            return Ok(());
        }
        let arr = ArrayVisitor.visit_seq(seq)?;
        self.inserters[InserterIndex(0)].push_array(arr, 1, true, false);
        Ok(())
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        if self.opts.objectify {
            if !self.update_last_field_access() {
                return Ok(());
            }
            let obj = ObjectVisitor.visit_map(map)?;
            self.inserters[InserterIndex(0)].push_object(obj, 1, true, false);
            return Ok(());
        }
        while let Some(key) = map.next_key::<DeCowStr<'de>>()? {
            let key_idx = self.ss.intern_cow(key.into());
            match self.inserter_map.entry(key_idx) {
                indexmap::map::Entry::Occupied(e) => {
                    let inserter_idx = *e.get();
                    if let Some(elem_count) =
                        &mut self.field_element_count[inserter_idx]
                    {
                        let value = map.next_value()?;
                        let gap = self.total_lines_produced
                            - elem_count.into_usize();
                        self.inserters[inserter_idx].push_zst(
                            self.opts.zst_to_push,
                            gap,
                            true,
                        );
                        *elem_count = DebuggableNonMaxUsize::new(
                            self.total_lines_produced + 1,
                        )
                        .unwrap();
                        self.inserters[inserter_idx]
                            .push_field_value_unpacked(value, 1, true, false);
                    } else {
                        map.next_value::<serde::de::IgnoredAny>()?;
                    }
                }
                indexmap::map::Entry::Vacant(e) => {
                    self.additional_fields.push(PendingField {
                        name: usize_to_str(self.inserters.len()).to_string(),
                        data: RefCell::default(),
                    });
                    let ii = InserterIndex(self.inserters.len());
                    self.inserters.push(VaryingTypeInserter::new(
                        self.additional_fields
                            .last()
                            .unwrap()
                            .data
                            .borrow_mut(),
                    ));
                    self.field_element_count.push(None);

                    if self.opts.dyn_access {
                        let value = map.next_value()?;
                        *self.field_element_count.last_mut().unwrap() = Some(
                            DebuggableNonMaxUsize::new(
                                self.total_lines_produced + 1,
                            )
                            .unwrap(),
                        );
                        self.inserters[ii].push_zst(
                            self.opts.zst_to_push,
                            self.total_lines_produced,
                            false,
                        );
                        self.inserters[ii]
                            .push_field_value_unpacked(value, 1, true, false);
                    } else {
                        map.next_value::<serde::de::IgnoredAny>()?;

                        self.inserters[ii].push_zst(
                            self.opts.zst_to_push,
                            self.opts.prefix_nulls,
                            false,
                        );
                    }
                    e.insert(ii);
                }
            }
        }
        Ok(())
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
