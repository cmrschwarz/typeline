use indexmap::IndexMap;

use crate::match_value::{MatchValueFormat, MatchValueKind, MatchValueTypeErased, ObjectLayout};
use crate::string_store::StringStoreEntry;
use crate::{
    match_value::MatchValue, match_value_into_iter::MatchValueIntoIter, sync_variant::SyncVariant,
};
use std::mem::transmute_copy;
use std::{collections::VecDeque, mem::ManuallyDrop};

//if the u32 overflows we just split into two values
type RunLength = u32;

type EntryId = usize;

struct RleFieldValue {
    kind: MatchValueKind,
    flags: u8, // is_stream, value_shared_with_next,
    run_length: u32,
}

struct Field {
    data: VecDeque<u8>,
}

type FieldIndex = usize;

#[repr(C)]
pub struct MatchSet {
    indices_field: Field,
    working_set_updates: Vec<(EntryId, FieldIndex)>,
    working_set: Vec<FieldIndex>,
    fields: IndexMap<StringStoreEntry, Field>,
}
