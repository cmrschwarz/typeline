use std::{
    collections::{hash_map, HashMap, HashSet, VecDeque},
    iter,
    num::NonZeroUsize,
};

use crate::{
    match_value::MatchValueKind, string_store::StringStoreEntry, sync_variant::SyncVariantImpl,
};

#[derive(Default)]

struct Field {
    // entry in the working set if this field is enabled
    // the indices field (id 0) is always enabled, but stores None here
    // so we can use NonZeroUsize
    #[allow(dead_code)] //TODO
    working_set_idx: Option<NonZeroUsize>,
    data: VecDeque<u8>,
}

pub type FieldId = usize;

//if the u32 overflows we just split into two values
pub type RunLength = u32;

pub type EntryId = usize;

#[allow(dead_code)] //TODO
struct RleFieldValue {
    kind: MatchValueKind,
    sync_variant: SyncVariantImpl,
    flags: u8, // is_stream, value_shared_with_next,
    run_length: u32,
}

pub const FIELD_ID_INDICES: FieldId = 0;
pub const FIELD_ID_INPUT: FieldId = 1;

#[repr(C)]
pub struct MatchSet {
    working_set_updates: Vec<(EntryId, FieldId)>,
    working_set: Vec<FieldId>,
    fields: Vec<Field>,
    field_names: Vec<Option<StringStoreEntry>>,
    field_name_map: HashMap<StringStoreEntry, HashSet<FieldId>>,
    unused_field_ids: Vec<FieldId>,
}

impl MatchSet {
    pub fn field_count(&self) -> usize {
        self.fields.len()
    }
    pub fn is_empty(&self) -> bool {
        self.fields[0].data.is_empty()
    }
    pub fn add_field(&mut self, name: Option<StringStoreEntry>) -> FieldId {
        let id = if let Some(unused_id) = self.unused_field_ids.pop() {
            self.field_names[unused_id as usize] = name;
            unused_id
        } else {
            let id = self.fields.len();
            self.fields.push(Default::default());
            self.field_names.push(name);
            id as FieldId
        };
        if let Some(name) = name {
            match self.field_name_map.entry(name) {
                hash_map::Entry::Occupied(ref mut e) => {
                    e.get_mut().insert(id);
                }
                hash_map::Entry::Vacant(e) => {
                    e.insert(HashSet::from_iter(iter::once(id)));
                }
            }
        }
        id
    }
    pub fn remove_field(&mut self, id: FieldId) {
        let index = id as usize;
        let name_opt = &mut self.field_names[index];
        if let Some(name) = name_opt {
            self.field_name_map.get_mut(name).unwrap().remove(&id);
            *name_opt = None;
        }
        self.fields[index].clear()
    }
}
impl Default for MatchSet {
    fn default() -> Self {
        Self {
            working_set_updates: Default::default(),
            working_set: vec![FIELD_ID_INDICES, FIELD_ID_INPUT],
            fields: vec![Default::default(), Default::default()],
            field_names: vec![None, None],
            field_name_map: Default::default(),
            unused_field_ids: Default::default(),
        }
    }
}

impl Field {
    pub fn clear(&mut self) {
        //TODO: free stuff
        self.data.clear()
    }
}
