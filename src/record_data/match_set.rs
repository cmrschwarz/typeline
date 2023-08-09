use std::collections::HashMap;

use nonmax::NonMaxUsize;

use crate::{
    operators::transform::TransformId,
    utils::{
        identity_hasher::BuildIdentityHasher, string_store::StringStoreEntry,
        universe::Universe,
    },
};

use super::{
    command_buffer::CommandBuffer,
    field::{FieldId, FieldManager},
};

pub type MatchSetId = NonMaxUsize;

#[repr(C)]
pub struct MatchSet {
    pub stream_participants: Vec<TransformId>,
    pub command_buffer: CommandBuffer,
    pub field_name_map:
        HashMap<StringStoreEntry, FieldId, BuildIdentityHasher>,
}

#[derive(Default)]
pub struct MatchSetManager {
    pub match_sets: Universe<MatchSetId, MatchSet>,
}

impl MatchSetManager {
    pub fn set_field_name(
        &mut self,
        fm: &FieldManager,
        field_id: FieldId,
        name: StringStoreEntry,
    ) {
        let mut field = fm.fields[field_id].borrow_mut();
        // we allow reassigning for the case of prebound outputs
        debug_assert!(field.name.is_none() || field.name == Some(name));
        field.name = Some(name);
        self.match_sets[field.match_set]
            .field_name_map
            .insert(name, field_id);
    }
    pub fn add_match_set(&mut self) -> MatchSetId {
        self.match_sets.claim_with(|| MatchSet {
            stream_participants: Default::default(),
            command_buffer: Default::default(),
            field_name_map: Default::default(),
        })
    }
    pub fn remove_match_set(&mut self, _ms_id: MatchSetId) {
        todo!()
    }
}
