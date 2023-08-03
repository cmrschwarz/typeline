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

pub struct MatchSetManager {
    pub match_sets: Universe<MatchSetId, MatchSet>,
}

impl MatchSetManager {
    pub fn add_field_name(
        &mut self,
        fm: &FieldManager,
        field_id: FieldId,
        name: StringStoreEntry,
    ) {
        let mut field = fm.fields[field_id].borrow_mut();
        if let Some(prev_field_id) = self.match_sets[field.match_set]
            .field_name_map
            .insert(name, field_id)
        {
            if prev_field_id != field_id {
                field.names.push(name);
                let mut prev_field = fm.fields[prev_field_id].borrow_mut();
                let pos = prev_field
                    .names
                    .iter()
                    .copied()
                    .enumerate()
                    .filter_map(
                        |(i, v)| if v == name { Some(i) } else { None },
                    )
                    .next()
                    .unwrap();
                prev_field.names.swap_remove(pos);
            }
        } else {
            field.names.push(name);
        }
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
