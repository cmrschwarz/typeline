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
    command_buffer::ActionBuffer,
    field::{FieldId, FieldManager},
};

pub type MatchSetId = NonMaxUsize;

#[repr(C)]
#[derive(Default)]
pub struct MatchSet {
    pub stream_participants: Vec<TransformId>,
    pub action_buffer: ActionBuffer,
    pub field_name_map:
        HashMap<StringStoreEntry, FieldId, BuildIdentityHasher>,
    // stores original field -> cow copy
    // Entries are added when fields from other MatchSets are cow'ed into this
    // one used to avoid duplicates, especially when automatically cowing
    // field refs once a cow is accessed
    // does *not* increase the refcount of either fields.
    // FieldManager::remove_field removes entries from this
    pub cow_map: HashMap<FieldId, FieldId, BuildIdentityHasher>,
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
        #[allow(unused_mut)]
        let mut ms = MatchSet {
            stream_participants: Default::default(),
            action_buffer: Default::default(),
            field_name_map: Default::default(),
            cow_map: Default::default(),
        };
        #[cfg(feature = "debug_logging")]
        {
            ms.action_buffer.match_set_id = self.match_sets.peek_claim_id();
        }
        self.match_sets.claim_with_value(ms)
    }
    pub fn remove_match_set(&mut self, _ms_id: MatchSetId) {
        todo!()
    }
}
