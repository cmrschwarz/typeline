use std::{cell::RefCell, collections::HashMap};

use nonmax::NonMaxUsize;

use crate::{
    operators::transform::TransformId,
    record_data::{field::VOID_FIELD_ID, iter_hall::FieldDataSource},
    utils::{
        identity_hasher::BuildIdentityHasher, string_store::StringStoreEntry,
        universe::Universe,
    },
};

use super::{
    action_buffer::ActionBuffer,
    field::{FieldId, FieldManager},
};

pub type MatchSetId = NonMaxUsize;

#[repr(C)]
#[derive(Default)]
pub struct MatchSet {
    pub stream_participants: Vec<TransformId>,
    pub action_buffer: RefCell<ActionBuffer>,
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
    pub fn add_field_alias(
        &mut self,
        fm: &mut FieldManager,
        field_id: FieldId,
        name: StringStoreEntry,
    ) -> FieldId {
        let field = fm.fields[field_id].borrow();
        let (ms_id, first_actor, shadowed_by) =
            (field.match_set, field.first_actor, field.shadowed_by);
        drop(field);
        debug_assert!(shadowed_by == VOID_FIELD_ID);
        fm.drop_field_refcount(shadowed_by, self);
        fm.bump_field_refcount(field_id);
        // PERF: if the field has no name, and no actor was added
        // after between it's first_actor and the last,
        // we can just set it's name instead of adding an alias field
        let alias_id = fm.add_field(self, ms_id, Some(name), first_actor);
        let mut field = fm.fields[field_id].borrow_mut();
        field.shadowed_by = alias_id;
        field.shadowed_since = self.match_sets[field.match_set]
            .action_buffer
            .borrow()
            .peek_next_actor_id();

        let mut alias = fm.fields[alias_id].borrow_mut();
        alias.iter_hall.data_source = FieldDataSource::Alias(field_id);
        alias.first_actor = field.first_actor;
        alias_id
    }

    pub fn add_match_set(&mut self) -> MatchSetId {
        #[allow(unused_mut)]
        let mut ms = MatchSet {
            stream_participants: Vec::new(),
            action_buffer: RefCell::default(),
            field_name_map: HashMap::default(),
            cow_map: HashMap::default(),
        };
        #[cfg(feature = "debug_logging")]
        {
            ms.action_buffer.borrow_mut().match_set_id =
                self.match_sets.peek_claim_id();
        }
        self.match_sets.claim_with_value(ms)
    }
    pub fn remove_match_set(&mut self, _ms_id: MatchSetId) {
        todo!()
    }
    pub fn update_cow_targets(
        &mut self,
        fm: &FieldManager,
        ms_id: MatchSetId,
    ) {
        let cm = &self.match_sets[ms_id].cow_map;
        for &src in cm.keys() {
            let src_ms_id = fm.fields[src].borrow().match_set;
            self.match_sets[src_ms_id]
                .action_buffer
                .borrow_mut()
                .update_field(fm, src);
            // TODO: append to targets
        }
    }
}
