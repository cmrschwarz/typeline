use std::{cell::RefCell, collections::HashMap};

use crate::{
    index_newtype,
    operators::transform::TransformId,
    record_data::iter_hall::FieldDataSource,
    utils::{
        debuggable_nonmax::DebuggableNonMaxUsize,
        identity_hasher::BuildIdentityHasher, indexing_type::IndexingType,
        string_store::StringStoreEntry, universe::Universe,
    },
};

use super::{
    action_buffer::{ActionBuffer, ActorId, ActorRef},
    field::{FieldId, FieldManager},
    iter_hall_action_applicator::IterHallActionApplicator,
    scope_manager::{ScopeId, ScopeManager},
};

index_newtype! {
    pub struct MatchSetId(DebuggableNonMaxUsize);

}

pub struct MatchSet {
    pub dummy_field: FieldId,
    pub stream_participants: Vec<TransformId>,
    pub action_buffer: RefCell<ActionBuffer>,
    pub active_scope: ScopeId,

    // stores original field -> cow copy
    // Entries are added when fields from other MatchSets are cow'ed into this
    // one used to avoid duplicates, especially when automatically cowing
    // field refs once a cow is accessed
    // does *not* increase the refcount of either fields.
    // FieldManager::remove_field removes entries from this
    // two primary usecases:
    // - avoid duplicates
    // - update our cow's when control flow enters into this match set
    pub fields_cow_map: HashMap<FieldId, FieldId, BuildIdentityHasher>,
}

#[derive(Default)]
pub struct MatchSetManager {
    pub match_sets: Universe<MatchSetId, MatchSet>,
}

impl MatchSetManager {
    pub fn add_field_alias(
        &mut self,
        fm: &mut FieldManager,
        sm: &mut ScopeManager,
        field_id: FieldId,
        name: StringStoreEntry,
    ) -> FieldId {
        let field = fm.fields[field_id].borrow();
        let (ms_id, first_actor, shadowed_by) =
            (field.match_set, field.first_actor.get(), field.shadowed_by);

        let scope_id = self.match_sets[ms_id].active_scope;
        drop(field);
        debug_assert!(shadowed_by.is_none());
        fm.bump_field_refcount(field_id);
        // PERF: if the field has no name, and no actor was added
        // after between it's first_actor and the last,
        // we can just set it's name instead of adding an alias field
        let alias_id = fm.add_field(self, ms_id, first_actor);
        sm.insert_field_name(scope_id, name, alias_id);
        let mut field = fm.fields[field_id].borrow_mut();
        field.shadowed_by = Some(alias_id);
        field.shadowed_since = self.match_sets[field.match_set]
            .action_buffer
            .borrow()
            .peek_next_actor_id();

        let mut alias = fm.fields[alias_id].borrow_mut();
        alias.iter_hall.data_source = FieldDataSource::Alias(field_id);
        alias.first_actor = field.first_actor.clone();
        alias_id
    }

    pub fn add_match_set(
        &mut self,
        fm: &mut FieldManager,
        _sm: &mut ScopeManager,
        scope: ScopeId,
    ) -> MatchSetId {
        let ms_id = self.match_sets.peek_claim_id();
        let ms = MatchSet {
            dummy_field: FieldId::MAX_VALUE,
            active_scope: scope,
            stream_participants: Vec::new(),
            action_buffer: RefCell::new(ActionBuffer::new(ms_id)),
            fields_cow_map: HashMap::default(),
        };

        self.match_sets.claim_with_value(ms);
        let dummy_field =
            fm.add_field(self, ms_id, ActorRef::Unconfirmed(ActorId::ZERO));

        let ms = &mut self.match_sets[ms_id];
        #[cfg(feature = "debug_logging")]
        {
            fm.fields[dummy_field].borrow_mut().producing_transform_arg =
                format!("<MS {ms_id} Dummy>");
            ms.action_buffer.borrow_mut().match_set_id = ms_id;
        }
        ms.dummy_field = dummy_field;
        ms_id
    }
    pub fn remove_match_set(&mut self, _ms_id: MatchSetId) {
        todo!()
    }
    #[cfg(feature = "debug_logging_cow_fields")]
    fn print_updating_cow_bindings(&self, ms_id: MatchSetId) {
        let cm = &self.match_sets[ms_id].fields_cow_map;
        let mut iter = cm.iter().peekable();
        while let Some((src, usr)) = iter.next() {
            eprint!("{src} <- {usr}");
            if iter.peek().is_some() {
                eprint!(", ");
            }
        }
    }
    // the heart of every ms transition. all the cow targets of
    // the ms need to aquire the updated state from their respective
    // sources and the group track has to copy over the new groups
    pub fn update_cross_ms_cow_targets(
        &self,
        fm: &FieldManager,
        ms_id: MatchSetId,
        cow_advancement: usize,
    ) {
        let cm = &self.match_sets[ms_id].fields_cow_map;
        #[cfg(feature = "debug_logging_cow_fields")]
        {
            eprintln!("{:-^80}", " <updating cow bindings> ");
            eprint!("updating: ");
            self.print_updating_cow_bindings(ms_id);
            eprintln!();
            fm.print_fields_with_header_data();
            eprintln!("{:=^80}", "");
            fm.print_fields_with_iter_data();
            eprintln!("{:-^80}", " </updating cow bindings> ");
        }

        for &src in cm.keys() {
            // PERF: in theory, we could be lazy here and just add
            // the current snapshot of the source field to the target

            fm.apply_field_actions(self, src, true);

            IterHallActionApplicator::update_cow_fields_post_exec(
                fm,
                src,
                ms_id,
                cow_advancement,
            );
        }

        #[cfg(feature = "debug_logging_cow_fields")]
        {
            eprintln!("{:-^80}", " <updated cow bindings> ");
            eprint!("updated: ");
            self.print_updating_cow_bindings(ms_id);
            eprintln!();
            fm.print_fields_with_header_data();
            eprintln!("{:=^80}", "");
            fm.print_fields_with_iter_data();
            eprintln!("{:-^80}", " </updated cow bindings> ");
        }
    }
    pub fn get_dummy_field(&self, ms_id: MatchSetId) -> FieldId {
        self.match_sets[ms_id].dummy_field
    }
}
