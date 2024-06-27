use std::{cell::RefCell, collections::HashMap};

use crate::{
    index_newtype,
    operators::transform::TransformId,
    record_data::iter_hall::FieldDataSource,
    utils::{
        debuggable_nonmax::DebuggableNonMaxUsize,
        identity_hasher::BuildIdentityHasher, string_store::StringStoreEntry,
        universe::Universe,
    },
};

use super::{
    action_buffer::{ActionBuffer, ActorRef},
    field::{FieldId, FieldManager},
};

index_newtype! {
    pub struct MatchSetId( DebuggableNonMaxUsize);
}

pub struct MatchSet {
    pub dummy_field: FieldId,
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
        field_id: FieldId,
        name: StringStoreEntry,
    ) -> FieldId {
        let field = fm.fields[field_id].borrow();
        let (ms_id, first_actor, shadowed_by) =
            (field.match_set, field.first_actor.get(), field.shadowed_by);
        drop(field);
        debug_assert!(shadowed_by.is_none());
        fm.bump_field_refcount(field_id);
        // PERF: if the field has no name, and no actor was added
        // after between it's first_actor and the last,
        // we can just set it's name instead of adding an alias field
        let alias_id = fm.add_field(self, ms_id, Some(name), first_actor);
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

    pub fn add_match_set(&mut self, fm: &mut FieldManager) -> MatchSetId {
        let ms_id = self.match_sets.peek_claim_id();
        let dummy_field =
            fm.add_field(self, ms_id, None, ActorRef::Unconfirmed(0));
        let ms = MatchSet {
            dummy_field,
            stream_participants: Vec::new(),
            action_buffer: RefCell::default(),
            field_name_map: HashMap::default(),
            fields_cow_map: HashMap::default(),
        };
        #[cfg(feature = "debug_logging")]
        {
            fm.fields[dummy_field].borrow_mut().producing_transform_arg =
                format!("<MS {ms_id} Dummy>");
            ms.action_buffer.borrow_mut().match_set_id = ms_id;
        }
        self.match_sets.claim_with_value(ms);
        ms_id
    }
    pub fn remove_match_set(&mut self, _ms_id: MatchSetId) {
        todo!()
    }
    #[cfg(feature = "cow_field_logging")]
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
        #[cfg(feature = "cow_field_logging")]
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
            let src_ms_id = fm.fields[src].borrow().match_set;
            // PERF: we should only apply actions when there is
            // at least one data cow, otherwise we can delay that
            // if we give the full cow a snapshot ref
            self.match_sets[src_ms_id]
                .action_buffer
                .borrow_mut()
                .update_field(fm, src, Some(ms_id));
            ActionBuffer::update_cow_fields_post_exec(
                fm,
                src,
                ms_id,
                cow_advancement,
            );
        }

        #[cfg(feature = "cow_field_logging")]
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
