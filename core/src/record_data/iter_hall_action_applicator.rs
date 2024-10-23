use std::{
    cell::{Cell, Ref},
    collections::VecDeque,
};

use num::Integer;

use crate::{
    record_data::{action_buffer::eprint_action_list, iter_hall::CowVariant},
    utils::{phantom_slot::PhantomSlot, temp_vec::TransmutableContainer},
};

use super::{
    action_buffer::ActorId,
    field::{Field, FieldId, FieldManager},
    field_action::FieldAction,
    field_action_applicator::FieldActionApplicator,
    field_data::{FieldValueHeader, RunLength, MAX_FIELD_ALIGN},
    iter_hall::{FieldDataSource, IterState},
    iters::FieldIterator,
    match_set::{MatchSetId, MatchSetManager},
};

// used to adjust iterators based on the dropped headers
#[derive(Default)]
struct HeaderDropInfo {
    dead_headers_leading: usize,
    header_count_rem: usize,
    first_header_dropped_elem_count: RunLength,
    last_header_run_len: RunLength,
    last_header_data_pos: usize,
}

#[derive(Clone, Copy)]
struct DeadDataReport {
    dead_data_leading: usize,
    dead_data_trailing: usize,
}

#[derive(Clone, Copy)]
struct HeaderDropInstructions {
    // the new padding to *set* (not add) to the first header alive after
    // droppage
    first_header_padding: usize,
    // leading dead data that we would like to drop. includes padding
    leading_drop: usize,
    // trailing dead data that we would like to drop (for this cow)
    // in case of full droppage, we prefer trailing over leading
    trailing_drop: usize,
}

struct DataCowFieldRef<'a> {
    #[allow(unused)]
    #[cfg(feature = "debug_state")]
    field_id: FieldId,
    field: Option<std::cell::RefMut<'a, Field>>,
    // For fields that have only partially copied over the data.
    // `dead_data_trailing` needs to take that into account
    data_end: usize,
    // required for the corresponding full cow fields
    // (`FullCowFieldRef::data_cow_idx`) that need this do adjust their
    // iterators
    drop_info: HeaderDropInfo,
}

struct FullCowFieldRef<'a> {
    #[allow(unused)]
    #[cfg(feature = "debug_state")]
    field_id: FieldId,
    field: Option<std::cell::RefMut<'a, Field>>,
    data_cow_idx: Option<usize>,
    // a full cow of a data cow. still relevant for advancing iterators
    // after dropping data, but not relevant for adjusting iterators during
    // action application
    through_data_cow: bool,
}

#[derive(Default)]
pub struct IterHallActionApplicator {
    // used in drop_dead_headers to preserve alive zst headers
    // that live between dead data
    preserved_headers: Vec<FieldValueHeader>,

    full_cow_field_refs_temp: Vec<PhantomSlot<FullCowFieldRef<'static>>>,
    data_cow_field_refs_temp: Vec<PhantomSlot<DataCowFieldRef<'static>>>,

    actions_applicator: FieldActionApplicator,
}

impl DeadDataReport {
    pub fn all_dead(field_data_size: usize) -> Self {
        DeadDataReport {
            dead_data_leading: field_data_size,
            dead_data_trailing: field_data_size,
        }
    }
}

impl IterHallActionApplicator {
    // returns the target index of the field and whether or not it is data cow
    fn push_cow_field<'a>(
        fm: &'a FieldManager,
        tgt_field_id: FieldId,
        through_data_cow: bool,
        field: &Field,
        // might be different in case of nested cow
        field_headers: &VecDeque<FieldValueHeader>,
        data_cow_field_refs: &mut Vec<DataCowFieldRef<'a>>,
        update_cow_ms: Option<MatchSetId>,
        full_cow_field_refs: &mut Vec<FullCowFieldRef<'a>>,
        data_cow_idx: Option<usize>,
        first_action_index: usize,
    ) -> (usize, bool) {
        let mut tgt_field = fm.fields[tgt_field_id].borrow_mut();

        let cow_variant = tgt_field.iter_hall.data_source.cow_variant();
        let is_data_cow = matches!(cow_variant, Some(CowVariant::DataCow));
        let ms_id = tgt_field.match_set;

        if !is_data_cow && through_data_cow {
            full_cow_field_refs.push(FullCowFieldRef {
                #[cfg(feature = "debug_state")]
                field_id: tgt_field_id,
                field: None,
                data_cow_idx,
                through_data_cow,
            });
            return (full_cow_field_refs.len() - 1, false);
        }
        let cds = tgt_field.iter_hall.get_cow_data_source_mut().unwrap();
        let tgt_cow_end = field.iter_hall.iters[cds.header_iter_id].get();

        if is_data_cow {
            data_cow_field_refs.push(DataCowFieldRef {
                #[cfg(feature = "debug_state")]
                field_id: tgt_field_id,
                field: None,
                data_end: Self::get_data_cow_data_end(
                    field_headers,
                    &tgt_cow_end,
                ),
                drop_info: HeaderDropInfo::default(),
            });
            return (data_cow_field_refs.len() - 1, true);
        }
        debug_assert!(cow_variant == Some(CowVariant::FullCow));
        if Some(ms_id) == update_cow_ms {
            full_cow_field_refs.push(FullCowFieldRef {
                #[cfg(feature = "debug_state")]
                field_id: tgt_field_id,
                field: None,
                data_cow_idx,
                through_data_cow,
            });
            return (full_cow_field_refs.len() - 1, false);
        }
        if tgt_cow_end.field_pos > first_action_index {
            tgt_field.iter_hall.data_source = FieldDataSource::DataCow(*cds);
            debug_assert!(tgt_field.iter_hall.field_data.is_empty());
            tgt_field
                .iter_hall
                .copy_headers_from_cow_src(field_headers, tgt_cow_end);
            // TODO: we could optimize this case because we might
            // end up calculating the dead data multiple times because of
            // this, but we don't care for now
            data_cow_field_refs.push(DataCowFieldRef {
                #[cfg(feature = "debug_state")]
                field_id: tgt_field_id,
                field: None,
                data_end: Self::get_data_cow_data_end(
                    field_headers,
                    &tgt_cow_end,
                ),
                drop_info: HeaderDropInfo::default(),
            });
            return (data_cow_field_refs.len() - 1, true);
        }
        full_cow_field_refs.push(FullCowFieldRef {
            #[cfg(feature = "debug_state")]
            field_id: tgt_field_id,
            field: None,
            data_cow_idx,
            through_data_cow,
        });
        // TODO: support RecordBuffers
        (full_cow_field_refs.len() - 1, false)
    }

    #[allow(clippy::only_used_in_recursion)] // msm used for assertion
    fn gather_cow_field_info_pre_exec<'a>(
        fm: &'a FieldManager,
        msm: &MatchSetManager,
        field_id: FieldId,
        update_cow_ms: Option<MatchSetId>,
        first_action_index: usize,
        through_data_cow: bool,
        full_cow_field_refs: &mut Vec<FullCowFieldRef<'a>>,
        data_cow_field_refs: &mut Vec<DataCowFieldRef<'a>>,
        data_cow_idx: Option<usize>,
    ) {
        let field = fm.fields[field_id].borrow();
        let field_headers = fm.get_field_headers(Ref::clone(&field));
        if !field.has_cow_targets() {
            return;
        }
        for &tgt_field_id in &field.iter_hall.cow_targets {
            let (curr_field_tgt_idx, data_cow) = Self::push_cow_field(
                fm,
                tgt_field_id,
                through_data_cow,
                &field,
                &field_headers.0,
                data_cow_field_refs,
                update_cow_ms,
                full_cow_field_refs,
                data_cow_idx,
                first_action_index,
            );
            Self::gather_cow_field_info_pre_exec(
                fm,
                msm,
                tgt_field_id,
                None,
                first_action_index,
                data_cow || through_data_cow,
                full_cow_field_refs,
                data_cow_field_refs,
                if data_cow {
                    Some(curr_field_tgt_idx)
                } else {
                    data_cow_idx
                },
            );
            let tgt_field = fm.fields[tgt_field_id].borrow_mut();

            // TODO: maybe find a way to reenable this assertion
            // if cfg!(debug_assertions) {
            // let actor = tgt_field.first_actor.get();
            // let snapshot = tgt_field.snapshot.get();
            // let cow_target_is_up_to_date =
            // if tgt_field.match_set == field.match_set {
            // self.is_snapshot_current(actor, snapshot)
            // } else {
            // msm.match_sets[tgt_field.match_set]
            // .action_buffer
            // .borrow_mut()
            // .is_snapshot_current(actor, snapshot)
            // };
            // debug_assert!(cow_target_is_up_to_date);
            // }

            if data_cow {
                data_cow_field_refs[curr_field_tgt_idx].field =
                    Some(tgt_field);
            } else {
                full_cow_field_refs[curr_field_tgt_idx].field =
                    Some(tgt_field);
            }
        }
    }
    pub fn update_cow_fields_post_exec(
        fm: &FieldManager,
        field_id: FieldId,
        update_cow_ms: MatchSetId,
        batch_size: usize,
    ) {
        let field = fm.fields[field_id].borrow();
        for &tgt_field_id in &field.iter_hall.cow_targets {
            let mut tgt_field = fm.fields[tgt_field_id].borrow_mut();
            if tgt_field.match_set != update_cow_ms {
                continue;
            }
            let cds = *tgt_field.iter_hall.get_cow_data_source().unwrap();

            let cow_variant = tgt_field.iter_hall.data_source.cow_variant();
            let tgt_cow_end = field.iter_hall.iters[cds.header_iter_id].get();
            // we can assume the snapshot to be up to date because we
            // just finished running field updates
            // we can't just use the field directly here because
            // it could be cow of cow
            let field_ref = fm.get_cow_field_ref_raw(field_id);
            let mut iter =
                fm.lookup_iter(field_id, &field_ref, cds.header_iter_id);
            iter.next_n_fields(batch_size, true);
            fm.store_iter(field_id, cds.header_iter_id, iter);

            if cow_variant != Some(CowVariant::DataCow) {
                // TODO: support RecordBufferDataCow
                debug_assert!(cow_variant == Some(CowVariant::FullCow));
                continue;
            }
            if tgt_cow_end.field_pos == 0
                && tgt_field.iter_hall.field_data.field_count == 0
            {
                tgt_field.iter_hall.data_source =
                    FieldDataSource::FullCow(cds);
                tgt_field.iter_hall.reset_iterators();
                tgt_field.iter_hall.field_data.headers.clear();
                debug_assert!(tgt_field.iter_hall.field_data.data.is_empty());
                continue;
            }
            let (headers, count) = fm.get_field_headers(Ref::clone(&field));
            let tgt_fd = &mut tgt_field.iter_hall.field_data;
            let start_idx = tgt_cow_end.header_idx;
            if start_idx != headers.len() {
                let mut first_header = headers[start_idx];
                first_header.run_length -= tgt_cow_end.header_rl_offset;
                if first_header.run_length != 0 {
                    if tgt_cow_end.header_rl_offset != 0 {
                        first_header.set_leading_padding(0);
                    }
                    if first_header.shared_value()
                        && tgt_cow_end.header_rl_offset != 0
                    {
                        // if we already have parts of this header,
                        // we must make sure to not mess up our data offset
                        first_header.set_same_value_as_previous(true);
                    }
                    tgt_fd.headers.push_back(first_header);
                }
            }
            if start_idx + 1 < headers.len() {
                tgt_fd.headers.extend(headers.range(start_idx + 1..));
            }
            tgt_fd.field_count += count - tgt_cow_end.field_pos;
        }
    }
    fn build_header_drop_instructions(
        dead_data: DeadDataReport,
        field_data_size: usize,
        cow_data_end: usize,
    ) -> HeaderDropInstructions {
        let mut lead = dead_data.dead_data_leading;
        let trail = dead_data
            .dead_data_trailing
            .saturating_sub(field_data_size - cow_data_end);

        let mut padding = 0;
        if cow_data_end == trail {
            lead = 0;
        } else {
            debug_assert!(lead + trail <= cow_data_end);
            // the first value in a field is always maximally aligned
            padding = lead % MAX_FIELD_ALIGN;
        }
        HeaderDropInstructions {
            first_header_padding: padding,
            leading_drop: lead,
            trailing_drop: trail,
        }
    }
    fn drop_dead_field_data(
        &mut self,
        #[cfg_attr(
            not(feature = "debug_logging_field_actions"),
            allow(unused)
        )]
        fm: &FieldManager,
        field: &mut Field,
        drop_instructions: HeaderDropInstructions,
    ) -> HeaderDropInfo {
        let field_data_size = field.iter_hall.field_data.data.len();
        let drop_info = self.drop_dead_headers(
            &mut field.iter_hall.field_data.headers,
            &mut field.iter_hall.iters,
            drop_instructions,
            field_data_size,
            field_data_size,
        );
        let fd = &mut field.iter_hall.field_data;
        // LEAK this leaks all resources of the data. //TODO: drop before
        fd.data.drop_front(
            drop_instructions
                .leading_drop
                .prev_multiple_of(&MAX_FIELD_ALIGN),
        );
        fd.data.drop_back(drop_instructions.trailing_drop);
        #[cfg(feature = "debug_logging_field_actions")]
        {
            eprintln!(
            "   + dropping dead data (leading: {}, pad: {}, rem: {}, trailing: {})",
            drop_instructions.leading_drop,
            drop_instructions.first_header_padding,
            field_data_size - drop_instructions.leading_drop - drop_instructions.trailing_drop,
            drop_instructions.trailing_drop
        );
            eprint!("    ");
            fm.print_field_header_data_for_ref(field, 4);
            #[cfg(feature = "debug_logging_iter_states")]
            {
                eprint!("\n    ");
                fm.print_field_iter_data_for_ref(field, 4);
            }
            eprintln!();
        }
        drop_info
    }
    fn adjust_iters_to_data_drop<'a>(
        iters: impl IntoIterator<Item = &'a mut Cell<IterState>>,
        dead_data_leading: usize,
        drop_info: &HeaderDropInfo,
    ) {
        let last_header_idx = drop_info.header_count_rem.saturating_sub(1);
        let actual_drop = dead_data_leading.prev_multiple_of(&MAX_FIELD_ALIGN);
        for it in iters.into_iter().map(Cell::get_mut) {
            if it.header_idx == drop_info.dead_headers_leading {
                it.header_rl_offset = it
                    .header_rl_offset
                    .saturating_sub(drop_info.first_header_dropped_elem_count);
            }
            it.header_idx =
                it.header_idx.saturating_sub(drop_info.dead_headers_leading);
            match it.header_idx.cmp(&last_header_idx) {
                std::cmp::Ordering::Less => (),
                std::cmp::Ordering::Equal => {
                    it.header_rl_offset =
                        it.header_rl_offset.min(drop_info.last_header_run_len);
                }
                std::cmp::Ordering::Greater => {
                    it.header_idx = last_header_idx;
                    it.data = drop_info.last_header_data_pos;
                    it.header_rl_offset = drop_info.last_header_run_len;
                }
            }

            if it.header_idx == 0 {
                // subtracting the 'actual_drop' is not enough to arrive at
                // this because some of the 'semantic drop'
                // might have ended up as padding of this
                // header, which would then not be subtracted from the data
                // offset
                it.data = 0;
            } else {
                it.data = it.data.saturating_sub(actual_drop);
            }
        }
    }
    fn drop_dead_headers<'a>(
        &mut self,
        headers: &mut VecDeque<FieldValueHeader>,
        iters: impl IntoIterator<Item = &'a mut Cell<IterState>>,
        drop_instructions: HeaderDropInstructions,
        origin_field_data_size: usize,
        field_data_size: usize,
    ) -> HeaderDropInfo {
        debug_assert!(self.preserved_headers.is_empty());
        let mut dead_data_leading_rem = drop_instructions.leading_drop;
        let mut dead_headers_leading = 0;
        let mut first_header_dropped_elem_count = 0;

        while dead_headers_leading < headers.len() {
            let h = &mut headers[dead_headers_leading];
            let h_ds = h.total_size_unique();
            if dead_data_leading_rem < h_ds {
                let header_elem_size = h.fmt.size as usize;
                let header_padding = h.leading_padding();
                first_header_dropped_elem_count =
                    ((dead_data_leading_rem - header_padding)
                        / header_elem_size) as RunLength;
                h.run_length -= first_header_dropped_elem_count;
                h.set_leading_padding(drop_instructions.first_header_padding);
                break;
            }
            if !h.deleted() {
                debug_assert_eq!(h_ds, 0);
                self.preserved_headers.push(*h);
            }
            dead_data_leading_rem -= h_ds;
            dead_headers_leading += 1;
        }
        headers.drain(0..dead_headers_leading);
        dead_headers_leading -= self.preserved_headers.len();
        while let Some(h) = self.preserved_headers.pop() {
            headers.push_front(h);
        }

        let field_size_diff = origin_field_data_size - field_data_size;
        let mut dead_data_rem_trailing = drop_instructions
            .trailing_drop
            .saturating_sub(field_size_diff);
        let mut first_dead_header = headers.len();
        while first_dead_header > 0 {
            let h = headers[first_dead_header - 1];
            let h_ds = h.total_size_unique();
            if dead_data_rem_trailing < h_ds {
                break;
            }
            if !h.deleted() {
                debug_assert_eq!(h_ds, 0);
                self.preserved_headers.push(h);
            }
            first_dead_header -= 1;
            dead_data_rem_trailing -= h_ds;
        }
        if dead_data_rem_trailing > 0 {
            let header_elem_size =
                headers[first_dead_header - 1].fmt.size as usize;
            let elem_count = dead_data_rem_trailing / header_elem_size;
            debug_assert!(
                elem_count * header_elem_size == dead_data_rem_trailing
            );
            headers[first_dead_header - 1].run_length -=
                elem_count as RunLength;
        }
        headers.drain(first_dead_header..);
        first_dead_header += self.preserved_headers.len();
        headers.extend(self.preserved_headers.drain(0..).rev());
        let last_header = headers.back().copied().unwrap_or_default();

        let field_end_new = field_data_size
            + drop_instructions.first_header_padding
            - drop_instructions.trailing_drop
            - drop_instructions.leading_drop;

        let last_header_size = last_header.total_size_unique();

        let drop_info = HeaderDropInfo {
            dead_headers_leading,
            first_header_dropped_elem_count,
            header_count_rem: first_dead_header,
            last_header_run_len: last_header.run_length,
            last_header_data_pos: field_end_new - last_header_size,
        };

        Self::adjust_iters_to_data_drop(
            iters,
            drop_instructions.leading_drop,
            &drop_info,
        );

        drop_info
    }
    fn calc_dead_data(
        headers: &VecDeque<FieldValueHeader>,
        dead_data_max: DeadDataReport,
        origin_field_data_size: usize,
        cow_data_end: usize,
    ) -> DeadDataReport {
        let mut data = 0;
        let mut dead_data_leading = dead_data_max.dead_data_leading;
        let mut dead_data_trailing = dead_data_max.dead_data_trailing;

        for &h in headers {
            if data >= dead_data_leading {
                break;
            }
            if h.references_alive_data() {
                data += h.leading_padding();
                dead_data_leading = dead_data_leading.min(data);
                break;
            }
            data += h.total_size_unique();
        }
        if dead_data_leading == origin_field_data_size {
            // if everything is dead, we don't reduce trailing
            return DeadDataReport {
                dead_data_leading,
                dead_data_trailing,
            };
        }
        data = origin_field_data_size - cow_data_end;
        for &h in headers.iter().rev() {
            if data >= dead_data_trailing {
                break;
            }
            if h.references_alive_data() {
                dead_data_trailing = dead_data_trailing.min(data);
                break;
            }
            data += h.total_size_unique();
        }
        DeadDataReport {
            dead_data_leading,
            dead_data_trailing,
        }
    }
    fn get_data_cow_data_end(
        field_headers: &VecDeque<FieldValueHeader>,
        iter_state: &IterState,
    ) -> usize {
        let mut data_end = iter_state.data;
        let h = field_headers[iter_state.header_idx];
        if !h.same_value_as_previous() {
            data_end += h.leading_padding();
            if h.shared_value() {
                data_end += h.size as usize;
            } else {
                data_end +=
                    h.size as usize * iter_state.header_rl_offset as usize;
            }
        }
        data_end
    }

    fn execute_actions_inner(
        &mut self,
        fm: &FieldManager,
        field_id: FieldId,
        actions: impl Iterator<Item = FieldAction>,
        full_cow_field_refs: &mut [FullCowFieldRef],
    ) {
        let mut field_ref_mut = fm.fields[field_id].borrow_mut();
        let field = &mut *field_ref_mut;
        let fd = &mut field.iter_hall.field_data;
        let (headers, field_count) = match &mut field.iter_hall.data_source {
            FieldDataSource::Owned => (&mut fd.headers, &mut fd.field_count),
            FieldDataSource::DataCow { .. }
            | FieldDataSource::RecordBufferDataCow(_) => {
                (&mut fd.headers, &mut fd.field_count)
            }
            FieldDataSource::Alias(_) => {
                panic!("cannot execute commands on Alias iter hall")
            }
            FieldDataSource::FullCow(_)
            | FieldDataSource::SameMsCow(_)
            | FieldDataSource::RecordBufferFullCow(_) => {
                panic!("cannot execute commands on FullCow iter hall")
            }
        };
        let iterators = field
            .iter_hall
            .iters
            .iter_mut()
            .chain(
                full_cow_field_refs
                    .iter_mut()
                    .filter(|fcf| !fcf.through_data_cow)
                    .flat_map(|f| {
                        f.field.as_mut().unwrap().iter_hall.iters.iter_mut()
                    }),
            )
            .map(Cell::get_mut);

        let _field_count_delta = self.actions_applicator.run(
            actions,
            headers,
            field_count,
            iterators,
        );
    }

    fn execute_actions(
        &mut self,
        fm: &FieldManager,
        field_id: FieldId,
        actions: impl Iterator<Item = FieldAction> + Clone,
        full_cow_field_refs: &mut [FullCowFieldRef],
    ) {
        #[cfg(feature = "debug_logging_field_actions")]
        {
            let field = fm.fields[field_id].borrow();
            eprintln!(
                "executing for field {} (ms {}, first actor: {:?}):",
                field_id, field.match_set, field.first_actor
            );
            drop(field);
            eprint_action_list(actions.clone());

            eprint!("   + before: ");
            fm.print_field_header_data(field_id, 3);
            #[cfg(feature = "debug_logging_iter_states")]
            {
                eprint!("\n   ");
                fm.print_field_iter_data(field_id, 3);
            }
            eprintln!();
        }
        self.execute_actions_inner(fm, field_id, actions, full_cow_field_refs);
        #[cfg(feature = "debug_logging_field_actions")]
        {
            eprint!("   + after: ");
            fm.print_field_header_data(field_id, 3);
            #[cfg(feature = "debug_logging_iter_states")]
            {
                eprint!("\n   ");
                fm.print_field_iter_data(field_id, 3);
            }
            eprintln!();
        }
    }
    fn apply_field_actions_inner<'a>(
        &mut self,
        fm: &'a FieldManager,
        msm: &MatchSetManager,
        field_id: FieldId,
        actor_id: ActorId,
        update_cow_ms: Option<MatchSetId>,
        actions: impl Iterator<Item = FieldAction> + Clone,
        actions_field_count_delta: isize,
        first_action_index: usize,
        full_cow_fields: &mut Vec<FullCowFieldRef<'a>>,
        data_cow_fields: &mut Vec<DataCowFieldRef<'a>>,
    ) {
        let mut field = fm.fields[field_id].borrow_mut();
        field.iter_hall.uncow_headers(fm);

        let field_count = field.iter_hall.get_field_count(fm);
        let field_ms_id = field.match_set;
        let field_data_size: usize = field.iter_hall.get_field_data_len(fm);
        let data_owned = field.iter_hall.data_source == FieldDataSource::Owned;

        drop(field);
        Self::gather_cow_field_info_pre_exec(
            fm,
            msm,
            field_id,
            update_cow_ms,
            first_action_index,
            false,
            full_cow_fields,
            data_cow_fields,
            None,
        );
        let mut dead_data = DeadDataReport::all_dead(field_data_size);
        for dcf in &mut *data_cow_fields {
            dead_data = Self::calc_dead_data(
                &dcf.field.as_ref().unwrap().iter_hall.field_data.headers,
                dead_data,
                field_data_size,
                dcf.data_end,
            );
        }
        debug_assert!(-actions_field_count_delta <= field_count as isize);
        let all_fields_dead =
            -actions_field_count_delta == field_count as isize;

        // the data is dead, but only for us not for the cows.
        // we could just toggle all dead bits on manually, but for now
        // we just execute, as that adjusts iters aswell
        let cow_keepalive =
            all_fields_dead && dead_data.dead_data_leading != field_data_size;

        if !all_fields_dead || !data_owned || cow_keepalive {
            self.execute_actions(
                fm,
                field_id,
                actions.clone(),
                full_cow_fields,
            );
        }
        let mut field = fm.fields[field_id].borrow_mut();

        if !all_fields_dead && data_owned {
            dead_data = Self::calc_dead_data(
                &field.iter_hall.field_data.headers,
                dead_data,
                field_data_size,
                field_data_size,
            );
        }
        // Even if the field no longer uses the data, some data COWs might,
        // in which case we can't clear it.
        let all_data_dead = data_owned
            && dead_data.dead_data_leading == field_data_size
            && all_fields_dead;
        let data_partially_dead = data_owned
            && (dead_data.dead_data_leading != 0
                || dead_data.dead_data_trailing != 0)
            && !all_data_dead;

        if all_data_dead {
            if cfg!(feature = "debug_logging_field_actions") {
                eprintln!(
                "clearing field {} (ms {}, first actor: {}, field_count: {}) ({}):",
                field_id, field_ms_id, actor_id, field_count,
                if all_data_dead {"all data dead"} else {"some data remains alive"}
            );
                eprint_action_list(actions);
            }
            field.iter_hall.reset_iterators();
            field.iter_hall.field_data.clear();
        }
        if data_partially_dead {
            let root_drop_instructions = Self::build_header_drop_instructions(
                dead_data,
                field_data_size,
                field_data_size,
            );
            let root_drop_info = self.drop_dead_field_data(
                fm,
                &mut field,
                root_drop_instructions,
            );
            for dcf in &mut *data_cow_fields {
                let cow_field = &mut **dcf.field.as_mut().unwrap();

                let cow_drop_instructions =
                    Self::build_header_drop_instructions(
                        dead_data,
                        field_data_size,
                        dcf.data_end,
                    );

                #[cfg(feature = "debug_logging_field_actions")]
                {
                    eprintln!(
                        "   + dropping dead data for cow field {}: before",
                        dcf.field_id
                    );
                    eprint!("    ");
                    fm.print_field_header_data_for_ref(cow_field, 4);
                    #[cfg(feature = "debug_logging_iter_states")]
                    {
                        eprint!("\n    ");
                        fm.print_field_iter_data_for_ref(cow_field, 4);
                    }
                    eprintln!();
                }
                dcf.drop_info = self.drop_dead_headers(
                    &mut cow_field.iter_hall.field_data.headers,
                    &mut cow_field.iter_hall.iters,
                    cow_drop_instructions,
                    field_data_size,
                    dcf.data_end,
                );
                #[cfg(feature = "debug_logging_field_actions")]
                {
                    eprintln!(
                        "   + dropping dead data for cow field {}: after",
                        dcf.field_id
                    );
                    eprint!("    ");
                    fm.print_field_header_data_for_ref(cow_field, 4);
                    #[cfg(feature = "debug_logging_iter_states")]
                    {
                        eprint!("\n    ");
                        fm.print_field_iter_data_for_ref(cow_field, 4);
                    }
                    eprintln!();
                }
            }
            for fcf in &mut *full_cow_fields {
                let drop_info = fcf
                    .data_cow_idx
                    .map(|idx| &data_cow_fields[idx].drop_info)
                    .unwrap_or(&root_drop_info);
                Self::adjust_iters_to_data_drop(
                    &mut fcf.field.as_mut().unwrap().iter_hall.iters,
                    root_drop_instructions.leading_drop,
                    drop_info,
                )
            }
        }
    }

    pub fn apply_field_actions(
        &mut self,
        fm: &FieldManager,
        msm: &MatchSetManager,
        field_id: FieldId,
        actor_id: ActorId,
        update_cow_ms: Option<MatchSetId>,
        actions: impl Iterator<Item = FieldAction> + Clone,
        actions_field_count_delta: isize,
        first_action_index: usize,
    ) {
        let mut full_cow_fields =
            self.full_cow_field_refs_temp.take_transmute();
        let mut data_cow_fields =
            self.data_cow_field_refs_temp.take_transmute();

        self.apply_field_actions_inner(
            fm,
            msm,
            field_id,
            actor_id,
            update_cow_ms,
            actions,
            actions_field_count_delta,
            first_action_index,
            &mut full_cow_fields,
            &mut data_cow_fields,
        );

        self.full_cow_field_refs_temp.reclaim_temp(full_cow_fields);
        self.data_cow_field_refs_temp.reclaim_temp(data_cow_fields);
    }
}

#[cfg(test)]
mod test_dead_data_drop {
    use std::{cell::Cell, collections::VecDeque};

    use crate::{
        record_data::{
            action_buffer::{ActorId, ActorRef},
            field::{FieldManager, FIELD_REF_LOOKUP_ITER_ID},
            field_action::FieldActionKind,
            field_data::{
                field_value_flags, FieldValueFormat, FieldValueHeader,
                FieldValueRepr,
            },
            field_value::FieldValue,
            iter_hall::{IterState, IterStateRaw},
            iter_hall_action_applicator::{
                DeadDataReport, IterHallActionApplicator,
            },
            match_set::MatchSetManager,
            push_interface::PushInterface,
            scope_manager::ScopeManager,
        },
        utils::indexing_type::IndexingType,
    };

    const LEAN_LEFT: ActorId = ActorId::MAX_VALUE;
    const LEAN_RIGHT: ActorId = ActorId::ZERO;

    #[track_caller]
    fn test_drop_dead_data_explicit(
        headers_before: impl IntoIterator<Item = FieldValueHeader>,
        headers_after: impl IntoIterator<Item = FieldValueHeader>,
        field_data_size_before: usize,
        cow_data_end: usize,
        dead_data: DeadDataReport,
        iters_before: impl IntoIterator<Item = IterStateRaw>,
        iters_after: impl IntoIterator<Item = IterStateRaw>,
    ) {
        fn collect_iters(
            iters: impl IntoIterator<Item = IterStateRaw>,
        ) -> Vec<Cell<IterState>> {
            iters
                .into_iter()
                .map(IterState::from_raw_with_dummy_kind)
                .map(Cell::new)
                .collect::<Vec<_>>()
        }

        let mut iters = collect_iters(iters_before);
        let iters_after = collect_iters(iters_after);

        let mut headers = headers_before.into_iter().collect::<VecDeque<_>>();
        let headers_after = headers_after.into_iter().collect::<VecDeque<_>>();

        let mut aa = IterHallActionApplicator::default();

        let header_drop_instructions =
            IterHallActionApplicator::build_header_drop_instructions(
                dead_data,
                field_data_size_before,
                cow_data_end,
            );

        aa.drop_dead_headers(
            &mut headers,
            &mut iters,
            header_drop_instructions,
            field_data_size_before,
            cow_data_end,
        );
        assert_eq!(headers, headers_after);
        assert_eq!(iters, iters_after);
    }

    #[track_caller]
    fn test_drop_dead_data(
        headers_before: impl IntoIterator<Item = FieldValueHeader>,
        headers_after: impl IntoIterator<Item = FieldValueHeader>,
        iters_before: impl IntoIterator<Item = IterStateRaw>,
        iters_after: impl IntoIterator<Item = IterStateRaw>,
    ) {
        let headers_before =
            headers_before.into_iter().collect::<VecDeque<_>>();
        let field_data_size_before = headers_before
            .iter()
            .map(FieldValueHeader::data_size_unique)
            .sum();
        let dead_data = IterHallActionApplicator::calc_dead_data(
            &headers_before,
            DeadDataReport::all_dead(field_data_size_before),
            field_data_size_before,
            field_data_size_before,
        );
        test_drop_dead_data_explicit(
            headers_before,
            headers_after,
            field_data_size_before,
            field_data_size_before,
            dead_data,
            iters_before,
            iters_after,
        )
    }

    #[test]
    fn padding_dropped_correctly() {
        let mut sm = ScopeManager::default();
        let mut fm = FieldManager::default();
        let mut msm = MatchSetManager::default();
        let scope_id = sm.add_scope(None);
        let ms_id = msm.add_match_set(&mut fm, &mut sm, scope_id);
        let field_id = fm.add_field(&msm, ms_id, ActorRef::default());

        {
            let mut field = fm.fields[field_id].borrow_mut();
            field.iter_hall.push_str("foo", 1, false, false);
            field.iter_hall.push_int(0, 1, false, false);
        }
        {
            let mut ab = msm.match_sets[ms_id].action_buffer.borrow_mut();
            let actor_id = ab.add_actor();
            ab.begin_action_group(actor_id);
            ab.push_action(FieldActionKind::Drop, 0, 1);
            ab.end_action_group();
        }
        fm.apply_field_actions(&msm, field_id, true);
        let field_ref = fm.get_cow_field_ref(&msm, field_id);
        let mut iter = fm.get_auto_deref_iter(
            field_id,
            &field_ref,
            FIELD_REF_LOOKUP_ITER_ID,
        );
        let mut res = Vec::new();
        while let Some((v, rl, _offs)) = iter.next_value(&msm, usize::MAX) {
            res.push((v.to_field_value(), rl));
        }
        assert_eq!(&res, &[(FieldValue::Int(0), 1)]);
    }

    #[test]
    fn test_sandwiched_by_undefined() {
        let headers_in = [
            FieldValueHeader {
                fmt: FieldValueFormat {
                    repr: FieldValueRepr::Undefined,
                    size: 0,
                    flags: field_value_flags::DELETED,
                },
                run_length: 1,
            },
            FieldValueHeader {
                fmt: FieldValueFormat {
                    repr: FieldValueRepr::TextInline,
                    size: 6,
                    flags: field_value_flags::DELETED
                        | field_value_flags::SHARED_VALUE,
                },
                run_length: 1,
            },
            FieldValueHeader {
                fmt: FieldValueFormat {
                    repr: FieldValueRepr::Undefined,
                    size: 0,
                    flags: field_value_flags::DEFAULT,
                },
                run_length: 1,
            },
        ];
        let headers_out = [FieldValueHeader {
            fmt: FieldValueFormat {
                repr: FieldValueRepr::Undefined,
                size: 0,
                flags: field_value_flags::DEFAULT,
            },
            run_length: 1,
        }];

        test_drop_dead_data(headers_in, headers_out, [], []);
    }

    #[test]
    fn correct_padding_between_same_type() {
        test_drop_dead_data(
            [
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::TextInline,
                        flags: field_value_flags::DELETED,
                        size: 1,
                    },
                    run_length: 1,
                },
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::TextInline,
                        flags: field_value_flags::DEFAULT,
                        size: 1,
                    },
                    run_length: 2,
                },
            ],
            [FieldValueHeader {
                fmt: FieldValueFormat {
                    repr: FieldValueRepr::TextInline,
                    flags: field_value_flags::padding(1),
                    size: 1,
                },
                run_length: 2,
            }],
            [],
            [],
        );
    }

    // extracted from bug in integration::foreach::batched_chunks
    #[test]
    fn correct_iter_adjustment_after_dead_header() {
        test_drop_dead_data(
            [
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::Int,
                        flags: field_value_flags::DELETED,
                        size: 8,
                    },
                    run_length: 1,
                },
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::Int,
                        flags: field_value_flags::DELETED,
                        size: 8,
                    },
                    run_length: 2,
                },
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::Int,
                        flags: field_value_flags::DEFAULT,
                        size: 8,
                    },
                    run_length: 1,
                },
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::Int,
                        flags: field_value_flags::DELETED
                            | field_value_flags::SHARED_VALUE,
                        size: 8,
                    },
                    run_length: 1,
                },
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::Int,
                        flags: field_value_flags::DEFAULT,
                        size: 8,
                    },
                    run_length: 2,
                },
            ],
            [
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::Int,
                        flags: field_value_flags::DEFAULT,
                        size: 8,
                    },
                    run_length: 1,
                },
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::Int,
                        flags: field_value_flags::DELETED
                            | field_value_flags::SHARED_VALUE,
                        size: 8,
                    },
                    run_length: 1,
                },
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::Int,
                        flags: field_value_flags::DEFAULT,
                        size: 8,
                    },
                    run_length: 2,
                },
            ],
            [IterStateRaw {
                field_pos: 1,
                data: 40,
                header_idx: 4,
                header_rl_offset: 0,
                first_right_leaning_actor_id: LEAN_RIGHT,
            }],
            [IterStateRaw {
                field_pos: 1,
                data: 16,
                header_idx: 2,
                header_rl_offset: 0,
                first_right_leaning_actor_id: LEAN_RIGHT,
            }],
        );
    }
    #[test]
    fn adjust_iters_after_padded_drop() {
        // make sure that the padding is not taken away from the iterators,
        // despite it technically counting as leading dead data
        test_drop_dead_data(
            [
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::TextInline,
                        size: 1,
                        flags: field_value_flags::padding(1),
                    },
                    run_length: 1,
                },
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::TextInline,
                        size: 1,
                        flags: field_value_flags::DELETED
                            | field_value_flags::SHARED_VALUE,
                    },
                    run_length: 1,
                },
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::TextInline,
                        size: 1,
                        flags: field_value_flags::SHARED_VALUE,
                    },
                    run_length: 1,
                },
            ],
            [
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::TextInline,
                        size: 1,
                        flags: field_value_flags::padding(1),
                    },
                    run_length: 1,
                },
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::TextInline,
                        size: 1,
                        flags: field_value_flags::DELETED
                            | field_value_flags::SHARED_VALUE,
                    },
                    run_length: 1,
                },
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::TextInline,
                        size: 1,
                        flags: field_value_flags::SHARED_VALUE,
                    },
                    run_length: 1,
                },
            ],
            [IterStateRaw {
                field_pos: 1,
                data: 3,
                header_idx: 2,
                header_rl_offset: 0,
                first_right_leaning_actor_id: LEAN_LEFT,
            }],
            [IterStateRaw {
                field_pos: 1,
                data: 3,
                header_idx: 2,
                header_rl_offset: 0,
                first_right_leaning_actor_id: LEAN_LEFT,
            }],
        );
    }

    #[test]
    fn adjust_iters_after_drop_became_padding() {
        // make sure that a deleted header that gets turned into padding
        // gets subtracted from iterator data offsets
        test_drop_dead_data(
            [
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::TextInline,
                        size: 1,
                        flags: field_value_flags::DELETED,
                    },
                    run_length: 1,
                },
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::TextInline,
                        size: 1,
                        flags: field_value_flags::DEFAULT
                            | field_value_flags::SHARED_VALUE,
                    },
                    run_length: 1,
                },
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::TextInline,
                        size: 1,
                        flags: field_value_flags::DELETED,
                    },
                    run_length: 1,
                },
            ],
            [FieldValueHeader {
                fmt: FieldValueFormat {
                    repr: FieldValueRepr::TextInline,
                    size: 1,
                    flags: field_value_flags::padding(1)
                        | field_value_flags::SHARED_VALUE,
                },
                run_length: 1,
            }],
            [IterStateRaw {
                field_pos: 0,
                data: 1,
                header_idx: 1,
                header_rl_offset: 0,
                first_right_leaning_actor_id: LEAN_LEFT,
            }],
            [IterStateRaw {
                field_pos: 0,
                data: 0,
                header_idx: 0,
                header_rl_offset: 0,
                first_right_leaning_actor_id: LEAN_LEFT,
            }],
        );
    }

    #[test]
    fn trailing_zero_sized_headers_skipped() {
        test_drop_dead_data(
            [
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::TextInline,
                        size: 1,
                        flags: field_value_flags::DELETED,
                    },
                    run_length: 2,
                },
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::Undefined,
                        size: 0,
                        flags: field_value_flags::SHARED_VALUE,
                    },
                    run_length: 1,
                },
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::Undefined,
                        size: 0,
                        flags: field_value_flags::SHARED_VALUE,
                    },
                    run_length: 2,
                },
            ],
            [
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::Undefined,
                        size: 0,
                        flags: field_value_flags::SHARED_VALUE,
                    },
                    run_length: 1,
                },
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::Undefined,
                        size: 0,
                        flags: field_value_flags::SHARED_VALUE,
                    },
                    run_length: 2,
                },
            ],
            [],
            [],
        );
    }

    #[test]
    fn non_trailing_dead_header_partially_dropped() {
        test_drop_dead_data_explicit(
            [
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::TextInline,
                        size: 1,
                        flags: field_value_flags::DELETED,
                    },
                    run_length: 2,
                },
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::Undefined,
                        size: 0,
                        flags: field_value_flags::SHARED_VALUE,
                    },
                    run_length: 3,
                },
            ],
            [
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::TextInline,
                        size: 1,
                        flags: field_value_flags::DELETED,
                    },
                    run_length: 1,
                },
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::Undefined,
                        size: 0,
                        flags: field_value_flags::SHARED_VALUE,
                    },
                    run_length: 3,
                },
            ],
            2,
            2,
            DeadDataReport {
                dead_data_leading: 0,
                dead_data_trailing: 1,
            },
            [],
            [],
        );
    }

    #[test]
    fn test_dead_data_after_cow_end() {
        test_drop_dead_data_explicit(
            [FieldValueHeader {
                fmt: FieldValueFormat {
                    repr: FieldValueRepr::TextInline,
                    size: 1,
                    flags: field_value_flags::padding(1),
                },
                run_length: 1,
            }],
            [FieldValueHeader {
                fmt: FieldValueFormat {
                    repr: FieldValueRepr::TextInline,
                    size: 1,
                    flags: field_value_flags::padding(1),
                },
                run_length: 1,
            }],
            4,
            2,
            DeadDataReport {
                dead_data_leading: 1,
                dead_data_trailing: 1,
            },
            [],
            [],
        );
    }

    #[test]
    fn test_trailing_iter_after_dead_data_adjusted_correctly() {
        test_drop_dead_data(
            [
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::Int,
                        size: 8,
                        flags: field_value_flags::DEFAULT,
                    },
                    run_length: 2,
                },
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::Int,
                        size: 8,
                        flags: field_value_flags::DELETED,
                    },
                    run_length: 1,
                },
            ],
            [FieldValueHeader {
                fmt: FieldValueFormat {
                    repr: FieldValueRepr::Int,
                    size: 8,
                    flags: field_value_flags::DEFAULT,
                },
                run_length: 2,
            }],
            [IterStateRaw {
                field_pos: 2,
                data: 16,
                header_idx: 1,
                header_rl_offset: 1,
                first_right_leaning_actor_id: LEAN_RIGHT,
            }],
            [IterStateRaw {
                field_pos: 2,
                data: 0,
                header_idx: 0,
                header_rl_offset: 2,
                first_right_leaning_actor_id: LEAN_RIGHT,
            }],
        );
    }

    #[test]
    fn test_iter_in_trailing_zst_header_adjusted_correctly() {
        test_drop_dead_data_explicit(
            [
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::TextInline,
                        size: 1,
                        flags: field_value_flags::DELETED,
                    },
                    run_length: 1,
                },
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::TextInline,
                        size: 1,
                        flags: field_value_flags::DELETED,
                    },
                    run_length: 1,
                },
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::Undefined,
                        size: 0,
                        flags: field_value_flags::DEFAULT,
                    },
                    run_length: 2,
                },
            ],
            [
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::TextInline,
                        size: 1,
                        flags: field_value_flags::DELETED,
                    },
                    run_length: 1,
                },
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::Undefined,
                        size: 0,
                        flags: field_value_flags::DEFAULT,
                    },
                    run_length: 2,
                },
            ],
            2,
            2,
            DeadDataReport {
                // we have a data-cow target that still uses the first field
                dead_data_leading: 0,
                dead_data_trailing: 1,
            },
            // This iter sits on the alive ZST after the dead 'Text' header.
            // Because dead headers and alive ZSTs could be interleaved,
            // This makes it difficult to adjust these iterators correctly.
            [IterStateRaw {
                field_pos: 1,
                data: 2,
                header_idx: 2,
                header_rl_offset: 1,
                first_right_leaning_actor_id: LEAN_RIGHT,
            }],
            [IterStateRaw {
                field_pos: 1,
                data: 1,
                header_idx: 1,
                header_rl_offset: 1,
                first_right_leaning_actor_id: LEAN_RIGHT,
            }],
        );
    }
}
