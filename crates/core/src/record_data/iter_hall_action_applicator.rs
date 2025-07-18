use std::{
    cell::{Cell, Ref, RefMut},
    collections::VecDeque,
};

use num::Integer;

use indexland::{
    debuggable_nonmax::DebuggableNonMaxU32,
    index_newtype,
    index_slice::IndexSlice,
    index_vec::IndexVec,
    indexing_type::IndexingType,
    random_access_container::RandomAccessContainer,
    temp_vec::{TempIndexVec, TempVec, TransmutableContainer},
    universe::Universe,
};

use crate::record_data::{
    action_buffer::eprint_action_list,
    field_action::FieldActionKind,
    field_data::{
        field_value_flags::{self, LEADING_PADDING_BIT_COUNT},
        FieldValueFormat, FieldValueRepr,
    },
    field_value_ref::drop_field_value_slice,
    iter_hall::CowVariant,
};

use super::{
    action_buffer::ActorId,
    field::{Field, FieldId, FieldManager},
    field_action::FieldAction,
    field_action_applicator::FieldActionApplicator,
    field_data::{
        FieldData, FieldDataBuffer, FieldValueHeader, RunLength,
        MAX_FIELD_ALIGN,
    },
    iter::field_iterator::FieldIterator,
    iter_hall::{FieldDataSource, FieldIterId, FieldLocation, IterState},
    match_set::{MatchSetId, MatchSetManager},
};

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

index_newtype! {
    struct DataCowIndex(DebuggableNonMaxU32);
    struct FullCowIndex(DebuggableNonMaxU32);
}

enum CowFieldIndex {
    Full(FullCowIndex),
    Data(DataCowIndex),
}

struct DataCowFieldRef<'a> {
    #[allow(unused)]
    #[cfg(feature = "debug_state")]
    field_id: FieldId,
    field: Option<RefMut<'a, Field>>,
    // For fields that have only partially copied over the data.
    // `dead_data_trailing` needs to take that into account
    data_end: usize,
    // linked list of full cows belonging to this data cow
    full_cow_list: Option<FullCowIndex>,
}

struct FullCowFieldRef<'a> {
    #[allow(unused)]
    #[cfg(feature = "debug_state")]
    field_id: FieldId,
    field: Option<RefMut<'a, Field>>,
    // linked list of full cows belonging to the original field or a data cow
    prev: Option<FullCowIndex>,
}

#[derive(Default)]
pub struct IterHallActionApplicator {
    // used in drop_dead_headers to preserve alive zst headers
    // that live between dead data
    preserved_headers: Vec<FieldValueHeader>,

    full_cow_field_refs_temp:
        TempIndexVec<FullCowIndex, FullCowFieldRef<'static>>,
    data_cow_field_refs_temp:
        TempIndexVec<DataCowIndex, DataCowFieldRef<'static>>,
    iters_temp: TempVec<&'static mut IterState>,

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

impl HeaderDropInstructions {
    fn physical_drop_leading(&self) -> usize {
        self.leading_drop - self.first_header_padding
    }
    fn physical_drop_total(&self) -> usize {
        self.physical_drop_leading() + self.trailing_drop
    }
}

impl IterHallActionApplicator {
    fn push_full_cow<'a>(
        full_cow_field_refs: &mut IndexVec<FullCowIndex, FullCowFieldRef<'a>>,
        data_cow_field_refs: &mut IndexVec<DataCowIndex, DataCowFieldRef<'a>>,
        #[cfg_attr(not(feature = "debug_state"), allow(unused))]
        tgt_field_id: FieldId,
        data_cow_idx: Option<DataCowIndex>,
        starting_field_full_cow_list_head: &mut Option<FullCowIndex>,
    ) -> CowFieldIndex {
        let full_cow_idx = full_cow_field_refs.next_idx();
        let mut next = None;
        if let Some(dci) = data_cow_idx {
            let dc = &mut data_cow_field_refs[dci];
            next = dc.full_cow_list;
            dc.full_cow_list = Some(full_cow_idx);
        } else if let Some(fci) = *starting_field_full_cow_list_head {
            next = Some(fci);
        } else {
            *starting_field_full_cow_list_head = Some(full_cow_idx);
        }
        full_cow_field_refs.push(FullCowFieldRef {
            #[cfg(feature = "debug_state")]
            field_id: tgt_field_id,
            field: None,
            prev: next,
        });
        CowFieldIndex::Full(full_cow_idx)
    }
    fn push_data_cow(
        data_cow_field_refs: &mut IndexVec<DataCowIndex, DataCowFieldRef>,
        #[cfg_attr(not(feature = "debug_state"), allow(unused))]
        tgt_field_id: FieldId,
        field_headers: &VecDeque<FieldValueHeader>,
        tgt_cow_end: IterState,
    ) -> CowFieldIndex {
        let idx = data_cow_field_refs.next_idx();
        data_cow_field_refs.push(DataCowFieldRef {
            #[cfg(feature = "debug_state")]
            field_id: tgt_field_id,
            field: None,
            data_end: Self::get_data_cow_data_end(field_headers, &tgt_cow_end),
            full_cow_list: None,
        });
        CowFieldIndex::Data(idx)
    }

    // returns the target index of the field and whether or not it is data cow
    fn push_cow_field<'a>(
        fm: &'a FieldManager,
        tgt_field_id: FieldId,
        field: &Field,
        // might be different in case of nested cow
        field_headers: &VecDeque<FieldValueHeader>,
        data_cow_field_refs: &mut IndexVec<DataCowIndex, DataCowFieldRef<'a>>,
        update_cow_ms: Option<MatchSetId>,
        full_cow_field_refs: &mut IndexVec<FullCowIndex, FullCowFieldRef<'a>>,
        data_cow_idx: Option<DataCowIndex>,
        first_action: FieldAction,
        starting_field_full_cow_list_head: &mut Option<FullCowIndex>,
    ) -> CowFieldIndex {
        let mut tgt_field = fm.fields[tgt_field_id].borrow_mut();

        let cow_variant = tgt_field.iter_hall.data_source.cow_variant();
        let is_data_cow = matches!(cow_variant, Some(CowVariant::DataCow));
        let ms_id = tgt_field.match_set;

        if !is_data_cow && data_cow_idx.is_some() {
            return Self::push_full_cow(
                full_cow_field_refs,
                data_cow_field_refs,
                tgt_field_id,
                data_cow_idx,
                starting_field_full_cow_list_head,
            );
        }
        let cds = *tgt_field.iter_hall.get_cow_data_source_mut().unwrap();
        let tgt_cow_end = field.iter_hall.iters[cds.header_iter_id].get();

        if is_data_cow {
            return Self::push_data_cow(
                data_cow_field_refs,
                tgt_field_id,
                field_headers,
                tgt_cow_end,
            );
        }
        debug_assert!(cow_variant == Some(CowVariant::FullCow));
        if Some(ms_id) == update_cow_ms {
            return Self::push_full_cow(
                full_cow_field_refs,
                data_cow_field_refs,
                tgt_field_id,
                data_cow_idx,
                starting_field_full_cow_list_head,
            );
        }
        let affected_by_actions = match first_action.kind {
            FieldActionKind::Dup => {
                first_action.field_idx + 1 < tgt_cow_end.field_pos
            }
            FieldActionKind::Drop => {
                first_action.field_idx < tgt_cow_end.field_pos
            }
            FieldActionKind::InsertZst { .. } => {
                // the iterator inside the full cow might sit at the
                // end and be right leaning, so instead of
                // some fancy scheme to make them left leaning for
                // this apply we just give up on this edge case for now
                first_action.field_idx <= tgt_cow_end.field_pos
            }
        };
        if affected_by_actions {
            debug_assert!(tgt_field.iter_hall.field_data.is_empty());
            let observed_data_size = tgt_field
                .iter_hall
                .copy_headers_from_cow_src(field_headers, tgt_cow_end);
            tgt_field.iter_hall.data_source = FieldDataSource::DataCow {
                source: cds,
                observed_data_size,
            };
            // TODO: we could optimize this case because we might
            // end up calculating the dead data multiple times because of
            // this, but we don't care for now
            return Self::push_data_cow(
                data_cow_field_refs,
                tgt_field_id,
                field_headers,
                tgt_cow_end,
            );
        }
        Self::push_full_cow(
            full_cow_field_refs,
            data_cow_field_refs,
            tgt_field_id,
            data_cow_idx,
            starting_field_full_cow_list_head,
        )
        // TODO: support RecordBuffers
    }
    #[allow(clippy::only_used_in_recursion)] // msm used for assertion
    fn gather_cow_field_info_pre_exec<'a>(
        fm: &'a FieldManager,
        msm: &MatchSetManager,
        field_id: FieldId,
        update_cow_ms: Option<MatchSetId>,
        first_action: FieldAction,
        full_cow_field_refs: &mut IndexVec<FullCowIndex, FullCowFieldRef<'a>>,
        data_cow_field_refs: &mut IndexVec<DataCowIndex, DataCowFieldRef<'a>>,
        data_cow_idx: Option<DataCowIndex>,
        starting_field_full_cow_list_head: &mut Option<FullCowIndex>,
    ) {
        let field = fm.fields[field_id].borrow();
        let field_headers = fm.get_field_headers(Ref::clone(&field));
        if !field.has_cow_targets() {
            return;
        }
        for &tgt_field_id in &field.iter_hall.cow_targets {
            let field_idx = Self::push_cow_field(
                fm,
                tgt_field_id,
                &field,
                &field_headers.0,
                data_cow_field_refs,
                update_cow_ms,
                full_cow_field_refs,
                data_cow_idx,
                first_action,
                starting_field_full_cow_list_head,
            );
            Self::gather_cow_field_info_pre_exec(
                fm,
                msm,
                tgt_field_id,
                None,
                first_action,
                full_cow_field_refs,
                data_cow_field_refs,
                match field_idx {
                    CowFieldIndex::Full(_) => data_cow_idx,
                    CowFieldIndex::Data(data_cow_index) => {
                        Some(data_cow_index)
                    }
                },
                starting_field_full_cow_list_head,
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
            match field_idx {
                CowFieldIndex::Full(full_cow_index) => {
                    full_cow_field_refs[full_cow_index].field =
                        Some(tgt_field);
                }
                CowFieldIndex::Data(data_cow_index) => {
                    data_cow_field_refs[data_cow_index].field =
                        Some(tgt_field);
                }
            }
        }
    }
    pub fn advance_cow_targets(
        fm: &FieldManager,
        msm: &MatchSetManager,
        field_id: FieldId,
        update_cow_ms: MatchSetId,
        batch_size: usize,
    ) {
        fm.apply_field_actions(msm, field_id, true);
        let field = fm.fields[field_id].borrow();
        for &tgt_field_id in &field.iter_hall.cow_targets {
            let mut tgt_field = fm.fields[tgt_field_id].borrow_mut();
            if tgt_field.match_set != update_cow_ms {
                continue;
            }
            let cds = *tgt_field.iter_hall.get_cow_data_source().unwrap();

            // - `_raw` is fine because we can assume the snapshot to be up to
            //   date because we just applied field actions
            // - we can't just use the field directly here because
            // it could be cow of cow.
            // - We have to do this even in case of full cow because
            // full cow could turn into data cow in the future and at
            // that moment we need this iterator to be in the right place.
            // That's why full cow has the iterator in the first place.
            let field_ref = fm.get_cow_field_ref_raw(field_id);
            let mut iter =
                fm.lookup_iter(field_id, &field_ref, cds.header_iter_id);
            // if we use after_last here we put in unneccessary padding.
            // TODO: flipping this reveals some bugs in the adjustment
            // code. get rid of thise and then flip back
            let cow_end_before = iter.get_field_location_before_next();
            iter.next_n_fields(batch_size, true);
            let cow_end_after = iter.get_field_location_after_last();
            fm.store_iter(field_id, cds.header_iter_id, iter);

            let tgt_field_count = tgt_field.iter_hall.field_data.field_count;

            let FieldDataSource::DataCow {
                source: _,
                observed_data_size,
            } = &mut tgt_field.iter_hall.data_source
            else {
                // TODO: support RecordBufferDataCow
                debug_assert!(matches!(
                    tgt_field.iter_hall.data_source,
                    FieldDataSource::FullCow(_)
                ));
                continue;
            };

            if cow_end_before.field_pos == 0 && tgt_field_count == 0 {
                tgt_field.iter_hall.data_source =
                    FieldDataSource::FullCow(cds);
                tgt_field.iter_hall.reset_iterators();
                tgt_field.iter_hall.field_data.headers.clear();
                debug_assert!(tgt_field.iter_hall.field_data.data.is_empty());
                continue;
            }

            if cow_end_before.field_pos == cow_end_after.field_pos {
                continue;
            }
            let observed_data_size_before = *observed_data_size;
            *observed_data_size = cow_end_after.data_pos;

            let (headers, _count) = fm.get_field_headers(Ref::clone(&field));

            append_data_cow_headers(
                &headers,
                &mut tgt_field.iter_hall.field_data,
                observed_data_size_before,
                cow_end_before,
                cow_end_after,
            );
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

    // TODO: this is a bit of a mess. We should probably split this up.
    #[allow(clippy::mut_mut)]
    fn drop_dead_headers(
        &mut self,
        headers: &mut VecDeque<FieldValueHeader>,
        mut data: Option<&mut FieldDataBuffer>,
        iters: &mut [&mut IterState],
        drop_instructions: HeaderDropInstructions,
        origin_field_data_size: usize,
        field_data_size: usize,
    ) {
        debug_assert!(self.preserved_headers.is_empty());

        iters.sort_unstable();
        // includes alive zst headers that we drain and then reinsert
        let mut leading_headers_to_drain = 0;
        let mut iter_idx_fwd = 0;
        let mut header_idx_new = 0;
        let dead_data_leading_total = drop_instructions.leading_drop;
        let mut dead_data_leading_rem = dead_data_leading_total;
        let mut partial_header_dropped_elem_count = 0;

        while leading_headers_to_drain < headers.len() {
            let h = &mut headers[leading_headers_to_drain];
            let header_size_old = h.total_size_unique();
            let header_padding_old = h.leading_padding();
            let mut size_alive = 0;

            if dead_data_leading_rem < header_size_old {
                let header_elem_size = h.fmt.size as usize;
                partial_header_dropped_elem_count =
                    ((dead_data_leading_rem - header_padding_old)
                        / header_elem_size) as RunLength;
                size_alive = (h.run_length - partial_header_dropped_elem_count)
                    as usize
                    * h.fmt.size as usize;
                h.run_length -= partial_header_dropped_elem_count;
                h.set_leading_padding(drop_instructions.first_header_padding);
            }

            if h.repr.needs_drop() {
                if let Some(data) = data.as_mut() {
                    let (s1, s2) = data.data_slices_mut(
                        dead_data_leading_total - dead_data_leading_rem
                            + header_padding_old,
                        header_size_old - header_padding_old - size_alive,
                    );
                    for s in [s1, s2] {
                        unsafe {
                            drop_field_value_slice(
                                h.fmt.repr,
                                s.as_mut_ptr(),
                                s.len(),
                            );
                        }
                    }
                }
            }

            if dead_data_leading_rem < header_size_old {
                break;
            }
            dead_data_leading_rem -= header_size_old;
            leading_headers_to_drain += 1;
            if h.deleted() {
                continue;
            }
            debug_assert_eq!(header_size_old, 0);
            self.preserved_headers.push(*h);

            while iter_idx_fwd < iters.len() {
                let it = &mut iters[iter_idx_fwd];
                if it.header_idx >= leading_headers_to_drain {
                    break;
                }
                if it.header_idx + 1 != leading_headers_to_drain {
                    it.header_rl_offset = 0;
                }
                it.header_idx = header_idx_new;
                it.header_start_data_pos_pre_padding = 0;
                iter_idx_fwd += 1;
            }
            header_idx_new += 1;
        }
        let mut iters_on_partially_drained_header = 0;
        while iter_idx_fwd < iters.len() {
            let it = &mut iters[iter_idx_fwd];
            if it.header_idx > leading_headers_to_drain {
                break;
            }
            if it.header_idx == leading_headers_to_drain {
                it.header_rl_offset = it
                    .header_rl_offset
                    .saturating_sub(partial_header_dropped_elem_count);
                iters_on_partially_drained_header += 1;
            } else {
                it.header_rl_offset = 0;
            }
            it.header_idx = header_idx_new;
            it.header_start_data_pos_pre_padding = 0;
            iter_idx_fwd += 1;
        }
        iter_idx_fwd -= iters_on_partially_drained_header;
        let preserved_headers_leading = self.preserved_headers.len();
        headers.drain(0..leading_headers_to_drain);
        while let Some(h) = self.preserved_headers.pop() {
            headers.push_front(h);
        }

        let field_size_diff = origin_field_data_size - field_data_size;
        let real_leading_drop = drop_instructions
            .leading_drop
            .prev_multiple_of(&MAX_FIELD_ALIGN);
        let trailing_drop_total = drop_instructions
            .trailing_drop
            .saturating_sub(field_size_diff);
        let data_size_after =
            field_data_size - real_leading_drop - trailing_drop_total;
        let mut trailing_drop_rem = trailing_drop_total;
        let mut last_header_alive = headers.len();
        let mut iter_idx_bwd = iters.len();
        let mut dropped_headers_back = 0;
        let leading_header_drops =
            leading_headers_to_drain - preserved_headers_leading;
        loop {
            if last_header_alive <= preserved_headers_leading {
                for it in &mut iters[iter_idx_bwd..] {
                    it.header_idx -= dropped_headers_back;
                }
                break;
            }
            let header_idx = last_header_alive - 1;
            let h = &mut headers[header_idx];
            let header_size_old = h.total_size_unique();
            // we use this for the deletion size below so we always want to
            // include the padding
            let mut header_size_remaining = h.leading_padding();
            if trailing_drop_rem < header_size_old {
                let header_elem_size = h.fmt.size as usize;
                let elems_to_drop = trailing_drop_rem / header_elem_size;
                debug_assert_eq!(
                    elems_to_drop * header_elem_size,
                    trailing_drop_rem
                );
                h.run_length -= elems_to_drop as RunLength;

                header_size_remaining = h.total_size_unique();
                let data_size_header_start =
                    data_size_after - header_size_remaining;

                for it in &mut iters[iter_idx_bwd..] {
                    it.header_idx -= dropped_headers_back;
                }

                while iter_idx_bwd > iter_idx_fwd {
                    let it = &mut iters[iter_idx_bwd - 1];
                    let mut old_header_idx = it.header_idx;
                    if iter_idx_bwd
                        > iter_idx_fwd + iters_on_partially_drained_header
                    {
                        old_header_idx -= leading_header_drops;
                    }
                    if old_header_idx < header_idx {
                        break;
                    }
                    if old_header_idx == header_idx {
                        it.header_rl_offset = it
                            .header_rl_offset
                            .saturating_sub(elems_to_drop as RunLength);
                    } else {
                        it.header_rl_offset = h.run_length;
                    }
                    it.header_idx = header_idx;
                    it.header_start_data_pos_pre_padding =
                        data_size_header_start;
                    iter_idx_bwd -= 1;
                }
            }
            let size_to_del = header_size_old - header_size_remaining;
            if size_to_del > 0 && h.repr.needs_drop() {
                if let Some(data) = data.as_mut() {
                    let (s1, s2) = data.data_slices_mut(
                        field_data_size
                            - (trailing_drop_total - trailing_drop_rem)
                            - size_to_del,
                        size_to_del,
                    );
                    for s in [s1, s2] {
                        unsafe {
                            drop_field_value_slice(
                                h.fmt.repr,
                                s.as_mut_ptr(),
                                s.len(),
                            );
                        }
                    }
                }
            }

            if trailing_drop_rem < header_size_old {
                break;
            }

            last_header_alive -= 1;
            trailing_drop_rem -= header_size_old;
            if h.deleted() {
                dropped_headers_back += 1;
                continue;
            }
            debug_assert_eq!(header_size_old, 0);
            self.preserved_headers.push(*h);
            while iter_idx_bwd > iter_idx_fwd {
                let it = &mut iters[iter_idx_bwd - 1];
                let mut old_header_idx = it.header_idx;
                if iter_idx_bwd
                    > iter_idx_fwd + iters_on_partially_drained_header
                {
                    old_header_idx -= leading_header_drops;
                }
                if old_header_idx < header_idx {
                    break;
                }
                if old_header_idx != header_idx {
                    it.header_rl_offset = h.run_length;
                }
                // we do a final pass afterwards were we subtract the
                // amount of dropped headers from all affected iters.
                // to counteract those headers already dropped before
                // us (going backwards), we add them here
                it.header_idx = header_idx + dropped_headers_back;
                it.header_start_data_pos_pre_padding = data_size_after;
                iter_idx_bwd -= 1;
            }
        }

        headers.drain(last_header_alive..);
        headers.extend(self.preserved_headers.drain(0..).rev());

        while iter_idx_bwd > iter_idx_fwd {
            iter_idx_bwd -= 1;
            let it = &mut iters[iter_idx_bwd];
            if iter_idx_bwd >= iter_idx_fwd + iters_on_partially_drained_header
            {
                it.header_start_data_pos_pre_padding -= real_leading_drop;
                it.header_idx -= leading_header_drops;
            }
            if it.header_idx >= last_header_alive {
                it.header_idx = last_header_alive.saturating_sub(1);
                it.header_rl_offset = headers
                    .get(it.header_idx)
                    .map(|h| h.run_length)
                    .unwrap_or(0);
            }
        }

        if let Some(data) = data {
            data.drop_front(drop_instructions.physical_drop_leading());
            data.drop_back(drop_instructions.trailing_drop);
        }
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
        let mut data_end = iter_state.header_start_data_pos_pre_padding;
        let Some(h) = field_headers.get(iter_state.header_idx) else {
            return 0;
        };
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
        full_cow_field_refs: &mut IndexSlice<FullCowIndex, FullCowFieldRef>,
        full_cow_fields_start: Option<FullCowIndex>,
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

        let mut iterators = self.iters_temp.borrow_container();
        Self::collect_full_cow_iters(
            &mut iterators,
            &mut field.iter_hall.iters,
            full_cow_field_refs,
            full_cow_fields_start,
        );

        let _field_count_delta = self.actions_applicator.run(
            actions,
            headers,
            field_count,
            &mut iterators,
        );
    }

    fn collect_full_cow_iters<'a>(
        iterators: &mut Vec<&'a mut IterState>,
        field_iters: &'a mut Universe<FieldIterId, Cell<IterState>>,
        full_cow_field_refs: &'a mut IndexSlice<
            FullCowIndex,
            FullCowFieldRef<'_>,
        >,
        full_cow_fields_start: Option<FullCowIndex>,
    ) {
        iterators.extend(field_iters.iter_mut().map(Cell::get_mut));
        let mut full_cow_idx = full_cow_fields_start;
        let mut full_cow_refs_head = full_cow_field_refs;
        while let Some(idx) = full_cow_idx {
            full_cow_idx = full_cow_refs_head[idx].prev;
            let (head, tail) = full_cow_refs_head.split_at_mut(idx);
            iterators.extend(
                tail[FullCowIndex::ZERO]
                    .field
                    .as_mut()
                    .unwrap()
                    .iter_hall
                    .iters
                    .iter_mut()
                    .map(Cell::get_mut),
            );
            full_cow_refs_head = head;
        }
    }

    fn execute_actions(
        &mut self,
        fm: &FieldManager,
        field_id: FieldId,
        actions: impl Iterator<Item = FieldAction> + Clone,
        full_cow_field_refs: &mut IndexSlice<FullCowIndex, FullCowFieldRef>,
        full_cow_fields_start: Option<FullCowIndex>,
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
        self.execute_actions_inner(
            fm,
            field_id,
            actions,
            full_cow_field_refs,
            full_cow_fields_start,
        );
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
        first_action: FieldAction,
        full_cow_fields: &mut IndexVec<FullCowIndex, FullCowFieldRef<'a>>,
        data_cow_fields: &mut IndexVec<DataCowIndex, DataCowFieldRef<'a>>,
    ) {
        let mut field = fm.fields[field_id].borrow_mut();
        field.iter_hall.uncow_headers(fm);

        let field_count = field.iter_hall.get_field_count(fm);
        let field_ms_id = field.match_set;
        let field_data_size: usize = field.iter_hall.get_field_data_len(fm);
        let data_owned = field.iter_hall.data_source == FieldDataSource::Owned;
        let mut full_cow_field_list = None;

        drop(field);
        Self::gather_cow_field_info_pre_exec(
            fm,
            msm,
            field_id,
            update_cow_ms,
            first_action,
            full_cow_fields,
            data_cow_fields,
            None,
            &mut full_cow_field_list,
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
                full_cow_field_list,
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
            self.drop_dead_data(
                dead_data,
                field_data_size,
                &mut field,
                fm,
                data_cow_fields,
                full_cow_fields,
                full_cow_field_list,
            );
        }
    }

    fn drop_dead_data<'a>(
        &mut self,
        dead_data: DeadDataReport,
        field_data_size: usize,
        field: &mut Field,
        #[cfg_attr(
            not(feature = "debug_logging_field_actions"),
            allow(unused)
        )]
        fm: &FieldManager,
        data_cow_fields: &mut IndexSlice<DataCowIndex, DataCowFieldRef<'a>>,
        full_cow_fields: &mut IndexSlice<FullCowIndex, FullCowFieldRef<'a>>,
        full_cow_field_list: Option<FullCowIndex>,
    ) {
        let root_drop_instructions = Self::build_header_drop_instructions(
            dead_data,
            field_data_size,
            field_data_size,
        );

        let field_data_size = field.iter_hall.field_data.data.len();
        {
            let mut iterators = self.iters_temp.take_transmute();
            Self::collect_full_cow_iters(
                &mut iterators,
                &mut field.iter_hall.iters,
                full_cow_fields,
                full_cow_field_list,
            );

            self.drop_dead_headers(
                &mut field.iter_hall.field_data.headers,
                Some(&mut field.iter_hall.field_data.data),
                &mut iterators,
                root_drop_instructions,
                field_data_size,
                field_data_size,
            );
            self.iters_temp.reclaim_temp(iterators);
        }

        #[cfg(feature = "debug_logging_field_actions")]
        {
            eprintln!(
                "   + dropping dead data (leading: {}, pad: {}, rem: {}, trailing: {})",
                root_drop_instructions.leading_drop,
                root_drop_instructions.first_header_padding,
                field_data_size - root_drop_instructions.leading_drop - root_drop_instructions.trailing_drop,
                root_drop_instructions.trailing_drop
            );
            eprint!("    ");
            fm.print_field_header_data_for_ref(&*field, 4);
            #[cfg(feature = "debug_logging_iter_states")]
            {
                eprint!("\n    ");
                fm.print_field_iter_data_for_ref(&*field, 4);
            }
            eprintln!();
        }

        for dcf in &mut *data_cow_fields {
            let cow_field = &mut **dcf.field.as_mut().unwrap();

            let cow_drop_instructions = Self::build_header_drop_instructions(
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
            {
                let mut iterators = self.iters_temp.take_transmute();
                Self::collect_full_cow_iters(
                    &mut iterators,
                    &mut cow_field.iter_hall.iters,
                    full_cow_fields,
                    dcf.full_cow_list,
                );
                self.drop_dead_headers(
                    &mut cow_field.iter_hall.field_data.headers,
                    None,
                    &mut iterators,
                    cow_drop_instructions,
                    field_data_size,
                    dcf.data_end,
                );
                self.iters_temp.reclaim_temp(iterators);
            }
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

            let FieldDataSource::DataCow {
                source: _,
                observed_data_size,
            } = &mut cow_field.iter_hall.data_source
            else {
                unreachable!()
            };
            *observed_data_size -= cow_drop_instructions.physical_drop_total();
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
        first_action: FieldAction,
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
            first_action,
            &mut full_cow_fields,
            &mut data_cow_fields,
        );

        self.full_cow_field_refs_temp.reclaim_temp(full_cow_fields);
        self.data_cow_field_refs_temp.reclaim_temp(data_cow_fields);
    }
}

fn append_header_try_merge(
    tgt: &mut FieldData,
    h: FieldValueHeader,
    deficit: bool,
) {
    if h.run_length == 0 {
        return;
    }
    let mut appendable = false;
    if let Some(prev) = tgt.headers.last_mut() {
        let type_compatible = prev.is_type_compatible(h.fmt);

        let same_deleted_status = prev.deleted() == h.deleted();

        let run_len_available =
            (RunLength::MAX - prev.run_length) >= h.run_length;

        let not_appending_new_value_to_same_as_prev =
            !prev.same_value_as_previous() || h.same_value_as_previous();

        let not_appending_new_value_to_shared_value = h
            .same_value_as_previous()
            || (!prev.shared_value_and_rl_not_one() || h.repr.is_zst());

        let not_appending_shared_value_to_prev = h.same_value_as_previous()
            || h.repr.is_zst()
            || !h.shared_value_and_rl_not_one();

        appendable = type_compatible
            && same_deleted_status
            && run_len_available
            && not_appending_new_value_to_same_as_prev
            && not_appending_new_value_to_shared_value
            && not_appending_shared_value_to_prev
            && h.leading_padding() == 0;

        if appendable {
            if !deficit && !h.repr.is_zst() {
                prev.set_shared_value(false);
            }
            prev.run_length += h.run_length;
        }
    }
    if !appendable {
        tgt.headers.push_back(h);
    }
}

fn append_data_cow_headers(
    headers: &VecDeque<FieldValueHeader>,
    tgt: &mut FieldData,
    last_observed_data_size: usize,
    append_begin: FieldLocation,
    append_end: FieldLocation,
) {
    debug_assert!(append_begin.data_pos <= append_end.data_pos);
    if append_begin.header_idx == headers.len() {
        debug_assert_eq!(append_begin.field_pos, append_end.field_pos);
        return;
    }
    tgt.field_count += append_end.field_pos - append_begin.field_pos;
    let mut header_idx = append_begin.header_idx;

    let mut h = headers[header_idx];
    if append_end.header_idx == header_idx {
        h.run_length = append_end.header_rl_offset;
    }
    if append_begin.header_rl_offset > 0 {
        h.run_length -= append_begin.header_rl_offset;
        h.set_leading_padding(0);
    }
    if h.run_length == 0 {
        header_idx += 1;
        if header_idx == headers.len() {
            return;
        }
        h = headers[header_idx];
        if append_end.header_idx == header_idx {
            h.run_length = append_end.header_rl_offset;
        }
    }

    if append_begin.data_pos > last_observed_data_size {
        let mut padding_to_insert =
            append_begin.data_pos - last_observed_data_size;

        if padding_to_insert >= (1 << LEADING_PADDING_BIT_COUNT) {
            // padding this large essentially never happens so there's no point
            // in wasting cycles on caculating whether we could
            // somehow do this in one header by finding a product
            // of size and run_length that would match exactly in
            // one header
            while padding_to_insert > u16::MAX as usize {
                // intentionally truncates
                let count =
                    (padding_to_insert / u16::MAX as usize) as RunLength;
                tgt.headers.push_back(FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::BytesInline,
                        flags: field_value_flags::DELETED,
                        size: u16::MAX,
                    },
                    run_length: count,
                });
                padding_to_insert -= (count as usize) * u16::MAX as usize;
            }
            tgt.headers.push_back(FieldValueHeader {
                fmt: FieldValueFormat {
                    repr: FieldValueRepr::BytesInline,
                    flags: field_value_flags::DELETED,
                    // guaranteed to fit now
                    size: padding_to_insert as u16,
                },
                run_length: 1,
            });
        }
        h.set_leading_padding(padding_to_insert);
    }

    if last_observed_data_size > append_begin.data_pos {
        let mut data_deficit = last_observed_data_size - append_begin.data_pos;

        while data_deficit > 0 {
            let hs = h.total_size_unique();
            if hs > data_deficit {
                #[cfg(debug_assertions)]
                {
                    let hs = hs - h.leading_padding();
                    if hs > data_deficit {
                        debug_assert!(!h.shared_value());
                        debug_assert_eq!(data_deficit % (h.size as usize), 0);
                    }
                }
                data_deficit = 0;
            } else {
                data_deficit -= hs;
            }

            h.set_same_value_as_previous(true);
            h.set_leading_padding(0);
            append_header_try_merge(tgt, h, true);
            header_idx += 1;
            if header_idx == headers.len() {
                return;
            }
            h = headers[header_idx];
            if append_end.header_idx == header_idx {
                h.run_length = append_end.header_rl_offset;
            }
        }
    }
    append_header_try_merge(tgt, h, false);
    header_idx += 1;
    if header_idx > append_end.header_idx {
        return;
    }
    tgt.headers
        .extend(headers.range(header_idx..append_end.header_idx));
    if append_end.header_idx < headers.len() {
        h = headers[append_end.header_idx];
        h.run_length = append_end.header_rl_offset;
        append_header_try_merge(tgt, h, false);
    }
}

#[cfg(test)]
mod test_dead_data_drop {
    use std::collections::VecDeque;

    use crate::record_data::{
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
    };
    use indexland::indexing_type::IndexingType;

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
        ) -> Vec<IterState> {
            iters
                .into_iter()
                .map(IterState::from_raw_with_dummy_kind)
                .collect::<Vec<_>>()
        }

        let mut iters = collect_iters(iters_before);
        let iters_after = collect_iters(iters_after);
        let mut iter_refs = iters.iter_mut().collect::<Vec<_>>();

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
            None,
            &mut iter_refs,
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
            .map(FieldValueHeader::total_size_unique)
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
                header_start_data_pos_pre_padding: 40,
                header_idx: 4,
                header_rl_offset: 0,
                first_right_leaning_actor_id: LEAN_RIGHT,
            }],
            [IterStateRaw {
                field_pos: 1,
                header_start_data_pos_pre_padding: 16,
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
                header_start_data_pos_pre_padding: 3,
                header_idx: 2,
                header_rl_offset: 0,
                first_right_leaning_actor_id: LEAN_LEFT,
            }],
            [IterStateRaw {
                field_pos: 1,
                header_start_data_pos_pre_padding: 3,
                header_idx: 2,
                header_rl_offset: 0,
                first_right_leaning_actor_id: LEAN_LEFT,
            }],
        );
    }

    #[test]
    fn adjust_iters_straight_after_leading_drop() {
        // make sure that the padding is not taken away from the iterators,
        // despite it technically counting as leading dead data
        test_drop_dead_data(
            [
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::Int,
                        size: 8,
                        flags: field_value_flags::DELETED,
                    },
                    run_length: 1,
                },
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::Int,
                        size: 8,
                        flags: field_value_flags::DEFAULT,
                    },
                    run_length: 1,
                },
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::Int,
                        size: 8,
                        flags: field_value_flags::DEFAULT,
                    },
                    run_length: 1,
                },
            ],
            [
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::Int,
                        size: 8,
                        flags: field_value_flags::DEFAULT,
                    },
                    run_length: 1,
                },
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::Int,
                        size: 8,
                        flags: field_value_flags::DEFAULT,
                    },
                    run_length: 1,
                },
            ],
            [IterStateRaw {
                field_pos: 0,
                header_start_data_pos_pre_padding: 8,
                header_idx: 1,
                header_rl_offset: 0,
                first_right_leaning_actor_id: LEAN_LEFT,
            }],
            [IterStateRaw {
                field_pos: 0,
                header_start_data_pos_pre_padding: 0,
                header_idx: 0,
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
                header_start_data_pos_pre_padding: 1,
                header_idx: 1,
                header_rl_offset: 0,
                first_right_leaning_actor_id: LEAN_LEFT,
            }],
            [IterStateRaw {
                field_pos: 0,
                header_start_data_pos_pre_padding: 0,
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
    fn test_header_sandwiched_by_deletions() {
        test_drop_dead_data_explicit(
            [FieldValueHeader {
                fmt: FieldValueFormat {
                    repr: FieldValueRepr::Int,
                    size: 8,
                    flags: field_value_flags::DELETED,
                },
                run_length: 10,
            }],
            [FieldValueHeader {
                fmt: FieldValueFormat {
                    repr: FieldValueRepr::Int,
                    size: 8,
                    flags: field_value_flags::DELETED,
                },
                run_length: 5,
            }],
            80,
            80,
            DeadDataReport {
                dead_data_leading: 24,
                dead_data_trailing: 16,
            },
            [IterStateRaw {
                field_pos: 0,
                header_start_data_pos_pre_padding: 1,
                header_idx: 0,
                header_rl_offset: 10,
                first_right_leaning_actor_id: LEAN_LEFT,
            }],
            [IterStateRaw {
                field_pos: 0,
                header_start_data_pos_pre_padding: 0,
                header_idx: 0,
                header_rl_offset: 5,
                first_right_leaning_actor_id: LEAN_LEFT,
            }],
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
                header_start_data_pos_pre_padding: 16,
                header_idx: 1,
                header_rl_offset: 1,
                first_right_leaning_actor_id: LEAN_RIGHT,
            }],
            [IterStateRaw {
                field_pos: 2,
                header_start_data_pos_pre_padding: 0,
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
                header_start_data_pos_pre_padding: 2,
                header_idx: 2,
                header_rl_offset: 1,
                first_right_leaning_actor_id: LEAN_RIGHT,
            }],
            [IterStateRaw {
                field_pos: 1,
                header_start_data_pos_pre_padding: 1,
                header_idx: 1,
                header_rl_offset: 1,
                first_right_leaning_actor_id: LEAN_RIGHT,
            }],
        );
    }

    #[test]
    fn test_cleared_field_does_not_underflow() {
        test_drop_dead_data(
            [FieldValueHeader {
                fmt: FieldValueFormat {
                    repr: FieldValueRepr::Int,
                    size: 8,
                    flags: field_value_flags::DELETED,
                },
                run_length: 1,
            }],
            [],
            [IterStateRaw {
                field_pos: 0,
                header_start_data_pos_pre_padding: 0,
                header_idx: 0,
                header_rl_offset: 0,
                first_right_leaning_actor_id: LEAN_RIGHT,
            }],
            [IterStateRaw {
                field_pos: 0,
                header_start_data_pos_pre_padding: 0,
                header_idx: 0,
                header_rl_offset: 0,
                first_right_leaning_actor_id: LEAN_RIGHT,
            }],
        );
    }
}

#[cfg(test)]
mod test_append_data_cow_headers {
    use crate::{
        record_data::{
            field_data::{
                field_value_flags, FieldData, FieldValueFormat,
                FieldValueHeader, FieldValueRepr,
            },
            iter_hall::FieldLocation,
        },
        utils::ringbuf::RingBuf,
    };

    use super::append_data_cow_headers;

    #[track_caller]
    fn test_append_data_cow_headers(
        headers_src: &[FieldValueHeader],
        cow_headers_before: &[FieldValueHeader],
        cow_headers_after: &[FieldValueHeader],
        last_observed_data_size: usize,
        before: FieldLocation,
        after: FieldLocation,
    ) {
        let field_count_before = cow_headers_before
            .iter()
            .map(|h| h.effective_run_length() as usize)
            .sum();
        let mut tgt = unsafe {
            FieldData::from_raw_parts(
                cow_headers_before.iter().copied().collect(),
                RingBuf::default(),
                field_count_before,
            )
        };
        append_data_cow_headers(
            &headers_src.iter().copied().collect(),
            &mut tgt,
            last_observed_data_size,
            before,
            after,
        );
        assert_eq!(
            &*tgt.headers.iter().copied().collect::<Vec<_>>(),
            cow_headers_after,
        );
        assert_eq!(
            tgt.field_count,
            field_count_before + after.field_pos - before.field_pos
        );
        // to prevent the drop from attempting to free any data
        tgt.headers.clear();
        tgt.data.clear();
        tgt.field_count = 0;
    }

    #[test]
    fn from_empty() {
        test_append_data_cow_headers(
            &[FieldValueHeader {
                fmt: FieldValueFormat {
                    repr: FieldValueRepr::Int,
                    flags: field_value_flags::DEFAULT,
                    size: 8,
                },
                run_length: 1,
            }],
            &[],
            &[FieldValueHeader {
                fmt: FieldValueFormat {
                    repr: FieldValueRepr::Int,
                    flags: field_value_flags::DEFAULT,
                    size: 8,
                },
                run_length: 1,
            }],
            0,
            FieldLocation {
                field_pos: 0,
                header_idx: 0,
                header_rl_offset: 0,
                data_pos: 0,
            },
            FieldLocation {
                field_pos: 1,
                header_idx: 0,
                header_rl_offset: 1,
                data_pos: 8,
            },
        );
    }

    #[test]
    fn with_same_as_previous_merge() {
        test_append_data_cow_headers(
            &[FieldValueHeader {
                fmt: FieldValueFormat {
                    repr: FieldValueRepr::TextInline,
                    flags: field_value_flags::SHARED_VALUE,
                    size: 40,
                },
                run_length: 5,
            }],
            &[FieldValueHeader {
                fmt: FieldValueFormat {
                    repr: FieldValueRepr::TextInline,
                    flags: field_value_flags::SHARED_VALUE,
                    size: 40,
                },
                run_length: 3,
            }],
            &[FieldValueHeader {
                fmt: FieldValueFormat {
                    repr: FieldValueRepr::TextInline,
                    flags: field_value_flags::SHARED_VALUE,
                    size: 40,
                },
                run_length: 8,
            }],
            40,
            FieldLocation {
                field_pos: 0,
                header_idx: 0,
                header_rl_offset: 0,
                // we copied over the header last time, but it was split
                // and the part that we copied over was dropped as deleted
                // this sort of pattern can be seen on
                // aoc2023_day1_part1::case_2 (step 17)
                data_pos: 0,
            },
            FieldLocation {
                field_pos: 5,
                header_idx: 0,
                header_rl_offset: 5,
                data_pos: 40,
            },
        );
    }

    #[test]
    fn after_deleted() {
        test_append_data_cow_headers(
            &[
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::Int,
                        flags: field_value_flags::DELETED,
                        size: 8,
                    },
                    run_length: 4,
                },
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::Int,
                        flags: field_value_flags::DEFAULT,
                        size: 8,
                    },
                    run_length: 1,
                },
            ],
            &[
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::Int,
                        flags: field_value_flags::DELETED,
                        size: 8,
                    },
                    run_length: 3,
                },
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::Int,
                        flags: field_value_flags::SHARED_VALUE,
                        size: 8,
                    },
                    run_length: 1,
                },
            ],
            &[
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::Int,
                        flags: field_value_flags::DELETED,
                        size: 8,
                    },
                    run_length: 3,
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
            32,
            FieldLocation {
                field_pos: 0,
                header_idx: 1,
                header_rl_offset: 0,
                data_pos: 32,
            },
            FieldLocation {
                field_pos: 1,
                header_idx: 1,
                header_rl_offset: 1,
                data_pos: 40,
            },
        );
    }

    #[test]
    fn cannot_merge_if_padding_needed() {
        test_append_data_cow_headers(
            &[
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::TextInline,
                        flags: field_value_flags::DELETED,
                        size: 1,
                    },
                    run_length: 3,
                },
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::TextInline,
                        flags: field_value_flags::DEFAULT,
                        size: 1,
                    },
                    run_length: 1,
                },
            ],
            &[FieldValueHeader {
                fmt: FieldValueFormat {
                    repr: FieldValueRepr::TextInline,
                    flags: field_value_flags::padding(1),
                    size: 1,
                },
                run_length: 1,
            }],
            &[
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::TextInline,
                        flags: field_value_flags::padding(1),
                        size: 1,
                    },
                    run_length: 1,
                },
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::TextInline,
                        flags: field_value_flags::padding(1),
                        size: 1,
                    },
                    run_length: 1,
                },
            ],
            2,
            FieldLocation {
                field_pos: 0,
                header_idx: 1,
                header_rl_offset: 0,
                data_pos: 3,
            },
            FieldLocation {
                field_pos: 1,
                header_idx: 1,
                header_rl_offset: 1,
                data_pos: 4,
            },
        );
    }

    #[test]
    fn partial_append() {
        test_append_data_cow_headers(
            &[FieldValueHeader {
                fmt: FieldValueFormat {
                    repr: FieldValueRepr::Undefined,
                    flags: field_value_flags::SHARED_VALUE,
                    size: 0,
                },
                run_length: 5,
            }],
            &[FieldValueHeader {
                fmt: FieldValueFormat {
                    repr: FieldValueRepr::Undefined,
                    flags: field_value_flags::SHARED_VALUE,
                    size: 0,
                },
                run_length: 3,
            }],
            &[FieldValueHeader {
                fmt: FieldValueFormat {
                    repr: FieldValueRepr::Undefined,
                    flags: field_value_flags::SHARED_VALUE,
                    size: 0,
                },
                run_length: 5,
            }],
            0,
            FieldLocation {
                field_pos: 0,
                header_idx: 0,
                header_rl_offset: 1,
                data_pos: 0,
            },
            FieldLocation {
                field_pos: 0,
                header_idx: 0,
                header_rl_offset: 3,
                data_pos: 0,
            },
        );
    }

    #[test]
    fn padded_field_got_deleted_but_is_back_from_dup() {
        // based on aoc test case_2 step 27
        test_append_data_cow_headers(
            &[
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::BytesInline,
                        flags: field_value_flags::SHARED_VALUE,
                        size: 5,
                    },
                    run_length: 1,
                },
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::SlicedFieldReference,
                        flags: field_value_flags::SHARED_VALUE
                            | field_value_flags::padding(3),
                        size: 24,
                    },
                    run_length: 1,
                },
            ],
            &[
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::BytesInline,
                        flags: field_value_flags::SHARED_VALUE,
                        size: 5,
                    },
                    run_length: 1,
                },
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::SlicedFieldReference,
                        flags: field_value_flags::SHARED_VALUE
                            | field_value_flags::padding(3),
                        size: 24,
                    },
                    run_length: 1,
                },
            ],
            &[
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::BytesInline,
                        flags: field_value_flags::SHARED_VALUE,
                        size: 5,
                    },
                    run_length: 1,
                },
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::SlicedFieldReference,
                        flags: field_value_flags::SHARED_VALUE
                            | field_value_flags::padding(3),
                        size: 24,
                    },
                    run_length: 2,
                },
            ],
            32,
            FieldLocation {
                field_pos: 1,
                header_idx: 1,
                header_rl_offset: 0,
                data_pos: 8,
            },
            FieldLocation {
                field_pos: 2,
                header_idx: 1,
                header_rl_offset: 1,
                data_pos: 32,
            },
        );
    }

    #[test]
    fn disrespected_dup() {
        test_append_data_cow_headers(
            &[
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::BytesInline,
                        flags: field_value_flags::SHARED_VALUE,
                        size: 42,
                    },
                    run_length: 1,
                },
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::Undefined,
                        flags: field_value_flags::SAME_VALUE_AS_PREVIOUS
                            | field_value_flags::SHARED_VALUE,
                        size: 0,
                    },
                    run_length: 1,
                },
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::BytesInline,
                        flags: field_value_flags::SAME_VALUE_AS_PREVIOUS
                            | field_value_flags::SHARED_VALUE,
                        size: 42,
                    },
                    run_length: 1,
                },
            ],
            &[FieldValueHeader {
                fmt: FieldValueFormat {
                    repr: FieldValueRepr::BytesInline,
                    flags: field_value_flags::SHARED_VALUE,
                    size: 42,
                },
                run_length: 2,
            }],
            &[
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::BytesInline,
                        flags: field_value_flags::SHARED_VALUE,
                        size: 42,
                    },
                    run_length: 2,
                },
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::Undefined,
                        flags: field_value_flags::SHARED_VALUE
                            | field_value_flags::SAME_VALUE_AS_PREVIOUS,
                        size: 0,
                    },
                    run_length: 1,
                },
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::BytesInline,
                        flags: field_value_flags::SHARED_VALUE
                            | field_value_flags::SAME_VALUE_AS_PREVIOUS,
                        size: 42,
                    },
                    run_length: 1,
                },
            ],
            42,
            FieldLocation {
                field_pos: 1,
                header_idx: 0,
                header_rl_offset: 1,
                data_pos: 42,
            },
            FieldLocation {
                field_pos: 3,
                header_idx: 2,
                header_rl_offset: 1,
                data_pos: 42,
            },
        );
    }

    #[test]
    fn append_shared_value_after_single_value() {
        test_append_data_cow_headers(
            &[
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::Int,
                        flags: field_value_flags::SHARED_VALUE,
                        size: 8,
                    },
                    run_length: 1,
                },
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::Int,
                        flags: field_value_flags::SHARED_VALUE,
                        size: 8,
                    },
                    run_length: 2,
                },
            ],
            &[FieldValueHeader {
                fmt: FieldValueFormat {
                    repr: FieldValueRepr::Int,
                    flags: field_value_flags::SHARED_VALUE,
                    size: 8,
                },
                run_length: 1,
            }],
            &[
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::Int,
                        flags: field_value_flags::SHARED_VALUE,
                        size: 8,
                    },
                    run_length: 1,
                },
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::Int,
                        flags: field_value_flags::SHARED_VALUE,
                        size: 8,
                    },
                    run_length: 2,
                },
            ],
            8,
            FieldLocation {
                field_pos: 1,
                header_idx: 0,
                header_rl_offset: 1,
                data_pos: 8,
            },
            FieldLocation {
                field_pos: 4,
                header_idx: 1,
                header_rl_offset: 2,
                data_pos: 32,
            },
        );
    }
}
