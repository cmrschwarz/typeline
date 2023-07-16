use std::cell::Cell;

use crate::{
    ref_iter::AutoDerefIter,
    utils::universe::Universe,
    worker_thread_session::{FieldId, MatchSet, MatchSetId},
};

use super::{
    iters::{FieldIterator, Iter},
    FieldData, FieldDataInternals, FieldValueHeader, RunLength,
};

pub type IterId = usize;

#[derive(Default)]
pub struct IterHall {
    pub(super) fd: FieldData,
    pub(super) iters: Universe<IterId, Cell<IterState>>,
}

#[derive(Default, Clone, Copy)]
pub(super) struct IterState {
    pub(super) field_pos: usize,
    pub(super) data: usize,
    pub(super) header_idx: usize,
    pub(super) header_rl_offset: RunLength,
    pub(super) cow_target: Option<FieldId>,
}

impl IterState {
    pub fn is_valid(&self) -> bool {
        self.field_pos != usize::MAX
    }
    pub fn invalidate(&mut self) {
        self.field_pos = usize::MAX
    }
}
impl IterHall {
    pub fn claim_iter(&mut self, cow_target: Option<FieldId>) -> IterId {
        let iter_id = self.iters.claim();
        self.iters[iter_id].set(IterState {
            field_pos: 0,
            data: 0,
            header_idx: 0,
            header_rl_offset: 0,
            cow_target,
        });
        iter_id
    }
    pub fn reserve_iter_id(&mut self, iter_id: IterId) {
        self.iters.reserve_id(iter_id);
    }
    pub fn release_iter(&mut self, iter_id: IterId) {
        self.iters[iter_id].get_mut().invalidate();
        self.iters.release(iter_id)
    }
    pub fn iter<'a>(&'a self) -> Iter<'a> {
        self.fd.iter()
    }
    fn get_iter_state(&self, iter_id: IterId) -> (IterState, FieldValueHeader) {
        let mut state = self.iters[iter_id].get();
        let mut h = self
            .fd
            .header
            .get(state.header_idx)
            .cloned()
            .unwrap_or_default();
        if h.run_length == state.header_rl_offset {
            state.header_idx += 1;
            state.header_rl_offset = 0;
            state.data += h.total_size();
            if state.header_idx == self.fd.header.len() {
                h.run_length = 0;
            }
        }
        (state, h)
    }
    pub fn get_iter<'a>(&'a self, iter_id: IterId) -> Iter<'a> {
        let (state, h) = self.get_iter_state(iter_id);
        let mut res = Iter {
            fd: &self.fd,
            field_pos: state.field_pos,
            data: state.data,
            header_idx: state.header_idx,
            header_rl_offset: state.header_rl_offset,
            header_rl_total: h.run_length,
            header_fmt: h.fmt,
        };
        res.skip_dead_fields();
        res
    }
    pub fn store_iter<'a>(&'a self, iter_id: IterId, iter: impl FieldIterator<'a>) {
        let mut iter = iter.as_base_iter();
        assert!(iter.fd as *const FieldData == &self.fd as *const FieldData);
        let mut state = self.iters[iter_id].get();
        state.field_pos = iter.field_pos;
        state.header_rl_offset = iter.header_rl_offset;
        if iter.header_idx == self.fd.header.len() && iter.header_idx > 0 {
            iter.prev_field();
            state.header_rl_offset = iter.field_run_length_bwd() + 1;
        }
        state.header_idx = iter.header_idx;
        state.data = iter.data;
        self.iters[iter_id].set(state);
    }

    /// returns a tuple of (FieldData, initial_field_offset, field_count)
    pub unsafe fn internals(&mut self) -> FieldDataInternals {
        self.fd.internals()
    }
    pub unsafe fn raw(&mut self) -> &mut FieldData {
        &mut self.fd
    }

    pub fn copy<'a>(
        iter: impl FieldIterator<'a> + Clone,
        targets_applicator: &mut impl FnMut(&mut dyn FnMut(&mut IterHall)),
    ) -> usize {
        let adapted_target_applicator = &mut |f: &mut dyn FnMut(&mut FieldData)| {
            let g = &mut |fdih: &mut IterHall| f(&mut fdih.fd);
            targets_applicator(g);
        };
        let copied_fields = FieldData::copy(iter, adapted_target_applicator);
        copied_fields
    }
    pub fn copy_resolve_refs<'a, I: FieldIterator<'a>>(
        match_sets: &mut Universe<MatchSetId, MatchSet>,
        iter: AutoDerefIter<'a, I>,
        targets_applicator: &mut impl FnMut(&mut dyn FnMut(&mut IterHall)),
    ) -> usize {
        let adapted_target_applicator = &mut |f: &mut dyn FnMut(&mut FieldData)| {
            let g = &mut |fdih: &mut IterHall| f(&mut fdih.fd);
            targets_applicator(g);
        };
        let copied_fields =
            FieldData::copy_resolve_refs(match_sets, iter, adapted_target_applicator);
        copied_fields
    }
    pub fn copy_iters_on_write<'a>(
        &mut self,
        targets_applicator: &mut impl FnMut(&mut dyn FnMut(FieldId, &mut IterHall)),
    ) {
        targets_applicator(&mut |field_id, ih| {
            debug_assert!(ih.iters.is_empty());
            ih.iters = self.iters.clone();
            for (i, is) in self.iters.iter_options().enumerate() {
                if let Some(is) = is {
                    if is.get().cow_target != Some(field_id) {
                        ih.iters.release(i);
                    } else {
                        self.iters.release(i);
                    }
                }
            }
        });
    }
    pub fn field_count(&self) -> usize {
        self.fd.field_count
    }
    pub fn clear(&mut self) {
        for it in self.iters.iter_mut() {
            let it = it.get_mut();
            it.data = 0;
            it.header_rl_offset = 0;
            it.header_idx = 0;
            it.field_pos = 0;
        }
        self.fd.clear();
    }
    pub fn reset(&mut self) {
        self.fd.clear();
    }
    pub fn reset_with_data(&mut self, fd: FieldData) {
        self.fd = fd;
    }
    pub fn new_with_data(fd: FieldData) -> Self {
        Self {
            fd,
            ..Default::default()
        }
    }
}
