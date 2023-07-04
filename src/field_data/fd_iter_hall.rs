use std::cell::Cell;

use crate::utils::universe::Universe;

use super::{
    fd_iter::{FDIter, FDIterMut, FDIterator},
    FieldData, RunLength,
};

pub type FDIterId = usize;

#[derive(Default)]
pub struct FDIterHall {
    pub(super) fd: FieldData,
    pub(super) initial_field_offset: usize,
    pub(super) iters: Universe<FDIterId, Cell<FDIterState>>,
}

pub struct FDIterHallInternals<'a> {
    pub fd: &'a mut FieldData,
    pub initial_field_offset: &'a mut usize,
}

#[derive(Default, Clone, Copy)]
pub(super) struct FDIterState {
    pub(super) field_pos: usize,
    pub(super) data: usize,
    pub(super) header_idx: usize,
    pub(super) header_rl_offset: RunLength,
}

impl FDIterState {
    pub fn is_valid(&self) -> bool {
        self.field_pos != usize::MAX
    }
    pub fn invalidate(&mut self) {
        self.field_pos = usize::MAX
    }
}
impl FDIterHall {
    pub fn claim_iter(&mut self) -> FDIterId {
        let iter_id = self.iters.claim();
        self.iters[iter_id].set(FDIterState {
            field_pos: self.initial_field_offset,
            data: 0,
            header_idx: 0,
            header_rl_offset: 0,
        });
        iter_id
    }
    pub fn reserve_iter_id(&mut self, iter_id: FDIterId) {
        self.iters.reserve_id(iter_id);
    }
    pub fn release_iter(&mut self, iter_id: FDIterId) {
        self.iters[iter_id].get_mut().invalidate();
        self.iters.release(iter_id)
    }
    pub fn iter<'a>(&'a self) -> FDIter<'a> {
        self.fd.iter()
    }
    pub fn get_iter<'a>(&'a self, iter_id: FDIterId) -> FDIter<'a> {
        let state = self.iters[iter_id].get();
        let h = self
            .fd
            .header
            .get(state.header_idx)
            .cloned()
            .unwrap_or_default();
        FDIter {
            fd: &self.fd,
            field_pos: state.field_pos - self.initial_field_offset,
            data: state.data,
            header_idx: state.header_idx,
            header_rl_offset: state.header_rl_offset,
            header_rl_total: h.run_length,
            header_fmt: h.fmt,
        }
    }
    pub fn get_iter_mut<'a>(&'a mut self, iter_id: FDIterId) -> FDIterMut<'a> {
        let state = self.iters[iter_id].get();
        let h = self
            .fd
            .header
            .get(state.header_idx)
            .cloned()
            .unwrap_or_default();
        FDIterMut {
            fd: &mut self.fd,
            field_pos: state.field_pos - self.initial_field_offset,
            data: state.data,
            header_idx: state.header_idx,
            header_rl_offset: state.header_rl_offset,
            header_rl_total: h.run_length,
            header_fmt: h.fmt,
        }
    }
    pub fn store_iter<'a>(&'a self, iter_id: FDIterId, iter: impl FDIterator<'a>) {
        let iter = iter.as_base_iter();
        assert!(iter.fd as *const FieldData == &self.fd as *const FieldData);
        let mut state = self.iters[iter_id].get();
        state.data = iter.data;
        state.header_idx = iter.header_idx;
        state.header_rl_offset = iter.header_rl_offset;
        state.field_pos = iter.field_pos;
        self.iters[iter_id].set(state);
    }

    /// returns a tuple of (FieldData, initial_field_offset, field_count)
    pub unsafe fn internals(&mut self) -> FDIterHallInternals {
        FDIterHallInternals {
            fd: &mut self.fd,
            initial_field_offset: &mut self.initial_field_offset,
        }
    }

    pub fn copy<'a, TargetApplicatorFn: FnMut(&mut dyn FnMut(&mut FDIterHall))>(
        iter: impl FDIterator<'a> + Clone,
        mut targets_applicator: TargetApplicatorFn,
    ) -> usize {
        let adapted_target_applicator = &mut |f: &mut dyn FnMut(&mut FieldData)| {
            let g = &mut |fdih: &mut FDIterHall| f(&mut fdih.fd);
            targets_applicator(g);
        };
        let copied_fields = FieldData::copy(iter, adapted_target_applicator);
        copied_fields
    }
    pub fn field_count(&self) -> usize {
        self.fd.field_count
    }
    pub fn field_index_offset(&self) -> usize {
        self.initial_field_offset
    }
    pub fn clear(&mut self) {
        self.initial_field_offset += self.fd.field_count;
        for it in self.iters.iter_mut() {
            let it = it.get_mut();
            it.data = 0;
            it.header_rl_offset = 0;
            it.header_idx = 0;
            it.field_pos = self.initial_field_offset;
        }
        self.fd.clear();
    }
    pub fn reset(&mut self) {
        self.initial_field_offset = 0;
        self.fd.clear();
    }
    pub fn reset_with_data(&mut self, fd: FieldData) {
        self.initial_field_offset = 0;
        self.fd = fd;
    }
    pub fn new_with_data(fd: FieldData) -> Self {
        Self {
            fd,
            ..Default::default()
        }
    }
}
