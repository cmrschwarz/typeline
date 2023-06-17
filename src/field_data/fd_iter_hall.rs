use std::{
    cell::Cell,
    ops::{Deref, DerefMut},
};

use crate::utils::universe::Universe;

use super::{
    fd_iter::{FDIter, FDIterMut},
    FieldData, RunLength,
};

pub type FDIterId = usize;

#[derive(Default)]
pub struct FDIterHall {
    fd: FieldData,
    iters: Universe<FDIterId, Cell<FDIterState>>,
}

#[derive(Default, Clone, Copy)]
struct FDIterState {
    field_pos: usize,
    data: usize,
    header_idx: usize,
    header_rl_offset: RunLength,
}

impl FDIterHall {
    pub fn claim_iter(&mut self) -> FDIterId {
        self.iters.claim()
    }
    pub fn release_iter(&mut self, iter_id: FDIterId) {
        self.iters.release(iter_id)
    }
    pub fn get_iter<'a>(&'a self, iter_id: FDIterId) -> FDIter<'a> {
        let state = self.iters[iter_id].get();
        let h = self
            .fd
            .header
            .get(state.header_idx)
            .map(|h| *h)
            .unwrap_or_default();
        FDIter {
            fd: &self.fd,
            field_pos: state.field_pos,
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
            .map(|h| *h)
            .unwrap_or_default();
        FDIterMut {
            fd: &mut self.fd,
            field_pos: state.field_pos,
            data: state.data,
            header_idx: state.header_idx,
            header_rl_offset: state.header_rl_offset,
            header_rl_total: h.run_length,
            header_fmt: h.fmt,
        }
    }
    pub fn store_iter<'a>(&'a self, iter_id: FDIterId, iter: FDIter<'a>) {
        assert!(iter.fd as *const FieldData == &self.fd as *const FieldData);
        let mut state = self.iters[iter_id].get();
        state.data = iter.data;
        state.header_idx = iter.header_idx;
        state.header_rl_offset = iter.header_rl_offset;
        self.iters[iter_id].set(state);
    }
}

//HACK: this is completely unsound, but helps with testing for now
impl Deref for FDIterHall {
    type Target = FieldData;

    fn deref(&self) -> &Self::Target {
        &self.fd
    }
}
impl DerefMut for FDIterHall {
    fn deref_mut(&mut self) -> &mut FieldData {
        &mut self.fd
    }
}
