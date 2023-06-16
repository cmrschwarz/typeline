use crate::utils::universe::Universe;

use super::{fd_iter::FDIter, FieldData, RunLength};

type FDIteratorId = usize;

#[derive(Default)]
pub struct FDIterHall {
    fd: FieldData,
    iters: Universe<FDIteratorId, FDIterState>,
}

#[derive(Default, Clone, Copy)]
struct FDIterState {
    data: usize,
    header_idx: usize,
    header_rl_offset: RunLength,
}

impl FDIterHall {
    pub fn claim_iter(&mut self) -> FDIteratorId {
        self.iters.claim()
    }
    pub fn release_iter(&mut self, iter_id: FDIteratorId) {
        self.iters.release(iter_id)
    }
    pub fn get_iter<'a>(&'a self, iter_id: FDIteratorId) -> FDIter<'a> {
        let state = self.iters[iter_id];
        let h = self
            .fd
            .header
            .get(state.header_idx)
            .map(|h| *h)
            .unwrap_or_default();
        FDIter {
            fd: &self.fd,
            data: state.data,
            header_idx: state.header_idx,
            header_rl_offset: state.header_rl_offset,
            header_rl_total: h.run_length,
            header_fmt: h.fmt,
        }
    }
    pub fn store_iter<'a>(&'a mut self, iter_id: FDIteratorId, iter: FDIter<'a>) {
        assert!(iter.fd as *const FieldData == &self.fd as *const FieldData);
        let state = &mut self.iters[iter_id];
        state.data = iter.data;
        state.header_idx = iter.header_idx;
        state.header_rl_offset = iter.header_rl_offset;
    }
}
