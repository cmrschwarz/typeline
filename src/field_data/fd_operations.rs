use crate::{field_data::fd_iter::FDIterator, worker_thread_session::Field};

use super::{fd_iter::FDIterMut, FieldData, FieldValueHeader, RunLength};

pub enum FieldActionKind {
    Dup,
    Drop,
}
pub struct FieldAction {
    pub kind: FieldActionKind,
    pub run_len: RunLength,
    pub field_idx: usize,
}

#[derive(Default)]
pub struct FieldCommandBuffer {
    pub actions: Vec<FieldAction>,
    copy_ranges: Vec<usize>,
    insertions: Vec<FieldValueHeader>,
}

impl FieldCommandBuffer {
    pub fn push_action_with_usize_rl(
        &mut self,
        kind: FieldActionKind,
        field_idx: usize,
        mut run_length: usize,
    ) {
        while run_length > 0 {
            let rl_to_push = run_length.min(RunLength::MAX as usize) as RunLength;
            self.actions.push(FieldAction {
                kind: FieldActionKind::Drop,
                field_idx,
                run_len: rl_to_push,
            });
            run_length -= rl_to_push as usize;
        }
    }

    pub fn prime<'a>(&'a mut self, field_data: &'a mut FieldData) -> PrimedFieldCommandBuffer<'a> {
        PrimedFieldCommandBuffer::new(self, FDIterMut::from_start(field_data))
    }
    pub fn prime_from_iter<'a>(&'a mut self, iter: FDIterMut<'a>) -> PrimedFieldCommandBuffer<'a> {
        PrimedFieldCommandBuffer::new(self, iter)
    }
}

// this is just so that this module can provide a sound API
// we can't let safe code execute commands on a different Field than they
// were built for as that might cause type confusions in the Field Data
pub struct PrimedFieldCommandBuffer<'a> {
    iter: FDIterMut<'a>,
    cb: &'a mut FieldCommandBuffer,
}

impl<'a> PrimedFieldCommandBuffer<'a> {
    pub fn new(
        command_buffer: &'a mut FieldCommandBuffer,
        iter: FDIterMut<'a>,
    ) -> PrimedFieldCommandBuffer<'a> {
        let mut tgt = PrimedFieldCommandBuffer {
            cb: command_buffer,
            iter,
        };
        tgt.build_copy_ranges();
        tgt
    }
    fn adjust_deletion_header_bounds(
        &mut self,
        first_header_idx: usize,
        first_header_oversize: RunLength,
        last_header_idx: usize,
        last_header_oversize: RunLength,
    ) {
        let headers = &mut self.iter.fd.header;
        let mut headers_to_insert = 0;
        if first_header_oversize > 0 {
            headers_to_insert += 1;
        }
        if last_header_oversize > 0 {
            headers_to_insert += 1;
        }
        if headers_to_insert == 0 {
            return;
        }
        if headers_to_insert == 1 {
            if first_header_oversize > 0 {
                let h = headers[first_header_idx];
                headers.insert(
                    first_header_idx + 1,
                    FieldValueHeader {
                        fmt: h.fmt,
                        run_length: h.run_length - first_header_oversize,
                    },
                );
                headers[first_header_idx].run_length = first_header_oversize;
                headers[first_header_idx].set_deleted(false);
                return;
            }
            let mut h = headers[last_header_idx];
            h.set_deleted(false);
            headers.insert(
                last_header_idx + 1,
                FieldValueHeader {
                    fmt: h.fmt,
                    run_length: last_header_oversize,
                },
            );
            headers[last_header_idx].run_length -= last_header_oversize;
            return;
        }
        unsafe {
            headers.reserve(2);
            let buf = headers.as_mut_ptr();
            std::ptr::copy(
                buf.add(last_header_idx),
                buf.add(last_header_idx + 2),
                headers.len() - last_header_idx,
            );
            std::ptr::copy(
                buf.add(first_header_idx),
                buf.add(first_header_idx + 1),
                last_header_idx - first_header_idx,
            );
            *buf.add(last_header_idx + 1) = *buf.add(last_header_idx + 2);
        }
        headers[first_header_idx].set_deleted(false);
        headers[first_header_idx].run_length = first_header_oversize;
        headers[first_header_idx + 1].run_length -= first_header_oversize;

        headers[last_header_idx + 1].run_length -= last_header_oversize;
        headers[last_header_idx + 2].set_deleted(false);
        headers[last_header_idx + 2].run_length = last_header_oversize;
    }
    pub fn delete_n_fwd(&mut self, n: RunLength) {
        if n == 0 {
            return;
        }
        let mut drops_rem = n;
        let mut last = true;
        // we have to initialize these because rust can't know that we
        // eliminated the empty range through the check above
        let mut last_header_idx = 0;
        let mut first_header_oversize = 0;
        let mut last_header_oversize = 0;
        while let Some(range) = self.iter.typed_range_bwd(drops_rem, 0) {
            let header_idx = unsafe {
                (range.headers.last().unwrap_unchecked() as *const FieldValueHeader)
                    .offset_from(iter.fd.header.as_ptr()) as usize
            };
            if last {
                last_header_idx = header_idx;
                last_header_oversize = range.first_header_run_length_oversize;
                last = false;
            }
            drops_rem -= range.field_count;
            for i in 0..range.headers.len() {
                iter.fd.header[i].set_deleted(true);
            }
            first_header_oversize = range.first_header_run_length_oversize;
        }
        debug_assert!(drops_rem == 0);

        let first_header_idx = iter.get_next_header_index();
        iter.fd.adjust_deletion_header_bounds(
            first_header_idx,
            first_header_oversize,
            last_header_idx,
            last_header_oversize,
        );
        iter
    }
    fn add_drop(&mut self, rl: RunLength) {}
    fn add_dup(&mut self, rl: RunLength) {}
    fn build_copy_ranges(&mut self) {
        self.cb.copy_ranges.clear();
        self.cb.insertions.clear();
    }

    pub fn execute(self) {}
}
