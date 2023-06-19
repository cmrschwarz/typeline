use crate::field_data::fd_iter::FDIterator;

use super::{fd_iter::FDIterMut, FieldData, FieldValueFormat, FieldValueHeader, RunLength};

pub enum FieldActionKind {
    Dup,
    Drop,
}
pub struct FieldAction {
    pub kind: FieldActionKind,
    pub run_len: RunLength,
    pub field_idx: usize,
}

pub struct InsertionCommand {
    index: usize,
    value: FieldValueHeader,
}
pub struct CopyCommand {
    source: usize,
    target: usize,
    len: usize,
}

#[derive(Default)]
pub struct FieldCommandBuffer {
    pub actions: Vec<FieldAction>,
    copies: Vec<CopyCommand>,
    insertions: Vec<InsertionCommand>,
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
    pub fn push_action(&mut self, kind: FieldActionKind, field_idx: usize, run_length: RunLength) {
        self.actions.push(FieldAction {
            kind: FieldActionKind::Drop,
            field_idx,
            run_len: run_length,
        });
    }
    pub fn execute(&mut self, fd: &mut FieldData) {
        self.execute_from_iter(FDIterMut::from_start(fd));
        self.execute_from_header_index_and_field_position(fd, 0, 0);
    }
    pub fn execute_from_iter<'a>(&mut self, mut iter: FDIterMut<'a>) {
        if self.actions.len() == 0 {
            return;
        }
        let first_idx = self.actions[0].field_idx;
        let iter_idx = iter.get_next_field_pos();
        if iter_idx > first_idx {
            iter.prev_n_fields(iter_idx - first_idx);
        }
        self.execute_from_header_index_and_field_position(
            iter.fd,
            iter.get_next_header_index(),
            iter.get_next_field_pos(),
        );
    }
    fn execute_from_header_index_and_field_position(
        &mut self,
        fd: &mut FieldData,
        mut header_idx: usize,
        mut field_pos: usize,
    ) {
        //stable sort so a dup stays before a drop
        //TODO: make action indices respect previous actions
        self.actions
            .sort_by(|lhs, rhs| lhs.field_idx.cmp(&rhs.field_idx));
        self.insertions.clear();
        self.perform_actions(fd, header_idx, field_pos);
        self.execute_commands(fd);
    }
    fn push_copy_command(
        &mut self,
        header_idx_new: usize,
        copy_range_start: &mut usize,
        copy_range_start_new: &mut usize,
    ) {
        let copy_len = header_idx_new - *copy_range_start_new;
        if copy_len > 0 && *copy_range_start_new > 0 {
            self.copies.push(CopyCommand {
                source: *copy_range_start,
                target: *copy_range_start_new,
                len: copy_len,
            });
        }
        *copy_range_start += copy_len;
        *copy_range_start_new += copy_len;
    }
    fn push_insert_command(
        &mut self,
        header_idx_new: &mut usize,
        copy_range_start_new: &mut usize,
        fmt: FieldValueFormat,
        run_length: RunLength,
    ) {
        *header_idx_new += 1;
        self.insertions.push(InsertionCommand {
            index: *copy_range_start_new,
            value: FieldValueHeader { fmt, run_length },
        });
        *copy_range_start_new += 1;
    }
    fn push_insert_command_check_run_length(
        &mut self,
        header_idx_new: &mut usize,
        copy_range_start_new: &mut usize,
        mut fmt: FieldValueFormat,
        run_length: RunLength,
    ) {
        if run_length == 0 {
            return;
        }
        if run_length == 1 {
            fmt.set_shared_value(true);
        }
        self.push_insert_command(header_idx_new, copy_range_start_new, fmt, run_length);
    }
    fn handle_dup(
        &mut self,
        action: FieldAction,
        header: &mut FieldValueHeader,
        field_pos: &mut usize,
        header_idx_new: &mut usize,
        copy_range_start: &mut usize,
        copy_range_start_new: &mut usize,
    ) {
        if header.shared_value() {
            let mut rl_res = header.run_length as usize + action.run_len as usize;
            if rl_res > RunLength::MAX as usize {
                self.push_copy_command(*header_idx_new, copy_range_start, copy_range_start_new);
                self.push_insert_command(
                    header_idx_new,
                    copy_range_start_new,
                    header.fmt,
                    RunLength::MAX,
                );
                header.set_same_value_as_previous(true);
                *field_pos += RunLength::MAX as usize;
                rl_res -= RunLength::MAX as usize;
            }
            header.run_length = rl_res as RunLength;
            return;
        }
        let pre = (action.field_idx - *field_pos) as RunLength;
        let (mid1, mid2) = if action.run_len == RunLength::MAX {
            (RunLength::MAX, 1)
        } else {
            (action.run_len + 1, 0)
        };
        let post = (header.run_length - pre).saturating_sub(1);
        self.push_copy_command(*header_idx_new, copy_range_start, copy_range_start_new);
        self.push_insert_command_check_run_length(
            header_idx_new,
            copy_range_start_new,
            header.fmt,
            pre,
        );
        *field_pos += pre as usize;
        if post == 0 && mid2 == 0 {
            header.run_length = mid1;
            header.set_shared_value(true);
            return;
        }
        self.push_insert_command_check_run_length(
            header_idx_new,
            copy_range_start_new,
            header.fmt,
            mid1,
        );
        *field_pos += mid1 as usize;
        if mid2 == 0 {
            header.run_length = post;
            header.set_shared_value(post == 1);
            return;
        }
        let mut fmt_mid2 = header.fmt;
        fmt_mid2.set_shared_value(true);
        fmt_mid2.set_same_value_as_previous(true);
        if post == 0 {
            header.run_length = mid2;
            header.fmt = fmt_mid2;
            return;
        }
        self.push_insert_command_check_run_length(
            header_idx_new,
            copy_range_start_new,
            fmt_mid2,
            mid2,
        );
        *field_pos += mid2 as usize;
        header.run_length = post;
        header.set_shared_value(post == 1);
        return;
    }
    fn perform_actions(&mut self, fd: &mut FieldData, mut header_idx: usize, mut field_pos: usize) {
        let mut header = &mut fd.header[header_idx];
        let mut header_idx_new = header_idx;

        let mut action;
        let mut action_idx_next = 0;

        let mut copy_range_start = 0;
        let mut copy_range_start_new = 0;
        'advance_action: loop {
            if action_idx_next == self.actions.len() {
                return;
            }
            action = self.actions[action_idx_next];
            action_idx_next += 1;
            'advance_header: loop {
                loop {
                    if !header.deleted() {
                        let field_pos_new = field_pos + header.run_length as usize;
                        if field_pos_new > action.field_idx {
                            break;
                        }
                        field_pos = field_pos_new;
                    }
                    header_idx += 1;
                    header_idx_new += 1;
                    header = &mut fd.header[header_idx];
                }
                match action.kind {
                    FieldActionKind::Dup => {
                        self.handle_dup(
                            action,
                            header,
                            &mut field_pos,
                            &mut header_idx_new,
                            &mut copy_range_start,
                            &mut copy_range_start_new,
                        );
                        continue 'advance_action;
                    }
                    FieldActionKind::Drop => {
                        let rl_to_del = action.run_len;
                        let rl_pre = (action.field_idx - field_pos) as RunLength;
                        if rl_pre > 0 {
                            let rl_rem = header.run_length - rl_pre;
                            if header.shared_value() && action.run_len <= rl_rem {
                                header.run_length -= action.run_len;
                                continue 'advance_action;
                            }
                            self.push_copy_command(
                                header_idx_new,
                                &mut copy_range_start,
                                &mut copy_range_start_new,
                            );
                            self.push_insert_command_check_run_length(
                                &mut header_idx_new,
                                &mut copy_range_start_new,
                                header.fmt,
                                rl_pre,
                            );
                            field_pos += rl_pre as usize;
                            if action.run_len <= rl_rem {
                                debug_assert!(!header.shared_value());
                                if action.run_len == rl_rem {
                                    header.set_deleted(true);
                                    continue 'advance_action;
                                }
                                let mut fmt_del = header.fmt;
                                fmt_del.set_deleted(true);
                                self.push_insert_command_check_run_length(
                                    &mut header_idx_new,
                                    &mut copy_range_start_new,
                                    fmt_del,
                                    rl_rem,
                                );
                                header.run_length -= action.run_len;
                                if header.run_length == 1 {
                                    header.set_shared_value(true);
                                }
                                continue 'advance_action;
                            }
                            if header.shared_value() {
                                header.set_deleted(true);
                                action.run_len -= rl_rem;
                                copy_range_start += 1;
                                copy_range_start_new += 1;
                                continue 'advance_header;
                            }
                            header.set_deleted(true);
                            if action.run_len == rl_rem {
                                continue 'advance_action;
                            }
                            action.run_len -= rl_rem;
                            continue 'advance_header;
                        }
                        if rl_to_del > header.run_length {
                            header.set_deleted(true);
                            action.run_len -= header.run_length;
                            continue 'advance_header;
                        }
                        if rl_to_del == header.run_length {
                            header.set_deleted(true);
                            continue 'advance_action;
                        }
                        if !header.shared_value() {
                            self.push_copy_command(
                                header_idx_new,
                                &mut copy_range_start,
                                &mut copy_range_start_new,
                            );
                            let mut fmt_del = header.fmt;
                            fmt_del.set_deleted(true);
                            self.push_insert_command_check_run_length(
                                &mut header_idx_new,
                                &mut copy_range_start_new,
                                fmt_del,
                                rl_to_del,
                            );
                            header.run_length -= rl_to_del;
                            continue 'advance_action;
                        }
                        header.run_length -= rl_to_del;
                        continue 'advance_action;
                    }
                }
            }
        }
    }
    fn execute_commands(&mut self, fd: &mut FieldData) {
        let new_size = self
            .insertions
            .last()
            .map(|i| i.index + 1)
            .unwrap_or(0)
            .max(self.copies.last().map(|c| c.target + c.len).unwrap_or(0));
        fd.header.reserve(new_size - fd.header.len());

        let header_ptr = fd.header.as_mut_ptr();
        // PERF: it *might* be faster to interleave the insertions and copies for
        // better cache utilization
        unsafe {
            for i in self.insertions.iter() {
                (*header_ptr.add(i.index)) = i.value;
            }
            for c in self.copies.iter().rev() {
                std::ptr::copy(header_ptr.add(c.source), header_ptr.add(c.target), c.len);
            }
            fd.header.set_len(new_size);
        }
    }
}
