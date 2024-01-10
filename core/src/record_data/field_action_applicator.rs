use crate::{
    record_data::field_action::FieldActionKind, utils::temp_vec::transmute_vec,
};

use super::{
    field_action::FieldAction,
    field_value_repr::{
        FieldDataBuffer, FieldValueFormat, FieldValueHeader, FieldValueRepr,
        RunLength,
    },
    iter_hall::IterState,
};

struct InsertionCommand {
    index: usize,
    value: FieldValueHeader,
}
struct CopyCommand {
    source: usize,
    target: usize,
    len: usize,
}

#[derive(Default)]
pub(super) struct FieldActionApplicator {
    copies: Vec<CopyCommand>,
    insertions: Vec<InsertionCommand>,
    iters: Vec<&'static mut FieldAction>,
}

struct FieldActionApplicationState {
    header_idx: usize,
    field_pos: usize,
    header_idx_new: usize,
    copy_range_start: usize,
    copy_range_start_new: usize,
    field_pos_old: usize,
    data_end: usize,
    curr_header_iter_count: usize,
    curr_header_original_rl: RunLength,
    curr_action_kind: FieldActionKind,
    curr_action_pos: usize,
    curr_action_run_length: usize,
}

impl FieldActionApplicator {
    fn push_copy_command(&mut self, faas: &mut FieldActionApplicationState) {
        let copy_len = faas.header_idx_new - faas.copy_range_start_new;
        if copy_len > 0 && faas.copy_range_start_new > 0 {
            self.copies.push(CopyCommand {
                source: faas.copy_range_start,
                target: faas.copy_range_start_new,
                len: copy_len,
            });
        }
        faas.copy_range_start += copy_len;
        faas.copy_range_start_new += copy_len;
    }
    fn push_insert_command(
        &mut self,
        faas: &mut FieldActionApplicationState,
        fmt: FieldValueFormat,
        run_length: RunLength,
    ) {
        faas.header_idx_new += 1;
        self.insertions.push(InsertionCommand {
            index: faas.copy_range_start_new,
            value: FieldValueHeader { fmt, run_length },
        });
        faas.copy_range_start_new += 1;
    }
    fn push_insert_command_if_rl_gt_0(
        &mut self,
        faas: &mut FieldActionApplicationState,
        mut fmt: FieldValueFormat,
        run_length: RunLength,
    ) {
        if run_length == 0 {
            return;
        }
        if run_length == 1 {
            fmt.set_shared_value(true);
        }
        self.push_insert_command(faas, fmt, run_length);
    }
    fn iters_adjust_drop_before(
        &self,
        curr_header_iter_count: usize,
        iterators: &mut [&mut IterState],
        field_pos: usize,
        amount: RunLength,
    ) {
        for it in &mut iterators[0..curr_header_iter_count] {
            if it.field_pos <= field_pos {
                continue;
            }
            let drops_before = (amount as usize).min(it.field_pos - field_pos);
            it.field_pos -= drops_before;
            it.header_rl_offset -= drops_before as RunLength;
        }
    }
    fn iters_after_offset_to_next_header_bumping_field_pos(
        &self,
        curr_header_iter_count: usize,
        offset: RunLength,
        extra_field_pos_bump: usize,
        header_pos_bump: usize,
        iterators: &mut [&mut IterState],
        current_header: &FieldValueHeader,
    ) {
        let data_offset = current_header.total_size_unique();
        let start = iterators.len() - curr_header_iter_count;
        for it in &mut iterators[start..] {
            if it.header_rl_offset <= offset {
                continue;
            }
            it.field_pos += extra_field_pos_bump;
            it.header_idx += header_pos_bump;
            it.data += data_offset;
            it.header_rl_offset -= current_header.run_length;
        }
    }
    fn iters_to_next_header(
        &self,
        curr_header_iter_count: usize,
        iterators: &mut [&mut IterState],
        current_header: &FieldValueHeader,
    ) {
        let data_offset = current_header.total_size_unique();
        let start = iterators.len() - curr_header_iter_count;
        for it in &mut iterators[start..] {
            it.header_idx += 1;
            it.data += data_offset;
            it.header_rl_offset -= current_header.run_length;
        }
    }
    fn iters_to_next_header_zero_offset(
        &self,
        curr_header_iter_count: usize,
        iterators: &mut [&mut IterState],
        current_header: &FieldValueHeader,
    ) {
        let data_offset = current_header.total_size_unique();
        let start = iterators.len() - curr_header_iter_count;
        for it in &mut iterators[start..] {
            it.field_pos -= it.header_rl_offset as usize;
            it.header_idx += 1;
            it.data += data_offset;
            it.header_rl_offset = 0;
        }
    }
    fn iters_to_next_header_adjusting_deleted_offset(
        &self,
        curr_header_iter_count: usize,
        iterators: &mut [&mut IterState],
        current_header: &FieldValueHeader,
    ) {
        let data_offset = current_header.total_size_unique();
        let start = iterators.len() - curr_header_iter_count;
        for it in &mut iterators[start..] {
            it.header_idx += 1;
            it.data += data_offset;
            if it.header_rl_offset < current_header.run_length {
                it.field_pos -= it.header_rl_offset as usize;
                it.header_rl_offset = 0;
            } else {
                it.field_pos -= current_header.run_length as usize;
                it.header_rl_offset -= current_header.run_length;
            }
        }
    }

    fn handle_zst_inserts(
        &mut self,
        header: &mut FieldValueHeader,
        iterators: &mut [&mut IterState],
        faas: &mut FieldActionApplicationState,
        zst_repr: FieldValueRepr,
    ) {
        if header.fmt.repr == zst_repr {
            return self.handle_dup(header, iterators, faas);
        }
        let insert_count = faas.curr_action_run_length;
        let pre = (faas.curr_action_pos - faas.field_pos) as RunLength;
        let mut mid_full_count = insert_count / RunLength::MAX as usize;
        let mut mid_rem =
            (insert_count % RunLength::MAX as usize) as RunLength;
        let post = header.run_length - pre;
        if mid_rem == 0 && post == 0 {
            mid_full_count -= 1; // must be > 0 because `insert_count` != 0
            mid_rem = RunLength::MAX;
        }

        self.push_copy_command(faas);
        self.push_insert_command_if_rl_gt_0(faas, header.fmt, pre);
        faas.field_pos += pre as usize;
        self.iters_after_offset_to_next_header_bumping_field_pos(
            faas.curr_header_iter_count,
            pre,
            insert_count,
            (pre > 0) as usize + mid_full_count,
            iterators,
            &FieldValueHeader {
                fmt: header.fmt,
                run_length: pre,
            },
        );
        let mut fmt_mid = header.fmt;
        if fmt_mid.shared_value() && pre > 0 {
            fmt_mid.set_same_value_as_previous(true);
            header.set_same_value_as_previous(true);
        }
        fmt_mid.set_shared_value(true);
        fmt_mid.repr = zst_repr;

        if mid_full_count != 0 {
            for _ in 0..mid_full_count {
                self.push_insert_command(faas, fmt_mid, RunLength::MAX);
                fmt_mid.set_same_value_as_previous(true);
            }
            faas.field_pos += mid_full_count * RunLength::MAX as usize;

            if mid_rem == 0 {
                header.run_length = post;
                header.set_shared_value(post == 1);
                return;
            }
        }

        if post == 0 {
            header.run_length = mid_rem;
            header.fmt = fmt_mid;
            return;
        }

        self.push_insert_command_if_rl_gt_0(faas, fmt_mid, mid_rem);
        faas.field_pos += mid_rem as usize;
        header.run_length = post;
        header.set_shared_value(post == 1);
    }

    fn handle_dup(
        &mut self,
        header: &mut FieldValueHeader,
        iterators: &mut [&mut IterState],
        faas: &mut FieldActionApplicationState,
    ) {
        // TODO: handle padding correctly and create tests for that
        let dup_count = faas.curr_action_run_length;
        let pre = (faas.curr_action_pos - faas.field_pos) as RunLength;
        if header.shared_value() {
            let start = iterators.len() - faas.curr_header_iter_count;
            for it in &mut iterators[start..] {
                if it.header_rl_offset <= pre {
                    // iterators are sorted backwards
                    // all later (-> smaller offset) ones will be unaffected
                    break;
                }
                it.field_pos += dup_count;
            }
            let mut rl_res = header.run_length as usize + dup_count;
            if rl_res > RunLength::MAX as usize {
                self.push_copy_command(faas);
                while rl_res > RunLength::MAX as usize {
                    self.push_insert_command(faas, header.fmt, RunLength::MAX);
                    header.set_same_value_as_previous(true);
                    faas.field_pos += RunLength::MAX as usize;
                    rl_res -= RunLength::MAX as usize;
                }
            }
            header.run_length = rl_res as RunLength;
            return;
        }

        let mid_full_count = (dup_count + 1) / RunLength::MAX as usize;
        let mid_rem = ((dup_count + 1)
            - (mid_full_count * RunLength::MAX as usize))
            as RunLength;
        let post = (header.run_length - pre).saturating_sub(1);
        self.push_copy_command(faas);
        self.push_insert_command_if_rl_gt_0(faas, header.fmt, pre);
        faas.field_pos += pre as usize;
        self.iters_after_offset_to_next_header_bumping_field_pos(
            faas.curr_header_iter_count,
            // only move iterators that refer to the element *after* the one
            // we are duping, so not pre and mid
            pre + 1,
            dup_count,
            (pre > 0) as usize + mid_full_count,
            iterators,
            &FieldValueHeader {
                fmt: header.fmt,
                run_length: pre,
            },
        );
        if post == 0 && mid_full_count == 0 {
            header.run_length = mid_rem;
            header.set_shared_value(true);
            return;
        }
        let mut fmt_mid = header.fmt;
        fmt_mid.set_shared_value(true);
        if mid_full_count != 0 {
            for _ in 0..mid_full_count {
                self.push_insert_command(faas, fmt_mid, RunLength::MAX);
                fmt_mid.set_same_value_as_previous(true);
            }
        }

        faas.field_pos += mid_full_count * RunLength::MAX as usize;
        if mid_rem == 0 {
            header.run_length = post;
            header.set_shared_value(post == 1);
            return;
        }

        if post == 0 {
            header.run_length = mid_rem;
            header.fmt = fmt_mid;
            return;
        }
        self.push_insert_command_if_rl_gt_0(faas, fmt_mid, mid_rem);
        faas.field_pos += mid_rem as usize;
        header.run_length = post;
        header.set_shared_value(post == 1);
    }
    fn handle_drop(
        &mut self,
        header: &mut FieldValueHeader,
        iterators: &mut [&mut IterState],
        faas: &mut FieldActionApplicationState,
    ) {
        let drop_count = faas.curr_action_run_length;
        let rl_pre = (faas.curr_action_pos - faas.field_pos) as RunLength;
        if rl_pre > 0 {
            let rl_rem = header.run_length - rl_pre;
            if header.shared_value() && drop_count <= rl_rem as usize {
                let rl_to_del = drop_count as RunLength;
                header.run_length -= rl_to_del;
                faas.curr_action_run_length = 0;
                self.iters_adjust_drop_before(
                    faas.curr_header_iter_count,
                    iterators,
                    faas.field_pos,
                    rl_to_del,
                );
                return;
            }
            self.push_copy_command(faas);
            self.push_insert_command_if_rl_gt_0(faas, header.fmt, rl_pre);
            self.iters_to_next_header(
                faas.curr_header_iter_count,
                iterators,
                &FieldValueHeader {
                    fmt: header.fmt,
                    run_length: rl_pre,
                },
            );
            faas.field_pos += rl_pre as usize;
            header.run_length -= rl_pre;
            if drop_count <= rl_rem as usize {
                let rl_to_del = drop_count as RunLength;
                debug_assert!(!header.shared_value());
                if rl_to_del == rl_rem {
                    header.set_deleted(true);
                    faas.curr_action_run_length = 0;
                    self.iters_to_next_header_zero_offset(
                        faas.curr_header_iter_count,
                        iterators,
                        header,
                    );
                    return;
                }
                let mut fmt_del = header.fmt;
                fmt_del.set_deleted(true);
                self.push_insert_command_if_rl_gt_0(faas, fmt_del, rl_to_del);
                header.run_length -= rl_to_del;
                self.iters_to_next_header_adjusting_deleted_offset(
                    faas.curr_header_iter_count,
                    iterators,
                    &FieldValueHeader {
                        fmt: header.fmt,
                        run_length: rl_pre,
                    },
                );
                if header.run_length == 1 {
                    header.set_shared_value(true);
                }
                faas.curr_action_run_length = 0;
                return;
            }
            header.set_deleted(true);
            self.iters_to_next_header_adjusting_deleted_offset(
                faas.curr_header_iter_count,
                iterators,
                header,
            );
            if header.shared_value() {
                faas.copy_range_start += 1;
                faas.copy_range_start_new += 1;
                faas.curr_action_run_length -= rl_rem as usize;
                return;
            }
            faas.curr_action_run_length -= rl_rem as usize;
            return;
        }
        if drop_count > header.run_length as usize {
            header.set_deleted(true);
            faas.curr_action_run_length -= header.run_length as usize;
            self.iters_to_next_header_adjusting_deleted_offset(
                faas.curr_header_iter_count,
                iterators,
                header,
            );
            return;
        }
        // otherwise the if statement above must be true
        let rl_to_del = drop_count as RunLength;
        faas.curr_action_run_length = 0;
        if rl_to_del == header.run_length {
            header.set_deleted(true);
            self.iters_to_next_header_zero_offset(
                faas.curr_header_iter_count,
                iterators,
                header,
            );
            return;
        }
        if !header.shared_value() {
            self.push_copy_command(faas);
            let mut fmt_del = header.fmt;
            fmt_del.set_deleted(true);
            self.push_insert_command_if_rl_gt_0(faas, fmt_del, rl_to_del);
            header.run_length -= rl_to_del;
            self.iters_to_next_header_adjusting_deleted_offset(
                faas.curr_header_iter_count,
                iterators,
                &FieldValueHeader {
                    fmt: header.fmt,
                    run_length: rl_to_del,
                },
            );
            return;
        }
        self.iters_adjust_drop_before(
            faas.curr_header_iter_count,
            iterators,
            faas.field_pos,
            rl_to_del,
        );
        header.run_length -= rl_to_del;
    }
    fn update_current_iters(
        &self,
        iterators: &mut Vec<&mut IterState>,
        faas: &mut FieldActionApplicationState,
    ) {
        while faas.curr_header_iter_count > 0 {
            let it = &iterators.last().unwrap();
            if it.field_pos < faas.curr_action_pos {
                iterators.pop();
                faas.curr_header_iter_count -= 1;
                continue;
            }
            break;
        }
    }

    // returns the field_count delta
    fn generate_commands_from_actions<'a>(
        &mut self,
        actions: impl Iterator<Item = &'a FieldAction>,
        headers: &mut Vec<FieldValueHeader>,
        data: Option<&mut FieldDataBuffer>,
        iterators: &mut Vec<&mut IterState>,
    ) -> isize {
        if headers.is_empty() {
            debug_assert!(actions
                .copied()
                .all(|a| a.kind == FieldActionKind::Drop));
            debug_assert!(data.map(|d| d.is_empty()).unwrap_or(true));
            return 0;
        }
        let mut faas = FieldActionApplicationState {
            header_idx: 0,
            field_pos: 0,
            header_idx_new: 0,
            copy_range_start: 0,
            copy_range_start_new: 0,
            field_pos_old: 0,
            data_end: 0,
            curr_header_iter_count: 0,
            curr_header_original_rl: headers
                .first()
                .map(|h| h.effective_run_length())
                .unwrap_or(0),
            curr_action_kind: FieldActionKind::Dup,
            curr_action_run_length: 0,
            curr_action_pos: 0,
        };
        for it in iterators.iter().rev() {
            if it.header_idx != faas.header_idx {
                break;
            }
            faas.curr_header_iter_count += 1;
        }
        let mut actions = actions.peekable();
        loop {
            let Some(action) = actions.next().copied() else {
                break;
            };
            faas.curr_action_kind = action.kind;
            faas.curr_action_pos = action.field_idx;
            faas.curr_action_run_length = action.run_len as usize;
            debug_assert!(
                faas.curr_action_pos >= faas.field_pos,
                "overlapping field actions"
            );
            while let Some(next_action) = actions.peek() {
                if next_action.field_idx != faas.curr_action_pos {
                    break;
                }
                if next_action.kind != faas.curr_action_kind {
                    // if we have a dup on an index, we won't drop
                    // that index or insert at its position later,
                    // as stated by the `FieldAction` invariants
                    debug_assert!(
                        faas.curr_action_kind != FieldActionKind::Dup
                    );
                    break;
                }
                faas.curr_action_run_length += next_action.run_len as usize;
                actions.next();
            }
            loop {
                self.move_header_idx_to_action_pos(
                    headers, iterators, &mut faas,
                );
                if faas.header_idx == headers.len() {
                    faas.header_idx -= 1;
                    break;
                }
                match faas.curr_action_kind {
                    FieldActionKind::Dup => {
                        self.handle_dup(
                            &mut headers[faas.header_idx],
                            iterators,
                            &mut faas,
                        );
                        faas.curr_action_pos += faas.curr_action_run_length;
                        self.update_current_iters(iterators, &mut faas);
                        break;
                    }
                    FieldActionKind::InsertGroupSeparator => {
                        self.handle_zst_inserts(
                            &mut headers[faas.header_idx],
                            iterators,
                            &mut faas,
                            FieldValueRepr::GroupSeparator,
                        );
                        faas.curr_action_pos += faas.curr_action_run_length;
                        self.update_current_iters(iterators, &mut faas);
                        break;
                    }
                    FieldActionKind::Drop => {
                        self.handle_drop(
                            &mut headers[faas.header_idx],
                            iterators,
                            &mut faas,
                        );
                        if faas.curr_action_run_length == 0 {
                            break;
                        }
                    }
                }
            }
        }
        let headers_rem = headers.len() - faas.header_idx;
        faas.header_idx_new += headers_rem;
        if headers_rem > 0 {
            faas.field_pos +=
                headers[faas.header_idx].effective_run_length() as usize;
            faas.field_pos_old += faas.curr_header_original_rl as usize;
            let header_idx_delta = faas.header_idx_new as isize
                - (faas.header_idx + headers_rem) as isize;
            let field_pos_delta =
                faas.field_pos as isize - faas.field_pos_old as isize;
            let iters_len = iterators.len();
            for it in
                &mut iterators[0..iters_len - faas.curr_header_iter_count]
            {
                it.field_pos =
                    (it.field_pos as isize + field_pos_delta) as usize;
                it.header_idx =
                    (it.header_idx as isize + header_idx_delta) as usize;
            }
        } else if let Some(data) = data {
            // if we touched all headers, there is a chance that the final
            // headers are deleted
            while faas.header_idx > 0 {
                let h = headers[faas.header_idx - 1];
                if !h.deleted() {
                    break;
                }
                faas.header_idx_new -= 1;
                faas.header_idx -= 1;
                if !h.same_value_as_previous() {
                    faas.data_end -= h.total_size();
                }
            }
            data.truncate(faas.data_end);
        }
        self.push_copy_command(&mut faas);
        faas.field_pos as isize - faas.field_pos_old as isize
    }

    fn move_header_idx_to_action_pos(
        &mut self,
        headers: &mut Vec<FieldValueHeader>,
        iterators: &mut Vec<&mut IterState>,
        faas: &mut FieldActionApplicationState,
    ) {
        let mut header;
        loop {
            header = &mut headers[faas.header_idx];
            if !header.deleted() {
                let field_pos_new =
                    faas.field_pos + header.run_length as usize;
                if field_pos_new > faas.curr_action_pos {
                    break;
                }
                faas.field_pos = field_pos_new;
            }
            faas.field_pos_old += faas.curr_header_original_rl as usize;
            if !header.same_value_as_previous() {
                faas.data_end += header.total_size();
            }
            let field_pos_delta =
                faas.field_pos as isize - faas.field_pos_old as isize;
            iterators.drain(iterators.len() - faas.curr_header_iter_count..);
            let len = iterators.len();
            faas.curr_header_iter_count = 0;
            while len > faas.curr_header_iter_count {
                let it = &mut iterators[len - faas.curr_header_iter_count - 1];
                if it.header_idx != faas.header_idx + 1 {
                    break;
                }
                it.field_pos =
                    (it.field_pos as isize + field_pos_delta) as usize;
                it.header_idx += faas.header_idx_new - faas.header_idx;
                faas.curr_header_iter_count += 1;
            }
            faas.header_idx += 1;
            // this can happen if the field is too short (has)
            // implicit nulls at the end
            if faas.header_idx == headers.len() {
                return;
            }
            faas.curr_header_original_rl =
                headers[faas.header_idx].effective_run_length();
            faas.header_idx_new += 1;
        }
        self.update_current_iters(iterators, faas);
    }

    fn execute_commands(&mut self, headers: &mut Vec<FieldValueHeader>) {
        if self.copies.is_empty() && self.insertions.is_empty() {
            return;
        }
        let new_size = self
            .insertions
            .last()
            .map(|i| i.index + 1)
            .unwrap_or(0)
            .max(self.copies.last().map(|c| c.target + c.len).unwrap());
        headers.reserve(new_size - headers.len());

        let header_ptr = headers.as_mut_ptr();
        // PERF: it *might* be faster to interleave the insertions and copies
        // for better cache utilization
        unsafe {
            for c in self.copies.iter().rev() {
                std::ptr::copy(
                    header_ptr.add(c.source),
                    header_ptr.add(c.target),
                    c.len,
                );
            }
            for i in self.insertions.iter() {
                (*header_ptr.add(i.index)) = i.value;
            }
            headers.set_len(new_size);
        }
    }

    pub fn run<'a>(
        &mut self,
        actions: impl Iterator<Item = &'a FieldAction>,
        headers: &mut Vec<FieldValueHeader>,
        data: Option<&mut FieldDataBuffer>,
        field_count: &mut usize,
        iterators: impl Iterator<Item = &'a mut IterState>,
    ) -> isize {
        let mut iters = transmute_vec(std::mem::take(&mut self.iters));
        iters.extend(iterators);
        // we sort and use this backwards so we can pop() the ones
        // we handled
        iters.sort_by(|lhs, rhs| lhs.field_pos.cmp(&rhs.field_pos).reverse());
        let field_count_delta = self.generate_commands_from_actions(
            actions, headers, data, &mut iters,
        );
        debug_assert!(*field_count as isize + field_count_delta >= 0);
        *field_count = (*field_count as isize + field_count_delta) as usize;
        self.iters = transmute_vec(iters);
        self.execute_commands(headers);
        self.insertions.clear();
        self.copies.clear();
        field_count_delta
    }
}

#[cfg(test)]
mod test {
    use crate::record_data::{
        field_action::{FieldAction, FieldActionKind},
        field_action_applicator::FieldActionApplicator,
        field_value::FieldValue,
        field_value_repr::{FieldData, FixedSizeFieldValueType, RunLength},
        iter_hall::IterState,
        iters::FieldIterator,
        push_interface::PushInterface,
    };
    use FieldActionKind as FAK;

    fn test_actions_on_range_raw(
        input: impl Iterator<Item = (FieldValue, RunLength)>,
        header_rle: bool,
        value_rle: bool,
        actions: &[FieldAction],
        output: &[(FieldValue, RunLength)],
        iter_states_in: &[IterState],
        iter_states_out: &[IterState],
    ) {
        let mut fd = FieldData::default();
        let mut len_before = 0;
        for (v, rl) in input {
            fd.push_field_value_unpacked(
                v,
                rl as usize,
                header_rle,
                value_rle,
            );
            len_before += 1;
        }
        let mut faa = FieldActionApplicator::default();
        let mut iter_states = iter_states_in.to_vec();
        let fc_delta = faa.run(
            actions.iter(),
            &mut fd.headers,
            Some(&mut fd.data),
            &mut fd.field_count,
            iter_states.iter_mut(),
        );
        let mut iter = fd.iter();
        let mut results = Vec::new();
        while let Some(field) = iter.typed_field_fwd(usize::MAX) {
            results
                .push((field.value.to_field_value(), field.header.run_length));
        }
        assert_eq!(results, output);
        assert_eq!(iter_states, iter_states_out);
        let expected_field_count_delta =
            output.iter().map(|(_v, rl)| *rl as isize).sum::<isize>()
                - len_before;
        assert_eq!(fc_delta, expected_field_count_delta);
    }
    fn test_actions_on_range_single_type<T: FixedSizeFieldValueType>(
        input: impl Iterator<Item = T>,
        header_rle: bool,
        value_rle: bool,
        actions: &[FieldAction],
        output: &[(T, RunLength)],
        iter_states_in: &[IterState],
        iter_states_out: &[IterState],
    ) {
        let output_fv = output
            .iter()
            .map(|(v, rl)| (FieldValue::from_fixed_sized_type(v.clone()), *rl))
            .collect::<Vec<_>>();
        test_actions_on_range_raw(
            input.map(|v| (FieldValue::from_fixed_sized_type(v), 1)),
            header_rle,
            value_rle,
            actions,
            &output_fv,
            iter_states_in,
            iter_states_out,
        );
    }
    fn test_actions_on_range<T: FixedSizeFieldValueType>(
        input: impl Iterator<Item = T>,
        actions: &[FieldAction],
        output: &[(T, RunLength)],
    ) {
        test_actions_on_range_single_type(
            input,
            true,
            true,
            actions,
            output,
            &[],
            &[],
        );
    }
    fn test_actions_on_range_no_rle(
        input: impl Iterator<Item = i64>,
        actions: &[FieldAction],
        output: &[(i64, RunLength)],
    ) {
        test_actions_on_range_single_type(
            input,
            false,
            false,
            actions,
            output,
            &[],
            &[],
        );
    }

    #[test]
    fn basic_dup() {
        //  Before  Dup(0, 2)
        //    0         0
        //    1         0
        //    2         0
        //              1
        //              2
        test_actions_on_range(
            0i64..3,
            &[FieldAction::new(FAK::Dup, 0, 2)],
            &[(0, 3), (1, 1), (2, 1)],
        );
    }

    #[test]
    fn basic_insert() {
        //  Before  Dup(0, 2)
        //    0         0
        //    1         1
        //    1         GS
        //    1         1
        //    2         2
        test_actions_on_range_raw(
            [0, 1, 1, 1, 2].iter().map(|v| (FieldValue::Int(*v), 1)),
            true,
            true,
            &[FieldAction::new(FAK::InsertGroupSeparator, 2, 1)],
            &[
                (FieldValue::Int(0), 1),
                (FieldValue::Int(1), 1),
                (FieldValue::GroupSeparator, 1),
                (FieldValue::Int(1), 1),
                (FieldValue::Int(2), 1),
            ],
            &[],
            &[],
        );
    }

    // we are testing for a debug assertion to trigger
    // so we can't use this in release mode
    #[cfg(debug_assertions)]
    #[test]
    #[should_panic = "overlapping field actions"]
    fn drop_within_dup() {
        //  Before  Dup(0, 2)  Drop(1, 1)
        //    0         0           0
        //    1         0           0
        //    2         0           1
        //              1           2
        //              2
        test_actions_on_range(
            0i64..3,
            &[
                FieldAction::new(FAK::Dup, 0, 2),
                FieldAction::new(FAK::Drop, 1, 1),
            ],
            &[(0, 2), (1, 1), (2, 1)],
        );
    }

    #[test]
    fn in_between_drop() {
        //  Before   Drop(1, 1)
        //    0           0
        //    1           2
        //    2
        test_actions_on_range(
            0i64..3,
            &[FieldAction::new(FAK::Drop, 1, 1)],
            &[(0, 1), (2, 1)],
        );
    }

    #[test]
    fn pure_run_length_drop() {
        //  Before   Drop(1, 1)
        //    0           0
        //    0           0
        //    0
        test_actions_on_range(
            std::iter::repeat(0i64).take(3),
            &[FieldAction::new(FAK::Drop, 1, 1)],
            &[(0, 2)],
        );
        test_actions_on_range_no_rle(
            std::iter::repeat(0i64).take(3),
            &[FieldAction::new(FAK::Drop, 1, 1)],
            &[(0, 1), (0, 1)],
        );
    }
}
