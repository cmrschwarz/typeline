use crate::{
    record_data::field_action::FieldActionKind, utils::temp_vec::transmute_vec,
};

use super::{
    field_action::FieldAction,
    field_data::{
        FieldDataBuffer, FieldValueFormat, FieldValueHeader, RunLength,
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

impl FieldActionApplicator {
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
        self.push_insert_command(
            header_idx_new,
            copy_range_start_new,
            fmt,
            run_length,
        );
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
    fn iters_to_next_header(
        &self,
        curr_header_iter_count: usize,
        iterators: &mut [&mut IterState],
        header_to_skip: &FieldValueHeader,
    ) {
        let data_offset = header_to_skip.total_size_unique();
        let start = iterators.len() - curr_header_iter_count;
        for it in &mut iterators[start..] {
            it.header_idx += 1;
            it.data += data_offset;
            it.header_rl_offset -= header_to_skip.run_length;
        }
    }
    fn iters_to_next_header_zero_offset(
        &self,
        curr_header_iter_count: usize,
        iterators: &mut [&mut IterState],
        header_to_skip: &FieldValueHeader,
    ) {
        let data_offset = header_to_skip.total_size_unique();
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
        header_to_skip: &FieldValueHeader,
    ) {
        let data_offset = header_to_skip.total_size_unique();
        let start = iterators.len() - curr_header_iter_count;
        for it in &mut iterators[start..] {
            it.header_idx += 1;
            it.data += data_offset;
            if it.header_rl_offset < header_to_skip.run_length {
                it.field_pos -= it.header_rl_offset as usize;
                it.header_rl_offset = 0;
            } else {
                it.field_pos -= header_to_skip.run_length as usize;
                it.header_rl_offset -= header_to_skip.run_length;
            }
        }
    }

    fn handle_dup(
        &mut self,
        field_idx: usize,
        run_len: usize,
        header: &mut FieldValueHeader,
        field_pos: &mut usize,
        header_idx_new: &mut usize,
        copy_range_start: &mut usize,
        copy_range_start_new: &mut usize,
        curr_header_iter_count: usize,
        iterators: &mut [&mut IterState],
    ) {
        if header.shared_value() {
            // iterators are unaffected in this case
            let mut rl_res = header.run_length as usize + run_len;
            if rl_res > RunLength::MAX as usize {
                self.push_copy_command(
                    *header_idx_new,
                    copy_range_start,
                    copy_range_start_new,
                );
                while rl_res > RunLength::MAX as usize {
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
            }
            header.run_length = rl_res as RunLength;
            return;
        }
        let pre = (field_idx - *field_pos) as RunLength;
        let mid_full_count = (run_len + 1) / RunLength::MAX as usize;
        let mid_rem = ((run_len + 1)
            - (mid_full_count * RunLength::MAX as usize))
            as RunLength;
        let post = (header.run_length - pre).saturating_sub(1);
        self.push_copy_command(
            *header_idx_new,
            copy_range_start,
            copy_range_start_new,
        );
        self.push_insert_command_check_run_length(
            header_idx_new,
            copy_range_start_new,
            header.fmt,
            pre,
        );
        *field_pos += pre as usize;
        self.iters_to_next_header(
            curr_header_iter_count,
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
                self.push_insert_command_check_run_length(
                    header_idx_new,
                    copy_range_start_new,
                    fmt_mid,
                    RunLength::MAX,
                );
                fmt_mid.set_same_value_as_previous(true);
            }
        }

        *field_pos += mid_full_count * RunLength::MAX as usize;
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
        self.push_insert_command_check_run_length(
            header_idx_new,
            copy_range_start_new,
            fmt_mid,
            mid_rem,
        );
        *field_pos += mid_rem as usize;
        header.run_length = post;
        header.set_shared_value(post == 1);
    }
    fn handle_drop(
        &mut self,
        action_pos: usize,
        curr_action_pos_outstanding_drops: &mut RunLength,
        header: &mut FieldValueHeader,
        field_pos: &mut usize,
        header_idx_new: &mut usize,
        copy_range_start: &mut usize,
        copy_range_start_new: &mut usize,
        curr_header_iter_count: usize,
        iterators: &mut [&mut IterState],
    ) {
        let rl_to_del = *curr_action_pos_outstanding_drops;
        let rl_pre = (action_pos - *field_pos) as RunLength;
        if rl_pre > 0 {
            let rl_rem = header.run_length - rl_pre;
            if header.shared_value() && rl_to_del <= rl_rem {
                header.run_length -= rl_to_del;
                *curr_action_pos_outstanding_drops = 0;
                self.iters_adjust_drop_before(
                    curr_header_iter_count,
                    iterators,
                    *field_pos,
                    rl_to_del,
                );
                return;
            }
            self.push_copy_command(
                *header_idx_new,
                copy_range_start,
                copy_range_start_new,
            );
            self.push_insert_command_check_run_length(
                header_idx_new,
                copy_range_start_new,
                header.fmt,
                rl_pre,
            );
            self.iters_to_next_header(
                curr_header_iter_count,
                iterators,
                &FieldValueHeader {
                    fmt: header.fmt,
                    run_length: rl_pre,
                },
            );
            *field_pos += rl_pre as usize;
            header.run_length -= rl_pre;
            if rl_to_del <= rl_rem {
                debug_assert!(!header.shared_value());
                if rl_to_del == rl_rem {
                    header.set_deleted(true);
                    *curr_action_pos_outstanding_drops = 0;
                    self.iters_to_next_header_zero_offset(
                        curr_header_iter_count,
                        iterators,
                        header,
                    );
                    return;
                }
                let mut fmt_del = header.fmt;
                fmt_del.set_deleted(true);
                self.push_insert_command_check_run_length(
                    header_idx_new,
                    copy_range_start_new,
                    fmt_del,
                    rl_to_del,
                );
                header.run_length -= rl_to_del;
                self.iters_to_next_header_adjusting_deleted_offset(
                    curr_header_iter_count,
                    iterators,
                    &FieldValueHeader {
                        fmt: header.fmt,
                        run_length: rl_pre,
                    },
                );
                if header.run_length == 1 {
                    header.set_shared_value(true);
                }
                *curr_action_pos_outstanding_drops = 0;
                return;
            }
            header.set_deleted(true);
            self.iters_to_next_header_adjusting_deleted_offset(
                curr_header_iter_count,
                iterators,
                header,
            );
            if header.shared_value() {
                *copy_range_start += 1;
                *copy_range_start_new += 1;
                *curr_action_pos_outstanding_drops -= rl_rem;
                return;
            }
            *curr_action_pos_outstanding_drops -= rl_rem;
            return;
        }
        if rl_to_del > header.run_length {
            header.set_deleted(true);
            *curr_action_pos_outstanding_drops -= header.run_length;
            self.iters_to_next_header_adjusting_deleted_offset(
                curr_header_iter_count,
                iterators,
                header,
            );
            return;
        }
        *curr_action_pos_outstanding_drops = 0;
        if rl_to_del == header.run_length {
            header.set_deleted(true);
            self.iters_to_next_header_zero_offset(
                curr_header_iter_count,
                iterators,
                header,
            );
            return;
        }
        if !header.shared_value() {
            self.push_copy_command(
                *header_idx_new,
                copy_range_start,
                copy_range_start_new,
            );
            let mut fmt_del = header.fmt;
            fmt_del.set_deleted(true);
            self.push_insert_command_check_run_length(
                header_idx_new,
                copy_range_start_new,
                fmt_del,
                rl_to_del,
            );
            header.run_length -= rl_to_del;
            self.iters_to_next_header_adjusting_deleted_offset(
                curr_header_iter_count,
                iterators,
                &FieldValueHeader {
                    fmt: header.fmt,
                    run_length: rl_to_del,
                },
            );
            return;
        }
        self.iters_adjust_drop_before(
            curr_header_iter_count,
            iterators,
            *field_pos,
            rl_to_del,
        );
        header.run_length -= rl_to_del;
    }
    fn update_current_iters(
        &self,
        iterators: &mut Vec<&mut IterState>,
        curr_header_iter_count: &mut usize,
        curr_action_pos: usize,
    ) {
        while *curr_header_iter_count > 0 {
            let it = &iterators.last().unwrap();
            if it.field_pos < curr_action_pos {
                iterators.pop();
                *curr_header_iter_count -= 1;
                continue;
            }
            break;
        }
    }

    // returns the field_count delta
    fn generate_commands_from_actions<'a>(
        &mut self,
        mut actions: impl Iterator<Item = &'a FieldAction>,
        headers: &mut Vec<FieldValueHeader>,
        data: Option<&mut FieldDataBuffer>,
        iterators: &mut Vec<&mut IterState>,
        mut header_idx: usize,
        mut field_pos: usize,
    ) -> isize {
        if headers.is_empty() {
            #[cfg(debug_assertions)]
            {
                debug_assert!(actions.all(|a| a.kind == FieldActionKind::Drop));
            }
            if let Some(data) = data {
                data.clear();
            }
            return 0;
        }
        let mut header;
        let mut header_idx_new = header_idx;

        let mut copy_range_start = 0;
        let mut copy_range_start_new = 0;
        let mut field_pos_old = field_pos;
        let mut curr_action_pos = 0;
        let mut curr_action_pos_outstanding_dups = 0;
        let mut curr_action_pos_outstanding_drops = 0;
        let mut curr_header_original_rl = headers
            .first()
            .map(|h| h.effective_run_length())
            .unwrap_or(0);
        let mut data_end = 0;
        let mut curr_header_iter_count = 0;

        for it in iterators.iter().rev() {
            if it.header_idx != header_idx {
                break;
            }
            curr_header_iter_count += 1;
        }
        let mut actions = actions.peekable();

        'advance_action: loop {
            loop {
                let Some(&action) = actions.peek() else {
                    if curr_action_pos_outstanding_dups > 0 {
                        break;
                    }
                    break 'advance_action;
                };
                match action.kind {
                    FieldActionKind::Dup => {
                        if action.field_idx != curr_action_pos {
                            if curr_action_pos_outstanding_dups > 0 {
                                break;
                            }
                            curr_action_pos = action.field_idx;
                        }
                        curr_action_pos_outstanding_dups +=
                            action.run_len as usize;
                    }
                    FieldActionKind::Drop => {
                        if curr_action_pos_outstanding_dups == 0 {
                            curr_action_pos = action.field_idx;
                            curr_action_pos_outstanding_drops = action.run_len;
                            actions.next();
                            break;
                        }
                        let action_gap = action.field_idx - curr_action_pos;
                        if curr_action_pos_outstanding_dups < action_gap {
                            break;
                        }
                        if curr_action_pos_outstanding_dups
                            >= action_gap + action.run_len as usize
                        {
                            curr_action_pos_outstanding_dups -=
                                action.run_len as usize;
                        } else if action_gap == 0 {
                            curr_action_pos_outstanding_dups = 0;
                            curr_action_pos_outstanding_drops = action.run_len
                                - curr_action_pos_outstanding_dups
                                    as RunLength;
                            actions.next();
                            break;
                        } else {
                            curr_action_pos_outstanding_drops = action.run_len
                                - (curr_action_pos_outstanding_dups
                                    - action_gap)
                                    as RunLength;
                            curr_action_pos_outstanding_dups = action_gap;
                        }
                    }
                }
                actions.next();
            }
            'advance_header: loop {
                loop {
                    header = &mut headers[header_idx];
                    if !header.deleted() {
                        let field_pos_new =
                            field_pos + header.run_length as usize;
                        if field_pos_new > curr_action_pos {
                            break;
                        }
                        field_pos = field_pos_new;
                    }
                    field_pos_old += curr_header_original_rl as usize;
                    if !header.same_value_as_previous() {
                        data_end += header.total_size();
                    }
                    let field_pos_delta =
                        field_pos as isize - field_pos_old as isize;
                    iterators
                        .drain(iterators.len() - curr_header_iter_count..);
                    let len = iterators.len();
                    curr_header_iter_count = 0;
                    while len > curr_header_iter_count {
                        let it =
                            &mut iterators[len - curr_header_iter_count - 1];
                        if it.header_idx != header_idx + 1 {
                            break;
                        }
                        it.field_pos =
                            (it.field_pos as isize + field_pos_delta) as usize;
                        it.header_idx += header_idx_new - header_idx;
                        curr_header_iter_count += 1;
                    }
                    if header_idx + 1 == headers.len() {
                        // this can happen if the field is too short (has)
                        // implicit nulls at the end
                        break 'advance_action;
                    }
                    header_idx += 1;
                    curr_header_original_rl =
                        headers[header_idx].effective_run_length();
                    header_idx_new += 1;
                }
                self.update_current_iters(
                    iterators,
                    &mut curr_header_iter_count,
                    curr_action_pos,
                );
                if curr_action_pos_outstanding_dups > 0 {
                    self.handle_dup(
                        curr_action_pos,
                        curr_action_pos_outstanding_dups,
                        header,
                        &mut field_pos,
                        &mut header_idx_new,
                        &mut copy_range_start,
                        &mut copy_range_start_new,
                        curr_header_iter_count,
                        iterators,
                    );
                    let prev_dups = curr_action_pos_outstanding_dups;
                    curr_action_pos_outstanding_dups = 0;
                    if curr_action_pos_outstanding_drops == 0 {
                        continue 'advance_action;
                    }
                    curr_action_pos += prev_dups;
                    self.update_current_iters(
                        iterators,
                        &mut curr_header_iter_count,
                        curr_action_pos,
                    );
                }
                self.handle_drop(
                    curr_action_pos,
                    &mut curr_action_pos_outstanding_drops,
                    header,
                    &mut field_pos,
                    &mut header_idx_new,
                    &mut copy_range_start,
                    &mut copy_range_start_new,
                    curr_header_iter_count,
                    iterators,
                );
                if curr_action_pos_outstanding_drops == 0 {
                    continue 'advance_action;
                } else {
                    continue 'advance_header;
                }
            }
        }
        let headers_rem = headers.len() - header_idx;
        header_idx_new += headers_rem;
        if headers_rem > 0 {
            field_pos += headers[header_idx].run_length as usize;
            field_pos_old += curr_header_original_rl as usize;
            let header_idx_delta =
                header_idx_new as isize - (header_idx + headers_rem) as isize;
            let field_pos_delta = field_pos as isize - field_pos_old as isize;
            let iters_len = iterators.len();
            for it in &mut iterators[0..iters_len - curr_header_iter_count] {
                it.field_pos =
                    (it.field_pos as isize + field_pos_delta) as usize;
                it.header_idx =
                    (it.header_idx as isize + header_idx_delta) as usize;
            }
        } else if let Some(data) = data {
            // if we touched all headers, there is a chance that the final
            // headers are deleted
            while header_idx > 0 {
                let h = headers[header_idx - 1];
                if !h.deleted() {
                    break;
                }
                header_idx_new -= 1;
                header_idx -= 1;
                if !h.same_value_as_previous() {
                    data_end -= h.total_size();
                }
            }
            data.truncate(data_end);
        }
        self.push_copy_command(
            header_idx_new,
            &mut copy_range_start,
            &mut copy_range_start_new,
        );

        field_pos as isize - field_pos_old as isize
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
        header_idx: usize,
        field_pos: usize,
    ) {
        let mut iters = transmute_vec(std::mem::take(&mut self.iters));
        iters.extend(iterators);
        // we sort and use this backwards so we can pop() the ones
        // we handled
        iters.sort_by(|lhs, rhs| lhs.field_pos.cmp(&rhs.field_pos).reverse());
        *field_count = (*field_count as isize
            + self.generate_commands_from_actions(
                actions, headers, data, &mut iters, header_idx, field_pos,
            )) as usize;
        self.iters = transmute_vec(iters);
        self.execute_commands(headers);
        self.insertions.clear();
        self.copies.clear();
    }
}

#[cfg(test)]
mod test {
    use crate::record_data::{
        field_action::{FieldAction, FieldActionKind},
        field_action_applicator::FieldActionApplicator,
        field_data::{FieldData, RunLength},
        iters::FieldIterator,
        push_interface::PushInterface,
        typed::TypedSlice,
        typed_iters::TypedSliceIter,
    };

    fn test_actions_on_range_with_rle_opts(
        input: impl Iterator<Item = i64>,
        header_rle: bool,
        value_rle: bool,
        actions: &[FieldAction],
        output: &[(i64, RunLength)],
    ) {
        let mut fd = FieldData::default();
        for v in input {
            fd.push_int(v, 1, header_rle, value_rle);
        }
        let mut faa = FieldActionApplicator::default();
        faa.run(
            actions.iter(),
            &mut fd.headers,
            Some(&mut fd.data),
            &mut fd.field_count,
            std::iter::empty(),
            0,
            0,
        );
        let mut iter = fd.iter();
        let mut results = Vec::new();
        while let Some(range) = iter.typed_range_fwd(usize::MAX, 0) {
            if let TypedSlice::Integer(ints) = range.data {
                results.extend(
                    TypedSliceIter::from_range(&range, ints)
                        .map(|(i, rl)| (*i, rl)),
                );
            } else {
                panic!("resulting field data has wrong type");
            }
        }
        assert_eq!(results, output);
    }
    fn test_actions_on_range(
        input: impl Iterator<Item = i64>,
        actions: &[FieldAction],
        output: &[(i64, RunLength)],
    ) {
        test_actions_on_range_with_rle_opts(
            input, true, true, actions, output,
        );
    }
    fn test_actions_on_range_no_rle(
        input: impl Iterator<Item = i64>,
        actions: &[FieldAction],
        output: &[(i64, RunLength)],
    ) {
        test_actions_on_range_with_rle_opts(
            input, false, false, actions, output,
        );
    }

    #[test]
    fn drop_after_dup() {
        //  Before  Dup(0, 2)  Drop(1, 1)
        //    0         0           0
        //    1         0           0
        //    2         0           1
        //              1           2
        //              2
        use FieldActionKind::*;
        test_actions_on_range(
            0..3,
            &[FieldAction::new(Dup, 0, 2)],
            &[(0, 3), (1, 1), (2, 1)],
        );
        test_actions_on_range(
            0..3,
            &[FieldAction::new(Dup, 0, 2), FieldAction::new(Drop, 1, 1)],
            &[(0, 2), (1, 1), (2, 1)],
        );
    }
    #[test]
    fn in_between_drop() {
        //  Before   Drop(1, 1)
        //    0           0
        //    1           2
        //    2
        use FieldActionKind::*;
        test_actions_on_range(
            0..3,
            &[FieldAction::new(Drop, 1, 1)],
            &[(0, 1), (2, 1)],
        );
    }

    #[test]
    fn pure_run_length_drop() {
        //  Before   Drop(1, 1)
        //    0           0
        //    0           0
        //    0
        use FieldActionKind::*;
        test_actions_on_range(
            std::iter::repeat(0).take(3),
            &[FieldAction::new(Drop, 1, 1)],
            &[(0, 2)],
        );
        test_actions_on_range_no_rle(
            std::iter::repeat(0).take(3),
            &[FieldAction::new(Drop, 1, 1)],
            &[(0, 1), (0, 1)],
        );
    }
}
