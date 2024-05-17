use std::collections::VecDeque;

use crate::{
    record_data::field_action::FieldActionKind, utils::temp_vec::transmute_vec,
};

use super::{
    field_action::FieldAction,
    field_data::{
        FieldValueFormat, FieldValueHeader, FieldValueRepr, RunLength,
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
    curr_header_iters_start: usize,
    curr_header_iters_end: usize,
    curr_header_original_rl: RunLength,
    curr_action_kind: FieldActionKind,
    curr_action_pos: usize,
    curr_action_run_length: usize,
}

impl FieldActionApplicator {
    fn push_copy_command(&mut self, faas: &mut FieldActionApplicationState) {
        let copy_len = faas.header_idx - faas.copy_range_start;
        if copy_len > 0 && faas.copy_range_start != faas.copy_range_start_new {
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
        debug_assert!(run_length > 0);
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
        faas: &mut FieldActionApplicationState,
        iterators: &mut [&mut IterState],
        field_pos: usize,
        amount: RunLength,
    ) {
        for it in &mut iterators
            [faas.curr_header_iters_start..faas.curr_header_iters_end]
        {
            if it.field_pos <= field_pos {
                continue;
            }
            let drops_before = (amount as usize).min(it.field_pos - field_pos);
            it.field_pos -= drops_before;
            it.header_rl_offset -= drops_before as RunLength;
        }
    }
    fn iters_after_offset_to_next_header_bumping_field_pos(
        faas: &mut FieldActionApplicationState,
        offset: RunLength,
        extra_field_pos_bump: usize,
        header_pos_bump: usize,
        iterators: &mut [&mut IterState],
        current_header: FieldValueHeader,
    ) {
        let data_offset = current_header.total_size_unique();
        for it in &mut iterators
            [faas.curr_header_iters_start..faas.curr_header_iters_end]
            .iter_mut()
            .rev()
        {
            if it.header_rl_offset <= offset {
                break;
            }
            it.field_pos += extra_field_pos_bump;
            it.header_idx += header_pos_bump;
            it.data += data_offset;
            it.header_rl_offset -= current_header.run_length;
        }
    }
    fn iters_to_next_header(
        faas: &mut FieldActionApplicationState,
        iterators: &mut [&mut IterState],
        current_header: FieldValueHeader,
    ) {
        let data_offset = current_header.total_size_unique();
        for it in &mut iterators
            [faas.curr_header_iters_start..faas.curr_header_iters_end]
        {
            it.header_idx += 1;
            it.data += data_offset;
            it.header_rl_offset -= current_header.run_length;
        }
    }
    fn iters_to_next_header_zero_offset(
        faas: &FieldActionApplicationState,
        iterators: &mut [&mut IterState],
        current_header: FieldValueHeader,
    ) {
        let data_offset = current_header.total_size_unique();
        for it in &mut iterators
            [faas.curr_header_iters_start..faas.curr_header_iters_end]
        {
            it.field_pos -= it.header_rl_offset as usize;
            it.header_idx += 1;
            it.data += data_offset;
            it.header_rl_offset = 0;
        }
    }
    fn iters_to_next_header_adjusting_deleted_offset(
        faas: &FieldActionApplicationState,
        iterators: &mut [&mut IterState],
        current_header: FieldValueHeader,
    ) {
        let data_offset = current_header.total_size_unique();
        for it in &mut iterators
            [faas.curr_header_iters_start..faas.curr_header_iters_end]
        {
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
        debug_assert!(zst_repr.is_zst());
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
        let mut pre_fmt = header.fmt;
        pre_fmt.set_shared_value(pre_fmt.shared_value() || pre == 1);
        self.push_insert_command_if_rl_gt_0(faas, header.fmt, pre);
        faas.field_pos += pre as usize;
        Self::iters_after_offset_to_next_header_bumping_field_pos(
            faas,
            pre,
            insert_count,
            usize::from(pre > 0) + mid_full_count,
            iterators,
            FieldValueHeader {
                fmt: header.fmt,
                run_length: pre,
            },
        );
        let mut fmt_mid = header.fmt;
        if pre > 0 {
            fmt_mid.set_leading_padding(0);
        }
        if fmt_mid.shared_value() && pre > 0 {
            fmt_mid.set_same_value_as_previous(true);
            header.set_same_value_as_previous(true);
        } else {
            fmt_mid.size = 0;
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
                header.set_shared_value_if_rl_1();
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
        header.set_shared_value_if_rl_1();
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
            let mut rl_res = header.run_length as usize + dup_count;
            let iterators = iterators
                [faas.curr_header_iters_start..faas.curr_header_iters_end]
                .iter_mut()
                .rev();
            if rl_res > RunLength::MAX as usize {
                self.push_copy_command(faas);
                let mut full_header_count = 0;
                while rl_res > RunLength::MAX as usize {
                    self.push_insert_command(faas, header.fmt, RunLength::MAX);
                    header.set_same_value_as_previous(true);
                    faas.field_pos += RunLength::MAX as usize;
                    rl_res -= RunLength::MAX as usize;
                    full_header_count += 1;
                }
                let new_rl_offset = (pre as usize + dup_count
                    - full_header_count * RunLength::MAX as usize)
                    as RunLength;
                for it in iterators {
                    if it.header_rl_offset <= pre {
                        break;
                    }
                    it.field_pos += dup_count;
                    it.header_idx += full_header_count;
                    it.header_rl_offset =
                        new_rl_offset + (it.header_rl_offset - pre);
                }
            } else {
                for it in iterators {
                    if it.header_rl_offset <= pre {
                        break;
                    }
                    it.field_pos += dup_count;
                    it.header_rl_offset += dup_count as RunLength;
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
        Self::iters_after_offset_to_next_header_bumping_field_pos(
            faas,
            // only move iterators that refer to the element *after* the one
            // we are duping, so not pre and mid
            pre + 1,
            dup_count,
            usize::from(pre > 0) + mid_full_count,
            iterators,
            FieldValueHeader {
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
            header.set_shared_value_if_rl_1();
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
        header.set_shared_value_if_rl_1();
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
            if header.shared_value() {
                if drop_count <= rl_rem as usize {
                    let rl_to_del = drop_count as RunLength;
                    header.run_length -= rl_to_del;
                    faas.curr_action_run_length = 0;
                    Self::iters_adjust_drop_before(
                        faas,
                        iterators,
                        faas.field_pos,
                        rl_to_del,
                    );
                    return;
                }
                header.run_length = rl_pre;
                faas.curr_action_run_length -= rl_rem as usize;
                return;
            }
            self.push_copy_command(faas);
            self.push_insert_command(faas, header.fmt, rl_pre);
            // this only affects the iterators ones after rl_pre
            // because the earlier ones were already advanced  past
            // by `update_current_iters_start`
            Self::iters_to_next_header(
                faas,
                iterators,
                FieldValueHeader {
                    fmt: header.fmt,
                    run_length: rl_pre,
                },
            );
            faas.field_pos += rl_pre as usize;
            header.set_leading_padding(0);
            header.run_length -= rl_pre;
            if drop_count <= rl_rem as usize {
                let rl_to_del = drop_count as RunLength;
                debug_assert!(!header.shared_value());
                if rl_to_del == rl_rem {
                    header.set_deleted(true);
                    faas.curr_action_run_length = 0;
                    Self::iters_to_next_header_zero_offset(
                        faas, iterators, *header,
                    );
                    return;
                }
                let mut fmt_del = header.fmt;
                fmt_del.set_deleted(true);
                self.push_insert_command_if_rl_gt_0(faas, fmt_del, rl_to_del);
                header.run_length -= rl_to_del;
                Self::iters_to_next_header_adjusting_deleted_offset(
                    faas,
                    iterators,
                    FieldValueHeader {
                        fmt: header.fmt,
                        run_length: rl_to_del,
                    },
                );
                if header.run_length == 1 {
                    header.set_shared_value(true);
                }
                faas.curr_action_run_length = 0;
                return;
            }
            header.set_deleted(true);
            Self::iters_to_next_header_adjusting_deleted_offset(
                faas, iterators, *header,
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
            Self::iters_to_next_header_adjusting_deleted_offset(
                faas, iterators, *header,
            );
            return;
        }
        // otherwise the if statement above must be true
        let rl_to_del = drop_count as RunLength;
        faas.curr_action_run_length = 0;
        if rl_to_del == header.run_length {
            header.set_deleted(true);
            Self::iters_to_next_header_zero_offset(faas, iterators, *header);
            return;
        }
        if !header.shared_value() {
            self.push_copy_command(faas);
            let mut fmt_del = header.fmt;
            fmt_del.set_deleted(true);
            self.push_insert_command(faas, fmt_del, rl_to_del);
            header.run_length -= rl_to_del;
            header.set_leading_padding(0);
            Self::iters_to_next_header_adjusting_deleted_offset(
                faas,
                iterators,
                FieldValueHeader {
                    fmt: fmt_del,
                    run_length: rl_to_del,
                },
            );
            return;
        }
        Self::iters_adjust_drop_before(
            faas,
            iterators,
            faas.field_pos,
            rl_to_del,
        );
        header.run_length -= rl_to_del;
    }
    fn update_current_iters_start(
        iterators: &mut [&mut IterState],
        faas: &mut FieldActionApplicationState,
    ) {
        while faas.curr_header_iters_start != faas.curr_header_iters_end {
            let it = &iterators[faas.curr_header_iters_start];
            if it.field_pos < faas.curr_action_pos {
                faas.curr_header_iters_start += 1;
                continue;
            }
            break;
        }
    }

    // returns the field_count delta
    fn generate_commands_from_actions<'a>(
        &mut self,
        actions: impl Iterator<Item = &'a FieldAction>,
        headers: &mut VecDeque<FieldValueHeader>,
        iterators: &mut [&mut IterState],
    ) -> isize {
        let mut faas = FieldActionApplicationState {
            header_idx: 0,
            field_pos: 0,
            header_idx_new: 0,
            copy_range_start: 0,
            copy_range_start_new: 0,
            field_pos_old: 0,
            data_end: 0,
            curr_header_iters_start: 0,
            curr_header_iters_end: 0,
            curr_header_original_rl: headers
                .front()
                .map(FieldValueHeader::effective_run_length)
                .unwrap_or(0),
            curr_action_kind: FieldActionKind::Dup,
            curr_action_run_length: 0,
            curr_action_pos: 0,
        };
        for it in iterators.iter() {
            if it.header_idx != 0 {
                break;
            }
            faas.curr_header_iters_end += 1;
        }
        let mut curr_action;
        let mut actions = actions.peekable();
        'consume_actions: loop {
            curr_action = actions.next().copied();
            let Some(action) = curr_action else {
                break;
            };
            faas.curr_action_kind = action.kind;
            faas.curr_action_pos = action.field_idx;
            faas.curr_action_run_length = action.run_len as usize;
            // SAFETY: If this assumption is violated,
            // we might produce misstyped fields, leading to unsound memory
            // casts by the iterators. It is possible to produce this state
            // by violating the `FieldAction` list invariants, which are
            // (currently) not fully checked by the ActionBuffer on insertion.
            // Therefore a `debug_assert` would be insufficient here.
            assert!(
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
                if faas.header_idx == headers.len() {
                    break 'consume_actions;
                }
                Self::move_header_idx_to_action_pos(
                    headers, iterators, &mut faas,
                );
                if faas.header_idx == headers.len() {
                    break 'consume_actions;
                }
                match faas.curr_action_kind {
                    FieldActionKind::Dup => {
                        self.handle_dup(
                            &mut headers[faas.header_idx],
                            iterators,
                            &mut faas,
                        );
                        faas.curr_action_pos += faas.curr_action_run_length;
                        Self::update_current_iters_start(iterators, &mut faas);
                        break;
                    }
                    FieldActionKind::InsertZst(zst_repr) => {
                        self.handle_zst_inserts(
                            &mut headers[faas.header_idx],
                            iterators,
                            &mut faas,
                            zst_repr,
                        );
                        faas.curr_action_pos += faas.curr_action_run_length;
                        Self::update_current_iters_start(iterators, &mut faas);
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
        if headers_rem > 0 {
            faas.field_pos +=
                headers[faas.header_idx].effective_run_length() as usize;
            faas.field_pos_old += faas.curr_header_original_rl as usize;
            let header_idx_delta =
                faas.header_idx_new as isize - faas.header_idx as isize;
            let field_pos_delta =
                faas.field_pos as isize - faas.field_pos_old as isize;
            for it in &mut iterators[faas.curr_header_iters_end..] {
                it.field_pos =
                    (it.field_pos as isize + field_pos_delta) as usize;
                it.header_idx =
                    (it.header_idx as isize + header_idx_delta) as usize;
            }
            faas.header_idx_new += headers_rem;
            faas.header_idx += headers_rem;
        }
        self.push_copy_command(&mut faas);
        for a in curr_action.iter().copied().chain(actions.copied()) {
            assert!(a.field_idx == faas.field_pos);
            let FieldActionKind::InsertZst(repr) = a.kind else {
                unreachable!()
            };
            self.push_insert_command(
                &mut faas,
                FieldValueFormat {
                    repr,
                    ..Default::default()
                },
                a.run_len,
            );
            faas.field_pos += a.run_len as usize;
        }
        faas.field_pos as isize - faas.field_pos_old as isize
    }
    #[allow(clippy::mut_mut)]
    fn move_header_idx_to_action_pos(
        headers: &mut VecDeque<FieldValueHeader>,
        iterators: &mut [&mut IterState],
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
            faas.curr_header_iters_start = faas.curr_header_iters_end;
            let len = iterators.len();
            while len > faas.curr_header_iters_end {
                let it = &mut iterators[faas.curr_header_iters_end];
                if it.header_idx != faas.header_idx + 1 {
                    break;
                }
                it.field_pos =
                    (it.field_pos as isize + field_pos_delta) as usize;
                it.header_idx += faas.header_idx_new - faas.header_idx;
                faas.curr_header_iters_end += 1;
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
        Self::update_current_iters_start(iterators, faas);
    }

    fn execute_commands(&mut self, headers: &mut VecDeque<FieldValueHeader>) {
        if self.copies.is_empty() && self.insertions.is_empty() {
            return;
        }
        let new_size = self
            .insertions
            .last()
            .map(|i| i.index + 1)
            .unwrap_or(0)
            .max(
                self.copies
                    .last()
                    .map(|c| c.target + c.len)
                    .unwrap_or(headers.len()),
            );
        // TODO: do something clever instead
        headers.resize(new_size, FieldValueHeader::default());
        headers.make_contiguous();
        let header_ptr = headers.as_mut_slices().0.as_mut_ptr();

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
            for i in &self.insertions {
                (*header_ptr.add(i.index)) = i.value;
            }
        }
        self.insertions.clear();
        self.copies.clear();
    }

    fn canonicalize_iters(
        field_count: usize,
        headers: &VecDeque<FieldValueHeader>,
        iterators: &mut [&mut IterState],
    ) {
        for it in iterators.iter_mut().rev() {
            if it.field_pos < field_count {
                break;
            }
            if it.header_rl_offset != 0 {
                continue;
            }
            if it.header_idx == 0 {
                continue;
            }
            // being on a deleted header is fine for this
            // the only reason we do this is to avoid pushes
            // from causing the iter to skip fields
            if it.header_idx == headers.len()
                || !headers[it.header_idx].same_value_as_previous()
            {
                it.data -= headers[it.header_idx - 1].total_size_unique();
            }
            it.header_idx -= 1;
            it.header_rl_offset = headers[it.header_idx].run_length;
        }
    }

    pub fn run<'a>(
        &mut self,
        actions: impl Iterator<Item = &'a FieldAction>,
        headers: &mut VecDeque<FieldValueHeader>,
        field_count: &mut usize,
        iterators: impl Iterator<Item = &'a mut IterState>,
    ) -> isize {
        let mut iters = transmute_vec(std::mem::take(&mut self.iters));
        iters.extend(iterators);
        iters.sort_by(|lhs, rhs| lhs.field_pos.cmp(&rhs.field_pos));
        let field_count_delta =
            self.generate_commands_from_actions(actions, headers, &mut iters);
        debug_assert!(*field_count as isize + field_count_delta >= 0);
        *field_count = (*field_count as isize + field_count_delta) as usize;
        self.execute_commands(headers);
        Self::canonicalize_iters(*field_count, headers, &mut iters);
        self.iters = transmute_vec(iters);
        field_count_delta
    }
}

#[cfg(test)]
mod test {
    use std::collections::VecDeque;

    use crate::record_data::{
        field_action::{FieldAction, FieldActionKind},
        field_action_applicator::FieldActionApplicator,
        field_data::{
            FieldData, FieldValueFormat, FieldValueHeader, FieldValueRepr,
            FixedSizeFieldValueType, RunLength,
        },
        field_value::FieldValue,
        iter_hall::IterState,
        iters::FieldIterator,
        push_interface::PushInterface,
    };

    use FieldActionKind as FAK;

    #[track_caller]
    fn test_actions_on_headers(
        input: impl IntoIterator<Item = FieldValueHeader>,
        actions: &[FieldAction],
        output: impl IntoIterator<Item = FieldValueHeader>,
        iter_states_in: &[IterState],
        iter_states_out: &[IterState],
    ) {
        let mut headers = input.into_iter().collect::<VecDeque<_>>();
        let mut field_count =
            headers.iter().map(|h| h.run_length as usize).sum();

        let mut faa = FieldActionApplicator::default();
        let mut iter_states = iter_states_in.to_vec();
        faa.run(
            actions.iter(),
            &mut headers,
            &mut field_count,
            iter_states.iter_mut(),
        );
        let headers_got = headers.iter().copied().collect::<Vec<_>>();
        let headers_expected = output.into_iter().collect::<Vec<_>>();
        assert_eq!(headers_got, headers_expected);
        assert_eq!(iter_states, iter_states_out);
    }

    #[track_caller]
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
            len_before += rl as isize;
        }
        let mut faa = FieldActionApplicator::default();
        let mut iter_states = iter_states_in.to_vec();
        let fc_delta = faa.run(
            actions.iter(),
            &mut fd.headers,
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
    #[track_caller]
    fn test_actions_on_range_single_type<T: FixedSizeFieldValueType>(
        input: impl IntoIterator<Item = (T, RunLength)>,
        header_rle: bool,
        value_rle: bool,
        actions: &[FieldAction],
        output: impl IntoIterator<Item = (T, RunLength)>,
        iter_states_in: &[IterState],
        iter_states_out: &[IterState],
    ) {
        let output_fv = output
            .into_iter()
            .map(|(v, rl)| (FieldValue::from_fixed_sized_type(v.clone()), rl))
            .collect::<Vec<_>>();
        test_actions_on_range_raw(
            input
                .into_iter()
                .map(|(v, rl)| (FieldValue::from_fixed_sized_type(v), rl)),
            header_rle,
            value_rle,
            actions,
            &output_fv,
            iter_states_in,
            iter_states_out,
        );
    }

    #[track_caller]
    fn test_actions_on_range_full_rle<T: FixedSizeFieldValueType>(
        input: impl Iterator<Item = T>,
        actions: &[FieldAction],
        output: &[(T, RunLength)],
    ) {
        test_actions_on_range_single_type(
            input.map(|v| (v, 1)),
            true,
            true,
            actions,
            output.iter().cloned(),
            &[],
            &[],
        );
    }

    #[track_caller]
    fn test_actions_on_range_no_rle<T: FixedSizeFieldValueType>(
        input: impl Iterator<Item = T>,
        actions: &[FieldAction],
        output: &[(T, RunLength)],
    ) {
        test_actions_on_range_single_type(
            input.map(|v| (v, 1)),
            false,
            false,
            actions,
            output.iter().cloned(),
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
        test_actions_on_range_no_rle(
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
        //    2         1
        //              2
        test_actions_on_range_raw(
            [0, 1, 1, 1, 2].iter().map(|v| (FieldValue::Int(*v), 1)),
            true,
            true,
            &[FieldAction::new(
                FAK::InsertZst(FieldValueRepr::Undefined),
                2,
                1,
            )],
            &[
                (FieldValue::Int(0), 1),
                (FieldValue::Int(1), 1),
                (FieldValue::Undefined, 1),
                (FieldValue::Int(1), 2),
                (FieldValue::Int(2), 1),
            ],
            &[],
            &[],
        );
    }

    #[test]
    #[should_panic = "overlapping field actions"]
    fn drop_within_dup() {
        //  Before  Dup(0, 2)  Drop(1, 1)
        //    0         0           0
        //    1         0           0
        //    2         0           1
        //              1           2
        //              2
        test_actions_on_range_full_rle(
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
        test_actions_on_range_no_rle(
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
        test_actions_on_range_full_rle(
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

    #[test]
    fn drop_spanning_shared_values() {
        //  Before   Drop(1, 5)
        //    0           0
        //    0           3
        //    0
        //    1
        //    1
        //    2
        //    3
        test_actions_on_range_single_type(
            [(0i64, 3), (1, 2), (2, 1), (3, 1)],
            false,
            false,
            &[FieldAction::new(FAK::Drop, 1, 5)],
            [(0, 1), (3, 1)],
            // TODO: test iters
            &[],
            &[],
        );
    }

    #[test]
    fn leading_padding_in_drop() {
        let header = |pad, del, run_length| {
            let mut fmt = FieldValueFormat {
                repr: FieldValueRepr::TextInline,
                flags: 0,
                size: 1,
            };
            fmt.set_leading_padding(pad);
            fmt.set_deleted(del);
            FieldValueHeader { fmt, run_length }
        };
        let iterstate =
            |field_pos, data, header_idx, header_rl_offset| IterState {
                field_pos,
                data,
                header_idx,
                header_rl_offset,
                #[cfg(feature = "debug_logging")]
                kind: crate::record_data::iter_hall::IterKind::Undefined,
            };

        test_actions_on_headers(
            [header(1, false, 2)],
            &[FieldAction::new(FAK::Drop, 0, 1)],
            [header(1, true, 1), header(0, false, 1)],
            &[iterstate(1, 0, 0, 1)],
            &[iterstate(0, 2, 1, 0)],
        );
    }
}
