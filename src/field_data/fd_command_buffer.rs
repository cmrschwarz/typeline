use std::cell::RefMut;

use crate::worker_thread_session::Field;

use super::{fd_iter_hall::FDIterState, FieldData, FieldValueFormat, FieldValueHeader, RunLength};

#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub enum FieldActionKind {
    #[default]
    Dup,
    Drop,
}

#[derive(Clone, Copy, Default)]
pub struct FieldAction {
    pub kind: FieldActionKind,
    pub run_len: RunLength,
    pub field_idx: usize,
}

struct ActionSet {
    set_index: usize,
    actions_start: usize,
    action_count: usize,
}

#[derive(Clone, Copy)]
struct ActionSetMergeResult {
    merge_set_index: usize,
    actions_start: usize,
    actions_end: usize,
}

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
pub struct FDCommandBuffer {
    actions: [Vec<FieldAction>; 4],
    action_sets: Vec<ActionSet>,
    iter_states: Vec<&'static mut FDIterState>,
    copies: Vec<CopyCommand>,
    insertions: Vec<InsertionCommand>,
}

const ACTIONS_RAW_IDX: usize = 0;

impl FDCommandBuffer {
    pub fn is_legal_field_idx_for_action(&self, field_idx: usize) -> bool {
        if let Some(acs) = self.action_sets.last() {
            if acs.action_count != 0 {
                self.actions[ACTIONS_RAW_IDX][acs.actions_start + acs.action_count - 1].field_idx
                    <= field_idx
            } else {
                true
            }
        } else {
            false
        }
    }
    pub fn push_action_with_usize_rl(
        &mut self,
        kind: FieldActionKind,
        field_idx: usize,
        mut run_length: usize,
    ) {
        while run_length > 0 {
            let rl_to_push = run_length.min(RunLength::MAX as usize) as RunLength;
            self.push_action(kind, field_idx, rl_to_push);
            run_length -= rl_to_push as usize;
        }
    }
    pub fn push_action(&mut self, kind: FieldActionKind, field_idx: usize, run_length: RunLength) {
        assert!(self.is_legal_field_idx_for_action(field_idx));
        self.action_sets.last_mut().unwrap().action_count += 1;
        self.actions[ACTIONS_RAW_IDX].push(FieldAction {
            kind,
            field_idx,
            run_len: run_length,
        });
    }
    pub fn begin_action_set(&mut self, set_index: usize) {
        if let Some(acs) = self.action_sets.last_mut() {
            if acs.action_count == 0 {
                acs.set_index = set_index;
                return;
            }
        }
        self.action_sets.push(ActionSet {
            set_index,
            actions_start: self.actions[ACTIONS_RAW_IDX].len(),
            action_count: 0,
        })
    }
    pub fn clear(&mut self) {
        self.actions[ACTIONS_RAW_IDX].clear();
        self.action_sets.clear();
    }
    pub fn last_action_set_id(&self) -> usize {
        self.action_sets
            .last()
            .map(|acs| acs.set_index)
            .unwrap_or(0)
    }
    pub fn execute<'a, 'b>(
        &mut self,
        fd_iter_halls: impl Iterator<Item = RefMut<'a, Field>>,
        min_action_set_id: usize,
        max_action_set_id: usize,
    ) {
        let merged_acs_idx = self.prepare_actions(min_action_set_id, max_action_set_id);

        for mut fdih in fd_iter_halls.into_iter() {
            self.iter_states
                .extend(fdih.field_data.iters.iter_mut().filter_map(|it| {
                    it.get_mut().is_valid().then(|| unsafe {
                        std::mem::transmute::<&'_ mut FDIterState, &'static mut FDIterState>(
                            it.get_mut(),
                        )
                    })
                }));
            // we reverse the sort order so we can pop back
            self.iter_states
                .sort_by(|lhs, rhs| lhs.field_pos.cmp(&rhs.field_pos).reverse());
            let field_offset = fdih.field_data.initial_field_offset;
            self.generate_commands_from_actions(
                merged_acs_idx,
                &mut fdih.field_data.fd,
                0,
                field_offset,
            );
            self.execute_commands(&mut fdih.field_data.fd);
            self.iter_states.clear();
            self.cleanup();
        }
    }

    fn cleanup(&mut self) {
        for ms in self.actions.iter_mut().skip(1) {
            ms.clear();
        }
        self.insertions.clear();
        self.copies.clear();
    }
    fn action_set_id_to_idx(&self, action_set_id: usize) -> usize {
        match self
            .action_sets
            .binary_search_by(|acs| acs.set_index.cmp(&action_set_id))
        {
            Ok(idx) => idx,
            Err(idx) => idx,
        }
    }
    pub fn erase_action_sets(&mut self, lowest_id_to_remove: usize) {
        self.action_sets
            .truncate(self.action_set_id_to_idx(lowest_id_to_remove));
    }
}

// prepare final actions list from actions_raw
impl FDCommandBuffer {
    fn merge_two_action_sets_raw(sets: [&[FieldAction]; 2], target: &mut Vec<FieldAction>) {
        const LEFT: usize = 0;
        const RIGHT: usize = 1;
        const COUNT: usize = 2;

        let mut offsets = [0isize; COUNT];
        let mut indices = [0usize; COUNT];
        let mut starts = [0usize; COUNT];

        loop {
            for i in 0..COUNT {
                starts[i] = (sets[i][indices[i]].field_idx as isize + offsets[i]) as usize;
            }
            if starts[LEFT] <= starts[RIGHT] {
                let left = &sets[LEFT][indices[LEFT]];
                let field_idx = (left.field_idx as isize + offsets[LEFT]) as usize;
                let drops_4_left = &mut offsets[RIGHT];
                let mut run_len = left.run_len;
                let mut kind = left.kind;
                match left.kind {
                    FieldActionKind::Dup => {
                        if *drops_4_left >= run_len as isize {
                            kind = FieldActionKind::Drop;
                            *drops_4_left -= run_len as isize;
                            let mut space_to_next = sets[LEFT]
                                .get(indices[LEFT] + 1)
                                .map(|a| a.field_idx - left.field_idx)
                                .unwrap_or(usize::MAX);
                            while space_to_next > *drops_4_left as usize
                                && *drops_4_left > RunLength::MAX as isize
                            {
                                *drops_4_left -= RunLength::MAX as isize;
                                space_to_next -= RunLength::MAX as usize;
                                target.push(FieldAction {
                                    kind,
                                    run_len: RunLength::MAX,
                                    field_idx,
                                });
                            }
                            run_len = (*drops_4_left).min(space_to_next as isize) as RunLength;
                            *drops_4_left -= run_len as isize;
                        } else {
                            run_len -= *drops_4_left as RunLength;
                            *drops_4_left = 0;
                        }
                    }
                    FieldActionKind::Drop => {
                        let mut space_to_next = sets[LEFT]
                            .get(indices[LEFT] + 1)
                            .map(|a| a.field_idx - left.field_idx)
                            .unwrap_or(usize::MAX);
                        *drops_4_left += run_len as isize;
                        while space_to_next > *drops_4_left as usize
                            && *drops_4_left > RunLength::MAX as isize
                        {
                            *drops_4_left -= RunLength::MAX as isize;
                            space_to_next -= RunLength::MAX as usize;
                            target.push(FieldAction {
                                kind,
                                run_len: RunLength::MAX,
                                field_idx,
                            });
                        }
                        run_len = (*drops_4_left).min(space_to_next as isize) as RunLength;
                        *drops_4_left -= run_len as isize;
                    }
                }
                target.push(FieldAction {
                    kind,
                    run_len,
                    field_idx,
                });
                indices[LEFT] += 1;
                if indices[LEFT] == sets[LEFT].len() {
                    break;
                }
            } else {
                let right = &sets[RIGHT][indices[RIGHT]];
                let field_idx = (right.field_idx as isize + offsets[RIGHT]) as usize;
                let mut run_len = right.run_len;

                match right.kind {
                    FieldActionKind::Dup => {
                        offsets[LEFT] += right.run_len as isize;
                    }
                    FieldActionKind::Drop => {
                        let gap_to_start_left = starts[LEFT] - starts[RIGHT];
                        if gap_to_start_left < right.run_len as usize {
                            let gap_rl = gap_to_start_left as RunLength;
                            run_len = gap_rl;
                            offsets[RIGHT] += (right.run_len - gap_rl) as isize;
                        }
                        offsets[LEFT] -= run_len as isize;
                    }
                }
                target.push(FieldAction {
                    kind: right.kind,
                    run_len,
                    field_idx,
                });
                indices[RIGHT] += 1;
            }
        }
        for i in 0..COUNT {
            if indices[i] == sets[i].len() {
                let other = (i + 1) % COUNT;
                for i in indices[other]..sets[other].len() {
                    let action = &sets[other][i];
                    target.push(FieldAction {
                        kind: action.kind,
                        run_len: action.run_len,
                        field_idx: (action.field_idx as isize + offsets[other]) as usize,
                    });
                }
                break;
            }
        }
    }
    fn get_merge_result_slice<'a>(
        &self,
        full_slice: &'a [FieldAction],
        asmr: ActionSetMergeResult,
    ) -> &'a [FieldAction] {
        &full_slice[asmr.actions_start..asmr.actions_end]
    }
    fn unclaim_merge_space(&mut self, asmr: ActionSetMergeResult) {
        let ms = &mut self.actions[asmr.merge_set_index];
        debug_assert!(ms.len() == asmr.actions_end);
        ms.truncate(asmr.actions_start);
    }
    fn merge_two_action_sets(
        &mut self,
        first: ActionSetMergeResult,
        second: ActionSetMergeResult,
        target_merge_set: usize,
    ) -> ActionSetMergeResult {
        let first_slice_full;
        let second_slice_full;
        let res_ms;
        unsafe {
            debug_assert!(first.merge_set_index != target_merge_set);
            debug_assert!(second.merge_set_index != target_merge_set);
            debug_assert!(first.merge_set_index != second.merge_set_index);
            let ac_sets_ptrs = self.actions.as_mut_ptr();
            first_slice_full = (*ac_sets_ptrs.add(first.merge_set_index)).as_slice();
            second_slice_full = (*ac_sets_ptrs.add(second.merge_set_index)).as_slice();
            res_ms = &mut *ac_sets_ptrs.add(target_merge_set);
        }

        let first_slice = self.get_merge_result_slice(first_slice_full, first);
        let second_slice = self.get_merge_result_slice(second_slice_full, second);

        let res_size = first_slice.len() + second_slice.len();
        res_ms.reserve(res_size);
        let res_len_before = res_ms.len();
        FDCommandBuffer::merge_two_action_sets_raw([first_slice, second_slice], res_ms);
        let res_len_after = res_ms.len();
        self.unclaim_merge_space(first);
        self.unclaim_merge_space(second);
        ActionSetMergeResult {
            merge_set_index: target_merge_set,
            actions_start: res_len_before,
            actions_end: res_len_after,
        }
    }
    fn action_set_as_result(&self, action_set_idx: usize) -> ActionSetMergeResult {
        let acs = &self.action_sets[action_set_idx];
        ActionSetMergeResult {
            merge_set_index: ACTIONS_RAW_IDX,
            actions_start: acs.actions_start,
            actions_end: acs.actions_start + acs.action_count,
        }
    }
    fn merge_action_sets(
        &mut self,
        action_sets_start: usize,
        action_sets_len: usize,
        target_merge_set: usize,
    ) -> ActionSetMergeResult {
        if action_sets_len == 1 {
            return self.action_set_as_result(action_sets_start);
        }
        if action_sets_len == 2 {
            return self.merge_two_action_sets(
                self.action_set_as_result(action_sets_start),
                self.action_set_as_result(action_sets_start + 1),
                target_merge_set,
            );
        }
        let len_half = action_sets_len / 2;
        let merge_first_half = self.merge_action_sets(
            action_sets_start,
            len_half,
            ((target_merge_set + 1) % 3) + 1,
        );
        let merge_rest = self.merge_action_sets(
            action_sets_start + len_half,
            action_sets_len - len_half,
            ((target_merge_set + 2) % 3) + 1,
        );
        self.merge_two_action_sets(merge_first_half, merge_rest, target_merge_set)
    }
    fn prepare_actions(
        &mut self,
        min_action_set_id: usize,
        max_action_set_id: usize,
    ) -> ActionSetMergeResult {
        let first = self.action_set_id_to_idx(min_action_set_id);
        let last = self.action_set_id_to_idx(max_action_set_id);
        self.merge_action_sets(first, last - first + 1, ACTIONS_RAW_IDX + 1)
    }
}

#[derive(Clone, Copy)]
enum CommandHandlerContinuation {
    AdvanceAction,
    AdvanceHeader,
}

// generate_commands_from_actions machinery
impl FDCommandBuffer {
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
        field_idx: usize,
        run_len: usize,
        header: &mut FieldValueHeader,
        field_pos: &mut usize,
        header_idx_new: &mut usize,
        copy_range_start: &mut usize,
        copy_range_start_new: &mut usize,
    ) {
        if header.shared_value() {
            let mut rl_res = header.run_length as usize + run_len as usize;
            if rl_res > RunLength::MAX as usize {
                self.push_copy_command(*header_idx_new, copy_range_start, copy_range_start_new);
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
        let mid_full_count = run_len / RunLength::MAX as usize;
        let mid_rem = (run_len - (mid_full_count * RunLength::MAX as usize)) as RunLength;
        let post = (header.run_length - pre).saturating_sub(1);
        self.push_copy_command(*header_idx_new, copy_range_start, copy_range_start_new);
        self.push_insert_command_check_run_length(
            header_idx_new,
            copy_range_start_new,
            header.fmt,
            pre,
        );
        *field_pos += pre as usize;
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
        return;
    }
    fn handle_drop(
        &mut self,
        action: &mut FieldAction,
        header: &mut FieldValueHeader,
        field_pos: &mut usize,
        header_idx_new: &mut usize,
        copy_range_start: &mut usize,
        copy_range_start_new: &mut usize,
    ) -> CommandHandlerContinuation {
        let rl_to_del = action.run_len;
        let rl_pre = (action.field_idx - *field_pos) as RunLength;
        if rl_pre > 0 {
            let rl_rem = header.run_length - rl_pre;
            if header.shared_value() && action.run_len <= rl_rem {
                header.run_length -= action.run_len;
                return CommandHandlerContinuation::AdvanceAction;
            }
            self.push_copy_command(*header_idx_new, copy_range_start, copy_range_start_new);
            self.push_insert_command_check_run_length(
                header_idx_new,
                copy_range_start_new,
                header.fmt,
                rl_pre,
            );
            *field_pos += rl_pre as usize;
            if action.run_len <= rl_rem {
                debug_assert!(!header.shared_value());
                if action.run_len == rl_rem {
                    header.set_deleted(true);
                    return CommandHandlerContinuation::AdvanceAction;
                }
                let mut fmt_del = header.fmt;
                fmt_del.set_deleted(true);
                self.push_insert_command_check_run_length(
                    header_idx_new,
                    copy_range_start_new,
                    fmt_del,
                    rl_rem,
                );
                header.run_length -= action.run_len;
                if header.run_length == 1 {
                    header.set_shared_value(true);
                }
                return CommandHandlerContinuation::AdvanceAction;
            }
            if header.shared_value() {
                header.set_deleted(true);
                action.run_len -= rl_rem;
                *copy_range_start += 1;
                *copy_range_start_new += 1;
                return CommandHandlerContinuation::AdvanceHeader;
            }
            header.set_deleted(true);
            if action.run_len == rl_rem {
                return CommandHandlerContinuation::AdvanceAction;
            }
            action.run_len -= rl_rem;
            return CommandHandlerContinuation::AdvanceHeader;
        }
        if rl_to_del > header.run_length {
            header.set_deleted(true);
            action.run_len -= header.run_length;
            return CommandHandlerContinuation::AdvanceHeader;
        }
        if rl_to_del == header.run_length {
            header.set_deleted(true);
            return CommandHandlerContinuation::AdvanceAction;
        }
        if !header.shared_value() {
            self.push_copy_command(*header_idx_new, copy_range_start, copy_range_start_new);
            let mut fmt_del = header.fmt;
            fmt_del.set_deleted(true);
            self.push_insert_command_check_run_length(
                header_idx_new,
                copy_range_start_new,
                fmt_del,
                rl_to_del,
            );
            header.run_length -= rl_to_del;
            return CommandHandlerContinuation::AdvanceAction;
        }
        header.run_length -= rl_to_del;
        return CommandHandlerContinuation::AdvanceAction;
    }
    fn generate_commands_from_actions(
        &mut self,
        merged_actions: ActionSetMergeResult,
        fd: &mut FieldData,
        mut header_idx: usize,
        mut field_pos: usize,
    ) {
        let mut header = &mut fd.header[header_idx];
        let mut header_idx_new = header_idx;

        let mut action = Default::default();
        let mut action_idx_next = 0;
        let mut prev_action_field_idx = usize::MAX;
        let mut copy_range_start = 0;
        let mut copy_range_start_new = 0;
        //TODO: update iterators
        #[allow(unused_variables)]
        let mut field_pos_old = field_pos;
        let mut curr_header_outstanding_dups = 0;
        'advance_action: loop {
            let actions = &self.actions[merged_actions.merge_set_index]
                [merged_actions.actions_start..merged_actions.actions_end];
            loop {
                if action_idx_next == actions.len() {
                    if curr_header_outstanding_dups > 0 {
                        break;
                    }
                    break 'advance_action;
                }
                if curr_header_outstanding_dups > 0
                    && actions[action_idx_next].field_idx != prev_action_field_idx
                {
                    break;
                }
                action = actions[action_idx_next];
                action_idx_next += 1;
                prev_action_field_idx = action.field_idx;
                match action.kind {
                    FieldActionKind::Dup => {
                        curr_header_outstanding_dups += action.run_len as usize;
                    }
                    FieldActionKind::Drop => {
                        if curr_header_outstanding_dups > action.run_len as usize {
                            curr_header_outstanding_dups -= action.run_len as usize;
                        } else {
                            action.run_len -= curr_header_outstanding_dups as RunLength;
                            break;
                        }
                    }
                }
            }
            'advance_header: loop {
                if curr_header_outstanding_dups > 0 {
                    self.handle_dup(
                        prev_action_field_idx,
                        curr_header_outstanding_dups,
                        header,
                        &mut field_pos,
                        &mut header_idx_new,
                        &mut copy_range_start,
                        &mut copy_range_start_new,
                    );
                    curr_header_outstanding_dups = 0;
                    continue 'advance_action;
                }
                loop {
                    if !header.deleted() {
                        let field_pos_new = field_pos + header.run_length as usize;
                        if field_pos_new > prev_action_field_idx {
                            break;
                        }
                        field_pos = field_pos_new;
                        field_pos_old += header.run_length as usize;
                    }
                    header_idx += 1;
                    header_idx_new += 1;
                    header = &mut fd.header[header_idx];
                }
                if action.kind != FieldActionKind::Drop {
                    continue 'advance_header;
                }
                let continuation = self.handle_drop(
                    &mut action,
                    header,
                    &mut field_pos,
                    &mut header_idx_new,
                    &mut copy_range_start,
                    &mut copy_range_start_new,
                );
                if let CommandHandlerContinuation::AdvanceAction = continuation {
                    continue 'advance_action;
                } else {
                    continue 'advance_header;
                }
            }
        }
        self.push_copy_command(
            header_idx_new,
            &mut copy_range_start,
            &mut copy_range_start_new,
        );
    }
}

// final execution step
impl FDCommandBuffer {
    fn execute_commands(&mut self, fd: &mut FieldData) {
        if self.copies.len() == 0 && self.insertions.len() == 0 {
            return;
        }
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
            for c in self.copies.iter().rev() {
                std::ptr::copy(header_ptr.add(c.source), header_ptr.add(c.target), c.len);
            }
            for i in self.insertions.iter() {
                (*header_ptr.add(i.index)) = i.value;
            }
            fd.header.set_len(new_size);
        }
    }
}
