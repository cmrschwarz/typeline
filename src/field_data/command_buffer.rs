use std::{cell::RefMut, num::NonZeroUsize};

use nonmax::NonMaxUsize;

use crate::{utils::universe::Universe, worker_thread_session::Field};

use super::{iter_hall::IterState, FieldData, FieldValueFormat, FieldValueHeader, RunLength};

#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub enum FieldActionKind {
    #[default]
    Dup,
    Drop,
}

#[derive(Clone, Copy, Default)]
pub struct FieldAction {
    pub kind: FieldActionKind,
    pub field_idx: usize,
    pub run_len: RunLength,
}

impl FieldAction {
    pub fn new(kind: FieldActionKind, field_idx: usize, run_len: RunLength) -> Self {
        Self {
            kind,
            field_idx,
            run_len,
        }
    }
}

#[derive(Default, Clone, Copy)]
enum ActionListMergeLocation {
    ApfMal {
        apf_idx: ActionProducingFieldIndex,
        mal_idx: MergedActionListsIndex,
    },
    ApfLocal {
        apf_idx: ActionProducingFieldIndex,
        mal_idx: MergedActionListsIndex,
    },
    CbActionList {
        idx: usize,
    },
    #[default]
    Empty,
}

#[derive(Default)]
struct ActionListMergeResult {
    location: ActionListMergeLocation,
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

type ActionListIndex = usize;
type MergedActionListsIndex = usize;
type ActionProducingFieldIndex = NonMaxUsize;

struct ActionList {
    first_unapplied_al_idx_in_prev_apf: ActionListIndex,
    actions_start: usize,
    actions_end: usize,
}

struct LocallyMergedActionList {
    al_idx_start: ActionListIndex,
    al_idx_end: ActionListIndex,
    actions_start: usize,
    actions_end: usize,
}

struct MergedActionLists {
    prev_apf_idx: Option<ActionProducingFieldIndex>,
    next_apf_idx: Option<ActionProducingFieldIndex>,
    action_sets_index_offset: usize,
    locally_merged_action_lists: Vec<LocallyMergedActionList>,
    locally_merged_actions: Vec<FieldAction>,
    action_lists: Vec<ActionList>,
    actions: Vec<FieldAction>,
}

struct ActionProducingField {
    ordering_id: usize,
    merged_action_lists: Vec<MergedActionLists>,
}

#[derive(Default)]
pub struct CommandBuffer {
    action_producing_fields: Universe<ActionProducingFieldIndex, ActionProducingField>,
    merged_actions: [Vec<FieldAction>; 3],
    iter_states: Vec<&'static mut IterState>,
    copies: Vec<CopyCommand>,
    insertions: Vec<InsertionCommand>,
}

impl MergedActionLists {
    pub fn is_legal_field_idx_for_action(&self, field_idx: usize) -> bool {
        if let Some(acs) = self.action_lists.last() {
            if acs.actions_end != 0 {
                self.actions[acs.actions_start + acs.actions_end - 1].field_idx <= field_idx
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
        let al = self.action_lists.last_mut().unwrap();
        if al.actions_end > 0 {
            // very simple early merging of actions to hopefully save some memory
            // this also allows operations to be slightly more 'wasteful' with their action pushes
            let last = &mut self.actions[al.actions_start + al.actions_end - 1];
            if last.kind == kind
                && last.field_idx == field_idx
                && RunLength::MAX as usize > last.run_len as usize + run_length as usize
            {
                last.run_len += run_length;
                return;
            }
        }
        al.actions_end += 1;
        self.actions.push(FieldAction {
            kind,
            field_idx,
            run_len: run_length,
        });
    }
}

impl CommandBuffer {
    pub fn begin_action_list(&mut self, apf_idx: ActionProducingFieldIndex) {
        let apf = &mut self.action_producing_fields[apf_idx];
        if let Some(acs) = apf.merged_action_lists[0].action_lists.last_mut() {
            if acs.actions_end == acs.actions_start {
                return;
            }
        }
        let start = apf.merged_action_lists[0].actions.len();
        apf.merged_action_lists[0].action_lists.push(ActionList {
            first_unapplied_al_idx_in_prev_apf: apf.merged_action_lists[0]
                .prev_apf_idx
                .map(|prev| {
                    self.action_producing_fields[prev].merged_action_lists[0]
                        .action_lists
                        .len()
                })
                .unwrap_or(0),
            actions_start: start,
            actions_end: start,
        });
    }
    pub fn push_action(
        &mut self,
        apf_idx: ActionProducingFieldIndex,
        kind: FieldActionKind,
        field_idx: usize,
        run_length: RunLength,
    ) {
        self.action_producing_fields[apf_idx].merged_action_lists[0]
            .push_action(kind, field_idx, run_length);
    }
    pub fn push_action_with_usize_rl(
        &mut self,
        apf_idx: ActionProducingFieldIndex,
        kind: FieldActionKind,
        field_idx: usize,
        run_length: usize,
    ) {
        self.action_producing_fields[apf_idx].merged_action_lists[0]
            .push_action_with_usize_rl(kind, field_idx, run_length);
    }
    pub fn execute<'a>(
        &mut self,
        mut field: RefMut<'a, Field>,
        min_apf_idx: ActionProducingFieldIndex,
        max_apf_idx: ActionProducingFieldIndex,
        first_unapplied_al_idx_in_max_apf: ActionListIndex,
    ) {
        self.iter_states
            .extend(field.field_data.iters.iter_mut().filter_map(|it| {
                it.get_mut().is_valid().then(|| unsafe {
                    std::mem::transmute::<&'_ mut IterState, &'static mut IterState>(it.get_mut())
                })
            }));
        // we reverse the sort order so we can pop back
        self.iter_states
            .sort_by(|lhs, rhs| lhs.field_pos.cmp(&rhs.field_pos).reverse());
        let field_offset = field.field_data.initial_field_offset;
        self.generate_commands_from_actions(
            self.merge_action_lists(min_apf_idx, max_apf_idx, first_unapplied_al_idx_in_max_apf),
            &mut field.field_data.fd,
            0,
            field_offset,
        );
        self.execute_commands(&mut field.field_data.fd);
        self.iter_states.clear();
        self.cleanup();
    }

    fn cleanup(&mut self) {
        self.insertions.clear();
        self.copies.clear();
    }
}

// action list merging
impl CommandBuffer {
    fn merge_two_action_lists_raw(sets: [&[FieldAction]; 2], target: &mut Vec<FieldAction>) {
        const LEFT: usize = 0;
        const RIGHT: usize = 1;
        const COUNT: usize = 2;

        let mut curr_action_idx = [0usize; COUNT];
        let mut next_action_field_idx = [0usize; COUNT];
        let mut field_pos_offset = [0isize; COUNT];

        loop {
            for i in 0..COUNT {
                next_action_field_idx[i] =
                    (sets[i][curr_action_idx[i]].field_idx as isize + field_pos_offset[i]) as usize;
            }
            if next_action_field_idx[LEFT] <= next_action_field_idx[RIGHT] {
                let left = &sets[LEFT][curr_action_idx[LEFT]];
                let field_idx = (left.field_idx as isize + field_pos_offset[LEFT]) as usize;
                let outstanding_drops = &mut field_pos_offset[RIGHT];
                let mut run_len = left.run_len;
                let mut kind = left.kind;
                match left.kind {
                    FieldActionKind::Dup => {
                        if *outstanding_drops >= run_len as isize {
                            kind = FieldActionKind::Drop;
                            *outstanding_drops -= run_len as isize;
                            let mut space_to_next = sets[LEFT]
                                .get(curr_action_idx[LEFT] + 1)
                                .map(|a| a.field_idx - left.field_idx)
                                .unwrap_or(usize::MAX);
                            while space_to_next > *outstanding_drops as usize
                                && *outstanding_drops > RunLength::MAX as isize
                            {
                                *outstanding_drops -= RunLength::MAX as isize;
                                space_to_next -= RunLength::MAX as usize;
                                target.push(FieldAction {
                                    kind,
                                    run_len: RunLength::MAX,
                                    field_idx,
                                });
                            }
                            run_len = (*outstanding_drops).min(space_to_next as isize) as RunLength;
                            *outstanding_drops -= run_len as isize;
                        } else {
                            run_len -= *outstanding_drops as RunLength;
                            *outstanding_drops = 0;
                        }
                    }
                    FieldActionKind::Drop => {
                        let mut space_to_next = sets[LEFT]
                            .get(curr_action_idx[LEFT] + 1)
                            .map(|a| a.field_idx - left.field_idx)
                            .unwrap_or(usize::MAX);
                        *outstanding_drops += run_len as isize;
                        while space_to_next > *outstanding_drops as usize
                            && *outstanding_drops > RunLength::MAX as isize
                        {
                            *outstanding_drops -= RunLength::MAX as isize;
                            space_to_next -= RunLength::MAX as usize;
                            target.push(FieldAction {
                                kind,
                                run_len: RunLength::MAX,
                                field_idx,
                            });
                        }
                        run_len = (*outstanding_drops).min(space_to_next as isize) as RunLength;
                        *outstanding_drops -= run_len as isize;
                    }
                }
                target.push(FieldAction {
                    kind,
                    run_len,
                    field_idx,
                });
                curr_action_idx[LEFT] += 1;
                if curr_action_idx[LEFT] == sets[LEFT].len() {
                    break;
                }
            } else {
                let right = &sets[RIGHT][curr_action_idx[RIGHT]];
                let field_idx = (right.field_idx as isize + field_pos_offset[RIGHT]) as usize;
                let mut run_len = right.run_len;

                match right.kind {
                    FieldActionKind::Dup => {
                        field_pos_offset[LEFT] += right.run_len as isize;
                    }
                    FieldActionKind::Drop => {
                        let gap_to_start_left =
                            next_action_field_idx[LEFT] - next_action_field_idx[RIGHT];
                        if gap_to_start_left < right.run_len as usize {
                            let gap_rl = gap_to_start_left as RunLength;
                            run_len = gap_rl;
                            field_pos_offset[RIGHT] += (right.run_len - gap_rl) as isize;
                        }
                        field_pos_offset[LEFT] -= run_len as isize;
                    }
                }
                target.push(FieldAction {
                    kind: right.kind,
                    run_len,
                    field_idx,
                });
                curr_action_idx[RIGHT] += 1;
            }
        }
        for i in 0..COUNT {
            if curr_action_idx[i] == sets[i].len() {
                let other = (i + 1) % COUNT;
                for i in curr_action_idx[other]..sets[other].len() {
                    let action = &sets[other][i];
                    target.push(FieldAction {
                        kind: action.kind,
                        run_len: action.run_len,
                        field_idx: (action.field_idx as isize + field_pos_offset[other]) as usize,
                    });
                }
                break;
            }
        }
    }
    fn get_merge_result_slice<'a>(&self, almr: ActionListMergeResult) -> &'a [FieldAction] {
        let action_list = match almr.location {
            ActionListMergeLocation::ApfMal { apf_idx, mal_idx } => {
                &self.action_producing_fields[apf_idx].merged_action_lists[mal_idx].actions
            }
            ActionListMergeLocation::ApfLocal { apf_idx, mal_idx } => {
                &self.action_producing_fields[apf_idx].merged_action_lists[mal_idx]
                    .locally_merged_actions
            }
            ActionListMergeLocation::CbActionList { idx } => &self.merged_actions[idx],
            ActionListMergeLocation::Empty => return &[],
        };
        &action_list[almr.actions_start..almr.actions_end]
    }
    fn unclaim_merge_space(&mut self, almr: ActionListMergeResult) {
        let idx = match almr.location {
            ActionListMergeLocation::ApfMal { .. } => return,
            ActionListMergeLocation::ApfLocal { .. } => return,
            ActionListMergeLocation::Empty => return,
            ActionListMergeLocation::CbActionList { idx } => idx,
        };
        let mal = &mut self.merged_actions[idx];
        debug_assert!(mal.len() == almr.actions_end);
        mal.truncate(almr.actions_start);
    }
    fn merge_two_action_lists(
        &mut self,
        first: ActionListMergeResult,
        second: ActionListMergeResult,
    ) -> ActionListMergeResult {
        let mut target_idx = 0;
        if matches!(
            first.location,
            ActionListMergeLocation::CbActionList { idx: target_idx }
        ) {
            target_idx += 1;
        }
        if matches!(
            second.location,
            ActionListMergeLocation::CbActionList { idx: target_idx }
        ) {
            target_idx += 1;
        }
        let first_slice = self.get_merge_result_slice(first);
        let second_slice = self.get_merge_result_slice(second);

        let res_size = first_slice.len() + second_slice.len();
        let res_ms = &mut self.merged_actions[target_idx];
        res_ms.reserve(res_size);
        let res_len_before = res_ms.len();
        CommandBuffer::merge_two_action_lists_raw([first_slice, second_slice], res_ms);
        let res_len_after = res_ms.len();
        self.unclaim_merge_space(first);
        self.unclaim_merge_space(second);
        ActionListMergeResult {
            location: ActionListMergeLocation::CbActionList { idx: target_idx },
            actions_start: res_len_before,
            actions_end: res_len_after,
        }
    }
    fn action_list_as_result(
        &self,
        apf_idx: ActionProducingFieldIndex,
        mal_idx: MergedActionListsIndex,
        al_idx: ActionListIndex,
    ) -> ActionListMergeResult {
        let al = &self.action_producing_fields[apf_idx].merged_action_lists[mal_idx as usize]
            .action_lists[al_idx];
        ActionListMergeResult {
            location: ActionListMergeLocation::ApfMal { apf_idx, mal_idx },
            actions_start: al.actions_start,
            actions_end: al.actions_start + al.actions_end,
        }
    }
    fn construct_missing_local_merges(
        &self,
        apf_idx: ActionProducingFieldIndex,
        mal_idx: MergedActionListsIndex,
        first_unapplied_al_idx_in_mal: ActionListIndex,
    ) {
        let apf = &mut self.action_producing_fields[apf_idx];
        let mal = &mut apf.merged_action_lists[mal_idx];
        let mut last_end = if let Some(last) = mal.locally_merged_action_lists.last() {
            last.al_idx_end
        } else {
            0
        };
        loop {
            let end = last_end + 2;
            if end > 1 + first_unapplied_al_idx_in_mal {
                return;
            }
            let max_width = 1 << end.trailing_zeros();
            let l1 = &mal.action_lists[end - 2];
            let l2 = &mal.action_lists[end - 1];
            let mut lists = [
                &mal.actions[l1.actions_start..l1.actions_end],
                &mal.actions[l2.actions_start..l2.actions_end],
            ];
            let actions_start = mal.locally_merged_actions.len();
            Self::merge_two_action_lists_raw(lists, &mut mal.locally_merged_actions);
            let mut lmal = LocallyMergedActionList {
                al_idx_start: end - 2,
                al_idx_end: end,
                actions_start,
                actions_end: mal.locally_merged_actions.len(),
            };
            mal.locally_merged_action_lists.push(lmal);
            let mut width = 2;
            let mut curr = mal.locally_merged_action_lists.len() - 1;
            let mut prev = curr - last_end.trailing_zeros() as usize;
            while width < max_width {
                let l1 = &mal.locally_merged_action_lists[prev];
                let l2 = &mal.locally_merged_action_lists[curr];
                let mut lists = [
                    &mal.actions[l1.actions_start..l1.actions_end],
                    &mal.actions[l2.actions_start..l2.actions_end],
                ];
                let actions_start = mal.locally_merged_actions.len();
                Self::merge_two_action_lists_raw(lists, &mut mal.locally_merged_actions);
                let mut lmal = LocallyMergedActionList {
                    al_idx_start: end - width,
                    al_idx_end: end,
                    actions_start,
                    actions_end: mal.locally_merged_actions.len(),
                };
                mal.locally_merged_action_lists.push(lmal);
                prev += 1;
                curr += 1;
                width *= 2;
            }
            last_end = end;
        }
    }
    fn merge_single_apf_plain_action_lists(
        &self,
        apf_idx: ActionProducingFieldIndex,
        first_unapplied_al_idx_in_mal: ActionListIndex,
    ) -> ActionListMergeResult {
        let apf = &self.action_producing_fields[apf_idx];
        let mal = apf.merged_action_lists[0];
        if first_unapplied_al_idx_in_mal == mal.action_lists.len() {
            return ActionListMergeResult::default();
        }
        debug_assert!(first_unapplied_al_idx_in_mal < mal.action_lists.len());
        self.construct_missing_local_merges(
            apf_idx,
            0 as MergedActionListsIndex,
            first_unapplied_al_idx_in_mal,
        );

        return ActionListMergeResult::default(); //TODO
    }
    fn merge_single_apf_action_lists(
        &self,
        apf_idx: ActionProducingFieldIndex,
        mal_idx: MergedActionListsIndex,
        first_unapplied_al_idx_in_mal: ActionListIndex,
    ) -> ActionListMergeResult {
        if mal_idx == 0 {
            return self
                .merge_single_apf_plain_action_lists(apf_idx, first_unapplied_al_idx_in_mal);
        }
        self.construct_missing_local_merges(apf_idx, mal_idx, first_unapplied_al_idx_in_mal);
        let apf = &self.action_producing_fields[apf_idx];
        let mal = apf.merged_action_lists[mal_idx];

        return ActionListMergeResult::default(); //TODO
    }
    fn merge_action_lists(
        &mut self,
        min_apf_idx: ActionProducingFieldIndex,
        mut max_apf_idx: ActionProducingFieldIndex,
        mut first_unapplied_al_idx_in_max_apf: ActionListIndex,
    ) -> ActionListMergeResult {
        if min_apf_idx == max_apf_idx {
            return self.merge_single_apf_plain_action_lists(
                min_apf_idx,
                first_unapplied_al_idx_in_max_apf,
            );
        }
        let mut apf = &self.action_producing_fields[max_apf_idx];
        let mut mal_idx = 0;
        let mut mal = &apf.merged_action_lists[mal_idx];
        while mal.action_lists.len() == first_unapplied_al_idx_in_max_apf {
            max_apf_idx = mal.prev_apf_idx.unwrap();
            if first_unapplied_al_idx_in_max_apf != 0 {
                first_unapplied_al_idx_in_max_apf = mal.action_lists
                    [first_unapplied_al_idx_in_max_apf]
                    .first_unapplied_al_idx_in_prev_apf;
            }
            if max_apf_idx == min_apf_idx {
                return self.merge_single_apf_plain_action_lists(
                    min_apf_idx,
                    first_unapplied_al_idx_in_max_apf,
                );
            }
            apf = &mut self.action_producing_fields[max_apf_idx];
            mal = &apf.merged_action_lists[mal_idx];
        }

        while mal.prev_apf_idx.unwrap() > min_apf_idx {
            if mal_idx == apf.merged_action_lists.len() {
                break;
            }
            mal_idx += 1;
            mal = &apf.merged_action_lists[mal_idx];
        }
        let prev_apf_idx = apf.merged_action_lists[mal_idx].prev_apf_idx.unwrap();
        let prev_apf_first_unapplied_al =
            mal.action_lists[first_unapplied_al_idx_in_max_apf].first_unapplied_al_idx_in_prev_apf;

        let prev = self.merge_action_lists(min_apf_idx, prev_apf_idx, prev_apf_first_unapplied_al);
        let curr = self.merge_single_apf_action_lists(
            max_apf_idx,
            mal_idx,
            first_unapplied_al_idx_in_max_apf,
        );

        return if mal_idx == 0 {
            self.merge_two_action_lists(prev, curr)
        } else {
            self.merge_two_action_lists(curr, prev)
        };
    }
}

// generate_commands_from_actions machinery
impl CommandBuffer {
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
        let mid_full_count = (run_len + 1) / RunLength::MAX as usize;
        let mid_rem = ((run_len + 1) - (mid_full_count * RunLength::MAX as usize)) as RunLength;
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
        action_pos: usize,
        curr_action_pos_outstanding_drops: &mut RunLength,
        header: &mut FieldValueHeader,
        field_pos: &mut usize,
        header_idx_new: &mut usize,
        copy_range_start: &mut usize,
        copy_range_start_new: &mut usize,
    ) {
        let rl_to_del = *curr_action_pos_outstanding_drops;
        let rl_pre = (action_pos - *field_pos) as RunLength;
        if rl_pre > 0 {
            let rl_rem = header.run_length - rl_pre;
            if header.shared_value() && rl_to_del <= rl_rem {
                header.run_length -= rl_to_del;
                *curr_action_pos_outstanding_drops = 0;
                return;
            }
            self.push_copy_command(*header_idx_new, copy_range_start, copy_range_start_new);
            self.push_insert_command_check_run_length(
                header_idx_new,
                copy_range_start_new,
                header.fmt,
                rl_pre,
            );
            *field_pos += rl_pre as usize;
            header.run_length -= rl_pre;
            if rl_to_del <= rl_rem {
                debug_assert!(!header.shared_value());
                if rl_to_del == rl_rem {
                    header.set_deleted(true);
                    *curr_action_pos_outstanding_drops = 0;
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
                if header.run_length == 1 {
                    header.set_shared_value(true);
                }
                *curr_action_pos_outstanding_drops = 0;
                return;
            }
            if header.shared_value() {
                header.set_deleted(true);
                *copy_range_start += 1;
                *copy_range_start_new += 1;
                *curr_action_pos_outstanding_drops -= rl_rem;
                return;
            }
            header.set_deleted(true);
            if rl_to_del == rl_rem {
                *curr_action_pos_outstanding_drops = 0;
                return;
            }
            *curr_action_pos_outstanding_drops -= rl_rem;
            return;
        }
        if rl_to_del > header.run_length {
            header.set_deleted(true);
            *curr_action_pos_outstanding_drops -= header.run_length;
            return;
        }
        *curr_action_pos_outstanding_drops = 0;
        if rl_to_del == header.run_length {
            header.set_deleted(true);
            return;
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
            return;
        }
        header.run_length -= rl_to_del;
        return;
    }
    fn generate_commands_from_actions(
        &mut self,
        merged_actions: ActionListMergeResult,
        fd: &mut FieldData,
        mut header_idx: usize,
        mut field_pos: usize,
    ) {
        let mut header;
        let mut header_idx_new = header_idx;

        let mut action_idx_next = 0;
        let mut copy_range_start = 0;
        let mut copy_range_start_new = 0;
        //TODO: update iterators
        #[allow(unused_variables)]
        let mut field_pos_old = field_pos;
        let mut curr_action_pos = 0;
        let mut curr_action_pos_outstanding_dups = 0;
        let mut curr_action_pos_outstanding_drops = 0;
        'advance_action: loop {
            let actions = &if let Some(apf) = merged_actions.apf {
                self.action_producing_fields[apf].merged_action_lists[merged_actions.aml].actions
            } else {
                self.merged_actions[merged_actions.aml]
            }[merged_actions.actions_start..merged_actions.actions_end];
            loop {
                let end_of_actions = action_idx_next == actions.len();
                if end_of_actions {
                    if curr_action_pos_outstanding_dups > 0 {
                        break;
                    }
                    break 'advance_action;
                }
                let action = actions[action_idx_next];
                action_idx_next += 1;
                match action.kind {
                    FieldActionKind::Dup => {
                        if action.field_idx != curr_action_pos {
                            if curr_action_pos_outstanding_dups > 0 {
                                action_idx_next -= 1;
                                break;
                            }
                            curr_action_pos = action.field_idx;
                        }
                        curr_action_pos_outstanding_dups += action.run_len as usize;
                    }
                    FieldActionKind::Drop => {
                        if curr_action_pos_outstanding_dups == 0 {
                            curr_action_pos = action.field_idx;
                            curr_action_pos_outstanding_drops = action.run_len;
                            break;
                        }
                        let action_gap = action.field_idx - curr_action_pos;
                        if curr_action_pos_outstanding_dups < action_gap {
                            action_idx_next -= 1;
                            break;
                        }
                        if curr_action_pos_outstanding_dups >= action_gap + action.run_len as usize
                        {
                            curr_action_pos_outstanding_dups -= action.run_len as usize;
                        } else if action_gap == 0 {
                            curr_action_pos_outstanding_dups = 0;
                            curr_action_pos_outstanding_drops =
                                action.run_len - curr_action_pos_outstanding_dups as RunLength;
                            break;
                        } else {
                            curr_action_pos_outstanding_drops = action.run_len
                                - (curr_action_pos_outstanding_dups - action_gap) as RunLength;
                            curr_action_pos_outstanding_dups = action_gap;
                        }
                    }
                }
            }
            'advance_header: loop {
                loop {
                    header = &mut fd.header[header_idx];
                    if !header.deleted() {
                        let field_pos_new = field_pos + header.run_length as usize;
                        if field_pos_new > curr_action_pos {
                            break;
                        }
                        field_pos = field_pos_new;
                        field_pos_old += header.run_length as usize;
                    }
                    header_idx += 1;
                    header_idx_new += 1;
                }
                if curr_action_pos_outstanding_dups > 0 {
                    self.handle_dup(
                        curr_action_pos,
                        curr_action_pos_outstanding_dups,
                        header,
                        &mut field_pos,
                        &mut header_idx_new,
                        &mut copy_range_start,
                        &mut copy_range_start_new,
                    );
                    let prev_dups = curr_action_pos_outstanding_dups;
                    curr_action_pos_outstanding_dups = 0;
                    if curr_action_pos_outstanding_drops == 0 {
                        continue 'advance_action;
                    }
                    curr_action_pos += prev_dups;
                }
                self.handle_drop(
                    curr_action_pos,
                    &mut curr_action_pos_outstanding_drops,
                    header,
                    &mut field_pos,
                    &mut header_idx_new,
                    &mut copy_range_start,
                    &mut copy_range_start_new,
                );
                if curr_action_pos_outstanding_drops == 0 {
                    continue 'advance_action;
                } else {
                    continue 'advance_header;
                }
            }
        }
        let headers_rem = fd.header.len() - header_idx;
        header_idx_new += headers_rem;
        self.push_copy_command(
            header_idx_new,
            &mut copy_range_start,
            &mut copy_range_start_new,
        );
    }
}

// final execution step
impl CommandBuffer {
    fn execute_commands(&mut self, fd: &mut FieldData) {
        if self.copies.len() == 0 && self.insertions.len() == 0 {
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
                    .unwrap_or(fd.header.len()),
            );
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

#[cfg(test)]
mod test {
    use crate::field_data::{
        iters::FieldIterator, push_interface::PushInterface, typed::TypedSlice,
        typed_iters::TypedSliceIter, FieldData, RunLength,
    };

    use super::{CommandBuffer, FieldAction, FieldActionKind};

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
        let mut cb = CommandBuffer::default();
        cb.begin_action_set(0);
        for a in actions {
            cb.push_action(a.kind, a.field_idx, a.run_len);
        }
        cb.execute(std::iter::once(&mut fd), 0, 0);
        let mut iter = fd.iter();
        let mut results = Vec::new();
        while let Some(range) = iter.typed_range_fwd(usize::MAX, 0) {
            if let TypedSlice::Integer(ints) = range.data {
                results.extend(TypedSliceIter::from_range(&range, ints).map(|(i, rl)| (*i, rl)));
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
        test_actions_on_range_with_rle_opts(input, true, true, actions, output);
    }
    fn test_actions_on_range_no_rle(
        input: impl Iterator<Item = i64>,
        actions: &[FieldAction],
        output: &[(i64, RunLength)],
    ) {
        test_actions_on_range_with_rle_opts(input, false, false, actions, output);
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
        test_actions_on_range(0..3, &[FieldAction::new(Drop, 1, 1)], &[(0, 1), (2, 1)]);
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
