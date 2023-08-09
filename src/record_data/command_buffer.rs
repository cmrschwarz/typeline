use std::cell::Ref;

use nonmax::NonMaxUsize;
use smallvec::SmallVec;

use crate::utils::{aligned_buf::AlignedBuf, universe::Universe};

use super::{
    field_data::{
        FieldData, FieldValueFormat, FieldValueHeader, RunLength,
        MAX_FIELD_ALIGN,
    },
    iter_hall::{FieldDataSource, IterHall, IterState},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FieldActionKind {
    #[default]
    Dup,
    Drop,
}

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct FieldAction {
    pub kind: FieldActionKind,
    pub field_idx: usize,
    pub run_len: RunLength,
}

impl FieldAction {
    pub fn new(
        kind: FieldActionKind,
        field_idx: usize,
        run_len: RunLength,
    ) -> Self {
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

#[derive(Default, Clone)]
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

pub type ActionListIndex = usize;
pub type MergedActionListsIndex = usize;
pub type ActionProducingFieldIndex = NonMaxUsize;
pub type ActionListOrderingId = usize;

#[derive(Default)]
pub struct FieldActionIndices {
    pub min_apf_idx: Option<ActionProducingFieldIndex>,
    pub curr_apf_idx: Option<ActionProducingFieldIndex>,
    pub first_unapplied_al_idx: ActionListIndex,
}

struct ActionList {
    ordering_id: ActionListOrderingId,
    first_unapplied_al_idx_in_prev_apf: ActionListIndex,
    actions_start: usize,
    actions_end: usize,
    // TODO: refcount + always have the next unsused
    // al ready so it can have a recount
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
    #[allow(dead_code)] // TODO: remove fields
    action_lists_index_offset: usize,
    locally_merged_action_lists: Vec<LocallyMergedActionList>,
    locally_merged_actions: Vec<FieldAction>,
    action_lists: Vec<ActionList>,
    actions: Vec<FieldAction>,
}

struct ActionProducingField {
    merged_action_lists: Vec<MergedActionLists>,
}

#[derive(Default)]
pub struct CommandBuffer {
    action_list_ids: ActionListOrderingId,
    first_apf_idx: Option<ActionProducingFieldIndex>,
    last_apf_idx: Option<ActionProducingFieldIndex>,
    action_producing_fields:
        Universe<ActionProducingFieldIndex, ActionProducingField>,
    merged_actions: [std::cell::RefCell<Vec<FieldAction>>; 3],
    copies: Vec<CopyCommand>,
    insertions: Vec<InsertionCommand>,
}
impl FieldActionIndices {
    pub fn new(min_apf_idx: Option<ActionProducingFieldIndex>) -> Self {
        Self {
            min_apf_idx,
            curr_apf_idx: None,
            first_unapplied_al_idx: 0,
        }
    }
}

impl MergedActionLists {
    pub fn is_legal_field_idx_for_action(&self, field_idx: usize) -> bool {
        if let Some(acs) = self.action_lists.last() {
            if acs.actions_end != acs.actions_start {
                self.actions[acs.actions_end - 1].field_idx <= field_idx
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
            let rl_to_push =
                run_length.min(RunLength::MAX as usize) as RunLength;
            self.push_action(kind, field_idx, rl_to_push);
            run_length -= rl_to_push as usize;
        }
    }
    pub fn push_action(
        &mut self,
        kind: FieldActionKind,
        field_idx: usize,
        run_length: RunLength,
    ) {
        assert!(self.is_legal_field_idx_for_action(field_idx));
        let al = self.action_lists.last_mut().unwrap();
        if al.actions_end > al.actions_start {
            // very simple early merging of actions to hopefully save some
            // memory this also allows operations to be slightly
            // more 'wasteful' with their action pushes
            let last = &mut self.actions[al.actions_end - 1];
            if last.kind == kind
                && last.field_idx == field_idx
                && RunLength::MAX as usize
                    > last.run_len as usize + run_length as usize
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
    pub fn begin_action_list(
        &mut self,
        apf_idx: ActionProducingFieldIndex,
    ) -> ActionListIndex {
        let apf = &self.action_producing_fields[apf_idx];
        let start = apf.merged_action_lists[0].actions.len();
        let first_unapplied_idx = apf.merged_action_lists[0]
            .prev_apf_idx
            .map(|prev| {
                self.action_producing_fields[prev].merged_action_lists[0]
                    .action_lists
                    .len()
            })
            .unwrap_or(0);
        let id = self.action_list_ids;
        self.action_list_ids += 1;
        let action_lists = &mut self.action_producing_fields[apf_idx]
            .merged_action_lists[0]
            .action_lists;
        let action_list_id = action_lists.len();
        action_lists.push(ActionList {
            ordering_id: id,
            first_unapplied_al_idx_in_prev_apf: first_unapplied_idx,
            actions_start: start,
            actions_end: start,
        });
        action_list_id
    }
    pub fn end_action_list(&mut self, apf_idx: ActionProducingFieldIndex) {
        let apf = &mut self.action_producing_fields[apf_idx];
        let mal = &mut apf.merged_action_lists[0];
        let al = mal.action_lists.last().unwrap();
        if al.actions_end == al.actions_start {
            mal.action_lists.pop();
        }
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
    pub fn execute_for_iter_hall(
        &mut self,
        field_id: usize, // for debug logging only
        iter_hall: &mut IterHall,
        fai: &mut FieldActionIndices,
    ) {
        if self.first_apf_idx.is_none() {
            return;
        }
        if fai.min_apf_idx.is_none() {
            fai.min_apf_idx = self.first_apf_idx;
        }
        if fai.curr_apf_idx.is_none() {
            fai.curr_apf_idx = fai.min_apf_idx;
        }
        let min_apf_idx = fai.min_apf_idx.unwrap();
        let curr_apf_idx = fai.curr_apf_idx.as_mut().unwrap();
        let prev_curr_apf_idx = *curr_apf_idx;
        let prev_first_unapplied_al_idx = fai.first_unapplied_al_idx;
        let als = self.prepare_action_lists(
            min_apf_idx,
            curr_apf_idx,
            &mut fai.first_unapplied_al_idx,
        );
        if als.actions_start == als.actions_end {
            debug_assert!(
                prev_curr_apf_idx == *curr_apf_idx
                    && prev_first_unapplied_al_idx
                        == fai.first_unapplied_al_idx
            );
            if cfg!(feature = "debug_logging") {
                println!(
                    "executing commands for field {} had no effect: min apf: {}, curr apf: {} [al {}]",
                    field_id, min_apf_idx, curr_apf_idx, fai.first_unapplied_al_idx,
                );
            }
            return;
        }
        if cfg!(feature = "debug_logging") {
            println!("--------------    <execution (field {field_id}) start>      --------------");
            println!("command buffer:");
            for (apf_idx, apf) in
                self.action_producing_fields.iter().enumerate()
            {
                println!("  apf {}:", apf_idx,);
                if apf.merged_action_lists[0].action_lists.is_empty() {
                    println!("    empty");
                } else {
                    for al in &apf.merged_action_lists[0].action_lists {
                        println!("    al {}:", al.ordering_id);
                        let actions = &apf.merged_action_lists[0].actions
                            [al.actions_start..al.actions_end];
                        if actions.is_empty() {
                            println!("      > empty")
                        } else {
                            for a in actions {
                                println!("      > {:?}:", a);
                            }
                        }
                    }
                }
            }
            println!(
                "executing commands: min apf: {}, curr apf: {} [al {}] -> {} [al {}]: ",
                min_apf_idx,
                prev_curr_apf_idx,
                prev_first_unapplied_al_idx,
                curr_apf_idx,
                fai.first_unapplied_al_idx,
            );
            let refs = self.get_merge_result_mal_ref(&als);
            let actions = self.get_merge_resuls_slice(refs.as_deref(), &als);
            for a in actions {
                println!("  > {:?}:", a);
            }
            if actions.is_empty() {
                println!("  > empty");
            }
            println!("--------------    </execution (field {field_id}) end>      --------------");
        }
        // TODO: avoid this allocation
        let mut iterators = IterStateSmallVec::new();
        iterators.extend(iter_hall.iters.iter_mut().map(|it| it.get_mut()));
        iterators.sort_by(|lhs, rhs| rhs.field_pos.cmp(&lhs.field_pos));
        let (headers, data, field_count) = match &mut iter_hall.data_source {
            FieldDataSource::Owned(fd) => {
                (&mut fd.headers, Some(&mut fd.data), &mut fd.field_count)
            }
            FieldDataSource::DataCow {
                headers,
                field_count,
                ..
            } => (headers, None, field_count),
            FieldDataSource::RecordBufferDataCow {
                headers,
                field_count,
                ..
            } => (headers, None, field_count),
            FieldDataSource::Cow(_) | FieldDataSource::RecordBufferCow(_) => {
                panic!("cannot execute commands on COW iter hall")
            }
        };

        let field_count_delta = self.generate_commands_from_actions(
            als,
            headers,
            data,
            &mut iterators,
            0,
            0,
        );

        *field_count = (*field_count as isize + field_count_delta) as usize;
        self.execute_commands(headers);
        self.cleanup();
    }
    pub fn requires_any_actions(
        &mut self,
        fai: &mut FieldActionIndices,
    ) -> bool {
        let first = if let Some(idx) = self.first_apf_idx {
            idx
        } else {
            return false;
        };
        let min = if let Some(min) = fai.min_apf_idx {
            min
        } else {
            fai.min_apf_idx = self.first_apf_idx;
            first
        };
        let curr = if let Some(curr) = fai.curr_apf_idx {
            curr
        } else {
            fai.curr_apf_idx = fai.min_apf_idx;
            min
        };

        let mut mal =
            &self.action_producing_fields[curr].merged_action_lists[0];
        if mal.action_lists.len() > fai.first_unapplied_al_idx {
            return true;
        }

        while let Some(next) = mal.next_apf_idx {
            mal = &self.action_producing_fields[next].merged_action_lists[0];
            if !mal.action_lists.is_empty() {
                return true;
            }
        }
        false
    }
    pub fn drop_field_commands(
        &mut self,
        field_id: usize, // for debug logging only
        fai: &mut FieldActionIndices,
    ) {
        if self.first_apf_idx.is_none() {
            return;
        }
        if fai.min_apf_idx.is_none() {
            fai.min_apf_idx = self.first_apf_idx;
        }
        if fai.curr_apf_idx.is_none() {
            fai.curr_apf_idx = fai.min_apf_idx;
        }

        let min_apf_idx = fai.min_apf_idx.unwrap();
        let curr_apf_idx = fai.curr_apf_idx.as_mut().unwrap();
        let first_unapplied_al_idx = &mut fai.first_unapplied_al_idx;
        let prev_curr_apf_idx = *curr_apf_idx;
        let prev_first_unapplied_apf_idx = *first_unapplied_al_idx;
        // TODO: this is pretty wasteful. figure out a better way to do this
        self.prepare_action_lists(
            min_apf_idx,
            curr_apf_idx,
            first_unapplied_al_idx,
        );
        self.cleanup();
        if cfg!(feature = "debug_logging")
            && prev_first_unapplied_apf_idx != *first_unapplied_al_idx
            || prev_curr_apf_idx != *curr_apf_idx
        {
            println!(
                    "dropping commands for field {}: min apf {}: curr apf {} [al {}] -> {} [al {}]",
                    field_id,
                    min_apf_idx,
                    prev_curr_apf_idx,
                    prev_first_unapplied_apf_idx,
                    curr_apf_idx,
                    first_unapplied_al_idx
                )
        }
    }
    pub fn execute_for_field_data(
        &mut self,
        field: &mut FieldData,
        min_apf_idx: ActionProducingFieldIndex,
        curr_apf_idx: &mut ActionProducingFieldIndex,
        first_unapplied_al_idx_in_curr_apf: &mut ActionListIndex,
    ) {
        let als = self.prepare_action_lists(
            min_apf_idx,
            curr_apf_idx,
            first_unapplied_al_idx_in_curr_apf,
        );
        let field_count_delta = self.generate_commands_from_actions(
            als,
            &mut field.headers,
            Some(&mut field.data),
            &mut SmallVec::new(),
            0,
            0,
        );
        self.execute_commands(&mut field.headers);
        field.field_count =
            (field.field_count as isize + field_count_delta) as usize;
        self.cleanup();
    }

    fn cleanup(&mut self) {
        self.insertions.clear();
        self.copies.clear();
    }
    pub fn claim_apf(&mut self) -> ActionProducingFieldIndex {
        let apf_idx = self.action_producing_fields.peek_claim_id();
        let mal_count = (apf_idx.get() + 1).trailing_zeros() as usize + 1;
        let mut apf = ActionProducingField {
            merged_action_lists: Vec::with_capacity(mal_count),
        };
        for _ in 0..mal_count {
            apf.merged_action_lists.push(MergedActionLists {
                prev_apf_idx: self.last_apf_idx,
                next_apf_idx: None,
                action_lists_index_offset: 0,
                locally_merged_action_lists: Default::default(),
                locally_merged_actions: Default::default(),
                action_lists: Default::default(),
                actions: Default::default(),
            });
        }
        let idx = self.action_producing_fields.claim_with_value(apf);
        self.last_apf_idx = Some(idx);
        if self.first_apf_idx.is_none() {
            self.first_apf_idx = Some(idx);
        }
        idx
    }
    pub fn peek_next_apf_id(&self) -> ActionProducingFieldIndex {
        self.action_producing_fields.peek_claim_id()
    }
}

// action list merging
impl CommandBuffer {
    fn push_merged_action(
        target: &mut Vec<FieldAction>,
        first_insert: &mut bool,
        kind: FieldActionKind,
        field_idx: usize,
        mut run_len: usize,
    ) {
        if *first_insert {
            if run_len == 0 {
                return;
            }
            *first_insert = false;
        } else {
            let prev = target.last_mut().unwrap();
            if prev.field_idx == field_idx && prev.kind == kind {
                let space_rem = (RunLength::MAX as usize
                    - prev.run_len as usize)
                    .min(run_len);
                prev.run_len += space_rem as RunLength;
                run_len -= space_rem;
            }
        }
        let mut action = FieldAction {
            kind,
            field_idx,
            run_len: 0,
        };
        while run_len > RunLength::MAX as usize {
            action.run_len = RunLength::MAX;
            target.push(action);
            run_len -= RunLength::MAX as usize;
        }
        if run_len > 0 {
            action.run_len = run_len as RunLength;
            target.push(action);
        }
    }
    fn merge_two_action_lists_raw(
        sets: [&[FieldAction]; 2],
        target: &mut Vec<FieldAction>,
    ) {
        let left_list = sets[0];
        let right_list = sets[1];
        let mut first_insert = true;

        let (mut curr_action_idx_left, mut curr_action_idx_right) = (0, 0);
        let mut next_action_field_idx_left;
        let mut next_action_field_idx_right;
        let mut field_pos_offset_left = 0isize;
        let mut outstanding_drops_right = 0usize;
        loop {
            if curr_action_idx_left == left_list.len() {
                break;
            }
            if curr_action_idx_right < right_list.len() {
                next_action_field_idx_right =
                    right_list[curr_action_idx_right].field_idx
                        + outstanding_drops_right;
            } else {
                next_action_field_idx_right = usize::MAX;
            }
            next_action_field_idx_left =
                (left_list[curr_action_idx_left].field_idx as isize
                    + field_pos_offset_left) as usize;
            if next_action_field_idx_left <= next_action_field_idx_right {
                let left = &left_list[curr_action_idx_left];
                let field_idx =
                    (left.field_idx as isize + field_pos_offset_left) as usize;
                let mut run_len = left.run_len as usize;
                let mut kind = left.kind;
                let space_to_next = left_list
                    .get(curr_action_idx_left + 1)
                    .map(|a| a.field_idx - left.field_idx)
                    .unwrap_or(usize::MAX);
                match left.kind {
                    FieldActionKind::Dup => {
                        if outstanding_drops_right >= run_len {
                            kind = FieldActionKind::Drop;
                            outstanding_drops_right -= run_len;
                            run_len =
                                outstanding_drops_right.min(space_to_next);
                            outstanding_drops_right -= run_len;
                            field_pos_offset_left -= run_len as isize;
                        } else {
                            run_len -= outstanding_drops_right;
                            outstanding_drops_right = 0;
                        }
                    }
                    FieldActionKind::Drop => {
                        outstanding_drops_right += run_len;
                        run_len = outstanding_drops_right.min(space_to_next);
                        outstanding_drops_right -= run_len;
                    }
                }
                Self::push_merged_action(
                    target,
                    &mut first_insert,
                    kind,
                    field_idx,
                    run_len,
                );
                curr_action_idx_left += 1;
            } else {
                debug_assert!(outstanding_drops_right == 0);
                let right = &right_list[curr_action_idx_right];
                let field_idx = right.field_idx;
                let mut run_len = right.run_len as usize;

                match right.kind {
                    FieldActionKind::Dup => {
                        field_pos_offset_left += run_len as isize;
                    }
                    FieldActionKind::Drop => {
                        let gap_to_start_left = next_action_field_idx_left
                            - next_action_field_idx_right;
                        if gap_to_start_left < run_len {
                            outstanding_drops_right +=
                                run_len - gap_to_start_left;
                            run_len = gap_to_start_left;
                        }
                        field_pos_offset_left -= run_len as isize;
                    }
                }
                Self::push_merged_action(
                    target,
                    &mut first_insert,
                    right.kind,
                    field_idx,
                    run_len,
                );
                curr_action_idx_right += 1;
            }
        }
        for action in &right_list[curr_action_idx_right..] {
            Self::push_merged_action(
                target,
                &mut first_insert,
                action.kind,
                action.field_idx,
                action.run_len as usize,
            );
        }
    }
    fn get_merge_result_mal_ref<'a>(
        &'a self,
        almr: &ActionListMergeResult,
    ) -> Option<Ref<'a, Vec<FieldAction>>> {
        if let ActionListMergeLocation::CbActionList { idx } = almr.location {
            return Some(self.merged_actions[idx].borrow());
        }
        None
    }
    fn get_merge_resuls_slice<'a>(
        &'a self,
        mal_ref: Option<&'a Vec<FieldAction>>,
        almr: &ActionListMergeResult,
    ) -> &'a [FieldAction] {
        let range = almr.actions_start..almr.actions_end;
        match almr.location {
            ActionListMergeLocation::ApfMal { apf_idx, mal_idx } => {
                &self.action_producing_fields[apf_idx].merged_action_lists
                    [mal_idx]
                    .actions[range]
            }
            ActionListMergeLocation::ApfLocal { apf_idx, mal_idx } => {
                &self.action_producing_fields[apf_idx].merged_action_lists
                    [mal_idx]
                    .locally_merged_actions[range]
            }
            ActionListMergeLocation::CbActionList { .. } => {
                &mal_ref.as_ref().unwrap()[range]
            }
            ActionListMergeLocation::Empty => &[],
        }
    }
    fn unclaim_merge_space(&mut self, almr: ActionListMergeResult) {
        let idx = match almr.location {
            ActionListMergeLocation::ApfMal { .. } => return,
            ActionListMergeLocation::ApfLocal { .. } => return,
            ActionListMergeLocation::Empty => return,
            ActionListMergeLocation::CbActionList { idx } => idx,
        };
        let mal = &mut self.merged_actions[idx].borrow_mut();
        debug_assert!(mal.len() == almr.actions_end);
        mal.truncate(almr.actions_start);
    }
    fn merge_two_action_lists(
        &mut self,
        first: ActionListMergeResult,
        second: ActionListMergeResult,
    ) -> ActionListMergeResult {
        let mut target_idx = 0;
        if let ActionListMergeLocation::CbActionList { idx } = first.location {
            if idx == target_idx {
                target_idx += 1;
            }
        }
        if let ActionListMergeLocation::CbActionList { idx } = second.location
        {
            if idx == target_idx {
                target_idx += 1;
            }
        }

        let first_ref = self.get_merge_result_mal_ref(&first);
        let second_ref = self.get_merge_result_mal_ref(&second);
        let first_slice =
            self.get_merge_resuls_slice(first_ref.as_deref(), &first);
        let second_slice =
            self.get_merge_resuls_slice(second_ref.as_deref(), &second);
        let res_size = first_slice.len() + second_slice.len();
        let mut res_ms = self.merged_actions[target_idx].borrow_mut();
        res_ms.reserve(res_size);
        let res_len_before = res_ms.len();
        CommandBuffer::merge_two_action_lists_raw(
            [first_slice, second_slice],
            &mut res_ms,
        );
        let res_len_after = res_ms.len();
        drop(res_ms);
        drop(first_ref);
        drop(second_ref);
        self.unclaim_merge_space(first);
        self.unclaim_merge_space(second);
        ActionListMergeResult {
            location: ActionListMergeLocation::CbActionList {
                idx: target_idx,
            },
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
        let al = &self.action_producing_fields[apf_idx].merged_action_lists
            [mal_idx]
            .action_lists[al_idx];
        ActionListMergeResult {
            location: ActionListMergeLocation::ApfMal { apf_idx, mal_idx },
            actions_start: al.actions_start,
            actions_end: al.actions_end,
        }
    }
    fn locally_merged_action_list_as_result(
        &self,
        apf_idx: ActionProducingFieldIndex,
        mal_idx: MergedActionListsIndex,
        al_idx: ActionListIndex,
    ) -> ActionListMergeResult {
        let al = &self.action_producing_fields[apf_idx].merged_action_lists
            [mal_idx]
            .locally_merged_action_lists[al_idx];
        ActionListMergeResult {
            location: ActionListMergeLocation::ApfLocal { apf_idx, mal_idx },
            actions_start: al.actions_start,
            actions_end: al.actions_end,
        }
    }
    fn construct_missing_local_merges(
        &mut self,
        apf_idx: ActionProducingFieldIndex,
        mal_idx: MergedActionListsIndex,
        required_al_idx_end: ActionListIndex,
    ) {
        let apf = &mut self.action_producing_fields[apf_idx];
        let mal = &mut apf.merged_action_lists[mal_idx];
        let mut last_end =
            if let Some(last) = mal.locally_merged_action_lists.last() {
                last.al_idx_end
            } else {
                0
            };
        loop {
            if last_end + 1 >= required_al_idx_end {
                return;
            }
            let end = last_end + 2;
            let max_width = 1 << end.trailing_zeros();
            let l1 = &mal.action_lists[end - 2];
            let l2 = &mal.action_lists[end - 1];
            let lists = [
                &mal.actions[l1.actions_start..l1.actions_end],
                &mal.actions[l2.actions_start..l2.actions_end],
            ];
            let actions_start = mal.locally_merged_actions.len();
            Self::merge_two_action_lists_raw(
                lists,
                &mut mal.locally_merged_actions,
            );

            let lmal = LocallyMergedActionList {
                al_idx_start: end - 2,
                al_idx_end: end,
                actions_start,
                actions_end: mal.locally_merged_actions.len(),
            };
            mal.locally_merged_action_lists.push(lmal);
            let mut curr = mal.locally_merged_action_lists.len() - 1;
            let mut prev = curr
                - if last_end == 0 {
                    0
                } else {
                    last_end.trailing_zeros() as usize
                };
            let mut width = 4;
            while width <= max_width {
                let l1 = &mal.locally_merged_action_lists[prev];
                let l2 = &mal.locally_merged_action_lists[curr];
                let lists = [
                    &mal.locally_merged_actions
                        [l1.actions_start..l1.actions_end],
                    &mal.locally_merged_actions
                        [l2.actions_start..l2.actions_end],
                ];
                let actions_start = mal.locally_merged_actions.len();
                let mut temp = self.merged_actions[0].borrow_mut();
                let len_before = temp.len();
                Self::merge_two_action_lists_raw(lists, &mut temp);
                mal.locally_merged_actions.extend(&temp[len_before..]);
                temp.truncate(len_before);
                drop(temp);
                let lmal = LocallyMergedActionList {
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
    fn merge_apf_action_list_plain(
        &mut self,
        apf_idx: ActionProducingFieldIndex,
        mal_idx: MergedActionListsIndex,
        al_idx_start: ActionListIndex,
        al_idx_end: Option<ActionListIndex>,
    ) -> ActionListMergeResult {
        let apf = &self.action_producing_fields[apf_idx];
        let mal = &apf.merged_action_lists[mal_idx];
        let al_idx_end = al_idx_end.unwrap_or(mal.action_lists.len());

        if al_idx_start == mal.action_lists.len() {
            return ActionListMergeResult::default();
        }
        if al_idx_start + 1 == mal.action_lists.len() {
            return self.action_list_as_result(apf_idx, 0, al_idx_start);
        }
        debug_assert!(al_idx_start < mal.action_lists.len());
        self.construct_missing_local_merges(apf_idx, mal_idx, al_idx_end);
        if al_idx_end - al_idx_start == 1 {
            return self.action_list_as_result(apf_idx, mal_idx, al_idx_start);
        }
        if al_idx_end - al_idx_start == 2 && al_idx_start & 1 == 1 {
            return self.merge_two_action_lists(
                self.action_list_as_result(apf_idx, mal_idx, al_idx_start),
                self.action_list_as_result(apf_idx, mal_idx, al_idx_start + 1),
            );
        }
        let apf = &self.action_producing_fields[apf_idx];
        let mal = &apf.merged_action_lists[mal_idx];
        let mut lmal_idx = mal.locally_merged_action_lists.len() - 1;
        let mut lmal = &mal.locally_merged_action_lists[lmal_idx];
        while lmal.al_idx_end > al_idx_end || lmal.al_idx_start < al_idx_start
        {
            lmal_idx -= 1;
            lmal = &mal.locally_merged_action_lists[lmal_idx];
        }

        let mut rhs = if al_idx_end
            != mal.locally_merged_action_lists[lmal_idx].al_idx_end
        {
            self.merge_two_action_lists(
                self.locally_merged_action_list_as_result(
                    apf_idx, mal_idx, lmal_idx,
                ),
                self.action_list_as_result(apf_idx, mal_idx, al_idx_end - 1),
            )
        } else {
            self.locally_merged_action_list_as_result(
                apf_idx, mal_idx, lmal_idx,
            )
        };
        let mut last_used_lmal = lmal_idx;
        loop {
            let apf = &self.action_producing_fields[apf_idx];
            let mal = &apf.merged_action_lists[mal_idx];
            let prev_lmal = &mal.locally_merged_action_lists[last_used_lmal];
            if prev_lmal.al_idx_start == al_idx_start {
                return rhs;
            }
            if prev_lmal.al_idx_start == al_idx_start + 1 {
                let lhs =
                    self.action_list_as_result(apf_idx, mal_idx, al_idx_start);
                return self.merge_two_action_lists(lhs, rhs);
            }
            lmal_idx -= 1;
            let mut lmal = &mal.locally_merged_action_lists[lmal_idx];
            while lmal.al_idx_start >= prev_lmal.al_idx_start
                || lmal.al_idx_start < al_idx_start
            {
                lmal_idx -= 1;
                lmal = &mal.locally_merged_action_lists[lmal_idx];
            }
            let lhs = self.locally_merged_action_list_as_result(
                apf_idx, mal_idx, lmal_idx,
            );
            rhs = self.merge_two_action_lists(lhs, rhs);
            last_used_lmal = lmal_idx;
        }
    }
    fn merge_apf_action_list_mal_0(
        &mut self,
        apf_idx: ActionProducingFieldIndex,
        first_unapplied_al_idx_in_mal: ActionListIndex,
    ) -> ActionListMergeResult {
        self.merge_apf_action_list_plain(
            apf_idx,
            0 as MergedActionListsIndex,
            first_unapplied_al_idx_in_mal,
            None,
        )
    }
    fn merge_apf_action_lists_with_crossover(
        &mut self,
        apf_idx: ActionProducingFieldIndex,
        mal_idx: MergedActionListsIndex,
        first_unapplied_al_idx_in_mal: ActionListIndex,
    ) -> ActionListMergeResult {
        if mal_idx == 0 {
            return self.merge_apf_action_list_mal_0(
                apf_idx,
                first_unapplied_al_idx_in_mal,
            );
        }
        let apf = &self.action_producing_fields[apf_idx];
        let mal = &apf.merged_action_lists[mal_idx];
        let rhs = if let Some(unapplied_al) =
            mal.action_lists.get(first_unapplied_al_idx_in_mal)
        {
            self.merge_apf_action_list_plain(
                mal.prev_apf_idx.unwrap(),
                mal_idx,
                unapplied_al.first_unapplied_al_idx_in_prev_apf,
                None,
            )
        } else {
            return ActionListMergeResult::default();
        };
        let lhs = self.merge_apf_action_list_plain(
            apf_idx,
            mal_idx,
            first_unapplied_al_idx_in_mal,
            None,
        );
        self.merge_two_action_lists(lhs, rhs)
    }
    fn merge_action_lists(
        &mut self,
        min_apf_idx: ActionProducingFieldIndex,
        first_unapplied_al_idx_in_min_apf: Option<ActionListIndex>,
        mut max_apf_idx: ActionProducingFieldIndex,
        mut first_unapplied_al_idx_in_max_apf: ActionListIndex,
    ) -> ActionListMergeResult {
        if min_apf_idx == max_apf_idx {
            if let Some(fual) = first_unapplied_al_idx_in_min_apf {
                debug_assert!(fual <= first_unapplied_al_idx_in_max_apf);
                first_unapplied_al_idx_in_max_apf = fual;
            }
            return self.merge_apf_action_list_mal_0(
                min_apf_idx,
                first_unapplied_al_idx_in_max_apf,
            );
        }
        let apf = &self.action_producing_fields[max_apf_idx];
        let mut mal_idx = 0;
        let mut mal = &apf.merged_action_lists[mal_idx];
        if mal.action_lists.len() == first_unapplied_al_idx_in_max_apf {
            max_apf_idx = mal.prev_apf_idx.unwrap();
            if first_unapplied_al_idx_in_max_apf != 0 {
                first_unapplied_al_idx_in_max_apf = mal.action_lists
                    [first_unapplied_al_idx_in_max_apf]
                    .first_unapplied_al_idx_in_prev_apf;
            }
            return self.merge_action_lists(
                min_apf_idx,
                first_unapplied_al_idx_in_min_apf,
                max_apf_idx,
                first_unapplied_al_idx_in_max_apf,
            );
        }

        while mal.prev_apf_idx.unwrap() > min_apf_idx {
            if mal_idx == apf.merged_action_lists.len() {
                break;
            }
            mal_idx += 1;
            mal = &apf.merged_action_lists[mal_idx];
        }
        let prev_apf_idx =
            apf.merged_action_lists[mal_idx].prev_apf_idx.unwrap();
        let prev_apf_first_unapplied_al = mal.action_lists
            [first_unapplied_al_idx_in_max_apf]
            .first_unapplied_al_idx_in_prev_apf;

        let prev = self.merge_action_lists(
            min_apf_idx,
            first_unapplied_al_idx_in_min_apf,
            prev_apf_idx,
            prev_apf_first_unapplied_al,
        );
        let curr = self.merge_apf_action_lists_with_crossover(
            max_apf_idx,
            mal_idx,
            first_unapplied_al_idx_in_max_apf,
        );
        if mal_idx == 0 {
            self.merge_two_action_lists(prev, curr)
        } else {
            self.merge_two_action_lists(curr, prev)
        }
    }
    fn prepare_action_lists(
        &mut self,
        min_apf_idx: ActionProducingFieldIndex,
        curr_apf_idx: &mut ActionProducingFieldIndex,
        first_unapplied_al_idx_in_curr_apf: &mut ActionListIndex,
    ) -> ActionListMergeResult {
        let mut last_apf_idx = *curr_apf_idx;
        let mut new_curr_apf_idx = last_apf_idx;
        let mal =
            &self.action_producing_fields[last_apf_idx].merged_action_lists[0];
        let mut new_first_unapplied_al_idx = mal.action_lists.len();
        let mut new_first_unapplied_al_ordering_id = mal
            .action_lists
            .last()
            .map(|al| al.ordering_id)
            .unwrap_or(0);
        while let Some(next) = self.action_producing_fields[last_apf_idx]
            .merged_action_lists[0]
            .next_apf_idx
        {
            last_apf_idx = next;
            let mal = &self.action_producing_fields[last_apf_idx]
                .merged_action_lists[0];
            if let Some(al) = mal.action_lists.last() {
                if al.ordering_id > new_first_unapplied_al_ordering_id {
                    new_curr_apf_idx = last_apf_idx;
                    new_first_unapplied_al_idx = mal.action_lists.len();
                    new_first_unapplied_al_ordering_id = al.ordering_id;
                }
            }
        }
        let res;
        if last_apf_idx != *curr_apf_idx {
            let rhs = self.merge_action_lists(
                *curr_apf_idx,
                Some(*first_unapplied_al_idx_in_curr_apf),
                last_apf_idx,
                0,
            );
            let lhs = self.merge_action_lists(
                min_apf_idx,
                None,
                *curr_apf_idx,
                *first_unapplied_al_idx_in_curr_apf,
            );
            res = self.merge_two_action_lists(lhs, rhs);
            *curr_apf_idx = last_apf_idx;
        } else {
            res = self.merge_action_lists(
                min_apf_idx,
                None,
                *curr_apf_idx,
                *first_unapplied_al_idx_in_curr_apf,
            );
        }
        *curr_apf_idx = new_curr_apf_idx;
        *first_unapplied_al_idx_in_curr_apf = new_first_unapplied_al_idx;
        res
    }
}

type IterStateSmallVec<'a> = SmallVec<[&'a mut IterState; 6]>;

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
            if it.field_pos > field_pos {
                it.field_pos -= amount as usize;
                it.header_rl_offset -= amount;
            }
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
        iterators: &mut IterStateSmallVec<'_>,
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
    fn generate_commands_from_actions(
        &mut self,
        merged_actions: ActionListMergeResult,
        headers: &mut Vec<FieldValueHeader>,
        data: Option<&mut AlignedBuf<MAX_FIELD_ALIGN>>,
        iterators: &mut IterStateSmallVec<'_>,
        mut header_idx: usize,
        mut field_pos: usize,
    ) -> isize {
        debug_assert!(
            merged_actions.actions_start != merged_actions.actions_end
        );
        debug_assert!(!headers.is_empty());
        let mut header;
        let mut header_idx_new = header_idx;

        let mut action_idx_next = 0;
        let mut copy_range_start = 0;
        let mut copy_range_start_new = 0;
        let mut field_pos_old = field_pos;
        let mut curr_action_pos = 0;
        let mut curr_action_pos_outstanding_dups = 0;
        let mut curr_action_pos_outstanding_drops = 0;
        let mut curr_header_original_rl =
            headers.first().map(|h| h.run_length).unwrap_or(0);
        let mut data_end = 0;
        let mut curr_header_iter_count = 0;

        let header_end = headers[0].run_length as usize;
        for it in iterators.iter().rev() {
            if it.field_pos >= header_end {
                break;
            }
            curr_header_iter_count += 1;
        }

        'advance_action: loop {
            let mal_ref = self.get_merge_result_mal_ref(&merged_actions);
            let actions = self
                .get_merge_resuls_slice(mal_ref.as_deref(), &merged_actions);
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
                        curr_action_pos_outstanding_dups +=
                            action.run_len as usize;
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
            }
            drop(mal_ref);
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
                    let header_end_old =
                        field_pos_old + header.run_length as usize;
                    let field_pos_delta =
                        field_pos as isize - field_pos_old as isize;
                    iterators
                        .drain(iterators.len() - curr_header_iter_count..);
                    let len = iterators.len();
                    curr_header_iter_count = 0;
                    while len > curr_header_iter_count {
                        let it =
                            &mut iterators[len - curr_header_iter_count - 1];
                        if it.field_pos >= header_end_old {
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
                    curr_header_original_rl = headers[header_idx].run_length;
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
}

// final execution step
impl CommandBuffer {
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
}

#[cfg(test)]
mod test {
    use crate::record_data::{
        field_data::{FieldData, RunLength},
        iters::FieldIterator,
        push_interface::PushInterface,
        typed::TypedSlice,
        typed_iters::TypedSliceIter,
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
        let mut apf_idx = cb.claim_apf();
        cb.begin_action_list(apf_idx);
        for a in actions {
            cb.push_action(apf_idx, a.kind, a.field_idx, a.run_len);
        }
        cb.execute_for_field_data(&mut fd, apf_idx, &mut apf_idx, &mut 0);
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

    fn test_raw_field_merge(
        left: &[FieldAction],
        right: &[FieldAction],
        out: &[FieldAction],
    ) {
        let mut output = Vec::new();
        super::CommandBuffer::merge_two_action_lists_raw(
            [left, right],
            &mut output,
        );
        assert_eq!(output.as_slice(), out);
    }

    #[test]
    fn actions_are_merged() {
        for kind in [FieldActionKind::Dup, FieldActionKind::Drop] {
            let unmerged = &[
                FieldAction {
                    kind,
                    field_idx: 0,
                    run_len: 1,
                },
                FieldAction {
                    kind,
                    field_idx: 0,
                    run_len: 1,
                },
            ];
            let blank = &[];
            let merged = &[FieldAction {
                kind,
                field_idx: 0,
                run_len: 2,
            }];
            test_raw_field_merge(unmerged, blank, merged);
            test_raw_field_merge(blank, unmerged, merged);
        }
    }
    #[test]
    fn left_field_indices_are_adjusted() {
        let left = &[FieldAction {
            kind: FieldActionKind::Drop,
            field_idx: 1,
            run_len: 1,
        }];
        let right = &[FieldAction {
            kind: FieldActionKind::Dup,
            field_idx: 0,
            run_len: 5,
        }];
        let merged = &[
            FieldAction {
                kind: FieldActionKind::Dup,
                field_idx: 0,
                run_len: 5,
            },
            FieldAction {
                kind: FieldActionKind::Drop,
                field_idx: 6,
                run_len: 1,
            },
        ];
        test_raw_field_merge(left, right, merged);
    }

    #[test]
    fn encompassed_dups_are_deleted() {
        let left = &[FieldAction {
            kind: FieldActionKind::Dup,
            field_idx: 1,
            run_len: 1,
        }];
        let right = &[FieldAction {
            kind: FieldActionKind::Drop,
            field_idx: 0,
            run_len: 5,
        }];
        let merged = &[FieldAction {
            kind: FieldActionKind::Drop,
            field_idx: 0,
            run_len: 4,
        }];
        test_raw_field_merge(left, right, merged);
    }

    #[test]
    fn interrupted_left_actions() {
        let left = &[
            FieldAction {
                kind: FieldActionKind::Dup,
                field_idx: 1,
                run_len: 1,
            },
            FieldAction {
                kind: FieldActionKind::Dup,
                field_idx: 10,
                run_len: 1,
            },
        ];
        let right = &[
            FieldAction {
                kind: FieldActionKind::Drop,
                field_idx: 0,
                run_len: 5,
            },
            FieldAction {
                kind: FieldActionKind::Drop,
                field_idx: 2,
                run_len: 3,
            },
        ];
        let merged = &[
            FieldAction {
                kind: FieldActionKind::Drop,
                field_idx: 0,
                run_len: 4,
            },
            FieldAction {
                kind: FieldActionKind::Drop,
                field_idx: 2,
                run_len: 3,
            },
            FieldAction {
                kind: FieldActionKind::Dup,
                field_idx: 3,
                run_len: 1,
            },
        ];
        test_raw_field_merge(left, right, merged);
    }

    #[test]
    fn chained_right_drops() {
        let left = &[FieldAction {
            kind: FieldActionKind::Dup,
            field_idx: 10,
            run_len: 1,
        }];
        let right = &[
            FieldAction {
                kind: FieldActionKind::Drop,
                field_idx: 0,
                run_len: 1,
            },
            FieldAction {
                kind: FieldActionKind::Drop,
                field_idx: 2,
                run_len: 1,
            },
        ];
        let merged = &[
            FieldAction {
                kind: FieldActionKind::Drop,
                field_idx: 0,
                run_len: 1,
            },
            FieldAction {
                kind: FieldActionKind::Drop,
                field_idx: 2,
                run_len: 1,
            },
            FieldAction {
                kind: FieldActionKind::Dup,
                field_idx: 8,
                run_len: 1,
            },
        ];
        test_raw_field_merge(left, right, merged);
    }

    #[test]
    fn overlapping_drops() {
        let a = &[FieldAction {
            kind: FieldActionKind::Drop,
            field_idx: 3,
            run_len: 5,
        }];
        let b = &[FieldAction {
            kind: FieldActionKind::Drop,
            field_idx: 2,
            run_len: 3,
        }];
        let merged_a_b = &[FieldAction {
            kind: FieldActionKind::Drop,
            field_idx: 2,
            run_len: 8,
        }];
        let merged_b_a = &[
            FieldAction {
                kind: FieldActionKind::Drop,
                field_idx: 2,
                run_len: 3,
            },
            FieldAction {
                kind: FieldActionKind::Drop,
                field_idx: 3,
                run_len: 5,
            },
        ];
        test_raw_field_merge(a, b, merged_a_b);
        test_raw_field_merge(b, a, merged_b_a);
    }
}
