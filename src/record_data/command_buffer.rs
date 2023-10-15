use std::{
    cell::{Ref, RefCell},
    ops::DerefMut,
};

use super::{
    field::{FieldId, FieldManager},
    field_action::{merge_action_lists, FieldAction, FieldActionKind},
    field_action_applicator::FieldActionApplicator,
    field_data::{FieldData, RunLength},
    iter_hall::FieldDataSource,
};

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

pub type ActionListIndex = usize;
pub type MergedActionListsIndex = usize;
pub type ActionProducingFieldIndex = usize;
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
    pending_action_list: Option<ActionList>,
}

#[derive(Default)]
pub struct CommandBuffer {
    action_list_ids: ActionListOrderingId,
    first_apf_idx: Option<ActionProducingFieldIndex>,
    last_apf_idx: Option<ActionProducingFieldIndex>,
    action_producing_fields: Vec<ActionProducingField>,
    merged_actions: [std::cell::RefCell<Vec<FieldAction>>; 3],
    actions_applicator: FieldActionApplicator,
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

impl CommandBuffer {
    pub fn begin_action_list(&mut self, apf_idx: ActionProducingFieldIndex) {
        let apf = &mut self.action_producing_fields[apf_idx];
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
        self.action_producing_fields[apf_idx].pending_action_list =
            Some(ActionList {
                ordering_id: id,
                first_unapplied_al_idx_in_prev_apf: first_unapplied_idx,
                actions_start: start,
                actions_end: start,
            });
    }
    pub fn is_legal_field_idx_for_action(
        &self,
        apf_idx: ActionProducingFieldIndex,
        field_idx: usize,
    ) -> bool {
        let apf = &self.action_producing_fields[apf_idx];
        if let Some(al) = &apf.pending_action_list {
            if al.actions_end != al.actions_start {
                apf.merged_action_lists[0].actions[al.actions_end - 1]
                    .field_idx
                    <= field_idx
            } else {
                true
            }
        } else {
            false
        }
    }
    pub fn end_action_list(&mut self, apf_idx: ActionProducingFieldIndex) {
        let apf = &mut self.action_producing_fields[apf_idx];
        let mal = &mut apf.merged_action_lists[0];
        let al = apf.pending_action_list.take().unwrap();
        if al.actions_end != al.actions_start {
            #[cfg(feature = "debug_logging")]
            {
                println!(
                    "apf {}: added al {}:",
                    apf_idx,
                    mal.action_lists.len()
                );
                for a in &mal.actions[al.actions_start..al.actions_end] {
                    println!("   > {:?}:", a);
                }
            }
            mal.action_lists.push(al);
        }
    }
    pub fn push_action_with_usize_rl(
        &mut self,
        apf_idx: ActionProducingFieldIndex,
        kind: FieldActionKind,
        field_idx: usize,
        mut run_length: usize,
    ) {
        while run_length > 0 {
            let rl_to_push =
                run_length.min(RunLength::MAX as usize) as RunLength;
            // PERF: this sucks
            self.push_action(apf_idx, kind, field_idx, rl_to_push);
            run_length -= rl_to_push as usize;
        }
    }
    pub fn push_action(
        &mut self,
        apf_idx: ActionProducingFieldIndex,
        kind: FieldActionKind,
        field_idx: usize,
        run_length: RunLength,
    ) {
        assert!(self.is_legal_field_idx_for_action(apf_idx, field_idx));
        let apf = &mut self.action_producing_fields[apf_idx];
        let al = apf.pending_action_list.as_mut().unwrap();
        let mal = &mut apf.merged_action_lists[0];
        if al.actions_end > al.actions_start {
            // very simple early merging of actions to hopefully save some
            // memory this also allows operations to be slightly
            // more 'wasteful' with their action pushes
            let last = &mut mal.actions[al.actions_end - 1];
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
        mal.actions.push(FieldAction {
            kind,
            field_idx,
            run_len: run_length,
        });
    }

    pub fn execute(&mut self, fm: &FieldManager, field_id: FieldId) {
        let mut field_ref = fm.fields[field_id].borrow_mut();
        let field = field_ref.deref_mut();
        if self.first_apf_idx.is_none() {
            return;
        }
        if field.action_indices.min_apf_idx.is_none() {
            field.action_indices.min_apf_idx = self.first_apf_idx;
        }
        if field.action_indices.curr_apf_idx.is_none() {
            field.action_indices.curr_apf_idx =
                field.action_indices.min_apf_idx;
        }
        let min_apf_idx = field.action_indices.min_apf_idx.unwrap();
        let curr_apf_idx = field.action_indices.curr_apf_idx.as_mut().unwrap();
        let prev_curr_apf_idx = *curr_apf_idx;
        let prev_first_unapplied_al_idx =
            field.action_indices.first_unapplied_al_idx;
        if min_apf_idx >= self.action_producing_fields.len() {
            return;
        }
        let als = self.prepare_action_lists(
            min_apf_idx,
            curr_apf_idx,
            &mut field.action_indices.first_unapplied_al_idx,
        );
        if als.actions_start == als.actions_end {
            // a common way for this assertion can fail is when an action
            // list is started, and then a field is selected
            // this will cause the empty action list to show up in here
            // TODO: maybe flag pending action lists?
            debug_assert!(
                prev_curr_apf_idx == *curr_apf_idx
                    && prev_first_unapplied_al_idx
                        == field.action_indices.first_unapplied_al_idx
            );
            if cfg!(feature = "debug_logging") {
                println!(
                    "executing commands for field {} had no effect: min apf: {}, curr apf: {} [al {}]",
                    field_id, min_apf_idx, curr_apf_idx, field.action_indices.first_unapplied_al_idx,
                );
            }
            return;
        }
        field.iter_hall.uncow_headers(fm);
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
                field.action_indices.first_unapplied_al_idx,
            );
            let refs =
                Self::get_merge_result_mal_ref(&self.merged_actions, &als);
            let actions = Self::get_merge_resuls_slice(
                &self.action_producing_fields,
                refs.as_deref(),
                &als,
            );
            for a in actions {
                println!("  > {:?}:", a);
            }
            if actions.is_empty() {
                println!("  > empty");
            }
            println!("--------------    </execution (field {field_id}) end>      --------------");
        }
        // TODO: avoid this allocation

        let fd = &mut field.iter_hall.field_data;
        let (headers, data, field_count) =
            match &mut field.iter_hall.data_source {
                FieldDataSource::Owned => {
                    (&mut fd.headers, Some(&mut fd.data), &mut fd.field_count)
                }
                FieldDataSource::DataCow(_)
                | FieldDataSource::RecordBufferDataCow(_) => {
                    (&mut fd.headers, None, &mut fd.field_count)
                }
                FieldDataSource::Cow(_)
                | FieldDataSource::RecordBufferCow(_) => {
                    panic!("cannot execute commands on COW iter hall")
                }
            };

        let mal = Self::get_merge_result_mal_ref(&self.merged_actions, &als);
        let actions = Self::get_merge_resuls_slice(
            &self.action_producing_fields,
            mal.as_deref(),
            &als,
        );
        let iterators =
            field.iter_hall.iters.iter_mut().map(|it| it.get_mut());
        self.actions_applicator.run(
            actions,
            headers,
            data,
            field_count,
            iterators,
            0,
            0,
        );
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
        field_id: FieldId, // for debug logging only
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
        if self.action_producing_fields.get(min_apf_idx).is_none() {
            return;
        }
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
        if cfg!(feature = "debug_logging")
            && (prev_first_unapplied_apf_idx != *first_unapplied_al_idx
                || prev_curr_apf_idx != *curr_apf_idx)
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
        let mal = Self::get_merge_result_mal_ref(&self.merged_actions, &als);
        let actions = Self::get_merge_resuls_slice(
            &self.action_producing_fields,
            mal.as_deref(),
            &als,
        );
        self.actions_applicator.run(
            actions,
            &mut field.headers,
            Some(&mut field.data),
            &mut field.field_count,
            std::iter::empty(),
            0,
            0,
        );
    }

    pub fn claim_apf(&mut self) -> ActionProducingFieldIndex {
        let apf_idx = self.action_producing_fields.len();
        let mal_count = (apf_idx + 1).trailing_zeros() as usize + 1;
        let mut apf = ActionProducingField {
            merged_action_lists: Vec::with_capacity(mal_count),
            pending_action_list: None,
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
        self.action_producing_fields.push(apf);
        if let Some(idx) = self.last_apf_idx {
            // TODO: handle higher orders?
            self.action_producing_fields[idx].merged_action_lists[0]
                .next_apf_idx = Some(apf_idx);
        }
        self.last_apf_idx = Some(apf_idx);
        if self.first_apf_idx.is_none() {
            self.first_apf_idx = Some(apf_idx);
        }
        apf_idx
    }
    pub fn peek_next_apf_id(&self) -> ActionProducingFieldIndex {
        self.action_producing_fields.len()
    }
}

// action list merging
impl CommandBuffer {
    fn get_merge_result_mal_ref<'a>(
        merged_actions: &'a [RefCell<Vec<FieldAction>>; 3],
        almr: &ActionListMergeResult,
    ) -> Option<Ref<'a, Vec<FieldAction>>> {
        if let ActionListMergeLocation::CbActionList { idx } = almr.location {
            return Some(merged_actions[idx].borrow());
        }
        None
    }
    fn get_merge_resuls_slice<'a>(
        apfs: &'a [ActionProducingField],
        mal_ref: Option<&'a Vec<FieldAction>>,
        almr: &ActionListMergeResult,
    ) -> &'a [FieldAction] {
        let range = almr.actions_start..almr.actions_end;
        match almr.location {
            ActionListMergeLocation::ApfMal { apf_idx, mal_idx } => {
                &apfs[apf_idx].merged_action_lists[mal_idx].actions[range]
            }
            ActionListMergeLocation::ApfLocal { apf_idx, mal_idx } => {
                &apfs[apf_idx].merged_action_lists[mal_idx]
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

        let first_ref =
            Self::get_merge_result_mal_ref(&self.merged_actions, &first);
        let second_ref =
            Self::get_merge_result_mal_ref(&self.merged_actions, &second);
        let first_slice = Self::get_merge_resuls_slice(
            &self.action_producing_fields,
            first_ref.as_deref(),
            &first,
        );
        let second_slice = Self::get_merge_resuls_slice(
            &self.action_producing_fields,
            second_ref.as_deref(),
            &second,
        );
        let res_size = first_slice.len() + second_slice.len();
        let mut res_ms = self.merged_actions[target_idx].borrow_mut();
        res_ms.reserve(res_size);
        let res_len_before = res_ms.len();
        merge_action_lists([first_slice, second_slice], &mut res_ms);
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
            merge_action_lists(lists, &mut mal.locally_merged_actions);

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
                merge_action_lists(lists, &mut temp);
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
        mut first_unapplied_al_idx_in_min_apf: Option<ActionListIndex>,
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
            match first_unapplied_al_idx_in_min_apf {
                Some(idx) if max_apf_idx == min_apf_idx => {
                    first_unapplied_al_idx_in_max_apf = idx;
                    first_unapplied_al_idx_in_min_apf = None;
                }
                _ => {
                    if !mal.action_lists.is_empty() {
                        first_unapplied_al_idx_in_max_apf = mal.action_lists
                            [first_unapplied_al_idx_in_max_apf - 1]
                            .first_unapplied_al_idx_in_prev_apf;
                    }
                }
            }
            return self.merge_action_lists(
                min_apf_idx,
                first_unapplied_al_idx_in_min_apf,
                max_apf_idx,
                first_unapplied_al_idx_in_max_apf,
            );
        }

        while mal.prev_apf_idx.unwrap() > min_apf_idx {
            if mal_idx + 1 == apf.merged_action_lists.len() {
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
        let mut second_apf_idx = None;
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
            second_apf_idx = Some(second_apf_idx.unwrap_or(next));
            let mal = &self.action_producing_fields[last_apf_idx]
                .merged_action_lists[0];
            if let Some(al) = mal.action_lists.last() {
                if al.ordering_id >= new_first_unapplied_al_ordering_id {
                    new_curr_apf_idx = last_apf_idx;
                    new_first_unapplied_al_idx = mal.action_lists.len();
                    new_first_unapplied_al_ordering_id = al.ordering_id + 1;
                }
            }
        }
        let res = if let Some(second_apf) = second_apf_idx {
            let lhs = self.merge_action_lists(
                min_apf_idx,
                None,
                *curr_apf_idx,
                *first_unapplied_al_idx_in_curr_apf,
            );
            let rhs =
                self.merge_action_lists(second_apf, None, last_apf_idx, 0);
            self.merge_two_action_lists(lhs, rhs)
        } else {
            self.merge_apf_action_list_mal_0(
                *curr_apf_idx,
                *first_unapplied_al_idx_in_curr_apf,
            )
        };
        *curr_apf_idx = new_curr_apf_idx;
        *first_unapplied_al_idx_in_curr_apf = new_first_unapplied_al_idx;
        res
    }
}
