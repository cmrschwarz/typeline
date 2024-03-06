use std::{
    cell::{Cell, Ref, RefCell, RefMut},
    ops::{Deref, DerefMut},
};

use crate::utils::{
    debuggable_nonmax::DebuggableNonMaxUsize,
    size_classed_vec_deque::SizeClassedVecDeque, universe::Universe,
};

use super::{
    action_buffer::{ActionBuffer, ActorRef, SnapshotRef},
    field_action::{FieldAction, FieldActionKind},
};

pub type GroupIdx = usize;
pub type GroupLen = usize;

pub type GroupListIterId = u32;
pub type GroupListId = DebuggableNonMaxUsize;

pub type PerGroupListIterIdx = u32;

#[derive(Default, Clone, Copy)]
pub struct GroupsIterState {
    field_pos: usize,
    group_idx: GroupIdx,
    group_offset: GroupLen,
}

#[derive(Default)]
pub struct GroupList {
    pub actor: ActorRef,
    pub parent_list: Option<GroupListId>,
    pub prev_list: Option<GroupListId>,
    pub group_index_offset: GroupIdx,
    pub group_lengths: SizeClassedVecDeque,
    // fields that passed the `end` of the grouping operator but weren't
    // dropped yet.
    pub passed_fields_count: usize,
    // Index of the 'parent group'. This is necessary to make sense of zero
    // length groups, where we would otherwise lose this connection, since
    // we can't find the right partner by lockstep iterating over both
    // group lists anymore. Used during `insert_fields` to update parents.
    pub parent_group_indices: SizeClassedVecDeque,
    pub iter_states: Universe<PerGroupListIterIdx, Cell<GroupsIterState>>,
    pub snapshot: SnapshotRef,
}

pub struct GroupTracker {
    lists: Universe<GroupListId, RefCell<GroupList>>,
    iters: Universe<GroupListIterId, (GroupListId, PerGroupListIterIdx)>,
    last_group: GroupListId,
    active_lists: Vec<GroupListId>,
}

impl Default for GroupTracker {
    fn default() -> Self {
        let mut lists = Universe::default();
        let dummy_gl = lists.claim();
        Self {
            lists,
            iters: Universe::default(),
            last_group: dummy_gl,
            active_lists: vec![dummy_gl],
        }
    }
}

impl GroupList {
    pub fn group_idx_to_physical_idx(&self, group_index: GroupIdx) -> usize {
        group_index.wrapping_sub(self.group_index_offset)
    }
    pub fn physical_idx_to_group_idx(&self, physical_idx: usize) -> GroupIdx {
        physical_idx.wrapping_add(self.group_index_offset)
    }
    pub fn next_group_idx(&self) -> GroupIdx {
        self.physical_idx_to_group_idx(self.group_lengths.len())
    }
    fn apply_field_actions_list<'a>(
        &mut self,
        action_list: impl Iterator<Item = &'a FieldAction>,
    ) {
        fn next_group(
            gl: &mut GroupList,
            group_idx: &mut Option<usize>,
            modified: &mut bool,
            group_len: &mut usize,
            group_len_rem: &mut usize,
        ) -> usize {
            let group_count = gl.group_lengths.len();
            let gi = match *group_idx {
                Some(gi) if gi == group_count => gi,
                Some(gi) => {
                    if *modified {
                        gl.group_lengths.set(gi, *group_len);
                        *modified = false;
                    }
                    gi + 1
                }
                None => {
                    if *modified {
                        gl.passed_fields_count = *group_len;
                        *modified = false;
                    }
                    0
                }
            };
            *group_idx = Some(gi);
            let len = gl.group_lengths.try_get(gi).unwrap_or(0);
            *group_len = len;
            *group_len_rem = len;
            gi
        }
        let mut field_pos = 0;
        let mut group_idx =
            if self.passed_fields_count > 0 || self.group_lengths.is_empty() {
                None
            } else {
                Some(0)
            };
        let mut modified = false;
        let Some(mut group_len) = self.group_lengths.first() else {
            return;
        };
        let mut group_len_rem = group_len;
        for a in action_list {
            while (a.field_idx - field_pos) > group_len_rem {
                field_pos += group_len_rem;
                next_group(
                    self,
                    &mut group_idx,
                    &mut modified,
                    &mut group_len,
                    &mut group_len_rem,
                );
            }
            let mut action_run_len = a.run_len as usize;
            match a.kind {
                FieldActionKind::Dup | FieldActionKind::InsertZst(_) => {
                    group_len_rem += action_run_len;
                    group_len += action_run_len;
                    modified = true;
                }
                FieldActionKind::Drop => {
                    while action_run_len > group_len_rem {
                        group_len -= group_len_rem;
                        action_run_len -= group_len_rem;
                        modified = true;
                        next_group(
                            self,
                            &mut group_idx,
                            &mut modified,
                            &mut group_len,
                            &mut group_len_rem,
                        );
                    }
                    group_len -= action_run_len;
                    group_len_rem -= action_run_len;
                }
            }
        }
        next_group(
            self,
            &mut group_idx,
            &mut modified,
            &mut group_len,
            &mut group_len_rem,
        );
    }
    pub fn apply_field_actions(&mut self, ab: &mut ActionBuffer) {
        let Some((actor_id, ss_prev)) =
            ab.update_snapshot(None, &mut self.actor, &mut self.snapshot)
        else {
            return;
        };
        let agi = ab.build_actions_from_snapshot(actor_id, ss_prev);
        if let Some(agi) = &agi {
            let (s1, s2) = ab.get_action_group_slices(agi);
            self.apply_field_actions_list(s1.iter().chain(s2.iter()));
        };
        ab.drop_snapshot_refcount(ss_prev, 1);
        ab.release_temp_action_group(agi);
    }
    pub fn drop_leading_fields(&mut self, count: usize, end_of_input: bool) {
        let (groups_done, field_count) = self
            .group_lengths
            .iter()
            .try_fold((0, 0), |(i, sum), v| {
                let sum_new = sum + v;
                if sum_new > count || (sum_new == count && end_of_input) {
                    return Err((i, sum));
                }
                Ok((i + 1, sum_new))
            })
            .unwrap_or_else(|e| e);

        let remainder = count - field_count;
        if remainder > 0 {
            self.group_lengths.sub_value(groups_done, remainder);
        }
        self.group_lengths.drain(0..groups_done);
        if self.parent_list.is_some() {
            self.parent_group_indices.drain(0..groups_done);
        }
        self.group_index_offset =
            self.group_index_offset.wrapping_add(groups_done);
        self.passed_fields_count += count;
    }
    pub fn lookup_group_list_iter<T: Deref<Target = Self>>(
        list: T,
        iter_id: PerGroupListIterIdx,
    ) -> GroupListIterBase<T> {
        let iter_state = list.iter_states[iter_id].get();
        GroupListIterBase {
            field_pos: iter_state.field_pos,
            group_idx_phys: iter_state.group_idx,
            group_len_rem: list.group_lengths.get(iter_state.group_idx)
                - iter_state.group_offset,
            list,
        }
    }
    pub fn store_group_list_iter<T: Deref<Target = Self>>(
        &self,
        iter_idx: PerGroupListIterIdx,
        iter: &GroupListIterBase<T>,
    ) {
        let iter_state = GroupsIterState {
            field_pos: iter.field_pos,
            group_idx: iter.group_idx_phys,
            group_offset: iter
                .list
                .group_lengths
                .try_get(iter.group_idx_phys)
                .unwrap_or(0)
                - iter.group_len_rem,
        };
        self.iter_states[iter_idx].set(iter_state);
    }
}

impl GroupTracker {
    pub fn add_group_list(&mut self, actor: ActorRef) -> GroupListId {
        let list_id = self.lists.claim_with_value(RefCell::new(GroupList {
            actor,
            parent_list: self.active_lists.last().copied(),
            prev_list: Some(self.last_group),
            group_index_offset: 0,
            passed_fields_count: 0,
            group_lengths: SizeClassedVecDeque::default(),
            parent_group_indices: SizeClassedVecDeque::default(),
            iter_states: Universe::default(),
            snapshot: SnapshotRef::default(),
        }));
        self.last_group = list_id;
        list_id
    }
    pub fn push_active_group_list(&mut self, group_list_id: GroupListId) {
        self.active_lists.push(group_list_id);
    }
    pub fn pop_active_group_list(&mut self) -> Option<GroupListId> {
        self.active_lists.pop()
    }
    pub fn claim_group_list_iter(
        &mut self,
        group_list_id: GroupListId,
    ) -> GroupListIterId {
        let per_group_iter_id =
            self.lists[group_list_id].borrow_mut().iter_states.claim();
        self.iters
            .claim_with_value((group_list_id, per_group_iter_id))
    }
    pub fn claim_group_list_iter_for_active(&mut self) -> GroupListIterId {
        self.claim_group_list_iter(self.active_group_list())
    }
    pub fn get_group_list_of_iter(
        &self,
        iter_id: GroupListIterId,
    ) -> GroupListId {
        self.iters[iter_id].0
    }
    pub fn lookup_group_list_iter(
        &self,
        iter_id: GroupListIterId,
    ) -> GroupListIter {
        let (list_id, iter_id) = self.iters[iter_id];
        let list = self.lists[list_id].borrow();
        GroupList::lookup_group_list_iter(list, iter_id)
    }
    pub fn lookup_group_list_iter_applying_actions(
        &self,
        ab: &mut ActionBuffer,
        iter_id: GroupListIterId,
    ) -> GroupListIter {
        let (list_id, iter_idx) = self.iters[iter_id];
        let list_ref = &self.lists[list_id];
        let mut list = list_ref.borrow_mut();
        list.apply_field_actions(ab);
        drop(list);
        GroupList::lookup_group_list_iter(list_ref.borrow(), iter_idx)
    }
    pub fn store_group_list_iter<L: Deref<Target = GroupList>>(
        &self,
        iter_id: GroupListIterId,
        iter: &GroupListIterBase<L>,
    ) {
        let (list_id, iter_idx) = self.iters[iter_id];
        self.lists[list_id]
            .borrow()
            .store_group_list_iter(iter_idx, iter);
    }
    pub fn active_group_list(&self) -> GroupListId {
        self.active_lists.last().copied().unwrap()
    }
    pub fn apply_actions_to_list_and_parents(
        &mut self,
        ab: &mut ActionBuffer,
        group_list_id: GroupListId,
    ) {
        let mut prev_diff = (SnapshotRef::default(), SnapshotRef::default());
        let mut agi = None;
        let mut list_id = group_list_id;
        loop {
            let mut list_ref = self.lists[list_id].borrow_mut();
            let list = &mut *list_ref;
            let Some((actor_id, ss_prev)) =
                ab.update_snapshot(None, &mut list.actor, &mut list.snapshot)
            else {
                return;
            };
            let diff = (ss_prev, list.snapshot);
            if prev_diff != diff {
                prev_diff = diff;
                ab.release_temp_action_group(agi);
                agi = ab.build_actions_from_snapshot(actor_id, ss_prev);
            }
            if let Some(agi) = &agi {
                let (s1, s2) = ab.get_action_group_slices(agi);
                list.apply_field_actions_list(s1.iter().chain(s2.iter()));
            };
            ab.drop_snapshot_refcount(ss_prev, 1);
            if let Some(prev) = list.prev_list {
                list_id = prev;
            } else {
                break;
            }
        }
        ab.release_temp_action_group(agi);
    }
    pub fn apply_actions_to_list(
        &mut self,
        ab: &mut ActionBuffer,
        group_list_id: GroupListId,
    ) {
        self.lists[group_list_id]
            .borrow_mut()
            .apply_field_actions(ab)
    }
    pub fn append_group_to_all_active_lists(&mut self, field_count: usize) {
        let mut parent_idx = None;
        let mut prev = None;
        for &gl_idx in &self.active_lists {
            let mut gl = self.lists[gl_idx].borrow_mut();
            gl.group_lengths.push_back(field_count);
            debug_assert!(gl.parent_list.is_some() == parent_idx.is_some());
            debug_assert!(gl.parent_list == prev);
            if let Some(parent_idx) = parent_idx {
                gl.parent_group_indices.push_back(parent_idx);
            }
            parent_idx = Some(gl.next_group_idx());
            prev = Some(gl_idx);
        }
    }
    pub fn borrow_group_list(
        &self,
        group_list_id: GroupListId,
    ) -> Ref<GroupList> {
        self.lists[group_list_id].borrow()
    }
    pub fn borrow_group_list_mut(
        &self,
        group_list_id: GroupListId,
    ) -> RefMut<GroupList> {
        self.lists[group_list_id].borrow_mut()
    }
}

pub struct GroupListIterBase<L> {
    list: L,
    field_pos: usize,
    group_idx_phys: GroupIdx,
    group_len_rem: GroupLen,
}

impl<L: Deref<Target = GroupList>> GroupListIterBase<L> {
    pub fn field_pos(&self) -> usize {
        self.field_pos
    }
    pub fn group_idx_phys(&self) -> usize {
        self.group_idx_phys
    }
    pub fn group_idx_logical(&self) -> GroupIdx {
        self.list.physical_idx_to_group_idx(self.group_idx_phys)
    }
    pub fn group_len_rem(&self) -> usize {
        self.group_len_rem
    }

    pub fn next_n_fields(&mut self, n: usize) -> usize {
        let mut n_rem = n;
        if n <= self.group_len_rem {
            self.group_len_rem -= n;
            return n;
        }
        while let Some(group_len) =
            self.list.group_lengths.try_get(self.group_idx_phys + 1)
        {
            self.group_idx_phys += 1;
            if group_len >= n_rem {
                self.group_len_rem = group_len - n_rem;
                return n;
            }
            n_rem -= group_len;
        }
        n - n_rem
    }
    pub fn next_group(&mut self) -> usize {
        let gl_rem = self.group_len_rem;
        self.group_idx_phys += 1;
        self.group_len_rem = self.list.group_lengths.get(self.group_idx_phys);
        gl_rem
    }
    pub fn try_next_group(&mut self) -> Option<usize> {
        let Some(next_group_len) =
            self.list.group_lengths.try_get(self.group_idx_phys + 1)
        else {
            return None;
        };
        self.group_idx_phys += 1;
        let prev_gl_len_rem = self.group_len_rem;
        self.group_len_rem = next_group_len;
        Some(prev_gl_len_rem)
    }
    pub fn is_last_group(&self) -> bool {
        self.list.group_lengths.len() == self.group_idx_phys + 1
    }
}

pub type GroupListIter<'a> = GroupListIterBase<Ref<'a, GroupList>>;

pub struct GroupListIterMut<'a> {
    base: GroupListIterBase<RefMut<'a, GroupList>>,
    tracker: &'a GroupTracker,
}

impl<'a> Deref for GroupListIterMut<'a> {
    type Target = GroupListIterBase<RefMut<'a, GroupList>>;

    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<'a> DerefMut for GroupListIterMut<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}

impl<'a> GroupListIterMut<'a> {
    pub fn insert_fields(&mut self, count: usize) {
        let mut group_index = self.base.group_idx_phys;
        self.base.list.group_lengths.add_value(group_index, count);
        let Some(mut parent_list_idx) = self.base.list.parent_list else {
            return;
        };
        group_index = self.base.list.parent_group_indices.get(group_index);
        loop {
            let mut list = self.tracker.lists[parent_list_idx].borrow_mut();
            let group_index_phys = list.group_idx_to_physical_idx(group_index);
            list.group_lengths.add_value(group_index_phys, count);
            let Some(idx) = list.parent_list else {
                break;
            };
            parent_list_idx = idx;
            group_index = list.parent_group_indices.get(group_index_phys);
        }
    }
}
