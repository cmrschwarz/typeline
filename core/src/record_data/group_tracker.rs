use std::{
    cell::{Ref, RefCell, RefMut},
    ops::{Deref, DerefMut},
};

use crate::utils::{
    debuggable_nonmax::DebuggableNonMaxUsize,
    size_classed_vec::SizeClassedVec, universe::Universe,
};

use super::{
    action_buffer::{ActionBuffer, ActorRef, SnapshotRef},
    field_action::{FieldAction, FieldActionKind},
};

pub type GroupIdx = usize;
pub type GroupLen = usize;

pub type GroupListIterId = u32;
pub type GroupListId = DebuggableNonMaxUsize;

type PerGroupListIterIdx = u32;

#[derive(Clone, Copy)]
struct GroupsIterState {
    field_pos: usize,
    group_idx: GroupIdx,
    group_offset: GroupLen,
}

pub struct GroupList {
    actor: ActorRef,
    parent_list: Option<GroupListId>,
    prev_list: Option<GroupListId>,
    group_index_offset: GroupIdx,
    group_lengths: SizeClassedVec,
    // fields that passed the `end` of the grouping operator but weren't
    // dropped yet.
    passed_fields_count: usize,
    // Index of the 'parent group'. This is necessary to make sense of zero
    // length groups, where we would otherwise lose this connection, since
    // we can't find the right partner by lockstep iterating over both
    // group lists anymore. Used during `insert_fields` to update parents.
    parent_group_indices: SizeClassedVec,
    iter_states: Universe<PerGroupListIterIdx, GroupsIterState>,
    snapshot: SnapshotRef,
}

#[derive(Default)]
pub struct GroupTracker {
    lists: Universe<GroupListId, RefCell<GroupList>>,
    iters: Universe<GroupListIterId, (GroupListId, PerGroupListIterIdx)>,
    last_group: Option<GroupListId>,
    active_lists: Vec<GroupListId>,
}

impl GroupList {
    pub fn adjust_group_index_from_child(&self, group_index: usize) -> usize {
        group_index.wrapping_sub(self.group_index_offset)
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
        let mut group_idx = None;
        let mut group_len = 0;
        let mut modified = false;
        let Some(mut group_len_rem) = self.group_lengths.first() else {
            return;
        };
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
    fn apply_field_actions(&mut self, ab: &mut ActionBuffer) {
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
}

impl GroupTracker {
    pub fn add_group_list(&mut self, actor: ActorRef) -> GroupListId {
        let list_id = self.lists.claim_with_value(RefCell::new(GroupList {
            actor,
            parent_list: self.active_lists.last().copied(),
            prev_list: self.last_group,
            group_index_offset: 0,
            passed_fields_count: 0,
            group_lengths: SizeClassedVec::default(),
            parent_group_indices: SizeClassedVec::default(),
            iter_states: Universe::default(),
            snapshot: SnapshotRef::default(),
        }));
        self.last_group = Some(list_id);
        self.active_lists.push(list_id);
        list_id
    }
    pub fn pop_active_group_list(&mut self) -> Option<GroupListId> {
        self.active_lists.pop()
    }
    pub fn claim_group_list_iter(&mut self) -> GroupListIterId {
        let list_id = *self.active_lists.last().unwrap();
        let per_group_iter_id = self.lists[list_id]
            .borrow_mut()
            .iter_states
            .claim_with_value(GroupsIterState {
                field_pos: 0,
                group_idx: 0,
                group_offset: 0,
            });
        self.iters.claim_with_value((list_id, per_group_iter_id))
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
        let iter_state = list.iter_states[iter_id];
        GroupListIter {
            field_pos: iter_state.field_pos,
            group_index: iter_state.group_idx,
            group_len_rem: list.group_lengths.get(iter_state.group_idx)
                - iter_state.group_offset,
            list,
        }
    }
    pub fn active_group_list(&self) -> GroupListId {
        *self.active_lists.last().unwrap()
    }
    pub fn apply_field_actions_to_all(
        &mut self,
        group_list_id: GroupListId,
        ab: &mut ActionBuffer,
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
    pub fn apply_field_actions(&mut self, ab: &mut ActionBuffer) {
        self.lists[self.active_group_list()]
            .borrow_mut()
            .apply_field_actions(ab)
    }
}

pub struct GroupListIterBase<L> {
    list: L,
    field_pos: usize,
    group_index: GroupIdx,
    group_len_rem: GroupLen,
}

impl<L: Deref<Target = GroupList>> GroupListIterBase<L> {
    pub fn field_pos(&self) -> usize {
        self.field_pos
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
            self.list.group_lengths.try_get(self.group_index + 1)
        {
            self.group_index += 1;
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
        self.group_index += 1;
        self.group_len_rem = self.list.group_lengths.get(self.group_len_rem);
        gl_rem
    }
    pub fn is_last_group(&self) -> bool {
        self.list.group_lengths.len() == self.group_index + 1
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
        let mut group_index = self.base.group_index;
        self.base.list.group_lengths.add_value(group_index, count);
        let Some(mut parent_list_idx) = self.base.list.parent_list else {
            return;
        };
        group_index = self.base.list.parent_group_indices.get(group_index);
        loop {
            let mut list = self.tracker.lists[parent_list_idx].borrow_mut();
            group_index = list.adjust_group_index_from_child(group_index);
            list.group_lengths.add_value(group_index, count);
            let Some(idx) = list.parent_list else {
                break;
            };
            parent_list_idx = idx;
            group_index = list.parent_group_indices.get(group_index);
        }
    }
}
