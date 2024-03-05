use std::{
    cell::{Ref, RefCell, RefMut},
    ops::{Deref, DerefMut},
};

use crate::utils::{size_classed_vec::SizeClassedVec, universe::Universe};

use super::{
    action_buffer::{ActionBuffer, ActorRef, SnapshotRef},
    field_action::{FieldAction, FieldActionKind},
};

pub type GroupIdx = usize;
pub type GroupLen = usize;

pub type GroupsIterId = u32;
pub type GroupsListId = u32;

type PerGroupListIterIdx = u32;

#[derive(Clone, Copy)]
struct GroupsIterState {
    field_pos: usize,
    group_idx: GroupIdx,
    group_offset: GroupLen,
}

pub struct GroupList {
    actor: ActorRef,
    parent_list: Option<GroupsListId>,
    group_index_offset: GroupIdx,
    group_lengths: SizeClassedVec,
    // Index of the 'parent group'. This is necessary to make sense of zero
    // length groups, where we would otherwise lose this connection, since
    // we can't find the right partner by lockstep iterating over both
    // group lists anymore. Used during `insert_fields` to update parents.
    parent_group_indices: SizeClassedVec,
    iter_states: Universe<PerGroupListIterIdx, GroupsIterState>,
    snapshot: SnapshotRef,
}

impl GroupList {
    pub fn adjust_group_index_from_child(&self, group_index: usize) -> usize {
        group_index.wrapping_sub(self.group_index_offset)
    }
    fn apply_field_actions_list<'a>(
        &mut self,
        action_list: impl Iterator<Item = &'a FieldAction>,
    ) {
        let mut iter = GroupsIterBase {
            field_pos: 0,
            group_index: 0,
            group_len_rem: self.group_lengths.try_get(0).unwrap_or(0),
            list: self,
        };
        for a in action_list {
            iter.next_n_fields(a.field_idx - iter.field_pos);
            match a.kind {
                FieldActionKind::Dup => todo!(),
                FieldActionKind::Drop => todo!(),
                FieldActionKind::InsertZst(_) => todo!(),
            }
        }
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
    pub fn iter(&self) -> GroupsIterBase<&GroupList> {
        GroupsIterBase {
            list: self,
            field_pos: 0,
            group_index: 0,
            group_len_rem: self.group_lengths.try_get(0).unwrap_or(0),
        }
    }
}

#[derive(Default)]
pub struct GroupTracker {
    lists: Universe<GroupsListId, RefCell<GroupList>>,
    iters: Universe<GroupsIterId, (GroupsListId, PerGroupListIterIdx)>,
    active_lists: Vec<GroupsListId>,
}

impl GroupTracker {
    pub fn add_group_list(&mut self, actor: ActorRef) -> GroupsListId {
        let list_id = self.lists.claim_with_value(RefCell::new(GroupList {
            actor,
            parent_list: self.active_lists.last().copied(),
            group_index_offset: 0,
            group_lengths: SizeClassedVec::default(),
            parent_group_indices: SizeClassedVec::default(),
            iter_states: Universe::default(),
            snapshot: SnapshotRef::default(),
        }));
        self.active_lists.push(list_id);
        list_id
    }
    pub fn pop_active_group_list(&mut self) -> Option<GroupsListId> {
        self.active_lists.pop()
    }
    pub fn claim_groups_iter(&mut self) -> GroupsIterId {
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
    pub fn get_groups_iter(&self, iter_id: GroupsIterId) -> GroupsIter {
        let (list_id, iter_id) = self.iters[iter_id];
        let list = self.lists[list_id].borrow();
        let iter_state = list.iter_states[iter_id];
        GroupsIter {
            field_pos: iter_state.field_pos,
            group_index: iter_state.group_idx,
            group_len_rem: list.group_lengths.get(iter_state.group_idx)
                - iter_state.group_offset,
            list,
        }
    }
    pub fn active_group_list(&self) -> GroupsListId {
        *self.active_lists.last().unwrap()
    }
    pub fn apply_field_actions_to_all_active(
        &mut self,
        ab: &mut ActionBuffer,
    ) {
        let mut prev_diff = (SnapshotRef::default(), SnapshotRef::default());
        let mut agi = None;
        for &list_id in &self.active_lists {
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
        }
        ab.release_temp_action_group(agi);
    }
    pub fn apply_field_actions_to_current(&mut self, ab: &mut ActionBuffer) {
        self.lists[self.active_group_list()]
            .borrow_mut()
            .apply_field_actions(ab)
    }
}

pub struct GroupsIterBase<L> {
    list: L,
    field_pos: usize,
    group_index: GroupIdx,
    group_len_rem: GroupLen,
}

impl<L: Deref<Target = GroupList>> GroupsIterBase<L> {
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

pub type GroupsIter<'a> = GroupsIterBase<Ref<'a, GroupList>>;

pub struct GroupsIterMut<'a> {
    base: GroupsIterBase<RefMut<'a, GroupList>>,
    tracker: &'a GroupTracker,
}

impl<'a> Deref for GroupsIterMut<'a> {
    type Target = GroupsIterBase<RefMut<'a, GroupList>>;

    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<'a> DerefMut for GroupsIterMut<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}

impl<'a> GroupsIterMut<'a> {
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
