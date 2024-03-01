use std::{
    cell::{Ref, RefCell, RefMut},
    ops::{Deref, DerefMut},
};

use crate::utils::{size_classed_vec::SizeClassedVec, universe::Universe};

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
    parent_list: Option<GroupsListId>,
    group_index_offset: GroupIdx,
    group_lengths: SizeClassedVec,
    parent_group_indices: SizeClassedVec,
    iter_states: Universe<PerGroupListIterIdx, GroupsIterState>,
}

impl GroupList {
    pub fn adjust_group_index_from_child(&self, group_index: usize) -> usize {
        group_index.wrapping_sub(self.group_index_offset)
    }
}

#[derive(Default)]
pub struct GroupTracker {
    lists: Universe<GroupsListId, RefCell<GroupList>>,
    iters: Universe<GroupsIterId, (GroupsListId, PerGroupListIterIdx)>,
    active_lists: Vec<GroupsListId>,
}

impl GroupTracker {
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
