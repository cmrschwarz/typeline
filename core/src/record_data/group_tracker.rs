use std::cell::{Ref, RefCell};

use crate::utils::universe::Universe;

#[derive(Default, Clone, Copy)]
struct GroupLength {
    length: usize,
    run_len: usize,
}

struct GroupsIterState {
    list_id: GroupsListId,
    index: usize,
    run_length_offset: usize,
    group_offset: usize,
    field_pos: usize,
}

pub struct GroupsIter<'a> {
    list: Ref<'a, GroupList>,
    index: usize,
    field_pos: usize,
    run_length_rem: usize,
    group_len_rem: usize,
}

pub type GroupsIterId = u32;
pub type GroupsListId = u32;

struct GroupList {
    lengths: Vec<GroupLength>,
}

pub struct GroupTracker {
    lists: Universe<GroupsListId, RefCell<GroupList>>,
    iters: Universe<GroupsIterId, GroupsIterState>,
    active_lists: Vec<GroupsListId>,
}

impl GroupTracker {
    pub fn claim_groups_iter(&mut self) -> GroupsIterId {
        self.iters.claim_with_value(GroupsIterState {
            list_id: *self.active_lists.last().unwrap(),
            index: 0,
            run_length_offset: 0,
            group_offset: 0,
            field_pos: 0,
        })
    }
    pub fn get_groups_iter(&self, iter_id: GroupsIterId) -> GroupsIter {
        let iter_state = &self.iters[iter_id];
        let list = self.lists[iter_state.list_id].borrow();
        let curr = list
            .lengths
            .get(iter_state.index)
            .copied()
            .unwrap_or_default();
        GroupsIter {
            index: iter_state.index,
            field_pos: iter_state.field_pos,
            run_length_rem: curr.run_len - iter_state.run_length_offset,
            group_len_rem: curr.run_len - iter_state.group_offset,
            list,
        }
    }
}

impl<'a> GroupsIter<'a> {
    fn group_len_rem(&self) -> usize {
        self.group_len_rem
    }
    fn next_n_fields(&mut self, mut n: usize) {
        if n <= self.group_len_rem {
            self.group_len_rem -= n;
            return;
        }
        n -= self.group_len_rem;
        todo!();
    }
}
