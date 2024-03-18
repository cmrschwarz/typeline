use std::{
    cell::{Cell, Ref, RefCell, RefMut},
    cmp::Ordering,
    fmt::Display,
    ops::{Deref, DerefMut},
};

use crate::utils::{
    debuggable_nonmax::DebuggableNonMaxU32,
    size_classed_vec_deque::SizeClassedVecDeque, subslice_slice_pair,
    universe::Universe,
};

use super::{
    action_buffer::{ActionBuffer, ActorId, ActorRef, SnapshotRef},
    field_action::{FieldAction, FieldActionKind},
    field_data::FieldValueRepr,
};

pub type GroupIdx = usize;
pub type GroupLen = usize;

pub type GroupListIterId = u32;
pub type GroupListId = DebuggableNonMaxU32;

#[derive(Clone, Copy)]
pub struct GroupListIterRef {
    pub list_id: GroupListId,
    pub iter_id: GroupListIterId,
}

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
    pub iter_states: Universe<GroupListIterId, Cell<GroupsIterState>>,
    pub snapshot: SnapshotRef,
}

impl Display for GroupList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "{} + {:?}",
            self.passed_fields_count, self.group_lengths
        ))
    }
}

pub struct GroupTracker {
    lists: Universe<GroupListId, RefCell<GroupList>>,
    last_group: GroupListId,
    active_lists: Vec<GroupListId>,
}

pub struct GroupListIter<L> {
    list: L,
    field_pos: usize,
    group_idx_phys: GroupIdx,
    group_len_rem: GroupLen,
}
pub struct GroupListIterMut<'a, T: DerefMut<Target = GroupList>> {
    base: GroupListIter<T>,
    tracker: &'a GroupTracker,
    group_len: usize,
    update_group_len: bool,
    actions_applied_in_parents: bool,
    action_count_applied_to_parents: usize,
    action_buffer: &'a mut ActionBuffer,
}

impl Default for GroupTracker {
    fn default() -> Self {
        let mut lists = Universe::default();
        let dummy_gl = lists.claim();
        Self {
            lists,
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
            loop {
                let field_pos_delta = a.field_idx - field_pos;
                if field_pos_delta == 0 || field_pos_delta < group_len_rem {
                    break;
                }
                field_pos += group_len_rem;
                debug_assert!(group_idx != Some(self.group_lengths.len()));
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
                    modified = true;
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
    pub fn apply_field_actions(
        &mut self,
        ab: &mut ActionBuffer,
        #[cfg_attr(not(feature = "debug_logging"), allow(unused))]
        list_id: GroupListId,
    ) {
        let Some((actor_id, ss_prev)) =
            ab.update_snapshot(None, &mut self.actor, &mut self.snapshot)
        else {
            return;
        };
        let agi = ab.build_actions_from_snapshot(actor_id, ss_prev);

        if let Some(agi) = &agi {
            let (s1, s2) = ab.get_action_group_slices(agi);

            let actions = s1.iter().chain(s2.iter());

            #[cfg(feature = "debug_logging")]
            {
                eprintln!(
                    "applying actions to group list {:02} (actor {:02}):",
                    list_id, actor_id
                );
                crate::record_data::action_buffer::eprint_action_list(
                    actions.clone(),
                );
                eprintln!(
                    "   before: {} + {:?}",
                    self.passed_fields_count, self.group_lengths
                );
            }
            self.apply_field_actions_list(actions);
            #[cfg(feature = "debug_logging")]
            eprintln!(
                "   after:  {} + {:?}",
                self.passed_fields_count, self.group_lengths
            );
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
    pub fn lookup_iter(
        &self,
        iter_id: GroupListIterId,
    ) -> GroupListIter<&Self> {
        Self::lookup_iter_for_deref(self, iter_id)
    }
    fn build_iter_from_iter_state<T: Deref<Target = Self>>(
        list: T,
        iter_state: GroupsIterState,
    ) -> GroupListIter<T> {
        GroupListIter {
            field_pos: iter_state.field_pos,
            group_idx_phys: iter_state.group_idx,
            group_len_rem: list.group_lengths.get(iter_state.group_idx)
                - iter_state.group_offset,
            list,
        }
    }
    pub fn lookup_iter_for_deref<T: Deref<Target = Self>>(
        list: T,
        iter_id: GroupListIterId,
    ) -> GroupListIter<T> {
        let iter_state = list.iter_states[iter_id].get();
        Self::build_iter_from_iter_state(list, iter_state)
    }
    pub fn lookup_iter_for_deref_mut<'a, T: DerefMut<Target = Self>>(
        tracker: &'a GroupTracker,
        list: T,
        iter_id: GroupListIterId,
        action_buffer: &'a mut ActionBuffer,
        actor_id: ActorId,
    ) -> GroupListIterMut<'a, T> {
        let iter_state = list.iter_states[iter_id].get();
        let base = Self::build_iter_from_iter_state(list, iter_state);
        action_buffer.begin_action_group(actor_id);
        GroupListIterMut {
            group_len: base.group_len_rem + iter_state.group_offset,
            base,
            tracker,
            actions_applied_in_parents: false,
            action_count_applied_to_parents: 0,
            update_group_len: false,
            action_buffer,
        }
    }
    pub fn store_iter<T: Deref<Target = Self>>(
        &self,
        iter_id: GroupListIterId,
        iter: &GroupListIter<T>,
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
        self.iter_states[iter_id].set(iter_state);
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
        self.lists[group_list_id].borrow_mut().iter_states.claim()
    }
    pub fn claim_group_list_iter_for_active(&mut self) -> GroupListIterId {
        self.claim_group_list_iter(self.active_group_list())
    }
    pub fn claim_group_list_iter_ref_for_active(
        &mut self,
    ) -> GroupListIterRef {
        let list_id = self.active_group_list();
        GroupListIterRef {
            list_id,
            iter_id: self.claim_group_list_iter(list_id),
        }
    }
    pub fn lookup_group_list_iter(
        &self,
        list_id: GroupListId,
        iter_id: GroupListIterId,
        action_buffer: &mut ActionBuffer,
    ) -> GroupListIter<Ref<GroupList>> {
        self.lists[list_id]
            .borrow_mut()
            .apply_field_actions(action_buffer, list_id);
        GroupList::lookup_iter_for_deref(self.lists[list_id].borrow(), iter_id)
    }
    pub fn lookup_group_list_iter_mut<'a>(
        &'a self,
        list_id: GroupListId,
        iter_id: GroupListIterId,
        action_buffer: &'a mut ActionBuffer,
        actor_id: ActorId,
    ) -> GroupListIterMut<RefMut<'a, GroupList>> {
        let mut list = self.borrow_group_list_mut(list_id);
        list.apply_field_actions(action_buffer, list_id);
        GroupList::lookup_iter_for_deref_mut(
            self,
            list,
            iter_id,
            action_buffer,
            actor_id,
        )
    }
    pub fn store_group_list_iter<L: Deref<Target = GroupList>>(
        &self,
        list_id: GroupListId,
        iter_id: GroupListIterId,
        iter: &GroupListIter<L>,
    ) {
        self.lists[list_id].borrow().store_iter(iter_id, iter);
    }
    pub fn active_group_list(&self) -> GroupListId {
        self.active_lists.last().copied().unwrap()
    }
    pub fn apply_actions_to_list_and_parents(
        &self,
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
            .apply_field_actions(ab, group_list_id)
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

impl<L: Deref<Target = GroupList>> GroupListIter<L> {
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
    pub fn store_iter(&self, iter_id: GroupListIterId) {
        self.list.store_iter(iter_id, self);
    }
    pub fn next_n_fields(&mut self, n: usize) -> usize {
        let mut n_rem = n;
        if n <= self.group_len_rem {
            self.group_len_rem -= n;
            self.field_pos += n;
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
        let fields_advanced = n - n_rem;
        self.field_pos += fields_advanced;
        fields_advanced
    }
    pub fn next_group(&mut self) {
        self.group_idx_phys += 1;
        self.field_pos += self.group_len_rem;
        self.group_len_rem = self.list.group_lengths.get(self.group_idx_phys);
    }
    pub fn try_next_group(&mut self) -> bool {
        let Some(next_group_len) =
            self.list.group_lengths.try_get(self.group_idx_phys + 1)
        else {
            return false;
        };
        self.group_idx_phys += 1;
        self.group_len_rem = next_group_len;
        true
    }
    pub fn is_last_group(&self) -> bool {
        self.list.group_lengths.len() == self.group_idx_phys + 1
    }
    pub fn is_end_of_group(&self, end_of_input: bool) -> bool {
        if self.group_len_rem != 0 {
            return false;
        }
        if self.group_idx_phys + 1 == self.list.group_lengths.len() {
            return end_of_input;
        }
        true
    }
}

impl<'a, T: DerefMut<Target = GroupList>> GroupListIterMut<'a, T> {
    pub fn field_pos(&self) -> usize {
        self.base.field_pos()
    }
    pub fn group_idx_phys(&self) -> usize {
        self.base.group_idx_phys()
    }
    pub fn group_idx_logical(&self) -> GroupIdx {
        self.base.group_idx_logical()
    }
    pub fn group_len_rem(&self) -> usize {
        self.base.group_len_rem()
    }
    pub fn is_last_group(&self) -> bool {
        self.base.is_last_group()
    }
    pub fn is_end_of_group(&self, end_of_input: bool) -> bool {
        self.base.is_end_of_group(end_of_input)
    }
    pub fn group_len_before(&self) -> usize {
        self.group_len - self.base.group_len_rem
    }
    pub fn store_iter(&self, iter_id: GroupListIterId) {
        self.base.store_iter(iter_id);
    }
    pub fn write_back_group_len(&mut self) {
        self.base
            .list
            .group_lengths
            .set(self.base.group_idx_phys, self.group_len);
    }
    pub fn update_group(&mut self) {
        if self.update_group_len {
            self.write_back_group_len();
            self.update_group_len = false;
        }
    }
    pub fn next_n_fields_in_group(&mut self, n: usize) {
        self.base.group_len_rem -= n;
        self.base.field_pos += n;
    }
    pub fn next_n_fields(&mut self, n: usize) -> usize {
        let mut n_rem = n;
        if n <= self.base.group_len_rem {
            self.next_n_fields_in_group(n);
            return n;
        }
        self.update_group();
        let mut new_group_len = self.group_len;
        let mut group_idx_phys = self.base.group_idx_phys;
        while let Some(group_len) =
            self.base.list.group_lengths.try_get(group_idx_phys + 1)
        {
            new_group_len = group_len;
            if group_len >= n_rem {
                n_rem = 0;
                break;
            }
            n_rem -= group_len;
            group_idx_phys += 1;
        }
        self.base.group_idx_phys = group_idx_phys;
        self.group_len = new_group_len;
        self.base.group_len_rem = new_group_len - n_rem;
        let fields_advanced = n - n_rem;
        self.base.field_pos += fields_advanced;
        fields_advanced
    }
    fn next_group_raw(&mut self) {
        self.base.group_idx_phys += 1;
        self.base.field_pos += self.base.group_len_rem;
        let gl = self.base.list.group_lengths.get(self.base.group_idx_phys);
        self.group_len = gl;
        self.base.group_len_rem = gl;
    }
    pub fn next_group(&mut self) {
        self.update_group();
        self.next_group_raw()
    }
    fn try_next_group_raw(&mut self) -> bool {
        if !self.base.try_next_group() {
            return false;
        }
        self.group_len = self.base.group_len_rem;
        true
    }
    pub fn try_next_group(&mut self) -> bool {
        self.update_group();
        self.try_next_group_raw()
    }
    pub fn insert_fields(&mut self, repr: FieldValueRepr, count: usize) {
        if self.group_len == 0 {
            if !self.actions_applied_in_parents {
                self.actions_applied_in_parents = true;
                if let Some(parent) = self.base.list.parent_list {
                    self.tracker.apply_actions_to_list_and_parents(
                        self.action_buffer,
                        parent,
                    )
                }
            }
            let action_count =
                self.action_buffer.get_curr_action_group_action_count();
            if action_count != self.action_count_applied_to_parents {
                let (s1, s2) = subslice_slice_pair(
                    self.action_buffer.get_curr_action_group_slices(),
                    self.action_count_applied_to_parents..action_count,
                );
                let actions = s1.iter().chain(s2.iter());
                let mut parent_opt = self.base.list.parent_list;
                while let Some(parent) = parent_opt {
                    let mut list = self.tracker.lists[parent].borrow_mut();
                    list.apply_field_actions_list(actions.clone());
                    parent_opt = list.parent_list;
                }
                self.action_count_applied_to_parents = action_count + 1;
            }
        }
        self.action_buffer.push_action(
            FieldActionKind::InsertZst(repr),
            self.base.field_pos,
            count,
        );
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
    pub fn field_pos_is_in_group(&self, field_pos: usize) -> bool {
        if field_pos > self.base.field_pos {
            field_pos - self.base.field_pos >= self.base.group_len_rem
        } else {
            self.base.field_pos - field_pos
                >= (self.group_len - self.base.group_len_rem)
        }
    }

    pub fn dup(&mut self, count: usize) {
        if count == 0 {
            return;
        }
        if self.group_len == 0 {
            self.update_group();
            loop {
                self.next_group_raw();
                if self.group_len != 0 {
                    break;
                }
            }
        }
        self.base.group_len_rem += count;
        self.group_len += count;
        self.update_group_len = true;
        self.action_buffer.push_action(
            FieldActionKind::Dup,
            self.base.field_pos,
            count,
        );
    }
    pub fn dup_before(&mut self, field_pos: usize, count: usize) {
        if count == 0 {
            return;
        }
        debug_assert!(
            self.group_len_before() > self.base.field_pos - field_pos
        );
        self.group_len += count;
        self.update_group_len = true;
        self.action_buffer
            .push_action(FieldActionKind::Dup, field_pos, count);
    }
    pub fn dup_after(&mut self, field_pos: usize, count: usize) {
        if count == 0 {
            return;
        }
        debug_assert!(
            field_pos - self.base.field_pos > self.base.group_len_rem
        );
        self.group_len += count;
        self.base.group_len_rem += count;
        self.update_group_len = true;
        self.action_buffer
            .push_action(FieldActionKind::Dup, field_pos, count);
    }
    pub fn dup_with_field_pos(&mut self, field_pos: usize, count: usize) {
        match self.base.field_pos.cmp(&field_pos) {
            Ordering::Less => self.dup_before(field_pos, count),
            Ordering::Equal => self.dup_after(field_pos, count),
            Ordering::Greater => self.dup(count),
        }
    }
    fn drop_raw(&mut self, mut count: usize) {
        if count > self.base.group_len_rem {
            self.group_len -= self.base.group_len_rem;
            count -= self.base.group_len_rem;
            self.base.group_len_rem = 0;
            self.update_group_len = false;
            self.write_back_group_len();
            self.next_group_raw();
            while self.group_len < count {
                count -= self.group_len;
                self.group_len = 0;
                self.base.group_len_rem = 0;
                self.write_back_group_len();
                self.next_group_raw();
            }
        }
        self.base.group_len_rem -= count;
        self.group_len -= count;
        self.update_group_len = true;
    }
    pub fn drop(&mut self, count: usize) {
        if count == 0 {
            return;
        }
        self.drop_raw(count);
        self.action_buffer.push_action(
            FieldActionKind::Drop,
            self.base.field_pos,
            count,
        );
    }
    pub fn drop_backwards(&mut self, count: usize) {
        if count == 0 {
            return;
        }
        debug_assert!(self.group_len - self.base.group_len_rem >= count);
        self.action_buffer.push_action(
            FieldActionKind::Drop,
            self.base.field_pos - count,
            count,
        );
        self.group_len -= count;
        self.update_group_len = true;
        self.base.field_pos -= count;
    }
    pub fn drop_before(&mut self, field_pos: usize, count: usize) {
        if count == 0 {
            return;
        }
        let pos_delta = self.base.field_pos - field_pos;
        debug_assert!(pos_delta <= self.group_len_before());
        self.action_buffer.push_action(
            FieldActionKind::Drop,
            field_pos,
            count,
        );
        if pos_delta >= count {
            self.group_len -= count;
            self.update_group_len = true;
            self.base.field_pos -= count;
            return;
        }
        self.group_len -= pos_delta;
        self.base.field_pos -= pos_delta;
        self.drop_raw(count - pos_delta);
    }
    pub fn drop_after(&mut self, field_pos: usize, count: usize) {
        if count == 0 {
            return;
        }
        let pos_delta = field_pos - self.base.field_pos;
        debug_assert!(self.base.group_len_rem >= pos_delta + count);
        self.action_buffer.push_action(
            FieldActionKind::Drop,
            field_pos,
            count,
        );
        if count <= self.base.group_len_rem - pos_delta {
            self.group_len -= count;
            self.base.group_len_rem -= count;
            self.update_group_len = true;
            return;
        }
        let group_pos = self.group_len - self.base.group_len_rem;
        let group_len = field_pos;
        let group_idx_phys = self.base.group_idx_phys;
        self.base.group_len_rem -= pos_delta;
        self.drop_raw(count);
        self.write_back_group_len();
        self.base.group_idx_phys = group_idx_phys;
        self.group_len = group_len;
        self.base.group_len_rem = group_len - group_pos;
    }

    pub fn drop_with_field_pos(&mut self, field_pos: usize, count: usize) {
        match self.base.field_pos.cmp(&field_pos) {
            Ordering::Less => self.drop_before(field_pos, count),
            Ordering::Equal => self.drop_after(field_pos, count),
            Ordering::Greater => self.drop(count),
        }
    }

    /// Advances the iterator until it points at the start of a a non empty
    /// group. Returns the number of groups skipped.
    /// The initial group does *not* have to be empty, but the iterator
    /// must have passed all it's elements. Otherwise 0 is returned.
    pub fn skip_empty_groups(&mut self) -> usize {
        if self.base.group_len_rem != 0 || !self.try_next_group() {
            return 0;
        };
        let mut count = 1;
        while self.group_len == 0 && self.try_next_group() {
            count += 1;
        }
        count
    }
}

impl<'a, T: DerefMut<Target = GroupList>> Drop for GroupListIterMut<'a, T> {
    fn drop(&mut self) {
        self.action_buffer.end_action_group();
        let list = &mut *self.base.list;
        if let Some((_actor_id, ss_prev)) = self.action_buffer.update_snapshot(
            None,
            &mut list.actor,
            &mut list.snapshot,
        ) {
            self.action_buffer.drop_snapshot_refcount(ss_prev, 1);
        }
        if self.action_count_applied_to_parents != 0 {
            let mut parent_id = list.parent_list;
            while let Some(list_id) = parent_id {
                let mut list_ref = self.tracker.lists[list_id].borrow_mut();
                let list = &mut *list_ref;
                if let Some((_actor_id, ss_prev)) = self
                    .action_buffer
                    .update_snapshot(None, &mut list.actor, &mut list.snapshot)
                {
                    self.action_buffer.drop_snapshot_refcount(ss_prev, 1);
                }
                parent_id = list.parent_list;
            }
        }
    }
}
