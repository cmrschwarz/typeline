use std::{
    cell::{Cell, Ref, RefCell, RefMut},
    cmp::Ordering,
    fmt::Display,
    ops::{Deref, DerefMut, Range, RangeBounds},
};

use crate::{
    index_newtype,
    utils::{
        bit_vec_deque::BitVecDeque, debuggable_nonmax::DebuggableNonMaxU32,
        indexing_type::IndexingType, range_bounds_to_range,
        size_classed_vec_deque::SizeClassedVecDeque, universe::Universe,
    },
};

use super::{
    action_buffer::{
        ActionBuffer, ActorId, ActorRef, ActorSubscriber, SnapshotRef,
    },
    field_action::{FieldAction, FieldActionKind},
    field_data::FieldValueRepr,
    iter_hall::IterKind,
    match_set::{MatchSetId, MatchSetManager},
};

pub type GroupIdx = usize;
pub type GroupLen = usize;

index_newtype! {
    pub struct GroupIdxStable(usize);
}

pub type GroupTrackIterId = u32;
type GroupTrackIterSortedIndex = u32;
pub type GroupTrackId = DebuggableNonMaxU32;
pub const VOID_GROUP_TRACK_ID: GroupTrackId = GroupTrackId::MAX;

#[derive(Clone, Copy)]
pub struct GroupTrackIterRef {
    pub track_id: GroupTrackId,
    pub iter_id: GroupTrackIterId,
}

#[derive(Debug, Clone, Copy)]
pub struct GroupTrackIterState {
    pub field_pos: usize,
    pub group_idx: GroupIdx,
    pub group_offset: GroupLen,
    pub iter_id: GroupTrackIterId,
    #[cfg(feature = "debug_state")]
    pub kind: IterKind,
}

#[derive(Default)]
pub struct GroupTrack {
    pub id: GroupTrackId,
    pub ms_id: MatchSetId,
    pub actor_ref: ActorRef,
    pub parent_group_track_id: Option<GroupTrackId>,

    // stable index of physical index 0
    pub group_index_offset: GroupIdxStable,
    pub parent_group_index_offset: GroupIdxStable,

    pub iter_lookup_table:
        Universe<GroupTrackIterId, GroupTrackIterSortedIndex>,
    pub iter_states: Vec<Cell<GroupTrackIterState>>,
    // store iter potentially invalidates the sort order of the iter_states
    // for performance reasons, it does not eagerly resort
    pub iter_states_sorted: Cell<bool>,
    pub snapshot: SnapshotRef,

    // fields that passed the `end` of the grouping operator but weren't
    // dropped yet.
    pub passed_fields_count: usize,
    pub group_lengths: SizeClassedVecDeque,
    // Contains a bit for each group to indicate whether it is a subgroup of
    // the same parent as it's predecessor or the next one. This is necessary
    // to make sense of zero length groups, where we would otherwise lose
    // this connection, since we can't find the right partner by lockstep
    // iterating over both group lists by length anymore.
    // Edge case: this is **false** for the very first group in each track,
    // because that one should retain the parent group index 0.
    pub starts_new_parent_group: BitVecDeque,

    #[cfg(feature = "debug_state")]
    // for forkcat suchains this points to the source
    pub alias_source: Option<GroupTrackId>,
    #[cfg(feature = "debug_state")]
    // for foreach trailer group tracks this points to the header
    pub corresponding_header: Option<GroupTrackId>,
}

#[derive(Default)]
pub struct GroupTrackManager {
    pub group_tracks: Universe<GroupTrackId, RefCell<GroupTrack>>,
}

pub struct GroupTrackIter<L> {
    group_track: L,
    field_pos: usize,
    group_idx: GroupIdx,
    group_len_rem: GroupLen,
}
pub struct GroupTrackIterMut<'a, T: DerefMut<Target = GroupTrack>> {
    base: GroupTrackIter<T>,
    group_len: usize,
    update_group_len: bool,
    action_buffer: &'a RefCell<ActionBuffer>,
}

impl GroupIdxStable {
    pub fn wrapping_add(self, add: usize) -> GroupIdxStable {
        GroupIdxStable::from(self.into_usize().wrapping_add(add))
    }
    pub fn next(self) -> GroupIdxStable {
        self.wrapping_add(1)
    }
}

impl Display for GroupTrack {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "{} + {:?}",
            self.passed_fields_count, self.group_lengths
        ))
    }
}

impl PartialEq for GroupTrackIterState {
    fn eq(&self, other: &Self) -> bool {
        self.field_pos == other.field_pos
            && self.group_idx == other.group_idx
            && self.group_offset == other.group_offset
    }
}
impl Eq for GroupTrackIterState {}
impl Ord for GroupTrackIterState {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.field_pos.cmp(&other.field_pos) {
            Ordering::Equal => (),
            ord => return ord,
        }
        match self.group_idx.cmp(&other.group_idx) {
            Ordering::Equal => (),
            ord => return ord,
        }
        self.group_offset.cmp(&other.group_offset)
    }
}
impl PartialOrd for GroupTrackIterState {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct RecordGroupActionsApplicator<'a> {
    gl: &'a mut GroupTrack,
    group_idx: GroupIdx,
    inside_passed_elems: bool,
    modified: bool,
    group_len: usize,
    group_len_rem: usize,
    // Only used to check whether the `affected_iters` are outdated.
    iter_group_idx: GroupIdx,
    affected_iters_start: GroupTrackIterSortedIndex,
    affected_iters_end: GroupTrackIterSortedIndex,
    field_pos: usize,
    future_iters_field_pos_delta: isize,
    curr_iters_field_pos_delta: isize,
}

impl<'a> RecordGroupActionsApplicator<'a> {
    fn new(gl: &'a mut GroupTrack) -> Self {
        gl.sort_iters();

        let inside_passed_elems = gl.passed_fields_count > 0;

        let group_len = if inside_passed_elems {
            gl.passed_fields_count
        } else {
            gl.group_lengths.try_get(0).unwrap_or(0)
        };

        RecordGroupActionsApplicator {
            group_idx: 0,
            gl,
            group_len,
            group_len_rem: group_len,
            inside_passed_elems,
            modified: false,
            // Just so it's not the current group.
            iter_group_idx: usize::MAX,
            affected_iters_start: 0,
            affected_iters_end: 0,
            field_pos: 0,
            curr_iters_field_pos_delta: 0,
            future_iters_field_pos_delta: 0,
        }
    }

    fn move_to_field_pos(&mut self, field_idx: usize) {
        if field_idx < self.gl.passed_fields_count {
            return;
        }
        loop {
            let field_pos_delta = field_idx - self.field_pos;
            if field_pos_delta == 0 {
                break;
            }
            if field_pos_delta < self.group_len_rem {
                self.group_len_rem -= field_pos_delta;
                self.field_pos += field_pos_delta;
                break;
            }
            self.field_pos += self.group_len_rem;
            self.next_group();
        }
        if self.iter_group_idx != self.group_idx {
            self.advance_affected_iters_to_group();
        }
        self.advance_affected_iters_to_group_offset();
    }
    fn apply_modifications(&mut self) {
        if !self.modified {
            return;
        }
        self.modified = false;
        self.phase_out_current_iters();
        if self.inside_passed_elems {
            self.gl.passed_fields_count = self.group_len;
            return;
        }
        self.gl.group_lengths.set(self.group_idx, self.group_len);
    }
    fn next_group(&mut self) {
        self.apply_modifications();
        if self.inside_passed_elems {
            self.inside_passed_elems = false;
        } else {
            self.group_idx += 1;
        }
        debug_assert!(
            self.group_idx < self.gl.group_lengths.len(),
            "action index out of bounds for group list"
        );
        self.group_len = self.gl.group_len(self.group_idx);
        self.group_len_rem = self.group_len;
    }
    fn apply_action(&mut self, a: &FieldAction) {
        let mut action_run_len = a.run_len as usize;
        self.move_to_field_pos(a.field_idx);
        match a.kind {
            FieldActionKind::Dup | FieldActionKind::InsertZst(_) => {
                self.group_len_rem += action_run_len;
                self.group_len += action_run_len;
                self.curr_iters_field_pos_delta += action_run_len as isize;
                self.modified = true;
            }
            FieldActionKind::Drop => {
                let group_offset = self.group_len - self.group_len_rem;
                if action_run_len > self.group_len_rem {
                    for is_idx in
                        self.affected_iters_start..self.affected_iters_end
                    {
                        let is =
                            self.gl.iter_states[is_idx as usize].get_mut();
                        is.field_pos = self.field_pos;
                        is.group_offset = group_offset;
                    }
                    self.affected_iters_start = self.affected_iters_end;
                    while action_run_len > self.group_len_rem {
                        self.group_len = group_offset;
                        self.curr_iters_field_pos_delta -=
                            self.group_len_rem as isize;
                        action_run_len -= self.group_len_rem;
                        self.modified = true;
                        self.next_group();
                    }
                }
                let field_pos_unmodified = (self.field_pos as isize
                    - self.curr_iters_field_pos_delta)
                    as usize;
                while self.affected_iters_start < self.affected_iters_end {
                    let is = self.gl.iter_states
                        [self.affected_iters_start as usize]
                        .get_mut();
                    if is.field_pos - field_pos_unmodified > action_run_len {
                        break;
                    }
                    is.field_pos = self.field_pos;
                    is.group_offset = group_offset;
                    self.affected_iters_start += 1;
                }
                self.curr_iters_field_pos_delta -= action_run_len as isize;
                self.group_len -= action_run_len;
                self.group_len_rem -= action_run_len;
                self.modified = true;
            }
        }
    }
    fn apply_iter_field_pos_delta(
        &mut self,
        iter_idx: GroupTrackIterSortedIndex,
    ) {
        let is = self.gl.iter_states[iter_idx as usize].get_mut();
        is.field_pos = (is.field_pos as isize
            + self.future_iters_field_pos_delta)
            as usize;
    }
    fn advance_affected_iters_to_group(&mut self) {
        debug_assert!(self.iter_group_idx != self.group_idx);
        self.iter_group_idx = self.group_idx;
        self.phase_out_current_iters();

        loop {
            if self.affected_iters_start as usize == self.gl.iter_states.len()
            {
                self.affected_iters_end = self.affected_iters_start;
                return;
            }
            let iter_state = self.gl.iter_states
                [self.affected_iters_start as usize]
                .get_mut();
            if iter_state.group_idx > self.group_idx {
                self.affected_iters_end = self.affected_iters_start;
                return;
            }
            if iter_state.group_idx == self.group_idx {
                break;
            }
            self.affected_iters_start += 1;
            self.apply_iter_field_pos_delta(self.affected_iters_start);
        }
        self.affected_iters_end = self.affected_iters_start + 1;
        loop {
            if self.affected_iters_end as usize == self.gl.iter_states.len() {
                return;
            }
            let iter_state = self.gl.iter_states
                [self.affected_iters_end as usize]
                .get_mut();
            if iter_state.group_idx > self.group_idx {
                return;
            }
            self.apply_iter_field_pos_delta(self.affected_iters_end);
            self.affected_iters_end += 1
        }
    }

    fn apply_curr_iter_offset(&mut self, iter_idx: GroupTrackIterSortedIndex) {
        let is = self.gl.iter_states[iter_idx as usize].get_mut();
        is.group_offset = (is.group_offset as isize
            + self.curr_iters_field_pos_delta)
            as usize;
        is.field_pos =
            (is.field_pos as isize + self.curr_iters_field_pos_delta) as usize;
    }

    fn advance_affected_iters_to_group_offset(&mut self) {
        let group_offset = ((self.group_len - self.group_len_rem) as isize
            - self.curr_iters_field_pos_delta)
            as usize;
        loop {
            if self.affected_iters_start == self.affected_iters_end {
                return;
            }
            let is = self.gl.iter_states[self.affected_iters_start as usize]
                .get_mut();
            debug_assert!(is.group_idx == self.group_idx);
            if is.group_offset > group_offset {
                return;
            }
            self.apply_curr_iter_offset(self.affected_iters_start);
            self.affected_iters_start += 1;
        }
    }

    fn phase_out_current_iters(&mut self) {
        for i in self.affected_iters_start..self.affected_iters_end {
            self.apply_curr_iter_offset(i);
        }
        self.affected_iters_start = self.affected_iters_end;
        self.future_iters_field_pos_delta += self.curr_iters_field_pos_delta;
        self.curr_iters_field_pos_delta = 0;
    }

    fn apply_future_iter_modifications(&mut self) {
        for i in self.affected_iters_end as usize..self.gl.iter_states.len() {
            let is = self.gl.iter_states[i].get_mut();
            is.field_pos = (is.field_pos as isize
                + self.future_iters_field_pos_delta)
                as usize;
        }
        self.affected_iters_end =
            self.gl.iter_states.len() as GroupTrackIterSortedIndex;
        self.affected_iters_start = self.affected_iters_end;
    }
}

impl<'a> Drop for RecordGroupActionsApplicator<'a> {
    fn drop(&mut self) {
        self.apply_modifications();
        self.apply_future_iter_modifications();
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct GroupTrackSlice {
    group_count: usize,
    first_group_len: usize,
    full_groups_field_count: usize,
    last_group_len: usize,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct LeadingGroupTrackSlice {
    full_group_count: usize,
    full_group_field_count: usize,
    partial_group_len: Option<usize>,
}

impl GroupTrackSlice {
    pub fn full_group_count(&self) -> usize {
        if self.group_count <= 2 {
            return 0;
        }
        self.group_count - 2
    }
}

impl GroupTrack {
    #[allow(clippy::iter_not_returning_iterator)]
    pub fn iter(&self) -> GroupTrackIter<&Self> {
        GroupTrackIter {
            group_track: self,
            field_pos: 0,
            group_idx: 0,
            group_len_rem: self.group_lengths.try_get(0).unwrap_or(0),
        }
    }
    pub fn parent_group_track_id(&self) -> Option<GroupTrackId> {
        self.parent_group_track_id
    }
    pub fn stable_idx_from_group_idx(
        &self,
        group_index: GroupIdx,
    ) -> GroupIdxStable {
        GroupIdxStable::from_usize(
            group_index.wrapping_add(self.group_index_offset.into_usize()),
        )
    }
    pub fn last_group_idx_stable(&self) -> GroupIdxStable {
        self.stable_idx_from_group_idx(
            self.group_lengths.len().saturating_sub(1),
        )
    }
    pub fn group_idx_from_stable_idx(
        &self,
        stable_idx: GroupIdxStable,
    ) -> GroupIdx {
        stable_idx
            .into_usize()
            .wrapping_sub(self.group_index_offset.into_usize())
    }
    pub fn next_group_idx(&self) -> GroupIdx {
        self.group_lengths.len()
    }
    pub fn group_len(&self, group_index: GroupIdx) -> usize {
        self.get_group_len(group_index).unwrap()
    }
    pub fn push_group(&mut self, group_len: usize, starts_new_parent: bool) {
        self.group_lengths.push_back(group_len);
        self.starts_new_parent_group.push_back(starts_new_parent);
    }
    pub fn get_group_len(&self, group_index: GroupIdx) -> Option<usize> {
        self.group_lengths.try_get(group_index)
    }
    fn apply_field_actions_list<'a>(
        &mut self,
        action_list: impl IntoIterator<Item = &'a FieldAction>,
    ) {
        let action_list = action_list.into_iter();

        let mut gaa = RecordGroupActionsApplicator::new(self);

        // PERF: we could split this loop into two phases,
        // inside_passed_elems and after
        for action in action_list {
            gaa.apply_action(action);
        }
    }
    pub fn eprint_iter_states(&self, indent_level: usize) {
        eprintln!("[");
        for &i in &self.iter_lookup_table {
            eprintln!(
                "{:padding$}{:?},",
                "",
                self.iter_states[i as usize].get(),
                padding = indent_level + 4
            );
        }
        eprint!("{:padding$}]", "", padding = indent_level.saturating_sub(1));
    }
    pub fn apply_field_actions(&mut self, msm: &MatchSetManager) {
        let mut ab = msm.match_sets[self.ms_id].action_buffer.borrow_mut();

        let Some(actor_id) = ab.initialize_actor_ref(
            Some(ActorSubscriber::GroupTrack(self.id)),
            &mut self.actor_ref,
        ) else {
            return;
        };

        let snapshot_prev = self.snapshot;
        let snapshot_new = ab.update_actor_snapshot(actor_id);

        if snapshot_new == snapshot_prev {
            return;
        };

        self.snapshot = snapshot_new;
        ab.bump_snapshot_refcount(snapshot_new);

        let agi = ab.build_actions_from_snapshot(actor_id, snapshot_prev);

        if let Some(agi) = &agi {
            let (s1, s2) = ab.get_action_group_slices(agi);

            let actions = s1.iter().chain(s2.iter());

            #[cfg(feature = "debug_logging_field_actions")]
            {
                eprintln!(
                    "applying actions to group list {:02} (actor {:02}):",
                    self.id, actor_id
                );
                crate::record_data::action_buffer::eprint_action_list(
                    actions.clone(),
                );
                eprint!("   before: {self}",);
                #[cfg(feature = "debug_logging_iter_states")]
                self.eprint_iter_states(4);
                eprintln!();
            }
            self.apply_field_actions_list(actions);
            #[cfg(feature = "debug_logging_field_actions")]
            {
                eprint!("   after:  {self}",);
                #[cfg(feature = "debug_logging_iter_states")]
                self.eprint_iter_states(4);
                eprintln!();
            }
        };
        ab.drop_snapshot_refcount(snapshot_prev);
        ab.release_temp_action_group(agi);
    }

    pub fn count_leading_groups(
        &mut self,
        count: usize,
        end_of_input: bool,
    ) -> LeadingGroupTrackSlice {
        let mut sum = 0;
        let mut full_group_count = 0;
        for group_len in &self.group_lengths {
            let sum_new = sum + group_len;
            if sum_new >= count {
                if sum_new == count && end_of_input {
                    return LeadingGroupTrackSlice {
                        full_group_count: full_group_count + 1,
                        full_group_field_count: count,
                        partial_group_len: None,
                    };
                }
                return LeadingGroupTrackSlice {
                    full_group_count,
                    full_group_field_count: sum,
                    partial_group_len: Some(count - sum),
                };
            }
            full_group_count += 1;
            sum = sum_new;
        }
        LeadingGroupTrackSlice {
            full_group_count,
            full_group_field_count: sum,
            partial_group_len: None,
        }
    }

    pub fn drop_leading_groups(
        &mut self,
        make_passed: bool,
        lgts: LeadingGroupTrackSlice,
        #[cfg_attr(not(feature = "debug_logging"), allow(unused))]
        end_of_input: bool,
    ) {
        let total_field_count =
            lgts.full_group_field_count + lgts.partial_group_len.unwrap_or(0);
        #[cfg(feature = "debug_logging")]
        {
            eprintln!(
                "{} {total_field_count} leading fields for group {}{}:",
                if make_passed { "passing" } else { "dropping" },
                self.id,
                if end_of_input { "(eof)" } else { "" }
            );
            eprint!("   before:  {self}",);
            #[cfg(feature = "debug_logging_iter_states")]
            self.eprint_iter_states(8);
            eprintln!();
        }
        self.group_lengths.drain(0..lgts.full_group_count);
        let parent_starts_dropped = self
            .starts_new_parent_group
            .iter()
            .take(lgts.full_group_count)
            .filter(|x| *x)
            .count();
        self.starts_new_parent_group
            .drop_front(lgts.full_group_count);
        self.parent_group_index_offset = self
            .parent_group_index_offset
            .wrapping_add(parent_starts_dropped);

        if let Some(v) = lgts.partial_group_len {
            self.group_lengths.sub_value(0, v);
        }

        self.group_index_offset =
            self.group_index_offset.wrapping_add(lgts.full_group_count);
        if make_passed {
            self.passed_fields_count += total_field_count;
        }
        for it in &mut self.iter_states {
            let it = it.get_mut();
            match it.group_idx.cmp(&lgts.full_group_count) {
                Ordering::Less => {
                    it.group_idx = 0;
                    it.group_offset = 0;
                }
                Ordering::Equal => {
                    it.group_offset = it
                        .group_offset
                        .saturating_sub(lgts.partial_group_len.unwrap_or(0))
                }
                Ordering::Greater => it.group_idx -= lgts.full_group_count,
            }
        }
        #[cfg(feature = "debug_logging")]
        {
            eprint!("   after:   {self}",);
            #[cfg(feature = "debug_logging_iter_states")]
            self.eprint_iter_states(8);
            eprintln!();
        }
    }

    pub fn drop_leading_fields(
        &mut self,
        make_passed: bool,
        count: usize,
        end_of_input: bool,
    ) {
        let lgts = self.count_leading_groups(count, end_of_input);
        self.drop_leading_groups(make_passed, lgts, end_of_input)
    }

    pub fn append_leading_groups_to_aliases(
        &self,
        lgts: LeadingGroupTrackSlice,
        alias: &mut GroupTrack,
    ) {
        if self.group_lengths.is_empty() {
            debug_assert_eq!(lgts.full_group_count, 0);
            debug_assert_eq!(lgts.partial_group_len, None);
            return;
        }

        #[cfg(feature = "debug_logging")]
        {
            eprintln!(
                "appending leading groups from track {} to {}: full: {}({}) partial: {}({}) ",
                self.id, alias.id,
                lgts.full_group_count,
                lgts.full_group_field_count,
                lgts.partial_group_len.map(|_|1).unwrap_or(0),
                lgts.partial_group_len.unwrap_or(0),
            );
            eprint!("   before:  {alias}",);
            #[cfg(feature = "debug_logging_iter_states")]
            self.eprint_iter_states(8);
            eprintln!();
        }

        alias
            .group_lengths
            .promote_to_size_class(self.group_lengths.size_class());

        if alias.group_lengths.is_empty() {
            alias.group_lengths.extend_truncated(
                self.group_lengths.iter().take(lgts.full_group_count),
            );
            alias.starts_new_parent_group.extend(
                self.starts_new_parent_group
                    .iter()
                    .take(lgts.full_group_count),
            );
            if let Some(v) = lgts.partial_group_len {
                alias.group_lengths.push_back_truncated(v);
                alias.starts_new_parent_group.push_back(
                    self.starts_new_parent_group[lgts.full_group_count],
                )
            }
        } else {
            debug_assert!(
                self.group_index_offset.into_usize()
                    == alias
                        .group_index_offset
                        .into_usize()
                        .wrapping_add(alias.group_lengths.len() - 1)
            );
            let child_last_index = alias.group_lengths.len() - 1;
            if lgts.full_group_count > 0 {
                let first_group_len = self.group_lengths.get(0);
                alias
                    .group_lengths
                    .add_value(child_last_index, first_group_len);
                let full_group_count = lgts.full_group_count.saturating_sub(1);
                alias.group_lengths.extend_truncated(
                    self.group_lengths.iter().skip(1).take(full_group_count),
                );
                alias.starts_new_parent_group.extend(
                    self.starts_new_parent_group
                        .iter()
                        .skip(1)
                        .take(full_group_count),
                );
                if let Some(v) = lgts.partial_group_len {
                    alias.group_lengths.push_back_truncated(v);
                    alias.starts_new_parent_group.push_back(
                        self.starts_new_parent_group[full_group_count + 1],
                    );
                }
            } else if let Some(v) = lgts.partial_group_len {
                alias.group_lengths.add_value(child_last_index, v);
            }
        }

        #[cfg(feature = "debug_logging")]
        {
            eprint!("   after:   {alias}",);
            #[cfg(feature = "debug_logging_iter_states")]
            self.eprint_iter_states(8);
            eprintln!();
        }
    }

    pub fn lookup_iter(
        &self,
        iter_id: GroupTrackIterId,
    ) -> GroupTrackIter<&Self> {
        Self::lookup_iter_for_deref(self, iter_id)
    }
    fn build_iter_from_iter_state<T: Deref<Target = Self>>(
        list: T,
        iter_state: GroupTrackIterState,
    ) -> GroupTrackIter<T> {
        GroupTrackIter {
            field_pos: iter_state.field_pos,
            group_idx: iter_state.group_idx,
            group_len_rem: list.group_len(iter_state.group_idx)
                - iter_state.group_offset,
            group_track: list,
        }
    }
    pub fn lookup_iter_for_deref<T: Deref<Target = Self>>(
        list: T,
        iter_id: GroupTrackIterId,
    ) -> GroupTrackIter<T> {
        let iter_index = list.iter_lookup_table[iter_id];
        let iter_state = list.iter_states[iter_index as usize].get();
        Self::build_iter_from_iter_state(list, iter_state)
    }
    pub fn lookup_iter_for_deref_mut<T: DerefMut<Target = Self>>(
        list: T,
        iter_id: GroupTrackIterId,
        msm: &MatchSetManager,
        actor_id: ActorId,
    ) -> GroupTrackIterMut<T> {
        let action_buffer = &msm.match_sets[list.ms_id].action_buffer;
        let iter_index = list.iter_lookup_table[iter_id];
        let iter_state = list.iter_states[iter_index as usize].get();
        let base = Self::build_iter_from_iter_state(list, iter_state);
        action_buffer.borrow_mut().begin_action_group(actor_id);
        GroupTrackIterMut {
            group_len: base.group_len_rem + iter_state.group_offset,
            base,
            update_group_len: false,
            action_buffer,
        }
    }
    pub fn store_iter<T: Deref<Target = Self>>(
        &self,
        iter_id: GroupTrackIterId,
        iter: &GroupTrackIter<T>,
    ) {
        let iter_sorting_idx = self.iter_lookup_table[iter_id] as usize;
        #[cfg(feature = "debug_state")]
        let kind = self.iter_states[iter_sorting_idx].get().kind;
        let iter_state = GroupTrackIterState {
            field_pos: iter.field_pos,
            group_idx: iter.group_idx,
            group_offset: iter
                .group_track
                .group_lengths
                .try_get(iter.group_idx)
                .unwrap_or(0)
                - iter.group_len_rem,
            iter_id,
            #[cfg(feature = "debug_state")]
            kind,
        };

        // #[cfg(feature = "debug_logging_iter_states")]
        // eprintln!("storing group {} iter {iter_id}: {iter_state:?}",
        // self.id);

        self.iter_states[iter_sorting_idx].set(iter_state);
        // PERF: we could do something clever here like checking if it's still
        // in order or storing that only one iter is out of order...
        self.iter_states_sorted.set(false);
    }
    fn lookup_iter_sort_key_range(
        &self,
        group_index_range: impl RangeBounds<GroupIdx>,
        field_pos_range: impl RangeBounds<usize>,
    ) -> Range<GroupTrackIterSortedIndex> {
        let group_index_range =
            range_bounds_to_range(group_index_range, self.iter_states.len());
        let field_pos_range =
            range_bounds_to_range(field_pos_range, usize::MAX);

        let mut start = self
            .iter_states
            .binary_search_by_key(
                &(group_index_range.start, field_pos_range.start),
                |is| {
                    let is = is.get();
                    (is.group_idx, is.group_offset)
                },
            )
            .unwrap_or_else(|insert_point| insert_point);
        let mut end = if start == self.iter_states.len() {
            start
        } else {
            start + 1
        };
        loop {
            if start == 0 {
                break;
            }
            let is = self.iter_states[start - 1].get();
            if is.group_idx < group_index_range.start {
                break;
            }
            if is.group_idx == group_index_range.start
                && is.field_pos < field_pos_range.start
            {
                break;
            }
            start -= 1;
        }
        loop {
            if end == self.iter_states.len() {
                break;
            }
            let is = self.iter_states[end].get();
            if is.group_idx >= group_index_range.end
                || is.field_pos >= field_pos_range.end
            {
                break;
            }
            end += 1;
        }
        start as u32..end as u32
    }

    pub fn claim_iter(
        &mut self,
        #[cfg_attr(not(feature = "debug_state"), allow(unused))]
        kind: IterKind,
    ) -> GroupTrackIterId {
        let iter_id = self.iter_lookup_table.claim_with_value(
            self.iter_states.len() as GroupTrackIterSortedIndex,
        );
        let iter_state = GroupTrackIterState {
            iter_id,
            field_pos: self.passed_fields_count,
            group_idx: 0,
            group_offset: 0,
            #[cfg(feature = "debug_state")]
            kind,
        };
        self.iter_states.push(Cell::new(iter_state));
        iter_id
    }
    pub fn sort_iters(&mut self) {
        if self.iter_states_sorted.get() {
            return;
        }
        self.iter_states_sorted.set(true);
        self.iter_states.sort();
        for (iter_sorting_idx, is) in self.iter_states.iter().enumerate() {
            let iter_id = is.get().iter_id;
            self.iter_lookup_table[iter_id] = iter_sorting_idx as u32;
        }
    }
    fn advance_affected_iters(
        &mut self,
        iters: Range<GroupTrackIterSortedIndex>,
        count: isize,
    ) {
        for i in iters.clone() {
            let is = self.iter_states[i as usize].get_mut();
            is.group_offset = (is.group_offset as isize + count) as usize;
            is.field_pos = (is.field_pos as isize + count) as usize;
        }
        // TODO: // PERF: this is terrible. optimize on the caller side
        // by being lazy and carrying some sort of pending delta
        for is in &mut self.iter_states[iters.end as usize..] {
            let is = is.get_mut();
            is.field_pos = (is.field_pos as isize + count) as usize;
        }
    }
    fn lookup_and_advance_affected_iters_(
        &mut self,
        group_index_range: impl RangeBounds<GroupIdx>,
        field_pos_range: impl RangeBounds<usize>,
        count: isize,
    ) {
        self.advance_affected_iters(
            self.lookup_iter_sort_key_range(
                group_index_range,
                field_pos_range,
            ),
            count,
        )
    }
    pub fn next_group_index_stable(&self) -> GroupIdxStable {
        if self.group_lengths.is_empty() {
            return self.group_index_offset;
        }
        GroupIdxStable::from_usize(
            self.group_index_offset
                .into_usize()
                .wrapping_add(self.group_lengths.len()),
        )
    }
}

impl GroupTrackManager {
    pub fn add_group_track(
        &mut self,
        parent_list: Option<GroupTrackId>,
        ms_id: MatchSetId,
        actor: ActorRef,
    ) -> GroupTrackId {
        let id = self.group_tracks.peek_claim_id();
        self.group_tracks.claim_with_value(RefCell::new(GroupTrack {
            id,
            ms_id,
            actor_ref: actor,
            parent_group_track_id: parent_list,
            group_index_offset: GroupIdxStable::ZERO,
            parent_group_index_offset: GroupIdxStable::ZERO,
            passed_fields_count: 0,
            group_lengths: SizeClassedVecDeque::default(),
            starts_new_parent_group: BitVecDeque::new(),
            iter_states: Vec::default(),
            iter_lookup_table: Universe::default(),
            snapshot: SnapshotRef::default(),
            iter_states_sorted: Cell::new(true),
            #[cfg(feature = "debug_state")]
            alias_source: None,
            #[cfg(feature = "debug_state")]
            corresponding_header: None,
        }));
        id
    }
    pub fn claim_group_track_iter(
        &mut self,
        list_id: GroupTrackId,
        kind: IterKind,
    ) -> GroupTrackIterId {
        self.group_tracks[list_id].borrow_mut().claim_iter(kind)
    }
    pub fn claim_group_track_iter_ref(
        &mut self,
        list_id: GroupTrackId,
        kind: IterKind,
    ) -> GroupTrackIterRef {
        GroupTrackIterRef {
            track_id: list_id,
            iter_id: self.claim_group_track_iter(list_id, kind),
        }
    }
    pub fn lookup_group_track_iter(
        &self,
        iter_ref: GroupTrackIterRef,
        msm: &MatchSetManager,
    ) -> GroupTrackIter<Ref<GroupTrack>> {
        self.group_tracks[iter_ref.track_id]
            .borrow_mut()
            .apply_field_actions(msm);
        GroupTrack::lookup_iter_for_deref(
            self.group_tracks[iter_ref.track_id].borrow(),
            iter_ref.iter_id,
        )
    }
    pub fn lookup_group_track_iter_mut<'a>(
        &'a self,
        track_id: GroupTrackId,
        iter_id: GroupTrackIterId,
        msm: &'a MatchSetManager,
        actor_id: ActorId,
    ) -> GroupTrackIterMut<RefMut<'a, GroupTrack>> {
        let mut list = self.borrow_group_track_mut(track_id);
        list.apply_field_actions(msm);
        GroupTrack::lookup_iter_for_deref_mut(list, iter_id, msm, actor_id)
    }
    pub fn lookup_group_track_iter_mut_from_ref<'a>(
        &'a self,
        iter_ref: GroupTrackIterRef,
        msm: &'a MatchSetManager,
        actor_id: ActorId,
    ) -> GroupTrackIterMut<RefMut<'a, GroupTrack>> {
        self.lookup_group_track_iter_mut(
            iter_ref.track_id,
            iter_ref.iter_id,
            msm,
            actor_id,
        )
    }

    pub fn store_record_group_track_iter<L: Deref<Target = GroupTrack>>(
        &self,
        iter_ref: GroupTrackIterRef,
        iter: &GroupTrackIter<L>,
    ) {
        self.group_tracks[iter_ref.track_id]
            .borrow()
            .store_iter(iter_ref.iter_id, iter);
    }

    pub fn apply_actions_to_list(
        &self,
        msm: &MatchSetManager,
        group_track_id: GroupTrackId,
    ) {
        self.group_tracks[group_track_id]
            .borrow_mut()
            .apply_field_actions(msm)
    }
    pub fn append_group_to_track(
        &self,
        group_track_id: GroupTrackId,
        field_count: usize,
        starts_new_parent: bool,
    ) {
        let mut gl = self.group_tracks[group_track_id].borrow_mut();
        debug_assert!(gl.parent_group_track_id.is_none());
        gl.group_lengths.push_back(field_count);
        gl.starts_new_parent_group.push_back(starts_new_parent);
    }
    pub fn borrow_group_track(
        &self,
        group_track_id: GroupTrackId,
    ) -> Ref<GroupTrack> {
        self.group_tracks[group_track_id].borrow()
    }
    pub fn borrow_group_track_mut(
        &self,
        group_track_id: GroupTrackId,
    ) -> RefMut<GroupTrack> {
        self.group_tracks[group_track_id].borrow_mut()
    }

    pub fn propagate_leading_groups_to_aliases(
        &self,
        msm: &MatchSetManager,
        group_track_id: GroupTrackId,
        count: usize,
        end_of_input: bool,
        pass_in_source_group: bool,
        aliases: impl IntoIterator<Item = GroupTrackId>,
    ) {
        let mut gt = self.group_tracks[group_track_id].borrow_mut();
        gt.apply_field_actions(msm);

        let lgts = gt.count_leading_groups(count, end_of_input);
        for alias_id in aliases {
            gt.append_leading_groups_to_aliases(
                lgts,
                &mut self.group_tracks[alias_id].borrow_mut(),
            );
        }
        if pass_in_source_group {
            gt.drop_leading_groups(true, lgts, end_of_input);
        }
    }

    pub fn merge_leading_groups_into_parent(
        &self,
        msm: &MatchSetManager,
        child_group_track_id: GroupTrackId,
        field_count: usize,
        end_of_input: bool,
        parent_new_group_track_id: GroupTrackId,
    ) {
        let mut child_gt =
            self.group_tracks[child_group_track_id].borrow_mut();
        child_gt.apply_field_actions(msm);

        let parent_prev_gt = self.group_tracks
            [child_gt.parent_group_track_id().unwrap()]
        .borrow();

        let mut parent_new_gt =
            self.group_tracks[parent_new_group_track_id].borrow_mut();

        // PERF: is that actually neccessary?
        parent_new_gt.apply_field_actions(msm);

        let mut processed_field_count = 0;

        let mut parent_group_id_stable = child_gt.parent_group_index_offset;
        let mut prev_parent_group_idx =
            parent_prev_gt.group_idx_from_stable_idx(parent_group_id_stable);

        let mut processed_child_group_count = 0;

        let mut first_subgroup_found = false;
        let mut first_starts_new_parent = true;

        let mut child_groups_sum = 0;

        let mut final_group_len = 0;

        let mut end_reached = field_count == 0;
        while !end_reached {
            let mut group_len =
                child_gt.group_lengths.get(processed_child_group_count);

            let starts_new_parent =
                child_gt.starts_new_parent_group[processed_child_group_count];

            if !first_subgroup_found {
                first_subgroup_found = true;
                first_starts_new_parent = starts_new_parent;
            }

            end_reached = processed_field_count + group_len >= field_count;

            if end_reached {
                group_len = field_count - processed_field_count;
                final_group_len = group_len;
            }

            processed_field_count += group_len;
            processed_child_group_count += 1;
            child_groups_sum += group_len;

            if !starts_new_parent {
                continue;
            }

            first_subgroup_found = false;

            if parent_new_gt.group_lengths.is_empty()
                || (first_starts_new_parent
                    && parent_group_id_stable
                        != parent_new_gt.last_group_idx_stable())
            {
                parent_new_gt.group_lengths.push_back(child_groups_sum);
                parent_new_gt.starts_new_parent_group.push_back(
                    parent_prev_gt.starts_new_parent_group
                        [prev_parent_group_idx],
                );
            } else {
                debug_assert!(
                    parent_new_gt.last_group_idx_stable()
                        == parent_group_id_stable
                );
                let last_index = parent_new_gt.group_lengths.len() - 1;
                parent_new_gt
                    .group_lengths
                    .add_value(last_index, child_groups_sum);
            }

            if first_starts_new_parent {
                prev_parent_group_idx += 1;
                parent_group_id_stable = parent_group_id_stable.next();
            }

            child_groups_sum = 0;
        }

        if first_subgroup_found {
            if parent_new_gt.group_lengths.is_empty()
                || (first_starts_new_parent
                    && parent_group_id_stable
                        != parent_new_gt.last_group_idx_stable())
            {
                parent_new_gt.group_lengths.push_back(child_groups_sum);
                parent_new_gt.starts_new_parent_group.push_back(
                    parent_prev_gt.starts_new_parent_group
                        [prev_parent_group_idx],
                );
            } else {
                let last_index = parent_new_gt.group_lengths.len() - 1;
                parent_new_gt
                    .group_lengths
                    .add_value(last_index, child_groups_sum);
            }
        }

        let lgts = if end_of_input {
            LeadingGroupTrackSlice {
                full_group_count: processed_child_group_count,
                full_group_field_count: field_count,
                partial_group_len: None,
            }
        } else {
            LeadingGroupTrackSlice {
                full_group_count: processed_child_group_count
                    .saturating_sub(1),
                full_group_field_count: field_count - final_group_len,
                partial_group_len: Some(final_group_len),
            }
        };
        child_gt.drop_leading_groups(true, lgts, end_of_input);
    }
}

impl<L: Deref<Target = GroupTrack>> GroupTrackIter<L> {
    pub fn field_pos(&self) -> usize {
        self.field_pos
    }
    pub fn group_idx(&self) -> usize {
        self.group_idx
    }
    pub fn group_idx_stable(&self) -> GroupIdxStable {
        self.group_track.stable_idx_from_group_idx(self.group_idx)
    }
    pub fn group_len_rem(&self) -> usize {
        self.group_len_rem
    }
    pub fn store_iter(&self, iter_id: GroupTrackIterId) {
        self.group_track.store_iter(iter_id, self);
    }
    pub fn next_n_fields(&mut self, n: usize) -> usize {
        let mut n_rem = n;
        loop {
            if n_rem <= self.group_len_rem {
                self.group_len_rem -= n_rem;
                self.field_pos += n_rem;
                return n;
            }
            n_rem -= self.group_len_rem;
            self.field_pos += self.group_len_rem;
            let Some(group_len) =
                self.group_track.group_lengths.try_get(self.group_idx + 1)
            else {
                self.group_len_rem = 0;
                return n - n_rem;
            };
            self.group_idx += 1;
            self.group_len_rem = group_len;
        }
    }
    pub fn next_group(&mut self) {
        self.group_idx += 1;
        self.field_pos += self.group_len_rem;
        self.group_len_rem =
            self.group_track.group_lengths.get(self.group_idx);
    }
    pub fn try_next_group(&mut self) -> bool {
        let Some(next_group_len) =
            self.group_track.group_lengths.try_get(self.group_idx + 1)
        else {
            return false;
        };
        self.group_idx += 1;
        self.group_len_rem = next_group_len;
        true
    }
    pub fn is_last_group(&self) -> bool {
        self.group_track.group_lengths.len() <= self.group_idx + 1
    }
    pub fn is_end(&self, end_of_input: bool) -> bool {
        self.is_last_group() && self.is_end_of_group(end_of_input)
    }
    pub fn is_end_of_group(&self, end_of_input: bool) -> bool {
        if self.group_len_rem != 0 {
            return false;
        }
        if self.group_idx + 1 == self.group_track.group_lengths.len() {
            return end_of_input;
        }
        true
    }
    pub fn consume_group_slice(&mut self, count: usize) -> GroupTrackSlice {
        let mut gts = GroupTrackSlice {
            group_count: 1,
            first_group_len: self.group_len_rem,
            full_groups_field_count: 0,
            last_group_len: 0,
        };
        if !self.try_next_group() {
            self.next_n_fields(self.group_len_rem);
            return gts;
        }
        gts.last_group_len = self.group_len_rem;
        gts.group_count += 1;
        let mut count_total = gts.first_group_len + gts.last_group_len;
        while self.try_next_group() {
            gts.group_count += 1;
            count_total += self.group_len_rem;
            if count_total > count {
                let rem = count_total - count;
                gts.last_group_len = rem;
                self.next_n_fields(rem);
                return gts;
            }
            gts.full_groups_field_count += gts.last_group_len;
            gts.last_group_len = self.group_len_rem;
            if count_total == count {
                self.next_n_fields(self.group_len_rem);
                return gts;
            }
        }
        gts
    }

    /// Advances the iterator until it points at the start of a a non empty
    /// group. Returns the number of groups skipped.
    /// The initial group does *not* have to be empty, but the iterator
    /// must have passed all it's elements. Otherwise 0 is returned.
    pub fn skip_empty_groups(&mut self) -> usize {
        let mut count = 0;
        while self.group_len_rem == 0 && self.try_next_group() {
            count += 1;
        }
        count
    }

    pub fn group_track(&self) -> &GroupTrack {
        &self.group_track
    }
}

impl<'a, T: DerefMut<Target = GroupTrack>> GroupTrackIterMut<'a, T> {
    pub fn field_pos(&self) -> usize {
        self.base.field_pos()
    }
    pub fn group_idx(&self) -> GroupIdx {
        self.base.group_idx()
    }
    pub fn group_idx_stable(&self) -> GroupIdxStable {
        self.base.group_idx_stable()
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
    pub fn is_end(&self, end_of_input: bool) -> bool {
        self.base.is_end(end_of_input)
    }
    pub fn group_len_before(&self) -> usize {
        self.group_len - self.base.group_len_rem
    }
    pub fn store_iter(mut self, iter_id: GroupTrackIterId) {
        self.update_group();
        self.base.store_iter(iter_id);
    }
    pub fn write_back_group_len(&mut self) {
        self.base
            .group_track
            .group_lengths
            .set(self.base.group_idx, self.group_len);
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
        let mut group_idx_phys = self.base.group_idx;
        while let Some(group_len) = self
            .base
            .group_track
            .group_lengths
            .try_get(group_idx_phys + 1)
        {
            new_group_len = group_len;
            if group_len >= n_rem {
                n_rem = 0;
                break;
            }
            n_rem -= group_len;
            group_idx_phys += 1;
        }
        self.base.group_idx = group_idx_phys;
        self.group_len = new_group_len;
        self.base.group_len_rem = new_group_len - n_rem;
        let fields_advanced = n - n_rem;
        self.base.field_pos += fields_advanced;
        fields_advanced
    }
    fn next_group_raw(&mut self) {
        self.base.group_idx += 1;
        self.base.field_pos += self.base.group_len_rem;
        let gl = self.base.group_track.group_lengths.get(self.base.group_idx);
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
        if count == 0 {
            return;
        }

        self.base.group_track.lookup_and_advance_affected_iters_(
            self.base.group_idx..=self.base.group_idx,
            self.base.field_pos..,
            count as isize,
        );

        let field_pos_prev = self.base.field_pos;
        self.update_group_len = true;
        self.group_len += count;
        self.base.field_pos += count;

        self.action_buffer.borrow_mut().push_action(
            FieldActionKind::InsertZst(repr),
            field_pos_prev,
            count,
        );
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
        self.action_buffer.borrow_mut().push_action(
            FieldActionKind::Dup,
            self.base.field_pos,
            count,
        );
        self.base.group_track.lookup_and_advance_affected_iters_(
            self.base.group_idx..=self.base.group_idx,
            self.base.field_pos + 1..,
            count as isize,
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
        self.base.field_pos += count;
        self.update_group_len = true;
        self.action_buffer.borrow_mut().push_action(
            FieldActionKind::Dup,
            field_pos,
            count,
        );
        self.base.group_track.lookup_and_advance_affected_iters_(
            self.base.group_idx..=self.base.group_idx,
            field_pos + 1..,
            count as isize,
        );
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
        self.action_buffer.borrow_mut().push_action(
            FieldActionKind::Dup,
            field_pos,
            count,
        );
        self.base.group_track.lookup_and_advance_affected_iters_(
            self.base.group_idx..=self.base.group_idx,
            field_pos + 1..,
            count as isize,
        );
    }
    pub fn dup_with_field_pos(&mut self, field_pos: usize, count: usize) {
        match field_pos.cmp(&self.base.field_pos) {
            Ordering::Less => self.dup_before(field_pos, count),
            Ordering::Equal => self.dup(count),
            Ordering::Greater => self.dup_after(field_pos, count),
        }
    }
    fn drop_no_field_action(&mut self, mut count: usize) {
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
            todo!("update affected iters")
        }
        self.base.group_len_rem -= count;
        self.group_len -= count;
        self.update_group_len = true;
        self.base.group_track.lookup_and_advance_affected_iters_(
            self.base.group_idx..=self.base.group_idx,
            self.base.field_pos + 1..,
            count as isize,
        );
    }
    pub fn drop(&mut self, count: usize) {
        if count == 0 {
            return;
        }
        self.drop_no_field_action(count);
        self.action_buffer.borrow_mut().push_action(
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
        self.action_buffer.borrow_mut().push_action(
            FieldActionKind::Drop,
            self.base.field_pos - count,
            count,
        );
        self.group_len -= count;
        self.update_group_len = true;
        self.base.field_pos -= count;
        self.base.group_track.lookup_and_advance_affected_iters_(
            self.base.group_idx..=self.base.group_idx,
            self.base.field_pos + 1..,
            -(count as isize),
        );
    }
    fn drop_in_earlier_groups_no_field_action(
        &mut self,
        field_pos: usize,
        count: usize,
    ) {
        if count == 0 {
            return;
        }
        // PERF: maybe heuristic to start from the beginning instead
        // common case is field_pos == 0
        let field_diff = self.field_pos() - field_pos;
        let mut fields_to_skip = field_diff - count;
        let mut group_idx = self.group_idx();
        let mut group_len = self.group_len_before();
        while fields_to_skip >= group_len {
            group_idx -= 1;
            fields_to_skip -= group_len;
            group_len = self.base.group_track.group_lengths.get(group_idx);
        }
        let group_len_to_drop = count.min(group_len - fields_to_skip);
        self.base
            .group_track
            .group_lengths
            .sub_value(group_idx, group_len_to_drop);
        let mut count_rem = count - group_len_to_drop;
        while count_rem > 0 {
            group_idx -= 1;
            group_len = self.base.group_track.group_lengths.get(group_idx);
            let group_len_to_drop = count.min(group_len);
            self.base
                .group_track
                .group_lengths
                .sub_value(group_idx, group_len_to_drop);
            count_rem -= group_len_to_drop;
        }
        // HACK: //TODO: iters need to be advanced
    }
    pub fn drop_before(&mut self, field_pos: usize, mut count: usize) {
        if count == 0 {
            return;
        }
        self.action_buffer.borrow_mut().push_action(
            FieldActionKind::Drop,
            field_pos,
            count,
        );
        let mut pos_delta = self.base.field_pos - field_pos;
        if pos_delta > self.group_len_before() {
            let group_start = self.field_pos() - self.group_len_before();
            let drops_in_group = count.saturating_sub(group_start - field_pos);
            let drops_before_group = count - drops_in_group;
            debug_assert!(drops_before_group != 0);
            self.drop_in_earlier_groups_no_field_action(
                field_pos,
                drops_before_group,
            );
            self.base.field_pos -= drops_before_group;
            if drops_in_group == 0 {
                return;
            }
            pos_delta = self.group_len_before() - drops_in_group;
            count = drops_in_group;
        }

        if pos_delta >= count {
            self.base.group_track.lookup_and_advance_affected_iters_(
                self.base.group_idx..=self.base.group_idx,
                self.base.field_pos - pos_delta + 1..,
                -(count as isize),
            );
            self.group_len -= count;
            self.update_group_len = true;
            self.base.field_pos -= count;
            return;
        }
        self.group_len -= pos_delta;
        self.base.field_pos -= pos_delta;
        self.drop_no_field_action(count - pos_delta);
    }
    pub fn drop_after(&mut self, field_pos: usize, count: usize) {
        if count == 0 {
            return;
        }
        let pos_delta = field_pos - self.base.field_pos;
        debug_assert!(self.base.group_len_rem >= pos_delta + count);
        self.action_buffer.borrow_mut().push_action(
            FieldActionKind::Drop,
            field_pos,
            count,
        );
        if count <= self.base.group_len_rem - pos_delta {
            self.base.group_track.lookup_and_advance_affected_iters_(
                self.base.group_idx..=self.base.group_idx,
                field_pos + 1..,
                -(count as isize),
            );
            self.group_len -= count;
            self.base.group_len_rem -= count;
            self.update_group_len = true;
            return;
        }
        let group_pos = self.group_len - self.base.group_len_rem;
        let group_len = field_pos;
        let group_idx_phys = self.base.group_idx;
        self.base.group_len_rem -= pos_delta;
        self.drop_no_field_action(count);
        self.write_back_group_len();
        self.base.group_idx = group_idx_phys;
        self.group_len = group_len;
        self.base.group_len_rem = group_len - group_pos;
    }

    pub fn drop_with_field_pos(&mut self, field_pos: usize, count: usize) {
        match field_pos.cmp(&self.base.field_pos) {
            Ordering::Less => self.drop_before(field_pos, count),
            Ordering::Equal => self.drop(count),
            Ordering::Greater => self.drop_after(field_pos, count),
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
        while self.group_len == 0 && self.try_next_group_raw() {
            count += 1;
        }
        count
    }

    /// Advances the iterator until it points at the start of a group with
    /// **not** exactly one element. Returns the number of groups skipped.
    /// The initial group must have 1 element *remaining* (not necessarily
    /// total), otherwise 0 is returned. If the iterator reaches the last
    /// group, it has exactly one element and `end_of_input` is true, the
    /// one element is skipped and the group is included in the count.
    pub fn skip_single_elem_groups(
        &mut self,
        end_of_input: bool,
        max: usize,
    ) -> usize {
        if self.base.group_len_rem != 1 || max == 0 {
            return 0;
        };
        if !self.try_next_group() {
            if end_of_input {
                self.base.group_len_rem = 0;
                return 1;
            }
            return 0;
        }
        let mut count = 1;
        while count < max && self.group_len == 1 {
            if !self.try_next_group_raw() {
                if end_of_input {
                    self.base.group_len_rem = 0;
                    return count + 1;
                }
                return count;
            }
            count += 1;
        }
        count
    }

    pub fn group_starts_new_parent(&self) -> bool {
        self.base.group_track.starts_new_parent_group[self.base.group_idx]
    }
}

impl<'a, T: DerefMut<Target = GroupTrack>> Drop for GroupTrackIterMut<'a, T> {
    fn drop(&mut self) {
        self.update_group();

        let mut ab = self.action_buffer.borrow_mut();
        ab.end_action_group();

        let list = &mut *self.base.group_track;

        let Some(actor_id) = ab.initialize_actor_ref(
            Some(ActorSubscriber::GroupTrack(list.id)),
            &mut list.actor_ref,
        ) else {
            return;
        };

        let snapshot = ab.update_actor_snapshot(actor_id);
        if snapshot == list.snapshot {
            return;
        }

        ab.drop_snapshot_refcount(list.snapshot);
        ab.bump_snapshot_refcount(snapshot);
        list.snapshot = snapshot;
    }
}

#[cfg(test)]
mod test {
    use std::{cell::Cell, collections::VecDeque};

    use crate::{
        record_data::{
            field_action::{FieldAction, FieldActionKind},
            group_track::{GroupTrack, GroupTrackIterState},
        },
        utils::{
            size_classed_vec_deque::SizeClassedVecDeque, universe::Universe,
        },
    };

    #[cfg(feature = "debug_state")]
    use crate::record_data::iter_hall::IterKind;

    #[test]
    fn drop_on_passed_fields() {
        let mut gl = GroupTrack {
            passed_fields_count: 2,
            ..Default::default()
        };

        gl.apply_field_actions_list(&[FieldAction::new(
            FieldActionKind::Drop,
            1,
            1,
        )]);

        assert_eq!(gl.passed_fields_count, 1);
    }

    #[test]
    fn drop_in_passed_affects_iterator_correctly() {
        let mut gl = GroupTrack {
            passed_fields_count: 2,
            group_lengths: SizeClassedVecDeque::Sc8(VecDeque::from([2])),
            iter_states: vec![Cell::new(GroupTrackIterState {
                field_pos: 3,
                group_idx: 0,
                group_offset: 1,
                iter_id: 0,
                #[cfg(feature = "debug_state")]
                kind: IterKind::Undefined,
            })],
            iter_lookup_table: Universe::from([0].into_iter()),
            ..Default::default()
        };

        gl.apply_field_actions_list(&[FieldAction::new(
            FieldActionKind::Drop,
            0,
            1,
        )]);

        assert_eq!(
            gl.iter_states[0].get(),
            GroupTrackIterState {
                field_pos: 2,
                group_idx: 0,
                group_offset: 1,
                iter_id: 0,
                #[cfg(feature = "debug_state")]
                kind: IterKind::Undefined
            }
        );
    }

    #[test]
    fn drop_in_group_affects_iterator_correctly() {
        let mut gl = GroupTrack {
            passed_fields_count: 1,
            group_lengths: SizeClassedVecDeque::Sc8(VecDeque::from([3])),
            iter_states: vec![
                Cell::new(GroupTrackIterState {
                    field_pos: 2,
                    group_idx: 0,
                    group_offset: 1,
                    iter_id: 0,
                    #[cfg(feature = "debug_state")]
                    kind: IterKind::Undefined,
                }),
                Cell::new(GroupTrackIterState {
                    field_pos: 3,
                    group_idx: 0,
                    group_offset: 2,
                    iter_id: 0,
                    #[cfg(feature = "debug_state")]
                    kind: IterKind::Undefined,
                }),
            ],
            iter_lookup_table: Universe::from([0].into_iter()),
            ..Default::default()
        };

        gl.apply_field_actions_list(&[FieldAction::new(
            FieldActionKind::Drop,
            2,
            1,
        )]);

        assert_eq!(
            &gl.iter_states.iter().map(Cell::get).collect::<Vec<_>>(),
            &[
                GroupTrackIterState {
                    field_pos: 2,
                    group_idx: 0,
                    group_offset: 1,
                    iter_id: 0,
                    #[cfg(feature = "debug_state")]
                    kind: IterKind::Undefined
                },
                GroupTrackIterState {
                    field_pos: 2,
                    group_idx: 0,
                    group_offset: 1,
                    iter_id: 0,
                    #[cfg(feature = "debug_state")]
                    kind: IterKind::Undefined
                }
            ]
        );
    }

    #[test]
    fn dup_after_drop_does_not_affect_iterator() {
        let mut gl = GroupTrack {
            passed_fields_count: 0,
            group_lengths: SizeClassedVecDeque::Sc8(VecDeque::from([3])),
            iter_states: vec![Cell::new(GroupTrackIterState {
                field_pos: 2,
                group_idx: 0,
                group_offset: 2,
                iter_id: 0,
                #[cfg(feature = "debug_state")]
                kind: IterKind::Undefined,
            })],
            iter_lookup_table: Universe::from([0].into_iter()),
            ..Default::default()
        };

        gl.apply_field_actions_list(&[
            FieldAction::new(FieldActionKind::Drop, 0, 1),
            FieldAction::new(FieldActionKind::Dup, 1, 2),
        ]);

        assert_eq!(
            gl.iter_states[0].get(),
            GroupTrackIterState {
                field_pos: 1,
                group_idx: 0,
                group_offset: 1,
                iter_id: 0,
                #[cfg(feature = "debug_state")]
                kind: IterKind::Undefined
            }
        );
    }
}
