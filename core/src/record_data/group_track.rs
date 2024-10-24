use std::{
    cell::{Cell, Ref, RefCell, RefMut},
    cmp::Ordering,
    fmt::Display,
    ops::{Deref, DerefMut, Range},
};

use crate::{
    index_newtype,
    utils::{
        debuggable_nonmax::DebuggableNonMaxU32, indexing_type::IndexingType,
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
    pub first_right_leaning_actor_id: ActorId,
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

    // Number of parent group indices to advance to get to the parent of this
    // group, relative to the previous one. This is necessary
    // to make sense of zero length groups, where we would otherwise lose
    // this connection, since we can't find the right partner by lockstep
    // iterating over both group lists by length anymore.
    // This is 0 for the very first group in each track.
    pub parent_group_advancement: SizeClassedVecDeque,

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
            "{} + {:?} ({:?})",
            self.passed_fields_count,
            self.group_lengths,
            self.parent_group_advancement
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
        loop {
            let field_pos_delta = field_idx - self.field_pos;
            if field_pos_delta == 0 && self.group_len_rem != 0 {
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
        if !self.inside_passed_elems && self.iter_group_idx != self.group_idx {
            self.advance_affected_iters_to_group();
        }
        self.advance_affected_iters_to_group_offset();
    }
    fn apply_modifications(&mut self) {
        if !self.modified {
            return;
        }
        self.modified = false;
        if self.inside_passed_elems {
            self.gl.passed_fields_count = self.group_len;
            return;
        }
        self.gl.group_lengths.set(self.group_idx, self.group_len);
    }
    fn next_group(&mut self) {
        self.phase_out_current_iters();
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
    fn apply_action(&mut self, a: FieldAction) {
        let mut action_run_len = a.run_len as usize;
        self.move_to_field_pos(a.field_idx);
        match a.kind {
            FieldActionKind::Dup | FieldActionKind::InsertZst { .. } => {
                self.group_len_rem += action_run_len;
                self.group_len += action_run_len;
                self.curr_iters_field_pos_delta += action_run_len as isize;
                self.modified = true;
            }
            FieldActionKind::Drop => {
                let mut group_offset = self.group_len - self.group_len_rem;
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
                        self.skip_same_pos_iters_for_drop(
                            self.group_len_rem,
                            group_offset,
                        );
                        self.group_len = group_offset;
                        self.curr_iters_field_pos_delta -=
                            self.group_len_rem as isize;
                        action_run_len -= self.group_len_rem;
                        self.modified = true;
                        // this resets group len rem so we don't have to change
                        // it
                        self.next_group();
                        self.advance_affected_iters_to_group();
                        group_offset = 0;
                    }
                }
                self.skip_same_pos_iters_for_drop(
                    action_run_len,
                    group_offset,
                );
                self.curr_iters_field_pos_delta -= action_run_len as isize;
                self.group_len -= action_run_len;
                self.group_len_rem -= action_run_len;
                self.modified = true;
            }
        }
    }

    fn skip_same_pos_iters_for_drop(
        &mut self,
        action_run_len_rem: usize,
        group_offset: usize,
    ) {
        let field_pos_unmodified = (self.field_pos as isize
            - self.curr_iters_field_pos_delta)
            as usize;
        while self.affected_iters_start < self.affected_iters_end {
            let is = self.gl.iter_states[self.affected_iters_start as usize]
                .get_mut();
            if is.field_pos - field_pos_unmodified > action_run_len_rem {
                break;
            }
            is.field_pos = self.field_pos;
            is.group_offset = group_offset;
            self.affected_iters_start += 1;
        }
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
            let group_idx = iter_state.group_idx;
            if group_idx > self.group_idx {
                self.affected_iters_end = self.affected_iters_start;
                return;
            }
            self.apply_future_iter_offset(self.affected_iters_start);
            if group_idx == self.group_idx {
                break;
            }
            self.affected_iters_start += 1;
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
            self.apply_future_iter_offset(self.affected_iters_end);
            self.affected_iters_end += 1
        }
    }

    fn apply_curr_iter_offset(&mut self, iter_idx: GroupTrackIterSortedIndex) {
        let is = self.gl.iter_states[iter_idx as usize].get_mut();
        is.group_offset = (is.group_offset as isize
            + self.curr_iters_field_pos_delta)
            as usize;
        debug_assert!(
            is.field_pos as isize + self.curr_iters_field_pos_delta >= 0
        );
        is.field_pos =
            (is.field_pos as isize + self.curr_iters_field_pos_delta) as usize;
    }

    fn apply_future_iter_offset(
        &mut self,
        iter_idx: GroupTrackIterSortedIndex,
    ) {
        let is = self.gl.iter_states[iter_idx as usize].get_mut();
        is.field_pos = (is.field_pos as isize
            + self.future_iters_field_pos_delta)
            as usize;
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
        for i in self.affected_iters_end
            ..self.gl.iter_states.len() as GroupTrackIterSortedIndex
        {
            self.apply_future_iter_offset(i);
        }
        self.affected_iters_end =
            self.gl.iter_states.len() as GroupTrackIterSortedIndex;
        self.affected_iters_start = self.affected_iters_end;
    }
}

impl<'a> Drop for RecordGroupActionsApplicator<'a> {
    fn drop(&mut self) {
        self.phase_out_current_iters();
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
            field_pos: self.passed_fields_count,
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
    pub fn push_group(
        &mut self,
        group_len: usize,
        parent_group_advanceent: usize,
    ) {
        self.group_lengths.push_back(group_len);
        self.parent_group_advancement
            .push_back(parent_group_advanceent);
    }
    pub fn get_group_len(&self, group_index: GroupIdx) -> Option<usize> {
        self.group_lengths.try_get(group_index)
    }
    fn apply_field_actions_list(
        &mut self,
        action_list: impl IntoIterator<Item = FieldAction>,
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
        if self.iter_lookup_table.indices().next().is_none() {
            eprint!("[]");
            return;
        }
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
        let snapshot_new = ab.get_latest_snapshot();

        if snapshot_new == snapshot_prev {
            return;
        };

        self.snapshot = snapshot_new;
        ab.bump_snapshot_refcount(snapshot_new);

        let agi = ab.build_actions_from_snapshot(actor_id, snapshot_prev);

        if let Some(agi) = &agi {
            let (s1, s2) = ab.get_action_group_slices(agi);

            let actions = s1.iter().chain(s2.iter()).copied();

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
            ab.release_temp_action_group(agi);
        };
        ab.drop_snapshot_refcount(snapshot_prev);
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
        #[cfg_attr(
            not(feature = "debug_logging_field_actions"),
            allow(unused)
        )]
        end_of_input: bool,
    ) {
        let total_field_count =
            lgts.full_group_field_count + lgts.partial_group_len.unwrap_or(0);
        #[cfg(feature = "debug_logging_field_actions")]
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
        self.group_lengths.drop_front(lgts.full_group_count);
        let parent_starts_dropped = self
            .parent_group_advancement
            .iter()
            .take(lgts.full_group_count)
            .sum();
        self.parent_group_advancement
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
            if it.field_pos < self.passed_fields_count {
                it.field_pos = self.passed_fields_count;
            }
            match it.group_idx.cmp(&lgts.full_group_count) {
                Ordering::Less => {
                    it.group_idx = 0;
                    it.group_offset = 0;
                }
                Ordering::Equal => {
                    it.group_idx = 0;
                    it.group_offset = it
                        .group_offset
                        .saturating_sub(lgts.partial_group_len.unwrap_or(0))
                }
                Ordering::Greater => it.group_idx -= lgts.full_group_count,
            }
        }
        #[cfg(feature = "debug_logging_field_actions")]
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
        alias
            .parent_group_advancement
            .promote_to_size_class(self.parent_group_advancement.size_class());

        if alias.group_lengths.is_empty() {
            alias.group_lengths.extend_truncated(
                self.group_lengths.iter().take(lgts.full_group_count),
            );
            alias.parent_group_advancement.extend(
                self.parent_group_advancement
                    .iter()
                    .take(lgts.full_group_count),
            );
            if let Some(v) = lgts.partial_group_len {
                alias.group_lengths.push_back_truncated(v);
                alias.parent_group_advancement.push_back_truncated(
                    self.parent_group_advancement.get(lgts.full_group_count),
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
                alias.parent_group_advancement.extend(
                    self.parent_group_advancement
                        .iter()
                        .skip(1)
                        .take(full_group_count),
                );
                if let Some(v) = lgts.partial_group_len {
                    alias.group_lengths.push_back_truncated(v);
                    alias.parent_group_advancement.push_back_truncated(
                        self.parent_group_advancement
                            .get(full_group_count + 1),
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
        group_track: T,
        iter_state: GroupTrackIterState,
    ) -> GroupTrackIter<T> {
        GroupTrackIter {
            field_pos: iter_state.field_pos,
            group_idx: iter_state.group_idx,
            group_len_rem: group_track
                .group_lengths
                .try_get(iter_state.group_idx)
                .unwrap_or(0)
                - iter_state.group_offset,
            group_track,
        }
    }
    pub fn lookup_iter_for_deref<T: Deref<Target = Self>>(
        group_track: T,
        iter_id: GroupTrackIterId,
    ) -> GroupTrackIter<T> {
        let iter_index = group_track.iter_lookup_table[iter_id];
        let iter_state = group_track.iter_states[iter_index as usize].get();
        Self::build_iter_from_iter_state(group_track, iter_state)
    }
    pub fn lookup_iter_for_deref_mut<T: DerefMut<Target = Self>>(
        mut group_track: T,
        iter_id: GroupTrackIterId,
        msm: &MatchSetManager,
        actor_id: ActorId,
    ) -> GroupTrackIterMut<T> {
        group_track.deref_mut().sort_iters();
        let action_buffer = &msm.match_sets[group_track.ms_id].action_buffer;
        let iter_index = group_track.iter_lookup_table[iter_id];
        let iter_state = group_track.iter_states[iter_index as usize].get();
        let base = Self::build_iter_from_iter_state(group_track, iter_state);
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
        let prev = self.iter_states[iter_sorting_idx].get();

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
            first_right_leaning_actor_id: prev.first_right_leaning_actor_id,
            #[cfg(feature = "debug_state")]
            kind: prev.kind,
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
        group_index: GroupIdx,
        field_pos_start: usize,
    ) -> Range<GroupTrackIterSortedIndex> {
        let mut start = self
            .iter_states
            .binary_search_by_key(&(group_index, field_pos_start), |is| {
                let is = is.get();
                (is.group_idx, is.group_offset)
            })
            .unwrap_or_else(|insert_point| insert_point);
        let mut end = if start == self.iter_states.len() {
            start
        } else {
            start + 1
        };
        while start < end
            && self.iter_states[start].get().group_idx < group_index
        {
            start += 1;
        }
        while start < end
            && self.iter_states[end - 1].get().group_idx > group_index
        {
            end -= 1;
        }
        loop {
            if start == 0 {
                break;
            }
            let is = self.iter_states[start - 1].get();
            if is.group_idx < group_index {
                break;
            }
            if is.group_idx == group_index && is.field_pos < field_pos_start {
                break;
            }
            start -= 1;
        }
        while end < self.iter_states.len()
            && self.iter_states[end].get().group_idx == group_index
        {
            end += 1;
        }
        start as u32..end as u32
    }

    pub fn claim_iter(
        &mut self,
        first_right_leaning_actor_id: ActorId,
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
            first_right_leaning_actor_id,
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
        field_pos_start: usize,
        count: isize,
    ) {
        for i in iters.clone() {
            let is = self.iter_states[i as usize].get_mut();
            if count >= 0 {
                let count = count as usize;
                is.group_offset += count;
                is.field_pos += count;
            } else {
                let count = (-count) as usize;
                // HACK: refactor this. we need the plus one here
                // because we start our affected iter range at +1
                // to not adjust the iters sitting on the same field pos
                let delta = is.field_pos - field_pos_start + 1;
                let offs = if delta < count { delta } else { count };
                is.group_offset -= offs;
                is.field_pos -= offs;
            }
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
        group_index: GroupIdx,
        field_pos_start: usize,
        count: isize,
    ) {
        self.advance_affected_iters(
            self.lookup_iter_sort_key_range(group_index, field_pos_start),
            field_pos_start,
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

pub fn merge_leading_groups_into_parent_raw(
    parent_prev_gt: &GroupTrack,
    child_gt: &mut GroupTrack,
    parent_new_gt: &mut GroupTrack,
    field_count: usize,
    end_of_input: bool,
) {
    let mut processed_field_count = 0;

    let mut parent_group_id_stable = child_gt.parent_group_index_offset;
    let mut prev_parent_group_idx =
        parent_prev_gt.group_idx_from_stable_idx(parent_group_id_stable);

    parent_new_gt
        .parent_group_advancement
        .promote_to_size_class(
            parent_prev_gt.parent_group_advancement.size_class(),
        );

    let mut processed_child_group_count = 0;

    let mut first_subgroup_found = false;

    let mut subgroup_finished = false;

    let mut first_parent_advancement = 0;

    let mut child_groups_sum = 0;

    let mut final_group_len = 0;

    let mut end_reached = field_count == 0;
    loop {
        if subgroup_finished || end_reached {
            let group_idx_present = !parent_new_gt.group_lengths.is_empty()
                && parent_group_id_stable
                    == parent_new_gt.last_group_idx_stable();

            if group_idx_present && first_parent_advancement == 0 {
                let last_index = parent_new_gt.group_lengths.len() - 1;
                parent_new_gt
                    .group_lengths
                    .add_value(last_index, child_groups_sum);
            } else {
                let zero_inserts = (first_parent_advancement
                    + usize::from(!group_idx_present))
                .saturating_sub(1);
                if zero_inserts > 0 {
                    parent_new_gt.group_lengths.extend_truncated(
                        std::iter::repeat(0).take(zero_inserts),
                    );
                    parent_new_gt.parent_group_advancement.extend_truncated(
                        parent_prev_gt
                            .parent_group_advancement
                            .iter()
                            .skip(
                                prev_parent_group_idx
                                    + usize::from(group_idx_present),
                            )
                            .take(zero_inserts),
                    );
                }

                parent_new_gt.group_lengths.push_back(child_groups_sum);
                parent_new_gt.parent_group_advancement.push_back(
                    parent_prev_gt
                        .parent_group_advancement
                        .get(prev_parent_group_idx + first_parent_advancement),
                );
            }
            first_subgroup_found = false;

            if first_parent_advancement != 0 {
                prev_parent_group_idx += first_parent_advancement;
                parent_group_id_stable = parent_group_id_stable
                    .wrapping_add(first_parent_advancement);
            }

            child_groups_sum = 0;
        }

        if end_reached {
            break;
        }

        let parent_advancement = child_gt
            .parent_group_advancement
            .get(processed_child_group_count);

        if !first_subgroup_found {
            first_subgroup_found = true;
            first_parent_advancement = parent_advancement;
        } else if parent_advancement != 0 {
            subgroup_finished = true;
            continue;
        }

        let mut group_len =
            child_gt.group_lengths.get(processed_child_group_count);

        end_reached = processed_field_count + group_len >= field_count;

        if end_reached {
            group_len = field_count - processed_field_count;
            final_group_len = group_len;
        }

        processed_field_count += group_len;
        processed_child_group_count += 1;
        child_groups_sum += group_len;
    }

    let lgts = if end_of_input {
        LeadingGroupTrackSlice {
            full_group_count: processed_child_group_count,
            full_group_field_count: field_count,
            partial_group_len: None,
        }
    } else {
        LeadingGroupTrackSlice {
            full_group_count: processed_child_group_count.saturating_sub(1),
            full_group_field_count: field_count - final_group_len,
            partial_group_len: Some(final_group_len),
        }
    };
    child_gt.drop_leading_groups(true, lgts, end_of_input);
}

impl GroupTrackManager {
    pub fn add_group_track(
        &mut self,
        ms: &MatchSetManager,
        parent_track: Option<GroupTrackId>,
        ms_id: MatchSetId,
        actor: ActorRef,
    ) -> GroupTrackId {
        let id = self.group_tracks.peek_claim_id();
        let mut ab = ms.match_sets[ms_id].action_buffer.borrow_mut();
        let snapshot = ab.get_latest_snapshot();
        ab.bump_snapshot_refcount(snapshot);
        self.group_tracks.claim_with_value(RefCell::new(GroupTrack {
            id,
            ms_id,
            actor_ref: actor,
            parent_group_track_id: parent_track,
            group_index_offset: GroupIdxStable::ZERO,
            parent_group_index_offset: GroupIdxStable::ZERO,
            passed_fields_count: 0,
            group_lengths: SizeClassedVecDeque::default(),
            parent_group_advancement: SizeClassedVecDeque::new(),
            iter_states: Vec::default(),
            iter_lookup_table: Universe::default(),
            snapshot,
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
        first_right_leaning_actor_id: ActorId,
        kind: IterKind,
    ) -> GroupTrackIterId {
        self.group_tracks[list_id]
            .borrow_mut()
            .claim_iter(first_right_leaning_actor_id, kind)
    }
    pub fn claim_group_track_iter_ref(
        &mut self,
        list_id: GroupTrackId,
        first_right_leaning_actor_id: ActorId,
        kind: IterKind,
    ) -> GroupTrackIterRef {
        GroupTrackIterRef {
            track_id: list_id,
            iter_id: self.claim_group_track_iter(
                list_id,
                first_right_leaning_actor_id,
                kind,
            ),
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
    ) -> GroupTrackIterMut<'a, RefMut<'a, GroupTrack>> {
        let mut list = self.borrow_group_track_mut(track_id);
        list.apply_field_actions(msm);
        GroupTrack::lookup_iter_for_deref_mut(list, iter_id, msm, actor_id)
    }
    pub fn lookup_group_track_iter_mut_from_ref<'a>(
        &'a self,
        iter_ref: GroupTrackIterRef,
        msm: &'a MatchSetManager,
        actor_id: ActorId,
    ) -> GroupTrackIterMut<'a, RefMut<'a, GroupTrack>> {
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

    pub fn apply_actions_to_track(
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
        parent_group_advancement: usize,
    ) {
        let mut gl = self.group_tracks[group_track_id].borrow_mut();
        debug_assert!(gl.parent_group_track_id.is_none());
        gl.group_lengths.push_back(field_count);
        gl.parent_group_advancement
            .push_back(parent_group_advancement);
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

        merge_leading_groups_into_parent_raw(
            &parent_prev_gt,
            &mut child_gt,
            &mut parent_new_gt,
            field_count,
            end_of_input,
        )
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
        self.field_pos += self.group_len_rem;
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

    pub fn is_invalid(&self) -> bool {
        self.group_idx >= self.group_track.group_lengths.len()
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
            self.base.group_idx,
            self.base.field_pos,
            count as isize,
        );

        let field_pos_prev = self.base.field_pos;
        self.update_group_len = true;
        self.group_len += count;
        self.base.field_pos += count;
        let mut ab = self.action_buffer.borrow_mut();
        let actor_id = ab.pending_action_group_id().unwrap();
        ab.push_action(
            FieldActionKind::InsertZst { repr, actor_id },
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
            self.base.group_idx,
            self.base.field_pos + 1,
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
            self.base.group_idx,
            field_pos + 1,
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
            self.base.group_idx,
            field_pos + 1,
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
            self.base.group_idx,
            self.base.field_pos + 1,
            -(count as isize),
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
            self.base.group_idx,
            self.base.field_pos + 1,
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
                self.base.group_idx,
                self.base.field_pos - pos_delta + 1,
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
                self.base.group_idx,
                field_pos + 1,
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

    pub fn parent_group_advancement(&self) -> usize {
        self.base
            .group_track
            .parent_group_advancement
            .get(self.base.group_idx)
    }

    pub fn is_invalid(&self) -> bool {
        self.base.is_invalid()
    }
}

impl<'a, T: DerefMut<Target = GroupTrack>> Drop for GroupTrackIterMut<'a, T> {
    fn drop(&mut self) {
        self.update_group();

        let mut ab = self.action_buffer.borrow_mut();
        ab.end_action_group();

        let list = &mut *self.base.group_track;

        if ab
            .initialize_actor_ref(
                Some(ActorSubscriber::GroupTrack(list.id)),
                &mut list.actor_ref,
            )
            .is_none()
        {
            return;
        };

        let latest_snapshot = ab.get_latest_snapshot();
        if latest_snapshot == list.snapshot {
            return;
        }

        ab.drop_snapshot_refcount(list.snapshot);
        ab.bump_snapshot_refcount(latest_snapshot);
        list.snapshot = latest_snapshot;
    }
}

#[cfg(test)]
pub(crate) mod testing_helpers {

    use crate::record_data::action_buffer::ActorId;

    use super::{GroupIdx, GroupTrackIterId, GroupTrackIterState};

    #[derive(Clone, Copy)]
    pub struct GroupTrackIterStateRaw {
        pub field_pos: usize,
        pub group_idx: GroupIdx,
        pub group_offset: GroupIdx,
        pub iter_id: GroupTrackIterId,
        pub first_right_leaning_actor_id: ActorId,
    }

    impl GroupTrackIterStateRaw {
        pub fn into_iter_state(self) -> GroupTrackIterState {
            GroupTrackIterState {
                field_pos: self.field_pos,
                group_idx: self.group_idx,
                group_offset: self.group_offset,
                iter_id: self.iter_id,
                first_right_leaning_actor_id: self
                    .first_right_leaning_actor_id,
                #[cfg(feature = "debug_state")]
                kind: crate::record_data::iter_hall::IterKind::Undefined,
            }
        }
    }
}

#[cfg(test)]
mod test_action_lists_through_iter {
    use std::cell::Cell;

    use crate::{
        record_data::{
            action_buffer::ActorId,
            field::FieldManager,
            group_track::{GroupTrack, GroupTrackIterSortedIndex},
            match_set::MatchSetManager,
            scope_manager::{ScopeId, ScopeManager},
        },
        utils::{
            indexing_type::IndexingType,
            size_classed_vec_deque::SizeClassedVecDeque, universe::Universe,
        },
    };

    use super::{
        testing_helpers::GroupTrackIterStateRaw, GroupTrackIterId,
        GroupTrackIterMut,
    };

    // first iter is used to apply actions
    #[track_caller]
    fn test_group_track_iter_mut(
        passed_fields_before: usize,
        group_lengths_before: impl IntoIterator<
            IntoIter = impl Iterator<Item = usize> + Clone,
        >,
        iter_states_before: impl IntoIterator<
            IntoIter = impl Iterator<Item = GroupTrackIterStateRaw> + Clone,
        >,
        passed_fields_after: usize,
        group_lengths_after: impl IntoIterator<
            IntoIter = impl Iterator<Item = usize> + Clone,
        >,
        iter_states_after: impl IntoIterator<Item = GroupTrackIterStateRaw>,
        group_track_iter_actions_fn: impl Fn(
            &mut GroupTrackIterMut<&mut GroupTrack>,
        ),
    ) {
        let group_lengths_before = group_lengths_before.into_iter();
        let group_lengths_after = group_lengths_after.into_iter();
        let iter_states_before = iter_states_before.into_iter();

        let mut msm = MatchSetManager::default();
        let mut fm = FieldManager::default();
        let mut sm = ScopeManager::default();
        msm.add_match_set(&mut fm, &mut sm, ScopeId::ZERO);

        let mut gt = GroupTrack {
            passed_fields_count: passed_fields_before,
            parent_group_advancement: SizeClassedVecDeque::from_iter(
                group_lengths_before.clone(),
            ),
            group_lengths: SizeClassedVecDeque::from_iter(
                group_lengths_before,
            ),
            iter_states: iter_states_before
                .clone()
                .map(|i| Cell::new(i.into_iter_state()))
                .collect::<Vec<_>>(),
            iter_lookup_table: Universe::from(
                0..iter_states_before.count() as GroupTrackIterSortedIndex,
            ),
            ..Default::default()
        };

        let mut iter = GroupTrack::lookup_iter_for_deref_mut(
            &mut gt,
            GroupTrackIterId::ZERO,
            &msm,
            ActorId::ZERO,
        );

        group_track_iter_actions_fn(&mut iter);

        iter.store_iter(GroupTrackIterId::ZERO);

        assert_eq!(gt.passed_fields_count, passed_fields_after);

        assert_eq!(
            gt.group_lengths,
            SizeClassedVecDeque::from_iter(group_lengths_after)
        );
        assert_eq!(
            gt.iter_states,
            iter_states_after
                .into_iter()
                .map(|i| Cell::new(i.into_iter_state()))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_drop_adjust_truncates_correctly_at_end() {
        test_group_track_iter_mut(
            1,
            [5],
            [
                GroupTrackIterStateRaw {
                    field_pos: 1,
                    group_idx: 0,
                    group_offset: 0,
                    iter_id: 0,
                    first_right_leaning_actor_id: ActorId::ZERO,
                },
                GroupTrackIterStateRaw {
                    field_pos: 4,
                    group_idx: 0,
                    group_offset: 3,
                    iter_id: 1,
                    first_right_leaning_actor_id: ActorId::ZERO,
                },
            ],
            1,
            [3],
            [
                GroupTrackIterStateRaw {
                    field_pos: 3,
                    group_idx: 0,
                    group_offset: 2,
                    iter_id: 0,
                    first_right_leaning_actor_id: ActorId::ZERO,
                },
                GroupTrackIterStateRaw {
                    field_pos: 3,
                    group_idx: 0,
                    group_offset: 2,
                    iter_id: 1,
                    first_right_leaning_actor_id: ActorId::ZERO,
                },
            ],
            |iter| {
                iter.next_n_fields(2);
                iter.drop(2);
            },
        )
    }
}

#[cfg(test)]
mod test_action_lists {
    use crate::{
        record_data::{
            action_buffer::ActorId,
            field_action::{FieldAction, FieldActionKind},
            group_track::{GroupTrack, GroupTrackIterSortedIndex},
        },
        utils::{
            indexing_type::IndexingType,
            size_classed_vec_deque::SizeClassedVecDeque, universe::Universe,
        },
    };
    use std::cell::Cell;

    use super::testing_helpers::GroupTrackIterStateRaw;

    #[track_caller]
    fn test_apply_field_actions(
        passed_fields_before: usize,
        group_lengths_before: impl IntoIterator<
            IntoIter = impl Iterator<Item = usize> + Clone,
        >,
        iter_states_before: impl IntoIterator<
            IntoIter = impl Iterator<Item = GroupTrackIterStateRaw> + Clone,
        >,
        field_actions: impl IntoIterator<Item = FieldAction>,
        passed_fields_after: usize,
        group_lengths_after: impl IntoIterator<
            IntoIter = impl Iterator<Item = usize> + Clone,
        >,
        iter_states_after: impl IntoIterator<Item = GroupTrackIterStateRaw>,
    ) {
        let group_lengths_before = group_lengths_before.into_iter();
        let group_lengths_after = group_lengths_after.into_iter();
        let iter_states_before = iter_states_before.into_iter();

        let mut gl = GroupTrack {
            passed_fields_count: passed_fields_before,
            parent_group_advancement: SizeClassedVecDeque::from_iter(
                group_lengths_before.clone(),
            ),
            group_lengths: SizeClassedVecDeque::from_iter(
                group_lengths_before,
            ),
            iter_states: iter_states_before
                .clone()
                .map(|i| Cell::new(i.into_iter_state()))
                .collect::<Vec<_>>(),
            iter_lookup_table: Universe::from(
                0..iter_states_before.count() as GroupTrackIterSortedIndex,
            ),
            ..Default::default()
        };

        gl.apply_field_actions_list(field_actions);

        assert_eq!(gl.passed_fields_count, passed_fields_after);

        assert_eq!(
            gl.group_lengths,
            SizeClassedVecDeque::from_iter(group_lengths_after)
        );
        assert_eq!(
            gl.iter_states,
            iter_states_after
                .into_iter()
                .map(|i| Cell::new(i.into_iter_state()))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn drop_on_passed_fields() {
        test_apply_field_actions(
            2,
            [],
            [],
            [FieldAction::new(FieldActionKind::Drop, 1, 1)],
            1,
            [],
            [],
        );
    }

    #[test]
    fn drop_in_passed_affects_iterator_correctly() {
        test_apply_field_actions(
            2,
            [2],
            [GroupTrackIterStateRaw {
                field_pos: 3,
                group_idx: 0,
                group_offset: 1,
                iter_id: 0,
                first_right_leaning_actor_id: ActorId::ZERO,
            }],
            [FieldAction::new(FieldActionKind::Drop, 0, 1)],
            1,
            [2],
            [GroupTrackIterStateRaw {
                field_pos: 2,
                group_idx: 0,
                group_offset: 1,
                iter_id: 0,
                first_right_leaning_actor_id: ActorId::ZERO,
            }],
        );
    }

    #[test]
    fn drop_in_group_affects_iterator_correctly() {
        test_apply_field_actions(
            1,
            [3],
            [
                GroupTrackIterStateRaw {
                    field_pos: 2,
                    group_idx: 0,
                    group_offset: 1,
                    iter_id: 0,
                    first_right_leaning_actor_id: ActorId::ZERO,
                },
                GroupTrackIterStateRaw {
                    field_pos: 3,
                    group_idx: 0,
                    group_offset: 2,
                    iter_id: 1,
                    first_right_leaning_actor_id: ActorId::ZERO,
                },
            ],
            [FieldAction::new(FieldActionKind::Drop, 2, 1)],
            1,
            [2],
            [
                GroupTrackIterStateRaw {
                    field_pos: 2,
                    group_idx: 0,
                    group_offset: 1,
                    iter_id: 0,
                    first_right_leaning_actor_id: ActorId::ZERO,
                },
                GroupTrackIterStateRaw {
                    field_pos: 2,
                    group_idx: 0,
                    group_offset: 1,
                    iter_id: 1,
                    first_right_leaning_actor_id: ActorId::ZERO,
                },
            ],
        );
    }

    #[test]
    fn dup_after_drop_does_not_affect_iterator() {
        test_apply_field_actions(
            0,
            [3],
            [GroupTrackIterStateRaw {
                field_pos: 2,
                group_idx: 0,
                group_offset: 2,
                iter_id: 0,
                first_right_leaning_actor_id: ActorId::ZERO,
            }],
            [
                FieldAction::new(FieldActionKind::Drop, 0, 1),
                FieldAction::new(FieldActionKind::Dup, 1, 2),
            ],
            0,
            [4],
            [GroupTrackIterStateRaw {
                field_pos: 1,
                group_idx: 0,
                group_offset: 1,
                iter_id: 0,
                first_right_leaning_actor_id: ActorId::ZERO,
            }],
        );
    }

    #[test]
    fn test_empty_group_skip_and_iter_adjustment() {
        // reduced from integration::exec::run_exec_into_join
        test_apply_field_actions(
            1,
            [0, 1, 1],
            [GroupTrackIterStateRaw {
                field_pos: 1,
                group_idx: 1,
                group_offset: 0,
                iter_id: 0,
                first_right_leaning_actor_id: ActorId::ZERO,
            }],
            [
                FieldAction::new(FieldActionKind::Drop, 0, 1),
                FieldAction::new(FieldActionKind::Dup, 0, 4),
                FieldAction::new(FieldActionKind::Dup, 5, 4),
            ],
            0,
            [0, 5, 5], // TODO: is this correct?
            [GroupTrackIterStateRaw {
                field_pos: 0,
                group_idx: 1,
                group_offset: 0,
                iter_id: 0,
                first_right_leaning_actor_id: ActorId::ZERO,
            }],
        );
    }

    #[test]
    fn test_iter_adjustment_on_dup() {
        // reduced from scr_ext_csv/integration::imdb_actor_count
        test_apply_field_actions(
            1,
            [1, 4, 3],
            [GroupTrackIterStateRaw {
                field_pos: 2,
                group_idx: 1,
                group_offset: 0,
                iter_id: 0,
                first_right_leaning_actor_id: ActorId::MAX_VALUE,
            }],
            [
                FieldAction::new(FieldActionKind::Drop, 1, 1),
                FieldAction::new(FieldActionKind::Drop, 2, 3),
                FieldAction::new(FieldActionKind::Drop, 3, 1),
            ],
            1,
            [0, 1, 2],
            [GroupTrackIterStateRaw {
                field_pos: 1,
                group_idx: 1,
                group_offset: 0,
                iter_id: 0,
                first_right_leaning_actor_id: ActorId::MAX_VALUE,
            }],
        );
    }
    #[test]
    fn test_iter_field_pos_adjustment_on_drop_spanning_passed() {
        test_apply_field_actions(
            5,
            [1, 1, 1, 1, 1],
            [GroupTrackIterStateRaw {
                field_pos: 5,
                group_idx: 0,
                group_offset: 0,
                iter_id: 0,
                first_right_leaning_actor_id: ActorId::MAX_VALUE,
            }],
            [
                FieldAction::new(FieldActionKind::Drop, 1, 6),
                FieldAction::new(FieldActionKind::Dup, 3, 1),
            ],
            1,
            [0, 0, 1, 1, 2],
            [GroupTrackIterStateRaw {
                field_pos: 1,
                group_idx: 0,
                group_offset: 0,
                iter_id: 0,
                first_right_leaning_actor_id: ActorId::MAX_VALUE,
            }],
        );
    }
}

#[cfg(test)]
mod test_merge {
    use std::iter;

    use crate::{
        record_data::group_track::{
            merge_leading_groups_into_parent_raw, GroupTrack,
        },
        utils::size_classed_vec_deque::SizeClassedVecDeque,
    };

    #[track_caller]
    fn test_merge_leading_groups(
        prev_parent_advancements: impl IntoIterator<
            IntoIter = impl Iterator<Item = usize> + Clone,
        >,
        child_parent_advancements: impl IntoIterator<
            IntoIter = impl Iterator<Item = usize> + Clone,
        >,
        child_groups: impl IntoIterator<
            IntoIter = impl Iterator<Item = usize> + Clone,
        >,
        new_parent_advancements: impl IntoIterator<
            IntoIter = impl Iterator<Item = usize> + Clone,
        >,
        new_parent_groups: impl IntoIterator<
            IntoIter = impl Iterator<Item = usize> + Clone,
        >,
        new_parent_advancements_result: impl IntoIterator<
            IntoIter = impl Iterator<Item = usize> + Clone,
        >,
        new_parent_groups_result: impl IntoIterator<
            IntoIter = impl Iterator<Item = usize> + Clone,
        >,
        field_count: usize,
        end_of_input: bool,
    ) {
        let prev_parent_advancements =
            SizeClassedVecDeque::from_iter(prev_parent_advancements);
        let prev_parent = GroupTrack {
            // this never matters
            group_lengths: SizeClassedVecDeque::from_iter(
                iter::repeat(0).take(prev_parent_advancements.len()),
            ),
            parent_group_advancement: prev_parent_advancements,
            ..Default::default()
        };
        let mut child = GroupTrack {
            group_lengths: SizeClassedVecDeque::from_iter(child_groups),
            parent_group_advancement: SizeClassedVecDeque::from_iter(
                child_parent_advancements,
            ),
            ..Default::default()
        };
        let mut new_parent = GroupTrack {
            group_lengths: SizeClassedVecDeque::from_iter(new_parent_groups),
            parent_group_advancement: SizeClassedVecDeque::from_iter(
                new_parent_advancements,
            ),
            ..Default::default()
        };
        merge_leading_groups_into_parent_raw(
            &prev_parent,
            &mut child,
            &mut new_parent,
            field_count,
            end_of_input,
        );
        let group_lengths_res =
            SizeClassedVecDeque::from_iter(new_parent_groups_result);
        let parent_group_advancement_res =
            SizeClassedVecDeque::from_iter(new_parent_advancements_result);

        assert_eq!(new_parent.group_lengths, group_lengths_res);
        assert_eq!(
            new_parent.parent_group_advancement,
            parent_group_advancement_res
        );
    }

    #[test]
    fn test_simple_merge() {
        test_merge_leading_groups(
            [42],
            [0, 0, 0],
            [1, 1, 1],
            [],
            [],
            [42],
            [3],
            3,
            true,
        );
    }

    #[test]
    fn test_simple_skip() {
        test_merge_leading_groups(
            [1, 22, 333],
            [0, 2],
            [1, 2],
            [],
            [],
            [1, 22, 333],
            [1, 0, 2],
            3,
            true,
        );
    }

    // reduced from integration::foreach::foreach_empty_group_skip
    #[test]
    fn test_multi_skip() {
        test_merge_leading_groups(
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            [5, 1, 1, 0, 1, 1],
            [1, 1, 1, 1, 1, 1],
            [],
            [],
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            [0, 0, 0, 0, 0, 1, 1, 2, 1, 1],
            6,
            true,
        );
    }
}
