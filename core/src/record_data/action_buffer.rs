use std::{cell::Cell, fmt::Debug, mem::size_of};

use crate::utils::{
    dynamic_freelist::DynamicArrayFreelist, launder_slice,
    offset_vec_deque::OffsetVecDeque, subslice_slice_pair,
};

#[cfg(feature = "debug_logging")]
use super::match_set::MatchSetId;

use super::{
    field::{FieldId, FieldManager},
    field_action::{merge_action_lists, FieldAction, FieldActionKind},
    field_action_applicator::FieldActionApplicator,
    field_value_repr::RunLength,
    iter_hall::FieldDataSource,
};
pub type ActorId = u32;
pub type ActionGroupId = u32;
pub type ActionId = usize;

pub type SnapshotLookupId = u32;

#[derive(Clone, Copy)]
pub enum ActorRef {
    Present(ActorId),
    // when a transform adds an actor, it sets the first_actor of it's
    // output fields to Unconfirmed()
    Unconfirmed(ActorId),
}

#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub struct SnapshotRef {
    snapshot_len: SnapshotEntry,
    freelist_id: SnapshotLookupId,
}

type SnapshotEntry = u32;
const SNAPSHOT_REFCOUNT_OFFSET: usize = 0;
const SNAPSHOT_ACTOR_COUNT_OFFSET: usize = 1;
const SNAPSHOT_LEN_MIN: usize = 1;
const SNAPSHOT_PREFIX_LEN: usize = 2;
// snapshots are a list of `SnapshotEntry`s where
// - the first one is the refcount,
// - the second one is the number of actors present at snapshot creation
// - all subsequent ones are the current action group id of the actors in the
//   order that would be returned by Pow2LookupStepsIter::new( <`ActorId`
//   containing the snapshot>, <number of actors present at snapshot creation>
//   )
const_assert!(
    size_of::<SnapshotEntry>() == size_of::<ActorId>()
        && size_of::<SnapshotEntry>() == size_of::<ActionGroupId>() //
);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ActionGroup {
    start: ActionId,
    length: ActionId,
    field_count_delta: isize,
}

struct ActionGroupWithRefs {
    group: ActionGroup,
    // initialized as usize::MAX, wrapping add, saturating sub
    #[allow(unused)] // TODO
    refcount: u32,
    next_action_group_id_self: ActionGroupId,
    next_action_group_id_succ: ActionGroupId,
}

#[allow(unused)] // TODO
struct ActionGroupMerges {
    action_groups: OffsetVecDeque<ActionGroupId, ActionGroup>,
    actions: OffsetVecDeque<ActionId, FieldAction>,
}

#[derive(Default)]
struct ActionGroupQueue {
    action_groups: OffsetVecDeque<ActionGroupId, ActionGroupWithRefs>,
    actions: OffsetVecDeque<ActionId, FieldAction>,
    dirty: bool,
    #[allow(unused)] // TODO
    refcount: u32,
}

struct Actor {
    action_group_queues: Vec<ActionGroupQueue>,
    merges: Vec<ActionGroupMerges>,
    subscribers: Vec<FieldId>,
    latest_snapshot: SnapshotRef,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ActionGroupIdentifier {
    group: ActionGroup,
    location: ActionGroupLocation,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ActionGroupLocation {
    Regular {
        actor_id: u32,
        pow2: u8,
    },
    #[allow(unused)] // TODO
    LocalMerge {
        actor_id: u32,
        merge_pow2: u8,
    },
    TempBuffer {
        idx: usize,
    },
}

impl Default for ActorRef {
    fn default() -> Self {
        ActorRef::Unconfirmed(0)
    }
}

impl Debug for ActorRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Present(id) => f.write_fmt(format_args!("{id}")),
            Self::Unconfirmed(id) => f.write_fmt(format_args!("{id}?")),
        }
    }
}

impl ActionGroupIdentifier {
    fn temp_idx(&self) -> Option<usize> {
        match self.location {
            ActionGroupLocation::Regular { .. }
            | ActionGroupLocation::LocalMerge { .. } => None,
            ActionGroupLocation::TempBuffer { idx, .. } => Some(idx),
        }
    }
}

#[derive(Default)]
pub struct ActionBuffer {
    actors: OffsetVecDeque<ActorId, Actor>,
    // we need 3 temp buffers in order to always have a free one as a target
    // when merging from two others
    action_temp_buffers: [Vec<FieldAction>; 3],
    pending_action_group_actor_id: Option<ActorId>,
    pending_action_group_action_count: usize,
    pending_action_group_field_count_delta: isize,
    snapshot_freelists:
        Vec<DynamicArrayFreelist<SnapshotLookupId, SnapshotEntry>>,
    actions_applicator: FieldActionApplicator,
    #[cfg(feature = "debug_logging")]
    pub match_set_id: MatchSetId,
}

type Pow2Index = u32;

#[derive(Clone)]
struct Pow2LookupStepsIter {
    value: Pow2Index,
    end: Pow2Index,
}

struct Pow2InsertStepsIter {
    value: Pow2Index,
    begin: Pow2Index,
    end: Pow2Index,
    bit: u8,
    bit_count: u8,
}

impl Pow2LookupStepsIter {
    pub fn new(start: Pow2Index, end: Pow2Index) -> Self {
        Self { value: start, end }
    }
}

impl Iterator for Pow2LookupStepsIter {
    type Item = (Pow2Index, u8);

    fn next(&mut self) -> Option<(Pow2Index, u8)> {
        if self.value >= self.end {
            return None;
        }
        if self.value == 0 {
            self.value = self.end;
            return Some((0, self.end.next_power_of_two().ilog2() as u8));
        }
        let val = self.value;
        let trailing_zeroes = self.value.trailing_zeros();
        let new_value = self.value + (1 << trailing_zeroes);
        if new_value > self.end {
            let bits = (self.end - val)
                .next_power_of_two()
                .ilog2()
                .saturating_sub(1);
            self.value += 1 << bits;
            return Some((val, bits as u8));
        }
        self.value = new_value;
        Some((val, trailing_zeroes as u8))
    }
}

impl Pow2InsertStepsIter {
    pub fn new(index: Pow2Index, begin: Pow2Index, end: Pow2Index) -> Self {
        Self {
            value: index,
            begin,
            end,
            bit: 0,
            bit_count: end.next_power_of_two().ilog2() as u8 + 1,
        }
    }
}

impl Iterator for Pow2InsertStepsIter {
    type Item = (Pow2Index, u8);

    fn next(&mut self) -> Option<(Pow2Index, u8)> {
        if self.value < self.begin || self.bit == self.bit_count {
            return None;
        }
        let val = self.value;
        let bit = self.bit;
        self.bit += 1;
        let shift = 1 << bit;
        if self.value >= shift {
            self.value -= self.value & shift;
        } else {
            self.value = 0;
        }
        if val + (shift >> 1) >= self.end {
            return self.next();
        }
        Some((val, bit))
    }
}
impl Default for Actor {
    fn default() -> Self {
        Self {
            action_group_queues: vec![ActionGroupQueue::default()],
            merges: Vec::new(),
            subscribers: Vec::new(),
            latest_snapshot: SnapshotRef::default(),
        }
    }
}

pub fn eprint_action_list<'a>(actions: impl Iterator<Item = &'a FieldAction>) {
    let mut idx_delta = 0isize;
    for a in actions {
        eprintln!(
            "   > {:?} (src_idx: {})",
            a,
            idx_delta + isize::try_from(a.field_idx).unwrap()
        );
        match a.kind {
            FieldActionKind::Dup | FieldActionKind::InsertZst(_) => {
                idx_delta -= isize::try_from(a.run_len).unwrap()
            }
            FieldActionKind::Drop => {
                idx_delta += isize::try_from(a.run_len).unwrap()
            }
        }
    }
}

impl ActionBuffer {
    pub const MAX_ACTOR_ID: ActorId = ActorId::MAX;
    pub fn begin_action_group(&mut self, actor_id: u32) {
        assert!(self.pending_action_group_actor_id.is_none());
        self.pending_action_group_actor_id = Some(actor_id)
    }
    pub fn end_action_group(&mut self) {
        let ai = self.pending_action_group_actor_id.take().unwrap();
        let action_count = self.pending_action_group_action_count;
        let field_count_delta = self.pending_action_group_field_count_delta;
        if action_count == 0 {
            return;
        }
        self.pending_action_group_action_count = 0;
        self.pending_action_group_field_count_delta = 0;
        let mut agq = &mut self.actors[ai].action_group_queues[0];
        let actions_start =
            agq.actions.next_free_index().wrapping_sub(action_count);
        #[cfg(feature = "debug_logging")]
        {
            eprintln!(
                "ms {}, actor {}: added action group {}:",
                self.match_set_id,
                ai,
                agq.action_groups.next_free_index()
            );
            eprint_action_list(agq.actions.data.range(actions_start..));
        }
        let next_action_group_id_succ = if ai == self.actors.max_index() {
            0
        } else {
            let next_agq = &self.actors[ai].action_group_queues[0];
            next_agq.action_groups.next_free_index() as ActionGroupId
        };
        agq = &mut self.actors[ai].action_group_queues[0];
        agq.action_groups.data.push_back(ActionGroupWithRefs {
            group: ActionGroup {
                start: actions_start,
                length: action_count,
                field_count_delta,
            },
            refcount: 0,
            next_action_group_id_self: agq.action_groups.next_free_index()
                as ActionGroupId,
            next_action_group_id_succ,
        });
        for (i, pow2) in Pow2InsertStepsIter::new(
            ai,
            self.actors.offset,
            self.actors.next_free_index(),
        )
        .skip(1)
        {
            let actor = &mut self.actors[i];
            let agq = &mut actor.action_group_queues[pow2 as usize];
            if agq.dirty {
                break;
            }
            agq.dirty = true;
        }
    }
    fn apply_action_field_count_delta(
        &mut self,
        kind: FieldActionKind,
        run_length: usize,
    ) {
        let rl_delta = isize::try_from(run_length).unwrap();
        match kind {
            FieldActionKind::Dup | FieldActionKind::InsertZst(_) => {
                // TODO: on 32 bit, consider checked adds?
                self.pending_action_group_field_count_delta += rl_delta
            }
            FieldActionKind::Drop => {
                self.pending_action_group_field_count_delta -= rl_delta
            }
        }
    }
    pub fn push_action(
        &mut self,
        kind: FieldActionKind,
        field_idx: usize,
        mut run_length: usize,
    ) {
        self.apply_action_field_count_delta(kind, run_length);
        let actor_id = self.pending_action_group_actor_id.unwrap();
        let actions =
            &mut self.actors[actor_id].action_group_queues[0].actions;
        if self.pending_action_group_action_count > 0 {
            let last = actions.data.back_mut().unwrap();
            // field indices in action groups must be ascending
            debug_assert!(last.field_idx <= field_idx);
            // very simple early merging of actions to hopefully save some
            // memory this also allows operations to be slightly
            // more 'wasteful' with their action pushes
            if last.kind == kind && last.field_idx == field_idx {
                let run_len_rem = (RunLength::MAX - last.run_len) as usize;
                if run_len_rem >= run_length {
                    last.run_len += run_length as RunLength;
                    return;
                }
                last.run_len = RunLength::MAX;
                run_length -= run_len_rem;
            }
        }
        while run_length > 0 {
            let rl_to_push =
                run_length.min(RunLength::MAX as usize) as RunLength;
            actions.data.push_back(FieldAction {
                kind,
                field_idx,
                run_len: rl_to_push,
            });
            self.pending_action_group_action_count += 1;
            run_length -= rl_to_push as usize;
        }
    }
    pub fn add_actor(&mut self) -> ActorId {
        let actor_id = self.actors.next_free_index();
        if actor_id != self.actors.offset {
            let pow2_to_add = actor_id.trailing_zeros() + 1;
            let tgt_actor_id = actor_id - (1 << (pow2_to_add - 1));
            let tgt_actor = &mut self.actors[tgt_actor_id];
            debug_assert_eq!(
                tgt_actor.action_group_queues.len(),
                pow2_to_add as usize
            );
            tgt_actor.action_group_queues.push(ActionGroupQueue {
                dirty: true,
                ..Default::default()
            })
        }
        self.actors.data.push_back(Actor::default());
        actor_id
    }
    pub fn last_valid_actor_id(&self) -> ActorId {
        self.actors.max_index()
    }
    pub fn peek_next_actor_id(&self) -> ActorId {
        self.actors.next_free_index()
    }
    fn release_temp_action_group(
        &mut self,
        agi: Option<ActionGroupIdentifier>,
    ) {
        let Some(ag) = agi else { return };
        let ActionGroupLocation::TempBuffer { idx } = ag.location else {
            return;
        };
        let tb = &mut self.action_temp_buffers[idx];
        debug_assert!(tb.len() == ag.group.start + ag.group.length);
        tb.truncate(tb.len() - ag.group.length);
    }
    fn get_free_temp_idx(
        lhs: &ActionGroupIdentifier,
        rhs: &ActionGroupIdentifier,
    ) -> usize {
        let lhs_idx = lhs.temp_idx().unwrap_or(3);
        let rhs_idx = rhs.temp_idx().unwrap_or(3);
        (6 - lhs_idx - rhs_idx) % 3
    }
    fn get_action_group_slices(
        &self,
        agi: &ActionGroupIdentifier,
    ) -> (&[FieldAction], &[FieldAction]) {
        match agi.location {
            ActionGroupLocation::Regular { actor_id, pow2 } => {
                let actor = &self.actors[actor_id];
                let agq = &actor.action_group_queues[pow2 as usize];
                let (s1, s2) = agq.actions.data.as_slices();
                let start = agi.group.start.wrapping_sub(agq.actions.offset);
                subslice_slice_pair(s1, s2, start..start + agi.group.length)
            }
            ActionGroupLocation::LocalMerge {
                actor_id,
                merge_pow2,
            } => {
                let actor = &self.actors[actor_id];
                let merge_lvl = &actor.merges[merge_pow2 as usize];
                let start =
                    agi.group.start.wrapping_sub(merge_lvl.actions.offset);
                let (s1, s2) = merge_lvl.actions.data.as_slices();
                subslice_slice_pair(s1, s2, start..start + agi.group.length)
            }
            ActionGroupLocation::TempBuffer { idx } => (
                &[],
                &self.action_temp_buffers[idx]
                    [agi.group.start..agi.group.start + agi.group.length],
            ),
        }
    }
    fn action_group_not_from_actor_pow2(
        actor_id: ActorId,
        pow2: u8,
        agi: &ActionGroupIdentifier,
    ) -> bool {
        match agi.location {
            ActionGroupLocation::Regular {
                actor_id: ag_actor_id,
                pow2: ag_pow2,
                ..
            } => ag_actor_id != actor_id || ag_pow2 != pow2,
            ActionGroupLocation::LocalMerge { .. }
            | ActionGroupLocation::TempBuffer { .. } => true,
        }
    }
    fn append_action_group(
        &mut self,
        actor_id: ActorId,
        pow2: u8,
        next_self: ActionGroupId,
        next_succ: ActionGroupId,
        ag: &ActionGroupIdentifier,
    ) -> ActionGroupIdentifier {
        let (s1, s2) = self.get_action_group_slices(ag);
        assert!(Self::action_group_not_from_actor_pow2(actor_id, pow2, ag));
        let start = unsafe {
            // SAFETY: the assert above ensures that s1 and s2 do not point
            // into the VecDeque that we are apppending to
            // and therefore won't get invalidated by doing so
            let s1 = launder_slice(s1);
            let s2 = launder_slice(s2);
            let actor = &mut self.actors[actor_id];
            let agq = &mut actor.action_group_queues[pow2 as usize];
            let start = agq.actions.next_free_index();
            agq.actions.data.extend(s1);
            agq.actions.data.extend(s2);
            start
        };
        let actor = &mut self.actors[actor_id];
        let agq = &mut actor.action_group_queues[pow2 as usize];
        agq.action_groups.data.push_back(ActionGroupWithRefs {
            group: ActionGroup { start, ..ag.group },
            refcount: 0,
            next_action_group_id_self: next_self,
            next_action_group_id_succ: next_succ,
        });
        ActionGroupIdentifier {
            location: ActionGroupLocation::Regular { actor_id, pow2 },
            group: ActionGroup { start, ..ag.group },
        }
    }
    fn merge_action_groups_into_actor_action_group(
        &mut self,
        lhs: Option<&ActionGroupIdentifier>,
        rhs: Option<&ActionGroupIdentifier>,
        actor_id: ActorId,
        pow2: u8,
        next_self: ActionGroupId,
        next_succ: ActionGroupId,
    ) -> Option<ActionGroupIdentifier> {
        if let Some(lhs) = lhs {
            if let Some(rhs) = rhs {
                let (l1, l2) = self.get_action_group_slices(lhs);
                let (r1, r2) = self.get_action_group_slices(rhs);
                assert!(Self::action_group_not_from_actor_pow2(
                    actor_id, pow2, lhs
                ));
                assert!(Self::action_group_not_from_actor_pow2(
                    actor_id, pow2, rhs
                ));
                let len_before = unsafe {
                    // SAFETY: the asserts above ensure that lhs and rhs do
                    // not come from the VecDeque that we are apppending to,
                    // and therefore won't get invalidated by doing so
                    let (l1, l2) = (launder_slice(l1), launder_slice(l2));
                    let (r1, r2) = (launder_slice(r1), launder_slice(r2));
                    let actor = &mut self.actors[actor_id];
                    let agq = &mut actor.action_group_queues[pow2 as usize];
                    let len_before = agq.actions.data.len();
                    merge_action_lists(
                        l1.iter().chain(l2),
                        r1.iter().chain(r2),
                        &mut agq.actions.data,
                    );
                    len_before
                };
                let actor = &mut self.actors[actor_id];
                let agq = &mut actor.action_group_queues[pow2 as usize];
                let len_after = agq.actions.data.len();
                let start = len_before;
                let length = len_after - len_before;
                let field_count_delta =
                    lhs.group.field_count_delta + rhs.group.field_count_delta;
                let ag = ActionGroup {
                    start,
                    length,
                    field_count_delta,
                };
                agq.action_groups.data.push_back(ActionGroupWithRefs {
                    group: ag,
                    refcount: 0,
                    next_action_group_id_self: next_self,
                    next_action_group_id_succ: next_succ,
                });
                return Some(ActionGroupIdentifier {
                    group: ag,
                    location: ActionGroupLocation::Regular { actor_id, pow2 },
                });
            }
            return Some(self.append_action_group(
                actor_id, pow2, next_self, next_succ, lhs,
            ));
        }
        if let Some(rhs) = rhs {
            return Some(self.append_action_group(
                actor_id, pow2, next_self, next_succ, rhs,
            ));
        }
        None
    }
    fn merge_action_groups_into_temp_buffer(
        &mut self,
        lhs: Option<&ActionGroupIdentifier>,
        rhs: Option<&ActionGroupIdentifier>,
    ) -> Option<ActionGroupIdentifier> {
        if let Some(lhs) = lhs {
            if let Some(rhs) = rhs {
                let (l1, l2) = self.get_action_group_slices(lhs);
                let (r1, r2) = self.get_action_group_slices(rhs);
                let idx = Self::get_free_temp_idx(lhs, rhs);
                let (start, length) = unsafe {
                    // SAFETY: get_free_temp_idx makes sure that
                    // action_temp_buffers[idx] is neither
                    // used by lhs nor by rhs. Therefore we can take
                    // a mutable reference to it
                    let (l1, l2) = (launder_slice(l1), launder_slice(l2));
                    let (r1, r2) = (launder_slice(r1), launder_slice(r2));
                    let buff = &mut self.action_temp_buffers[idx];
                    let start = buff.len();
                    merge_action_lists(
                        l1.iter().chain(l2),
                        r1.iter().chain(r2),
                        buff,
                    );
                    (start, buff.len() - start)
                };
                return Some(ActionGroupIdentifier {
                    group: ActionGroup {
                        start,
                        length,
                        field_count_delta: lhs.group.field_count_delta
                            + rhs.group.field_count_delta,
                    },
                    location: ActionGroupLocation::TempBuffer { idx },
                });
            }
            return Some(lhs.clone());
        }
        rhs.cloned()
    }
    fn merge_action_groups_into_temp_buffer_release_inputs(
        &mut self,
        lhs: Option<ActionGroupIdentifier>,
        rhs: Option<ActionGroupIdentifier>,
    ) -> Option<ActionGroupIdentifier> {
        let res = self
            .merge_action_groups_into_temp_buffer(lhs.as_ref(), rhs.as_ref());
        if res != lhs {
            self.release_temp_action_group(lhs);
        }
        if res != rhs {
            self.release_temp_action_group(rhs);
        }
        res
    }
    fn merge_action_groups_of_single_pow2(
        &mut self,
        actor_id: ActorId,
        pow2: u8,
        prev: ActionGroupId,
        new: ActionGroupId,
    ) -> Option<ActionGroupIdentifier> {
        // TODO: use local merges if this is the hightest pow2
        // TODO: do O(log n) merges instead of O(n) here
        let mut curr_ag_id = prev;
        let mut res = None;
        loop {
            if curr_ag_id == new {
                return res;
            }
            let actor = &mut self.actors[actor_id];
            let agq = &mut actor.action_group_queues[pow2 as usize];
            let ag = &mut agq.action_groups[curr_ag_id];

            let curr = Some(ActionGroupIdentifier {
                location: ActionGroupLocation::Regular { actor_id, pow2 },
                group: ag.group,
            });
            res = self.merge_action_groups_into_temp_buffer_release_inputs(
                res, curr,
            );
            curr_ag_id += 1;
        }
    }
    fn refresh_action_group(
        &mut self,
        actor_id: ActorId,
        pow2: u8,
    ) -> ActionGroupId {
        let mut actor = &mut self.actors[actor_id];
        let mut agq = &mut actor.action_group_queues[pow2 as usize];
        if !agq.dirty {
            return agq.action_groups.next_free_index() as ActionGroupId;
        }
        let (self_prev, succ_prev) = agq
            .action_groups
            .data
            .back()
            .map(|ag| {
                (ag.next_action_group_id_self, ag.next_action_group_id_succ)
            })
            .unwrap_or((0, 0));
        debug_assert!(pow2 > 0);
        let children_pow2 = pow2 - 1;
        let self_next = self.refresh_action_group(actor_id, children_pow2);
        let self_merge = self.merge_action_groups_of_single_pow2(
            actor_id,
            children_pow2,
            self_prev,
            self_next,
        );
        let succ_id = actor_id + (1 << children_pow2);
        let succ_pow2 = children_pow2
            .min(self.actors[succ_id].action_group_queues.len() as u8 - 1);
        let succ_next = self.refresh_action_group(succ_id, succ_pow2);
        let succ_merge = self.merge_action_groups_of_single_pow2(
            succ_id, succ_pow2, succ_prev, succ_next,
        );

        self.merge_action_groups_into_actor_action_group(
            self_merge.as_ref(),
            succ_merge.as_ref(),
            actor_id,
            pow2,
            self_next,
            succ_next,
        );
        self.release_temp_action_group(self_merge);
        self.release_temp_action_group(succ_merge);
        actor = &mut self.actors[actor_id];
        agq = &mut actor.action_group_queues[pow2 as usize];
        agq.dirty = false;
        agq.action_groups.next_free_index() as ActionGroupId
    }

    fn generate_snapshot(
        &mut self,
        actor_id: ActorId,
        refcount: SnapshotEntry,
    ) -> SnapshotRef {
        let next_actor_index = self.actors.next_free_index();
        // PERF: find a O(1) way to calculate this
        let iter = Pow2LookupStepsIter::new(actor_id, next_actor_index);
        let snapshot_len = iter.clone().count();
        while snapshot_len >= self.snapshot_freelists.len() + SNAPSHOT_LEN_MIN
        {
            self.snapshot_freelists.push(DynamicArrayFreelist::new(
                self.snapshot_freelists.len()
                    + SNAPSHOT_LEN_MIN
                    + SNAPSHOT_PREFIX_LEN,
            ))
        }
        let (freelist_id, ss) =
            self.snapshot_freelists[snapshot_len - SNAPSHOT_LEN_MIN].claim();
        ss[SNAPSHOT_REFCOUNT_OFFSET] = refcount;
        ss[SNAPSHOT_ACTOR_COUNT_OFFSET] = next_actor_index as SnapshotEntry;
        for (i, (actor_id, pow2)) in iter.enumerate() {
            let actor = &self.actors[actor_id];
            let agq = &actor.action_group_queues[pow2 as usize];
            ss[i + SNAPSHOT_PREFIX_LEN] =
                agq.action_groups.next_free_index() as SnapshotEntry;
        }
        SnapshotRef {
            snapshot_len: snapshot_len as SnapshotLookupId,
            freelist_id,
        }
    }
    fn validate_snapshot_and_refresh_action_groups(
        &mut self,
        actor_id: ActorId,
        snapshot: SnapshotRef,
    ) -> bool {
        let mut snapshot_valid = true;
        let snapshot_len = snapshot.snapshot_len as usize;
        let actor_count = self.actors.next_free_index();
        if snapshot_len < SNAPSHOT_LEN_MIN {
            snapshot_valid = false;
        } else {
            let ss = &self.snapshot_freelists
                [snapshot.snapshot_len as usize - SNAPSHOT_LEN_MIN]
                [snapshot.freelist_id];
            let ss_actor_count = ss[SNAPSHOT_ACTOR_COUNT_OFFSET];
            if ss_actor_count != actor_count {
                snapshot_valid = false;
            }
        }
        for (i, (actor_id, pow2)) in
            Pow2LookupStepsIter::new(actor_id, actor_count).enumerate()
        {
            let next_ag = self.refresh_action_group(actor_id, pow2);
            if snapshot_valid && i < snapshot_len {
                let ss = &self.snapshot_freelists
                    [snapshot_len - SNAPSHOT_LEN_MIN][snapshot.freelist_id];
                snapshot_valid &= ss[SNAPSHOT_PREFIX_LEN + i] == next_ag;
            }
        }
        snapshot_valid
    }
    fn get_snapshot_actor_count(
        &self,
        snapshot: SnapshotRef,
    ) -> SnapshotEntry {
        if snapshot.snapshot_len < SNAPSHOT_LEN_MIN as SnapshotEntry {
            return 0;
        }
        self.snapshot_freelists
            [snapshot.snapshot_len as usize - SNAPSHOT_LEN_MIN]
            [snapshot.freelist_id][SNAPSHOT_ACTOR_COUNT_OFFSET]
    }
    fn bump_snapshot_refcount(&mut self, snapshot: SnapshotRef, bump: u32) {
        debug_assert!(
            snapshot.snapshot_len >= SNAPSHOT_LEN_MIN as SnapshotEntry
        );
        let ss_rc = &mut self.snapshot_freelists
            [snapshot.snapshot_len as usize - SNAPSHOT_LEN_MIN]
            [snapshot.freelist_id][SNAPSHOT_REFCOUNT_OFFSET];
        *ss_rc += bump;
    }
    fn drop_snapshot_refcount(&mut self, snapshot: SnapshotRef, drop: u32) {
        if snapshot.snapshot_len < SNAPSHOT_LEN_MIN as SnapshotEntry {
            return;
        }
        let ss_rc = &mut self.snapshot_freelists
            [snapshot.snapshot_len as usize - SNAPSHOT_LEN_MIN]
            [snapshot.freelist_id][SNAPSHOT_REFCOUNT_OFFSET];
        *ss_rc -= drop;
        if *ss_rc == 0 {
            self.snapshot_freelists
                [snapshot.snapshot_len as usize - SNAPSHOT_LEN_MIN]
                .release(snapshot.freelist_id);
        }
    }
    fn apply_from_snapshot_with_same_actor_count(
        &mut self,
        actor_id: ActorId,
        ssr: SnapshotRef,
    ) -> Option<ActionGroupIdentifier> {
        let mut res = None;
        for (i, (ai, pow2)) in
            Pow2LookupStepsIter::new(actor_id, self.actors.next_free_index())
                .enumerate()
        {
            let ss = &self.snapshot_freelists
                [ssr.snapshot_len as usize - SNAPSHOT_LEN_MIN]
                [ssr.freelist_id];
            let prev = ss[SNAPSHOT_PREFIX_LEN + i];
            let next = self.actors[ai].action_group_queues[pow2 as usize]
                .action_groups
                .next_free_index();
            let next_ag =
                self.merge_action_groups_of_single_pow2(ai, pow2, prev, next);
            res = self.merge_action_groups_into_temp_buffer_release_inputs(
                res, next_ag,
            );
        }
        res
    }
    fn apply_from_snapshot_with_different_actor_count(
        &mut self,
        actor_id: ActorId,
        ssr: SnapshotRef,
    ) -> Option<ActionGroupIdentifier> {
        let prev_ss_actor_iter = Pow2LookupStepsIter::new(
            actor_id,
            self.get_snapshot_actor_count(ssr),
        );
        let mut iter =
            Pow2LookupStepsIter::new(actor_id, self.actors.next_free_index());
        let mut i = 0;
        let mut res = None;
        for (ai, pow2) in prev_ss_actor_iter {
            let prev = self.snapshot_freelists
                [ssr.snapshot_len as usize - SNAPSHOT_LEN_MIN]
                [ssr.freelist_id][SNAPSHOT_PREFIX_LEN + i];
            let (n_ai, n_pow2) = iter.next().unwrap();
            debug_assert!(n_ai == ai && n_pow2 >= pow2);
            if prev == 0 || n_pow2 == pow2 {
                let next = self.actors[ai].action_group_queues
                    [n_pow2 as usize]
                    .action_groups
                    .next_free_index();
                let next_ag = self.merge_action_groups_of_single_pow2(
                    n_ai, n_pow2, prev, next,
                );
                res = self
                    .merge_action_groups_into_temp_buffer_release_inputs(
                        res, next_ag,
                    );
                continue;
            }
            let mut self_prev = prev;
            let mut succ_prev = 0;
            for i_pow2 in pow2 + 1..=n_pow2 {
                let children_pow2 = i_pow2 - 1;
                let ag = &self.actors[ai].action_group_queues[i_pow2 as usize]
                    .action_groups
                    .data
                    .front()
                    .unwrap();
                let (self_min, succ_min) = (
                    ag.next_action_group_id_self,
                    ag.next_action_group_id_succ,
                );
                let self_ag = self.merge_action_groups_of_single_pow2(
                    ai,
                    children_pow2,
                    self_prev,
                    self_min,
                );
                self_prev = 1;
                let succ_ag = self.merge_action_groups_of_single_pow2(
                    ai + (1 << children_pow2),
                    i_pow2 - 1,
                    succ_prev,
                    succ_min,
                );
                succ_prev = 1;
                let combined_ag = self
                    .merge_action_groups_into_temp_buffer_release_inputs(
                        self_ag, succ_ag,
                    );
                res = self
                    .merge_action_groups_into_temp_buffer_release_inputs(
                        res,
                        combined_ag,
                    );
            }
            let next = self.actors[ai].action_group_queues[n_pow2 as usize]
                .action_groups
                .next_free_index();
            let self_ag =
                self.merge_action_groups_of_single_pow2(ai, n_pow2, 1, next);
            res = self.merge_action_groups_into_temp_buffer_release_inputs(
                res, self_ag,
            );
            i += 1;
        }
        for (ai, pow2) in iter {
            let next = self.actors[ai].action_group_queues[pow2 as usize]
                .action_groups
                .next_free_index();
            let next_ag =
                self.merge_action_groups_of_single_pow2(ai, pow2, 0, next);
            res = self.merge_action_groups_into_temp_buffer_release_inputs(
                res, next_ag,
            );
        }
        res
    }
    fn update_actor_snapshot(&mut self, actor_id: ActorId) -> SnapshotRef {
        let actor_ss = self.actors[actor_id].latest_snapshot;
        if self.validate_snapshot_and_refresh_action_groups(actor_id, actor_ss)
        {
            return actor_ss;
        }
        let new_ss = self.generate_snapshot(actor_id, 1);
        // we generate the new one first before dropping the reference to
        // the old one to make sure that when we later compare refs to the two
        // they are not the same
        self.drop_snapshot_refcount(actor_ss, 1);
        self.actors[actor_id].latest_snapshot = new_ss;
        new_ss
    }
    pub fn execute(&mut self, fm: &FieldManager, field_id: FieldId) {
        let mut field_ref_mut = fm.fields[field_id].borrow_mut();
        let field = &mut *field_ref_mut;
        let Some(actor_id) =
            self.initialize_first_actor(field_id, &mut field.first_actor)
        else {
            return;
        };
        let field_ss = field.snapshot;
        let actor_ss = self.update_actor_snapshot(actor_id);
        if actor_ss == field_ss {
            return;
        }
        let field_ss_actor_count = self.get_snapshot_actor_count(field_ss);
        let agi = if field_ss_actor_count == self.actors.next_free_index() {
            self.apply_from_snapshot_with_same_actor_count(actor_id, field_ss)
        } else {
            self.apply_from_snapshot_with_different_actor_count(
                actor_id, field_ss,
            )
        };
        self.drop_snapshot_refcount(field_ss, 1);
        field.snapshot = actor_ss;
        self.bump_snapshot_refcount(actor_ss, 1);
        let Some(agi) = agi else { return };

        field.iter_hall.uncow_headers(fm);

        drop(field_ref_mut);
        // uncow headers reborrows the field unfortunately.
        // PERF: inline `uncow_headers` to avoid that
        let field_ref = fm.fields[field_id].borrow();
        for &f in &field_ref.iter_hall.cow_targets {
            fm.fields[f].borrow_mut().iter_hall.uncow_headers(fm);
        }
        drop(field_ref);
        let mut field_ref_mut = fm.fields[field_id].borrow_mut();
        let field = &mut *field_ref_mut;

        let fd = &mut field.iter_hall.field_data;
        let (headers, data, field_count) =
            match &mut field.iter_hall.data_source {
                FieldDataSource::Owned => {
                    (&mut fd.headers, Some(&mut fd.data), &mut fd.field_count)
                }
                FieldDataSource::DataCow { .. }
                | FieldDataSource::RecordBufferDataCow(_) => {
                    (&mut fd.headers, None, &mut fd.field_count)
                }
                FieldDataSource::Alias(_) => {
                    panic!("cannot execute commands on Alias iter hall")
                }
                FieldDataSource::Cow(_)
                | FieldDataSource::RecordBufferCow(_) => {
                    panic!("cannot execute commands on COW iter hall")
                }
            };
        let iterators = field.iter_hall.iters.iter_mut().map(Cell::get_mut);
        let start = agi.group.start;
        let length = agi.group.length;
        let actions = match agi.location {
            ActionGroupLocation::Regular { actor_id, pow2 } => {
                let actions = &self.actors[actor_id].action_group_queues
                    [pow2 as usize]
                    .actions;
                let start = start.wrapping_sub(actions.offset);
                let (s1, s2) = actions.data.as_slices();
                let (s1, s2) =
                    subslice_slice_pair(s1, s2, start..start + length);
                s1.iter().chain(s2)
            }
            ActionGroupLocation::LocalMerge {
                actor_id,
                merge_pow2,
            } => {
                let actions = &self.actors[actor_id].action_group_queues
                    [merge_pow2 as usize]
                    .actions;
                let start = start.wrapping_sub(actions.offset);
                let (s1, s2) = actions.data.as_slices();
                let (s1, s2) =
                    subslice_slice_pair(s1, s2, start..start + length);
                s1.iter().chain(s2)
            }
            ActionGroupLocation::TempBuffer { idx } => self
                .action_temp_buffers[idx][start..start + length]
                .iter()
                .chain(&[]),
        };
        #[cfg(feature = "debug_logging")]
        {
            eprintln!(
                "executing for field {} (ms {}, first actor: {}):",
                field_id, field.match_set, actor_id
            );
            eprint_action_list(actions.clone());
        }
        let field_count_delta = self.actions_applicator.run(
            actions,
            headers,
            data,
            field_count,
            iterators,
        );
        debug_assert!(field_count_delta == agi.group.field_count_delta);
    }
    fn initialize_first_actor(
        &mut self,
        field_id: FieldId,
        first_actor: &mut ActorRef,
    ) -> Option<ActorId> {
        match *first_actor {
            ActorRef::Present(actor) => Some(actor),
            ActorRef::Unconfirmed(actor) => {
                if self.actors.next_free_index() > actor {
                    *first_actor = ActorRef::Present(actor);
                    self.actors[actor].subscribers.push(field_id);
                    Some(actor)
                } else {
                    None
                }
            }
        }
    }
    pub fn drop_field_commands(
        &mut self,
        field_id: FieldId,
        first_actor: &mut ActorRef,
        snapshot: &mut SnapshotRef,
    ) {
        if self.actors.data.is_empty() {
            return;
        }
        let Some(actor_id) =
            self.initialize_first_actor(field_id, first_actor)
        else {
            return;
        };
        let ss = self.update_actor_snapshot(actor_id);
        if ss == *snapshot {
            return;
        }
        #[cfg(feature = "debug_logging")]
        {
            eprintln!(
                "dropping actions for field {} (ms: {}, first actor: {})",
                field_id, self.match_set_id, actor_id,
            );
        }
        self.drop_snapshot_refcount(*snapshot, 1);
        self.bump_snapshot_refcount(ss, 1);
        *snapshot = ss;
    }
}

#[cfg(test)]
mod test {
    use crate::record_data::action_buffer::{
        Pow2InsertStepsIter, Pow2LookupStepsIter,
    };

    use super::Pow2Index;

    fn collect_lookup_steps(
        start: Pow2Index,
        end: Pow2Index,
    ) -> Vec<(Pow2Index, u8)> {
        Pow2LookupStepsIter::new(start, end).collect::<Vec<_>>()
    }

    #[test]
    fn test_pow2_lookup_steps_iter() {
        assert_eq!(collect_lookup_steps(0, 0), []);
        assert_eq!(collect_lookup_steps(0, 1), [(0, 0)]);
        assert_eq!(collect_lookup_steps(0, 2), [(0, 1)]);
        assert_eq!(collect_lookup_steps(0, 3), [(0, 2)]);
        assert_eq!(collect_lookup_steps(0, 4), [(0, 2)]);
        assert_eq!(collect_lookup_steps(0, 5), [(0, 3)]);
        assert_eq!(collect_lookup_steps(1, 0), []);
        assert_eq!(collect_lookup_steps(1, 1), []);
        assert_eq!(collect_lookup_steps(1, 2), [(1, 0)]);
        assert_eq!(collect_lookup_steps(1, 3), [(1, 0), (2, 0)]);
        assert_eq!(collect_lookup_steps(1, 4), [(1, 0), (2, 1)]);
        assert_eq!(collect_lookup_steps(2, 8), [(2, 1), (4, 2)]);
        assert_eq!(collect_lookup_steps(3, 7), [(3, 0), (4, 1), (6, 0)]);
    }

    fn collect_insert_steps(
        index: Pow2Index,
        begin: Pow2Index,
        end: Pow2Index,
    ) -> Vec<(Pow2Index, u8)> {
        Pow2InsertStepsIter::new(index, begin, end).collect::<Vec<_>>()
    }

    #[test]
    fn test_pow2_insert_steps_iter() {
        assert_eq!(collect_insert_steps(0, 0, 0), []);
        assert_eq!(collect_insert_steps(1, 0, 0), []);
        assert_eq!(collect_insert_steps(1, 0, 1), []);
        assert_eq!(collect_insert_steps(0, 0, 1), [(0, 0)]);
        assert_eq!(collect_insert_steps(0, 0, 3), [(0, 0), (0, 1), (0, 2)]);
        assert_eq!(collect_insert_steps(1, 0, 2), [(1, 0), (0, 1)]);
        assert_eq!(collect_insert_steps(2, 0, 3), [(2, 0), (0, 2)]);
        assert_eq!(
            collect_insert_steps(7, 0, 8),
            [(7, 0), (6, 1), (4, 2), (0, 3)]
        );
        assert_eq!(collect_insert_steps(8, 0, 9), [(8, 0), (0, 4)]);
        assert_eq!(
            collect_insert_steps(1, 0, 9),
            [(1, 0), (0, 1), (0, 2), (0, 3), (0, 4)],
        );
        assert_eq!(collect_insert_steps(2, 1, 5), [(2, 0), (2, 1)],);
    }
}
