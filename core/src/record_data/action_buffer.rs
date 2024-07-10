use std::{
    cell::{Cell, Ref},
    collections::VecDeque,
    fmt::{Debug, Write},
    mem::size_of,
};

use num::Integer;
use static_assertions::const_assert;

use crate::utils::{
    dynamic_freelist::DynamicArrayFreelist, launder_slice,
    offset_vec_deque::OffsetVecDeque, phantom_slot::PhantomSlot,
    subslice_slice_pair, temp_vec::transmute_vec,
};

use super::{
    field::{Field, FieldId, FieldManager},
    field_action::{merge_action_lists, FieldAction, FieldActionKind},
    field_action_applicator::FieldActionApplicator,
    field_data::{FieldValueHeader, RunLength, MAX_FIELD_ALIGN},
    group_track::GroupTrackId,
    iter_hall::{CowVariant, FieldDataSource, IterState},
    iters::FieldIterator,
    match_set::{MatchSetId, MatchSetManager},
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
    snapshot_id: SnapshotLookupId,
}

type SnapshotEntry = u32;
const SNAPSHOT_REFCOUNT_OFFSET: usize = 0;
// **not** the amount of actors in the snapshot, but the total amount of actors
// present in the action buffer at the time the snapshot was created
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

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ActorSubscriber {
    Field(FieldId),
    GroupTrack(GroupTrackId),
}

struct Actor {
    action_group_queues: Vec<ActionGroupQueue>,
    merges: Vec<ActionGroupMerges>,
    subscribers: Vec<ActorSubscriber>,
    latest_snapshot: SnapshotRef,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActionGroupIdentifier {
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
            Self::Present(id) => f.write_fmt(format_args!(" {id}")),
            Self::Unconfirmed(id) => f.write_fmt(format_args!("?{id}")),
        }
    }
}
impl ActorRef {
    pub fn confirmed_id(&self) -> Option<ActorId> {
        match self {
            ActorRef::Present(id) => Some(*id),
            ActorRef::Unconfirmed(_) => None,
        }
    }
    pub fn unwrap(&self) -> ActorId {
        self.confirmed_id().unwrap()
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

// used to adjust iterators based on the dropped headers
#[derive(Default)]
struct HeaderDropInfo {
    dead_headers_leading: usize,
    header_count_rem: usize,
    first_header_dropped_elem_count: RunLength,
    last_header_run_len: RunLength,
    last_header_data_pos: usize,
}

#[derive(Clone, Copy)]
struct DeadDataReport {
    dead_data_leading: usize,
    dead_data_trailing: usize,
}

#[derive(Clone, Copy)]
struct HeaderDropInstructions {
    // the new padding to *set* (not add) to the first header
    // alive after droppage
    first_header_padding: usize,
    leading_drop: usize,
    trailing_drop: usize,
}

struct DataCowFieldRef<'a> {
    #[allow(unused)]
    #[cfg(feature = "debug_state")]
    field_id: FieldId,
    field: Option<std::cell::RefMut<'a, Field>>,
    // For fields that have only partially copied over the data.
    // `dead_data_trailing` needs to take that into account
    data_end: usize,
    // required for the corresponding full cow fields
    // (`FullCowFieldRef::data_cow_idx`) that need this do adjust their
    // iterators
    drop_info: HeaderDropInfo,
}

struct FullCowFieldRef<'a> {
    #[allow(unused)]
    #[cfg(feature = "debug_state")]
    field_id: FieldId,
    field: Option<std::cell::RefMut<'a, Field>>,
    data_cow_idx: Option<usize>,
    // a full cow of a data cow. still relevant for advancing iterators
    // after dropping data, but not relevant for adjusting iterators during
    // action application
    through_data_cow: bool,
}

pub struct ActionBuffer {
    #[allow(unused)]
    #[cfg(feature = "debug_state")]
    pub(crate) match_set_id: MatchSetId,
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
    full_cow_field_refs_temp: Vec<PhantomSlot<FullCowFieldRef<'static>>>,
    data_cow_field_refs_temp: Vec<PhantomSlot<DataCowFieldRef<'static>>>,
    // used in drop_dead_headers to preserve alive zst headers
    // that live between dead data
    preserved_headers: Vec<FieldValueHeader>,
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

impl DeadDataReport {
    pub fn all_dead(field_data_size: usize) -> Self {
        DeadDataReport {
            dead_data_leading: field_data_size,
            dead_data_trailing: field_data_size,
        }
    }
}

pub fn eprint_action_list<'a>(actions: impl Iterator<Item = &'a FieldAction>) {
    let mut idx_delta = 0isize;
    for a in actions {
        eprintln!(
            "   / {:?} (src_idx: {})",
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

    pub fn new(
        #[cfg_attr(not(feature = "debug_state"), allow(unused))]
        ms_id: MatchSetId,
    ) -> Self {
        Self {
            #[cfg(feature = "debug_state")]
            match_set_id: ms_id,
            actors: OffsetVecDeque::default(),
            action_temp_buffers: Default::default(),
            pending_action_group_actor_id: None,
            pending_action_group_action_count: 0,
            pending_action_group_field_count_delta: 0,
            snapshot_freelists: Vec::new(),
            actions_applicator: FieldActionApplicator::default(),
            full_cow_field_refs_temp: Vec::new(),
            data_cow_field_refs_temp: Vec::new(),
            preserved_headers: Vec::new(),
        }
    }
    fn eprint_action_list_from_agi(&self, agi: &ActionGroupIdentifier) {
        let (s1, s2) = Self::get_action_group_slices_raw(
            &self.actors,
            &self.action_temp_buffers,
            agi,
        );
        eprint_action_list(s1.iter().chain(s2));
    }
    pub fn begin_action_group(&mut self, actor_id: u32) {
        assert!(self.pending_action_group_actor_id.is_none());
        self.pending_action_group_actor_id = Some(actor_id)
    }
    pub fn get_curr_action_group_slices(
        &self,
    ) -> (&[FieldAction], &[FieldAction]) {
        let ai = self.pending_action_group_actor_id.unwrap();
        let action_count = self.pending_action_group_action_count;
        let agq = &self.actors[ai].action_group_queues[0];
        let actions_start =
            agq.actions.next_free_index().wrapping_sub(action_count);
        subslice_slice_pair(
            agq.actions.data.as_slices(),
            actions_start..actions_start + action_count,
        )
    }
    pub fn get_curr_action_group_action_count(&self) -> usize {
        debug_assert!(self.pending_action_group_actor_id.is_some());
        self.pending_action_group_action_count
    }
    pub fn get_curr_action_group_field_count_delta(&self) -> isize {
        debug_assert!(self.pending_action_group_actor_id.is_some());
        self.pending_action_group_field_count_delta
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
        #[cfg(feature = "debug_logging_field_action_groups")]
        {
            eprintln!(
                "added action group {}, ms {}, actor {ai}:",
                agq.action_groups.next_free_index(),
                self.match_set_id,
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
    pub fn last_actor_ref(&self) -> ActorRef {
        if self.actors.is_empty() {
            ActorRef::Unconfirmed(self.actors.next_free_index())
        } else {
            ActorRef::Present(self.actors.max_index())
        }
    }
    pub fn next_actor_ref(&self) -> ActorRef {
        ActorRef::Unconfirmed(self.actors.next_free_index())
    }
    pub(super) fn release_temp_action_group(
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
        let lhs_idx = lhs.temp_idx().unwrap_or(0);
        let rhs_idx = rhs.temp_idx().unwrap_or(0);
        match lhs_idx + rhs_idx {
            0 => 1,
            1 => 2,
            2 | 3 => 0,
            _ => unreachable!(),
        }
    }
    fn get_action_group_slices_raw<'a>(
        actors: &'a OffsetVecDeque<ActorId, Actor>,
        action_temp_buffers: &'a [Vec<FieldAction>; 3],
        agi: &ActionGroupIdentifier,
    ) -> (&'a [FieldAction], &'a [FieldAction]) {
        match agi.location {
            ActionGroupLocation::Regular { actor_id, pow2 } => {
                let actor = &actors[actor_id];
                let agq = &actor.action_group_queues[pow2 as usize];
                let start = agi.group.start.wrapping_sub(agq.actions.offset);
                subslice_slice_pair(
                    agq.actions.data.as_slices(),
                    start..start + agi.group.length,
                )
            }
            ActionGroupLocation::LocalMerge {
                actor_id,
                merge_pow2,
            } => {
                let actor = &actors[actor_id];
                let merge_lvl = &actor.merges[merge_pow2 as usize];
                let start =
                    agi.group.start.wrapping_sub(merge_lvl.actions.offset);
                subslice_slice_pair(
                    merge_lvl.actions.data.as_slices(),
                    start..start + agi.group.length,
                )
            }
            ActionGroupLocation::TempBuffer { idx } => (
                &action_temp_buffers[idx]
                    [agi.group.start..agi.group.start + agi.group.length],
                &[],
            ),
        }
    }
    pub(super) fn get_action_group_slices(
        &self,
        agi: &ActionGroupIdentifier,
    ) -> (&[FieldAction], &[FieldAction]) {
        Self::get_action_group_slices_raw(
            &self.actors,
            &self.action_temp_buffers,
            agi,
        )
    }
    pub fn get_action_group_iter(
        &self,
        agi: &ActionGroupIdentifier,
    ) -> std::iter::Chain<
        std::slice::Iter<FieldAction>,
        std::slice::Iter<FieldAction>,
    > {
        let (s1, s2) = self.get_action_group_slices(agi);
        s1.iter().chain(s2.iter())
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
        #[cfg(feature = "debug_logging_field_action_group_accel")]
        eprintln!(
            "@ appending action group to actor {actor_id} pow2 {pow2} (group count -> {}): \n    > {ag:?}",
            self.actors[actor_id].action_group_queues[pow2 as usize].action_groups.len() + 1
        );
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
                #[cfg(feature = "debug_logging_field_action_group_accel")]
                {
                    eprintln!("@ merging actor {actor_id} pow2 {pow2}:");
                    eprintln!("  + {lhs:?}");
                    eprint_action_list(self.get_action_group_iter(lhs));
                    eprintln!("  + {rhs:?}");
                    eprint_action_list(self.get_action_group_iter(rhs));
                }
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
                let res = ActionGroupIdentifier {
                    group: ag,
                    location: ActionGroupLocation::Regular { actor_id, pow2 },
                };
                #[cfg(feature = "debug_logging_field_action_group_accel")]
                {
                    eprintln!(" -> {res:?}");
                    eprint_action_list(self.get_action_group_iter(&res));
                }
                return Some(res);
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

                #[cfg(feature = "debug_logging_field_action_group_accel")]
                {
                    eprintln!("@ merging into temp:");
                    eprintln!("  + {lhs:?}");
                    eprint_action_list(l1.iter().chain(l2.iter()));
                    eprintln!("  + {rhs:?}");
                    eprint_action_list(r1.iter().chain(r2.iter()));
                }

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
                let res = ActionGroupIdentifier {
                    group: ActionGroup {
                        start,
                        length,
                        field_count_delta: lhs.group.field_count_delta
                            + rhs.group.field_count_delta,
                    },
                    location: ActionGroupLocation::TempBuffer { idx },
                };
                #[cfg(feature = "debug_logging_field_action_group_accel")]
                {
                    eprintln!(" -> {res:?}");
                    eprint_action_list(self.get_action_group_iter(&res));
                }
                return Some(res);
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

    fn generate_snapshot(&mut self, actor_id: ActorId) -> SnapshotRef {
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
        ss[SNAPSHOT_REFCOUNT_OFFSET] = 1;
        ss[SNAPSHOT_ACTOR_COUNT_OFFSET] = next_actor_index as SnapshotEntry;
        for (i, (actor_id, pow2)) in iter.enumerate() {
            let actor = &self.actors[actor_id];
            let agq = &actor.action_group_queues[pow2 as usize];
            ss[i + SNAPSHOT_PREFIX_LEN] =
                agq.action_groups.next_free_index() as SnapshotEntry;
        }
        SnapshotRef {
            snapshot_len: snapshot_len as SnapshotLookupId,
            snapshot_id: freelist_id,
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
                [snapshot.snapshot_id];
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
                    [snapshot_len - SNAPSHOT_LEN_MIN][snapshot.snapshot_id];
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
            [snapshot.snapshot_id][SNAPSHOT_ACTOR_COUNT_OFFSET]
    }
    pub(crate) fn bump_snapshot_refcount(&mut self, snapshot: SnapshotRef) {
        debug_assert!(
            snapshot.snapshot_len >= SNAPSHOT_LEN_MIN as SnapshotEntry
        );
        let ss_rc = &mut self.snapshot_freelists
            [snapshot.snapshot_len as usize - SNAPSHOT_LEN_MIN]
            [snapshot.snapshot_id][SNAPSHOT_REFCOUNT_OFFSET];
        *ss_rc += 1;
    }
    pub(super) fn drop_snapshot_refcount(&mut self, snapshot: SnapshotRef) {
        if snapshot.snapshot_len < SNAPSHOT_LEN_MIN as SnapshotEntry {
            return;
        }
        let ss_rc = &mut self.snapshot_freelists
            [snapshot.snapshot_len as usize - SNAPSHOT_LEN_MIN]
            [snapshot.snapshot_id][SNAPSHOT_REFCOUNT_OFFSET];
        *ss_rc -= 1;
        if *ss_rc == 0 {
            self.snapshot_freelists
                [snapshot.snapshot_len as usize - SNAPSHOT_LEN_MIN]
                .release(snapshot.snapshot_id);
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
                [ssr.snapshot_id];
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
                [ssr.snapshot_id][SNAPSHOT_PREFIX_LEN + i];
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
    pub fn stringify_snapshot(
        &self,
        actor_id: ActorId,
        ss: SnapshotRef,
    ) -> String {
        if ss.snapshot_len < SNAPSHOT_LEN_MIN as SnapshotEntry {
            return "Snapshot { ac: 0, rc: 0, [ ] }".to_string();
        }
        let ss = &self.snapshot_freelists
            [ss.snapshot_len as usize - SNAPSHOT_LEN_MIN][ss.snapshot_id];
        let ac = ss[SNAPSHOT_ACTOR_COUNT_OFFSET];
        let rc = ss[SNAPSHOT_REFCOUNT_OFFSET];
        let data = &ss[SNAPSHOT_PREFIX_LEN..];

        let mut res = format!("Snapshot {{ ac: {ac}, rc: {rc}, [ ");
        let iter = Pow2LookupStepsIter::new(actor_id, ac);
        for (i, (ai, pow2)) in iter.enumerate() {
            res.write_fmt(format_args!(
                "(ai: {ai}, pow2: {pow2}, gi: {}) ",
                data[i]
            ))
            .unwrap();
        }
        res.push_str("] }");
        res
    }
    pub(crate) fn update_actor_snapshot(
        &mut self,
        actor_id: ActorId,
    ) -> SnapshotRef {
        let actor_ss = self.actors[actor_id].latest_snapshot;
        if self.validate_snapshot_and_refresh_action_groups(actor_id, actor_ss)
        {
            return actor_ss;
        }
        let new_ss = self.generate_snapshot(actor_id);
        // we generate the new one first before dropping the reference to
        // the old one to make sure that when we later compare refs to the two
        // they are not the same
        self.drop_snapshot_refcount(actor_ss);
        self.actors[actor_id].latest_snapshot = new_ss;
        new_ss
    }

    pub(super) fn build_actions_from_snapshot(
        &mut self,
        actor_id: ActorId,
        snapshot: SnapshotRef,
    ) -> Option<ActionGroupIdentifier> {
        let field_ss_actor_count = self.get_snapshot_actor_count(snapshot);
        if field_ss_actor_count == self.actors.next_free_index() {
            self.apply_from_snapshot_with_same_actor_count(actor_id, snapshot)
        } else {
            self.apply_from_snapshot_with_different_actor_count(
                actor_id, snapshot,
            )
        }
    }
    fn execute_actions(
        &mut self,
        fm: &FieldManager,
        field_id: FieldId,
        agi: &ActionGroupIdentifier,
        full_cow_field_refs: &mut [FullCowFieldRef],
    ) {
        let (s1, s2) = Self::get_action_group_slices_raw(
            &self.actors,
            &self.action_temp_buffers,
            agi,
        );
        let actions = s1.iter().chain(s2);
        #[cfg(feature = "debug_logging_field_actions")]
        {
            let field = fm.fields[field_id].borrow();
            eprintln!(
                "executing for field {} (ms {}, first actor: {:?}):",
                field_id, field.match_set, field.first_actor
            );
            drop(field);
            eprint_action_list(actions.clone());

            eprint!("   + before: ");
            fm.print_field_header_data(field_id, 3);
            #[cfg(feature = "debug_logging_iter_states")]
            {
                eprint!("\n   ");
                fm.print_field_iter_data(field_id, 3);
            }
            eprintln!();
        }
        let mut field_ref_mut = fm.fields[field_id].borrow_mut();
        let field = &mut *field_ref_mut;
        let fd = &mut field.iter_hall.field_data;
        let (headers, field_count) = match &mut field.iter_hall.data_source {
            FieldDataSource::Owned => (&mut fd.headers, &mut fd.field_count),
            FieldDataSource::DataCow { .. }
            | FieldDataSource::RecordBufferDataCow(_) => {
                (&mut fd.headers, &mut fd.field_count)
            }
            FieldDataSource::Alias(_) => {
                panic!("cannot execute commands on Alias iter hall")
            }
            FieldDataSource::FullCow(_)
            | FieldDataSource::SameMsCow(_)
            | FieldDataSource::RecordBufferFullCow(_) => {
                panic!("cannot execute commands on FullCow iter hall")
            }
        };
        let iterators = field
            .iter_hall
            .iters
            .iter_mut()
            .chain(
                full_cow_field_refs
                    .iter_mut()
                    .filter(|fcf| !fcf.through_data_cow)
                    .flat_map(|f| {
                        f.field.as_mut().unwrap().iter_hall.iters.iter_mut()
                    }),
            )
            .map(Cell::get_mut);

        let _field_count_delta = self.actions_applicator.run(
            actions,
            headers,
            field_count,
            iterators,
        );
        #[cfg(feature = "debug_logging_field_actions")]
        {
            drop(field_ref_mut);
            eprint!("   + after: ");
            fm.print_field_header_data(field_id, 3);
            #[cfg(feature = "debug_logging_iter_states")]
            {
                eprint!("\n   ");
                fm.print_field_iter_data(field_id, 3);
            }
            eprintln!();
        }
        // debug_assert!(_field_count_delta == agi.group.field_count_delta);
    }
    fn calc_dead_data(
        headers: &VecDeque<FieldValueHeader>,
        dead_data_max: DeadDataReport,
        cow_data_end: usize,
        origin_field_data_size: usize,
    ) -> DeadDataReport {
        let mut data = 0;
        let mut dead_data_leading = dead_data_max.dead_data_leading;
        let mut dead_data_trailing = dead_data_max.dead_data_trailing;

        for &h in headers {
            if data >= dead_data_leading {
                break;
            }
            if h.references_alive_data() {
                data += h.leading_padding();
                dead_data_leading = dead_data_leading.min(data);
                break;
            }
            data += h.total_size_unique();
        }
        if dead_data_leading == origin_field_data_size {
            // if everything is dead, we don't reduce trailing
            return DeadDataReport {
                dead_data_leading,
                dead_data_trailing,
            };
        }
        data = origin_field_data_size - cow_data_end;
        for &h in headers.iter().rev() {
            if data >= dead_data_trailing {
                break;
            }
            if h.references_alive_data() {
                dead_data_trailing = dead_data_trailing.min(data);
                break;
            }
            data += h.total_size_unique();
        }
        DeadDataReport {
            dead_data_leading,
            dead_data_trailing,
        }
    }
    fn get_data_cow_data_end(field: &Field, iter_state: &IterState) -> usize {
        let headers = &field.iter_hall.field_data.headers;
        let mut data_end = iter_state.data;
        let h = headers[iter_state.header_idx];
        if !h.same_value_as_previous() {
            data_end += h.leading_padding();
            if h.shared_value() {
                data_end += h.size as usize;
            } else {
                data_end +=
                    h.size as usize * iter_state.header_rl_offset as usize;
            }
        }
        data_end
    }
    // returns the target index of the field and whether or not it is data cow
    fn push_cow_field<'a>(
        fm: &'a FieldManager,
        tgt_field_id: u32,
        through_data_cow: bool,
        field: &Field,
        data_cow_field_refs: &mut Vec<DataCowFieldRef<'a>>,
        update_cow_ms: Option<MatchSetId>,
        full_cow_field_refs: &mut Vec<FullCowFieldRef<'a>>,
        data_cow_idx: Option<usize>,
        first_action_index: usize,
    ) -> (usize, bool) {
        let mut tgt_field = fm.fields[tgt_field_id].borrow_mut();

        let cow_variant = tgt_field.iter_hall.data_source.cow_variant();
        let is_data_cow = matches!(cow_variant, Some(CowVariant::DataCow));
        let ms_id = tgt_field.match_set;

        if !is_data_cow && through_data_cow {
            full_cow_field_refs.push(FullCowFieldRef {
                #[cfg(feature = "debug_state")]
                field_id: tgt_field_id,
                field: None,
                data_cow_idx,
                through_data_cow,
            });
            return (full_cow_field_refs.len() - 1, false);
        }
        let cds = tgt_field.iter_hall.get_cow_data_source_mut().unwrap();
        let tgt_cow_end = field.iter_hall.iters[cds.header_iter_id].get();

        if is_data_cow {
            data_cow_field_refs.push(DataCowFieldRef {
                #[cfg(feature = "debug_state")]
                field_id: tgt_field_id,
                field: None,
                data_end: Self::get_data_cow_data_end(field, &tgt_cow_end),
                drop_info: HeaderDropInfo::default(),
            });
            return (data_cow_field_refs.len() - 1, true);
        }
        debug_assert!(cow_variant == Some(CowVariant::FullCow));
        if Some(ms_id) == update_cow_ms {
            full_cow_field_refs.push(FullCowFieldRef {
                #[cfg(feature = "debug_state")]
                field_id: tgt_field_id,
                field: None,
                data_cow_idx,
                through_data_cow,
            });
            return (full_cow_field_refs.len() - 1, false);
        }
        if tgt_cow_end.field_pos > first_action_index {
            tgt_field.iter_hall.data_source = FieldDataSource::DataCow(*cds);
            debug_assert!(tgt_field.iter_hall.field_data.is_empty());
            tgt_field.iter_hall.copy_headers_from_cow_src(
                &field.iter_hall.field_data.headers,
                tgt_cow_end,
            );
            // TODO: we could optimize this case because we might
            // end up calculating the dead data multiple times because of
            // this, but we don't care for now
            data_cow_field_refs.push(DataCowFieldRef {
                #[cfg(feature = "debug_state")]
                field_id: tgt_field_id,
                field: None,
                data_end: Self::get_data_cow_data_end(field, &tgt_cow_end),
                drop_info: HeaderDropInfo::default(),
            });
            return (data_cow_field_refs.len() - 1, true);
        }
        full_cow_field_refs.push(FullCowFieldRef {
            #[cfg(feature = "debug_state")]
            field_id: tgt_field_id,
            field: None,
            data_cow_idx,
            through_data_cow,
        });
        // TODO: support RecordBuffers
        (full_cow_field_refs.len() - 1, false)
    }

    fn gather_cow_field_info_pre_exec<'a>(
        &mut self,
        fm: &'a FieldManager,
        msm: &MatchSetManager,
        field_id: FieldId,
        update_cow_ms: Option<MatchSetId>,
        first_action_index: usize,
        through_data_cow: bool,
        full_cow_field_refs: &mut Vec<FullCowFieldRef<'a>>,
        data_cow_field_refs: &mut Vec<DataCowFieldRef<'a>>,
        data_cow_idx: Option<usize>,
    ) {
        let field = fm.fields[field_id].borrow();
        if !field.has_cow_targets() {
            return;
        }
        for &tgt_field_id in &field.iter_hall.cow_targets {
            let (curr_field_tgt_idx, data_cow) = Self::push_cow_field(
                fm,
                tgt_field_id,
                through_data_cow,
                &field,
                data_cow_field_refs,
                update_cow_ms,
                full_cow_field_refs,
                data_cow_idx,
                first_action_index,
            );
            self.gather_cow_field_info_pre_exec(
                fm,
                msm,
                tgt_field_id,
                None,
                first_action_index,
                data_cow || through_data_cow,
                full_cow_field_refs,
                data_cow_field_refs,
                if data_cow {
                    Some(curr_field_tgt_idx)
                } else {
                    data_cow_idx
                },
            );
            let tgt_field = fm.fields[tgt_field_id].borrow_mut();

            if cfg!(debug_assertions) {
                let actor = tgt_field.first_actor.get();
                let snapshot = tgt_field.snapshot.get();
                let cow_target_is_up_to_date =
                    if tgt_field.match_set == field.match_set {
                        self.is_snapshot_current(actor, snapshot)
                    } else {
                        msm.match_sets[tgt_field.match_set]
                            .action_buffer
                            .borrow_mut()
                            .is_snapshot_current(actor, snapshot)
                    };
                debug_assert!(cow_target_is_up_to_date);
            }

            if data_cow {
                data_cow_field_refs[curr_field_tgt_idx].field =
                    Some(tgt_field);
            } else {
                full_cow_field_refs[curr_field_tgt_idx].field =
                    Some(tgt_field);
            }
        }
    }
    pub(super) fn update_cow_fields_post_exec(
        fm: &FieldManager,
        field_id: FieldId,
        update_cow_ms: MatchSetId,
        batch_size: usize,
    ) {
        let field = fm.fields[field_id].borrow();
        for &tgt_field_id in &field.iter_hall.cow_targets {
            let mut tgt_field = fm.fields[tgt_field_id].borrow_mut();
            if tgt_field.match_set != update_cow_ms {
                continue;
            }
            let cds = *tgt_field.iter_hall.get_cow_data_source().unwrap();

            let cow_variant = tgt_field.iter_hall.data_source.cow_variant();
            let tgt_cow_end = field.iter_hall.iters[cds.header_iter_id].get();
            // we can assume the snapshot to be up to date because we
            // just finished running field updates
            // we can't just use the field directly here because
            // it could be cow of cow
            let field_ref = fm.get_cow_field_ref_raw(field_id);
            let mut iter =
                fm.lookup_iter(field_id, &field_ref, cds.header_iter_id);
            iter.next_n_fields(batch_size, true);
            fm.store_iter(field_id, cds.header_iter_id, iter);

            if cow_variant != Some(CowVariant::DataCow) {
                // TODO: support RecordBufferDataCow
                debug_assert!(cow_variant == Some(CowVariant::FullCow));
                continue;
            }
            if tgt_cow_end.field_pos == 0
                && tgt_field.iter_hall.field_data.field_count == 0
            {
                tgt_field.iter_hall.data_source =
                    FieldDataSource::FullCow(cds);
                tgt_field.iter_hall.reset_iterators();
                tgt_field.iter_hall.field_data.headers.clear();
                debug_assert!(tgt_field.iter_hall.field_data.data.is_empty());
                continue;
            }
            let (headers, count) = fm.get_field_headers(Ref::clone(&field));
            let tgt_fd = &mut tgt_field.iter_hall.field_data;
            let start_idx = tgt_cow_end.header_idx;
            if start_idx != headers.len() {
                let mut first_header = headers[start_idx];
                first_header.run_length -= tgt_cow_end.header_rl_offset;
                if first_header.run_length != 0 {
                    if tgt_cow_end.header_rl_offset != 0 {
                        first_header.set_leading_padding(0);
                    }
                    if first_header.shared_value()
                        && tgt_cow_end.header_rl_offset != 0
                    {
                        // if we already have parts of this header,
                        // we must make sure to not mess up our data offset
                        first_header.set_same_value_as_previous(true);
                    }
                    tgt_fd.headers.push_back(first_header);
                }
            }
            if start_idx + 1 < headers.len() {
                tgt_fd.headers.extend(headers.range(start_idx + 1..));
            }
            tgt_fd.field_count += count - tgt_cow_end.field_pos;
        }
    }
    fn build_header_drop_instructions(
        dead_data: DeadDataReport,
        field_data_size: usize,
    ) -> HeaderDropInstructions {
        let mut lead = dead_data.dead_data_leading;
        let trail = dead_data.dead_data_trailing;
        let mut padding = 0;
        if field_data_size == trail {
            lead = 0;
        } else {
            lead = lead.min(field_data_size - trail);
            // the first value in a field is always maximally aligned
            padding = lead % MAX_FIELD_ALIGN;
        }
        HeaderDropInstructions {
            first_header_padding: padding,
            leading_drop: lead,
            trailing_drop: trail,
        }
    }
    fn drop_dead_field_data(
        &mut self,
        #[cfg_attr(
            not(feature = "debug_logging_field_actions"),
            allow(unused)
        )]
        fm: &FieldManager,
        field: &mut Field,
        drop_instructions: HeaderDropInstructions,
    ) -> HeaderDropInfo {
        let field_data_size = field.iter_hall.field_data.data.len();
        let drop_info = self.drop_dead_headers(
            &mut field.iter_hall.field_data.headers,
            &mut field.iter_hall.iters,
            drop_instructions,
            field_data_size,
            field_data_size,
        );
        Self::adjust_iters_to_data_drop(
            &mut field.iter_hall.iters,
            drop_instructions.leading_drop,
            &drop_info,
        );
        let fd = &mut field.iter_hall.field_data;
        // LEAK this leaks all resources of the data. //TODO: drop before
        fd.data.drop_front(
            drop_instructions
                .leading_drop
                .prev_multiple_of(&MAX_FIELD_ALIGN),
        );
        fd.data.drop_back(drop_instructions.trailing_drop);
        #[cfg(feature = "debug_logging_field_actions")]
        {
            eprintln!(
            "   + dropping dead data (leading: {}, pad: {}, rem: {}, trailing: {})",
            drop_instructions.leading_drop,
            drop_instructions.first_header_padding,
            field_data_size - drop_instructions.leading_drop - drop_instructions.trailing_drop,
            drop_instructions.trailing_drop
        );
            eprint!("    ");
            fm.print_field_header_data_for_ref(field, 4);
            #[cfg(feature = "debug_logging_iter_states")]
            {
                eprint!("\n    ");
                fm.print_field_iter_data_for_ref(field, 4);
            }
            eprintln!();
        }
        drop_info
    }
    fn adjust_iters_to_data_drop<'a>(
        iters: impl IntoIterator<Item = &'a mut Cell<IterState>>,
        dead_data_leading: usize,
        drop_info: &HeaderDropInfo,
    ) {
        let last_header_idx = drop_info.header_count_rem.saturating_sub(1);
        for it in iters.into_iter().map(Cell::get_mut) {
            it.data = it.data.saturating_sub(dead_data_leading);
            if it.header_idx == drop_info.dead_headers_leading {
                it.header_rl_offset = it
                    .header_rl_offset
                    .saturating_sub(drop_info.first_header_dropped_elem_count);
            }
            it.header_idx =
                it.header_idx.saturating_sub(drop_info.dead_headers_leading);
            if it.header_idx > last_header_idx {
                it.header_idx = last_header_idx;
                it.data = drop_info.last_header_data_pos;
            }
            if it.header_idx == last_header_idx {
                it.header_rl_offset =
                    it.header_rl_offset.min(drop_info.last_header_run_len);
            }
        }
    }
    fn drop_dead_headers<'a>(
        &mut self,
        headers: &mut VecDeque<FieldValueHeader>,
        iters: impl IntoIterator<Item = &'a mut Cell<IterState>>,
        drop_instructions: HeaderDropInstructions,
        origin_field_data_size: usize,
        field_data_size: usize,
    ) -> HeaderDropInfo {
        debug_assert!(self.preserved_headers.is_empty());
        let mut dead_data_leading_rem = drop_instructions.leading_drop;
        let mut dead_headers_leading = 0;
        let mut first_header_dropped_elem_count = 0;

        while dead_headers_leading < headers.len() {
            let h = &mut headers[dead_headers_leading];
            let h_ds = h.total_size_unique();
            if dead_data_leading_rem < h_ds {
                let header_elem_size = h.fmt.size as usize;
                let header_padding = h.leading_padding();
                first_header_dropped_elem_count =
                    ((dead_data_leading_rem - header_padding)
                        / header_elem_size) as RunLength;
                h.run_length -= first_header_dropped_elem_count;
                h.set_leading_padding(drop_instructions.first_header_padding);
                break;
            }
            if !h.deleted() {
                debug_assert_eq!(h_ds, 0);
                self.preserved_headers.push(*h);
            }
            dead_data_leading_rem -= h_ds;
            dead_headers_leading += 1;
        }
        headers.drain(0..dead_headers_leading);
        dead_headers_leading -= self.preserved_headers.len();
        while let Some(h) = self.preserved_headers.pop() {
            headers.push_front(h);
        }

        let field_size_diff = origin_field_data_size - field_data_size;
        let mut dead_data_rem_trailing = drop_instructions
            .trailing_drop
            .saturating_sub(field_size_diff);
        let mut last_header_alive = headers.len();
        while last_header_alive > 0 {
            let h = headers[last_header_alive - 1];
            let h_ds = h.total_size_unique();
            if dead_data_rem_trailing < h_ds {
                break;
            }
            if !h.deleted() {
                debug_assert_eq!(h_ds, 0);
                self.preserved_headers.push(h);
            }
            last_header_alive -= 1;
            dead_data_rem_trailing -= h_ds;
        }
        if dead_data_rem_trailing > 0 {
            let header_elem_size =
                headers[last_header_alive].fmt.size as usize;
            let elem_count = dead_data_rem_trailing / header_elem_size;
            debug_assert!(
                elem_count * header_elem_size == dead_data_rem_trailing
            );
            headers[last_header_alive].run_length -= elem_count as RunLength;
        }
        headers.drain(last_header_alive..);
        last_header_alive += self.preserved_headers.len();
        headers.extend(self.preserved_headers.drain(0..));
        let last_header = headers.back().copied().unwrap_or_default();

        let field_end_new = field_data_size
            + drop_instructions.first_header_padding
            - drop_instructions.trailing_drop
            - drop_instructions.leading_drop;

        let last_header_size = last_header.total_size_unique();

        let drop_info = HeaderDropInfo {
            dead_headers_leading,
            first_header_dropped_elem_count,
            header_count_rem: last_header_alive,
            last_header_run_len: last_header.run_length,
            last_header_data_pos: field_end_new - last_header_size,
        };

        if drop_instructions.leading_drop != 0 || dead_headers_leading != 0 {
            Self::adjust_iters_to_data_drop(
                iters,
                drop_instructions.leading_drop,
                &drop_info,
            );
        }

        drop_info
    }
    pub fn gather_pending_actions(
        &mut self,
        actions: &mut Vec<FieldAction>,
        actor_ref: ActorRef,
        snapshot: SnapshotRef,
    ) {
        let Some(actor_id) = self.get_actor_id_from_ref(actor_ref) else {
            return;
        };
        let snapshot_new = self.update_actor_snapshot(actor_id);
        if snapshot_new == snapshot {
            return;
        }
        let Some(agi) = self.build_actions_from_snapshot(actor_id, snapshot)
        else {
            return;
        };

        let (s1, s2) = self.get_action_group_slices(&agi);
        actions.extend(s1);
        actions.extend(s2);

        self.release_temp_action_group(Some(agi));
    }

    pub fn is_snapshot_current(
        &mut self,
        actor_ref: ActorRef,
        snapshot: SnapshotRef,
    ) -> bool {
        let Some(actor_id) = self.get_actor_id_from_ref(actor_ref) else {
            return true;
        };
        let snapshot_new = self.update_actor_snapshot(actor_id);
        snapshot_new == snapshot
    }

    pub fn update_field(
        &mut self,
        fm: &FieldManager,
        msm: &MatchSetManager,
        field_id: FieldId,
        update_cow_ms: Option<MatchSetId>,
    ) {
        // can't `borrow_mut`: if the snapshot is current,
        // we might already be using this field through cow in
        // some active iterator
        let field = fm.fields[field_id].borrow();

        let mut field_actor_ref = field.first_actor.get();
        let field_snapshot = field.snapshot.get();

        let Some(actor_id) = self.initialize_actor_ref(
            Some(ActorSubscriber::Field(field_id)),
            &mut field_actor_ref,
        ) else {
            return;
        };
        field.first_actor.set(field_actor_ref);

        let snapshot_new = self.update_actor_snapshot(actor_id);
        if field_snapshot == snapshot_new {
            return;
        }
        field.snapshot.set(snapshot_new);
        self.bump_snapshot_refcount(snapshot_new);
        drop(field);

        #[cfg(feature = "debug_logging_field_action_group_accel")]
        eprintln!(
            "@ updated snapshot for of actor {actor_id} for field {field_id}: \n - prev: {}\n - next: {}",
            self.stringify_snapshot(actor_id, field_snapshot),
            self.stringify_snapshot(actor_id, snapshot_new),
        );

        let res = self.build_actions_from_snapshot(actor_id, field_snapshot);
        self.drop_snapshot_refcount(field_snapshot);

        let Some(agi) = res else {
            return;
        };

        let (s1, s2) = self.get_action_group_slices(&agi);
        let Some(first_action_index) =
            s1.first().or_else(|| s2.first()).map(|a| a.field_idx)
        else {
            return;
        };

        let mut full_cow_fields =
            transmute_vec(std::mem::take(&mut self.full_cow_field_refs_temp));
        let mut data_cow_fields =
            transmute_vec(std::mem::take(&mut self.data_cow_field_refs_temp));

        self.apply_actions_to_field(
            fm,
            msm,
            field_id,
            actor_id,
            update_cow_ms,
            &agi,
            first_action_index,
            &mut full_cow_fields,
            &mut data_cow_fields,
        );

        self.full_cow_field_refs_temp = transmute_vec(full_cow_fields);
        self.data_cow_field_refs_temp = transmute_vec(data_cow_fields);
        self.release_temp_action_group(Some(agi));
    }

    fn apply_actions_to_field<'a>(
        &mut self,
        fm: &'a FieldManager,
        msm: &MatchSetManager,
        field_id: u32,
        actor_id: u32,
        update_cow_ms: Option<MatchSetId>,
        agi: &ActionGroupIdentifier,
        first_action_index: usize,
        full_cow_fields: &mut Vec<FullCowFieldRef<'a>>,
        data_cow_fields: &mut Vec<DataCowFieldRef<'a>>,
    ) {
        let mut field = fm.fields[field_id].borrow_mut();
        field.iter_hall.uncow_headers(fm);

        let field_count = field.iter_hall.get_field_count(fm);
        let field_ms_id = field.match_set;
        let field_data_size: usize = field.iter_hall.get_field_data_len(fm);
        let data_owned = field.iter_hall.data_source == FieldDataSource::Owned;

        drop(field);
        self.gather_cow_field_info_pre_exec(
            fm,
            msm,
            field_id,
            update_cow_ms,
            first_action_index,
            false,
            full_cow_fields,
            data_cow_fields,
            None,
        );
        let mut dead_data = DeadDataReport::all_dead(field_data_size);
        for dcf in &mut *data_cow_fields {
            dead_data = Self::calc_dead_data(
                &dcf.field.as_ref().unwrap().iter_hall.field_data.headers,
                dead_data,
                dcf.data_end,
                field_data_size,
            );
        }
        debug_assert!(-agi.group.field_count_delta <= field_count as isize);
        let all_fields_dead =
            -agi.group.field_count_delta == field_count as isize;
        if !all_fields_dead || !data_owned {
            self.execute_actions(fm, field_id, agi, full_cow_fields);
        }
        let mut field = fm.fields[field_id].borrow_mut();

        if !all_fields_dead && data_owned {
            dead_data = Self::calc_dead_data(
                &field.iter_hall.field_data.headers,
                dead_data,
                field_data_size,
                field_data_size,
            );
        }
        // Even if the field no longer uses the data, some data COWs might,
        // in which case we can't clear it.
        let all_data_dead = data_owned
            && dead_data.dead_data_leading == field_data_size
            && all_fields_dead;
        let data_partially_dead = data_owned
            && (dead_data.dead_data_leading != 0
                || dead_data.dead_data_trailing != 0)
            && !all_data_dead;

        if all_data_dead {
            if cfg!(feature = "debug_logging_field_actions") {
                eprintln!(
                "clearing field {} (ms {}, first actor: {}, field_count: {}) ({}):",
                field_id, field_ms_id, actor_id, field_count,
                if all_data_dead {"all data dead"} else {"some data remains alive"}
            );
                self.eprint_action_list_from_agi(agi);
            }
            field.iter_hall.reset_iterators();
            field.iter_hall.field_data.clear();
        }
        if data_partially_dead {
            let header_drop_instructions =
                Self::build_header_drop_instructions(
                    dead_data,
                    field_data_size,
                );
            let root_drop_info = self.drop_dead_field_data(
                fm,
                &mut field,
                header_drop_instructions,
            );
            for dcf in &mut *data_cow_fields {
                let cow_field = &mut **dcf.field.as_mut().unwrap();
                #[cfg(feature = "debug_logging_field_actions")]
                {
                    eprintln!(
                        "   + dropping dead data for cow field {}: before",
                        dcf.field_id
                    );
                    eprint!("    ");
                    fm.print_field_header_data_for_ref(cow_field, 4);
                    #[cfg(feature = "debug_logging_iter_states")]
                    {
                        eprint!("\n    ");
                        fm.print_field_iter_data_for_ref(cow_field, 4);
                    }
                    eprintln!();
                }
                dcf.drop_info = self.drop_dead_headers(
                    &mut cow_field.iter_hall.field_data.headers,
                    &mut cow_field.iter_hall.iters,
                    header_drop_instructions,
                    field_data_size,
                    dcf.data_end,
                );
                #[cfg(feature = "debug_logging_field_actions")]
                {
                    eprintln!(
                        "   + dropping dead data for cow field {}: after",
                        dcf.field_id
                    );
                    eprint!("    ");
                    fm.print_field_header_data_for_ref(cow_field, 4);
                    #[cfg(feature = "debug_logging_iter_states")]
                    {
                        eprint!("\n    ");
                        fm.print_field_iter_data_for_ref(cow_field, 4);
                    }
                    eprintln!();
                }
            }
            for fcf in &mut *full_cow_fields {
                let drop_info = fcf
                    .data_cow_idx
                    .map(|idx| &data_cow_fields[idx].drop_info)
                    .unwrap_or(&root_drop_info);
                Self::adjust_iters_to_data_drop(
                    &mut fcf.field.as_mut().unwrap().iter_hall.iters,
                    header_drop_instructions.leading_drop,
                    drop_info,
                )
            }
        }
    }
    pub fn get_actor_id_from_ref(
        &self,
        actor_ref: ActorRef,
    ) -> Option<ActorId> {
        match actor_ref {
            ActorRef::Present(actor) => Some(actor),
            ActorRef::Unconfirmed(actor) => {
                if self.actors.next_free_index() > actor {
                    Some(actor)
                } else {
                    None
                }
            }
        }
    }
    pub fn initialize_actor_ref(
        &mut self,
        subscriber: Option<ActorSubscriber>,
        actor_ref: &mut ActorRef,
    ) -> Option<ActorId> {
        match *actor_ref {
            ActorRef::Present(actor) => Some(actor),
            ActorRef::Unconfirmed(actor) => {
                let actor_id = self.get_actor_id_from_ref(*actor_ref)?;
                *actor_ref = ActorRef::Present(actor_id);
                if let Some(subscriber) = subscriber {
                    self.actors[actor].subscribers.push(subscriber);
                }
                Some(actor)
            }
        }
    }
    pub fn drop_field_actions(&mut self, field_id: FieldId, field: &Field) {
        if self.actors.data.is_empty() {
            return;
        }

        let mut first_actor = field.first_actor.get();
        let snapshot = field.snapshot.get();

        let Some(actor_id) = self.initialize_actor_ref(
            Some(ActorSubscriber::Field(field_id)),
            &mut first_actor,
        ) else {
            return;
        };
        field.first_actor.set(first_actor);

        let ss = self.update_actor_snapshot(actor_id);
        if ss == snapshot {
            return;
        }
        #[cfg(feature = "debug_logging")]
        {
            eprintln!(
                "dropping actions for field {} (ms: {}, first actor: {})",
                field_id, self.match_set_id, actor_id,
            );
        }
        #[cfg(feature = "debug_logging_field_action_group_accel")]
        eprintln!(
            "@ updated snapshot for field {field_id}: \n - prev: {}\n - next: {}",
            self.stringify_snapshot(actor_id, snapshot),
            self.stringify_snapshot(actor_id, ss),
        );
        self.drop_snapshot_refcount(snapshot);
        self.bump_snapshot_refcount(ss);

        field.snapshot.set(ss);
    }
}

#[cfg(test)]
mod test_iter {
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

#[cfg(test)]
mod test_dead_data_drop {
    use std::{cell::Cell, collections::VecDeque};

    use crate::{
        record_data::{
            action_buffer::{ActionBuffer, DeadDataReport},
            field::{FieldManager, FIELD_REF_LOOKUP_ITER_ID},
            field_action::FieldActionKind,
            field_action_applicator::testing_helpers::{
                iter_state_dummy_to_iter_state, IterStateDummy,
            },
            field_data::{
                field_value_flags, FieldValueFormat, FieldValueHeader,
                FieldValueRepr,
            },
            field_value::FieldValue,
            iter_hall::IterState,
            match_set::{MatchSetId, MatchSetManager},
            push_interface::PushInterface,
            scope_manager::ScopeManager,
        },
        utils::indexing_type::IndexingType,
    };

    use super::ActorRef;

    #[track_caller]
    fn test_drop_dead_data(
        headers_before: impl IntoIterator<Item = FieldValueHeader>,
        headers_after: impl IntoIterator<Item = FieldValueHeader>,
        field_data_size_before: usize,
        cow_data_end: usize,
        iters_before: impl IntoIterator<Item = IterStateDummy>,
        iters_after: impl IntoIterator<Item = IterStateDummy>,
    ) {
        let mut headers = headers_before.into_iter().collect::<VecDeque<_>>();
        let headers_after = headers_after.into_iter().collect::<VecDeque<_>>();

        fn collect_iters(
            iters: impl IntoIterator<Item = IterStateDummy>,
        ) -> Vec<Cell<IterState>> {
            iters
                .into_iter()
                .map(iter_state_dummy_to_iter_state)
                .map(Cell::new)
                .collect::<Vec<_>>()
        }

        let mut iters = collect_iters(iters_before);
        let iters_after = collect_iters(iters_after);

        let mut ab = ActionBuffer::new(MatchSetId::MAX_VALUE);

        let dead_data = ActionBuffer::calc_dead_data(
            &headers,
            DeadDataReport::all_dead(field_data_size_before),
            cow_data_end,
            field_data_size_before,
        );
        let header_drop_instructions =
            ActionBuffer::build_header_drop_instructions(
                dead_data,
                field_data_size_before,
            );

        ab.drop_dead_headers(
            &mut headers,
            &mut iters,
            header_drop_instructions,
            field_data_size_before,
            cow_data_end,
        );

        assert_eq!(headers, headers_after);
        assert_eq!(iters, iters_after);
    }

    #[test]
    fn padding_dropped_correctly() {
        let mut sm = ScopeManager::default();
        let mut fm = FieldManager::default();
        let mut msm = MatchSetManager::default();
        let scope_id = sm.add_scope(None);
        let ms_id = msm.add_match_set(&mut fm, &mut sm, scope_id);
        let field_id = fm.add_field(ms_id, ActorRef::default());

        {
            let mut field = fm.fields[field_id].borrow_mut();
            field.iter_hall.push_str("foo", 1, false, false);
            field.iter_hall.push_int(0, 1, false, false);
        }
        {
            let mut ab = msm.match_sets[ms_id].action_buffer.borrow_mut();
            let actor_id = ab.add_actor();
            ab.begin_action_group(actor_id);
            ab.push_action(FieldActionKind::Drop, 0, 1);
            ab.end_action_group();
        }
        fm.apply_field_actions(&msm, field_id, true);
        let field_ref = fm.get_cow_field_ref(&msm, field_id);
        let iter = fm.get_auto_deref_iter(
            field_id,
            &field_ref,
            FIELD_REF_LOOKUP_ITER_ID,
        );
        let res = iter
            .into_value_ref_iter(&msm)
            .map(|(v, rl, _offs)| (v.to_field_value(), rl))
            .collect::<Vec<_>>();
        assert_eq!(&res, &[(FieldValue::Int(0), 1)]);
    }

    #[test]
    fn test_sandwiched_by_undefined() {
        let headers_in = [
            FieldValueHeader {
                fmt: FieldValueFormat {
                    repr: FieldValueRepr::Undefined,
                    size: 0,
                    flags: field_value_flags::DELETED,
                },
                run_length: 1,
            },
            FieldValueHeader {
                fmt: FieldValueFormat {
                    repr: FieldValueRepr::TextInline,
                    size: 6,
                    flags: field_value_flags::DELETED
                        | field_value_flags::SHARED_VALUE,
                },
                run_length: 1,
            },
            FieldValueHeader {
                fmt: FieldValueFormat {
                    repr: FieldValueRepr::Undefined,
                    size: 0,
                    flags: field_value_flags::DEFAULT,
                },
                run_length: 1,
            },
        ];
        let headers_out = [FieldValueHeader {
            fmt: FieldValueFormat {
                repr: FieldValueRepr::Undefined,
                size: 0,
                flags: field_value_flags::DEFAULT,
            },
            run_length: 1,
        }];

        test_drop_dead_data(headers_in, headers_out, 6, 6, [], []);
    }

    #[test]
    fn correct_padding_between_same_type() {
        test_drop_dead_data(
            [
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::TextInline,
                        flags: field_value_flags::DELETED,
                        size: 1,
                    },
                    run_length: 1,
                },
                FieldValueHeader {
                    fmt: FieldValueFormat {
                        repr: FieldValueRepr::TextInline,
                        flags: field_value_flags::DEFAULT,
                        size: 1,
                    },
                    run_length: 2,
                },
            ],
            [FieldValueHeader {
                fmt: FieldValueFormat {
                    repr: FieldValueRepr::TextInline,
                    flags: field_value_flags::padding(1),
                    size: 1,
                },
                run_length: 2,
            }],
            3,
            3,
            [],
            [],
        );
    }
}
