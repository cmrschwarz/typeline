use std::{collections::VecDeque, mem::size_of, ops::Index};

use crate::utils::{
    dynamic_freelist::DynamicArrayFreelist, launder_slice,
    offset_vec_deque::OffsetVecDeque, subslice_slice_pair,
};

use super::{
    field::FieldId,
    field_action::{merge_action_lists, FieldAction, FieldActionKind},
    field_data::RunLength,
};

type ActorId = u32;
type ActionGroupId = u32;
type ActionId = usize;

type SnapshotLookupId = u32;

#[derive(Clone, Copy, PartialEq, Eq, Default)]
struct SnapshotRef {
    snapshot_len: SnapshotLookupId,
    freelist_id: SnapshotLookupId,
}

type SnapshotEntry = u32;
const SNAPSHOT_REFCOUNT_OFFSET: usize = 0;
const SNAPSHOT_ACTOR_COUNT_OFFSET: usize = 1;
const SNAPSHOT_FREELISTS_MIN_LEN: usize = 1;
const SNAPSHOT_PREFIX_LEN: usize = 2;
// snapshots are a list of `SnapshotEntry`s where
// - the first one is the refcount,
// - the second one is the number of actors present at snapshot creation
// - all subsequent ones are the current action group id of the actors in
//   the order that would be returned by
//   Pow2LookupStepsIter::new(
//       <`ActorId` containing the snapshot>,
//       <number of actors present at snapshot creation>
//   )
const_assert!(
    size_of::<SnapshotEntry>() == size_of::<ActorId>()
        && size_of::<SnapshotEntry>() == size_of::<ActionGroupId>() //
);

struct ActionGroup {
    start: ActionId,
    length: ActionId,
}

struct ActionGroupWithRefs {
    ag: ActionGroup,
    // initialized as usize::MAX, wrapping add, saturating sub
    refcount: u32,
    next_action_group_id_self: ActionGroupId,
    next_action_group_id_succ: ActionGroupId,
}

struct ActionGroupMerges {
    action_groups: OffsetVecDeque<ActionGroupId, ActionGroup>,
    actions: OffsetVecDeque<ActionId, FieldAction>,
}

#[derive(Default)]
struct ActionGroupQueue {
    action_groups: OffsetVecDeque<ActionGroupId, ActionGroupWithRefs>,
    actions: OffsetVecDeque<ActionId, FieldAction>,
    dirty: bool,
    refcount: u32,
}

#[derive(Default)]
struct Actor {
    action_group_queues: Vec<ActionGroupQueue>,
    merges: Vec<ActionGroupMerges>,
    subscribers: Vec<FieldId>,
    latest_snapshot: SnapshotRef,
}

#[derive(Clone, Copy)]
enum ActionGroupIdentifier {
    Regular {
        actor_id: u32,
        pow2: u8,
        start: usize,
        length: usize,
    },
    LocalMerge {
        actor_id: u32,
        merge_pow2: u8,
        start: usize,
        length: usize,
    },
    TempBuffer {
        idx: usize,
        start: usize,
        length: usize,
    },
}

impl ActionGroupIdentifier {
    fn temp_idx(&self) -> Option<usize> {
        match self {
            ActionGroupIdentifier::Regular { .. } => None,
            ActionGroupIdentifier::LocalMerge { .. } => None,
            ActionGroupIdentifier::TempBuffer { idx, .. } => Some(*idx),
        }
    }
}

pub struct ActionBuffer {
    actors: OffsetVecDeque<ActorId, Actor>,
    // we need 3 temp buffers in order to always have a free one as a target
    // when merging from two others
    action_temp_buffers: [Vec<FieldAction>; 3],
    pending_action_group_actor_id: Option<ActorId>,
    pending_action_group_action_count: usize,
    snapshot_freelists:
        Vec<DynamicArrayFreelist<SnapshotLookupId, SnapshotEntry>>,
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

impl ActionBuffer {
    pub fn begin_action_group(&mut self, actor_id: u32) {
        assert!(self.pending_action_group_actor_id.is_none());
        self.pending_action_group_actor_id = Some(actor_id)
    }
    pub fn end_action_group(&mut self) {
        let ai = self.pending_action_group_actor_id.take().unwrap();
        let action_count = self.pending_action_group_action_count;
        if action_count == 0 {
            return;
        }
        self.pending_action_group_action_count = 0;
        let mut agq = &mut self.actors[ai].action_group_queues[0];
        let actions_start =
            agq.actions.next_free_index().wrapping_sub(action_count);
        #[cfg(feature = "debug_logging")]
        {
            println!(
                "ai {}: added ag {}:",
                ai,
                agq.action_groups.next_free_index()
            );
            for a in agq.actions.data.range(actions_start..) {
                println!("   > {:?}:", a);
            }
        }
        let next_action_group_id_succ = if ai != self.actors.max_index() {
            let next_agq = &self.actors[ai].action_group_queues[0];

            next_agq.action_groups.next_free_index() as ActionGroupId
        } else {
            0
        };
        agq = &mut self.actors[ai].action_group_queues[0];
        agq.action_groups.data.push_back(ActionGroupWithRefs {
            ag: ActionGroup {
                start: agq.actions.next_free_index(),
                length: action_count,
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
        ) {
            let actor = &mut self.actors[i];
            let agq = &mut actor.action_group_queues[pow2 as usize];
            if agq.dirty {
                break;
            }
            if pow2 > 0 {
                agq.dirty = true;
            }
        }
    }
    pub fn push_action(
        &mut self,
        kind: FieldActionKind,
        field_idx: usize,
        mut run_length: usize,
    ) {
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
    pub fn add_actor(&mut self, initial_subscriber: FieldId) -> ActorId {
        let actor_id = self.actors.next_free_index() as SnapshotEntry;
        let (ss_id, ss) = self.snapshot_freelists[0].claim();
        ss[0] = 1; // refcount
        ss[1] = actor_id; // actor count
        ss[2] = 0; // next action group id
        let mut actor = Actor {
            action_group_queues: Vec::new(),
            merges: Vec::new(),
            subscribers: vec![initial_subscriber],
            latest_snapshot: SnapshotRef {
                snapshot_len: 1,
                freelist_id: ss_id,
            },
        };
        actor.action_group_queues.push(ActionGroupQueue::default());
        self.actors.data.push_back(actor);
        if actor_id != self.actors.offset {
            let pow2_to_add = actor_id.trailing_zeros() + 1;
            let tgt_actor_id = actor_id - (1 << pow2_to_add);
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
        actor_id
    }
    fn release_temp_action_group(
        &mut self,
        ag: Option<ActionGroupIdentifier>,
    ) {
        if let Some(ActionGroupIdentifier::TempBuffer { idx, start, length }) =
            ag
        {
            let tb = &mut self.action_temp_buffers[idx];
            debug_assert!(tb.len() == start + length);
            tb.truncate(tb.len() - length);
        }
    }
    fn get_free_temp_idx(
        &self,
        lhs: &ActionGroupIdentifier,
        rhs: &ActionGroupIdentifier,
    ) -> usize {
        let lhs_idx = lhs.temp_idx().unwrap_or(3);
        let rhs_idx = rhs.temp_idx().unwrap_or(3);
        (6 - lhs_idx - rhs_idx) % 3
    }
    fn get_action_group_slices(
        &self,
        ag: ActionGroupIdentifier,
    ) -> (&[FieldAction], &[FieldAction]) {
        match ag {
            ActionGroupIdentifier::Regular {
                actor_id,
                pow2,
                mut start,
                length,
            } => {
                let actor = &self.actors[actor_id];
                let agq = &actor.action_group_queues[pow2 as usize];
                let (s1, s2) = agq.actions.data.as_slices();
                start = start.wrapping_sub(agq.actions.offset);
                subslice_slice_pair(s1, s2, start..start + length)
            }
            ActionGroupIdentifier::LocalMerge {
                actor_id,
                merge_pow2,
                mut start,
                length,
            } => {
                let actor = &self.actors[actor_id];
                let merge_lvl = &actor.merges[merge_pow2 as usize];
                start = start.wrapping_sub(merge_lvl.actions.offset);
                let (s1, s2) = merge_lvl.actions.data.as_slices();
                subslice_slice_pair(s1, s2, start..start + length)
            }
            ActionGroupIdentifier::TempBuffer { idx, start, length } => {
                (&[], &self.action_temp_buffers[idx][start..start + length])
            }
        }
    }
    fn action_group_not_from_actor_pow2(
        &self,
        actor_id: ActorId,
        pow2: u8,
        ag: &ActionGroupIdentifier,
    ) -> bool {
        match ag {
            ActionGroupIdentifier::Regular {
                actor_id: ag_actor_id,
                pow2: ag_pow2,
                ..
            } => *ag_actor_id != actor_id || *ag_pow2 != pow2,
            ActionGroupIdentifier::LocalMerge { .. } => true,
            ActionGroupIdentifier::TempBuffer { .. } => true,
        }
    }
    fn append_action_group(
        &mut self,
        actor_id: ActorId,
        pow2: u8,
        next_self: ActionGroupId,
        next_succ: ActionGroupId,
        ag: ActionGroupIdentifier,
    ) -> ActionGroupIdentifier {
        let (s1, s2) = self.get_action_group_slices(ag);
        let length = s1.len() + s2.len();
        assert!(self.action_group_not_from_actor_pow2(actor_id, pow2, &ag));
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
            ag: ActionGroup { start, length },
            refcount: 0,
            next_action_group_id_self: next_self,
            next_action_group_id_succ: next_succ,
        });
        ActionGroupIdentifier::Regular {
            actor_id,
            pow2,
            start,
            length,
        }
    }
    fn merge_action_groups_into_actor_action_group(
        &mut self,
        lhs: Option<ActionGroupIdentifier>,
        rhs: Option<ActionGroupIdentifier>,
        actor_id: ActorId,
        pow2: u8,
        next_self: ActionGroupId,
        next_succ: ActionGroupId,
    ) -> Option<ActionGroupIdentifier> {
        if let Some(lhs) = lhs {
            if let Some(rhs) = rhs {
                let (l1, l2) = self.get_action_group_slices(lhs);
                let (r1, r2) = self.get_action_group_slices(rhs);
                assert!(self
                    .action_group_not_from_actor_pow2(actor_id, pow2, &lhs));
                assert!(self
                    .action_group_not_from_actor_pow2(actor_id, pow2, &rhs));
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
                let start = len_after - len_before;
                let length = len_after - len_before;
                agq.action_groups.data.push_back(ActionGroupWithRefs {
                    ag: ActionGroup { start, length },
                    refcount: 0,
                    next_action_group_id_self: next_self,
                    next_action_group_id_succ: next_succ,
                });
                return Some(ActionGroupIdentifier::Regular {
                    actor_id,
                    pow2,
                    start,
                    length,
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
        lhs: Option<ActionGroupIdentifier>,
        rhs: Option<ActionGroupIdentifier>,
    ) -> Option<ActionGroupIdentifier> {
        if let Some(lhs) = lhs {
            if let Some(rhs) = rhs {
                let (l1, l2) = self.get_action_group_slices(lhs);
                let (r1, r2) = self.get_action_group_slices(rhs);
                let idx = self.get_free_temp_idx(&lhs, &rhs);
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
                return Some(ActionGroupIdentifier::TempBuffer {
                    idx,
                    start,
                    length,
                });
            }
            return Some(lhs);
        }
        rhs
    }
    fn merge_action_groups_of_single_pow2(
        &mut self,
        actor_id: ActorId,
        pow2: u8,
        prev: ActionGroupId,
        new: ActionGroupId,
    ) -> Option<ActionGroupIdentifier> {
        // TODO: use local merges if this is the hightest pow2
        let mut curr_ag_id = prev.wrapping_add(1);
        let mut res = None;
        loop {
            if curr_ag_id == new {
                return res;
            }
            let actor = &mut self.actors[actor_id];
            let agq = &mut actor.action_group_queues[pow2 as usize];
            let ag = &mut agq.action_groups[curr_ag_id];

            let curr = Some(ActionGroupIdentifier::Regular {
                actor_id,
                pow2,
                start: ag.ag.start,
                length: ag.ag.length,
            });
            let res_new = self.merge_action_groups_into_temp_buffer(res, curr);
            self.release_temp_action_group(res);
            res = res_new;
            curr_ag_id = curr_ag_id.wrapping_add(1);
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
        let (prev_self, prev_succ) = agq
            .action_groups
            .data
            .back()
            .map(|ag| {
                (ag.next_action_group_id_self, ag.next_action_group_id_succ)
            })
            .unwrap_or((0, 0));
        debug_assert!(pow2 > 0);

        let next_self = self.refresh_action_group(actor_id, pow2 - 1);
        let self_merge = self.merge_action_groups_of_single_pow2(
            actor_id, pow2, prev_self, next_self,
        );

        let succ_id = actor_id + (1 << pow2);
        let succ_pow2 = pow2 - 1;
        let next_succ = self.refresh_action_group(succ_id, succ_pow2);
        let succ_merge = self.merge_action_groups_of_single_pow2(
            succ_id, succ_pow2, prev_succ, next_succ,
        );

        self.merge_action_groups_into_actor_action_group(
            self_merge, succ_merge, actor_id, pow2, next_self, next_succ,
        );
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
        let (freelist_id, ss) = self.snapshot_freelists
            [snapshot_len - SNAPSHOT_FREELISTS_MIN_LEN]
            .claim();
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
    fn validate_snapshot(
        &self,
        actor_id: ActorId,
        snapshot: SnapshotRef,
    ) -> bool {
        let mut snapshot_valid = true;
        let ss = &self.snapshot_freelists[snapshot.snapshot_len as usize - 1]
            [snapshot.freelist_id];
        let actor_count = ss[SNAPSHOT_ACTOR_COUNT_OFFSET];
        if actor_count != self.actors.next_free_index() as SnapshotEntry {
            return false;
        }
        for (i, (actor_id, pow2)) in
            Pow2LookupStepsIter::new(actor_id, actor_count).enumerate()
        {
            let next_ag = self.refresh_action_group(actor_id, pow2);
            snapshot_valid &= ss[SNAPSHOT_PREFIX_LEN + i] == next_ag;
        }
        snapshot_valid
    }
    fn drop_snapshot_refcount(&mut self, snapshot: SnapshotRef, drop: u32) {
        let ss_rc = &mut self.snapshot_freelists
            [snapshot.snapshot_len as usize - SNAPSHOT_FREELISTS_MIN_LEN]
            [snapshot.freelist_id][SNAPSHOT_REFCOUNT_OFFSET];
        *ss_rc -= drop;
        if *ss_rc == 0 {
            self.snapshot_freelists
                [snapshot.snapshot_len as usize - SNAPSHOT_FREELISTS_MIN_LEN]
                .release(snapshot.freelist_id);
        }
    }
    fn apply_from_snapshot_same_actor_count(
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
                [ssr.snapshot_len as usize - SNAPSHOT_FREELISTS_MIN_LEN]
                [ssr.freelist_id];
            let prev = ss[i - SNAPSHOT_PREFIX_LEN];
            let next = self.actors[ai].action_group_queues[pow2 as usize]
                .action_groups
                .next_free_index();
            let next_ag =
                self.merge_action_groups_of_single_pow2(ai, pow2, prev, next);
            res = self.merge_action_groups_into_temp_buffer(res, next_ag);
        }
        res
    }
    fn apply_from_snapshot_different_actor_count(
        &mut self,
        actor_id: ActorId,
        ssr: SnapshotRef,
    ) -> Option<ActionGroupIdentifier> {
        let ss = &self.snapshot_freelists
            [ssr.snapshot_len as usize - SNAPSHOT_FREELISTS_MIN_LEN]
            [ssr.freelist_id];
        let mut prev_ss_actor_iter = Pow2LookupStepsIter::new(
            actor_id,
            ss[SNAPSHOT_ACTOR_COUNT_OFFSET],
        );
        let mut iter =
            Pow2LookupStepsIter::new(actor_id, self.actors.next_free_index());
        let mut i = 0;
        let mut res = None;
        loop {
            prev = i += 1;
        }
    }

    #[allow(unused)]
    fn update(
        &mut self,
        actor_id: ActorId,
        snapshot_ref: SnapshotRef,
    ) -> (SnapshotRef, Option<ActionGroupIdentifier>) {
        let actor = &self.actors[actor_id];
        let latest_snapshot_valid =
            self.validate_snapshot(actor_id, actor.latest_snapshot);
        let refers_to_latest_snapshot = snapshot_ref == actor.latest_snapshot;
        if latest_snapshot_valid && refers_to_latest_snapshot {
            return (snapshot_ref, None);
        }
        let current = if latest_snapshot_valid {
            let ss = self.actors[actor_id].latest_snapshot;
            self.snapshot_freelists
                [ss.snapshot_len as usize - SNAPSHOT_FREELISTS_MIN_LEN]
                [ss.freelist_id][SNAPSHOT_REFCOUNT_OFFSET] += 1;
            ss
        } else {
            let ss = self.generate_snapshot(actor_id, 2);
            self.actors[actor_id].latest_snapshot = ss;
            ss
        };
        let ss_actor_count = self.snapshot_freelists
            [snapshot_ref.snapshot_len as usize - SNAPSHOT_FREELISTS_MIN_LEN]
            [snapshot_ref.freelist_id][SNAPSHOT_ACTOR_COUNT_OFFSET];
        let diff_actions = if ss_actor_count == self.actors.next_free_index() {
            self.apply_from_snapshot_same_actor_count(actor_id, snapshot_ref)
        } else {
            self.apply_from_snapshot_different_actor_count(
                actor_id,
                snapshot_ref,
            )
        };
        self.drop_snapshot_refcount(snapshot_ref, 1);
        (current, None)
    }
}

#[cfg(test)]
mod test {
    use crate::record_data::command_buffer_v2::{
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
