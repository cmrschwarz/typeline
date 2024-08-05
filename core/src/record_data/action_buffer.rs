use std::fmt::Debug;

use super::{
    field::{Field, FieldId, FieldManager},
    field_action::{merge_action_lists, FieldAction, FieldActionKind},
    field_data::RunLength,
    group_track::GroupTrackId,
    iter_hall_action_applicator::IterHallActionApplicator,
    match_set::{MatchSetId, MatchSetManager},
};
use crate::{
    index_newtype,
    utils::{
        indexing_type::IndexingType, launder_slice,
        offset_vec_deque::OffsetVecDeque, subslice_slice_pair,
    },
};

index_newtype! {
    pub struct ActorId(u32);
    pub struct SnapshotRef(ActionGroupId);

    pub struct ActionGroupId(u32);

    pub struct ActionGroupDataOffset(u32);
}

pub type SnapshotRefCount = u32;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ActorSubscriber {
    Field(FieldId),
    GroupTrack(GroupTrackId),
}

#[derive(Clone, Copy)]
pub enum ActorRef {
    Present(ActorId),
    // when a transform adds an actor, it sets the first_actor of it's
    // output fields to Unconfirmed()
    Unconfirmed(ActorId),
}

#[derive(Default)]
struct Actor {
    subscribers: Vec<ActorSubscriber>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ActionGroupLocation {
    Regular {
        actions_begin: ActionGroupDataOffset,
        actions_end: ActionGroupDataOffset,
    },
    TempBuffer {
        idx: u32,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ActionGroupIdentifier {
    location: ActionGroupLocation,
    field_count_delta: isize,
}

pub struct ActionGroup {
    actions_begin: ActionGroupDataOffset,
    actions_end: ActionGroupDataOffset,
    actor_id: ActorId,
    refcount: SnapshotRefCount,
    field_count_delta: isize,
}

pub struct ActionBuffer {
    #[allow(unused)]
    #[cfg(feature = "debug_state")]
    pub(crate) match_set_id: MatchSetId,
    actors: OffsetVecDeque<ActorId, Actor>,
    action_groups: OffsetVecDeque<ActionGroupId, ActionGroup>,
    action_group_data: OffsetVecDeque<ActionGroupDataOffset, FieldAction>,

    pending_action_group_actor_id: Option<ActorId>,
    pending_actions_start: ActionGroupDataOffset,
    pending_action_group_field_count_delta: isize,

    iter_hall_action_applicator: IterHallActionApplicator,

    action_temp_buffers: Vec<Vec<FieldAction>>,
    merges: Vec<ActionGroupIdentifier>,

    latest_snapshot_ref_count: SnapshotRefCount,
}

impl Default for ActorRef {
    fn default() -> Self {
        ActorRef::Unconfirmed(ActorId::ZERO)
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

impl ActionGroupLocation {
    fn temp_idx(&self) -> Option<usize> {
        match self {
            ActionGroupLocation::Regular { .. } => None,
            ActionGroupLocation::TempBuffer { idx, .. } => Some(*idx as usize),
        }
    }

    fn is_emtpy(&self, action_temp_buffers: &[Vec<FieldAction>]) -> bool {
        match self {
            ActionGroupLocation::Regular {
                actions_begin,
                actions_end,
            } => actions_begin == actions_end,
            ActionGroupLocation::TempBuffer { idx } => {
                action_temp_buffers[*idx as usize].is_empty()
            }
        }
    }
}

pub fn eprint_action_list(actions: impl Iterator<Item = FieldAction>) {
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
    pub const MAX_ACTOR_ID: ActorId = ActorId::MAX_VALUE;

    pub fn new(
        #[cfg_attr(not(feature = "debug_state"), allow(unused))]
        ms_id: MatchSetId,
    ) -> Self {
        Self {
            #[cfg(feature = "debug_state")]
            match_set_id: ms_id,
            actors: OffsetVecDeque::default(),
            action_temp_buffers: Vec::new(),
            pending_action_group_actor_id: None,
            pending_actions_start: ActionGroupDataOffset::ZERO,
            pending_action_group_field_count_delta: 0,
            iter_hall_action_applicator: IterHallActionApplicator::default(),
            action_groups: OffsetVecDeque::default(),
            action_group_data: OffsetVecDeque::default(),
            merges: Vec::new(),
            latest_snapshot_ref_count: 0,
        }
    }
    pub fn begin_action_group(&mut self, actor_id: ActorId) {
        debug_assert!(self.pending_action_group_actor_id.is_none());
        debug_assert!(self.pending_action_group_field_count_delta == 0);
        self.pending_action_group_actor_id = Some(actor_id);
        self.pending_actions_start = self.action_group_data.next_free_index();
    }

    pub fn end_action_group(&mut self) {
        let actor_id = self.pending_action_group_actor_id.take().unwrap();
        let field_count_delta = self.pending_action_group_field_count_delta;
        let pending_actions_end = self.action_group_data.next_free_index();
        if pending_actions_end == self.pending_actions_start {
            return;
        }
        self.action_groups.push_back(ActionGroup {
            actions_begin: self.pending_actions_start,
            actions_end: pending_actions_end,
            actor_id,
            refcount: self.latest_snapshot_ref_count,
            field_count_delta,
        });
        self.latest_snapshot_ref_count = 0;
        self.pending_action_group_field_count_delta = 0;
        #[cfg(feature = "debug_logging_field_action_groups")]
        {
            let action_list_index = self.action_groups.last_used_index();
            eprintln!(
                "added action group {}, ms {}, actor {actor_id}:",
                action_list_index, self.match_set_id,
            );
            eprint_action_list(
                self.action_group_data
                    .range(self.pending_actions_start..pending_actions_end)
                    .copied(),
            );
        }
    }
    pub fn push_action(
        &mut self,
        kind: FieldActionKind,
        field_idx: usize,
        mut run_length: usize,
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
        if self.pending_actions_start
            != self.action_group_data.next_free_index()
        {
            let last = self.action_group_data.back_mut().unwrap();
            // field indices in action groups must be ascending
            debug_assert!(last.field_idx <= field_idx);
            // Very simple early merging of actions to hopefully save some
            // memory. This also allows operations to be slightly
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
            self.action_group_data.push_back(FieldAction {
                kind,
                field_idx,
                run_len: rl_to_push,
            });
            run_length -= rl_to_push as usize;
        }
    }
    pub fn add_actor(&mut self) -> ActorId {
        let actor_id = self.actors.next_free_index();
        self.actors.push_back(Actor::default());
        actor_id
    }
    pub fn last_valid_actor_id(&self) -> ActorId {
        self.actors.last_used_index()
    }
    pub fn peek_next_actor_id(&self) -> ActorId {
        self.actors.next_free_index()
    }
    pub fn last_actor_ref(&self) -> ActorRef {
        if self.actors.is_empty() {
            ActorRef::Unconfirmed(self.actors.next_free_index())
        } else {
            ActorRef::Present(self.actors.last_used_index())
        }
    }
    pub fn next_actor_ref(&self) -> ActorRef {
        ActorRef::Unconfirmed(self.actors.next_free_index())
    }
    pub(crate) fn release_temp_action_group(
        &mut self,
        agi: &ActionGroupIdentifier,
    ) {
        let ActionGroupLocation::TempBuffer { idx } = agi.location else {
            return;
        };
        let tb = &mut self.action_temp_buffers[idx as usize];
        tb.clear();
    }
    fn get_action_group_slices_raw<'a>(
        field_action_data: &'a OffsetVecDeque<
            ActionGroupDataOffset,
            FieldAction,
        >,
        action_temp_buffers: &'a [Vec<FieldAction>],
        agi: &ActionGroupIdentifier,
    ) -> (&'a [FieldAction], &'a [FieldAction]) {
        match agi.location {
            ActionGroupLocation::Regular {
                actions_begin,
                actions_end,
            } => {
                let start = field_action_data.index_to_phys(actions_begin);
                let end = field_action_data.index_to_phys(actions_end);
                subslice_slice_pair(field_action_data.as_slices(), start..end)
            }
            ActionGroupLocation::TempBuffer { idx } => {
                (&action_temp_buffers[idx as usize], &[])
            }
        }
    }
    pub(super) fn get_action_group_slices(
        &self,
        agi: &ActionGroupIdentifier,
    ) -> (&[FieldAction], &[FieldAction]) {
        Self::get_action_group_slices_raw(
            &self.action_group_data,
            &self.action_temp_buffers,
            agi,
        )
    }
    pub fn get_action_group_iter<'a>(
        &'a self,
        agi: &ActionGroupIdentifier,
    ) -> impl Iterator<Item = FieldAction> + Clone + 'a {
        let (s1, s2) = self.get_action_group_slices(agi);
        s1.iter().chain(s2.iter()).copied()
    }

    fn merge_action_groups_into_temp_buffer(
        &mut self,
        lhs: &ActionGroupIdentifier,
        rhs: &ActionGroupIdentifier,
        temp_idx: usize,
    ) -> ActionGroupIdentifier {
        let (l1, l2) = self.get_action_group_slices(lhs);
        let (r1, r2) = self.get_action_group_slices(rhs);

        #[cfg(feature = "debug_logging_field_action_group_accel")]
        {
            eprintln!("@ merging into temp:");
            eprintln!("  + {lhs:?}");
            eprint_action_list(l1.iter().chain(l2.iter()).copied());
            eprintln!("  + {rhs:?}");
            eprint_action_list(r1.iter().chain(r2.iter()).copied());
        }

        unsafe {
            assert!(lhs.location.temp_idx() != Some(temp_idx));
            assert!(rhs.location.temp_idx() != Some(temp_idx));
            // SAFETY: the asserts above make sure that
            // action_temp_buffers[temp_idx] is neither
            // used by lhs nor by rhs. Therefore we can take
            // a mutable reference to it
            let (l1, l2) = (launder_slice(l1), launder_slice(l2));
            let (r1, r2) = (launder_slice(r1), launder_slice(r2));

            if self.action_temp_buffers.len() <= temp_idx {
                self.action_temp_buffers.resize(temp_idx + 1, Vec::new());
            }

            let buff = &mut self.action_temp_buffers[temp_idx];
            debug_assert!(buff.is_empty());
            merge_action_lists(l1.iter().chain(l2), r1.iter().chain(r2), buff);
        };
        let res = ActionGroupIdentifier {
            location: ActionGroupLocation::TempBuffer {
                idx: temp_idx as u32,
            },
            field_count_delta: lhs.field_count_delta + rhs.field_count_delta,
        };
        #[cfg(feature = "debug_logging_field_action_group_accel")]
        {
            eprintln!(" -> {res:?}");
            eprint_action_list(self.get_action_group_iter(&res));
        }
        res
    }
    fn merge_action_groups_into_temp_buffer_release_inputs(
        &mut self,
        lhs: &ActionGroupIdentifier,
        rhs: &ActionGroupIdentifier,
        temp_idx: usize,
    ) -> ActionGroupIdentifier {
        if rhs.location.is_emtpy(&self.action_temp_buffers) {
            return *lhs;
        }
        if lhs.location.is_emtpy(&self.action_temp_buffers) {
            return *rhs;
        }
        let res =
            self.merge_action_groups_into_temp_buffer(lhs, rhs, temp_idx);
        self.release_temp_action_group(lhs);
        self.release_temp_action_group(rhs);
        res
    }

    pub(super) fn drop_snapshot_refcount(&mut self, snapshot: SnapshotRef) {
        if snapshot == self.get_latest_snapshot() {
            self.latest_snapshot_ref_count -= 1;
            return;
        }

        let ag_id = snapshot.into_inner();

        let ag = &mut self.action_groups[ag_id];

        ag.refcount -= 1;

        let mut actions_end = ag.actions_end;

        if ag.refcount > 0 || self.action_groups.offset() != ag_id {
            return;
        }

        let mut ag_pos = 1;

        while ag_pos < self.action_groups.len() {
            let ag = self.action_groups.get_phys(ag_pos);
            if ag.refcount != 0 {
                break;
            }
            actions_end = ag.actions_end;
            ag_pos += 1;
        }

        #[cfg(feature = "debug_logging_field_action_group_accel")]
        {
            eprintln!("@ dropping snapshots: action groups {} - {}, action offsets {} - {}",
                self.action_groups.index_from_phys(0),
                self.action_groups.index_from_phys(ag_pos),
                self.action_group_data.index_from_phys(0),
                actions_end
            )
        }

        self.action_groups.drop_front(ag_pos);
        self.action_group_data.drop_front_until(actions_end);
    }
    pub(super) fn bump_snapshot_refcount(&mut self, snapshot: SnapshotRef) {
        if snapshot == self.get_latest_snapshot() {
            self.latest_snapshot_ref_count += 1;
            return;
        }
        let ag = &mut self.action_groups[snapshot.into_inner()];
        ag.refcount += 1;
    }

    pub fn get_latest_snapshot(&self) -> SnapshotRef {
        SnapshotRef(self.action_groups.next_free_index())
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

        let latest_snapshot = self.get_latest_snapshot();

        if latest_snapshot == field_snapshot {
            return;
        }
        field.snapshot.set(latest_snapshot);
        self.bump_snapshot_refcount(latest_snapshot);
        drop(field);

        let res = self.build_actions_from_snapshot(actor_id, field_snapshot);

        let Some(agi) = res else {
            return;
        };

        let (s1, s2) = Self::get_action_group_slices_raw(
            &self.action_group_data,
            &self.action_temp_buffers,
            &agi,
        );
        let Some(first_action_index) =
            s1.first().or_else(|| s2.first()).map(|a| a.field_idx)
        else {
            return;
        };

        let actions = s1.iter().chain(s2.iter()).copied();

        self.iter_hall_action_applicator.apply_field_actions(
            fm,
            msm,
            field_id,
            actor_id,
            update_cow_ms,
            actions,
            agi.field_count_delta,
            first_action_index,
        );
        self.release_temp_action_group(&agi);
        self.drop_snapshot_refcount(field_snapshot);
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
        let mut first_actor = field.first_actor.get();
        let field_snapshot = field.snapshot.get();

        if self
            .initialize_actor_ref(
                Some(ActorSubscriber::Field(field_id)),
                &mut first_actor,
            )
            .is_none()
        {
            return;
        };
        field.first_actor.set(first_actor);

        let latest_snapshot = self.get_latest_snapshot();

        if latest_snapshot == field_snapshot {
            return;
        }
        #[cfg(feature = "debug_logging")]
        {
            eprintln!(
                "@ dropping actions for field {} (ms: {}, first actor: {})",
                field_id,
                self.match_set_id,
                first_actor.unwrap(),
            );
        }

        self.bump_snapshot_refcount(latest_snapshot);
        self.drop_snapshot_refcount(field_snapshot);

        field.snapshot.set(latest_snapshot);
    }

    pub(crate) fn build_actions_from_snapshot(
        &mut self,
        actor_id: ActorId,
        field_snapshot: SnapshotRef,
    ) -> Option<ActionGroupIdentifier> {
        let mut count = 0usize;
        let mut ag_idx = self
            .action_groups
            .index_to_phys(field_snapshot.into_inner());

        'find_groups: loop {
            let mut action_group;
            loop {
                if ag_idx == self.action_groups.len() {
                    break 'find_groups;
                }
                action_group = self.action_groups.get_phys(ag_idx);
                ag_idx += 1;
                if action_group.actor_id.into_usize() >= actor_id.into_usize()
                {
                    break;
                }
            }
            count += 1;
            let pow2 = count.trailing_zeros() as usize;

            let mut rhs = ActionGroupIdentifier {
                location: ActionGroupLocation::Regular {
                    actions_begin: action_group.actions_begin,
                    actions_end: action_group.actions_end,
                },
                field_count_delta: action_group.field_count_delta,
            };
            for i in 0..pow2 {
                let prev_merge = self.merges[i];
                rhs = self
                    .merge_action_groups_into_temp_buffer_release_inputs(
                        &prev_merge,
                        &rhs,
                        if i + 1 == pow2 { pow2 } else { i },
                    );
            }
            if self.merges.len() == pow2 {
                self.merges.push(rhs);
            } else {
                self.merges[pow2] = rhs;
            }
        }

        if count == 0 {
            return None;
        }

        let highest_pow2 = count.ilog2() as usize;
        debug_assert!(self.merges.len() == highest_pow2 + 1);
        let highest_pow2_count = 1 << highest_pow2;

        let mut pow2 = highest_pow2;
        let mut lhs = self.merges[highest_pow2];
        let mut temp_idx = highest_pow2 + 1;
        while pow2 != 0 {
            pow2 /= 2;
            if highest_pow2_count + (1 << pow2) > count {
                continue;
            }
            let rhs = self.merges[pow2];
            lhs = self.merge_action_groups_into_temp_buffer_release_inputs(
                &lhs, &rhs, temp_idx,
            );
            temp_idx -= 1;
        }

        if cfg!(debug_assertions) {
            for (i, b) in self.action_temp_buffers.iter().enumerate() {
                if Some(i) != lhs.location.temp_idx() {
                    debug_assert!(b.is_empty());
                }
            }
        }
        self.merges.clear();
        Some(lhs)
    }

    pub fn gather_pending_actions(
        &mut self,
        pending_actions: &mut Vec<FieldAction>,
        actor_ref: ActorRef,
        snapshot: SnapshotRef,
    ) {
        let Some(actor) = self.get_actor_id_from_ref(actor_ref) else {
            return;
        };
        let Some(agi) = self.build_actions_from_snapshot(actor, snapshot)
        else {
            return;
        };

        let (s1, s2) = self.get_action_group_slices(&agi);
        pending_actions.extend(s1);
        pending_actions.extend(s2);
        self.release_temp_action_group(&agi);
    }
}
