use std::fmt::Debug;

use super::{
    field::{Field, FieldId, FieldManager},
    field_action::{
        merge_action_lists, FieldAction, FieldActionKind, PendingAction,
    },
    field_data::{RunLength, RUN_LEN_MAX_USIZE},
    group_track::GroupTrackId,
    iter_hall_action_applicator::IterHallActionApplicator,
    match_set::{MatchSetId, MatchSetManager},
};

use indexland::index_newtype;

use indexland::{
    Idx, index_slice::IndexSlice, index_vec::IndexVec,
    offset_vec_deque::OffsetVecDeque, subslice_slice_pair,
};

use crate::utils::launder_slice;

index_newtype! {
    pub struct ActorId(u16);
    pub struct SnapshotRef(pub(crate) ActionGroupId);

    pub struct ActionGroupId(u32);

    pub struct ActionGroupDataOffset(u32);

    struct TempBufferIndex(u32);
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
        idx: TempBufferIndex,
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

    // dear users, I can't be bothered to figure out
    // a decent merge strategy scheme right now, so we track at runtime.
    // please forgive me or send a PR
    free_temp_buffers: Vec<TempBufferIndex>,
    action_temp_buffers: IndexVec<TempBufferIndex, Vec<FieldAction>>,
    merges: Vec<ActionGroupIdentifier>,

    latest_snapshot_ref_count: SnapshotRefCount,

    // intermediate memory for action merging, needed by `merge_action_lists`
    pending_actions: Vec<PendingAction>,
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
    pub fn get_id(&self) -> ActorId {
        match *self {
            ActorRef::Present(id) | ActorRef::Unconfirmed(id) => id,
        }
    }
    pub fn unwrap(&self) -> ActorId {
        self.confirmed_id().unwrap()
    }
}

impl ActionGroupLocation {
    fn temp_idx(&self) -> Option<TempBufferIndex> {
        match self {
            ActionGroupLocation::Regular { .. } => None,
            ActionGroupLocation::TempBuffer { idx, .. } => Some(*idx),
        }
    }

    fn is_emtpy(
        &self,
        action_temp_buffers: &IndexSlice<TempBufferIndex, Vec<FieldAction>>,
    ) -> bool {
        match self {
            ActionGroupLocation::Regular {
                actions_begin,
                actions_end,
            } => actions_begin == actions_end,
            &ActionGroupLocation::TempBuffer { idx } => {
                action_temp_buffers[idx].is_empty()
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
            FieldActionKind::Dup | FieldActionKind::InsertZst { .. } => {
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
            action_temp_buffers: IndexVec::new(),
            free_temp_buffers: Vec::new(),
            pending_action_group_actor_id: None,
            pending_actions_start: ActionGroupDataOffset::ZERO,
            pending_action_group_field_count_delta: 0,
            iter_hall_action_applicator: IterHallActionApplicator::default(),
            action_groups: OffsetVecDeque::default(),
            action_group_data: OffsetVecDeque::default(),
            merges: Vec::new(),
            latest_snapshot_ref_count: 0,
            pending_actions: Vec::new(),
        }
    }
    pub fn begin_action_group(&mut self, actor_id: ActorId) {
        debug_assert!(self.pending_action_group_actor_id.is_none());
        debug_assert!(self.pending_action_group_field_count_delta == 0);
        self.pending_action_group_actor_id = Some(actor_id);
        self.pending_actions_start = self.action_group_data.next_free_index();
    }
    pub fn pending_action_group_id(&self) -> Option<ActorId> {
        self.pending_action_group_actor_id
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
        // TODO: on 32 bit, consider checked adds?
        let rl_delta = isize::try_from(run_length).unwrap();
        match kind {
            FieldActionKind::InsertZst { repr: _, actor_id } => {
                debug_assert_eq!(
                    Some(actor_id),
                    self.pending_action_group_actor_id
                );
                self.pending_action_group_field_count_delta += rl_delta
            }
            FieldActionKind::Dup => {
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
            let rl_to_push = run_length.min(RUN_LEN_MAX_USIZE) as RunLength;
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
        if let ActionGroupLocation::TempBuffer { idx } = agi.location {
            self.release_temp_buffer_index(idx);
        }
    }
    fn get_action_group_slices_raw<'a>(
        field_action_data: &'a OffsetVecDeque<
            ActionGroupDataOffset,
            FieldAction,
        >,
        action_temp_buffers: &'a IndexSlice<TempBufferIndex, Vec<FieldAction>>,
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
                (&action_temp_buffers[idx], &[])
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

    fn claim_temp_buffer_index(&mut self) -> TempBufferIndex {
        if let Some(idx) = self.free_temp_buffers.pop() {
            return idx;
        }
        let idx = TempBufferIndex::from_usize(self.action_temp_buffers.len());
        self.action_temp_buffers.push(Vec::new());
        idx
    }
    fn release_temp_buffer_index(&mut self, idx: TempBufferIndex) {
        self.action_temp_buffers[idx].clear();
        self.free_temp_buffers.push(idx);
    }

    fn merge_action_groups_into_temp_buffer(
        &mut self,
        lhs: &ActionGroupIdentifier,
        rhs: &ActionGroupIdentifier,
        temp_idx: TempBufferIndex,
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

            let buff = &mut self.action_temp_buffers[temp_idx];
            debug_assert!(buff.is_empty());
            merge_action_lists(
                l1.iter().chain(l2),
                r1.iter().chain(r2),
                &mut self.pending_actions,
                buff,
            );
        };
        let res = ActionGroupIdentifier {
            location: ActionGroupLocation::TempBuffer { idx: temp_idx },
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
    ) -> ActionGroupIdentifier {
        if rhs.location.is_emtpy(&self.action_temp_buffers) {
            self.release_temp_action_group(rhs);
            return *lhs;
        }
        if lhs.location.is_emtpy(&self.action_temp_buffers) {
            self.release_temp_action_group(lhs);
            return *rhs;
        }
        let temp_idx = self.claim_temp_buffer_index();
        let res =
            self.merge_action_groups_into_temp_buffer(lhs, rhs, temp_idx);
        self.release_temp_action_group(rhs);
        self.release_temp_action_group(lhs);
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
            self.drop_snapshot_refcount(field_snapshot);
            return;
        };

        let (s1, s2) = Self::get_action_group_slices_raw(
            &self.action_group_data,
            &self.action_temp_buffers,
            &agi,
        );
        let Some(first_action) = s1.first().or_else(|| s2.first()) else {
            self.release_temp_action_group(&agi);
            self.drop_snapshot_refcount(field_snapshot);
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
            *first_action,
        );
        self.release_temp_action_group(&agi);
        self.drop_snapshot_refcount(field_snapshot);
    }

    pub fn field_has_pending_actions(
        &self,
        fm: &FieldManager,
        field_id: FieldId,
    ) -> bool {
        let field = fm.fields[field_id].borrow();
        let field_snapshot = field.snapshot.get();

        let Some(field_actor_id) =
            self.get_actor_id_from_ref(field.first_actor.get())
        else {
            return false;
        };

        let latest_snapshot = self.get_latest_snapshot();

        if latest_snapshot == field_snapshot {
            return false;
        }

        for ag in self.action_groups.range(field_snapshot.into_inner()..) {
            if ag.actor_id.into_usize() >= field_actor_id.into_usize() {
                return true;
            }
        }
        false
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
        debug_assert_eq!(
            self.free_temp_buffers.len(),
            self.action_temp_buffers.len()
        );
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
        let mut aggregated_count = 1 << highest_pow2;

        let mut pow2 = highest_pow2;
        let mut lhs = self.merges[highest_pow2];
        while pow2 != 0 {
            pow2 -= 1;
            let pow2_count = 1 << pow2;
            if aggregated_count + pow2_count > count {
                continue;
            }
            aggregated_count += pow2_count;
            let rhs = self.merges[pow2];
            lhs = self.merge_action_groups_into_temp_buffer_release_inputs(
                &lhs, &rhs,
            );
        }

        self.merges.clear();

        if cfg!(debug_assertions) {
            for (i, b) in self.action_temp_buffers.iter_enumerated() {
                if Some(i) != lhs.location.temp_idx() {
                    debug_assert!(b.is_empty());
                }
            }
            debug_assert!(
                self.free_temp_buffers.len() + 1
                    >= self.action_temp_buffers.len()
            );
        }

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
    pub fn action_group_count(&self) -> usize {
        self.action_groups.len()
    }
}

#[cfg(test)]
mod test {
    use std::iter;

    use rstest::rstest;

    use crate::record_data::{
        action_buffer::{ActorId, ActorRef},
        field_action::{FieldAction, FieldActionKind},
        field_data::RunLength,
        match_set::MatchSetId,
    };
    use indexland::Idx;

    use super::ActionBuffer;

    #[track_caller]
    fn test_action_merge(
        ag_actors: impl IntoIterator<Item = usize>,
        ags: impl IntoIterator<Item = impl IntoIterator<Item = FieldAction>>,
        actor: usize,
        output: impl IntoIterator<Item = FieldAction>,
    ) {
        let mut ab = ActionBuffer::new(MatchSetId::ZERO);
        let initial_snapshot = ab.get_latest_snapshot();
        let actors = ag_actors.into_iter().collect::<Vec<_>>();
        for a in actors.iter().copied().chain(iter::once(actor)) {
            while a >= ab.actors.len() {
                ab.add_actor();
            }
        }
        let action_groups = ags
            .into_iter()
            .map(|ag| ag.into_iter().collect::<Vec<_>>())
            .collect::<Vec<_>>();
        assert_eq!(action_groups.len(), actors.len());
        for (&a, ag) in actors.iter().zip(&action_groups) {
            ab.begin_action_group(ActorId::from_usize(a));
            for a in ag {
                ab.push_action(a.kind, a.field_idx, a.run_len as usize);
            }
            ab.end_action_group();
        }
        let output = output.into_iter().collect::<Vec<_>>();
        let mut pending = Vec::new();
        ab.gather_pending_actions(
            &mut pending,
            ActorRef::Present(ActorId::from_usize(actor)),
            initial_snapshot, // TODO: maybe test other variants here
        );
        assert_eq!(pending, output);
    }

    #[test]
    fn simple_merge() {
        test_action_merge(
            [0, 0],
            [
                [FieldAction::new(FieldActionKind::Dup, 0, 2)],
                [FieldAction::new(FieldActionKind::Drop, 0, 1)],
            ],
            0,
            [FieldAction::new(FieldActionKind::Dup, 0, 1)],
        )
    }

    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[case(7)]
    #[case(8)]
    #[case(9)]
    #[case(10)]
    #[case(11)]
    #[case(12)]
    #[case(41)]
    fn merge_n(#[case] count: usize) {
        test_action_merge(
            std::iter::repeat(0).take(count),
            std::iter::repeat([FieldAction::new(FieldActionKind::Dup, 0, 1)])
                .take(count),
            0,
            [FieldAction::new(
                FieldActionKind::Dup,
                0,
                count as RunLength,
            )],
        )
    }
}
