use std::{
    cell::RefMut,
    collections::HashMap,
    sync::{Arc, Mutex},
};

use bitvec::vec::BitVec;

use crate::{
    chain::{ChainId, SubchainIndex},
    cli::{
        call_expr::{Argument, CallExpr, Span},
        parse_operator_data, CliArgumentError,
    },
    context::SessionData,
    job::{add_transform_to_job, Job, JobData},
    liveness_analysis::{
        LivenessData, OperatorCallEffect, Var, VarId, VarLivenessSlotGroup,
        VarLivenessSlotKind, BB_INPUT_VAR_ID,
    },
    options::{
        chain_settings::SettingBatchSize, session_setup::SessionSetupData,
    },
    record_data::{
        action_buffer::{ActorId, ActorRef},
        field::{CowFieldDataRef, FieldId, FieldIterRef, FieldRefOffset},
        field_action::FieldActionKind,
        field_data::{FieldData, FieldValueRepr},
        field_data_ref::DestructuredFieldDataRef,
        field_value::FieldValue,
        group_track::{
            GroupTrack, GroupTrackId, GroupTrackIterMut, GroupTrackIterRef,
        },
        iter::{
            field_iter::FieldIter,
            field_iterator::{FieldIterRangeOptions, FieldIterator},
        },
        iter_hall::{FieldIterId, IterKind},
        match_set::MatchSetId,
        push_interface::PushInterface,
        varying_type_inserter::VaryingTypeInserter,
    },
    typeline_error::TypelineError,
};

use indexland::{
    index_slice::IndexSlice, index_vec::IndexVec, Idx, IdxNewtype,
    IndexRangeBounds,
};

use indexland_utils::{
    phantom_slot::PhantomSlot, stable_vec::StableVec,
    temp_vec::TransmutableContainer,
};

use super::{
    errors::OperatorSetupError,
    nop::create_op_nop,
    operator::{
        OffsetInChain, Operator, OperatorBase, OperatorDataId, OperatorId,
        OperatorInstantiation, OperatorOffsetInChain,
    },
    transform::{Transform, TransformId, TransformState},
};

// Forkcat: split up pipeline into multiple subchains, concatenate the results
// - create matchset for each subchain
// - create machset for the continuation
// - cow all fields accessed by each subchain (or the continuation) into each
//   subchain
// - create accessed fields in continuation that all cow a single field also in
//   the continuation that contains field refs
// - prebind outputs used in continuation for each subchain
// - create trailer tf for each subchain that does the following:
//      - check if current the current group of this sc is ready to be emitted
//        (have an Arc<Mutex> to figure that out), if no do nothing
//      - if yes:
//              - create / append group length
//              - append to pseudo data column, inform comsumers

#[derive(IdxNewtype)]
pub struct FcSubchainIdx(u32);

#[derive(IdxNewtype)]
pub struct FcSubchainRoundRobinIdx(u32);

#[derive(IdxNewtype)]
pub struct ContinuationVarIdx(u32);

#[derive(Default)]
pub struct ForkcatOpts {
    pub copy: bool,
}

pub struct OpForkCat {
    pub subchains: Vec<Vec<(Box<dyn Operator>, Span)>>,

    pub subchains_start: SubchainIndex,
    pub subchains_end: SubchainIndex,

    pub direct_offset_in_chain: OffsetInChain,
    // list of input vars needed in each subchain
    input_mappings: HashMap<Var, BitVec<usize>>,

    // var names accessed by continuation
    continuation_vars: IndexVec<ContinuationVarIdx, Var>,

    #[allow(unused)] // TODO
    opts: ForkcatOpts,
}

pub struct SubchainEntry {
    pub ms_id: MatchSetId,
    pub start_tf_id: TransformId,
    pub trailer_tf_id: TransformId,
    pub group_track_iter_ref: GroupTrackIterRef,
    pub actor_id: ActorId,
    pub input_done: bool,
    pub leading_padding_introduced: usize,
    pub fields_consumed: bool,
    pub batch_size_available: usize,
    pub continuation_field_mappings:
        IndexVec<ContinuationVarIdx, ContinuationFieldMapping>,

    field_iters_temp_slot: IndexVec<
        ContinuationVarIdx,
        PhantomSlot<FieldIter<DestructuredFieldDataRef<'static>>>,
    >,
}

pub struct TfForkCatHeader {
    pub continuation_state: Arc<Mutex<FcContinuationState>>,
    pub actor_id: ActorId,
    // we split this transform into two phases.
    // This is just to get separate debug log entries
    // and has no semantic benefits/consequences.
    pub relaunch_after_cow_advance: bool,
}

pub struct ContinuationFieldMapping {
    pub cont_field_id: FieldId,
    pub sc_field_id: FieldId,
    pub sc_field_iter_id: FieldIterId,
    pub sc_field_ref_offset_in_cont: FieldRefOffset,
    // start of the field_refs list from the field in the subchain in the
    // continuation field's field_refs list
    pub sc_field_refs_offsets_start_in_cont_field: FieldRefOffset,
}

pub struct FcContinuationState {
    pub continuation_tf_id: Option<TransformId>,
    pub continuation_input_group_track: GroupTrackId,
    pub continuation_ms_id: MatchSetId,
    pub continuation_dummy_iter: FieldIterRef,
    pub subchains: IndexVec<FcSubchainIdx, SubchainEntry>,
    pub advance_to_next: bool,
    pub current_sc: FcSubchainIdx,

    group_iters_temp: IndexVec<
        FcSubchainRoundRobinIdx,
        PhantomSlot<GroupTrackIterMut<'static, RefMut<'static, GroupTrack>>>,
    >,
    fields_temp: StableVec<PhantomSlot<CowFieldDataRef<'static>>>,
    field_iters_temp: IndexVec<
        FcSubchainRoundRobinIdx,
        IndexVec<
            ContinuationVarIdx,
            PhantomSlot<FieldIter<DestructuredFieldDataRef<'static>>>,
        >,
    >,

    cont_field_inserters: IndexVec<
        ContinuationVarIdx,
        PhantomSlot<VaryingTypeInserter<RefMut<'static, FieldData>>>,
    >,
}

pub struct TfForkCatSubchainTrailer<'a> {
    pub op: &'a OpForkCat,
    pub subchain_idx: FcSubchainIdx,
    // TODO: figure out a better mechanism for this, this is stupid
    // we can't use Arc<RefCell> because transforms need to be Send
    pub continuation_state: Arc<Mutex<FcContinuationState>>,
    // field ref offset of subchain field in continuation field (with id)

    // Same as TfForkCatHeader, just to get separate debug log entries.
    pub relaunch_for_cow_advance: bool,
}

fn sc_index_offset(
    fc_sc_count: usize,
    sc_idx: FcSubchainIdx,
    offset: isize,
) -> FcSubchainIdx {
    FcSubchainIdx::from_usize(
        ((fc_sc_count + sc_idx.into_usize()) as isize + offset) as usize
            % fc_sc_count,
    )
}

fn sc_next(fc_sc_count: usize, sc_idx: FcSubchainIdx) -> FcSubchainIdx {
    sc_index_offset(fc_sc_count, sc_idx, 1)
}

fn declare_var_needed_for_sc(
    op: &mut OpForkCat,
    ld: &LivenessData,
    var_id: VarId,
    sc_count: usize,
    sc_idx: usize,
) {
    op.input_mappings
        .entry(ld.vars[var_id])
        .or_insert_with(|| {
            let mut bv = BitVec::with_capacity(sc_count);
            bv.resize(sc_count, false);
            bv
        })
        .set(sc_idx, true);
}

fn setup_subchain<'a>(
    op: &'a OpForkCat,
    continuation_state: Arc<Mutex<FcContinuationState>>,
    cont_ms_id: MatchSetId,
    job: &mut Job<'a>,
    op_base: &OperatorBase,
    sc_idx: SubchainIndex,
    fc_sc_idx: FcSubchainIdx,
    fc_input_field: FieldId,
    fc_tf_id: TransformId,
    continuation_vars: &IndexSlice<ContinuationVarIdx, FieldId>,
) -> SubchainEntry {
    let fc_tf = &job.job_data.tf_mgr.transforms[fc_tf_id];
    let fc_ms_id = fc_tf.match_set_id;

    // TODO: scope setup
    let sc_scope = job.job_data.scope_mgr.add_scope(None);
    let sc_ms_id = job.job_data.match_set_mgr.add_match_set(
        &mut job.job_data.field_mgr,
        &mut job.job_data.scope_mgr,
        sc_scope,
    );

    let fc_dummy_field = job.job_data.match_set_mgr.get_dummy_field(fc_ms_id);
    let sc_dummy_field = job.job_data.match_set_mgr.get_dummy_field(sc_ms_id);

    job.job_data.field_mgr.setup_cow_between_fields(
        &mut job.job_data.match_set_mgr,
        fc_dummy_field,
        sc_dummy_field,
    );

    let group_track = job.job_data.group_track_manager.add_group_track(
        &job.job_data.match_set_mgr,
        None,
        sc_ms_id,
        ActorRef::Unconfirmed(ActorId::ZERO),
    );

    #[cfg(feature = "debug_state")]
    {
        job.job_data.group_track_manager.group_tracks[group_track]
            .borrow_mut()
            .alias_source = Some(fc_tf.output_group_track_id);
    }

    let sc_chain_id =
        job.job_data.session_data.chains[op_base.chain_id].subchains[sc_idx];

    let mut sc_input_field = sc_dummy_field;
    let fc_ms_scope =
        job.job_data.match_set_mgr.match_sets[fc_ms_id].active_scope;

    for (&var, needing_subchains) in &op.input_mappings {
        if !needing_subchains[fc_sc_idx.into_usize()] {
            continue;
        }

        let src_field = if let Some(name) = var.get_name() {
            job.job_data
                .scope_mgr
                .lookup_field(fc_ms_scope, name)
                .unwrap_or(sc_dummy_field)
        } else if matches!(var, Var::BBInput) {
            fc_input_field
        } else {
            fc_dummy_field
        };

        let field_id = job.job_data.field_mgr.get_cross_ms_cow_field(
            &mut job.job_data.match_set_mgr,
            sc_ms_id,
            src_field,
        );
        if var == Var::BBInput {
            sc_input_field = field_id;
        } else if let Some(name) = var.get_name() {
            job.job_data
                .scope_mgr
                .insert_field_name(sc_scope, name, field_id);
        }
    }

    let instantiation = job.setup_transforms_for_chain(
        sc_ms_id,
        sc_chain_id,
        sc_input_field,
        group_track,
        Some(fc_tf_id),
        &HashMap::default(),
    );

    let mut continuation_field_mappings = IndexVec::new();

    let fc_sc_terminator_tf_id =
        job.job_data.tf_mgr.transforms.peek_claim_id();

    let sc_terminator_actor_id = job.job_data.match_set_mgr.match_sets
        [sc_ms_id]
        .action_buffer
        .borrow_mut()
        .add_actor();

    for (i, var) in op.continuation_vars.iter_enumerated() {
        let sc_ms = &job.job_data.match_set_mgr.match_sets[sc_ms_id];
        let sc_scope = sc_ms.active_scope;
        let field_id = if let Some(field_name) = var.get_name() {
            if let Some(field_id) =
                job.job_data.scope_mgr.lookup_field(sc_scope, field_name)
            {
                field_id
            } else if let Some(fc_field_id) =
                job.job_data.scope_mgr.lookup_field(
                    job.job_data.match_set_mgr.match_sets[fc_ms_id]
                        .active_scope,
                    field_name,
                )
            {
                let field_id = job.job_data.field_mgr.get_cross_ms_cow_field(
                    &mut job.job_data.match_set_mgr,
                    sc_ms_id,
                    fc_field_id,
                );
                // debatable whether we should give a name here?
                job.job_data
                    .scope_mgr
                    .insert_field_name(sc_scope, field_name, field_id);
                field_id
            } else {
                // TODO: we should set up the field in fc for aliasing
                sc_dummy_field
            }
        } else {
            instantiation.next_input_field
        };

        let field_cow_tgt_id = job.job_data.field_mgr.get_cross_ms_cow_field(
            &mut job.job_data.match_set_mgr,
            cont_ms_id,
            field_id,
        );
        job.job_data.scope_mgr.insert_field_name_opt(
            sc_scope,
            var.get_name(),
            field_id,
        );

        job.job_data.field_mgr.setup_field_refs(
            &mut job.job_data.match_set_mgr,
            field_cow_tgt_id,
        );

        let field_iter = job.job_data.field_mgr.claim_iter(
            field_id,
            sc_terminator_actor_id,
            IterKind::Transform(fc_sc_terminator_tf_id),
        );

        let cont_field_id = continuation_vars[i];
        let mut cont_field =
            job.job_data.field_mgr.fields[cont_field_id].borrow_mut();

        let refs_offset_in_cont =
            cont_field.field_refs.len() as FieldRefOffset;

        {
            let field_cow_target =
                job.job_data.field_mgr.fields[field_cow_tgt_id].borrow();

            cont_field
                .field_refs
                .extend_from_slice(&field_cow_target.field_refs);
        }
        let field_ref_offset = cont_field.field_refs.len() as FieldRefOffset;
        cont_field.field_refs.push(field_cow_tgt_id);
        continuation_field_mappings.push(ContinuationFieldMapping {
            sc_field_id: field_id,
            sc_field_iter_id: field_iter,
            cont_field_id,
            sc_field_refs_offsets_start_in_cont_field: refs_offset_in_cont,
            sc_field_ref_offset_in_cont: field_ref_offset,
        });
    }

    let desired_batch_size = job
        .job_data
        .get_scope_setting_or_default::<SettingBatchSize>(fc_ms_scope);

    let trailer_tf_id_peek = job.job_data.tf_mgr.transforms.peek_claim_id();
    let group_track_iter_ref =
        job.job_data.group_track_manager.claim_group_track_iter_ref(
            instantiation.next_group_track,
            job.job_data.match_set_mgr.match_sets[sc_ms_id]
                .action_buffer
                .borrow()
                .peek_next_actor_id(),
            IterKind::Transform(trailer_tf_id_peek),
        );

    let trailer_tf = TfForkCatSubchainTrailer::<'a> {
        op,
        subchain_idx: fc_sc_idx,
        continuation_state,
        relaunch_for_cow_advance: false,
    };

    let trailer_tf_id = add_transform_to_job(
        &mut job.job_data,
        &mut job.transform_data,
        TransformState::new(
            instantiation.next_input_field,
            instantiation.next_input_field,
            sc_ms_id,
            desired_batch_size,
            None,
            instantiation.next_group_track,
        ),
        Box::new(trailer_tf),
    );
    debug_assert_eq!(trailer_tf_id_peek, trailer_tf_id);

    job.job_data.tf_mgr.transforms[instantiation.tfs_end].successor =
        Some(trailer_tf_id);

    SubchainEntry {
        ms_id: sc_ms_id,
        start_tf_id: instantiation.tfs_begin,
        trailer_tf_id,
        actor_id: sc_terminator_actor_id,
        group_track_iter_ref,
        continuation_field_mappings,
        field_iters_temp_slot: IndexVec::new(),
        input_done: false,
        batch_size_available: 0,
        fields_consumed: false,
        leading_padding_introduced: 0,
    }
}

struct ScVisitResult {
    field_pos_cont_start: usize,
    field_pos_cont_end: usize,
    scs_visited: usize,
}

/// visits the subchains in a round robin fashion starting form the currently
/// active one until batch size records have been produced or a subchain
/// has an unfinished group
fn visit_subchains(
    jd: &mut JobData,
    cont_state: &mut FcContinuationState,
    batch_size: usize,
) -> ScVisitResult {
    let mut cont_field_inserters =
        cont_state.cont_field_inserters.take_transmute();
    for cvm in
        &cont_state.subchains[FcSubchainIdx::ZERO].continuation_field_mappings
    {
        let field = jd.field_mgr.fields[cvm.cont_field_id].borrow_mut();
        let inserter = VaryingTypeInserter::new(RefMut::map(field, |f| {
            f.iter_hall.get_owned_data_mut()
        }));
        cont_field_inserters.push(inserter);
    }

    let sc_count = cont_state.subchains.len();

    let fields = cont_state.fields_temp.borrow_container();
    let mut field_iters = cont_state.field_iters_temp.borrow_container();
    let mut group_iters = cont_state.group_iters_temp.borrow_container();

    let final_sc = FcSubchainIdx::from_usize(sc_count - 1);
    let mut curr_sc = cont_state.current_sc;

    let mut scs_in_flight = 0;

    let mut sc_round_robin_index = FcSubchainRoundRobinIdx::ZERO;

    let mut batch_size_rem = batch_size;

    let mut cont_group_track = jd.group_track_manager.group_tracks
        [cont_state.continuation_input_group_track]
        .borrow_mut();
    let mut cont_group_track_next_group_id =
        cont_group_track.next_group_index_stable();

    let field_pos_cont_start = {
        let cont_field = jd.field_mgr.get_cow_field_ref(
            &jd.match_set_mgr,
            cont_state.continuation_dummy_iter.field_id,
        );
        let cont_iter = jd.field_mgr.lookup_iter_from_ref(
            cont_state.continuation_dummy_iter,
            &cont_field,
        );
        cont_iter.get_next_field_pos()
    };
    let mut field_pos_cont = field_pos_cont_start;

    loop {
        let sc_entry = &mut cont_state.subchains[curr_sc];

        if scs_in_flight < sc_count {
            scs_in_flight += 1;

            let mut sc_field_iters =
                sc_entry.field_iters_temp_slot.take_transmute();

            for cfm in &sc_entry.continuation_field_mappings {
                let sc_field = jd
                    .field_mgr
                    .get_cow_field_ref(&jd.match_set_mgr, cfm.sc_field_id);
                fields.push(sc_field);
                let iter = jd.field_mgr.lookup_iter(
                    cfm.sc_field_id,
                    fields.last().unwrap(),
                    cfm.sc_field_iter_id,
                );
                sc_field_iters.push(iter);
            }
            field_iters.push(sc_field_iters);

            let mut group_iter =
                jd.group_track_manager.lookup_group_track_iter_mut_from_ref(
                    sc_entry.group_track_iter_ref,
                    &jd.match_set_mgr,
                    sc_entry.actor_id,
                );
            let field_pos_sc = group_iter.field_pos();

            let fields_dropped_by_cont =
                field_pos_sc.saturating_sub(field_pos_cont_start);
            // PERF: maybe we should drop from the start to increase
            // our changes of ringbuffering the data?
            // we already cow'ed these fields, so this is essentially just
            // padding anyways. can't we drop everything?
            group_iter.drop_with_field_pos(0, fields_dropped_by_cont);

            group_iters.push(group_iter);
        }

        let group_track_iter = &mut group_iters[sc_round_robin_index];

        let field_pos_sc = group_track_iter.field_pos();

        let padding_needed = field_pos_cont - field_pos_sc;

        group_track_iter
            .insert_fields(FieldValueRepr::Undefined, padding_needed);

        if !sc_entry.fields_consumed {
            sc_entry.leading_padding_introduced += padding_needed;
        }

        let fields_to_consume = group_track_iter
            .group_len_rem()
            .min(batch_size_rem)
            .min(sc_entry.batch_size_available);

        group_track_iter.next_n_fields(fields_to_consume);

        if fields_to_consume > 0 {
            sc_entry.fields_consumed = true;
        }

        if group_track_iter.group_idx_stable()
            == cont_group_track_next_group_id
        {
            assert_eq!(curr_sc, FcSubchainIdx::ZERO);
            cont_group_track.push_group(
                fields_to_consume,
                group_track_iter.parent_group_advancement(),
            );
            cont_group_track_next_group_id =
                cont_group_track_next_group_id.next();
        } else {
            let group_idx = cont_group_track.group_lengths.len() - 1;
            cont_group_track
                .group_lengths
                .add_value(group_idx, fields_to_consume);
        }

        for (mapping_idx, cim) in
            sc_entry.continuation_field_mappings.iter_enumerated()
        {
            let iter = &mut field_iters[sc_round_robin_index][mapping_idx];
            let inserter = &mut cont_field_inserters[mapping_idx];

            let mut range_rem = fields_to_consume;
            while let Some(range) = iter
                .typed_range_fwd(range_rem, FieldIterRangeOptions::default())
            {
                range_rem -= range.field_count;
                inserter.extend_from_valid_range_re_ref(
                    range,
                    true,
                    false,
                    false,
                    cim.sc_field_ref_offset_in_cont,
                    cim.sc_field_refs_offsets_start_in_cont_field,
                );
            }
            debug_assert_eq!(range_rem, 0);
        }

        field_pos_cont += fields_to_consume;
        batch_size_rem -= fields_to_consume;
        sc_entry.batch_size_available -= fields_to_consume;

        if !group_track_iter.is_end_of_group(sc_entry.input_done) {
            break;
        }

        let is_end = group_track_iter.is_end(sc_entry.input_done);
        if !is_end {
            group_track_iter.next_group();
        }

        if batch_size_rem == 0 || (curr_sc == final_sc && is_end) {
            break;
        }

        curr_sc = sc_next(sc_count, curr_sc);
        sc_round_robin_index = FcSubchainRoundRobinIdx::from_usize(
            (sc_round_robin_index.into_usize() + 1) % sc_count,
        );
    }

    let scs_visited = field_iters.len();

    // insert trailing padding into visited scs and store back the iters
    for rr_idx in (0..scs_visited).rev() {
        let sc_idx = FcSubchainIdx::from_usize(
            (rr_idx + cont_state.current_sc.into_usize()) % sc_count,
        );
        let sc_entry = &mut cont_state.subchains[sc_idx];

        sc_entry
            .field_iters_temp_slot
            .reclaim_temp(field_iters.pop().unwrap());

        let mut group_iter = group_iters.pop().unwrap();

        let padding_needed = field_pos_cont - group_iter.field_pos();

        if !sc_entry.fields_consumed {
            sc_entry.leading_padding_introduced += padding_needed;
        }
        group_iter.insert_fields(FieldValueRepr::Undefined, padding_needed);
        group_iter.store_iter(sc_entry.group_track_iter_ref.iter_id);
    }

    cont_state.current_sc = curr_sc;

    ScVisitResult {
        field_pos_cont_start,
        field_pos_cont_end: field_pos_cont,
        scs_visited,
    }
}

// even if we did not visit a subchain, we still have to make sure that
// the field count in the continuation lines up correctly for cow'ed fields
fn pad_non_visited_subchains(
    jd: &mut JobData,
    cont_state: &mut FcContinuationState,
    sc_idx_before: FcSubchainIdx,
    scs_visited: usize,
    field_pos_cont_start: usize,
    field_pos_cont_end: usize,
) -> usize {
    let sc_count = cont_state.subchains.len();

    for rr_idx in scs_visited..sc_count {
        let sc_idx = FcSubchainIdx::from_usize(
            (rr_idx + sc_idx_before.into_usize()) % sc_count,
        );
        let sc_entry = &mut cont_state.subchains[sc_idx];
        let mut group_iter =
            jd.group_track_manager.lookup_group_track_iter_mut_from_ref(
                sc_entry.group_track_iter_ref,
                &jd.match_set_mgr,
                sc_entry.actor_id,
            );
        let field_pos_sc = group_iter.field_pos();
        let fields_dropped_by_cont =
            field_pos_sc.saturating_sub(field_pos_cont_start);
        group_iter.drop_with_field_pos(0, fields_dropped_by_cont);

        let padding_needed =
            field_pos_cont_end - (field_pos_sc - fields_dropped_by_cont);

        group_iter.insert_fields(FieldValueRepr::Undefined, padding_needed);
        group_iter.store_iter(sc_entry.group_track_iter_ref.iter_id);
    }
    sc_count
}

// We aquire the subchain field iters before any padding is introduced,
// so it's impossible to store them back at the correct position before
// those insert actions are applied.
// This method reaquires them after the actions have been committed and
// advances them accordingly.
// PERF: this reiterates through the fields that were added by this sc this
// round unneccessarily, we could avoid that, blerg.
fn advance_subchain_field_iters(
    jd: &mut JobData,
    cont_state: &mut FcContinuationState,
    sc_idx_before: FcSubchainIdx,
    scs_visited: usize,
    fields_produced: usize,
) {
    let sc_count = cont_state.subchains.len();
    for rr_idx in (0..scs_visited).rev() {
        let sc_idx = FcSubchainIdx::from_usize(
            (rr_idx + sc_idx_before.into_usize()) % sc_count,
        );
        let sc_entry = &mut cont_state.subchains[sc_idx];
        // These iters are right leaning (they aren't cow marker iters),
        // therefore they were already nudged by the leading padding.
        // We therefore only have to advance them by the actually inserted
        // fields.
        let iter_advancement =
            fields_produced - sc_entry.leading_padding_introduced;
        sc_entry.leading_padding_introduced = 0;
        sc_entry.fields_consumed = false;
        if iter_advancement == 0 {
            continue;
        }
        for cfm in &sc_entry.continuation_field_mappings {
            let field = jd
                .field_mgr
                .get_cow_field_ref(&jd.match_set_mgr, cfm.sc_field_id);
            let mut iter = jd.field_mgr.lookup_iter(
                cfm.sc_field_id,
                &field,
                cfm.sc_field_iter_id,
            );
            iter.next_n_fields(iter_advancement, true);
            jd.field_mgr.store_iter(
                cfm.sc_field_id,
                cfm.sc_field_iter_id,
                iter,
            );
        }
    }
}

fn pad_continuation_dummy_field(
    jd: &mut JobData,
    cont_state: &mut FcContinuationState,
    fields_produced: usize,
) {
    jd.field_mgr.apply_field_actions(
        &jd.match_set_mgr,
        cont_state.continuation_dummy_iter.field_id,
        true,
    );
    jd.field_mgr.fields[cont_state.continuation_dummy_iter.field_id]
        .borrow_mut()
        .iter_hall
        .push_undefined(fields_produced, true);
    let cont_field = jd.field_mgr.get_cow_field_ref(
        &jd.match_set_mgr,
        cont_state.continuation_dummy_iter.field_id,
    );
    let mut cont_iter = jd
        .field_mgr
        .lookup_iter_from_ref(cont_state.continuation_dummy_iter, &cont_field);
    cont_iter.next_n_fields(fields_produced, true);
    jd.field_mgr
        .store_iter_from_ref(cont_state.continuation_dummy_iter, cont_iter);
    drop(cont_field);
}

impl Operator for OpForkCat {
    fn default_name(&self) -> super::operator::OperatorName {
        "forkcat".into()
    }

    fn output_count(&self, _sess: &SessionData, _op_id: OperatorId) -> usize {
        // technically this has output, but it always introduces a
        // separate BB so we don't want to allocate slots for that
        0
    }

    fn output_field_kind(
        &self,
        _sess: &SessionData,
        _op_id: OperatorId,
    ) -> super::operator::OutputFieldKind {
        super::operator::OutputFieldKind::Unconfigured
    }

    fn build_transforms<'a>(
        &'a self,
        job: &mut Job<'a>,
        tf_state: &mut TransformState,
        op_id: OperatorId,
        _prebound_outputs: &super::operator::PreboundOutputsMap,
    ) -> super::operator::TransformInstatiation<'a> {
        let input_field = tf_state.input_field;

        // TODO: scope sharing stuff
        let cont_scope_id = job.job_data.scope_mgr.add_scope(None);

        let cont_ms_id = job.job_data.match_set_mgr.add_match_set(
            &mut job.job_data.field_mgr,
            &mut job.job_data.scope_mgr,
            cont_scope_id,
        );

        let gt_parent = job.job_data.group_track_manager.group_tracks
            [tf_state.input_group_track_id]
            .borrow()
            .parent_group_track_id();

        let cont_group_track =
            job.job_data.group_track_manager.add_group_track(
                &job.job_data.match_set_mgr,
                gt_parent,
                cont_ms_id,
                ActorRef::Unconfirmed(ActorId::ZERO),
            );

        #[cfg(feature = "debug_state")]
        {
            job.job_data.group_track_manager.group_tracks[cont_group_track]
                .borrow_mut()
                .alias_source = Some(tf_state.input_group_track_id);
        }

        let mut cont_input_field =
            job.job_data.match_set_mgr.get_dummy_field(cont_ms_id);
        let mut continuation_var_mapping =
            IndexVec::<ContinuationVarIdx, FieldId>::new();

        for cv in &self.continuation_vars {
            let field_id = job.job_data.field_mgr.add_field(
                &job.job_data.match_set_mgr,
                cont_ms_id,
                ActorRef::default(),
            );
            job.job_data.scope_mgr.insert_field_name_opt(
                cont_scope_id,
                cv.get_name(),
                field_id,
            );
            if cv == &Var::BBInput {
                cont_input_field = field_id;
            }
            continuation_var_mapping.push(field_id);
        }

        let cont_dummy_field =
            job.job_data.match_set_mgr.get_dummy_field(cont_ms_id);

        let continuation_dummy_iter = job.job_data.field_mgr.claim_iter_ref(
            cont_dummy_field,
            ActorId::ZERO,
            IterKind::Transform(
                job.job_data.tf_mgr.transforms.peek_claim_id(),
            ),
        );

        let continuation_state = Arc::new(Mutex::new(FcContinuationState {
            continuation_tf_id: None, // filled in by the header transform
            continuation_input_group_track: cont_group_track,
            continuation_ms_id: cont_ms_id,
            current_sc: FcSubchainIdx::ZERO,
            subchains: IndexVec::default(),
            advance_to_next: false,
            continuation_dummy_iter,
            group_iters_temp: IndexVec::new(),
            field_iters_temp: IndexVec::new(),
            fields_temp: StableVec::new(),
            cont_field_inserters: IndexVec::new(),
        }));

        let tf_data = Box::new(TfForkCatHeader {
            continuation_state: continuation_state.clone(),
            actor_id: job
                .job_data
                .add_actor_for_tf_state_ignore_output_field(tf_state),
            relaunch_after_cow_advance: false,
        });
        let fc_tf_id = add_transform_to_job(
            &mut job.job_data,
            &mut job.transform_data,
            tf_state.clone(),
            tf_data,
        );

        let mut subchains = IndexVec::new();

        for (fc_sc_idx, sc_idx) in (self.subchains_start..self.subchains_end)
            .index_range()
            .enumerate()
        {
            let sc_entry = setup_subchain(
                self,
                continuation_state.clone(),
                cont_ms_id,
                job,
                &job.job_data.session_data.operator_bases[op_id],
                sc_idx,
                FcSubchainIdx::from_usize(fc_sc_idx),
                input_field,
                fc_tf_id,
                &continuation_var_mapping,
            );
            subchains.push(sc_entry);
        }

        let fc = &mut job.transform_data[fc_tf_id];
        let fc: &TfForkCatHeader = fc.downcast_ref().unwrap();

        let mut cont = fc.continuation_state.lock().unwrap();
        cont.subchains = subchains;

        let tf = &mut job.job_data.tf_mgr.transforms[fc_tf_id];
        tf.output_field = cont_input_field;
        job.job_data.field_mgr.bump_field_refcount(cont_input_field);

        super::operator::TransformInstatiation::Multiple(
            OperatorInstantiation {
                tfs_begin: fc_tf_id,
                tfs_end: fc_tf_id,
                next_input_field: cont_input_field,
                next_group_track: cont_group_track,
                next_match_set: cont_ms_id,
            },
        )
    }

    fn update_bb_for_op(
        &self,
        sess: &SessionData,
        ld: &mut LivenessData,
        _op_id: OperatorId,
        op_n: OffsetInChain,
        cn: &crate::chain::Chain,
        bb_id: crate::liveness_analysis::BasicBlockId,
    ) -> bool {
        let bb = &mut ld.basic_blocks[bb_id];
        for sc in &cn.subchains[self.subchains_start..self.subchains_end] {
            bb.calls.push(sc.into_bb_id());
        }
        ld.split_bb_at_call(sess, bb_id, op_n);
        true
    }

    fn on_liveness_computed(
        &mut self,
        _sess: &mut SessionData,
        ld: &LivenessData,
        op_id: OperatorId,
    ) {
        let sc_count =
            (self.subchains_end - self.subchains_start).into_usize();
        let bb_id = ld.operator_liveness_data[op_id].basic_block_id;
        let bb = &ld.basic_blocks[bb_id];
        // TODO: introduce direct reads (not affected by field refs)
        // to reduce this set here
        let successors = ld.get_var_liveness_ored(
            bb.successors.iter().chain(bb.caller_successors.iter()),
            VarLivenessSlotGroup::Global,
        );
        let successor_reads = successors.get_slot(VarLivenessSlotKind::Reads);
        for var_id in successor_reads.iter_ones().map(VarId::from_usize) {
            self.continuation_vars.push(ld.vars[var_id]);
        }
        let input_var_idx = BB_INPUT_VAR_ID.into_usize();
        let successors_read_input = successor_reads[input_var_idx];
        for (sc_idx, &callee_id) in bb.calls.iter().enumerate() {
            let call_reads = ld.get_var_liveness_slot(
                callee_id,
                VarLivenessSlotGroup::Global,
                VarLivenessSlotKind::Reads,
            );
            let reads = call_reads.iter_ones().map(VarId::from_usize);

            for var_id in reads {
                declare_var_needed_for_sc(self, ld, var_id, sc_count, sc_idx);
            }
            // In case the input field is 'passed through', we mark it as used
            // here. Otherwise the the first transform will be passed the dummy
            // input (since it doesn't read it's input directly), which would
            // cause that to be passed through.
            let input_read = call_reads[input_var_idx];
            let input_survives = ld.get_var_liveness_slot(
                callee_id,
                VarLivenessSlotGroup::Local,
                VarLivenessSlotKind::Survives,
            )[input_var_idx];
            if !input_read && input_survives && successors_read_input {
                declare_var_needed_for_sc(
                    self,
                    ld,
                    BB_INPUT_VAR_ID,
                    sc_count,
                    sc_idx,
                );
            };
        }
    }

    fn setup(
        &mut self,
        sess: &mut SessionSetupData,
        op_data_id: OperatorDataId,
        chain_id: ChainId,
        offset_in_chain: OperatorOffsetInChain,
        span: Span,
    ) -> Result<OperatorId, TypelineError> {
        let op_id = sess.add_op(op_data_id, chain_id, offset_in_chain, span);

        let OperatorOffsetInChain::Direct(direct_offset_in_chain) =
            offset_in_chain
        else {
            return Err(OperatorSetupError::new(
                "operator `forkcat` cannot be part of an aggregation",
                op_id,
            )
            .into());
        };
        self.direct_offset_in_chain = direct_offset_in_chain;

        self.subchains_start = sess.chains[chain_id].subchains.len_idx();

        for sc in std::mem::take(&mut self.subchains) {
            sess.setup_subchain(chain_id, sc)?;
        }

        self.subchains_end = sess.chains[chain_id].subchains.len_idx();

        Ok(op_id)
    }

    fn update_variable_liveness(
        &self,
        _sess: &SessionData,
        _ld: &mut LivenessData,
        _op_offset_after_last_write: OffsetInChain,
        _op_id: OperatorId,
        _bb_id: crate::liveness_analysis::BasicBlockId,
        _input_field: crate::liveness_analysis::OpOutputIdx,
        output: &mut crate::liveness_analysis::OperatorLivenessOutput,
    ) {
        output.flags.input_accessed = false;
        output.call_effect = OperatorCallEffect::Diverge;
    }
}

impl Transform<'_> for TfForkCatHeader {
    fn display_name(
        &self,
        _jd: &JobData,
        _tf_id: TransformId,
    ) -> super::transform::DefaultTransformName {
        if self.relaunch_after_cow_advance {
            "forkcat_header(cow advance only)"
        } else {
            "forkcat_header"
        }
        .into()
    }
    fn update(&mut self, jd: &mut JobData<'_>, tf_id: TransformId) {
        let (batch_size, ps) = jd.tf_mgr.claim_all(tf_id);

        let mut cont_state = self.continuation_state.lock().unwrap();

        let tf = &jd.tf_mgr.transforms[tf_id];
        cont_state.continuation_tf_id = tf.successor;
        let ms_id = tf.match_set_id;

        if !self.relaunch_after_cow_advance {
            // rev so the first subchain ends up at the top of the stack
            for sc in cont_state.subchains.iter().rev() {
                // PERF: maybe provide a bulk version of this?
                jd.match_set_mgr.advance_cross_ms_cow_targets(
                    &jd.field_mgr,
                    None,
                    jd.tf_mgr.transforms[sc.start_tf_id].match_set_id,
                    batch_size,
                );
                jd.tf_mgr.inform_transform_batch_available(
                    sc.start_tf_id,
                    batch_size,
                    ps.input_done,
                );
            }
            self.relaunch_after_cow_advance = true;
            jd.tf_mgr.unclaim_batch_size(tf_id, batch_size);
            jd.tf_mgr.push_tf_in_ready_stack(tf_id);
            return;
        }
        self.relaunch_after_cow_advance = false;
        jd.group_track_manager.propagate_leading_groups_to_aliases(
            &jd.match_set_mgr,
            jd.tf_mgr.transforms[tf_id].input_group_track_id,
            batch_size,
            ps.input_done,
            true,
            cont_state
                .subchains
                .iter()
                .map(|sc| sc.group_track_iter_ref.track_id),
        );

        if !cfg!(feature = "debug_disable_terminator") {
            let mut ab = jd.match_set_mgr.match_sets[ms_id]
                .action_buffer
                .borrow_mut();
            ab.begin_action_group(self.actor_id);
            ab.push_action(FieldActionKind::Drop, 0, batch_size);
            ab.end_action_group();
        }
    }
    fn as_any(&self) -> Option<&dyn std::any::Any> {
        Some(self)
    }
    fn as_any_mut(&mut self) -> Option<&mut dyn std::any::Any> {
        Some(self)
    }
}

impl<'a> Transform<'a> for TfForkCatSubchainTrailer<'a> {
    fn display_name(
        &self,
        _jd: &JobData,
        _tf_id: TransformId,
    ) -> super::transform::DefaultTransformName {
        if self.relaunch_for_cow_advance {
            "forkcat_subchain_trailer(pre cow advance)"
        } else {
            "forkcat_subchain_trailer"
        }
        .into()
    }
    fn update(&mut self, jd: &mut JobData, tf_id: TransformId) {
        let (batch_size, ps) = jd.tf_mgr.claim_all(tf_id);
        let cont_state = &mut *(*self.continuation_state).lock().unwrap();

        if self.relaunch_for_cow_advance {
            self.relaunch_for_cow_advance = false;
            let cont_tf_id = cont_state.continuation_tf_id.unwrap();

            // slight hack
            let fields_produced = batch_size;

            let done = cont_state.subchains.iter().all(|sc| sc.input_done);

            if done {
                // we do not use the successor mechanism for this
                // so we have to do this manually
                jd.tf_mgr.transforms[tf_id].done = true;
            }

            jd.match_set_mgr.advance_cross_ms_cow_targets(
                &jd.field_mgr,
                None,
                jd.tf_mgr.transforms[cont_tf_id].match_set_id,
                fields_produced,
            );
            jd.tf_mgr.inform_transform_batch_available(
                cont_tf_id,
                fields_produced,
                done,
            );

            return;
        }

        let sc_entry = &mut cont_state.subchains[self.subchain_idx];
        sc_entry.input_done = ps.input_done;
        sc_entry.batch_size_available += batch_size;

        let sc_idx_before = cont_state.current_sc;

        let ScVisitResult {
            field_pos_cont_start,
            field_pos_cont_end,
            scs_visited,
        } = visit_subchains(jd, cont_state, usize::MAX);

        let fields_produced = field_pos_cont_end - field_pos_cont_start;

        pad_non_visited_subchains(
            jd,
            cont_state,
            sc_idx_before,
            scs_visited,
            field_pos_cont_start,
            field_pos_cont_end,
        );

        pad_continuation_dummy_field(jd, cont_state, fields_produced);

        advance_subchain_field_iters(
            jd,
            cont_state,
            sc_idx_before,
            scs_visited,
            fields_produced,
        );

        self.relaunch_for_cow_advance = true;
        jd.tf_mgr.unclaim_batch_size(tf_id, fields_produced);
        jd.tf_mgr.push_tf_in_ready_stack(tf_id);
    }
}

pub fn create_op_forkcat_with_spans(
    mut subchains: Vec<Vec<(Box<dyn Operator>, Span)>>,
    opts: ForkcatOpts,
) -> Box<dyn Operator> {
    for sc in &mut subchains {
        if sc.is_empty() {
            sc.push((create_op_nop(), Span::Generated));
        }
    }
    Box::new(OpForkCat {
        subchains,
        subchains_start: SubchainIndex::MAX,
        subchains_end: SubchainIndex::MAX,
        direct_offset_in_chain: OffsetInChain::MAX,
        input_mappings: HashMap::default(),
        continuation_vars: IndexVec::new(),
        opts,
    })
}

pub fn create_op_forkcat(
    subchains: impl IntoIterator<
        Item = impl IntoIterator<Item = Box<dyn Operator>>,
    >,
    opts: ForkcatOpts,
) -> Box<dyn Operator> {
    let subchains = subchains
        .into_iter()
        .map(|sc| {
            sc.into_iter()
                .map(|op_data| (op_data, Span::Generated))
                .collect::<Vec<_>>()
        })
        .collect();
    create_op_forkcat_with_spans(subchains, opts)
}

pub fn parse_op_forkcat(
    sess: &mut SessionSetupData,
    mut arg: Argument,
) -> Result<Box<dyn Operator>, TypelineError> {
    let mut subchains = Vec::new();
    let mut curr_subchain = Vec::new();
    let mut copy = false;
    let expr = CallExpr::from_argument(&arg)?;
    if let (Some(flags), _) = expr.split_flags_arg(false) {
        for (k, v) in flags {
            let FieldValue::Argument(v) = v else {
                unreachable!()
            };
            if k == "-c" || k == "--copy" {
                copy = true;
                continue;
            }
            return Err(CliArgumentError::new_s(
                format!("forkcat has no option `{}`", k),
                v.span,
            )
            .into());
        }
    }
    for arg in std::mem::take(arg.expect_arg_array_mut()?.0)
        .into_iter()
        .skip(1)
    {
        let expr = CallExpr::from_argument(&arg)?;
        if expr.op_name == "next" {
            expr.reject_args()?;
            subchains.push(curr_subchain);
            curr_subchain = Vec::new();
            continue;
        };
        let span = arg.span;
        curr_subchain.push((parse_operator_data(sess, arg)?, span));
    }
    subchains.push(curr_subchain);
    Ok(create_op_forkcat_with_spans(
        subchains,
        ForkcatOpts { copy },
    ))
}
