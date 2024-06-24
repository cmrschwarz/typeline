use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use bitvec::vec::BitVec;

use crate::{
    chain::{ChainId, SubchainIndex},
    cli::{
        call_expr::CallExpr, call_expr_iter::CallExprIter, parse_operator_data,
    },
    context::{SessionData, SessionSetupData},
    index_newtype,
    job::{add_transform_to_job, Job, JobData},
    liveness_analysis::{
        LivenessData, Var, VarId, VarLivenessSlotGroup, VarLivenessSlotKind,
    },
    options::{
        operator_base_options::{
            OperatorBaseOptions, OperatorBaseOptionsInterned,
        },
        session_options::SessionOptions,
    },
    record_data::{
        action_buffer::{ActorId, ActorRef},
        field::{FieldId, FieldRefOffset},
        field_data::{field_value_flags, FieldValueRepr},
        group_track::{GroupTrackId, GroupTrackIterRef},
        iter_hall::{IterId, IterKind},
        iters::FieldIterator,
        match_set::MatchSetId,
        push_interface::PushInterface,
    },
    utils::{
        index_vec::{IndexSlice, IndexVec},
        indexing_type::{IndexingType, IndexingTypeRange},
    },
};

use super::{
    errors::{OperatorCreationError, OperatorSetupError},
    nop::create_op_nop,
    operator::{
        OffsetInChain, OperatorBase, OperatorData, OperatorDataId, OperatorId,
        OperatorInstantiation, OperatorOffsetInChain,
        TransformContinuationKind, DUMMY_OP_NOP,
    },
    transform::{TransformData, TransformId, TransformState},
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
//        (have an Arc<RefCell> to figure that out), if no do nothing
//      - if yes:
//              - create / append group length
//              - append to pseudo data column, inform comsumers

index_newtype! {
    pub struct FcSubchainIdx(u32);
    pub struct ContinuationVarIdx(u32);
}

pub struct OpForkCat {
    pub subchains: Vec<Vec<(OperatorBaseOptions, OperatorData)>>,

    pub subchains_start: SubchainIndex,
    pub subchains_end: SubchainIndex,

    pub direct_offset_in_chain: OffsetInChain,
    // list of input vars needed in each subchain
    input_mappings: HashMap<Var, BitVec<usize>>,

    // var names accessed by continuation
    continuation_vars: IndexVec<ContinuationVarIdx, Var>,
}

pub struct SubchainEntry {
    pub start_tf_id: TransformId,
    pub trailer_tf_id: TransformId,
    pub group_track_id: GroupTrackId,
}

pub struct TfForkCat {
    pub continuation_state: Arc<Mutex<FcContinuationState>>,
}

pub struct ContinuationFieldMapping {
    pub sc_field_id: FieldId,
    pub sc_field_iter_id: IterId,
    pub cont_field_id: FieldId,
    pub sc_field_ref_offset_in_cont: FieldRefOffset,
    // start of the field_refs list from the field in the subchain in the
    // continuation field's field_refs list
    pub sc_field_refs_offsets_start_in_cont_field: FieldRefOffset,
}

pub struct FcContinuationState {
    pub subchains: IndexVec<FcSubchainIdx, SubchainEntry>,
    pub continuation_tf_id: TransformId,
    pub current_turn: FcSubchainIdx,
    pub produced_on_last_turn: IndexVec<FcSubchainIdx, usize>,
    pub rolling_sum: usize,
    pub advance_to_next: bool,
}

pub struct TfForkCatSubchainTrailer<'a> {
    pub op: &'a OpForkCat,
    pub actor_id: ActorId,
    pub group_track_iter_ref: GroupTrackIterRef,
    pub subchain_idx: FcSubchainIdx,
    // TODO: figure out a better mechanism for this, this is stupid
    pub continuation_state: Arc<Mutex<FcContinuationState>>,

    // field ref offset of subchain field in continuation field (with id)
    pub continuation_field_mappings: Vec<ContinuationFieldMapping>,
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

impl FcContinuationState {
    fn sc_count(&self) -> usize {
        self.produced_on_last_turn.len()
    }
    fn next_sc(&self) -> FcSubchainIdx {
        sc_index_offset(self.sc_count(), self.current_turn, 1)
    }
    fn last_sc(&self) -> FcSubchainIdx {
        FcSubchainIdx::from_usize(self.sc_count().saturating_sub(1))
    }
    fn should_act(&self, subchain_idx: FcSubchainIdx) -> bool {
        if self.advance_to_next {
            self.next_sc() == subchain_idx
        } else {
            self.current_turn == subchain_idx
        }
    }
}

pub fn setup_op_forkcat(
    op: &mut OpForkCat,
    sess: &mut SessionSetupData,
    chain_id: ChainId,
    offset_in_chain: OperatorOffsetInChain,
    opts_interned: OperatorBaseOptionsInterned,
    op_data_id: OperatorDataId,
) -> Result<OperatorId, OperatorSetupError> {
    let op_id = sess.add_op_from_offset_in_chain(
        chain_id,
        offset_in_chain,
        opts_interned,
        op_data_id,
    );

    let OperatorOffsetInChain::Direct(direct_offset_in_chain) =
        offset_in_chain
    else {
        return Err(OperatorSetupError::new(
            "operator `forkcat` cannot be part of an aggregation",
            op_id,
        ));
    };
    op.direct_offset_in_chain = direct_offset_in_chain;

    op.subchains_start = sess.chains[chain_id].subchains.next_idx();

    for sc in std::mem::take(&mut op.subchains) {
        sess.create_subchain(chain_id, sc)?;
    }

    op.subchains_end = sess.chains[chain_id].subchains.next_idx();

    Ok(op_id)
}

pub fn setup_op_forkcat_liveness_data(
    _sess: &SessionData,
    op: &mut OpForkCat,
    op_id: OperatorId,
    ld: &LivenessData,
) {
    let sc_count = (op.subchains_end - op.subchains_start).into_usize();
    let bb_id = ld.operator_liveness_data[op_id].basic_block_id;
    let bb = &ld.basic_blocks[bb_id];
    let successors = ld.get_var_liveness_ored(
        VarLivenessSlotGroup::Global,
        bb.successors.iter(),
    );
    // TODO: introduce direct reads (not affected by field refs)
    // to reduce this set here
    let successors_reads = successors.get_slot(VarLivenessSlotKind::Reads);
    for var_id in successors_reads.iter_ones().map(VarId::from_usize) {
        op.continuation_vars.push(ld.vars[var_id]);
    }
    for (sc_idx, &callee_id) in bb.calls.iter().enumerate() {
        let call_reads = ld.get_var_liveness_slot(
            callee_id,
            VarLivenessSlotGroup::Global,
            VarLivenessSlotKind::Reads,
        );
        for var_id in call_reads.iter_ones().map(VarId::from_usize) {
            op.input_mappings
                .entry(ld.vars[var_id])
                .or_insert_with(|| {
                    let mut bv = BitVec::with_capacity(sc_count);
                    bv.resize(sc_count, false);
                    bv
                })
                .set(sc_idx, true);
        }
    }
}

pub fn insert_tf_forkcat<'a>(
    job: &mut Job<'a>,
    op_base: &'a OperatorBase,
    op: &'a OpForkCat,
    tf_state: TransformState,
) -> OperatorInstantiation {
    let tf_op = tf_state.op_id.unwrap();
    let chain_id = job.job_data.session_data.operator_bases[tf_op].chain_id;
    let input_field = tf_state.input_field;
    let cont_ms_id = job
        .job_data
        .match_set_mgr
        .add_match_set(&mut job.job_data.field_mgr);

    let cont_group_track = job.job_data.group_track_manager.add_group_track(
        None,
        cont_ms_id,
        ActorRef::Unconfirmed(0),
    );

    let mut cont_input_field =
        job.job_data.match_set_mgr.get_dummy_field(cont_ms_id);
    let mut continuation_var_mapping =
        IndexVec::<ContinuationVarIdx, FieldId>::new();

    for cv in &op.continuation_vars {
        let field_id = job.job_data.field_mgr.add_field(
            &mut job.job_data.match_set_mgr,
            cont_ms_id,
            cv.get_name(),
            ActorRef::default(),
        );
        if cv == &Var::BBInput {
            cont_input_field = field_id;
        }
        continuation_var_mapping.push(field_id);
    }

    let sc_count = (op.subchains_end - op.subchains_start).into_usize();

    let continuation_state = Arc::new(Mutex::new(FcContinuationState {
        continuation_tf_id: TransformId::zero(), // fill in later
        current_turn: FcSubchainIdx::zero(),
        subchains: IndexVec::default(),
        produced_on_last_turn: IndexVec::from(vec![0; sc_count]),
        rolling_sum: 0,
        advance_to_next: false,
    }));

    let tf_data = TransformData::ForkCat(TfForkCat {
        continuation_state: continuation_state.clone(),
    });
    let fc_tf_id = add_transform_to_job(
        &mut job.job_data,
        &mut job.transform_data,
        tf_state,
        tf_data,
    );

    let mut subchains = IndexVec::new();

    for (fc_sc_idx, sc_idx) in
        IndexingTypeRange::new(op.subchains_start..op.subchains_end)
            .enumerate()
    {
        let sc_entry = setup_subchain(
            op,
            continuation_state.clone(),
            cont_ms_id,
            job,
            op_base,
            sc_idx,
            FcSubchainIdx::from_usize(fc_sc_idx),
            input_field,
            fc_tf_id,
            &continuation_var_mapping,
        );
        subchains.push(sc_entry);
    }

    let cont_inst = if let Some(cont_op_id) = job.job_data.session_data.chains
        [chain_id]
        .operators
        .get(op.direct_offset_in_chain + OffsetInChain::one())
        .copied()
    {
        job.setup_transforms_from_op(
            cont_ms_id,
            cont_op_id,
            cont_input_field,
            cont_group_track,
            None,
            &HashMap::default(),
        )
    } else {
        job.setup_transforms_for_op_iter(
            std::iter::once((tf_op, op_base, &DUMMY_OP_NOP)),
            cont_ms_id,
            cont_input_field,
            cont_group_track,
            None,
            &HashMap::default(),
        )
    };

    let TransformData::ForkCat(fc) = &mut job.transform_data[fc_tf_id] else {
        unreachable!()
    };

    let mut cont = fc.continuation_state.lock().unwrap();
    cont.subchains = subchains;
    cont.continuation_tf_id = cont_inst.tfs_begin;

    let tf = &mut job.job_data.tf_mgr.transforms[fc_tf_id];
    tf.successor = Some(cont_inst.tfs_begin);
    tf.output_field = cont_input_field;
    job.job_data.field_mgr.bump_field_refcount(cont_input_field);

    OperatorInstantiation {
        tfs_begin: fc_tf_id,
        tfs_end: cont_inst.tfs_end,
        next_input_field: cont_inst.next_input_field,
        next_group_track: cont_inst.next_group_track,
        continuation: TransformContinuationKind::SelfExpanded,
    }
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
    let fc_ms_id = job.job_data.tf_mgr.transforms[fc_tf_id].match_set_id;

    let sc_ms_id = job
        .job_data
        .match_set_mgr
        .add_match_set(&mut job.job_data.field_mgr);

    let fc_dummy_field = job.job_data.match_set_mgr.get_dummy_field(fc_ms_id);
    let sc_dummy_field = job.job_data.match_set_mgr.get_dummy_field(sc_ms_id);

    job.job_data.field_mgr.setup_cow_between_fields(
        &mut job.job_data.match_set_mgr,
        fc_dummy_field,
        sc_dummy_field,
    );

    let group_track = job.job_data.group_track_manager.add_group_track(
        None,
        sc_ms_id,
        ActorRef::Unconfirmed(0),
    );

    let sc_chain_id =
        job.job_data.session_data.chains[op_base.chain_id].subchains[sc_idx];

    let mut sc_input_field = sc_dummy_field;
    for (&var, needing_subchains) in &op.input_mappings {
        if !needing_subchains[fc_sc_idx.into_usize()] {
            continue;
        }

        let src_field = if let Some(name) = var.get_name() {
            *job.job_data.match_set_mgr.match_sets[fc_ms_id]
                .field_name_map
                .get(&name)
                .unwrap_or(&sc_dummy_field)
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

    let mut continuation_field_mappings = Vec::new();

    let fc_sc_terminator_tf_id =
        job.job_data.tf_mgr.transforms.peek_claim_id();

    for (i, var) in op.continuation_vars.iter_enumerated() {
        let sc_ms = &job.job_data.match_set_mgr.match_sets[sc_ms_id];
        let field_id = if let Some(field_name) = var.get_name() {
            if let Some(&field_id) = sc_ms.field_name_map.get(&field_name) {
                field_id
            } else if let Some(&fc_field_id) =
                job.job_data.match_set_mgr.match_sets[fc_ms_id]
                    .field_name_map
                    .get(&field_name)
            {
                job.job_data.field_mgr.get_cross_ms_cow_field(
                    &mut job.job_data.match_set_mgr,
                    sc_ms_id,
                    fc_field_id,
                )
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

        job.job_data.field_mgr.setup_field_refs(
            &mut job.job_data.match_set_mgr,
            field_cow_tgt_id,
        );

        let field_iter = job
            .job_data
            .field_mgr
            .claim_iter(field_id, IterKind::Transform(fc_sc_terminator_tf_id));

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

    let desired_batch_size = job.job_data.session_data.chains
        [op_base.chain_id]
        .settings
        .default_batch_size;

    let actor_id = job.job_data.match_set_mgr.match_sets[sc_ms_id]
        .action_buffer
        .borrow_mut()
        .add_actor();

    let trailer_tf_id_peek = job.job_data.tf_mgr.transforms.peek_claim_id();
    let group_track_iter_ref =
        job.job_data.group_track_manager.claim_group_track_iter_ref(
            instantiation.next_group_track,
            IterKind::Transform(trailer_tf_id_peek),
        );
    let trailer_tf = TfForkCatSubchainTrailer::<'a> {
        op,
        actor_id,
        group_track_iter_ref,
        subchain_idx: fc_sc_idx,
        continuation_state,
        continuation_field_mappings,
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
        TransformData::ForkCatSubchainTrailer(trailer_tf),
    );
    debug_assert_eq!(trailer_tf_id_peek, trailer_tf_id);

    job.job_data.tf_mgr.transforms[instantiation.tfs_end].successor =
        Some(trailer_tf_id);

    SubchainEntry {
        start_tf_id: instantiation.tfs_begin,
        trailer_tf_id,
        group_track_id: group_track,
    }
}

pub fn handle_tf_forkcat(
    jd: &mut JobData,
    tf_id: TransformId,
    fc: &mut TfForkCat,
) {
    let (batch_size, ps) = jd.tf_mgr.claim_all(tf_id);

    let cont_state = fc.continuation_state.lock().unwrap();

    // rev so the first subchain ends up at the top of the stack
    for sc in cont_state.subchains.iter().rev() {
        // PERF: maybe provide a bulk version of this?
        jd.tf_mgr.inform_cross_ms_transform_batch_available(
            &jd.field_mgr,
            &jd.match_set_mgr,
            sc.start_tf_id,
            batch_size,
            batch_size,
            ps.input_done,
        );
    }
    jd.group_track_manager.pass_leading_groups_to_children(
        &jd.match_set_mgr,
        jd.tf_mgr.transforms[tf_id].input_group_track_id,
        batch_size,
        ps.input_done,
        cont_state.subchains.iter().map(|sc| sc.group_track_id),
    );
}

pub fn handle_tf_forcat_subchain_trailer(
    jd: &mut JobData,
    tf_id: TransformId,
    fcst: &mut TfForkCatSubchainTrailer,
) {
    let (batch_size, ps) = jd.tf_mgr.claim_all(tf_id);

    let mut cont_state = (*fcst.continuation_state).lock().unwrap();

    if !cont_state.should_act(fcst.subchain_idx) {
        jd.tf_mgr.unclaim_batch_size(tf_id, batch_size);
        return;
    }

    if cont_state.advance_to_next {
        cont_state.current_turn = fcst.subchain_idx;
    }

    let mut group_track_iter =
        jd.group_track_manager.lookup_group_track_iter_mut(
            fcst.group_track_iter_ref.list_id,
            fcst.group_track_iter_ref.iter_id,
            &jd.match_set_mgr,
            fcst.actor_id,
        );

    let mut padding_inserted = 0;
    if cont_state.advance_to_next {
        cont_state.rolling_sum -=
            cont_state.produced_on_last_turn[fcst.subchain_idx];
        cont_state.produced_on_last_turn[fcst.subchain_idx] = 0;
        padding_inserted = cont_state.rolling_sum;

        group_track_iter
            .insert_fields(FieldValueRepr::Undefined, padding_inserted);
        cont_state.advance_to_next = false;
    }

    let fields_to_consume = group_track_iter.group_len_rem().min(batch_size);
    jd.tf_mgr
        .unclaim_batch_size(tf_id, batch_size - fields_to_consume);

    cont_state.produced_on_last_turn[fcst.subchain_idx] += fields_to_consume;

    group_track_iter.next_n_fields(fields_to_consume);
    if group_track_iter.is_end_of_group(ps.input_done) {
        cont_state.advance_to_next = true;
        cont_state.rolling_sum +=
            cont_state.produced_on_last_turn[fcst.subchain_idx];

        jd.tf_mgr.inform_transform_batch_available(
            cont_state.subchains[cont_state.next_sc()].trailer_tf_id,
            0,
            false,
        )
    }

    let end_reached = group_track_iter.is_end(ps.input_done);
    group_track_iter.store_iter(fcst.group_track_iter_ref.iter_id);

    let cont_ms_id =
        jd.tf_mgr.transforms[cont_state.continuation_tf_id].match_set_id;
    let sc_ms_id = jd.tf_mgr.transforms[tf_id].match_set_id;
    jd.match_set_mgr.match_sets[cont_ms_id].active_source_ms = Some(sc_ms_id);

    let done = ps.input_done && fcst.subchain_idx == cont_state.last_sc();

    jd.tf_mgr.inform_cross_ms_transform_batch_available(
        &jd.field_mgr,
        &jd.match_set_mgr,
        cont_state.continuation_tf_id,
        fields_to_consume,
        padding_inserted + fields_to_consume,
        done,
    );

    if done {
        jd.tf_mgr.transforms[tf_id].done = true;
    }

    // the 'pass to children' below will deal with the `fields_to_consume`
    // so just the padding is dropped here
    jd.group_track_manager.group_tracks[fcst.group_track_iter_ref.list_id]
        .borrow_mut()
        .drop_leading_fields(true, padding_inserted, end_reached);

    jd.group_track_manager.pass_leading_groups_to_children(
        &jd.match_set_mgr,
        fcst.group_track_iter_ref.list_id,
        fields_to_consume,
        end_reached,
        std::iter::once(
            jd.tf_mgr.transforms[cont_state.continuation_tf_id]
                .input_group_track_id,
        ),
    );
    for cim in &fcst.continuation_field_mappings {
        let sc_field = jd
            .field_mgr
            .get_cow_field_ref(&jd.match_set_mgr, cim.sc_field_id);
        let mut iter = jd
            .field_mgr
            .lookup_iter(cim.sc_field_id, &sc_field, cim.sc_field_iter_id)
            .bounded(0, fields_to_consume + padding_inserted);

        iter.next_n_fields(padding_inserted, true);

        let mut cont_field =
            jd.field_mgr.fields[cim.cont_field_id].borrow_mut();

        while let Some(range) =
            iter.typed_range_fwd(usize::MAX, field_value_flags::DEFAULT)
        {
            cont_field.iter_hall.extend_from_valid_range_re_ref(
                range,
                true,
                false,
                false,
                cim.sc_field_ref_offset_in_cont,
                cim.sc_field_refs_offsets_start_in_cont_field,
            );
        }
        jd.field_mgr
            .store_iter(cim.sc_field_id, cim.sc_field_iter_id, iter);
    }

    // PERF: this is dumb?
    let cont_dummy_field_id = jd.match_set_mgr.get_dummy_field(cont_ms_id);
    jd.field_mgr
        .apply_field_actions(&jd.match_set_mgr, cont_dummy_field_id);
    jd.field_mgr.fields[cont_dummy_field_id]
        .borrow_mut()
        .iter_hall
        .push_undefined(fields_to_consume, true);
}

pub fn create_op_forkcat_with_opts(
    mut subchains: Vec<Vec<(OperatorBaseOptions, OperatorData)>>,
) -> Result<OperatorData, OperatorCreationError> {
    for sc in &mut subchains {
        if sc.is_empty() {
            sc.push((
                OperatorBaseOptions::from_name("nop".into()),
                create_op_nop(),
            ));
        }
    }
    Ok(OperatorData::ForkCat(OpForkCat {
        subchains,
        subchains_start: SubchainIndex::max_value(),
        subchains_end: SubchainIndex::max_value(),
        direct_offset_in_chain: OffsetInChain::max_value(),
        input_mappings: HashMap::default(),
        continuation_vars: IndexVec::new(),
    }))
}

pub fn create_op_forkcat(
    subchains: impl IntoIterator<Item = impl IntoIterator<Item = OperatorData>>,
) -> Result<OperatorData, OperatorCreationError> {
    let subchains = subchains
        .into_iter()
        .map(|sc| {
            sc.into_iter()
                .map(|op_data| {
                    (
                        OperatorBaseOptions::from_name(
                            op_data.default_op_name(),
                        ),
                        op_data,
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect();
    create_op_forkcat_with_opts(subchains)
}

pub fn parse_op_forkcat(
    sess_opts: &mut SessionOptions,
    args: CallExpr,
) -> Result<OperatorData, OperatorCreationError> {
    let mut subchains = Vec::new();
    let mut curr_subchain = Vec::new();
    for expr in CallExprIter::from_args_iter(args.args) {
        let expr = expr?;
        if expr.op_name == "next" {
            expr.reject_args()?;
            subchains.push(curr_subchain);
            curr_subchain = Vec::new();
            continue;
        };
        let op_base = expr.op_base_options();
        let op_data = parse_operator_data(sess_opts, expr)?;
        curr_subchain.push((op_base, op_data));
    }
    subchains.push(curr_subchain);
    create_op_forkcat_with_opts(subchains)
}
