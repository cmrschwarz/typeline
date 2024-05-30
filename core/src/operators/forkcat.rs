use std::collections::HashMap;

use bitvec::vec::BitVec;

use crate::{
    chain::{Chain, SubchainIndex},
    context::SessionData,
    job::{add_transform_to_job, Job, JobData},
    liveness_analysis::{
        LivenessData, Var, VarLivenessSlotGroup, VarLivenessSlotKind,
    },
    options::argument::CliArgIdx,
    record_data::{
        action_buffer::ActorRef,
        field::{FieldId, VOID_FIELD_ID},
        group_track::GroupTrackIterRef,
        match_set::MatchSetId,
    },
    utils::index_vec::IndexVec,
};

use super::{
    errors::{OperatorCreationError, OperatorSetupError},
    operator::{
        OperatorBase, OperatorData, OperatorId, OperatorInstantiation,
        OperatorOffsetInChain, TransformContinuationKind, DUMMY_OP_NOP,
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

type FcSubchainIdx = u32;

type ContinuationVarIdx = u32;

#[derive(Clone, Default)]
pub struct OpForkCat {
    pub subchains_start: SubchainIndex,
    pub subchains_end: SubchainIndex,
    pub continuation: Option<OperatorId>,

    // list of input vars needed in each subchain
    input_mappings: HashMap<Var, BitVec<usize>>,

    // var names accessed by continuation
    continuation_vars: IndexVec<ContinuationVarIdx, Var>,
}

pub struct TfForkCat<'a> {
    op: &'a OpForkCat,
    subchain_match_sets: IndexVec<FcSubchainIdx, MatchSetId>,
    buffered_record_count: usize,
    input_size: usize,
    group_track_iter: GroupTrackIterRef,
    continuation: TransformId,
}

pub struct TfForkCatSubchainTerminator<'a> {
    pub op: &'a OpForkCat,
    continuation_ms_id: MatchSetId,
    // field id in subchain -> field id in continuation
    continuation_var_mappings:
        IndexVec<ContinuationVarIdx, (FieldId, FieldId)>,
}

pub fn parse_op_forkcat(
    value: Option<&[u8]>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    if value.is_some() {
        return Err(OperatorCreationError::new(
            "this operator takes no arguments",
            arg_idx,
        ));
    }
    Ok(OperatorData::ForkCat(OpForkCat::default()))
}

pub fn setup_op_forkcat(
    chain: &Chain,
    _op_base: &OperatorBase,
    op: &mut OpForkCat,
    offset_in_chain: OperatorOffsetInChain,
) -> Result<(), OperatorSetupError> {
    if op.subchains_end == 0 {
        op.subchains_end = chain.subchains.len() as u32;
    }
    op.continuation =
        chain.operators.get(offset_in_chain as usize + 1).copied();
    Ok(())
}

pub fn setup_op_forkcat_liveness_data(
    _sess: &SessionData,
    op: &mut OpForkCat,
    op_id: OperatorId,
    ld: &LivenessData,
) {
    let sc_count = (op.subchains_end - op.subchains_start) as usize;
    let bb_id = ld.operator_liveness_data[op_id].basic_block_id;
    let bb = &ld.basic_blocks[bb_id];
    let successors = ld.get_var_liveness_ored(
        VarLivenessSlotGroup::Global,
        bb.successors.iter(),
    );
    // TODO: introduce direct reads (not affected by field refs)
    // to reduce this set here
    let successors_reads = successors.get_slot(VarLivenessSlotKind::Reads);
    for var_id in successors_reads.iter_ones() {
        op.continuation_vars.push(ld.vars[var_id]);
    }
    for (sc_idx, &callee_id) in bb.calls.iter().enumerate() {
        let call_reads = ld.get_var_liveness_slot(
            callee_id,
            VarLivenessSlotGroup::Global,
            VarLivenessSlotKind::Reads,
        );
        for i in call_reads.iter_ones() {
            op.input_mappings
                .entry(ld.vars[i])
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
    let input_field = tf_state.input_field;
    let sc_count = (op.subchains_end - op.subchains_start) as usize;
    let cont_ms_id = job.job_data.match_set_mgr.add_match_set();

    let cont_group_track = job.job_data.group_track_manager.add_group_track(
        None,
        cont_ms_id,
        ActorRef::Unconfirmed(0),
    );

    let cont_input_field = job.job_data.field_mgr.add_field(
        &mut job.job_data.match_set_mgr,
        cont_ms_id,
        None,
        ActorRef::default(),
    );

    let cont_inst = if let Some(cont) = op.continuation {
        job.setup_transforms_from_op(
            cont_ms_id,
            cont,
            cont_input_field,
            cont_group_track,
            None,
            &HashMap::default(),
        )
    } else {
        job.setup_transforms_for_op_iter(
            std::iter::once((tf_state.op_id.unwrap(), op_base, &DUMMY_OP_NOP)),
            cont_ms_id,
            cont_input_field,
            cont_group_track,
            None,
            &HashMap::default(),
        )
    };

    let tf_data = TransformData::ForkCat(TfForkCat {
        continuation: cont_inst.tfs_begin,
        op,
        input_size: 0,
        buffered_record_count: 0,
        group_track_iter: job
            .job_data
            .group_track_manager
            .claim_group_track_iter_ref(tf_state.input_group_track_id),
        subchain_match_sets: Default::default(),
    });
    let fc_tf_id = add_transform_to_job(
        &mut job.job_data,
        &mut job.transform_data,
        tf_state,
        tf_data,
    );

    let mut subchain_match_sets = IndexVec::new();

    for (fc_sc_idx, sc_idx) in
        (op.subchains_start..op.subchains_end).enumerate()
    {
        let sc_ms_id = setup_subchain(
            op,
            cont_ms_id,
            sc_count,
            job,
            op_base,
            sc_idx,
            fc_sc_idx,
            input_field,
            fc_tf_id,
        );
        subchain_match_sets.push(sc_ms_id);
    }

    let TransformData::ForkCat(fc) = &mut job.transform_data[fc_tf_id] else {
        unreachable!()
    };

    fc.subchain_match_sets = subchain_match_sets;

    OperatorInstantiation {
        tfs_begin: fc_tf_id,
        tfs_end: cont_inst.tfs_end,
        next_input_field: cont_inst.next_input_field,
        next_group_track: cont_inst.next_group_track,
        continuation: TransformContinuationKind::SelfExpanded,
    }
}

fn setup_subchain(
    op: &OpForkCat,
    cont_ms_id: crate::utils::debuggable_nonmax::DebuggableNonMaxUsize,
    sc_count: usize,
    job: &mut Job,
    op_base: &OperatorBase,
    sc_idx: u32,
    fc_sc_idx: usize,
    input_field: FieldId,
    fc_tf_id: TransformId,
) -> MatchSetId {
    let mut sc_terminator_tf = TfForkCatSubchainTerminator {
        op,
        continuation_ms_id: cont_ms_id,
        continuation_var_mappings: IndexVec::with_capacity(sc_count),
    };

    let sc_ms_id = job.job_data.match_set_mgr.add_match_set();

    let sc_group_track = job.job_data.group_track_manager.add_group_track(
        None,
        cont_ms_id,
        ActorRef::Unconfirmed(0),
    );

    let sc_chain_id = job.job_data.session_data.chains
        [op_base.chain_id.unwrap()]
    .subchains[sc_idx];

    let mut sc_input_field = VOID_FIELD_ID;

    for (&var, needing_subchains) in &op.input_mappings {
        if !needing_subchains[fc_sc_idx] {
            continue;
        }
        let field_id = job.job_data.field_mgr.get_cross_ms_cow_field(
            &mut job.job_data.match_set_mgr,
            sc_ms_id,
            input_field,
        );
        if var == Var::BBInput {
            sc_input_field = field_id;
        }
    }

    let sc_cont = job.setup_transforms_for_chain(
        sc_ms_id,
        sc_chain_id,
        sc_input_field,
        sc_group_track,
        Some(fc_tf_id),
        &HashMap::default(),
    );

    for var in &op.continuation_vars {
        let sc_ms = &job.job_data.match_set_mgr.match_sets[sc_ms_id];
        let field_cow_id = if let Some(field_name) = var.get_name() {
            if let Some(&field_id) = sc_ms.field_name_map.get(&field_name) {
                job.job_data.field_mgr.get_cross_ms_cow_field(
                    &mut job.job_data.match_set_mgr,
                    cont_ms_id,
                    field_id,
                )
            } else {
                // TODO: make this a per match set thing
                VOID_FIELD_ID
            }
        } else {
            sc_cont.next_input_field
        };

        sc_terminator_tf
            .continuation_var_mappings
            .push((field_cow_id, 0));
    }

    sc_ms_id
}

pub fn handle_tf_forkcat(
    jd: &mut JobData,
    tf_id: TransformId,
    fc: &mut TfForkCat,
) {
    let (batch_size, ps) = jd.tf_mgr.claim_batch(tf_id);
    fc.input_size += batch_size;
    let tf = &jd.tf_mgr.transforms[tf_id];
    let curr_subchain_start = tf.successor.unwrap();
    let sc_group_track =
        jd.tf_mgr.transforms[curr_subchain_start].input_group_track_id;

    jd.tf_mgr.inform_cross_ms_transform_batch_available(
        &jd.field_mgr,
        &jd.match_set_mgr,
        curr_subchain_start,
        batch_size,
        ps.input_done,
    );
}

pub fn create_op_forkcat() -> OperatorData {
    OperatorData::ForkCat(OpForkCat::default())
}
