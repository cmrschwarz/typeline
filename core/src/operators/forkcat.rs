use std::collections::HashMap;

use bitvec::vec::BitVec;

use crate::{
    chain::{Chain, SubchainIndex},
    context::SessionData,
    index_newtype,
    job::{add_transform_to_job, Job, JobData},
    liveness_analysis::{
        LivenessData, Var, VarId, VarLivenessSlotGroup, VarLivenessSlotKind,
    },
    options::argument::CliArgIdx,
    record_data::{
        action_buffer::ActorRef,
        field::{FieldId, FieldRefOffset, VOID_FIELD_ID},
        field_value::FieldReference,
        group_track::GroupTrackId,
        push_interface::PushInterface,
    },
    utils::{
        index_vec::{IndexSlice, IndexVec},
        indexing_type::{IndexingType, IndexingTypeRange},
    },
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

index_newtype! {
    pub struct FcSubchainIdx(u32);
    pub struct ContinuationVarIdx(u32);
}

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

pub struct SubchainEntry {
    pub start_tf: TransformId,
    pub group_track_id: GroupTrackId,
}

pub struct TfForkCat {
    pub subchains: IndexVec<FcSubchainIdx, SubchainEntry>,
}

pub struct TfForkCatSubchainTrailer<'a> {
    pub op: &'a OpForkCat,
    continuation_tf_id: TransformId,
    // field ref offset of subchain field in continuation field (with id)
    continuation_input_field: FieldId,
    continuation_field_offsets: FieldRefOffset,
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
    op_base: &OperatorBase,
    op: &mut OpForkCat,
    _op_id: OperatorId,
) -> Result<(), OperatorSetupError> {
    if op.subchains_end == SubchainIndex::zero() {
        op.subchains_end = chain.subchains.next_idx();
    }
    op.continuation = chain
        .operators
        .get(op_base.offset_in_chain + OperatorOffsetInChain::one())
        .copied();
    Ok(())
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
    let input_field = tf_state.input_field;
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
    let mut continuation_var_mapping =
        IndexVec::<ContinuationVarIdx, FieldId>::new();

    for cv in &op.continuation_vars {
        let field_id = if cv == &Var::BBInput {
            cont_input_field
        } else {
            job.job_data.field_mgr.create_same_ms_cow_field(
                &mut job.job_data.match_set_mgr,
                cont_input_field,
                cv.get_name(),
            )
        };
        continuation_var_mapping.push(field_id);
    }

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
        subchains: IndexVec::default(),
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
            cont_inst.tfs_begin,
            cont_input_field,
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

    let TransformData::ForkCat(fc) = &mut job.transform_data[fc_tf_id] else {
        unreachable!()
    };

    fc.subchains = subchains;

    job.job_data.tf_mgr.transforms[fc_tf_id].successor =
        Some(cont_inst.tfs_begin);

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
    cont_tf_id: TransformId,
    cont_input_field: FieldId,
    job: &mut Job<'a>,
    op_base: &OperatorBase,
    sc_idx: SubchainIndex,
    fc_sc_idx: FcSubchainIdx,
    fc_input_field: FieldId,
    fc_tf_id: TransformId,
    continuation_vars: &IndexSlice<ContinuationVarIdx, FieldId>,
) -> SubchainEntry {
    let cont_ms_id = job.job_data.tf_mgr.transforms[cont_tf_id].match_set_id;

    let ms_id = job.job_data.match_set_mgr.add_match_set();

    let group_track = job.job_data.group_track_manager.add_group_track(
        None,
        cont_ms_id,
        ActorRef::Unconfirmed(0),
    );

    let sc_chain_id = job.job_data.session_data.chains
        [op_base.chain_id.unwrap()]
    .subchains[sc_idx];

    let mut sc_input_field = VOID_FIELD_ID;

    for (&var, needing_subchains) in &op.input_mappings {
        if !needing_subchains[fc_sc_idx.into_usize()] {
            continue;
        }
        let field_id = job.job_data.field_mgr.get_cross_ms_cow_field(
            &mut job.job_data.match_set_mgr,
            ms_id,
            fc_input_field,
        );
        if var == Var::BBInput {
            sc_input_field = field_id;
        }
    }

    let instantiation = job.setup_transforms_for_chain(
        ms_id,
        sc_chain_id,
        sc_input_field,
        group_track,
        Some(fc_tf_id),
        &HashMap::default(),
    );

    let mut field_ref_offset = usize::MAX;

    for (i, var) in op.continuation_vars.iter_enumerated() {
        let sc_ms = &job.job_data.match_set_mgr.match_sets[ms_id];
        let field_id = if let Some(field_name) = var.get_name() {
            if let Some(&field_id) = sc_ms.field_name_map.get(&field_name) {
                field_id
            } else {
                // TODO: make this a per match set thing
                VOID_FIELD_ID
            }
        } else {
            instantiation.next_input_field
        };

        let field_cow_tgt = job.job_data.field_mgr.get_cross_ms_cow_field(
            &mut job.job_data.match_set_mgr,
            cont_ms_id,
            field_id,
        );

        let cont_field_id = continuation_vars[i];
        let mut cont_field =
            job.job_data.field_mgr.fields[cont_field_id].borrow_mut();
        debug_assert!(
            field_ref_offset == usize::MAX
                || field_ref_offset == cont_field.field_refs.len(),
            "field ref offset must be the same for all continuation vars"
        );
        field_ref_offset = cont_field.field_refs.len();
        cont_field.field_refs.push(field_cow_tgt);
    }

    let desired_batch_size = job.job_data.session_data.chains
        [op_base.chain_id.unwrap()]
    .settings
    .default_batch_size;

    let terminator_tf = TfForkCatSubchainTrailer::<'a> {
        op,
        continuation_tf_id: cont_tf_id,
        continuation_input_field: cont_input_field,
        continuation_field_offsets: field_ref_offset as FieldRefOffset,
    };

    let tf_id = add_transform_to_job(
        &mut job.job_data,
        &mut job.transform_data,
        TransformState::new(
            instantiation.next_input_field,
            instantiation.next_input_field,
            ms_id,
            desired_batch_size,
            None,
            instantiation.next_group_track,
        ),
        TransformData::ForkCatSubchainTrailer(terminator_tf),
    );

    job.job_data.tf_mgr.transforms[instantiation.tfs_begin].successor =
        Some(tf_id);

    SubchainEntry {
        start_tf: instantiation.tfs_begin,
        group_track_id: group_track,
    }
}

pub fn handle_tf_forkcat(
    jd: &mut JobData,
    tf_id: TransformId,
    fc: &mut TfForkCat,
) {
    let (batch_size, ps) = jd.tf_mgr.claim_batch(tf_id);

    for sc in &fc.subchains {
        // PERF: maybe provide a bulk version of this?
        jd.tf_mgr.inform_cross_ms_transform_batch_available(
            &jd.field_mgr,
            &jd.match_set_mgr,
            sc.start_tf,
            batch_size,
            ps.input_done,
        );
    }
    jd.group_track_manager.copy_all_fields_to_children(
        jd.tf_mgr.transforms[tf_id].input_group_track_id,
        fc.subchains.iter().map(|sc| sc.group_track_id),
    );
}

pub fn handle_tf_forcat_subchain_trailer(
    jd: &mut JobData,
    tf_id: TransformId,
    fc: &mut TfForkCatSubchainTrailer,
) {
    let (batch_size, ps) = jd.tf_mgr.claim_batch(tf_id);
    jd.tf_mgr.inform_cross_ms_transform_batch_available(
        &jd.field_mgr,
        &jd.match_set_mgr,
        fc.continuation_tf_id,
        batch_size,
        ps.input_done,
    );
    jd.field_mgr.fields[fc.continuation_input_field]
        .borrow_mut()
        .iter_hall
        .push_field_reference(
            FieldReference::new(fc.continuation_field_offsets),
            batch_size,
            true,
            true,
        );
    jd.match_set_mgr.update_same_ms_cow_targets(
        &jd.field_mgr,
        jd.tf_mgr.transforms[fc.continuation_tf_id].match_set_id,
    );
}

pub fn create_op_forkcat() -> OperatorData {
    OperatorData::ForkCat(OpForkCat::default())
}
