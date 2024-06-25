use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use smallvec::SmallVec;

use crate::{
    chain::{ChainId, SubchainIndex},
    cli::{
        call_expr::CallExpr, call_expr_iter::CallExprIter, parse_operator_data,
    },
    context::{ContextData, SessionSetupData},
    job::{Job, JobData},
    liveness_analysis::{
        LivenessData, VarId, VarLivenessSlotGroup, VarLivenessSlotKind,
    },
    operators::operator::OffsetInChain,
    options::{
        operator_base_options::{
            OperatorBaseOptions, OperatorBaseOptionsInterned,
        },
        session_options::SessionOptions,
    },
    record_data::{
        action_buffer::ActorRef, field::FieldId, group_track::GroupTrackId,
        iter_hall::IterId, match_set::MatchSetId,
    },
    utils::{
        index_vec::IndexVec,
        indexing_type::{IndexingType, IndexingTypeRange},
        string_store::StringStoreEntry,
    },
};

use super::{
    errors::{OperatorCreationError, OperatorSetupError},
    operator::{
        OperatorBase, OperatorData, OperatorDataId, OperatorId,
        OperatorOffsetInChain,
    },
    terminator::add_terminator_tf_cont_dependant,
    transform::{TransformData, TransformId, TransformState},
};

pub struct OpFork {
    pub subchains: Vec<Vec<(OperatorBaseOptions, OperatorData)>>,

    pub subchains_start: SubchainIndex,
    pub subchains_end: SubchainIndex,
    pub accessed_fields_per_subchain:
        IndexVec<SubchainIndex, HashSet<Option<StringStoreEntry>>>,
}

pub struct TfForkFieldMapping {
    pub source_iter_id: IterId,
    pub targets_cow: SmallVec<[FieldId; 4]>,
    pub targets_data_cow: SmallVec<[FieldId; 4]>,
    pub targets_copy: SmallVec<[FieldId; 4]>,
}

pub struct ForkTarget {
    pub tf_id: TransformId,
    pub gt_id: GroupTrackId,
}

pub struct TfFork<'a> {
    pub expanded: bool,
    pub targets: Vec<ForkTarget>,
    pub accessed_fields_per_subchain:
        &'a IndexVec<SubchainIndex, HashSet<Option<StringStoreEntry>>>,
}

pub fn setup_op_fork(
    op: &mut OpFork,
    sess: &mut SessionSetupData,
    chain_id: ChainId,
    offset_in_chain: OperatorOffsetInChain,
    op_base_opts_interned: OperatorBaseOptionsInterned,
    op_data_id: OperatorDataId,
) -> Result<OperatorId, OperatorSetupError> {
    let op_id = sess.add_op_from_offset_in_chain(
        chain_id,
        offset_in_chain,
        op_base_opts_interned,
        op_data_id,
    );

    if !matches!(offset_in_chain, OperatorOffsetInChain::Direct(_)) {
        return Err(OperatorSetupError::new(
            "operator `fork` cannot be part of an aggregation",
            op_id,
        ));
    };

    op.subchains_start = sess.chains[chain_id].subchains.next_idx();

    for sc in std::mem::take(&mut op.subchains) {
        sess.create_subchain(chain_id, sc)?;
    }

    op.subchains_end = sess.chains[chain_id].subchains.next_idx();

    Ok(op_id)
}

pub fn setup_op_fork_liveness_data(
    op: &mut OpFork,
    op_id: OperatorId,
    ld: &LivenessData,
) {
    let bb_id = ld.operator_liveness_data[op_id].basic_block_id;
    debug_assert!(ld.basic_blocks[bb_id].calls.is_empty());
    let bb = &ld.basic_blocks[bb_id];
    for &callee_bb_id in &bb.successors {
        let mut accessed_vars = HashSet::new();
        for var_id in ld
            .get_var_liveness_slot(
                callee_bb_id,
                VarLivenessSlotGroup::Global,
                VarLivenessSlotKind::Reads,
            )
            .iter_ones()
            .map(VarId::from_usize)
        {
            accessed_vars.insert(ld.vars[var_id].get_name());
        }
        op.accessed_fields_per_subchain.push(accessed_vars);
    }
}

pub fn build_tf_fork<'a>(
    _jd: &mut JobData,
    _op_base: &OperatorBase,
    op: &'a OpFork,
    _tf_state: &mut TransformState,
) -> TransformData<'a> {
    TransformData::Fork(TfFork {
        expanded: false,
        targets: Vec::new(),
        accessed_fields_per_subchain: &op.accessed_fields_per_subchain,
    })
}

pub fn handle_tf_fork(jd: &mut JobData, tf_id: TransformId, sp: &mut TfFork) {
    let (batch_size, ps) = jd.tf_mgr.claim_all(tf_id);
    if ps.input_done {
        jd.tf_mgr.declare_transform_done(tf_id);
    }
    if ps.next_batch_ready {
        jd.tf_mgr.push_tf_in_ready_stack(tf_id);
    }

    let tf = &jd.tf_mgr.transforms[tf_id];

    jd.group_track_manager.pass_leading_groups_to_children(
        &jd.match_set_mgr,
        tf.input_group_track_id,
        batch_size,
        ps.input_done,
        sp.targets.iter().map(|tgt| tgt.gt_id),
    );

    // we reverse to make sure that the first subchain ends up
    // on top of the stack and gets executed first
    for tgt in sp.targets.iter().rev() {
        jd.tf_mgr.inform_cross_ms_transform_batch_available(
            &jd.field_mgr,
            &jd.match_set_mgr,
            tgt.tf_id,
            batch_size,
            batch_size,
            ps.input_done,
        );
    }
}

pub(crate) fn handle_fork_expansion(
    sess: &mut Job,
    tf_id: TransformId,
    _ctx: Option<&Arc<ContextData>>,
) {
    // we have to temporarily move the targets out of fork so we can modify
    // sess while accessing them
    let mut targets = Vec::new();

    let tf = &sess.job_data.tf_mgr.transforms[tf_id];
    let fork_input_field_id = tf.input_field;
    let fork_ms_id = tf.match_set_id;
    let fork_op_id = tf.op_id.unwrap();
    let fork_chain_id =
        sess.job_data.session_data.operator_bases[fork_op_id].chain_id;

    for i in IndexingTypeRange::new(
        SubchainIndex::zero()
            ..sess.job_data.session_data.chains[fork_chain_id]
                .subchains
                .next_idx(),
    ) {
        let target = setup_fork_subchain(
            sess,
            fork_chain_id,
            i,
            tf_id,
            fork_ms_id,
            fork_input_field_id,
        );
        targets.push(target);
    }

    sess.log_state("expanded fork");
    if let TransformData::Fork(ref mut fork) = sess.transform_data[tf_id] {
        fork.targets = targets;
        fork.expanded = true;
    } else {
        unreachable!();
    }
}

fn setup_fork_subchain(
    job: &mut Job,
    fork_chain_id: ChainId,
    subchain_index: SubchainIndex,
    tf_id: TransformId,
    fork_ms_id: MatchSetId,
    fork_input_field_id: u32,
) -> ForkTarget {
    // actual chain id as opposed to the index to the nth subchain
    let subchain_id = job.job_data.session_data.chains[fork_chain_id]
        .subchains[subchain_index];
    let target_ms_id = job
        .job_data
        .match_set_mgr
        .add_match_set(&mut job.job_data.field_mgr);

    let fork_chain_dummy_field =
        job.job_data.match_set_mgr.get_dummy_field(fork_ms_id);
    let sc_dummy_field =
        job.job_data.match_set_mgr.get_dummy_field(target_ms_id);

    job.job_data.field_mgr.setup_cow_between_fields(
        &mut job.job_data.match_set_mgr,
        fork_chain_dummy_field,
        sc_dummy_field,
    );

    let target_group_track = job.job_data.group_track_manager.add_group_track(
        None,
        target_ms_id,
        ActorRef::Unconfirmed(0),
    );

    let field_access_mapping =
        if let TransformData::Fork(f) = &job.transform_data[tf_id] {
            &f.accessed_fields_per_subchain[subchain_index]
        } else {
            unreachable!();
        };
    let mut chain_input_field = None;
    for &name in field_access_mapping {
        let src_field_id;
        if let Some(name) = name {
            if let Some(field) = job.job_data.match_set_mgr.match_sets
                [fork_ms_id]
                .field_name_map
                .get(&name)
            {
                // the input field is always first in this iterator
                debug_assert!(*field != fork_input_field_id);
                src_field_id = *field;
            } else {
                continue;
            };
        } else {
            debug_assert!(chain_input_field.is_none());
            src_field_id = fork_input_field_id;
        };

        let mut src_field =
            job.job_data.field_mgr.fields[src_field_id].borrow_mut();

        drop(src_field);
        let target_field_id = job.job_data.field_mgr.get_cross_ms_cow_field(
            &mut job.job_data.match_set_mgr,
            target_ms_id,
            src_field_id,
        );
        src_field = job.job_data.field_mgr.fields[src_field_id].borrow_mut();

        if name.is_none() {
            chain_input_field = Some(target_field_id);
        }
        drop(src_field);
    }
    let input_field = chain_input_field.unwrap_or(sc_dummy_field);
    let start_op_id = job.job_data.session_data.chains[subchain_id].operators
        [OffsetInChain::zero()];
    let instantiation = job.setup_transforms_from_op(
        target_ms_id,
        start_op_id,
        input_field,
        target_group_track,
        None,
        &HashMap::default(),
    );
    add_terminator_tf_cont_dependant(
        job,
        instantiation.tfs_end,
        instantiation.continuation,
    );
    ForkTarget {
        tf_id: instantiation.tfs_begin,
        gt_id: target_group_track,
    }
}

pub fn create_op_fork_with_opts(
    subchains: Vec<Vec<(OperatorBaseOptions, OperatorData)>>,
) -> Result<OperatorData, OperatorCreationError> {
    Ok(OperatorData::Fork(OpFork {
        subchains_start: SubchainIndex::max_value(),
        subchains_end: SubchainIndex::max_value(),
        accessed_fields_per_subchain: IndexVec::new(),
        subchains,
    }))
}

pub fn create_op_fork(
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
    create_op_fork_with_opts(subchains)
}

pub fn parse_op_fork(
    sess_opts: &mut SessionOptions,
    expr: CallExpr,
) -> Result<OperatorData, OperatorCreationError> {
    let mut subchains = Vec::new();
    let mut curr_subchain = Vec::new();
    for expr in CallExprIter::from_args_iter(expr.args) {
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
    create_op_fork_with_opts(subchains)
}
