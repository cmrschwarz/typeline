use std::collections::HashMap;

use crate::{
    chain::ChainId,
    cli::call_expr::CallExpr,
    context::SessionSetupData,
    job::{Job, JobData},
    options::operator_base_options::OperatorBaseOptionsInterned,
    record_data::{
        field::FieldId, group_track::GroupTrackId, match_set::MatchSetId,
    },
    utils::indexing_type::IndexingType,
};

use super::{
    errors::{OperatorCreationError, OperatorSetupError},
    operator::{
        OffsetInChain, OperatorBase, OperatorData, OperatorDataId, OperatorId,
        OperatorInstantiation, OperatorOffsetInChain,
    },
    transform::{TransformData, TransformId, TransformState},
};

#[derive(Clone)]
pub struct OpCall {
    pub lazy: bool,
    pub target_name: String,
    pub target_resolved: Option<ChainId>,
}
pub struct TfCall {
    pub target: ChainId,
}

pub fn parse_op_call(
    expr: &CallExpr,
) -> Result<OperatorData, OperatorCreationError> {
    let target = expr.require_single_string_arg()?;
    Ok(OperatorData::Call(OpCall {
        lazy: true,
        target_name: target.to_owned(),
        target_resolved: None,
    }))
}

pub fn setup_op_call(
    op: &mut OpCall,
    sess: &mut SessionSetupData,
    curr_chain_id: ChainId,
    offset_in_chain: OperatorOffsetInChain,
    op_opts: OperatorBaseOptionsInterned,
    op_data_id: OperatorDataId,
) -> Result<OperatorId, OperatorSetupError> {
    let op_id = sess.add_op_from_offset_in_chain(
        curr_chain_id,
        offset_in_chain,
        op_opts,
        op_data_id,
    );

    if op.target_resolved.is_some() {
        // this happens in case of call targets caused by labels ending the
        // chain
        debug_assert!(!op.lazy);
    } else if let Some(target) = sess
        .string_store
        .lookup_str(&op.target_name)
        .and_then(|sse| sess.chain_labels.get(&sse))
    {
        op.target_resolved = Some(*target);
    } else {
        return Err(OperatorSetupError::new_s(
            format!("unknown chain label '{}'", op.target_name),
            op_id,
        ));
    }

    Ok(op_id)
}

pub fn create_op_call(name: String) -> OperatorData {
    OperatorData::Call(OpCall {
        lazy: true,
        target_name: name,
        target_resolved: None,
    })
}

pub fn create_op_call_eager(target: ChainId) -> OperatorData {
    OperatorData::Call(OpCall {
        lazy: false,
        target_name: String::new(),
        target_resolved: Some(target),
    })
}

pub fn build_tf_call<'a>(
    _jd: &mut JobData,
    _op_base: &OperatorBase,
    op: &OpCall,
    _tf_state: &mut TransformState,
) -> TransformData<'a> {
    TransformData::Call(TfCall {
        target: op.target_resolved.unwrap(),
    })
}
pub(crate) fn handle_eager_call_expansion(
    op: &OpCall,
    sess: &mut Job,
    ms_id: MatchSetId,
    input_field: FieldId,
    group_track: GroupTrackId,
    predecessor_tf: Option<TransformId>,
) -> OperatorInstantiation {
    let chain =
        &sess.job_data.session_data.chains[op.target_resolved.unwrap()];
    sess.setup_transforms_from_op(
        ms_id,
        chain.operators[OffsetInChain::zero()],
        input_field,
        group_track,
        predecessor_tf,
        &HashMap::default(),
    )
}

pub(crate) fn handle_lazy_call_expansion(sess: &mut Job, tf_id: TransformId) {
    let tf = &mut sess.job_data.tf_mgr.transforms[tf_id];
    let old_successor = tf.successor;
    let input_field = tf.input_field;
    let input_group_track = tf.input_group_track_id;
    let ms_id = tf.match_set_id;
    let TransformData::Call(call) = &sess.transform_data[tf_id] else {
        unreachable!()
    };
    // TODO: do we need a prebound output so succesor can keep it's input
    // field?
    let instantiation = sess.setup_transforms_from_op(
        ms_id,
        sess.job_data.session_data.chains[call.target].operators
            [OffsetInChain::zero()],
        input_field,
        input_group_track,
        Some(tf_id),
        &HashMap::default(),
    );
    sess.job_data.tf_mgr.transforms[instantiation.tfs_end].successor =
        old_successor;
    let (batch_size, _input_done) = sess.job_data.tf_mgr.claim_all(tf_id);
    // TODO: is this fine considering e.g. forkcat with no predecessors?
    sess.job_data.unlink_transform(tf_id, batch_size);
}
