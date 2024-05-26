use std::collections::HashMap;

use bstr::ByteSlice;

use crate::{
    chain::ChainId,
    job::{Job, JobData},
    options::argument::CliArgIdx,
    record_data::{
        field::FieldId, group_track::GroupTrackId,
        match_set::MatchSetId,
    },
    utils::{
        identity_hasher::BuildIdentityHasher,
        string_store::{StringStore, StringStoreEntry},
    },
};

use super::{
    errors::{OperatorCreationError, OperatorSetupError},
    operator::{
        OperatorBase, OperatorData, OperatorId, OperatorInstantiation,
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
    value: Option<&[u8]>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    let value_str = value
        .ok_or_else(|| {
            OperatorCreationError::new(
                "missing argument with target chain name",
                arg_idx,
            )
        })?
        .to_str()
        .map_err(|_| {
            OperatorCreationError::new(
                "target chain name must be valid UTF-8",
                arg_idx,
            )
        })?;
    Ok(OperatorData::Call(OpCall {
        lazy: true,
        target_name: value_str.to_owned(),
        target_resolved: None,
    }))
}

pub fn setup_op_call(
    chain_labels: &HashMap<StringStoreEntry, ChainId, BuildIdentityHasher>,
    string_store: &StringStore,
    op: &mut OpCall,
    op_id: OperatorId,
) -> Result<(), OperatorSetupError> {
    if op.target_resolved.is_some() {
        // this happens in case of call targets caused by labels ending the
        // chain
        debug_assert!(!op.lazy);
        return Ok(());
    }
    if let Some(target) = string_store
        .lookup_str(&op.target_name)
        .and_then(|sse| chain_labels.get(&sse))
    {
        op.target_resolved = Some(*target);
        Ok(())
    } else {
        Err(OperatorSetupError::new_s(
            format!("unknown chain label '{}'", op.target_name),
            op_id,
        ))
    }
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
    let chain = &sess.job_data.session_data.chains
        [op.target_resolved.unwrap() as usize];
    sess.setup_transforms_from_op(
        ms_id,
        chain.operators[0],
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
    let TransformData::Call(call) = &sess.transform_data[tf_id.get()] else {
        unreachable!()
    };
    // TODO: do we need a prebound output so succesor can keep it's input
    // field?
    let instantiation = sess.setup_transforms_from_op(
        ms_id,
        sess.job_data.session_data.chains[call.target as usize].operators[0],
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
