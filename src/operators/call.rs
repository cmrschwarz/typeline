use std::collections::HashMap;

use bstr::ByteSlice;

use crate::{
    chain::{ChainId, INVALID_CHAIN_ID},
    job_session::{FieldId, JobData, JobSession, MatchSetId},
    options::argument::CliArgIdx,
    utils::{
        identity_hasher::BuildIdentityHasher,
        string_store::{StringStore, StringStoreEntry},
    },
};

use super::{
    errors::{OperatorCreationError, OperatorSetupError},
    operator::{OperatorBase, OperatorData, OperatorId},
    transform::{TransformData, TransformId, TransformState},
};

#[derive(Clone)]
pub struct OpCall {
    pub lazy: bool,
    pub target_name: String,
    pub target_resolved: ChainId,
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
                "missing argument with key for select",
                arg_idx,
            )
        })?
        .to_str()
        .map_err(|_| {
            OperatorCreationError::new(
                "target label must be valid UTF-8",
                arg_idx,
            )
        })?;
    Ok(OperatorData::Call(OpCall {
        lazy: true,
        target_name: value_str.to_owned(),
        target_resolved: INVALID_CHAIN_ID,
    }))
}

pub fn setup_op_call(
    chain_labels: &HashMap<StringStoreEntry, ChainId, BuildIdentityHasher>,
    string_store: &StringStore,
    op: &mut OpCall,
    op_id: OperatorId,
) -> Result<(), OperatorSetupError> {
    if op.target_resolved != INVALID_CHAIN_ID {
        // this happens in case of call targets caused by labels ending the
        // chain
        debug_assert!(!op.lazy);
        return Ok(());
    }
    if let Some(target) = string_store
        .lookup_str(&op.target_name)
        .and_then(|sse| chain_labels.get(&sse))
    {
        op.target_resolved = *target;
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
        target_resolved: INVALID_CHAIN_ID,
    })
}

pub fn create_op_call_eager(target: ChainId) -> OperatorData {
    OperatorData::Call(OpCall {
        lazy: false,
        target_name: String::new(),
        target_resolved: target,
    })
}

pub fn setup_tf_call(
    _sess: &mut JobData,
    _op_base: &OperatorBase,
    op: &OpCall,
    _tf_state: &mut TransformState,
) -> TransformData<'static> {
    TransformData::Call(TfCall {
        target: op.target_resolved,
    })
}
pub(crate) fn handle_eager_call_expansion(
    sess: &mut JobSession,
    op_id: OperatorId,
    ms_id: MatchSetId,
    input_field: FieldId,
    predecessor_tf: Option<TransformId>,
) -> (TransformId, TransformId, bool) {
    if let OperatorData::Call(op) =
        &sess.job_data.session_data.operator_data[op_id as usize]
    {
        let chain =
            &sess.job_data.session_data.chains[op.target_resolved as usize];
        sess.setup_transforms_from_op(
            ms_id,
            chain.operators[0],
            input_field,
            predecessor_tf,
        )
    } else {
        unreachable!();
    }
}

pub(crate) fn handle_lazy_call_expansion(
    sess: &mut JobSession,
    tf_id: TransformId,
) {
    let tf = &mut sess.job_data.tf_mgr.transforms[tf_id];
    let input_field = tf.input_field;
    let ms_id = tf.match_set_id;
    let call =
        if let TransformData::Call(tf) = &sess.transform_data[tf_id.get()] {
            tf
        } else {
            unreachable!()
        };
    let (target_tf, _end_tf, _end_reachable) = sess.setup_transforms_from_op(
        ms_id,
        sess.job_data.session_data.chains[call.target as usize].operators[0],
        input_field,
        Some(tf_id),
    );
    sess.job_data.tf_mgr.transforms[target_tf].predecessor = Some(tf_id);
    sess.job_data.tf_mgr.transforms[tf_id].successor = Some(target_tf);
    let (batch_size, _input_done) = sess.job_data.tf_mgr.claim_all(tf_id);
    sess.job_data.unlink_transform(tf_id, batch_size);
}
