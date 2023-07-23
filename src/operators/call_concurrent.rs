use std::collections::HashMap;

use bstr::ByteSlice;

use crate::{
    chain::{ChainId, INVALID_CHAIN_ID},
    job_session::{JobData, JobSession},
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
pub struct OpCallConcurrent {
    pub target_name: String,
    pub target_resolved: ChainId,
}
pub struct TfCallConcurrent {
    pub expanded: bool,
    pub target: ChainId,
}

pub fn parse_op_call_concurrent(
    value: Option<&[u8]>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    let value_str = value
        .ok_or_else(|| OperatorCreationError::new("missing argument with key for select", arg_idx))?
        .to_str()
        .map_err(|_| OperatorCreationError::new("target label must be valid UTF-8", arg_idx))?;
    Ok(OperatorData::CallConcurrent(OpCallConcurrent {
        target_name: value_str.to_owned(),
        target_resolved: INVALID_CHAIN_ID,
    }))
}

pub fn setup_op_call_concurrent(
    chain_labels: &HashMap<StringStoreEntry, ChainId, BuildIdentityHasher>,
    string_store: &mut StringStore,
    op: &mut OpCallConcurrent,
    op_id: OperatorId,
) -> Result<(), OperatorSetupError> {
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

pub fn create_op_callcc(name: String) -> OperatorData {
    OperatorData::CallConcurrent(OpCallConcurrent {
        target_name: name,
        target_resolved: INVALID_CHAIN_ID,
    })
}

pub fn setup_tf_call_concurrent(
    _sess: &mut JobData,
    _op_base: &OperatorBase,
    op: &OpCallConcurrent,
    _tf_state: &mut TransformState,
) -> TransformData<'static> {
    TransformData::CallConcurrent(TfCallConcurrent {
        target: op.target_resolved,
        expanded: false,
    })
}

pub(crate) fn handle_call_concurrent_expansion<'a>(sess: &mut JobSession, tf_id: TransformId) {
    let tf = &mut sess.job_data.tf_mgr.transforms[tf_id];
    let input_field = tf.input_field;
    let ms_id = tf.match_set_id;
    let call = if let TransformData::CallConcurrent(call) = &mut sess.transform_data[tf_id.get()] {
        call
    } else {
        unreachable!()
    };
    call.expanded = true;
    let target = call.target;
    let (target_tf, _end_tf) = sess.setup_transforms_from_op(
        ms_id,
        sess.job_data.session_data.chains[target as usize].operators[0],
        input_field,
    );
    sess.job_data.tf_mgr.transforms[target_tf].predecessor = Some(tf_id);
    sess.job_data.tf_mgr.transforms[tf_id].successor = Some(target_tf);
    let (batch_size, _input_done) = sess.job_data.tf_mgr.claim_all(tf_id);
    sess.job_data.unlink_transform(tf_id, batch_size);
}

pub fn handle_tf_call_concurrent(
    _sess: &mut JobData,
    _tf_id: TransformId,
    _tfc: &mut TfCallConcurrent,
) {
    todo!()
}
