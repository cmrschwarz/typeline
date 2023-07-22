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
pub struct OpJump {
    pub lazy: bool,
    pub target_name: String,
    pub target_resolved: ChainId,
}
pub struct TfJump {
    pub target: ChainId,
}

pub fn parse_op_jump(
    value: Option<&[u8]>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    let value_str = value
        .ok_or_else(|| OperatorCreationError::new("missing argument with key for select", arg_idx))?
        .to_str()
        .map_err(|_| OperatorCreationError::new("jump label must be valid UTF-8", arg_idx))?;
    Ok(OperatorData::Jump(OpJump {
        lazy: true,
        target_name: value_str.to_owned(),
        target_resolved: INVALID_CHAIN_ID,
    }))
}

pub fn setup_op_jump(
    chain_labels: &HashMap<StringStoreEntry, ChainId, BuildIdentityHasher>,
    string_store: &mut StringStore,
    op: &mut OpJump,
    op_id: OperatorId,
) -> Result<(), OperatorSetupError> {
    if op.target_resolved != INVALID_CHAIN_ID {
        // this happens in case of jump targets caused by labels ending the chain
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

pub fn create_op_jump(name: String) -> OperatorData {
    OperatorData::Jump(OpJump {
        lazy: true,
        target_name: name,
        target_resolved: INVALID_CHAIN_ID,
    })
}

pub(crate) fn create_op_jump_eager(target: ChainId) -> OperatorData {
    OperatorData::Jump(OpJump {
        lazy: false,
        target_name: String::new(),
        target_resolved: target,
    })
}

pub fn setup_tf_jump(
    _sess: &mut JobData,
    _op_base: &OperatorBase,
    op: &OpJump,
    _tf_state: &mut TransformState,
) -> TransformData<'static> {
    TransformData::Jump(TfJump {
        target: op.target_resolved,
    })
}
pub(crate) fn handle_eager_jump_expansion<'a>(
    sess: &mut JobSession,
    op_id: OperatorId,
    ms_id: MatchSetId,
    input_field: FieldId,
) -> (TransformId, TransformId) {
    if let OperatorData::Jump(op) = &sess.job_data.session_data.operator_data[op_id as usize] {
        let chain = &sess.job_data.session_data.chains[op.target_resolved as usize];
        sess.setup_transforms_from_op(ms_id, chain.operators[0], input_field)
    } else {
        unreachable!();
    }
}

pub(crate) fn handle_lazy_jump_expansion<'a>(sess: &mut JobSession, tf_id: TransformId) {
    let tf = &mut sess.job_data.tf_mgr.transforms[tf_id];
    let op_id = tf.op_id.unwrap();
    let input_field = tf.input_field;
    let ms_id = tf.match_set_id;
    let (target_tf, _end_tf) = handle_eager_jump_expansion(sess, op_id, ms_id, input_field);
    sess.job_data.tf_mgr.transforms[tf_id].successor = Some(target_tf);
    sess.job_data.unlink_transform(tf_id, 0);
}
