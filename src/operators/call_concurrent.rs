use std::{collections::HashMap, sync::Arc};

use bstr::ByteSlice;

use crate::{
    chain::{ChainId, INVALID_CHAIN_ID},
    context::{ContextData, VentureDescription},
    field_data::record_buffer::{RecordBuffer, RecordBufferFieldId},
    job_session::{FieldId, JobData, JobSession},
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
    pub target_fields: Vec<(FieldId, RecordBufferFieldId)>,
    pub buffer: Arc<RecordBuffer>,
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
        buffer: Default::default(),
        target_fields: Default::default(),
    })
}

pub(crate) fn handle_call_concurrent_expansion(
    sess: &mut JobSession,
    tf_id: TransformId,
    ctx: Option<&ContextData>,
) -> Result<(), VentureDescription> {
    let _tf = &mut sess.job_data.tf_mgr.transforms[tf_id];
    let call = if let TransformData::CallConcurrent(call) = &mut sess.transform_data[tf_id.get()] {
        call
    } else {
        unreachable!()
    };
    call.expanded = true;
    let mut _buf = Arc::get_mut(&mut call.buffer).unwrap();
    //TODO: setup buffer with all required fields
    let venture_desc = VentureDescription {
        participans_needed: 2,
        starting_points: smallvec::smallvec![call.target],
        buffer: call.buffer.clone(),
    };
    let ctx = ctx.unwrap();
    let sess = ctx.sess_mgr.lock().unwrap();
    let threads_needed = sess
        .venture_queue
        .front()
        .map(|v| v.description.participans_needed)
        .unwrap_or(0)
        + 2;
    if threads_needed > sess.session.max_threads {
        return Err(venture_desc);
    }
    if threads_needed > sess.total_worker_threads {}
    return Ok(());
}

pub fn handle_tf_call_concurrent(
    _sess: &mut JobData,
    _tf_id: TransformId,
    _tfc: &mut TfCallConcurrent,
) {
    todo!()
}
