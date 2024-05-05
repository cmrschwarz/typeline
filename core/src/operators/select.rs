use bstr::ByteSlice;

use crate::{
    job::JobData,
    liveness_analysis::{LivenessData, READS_OFFSET},
    options::argument::CliArgIdx,
    utils::string_store::{StringStore, StringStoreEntry},
};

use super::{
    errors::{OperatorCreationError, OperatorSetupError},
    operator::{OperatorBase, OperatorData, OperatorId},
    transform::{TransformData, TransformId, TransformState},
};

#[derive(Clone)]
pub struct OpSelect {
    key: String,
    pub key_interned: Option<StringStoreEntry>,
    pub field_is_read: bool,
}
pub struct TfSelect {}

pub fn parse_op_select(
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
            OperatorCreationError::new("key must be valid UTF-8", arg_idx)
        })?;
    Ok(OperatorData::Select(OpSelect {
        key: value_str.to_owned(),
        key_interned: None,
        field_is_read: true,
    }))
}

pub fn setup_op_select(
    op: &mut OpSelect,
    string_store: &mut StringStore,
) -> Result<(), OperatorSetupError> {
    op.key_interned =
        Some(string_store.intern_moved(std::mem::take(&mut op.key)));
    Ok(())
}

pub fn setup_op_select_liveness_data(
    op: &mut OpSelect,
    op_id: OperatorId,
    ld: &LivenessData,
) {
    op.field_is_read = ld.op_outputs_data
        [READS_OFFSET * ld.op_outputs.len() + op_id as usize];
}

pub fn create_op_select(key: String) -> OperatorData {
    OperatorData::Select(OpSelect {
        key,
        key_interned: None,
        field_is_read: true,
    })
}

pub fn build_tf_select(
    _jd: &mut JobData,
    _op_base: &OperatorBase,
    _op: &OpSelect,
    _tf_state: &mut TransformState,
) -> TransformData<'static> {
    TransformData::Select(TfSelect {})
}

pub fn handle_tf_select(
    jd: &mut JobData,
    tf_id: TransformId,
    _sel: &mut TfSelect,
) {
    let tf = &jd.tf_mgr.transforms[tf_id];
    jd.field_mgr
        .apply_field_actions(&jd.match_set_mgr, tf.input_field);
    let (batch_size, ps) = jd.tf_mgr.claim_all(tf_id);
    jd.tf_mgr.submit_batch(tf_id, batch_size, ps.input_done);
}
