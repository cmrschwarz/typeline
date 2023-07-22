use bstr::ByteSlice;

use crate::{
    job_session::JobData,
    options::argument::CliArgIdx,
    utils::string_store::{StringStore, StringStoreEntry, INVALID_STRING_STORE_ENTRY},
};

use super::{
    errors::{OperatorCreationError, OperatorSetupError},
    operator::{OperatorBase, OperatorData},
    transform::{TransformData, TransformId, TransformState},
};

#[derive(Clone)]
pub struct OpSelect {
    key: String,
    pub key_interned: StringStoreEntry,
}
pub struct TfSelect {}

pub fn parse_op_select(
    value: Option<&[u8]>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    let value_str = value
        .ok_or_else(|| OperatorCreationError::new("missing argument with key for select", arg_idx))?
        .to_str()
        .map_err(|_| OperatorCreationError::new("key must be valid UTF-8", arg_idx))?;
    Ok(OperatorData::Select(OpSelect {
        key: value_str.to_owned(),
        key_interned: INVALID_STRING_STORE_ENTRY,
    }))
}

pub fn setup_op_select(
    string_store: &mut StringStore,
    op: &mut OpSelect,
) -> Result<(), OperatorSetupError> {
    op.key_interned = string_store.intern_moved(std::mem::replace(&mut op.key, Default::default()));
    Ok(())
}

pub fn create_op_select(key: String) -> OperatorData {
    OperatorData::Select(OpSelect {
        key: key,
        key_interned: INVALID_STRING_STORE_ENTRY,
    })
}

pub fn setup_tf_select(
    _sess: &mut JobData,
    _op_base: &OperatorBase,
    _op: &OpSelect,
    _tf_state: &mut TransformState,
) -> TransformData<'static> {
    TransformData::Select(TfSelect {})
}

pub fn handle_tf_select(sess: &mut JobData, tf_id: TransformId, _sel: &mut TfSelect) {
    //TODO: think about maybe handling errors from the input field here?
    let tf = &sess.tf_mgr.transforms[tf_id];
    sess.field_mgr
        .apply_field_actions(&mut sess.match_set_mgr, tf.input_field);
    let (batch_size, input_done) = sess.tf_mgr.claim_all(tf_id);
    if input_done {
        sess.unlink_transform(tf_id, batch_size);
    } else {
        sess.tf_mgr
            .inform_successor_batch_available(tf_id, batch_size);
    }
}
