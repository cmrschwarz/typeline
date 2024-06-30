use crate::{
    chain::ChainId,
    cli::call_expr::CallExpr,
    context::SessionSetupData,
    job::JobData,
    liveness_analysis::{LivenessData, VarLivenessSlotKind},
    options::operator_base_options::OperatorBaseOptionsInterned,
    utils::{indexing_type::IndexingType, string_store::StringStoreEntry},
};

use super::{
    errors::{OperatorCreationError, OperatorSetupError},
    operator::{
        OperatorBase, OperatorData, OperatorDataId, OperatorId,
        OperatorOffsetInChain,
    },
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
    expr: &CallExpr,
) -> Result<OperatorData, OperatorCreationError> {
    let val = expr.require_single_string_arg()?;
    Ok(OperatorData::Select(OpSelect {
        key: val.to_owned(),
        key_interned: None,
        field_is_read: true,
    }))
}

pub fn setup_op_select(
    op: &mut OpSelect,
    sess: &mut SessionSetupData,
    chain_id: ChainId,
    offset_in_chain: OperatorOffsetInChain,
    op_base_opts_interned: OperatorBaseOptionsInterned,
    op_data_id: OperatorDataId,
) -> Result<OperatorId, OperatorSetupError> {
    op.key_interned =
        Some(sess.string_store.intern_moved(std::mem::take(&mut op.key)));
    Ok(sess.add_op_from_offset_in_chain(
        chain_id,
        offset_in_chain,
        op_base_opts_interned,
        op_data_id,
    ))
}

pub fn setup_op_select_liveness_data(
    op: &mut OpSelect,
    op_id: OperatorId,
    ld: &LivenessData,
) {
    op.field_is_read = ld.op_outputs_data.get_slot(VarLivenessSlotKind::Reads)
        [op_id.into_usize()];
}

pub fn create_op_select(key: String) -> OperatorData {
    OperatorData::Select(OpSelect {
        key,
        key_interned: None,
        field_is_read: true,
    })
}

pub fn build_tf_select<'a>(
    _jd: &mut JobData,
    _op_base: &OperatorBase,
    _op: &OpSelect,
    _tf_state: &mut TransformState,
) -> TransformData<'a> {
    TransformData::Select(TfSelect {})
}

pub fn handle_tf_select(
    jd: &mut JobData,
    tf_id: TransformId,
    _sel: &mut TfSelect,
) {
    let tf = &jd.tf_mgr.transforms[tf_id];
    jd.field_mgr
        .apply_field_actions(&jd.match_set_mgr, tf.input_field, true);
    let (batch_size, ps) = jd.tf_mgr.claim_all(tf_id);
    jd.tf_mgr.submit_batch(tf_id, batch_size, ps.input_done);
}
