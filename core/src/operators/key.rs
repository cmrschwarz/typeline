use crate::{
    chain::ChainId, cli::call_expr::CallExpr, context::SessionSetupData,
    options::operator_base_options::OperatorBaseOptionsInterned,
    utils::string_store::StringStoreEntry,
};

use super::{
    errors::{OperatorCreationError, OperatorSetupError},
    operator::{
        OperatorData, OperatorDataId, OperatorId, OperatorOffsetInChain,
    },
};

#[derive(Clone)]
pub struct OpKey {
    key: String,
    pub key_interned: Option<StringStoreEntry>,
}

pub fn parse_op_key(
    expr: &CallExpr,
) -> Result<OperatorData, OperatorCreationError> {
    let key = expr.require_single_string_arg()?;
    Ok(OperatorData::Key(OpKey {
        key: key.to_owned(),
        key_interned: None,
    }))
}

pub fn setup_op_key(
    op: &mut OpKey,
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

pub fn create_op_key(key: String) -> OperatorData {
    OperatorData::Key(OpKey {
        key,
        key_interned: None,
    })
}
