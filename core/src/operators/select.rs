use crate::{
    chain::ChainId,
    cli::call_expr::{CallExpr, Span},
    options::session_setup::SessionSetupData,
    scr_error::ScrError,
    utils::string_store::StringStoreEntry,
};

use super::{
    errors::OperatorCreationError,
    operator::{
        OperatorData, OperatorDataId, OperatorId, OperatorOffsetInChain,
    },
};

#[derive(Clone)]
pub struct OpSelect {
    key: String,
    pub key_interned: Option<StringStoreEntry>,
}

pub fn parse_op_select(
    expr: &CallExpr,
) -> Result<OperatorData, OperatorCreationError> {
    let val = expr.require_single_string_arg()?;
    Ok(OperatorData::Select(OpSelect {
        key: val.to_owned(),
        key_interned: None,
    }))
}

pub fn setup_op_select(
    op: &mut OpSelect,
    sess: &mut SessionSetupData,
    op_data_id: OperatorDataId,
    chain_id: ChainId,
    offset_in_chain: OperatorOffsetInChain,
    span: Span,
) -> Result<OperatorId, ScrError> {
    op.key_interned =
        Some(sess.string_store.intern_moved(std::mem::take(&mut op.key)));
    Ok(sess.add_op(op_data_id, chain_id, offset_in_chain, span))
}

pub fn create_op_select(key: impl Into<String>) -> OperatorData {
    OperatorData::Select(OpSelect {
        key: key.into(),
        key_interned: None,
    })
}
