use std::sync::Arc;

use crate::{
    chain::ChainId,
    cli::call_expr::{CallExpr, Span},
    job::JobData,
    options::session_setup::SessionSetupData,
    record_data::{
        field_value::FieldValue,
        scope_manager::{Atom, ScopeId},
    },
    scr_error::ScrError,
    utils::string_store::StringStoreEntry,
};

use super::{
    errors::OperatorCreationError,
    operator::{
        OperatorData, OperatorDataId, OperatorId, OperatorOffsetInChain,
    },
};

pub struct OpAtom {
    pub key: String,
    pub key_interned: Option<StringStoreEntry>,
    pub value: FieldValue,
}

pub fn parse_op_atom(
    _sess: &mut SessionSetupData,
    expr: &mut CallExpr,
) -> Result<OperatorData, ScrError> {
    let op_name = expr.op_name;

    if expr.args.len() < 2 {
        return Err(OperatorCreationError::new(
            "missing label argument for operator `atom`",
            expr.span,
        )
        .into());
    }

    let key_span = expr.args[0].span;

    let key = std::mem::take(&mut expr.args[0].value)
        .into_maybe_text()
        .ok_or_else(|| expr.error_positional_arg_not_plaintext(key_span))?;

    let key = key
        .into_text()
        .ok_or_else(|| expr.error_arg_invalid_utf8(op_name, key_span))?;

    let Some(value_arg) = expr.args.get_mut(1) else {
        return Err(expr.error_require_exact_positional_count(2).into());
    };

    Ok(create_op_atom(
        key,
        FieldValue::Argument(Box::new(std::mem::take(value_arg))),
    ))
}

pub fn setup_op_atom(
    op: &mut OpAtom,
    sess: &mut SessionSetupData,
    op_data_id: OperatorDataId,
    chain_id: ChainId,
    offset_in_chain: OperatorOffsetInChain,
    span: Span,
) -> Result<OperatorId, ScrError> {
    let key_interned = sess.string_store.intern_cloned(&op.key);
    op.key_interned = Some(key_interned);
    let op_id = sess.add_op(op_data_id, chain_id, offset_in_chain, span);

    sess.scope_mgr.insert_atom(
        sess.chains[chain_id].scope_id,
        key_interned,
        Arc::new(Atom::new(op.value.clone())),
    );

    Ok(op_id)
}
pub fn assign_atom(atom: &OpAtom, jd: &mut JobData, scope: ScopeId) {
    jd.scope_mgr.insert_atom(
        scope,
        atom.key_interned.unwrap(),
        Arc::new(Atom::new(atom.value.clone())),
    );
}

pub fn create_op_atom(key: String, value: FieldValue) -> OperatorData {
    OperatorData::Atom(OpAtom {
        key,
        key_interned: None,
        value,
    })
}
