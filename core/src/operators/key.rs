use crate::{
    chain::ChainId,
    cli::{
        call_expr::{Argument, CallExpr, Span},
        parse_operator_data,
    },
    options::session_setup::SessionSetupData,
    scr_error::ScrError,
    utils::{indexing_type::IndexingType, string_store::StringStoreEntry},
};

use super::{
    errors::OperatorCreationError,
    operator::{
        OffsetInAggregation, OperatorData, OperatorDataId, OperatorId,
        OperatorOffsetInChain,
    },
};

pub enum NestedOp {
    Operator(Box<(OperatorData, Span)>),
    SetUp(OperatorId),
}

pub struct OpKey {
    key: String,
    pub key_interned: Option<StringStoreEntry>,
    pub nested_op: Option<NestedOp>,
}

pub fn parse_op_key(
    sess: &mut SessionSetupData,
    mut arg: Argument,
) -> Result<OperatorData, ScrError> {
    let expr = CallExpr::from_argument_mut(&mut arg)?;
    let op_name = expr.op_name;

    if expr.args.len() < 2 {
        return Err(OperatorCreationError::new(
            "missing label argument for operator `key`",
            expr.span,
        )
        .into());
    }

    let key_span = expr.args[0].span;

    let key = std::mem::take(&mut expr.args[0].value)
        .into_maybe_text()
        .ok_or_else(|| expr.error_positional_arg_not_plaintext(key_span))?;

    let key = key.into_text().ok_or_else(|| {
        expr.error_arg_invalid_utf8(op_name.as_bytes(), key_span)
    })?;

    let mut nested_op = None;
    if let Some(arg) = expr.args.get_mut(1) {
        let span = arg.span;
        let op = parse_operator_data(sess, std::mem::take(arg))?;
        nested_op = Some(NestedOp::Operator(Box::new((op, span))));
    }

    if expr.args.len() > 2 {
        return Err(OperatorCreationError::new(
            "operator key only accepts two arguments`",
            expr.args[2].span,
        )
        .into());
    }

    Ok(OperatorData::Key(OpKey {
        key,
        key_interned: None,
        nested_op,
    }))
}

pub fn setup_op_key(
    op: &mut OpKey,
    sess: &mut SessionSetupData,
    op_data_id: OperatorDataId,
    chain_id: ChainId,
    offset_in_chain: OperatorOffsetInChain,
    span: Span,
) -> Result<OperatorId, ScrError> {
    op.key_interned =
        Some(sess.string_store.intern_moved(std::mem::take(&mut op.key)));
    let op_id = sess.add_op(op_data_id, chain_id, offset_in_chain, span);
    let Some(nested_op) = &mut op.nested_op else {
        return Ok(op_id);
    };
    let NestedOp::Operator(op_span) = nested_op else {
        panic!("operator was already set up");
    };
    let (sub_op, span) = *std::mem::take(op_span);
    let sub_op_id = sess.setup_op_from_data(
        sub_op,
        sess.curr_chain,
        OperatorOffsetInChain::AggregationMember(
            op_id,
            OffsetInAggregation::ZERO,
        ),
        span,
    )?;
    op.nested_op = Some(NestedOp::SetUp(sub_op_id));

    Ok(op_id)
}

pub fn create_op_key(key: String) -> OperatorData {
    OperatorData::Key(OpKey {
        key,
        key_interned: None,
        nested_op: None,
    })
}

pub fn create_op_key_with_op(key: String, op: OperatorData) -> OperatorData {
    OperatorData::Key(OpKey {
        key,
        key_interned: None,
        nested_op: Some(NestedOp::Operator(Box::new((op, Span::Generated)))),
    })
}
