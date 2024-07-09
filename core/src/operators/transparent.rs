use crate::{
    chain::ChainId,
    cli::{
        call_expr::{Argument, CallExpr, Span},
        parse_operator_data,
    },
    options::session_setup::SessionSetupData,
    scr_error::ScrError,
    utils::indexing_type::IndexingType,
};

use super::{
    errors::OperatorCreationError,
    key::NestedOp,
    operator::{
        OffsetInAggregation, OperatorData, OperatorDataId, OperatorId,
        OperatorOffsetInChain,
    },
};

pub struct OpTransparent {
    pub nested_op: NestedOp,
}

pub fn setup_op_transparent(
    op: &mut OpTransparent,
    sess: &mut SessionSetupData,
    op_data_id: OperatorDataId,
    chain_id: ChainId,
    offset_in_chain: OperatorOffsetInChain,
    span: Span,
) -> Result<OperatorId, ScrError> {
    let op_id = sess.add_op(op_data_id, chain_id, offset_in_chain, span);
    let NestedOp::Operator(op_span) = &mut op.nested_op else {
        panic!("operator was already set up");
    };
    let (op_data, span) = *std::mem::take(op_span);

    let sub_op_id = sess.add_op_data(op_data);
    let nested_op_id = sess.add_op(
        sub_op_id,
        sess.curr_chain,
        OperatorOffsetInChain::AggregationMember(
            op_id,
            OffsetInAggregation::ZERO,
        ),
        span,
    );
    op.nested_op = NestedOp::SetUp(nested_op_id);
    Ok(op_id)
}

pub fn create_op_transparent_with_span(
    op: OperatorData,
    span: Span,
) -> OperatorData {
    OperatorData::Transparent(OpTransparent {
        nested_op: NestedOp::Operator(Box::new((op, span))),
    })
}

pub fn create_op_transparent(op: OperatorData) -> OperatorData {
    create_op_transparent_with_span(op, Span::Generated)
}

pub fn parse_op_transparent(
    sess: &mut SessionSetupData,
    mut arg: Argument,
) -> Result<OperatorData, ScrError> {
    let expr = CallExpr::from_argument_mut(&mut arg)?;

    if expr.args.len() != 1 {
        return Err(OperatorCreationError::new(
            "operator `transparent` expects exactly one argument",
            expr.span,
        )
        .into());
    }

    let arg = &mut expr.args[0];
    let span = arg.span;
    let op = parse_operator_data(sess, std::mem::take(arg))?;
    Ok(create_op_transparent_with_span(op, span))
}
