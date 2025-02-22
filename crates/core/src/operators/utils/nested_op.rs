use crate::{
    cli::call_expr::Span,
    context::SessionData,
    liveness_analysis::{LivenessData, OpOutputIdx},
    operators::operator::{Operator, OperatorId},
};

pub enum NestedOp {
    Operator(Box<(Box<dyn Operator>, Span)>),
    SetUp(OperatorId),
}

pub fn setup_op_outputs_for_nested_op(
    nested_op: &NestedOp,
    sess: &mut SessionData,
    ld: &mut LivenessData,
    op_id: OperatorId,
    output_count: &mut OpOutputIdx,
) {
    let &NestedOp::SetUp(nested_op_id) = nested_op else {
        unreachable!()
    };
    sess.with_mut_op_data(nested_op_id, |sess, op| {
        op.assign_op_outputs(sess, ld, nested_op_id, output_count)
    });
    let (op_base, nested_op_base) =
        sess.operator_bases.two_distinct_mut(op_id, nested_op_id);
    op_base.outputs_start = nested_op_base.outputs_start;
    op_base.outputs_end = nested_op_base.outputs_end;
}
