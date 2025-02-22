use std::convert::Infallible;

use crate::{
    operators::{errors::OperatorApplicationError, operator::OperatorId},
    record_data::{
        field_value::FieldValueKind,
        field_value_ref::{FieldValueBlock, FieldValueSlice},
        iter::{
            field_value_slice_iter::FieldValueRangeIter,
            ref_iter::RefAwareTypedRange,
        },
        push_interface::PushInterface,
    },
};

use super::{
    ast::UnaryOpKind,
    executor_inserter::ExecutorInserter,
    operations::{
        bitwise_not::UnaryOpBitwiseNotI64,
        logical_not::UnaryOpLogicalNotI64,
        negate::{UnaryOpNegateF64, UnaryOpNegateI64},
        ErrorToOperatorApplicationError, UnaryOp,
    },
};

fn insert_unary_op_type_error(
    op_id: OperatorId,
    op_kind: UnaryOpKind,
    value_kind: FieldValueKind,
    count: usize,
    inserter: &mut ExecutorInserter,
) {
    inserter.push_error(
        OperatorApplicationError::new_s(
            format!("invalid operands for unary op: {op_kind} `{value_kind}`"),
            op_id,
        ),
        count,
        true,
        false,
    );
}

fn execute_unary_op_erroring<Op: UnaryOp>(
    range: &RefAwareTypedRange,
    data: &[Op::Value],
    inserter: &mut ExecutorInserter,
    op_id: OperatorId,
) {
    let mut iter = FieldValueRangeIter::from_range(range, data);
    while let Some(block) = iter.next_block() {
        match block {
            FieldValueBlock::Plain(block_data) => {
                let len = block_data.len();
                let mut i = 0;
                while i < len {
                    let res =
                        inserter.reserve_for_fixed_size::<Op::Output>(len - i);
                    let (len, err) =
                        Op::calc_until_error(&block_data[i..], res);
                    unsafe {
                        inserter.add_count(len);
                    }
                    i += len;
                    if let Some(e) = err {
                        inserter.push_error(
                            e.to_operator_application_error(op_id),
                            1,
                            true,
                            true,
                        );
                        i += 1;
                    }
                }
            }
            FieldValueBlock::WithRunLength(val, rl) => {
                match Op::try_calc_single(val) {
                    Ok(v) => inserter.push_fixed_size_type(
                        v,
                        rl as usize,
                        true,
                        false,
                    ),
                    Err(e) => {
                        inserter.push_error(
                            e.to_operator_application_error(op_id),
                            rl as usize,
                            true,
                            true,
                        );
                    }
                }
            }
        }
    }
}

fn execute_unary_op_infallible<Op: UnaryOp<Error = Infallible>>(
    range: &RefAwareTypedRange,
    data: &[Op::Value],
    inserter: &mut ExecutorInserter,
) {
    let mut iter = FieldValueRangeIter::from_range(range, data);
    while let Some(block) = iter.next_block() {
        match block {
            FieldValueBlock::Plain(block_data) => {
                let bd_len = block_data.len();
                let res =
                    inserter.reserve_for_fixed_size::<Op::Output>(bd_len);
                let (len, _) = Op::calc_until_error(block_data, res);
                debug_assert!(bd_len == len);
                unsafe {
                    inserter.add_count(len);
                }
            }
            FieldValueBlock::WithRunLength(val, rl) => {
                let v = Op::try_calc_single(val).unwrap();
                inserter.push_fixed_size_type(v, rl as usize, true, false)
            }
        }
    }
}

fn execute_unary_op_int(
    op_id: OperatorId,
    op_kind: UnaryOpKind,
    range: &RefAwareTypedRange,
    data: &[i64],
    inserter: &mut ExecutorInserter,
) {
    match op_kind {
        UnaryOpKind::UnaryMinus => {
            execute_unary_op_erroring::<UnaryOpNegateI64>(
                range, data, inserter, op_id,
            )
        }
        UnaryOpKind::UnaryPlus => {
            inserter.extend_from_ref_aware_range(range, true, false);
        }
        UnaryOpKind::BitwiseNot => execute_unary_op_erroring::<
            UnaryOpBitwiseNotI64,
        >(range, data, inserter, op_id),
        UnaryOpKind::LogicalNot => execute_unary_op_erroring::<
            UnaryOpLogicalNotI64,
        >(range, data, inserter, op_id),
    }
}

fn execute_unary_op_float(
    op_id: OperatorId,
    op_kind: UnaryOpKind,
    range: &RefAwareTypedRange,
    data: &[f64],
    inserter: &mut ExecutorInserter,
) {
    match op_kind {
        UnaryOpKind::UnaryMinus => execute_unary_op_infallible::<
            UnaryOpNegateF64,
        >(range, data, inserter),
        UnaryOpKind::UnaryPlus => {
            inserter.extend_from_ref_aware_range(range, true, false);
        }
        UnaryOpKind::BitwiseNot | UnaryOpKind::LogicalNot => {
            insert_unary_op_type_error(
                op_id,
                op_kind,
                range.base.data.repr().kind(),
                range.base.field_count,
                inserter,
            )
        }
    }
}

pub(super) fn execute_unary_op_on_range(
    op_id: OperatorId,
    op_kind: UnaryOpKind,
    range: &RefAwareTypedRange,
    inserter: &mut ExecutorInserter,
) {
    match range.base.data {
        FieldValueSlice::Int(data) => {
            execute_unary_op_int(op_id, op_kind, range, data, inserter)
        }
        FieldValueSlice::Float(data) => {
            execute_unary_op_float(op_id, op_kind, range, data, inserter)
        }
        FieldValueSlice::BigInt(_) => todo!(),
        FieldValueSlice::BigRational(_) => todo!(),
        FieldValueSlice::Null(_)
        | FieldValueSlice::Undefined(_)
        | FieldValueSlice::Bool(_)
        | FieldValueSlice::TextInline(_)
        | FieldValueSlice::TextBuffer(_)
        | FieldValueSlice::BytesInline(_)
        | FieldValueSlice::BytesBuffer(_)
        | FieldValueSlice::Object(_)
        | FieldValueSlice::Array(_)
        | FieldValueSlice::Custom(_)
        | FieldValueSlice::Error(_)
        | FieldValueSlice::Argument(_)
        | FieldValueSlice::OpDecl(_)
        | FieldValueSlice::StreamValueId(_) => {
            // PERF: we could consume more values from rhs here
            insert_unary_op_type_error(
                op_id,
                op_kind,
                range.base.data.repr().kind(),
                range.base.field_count,
                inserter,
            )
        }
        FieldValueSlice::FieldReference(_)
        | FieldValueSlice::SlicedFieldReference(_) => unreachable!(),
    }
}
