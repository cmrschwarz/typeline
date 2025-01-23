use std::convert::Infallible;

use num::{BigInt, FromPrimitive};

use crate::{
    operators::{errors::OperatorApplicationError, operator::OperatorId},
    record_data::{
        field_data::FixedSizeFieldValueType,
        field_value::FieldValueKind,
        field_value_ref::{FieldValueBlock, FieldValueSlice},
        iter::{
            field_iterator::FieldIterOpts,
            field_value_slice_iter::FieldValueRangeIter,
            ref_iter::RefAwareTypedRange,
        },
        match_set::MatchSetManager,
        push_interface::PushInterface,
    },
};

use super::{
    ast::BinaryOpKind,
    executor::ExecutorInputIter,
    executor_inserter::ExecutorInserter,
    operations::{
        add::{
            BinaryOpAddBigIntI64, BinaryOpAddF64F64, BinaryOpAddF64I64,
            BinaryOpAddI64BigInt, BinaryOpAddI64F64, BinaryOpAddI64I64,
        },
        div::{
            BinaryOpDivF64F64, BinaryOpDivF64I64, BinaryOpDivI64BigInt,
            BinaryOpDivI64F64, BinaryOpDivI64I64,
        },
        eq::{BasicBinaryOpEq, BinaryOpEqF64F64, BinaryOpEqI64I64},
        ge::{BasicBinaryOpGe, BinaryOpGeF64F64, BinaryOpGeI64I64},
        gt::{BasicBinaryOpGt, BinaryOpGtF64F64, BinaryOpGtI64I64},
        le::{BasicBinaryOpLe, BinaryOpLeF64F64, BinaryOpLeI64I64},
        lt::{BasicBinaryOpLt, BinaryOpLtF64F64, BinaryOpLtI64I64},
        mul::{
            BinaryOpMulBigIntI64, BinaryOpMulF64F64, BinaryOpMulF64I64,
            BinaryOpMulI64BigInt, BinaryOpMulI64F64, BinaryOpMulI64I64,
        },
        ne::{BasicBinaryOpNe, BinaryOpNeF64F64, BinaryOpNeI64I64},
        pow::{
            BinaryOpPowBigIntI64, BinaryOpPowF64F64, BinaryOpPowF64I64,
            BinaryOpPowI64BigInt, BinaryOpPowI64F64, BinaryOpPowI64I64,
        },
        sub::{
            BinaryOpSubBigIntI64, BinaryOpSubF64F64, BinaryOpSubF64I64,
            BinaryOpSubI64BigInt, BinaryOpSubI64F64, BinaryOpSubI64I64,
        },
        BinaryOp, ErrorToOperatorApplicationError,
    },
};

fn insert_binary_op_type_error(
    op_id: OperatorId,
    op_kind: BinaryOpKind,
    lhs_kind: FieldValueKind,
    rhs_kind: FieldValueKind,
    count: usize,
    inserter: &mut ExecutorInserter,
) {
    inserter.push_error(
        OperatorApplicationError::new_s(
            format!("invalid operands for binary op: `{lhs_kind}` {op_kind} `{rhs_kind}`"),
            op_id,
        ),
        count,
        true,
        false,
    );
}

fn insert_binary_op_type_error_iter_rhs(
    op_id: OperatorId,
    msm: &MatchSetManager,
    kind: BinaryOpKind,
    lhs_kind: FieldValueKind,
    rhs_iter: &mut ExecutorInputIter,
    mut count: usize,
    inserter: &mut ExecutorInserter,
) {
    while count > 0 {
        let rhs_range = rhs_iter
            .typed_range_fwd(msm, count, FieldIterOpts::default())
            .unwrap();
        insert_binary_op_type_error(
            op_id,
            kind,
            lhs_kind,
            rhs_range.base.data.repr().kind(),
            rhs_range.base.field_count,
            inserter,
        );
        count -= rhs_range.base.field_count;
    }
}

fn execute_binary_op_erroring<Op: BinaryOp>(
    lhs_block: FieldValueBlock<Op::Lhs>,
    rhs_range: &RefAwareTypedRange,
    rhs_data: &[Op::Rhs],
    inserter: &mut ExecutorInserter,
    op_id: OperatorId,
) {
    let mut rhs_iter = FieldValueRangeIter::from_range(rhs_range, rhs_data);
    match lhs_block {
        FieldValueBlock::Plain(lhs_data) => {
            let mut lhs_block_offset = 0;
            while let Some(rhs_block) = rhs_iter.next_block() {
                match rhs_block {
                    FieldValueBlock::Plain(rhs_data) => {
                        let len = rhs_data.len();
                        let mut i = 0;
                        while i < len {
                            let res = inserter
                                .reserve_for_fixed_size::<Op::Output>(len - i);
                            let (len, err) = Op::calc_until_error(
                                &lhs_data[lhs_block_offset + i..],
                                &rhs_data[i..],
                                res,
                            );
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
                        lhs_block_offset += len;
                    }
                    FieldValueBlock::WithRunLength(rhs_val, rhs_rl) => {
                        let len = rhs_rl as usize;
                        let mut i = 0;
                        while i < len {
                            let res = inserter
                                .reserve_for_fixed_size::<Op::Output>(len - i);
                            let (len, err) =
                                Op::calc_until_error_rhs_immediate(
                                    &lhs_data[lhs_block_offset + i..],
                                    rhs_val,
                                    res,
                                );
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
                        lhs_block_offset += len;
                    }
                }
            }
        }
        FieldValueBlock::WithRunLength(lhs_val, _lhs_rl) => {
            while let Some(rhs_block) = rhs_iter.next_block() {
                match rhs_block {
                    FieldValueBlock::Plain(rhs_data) => {
                        let len = rhs_data.len();
                        let mut i = 0;
                        while i < len {
                            let res = inserter
                                .reserve_for_fixed_size::<Op::Output>(len - i);
                            let (len, err) =
                                Op::calc_until_error_lhs_immediate(
                                    lhs_val,
                                    &rhs_data[i..],
                                    res,
                                );
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
                    FieldValueBlock::WithRunLength(rhs_val, rhs_rl) => {
                        let len = rhs_rl as usize;
                        match Op::try_calc_single(lhs_val, rhs_val) {
                            Ok(res) => inserter
                                .push_fixed_size_type(res, len, true, false),
                            Err(e) => {
                                inserter.push_error(
                                    e.to_operator_application_error(op_id),
                                    len,
                                    true,
                                    true,
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}

fn execute_binary_op_bigint_fallback<
    Op: BinaryOp<Lhs = i64>,
    OpBigint: BinaryOp<Lhs = BigInt, Rhs = Op::Rhs, Error = Infallible>,
>(
    lhs_block: FieldValueBlock<Op::Lhs>,
    rhs_range: &RefAwareTypedRange,
    rhs_data: &[Op::Rhs],
    inserter: &mut ExecutorInserter,
) where
    Op::Rhs: FixedSizeFieldValueType,
{
    let mut rhs_iter = FieldValueRangeIter::from_range(rhs_range, rhs_data);
    match lhs_block {
        FieldValueBlock::Plain(lhs_data) => {
            let mut lhs_block_offset = 0;
            while let Some(rhs_block) = rhs_iter.next_block() {
                match rhs_block {
                    FieldValueBlock::Plain(rhs_data) => {
                        let len = rhs_data.len();
                        let mut i = 0;
                        while i < len {
                            let res = inserter
                                .reserve_for_fixed_size::<Op::Output>(len - i);
                            let (len, err) = Op::calc_until_error(
                                &lhs_data[lhs_block_offset + i..],
                                &rhs_data[i..],
                                res,
                            );
                            unsafe {
                                inserter.add_count(len);
                            }
                            i += len;
                            if err.is_some() {
                                let res = OpBigint::try_calc_single(
                                    &BigInt::from_i64(
                                        lhs_data[lhs_block_offset + i],
                                    )
                                    .unwrap(),
                                    &rhs_data[i],
                                )
                                .unwrap();
                                inserter
                                    .push_fixed_size_type(res, 1, true, false);
                                i += 1;
                            }
                        }
                        lhs_block_offset += len;
                    }
                    FieldValueBlock::WithRunLength(rhs_val, rhs_rl) => {
                        let len = rhs_rl as usize;
                        let mut i = 0;
                        while i < len {
                            let res = inserter
                                .reserve_for_fixed_size::<Op::Output>(len - i);
                            let (len, err) =
                                Op::calc_until_error_rhs_immediate(
                                    &lhs_data[lhs_block_offset + i..],
                                    rhs_val,
                                    res,
                                );
                            unsafe {
                                inserter.add_count(len);
                            }
                            i += len;
                            if err.is_some() {
                                let res = OpBigint::try_calc_single(
                                    &BigInt::from_i64(
                                        lhs_data[lhs_block_offset + i],
                                    )
                                    .unwrap(),
                                    rhs_val,
                                )
                                .unwrap();
                                inserter
                                    .push_fixed_size_type(res, 1, true, false);
                                i += 1;
                            }
                        }
                        lhs_block_offset += len;
                    }
                }
            }
        }
        FieldValueBlock::WithRunLength(&lhs_val, _lhs_rl) => {
            while let Some(rhs_block) = rhs_iter.next_block() {
                match rhs_block {
                    FieldValueBlock::Plain(rhs_data) => {
                        let len = rhs_data.len();
                        let mut i = 0;
                        while i < len {
                            let res = inserter
                                .reserve_for_fixed_size::<Op::Output>(len - i);
                            let (len, err) =
                                Op::calc_until_error_lhs_immediate(
                                    &lhs_val,
                                    &rhs_data[i..],
                                    res,
                                );
                            unsafe {
                                inserter.add_count(len);
                            }
                            i += len;

                            if err.is_some() {
                                let res = OpBigint::try_calc_single(
                                    &BigInt::from_i64(lhs_val).unwrap(),
                                    &rhs_data[i],
                                )
                                .unwrap();
                                inserter
                                    .push_fixed_size_type(res, 1, true, false);
                                i += 1;
                            }
                        }
                    }
                    FieldValueBlock::WithRunLength(rhs_val, rhs_rl) => {
                        let len = rhs_rl as usize;
                        match Op::try_calc_single(&lhs_val, rhs_val) {
                            Ok(res) => inserter
                                .push_fixed_size_type(res, len, true, false),
                            Err(_) => inserter.push_fixed_size_type(
                                OpBigint::try_calc_single(
                                    &BigInt::from_i64(lhs_val).unwrap(),
                                    rhs_val,
                                )
                                .unwrap(),
                                len,
                                true,
                                false,
                            ),
                        }
                    }
                }
            }
        }
    }
}

fn execute_binary_op_infallable<Op: BinaryOp<Error = Infallible>>(
    lhs_block: FieldValueBlock<Op::Lhs>,
    rhs_range: &RefAwareTypedRange,
    rhs_data: &[Op::Rhs],
    inserter: &mut ExecutorInserter,
) {
    let mut rhs_iter = FieldValueRangeIter::from_range(rhs_range, rhs_data);
    match lhs_block {
        FieldValueBlock::Plain(lhs_data) => {
            let mut lhs_block_offset = 0;
            while let Some(rhs_block) = rhs_iter.next_block() {
                match rhs_block {
                    FieldValueBlock::Plain(rhs_data) => {
                        let len = rhs_data.len();
                        let res =
                            inserter.reserve_for_fixed_size::<Op::Output>(len);
                        Op::calc_until_error(
                            &lhs_data[lhs_block_offset..],
                            rhs_data,
                            res,
                        );
                        unsafe {
                            inserter.add_count(len);
                        }
                        lhs_block_offset += len;
                    }
                    FieldValueBlock::WithRunLength(rhs_val, rhs_rl) => {
                        let len = rhs_rl as usize;
                        let res =
                            inserter.reserve_for_fixed_size::<Op::Output>(len);
                        Op::calc_until_error_rhs_immediate(
                            &lhs_data[lhs_block_offset..],
                            rhs_val,
                            res,
                        );
                        unsafe {
                            inserter.add_count(len);
                        }
                        lhs_block_offset += len;
                    }
                }
            }
        }
        FieldValueBlock::WithRunLength(lhs_val, _lhs_rl) => {
            while let Some(rhs_block) = rhs_iter.next_block() {
                match rhs_block {
                    FieldValueBlock::Plain(rhs_data) => {
                        let len = rhs_data.len();
                        let res =
                            inserter.reserve_for_fixed_size::<Op::Output>(len);
                        Op::calc_until_error_lhs_immediate(
                            lhs_val, rhs_data, res,
                        );
                        unsafe {
                            inserter.add_count(len);
                        }
                    }
                    FieldValueBlock::WithRunLength(rhs_val, rhs_rl) => {
                        let len = rhs_rl as usize;
                        inserter.push_fixed_size_type(
                            Op::try_calc_single(lhs_val, rhs_val).unwrap(),
                            len,
                            true,
                            false,
                        )
                    }
                }
            }
        }
    }
}

fn execute_binary_op_double_int(
    op_id: OperatorId,
    op_kind: BinaryOpKind,
    lhs_block: FieldValueBlock<i64>,
    rhs_range: &RefAwareTypedRange,
    rhs_data: &[i64],
    inserter: &mut ExecutorInserter,
) {
    match op_kind {
        BinaryOpKind::Equals => {
            execute_binary_op_infallable::<BinaryOpEqI64I64>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::NotEquals => {
            execute_binary_op_infallable::<BinaryOpNeI64I64>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::LessThan => {
            execute_binary_op_infallable::<BinaryOpLtI64I64>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::GreaterThan => {
            execute_binary_op_infallable::<BinaryOpGtI64I64>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::LessThanEquals => {
            execute_binary_op_infallable::<BinaryOpLeI64I64>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::GreaterThanEquals => {
            execute_binary_op_infallable::<BinaryOpGeI64I64>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::Add => {
            execute_binary_op_bigint_fallback::<
                BinaryOpAddI64I64,
                BinaryOpAddBigIntI64,
            >(lhs_block, rhs_range, rhs_data, inserter)
        }
        BinaryOpKind::Subtract => {
            execute_binary_op_bigint_fallback::<
                BinaryOpSubI64I64,
                BinaryOpSubBigIntI64,
            >(lhs_block, rhs_range, rhs_data, inserter)
        }
        BinaryOpKind::AddAssign => todo!(),
        BinaryOpKind::SubtractAssign => todo!(),
        BinaryOpKind::Multiply => {
            execute_binary_op_bigint_fallback::<
                BinaryOpMulI64I64,
                BinaryOpMulBigIntI64,
            >(lhs_block, rhs_range, rhs_data, inserter)
        }
        BinaryOpKind::PowerOf => {
            execute_binary_op_bigint_fallback::<
                BinaryOpPowI64I64,
                BinaryOpPowBigIntI64,
            >(lhs_block, rhs_range, rhs_data, inserter)
        }
        BinaryOpKind::MultiplyAssign => todo!(),
        BinaryOpKind::PowerOfAssign => todo!(),
        BinaryOpKind::Divide => {
            execute_binary_op_erroring::<BinaryOpDivI64I64>(
                lhs_block, rhs_range, rhs_data, inserter, op_id,
            )
        }
        BinaryOpKind::DivideAssign => todo!(),
        BinaryOpKind::Modulus => todo!(),
        BinaryOpKind::ModulusAssign => todo!(),
        BinaryOpKind::LShift => todo!(),
        BinaryOpKind::LShiftAssign => todo!(),
        BinaryOpKind::RShift => todo!(),
        BinaryOpKind::RShiftAssign => todo!(),
        BinaryOpKind::LogicalAnd => todo!(),
        BinaryOpKind::LogicalOr => todo!(),
        BinaryOpKind::LogicalXor => todo!(),
        BinaryOpKind::BitwiseAnd => todo!(),
        BinaryOpKind::BitwiseOr => todo!(),
        BinaryOpKind::BitwiseXor => todo!(),
        BinaryOpKind::BitwiseAndAssign => todo!(),
        BinaryOpKind::BitwiseOrAssign => todo!(),
        BinaryOpKind::BitwiseXorAssign => todo!(),
        BinaryOpKind::BitwiseNotAssign => todo!(),
        BinaryOpKind::LogicalAndAssign => todo!(),
        BinaryOpKind::LogicalOrAssign => todo!(),
        BinaryOpKind::LogicalXorAssign => todo!(),
        BinaryOpKind::Access => todo!(),
        BinaryOpKind::Assign => todo!(),
    }
}

fn execute_binary_op_int_bigint(
    op_id: OperatorId,
    op_kind: BinaryOpKind,
    lhs_block: FieldValueBlock<i64>,
    rhs_range: &RefAwareTypedRange,
    rhs_data: &[BigInt],
    inserter: &mut ExecutorInserter,
) {
    match op_kind {
        BinaryOpKind::Equals => {
            execute_binary_op_infallable::<BasicBinaryOpEq<i64, BigInt>>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::NotEquals => {
            execute_binary_op_infallable::<BasicBinaryOpNe<i64, BigInt>>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::LessThan => {
            execute_binary_op_infallable::<BasicBinaryOpLt<i64, BigInt>>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::GreaterThan => {
            execute_binary_op_infallable::<BasicBinaryOpGt<i64, BigInt>>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::LessThanEquals => {
            execute_binary_op_infallable::<BasicBinaryOpLe<i64, BigInt>>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::GreaterThanEquals => {
            execute_binary_op_infallable::<BasicBinaryOpGe<i64, BigInt>>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::Add => {
            execute_binary_op_infallable::<BinaryOpAddI64BigInt>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::Subtract => {
            execute_binary_op_infallable::<BinaryOpSubI64BigInt>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::AddAssign => todo!(),
        BinaryOpKind::SubtractAssign => todo!(),
        BinaryOpKind::Multiply => {
            execute_binary_op_infallable::<BinaryOpMulI64BigInt>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::PowerOf => {
            execute_binary_op_infallable::<BinaryOpPowI64BigInt>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::MultiplyAssign => todo!(),
        BinaryOpKind::PowerOfAssign => todo!(),
        BinaryOpKind::Divide => {
            execute_binary_op_erroring::<BinaryOpDivI64BigInt>(
                lhs_block, rhs_range, rhs_data, inserter, op_id,
            )
        }
        BinaryOpKind::DivideAssign => todo!(),
        BinaryOpKind::Modulus => todo!(),
        BinaryOpKind::ModulusAssign => todo!(),
        BinaryOpKind::LShift => todo!(),
        BinaryOpKind::LShiftAssign => todo!(),
        BinaryOpKind::RShift => todo!(),
        BinaryOpKind::RShiftAssign => todo!(),
        BinaryOpKind::LogicalAnd => todo!(),
        BinaryOpKind::LogicalOr => todo!(),
        BinaryOpKind::LogicalXor => todo!(),
        BinaryOpKind::BitwiseAnd => todo!(),
        BinaryOpKind::BitwiseOr => todo!(),
        BinaryOpKind::BitwiseXor => todo!(),
        BinaryOpKind::BitwiseAndAssign => todo!(),
        BinaryOpKind::BitwiseOrAssign => todo!(),
        BinaryOpKind::BitwiseXorAssign => todo!(),
        BinaryOpKind::BitwiseNotAssign => todo!(),
        BinaryOpKind::LogicalAndAssign => todo!(),
        BinaryOpKind::LogicalOrAssign => todo!(),
        BinaryOpKind::LogicalXorAssign => todo!(),
        BinaryOpKind::Access => todo!(),
        BinaryOpKind::Assign => todo!(),
    }
}

fn execute_binary_op_double_float(
    _op_id: OperatorId,
    op_kind: BinaryOpKind,
    lhs_block: FieldValueBlock<f64>,
    rhs_range: &RefAwareTypedRange,
    rhs_data: &[f64],
    inserter: &mut ExecutorInserter,
) {
    match op_kind {
        BinaryOpKind::Equals => {
            execute_binary_op_infallable::<BinaryOpEqF64F64>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::NotEquals => {
            execute_binary_op_infallable::<BinaryOpNeF64F64>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::LessThan => {
            execute_binary_op_infallable::<BinaryOpLtF64F64>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::GreaterThan => {
            execute_binary_op_infallable::<BinaryOpGtF64F64>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::LessThanEquals => {
            execute_binary_op_infallable::<BinaryOpLeF64F64>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::GreaterThanEquals => {
            execute_binary_op_infallable::<BinaryOpGeF64F64>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::Add => {
            execute_binary_op_infallable::<BinaryOpAddF64F64>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::Subtract => {
            execute_binary_op_infallable::<BinaryOpSubF64F64>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::Multiply => {
            execute_binary_op_infallable::<BinaryOpMulF64F64>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::PowerOf => {
            execute_binary_op_infallable::<BinaryOpPowF64F64>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::MultiplyAssign => todo!(),
        BinaryOpKind::PowerOfAssign => todo!(),
        BinaryOpKind::Divide => {
            execute_binary_op_infallable::<BinaryOpDivF64F64>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::DivideAssign => todo!(),
        BinaryOpKind::Modulus => todo!(),
        BinaryOpKind::ModulusAssign => todo!(),
        BinaryOpKind::LShift => todo!(),
        BinaryOpKind::LShiftAssign => todo!(),
        BinaryOpKind::RShift => todo!(),
        BinaryOpKind::RShiftAssign => todo!(),
        BinaryOpKind::LogicalAnd => todo!(),
        BinaryOpKind::LogicalOr => todo!(),
        BinaryOpKind::LogicalXor => todo!(),
        BinaryOpKind::BitwiseAnd => todo!(),
        BinaryOpKind::BitwiseOr => todo!(),
        BinaryOpKind::BitwiseXor => todo!(),
        BinaryOpKind::BitwiseAndAssign => todo!(),
        BinaryOpKind::BitwiseOrAssign => todo!(),
        BinaryOpKind::BitwiseXorAssign => todo!(),
        BinaryOpKind::BitwiseNotAssign => todo!(),
        BinaryOpKind::LogicalAndAssign => todo!(),
        BinaryOpKind::LogicalOrAssign => todo!(),
        BinaryOpKind::LogicalXorAssign => todo!(),
        BinaryOpKind::AddAssign => todo!(),
        BinaryOpKind::SubtractAssign => todo!(),
        BinaryOpKind::Access => todo!(),
        BinaryOpKind::Assign => todo!(),
    }
}

fn execute_binary_op_int_float(
    op_id: OperatorId,
    op_kind: BinaryOpKind,
    lhs_block: FieldValueBlock<i64>,
    rhs_range: &RefAwareTypedRange,
    rhs_data: &[f64],
    inserter: &mut ExecutorInserter,
) {
    match op_kind {
        BinaryOpKind::Equals => {
            execute_binary_op_infallable::<BasicBinaryOpEq<i64, f64>>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::NotEquals => {
            execute_binary_op_infallable::<BasicBinaryOpNe<i64, f64>>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::LessThan => {
            execute_binary_op_infallable::<BasicBinaryOpLt<i64, f64>>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::GreaterThan => {
            execute_binary_op_infallable::<BasicBinaryOpGt<i64, f64>>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::LessThanEquals => {
            execute_binary_op_infallable::<BasicBinaryOpLe<i64, f64>>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::GreaterThanEquals => {
            execute_binary_op_infallable::<BasicBinaryOpGe<i64, f64>>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::Add => {
            execute_binary_op_infallable::<BinaryOpAddI64F64>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::Subtract => {
            execute_binary_op_infallable::<BinaryOpSubI64F64>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::Multiply => {
            execute_binary_op_erroring::<BinaryOpMulI64F64>(
                lhs_block, rhs_range, rhs_data, inserter, op_id,
            )
        }
        BinaryOpKind::PowerOf => {
            execute_binary_op_erroring::<BinaryOpPowI64F64>(
                lhs_block, rhs_range, rhs_data, inserter, op_id,
            )
        }
        BinaryOpKind::MultiplyAssign => todo!(),
        BinaryOpKind::PowerOfAssign => todo!(),
        BinaryOpKind::Divide => {
            execute_binary_op_erroring::<BinaryOpDivI64F64>(
                lhs_block, rhs_range, rhs_data, inserter, op_id,
            )
        }
        BinaryOpKind::AddAssign => todo!(),
        BinaryOpKind::SubtractAssign => todo!(),
        BinaryOpKind::DivideAssign => todo!(),
        BinaryOpKind::Modulus => todo!(),
        BinaryOpKind::ModulusAssign => todo!(),
        BinaryOpKind::LShift => todo!(),
        BinaryOpKind::LShiftAssign => todo!(),
        BinaryOpKind::RShift => todo!(),
        BinaryOpKind::RShiftAssign => todo!(),
        BinaryOpKind::LogicalAnd => todo!(),
        BinaryOpKind::LogicalOr => todo!(),
        BinaryOpKind::LogicalXor => todo!(),
        BinaryOpKind::BitwiseAnd => todo!(),
        BinaryOpKind::BitwiseOr => todo!(),
        BinaryOpKind::BitwiseXor => todo!(),
        BinaryOpKind::BitwiseAndAssign => todo!(),
        BinaryOpKind::BitwiseOrAssign => todo!(),
        BinaryOpKind::BitwiseXorAssign => todo!(),
        BinaryOpKind::BitwiseNotAssign => todo!(),
        BinaryOpKind::LogicalAndAssign => todo!(),
        BinaryOpKind::LogicalOrAssign => todo!(),
        BinaryOpKind::LogicalXorAssign => todo!(),
        BinaryOpKind::Access => todo!(),
        BinaryOpKind::Assign => todo!(),
    }
}

fn execute_binary_op_float_int(
    op_id: OperatorId,
    op_kind: BinaryOpKind,
    lhs_block: FieldValueBlock<f64>,
    rhs_range: &RefAwareTypedRange,
    rhs_data: &[i64],
    inserter: &mut ExecutorInserter,
) {
    match op_kind {
        BinaryOpKind::Equals => {
            execute_binary_op_infallable::<BasicBinaryOpEq<f64, i64>>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::NotEquals => {
            execute_binary_op_infallable::<BasicBinaryOpNe<f64, i64>>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::LessThan => {
            execute_binary_op_infallable::<BasicBinaryOpLt<f64, i64>>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::GreaterThan => {
            execute_binary_op_infallable::<BasicBinaryOpGt<f64, i64>>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::LessThanEquals => {
            execute_binary_op_infallable::<BasicBinaryOpLe<f64, i64>>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::GreaterThanEquals => {
            execute_binary_op_infallable::<BasicBinaryOpGe<f64, i64>>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::Add => {
            execute_binary_op_infallable::<BinaryOpAddF64I64>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::Subtract => {
            execute_binary_op_infallable::<BinaryOpSubF64I64>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::Multiply => {
            execute_binary_op_erroring::<BinaryOpMulF64I64>(
                lhs_block, rhs_range, rhs_data, inserter, op_id,
            )
        }
        BinaryOpKind::PowerOf => {
            execute_binary_op_erroring::<BinaryOpPowF64I64>(
                lhs_block, rhs_range, rhs_data, inserter, op_id,
            )
        }
        BinaryOpKind::Divide => {
            execute_binary_op_erroring::<BinaryOpDivF64I64>(
                lhs_block, rhs_range, rhs_data, inserter, op_id,
            )
        }
        BinaryOpKind::MultiplyAssign => todo!(),
        BinaryOpKind::PowerOfAssign => todo!(),

        BinaryOpKind::AddAssign => todo!(),
        BinaryOpKind::SubtractAssign => todo!(),
        BinaryOpKind::DivideAssign => todo!(),
        BinaryOpKind::Modulus => todo!(),
        BinaryOpKind::ModulusAssign => todo!(),
        BinaryOpKind::LShift => todo!(),
        BinaryOpKind::LShiftAssign => todo!(),
        BinaryOpKind::RShift => todo!(),
        BinaryOpKind::RShiftAssign => todo!(),
        BinaryOpKind::LogicalAnd => todo!(),
        BinaryOpKind::LogicalOr => todo!(),
        BinaryOpKind::LogicalXor => todo!(),
        BinaryOpKind::BitwiseAnd => todo!(),
        BinaryOpKind::BitwiseOr => todo!(),
        BinaryOpKind::BitwiseXor => todo!(),
        BinaryOpKind::BitwiseAndAssign => todo!(),
        BinaryOpKind::BitwiseOrAssign => todo!(),
        BinaryOpKind::BitwiseXorAssign => todo!(),
        BinaryOpKind::BitwiseNotAssign => todo!(),
        BinaryOpKind::LogicalAndAssign => todo!(),
        BinaryOpKind::LogicalOrAssign => todo!(),
        BinaryOpKind::LogicalXorAssign => todo!(),
        BinaryOpKind::Access => todo!(),
        BinaryOpKind::Assign => todo!(),
    }
}

fn execute_binary_op_for_int_lhs(
    op_id: OperatorId,
    msm: &MatchSetManager,
    op_kind: BinaryOpKind,
    lhs_range: &RefAwareTypedRange,
    lhs_data: &[i64],
    rhs_iter: &mut ExecutorInputIter,
    inserter: &mut ExecutorInserter,
) {
    let mut lhs_iter = FieldValueRangeIter::from_range(lhs_range, lhs_data);
    while let Some(lhs_block) = lhs_iter.next_block() {
        let mut rem = lhs_block.len();
        while rem > 0 {
            let rhs_range = rhs_iter
                .typed_range_fwd(msm, rem, FieldIterOpts::default())
                .unwrap();
            rem -= rhs_range.base.field_count;

            match rhs_range.base.data {
                FieldValueSlice::Int(rhs_data) => {
                    execute_binary_op_double_int(
                        op_id, op_kind, lhs_block, &rhs_range, rhs_data,
                        inserter,
                    )
                }
                FieldValueSlice::Float(rhs_data) => {
                    execute_binary_op_int_float(
                        op_id, op_kind, lhs_block, &rhs_range, rhs_data,
                        inserter,
                    )
                }
                FieldValueSlice::BigInt(rhs_data) => {
                    execute_binary_op_int_bigint(
                        op_id, op_kind, lhs_block, &rhs_range, rhs_data,
                        inserter,
                    )
                }
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
                    insert_binary_op_type_error(
                        op_id,
                        op_kind,
                        lhs_range.base.data.repr().kind(),
                        rhs_range.base.data.repr().kind(),
                        rhs_range.base.field_count,
                        inserter,
                    )
                }
                FieldValueSlice::FieldReference(_)
                | FieldValueSlice::SlicedFieldReference(_) => unreachable!(),
            }
        }
    }
}

fn execute_binary_op_for_float_lhs(
    op_id: OperatorId,
    msm: &MatchSetManager,
    op_kind: BinaryOpKind,
    lhs_range: &RefAwareTypedRange,
    lhs_data: &[f64],
    rhs_iter: &mut ExecutorInputIter,
    inserter: &mut ExecutorInserter,
) {
    let mut lhs_iter = FieldValueRangeIter::from_range(lhs_range, lhs_data);
    while let Some(lhs_block) = lhs_iter.next_block() {
        let mut rem = lhs_block.len();
        while rem > 0 {
            let rhs_range = rhs_iter
                .typed_range_fwd(msm, rem, FieldIterOpts::default())
                .unwrap();
            rem -= rhs_range.base.field_count;

            match rhs_range.base.data {
                FieldValueSlice::Int(rhs_data) => execute_binary_op_float_int(
                    op_id, op_kind, lhs_block, &rhs_range, rhs_data, inserter,
                ),
                FieldValueSlice::BigInt(_) => todo!(),
                FieldValueSlice::Float(rhs_data) => {
                    execute_binary_op_double_float(
                        op_id, op_kind, lhs_block, &rhs_range, rhs_data,
                        inserter,
                    )
                }
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
                    insert_binary_op_type_error(
                        op_id,
                        op_kind,
                        lhs_range.base.data.repr().kind(),
                        rhs_range.base.data.repr().kind(),
                        rhs_range.base.field_count,
                        inserter,
                    )
                }
                FieldValueSlice::FieldReference(_)
                | FieldValueSlice::SlicedFieldReference(_) => unreachable!(),
            }
        }
    }
}

pub(super) fn execute_binary_op_on_lhs_range(
    op_id: OperatorId,
    msm: &MatchSetManager,
    op_kind: BinaryOpKind,
    lhs_range: &RefAwareTypedRange,
    rhs_iter: &mut ExecutorInputIter,
    inserter: &mut ExecutorInserter,
) {
    match lhs_range.base.data {
        FieldValueSlice::Int(lhs_data) => execute_binary_op_for_int_lhs(
            op_id, msm, op_kind, lhs_range, lhs_data, rhs_iter, inserter,
        ),
        FieldValueSlice::Float(lhs_data) => execute_binary_op_for_float_lhs(
            op_id, msm, op_kind, lhs_range, lhs_data, rhs_iter, inserter,
        ),
        FieldValueSlice::Bool(_) => todo!(),
        FieldValueSlice::BigInt(_)
        | FieldValueSlice::BigRational(_)
        | FieldValueSlice::TextInline(_)
        | FieldValueSlice::TextBuffer(_)
        | FieldValueSlice::BytesInline(_)
        | FieldValueSlice::BytesBuffer(_)
        | FieldValueSlice::Array(_)
        | FieldValueSlice::Object(_)
        | FieldValueSlice::Null(_)
        | FieldValueSlice::Undefined(_)
        | FieldValueSlice::Custom(_)
        | FieldValueSlice::Error(_)
        | FieldValueSlice::Argument(_)
        | FieldValueSlice::OpDecl(_)
        | FieldValueSlice::StreamValueId(_) => {
            insert_binary_op_type_error_iter_rhs(
                op_id,
                msm,
                op_kind,
                lhs_range.base.data.repr().kind(),
                rhs_iter,
                lhs_range.base.field_count,
                inserter,
            )
        }
        FieldValueSlice::FieldReference(_)
        | FieldValueSlice::SlicedFieldReference(_) => unreachable!(),
    }
}
