use bstr::ByteSlice;
use num::{BigInt, BigRational, FromPrimitive};

use super::{
    ast::{AccessIdx, BinaryOpKind, BuiltinFunction, ExternIdentId},
    compiler::{
        Compilation, Instruction, InstructionId, TargetRef, TempFieldIdRaw,
        ValueAccess,
    },
    executor_inserter::ExecutorInserter,
    operations::{
        add::{
            BinaryOpAddBigIntI64, BinaryOpAddF64F64, BinaryOpAddF64I64,
            BinaryOpAddI64F64, BinaryOpAddI64I64,
        },
        div::{
            BinaryOpDivF64F64, BinaryOpDivF64I64, BinaryOpDivI64F64,
            BinaryOpDivI64I64,
        },
        eq::{BasicBinaryOpEq, BinaryOpEqF64F64, BinaryOpEqI64I64},
        ge::{BasicBinaryOpGe, BinaryOpGeF64F64, BinaryOpGeI64I64},
        gt::{BasicBinaryOpGt, BinaryOpGtF64F64, BinaryOpGtI64I64},
        le::{BasicBinaryOpLe, BinaryOpLeF64F64, BinaryOpLeI64I64},
        lt::{BasicBinaryOpLt, BinaryOpLtF64F64, BinaryOpLtI64I64},
        mul::{
            BinaryOpMulBigIntI64, BinaryOpMulF64F64, BinaryOpMulF64I64,
            BinaryOpMulI64F64, BinaryOpMulI64I64,
        },
        ne::{BasicBinaryOpNe, BinaryOpNeF64F64, BinaryOpNeI64I64},
        pow::{
            BinaryOpPowBigIntI64, BinaryOpPowF64F64, BinaryOpPowF64I64,
            BinaryOpPowI64F64, BinaryOpPowI64I64,
        },
        sub::{
            BinaryOpSubBigIntI64, BinaryOpSubF64F64, BinaryOpSubF64I64,
            BinaryOpSubI64F64, BinaryOpSubI64I64,
        },
        BinaryOp, ErrorToOperatorApplicationError,
    },
    ExternField, ExternFieldIdx, ExternVarData, TempField,
};
use crate::{
    index_newtype,
    operators::{errors::OperatorApplicationError, operator::OperatorId},
    record_data::{
        array::ArrayBuilder,
        field::{FieldManager, FieldRefOffset},
        field_data::{FieldData, FixedSizeFieldValueType, RunLength},
        field_data_ref::DestructuredFieldDataRef,
        field_value::FieldValueKind,
        field_value_ref::{FieldValueBlock, FieldValueRef, FieldValueSlice},
        iter::{
            field_iter::FieldIter,
            field_iterator::{FieldIterOpts, FieldIterator},
            field_value_slice_iter::FieldValueRangeIter,
            ref_iter::{
                AutoDerefIter, RefAwareBytesBufferIter,
                RefAwareInlineBytesIter, RefAwareInlineTextIter,
                RefAwareTextBufferIter, RefAwareTypedRange,
            },
            single_value_iter::{AtomIter, FieldValueIter, SingleValueIter},
        },
        iter_hall::{IterHall, IterStateRaw},
        match_set::MatchSetManager,
        push_interface::PushInterface,
        varying_type_inserter::VaryingTypeInserter,
    },
    tyson::TysonParser,
    utils::{
        compare_i64_bigint::{
            convert_int_to_float, try_convert_bigint_to_i64,
        },
        debuggable_nonmax::DebuggableNonMaxUsize,
        index_slice::IndexSlice,
        stable_universe::StableUniverse,
        temp_vec::{TempVec, TransmutableContainer},
    },
};
use metamatch::metamatch;
use std::{
    cell::{Ref, RefCell, RefMut},
    convert::Infallible,
    ops::Range,
};

index_newtype! {
    pub struct ExternFieldTempIterId(u32);
    pub struct NextLowestArrayLink(DebuggableNonMaxUsize);
}

pub struct Executor<'a, 'b> {
    pub op_id: OperatorId,
    pub compilation: &'a Compilation,
    pub fm: &'a FieldManager,
    pub msm: &'a MatchSetManager,
    pub temp_fields: &'a mut IndexSlice<TempFieldIdRaw, TempField>,
    pub output: &'a mut IterHall,
    pub extern_vars: &'a mut IndexSlice<ExternIdentId, ExternVarData>,
    pub extern_fields: &'a mut IndexSlice<ExternFieldIdx, ExternField>,
    pub extern_field_iters: &'a mut IndexSlice<
        ExternFieldIdx,
        AutoDerefIter<'b, FieldIter<DestructuredFieldDataRef<'b>>>,
    >,
    pub extern_field_temp_iters: &'a mut StableUniverse<
        ExternFieldTempIterId,
        RefCell<AutoDerefIter<'b, FieldIter<DestructuredFieldDataRef<'b>>>>,
    >,
    pub executor_iters_temp:
        &'a mut TempVec<ExecutorInputIter<'static, 'static>>,
    pub array_builder: &'a mut ArrayBuilder,
}

#[allow(clippy::large_enum_variant)]
pub enum ExecutorInputIter<'a, 'b> {
    AutoDerefIter(
        RefMut<'a, AutoDerefIter<'b, FieldIter<DestructuredFieldDataRef<'b>>>>,
    ),
    FieldIter(FieldIter<Ref<'a, FieldData>>),
    Atom(AtomIter<'a>),
    FieldValue(FieldValueIter<'a>),
}

impl<'a, 'b> ExecutorInputIter<'a, 'b> {
    pub fn typed_range_fwd(
        &mut self,
        msm: &MatchSetManager,
        limit: usize,
        opts: FieldIterOpts,
    ) -> Option<RefAwareTypedRange> {
        match self {
            ExecutorInputIter::AutoDerefIter(iter) => {
                iter.typed_range_fwd(msm, limit, opts)
            }
            ExecutorInputIter::FieldIter(iter) => {
                Some(RefAwareTypedRange::without_refs(
                    iter.typed_range_fwd(limit, opts)?,
                ))
            }
            ExecutorInputIter::Atom(iter) => iter.typed_range_fwd(limit),
            ExecutorInputIter::FieldValue(iter) => iter.typed_range_fwd(limit),
        }
    }

    fn next_field(
        &mut self,
        msm: &MatchSetManager,
        limit: usize,
    ) -> Option<(FieldValueRef<'_>, RunLength, Option<FieldRefOffset>)> {
        match self {
            ExecutorInputIter::AutoDerefIter(ref_mut) => {
                ref_mut.next_value(msm, limit)
            }
            ExecutorInputIter::FieldIter(field_iter) => {
                let tf = field_iter.typed_field_fwd(limit)?;
                if tf.header.shared_value() {
                    Some((tf.value, tf.header.run_length, None))
                } else {
                    Some((tf.value, 1, None))
                }
            }
            ExecutorInputIter::Atom(value) => {
                let (v, rl) = value.next_field(limit)?;
                Some((v, rl, None))
            }
            ExecutorInputIter::FieldValue(single_value_iter) => {
                let (v, rl) = single_value_iter.next_field(limit)?;
                Some((v, rl, None))
            }
        }
    }
}

fn get_inserter<'a: 'b, 'b>(
    output: &'b mut IterHall,
    temp_fields: &'a IndexSlice<TempFieldIdRaw, TempField>,
    idx: Option<TempFieldIdRaw>,
    field_pos: usize,
) -> ExecutorInserter<'b> {
    match idx {
        Some(tmp_id) => {
            let tmp = &temp_fields[tmp_id];
            let mut inserter = VaryingTypeInserter::new(tmp.data.borrow_mut());
            if tmp.field_pos.get() == usize::MAX {
                tmp.field_pos.set(field_pos);
            } else {
                debug_assert!(tmp.field_pos.get() <= field_pos);
                inserter.push_undefined(field_pos - tmp.field_pos.get(), true);
            }
            ExecutorInserter::TempField(inserter)
        }
        None => ExecutorInserter::Output(output.varying_type_inserter()),
    }
}

fn get_extern_iter<'a, 'b>(
    extern_fields: &mut IndexSlice<ExternFieldIdx, ExternField>,
    extern_field_iters: &IndexSlice<
        ExternFieldIdx,
        AutoDerefIter<'b, FieldIter<DestructuredFieldDataRef<'b>>>,
    >,
    extern_field_temp_iters: &'a StableUniverse<
        ExternFieldTempIterId,
        RefCell<AutoDerefIter<'b, FieldIter<DestructuredFieldDataRef<'b>>>>,
    >,
    extern_field_idx: ExternFieldIdx,
    access_idx: AccessIdx,
) -> RefMut<'a, AutoDerefIter<'b, FieldIter<DestructuredFieldDataRef<'b>>>> {
    let ef = &mut extern_fields[extern_field_idx];
    if let Some(iter_slot_idx) = ef.iter_slots[access_idx] {
        return extern_field_temp_iters[iter_slot_idx].borrow_mut();
    }
    let iter_slot_idx = extern_field_temp_iters.claim_with_value(
        RefCell::new(extern_field_iters[extern_field_idx].clone()),
    );
    ef.iter_slots[access_idx] = Some(iter_slot_idx);
    extern_field_temp_iters[iter_slot_idx].borrow_mut()
}

fn get_executor_input_iter<'a, 'b>(
    input: &'a ValueAccess,
    temp_fields: &'a IndexSlice<TempFieldIdRaw, TempField>,
    extern_vars: &'a IndexSlice<ExternIdentId, ExternVarData>,
    extern_fields: &mut IndexSlice<ExternFieldIdx, ExternField>,
    extern_field_iters: &IndexSlice<
        ExternFieldIdx,
        AutoDerefIter<'b, FieldIter<DestructuredFieldDataRef<'b>>>,
    >,
    extern_field_temp_iter_handouts: &'a StableUniverse<
        ExternFieldTempIterId,
        RefCell<AutoDerefIter<'b, FieldIter<DestructuredFieldDataRef<'b>>>>,
    >,
    field_pos: usize,
    count: usize,
) -> ExecutorInputIter<'a, 'b> {
    match input {
        ValueAccess::Extern(acc) => match &extern_vars[acc.index] {
            ExternVarData::Atom(atom) => ExecutorInputIter::Atom(
                SingleValueIter::from_atom(atom, count),
            ),
            ExternVarData::Literal(v) => {
                ExecutorInputIter::FieldValue(SingleValueIter::new(v, count))
            }
            ExternVarData::Field(extern_field_idx) => {
                let mut iter = get_extern_iter(
                    extern_fields,
                    extern_field_iters,
                    extern_field_temp_iter_handouts,
                    *extern_field_idx,
                    acc.access_idx,
                );
                iter.move_to_field_pos(field_pos);
                ExecutorInputIter::AutoDerefIter(iter)
            }
        },
        ValueAccess::TempField(tmp_in) => {
            let temp_field = temp_fields[tmp_in.index].data.borrow();
            let iter = FieldIter::from_start(temp_field);
            ExecutorInputIter::FieldIter(iter)
        }
        ValueAccess::Literal(v) => {
            ExecutorInputIter::FieldValue(SingleValueIter::new(v, count))
        }
    }
}

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

#[allow(clippy::needless_pass_by_value)]
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

#[allow(clippy::needless_pass_by_value)]
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

#[allow(clippy::needless_pass_by_value)]
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

fn execute_binary_op(
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

fn execute_builtin_func_on_range(
    op_id: OperatorId,
    kind: BuiltinFunction,
    arg_range: RefAwareTypedRange<'_>,
    inserter: &mut ExecutorInserter,
) {
    match kind {
        BuiltinFunction::Cast(FieldValueKind::Int) => {
            execute_cast_int(op_id, arg_range, inserter)
        }
        BuiltinFunction::Cast(FieldValueKind::Float) => {
            execute_cast_float(op_id, arg_range, inserter)
        }
        _ => todo!(),
    }
}

fn execute_cast_int(
    op_id: OperatorId,
    range: RefAwareTypedRange<'_>,
    inserter: &mut ExecutorInserter,
) {
    metamatch!(match range.base.data {
        #[expand((REPR, ITER, VAL_BYTES, VAL_ERR) in [
            (TextInline, RefAwareInlineTextIter, v.as_bytes(), v),
            (TextBuffer, RefAwareTextBufferIter, v.as_bytes(), v),
            (BytesInline, RefAwareInlineBytesIter, v, v.to_str_lossy()),
            (BytesBuffer, RefAwareBytesBufferIter, v, v.to_str_lossy())
        ])]
        FieldValueSlice::REPR(data) => {
            for (v, rl, _) in ITER::from_range(&range, data) {
                let rl = rl as usize;
                match lexical_core::parse::<i64>(VAL_BYTES) {
                    Ok(v) => {
                        inserter.push_int(v, rl, true, false);
                    }
                    Err(e) => {
                        if matches!(
                            e,
                            lexical_core::Error::Overflow(_)
                                | lexical_core::Error::Underflow(_)
                        ) {
                            if let Some(v) = BigInt::parse_bytes(VAL_BYTES, 10)
                            {
                                inserter.push_big_int(v, rl, true, false);
                                continue;
                            }
                        }
                        inserter.push_error(
                            OperatorApplicationError::new_s(
                                format!(
                                    "failed to parse '{}' as int",
                                    VAL_ERR,
                                ),
                                op_id,
                            ),
                            rl,
                            true,
                            false,
                        );
                    }
                }
            }
        }
        FieldValueSlice::Float(values) => {
            for (&v, rl) in FieldValueRangeIter::from_range(&range, values) {
                if v.is_nan() || v.is_infinite() {
                    inserter.push_error(
                        OperatorApplicationError::new_s(
                            format!("cannot cast '{}' to int", v),
                            op_id,
                        ),
                        rl as usize,
                        true,
                        false,
                    );
                    continue;
                }
                #[allow(clippy::cast_precision_loss)]
                {
                    // TODO: verify that this is not off by one
                    if v < (i64::MAX as f64) && v > (i64::MIN as f64) {
                        inserter.push_int(v as i64, rl as usize, true, false);
                    }
                }
                inserter.push_big_int(
                    BigInt::from_f64(v).unwrap(),
                    rl as usize,
                    true,
                    false,
                );
            }
        }
        FieldValueSlice::BigRational(values) => {
            for (v, rl) in FieldValueRangeIter::from_range(&range, values) {
                let v = v.to_integer();
                if let Some(v) = try_convert_bigint_to_i64(&v) {
                    inserter.push_int(v, rl as usize, true, false);
                } else {
                    inserter.push_big_int(v, rl as usize, true, false);
                }
            }
        }
        FieldValueSlice::Bool(values) => {
            for (v, rl) in FieldValueRangeIter::from_range(&range, values) {
                inserter.push_int(i64::from(*v), rl as usize, true, false);
            }
        }
        FieldValueSlice::Int(_)
        | FieldValueSlice::BigInt(_)
        | FieldValueSlice::Error(_) => {
            inserter.extend_from_ref_aware_range(range, true, false)
        }
        FieldValueSlice::Null(_)
        | FieldValueSlice::Undefined(_)
        | FieldValueSlice::Object(_)
        | FieldValueSlice::Array(_)
        | FieldValueSlice::Custom(_)
        | FieldValueSlice::Argument(_)
        | FieldValueSlice::OpDecl(_)
        | FieldValueSlice::StreamValueId(_)
        | FieldValueSlice::FieldReference(_)
        | FieldValueSlice::SlicedFieldReference(_) => {
            inserter.push_error(
                OperatorApplicationError::new_s(
                    format!("cannot cast '{}' to int", range.base.data.kind()),
                    op_id,
                ),
                range.base.field_count,
                true,
                false,
            );
        }
    })
}

fn execute_cast_float(
    op_id: OperatorId,
    range: RefAwareTypedRange<'_>,
    inserter: &mut ExecutorInserter,
) {
    metamatch!(match range.base.data {
        #[expand((REPR, ITER, VAL_BYTES, VAL_ERR) in [
            (TextInline, RefAwareInlineTextIter, v.as_bytes(), v),
            (TextBuffer, RefAwareTextBufferIter, v.as_bytes(), v),
            (BytesInline, RefAwareInlineBytesIter, v, v.to_str_lossy()),
            (BytesBuffer, RefAwareBytesBufferIter, v, v.to_str_lossy())
        ])]
        FieldValueSlice::REPR(data) => {
            for (v, rl, _) in ITER::from_range(&range, data) {
                let rl = rl as usize;
                match lexical_core::parse::<f64>(VAL_BYTES) {
                    Ok(v) => {
                        inserter.push_float(v, rl, true, false);
                    }
                    Err(e) => {
                        if matches!(
                            e,
                            lexical_core::Error::Overflow(_)
                                | lexical_core::Error::Underflow(_)
                        ) {
                            let mut p =
                                TysonParser::new(&VAL_BYTES[1..], false, None);

                            if let Ok(v) = p.parse_number(VAL_BYTES[0]) {
                                inserter.push_field_value_unboxed(
                                    v, rl, true, false,
                                );
                                continue;
                            }
                        }
                        inserter.push_error(
                            OperatorApplicationError::new_s(
                                format!(
                                    "failed to parse '{}' as int",
                                    VAL_ERR,
                                ),
                                op_id,
                            ),
                            rl,
                            true,
                            false,
                        );
                    }
                }
            }
        }
        FieldValueSlice::Float(_) | FieldValueSlice::BigRational(_) => {
            inserter.extend_from_ref_aware_range(range, true, false);
        }
        FieldValueSlice::Int(values) => {
            for (v, rl) in FieldValueRangeIter::from_range(&range, values) {
                match convert_int_to_float(*v) {
                    Ok(v) => inserter.push_float(v, rl as usize, true, false),
                    Err(v) => {
                        inserter.push_big_rational(v, rl as usize, true, false)
                    }
                }
            }
        }
        FieldValueSlice::BigInt(values) => {
            for (v, rl) in FieldValueRangeIter::from_range(&range, values) {
                if let Some(v) = try_convert_bigint_to_i64(v) {
                    if let Ok(v) = convert_int_to_float(v) {
                        inserter.push_float(v, rl as usize, true, false);
                        continue;
                    }
                }
                inserter.push_big_rational(
                    BigRational::from_integer(v.clone()),
                    rl as usize,
                    true,
                    false,
                );
            }
        }
        FieldValueSlice::Bool(values) => {
            for (v, rl) in FieldValueRangeIter::from_range(&range, values) {
                inserter.push_float(
                    f64::from(i8::from(*v)),
                    rl as usize,
                    true,
                    false,
                );
            }
        }
        FieldValueSlice::Error(_) => {
            inserter.extend_from_ref_aware_range(range, true, false)
        }
        FieldValueSlice::Null(_)
        | FieldValueSlice::Undefined(_)
        | FieldValueSlice::Object(_)
        | FieldValueSlice::Array(_)
        | FieldValueSlice::Custom(_)
        | FieldValueSlice::Argument(_)
        | FieldValueSlice::OpDecl(_)
        | FieldValueSlice::StreamValueId(_)
        | FieldValueSlice::FieldReference(_)
        | FieldValueSlice::SlicedFieldReference(_) => {
            inserter.push_error(
                OperatorApplicationError::new_s(
                    format!(
                        "cannot cast '{}' to int",
                        range.base.data.kind().to_str()
                    ),
                    op_id,
                ),
                range.base.field_count,
                true,
                false,
            );
        }
    })
}

impl<'a, 'b> Executor<'a, 'b> {
    fn execute_op_binary(
        &mut self,
        kind: BinaryOpKind,
        lhs: &ValueAccess,
        rhs: &ValueAccess,
        tgt: TargetRef,
        field_pos: usize,
        count: usize,
    ) {
        let output_tmp_id = match tgt {
            TargetRef::TempField(id) => Some(id),
            TargetRef::Output => None,
            TargetRef::Discard => return,
        };
        let mut inserter = get_inserter(
            self.output,
            &*self.temp_fields,
            output_tmp_id,
            field_pos,
        );
        let mut lhs_iter = get_executor_input_iter(
            lhs,
            self.temp_fields,
            self.extern_vars,
            self.extern_fields,
            self.extern_field_iters,
            self.extern_field_temp_iters,
            field_pos,
            count,
        );
        let mut rhs_iter = get_executor_input_iter(
            rhs,
            self.temp_fields,
            self.extern_vars,
            self.extern_fields,
            self.extern_field_iters,
            self.extern_field_temp_iters,
            field_pos,
            count,
        );
        let mut count_rem = count;

        while count_rem > 0 {
            let lhs_range = lhs_iter
                .typed_range_fwd(self.msm, count_rem, FieldIterOpts::default())
                .unwrap();

            count_rem -= lhs_range.base.field_count;

            execute_binary_op(
                self.op_id,
                self.msm,
                kind,
                &lhs_range,
                &mut rhs_iter,
                &mut inserter,
            );
            if count_rem == 0 {
                break;
            }
        }
    }

    fn execute_builtin_fn(
        &mut self,
        kind: BuiltinFunction,
        args: &[ValueAccess],
        tgt: TargetRef,
        field_pos: usize,
        count: usize,
    ) {
        let output_tmp_id = match tgt {
            TargetRef::TempField(id) => Some(id),
            TargetRef::Output => None,
            TargetRef::Discard => {
                return;
            }
        };

        let mut inserter = get_inserter(
            self.output,
            self.temp_fields,
            output_tmp_id,
            field_pos,
        );

        if args.len() != 1 {
            // TODO: once we have builtin fns with more than one
            // arg make this more sophisticated
            inserter.push_error(
                OperatorApplicationError::new_s(
                    format!(
                        "invalid argument count for builtin op: '{}' expects 1 argument, got {}",
                        kind.to_str(),
                        args.len()
                    ),
                    self.op_id
                ),
                count,
                true,
                false,
            );
            return;
        }

        let mut arg_iter = get_executor_input_iter(
            &args[0],
            self.temp_fields,
            self.extern_vars,
            self.extern_fields,
            self.extern_field_iters,
            self.extern_field_temp_iters,
            field_pos,
            count,
        );
        let mut count_rem = count;

        while count_rem > 0 {
            let arg_range = arg_iter
                .typed_range_fwd(self.msm, count_rem, FieldIterOpts::default())
                .unwrap();

            count_rem -= arg_range.base.field_count;

            execute_builtin_func_on_range(
                self.op_id,
                kind,
                arg_range,
                &mut inserter,
            );
            if count_rem == 0 {
                break;
            }
        }
    }

    fn execute_array(
        &mut self,
        elements: &[ValueAccess],
        tgt: TargetRef,
        field_pos: usize,
        count: usize,
    ) {
        debug_assert!(count > 0);
        let output_tmp_id = match tgt {
            TargetRef::TempField(id) => Some(id),
            TargetRef::Output => None,
            TargetRef::Discard => return,
        };

        let mut inserter = get_inserter(
            self.output,
            self.temp_fields,
            output_tmp_id,
            field_pos,
        );

        let input_iters = &mut *self.executor_iters_temp.borrow_container();

        debug_assert!(self.array_builder.is_empty());

        for e in elements {
            let mut iter = get_executor_input_iter(
                e,
                self.temp_fields,
                self.extern_vars,
                self.extern_fields,
                self.extern_field_iters,
                self.extern_field_temp_iters,
                field_pos,
                count,
            );
            let (v, rl, _) = iter.next_field(self.msm, count).unwrap();
            self.array_builder.push(v, rl as usize);
            input_iters.push(iter);
        }
        let mut rl_rem = count;
        loop {
            let len = self.array_builder.available_len();
            self.array_builder.consume_len(len);
            rl_rem -= len;
            let arr = if rl_rem == 0 {
                self.array_builder.take()
            } else {
                self.array_builder.build()
            };
            inserter.push_array(arr, len, true, false);
            if rl_rem == 0 {
                break;
            }
            while let Some(idx) = self.array_builder.drained_idx() {
                let (v, rl, _) =
                    input_iters[idx].next_field(self.msm, len).unwrap();
                self.array_builder.replenish(v, rl as usize);
            }
        }
    }

    fn execute_move(
        &mut self,
        src: &ValueAccess,
        tgt: TargetRef,
        field_pos: usize,
        count: usize,
    ) {
        let output_tmp_id = match tgt {
            TargetRef::TempField(id) => Some(id),
            TargetRef::Output => None,
            TargetRef::Discard => return,
        };

        let mut inserter = get_inserter(
            self.output,
            self.temp_fields,
            output_tmp_id,
            field_pos,
        );

        match src {
            ValueAccess::Extern(acc) => {
                match &mut self.extern_vars[acc.index] {
                    ExternVarData::Atom(atom) => inserter
                        .push_field_value_ref(
                            &atom.value.read().unwrap(),
                            count,
                            true,
                            false,
                        ),
                    ExternVarData::Literal(v) => {
                        inserter.push_field_value_ref(v, count, true, false)
                    }
                    ExternVarData::Field(extern_field_idx) => {
                        let mut iter = get_extern_iter(
                            self.extern_fields,
                            self.extern_field_iters,
                            self.extern_field_temp_iters,
                            *extern_field_idx,
                            acc.access_idx,
                        );
                        iter.move_to_field_pos(field_pos);
                        inserter.extend_from_auto_deref_iter(
                            self.msm, &mut iter, count, true, false,
                        );
                    }
                }
            }
            ValueAccess::TempField(tmp_in) => {
                inserter.extend_from_iter(
                    &mut self.temp_fields[tmp_in.index].data.borrow().iter(),
                    count,
                    true,
                    false,
                );
            }
            ValueAccess::Literal(v) => {
                inserter.push_field_value_ref(v, count, true, false)
            }
        }
    }

    fn handle_batch(
        &mut self,
        insn_range: Range<InstructionId>,
        field_pos: usize,
        count: usize,
    ) {
        for insn in &self.compilation.instructions[insn_range] {
            match insn {
                Instruction::OpUnary {
                    kind: _,
                    value: _,
                    target: _,
                } => todo!(),
                Instruction::OpBinary {
                    kind,
                    lhs,
                    rhs,
                    target,
                } => {
                    self.execute_op_binary(
                        *kind, lhs, rhs, *target, field_pos, count,
                    );
                }
                Instruction::BuiltinFunction { kind, args, target } => {
                    self.execute_builtin_fn(
                        *kind, args, *target, field_pos, count,
                    );
                }
                Instruction::CondCall {
                    cond: _,
                    else_start: _,
                    continuation: _,
                } => {}
                Instruction::Object {
                    mappings: _,
                    target: _,
                } => todo!(),
                Instruction::Array {
                    elements: elems,
                    target: tgt,
                } => self.execute_array(elems, *tgt, field_pos, count),
                Instruction::Move { src, tgt } => {
                    self.execute_move(src, *tgt, field_pos, count);
                }
                Instruction::ClearTemporary(temp_id) => {
                    let tmp = &mut self.temp_fields[temp_id.index];
                    for slot in &mut *tmp.iter_slots {
                        *slot = IterStateRaw::default();
                    }
                    debug_assert!(
                        tmp.field_pos.get() + tmp.data.borrow().field_count()
                            == field_pos + count
                    );
                    tmp.data.borrow_mut().clear();
                    tmp.field_pos.set(usize::MAX);
                }
            }
        }
    }

    pub fn run(
        &mut self,
        insn_range: Range<InstructionId>,
        field_pos: usize,
        count: usize,
    ) {
        if count == 0 {
            return;
        }
        self.handle_batch(insn_range, field_pos, count);
        for ef in self.extern_fields.iter_mut() {
            for slot in &mut *ef.iter_slots {
                *slot = None;
            }
        }
    }
}
