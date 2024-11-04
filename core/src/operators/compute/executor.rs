use bstr::ByteSlice;
use num::BigInt;

use super::{
    ast::{AccessIdx, BinaryOpKind, BuiltinFunction, ExternIdentId},
    binary_ops::{BinOpAdd, BinOpSub, OverflowingBinOp},
    compiler::{
        Compilation, Instruction, InstructionId, TargetRef, TempFieldIdRaw,
        ValueAccess,
    },
    ExternField, ExternFieldIdx, ExternVarData, TempField,
};
use crate::{
    index_newtype,
    operators::{errors::OperatorApplicationError, operator::OperatorId},
    record_data::{
        field::FieldManager,
        field_data::FieldData,
        field_value::FieldValueKind,
        field_value_ref::FieldValueSlice,
        field_value_slice_iter::{FieldValueBlock, FieldValueRangeIter},
        iter_hall::{IterHall, IterStateRaw},
        iters::{
            DestructuredFieldDataRef, FieldIter, FieldIterOpts, FieldIterator,
        },
        match_set::MatchSetManager,
        push_interface::PushInterface,
        ref_iter::{
            AutoDerefIter, RefAwareBytesBufferIter, RefAwareInlineBytesIter,
            RefAwareInlineTextIter, RefAwareTextBufferIter,
            RefAwareTypedRange,
        },
        single_value_iter::{AtomIter, FieldValueIter, SingleValueIter},
        varying_type_inserter::VaryingTypeInserter,
    },
    utils::{
        index_slice::IndexSlice,
        multi_ref_mut_handout::MultiRefMutHandout,
        universe::{Universe, UniverseMultiRefMutHandout},
    },
};
use metamatch::metamatch;
use std::ops::Range;

index_newtype! {
    pub struct ExternFieldTempIterId(u32);
}

pub struct Exectutor<'a, 'b> {
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
        AutoDerefIter<'b, FieldIter<'b, DestructuredFieldDataRef<'b>>>,
    >,
    pub extern_field_temp_iters: Universe<
        ExternFieldTempIterId,
        AutoDerefIter<'b, FieldIter<'b, DestructuredFieldDataRef<'b>>>,
    >,
}

#[allow(clippy::large_enum_variant)]
pub enum ExecutorInputIter<'a, 'b, 'c> {
    AutoDerefIter(
        &'a mut AutoDerefIter<'b, FieldIter<'b, DestructuredFieldDataRef<'b>>>,
    ),
    FieldIter(FieldIter<'c, &'c FieldData>),
    Atom(AtomIter<'a>),
    FieldValue(FieldValueIter<'a>),
}

impl<'a, 'b, 'c> ExecutorInputIter<'a, 'b, 'c> {
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
}

fn get_inserter<'a, const CAP: usize>(
    output: &'a mut IterHall,
    temp_field_handouts: &mut MultiRefMutHandout<
        'a,
        TempFieldIdRaw,
        TempField,
        CAP,
    >,
    idx: Option<TempFieldIdRaw>,
    field_pos: usize,
) -> VaryingTypeInserter<&'a mut FieldData> {
    match idx {
        Some(tmp_id) => {
            let tmp = temp_field_handouts.claim(tmp_id);

            let mut inserter = tmp.data.varying_type_inserter();
            if tmp.field_pos == usize::MAX {
                tmp.field_pos = field_pos;
            } else {
                debug_assert!(tmp.field_pos <= field_pos);
                inserter.push_undefined(field_pos - tmp.field_pos, true);
            }
            inserter
        }
        None => output.varying_type_inserter(),
    }
}

fn get_extern_iter<'a, 'b, const ITER_CAP: usize>(
    extern_fields: &mut IndexSlice<ExternFieldIdx, ExternField>,
    extern_field_iters: &mut IndexSlice<
        ExternFieldIdx,
        AutoDerefIter<'b, FieldIter<'b, DestructuredFieldDataRef<'b>>>,
    >,
    extern_field_temp_iter_handouts: &mut UniverseMultiRefMutHandout<
        'a,
        ExternFieldTempIterId,
        AutoDerefIter<'b, FieldIter<'b, DestructuredFieldDataRef<'b>>>,
        ITER_CAP,
    >,
    extern_field_idx: ExternFieldIdx,
    access_idx: AccessIdx,
) -> &'a mut AutoDerefIter<'b, FieldIter<'b, DestructuredFieldDataRef<'b>>> {
    let ef = &mut extern_fields[extern_field_idx];
    if let Some(iter_slot_idx) = ef.iter_slots[access_idx] {
        return extern_field_temp_iter_handouts.claim(iter_slot_idx);
    }
    let (iter_slot_idx, iter) = extern_field_temp_iter_handouts
        .claim_new(extern_field_iters[extern_field_idx].clone());
    ef.iter_slots[access_idx] = Some(iter_slot_idx);
    iter
}

fn get_executor_input_iter<
    'a,
    'b,
    const FIELD_CAP: usize,
    const ITER_CAP: usize,
>(
    input: &'a ValueAccess,
    extern_vars: &'a IndexSlice<ExternIdentId, ExternVarData>,
    extern_fields: &mut IndexSlice<ExternFieldIdx, ExternField>,
    extern_field_iters: &mut IndexSlice<
        ExternFieldIdx,
        AutoDerefIter<'b, FieldIter<'b, DestructuredFieldDataRef<'b>>>,
    >,
    temp_field_handouts: &mut MultiRefMutHandout<
        'a,
        TempFieldIdRaw,
        TempField,
        FIELD_CAP,
    >,
    extern_field_temp_iter_handouts: &mut UniverseMultiRefMutHandout<
        'a,
        ExternFieldTempIterId,
        AutoDerefIter<'b, FieldIter<'b, DestructuredFieldDataRef<'b>>>,
        ITER_CAP,
    >,
    field_pos: usize,
    count: usize,
) -> ExecutorInputIter<'a, 'b, 'a> {
    match input {
        ValueAccess::Extern(acc) => match &extern_vars[acc.index] {
            ExternVarData::Atom(atom) => ExecutorInputIter::Atom(
                SingleValueIter::from_atom(atom, count),
            ),
            ExternVarData::Literal(v) => {
                ExecutorInputIter::FieldValue(SingleValueIter::new(v, count))
            }
            ExternVarData::Field(extern_field_idx) => {
                let iter = get_extern_iter(
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
            let temp_field = temp_field_handouts.claim(tmp_in.index);
            ExecutorInputIter::FieldIter(temp_field.data.iter())
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
    inserter: &mut VaryingTypeInserter<&mut FieldData>,
) {
    inserter.push_error(
        OperatorApplicationError::new_s(
            format!("invalid operands for binary op: '{lhs_kind}' {op_kind} '{rhs_kind}'"),
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
    inserter: &mut VaryingTypeInserter<&mut FieldData>,
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

fn execute_binary_op_double_int_overflowing<BinOp: OverflowingBinOp>(
    lhs_block: FieldValueBlock<i64>,
    rhs_range: &RefAwareTypedRange,
    rhs_data: &[i64],
    inserter: &mut VaryingTypeInserter<&mut FieldData>,
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
                                .reserve_for_fixed_size::<i64>(len - i);
                            let success = BinOp::calc_until_overflow(
                                &lhs_data[lhs_block_offset + i..],
                                &rhs_data[i..],
                                res,
                            );
                            unsafe {
                                inserter.add_count(success);
                            }
                            i += success;
                            if i == len {
                                break;
                            }
                            let res = BinOp::calc_into_bigint(
                                lhs_data[lhs_block_offset + i],
                                rhs_data[i],
                            );
                            inserter.push_big_int(res, 1, true, false);
                            i += 1;
                        }
                        lhs_block_offset += len;
                    }
                    FieldValueBlock::WithRunLength(&rhs_val, rhs_rl) => {
                        let len = rhs_rl as usize;
                        let mut i = 0;
                        while i < len {
                            let res = inserter
                                .reserve_for_fixed_size::<i64>(len - i);
                            let success =
                                BinOp::calc_until_overflow_rhs_immediate(
                                    &lhs_data[lhs_block_offset + i..],
                                    rhs_val,
                                    res,
                                );
                            unsafe {
                                inserter.add_count(success);
                            }
                            i += success;
                            if i == len {
                                break;
                            }
                            let res = BinOp::calc_into_bigint(
                                lhs_data[lhs_block_offset + i],
                                rhs_val,
                            );
                            inserter.push_big_int(res, 1, true, false);
                            i += 1;
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
                                .reserve_for_fixed_size::<i64>(len - i);
                            let success =
                                BinOp::calc_until_overflow_lhs_immediate(
                                    lhs_val,
                                    &rhs_data[i..],
                                    res,
                                );
                            unsafe {
                                inserter.add_count(success);
                            }
                            i += success;
                            if i == len {
                                break;
                            }
                            let res =
                                BinOp::calc_into_bigint(lhs_val, rhs_data[i]);
                            inserter.push_big_int(res, 1, true, false);
                            i += 1;
                        }
                    }
                    FieldValueBlock::WithRunLength(&rhs_val, rhs_rl) => {
                        let len = rhs_rl as usize;
                        match BinOp::try_calc_single(lhs_val, rhs_val) {
                            Some(res) => {
                                inserter.push_int(res, len, true, false)
                            }
                            None => inserter.push_big_int(
                                BinOp::calc_into_bigint(lhs_val, rhs_val),
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

fn execute_binary_op_double_int(
    _op_id: OperatorId,
    op_kind: BinaryOpKind,
    lhs_block: FieldValueBlock<i64>,
    rhs_range: &RefAwareTypedRange,
    rhs_data: &[i64],
    inserter: &mut VaryingTypeInserter<&mut FieldData>,
) {
    match op_kind {
        BinaryOpKind::Equals => todo!(),
        BinaryOpKind::NotEquals => todo!(),
        BinaryOpKind::LessThan => todo!(),
        BinaryOpKind::GreaterThan => todo!(),
        BinaryOpKind::LessThanEquals => todo!(),
        BinaryOpKind::GreaterThanEquals => todo!(),
        BinaryOpKind::Add => {
            execute_binary_op_double_int_overflowing::<BinOpAdd>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::Subtract => {
            execute_binary_op_double_int_overflowing::<BinOpSub>(
                lhs_block, rhs_range, rhs_data, inserter,
            )
        }
        BinaryOpKind::AddAssign => todo!(),
        BinaryOpKind::SubtractAssign => todo!(),
        BinaryOpKind::Multiply => todo!(),
        BinaryOpKind::MultiplyAssign => todo!(),
        BinaryOpKind::Divide => todo!(),
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
    inserter: &mut VaryingTypeInserter<&mut FieldData>,
) {
    let mut lhs_iter = FieldValueRangeIter::from_range(lhs_range, lhs_data);
    while let Some(lhs_block) = lhs_iter.next_block() {
        let rhs_range = rhs_iter
            .typed_range_fwd(msm, lhs_block.len(), FieldIterOpts::default())
            .unwrap();

        match rhs_range.base.data {
            FieldValueSlice::Int(rhs_data) => execute_binary_op_double_int(
                op_id, op_kind, lhs_block, &rhs_range, rhs_data, inserter,
            ),
            FieldValueSlice::BigInt(_) => todo!(),
            FieldValueSlice::Float(_) => todo!(),
            FieldValueSlice::BigRational(_) => todo!(),
            FieldValueSlice::Null(_)
            | FieldValueSlice::Undefined(_)
            | FieldValueSlice::TextInline(_)
            | FieldValueSlice::TextBuffer(_)
            | FieldValueSlice::BytesInline(_)
            | FieldValueSlice::BytesBuffer(_)
            | FieldValueSlice::Object(_)
            | FieldValueSlice::Array(_)
            | FieldValueSlice::Custom(_)
            | FieldValueSlice::Error(_)
            | FieldValueSlice::Argument(_)
            | FieldValueSlice::Macro(_)
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

fn execute_binary_op(
    op_id: OperatorId,
    msm: &MatchSetManager,
    op_kind: BinaryOpKind,
    lhs_range: &RefAwareTypedRange,
    rhs_iter: &mut ExecutorInputIter,
    inserter: &mut VaryingTypeInserter<&mut FieldData>,
) {
    match lhs_range.base.data {
        FieldValueSlice::Int(lhs_data) => execute_binary_op_for_int_lhs(
            op_id, msm, op_kind, lhs_range, lhs_data, rhs_iter, inserter,
        ),
        FieldValueSlice::BigInt(_)
        | FieldValueSlice::BigRational(_)
        | FieldValueSlice::Float(_)
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
        | FieldValueSlice::Macro(_)
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
    inserter: &mut VaryingTypeInserter<&mut FieldData>,
) {
    match kind {
        BuiltinFunction::Cast(FieldValueKind::Int) => {
            execute_cast_int(op_id, arg_range, inserter)
        }
        _ => todo!(),
    }
}

fn execute_cast_int(
    op_id: OperatorId,
    range: RefAwareTypedRange<'_>,
    inserter: &mut VaryingTypeInserter<&mut FieldData>,
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
        FieldValueSlice::Float(_) => todo!(),
        FieldValueSlice::BigRational(_) => todo!(),
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
        | FieldValueSlice::Macro(_)
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

impl<'a, 'b> Exectutor<'a, 'b> {
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
        let mut temp_field_handouts =
            self.temp_fields.multi_ref_mut_handout::<3>();
        let mut extern_field_temp_iter_handouts =
            self.extern_field_temp_iters.multi_ref_mut_handout::<2>();
        let mut inserter = get_inserter(
            self.output,
            &mut temp_field_handouts,
            output_tmp_id,
            field_pos,
        );
        let mut lhs_iter = get_executor_input_iter(
            lhs,
            self.extern_vars,
            self.extern_fields,
            self.extern_field_iters,
            &mut temp_field_handouts,
            &mut extern_field_temp_iter_handouts,
            field_pos,
            count,
        );
        let mut rhs_iter = get_executor_input_iter(
            rhs,
            self.extern_vars,
            self.extern_fields,
            self.extern_field_iters,
            &mut temp_field_handouts,
            &mut extern_field_temp_iter_handouts,
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
        const MAX_ARGS: usize = 1;
        let output_tmp_id = match tgt {
            TargetRef::TempField(id) => Some(id),
            TargetRef::Output => None,
            TargetRef::Discard => {
                return;
            }
        };
        let mut temp_field_handouts =
            self.temp_fields.multi_ref_mut_handout::<{ MAX_ARGS + 1 }>();
        let mut extern_field_temp_iter_handouts = self
            .extern_field_temp_iters
            .multi_ref_mut_handout::<MAX_ARGS>();
        let mut inserter = get_inserter(
            self.output,
            &mut temp_field_handouts,
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
            self.extern_vars,
            self.extern_fields,
            self.extern_field_iters,
            &mut temp_field_handouts,
            &mut extern_field_temp_iter_handouts,
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
        let mut extern_field_temp_iter_handouts =
            self.extern_field_temp_iters.multi_ref_mut_handout::<1>();
        let mut temp_handouts = self.temp_fields.multi_ref_mut_handout::<2>();

        let mut inserter = get_inserter(
            self.output,
            &mut temp_handouts,
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
                        let iter = get_extern_iter(
                            self.extern_fields,
                            self.extern_field_iters,
                            &mut extern_field_temp_iter_handouts,
                            *extern_field_idx,
                            acc.access_idx,
                        );
                        iter.move_to_field_pos(field_pos);
                        inserter.extend_from_auto_deref_iter(
                            self.msm, iter, count, true, false,
                        );
                    }
                }
            }
            ValueAccess::TempField(tmp_in) => {
                inserter.extend_from_iter(
                    temp_handouts.claim(tmp_in.index).data.iter(),
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
                    elements: _,
                    target: _,
                } => todo!(),
                Instruction::Move { src, tgt } => {
                    self.execute_move(src, *tgt, field_pos, count);
                }
                Instruction::ClearTemporary(temp_id) => {
                    let tmp = &mut self.temp_fields[temp_id.index];
                    for slot in &mut *tmp.iter_slots {
                        *slot = IterStateRaw::default();
                    }
                    debug_assert!(
                        tmp.field_pos + tmp.data.field_count()
                            == field_pos + count
                    );
                    tmp.data.clear();
                    tmp.field_pos = usize::MAX;
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
        self.handle_batch(insn_range, field_pos, count);
        for ef in self.extern_fields.iter_mut() {
            for slot in &mut *ef.iter_slots {
                *slot = None;
            }
        }
    }
}
