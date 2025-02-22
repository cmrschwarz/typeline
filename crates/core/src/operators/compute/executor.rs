use bstr::ByteSlice;
use num::{BigInt, BigRational, FromPrimitive};

use super::{
    ast::{
        AccessIdx, BinaryOpKind, BuiltinFunction, ExternIdentId, UnaryOpKind,
    },
    binary_op::execute_binary_op_on_lhs_range,
    compiler::{
        Compilation, Instruction, InstructionId, TargetRef, TempFieldIdRaw,
        ValueAccess,
    },
    executor_inserter::ExecutorInserter,
    unary_op::execute_unary_op_on_range,
    ExternField, ExternFieldIdx, ExternVarData, TempField,
};

use indexland::index_newtype;

use crate::{
    operators::{errors::OperatorApplicationError, operator::OperatorId},
    record_data::{
        array::ArrayBuilder,
        field::{FieldManager, FieldRefOffset},
        field_data::{FieldData, RunLength},
        field_data_ref::DestructuredFieldDataRef,
        field_value::{FieldValue, FieldValueKind},
        field_value_ref::{FieldValueRef, FieldValueSlice},
        iter::{
            field_iter::FieldIter,
            field_iterator::{FieldIterRangeOptions, FieldIterator},
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
        object::{Object, ObjectKeysInternedBuilder, ObjectKeysStoredBuilder},
        push_interface::PushInterface,
        varying_type_inserter::VaryingTypeInserter,
    },
    tyson::TysonParser,
    utils::{
        compare_i64_bigint::{
            convert_int_to_float, try_convert_bigint_to_i64,
        },
        string_store::StringStoreEntry,
    },
};

use indexland::{
    debuggable_nonmax::DebuggableNonMaxUsize,
    index_slice::IndexSlice,
    stable_universe::StableUniverse,
    temp_vec::{TempVec, TransmutableContainer},
};

use metamatch::metamatch;
use std::{
    cell::{Ref, RefCell, RefMut},
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
    pub object_keys_interned_builder: &'a mut ObjectKeysInternedBuilder,
    pub object_keys_stored_builder: &'a mut ObjectKeysStoredBuilder,
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
    ) -> Option<RefAwareTypedRange> {
        match self {
            ExecutorInputIter::AutoDerefIter(iter) => {
                iter.typed_range_fwd(msm, limit)
            }
            ExecutorInputIter::FieldIter(iter) => {
                Some(RefAwareTypedRange::without_refs(iter.typed_range_fwd(
                    limit,
                    FieldIterRangeOptions::default(),
                )?))
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
            // we can skip the check for deleted fields here because
            // we never delete data from temp fields
            let iter = FieldIter::from_start(temp_field, false);
            debug_assert!(iter
                .get_next_header()
                .map(|h| !h.fmt.deleted())
                .unwrap_or(true));
            ExecutorInputIter::FieldIter(iter)
        }
        ValueAccess::Literal(v) => {
            ExecutorInputIter::FieldValue(SingleValueIter::new(v, count))
        }
    }
}

fn execute_builtin_func_on_range(
    op_id: OperatorId,
    kind: BuiltinFunction,
    arg_range: &RefAwareTypedRange<'_>,
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
    range: &RefAwareTypedRange<'_>,
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
            for (v, rl, _) in ITER::from_range(range, data) {
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
            for (&v, rl) in FieldValueRangeIter::from_range(range, values) {
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
            for (v, rl) in FieldValueRangeIter::from_range(range, values) {
                let v = v.to_integer();
                if let Some(v) = try_convert_bigint_to_i64(&v) {
                    inserter.push_int(v, rl as usize, true, false);
                } else {
                    inserter.push_big_int(v, rl as usize, true, false);
                }
            }
        }
        FieldValueSlice::Bool(values) => {
            for (v, rl) in FieldValueRangeIter::from_range(range, values) {
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
    range: &RefAwareTypedRange<'_>,
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
            for (v, rl, _) in ITER::from_range(range, data) {
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
            for (v, rl) in FieldValueRangeIter::from_range(range, values) {
                match convert_int_to_float(*v) {
                    Ok(v) => inserter.push_float(v, rl as usize, true, false),
                    Err(v) => {
                        inserter.push_big_rational(v, rl as usize, true, false)
                    }
                }
            }
        }
        FieldValueSlice::BigInt(values) => {
            for (v, rl) in FieldValueRangeIter::from_range(range, values) {
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
            for (v, rl) in FieldValueRangeIter::from_range(range, values) {
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
    fn execute_unary_op(
        &mut self,
        kind: UnaryOpKind,
        value: &ValueAccess,
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
        let mut value_iter = get_executor_input_iter(
            value,
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
            let range =
                value_iter.typed_range_fwd(self.msm, count_rem).unwrap();

            count_rem -= range.base.field_count;

            execute_unary_op_on_range(self.op_id, kind, &range, &mut inserter);
            if count_rem == 0 {
                break;
            }
        }
    }

    fn execute_binary_op(
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
            let lhs_range =
                lhs_iter.typed_range_fwd(self.msm, count_rem).unwrap();

            count_rem -= lhs_range.base.field_count;

            execute_binary_op_on_lhs_range(
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
            let arg_range =
                arg_iter.typed_range_fwd(self.msm, count_rem).unwrap();

            count_rem -= arg_range.base.field_count;

            execute_builtin_func_on_range(
                self.op_id,
                kind,
                &arg_range,
                &mut inserter,
            );
            if count_rem == 0 {
                break;
            }
        }
    }

    fn execute_dot_access(
        &mut self,
        op_id: OperatorId,
        lhs: &ValueAccess,
        ident: StringStoreEntry,
        ident_str: &str,
        field_pos: usize,
        target: TargetRef,
        count: usize,
    ) {
        debug_assert!(count > 0);
        let output_tmp_id = match target {
            TargetRef::TempField(id) => Some(id),
            TargetRef::Output => None,
            TargetRef::Discard => return,
        };

        let mut iter = get_executor_input_iter(
            lhs,
            self.temp_fields,
            self.extern_vars,
            self.extern_fields,
            self.extern_field_iters,
            self.extern_field_temp_iters,
            field_pos,
            count,
        );

        let mut inserter = get_inserter(
            self.output,
            self.temp_fields,
            output_tmp_id,
            field_pos,
        );

        let mut count_rem = count;

        while count_rem > 0 {
            let range = iter.typed_range_fwd(self.msm, count_rem).unwrap();

            count_rem -= range.base.field_count;

            metamatch!(match range.base.data {
                FieldValueSlice::Object(objects) => {
                    for (v, rl) in
                        FieldValueRangeIter::from_range(&range, objects)
                    {
                        let rl = rl as usize;
                        match v {
                            Object::KeysStored(obj) => {
                                inserter.push_field_value_unpacked(
                                    obj.get(ident_str)
                                        .unwrap_or(&FieldValue::Undefined)
                                        .clone(),
                                    rl,
                                    true,
                                    false,
                                );
                            }
                            Object::KeysInterned(obj) => {
                                inserter.push_field_value_unpacked(
                                    obj.get(&ident)
                                        .unwrap_or(&FieldValue::Undefined)
                                        .clone(),
                                    rl,
                                    true,
                                    false,
                                );
                            }
                        }
                    }
                }

                FieldValueSlice::Error(_) => {
                    inserter.extend_from_ref_aware_range(&range, true, false);
                }
                #[expand_pattern(T in [
                    Null, Undefined, TextInline, TextBuffer, BytesInline,
                    BytesBuffer, Bool, Int, BigInt, Float, BigRational,
                    Array, Custom, Argument, OpDecl, StreamValueId,
                    FieldReference, SlicedFieldReference,
                ])]
                FieldValueSlice::T(_) => {
                    inserter.push_error(
                        OperatorApplicationError::new_s(
                            format!(
                                "cannot cast '{}' to int",
                                range.base.data.kind()
                            ),
                            op_id,
                        ),
                        range.base.field_count,
                        true,
                        false,
                    );
                }
            });

            if count_rem == 0 {
                break;
            }
        }
    }

    fn execute_array(
        &mut self,
        elements: &[ValueAccess],
        target: TargetRef,
        field_pos: usize,
        count: usize,
    ) {
        debug_assert!(count > 0);
        let output_tmp_id = match target {
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
            self.array_builder.push_value(v, rl as usize);
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
            while let Some(idx) = self.array_builder.get_drained_idx() {
                let (v, rl, _) =
                    input_iters[idx].next_field(self.msm, len).unwrap();
                self.array_builder.replenish_drained_value(v, rl as usize);
            }
        }
    }

    fn execute_object_keys_interned(
        &mut self,
        mappings: &[(StringStoreEntry, ValueAccess)],
        target: TargetRef,
        field_pos: usize,
        count: usize,
    ) {
        debug_assert!(count > 0);
        let output_tmp_id = match target {
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

        debug_assert!(self.object_keys_interned_builder.is_empty());

        for (k, v) in mappings {
            let mut iter = get_executor_input_iter(
                v,
                self.temp_fields,
                self.extern_vars,
                self.extern_fields,
                self.extern_field_iters,
                self.extern_field_temp_iters,
                field_pos,
                count,
            );
            let (v, rl, _) = iter.next_field(self.msm, count).unwrap();
            self.object_keys_interned_builder.push_entry(
                *k,
                v.to_field_value(),
                rl as usize,
            );
            input_iters.push(iter);
        }
        let mut rl_rem = count;
        loop {
            let len = self.object_keys_interned_builder.available_len();
            self.object_keys_interned_builder.consume_len(len);
            rl_rem -= len;
            let obj = if rl_rem == 0 {
                self.object_keys_interned_builder.take()
            } else {
                self.object_keys_interned_builder.build()
            };
            inserter.push_object(obj, len, true, false);
            if rl_rem == 0 {
                break;
            }
            while let Some(idx) =
                self.object_keys_interned_builder.get_drained_idx()
            {
                let (v, rl, _) =
                    input_iters[idx].next_field(self.msm, len).unwrap();
                self.object_keys_interned_builder
                    .replenish_drained_value(v.to_field_value(), rl as usize);
            }
        }
    }

    fn execute_object_keys_stored(
        &mut self,
        mappings: &[(ValueAccess, ValueAccess)],
        target: TargetRef,
        field_pos: usize,
        count: usize,
    ) {
        debug_assert!(count > 0);
        let output_tmp_id = match target {
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

        debug_assert!(self.object_keys_stored_builder.is_empty());

        for (k, v) in mappings {
            let mut key_iter = get_executor_input_iter(
                k,
                self.temp_fields,
                self.extern_vars,
                self.extern_fields,
                self.extern_field_iters,
                self.extern_field_temp_iters,
                field_pos,
                count,
            );
            let mut value_iter = get_executor_input_iter(
                v,
                self.temp_fields,
                self.extern_vars,
                self.extern_fields,
                self.extern_field_iters,
                self.extern_field_temp_iters,
                field_pos,
                count,
            );
            let (k, k_rl, _) = key_iter.next_field(self.msm, count).unwrap();
            let (v, v_rl, _) = value_iter.next_field(self.msm, count).unwrap();
            self.object_keys_stored_builder.push_entry(
                // TODO: fixme
                k.text_or_bytes().unwrap().to_str_lossy().to_string(),
                v.to_field_value(),
                k_rl as usize,
                v_rl as usize,
            );
            input_iters.push(key_iter);
            input_iters.push(value_iter);
        }
        let mut rl_rem = count;
        loop {
            let len = self.object_keys_stored_builder.available_len();
            self.object_keys_stored_builder.consume_len(len);
            rl_rem -= len;
            let obj = if rl_rem == 0 {
                self.object_keys_stored_builder.take()
            } else {
                self.object_keys_stored_builder.build()
            };
            inserter.push_object(obj, len, true, false);
            if rl_rem == 0 {
                break;
            }
            while let Some(idx) =
                self.object_keys_stored_builder.get_drained_idx()
            {
                let (x, rl, _) =
                    input_iters[idx].next_field(self.msm, len).unwrap();
                if idx % 2 == 0 {
                    self.object_keys_stored_builder.replenish_drained_key(
                        // TODO: fixme
                        x.text_or_bytes().unwrap().to_str_lossy().to_string(),
                        rl as usize,
                    );
                } else {
                    self.object_keys_stored_builder.replenish_drained_value(
                        x.to_field_value(),
                        rl as usize,
                    );
                }
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
                let tf = self.temp_fields[tmp_in.index].data.borrow();
                let mut iter = tf.iter(false);
                // we can skip the check for deleted fields here because
                // we never delete data from temp fields
                debug_assert!(iter
                    .get_next_header()
                    .map(|h| !h.fmt.deleted())
                    .unwrap_or(true));
                inserter.extend_from_iter(&mut iter, count, true, false);
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
                    kind,
                    value,
                    target,
                } => self
                    .execute_unary_op(*kind, value, *target, field_pos, count),
                Instruction::OpBinary {
                    kind,
                    lhs,
                    rhs,
                    target,
                } => {
                    self.execute_binary_op(
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
                Instruction::ObjectKeysInterned { mappings, target } => self
                    .execute_object_keys_interned(
                        mappings, *target, field_pos, count,
                    ),
                Instruction::ObjectKeysStored { mappings, target } => self
                    .execute_object_keys_stored(
                        mappings, *target, field_pos, count,
                    ),
                Instruction::Array {
                    elements: elems,
                    target,
                } => self.execute_array(elems, *target, field_pos, count),
                Instruction::DotAccess {
                    lhs,
                    ident,
                    ident_str,
                    target,
                } => self.execute_dot_access(
                    self.op_id, lhs, *ident, ident_str, field_pos, *target,
                    count,
                ),
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
                Instruction::ArrayAccess { .. } => todo!(),
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
    }
}
