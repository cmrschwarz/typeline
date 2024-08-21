use crate::{
    index_newtype,
    record_data::{
        field::FieldManager,
        field_data::FieldData,
        iter_hall::{IterHall, IterStateRaw},
        iters::{
            DestructuredFieldDataRef, FieldIter, FieldIterOpts, FieldIterator,
        },
        match_set::MatchSetManager,
        push_interface::PushInterface,
        ref_iter::{AutoDerefIter, RefAwareTypedRange},
        single_value_iter::{AtomIter, FieldValueIter, SingleValueIter},
        varying_type_inserter::VaryingTypeInserter,
    },
    utils::{
        index_slice::IndexSlice,
        multi_ref_mut_handout::MultiRefMutHandout,
        universe::{Universe, UniverseMultiRefMutHandout},
    },
};
use std::ops::Range;

use super::{
    ast::{AccessIdx, BinaryOpKind, ExternIdentId},
    compiler::{
        Compilation, Instruction, InstructionId, TargetRef, TempFieldIdRaw,
        ValueAccess,
    },
    ExternField, ExternFieldIdx, ExternVarData, TempField,
};

index_newtype! {
    pub struct ExternFieldTempIterId(u32);
}

pub struct Exectutor<'a, 'b> {
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
    count: usize,
    field_pos: usize,
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
            self.extern_field_temp_iters.multi_ref_mut_handout::<3>();
        let mut _inserter = get_inserter(
            self.output,
            &mut temp_field_handouts,
            output_tmp_id,
            field_pos,
        );
        let _lhs_iter = get_executor_input_iter(
            lhs,
            self.extern_vars,
            self.extern_fields,
            self.extern_field_iters,
            &mut temp_field_handouts,
            &mut extern_field_temp_iter_handouts,
            count,
            field_pos,
        );
        let _rhs_iter = get_executor_input_iter(
            rhs,
            self.extern_vars,
            self.extern_fields,
            self.extern_field_iters,
            &mut temp_field_handouts,
            &mut extern_field_temp_iter_handouts,
            count,
            field_pos,
        );
        if kind != BinaryOpKind::Add {
            todo!()
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

    pub fn handle_batch(
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
}
