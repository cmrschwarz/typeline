use std::ops::Range;

use crate::{
    index_newtype,
    record_data::{
        field::FieldManager,
        field_data::FieldData,
        iter_hall::{IterHall, IterStateRaw},
        iters::{DestructuredFieldDataRef, FieldIter},
        match_set::MatchSetManager,
        push_interface::PushInterface,
        ref_iter::AutoDerefIter,
        varying_type_inserter::VaryingTypeInserter,
    },
    utils::{
        index_slice::IndexSlice, multi_ref_mut_handout::MultiRefMutHandout,
        universe::Universe,
    },
};

use super::{
    ast::{BinaryOpKind, ExternIdentId},
    compiler::{
        Compilation, Instruction, InstructionId, TargetRef, TemporaryIdRaw,
        ValueAccess,
    },
    ExternField, ExternFieldIdx, ExternVarData, TempVarData,
};

index_newtype! {
    pub struct UnboundVarIterId(u32);
}

pub struct Exectutor<'a, 'b> {
    pub compilation: &'a Compilation,
    pub fm: &'a FieldManager,
    pub msm: &'a MatchSetManager,
    pub temp_vars: &'a mut IndexSlice<TemporaryIdRaw, TempVarData>,
    pub extern_vars: &'a mut IndexSlice<ExternIdentId, ExternVarData>,
    pub extern_fields: &'a mut IndexSlice<ExternFieldIdx, ExternField>,
    pub extern_field_iters: &'a mut IndexSlice<
        ExternFieldIdx,
        AutoDerefIter<'b, FieldIter<'b, DestructuredFieldDataRef<'b>>>,
    >,
    pub output: &'a mut IterHall,
    pub extern_field_temp_iters: Universe<
        UnboundVarIterId,
        AutoDerefIter<'b, FieldIter<'b, DestructuredFieldDataRef<'b>>>,
    >,
}

fn get_inserter<'a, const CAP: usize>(
    output: &'a mut IterHall,
    temp: &mut MultiRefMutHandout<'a, TemporaryIdRaw, TempVarData, CAP>,
    idx: Option<TemporaryIdRaw>,
    field_pos: usize,
) -> VaryingTypeInserter<&'a mut FieldData> {
    match idx {
        Some(tmp_id) => {
            let tmp = temp.claim(tmp_id);

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

impl<'a, 'b> Exectutor<'a, 'b> {
    fn execute_op_binary(
        &mut self,
        _kind: BinaryOpKind,
        _lhs: &ValueAccess,
        _rhs: &ValueAccess,
        _tgt: TargetRef,
        _field_pos: usize,
        _count: usize,
    ) {
        todo!()
    }

    fn execute_move(
        &mut self,
        src: &ValueAccess,
        tgt: TargetRef,
        field_pos: usize,
        count: usize,
    ) {
        let output_tmp_id = match tgt {
            TargetRef::Temporary(id) => Some(id),
            TargetRef::Output => None,
            TargetRef::Discard => return,
        };
        let mut temp_handouts = self.temp_vars.multi_ref_mut_handout::<2>();
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
                        let ef = &mut self.extern_fields[*extern_field_idx];
                        let iter_slot_idx = if let Some(iter_slot_idx) =
                            ef.iter_slots[acc.access_idx]
                        {
                            iter_slot_idx
                        } else {
                            let iter_slot_idx =
                                self.extern_field_temp_iters.claim_with_value(
                                    self.extern_field_iters[*extern_field_idx]
                                        .clone(),
                                );
                            ef.iter_slots[acc.access_idx] =
                                Some(iter_slot_idx);
                            iter_slot_idx
                        };
                        let iter =
                            &mut self.extern_field_temp_iters[iter_slot_idx];

                        iter.move_to_field_pos(field_pos);
                        inserter.extend_from_auto_deref_iter(
                            self.msm, iter, count, true, false,
                        );
                    }
                }
            }
            ValueAccess::Temporary(tmp_in) => {
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
                    let tmp = &mut self.temp_vars[temp_id.index];
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
