use std::collections::HashMap;

use scr_core::{
    job::JobData,
    liveness_analysis::{
        AccessFlags, BasicBlockId, LivenessData, OpOutputIdx,
    },
    operators::{
        errors::OperatorApplicationError,
        operator::{Operator, OperatorBase, OperatorData, OperatorId},
        transform::{
            basic_transform_update, BasicUpdateData, DefaultTransformName,
            Transform, TransformData, TransformState,
        },
    },
    record_data::{
        action_buffer::{ActorId, ActorRef},
        field::FieldId,
        field_action::FieldActionKind::{self, Drop},
        field_value_repr::{FieldData, FieldValueRepr},
        iter_hall::IterId,
        push_interface::{PushInterface, VaryingTypeInserter},
        ref_iter::RefAwareTypedSliceIter,
        typed::TypedSlice,
        typed_iters::TypedSliceIter,
    },
    smallbox,
    utils::identity_hasher::BuildIdentityHasher,
};

use scr_core::operators::utils::any_number::AnyNumber;

#[derive(Clone, Default)]
pub struct OpSum {}

pub struct TfSum {
    input_iter_id: IterId,
    aggregate: AnyNumber,
    current_group_error_type: Option<FieldValueRepr>,
    actor_id: ActorId,
    floating_point_math: bool,
}

impl Operator for OpSum {
    fn default_name(
        &self,
    ) -> scr_core::operators::operator::DefaultOperatorName {
        "sum".into()
    }

    fn output_count(&self, _op_base: &OperatorBase) -> usize {
        1
    }

    fn has_dynamic_outputs(&self, _op_base: &OperatorBase) -> bool {
        false
    }

    fn update_variable_liveness(
        &self,
        _ld: &mut LivenessData,
        _bb_id: BasicBlockId,
        _access_flags: &mut AccessFlags,
    ) {
    }

    fn build_transform<'a>(
        &'a self,
        jd: &mut JobData,
        op_base: &OperatorBase,
        tf_state: &mut TransformState,
        _prebound_outputs: &HashMap<OpOutputIdx, FieldId, BuildIdentityHasher>,
    ) -> TransformData<'a> {
        let ab = &mut jd.match_set_mgr.match_sets[tf_state.match_set_id]
            .action_buffer;
        let actor_id = ab.add_actor();
        jd.field_mgr.fields[tf_state.output_field]
            .borrow_mut()
            .first_actor = ActorRef::Unconfirmed(ab.peek_next_actor_id());
        let floating_point_math = jd.session_data.chains
            [op_base.chain_id.unwrap() as usize]
            .settings
            .floating_point_math;
        TransformData::Custom(smallbox!(TfSum {
            input_iter_id: jd.field_mgr.claim_iter(tf_state.input_field),
            aggregate: AnyNumber::Int(0),
            actor_id,
            current_group_error_type: None,
            floating_point_math
        }))
    }
}

impl TfSum {
    fn finish_group(
        &mut self,
        op_id: OperatorId,
        inserter: &mut VaryingTypeInserter<&mut FieldData>,
    ) {
        let result = std::mem::take(&mut self.aggregate);
        if let Some(err_type) = self.current_group_error_type.take() {
            inserter.push_error(
                OperatorApplicationError::new_s(
                    format!("cannot sum over type `{err_type}`"),
                    op_id,
                ),
                1,
                false,
                false,
            );
        } else {
            result.push(inserter);
        }
    }
    fn transform_update(&mut self, bud: BasicUpdateData) -> (usize, bool) {
        let op_id = bud.tf_mgr.transforms[bud.tf_id].op_id.unwrap();
        let fpm = self.floating_point_math;

        let mut finished_group_count = 0;
        let mut last_finished_group_end = 0;
        let mut output_field = bud
            .field_mgr
            .borrow_field_dealiased_mut(bud.output_field_id);
        let mut inserter = output_field.iter_hall.varying_type_inserter();
        let mut field_pos = 0;

        bud.match_set_mgr.match_sets[bud.match_set_id]
            .action_buffer
            .begin_action_group(self.actor_id);

        while let Some(range) = bud.iter.next_range(bud.match_set_mgr) {
            field_pos += range.base.field_count;
            match range.base.data {
                TypedSlice::GroupSeparator(_) => {
                    // group separators don't count
                    let gs_count = range.base.field_count;
                    let group_size =
                        field_pos - last_finished_group_end - gs_count;
                    let output_record_count = finished_group_count * 2;
                    let ab = &mut bud.match_set_mgr.match_sets
                        [bud.match_set_id]
                        .action_buffer;
                    ab.push_action(
                        Drop,
                        output_record_count,
                        group_size.saturating_sub(1),
                    );
                    if group_size == 0 {
                        ab.push_action(
                            FieldActionKind::InsertZst(
                                FieldValueRepr::Undefined,
                            ),
                            output_record_count,
                            1,
                        );
                    }
                    self.finish_group(op_id, &mut inserter);
                    inserter.push_group_separator(1, true);
                    for _ in 1..gs_count {
                        inserter.push_int(0, 1, true, true);
                        inserter.push_group_separator(1, true);
                    }
                    last_finished_group_end = field_pos;
                    finished_group_count += gs_count;
                }
                TypedSlice::Int(ints) => {
                    for (v, rl) in TypedSliceIter::from_range(&range, ints) {
                        self.aggregate.add_int(*v, rl, fpm)
                    }
                }
                TypedSlice::BigInt(ints) => {
                    for (v, rl) in
                        RefAwareTypedSliceIter::from_range(&range, ints)
                    {
                        self.aggregate.add_big_int(v, rl, fpm)
                    }
                }
                TypedSlice::Float(floats) => {
                    for (v, rl) in TypedSliceIter::from_range(&range, floats) {
                        self.aggregate.add_float(*v, rl, fpm)
                    }
                }
                TypedSlice::Rational(rationals) => {
                    for (v, rl) in
                        RefAwareTypedSliceIter::from_range(&range, rationals)
                    {
                        self.aggregate.add_rational(v, rl, fpm)
                    }
                }
                TypedSlice::Null(_)
                | TypedSlice::Undefined(_)
                | TypedSlice::BytesInline(_)
                | TypedSlice::TextInline(_)
                | TypedSlice::TextBuffer(_)
                | TypedSlice::BytesBuffer(_)
                | TypedSlice::Array(_)
                | TypedSlice::Object(_)
                | TypedSlice::Custom(_)
                | TypedSlice::StreamValueId(_)
                | TypedSlice::Error(_)
                | TypedSlice::FieldReference(_)
                | TypedSlice::SlicedFieldReference(_) => {
                    self.current_group_error_type =
                        Some(range.base.data.repr());
                }
            }
        }
        let last_group_size = field_pos - last_finished_group_end;
        let mut output_record_count = finished_group_count * 2;
        let ab =
            &mut bud.match_set_mgr.match_sets[bud.match_set_id].action_buffer;
        if bud.ps.input_done {
            self.finish_group(op_id, &mut inserter);
            ab.push_action(
                Drop,
                output_record_count,
                last_group_size.saturating_sub(bud.ps.input_done as usize),
            );
            if last_group_size == 0 {
                ab.push_action(
                    FieldActionKind::InsertZst(FieldValueRepr::Undefined),
                    output_record_count,
                    1,
                );
                output_record_count += 1;
            }
            output_record_count += 1;
        }
        ab.end_action_group();
        (output_record_count, bud.ps.input_done)
    }
}

impl Transform for TfSum {
    fn display_name(&self) -> DefaultTransformName {
        "sum".into()
    }

    fn update(
        &mut self,
        jd: &mut JobData,
        tf_id: scr_core::operators::transform::TransformId,
    ) {
        basic_transform_update(jd, tf_id, [], self.input_iter_id, |bud| {
            self.transform_update(bud)
        });
    }
}

pub fn create_op_sum() -> OperatorData {
    OperatorData::Custom(smallbox!(OpSum {}))
}
