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
        field_data::{FieldData, FieldValueRepr},
        field_value_ref::FieldValueSlice,
        field_value_slice_iter::{FieldValueBlock, FieldValueSliceIter},
        group_tracker::{GroupListId, GroupListIterId},
        iter_hall::{IterId, IterKind},
        push_interface::PushInterface,
        ref_iter::RefAwareFieldValueSliceIter,
        varying_type_inserter::VaryingTypeInserter,
    },
    smallbox,
    utils::identity_hasher::BuildIdentityHasher,
};

use scr_core::operators::utils::any_number::AnyNumber;

#[derive(Clone, Default)]
pub struct OpSum {}

pub struct TfSum {
    input_iter_id: IterId,
    group_list_id: GroupListId,
    group_list_iter_id: GroupListIterId,
    aggregate: AnyNumber,
    current_group_error_type: Option<FieldValueRepr>,
    actor_id: ActorId,
    floating_point_math: bool,
    pending_field: bool,
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
        let ms = &mut jd.match_set_mgr.match_sets[tf_state.match_set_id];
        let mut ab = ms.action_buffer.borrow_mut();
        let actor_id = ab.add_actor();
        jd.field_mgr.fields[tf_state.output_field]
            .borrow_mut()
            .first_actor = ActorRef::Unconfirmed(ab.peek_next_actor_id());
        let floating_point_math = jd.session_data.chains
            [op_base.chain_id.unwrap() as usize]
            .settings
            .floating_point_math;
        TransformData::Custom(smallbox!(TfSum {
            group_list_id: ms.group_tracker.active_group_list(),
            group_list_iter_id: ms
                .group_tracker
                .claim_group_list_iter_for_active(),
            input_iter_id: jd.field_mgr.claim_iter(
                tf_state.input_field,
                IterKind::Transform(jd.tf_mgr.transforms.peek_claim_id())
            ),
            aggregate: AnyNumber::Int(0),
            actor_id,
            current_group_error_type: None,
            floating_point_math,
            pending_field: false
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
            result.push(inserter, true, true);
        }
    }
    fn transform_update(&mut self, bud: BasicUpdateData) -> (usize, bool) {
        let op_id = bud.tf_mgr.transforms[bud.tf_id].op_id.unwrap();
        let fpm = self.floating_point_math;

        let mut output_field = bud
            .field_mgr
            .borrow_field_dealiased_mut(bud.output_field_id);
        let mut inserter = output_field.iter_hall.varying_type_inserter();

        let ms = &bud.match_set_mgr.match_sets[bud.match_set_id];

        let mut ab = ms.action_buffer.borrow_mut();

        let mut group_iter = ms.group_tracker.lookup_group_list_iter_mut(
            self.group_list_id,
            self.group_list_iter_id,
            &mut ab,
            self.actor_id,
        );

        let mut finished_group_count = 0;
        let mut last_finished_group_end = group_iter.field_pos();
        let mut batch_size_rem = bud.batch_size;

        loop {
            if group_iter.is_end_of_group(bud.ps.input_done) {
                let group_size = group_iter.field_pos()
                    - last_finished_group_end
                    + usize::from(self.pending_field);
                self.pending_field = false;
                let mut zero_count = 0;
                if group_size == 0 {
                    group_iter.insert_fields(FieldValueRepr::Undefined, 1);
                    zero_count += 1;
                } else {
                    group_iter.drop_backwards(group_size - 1);
                    self.finish_group(op_id, &mut inserter);
                    finished_group_count += 1;
                }
                loop {
                    if !group_iter.try_next_group() {
                        break;
                    }
                    if !group_iter.is_end_of_group(bud.ps.input_done) {
                        break;
                    }
                    group_iter.insert_fields(FieldValueRepr::Undefined, 1);
                    zero_count += 1;
                }
                last_finished_group_end = group_iter.field_pos();
                inserter.push_int(0, zero_count, true, true);
                finished_group_count += zero_count;
            }
            let Some(range) = bud.iter.typed_range_fwd(
                bud.match_set_mgr,
                group_iter.group_len_rem().min(batch_size_rem),
                0,
            ) else {
                break;
            };

            let count = range.base.field_count;
            batch_size_rem -= count;
            group_iter.next_n_fields_in_group(count);
            match range.base.data {
                FieldValueSlice::Int(ints) => {
                    let mut iter =
                        FieldValueSliceIter::from_range(&range, ints);
                    while let Some(b) = iter.next_block() {
                        match b {
                            FieldValueBlock::Plain(v) => {
                                self.aggregate.add_ints(v, fpm)
                            }
                            FieldValueBlock::WithRunLength(v, rl) => {
                                self.aggregate.add_int_with_rl(*v, rl, fpm)
                            }
                        }
                    }
                }
                FieldValueSlice::BigInt(ints) => {
                    for (v, rl) in
                        RefAwareFieldValueSliceIter::from_range(&range, ints)
                    {
                        self.aggregate.add_big_int(v, rl, fpm)
                    }
                }
                FieldValueSlice::Float(floats) => {
                    for (v, rl) in
                        FieldValueSliceIter::from_range(&range, floats)
                    {
                        self.aggregate.add_float(*v, rl, fpm)
                    }
                }
                FieldValueSlice::Rational(rationals) => {
                    for (v, rl) in RefAwareFieldValueSliceIter::from_range(
                        &range, rationals,
                    ) {
                        self.aggregate.add_rational(v, rl, fpm)
                    }
                }
                FieldValueSlice::Null(_)
                | FieldValueSlice::Undefined(_)
                | FieldValueSlice::BytesInline(_)
                | FieldValueSlice::TextInline(_)
                | FieldValueSlice::TextBuffer(_)
                | FieldValueSlice::BytesBuffer(_)
                | FieldValueSlice::Array(_)
                | FieldValueSlice::Object(_)
                | FieldValueSlice::Custom(_)
                | FieldValueSlice::StreamValueId(_)
                | FieldValueSlice::Error(_)
                | FieldValueSlice::FieldReference(_)
                | FieldValueSlice::SlicedFieldReference(_) => {
                    self.current_group_error_type =
                        Some(range.base.data.repr());
                }
            }
        }
        let pending_group_size =
            group_iter.field_pos() - last_finished_group_end;

        if pending_group_size > 0 {
            let drop_count =
                pending_group_size - usize::from(!self.pending_field);
            self.pending_field = true;
            group_iter.drop_backwards(drop_count);
        }
        group_iter.store_iter(self.group_list_iter_id);

        (finished_group_count, bud.ps.input_done)
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
