use std::collections::HashMap;

use scr_core::{
    job::JobData,
    liveness_analysis::{
        AccessFlags, BasicBlockId, LivenessData, OpOutputIdx,
    },
    operators::{
        errors::OperatorApplicationError,
        operator::{Operator, OperatorBase, OperatorData},
        transform::{
            basic_transform_update, BasicUpdateData, DefaultTransformName,
            Transform, TransformData, TransformState,
        },
    },
    record_data::{
        action_buffer::{ActorId, ActorRef},
        field::FieldId,
        field_action::FieldActionKind::Drop,
        iter_hall::IterId,
        push_interface::PushInterface,
        ref_iter::RefAwareTypedSliceIter,
        typed::TypedSlice,
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
    error_occured: bool,
    actor_id: ActorId,
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
        _op_base: &OperatorBase,
        tf_state: &mut TransformState,
        _prebound_outputs: &HashMap<OpOutputIdx, FieldId, BuildIdentityHasher>,
    ) -> TransformData<'a> {
        let ab = &mut jd.match_set_mgr.match_sets[tf_state.match_set_id]
            .action_buffer;
        let actor_id = ab.add_actor();
        jd.field_mgr.fields[tf_state.output_field]
            .borrow_mut()
            .first_actor = ActorRef::Unconfirmed(ab.peek_next_actor_id());
        TransformData::Custom(smallbox!(TfSum {
            input_iter_id: jd.field_mgr.claim_iter(tf_state.input_field),
            aggregate: AnyNumber::Int(0),
            actor_id,
            error_occured: false
        }))
    }
}

impl TfSum {
    fn transform_update(&mut self, bud: BasicUpdateData) -> (usize, bool) {
        let fpm = bud.session_data.chains[bud.session_data.operator_bases
            [bud.tf_mgr.transforms[bud.tf_id].op_id.unwrap() as usize]
            .chain_id
            .unwrap() as usize]
            .settings
            .floating_point_math;
        let mut res = 0;
        while let (Some(range), false) =
            (bud.iter.next_range(bud.match_set_mgr), self.error_occured)
        {
            match range.base.data {
                TypedSlice::Int(ints) => {
                    for (v, rl) in
                        RefAwareTypedSliceIter::from_range(&range, ints)
                    {
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
                    for (v, rl) in
                        RefAwareTypedSliceIter::from_range(&range, floats)
                    {
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
                    bud.field_mgr
                        .borrow_field_dealiased_mut(bud.output_field_id)
                        .iter_hall
                        .push_error(
                            OperatorApplicationError::new_s(
                                format!(
                                    "cannot sum over type `{}`",
                                    range.base.data.repr()
                                ),
                                bud.tf_mgr.transforms[bud.tf_id]
                                    .op_id
                                    .unwrap(),
                            ),
                            1,
                            false,
                            false,
                        );
                    res = 1;
                    self.error_occured = true;
                }
            }
        }
        if bud.ps.input_done && !self.error_occured {
            let of = &mut bud
                .field_mgr
                .borrow_field_dealiased_mut(bud.output_field_id)
                .iter_hall;
            std::mem::take(&mut self.aggregate).push(of);
            res = 1;
        }
        let ab =
            &mut bud.match_set_mgr.match_sets[bud.match_set_id].action_buffer;
        if bud.batch_size > 0 {
            ab.begin_action_group(self.actor_id);
            ab.push_action(Drop, 0, bud.batch_size - res);
            ab.end_action_group();
        }
        (res, res != 0)
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
