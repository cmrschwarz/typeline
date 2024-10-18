use scr_core::{
    cli::call_expr::CallExpr,
    context::SessionData,
    job::{Job, JobData},
    liveness_analysis::{
        AccessFlags, BasicBlockId, LivenessData, OpOutputIdx,
        OperatorCallEffect,
    },
    operators::{
        errors::{OperatorApplicationError, OperatorCreationError},
        operator::{
            OffsetInChain, Operator, OperatorData, OperatorId,
            PreboundOutputsMap, TransformInstatiation,
        },
        transform::{
            DefaultTransformName, Transform, TransformData, TransformState,
        },
        utils::basic_transform_update::{
            basic_transform_update_claim_all, BasicUpdateData,
        },
    },
    options::chain_settings::SettingUseFloatingPointMath,
    record_data::{
        action_buffer::ActorId,
        field_data::{FieldData, FieldValueRepr},
        field_value_ref::FieldValueSlice,
        field_value_slice_iter::{FieldValueBlock, FieldValueRangeIter},
        group_track::GroupTrackIterRef,
        iter_hall::IterId,
        iters::FieldIterOpts,
        push_interface::PushInterface,
        ref_iter::RefAwareFieldValueRangeIter,
        varying_type_inserter::VaryingTypeInserter,
    },
    smallbox,
};

use metamatch::metamatch;
use scr_core::operators::utils::any_number::AnyNumber;

#[derive(Clone, Default)]
pub struct OpSum {}

pub struct TfSum {
    input_iter_id: IterId,
    group_track_iter: GroupTrackIterRef,
    aggregate: AnyNumber,
    current_group_error_type: Option<FieldValueRepr>,
    actor_id: ActorId,
    floating_point_math: bool,
    pending_field: bool,
}

impl Operator for OpSum {
    fn default_name(&self) -> scr_core::operators::operator::OperatorName {
        "sum".into()
    }

    fn output_count(&self, _sess: &SessionData, _op_id: OperatorId) -> usize {
        1
    }

    fn has_dynamic_outputs(
        &self,
        _sess: &SessionData,
        _op_id: OperatorId,
    ) -> bool {
        false
    }

    fn update_variable_liveness(
        &self,
        _sess: &SessionData,
        _ld: &mut LivenessData,
        _access_flags: &mut AccessFlags,
        _op_offset_after_last_write: OffsetInChain,
        _op_id: OperatorId,
        _bb_id: BasicBlockId,
        _input_field: OpOutputIdx,
        _outputs_offset: usize,
    ) -> Option<(OpOutputIdx, OperatorCallEffect)> {
        None
    }

    fn build_transforms<'a>(
        &'a self,
        job: &mut Job,
        tf_state: &mut TransformState,
        _op_id: OperatorId,
        _prebound_outputs: &PreboundOutputsMap,
    ) -> TransformInstatiation<'a> {
        let jd = &mut job.job_data;

        let floating_point_math = jd
            .get_setting_from_tf_state::<SettingUseFloatingPointMath>(
                tf_state,
            );

        let actor_id = jd.add_actor_for_tf_state(tf_state);
        let iter_id = jd.claim_iter_for_tf_state(tf_state);

        TransformInstatiation::Simple(TransformData::Custom(smallbox!(
            TfSum {
                group_track_iter: jd
                    .claim_group_track_iter_for_tf_state(tf_state),
                input_iter_id: iter_id,
                aggregate: AnyNumber::Int(0),
                actor_id,
                current_group_error_type: None,
                floating_point_math,
                pending_field: false
            }
        )))
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
                    format!("cannot sum over type `{}`", err_type.kind()),
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

        let mut group_iter = bud.group_tracker.lookup_group_track_iter_mut(
            self.group_track_iter.track_id,
            self.group_track_iter.iter_id,
            bud.match_set_mgr,
            self.actor_id,
        );

        if group_iter.is_invalid() {
            return (0, bud.ps.input_done);
        }

        let mut output_field = bud
            .field_mgr
            .borrow_field_dealiased_mut(bud.output_field_id);
        let mut inserter = output_field.iter_hall.varying_type_inserter();

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
                FieldIterOpts::default(),
            ) else {
                break;
            };

            let count = range.base.field_count;
            batch_size_rem -= count;
            group_iter.next_n_fields_in_group(count);
            metamatch!(match range.base.data {
                FieldValueSlice::Int(ints) => {
                    let mut iter =
                        FieldValueRangeIter::from_range(&range, ints);
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
                        RefAwareFieldValueRangeIter::from_range(&range, ints)
                    {
                        self.aggregate.add_big_int(v, rl, fpm)
                    }
                }
                FieldValueSlice::Float(floats) => {
                    for (v, rl) in
                        FieldValueRangeIter::from_range(&range, floats)
                    {
                        self.aggregate.add_float(*v, rl, fpm)
                    }
                }
                FieldValueSlice::BigRational(rationals) => {
                    for (v, rl) in RefAwareFieldValueRangeIter::from_range(
                        &range, rationals,
                    ) {
                        self.aggregate.add_rational(v, rl, fpm)
                    }
                }
                #[expand_pattern(REP in [
                    Null, Undefined,
                    BytesInline, TextInline, TextBuffer, BytesBuffer,
                    Array, Object, Argument, Macro, Custom,
                    StreamValueId, Error, FieldReference, SlicedFieldReference
                ])]
                FieldValueSlice::REP(_) => {
                    self.current_group_error_type =
                        Some(range.base.data.repr());
                }
            })
        }
        let pending_group_size =
            group_iter.field_pos() - last_finished_group_end;

        if pending_group_size > 0 {
            let drop_count =
                pending_group_size - usize::from(!self.pending_field);
            self.pending_field = true;
            group_iter.drop_backwards(drop_count);
        }
        group_iter.store_iter(self.group_track_iter.iter_id);

        (finished_group_count, bud.ps.input_done)
    }
}

impl Transform<'_> for TfSum {
    fn display_name(&self) -> DefaultTransformName {
        "sum".into()
    }

    fn update(
        &mut self,
        jd: &mut JobData,
        tf_id: scr_core::operators::transform::TransformId,
    ) {
        basic_transform_update_claim_all(
            jd,
            tf_id,
            [],
            self.input_iter_id,
            |bud| self.transform_update(bud),
        );
    }
}

pub fn parse_op_sum(
    expr: &CallExpr,
) -> Result<OperatorData, OperatorCreationError> {
    expr.reject_args()?;
    Ok(create_op_sum())
}

pub fn create_op_sum() -> OperatorData {
    OperatorData::Custom(smallbox!(OpSum {}))
}
