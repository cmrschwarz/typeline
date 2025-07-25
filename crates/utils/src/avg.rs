use typeline_core::{
    cli::call_expr::CallExpr,
    context::SessionData,
    job::{Job, JobData},
    operators::{
        errors::{OperatorApplicationError, OperatorCreationError},
        operator::{
            Operator, OperatorId, PreboundOutputsMap, TransformInstatiation,
        },
        transform::{Transform, TransformState},
        utils::basic_transform_update::{
            basic_transform_update_claim_all, BasicUpdateData,
        },
    },
    options::chain_settings::SettingUseFloatingPointMath,
    record_data::{
        action_buffer::ActorId,
        field_data::{FieldData, FieldValueRepr},
        field_value_ref::{FieldValueBlock, FieldValueSlice},
        group_track::GroupTrackIterRef,
        iter::{
            field_value_slice_iter::FieldValueRangeIter,
            ref_iter::RefAwareFieldValueRangeIter,
        },
        iter_hall::FieldIterId,
        push_interface::PushInterface,
        varying_type_inserter::VaryingTypeInserter,
    },
};

use metamatch::metamatch;
use typeline_core::operators::utils::any_number::AnyNumber;

#[derive(Clone, Default)]
pub struct OpAvg {}

pub struct TfAvg {
    input_iter_id: FieldIterId,
    group_track_iter: GroupTrackIterRef,
    aggregate: AnyNumber,
    count: usize,
    current_group_error_type: Option<FieldValueRepr>,
    actor_id: ActorId,
    floating_point_math: bool,
    pending_field: bool,
}

impl Operator for OpAvg {
    fn default_name(
        &self,
    ) -> typeline_core::operators::operator::OperatorName {
        "avg".into()
    }

    fn output_count(&self, _sess: &SessionData, _op_id: OperatorId) -> usize {
        1
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

        TransformInstatiation::Single(Box::new(TfAvg {
            group_track_iter: jd.claim_group_track_iter_for_tf_state(tf_state),
            input_iter_id: iter_id,
            aggregate: AnyNumber::Int(0),
            count: 0,
            actor_id,
            current_group_error_type: None,
            floating_point_math,
            pending_field: false,
        }))
    }
}

impl TfAvg {
    fn finish_group(
        &mut self,
        op_id: OperatorId,
        inserter: &mut VaryingTypeInserter<&mut FieldData>,
    ) {
        let count = self.count;
        self.count = 0;

        let mut result = std::mem::take(&mut self.aggregate);

        if let Some(err_type) = self.current_group_error_type.take() {
            inserter.push_error(
                OperatorApplicationError::new_s(
                    format!("cannot average over type `{}`", err_type.kind()),
                    op_id,
                ),
                1,
                false,
                false,
            );
            return;
        }
        if count == 0 {
            inserter.push_int(0, 1, true, true);
            return;
        }
        result.div_usize(count);
        result.push(inserter, true, true);
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
            ) else {
                break;
            };

            let count = range.base.field_count;
            batch_size_rem -= count;
            group_iter.next_n_fields_in_group(count);

            self.count += count;

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
                FieldValueSlice::Bool(bools) => {
                    for (v, rl) in
                        FieldValueRangeIter::from_range(&range, bools)
                    {
                        self.aggregate.add_bool_with_rl(*v, rl, fpm)
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
                    Array, Object, Argument, OpDecl, Custom,
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

impl Transform<'_> for TfAvg {
    fn update(
        &mut self,
        jd: &mut JobData,
        tf_id: typeline_core::operators::transform::TransformId,
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

pub fn parse_op_avg(
    expr: &CallExpr,
) -> Result<Box<dyn Operator>, OperatorCreationError> {
    expr.reject_args()?;
    Ok(create_op_avg())
}

pub fn create_op_avg() -> Box<dyn Operator> {
    Box::new(OpAvg {})
}
