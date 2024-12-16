use scr_core::{
    cli::call_expr::CallExpr,
    context::SessionData,
    job::{Job, JobData},
    operators::{
        errors::OperatorCreationError,
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
        array::Array,
        field_data::{FieldData, FieldValueRepr},
        field_value_ref::{
            DynFieldValueBlock, FieldValueBlock, FieldValueSlice,
        },
        group_track::GroupTrackIterRef,
        iter::{
            dyn_ref_iter::RefAwareDynFieldValueRangeIter,
            field_iterator::FieldIterOpts,
            field_value_slice_iter::FieldValueRangeIter,
        },
        iter_hall::FieldIterId,
        push_interface::PushInterface,
        varying_type_inserter::VaryingTypeInserter,
    },
};

#[derive(Clone, Default)]
pub struct OpCollect {}

pub struct TfCollect {
    input_iter_id: FieldIterId,
    group_track_iter: GroupTrackIterRef,
    aggregate: Array,
    actor_id: ActorId,
    floating_point_math: bool,
    pending_field: bool,
}

impl Operator for OpCollect {
    fn default_name(&self) -> scr_core::operators::operator::OperatorName {
        "collect".into()
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
        let input_iter_id = jd.claim_iter_for_tf_state(tf_state);
        let group_track_iter =
            jd.claim_group_track_iter_for_tf_state(tf_state);

        TransformInstatiation::Single(Box::new(TfCollect {
            group_track_iter,
            input_iter_id,
            aggregate: Array::default(),
            actor_id,
            floating_point_math,
            pending_field: false,
        }))
    }
}

impl TfCollect {
    fn finish_group(
        &mut self,
        inserter: &mut VaryingTypeInserter<&mut FieldData>,
    ) {
        let result = std::mem::take(&mut self.aggregate);
        inserter.push_array(result, 1, true, false);
    }
    fn transform_update(&mut self, bud: BasicUpdateData) -> (usize, bool) {
        let _op_id = bud.tf_mgr.transforms[bud.tf_id].op_id.unwrap();
        let _fpm = self.floating_point_math;

        let mut output_field = bud
            .field_mgr
            .borrow_field_dealiased_mut(bud.output_field_id);
        let mut inserter = output_field.iter_hall.varying_type_inserter();

        let mut group_iter = bud.group_tracker.lookup_group_track_iter_mut(
            self.group_track_iter.track_id,
            self.group_track_iter.iter_id,
            bud.match_set_mgr,
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
                    self.finish_group(&mut inserter);
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
            match range.base.data {
                FieldValueSlice::Int(ints) => {
                    let mut iter =
                        FieldValueRangeIter::from_range(&range, ints);
                    while let Some(b) = iter.next_block() {
                        match b {
                            FieldValueBlock::Plain(v) => {
                                self.aggregate.extend(v.iter().copied());
                            }
                            FieldValueBlock::WithRunLength(v, rl) => {
                                self.aggregate.extend(
                                    std::iter::repeat(*v).take(rl as usize),
                                )
                            }
                        }
                    }
                }
                _ => {
                    // PERF: `extend_from_field_value` is not the most
                    // efficient way to do this, should use metamatch instead
                    let mut iter = RefAwareDynFieldValueRangeIter::new(range);
                    while let Some(b) = iter.next_block() {
                        match b {
                            DynFieldValueBlock::Plain(v) => {
                                self.aggregate.extend_from_field_value(
                                    v.into_iter()
                                        .map(|fvr| fvr.to_field_value()),
                                );
                            }
                            DynFieldValueBlock::WithRunLength(v, rl) => {
                                self.aggregate.extend_from_field_value(
                                    std::iter::repeat(v.to_field_value())
                                        .take(rl as usize),
                                )
                            }
                        }
                    }
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
        group_iter.store_iter(self.group_track_iter.iter_id);

        (finished_group_count, bud.ps.input_done)
    }
}

impl Transform<'_> for TfCollect {
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

pub fn create_op_collect() -> Box<dyn Operator> {
    Box::new(OpCollect {})
}

pub fn parse_op_collect(
    expr: &CallExpr,
) -> Result<Box<dyn Operator>, OperatorCreationError> {
    expr.reject_args()?;
    Ok(create_op_collect())
}
