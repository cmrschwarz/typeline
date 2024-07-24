use scr_core::{
    context::SessionData,
    job::{Job, JobData},
    liveness_analysis::{
        AccessFlags, BasicBlockId, LivenessData, OpOutputIdx,
        OperatorCallEffect,
    },
    operators::{
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
        action_buffer::{ActorId, ActorRef},
        array::Array,
        dyn_ref_iter::{DynFieldValueBlock, RefAwareDynFieldValueRangeIter},
        field_data::{FieldData, FieldValueRepr},
        field_value_ref::FieldValueSlice,
        field_value_slice_iter::{FieldValueBlock, FieldValueRangeIter},
        group_track::GroupTrackIterRef,
        iter_hall::{IterId, IterKind},
        iters::FieldIterOpts,
        push_interface::PushInterface,
        varying_type_inserter::VaryingTypeInserter,
    },
    smallbox,
};

#[derive(Clone, Default)]
pub struct OpCollect {}

pub struct TfCollect {
    input_iter_id: IterId,
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

    fn update_variable_liveness(
        &self,
        _sess: &SessionData,
        _ld: &mut LivenessData,
        _access_flags: &mut AccessFlags,
        _op_offset_after_last_write: OffsetInChain,
        _op_id: OperatorId,
        _bb_id: BasicBlockId,
        _input_field: OpOutputIdx,
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

        let ms = &mut jd.match_set_mgr.match_sets[tf_state.match_set_id];
        let mut ab = ms.action_buffer.borrow_mut();
        let actor_id = ab.add_actor();
        jd.field_mgr.fields[tf_state.output_field]
            .borrow_mut()
            .first_actor
            .set(ActorRef::Unconfirmed(ab.peek_next_actor_id()));

        let iter_kind =
            IterKind::Transform(jd.tf_mgr.transforms.peek_claim_id());
        TransformInstatiation::Simple(TransformData::Custom(smallbox!(
            TfCollect {
                group_track_iter: jd
                    .group_track_manager
                    .claim_group_track_iter_ref(
                        tf_state.input_group_track_id,
                        iter_kind
                    ),
                input_iter_id: jd
                    .field_mgr
                    .claim_iter_non_cow(tf_state.input_field, iter_kind),
                aggregate: Array::default(),
                actor_id,
                floating_point_math,
                pending_field: false
            }
        )))
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
    fn display_name(&self) -> DefaultTransformName {
        "collect".into()
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

pub fn create_op_collect() -> OperatorData {
    OperatorData::Custom(smallbox!(OpCollect {}))
}
