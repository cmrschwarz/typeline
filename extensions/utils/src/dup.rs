use scr_core::{
    cli::call_expr::CallExpr,
    context::SessionData,
    job::{Job, JobData},
    liveness_analysis::{
        AccessFlags, BasicBlockId, LivenessData, OpOutputIdx,
        OperatorCallEffect,
    },
    operators::{
        errors::OperatorCreationError,
        operator::{
            OffsetInChain, Operator, OperatorData, OperatorId,
            PreboundOutputsMap, TransformInstatiation,
        },
        transform::{
            DefaultTransformName, Transform, TransformData, TransformId,
            TransformState,
        },
    },
    record_data::{
        action_buffer::ActorId, field_action::FieldActionKind,
        group_track::GroupTrackIterRef, iter_hall::IterKind,
    },
    smallbox,
};

#[derive(Default)]
pub struct OpDup {
    count: usize,
}

pub struct TfDup {
    count: usize,
    actor_id: ActorId,
    // we neeed *some* iterator to keep track of our position, and the group
    // iterator is more likely to be simple
    record_group_track_iter: GroupTrackIterRef,
}

impl Operator for OpDup {
    fn default_name(&self) -> scr_core::operators::operator::OperatorName {
        "dup".into()
    }

    fn output_count(&self, _sess: &SessionData, _op_id: OperatorId) -> usize {
        0
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
        access_flags: &mut AccessFlags,
        _op_offset_after_last_write: OffsetInChain,
        _op_id: OperatorId,
        _bb_id: BasicBlockId,
        _input_field: OpOutputIdx,
    ) -> Option<(OpOutputIdx, OperatorCallEffect)> {
        access_flags.input_accessed = false;
        None
    }

    fn build_transforms(
        &self,
        job: &mut Job,
        tf_state: &mut TransformState,
        _op_id: OperatorId,
        _prebound_outputs: &PreboundOutputsMap,
    ) -> TransformInstatiation {
        let actor_id = job.job_data.match_set_mgr.match_sets
            [tf_state.match_set_id]
            .action_buffer
            .borrow_mut()
            .add_actor();
        job.job_data.field_mgr.drop_field_refcount(
            tf_state.output_field,
            &mut job.job_data.match_set_mgr,
        );
        tf_state.output_field = tf_state.input_field;
        let record_group_track_iter =
            job.job_data.group_track_manager.claim_group_track_iter_ref(
                tf_state.input_group_track_id,
                IterKind::Transform(
                    job.job_data.tf_mgr.transforms.peek_claim_id(),
                ),
            );
        TransformInstatiation::Simple(TransformData::Custom(smallbox!(
            TfDup {
                count: self.count,
                actor_id,
                record_group_track_iter
            }
        )))
    }
}

impl Transform<'_> for TfDup {
    fn display_name(&self) -> DefaultTransformName {
        if self.count == 0 { "drop" } else { "dup" }.into()
    }

    fn update(&mut self, jd: &mut JobData, tf_id: TransformId) {
        let (batch_size, ps) = jd.tf_mgr.claim_batch(tf_id);
        let mut iter = jd.group_track_manager.lookup_group_track_iter(
            self.record_group_track_iter,
            &jd.match_set_mgr,
        );
        let field_pos_start = iter.field_pos();
        let mut field_pos = field_pos_start;
        iter.next_n_fields(batch_size);
        jd.group_track_manager.store_record_group_track_iter(
            self.record_group_track_iter,
            &iter,
        );
        let tf = &jd.tf_mgr.transforms[tf_id];

        if ps.successor_done {
            jd.tf_mgr.help_out_with_output_done(
                &mut jd.match_set_mgr,
                tf_id,
                self.actor_id,
                batch_size,
            );
            return;
        }
        if self.count == 1 {
            jd.tf_mgr.submit_batch(tf_id, batch_size, ps.input_done);
            return;
        }

        let mut ab = jd.match_set_mgr.match_sets[tf.match_set_id]
            .action_buffer
            .borrow_mut();
        ab.begin_action_group(self.actor_id);
        if self.count == 0 {
            ab.push_action(FieldActionKind::Drop, field_pos, batch_size);
        } else {
            for _ in 0..batch_size {
                ab.push_action(
                    FieldActionKind::Dup,
                    field_pos,
                    self.count - 1,
                );
                field_pos += self.count;
            }
        }
        ab.end_action_group();
        jd.tf_mgr.submit_batch(
            tf_id,
            field_pos - field_pos_start,
            ps.input_done,
        );
    }
    fn get_out_fields(
        &self,
        _tf_state: &TransformState,
        _fields: &mut Vec<scr_core::record_data::field::FieldId>,
    ) {
        // no output fields
    }
}

pub fn create_op_dup(count: usize) -> OperatorData {
    OperatorData::Custom(smallbox!(OpDup { count }))
}

pub fn parse_op_dup(
    expr: &CallExpr,
) -> Result<OperatorData, OperatorCreationError> {
    let count = expr.require_at_most_one_number_arg(false)?.unwrap_or(2);
    Ok(create_op_dup(count))
}

pub fn parse_op_drop(
    expr: &CallExpr,
) -> Result<OperatorData, OperatorCreationError> {
    expr.reject_args()?;
    Ok(create_op_dup(0))
}
