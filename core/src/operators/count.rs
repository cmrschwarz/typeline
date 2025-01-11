use crate::{
    cli::call_expr::CallExpr,
    job::JobData,
    liveness_analysis::OperatorLivenessOutput,
    record_data::{action_buffer::ActorId, group_track::GroupTrackIterRef},
};

use super::{
    errors::OperatorCreationError,
    operator::{Operator, TransformInstatiation},
    transform::{Transform, TransformId, TransformState},
};

#[derive(Clone)]
pub struct OpCount {}
pub struct TfCount {
    count: i64,
    actor: ActorId,
    iter: GroupTrackIterRef,
}

impl Operator for OpCount {
    fn default_name(&self) -> super::operator::OperatorName {
        "count".into()
    }

    fn output_count(
        &self,
        _sess: &crate::context::SessionData,
        _op_id: super::operator::OperatorId,
    ) -> usize {
        1
    }

    fn has_dynamic_outputs(
        &self,
        _sess: &crate::context::SessionData,
        _op_id: super::operator::OperatorId,
    ) -> bool {
        false
    }

    fn build_transforms<'a>(
        &'a self,
        job: &mut crate::job::Job<'a>,
        tf_state: &mut TransformState,
        _op_id: super::operator::OperatorId,
        _prebound_outputs: &super::operator::PreboundOutputsMap,
    ) -> TransformInstatiation<'a> {
        TransformInstatiation::Single(Box::new(TfCount {
            count: 0,
            actor: job.job_data.add_actor_for_tf_state(tf_state),
            iter: job.job_data.claim_group_track_iter_for_tf_state(tf_state),
        }))
    }

    fn update_variable_liveness(
        &self,
        _sess: &crate::context::SessionData,
        _ld: &mut crate::liveness_analysis::LivenessData,
        _op_offset_after_last_write: super::operator::OffsetInChain,
        _op_id: super::operator::OperatorId,
        _bb_id: crate::liveness_analysis::BasicBlockId,
        _input_field: crate::liveness_analysis::OpOutputIdx,
        output: &mut OperatorLivenessOutput,
    ) {
        output.flags.input_accessed = false;
        output.flags.non_stringified_input_access = false;
    }

    fn setup(
        &mut self,
        sess: &mut crate::options::session_setup::SessionSetupData,
        op_data_id: super::operator::OperatorDataId,
        chain_id: crate::chain::ChainId,
        offset_in_chain: super::operator::OperatorOffsetInChain,
        span: crate::cli::call_expr::Span,
    ) -> Result<
        super::operator::OperatorId,
        crate::typeline_error::TypelineError,
    > {
        Ok(sess.add_op(op_data_id, chain_id, offset_in_chain, span))
    }

    fn on_liveness_computed(
        &mut self,
        _sess: &mut crate::context::SessionData,
        _ld: &crate::liveness_analysis::LivenessData,
        _op_id: super::operator::OperatorId,
    ) {
    }
}

impl Transform<'_> for TfCount {
    fn update(&mut self, jd: &mut JobData, tf_id: TransformId) {
        // TODO: propagate errors
        let (batch_size, ps) = jd.tf_mgr.claim_batch(tf_id);

        let mut iter =
            jd.group_track_manager.lookup_group_track_iter_mut_from_ref(
                self.iter,
                &jd.match_set_mgr,
                self.actor,
            );

        let mut groups_emitted = 0;

        if iter.is_invalid() {
            jd.tf_mgr.submit_batch(
                tf_id,
                groups_emitted,
                ps.group_to_truncate,
                ps.input_done,
            );
            return;
        }

        let output_field_id = jd.tf_mgr.transforms[tf_id].output_field;
        let mut output_field =
            jd.field_mgr.fields[output_field_id].borrow_mut();
        let mut inserter =
            output_field.iter_hall.fixed_size_type_inserter::<i64>();

        let mut batch_size_rem = batch_size;
        let mut count = self.count;
        loop {
            let gl_rem = iter.group_len_rem();
            let consumed = iter.next_n_fields(gl_rem.min(batch_size_rem));
            iter.drop_backwards(
                consumed.saturating_sub(usize::from(count == 0)),
            );
            count += consumed as i64;
            batch_size_rem -= consumed;

            if !iter.is_end_of_group(ps.input_done) {
                break;
            }
            inserter.push(count);
            groups_emitted += 1;
            count = 0;
            if !iter.try_next_group() {
                break;
            }
            let zero_count = iter.skip_empty_groups();
            if zero_count > 0 {
                inserter.push_with_rl(0, zero_count);
                groups_emitted += zero_count;
                if !iter.is_end_of_group(ps.input_done) {
                    break;
                }
            }
        }
        self.count = count;
        iter.store_iter(self.iter.iter_id);
        jd.tf_mgr
            .submit_batch_ready_for_more(tf_id, groups_emitted, ps);
    }
}

pub fn parse_op_count(
    expr: &CallExpr,
) -> Result<Box<dyn Operator>, OperatorCreationError> {
    expr.reject_args()?;
    Ok(Box::new(OpCount {}))
}

pub fn create_op_count() -> Box<dyn Operator> {
    Box::new(OpCount {})
}
