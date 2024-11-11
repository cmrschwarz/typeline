use crate::record_data::{action_buffer::ActorId, iter_hall::FieldIterId};

use super::{
    operator::{Operator, TransformInstatiation},
    transform::{Transform, TransformData},
};

pub struct OpLines {}
pub struct TfLines {
    iter: FieldIterId,
    actor: ActorId,
    _line_offset: usize,
}

impl Operator for OpLines {
    fn default_name(&self) -> super::operator::OperatorName {
        "lines".into()
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
        tf_state: &mut super::transform::TransformState,
        _op_id: super::operator::OperatorId,
        _prebound_outputs: &super::operator::PreboundOutputsMap,
    ) -> TransformInstatiation<'a> {
        TransformInstatiation::Single(TransformData::from_custom(TfLines {
            iter: job.job_data.claim_iter_for_tf_state(tf_state),
            actor: job.job_data.add_actor_for_tf_state(tf_state),
            _line_offset: 0,
        }))
    }
}

impl Transform<'_> for TfLines {
    fn update(
        &mut self,
        jd: &mut crate::job::JobData<'_>,
        tf_id: super::transform::TransformId,
    ) {
        let tf = &jd.tf_mgr.transforms[tf_id];
        let field_id = tf.input_field;
        let ms_id = tf.match_set_id;

        let input_field =
            jd.field_mgr.get_cow_field_ref(&jd.match_set_mgr, field_id);

        let mut _iter = jd.field_mgr.get_auto_deref_iter(
            field_id,
            &input_field,
            self.iter,
        );

        let mut ab = jd.match_set_mgr.match_sets[ms_id]
            .action_buffer
            .borrow_mut();
        ab.begin_action_group(self.actor);
    }
}
