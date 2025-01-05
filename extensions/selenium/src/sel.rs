use typeline_core::{
    cli::call_expr::CallExpr,
    operators::{
        errors::OperatorApplicationError,
        operator::{Operator, TransformInstatiation},
        transform::Transform,
        utils::maintain_single_value::maintain_single_value,
    },
    options::session_setup::SessionSetupData,
    record_data::{field::FieldIterRef, push_interface::PushInterface},
    typeline_error::TypelineError,
};

use crate::selenium_data::SeleniumWindow;

struct OpSel {
    initial_url: Option<String>,
}

struct TfSel<'a> {
    instance_created: bool,
    iter_ref: FieldIterRef,
    initial_url: Option<&'a str>,
}

pub fn parse_op_sel(
    sess: &mut SessionSetupData,
    expr: CallExpr,
) -> Result<Box<dyn Operator>, TypelineError> {
    expr.require_at_most_one_arg()?;
    let initial_url = expr
        .args
        .get_mut(0)
        .map(|arg| std::mem::take(arg).try_into_str("url", sess))
        .transpose()?;
    Ok(Box::new(OpSel { initial_url }))
}

impl Operator for OpSel {
    fn default_name(
        &self,
    ) -> typeline_core::operators::operator::OperatorName {
        "sel".into()
    }

    fn output_count(
        &self,
        _sess: &typeline_core::context::SessionData,
        _op_id: typeline_core::operators::operator::OperatorId,
    ) -> usize {
        1
    }

    fn input_field_kind(
        &self,
    ) -> typeline_core::operators::operator::InputFieldKind {
        typeline_core::operators::operator::InputFieldKind::Dummy
    }

    fn has_dynamic_outputs(
        &self,
        _sess: &typeline_core::context::SessionData,
        _op_id: typeline_core::operators::operator::OperatorId,
    ) -> bool {
        false
    }

    fn build_transforms<'a>(
        &'a self,
        job: &mut typeline_core::job::Job<'a>,
        tf_state: &mut typeline_core::operators::transform::TransformState,
        _op_id: typeline_core::operators::operator::OperatorId,
        _prebound_outputs: &typeline_core::operators::operator::PreboundOutputsMap,
    ) -> TransformInstatiation<'a> {
        TransformInstatiation::Single(Box::new(TfSel {
            instance_created: false,
            iter_ref: job.job_data.claim_iter_ref_for_tf_state(tf_state),
            initial_url: self.initial_url.as_deref(),
        }))
    }
}

impl<'a> Transform<'a> for TfSel<'a> {
    fn update(
        &mut self,
        jd: &mut typeline_core::job::JobData<'_>,
        tf_id: typeline_core::operators::transform::TransformId,
    ) {
        let tf = &jd.tf_mgr.transforms[tf_id];
        if !self.instance_created {
            let output_field_id = tf.output_field;
            let op_id = tf.op_id.unwrap();
            if tf.available_batch_size == 0 {
                let (bs, ps) = jd.tf_mgr.claim_all(tf_id);
                jd.tf_mgr.submit_batch_ready_for_more(tf_id, bs, ps);
                return;
            }
            jd.tf_mgr.prepare_output_field(
                &mut jd.field_mgr,
                &mut jd.match_set_mgr,
                tf_id,
            );
            let mut field = jd.field_mgr.fields[output_field_id].borrow_mut();
            match SeleniumWindow::new(self.initial_url) {
                Ok(win) => {
                    field.iter_hall.push_custom(Box::new(win), 1, false, false)
                }
                Err(message) => field.iter_hall.push_error(
                    OperatorApplicationError::new_s(message, op_id),
                    1,
                    false,
                    false,
                ),
            }
        }
        let (bs, ps) = maintain_single_value(jd, tf_id, None, self.iter_ref);
        jd.tf_mgr.submit_batch_ready_for_more(tf_id, bs, ps);
    }
}
