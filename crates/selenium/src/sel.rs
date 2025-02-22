use typeline_core::{
    cli::call_expr::{CallExpr, ParsedArgValue},
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
    use_profile: bool,
}

struct TfSel<'a> {
    op: &'a OpSel,
    instance_created: bool,
    iter_ref: FieldIterRef,
}

pub fn parse_op_sel(
    _sess: &mut SessionSetupData,
    expr: CallExpr,
) -> Result<Box<dyn Operator>, TypelineError> {
    let mut use_profile = false;
    let mut initial_url = None;
    for arg in expr.parsed_args_iter_with_bounded_positionals(0, 1) {
        let arg = arg?;
        match arg.value {
            ParsedArgValue::Flag(flag) => {
                if flag == "-p" || flag == "--profile" {
                    use_profile = true;
                    continue;
                }
                return Err(expr
                    .error_flag_unsupported(flag, arg.span)
                    .into());
            }
            ParsedArgValue::NamedArg { key, value: _ } => {
                return Err(expr
                    .error_named_arg_unsupported(key, arg.span)
                    .into());
            }
            ParsedArgValue::PositionalArg { value: v, .. } => {
                // TODO: autoconvert ints etc. get rid of this mess
                // in favor of a decent arg parsing generatlizationj
                let Some(url) = v.as_maybe_text_ref().and_then(|t| t.as_str())
                else {
                    return Err(expr
                        .error_positional_arg_not_plaintext(arg.span)
                        .into());
                };
                initial_url = Some(url.to_string());
            }
        }
    }

    Ok(Box::new(OpSel {
        initial_url,
        use_profile,
    }))
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
            op: self,
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
            match SeleniumWindow::new(
                self.op.initial_url.as_deref(),
                self.op.use_profile,
            ) {
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
