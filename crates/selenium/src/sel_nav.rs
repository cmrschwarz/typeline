use thirtyfour::error::WebDriverResult;
use typeline_core::{
    cli::call_expr::CallExpr,
    operators::{
        errors::OperatorApplicationError,
        operator::{Operator, TransformInstatiation},
        transform::Transform,
    },
    options::session_setup::SessionSetupData,
    record_data::{
        custom_data::CustomData, field_value::FieldReference,
        field_value_ref::FieldValueSlice,
        iter::field_value_slice_iter::FieldValueRangeIter,
        iter_hall::FieldIterId, push_interface::PushInterface,
    },
    typeline_error::TypelineError,
};

use crate::selenium_data::SeleniumWindow;

pub fn parse_op_sel_nav(
    sess: &mut SessionSetupData,
    expr: CallExpr,
) -> Result<Box<dyn Operator>, TypelineError> {
    let target = expr.require_single_string_arg_autoconvert(sess)?;
    Ok(Box::new(OpSelNav {
        target: target.to_string(),
    }))
}

struct OpSelNav {
    target: String,
}

struct TfSelNav<'a> {
    tgt: &'a str,
    input_iter: FieldIterId,
    input_field_ref: FieldReference,
}

impl Operator for OpSelNav {
    fn default_name(
        &self,
    ) -> typeline_core::operators::operator::OperatorName {
        "sel_nav".into()
    }

    fn update_variable_liveness(
        &self,
        _sess: &typeline_core::context::SessionData,
        ld: &mut typeline_core::liveness_analysis::LivenessData,
        _op_offset_after_last_write: typeline_core::operators::operator::OffsetInChain,
        _op_id: typeline_core::operators::operator::OperatorId,
        _bb_id: typeline_core::liveness_analysis::BasicBlockId,
        input_field: typeline_core::liveness_analysis::OpOutputIdx,
        output: &mut typeline_core::liveness_analysis::OperatorLivenessOutput,
    ) {
        ld.op_outputs[output.primary_output]
            .field_references
            .push(input_field);
        output.flags.may_dup_or_drop = false;
    }

    fn build_transforms<'a>(
        &'a self,
        job: &mut typeline_core::job::Job<'a>,
        tf_state: &mut typeline_core::operators::transform::TransformState,
        _op_id: typeline_core::operators::operator::OperatorId,
        _prebound_outputs: &typeline_core::operators::operator::PreboundOutputsMap,
    ) -> TransformInstatiation<'a> {
        TransformInstatiation::Single(Box::new(TfSelNav {
            input_iter: job
                .job_data
                .claim_iter_ref_for_tf_state(tf_state)
                .iter_id,
            tgt: &self.target,
            input_field_ref: job
                .job_data
                .field_mgr
                .register_field_reference(
                    tf_state.output_field,
                    tf_state.input_field,
                )
                .into(),
        }))
    }
}

impl<'a> Transform<'a> for TfSelNav<'a> {
    fn update(
        &mut self,
        jd: &mut typeline_core::job::JobData<'_>,
        tf_id: typeline_core::operators::transform::TransformId,
    ) {
        jd.tf_mgr.prepare_output_field(
            &mut jd.field_mgr,
            &mut jd.match_set_mgr,
            tf_id,
        );
        let tf = &jd.tf_mgr.transforms[tf_id];
        let op_id = tf.op_id.unwrap();
        let mut input_iter = jd.field_mgr.lookup_auto_deref_iter(
            &jd.match_set_mgr,
            tf.input_field,
            self.input_iter,
        );
        let mut output = jd.field_mgr.fields[tf.output_field].borrow_mut();
        let (bs, ps) = jd.tf_mgr.claim_batch(tf_id);
        let mut bs_rem = bs;
        while let Some(range) =
            input_iter.typed_range_fwd(&jd.match_set_mgr, bs_rem)
        {
            bs_rem -= range.base.field_count;
            match range.base.data {
                FieldValueSlice::Custom(data) => {
                    for (c, rl) in
                        FieldValueRangeIter::from_range(&range, data)
                    {
                        match navigate(self.tgt, &**c) {
                            Ok(_) => output.iter_hall.push_field_reference(
                                self.input_field_ref,
                                rl as usize,
                                true,
                                true,
                            ),
                            Err(e) => output.iter_hall.push_error(
                                OperatorApplicationError::new_s(
                                    format!("failed to navigate: {e}"),
                                    op_id,
                                ),
                                range.base.field_count,
                                true,
                                true,
                            ),
                        }
                    }
                }
                FieldValueSlice::Error(_) => {
                    output.iter_hall.push_field_reference(
                        self.input_field_ref,
                        range.base.field_count,
                        true,
                        true,
                    )
                }
                other => {
                    output.iter_hall.push_error(
                        OperatorApplicationError::new_s(
                            format!(
                                "sel_nav expected selenium window, found {}",
                                other.kind()
                            ),
                            op_id,
                        ),
                        range.base.field_count,
                        true,
                        true,
                    );
                }
            }
        }

        jd.tf_mgr.submit_batch_ready_for_more(tf_id, bs, ps);
    }
}

fn navigate(tgt: &str, c: &dyn CustomData) -> Result<(), String> {
    let Some(window) = c.downcast_ref::<SeleniumWindow>() else {
        return Err(format!(
            "sel_nav expected selenium window, found {}",
            c.type_name()
        ));
    };
    let win = &window.window_handle;
    let inst = &mut *window.instance.lock().unwrap();
    let wad = &mut inst.window_aware_driver;
    let res: WebDriverResult<()> = inst.runtime.block_on(async {
        wad.switch_to_window(win).await?;
        wad.driver.goto(tgt).await?;
        Ok(())
    });
    res.map_err(|e| format!("failed to navigate: {e}"))
}
