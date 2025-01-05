use thirtyfour::{error::WebDriverResult, WebElement};
use typeline_core::{
    cli::call_expr::CallExpr,
    context::SessionData,
    liveness_analysis::{
        BasicBlockId, LivenessData, OpOutputIdx, OperatorLivenessOutput,
    },
    operators::{
        errors::OperatorApplicationError,
        operator::{
            OffsetInChain, Operator, OperatorId, PreboundOutputsMap,
            TransformInstatiation,
        },
        transform::{Transform, TransformId, TransformState},
    },
    options::session_setup::SessionSetupData,
    record_data::{
        custom_data::CustomData,
        field_value::FieldReference,
        field_value_ref::FieldValueSlice,
        iter::{
            field_iterator::FieldIterOpts,
            field_value_slice_iter::FieldValueRangeIter,
        },
        iter_hall::FieldIterId,
        push_interface::PushInterface,
    },
    typeline_error::TypelineError,
};

use crate::selenium_data::{SeleniumWebElement, SeleniumWindow};

pub fn parse_op_sel_xpath(
    sess: &mut SessionSetupData,
    expr: CallExpr,
) -> Result<Box<dyn Operator>, TypelineError> {
    let xpath = expr
        .require_single_string_arg_autoconvert(sess)?
        .to_string();
    Ok(Box::new(OpSelXpath { xpath }))
}

struct OpSelXpath {
    xpath: String,
}

struct TfSelXpath<'a> {
    xpath: &'a str,
    input_iter: FieldIterId,
    input_field_ref: FieldReference,
}

impl Operator for OpSelXpath {
    fn default_name(
        &self,
    ) -> typeline_core::operators::operator::OperatorName {
        "sel_xpath".into()
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
        ld: &mut LivenessData,
        _op_offset_after_last_write: OffsetInChain,
        _op_id: OperatorId,
        _bb_id: BasicBlockId,
        input_field: OpOutputIdx,
        output: &mut OperatorLivenessOutput,
    ) {
        ld.op_outputs[output.primary_output]
            .field_references
            .push(input_field);
        output.flags.may_dup_or_drop = false;
    }

    fn build_transforms<'a>(
        &'a self,
        job: &mut typeline_core::job::Job<'a>,
        tf_state: &mut TransformState,
        _op_id: OperatorId,
        _prebound_outputs: &PreboundOutputsMap,
    ) -> TransformInstatiation<'a> {
        TransformInstatiation::Single(Box::new(TfSelXpath {
            input_iter: job
                .job_data
                .claim_iter_ref_for_tf_state(tf_state)
                .iter_id,
            xpath: &self.xpath,
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

impl<'a> Transform<'a> for TfSelXpath<'a> {
    fn update(
        &mut self,
        jd: &mut typeline_core::job::JobData<'_>,
        tf_id: TransformId,
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
        while let Some(range) = input_iter.typed_range_fwd(
            &jd.match_set_mgr,
            bs_rem,
            FieldIterOpts::default(),
        ) {
            bs_rem -= range.base.field_count;
            match range.base.data {
                FieldValueSlice::Custom(data) => {
                    for (c, rl) in
                        FieldValueRangeIter::from_range(&range, data)
                    {
                        match find_all_by_xpath(self.xpath, &**c) {
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

fn find_all_by_xpath(
    xpath: &str,
    c: &dyn CustomData,
) -> Result<Vec<WebElement>, String> {
    if let Some(window) = c.downcast_ref::<SeleniumWindow>() {
        let win = &window.window_handle;
        let inst = &mut *window.instance.lock().unwrap();
        let wad = &mut inst.window_aware_driver;
        let res: WebDriverResult<Vec<WebElement>> =
            inst.runtime.block_on(async {
                wad.switch_to_window(win).await?;
                wad.find_all(thirtyfour::By::XPath(xpath)).await
            });
        return res.map_err(|e| format!("failed to navigate: {e}"));
    }

    if let Some(elem) = c.downcast_ref::<SeleniumWebElement>() {
        let win = &elem.window_handle;
        let inst = &mut *elem.instance.lock().unwrap();
        let wad = &mut inst.window_aware_driver;
        let src_elem = WebElement {
            element_id: elem.element_id.clone(),
            handle: wad.driver.handle.clone(),
        };
        let res: WebDriverResult<Vec<WebElement>> =
            inst.runtime.block_on(async {
                wad.switch_to_window(win).await?;
                src_elem.find_all(thirtyfour::By::XPath(xpath)).await
            });
        return res.map_err(|e| format!("failed to navigate: {e}"));
    }

    Err(format!(
        "sel_xpath expected selenium element, found {}",
        c.type_name()
    ))
}
