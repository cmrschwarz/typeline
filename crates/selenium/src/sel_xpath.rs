use std::sync::{Arc, LazyLock, Mutex};

use regex::Regex;
use thirtyfour::{
    error::WebDriverResult, extensions::query::ElementQuerySource, ElementId,
    WebElement, WindowHandle,
};
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
        action_buffer::ActorId,
        custom_data::CustomData,
        field_action::FieldActionKind,
        field_value::FieldReference,
        field_value_ref::FieldValueSlice,
        iter::{
            field_value_slice_iter::FieldValueRangeIter,
            iter_adapters::UnfoldIterRunLength,
        },
        iter_hall::FieldIterId,
        push_interface::PushInterface,
    },
    typeline_error::TypelineError,
    utils::maybe_text::MaybeText,
};

use crate::selenium_data::{
    SeleniumInstance, SeleniumWebElement, SeleniumWindow,
};

struct XPathMatchResult {
    instance: Arc<Mutex<SeleniumInstance>>,
    window_handle: WindowHandle,
    elems: Vec<WebElemMatch>,
}

enum XPathFinalComponent {
    Attribute(String),
    Text,
}

struct UserXPath {
    main: thirtyfour::By,
    final_component: Option<XPathFinalComponent>,
}

#[derive(Clone)]
pub enum WebElemMatch {
    Null,
    Data(MaybeText),
    Elem {
        element_id: ElementId,
        inner_html: MaybeText,
    },
    Error(OperatorApplicationError),
}

pub fn parse_op_sel_xpath(
    sess: &mut SessionSetupData,
    expr: CallExpr,
) -> Result<Box<dyn Operator>, TypelineError> {
    let xpath = expr.require_single_string_arg_autoconvert(sess)?;
    Ok(Box::new(OpSelXpath {
        xpath: UserXPath::from_str(&xpath),
    }))
}

struct OpSelXpath {
    xpath: UserXPath,
}

struct TfSelXpath<'a> {
    xpath: &'a UserXPath,
    input_iter: FieldIterId,
    input_field_ref: FieldReference,
    actor_id: ActorId,
}

static ATTR_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"/(@[^/]+|text\(\))$"#).unwrap());

impl UserXPath {
    fn from_str(xp: &str) -> Self {
        let (main, extra) = if let Some(trailing_comp) = ATTR_REGEX.find(xp) {
            let main = &xp[0..trailing_comp.start()];
            let extra_str = trailing_comp.as_str().to_string();
            let extra = if let Some(attr) = extra_str.strip_prefix("/@") {
                XPathFinalComponent::Attribute(attr.to_string())
            } else {
                assert!(extra_str.starts_with("/text"));
                XPathFinalComponent::Text
            };
            (main, Some(extra))
        } else {
            (xp, None)
        };
        UserXPath {
            main: thirtyfour::By::XPath(main),
            final_component: extra,
        }
    }
}

impl Operator for OpSelXpath {
    fn default_name(
        &self,
    ) -> typeline_core::operators::operator::OperatorName {
        "sel_xpath".into()
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
            actor_id: job.job_data.add_actor_for_tf_state(tf_state),
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
        let ab = &jd.match_set_mgr.match_sets[tf.match_set_id].action_buffer;
        ab.borrow_mut().begin_action_group(self.actor_id);
        let mut output = jd.field_mgr.fields[tf.output_field].borrow_mut();
        let (bs, ps) = jd.tf_mgr.claim_batch(tf_id);
        let mut bs_rem = bs;
        let field_pos_start = input_iter.get_next_field_pos();
        let mut field_pos = field_pos_start;
        while let Some(range) =
            input_iter.typed_range_fwd(&jd.match_set_mgr, bs_rem)
        {
            bs_rem -= range.base.field_count;
            match range.base.data {
                FieldValueSlice::Custom(data) => {
                    // prevents double borrow issue.
                    // TODO: this is subtle and non intuitive. fix?
                    let mut abb = ab.borrow_mut();
                    for c in FieldValueRangeIter::from_range(&range, data)
                        .unfold_rl()
                    {
                        match find_all_by_xpath(self.xpath, &**c, op_id) {
                            Ok(res) => {
                                if res.elems.is_empty() {
                                    abb.push_action(
                                        FieldActionKind::Drop,
                                        field_pos,
                                        1,
                                    );
                                }
                                if res.elems.len() > 1 {
                                    abb.push_action(
                                        FieldActionKind::Dup,
                                        field_pos,
                                        res.elems.len() - 1,
                                    );
                                }
                                field_pos += res.elems.len();
                                for elem in res.elems {
                                    match elem {
                                        WebElemMatch::Null => {
                                            output.iter_hall.push_null(1, true)
                                        }
                                        WebElemMatch::Data(data) => {
                                            output.iter_hall.push_maybe_text(
                                                data, 1, true, false,
                                            )
                                        }
                                        WebElemMatch::Elem {
                                            element_id,
                                            inner_html: outer_html,
                                        } => {
                                            output.iter_hall.push_custom(
                                                Box::new(SeleniumWebElement {
                                                    instance: res
                                                        .instance
                                                        .clone(),
                                                    window_handle: res
                                                        .window_handle
                                                        .clone(),
                                                    elem_id: element_id,
                                                    outer_html,
                                                }),
                                                1,
                                                true,
                                                false,
                                            );
                                        }
                                        WebElemMatch::Error(err) => output
                                            .iter_hall
                                            .push_error(err, 1, true, false),
                                    }
                                }
                            }
                            Err(e) => {
                                output.iter_hall.push_error(
                                    OperatorApplicationError::new_s(
                                        format!("failed to navigate: {e}"),
                                        op_id,
                                    ),
                                    range.base.field_count,
                                    true,
                                    true,
                                );
                                field_pos += 1;
                            }
                        }
                    }
                }
                FieldValueSlice::Error(_) => {
                    output.iter_hall.push_field_reference(
                        self.input_field_ref,
                        range.base.field_count,
                        true,
                        true,
                    );
                    field_pos += range.base.field_count;
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
                    field_pos += range.base.field_count;
                }
            }
        }
        ab.borrow_mut().end_action_group();
        jd.tf_mgr.submit_batch_ready_for_more(
            tf_id,
            field_pos - field_pos_start,
            ps,
        );
    }
}

async fn find_xpath_query(
    source: ElementQuerySource,
    xpath: &UserXPath,
    op_id: OperatorId,
) -> WebDriverResult<Vec<WebElemMatch>> {
    let elements = match &source {
        ElementQuerySource::Driver(driver) => {
            driver.find_all(xpath.main.clone()).await?
        }
        ElementQuerySource::Element(element) => {
            element.find_all(xpath.main.clone()).await?
        }
    };

    let mut res = Vec::new();
    let Some(fc) = xpath.final_component.as_ref() else {
        for e in elements.into_iter() {
            res.push(WebElemMatch::Elem {
                inner_html: MaybeText::Text(e.outer_html().await?),
                element_id: e.element_id,
            });
        }
        return Ok(res);
    };

    match &fc {
        XPathFinalComponent::Attribute(attr) => {
            for e in elements.into_iter() {
                match e.attr(attr).await {
                    WebDriverResult::Ok(Some(v)) => {
                        res.push(WebElemMatch::Data(MaybeText::Text(v)));
                    }
                    WebDriverResult::Ok(None) => {
                        continue;
                    }
                    WebDriverResult::Err(e) => {
                        res.push(WebElemMatch::Error(
                            OperatorApplicationError::new_s(
                                format!("failed to match xpath: {e}"),
                                op_id,
                            ),
                        ));
                    }
                }
            }
        }
        XPathFinalComponent::Text => {
            for e in elements.into_iter() {
                match e.text().await {
                    WebDriverResult::Ok(v) => {
                        res.push(WebElemMatch::Data(MaybeText::Text(v)));
                    }
                    WebDriverResult::Err(e) => {
                        res.push(WebElemMatch::Error(
                            OperatorApplicationError::new_s(
                                format!("failed to match xpath: {e}"),
                                op_id,
                            ),
                        ));
                    }
                }
            }
        }
    }

    Ok(res)
}

fn find_all_by_xpath(
    xpath: &UserXPath,
    c: &dyn CustomData,
    op_id: OperatorId,
) -> Result<XPathMatchResult, String> {
    if let Some(window) = c.downcast_ref::<SeleniumWindow>() {
        let win = &window.window_handle;
        let inst = &mut *window.instance.lock().unwrap();
        let wad = &mut inst.window_aware_driver;
        let res: WebDriverResult<_> = inst
            .runtime
            .block_on(async {
                wad.switch_to_window(win).await?;
                find_xpath_query(
                    ElementQuerySource::Driver(wad.driver.handle.clone()),
                    xpath,
                    op_id,
                )
                .await
            })
            .map(|v| XPathMatchResult {
                instance: window.instance.clone(),
                window_handle: win.clone(),
                elems: v,
            });

        return res.map_err(|e| format!("failed to navigate: {e}"));
    }

    if let Some(elem) = c.downcast_ref::<SeleniumWebElement>() {
        let win = &elem.window_handle;
        let inst = &mut *elem.instance.lock().unwrap();
        let wad = &mut inst.window_aware_driver;
        let src_elem = WebElement {
            element_id: elem.elem_id.clone(),
            handle: wad.driver.handle.clone(),
        };
        let res: WebDriverResult<_> = inst
            .runtime
            .block_on(async {
                wad.switch_to_window(win).await?;
                find_xpath_query(
                    ElementQuerySource::Element(src_elem),
                    xpath,
                    op_id,
                )
                .await
            })
            .map(|v| XPathMatchResult {
                instance: elem.instance.clone(),
                window_handle: win.clone(),
                elems: v,
            });

        return res.map_err(|e| format!("failed to navigate: {e}"));
    }

    Err(format!(
        "sel_xpath expected selenium element, found {}",
        c.type_name()
    ))
}
