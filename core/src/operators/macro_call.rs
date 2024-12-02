use std::sync::Arc;

use crate::{
    chain::ChainId,
    cli::call_expr::{Argument, CallExprEndKind, MetaInfo, Span},
    options::{
        context_builder::ContextBuilder,
        session_setup::{ScrSetupOptions, SessionSetupData},
    },
    record_data::{
        array::Array, field_data::FieldValueRepr, field_value::FieldValue,
    },
    scr_error::{ContextualizedScrError, ScrError},
};

use super::{
    errors::OperatorSetupError,
    macro_def::MacroDeclData,
    multi_op::OpMultiOp,
    operator::{Operator, OperatorDataId, OperatorId, OperatorOffsetInChain},
};

pub struct OpMacroCall {
    pub decl: Arc<MacroDeclData>,
    pub arg: Argument,
    pub multi_op: OpMultiOp,
}

impl Operator for OpMacroCall {
    fn default_name(&self) -> super::operator::OperatorName {
        format!(
            "macro-call<`{}`, {}>",
            self.decl.name_stored,
            self.multi_op.default_name()
        )
        .into()
    }

    fn output_count(
        &self,
        sess: &crate::context::SessionData,
        op_id: OperatorId,
    ) -> usize {
        self.multi_op.output_count(sess, op_id)
    }

    fn has_dynamic_outputs(
        &self,
        sess: &crate::context::SessionData,
        op_id: OperatorId,
    ) -> bool {
        self.multi_op.has_dynamic_outputs(sess, op_id)
    }

    fn build_transforms<'a>(
        &'a self,
        job: &mut crate::job::Job<'a>,
        tf_state: &mut super::transform::TransformState,
        op_id: OperatorId,
        prebound_outputs: &super::operator::PreboundOutputsMap,
    ) -> super::operator::TransformInstatiation<'a> {
        self.multi_op
            .build_transforms(job, tf_state, op_id, prebound_outputs)
    }

    fn output_field_kind(
        &self,
        sess: &crate::context::SessionData,
        op_id: OperatorId,
    ) -> super::operator::OutputFieldKind {
        self.multi_op.output_field_kind(sess, op_id)
    }

    fn register_output_var_names(
        &self,
        ld: &mut crate::liveness_analysis::LivenessData,
        sess: &crate::context::SessionData,
        op_id: OperatorId,
    ) {
        self.multi_op.register_output_var_names(ld, sess, op_id);
    }

    fn update_bb_for_op(
        &self,
        sess: &crate::context::SessionData,
        ld: &mut crate::liveness_analysis::LivenessData,
        op_id: OperatorId,
        op_n: super::operator::OffsetInChain,
        cn: &crate::chain::Chain,
        bb_id: crate::liveness_analysis::BasicBlockId,
    ) -> bool {
        self.multi_op
            .update_bb_for_op(sess, ld, op_id, op_n, cn, bb_id)
    }

    fn assign_op_outputs(
        &mut self,
        sess: &mut crate::context::SessionData,
        ld: &mut crate::liveness_analysis::LivenessData,
        op_id: OperatorId,
        output_count: &mut crate::liveness_analysis::OpOutputIdx,
    ) {
        self.multi_op
            .assign_op_outputs(sess, ld, op_id, output_count);
    }

    fn aggregation_member(
        &self,
        agg_offset: super::operator::OffsetInAggregation,
    ) -> Option<OperatorId> {
        self.multi_op.aggregation_member(agg_offset)
    }

    fn update_variable_liveness(
        &self,
        sess: &crate::context::SessionData,
        ld: &mut crate::liveness_analysis::LivenessData,
        op_offset_after_last_write: super::operator::OffsetInChain,
        op_id: OperatorId,
        bb_id: crate::liveness_analysis::BasicBlockId,
        input_field: crate::liveness_analysis::OpOutputIdx,
        output: &mut crate::liveness_analysis::OperatorLivenessOutput,
    ) {
        self.multi_op.update_variable_liveness(
            sess,
            ld,
            op_offset_after_last_write,
            op_id,
            bb_id,
            input_field,
            output,
        );
    }

    fn setup(
        &mut self,
        sess: &mut SessionSetupData,
        op_data_id: OperatorDataId,
        chain_id: ChainId,
        offset_in_chain: OperatorOffsetInChain,
        span: Span,
    ) -> Result<OperatorId, ScrError> {
        let parent_scope_id = sess.chains[chain_id].scope_id;

        let op_id = sess.add_op(op_data_id, chain_id, offset_in_chain, span);

        let map_error = |e: ContextualizedScrError| {
            OperatorSetupError::new_s(
                format!(
                    "error during macro instantiation '{}': {}",
                    self.decl.name_stored, e.contextualized_message
                ),
                op_id,
            )
        };

        let result_args = ContextBuilder::from_arguments(
            ScrSetupOptions {
                extensions: sess.extensions.clone(),
                deny_threading: true,
                allow_repl: false,
                start_with_stdin: false,
                print_output: false,
                add_success_updator: false,
                skip_first_cli_arg: false,
            },
            None,
            Vec::from_iter(self.decl.args.iter().cloned()),
        )
        .map_err(map_error)?
        .push_fixed_size_type(self.arg.clone(), 1)
        .run_collect()
        .map_err(map_error)?;

        if result_args.len() != 1 {
            return Err(OperatorSetupError::new_s(
            format!("error during macro instantiation: single code block expected, macro returned {}", result_args.len()),
            op_id,
        )
        .into());
        }

        let arr = match result_args.into_iter().next().unwrap() {
            FieldValue::Array(arr) => arr,
            FieldValue::Argument(arg) => {
                if let FieldValue::Array(arr) = arg.value {
                    arr
                } else {
                    return Err(OperatorSetupError::new_s(
                    format!(
                        "error during macro instantiation: code block expected, macro returned argument[{}]",
                        arg.value.kind()
                    ),
                    op_id,
                )
                .into());
                }
            }
            arg @ _ => {
                return Err(OperatorSetupError::new_s(
                format!("error during macro instantiation: code block expected, macro returned {}", arg.kind()),
                op_id,
            )
            .into());
            }
        };

        for (i, arg) in arr.into_iter().enumerate() {
            let mut arg = if let FieldValue::Argument(arg) = arg {
                *arg
            } else {
                Argument {
                    value: arg,
                    span: Span::MacroExpansion { op_id },
                    source_scope: parent_scope_id,
                    meta_info: Some(MetaInfo::EndKind(
                        CallExprEndKind::SpecialBuiltin,
                    )),
                }
            };

            let FieldValue::Array(arr) = &mut arg.value else {
                return Err(OperatorSetupError::new_s(
                format!(
                    "error during macro instantiation: in operator {i}: s-expr expected, macro returned {}",
                    arg.value.kind()
                ),
                op_id,
            )
            .into());
            };

            if !arr.is_empty() && arr.repr() != Some(FieldValueRepr::Argument)
            {
                let mut arr_argumentized = Vec::new();
                for v in std::mem::take(arr).into_iter() {
                    if let FieldValue::Argument(arg) = v {
                        arr_argumentized.push(*arg);
                    } else {
                        arr_argumentized.push(Argument {
                            value: v,
                            span: Span::MacroExpansion { op_id },
                            source_scope: parent_scope_id,
                            meta_info: Some(MetaInfo::EndKind(
                                CallExprEndKind::SpecialBuiltin,
                            )),
                        });
                    }
                }
                *arr = Array::Argument(arr_argumentized);
            }

            // TODO: contextualize error
            let op_data = sess.parse_argument(arg)?;

            self.multi_op.sub_op_ids.push(sess.setup_op_from_data(
                op_data,
                chain_id,
                OperatorOffsetInChain::AggregationMember(
                    op_id,
                    self.multi_op.sub_op_ids.next_idx(),
                ),
                span,
            )?);
        }

        Ok(op_id)
    }

    fn on_liveness_computed(
        &mut self,
        sess: &mut crate::context::SessionData,
        ld: &crate::liveness_analysis::LivenessData,
        op_id: OperatorId,
    ) {
        self.multi_op.on_liveness_computed(sess, ld, op_id);
    }
}
