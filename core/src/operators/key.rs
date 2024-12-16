use crate::{
    chain::ChainId,
    cli::{
        call_expr::{Argument, CallExpr, Span},
        parse_operator_data,
    },
    liveness_analysis::OperatorCallEffect,
    options::session_setup::SessionSetupData,
    scr_error::ScrError,
    utils::{indexing_type::IndexingType, string_store::StringStoreEntry},
};

use super::{
    errors::OperatorCreationError,
    nop::OpNop,
    operator::{
        OffsetInAggregation, Operator, OperatorDataId, OperatorId,
        OperatorOffsetInChain, OutputFieldKind,
    },
    utils::nested_op::{setup_op_outputs_for_nested_op, NestedOp},
};

pub struct OpKey {
    pub key: String,
    pub key_interned: Option<StringStoreEntry>,
    pub nested_op: Option<NestedOp>,
}

pub fn parse_op_key(
    sess: &mut SessionSetupData,
    mut arg: Argument,
) -> Result<Box<dyn Operator>, ScrError> {
    let expr = CallExpr::from_argument_mut(&mut arg)?;
    let op_name = expr.op_name;

    if expr.args.len() < 2 {
        return Err(OperatorCreationError::new(
            "missing label argument for operator `key`",
            expr.span,
        )
        .into());
    }

    let key_span = expr.args[0].span;

    let key = std::mem::take(&mut expr.args[0].value)
        .into_maybe_text()
        .ok_or_else(|| expr.error_positional_arg_not_plaintext(key_span))?;

    let key = key
        .into_text()
        .ok_or_else(|| expr.error_arg_invalid_utf8(op_name, key_span))?;

    let mut nested_op = None;
    if let Some(arg) = expr.args.get_mut(1) {
        let span = arg.span;
        let op = parse_operator_data(sess, std::mem::take(arg))?;
        nested_op = Some(NestedOp::Operator(Box::new((op, span))));
    }

    if expr.args.len() > 2 {
        return Err(OperatorCreationError::new(
            "operator key only accepts two arguments`",
            expr.args[2].span,
        )
        .into());
    }

    Ok(Box::new(OpKey {
        key,
        key_interned: None,
        nested_op,
    }))
}

pub fn create_op_key(key: String) -> Box<dyn Operator> {
    Box::new(OpKey {
        key,
        key_interned: None,
        nested_op: None,
    })
}

pub fn create_op_key_with_op(
    key: String,
    op: Box<dyn Operator>,
) -> Box<dyn Operator> {
    Box::new(OpKey {
        key,
        key_interned: None,
        nested_op: Some(NestedOp::Operator(Box::new((op, Span::Generated)))),
    })
}

impl Operator for OpKey {
    fn setup(
        &mut self,
        sess: &mut SessionSetupData,
        op_data_id: OperatorDataId,
        chain_id: ChainId,
        offset_in_chain: OperatorOffsetInChain,
        span: Span,
    ) -> Result<OperatorId, ScrError> {
        self.key_interned = Some(sess.string_store.intern_cloned(&self.key));
        let op_id = sess.add_op(op_data_id, chain_id, offset_in_chain, span);
        let Some(nested_op) = &mut self.nested_op else {
            return Ok(op_id);
        };
        let NestedOp::Operator(op_span) = nested_op else {
            panic!("operator was already set up");
        };
        let (sub_op, span) = *std::mem::replace(
            op_span,
            Box::new((Box::new(OpNop::default()), Span::default())),
        );
        let sub_op_id = sess.setup_op_from_data(
            sub_op,
            sess.curr_chain,
            OperatorOffsetInChain::AggregationMember(
                op_id,
                OffsetInAggregation::ZERO,
            ),
            span,
        )?;
        self.nested_op = Some(NestedOp::SetUp(sub_op_id));

        Ok(op_id)
    }
    fn update_bb_for_op(
        &self,
        sess: &crate::context::SessionData,
        ld: &mut crate::liveness_analysis::LivenessData,
        _op_id: OperatorId,
        op_n: super::operator::OffsetInChain,
        cn: &crate::chain::Chain,
        bb_id: crate::liveness_analysis::BasicBlockId,
    ) -> bool {
        let Some(nested_op) = &self.nested_op else {
            return false;
        };
        let &NestedOp::SetUp(sub_op_id) = nested_op else {
            unreachable!()
        };
        return ld.update_bb_for_op(sess, sub_op_id, op_n, cn, bb_id);
    }

    fn default_name(&self) -> super::operator::OperatorName {
        "key".into()
    }

    fn debug_op_name(&self) -> super::operator::OperatorName {
        let Some(nested) = &self.nested_op else {
            return self.default_name();
        };
        match nested {
            NestedOp::Operator(nested_op) => {
                format!(
                    "[ key '{}' {} ]",
                    self.key,
                    nested_op.0.debug_op_name()
                )
            }
            NestedOp::SetUp(op_id) => {
                format!("[ key '{}' <op {op_id:02}> ]", self.key)
            }
        }
        .into()
    }

    fn update_variable_liveness(
        &self,
        sess: &crate::context::SessionData,
        ld: &mut crate::liveness_analysis::LivenessData,
        op_offset_after_last_write: super::operator::OffsetInChain,
        _op_id: OperatorId,
        bb_id: crate::liveness_analysis::BasicBlockId,
        input_field: crate::liveness_analysis::OpOutputIdx,
        output: &mut crate::liveness_analysis::OperatorLivenessOutput,
    ) {
        if let Some(NestedOp::SetUp(nested_op_id)) = self.nested_op {
            sess.operator_data[sess.op_data_id(nested_op_id)]
                .update_variable_liveness(
                    sess,
                    ld,
                    op_offset_after_last_write,
                    nested_op_id,
                    bb_id,
                    input_field,
                    output,
                );
        }

        let var_id = ld.var_names[&self.key_interned.unwrap()];
        ld.vars_to_op_outputs_map[var_id] = output.primary_output;
        ld.op_outputs[output.primary_output]
            .field_references
            .push(input_field);
        if let Some(prev_tgt) = ld.key_aliases_map.insert(var_id, input_field)
        {
            ld.apply_var_remapping(var_id, prev_tgt);
        }
        output.primary_output = input_field;
        output.call_effect = OperatorCallEffect::NoCall;
    }

    fn output_field_kind(
        &self,
        sess: &crate::context::SessionData,
        _op_id: OperatorId,
    ) -> super::operator::OutputFieldKind {
        let Some(nested) = &self.nested_op else {
            return OutputFieldKind::SameAsInput;
        };
        let &NestedOp::SetUp(op_id) = nested else {
            unreachable!()
        };
        sess.operator_data[sess.op_data_id(op_id)]
            .output_field_kind(sess, op_id)
    }

    fn register_output_var_names(
        &self,
        ld: &mut crate::liveness_analysis::LivenessData,
        sess: &crate::context::SessionData,
        _op_id: OperatorId,
    ) {
        ld.add_var_name(self.key_interned.unwrap());
        if let Some(NestedOp::SetUp(op_id)) = self.nested_op {
            sess.operator_data[sess.op_data_id(op_id)]
                .register_output_var_names(ld, sess, op_id);
        }
    }

    fn output_count(
        &self,
        sess: &crate::context::SessionData,
        _op_id: OperatorId,
    ) -> usize {
        let Some(nested) = &self.nested_op else {
            return 0;
        };
        let &NestedOp::SetUp(op_id) = nested else {
            unreachable!()
        };
        sess.operator_data[sess.op_data_id(op_id)].output_count(sess, op_id)
    }

    fn aggregation_member(
        &self,
        agg_offset: OffsetInAggregation,
    ) -> Option<OperatorId> {
        if agg_offset != OffsetInAggregation::ZERO {
            return None;
        }
        if let Some(NestedOp::SetUp(op_id)) = self.nested_op {
            return Some(op_id);
        }
        None
    }

    fn on_liveness_computed(
        &mut self,
        sess: &mut crate::context::SessionData,
        ld: &crate::liveness_analysis::LivenessData,
        _op_id: OperatorId,
    ) {
        if let Some(NestedOp::SetUp(op_id)) = self.nested_op {
            let op_data_id = sess.op_data_id(op_id);
            let mut op_data = std::mem::replace(
                &mut sess.operator_data[op_data_id],
                Box::new(OpNop::default()),
            );
            op_data.on_liveness_computed(sess, ld, op_id);
            sess.operator_data[op_data_id] = op_data;
        }
    }

    fn assign_op_outputs(
        &mut self,
        sess: &mut crate::context::SessionData,
        ld: &mut crate::liveness_analysis::LivenessData,
        op_id: OperatorId,
        output_count: &mut crate::liveness_analysis::OpOutputIdx,
    ) {
        if let Some(nested_op) = &self.nested_op {
            setup_op_outputs_for_nested_op(
                nested_op,
                sess,
                ld,
                op_id,
                output_count,
            );
            return;
        }
        let op_base = &mut sess.operator_bases[op_id];
        op_base.outputs_start = *output_count;
        op_base.outputs_end = op_base.outputs_start;
    }

    fn has_dynamic_outputs(
        &self,
        sess: &crate::context::SessionData,
        _op_id: OperatorId,
    ) -> bool {
        let Some(nested) = &self.nested_op else {
            return false;
        };
        let &NestedOp::SetUp(op_id) = nested else {
            unreachable!()
        };
        sess.operator_data[sess.op_data_id(op_id)]
            .has_dynamic_outputs(sess, op_id)
    }

    fn build_transforms<'a>(
        &'a self,
        _job: &mut crate::job::Job<'a>,
        _tf_state: &mut super::transform::TransformState,
        _op_id: OperatorId,
        _prebound_outputs: &super::operator::PreboundOutputsMap,
    ) -> super::operator::TransformInstatiation<'a> {
        unreachable!()
    }

    fn as_any(&self) -> Option<&dyn std::any::Any> {
        Some(self)
    }

    fn as_any_mut(&mut self) -> Option<&mut dyn std::any::Any> {
        Some(self)
    }
}
