use crate::{
    chain::ChainId,
    cli::{
        call_expr::{Argument, CallExpr, Span},
        parse_operator_data,
    },
    job::Job,
    operators::operator::TransformInstatiation,
    options::session_setup::SessionSetupData,
    scr_error::ScrError,
    utils::indexing_type::IndexingType,
};

use super::{
    errors::OperatorCreationError,
    operator::{
        OffsetInAggregation, Operator, OperatorData, OperatorDataId,
        OperatorId, OperatorOffsetInChain, PreboundOutputsMap,
    },
    transform::TransformState,
    utils::nested_op::{setup_op_outputs_for_nested_op, NestedOp},
};

pub struct OpTransparent {
    pub nested_op: NestedOp,
}

pub fn create_op_transparent_with_span(
    op: OperatorData,
    span: Span,
) -> OperatorData {
    OperatorData::from_custom(OpTransparent {
        nested_op: NestedOp::Operator(Box::new((op, span))),
    })
}

pub fn create_op_transparent(op: OperatorData) -> OperatorData {
    create_op_transparent_with_span(op, Span::Generated)
}

pub fn parse_op_transparent(
    sess: &mut SessionSetupData,
    mut arg: Argument,
) -> Result<OperatorData, ScrError> {
    let expr = CallExpr::from_argument_mut(&mut arg)?;

    if expr.args.len() != 1 {
        return Err(OperatorCreationError::new(
            "operator `transparent` expects exactly one argument",
            expr.span,
        )
        .into());
    }

    let arg = &mut expr.args[0];
    let span = arg.span;
    let op = parse_operator_data(sess, std::mem::take(arg))?;
    Ok(create_op_transparent_with_span(op, span))
}

impl Operator for OpTransparent {
    fn default_name(&self) -> super::operator::OperatorName {
        todo!()
    }

    fn output_count(
        &self,
        sess: &crate::context::SessionData,
        _op_id: OperatorId,
    ) -> usize {
        let NestedOp::SetUp(op_id) = self.nested_op else {
            unreachable!()
        };
        sess.operator_data[sess.op_data_id(op_id)].output_count(sess, op_id)
    }

    fn has_dynamic_outputs(
        &self,
        sess: &crate::context::SessionData,
        _op_id: OperatorId,
    ) -> bool {
        let NestedOp::SetUp(op_id) = self.nested_op else {
            unreachable!()
        };
        sess.operator_data[sess.op_data_id(op_id)]
            .has_dynamic_outputs(sess, op_id)
    }

    fn output_field_kind(
        &self,
        sess: &crate::context::SessionData,
        _op_id: OperatorId,
    ) -> super::operator::OutputFieldKind {
        // TODO: debatable. shouldn't this be SameAsInput?
        let NestedOp::SetUp(op_id) = self.nested_op else {
            unreachable!()
        };
        sess.operator_data[sess.op_data_id(op_id)]
            .output_field_kind(sess, op_id)
    }

    fn setup(
        &mut self,
        sess: &mut SessionSetupData,
        op_data_id: OperatorDataId,
        chain_id: ChainId,
        offset_in_chain: OperatorOffsetInChain,
        span: Span,
    ) -> Result<OperatorId, ScrError> {
        let op_id = sess.add_op(op_data_id, chain_id, offset_in_chain, span);
        let NestedOp::Operator(op_span) = &mut self.nested_op else {
            panic!("operator was already set up");
        };
        let (op_data, span) = *std::mem::take(op_span);

        let sub_op_id = sess.add_op_data(op_data);
        let nested_op_id = sess.add_op(
            sub_op_id,
            sess.curr_chain,
            OperatorOffsetInChain::AggregationMember(
                op_id,
                OffsetInAggregation::ZERO,
            ),
            span,
        );
        self.nested_op = NestedOp::SetUp(nested_op_id);
        Ok(op_id)
    }

    fn register_output_var_names(
        &self,
        ld: &mut crate::liveness_analysis::LivenessData,
        sess: &crate::context::SessionData,
        _op_id: OperatorId,
    ) {
        let NestedOp::SetUp(op_id) = self.nested_op else {
            unreachable!()
        };
        sess.operator_data[sess.op_data_id(op_id)]
            .register_output_var_names(ld, sess, op_id);
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
        let NestedOp::SetUp(nested_op_id) = self.nested_op else {
            unreachable!()
        };
        sess.operator_data[sess.op_data_id(op_id)].update_liveness_for_op(
            sess,
            ld,
            op_offset_after_last_write,
            nested_op_id,
            bb_id,
            input_field,
            output,
        );
        output.primary_output = input_field;
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
        let NestedOp::SetUp(sub_op_id) = self.nested_op else {
            unreachable!()
        };
        ld.update_bb_for_op(sess, sub_op_id, op_n, cn, bb_id)
    }

    fn assign_op_outputs(
        &mut self,
        sess: &mut crate::context::SessionData,
        ld: &mut crate::liveness_analysis::LivenessData,
        op_id: OperatorId,
        output_count: &mut crate::liveness_analysis::OpOutputIdx,
    ) {
        setup_op_outputs_for_nested_op(
            &self.nested_op,
            sess,
            ld,
            op_id,
            output_count,
        );
    }

    fn on_liveness_computed(
        &mut self,
        sess: &mut crate::context::SessionData,
        ld: &crate::liveness_analysis::LivenessData,
        _op_id: OperatorId,
    ) {
        if let NestedOp::SetUp(op_id) = self.nested_op {
            let op_data_id = sess.op_data_id(op_id);
            let mut op_data =
                std::mem::take(&mut sess.operator_data[op_data_id]);
            op_data.on_liveness_computed(sess, ld, op_id);
            sess.operator_data[op_data_id] = op_data;
        }
    }

    fn aggregation_member(
        &self,
        _agg_offset: OffsetInAggregation,
    ) -> Option<OperatorId> {
        if let NestedOp::SetUp(op_id) = self.nested_op {
            return Some(op_id);
        }
        None
    }

    fn build_transforms<'a>(
        &'a self,
        job: &mut Job<'a>,
        tf_state: &mut TransformState,
        _op_id: OperatorId,
        prebound_outputs: &PreboundOutputsMap,
    ) -> super::operator::TransformInstatiation<'a> {
        let NestedOp::SetUp(nested_op_id) = self.nested_op else {
            unreachable!()
        };
        let field_before = tf_state.input_field;
        let ms_id_before = tf_state.match_set_id;

        job.job_data.field_mgr.bump_field_refcount(field_before);

        let sess = &job.job_data.session_data;
        let mut instantiation = sess.operator_data
            [sess.op_data_id(nested_op_id)]
        .operator_build_transforms(
            job,
            tf_state.clone(),
            nested_op_id,
            prebound_outputs,
        );

        assert!(
            instantiation.next_match_set == ms_id_before,
            "transparent does not support cross ms yet"
        );
        instantiation.next_input_field = field_before;
        instantiation.next_match_set = ms_id_before;
        TransformInstatiation::Multiple(instantiation)
    }
}
