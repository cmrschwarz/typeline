use crate::{
    chain::{ChainId, SubchainIndex},
    cli::call_expr::Span,
    context::SessionData,
    job::Job,
    liveness_analysis::{
        BasicBlockId, LivenessData, OpOutputIdx, OperatorCallEffect,
        OperatorLivenessOutput,
    },
    options::session_setup::SessionSetupData,
    scr_error::ScrError,
    utils::index_vec::IndexVec,
};

use super::{
    operator::{
        OffsetInAggregation, OffsetInChain, Operator, OperatorData,
        OperatorDataId, OperatorId, OperatorName, OperatorOffsetInChain,
        OutputFieldKind, PreboundOutputsMap, TransformInstatiation,
    },
    transform::TransformState,
};

pub struct OpMultiOp {
    pub operations: Vec<(OperatorData, Span)>,
    pub sub_op_ids: IndexVec<OffsetInAggregation, OperatorId>,
}

impl Operator for OpMultiOp {
    fn default_name(&self) -> OperatorName {
        "<multi_op>".into()
    }
    fn debug_op_name(&self) -> super::operator::OperatorName {
        let mut res = String::from("multi-op<");
        for (i, (op, _span)) in self.operations.iter().enumerate() {
            if i > 0 {
                res.push_str(", ");
            }
            res.push_str(&op.debug_op_name());
        }
        res.push('>');
        res.into()
    }

    fn output_count(&self, sess: &SessionData, _op_id: OperatorId) -> usize {
        self.sub_op_ids
            .iter()
            .map(|&op_id| {
                sess.operator_data[sess.op_data_id(op_id)]
                    .output_count(sess, op_id)
            })
            .sum()
    }

    fn has_dynamic_outputs(
        &self,
        sess: &SessionData,
        _op_id: OperatorId,
    ) -> bool {
        self.sub_op_ids.iter().any(|&op_id| {
            sess.operator_data[sess.op_data_id(op_id)]
                .has_dynamic_outputs(sess, op_id)
        })
    }

    fn update_variable_liveness(
        &self,
        sess: &SessionData,
        ld: &mut LivenessData,
        op_offset_after_last_write: OffsetInChain,
        op_id: OperatorId,
        bb_id: BasicBlockId,
        input_field: OpOutputIdx,
        mut outputs_offset: usize,
        output: &mut OperatorLivenessOutput,
    ) {
        let mut next_input = input_field;
        for (agg_offset, &sub_op_id) in self.sub_op_ids.iter_enumerated() {
            let op = &sess.operator_data[sess.op_data_id(sub_op_id)];
            // TODO: manage access flags for subsequent ops correctly
            let mut sub_output =
                OperatorLivenessOutput::with_defaults(next_input, 0);
            op.update_liveness_for_op(
                sess,
                ld,
                op_offset_after_last_write,
                op_id,
                bb_id,
                next_input,
                outputs_offset,
                &mut sub_output,
            );
            outputs_offset += op.output_count(sess, op_id);
            next_input = sub_output.primary_output;
            if agg_offset == self.sub_op_ids.last_idx().unwrap() {
                *output = sub_output;
                return;
            }
            assert!(
                sub_output.call_effect != OperatorCallEffect::Diverge,
                "non final operator `{}`, index {} inside multi-op (len {}) may not diverge",
                agg_offset , // we already went to the next index
                op.debug_op_name(),
                self.sub_op_ids.len()
            );
        }
        output.primary_output = input_field;
        output.call_effect = OperatorCallEffect::NoCall;
    }

    fn build_transforms<'a>(
        &'a self,
        job: &mut Job<'a>,
        tf_state: &mut TransformState,
        _op_id: OperatorId,
        prebound_outputs: &PreboundOutputsMap,
    ) -> TransformInstatiation<'a> {
        TransformInstatiation::Multiple(job.setup_transforms_for_op_iter(
            self.sub_op_ids.iter().map(|&sub_op_id| {
                let op_base =
                    &job.job_data.session_data.operator_bases[sub_op_id];
                let op_data = &job.job_data.session_data.operator_data
                    [op_base.op_data_id];
                (sub_op_id, op_base, op_data)
            }),
            tf_state.match_set_id,
            tf_state.input_field,
            tf_state.input_group_track_id,
            None,
            prebound_outputs,
        ))
    }

    fn output_field_kind(
        &self,
        _sess: &SessionData,
        _op_id: OperatorId,
    ) -> super::operator::OutputFieldKind {
        OutputFieldKind::Unconfigured
    }

    fn on_subchains_added(&mut self, _current_subchain_count: SubchainIndex) {}

    fn register_output_var_names(
        &self,
        ld: &mut LivenessData,
        sess: &SessionData,
        _op_id: OperatorId,
    ) {
        for &op_id in &self.sub_op_ids {
            sess.operator_data[sess.op_data_id(op_id)]
                .register_output_var_names(ld, sess, op_id)
        }
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
        for (op_data, span) in std::mem::take(&mut self.operations) {
            self.sub_op_ids.push(sess.setup_op_from_data(
                op_data,
                chain_id,
                OperatorOffsetInChain::AggregationMember(
                    op_id,
                    self.sub_op_ids.next_idx(),
                ),
                span,
            )?);
        }
        Ok(op_id)
    }

    fn on_liveness_computed(
        &mut self,
        sess: &mut SessionData,
        ld: &LivenessData,
        _op_id: OperatorId,
    ) {
        for &op_id in &self.sub_op_ids {
            sess.with_mut_op_data(op_id, |sess, op_data| {
                op_data.on_liveness_computed(sess, ld, op_id)
            });
        }
    }
}

pub fn create_multi_op_with_span(
    ops: impl IntoIterator<Item = (OperatorData, Span)>,
) -> OperatorData {
    OperatorData::MultiOp(OpMultiOp {
        operations: ops.into_iter().collect(),
        sub_op_ids: IndexVec::new(),
    })
}

pub fn create_multi_op(
    ops: impl IntoIterator<Item = OperatorData>,
) -> OperatorData {
    create_multi_op_with_span(ops.into_iter().map(|op| (op, Span::Generated)))
}
