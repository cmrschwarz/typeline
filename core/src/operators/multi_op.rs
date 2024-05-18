use crate::{
    context::SessionData,
    job::Job,
    liveness_analysis::{
        AccessFlags, BasicBlockId, LivenessData, OpOutputIdx,
        OperatorCallEffect,
    },
    options::session_options::SessionOptions,
};

use super::{
    operator::{
        DefaultOperatorName, Operator, OperatorBase, OperatorData, OperatorId,
        OperatorOffsetInChain, OutputFieldKind, PreboundOutputsMap,
        TransformInstatiation,
    },
    transform::TransformState,
};

pub struct OpMultiOp {
    ops: Vec<OperatorData>,
}

impl Operator for OpMultiOp {
    fn default_name(&self) -> super::operator::DefaultOperatorName {
        let mut res = DefaultOperatorName::from("multi-op<");
        let mut iter = self.ops.iter().peekable();
        while let Some(op) = iter.next() {
            res.push_str(&op.default_op_name());
            if iter.peek().is_some() {
                res.push_str(", ");
            }
        }
        res.push('>');
        res
    }

    fn output_count(&self, sess: &SessionData, op_id: OperatorId) -> usize {
        self.ops.iter().map(|op| op.output_count(sess, op_id)).sum()
    }

    fn has_dynamic_outputs(
        &self,
        sess: &SessionData,
        op_id: OperatorId,
    ) -> bool {
        self.ops
            .iter()
            .any(|op| op.has_dynamic_outputs(sess, op_id))
    }

    fn update_variable_liveness(
        &self,
        sess: &SessionData,
        ld: &mut LivenessData,
        flags: &mut AccessFlags,
        op_offset_after_last_write: OperatorOffsetInChain,
        op_id: OperatorId,
        bb_id: BasicBlockId,
        input_field: OpOutputIdx,
    ) -> Option<(OpOutputIdx, OperatorCallEffect)> {
        if self.ops.is_empty() {
            return Some((input_field, OperatorCallEffect::NoCall));
        }
        let mut i = 0;
        let mut next_input = input_field;
        let mut outputs_offset = 0;
        loop {
            let op = &self.ops[i];
            let (output, ce) = op.update_liveness_for_op(
                sess,
                ld,
                flags,
                op_offset_after_last_write,
                op_id,
                bb_id,
                next_input,
                outputs_offset,
            );
            i += 1;
            outputs_offset += op.output_count(sess, op_id) as OpOutputIdx;
            next_input = output;
            if i == self.ops.len() {
                return Some((output, ce));
            }
            assert!(
                ce != OperatorCallEffect::Diverge,
                "non final operator `{}`, index {} inside multi-op (len {}) may not diverge",
                i - 1, // we already went to the next index
                op.debug_op_name(),
                self.ops.len()
            );
        }
    }

    fn build_transforms<'a>(
        &'a self,
        job: &mut Job<'a>,
        tf_state: &mut TransformState,
        op_id: OperatorId,
        prebound_outputs: &PreboundOutputsMap,
    ) -> TransformInstatiation {
        TransformInstatiation::Multi(job.setup_transforms_for_op_iter(
            self.ops.iter().map(|op_data| {
                (
                    op_id,
                    &job.job_data.session_data.operator_bases[op_id as usize],
                    op_data,
                )
            }),
            tf_state.match_set_id,
            tf_state.input_field,
            tf_state.input_group_track_id,
            None,
            None,
            prebound_outputs,
        ))
    }

    fn output_field_kind(
        &self,
        op_base: &OperatorBase,
    ) -> super::operator::OutputFieldKind {
        // TODO: this is not correct, if the last one says same as
        let Some(op) = self.ops.last() else {
            return OutputFieldKind::SameAsInput;
        };
        let mut ofk = op.output_field_kind(op_base);
        if ofk != OutputFieldKind::SameAsInput {
            return ofk;
        }
        for op in self.ops.iter().rev().skip(1) {
            ofk = op.output_field_kind(op_base);
            if ofk != OutputFieldKind::SameAsInput {
                return ofk;
            }
        }
        ofk
    }

    fn on_op_added(
        &mut self,
        so: &mut SessionOptions,
        op_id: OperatorId,
        add_to_chain: bool,
    ) {
        for op in &mut self.ops {
            op.on_op_added(so, op_id, add_to_chain);
        }
    }

    fn on_subchains_added(&mut self, _current_subchain_count: u32) {}

    fn register_output_var_names(
        &self,
        ld: &mut LivenessData,
        sess: &SessionData,
        op_id: OperatorId,
    ) {
        for op in &self.ops {
            op.register_output_var_names(ld, sess, op_id)
        }
    }

    fn setup(
        &mut self,
        sess: &mut SessionData,
        chain_id: OperatorId,
        op_id: OperatorId,
    ) -> Result<(), super::errors::OperatorSetupError> {
        for op in &mut self.ops {
            op.setup(sess, chain_id, op_id)?;
        }
        Ok(())
    }

    fn on_liveness_computed(
        &mut self,
        sess: &mut SessionData,
        ld: &LivenessData,
        op_id: OperatorId,
    ) {
        for op in &mut self.ops {
            op.on_liveness_computed(sess, ld, op_id)
        }
    }

    fn can_be_appended(&self) -> bool {
        self.ops
            .last()
            .map(OperatorData::can_be_appended)
            .unwrap_or(true)
    }
}

pub fn create_multi_op(
    ops: impl IntoIterator<Item = OperatorData>,
) -> OperatorData {
    OperatorData::MultiOp(OpMultiOp {
        ops: ops.into_iter().collect(),
    })
}
