use crate::{
    chain::{ChainId, SubchainIndex},
    context::{SessionData, SessionSetupData},
    job::Job,
    liveness_analysis::{
        AccessFlags, BasicBlockId, LivenessData, OpOutputIdx,
        OperatorCallEffect,
    },
    options::operator_base_options::{
        OperatorBaseOptions, OperatorBaseOptionsInterned,
    },
    utils::index_vec::IndexVec,
};

use super::{
    errors::OperatorSetupError,
    operator::{
        OffsetInAggregation, OffsetInChain, Operator, OperatorData,
        OperatorDataId, OperatorId, OperatorName, OperatorOffsetInChain,
        OutputFieldKind, PreboundOutputsMap, TransformInstatiation,
    },
    transform::TransformState,
};

pub struct OpMultiOp {
    pub operations: Vec<(OperatorBaseOptions, OperatorData)>,
    pub sub_op_ids: IndexVec<OffsetInAggregation, OperatorId>,
}

impl Operator for OpMultiOp {
    fn default_name(&self) -> OperatorName {
        "<multi_op>".into()
    }
    fn debug_op_name(&self) -> super::operator::OperatorName {
        let mut res = String::from("multi-op<");
        for (i, (_op_opts, op)) in self.operations.iter().enumerate() {
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
        flags: &mut AccessFlags,
        op_offset_after_last_write: OffsetInChain,
        op_id: OperatorId,
        bb_id: BasicBlockId,
        input_field: OpOutputIdx,
    ) -> Option<(OpOutputIdx, OperatorCallEffect)> {
        let mut next_input = input_field;
        let mut outputs_offset = 0;
        for (agg_offset, &sub_op_id) in self.sub_op_ids.iter_enumerated() {
            // TODO: manage access flags for subsequent ops correctly
            let op = &sess.operator_data[sess.op_data_id(sub_op_id)];
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
            outputs_offset += op.output_count(sess, op_id);
            next_input = output;
            if agg_offset == self.sub_op_ids.last_idx().unwrap() {
                return Some((output, ce));
            }
            assert!(
                ce != OperatorCallEffect::Diverge,
                "non final operator `{}`, index {} inside multi-op (len {}) may not diverge",
                agg_offset , // we already went to the next index
                op.debug_op_name(),
                self.sub_op_ids.len()
            );
        }
        Some((input_field, OperatorCallEffect::NoCall))
    }

    fn build_transforms<'a>(
        &'a self,
        job: &mut Job<'a>,
        tf_state: &mut TransformState,
        _op_id: OperatorId,
        prebound_outputs: &PreboundOutputsMap,
    ) -> TransformInstatiation {
        TransformInstatiation::Multi(job.setup_transforms_for_op_iter(
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
        sess: &SessionData,
        _op_id: OperatorId,
    ) -> super::operator::OutputFieldKind {
        // TODO: this is not correct, if the last one says same as
        let mut ofk = OutputFieldKind::SameAsInput;
        for &op_id in self.sub_op_ids.iter().rev() {
            ofk = sess.operator_data[sess.op_data_id(op_id)]
                .output_field_kind(sess, op_id);
            if ofk != OutputFieldKind::SameAsInput {
                return ofk;
            }
        }
        ofk
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
        chain_id: ChainId,
        offset_in_chain: OperatorOffsetInChain,
        op_base_opts_interned: OperatorBaseOptionsInterned,
        op_data_id: OperatorDataId,
    ) -> Result<OperatorId, OperatorSetupError> {
        let op_id = sess.add_op_from_offset_in_chain(
            chain_id,
            offset_in_chain,
            op_base_opts_interned,
            op_data_id,
        );
        for (op_opts, op_data) in std::mem::take(&mut self.operations) {
            let op_opts =
                op_opts.intern(Some(&op_data), &mut sess.string_store);
            self.sub_op_ids.push(sess.setup_for_op_data(
                chain_id,
                OperatorOffsetInChain::AggregationMember(
                    op_id,
                    self.sub_op_ids.next_idx(),
                ),
                op_opts,
                op_data,
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
            sess.with_mut_op_data(sess.op_data_id(op_id), |sess, op_data| {
                op_data.on_liveness_computed(sess, ld, op_id)
            });
        }
    }
}

pub fn create_multi_op_with_opts(
    ops: impl IntoIterator<Item = (OperatorBaseOptions, OperatorData)>,
) -> OperatorData {
    OperatorData::MultiOp(OpMultiOp {
        operations: ops.into_iter().collect(),
        sub_op_ids: IndexVec::new(),
    })
}

pub fn create_multi_op(
    ops: impl IntoIterator<Item = OperatorData>,
) -> OperatorData {
    create_multi_op_with_opts(
        ops.into_iter()
            .map(|op| (OperatorBaseOptions::default(), op)),
    )
}
