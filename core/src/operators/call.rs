use std::{any::Any, collections::HashMap, sync::Arc};

use crate::{
    chain::ChainId,
    cli::call_expr::{CallExpr, Span},
    context::{ContextData, VentureDescription},
    job::{Job, JobData},
    liveness_analysis::OperatorCallEffect,
    options::session_setup::SessionSetupData,
    record_data::{
        field::FieldId, group_track::GroupTrackId, match_set::MatchSetId,
    },
    scr_error::ScrError,
    utils::indexing_type::IndexingType,
};

use super::{
    errors::{OperatorCreationError, OperatorSetupError},
    operator::{
        OffsetInChain, Operator, OperatorDataId, OperatorId,
        OperatorInstantiation, OperatorOffsetInChain, TransformInstatiation,
    },
    transform::{Transform, TransformData, TransformId, TransformState},
};

#[derive(Clone)]
pub struct OpCall {
    pub lazy: bool,
    pub target_name: String,
    pub target_resolved: Option<ChainId>,
}
pub struct TfCall {
    pub target: ChainId,
}

impl Operator for OpCall {
    fn setup(
        &mut self,
        sess: &mut SessionSetupData,
        op_data_id: OperatorDataId,
        chain_id: ChainId,
        offset_in_chain: OperatorOffsetInChain,
        span: Span,
    ) -> Result<OperatorId, ScrError> {
        let op_id = sess.add_op(op_data_id, chain_id, offset_in_chain, span);

        if self.target_resolved.is_some() {
            // this happens in case of call targets caused by labels ending the
            // chain
            debug_assert!(!self.lazy);
        } else if let Some(target) = sess
            .string_store
            .lookup_str(&self.target_name)
            .and_then(|sse| sess.chain_labels.get(&sse))
        {
            self.target_resolved = Some(*target);
        } else {
            return Err(OperatorSetupError::new_s(
                format!("unknown chain label '{}'", self.target_name),
                op_id,
            )
            .into());
        }

        Ok(op_id)
    }

    fn default_name(&self) -> super::operator::OperatorName {
        "call".into()
    }

    fn output_count(
        &self,
        _sess: &crate::context::SessionData,
        _op_id: OperatorId,
    ) -> usize {
        0
    }

    fn has_dynamic_outputs(
        &self,
        _sess: &crate::context::SessionData,
        _op_id: OperatorId,
    ) -> bool {
        false
    }

    fn output_field_kind(
        &self,
        _sess: &crate::context::SessionData,
        _op_id: OperatorId,
    ) -> super::operator::OutputFieldKind {
        super::operator::OutputFieldKind::Unconfigured
    }

    fn update_variable_liveness(
        &self,
        _sess: &crate::context::SessionData,
        _ld: &mut crate::liveness_analysis::LivenessData,
        _op_offset_after_last_write: OffsetInChain,
        _op_id: OperatorId,
        _bb_id: crate::liveness_analysis::BasicBlockId,
        _input_field: crate::liveness_analysis::OpOutputIdx,
        output: &mut crate::liveness_analysis::OperatorLivenessOutput,
    ) {
        output.call_effect = OperatorCallEffect::Diverge;
    }

    fn update_bb_for_op(
        &self,
        sess: &crate::context::SessionData,
        ld: &mut crate::liveness_analysis::LivenessData,
        _op_id: OperatorId,
        op_n: OffsetInChain,
        _cn: &crate::chain::Chain,
        bb_id: crate::liveness_analysis::BasicBlockId,
    ) -> bool {
        ld.basic_blocks[bb_id]
            .calls
            .push(self.target_resolved.unwrap().into_bb_id());
        ld.split_bb_at_call(sess, bb_id, op_n);
        true
    }

    fn build_transforms<'a>(
        &'a self,
        _job: &mut Job<'a>,
        _tf_state: &mut TransformState,
        _op_id: OperatorId,
        _prebound_outputs: &super::operator::PreboundOutputsMap,
    ) -> TransformInstatiation<'a> {
        TransformInstatiation::Single(TransformData::from_custom(TfCall {
            target: self.target_resolved.unwrap(),
        }))
    }

    fn as_any(&self) -> Option<&dyn Any> {
        Some(self)
    }
    fn as_any_mut(&mut self) -> Option<&mut dyn Any> {
        Some(self)
    }
}

pub(crate) fn handle_eager_call_expansion(
    op: &OpCall,
    sess: &mut Job,
    ms_id: MatchSetId,
    input_field: FieldId,
    group_track: GroupTrackId,
    predecessor_tf: Option<TransformId>,
) -> OperatorInstantiation {
    let chain =
        &sess.job_data.session_data.chains[op.target_resolved.unwrap()];
    sess.setup_transforms_from_op(
        ms_id,
        chain.operators[OffsetInChain::zero()],
        input_field,
        group_track,
        predecessor_tf,
        &HashMap::default(),
    )
}

impl<'a> Transform<'a> for TfCall {
    fn update(&mut self, _jd: &mut JobData<'a>, _tf_id: TransformId) {
        unreachable!()
    }
    fn pre_update_required(&self) -> bool {
        true
    }
    fn pre_update(
        &mut self,
        _ctx: Option<&Arc<ContextData>>,
        job: &mut Job<'a>,
        tf_id: TransformId,
    ) -> Result<(), VentureDescription> {
        let tf = &mut job.job_data.tf_mgr.transforms[tf_id];
        let old_successor = tf.successor;
        let input_field = tf.input_field;
        let input_group_track = tf.input_group_track_id;
        let ms_id = tf.match_set_id;
        let call = job.transform_data[tf_id].downcast_ref::<TfCall>().unwrap();
        // TODO: do we need a prebound output so succesor can keep it's input
        // field?
        let instantiation = job.setup_transforms_from_op(
            ms_id,
            job.job_data.session_data.chains[call.target].operators
                [OffsetInChain::zero()],
            input_field,
            input_group_track,
            Some(tf_id),
            &HashMap::default(),
        );
        job.job_data.tf_mgr.transforms[instantiation.tfs_end].successor =
            old_successor;
        let (batch_size, _input_done) = job.job_data.tf_mgr.claim_all(tf_id);
        // TODO: is this fine considering e.g. forkcat with no predecessors?
        job.job_data.unlink_transform(tf_id, batch_size);
        Ok(())
    }
    fn as_any(&self) -> Option<&dyn Any> {
        Some(self)
    }
    fn as_any_mut(&mut self) -> Option<&mut dyn Any> {
        Some(self)
    }
}

pub fn parse_op_call(
    expr: &CallExpr,
) -> Result<Box<dyn Operator>, OperatorCreationError> {
    let target = expr.require_single_string_arg()?;
    Ok(Box::new(OpCall {
        lazy: true,
        target_name: target.to_owned(),
        target_resolved: None,
    }))
}

pub fn create_op_call(name: String) -> Box<dyn Operator> {
    Box::new(OpCall {
        lazy: true,
        target_name: name,
        target_resolved: None,
    })
}

pub fn create_op_call_eager(target: ChainId) -> Box<dyn Operator> {
    Box::new(OpCall {
        lazy: false,
        target_name: String::new(),
        target_resolved: Some(target),
    })
}
