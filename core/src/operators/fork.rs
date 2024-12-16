use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use smallvec::SmallVec;

use crate::{
    chain::{ChainId, SubchainIndex},
    cli::call_expr::{Argument, CallExpr, Span},
    context::ContextData,
    job::{Job, JobData},
    liveness_analysis::{
        LivenessData, OperatorCallEffect, VarId, VarLivenessSlotGroup,
        VarLivenessSlotKind,
    },
    operators::operator::OffsetInChain,
    options::session_setup::SessionSetupData,
    record_data::{
        action_buffer::{ActorId, ActorRef},
        field::FieldId,
        group_track::GroupTrackId,
        iter_hall::FieldIterId,
        match_set::MatchSetId,
    },
    scr_error::ScrError,
    utils::{
        index_vec::IndexVec,
        indexing_type::{IndexingType, IndexingTypeRange},
        string_store::StringStoreEntry,
    },
};

use super::{
    errors::{OperatorCreationError, OperatorSetupError},
    operator::{Operator, OperatorDataId, OperatorId, OperatorOffsetInChain},
    terminator::add_terminator,
    transform::{Transform, TransformData, TransformId, TransformState},
};

pub struct OpFork {
    pub prebound_ops: Vec<Vec<(Box<dyn Operator>, Span)>>,
    pub arguments: Vec<Vec<Argument>>,

    pub subchains_start: SubchainIndex,
    pub subchains_end: SubchainIndex,
    pub accessed_fields_per_subchain:
        IndexVec<SubchainIndex, HashSet<Option<StringStoreEntry>>>,
}

pub struct TfForkFieldMapping {
    pub source_iter_id: FieldIterId,
    pub targets_cow: SmallVec<[FieldId; 4]>,
    pub targets_data_cow: SmallVec<[FieldId; 4]>,
    pub targets_copy: SmallVec<[FieldId; 4]>,
}

pub struct ForkTarget {
    pub tf_id: TransformId,
    pub gt_id: GroupTrackId,
}

pub struct TfFork {
    pub expanded: bool,
    pub targets: Vec<ForkTarget>,
}

pub fn create_op_fork_with_spans(
    subchains: Vec<Vec<(Box<dyn Operator>, Span)>>,
) -> Result<Box<dyn Operator>, OperatorCreationError> {
    Ok(Box::new(OpFork {
        subchains_start: SubchainIndex::MAX_VALUE,
        subchains_end: SubchainIndex::MAX_VALUE,
        accessed_fields_per_subchain: IndexVec::new(),
        prebound_ops: subchains,
        arguments: Vec::new(),
    }))
}

pub fn create_op_fork(
    subchains: impl IntoIterator<
        Item = impl IntoIterator<Item = Box<dyn Operator>>,
    >,
) -> Result<Box<dyn Operator>, OperatorCreationError> {
    let subchains = subchains
        .into_iter()
        .map(|it| it.into_iter().map(|v| (v, Span::Generated)).collect())
        .collect();
    create_op_fork_with_spans(subchains)
}

pub fn parse_op_fork(
    mut arg: Argument,
) -> Result<Box<dyn Operator>, ScrError> {
    let mut subchains = Vec::new();
    let mut curr_subchain = Vec::new();

    let sub_args = arg.expect_arg_array_mut()?;

    for arg in sub_args.drain(1..) {
        let expr = CallExpr::from_argument(&arg)?;
        if expr.op_name == "next" {
            expr.reject_args()?;
            subchains.push(curr_subchain);
            curr_subchain = Vec::new();
            continue;
        };
        curr_subchain.push(arg);
    }

    subchains.push(curr_subchain);
    Ok(Box::new(OpFork {
        subchains_start: SubchainIndex::MAX_VALUE,
        subchains_end: SubchainIndex::MAX_VALUE,
        accessed_fields_per_subchain: IndexVec::new(),
        prebound_ops: Vec::new(),
        arguments: subchains,
    }))
}

impl Operator for OpFork {
    fn default_name(&self) -> super::operator::OperatorName {
        "fork".into()
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
        super::operator::OutputFieldKind::SameAsInput
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

        if !matches!(offset_in_chain, OperatorOffsetInChain::Direct(_)) {
            return Err(OperatorSetupError::new(
                "operator `fork` cannot be part of an aggregation",
                op_id,
            )
            .into());
        };

        self.subchains_start = sess.chains[chain_id].subchains.next_idx();

        debug_assert!(
            self.prebound_ops.is_empty() || self.arguments.is_empty()
        );

        for sc in std::mem::take(&mut self.prebound_ops) {
            sess.setup_subchain(chain_id, sc)?;
        }

        for args in std::mem::take(&mut self.arguments) {
            sess.setup_subchain_from_args(chain_id, args)?;
        }

        self.subchains_end = sess.chains[chain_id].subchains.next_idx();

        Ok(op_id)
    }

    fn update_variable_liveness(
        &self,
        _sess: &crate::context::SessionData,
        _ld: &mut LivenessData,
        _op_offset_after_last_write: OffsetInChain,
        _op_id: OperatorId,
        _bb_id: crate::liveness_analysis::BasicBlockId,
        _input_field: crate::liveness_analysis::OpOutputIdx,
        output: &mut crate::liveness_analysis::OperatorLivenessOutput,
    ) {
        output.call_effect = OperatorCallEffect::Diverge;
    }

    fn on_liveness_computed(
        &mut self,
        _sess: &mut crate::context::SessionData,
        ld: &LivenessData,
        op_id: OperatorId,
    ) {
        let bb_id = ld.operator_liveness_data[op_id].basic_block_id;
        debug_assert!(ld.basic_blocks[bb_id].calls.is_empty());
        let bb = &ld.basic_blocks[bb_id];
        for &callee_bb_id in &bb.successors {
            let mut accessed_vars = HashSet::new();
            for var_id in ld
                .get_var_liveness_slot(
                    callee_bb_id,
                    VarLivenessSlotGroup::Global,
                    VarLivenessSlotKind::Reads,
                )
                .iter_ones()
                .map(VarId::from_usize)
            {
                accessed_vars.insert(ld.vars[var_id].get_name());
            }
            self.accessed_fields_per_subchain.push(accessed_vars);
        }
    }

    fn build_transforms<'a>(
        &'a self,
        _job: &mut Job<'a>,
        _tf_state: &mut TransformState,
        _op_id: OperatorId,
        _prebound_outputs: &super::operator::PreboundOutputsMap,
    ) -> super::operator::TransformInstatiation<'a> {
        super::operator::TransformInstatiation::Single(
            TransformData::from_custom(TfFork {
                expanded: false,
                targets: Vec::new(),
            }),
        )
    }

    fn update_bb_for_op(
        &self,
        _sess: &crate::context::SessionData,
        ld: &mut LivenessData,
        _op_id: OperatorId,
        _op_n: OffsetInChain,
        cn: &crate::chain::Chain,
        bb_id: crate::liveness_analysis::BasicBlockId,
    ) -> bool {
        let bb = &mut ld.basic_blocks[bb_id];
        for sc in &cn.subchains[self.subchains_start..self.subchains_end] {
            bb.successors.push(sc.into_bb_id());
        }
        true
    }

    fn as_any(&self) -> Option<&dyn std::any::Any> {
        Some(self)
    }
    fn as_any_mut(&mut self) -> Option<&mut dyn std::any::Any> {
        Some(self)
    }
}

fn setup_fork_subchain(
    job: &mut Job,
    fork_chain_id: ChainId,
    subchain_index: SubchainIndex,
    tf_id: TransformId,
    fork_ms_id: MatchSetId,
    fork_input_field_id: FieldId,
) -> ForkTarget {
    // actual chain id as opposed to the index to the nth subchain
    let subchain_id = job.job_data.session_data.chains[fork_chain_id]
        .subchains[subchain_index];

    let fork_ms_scope =
        job.job_data.match_set_mgr.match_sets[fork_ms_id].active_scope;

    let sc_scope_id = job.job_data.scope_mgr.add_scope(Some(fork_ms_scope));
    let target_ms_id = job.job_data.match_set_mgr.add_match_set(
        &mut job.job_data.field_mgr,
        &mut job.job_data.scope_mgr,
        sc_scope_id,
    );

    let fork_chain_dummy_field =
        job.job_data.match_set_mgr.get_dummy_field(fork_ms_id);
    let sc_dummy_field =
        job.job_data.match_set_mgr.get_dummy_field(target_ms_id);

    job.job_data.field_mgr.setup_cow_between_fields(
        &mut job.job_data.match_set_mgr,
        fork_chain_dummy_field,
        sc_dummy_field,
    );

    let target_group_track = job.job_data.group_track_manager.add_group_track(
        &job.job_data.match_set_mgr,
        None,
        target_ms_id,
        ActorRef::Unconfirmed(ActorId::new(0)),
    );

    let op_id = job.job_data.tf_mgr.transforms[tf_id].op_id.unwrap();
    let op = job.job_data.session_data.operator_data
        [job.job_data.session_data.op_data_id(op_id)]
    .downcast_ref::<OpFork>()
    .unwrap();
    let field_access_mapping =
        &op.accessed_fields_per_subchain[subchain_index];
    let mut chain_input_field = None;
    let fork_ms_scope =
        job.job_data.match_set_mgr.match_sets[fork_ms_id].active_scope;
    for &name in field_access_mapping {
        let src_field_id;
        if let Some(name) = name {
            if let Some(field) =
                job.job_data.scope_mgr.lookup_field(fork_ms_scope, name)
            {
                // the input field is always first in this iterator
                debug_assert!(field != fork_input_field_id);
                src_field_id = field;
            } else {
                continue;
            };
        } else {
            debug_assert!(chain_input_field.is_none());
            src_field_id = fork_input_field_id;
        };

        let mut src_field =
            job.job_data.field_mgr.fields[src_field_id].borrow_mut();

        drop(src_field);
        let target_field_id = job.job_data.field_mgr.get_cross_ms_cow_field(
            &mut job.job_data.match_set_mgr,
            target_ms_id,
            src_field_id,
        );
        job.job_data.scope_mgr.insert_field_name_opt(
            sc_scope_id,
            name,
            target_field_id,
        );
        src_field = job.job_data.field_mgr.fields[src_field_id].borrow_mut();

        if name.is_none() {
            chain_input_field = Some(target_field_id);
        }
        drop(src_field);
    }
    let input_field = chain_input_field.unwrap_or(sc_dummy_field);
    let start_op_id = job.job_data.session_data.chains[subchain_id].operators
        [OffsetInChain::zero()];
    let instantiation = job.setup_transforms_from_op(
        target_ms_id,
        start_op_id,
        input_field,
        target_group_track,
        None,
        &HashMap::default(),
    );
    if !cfg!(feature = "debug_disable_terminator") {
        add_terminator(job, instantiation.tfs_end);
    }
    ForkTarget {
        tf_id: instantiation.tfs_begin,
        gt_id: target_group_track,
    }
}

impl<'a> Transform<'a> for TfFork {
    fn pre_update_required(&self) -> bool {
        !self.expanded
    }
    fn pre_update(
        &mut self,
        _ctx: Option<&Arc<ContextData>>,
        job: &mut Job<'a>,
        tf_id: TransformId,
    ) -> Result<(), crate::context::VentureDescription> {
        // we have to temporarily move the targets out of fork so we can modify
        // sess while accessing them
        let mut targets = Vec::new();

        let tf = &job.job_data.tf_mgr.transforms[tf_id];
        let fork_input_field_id = tf.input_field;
        let fork_ms_id = tf.match_set_id;
        let fork_op_id = tf.op_id.unwrap();
        let fork_chain_id =
            job.job_data.session_data.operator_bases[fork_op_id].chain_id;

        for i in IndexingTypeRange::new(
            SubchainIndex::zero()
                ..job.job_data.session_data.chains[fork_chain_id]
                    .subchains
                    .next_idx(),
        ) {
            let target = setup_fork_subchain(
                job,
                fork_chain_id,
                i,
                tf_id,
                fork_ms_id,
                fork_input_field_id,
            );
            targets.push(target);
        }

        job.log_state("expanded fork");
        self.targets = targets;
        self.expanded = true;
        Ok(())
    }
    fn update(&mut self, jd: &mut JobData<'a>, tf_id: TransformId) {
        let (batch_size, ps) = jd.tf_mgr.claim_all(tf_id);
        if ps.input_done {
            jd.tf_mgr.declare_transform_done(tf_id);
        }
        if ps.next_batch_ready {
            jd.tf_mgr.push_tf_in_ready_stack(tf_id);
        }

        let tf = &jd.tf_mgr.transforms[tf_id];

        jd.group_track_manager.propagate_leading_groups_to_aliases(
            &jd.match_set_mgr,
            tf.input_group_track_id,
            batch_size,
            ps.input_done,
            true,
            self.targets.iter().map(|tgt| tgt.gt_id),
        );

        // we reverse to make sure that the first subchain ends up
        // on top of the stack and gets executed first
        for tgt in self.targets.iter().rev() {
            jd.match_set_mgr.advance_cross_ms_cow_targets(
                &jd.field_mgr,
                None,
                jd.tf_mgr.transforms[tgt.tf_id].match_set_id,
                batch_size,
            );
            jd.tf_mgr.inform_transform_batch_available(
                tgt.tf_id,
                batch_size,
                ps.input_done,
            );
        }
    }

    fn as_any(&self) -> Option<&dyn std::any::Any> {
        Some(self)
    }
    fn as_any_mut(&mut self) -> Option<&mut dyn std::any::Any> {
        Some(self)
    }
}
