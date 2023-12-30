use std::sync::Arc;

use smallvec::SmallVec;

use crate::{
    chain::Chain,
    context::ContextData,
    job_session::{JobData, JobSession},
    liveness_analysis::LivenessData,
    options::argument::CliArgIdx,
    record_data::{
        field::{FieldId, VOID_FIELD_ID},
        iter_hall::IterId,
    },
};

use super::{
    errors::{OperatorCreationError, OperatorSetupError},
    operator::{OperatorBase, OperatorData, OperatorId},
    transform::{TransformData, TransformId, TransformState},
    utils::field_access_mappings::FieldAccessMappings,
};

#[derive(Clone)]
pub struct OpFork {
    // call
    // callcc
    // fork
    // forkcc
    // forkjoin[=merge_col,..] [CC]
    // forkcat [CC]
    pub subchains_start: u32,
    pub subchains_end: u32,
    pub accessed_fields_per_subchain: Vec<FieldAccessMappings>,
}

pub struct TfForkFieldMapping {
    pub source_iter_id: IterId,
    pub targets_cow: SmallVec<[FieldId; 4]>,
    pub targets_data_cow: SmallVec<[FieldId; 4]>,
    pub targets_copy: SmallVec<[FieldId; 4]>,
}

pub struct TfFork<'a> {
    pub expanded: bool,
    pub targets: Vec<TransformId>,
    pub accessed_fields_per_subchain: &'a Vec<FieldAccessMappings>,
}

pub fn parse_op_fork(
    value: Option<&[u8]>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    if value.is_some() {
        return Err(OperatorCreationError::new(
            "this operator takes no arguments",
            arg_idx,
        ));
    }
    Ok(OperatorData::Fork(OpFork {
        subchains_start: 0,
        subchains_end: 0,
        accessed_fields_per_subchain: Default::default(),
    }))
}

pub fn setup_op_fork(
    chain: &Chain,
    op_base: &OperatorBase,
    op: &mut OpFork,
    _op_id: OperatorId,
) -> Result<(), OperatorSetupError> {
    if op.subchains_end == 0 {
        //TODO: this can happen for ContextBuilder::run_collect
        //throw a decent error instead
        debug_assert!(
            op_base.offset_in_chain as usize + 1 == chain.operators.len()
        );
        op.subchains_end = chain.subchains.len() as u32;
    }
    Ok(())
}

pub fn setup_op_fork_liveness_data(
    op: &mut OpFork,
    op_id: OperatorId,
    ld: &LivenessData,
) {
    let bb_id = ld.operator_liveness_data[op_id as usize].basic_block_id;
    debug_assert!(ld.basic_blocks[bb_id].calls.is_empty());
    let bb = &ld.basic_blocks[bb_id];
    for callee_bb_id in bb.successors.iter() {
        op.accessed_fields_per_subchain.push(
            FieldAccessMappings::from_var_data(
                &mut (),
                ld,
                ld.get_global_var_data(*callee_bb_id),
            ),
        );
    }
}

pub fn build_tf_fork<'a>(
    _sess: &mut JobData,
    _op_base: &OperatorBase,
    op: &'a OpFork,
    _tf_state: &mut TransformState,
) -> TransformData<'a> {
    TransformData::Fork(TfFork {
        expanded: false,
        targets: Default::default(),
        accessed_fields_per_subchain: &op.accessed_fields_per_subchain,
    })
}

pub fn handle_tf_fork(
    sess: &mut JobData,
    tf_id: TransformId,
    sp: &mut TfFork,
) {
    let (batch_size, ps) = sess.tf_mgr.claim_all(tf_id);
    if ps.input_done {
        sess.tf_mgr.declare_transform_done(tf_id);
    }
    if ps.next_batch_ready {
        sess.tf_mgr.push_tf_in_ready_stack(tf_id);
    }
    // we reverse to make sure that the first subchain ends up
    // on top of the stack and gets executed first
    for &tf in sp.targets.iter().rev() {
        sess.tf_mgr.inform_transform_batch_available(
            tf,
            batch_size,
            ps.input_done,
        );
    }
}

pub(crate) fn handle_fork_expansion(
    sess: &mut JobSession,
    tf_id: TransformId,
    _ctx: Option<&Arc<ContextData>>,
) {
    // we have to temporarily move the targets out of fork so we can modify
    // sess while accessing them
    let mut targets = Vec::<TransformId>::new();

    let tf = &sess.job_data.tf_mgr.transforms[tf_id];
    let fork_input_field_id = tf.input_field;
    let fork_ms_id = tf.match_set_id;
    let fork_op_id = tf.op_id.unwrap() as usize;
    let fork_chain_id = sess.job_data.session_data.operator_bases[fork_op_id]
        .chain_id
        .unwrap() as usize;

    for i in 0..sess.job_data.session_data.chains[fork_chain_id]
        .subchains
        .len()
    {
        let subchain_id = sess.job_data.session_data.chains[fork_chain_id]
            .subchains[i] as usize;
        let target_ms_id = sess.job_data.match_set_mgr.add_match_set();
        let field_access_mapping = if let TransformData::Fork(f) =
            &sess.transform_data[tf_id.get()]
        {
            &f.accessed_fields_per_subchain[i]
        } else {
            unreachable!();
        };
        let mut chain_input_field = None;
        for (name, _fam) in field_access_mapping.iter_name_opt() {
            let src_field_id;
            if let Some(name) = name {
                if let Some(field) = sess.job_data.match_set_mgr.match_sets
                    [fork_ms_id]
                    .field_name_map
                    .get(&name)
                {
                    // the input field is always first in this iterator
                    debug_assert!(*field != fork_input_field_id);
                    src_field_id = *field;
                } else {
                    continue;
                };
            } else {
                debug_assert!(chain_input_field.is_none());
                src_field_id = fork_input_field_id;
            };

            let mut src_field =
                sess.job_data.field_mgr.fields[src_field_id].borrow_mut();

            drop(src_field);
            let target_field_id =
                sess.job_data.field_mgr.get_cross_ms_cow_field(
                    &mut sess.job_data.match_set_mgr,
                    target_ms_id,
                    src_field_id,
                );
            src_field =
                sess.job_data.field_mgr.fields[src_field_id].borrow_mut();

            if name.is_none() {
                chain_input_field = Some(target_field_id);
            }
            drop(src_field);
        }
        let input_field = chain_input_field.unwrap_or(VOID_FIELD_ID);
        let start_op_id =
            sess.job_data.session_data.chains[subchain_id].operators[0];
        let (start_tf, _end_tf) = sess.setup_transforms_from_op(
            target_ms_id,
            start_op_id,
            input_field,
            None,
            &Default::default(),
        );
        targets.push(start_tf);
    }
    sess.log_state("expanded fork");
    if let TransformData::Fork(ref mut fork) =
        sess.transform_data[usize::from(tf_id)]
    {
        fork.targets = targets;
        fork.expanded = true;
    } else {
        unreachable!();
    }
}

pub fn create_op_fork() -> OperatorData {
    OperatorData::Fork(OpFork {
        subchains_start: 0,
        subchains_end: 0,
        accessed_fields_per_subchain: Default::default(),
    })
}
