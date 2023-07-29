use std::{cell::Cell, collections::HashMap, sync::Arc};

use bitvec::vec::BitVec;

use crate::{
    chain::Chain,
    context::{ContextData, VentureDescription},
    field_data::{
        iter_hall::{IterHall, IterId},
        iters::FieldIterator,
    },
    job_session::{FieldId, JobData, JobSession},
    liveness_analysis::{LivenessData, LOCAL_SLOTS_PER_BASIC_BLOCK},
    options::argument::CliArgIdx,
    ref_iter::AutoDerefIter,
    utils::identity_hasher::BuildIdentityHasher,
};

use super::{
    errors::{OperatorCreationError, OperatorSetupError},
    operator::{OperatorBase, OperatorData, OperatorId},
    transform::{TransformData, TransformId, TransformState},
    utils::field_access_mappings::FieldAccessMappings,
};

#[derive(Clone, Default)]
pub struct OpForkCat {
    pub subchain_count_before: u32,
    pub subchain_count_after: u32,
    pub accessed_fields: FieldAccessMappings,
    pub accessed_fields_per_subchain: Vec<FieldAccessMappings>,
}

pub struct TfForkCatFieldMapping {
    pub source_iter_id: IterId,
    pub targets_cow: Vec<FieldId>,
    pub targets_non_cow: Vec<FieldId>,
}

pub struct TfForkCat<'a> {
    pub curr_subchain: u32,
    pub curr_target: Option<TransformId>,
    pub current_mappings:
        HashMap<FieldId, TfForkCatFieldMapping, BuildIdentityHasher>,
    pub op: &'a OpForkCat,
}

pub fn parse_op_forkcat(
    value: Option<&[u8]>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    if value.is_some() {
        return Err(OperatorCreationError::new(
            "this operator takes no arguments",
            arg_idx,
        ));
    }
    Ok(OperatorData::ForkCat(OpForkCat::default()))
}

pub fn setup_op_forkcat(
    chain: &Chain,
    op_base: &OperatorBase,
    op: &mut OpForkCat,
    _op_id: OperatorId,
) -> Result<(), OperatorSetupError> {
    if op.subchain_count_after == 0 {
        debug_assert!(
            op_base.offset_in_chain as usize + 1 == chain.operators.len()
        );
        op.subchain_count_after = chain.subchains.len() as u32;
    }
    Ok(())
}

pub fn setup_op_forkcat_liveness_data(
    op: &mut OpForkCat,
    op_id: OperatorId,
    ld: &LivenessData,
) {
    let bb_id = ld.operator_liveness_data[op_id as usize].basic_block_id;
    debug_assert!(ld.basic_blocks[bb_id].calls.len() == 1);
    let bb = &ld.basic_blocks[bb_id];
    let var_count = ld.vars.len();

    let succ_var_data = &ld.var_data[ld.get_succession_var_data_bounds(bb_id)];
    op.accessed_fields = FieldAccessMappings::from_var_data(ld, succ_var_data);

    let mut call = BitVec::<Cell<usize>>::new();
    let mut successors = BitVec::<Cell<usize>>::new();
    call.reserve(var_count * LOCAL_SLOTS_PER_BASIC_BLOCK);
    successors.reserve(var_count * LOCAL_SLOTS_PER_BASIC_BLOCK);
    ld.get_global_var_data_ored(&mut successors, bb.successors.iter());
    for callee_id in &bb.calls {
        call.copy_from_bitslice(ld.get_global_var_data(bb_id));
        ld.apply_bb_aliases(
            &mut call,
            &successors,
            &ld.basic_blocks[*callee_id],
        );
        op.accessed_fields_per_subchain
            .push(FieldAccessMappings::from_var_data(ld, &call));
    }
}

pub fn setup_tf_forkcat<'a>(
    _sess: &mut JobData,
    _op_base: &OperatorBase,
    op: &'a OpForkCat,
    _tf_state: &mut TransformState,
) -> TransformData<'a> {
    TransformData::ForkCat(TfForkCat {
        curr_target: None,
        curr_subchain: op.subchain_count_before,
        current_mappings: Default::default(),
        op,
    })
}

pub fn handle_tf_forkcat(
    sess: &mut JobData,
    tf_id: TransformId,
    fc: &mut TfForkCat,
) {
    let (batch_size, end_of_input) = sess.tf_mgr.claim_batch(tf_id);
    let target_tf = fc.curr_target.unwrap();
    let unconsumed_input =
        sess.tf_mgr.transforms[tf_id].has_unconsumed_input();
    let match_set_mgr = &mut sess.match_set_mgr;
    for (src_field_id, mapping) in fc.current_mappings.iter_mut() {
        sess.field_mgr
            .apply_field_actions(match_set_mgr, *src_field_id);
        let mut i = 0;
        while i < mapping.targets_cow.len() {
            let tgt_id = mapping.targets_cow[i];
            let tgt = &mut sess.field_mgr.fields[tgt_id].borrow();
            if tgt.cow_source.is_none() {
                mapping.targets_cow.push(tgt_id);
                mapping.targets_cow.swap_remove(i);
                continue;
            }
            i += 1;
        }
        if mapping.targets_non_cow.is_empty() {
            continue;
        }
        let src = sess
            .field_mgr
            .borrow_field_cow(*src_field_id, unconsumed_input);
        let mut iter = AutoDerefIter::new(
            &sess.field_mgr,
            *src_field_id,
            sess.field_mgr
                .get_iter_cow_aware(
                    *src_field_id,
                    &src,
                    mapping.source_iter_id,
                )
                .bounded(0, batch_size),
        );
        IterHall::copy_resolve_refs(
            match_set_mgr,
            &mut iter,
            &mut |f: &mut dyn FnMut(&mut IterHall)| {
                for t in &mapping.targets_non_cow {
                    let mut tgt = sess.field_mgr.fields[*t].borrow_mut();
                    f(&mut tgt.field_data);
                }
            },
        );
        sess.field_mgr.store_iter_cow_aware(
            *src_field_id,
            &src,
            mapping.source_iter_id,
            iter.into_base_iter(),
        );
    }

    sess.tf_mgr
        .inform_transform_batch_available(target_tf, batch_size, false);
    if end_of_input {
        sess.tf_mgr.transforms[target_tf].input_is_done = true;
        sess.unlink_transform(tf_id, 0);
    }
}

pub(crate) fn handle_forkcat_expansion(
    _sess: &mut JobSession,
    _tf_id: TransformId,
    _ctx: Option<&Arc<ContextData>>,
) -> Result<(), VentureDescription> {
    todo!();
}

pub fn create_op_forkcat() -> OperatorData {
    OperatorData::ForkCat(OpForkCat::default())
}
