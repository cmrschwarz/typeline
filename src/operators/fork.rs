use std::{
    cell::Cell,
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};

use bitvec::vec::BitVec;

use crate::{
    chain::Chain,
    context::{ContextData, VentureDescription},
    field_data::{
        iter_hall::{IterHall, IterId},
        iters::FieldIterator,
    },
    job_session::{FieldId, JobData, JobSession, MatchSetId},
    liveness_analysis::{
        LivenessData, Var, INVALID_FIELD_NAME, LOCAL_SLOTS_PER_BASIC_BLOCK,
        READS_OFFSET, WRITES_OFFSET,
    },
    options::argument::CliArgIdx,
    ref_iter::AutoDerefIter,
    utils::{
        identity_hasher::BuildIdentityHasher, string_store::StringStoreEntry,
    },
};

use super::{
    errors::{OperatorCreationError, OperatorSetupError},
    operator::{OperatorBase, OperatorData, OperatorId},
    transform::{TransformData, TransformId, TransformState},
};

#[derive(Clone)]
pub struct OpFork {
    // call
    // callcc
    // fork
    // forkcc
    // forkjoin[=merge_col,..] [CC]
    // forkcat [CC]
    pub subchain_count_before: u32,
    pub subchain_count_after: u32,
    pub accessed_fields_per_subchain:
        Vec<HashMap<StringStoreEntry, bool, BuildIdentityHasher>>,
}

pub struct TfForkFieldMapping {
    pub source_iter_id: IterId,
    pub targets_cow: Vec<FieldId>,
    pub targets_non_cow: Vec<FieldId>,
}

pub struct TfFork<'a> {
    pub expanded: bool,
    pub targets: Vec<TransformId>,
    pub mappings: HashMap<FieldId, TfForkFieldMapping, BuildIdentityHasher>,
    pub accessed_fields_per_subchain:
        &'a Vec<HashMap<StringStoreEntry, bool, BuildIdentityHasher>>,
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
        subchain_count_before: 0,
        subchain_count_after: 0,
        accessed_fields_per_subchain: Default::default(),
    }))
}

pub fn setup_op_fork(
    chain: &Chain,
    op_base: &OperatorBase,
    op: &mut OpFork,
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

// TODO: this was accidentally implemented while trying to implement the fork
// version use this once we have forkjoin
pub fn setup_op_forkjoin_liveness_data(
    op: &mut OpFork,
    op_id: OperatorId,
    ld: &LivenessData,
) {
    let bb_id = ld.operator_data[op_id as usize].basic_block_id;
    debug_assert!(ld.basic_blocks[bb_id].calls.len() == 1);
    let bb = &ld.basic_blocks[bb_id];
    let var_count = ld.vars.len();
    let mut call = BitVec::<Cell<usize>>::new();
    let mut successors = BitVec::<Cell<usize>>::new();
    call.reserve(var_count * LOCAL_SLOTS_PER_BASIC_BLOCK);
    successors.reserve(var_count * LOCAL_SLOTS_PER_BASIC_BLOCK);
    ld.get_global_var_data_ored(&mut successors, bb.successors.iter());
    let succ_var_data = &ld.var_data[ld.get_succession_var_data_bounds(bb_id)];
    for (sc_id, callee_id) in bb.calls.iter().enumerate() {
        call.copy_from_bitslice(ld.get_global_var_data(bb_id));
        ld.apply_bb_aliases(
            &mut call,
            &successors,
            &ld.basic_blocks[*callee_id],
        );
        for var_id in call
            [var_count * READS_OFFSET..var_count * (READS_OFFSET + 1)]
            .iter_ones()
        {
            let writes = succ_var_data[var_count * WRITES_OFFSET + var_id];
            match ld.vars[var_id] {
                Var::Named(name) => {
                    op.accessed_fields_per_subchain[sc_id]
                        .insert(name, writes);
                }
                Var::BBInput => {
                    op.accessed_fields_per_subchain[sc_id]
                        .insert(INVALID_FIELD_NAME, writes);
                }
            }
        }
    }
}

pub fn setup_op_fork_liveness_data(
    op: &mut OpFork,
    op_id: OperatorId,
    ld: &LivenessData,
) {
    let bb_id = ld.operator_data[op_id as usize].basic_block_id;
    debug_assert!(ld.basic_blocks[bb_id].calls.len() == 0);
    let bb = &ld.basic_blocks[bb_id];
    let var_count = ld.vars.len();
    op.accessed_fields_per_subchain.resize(
        (op.subchain_count_after - op.subchain_count_before) as usize,
        Default::default(),
    );
    for (sc_id, callee_bb_id) in bb.successors.iter().enumerate() {
        let var_data = ld.get_global_var_data(*callee_bb_id);
        for var_id in var_data
            [var_count * READS_OFFSET..var_count * (READS_OFFSET + 1)]
            .iter_ones()
        {
            let writes = var_data[var_count * WRITES_OFFSET + var_id];
            match ld.vars[var_id] {
                Var::Named(name) => {
                    op.accessed_fields_per_subchain[sc_id]
                        .insert(name, writes);
                }
                Var::BBInput => {
                    op.accessed_fields_per_subchain[sc_id]
                        .insert(INVALID_FIELD_NAME, writes);
                }
            }
        }
    }
}

pub fn setup_tf_fork_as_entry_point<'a, 'b>(
    sess: &mut JobData,
    input_field: FieldId,
    ms_id: MatchSetId,
    entry_count: usize,
    ops: impl Iterator<Item = &'b OperatorId> + Clone,
) -> (TransformState, TransformData<'a>) {
    let mut state = TransformState::new(
        input_field,
        input_field, /* does not have any output field since it terminates
                      * the chain */
        ms_id,
        ops.clone().fold(usize::MAX, |minimum_batch_size, op| {
            let cid = sess.session_data.operator_bases[*op as usize].chain_id;
            minimum_batch_size.min(
                sess.session_data.chains[cid as usize]
                    .settings
                    .default_batch_size,
            )
        }),
        None,
        None,
        sess.tf_mgr.claim_transform_ordering_id(),
    );
    state.available_batch_size = entry_count;
    state.input_is_done = true;
    todo!();
}

pub fn setup_tf_fork<'a>(
    _sess: &mut JobData,
    _op_base: &OperatorBase,
    op: &'a OpFork,
    _tf_state: &mut TransformState,
) -> TransformData<'a> {
    TransformData::Fork(TfFork {
        expanded: false,
        targets: Default::default(),
        mappings: Default::default(),
        accessed_fields_per_subchain: &op.accessed_fields_per_subchain,
    })
}

pub fn handle_tf_fork(
    sess: &mut JobData,
    tf_id: TransformId,
    sp: &mut TfFork,
) {
    let (batch_size, end_of_input) = sess.tf_mgr.claim_batch(tf_id);
    let unconsumed_input =
        sess.tf_mgr.transforms[tf_id].has_unconsumed_input();
    let match_set_mgr = &mut sess.match_set_mgr;
    for (src_field_id, mapping) in sp.mappings.iter_mut() {
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
    for tf in &sp.targets {
        sess.tf_mgr
            .inform_transform_batch_available(*tf, batch_size, false);
    }
    if end_of_input {
        for tf in &sp.targets {
            sess.tf_mgr.transforms[*tf].input_is_done = true;
        }
        sess.unlink_transform(tf_id, 0);
    }
}

pub(crate) fn handle_fork_expansion(
    sess: &mut JobSession,
    tf_id: TransformId,
    _ctx: Option<&Arc<ContextData>>,
) -> Result<(), VentureDescription> {
    // we have to temporarily move the targets out of fork so we can modify
    // sess while accessing them
    let mut targets = Vec::<TransformId>::new();
    let mut mappings =
        HashMap::<FieldId, TfForkFieldMapping, BuildIdentityHasher>::default();
    let tf = &sess.job_data.tf_mgr.transforms[tf_id];
    let fork_input_field_id = tf.input_field;
    let fork_ms_id = tf.match_set_id;
    let fork_op_id = tf.op_id.unwrap() as usize;
    let fork_chain_id = sess.job_data.session_data.operator_bases[fork_op_id]
        .chain_id as usize;

    // by reversing this, the earlier subchains get the higher tf ordering ids
    // -> get executed first
    for i in (0..sess.job_data.session_data.chains[fork_chain_id]
        .subchains
        .len())
        .rev()
    {
        let subchain_id = sess.job_data.session_data.chains[fork_chain_id]
            .subchains[i] as usize;
        let target_ms_id = sess.job_data.match_set_mgr.add_match_set();
        let match_sets = &mut sess.job_data.match_set_mgr.match_sets;
        let (fork_match_set, target_match_set) =
            match_sets.two_distinct_mut(fork_ms_id, target_ms_id);
        let accessed_fields_map = if let TransformData::Fork(f) =
            &sess.transform_data[tf_id.get()]
        {
            &f.accessed_fields_per_subchain[i]
        } else {
            unreachable!();
        };
        let mut chain_input_field_id = fork_input_field_id;
        for (name, writes) in accessed_fields_map {
            match target_match_set.field_name_map.entry(*name) {
                std::collections::hash_map::Entry::Occupied(_) => continue,
                std::collections::hash_map::Entry::Vacant(e) => {
                    let src_field_id = if let Some(src_field_id) =
                        fork_match_set.field_name_map.get(name)
                    {
                        *src_field_id
                    } else if *name == INVALID_FIELD_NAME {
                        fork_input_field_id
                    } else {
                        let target_field_id = sess
                            .job_data
                            .field_mgr
                            .add_field(target_ms_id, None);
                        let mut tgt = sess.job_data.field_mgr.fields
                            [target_field_id]
                            .borrow_mut();
                        tgt.added_as_placeholder_by_tf = Some(tf_id);
                        e.insert(target_field_id);
                        continue;
                    };
                    let mut src_field = sess.job_data.field_mgr.fields
                        [src_field_id]
                        .borrow_mut();
                    let mut any_writes = *writes;
                    if any_writes == false {
                        for other_name in &src_field.names {
                            if name == other_name {
                                continue;
                            }
                            if let Some(true) =
                                accessed_fields_map.get(other_name)
                            {
                                any_writes = true;
                                break;
                            }
                        }
                    }

                    let target_field_id = if any_writes {
                        drop(src_field);
                        let target = sess
                            .job_data
                            .field_mgr
                            .add_field(target_ms_id, None);
                        src_field = sess.job_data.field_mgr.fields
                            [src_field_id]
                            .borrow_mut();
                        match mappings.entry(src_field_id) {
                            Entry::Occupied(ref mut e) => {
                                e.get_mut().targets_cow.push(target);
                            }
                            Entry::Vacant(e) => {
                                e.insert(TfForkFieldMapping {
                                    source_iter_id: src_field
                                        .field_data
                                        .claim_iter(),
                                    targets_cow: vec![target],
                                    targets_non_cow: Vec::new(),
                                });
                            }
                        }
                        target
                    } else {
                        src_field_id
                    };
                    let mut tgt_field = if any_writes {
                        let mut tgt = sess.job_data.field_mgr.fields
                            [target_field_id]
                            .borrow_mut();
                        if *name != INVALID_FIELD_NAME {
                            tgt.names.push(*name);
                        }
                        tgt.cow_source = Some(src_field_id);
                        src_field.ref_count += 1;
                        Some(tgt)
                    } else {
                        None
                    };
                    e.insert(target_field_id);
                    for other_name in &src_field.names {
                        if name == other_name {
                            continue;
                        }
                        if accessed_fields_map.contains_key(other_name) {
                            target_match_set
                                .field_name_map
                                .insert(*other_name, target_field_id);
                            tgt_field
                                .as_mut()
                                .map(|f| f.names.push(*other_name));
                        }
                    }
                    if *name == INVALID_FIELD_NAME {
                        chain_input_field_id = target_field_id;
                    }
                }
            }
        }
        target_match_set.field_name_map.remove(&INVALID_FIELD_NAME);
        let start_op =
            sess.job_data.session_data.chains[subchain_id].operators[0];
        let (start_tf, end_tf, end_reachable) = sess.setup_transforms_from_op(
            target_ms_id,
            start_op,
            chain_input_field_id,
        );
        if end_reachable {
            sess.add_terminator(end_tf);
        }
        targets.push(start_tf);
    }
    if let TransformData::Fork(ref mut fork) =
        sess.transform_data[usize::from(tf_id)]
    {
        fork.targets = targets;
        fork.mappings = mappings;
        fork.expanded = true;
    } else {
        unreachable!();
    }
    Ok(())
}

pub fn create_op_fork() -> OperatorData {
    OperatorData::Fork(OpFork {
        subchain_count_before: 0,
        subchain_count_after: 0,
        accessed_fields_per_subchain: Default::default(),
    })
}
