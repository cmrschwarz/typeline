use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};

use crate::{
    chain::Chain,
    context::ContextData,
    job_session::{JobData, JobSession},
    liveness_analysis::LivenessData,
    options::argument::CliArgIdx,
    record_data::{
        field::{FieldId, DUMMY_INPUT_FIELD_ID},
        iter_hall::{IterHall, IterId},
        iters::FieldIterator,
        ref_iter::AutoDerefIter,
    },
    utils::identity_hasher::BuildIdentityHasher,
};

use super::{
    errors::{OperatorCreationError, OperatorSetupError},
    nop::setup_tf_nop,
    operator::{OperatorBase, OperatorData, OperatorId},
    transform::{TransformData, TransformId, TransformState},
    utils::field_access_mappings::{FieldAccessMappings, FieldAccessMode},
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
    pub targets_copy: Vec<FieldId>,
    pub targets_cow: Vec<FieldId>,
}

pub struct TfFork<'a> {
    pub expanded: bool,
    pub targets: Vec<TransformId>,
    pub mappings: HashMap<FieldId, TfForkFieldMapping, BuildIdentityHasher>,
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
                (),
                ld,
                ld.get_global_var_data(*callee_bb_id),
            ),
        );
    }
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
            debug_assert!(tgt.get_clear_delay_request_count() == 0);
            i += 1;
        }
        sess.tf_mgr.prepare_for_output(
            &sess.field_mgr,
            match_set_mgr,
            tf_id,
            mapping.targets_cow.iter().copied(),
        );
        sess.tf_mgr.prepare_for_output(
            &sess.field_mgr,
            match_set_mgr,
            tf_id,
            mapping.targets_copy.iter().copied(),
        );
        if mapping.targets_copy.is_empty() {
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
                for t in &mapping.targets_copy {
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
        sess.tf_mgr.inform_transform_batch_available(
            *tf,
            batch_size,
            unconsumed_input,
        );
    }
    if end_of_input {
        for tf in &sp.targets {
            sess.tf_mgr.push_tf_in_ready_queue(*tf);
        }
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
) {
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
        .chain_id
        .unwrap() as usize;

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
        let field_access_mapping = if let TransformData::Fork(f) =
            &sess.transform_data[tf_id.get()]
        {
            &f.accessed_fields_per_subchain[i]
        } else {
            unreachable!();
        };
        let mut chain_input_field = None;
        for (name, writes) in field_access_mapping.iter_name_opt() {
            let src_field_id;
            let mut entry;
            if let Some(name) = name {
                let vacant = match target_match_set.field_name_map.entry(name)
                {
                    Entry::Occupied(_) => continue,
                    Entry::Vacant(e) => e,
                };
                if let Some(field) = fork_match_set.field_name_map.get(&name) {
                    // the input field is always first in this iterator
                    debug_assert!(*field != fork_input_field_id);
                    src_field_id = *field;
                } else {
                    let target_field_id =
                        sess.job_data.field_mgr.add_field(target_ms_id, None);
                    //let mut tgt = sess.job_data.field_mgr.fields
                    //    [target_field_id]
                    //    .borrow_mut();
                    // tgt.added_as_placeholder_by_tf = Some(tf_id);
                    vacant.insert(target_field_id);
                    continue;
                };
                entry = Some(vacant);
            } else {
                if chain_input_field.is_some() {
                    continue;
                }
                src_field_id = fork_input_field_id;
                entry = None;
            };

            let mut src_field =
                sess.job_data.field_mgr.fields[src_field_id].borrow_mut();
            // TODO: handle WriteData
            let mut any_writes = writes != FieldAccessMode::Read;
            if !any_writes {
                for other_name in &src_field.names {
                    if name == Some(*other_name) {
                        continue;
                    }
                    if field_access_mapping
                        .fields
                        .get(other_name)
                        .unwrap_or(&FieldAccessMode::Read)
                        != &FieldAccessMode::Read
                    {
                        any_writes = true;
                        break;
                    }
                }
            }

            let target_field_id = if any_writes {
                drop(src_field);
                let target =
                    sess.job_data.field_mgr.add_field(target_ms_id, None);
                src_field =
                    sess.job_data.field_mgr.fields[src_field_id].borrow_mut();
                match mappings.entry(src_field_id) {
                    Entry::Occupied(ref mut e) => {
                        e.get_mut().targets_cow.push(target);
                    }
                    Entry::Vacant(e) => {
                        e.insert(TfForkFieldMapping {
                            source_iter_id: src_field.field_data.claim_iter(),
                            targets_cow: vec![target],
                            targets_copy: Vec::new(),
                        });
                    }
                }
                target
            } else {
                src_field_id
            };
            let mut tgt_field = if any_writes {
                let mut tgt = sess.job_data.field_mgr.fields[target_field_id]
                    .borrow_mut();
                if let Some(name) = name {
                    tgt.names.push(name);
                }
                tgt.cow_source = Some(src_field_id);
                src_field.ref_count += 1;
                Some(tgt)
            } else {
                None
            };
            entry.take().map(|e| e.insert(target_field_id));
            for other_name in &src_field.names {
                if name == Some(*other_name) {
                    continue;
                }
                if field_access_mapping.fields.contains_key(other_name) {
                    target_match_set
                        .field_name_map
                        .insert(*other_name, target_field_id);
                    if let Some(f) = tgt_field.as_mut() {
                        f.names.push(*other_name)
                    }
                }
            }
            if name.is_none() {
                chain_input_field = Some(target_field_id);
            }
        }
        let input_field = chain_input_field.unwrap_or(DUMMY_INPUT_FIELD_ID);
        let start_op_id =
            sess.job_data.session_data.chains[subchain_id].operators[0];

        // if the initial transform after the fork may unlink
        // before input is done, we insert a dummy transform
        // so our target transform ids are stable
        let mut tf_state = TransformState::new(
            input_field,
            DUMMY_INPUT_FIELD_ID,
            target_ms_id,
            sess.job_data.session_data.chains[subchain_id]
                .settings
                .default_batch_size,
            None,
            None,
            sess.job_data.tf_mgr.claim_transform_ordering_id(),
        );
        sess.job_data.field_mgr.bump_field_refcount(input_field);
        sess.job_data
            .field_mgr
            .bump_field_refcount(DUMMY_INPUT_FIELD_ID);
        tf_state.is_transparent = true;
        let tf_data = setup_tf_nop(&tf_state);
        let mut pred_tf = sess.add_transform(tf_state, tf_data);
        let (start_tf, end_tf, end_reachable) = sess.setup_transforms_from_op(
            target_ms_id,
            start_op_id,
            input_field,
            Some(pred_tf),
        );
        if end_reachable {
            sess.add_terminator(end_tf);
        }
        if sess.job_data.tf_mgr.transforms[start_tf]
            .continuation
            .is_none()
        {
            sess.job_data.unlink_transform(pred_tf, 0);
            sess.remove_transform(pred_tf);
            pred_tf = start_tf;
        }
        targets.push(pred_tf);
    }
    sess.log_state("expanded fork");
    if let TransformData::Fork(ref mut fork) =
        sess.transform_data[usize::from(tf_id)]
    {
        fork.targets = targets;
        fork.mappings = mappings;
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
