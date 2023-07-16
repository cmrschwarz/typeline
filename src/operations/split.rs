use std::collections::{hash_map::Entry, HashMap};

use bstr::{BStr, ByteVec};

use crate::{
    chain::DEFAULT_INPUT_FIELD,
    field_data::iter_hall::IterHall,
    options::argument::CliArgIdx,
    ref_iter::AutoDerefIter,
    utils::identity_hasher::{BuildIdentityHasher, IdentityHasher},
    worker_thread_session::{FieldId, JobData, MatchSetId, RecordManager, WorkerThreadSession},
};

use super::{
    errors::{OperatorCreationError, TransformSetupError},
    operator::{OperatorBase, OperatorData, OperatorId},
    transform::{TransformData, TransformId, TransformState},
};

#[derive(Clone)]
pub struct OpSplit {}

pub struct TfSplitFieldMapping {
    pub source: FieldId,
    pub target: FieldId,
}

pub struct TfSplit {
    pub targets: Vec<TransformId>,
    pub mappings: HashMap<FieldId, Vec<FieldId>, BuildIdentityHasher>,
}

pub fn parse_op_split(
    value: Option<&BStr>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    if value.is_some() {
        return Err(OperatorCreationError::new(
            "this operator takes no arguments",
            arg_idx,
        ));
    }
    Ok(OperatorData::Split(OpSplit {}))
}
pub fn setup_ts_split_as_entry_point<'a, 'b>(
    sess: &mut JobData<'a>,
    input_field: FieldId,
    ms_id: MatchSetId,
    entry_count: usize,
    ops: impl Iterator<Item = &'b OperatorId> + Clone,
) -> (TransformState, TransformData<'a>) {
    let mut state = TransformState::new(
        input_field,
        input_field, // HACK
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

pub fn setup_tf_split<'a>(
    _sess: &mut JobData,
    _op_base: &OperatorBase,
    _op: &'a OpSplit,
    _tf_state: &mut TransformState,
) -> TransformData<'static> {
    TransformData::Split(TfSplit {
        targets: Default::default(),
        mappings: Default::default(),
    })
}

pub fn handle_tf_split(sess: &mut JobData, tf_id: TransformId, sp: &mut TfSplit) {
    let (batch, end_of_input) = sess.tf_mgr.claim_batch(tf_id);
    drop(sess.prepare_output_field(tf_id));
    let match_sets = &mut sess.record_mgr.match_sets;
    for (src_field_id, tgts) in &sp.mappings {
        RecordManager::apply_field_actions(&sess.record_mgr.fields, match_sets, *src_field_id);
        let src = sess.record_mgr.fields[*src_field_id].borrow_mut();
        let iter = AutoDerefIter::new(
            &sess.record_mgr.fields,
            match_sets,
            *src_field_id,
            src.field_data.iter(),
            None,
        );
        IterHall::copy_resolve_refs(match_sets, iter, &mut |f: &mut dyn FnMut(&mut IterHall)| {
            for t in tgts {
                let tgt = &mut sess.record_mgr.fields[*t].borrow_mut();
                if !tgt.cow_source.is_some() {
                    f(&mut tgt.field_data);
                }
            }
        });
    }
    for tf in &sp.targets {
        sess.tf_mgr.inform_successor_batch_available(*tf, batch);
    }
    if end_of_input {
        sess.unlink_transform(tf_id, 0);
    }
}

pub fn handle_split_expansion(
    sess: &mut WorkerThreadSession,
    tf_id: TransformId,
) -> Result<(), TransformSetupError> {
    // we have to temporarily move the targets out of split so we can modify
    // sess while accessing them
    let mut targets = Vec::new();
    let mut mappings = HashMap::<FieldId, Vec<FieldId>, BuildIdentityHasher>::default();
    let tf = &sess.job_data.tf_mgr.transforms[tf_id];
    let split_input_field_id = tf.input_field;
    let split_ms_id = tf.match_set_id;
    let split_op_id = tf.op_id.unwrap() as usize;
    let split_chain_id = sess.job_data.session_data.operator_bases[split_op_id].chain_id as usize;

    for i in 0..sess.job_data.session_data.chains[split_chain_id]
        .subchains
        .len()
    {
        let subchain_id = sess.job_data.session_data.chains[split_chain_id].subchains[i] as usize;
        let target_ms_id = sess.job_data.record_mgr.add_match_set();
        let match_sets = &mut sess.job_data.record_mgr.match_sets;
        let (split_match_set, target_match_set) =
            match_sets.two_distinct_mut(split_ms_id, target_ms_id);
        let mut target_fields = Vec::new();
        let accessed_fields_map = &sess.job_data.session_data.chains[subchain_id]
            .liveness_data
            .fields_accessed_before_assignment;
        let mut chain_input_field_id = split_input_field_id;
        for (name, writes) in accessed_fields_map {
            match target_match_set.field_name_map.entry(*name) {
                std::collections::hash_map::Entry::Occupied(_) => continue,
                std::collections::hash_map::Entry::Vacant(e) => {
                    let src_field_id = if let Some(src_field_id) =
                        split_match_set.field_name_map.get(name)
                    {
                        *src_field_id
                    } else if *name == DEFAULT_INPUT_FIELD {
                        split_input_field_id
                    } else {
                        let target_field_id = sess.job_data.record_mgr.fields.claim();
                        let mut tgt = sess.job_data.record_mgr.fields[target_field_id].borrow_mut();
                        tgt.match_set = target_ms_id;
                        tgt.added_as_placeholder_by_tf = Some(tf_id);
                        e.insert(target_field_id);
                        continue;
                    };
                    let src_field = sess.job_data.record_mgr.fields[src_field_id].borrow();
                    let mut any_writes = *writes;
                    if any_writes == false {
                        for other_name in &src_field.names {
                            if name == other_name {
                                continue;
                            }
                            if let Some(true) = accessed_fields_map.get(other_name) {
                                any_writes = true;
                                break;
                            }
                        }
                    }
                    drop(src_field);
                    let target_field_id = if any_writes {
                        let target = sess.job_data.record_mgr.fields.claim();
                        match mappings.entry(src_field_id) {
                            Entry::Occupied(ref mut e) => {
                                e.get_mut().push(src_field_id);
                            }
                            Entry::Vacant(e) => {
                                e.insert(vec![src_field_id]);
                            }
                        }
                        target_fields.push(TfSplitFieldMapping {
                            source: src_field_id,
                            target,
                        });
                        target
                    } else {
                        src_field_id
                    };
                    let mut src_field = sess.job_data.record_mgr.fields[src_field_id].borrow_mut();
                    let mut tgt_field = if any_writes {
                        let mut tgt = sess.job_data.record_mgr.fields[target_field_id].borrow_mut();
                        tgt.match_set = target_ms_id;
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
                            tgt_field.as_mut().map(|f| f.names.push(*other_name));
                        }
                    }
                    if *name == DEFAULT_INPUT_FIELD {
                        chain_input_field_id = target_field_id;
                    }
                }
            }
        }
        let start_op = sess.job_data.session_data.chains[subchain_id].operations[0];
        let tf_id = sess.setup_transforms_from_op(target_ms_id, start_op, chain_input_field_id)?;
    }
    if let TransformData::Split(ref mut split) = sess.transform_data[usize::from(tf_id)] {
        split.targets = targets;
        split.mappings = mappings;
    } else {
        unreachable!();
    }
    return Ok(());
}
