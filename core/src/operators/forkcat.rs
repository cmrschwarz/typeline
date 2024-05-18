use std::{cell::Cell, collections::HashMap};

use bitvec::vec::BitVec;

use crate::{
    chain::{Chain, SubchainOffset},
    context::SessionData,
    job::{add_transform_to_job, Job, JobData},
    liveness_analysis::{
        LivenessData, OpOutputIdx, Var, GLOBAL_SLOTS_OFFSET,
        LOCAL_SLOTS_PER_BASIC_BLOCK, READS_OFFSET,
    },
    operators::terminator::add_terminator_tf_cont_dependant,
    options::argument::CliArgIdx,
    record_data::{
        action_buffer::ActorRef,
        field::{FieldId, VOID_FIELD_ID},
        group_track_manager::VOID_GROUP_TRACK_ID,
        match_set::GroupTrackMapping,
    },
    utils::{
        identity_hasher::BuildIdentityHasher, string_store::StringStoreEntry,
    },
};

use super::{
    errors::{OperatorCreationError, OperatorSetupError},
    input_done_eater::add_input_done_eater,
    operator::{
        OperatorBase, OperatorData, OperatorId, OperatorInstantiation,
        OperatorOffsetInChain, TransformContinuationKind,
    },
    transform::{TransformData, TransformId, TransformState},
};

#[derive(Clone, Copy)]
enum OutputMapping {
    // field is produced / shadowed by the subchain
    OutputIdx(OpOutputIdx),
    // field is cow'ed into the subchain
    // index into accessed_names_of_subchains
    InputIndex(u32),
}

#[derive(Clone, Default)]
pub struct OpForkCat {
    pub subchains_start: SubchainOffset,
    pub subchains_end: SubchainOffset,
    pub continuation: Option<OperatorId>,

    // all names accessed by any of the subchains or any of the successors
    accessed_names_of_subchains_or_succs: Vec<Option<StringStoreEntry>>,

    // names accessed by any successor (this is *not* affected by subchains)
    accessed_names_of_successors: Vec<Option<StringStoreEntry>>,

    // contains indices into accessed_names_of_subchains_or_succs
    accessed_names_per_subchain: Vec<Vec<u32>>,

    // index into accessed_names_of_successors + OutputIdx
    // parallel to accessed_names_of_successors
    output_mappings_per_subchain: Vec<Vec<OutputMapping>>,
}

pub struct TfForkCat<'a> {
    pub op: &'a OpForkCat,
    pub curr_subchain_n: u32,
    pub continuation: Option<TransformId>,
    pub buffered_record_count: usize,
    pub input_size: usize,

    // temp buffer passed to setup_transforms_from_op. contains the fields
    // from output_mappings that the current subchain will write to
    pub prebound_outputs: HashMap<OpOutputIdx, FieldId, BuildIdentityHasher>,

    // fields created before the fc that are used during or after the fc
    // parallel to OpForkCat::accessed_names_of_subchains_or_succs
    input_fields: Vec<FieldId>,

    // if the subchain does not shadow an input field, we add a cow'ed
    // copy of the field into to the subchain match set to get
    // actions applied to it
    // parallel to OpForkCat::accessed_names_of_successors
    output_field_sources: Vec<FieldId>,

    // Contains all fields that may be used by successors of the forkcat.
    // successor's match set, and have a cow source mirror fields
    // in the subchain
    // parallel to OpForkCat::accessed_names_of_successors
    output_fields: Vec<FieldId>,
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
    _op_base: &OperatorBase,
    op: &mut OpForkCat,
    offset_in_chain: OperatorOffsetInChain,
) -> Result<(), OperatorSetupError> {
    if op.subchains_end == 0 {
        op.subchains_end = chain.subchains.len() as u32;
    }
    op.continuation =
        chain.operators.get(offset_in_chain as usize + 1).copied();
    Ok(())
}

pub fn setup_op_forkcat_liveness_data(
    sess: &SessionData,
    op: &mut OpForkCat,
    op_id: OperatorId,
    ld: &LivenessData,
) {
    let bb_id = ld.operator_liveness_data[op_id as usize].basic_block_id;
    let bb = &ld.basic_blocks[bb_id];
    let var_count = ld.vars.len();

    let mut successors = BitVec::<Cell<usize>>::new();
    let mut calls = BitVec::<Cell<usize>>::new();
    calls.resize(var_count * LOCAL_SLOTS_PER_BASIC_BLOCK, false);
    successors.resize(var_count * LOCAL_SLOTS_PER_BASIC_BLOCK, false);
    ld.get_global_var_data_ored(&mut calls, bb.calls.iter());
    ld.get_global_var_data_ored(&mut successors, bb.successors.iter());
    let successors_reads = &successors[ld.get_var_slot_range(READS_OFFSET)];
    let mut reads_of_succs_idx_map = HashMap::new();
    for var_id in successors_reads.iter_ones() {
        let var_name = ld.vars[var_id].get_name();
        reads_of_succs_idx_map
            .insert(var_name, op.accessed_names_of_successors.len());
        op.accessed_names_of_successors
            .push(ld.vars[var_id].get_name());
    }
    ld.kill_non_survivors(&calls, &mut successors);
    calls |= &successors;
    let reads_of_subchains_or_succs =
        &calls[ld.get_var_slot_range(READS_OFFSET)];

    let mut reads_of_scs_or_succs_idx_map = HashMap::new();
    for var_id in reads_of_subchains_or_succs.iter_ones() {
        let var_name = ld.vars[var_id].get_name();
        reads_of_scs_or_succs_idx_map.insert(
            var_name,
            op.accessed_names_of_subchains_or_succs.len() as u32,
        );
        op.accessed_names_of_subchains_or_succs.push(var_name);
    }

    for &callee_id in &bb.calls {
        let mut accessed_names = Vec::default();
        let call_reads = ld.get_var_data_field(
            callee_id,
            GLOBAL_SLOTS_OFFSET,
            READS_OFFSET,
        );
        let mut reads_of_subchains_or_succs_offset = 0;
        let mut prev = 0;
        for i in call_reads.iter_ones() {
            // let var_name = match ld.vars[i] {
            //    Var::Named(name) => Some(name),
            //    Var::BBInput => None,
            //    // TODO: investigate why this happens
            //    Var::AnyVar | Var::DynVar | Var::VoidVar => continue,
            //};
            reads_of_subchains_or_succs_offset +=
                reads_of_subchains_or_succs[prev..i].count_ones() as u32;
            accessed_names.push(reads_of_subchains_or_succs_offset);
            prev = i;
        }
        op.accessed_names_per_subchain.push(accessed_names);
    }
    for sc_n in 0..op.accessed_names_per_subchain.len() {
        let mut mappings = Vec::new();
        mappings.resize(
            op.accessed_names_of_successors.len(),
            OutputMapping::InputIndex(0),
        );
        let sc_id = sess.chains
            [sess.operator_bases[op_id as usize].chain_id.unwrap() as usize]
            .subchains[op.subchains_start as usize + sc_n]
            as usize;
        for &op_id in &sess.chains[sc_id].operators {
            let op_base = &sess.operator_bases[op_id as usize];
            for oo_id in op_base.outputs_start..op_base.outputs_end {
                for bv in &ld.op_outputs[oo_id as usize].bound_vars {
                    let var_name = match ld.vars[*bv as usize] {
                        Var::Named(name) => Some(name),
                        Var::BBInput => None,
                        Var::DynVar => todo!(),
                        Var::AnyVar => todo!(),
                        Var::VoidVar => unreachable!(),
                    };
                    if let Some(&idx) = reads_of_succs_idx_map.get(&var_name) {
                        mappings[idx] = OutputMapping::OutputIdx(oo_id);
                    }
                }
            }
        }
        for (i, m) in mappings.iter_mut().enumerate() {
            if let OutputMapping::InputIndex(_) = m {
                *m = OutputMapping::InputIndex(
                    reads_of_scs_or_succs_idx_map
                        [&op.accessed_names_of_successors[i]],
                );
            }
        }
        op.output_mappings_per_subchain.push(mappings);
    }
}

pub fn insert_tf_forkcat<'a>(
    job: &mut Job<'a>,
    _op_base: &OperatorBase,
    op: &'a OpForkCat,
    tf_state: TransformState,
) -> OperatorInstantiation {
    let tf_data = TransformData::ForkCat(TfForkCat {
        curr_subchain_n: op.subchains_start,
        continuation: None,
        op,
        input_size: 0,
        buffered_record_count: 0,
        prebound_outputs: HashMap::default(),
        input_fields: Vec::with_capacity(
            op.accessed_names_of_subchains_or_succs.len(),
        ),
        output_fields: Vec::with_capacity(
            op.accessed_names_of_successors.len(),
        ),
        output_field_sources: Vec::with_capacity(
            op.accessed_names_of_successors.len(),
        ),
    });
    let fc_tf_id = add_transform_to_job(
        &mut job.job_data,
        &mut job.transform_data,
        tf_state,
        tf_data,
    );
    let tf = &job.job_data.tf_mgr.transforms[fc_tf_id];
    let TransformData::ForkCat(forkcat) =
        &mut job.transform_data[fc_tf_id.get()]
    else {
        unreachable!()
    };
    for &f in &forkcat.op.accessed_names_of_subchains_or_succs {
        let input_field = if let Some(name) = f {
            job.job_data.match_set_mgr.match_sets[tf.match_set_id]
                .field_name_map
                .get(&name)
                .copied()
                .unwrap_or(VOID_FIELD_ID)
        } else {
            tf.input_field
        };
        forkcat.input_fields.push(input_field);
    }

    let cont_instantiation = setup_continuation(job, fc_tf_id);
    let TransformData::ForkCat(fc) = &mut job.transform_data[fc_tf_id.get()]
    else {
        unreachable!()
    };
    fc.continuation = Some(cont_instantiation.tfs_begin);
    expand_for_subchain(job, fc_tf_id, 0);
    let cont_end = add_terminator_tf_cont_dependant(
        job,
        cont_instantiation.tfs_end,
        cont_instantiation.continuation,
    )
    .unwrap_or(cont_instantiation.tfs_end);
    OperatorInstantiation {
        tfs_begin: fc_tf_id,
        tfs_end: cont_end,
        next_input_field: cont_instantiation.next_input_field,
        next_group_track: cont_instantiation.next_group_track,
        continuation: TransformContinuationKind::SelfExpanded,
    }
}

pub fn handle_tf_forkcat(
    jd: &mut JobData,
    tf_id: TransformId,
    fc: &mut TfForkCat,
) {
    if fc.op.subchains_start + fc.curr_subchain_n == fc.op.subchains_end {
        let cont = fc.continuation.unwrap();
        jd.tf_mgr.transforms[cont].predecessor_done = true;
        jd.tf_mgr.declare_transform_done(tf_id);
        return;
    }

    if fc.curr_subchain_n != 0 {
        jd.tf_mgr.push_tf_in_ready_stack(tf_id);
        let sc_start = jd.tf_mgr.transforms[tf_id].successor.take().unwrap();
        jd.tf_mgr.inform_cross_ms_transform_batch_available(
            &jd.field_mgr,
            &mut jd.match_set_mgr,
            sc_start,
            fc.input_size,
            true,
        );
        fc.curr_subchain_n += 1;
        return;
    }
    let (batch_size, ps) = jd.tf_mgr.claim_all(tf_id);
    fc.input_size += batch_size;
    let curr_subchain_start = jd.tf_mgr.transforms[tf_id].successor.unwrap();
    if ps.input_done {
        jd.tf_mgr.push_tf_in_ready_stack(tf_id);
        fc.curr_subchain_n += 1;
        jd.tf_mgr.transforms[tf_id].successor = None;
    }
    jd.tf_mgr.inform_cross_ms_transform_batch_available(
        &jd.field_mgr,
        &mut jd.match_set_mgr,
        curr_subchain_start,
        batch_size,
        ps.input_done,
    );
}

fn setup_continuation(
    sess: &mut Job,
    tf_id: TransformId,
) -> OperatorInstantiation {
    let TransformData::ForkCat(forkcat) =
        &mut sess.transform_data[tf_id.get()]
    else {
        unreachable!()
    };
    let cont_op_id = forkcat.op.continuation.unwrap();

    let output_ms_id = sess.job_data.match_set_mgr.add_match_set();
    for name in &forkcat.op.accessed_names_of_successors {
        let output_field_id = sess.job_data.field_mgr.add_field(
            &mut sess.job_data.match_set_mgr,
            output_ms_id,
            *name,
            ActorRef::default(),
        );
        forkcat.output_fields.push(output_field_id);
    }
    forkcat.output_field_sources.extend(
        std::iter::repeat(VOID_FIELD_ID).take(forkcat.output_fields.len()),
    );
    let cont_input_field =
        if forkcat.op.accessed_names_of_successors.first() == Some(&None) {
            forkcat.output_fields[0]
        } else {
            VOID_FIELD_ID
        };
    let sc_count = forkcat.op.subchains_end - forkcat.op.subchains_start;
    let chain_id = sess.job_data.session_data.operator_bases
        [cont_op_id as usize]
        .chain_id
        .unwrap();
    let begin = add_input_done_eater(
        sess,
        chain_id,
        output_ms_id,
        sc_count as usize - 1,
    );
    let mut instantiation = sess.setup_transforms_from_op(
        output_ms_id,
        cont_op_id,
        cont_input_field,
        // TODO: //HACK:
        VOID_GROUP_TRACK_ID,
        Some(begin),
        &HashMap::default(),
    );
    instantiation.tfs_begin = begin;
    instantiation
}

fn expand_for_subchain(sess: &mut Job, tf_id: TransformId, sc_n: u32) {
    let tgt_ms_id = sess.job_data.match_set_mgr.add_match_set();
    let tf = &sess.job_data.tf_mgr.transforms[tf_id];

    sess.job_data.match_set_mgr.match_sets[tgt_ms_id].group_track_mapping =
        Some(GroupTrackMapping {
            source: sess
                .job_data
                .group_track_manager
                .claim_group_track_iter_ref(tf.input_group_track_id),
            target: sess.job_data.group_track_manager.add_group_track(
                None,
                tgt_ms_id,
                ActorRef::Unconfirmed(0),
            ),
        });

    let TransformData::ForkCat(forkcat) =
        &mut sess.transform_data[tf_id.get()]
    else {
        unreachable!()
    };

    let chain_id = sess.job_data.session_data.operator_bases
        [tf.op_id.unwrap() as usize]
        .chain_id
        .unwrap();
    let sc_id = sess.job_data.session_data.chains[chain_id as usize].subchains
        [(forkcat.op.subchains_start + sc_n) as usize];
    let mut prebound_outputs = std::mem::take(&mut forkcat.prebound_outputs);
    prebound_outputs.clear();

    let mut subchain_input_field = VOID_FIELD_ID;
    for &idx in &forkcat.op.accessed_names_per_subchain[sc_n as usize] {
        let idx = idx as usize;
        let input_field = forkcat.input_fields[idx];
        let input_mirror_field =
            sess.job_data.field_mgr.get_cross_ms_cow_field(
                &mut sess.job_data.match_set_mgr,
                tgt_ms_id,
                input_field,
            );
        let name = forkcat.op.accessed_names_of_subchains_or_succs[idx];
        let tgt_ms = &mut sess.job_data.match_set_mgr.match_sets[tgt_ms_id];
        if let Some(name) = name {
            tgt_ms.field_name_map.insert(name, input_mirror_field);
        } else {
            subchain_input_field = input_mirror_field;
        }
    }

    for (idx, &output_mapping) in forkcat.op.output_mappings_per_subchain
        [sc_n as usize]
        .iter()
        .enumerate()
    {
        forkcat.output_field_sources[idx] = match output_mapping {
            OutputMapping::OutputIdx(output_idx) => {
                let id = sess.job_data.field_mgr.add_field(
                    &mut sess.job_data.match_set_mgr,
                    tgt_ms_id,
                    forkcat.op.accessed_names_of_successors[idx],
                    ActorRef::default(),
                );
                prebound_outputs.insert(output_idx, id);
                id
            }
            OutputMapping::InputIndex(input_idx) => {
                sess.job_data.field_mgr.get_cross_ms_cow_field(
                    &mut sess.job_data.match_set_mgr,
                    tgt_ms_id,
                    forkcat.input_fields[input_idx as usize],
                )
            }
        };
    }

    let instantiation = sess.setup_transforms_from_op(
        tgt_ms_id,
        sess.job_data.session_data.chains[sc_id as usize].operators[0],
        subchain_input_field,
        // TODO: //HACK
        VOID_GROUP_TRACK_ID,
        None,
        &prebound_outputs,
    );
    let end_tf = add_terminator_tf_cont_dependant(
        sess,
        instantiation.tfs_end,
        instantiation.continuation,
    )
    .unwrap_or(instantiation.tfs_end);
    let TransformData::ForkCat(forkcat) =
        &mut sess.transform_data[tf_id.get()]
    else {
        unreachable!()
    };
    let cont = forkcat.continuation.unwrap();
    let cont_tf = &mut sess.job_data.tf_mgr.transforms[cont];

    // the input done eater should have done it's job
    debug_assert!(!cont_tf.predecessor_done);

    sess.job_data.field_mgr.setup_cross_ms_cow_fields(
        &mut sess.job_data.match_set_mgr,
        tgt_ms_id,
        cont_tf.match_set_id,
        &forkcat.output_field_sources,
        &forkcat.output_fields,
    );

    sess.job_data.tf_mgr.transforms[end_tf].successor = Some(cont);
    sess.job_data.tf_mgr.transforms[cont].predecessor_done = false;

    sess.job_data.tf_mgr.transforms[tf_id].successor =
        Some(instantiation.tfs_begin);

    forkcat.prebound_outputs = prebound_outputs;

    #[cfg(feature = "debug_logging")]
    {
        eprintln!("input fields: {:?}", forkcat.input_fields);
        eprintln!("output fields sources: {:?}", forkcat.output_field_sources);
        eprintln!("output fields: {:?}", forkcat.output_fields);
        sess.log_state(&format!("expanded sc #{sc_n} for forkcat"));
    }
}

pub(crate) fn handle_forkcat_subchain_expansion(
    job: &mut Job,
    tf_id: TransformId,
) {
    let TransformData::ForkCat(fc) = &mut job.transform_data[tf_id.get()]
    else {
        unreachable!()
    };
    let sc_n = fc.curr_subchain_n;
    let sc_idx = fc.op.subchains_start + sc_n;
    if sc_idx == fc.op.subchains_end {
        for &f in &fc.output_fields {
            job.job_data
                .field_mgr
                .drop_field_refcount(f, &mut job.job_data.match_set_mgr);
        }
        let _cont_id = fc.continuation.unwrap().get();
        // TODO: unlink the subchain(s) ?
        return;
    }
    for &of in &fc.output_fields {
        let mut f = job.job_data.field_mgr.fields[of].borrow_mut();
        f.iter_hall.reset_iterators();
        let msm = &mut job.job_data.match_set_mgr.match_sets[f.match_set];
        let fr = &mut *f;
        msm.action_buffer.borrow_mut().drop_field_commands(
            of,
            &mut fr.first_actor,
            &mut fr.snapshot,
        );
    }
    expand_for_subchain(job, tf_id, sc_n);
}

pub fn create_op_forkcat() -> OperatorData {
    OperatorData::ForkCat(OpForkCat::default())
}
