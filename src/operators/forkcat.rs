use std::{cell::Cell, collections::HashMap};

use bitvec::vec::BitVec;

use crate::{
    chain::Chain,
    context::Session,
    job_session::{JobData, JobSession},
    liveness_analysis::{
        LivenessData, OpOutputIdx, Var, DATA_WRITES_OFFSET,
        HEADER_WRITES_OFFSET, LOCAL_SLOTS_PER_BASIC_BLOCK, READS_OFFSET,
    },
    options::argument::CliArgIdx,
    record_data::{
        command_buffer::FieldActionIndices,
        field::{FieldId, DUMMY_INPUT_FIELD_ID},
        match_set::MatchSetId,
    },
    utils::{
        identity_hasher::BuildIdentityHasher, nonzero_ext::NonMaxUsizeExt,
        string_store::StringStoreEntry,
    },
};

use super::{
    errors::{OperatorCreationError, OperatorSetupError},
    operator::{
        OperatorBase, OperatorData, OperatorId, OperatorOffsetInChain,
    },
    transform::{TransformData, TransformId, TransformState},
    utils::field_access_mappings::{
        AccessKind, AccessMappings, FieldAccessMode,
    },
};

#[derive(Clone, Copy)]
enum OutputMapping {
    OutputIdx(OpOutputIdx),
    InputIndex(u32),
}

#[derive(Clone, Default)]
pub struct OpForkCat {
    pub subchains_start: u32,
    pub subchains_end: u32,
    pub continuation: Option<OperatorId>,

    // accessed names of any of the subchains or successors
    accessed_names_of_subchains: Vec<Option<StringStoreEntry>>,

    // index into accessed_names_of_subchains
    accessed_names_per_subchain: Vec<Vec<usize>>,

    // index into accessed_names_afterwards + OutputIdx
    // parallel to accessed_names_afterwards
    output_mappings_per_subchain: Vec<Vec<OutputMapping>>,

    accessed_names_afterwards: Vec<Option<StringStoreEntry>>,
}

pub struct TfForkCat<'a> {
    pub op: &'a OpForkCat,
    pub curr_subchain_n: u32,
    pub curr_subchain_start: Option<TransformId>,
    pub continuation: Option<TransformId>,
    pub buffered_record_count: usize,
    pub input_size: usize,

    // temp buffer passed to setup_transforms_from_op. contains the fields
    // from output_mappings that the current subchain will write to
    pub prebound_outputs: HashMap<OpOutputIdx, FieldId, BuildIdentityHasher>,

    pub input_mirror_fields: Vec<FieldId>,
    // Contains all fields that may be used by successors of the forkcat.
    // successor's match set, and have a cow source mirror fields
    // in the subchain
    pub output_fields: Vec<FieldId>,
    // if the subchain does not shadow an input field, we add a cow'ed
    // copy of the field into to the subchain match set to get
    // actions applied to it
    pub output_mirror_fields: Vec<FieldId>,
}

#[derive(Default, Clone)]
struct AccessedNamesIndexed(usize);

impl AccessKind for AccessedNamesIndexed {
    type ContextType = usize;

    fn from_field_access_mode(
        output_idx: &mut usize,
        _fam: FieldAccessMode,
    ) -> Self {
        let idx = *output_idx;
        *output_idx += 1;
        AccessedNamesIndexed(idx)
    }

    fn append_field_access_mode(
        &mut self,
        _output_idx: &mut usize,
        _fam: FieldAccessMode,
    ) {
    }
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
    offset_in_chain: OperatorOffsetInChain,
) -> Result<(), OperatorSetupError> {
    if op.subchains_end == 0 {
        debug_assert!(
            op_base.offset_in_chain as usize + 1 == chain.operators.len()
        );
        op.subchains_end = chain.subchains.len() as u32;
    }
    op.continuation =
        chain.operators.get(offset_in_chain as usize + 1).copied();
    Ok(())
}

pub fn setup_op_forkcat_liveness_data(
    sess: &Session,
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
    let mut accessed_names_afterwards_count = 0;
    let accessed_names_afterwards =
        AccessMappings::<AccessedNamesIndexed>::from_var_data(
            &mut accessed_names_afterwards_count,
            ld,
            &successors,
        );
    op.accessed_names_afterwards
        .resize(accessed_names_afterwards_count, None);
    for (name, idx) in accessed_names_afterwards.iter_name_opt() {
        op.accessed_names_afterwards[idx.0] = name;
    }
    ld.kill_non_survivors(&mut successors, &calls);
    calls |= successors;
    let mut accessed_names_of_subchains_count = 0;
    let accessed_names_of_subchains =
        AccessMappings::<AccessedNamesIndexed>::from_var_data(
            &mut accessed_names_of_subchains_count,
            ld,
            &calls,
        );
    op.accessed_names_of_subchains
        .resize(accessed_names_of_subchains_count, None);
    for (name, idx) in accessed_names_of_subchains.iter_name_opt() {
        op.accessed_names_of_subchains[idx.0] = name;
    }
    for &callee_id in bb.calls.iter() {
        let mut accessed_names = Vec::default();
        let call_liveness = ld.get_global_var_data(callee_id);
        let reads_range =
            READS_OFFSET * var_count..(READS_OFFSET + 1) * var_count;
        calls[reads_range.clone()].copy_from_bitslice(
            &call_liveness
                [READS_OFFSET * var_count..(READS_OFFSET + 1) * var_count],
        );
        for o in [HEADER_WRITES_OFFSET, DATA_WRITES_OFFSET] {
            calls[reads_range.clone()] |=
                &call_liveness[o * var_count..(o + 1) * var_count];
        }
        for i in calls[reads_range].iter_ones() {
            let var_name = match ld.vars[i] {
                Var::Named(name) => Some(name),
                Var::BBInput => None,
                Var::UnreachableDummyVar => continue,
            };
            accessed_names
                .push(accessed_names_of_subchains.get(var_name).unwrap().0);
        }
        op.accessed_names_per_subchain.push(accessed_names);
    }
    for sc_n in 0..op.accessed_names_per_subchain.len() {
        let mut mappings = Vec::new();
        mappings.resize(
            op.accessed_names_afterwards.len(),
            OutputMapping::InputIndex(0),
        );
        let sc_id = sess.chains
            [sess.operator_bases[op_id as usize].chain_id.unwrap() as usize]
            .subchains[op.subchains_start as usize + sc_n]
            as usize;
        for &op_id in &sess.chains[sc_id].operators {
            let op_base = &sess.operator_bases[op_id as usize];
            for oo_id in op_base.outputs_start..op_base.outputs_end {
                for bv in &ld.op_outputs[oo_id as usize].bound_vars_after_bb {
                    let var_name = match ld.vars[*bv as usize] {
                        Var::Named(name) => Some(name),
                        Var::BBInput => None,
                        Var::UnreachableDummyVar => continue,
                    };
                    if let Some(idx) = accessed_names_afterwards.get(var_name)
                    {
                        mappings[idx.0] = OutputMapping::OutputIdx(oo_id);
                    }
                }
            }
        }
        for (i, m) in mappings.iter_mut().enumerate() {
            if let OutputMapping::InputIndex(_) = m {
                *m = OutputMapping::InputIndex(
                    accessed_names_of_subchains
                        .get(op.accessed_names_afterwards[i])
                        .unwrap()
                        .0 as u32,
                );
            }
        }
        op.output_mappings_per_subchain.push(mappings);
    }
}

pub fn setup_tf_forkcat<'a>(
    _sess: &mut JobData,
    _op_base: &OperatorBase,
    op: &'a OpForkCat,
    _tf_state: &mut TransformState,
) -> TransformData<'a> {
    TransformData::ForkCat(TfForkCat {
        curr_subchain_n: op.subchains_start,
        curr_subchain_start: None,
        continuation: None,
        op,
        input_size: 0,
        buffered_record_count: 0,
        prebound_outputs: Default::default(),
        output_fields: Vec::with_capacity(op.accessed_names_afterwards.len()),
        output_mirror_fields: Vec::with_capacity(
            op.accessed_names_afterwards.len(),
        ),
        input_mirror_fields: Vec::with_capacity(
            op.accessed_names_of_subchains.len(),
        ),
    })
}

pub fn handle_tf_forkcat_sc(
    sess: &mut JobData,
    tf_id: TransformId,
    fc: &mut TfForkCat,
    batch_size: usize,
    end_of_input: bool,
) {
    let target_tf = fc.curr_subchain_start.unwrap();
    let unconsumed_input =
        sess.tf_mgr.transforms[tf_id].has_unconsumed_input();
    sess.tf_mgr.inform_transform_batch_available(
        target_tf,
        batch_size,
        unconsumed_input,
    );
    if end_of_input {
        sess.tf_mgr.push_tf_in_ready_stack(target_tf);
        sess.tf_mgr.transforms[target_tf].input_is_done = true;
    }
}

pub fn handle_tf_forkcat(
    sess: &mut JobData,
    tf_id: TransformId,
    fc: &mut TfForkCat,
) {
    if fc.op.subchains_start + fc.curr_subchain_n == fc.op.subchains_end {
        let cont = fc.continuation.unwrap();
        sess.tf_mgr.transforms[cont].input_is_done = true;
        sess.tf_mgr.push_tf_in_ready_stack(cont);
        sess.unlink_transform(tf_id, 0);
        return;
    }
    sess.tf_mgr.push_tf_in_ready_stack(tf_id);
    if fc.curr_subchain_n == 0 {
        let (batch_size, end_of_input) = sess.tf_mgr.claim_all(tf_id);
        fc.input_size += batch_size;
        handle_tf_forkcat_sc(sess, tf_id, fc, batch_size, end_of_input);
        if !end_of_input {
            return;
        }
    } else {
        handle_tf_forkcat_sc(sess, tf_id, fc, fc.input_size, true);
    }
    fc.curr_subchain_n += 1;
    fc.curr_subchain_start = None;
}
fn expand_for_subchain(sess: &mut JobSession, tf_id: TransformId, sc_n: u32) {
    let tgt_ms_id = sess.job_data.match_set_mgr.add_match_set();
    let forkcat = match_unwrap!(
        &mut sess.transform_data[tf_id.get()],
        TransformData::ForkCat(fc),
        fc
    );
    let chain_id = sess.job_data.session_data.operator_bases
        [sess.job_data.tf_mgr.transforms[tf_id].op_id.unwrap() as usize]
        .chain_id
        .unwrap();
    let sc_id = sess.job_data.session_data.chains[chain_id as usize].subchains
        [(forkcat.op.subchains_start + sc_n) as usize];
    let mut prebound_outputs = std::mem::take(&mut forkcat.prebound_outputs);
    prebound_outputs.clear();

    let mut subchain_input_field = DUMMY_INPUT_FIELD_ID;
    for &idx in &forkcat.op.accessed_names_per_subchain[sc_n as usize] {
        let input_mirror_field_id = forkcat.input_mirror_fields[idx];
        let mut input_mirror_field =
            sess.job_data.field_mgr.fields[input_mirror_field_id].borrow_mut();
        input_mirror_field.match_set = tgt_ms_id;
        input_mirror_field.action_indices = FieldActionIndices::default();
        drop(input_mirror_field);
        let tgt_ms = &mut sess.job_data.match_set_mgr.match_sets[tgt_ms_id];
        let name = forkcat.op.accessed_names_of_subchains[idx];
        if let Some(name) = name {
            tgt_ms.field_name_map.insert(name, input_mirror_field_id);
        } else {
            subchain_input_field = input_mirror_field_id;
        }
    }

    for (idx, &output_mapping) in forkcat.op.output_mappings_per_subchain
        [sc_n as usize]
        .iter()
        .enumerate()
    {
        let output_mirror_field_id = forkcat.output_mirror_fields[idx];
        let mut output_mirror_field = sess.job_data.field_mgr.fields
            [output_mirror_field_id]
            .borrow_mut();
        output_mirror_field.match_set = tgt_ms_id;
        output_mirror_field.action_indices = FieldActionIndices::default();
        drop(output_mirror_field);
        match output_mapping {
            OutputMapping::OutputIdx(output_idx) => {
                prebound_outputs.insert(output_idx, output_mirror_field_id);
            }
            OutputMapping::InputIndex(input_idx) => {
                sess.job_data.field_mgr.setup_cow(
                    &mut sess.job_data.match_set_mgr,
                    output_mirror_field_id,
                    forkcat.input_mirror_fields[input_idx as usize],
                );
            }
        }
    }

    let (start_tf, end_tf, end_reachable) = sess
        .setup_transforms_with_stable_start(
            tgt_ms_id,
            sc_id,
            sess.job_data.session_data.chains[sc_id as usize].operators[0],
            subchain_input_field,
            &prebound_outputs,
            false,
        );
    let forkcat = match_unwrap!(
        &mut sess.transform_data[tf_id.get()],
        TransformData::ForkCat(fc),
        fc
    );
    if end_reachable {
        sess.job_data
            .tf_mgr
            .connect_tfs(end_tf, forkcat.continuation.unwrap());
    }
    forkcat.curr_subchain_start = Some(start_tf);
    forkcat.prebound_outputs = prebound_outputs;
    #[cfg(feature = "debug_logging")]
    {
        println!("input mirror fields: {:?}", forkcat.input_mirror_fields);
        println!("output mirror fields: {:?}", forkcat.output_mirror_fields);
        println!("output fields: {:?}", forkcat.output_fields);
        sess.log_state(&format!("expanded sc #{sc_n} for forkcat"));
    }
}
fn setup_continuation(sess: &mut JobSession, tf_id: TransformId) {
    let tf = &sess.job_data.tf_mgr.transforms[tf_id];
    let tf_input_field = tf.input_field;
    let src_ms = tf.match_set_id;
    let forkcat = match_unwrap!(
        &mut sess.transform_data[tf_id.get()],
        TransformData::ForkCat(fc),
        fc
    );
    for &name in &forkcat.op.accessed_names_of_subchains {
        let field_name_map =
            &sess.job_data.match_set_mgr.match_sets[src_ms].field_name_map;
        let input_field_id = if let Some(name) = name {
            if let Some(field) = field_name_map.get(&name) {
                *field
            } else {
                DUMMY_INPUT_FIELD_ID
            }
        } else {
            tf_input_field
        };
        let f = sess.job_data.field_mgr.fields[input_field_id].borrow();
        f.request_clear_delay();
        drop(f);
        let mirror_field_id =
            sess.job_data.field_mgr.add_field(MatchSetId::MAX, None);
        sess.job_data.field_mgr.fields[mirror_field_id]
            .borrow_mut()
            .name = name;
        sess.job_data.field_mgr.setup_cow(
            &mut sess.job_data.match_set_mgr,
            mirror_field_id,
            input_field_id,
        );
        forkcat.input_mirror_fields.push(mirror_field_id);
    }

    let output_ms_id = sess.job_data.match_set_mgr.add_match_set();
    for name in &forkcat.op.accessed_names_afterwards {
        let output_field_id =
            sess.job_data.field_mgr.add_field(output_ms_id, None);
        if let Some(name) = name {
            sess.job_data.match_set_mgr.set_field_name(
                &sess.job_data.field_mgr,
                output_field_id,
                *name,
            );
        }
        forkcat.output_fields.push(output_field_id);
        let mirror_field_id =
            sess.job_data.field_mgr.add_field(MatchSetId::MAX, None);
        sess.job_data.field_mgr.fields[mirror_field_id]
            .borrow_mut()
            .name = *name;
        sess.job_data.field_mgr.setup_cow(
            &mut sess.job_data.match_set_mgr,
            output_field_id,
            mirror_field_id,
        );
        forkcat.output_mirror_fields.push(mirror_field_id);
    }
    let cont_op_id = if let Some(cont) = forkcat.op.continuation {
        cont
    } else {
        // TODO: we can't do this: if the fc ist the last op in a chain
        // but that chain is then called, we don't want the fc to add
        // termination
        let terminator_id = sess.add_terminator(tf_id, true);
        match_unwrap!(
            &mut sess.transform_data[tf_id.get()],
            TransformData::ForkCat(fc),
            fc.continuation = Some(terminator_id)
        );
        return;
    };
    let cont_chain_id = sess.job_data.session_data.operator_bases
        [cont_op_id as usize]
        .chain_id
        .unwrap();
    let cont_input_field =
        if forkcat.op.accessed_names_afterwards.first() == Some(&None) {
            forkcat.output_fields[0]
        } else {
            DUMMY_INPUT_FIELD_ID
        };
    let (start_tf, end_tf, end_reachable) = sess
        .setup_transforms_with_stable_start(
            output_ms_id,
            cont_chain_id,
            cont_op_id,
            cont_input_field,
            &Default::default(),
            true,
        );
    let forkcat = match_unwrap!(
        &mut sess.transform_data[tf_id.get()],
        TransformData::ForkCat(fc),
        fc
    );
    forkcat.continuation = Some(start_tf);
    if end_reachable {
        sess.add_terminator(end_tf, false);
    }
}
pub(crate) fn handle_forkcat_expansion(
    sess: &mut JobSession,
    tf_id: TransformId,
) {
    let fc = match_unwrap!(
        &mut sess.transform_data[tf_id.get()],
        TransformData::ForkCat(fc),
        fc
    );
    let sc_n = fc.curr_subchain_n;
    if fc.op.subchains_start + sc_n == fc.op.subchains_end {
        let cont_id = fc.continuation.unwrap().get();
        for &f in &fc.output_fields {
            sess.job_data
                .field_mgr
                .drop_field_refcount(f, &mut sess.job_data.match_set_mgr);
        }
        for &f in fc
            .input_mirror_fields
            .iter()
            .chain(fc.output_mirror_fields.iter())
        {
            sess.job_data
                .field_mgr
                .drop_field_refcount(f, &mut sess.job_data.match_set_mgr);
        }
        match &mut sess.transform_data[cont_id] {
            TransformData::Nop(nop) => nop.manual_unlink = false,
            TransformData::Terminator(tm) => tm.manual_unlink = false,
            _ => unreachable!(),
        }
        return;
    }
    if sc_n == 0 {
        setup_continuation(sess, tf_id);
    } else {
        for &of in &fc.output_fields {
            let mut f = sess.job_data.field_mgr.fields[of].borrow_mut();
            f.iter_hall.reset_iterators();
            assert!(f.get_clear_delay_request_count() == 0);
            let msm = &mut sess.job_data.match_set_mgr.match_sets[f.match_set];
            msm.command_buffer
                .drop_field_commands(of, &mut f.action_indices);
        }
    }
    expand_for_subchain(sess, tf_id, sc_n);
}

pub fn create_op_forkcat() -> OperatorData {
    OperatorData::ForkCat(OpForkCat::default())
}
