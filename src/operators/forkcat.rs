use std::{
    cell::Cell,
    collections::{hash_map::Entry, HashMap},
};

use bitvec::vec::BitVec;

use crate::{
    chain::Chain,
    context::Session,
    job_session::{JobData, JobSession},
    liveness_analysis::{
        LivenessData, OpOutputIdx, Var, LOCAL_SLOTS_PER_BASIC_BLOCK,
    },
    options::argument::CliArgIdx,
    record_data::{
        field::{FieldId, DUMMY_INPUT_FIELD_ID},
        iter_hall::{IterHall, IterId},
        iters::FieldDataRef,
    },
    utils::{
        identity_hasher::BuildIdentityHasher, string_store::StringStoreEntry,
    },
};

use super::{
    errors::{OperatorCreationError, OperatorSetupError},
    operator::{
        OperatorBase, OperatorData, OperatorId, OperatorOffsetInChain,
    },
    transform::{TransformData, TransformId, TransformState},
    utils::field_access_mappings::{
        AccessKind, AccessMappings, FieldAccessMappings, FieldAccessMode,
        WriteCountingAccessMappings,
    },
};

#[derive(Clone, Default)]
pub struct OpForkCat {
    pub subchains_start: u32,
    pub subchains_end: u32,
    pub continuation: Option<OperatorId>,
    // includes successors. used to select fields to copy / cow into subchain
    pub accessed_fields_per_subchain: Vec<FieldAccessMappings>,
    // does *not* include successors. used to decide whether to move or copy
    // the data into the subchain based on whether it is the last access
    pub accessed_fields_of_any_subchain: WriteCountingAccessMappings,
    // which op outputs of the chain should be prealloced to point to which
    // output name (eventually field in the tf)
    // parallel to accessed_names_afterwards
    output_mappings_per_subchain: Vec<Vec<Option<OpOutputIdx>>>,
    // name -> index into accessed_names_afterwards
    accessed_names_afterwards_map:
        AccessMappings<AccessedNamesAfterwardsIndex>,
    accessed_names_afterwards: Vec<Option<StringStoreEntry>>,
}

#[derive(Default, Clone)]
struct AccessedNamesAfterwardsIndex(usize);

impl AccessKind for AccessedNamesAfterwardsIndex {
    type ContextType = usize;

    fn from_field_access_mode(
        output_idx: &mut usize,
        _fam: FieldAccessMode,
    ) -> Self {
        let idx = *output_idx;
        *output_idx += 1;
        AccessedNamesAfterwardsIndex(idx)
    }

    fn append_field_access_mode(
        &mut self,
        _output_idx: &mut usize,
        _fam: FieldAccessMode,
    ) {
    }
}

pub struct TfForkCatOutputMapping {
    pub subchain_field_id: FieldId,
    pub output_field_id: FieldId,
}

pub struct TfForkCatInputMapping {
    pub source_field_id: FieldId,
    pub source_field_iter: IterId,
    pub target_field_id: FieldId,
    pub header_writer: bool,
    pub data_writer: bool,
    pub last_access: bool,
}

pub struct TfForkCat<'a> {
    pub op: &'a OpForkCat,
    pub curr_subchain_n: u32,
    pub curr_subchain_start: Option<TransformId>,
    pub continuation: Option<TransformId>,
    pub buffered_record_count: usize,
    pub input_size: usize,
    // field name -> index into input_mappings
    // because fields can have multiple names, we use this to deduplicate
    // we could make this local to the setup function but meh, this way
    // we can at least reuse the allocation
    pub input_mapping_ids: HashMap<FieldId, usize, BuildIdentityHasher>,
    pub input_mappings: Vec<TfForkCatInputMapping>,
    // temp buffer passed to setup_transforms_from_op
    pub prebound_outputs: HashMap<OpOutputIdx, FieldId, BuildIdentityHasher>,

    // parallel with op.accessed_names_afterwards
    pub output_mappings: Vec<FieldId>,
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

    let succ_var_data = &ld.var_data[ld.get_succession_var_data_bounds(bb_id)];

    let mut call = BitVec::<Cell<usize>>::new();
    let mut successors = BitVec::<Cell<usize>>::new();
    call.resize(var_count * LOCAL_SLOTS_PER_BASIC_BLOCK, false);
    successors.resize(var_count * LOCAL_SLOTS_PER_BASIC_BLOCK, false);
    ld.get_global_var_data_ored(&mut successors, bb.successors.iter());
    for callee_id in &bb.calls {
        call.copy_from_bitslice(ld.get_global_var_data(bb_id));
        op.accessed_fields_of_any_subchain
            .append_var_data(&mut 0, ld, &call);
        ld.apply_bb_aliases(&call, &successors, &ld.basic_blocks[*callee_id]);
        op.accessed_fields_per_subchain
            .push(FieldAccessMappings::from_var_data(&mut (), ld, &call));
    }
    let mut count = 0;
    op.accessed_names_afterwards_map = AccessMappings::<
        AccessedNamesAfterwardsIndex,
    >::from_var_data(
        &mut count, ld, succ_var_data
    );
    op.accessed_names_afterwards.reserve(count);
    for (name, _idx) in op.accessed_names_afterwards_map.iter_name_opt() {
        op.accessed_names_afterwards.push(name);
    }
    for sc_n in 0..op.accessed_fields_per_subchain.len() {
        let mut mappings = Vec::new();
        mappings.resize(op.accessed_names_afterwards.len(), None);
        let sc_id = op.subchains_start as usize + sc_n;
        for op_id in &sess.chains[sc_id].operators {
            for bv in &ld.op_outputs[*op_id as usize].bound_vars_after_bb {
                let var_name = match ld.vars[*bv as usize] {
                    Var::Named(name) => Some(name),
                    Var::BBInput => None,
                    Var::UnreachableDummyVar => continue,
                };
                if let Some(binding_after) =
                    op.accessed_names_afterwards_map.get(var_name)
                {
                    mappings[binding_after.0] = Some(*op_id);
                }
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
        input_mapping_ids: Default::default(),
        op,
        input_size: 0,
        buffered_record_count: 0,
        input_mappings: Default::default(),
        prebound_outputs: Default::default(),
        output_mappings: Vec::with_capacity(
            op.accessed_names_afterwards.len(),
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
    let match_set_mgr = &mut sess.match_set_mgr;
    sess.tf_mgr.prepare_for_output(
        &sess.field_mgr,
        match_set_mgr,
        tf_id,
        fc.input_mappings.iter().map(|im| im.target_field_id),
    );
    for m in fc.input_mappings.iter_mut() {
        sess.field_mgr
            .apply_field_actions(match_set_mgr, m.source_field_id);
        let src_field = sess
            .field_mgr
            .get_cow_field_ref(m.source_field_id, unconsumed_input);
        let src_field_dr = src_field.destructured_field_ref().clone();
        let src_field_iter = sess.field_mgr.lookup_iter(
            m.source_field_id,
            &src_field,
            m.source_field_iter,
        );
        let mut tgt = sess.field_mgr.fields[m.target_field_id].borrow_mut();
        if !tgt.field_data.are_headers_owned() {
            continue;
        }
        if !tgt.field_data.is_data_owned() {
            unsafe {
                tgt.field_data
                    .internals()
                    .header
                    .extend_from_slice(src_field_dr.headers());
            }
        }
        IterHall::copy(src_field_iter, &mut |f| f(&mut tgt.field_data));
    }
    if !end_of_input {
        if batch_size == 0 {
            sess.tf_mgr.push_tf_in_ready_stack(tf_id);
        } else {
            sess.tf_mgr.update_ready_state(tf_id);
        }
    }
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
    if fc.curr_subchain_n == 0 && fc.curr_subchain_start.is_some() {
        let (batch_size, end_of_input) = sess.tf_mgr.claim_all(tf_id);
        fc.input_size += batch_size;
        handle_tf_forkcat_sc(sess, tf_id, fc, batch_size, end_of_input);
        if !end_of_input {
            return;
        }
    } else {
        handle_tf_forkcat_sc(sess, tf_id, fc, fc.input_size, true);
    }
    if fc.op.subchains_start + fc.curr_subchain_n == fc.op.subchains_end {
        let cont = fc.continuation.unwrap();
        sess.tf_mgr.transforms[cont].input_is_done = true;
        sess.tf_mgr.push_tf_in_ready_stack(fc.continuation.unwrap());
        sess.unlink_transform(tf_id, 0);
        return;
    }
    sess.tf_mgr
        .disconnect_tf_from_predecessor(fc.continuation.unwrap());
    fc.curr_subchain_n += 1;
    fc.curr_subchain_start = None;
}
fn expand_for_subchain(sess: &mut JobSession, tf_id: TransformId, sc_n: u32) {
    let tgt_ms_id = sess.job_data.match_set_mgr.add_match_set();
    let mut chain_input_field = None;
    let tf = &sess.job_data.tf_mgr.transforms[tf_id];
    let src_input_field_id = tf.input_field;
    let src_ms_id = tf.match_set_id;
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
    let mut input_mapping_ids = std::mem::take(&mut forkcat.input_mapping_ids);
    let mut input_mappings = std::mem::take(&mut forkcat.input_mappings);
    let mut prebound_outputs = std::mem::take(&mut forkcat.prebound_outputs);
    input_mapping_ids.clear();
    prebound_outputs.clear();
    input_mappings.clear();
    let src_ms = &sess.job_data.match_set_mgr.match_sets[src_ms_id];
    for (i, op) in forkcat.op.output_mappings_per_subchain[sc_n as usize]
        .iter()
        .enumerate()
    {
        let target_field = forkcat.output_mappings[i];
        if let Some(op_id) = op {
            prebound_outputs.insert(*op_id, target_field);
        } else {
            let source_field =
                if let Some(name) = forkcat.op.accessed_names_afterwards[i] {
                    src_ms
                        .field_name_map
                        .get(&name)
                        .copied()
                        .unwrap_or(DUMMY_INPUT_FIELD_ID)
                } else {
                    src_input_field_id
                };
            sess.job_data
                .field_mgr
                .setup_cow(source_field, target_field);
        }
    }

    let combined_field_accesses = &forkcat.op.accessed_fields_of_any_subchain;
    let accessed_fields_of_sc =
        &forkcat.op.accessed_fields_per_subchain[sc_n as usize];
    for (name, access_mode) in accessed_fields_of_sc.iter_name_opt() {
        let (src_ms, tgt_ms) = &mut sess
            .job_data
            .match_set_mgr
            .match_sets
            .two_distinct_mut(src_ms_id, tgt_ms_id);
        let mut entry;
        let source_field_id;
        if let Some(name) = name {
            let vacant = match tgt_ms.field_name_map.entry(name) {
                Entry::Occupied(_) => continue,
                Entry::Vacant(e) => e,
            };
            if let Some(field) = src_ms.field_name_map.get(&name) {
                // the input field is always first in this iterator
                debug_assert!(*field != src_input_field_id);
                source_field_id = *field;
            } else {
                let target_field_id =
                    sess.job_data.field_mgr.add_field(tgt_ms_id, None);
                // let mut tgt =
                // sess.job_data.field_mgr.fields[target_field_id]
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
            source_field_id = src_input_field_id;
            entry = None;
        };
        let mut src_field =
            sess.job_data.field_mgr.fields[source_field_id].borrow_mut();
        let combined_fam = combined_field_accesses.get(name).unwrap();
        // TODO: differentiate header writes?
        let mut write_count = combined_fam.total_write_count();
        for other_name in &src_field.names {
            if write_count > 1 {
                break;
            }
            if Some(*other_name) == name {
                continue;
            }
            write_count += combined_field_accesses
                .get(name)
                .map(|acc| acc.total_write_count())
                .unwrap_or(0);
        }
        let last_access = combined_fam.last_accessing_sc == sc_n;

        let (target_field_id, mut tgt_field) =
            if access_mode.any_writes() && !last_access {
                drop(src_field);
                let target_field_id =
                    sess.job_data.field_mgr.add_field(tgt_ms_id, None);
                src_field = sess.job_data.field_mgr.fields[source_field_id]
                    .borrow_mut();
                let mut tgt = sess.job_data.field_mgr.fields[target_field_id]
                    .borrow_mut();
                if let Some(name) = name {
                    tgt.names.push(name);
                }
                (target_field_id, Some(tgt))
            } else {
                (source_field_id, None)
            };
        input_mapping_ids.insert(source_field_id, input_mappings.len());
        input_mappings.push(TfForkCatInputMapping {
            source_field_id,
            target_field_id,
            header_writer: access_mode.header_writes,
            data_writer: access_mode.data_writes,
            last_access,
            source_field_iter: src_field.field_data.claim_iter(),
        });
        entry.take().map(|e| e.insert(target_field_id));
        for other_name in &src_field.names {
            if name == Some(*other_name) {
                continue;
            }
            if accessed_fields_of_sc.fields.contains_key(other_name) {
                tgt_ms.field_name_map.insert(*other_name, target_field_id);
                if let Some(f) = tgt_field.as_mut() {
                    f.names.push(*other_name)
                }
            }
        }
        if name.is_none() {
            chain_input_field = Some(target_field_id);
        }
    }
    let mut i = 0;
    while i < input_mappings.len() {
        let im = &input_mappings[i];
        let source_field_id = im.source_field_id;
        let header_writer = im.header_writer;
        let data_writer = im.data_writer;
        let last_access = im.last_access;
        let mut input_field =
            sess.job_data.field_mgr.fields[im.source_field_id].borrow_mut();
        let mut fr_i = 0;
        let fr_len = input_field.field_refs.len();
        while fr_i < fr_len {
            input_field =
                sess.job_data.field_mgr.fields[source_field_id].borrow_mut();
            let fr = input_field.field_refs[fr_i];

            match input_mapping_ids.entry(fr) {
                Entry::Occupied(_) => continue,
                Entry::Vacant(e) => {
                    e.insert(input_mappings.len());
                    let source_field_iter =
                        input_field.field_data.claim_iter();
                    drop(input_field);
                    let target_field_id =
                        sess.job_data.field_mgr.add_field(tgt_ms_id, None);

                    input_mappings.push(TfForkCatInputMapping {
                        source_field_id: fr,
                        source_field_iter,
                        target_field_id,
                        header_writer,
                        data_writer,
                        last_access,
                    });
                }
            }
            fr_i += 1;
        }
        i += 1;
    }
    let src_ms = &sess.job_data.match_set_mgr.match_sets[src_ms_id];
    for (name, cat) in combined_field_accesses.iter_name_opt() {
        let src_field_id = if let Some(name) = name {
            if let Some(field) = src_ms.field_name_map.get(&name) {
                *field
            } else {
                continue;
            }
        } else {
            src_input_field_id
        };
        let last_access = cat.last_accessing_sc == sc_n;
        if !last_access {
            let src_field =
                sess.job_data.field_mgr.fields[src_field_id].borrow();
            for fr in &src_field.field_refs {
                if let Some(idx) = input_mapping_ids.get(fr) {
                    let im = &mut input_mappings[*idx];
                    im.last_access = false;
                }
            }
        }
    }
    for im in &mut input_mappings {
        if !im.last_access {
            sess.job_data
                .field_mgr
                .setup_cow(im.target_field_id, im.source_field_id);
        }
    }
    let (start_tf, end_tf, end_reachable) = sess
        .setup_transforms_with_stable_start(
            tgt_ms_id,
            sc_id,
            sess.job_data.session_data.chains[sc_id as usize].operators[0],
            chain_input_field.unwrap_or(DUMMY_INPUT_FIELD_ID),
            &prebound_outputs,
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
    forkcat.input_mappings = input_mappings;
    forkcat.input_mapping_ids = input_mapping_ids;
    forkcat.curr_subchain_start = Some(start_tf);
}
fn setup_continuation(sess: &mut JobSession, tf_id: TransformId) {
    let tf = &sess.job_data.tf_mgr.transforms[tf_id];
    let tf_input_field = tf.input_field;
    let src_ms = tf.match_set_id;
    let field_name_map =
        &sess.job_data.match_set_mgr.match_sets[src_ms].field_name_map;
    let forkcat = match_unwrap!(
        &mut sess.transform_data[tf_id.get()],
        TransformData::ForkCat(fc),
        fc
    );
    forkcat.continuation = Some(tf.successor.unwrap());
    for (name, mode) in
        forkcat.op.accessed_fields_of_any_subchain.iter_name_opt()
    {
        if mode.last_accessing_sc != 0 {
            let field_id = if let Some(name) = name {
                if let Some(field) = field_name_map.get(&name) {
                    *field
                } else {
                    continue;
                }
            } else {
                tf_input_field
            };
            let f = sess.job_data.field_mgr.fields[field_id].borrow();
            f.request_clear_delay();
        }
    }
    let cont_op_id = if let Some(cont) = forkcat.op.continuation {
        cont
    } else {
        return;
    };
    let output_ms_id = sess.job_data.match_set_mgr.add_match_set();
    for name in &forkcat.op.accessed_names_afterwards {
        let field_id = sess.job_data.field_mgr.add_field(output_ms_id, None);
        if let Some(name) = name {
            sess.job_data.match_set_mgr.add_field_name(
                &sess.job_data.field_mgr,
                field_id,
                *name,
            );
        }
        forkcat.output_mappings.push(field_id);
    }
    let cont_chain_id = sess.job_data.session_data.operator_bases
        [cont_op_id as usize]
        .chain_id
        .unwrap();
    let cont_input_field =
        if forkcat.op.accessed_names_afterwards.first() == Some(&None) {
            forkcat.output_mappings[0]
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
        );
    let forkcat = match_unwrap!(
        &mut sess.transform_data[tf_id.get()],
        TransformData::ForkCat(fc),
        fc
    );
    if end_reachable {
        sess.job_data
            .tf_mgr
            .connect_tfs(end_tf, forkcat.continuation.unwrap())
    }
    forkcat.continuation = Some(start_tf);
}
pub(crate) fn handle_forkcat_expansion(
    sess: &mut JobSession,
    tf_id: TransformId,
) {
    let forkcat = match_unwrap!(
        &mut sess.transform_data[tf_id.get()],
        TransformData::ForkCat(fc),
        fc
    );
    let sc_n = forkcat.curr_subchain_n;
    if sc_n == 0 {
        setup_continuation(sess, tf_id);
    }
    expand_for_subchain(sess, tf_id, sc_n);
    #[cfg(feature = "debug_logging")]
    sess.log_state(&format!("expanded subchain {sc_n} for forkcat"));
}

pub fn create_op_forkcat() -> OperatorData {
    OperatorData::ForkCat(OpForkCat::default())
}
