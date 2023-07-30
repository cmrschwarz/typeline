use std::{
    cell::Cell,
    collections::{hash_map::Entry, HashMap},
};

use bitvec::vec::BitVec;

use crate::{
    chain::Chain,
    job_session::{FieldId, JobData, JobSession},
    liveness_analysis::{LivenessData, LOCAL_SLOTS_PER_BASIC_BLOCK},
    options::argument::CliArgIdx,
    utils::identity_hasher::BuildIdentityHasher,
};

use super::{
    errors::{OperatorCreationError, OperatorSetupError},
    operator::{OperatorBase, OperatorData, OperatorId},
    transform::{TransformData, TransformId, TransformState},
    utils::field_access_mappings::{
        FieldAccessMappings, FieldAccessMode, WriteCountingAccessMappings,
    },
};

#[derive(Clone, Default)]
pub struct OpForkCat {
    pub subchain_count_before: u32,
    pub subchain_count_after: u32,
    pub accessed_fields: WriteCountingAccessMappings,
    pub accessed_fields_per_subchain: Vec<FieldAccessMappings>,
}

pub struct TfForkCatFieldMapping {
    pub source_field_id: FieldId,
    pub target_field_id: FieldId,
    pub writer: bool,
    pub last_access: bool,
}

pub struct TfForkCat<'a> {
    pub curr_subchain: u32,
    pub curr_target: Option<TransformId>,
    pub buffered_record_count: usize,
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
    let bb = &ld.basic_blocks[bb_id];
    let var_count = ld.vars.len();

    let succ_var_data = &ld.var_data[ld.get_succession_var_data_bounds(bb_id)];
    op.accessed_fields =
        WriteCountingAccessMappings::from_var_data(0, ld, succ_var_data);

    let mut call = BitVec::<Cell<usize>>::new();
    let mut successors = BitVec::<Cell<usize>>::new();
    call.resize(var_count * LOCAL_SLOTS_PER_BASIC_BLOCK, false);
    successors.resize(var_count * LOCAL_SLOTS_PER_BASIC_BLOCK, false);
    ld.get_global_var_data_ored(&mut successors, bb.successors.iter());
    for callee_id in &bb.calls {
        call.copy_from_bitslice(ld.get_global_var_data(bb_id));
        ld.apply_bb_aliases(
            &mut call,
            &successors,
            &ld.basic_blocks[*callee_id],
        );
        op.accessed_fields_per_subchain
            .push(FieldAccessMappings::from_var_data((), ld, &call));
        op.accessed_fields.append_var_data(0, ld, succ_var_data);
    }
}

pub fn setup_tf_forkcat<'a>(
    _sess: &mut JobData,
    _op_base: &OperatorBase,
    op: &'a OpForkCat,
    _tf_state: &mut TransformState,
) -> TransformData<'a> {
    TransformData::ForkCat(TfForkCat {
        curr_subchain: op.subchain_count_before,
        curr_target: None,
        current_mappings: Default::default(),
        op,
        buffered_record_count: 0,
    })
}

pub fn handle_tf_forkcat(
    _sess: &mut JobData,
    _tf_id: TransformId,
    _fc: &mut TfForkCat,
) {
    todo!();
}

fn expand_for_subchain(
    sess: &mut JobSession,
    tf_id: TransformId,
    sc_n: usize,
) {
    let tgt_ms_id = sess.job_data.match_set_mgr.add_match_set();
    let mut chain_input_field = None;
    let tf = &sess.job_data.tf_mgr.transforms[tf_id];
    let src_input_field_id = tf.input_field;
    let src_ms_id = tf.match_set_id;
    let forkcat = if let TransformData::ForkCat(fc) =
        &mut sess.transform_data[tf_id.get()]
    {
        fc
    } else {
        unreachable!();
    };
    let mut current_mappings =
        std::mem::replace(&mut forkcat.current_mappings, Default::default());
    let combined_field_accesses = &forkcat.op.accessed_fields;
    let accessed_fields_of_sc = &forkcat.op.accessed_fields_per_subchain[sc_n];
    for (name, access_mode) in accessed_fields_of_sc.iter_name_opt() {
        let (src_ms, tgt_ms) = &mut sess
            .job_data
            .match_set_mgr
            .match_sets
            .two_distinct_mut(src_ms_id, tgt_ms_id);
        let mut entry;
        let src_field_id;
        if let Some(name) = name {
            let vacant = match tgt_ms.field_name_map.entry(name) {
                Entry::Occupied(_) => continue,
                Entry::Vacant(e) => e,
            };
            if let Some(field) = src_ms.field_name_map.get(&name) {
                // the input field is always first in this iterator
                debug_assert!(*field != src_input_field_id);
                src_field_id = *field;
            } else {
                let target_field_id =
                    sess.job_data.field_mgr.add_field(tgt_ms_id, None);
                let mut tgt = sess.job_data.field_mgr.fields[target_field_id]
                    .borrow_mut();
                tgt.added_as_placeholder_by_tf = Some(tf_id);
                vacant.insert(target_field_id);
                continue;
            };
            entry = Some(vacant);
        } else {
            if chain_input_field.is_some() {
                continue;
            }
            src_field_id = src_input_field_id;
            entry = None;
        };
        let mut src_field =
            sess.job_data.field_mgr.fields[src_field_id].borrow_mut();
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
        let writer = access_mode != FieldAccessMode::Read;
        let _any_writes = write_count > 0; // TODO
        let last_access = combined_fam.last_accessing_sc == sc_n as u32;

        let target_field_id = if writer && !last_access {
            drop(src_field);
            let target = sess.job_data.field_mgr.add_field(tgt_ms_id, None);
            src_field =
                sess.job_data.field_mgr.fields[src_field_id].borrow_mut();
            target
        } else {
            src_field_id
        };
        current_mappings.insert(
            src_field_id,
            TfForkCatFieldMapping {
                source_field_id: src_field_id,
                target_field_id,
                writer,
                last_access,
            },
        );
        let mut tgt_field = if target_field_id != src_field_id {
            let mut tgt =
                sess.job_data.field_mgr.fields[target_field_id].borrow_mut();
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
            if accessed_fields_of_sc.fields.contains_key(other_name) {
                tgt_ms.field_name_map.insert(*other_name, target_field_id);
                tgt_field.as_mut().map(|f| f.names.push(*other_name));
            }
        }
        if name.is_none() {
            chain_input_field = Some(target_field_id);
        }
    }
    if let TransformData::ForkCat(ref mut forkcat) =
        sess.transform_data[usize::from(tf_id)]
    {
        forkcat.current_mappings = current_mappings;
    } else {
        unreachable!();
    }
}

pub(crate) fn handle_forkcat_expansion(
    sess: &mut JobSession,
    tf_id: TransformId,
) {
    expand_for_subchain(sess, tf_id, 0);
}

pub fn create_op_forkcat() -> OperatorData {
    OperatorData::ForkCat(OpForkCat::default())
}
