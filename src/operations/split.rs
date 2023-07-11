use std::{cell::RefMut, collections::HashMap};

use bstr::{BStr, ByteSlice};

use nonmax::NonMaxUsize;
use smallvec::{smallvec, SmallVec};

use crate::{
    chain::ChainId,
    field_data::{iter_hall::IterHall, iters::FieldIterator},
    options::{argument::CliArgIdx, range_spec::RangeSpec},
    utils::string_store::StringStoreEntry,
    worker_thread_session::{Field, FieldId, JobData, MatchSetId},
};

use super::{
    errors::OperatorCreationError,
    operator::{OperatorData, OperatorId},
    transform::{TransformData, TransformId, TransformState},
};

#[derive(Clone)]
pub struct OpSplit {
    pub range_spec: RangeSpec<ChainId>,
    pub target_operators: Vec<OperatorId>,
}

pub struct TfSplit {
    pub expanded: bool,
    // Operator Ids before expansion, transform ids after
    pub targets: Vec<NonMaxUsize>,
    pub field_names_set: HashMap<StringStoreEntry, SmallVec<[FieldId; 2]>>,
}

pub fn parse_op_split(
    value: Option<&BStr>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    let range_spec = if let Some(value) = value {
        RangeSpec::<ChainId>::parse(value.to_str().map_err(|_| {
            OperatorCreationError::new(
                "failed to parse split argument as range spec: invalid UTF-8",
                arg_idx,
            )
        })?)
        .map_err(|_| {
            OperatorCreationError::new("failed to parse split argument as range spec", arg_idx)
        })?
    } else {
        RangeSpec::Bounded(Some(0), None)
    };
    Ok(OperatorData::Split(OpSplit {
        range_spec,
        target_operators: Default::default(),
    }))
}

pub fn setup_ts_split_as_entry_point<'a, 'b>(
    sess: &mut JobData<'a>,
    input_field: FieldId,
    ms_id: MatchSetId,
    entry_count: usize,
    ops: impl Iterator<Item = &'b OperatorId> + Clone,
) -> (TransformState, TransformData<'a>) {
    let state = TransformState {
        input_field,
        available_batch_size: entry_count,
        match_set_id: ms_id,
        successor: None,
        continuation: None,
        predecessor: None,
        desired_batch_size: ops.clone().fold(usize::MAX, |minimum_batch_size, op| {
            let cid = sess.session_data.operator_bases[*op as usize].chain_id;
            minimum_batch_size.min(
                sess.session_data.chains[cid as usize]
                    .settings
                    .default_batch_size,
            )
        }),
        op_id: OperatorId::MAX,
        ordering_id: sess.tf_mgr.claim_transform_ordering_id(),
        is_ready: false,
        is_stream_producer: false,
        is_stream_subscriber: false,
        is_appending: false,
        preferred_input_type: None,
        done_if_input_done: true,
    };
    let data = TransformData::Split(TfSplit {
        expanded: false,
        targets: ops
            .clone()
            .map(|op| (*op as usize).try_into().unwrap())
            .collect(),
        field_names_set: Default::default(),
    });
    (state, data)
}

pub fn setup_tf_split<'a>(
    _sess: &mut JobData,
    op: &'a OpSplit,
    tf_state: &mut TransformState,
) -> (TransformData<'static>, FieldId) {
    let tf = TfSplit {
        expanded: false,
        targets: op
            .target_operators
            .iter()
            .map(|op| (*op as usize).try_into().unwrap())
            .collect(),
        field_names_set: Default::default(),
    };
    (TransformData::Split(tf), tf_state.input_field)
}

pub fn handle_tf_split(sess: &mut JobData, tf_id: TransformId, s: &mut TfSplit) {
    let tf = &mut sess.tf_mgr.transforms[tf_id];
    let tf_ms_id = tf.match_set_id;
    let bs = tf.available_batch_size;
    tf.available_batch_size = 0;
    sess.tf_mgr.ready_queue.pop();
    //TODO: detect invalidations somehow instead
    s.field_names_set.clear();
    //TODO: do something clever, per target, cow, etc. instead of this dumb copy
    for field_id in sess.record_mgr.match_sets[tf_ms_id].working_set.iter() {
        // we should only have named fields in the working set (?)
        if let Some(name) = sess.record_mgr.fields[*field_id].borrow().name {
            s.field_names_set
                .entry(name)
                .or_insert_with(|| smallvec![])
                .push(*field_id);
        }
    }
    for (name, targets) in &mut s.field_names_set {
        let source_id = *sess.record_mgr.match_sets[tf_ms_id]
            .field_name_map
            .get(&name)
            .unwrap()
            .back()
            .unwrap();
        let source = sess.record_mgr.fields[source_id].borrow();
        let mut targets_borrows_arr: SmallVec<[RefMut<'_, Field>; 8]> = Default::default();
        for i in targets.iter() {
            targets_borrows_arr.push(sess.record_mgr.fields[*i].borrow_mut());
        }
        IterHall::copy(source.field_data.iter().bounded(0, bs), |f| {
            targets_borrows_arr
                .iter_mut()
                .for_each(|fd| f(&mut fd.field_data));
        });
    }
    debug_assert!(sess.tf_mgr.transforms[tf_id].successor.is_none());
}
