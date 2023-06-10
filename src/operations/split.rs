use std::collections::HashMap;

use bstring::bstr;
use nonmax::NonMaxUsize;
use smallvec::{smallvec, SmallVec};

use crate::{
    chain::ChainId,
    options::{argument::CliArgIdx, range_spec::RangeSpec},
    scratch_vec::ScratchVec,
    string_store::StringStoreEntry,
    worker_thread_session::{FieldId, MatchSetId, TransformId, WorkerThreadSession},
};

use super::{
    operator_base::OperatorId,
    transform_state::{TransformData, TransformState},
    OperatorCreationError,
};

#[derive(Clone)]
pub struct OpSplit {
    pub range_spec: RangeSpec<ChainId>,
    pub target_operators: Vec<OperatorId>,
}

pub struct TfSplit {
    expanded: bool,
    // Operator Ids before expansion, transform ids after
    targets: Vec<NonMaxUsize>,
    field_names_set: HashMap<StringStoreEntry, SmallVec<[FieldId; 2]>>,
}

pub fn parse_split_op(
    value: Option<&bstr>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OpSplit, OperatorCreationError> {
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
    Ok(OpSplit {
        range_spec,
        target_operators: Default::default(),
    })
}

pub fn setup_ts_split_as_entry_point<'a, 'b>(
    sess: &mut WorkerThreadSession<'a>,
    input_field: FieldId,
    ms_id: MatchSetId,
    entry_count: usize,
    ops: impl Iterator<Item = &'b OperatorId> + Clone,
) -> TransformState<'a> {
    TransformState {
        input_field: input_field,
        data: TransformData::Split(Some(TfSplit {
            expanded: false,
            targets: ops
                .clone()
                .map(|op| (*op as usize).try_into().unwrap())
                .collect(),
            field_names_set: Default::default(),
        })),
        available_batch_size: entry_count,
        stream_producers_slot_index: None,
        stream_successor: None,
        match_set_id: ms_id,
        successor: None,
        desired_batch_size: ops.clone().fold(usize::MAX, |minimum_batch_size, op| {
            let cid = sess.session_data.operator_bases[*op as usize].chain_id;
            minimum_batch_size.min(
                sess.session_data.chains[cid as usize]
                    .settings
                    .default_batch_size,
            )
        }),
    }
}

pub fn setup_tf_split(op: &OpSplit) -> Option<TfSplit> {
    Some(TfSplit {
        expanded: false,
        targets: op
            .target_operators
            .iter()
            .map(|op| (*op as usize).try_into().unwrap())
            .collect(),
        field_names_set: Default::default(),
    })
}

pub fn handle_split(
    sess: &mut WorkerThreadSession,
    stream_mode: bool,
    tf_id: TransformId,
    mut s: TfSplit,
) -> TfSplit {
    let tf = &mut sess.transforms[tf_id];
    let tf_ms_id = tf.match_set_id;
    let bs = tf.available_batch_size;
    tf.available_batch_size = 0;
    sess.ready_queue.pop();
    if !s.expanded {
        for i in 0..s.targets.len() {
            let op = s.targets[i];
            let ms_id = sess.add_match_set();
            let input_field = sess.add_field(tf_ms_id, None);
            let _tf = sess.setup_transforms_from_op(
                ms_id,
                0,
                <NonMaxUsize as TryInto<usize>>::try_into(op).unwrap() as OperatorId,
                input_field,
            );
        }
    }
    if stream_mode {
        todo!();
    } else {
        //TODO: detect invalidations somehow instead
        s.field_names_set.clear();
        //TODO: do something clever, per target, cow, etc. instead of this dumb copy
        for field_id in sess.match_sets[tf_ms_id].working_set.iter() {
            // we should only have named fields in the working set (?)
            if let Some(name) = sess.fields[*field_id].name {
                s.field_names_set
                    .entry(name)
                    .or_insert_with(|| smallvec![])
                    .push(*field_id);
            }
        }
        for (name, targets) in &mut s.field_names_set {
            let source_id = *sess.match_sets[tf_ms_id]
                .field_name_map
                .get(&name)
                .unwrap()
                .back()
                .unwrap();
            let mut offset: usize = 0;
            let mut source = None;
            let mut rem = sess.fields.as_mut_slice();

            // the following is a annoyingly complicated way to gathering a
            // list of mutable references from a list of indices without using unsafe
            let mut targets_arr = ScratchVec::new(&mut sess.scrach_memory);
            targets.sort();

            for i in targets.iter() {
                let (tgt, rem_new) = rem.split_at_mut(usize::from(*i) as usize - offset + 1);
                offset += tgt.len();
                let fd = &mut tgt[tgt.len() - 1].field_data;
                if source_id == *i {
                    source = Some(fd);
                } else {
                    targets_arr.push(fd);
                }
                rem = rem_new;
            }

            source.unwrap().copy_n(bs, targets_arr.as_mut_slice());
        }
    }
    debug_assert!(sess.transforms[tf_id].successor.is_none());
    s
}
