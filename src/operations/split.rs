use std::collections::HashMap;

use bstr::BStr;

use smallvec::SmallVec;

use crate::{
    options::argument::CliArgIdx,
    utils::string_store::StringStoreEntry,
    worker_thread_session::{FieldId, JobData, MatchSetId},
};

use super::{
    errors::OperatorCreationError,
    operator::{OperatorBase, OperatorData, OperatorId},
    transform::{TransformData, TransformId, TransformState},
};

#[derive(Clone)]
pub struct OpSplit {}

pub struct TfSplit {
    pub expanded: bool,
    // Operator Ids before expansion, transform ids after
    pub targets: Vec<TransformId>,
    pub field_names_set: HashMap<StringStoreEntry, SmallVec<[FieldId; 2]>>,
}

pub fn parse_op_split(
    _value: Option<&BStr>,
    _arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    todo!();
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
    todo!();
}

pub fn handle_tf_split(_sess: &mut JobData, _tf_id: TransformId, _s: &mut TfSplit) {
    todo!();
}
