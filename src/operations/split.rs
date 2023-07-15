use bstr::BStr;

use crate::{
    options::argument::CliArgIdx,
    worker_thread_session::{FieldId, JobData, MatchSetId},
};

use super::{
    errors::OperatorCreationError,
    operator::{OperatorBase, OperatorData, OperatorId},
    transform::{TransformData, TransformId, TransformState},
};

#[derive(Clone)]
pub struct OpSplit {}

pub struct TfSplitFieldMapping {
    pub source: FieldId,
    pub target: FieldId,
}

pub struct TfSplitTarget {
    pub tf_id: TransformId,
    pub fields: Vec<TfSplitFieldMapping>,
}

pub struct TfSplit {
    pub targets: Vec<TfSplitTarget>,
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
    todo!();
}

pub fn handle_tf_split(_sess: &mut JobData, _tf_id: TransformId, _s: &mut TfSplit) {
    todo!();
}
