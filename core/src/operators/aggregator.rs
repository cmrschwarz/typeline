#![allow(unused)] // TODO

use std::collections::HashMap;

use crate::{
    job_session::JobData,
    liveness_analysis::OpOutputIdx,
    record_data::{field::FieldId, stream_value::StreamValueId},
    utils::identity_hasher::BuildIdentityHasher,
};

use super::{
    operator::OperatorId,
    transform::{TransformData, TransformId, TransformState},
};

pub struct OpAggregator {
    pub aggregate_starter_is_appending: bool,
    pub sub_ops: Vec<OperatorId>,
}

pub struct TfAggregator {
    pub(crate) current_sub_tf: TransformId,
    pub(crate) current_sub_tf_idx: usize,
    pub(crate) sub_tfs: Vec<TransformId>,
}

pub fn handle_tf_aggregator() {}

pub fn handle_tf_aggregator_stream_value_update(
    _fr: &mut TfAggregator,
    _sess: &mut JobData,
    _tf_id: TransformId,
    _sv_id: StreamValueId,
    _custom: usize,
) {
    todo!();
}

pub fn handle_tf_aggregator_stream_producer_update(
    _op: &mut TfAggregator,
    _sess: &mut JobData,
    _tf_id: TransformId,
) {
    todo!()
}
