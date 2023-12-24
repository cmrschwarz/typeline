use super::{operator::OperatorId, transform::TransformId};

pub struct OpAggregator {
    pub aggregate_starter_is_appending: bool,
    pub sub_ops: Vec<OperatorId>,
}

pub struct TfAggregatorHeader {
    pub(crate) trailer_tf_id: TransformId,
    pub(crate) sub_tfs: Vec<TransformId>,
}

pub struct TfAggregatorTrailer {
    pub(crate) curr_sub_tf_idx: usize,
    pub(crate) sub_tf_count: usize,
}
