use std::num::NonZeroUsize;

use nonmax::NonMaxUsize;

use crate::worker_thread_session::{FieldId, MatchSetId};

use super::{
    file_reader::TfFileReader, format::TfFormat, operator_base::OperatorId, regex::TfRegex,
    split::TfSplit,
};

pub type TransformId = NonMaxUsize;
// intentionally incompatible with TransformId to avoid mixups
pub type TransformOrderingId = NonZeroUsize;

pub enum TransformData<'a> {
    Disabled,
    Print,
    Split(TfSplit),
    Regex(TfRegex),
    Format(TfFormat<'a>),
    FileReader(TfFileReader<'a>),
}

impl Default for TransformData<'_> {
    fn default() -> Self {
        Self::Disabled
    }
}

pub struct TransformState {
    pub successor: Option<TransformId>,
    pub stream_successor: Option<TransformId>,
    pub stream_producers_slot_index: Option<NonMaxUsize>,
    pub input_field: FieldId,
    pub available_batch_size: usize,
    pub desired_batch_size: usize,
    pub match_set_id: MatchSetId,
    pub op_id: OperatorId,
    pub ordering_id: TransformOrderingId,
}
