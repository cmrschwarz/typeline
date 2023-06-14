use std::num::NonZeroUsize;

use nonmax::NonMaxUsize;

use crate::worker_thread_session::{FieldId, MatchSetId};

use super::{
    file_reader::TfFileReader, format::TfFormat, operator_base::OperatorId, print::TfPrint,
    regex::TfRegex, split::TfSplit,
};

pub type TransformId = NonMaxUsize;
// intentionally incompatible with TransformId to avoid mixups
pub type TransformOrderingId = NonZeroUsize;

pub enum TransformData<'a> {
    Disabled,
    Print(TfPrint),
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
    // when a producer gets ready in stream mode we need to round robin
    // instead of picking him
    pub stream_producers_slot_index: Option<NonMaxUsize>,
    pub input_field: FieldId,
    pub available_batch_size: usize,
    pub desired_batch_size: usize,
    pub match_set_id: MatchSetId,
    pub op_id: OperatorId,
    pub ordering_id: TransformOrderingId,
}
