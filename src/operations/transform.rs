use std::num::NonZeroUsize;

use nonmax::NonMaxUsize;

use crate::{
    field_data::FieldValueKind,
    worker_thread_session::{FieldId, MatchSetId},
};

use super::{
    data_inserter::TfDataInserter, file_reader::TfFileReader, format::TfFormat,
    operator::OperatorId, print::TfPrint, regex::TfRegex, sequence::TfSequence, split::TfSplit,
    string_sink::TfStringSink,
};

pub type TransformId = NonMaxUsize;
// intentionally incompatible with TransformId to avoid mixups
pub type TransformOrderingId = NonZeroUsize;

pub enum TransformData<'a> {
    Disabled,
    Print(TfPrint),
    StringSink(TfStringSink<'a>),
    Split(TfSplit),
    Regex(TfRegex),
    Format(TfFormat<'a>),
    FileReader(TfFileReader),
    DataInserter(TfDataInserter<'a>),
    Sequence(TfSequence),
}

impl Default for TransformData<'_> {
    fn default() -> Self {
        Self::Disabled
    }
}

pub struct TransformState {
    pub successor: Option<TransformId>,
    pub predecessor: Option<TransformId>,
    pub continuation: Option<TransformId>, // next transform in line that is in append mode
    pub input_field: FieldId,
    pub available_batch_size: usize,
    pub desired_batch_size: usize,
    pub match_set_id: MatchSetId,
    pub op_id: OperatorId,
    pub ordering_id: TransformOrderingId,
    pub is_stream_producer: bool,
    pub is_stream_subscriber: bool,
    pub is_ready: bool,
    pub is_appending: bool,
    pub preferred_input_type: Option<FieldValueKind>,
}
