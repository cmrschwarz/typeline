use std::num::NonZeroUsize;

use nonmax::NonMaxUsize;

use crate::worker_thread_session::{FieldId, MatchSetId};

use super::{
    data_inserter::TfDataInserter, file_reader::TfFileReader, format::TfFormat,
    operator::OperatorId, print::TfPrint, regex::TfRegex, split::TfSplit,
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
}

impl Default for TransformData<'_> {
    fn default() -> Self {
        Self::Disabled
    }
}

pub struct TransformState {
    pub successor: Option<TransformId>,
    pub predecessor: Option<TransformId>,
    pub input_field: FieldId,
    // at the start of the next batch, this is used to drop the fields from the
    // previous batch swe cannot do this earlier because of field references
    pub last_consumed_batch_size: usize,
    pub available_batch_size: usize,
    pub desired_batch_size: usize,
    pub match_set_id: MatchSetId,
    pub op_id: OperatorId,
    pub ordering_id: TransformOrderingId,
    pub is_stream_producer: bool,
    pub is_batch_producer: bool,
    pub is_ready: bool,
}
