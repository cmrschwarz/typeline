use nonmax::NonMaxUsize;

use crate::{
    job_session::{FieldId, MatchSetId},
    record_data::field_data::FieldValueKind,
};

use super::{
    call::TfCall,
    call_concurrent::{TfCallConcurrent, TfCalleeConcurrent},
    cast::TfCast,
    count::TfCount,
    file_reader::TfFileReader,
    fork::TfFork,
    forkcat::TfForkCat,
    format::TfFormat,
    join::TfJoin,
    literal::TfLiteral,
    nop::TfNop,
    operator::OperatorId,
    print::TfPrint,
    regex::TfRegex,
    select::TfSelect,
    sequence::TfSequence,
    string_sink::TfStringSink,
    terminator::TfTerminator,
};

pub type TransformId = NonMaxUsize;
// intentionally incompatible with TransformId to avoid mixups
pub type TransformOrderingId = NonMaxUsize;

pub enum TransformData<'a> {
    Disabled,
    Nop(TfNop),
    Call(TfCall),
    CallConcurrent(TfCallConcurrent<'a>),
    CalleeConcurrent(TfCalleeConcurrent),
    Cast(TfCast),
    Count(TfCount),
    Print(TfPrint),
    Join(TfJoin<'a>),
    Select(TfSelect),
    StringSink(TfStringSink<'a>),
    Fork(TfFork<'a>),
    ForkCat(TfForkCat<'a>),
    Regex(TfRegex),
    Format(TfFormat<'a>),
    FileReader(TfFileReader),
    Literal(TfLiteral<'a>),
    Sequence(TfSequence),
    Terminator(TfTerminator),
}

impl Default for TransformData<'_> {
    fn default() -> Self {
        Self::Disabled
    }
}

pub struct TransformState {
    pub successor: Option<TransformId>,
    pub predecessor: Option<TransformId>,
    pub continuation: Option<TransformId>, /* next transform in line that
                                            * is in append mode */
    pub input_field: FieldId,
    pub output_field: FieldId,
    pub any_prev_has_unconsumed_input: bool,
    pub available_batch_size: usize,
    pub desired_batch_size: usize,
    pub match_set_id: MatchSetId,
    pub op_id: Option<OperatorId>,
    pub ordering_id: TransformOrderingId,
    pub is_stream_producer: bool,
    pub is_ready: bool,
    pub is_appending: bool,
    pub is_transparent: bool,
    pub input_is_done: bool,
    pub mark_for_removal: bool,
    pub preferred_input_type: Option<FieldValueKind>,
}

impl TransformState {
    pub fn new(
        input_field: FieldId,
        output_field: FieldId,
        ms_id: MatchSetId,
        desired_batch_size: usize,
        predecessor: Option<TransformId>,
        op_id: Option<OperatorId>,
        ordering_id: TransformOrderingId,
    ) -> Self {
        TransformState {
            available_batch_size: 0,
            input_field,
            output_field,
            match_set_id: ms_id,
            desired_batch_size,
            successor: None,
            continuation: None,
            predecessor,
            op_id,
            ordering_id,
            is_ready: false,
            is_stream_producer: false,
            is_appending: false,
            is_transparent: false,
            input_is_done: false,
            preferred_input_type: None,
            mark_for_removal: false,
            any_prev_has_unconsumed_input: false,
        }
    }
    pub fn has_unconsumed_input(&self) -> bool {
        self.any_prev_has_unconsumed_input || self.available_batch_size > 0
    }
}
