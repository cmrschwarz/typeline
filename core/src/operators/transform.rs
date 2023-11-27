use nonmax::NonMaxUsize;
use smallstr::SmallString;

use crate::{
    job_session::{JobData, JobSession},
    record_data::{
        field::FieldId, field_data::FieldValueKind, match_set::MatchSetId,
        stream_value::StreamValueId,
    },
    utils::small_box::SmallBox,
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

pub type DefaultTransformName = SmallString<[u8; 16]>;
pub type TransformId = NonMaxUsize;

pub enum TransformData<'a> {
    Disabled,
    Nop(TfNop),
    Terminator(TfTerminator),
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
    Custom(SmallBox<dyn Transform, 192>),
}

impl Default for TransformData<'_> {
    fn default() -> Self {
        Self::Disabled
    }
}

impl TransformData<'_> {
    pub fn display_name(&self) -> DefaultTransformName {
        match self {
            TransformData::Disabled => "disabled",
            TransformData::Nop(_) => "nop",
            TransformData::Call(_) => "call",
            TransformData::CallConcurrent(_) => "call-cc",
            TransformData::CalleeConcurrent(_) => "callee-cc",
            TransformData::Cast(_) => "cast",
            TransformData::Count(_) => "count",
            TransformData::Print(_) => "print",
            TransformData::Join(_) => "join",
            TransformData::Select(_) => "select",
            TransformData::StringSink(_) => "string_sink",
            TransformData::Fork(_) => "fork",
            TransformData::ForkCat(_) => "forkcat",
            TransformData::Regex(_) => "regex",
            TransformData::Format(_) => "format",
            TransformData::FileReader(_) => "file_reader",
            TransformData::Literal(_) => "literal",
            TransformData::Sequence(_) => "sequence",
            TransformData::Terminator(_) => "terminator",
            TransformData::Custom(tf) => return tf.display_name(),
        }
        .into()
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
    pub is_stream_producer: bool,
    pub is_ready: bool,
    pub is_appending: bool,
    pub request_uncow: bool,
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
            is_ready: false,
            is_stream_producer: false,
            is_appending: false,
            request_uncow: false,
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

pub trait Transform: Send {
    fn display_name(&self) -> DefaultTransformName;
    fn handle_stream_value_update(
        &mut self,
        _sess: &mut JobData,
        _tf_id: TransformId,
        _sv_id: StreamValueId,
        _custom: usize,
    ) {
        unimplemented!("the transform does not implement stream value updates")
    }
    fn pre_update_required(&self) -> bool {
        false
    }
    fn pre_update(&mut self, _sess: &mut JobSession, _tf_id: TransformId) {}
    fn update(&mut self, jd: &mut JobData, tf_id: TransformId);
}
