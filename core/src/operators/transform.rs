use nonmax::NonMaxUsize;
use smallstr::SmallString;

use crate::{
    context::SessionData,
    job::{Job, JobData, PipelineState, TransformManager},
    record_data::{
        field::{FieldId, FieldManager},
        iter_hall::IterId,
        iters::{BoundedIter, DestructuredFieldDataRef, Iter},
        match_set::{MatchSetId, MatchSetManager},
        ref_iter::AutoDerefIter,
        stream_value::{StreamValueId, StreamValueManager},
    },
    utils::small_box::SmallBox,
};

use super::{
    aggregator::{TfAggregatorHeader, TfAggregatorTrailer},
    call::TfCall,
    call_concurrent::{TfCallConcurrent, TfCalleeConcurrent},
    cast::TfCast,
    count::TfCount,
    field_value_sink::TfFieldValueSink,
    file_reader::TfFileReader,
    foreach::{TfForeachHeader, TfForeachTrailer},
    fork::TfFork,
    forkcat::TfForkCat,
    format::TfFormat,
    input_done_eater::TfInputDoneEater,
    join::TfJoin,
    literal::TfLiteral,
    nop::TfNop,
    nop_copy::TfNopCopy,
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
pub type StreamProducerIndex = NonMaxUsize;

pub enum TransformData<'a> {
    Disabled,
    Nop(TfNop),
    NopCopy(TfNopCopy),
    InputDoneEater(TfInputDoneEater),
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
    FieldValueSink(TfFieldValueSink<'a>),
    Fork(TfFork<'a>),
    ForkCat(TfForkCat<'a>),
    Regex(TfRegex),
    Format(TfFormat<'a>),
    FileReader(TfFileReader),
    Literal(TfLiteral<'a>),
    Sequence(TfSequence),
    AggregatorHeader(TfAggregatorHeader),
    AggregatorTrailer(TfAggregatorTrailer),
    ForeachHeader(TfForeachHeader),
    ForeachTrailer(TfForeachTrailer),
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
            TransformData::NopCopy(_) => "nop-c",
            TransformData::Call(_) => "call",
            TransformData::CallConcurrent(_) => "callcc",
            TransformData::CalleeConcurrent(_) => "callcc_callee",
            TransformData::Cast(_) => "cast",
            TransformData::Count(_) => "count",
            TransformData::Print(_) => "print",
            TransformData::Join(_) => "join",
            TransformData::Select(_) => "select",
            TransformData::StringSink(_) => "string_sink",
            TransformData::FieldValueSink(_) => "field_value_sink",
            TransformData::Fork(_) => "fork",
            TransformData::ForkCat(_) => "forkcat",
            TransformData::Regex(_) => "regex",
            TransformData::Format(_) => "format",
            TransformData::FileReader(_) => "file_reader",
            TransformData::Literal(_) => "literal",
            TransformData::Sequence(_) => "sequence",
            TransformData::Terminator(_) => "terminator",
            TransformData::AggregatorHeader(_) => "aggregator_header",
            TransformData::AggregatorTrailer(_) => "aggregator_trailer",
            TransformData::ForeachHeader(_) => "each_header",
            TransformData::ForeachTrailer(_) => "each_trailer",
            TransformData::InputDoneEater(_) => "input_done_eater",
            TransformData::Custom(tf) => return tf.display_name(),
        }
        .into()
    }
}

pub struct TransformState {
    pub successor: Option<TransformId>,
    pub input_field: FieldId,
    pub output_field: FieldId,
    pub available_batch_size: usize,
    pub desired_batch_size: usize,
    pub match_set_id: MatchSetId,
    pub op_id: Option<OperatorId>,
    pub is_stream_producer: bool,
    pub is_ready: bool,
    pub is_transparent: bool,
    // means that the a transform that has us as it's successor indicated to
    // us that it will not produce any more records
    pub predecessor_done: bool,
    // means that this transform will not produce any more records
    pub done: bool,
    pub mark_for_removal: bool,
}

impl TransformState {
    pub fn new(
        input_field: FieldId,
        output_field: FieldId,
        ms_id: MatchSetId,
        desired_batch_size: usize,
        op_id: Option<OperatorId>,
    ) -> Self {
        TransformState {
            available_batch_size: 0,
            input_field,
            output_field,
            match_set_id: ms_id,
            desired_batch_size,
            successor: None,
            op_id,
            is_ready: false,
            is_stream_producer: false,
            is_transparent: false,
            predecessor_done: false,
            done: false,
            mark_for_removal: false,
        }
    }
}

pub trait Transform: Send {
    fn display_name(&self) -> DefaultTransformName;
    fn stream_producer_update(
        &mut self,
        _jd: &mut JobData,
        _tf_id: TransformId,
    ) {
        unimplemented!(
            "transform `{}` does not implement stream production",
            self.display_name()
        )
    }
    fn handle_stream_value_update(
        &mut self,
        _jd: &mut JobData,
        _tf_id: TransformId,
        _sv_id: StreamValueId,
        _custom: usize,
    ) {
        unimplemented!(
            "transform `{}` does not implement stream value updates",
            self.display_name()
        )
    }
    fn pre_update_required(&self) -> bool {
        false
    }
    fn pre_update(&mut self, _sess: &mut Job, _tf_id: TransformId) {}
    fn update(&mut self, jd: &mut JobData, tf_id: TransformId);
}

// a helper type around JobData that works around the fact that
// TransformUtils::basic_update needs
// to borrow the field manager in order to produce the iterator
// that it forwards into its closure
pub struct BasicUpdateData<'a, 'b> {
    pub session_data: &'a SessionData,
    pub tf_mgr: &'a mut TransformManager,
    pub match_set_mgr: &'a mut MatchSetManager,
    pub field_mgr: &'a FieldManager,
    pub sv_mgr: &'a mut StreamValueManager,
    pub temp_vec: &'a mut Vec<u8>,
    pub batch_size: usize,
    pub ps: PipelineState,
    pub input_field_id: FieldId,
    pub output_field_id: FieldId,
    pub match_set_id: MatchSetId,
    pub tf_id: TransformId,
    pub iter: &'a mut AutoDerefIter<
        'b,
        BoundedIter<'b, Iter<'b, DestructuredFieldDataRef<'b>>>,
    >,
}

pub fn basic_transform_update(
    jd: &mut JobData,
    tf_id: TransformId,
    // if this is None, assume the output field of the transform
    // is the only output
    extra_output_fields: impl IntoIterator<Item = FieldId>,
    input_iter_id: IterId,
    mut f: impl for<'b> FnMut(BasicUpdateData<'_, 'b>) -> (usize, bool),
) {
    let (batch_size, ps) = jd.tf_mgr.claim_batch(tf_id);
    let tf = &jd.tf_mgr.transforms[tf_id];
    let output_field_id = tf.output_field;
    let match_set_id = tf.match_set_id;
    jd.tf_mgr.prepare_for_output(
        &mut jd.field_mgr,
        &mut jd.match_set_mgr,
        tf_id,
        std::iter::once(output_field_id).chain(extra_output_fields),
    );

    let input_field_id = jd.tf_mgr.get_input_field_id(&jd.field_mgr, tf_id);
    let input_field = jd
        .field_mgr
        .get_cow_field_ref(&mut jd.match_set_mgr, input_field_id);
    let mut iter = jd.field_mgr.get_auto_deref_iter(
        input_field_id,
        &input_field,
        input_iter_id,
        batch_size,
    );
    let (produced_fields, done) = f(BasicUpdateData {
        field_mgr: &jd.field_mgr,
        session_data: jd.session_data,
        tf_mgr: &mut jd.tf_mgr,
        match_set_mgr: &mut jd.match_set_mgr,
        sv_mgr: &mut jd.sv_mgr,
        temp_vec: &mut jd.temp_vec,
        batch_size,
        ps,
        input_field_id,
        output_field_id,
        match_set_id,
        tf_id,
        iter: &mut iter,
    });
    jd.field_mgr.store_iter(input_field_id, input_iter_id, iter);
    jd.tf_mgr.submit_batch(tf_id, produced_fields, done);
}
