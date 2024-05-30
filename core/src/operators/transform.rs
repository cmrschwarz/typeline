use std::sync::Arc;

use smallstr::SmallString;

use crate::{
    context::{ContextData, VentureDescription},
    job::{Job, JobData},
    record_data::{
        field::FieldId,
        group_track::GroupTrackId,
        match_set::MatchSetId,
        stream_value::{StreamValueId, StreamValueUpdate},
    },
    utils::{debuggable_nonmax::DebuggableNonMaxUsize, small_box::SmallBox},
};

use super::{
    aggregator::{
        handle_tf_aggregator_header, handle_tf_aggregator_trailer,
        TfAggregatorHeader, TfAggregatorTrailer,
    },
    call::{handle_lazy_call_expansion, TfCall},
    call_concurrent::{
        handle_call_concurrent_expansion, handle_tf_call_concurrent,
        handle_tf_callee_concurrent, TfCallConcurrent, TfCalleeConcurrent,
    },
    count::{handle_tf_count, TfCount},
    field_value_sink::{
        handle_tf_field_value_sink,
        handle_tf_field_value_sink_stream_value_update, TfFieldValueSink,
    },
    file_reader::{
        handle_tf_file_reader, handle_tf_file_reader_stream, TfFileReader,
    },
    foreach::{
        handle_tf_foreach_header, handle_tf_foreach_trailer, TfForeachHeader,
        TfForeachTrailer,
    },
    fork::{handle_fork_expansion, handle_tf_fork, TfFork},
    forkcat::{handle_tf_forkcat, TfForkCat},
    format::{
        handle_tf_format, handle_tf_format_stream_value_update, TfFormat,
    },
    input_done_eater::{handle_tf_input_done_eater, TfInputDoneEater},
    join::{
        handle_tf_join, handle_tf_join_stream_producer_update,
        handle_tf_join_stream_value_update, TfJoin,
    },
    literal::{handle_tf_literal, TfLiteral},
    nop::{handle_tf_nop, TfNop},
    nop_copy::{handle_tf_nop_copy, TfNopCopy},
    operator::OperatorId,
    print::{handle_tf_print, handle_tf_print_stream_value_update, TfPrint},
    regex::{handle_tf_regex, handle_tf_regex_stream_value_update, TfRegex},
    select::{handle_tf_select, TfSelect},
    sequence::{handle_tf_sequence, TfSequence},
    string_sink::{
        handle_tf_string_sink, handle_tf_string_sink_stream_value_update,
        TfStringSink,
    },
    success_updater::{handle_tf_success_updator, TfSuccessUpdator},
    terminator::{handle_tf_terminator, TfTerminator},
    to_str::{
        handle_tf_to_str, handle_tf_to_str_stream_value_update, TfToStr,
    },
};

pub type DefaultTransformName = SmallString<[u8; 16]>;
pub type TransformId = DebuggableNonMaxUsize;
pub type StreamProducerIndex = DebuggableNonMaxUsize;

pub enum TransformData<'a> {
    Disabled,
    Nop(TfNop),
    NopCopy(TfNopCopy),
    InputDoneEater(TfInputDoneEater),
    Terminator(TfTerminator),
    Call(TfCall),
    CallConcurrent(TfCallConcurrent<'a>),
    CalleeConcurrent(TfCalleeConcurrent),
    ToStr(TfToStr),
    Count(TfCount),
    Print(TfPrint),
    Join(TfJoin<'a>),
    Select(TfSelect),
    StringSink(TfStringSink<'a>),
    FieldValueSink(TfFieldValueSink<'a>),
    Fork(TfFork<'a>),
    ForkCat(TfForkCat<'a>),
    Regex(TfRegex<'a>),
    Format(TfFormat<'a>),
    FileReader(TfFileReader),
    Literal(TfLiteral<'a>),
    Sequence(TfSequence),
    AggregatorHeader(TfAggregatorHeader),
    AggregatorTrailer(TfAggregatorTrailer),
    ForeachHeader(TfForeachHeader),
    ForeachTrailer(TfForeachTrailer),
    SuccessUpdator(TfSuccessUpdator),
    Custom(SmallBox<dyn Transform<'a>, 192>),
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
            TransformData::ToStr(_) => "cast",
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
            TransformData::SuccessUpdator(_) => "success_updator",
            TransformData::Custom(tf) => return tf.display_name(),
        }
        .into()
    }
}
#[derive(Clone)]
pub struct TransformState {
    pub successor: Option<TransformId>,
    pub input_field: FieldId,
    pub output_field: FieldId,
    pub input_group_track_id: GroupTrackId,
    pub output_group_track_id: GroupTrackId,
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
    // true if this transform is part of a splictat and not the last
    // element. Used in maintain_single_value to yield early
    pub is_split: bool,
}

impl TransformState {
    pub fn new(
        input_field: FieldId,
        output_field: FieldId,
        ms_id: MatchSetId,
        desired_batch_size: usize,
        op_id: Option<OperatorId>,
        input_group_track_id: GroupTrackId,
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
            is_split: false,
            input_group_track_id,
            output_group_track_id: input_group_track_id,
        }
    }
}

pub trait Transform<'a>: Send + 'a {
    fn display_name(&self) -> DefaultTransformName;
    fn stream_producer_update(
        &mut self,
        _jd: &mut JobData<'a>,
        _tf_id: TransformId,
    ) {
        unimplemented!(
            "transform `{}` does not implement stream production",
            self.display_name()
        )
    }
    fn handle_stream_value_update(
        &mut self,
        _jd: &mut JobData<'a>,
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
    fn pre_update(&mut self, _sess: &mut Job<'a>, _tf_id: TransformId) {}
    fn update(&mut self, jd: &mut JobData<'a>, tf_id: TransformId);
}

pub fn transform_pre_update(
    job: &mut Job,
    tf_id: TransformId,
    ctx: Option<&Arc<ContextData>>,
) -> Result<(), VentureDescription> {
    match &mut job.transform_data[tf_id] {
        TransformData::Fork(fork) => {
            if !fork.expanded {
                handle_fork_expansion(job, tf_id, ctx);
            }
        }
        TransformData::CallConcurrent(callcc) => {
            if !callcc.expanded {
                handle_call_concurrent_expansion(job, tf_id, ctx)?;
            }
        }
        TransformData::Call(_) => {
            // this removes itself on the first invocation,
            // so no need for any check
            handle_lazy_call_expansion(job, tf_id);
        }
        TransformData::Disabled
        | TransformData::ForkCat(_)
        | TransformData::ForeachHeader(_)
        | TransformData::ForeachTrailer(_)
        | TransformData::CalleeConcurrent(_)
        | TransformData::ToStr(_)
        | TransformData::Nop(_)
        | TransformData::SuccessUpdator(_)
        | TransformData::NopCopy(_)
        | TransformData::InputDoneEater(_)
        | TransformData::Count(_)
        | TransformData::Print(_)
        | TransformData::Join(_)
        | TransformData::Select(_)
        | TransformData::StringSink(_)
        | TransformData::FieldValueSink(_)
        | TransformData::Regex(_)
        | TransformData::Format(_)
        | TransformData::FileReader(_)
        | TransformData::Literal(_)
        | TransformData::Sequence(_)
        | TransformData::Terminator(_)
        | TransformData::AggregatorHeader(_)
        | TransformData::AggregatorTrailer(_) => (),
        TransformData::Custom(tf) => {
            if tf.pre_update_required() {
                let mut tf = std::mem::replace(
                    &mut job.transform_data[tf_id],
                    TransformData::Disabled,
                );
                let TransformData::Custom(tf_custom) = &mut tf else {
                    unreachable!()
                };
                tf_custom.pre_update(job, tf_id);
                let _ = std::mem::replace(&mut job.transform_data[tf_id], tf);
            }
        }
    }
    Ok(())
}

pub fn transform_update(job: &mut Job, tf_id: TransformId) {
    let jd = &mut job.job_data;
    match &mut job.transform_data[tf_id] {
        TransformData::Fork(tf) => {
            handle_tf_fork(jd, tf_id, tf);
        }
        TransformData::ForkCat(fork) => {
            handle_tf_forkcat(jd, tf_id, fork);
        }
        TransformData::Nop(tf) => handle_tf_nop(jd, tf_id, tf),
        TransformData::SuccessUpdator(tf) => {
            handle_tf_success_updator(jd, tf_id, tf)
        }
        TransformData::NopCopy(tf) => handle_tf_nop_copy(jd, tf_id, tf),
        TransformData::InputDoneEater(tf) => {
            handle_tf_input_done_eater(jd, tf_id, tf);
        }
        TransformData::Print(tf) => handle_tf_print(jd, tf_id, tf),
        TransformData::Regex(tf) => handle_tf_regex(jd, tf_id, tf),
        TransformData::StringSink(tf) => {
            handle_tf_string_sink(jd, tf_id, tf);
        }
        TransformData::FieldValueSink(tf) => {
            handle_tf_field_value_sink(jd, tf_id, tf);
        }
        TransformData::FileReader(tf) => {
            handle_tf_file_reader(jd, tf_id, tf);
        }
        TransformData::Literal(tf) => handle_tf_literal(jd, tf_id, tf),
        TransformData::Sequence(tf) => handle_tf_sequence(jd, tf_id, tf),
        TransformData::Format(tf) => handle_tf_format(jd, tf_id, tf),
        TransformData::Join(tf) => handle_tf_join(jd, tf_id, tf),
        TransformData::Select(tf) => handle_tf_select(jd, tf_id, tf),
        TransformData::Count(tf) => handle_tf_count(jd, tf_id, tf),
        TransformData::ToStr(tf) => handle_tf_to_str(jd, tf_id, tf),
        TransformData::CallConcurrent(tf) => {
            handle_tf_call_concurrent(jd, tf_id, tf)
        }
        TransformData::CalleeConcurrent(tf) => {
            handle_tf_callee_concurrent(jd, tf_id, tf)
        }
        TransformData::Call(_) => (),
        TransformData::Terminator(tf) => handle_tf_terminator(jd, tf_id, tf),
        TransformData::Custom(tf) => tf.update(jd, tf_id),
        TransformData::AggregatorHeader(agg_header) => {
            handle_tf_aggregator_header(jd, tf_id, agg_header);
        }
        TransformData::AggregatorTrailer(_) => {
            handle_tf_aggregator_trailer(job, tf_id)
        }
        TransformData::ForeachHeader(eh) => {
            handle_tf_foreach_header(jd, tf_id, eh)
        }
        TransformData::ForeachTrailer(et) => {
            handle_tf_foreach_trailer(jd, tf_id, et)
        }
        TransformData::Disabled => unreachable!(),
    }
}

pub fn stream_producer_update(job: &mut Job, tf_id: TransformId) {
    match &mut job.transform_data[tf_id] {
            TransformData::Disabled
            | TransformData::Nop(_)
            | TransformData::SuccessUpdator(_)
            | TransformData::NopCopy(_)
            | TransformData::InputDoneEater(_)
            | TransformData::Terminator(_)
            | TransformData::Call(_)
            | TransformData::CallConcurrent(_)
            | TransformData::CalleeConcurrent(_)
            | TransformData::ToStr(_)
            | TransformData::Count(_)
            | TransformData::Print(_)
            | TransformData::Select(_)
            | TransformData::StringSink(_)
            | TransformData::FieldValueSink(_)
            | TransformData::Fork(_)
            | TransformData::ForkCat(_)
            | TransformData::Regex(_)
            | TransformData::Literal(_)
            | TransformData::Sequence(_)
            | TransformData::Format(_)
            //these go straight to the sub transforms
            | TransformData::AggregatorHeader(_)
            | TransformData::AggregatorTrailer(_)
            | TransformData::ForeachHeader(_)
            | TransformData::ForeachTrailer(_) => unreachable!(),
            TransformData::Join(j) => {
                handle_tf_join_stream_producer_update(
                    &mut job.job_data, j, tf_id
                )
            }
            TransformData::FileReader(f) => {
                handle_tf_file_reader_stream(&mut job.job_data, tf_id, f)
            }
            TransformData::Custom(c) => {
                c.stream_producer_update(&mut job.job_data, tf_id)
            }
        }
}

pub fn transform_stream_value_update(job: &mut Job, svu: StreamValueUpdate) {
    let jd = &mut job.job_data;
    match &mut job.transform_data[svu.tf_id] {
        TransformData::Print(tf) => handle_tf_print_stream_value_update(
            jd,
            tf,
            svu
        ),
        TransformData::Join(tf) => handle_tf_join_stream_value_update(
            jd,
            tf,
            svu
        ),
        TransformData::StringSink(tf) => {
            handle_tf_string_sink_stream_value_update(
                jd,
                tf,
                svu
            );
        }
        TransformData::FieldValueSink(tf) => {
            handle_tf_field_value_sink_stream_value_update(
                jd,
                svu.tf_id,
                tf,
                svu.sv_id,
                svu.custom,
            );
        }
        TransformData::Format(tf) => handle_tf_format_stream_value_update(
            jd,
            tf,
            svu
        ),
        TransformData::Regex(tf) => handle_tf_regex_stream_value_update(
            jd,
            svu.tf_id,
            tf,
            svu.sv_id,
            svu.custom,
        ),
        TransformData::ToStr(tf) => handle_tf_to_str_stream_value_update(jd, tf, svu),
        TransformData::CallConcurrent(_) |
        TransformData::Fork(_) |
        TransformData::ForkCat(_) |
        TransformData::ForeachHeader(_) |
        TransformData::ForeachTrailer(_) |
        TransformData::Terminator(_) |
        TransformData::Call(_) |
        TransformData::Nop(_) |
         TransformData::SuccessUpdator(_) |
        TransformData::NopCopy(_) |
        TransformData::InputDoneEater(_) |
        TransformData::Count(_) |
        TransformData::Select(_) |
        TransformData::FileReader(_) |
        TransformData::Sequence(_) |
        TransformData::Disabled |
        TransformData::Literal(_) |
        TransformData::CalleeConcurrent(_) |
        // these go to the individual transforms
        TransformData::AggregatorHeader(_) |
        TransformData::AggregatorTrailer(_) => unreachable!(),
        TransformData::Custom(tf) => tf.handle_stream_value_update(
            jd,
            svu.tf_id,
            svu.sv_id,
            svu.custom,
        ),
    }
}
