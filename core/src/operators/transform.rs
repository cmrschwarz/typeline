use std::sync::Arc;

use smallstr::SmallString;

use crate::{
    context::{ContextData, VentureDescription},
    index_newtype,
    job::{Job, JobData},
    record_data::{
        field::FieldId,
        group_track::{GroupIdxStable, GroupTrackId},
        match_set::MatchSetId,
        stream_value::StreamValueUpdate,
    },
    smallbox,
    utils::{
        debuggable_nonmax::{DebuggableNonMaxU32, DebuggableNonMaxUsize},
        small_box::SmallBox,
    },
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
    chunks::{
        handle_tf_chunks_header, handle_tf_chunks_trailer, TfChunksHeader,
        TfChunksTrailer,
    },
    field_value_sink::{
        handle_tf_field_value_sink,
        handle_tf_field_value_sink_stream_value_update, TfFieldValueSink,
    },
    foreach::{
        handle_tf_foreach_header, handle_tf_foreach_trailer, TfForeachHeader,
        TfForeachTrailer,
    },
    foreach_unique::{handle_tf_foreach_unique_header, TfForeachUniqueHeader},
    fork::{handle_fork_expansion, handle_tf_fork, TfFork},
    forkcat::{
        handle_tf_forcat_subchain_trailer, handle_tf_forkcat, TfForkCat,
        TfForkCatSubchainTrailer,
    },
    literal::{handle_tf_literal, TfLiteral},
    nop::{handle_tf_nop, TfNop},
    nop_copy::{handle_tf_nop_copy, TfNopCopy},
    operator::OperatorId,
    string_sink::{
        handle_tf_string_sink, handle_tf_string_sink_stream_value_update,
        TfStringSink,
    },
    terminator::{handle_tf_terminator, TfTerminator},
};

pub type DefaultTransformName = SmallString<[u8; 32]>;

index_newtype! {
    pub struct TransformId(DebuggableNonMaxU32);
    pub struct StreamProducerIndex(DebuggableNonMaxUsize);
}

pub enum TransformData<'a> {
    Disabled,
    Nop(TfNop),
    NopCopy(TfNopCopy),
    Terminator(TfTerminator),
    Call(TfCall),
    CallConcurrent(TfCallConcurrent<'a>),
    CalleeConcurrent(TfCalleeConcurrent),
    StringSink(TfStringSink<'a>),
    FieldValueSink(TfFieldValueSink<'a>),
    Fork(TfFork<'a>),
    ForkCat(TfForkCat),
    ForkCatSubchainTrailer(TfForkCatSubchainTrailer<'a>),
    Literal(TfLiteral<'a>),
    AggregatorHeader(TfAggregatorHeader),
    AggregatorTrailer(TfAggregatorTrailer),
    ForeachHeader(TfForeachHeader),
    ForeachUniqueHeader(TfForeachUniqueHeader),
    ForeachTrailer(TfForeachTrailer),
    ChunksHeader(TfChunksHeader),
    ChunksTrailer(TfChunksTrailer),
    Custom(SmallBox<dyn Transform<'a> + 'a, 192>),
}

impl Default for TransformData<'_> {
    fn default() -> Self {
        Self::Disabled
    }
}

impl<'a> TransformData<'a> {
    pub fn from_custom(tf: impl Transform<'a> + 'a) -> Self {
        Self::Custom(smallbox!(tf))
    }
    pub fn display_name(&self) -> DefaultTransformName {
        match self {
            TransformData::Disabled => "disabled",
            TransformData::Nop(_) => "nop",
            TransformData::NopCopy(_) => "nop-c",
            TransformData::Call(_) => "call",
            TransformData::CallConcurrent(_) => "callcc",
            TransformData::CalleeConcurrent(_) => "callcc_callee",
            TransformData::StringSink(_) => "string_sink",
            TransformData::FieldValueSink(_) => "field_value_sink",
            TransformData::Fork(_) => "fork",
            TransformData::ForkCat(_) => "forkcat",
            TransformData::Literal(_) => "literal",
            TransformData::Terminator(_) => "terminator",
            TransformData::AggregatorHeader(_) => "aggregator_header",
            TransformData::AggregatorTrailer(_) => "aggregator_trailer",
            TransformData::ForeachHeader(_) => "foreach_header",
            TransformData::ForeachUniqueHeader(_) => "foreach_unique_header",
            TransformData::ForeachTrailer(_) => "foreach_trailer",
            TransformData::ChunksHeader(_) => "chunks_header",
            TransformData::ChunksTrailer(_) => "chunks_trailer",
            TransformData::ForkCatSubchainTrailer(_) => {
                "forkcat_subchain_trailer"
            }
            TransformData::Custom(tf) => return tf.display_name(),
        }
        .into()
    }
    pub fn get_out_fields(
        &self,
        tf_state: &TransformState,
        fields: &mut Vec<FieldId>,
    ) {
        match self {
            TransformData::NopCopy(_)
            | TransformData::StringSink(_)
            | TransformData::Literal(_)
            | TransformData::AggregatorTrailer(_)
            | TransformData::FieldValueSink(_) => {
                fields.push(tf_state.output_field)
            }

            // TODO: fix this
            TransformData::ForkCat(_)
            | TransformData::ForkCatSubchainTrailer(_)
            | TransformData::Disabled
            | TransformData::Nop(_)
            | TransformData::Terminator(_)
            | TransformData::Fork(_)
            | TransformData::AggregatorHeader(_)
            | TransformData::ForeachHeader(_)
            | TransformData::ForeachUniqueHeader(_)
            | TransformData::ForeachTrailer(_)
            | TransformData::ChunksHeader(_)
            | TransformData::ChunksTrailer(_)
            | TransformData::Call(_)
            | TransformData::CallConcurrent(_)
            | TransformData::CalleeConcurrent(_) => (),

            TransformData::Custom(custom) => {
                custom.collect_out_fields(tf_state, fields)
            }
        }
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
    // means that the a transform that has us as it's successor indicated to
    // us that it will not produce any more records
    pub predecessor_done: bool,
    // means that this transform will not produce any more records
    pub done: bool,
    pub mark_for_removal: bool,
    // true if this transform is part of a splictat and not the last
    // element. Used in maintain_single_value to yield early
    pub is_split: bool,

    // the last group that this transform is supposed to truncate
    pub group_to_truncate: Option<GroupIdxStable>,

    // used to warn if an actor is added using
    // `JobData::add_actor_for_tf_state` *after* an iterator has been
    // added using `JobData::claim_iter_for_tf_state`, which would lead to
    // the iterator containing an incorrect actor index
    #[cfg(debug_assertions)]
    pub iters_added: std::cell::Cell<bool>,
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
            predecessor_done: false,
            done: false,
            mark_for_removal: false,
            group_to_truncate: None,
            is_split: false,
            input_group_track_id,
            output_group_track_id: input_group_track_id,
            #[cfg(debug_assertions)]
            iters_added: std::cell::Cell::new(false),
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
        _svu: StreamValueUpdate,
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
    fn collect_out_fields(
        &self,
        tf_state: &TransformState,
        fields: &mut Vec<FieldId>,
    ) {
        fields.push(tf_state.output_field);
    }
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
        | TransformData::ForeachUniqueHeader(_)
        | TransformData::ForeachTrailer(_)
        | TransformData::ChunksHeader(_)
        | TransformData::ChunksTrailer(_)
        | TransformData::CalleeConcurrent(_)
        | TransformData::Nop(_)
        | TransformData::NopCopy(_)
        | TransformData::ForkCatSubchainTrailer(_)
        | TransformData::StringSink(_)
        | TransformData::FieldValueSink(_)
        | TransformData::Literal(_)
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
        TransformData::NopCopy(tf) => handle_tf_nop_copy(jd, tf_id, tf),
        TransformData::ForkCatSubchainTrailer(tf) => {
            handle_tf_forcat_subchain_trailer(jd, tf_id, tf);
        }
        TransformData::StringSink(tf) => {
            handle_tf_string_sink(jd, tf_id, tf);
        }
        TransformData::FieldValueSink(tf) => {
            handle_tf_field_value_sink(jd, tf_id, tf);
        }
        TransformData::Literal(tf) => handle_tf_literal(jd, tf_id, tf),
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
        TransformData::ForeachUniqueHeader(eh) => {
            handle_tf_foreach_unique_header(jd, tf_id, eh)
        }
        TransformData::ForeachTrailer(et) => {
            handle_tf_foreach_trailer(jd, tf_id, et)
        }
        TransformData::ChunksHeader(eh) => {
            handle_tf_chunks_header(jd, tf_id, eh)
        }
        TransformData::ChunksTrailer(et) => {
            handle_tf_chunks_trailer(jd, tf_id, et)
        }
        TransformData::Disabled => unreachable!(),
    }
}

pub fn stream_producer_update(job: &mut Job, tf_id: TransformId) {
    match &mut job.transform_data[tf_id] {
            TransformData::Disabled
            | TransformData::Nop(_)
            | TransformData::NopCopy(_)
            | TransformData::ForkCatSubchainTrailer(_)
            | TransformData::Terminator(_)
            | TransformData::Call(_)
            | TransformData::CallConcurrent(_)
            | TransformData::CalleeConcurrent(_)
            | TransformData::StringSink(_)
            | TransformData::FieldValueSink(_)
            | TransformData::Fork(_)
            | TransformData::ForkCat(_)
            | TransformData::Literal(_)
            //these go straight to the sub transforms
            | TransformData::AggregatorHeader(_)
            | TransformData::AggregatorTrailer(_)
            | TransformData::ForeachHeader(_)
            | TransformData::ForeachUniqueHeader(_)
            | TransformData::ForeachTrailer(_)
            | TransformData::ChunksHeader(_)
            | TransformData::ChunksTrailer(_) => unreachable!(),
            TransformData::Custom(c) => {
                c.stream_producer_update(&mut job.job_data, tf_id)
            }
        }
}

pub fn transform_stream_value_update(job: &mut Job, svu: StreamValueUpdate) {
    let jd = &mut job.job_data;
    match &mut job.transform_data[svu.tf_id] {
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
        TransformData::CallConcurrent(_) |
        TransformData::Fork(_) |
        TransformData::ForkCat(_) |
        TransformData::ForeachHeader(_) |
        TransformData::ForeachUniqueHeader(_) |
        TransformData::ForeachTrailer(_) |
        TransformData::ChunksHeader(_) |
        TransformData::ChunksTrailer(_) |
        TransformData::Terminator(_) |
        TransformData::Call(_) |
        TransformData::Nop(_) |
        TransformData::NopCopy(_) |
        TransformData::ForkCatSubchainTrailer(_) |
        TransformData::Disabled |
        TransformData::Literal(_) |
        TransformData::CalleeConcurrent(_) |
        // these go to the individual transforms
        TransformData::AggregatorHeader(_) |
        TransformData::AggregatorTrailer(_) => unreachable!(),
        TransformData::Custom(tf) => tf.handle_stream_value_update(
            jd,
            svu
        ),
    }
}
