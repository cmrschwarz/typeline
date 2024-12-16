use std::{any::Any, fmt::Write, sync::Arc};

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

use metamatch::metamatch;

use super::{
    aggregator::{
        handle_tf_aggregator_header, handle_tf_aggregator_trailer,
        TfAggregatorHeader, TfAggregatorTrailer,
    },
    fork::{handle_fork_expansion, handle_tf_fork, TfFork},
    literal::{handle_tf_literal, TfLiteral},
    operator::{OperatorId, OutputFieldKind},
};

pub type DefaultTransformName = SmallString<[u8; 32]>;

index_newtype! {
    pub struct TransformId(DebuggableNonMaxU32);
    pub struct StreamProducerIndex(DebuggableNonMaxUsize);
}

pub enum TransformData<'a> {
    Disabled,
    Fork(TfFork<'a>),
    Literal(TfLiteral<'a>),
    AggregatorHeader(TfAggregatorHeader),
    AggregatorTrailer(TfAggregatorTrailer),
    Custom(SmallBox<dyn Transform<'a> + 'a, 32>),
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
    pub fn display_name(
        &self,
        jd: &JobData,
        tf_id: TransformId,
    ) -> DefaultTransformName {
        match self {
            TransformData::Disabled => "disabled",
            TransformData::Fork(_) => "fork",
            TransformData::Literal(_) => "literal",
            TransformData::AggregatorHeader(_) => "aggregator_header",
            TransformData::AggregatorTrailer(_) => "aggregator_trailer",
            TransformData::Custom(tf) => return tf.display_name(jd, tf_id),
        }
        .into()
    }
    pub fn get_out_fields(
        &self,
        jd: &JobData,
        tf_state: &TransformState,
        fields: &mut Vec<FieldId>,
    ) {
        match self {
            TransformData::Literal(_)
            | TransformData::AggregatorTrailer(_) => {
                fields.push(tf_state.output_field)
            }

            // TODO: fix this
            TransformData::Disabled
            | TransformData::Fork(_)
            | TransformData::AggregatorHeader(_) => (),

            TransformData::Custom(custom) => {
                custom.collect_out_fields(jd, tf_state, fields)
            }
        }
    }

    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        metamatch!(match self {
            TransformData::Disabled
            | TransformData::Literal(_)
            | TransformData::Fork(_) => None,

            #[expand(T in [
                AggregatorHeader,
                AggregatorTrailer,
            ])]
            TransformData::T(o) => {
                let a: &dyn Any = o;
                a.downcast_ref()
            }

            TransformData::Custom(o) => o.downcast_ref(),
        })
    }

    pub fn downcast_mut<T: Any>(&mut self) -> Option<&mut T> {
        metamatch!(match self {
            TransformData::Disabled
            | TransformData::Literal(_)
            | TransformData::Fork(_) => None,

            #[expand(T in [
                AggregatorHeader,
                AggregatorTrailer,
            ])]
            TransformData::T(o) => {
                let a: &mut dyn Any = o;
                a.downcast_mut()
            }

            TransformData::Custom(o) => o.downcast_mut(),
        })
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

    // This might be None for special transforms like the subchain terminator
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
    fn display_name(
        &self,
        jd: &JobData,
        tf_id: TransformId,
    ) -> DefaultTransformName {
        if let Some(op_id) = jd.tf_mgr.transforms[tf_id].op_id {
            return jd.session_data.operator_data
                [jd.session_data.op_data_id(op_id)]
            .debug_op_name();
        }
        let mut res = DefaultTransformName::default();
        res.write_fmt(format_args!("<tf id {tf_id} (no operator id)>"))
            .unwrap();
        res
    }
    fn stream_producer_update(
        &mut self,
        jd: &mut JobData<'a>,
        tf_id: TransformId,
    ) {
        unimplemented!(
            "transform `{}` does not implement stream production",
            self.display_name(jd, tf_id)
        )
    }
    fn handle_stream_value_update(
        &mut self,
        jd: &mut JobData<'a>,
        svu: StreamValueUpdate,
    ) {
        unimplemented!(
            "transform `{}` does not implement stream value updates",
            self.display_name(jd, svu.tf_id)
        )
    }
    fn pre_update_required(&self) -> bool {
        false
    }
    fn pre_update(
        &mut self,
        _ctx: Option<&Arc<ContextData>>,
        _job: &mut Job<'a>,
        _tf_id: TransformId,
    ) -> Result<(), VentureDescription> {
        Ok(())
    }
    fn update(&mut self, jd: &mut JobData<'a>, tf_id: TransformId);
    fn collect_out_fields(
        &self,
        jd: &JobData,
        tf_state: &TransformState,
        fields: &mut Vec<FieldId>,
    ) {
        if let Some(op_id) = tf_state.op_id {
            let out_kind = jd.session_data.operator_data
                [jd.session_data.op_data_id(op_id)]
            .output_field_kind(jd.session_data, op_id);
            if matches!(
                out_kind,
                OutputFieldKind::SameAsInput | OutputFieldKind::Unconfigured
            ) {
                return;
            }
        }
        fields.push(tf_state.output_field);
    }

    fn as_any(&self) -> Option<&dyn Any> {
        None
    }
    fn as_any_mut(&mut self) -> Option<&mut dyn Any> {
        None
    }
}

impl<'a> dyn Transform<'a> {
    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        self.as_any().and_then(|t| t.downcast_ref::<T>())
    }
    pub fn downcast_mut<T: Any>(&mut self) -> Option<&mut T> {
        self.as_any_mut().and_then(|t| t.downcast_mut::<T>())
    }
}

pub fn transform_pre_update(
    ctx: Option<&Arc<ContextData>>,
    job: &mut Job,
    tf_id: TransformId,
) -> Result<(), VentureDescription> {
    match &mut job.transform_data[tf_id] {
        TransformData::Fork(fork) => {
            if !fork.expanded {
                handle_fork_expansion(job, tf_id, ctx);
            }
        }
        TransformData::Disabled
        | TransformData::Literal(_)
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
                let res = tf_custom.pre_update(ctx, job, tf_id);
                let _ = std::mem::replace(&mut job.transform_data[tf_id], tf);
                return res;
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
        TransformData::Literal(tf) => handle_tf_literal(jd, tf_id, tf),
        TransformData::Custom(tf) => tf.update(jd, tf_id),
        TransformData::AggregatorHeader(agg_header) => {
            handle_tf_aggregator_header(jd, tf_id, agg_header);
        }
        TransformData::AggregatorTrailer(_) => {
            handle_tf_aggregator_trailer(job, tf_id)
        }
        TransformData::Disabled => unreachable!(),
    }
}

pub fn stream_producer_update(job: &mut Job, tf_id: TransformId) {
    match &mut job.transform_data[tf_id] {
            TransformData::Disabled
            | TransformData::Fork(_)
            | TransformData::Literal(_)
            //these go straight to the sub transforms
            | TransformData::AggregatorHeader(_)
            | TransformData::AggregatorTrailer(_)=> unreachable!(),
            TransformData::Custom(c) => {
                c.stream_producer_update(&mut job.job_data, tf_id)
            }
        }
}

pub fn transform_stream_value_update(job: &mut Job, svu: StreamValueUpdate) {
    let jd = &mut job.job_data;
    match &mut job.transform_data[svu.tf_id] {
        TransformData::Fork(_) |
        TransformData::Disabled |
        TransformData::Literal(_) |
        // these go to the individual transforms
        TransformData::AggregatorHeader(_) |
        TransformData::AggregatorTrailer(_) => unreachable!(),
        TransformData::Custom(tf) => tf.handle_stream_value_update(
            jd,
            svu
        ),
    }
}
