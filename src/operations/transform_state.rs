use nonmax::NonMaxUsize;
use regex::Regex;

use crate::worker_thread_session::{FieldId, MatchSetId, TransformId};

use super::{format::TfFormat, split::TfSplit};

type Type = Option<TfSplit>;

pub enum TransformData<'a> {
    Disabled,
    Print,
    Split(Type),
    Regex(Regex),
    Format(TfFormat<'a>),
}

impl Default for TransformData<'_> {
    fn default() -> Self {
        Self::Disabled
    }
}

pub struct TransformState<'a> {
    pub successor: Option<TransformId>,
    pub stream_successor: Option<TransformId>,
    pub stream_producers_slot_index: Option<NonMaxUsize>,
    pub input_field: FieldId,
    pub available_batch_size: usize,
    pub desired_batch_size: usize,
    pub match_set_id: MatchSetId,
    pub data: TransformData<'a>,
}
