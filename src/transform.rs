use std::ops::Range;

use smallvec::SmallVec;

use crate::operations::OperationRef;

#[derive(Clone, Copy)]
pub enum DataKind {
    Html,
    Text,
    Bytes,
    Png,
    None,
}

pub struct HtmlMatchData {}

pub enum MatchData {
    Html(HtmlMatchData),
    Text(String),
    Bytes(Vec<u8>),
    Png(Vec<u8>),
}

impl MatchData {
    pub fn kind(&self) -> DataKind {
        match &self {
            MatchData::Html(_) => DataKind::Html,
            MatchData::Text(_) => DataKind::Text,
            MatchData::Bytes(_) => DataKind::Bytes,
            MatchData::Png(_) => DataKind::Png,
        }
    }
}

pub enum StreamChunk<'a> {
    Bytes(&'a [u8]),
    Text(&'a str),
}

pub type TransformStackIndex = u32;

pub struct TfBase {
    pub data_kind: DataKind,
    pub is_stream: bool,
    pub needs_stdout: bool,
    pub requires_eval: bool,
    pub dependants: SmallVec<[Box<dyn Transform>; 1]>,
    pub stack_index: TransformStackIndex,
}

impl TfBase {
    pub fn from_parent(parent: &TfBase) -> TfBase {
        TfBase {
            data_kind: parent.data_kind,
            is_stream: parent.is_stream,
            needs_stdout: parent.needs_stdout,
            stack_index: parent.stack_index + 1,
            requires_eval: parent.requires_eval,
            dependants: SmallVec::new(),
        }
    }
}

pub trait Transform: Send + Sync {
    fn base(&self) -> &TfBase;
    fn base_mut(&mut self) -> &mut TfBase;
    fn process_chunk<'a: 'b, 'b>(
        &'a mut self,
        tf_stack: &'b [&'a dyn Transform],
        sc: &'a StreamChunk,
    ) -> Option<&'a StreamChunk>;
    fn evaluate<'a, 'b>(&'a mut self, tf_stack: &'b [&'a dyn Transform]);
    fn data<'a, 'b>(&'a self, tf_stack: &'b [&'a dyn Transform]) -> Option<&'a MatchData>;
}

impl std::ops::Deref for dyn Transform {
    type Target = TfBase;

    fn deref(&self) -> &TfBase {
        self.base()
    }
}

impl std::ops::DerefMut for dyn Transform {
    fn deref_mut(&mut self) -> &mut TfBase {
        self.base_mut()
    }
}
