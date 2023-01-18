use std::{fmt::Display, ops::Range};

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

impl Display for HtmlMatchData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "todo: serialize html!")
    }
}

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
    pub begin_of_chain: bool,
    pub dependants: SmallVec<[TransformStackIndex; 1]>,
    pub tfs_index: TransformStackIndex,
}

impl TfBase {
    pub fn from_parent(parent: &TfBase) -> TfBase {
        TfBase {
            data_kind: parent.data_kind,
            is_stream: parent.is_stream,
            needs_stdout: parent.needs_stdout,
            tfs_index: parent.tfs_index + 1,
            requires_eval: parent.requires_eval,
            begin_of_chain: false,
            dependants: SmallVec::new(),
        }
    }
}

pub trait Transform: Send + Sync {
    fn base(&self) -> &TfBase;
    fn base_mut(&mut self) -> &mut TfBase;
    fn process_chunk<'a: 'b, 'b>(
        &'a mut self,
        _tf_stack: &'a [Box<dyn Transform>],
        sc: &'b StreamChunk<'b>,
    ) -> Option<&'b StreamChunk<'b>>;
    fn evaluate(&mut self, tf_stack: &mut [Box<dyn Transform>]) -> bool;
    fn data<'a>(&'a self, tf_stack: &'a [Box<dyn Transform>]) -> Option<&'a MatchData>;
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
