use std::{collections::HashMap, fmt::Display};

use smallvec::SmallVec;
use thiserror::Error;

use crate::context::ContextData;

use super::OperationRef;

pub type MatchIdx = u32;

#[derive(Error, Debug, Clone)]
#[error("in op {0} of chain {1}: {message}", op_ref.op_offset, op_ref.chain_id)]
pub struct TransformApplicationError {
    pub message: String,
    pub op_ref: OperationRef,
}

impl TransformApplicationError {
    pub fn new(message: &str, op_ref: OperationRef) -> TransformApplicationError {
        TransformApplicationError {
            message: message.to_owned(),
            op_ref,
        }
    }
}

#[derive(Clone, Copy)]
pub enum DataKind {
    Html,
    Text,
    Bytes,
    Png,
    None,
}

impl DataKind {
    pub fn to_str(&self) -> &'static str {
        match self {
            DataKind::Html => "html",
            DataKind::Text => "text",
            DataKind::Bytes => "bytes",
            DataKind::Png => "png",
            DataKind::None => "none",
        }
    }
}

impl Display for DataKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.to_str())
    }
}

#[derive(Clone)]
pub struct HtmlMatchData {}

impl Display for HtmlMatchData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "todo: serialize html!")
    }
}

#[derive(Clone)]
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

pub struct TransformOutput {
    pub match_index: MatchIdx,
    pub data: Option<MatchData>,
    pub args: Vec<(String, MatchData)>,
    pub is_last_chunk: Option<bool>, //None if not chunked
}

pub type TransformStackIndex = u32;

#[derive(Clone)]
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

pub trait Transform: Send {
    fn base(&self) -> &TfBase;
    fn base_mut(&mut self) -> &mut TfBase;
    fn process(
        &mut self,
        ctx: &ContextData,
        args: &HashMap<String, MatchData>,
        tfo: &TransformOutput,
    ) -> Result<SmallVec<[TransformOutput; 1]>, TransformApplicationError>;
    fn add_dependant(
        &mut self,
        _tf_stack: &mut [Box<dyn Transform>],
        dependant: TransformStackIndex,
    ) -> Result<(), TransformApplicationError> {
        self.base_mut().dependants.push(dependant);
        Ok(())
    }
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
