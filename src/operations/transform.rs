use std::collections::{HashMap, VecDeque};

use smallvec::SmallVec;
use thiserror::Error;

use crate::{
    context::SessionData,
    match_data::{MatchData, MatchDataKind},
};

use super::operation::OperationRef;

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

pub struct TransformOutput {
    pub match_index: MatchIdx,
    pub data: Option<MatchData>,
    pub args: Vec<(String, MatchData)>,
    pub is_last_chunk: Option<bool>, //None if not chunked
}

pub type TransformStackIndex = u32;

#[derive(Clone)]
pub struct TfBase {
    pub data_kind: MatchDataKind,
    pub is_stream: bool,
    pub needs_stdout: bool,
    pub requires_eval: bool,
    pub begin_of_chain: bool,
    // !adding a dependant should be done by calling add_dependant on the transform
    // to allow e.g. TfParent to redirect it further upwards
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
        ctx: &SessionData,
        args: &HashMap<String, SmallVec<[(TransformStackIndex, MatchData); 1]>>,
        tfo: &TransformOutput,
        output: &mut VecDeque<TransformOutput>,
    ) -> Result<(), TransformApplicationError>;
    fn add_dependant<'a>(
        &mut self,
        _tf_stack: &mut [Box<dyn Transform + 'a>],
        dependant: TransformStackIndex,
    ) -> Result<(), TransformApplicationError> {
        self.base_mut().dependants.push(dependant);
        Ok(())
    }
}
