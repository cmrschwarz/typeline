use smallvec::SmallVec;

use crate::{
    options::{chain_spec::ChainSpec, context_options::ContextOptions},
    transform::{DataKind, MatchData, StreamChunk, TfBase, Transform},
};

use super::{Operation, OperationCatalogMember};

pub struct TfReadStdin(pub TfBase);

impl Transform for TfReadStdin {
    fn base(&self) -> &TfBase {
        &self.0
    }

    fn base_mut(&mut self) -> &mut TfBase {
        &mut self.0
    }

    fn process_chunk<'a: 'b, 'b>(
        &'a mut self,
        _tf_stack: &'b [&'a dyn Transform],
        sc: &'a StreamChunk,
    ) -> Option<&'a StreamChunk> {
        None
    }

    fn data<'a, 'b>(&'a self, tf_stack: &'b [&'a dyn Transform]) -> Option<&'a MatchData> {
        None
    }

    fn evaluate<'a, 'b>(&'a mut self, tf_stack: &'b [&'a dyn Transform]) {}
}

impl TfReadStdin {
    pub fn new() -> Self {
        Self(TfBase {
            data_kind: DataKind::Bytes,
            needs_stdout: false,
            is_stream: true,
            requires_eval: false,
            dependants: Default::default(),
            stack_index: 0,
        })
    }
}
