use smallvec::SmallVec;

use crate::{
    options::{chain_spec::ChainSpec, context_options::ContextOptions},
    transform::{DataKind, MatchData, StreamChunk, TfBase, Transform},
};

use super::{Operation, OperationCatalogMember};

pub struct TfReadStdin {
    pub tf_base: TfBase,
}

impl Transform for TfReadStdin {
    fn base(&self) -> &TfBase {
        &self.tf_base
    }

    fn base_mut(&mut self) -> &mut TfBase {
        &mut self.tf_base
    }

    fn process_chunk<'a: 'b, 'b>(
        &'a mut self,
        _tf_stack: &'a [Box<dyn Transform>],
        sc: &'b StreamChunk<'b>,
    ) -> Option<&'b StreamChunk<'b>> {
        None
    }

    fn data<'a>(&'a self, tf_stack: &'a [Box<dyn Transform>]) -> Option<&'a MatchData> {
        None
    }

    fn evaluate(&mut self, tf_stack: &mut [Box<dyn Transform>]) -> bool {
        true
    }
}

impl TfReadStdin {
    pub fn new() -> Self {
        Self {
            tf_base: TfBase {
                data_kind: DataKind::Bytes,
                needs_stdout: false,
                is_stream: true,
                requires_eval: false,
                dependants: Default::default(),
                tfs_index: 0,
            },
        }
    }
}
