use crate::operations::transform::{DataKind, MatchData, StreamChunk, TfBase, Transform};

use super::OperationError;

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
        _sc: &'b StreamChunk<'b>,
        _final_chunk: bool,
    ) -> Result<Option<&'b StreamChunk<'b>>, OperationError> {
        Ok(None)
    }

    fn data<'a>(&'a self, _tf_stack: &'a [Box<dyn Transform>]) -> Option<&'a MatchData> {
        None
    }

    fn evaluate(&mut self, _tf_stack: &mut [Box<dyn Transform>]) -> Result<bool, OperationError> {
        Ok(true)
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
                begin_of_chain: true,
                dependants: Default::default(),
                tfs_index: 0,
            },
        }
    }
}
