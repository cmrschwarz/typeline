use crate::operations::transform::{DataKind, MatchData, StreamChunk, TfBase, Transform};

use super::transform::TransformApplicationError;

#[derive(Clone)]
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
    ) -> Result<Option<&'b StreamChunk<'b>>, TransformApplicationError> {
        panic!("requested to process data in a data source");
    }

    fn data<'a>(
        &'a self,
        _tf_stack: &'a [Box<dyn Transform>],
    ) -> Result<Option<&'a MatchData>, TransformApplicationError> {
        panic!("attempted to get MatchData from a stream");
    }

    fn evaluate(
        &mut self,
        _tf_stack: &mut [Box<dyn Transform>],
    ) -> Result<bool, TransformApplicationError> {
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
