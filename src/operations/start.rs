use smallvec::SmallVec;

use crate::{
    context::ContextData,
    operations::transform::{MatchData, StreamChunk, TfBase, Transform},
};

use super::transform::TransformApplicationError;

pub struct TfStart {
    pub tf_base: TfBase,
    pub data: MatchData,
}

impl Transform for TfStart {
    fn base(&self) -> &TfBase {
        &self.tf_base
    }

    fn base_mut(&mut self) -> &mut TfBase {
        &mut self.tf_base
    }

    fn process_chunk<'a: 'b, 'b>(
        &'a mut self,
        _ctx: &'a ContextData,
        _tf_stack: &'a [Box<dyn Transform>],
        _sc: &'b StreamChunk<'b>,
        _final_chunk: bool,
    ) -> Result<Option<&'b StreamChunk<'b>>, TransformApplicationError> {
        Ok(None)
    }

    fn evaluate(
        &mut self,
        _ctx: &ContextData,
        _tf_stack: &mut [Box<dyn Transform>],
    ) -> Result<bool, TransformApplicationError> {
        Ok(true)
    }

    fn data<'a>(
        &'a self,
        _ctx: &'a ContextData,
        _tf_stack: &'a [Box<dyn Transform>],
    ) -> Result<Option<&'a MatchData>, TransformApplicationError> {
        Ok(Some(&self.data))
    }
}

impl TfStart {
    pub fn new(data: MatchData) -> TfStart {
        Self {
            tf_base: TfBase {
                data_kind: data.kind(),
                is_stream: false,
                requires_eval: false,
                needs_stdout: false,
                begin_of_chain: true,
                dependants: SmallVec::new(),
                tfs_index: 0,
            },
            data: data,
        }
    }
}
