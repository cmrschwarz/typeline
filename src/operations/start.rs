use smallvec::SmallVec;

use crate::transform::{MatchData, StreamChunk, TfBase, Transform};

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
        _tf_stack: &'a [Box<dyn Transform>],
        sc: &'b StreamChunk<'b>,
    ) -> Option<&'b StreamChunk<'b>> {
        None
    }

    fn evaluate(&mut self, tf_stack: &mut [Box<dyn Transform>]) -> bool {
        true
    }

    fn data<'a>(&'a self, tf_stack: &'a [Box<dyn Transform>]) -> Option<&'a MatchData> {
        Some(&self.data)
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
