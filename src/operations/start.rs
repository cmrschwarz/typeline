use smallvec::SmallVec;

use crate::transform::{MatchData, StreamChunk, TfBase, Transform};

pub struct TfStart {
    pub tfb: TfBase,
    pub data: MatchData,
}

impl Transform for TfStart {
    fn base(&self) -> &TfBase {
        &self.tfb
    }

    fn base_mut(&mut self) -> &mut TfBase {
        &mut self.tfb
    }

    fn process_chunk<'a: 'b, 'b>(
        &'a mut self,
        tf_stack: &'b [&'a dyn Transform],
        sc: &'a StreamChunk,
    ) -> Option<&'a StreamChunk> {
        None
    }

    fn evaluate<'a, 'b>(&'a mut self, tf_stack: &'b [&'a dyn Transform]) {
        for d in &mut self.tfb.dependants {
            d.evaluate(tf_stack);
        }
    }

    fn data<'a, 'b>(&'a self, tf_stack: &'b [&'a dyn Transform]) -> Option<&'a MatchData> {
        Some(&self.data)
    }
}

impl TfStart {
    pub fn new(data: MatchData) -> TfStart {
        Self {
            tfb: TfBase {
                data_kind: data.kind(),
                is_stream: false,
                requires_eval: false,
                dependants: SmallVec::new(),
                stack_index: 0,
            },
            data: data,
        }
    }
}
