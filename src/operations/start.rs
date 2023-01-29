use std::collections::{HashMap, VecDeque};

use smallvec::SmallVec;

use crate::{
    context::ContextData,
    match_data::{MatchData, MatchDataKind},
    operations::transform::{TfBase, Transform},
};

use super::transform::{TransformApplicationError, TransformOutput, TransformStackIndex};

pub struct TfStart {
    pub tf_base: TfBase,
}

impl Transform for TfStart {
    fn base(&self) -> &TfBase {
        &self.tf_base
    }

    fn base_mut(&mut self) -> &mut TfBase {
        &mut self.tf_base
    }

    fn process(
        &mut self,
        _ctx: &ContextData,
        _args: &HashMap<String, SmallVec<[(TransformStackIndex, MatchData); 1]>>,
        _tfo: &TransformOutput,
        _output: &mut VecDeque<TransformOutput>,
    ) -> Result<(), TransformApplicationError> {
        panic!("attempted to process TfStart")
    }
}

impl TfStart {
    pub fn new(data_kind: MatchDataKind) -> TfStart {
        Self {
            tf_base: TfBase {
                data_kind: data_kind,
                is_stream: false,
                requires_eval: false,
                needs_stdout: false,
                begin_of_chain: true,
                dependants: SmallVec::new(),
                tfs_index: 0,
            },
        }
    }
}
