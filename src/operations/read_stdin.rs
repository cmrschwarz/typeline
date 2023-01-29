use std::collections::{HashMap, VecDeque};

use smallvec::SmallVec;

use crate::{
    context::ContextData,
    match_data::{MatchData, MatchDataKind},
    operations::transform::{TfBase, Transform},
};

use super::transform::{TransformApplicationError, TransformOutput, TransformStackIndex};

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

    fn process(
        &mut self,
        _ctx: &ContextData,
        _args: &HashMap<String, SmallVec<[(TransformStackIndex, MatchData); 1]>>,
        _tfo: &TransformOutput,
        _output: &mut VecDeque<TransformOutput>,
    ) -> Result<(), TransformApplicationError> {
        panic!("requested to process data in a data source");
    }
}

impl TfReadStdin {
    pub fn new() -> Self {
        Self {
            tf_base: TfBase {
                data_kind: MatchDataKind::Bytes,
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
