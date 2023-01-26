use std::collections::HashMap;

use smallvec::{smallvec, SmallVec};

use crate::{
    context::ContextData,
    operations::transform::{MatchData, MatchIdx, TfBase, Transform},
};

use super::transform::{DataKind, TransformApplicationError, TransformOutput};

pub struct TfStart {
    pub tf_base: TfBase,
    pub data: Option<MatchData>,
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
        _args: &HashMap<String, MatchData>,
        tfo: &TransformOutput,
    ) -> Result<SmallVec<[TransformOutput; 1]>, TransformApplicationError> {
        debug_assert!(tfo.data.is_none());
        Ok(smallvec![TransformOutput {
            match_index: 0 as MatchIdx,
            data: self.data.take(),
            args: Vec::default(),
            is_last_chunk: None,
        }])
    }
}

impl TfStart {
    pub fn new(data: Option<MatchData>) -> TfStart {
        Self {
            tf_base: TfBase {
                data_kind: data.as_ref().map_or(DataKind::None, |d| d.kind()),
                is_stream: false,
                requires_eval: false,
                needs_stdout: false,
                begin_of_chain: true,
                dependants: SmallVec::new(),
                tfs_index: 0,
            },
            data,
        }
    }
}
