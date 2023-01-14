use smallvec::SmallVec;

use crate::{
    options::{ChainSpec, ContextOptions},
    transform::{MatchData, StreamChunk, TfBase, Transform},
};

use super::{Operation, OperationCatalogMember};

struct TfParent {
    tfb: TfBase,
    parent_idx: usize,
}

impl Transform for TfParent {
    fn base(&self) -> &TfBase {
        &self.tfb
    }

    fn base_mut(&mut self) -> &mut TfBase {
        &mut self.tfb
    }

    fn process_chunk<'a: 'b, 'b>(
        &'a mut self,
        _tf_stack: &'b [&'a dyn Transform],
        sc: &'a StreamChunk,
    ) -> Option<&'a StreamChunk> {
        Some(sc)
    }

    fn data<'a, 'b>(&'a self, tf_stack: &'b [&'a dyn Transform]) -> Option<&'a MatchData> {
        let pidx = tf_stack.len() - 1 - self.parent_idx;
        tf_stack[pidx].data(&tf_stack[0..pidx])
    }

    fn evaluate<'a, 'b>(&'a mut self, _tf_stack: &'b [&'a dyn Transform]) {}
}

#[derive(Clone)]
pub struct OpParent {}

impl Operation for OpParent {
    fn apply<'a>(&'a mut self, btf: &'a mut dyn Transform) -> &'a mut dyn Transform {
        let btfb = btf.base_mut();
        let tfp = Box::new(TfParent {
            tfb: TfBase {
                data_kind: btfb.data_kind,
                is_stream: btfb.is_stream,
                stack_index: btfb.stack_index + 1,
                requires_eval: btfb.requires_eval,
                dependants: SmallVec::new(),
            },
            parent_idx: 1,
        });
        btfb.dependants.push(tfp);
        btfb.dependants.last_mut().unwrap().as_mut()
    }
}

impl OperationCatalogMember for OpParent {
    fn name_matches(name: &str) -> bool {
        todo!()
    }

    fn create(
        ctx: &ContextOptions,
        label: String,
        value: Option<String>,
        chain: crate::chain::ChainId,
        chainspec: Option<ChainSpec>,
    ) -> Result<(), String> {
        todo!()
    }
}
