use smallvec::SmallVec;

use crate::{
    chain::ChainId,
    options::{ChainSpec, ContextOptions},
    transform::{MatchData, StreamChunk, TfBase, Transform, TransformStackIndex},
    xstr::{XStr, XString},
};

use super::{Operation, OperationCatalogMember, OperationId};

struct TfParent {
    tfb: TfBase,
    parent_idx: TransformStackIndex,
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
        tf_stack[self.parent_idx as usize].data(&tf_stack[0..self.parent_idx as usize])
    }

    fn evaluate<'a, 'b>(&'a mut self, _tf_stack: &'b [&'a dyn Transform]) {}
}

#[derive(Clone)]
pub struct OpParent {
    up_count: TransformStackIndex,
}

impl Operation for OpParent {
    fn apply<'a: 'b, 'b>(
        &'a mut self,
        tf_stack: &'b mut [&'a mut dyn Transform],
    ) -> &'b mut dyn Transform {
        let btfb = tf_stack.last_mut().unwrap().base_mut();
        let tfp = Box::new(TfParent {
            tfb: TfBase {
                data_kind: btfb.data_kind,
                is_stream: btfb.is_stream,
                stack_index: btfb.stack_index + 1,
                requires_eval: btfb.requires_eval,
                dependants: SmallVec::new(),
            },
            parent_idx: btfb.stack_index + 1 - self.up_count,
        });
        btfb.dependants.push(tfp);
        btfb.dependants.last_mut().unwrap().as_mut()
    }
}

impl OperationCatalogMember for OpParent {
    fn name_matches(name: &str) -> bool {
        "parent".starts_with(name) && name.len() > 1
    }

    fn create(
        ctx: &ContextOptions,
        label: String,
        value: Option<XString>,
        curr_chain: ChainId,
        chainspec: Option<ChainSpec>,
    ) -> Result<(), String> {
        todo!()
    }
}
