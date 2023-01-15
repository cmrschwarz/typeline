use bstring::BString;
use smallvec::SmallVec;

use crate::{
    chain::ChainId,
    options::{chain_spec::ChainSpec, context_options::ContextOptions},
    transform::{MatchData, StreamChunk, TfBase, Transform, TransformStackIndex},
};

use super::{OpBase, Operation, OperationCatalogMember, OperationId};

struct TfParent {
    tf_base: TfBase,
    parent_idx: TransformStackIndex,
}

impl Transform for TfParent {
    fn base(&self) -> &TfBase {
        &self.tf_base
    }

    fn base_mut(&mut self) -> &mut TfBase {
        &mut self.tf_base
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
    op_base: OpBase,
    up_count: TransformStackIndex,
}

impl Operation for OpParent {
    fn apply<'a: 'b, 'b>(
        &'a mut self,
        tf_stack: &'b mut [&'a mut dyn Transform],
    ) -> &'b mut dyn Transform {
        let parent = tf_stack.last_mut().unwrap().base_mut();
        let tf_base = TfBase::from_parent(parent);
        let tfp = Box::new(TfParent {
            tf_base,
            parent_idx: parent.stack_index + 1 - self.up_count,
        });
        parent.dependants.push(tfp);
        parent.dependants.last_mut().unwrap().as_mut()
    }

    fn base(&self) -> &super::OpBase {
        todo!()
    }

    fn base_mut(&mut self) -> &mut super::OpBase {
        todo!()
    }
}

impl OperationCatalogMember for OpParent {
    fn name_matches(name: &str) -> bool {
        "parent".starts_with(name) && name.len() > 1
    }

    fn create(
        ctx: &ContextOptions,
        argname: String,
        label: Option<String>,
        value: Option<BString>,
        curr_chain: ChainId,
        chainspec: Option<ChainSpec>,
    ) -> Result<(), String> {
        todo!()
    }
}
