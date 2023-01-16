use bstring::BString;
use smallvec::SmallVec;

use crate::{
    chain::ChainId,
    options::{chain_spec::ChainSpec, context_options::ContextOptions},
    transform::{MatchData, StreamChunk, TfBase, Transform, TransformStackIndex},
};

use super::{
    OpBase, Operation, OperationCatalogMember, OperationError, OperationId, OperationOffsetInChain,
    OperationRef,
};

pub struct TfParent {
    pub tf_base: TfBase,
    pub parent_idx: TransformStackIndex,
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
        _tf_stack: &'a [Box<dyn Transform>],
        sc: &'b StreamChunk<'b>,
    ) -> Option<&'b StreamChunk<'b>> {
        Some(sc)
    }

    fn data<'a>(&'a self, tf_stack: &'a [Box<dyn Transform>]) -> Option<&'a MatchData> {
        tf_stack[self.parent_idx as usize].data(&tf_stack[0..self.parent_idx as usize])
    }

    fn evaluate(&mut self, tf_stack: &mut [Box<dyn Transform>]) -> bool {
        true
    }
}

#[derive(Clone)]
pub struct OpParent {
    op_base: OpBase,
    up_count: TransformStackIndex,
}

impl Operation for OpParent {
    fn apply(&self, tf_stack: &mut [Box<dyn Transform>]) -> Box<dyn Transform> {
        let parent = tf_stack.last_mut().unwrap().base_mut();
        let tf_base = TfBase::from_parent(parent);
        let tfp = Box::new(TfParent {
            tf_base,
            parent_idx: parent.tfs_index + 1 - self.up_count,
        });
        parent.dependants.push(tfp.tf_base.tfs_index);
        tfp
    }

    fn base(&self) -> &super::OpBase {
        &self.op_base
    }

    fn base_mut(&mut self) -> &mut super::OpBase {
        &mut self.op_base
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
        chainspec: Option<ChainSpec>,
    ) -> Result<Box<dyn Operation>, OperationError> {
        Ok(Box::new(OpParent {
            op_base: OpBase::new(argname, label, chainspec),
            up_count: if let Some(value) = value {
                value.parse::<TransformStackIndex>().map_err(|_| {
                    OperationError::new(
                        "failed to parse parent argument as integer".to_owned(),
                        OperationRef::new(
                            ctx.curr_chain,
                            ctx.chains[ctx.curr_chain].operations.len() as OperationOffsetInChain,
                        ),
                    )
                })?
            } else {
                1
            },
        }))
    }
}
