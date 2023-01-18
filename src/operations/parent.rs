use bstring::BString;
use smallvec::SmallVec;

use crate::{
    chain::ChainId,
    options::{argument::CliArgument, chain_spec::ChainSpec, context_options::ContextOptions},
    transform::{MatchData, StreamChunk, TfBase, Transform, TransformStackIndex},
};

use super::{
    OpBase, Operation, OperationCatalogMember, OperationError, OperationId, OperationOffsetInChain,
    OperationRef,
};

pub struct TfParent {
    pub tf_base: TfBase,
    pub op_ref: Option<OperationRef>,
    pub offset: TransformStackIndex,
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
        let mut parent_idx = self.tf_base.tfs_index as usize;
        for i in 0..self.offset {
            if tf_stack[parent_idx].base().begin_of_chain || parent_idx == 0 {
                assert!(false);
            }
            parent_idx -= 1;
        }

        tf_stack[parent_idx].data(&tf_stack[0..parent_idx as usize])
    }

    fn evaluate(&mut self, tf_stack: &mut [Box<dyn Transform>]) -> Result<bool, OperationError> {
        Ok(true)
    }
}

#[derive(Clone)]
pub struct OpParent {
    op_base: OpBase,
    offset: TransformStackIndex,
}

impl OpParent {
    pub fn new(
        label: Option<String>,
        chainspec: Option<ChainSpec>,
        offset: TransformStackIndex,
    ) -> OpParent {
        assert!(offset > 0);
        OpParent {
            op_base: OpBase::new(label, chainspec, None),
            offset,
        }
    }
}

impl Operation for OpParent {
    fn apply(
        &self,
        op_ref: OperationRef,
        tf_stack: &mut [Box<dyn Transform>],
    ) -> Result<Box<dyn Transform>, OperationError> {
        let parent = tf_stack.last_mut().unwrap().base_mut();
        let tf_base = TfBase::from_parent(parent);
        let tfp = Box::new(TfParent {
            tf_base,
            op_ref: Some(op_ref),
            offset: self.offset,
        });
        parent.dependants.push(tfp.tf_base.tfs_index);
        Ok(tfp)
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
        label: Option<String>,
        chainspec: Option<ChainSpec>,
        value: Option<BString>,
        cli_arg: Option<CliArgument>,
    ) -> Result<Box<dyn Operation>, OperationError> {
        let up_count = if let Some(value) = value {
            value.parse::<TransformStackIndex>().map_err(|_| {
                OperationError::new(
                    "failed to parse parent argument as integer".to_owned(),
                    Some(ctx.curr_chain),
                    None,
                )
            })?
        } else {
            1
        };
        let mut op_parent = OpParent::new(label, chainspec, up_count);
        op_parent.op_base.cli_arg = cli_arg;
        Ok(Box::new(op_parent))
    }
}
