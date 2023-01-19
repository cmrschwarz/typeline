use crate::{
    context::ContextData,
    operations::transform::{MatchData, StreamChunk, TfBase, Transform, TransformStackIndex},
    options::context_options::ContextOptions,
};

use super::{
    transform::TransformApplicationError, OpBase, Operation, OperationApplicationError,
    OperationCatalogMember, OperationCreationError, OperationParameters, OperationRef,
};

pub struct TfParent {
    pub tf_base: TfBase,
    pub op_ref: OperationRef,
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
        _ctx: &'a ContextData,
        _tf_stack: &'a [Box<dyn Transform>],
        sc: &'b StreamChunk<'b>,
        _final_chunk: bool,
    ) -> Result<Option<&'b StreamChunk<'b>>, TransformApplicationError> {
        Ok(Some(sc))
    }

    fn data<'a>(
        &'a self,
        ctx: &'a ContextData,
        tf_stack: &'a [Box<dyn Transform>],
    ) -> Result<Option<&'a MatchData>, TransformApplicationError> {
        let mut parent_idx = self.tf_base.tfs_index as usize;
        for _ in 0..self.offset {
            if tf_stack[parent_idx].base().begin_of_chain || parent_idx == 0 {
                return Err(TransformApplicationError::new(
                    "no element at the requested depth for 'parent'",
                    self.op_ref,
                ));
            }
            parent_idx -= 1;
        }

        tf_stack[parent_idx].data(ctx, &tf_stack[0..parent_idx as usize])
    }

    fn evaluate(
        &mut self,
        _ctx: &ContextData,
        _tf_stack: &mut [Box<dyn Transform>],
    ) -> Result<bool, TransformApplicationError> {
        Ok(true)
    }
}

#[derive(Clone)]
pub struct OpParent {
    op_base: OpBase,
    offset: TransformStackIndex,
}

impl OpParent {
    pub fn new(offset: TransformStackIndex) -> Box<OpParent> {
        assert!(offset > 0);
        Box::new(OpParent {
            op_base: OpBase::new("parent".to_owned(), None, None, None),
            offset,
        })
    }
}

impl Operation for OpParent {
    fn apply(
        &self,
        op_ref: OperationRef,
        tf_stack: &mut [Box<dyn Transform>],
    ) -> Result<Box<dyn Transform>, OperationApplicationError> {
        let parent = tf_stack.last_mut().unwrap().base_mut();
        let tf_base = TfBase::from_parent(parent);
        let tfp = Box::new(TfParent {
            tf_base,
            op_ref: op_ref,
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
        _ctx: &ContextOptions,
        params: OperationParameters,
    ) -> Result<Box<dyn Operation>, OperationCreationError> {
        let offset = if let Some(ref value) = params.value {
            value.parse::<TransformStackIndex>().map_err(|_| {
                OperationCreationError::new(
                    "failed to parse parent argument as integer",
                    params.cli_arg.as_ref().map(|arg| arg.idx),
                )
            })?
        } else {
            1
        };
        Ok(Box::new(OpParent {
            op_base: OpBase::from_op_params(params),
            offset,
        }))
    }
}
