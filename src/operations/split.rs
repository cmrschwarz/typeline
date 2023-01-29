use std::collections::{HashMap, VecDeque};

use smallvec::SmallVec;

use crate::{
    context::ContextData,
    operations::transform::{MatchData, TfBase, Transform, TransformStackIndex},
    options::context_options::ContextOptions,
};

use super::{
    transform::{TransformApplicationError, TransformOutput},
    OpBase, Operation, OperationApplicationError, OperationCatalogMember, OperationCreationError,
    OperationParameters, OperationRef,
};

pub struct TfSplit {
    pub tf_base: TfBase,
    pub op_ref: OperationRef,
    pub offset: TransformStackIndex,
}

impl Transform for TfSplit {
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
        tfo: &TransformOutput,
        _output: &mut VecDeque<TransformOutput>,
    ) -> Result<(), TransformApplicationError> {
        debug_assert!(tfo.data.is_none());
        Ok(())
    }
}

#[derive(Clone)]
pub struct OpSplit {
    op_base: OpBase,
    range: RangeSpec<ChainId>,
}

impl OpSplit {
    pub fn new(range: RangeSpec<ChainId>) -> OpSplit {
        assert!(offset > 0);
        Box::new(OpSplit {
            op_base: OpBase::new("split".to_owned(), None, None, None),
            offset,
        })
    }
}

impl Operation for OpSplit {
    fn apply(
        &self,
        op_ref: OperationRef,
        tf_stack: &mut [Box<dyn Transform>],
    ) -> Result<Box<dyn Transform>, OperationApplicationError> {
        let parent = tf_stack.last_mut().unwrap().base_mut();
        let tf_base = TfBase::from_parent(parent);
        let tfp = Box::new(TfSplit {
            tf_base,
            op_ref: op_ref,
            offset: self.offset,
        });
        Ok(tfp)
    }

    fn base(&self) -> &super::OpBase {
        &self.op_base
    }

    fn base_mut(&mut self) -> &mut super::OpBase {
        &mut self.op_base
    }
}

impl OperationCatalogMember for OpSplit {
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
        if offset == 0 {
            return Err(OperationCreationError::new(
                "parent offset cannot be 0",
                params.cli_arg.as_ref().map(|arg| arg.idx),
            ));
        }
        Ok(Box::new(OpSplit {
            op_base: OpBase::from_op_params(params),
            offset,
        }))
    }
}
