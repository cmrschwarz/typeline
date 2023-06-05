use std::collections::{HashMap, VecDeque};

use smallvec::SmallVec;

use crate::{
    context::SessionData,
    match_data::MatchData,
    operations::transform::{TfBase, Transform, TransformStackIndex},
    options::context_options::ContextOptions,
};

use super::{
    operation_catalog::OperationCatalogMember,
    operator::{
        Operation, OperationParameters, OperatorApplicationError, OperatorBase,
        OperatorCreationError, OperatorRef,
    },
    transform::{TransformApplicationError, TransformOutput},
};

pub struct TfParent {
    pub tf_base: TfBase,
    pub op_ref: OperatorRef,
    pub offset: TransformStackIndex,
}

impl Transform for TfParent {
    fn base(&self) -> &TfBase {
        &self.tf_base
    }

    fn base_mut(&mut self) -> &mut TfBase {
        &mut self.tf_base
    }

    fn process(
        &mut self,
        _ctx: &SessionData,
        _args: &HashMap<String, SmallVec<[(TransformStackIndex, MatchData); 1]>>,
        _tfo: &TransformOutput,
        _output: &mut VecDeque<TransformOutput>,
    ) -> Result<(), TransformApplicationError> {
        panic!("TfParent does not process elements");
    }

    fn add_dependant<'a>(
        &mut self,
        tf_stack: &mut [Box<dyn Transform + 'a>],
        dependant: TransformStackIndex,
    ) -> Result<(), TransformApplicationError> {
        let mut parent_idx = self.tf_base.tfs_index as usize - 1;
        for _ in 0..self.offset {
            if tf_stack[parent_idx].base().begin_of_chain || parent_idx == 0 {
                return Err(TransformApplicationError::new(
                    "no element at the requested depth for 'parent'",
                    self.op_ref,
                ));
            }
            parent_idx -= 1;
        }
        let (head, tail) = tf_stack.split_at_mut(parent_idx);
        tail[0].add_dependant(head, dependant)
    }
}

impl OpParent {
    pub fn new(offset: TransformStackIndex) -> OpParent {
        assert!(offset > 0);
        OpParent {
            op_base: OperatorBase::new("parent".to_owned(), None, None, None),
            offset,
        }
    }
}

impl Operation for OpParent {
    fn apply<'a, 'b>(
        &'a self,
        op_ref: OperatorRef,
        tf_stack: &mut [Box<dyn Transform + 'b>],
    ) -> Result<Box<dyn Transform + 'a>, OperatorApplicationError> {
        let parent = tf_stack.last_mut().unwrap().base_mut();
        let tf_base = TfBase::from_parent(parent);
        let tfp = Box::new(TfParent {
            tf_base,
            op_ref: op_ref,
            offset: self.offset,
        });
        Ok(tfp)
    }

    fn base(&self) -> &OperatorBase {
        &self.op_base
    }

    fn base_mut(&mut self) -> &mut OperatorBase {
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
    ) -> Result<Box<dyn Operation>, OperatorCreationError> {
        let offset = if let Some(ref value) = params.value {
            value.parse::<TransformStackIndex>().map_err(|_| {
                OperatorCreationError::new(
                    "failed to parse parent argument as integer",
                    params.cli_arg.as_ref().map(|arg| arg.idx),
                )
            })?
        } else {
            1
        };
        if offset == 0 {
            return Err(OperatorCreationError::new(
                "parent offset cannot be 0",
                params.cli_arg.as_ref().map(|arg| arg.idx),
            ));
        }
        Ok(Box::new(OpParent {
            op_base: OperatorBase::from_op_params(params),
            offset,
        }))
    }
}
