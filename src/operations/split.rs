use std::collections::{HashMap, VecDeque};

use smallvec::SmallVec;

use crate::{
    chain::ChainId,
    context::SessionData,
    match_data::MatchData,
    operations::transform::{TfBase, Transform, TransformStackIndex},
    options::{context_options::ContextOptions, range_spec::RangeSpec},
};

use super::{
    operation::{
        OpBase, Operation, OperationApplicationError, OperationCreationError, OperationParameters,
        OperationRef,
    },
    operation_catalog::OperationCatalogMember,
    transform::{TransformApplicationError, TransformOutput},
};

pub struct TfSplit<'a> {
    pub tf_base: TfBase,
    pub op_ref: OperationRef,
    pub target_chains: &'a Vec<ChainId>,
}

impl<'a> Transform for TfSplit<'a> {
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
    range_spec: RangeSpec<ChainId>,
    target_chains: HashMap<ChainId, Vec<ChainId>>,
}

impl OpSplit {
    pub fn new(range_spec: RangeSpec<ChainId>) -> OpSplit {
        OpSplit {
            op_base: OpBase::new("split".to_owned(), None, None, None),
            range_spec,
            target_chains: HashMap::new(),
        }
    }
}

impl Operation for OpSplit {
    fn apply<'a, 'b>(
        &'a self,
        op_ref: OperationRef,
        tf_stack: &mut [Box<dyn Transform + 'b>],
    ) -> Result<Box<dyn Transform + 'a>, OperationApplicationError> {
        let parent = tf_stack.last_mut().unwrap().base_mut();
        let tf_base = TfBase::from_parent(parent);
        let tfp = Box::new(TfSplit {
            tf_base,
            op_ref: op_ref,
            target_chains: &self.target_chains[&op_ref.chain_id],
        });
        Ok(tfp)
    }

    fn base(&self) -> &OpBase {
        &self.op_base
    }

    fn base_mut(&mut self) -> &mut OpBase {
        &mut self.op_base
    }
}

impl OperationCatalogMember for OpSplit {
    fn name_matches(name: &str) -> bool {
        "split".starts_with(name) && name.len() > 1
    }
    fn create(
        _ctx: &ContextOptions,
        params: OperationParameters,
    ) -> Result<Box<dyn Operation>, OperationCreationError> {
        let range_spec = if let Some(ref value) = params.value {
            RangeSpec::<ChainId>::parse(value.to_str().map_err(|_| {
                OperationCreationError::new(
                    "failed to parse parent argument as range spec: invalid UTF-8",
                    params.cli_arg.as_ref().map(|arg| arg.idx),
                )
            })?)
            .map_err(|_| {
                OperationCreationError::new(
                    "failed to parse parent argument as range spec",
                    params.cli_arg.as_ref().map(|arg| arg.idx),
                )
            })?
        } else {
            RangeSpec::Bounded(Some(0), None)
        };
        Ok(Box::new(OpSplit {
            op_base: OpBase::from_op_params(params),
            range_spec,
            target_chains: Default::default(),
        }))
    }
}
