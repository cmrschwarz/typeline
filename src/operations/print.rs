use std::{collections::HashMap, io::Write};

use smallvec::SmallVec;

use crate::{
    context::ContextData,
    operations::transform::{DataKind, MatchData, TfBase, Transform},
    options::context_options::ContextOptions,
    plattform::NEWLINE_BYTES,
};

use super::{
    transform::{TransformApplicationError, TransformOutput},
    OpBase, Operation, OperationApplicationError, OperationCatalogMember, OperationCreationError,
    OperationParameters, OperationRef,
};

struct TfPrint {
    tf_base: TfBase,
    op_ref: OperationRef,
}

#[derive(Clone)]
pub struct OpPrint {
    pub op_base: OpBase,
}

impl OpPrint {
    pub fn new() -> Box<OpPrint> {
        Box::new(OpPrint {
            op_base: OpBase::new("print".to_owned(), None, None, None),
        })
    }
}

impl Operation for OpPrint {
    fn base(&self) -> &super::OpBase {
        &self.op_base
    }

    fn base_mut(&mut self) -> &mut super::OpBase {
        &mut self.op_base
    }

    fn apply(
        &self,
        op_ref: OperationRef,
        tf_stack: &mut [Box<dyn Transform>],
    ) -> Result<Box<dyn Transform>, OperationApplicationError> {
        let parent = tf_stack.last_mut().unwrap().base_mut();
        let mut tf_base = TfBase::from_parent(parent);
        tf_base.needs_stdout = true;
        tf_base.data_kind = DataKind::None;
        let tfp = Box::new(TfPrint { tf_base, op_ref });
        parent.dependants.push(tfp.tf_base.tfs_index);
        Ok(tfp)
    }
}

impl Transform for TfPrint {
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
        match &tfo.data {
            Some(MatchData::Bytes(b)) => {
                let mut s = std::io::stdout();
                s.write(b.as_slice())
                    .and_then(|_| s.write(NEWLINE_BYTES))
                    .map_err(|_| TransformApplicationError::new("io error", self.op_ref))?;
            }
            Some(MatchData::Text(t)) => {
                let mut s = std::io::stdout();
                s.write(t.as_bytes())
                    .and_then(|_| s.write(NEWLINE_BYTES))
                    .map_err(|_| TransformApplicationError::new("io error", self.op_ref))?;
            }
            Some(MatchData::Html(_)) => {
                return Err(TransformApplicationError::new(
                    "the print transform does not support html",
                    self.op_ref,
                ));
            }
            Some(MatchData::Png(_)) => {
                return Err(TransformApplicationError::new(
                    "the print transform does not support images",
                    self.op_ref,
                ));
            }
            _ => panic!("missing TfSerialize"),
        }
        Ok(SmallVec::default())
    }
}

impl OperationCatalogMember for OpPrint {
    fn name_matches(name: &str) -> bool {
        "print".starts_with(name)
    }

    fn create(
        _ctx: &ContextOptions,
        params: OperationParameters,
    ) -> Result<Box<dyn Operation>, OperationCreationError> {
        if params.value.is_some() {
            return Err(OperationCreationError::new(
                "print takes no argument",
                params.cli_arg.map(|arg| arg.idx),
            ));
        }
        Ok(Box::new(OpPrint {
            op_base: OpBase::from_op_params(params),
        }))
    }
}
