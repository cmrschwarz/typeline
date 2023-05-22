use std::{
    collections::{HashMap, VecDeque},
    io::Write,
};

use smallvec::SmallVec;

use crate::{
    context::SessionData,
    match_data::{MatchData, MatchDataKind},
    operations::transform::{TfBase, Transform},
    options::context_options::ContextOptions,
    plattform::NEWLINE_BYTES,
};

use super::{
    operation::{
        OpBase, Operation, OperationApplicationError, OperationCreationError, OperationParameters,
        OperationRef,
    },
    operation_catalog::OperationCatalogMember,
    transform::{TransformApplicationError, TransformOutput, TransformStackIndex},
};

struct TfPrint {
    tf_base: TfBase,
    op_ref: OperationRef,
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
        _ctx: &SessionData,
        _args: &HashMap<String, SmallVec<[(TransformStackIndex, MatchData); 1]>>,
        tfo: &TransformOutput,
        _output: &mut VecDeque<TransformOutput>,
    ) -> Result<(), TransformApplicationError> {
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
        Ok(())
    }
}

#[derive(Clone)]
pub struct OpPrint {
    pub op_base: OpBase,
}

impl OpPrint {
    pub fn new() -> OpPrint {
        OpPrint {
            op_base: OpBase::new("print".to_owned(), None, None, None),
        }
    }
}

pub fn split_last_mut_box_aray<'a, 'b>(
    arg: &'b mut [Box<dyn Transform + 'a>],
) -> Option<(
    &'b mut Box<dyn Transform + 'a>,
    &'b mut [Box<dyn Transform + 'a>],
)> {
    if let [init @ .., last] = arg {
        Some((last, init))
    } else {
        None
    }
}

impl Operation for OpPrint {
    fn base(&self) -> &OpBase {
        &self.op_base
    }

    fn base_mut(&mut self) -> &mut OpBase {
        &mut self.op_base
    }

    fn apply<'a, 'b>(
        &'a self,
        op_ref: OperationRef,
        tf_stack: &mut [Box<dyn Transform + 'b>],
    ) -> Result<Box<dyn Transform + 'a>, OperationApplicationError> {
        let (parent, tf_stack) = tf_stack.split_last_mut().unwrap();
        let mut tf_base = TfBase::from_parent(parent.base());
        tf_base.needs_stdout = true;
        tf_base.data_kind = MatchDataKind::None;
        let tfp = Box::new(TfPrint { tf_base, op_ref });
        parent
            .add_dependant(tf_stack, tfp.base().tfs_index)
            .map_err(|tae| OperationApplicationError::from_transform_application_error(tae))?;
        Ok(tfp)
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
