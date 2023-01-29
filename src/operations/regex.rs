use std::collections::{HashMap, VecDeque};

use regex::Regex;
use smallvec::SmallVec;

use crate::{
    context::ContextData,
    operations::transform::{DataKind, MatchData, TfBase, Transform},
    options::context_options::ContextOptions,
};

use super::{
    transform::{TransformApplicationError, TransformOutput, TransformStackIndex},
    OpBase, Operation, OperationApplicationError, OperationCatalogMember, OperationCreationError,
    OperationParameters, OperationRef,
};

struct TfRegex {
    tf_base: TfBase,
    regex: Regex,
    op_ref: OperationRef,
}

#[derive(Clone)]
pub struct OpRegex {
    pub op_base: OpBase,
    pub regex: Regex,
}

impl OpRegex {
    pub fn new(regex: Regex) -> OpRegex {
        OpRegex {
            op_base: OpBase::new("regex".to_owned(), None, None, None),
            regex,
        }
    }
}

impl Operation for OpRegex {
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
        let (parent, tf_stack) = tf_stack.split_last_mut().unwrap();
        let mut tf_base = TfBase::from_parent(parent);
        tf_base.data_kind = DataKind::Text;
        let tfp = Box::new(TfRegex {
            tf_base,
            op_ref,
            regex: self.regex.clone(),
        });
        parent
            .add_dependant(tf_stack, tfp.tf_base.tfs_index)
            .map_err(|tae| {
                OperationApplicationError::from_transform_application_error(
                    tae,
                    self.op_base.op_id.unwrap(),
                )
            })?;
        Ok(tfp)
    }
}

impl Transform for TfRegex {
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
        output: &mut VecDeque<TransformOutput>,
    ) -> Result<(), TransformApplicationError> {
        if tfo.is_last_chunk.is_some() {
            return Err(TransformApplicationError::new(
                "the regex transform does not support streams",
                self.op_ref,
            ));
        }
        match &tfo.data {
            Some(MatchData::Text(text)) => {
                let mut match_index = tfo.match_index;
                for cap in self.regex.captures_iter(text) {
                    output.push_back(TransformOutput {
                        match_index,
                        data: Some(MatchData::Text(cap.get(0).unwrap().as_str().to_owned())),
                        args: Vec::default(), //todo
                        is_last_chunk: None,
                    });
                    match_index += 1;
                }
                Ok(())
            }
            Some(md) => Err(TransformApplicationError {
                message: format!(
                    "the regex transform does not support match data kind '{}'",
                    md.kind().to_str()
                ),
                op_ref: self.op_ref,
            }),
            None => Err(TransformApplicationError::new(
                "unexpected none match for regex transform",
                self.op_ref,
            )),
        }
    }
}

impl OperationCatalogMember for OpRegex {
    fn name_matches(name: &str) -> bool {
        "regex".starts_with(name)
    }

    fn create(
        _ctx: &ContextOptions,
        params: OperationParameters,
    ) -> Result<Box<dyn Operation>, OperationCreationError> {
        if let Some(ref value) = params.value {
            match value.to_str() {
                Err(_) => Err(OperationCreationError::new(
                    "regex pattern must be legal UTF-8",
                    params.cli_arg.map(|arg| arg.idx),
                )),
                Ok(value) => Ok(Box::new(OpRegex {
                    regex: Regex::new(value).map_err(|_| {
                        OperationCreationError::new(
                            "failed to compile regex",
                            params.cli_arg.as_ref().map(|arg| arg.idx),
                        )
                    })?,
                    op_base: OpBase::from_op_params(params),
                })),
            }
        } else {
            return Err(OperationCreationError::new(
                "regex needs an argument",
                params.cli_arg.map(|arg| arg.idx),
            ));
        }
    }
}
