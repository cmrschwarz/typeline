use std::io::Write;

use regex::Regex;

use crate::{
    context::ContextData,
    operations::transform::{DataKind, MatchData, StreamChunk, TfBase, Transform},
    options::context_options::ContextOptions,
    plattform::NEWLINE_BYTES,
};

use super::{
    transform::TransformApplicationError, OpBase, Operation, OperationApplicationError,
    OperationCatalogMember, OperationCreationError, OperationParameters, OperationRef,
};

struct TfRegex {
    tf_base: TfBase,
    op_ref: OperationRef,
}

#[derive(Clone)]
pub struct OpRegex {
    pub op_base: OpBase,
    pub regex: Regex,
}

impl OpRegex {
    pub fn new(regex: Regex) -> Box<OpRegex> {
        Box::new(OpRegex {
            op_base: OpBase::new("regex".to_owned(), None, None, None),
            regex,
        })
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
        let parent = tf_stack.last_mut().unwrap().base_mut();
        let mut tf_base = TfBase::from_parent(parent);
        tf_base.data_kind = DataKind::Text;
        let tfp = Box::new(TfRegex { tf_base, op_ref });
        parent.dependants.push(tfp.tf_base.tfs_index);
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

    fn process_chunk<'a: 'b, 'b>(
        &'a mut self,
        _ctx: &'a ContextData,
        _tf_stack: &'a [Box<dyn Transform>],
        _sc: &'b StreamChunk<'b>,
        _final_chunk: bool,
    ) -> Result<Option<&'b StreamChunk<'b>>, TransformApplicationError> {
        panic!("regex doesn't support streaming mode");
    }

    fn evaluate(
        &mut self,
        ctx: &ContextData,
        tf_stack: &mut [Box<dyn Transform>],
    ) -> Result<bool, TransformApplicationError> {
        match tf_stack[self.tf_base.tfs_index as usize - 1].data(ctx, tf_stack)? {
            Some(MatchData::Bytes(b)) => {
                let mut s = std::io::stdout();
                s.write(b.as_slice())
                    .and_then(|_| s.write(NEWLINE_BYTES))
                    .map_err(|_| TransformApplicationError::new("io error", self.op_ref))?;
            }
            Some(MatchData::Text(s)) => {
                println!("{}", s);
            }
            Some(MatchData::Html(html)) => {
                println!("{}", html);
            }
            Some(MatchData::Png(_)) => {
                //TODO: error
                panic!("refusing to print image");
            }
            _ => panic!("missing TfSerialize"),
        }
        Ok(true)
    }

    fn data<'a>(
        &'a self,
        _ctx: &'a ContextData,
        _tf_stack: &'a [Box<dyn Transform>],
    ) -> Result<Option<&'a MatchData>, TransformApplicationError> {
        todo!()
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
