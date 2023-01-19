use std::io::Write;

use crate::{
    operations::transform::{DataKind, MatchData, StreamChunk, TfBase, Transform},
    options::context_options::ContextOptions,
    plattform::NEWLINE_BYTES,
};

use super::{
    transform::TransformApplicationError, OpBase, Operation, OperationApplicationError,
    OperationCatalogMember, OperationCreationError, OperationParameters, OperationRef,
};

#[derive(Clone)]
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

    fn process_chunk<'a: 'b, 'b>(
        &'a mut self,
        _tf_stack: &'a [Box<dyn Transform>],
        _sc: &'b StreamChunk<'b>,
        _final_chunk: bool,
    ) -> Result<Option<&'b StreamChunk<'b>>, TransformApplicationError> {
        todo!()
    }

    fn evaluate(
        &mut self,
        tf_stack: &mut [Box<dyn Transform>],
    ) -> Result<bool, TransformApplicationError> {
        match tf_stack[self.tf_base.tfs_index as usize - 1].data(tf_stack)? {
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
        _tf_stack: &'a [Box<dyn Transform>],
    ) -> Result<Option<&'a MatchData>, TransformApplicationError> {
        todo!()
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
