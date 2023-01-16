use std::io::Write;

use bstring::BString;
use smallvec::SmallVec;

use crate::{
    chain::ChainId,
    options::{chain_spec::ChainSpec, context_options::ContextOptions},
    plattform::{NEWLINE, NEWLINE_BYTES},
    transform::{DataKind, MatchData, StreamChunk, TfBase, Transform},
};

use super::{OpBase, Operation, OperationCatalogMember, OperationError};

struct TfPrint {
    tf_base: TfBase,
}

#[derive(Clone)]
pub struct OpPrint {
    op_base: OpBase,
}

impl Operation for OpPrint {
    fn base(&self) -> &super::OpBase {
        &self.op_base
    }

    fn base_mut(&mut self) -> &mut super::OpBase {
        &mut self.op_base
    }

    fn apply(&self, tf_stack: &mut [Box<dyn Transform>]) -> Box<dyn Transform> {
        let parent = tf_stack.last_mut().unwrap().base_mut();
        let mut tf_base = TfBase::from_parent(parent);
        tf_base.needs_stdout = true;
        tf_base.data_kind = DataKind::None;
        let tfp = Box::new(TfPrint { tf_base });
        parent.dependants.push(tfp.tf_base.tfs_index);
        tfp
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
        sc: &'b StreamChunk<'b>,
    ) -> Option<&'b StreamChunk<'b>> {
        todo!()
    }

    fn evaluate(&mut self, tf_stack: &mut [Box<dyn Transform>]) -> bool {
        match tf_stack[self.tf_base.tfs_index as usize - 1].data(tf_stack) {
            Some(MatchData::Bytes(b)) => {
                let mut s = std::io::stdout();
                s.write(b.as_slice());
                s.write(NEWLINE_BYTES);
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
        true
    }

    fn data<'a>(&'a self, tf_stack: &'a [Box<dyn Transform>]) -> Option<&'a MatchData> {
        todo!()
    }
}

impl OperationCatalogMember for OpPrint {
    fn name_matches(name: &str) -> bool {
        "print".starts_with(name)
    }

    fn create(
        ctx: &ContextOptions,
        argname: String,
        label: Option<String>,
        value: Option<BString>,
        chainspec: Option<ChainSpec>,
    ) -> Result<Box<dyn Operation>, OperationError> {
        Ok(Box::new(OpPrint {
            op_base: OpBase::new(argname, label, chainspec),
        }))
    }
}
