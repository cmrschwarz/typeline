use bstring::BString;
use smallvec::SmallVec;

use crate::{
    chain::ChainId,
    options::{chain_spec::ChainSpec, context_options::ContextOptions},
    transform::{DataKind, TfBase, Transform},
};

use super::{OpBase, Operation, OperationCatalogMember};

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

    fn apply<'a: 'b, 'b>(
        &'a mut self,
        tf_stack: &'b mut [&'a mut dyn crate::transform::Transform],
    ) -> &'b mut dyn crate::transform::Transform {
        let parent = tf_stack.last_mut().unwrap().base_mut();
        let mut tf_base = TfBase::from_parent(parent);
        tf_base.needs_stdout = true;
        tf_base.data_kind = DataKind::None;
        let tfp = Box::new(TfPrint { tf_base });
        parent.dependants.push(tfp);
        parent.dependants.last_mut().unwrap().as_mut()
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
        tf_stack: &'b [&'a dyn Transform],
        sc: &'a crate::transform::StreamChunk,
    ) -> Option<&'a crate::transform::StreamChunk> {
        todo!()
    }

    fn evaluate<'a, 'b>(&'a mut self, tf_stack: &'b [&'a dyn Transform]) {
        todo!()
    }

    fn data<'a, 'b>(
        &'a self,
        tf_stack: &'b [&'a dyn Transform],
    ) -> Option<&'a crate::transform::MatchData> {
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
        curr_chain: ChainId,
        chainspec: Option<ChainSpec>,
    ) -> Result<(), String> {
        todo!()
    }
}
