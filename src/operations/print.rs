use bstring::BString;
use smallvec::SmallVec;

use crate::{
    chain::ChainId,
    options::ChainSpec,
    transform::{DataKind, TfBase, Transform},
};

use super::{Operation, OperationCatalogMember};

struct TfPrint {
    tfb: TfBase,
}

#[derive(Clone)]
pub struct OpPrint {}

impl Operation for OpPrint {
    fn apply<'a: 'b, 'b>(
        &'a mut self,
        tf_stack: &'b mut [&'a mut dyn crate::transform::Transform],
    ) -> &'b mut dyn crate::transform::Transform {
        let btfb = tf_stack.last_mut().unwrap().base_mut();
        let tfp = Box::new(TfPrint {
            tfb: TfBase {
                data_kind: DataKind::None,
                is_stream: btfb.is_stream,
                stack_index: btfb.stack_index + 1,
                requires_eval: btfb.requires_eval,
                dependants: SmallVec::new(),
            },
        });
        btfb.dependants.push(tfp);
        btfb.dependants.last_mut().unwrap().as_mut()
    }
}

impl Transform for TfPrint {
    fn base(&self) -> &TfBase {
        &self.tfb
    }

    fn base_mut(&mut self) -> &mut TfBase {
        &mut self.tfb
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
        ctx: &crate::options::ContextOptions,
        argname: String,
        label: Option<String>,
        value: Option<BString>,
        curr_chain: ChainId,
        chainspec: Option<ChainSpec>,
    ) -> Result<(), String> {
        todo!()
    }
}
