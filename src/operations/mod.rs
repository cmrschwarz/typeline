pub mod html;
pub mod parent;
pub mod print;
pub mod read_stdin;
pub mod start;
pub mod transform;
use bstring::BString;
use smallvec::SmallVec;
use thiserror::Error;

use self::transform::Transform;
use crate::chain::{Chain, ChainId};
use crate::options::argument::CliArgument;
use crate::options::{chain_spec::ChainSpec, context_options::ContextOptions};

use self::parent::OpParent;
use self::print::OpPrint;

pub type OperationId = u32;
pub type OperationOffsetInChain = u32;

#[derive(Error, Debug, Clone)]
#[error("{message}")]
pub struct OperationCreationError {
    pub message: String,
}

impl OperationCreationError {
    pub fn new(message: &str) -> OperationCreationError {
        OperationCreationError {
            message: message.to_owned(),
        }
    }
}

#[derive(Error, Debug, Clone)]
#[error("in op id {op_id}: {message}")]
pub struct OperationSetupError {
    pub message: String,
    pub op_id: OperationId,
}

impl OperationSetupError {
    pub fn new(message: &str, op_id: OperationId) -> OperationSetupError {
        OperationSetupError {
            message: message.to_owned(),
            op_id,
        }
    }
}

#[derive(Error, Debug, Clone)]
#[error("in op {0} of chain {1}: {message}", op_ref.op_offset, op_ref.chain_id)]
pub struct OperationApplicationError {
    pub message: String,
    pub op_id: OperationId,
    pub op_ref: OperationRef,
}

impl OperationApplicationError {
    pub fn new(
        message: &str,
        op_id: OperationId,
        op_ref: OperationRef,
    ) -> OperationApplicationError {
        OperationApplicationError {
            message: message.to_owned(),
            op_id,
            op_ref,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct OperationRef {
    pub chain_id: ChainId,
    pub op_offset: OperationOffsetInChain,
}

impl OperationRef {
    pub fn new(chain_id: ChainId, op_offset: OperationOffsetInChain) -> Self {
        Self {
            chain_id,
            op_offset,
        }
    }
}

pub trait OperationCloneBox {
    fn clone_box(&self) -> Box<dyn Operation>;
}

impl<T: Operation + Clone + 'static> OperationCloneBox for T {
    fn clone_box(&self) -> Box<dyn Operation> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn Operation> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

#[derive(Clone)]
pub struct OpBase {
    pub(crate) argname: String,
    pub(crate) label: Option<String>,
    pub(crate) chainspec: Option<ChainSpec>,
    pub(crate) curr_chain: Option<ChainId>, // set by the context on add_op
    pub(crate) op_id: Option<OperationId>,  // set by the context on add_op
    // filled during setup once the chainspec can be evaluated
    pub(crate) op_refs: SmallVec<[OperationRef; 2]>,
}

impl OpBase {
    pub fn new(
        argname: String,
        label: Option<String>,
        chainspec: Option<ChainSpec>,
        _cli_arg: Option<CliArgument>,
    ) -> OpBase {
        OpBase {
            argname,
            label,
            chainspec,
            curr_chain: None,
            op_id: None,
            op_refs: SmallVec::new(),
        }
    }
    pub fn from_op_params(params: OperationParameters) -> OpBase {
        OpBase {
            argname: params.argname,
            label: params.label,
            chainspec: params.chainspec,
            curr_chain: None,
            op_id: None,
            op_refs: SmallVec::new(),
        }
    }
}

pub trait Operation: OperationCloneBox + Send + Sync {
    fn base_mut(&mut self) -> &mut OpBase;
    fn base(&self) -> &OpBase;
    fn apply(
        &self,
        op_ref: OperationRef,
        tf_stack: &mut [Box<dyn Transform>],
    ) -> Result<Box<dyn Transform>, OperationApplicationError>;
    fn setup(&mut self, chains: &mut Vec<Chain>) -> Result<(), OperationSetupError> {
        if let Some(_cs) = &self.base().chainspec {
            todo!("ChainSpec::iter");
        } else {
            let chain_id = self.base().curr_chain.unwrap();
            chains[chain_id as usize]
                .operations
                .push(self.base().op_id.unwrap());
            self.base_mut().op_refs.push(OperationRef::new(
                chain_id,
                chains[chain_id as usize].operations.len() as OperationOffsetInChain,
            ))
        }
        Ok(())
    }
}

pub trait OperationOps {
    fn set_argname(self, argname: String) -> Self;
    fn set_label(self, label: String) -> Self;
    fn set_chainspec(self, chainspec: ChainSpec) -> Self;
}

impl<T: Operation> OperationOps for Box<T> {
    fn set_argname(mut self, argname: String) -> Self {
        self.base_mut().argname = argname;
        self
    }
    fn set_label(mut self, label: String) -> Self {
        self.base_mut().label = Some(label);
        self
    }
    fn set_chainspec(mut self, chainspec: ChainSpec) -> Self {
        self.base_mut().chainspec = Some(chainspec);
        self
    }
}

pub struct OperationParameters {
    pub argname: String,
    pub label: Option<String>,
    pub chainspec: Option<ChainSpec>,
    pub value: Option<BString>,
    pub cli_arg: Option<CliArgument>,
}

pub trait OperationCatalogMember: Operation {
    fn name_matches(name: &str) -> bool;
    fn create(
        ctx: &ContextOptions,
        params: OperationParameters,
    ) -> Result<Box<dyn Operation>, OperationCreationError>;
}

pub struct OperationCatalogEntry {
    pub name_matches: fn(name: &str) -> bool,
    pub create: fn(
        ctx: &ContextOptions,
        params: OperationParameters,
    ) -> Result<Box<dyn Operation>, OperationCreationError>,
}

pub const fn create_catalog_entry<TS: OperationCatalogMember>() -> OperationCatalogEntry {
    OperationCatalogEntry {
        name_matches: TS::name_matches,
        create: TS::create,
    }
}

pub const BUILTIN_OPERATIONS_CATALOG: [OperationCatalogEntry; 2] = [
    create_catalog_entry::<OpParent>(),
    create_catalog_entry::<OpPrint>(),
];
