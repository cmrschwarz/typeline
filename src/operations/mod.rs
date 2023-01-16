pub mod html;
pub mod parent;
pub mod print;
pub mod read_stdin;
pub mod start;

use std::error::Error;

use bstring::BString;
use smallvec::SmallVec;

use crate::chain::ChainId;
use crate::options::argument::CliArgument;
use crate::options::{chain_spec::ChainSpec, context_options::ContextOptions};
use crate::transform::Transform;

use self::parent::OpParent;
use self::print::OpPrint;

pub type OperationId = u32;
pub type OperationOffsetInChain = u32;

#[derive(Clone, Debug)]
pub struct OperationRef {
    pub cn_id: ChainId,
    pub op_offset: OperationOffsetInChain,
}

#[derive(Debug)]
pub struct OperationError {
    pub message: String,
    pub op_ref: OperationRef,
}
impl std::fmt::Display for OperationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "in chain {}, op {}: {}",
            self.op_ref.cn_id, self.op_ref.op_offset, self.message
        )
    }
}
impl Error for OperationError {}
impl OperationError {
    pub fn new(message: String, op_ref: OperationRef) -> OperationError {
        OperationError { message, op_ref }
    }
}

impl OperationRef {
    pub fn new(cn_id: ChainId, op_offset: OperationOffsetInChain) -> Self {
        Self { cn_id, op_offset }
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
    pub(crate) label: Option<String>,
    pub(crate) chainspec: Option<ChainSpec>,
    pub(crate) curr_chain: Option<ChainId>, // set by the context on add_op
    pub(crate) cli_arg: Option<CliArgument>,
    // filled during setup once the chainspec can be evaluated
    pub(crate) op_refs: SmallVec<[OperationRef; 2]>,
}

impl OpBase {
    pub fn new(
        label: Option<String>,
        chainspec: Option<ChainSpec>,
        cli_arg: Option<CliArgument>,
    ) -> OpBase {
        OpBase {
            label,
            chainspec,
            cli_arg,
            curr_chain: None,
            op_refs: SmallVec::new(),
        }
    }
}

pub trait Operation: OperationCloneBox + Send + Sync {
    fn base(&self) -> &OpBase;
    fn base_mut(&mut self) -> &mut OpBase;
    fn apply(&self, tf_stack: &mut [Box<dyn Transform>]) -> Box<dyn Transform>;
}

impl std::ops::Deref for dyn Operation {
    type Target = OpBase;

    fn deref(&self) -> &Self::Target {
        self.base()
    }
}
impl std::ops::DerefMut for dyn Operation {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.base_mut()
    }
}

pub trait OperationCatalogMember: Operation {
    fn name_matches(name: &str) -> bool;
    fn create(
        ctx: &ContextOptions,
        label: Option<String>,
        chainspec: Option<ChainSpec>,
        value: Option<BString>,
        cli_arg: Option<CliArgument>,
    ) -> Result<Box<dyn Operation>, OperationError>;
}

pub struct OperationCatalogEntry {
    pub name_matches: fn(name: &str) -> bool,
    pub create: fn(
        ctx: &ContextOptions,
        label: Option<String>,
        chainspec: Option<ChainSpec>,
        value: Option<BString>,
        cli_arg: Option<CliArgument>,
    ) -> Result<Box<dyn Operation>, OperationError>,
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
