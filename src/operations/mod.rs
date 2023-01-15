pub mod html;
pub mod parent;
pub mod print;
pub mod read_stdin;
pub mod start;

use std::error::Error;

use bstring::BString;
use smallvec::SmallVec;

use crate::chain::ChainId;
use crate::options::{chain_spec::ChainSpec, context_options::ContextOptions};
use crate::transform::Transform;

use self::parent::OpParent;
use self::print::OpPrint;

pub type OperationId = usize;

#[derive(Clone, Debug)]
pub struct OperationRef {
    pub cn_id: ChainId,
    pub op_id: OperationId,
}

#[derive(Debug)]
pub struct OperationError {
    message: String,
    op_ref: OperationRef,
}
impl std::fmt::Display for OperationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "in chain {}, op {}: {}",
            self.op_ref.cn_id, self.op_ref.op_id, self.message
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
    pub fn new(cn_id: ChainId, op_id: OperationId) -> Self {
        Self { cn_id, op_id }
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
    argname: String,
    label: String,
    op_refs: SmallVec<[OperationRef; 2]>,
}

pub trait Operation: OperationCloneBox + Send + Sync {
    fn base(&self) -> &OpBase;
    fn base_mut(&mut self) -> &mut OpBase;
    fn apply<'a: 'b, 'b>(
        &'a mut self,
        tf_stack: &'b mut [&'a mut dyn Transform],
    ) -> &'b mut dyn Transform;
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
        argname: String,
        label: Option<String>,
        value: Option<BString>,
        curr_chain: ChainId,
        chainspec: Option<ChainSpec>,
    ) -> Result<(), String>;
}

pub struct OperationCatalogEntry {
    pub name_matches: fn(name: &str) -> bool,
    pub create: fn(
        ctx: &ContextOptions,
        argname: String,
        label: Option<String>,
        value: Option<BString>,
        curr_chain: ChainId,
        chainspec: Option<ChainSpec>,
    ) -> Result<(), String>,
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
