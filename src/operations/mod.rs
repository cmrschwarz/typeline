pub mod html;
pub mod parent;
pub mod print;
pub mod read_stdin;
pub mod start;

use bstring::BString;

use crate::chain::ChainId;
use crate::options::{ChainSpec, ContextOptions};
use crate::transform::Transform;

use self::parent::OpParent;

pub type OperationId = u32;

pub struct OperationRef {
    pub cn_id: ChainId,
    pub tf_id: OperationId,
}

impl OperationRef {
    pub fn new(cn_id: ChainId, tf_id: OperationId) -> Self {
        Self { cn_id, tf_id }
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

pub trait Operation: OperationCloneBox + Send + Sync {
    fn apply<'a: 'b, 'b>(
        &'a mut self,
        tf_stack: &'b mut [&'a mut dyn Transform],
    ) -> &'b mut dyn Transform;
}

pub struct OperationBase {
    label: String,
}

pub trait OperationCatalogMember: Operation {
    fn name_matches(name: &str) -> bool;
    fn create(
        ctx: &ContextOptions,
        label: String,
        value: Option<BString>,
        curr_chain: ChainId,
        chainspec: Option<ChainSpec>,
    ) -> Result<(), String>;
}

pub struct OperationCatalogEntry {
    pub name_matches: fn(name: &str) -> bool,
    pub create: fn(
        ctx: &ContextOptions,
        label: String,
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

pub const BUILTIN_OPERATIONS_CATALOG: [OperationCatalogEntry; 1] =
    [create_catalog_entry::<OpParent>()];
