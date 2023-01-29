use crate::options::context_options::ContextOptions;

use super::{
    operation::{Operation, OperationCreationError, OperationParameters},
    parent::OpParent,
    print::OpPrint,
    regex::OpRegex,
};

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

pub const BUILTIN_OPERATIONS_CATALOG: &[OperationCatalogEntry] = &[
    create_catalog_entry::<OpParent>(),
    create_catalog_entry::<OpPrint>(),
    create_catalog_entry::<OpRegex>(),
];
