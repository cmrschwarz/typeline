use crate::{
    encoding::TextEncoding,
    operations::{OperationId, OperationRef},
    selenium::SeleniumDownloadStrategy,
};

pub type ChainId = u32;

#[derive(Clone)]
pub struct Chain {
    pub default_text_encoding: TextEncoding,
    pub prefer_parent_text_encoding: bool,
    pub force_text_encoding: bool,

    pub selenium_download_strategy: SeleniumDownloadStrategy,

    pub operations: Vec<OperationId>,

    pub subchains: Vec<ChainId>,

    pub aggregation_targets: Vec<OperationRef>,
}
