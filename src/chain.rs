use crate::{
    encoding::TextEncoding, operations::operator_base::OperatorId,
    selenium::SeleniumDownloadStrategy,
};

pub type ChainId = u32;

#[derive(Clone)]
pub struct ChainSettings {
    pub default_text_encoding: TextEncoding,
    pub prefer_parent_text_encoding: bool,
    pub force_text_encoding: bool,
    pub selenium_download_strategy: SeleniumDownloadStrategy,
    pub default_batch_size: usize,
}

#[derive(Clone)]
pub struct Chain {
    pub settings: ChainSettings,

    pub operations: Vec<OperatorId>,

    pub subchains: Vec<ChainId>,
}
