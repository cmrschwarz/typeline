use crate::{
    document::TextEncoding, operations::operator::OperatorId, selenium::SeleniumDownloadStrategy,
};

pub type ChainId = u32;

#[derive(Clone, Copy)]
pub enum BufferingMode {
    BlockBuffer,
    LineBuffer,
    LineBufferStdin,
    LineBufferStdinIfTTY,
}

#[derive(Clone)]
pub struct ChainSettings {
    pub default_text_encoding: TextEncoding,
    pub prefer_parent_text_encoding: bool,
    pub force_text_encoding: bool,
    pub selenium_download_strategy: SeleniumDownloadStrategy,
    pub default_batch_size: usize,
    pub buffering_mode: BufferingMode,
}

#[derive(Clone)]
pub struct Chain {
    pub settings: ChainSettings,

    pub operations: Vec<OperatorId>,

    pub subchains: Vec<ChainId>,
}
