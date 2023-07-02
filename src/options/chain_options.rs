use crate::{
    chain::{BufferingMode, Chain, ChainId, ChainSettings},
    document::TextEncoding,
    selenium::{SeleniumDownloadStrategy, SeleniumVariant},
};

use super::argument::Argument;

#[derive(Clone, Default)]
pub struct ChainOptions {
    pub default_text_encoding: Argument<TextEncoding>,
    pub prefer_parent_text_encoding: Argument<bool>,
    pub force_text_encoding: Argument<bool>,
    pub selenium_variant: Argument<Option<SeleniumVariant>>,
    pub selenium_download_strategy: Argument<SeleniumDownloadStrategy>,
    pub default_batch_size: Argument<usize>,
    pub stream_buffer_size: Argument<usize>,
    pub buffering_mode: Argument<BufferingMode>,
    pub parent: ChainId,
}
const DEFAULT_CHAIN_OPTIONS: ChainOptions = ChainOptions {
    default_text_encoding: Argument::new(TextEncoding::UTF8),
    prefer_parent_text_encoding: Argument::new(false),
    force_text_encoding: Argument::new(false),
    selenium_variant: Argument::new(None),
    selenium_download_strategy: Argument::new(SeleniumDownloadStrategy::Scr),
    default_batch_size: Argument::new(1024), //TODO: tweak me
    stream_buffer_size: Argument::new(1024),
    buffering_mode: Argument::new(BufferingMode::LineBufferStdinIfTTY),
    parent: 0,
};
impl ChainOptions {
    pub fn build_chain(&self) -> Chain {
        Chain {
            settings: ChainSettings {
                default_text_encoding: self
                    .default_text_encoding
                    .unwrap_or(DEFAULT_CHAIN_OPTIONS.default_text_encoding.unwrap()),
                prefer_parent_text_encoding: self
                    .prefer_parent_text_encoding
                    .unwrap_or(DEFAULT_CHAIN_OPTIONS.prefer_parent_text_encoding.unwrap()),
                force_text_encoding: self
                    .force_text_encoding
                    .unwrap_or(DEFAULT_CHAIN_OPTIONS.force_text_encoding.unwrap()),
                selenium_download_strategy: self
                    .selenium_download_strategy
                    .unwrap_or(DEFAULT_CHAIN_OPTIONS.selenium_download_strategy.unwrap()),
                default_batch_size: self
                    .default_batch_size
                    .unwrap_or(DEFAULT_CHAIN_OPTIONS.default_batch_size.unwrap()),
                stream_buffer_size: self
                    .default_batch_size
                    .unwrap_or(DEFAULT_CHAIN_OPTIONS.stream_buffer_size.unwrap()),
                buffering_mode: self
                    .buffering_mode
                    .unwrap_or(DEFAULT_CHAIN_OPTIONS.buffering_mode.unwrap()),
            },
            subchains: Vec::new(),
            operations: Vec::new(),
        }
    }
}
