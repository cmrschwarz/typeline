use crate::{
    chain::{BufferingMode, Chain, ChainId, ChainSettings},
    document::TextEncoding,
    selenium::{SeleniumDownloadStrategy, SeleniumVariant},
    utils::string_store::StringStoreEntry,
};

use super::argument::Argument;

#[derive(Clone, Default)]
pub struct ChainOptions {
    pub label: Option<StringStoreEntry>,
    pub default_text_encoding: Argument<TextEncoding>,
    pub prefer_parent_text_encoding: Argument<bool>,
    pub force_text_encoding: Argument<bool>,
    pub floating_point_math: Argument<bool>,
    pub print_rationals_raw: Argument<bool>,
    pub selenium_variant: Argument<Option<SeleniumVariant>>,
    pub selenium_download_strategy: Argument<SeleniumDownloadStrategy>,
    pub default_batch_size: Argument<usize>,
    pub stream_buffer_size: Argument<usize>,
    pub stream_size_threshold: Argument<usize>,
    pub buffering_mode: Argument<BufferingMode>,
    pub parent: ChainId,
    pub subchain_count: u32,
}

pub const DEFAULT_CHAIN_OPTIONS: ChainOptions = ChainOptions {
    label: None,
    default_text_encoding: Argument::new_v(TextEncoding::UTF8),
    prefer_parent_text_encoding: Argument::new_v(false),
    force_text_encoding: Argument::new_v(false),
    floating_point_math: Argument::new_v(false),
    print_rationals_raw: Argument::new_v(false),
    selenium_variant: Argument::new_v(None),
    selenium_download_strategy: Argument::new_v(SeleniumDownloadStrategy::Scr),
    default_batch_size: Argument::new_v(1024),
    stream_buffer_size: Argument::new_v(1024),
    stream_size_threshold: Argument::new_v(1024),
    buffering_mode: Argument::new_v(BufferingMode::LineBufferStdinIfTTY),
    parent: 0,
    subchain_count: 0,
};
impl ChainOptions {
    pub fn build_chain(&self, parent: Option<&Chain>) -> Chain {
        Chain {
            label: self.label,
            settings: ChainSettings {
                default_text_encoding: self
                    .default_text_encoding
                    .or_else(|| {
                        parent.map(|p| p.settings.default_text_encoding)
                    })
                    .unwrap_or(
                        DEFAULT_CHAIN_OPTIONS.default_text_encoding.unwrap(),
                    ),
                prefer_parent_text_encoding: self
                    .prefer_parent_text_encoding
                    .or_else(|| {
                        parent.map(|p| p.settings.prefer_parent_text_encoding)
                    })
                    .unwrap_or(
                        DEFAULT_CHAIN_OPTIONS
                            .prefer_parent_text_encoding
                            .unwrap(),
                    ),
                force_text_encoding: self
                    .force_text_encoding
                    .or_else(|| parent.map(|p| p.settings.force_text_encoding))
                    .unwrap_or(
                        DEFAULT_CHAIN_OPTIONS.force_text_encoding.unwrap(),
                    ),
                floating_point_math: self
                    .floating_point_math
                    .or_else(|| parent.map(|p| p.settings.floating_point_math))
                    .unwrap_or(
                        DEFAULT_CHAIN_OPTIONS.floating_point_math.unwrap(),
                    ),
                print_rationals_raw: self
                    .print_rationals_raw
                    .or_else(|| parent.map(|p| p.settings.print_rationals_raw))
                    .unwrap_or(
                        DEFAULT_CHAIN_OPTIONS.print_rationals_raw.unwrap(),
                    ),
                selenium_download_strategy: self
                    .selenium_download_strategy
                    .or_else(|| {
                        parent.map(|p| p.settings.selenium_download_strategy)
                    })
                    .unwrap_or(
                        DEFAULT_CHAIN_OPTIONS
                            .selenium_download_strategy
                            .unwrap(),
                    ),
                default_batch_size: self
                    .default_batch_size
                    .or_else(|| parent.map(|p| p.settings.default_batch_size))
                    .unwrap_or(
                        DEFAULT_CHAIN_OPTIONS.default_batch_size.unwrap(),
                    ),
                stream_buffer_size: self
                    .stream_buffer_size
                    .or_else(|| parent.map(|p| p.settings.stream_buffer_size))
                    .unwrap_or(
                        DEFAULT_CHAIN_OPTIONS.stream_buffer_size.unwrap(),
                    ),
                stream_size_threshold: self
                    .stream_size_threshold
                    .or_else(|| {
                        parent.map(|p| p.settings.stream_size_threshold)
                    })
                    .unwrap_or(
                        DEFAULT_CHAIN_OPTIONS.stream_size_threshold.unwrap(),
                    ),
                buffering_mode: self
                    .buffering_mode
                    .or_else(|| parent.map(|p| p.settings.buffering_mode))
                    .unwrap_or(DEFAULT_CHAIN_OPTIONS.buffering_mode.unwrap()),
            },
            subchains: Vec::new(),
            operators: Vec::new(),
        }
    }
}
