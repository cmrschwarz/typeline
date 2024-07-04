use crate::{
    chain::{
        BufferingMode, ChainId, ChainSettings, SubchainIndex, TextEncoding,
    },
    operators::operator::{OffsetInChainOptions, OperatorDataId},
    record_data::scope_manager::{ScopeId, DEFAULT_SCOPE_ID},
    utils::{index_vec::IndexVec, string_store::StringStoreEntry},
};

use super::setting::Setting;

#[derive(Clone, Default)]
pub struct ChainOptions {
    pub label: Option<StringStoreEntry>,
    pub default_text_encoding: Setting<TextEncoding>,
    pub prefer_parent_text_encoding: Setting<bool>,
    pub force_text_encoding: Setting<bool>,
    pub floating_point_math: Setting<bool>,
    pub print_rationals_raw: Setting<bool>,
    pub default_batch_size: Setting<usize>,
    pub stream_buffer_size: Setting<usize>,
    pub stream_size_threshold: Setting<usize>,
    pub buffering_mode: Setting<BufferingMode>,
    pub parent: ChainId,
    pub scope_id: ScopeId,
    pub subchain_count: SubchainIndex,
    pub operators: IndexVec<OffsetInChainOptions, OperatorDataId>,
}

pub const DEFAULT_CHAIN_OPTIONS: ChainOptions = ChainOptions {
    label: None,
    default_text_encoding: Setting::new_v(TextEncoding::UTF8),
    prefer_parent_text_encoding: Setting::new_v(false),
    force_text_encoding: Setting::new_v(false),
    floating_point_math: Setting::new_v(false),
    print_rationals_raw: Setting::new_v(false),
    default_batch_size: Setting::new_v(1024),
    stream_buffer_size: Setting::new_v(1024),
    scope_id: DEFAULT_SCOPE_ID,
    stream_size_threshold: Setting::new_v(1024),
    buffering_mode: Setting::new_v(BufferingMode::LineBufferStdinIfTTY),
    parent: ChainId(0),
    subchain_count: SubchainIndex(0),
    operators: IndexVec::new(),
};

impl ChainOptions {
    pub fn build_chain_settings(
        &self,
        parent: Option<&ChainSettings>,
    ) -> ChainSettings {
        ChainSettings {
            default_text_encoding: self
                .default_text_encoding
                .or_else(|| parent.map(|p| p.default_text_encoding))
                .unwrap_or(
                    DEFAULT_CHAIN_OPTIONS.default_text_encoding.unwrap(),
                ),
            prefer_parent_text_encoding: self
                .prefer_parent_text_encoding
                .or_else(|| parent.map(|p| p.prefer_parent_text_encoding))
                .unwrap_or(
                    DEFAULT_CHAIN_OPTIONS.prefer_parent_text_encoding.unwrap(),
                ),
            force_text_encoding: self
                .force_text_encoding
                .or_else(|| parent.map(|p| p.force_text_encoding))
                .unwrap_or(DEFAULT_CHAIN_OPTIONS.force_text_encoding.unwrap()),
            floating_point_math: self
                .floating_point_math
                .or_else(|| parent.map(|p| p.floating_point_math))
                .unwrap_or(DEFAULT_CHAIN_OPTIONS.floating_point_math.unwrap()),
            print_rationals_raw: self
                .print_rationals_raw
                .or_else(|| parent.map(|p| p.print_rationals_raw))
                .unwrap_or(DEFAULT_CHAIN_OPTIONS.print_rationals_raw.unwrap()),
            default_batch_size: self
                .default_batch_size
                .or_else(|| parent.map(|p| p.default_batch_size))
                .unwrap_or(DEFAULT_CHAIN_OPTIONS.default_batch_size.unwrap()),
            stream_buffer_size: self
                .stream_buffer_size
                .or_else(|| parent.map(|p| p.stream_buffer_size))
                .unwrap_or(DEFAULT_CHAIN_OPTIONS.stream_buffer_size.unwrap()),
            stream_size_threshold: self
                .stream_size_threshold
                .or_else(|| parent.map(|p| p.stream_size_threshold))
                .unwrap_or(
                    DEFAULT_CHAIN_OPTIONS.stream_size_threshold.unwrap(),
                ),
            buffering_mode: self
                .buffering_mode
                .or_else(|| parent.map(|p| p.buffering_mode))
                .unwrap_or(DEFAULT_CHAIN_OPTIONS.buffering_mode.unwrap()),
        }
    }
}
