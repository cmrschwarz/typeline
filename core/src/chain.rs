use crate::{
    operators::operator::{OperatorId, OperatorOffsetInChain},
    utils::{index_vec::IndexVec, string_store::StringStoreEntry},
};

pub type ChainId = u32;
pub type SubchainIndex = u32;

#[derive(Clone, Copy)]
pub enum BufferingMode {
    BlockBuffer,
    LineBuffer,
    LineBufferStdin,
    LineBufferIfTTY,
    LineBufferStdinIfTTY,
}

#[derive(Default, Clone, Copy)]
pub enum TextEncoding {
    #[default]
    UTF8,
    UTF16,
    UTF32,
    ASCII,
}

#[derive(Clone)]
pub struct ChainSettings {
    pub default_text_encoding: TextEncoding,
    pub prefer_parent_text_encoding: bool,
    pub force_text_encoding: bool,
    pub floating_point_math: bool,
    pub print_rationals_raw: bool,
    pub default_batch_size: usize,
    pub stream_buffer_size: usize,
    pub stream_size_threshold: usize,
    pub buffering_mode: BufferingMode,
}

#[derive(Clone)]
pub struct Chain {
    pub label: Option<StringStoreEntry>,
    pub settings: ChainSettings,
    pub operators: IndexVec<OperatorOffsetInChain, OperatorId>,
    pub subchains: IndexVec<SubchainIndex, ChainId>,
}
