use crate::{
    index_newtype,
    operators::operator::{OffsetInChain, OperatorId},
    record_data::scope_manager::ScopeId,
    utils::{index_vec::IndexVec, string_store::StringStoreEntry},
};

index_newtype! {
    pub struct ChainId (pub(crate) u32);

    #[derive(derive_more::Add, derive_more::Sub, derive_more::AddAssign, derive_more::SubAssign)]
    pub struct SubchainIndex(pub(crate) u32);
}

#[derive(Clone, Copy, Default)]
pub enum BufferingMode {
    BlockBuffer,
    LineBuffer,
    LineBufferStdin,
    #[default]
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

#[derive(Clone, Default)] // TODO: kill these
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
    pub operators: IndexVec<OffsetInChain, OperatorId>,
    pub subchains: IndexVec<SubchainIndex, ChainId>,
    pub scope_id: ScopeId,
    pub parent: Option<ChainId>,
    pub subchain_idx: Option<SubchainIndex>,
}
