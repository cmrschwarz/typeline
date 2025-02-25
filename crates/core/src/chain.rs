use indexland::{idx_newtype, index_vec::IndexVec};

use crate::{
    operators::operator::{OffsetInChain, OperatorId},
    record_data::scope_manager::ScopeId,
    utils::string_store::StringStoreEntry,
};

idx_newtype! {
    pub struct ChainId (pub(crate) u32);
    pub struct SubchainIndex(pub(crate) u32);
}

#[derive(Clone)]
pub struct Chain {
    pub label: Option<StringStoreEntry>,
    pub operators: IndexVec<OffsetInChain, OperatorId>,
    pub subchains: IndexVec<SubchainIndex, ChainId>,
    pub scope_id: ScopeId,
    pub parent: Option<ChainId>,
    pub subchain_idx: Option<SubchainIndex>,
}
