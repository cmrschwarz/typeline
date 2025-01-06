use crate::{
    index_newtype,
    operators::operator::{OffsetInChain, OperatorId},
    record_data::scope_manager::ScopeId,
    utils::{index_vec::IndexVec, string_store::StringStoreEntry},
};

index_newtype! {
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
