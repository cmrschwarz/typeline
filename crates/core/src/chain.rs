use indexland::{index_vec::IndexVec, NewtypeIdx};

use crate::{
    operators::operator::{OffsetInChain, OperatorId},
    record_data::scope_manager::ScopeId,
    utils::string_store::StringStoreEntry,
};

#[derive(NewtypeIdx)]
pub struct ChainId(pub(crate) u32);

#[derive(NewtypeIdx)]
pub struct SubchainIndex(pub(crate) u32);

#[derive(Clone)]
pub struct Chain {
    pub label: Option<StringStoreEntry>,
    pub operators: IndexVec<OffsetInChain, OperatorId>,
    pub subchains: IndexVec<SubchainIndex, ChainId>,
    pub scope_id: ScopeId,
    pub parent: Option<ChainId>,
    pub subchain_idx: Option<SubchainIndex>,
}
