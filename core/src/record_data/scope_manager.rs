use std::{collections::HashMap, sync::Arc};

use crate::{
    index_newtype,
    operators::macro_def::Macro,
    utils::{
        debuggable_nonmax::DebuggableNonMaxU32,
        identity_hasher::BuildIdentityHasher, string_store::StringStoreEntry,
        universe::Universe,
    },
};

use super::{field::FieldId, stream_value::StreamValueId};

index_newtype! {
    pub struct ScopeId(DebuggableNonMaxU32);
}

pub enum Symbol {
    Atom(StreamValueId),
    Field(FieldId),
    Macro(Arc<Macro>),
}

pub struct Scope {
    pub parent: Option<ScopeId>,
    pub symbols: HashMap<StringStoreEntry, Symbol, BuildIdentityHasher>,
}
impl Scope {
    pub fn insert_symbol(&mut self, name: StringStoreEntry, symbol: Symbol) {
        self.symbols.insert(name, symbol);
    }
}

#[derive(Default)]
pub struct ScopeManager {
    pub scopes: Universe<ScopeId, Scope>,
}

impl ScopeManager {
    pub fn insert_symbol(
        &mut self,
        scope_id: ScopeId,
        name: StringStoreEntry,
        symbol: Symbol,
    ) {
        self.scopes[scope_id].insert_symbol(name, symbol);
    }
    pub fn add_scope(&mut self, parent: Option<ScopeId>) -> ScopeId {
        self.scopes.claim_with_value(Scope {
            parent,
            symbols: HashMap::default(),
        })
    }
}
