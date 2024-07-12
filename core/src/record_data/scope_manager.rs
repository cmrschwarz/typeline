use std::{collections::HashMap, sync::Arc};

use crate::{
    index_newtype,
    operators::macro_def::Macro,
    utils::{
        debuggable_nonmax::DebuggableNonMaxU32,
        identity_hasher::BuildIdentityHasher, indexing_type::IndexingType,
        string_store::StringStoreEntry, universe::Universe,
    },
};

use super::{field::FieldId, field_value::FieldValue};

index_newtype! {
    pub struct ScopeId(DebuggableNonMaxU32);
}

pub const DEFAULT_SCOPE_ID: ScopeId = ScopeId::ZERO;

#[derive(Clone)]
pub enum Symbol {
    Atom(FieldValue),
    Field(FieldId),
    Macro(Arc<Macro>),
}

#[derive(Clone)]
pub struct Scope {
    pub parent: Option<ScopeId>,
    pub symbols: HashMap<StringStoreEntry, Symbol, BuildIdentityHasher>,
}
impl Scope {
    pub fn insert_symbol(&mut self, name: StringStoreEntry, symbol: Symbol) {
        self.symbols.insert(name, symbol);
    }
}

#[derive(Clone)]
pub struct ScopeManager {
    pub scopes: Universe<ScopeId, Scope>,
}

impl Default for ScopeManager {
    fn default() -> Self {
        let mut scopes = Universe::default();
        scopes.claim_with_value(Scope {
            parent: None,
            symbols: HashMap::default(),
        });
        Self { scopes }
    }
}

impl Symbol {
    pub fn kind_str(&self) -> &'static str {
        match self {
            Symbol::Atom(_) => "atom",
            Symbol::Field(_) => "field",
            Symbol::Macro(_) => "macro",
        }
    }
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
    pub fn insert_field_name(
        &mut self,
        scope_id: ScopeId,
        name: StringStoreEntry,
        field_id: FieldId,
    ) {
        self.scopes[scope_id].insert_symbol(name, Symbol::Field(field_id));
    }
    pub fn insert_field_name_opt(
        &mut self,
        scope_id: ScopeId,
        name: Option<StringStoreEntry>,
        field_id: FieldId,
    ) {
        if let Some(name) = name {
            self.insert_field_name(scope_id, name, field_id)
        }
    }
    pub fn add_scope(&mut self, parent: Option<ScopeId>) -> ScopeId {
        self.scopes.claim_with_value(Scope {
            parent,
            symbols: HashMap::default(),
        })
    }

    pub fn lookup_symbol(
        &self,
        mut scope_id: ScopeId,
        name: StringStoreEntry,
    ) -> Option<&Symbol> {
        loop {
            let scope = &self.scopes[scope_id];
            if let Some(sym) = scope.symbols.get(&name) {
                return Some(sym);
            }
            scope_id = scope.parent?;
        }
    }

    pub fn lookup_field(
        &self,
        scope_id: ScopeId,
        name: StringStoreEntry,
    ) -> Option<u32> {
        let Some(Symbol::Field(field_id)) = self.lookup_symbol(scope_id, name)
        else {
            return None;
        };
        Some(*field_id)
    }
}
