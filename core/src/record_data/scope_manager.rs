pub enum Symbol {
    Atom(StreamValueId),
    Field(FieldId),
    Macro(Arc<Macro>),
}

pub struct ScopeEntry {
    previous_mapping: Option<Symbol>,
    value: Symbol,
}

pub struct Scope {
    pub parent: Option<ScopeId>,
    pub symbols: HashMap<StringStoreEntry, Symbol, BuildIdentityHasher>,
}

pub struct ScopeManager {
    pub scopes: Universe<ScopeId, Scope>,
}
