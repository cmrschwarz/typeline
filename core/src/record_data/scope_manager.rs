use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

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

#[derive(Default, Clone)]
pub struct ValueCell {
    pub field: Option<FieldId>,
    pub macro_def: Option<Arc<Macro>>,
    pub atom: Option<Arc<Atom>>,
}

#[derive(Clone, Default)]
pub struct Scope {
    pub parent: Option<ScopeId>,
    pub values: HashMap<StringStoreEntry, ValueCell, BuildIdentityHasher>,
}

pub struct Atom {
    pub value: RwLock<FieldValue>,
}

#[derive(Clone)]
pub struct ScopeManager {
    pub scopes: Universe<ScopeId, Scope>,
}

impl Default for ScopeManager {
    fn default() -> Self {
        let mut scopes = Universe::default();
        scopes.claim_with_value(Scope::default());
        Self { scopes }
    }
}

impl Atom {
    pub fn new(value: FieldValue) -> Self {
        Atom {
            value: RwLock::new(value),
        }
    }
}

impl ScopeManager {
    pub fn lookup_value_cell<'a, T: 'a>(
        &'a self,
        mut scope_id: ScopeId,
        name: StringStoreEntry,
        mut val_access: impl FnMut(&'a ValueCell) -> Option<T>,
    ) -> Option<T> {
        loop {
            let scope = &self.scopes[scope_id];
            if let Some(value_cell) = scope.values.get(&name) {
                if let Some(res) = val_access(value_cell) {
                    return Some(res);
                }
            }
            scope_id = scope.parent?;
        }
    }

    pub fn lookup_value_cell_mut<T>(
        &mut self,
        mut scope_id: ScopeId,
        name: StringStoreEntry,
        mut val_access: impl FnMut(&mut ValueCell) -> Option<T>,
    ) -> Option<T> {
        loop {
            let scope = &mut self.scopes[scope_id];
            if let Some(value_cell) = scope.values.get_mut(&name) {
                if let Some(res) = val_access(value_cell) {
                    return Some(res);
                }
            }
            scope_id = scope.parent?;
        }
    }

    pub fn insert_value_cell(
        &mut self,
        scope_id: ScopeId,
        name: StringStoreEntry,
    ) -> &mut ValueCell {
        self.scopes[scope_id].values.entry(name).or_default()
    }

    pub fn insert_field_name(
        &mut self,
        scope_id: ScopeId,
        name: StringStoreEntry,
        field_id: FieldId,
    ) {
        self.insert_value_cell(scope_id, name).field = Some(field_id);
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
    pub fn insert_macro(
        &mut self,
        scope_id: ScopeId,
        name: StringStoreEntry,
        macro_def: Arc<Macro>,
    ) {
        self.insert_value_cell(scope_id, name).macro_def = Some(macro_def);
    }
    pub fn insert_atom(
        &mut self,
        scope_id: ScopeId,
        name: StringStoreEntry,
        atom: Arc<Atom>,
    ) {
        self.insert_value_cell(scope_id, name).atom = Some(atom);
    }
    pub fn add_scope(&mut self, parent: Option<ScopeId>) -> ScopeId {
        self.scopes.claim_with_value(Scope {
            parent,
            ..Scope::default()
        })
    }

    pub fn lookup_field(
        &self,
        scope_id: ScopeId,
        name: StringStoreEntry,
    ) -> Option<u32> {
        self.lookup_value_cell(scope_id, name, |v| v.field)
    }

    pub fn lookup_atom(
        &self,
        scope_id: ScopeId,
        name: StringStoreEntry,
    ) -> Option<Arc<Atom>> {
        self.lookup_value_cell(scope_id, name, |v| v.atom.clone())
    }
}
