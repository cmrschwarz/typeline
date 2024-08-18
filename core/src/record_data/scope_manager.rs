use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use crate::{
    index_newtype,
    operators::macro_def::MacroRef,
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
pub enum ScopeValue {
    Field(FieldId),
    Macro(MacroRef),
    Atom(Arc<Atom>),
}

#[derive(Clone, Default)]
pub struct Scope {
    pub parent: Option<ScopeId>,
    pub values: HashMap<StringStoreEntry, ScopeValue, BuildIdentityHasher>,
}

pub struct Atom {
    pub value: RwLock<FieldValue>,
}

#[derive(Clone)]
pub struct ScopeManager {
    pub scopes: Universe<ScopeId, Scope>,
}

impl ScopeValue {
    pub fn field_id(&self) -> Option<FieldId> {
        match self {
            ScopeValue::Field(field_id) => Some(*field_id),
            ScopeValue::Macro(_) | ScopeValue::Atom(_) => None,
        }
    }
    pub fn macro_ref(&self) -> Option<&MacroRef> {
        match self {
            ScopeValue::Macro(m) => Some(m),
            ScopeValue::Field(_) | ScopeValue::Atom(_) => None,
        }
    }
    pub fn atom(&self) -> Option<&Arc<Atom>> {
        match self {
            ScopeValue::Atom(a) => Some(a),
            ScopeValue::Field(_) | ScopeValue::Macro(_) => None,
        }
    }
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
    pub fn lookup_value(
        &self,
        mut scope_id: ScopeId,
        name: StringStoreEntry,
    ) -> Option<&ScopeValue> {
        loop {
            let scope = &self.scopes[scope_id];
            if let Some(value) = scope.values.get(&name) {
                return Some(value);
            }
            scope_id = scope.parent?;
        }
    }

    pub fn visit_value<'a, T: 'a>(
        &'a self,
        mut scope_id: ScopeId,
        name: StringStoreEntry,
        mut val_access: impl FnMut(&'a ScopeValue) -> Option<T>,
    ) -> Option<T> {
        loop {
            let scope = &self.scopes[scope_id];
            if let Some(value) = scope.values.get(&name) {
                if let Some(res) = val_access(value) {
                    return Some(res);
                }
            }
            scope_id = scope.parent?;
        }
    }

    pub fn visit_value_mut<T>(
        &mut self,
        mut scope_id: ScopeId,
        name: StringStoreEntry,
        mut val_access: impl FnMut(&mut ScopeValue) -> Option<T>,
    ) -> Option<T> {
        loop {
            let scope = &mut self.scopes[scope_id];
            if let Some(value) = scope.values.get_mut(&name) {
                if let Some(res) = val_access(value) {
                    return Some(res);
                }
            }
            scope_id = scope.parent?;
        }
    }

    pub fn insert_value(
        &mut self,
        scope_id: ScopeId,
        name: StringStoreEntry,
        value: ScopeValue,
    ) {
        self.scopes[scope_id].values.insert(name, value);
    }

    pub fn insert_field_name(
        &mut self,
        scope_id: ScopeId,
        name: StringStoreEntry,
        field_id: FieldId,
    ) {
        self.insert_value(scope_id, name, ScopeValue::Field(field_id))
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
        macro_def: MacroRef,
    ) {
        self.insert_value(scope_id, name, ScopeValue::Macro(macro_def));
    }
    pub fn insert_atom(
        &mut self,
        scope_id: ScopeId,
        name: StringStoreEntry,
        atom: Arc<Atom>,
    ) {
        self.insert_value(scope_id, name, ScopeValue::Atom(atom));
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
    ) -> Option<FieldId> {
        self.visit_value(scope_id, name, ScopeValue::field_id)
    }

    pub fn lookup_atom(
        &self,
        scope_id: ScopeId,
        name: StringStoreEntry,
    ) -> Option<Arc<Atom>> {
        self.visit_value(scope_id, name, |v| v.atom().cloned())
    }

    pub fn lookup_macro(
        &self,
        scope_id: ScopeId,
        name: StringStoreEntry,
    ) -> Option<MacroRef> {
        self.visit_value(scope_id, name, |v| v.macro_ref().cloned())
    }
}
