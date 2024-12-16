use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use crate::{
    cli::call_expr::Argument,
    index_newtype,
    operators::operator::Operator,
    options::session_setup::SessionSetupData,
    scr_error::ScrError,
    utils::{
        debuggable_nonmax::DebuggableNonMaxU32,
        identity_hasher::BuildIdentityHasher, indexing_type::IndexingType,
        string_store::StringStoreEntry, text_write::MaybeTextWrite,
        universe::Universe,
    },
};

use super::{
    field::FieldId,
    field_value::FieldValue,
    formattable::{Formattable, FormattingContext},
};

index_newtype! {
    pub struct ScopeId(DebuggableNonMaxU32);
}

pub const DEFAULT_SCOPE_ID: ScopeId = ScopeId::ZERO;

#[derive(Clone)]
pub enum ScopeValue {
    Field(FieldId),
    Atom(Arc<Atom>),
    OpDecl(OpDeclRef),
}

#[derive(Clone, Default)]
pub struct Scope {
    pub parent: Option<ScopeId>,
    pub values: HashMap<StringStoreEntry, ScopeValue, BuildIdentityHasher>,
}

pub trait OperatorDeclaration: Send + Sync {
    fn name_stored(&self) -> &str;
    fn name_interned(&self) -> StringStoreEntry;
    fn instantiate(
        &self,
        sess: &mut SessionSetupData,
        arg: Argument,
    ) -> Result<Box<dyn Operator>, ScrError>;
    fn format<'a, 'b>(
        &self,
        _ctx: &mut FormattingContext,
        w: &mut dyn MaybeTextWrite,
    ) -> std::io::Result<()> {
        w.write_fmt(format_args!("<Op Decl `{}`>", self.name_stored()))
    }
}

#[derive(Clone, derive_more::Deref, derive_more::DerefMut)]
pub struct OpDeclRef(pub Arc<dyn OperatorDeclaration>);

impl std::fmt::Debug for OpDeclRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("<Op Decl `{}`>", self.name_stored()))
    }
}

impl<'a, 'b: 'a> Formattable<'a, 'b> for OpDeclRef {
    type Context = FormattingContext<'a, 'b>;

    fn format<W: MaybeTextWrite + ?Sized>(
        &self,
        ctx: &mut Self::Context,
        w: &mut W,
    ) -> std::io::Result<()> {
        self.0.format(ctx, w.deref_dyn())
    }
}

pub struct Atom {
    pub value: RwLock<FieldValue>,
}

#[derive(Clone)]
pub struct ScopeManager {
    pub scopes: Universe<ScopeId, Scope>,
}

impl PartialEq for OpDeclRef {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::addr_eq(&self.0, &other.0)
    }
}

impl ScopeValue {
    pub fn field_id(&self) -> Option<FieldId> {
        match self {
            ScopeValue::Field(field_id) => Some(*field_id),
            ScopeValue::OpDecl(_) | ScopeValue::Atom(_) => None,
        }
    }
    pub fn op_decl(&self) -> Option<&OpDeclRef> {
        match self {
            ScopeValue::OpDecl(m) => Some(m),
            ScopeValue::Field(_) | ScopeValue::Atom(_) => None,
        }
    }
    pub fn atom(&self) -> Option<&Arc<Atom>> {
        match self {
            ScopeValue::Atom(a) => Some(a),
            ScopeValue::Field(_) | ScopeValue::OpDecl(_) => None,
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
    pub fn insert_op_decl(
        &mut self,
        scope_id: ScopeId,
        name: StringStoreEntry,
        op_decl: OpDeclRef,
    ) {
        self.insert_value(scope_id, name, ScopeValue::OpDecl(op_decl));
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

    pub fn lookup_op_decl(
        &self,
        scope_id: ScopeId,
        name: StringStoreEntry,
    ) -> Option<OpDeclRef> {
        self.visit_value(scope_id, name, |v| v.op_decl().cloned())
    }
}
