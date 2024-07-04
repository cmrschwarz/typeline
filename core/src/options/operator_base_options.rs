use crate::{
    cli::call_expr::Span,
    operators::operator::OperatorData,
    utils::string_store::{StringStore, StringStoreEntry},
};

#[derive(Clone, Copy)]
pub struct OperatorBaseOptionsInterned {
    pub argname: StringStoreEntry,
    pub label: Option<StringStoreEntry>,
    pub span: Span,
    pub transparent_mode: bool,
    pub append_mode: bool,
    pub output_is_atom: bool,
}

#[derive(Clone, Default)]
pub struct OperatorBaseOptions {
    pub argname: Option<String>,
    pub label: Option<String>,
    pub transparent_mode: bool,
    pub append_mode: bool,
    pub output_is_atom: bool,
    pub span: Span,
}

impl OperatorBaseOptions {
    pub fn from_name(argname: impl Into<String>) -> Self {
        Self {
            argname: Some(argname.into()),
            ..Self::default()
        }
    }
    pub fn intern(
        self,
        op_data: Option<&OperatorData>,
        string_store: &mut StringStore,
    ) -> OperatorBaseOptionsInterned {
        let op_name_interned = if let Some(argname) = &self.argname {
            string_store.intern_cloned(argname)
        } else if let Some(op_data) = op_data {
            string_store.intern_cow(op_data.default_op_name())
        } else {
            string_store.intern_static("<unnamed>")
        };

        OperatorBaseOptionsInterned {
            argname: op_name_interned,
            label: self.label.map(|l| string_store.intern_cloned(&l)),
            span: self.span,
            transparent_mode: self.transparent_mode,
            append_mode: self.append_mode,
            output_is_atom: self.output_is_atom,
        }
    }
}
