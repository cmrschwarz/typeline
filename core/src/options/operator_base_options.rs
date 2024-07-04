use crate::{
    cli::call_expr::Span,
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
    pub argname: String,
    pub label: Option<String>,
    pub span: Span,
    pub transparent_mode: bool,
    pub append_mode: bool,
    pub output_is_atom: bool,
}

impl OperatorBaseOptions {
    pub fn new(
        argname: String,
        label: Option<String>,
        append_mode: bool,
        transparent_mode: bool,
        output_is_atom: bool,
        span: Span,
    ) -> OperatorBaseOptions {
        OperatorBaseOptions {
            argname,
            label,
            span,
            transparent_mode,
            append_mode,
            output_is_atom,
        }
    }
    pub fn from_name(argname: impl Into<String>) -> OperatorBaseOptions {
        OperatorBaseOptions::new(
            argname.into(),
            None,
            false,
            false,
            false,
            Span::Generated,
        )
    }
    pub fn intern(
        self,
        string_store: &mut StringStore,
    ) -> OperatorBaseOptionsInterned {
        OperatorBaseOptionsInterned {
            argname: string_store.intern_cloned(&self.argname),
            label: self.label.map(|l| string_store.intern_cloned(&l)),
            span: self.span,
            transparent_mode: self.transparent_mode,
            append_mode: self.append_mode,
            output_is_atom: self.output_is_atom,
        }
    }
}
