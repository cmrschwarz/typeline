use std::sync::Arc;

use crate::{
    chain::ChainId,
    cli::call_expr::{Argument, Span},
    job::Job,
    options::session_setup::SessionSetupData,
    record_data::{
        formattable::Formattable,
        scope_manager::{OpDeclRef, OperatorDeclaration},
    },
    scr_error::ScrError,
    utils::{
        escaped_writer::EscapedWriter, indexing_type::IndexingType,
        string_store::StringStoreEntry, text_write::TextWrite,
    },
};

use super::{
    errors::OperatorCreationError,
    macro_call::OpMacroCall,
    operator::{
        OperatorData, OperatorDataId, OperatorId, OperatorInstantiation,
        OperatorOffsetInChain, PreboundOutputsMap,
    },
};

pub struct MacroDeclData {
    pub name_stored: String,
    pub name_interned: StringStoreEntry,
    pub args: Arc<[Argument]>,
    pub span: Span,
}

pub struct MacroOpDecl {
    data: Arc<MacroDeclData>,
}

pub struct OpMacroDef {
    pub macro_decl: Arc<MacroOpDecl>,
}

impl OperatorDeclaration for MacroOpDecl {
    fn name_interned(&self) -> StringStoreEntry {
        self.data.name_interned
    }
    fn name_stored(&self) -> &str {
        &self.data.name_stored
    }

    fn instantiate(
        &self,
        _sess: &mut SessionSetupData,
        arg: Argument,
    ) -> Result<OperatorData, ScrError> {
        Ok(OperatorData::MacroCall(OpMacroCall {
            decl: self.data.clone(),
            arg,
            multi_op_op_id: OperatorId::MAX_VALUE,
        }))
    }

    fn format<'a, 'b>(
        &self,
        ctx: &mut crate::record_data::formattable::FormattingContext,
        w: &mut dyn crate::utils::text_write::MaybeTextWrite,
    ) -> std::io::Result<()> {
        w.write_all_text("[ \"")?;
        let mut ew = EscapedWriter::new(w.as_text_write(), b'\"');
        ew.write_all_text(self.name_stored())?;
        drop(ew);
        w.write_all_text("\", [")?;
        for (i, arg) in self.data.args.iter().enumerate() {
            if i != 0 {
                w.write_all_text(", ")?;
            }
            arg.format(ctx, w)?
        }
        w.write_all_text("] ]")?;
        Ok(())
    }
}

pub fn setup_op_macro_def(
    op: &mut OpMacroDef,
    sess: &mut SessionSetupData,
    op_data_id: OperatorDataId,
    chain_id: ChainId,
    offset_in_chain: OperatorOffsetInChain,
    span: Span,
) -> Result<OperatorId, ScrError> {
    let op_id = sess.add_op(op_data_id, chain_id, offset_in_chain, span);

    let current_chain = sess.get_current_chain();
    sess.scope_mgr.insert_op_decl(
        sess.chains[current_chain].scope_id,
        op.macro_decl.data.name_interned,
        OpDeclRef(op.macro_decl.clone()),
    );

    Ok(op_id)
}

pub fn insert_tf_macro_def(
    _job: &mut Job,
    _op: &OpMacroDef,
    _op_id: OperatorId,
    _prebound_outputs: &PreboundOutputsMap,
) -> OperatorInstantiation {
    todo!()
}

pub fn parse_op_macro_def(
    sess_opts: &mut SessionSetupData,
    mut arg: Argument,
) -> Result<OperatorData, ScrError> {
    let span = arg.span;

    let mut args = std::mem::take(arg.expect_arg_array_mut()?).into_iter();

    let _ = args.next();

    let Some(name) = args.next() else {
        return Err(OperatorCreationError::new(
            "missing name argument for macro definition",
            arg.span,
        )
        .into());
    };
    let name = name.try_into_str("macro", sess_opts)?;

    Ok(OperatorData::MacroDef(OpMacroDef {
        macro_decl: Arc::new(MacroOpDecl {
            data: Arc::new(MacroDeclData {
                name_interned: sess_opts.string_store.intern_cloned(&name),
                name_stored: name,
                args: Arc::from_iter(args),
                span,
            }),
        }),
    }))
}
