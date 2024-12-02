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
        Operator, OperatorData, OperatorDataId, OperatorId,
        OperatorOffsetInChain, OutputFieldKind, PreboundOutputsMap,
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

impl Operator for OpMacroDef {
    fn default_name(&self) -> super::operator::OperatorName {
        format!("macro={}", self.macro_decl.data.name_stored).into()
    }

    fn output_count(
        &self,
        _sess: &crate::context::SessionData,
        _op_id: OperatorId,
    ) -> usize {
        0
    }

    fn has_dynamic_outputs(
        &self,
        _sess: &crate::context::SessionData,
        _op_id: OperatorId,
    ) -> bool {
        false
    }

    fn build_transforms<'a>(
        &'a self,
        _job: &mut Job<'a>,
        _tf_state: &mut super::transform::TransformState,
        _op_id: OperatorId,
        _prebound_outputs: &PreboundOutputsMap,
    ) -> super::operator::TransformInstatiation<'a> {
        super::operator::TransformInstatiation::None
    }

    fn output_field_kind(
        &self,
        _sess: &crate::context::SessionData,
        _op_id: OperatorId,
    ) -> OutputFieldKind {
        OutputFieldKind::SameAsInput
    }

    fn setup(
        &mut self,
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
            self.macro_decl.data.name_interned,
            OpDeclRef(self.macro_decl.clone()),
        );

        Ok(op_id)
    }

    fn on_liveness_computed(
        &mut self,
        _sess: &mut crate::context::SessionData,
        _ld: &crate::liveness_analysis::LivenessData,
        _op_id: OperatorId,
    ) {
    }
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

    Ok(OperatorData::from_custom(OpMacroDef {
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
