use std::sync::Arc;

use crate::{
    chain::ChainId,
    cli::call_expr::{CallExpr, Span},
    job::JobData,
    options::session_setup::SessionSetupData,
    record_data::{
        field_value::FieldValue,
        scope_manager::{Atom, ScopeId},
    },
    typeline_error::TypelineError,
    utils::string_store::StringStoreEntry,
};

use super::{
    errors::OperatorCreationError,
    operator::{Operator, OperatorDataId, OperatorId, OperatorOffsetInChain},
};

pub struct OpAtom {
    pub key: String,
    pub key_interned: Option<StringStoreEntry>,
    pub value: FieldValue,
}

pub fn parse_op_atom(
    _sess: &mut SessionSetupData,
    expr: &mut CallExpr,
) -> Result<Box<dyn Operator>, TypelineError> {
    let op_name = expr.op_name;

    if expr.args.is_empty() {
        return Err(OperatorCreationError::new(
            "missing label argument for operator `atom`",
            expr.span,
        )
        .into());
    }

    let key_span = expr.args[0].span;

    let key = std::mem::take(&mut expr.args[0].value)
        .into_maybe_text()
        .ok_or_else(|| expr.error_positional_arg_not_plaintext(key_span))?;

    let key = key
        .into_text()
        .ok_or_else(|| expr.error_arg_invalid_utf8(op_name, key_span))?;

    let Some(value_arg) = expr.args.get_mut(1) else {
        return Err(expr.error_require_exact_positional_count(2).into());
    };

    Ok(create_op_atom(
        key,
        FieldValue::Argument(Box::new(std::mem::take(value_arg))),
    ))
}

impl Operator for OpAtom {
    fn default_name(&self) -> super::operator::OperatorName {
        "atom".into()
    }

    fn output_count(
        &self,
        _sess: &crate::context::SessionData,
        _op_id: OperatorId,
    ) -> usize {
        0
    }

    fn output_field_kind(
        &self,
        _sess: &crate::context::SessionData,
        _op_id: OperatorId,
    ) -> super::operator::OutputFieldKind {
        super::operator::OutputFieldKind::SameAsInput
    }

    fn setup(
        &mut self,
        sess: &mut SessionSetupData,
        op_data_id: OperatorDataId,
        chain_id: ChainId,
        offset_in_chain: OperatorOffsetInChain,
        span: Span,
    ) -> Result<OperatorId, TypelineError> {
        let key_interned = sess.string_store.intern_cloned(&self.key);
        self.key_interned = Some(key_interned);
        let op_id = sess.add_op(op_data_id, chain_id, offset_in_chain, span);

        sess.scope_mgr.insert_atom(
            sess.chains[chain_id].scope_id,
            key_interned,
            Arc::new(Atom::new(self.value.clone())),
        );

        Ok(op_id)
    }

    fn update_variable_liveness(
        &self,
        _sess: &crate::context::SessionData,
        _ld: &mut crate::liveness_analysis::LivenessData,
        _op_offset_after_last_write: super::operator::OffsetInChain,
        _op_id: OperatorId,
        _bb_id: crate::liveness_analysis::BasicBlockId,
        _input_field: crate::liveness_analysis::OpOutputIdx,
        output: &mut crate::liveness_analysis::OperatorLivenessOutput,
    ) {
        output.flags.may_dup_or_drop = false;
        output.flags.non_stringified_input_access = false;
        output.flags.input_accessed = false;
    }

    fn build_transforms<'a>(
        &'a self,
        _job: &mut crate::job::Job<'a>,
        _tf_state: &mut super::transform::TransformState,
        _op_id: OperatorId,
        _prebound_outputs: &super::operator::PreboundOutputsMap,
    ) -> super::operator::TransformInstatiation<'a> {
        super::operator::TransformInstatiation::None
    }

    fn as_any(&self) -> Option<&dyn std::any::Any> {
        Some(self)
    }
    fn as_any_mut(&mut self) -> Option<&mut dyn std::any::Any> {
        Some(self)
    }
}

pub fn assign_atom(atom: &OpAtom, jd: &mut JobData, scope: ScopeId) {
    jd.scope_mgr.insert_atom(
        scope,
        atom.key_interned.unwrap(),
        Arc::new(Atom::new(atom.value.clone())),
    );
}

pub fn create_op_atom(key: String, value: FieldValue) -> Box<dyn Operator> {
    Box::new(OpAtom {
        key,
        key_interned: None,
        value,
    })
}
