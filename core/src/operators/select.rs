use crate::{
    chain::ChainId,
    cli::call_expr::{CallExpr, Span},
    liveness_analysis::{OperatorCallEffect, VarId},
    options::session_setup::SessionSetupData,
    typeline_error::TypelineError,
    utils::{indexing_type::IndexingType, string_store::StringStoreEntry},
};

use super::{
    errors::OperatorCreationError,
    operator::{
        Operator, OperatorDataId, OperatorId, OperatorOffsetInChain,
        OutputFieldKind,
    },
};

#[derive(Clone)]
pub struct OpSelect {
    key: String,
    pub key_interned: Option<StringStoreEntry>,
}

pub fn parse_op_select(
    expr: &CallExpr,
) -> Result<Box<dyn Operator>, OperatorCreationError> {
    let val = expr.require_single_string_arg()?;
    Ok(Box::new(OpSelect {
        key: val.to_owned(),
        key_interned: None,
    }))
}

pub fn create_op_select(key: impl Into<String>) -> Box<dyn Operator> {
    Box::new(OpSelect {
        key: key.into(),
        key_interned: None,
    })
}

impl Operator for OpSelect {
    fn setup(
        &mut self,
        sess: &mut SessionSetupData,
        op_data_id: OperatorDataId,
        chain_id: ChainId,
        offset_in_chain: OperatorOffsetInChain,
        span: Span,
    ) -> Result<OperatorId, TypelineError> {
        self.key_interned = Some(
            sess.string_store
                .intern_moved(std::mem::take(&mut self.key)),
        );
        Ok(sess.add_op(op_data_id, chain_id, offset_in_chain, span))
    }
    fn register_output_var_names(
        &self,
        ld: &mut crate::liveness_analysis::LivenessData,
        _sess: &crate::context::SessionData,
        _op_id: OperatorId,
    ) {
        ld.add_var_name(self.key_interned.unwrap());
    }
    fn update_variable_liveness(
        &self,
        _sess: &crate::context::SessionData,
        ld: &mut crate::liveness_analysis::LivenessData,
        _op_offset_after_last_write: super::operator::OffsetInChain,
        _op_id: OperatorId,
        _bb_id: crate::liveness_analysis::BasicBlockId,
        _input_field: crate::liveness_analysis::OpOutputIdx,
        output: &mut crate::liveness_analysis::OperatorLivenessOutput,
    ) {
        let mut var = ld.var_names[&self.key_interned.unwrap()];
        // resolve rebinds
        loop {
            let field = ld.vars_to_op_outputs_map[var];
            if field.into_usize() >= ld.vars.len() {
                break;
            }
            // var points to itself
            if field.into_usize() == var.into_usize() {
                break;
            }
            // OpOutput indices below vars.len() are the vars
            var = VarId::from_usize(field.into_usize());
        }
        output.primary_output = var.natural_output_idx();
        output.flags.input_accessed = false;
        output.call_effect = OperatorCallEffect::NoCall;
    }

    fn output_field_kind(
        &self,
        _sess: &crate::context::SessionData,
        _op_id: OperatorId,
    ) -> OutputFieldKind {
        OutputFieldKind::Unconfigured
    }

    fn default_name(&self) -> super::operator::OperatorName {
        "select".into()
    }

    fn output_count(
        &self,
        _sess: &crate::context::SessionData,
        _op_id: OperatorId,
    ) -> usize {
        0
    }

    fn build_transforms<'a>(
        &'a self,
        _job: &mut crate::job::Job<'a>,
        _tf_state: &mut super::transform::TransformState,
        _op_id: OperatorId,
        _prebound_outputs: &super::operator::PreboundOutputsMap,
    ) -> super::operator::TransformInstatiation<'a> {
        unreachable!()
    }

    fn as_any(&self) -> Option<&dyn std::any::Any> {
        Some(self)
    }
    fn as_any_mut(&mut self) -> Option<&mut dyn std::any::Any> {
        Some(self)
    }
}
