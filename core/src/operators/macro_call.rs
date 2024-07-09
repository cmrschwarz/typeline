use std::sync::Arc;

use crate::{
    chain::ChainId,
    cli::{
        call_expr::{Argument, CallExpr, Span},
        parse_operator_data,
    },
    context::SessionData,
    job::Job,
    operators::operator::OffsetInAggregation,
    options::{
        context_builder::ContextBuilder, session_setup::SessionSetupData,
    },
    record_data::{field_value::FieldValue, scope_manager::Symbol},
    scr_error::ScrError,
    utils::indexing_type::IndexingType,
};

use super::{
    errors::OperatorSetupError,
    macro_def::{Macro, OpMacroDef},
    multi_op::{create_multi_op_with_span, OpMultiOp},
    operator::{
        Operator, OperatorData, OperatorDataId, OperatorId,
        OperatorInstantiation, OperatorOffsetInChain, PreboundOutputsMap,
    },
};

pub struct OpMacroCall {
    pub name: String,
    pub target: Option<Arc<Macro>>,
    pub op_multi_op: OpMultiOp,
    pub span: Span,
}

pub fn setup_op_macro_call(
    op: &mut OpMacroCall,
    sess: &mut SessionSetupData,
    op_data_id: OperatorDataId,
    chain_id: ChainId,
    offset_in_chain: OperatorOffsetInChain,
    span: Span,
) -> Result<OperatorId, ScrError> {
    let parent_scope_id = sess.chains[chain_id].scope_id;

    let macro_instaniation_scope =
        sess.scope_mgr.add_scope(Some(parent_scope_id));

    let op_id = sess.add_op(op_data_id, chain_id, offset_in_chain, span);

    let macro_name = sess.string_store.intern_cloned(&op.name);

    let macro_def =
        match sess.scope_mgr.lookup_symbol(parent_scope_id, macro_name) {
            Some(Symbol::Macro(mac)) => mac.clone(),
            Some(other) => {
                return Err(OperatorSetupError::new_s(
                    format!(
                        "symbol of type `{}` is not callable: '{}'",
                        other.kind_str(),
                        op.name
                    ),
                    op_id,
                )
                .into());
            }
            None => {
                return Err(OperatorSetupError::new_s(
                    format!("call to undeclared symbol '{}'", op.name),
                    op_id,
                )
                .into());
            }
        };

    let result_args = ContextBuilder::with_exts(sess.extensions.clone())
        .add_ops_with_spans(std::mem::take(&mut op.op_multi_op.operations))
        .run_collect()
        .map_err(|e| {
            OperatorSetupError::new_s(
                format!(
                    "error during macro instantiation '{}': {}",
                    op.name, e.contextualized_message
                ),
                op_id,
            )
        })?
        .into_iter()
        .map(|val| {
            // TODO: handle errors
            if let FieldValue::Argument(arg) = val {
                *arg
            } else {
                Argument {
                    value: val,
                    span: Span::MacroExpansion { op_id },
                    source_scope: macro_instaniation_scope,
                    end_kind: None,
                }
            }
        })
        .collect::<Vec<_>>();

    for (i, arg) in result_args.into_iter().enumerate() {
        let op_data = sess.parse_argument(arg)?;
        sess.setup_op_from_data(
            op_data,
            chain_id,
            OperatorOffsetInChain::AggregationMember(
                op_id,
                OffsetInAggregation::from_usize(i),
            ),
            span,
        )?;
    }

    op.target = Some(macro_def);

    Ok(op_id)
}

pub fn insert_tf_macro_call(
    _job: &mut Job,
    _op: &OpMacroDef,
    _op_id: OperatorId,
    _prebound_outputs: &PreboundOutputsMap,
) -> OperatorInstantiation {
    todo!()
}

pub fn macro_call_has_dynamic_outputs(
    op: &OpMacroCall,
    sess: &SessionData,
    op_id: OperatorId,
) -> bool {
    op.op_multi_op.has_dynamic_outputs(sess, op_id)
}

pub fn create_op_macro_call_raw(
    name: String,
    operations: Vec<(OperatorData, Span)>,
    span: Span,
) -> OpMacroCall {
    let OperatorData::MultiOp(op_multi_op) =
        create_multi_op_with_span(operations)
    else {
        unreachable!()
    };
    OpMacroCall {
        name,
        op_multi_op,
        target: None,
        span,
    }
}

pub fn create_op_macro_call_with_spans(
    name: String,
    operations: Vec<(OperatorData, Span)>,
) -> OperatorData {
    OperatorData::MacroCall(create_op_macro_call_raw(
        name,
        operations,
        Span::Generated,
    ))
}
pub fn create_op_macro_call(
    name: String,
    operators: impl IntoIterator<Item = OperatorData>,
) -> OperatorData {
    let subchain_with_opts = operators
        .into_iter()
        .map(|op_data| (op_data, Span::Generated))
        .collect();
    create_op_macro_call_with_spans(name, subchain_with_opts)
}

pub fn parse_op_macro_call(
    sess_opts: &mut SessionSetupData,
    mut arg: Argument,
) -> Result<OperatorData, ScrError> {
    let mut operations = Vec::new();

    let expr = CallExpr::from_argument_mut(&mut arg)?;

    for arg in expr.args {
        let span = arg.span;
        let op_data = parse_operator_data(sess_opts, std::mem::take(arg))?;
        operations.push((op_data, span));
    }

    Ok(OperatorData::MacroCall(create_op_macro_call_raw(
        expr.op_name.to_string(),
        operations,
        arg.span,
    )))
}
