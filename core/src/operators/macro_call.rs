use std::sync::Arc;

use crate::{
    chain::ChainId,
    cli::{
        call_expr::{Argument, CallExpr, Span},
        call_expr_iter::CallExprIter,
        parse_operator_data,
    },
    context::{SessionData, SessionSetupData},
    job::Job,
    options::{
        context_builder::ContextBuilder,
        operator_base_options::{
            OperatorBaseOptions, OperatorBaseOptionsInterned,
        },
        session_options::SessionOptions,
    },
    record_data::{field_value::FieldValue, scope_manager::Symbol},
};

use super::{
    errors::{OperatorCreationError, OperatorSetupError},
    macro_def::{Macro, OpMacroDef},
    multi_op::{create_multi_op_with_opts, OpMultiOp},
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
    chain_id: ChainId,
    offset_in_chain: OperatorOffsetInChain,
    opts_interned: OperatorBaseOptionsInterned,
    op_data_id: OperatorDataId,
) -> Result<OperatorId, OperatorSetupError> {
    let parent_scope_id = sess.chains[chain_id].scope_id;

    let macro_instaniation_scope =
        sess.scope_mgr.add_scope(Some(parent_scope_id));

    let op_id = sess.add_op_from_offset_in_chain(
        chain_id,
        offset_in_chain,
        opts_interned,
        op_data_id,
    );

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
                ));
            }
            None => {
                return Err(OperatorSetupError::new_s(
                    format!("call to undeclared symbol '{}'", op.name),
                    op_id,
                ));
            }
        };

    let mut ctx = ContextBuilder::with_exts(sess.extensions.clone());

    ctx.ref_add_ops_with_opts(std::mem::take(&mut op.op_multi_op.operations));

    let result_args = ctx
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
                }
            }
        })
        .collect::<Vec<_>>();

    for expr in CallExprIter::from_args_iter(result_args) {
        let expr = expr.map_err(|e| {
            OperatorSetupError::new_s(
                format!(
                    "error in expanded macro '{}': {}",
                    op.name, e.message
                ),
                op_id,
            )
        })?;
        let _op_base = expr.op_base_options();
        todo!()
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
    operations: Vec<(OperatorBaseOptions, OperatorData)>,
    span: Span,
) -> OpMacroCall {
    let OperatorData::MultiOp(op_multi_op) =
        create_multi_op_with_opts(operations)
    else {
        unreachable!()
    };
    OpMacroCall {
        op_multi_op,
        name,
        target: None,
        span,
    }
}

pub fn create_op_macro_call_with_opts(
    name: String,
    operations: Vec<(OperatorBaseOptions, OperatorData)>,
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
        .map(|op_data| {
            (
                OperatorBaseOptions::from_name(op_data.default_op_name()),
                op_data,
            )
        })
        .collect();
    create_op_macro_call_with_opts(name, subchain_with_opts)
}

pub fn parse_op_macro_call(
    sess_opts: &mut SessionOptions,
    expr: CallExpr,
) -> Result<OperatorData, OperatorCreationError> {
    let mut operations = Vec::new();

    let name = expr.op_name;

    for expr in CallExprIter::from_args_iter(expr.args) {
        let expr = expr?;
        let op_base = expr.op_base_options();
        let op_data = parse_operator_data(sess_opts, expr)?;
        operations.push((op_base, op_data));
    }

    Ok(OperatorData::MacroCall(create_op_macro_call_raw(
        name, operations, expr.span,
    )))
}
