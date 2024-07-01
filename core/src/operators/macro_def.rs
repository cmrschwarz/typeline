use std::sync::Arc;

use crate::{
    chain::ChainId,
    cli::{
        call_expr::{ArgumentValue, CallExpr},
        call_expr_iter::CallExprIter,
        parse_operator_data,
    },
    context::SessionSetupData,
    job::Job,
    options::{
        operator_base_options::{
            OperatorBaseOptions, OperatorBaseOptionsInterned,
        },
        session_options::SessionOptions,
    },
    utils::{cow_to_str, string_store::StringStoreEntry},
};

use super::{
    errors::{OperatorCreationError, OperatorSetupError},
    nop::create_op_nop,
    operator::{
        OperatorData, OperatorDataId, OperatorId, OperatorInstantiation,
        OperatorOffsetInChain, PreboundOutputsMap,
    },
};

pub struct Macro {
    pub name: StringStoreEntry,
    pub operations: Vec<(OperatorBaseOptions<'static>, OperatorData)>,
}

pub struct OpMacroDef {
    pub name: String,
    pub operations: Vec<(OperatorBaseOptions<'static>, OperatorData)>,
    pub macro_def: Option<Arc<Macro>>,
}

pub fn setup_op_macro(
    op: &mut OpMacroDef,
    sess: &mut SessionSetupData,
    chain_id: ChainId,
    offset_in_chain: OperatorOffsetInChain,
    opts_interned: OperatorBaseOptionsInterned,
    op_data_id: OperatorDataId,
) -> Result<OperatorId, OperatorSetupError> {
    let op_id = sess.add_op_from_offset_in_chain(
        chain_id,
        offset_in_chain,
        opts_interned,
        op_data_id,
    );
    op.macro_def = Some(Arc::new(Macro {
        name: sess.string_store.intern_moved(std::mem::take(&mut op.name)),
        operations: std::mem::take(&mut op.operations),
    }));

    Ok(op_id)
}

pub fn insert_tf_macro_def(
    _job: &mut Job,
    _op: &OpMacroDef,
    _chain_id: ChainId,
    _op_id: OperatorId,
    _prebound_outputs: &PreboundOutputsMap,
) -> OperatorInstantiation {
    todo!()
}

pub fn create_op_macro_with_opts(
    name: String,
    mut operations: Vec<(OperatorBaseOptions<'static>, OperatorData)>,
) -> OperatorData {
    if operations.is_empty() {
        operations.push((
            OperatorBaseOptions::from_name("nop".into()),
            create_op_nop(),
        ));
    }
    OperatorData::MacroDef(OpMacroDef {
        name,
        operations,
        macro_def: None,
    })
}
pub fn create_op_macro(
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
    create_op_macro_with_opts(name, subchain_with_opts)
}

pub fn parse_op_macro(
    sess_opts: &mut SessionOptions,
    mut expr: CallExpr,
) -> Result<OperatorData, OperatorCreationError> {
    let mut operations = Vec::new();

    let expr_span = expr.span;

    let mut args_iter = expr.args.drain(0..);
    let first_arg = args_iter.next();
    let Some(first_arg) = first_arg else {
        return Err(OperatorCreationError::new(
            "missing name argument for macro definition",
            expr_span,
        ));
    };
    first_arg
        .value
        .expect_plain(&expr.op_name, first_arg.span)?;
    let ArgumentValue::Plain(name_bytes) = first_arg.value else {
        return Err(OperatorCreationError::new(
            "macro name argument cannot be a list",
            first_arg.span,
        ));
    };
    let Ok(name) = cow_to_str(name_bytes) else {
        return Err(OperatorCreationError::new(
            "macro name must be valid utf-8",
            first_arg.span,
        ));
    };

    for expr in CallExprIter::from_args_iter(args_iter) {
        let expr = expr?;
        let op_base = expr.op_base_options_static();
        let op_data = parse_operator_data(sess_opts, expr)?;
        operations.push((op_base, op_data));
    }

    Ok(create_op_macro_with_opts(name.into_owned(), operations))
}
