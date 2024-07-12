use std::sync::Arc;

use crate::{
    chain::ChainId,
    cli::{
        call_expr::{Argument, Span},
        parse_operator_data,
    },
    job::Job,
    options::session_setup::SessionSetupData,
    scr_error::ScrError,
    utils::string_store::StringStoreEntry,
};

use super::{
    errors::OperatorCreationError,
    nop::create_op_nop,
    operator::{
        OperatorData, OperatorDataId, OperatorId, OperatorInstantiation,
        OperatorOffsetInChain, PreboundOutputsMap,
    },
};

pub struct Macro {
    pub name: StringStoreEntry,
    pub operations: Vec<(OperatorData, Span)>,
}

pub struct OpMacroDef {
    pub name: String,
    pub operations: Vec<(OperatorData, Span)>,
    pub macro_def: Option<Arc<Macro>>,
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

    let macro_name =
        sess.string_store.intern_moved(std::mem::take(&mut op.name));

    let macro_def = Arc::new(Macro {
        name: macro_name,
        operations: std::mem::take(&mut op.operations),
    });

    op.macro_def = Some(macro_def.clone());

    let scope_id = sess.chains[chain_id].scope_id;

    sess.scope_mgr.insert_macro(scope_id, macro_name, macro_def);

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

pub fn create_op_macro_def_with_opts(
    name: String,
    mut operations: Vec<(OperatorData, Span)>,
) -> OperatorData {
    if operations.is_empty() {
        operations.push((create_op_nop(), Span::Generated));
    }
    OperatorData::MacroDef(OpMacroDef {
        name,
        operations,
        macro_def: None,
    })
}
pub fn create_op_macro_def(
    name: String,
    operators: impl IntoIterator<Item = OperatorData>,
) -> OperatorData {
    let subchain_with_opts = operators
        .into_iter()
        .map(|op_data| (op_data, Span::Generated))
        .collect();
    create_op_macro_def_with_opts(name, subchain_with_opts)
}

pub fn parse_op_macro_def(
    sess_opts: &mut SessionSetupData,
    mut arg: Argument,
) -> Result<OperatorData, ScrError> {
    let mut operations = Vec::new();

    let mut args_iter = arg.expect_arg_array_mut()?.drain(1..);
    let first_arg = args_iter.next();
    let Some(first_arg) = first_arg else {
        drop(args_iter);
        return Err(OperatorCreationError::new(
            "missing name argument for macro definition",
            arg.span,
        )
        .into());
    };
    let name = first_arg.expect_string("macro")?;

    for arg in args_iter {
        let span = arg.span;
        let op_data = parse_operator_data(sess_opts, arg)?;
        operations.push((op_data, span));
    }

    Ok(create_op_macro_def_with_opts(name.into(), operations))
}
