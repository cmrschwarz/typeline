use std::sync::Arc;

use crate::{
    chain::ChainId,
    cli::call_expr::{Argument, Span},
    context::SessionData,
    job::Job,
    operators::operator::OffsetInAggregation,
    options::{
        context_builder::ContextBuilder,
        session_setup::{ScrSetupOptions, SessionSetupData},
    },
    record_data::{array::Array, field_value::FieldValue},
    scr_error::{ContextualizedScrError, ScrError},
    utils::indexing_type::IndexingType,
};

use super::{
    errors::OperatorSetupError,
    macro_def::{MacroDeclData, OpMacroDef},
    multi_op::OpMultiOp,
    operator::{
        Operator, OperatorDataId, OperatorId, OperatorInstantiation,
        OperatorOffsetInChain, PreboundOutputsMap,
    },
};

pub struct OpMacroCall {
    pub decl: Arc<MacroDeclData>,
    pub arg: Argument,
    pub op_multi_op: OpMultiOp,
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

    let map_error = |e: ContextualizedScrError| {
        OperatorSetupError::new_s(
            format!(
                "error during macro instantiation '{}': {}",
                op.decl.name_stored, e.contextualized_message
            ),
            op_id,
        )
    };

    let result_args = ContextBuilder::from_arguments(
        ScrSetupOptions {
            extensions: sess.extensions.clone(),
            deny_threading: true,
            allow_repl: false,
            start_with_stdin: false,
            print_output: false,
            add_success_updator: false,
            skip_first_cli_arg: false,
        },
        None,
        Vec::from_iter(op.decl.args.iter().cloned()),
    )
    .map_err(map_error)?
    .push_fixed_size_type(op.arg.clone(), 1)
    .add_ops_with_spans(std::mem::take(&mut op.op_multi_op.operations))
    .run_collect()
    .map_err(map_error)?;

    if result_args.len() != 1 {
        return Err(OperatorSetupError::new_s(
            format!("error during macro instantiation: single code block expected, macro returned {}", result_args.len()),
            op_id,
        )
        .into());
    }

    let arr = match result_args.into_iter().next().unwrap() {
        FieldValue::Array(arr) => arr,
        FieldValue::Argument(arg) => {
            if let FieldValue::Array(arr) = arg.value {
                arr
            } else {
                return Err(OperatorSetupError::new_s(
                    format!(
                        "error during macro instantiation: code block expected, macro returned argument[{}]",
                        arg.value.kind()
                    ),
                    op_id,
                )
                .into());
            }
        }
        arg @ _ => {
            return Err(OperatorSetupError::new_s(
                format!("error during macro instantiation: code block expected, macro returned {}", arg.kind()),
                op_id,
            )
            .into());
        }
    };
    let result_args = if arr.is_empty() {
        arr.into_cleared_vec()
    } else if let Array::Argument(arg) = arr {
        arg
    } else {
        return Err(OperatorSetupError::new_s(
                    format!("error during macro instantiation: code block expected, macro returned array[{}]", arr.repr().unwrap()),
                    op_id,
                )
                .into());
    };

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
