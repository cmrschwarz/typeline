use std::sync::Arc;

use crate::{
    chain::ChainId,
    cli::call_expr::{Argument, CallExprEndKind, MetaInfo, Span},
    options::{
        context_builder::ContextBuilder,
        session_setup::{ScrSetupOptions, SessionSetupData},
    },
    record_data::{
        array::Array, field_data::FieldValueRepr, field_value::FieldValue,
    },
    scr_error::{ContextualizedScrError, ScrError},
    utils::indexing_type::IndexingType,
};

use super::{
    errors::OperatorSetupError,
    macro_def::MacroDeclData,
    multi_op::create_multi_op,
    operator::{
        OffsetInAggregation, OperatorDataId, OperatorId, OperatorOffsetInChain,
    },
};

pub struct OpMacroCall {
    pub decl: Arc<MacroDeclData>,
    pub arg: Argument,
    pub multi_op_op_id: OperatorId,
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

    let mut ops = Vec::new();

    for (i, arg) in arr.into_iter().enumerate() {
        let mut arg = if let FieldValue::Argument(arg) = arg {
            *arg
        } else {
            Argument {
                value: arg,
                span: Span::MacroExpansion { op_id },
                source_scope: parent_scope_id,
                meta_info: Some(MetaInfo::EndKind(
                    CallExprEndKind::SpecialBuiltin,
                )),
            }
        };

        let FieldValue::Array(arr) = &mut arg.value else {
            return Err(OperatorSetupError::new_s(
                format!(
                    "error during macro instantiation: in operator {i}: s-expr expected, macro returned {}",
                    arg.value.kind()
                ),
                op_id,
            )
            .into());
        };

        if !arr.is_empty() && arr.repr() != Some(FieldValueRepr::Argument) {
            let mut arr_argumentized = Vec::new();
            for v in std::mem::take(arr).into_iter() {
                if let FieldValue::Argument(arg) = v {
                    arr_argumentized.push(*arg);
                } else {
                    arr_argumentized.push(Argument {
                        value: v,
                        span: Span::MacroExpansion { op_id },
                        source_scope: parent_scope_id,
                        meta_info: Some(MetaInfo::EndKind(
                            CallExprEndKind::SpecialBuiltin,
                        )),
                    });
                }
            }
            *arr = Array::Argument(arr_argumentized);
        }

        // TODO: contextualize error

        ops.push(sess.parse_argument(arg)?);
    }

    let multi_op = create_multi_op(ops);

    let multi_op_data = sess.add_op_data(multi_op);

    op.multi_op_op_id = sess.add_op(
        multi_op_data,
        chain_id,
        OperatorOffsetInChain::AggregationMember(
            op_id,
            OffsetInAggregation::ZERO,
        ),
        Span::Generated,
    );

    Ok(op_id)
}
