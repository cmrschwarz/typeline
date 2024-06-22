use std::sync::Arc;

use bstr::ByteSlice;

use crate::{
    cli::call_expr::{OperatorArg, OperatorCallExpr, ParsedArgValue},
    job::JobData,
    options::argument::CliArgIdx,
    record_data::{
        field_value_ref::FieldValueSlice,
        iter_hall::{IterId, IterKind},
        iters::FieldIterator,
        push_interface::PushInterface,
        ref_iter::{AutoDerefIter, RefAwareFieldValueRangeIter},
        stream_value::{StreamValueData, StreamValueUpdate},
    },
    utils::{
        encoding::{
            self, utf8_surrocate_escape, UTF8_REPLACEMENT_CHARACTER_BYTES,
        },
        retain_vec_range,
    },
};

use super::{
    errors::{OperatorApplicationError, OperatorCreationError},
    operator::{DefaultOperatorName, OperatorBase, OperatorData},
    transform::{TransformData, TransformId, TransformState},
};

#[derive(Clone)]
pub struct OpToStr {
    invalid_unicode_handler: Option<InvalidUnicodeHandler>,
    convert_errors: bool,
}

impl OpToStr {
    pub fn default_op_name(&self) -> DefaultOperatorName {
        "to_str".into()
    }
}

pub fn parse_op_to_str(
    expr: &OperatorCallExpr,
) -> Result<OperatorData, OperatorCreationError> {
    // this should not happen in the cli parser because it checks using
    // `argument_matches_data_inserter`
    let mut force = false;

    for arg in expr.parsed_args_iter() {
        match arg.value {
            ParsedArgValue::Flag(flag) => {
                if flag == b"f" || flag == b"force" {
                    force = true;
                } else {
                    return Err(
                        expr.error_flag_value_unsupported(flag, arg.span)
                    );
                }
            }
            ParsedArgValue::NamedArg { .. } => {
                return Err(expr.error_named_args_unsupported(arg.span))
            }
            ParsedArgValue::PositionalArg { .. } => {
                return Err(expr.error_positional_args_unsupported(arg.span))
            }
        }
    }

    Ok(create_op_to_str(None, force))
}

pub fn create_op_to_str(
    invalid_unicode_handler: Option<InvalidUnicodeHandler>,
    convert_errors: bool,
) -> OperatorData {
    OperatorData::ToStr(OpToStr {
        invalid_unicode_handler,
        convert_errors,
    })
}

pub trait InvalidUnicodeHandlerFn:
    FnMut(usize, &[u8], &mut Vec<u8>) -> Result<(), String> + Send + Sync
{
    fn clone_dyn(&self) -> Box<dyn InvalidUnicodeHandlerFn>;
}

impl<
        F: FnMut(usize, &[u8], &mut Vec<u8>) -> Result<(), String>
            + Send
            + Sync
            + Clone
            + 'static,
    > InvalidUnicodeHandlerFn for F
{
    fn clone_dyn(&self) -> Box<dyn InvalidUnicodeHandlerFn> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn InvalidUnicodeHandlerFn> {
    fn clone(&self) -> Self {
        self.clone_dyn()
    }
}

#[derive(Clone)]
pub enum InvalidUnicodeHandler {
    Lossy,
    SurrogateEscape,
    Custom(Box<dyn InvalidUnicodeHandlerFn>),
}

pub struct TfToStr {
    batch_iter: IterId,
    pending_streams: usize,
    invalid_unicode_handler: Box<dyn InvalidUnicodeHandlerFn>,
    convert_errors: bool,
}

pub fn build_tf_to_str<'a>(
    jd: &mut JobData<'a>,
    _op_base: &OperatorBase,
    op: &'a OpToStr,
    tf_state: &mut TransformState,
) -> TransformData<'a> {
    let replacement_fn: Box<dyn InvalidUnicodeHandlerFn> = match &op
        .invalid_unicode_handler
    {
        Some(h) => match h {
            InvalidUnicodeHandler::Lossy => {
                |_pos,
                 _input: &[u8],
                 out: &mut Vec<u8>|
                 -> Result<(), String> {
                    out.extend_from_slice(&UTF8_REPLACEMENT_CHARACTER_BYTES);
                    Ok(())
                }
                .clone_dyn()
            }
            InvalidUnicodeHandler::SurrogateEscape => {
                |_pos, input: &[u8], out: &mut Vec<u8>| -> Result<(), String> {
                    utf8_surrocate_escape(input, out);
                    Ok(())
                }
                .clone_dyn()
            }
            InvalidUnicodeHandler::Custom(custom) => custom.clone_dyn(),
        },
        None => |_pos,
                 _input: &[u8],
                 _out: &mut Vec<u8>|
         -> Result<(), String> { Err(String::new()) } // TODO:?
        .clone_dyn(),
    };
    TransformData::ToStr(TfToStr {
        batch_iter: jd.field_mgr.claim_iter(
            tf_state.input_field,
            IterKind::Transform(jd.tf_mgr.transforms.peek_claim_id()),
        ),
        pending_streams: 0,
        invalid_unicode_handler: replacement_fn,
        convert_errors: op.convert_errors,
    })
}

pub fn handle_tf_to_str(jd: &mut JobData, tf_id: TransformId, tfc: &TfToStr) {
    let (batch_size, ps) = jd.tf_mgr.claim_batch(tf_id);
    let tf = &jd.tf_mgr.transforms[tf_id];
    let _op_id = tf.op_id.unwrap();
    let input_field_id = tf.input_field;
    let input_field = jd
        .field_mgr
        .get_cow_field_ref(&jd.match_set_mgr, tf.input_field);

    let mut output_field = jd.field_mgr.fields[tf.output_field].borrow_mut();

    let ofd = &mut output_field.iter_hall;
    let base_iter = jd
        .field_mgr
        .lookup_iter(tf.input_field, &input_field, tfc.batch_iter)
        .bounded(0, batch_size);
    let starting_pos = base_iter.get_next_field_pos();

    let mut iter =
        AutoDerefIter::new(&jd.field_mgr, tf.input_field, base_iter);

    while let Some(range) = iter.next_range(&jd.match_set_mgr) {
        match range.base.data {
            FieldValueSlice::Error(errs) => {
                if tfc.convert_errors {
                } else {
                    for (v, rl) in
                        RefAwareFieldValueRangeIter::from_range(&range, errs)
                    {
                        ofd.push_error(v.clone(), rl as usize, true, true);
                    }
                }
            }
            _ => todo!(),
        }
    }
    let iter_base = iter.into_base_iter();
    let consumed_fields = iter_base.get_next_field_pos() - starting_pos;
    jd.field_mgr
        .store_iter(input_field_id, tfc.batch_iter, iter_base);
    drop(input_field);
    drop(output_field);
    let streams_done = tfc.pending_streams == 0;

    if streams_done && ps.next_batch_ready {
        jd.tf_mgr.push_tf_in_ready_stack(tf_id);
    }
    jd.tf_mgr
        .submit_batch(tf_id, consumed_fields, ps.input_done);
}

pub fn handle_tf_to_str_stream_value_update(
    jd: &mut JobData,
    tf: &mut TfToStr,
    update: StreamValueUpdate,
) {
    let op_id = jd.tf_mgr.transforms[update.tf_id].op_id.unwrap();
    let stream_buffer_size = jd
        .get_transform_chain(update.tf_id)
        .settings
        .stream_buffer_size;
    let sv_in_id = update.sv_id;
    let sv_out_id = update.custom;
    let (sv_in, sv_out) = jd
        .sv_mgr
        .stream_values
        .two_distinct_mut(update.sv_id, sv_out_id);

    if sv_out.propagate_error(&sv_in.error) {
        jd.sv_mgr.inform_stream_value_subscribers(sv_out_id);
        jd.sv_mgr
            .drop_field_value_subscription(sv_in_id, Some(update.tf_id));
        return;
    }
    let input_done = sv_in.done;
    let mut iter = sv_in.data_cursor_from_update(&update);
    let mut inserter =
        sv_out.data_inserter(sv_out_id, stream_buffer_size, true);

    while let Some(mut data) = iter.next_steal(inserter.may_append_buffer()) {
        let input_data = match &mut data {
            StreamValueData::Text { .. } | StreamValueData::StaticText(_) => {
                inserter.append(data);
                continue;
            }
            StreamValueData::StaticBytes(v) => {
                if let Ok(s) = v.to_str() {
                    inserter.append(StreamValueData::StaticText(s));
                    continue;
                }
                *v
            }
            StreamValueData::Bytes { data, range } => {
                if let Some(data) = Arc::get_mut(data) {
                    retain_vec_range(data, range.clone());
                    if let Ok(text) = String::from_utf8(std::mem::take(data)) {
                        inserter.append(StreamValueData::from_string(text));
                        continue;
                    }
                }
                &data[range.clone()]
            }
        };
        let res = inserter
            .with_text_buffer(|buf| {
                encoding::decode_to_utf8(
                    &mut encoding_rs::UTF_8.new_decoder_without_bom_handling(),
                    input_data,
                    &mut tf.invalid_unicode_handler,
                    unsafe { buf.as_mut_vec() },
                    input_done,
                )
            })
            .unwrap();
        if let Err(message) = res {
            drop(inserter);
            sv_out.done = true;
            sv_out.error = Some(Arc::new(OperatorApplicationError::new_s(
                message, op_id,
            )));
            jd.sv_mgr.inform_stream_value_subscribers(sv_out_id);
            jd.sv_mgr
                .drop_field_value_subscription(sv_in_id, Some(update.tf_id));
            return;
        }
    }
}
