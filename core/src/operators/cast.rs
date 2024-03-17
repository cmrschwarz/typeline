use regex::Regex;

use crate::{
    job::JobData,
    options::argument::CliArgIdx,
    record_data::{
        field_value::{FieldValue, FieldValueKind},
        field_value_ref::FieldValueSlice,
        iter_hall::{IterId, IterKind},
        iters::FieldIterator,
        push_interface::PushInterface,
        ref_iter::{AutoDerefIter, RefAwareFieldValueSliceIter},
        stream_value::StreamValueId,
    },
    utils::encoding::{
        self, utf8_surrocate_escape, UTF8_REPLACEMENT_CHARACTER,
    },
};

use super::{
    errors::{OperatorApplicationError, OperatorCreationError},
    operator::{DefaultOperatorName, OperatorBase, OperatorData},
    transform::{TransformData, TransformId, TransformState},
};

#[derive(Clone)]
pub struct OpCast {
    target_type: FieldValueKind,
    invalid_unicode_handler: Option<InvalidUnicodeHandler>,
    dont_convert_text_to_bytes: bool,
    convert_errors: bool,
}

impl OpCast {
    pub fn default_op_name(&self) -> DefaultOperatorName {
        self.target_type.to_str().into()
    }
}

lazy_static::lazy_static! {
    static ref ARG_REGEX: Regex = Regex::new(r"^to-(?<type>int|bytes|str|(?:~)error|null|undefined)?$").unwrap();
}

pub fn argument_matches_op_cast(arg: &str, value: Option<&[u8]>) -> bool {
    ARG_REGEX.is_match(arg) && value.is_none()
}

pub fn parse_op_cast(
    argument: &str,
    value: Option<&[u8]>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    // this should not happen in the cli parser because it checks using
    // `argument_matches_data_inserter`
    let args = ARG_REGEX.captures(argument).ok_or_else(|| {
        OperatorCreationError::new("invalid argument syntax for cast", arg_idx)
    })?;
    if value.is_some() {
        return Err(OperatorCreationError::new(
            "the cast operator does not take an argument",
            arg_idx,
        ));
    }
    let arg_str = args.name("type").unwrap().as_str();
    let target_type = match arg_str {
        "int" => FieldValueKind::Int,
        "bytes" => FieldValueKind::Bytes,
        "str" => FieldValueKind::Text,
        "error" => FieldValueKind::Error,
        "null" => FieldValueKind::Null,
        "undefined" => FieldValueKind::Undefined,
        _ => unreachable!(),
    };
    // TODO: parse options
    Ok(create_op_cast(target_type, None, false, false))
}

pub fn create_op_cast(
    target_type: FieldValueKind,
    invalid_unicode_handler: Option<InvalidUnicodeHandler>,
    dont_convert_text_to_bytes: bool,
    convert_errors: bool,
) -> OperatorData {
    OperatorData::Cast(OpCast {
        target_type,
        invalid_unicode_handler,
        dont_convert_text_to_bytes,
        convert_errors,
    })
}

pub trait InvalidUnicodeHandlerFn:
    FnMut(&[u8], &mut Vec<u8>) -> Result<(), String> + Send + Sync
{
    fn clone_dyn(&self) -> Box<dyn InvalidUnicodeHandlerFn>;
}

impl<
        F: FnMut(&[u8], &mut Vec<u8>) -> Result<(), String>
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

pub struct TfCast {
    batch_iter: IterId,
    pending_streams: usize,
    invalid_unicode_handler: Box<dyn InvalidUnicodeHandlerFn>,
    // TODO
    #[allow(dead_code)]
    dont_convert_text_to_bytes: bool,
    #[allow(dead_code)]
    target_type: FieldValueKind,
    convert_errors: bool,
}

pub fn build_tf_cast<'a>(
    jd: &mut JobData,
    _op_base: &OperatorBase,
    op: &'a OpCast,
    tf_state: &mut TransformState,
) -> TransformData<'a> {
    let replacement_fn: Box<dyn InvalidUnicodeHandlerFn> =
        match &op.invalid_unicode_handler {
            Some(h) => match h {
                InvalidUnicodeHandler::Lossy => {
                    |_input: &[u8], out: &mut Vec<u8>| -> Result<(), String> {
                        out.extend_from_slice(&UTF8_REPLACEMENT_CHARACTER);
                        Ok(())
                    }
                    .clone_dyn()
                }
                InvalidUnicodeHandler::SurrogateEscape => {
                    |input: &[u8], out: &mut Vec<u8>| -> Result<(), String> {
                        utf8_surrocate_escape(input, out);
                        Ok(())
                    }
                    .clone_dyn()
                }
                InvalidUnicodeHandler::Custom(custom) => custom.clone_dyn(),
            },
            None => |_input: &[u8],
                     _out: &mut Vec<u8>|
             -> Result<(), String> { Err(String::new()) }
            .clone_dyn(),
        };
    TransformData::Cast(TfCast {
        batch_iter: jd.field_mgr.claim_iter(
            tf_state.input_field,
            IterKind::Transform(jd.tf_mgr.transforms.peek_claim_id()),
        ),
        pending_streams: 0,
        invalid_unicode_handler: replacement_fn,
        target_type: op.target_type,
        dont_convert_text_to_bytes: op.dont_convert_text_to_bytes,
        convert_errors: op.convert_errors,
    })
}

pub fn handle_tf_cast(jd: &mut JobData, tf_id: TransformId, tfc: &TfCast) {
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
                        RefAwareFieldValueSliceIter::from_range(&range, errs)
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

pub fn handle_tf_cast_stream_value_update(
    jd: &mut JobData,
    tf_id: TransformId,
    tf: &mut TfCast,
    sv_id: StreamValueId,
    custom: usize,
) {
    let op_id = jd.tf_mgr.transforms[tf_id].op_id.unwrap();
    let sv_out_id = custom;
    let (sv_in, sv_out) =
        jd.sv_mgr.stream_values.two_distinct_mut(sv_id, sv_out_id);
    match &sv_in.value {
        FieldValue::Error(err) => {
            sv_out.value = FieldValue::Error(err.clone());
            sv_out.done = true;
            jd.sv_mgr.inform_stream_value_subscribers(sv_out_id);
            jd.sv_mgr.drop_field_value_subscription(sv_id, Some(tf_id));
        }
        FieldValue::Bytes(bb) => {
            let (out_is_utf8, out_data) = match &mut sv_out.value {
                FieldValue::Bytes(data) => (false, data),
                // SAFETY: we trust the decoder to produce valid utf-8
                FieldValue::Text(data) => (true, unsafe { data.as_mut_vec() }),
                _ => unreachable!(),
            };
            if out_is_utf8 {
                let res = encoding::decode_to_utf8(
                    &mut encoding_rs::UTF_8.new_decoder_without_bom_handling(),
                    bb,
                    &mut tf.invalid_unicode_handler,
                    out_data,
                    sv_in.done,
                );
                if let Err((_i, e)) = res {
                    sv_out.done = true;
                    sv_out.value = FieldValue::Error(
                        OperatorApplicationError::new_s(e, op_id),
                    );
                    jd.sv_mgr.inform_stream_value_subscribers(sv_out_id);
                    jd.sv_mgr
                        .drop_field_value_subscription(sv_id, Some(tf_id));
                    return;
                }
            }
            // PERF: find a way to avoid this. maybe some
            // AntiTextFieldValueReference or something
            sv_out.value = sv_in.value.clone();
            sv_out.done = sv_in.done;
            sv_out.is_buffered = sv_in.is_buffered;
        }
        _ => todo!(),
    }
}
