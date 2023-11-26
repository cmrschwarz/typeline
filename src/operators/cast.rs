use regex::Regex;
use smallstr::SmallString;

use crate::{
    job_session::JobData,
    options::argument::CliArgIdx,
    record_data::{
        field_data::{field_value_flags, FieldDataType, FieldValueKind},
        iter_hall::IterId,
        iters::FieldIterator,
        push_interface::PushInterface,
        ref_iter::AutoDerefIter,
        stream_value::{StreamValueData, StreamValueId},
        typed::TypedSlice,
        typed_iters::TypedSliceIter,
    },
    utils::encoding::{
        self, utf8_surrocate_escape, UTF8_REPLACEMENT_CHARACTER,
    },
};

use super::{
    errors::{OperatorApplicationError, OperatorCreationError},
    operator::{OperatorBase, OperatorData, DEFAULT_OP_NAME_SMALL_STR_LEN},
    transform::{TransformData, TransformId, TransformState},
};

#[derive(Clone)]
pub struct OpCast {
    target_type: FieldDataType,
    invalid_unicode_handler: Option<InvalidUnicodeHandler>,
    dont_convert_text_to_bytes: bool,
    convert_errors: bool,
}

impl OpCast {
    pub fn default_op_name(
        &self,
    ) -> SmallString<[u8; DEFAULT_OP_NAME_SMALL_STR_LEN]> {
        match self.target_type {
            FieldDataType::Html => "html",
            FieldDataType::Undefined => "undefined",
            FieldDataType::Null => "null",
            FieldDataType::Integer => "int",
            FieldDataType::Error => "error",
            FieldDataType::Text => "str",
            FieldDataType::Bytes => "bytes",
            FieldDataType::Object => unreachable!(),
        }
        .into()
    }
}

lazy_static::lazy_static! {
    static ref ARG_REGEX: Regex = Regex::new(r"^to_(?<type>int|bytes|str|(?:~)error|null|undefined)?$").unwrap();
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
        "int" => FieldDataType::Integer,
        "bytes" => FieldDataType::Bytes,
        "str" => FieldDataType::Text,
        "error" => FieldDataType::Error,
        "null" => FieldDataType::Null,
        "undefined" => FieldDataType::Undefined,
        _ => unreachable!(),
    };
    // TODO: parse options
    Ok(create_op_cast(target_type, None, false, false))
}

pub fn create_op_cast(
    target_type: FieldDataType,
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
    convert_errors: bool,
    target_type: FieldDataType,
}

pub fn setup_tf_cast<'a>(
    sess: &mut JobData,
    _op_base: &OperatorBase,
    op: &'a OpCast,
    tf_state: &mut TransformState,
) -> TransformData<'a> {
    tf_state.preferred_input_type = Some(match op.target_type {
        FieldDataType::Undefined => FieldValueKind::Undefined,
        FieldDataType::Null => FieldValueKind::Null,
        FieldDataType::Integer => FieldValueKind::Integer,
        FieldDataType::Error => FieldValueKind::Error,
        FieldDataType::Html => FieldValueKind::Html,
        FieldDataType::Bytes => FieldValueKind::BytesInline,
        FieldDataType::Text => FieldValueKind::BytesInline,
        FieldDataType::Object => FieldValueKind::Object,
    });
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
    let mut input_field =
        sess.field_mgr.fields[tf_state.input_field].borrow_mut();
    TransformData::Cast(TfCast {
        batch_iter: input_field.iter_hall.claim_iter(),
        pending_streams: 0,
        invalid_unicode_handler: replacement_fn,
        target_type: op.target_type,
        dont_convert_text_to_bytes: op.dont_convert_text_to_bytes,
        convert_errors: op.convert_errors,
    })
}

pub fn handle_tf_cast(sess: &mut JobData, tf_id: TransformId, tfc: &TfCast) {
    let (batch_size, input_done) = sess.tf_mgr.claim_batch(tf_id);
    let tf = &sess.tf_mgr.transforms[tf_id];
    let _op_id = tf.op_id.unwrap();
    let input_field_id = tf.input_field;
    let input_field = sess.field_mgr.get_cow_field_ref(
        &mut sess.match_set_mgr,
        tf.input_field,
        tf.has_unconsumed_input(),
    );

    let mut output_field = sess.field_mgr.fields[tf.output_field].borrow_mut();

    if tf.preferred_input_type.is_some_and(|i| i.is_zst())
        && tfc.convert_errors
    {
        output_field.iter_hall.push_zst(
            tf.preferred_input_type.unwrap(),
            batch_size,
            true,
        );
        if input_done {
            drop(output_field);
            drop(input_field);
            sess.unlink_transform(tf_id, batch_size);
            return;
        }
        sess.tf_mgr.update_ready_state(tf_id);
        sess.tf_mgr
            .inform_successor_batch_available(tf_id, batch_size);
        return;
    }
    let ofd = &mut output_field.iter_hall;
    let base_iter = sess
        .field_mgr
        .lookup_iter(tf.input_field, &input_field, tfc.batch_iter)
        .bounded(0, batch_size);
    let starting_pos = base_iter.get_next_field_pos();

    let mut iter =
        AutoDerefIter::new(&sess.field_mgr, tf.input_field, base_iter);

    while let Some(range) = iter.typed_range_fwd(
        &mut sess.match_set_mgr,
        usize::MAX,
        field_value_flags::BYTES_ARE_UTF8,
    ) {
        match range.base.data {
            TypedSlice::Error(errs) => {
                if tfc.convert_errors {
                } else {
                    for (v, rl) in
                        TypedSliceIter::from_range(&range.base, errs)
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
    sess.field_mgr
        .store_iter(input_field_id, tfc.batch_iter, iter_base);
    drop(input_field);
    drop(output_field);
    let streams_done = tfc.pending_streams == 0;
    if input_done && streams_done {
        sess.unlink_transform(tf_id, consumed_fields);
    } else {
        if streams_done {
            sess.tf_mgr.update_ready_state(tf_id);
        }
        sess.tf_mgr
            .inform_successor_batch_available(tf_id, consumed_fields);
    }
}

pub fn handle_tf_cast_stream_value_update(
    sess: &mut JobData,
    tf_id: TransformId,
    tf: &mut TfCast,
    sv_id: StreamValueId,
    custom: usize,
) {
    let op_id = sess.tf_mgr.transforms[tf_id].op_id.unwrap();
    let sv_out_id = custom;
    let (sv_in, sv_out) =
        sess.sv_mgr.stream_values.two_distinct_mut(sv_id, sv_out_id);
    match &sv_in.data {
        StreamValueData::Dropped => unreachable!(),
        StreamValueData::Error(err) => {
            if tf.convert_errors {
                sv_out.data =
                    StreamValueData::Bytes(err.message.as_bytes().to_owned());
                sv_out.bytes_are_chunk = false;
                sv_out.drop_previous_chunks = true;
                sv_out.bytes_are_utf8 = tf.target_type == FieldDataType::Text;
            } else {
                sv_out.data = StreamValueData::Error(err.clone());
            }
            sv_out.done = true;
            sess.sv_mgr.inform_stream_value_subscribers(sv_out_id);
            sess.sv_mgr
                .drop_field_value_subscription(sv_id, Some(tf_id));
        }
        StreamValueData::Bytes(bb) => {
            let StreamValueData::Bytes(out_data) = &mut sv_out.data else {
                unreachable!()
            };
            if sv_out.bytes_are_utf8 && !sv_in.bytes_are_utf8 {
                let res = encoding::decode_to_utf8(
                    &mut encoding_rs::UTF_8.new_decoder_without_bom_handling(),
                    bb,
                    &mut tf.invalid_unicode_handler,
                    out_data,
                    sv_in.done,
                );
                if let Err((_i, e)) = res {
                    sv_out.done = true;
                    sv_out.data = StreamValueData::Error(
                        OperatorApplicationError::new_s(e, op_id),
                    );
                    sess.sv_mgr.inform_stream_value_subscribers(sv_out_id);
                    sess.sv_mgr
                        .drop_field_value_subscription(sv_id, Some(tf_id));
                    return;
                }
            }
            // PERF: find a way to avoid this. maybe some
            // AntiTextFieldValueReference or something
            sv_out.data = sv_in.data.clone();
            sv_out.done = sv_in.done;
            sv_out.bytes_are_chunk = sv_in.bytes_are_chunk;
            sv_out.drop_previous_chunks = sv_in.drop_previous_chunks;
        }
    }
}
